//! Server-wide pool of fixed-size outbound DATA slabs.
//!
//! # Why this replaces per-connection preallocation
//!
//! `FairScheduler` used to reserve `max_streams_per_connection` slabs at
//! connection boot, so a connection cost `max_streams × outbound_bytes_per_stream`
//! whether it served one request or two hundred. At the defaults that is 16 MiB
//! per connection, and the memory bound scaled with CONNECTIONS rather than with
//! concurrent work.
//!
//! Go and Kestrel both rent write buffers from a shared pool for this reason,
//! and neither can tell an operator what its ceiling is. This pool keeps the
//! ceiling: capacity is configured, `tryRent` returns null at the limit, and
//! `Limits.resourceUpperBound` counts the pool once for the whole server
//! instead of multiplying it by every connection.
//!
//! # What a shared pool makes possible that preallocation did not
//!
//! Preallocation made three failure modes unrepresentable, and they are now
//! real. They are the reason this file has the tests it has:
//!
//! - A LEAK. A slab not returned on some teardown path is gone for the life of
//!   the process, and starves every other connection rather than just its own.
//! - A DOUBLE RETURN, which hands the same bytes to two streams at once.
//! - EXHAUSTION. `tryRent` can fail, which a per-connection slab could never
//!   do. It must degrade to backpressure on the writer, never to a dropped
//!   write and never to a stalled drainer — a stream that already HOLDS a slab
//!   must always be able to make progress, or the pool deadlocks under load.
//!
//! Storage stays one contiguous virtual allocation, while an atomic bitmap
//! tracks free indices. A FIFO free-index queue is wrong here: a workload with
//! one tiny write at a time eventually rotates through every slab and faults
//! one OS page from each. At 4,096 × 64 KiB slabs on a 16 KiB-page host that
//! turns a one-slab working set into 64 MiB RSS. Claiming the lowest free bit
//! keeps resident memory proportional to concurrent writers while preserving
//! the same hard capacity and allocation-free hot path.
const std = @import("std");
const boot_plan = @import("../core/boot_plan.zig");

/// A fixed array of bits with atomic access. `FreeBitmap` is one of these plus
/// the claim protocol; `RequestPool` keeps a second one beside it to mark
/// which blocks are resident.
pub const AtomicBits = struct {
    words: []std.atomic.Value(u64),

    pub fn wordCount(capacity: usize) usize {
        return (capacity + 63) / 64;
    }

    pub fn get(self: AtomicBits, idx: u32) bool {
        const mask = @as(u64, 1) << @as(u6, @intCast(idx % 64));
        return self.words[idx / 64].load(.acquire) & mask != 0;
    }

    /// Returns the previous value of the bit.
    pub fn set(self: AtomicBits, idx: u32) bool {
        const mask = @as(u64, 1) << @as(u6, @intCast(idx % 64));
        return self.words[idx / 64].fetchOr(mask, .acq_rel) & mask != 0;
    }

    /// Returns the previous value of the bit.
    pub fn clear(self: AtomicBits, idx: u32) bool {
        const mask = @as(u64, 1) << @as(u6, @intCast(idx % 64));
        return self.words[idx / 64].fetchAnd(~mask, .acq_rel) & mask != 0;
    }

    pub fn count(self: AtomicBits) usize {
        var n: usize = 0;
        for (self.words) |*w| n += @popCount(w.load(.acquire));
        return n;
    }
};

/// The free map of a pool: a set bit is a free index. `claim` takes the
/// LOWEST free index, which is the whole point of this type (see the file
/// header: a FIFO list would walk the pool and fault every page).
pub const FreeBitmap = struct {
    bits: AtomicBits,
    capacity: usize,

    /// Every one of `capacity` indices free. `words` must hold
    /// `AtomicBits.wordCount(capacity)` words; the bits past `capacity` stay
    /// clear, so a claim can never return an index that does not exist.
    pub fn initAllFree(words: []std.atomic.Value(u64), capacity: usize) FreeBitmap {
        std.debug.assert(words.len == AtomicBits.wordCount(capacity));
        for (words, 0..) |*word, wi| {
            const remaining = capacity - wi * 64;
            const valid_bits = @min(remaining, 64);
            const bits = if (valid_bits == 64)
                std.math.maxInt(u64)
            else
                (@as(u64, 1) << @intCast(valid_bits)) - 1;
            word.* = .init(bits);
        }
        return .{ .bits = .{ .words = words }, .capacity = capacity };
    }

    /// Take the lowest free index, or null when none is free.
    pub fn claim(self: *FreeBitmap) ?u32 {
        return self.claimWhere(null, false);
    }

    /// Take the lowest free index whose bit in `other` equals `other_set`.
    ///
    /// `other` is read without holding the index, so by the time the claim
    /// lands the other bit may have changed: a caller that cares must read it
    /// again after the claim, when it owns the index. (Two takers can move one
    /// index through claim, change and release between this read and this
    /// compare-exchange, and the word then looks unchanged.)
    pub fn claimWhere(self: *FreeBitmap, other: ?AtomicBits, other_set: bool) ?u32 {
        for (self.bits.words, 0..) |*word, wi| {
            var bits = word.load(.acquire);
            while (true) {
                var candidates = bits;
                if (other) |o| {
                    const ow = o.words[wi].load(.acquire);
                    candidates &= if (other_set) ow else ~ow;
                }
                if (candidates == 0) break;
                const bit: u6 = @intCast(@ctz(candidates));
                const mask = @as(u64, 1) << bit;
                const next = bits & ~mask;
                if (word.cmpxchgWeak(bits, next, .acq_rel, .acquire)) |observed| {
                    bits = observed;
                    continue;
                }
                const idx = wi * 64 + bit;
                std.debug.assert(idx < self.capacity);
                return @intCast(idx);
            }
        }
        return null;
    }

    /// Give an index back. A double return trips the assert.
    pub fn release(self: *FreeBitmap, idx: u32) void {
        std.debug.assert(idx < self.capacity);
        const was_free = self.bits.set(idx);
        std.debug.assert(!was_free);
    }

    pub fn freeCount(self: *const FreeBitmap) usize {
        return self.bits.count();
    }
};

pub const SlabPool = struct {
    storage: []u8,
    slab_bytes: usize,
    capacity: usize,
    free: FreeBitmap,
    /// Observable so a test and `deinit` can prove every slab came home.
    rented: std.atomic.Value(usize) = .init(0),

    /// The two allocations of `init`, in its order. See `boot_plan`.
    pub fn planBoot(plan: *boot_plan.Plan, slab_bytes: usize, capacity: usize) error{InvalidConfig}!void {
        // `init` asserts the capacity fits a `u32` slab index.
        if (capacity > boot_plan.max_count) return error.InvalidConfig;
        const total = try boot_plan.mul(slab_bytes, capacity);
        try plan.array(u8, total);
        try plan.array(std.atomic.Value(u64), AtomicBits.wordCount(capacity));
    }

    /// One contiguous allocation for the bytes, one atomic free bitmap.
    /// Both are sized at boot and never grow, so the pool's own cost is exactly
    /// what `resourceUpperBound` reports.
    pub fn init(gpa: std.mem.Allocator, io: std.Io, slab_bytes: usize, capacity: usize) !SlabPool {
        _ = io;
        std.debug.assert(slab_bytes > 0);
        std.debug.assert(capacity > 0);
        std.debug.assert(capacity <= std.math.maxInt(u32));
        const storage = try gpa.alloc(u8, slab_bytes * capacity);
        errdefer gpa.free(storage);
        const free_words = try gpa.alloc(std.atomic.Value(u64), AtomicBits.wordCount(capacity));
        errdefer gpa.free(free_words);

        return .{
            .storage = storage,
            .slab_bytes = slab_bytes,
            .capacity = capacity,
            .free = .initAllFree(free_words, capacity),
        };
    }

    /// Every slab must be back before the storage goes away. A rented slab at
    /// this point is a lease some teardown path forgot, and the assert names it
    /// here rather than letting the next process reuse freed bytes.
    pub fn deinit(self: *SlabPool, gpa: std.mem.Allocator) void {
        self.assertAllReturned();
        gpa.free(self.storage);
        gpa.free(self.free.bits.words);
        self.* = undefined;
    }

    /// The check `deinit` makes, for an owner that rewinds the pool's memory
    /// instead of freeing it.
    pub fn assertAllReturned(self: *const SlabPool) void {
        std.debug.assert(self.rented.load(.acquire) == 0);
    }

    /// Take a slab, or null when the pool is empty. Never allocates.
    ///
    /// A null is BACKPRESSURE, not an error: the caller must park the writer
    /// and retry, never drop the write. A stream that already holds a slab is
    /// unaffected, which is what keeps exhaustion from stalling the drainer.
    pub fn tryRent(self: *SlabPool, io: std.Io) ?[]u8 {
        const idx = self.tryRentIndex(io) orelse return null;
        return self.slabAt(idx);
    }

    /// `tryRent` for callers that carry the slab as an index (wire chunks: the
    /// index travels in `WireChunk.pool_index` and comes back through a pump).
    /// Same lowest-free-bit claim, same backpressure meaning of null.
    pub fn tryRentIndex(self: *SlabPool, io: std.Io) ?u32 {
        _ = io;
        const idx = self.free.claim() orelse return null;
        _ = self.rented.fetchAdd(1, .acq_rel);
        return idx;
    }

    pub fn slabAt(self: *SlabPool, idx: u32) []u8 {
        std.debug.assert(idx < self.capacity);
        const off = @as(usize, idx) * self.slab_bytes;
        return self.storage[off..][0..self.slab_bytes];
    }

    /// True when `ptr` is inside this pool's storage (including a used-prefix
    /// view that shares the slab's base pointer).
    pub fn containsPtr(self: *const SlabPool, ptr: [*]const u8) bool {
        if (self.storage.len == 0) return false;
        const here = @intFromPtr(ptr);
        const base = @intFromPtr(self.storage.ptr);
        return here >= base and here < base + self.storage.len;
    }

    /// Reconstruct the full slab from a pointer that may be a used prefix.
    /// The prefix MUST start at the slab base; a mid-slab pointer asserts.
    pub fn slabFromPtr(self: *SlabPool, ptr: [*]u8) []u8 {
        const here = @intFromPtr(ptr);
        const base = @intFromPtr(self.storage.ptr);
        std.debug.assert(here >= base);
        const off = here - base;
        std.debug.assert(off % self.slab_bytes == 0);
        std.debug.assert(off < self.storage.len);
        return self.storage[off..][0..self.slab_bytes];
    }

    /// Index of the slab a pointer starts. Same rules as `slabFromPtr`: the
    /// pointer must be a slab base inside this pool.
    pub fn indexFromPtr(self: *const SlabPool, ptr: [*]const u8) u32 {
        const here = @intFromPtr(ptr);
        const base = @intFromPtr(self.storage.ptr);
        std.debug.assert(here >= base);
        const off = here - base;
        std.debug.assert(off % self.slab_bytes == 0);
        std.debug.assert(off < self.storage.len);
        return @intCast(off / self.slab_bytes);
    }

    /// Give a slab back. It must have come from this pool.
    ///
    /// The index is derived from the pointer rather than tracked by the caller,
    /// so a slab cannot be returned to the wrong slot. The asserts catch a
    /// foreign pointer and a misaligned one, which is what a double return or a
    /// subslice would look like.
    pub fn release(self: *SlabPool, io: std.Io, slab: []u8) void {
        const base = @intFromPtr(self.storage.ptr);
        const here = @intFromPtr(slab.ptr);
        std.debug.assert(here >= base);
        const off = here - base;
        std.debug.assert(off % self.slab_bytes == 0);
        std.debug.assert(slab.len == self.slab_bytes);
        self.releaseIndex(io, @intCast(off / self.slab_bytes));
    }

    /// `release` by index. A double return trips the bitmap assert below.
    pub fn releaseIndex(self: *SlabPool, io: std.Io, idx: u32) void {
        _ = io;
        std.debug.assert(idx < self.capacity);
        const prev = self.rented.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
        self.free.release(idx);
    }

    pub fn rentedCount(self: *const SlabPool) usize {
        return self.rented.load(.acquire);
    }
};

const testing = std.testing;

test "containsPtr and slabFromPtr recover a used prefix" {
    var pool = try SlabPool.init(testing.allocator, testing.io, 64, 2);
    defer pool.deinit(testing.allocator);
    const slab = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    const prefix = slab[0..9];
    try testing.expect(pool.containsPtr(prefix.ptr));
    const full = pool.slabFromPtr(prefix.ptr);
    try testing.expectEqual(slab.ptr, full.ptr);
    try testing.expectEqual(slab.len, full.len);
    pool.release(testing.io, full);
    var foreign: [8]u8 = undefined;
    try testing.expect(!pool.containsPtr((&foreign).ptr));
}

test "a fresh pool has every slab free" {
    var pool = try SlabPool.init(testing.allocator, testing.io, 1024, 4);
    defer pool.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 0), pool.rentedCount());
    try testing.expectEqual(@as(usize, 4), pool.capacity);
}

test "a rented slab is exactly slab_bytes" {
    var pool = try SlabPool.init(testing.allocator, testing.io, 1024, 4);
    defer pool.deinit(testing.allocator);
    const slab = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    defer pool.release(testing.io, slab);
    try testing.expectEqual(@as(usize, 1024), slab.len);
    try testing.expectEqual(@as(usize, 1), pool.rentedCount());
}

test "exhaustion returns null rather than allocating" {
    // The property preallocation gave for free. A writer that cannot rent must
    // be told so, not handed memory the bound did not count.
    var pool = try SlabPool.init(testing.allocator, testing.io, 64, 2);
    defer pool.deinit(testing.allocator);

    const a = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    const b = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    try testing.expectEqual(@as(?[]u8, null), pool.tryRent(testing.io));
    try testing.expectEqual(@as(usize, 2), pool.rentedCount());

    pool.release(testing.io, a);
    pool.release(testing.io, b);
}

test "a released slab is reusable, and accounting returns to zero" {
    var pool = try SlabPool.init(testing.allocator, testing.io, 64, 1);
    defer pool.deinit(testing.allocator);

    const first = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    try testing.expectEqual(@as(?[]u8, null), pool.tryRent(testing.io));
    pool.release(testing.io, first);
    try testing.expectEqual(@as(usize, 0), pool.rentedCount());

    const second = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    defer pool.release(testing.io, second);
    try testing.expectEqual(@as(usize, 64), second.len);
}

test "released low slab is reused before untouched slabs" {
    var pool = try SlabPool.init(testing.allocator, testing.io, 64, 4);
    defer pool.deinit(testing.allocator);

    const first = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    const first_ptr = first.ptr;
    pool.release(testing.io, first);
    const second = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    defer pool.release(testing.io, second);
    // Locality is load-bearing: rotating through all free slabs faults one OS
    // page per slab and turns a tiny working set into the pool's full RSS.
    try testing.expectEqual(first_ptr, second.ptr);
}

test "two live slabs never alias" {
    // A double return would hand the same bytes to two streams. Writing through
    // one lease must never be visible through another.
    var pool = try SlabPool.init(testing.allocator, testing.io, 8, 3);
    defer pool.deinit(testing.allocator);

    const a = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    const b = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    const c = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
    defer pool.release(testing.io, a);
    defer pool.release(testing.io, b);
    defer pool.release(testing.io, c);

    @memset(a, 0xAA);
    @memset(b, 0xBB);
    @memset(c, 0xCC);
    for (a) |byte| try testing.expectEqual(@as(u8, 0xAA), byte);
    for (b) |byte| try testing.expectEqual(@as(u8, 0xBB), byte);
    for (c) |byte| try testing.expectEqual(@as(u8, 0xCC), byte);
}

test "accounting stays exact across an interleaved rent and release run" {
    var pool = try SlabPool.init(testing.allocator, testing.io, 16, 4);
    defer pool.deinit(testing.allocator);

    var held: [4]?[]u8 = .{ null, null, null, null };
    var round: usize = 0;
    while (round < 200) : (round += 1) {
        const slot = round % held.len;
        if (held[slot]) |slab| {
            pool.release(testing.io, slab);
            held[slot] = null;
        } else {
            held[slot] = pool.tryRent(testing.io);
        }
        // The pool never reports more rented than it owns.
        try testing.expect(pool.rentedCount() <= pool.capacity);

        var live: usize = 0;
        for (held) |maybe| {
            if (maybe != null) live += 1;
        }
        try testing.expectEqual(live, pool.rentedCount());
    }
    for (held) |maybe| {
        if (maybe) |slab| pool.release(testing.io, slab);
    }
    try testing.expectEqual(@as(usize, 0), pool.rentedCount());
}

test "a full pool drains and refills without losing a slab" {
    // The leak gate in miniature: rent everything, return everything, twice.
    var pool = try SlabPool.init(testing.allocator, testing.io, 32, 8);
    defer pool.deinit(testing.allocator);

    var pass: usize = 0;
    while (pass < 2) : (pass += 1) {
        var taken: [8][]u8 = undefined;
        for (&taken) |*slot| slot.* = pool.tryRent(testing.io) orelse return error.ExpectedSlab;
        try testing.expectEqual(@as(usize, 8), pool.rentedCount());
        try testing.expectEqual(@as(?[]u8, null), pool.tryRent(testing.io));
        for (taken) |slab| pool.release(testing.io, slab);
        try testing.expectEqual(@as(usize, 0), pool.rentedCount());
    }
}

test "index rent reuses the lowest free index, so one writer at a time touches one slab" {
    var pool = try SlabPool.init(testing.allocator, testing.io, 64, 4);
    defer pool.deinit(testing.allocator);
    // One write in flight at a time, many times: always slab 0. A FIFO free
    // list would walk 0, 1, 2, 3 and fault every slab's pages.
    var round: usize = 0;
    while (round < 8) : (round += 1) {
        const idx = pool.tryRentIndex(testing.io).?;
        try testing.expectEqual(@as(u32, 0), idx);
        pool.releaseIndex(testing.io, idx);
    }
    const a = pool.tryRentIndex(testing.io).?;
    const b = pool.tryRentIndex(testing.io).?;
    try testing.expectEqual(@as(u32, 0), a);
    try testing.expectEqual(@as(u32, 1), b);
    try testing.expect(pool.slabAt(a).ptr != pool.slabAt(b).ptr);
    pool.releaseIndex(testing.io, a);
    try testing.expectEqual(@as(u32, 0), pool.tryRentIndex(testing.io).?);
    pool.releaseIndex(testing.io, 0);
    pool.releaseIndex(testing.io, b);
}

test "FreeBitmap claims the lowest free index and refuses past capacity" {
    var words: [2]std.atomic.Value(u64) = undefined;
    var map = FreeBitmap.initAllFree(&words, 70);
    try testing.expectEqual(@as(usize, 70), map.freeCount());
    var taken: [70]u32 = undefined;
    for (&taken, 0..) |*t, i| {
        t.* = map.claim().?;
        try testing.expectEqual(@as(u32, @intCast(i)), t.*);
    }
    try testing.expectEqual(@as(?u32, null), map.claim());
    try testing.expectEqual(@as(usize, 0), map.freeCount());
    map.release(65);
    map.release(3);
    try testing.expectEqual(@as(?u32, 3), map.claim());
    try testing.expectEqual(@as(?u32, 65), map.claim());
}

test "FreeBitmap claimWhere takes the lowest free index with the other bit as asked" {
    var free_words: [2]std.atomic.Value(u64) = undefined;
    var other_words: [2]std.atomic.Value(u64) = .{ .init(0), .init(0) };
    var map = FreeBitmap.initAllFree(&free_words, 100);
    const other: AtomicBits = .{ .words = &other_words };
    // Indices 5 and 70 have the other bit set.
    try testing.expect(!other.set(5));
    try testing.expect(!other.set(70));
    try testing.expectEqual(@as(?u32, 5), map.claimWhere(other, true));
    try testing.expectEqual(@as(?u32, 70), map.claimWhere(other, true));
    try testing.expectEqual(@as(?u32, null), map.claimWhere(other, true));
    // Without the bit, 0 is first, and the already claimed 5 is skipped.
    try testing.expectEqual(@as(?u32, 0), map.claimWhere(other, false));
    map.release(5);
    // 5 is free again but has the bit set, so a "clear" claim skips it.
    try testing.expectEqual(@as(?u32, 1), map.claimWhere(other, false));
    try testing.expect(other.clear(5));
    try testing.expectEqual(@as(?u32, 2), map.claimWhere(other, false));
}
