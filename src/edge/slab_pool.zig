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
//! The lease shape matches the inbound read-chunk pool in `wire_pump.zig`:
//! one contiguous allocation, a queue of free indices, and an index handed
//! back on release.
const std = @import("std");
const io_queue = @import("io_queue.zig");

pub const SlabPool = struct {
    storage: []u8,
    slab_bytes: usize,
    capacity: usize,
    free_buf: []u32,
    free_q: std.Io.Queue(u32) = undefined,
    /// Observable so a test and `deinit` can prove every slab came home.
    rented: std.atomic.Value(usize) = .init(0),

    /// One contiguous allocation for the bytes, one queue of free indices.
    /// Both are sized at boot and never grow, so the pool's own cost is exactly
    /// what `resourceUpperBound` reports.
    pub fn init(gpa: std.mem.Allocator, io: std.Io, slab_bytes: usize, capacity: usize) !SlabPool {
        std.debug.assert(slab_bytes > 0);
        std.debug.assert(capacity > 0);
        const storage = try gpa.alloc(u8, slab_bytes * capacity);
        errdefer gpa.free(storage);
        const free_buf = try gpa.alloc(u32, capacity);
        errdefer gpa.free(free_buf);

        var self: SlabPool = .{
            .storage = storage,
            .slab_bytes = slab_bytes,
            .capacity = capacity,
            .free_buf = free_buf,
        };
        self.free_q = .init(self.free_buf);
        var i: u32 = 0;
        while (i < capacity) : (i += 1) {
            self.free_q.putOneUncancelable(io, i) catch {
                // The queue was sized for exactly `capacity` indices.
                unreachable;
            };
        }
        return self;
    }

    /// Every slab must be back before the storage goes away. A rented slab at
    /// this point is a lease some teardown path forgot, and the assert names it
    /// here rather than letting the next process reuse freed bytes.
    pub fn deinit(self: *SlabPool, gpa: std.mem.Allocator) void {
        std.debug.assert(self.rented.load(.acquire) == 0);
        gpa.free(self.storage);
        gpa.free(self.free_buf);
        self.* = undefined;
    }

    /// Take a slab, or null when the pool is empty. Never allocates.
    ///
    /// A null is BACKPRESSURE, not an error: the caller must park the writer
    /// and retry, never drop the write. A stream that already holds a slab is
    /// unaffected, which is what keeps exhaustion from stalling the drainer.
    pub fn tryRent(self: *SlabPool, io: std.Io) ?[]u8 {
        const idx = io_queue.tryGet(u32, &self.free_q, io) orelse return null;
        _ = self.rented.fetchAdd(1, .acq_rel);
        const off = @as(usize, idx) * self.slab_bytes;
        return self.storage[off..][0..self.slab_bytes];
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
        const idx: u32 = @intCast(off / self.slab_bytes);
        std.debug.assert(idx < self.capacity);
        std.debug.assert(slab.len == self.slab_bytes);

        const prev = self.rented.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
        if (!io_queue.tryPut(u32, &self.free_q, io, idx)) {
            // The queue holds exactly `capacity` indices and this one was taken
            // out of it, so there is always room. A full queue means the index
            // is already in it: a double return.
            unreachable;
        }
    }

    pub fn rentedCount(self: *const SlabPool) usize {
        return self.rented.load(.acquire);
    }
};

const testing = std.testing;

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
