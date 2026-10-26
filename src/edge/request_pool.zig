//! Server-wide pool of request memory blocks, and the bump arena that runs on
//! top of it.
//!
//! # Why
//!
//! Under `std.heap.smp_allocator` a block freed on a different thread than the
//! one that made it is not given back to the thread that made it, and the
//! waste moves to wherever the traffic moves. A request's arena was made on the
//! actor, grown on the handler's executor and freed on whichever task finished
//! the job. This pool takes the general allocator out of that path: every
//! request block comes from, and goes back to, one fixed mapping, and the actor
//! is the only party that returns blocks after a handler ran.
//!
//! # Shape
//!
//! Blocks come in power-of-two classes from `min_block_bytes` (64 KiB) up to
//! the per-request cap `arena_bytes` (4 MiB by default). One class is one
//! contiguous mapping of `server_bytes / class_size` blocks plus an atomic
//! free bitmap (`slab_pool.FreeBitmap`, the structure `SlabPool` uses) and a
//! second bitmap that says which blocks are resident. A taker claims the
//! lowest free resident block, so a small working set touches a small number
//! of pages (see the header of `slab_pool.zig` for why lowest-bit and not
//! FIFO). There is no mutex, no per-executor cache, and no allocation on the
//! take and return path.
//!
//! The mappings are starh2's own (`std.heap.page_allocator`), not the
//! application's allocator, for two reasons. Only memory that nobody else
//! shares can be decommitted. And the allocator's own debug fill would write
//! the whole range if it came from `alloc`. Reserving the address space is a
//! boot step: about `class_count` times `server_bytes` of it, of which only
//! pages a request touched cost RSS. A failed reservation is a boot error.
//!
//! # The cap counts RESIDENT bytes
//!
//! Blocks are never given back to the general allocator, so a cap on the bytes
//! requests hold at one moment would let each class's high-water mark add up
//! past the cap over a mixed history (a burst of 64 KiB blocks, then a burst
//! of 2 MiB blocks). So `resident_bytes` counts every block whose pages may be
//! committed, free or in use. An untouched block is reserved with a
//! compare-exchange before its first use, so two takers cannot both pass the
//! check. When a take needs an untouched block and the counter has no room, it
//! RECLAIMS: it claims a free resident block of some class (the largest
//! first), decommits it, clears its resident bit, gives its bytes back to the
//! counter, and tries again. Only when no free resident block exists anywhere
//! is the take refused. In a steady state nothing is reclaimed.
//!
//! Decommit follows Ghostty (`terminal/mem.zig`): Linux `MADV_DONTNEED`, Darwin
//! `MADV_FREE_REUSABLE` and `MADV_FREE_REUSE` before the block is used again.
//! The bytes of a decommitted block are undefined to its next user. On Darwin
//! `FREE_REUSABLE` lowers the task's `phys_footprint`, which is what the OS
//! uses to judge memory pressure; it does NOT lower `ps` RSS or
//! `resident_size` until the kernel reclaims the pages itself.
//!
//! # Three caps, three reasons
//!
//! A request holds blocks through an `Arena`. Every block it takes is charged,
//! by its CLASS size and never by how much of it is used, against three limits
//! in this order: the request's own (`arena_bytes`), the connection's
//! (`ConnBudget`), and the server's (the resident counter above). Whichever
//! refuses is recorded in `Arena.refusal` as a `Reason`, and the edge maps it
//! to a status: `request_cap` is a 500, the others a 503.
//!
//! # Threads
//!
//! `RequestPool` and `ConnBudget` are safe from any thread. An `Arena` is
//! touched by ONE task at a time: the actor decodes the headers and appends
//! the body into it, then the handler's task allocates from it, then the
//! actor returns its blocks after the handler was joined. Each hand-over is
//! ordered by a spawn, a join or a channel, so the arena has no atomics of its
//! own. Blocks are returned only by the actor (`Arena.release`), except that
//! the handler's own allocator may return an emptied non-first block at once.
const std = @import("std");
const builtin = @import("builtin");
const slab_pool = @import("slab_pool.zig");

pub const min_block_bytes: usize = 64 * 1024;
/// 64 KiB << 15 is 2 GiB, far past any sane per-request cap.
pub const max_classes: usize = 16;
/// Bytes at the start of every block that belong to the arena's bookkeeping.
/// 64 keeps the first allocation cache-line aligned.
pub const header_bytes: usize = 64;

const page_align: std.mem.Alignment = .fromByteUnits(std.heap.page_size_min);
const Page = []align(std.heap.page_size_min) u8;

comptime {
    std.debug.assert(min_block_bytes % std.heap.page_size_min == 0);
    std.debug.assert(std.math.isPowerOfTwo(min_block_bytes));
}

/// Which limit refused an allocation.
pub const Reason = enum(u8) {
    /// The request would hold more than `arena_bytes`.
    request_cap,
    /// The connection's requests would hold more than `request_bytes_per_connection`.
    connection_cap,
    /// The resident cap is reached and nothing can be reclaimed, or every
    /// block of the needed class is in use.
    server_cap,

    pub const count = @typeInfo(Reason).@"enum".field_names.len;

    /// The status a request gets when its memory is refused before its
    /// response is committed.
    pub fn status(self: Reason) u16 {
        return switch (self) {
            .request_cap => 500,
            .connection_cap, .server_cap => 503,
        };
    }
};

pub const Config = struct {
    /// `Limits.request_bytes_per_server`: the cap on resident bytes.
    server_bytes: usize,
    /// `Limits.request_arena_bytes`: the per-request cap, and the largest
    /// class. A power of two, at least `min_block_bytes`.
    arena_bytes: usize,
    /// `Limits.request_pool_retain_bytes`: the most FREE resident bytes the
    /// pool keeps. A block returned while free resident bytes are over this is
    /// decommitted at once (see `give`). At most `server_bytes`.
    retain_bytes: usize,

    pub fn check(self: Config) error{InvalidConfig}!void {
        if (self.arena_bytes < min_block_bytes) return error.InvalidConfig;
        if (!std.math.isPowerOfTwo(self.arena_bytes)) return error.InvalidConfig;
        if (self.server_bytes < self.arena_bytes) return error.InvalidConfig;
        if (self.retain_bytes > self.server_bytes) return error.InvalidConfig;
        // Past this the class index would not fit `max_classes`.
        if (self.arena_bytes / min_block_bytes >= (@as(usize, 1) << (max_classes - 1))) return error.InvalidConfig;
        // A block index is a `u32`.
        if (self.server_bytes / min_block_bytes > std.math.maxInt(u32)) return error.InvalidConfig;
    }

    pub fn classCount(self: Config) usize {
        return std.math.log2_int(usize, self.arena_bytes / min_block_bytes) + 1;
    }

    /// Bytes of bitmap words the pool allocates: two bitmaps per class, with
    /// the allocator's page rounding included. This is the part of the pool
    /// that lives in the general address space; the block mappings do not.
    pub fn bitmapBytes(self: Config) error{InvalidConfig}!usize {
        try self.check();
        var words: usize = 0;
        for (0..self.classCount()) |k| {
            const blocks = self.server_bytes / (min_block_bytes << @intCast(k));
            words += 2 * slab_pool.AtomicBits.wordCount(blocks);
        }
        const bytes = words * @sizeOf(std.atomic.Value(u64));
        return std.mem.alignForward(usize, bytes, std.heap.page_size_max);
    }
};

/// One block, as the pool hands it out.
pub const Block = struct {
    class: u8,
    index: u32,
    mem: Page,
};

pub const ClassCounters = struct {
    size: usize,
    blocks: usize,
    /// An untouched or decommitted block made resident.
    made: u64,
    /// A resident block taken again.
    reused: u64,
    /// A resident block decommitted to make room for another class.
    reclaimed: u64,
    /// A block decommitted when it was returned, because free resident bytes
    /// were over the retain limit.
    retired: u64,
};

pub const Snapshot = struct {
    class_count: usize,
    classes: [max_classes]ClassCounters,
    refused: [Reason.count]u64,
    resident_bytes: usize,
    resident_high: usize,
    held_bytes: usize,
    held_high: usize,
    free_resident_bytes: usize,
};

pub const RequestPool = struct {
    classes: [max_classes]Class,
    class_count: usize,
    server_bytes: usize,
    arena_bytes: usize,
    retain_bytes: usize,
    /// The transparent huge page size the touched-bytes scan rounds to, read
    /// once at init (`readHugePageSize`). Zero means unknown: the scan then
    /// covers each used class's whole mapping, an answer that can only be too
    /// large, never too small.
    huge_page_bytes: usize,
    bit_words: []std.atomic.Value(u64),
    /// Bytes of resident blocks, free or in use. Never passes `server_bytes`.
    resident_bytes: std.atomic.Value(usize) = .init(0),
    resident_high: std.atomic.Value(usize) = .init(0),
    /// Bytes of blocks in use, for the high-water mark.
    held_bytes: std.atomic.Value(usize) = .init(0),
    held_high: std.atomic.Value(usize) = .init(0),
    /// Bytes of blocks that are free AND resident. Returns raise it with a
    /// compare-exchange that keeps it within `retain_bytes`; a take or a
    /// reclaim of a resident free block lowers it. It is the one number the
    /// keep-or-retire decision in `give` reads, so that decision is a single
    /// atomic step.
    free_resident_bytes: std.atomic.Value(usize) = .init(0),
    refused: [Reason.count]std.atomic.Value(u64) = @splat(.init(0)),

    const Class = struct {
        size: usize,
        blocks: usize,
        storage: Page,
        free: slab_pool.FreeBitmap,
        resident: slab_pool.AtomicBits,
        made: std.atomic.Value(u64) = .init(0),
        reused: std.atomic.Value(u64) = .init(0),
        reclaimed: std.atomic.Value(u64) = .init(0),
        retired: std.atomic.Value(u64) = .init(0),
        /// One past the highest block index ever made resident. Pages of this
        /// class exist only below it, so `touchedBytes` scans only that prefix.
        made_end: std.atomic.Value(u32) = .init(0),

        fn block(self: *const Class, class: u8, index: u32) Block {
            const off = @as(usize, index) * self.size;
            return .{ .class = class, .index = index, .mem = @alignCast(self.storage[off..][0..self.size]) };
        }
    };

    pub fn init(cfg: Config) error{ OutOfMemory, InvalidConfig }!RequestPool {
        try cfg.check();
        const class_count = cfg.classCount();
        var total_words: usize = 0;
        for (0..class_count) |k| {
            const blocks = cfg.server_bytes / (min_block_bytes << @intCast(k));
            total_words += 2 * slab_pool.AtomicBits.wordCount(blocks);
        }
        const bit_words = try std.heap.page_allocator.alloc(std.atomic.Value(u64), total_words);
        errdefer std.heap.page_allocator.free(bit_words);

        var self: RequestPool = .{
            .classes = undefined,
            .class_count = class_count,
            .server_bytes = cfg.server_bytes,
            .arena_bytes = cfg.arena_bytes,
            .retain_bytes = cfg.retain_bytes,
            .huge_page_bytes = readHugePageSize() orelse 0,
            .bit_words = bit_words,
        };
        var made: usize = 0;
        errdefer for (self.classes[0..made]) |c| rawUnmap(c.storage);
        var word_at: usize = 0;
        for (0..class_count) |k| {
            const size = min_block_bytes << @intCast(k);
            const blocks = cfg.server_bytes / size;
            const nwords = slab_pool.AtomicBits.wordCount(blocks);
            // `rawAlloc`, not `alloc`: `alloc` fills the range with the debug
            // pattern, which would make the whole reservation resident.
            const raw = std.heap.page_allocator.rawAlloc(size * blocks, page_align, @returnAddress()) orelse
                return error.OutOfMemory;
            const resident_words = bit_words[word_at + nwords ..][0..nwords];
            for (resident_words) |*w| w.* = .init(0);
            self.classes[k] = .{
                .size = size,
                .blocks = blocks,
                .storage = @alignCast(raw[0 .. size * blocks]),
                .free = .initAllFree(bit_words[word_at..][0..nwords], blocks),
                .resident = .{ .words = resident_words },
            };
            // Linux transparent huge pages: with THP `always`, a touched page of
            // a mapping this large is backed by a 2 MiB huge page, and the
            // pool then holds far more than the bytes it counts. Measured on
            // nachos (stage 2, smp, 10 cycles): idle RSS about 2.4 GB against
            // about 0.4 GB for stage 1, and 12 to 14 MB of growth in every idle
            // phase while the pool's counters did not move; with this advice
            // on the pool mapping alone, idle RSS fell by about 1 GB. Advisory:
            // a kernel without THP answers EINVAL, which is fine. Darwin has no
            // THP.
            if (comptime builtin.os.tag == .linux) {
                std.posix.madvise(self.classes[k].storage.ptr, self.classes[k].storage.len, std.posix.MADV.NOHUGEPAGE) catch {};
            }
            word_at += 2 * nwords;
            made += 1;
        }
        return self;
    }

    fn rawUnmap(storage: Page) void {
        std.heap.page_allocator.rawFree(storage, page_align, @returnAddress());
    }

    /// Every block must be home. A block still held at this point is a request
    /// memory leak, and the assert names it before the mappings go away.
    pub fn deinit(self: *RequestPool) void {
        self.assertAllHome();
        for (self.classes[0..self.class_count]) |c| rawUnmap(c.storage);
        std.heap.page_allocator.free(self.bit_words);
        self.* = undefined;
    }

    pub fn assertAllHome(self: *const RequestPool) void {
        std.debug.assert(self.held_bytes.load(.acquire) == 0);
        for (self.classes[0..self.class_count]) |*c| {
            std.debug.assert(c.free.freeCount() == c.blocks);
        }
    }

    /// The smallest class whose blocks hold `need` bytes, or null when `need`
    /// is past the largest class (the per-request cap).
    pub fn classFor(self: *const RequestPool, need: usize) ?u8 {
        if (need > self.arena_bytes) return null;
        const blocks = std.math.divCeil(usize, @max(need, min_block_bytes), min_block_bytes) catch unreachable;
        const class = std.math.log2_int_ceil(usize, blocks);
        std.debug.assert(class < self.class_count);
        return @intCast(class);
    }

    pub fn classBytes(self: *const RequestPool, class: u8) usize {
        return self.classes[class].size;
    }

    /// Take a block of `class`, or null when the server cap refuses. Never
    /// allocates, and never blocks.
    ///
    /// Null is returned only if, at some instant during the call, no block of
    /// the class was free, or the cap had no room and nothing could be
    /// reclaimed. Two races would otherwise make it spurious, and each has a
    /// retry: a block of the class freed between the first two claims, and a
    /// reclaim by another taker that freed the room this one just found
    /// missing.
    pub fn take(self: *RequestPool, class: u8) ?Block {
        const c = &self.classes[class];
        while (true) {
            // A resident block first: it is warm, and it costs no cap. Then an
            // untouched or decommitted one, which does. Then any, for a block
            // freed after the first claim looked.
            const idx = c.free.claimWhere(c.resident, true) orelse
                c.free.claimWhere(c.resident, false) orelse
                c.free.claim() orelse return null;
            // The resident bit is read again now that the block is ours: the
            // claim read it before it won the block (see `claimWhere`).
            if (c.resident.get(idx)) {
                // It was counted as free resident when it was returned.
                const was = self.free_resident_bytes.fetchSub(c.size, .acq_rel);
                std.debug.assert(was >= c.size);
                _ = c.reused.fetchAdd(1, .monotonic);
                self.noteHeld(c.size);
                return c.block(class, idx);
            }
            if (self.reserveResident(c.size)) {
                const blk = c.block(class, idx);
                recommit(blk.mem);
                _ = c.resident.set(idx);
                _ = c.made_end.fetchMax(idx + 1, .monotonic);
                _ = c.made.fetchAdd(1, .monotonic);
                self.noteHeld(c.size);
                return blk;
            }
            // No room under the cap. Put the block back and make room.
            c.free.release(idx);
            if (self.reclaimOne()) continue;
            // Nothing to reclaim. Another taker may have freed room since the
            // reserve failed (it reclaimed the block this one would have).
            if (self.resident_bytes.load(.acquire) <= self.server_bytes - c.size) continue;
            return null;
        }
    }

    /// Give a block back.
    ///
    /// Keep or retire is ONE atomic step on `free_resident_bytes`: add this
    /// block's size with a compare-exchange if that keeps the total within the
    /// retain limit (the block stays resident for the next taker), else retire
    /// it. A retired block is decommitted before it is released: its pages go
    /// back to the OS, its resident bit and its bytes leave the resident
    /// counter, and `retired` counts it. So however many threads return at
    /// once, free resident bytes never pass the limit: the returners that
    /// find room are exactly as many as fit.
    ///
    /// What a request pays above the limit: one madvise on return and fresh
    /// page faults on the next use of the block. That is what `munmap` cost
    /// stage 1 for every buffer above 32 KiB (`SmpAllocator` sends those to
    /// mmap and munmap); the limit decides how many bytes of reuse are free.
    pub fn give(self: *RequestPool, blk: Block) void {
        const c = &self.classes[blk.class];
        std.debug.assert(c.resident.get(blk.index));
        const prev = self.held_bytes.fetchSub(c.size, .acq_rel);
        std.debug.assert(prev >= c.size);
        var cur = self.free_resident_bytes.load(.acquire);
        while (true) {
            if (c.size > self.retain_bytes or cur > self.retain_bytes - c.size) {
                decommit(blk.mem);
                _ = c.resident.clear(blk.index);
                const before = self.resident_bytes.fetchSub(c.size, .acq_rel);
                std.debug.assert(before >= c.size);
                _ = c.retired.fetchAdd(1, .monotonic);
                break;
            }
            // Counted before the block is released, so a taker that claims it
            // always finds its bytes in the counter to take out.
            cur = self.free_resident_bytes.cmpxchgWeak(cur, cur + c.size, .acq_rel, .acquire) orelse break;
        }
        c.free.release(blk.index);
    }

    /// Where the scan of a class ends: the blocks up to the highest one ever
    /// made resident, extended to the end of the huge page that holds the last
    /// of them (`scanEndFor`). A block past that was never touched.
    fn scanEnd(self: *const RequestPool, c: *const Class) usize {
        const made = @as(usize, c.made_end.load(.acquire)) * c.size;
        return scanEndFor(@intFromPtr(c.storage.ptr), made, c.storage.len, self.huge_page_bytes);
    }

    /// Bytes of the class mappings that `touchedBytes` scans.
    pub fn scannedBytes(self: *const RequestPool) usize {
        var total: usize = 0;
        for (self.classes[0..self.class_count]) |*c| total += self.scanEnd(c);
        return total;
    }

    /// Bytes of the class mappings that really have pages, from `mincore`.
    /// Null only where this OS cannot say (Darwin: `mincore` does not see
    /// `MADV_FREE_REUSABLE` pages as gone, and the task's footprint is
    /// process-wide). On Linux a failing `mincore` is an error, never a null:
    /// a reader must be able to tell "not available here" from "failed".
    /// With transparent huge pages this can be far more than
    /// `resident_bytes`, which is how the pool measures itself.
    ///
    /// Scans `scannedBytes()`, not the reservation: at a 4 GiB cap the
    /// reservation is tens of GiB of page table entries.
    pub fn touchedBytes(self: *const RequestPool) std.posix.MincoreError!?usize {
        if (comptime builtin.os.tag != .linux) return null;
        const page = std.heap.pageSize();
        var vec: [4096]u8 = undefined;
        var total: usize = 0;
        for (self.classes[0..self.class_count]) |*c| {
            const end = self.scanEnd(c);
            var off: usize = 0;
            while (off < end) {
                const n = @min(end - off, vec.len * page);
                const part: []align(std.heap.page_size_min) u8 = @alignCast(c.storage[off..][0..n]);
                try std.posix.mincore(part.ptr, n, &vec);
                for (vec[0 .. n / page]) |v| total += @as(usize, v & 1) * page;
                off += n;
            }
        }
        return total;
    }

    pub fn noteRefusal(self: *RequestPool, reason: Reason) void {
        _ = self.refused[@backingInt(reason)].fetchAdd(1, .monotonic);
    }

    fn noteHeld(self: *RequestPool, n: usize) void {
        const now = self.held_bytes.fetchAdd(n, .acq_rel) + n;
        _ = self.held_high.fetchMax(now, .monotonic);
    }

    /// Add `n` to the resident counter if that keeps it within the cap.
    fn reserveResident(self: *RequestPool, n: usize) bool {
        var cur = self.resident_bytes.load(.acquire);
        while (true) {
            if (n > self.server_bytes or cur > self.server_bytes - n) return false;
            if (self.resident_bytes.cmpxchgWeak(cur, cur + n, .acq_rel, .acquire)) |observed| {
                cur = observed;
                continue;
            }
            _ = self.resident_high.fetchMax(cur + n, .monotonic);
            return true;
        }
    }

    /// Decommit one free resident block, the largest class first, and give its
    /// bytes back to the counter. False when no class has one.
    fn reclaimOne(self: *RequestPool) bool {
        var k = self.class_count;
        while (k != 0) {
            k -= 1;
            const c = &self.classes[k];
            const idx = c.free.claimWhere(c.resident, true) orelse continue;
            // Ours now, but a racing taker can have turned the block
            // non-resident between the read and the claim; then it is simply
            // a free block and needs nothing.
            if (!c.resident.get(idx)) {
                c.free.release(idx);
                continue;
            }
            const was = self.free_resident_bytes.fetchSub(c.size, .acq_rel);
            std.debug.assert(was >= c.size);
            const blk = c.block(@intCast(k), idx);
            decommit(blk.mem);
            _ = c.resident.clear(idx);
            const prev = self.resident_bytes.fetchSub(c.size, .acq_rel);
            std.debug.assert(prev >= c.size);
            _ = c.reclaimed.fetchAdd(1, .monotonic);
            c.free.release(idx);
            return true;
        }
        return false;
    }

    pub fn snapshot(self: *const RequestPool) Snapshot {
        var s: Snapshot = .{
            .class_count = self.class_count,
            .classes = undefined,
            .refused = undefined,
            .resident_bytes = self.resident_bytes.load(.acquire),
            .resident_high = self.resident_high.load(.acquire),
            .held_bytes = self.held_bytes.load(.acquire),
            .held_high = self.held_high.load(.acquire),
            .free_resident_bytes = self.free_resident_bytes.load(.acquire),
        };
        for (self.classes[0..self.class_count], 0..) |*c, k| {
            s.classes[k] = .{
                .size = c.size,
                .blocks = c.blocks,
                .made = c.made.load(.monotonic),
                .reused = c.reused.load(.monotonic),
                .reclaimed = c.reclaimed.load(.monotonic),
                .retired = c.retired.load(.monotonic),
            };
        }
        for (&s.refused, &self.refused) |*dst, *src| dst.* = src.load(.monotonic);
        return s;
    }
};

/// The end, as an offset into a mapping at address `base` of `len` bytes, of a
/// scan that must cover the first `made` bytes and any huge page of size
/// `huge` that holds the last of them. Huge page boundaries are absolute
/// addresses, so the rounding is done on `base + made`, not on `made`: the
/// mapping is only page aligned. Capped at the mapping. `huge` zero means the
/// size is unknown, and then the whole mapping counts once anything is used:
/// the scan can be too large, never too small.
fn scanEndFor(base: usize, made: usize, len: usize, huge: usize) usize {
    if (made == 0) return 0;
    if (huge == 0) return len;
    return @min(std.mem.alignForward(usize, base + made, huge) - base, len);
}

/// A huge page size from the text of
/// `/sys/kernel/mm/transparent_hugepage/hpage_pmd_size`: a decimal number of
/// bytes that is a power of two and at least a base page. Null for anything
/// else.
fn parseHugePageSize(text: []const u8) ?usize {
    const n = std.fmt.parseInt(usize, std.mem.trim(u8, text, " \t\r\n"), 10) catch return null;
    if (n < std.heap.page_size_min or !std.math.isPowerOfTwo(n)) return null;
    return n;
}

/// The kernel's transparent huge page size (Linux: 2 MiB with 4 KiB base
/// pages on x86_64 and aarch64, 32 MiB or 512 MiB with 16 KiB or 64 KiB base
/// pages on aarch64). Null off Linux, and on Linux when the file cannot be
/// read or parsed (no THP in the kernel has no file); the caller then scans
/// whole mappings, which fails toward a number that is too large.
fn readHugePageSize() ?usize {
    if (comptime builtin.os.tag != .linux) return null;
    const linux = std.os.linux;
    const opened = linux.openat(linux.AT.FDCWD, "/sys/kernel/mm/transparent_hugepage/hpage_pmd_size", .{}, 0);
    if (linux.errno(opened) != .SUCCESS) return null;
    const fd: i32 = @intCast(opened);
    defer _ = linux.close(fd);
    var buf: [32]u8 = undefined;
    const got = linux.read(fd, &buf, buf.len);
    if (linux.errno(got) != .SUCCESS) return null;
    return parseHugePageSize(buf[0..got]);
}

/// Hand the physical pages of `mem` back to the OS and keep the address range.
/// A failure is a panic: the resident counter is about to say these bytes are
/// gone, and a madvise that did nothing would make that a lie.
fn decommit(mem: Page) void {
    switch (comptime builtin.os.tag) {
        .linux => std.posix.madvise(mem.ptr, mem.len, std.posix.MADV.DONTNEED) catch |err|
            std.debug.panic("madvise(DONTNEED) failed: {s}", .{@errorName(err)}),
        .macos, .ios, .tvos, .watchos, .visionos => std.posix.madvise(mem.ptr, mem.len, std.posix.MADV.FREE_REUSABLE) catch |err|
            std.debug.panic("madvise(FREE_REUSABLE) failed: {s}", .{@errorName(err)}),
        else => @compileError("RequestPool has no decommit for this OS"),
    }
}

/// Darwin accounts a `FREE_REUSABLE` range as reusable until `FREE_REUSE` says
/// it is wanted again. Harmless on a range that was never decommitted. Linux
/// needs nothing.
fn recommit(mem: Page) void {
    switch (comptime builtin.os.tag) {
        .linux => {},
        .macos, .ios, .tvos, .watchos, .visionos => std.posix.madvise(mem.ptr, mem.len, std.posix.MADV.FREE_REUSE) catch |err|
            std.debug.panic("madvise(FREE_REUSE) failed: {s}", .{@errorName(err)}),
        else => @compileError("RequestPool has no recommit for this OS"),
    }
}

/// What one connection's requests may hold at once, by class bytes. Atomic,
/// because the handlers of one connection run on different executors.
pub const ConnBudget = struct {
    held: std.atomic.Value(usize) = .init(0),
    limit: usize,

    pub fn init(limit: usize) ConnBudget {
        return .{ .limit = limit };
    }

    pub fn tryCharge(self: *ConnBudget, n: usize) bool {
        var cur = self.held.load(.acquire);
        while (true) {
            if (n > self.limit or cur > self.limit - n) return false;
            cur = self.held.cmpxchgWeak(cur, cur + n, .acq_rel, .acquire) orelse return true;
        }
    }

    pub fn uncharge(self: *ConnBudget, n: usize) void {
        const prev = self.held.fetchSub(n, .acq_rel);
        std.debug.assert(prev >= n);
    }
};

/// The bump arena of one request, over pool blocks. See the file header for
/// the threading rule: one task at a time.
///
/// Allocator semantics:
/// - `alloc` bumps the newest block; when it does not fit, takes the smallest
///   class that holds the request (plus the header and alignment).
/// - `resize` and `remap` grow or shrink in place when the memory is the tail
///   of its block and the block has room. `ArrayList` growth relies on this.
/// - `free` rewinds a tail allocation. A block that is not the request's first
///   and is left empty goes back to its class at once, so a buffer that grows
///   through every class holds its first block, the block it is leaving and
///   the block it is entering, and not every class it passed through.
/// - `release` returns every block; only the actor calls it, after the
///   handler was joined.
///
/// Every charge is the CLASS size, so a 1.5 MiB buffer in a 2 MiB block costs
/// 2 MiB against all three caps.
pub const Arena = struct {
    pool: *RequestPool,
    budget: *ConnBudget,
    /// Class bytes of the blocks this request holds now.
    held: usize = 0,
    /// The most `held` reached. Tests and the bench read it.
    peak: usize = 0,
    /// Newest block, the one `alloc` bumps first.
    head: ?*Header = null,
    /// Oldest block. Never returned before `release`.
    first: ?*Header = null,
    /// The reason of the most recent refusal, null if none.
    refusal: ?Reason = null,

    /// Lives in the first `header_bytes` of every block.
    const Header = struct {
        /// The next older block of this request.
        prev: ?*Header,
        size: usize,
        /// Offset of the next free byte from the block's base.
        cursor: usize,
        /// Offset where the block's first allocation starts. A cursor at or
        /// below this means the block holds nothing.
        floor: usize,
        index: u32,
        class: u8,

        fn base(self: *Header) usize {
            return @intFromPtr(self);
        }
    };

    comptime {
        std.debug.assert(@sizeOf(Header) <= header_bytes);
    }

    pub fn init(pool: *RequestPool, budget: *ConnBudget) Arena {
        return .{ .pool = pool, .budget = budget };
    }

    pub fn allocator(self: *Arena) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = vAlloc,
        .resize = vResize,
        .remap = vRemap,
        .free = vFree,
    };

    /// Make sure the newest block has `n` bytes free, taking a new block if
    /// not. After it succeeds, allocations that total at most `n` bytes,
    /// alignment padding included, cannot fail. HPACK decode uses it: its
    /// output is bounded (`hpack.Decoder.requestBytesBound`), so a request that
    /// holds this much cannot run out in the middle of a decode.
    pub fn reserve(self: *Arena, n: usize) error{OutOfMemory}!void {
        if (self.head) |h| {
            if (h.size - h.cursor >= n) return;
        }
        _ = self.newBlock(header_bytes, n) orelse return error.OutOfMemory;
    }

    /// Return every block. The arena can be used again afterwards.
    pub fn release(self: *Arena) void {
        while (self.head) |h| self.returnBlock(h);
        self.first = null;
        std.debug.assert(self.held == 0);
    }

    fn newBlock(self: *Arena, start: usize, len: usize) ?*Header {
        const need = std.math.add(usize, start, len) catch {
            self.refuse(.request_cap);
            return null;
        };
        const class = self.pool.classFor(need) orelse {
            self.refuse(.request_cap);
            return null;
        };
        const size = self.pool.classBytes(class);
        if (self.held + size > self.pool.arena_bytes) {
            self.refuse(.request_cap);
            return null;
        }
        if (!self.budget.tryCharge(size)) {
            self.refuse(.connection_cap);
            return null;
        }
        const blk = self.pool.take(class) orelse {
            self.budget.uncharge(size);
            self.refuse(.server_cap);
            return null;
        };
        self.held += size;
        self.peak = @max(self.peak, self.held);
        const h: *Header = @ptrCast(@alignCast(blk.mem.ptr));
        h.* = .{
            .prev = self.head,
            .size = size,
            .cursor = header_bytes,
            .floor = header_bytes,
            .index = blk.index,
            .class = blk.class,
        };
        self.head = h;
        if (self.first == null) self.first = h;
        return h;
    }

    fn refuse(self: *Arena, reason: Reason) void {
        self.refusal = reason;
        self.pool.noteRefusal(reason);
    }

    fn returnBlock(self: *Arena, h: *Header) void {
        if (self.head == h) {
            self.head = h.prev;
        } else {
            var newer = self.head.?;
            while (newer.prev != h) newer = newer.prev.?;
            newer.prev = h.prev;
        }
        if (self.first == h) self.first = null;
        const size = h.size;
        const blk: Block = .{
            .class = h.class,
            .index = h.index,
            .mem = @as([*]align(std.heap.page_size_min) u8, @ptrCast(@alignCast(h)))[0..size],
        };
        self.held -= size;
        self.pool.give(blk);
        self.budget.uncharge(size);
    }

    fn bump(h: *Header, len: usize, alignment: std.mem.Alignment) ?[*]u8 {
        const addr = alignment.forward(h.base() + h.cursor);
        const end = std.math.add(usize, addr - h.base(), len) catch return null;
        if (end > h.size) return null;
        h.cursor = end;
        return @ptrFromInt(addr);
    }

    fn vAlloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, _: usize) ?[*]u8 {
        const self: *Arena = @ptrCast(@alignCast(ctx));
        if (self.head) |h| {
            if (bump(h, len, alignment)) |p| return p;
        }
        // A new block, sized for the worst start of this allocation. Every
        // block base is page aligned, so for an alignment up to a page the
        // start is exactly the header rounded up to it. Above a page the base
        // is only known to be a multiple of a page: the next multiple of the
        // alignment after `base + header` is at most one alignment past the
        // base (the base plus the header is never itself a multiple, since
        // the header is less than a page). An alignment no class can hold,
        // and a size that overflows, are a request over its cap: `newBlock`
        // records that.
        const align_bytes = alignment.toByteUnits();
        const start_max = if (align_bytes <= std.heap.page_size_min)
            std.mem.alignForward(usize, header_bytes, align_bytes)
        else
            align_bytes;
        const h = self.newBlock(start_max, len) orelse return null;
        // Align from the block's real base.
        const start = alignment.forward(h.base() + header_bytes) - h.base();
        std.debug.assert(start + len <= h.size);
        h.floor = start;
        h.cursor = start;
        return bump(h, len, alignment) orelse unreachable;
    }

    /// The block `ptr` lies in. Every pointer the arena handed out is in one of
    /// its blocks, so a miss is a foreign pointer or a double free.
    fn blockOf(self: *Arena, ptr: [*]u8) *Header {
        const at = @intFromPtr(ptr);
        var cur = self.head;
        while (cur) |h| : (cur = h.prev) {
            if (at >= h.base() and at < h.base() + h.size) return h;
        }
        std.debug.panic("request arena: pointer {x} is not in any of its blocks", .{at});
    }

    fn vResize(ctx: *anyopaque, memory: []u8, _: std.mem.Alignment, new_len: usize, _: usize) bool {
        const self: *Arena = @ptrCast(@alignCast(ctx));
        const h = self.blockOf(memory.ptr);
        const start = @intFromPtr(memory.ptr) - h.base();
        const is_tail = start + memory.len == h.cursor;
        if (new_len <= memory.len) {
            if (is_tail) h.cursor = start + new_len;
            return true;
        }
        if (!is_tail) return false;
        const end = std.math.add(usize, start, new_len) catch return false;
        if (end > h.size) return false;
        h.cursor = end;
        return true;
    }

    fn vRemap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        return if (vResize(ctx, memory, alignment, new_len, ret_addr)) memory.ptr else null;
    }

    fn vFree(ctx: *anyopaque, memory: []u8, _: std.mem.Alignment, _: usize) void {
        const self: *Arena = @ptrCast(@alignCast(ctx));
        if (memory.len == 0) return;
        const h = self.blockOf(memory.ptr);
        const start = @intFromPtr(memory.ptr) - h.base();
        if (start + memory.len == h.cursor) h.cursor = start;
        if (h.cursor <= h.floor and self.first != h) self.returnBlock(h);
    }
};

const testing = std.testing;

fn testPool(server_bytes: usize, arena_bytes: usize) !RequestPool {
    return testPoolRetain(server_bytes, arena_bytes, server_bytes);
}

fn testPoolRetain(server_bytes: usize, arena_bytes: usize, retain_bytes: usize) !RequestPool {
    return RequestPool.init(.{ .server_bytes = server_bytes, .arena_bytes = arena_bytes, .retain_bytes = retain_bytes });
}

test "class sizes run from 64 KiB to the request cap" {
    var pool = try testPool(32 * 1024 * 1024, 4 * 1024 * 1024);
    defer pool.deinit();
    try testing.expectEqual(@as(usize, 7), pool.class_count);
    try testing.expectEqual(@as(usize, 64 * 1024), pool.classBytes(0));
    try testing.expectEqual(@as(usize, 4 * 1024 * 1024), pool.classBytes(6));
    try testing.expectEqual(@as(?u8, 0), pool.classFor(1));
    try testing.expectEqual(@as(?u8, 0), pool.classFor(64 * 1024));
    try testing.expectEqual(@as(?u8, 1), pool.classFor(64 * 1024 + 1));
    try testing.expectEqual(@as(?u8, 6), pool.classFor(4 * 1024 * 1024));
    try testing.expectEqual(@as(?u8, null), pool.classFor(4 * 1024 * 1024 + 1));
    // Each class reserves the whole cap in its own blocks.
    for (pool.classes[0..pool.class_count]) |c| {
        try testing.expectEqual(@as(usize, 32 * 1024 * 1024), c.size * c.blocks);
    }
}

test "config check refuses what cannot be a pool" {
    try testing.expectError(error.InvalidConfig, (Config{ .server_bytes = 1 << 30, .arena_bytes = 32 * 1024, .retain_bytes = 0 }).check());
    try testing.expectError(error.InvalidConfig, (Config{ .server_bytes = 1 << 30, .arena_bytes = 96 * 1024, .retain_bytes = 0 }).check());
    try testing.expectError(error.InvalidConfig, (Config{ .server_bytes = 1 << 20, .arena_bytes = 4 << 20, .retain_bytes = 0 }).check());
    try (Config{ .server_bytes = 4 << 20, .arena_bytes = 4 << 20, .retain_bytes = 4 << 20 }).check();
    // The retain limit cannot pass the server cap.
    try testing.expectError(error.InvalidConfig, (Config{ .server_bytes = 4 << 20, .arena_bytes = 4 << 20, .retain_bytes = (4 << 20) + 1 }).check());
    try (Config{ .server_bytes = 64 * 1024, .arena_bytes = 64 * 1024, .retain_bytes = 0 }).check();
}

test "a take claims the lowest free resident block, per class" {
    var pool = try testPool(8 * 1024 * 1024, 1024 * 1024);
    defer pool.deinit();
    inline for (.{ 0, 2, 4 }) |class| {
        const a = pool.take(class).?;
        const b = pool.take(class).?;
        try testing.expectEqual(@as(u32, 0), a.index);
        try testing.expectEqual(@as(u32, 1), b.index);
        pool.give(a);
        // The freed low block is taken again before the untouched one.
        const again = pool.take(class).?;
        try testing.expectEqual(@as(u32, 0), again.index);
        try testing.expectEqual(a.mem.ptr, again.mem.ptr);
        pool.give(again);
        pool.give(b);
    }
}

test "made stays flat across a second round of the same work" {
    var pool = try testPool(16 * 1024 * 1024, 2 * 1024 * 1024);
    defer pool.deinit();
    var round: usize = 0;
    var made_after_first: [max_classes]u64 = undefined;
    while (round < 2) : (round += 1) {
        var held: [12]Block = undefined;
        var n: usize = 0;
        for ([_]u8{ 0, 0, 0, 1, 1, 2, 3, 4, 5, 5 }) |class| {
            held[n] = pool.take(class).?;
            n += 1;
        }
        for (held[0..n]) |b| pool.give(b);
        const s = pool.snapshot();
        if (round == 0) {
            for (s.classes[0..s.class_count], 0..) |c, k| made_after_first[k] = c.made;
        } else {
            for (s.classes[0..s.class_count], 0..) |c, k| try testing.expectEqual(made_after_first[k], c.made);
            try testing.expect(s.classes[0].reused >= 3);
        }
    }
    try testing.expectEqual(@as(u64, 0), pool.snapshot().classes[0].reclaimed);
}

test "the server cap refuses at exactly its limit, in class bytes, and records the reason" {
    // Cap 512 KiB, request cap 256 KiB.
    var pool = try testPool(512 * 1024, 256 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    var a = Arena.init(&pool, &budget);
    var b = Arena.init(&pool, &budget);
    var c = Arena.init(&pool, &budget);
    defer a.release();
    defer b.release();
    defer c.release();
    // Two 256 KiB blocks fill the cap exactly.
    _ = try a.allocator().alloc(u8, 200 * 1024);
    try testing.expectEqual(@as(usize, 256 * 1024), a.held);
    _ = try b.allocator().alloc(u8, 200 * 1024);
    try testing.expectEqual(@as(usize, 512 * 1024), pool.snapshot().resident_bytes);
    // One byte of anything more needs a block, and the cap is full.
    try testing.expectError(error.OutOfMemory, c.allocator().alloc(u8, 1));
    try testing.expectEqual(Reason.server_cap, c.refusal.?);
    try testing.expectEqual(@as(u64, 1), pool.snapshot().refused[@backingInt(Reason.server_cap)]);
    try testing.expectEqual(@as(usize, 0), c.held);
    try testing.expectEqual(@as(usize, 0), budget.held.load(.acquire) - a.held - b.held);
}

test "the request cap refuses at exactly its limit, charging class bytes" {
    var pool = try testPool(8 * 1024 * 1024, 256 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    var a = Arena.init(&pool, &budget);
    defer a.release();
    const al = a.allocator();
    // 100 KiB lands in the 128 KiB class: 128 KiB charged, not 100.
    const x = try al.alloc(u8, 100 * 1024);
    try testing.expectEqual(@as(usize, 128 * 1024), a.held);
    // A second 100 KiB does not fit the first block and needs another 128 KiB:
    // 256 KiB held, exactly the cap.
    const y = try al.alloc(u8, 100 * 1024);
    try testing.expectEqual(@as(usize, 256 * 1024), a.held);
    try testing.expectEqual(@as(?Reason, null), a.refusal);
    // Anything that needs a third block is over the cap.
    try testing.expectError(error.OutOfMemory, al.alloc(u8, 64 * 1024));
    try testing.expectEqual(Reason.request_cap, a.refusal.?);
    // An allocation past the largest class can never fit.
    try testing.expectError(error.OutOfMemory, al.alloc(u8, 257 * 1024));
    try testing.expectEqual(@as(u64, 2), pool.snapshot().refused[@backingInt(Reason.request_cap)]);
    al.free(y);
    al.free(x);
}

test "the connection cap refuses at exactly its limit and the budget is exact" {
    var pool = try testPool(8 * 1024 * 1024, 1024 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(192 * 1024);
    var a = Arena.init(&pool, &budget);
    var b = Arena.init(&pool, &budget);
    defer a.release();
    defer b.release();
    _ = try a.allocator().alloc(u8, 10);
    try testing.expectEqual(@as(usize, 64 * 1024), budget.held.load(.acquire));
    _ = try b.allocator().alloc(u8, 100 * 1024); // 128 KiB: 192 KiB in total
    try testing.expectEqual(@as(usize, 192 * 1024), budget.held.load(.acquire));
    try testing.expectError(error.OutOfMemory, a.allocator().alloc(u8, 70 * 1024));
    try testing.expectEqual(Reason.connection_cap, a.refusal.?);
    try testing.expectEqual(@as(usize, 192 * 1024), budget.held.load(.acquire));
    try testing.expectEqual(@as(u64, 1), pool.snapshot().refused[@backingInt(Reason.connection_cap)]);
    a.release();
    b.release();
    try testing.expectEqual(@as(usize, 0), budget.held.load(.acquire));
}

test "bump arena: alloc, alignment and distinct memory" {
    var pool = try testPool(8 * 1024 * 1024, 1024 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    var a = Arena.init(&pool, &budget);
    defer a.release();
    const al = a.allocator();
    const p = try al.alloc(u8, 7);
    const q = try al.alignedAlloc(u8, .@"64", 100);
    const r = try al.create(u64);
    try testing.expectEqual(@as(usize, 0), @intFromPtr(q.ptr) % 64);
    try testing.expectEqual(@as(usize, 0), @intFromPtr(r) % 8);
    @memset(p, 1);
    @memset(q, 2);
    r.* = 0x0303030303030303;
    for (p) |v| try testing.expectEqual(@as(u8, 1), v);
    for (q) |v| try testing.expectEqual(@as(u8, 2), v);
    // One block served all three.
    try testing.expectEqual(@as(usize, 64 * 1024), a.held);
    // A block starts on a page, with the header ahead of the first byte.
    try testing.expectEqual(header_bytes, @intFromPtr(p.ptr) % std.heap.page_size_min);
}

test "bump arena: tail resize grows and shrinks in place, a non-tail does not grow" {
    var pool = try testPool(8 * 1024 * 1024, 1024 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    var a = Arena.init(&pool, &budget);
    defer a.release();
    const al = a.allocator();
    var list: std.ArrayList(u8) = .empty;
    try list.appendNTimes(al, 'x', 1000);
    const head = list.items.ptr;
    // Growth within the block's room stays put: it is the tail.
    try list.appendNTimes(al, 'y', 30 * 1024);
    try testing.expectEqual(head, list.items.ptr);
    try testing.expectEqual(@as(usize, 64 * 1024), a.held);
    // Shrink in place, then a following allocation lands right behind it.
    const shrunk = al.remap(list.allocatedSlice(), 2000).?;
    try testing.expectEqual(head, shrunk.ptr);
    const after = try al.alloc(u8, 8);
    try testing.expectEqual(@intFromPtr(head) + 2000, @intFromPtr(after.ptr));
    // `list` is no longer the tail: it cannot grow in place, so it moves.
    try testing.expect(al.remap(shrunk, 4000) == null);
    list = .empty;
    al.free(after);
}

test "bump arena: a tail free rewinds, a non-tail free changes nothing" {
    var pool = try testPool(8 * 1024 * 1024, 1024 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    var a = Arena.init(&pool, &budget);
    defer a.release();
    const al = a.allocator();
    const one = try al.alloc(u8, 100);
    const two = try al.alloc(u8, 100);
    // Freeing `one` while `two` follows it does nothing: the next allocation
    // lands after `two`.
    al.free(one);
    const three = try al.alloc(u8, 100);
    try testing.expect(@intFromPtr(three.ptr) > @intFromPtr(two.ptr));
    // Freeing the tail rewinds: the same address is handed out again.
    al.free(three);
    const four = try al.alloc(u8, 100);
    try testing.expectEqual(three.ptr, four.ptr);
    al.free(four);
    al.free(two);
}

test "bump arena: a block emptied by a free goes home at once, the first block never" {
    var pool = try testPool(16 * 1024 * 1024, 4 * 1024 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    var a = Arena.init(&pool, &budget);
    defer a.release();
    const al = a.allocator();
    const small = try al.alloc(u8, 1000);
    try testing.expectEqual(@as(usize, 64 * 1024), a.held);
    const big = try al.alloc(u8, 300 * 1024); // 512 KiB class
    try testing.expectEqual(@as(usize, 64 * 1024 + 512 * 1024), a.held);
    try testing.expectEqual(@as(usize, 64 * 1024 + 512 * 1024), budget.held.load(.acquire));
    al.free(big);
    try testing.expectEqual(@as(usize, 64 * 1024), a.held);
    try testing.expectEqual(@as(usize, 64 * 1024), budget.held.load(.acquire));
    try testing.expectEqual(@as(usize, 64 * 1024), pool.snapshot().held_bytes);
    // The first block, emptied, stays: the request will want it again.
    al.free(small);
    try testing.expectEqual(@as(usize, 64 * 1024), a.held);
    // Freeing out of order: the older block's tail free does not return the
    // newer, still used block.
    const x = try al.alloc(u8, 100 * 1024); // 128 KiB
    const y = try al.alloc(u8, 20 * 1024); // fits the 128 KiB head
    try testing.expectEqual(@as(usize, 64 * 1024 + 128 * 1024), a.held);
    al.free(x); // not the tail of its block: nothing happens
    try testing.expectEqual(@as(usize, 64 * 1024 + 128 * 1024), a.held);
    // The hole `x` left is not tracked, so the block is not seen as empty when
    // `y` goes: it stays until the request ends. Only LIFO frees return a block.
    al.free(y);
    try testing.expectEqual(@as(usize, 64 * 1024 + 128 * 1024), a.held);
}

test "an ArrayList render of 1 MiB stays under the 4 MiB cap and holds a bounded peak" {
    // The bench's `renderFragment` shape: an ArrayList grown by appendSlice.
    var pool = try testPool(64 * 1024 * 1024, 4 * 1024 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    var a = Arena.init(&pool, &budget);
    defer a.release();
    const al = a.allocator();
    // Headers decode first: the first block.
    _ = try al.alloc(u8, 8 * 1024);
    var out: std.ArrayList(u8) = .empty;
    const row: [100]u8 = @splat('r');
    while (out.items.len < 1024 * 1024) try out.appendSlice(al, &row);
    try testing.expect(out.items.len >= 1024 * 1024);
    // The peak is the first block plus the block being left plus the block
    // being entered at the last growth step, never every class on the way.
    try testing.expect(a.peak <= 64 * 1024 + 1024 * 1024 + 2 * 1024 * 1024);
    try testing.expect(a.peak < 4 * 1024 * 1024);
    try testing.expectEqual(@as(?Reason, null), a.refusal);
    // Exactly the peak, so a change to growth or to the early return shows.
    try testing.expectEqual(@as(usize, 64 * 1024 + 1024 * 1024 + 2 * 1024 * 1024), a.peak);
    try testing.expectEqual(@as(usize, 64 * 1024 + 2 * 1024 * 1024), a.held);
}

test "reserve guarantees room for a bounded run of allocations" {
    var pool = try testPool(8 * 1024 * 1024, 1024 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    var a = Arena.init(&pool, &budget);
    defer a.release();
    const al = a.allocator();
    _ = try al.alloc(u8, 60 * 1024);
    try a.reserve(30 * 1024);
    // The head block could not hold 30 KiB more, so reserve took another.
    try testing.expectEqual(@as(usize, 64 * 1024 + 64 * 1024), a.held);
    const held_before = a.held;
    var used: usize = 0;
    while (used < 30 * 1024) : (used += 1024) _ = try al.alloc(u8, 1024);
    try testing.expectEqual(held_before, a.held);
}

test "release returns every block, and the arena is reusable" {
    var pool = try testPool(8 * 1024 * 1024, 1024 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    var a = Arena.init(&pool, &budget);
    const al = a.allocator();
    _ = try al.alloc(u8, 1);
    _ = try al.alloc(u8, 200 * 1024);
    _ = try al.alloc(u8, 500 * 1024);
    try testing.expect(pool.snapshot().held_bytes > 0);
    a.release();
    try testing.expectEqual(@as(usize, 0), pool.snapshot().held_bytes);
    try testing.expectEqual(@as(usize, 0), budget.held.load(.acquire));
    pool.assertAllHome();
    _ = try al.alloc(u8, 1);
    a.release();
}

test "reclaim: a history of small blocks does not pin the cap against large ones" {
    // The cap is 4 MiB. Fill it with 64 KiB blocks, free them, then ask for
    // 2 MiB blocks. Without reclaim the 64 KiB blocks stay resident and the
    // first 2 MiB block that does not fit is refused.
    const cap = 4 * 1024 * 1024;
    var pool = try testPool(cap, 2 * 1024 * 1024);
    defer pool.deinit();
    var small: [cap / (64 * 1024)]Block = undefined;
    for (&small) |*b| b.* = pool.take(0).?;
    try testing.expectEqual(@as(usize, cap), pool.snapshot().resident_bytes);
    for (small) |b| pool.give(b);
    // Resident is still the full cap: nothing was given back.
    try testing.expectEqual(@as(usize, cap), pool.snapshot().resident_bytes);

    const big1 = pool.take(5).?;
    const big2 = pool.take(5).?;
    try testing.expect(pool.snapshot().resident_high <= cap);
    try testing.expectEqual(@as(usize, cap), pool.snapshot().resident_bytes);
    // The third has nowhere to come from: two 2 MiB blocks are in use.
    try testing.expectEqual(@as(?Block, null), pool.take(5));
    const s = pool.snapshot();
    try testing.expectEqual(@as(u64, 64), s.classes[0].reclaimed);
    try testing.expectEqual(@as(u64, 2), s.classes[5].made);
    pool.give(big1);
    pool.give(big2);
}

test "reclaim: nothing is reclaimed in a steady state" {
    var pool = try testPool(4 * 1024 * 1024, 2 * 1024 * 1024);
    defer pool.deinit();
    for (0..100) |_| {
        const a = pool.take(0).?;
        const b = pool.take(3).?;
        pool.give(b);
        pool.give(a);
    }
    var total: u64 = 0;
    const s = pool.snapshot();
    for (s.classes[0..s.class_count]) |c| total += c.reclaimed;
    try testing.expectEqual(@as(u64, 0), total);
}

test "reclaim: a reclaimed block, used again, works" {
    var pool = try testPool(2 * 1024 * 1024, 2 * 1024 * 1024);
    defer pool.deinit();
    const a = pool.take(0).?;
    @memset(a.mem, 0xAA);
    pool.give(a);
    // The only way to a 2 MiB block under a 2 MiB cap is to reclaim the 64 KiB.
    const big = pool.take(5).?;
    try testing.expectEqual(@as(u64, 1), pool.snapshot().classes[0].reclaimed);
    @memset(big.mem, 0xBB);
    pool.give(big);
    // And back again.
    const b = pool.take(0).?;
    try testing.expectEqual(@as(u64, 2), pool.snapshot().classes[0].made);
    @memset(b.mem, 0xCC);
    for (b.mem) |v| try testing.expectEqual(@as(u8, 0xCC), v);
    pool.give(b);
}

/// Resident bytes of `mem` as this OS reports them. Linux: pages present, by
/// mincore. Darwin: the task's `phys_footprint`, because `FREE_REUSABLE`
/// lowers that and not the pages `mincore` or `ps` count.
fn residentProbe(mem: Page) usize {
    switch (comptime builtin.os.tag) {
        .linux => {
            const page = std.heap.pageSize();
            var vec: [1024]u8 = undefined;
            var total: usize = 0;
            var off: usize = 0;
            while (off < mem.len) {
                const n = @min(mem.len - off, vec.len * page);
                const part: []align(std.heap.page_size_min) u8 = @alignCast(mem[off..][0..n]);
                std.posix.mincore(part.ptr, n, &vec) catch |err| std.debug.panic("mincore: {s}", .{@errorName(err)});
                for (vec[0 .. n / page]) |v| total += @as(usize, v & 1) * page;
                off += n;
            }
            return total;
        },
        .macos => {
            var info: std.c.task_vm_info_data_t = undefined;
            var count: std.c.mach_msg_type_number_t = std.c.TASK.VM.INFO_COUNT;
            const rc = std.c.task_info(std.c.mach_task_self(), std.c.TASK.VM.INFO, @ptrCast(&info), &count);
            std.debug.assert(rc == 0);
            return info.phys_footprint;
        },
        else => @compileError("no resident probe for this OS"),
    }
}

test "reclaim: the pages really leave" {
    // The dormant path made to run: touch every block, give them back, force
    // one reclaim, and read the OS's own number before and after.
    const block_class: u8 = 5; // 2 MiB
    const cap = 16 * 1024 * 1024;
    var pool = try testPool(cap, 2 * 1024 * 1024);
    defer pool.deinit();
    var held: [cap / (2 * 1024 * 1024)]Block = undefined;
    for (&held) |*b| {
        b.* = pool.take(block_class).?;
        @memset(b.mem, 0x5A);
    }
    // Linux reads the first block alone; Darwin reads the whole task.
    const before = residentProbe(held[0].mem);
    for (held) |b| pool.give(b);
    // All eight 2 MiB blocks are free and resident and the cap is full, so a
    // 64 KiB block has to reclaim one, the lowest index of the largest class.
    const small = pool.take(0).?;
    pool.give(small);
    try testing.expectEqual(@as(u64, 1), pool.snapshot().classes[block_class].reclaimed);
    const after = residentProbe(held[0].mem);
    std.debug.print("reclaim of one 2 MiB block: {s} {d} KiB before, {d} KiB after\n", .{
        if (builtin.os.tag == .linux) "block resident" else "task footprint",
        before / 1024,
        after / 1024,
    });
    try testing.expect(before -| after >= 2 * 1024 * 1024 - 256 * 1024);
}

test "the resident counter holds under concurrent takers on several threads" {
    const cap = 8 * 1024 * 1024;
    var pool = try testPool(cap, 2 * 1024 * 1024);
    defer pool.deinit();
    const Worker = struct {
        fn run(p: *RequestPool, seed: u64, violations: *std.atomic.Value(usize)) void {
            var rng = std.Random.DefaultPrng.init(seed);
            var held: [4]?Block = @splat(null);
            for (0..4000) |_| {
                const slot = rng.random().uintLessThan(usize, held.len);
                if (held[slot]) |b| {
                    p.give(b);
                    held[slot] = null;
                } else {
                    const class = rng.random().uintLessThan(u8, 6);
                    held[slot] = p.take(class);
                }
                if (p.resident_bytes.load(.acquire) > cap) _ = violations.fetchAdd(1, .monotonic);
            }
            for (held) |maybe| if (maybe) |b| p.give(b);
        }
    };
    var violations: std.atomic.Value(usize) = .init(0);
    var threads: [6]std.Thread = undefined;
    for (&threads, 0..) |*t, i| t.* = try std.Thread.spawn(.{}, Worker.run, .{ &pool, @as(u64, i) + 1, &violations });
    for (threads) |t| t.join();
    try testing.expectEqual(@as(usize, 0), violations.load(.monotonic));
    const s = pool.snapshot();
    try testing.expect(s.resident_high <= cap);
    try testing.expectEqual(@as(usize, 0), s.held_bytes);
    // The workload did force reclaims, or the run proved little.
    var reclaimed: u64 = 0;
    for (s.classes[0..s.class_count]) |c| reclaimed += c.reclaimed;
    try testing.expect(reclaimed > 0);
}

test "deinit asserts every block is home" {
    // Not runnable as a failing test (the assert is a panic); this checks the
    // positive side: a pool whose blocks all came back deinits cleanly, and
    // `assertAllHome` reads the same state a leak would break.
    var pool = try testPool(2 * 1024 * 1024, 1024 * 1024);
    const b = pool.take(3).?;
    try testing.expectEqual(@as(usize, 512 * 1024), pool.snapshot().held_bytes);
    pool.give(b);
    pool.assertAllHome();
    pool.deinit();
}

test "a take never fails while a block of its class is free and the cap has room" {
    // Eight blocks of a class and six threads holding one each at most: at the
    // moment any thread takes, at most five are held, so a refusal here would
    // be spurious (the cap is the whole class). The threads take and give in
    // a tight loop to make a free block change state between a taker's claims.
    var pool = try testPool(8 * 64 * 1024, 64 * 1024);
    defer pool.deinit();
    const Worker = struct {
        fn run(p: *RequestPool, refusals: *std.atomic.Value(usize)) void {
            for (0..20_000) |_| {
                if (p.take(0)) |b| {
                    p.give(b);
                } else {
                    _ = refusals.fetchAdd(1, .monotonic);
                }
            }
        }
    };
    var refusals: std.atomic.Value(usize) = .init(0);
    var threads: [6]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Worker.run, .{ &pool, &refusals });
    for (threads) |t| t.join();
    try testing.expectEqual(@as(usize, 0), refusals.load(.monotonic));
}

test "an alignment above a page takes a block sized for the padding, and a block it cannot get is a request cap" {
    var pool = try testPool(16 * 1024 * 1024, 1024 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    var a = Arena.init(&pool, &budget);
    defer a.release();
    const al = a.allocator();
    // 64 KiB alignment: the block is a page-aligned class block, and the
    // worst padding is 64 KiB - 1, so a 100-byte allocation needs the 128 KiB
    // class. It comes back aligned, usable, and refuses nothing.
    const p = try al.alignedAlloc(u8, .fromByteUnits(64 * 1024), 100);
    try testing.expectEqual(@as(usize, 0), @intFromPtr(p.ptr) % (64 * 1024));
    @memset(p, 7);
    try testing.expectEqual(@as(?Reason, null), a.refusal);
    try testing.expectEqual(@as(usize, 128 * 1024), a.held);
    // Freed at the tail, the block is empty again and goes home when it is
    // not the first (this one is the first, so it stays).
    al.free(p);
    try testing.expectEqual(@as(usize, 128 * 1024), a.held);
    // A second one, in a request that already holds a first block: the new
    // block is not the first, and goes home at once on the tail free. A fresh
    // arena whose first block (64 KiB) is nearly full, so no address in it
    // can hold 100 KiB: otherwise, when the mapping happens to put a 256 KiB
    // boundary inside a held block with room after it, the arena rightly
    // bumps there, and the result depends on where the OS placed the mapping
    // (t-3568).
    var c = Arena.init(&pool, &budget);
    defer c.release();
    const cl = c.allocator();
    const first = try cl.alloc(u8, 60 * 1024);
    try testing.expectEqual(@as(usize, 64 * 1024), c.held);
    const q = try cl.alignedAlloc(u8, .fromByteUnits(256 * 1024), 100 * 1024);
    try testing.expectEqual(@as(usize, 0), @intFromPtr(q.ptr) % (256 * 1024));
    try testing.expectEqual(@as(?Reason, null), c.refusal);
    try testing.expectEqual(@as(usize, 64 * 1024 + 512 * 1024), c.held);
    cl.free(q);
    try testing.expectEqual(@as(usize, 64 * 1024), c.held);
    cl.free(first);
    // An alignment past what the largest class can hold is not a server cap
    // and not a memory error: the request would be over its own cap. (On an
    // arena with no block yet, so no address in a held block can happen to
    // be aligned.)
    var b = Arena.init(&pool, &budget);
    defer b.release();
    try testing.expectError(error.OutOfMemory, b.allocator().alignedAlloc(u8, .fromByteUnits(2 * 1024 * 1024), 100));
    try testing.expectEqual(Reason.request_cap, b.refusal.?);
    try testing.expectEqual(@as(u64, 1), pool.snapshot().refused[@backingInt(Reason.request_cap)]);
    try testing.expectEqual(@as(u64, 0), pool.snapshot().refused[@backingInt(Reason.server_cap)]);
    // And a length that overflows with the padding.
    b.refusal = null;
    try testing.expectEqual(@as(?[*]u8, null), Arena.vAlloc(&b, std.math.maxInt(usize) - 10, .fromByteUnits(64 * 1024), 0));
    try testing.expectEqual(Reason.request_cap, b.refusal.?);
}

fn residentFromBits(pool: *const RequestPool) usize {
    var total: usize = 0;
    for (pool.classes[0..pool.class_count]) |c| total += c.resident.count() * c.size;
    return total;
}

test "retain limit: a burst above it leaves free resident bytes at the limit" {
    const retain = 2 * 1024 * 1024;
    var pool = try testPoolRetain(32 * 1024 * 1024, 4 * 1024 * 1024, retain);
    defer pool.deinit();
    // A burst: 8 blocks of 1 MiB in use at once (8 MiB), then all returned.
    var held: [8]Block = undefined;
    for (&held) |*b| b.* = pool.take(4).?;
    try testing.expectEqual(@as(usize, 8 * 1024 * 1024), pool.snapshot().resident_bytes);
    for (held) |b| pool.give(b);
    const s = pool.snapshot();
    // Held is zero, so free resident equals resident: exactly the limit.
    try testing.expectEqual(@as(usize, 0), s.held_bytes);
    try testing.expectEqual(@as(usize, retain), s.resident_bytes);
    try testing.expectEqual(@as(usize, retain), residentFromBits(&pool));
    try testing.expectEqual(@as(u64, 6), s.classes[4].retired);
    // The next take reuses a kept block (the lowest index) and makes nothing.
    const again = pool.take(4).?;
    try testing.expectEqual(@as(u32, 0), again.index);
    pool.give(again);
    try testing.expectEqual(@as(u64, 1), pool.snapshot().classes[4].reused);
    try testing.expectEqual(@as(u64, 8), pool.snapshot().classes[4].made);
}

test "retain limit: blocks below it stay resident and nothing is retired" {
    var pool = try testPoolRetain(32 * 1024 * 1024, 4 * 1024 * 1024, 4 * 1024 * 1024);
    defer pool.deinit();
    for (0..50) |_| {
        const a = pool.take(2).?;
        const b = pool.take(4).?;
        pool.give(a);
        pool.give(b);
    }
    const s = pool.snapshot();
    var retired: u64 = 0;
    for (s.classes[0..s.class_count]) |c| retired += c.retired;
    try testing.expectEqual(@as(u64, 0), retired);
    try testing.expectEqual(@as(usize, 256 * 1024 + 1024 * 1024), s.resident_bytes);
    // A retain limit of zero retires every return.
    var none = try testPoolRetain(8 * 1024 * 1024, 1024 * 1024, 0);
    defer none.deinit();
    for (0..5) |_| none.give(none.take(0).?);
    try testing.expectEqual(@as(u64, 5), none.snapshot().classes[0].retired);
    try testing.expectEqual(@as(usize, 0), none.snapshot().resident_bytes);
}

test "retain limit: held bytes do not count against the limit" {
    // 4 blocks held, limit 1 MiB: the free resident bytes are what is
    // compared, so returning one block while three are held keeps it.
    var pool = try testPoolRetain(16 * 1024 * 1024, 1024 * 1024, 1024 * 1024);
    defer pool.deinit();
    var held: [4]Block = undefined;
    for (&held) |*b| b.* = pool.take(4).?;
    pool.give(held[0]);
    try testing.expectEqual(@as(u64, 0), pool.snapshot().classes[4].retired);
    try testing.expectEqual(@as(usize, 4 * 1024 * 1024), pool.snapshot().resident_bytes);
    // A second return makes free resident 2 MiB, over the limit.
    pool.give(held[1]);
    try testing.expectEqual(@as(u64, 1), pool.snapshot().classes[4].retired);
    pool.give(held[2]);
    pool.give(held[3]);
    try testing.expectEqual(@as(usize, 1024 * 1024), pool.snapshot().resident_bytes);
}

test "retain limit: the counter and the bitmap agree after concurrent returns and takes" {
    const cap = 16 * 1024 * 1024;
    var pool = try testPoolRetain(cap, 2 * 1024 * 1024, 1024 * 1024);
    defer pool.deinit();
    const Worker = struct {
        fn run(p: *RequestPool, seed: u64) void {
            var rng = std.Random.DefaultPrng.init(seed);
            var held: [3]?Block = @splat(null);
            for (0..3000) |_| {
                const slot = rng.random().uintLessThan(usize, held.len);
                if (held[slot]) |b| {
                    p.give(b);
                    held[slot] = null;
                } else {
                    held[slot] = p.take(rng.random().uintLessThan(u8, 6));
                }
            }
            for (held) |maybe| if (maybe) |b| p.give(b);
        }
    };
    var threads: [6]std.Thread = undefined;
    for (&threads, 0..) |*t, i| t.* = try std.Thread.spawn(.{}, Worker.run, .{ &pool, @as(u64, i) + 100 });
    for (threads) |t| t.join();
    const s = pool.snapshot();
    try testing.expectEqual(@as(usize, 0), s.held_bytes);
    try testing.expectEqual(residentFromBits(&pool), s.resident_bytes);
    try testing.expect(s.resident_high <= cap);
    var retired: u64 = 0;
    for (s.classes[0..s.class_count]) |c| retired += c.retired;
    try testing.expect(retired > 0);
    // The keep decision is one atomic step, so the pool never ends over the
    // limit (with 6 threads holding at most 3 blocks each, and all returned).
    try testing.expect(s.resident_bytes <= 1024 * 1024);
    try testing.expectEqual(s.resident_bytes, s.free_resident_bytes);
}

test "retain limit: a retired block's pages leave" {
    const class: u8 = 5; // 2 MiB
    var pool = try testPoolRetain(16 * 1024 * 1024, 2 * 1024 * 1024, 0);
    defer pool.deinit();
    const b = pool.take(class).?;
    @memset(b.mem, 0x5A);
    const before = residentProbe(b.mem);
    pool.give(b);
    try testing.expectEqual(@as(u64, 1), pool.snapshot().classes[class].retired);
    const after = residentProbe(b.mem);
    std.debug.print("retire of one 2 MiB block: {s} {d} KiB before, {d} KiB after\n", .{
        if (builtin.os.tag == .linux) "block resident" else "task footprint",
        before / 1024,
        after / 1024,
    });
    try testing.expect(before -| after >= 2 * 1024 * 1024 - 256 * 1024);
    // And the block works again.
    const again = pool.take(class).?;
    @memset(again.mem, 0xA5);
    pool.give(again);
}

test "touchedBytes sees the pool's pages where the OS can say" {
    var pool = try testPoolRetain(8 * 1024 * 1024, 1024 * 1024, 8 * 1024 * 1024);
    defer pool.deinit();
    if (comptime builtin.os.tag != .linux) {
        try testing.expectEqual(@as(?usize, null), try pool.touchedBytes());
        return;
    }
    try testing.expectEqual(@as(?usize, 0), try pool.touchedBytes());
    const b = pool.take(3).?;
    @memset(b.mem, 1);
    // The pool's own bookkeeping counts 512 KiB; the pages touched are at
    // least that (more under huge pages, which the pool asks not to get).
    try testing.expect((try pool.touchedBytes()).? >= 512 * 1024);
    pool.give(b);
}

test "an aligned allocation that ends exactly at a class boundary fits that class, and one byte more takes the next" {
    var pool = try testPool(64 * 1024 * 1024, 4 * 1024 * 1024);
    defer pool.deinit();
    var budget = ConnBudget.init(1 << 30);
    const page = std.heap.page_size_min;
    // The worst start of the first allocation in a fresh block: the header
    // rounded up to the alignment up to a page; the alignment itself above it.
    for ([_]usize{ 1, 8, 64, 256, 4096, page, 2 * page, 64 * 1024, 128 * 1024 }) |align_bytes| {
        const alignment: std.mem.Alignment = .fromByteUnits(align_bytes);
        const start_max = if (align_bytes <= page) std.mem.alignForward(usize, header_bytes, align_bytes) else align_bytes;
        for (0..pool.class_count) |class| {
            const size = min_block_bytes << @intCast(class);
            if (size <= start_max) continue;
            const exact = size - start_max;
            // Exactly the class: fits, aligned, and charges the class size.
            var a = Arena.init(&pool, &budget);
            const p = a.allocator().rawAlloc(exact, alignment, 0) orelse return error.TestUnexpectedResult;
            try testing.expectEqual(@as(usize, 0), @intFromPtr(p) % align_bytes);
            @memset(p[0..exact], 0x77);
            try testing.expectEqual(size, a.held);
            try testing.expectEqual(@as(?Reason, null), a.refusal);
            a.release();
            // One byte more: the next class, or the request cap at the top.
            var b = Arena.init(&pool, &budget);
            if (b.allocator().rawAlloc(exact + 1, alignment, 0)) |q| {
                try testing.expectEqual(@as(usize, 0), @intFromPtr(q) % align_bytes);
                try testing.expectEqual(size * 2, b.held);
            } else {
                try testing.expectEqual(pool.arena_bytes, size);
                try testing.expectEqual(Reason.request_cap, b.refusal.?);
            }
            b.release();
        }
    }
}

test "retain limit: returners that decide together leave free resident bytes at or under the limit" {
    // Eight threads each hold one 64 KiB block. At a barrier they all return
    // it at once. The limit keeps two blocks. A decision that reads the held
    // and resident counters separately lets several returners all see room and
    // keep their block; a single linearized decision cannot. Many rounds, since
    // the interleaving is the point.
    const n = 8;
    const retain = 2 * 64 * 1024;
    var pool = try testPoolRetain(n * 64 * 1024, 64 * 1024, retain);
    defer pool.deinit();
    const Round = struct {
        var arrived: std.atomic.Value(usize) = .init(0);
        var go: std.atomic.Value(usize) = .init(0);
        var done: std.atomic.Value(usize) = .init(0);
    };
    Round.arrived.store(0, .release);
    Round.go.store(0, .release);
    Round.done.store(0, .release);
    const rounds = 3000;
    const Worker = struct {
        fn run(p: *RequestPool) void {
            for (0..rounds) |r| {
                const b = p.take(0).?;
                // Barrier 1: everyone holds a block.
                _ = Round.arrived.fetchAdd(1, .acq_rel);
                while (Round.go.load(.acquire) <= r * 2) std.atomic.spinLoopHint();
                p.give(b);
                // Barrier 2: everyone has returned.
                _ = Round.done.fetchAdd(1, .acq_rel);
                while (Round.go.load(.acquire) <= r * 2 + 1) std.atomic.spinLoopHint();
            }
        }
    };
    var threads: [n]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Worker.run, .{&pool});
    var worst_free: usize = 0;
    var best_free: usize = std.math.maxInt(usize);
    for (0..rounds) |r| {
        while (Round.arrived.load(.acquire) < n * (r + 1)) std.atomic.spinLoopHint();
        _ = Round.go.fetchAdd(1, .release); // release the returners
        while (Round.done.load(.acquire) < n * (r + 1)) std.atomic.spinLoopHint();
        const s = pool.snapshot();
        worst_free = @max(worst_free, s.resident_bytes -| s.held_bytes);
        best_free = @min(best_free, s.resident_bytes -| s.held_bytes);
        // The counter the decision uses is the free resident bytes.
        try testing.expectEqual(s.resident_bytes - s.held_bytes, s.free_resident_bytes);
        _ = Round.go.fetchAdd(1, .release); // release them into the next round
    }
    for (threads) |t| t.join();
    // Exactly the limit, every round: not over it, and not under it either
    // (a returner that finds room keeps its block).
    try testing.expectEqual(@as(usize, retain), worst_free);
    try testing.expectEqual(@as(usize, retain), best_free);
    try testing.expectEqual(residentFromBits(&pool), pool.snapshot().resident_bytes);
}

test "touchedBytes scans only the blocks that were ever made resident" {
    // A large reservation (every class holds the whole 1 GiB cap) with a few
    // blocks used: the scan covers those, not the reservation. Arithmetic, so
    // it runs on every OS; the huge page size is set, since Darwin has none to
    // read and an unknown size scans whole mappings (tested below).
    var pool = try testPoolRetain(1024 * 1024 * 1024, 4 * 1024 * 1024, 1024 * 1024 * 1024);
    defer pool.deinit();
    pool.huge_page_bytes = 2 * 1024 * 1024;
    try testing.expectEqual(@as(usize, 0), pool.scannedBytes());
    const a = pool.take(0).?; // block 0 of the 64 KiB class
    const b = pool.take(0).?; // block 1
    const c = pool.take(6).?; // block 0 of the 4 MiB class
    const used = 2 * 64 * 1024 + 4 * 1024 * 1024;
    const scanned = pool.scannedBytes();
    // At least the made blocks, and at most one huge page more per class (the
    // mapping base decides how much of the last huge page there is).
    try testing.expect(scanned >= used);
    try testing.expect(scanned <= used + 2 * pool.huge_page_bytes);
    // A block that goes back, even one retired, stays inside the prefix: its
    // pages may still be there on an OS that keeps them.
    pool.give(a);
    pool.give(b);
    pool.give(c);
    try testing.expectEqual(scanned, pool.scannedBytes());
    // And it is a sliver of what the pool reserves.
    var reserved: usize = 0;
    for (pool.classes[0..pool.class_count]) |cls| reserved += cls.storage.len;
    try testing.expect(scanned * 100 < reserved);
    _ = try pool.touchedBytes();
}

test "touchedBytes reports a failing mincore as an error, not as null" {
    // Linux only: elsewhere null means "not available", and there is nothing
    // to fail. The mapping of the one class in use is unmapped under the scan,
    // so `mincore` answers ENOMEM, which must reach the caller.
    if (comptime builtin.os.tag != .linux) return error.SkipZigTest;
    var pool = try testPoolRetain(4 * 1024 * 1024, 1024 * 1024, 4 * 1024 * 1024);
    defer pool.deinit();
    pool.give(pool.take(0).?);
    const mapped = pool.classes[0].storage;
    RequestPool.rawUnmap(mapped);
    try testing.expectError(error.OutOfMemory, pool.touchedBytes());
    // Put a mapping back so `deinit` unmaps something real.
    const raw = std.heap.page_allocator.rawAlloc(mapped.len, page_align, 0) orelse return error.OutOfMemory;
    pool.classes[0].storage = @alignCast(raw[0..mapped.len]);
}

test "the scan end is the end of the huge page that holds the last made block, by absolute address" {
    const mib = 1024 * 1024;
    const huge = 2 * mib;
    // A 2 MiB aligned base: 512 KiB made ends inside the first huge page.
    try testing.expectEqual(huge, scanEndFor(0x4000_0000, 512 * 1024, 8 * mib, huge));
    // A base that is only page aligned (16 KiB past a huge page boundary): the
    // huge page that holds the last made byte ends 16 KiB sooner, at an
    // absolute boundary. Rounding the OFFSET up to 2 MiB would stop 16 KiB
    // short of the page's end.
    const offset_base = 0x4000_0000 + 16 * 1024;
    try testing.expectEqual(huge - 16 * 1024, scanEndFor(offset_base, 512 * 1024, 8 * mib, huge));
    // Made bytes reaching just into the next huge page take that whole page.
    try testing.expectEqual(2 * huge - 16 * 1024, scanEndFor(offset_base, huge - 16 * 1024 + 1, 8 * mib, huge));
    // Always an absolute boundary, or the end of the mapping, and never less
    // than the made bytes, for a spread of bases and sizes.
    for ([_]usize{ 0, 0x4000, 0x10000, 0x1f_c000, 0x20_0000 + 0x8000 }) |off| {
        for ([_]usize{ 1, 0x4000, 512 * 1024, huge - 1, huge, huge + 1, 5 * mib }) |made| {
            const base = 0x4000_0000 + off;
            const end = scanEndFor(base, made, 16 * mib, huge);
            try testing.expect(end >= made);
            try testing.expect(end == 16 * mib or (base + end) % huge == 0);
            try testing.expect(end < made + huge);
        }
    }
    // Capped at the mapping.
    try testing.expectEqual(@as(usize, mib), scanEndFor(0x4000_0000, 64 * 1024, mib, huge));
    // Nothing made, nothing scanned.
    try testing.expectEqual(@as(usize, 0), scanEndFor(0x4000_0000, 0, 8 * mib, huge));
    // Unknown huge page size: the whole mapping once anything is made.
    try testing.expectEqual(8 * mib, scanEndFor(0x4000_0000, 1, 8 * mib, 0));
}

test "an unknown huge page size scans the whole mapping of a used class" {
    var pool = try testPoolRetain(4 * 1024 * 1024, 1024 * 1024, 4 * 1024 * 1024);
    defer pool.deinit();
    pool.huge_page_bytes = 0;
    try testing.expectEqual(@as(usize, 0), pool.scannedBytes());
    const a = pool.take(0).?;
    try testing.expectEqual(@as(usize, 4 * 1024 * 1024), pool.scannedBytes());
    pool.give(a);
}

test "the huge page size is read as a power of two of at least a page, or not at all" {
    try testing.expectEqual(@as(?usize, 2 * 1024 * 1024), parseHugePageSize("2097152\n"));
    try testing.expectEqual(@as(?usize, 32 * 1024 * 1024), parseHugePageSize(" 33554432 \n"));
    try testing.expectEqual(@as(?usize, 512 * 1024 * 1024), parseHugePageSize("536870912"));
    try testing.expectEqual(@as(?usize, null), parseHugePageSize(""));
    try testing.expectEqual(@as(?usize, null), parseHugePageSize("0\n"));
    try testing.expectEqual(@as(?usize, null), parseHugePageSize("3000000\n"));
    try testing.expectEqual(@as(?usize, null), parseHugePageSize("2M\n"));
    try testing.expectEqual(@as(?usize, null), parseHugePageSize("-2097152"));
    var below: [24]u8 = undefined;
    const half_page = try std.fmt.bufPrint(&below, "{d}", .{std.heap.page_size_min / 2});
    try testing.expectEqual(@as(?usize, null), parseHugePageSize(half_page));
    // On Linux the real file is read at init; the value is one of the above
    // shapes or the pool scans whole mappings (0).
    var pool = try testPoolRetain(4 * 1024 * 1024, 1024 * 1024, 4 * 1024 * 1024);
    defer pool.deinit();
    try testing.expect(pool.huge_page_bytes == 0 or std.math.isPowerOfTwo(pool.huge_page_bytes));
    if (builtin.os.tag != .linux) try testing.expectEqual(@as(usize, 0), pool.huge_page_bytes);
}
