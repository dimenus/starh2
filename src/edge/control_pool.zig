//! Ordinary vs terminal/ack control-frame occupancy + CONTROL_BEFORE_DATA fairness.
//! Connection actor owns one pool; write completion releases held entries/bytes.
//!
//! Control frames are bounded because a peer generates them. A flood of PING or
//! SETTINGS produces a matching flood of acks, so an unbounded control queue is
//! a memory target that costs the peer almost nothing.
//!
//! The pool is split rather than merely capped. Ordinary controls may use only
//! `max - reserve`; terminal frames may use the whole pool. The reserved slice
//! is what guarantees a connection can always emit the GOAWAY or RST_STREAM
//! that ENDS the flood. Without it, the frame that stops the attack is the one
//! frame the attack has already made impossible.
//!
//! Occupancy is held from the moment a frame is queued until its write
//! completes, and not merely until it is emitted. Bytes waiting in the write
//! queue are real memory, so releasing them at emit time would report capacity
//! the connection does not have.
const std = @import("std");
const limits_mod = @import("../core/limits.zig");

/// The starvation bound. Controls drain before DATA, so a peer that keeps a
/// control frame always available would otherwise stop every response body on
/// the connection while every individual frame is answered correctly. After
/// this many controls the scheduler forces one DATA quantum, which converts
/// unbounded starvation into a bounded delay.
pub const CONTROL_BEFORE_DATA: usize = 64;
pub const TERMINAL_CONTROL_RESERVE_BYTES = limits_mod.TERMINAL_CONTROL_RESERVE_BYTES;
pub const TERMINAL_CONTROL_RESERVE_ENTRIES = limits_mod.TERMINAL_CONTROL_RESERVE_ENTRIES;

pub const ControlPool = struct {
    max_bytes: usize,
    max_entries: usize,
    bytes_held: std.atomic.Value(usize) = .init(0),
    entries_held: std.atomic.Value(usize) = .init(0),
    controls_since_data: usize = 0,

    pub fn init(max_bytes: usize, max_entries: usize) ControlPool {
        return .{ .max_bytes = max_bytes, .max_entries = max_entries };
    }

    pub fn ordinaryCaps(self: *const ControlPool) struct { bytes: usize, entries: usize } {
        if (self.max_bytes <= TERMINAL_CONTROL_RESERVE_BYTES or self.max_entries <= TERMINAL_CONTROL_RESERVE_ENTRIES) {
            return .{ .bytes = 0, .entries = 0 };
        }
        return .{
            .bytes = self.max_bytes - TERMINAL_CONTROL_RESERVE_BYTES,
            .entries = self.max_entries - TERMINAL_CONTROL_RESERVE_ENTRIES,
        };
    }

    pub fn tryReserveOrdinary(self: *ControlPool, n: usize) bool {
        const caps = self.ordinaryCaps();
        if (caps.bytes == 0 or caps.entries == 0) return false;
        while (true) {
            const cur_e = self.entries_held.load(.acquire);
            const cur_b = self.bytes_held.load(.acquire);
            if (cur_e >= caps.entries) return false;
            if (cur_b > caps.bytes or caps.bytes - cur_b < n) return false;
            if (self.entries_held.cmpxchgWeak(cur_e, cur_e + 1, .acq_rel, .acquire) != null) continue;
            if (self.bytes_held.cmpxchgWeak(cur_b, cur_b + n, .acq_rel, .acquire) == null) return true;
            _ = self.entries_held.fetchSub(1, .acq_rel);
        }
    }

    /// Terminal / required ACK / SETTINGS ACK — may consume the reserved slice.
    pub fn tryReserveTerminal(self: *ControlPool, n: usize) bool {
        while (true) {
            const cur_e = self.entries_held.load(.acquire);
            const cur_b = self.bytes_held.load(.acquire);
            if (cur_e >= self.max_entries) return false;
            if (cur_b > self.max_bytes or self.max_bytes - cur_b < n) return false;
            if (self.entries_held.cmpxchgWeak(cur_e, cur_e + 1, .acq_rel, .acquire) != null) continue;
            if (self.bytes_held.cmpxchgWeak(cur_b, cur_b + n, .acq_rel, .acquire) == null) return true;
            _ = self.entries_held.fetchSub(1, .acq_rel);
        }
    }

    pub fn release(self: *ControlPool, n: usize, entries: usize) void {
        if (entries != 0) {
            const prev = self.entries_held.fetchSub(entries, .acq_rel);
            std.debug.assert(prev >= entries);
        }
        if (n != 0) {
            const prev = self.bytes_held.fetchSub(n, .acq_rel);
            std.debug.assert(prev >= n);
        }
    }

    pub fn noteControl(self: *ControlPool) void {
        self.controls_since_data += 1;
    }

    pub fn noteDataQuantum(self: *ControlPool) void {
        self.controls_since_data = 0;
    }

    pub fn shouldForceData(self: *const ControlPool) bool {
        return self.controls_since_data >= CONTROL_BEFORE_DATA;
    }

};

test "ordinary pool leaves terminal reserve" {
    var pool = ControlPool.init(64 * 1024, 256);
    const caps = pool.ordinaryCaps();
    try std.testing.expectEqual(@as(usize, 64 * 1024 - TERMINAL_CONTROL_RESERVE_BYTES), caps.bytes);
    try std.testing.expectEqual(@as(usize, 256 - TERMINAL_CONTROL_RESERVE_ENTRIES), caps.entries);

    var i: usize = 0;
    while (i < caps.entries) : (i += 1) {
        try std.testing.expect(pool.tryReserveOrdinary(1));
    }
    try std.testing.expect(!pool.tryReserveOrdinary(1));
    // Terminal/ACK still fits in reserved slice.
    try std.testing.expect(pool.tryReserveTerminal(1));
    try std.testing.expectEqual(caps.entries + 1, pool.entries_held.load(.acquire));
}

// The fairness gate. It replaced a `simulateFairnessGaps` helper that ignored
// its argument and returned an empty slice — a checker that reports "clean"
// while measuring nothing. This test drives the real counters instead, and
// asserts both directions: DATA is forced often enough, and no gap ever
// exceeds the bound.
test "10k nonterminal controls force DATA at <=64 gaps" {
    var pool = ControlPool.init(64 * 1024, 256);
    var max_gap: usize = 0;
    var gap: usize = 0;
    var data_quanta: usize = 0;
    var i: usize = 0;
    while (i < 10_000) : (i += 1) {
        pool.noteControl();
        gap += 1;
        if (pool.shouldForceData()) {
            if (gap > max_gap) max_gap = gap;
            pool.noteDataQuantum();
            data_quanta += 1;
            gap = 0;
        }
    }
    try std.testing.expect(data_quanta >= 10_000 / CONTROL_BEFORE_DATA);
    try std.testing.expect(max_gap <= CONTROL_BEFORE_DATA);
}

test "exact occupancy release asserts once" {
    var pool = ControlPool.init(64 * 1024, 256);
    try std.testing.expect(pool.tryReserveOrdinary(100));
    try std.testing.expectEqual(@as(usize, 100), pool.bytes_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), pool.entries_held.load(.acquire));
    pool.release(100, 1);
    try std.testing.expectEqual(@as(usize, 0), pool.bytes_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool.entries_held.load(.acquire));
}

test "release of N entries matches N reserves" {
    // A TLS drain turn may copy several HEADERS into one plaintext. Occupancy
    // stays held until that write completes, so the completion must release
    // every reserved entry, not a bool's worth. Releasing 1 after 3 reserves
    // would leak two entries for the life of the connection.
    var pool = ControlPool.init(64 * 1024, 256);
    try std.testing.expect(pool.tryReserveOrdinary(10));
    try std.testing.expect(pool.tryReserveOrdinary(20));
    try std.testing.expect(pool.tryReserveOrdinary(30));
    try std.testing.expectEqual(@as(usize, 3), pool.entries_held.load(.acquire));
    pool.release(60, 3);
    try std.testing.expectEqual(@as(usize, 0), pool.bytes_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool.entries_held.load(.acquire));
}

test "releasing one entry after three reserves leaves occupancy that starves ordinary" {
    // The bool-shaped WireChunk. The completion looks successful and the
    // connection stays up, while two slots never return. A later HEADERS then
    // hits PoolFull on a connection that is not actually full.
    var pool = ControlPool.init(64 * 1024, 256);
    const caps = pool.ordinaryCaps();
    try std.testing.expect(pool.tryReserveOrdinary(10));
    try std.testing.expect(pool.tryReserveOrdinary(20));
    try std.testing.expect(pool.tryReserveOrdinary(30));
    pool.release(60, 1);
    try std.testing.expectEqual(@as(usize, 2), pool.entries_held.load(.acquire));
    var i: usize = 2;
    while (i < caps.entries) : (i += 1) {
        try std.testing.expect(pool.tryReserveOrdinary(1));
    }
    try std.testing.expect(!pool.tryReserveOrdinary(1));
}

test "byte-only release does not drop an entry" {
    var pool = ControlPool.init(64 * 1024, 256);
    try std.testing.expect(pool.tryReserveOrdinary(100));
    pool.release(100, 0);
    try std.testing.expectEqual(@as(usize, 0), pool.bytes_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), pool.entries_held.load(.acquire));
    pool.release(0, 1);
    try std.testing.expectEqual(@as(usize, 0), pool.entries_held.load(.acquire));
}
