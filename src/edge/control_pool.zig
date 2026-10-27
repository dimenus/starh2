//! Ordinary vs terminal/ack control-frame occupancy + CONTROL_BEFORE_DATA fairness.
//! Connection actor owns one pool; write completion releases held entries/bytes.
//!
//! Control frames are bounded because a peer generates them. A flood of PING or
//! SETTINGS produces a matching flood of acks, so an unbounded control queue is
//! a memory target that costs the peer almost nothing.
//!
//! The pool is two shares in one counter. Ordinary controls may use only the
//! ordinary share (`Limits.control_*_per_connection`). Terminal frames
//! (GOAWAY, RST_STREAM) may use the whole pool: the ordinary share plus a
//! terminal share of their own, `Limits.terminalFrames` entries and
//! `Limits.terminalBytes` bytes. Ordinary frames never hold more than the
//! ordinary share, so the terminal share is always free for the frames that
//! END a flood. Without it, the frame that stops the attack is the one frame
//! the attack has already made impossible.
//!
//! The terminal share is T = M + 3 because a well-behaved peer can have at
//! most M unwritten RST_STREAMs (one per stream it still counts as open) and
//! three GOAWAYs outstanding; see `Limits.terminalFrames`. The terminal ring
//! (`FairScheduler.terminal_q`) and the terminal frame store
//! (`Connection.terminal_store`) hold the same T.
//!
//! Occupancy is held from the moment a frame is queued until its write
//! completes, and not merely until it is emitted. Bytes waiting in the write
//! queue are real memory, so releasing them at emit time would report capacity
//! the connection does not have.
const std = @import("std");

/// The starvation bound. Controls drain before DATA, so a peer that keeps a
/// control frame always available would otherwise stop every response body on
/// the connection while every individual frame is answered correctly. After
/// this many controls the scheduler forces one DATA quantum, which converts
/// unbounded starvation into a bounded delay.
pub const CONTROL_BEFORE_DATA: usize = 64;
pub const ControlPool = struct {
    /// Ordinary share plus terminal share.
    max_bytes: usize,
    max_entries: usize,
    terminal_bytes: usize,
    terminal_entries: usize,
    bytes_held: std.atomic.Value(usize) = .init(0),
    entries_held: std.atomic.Value(usize) = .init(0),
    controls_since_data: usize = 0,

    /// The ordinary share and the terminal share are separate arguments, so
    /// no caller can size the pool without saying how much is terminal.
    pub fn init(ordinary_bytes: usize, ordinary_entries: usize, terminal_bytes: usize, terminal_entries: usize) ControlPool {
        return .{
            .max_bytes = ordinary_bytes + terminal_bytes,
            .max_entries = ordinary_entries + terminal_entries,
            .terminal_bytes = terminal_bytes,
            .terminal_entries = terminal_entries,
        };
    }

    pub fn ordinaryCaps(self: *const ControlPool) struct { bytes: usize, entries: usize } {
        return .{
            .bytes = self.max_bytes - self.terminal_bytes,
            .entries = self.max_entries - self.terminal_entries,
        };
    }

    /// Entries and bytes not held by anything. Terminal admission reads it.
    pub fn freeEntries(self: *const ControlPool) usize {
        return self.max_entries - @min(self.max_entries, self.entries_held.load(.acquire));
    }

    pub fn freeBytes(self: *const ControlPool) usize {
        return self.max_bytes - @min(self.max_bytes, self.bytes_held.load(.acquire));
    }

    pub fn tryReserveOrdinary(self: *ControlPool, n: usize) bool {
        const caps = self.ordinaryCaps();
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

    /// True when one more ordinary entry fits. Bytes are not checked: the
    /// caller does not know its frame's size yet.
    pub fn hasOrdinaryEntry(self: *const ControlPool) bool {
        return self.entries_held.load(.acquire) < self.ordinaryCaps().entries;
    }

    /// Terminal / required ACK / SETTINGS ACK — may consume the terminal share.
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

const test_terminal_entries: usize = 16;
const test_terminal_bytes: usize = 4 * 1024;

test "ordinary pool leaves terminal reserve" {
    var pool = ControlPool.init(64 * 1024, 256, test_terminal_bytes, test_terminal_entries);
    const caps = pool.ordinaryCaps();
    try std.testing.expectEqual(@as(usize, 64 * 1024), caps.bytes);
    try std.testing.expectEqual(@as(usize, 256), caps.entries);

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
    var pool = ControlPool.init(64 * 1024, 256, test_terminal_bytes, test_terminal_entries);
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
    var pool = ControlPool.init(64 * 1024, 256, test_terminal_bytes, test_terminal_entries);
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
    var pool = ControlPool.init(64 * 1024, 256, test_terminal_bytes, test_terminal_entries);
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
    var pool = ControlPool.init(64 * 1024, 256, test_terminal_bytes, test_terminal_entries);
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
    var pool = ControlPool.init(64 * 1024, 256, test_terminal_bytes, test_terminal_entries);
    try std.testing.expect(pool.tryReserveOrdinary(100));
    pool.release(100, 0);
    try std.testing.expectEqual(@as(usize, 0), pool.bytes_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), pool.entries_held.load(.acquire));
    pool.release(0, 1);
    try std.testing.expectEqual(@as(usize, 0), pool.entries_held.load(.acquire));
}
