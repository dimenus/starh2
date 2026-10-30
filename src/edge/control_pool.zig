//! Ordinary vs terminal/ack control-frame occupancy + CONTROL_BEFORE_DATA fairness.
//! Connection actor owns one pool; write completion releases held entries/bytes.
//!
//! Control frames are bounded because a peer generates them. A flood of PING or
//! SETTINGS produces a matching flood of acks, so an unbounded control queue is
//! a memory target that costs the peer almost nothing.
//!
//! The pool is two shares with a counter each, so neither can starve the
//! other. Ordinary controls (`Limits.control_*_per_connection`) are held from
//! the moment a frame is queued until its write completes, and not merely until
//! it is emitted: bytes waiting in the write queue are real memory, so releasing
//! them at emit time would report capacity the connection does not have.
//!
//! Terminal frames (GOAWAY, RST_STREAM) have a share of their own,
//! `Limits.terminalFrames` entries and `Limits.terminalBytes` bytes, so a
//! connection can always emit the frame that ENDS a flood, and a pile of
//! server-driven resets can never take the room a PING or SETTINGS ack needs.
//! A terminal frame holds its share from the moment it is queued until it is
//! popped from the terminal ring, which is when its bytes move into the write
//! memory (`outbound_memory`), a pool that is bounded and pauses the pop when
//! it is full. Holding it longer would count the same bytes twice.
//!
//! The terminal share is T = M + 3 because a well-behaved peer can have at
//! most M unwritten RST_STREAMs (one per stream it still counts as open) and
//! three GOAWAYs outstanding; see `Limits.terminalFrames`. The terminal ring
//! (`FairScheduler.terminal_q`) and the terminal frame store
//! (`Connection.terminal_store`) hold the same T.
//!
const std = @import("std");

/// The starvation bound. Controls drain before DATA, so a peer that keeps a
/// control frame always available would otherwise stop every response body on
/// the connection while every individual frame is answered correctly. After
/// this many controls the scheduler forces one DATA quantum, which converts
/// unbounded starvation into a bounded delay.
pub const CONTROL_BEFORE_DATA: usize = 64;
pub const ControlPool = struct {
    /// The ordinary share.
    max_bytes: usize,
    max_entries: usize,
    /// The terminal share.
    terminal_bytes: usize,
    terminal_entries: usize,
    bytes_held: std.atomic.Value(usize) = .init(0),
    entries_held: std.atomic.Value(usize) = .init(0),
    terminal_bytes_held: std.atomic.Value(usize) = .init(0),
    terminal_entries_held: std.atomic.Value(usize) = .init(0),
    controls_since_data: usize = 0,

    /// The two shares are separate arguments, so no caller can size the pool
    /// without saying how much is terminal.
    pub fn init(ordinary_bytes: usize, ordinary_entries: usize, terminal_bytes: usize, terminal_entries: usize) ControlPool {
        return .{
            .max_bytes = ordinary_bytes,
            .max_entries = ordinary_entries,
            .terminal_bytes = terminal_bytes,
            .terminal_entries = terminal_entries,
        };
    }

    pub fn ordinaryCaps(self: *const ControlPool) struct { bytes: usize, entries: usize } {
        return .{ .bytes = self.max_bytes, .entries = self.max_entries };
    }

    pub fn tryReserveOrdinary(self: *ControlPool, n: usize) bool {
        return reserve(&self.entries_held, &self.bytes_held, self.max_entries, self.max_bytes, n, 1);
    }

    /// A whole header block: `entries` frames of `n` wire bytes in all, all
    /// or none. The peer waits for every fragment once the first is out, so
    /// a block is admitted as one unit (`FairScheduler.enqueueControlBlock`).
    pub fn tryReserveOrdinaryBlock(self: *ControlPool, n: usize, entries: usize) bool {
        return reserve(&self.entries_held, &self.bytes_held, self.max_entries, self.max_bytes, n, entries);
    }

    /// Ordinary entries free now.
    pub fn ordinaryFree(self: *const ControlPool) usize {
        return self.max_entries -| self.entries_held.load(.acquire);
    }

    /// True when one more ordinary entry fits. Bytes are not checked: the
    /// caller does not know its frame's size yet.
    pub fn hasOrdinaryEntry(self: *const ControlPool) bool {
        return self.entries_held.load(.acquire) < self.max_entries;
    }

    /// A GOAWAY or RST_STREAM of `n` wire bytes, from the terminal share.
    pub fn tryReserveTerminal(self: *ControlPool, n: usize) bool {
        return reserve(&self.terminal_entries_held, &self.terminal_bytes_held, self.terminal_entries, self.terminal_bytes, n, 1);
    }

    fn reserve(entries_held: *std.atomic.Value(usize), bytes_held: *std.atomic.Value(usize), max_entries: usize, max_bytes: usize, n: usize, k: usize) bool {
        while (true) {
            const cur_e = entries_held.load(.acquire);
            const cur_b = bytes_held.load(.acquire);
            if (cur_e > max_entries or max_entries - cur_e < k) return false;
            if (cur_b > max_bytes or max_bytes - cur_b < n) return false;
            if (entries_held.cmpxchgWeak(cur_e, cur_e + k, .acq_rel, .acquire) != null) continue;
            if (bytes_held.cmpxchgWeak(cur_b, cur_b + n, .acq_rel, .acquire) == null) return true;
            _ = entries_held.fetchSub(k, .acq_rel);
        }
    }

    /// Give back ordinary occupancy when its write completes.
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

    /// Give back one terminal frame's share when it leaves the terminal ring.
    pub fn releaseTerminal(self: *ControlPool, n: usize) void {
        const prev_e = self.terminal_entries_held.fetchSub(1, .acq_rel);
        std.debug.assert(prev_e >= 1);
        const prev_b = self.terminal_bytes_held.fetchSub(n, .acq_rel);
        std.debug.assert(prev_b >= n);
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

test "ordinary and terminal shares do not starve each other" {
    var pool = ControlPool.init(64 * 1024, 256, test_terminal_bytes, test_terminal_entries);
    const caps = pool.ordinaryCaps();
    try std.testing.expectEqual(@as(usize, 64 * 1024), caps.bytes);
    try std.testing.expectEqual(@as(usize, 256), caps.entries);

    var i: usize = 0;
    while (i < caps.entries) : (i += 1) {
        try std.testing.expect(pool.tryReserveOrdinary(1));
    }
    try std.testing.expect(!pool.tryReserveOrdinary(1));
    // The terminal share is its own: a full ordinary share does not touch it,
    // and terminal frames do not touch the ordinary counter.
    var held: usize = 0;
    while (held < test_terminal_entries) : (held += 1) {
        try std.testing.expect(pool.tryReserveTerminal(1));
    }
    try std.testing.expect(!pool.tryReserveTerminal(1));
    try std.testing.expectEqual(caps.entries, pool.entries_held.load(.acquire));
    pool.releaseTerminal(1);
    try std.testing.expect(pool.tryReserveTerminal(1));
    // And the other way: a full terminal share leaves ordinary frames alone.
    pool.release(1, 1);
    try std.testing.expect(pool.tryReserveOrdinary(1));
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

test "a header block is reserved whole or not at all" {
    var pool = ControlPool.init(1024, 4, test_terminal_bytes, test_terminal_entries);
    try std.testing.expect(pool.tryReserveOrdinary(10));
    try std.testing.expect(pool.tryReserveOrdinary(10));
    // Two entries left: a three-fragment block takes none of them.
    try std.testing.expect(!pool.tryReserveOrdinaryBlock(30, 3));
    try std.testing.expectEqual(@as(usize, 2), pool.entries_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 20), pool.bytes_held.load(.acquire));
    // Bytes too: two entries, but not the bytes.
    try std.testing.expect(!pool.tryReserveOrdinaryBlock(1024, 2));
    try std.testing.expect(pool.tryReserveOrdinaryBlock(40, 2));
    try std.testing.expectEqual(@as(usize, 4), pool.entries_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 60), pool.bytes_held.load(.acquire));
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
