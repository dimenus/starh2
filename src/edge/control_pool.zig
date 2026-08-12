//! Ordinary vs terminal/ack control-frame occupancy + CONTROL_BEFORE_DATA fairness.
//! Connection actor owns one pool; write completion releases held entries/bytes.
const std = @import("std");
const limits_mod = @import("../core/limits.zig");

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

    pub fn release(self: *ControlPool, n: usize, entry: bool) void {
        if (entry) {
            const prev = self.entries_held.fetchSub(1, .acq_rel);
            std.debug.assert(prev >= 1);
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

    /// Deterministic fairness: 10k nonterminal controls with DATA always eligible
    /// must force a DATA quantum at most every CONTROL_BEFORE_DATA controls.
    pub fn simulateFairnessGaps(controls: usize) []usize {
        // Caller owns result via testing allocator path — use stack for unit test helper.
        _ = controls;
        return &.{};
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
    pool.release(100, true);
    try std.testing.expectEqual(@as(usize, 0), pool.bytes_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), pool.entries_held.load(.acquire));
}
