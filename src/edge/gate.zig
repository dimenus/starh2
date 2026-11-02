//! The gate between a handler's task and the connection that dispatched it.
//!
//! A task handler can outlive its connection: zio cancellation is a request,
//! and a handler that is shielded, swallows `error.Canceled`, or never yields
//! keeps running. The connection must still end and give its slot back. So a
//! handler reaches connection memory only through a gate, and only while the
//! gate is open:
//!
//! - `enter` before touching the connection, `exit` after. `enter` fails once
//!   the gate is closed, and the caller then acts as if the connection is gone.
//! - The connection's actor `close`s the gate at teardown, then
//!   `waitDrained` waits for the calls already inside. That wait is bounded by
//!   starh2 code, not by handler code: every wait a call can reach inside the
//!   connection also wakes on the connection's `dead` event.
//! - After `waitDrained` returns, no call is inside and none can enter, so the
//!   connection may be freed while the handler keeps running.
//!
//! The gate lives in memory the handler owns (its stream link), never in the
//! connection, so `exit` and `drained.set` never touch connection memory.
//! `Event.set` does not touch the event after it wakes the last waiter, so the
//! actor may go on to drop its reference as soon as it wakes.
//!
//! Calls nest: a call inside the gate may enter again (an arena allocation made
//! while building a response). A nested `enter` after `close` fails like any
//! other, and the outer call still holds the gate open until it exits.
const std = @import("std");
const zio = @import("zio");

pub const Gate = struct {
    /// Bit 31: closed. The low 31 bits: calls inside.
    state: std.atomic.Value(u32) = .init(0),
    /// Set once the gate is closed and no call is inside.
    drained: zio.Event = .init,

    const closed_bit: u32 = 1 << 31;
    const count_mask: u32 = closed_bit - 1;

    /// True when the caller may touch the connection until its `exit`.
    pub fn enter(self: *Gate) bool {
        var s = self.state.load(.acquire);
        while (true) {
            if (s & closed_bit != 0) return false;
            std.debug.assert(s & count_mask != count_mask);
            s = self.state.cmpxchgWeak(s, s + 1, .acquire, .acquire) orelse return true;
        }
    }

    /// Leave after a successful `enter`. Nothing in the connection may be
    /// touched after this.
    pub fn exit(self: *Gate) void {
        const prev = self.state.fetchSub(1, .release);
        std.debug.assert(prev & count_mask != 0);
        if (prev == closed_bit | 1) self.drained.set();
    }

    /// Actor only, once. Every later `enter` fails.
    pub fn close(self: *Gate) void {
        const prev = self.state.fetchOr(closed_bit, .acq_rel);
        std.debug.assert(prev & closed_bit == 0);
        if (prev == 0) self.drained.set();
    }

    pub fn isClosed(self: *const Gate) bool {
        return self.state.load(.acquire) & closed_bit != 0;
    }

    /// Calls inside right now. For a test or a diagnosis only.
    pub fn inside(self: *const Gate) u32 {
        return self.state.load(.acquire) & count_mask;
    }

    /// Actor only, after `close`: wait until no call is inside. The wait does
    /// not take a cancel, because nothing may be freed until it returns.
    pub fn waitDrained(self: *Gate) void {
        std.debug.assert(self.isClosed());
        zio.beginShield();
        defer zio.endShield();
        while (!self.drained.isSet()) {
            self.drained.wait() catch {};
        }
        std.debug.assert(self.state.load(.acquire) == closed_bit);
    }
};

test "a closed gate refuses entry, and drains when the last call leaves" {
    var g: Gate = .{};
    try std.testing.expect(g.enter());
    try std.testing.expect(g.enter());
    g.close();
    try std.testing.expect(!g.enter());
    try std.testing.expect(!g.drained.isSet());
    g.exit();
    try std.testing.expect(!g.drained.isSet());
    g.exit();
    try std.testing.expect(g.drained.isSet());
    try std.testing.expectEqual(@as(u32, 0), g.inside());
}

test "closing an empty gate drains it at once" {
    var g: Gate = .{};
    g.close();
    try std.testing.expect(g.drained.isSet());
    try std.testing.expect(!g.enter());
}
