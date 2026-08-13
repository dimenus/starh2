//! HTTP/2 flow-control windows (RFC 9113 section 5.2).
//!
//! Two independent credit systems apply to every DATA byte: one for the
//! connection and one for the stream. A byte needs credit from both. The
//! sender-side effect is in `availableSend`, which returns the minimum.
//!
//! Windows are `i32` and not unsigned on purpose. A send window can go
//! negative, and only through one route: the peer lowers
//! SETTINGS_INITIAL_WINDOW_SIZE after the server already sent bytes under the
//! older, larger window. The protocol allows that state and forbids more
//! sending until a WINDOW_UPDATE restores credit. An unsigned type would have
//! to clamp, and a clamp would let the server send bytes the peer never
//! credited.
const std = @import("std");

pub const INITIAL_WINDOW: i32 = 65_535;
/// The server raises its own receive window to 4 MiB right after the preface.
/// The 64 KiB protocol default costs one WINDOW_UPDATE round trip per 64 KiB of
/// request body, which caps upload throughput at one window per round trip.
/// This is the receive side only, so it spends server memory the request-byte
/// limits already bound. It does not change what the peer accepts.
pub const CONNECTION_RECV_TARGET: i32 = 4 * 1024 * 1024;

pub const FlowError = error{
    ConnectionFlowControl,
    StreamFlowControl,
    ProtocolError,
};

pub const Windows = struct {
    conn_recv: i32 = INITIAL_WINDOW,
    conn_send: i32 = INITIAL_WINDOW,
    /// Per-stream windows keyed externally; helpers operate on pairs.
    pub fn debitConnRecv(self: *Windows, len: i32) FlowError!void {
        if (len < 0) return error.ProtocolError;
        if (self.conn_recv < len) return error.ConnectionFlowControl;
        self.conn_recv -= len;
    }

    pub fn creditConnRecv(self: *Windows, len: i32) void {
        self.conn_recv += len;
    }

    pub fn applyConnWindowUpdate(self: *Windows, increment: u31) FlowError!void {
        if (increment == 0) return error.ProtocolError;
        const sum = @as(i64, self.conn_send) + @as(i64, increment);
        if (sum > std.math.maxInt(i32)) return error.ConnectionFlowControl;
        self.conn_send = @intCast(sum);
    }

    /// Replenish at the half-way point, not when the window empties. A sender
    /// that waits for zero stalls the peer for one round trip on every window.
    pub fn needsConnWindowUpdate(self: *const Windows) bool {
        return self.conn_recv <= CONNECTION_RECV_TARGET / 2;
    }

    pub fn connWindowUpdateIncrement(self: *const Windows) u31 {
        const target: i32 = CONNECTION_RECV_TARGET;
        const delta = target - self.conn_recv;
        if (delta <= 0) return 0;
        return @intCast(delta);
    }
};

pub const StreamWindow = struct {
    recv: i32 = INITIAL_WINDOW,
    send: i32 = INITIAL_WINDOW,

    pub fn debitRecv(self: *StreamWindow, len: i32) FlowError!void {
        if (self.recv < len) return error.StreamFlowControl;
        self.recv -= len;
    }

    pub fn creditRecv(self: *StreamWindow, len: i32) void {
        self.recv += len;
    }

    pub fn applySendUpdate(self: *StreamWindow, increment: u31) FlowError!void {
        if (increment == 0) return error.ProtocolError;
        const sum = @as(i64, self.send) + @as(i64, increment);
        if (sum > std.math.maxInt(i32)) return error.StreamFlowControl;
        self.send = @intCast(sum);
    }

    /// A new SETTINGS_INITIAL_WINDOW_SIZE retunes every OPEN stream by the
    /// difference, and not by an assignment. Bytes already in flight were sent
    /// against the old window, so a plain assignment would credit them twice.
    /// This is the one path that can make `send` negative.
    pub fn applyInitialWindowDelta(self: *StreamWindow, delta: i32) FlowError!void {
        const sum = @as(i64, self.send) + @as(i64, delta);
        if (sum > std.math.maxInt(i32) or sum < std.math.minInt(i32)) return error.ConnectionFlowControl;
        // Negative windows are legal until a later WINDOW_UPDATE restores credit.
        self.send = @intCast(sum);
    }

    pub fn needsWindowUpdate(self: *const StreamWindow) bool {
        return self.recv <= INITIAL_WINDOW / 2;
    }

    pub fn windowUpdateIncrement(self: *const StreamWindow) u31 {
        const delta = INITIAL_WINDOW - self.recv;
        if (delta <= 0) return 0;
        return @intCast(delta);
    }

    pub fn availableSend(self: *const StreamWindow, conn_send: i32) i32 {
        // Negative peer windows are legal after SETTINGS_INITIAL_WINDOW_SIZE
        // reductions; treat as zero until WINDOW_UPDATE restores credit.
        if (self.send <= 0 or conn_send <= 0) return 0;
        return @min(self.send, conn_send);
    }
};

test "connection underflow" {
    var w: Windows = .{};
    try w.debitConnRecv(100);
    try std.testing.expectError(error.ConnectionFlowControl, w.debitConnRecv(w.conn_recv + 1));
}

test "zero window update is protocol error" {
    var s: StreamWindow = .{};
    try std.testing.expectError(error.ProtocolError, s.applySendUpdate(0));
}

test "negative send window blocks DATA until WINDOW_UPDATE" {
    var s: StreamWindow = .{ .send = 100 };
    try s.applyInitialWindowDelta(-200); // send = -100
    try std.testing.expect(s.send < 0);
    try std.testing.expectEqual(@as(i32, 0), s.availableSend(65_535));
    try s.applySendUpdate(250);
    try std.testing.expectEqual(@as(i32, 150), s.availableSend(65_535));
}
