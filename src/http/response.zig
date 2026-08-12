const std = @import("std");
const request = @import("request.zig");
const frame = @import("../core/frame.zig");

pub const TerminalCause = union(enum) {
    peer_reset: frame.ErrorCode,
    slow_consumer,
    connection_closed,
    server_shutdown,
    internal,
};

pub const ResponseError = error{
    Canceled,
    OutOfMemory,
    InvalidStatus,
    InvalidHeader,
    ResponseCommitted,
    BodyClosed,
    ConnectionClosed,
    PeerReset,
    SlowConsumer,
    WriteFailed,
};

/// Actor-owned slot state. Never points into handler-job memory.
pub const SlotTerminal = struct {
    kind: std.atomic.Value(u8) = .init(0),
    code: std.atomic.Value(u32) = .init(0),
    generation: std.atomic.Value(u32) = .init(0),
    cancel_flag: std.atomic.Value(bool) = .init(false),

    const none: u8 = 0;
    const peer_reset: u8 = 1;
    const slow_consumer: u8 = 2;
    const connection_closed: u8 = 3;
    const server_shutdown: u8 = 4;
    const internal: u8 = 5;

    pub fn clear(self: *SlotTerminal) void {
        self.kind.store(none, .release);
        self.code.store(0, .release);
        self.cancel_flag.store(false, .release);
    }

    pub fn setCause(self: *SlotTerminal, cause: TerminalCause) void {
        // First writer wins; always bump generation to invalidate Body aliases.
        const kind: u8 = switch (cause) {
            .peer_reset => peer_reset,
            .slow_consumer => slow_consumer,
            .connection_closed => connection_closed,
            .server_shutdown => server_shutdown,
            .internal => internal,
        };
        const code: u32 = switch (cause) {
            .peer_reset => |c| @intFromEnum(c),
            else => 0,
        };
        _ = self.kind.cmpxchgStrong(none, kind, .acq_rel, .acquire);
        if (kind == peer_reset) self.code.store(code, .release);
        self.cancel_flag.store(true, .release);
        _ = self.generation.fetchAdd(1, .acq_rel);
    }

    pub fn getCause(self: *const SlotTerminal) ?TerminalCause {
        return switch (self.kind.load(.acquire)) {
            none => null,
            peer_reset => .{ .peer_reset = @enumFromInt(self.code.load(.acquire)) },
            slow_consumer => .slow_consumer,
            connection_closed => .connection_closed,
            server_shutdown => .server_shutdown,
            internal => .internal,
            else => .internal,
        };
    }

    pub fn currentGeneration(self: *const SlotTerminal) u32 {
        return self.generation.load(.acquire);
    }
};

pub const Body = struct {
    response: *Response,
    stream_id: u31,
    generation: u32,

    pub fn writeAll(self: *Body, bytes: []const u8) ResponseError!void {
        return self.response.writeBody(self, bytes);
    }
    pub fn flush(self: *Body) ResponseError!void {
        return self.response.flushBody(self);
    }
    pub fn finish(self: *Body) ResponseError!void {
        return self.response.finishBody(self);
    }
    pub fn abort(self: *Body) ResponseError!void {
        return self.response.abortBody(self);
    }
    pub fn terminalCause(self: *const Body) ?TerminalCause {
        return self.response.slotTerminal();
    }
};

pub const Response = struct {
    stream_id: u31,
    /// Snapshot at Body creation; live generation/cause live in `terminal`.
    generation: u32,
    committed: bool = false,
    sse: bool = false,
    body_open: bool = false,
    finished: bool = false,
    /// Stable actor-owned terminal/generation (HandlerSlot). Never handler-job memory.
    terminal: *SlotTerminal,
    ctx: *anyopaque,
    sendFn: *const fn (*anyopaque, u31, u16, []const request.Header, []const u8) ResponseError!void,
    startFn: *const fn (*anyopaque, u31, u16, []const request.Header, bool) ResponseError!void,
    writeFn: *const fn (*anyopaque, u31, []const u8, bool, bool) ResponseError!void,
    flushFn: *const fn (*anyopaque, u31) ResponseError!void,
    abortFn: *const fn (*anyopaque, u31) ResponseError!void,

    pub fn slotTerminal(self: *const Response) ?TerminalCause {
        return self.terminal.getCause();
    }

    pub fn send(self: *Response, status: u16, headers: []const request.Header, body: []const u8) ResponseError!void {
        if (self.slotTerminal()) |c| return causeToError(c);
        if (self.committed) return error.ResponseCommitted;
        if (status < 100 or status > 599) return error.InvalidStatus;
        self.committed = true;
        try self.sendFn(self.ctx, self.stream_id, status, headers, body);
        self.finished = true;
    }

    pub fn start(self: *Response, status: u16, headers: []const request.Header) ResponseError!Body {
        if (self.slotTerminal()) |c| return causeToError(c);
        if (self.committed) return error.ResponseCommitted;
        if (status < 100 or status > 599) return error.InvalidStatus;
        self.committed = true;
        self.body_open = true;
        try self.startFn(self.ctx, self.stream_id, status, headers, false);
        return .{ .response = self, .stream_id = self.stream_id, .generation = self.terminal.currentGeneration() };
    }

    pub fn startSse(self: *Response, headers: []const request.Header) ResponseError!Body {
        if (self.slotTerminal()) |c| return causeToError(c);
        if (self.committed) return error.ResponseCommitted;
        self.committed = true;
        self.sse = true;
        self.body_open = true;
        try self.startFn(self.ctx, self.stream_id, 200, headers, true);
        return .{ .response = self, .stream_id = self.stream_id, .generation = self.terminal.currentGeneration() };
    }

    fn checkBody(self: *Response, body: *Body) ResponseError!void {
        // Exact terminal cause before stale-generation BodyClosed.
        if (self.terminal.getCause()) |c| return causeToError(c);
        if (body.generation != self.terminal.currentGeneration()) return error.BodyClosed;
        if (self.finished or !self.body_open) return error.BodyClosed;
    }

    fn writeBody(self: *Response, body: *Body, bytes: []const u8) ResponseError!void {
        try self.checkBody(body);
        // SSE write has exactly one implicit flush — no second flushCb.
        try self.writeFn(self.ctx, self.stream_id, bytes, false, self.sse);
    }

    fn flushBody(self: *Response, body: *Body) ResponseError!void {
        try self.checkBody(body);
        try self.flushFn(self.ctx, self.stream_id);
    }

    fn finishBody(self: *Response, body: *Body) ResponseError!void {
        if (self.terminal.getCause()) |c| return causeToError(c);
        if (body.generation != self.terminal.currentGeneration()) return error.BodyClosed;
        if (self.finished) return error.BodyClosed;
        try self.writeFn(self.ctx, self.stream_id, &.{}, true, true);
        self.body_open = false;
        self.finished = true;
    }

    fn abortBody(self: *Response, body: *Body) ResponseError!void {
        _ = body;
        if (self.terminal.getCause()) |c| return causeToError(c);
        try self.abortFn(self.ctx, self.stream_id);
        self.body_open = false;
        self.finished = true;
        self.terminal.setCause(.internal);
    }
};

pub fn causeToError(c: TerminalCause) ResponseError {
    return switch (c) {
        .peer_reset => error.PeerReset,
        .slow_consumer => error.SlowConsumer,
        .connection_closed, .server_shutdown => error.ConnectionClosed,
        .internal => error.WriteFailed,
    };
}

test "Body terminal cause precedes generation bump" {
    var slot: SlotTerminal = .{};
    var resp: Response = .{
        .stream_id = 1,
        .generation = 0,
        .terminal = &slot,
        .ctx = undefined,
        .sendFn = undefined,
        .startFn = undefined,
        .writeFn = struct {
            fn f(_: *anyopaque, _: u31, _: []const u8, _: bool, _: bool) ResponseError!void {
                return error.WriteFailed;
            }
        }.f,
        .flushFn = undefined,
        .abortFn = undefined,
        .body_open = true,
        .committed = true,
    };
    var body: Body = .{ .response = &resp, .stream_id = 1, .generation = slot.currentGeneration() };
    slot.setCause(.{ .peer_reset = .cancel });
    try std.testing.expectError(error.PeerReset, body.writeAll("x"));
    const tc = body.terminalCause() orelse return error.TestUnexpectedResult;
    try std.testing.expect(tc == .peer_reset);

    slot.clear();
    _ = slot.generation.fetchAdd(1, .acq_rel); // stale without cause
    body.generation = 0;
    slot.kind.store(0, .release);
    // generation mismatch alone → BodyClosed
    const gen_now = slot.currentGeneration();
    body.generation = gen_now -% 1;
    try std.testing.expectError(error.BodyClosed, body.writeAll("y"));
}

test "every terminal cause maps to exact Body error" {
    const cases = [_]struct { TerminalCause, ResponseError }{
        .{ .{ .peer_reset = .cancel }, error.PeerReset },
        .{ .slow_consumer, error.SlowConsumer },
        .{ .connection_closed, error.ConnectionClosed },
        .{ .server_shutdown, error.ConnectionClosed },
        .{ .internal, error.WriteFailed },
    };
    for (cases) |c| {
        var slot: SlotTerminal = .{};
        var resp: Response = .{
            .stream_id = 1,
            .generation = 0,
            .terminal = &slot,
            .ctx = undefined,
            .sendFn = undefined,
            .startFn = undefined,
            .writeFn = struct {
                fn f(_: *anyopaque, _: u31, _: []const u8, _: bool, _: bool) ResponseError!void {
                    return;
                }
            }.f,
            .flushFn = struct {
                fn f(_: *anyopaque, _: u31) ResponseError!void {
                    return;
                }
            }.f,
            .abortFn = undefined,
            .body_open = true,
            .committed = true,
        };
        var body: Body = .{ .response = &resp, .stream_id = 1, .generation = 0 };
        slot.setCause(c[0]);
        try std.testing.expectError(c[1], body.writeAll("z"));
        try std.testing.expectError(c[1], body.flush());
        try std.testing.expectError(c[1], body.finish());
    }
}
