//! The handler-facing response API, and the state that makes it safe to use
//! from a task the connection can cancel at any moment.
//!
//! A handler runs concurrently with the connection actor, so the stream it is
//! answering can end underneath it: the peer resets, the peer stops reading,
//! the transport fails, or the server shuts down. Every one of those must reach
//! the handler as an EXACT error rather than as a generic failure, because a
//! handler's correct reaction differs — a peer reset is normal and needs no
//! log, while a write failure is not.
//!
//! `Response` therefore holds no connection state of its own. It carries
//! function pointers into `edge.connection` and a pointer to actor-owned
//! `SlotTerminal`. Every entry point reads the terminal cause first, so a
//! handler cannot act on a stream that is already gone.
const std = @import("std");
const request = @import("request.zig");
const frame = @import("../core/frame.zig");

/// Why a stream ended before its handler finished. The variants exist so that
/// `causeToError` can hand the handler the precise reason, and so that a log
/// can tell a peer's decision apart from a server fault.
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
///
/// It lives in the `HandlerSlot` because the actor must be able to record a
/// cause after the handler job is gone, and a handler must be able to read one
/// while the actor writes it. All fields are atomic for that reason: the two
/// tasks share this struct with no lock between them.
///
/// Two independent facts are stored here, and confusing them is a bug:
/// - the CAUSE, which is first-writer-wins, because the first reason is the
///   real one and every later cause is a consequence of it;
/// - the GENERATION, which bumps on every cause and on every slot reuse. A
///   `Body` records the generation it was created under, so a stale `Body`
///   fails with `BodyClosed` instead of writing into a stream that now belongs
///   to a different request.
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

/// A handle to an open response body. It is a capability with an expiry: the
/// generation it was created under. That is what makes a stale handle — one
/// kept across a stream reset and a slot reuse — fail loudly instead of writing
/// into another request's stream.
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
    writeFn: *const fn (*anyopaque, u31, []const u8, bool, bool, bool) ResponseError!void,
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

    /// The guard every body operation runs first.
    ///
    /// The order is deliberate: a terminal CAUSE outranks a generation
    /// mismatch. Setting a cause also bumps the generation, so both conditions
    /// are true at once on a reset stream. Reporting `BodyClosed` there would
    /// replace the real reason — "the peer reset this stream" — with a
    /// misuse-shaped error that tells the handler nothing.
    fn checkBody(self: *Response, body: *Body) ResponseError!void {
        // Exact terminal cause before stale-generation BodyClosed.
        if (self.terminal.getCause()) |c| return causeToError(c);
        if (body.generation != self.terminal.currentGeneration()) return error.BodyClosed;
        if (self.finished or !self.body_open) return error.BodyClosed;
    }

    fn writeBody(self: *Response, body: *Body, bytes: []const u8) ResponseError!void {
        try self.checkBody(body);
        // Two things that used to be one flag, now separate:
        //
        // - EMIT. An SSE write still emits immediately: the compressor is told
        //   to flush, so the bytes leave the encoder and the actor puts them on
        //   the wire on its next turn. Without this a compressed stream keeps
        //   every event until the response ends, which for a long-lived stream
        //   means the client sees nothing at all. That failure is silent, and
        //   the I4 gates could not see it, so it is not left to the caller.
        //
        // - WAIT. An SSE write blocks for a receipt. Removing that wait kept
        //   throughput flat but made latency 21x worse: the receipt keeps the
        //   producer self-clocked instead of turning queue depth into latency.
        //
        // `Body.flush()` also emits and waits. Capacity backpressure remains
        // independent of receipts: a handler that outruns the connection blocks
        // in `waitForStreamSpace`, because the per-stream slab is bounded by
        // `outbound_bytes_per_stream`.
        try self.writeFn(self.ctx, self.stream_id, bytes, false, self.sse, self.sse);
    }

    fn flushBody(self: *Response, body: *Body) ResponseError!void {
        try self.checkBody(body);
        try self.flushFn(self.ctx, self.stream_id);
    }

    fn finishBody(self: *Response, body: *Body) ResponseError!void {
        if (self.terminal.getCause()) |c| return causeToError(c);
        if (body.generation != self.terminal.currentGeneration()) return error.BodyClosed;
        if (self.finished) return error.BodyClosed;
        // end: emit and WAIT — finish must not return before the wire has it.
        try self.writeFn(self.ctx, self.stream_id, &.{}, true, true, true);
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
            fn f(_: *anyopaque, _: u31, _: []const u8, _: bool, _: bool, _: bool) ResponseError!void {
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
                fn f(_: *anyopaque, _: u31, _: []const u8, _: bool, _: bool, _: bool) ResponseError!void {
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
