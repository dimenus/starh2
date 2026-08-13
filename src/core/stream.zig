//! Per-stream state machine (RFC 9113 section 5.1).
//!
//! `Session` owns one `Stream` per live peer stream and calls the `on*`
//! transitions as frames arrive. Each transition either advances the state or
//! returns the error that names the frame's fault, and `toErrorCode` maps that
//! error to the wire code. `Session` decides whether the fault costs the stream
//! (RST_STREAM) or the connection (GOAWAY), because only `Session` knows the
//! connection-wide context.
//!
//! The `reserved_local` and `reserved_remote` states exist for server push.
//! This stack never sends PUSH_PROMISE, so nothing reaches them. They stay in
//! the enum so that the state names match the RFC that a reader compares
//! against.
const std = @import("std");
const frame = @import("frame.zig");
const flow = @import("flow.zig");

pub const State = enum {
    idle,
    open,
    half_closed_local,
    half_closed_remote,
    closed,
    reserved_local,
    reserved_remote,
};

pub const StreamError = error{
    ProtocolError,
    StreamClosed,
};

pub const Stream = struct {
    id: u31,
    state: State = .idle,
    window: flow.StreamWindow = .{},
    end_stream_remote: bool = false,
    end_stream_local: bool = false,
    headers_done: bool = false,
    trailers_done: bool = false,
    content_length: ?u64 = null,
    body_received: u64 = 0,
    refused_before_dispatch: bool = false,
    handler_started: bool = false,
    generation: u32 = 0,
    /// Set when emitting 431/414/413 without dispatch.
    early_status: ?u16 = null,
    /// Exactly one early HTTP response frameset has been emitted.
    early_response_sent: bool = false,
    /// Concurrent active_streams credit already released for this stream.
    concurrency_released: bool = false,

    /// The first HEADERS opens the stream. A later HEADERS is a trailer block,
    /// and a trailer block must carry END_STREAM, because nothing may follow
    /// it. A second header block without END_STREAM is therefore a protocol
    /// error and not a second request.
    pub fn onHeaders(self: *Stream, end_stream: bool) StreamError!void {
        switch (self.state) {
            .idle => {
                self.state = if (end_stream) .half_closed_remote else .open;
                self.headers_done = true;
                if (end_stream) self.end_stream_remote = true;
            },
            .open, .half_closed_local => {
                if (!end_stream) return error.ProtocolError;
                self.trailers_done = true;
                self.end_stream_remote = true;
                self.state = if (self.state == .open) .half_closed_remote else .closed;
            },
            else => return error.StreamClosed,
        }
    }

    /// A declared `content-length` becomes an enforced promise here. The check
    /// runs in both directions: too many bytes fail at once, and too few fail
    /// at END_STREAM. A handler that trusts `content-length` must never see a
    /// body that disagrees with it.
    pub fn onData(self: *Stream, len: usize, end_stream: bool) StreamError!void {
        switch (self.state) {
            .open, .half_closed_local => {},
            else => return error.StreamClosed,
        }
        self.body_received += len;
        if (self.content_length) |cl| {
            if (self.body_received > cl) return error.ProtocolError;
            if (end_stream and self.body_received != cl) return error.ProtocolError;
        }
        if (end_stream) {
            self.end_stream_remote = true;
            self.state = if (self.state == .open) .half_closed_remote else .closed;
        }
    }

    pub fn onRst(self: *Stream) void {
        self.state = .closed;
    }

    pub fn localEndStream(self: *Stream) void {
        self.end_stream_local = true;
        self.state = switch (self.state) {
            .open => .half_closed_local,
            .half_closed_remote => .closed,
            else => self.state,
        };
    }

    pub fn toErrorCode(err: StreamError) frame.ErrorCode {
        return switch (err) {
            error.ProtocolError => .protocol_error,
            error.StreamClosed => .stream_closed,
        };
    }
};

test "headers open then end" {
    var s: Stream = .{ .id = 1 };
    try s.onHeaders(false);
    try std.testing.expectEqual(State.open, s.state);
    try s.onData(0, true);
    try std.testing.expectEqual(State.half_closed_remote, s.state);
}
