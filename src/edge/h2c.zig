//! Cleartext prior-knowledge HTTP/2 startup.
//!
//! Prior knowledge only. There is no HTTP/1.1 Upgrade path, because supporting
//! one would mean carrying an HTTP/1.1 parser for the sole purpose of leaving
//! HTTP/1.1. A client on an h2c endpoint sends the HTTP/2 preface immediately
//! or it is refused.
//!
//! The preface is matched incrementally, because a client may split those 24
//! bytes across TCP segments — and a client that pipelines its preface with its
//! first request is the normal case, not an edge case.
const std = @import("std");
const frame = @import("../core/frame.zig");

pub const PrefaceError = error{
    ProtocolError,
    Timeout,
};

/// Verify the first bytes match the client connection preface (may be fragmented).
pub const PrefaceMatcher = struct {
    matched: usize = 0,

    pub fn feed(self: *PrefaceMatcher, bytes: []const u8) PrefaceError!struct { consumed: usize, done: bool } {
        const need = frame.CLIENT_PREFACE.len - self.matched;
        const take = @min(need, bytes.len);
        if (!std.mem.eql(u8, bytes[0..take], frame.CLIENT_PREFACE[self.matched..][0..take])) {
            return error.ProtocolError;
        }
        self.matched += take;
        return .{ .consumed = take, .done = self.matched == frame.CLIENT_PREFACE.len };
    }
};
