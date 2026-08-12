//! Cleartext prior-knowledge HTTP/2 startup.
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
