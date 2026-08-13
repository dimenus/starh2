//! Request field validation — the HTTP/2 half of RFC 9113 section 8.3.
//!
//! Every decoded header block passes through here before a handler exists. The
//! purpose is request smuggling, not tidiness. HTTP/2 and HTTP/1.1 disagree on
//! what a message is, and a proxy chain usually runs both. A field that this
//! server accepts and a downstream HTTP/1.1 hop re-reads differently becomes
//! two requests where the operator counted one. So the rules below reject
//! rather than repair: no case folding, no whitespace trim, no
//! `transfer-encoding` translation.
//!
//! `validateRequestFields` is called twice for one request, and that is
//! deliberate. `finishHeaderBlock` calls it to decide admission, and
//! `maybeDispatch` calls it again on the stored fields just before it hands
//! ownership to a handler. The function is pure, so the second call costs only
//! time and proves that the fields a handler receives are the fields that
//! passed.
const std = @import("std");
const hpack = @import("hpack.zig");

pub const ValidateError = error{
    ProtocolError,
    PathTooLong,
    HeaderTooLarge,
};

pub const ParsedRequest = struct {
    method: []const u8,
    scheme: []const u8,
    authority: []const u8,
    path: []const u8,
    query: []const u8,
    headers: []hpack.HeaderField,
    content_length: ?u64,
};

/// HTTP/2 has no connection-specific header fields. The connection itself
/// carries what these once expressed, so their presence means the peer speaks
/// HTTP/1.1 semantics into an HTTP/2 frame. `transfer-encoding` is the
/// dangerous member: a downstream HTTP/1.1 hop would use it to re-frame the
/// body and would then disagree with this server about where the request ends.
const connection_specific = [_][]const u8{
    "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade",
};

fn isLowerToken(s: []const u8) bool {
    for (s) |c| {
        if (c == ':' ) continue;
        if (c < 'a' or c > 'z') {
            if (c == '-' or c == '_' or (c >= '0' and c <= '9')) continue;
            return false;
        }
    }
    return true;
}

/// A CR, an LF, or a NUL inside a field value is the request-smuggling
/// primitive: a downstream HTTP/1.1 hop reads the same bytes as a field
/// terminator and gains a field the operator never wrote. Leading and trailing
/// whitespace is refused for the same reason, because a hop that trims and a
/// hop that does not will read two different values.
fn valueOk(v: []const u8) bool {
    if (v.len == 0) return true;
    if (v[0] == ' ' or v[0] == '\t') return false;
    if (v[v.len - 1] == ' ' or v[v.len - 1] == '\t') return false;
    for (v) |c| {
        if (c == 0 or c == '\r' or c == '\n') return false;
    }
    return true;
}

pub fn validateRequestFields(fields: []const hpack.HeaderField) ValidateError!struct {
    method: []const u8,
    scheme: []const u8,
    authority: []const u8,
    path: []const u8,
    query: []const u8,
    content_length: ?u64,
} {
    var method: ?[]const u8 = null;
    var scheme: ?[]const u8 = null;
    var authority: []const u8 = "";
    var path: ?[]const u8 = null;
    var content_length: ?u64 = null;
    // Pseudo-header fields must all precede the regular fields, so one flag in
    // source order is enough to enforce it. A pseudo-header after a regular one
    // is a protocol error and not a late field.
    var seen_regular = false;

    for (fields) |f| {
        if (f.name.len == 0) return error.ProtocolError;
        if (f.name[0] == ':') {
            if (seen_regular) return error.ProtocolError;
            if (!isLowerToken(f.name)) return error.ProtocolError;
            if (!valueOk(f.value)) return error.ProtocolError;
            if (std.mem.eql(u8, f.name, ":method")) {
                if (method != null) return error.ProtocolError;
                method = f.value;
            } else if (std.mem.eql(u8, f.name, ":scheme")) {
                if (scheme != null) return error.ProtocolError;
                scheme = f.value;
            } else if (std.mem.eql(u8, f.name, ":authority")) {
                if (authority.len != 0) return error.ProtocolError;
                authority = f.value;
            } else if (std.mem.eql(u8, f.name, ":path")) {
                if (path != null) return error.ProtocolError;
                path = f.value;
            } else if (std.mem.eql(u8, f.name, ":status")) {
                return error.ProtocolError;
            } else {
                return error.ProtocolError;
            }
        } else {
            seen_regular = true;
            if (!isLowerToken(f.name)) return error.ProtocolError;
            if (!valueOk(f.value)) return error.ProtocolError;
            for (connection_specific) |cs| {
                if (std.mem.eql(u8, f.name, cs)) return error.ProtocolError;
            }
            if (std.mem.eql(u8, f.name, "te")) {
                if (!std.ascii.eqlIgnoreCase(f.value, "trailers")) return error.ProtocolError;
            }
            if (std.mem.eql(u8, f.name, "content-length")) {
                const n = std.fmt.parseInt(u64, f.value, 10) catch return error.ProtocolError;
                if (content_length) |cl| {
                    if (cl != n) return error.ProtocolError;
                } else content_length = n;
            }
        }
    }
    const m = method orelse return error.ProtocolError;
    const s = scheme orelse return error.ProtocolError;
    const p = path orelse return error.ProtocolError;
    if (p.len == 0) return error.ProtocolError;
    if (p.len > 24 * 1024) return error.PathTooLong;

    var path_part = p;
    var query: []const u8 = "";
    if (std.mem.indexOfScalar(u8, p, '?')) |q| {
        path_part = p[0..q];
        query = p[q + 1 ..];
    }
    if (path_part.len > 16 * 1024 or query.len > 16 * 1024) return error.PathTooLong;

    _ = m;
    _ = s;
    return .{
        .method = method.?,
        .scheme = scheme.?,
        .authority = authority,
        .path = path_part,
        .query = query,
        .content_length = content_length,
    };
}

test "reject uppercase" {
    const fields = [_]hpack.HeaderField{.{ .name = "Host", .value = "x" }};
    try std.testing.expectError(error.ProtocolError, validateRequestFields(&fields));
}
