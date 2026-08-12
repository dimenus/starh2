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
