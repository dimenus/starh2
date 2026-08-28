//! Pico-style HTTP/1.1 request-head parser for the edge channel.
//!
//! One reserved contiguous buffer. Bytes accumulate until `\r\n\r\n` (or a
//! bound). One pass over the complete head. A `u64` counter then consumes a
//! Content-Length body. No byte-callback machine and no generated parser.
//!
//! Status codes on the reject paths are the cut table in `tools/h1-channel-brief.md`.
//! Do not weaken a rejection into an accept.
const std = @import("std");
const request = @import("../http/request.zig");
const fields_mod = @import("../core/fields.zig");
const hpack = @import("../core/hpack.zig");

pub const Header = request.Header;

/// Test-only mutation hooks. Default `none` is production. The battery's
/// mutation tests flip one at a time and expect a named fixture to fail.
pub const Mutation = enum {
    none,
    /// Consume one byte too few after the head terminator (M1).
    m1_short_consume,
    /// Parse Content-Length with a lenient integer parse (M8).
    m8_lenient_cl,
};

pub var test_mutation: Mutation = .none;

pub const Version = enum { http10, http11 };

pub const Limits = struct {
    head_bytes: usize,
    header_fields: usize,
    body_bytes: usize,
};

pub const Accumulator = struct {
    buf: []u8,
    filled: usize = 0,
    /// `HeadEnd` match count (0..4) for `\r\n\r\n`.
    matched: u8 = 0,
    leading_crlf_skipped: bool = false,
    first_line_end: ?usize = null,

    pub fn init(buf: []u8) Accumulator {
        return .{ .buf = buf };
    }

    pub fn reset(self: *Accumulator) void {
        self.filled = 0;
        self.matched = 0;
        self.leading_crlf_skipped = false;
        self.first_line_end = null;
    }

    pub const Feed = union(enum) {
        need_more,
        /// `consumed` is how many of this chunk belong to the head, including
        /// the terminator. I1: the next request starts at this offset plus
        /// the Content-Length body.
        head: struct { consumed: usize, len: usize },
        /// HTTP/0.9: first line has no version token. No response.
        http09,
        reject: struct { status: u16, consumed: usize },
    };

    /// Copy `bytes` into the reserved buffer. Never grows it. A head that
    /// would pass `buf.len` is 431, a peer fault.
    pub fn feed(self: *Accumulator, bytes: []const u8) Feed {
        var i: usize = 0;
        while (i < bytes.len) : (i += 1) {
            const b = bytes[i];
            if (self.filled == 0 and !self.leading_crlf_skipped and b == '\r') {
                // Fall through: a leading CR starts the usual matcher. A
                // single leading `\r\n` is skipped after the first line
                // search in `parse`. Extra leading CRLFs stay in the buffer
                // and parse rejects them as a second blank line.
            }

            if (self.filled >= self.buf.len) {
                return .{ .reject = .{ .status = 431, .consumed = i } };
            }

            if (b == '\n' and self.matched != 1 and self.matched != 3) {
                self.buf[self.filled] = b;
                self.filled += 1;
                return .{ .reject = .{ .status = 400, .consumed = i + 1 } };
            }

            self.buf[self.filled] = b;
            self.filled += 1;

            if (self.first_line_end == null and self.filled >= 2 and
                self.buf[self.filled - 2] == '\r' and b == '\n')
            {
                self.first_line_end = self.filled;
                const line = firstRequestLine(self.buf[0..self.filled]);
                if (line.kind == .http09) return .http09;
            }

            if (b == '\r') {
                if (self.matched == 0 or self.matched == 2) {
                    self.matched += 1;
                } else if (self.matched == 1 or self.matched == 3) {
                    self.matched = 1;
                }
            } else if (b == '\n' and (self.matched == 1 or self.matched == 3)) {
                self.matched += 1;
                if (self.matched == 4) {
                    var consumed = i + 1;
                    if (test_mutation == .m1_short_consume and consumed > 0) {
                        consumed -= 1;
                    }
                    return .{ .head = .{ .consumed = consumed, .len = self.filled } };
                }
            } else {
                self.matched = 0;
            }
        }
        return .need_more;
    }

    pub fn headBytes(self: Accumulator) []const u8 {
        return self.buf[0..self.filled];
    }
};

const FirstLineKind = enum { http09, other };

fn firstRequestLine(buf: []const u8) struct { kind: FirstLineKind } {
    // Skip one leading CRLF (h1.accept.leading_crlf).
    var start: usize = 0;
    if (buf.len >= 2 and buf[0] == '\r' and buf[1] == '\n') start = 2;
    const line_end = std.mem.findPos(u8, buf, start, "\r\n") orelse return .{ .kind = .other };
    const line = buf[start..line_end];
    var spaces: usize = 0;
    for (line) |c| {
        if (c == ' ') spaces += 1;
    }
    if (spaces < 2) return .{ .kind = .http09 };
    return .{ .kind = .other };
}

pub const Head = struct {
    method_raw: []const u8,
    target: []const u8,
    path: []const u8,
    query: []const u8,
    authority: []const u8,
    version: Version,
    headers: []const Header,
    content_length: u64,
    has_content_length: bool,
    connection_close: bool,
    expect_100: bool,
    /// Bytes of the complete head including `\r\n\r\n`.
    head_len: usize,
};

pub const ParseResult = union(enum) {
    ok: Head,
    reject: u16,
    http09,
};

pub fn parse(head: []const u8, storage: []Header, field_scratch: []hpack.HeaderField, limits: Limits) ParseResult {
    if (head.len > limits.head_bytes) return .{ .reject = 431 };
    if (head.len < 4 or !std.mem.endsWith(u8, head, "\r\n\r\n")) return .{ .reject = 400 };

    var start: usize = 0;
    if (std.mem.startsWith(u8, head, "\r\n")) start = 2;

    const rest = head[start..];
    const line_end = std.mem.findPos(u8, rest, 0, "\r\n") orelse return .{ .reject = 400 };
    const line = rest[0..line_end];
    if (line.len == 0) return .{ .reject = 400 };

    var spaces: usize = 0;
    for (line) |c| {
        if (c == ' ') spaces += 1;
    }
    if (spaces < 2) return .http09;
    if (spaces > 2) return .{ .reject = 400 };

    const method_end = std.mem.findScalar(u8, line, ' ') orelse return .{ .reject = 400 };
    const method = line[0..method_end];
    if (method.len == 0 or !isToken(method)) return .{ .reject = 400 };

    const after_method = line[method_end + 1 ..];
    const target_end = std.mem.findScalar(u8, after_method, ' ') orelse return .{ .reject = 400 };
    if (target_end == 0) return .{ .reject = 400 };
    const target = after_method[0..target_end];
    for (target) |c| {
        if (c <= 0x20) return .{ .reject = 400 };
    }

    const version_tok = after_method[target_end + 1 ..];
    const version = parseVersion(version_tok) orelse {
        if (std.mem.startsWith(u8, version_tok, "HTTP/") and version_tok.len >= 8 and version_tok[5] != '1') {
            return .{ .reject = 505 };
        }
        // A version token is present (two SPs on the request line). A token
        // that is not HTTP/1.x is a malformed version, not HTTP/0.9.
        return .{ .reject = 400 };
    };

    if (std.mem.eql(u8, method, "CONNECT")) return .{ .reject = 501 };

    var path: []const u8 = target;
    var query: []const u8 = "";
    var authority_from_target: []const u8 = "";

    if (target.len == 1 and target[0] == '*') {
        path = target;
    } else if (isAbsoluteForm(target)) {
        const abs = splitAbsoluteForm(target) orelse return .{ .reject = 400 };
        path = abs.path;
        query = abs.query;
        authority_from_target = abs.authority;
    } else if (target.len == 0 or target[0] != '/') {
        return .{ .reject = 400 };
    } else if (std.mem.findScalar(u8, target, '?')) |q| {
        path = target[0..q];
        query = target[q + 1 ..];
    }

    const fields_start = start + line_end + 2;
    const scanned = scanFields(head, fields_start, storage, limits) orelse
        return .{ .reject = 400 };
    if (scanned.status) |st| return .{ .reject = st };

    // RFC 9112: absolute-form request-target authority replaces Host.
    const host = if (authority_from_target.len != 0) authority_from_target else scanned.host;
    if (host.len != 0 and !hostOk(host)) return .{ .reject = 400 };

    if (version == .http11 and host.len == 0) return .{ .reject = 400 };

    const result: Head = .{
        .method_raw = method,
        .target = target,
        .path = path,
        .query = query,
        .authority = host,
        .version = version,
        .headers = scanned.headers,
        .content_length = scanned.content_length orelse 0,
        .has_content_length = scanned.content_length != null,
        .connection_close = scanned.connection_close or version == .http10,
        .expect_100 = scanned.expect_100,
        .head_len = head.len,
    };
    _ = field_scratch;
    return .{ .ok = result };
}

/// Build the `validateRequestFields` input: lowercase names, hop-by-hop
/// stripped, synthetic `:method` / `:scheme` / `:authority` / `:path`.
pub const Validated = struct {
    status: u16,
    field_n: usize = 0,
    path: []const u8 = "",
    query: []const u8 = "",
    authority: []const u8 = "",
};

/// Returns `.status == 0` on success. `field_scratch[4..field_n]` are the
/// lowercase regular headers after hop-by-hop strip.
pub fn validateAsH2Fields(
    head: Head,
    scheme: []const u8,
    field_scratch: []hpack.HeaderField,
    name_scratch: []u8,
    path_q_buf: []u8,
) Validated {
    var n: usize = 0;
    if (n + 4 > field_scratch.len) return .{ .status = 431 };

    field_scratch[n] = .{ .name = ":method", .value = head.method_raw };
    n += 1;
    field_scratch[n] = .{ .name = ":scheme", .value = scheme };
    n += 1;
    field_scratch[n] = .{ .name = ":authority", .value = head.authority };
    n += 1;
    field_scratch[n] = .{ .name = ":path", .value = colonPath(head, path_q_buf) };
    n += 1;

    var name_off: usize = 0;
    for (head.headers) |h| {
        if (isHopByHop(h.name)) continue;
        if (n >= field_scratch.len) return .{ .status = 431 };
        if (name_off + h.name.len > name_scratch.len) return .{ .status = 431 };
        const lower = name_scratch[name_off..][0..h.name.len];
        _ = std.ascii.lowerString(lower, h.name);
        name_off += h.name.len;
        field_scratch[n] = .{ .name = lower, .value = h.value };
        n += 1;
    }

    const v = fields_mod.validateRequestFields(field_scratch[0..n]) catch |err| return switch (err) {
        error.PathTooLong => .{ .status = 400, .field_n = n },
        error.HeaderTooLarge => .{ .status = 431, .field_n = n },
        error.ProtocolError => .{ .status = 400, .field_n = n },
    };
    return .{
        .status = 0,
        .field_n = n,
        .path = v.path,
        .query = v.query,
        .authority = v.authority,
    };
}

pub fn colonPath(head: Head, buf: []u8) []const u8 {
    if (head.query.len == 0) return head.path;
    if (head.target.len > 0 and head.target[0] == '/') return head.target;
    const need = head.path.len + 1 + head.query.len;
    if (buf.len < need) return head.path;
    @memcpy(buf[0..head.path.len], head.path);
    buf[head.path.len] = '?';
    @memcpy(buf[head.path.len + 1 ..][0..head.query.len], head.query);
    return buf[0..need];
}

const Scanned = struct {
    headers: []const Header,
    content_length: ?u64,
    host: []const u8,
    connection_close: bool,
    expect_100: bool,
    status: ?u16,
};

fn scanFields(head: []const u8, start: usize, storage: []Header, limits: Limits) ?Scanned {
    var n: usize = 0;
    var content_length: ?u64 = null;
    var host: []const u8 = "";
    var host_count: usize = 0;
    var connection_close = false;
    var expect_100 = false;
    var saw_te = false;
    var te_is_chunked_only = true;
    var pos = start;
    while (pos + 2 <= head.len) {
        const next = std.mem.findPos(u8, head, pos, "\r\n") orelse return null;
        const line = head[pos..next];
        if (line.len == 0) {
            if (next + 2 != head.len) return null;
            break;
        }
        if (line[0] == ' ' or line[0] == '\t') return .{
            .headers = storage[0..n],
            .content_length = content_length,
            .host = host,
            .connection_close = connection_close,
            .expect_100 = expect_100,
            .status = 400,
        };
        const colon = std.mem.findScalar(u8, line, ':') orelse return .{
            .headers = storage[0..n],
            .content_length = content_length,
            .host = host,
            .connection_close = connection_close,
            .expect_100 = expect_100,
            .status = 400,
        };
        const name = line[0..colon];
        if (name.len == 0 or !isToken(name)) return .{
            .headers = storage[0..n],
            .content_length = content_length,
            .host = host,
            .connection_close = connection_close,
            .expect_100 = expect_100,
            .status = 400,
        };
        const value = trimOws(line[colon + 1 ..]);
        if (!valueOk(value)) return .{
            .headers = storage[0..n],
            .content_length = content_length,
            .host = host,
            .connection_close = connection_close,
            .expect_100 = expect_100,
            .status = 400,
        };
        if (n >= storage.len or n >= limits.header_fields) return .{
            .headers = storage[0..n],
            .content_length = content_length,
            .host = host,
            .connection_close = connection_close,
            .expect_100 = expect_100,
            .status = 431,
        };
        storage[n] = .{ .name = name, .value = value };
        n += 1;

        if (eqlIgnoreCase(name, "content-length")) {
            const parsed = parseContentLength(value) orelse return .{
                .headers = storage[0..n],
                .content_length = content_length,
                .host = host,
                .connection_close = connection_close,
                .expect_100 = expect_100,
                .status = 400,
            };
            if (content_length) |cl| {
                if (cl != parsed) return .{
                    .headers = storage[0..n],
                    .content_length = content_length,
                    .host = host,
                    .connection_close = connection_close,
                    .expect_100 = expect_100,
                    .status = 400,
                };
            } else content_length = parsed;
        } else if (eqlIgnoreCase(name, "transfer-encoding")) {
            saw_te = true;
            if (!isChunkedOnly(value)) te_is_chunked_only = false;
        } else if (eqlIgnoreCase(name, "host")) {
            host_count += 1;
            if (host_count > 1 or value.len == 0) return .{
                .headers = storage[0..n],
                .content_length = content_length,
                .host = host,
                .connection_close = connection_close,
                .expect_100 = expect_100,
                .status = 400,
            };
            if (!hostOk(value)) return .{
                .headers = storage[0..n],
                .content_length = content_length,
                .host = host,
                .connection_close = connection_close,
                .expect_100 = expect_100,
                .status = 400,
            };
            host = value;
        } else if (eqlIgnoreCase(name, "connection")) {
            if (connectionHasClose(value)) connection_close = true;
        } else if (eqlIgnoreCase(name, "expect")) {
            if (!eqlIgnoreCase(value, "100-continue")) return .{
                .headers = storage[0..n],
                .content_length = content_length,
                .host = host,
                .connection_close = connection_close,
                .expect_100 = expect_100,
                .status = 417,
            };
            expect_100 = true;
        }
        pos = next + 2;
    }

    if (saw_te and content_length != null) return .{
        .headers = storage[0..n],
        .content_length = content_length,
        .host = host,
        .connection_close = true,
        .expect_100 = expect_100,
        .status = 400,
    };
    if (saw_te and te_is_chunked_only) return .{
        .headers = storage[0..n],
        .content_length = content_length,
        .host = host,
        .connection_close = true,
        .expect_100 = expect_100,
        .status = 411,
    };
    if (saw_te) return .{
        .headers = storage[0..n],
        .content_length = content_length,
        .host = host,
        .connection_close = true,
        .expect_100 = expect_100,
        .status = 501,
    };

    return .{
        .headers = storage[0..n],
        .content_length = content_length,
        .host = host,
        .connection_close = connection_close,
        .expect_100 = expect_100,
        .status = null,
    };
}

fn hostOk(h: []const u8) bool {
    if (h.len == 0) return false;
    for (h) |c| {
        if (c == ',' or c == ' ' or c == '\t' or c < 0x21 or c == 0x7f) return false;
    }
    return true;
}

fn parseVersion(tok: []const u8) ?Version {
    if (tok.len < 8) return null;
    if (!std.mem.startsWith(u8, tok, "HTTP/")) return null;
    if (tok[5] != '1' or tok[6] != '.') return null;
    if (tok.len != 8) return null;
    return switch (tok[7]) {
        '0' => .http10,
        '1' => .http11,
        else => null,
    };
}

fn isAbsoluteForm(target: []const u8) bool {
    return std.ascii.startsWithIgnoreCase(target, "http://") or
        std.ascii.startsWithIgnoreCase(target, "https://");
}

const Absolute = struct { authority: []const u8, path: []const u8, query: []const u8 };

fn splitAbsoluteForm(target: []const u8) ?Absolute {
    const scheme_end = std.mem.findPos(u8, target, 0, "://") orelse return null;
    const after = target[scheme_end + 3 ..];
    if (after.len == 0) return null;
    const slash = std.mem.findScalar(u8, after, '/') orelse {
        const q = std.mem.findScalar(u8, after, '?');
        if (q) |qi| {
            return .{ .authority = after[0..qi], .path = "/", .query = after[qi + 1 ..] };
        }
        return .{ .authority = after, .path = "/", .query = "" };
    };
    const authority = after[0..slash];
    if (authority.len == 0) return null;
    const path_and_query = after[slash..];
    if (std.mem.findScalar(u8, path_and_query, '?')) |qi| {
        return .{ .authority = authority, .path = path_and_query[0..qi], .query = path_and_query[qi + 1 ..] };
    }
    return .{ .authority = authority, .path = path_and_query, .query = "" };
}

fn isHopByHop(name: []const u8) bool {
    return eqlIgnoreCase(name, "connection") or
        eqlIgnoreCase(name, "keep-alive") or
        eqlIgnoreCase(name, "proxy-connection") or
        eqlIgnoreCase(name, "transfer-encoding") or
        eqlIgnoreCase(name, "upgrade") or
        eqlIgnoreCase(name, "host") or
        eqlIgnoreCase(name, "expect");
}

fn isChunkedOnly(value: []const u8) bool {
    var it = std.mem.splitScalar(u8, value, ',');
    var n: usize = 0;
    while (it.next()) |part| {
        const t = trimOws(part);
        if (t.len == 0) continue;
        n += 1;
        if (!eqlIgnoreCase(t, "chunked")) return false;
    }
    return n == 1;
}

fn parseContentLength(value: []const u8) ?u64 {
    if (value.len == 0) return null;
    if (test_mutation == .m8_lenient_cl) {
        return std.fmt.parseInt(u64, trimOws(value), 10) catch null;
    }
    for (value) |c| {
        if (c < '0' or c > '9') return null;
    }
    return std.fmt.parseInt(u64, value, 10) catch null;
}

fn connectionHasClose(value: []const u8) bool {
    var it = std.mem.splitScalar(u8, value, ',');
    while (it.next()) |part| {
        if (eqlIgnoreCase(trimOws(part), "close")) return true;
    }
    return false;
}

fn trimOws(s: []const u8) []const u8 {
    var start: usize = 0;
    var end = s.len;
    while (start < end and isOws(s[start])) start += 1;
    while (end > start and isOws(s[end - 1])) end -= 1;
    return s[start..end];
}

fn isOws(c: u8) bool {
    return c == ' ' or c == '\t';
}

fn valueOk(v: []const u8) bool {
    for (v) |c| {
        if (c == '\t') continue;
        if (c < 0x20 or c == 0x7f) return false;
    }
    return true;
}

fn isToken(s: []const u8) bool {
    if (s.len == 0) return false;
    for (s) |c| {
        if (!isTchar(c)) return false;
    }
    return true;
}

fn isTchar(c: u8) bool {
    return switch (c) {
        '0'...'9',
        'A'...'Z',
        'a'...'z',
        '!',
        '#',
        '$',
        '%',
        '&',
        '\'',
        '*',
        '+',
        '-',
        '.',
        '^',
        '_',
        '`',
        '|',
        '~',
        => true,
        else => false,
    };
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

pub fn reasonPhrase(status: u16) []const u8 {
    return switch (status) {
        100 => "Continue",
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        304 => "Not Modified",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        411 => "Length Required",
        413 => "Content Too Large",
        417 => "Expectation Failed",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        505 => "HTTP Version Not Supported",
        else => "",
    };
}

fn parseOne(head: []const u8) ParseResult {
    var storage: [32]Header = undefined;
    var scratch: [36]hpack.HeaderField = undefined;
    return parse(head, &storage, &scratch, .{
        .head_bytes = 16 * 1024,
        .header_fields = 32,
        .body_bytes = 256 * 1024,
    });
}

fn expectReject(head: []const u8, status: u16) !void {
    switch (parseOne(head)) {
        .reject => |st| try std.testing.expectEqual(status, st),
        else => return error.ExpectedReject,
    }
}

fn expectOk(head: []const u8) !Head {
    switch (parseOne(head)) {
        .ok => |h| return h,
        .reject => |st| {
            std.debug.print("unexpected reject {d}\n", .{st});
            return error.ExpectedOk;
        },
        .http09 => return error.ExpectedOk,
    }
}

test "cut: chunked request is 411" {
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n", 411);
}

test "cut: te gzip is 501" {
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: gzip\r\n\r\n", 501);
}

test "cut: te + cl is 400" {
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nContent-Length: 1\r\n\r\n", 400);
}

test "cut: unequal dup cl is 400" {
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n", 400);
}

test "cut: nonnumeric cl is 400" {
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: +5\r\n\r\n", 400);
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: -5\r\n\r\n", 400);
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 0x5\r\n\r\n", 400);
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5 5\r\n\r\n", 400);
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5,5\r\n\r\n", 400);
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 18446744073709551617\r\n\r\n", 400);
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: \r\n\r\n", 400);
}

test "cut: obs-fold is 400" {
    try expectReject("GET / HTTP/1.1\r\nHost: h\r\nX: a\r\n b\r\n\r\n", 400);
}

test "cut: bare lf is 400" {
    var acc = Accumulator.init(&buf_scratch);
    const r = acc.feed("GET / HTTP/1.1\nHost: h\r\n\r\n");
    switch (r) {
        .reject => |rj| try std.testing.expectEqual(@as(u16, 400), rj.status),
        else => return error.ExpectedBareLfReject,
    }
}

test "cut: ws before colon is 400" {
    try expectReject("GET / HTTP/1.1\r\nHost : h\r\n\r\n", 400);
}

test "cut: ctl in value is 400" {
    try expectReject("GET / HTTP/1.1\r\nHost: h\r\nX: a\x01b\r\n\r\n", 400);
}

test "cut: space in target is 400" {
    try expectReject("GET /foo bar HTTP/1.1\r\nHost: h\r\n\r\n", 400);
}

test "cut: no host on 1.1 is 400" {
    try expectReject("GET / HTTP/1.1\r\n\r\n", 400);
}

test "cut: dup host is 400" {
    try expectReject("GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n", 400);
}

test "cut: CONNECT is 501" {
    try expectReject("CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\n\r\n", 501);
}

test "cut: HTTP/2.0 is 505" {
    try expectReject("GET / HTTP/2.0\r\nHost: h\r\n\r\n", 505);
}

test "cut: Expect other is 417" {
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 0\r\nExpect: 100-wait\r\n\r\n", 417);
}

test "cut: te casing still 411" {
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunKed\r\n\r\n", 411);
    try expectReject("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n", 411);
}

test "accept: origin-form GET" {
    const h = try expectOk("GET /v1/tasks/t-7?wait=1 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    try std.testing.expectEqualStrings("GET", h.method_raw);
    try std.testing.expectEqualStrings("/v1/tasks/t-7", h.path);
    try std.testing.expectEqualStrings("wait=1", h.query);
    try std.testing.expectEqualStrings("127.0.0.1", h.authority);
    try std.testing.expect(h.version == .http11);
}

test "accept: absolute-form reduces to path + authority" {
    const h = try expectOk("GET http://host/path?q=1 HTTP/1.1\r\nHost: host\r\n\r\n");
    try std.testing.expectEqualStrings("/path", h.path);
    try std.testing.expectEqualStrings("q=1", h.query);
    try std.testing.expectEqualStrings("host", h.authority);
}

test "accept: absolute-form authority replaces a mismatched Host" {
    const h = try expectOk("GET http://target.example/hello HTTP/1.1\r\nHost: other.example\r\n\r\n");
    try std.testing.expectEqualStrings("/hello", h.path);
    try std.testing.expectEqualStrings("target.example", h.authority);
}

test "cut: malformed version is 400 not HTTP/0.9" {
    try expectReject("GET / HTTX/1.1\r\nHost: h\r\n\r\n", 400);
}

test "cut: tab in request-target is 400" {
    try expectReject("GET /\tfoo HTTP/1.1\r\nHost: h\r\n\r\n", 400);
}

test "cut: comma in Host is 400" {
    try expectReject("GET / HTTP/1.1\r\nHost: good.example,evil.example\r\n\r\n", 400);
}

test "accept: leading CRLF" {
    const h = try expectOk("\r\nGET / HTTP/1.1\r\nHost: h\r\n\r\n");
    try std.testing.expectEqualStrings("/", h.path);
}

test "accept: HTTP/1.0 has no Host requirement" {
    const h = try expectOk("OPTIONS / HTTP/1.0\r\n\r\n");
    try std.testing.expect(h.version == .http10);
    try std.testing.expect(h.connection_close);
}

test "accept: Expect 100-continue" {
    const h = try expectOk("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n");
    try std.testing.expect(h.expect_100);
    try std.testing.expectEqual(@as(u64, 4), h.content_length);
}

test "accept: Upgrade is ignored at parse" {
    const h = try expectOk("GET / HTTP/1.1\r\nHost: h\r\nUpgrade: websocket\r\n\r\n");
    try std.testing.expectEqualStrings("GET", h.method_raw);
}

test "http09: no version token" {
    var acc = Accumulator.init(&buf_scratch);
    const r = acc.feed("GET /\r\n");
    try std.testing.expect(r == .http09);
}

test "I1: consumed includes terminator" {
    var acc = Accumulator.init(&buf_scratch);
    const data = "GET / HTTP/1.1\r\nHost: h\r\n\r\nBODY";
    const r = acc.feed(data);
    switch (r) {
        .head => |h| {
            try std.testing.expectEqual(@as(usize, data.len - 4), h.consumed);
            try std.testing.expectEqual(h.consumed, h.len);
        },
        else => return error.ExpectedHead,
    }
}

test "head overflow is 431 and does not grow" {
    var tiny: [8]u8 = undefined;
    var acc = Accumulator.init(&tiny);
    const r = acc.feed("GET / HTTP/1.1\r\nHost: h\r\n\r\n");
    switch (r) {
        .reject => |rj| try std.testing.expectEqual(@as(u16, 431), rj.status),
        else => return error.Expected431,
    }
}

test "validateRequestFields is called on the h1 field list" {
    const h = try expectOk("GET / HTTP/1.1\r\nHost: h\r\nX-A: 1\r\n\r\n");
    var scratch: [16]hpack.HeaderField = undefined;
    var names: [64]u8 = undefined;
    var path_q: [64]u8 = undefined;
    try std.testing.expectEqual(@as(u16, 0), validateAsH2Fields(h, "http", &scratch, &names, &path_q).status);
}

var buf_scratch: [4096]u8 = undefined;
