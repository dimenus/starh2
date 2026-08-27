//! HTTP/1.1 request and response framing. No I/O.
//!
//! This is a different parser from the HTTP/2 frame/session path. HTTP/1.1
//! messages are a request or status line, a header block, and a body whose
//! length is `Content-Length`. That is the only body delimiter this slice
//! understands.
//!
//! `Transfer-Encoding` is rejected. A final response without `Content-Length`
//! is rejected rather than delimited by connection close. Waiting on peer close
//! is the defect this module exists to replace: a client that does that hangs
//! until a watchdog, while curl finishes because it trusts the length.
//!
//! First slice: no chunked encoding, no pipelining, no `CONNECT`, no
//! `100-continue`, no keep-alive reuse. `Connection: close` is always written.
const std = @import("std");
const request = @import("../http/request.zig");

pub const Header = request.Header;

pub const Error = error{
    ProtocolError,
    HeaderTooLarge,
    TooManyHeaders,
    MissingContentLength,
    MissingHost,
    BodyTooLarge,
    InvalidContentLength,
};

pub const Limits = struct {
    header_bytes: usize = 32 * 1024,
    header_fields: usize = 100,
    body_bytes: usize = 256 * 1024,
};

/// Incremental scan for the `\r\n\r\n` that ends a head. `feed` returns how
/// many of the supplied bytes belong to the head, including the terminator
/// once it is complete.
pub const HeadEnd = struct {
    matched: u8 = 0,

    pub fn feed(self: *HeadEnd, bytes: []const u8) usize {
        const want = "\r\n\r\n";
        var i: usize = 0;
        while (i < bytes.len) : (i += 1) {
            const b = bytes[i];
            if (b == want[self.matched]) {
                self.matched += 1;
                if (self.matched == 4) return i + 1;
            } else if (b == '\r') {
                self.matched = 1;
            } else {
                self.matched = 0;
            }
        }
        return bytes.len;
    }

    pub fn done(self: HeadEnd) bool {
        return self.matched == 4;
    }
};

pub const RequestHead = struct {
    method_raw: []const u8,
    target: []const u8,
    path: []const u8,
    query: []const u8,
    version: []const u8,
    headers: []const Header,
    content_length: ?u64,
    connection_close: bool,
    host: []const u8,
};

pub const ResponseHead = struct {
    version: []const u8,
    status: u16,
    reason: []const u8,
    headers: []const Header,
    content_length: ?u64,
    connection_close: bool,
};

pub fn isNoBodyStatus(status: u16) bool {
    return status < 200 or status == 204 or status == 304;
}

pub fn methodRequiresLength(method: []const u8) bool {
    return std.mem.eql(u8, method, "POST") or
        std.mem.eql(u8, method, "PUT") or
        std.mem.eql(u8, method, "PATCH");
}

pub fn parseRequestHead(head: []const u8, storage: []Header, limits: Limits) Error!RequestHead {
    if (head.len > limits.header_bytes) return error.HeaderTooLarge;
    if (!std.mem.endsWith(u8, head, "\r\n\r\n")) return error.ProtocolError;

    const line_end = std.mem.findPos(u8, head, 0, "\r\n") orelse return error.ProtocolError;
    const line = head[0..line_end];
    const method_end = std.mem.findScalar(u8, line, ' ') orelse return error.ProtocolError;
    const method = line[0..method_end];
    if (method.len == 0 or !isToken(method)) return error.ProtocolError;

    const after_method = line[method_end + 1 ..];
    const target_end = std.mem.findScalar(u8, after_method, ' ') orelse return error.ProtocolError;
    if (target_end == 0) return error.ProtocolError;
    const target = after_method[0..target_end];
    if (target[0] != '/') return error.ProtocolError;

    const version = after_method[target_end + 1 ..];
    if (!std.mem.eql(u8, version, "HTTP/1.1")) return error.ProtocolError;

    var path = target;
    var query: []const u8 = "";
    if (std.mem.findScalar(u8, target, '?')) |q| {
        path = target[0..q];
        query = target[q + 1 ..];
    }

    const fields = try parseFields(head, line_end + 2, storage, limits);
    if (fields.host.len == 0) return error.MissingHost;
    return .{
        .method_raw = method,
        .target = target,
        .path = path,
        .query = query,
        .version = version,
        .headers = fields.headers,
        .content_length = fields.content_length,
        .connection_close = fields.connection_close,
        .host = fields.host,
    };
}

pub fn parseResponseHead(head: []const u8, storage: []Header, limits: Limits) Error!ResponseHead {
    if (head.len > limits.header_bytes) return error.HeaderTooLarge;
    if (!std.mem.endsWith(u8, head, "\r\n\r\n")) return error.ProtocolError;

    const line_end = std.mem.findPos(u8, head, 0, "\r\n") orelse return error.ProtocolError;
    const line = head[0..line_end];
    if (line.len < 12) return error.ProtocolError;
    const version = line[0..8];
    if (!std.mem.eql(u8, version, "HTTP/1.1")) return error.ProtocolError;
    if (line[8] != ' ') return error.ProtocolError;
    const status = std.fmt.parseInt(u16, line[9..12], 10) catch return error.ProtocolError;
    if (status < 100 or status > 599) return error.ProtocolError;
    var reason: []const u8 = "";
    if (line.len > 12) {
        if (line[12] != ' ') return error.ProtocolError;
        reason = line[13..];
    }

    const fields = try parseFields(head, line_end + 2, storage, limits);
    if (!isNoBodyStatus(status) and fields.content_length == null) return error.MissingContentLength;
    return .{
        .version = version,
        .status = status,
        .reason = reason,
        .headers = fields.headers,
        .content_length = fields.content_length,
        .connection_close = fields.connection_close,
    };
}

pub fn writeRequest(
    w: *std.Io.Writer,
    method: []const u8,
    target: []const u8,
    host: []const u8,
    headers: []const Header,
    body: []const u8,
) std.Io.Writer.Error!void {
    try w.print("{s} {s} HTTP/1.1\r\n", .{ method, target });
    try w.print("host: {s}\r\n", .{host});
    try w.print("content-length: {d}\r\n", .{body.len});
    try w.writeAll("connection: close\r\n");
    try writeExtraHeaders(w, headers);
    try w.writeAll("\r\n");
    if (body.len != 0) try w.writeAll(body);
}

pub fn writeResponse(
    w: *std.Io.Writer,
    status: u16,
    headers: []const Header,
    body: []const u8,
) std.Io.Writer.Error!void {
    try w.print("HTTP/1.1 {d} {s}\r\n", .{ status, reasonPhrase(status) });
    try w.print("content-length: {d}\r\n", .{body.len});
    try w.writeAll("connection: close\r\n");
    try writeExtraHeaders(w, headers);
    try w.writeAll("\r\n");
    if (body.len != 0) try w.writeAll(body);
}

pub fn reasonPhrase(status: u16) []const u8 {
    return switch (status) {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        411 => "Length Required",
        413 => "Content Too Large",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        else => "",
    };
}

const ParsedFields = struct {
    headers: []const Header,
    content_length: ?u64,
    connection_close: bool,
    host: []const u8,
};

fn parseFields(head: []const u8, start: usize, storage: []Header, limits: Limits) Error!ParsedFields {
    var n: usize = 0;
    var content_length: ?u64 = null;
    var connection_close = false;
    var host: []const u8 = "";
    var pos = start;
    while (pos + 2 <= head.len) {
        const next = std.mem.findPos(u8, head, pos, "\r\n") orelse return error.ProtocolError;
        const line = head[pos..next];
        if (line.len == 0) {
            if (next + 2 != head.len) return error.ProtocolError;
            break;
        }
        if (line[0] == ' ' or line[0] == '\t') return error.ProtocolError;
        const colon = std.mem.findScalar(u8, line, ':') orelse return error.ProtocolError;
        const name = line[0..colon];
        if (name.len == 0 or !isToken(name)) return error.ProtocolError;
        const value = trimOws(line[colon + 1 ..]);
        if (!valueOk(value)) return error.ProtocolError;
        if (n >= storage.len or n >= limits.header_fields) return error.TooManyHeaders;
        storage[n] = .{ .name = name, .value = value };
        n += 1;

        if (eqlIgnoreCase(name, "content-length")) {
            const parsed = parseContentLength(value) orelse return error.InvalidContentLength;
            if (content_length) |cl| {
                if (cl != parsed) return error.InvalidContentLength;
            } else content_length = parsed;
        } else if (eqlIgnoreCase(name, "transfer-encoding")) {
            return error.ProtocolError;
        } else if (eqlIgnoreCase(name, "host")) {
            if (host.len != 0 or value.len == 0) return error.ProtocolError;
            host = value;
        } else if (eqlIgnoreCase(name, "connection")) {
            if (connectionHasClose(value)) connection_close = true;
        }
        pos = next + 2;
    }
    return .{
        .headers = storage[0..n],
        .content_length = content_length,
        .connection_close = connection_close,
        .host = host,
    };
}

fn writeExtraHeaders(w: *std.Io.Writer, headers: []const Header) std.Io.Writer.Error!void {
    for (headers) |h| {
        if (isReservedHeader(h.name)) continue;
        try w.print("{s}: {s}\r\n", .{ h.name, h.value });
    }
}

fn isReservedHeader(name: []const u8) bool {
    return eqlIgnoreCase(name, "host") or
        eqlIgnoreCase(name, "content-length") or
        eqlIgnoreCase(name, "connection") or
        eqlIgnoreCase(name, "transfer-encoding");
}

fn parseContentLength(value: []const u8) ?u64 {
    if (value.len == 0) return null;
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
        if (c == 0 or c == '\r' or c == '\n') return false;
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

fn dump(gpa: std.mem.Allocator, write: *const fn (*std.Io.Writer) std.Io.Writer.Error!void) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(gpa);
    errdefer aw.deinit();
    try write(&aw.writer);
    return aw.toOwnedSlice();
}

test "HeadEnd finds CRLFCRLF across feeds" {
    const data = "GET / HTTP/1.1\r\nHost: x\r\n\r\nBODY";
    var i: usize = 0;
    while (i < 35) : (i += 1) {
        var p: HeadEnd = .{};
        const a = p.feed(data[0..i]);
        try std.testing.expectEqual(i, a);
        try std.testing.expect(!p.done());
        const b = p.feed(data[i..]);
        try std.testing.expect(p.done());
        try std.testing.expectEqual(@as(usize, 35 - i), b);
    }
}

test "parse GET origin-form and Host" {
    const head = "GET /v1/tasks/t-7?wait=1 HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/plain\r\n\r\n";
    var storage: [8]Header = undefined;
    const req = try parseRequestHead(head, &storage, .{});
    try std.testing.expectEqualStrings("GET", req.method_raw);
    try std.testing.expectEqualStrings("/v1/tasks/t-7?wait=1", req.target);
    try std.testing.expectEqualStrings("/v1/tasks/t-7", req.path);
    try std.testing.expectEqualStrings("wait=1", req.query);
    try std.testing.expectEqualStrings("127.0.0.1", req.host);
    try std.testing.expect(req.content_length == null);
    try std.testing.expect(!req.connection_close);
    try std.testing.expectEqual(@as(usize, 2), req.headers.len);
}

test "parse response Content-Length without waiting for close" {
    const head = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n";
    var storage: [4]Header = undefined;
    const resp = try parseResponseHead(head, &storage, .{});
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(u64, 2), resp.content_length.?);
    try std.testing.expect(!resp.connection_close);
}

test "response without Content-Length fails closed" {
    const head = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
    var storage: [4]Header = undefined;
    try std.testing.expectError(error.MissingContentLength, parseResponseHead(head, &storage, .{}));
}

test "204 may omit Content-Length" {
    const head = "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n";
    var storage: [4]Header = undefined;
    const resp = try parseResponseHead(head, &storage, .{});
    try std.testing.expectEqual(@as(u16, 204), resp.status);
    try std.testing.expect(resp.content_length == null);
    try std.testing.expect(resp.connection_close);
}

test "Transfer-Encoding is rejected" {
    const head = "POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n";
    var storage: [8]Header = undefined;
    try std.testing.expectError(error.ProtocolError, parseRequestHead(head, &storage, .{}));
}

test "missing Host fails closed" {
    const head = "GET / HTTP/1.1\r\n\r\n";
    var storage: [4]Header = undefined;
    try std.testing.expectError(error.MissingHost, parseRequestHead(head, &storage, .{}));
}

test "huge header block fails closed" {
    const head = "GET / HTTP/1.1\r\nHost: h\r\n\r\n";
    var storage: [4]Header = undefined;
    try std.testing.expectError(error.HeaderTooLarge, parseRequestHead(head, &storage, .{ .header_bytes = 8 }));
}

test "too many header fields fails closed" {
    const head = "GET / HTTP/1.1\r\nHost: h\r\nX: 1\r\nY: 2\r\n\r\n";
    var storage: [8]Header = undefined;
    try std.testing.expectError(error.TooManyHeaders, parseRequestHead(head, &storage, .{ .header_fields = 2 }));
}

test "CR in a field value is rejected" {
    const head = "GET / HTTP/1.1\r\nHost: h\r\nX: a\rb\r\n\r\n";
    var storage: [4]Header = undefined;
    try std.testing.expectError(error.ProtocolError, parseRequestHead(head, &storage, .{}));
}

test "obs-fold is rejected" {
    const head = "GET / HTTP/1.1\r\nHost: h\r\nX: a\r\n b\r\n\r\n";
    var storage: [4]Header = undefined;
    try std.testing.expectError(error.ProtocolError, parseRequestHead(head, &storage, .{}));
}

test "conflicting Content-Length fails closed" {
    const head = "POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n";
    var storage: [8]Header = undefined;
    try std.testing.expectError(error.InvalidContentLength, parseRequestHead(head, &storage, .{}));
}

test "Connection close token is detected" {
    const head = "GET / HTTP/1.1\r\nHost: h\r\nConnection: keep-alive, close\r\n\r\n";
    var storage: [4]Header = undefined;
    const req = try parseRequestHead(head, &storage, .{});
    try std.testing.expect(req.connection_close);
}

test "writeResponse always frames with Content-Length and close" {
    const gpa = std.testing.allocator;
    const bytes = try dump(gpa, struct {
        fn f(w: *std.Io.Writer) std.Io.Writer.Error!void {
            try writeResponse(w, 200, &.{.{ .name = "content-type", .value = "text/plain" }}, "ok");
        }
    }.f);
    defer gpa.free(bytes);
    try std.testing.expectEqualStrings(
        "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\ncontent-type: text/plain\r\n\r\nok",
        bytes,
    );
    var storage: [8]Header = undefined;
    const crlf = std.mem.findPos(u8, bytes, 0, "\r\n\r\n").?;
    const resp = try parseResponseHead(bytes[0 .. crlf + 4], &storage, .{});
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(u64, 2), resp.content_length.?);
    try std.testing.expect(resp.connection_close);
}

test "writeRequest is origin-form HTTP/1.1 with Host" {
    const gpa = std.testing.allocator;
    const bytes = try dump(gpa, struct {
        fn f(w: *std.Io.Writer) std.Io.Writer.Error!void {
            try writeRequest(w, "GET", "/v1/tasks/t-7", "127.0.0.1", &.{}, "");
        }
    }.f);
    defer gpa.free(bytes);
    try std.testing.expectEqualStrings(
        "GET /v1/tasks/t-7 HTTP/1.1\r\nhost: 127.0.0.1\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        bytes,
    );
    var storage: [8]Header = undefined;
    const req = try parseRequestHead(bytes, &storage, .{});
    try std.testing.expectEqualStrings("GET", req.method_raw);
    try std.testing.expectEqualStrings("/v1/tasks/t-7", req.path);
    try std.testing.expectEqualStrings("127.0.0.1", req.host);
    try std.testing.expectEqual(@as(u64, 0), req.content_length.?);
    try std.testing.expect(req.connection_close);
}

test "writer-supplied framing headers are not duplicated" {
    const gpa = std.testing.allocator;
    const bytes = try dump(gpa, struct {
        fn f(w: *std.Io.Writer) std.Io.Writer.Error!void {
            try writeResponse(w, 200, &.{
                .{ .name = "Content-Length", .value = "99" },
                .{ .name = "Connection", .value = "keep-alive" },
            }, "ok");
        }
    }.f);
    defer gpa.free(bytes);
    try std.testing.expectEqualStrings(
        "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
        bytes,
    );
}
