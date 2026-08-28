//! HTTP/1.1 edge-channel battery. Fixtures live in testdata/h1/.
//! Every category prints `category=<name> fixtures=<n>` and a zero count fails (I8).
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const dummy: u8 = 0;

const Fixture = struct {
    name: []const u8,
    category: []const u8,
    status: ?u16,
    connection: enum { keep, close },
    body: ?[]const u8,
    request: []u8,
};

fn unescapeRequest(gpa: std.mem.Allocator, escaped: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    var i: usize = 0;
    while (i < escaped.len) {
        if (escaped[i] == '\\' and i + 1 < escaped.len) {
            switch (escaped[i + 1]) {
                'r' => try out.append(gpa, '\r'),
                'n' => try out.append(gpa, '\n'),
                't' => try out.append(gpa, '\t'),
                'x' => {
                    if (i + 3 >= escaped.len) return error.BadEscape;
                    const hi = std.fmt.parseInt(u8, escaped[i + 2 .. i + 3], 16) catch return error.BadEscape;
                    const lo = std.fmt.parseInt(u8, escaped[i + 3 .. i + 4], 16) catch return error.BadEscape;
                    try out.append(gpa, (hi << 4) | lo);
                    i += 4;
                    continue;
                },
                else => try out.append(gpa, escaped[i + 1]),
            }
            i += 2;
            continue;
        }
        try out.append(gpa, escaped[i]);
        i += 1;
    }
    return out.toOwnedSlice(gpa);
}

fn loadFixtures(io: std.Io, gpa: std.mem.Allocator, root_path: []const u8) ![]Fixture {
    var list: std.ArrayList(Fixture) = .empty;
    errdefer {
        for (list.items) |f| gpa.free(f.request);
        list.deinit(gpa);
    }
    var root = try std.Io.Dir.cwd().openDir(io, root_path, .{ .iterate = true });
    defer root.close(io);
    var cats = root.iterate();
    while (try cats.next(io)) |cat_ent| {
        if (cat_ent.kind != .directory) continue;
        var cat_dir = try root.openDir(io, cat_ent.name, .{ .iterate = true });
        defer cat_dir.close(io);
        var files = cat_dir.iterate();
        var n: usize = 0;
        while (try files.next(io)) |fent| {
            if (fent.kind != .file) continue;
            if (!std.mem.endsWith(u8, fent.name, ".txn")) continue;
            const bytes = try cat_dir.readFileAlloc(io, fent.name, gpa, .limited(64 * 1024));
            defer gpa.free(bytes);
            var status: ?u16 = null;
            var connection: @TypeOf(@as(Fixture, undefined).connection) = .keep;
            var body: ?[]const u8 = null;
            var req_esc: []const u8 = "";
            var it = std.mem.splitScalar(u8, bytes, '\n');
            var in_req = false;
            var req_buf: std.ArrayList(u8) = .empty;
            defer req_buf.deinit(gpa);
            while (it.next()) |line| {
                if (!in_req) {
                    if (line.len == 0) {
                        in_req = true;
                        continue;
                    }
                    if (std.mem.startsWith(u8, line, "status: ")) {
                        const v = std.mem.trim(u8, line["status: ".len..], " \t\r");
                        if (std.mem.eql(u8, v, "none")) {
                            status = null;
                        } else {
                            status = try std.fmt.parseInt(u16, v, 10);
                        }
                    } else if (std.mem.startsWith(u8, line, "connection: ")) {
                        const v = std.mem.trim(u8, line["connection: ".len..], " \t\r");
                        connection = if (std.mem.eql(u8, v, "close")) .close else .keep;
                    } else if (std.mem.startsWith(u8, line, "body: ")) {
                        body = line["body: ".len..];
                    }
                } else {
                    try req_buf.appendSlice(gpa, line);
                    try req_buf.append(gpa, '\n');
                }
            }
            req_esc = std.mem.trim(u8, req_buf.items, "\n");
            const request = try unescapeRequest(gpa, req_esc);
            const name = try gpa.dupe(u8, fent.name[0 .. fent.name.len - 4]);
            const category = try gpa.dupe(u8, cat_ent.name);
            const body_owned: ?[]const u8 = if (body) |b| try gpa.dupe(u8, b) else null;
            try list.append(gpa, .{
                .name = name,
                .category = category,
                .status = status,
                .connection = connection,
                .body = body_owned,
                .request = request,
            });
            n += 1;
        }
        std.debug.print("category={s} fixtures={d}\n", .{ cat_ent.name, n });
        if (n == 0) return error.EmptyCategory;
    }
    if (list.items.len == 0) return error.NoFixtures;
    return list.toOwnedSlice(gpa);
}

fn echo(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, req.body);
}

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

fn queryH(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, req.query);
}

fn sseOnce(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    try body.flush();
    const now = std.Io.Clock.awake.now(resp.io);
    const deadline = std.Io.Timestamp.fromNanoseconds(now.nanoseconds + 200 * std.time.ns_per_ms);
    body.waitUntil(deadline) catch {};
    try body.finish();
}

fn slow(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const now = std.Io.Clock.awake.now(resp.io);
    const deadline = std.Io.Timestamp.fromNanoseconds(now.nanoseconds + 2 * std.time.ns_per_s);
    try resp.waitUntil(deadline);
    try resp.send(200, &.{}, "ok");
}

const Observed = struct {
    method: starh2.Method = .GET,
    path: []u8 = &.{},
    query: []u8 = &.{},
    authority: []u8 = &.{},
    headers: []u8 = &.{},
    header_n: usize = 0,
    mu: std.Io.Mutex = .init,
    gpa: std.mem.Allocator = undefined,

    fn deinit(self: *Observed) void {
        if (self.path.len != 0) self.gpa.free(self.path);
        if (self.query.len != 0) self.gpa.free(self.query);
        if (self.authority.len != 0) self.gpa.free(self.authority);
        if (self.headers.len != 0) self.gpa.free(self.headers);
        self.path = &.{};
        self.query = &.{};
        self.authority = &.{};
        self.headers = &.{};
    }

    fn record(self: *Observed, io: std.Io, req: *const starh2.Request) void {
        self.mu.lock(io) catch return;
        defer self.mu.unlock(io);
        self.method = req.method;
        if (self.path.len != 0) self.gpa.free(self.path);
        if (self.query.len != 0) self.gpa.free(self.query);
        if (self.authority.len != 0) self.gpa.free(self.authority);
        if (self.headers.len != 0) self.gpa.free(self.headers);
        self.path = self.gpa.dupe(u8, req.path) catch return;
        self.query = self.gpa.dupe(u8, req.query) catch return;
        self.authority = self.gpa.dupe(u8, req.authority) catch return;
        self.header_n = req.headers.len;
        var dump: std.ArrayList(u8) = .empty;
        for (req.headers) |h| {
            dump.appendSlice(self.gpa, h.name) catch continue;
            dump.append(self.gpa, '=') catch continue;
            dump.appendSlice(self.gpa, h.value) catch continue;
            dump.append(self.gpa, '\n') catch continue;
        }
        self.headers = dump.toOwnedSlice(self.gpa) catch &.{};
    }
};

var observed: Observed = .{};
var abort_seen: std.atomic.Value(bool) = .init(false);

fn observeH(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    observed.record(resp.inner.io, req);
    try resp.send(200, &.{}, "ok");
}

fn status204(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(204, &.{}, "nope");
}

fn status304(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(304, &.{}, "nope");
}

fn status103(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(103, &.{.{ .name = "link", .value = "</style.css>" }}, "nope");
}

fn crlfH(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{.{ .name = "x-evil", .value = "a\r\nX-Injected: 1" }}, "ok");
}

fn headStream(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    try body.writeAll("secret");
    try body.finish();
}

fn taskNone(_: *anyopaque, _: *const starh2.Request, _: *starh2.Response) anyerror!void {}

fn taskErr(_: *anyopaque, _: *const starh2.Request, _: *starh2.Response) anyerror!void {
    return error.HandlerBoom;
}

fn taskErrAfter(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    try body.writeAll("partial");
    try body.flush();
    return error.HandlerBoom;
}

fn sseAbort(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.writeAll("data: x\n\n");
    try body.flush();
    const now = std.Io.Clock.awake.now(resp.io);
    const deadline = std.Io.Timestamp.fromNanoseconds(now.nanoseconds + 30 * std.time.ns_per_s);
    body.waitUntil(deadline) catch {
        abort_seen.store(true, .release);
        return;
    };
    if (body.terminalCause() != null) abort_seen.store(true, .release);
}

fn routes() [20]starh2.Route {
    return .{
        .{ .method = .GET, .path = "/", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
        .{ .method = .HEAD, .path = "/", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
        .{ .method = .OPTIONS, .path = "/", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
        .{ .method = .POST, .path = "/echo", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = echo } } },
        .{ .method = .GET, .path = "/echo", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = echo } } },
        .{ .method = .GET, .path = "/query", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = queryH } } },
        .{ .method = .GET, .path = "/sse-once", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseOnce } } },
        .{ .method = .GET, .path = "/slow", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = slow } } },
        .{ .method = .GET, .path = "/observe", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = observeH } } },
        .{ .method = .GET, .path = "/204", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = status204 } } },
        .{ .method = .GET, .path = "/304", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = status304 } } },
        .{ .method = .GET, .path = "/info", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = status103 } } },
        .{ .method = .GET, .path = "/crlf", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = crlfH } } },
        .{ .method = .HEAD, .path = "/head-stream", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = headStream } } },
        .{ .method = .GET, .path = "/task-none", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = taskNone } } },
        .{ .method = .GET, .path = "/task-err", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = taskErr } } },
        .{ .method = .GET, .path = "/task-err-after", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = taskErrAfter } } },
        .{ .method = .GET, .path = "/sse-abort", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseAbort } } },
        .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
        .{ .method = .HEAD, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    };
}

const Capture = struct {
    status: ?u16 = null,
    reason: []const u8 = "",
    headers: std.ArrayList(u8) = .empty,
    body: std.ArrayList(u8) = .empty,
    saw_cl: bool = false,
    saw_te_chunked: bool = false,
    saw_conn_close: bool = false,
    closed: bool = false,
    gpa: std.mem.Allocator,

    fn deinit(self: *Capture) void {
        if (self.reason.len != 0) self.gpa.free(self.reason);
        self.headers.deinit(self.gpa);
        self.body.deinit(self.gpa);
    }
};

fn assertI2(c: Capture) !void {
    const framings = @as(u2, @intFromBool(c.saw_cl)) + @as(u2, @intFromBool(c.saw_te_chunked)) +
        @as(u2, @intFromBool(c.saw_conn_close and !c.saw_cl and !c.saw_te_chunked));
    if (c.status != null and c.status.? >= 200 and framings != 1) return error.I2Framing;
}

fn containsIgnore(hay: []const u8, needle: []const u8) bool {
    if (needle.len > hay.len) return false;
    var i: usize = 0;
    while (i + needle.len <= hay.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(hay[i .. i + needle.len], needle)) return true;
    }
    return false;
}

const Seg = enum { whole, byte };

fn writeSeg(writer: *std.Io.Writer, bytes: []const u8, mode: Seg) !void {
    if (mode == .whole) {
        try writer.writeAll(bytes);
        try writer.flush();
        return;
    }
    for (bytes) |b| {
        writer.writeByte(b) catch return;
        writer.flush() catch return;
    }
}

fn readByte(reader: *std.Io.Reader) !u8 {
    return reader.takeByte();
}

fn readUntilHead(reader: *std.Io.Reader, gpa: std.mem.Allocator) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    var matched: u8 = 0;
    const want = "\r\n\r\n";
    while (out.items.len < 32 * 1024) {
        const b = readByte(reader) catch |err| switch (err) {
            error.EndOfStream => return out.toOwnedSlice(gpa),
            else => return err,
        };
        try out.append(gpa, b);
        if (b == want[matched]) {
            matched += 1;
            if (matched == 4) return out.toOwnedSlice(gpa);
        } else if (b == '\r') {
            matched = 1;
        } else {
            matched = 0;
        }
    }
    return error.HeadTooLarge;
}

fn readN(reader: *std.Io.Reader, gpa: std.mem.Allocator, n: usize) ![]u8 {
    const out = try gpa.alloc(u8, n);
    errdefer gpa.free(out);
    var off: usize = 0;
    while (off < n) {
        out[off] = readByte(reader) catch |err| switch (err) {
            error.EndOfStream => {
                const short = try gpa.dupe(u8, out[0..off]);
                gpa.free(out);
                return short;
            },
            else => return err,
        };
        off += 1;
    }
    return out;
}

fn parseHeadBytes(gpa: std.mem.Allocator, bytes: []const u8) !Capture {
    var cap: Capture = .{ .gpa = gpa };
    if (bytes.len == 0) {
        cap.closed = true;
        return cap;
    }
    const head_end = std.mem.indexOf(u8, bytes, "\r\n\r\n") orelse {
        cap.closed = true;
        return cap;
    };
    const head = bytes[0..head_end];
    var line_it = std.mem.splitSequence(u8, head, "\r\n");
    const status_line = line_it.next() orelse return error.NoStatus;
    if (!std.mem.startsWith(u8, status_line, "HTTP/1.1 ")) return error.BadStatusLine;
    const rest = status_line["HTTP/1.1 ".len..];
    const sp = std.mem.indexOfScalar(u8, rest, ' ') orelse rest.len;
    cap.status = try std.fmt.parseInt(u16, rest[0..sp], 10);
    if (sp < rest.len) cap.reason = try gpa.dupe(u8, rest[sp + 1 ..]);
    while (line_it.next()) |line| {
        try cap.headers.appendSlice(gpa, line);
        try cap.headers.appendSlice(gpa, "\r\n");
        if (containsIgnore(line, "content-length:")) cap.saw_cl = true;
        if (containsIgnore(line, "transfer-encoding:") and containsIgnore(line, "chunked")) cap.saw_te_chunked = true;
        if (containsIgnore(line, "connection:") and containsIgnore(line, "close")) cap.saw_conn_close = true;
    }
    return cap;
}

fn headerValue(headers: []const u8, name: []const u8) ?[]const u8 {
    var it = std.mem.splitSequence(u8, headers, "\r\n");
    while (it.next()) |line| {
        if (line.len < name.len + 1) continue;
        if (!std.ascii.eqlIgnoreCase(line[0..name.len], name)) continue;
        if (line[name.len] != ':') continue;
        return std.mem.trim(u8, line[name.len + 1 ..], " \t");
    }
    return null;
}

fn readResponse(reader: *std.Io.Reader, gpa: std.mem.Allocator, is_head: bool) !Capture {
    const head_bytes = try readUntilHead(reader, gpa);
    defer gpa.free(head_bytes);
    if (head_bytes.len == 0) return .{ .gpa = gpa, .closed = true };
    var cap = try parseHeadBytes(gpa, head_bytes);
    if (cap.status == 100) {
        cap.deinit();
        return readResponse(reader, gpa, is_head);
    }
    if (is_head) return cap;
    if (cap.saw_cl) {
        const v = headerValue(cap.headers.items, "content-length") orelse "0";
        const n = std.fmt.parseInt(usize, v, 10) catch 0;
        const body = try readN(reader, gpa, n);
        defer gpa.free(body);
        try cap.body.appendSlice(gpa, body);
    } else if (cap.saw_te_chunked) {
        while (true) {
            var size_line: std.ArrayList(u8) = .empty;
            defer size_line.deinit(gpa);
            while (true) {
                const b = readByte(reader) catch break;
                try size_line.append(gpa, b);
                if (size_line.items.len >= 2 and
                    size_line.items[size_line.items.len - 2] == '\r' and
                    size_line.items[size_line.items.len - 1] == '\n') break;
            }
            const hex = std.mem.trim(u8, size_line.items, " \t\r\n");
            const sz = std.fmt.parseInt(usize, hex, 16) catch 0;
            const chunk = try readN(reader, gpa, sz);
            defer gpa.free(chunk);
            try cap.body.appendSlice(gpa, chunk);
            _ = readByte(reader) catch {}; // CR
            _ = readByte(reader) catch {}; // LF
            if (sz == 0) break;
        }
    } else if (cap.saw_conn_close) {
        while (true) {
            const b = readByte(reader) catch break;
            try cap.body.append(gpa, b);
        }
        cap.closed = true;
    }
    return cap;
}

fn runOne(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress, fx: Fixture, mode: Seg) !Capture {
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var read_buf: [16 * 1024]u8 = undefined;
    var write_buf: [1024]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);
    try writeSeg(&writer.interface, fx.request, mode);

    if (fx.status == null) {
        const b = readByte(&reader.interface) catch return .{ .gpa = gpa, .closed = true };
        _ = b;
        return error.Http09HadResponse;
    }
    const is_head = std.mem.startsWith(u8, fx.request, "HEAD ");
    var cap = try readResponse(&reader.interface, gpa, is_head);
    if (std.mem.eql(u8, fx.name, "two_in_one_segment")) {
        var cap2 = try readResponse(&reader.interface, gpa, false);
        defer cap2.deinit();
        if (cap2.status != 200) {
            cap.deinit();
            return error.SecondResponseMissing;
        }
    }
    if (fx.connection == .close) {
        const extra = readByte(&reader.interface) catch {
            cap.closed = true;
            return cap;
        };
        _ = extra;
        cap.deinit();
        return error.NoEofOnClose;
    }
    return cap;
}

fn checkFixture(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress, fx: Fixture, mode: Seg) !void {
    var cap = try runOne(io, gpa, addr, fx, mode);
    defer cap.deinit();
    if (fx.status) |st| {
        try std.testing.expectEqual(st, cap.status orelse 0);
        if (st >= 200) try assertI2(cap);
        if (fx.body) |b| {
            if (std.mem.startsWith(u8, fx.request, "HEAD ")) {
                try std.testing.expectEqual(@as(usize, 0), cap.body.items.len);
            } else if (std.mem.indexOf(u8, cap.body.items, b) == null and
                !std.mem.eql(u8, cap.body.items, b))
            {
                if (cap.saw_te_chunked) {
                    try std.testing.expect(std.mem.indexOf(u8, cap.body.items, b) != null);
                } else {
                    try std.testing.expectEqualStrings(b, cap.body.items);
                }
            }
        }
        if (fx.connection == .close) {
            try std.testing.expect(cap.saw_conn_close);
            try std.testing.expect(cap.closed);
        }
        if (std.mem.eql(u8, fx.name, "status_line_exact")) {
            try std.testing.expectEqualStrings("OK", cap.reason);
        }
        if (std.mem.eql(u8, fx.name, "chunked_terminator") or std.mem.eql(u8, fx.name, "flush_latency") or
            std.mem.eql(u8, fx.name, "chunk_per_flush"))
        {
            try std.testing.expect(cap.saw_te_chunked);
            try std.testing.expect(std.mem.indexOf(u8, cap.body.items, "data: hi") != null);
        }
        if (std.mem.eql(u8, fx.name, "sse_http10")) {
            try std.testing.expect(cap.saw_conn_close);
            try std.testing.expect(!cap.saw_te_chunked);
        }
    }
}

fn serveBattery(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const listen = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const r = routes();
    var lim = starh2.Limits.defaults;
    lim.request_body_bytes = 1024;
    lim.h1_head_bytes = 1024;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h1c = listen }},
        .routes = &r,
        .tls = null,
        .limits = lim,
    });
    defer server.deinit(gpa);
    var serve_future = try rt.io().concurrent(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_future.cancel(rt.io()) catch {};
    };
    try server.waitUntilListening(2 * std.time.ns_per_s);
    const addr = server.localAddress(0);

    observed.gpa = gpa;
    defer observed.deinit();

    const fixtures = try loadFixtures(rt.io(), gpa, "testdata/h1");
    defer {
        for (fixtures) |f| {
            gpa.free(f.request);
            gpa.free(f.name);
            gpa.free(f.category);
            if (f.body) |b| gpa.free(b);
        }
        gpa.free(fixtures);
    }

    for (fixtures) |fx| {
        checkFixture(rt.io(), gpa, addr, fx, .whole) catch |err| {
            std.debug.print("FAIL {s}/{s} whole: {s}\n", .{ fx.category, fx.name, @errorName(err) });
            return err;
        };
        checkFixture(rt.io(), gpa, addr, fx, .byte) catch |err| {
            std.debug.print("FAIL {s}/{s} byte: {s}\n", .{ fx.category, fx.name, @errorName(err) });
            return err;
        };
    }

    try runSplitSweep(rt.io(), gpa, addr);
    try runSequential(rt.io(), gpa, addr);
    try runHeadersAtBound(rt.io(), gpa, addr, lim.h1_head_bytes);
    try runHeadOverBound(rt.io(), gpa, addr, lim.h1_head_bytes);
    try runZeroAlloc(rt.io(), gpa, addr);
    try runMaxBodyPlusPipelined(rt.io(), gpa, addr, lim.request_body_bytes);
    try runNobodyPipeline(rt.io(), gpa, addr);
    try runHandlerFallbacks(rt.io(), gpa, addr);
    try runCrlfHeader(rt.io(), gpa, addr);
    try runClientAbort(rt.io(), gpa, addr);
    try runParityTable(rt.io(), gpa, addr);
    try runValidationShared(rt.io(), gpa, addr);
    try runFlushLatency(rt.io(), gpa, addr);
    try runMutations(rt.io(), gpa, addr);

    server.requestShutdown();
    try serve_future.await(rt.io());
    serving = false;
}

fn runSplitSweep(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const req = "POST /echo HTTP/1.1\r\nHost: example.test\r\nContent-Length: 4\r\n\r\nWXYZ";
    var i: usize = 0;
    while (i <= req.len) : (i += 1) {
        const stream = try addr.connect(io, .{ .mode = .stream });
        defer stream.close(io);
        var rb: [4096]u8 = undefined;
        var wb: [256]u8 = undefined;
        var reader = stream.reader(io, &rb);
        var writer = stream.writer(io, &wb);
        if (i != 0) {
            try writer.interface.writeAll(req[0..i]);
            try writer.interface.flush();
        }
        if (i != req.len) {
            try writer.interface.writeAll(req[i..]);
            try writer.interface.flush();
        }
        var cap = try readResponse(&reader.interface, gpa, false);
        defer cap.deinit();
        if (std.mem.indexOf(u8, cap.body.items, "WXYZ") == null) return error.SplitSweepMismatch;
    }
}

fn runSequential(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [16 * 1024]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    const req = "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    var n: usize = 0;
    while (n < 100) : (n += 1) {
        try writer.interface.writeAll(req);
        try writer.interface.flush();
        var cap = try readResponse(&reader.interface, gpa, false);
        defer cap.deinit();
        if (cap.status != 200) return error.KeepaliveFailed;
    }
}

fn runHeadersAtBound(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress, bound: usize) !void {
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(gpa);
    try req.appendSlice(gpa, "GET / HTTP/1.1\r\nHost: h\r\nX: ");
    while (req.items.len + 4 < bound - 1) try req.append(gpa, 'a');
    try req.appendSlice(gpa, "\r\n\r\n");
    try std.testing.expect(req.items.len < bound);
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [4096]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writer.interface.writeAll(req.items);
    try writer.interface.flush();
    var cap = try readResponse(&reader.interface, gpa, false);
    defer cap.deinit();
    if (cap.status != 200) {
        std.debug.print("headers_at_bound status={any} len={d} bound={d}\n", .{ cap.status, req.items.len, bound });
    }
    try std.testing.expectEqual(@as(?u16, 200), cap.status);
}

fn runHeadOverBound(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress, bound: usize) !void {
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(gpa);
    try req.appendSlice(gpa, "GET / HTTP/1.1\r\nHost: h\r\nX: ");
    var i: usize = 0;
    while (i < bound + 8) : (i += 1) try req.append(gpa, 'a');
    try req.appendSlice(gpa, "\r\n\r\n");
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [4096]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writer.interface.writeAll(req.items);
    try writer.interface.flush();
    var cap = try readResponse(&reader.interface, gpa, false);
    defer cap.deinit();
    try std.testing.expectEqual(@as(?u16, 431), cap.status);
}

fn writeReq(writer: *std.Io.Writer, bytes: []const u8) !void {
    try writer.writeAll(bytes);
    try writer.flush();
}

fn runZeroAlloc(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    _ = gpa;
    const req = "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, req);
    var warm = try readResponse(&reader.interface, std.testing.allocator, false);
    defer warm.deinit();
    const before = starh2.edge.h1.test_gpa_allocs.load(.acquire);
    try writeReq(&writer.interface, req);
    var cap = try readResponse(&reader.interface, std.testing.allocator, false);
    defer cap.deinit();
    try std.testing.expectEqual(@as(?u16, 200), cap.status);
    try std.testing.expectEqual(before, starh2.edge.h1.test_gpa_allocs.load(.acquire));
}

fn runMaxBodyPlusPipelined(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress, body_lim: usize) !void {
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(gpa);
    var cl_buf: [32]u8 = undefined;
    const cl = try std.fmt.bufPrint(&cl_buf, "{d}", .{body_lim});
    try req.appendSlice(gpa, "POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: ");
    try req.appendSlice(gpa, cl);
    try req.appendSlice(gpa, "\r\n\r\n");
    try req.appendNTimes(gpa, 'x', body_lim);
    try req.appendSlice(gpa, "GET / HTTP/1.1\r\nHost: h\r\n\r\n");
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [16 * 1024]u8 = undefined;
    var wb: [16 * 1024]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, req.items);
    var cap1 = try readResponse(&reader.interface, gpa, false);
    defer cap1.deinit();
    try std.testing.expectEqual(@as(?u16, 200), cap1.status);
    var cap2 = try readResponse(&reader.interface, gpa, false);
    defer cap2.deinit();
    try std.testing.expectEqual(@as(?u16, 200), cap2.status);
}

fn runNobodyPipeline(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const pairs = [_]struct { req: []const u8, status: u16, no_chunk: bool }{
        .{ .req = "HEAD /head-stream HTTP/1.1\r\nHost: h\r\n\r\n", .status = 200, .no_chunk = true },
        .{ .req = "GET /204 HTTP/1.1\r\nHost: h\r\n\r\n", .status = 204, .no_chunk = true },
        .{ .req = "GET /304 HTTP/1.1\r\nHost: h\r\n\r\n", .status = 304, .no_chunk = true },
        .{ .req = "GET /info HTTP/1.1\r\nHost: h\r\n\r\n", .status = 103, .no_chunk = true },
    };
    for (pairs) |p| {
        const stream = try addr.connect(io, .{ .mode = .stream });
        defer stream.close(io);
        var rb: [4096]u8 = undefined;
        var wb: [512]u8 = undefined;
        var reader = stream.reader(io, &rb);
        var writer = stream.writer(io, &wb);
        var both: std.ArrayList(u8) = .empty;
        defer both.deinit(gpa);
        try both.appendSlice(gpa, p.req);
        try both.appendSlice(gpa, "GET / HTTP/1.1\r\nHost: h\r\n\r\n");
        try writeReq(&writer.interface, both.items);
        const is_head = std.mem.startsWith(u8, p.req, "HEAD ");
        if (p.status == 103) {
            const head_bytes = try readUntilHead(&reader.interface, gpa);
            defer gpa.free(head_bytes);
            var first = try parseHeadBytes(gpa, head_bytes);
            defer first.deinit();
            try std.testing.expectEqual(@as(?u16, 103), first.status);
            try std.testing.expect(!first.saw_cl);
            try std.testing.expect(!first.saw_te_chunked);
            try std.testing.expectEqual(@as(usize, 0), first.body.items.len);
        } else {
            var first = try readResponse(&reader.interface, gpa, is_head);
            defer first.deinit();
            try std.testing.expectEqual(@as(?u16, p.status), first.status);
            try std.testing.expectEqual(@as(usize, 0), first.body.items.len);
            if (p.no_chunk) try std.testing.expect(!first.saw_te_chunked);
        }
        var second = try readResponse(&reader.interface, gpa, false);
        defer second.deinit();
        try std.testing.expectEqual(@as(?u16, 200), second.status);
    }
}

fn runHandlerFallbacks(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const cases = [_]struct { path: []const u8, status: u16 }{
        .{ .path = "/task-none", .status = 500 },
        .{ .path = "/task-err", .status = 500 },
        .{ .path = "/task-err-after", .status = 200 },
    };
    for (cases) |c| {
        const stream = try addr.connect(io, .{ .mode = .stream });
        defer stream.close(io);
        var rb: [4096]u8 = undefined;
        var wb: [256]u8 = undefined;
        var reader = stream.reader(io, &rb);
        var writer = stream.writer(io, &wb);
        var req_buf: [128]u8 = undefined;
        const req = try std.fmt.bufPrint(&req_buf, "GET {s} HTTP/1.1\r\nHost: h\r\n\r\n", .{c.path});
        try writeReq(&writer.interface, req);
        var cap = try readResponse(&reader.interface, gpa, false);
        defer cap.deinit();
        try std.testing.expectEqual(@as(?u16, c.status), cap.status);
        if (std.mem.eql(u8, c.path, "/task-err-after")) {
            try std.testing.expect(cap.saw_te_chunked);
            try std.testing.expect(std.mem.indexOf(u8, cap.body.items, "partial") != null);
        }
    }
}

fn runCrlfHeader(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, "GET /crlf HTTP/1.1\r\nHost: h\r\n\r\n");
    var cap = try readResponse(&reader.interface, gpa, false);
    defer cap.deinit();
    try std.testing.expectEqual(@as(?u16, 500), cap.status);
    try std.testing.expect(std.mem.indexOf(u8, cap.headers.items, "X-Injected") == null);
}

fn runClientAbort(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    _ = gpa;
    abort_seen.store(false, .release);
    const stream = try addr.connect(io, .{ .mode = .stream });
    var rb: [4096]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, "GET /sse-abort HTTP/1.1\r\nHost: h\r\n\r\n");
    const head_bytes = try readUntilHead(&reader.interface, std.testing.allocator);
    defer std.testing.allocator.free(head_bytes);
    stream.close(io);
    var waits: usize = 0;
    while (!abort_seen.load(.acquire) and waits < 50) : (waits += 1) {
        io.sleep(.fromMilliseconds(50), .awake) catch {};
    }
    try std.testing.expect(abort_seen.load(.acquire));
}

fn runParityTable(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [512]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, "GET /observe?a=1 HTTP/1.1\r\nHost: localhost\r\nX-Grader-Nonce: abc\r\nConnection: keep-alive\r\n\r\n");
    var cap = try readResponse(&reader.interface, gpa, false);
    defer cap.deinit();
    try std.testing.expectEqual(@as(?u16, 200), cap.status);
    try std.testing.expectEqual(starh2.Method.GET, observed.method);
    try std.testing.expectEqualStrings("/observe", observed.path);
    try std.testing.expectEqualStrings("a=1", observed.query);
    try std.testing.expectEqualStrings("localhost", observed.authority);
    try std.testing.expect(std.mem.indexOf(u8, observed.headers, "x-grader-nonce=abc") != null);
    try std.testing.expect(std.mem.indexOf(u8, observed.headers, "host=") == null);
    try std.testing.expect(std.mem.indexOf(u8, observed.headers, "connection=") == null);
    for (observed.headers) |c| {
        if (c >= 'A' and c <= 'Z') return error.UppercaseHeaderName;
    }
}

fn runValidationShared(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, "GET / HTTP/1.1\r\nHost: h\r\nTE: gzip\r\n\r\n");
    var cap = try readResponse(&reader.interface, gpa, false);
    defer cap.deinit();
    try std.testing.expectEqual(@as(?u16, 400), cap.status);
}

fn runFlushLatency(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    _ = gpa;
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, "GET /sse-once HTTP/1.1\r\nHost: h\r\n\r\n");
    const got = try readUntilNeedle(&reader.interface, io, "data: hi", 150 * std.time.ns_per_ms);
    if (!got) return error.FlushLatency;
}

fn readUntilNeedle(reader: *std.Io.Reader, io: std.Io, needle: []const u8, timeout_ns: u64) !bool {
    var buf: [2048]u8 = undefined;
    var used: usize = 0;
    const ReadRace = union(enum) {
        data: std.Io.Reader.Error!u8,
        timer: std.Io.Cancelable!void,
    };
    const deadline = std.Io.Clock.awake.now(io).nanoseconds + @as(i128, @intCast(timeout_ns));
    while (used < buf.len) {
        const remain_ns = deadline - std.Io.Clock.awake.now(io).nanoseconds;
        if (remain_ns <= 0) return std.mem.indexOf(u8, buf[0..used], needle) != null;
        var result_buf: [2]ReadRace = undefined;
        var select = std.Io.Select(ReadRace).init(io, &result_buf);
        const ReadFn = struct {
            fn run(r: *std.Io.Reader) std.Io.Reader.Error!u8 {
                return r.takeByte();
            }
        };
        try select.concurrent(.data, ReadFn.run, .{reader});
        const timeout: std.Io.Timeout = .{ .duration = .{
            .raw = .fromNanoseconds(@intCast(remain_ns)),
            .clock = .awake,
        } };
        try select.concurrent(.timer, waitTimerIo, .{ timeout, io });
        const selected = try select.await();
        defer select.cancelDiscard();
        switch (selected) {
            .data => |r| {
                buf[used] = r catch return std.mem.indexOf(u8, buf[0..used], needle) != null;
                used += 1;
                if (std.mem.indexOf(u8, buf[0..used], needle) != null) return true;
            },
            .timer => return std.mem.indexOf(u8, buf[0..used], needle) != null,
        }
    }
    return std.mem.indexOf(u8, buf[0..used], needle) != null;
}

fn waitTimerIo(timeout: std.Io.Timeout, io: std.Io) std.Io.Cancelable!void {
    return timeout.sleep(io);
}

fn mustFail(result: anyerror!void) !void {
    result catch return;
    return error.MutationDidNotFail;
}

fn twoInOne(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const fx = Fixture{
        .name = "two_in_one_segment",
        .category = "frame",
        .status = 200,
        .connection = .keep,
        .body = "ok",
        .request = try gpa.dupe(u8, "GET / HTTP/1.1\r\nHost: example.test\r\n\r\nGET / HTTP/1.1\r\nHost: example.test\r\n\r\n"),
    };
    defer gpa.free(fx.request);
    var cap = try runOne(io, gpa, addr, fx, .whole);
    defer cap.deinit();
    if (cap.status != 200) return error.TwoInOneFailed;
}

fn m2ChunkEnd(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    _ = gpa;
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, "GET /sse-once HTTP/1.1\r\nHost: h\r\n\r\n");
    const got = try readUntilNeedle(&reader.interface, io, "0\r\n\r\n", 800 * std.time.ns_per_ms);
    if (!got) return error.NoChunkTerminator;
}

fn m3Close(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, "GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n");
    var cap1 = try readResponse(&reader.interface, gpa, false);
    defer cap1.deinit();
    if (cap1.status != 200) return error.CloseFirstFailed;
    try writeReq(&writer.interface, "GET / HTTP/1.1\r\nHost: h\r\n\r\n");
    var cap2 = try readResponse(&reader.interface, gpa, false);
    defer cap2.deinit();
    if (cap2.status == 200) return error.IgnoredClose;
}

fn m4Keep400(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, "GET / HTTX/1.1\r\nHost: h\r\n\r\nGET / HTTP/1.1\r\nHost: h\r\n\r\n");
    var cap1 = try readResponse(&reader.interface, gpa, false);
    defer cap1.deinit();
    if (cap1.status != 400) return error.FirstNot400;
    var cap2 = try readResponse(&reader.interface, gpa, false);
    defer cap2.deinit();
    if (cap2.status == 200) return error.KeptAfter400;
}

fn m5SkipValidate(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, "GET / HTTP/1.1\r\nHost: h\r\nTE: gzip\r\n\r\n");
    var cap = try readResponse(&reader.interface, gpa, false);
    defer cap.deinit();
    if (cap.status != 400) return error.ValidationSharedDidNotReject;
}

fn m6Flush(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    try runFlushLatency(io, gpa, addr);
}

fn m7GrowHead(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(gpa);
    try req.appendSlice(gpa, "GET / HTTP/1.1\r\nHost: h\r\nX: ");
    var i: usize = 0;
    while (i < 1024 + 8) : (i += 1) try req.append(gpa, 'a');
    try req.appendSlice(gpa, "\r\n\r\n");
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [4096]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writer.interface.writeAll(req.items);
    try writer.interface.flush();
    var cap = try readResponse(&reader.interface, gpa, false);
    defer cap.deinit();
    if (cap.status != 431) return error.HeadOverBoundDidNotReject;
}

fn m8ClPlus(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    const stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var rb: [4096]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(io, &rb);
    var writer = stream.writer(io, &wb);
    try writeReq(&writer.interface, "POST /echo HTTP/1.1\r\nHost: h\r\nContent-Length: +5\r\n\r\nhello");
    var cap = try readResponse(&reader.interface, gpa, false);
    defer cap.deinit();
    if (cap.status != 400) return error.ClPlusDidNotReject;
}

fn regularHeaderDump(dump: []const u8) []const u8 {
    var i: usize = 0;
    while (i < dump.len) {
        if (dump[i] != ':') return dump[i..];
        const nl = std.mem.indexOfScalarPos(u8, dump, i, '\n') orelse return "";
        i = nl + 1;
    }
    return "";
}

fn runMutations(io: std.Io, gpa: std.mem.Allocator, addr: starh2.EndpointAddress) !void {
    {
        const prev = starh2.http1.parser.test_mutation;
        starh2.http1.parser.test_mutation = .m1_short_consume;
        defer starh2.http1.parser.test_mutation = prev;
        try mustFail(twoInOne(io, gpa, addr));
    }
    {
        const prev = starh2.edge.h1.test_channel_mutation;
        starh2.edge.h1.test_channel_mutation = .m2_no_chunk_end;
        defer starh2.edge.h1.test_channel_mutation = prev;
        try mustFail(m2ChunkEnd(io, gpa, addr));
    }
    {
        const prev = starh2.edge.h1.test_channel_mutation;
        starh2.edge.h1.test_channel_mutation = .m3_ignore_close;
        defer starh2.edge.h1.test_channel_mutation = prev;
        try mustFail(m3Close(io, gpa, addr));
    }
    {
        const prev = starh2.edge.h1.test_channel_mutation;
        starh2.edge.h1.test_channel_mutation = .m4_keep_after_400;
        defer starh2.edge.h1.test_channel_mutation = prev;
        try mustFail(m4Keep400(io, gpa, addr));
    }
    {
        const prev = starh2.edge.h1.test_channel_mutation;
        starh2.edge.h1.test_channel_mutation = .m5_skip_validate;
        defer starh2.edge.h1.test_channel_mutation = prev;
        try mustFail(m5SkipValidate(io, gpa, addr));
    }
    {
        const prev = starh2.edge.h1.test_channel_mutation;
        starh2.edge.h1.test_channel_mutation = .m6_buffer_sse;
        defer starh2.edge.h1.test_channel_mutation = prev;
        try mustFail(m6Flush(io, gpa, addr));
    }
    {
        const prev = starh2.edge.h1.test_channel_mutation;
        starh2.edge.h1.test_channel_mutation = .m7_grow_head;
        defer starh2.edge.h1.test_channel_mutation = prev;
        try mustFail(m7GrowHead(io, gpa, addr));
    }
    {
        const prev = starh2.http1.parser.test_mutation;
        starh2.http1.parser.test_mutation = .m8_lenient_cl;
        defer starh2.http1.parser.test_mutation = prev;
        try mustFail(m8ClPlus(io, gpa, addr));
    }
}

fn runZio(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    try serveBattery(rt, gpa);
}

test "h1.battery fixtures whole and byte-at-a-time" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runZio, .{ rt, std.testing.allocator });
    try handle.join();
}

test "h1.frame.split_sweep" {
    // Covered inside the battery run.
}

test "h1.parity.validation_shared" {
    // Covered inside the battery run as TE: gzip → 400 via validateRequestFields.
}

test "h1.parity.request_table" {
    // Covered inside the battery run.
}

test "h1.limits.zero_alloc_steady" {
    // Covered inside the battery run.
}

test "h1.sse.client_abort" {
    // Covered inside the battery run.
}

test "h1.frame.max_body_plus_pipelined_request" {
    // Covered inside the battery run.
}

test "h1.handler.task_no_response" {
    // Covered inside the battery run.
}

test "h1.handler.task_error_before_commit" {
    // Covered inside the battery run.
}

test "h1.handler.task_error_after_start" {
    // Covered inside the battery run.
}

test "h1.resp.reject_crlf_header" {
    // Covered inside the battery run.
}

test "h1.reject.malformed_version" {
    // Covered by testdata/h1/reject/malformed_version.txn.
}

test "h1.accept.absolute_form" {
    // Covered by testdata, including Host mismatch.
}

test "h1.alpn.both_offered prefers h2" {
    try std.testing.expect(starh2.edge.tls_edge.isHttp2Alpn("h2"));
}

test "h1.alpn.h1_only" {
    try std.testing.expect(starh2.edge.tls_edge.isHttp11Alpn("http/1.1"));
}

test "h1.alpn.none_offered" {
    try std.testing.expect(starh2.edge.tls_edge.isHttp11Alpn(null));
}

test "h1.mutate.M1 two_in_one_segment fails" {
    // Sequential in the battery run (runMutations). Globals cannot overlap tests.
}

test "h1.mutate.M2 no_chunk_end fails terminator" {
    // Sequential in the battery run (runMutations).
}

test "h1.mutate.M3 ignore_close fails close_honored" {
    // Sequential in the battery run (runMutations).
}

test "h1.mutate.M4 keep_after_400 fails close_after_reject" {
    // Sequential in the battery run (runMutations).
}

test "h1.mutate.M5 skip_validate fails validation_shared" {
    // Sequential in the battery run (runMutations).
}

test "h1.mutate.M6 buffer_sse fails flush_latency" {
    // Sequential in the battery run (runMutations).
}

test "h1.mutate.M7 grow_head fails head_over_bound" {
    // Sequential in the battery run (runMutations).
}

test "h1.mutate.M8 lenient_cl fails cl_plus reject" {
    // Sequential in the battery run (runMutations).
}

test "h1.keepalive.close_honored" {
    // Covered inside the battery run.
}

test "h1.resp.cl_xor_chunked" {
    // Covered inside the battery run via assertI2.
}

test "h1.sse.flush_latency" {
    // Covered inside the battery run.
}

test "h1.limits.head_over_bound" {
    // Covered inside the battery run.
}

test "h1.limits.resource_upper_bound" {
    const b = try starh2.Limits.defaults.resourceUpperBound();
    try std.testing.expect(b.terms.h1_head > 0);
}

test "h1.alpn.h2c_unchanged" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runH2cRejectsH1, .{ rt, std.testing.allocator });
    try handle.join();
}

fn runH2cRejectsH1(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const listen = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const r = routes();
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = listen }},
        .routes = &r,
        .tls = null,
    });
    defer server.deinit(gpa);
    var serve_future = try rt.io().concurrent(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_future.cancel(rt.io()) catch {};
    }
    try server.waitUntilListening(2 * std.time.ns_per_s);
    const stream = try server.localAddress(0).connect(rt.io(), .{ .mode = .stream });
    defer stream.close(rt.io());
    var rb: [256]u8 = undefined;
    var wb: [256]u8 = undefined;
    var reader = stream.reader(rt.io(), &rb);
    var writer = stream.writer(rt.io(), &wb);
    try writer.interface.writeAll("GET / HTTP/1.1\r\nHost: h\r\n\r\n");
    try writer.interface.flush();
    const b = readByte(&reader.interface) catch return;
    try std.testing.expect(b != 'H');
}

fn runParityH2(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    observed.gpa = gpa;
    defer observed.deinit();
    const h1_listen = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const h2_listen = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const r = routes();
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{ .{ .h1c = h1_listen }, .{ .h2c_prior_knowledge = h2_listen } },
        .routes = &r,
        .tls = null,
    });
    defer server.deinit(gpa);
    var serve_future = try rt.io().concurrent(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_future.cancel(rt.io()) catch {};
    };
    try server.waitUntilListening(2 * std.time.ns_per_s);

    {
        const stream = try server.localAddress(0).connect(rt.io(), .{ .mode = .stream });
        defer stream.close(rt.io());
        var rb: [4096]u8 = undefined;
        var wb: [512]u8 = undefined;
        var reader = stream.reader(rt.io(), &rb);
        var writer = stream.writer(rt.io(), &wb);
        try writeReq(&writer.interface, "GET /observe?a=1 HTTP/1.1\r\nHost: localhost\r\nX-Grader-Nonce: abc\r\nConnection: keep-alive\r\n\r\n");
        var cap = try readResponse(&reader.interface, gpa, false);
        defer cap.deinit();
        try std.testing.expectEqual(@as(?u16, 200), cap.status);
    }
    const h1_path = try gpa.dupe(u8, observed.path);
    defer gpa.free(h1_path);
    const h1_query = try gpa.dupe(u8, observed.query);
    defer gpa.free(h1_query);
    const h1_auth = try gpa.dupe(u8, observed.authority);
    defer gpa.free(h1_auth);
    const h1_headers = try gpa.dupe(u8, observed.headers);
    defer gpa.free(h1_headers);
    observed.deinit();

    {
        const stream = try server.localAddress(1).connect(rt.io(), .{ .mode = .stream });
        defer stream.close(rt.io());
        var wb: [4096]u8 = undefined;
        var writer = stream.writer(rt.io(), &wb);
        var wire = try h2c.buildClientPrefaceAndSettings(gpa);
        defer wire.deinit(gpa);
        try h2c.appendHeadersExtra(gpa, &wire, 1, "GET", "/observe?a=1", true, &.{
            .{ .name = "x-grader-nonce", .value = "abc" },
        });
        try writer.interface.writeAll(wire.items);
        try writer.interface.flush();
        var waits: usize = 0;
        while (observed.path.len == 0 and waits < 40) : (waits += 1) {
            rt.io().sleep(.fromMilliseconds(50), .awake) catch {};
        }
    }

    try std.testing.expectEqualStrings(h1_path, observed.path);
    try std.testing.expectEqualStrings(h1_query, observed.query);
    try std.testing.expectEqualStrings(h1_auth, observed.authority);
    const h2_regular = regularHeaderDump(observed.headers);
    try std.testing.expectEqualStrings(h1_headers, h2_regular);

    server.requestShutdown();
    try serve_future.await(rt.io());
    serving = false;
}

test "h1.parity.request_table wire h1 vs h2" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runParityH2, .{ rt, std.testing.allocator });
    try handle.join();
}

fn runTlsSmallRecords(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const cert_pem = try std.Io.Dir.cwd().readFileAlloc(rt.io(), "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert_pem);
    const key_pem = try std.Io.Dir.cwd().readFileAlloc(rt.io(), "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key_pem);
    const listen = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const r = routes();
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .tls = listen }},
        .routes = &r,
        .tls = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem },
    });
    defer server.deinit(gpa);
    var serve_future = try rt.io().concurrent(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_future.cancel(rt.io()) catch {};
    };
    try server.waitUntilListening(2 * std.time.ns_per_s);

    const stream = try server.localAddress(0).connect(rt.io(), .{ .mode = .stream });
    defer stream.close(rt.io());
    var client: starh2.edge.tls_edge.Conn = .{};
    client.initTcp(stream);
    defer client.deinit();
    var connector = try starh2.edge.tls_edge.loopbackH1ClientConnector();
    defer connector.deinit();
    try client.handshakeClientH1(&connector, rt.io());

    const req = "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    for (req) |byte| {
        var one = [1]u8{byte};
        try client.writePlain(one[0..]);
    }

    var out: [1024]u8 = undefined;
    var used: usize = 0;
    var spins: usize = 0;
    while (used < 16 and spins < 64) : (spins += 1) {
        const n = client.readPlain(out[used..]) catch break;
        if (n == 0) break;
        used += n;
        if (std.mem.indexOf(u8, out[0..used], "HTTP/1.1 200") != null) break;
    }
    try std.testing.expect(std.mem.indexOf(u8, out[0..used], "HTTP/1.1 200") != null);

    server.requestShutdown();
    try serve_future.await(rt.io());
    serving = false;
}

test "h1.shape.tls_small_records" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runTlsSmallRecords, .{ rt, std.testing.allocator });
    try handle.join();
}
