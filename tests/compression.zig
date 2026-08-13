//! Response brotli compression — I1–I8 live gates.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const dummy: u8 = 0;
const brotli = starh2.http.brotli;
const hpack = starh2.core.hpack;
const frame = starh2.core.frame;

fn writeAllStream(stream: zio.net.Stream, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = try stream.write(bytes[off..], .none);
        off += n;
    }
}

const Capture = struct {
    gpa: std.mem.Allocator,
    parser: frame.Parser,
    body: std.ArrayList(u8) = .empty,
    headers: std.ArrayList(hpack.HeaderField) = .empty,
    end_stream: bool = false,
    saw_rst: bool = false,
    data_frames: usize = 0,
    /// Compressed body prefixes after each DATA frame (for I4 incremental decode).
    data_chunks: std.ArrayList([]u8) = .empty,

    fn init(gpa: std.mem.Allocator) !Capture {
        var parser = frame.Parser.init(gpa, frame.DEFAULT_MAX_FRAME_SIZE);
        parser.skipPreface();
        return .{ .gpa = gpa, .parser = parser };
    }

    fn deinit(self: *Capture) void {
        self.body.deinit(self.gpa);
        for (self.headers.items) |h| {
            self.gpa.free(h.name);
            self.gpa.free(h.value);
        }
        self.headers.deinit(self.gpa);
        for (self.data_chunks.items) |c| self.gpa.free(c);
        self.data_chunks.deinit(self.gpa);
        self.parser.deinit();
    }

    fn ingest(self: *Capture, bytes: []const u8) !void {
        var rem = bytes;
        while (rem.len > 0) {
            const maybe = try self.parser.ingestOne(rem);
            if (maybe) |r| {
                defer if (r.event.payload.len != 0) self.gpa.free(r.event.payload);
                rem = rem[r.consumed..];
                const hdr = r.event.header;
                switch (hdr.type) {
                    .data => {
                        try self.body.appendSlice(self.gpa, r.event.payload);
                        const chunk = try self.gpa.dupe(u8, r.event.payload);
                        try self.data_chunks.append(self.gpa, chunk);
                        self.data_frames += 1;
                        if (hdr.flags.end_stream) self.end_stream = true;
                    },
                    .headers => {
                        var dec = hpack.Decoder.init(self.gpa);
                        defer dec.deinit();
                        const result = try dec.decode(r.event.payload, 100, 64 * 1024, 256, 8 * 1024);
                        defer {
                            for (result.fields) |f| {
                                self.gpa.free(f.name);
                                self.gpa.free(f.value);
                            }
                            self.gpa.free(result.fields);
                        }
                        for (result.fields) |f| {
                            try self.headers.append(self.gpa, .{
                                .name = try self.gpa.dupe(u8, f.name),
                                .value = try self.gpa.dupe(u8, f.value),
                            });
                        }
                        if (hdr.flags.end_stream) self.end_stream = true;
                    },
                    .rst_stream => self.saw_rst = true,
                    .settings => {
                        // ACK handled by discarding; server does not require client SETTINGS ACK for these tests.
                    },
                    else => {},
                }
            } else break;
        }
    }

    fn headerValue(self: *const Capture, name: []const u8) ?[]const u8 {
        for (self.headers.items) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }

    fn hasVaryAcceptEncoding(self: *const Capture) bool {
        for (self.headers.items) |h| {
            if (!std.ascii.eqlIgnoreCase(h.name, "vary")) continue;
            var it = std.mem.splitScalar(u8, h.value, ',');
            while (it.next()) |part| {
                const t = std.mem.trim(u8, part, " \t");
                if (std.ascii.eqlIgnoreCase(t, "accept-encoding")) return true;
            }
        }
        return false;
    }
};

fn readUntil(
    stream: zio.net.Stream,
    cap: *Capture,
    timeout_ms: u64,
    done: *const fn (*Capture) bool,
) !void {
    var buf: [16 * 1024]u8 = undefined;
    var waited: u64 = 0;
    while (waited < timeout_ms) : (waited += 20) {
        const n = stream.read(&buf, .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
            error.Timeout => 0,
            else => return err,
        };
        if (n > 0) try cap.ingest(buf[0..n]);
        if (done(cap)) return;
    }
    return error.Timeout;
}

fn doneEnd(c: *Capture) bool {
    return c.end_stream or c.saw_rst;
}

fn doneHeaders(c: *Capture) bool {
    return c.headers.items.len > 0 or c.saw_rst;
}

fn enableCompressionLimits(base: starh2.Limits) starh2.Limits {
    var lim = base;
    lim.response_compression = true;
    lim.max_connections = 16;
    lim.max_streams_per_connection = 32;
    lim.max_streams_per_server = 64;
    lim.cancellation_reaper_jobs = 64;
    lim.cancellation_reaper_tasks = 2;
    lim.outbound_slabs_per_server = 64;
    return lim;
}


fn withRuntime(comptime func: anytype) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(2),
        .enable_task_migration = true,
    });
    defer rt.deinit();
    var handle = try rt.spawn(func, .{ rt, gpa });
    try handle.join();
}

fn runServer(
    rt: *zio.Runtime,
    gpa: std.mem.Allocator,
    routes: []const starh2.Route,
    limits: starh2.Limits,
    comptime body: anytype,
) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    try body(&server, rt, gpa);
}

fn compressiblePlain(n: usize, gpa: std.mem.Allocator) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    const chunk = "<div class=\"row\"><span>item</span><p>lorem ipsum dolor</p></div>\n";
    while (out.items.len < n) try out.appendSlice(gpa, chunk);
    return try out.toOwnedSlice(gpa);
}

// --- handlers ---------------------------------------------------------------

fn sendTextHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    const plain = "hello compressed world " ** 20; // > 256 bytes
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/plain" }}, plain);
}

fn sendSmallHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/plain" }}, "tiny");
}

fn sendBigHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const plain = try compressiblePlain(64 * 1024, req.arena);
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/html" }}, plain);
}

/// `/big` shape from the conformance server: ≥5 MiB one-shot send.
fn sendFiveMiBHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const plain = try req.arena.alloc(u8, 5 * 1024 * 1024);
    @memset(plain, 'B');
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/plain" }}, plain);
}

fn sendWithContentEncoding(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    const plain = "already-encoded-body-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad";
    try resp.send(200, &.{
        .{ .name = "content-type", .value = "text/plain" },
        .{ .name = "content-encoding", .value = "gzip" },
    }, plain);
}

fn sendWithVaryAndLength(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    const plain = "vary-merge-body " ** 40;
    var len_buf: [16]u8 = undefined;
    const len_s = try std.fmt.bufPrint(&len_buf, "{d}", .{plain.len});
    try resp.send(200, &.{
        .{ .name = "content-type", .value = "text/plain" },
        .{ .name = "vary", .value = "origin" },
        .{ .name = "content-length", .value = len_s },
    }, plain);
}

fn sendPngHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    const plain = "\x89PNG" ++ ("binary-not-compressible-pad" ** 20);
    try resp.send(200, &.{.{ .name = "content-type", .value = "image/png" }}, plain);
}

fn streamHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.start(200, &.{.{ .name = "content-type", .value = "text/plain" }});
    // many small writes, one flush
    try body.writeAll("part-a-");
    try body.writeAll("part-b-");
    try body.writeAll("part-c-and-more-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding");
    try body.flush();
    try body.writeAll("-tail");
    try body.finish();
}

fn sseHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    const e1 = "data: <div id=\"a\">one</div>\n\n";
    const e2 = "data: <div id=\"a\">two-repeats-similar-html</div>\n\n";
    const e3 = "data: <div id=\"a\">three-still-similar</div>\n\n";
    try body.writeAll(e1); // implicit flush
    try body.writeAll(e2);
    try body.writeAll(e3);
    try body.finish();
}

fn sseLargeEventHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    // One event larger than outbound_bytes_per_stream (64KiB default in test limits).
    var big: std.ArrayList(u8) = .empty;
    defer big.deinit(req.arena);
    try big.appendSlice(req.arena, "data: ");
    while (big.items.len < 70 * 1024) try big.appendSlice(req.arena, "<div>pad</div>");
    try big.appendSlice(req.arena, "\n\n");
    try body.writeAll(big.items);
    try body.finish();
}

fn sseOneByteHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    try body.writeAll("d");
    try body.writeAll("ata: x\n\n");
    try body.finish();
}

var peer_reset_err: std.atomic.Value(u8) = .init(0);

fn ssePeerResetHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    body.writeAll("data: before-reset\n\n") catch |err| {
        peer_reset_err.store(if (err == error.PeerReset) 3 else 5, .release);
        return err;
    };
    // Keep writing until peer RST surfaces.
    while (true) {
        body.writeAll("data: <div>more</div>\n\n") catch |err| {
            peer_reset_err.store(if (err == error.PeerReset) 3 else 1, .release);
            if (err == error.PeerReset) return;
            return err;
        };
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

fn hangCompressedSse(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hold\n\n");
    while (true) {
        zio.sleep(.fromMilliseconds(50)) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            return err;
        };
        if (body.terminalCause() != null) return error.Canceled;
    }
}

fn streamExactOutboundHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    // Write exactly outbound_bytes_per_stream.
    const n = 64 * 1024;
    const buf = try req.arena.alloc(u8, n);
    @memset(buf, 'Z');
    var body = try resp.start(200, &.{.{ .name = "content-type", .value = "text/plain" }});
    try body.writeAll(buf);
    try body.finish();
}

// --- tests ------------------------------------------------------------------

test "I2 send path round-trips brotli" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/t",
                .handler = .{ .ptr = @constCast(&dummy), .runFn = sendTextHandler },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    const wire = try h2c.buildClientHelloExtra(alloc, "/t", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 5000, doneEnd);
                    try std.testing.expect(cap.end_stream);
                    const ce = cap.headerValue("content-encoding") orelse return error.MissingContentEncoding;
                    try std.testing.expectEqualStrings("br", ce);
                    try std.testing.expect(cap.hasVaryAcceptEncoding());
                    const decoded = try brotli.Decoder.decompressAll(alloc, 2 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    const expect = "hello compressed world " ** 20;
                    try std.testing.expectEqualStrings(expect, decoded);
                }
            }.go);
        }
    }.f);
}

test "I3 big compressible body shrinks under 25 percent" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/big",
                .handler = .{ .ptr = @constCast(&dummy), .runFn = sendBigHandler },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "gzip, br" }};
                    const wire = try h2c.buildClientHelloExtra(alloc, "/big", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 8000, doneEnd);
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    try std.testing.expect(cap.body.items.len * 4 <= 64 * 1024);
                    const decoded = try brotli.Decoder.decompressAll(alloc, 4 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    try std.testing.expect(decoded.len >= 64 * 1024);
                }
            }.go);
        }
    }.f);
}

// Pins fix-round-1: 5 MiB one-shot + br must not kill the process; second
// request on the same connection proves the server stayed up.
test "five MiB one-shot br round-trip keeps server alive" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/big", .handler = .{ .ptr = @constCast(&dummy), .runFn = sendFiveMiBHandler } },
                .{ .method = .GET, .path = "/t", .handler = .{ .ptr = @constCast(&dummy), .runFn = sendTextHandler } },
            };
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();

                    // Large client windows so a multi-MiB payload can drain even
                    // if compression unexpectedly falls back to identity.
                    var wire = try h2c.buildClientPrefaceAndSettings(alloc);
                    defer wire.deinit(alloc);
                    {
                        var sbuf: [64]u8 = undefined;
                        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 1 << 23 }};
                        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
                        try wire.appendSlice(alloc, sbuf[0..sn]);
                    }
                    try h2c.appendWindowUpdate(alloc, &wire, 0, 1 << 23);
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    try h2c.appendHeadersExtra(alloc, &wire, 1, "GET", "/big", true, &extra);
                    try writeAllStream(stream, wire.items);

                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 30000, doneEnd);
                    try std.testing.expect(!cap.saw_rst);
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    const decoded = try brotli.Decoder.decompressAll(alloc, 4 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    try std.testing.expectEqual(@as(usize, 5 * 1024 * 1024), decoded.len);
                    try std.testing.expect(std.mem.allEqual(u8, decoded, 'B'));

                    // Keep-alive probe: server must still accept work.
                    var wire2: std.ArrayList(u8) = .empty;
                    defer wire2.deinit(alloc);
                    try h2c.appendHeadersExtra(alloc, &wire2, 3, "GET", "/t", true, &extra);
                    try writeAllStream(stream, wire2.items);
                    var cap2 = try Capture.init(alloc);
                    defer cap2.deinit();
                    try readUntil(stream, &cap2, 8000, doneEnd);
                    try std.testing.expectEqualStrings("br", cap2.headerValue("content-encoding").?);
                    const d2 = try brotli.Decoder.decompressAll(alloc, 4 * 1024 * 1024, cap2.body.items);
                    defer alloc.free(d2);
                    const expect = "hello compressed world " ** 20;
                    try std.testing.expectEqualStrings(expect, d2);
                }
            }.go);
        }
    }.f);
}

test "I5 hygiene: min bytes, content-encoding passthrough, png, vary merge, no content-length" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/small", .handler = .{ .ptr = @constCast(&dummy), .runFn = sendSmallHandler } },
                .{ .method = .GET, .path = "/ce", .handler = .{ .ptr = @constCast(&dummy), .runFn = sendWithContentEncoding } },
                .{ .method = .GET, .path = "/png", .handler = .{ .ptr = @constCast(&dummy), .runFn = sendPngHandler } },
                .{ .method = .GET, .path = "/vary", .handler = .{ .ptr = @constCast(&dummy), .runFn = sendWithVaryAndLength } },
            };
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const cases = [_]struct { path: []const u8, want_br: bool, check_vary: bool }{
                        .{ .path = "/small", .want_br = false, .check_vary = true },
                        .{ .path = "/ce", .want_br = false, .check_vary = false },
                        .{ .path = "/png", .want_br = false, .check_vary = false },
                        .{ .path = "/vary", .want_br = true, .check_vary = true },
                    };
                    for (cases) |case| {
                        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                        var stream = try peer.connect(.{});
                        defer stream.close();
                        const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                        const wire = try h2c.buildClientHelloExtra(alloc, case.path, &extra);
                        defer alloc.free(wire);
                        try writeAllStream(stream, wire);
                        var cap = try Capture.init(alloc);
                        defer cap.deinit();
                        try readUntil(stream, &cap, 5000, doneEnd);
                        const ce = cap.headerValue("content-encoding");
                        if (case.want_br) {
                            try std.testing.expectEqualStrings("br", ce.?);
                            try std.testing.expect(cap.headerValue("content-length") == null);
                        } else if (std.mem.eql(u8, case.path, "/ce")) {
                            try std.testing.expectEqualStrings("gzip", ce.?);
                        } else {
                            try std.testing.expect(ce == null);
                        }
                        if (case.check_vary) try std.testing.expect(cap.hasVaryAcceptEncoding());
                        if (std.mem.eql(u8, case.path, "/vary")) {
                            // origin must still be present
                            var saw_origin = false;
                            for (cap.headers.items) |h| {
                                if (!std.ascii.eqlIgnoreCase(h.name, "vary")) continue;
                                if (std.mem.indexOf(u8, h.value, "origin") != null) saw_origin = true;
                            }
                            try std.testing.expect(saw_origin);
                        }
                    }
                }
            }.go);
        }
    }.f);
}

test "I4 SSE flush is incrementally decodable before next event" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/sse",
                .handler = .{ .ptr = @constCast(&dummy), .runFn = sseHandler },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    // end_stream false so we can observe progressive DATA before finish... actually
                    // startSse opens stream; client GET with end_stream true is fine.
                    const wire = try h2c.buildClientHelloExtra(alloc, "/sse", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);

                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    const dec = try brotli.Decoder.create(alloc, 2 * 1024 * 1024);
                    defer dec.destroy();
                    var plain: std.ArrayList(u8) = .empty;
                    defer plain.deinit(alloc);

                    const e1 = "data: <div id=\"a\">one</div>\n\n";
                    const e2 = "data: <div id=\"a\">two-repeats-similar-html</div>\n\n";
                    const e3 = "data: <div id=\"a\">three-still-similar</div>\n\n";

                    // Incremental: after each new DATA chunk, decode and check prefix.
                    var buf: [16 * 1024]u8 = undefined;
                    var saw_e1 = false;
                    var saw_e2 = false;
                    var waited: u64 = 0;
                    while (waited < 8000) : (waited += 20) {
                        const n = stream.read(&buf, .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
                            error.Timeout => 0,
                            else => return err,
                        };
                        if (n == 0) continue;
                        const before_chunks = cap.data_chunks.items.len;
                        try cap.ingest(buf[0..n]);
                        // Feed only newly observed DATA payload to the decoder.
                        var ci = before_chunks;
                        while (ci < cap.data_chunks.items.len) : (ci += 1) {
                            _ = try dec.decompress(cap.data_chunks.items[ci], &plain, alloc);
                        }
                        if (!saw_e1 and plain.items.len >= e1.len) {
                            try std.testing.expectEqualStrings(e1, plain.items[0..e1.len]);
                            saw_e1 = true;
                        }
                        if (saw_e1 and !saw_e2 and plain.items.len >= e1.len + e2.len) {
                            try std.testing.expectEqualStrings(e1 ++ e2, plain.items[0 .. e1.len + e2.len]);
                            saw_e2 = true;
                        }
                        if (cap.end_stream) break;
                    }
                    try std.testing.expect(saw_e1);
                    try std.testing.expect(saw_e2);
                    try std.testing.expect(cap.end_stream);
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    try std.testing.expectEqualStrings(e1 ++ e2 ++ e3, plain.items);
                }
            }.go);
        }
    }.f);
}

test "I2 streaming start path round-trips" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/s",
                .handler = .{ .ptr = @constCast(&dummy), .runFn = streamHandler },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "BR" }};
                    const wire = try h2c.buildClientHelloExtra(alloc, "/s", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 5000, doneEnd);
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    const decoded = try brotli.Decoder.decompressAll(alloc, 2 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    try std.testing.expect(std.mem.indexOf(u8, decoded, "part-a-") != null);
                    try std.testing.expect(std.mem.endsWith(u8, decoded, "-tail"));
                }
            }.go);
        }
    }.f);
}

test "I6 pool exhaustion serves identity and counts fallback" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            var lim = enableCompressionLimits(.{});
            lim.compression_contexts_per_server = 1;
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/hold", .handler = .{ .ptr = @constCast(&dummy), .runFn = hangCompressedSse } },
                .{ .method = .GET, .path = "/t", .handler = .{ .ptr = @constCast(&dummy), .runFn = sendTextHandler } },
            };
            try runServer(rt, gpa, &routes, lim, struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    // Hold the single encoder context with a live SSE.
                    const peer1 = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var s1 = try peer1.connect(.{});
                    defer s1.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    const w1 = try h2c.buildClientHelloExtra(alloc, "/hold", &extra);
                    defer alloc.free(w1);
                    try writeAllStream(s1, w1);
                    var cap1 = try Capture.init(alloc);
                    defer cap1.deinit();
                    try readUntil(s1, &cap1, 5000, doneHeaders);
                    try std.testing.expectEqualStrings("br", cap1.headerValue("content-encoding").?);

                    // Second stream must be identity.
                    const peer2 = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var s2 = try peer2.connect(.{});
                    defer s2.close();
                    const w2 = try h2c.buildClientHelloExtra(alloc, "/t", &extra);
                    defer alloc.free(w2);
                    try writeAllStream(s2, w2);
                    var cap2 = try Capture.init(alloc);
                    defer cap2.deinit();
                    try readUntil(s2, &cap2, 5000, doneEnd);
                    try std.testing.expect(cap2.headerValue("content-encoding") == null);
                    try std.testing.expect(cap2.hasVaryAcceptEncoding());
                    const expect = "hello compressed world " ** 20;
                    try std.testing.expectEqualStrings(expect, cap2.body.items);
                    const pool = server.compression_pool orelse return error.NoPool;
                    try std.testing.expect(pool.identity_fallbacks.load(.acquire) >= 1);
                }
            }.go);
        }
    }.f);
}

test "I8 compressed stream surfaces PeerReset exactly" {
    peer_reset_err.store(0, .release);
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/sse",
                .handler = .{ .ptr = @constCast(&dummy), .runFn = ssePeerResetHandler },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    const wire = try h2c.buildClientHelloExtra(alloc, "/sse", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);
                    // Wait until some DATA, then RST.
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 5000, struct {
                        fn d(c: *Capture) bool {
                            return c.data_frames > 0 or c.headers.items.len > 0;
                        }
                    }.d);
                    var rst: [13]u8 = undefined;
                    const rn = try frame.Serializer.rstStream(&rst, 1, .cancel);
                    try writeAllStream(stream, rst[0..rn]);
                    var waited: u64 = 0;
                    while (waited < 5000) : (waited += 20) {
                        if (peer_reset_err.load(.acquire) == 3) break;
                        zio.sleep(.fromMilliseconds(20)) catch {};
                    }
                    try std.testing.expectEqual(@as(u8, 3), peer_reset_err.load(.acquire));
                    // Drain accounting.
                    var t: u64 = 0;
                    while (t < 3000) : (t += 20) {
                        if (server.accounting.active_streams.load(.acquire) == 0) break;
                        zio.sleep(.fromMilliseconds(20)) catch {};
                    }
                }
            }.go);
        }
    }.f);
}

test "I4 large SSE event over outbound_bytes_per_stream still flushes decodably" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            var lim = enableCompressionLimits(.{});
            lim.outbound_bytes_per_stream = 64 * 1024;
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/sse",
                .handler = .{ .ptr = @constCast(&dummy), .runFn = sseLargeEventHandler },
            }};
            try runServer(rt, gpa, &routes, lim, struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    // Enlarge client window so the large event can drain.
                    var wire = try h2c.buildClientPrefaceAndSettings(alloc);
                    defer wire.deinit(alloc);
                    {
                        var sbuf: [64]u8 = undefined;
                        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 1 << 22 }};
                        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
                        try wire.appendSlice(alloc, sbuf[0..sn]);
                    }
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    try h2c.appendHeadersExtra(alloc, &wire, 1, "GET", "/sse", true, &extra);
                    try writeAllStream(stream, wire.items);
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 10000, doneEnd);
                    try std.testing.expect(cap.data_frames >= 2); // split across DATA frames
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    const decoded = try brotli.Decoder.decompressAll(alloc, 4 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    try std.testing.expect(decoded.len > 70 * 1024);
                    try std.testing.expect(std.mem.startsWith(u8, decoded, "data: "));
                }
            }.go);
        }
    }.f);
}

test "lifecycle: shutdown releases live encoder context" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/hold",
                .handler = .{ .ptr = @constCast(&dummy), .runFn = hangCompressedSse },
            }};
            const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
            var server = try starh2.Server.init(gpa, rt.io(), .{
                .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
                .routes = &routes,
                .tls = null,
                .limits = enableCompressionLimits(.{}),
            });
            defer server.deinit(gpa);
            var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
            try server.waitUntilListening(5 * std.time.ns_per_s);
            const port = server.localAddress(0).getPort();
            const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
            var stream = try peer.connect(.{});
            defer stream.close();
            const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
            const wire = try h2c.buildClientHelloExtra(gpa, "/hold", &extra);
            defer gpa.free(wire);
            try writeAllStream(stream, wire);
            var cap = try Capture.init(gpa);
            defer cap.deinit();
            try readUntil(stream, &cap, 5000, doneHeaders);
            try std.testing.expect(server.compression_pool.?.liveCount() >= 1);
            server.requestShutdown();
            serve_handle.join() catch {};
            try std.testing.expectEqual(@as(usize, 0), server.compression_pool.?.liveCount());
        }
    }.f);
}

test "write sizes: one-byte SSE and exact outbound quantum" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/one", .handler = .{ .ptr = @constCast(&dummy), .runFn = sseOneByteHandler } },
                .{ .method = .GET, .path = "/exact", .handler = .{ .ptr = @constCast(&dummy), .runFn = streamExactOutboundHandler } },
            };
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    for ([_][]const u8{ "/one", "/exact" }) |path| {
                        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                        var stream = try peer.connect(.{});
                        defer stream.close();
                        const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                        const wire = try h2c.buildClientHelloExtra(alloc, path, &extra);
                        defer alloc.free(wire);
                        try writeAllStream(stream, wire);
                        var cap = try Capture.init(alloc);
                        defer cap.deinit();
                        try readUntil(stream, &cap, 8000, doneEnd);
                        try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                        const decoded = try brotli.Decoder.decompressAll(alloc, 4 * 1024 * 1024, cap.body.items);
                        defer alloc.free(decoded);
                        if (std.mem.eql(u8, path, "/one")) {
                            try std.testing.expectEqualStrings("data: x\n\n", decoded);
                        } else {
                            try std.testing.expectEqual(@as(usize, 64 * 1024), decoded.len);
                        }
                    }
                }
            }.go);
        }
    }.f);
}

test "no accept-encoding stays identity" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/t",
                .handler = .{ .ptr = @constCast(&dummy), .runFn = sendTextHandler },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const wire = try h2c.buildClientHello(alloc, "/t");
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 5000, doneEnd);
                    try std.testing.expect(cap.headerValue("content-encoding") == null);
                    // Eligible by configuration still adds Vary.
                    try std.testing.expect(cap.hasVaryAcceptEncoding());
                    const expect = "hello compressed world " ** 20;
                    try std.testing.expectEqualStrings(expect, cap.body.items);
                }
            }.go);
        }
    }.f);
}
