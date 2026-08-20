//! Actor-owned handler deadlines (t-824). T1 fails if waitUntil is Io.sleep.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const dummy: u8 = 0;
const frame = starh2.core.frame;

const token_t1 = "data: token-t1\n\n";
const token_early = "data: token-early\n\n";
const token_late = "data: token-late\n\n";

var t3_err: std.atomic.Value(u8) = .init(0);

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
    oneshot_ended: usize = 0,
    saw_rst: bool = false,

    fn init(gpa: std.mem.Allocator) !Capture {
        var parser = frame.Parser.init(gpa, frame.DEFAULT_MAX_FRAME_SIZE);
        parser.skipPreface();
        return .{ .gpa = gpa, .parser = parser };
    }

    fn deinit(self: *Capture) void {
        self.body.deinit(self.gpa);
        self.parser.deinit();
    }

    fn ingest(self: *Capture, bytes: []const u8) !void {
        var rem = bytes;
        while (rem.len > 0) {
            const maybe = try self.parser.ingestOne(rem);
            if (maybe) |r| {
                defer r.event.deinit(self.gpa);
                rem = rem[r.consumed..];
                const hdr = r.event.header;
                switch (hdr.type) {
                    .data => {
                        try self.body.appendSlice(self.gpa, r.event.payload.bytes());
                        if (hdr.flags.end_stream and hdr.stream_id != 1) self.oneshot_ended += 1;
                    },
                    .headers => {
                        if (hdr.flags.end_stream and hdr.stream_id != 1) self.oneshot_ended += 1;
                    },
                    .rst_stream => self.saw_rst = true,
                    else => {},
                }
            } else break;
        }
    }

    fn has(self: *const Capture, needle: []const u8) bool {
        return std.mem.indexOf(u8, self.body.items, needle) != null;
    }
};

fn sseTokenT1(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    const now = std.Io.Clock.awake.now(resp.io);
    const deadline = std.Io.Timestamp.fromNanoseconds(now.nanoseconds + 20 * std.time.ns_per_ms);
    try body.waitUntil(deadline);
    try body.writeAll(token_t1);
    try body.finish();
}

fn completeOk(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

fn sseEarly(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    const now = std.Io.Clock.awake.now(resp.io);
    try body.waitUntil(std.Io.Timestamp.fromNanoseconds(now.nanoseconds + 10 * std.time.ns_per_ms));
    try body.writeAll(token_early);
    try body.finish();
}

fn sseLate(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    const now = std.Io.Clock.awake.now(resp.io);
    try body.waitUntil(std.Io.Timestamp.fromNanoseconds(now.nanoseconds + 50 * std.time.ns_per_ms));
    try body.writeAll(token_late);
    try body.finish();
}

fn sseFar(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    const now = std.Io.Clock.awake.now(resp.io);
    const far = std.Io.Timestamp.fromNanoseconds(now.nanoseconds + 3600 * std.time.ns_per_s);
    body.waitUntil(far) catch |err| {
        t3_err.store(switch (err) {
            error.PeerReset => 3,
            error.ConnectionClosed => 2,
            error.Canceled => 1,
            else => 4,
        }, .release);
        return err;
    };
    try body.writeAll("data: should-not\n\n");
    try body.finish();
}

fn runServer(
    rt: *zio.Runtime,
    gpa: std.mem.Allocator,
    routes: []const starh2.Route,
    comptime body: anytype,
) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var limits = starh2.Limits.defaults;
    limits.max_connections = 8;
    limits.max_streams_per_connection = 128;
    limits.max_streams_per_server = 256;
    limits.cancellation_reaper_jobs = 256;
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

test "T1: 1 executor, ingest stays ready, waitUntil fires" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(1),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var handle = try rt.spawn(struct {
        fn f(runtime: *zio.Runtime, alloc: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/sse-token", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseTokenT1 } } },
                .{ .method = .GET, .path = "/", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = completeOk } } },
            };
            try runServer(runtime, alloc, &routes, struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, a: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const hello = try h2c.buildClientHello(a, "/sse-token");
                    defer a.free(hello);
                    try writeAllStream(stream, hello);

                    var cap = try Capture.init(a);
                    defer cap.deinit();
                    var sid: u31 = 3;
                    var buf: [16 * 1024]u8 = undefined;
                    const t0 = zio.Timestamp.now(.monotonic).toNanoseconds();
                    while (zio.Timestamp.now(.monotonic).toNanoseconds() -% t0 < 100 * std.time.ns_per_ms) {
                        var req: std.ArrayList(u8) = .empty;
                        defer req.deinit(a);
                        try h2c.appendHeaders(a, &req, sid, "/", true);
                        try writeAllStream(stream, req.items);
                        sid += 2;
                        const n = stream.read(&buf, .{ .duration = .fromMilliseconds(5) }) catch |err| switch (err) {
                            error.Timeout => 0,
                            else => return err,
                        };
                        if (n > 0) try cap.ingest(buf[0..n]);
                        if (cap.has(token_t1)) break;
                    }
                    if (!cap.has(token_t1)) return error.TokenMissing;
                    if (cap.oneshot_ended == 0) return error.IngestParked;
                }
            }.go);
        }
    }.f, .{ rt, gpa });
    try handle.join();
}

test "T2: two deadlines keep order; RST of one does not strand the other" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var handle = try rt.spawn(struct {
        fn f(runtime: *zio.Runtime, alloc: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/early", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseEarly } } },
                .{ .method = .GET, .path = "/late", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseLate } } },
            };
            try runServer(runtime, alloc, &routes, struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, a: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    var wire = try h2c.buildClientPrefaceAndSettings(a);
                    defer wire.deinit(a);
                    try h2c.appendHeaders(a, &wire, 1, "/early", true);
                    try h2c.appendHeaders(a, &wire, 3, "/late", true);
                    try writeAllStream(stream, wire.items);

                    var cap = try Capture.init(a);
                    defer cap.deinit();
                    var buf: [16 * 1024]u8 = undefined;
                    const t0 = zio.Timestamp.now(.monotonic).toNanoseconds();
                    while (zio.Timestamp.now(.monotonic).toNanoseconds() -% t0 < 20 * std.time.ns_per_ms) {
                        const n = stream.read(&buf, .{ .duration = .fromMilliseconds(2) }) catch |err| switch (err) {
                            error.Timeout => 0,
                            else => return err,
                        };
                        if (n > 0) try cap.ingest(buf[0..n]);
                    }
                    try std.testing.expect(cap.has(token_early));
                    try std.testing.expect(!cap.has(token_late));

                    var rst: [13]u8 = undefined;
                    const rn = try frame.Serializer.rstStream(&rst, 1, .cancel);
                    try writeAllStream(stream, rst[0..rn]);

                    const t1 = zio.Timestamp.now(.monotonic).toNanoseconds();
                    while (zio.Timestamp.now(.monotonic).toNanoseconds() -% t1 < 80 * std.time.ns_per_ms) {
                        const n = stream.read(&buf, .{ .duration = .fromMilliseconds(5) }) catch |err| switch (err) {
                            error.Timeout => 0,
                            else => return err,
                        };
                        if (n > 0) try cap.ingest(buf[0..n]);
                        if (cap.has(token_late)) break;
                    }
                    try std.testing.expect(cap.has(token_late));
                }
            }.go);
        }
    }.f, .{ rt, gpa });
    try handle.join();
}

test "T3: terminal during waitUntil returns exact error and frees the heap" {
    t3_err.store(0, .release);
    starh2.edge.connection.test_deadline_waits.store(0, .release);
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var handle = try rt.spawn(struct {
        fn f(runtime: *zio.Runtime, alloc: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/far",
                .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseFar } },
            }};
            try runServer(runtime, alloc, &routes, struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, a: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const hello = try h2c.buildClientHello(a, "/far");
                    defer a.free(hello);
                    try writeAllStream(stream, hello);

                    var cap = try Capture.init(a);
                    defer cap.deinit();
                    var buf: [16 * 1024]u8 = undefined;
                    var waited: u64 = 0;
                    while (waited < 2000) : (waited += 10) {
                        if (starh2.edge.connection.test_observed_live_handlers.load(.acquire) >= 1) break;
                        const n = stream.read(&buf, .{ .duration = .fromMilliseconds(10) }) catch |err| switch (err) {
                            error.Timeout => 0,
                            else => return err,
                        };
                        if (n > 0) try cap.ingest(buf[0..n]);
                    }
                    try std.testing.expect(starh2.edge.connection.test_observed_live_handlers.load(.acquire) >= 1);

                    var rst: [13]u8 = undefined;
                    const rn = try frame.Serializer.rstStream(&rst, 1, .cancel);
                    try writeAllStream(stream, rst[0..rn]);

                    waited = 0;
                    while (waited < 3000) : (waited += 10) {
                        if (t3_err.load(.acquire) != 0 and
                            starh2.edge.connection.test_observed_live_handlers.load(.acquire) == 0 and
                            starh2.edge.connection.test_deadline_waits.load(.acquire) == 0)
                            break;
                        const n = stream.read(&buf, .{ .duration = .fromMilliseconds(10) }) catch |err| switch (err) {
                            error.Timeout => 0,
                            else => return err,
                        };
                        if (n > 0) try cap.ingest(buf[0..n]);
                    }
                    try std.testing.expectEqual(@as(u8, 3), t3_err.load(.acquire));
                    try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_observed_live_handlers.load(.acquire));
                    try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_deadline_waits.load(.acquire));
                    try std.testing.expectEqual(@as(usize, 0), server.accounting.active_streams.load(.acquire));
                }
            }.go);
        }
    }.f, .{ rt, gpa });
    try handle.join();
}
