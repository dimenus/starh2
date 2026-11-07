//! Actor-owned handler deadlines (t-824). T1 fails if waitUntil is Io.sleep.
const std = @import("std");
const zio = @import("zio");
const serve_deadline = @import("serve_deadline.zig");
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
    limits.max_task_handlers = 256;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = .{ .address = addr } }},
        .routes = routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_deadline.join(&server, &serve_handle);
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    try body(&server, rt, gpa);
}

test "T1: 1 executor, ingest stays ready, waitUntil fires" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(1),
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
                    // The RST must come while the handler is inside
                    // `waitUntil`, the call under test. An admitted handler
                    // (`test_observed_live_handlers`) may not have reached
                    // it yet: an RST then fails its `startSse` with
                    // PeerReset and it never waits (t-3900: 6 of 210 runs
                    // on the Mac once the server had a once-a-second task).
                    var waited: u64 = 0;
                    while (waited < 2000) : (waited += 10) {
                        if (starh2.edge.connection.test_deadline_waits.load(.acquire) >= 1) break;
                        const n = stream.read(&buf, .{ .duration = .fromMilliseconds(10) }) catch |err| switch (err) {
                            error.Timeout => 0,
                            else => return err,
                        };
                        if (n > 0) try cap.ingest(buf[0..n]);
                    }
                    try std.testing.expect(starh2.edge.connection.test_observed_live_handlers.load(.acquire) >= 1);
                    try std.testing.expect(starh2.edge.connection.test_deadline_waits.load(.acquire) >= 1);

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

// T4: `Limits.handler_deadline_slack_ns`, through a real server on both the
// HTTP/2 and the HTTP/1.1 wait paths. The grid math is written out here
// again rather than calling `Limits.handlerDeadlineNs`, so the test does not
// share the code it checks.
const t4_slack_ns: u64 = 200 * std.time.ns_per_ms;
const t4_token = "data: t4-done\n\n";

/// 1: the first wait returned at or after the rounded deadline. 2: before it.
var t4_woke: std.atomic.Value(u8) = .init(0);
/// How long a wait on a deadline already past took.
var t4_late_wait_ns: std.atomic.Value(u64) = .init(std.math.maxInt(u64));

fn t4GridUp(ns: u64) u64 {
    return ns + (t4_slack_ns - ns % t4_slack_ns) % t4_slack_ns;
}

fn t4NowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn sseSlack(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    const io = resp.io;
    // 1 ms past a multiple, so an exact wait returns about 199 ms before the
    // rounded one: the two cannot be confused.
    const deadline = t4GridUp(t4NowNs(io) + 1) + std.time.ns_per_ms;
    try body.waitUntil(.fromNanoseconds(@intCast(deadline)));
    const woke = t4NowNs(io);
    t4_woke.store(if (woke >= t4GridUp(deadline)) 1 else 2, .release);

    // `woke` is just past a multiple, so a deadline 1 ns in the past rounds
    // up to the NEXT multiple, nearly a whole slack away. Waiting for it
    // would be the bug.
    const before = t4NowNs(io);
    try body.waitUntil(.fromNanoseconds(@intCast(before - 1)));
    t4_late_wait_ns.store(t4NowNs(io) - before, .release);

    try body.writeAll(t4_token);
    try body.finish();
}

fn t4ReadUntilToken(stream: zio.net.Stream) !void {
    var seen: [64 * 1024]u8 = undefined;
    var len: usize = 0;
    const t0 = zio.Timestamp.now(.monotonic).toNanoseconds();
    while (zio.Timestamp.now(.monotonic).toNanoseconds() -% t0 < 3 * std.time.ns_per_s) {
        if (len == seen.len) return error.ResponseTooLarge;
        const n = stream.read(seen[len..], .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
            error.Timeout => 0,
            else => return err,
        };
        len += n;
        if (std.mem.indexOf(u8, seen[0..len], "t4-done") != null) return;
    }
    return error.TokenMissing;
}

fn t4Check() !void {
    try std.testing.expectEqual(@as(u8, 1), t4_woke.load(.acquire));
    try std.testing.expect(t4_late_wait_ns.load(.acquire) < t4_slack_ns / 2);
}

test "T4: deadline slack rounds the wait up, and never holds back a late handler (h2c and h1c)" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(2),
    });
    defer rt.deinit();
    var handle = try rt.spawn(struct {
        fn f(runtime: *zio.Runtime, a: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/slack", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseSlack } } },
            };
            const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
            var limits = starh2.Limits.defaults;
            limits.max_connections = 8;
            limits.max_streams_per_connection = 128;
            limits.max_streams_per_server = 256;
            limits.max_task_handlers = 256;
            limits.handler_deadline_slack_ns = t4_slack_ns;
            var server = try starh2.Server.init(a, runtime.io(), .{
                .endpoints = &.{ .{ .h2c_prior_knowledge = .{ .address = addr } }, .{ .h1c = .{ .address = addr } } },
                .routes = &routes,
                .tls = null,
                .limits = limits,
            });
            defer server.deinit(a);
            var serve_handle = try runtime.spawn(starh2.Server.serve, .{ &server, a });
            defer {
                server.requestShutdown();
                serve_deadline.join(&server, &serve_handle);
            }
            try server.waitUntilListening(5 * std.time.ns_per_s);

            {
                t4_woke.store(0, .release);
                t4_late_wait_ns.store(std.math.maxInt(u64), .release);
                const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());
                var stream = try peer.connect(.{});
                defer stream.close();
                const hello = try h2c.buildClientHello(a, "/slack");
                defer a.free(hello);
                try writeAllStream(stream, hello);
                try t4ReadUntilToken(stream);
                try t4Check();
            }
            {
                t4_woke.store(0, .release);
                t4_late_wait_ns.store(std.math.maxInt(u64), .release);
                const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(1).getPort());
                var stream = try peer.connect(.{});
                defer stream.close();
                try writeAllStream(stream, "GET /slack HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
                try t4ReadUntilToken(stream);
                try t4Check();
            }
        }
    }.f, .{ rt, gpa });
    try handle.join();
}

// T5: a deadline before zero is a deadline already past. `waitUntil` returns
// at once, on both wait paths, instead of trapping on the narrowing to the
// awake clock's u64 (zio's `Timeout` saturates the same way).
const t5_token = "data: t5-done\n\n";
var t5_wait_ns: std.atomic.Value(u64) = .init(std.math.maxInt(u64));

fn ssePastZero(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    const before = t4NowNs(resp.io);
    try body.waitUntil(.fromNanoseconds(-1));
    t5_wait_ns.store(t4NowNs(resp.io) - before, .release);
    try body.writeAll(t5_token);
    try body.finish();
}

fn t5ReadUntilToken(stream: zio.net.Stream) !void {
    var seen: [64 * 1024]u8 = undefined;
    var len: usize = 0;
    const t0 = zio.Timestamp.now(.monotonic).toNanoseconds();
    while (zio.Timestamp.now(.monotonic).toNanoseconds() -% t0 < 3 * std.time.ns_per_s) {
        if (len == seen.len) return error.ResponseTooLarge;
        const n = stream.read(seen[len..], .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
            error.Timeout => 0,
            else => return err,
        };
        len += n;
        if (std.mem.indexOf(u8, seen[0..len], "t5-done") != null) return;
    }
    return error.TokenMissing;
}

test "T5: a deadline before zero returns at once on both wait paths (h2c and h1c)" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(2),
    });
    defer rt.deinit();
    var handle = try rt.spawn(struct {
        fn f(runtime: *zio.Runtime, a: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/past", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = ssePastZero } } },
            };
            const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
            var limits = starh2.Limits.defaults;
            limits.max_connections = 8;
            limits.max_streams_per_connection = 128;
            limits.max_streams_per_server = 256;
            limits.max_task_handlers = 256;
            var server = try starh2.Server.init(a, runtime.io(), .{
                .endpoints = &.{ .{ .h2c_prior_knowledge = .{ .address = addr } }, .{ .h1c = .{ .address = addr } } },
                .routes = &routes,
                .tls = null,
                .limits = limits,
            });
            defer server.deinit(a);
            var serve_handle = try runtime.spawn(starh2.Server.serve, .{ &server, a });
            defer {
                server.requestShutdown();
                serve_deadline.join(&server, &serve_handle);
            }
            try server.waitUntilListening(5 * std.time.ns_per_s);
            {
                t5_wait_ns.store(std.math.maxInt(u64), .release);
                const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());
                var stream = try peer.connect(.{});
                defer stream.close();
                const hello = try h2c.buildClientHello(a, "/past");
                defer a.free(hello);
                try writeAllStream(stream, hello);
                try t5ReadUntilToken(stream);
                try std.testing.expect(t5_wait_ns.load(.acquire) < 100 * std.time.ns_per_ms);
            }
            {
                t5_wait_ns.store(std.math.maxInt(u64), .release);
                const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(1).getPort());
                var stream = try peer.connect(.{});
                defer stream.close();
                try writeAllStream(stream, "GET /past HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
                try t5ReadUntilToken(stream);
                try std.testing.expect(t5_wait_ns.load(.acquire) < 100 * std.time.ns_per_ms);
            }
        }
    }.f, .{ rt, gpa });
    try handle.join();
}

// T6: a handler's write moves the actor's park deadline, and the parked actor
// wakes at the moved deadline. The actor parks with only the connection idle
// timer armed (30 s). Then a task handler writes DATA that the peer's zero
// window holds, which starts the stream's response-rate clock
// (`Limits.response_min_rate_grace_ns`, 300 ms here). The actor reads its park
// deadline under `session_mu` (`Connection.parkDeadlineNs`), so it learns of
// the new clock only because the handler's `unlockSession` rings the doorbell
// and the next turn recomputes. If either half is missing, the stream is
// reset at the idle timer instead, 30 s later, and the kill misses its window.
const t6_grace_ns: u64 = 300 * std.time.ns_per_ms;
var t6_write_ns: std.atomic.Value(u64) = .init(0);
var t6_killed_ns: std.atomic.Value(u64) = .init(0);
var t6_handler_err: std.atomic.Value(u8) = .init(0);

fn t6WriteThenFlush(body: *starh2.Body) starh2.ResponseError!void {
    try body.writeAll("data: held-by-the-window\n\n");
    try body.flush();
}

fn sseLateWrite(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.flush();
    // The precondition, read from the actor's own park gauge: the actor sits
    // in its select, so the only way it can learn of the write below is a
    // wake. Twice, 50 ms apart, so a turn that is merely passing through the
    // select does not count.
    const conn_mod = starh2.edge.connection;
    if (comptime @TypeOf(conn_mod.test_actors_in_select) != void) {
        const give_up = t4NowNs(resp.io) + 5 * std.time.ns_per_s;
        while (true) {
            if (conn_mod.test_actors_in_select.load(.acquire) == 1) {
                try zio.sleep(.fromMilliseconds(50));
                if (conn_mod.test_actors_in_select.load(.acquire) == 1) break;
            }
            if (t4NowNs(resp.io) >= give_up) {
                t6_handler_err.store(1, .release);
                return error.ActorNeverParked;
            }
            try zio.sleep(.fromMilliseconds(5));
        }
    } else {
        try zio.sleep(.fromMilliseconds(300));
    }
    t6_write_ns.store(t4NowNs(resp.io), .release);
    t6WriteThenFlush(&body) catch |err| {
        if (err == error.SlowConsumer) {
            t6_killed_ns.store(t4NowNs(resp.io), .release);
        } else t6_handler_err.store(3, .release);
        return err;
    };
    // The window is zero: a flush that succeeds means the bytes left without
    // credit.
    t6_handler_err.store(4, .release);
}

test "T6: a handler write that starts a response-rate clock wakes the parked actor at its deadline" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(2),
    });
    defer rt.deinit();
    var handle = try rt.spawn(struct {
        fn f(runtime: *zio.Runtime, a: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/late", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseLateWrite } } },
            };
            const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
            var limits = starh2.Limits.defaults;
            limits.response_min_rate_grace_ns = t6_grace_ns;
            limits.connection_idle_timeout_ns = 30 * std.time.ns_per_s;
            var server = try starh2.Server.init(a, runtime.io(), .{
                .endpoints = &.{.{ .h2c_prior_knowledge = .{ .address = addr } }},
                .routes = &routes,
                .tls = null,
                .limits = limits,
            });
            defer server.deinit(a);
            var serve_handle = try runtime.spawn(starh2.Server.serve, .{ &server, a });
            defer {
                server.requestShutdown();
                serve_deadline.join(&server, &serve_handle);
            }
            try server.waitUntilListening(5 * std.time.ns_per_s);

            t6_write_ns.store(0, .release);
            t6_killed_ns.store(0, .release);
            t6_handler_err.store(0, .release);
            const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());
            var stream = try peer.connect(.{});
            defer stream.close();
            // Every stream window is zero, so the handler's DATA waits on the
            // peer from its first byte.
            const hello = try h2c.buildClientHelloWindow(a, "/late", 1, 0, .empty);
            defer a.free(hello);
            try writeAllStream(stream, hello);

            var cap = try Capture.init(a);
            defer cap.deinit();
            var buf: [16 * 1024]u8 = undefined;
            const give_up = t4NowNs(runtime.io()) + 10 * std.time.ns_per_s;
            while (!cap.saw_rst) {
                if (t6_handler_err.load(.acquire) != 0) break;
                if (t4NowNs(runtime.io()) >= give_up) break;
                const n = stream.read(&buf, .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
                    error.Timeout => 0,
                    else => return err,
                };
                if (n > 0) try cap.ingest(buf[0..n]);
            }
            try std.testing.expectEqual(@as(u8, 0), t6_handler_err.load(.acquire));
            const write_ns = t6_write_ns.load(.acquire);
            try std.testing.expect(write_ns != 0);
            if (!cap.saw_rst) return error.NoResetWithinTenSeconds;
            // The handler's SlowConsumer is stamped after the reset is
            // queued; wait for it, it is a few ms behind.
            const stamp_by = t4NowNs(runtime.io()) + 2 * std.time.ns_per_s;
            while (t6_killed_ns.load(.acquire) == 0 and t4NowNs(runtime.io()) < stamp_by) {
                zio.sleep(.fromMilliseconds(5)) catch {};
            }
            const killed_ns = t6_killed_ns.load(.acquire);
            if (killed_ns == 0) return error.HandlerNeverSawSlowConsumer;
            const killed_ms = (killed_ns - write_ns) / std.time.ns_per_ms;
            std.debug.print("T6: stream reset {d} ms after the handler's write (grace {d} ms, idle timer 30000 ms)\n", .{ killed_ms, t6_grace_ns / std.time.ns_per_ms });
            // The clock starts at the actor's pop after the write, so never
            // before the grace. The ceiling is far below the 30 s idle timer
            // that is the only other wake, and allows a loaded machine.
            try std.testing.expect(killed_ns - write_ns >= t6_grace_ns);
            try std.testing.expect(killed_ms <= t6_grace_ns / std.time.ns_per_ms + 1500);
        }
    }.f, .{ rt, gpa });
    try handle.join();
}
