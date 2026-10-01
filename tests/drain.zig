//! Gate for t-2652: a draining connection must not hold its executor.
//!
//! Graceful shutdown keeps a connection alive through the PING guard and the
//! drain timeout. The actor waited in a `zio.select` that included the
//! server's shutdown event, which stays set once fired, so the select won at
//! once on every turn and the actor spun for the whole drain. Under pinned
//! scheduling that starves every other task on the executor.
//!
//! The test holds a live SSE handler through a drain of about 1.3 s, runs a
//! 10 ms ticker task on every executor, and fails if any ticker sees a gap
//! longer than `max_gap_ms`. Only meaningful under `-Dzio-scheduling=pinned`
//! (under work stealing an idle executor takes the ticker); skips otherwise.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const conn_mod = starh2.edge.connection;
const pinned = conn_mod.zio_scheduling == .pinned;
const executors = 4;
const max_gap_ms = 50;

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

fn writeAll(stream: zio.net.Stream, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) off += try stream.write(bytes[off..], .none);
}

var started: std.atomic.Value(u32) = .init(0);
const dummy: u8 = 0;

fn hangSse(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    _ = started.fetchAdd(1, .acq_rel);
    while (true) {
        try zio.sleep(.fromMilliseconds(20));
        if (body.terminalCause() != null) return error.Canceled;
    }
}

fn ticker(stop: *std.atomic.Value(bool), worst_ns: *std.atomic.Value(u64)) void {
    var last = nowNs();
    while (!stop.load(.acquire)) {
        zio.sleep(.fromMilliseconds(10)) catch {};
        const now = nowNs();
        const gap = now - last;
        if (gap > worst_ns.load(.monotonic)) worst_ns.store(gap, .monotonic);
        last = now;
    }
}

fn runDrain(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangSse } } },
    };
    var limits = starh2.Limits.defaults;
    limits.graceful_drain_timeout_ns = 300 * std.time.ns_per_ms;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);

    started.store(0, .release);
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());
    var client = try peer.connect(.{});
    defer client.close();
    var wire = try h2c.buildClientPrefaceAndSettings(gpa);
    defer wire.deinit(gpa);
    try h2c.appendHeaders(gpa, &wire, 1, "/sse", true);
    try writeAll(client, wire.items);
    const deadline = nowNs() +% 5 * std.time.ns_per_s;
    while (started.load(.acquire) == 0) {
        if (nowNs() >= deadline) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }

    var stop = std.atomic.Value(bool).init(false);
    var worst: [executors]std.atomic.Value(u64) = undefined;
    var tickers: [executors]zio.JoinHandle(void) = undefined;
    for (&worst, &tickers, 0..) |*w, *t, i| {
        w.* = .init(0);
        t.* = try rt.spawnInto(.{ .executor = @intCast(i) }, ticker, .{ &stop, w });
    }
    const t0 = nowNs();
    server.requestShutdown();
    serve_handle.join() catch {};
    const drain_ms = (nowNs() - t0) / std.time.ns_per_ms;
    stop.store(true, .release);
    var max_ms: u64 = 0;
    for (&tickers, &worst) |*t, *w| {
        t.join();
        max_ms = @max(max_ms, w.load(.monotonic) / std.time.ns_per_ms);
    }
    std.debug.print("drain: serve took {d} ms; longest ticker gap on any executor {d} ms (limit {d})\n", .{ drain_ms, max_ms, max_gap_ms });
    // The drain must really have lasted (PING guard + drain timeout), or the
    // gap test measured nothing.
    try std.testing.expect(drain_ms >= 1000);
    try std.testing.expect(max_ms <= max_gap_ms);
}

test "drain: a draining connection does not hold its executor" {
    if (!pinned) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(executors),
    });
    defer rt.deinit();
    var h = try rt.spawnInto(.{ .executor = 0 }, runDrain, .{ rt, gpa });
    try h.join();
}
