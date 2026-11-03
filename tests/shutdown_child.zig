//! The shutdown cases that cannot run inside the test process: each ends in a
//! state a test binary cannot leave behind. `build.zig` runs this with one
//! argument and checks what it prints on stderr.
//!
//! - `stuck`: a handler that never ends. `serve` must keep waiting and keep
//!   reporting; after the cancel report, the grace report and two interval
//!   reports this process exits on its own with `serve` still running (the
//!   test's "kill"), printing whether `serve` had returned.
//! - `deinit-live`: `deinit` on a server that still counts a task handler
//!   must report it and free nothing.
//! - `deinit-idle`: `deinit` on a server whose `serve` still runs with
//!   nothing charged (no connection yet) must free nothing either: the
//!   server then still accepts and serves a request.
//! - `underflow`: a release of a counter at zero is refused, counted and
//!   logged, and the counter stays at zero.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");

const dummy: u8 = 0;
var started: std.atomic.Value(bool) = .init(false);

/// Ignores cancel: each canceled sleep is swallowed and the next one waits
/// in full, so it never ends and never spins.
fn stuck(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    _ = &body;
    started.store(true, .release);
    while (true) zio.sleep(.fromMilliseconds(20)) catch {};
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/stuck", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = stuck } } },
};

const drain_ms = 200;
const grace_ms = 300;
const interval_ms = 300;

fn clientMain(port: u16, release: *std.atomic.Value(bool)) void {
    const gpa = std.heap.page_allocator;
    const w = wire_client.Wire.open(gpa, .h2c, port) catch return;
    defer w.close();
    var req = h2c.buildClientPrefaceAndSettings(gpa) catch return;
    defer req.deinit(gpa);
    h2c.appendHeaders(gpa, &req, 1, "/stuck", true) catch return;
    w.send(req.items) catch return;
    while (!release.load(.acquire)) {
        const t: std.c.timespec = .{ .sec = 0, .nsec = 5 * std.time.ns_per_ms };
        _ = std.c.nanosleep(&t, null);
    }
}

fn runStuck(rt: *zio.Runtime) anyerror!void {
    // The page allocator: this process exits with the server live, on purpose.
    const gpa = std.heap.page_allocator;
    var limits = starh2.Limits.defaults;
    limits.graceful_drain_timeout_ns = drain_ms * std.time.ns_per_ms;
    limits.shutdown_cancel_grace_ns = grace_ms * std.time.ns_per_ms;
    limits.shutdown_report_interval_ns = interval_ms * std.time.ns_per_ms;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    var handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    var release: std.atomic.Value(bool) = .init(false);
    const t = try std.Thread.spawn(.{}, clientMain, .{ server.localAddress(0).getPort(), &release });
    t.detach();
    while (!started.load(.acquire)) try zio.sleep(.fromMilliseconds(5));
    server.requestShutdown();
    const t0 = zio.Timestamp.now(.monotonic).toNanoseconds();
    // The first phase (1 s), the drain, the grace, two intervals, and room.
    const wait_ms: u64 = 1000 + drain_ms + grace_ms + 2 * interval_ms + 250;
    while (server.shutdown_reports.load(.acquire) < 4) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() - t0 > (wait_ms + 2000) * std.time.ns_per_ms) break;
        try zio.sleep(.fromMilliseconds(10));
    }
    const ms = (zio.Timestamp.now(.monotonic).toNanoseconds() - t0) / std.time.ns_per_ms;
    std.debug.print("child: {d} ms after shutdown, serve returned={}, reports={d}, last report counted {d} task handlers\n", .{
        ms, handle.hasResult(), server.shutdown_reports.load(.acquire), server.shutdown_report_handlers.load(.acquire),
    });
    if (!handle.hasResult() and server.shutdown_reports.load(.acquire) >= 4 and server.shutdown_report_handlers.load(.acquire) == 1) {
        std.debug.print("child: serve still waiting for the stuck handler\n", .{});
    }
    // The kill: leave with `serve` and the handler running.
    std.process.exit(0);
}

/// A handler that holds request memory and keeps writing to it.
var held_touches: std.atomic.Value(u64) = .init(0);

fn holder(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    _ = &body;
    // From the request pool, through the request's arena.
    const mem = try req.arena.alloc(u8, 256 * 1024);
    started.store(true, .release);
    var i: u8 = 0;
    while (true) {
        @memset(mem, i);
        i +%= 1;
        _ = held_touches.fetchAdd(1, .monotonic);
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

const holder_routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/stuck", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = holder } } },
};

/// `deinit` called while `serve` runs, with a handler that holds 256 KiB of
/// request-pool memory and keeps writing to it. `deinit` must free nothing:
/// the handler goes on writing, and the process ends cleanly.
fn runDeinitLive(rt: *zio.Runtime) anyerror!void {
    const gpa = std.heap.page_allocator;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &holder_routes,
        .tls = null,
        .limits = .defaults,
    });
    _ = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    var release: std.atomic.Value(bool) = .init(false);
    const t = try std.Thread.spawn(.{}, clientMain, .{ server.localAddress(0).getPort(), &release });
    t.detach();
    while (!started.load(.acquire)) try zio.sleep(.fromMilliseconds(5));
    const handlers = server.accounting.task_handlers.load(.acquire);
    server.deinit(gpa);
    // The handler still writes its request memory after `deinit`.
    const before = held_touches.load(.acquire);
    try zio.sleep(.fromMilliseconds(200));
    const after = held_touches.load(.acquire);
    std.debug.print("child: deinit returned with {d} task handlers live; the handler wrote its request memory {d} times after it\n", .{ handlers, after - before });
    if (after > before) std.debug.print("child: request memory still live after deinit\n", .{});
    std.process.exit(0);
}

/// astra 6: `deinit` while `serve` runs and no counter is charged. The
/// listeners and the accept loops are still in use, so
/// `deinit` must free nothing; the server then serves a request.
fn runDeinitIdle(rt: *zio.Runtime) anyerror!void {
    const gpa = std.heap.page_allocator;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &ok_routes,
        .tls = null,
        .limits = .defaults,
    });
    _ = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();
    server.deinit(gpa);
    std.debug.print("child: deinit returned with serve running and nothing charged\n", .{});
    var ok: std.atomic.Value(bool) = .init(false);
    var done: std.atomic.Value(bool) = .init(false);
    const t = try std.Thread.spawn(.{}, okClient, .{ port, &ok, &done });
    // Poll, never a blocking join here: it would stall this zio executor
    // and every connection placed on it.
    while (!done.load(.acquire)) try zio.sleep(.fromMilliseconds(5));
    t.join();
    if (ok.load(.acquire)) std.debug.print("child: served a request after deinit\n", .{});
    std.process.exit(0);
}

fn okHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const ok_routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/ok", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = okHandler } } },
};

fn okClient(port: u16, ok: *std.atomic.Value(bool), done: *std.atomic.Value(bool)) void {
    defer done.store(true, .release);
    const gpa = std.heap.page_allocator;
    const w = wire_client.Wire.open(gpa, .h2c, port) catch return;
    defer w.close();
    var req = h2c.buildClientPrefaceAndSettings(gpa) catch return;
    defer req.deinit(gpa);
    h2c.appendHeaders(gpa, &req, 1, "/ok", true) catch return;
    w.send(req.items) catch return;
    const deadline = wire_client.nowNs() + 3 * std.time.ns_per_s;
    while (true) {
        const f = (w.next(deadline) catch return) orelse return;
        if (f.sid == 1 and f.flags & 1 != 0) {
            ok.store(true, .release);
            return;
        }
    }
}

fn runUnderflow(_: *zio.Runtime) anyerror!void {
    var a: starh2.edge.connection.GlobalAccounting = .{ .max_streams = 1, .task_handler_capacity = 1, .max_outbound_bytes = 1, .max_handshakes = 1 };
    a.releaseStream();
    a.releaseOutbound(7);
    std.debug.print("child: active_streams={d} outbound_bytes={d} underflows={d}\n", .{ a.active_streams.load(.acquire), a.outbound_bytes.load(.acquire), a.underflows.load(.acquire) });
}

pub fn main(init: std.process.Init.Minimal) !void {
    var it = std.process.Args.Iterator.init(init.args);
    _ = it.next();
    const case = it.next() orelse return error.NoCase;
    const rt = try zio.Runtime.init(std.heap.page_allocator, .{ .executors = .exact(2) });
    var h = if (std.mem.eql(u8, case, "stuck"))
        try rt.spawn(runStuck, .{rt})
    else if (std.mem.eql(u8, case, "deinit-live"))
        try rt.spawn(runDeinitLive, .{rt})
    else if (std.mem.eql(u8, case, "deinit-idle"))
        try rt.spawn(runDeinitIdle, .{rt})
    else if (std.mem.eql(u8, case, "underflow"))
        try rt.spawn(runUnderflow, .{rt})
    else
        return error.UnknownCase;
    try h.join();
}
