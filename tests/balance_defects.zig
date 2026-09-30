//! Gates for the `--conn-balance` experiment (connection.balancePick).
//!
//! Each test states the behaviour the balancer must have before it can be a
//! default. At the time of writing every one of them FAILS: they are the
//! reproductions of the defects the t-2502 reviews found, and
//! captures/placement-followup/balance-defects.txt holds the run. They are a
//! separate step (`test-balance-defects`), not part of `ci`, for that reason;
//! when a fix lands, its test goes green and moves into `test-placement`.
//!
//! The one control, "serve waits for connections when balancing is off",
//! passes, so the shutdown instrument is shown to tell the two states apart.
//!
//! Needs `-Dzio-scheduling=pinned`: balancePick is compiled to `null` under
//! work_stealing, and every test skips there.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const conn_mod = starh2.edge.connection;
const pinned = conn_mod.zio_scheduling == .pinned;

const dummy: u8 = 0;

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

fn writeAll(stream: zio.net.Stream, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        off += try stream.write(bytes[off..], .none);
    }
}

fn resetBalance() void {
    conn_mod.conn_balance = false;
    conn_mod.conn_balance_executors = 0;
    for (&conn_mod.exec_live_handlers) |*h| h.store(0, .release);
    for (&conn_mod.exec_live_conns) |*c| c.store(0, .release);
}

/// Records the thread each handler first ran on, per route owner.
const HandlerSpot = struct {
    thread: std.atomic.Value(std.Thread.Id) = .init(0),
    started: std.atomic.Value(u32) = .init(0),
};

fn hangSse(ptr: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const spot: *HandlerSpot = @ptrCast(@alignCast(ptr));
    spot.thread.store(std.Thread.getCurrentId(), .release);
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    _ = spot.started.fetchAdd(1, .acq_rel);
    while (true) {
        try zio.sleep(.fromMilliseconds(20));
        if (body.terminalCause() != null) return error.Canceled;
    }
}

/// Outlives its cancellation: after the terminal cause it parks, without
/// holding its executor, until `release` is posted. Used to keep a
/// connection alive past the drain so the shutdown test can see whether
/// `serve` waited for it.
const Stubborn = struct {
    spot: HandlerSpot = .{},
    release: zio.Semaphore = .{ .permits = 0 },
};

fn stubbornSse(ptr: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const st: *Stubborn = @ptrCast(@alignCast(ptr));
    st.spot.thread.store(std.Thread.getCurrentId(), .release);
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    _ = st.spot.started.fetchAdd(1, .acq_rel);
    while (body.terminalCause() == null) {
        zio.sleep(.fromMilliseconds(20)) catch break;
    }
    st.release.waitUncancelable();
    return error.Canceled;
}

fn releaseAfter(sem: *zio.Semaphore, ms: u64) void {
    zio.sleep(.fromMilliseconds(ms)) catch {};
    sem.post();
}

fn openH2cSse(gpa: std.mem.Allocator, port: u16) !zio.net.Stream {
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    errdefer stream.close();
    var wire = try h2c.buildClientPrefaceAndSettings(gpa);
    defer wire.deinit(gpa);
    try h2c.appendHeaders(gpa, &wire, 1, "/sse", true);
    try writeAll(stream, wire.items);
    return stream;
}

fn waitStreams(server: *starh2.Server, want: usize, timeout_ms: u64) !void {
    const deadline = nowNs() +% timeout_ms * std.time.ns_per_ms;
    while (server.accounting.active_streams.load(.acquire) < want) {
        if (nowNs() >= deadline) return error.SseStreamsNotOpen;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

fn testRuntime(gpa: std.mem.Allocator, executors: u8) !*zio.Runtime {
    return zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(executors),
    });
}

// ---------------------------------------------------------------------------
// Shutdown: `serve` must not return while a balanced connection is live.
// ---------------------------------------------------------------------------

/// The handler outlives its cancellation until a releaser posts it
/// `hold_ms` after shutdown starts. That matters because the drain
/// busy-spins the actor (t-2652) and stalls every task on its executor,
/// including the reaper workers `serve` waits for last, so a connection that
/// ends with the drain holds `serve` up by accident. `target` pre-loads the
/// handler counts of executors below it, so the balanced connection lands on
/// executor `target`; every placement runs.
fn runShutdownWaits(rt: *zio.Runtime, gpa: std.mem.Allocator, balance: bool, target: u8) !usize {
    resetBalance();
    defer resetBalance();
    conn_mod.conn_balance = balance;
    conn_mod.conn_balance_executors = shutdown_executors;

    var st: Stubborn = .{};
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = &st, .runFn = stubbornSse } } },
    };
    var limits = starh2.Limits.defaults;
    // The drain after GOAWAY still takes the 1 s PING guard; this keeps the
    // second phase short so the test is quick either way.
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

    for (conn_mod.exec_live_handlers[0..target]) |*h| _ = h.fetchAdd(1, .acq_rel);
    var client = try openH2cSse(gpa, server.localAddress(0).getPort());
    defer client.close();
    try waitStreams(&server, 1, 5000);
    for (conn_mod.exec_live_handlers[0..target]) |*h| _ = h.fetchSub(1, .acq_rel);

    const hold_ms = 2500;
    var releaser = try rt.spawn(releaseAfter, .{ &st.release, hold_ms });
    defer releaser.join();
    const t0 = nowNs();
    server.requestShutdown();
    serve_handle.join() catch {};
    const took_ms = (nowNs() - t0) / std.time.ns_per_ms;
    const live_at_return = server.active_connections.load(.acquire);
    std.debug.print("shutdown: balance={} connection on executor {s}: serve returned after {d} ms with {d} live connection(s)\n", .{ balance, if (!balance) "auto" else &[_]u8{'0' + target}, took_ms, live_at_return });

    // A detached connection still uses `server`; let it finish before
    // deinit runs, so the failure below is the assertion, not a crash.
    const deadline = nowNs() +% 10 * std.time.ns_per_s;
    while (server.active_connections.load(.acquire) != 0 and nowNs() < deadline) {
        zio.sleep(.fromMilliseconds(10)) catch {};
    }
    return live_at_return;
}

const shutdown_executors = 4;

fn runShutdownBoth(rt: *zio.Runtime, gpa: std.mem.Allocator, balance: bool) !void {
    var live: [shutdown_executors]usize = undefined;
    for (&live, 0..) |*l, i| l.* = try runShutdownWaits(rt, gpa, balance, @intCast(i));
    for (live) |l| try std.testing.expectEqual(@as(usize, 0), l);
}

test "control: serve waits for connections when balancing is off" {
    if (!pinned) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try testRuntime(gpa, shutdown_executors);
    defer rt.deinit();
    var h = try rt.spawnInto(.{ .executor = 0 }, runShutdownBoth, .{ rt, gpa, false });
    try h.join();
}

test "balance: serve waits for balanced connections at shutdown" {
    if (!pinned) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try testRuntime(gpa, shutdown_executors);
    defer rt.deinit();
    var h = try rt.spawnInto(.{ .executor = 0 }, runShutdownBoth, .{ rt, gpa, true });
    try h.join();
}

// ---------------------------------------------------------------------------
// Width: every executor must be reachable, whatever the count.
// ---------------------------------------------------------------------------

test "balance: the pick can reach executors above 16" {
    if (!pinned) return error.SkipZigTest;
    resetBalance();
    defer resetBalance();
    conn_mod.conn_balance = true;
    conn_mod.conn_balance_executors = 24;
    // Executors 0..15 each carry one task handler; 16..23 are empty.
    for (conn_mod.exec_live_handlers[0..@min(16, conn_mod.exec_live_handlers.len)]) |*h| h.store(1, .release);
    const pick = conn_mod.balancePick().?;
    std.debug.print("width: 24 executors, 0..15 busy, pick={d}\n", .{pick});
    try std.testing.expect(pick >= 16);
}

// ---------------------------------------------------------------------------
// Count: a configured width wider than the runtime must not close sockets.
// This is the single_executor build (zio resolves any width to one) and any
// caller that sets conn_balance_executors by hand.
// ---------------------------------------------------------------------------

fn runNarrowRuntime(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetBalance();
    defer resetBalance();
    conn_mod.conn_balance = true;
    conn_mod.conn_balance_executors = 2;

    var spot: HandlerSpot = .{};
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = &spot, .runFn = hangSse } } },
    };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();

    var c1 = try openH2cSse(gpa, port);
    var c1_open = true;
    defer if (c1_open) c1.close();
    try waitStreams(&server, 1, 5000);
    // Executor 0 now has a handler, so the pick for the next connection is
    // executor 1, which this runtime does not have.
    var c2 = try openH2cSse(gpa, port);
    var c2_open = true;
    defer if (c2_open) c2.close();
    const second = waitStreams(&server, 2, 3000);
    std.debug.print("narrow runtime: executors=1 configured=2 streams open={d} (want 2)\n", .{server.accounting.active_streams.load(.acquire)});

    c1.close();
    c1_open = false;
    c2.close();
    c2_open = false;
    const deadline = nowNs() +% 5 * std.time.ns_per_s;
    while (server.accounting.active_streams.load(.acquire) != 0 and nowNs() < deadline) {
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    server.requestShutdown();
    serve_handle.join() catch {};
    try second;
}

test "balance: a runtime narrower than the configured width still serves every connection" {
    if (!pinned) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try testRuntime(gpa, 1);
    defer rt.deinit();
    var h = try rt.spawnInto(.{ .executor = 0 }, runNarrowRuntime, .{ rt, gpa });
    try h.join();
}

// ---------------------------------------------------------------------------
// Inline work: one idle task handler must not steer every inline-only
// connection onto the other executor.
// ---------------------------------------------------------------------------

test "balance: inline-only connections do not pile behind one idle task handler" {
    if (!pinned) return error.SkipZigTest;
    resetBalance();
    defer resetBalance();
    conn_mod.conn_balance = true;
    conn_mod.conn_balance_executors = 2;
    // Executor 0: one connection with one mostly-sleeping SSE handler.
    conn_mod.exec_live_handlers[0].store(1, .release);
    conn_mod.exec_live_conns[0].store(1, .release);
    // 100 connections that only ever run complete (inline) handlers arrive.
    for (0..100) |_| {
        const p = conn_mod.balancePick().?;
        _ = conn_mod.exec_live_conns[p].fetchAdd(1, .acq_rel);
    }
    const c0 = conn_mod.exec_live_conns[0].load(.acquire);
    const c1 = conn_mod.exec_live_conns[1].load(.acquire);
    std.debug.print("inline pile-up: connections per executor = [{d}, {d}]\n", .{ c0, c1 });
    const spread = if (c0 > c1) c0 - c1 else c1 - c0;
    try std.testing.expect(spread <= 10);
}

// ---------------------------------------------------------------------------
// HTTP/1.1: an H1 task handler must count toward its executor's load.
// ---------------------------------------------------------------------------

fn runH1Counted(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetBalance();
    defer resetBalance();
    conn_mod.conn_balance = true;
    conn_mod.conn_balance_executors = 2;

    var spot: HandlerSpot = .{};
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = &spot, .runFn = hangSse } } },
    };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h1c = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());
    var client = try peer.connect(.{});
    var client_open = true;
    defer if (client_open) client.close();
    try writeAll(client, "GET /sse HTTP/1.1\r\nHost: t\r\n\r\n");
    const deadline = nowNs() +% 5 * std.time.ns_per_s;
    while (spot.started.load(.acquire) == 0) {
        if (nowNs() >= deadline) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    var counted: u32 = 0;
    for (&conn_mod.exec_live_handlers) |*h| counted += h.load(.acquire);
    var conns: u32 = 0;
    for (&conn_mod.exec_live_conns) |*c| conns += c.load(.acquire);
    std.debug.print("h1: live H1 task handlers=1, counted by the balancer={d}, balanced connections={d}\n", .{ counted, conns });

    client.close();
    client_open = false;
    server.requestShutdown();
    serve_handle.join() catch {};
    // The balanced connection is detached from serve (see the shutdown test);
    // without this wait, deinit's active_connections assert fires. That
    // crash was observed before this wait was added.
    const drained = nowNs() +% 5 * std.time.ns_per_s;
    while (server.active_connections.load(.acquire) != 0 and nowNs() < drained) {
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    try std.testing.expectEqual(@as(u32, 1), conns);
    try std.testing.expectEqual(@as(u32, 1), counted);
}

test "balance: an HTTP/1.1 task handler counts toward its executor's load" {
    if (!pinned) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try testRuntime(gpa, 2);
    defer rt.deinit();
    var h = try rt.spawnInto(.{ .executor = 0 }, runH1Counted, .{ rt, gpa });
    try h.join();
}

// ---------------------------------------------------------------------------
// Scope: load on one runtime must not steer placement on another. The
// counters are file-level globals indexed by executor id, and executor 0 of
// one runtime is not executor 0 of another.
// ---------------------------------------------------------------------------

const ServerB = struct {
    spot: HandlerSpot = .{},
    runtime_thread: std.Thread.Id = 0,
    err: ?anyerror = null,
};

fn serveB(rt: *zio.Runtime, gpa: std.mem.Allocator, b: *ServerB) !void {
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = &b.spot, .runFn = hangSse } } },
    };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    var client = try openH2cSse(gpa, server.localAddress(0).getPort());
    var client_open = true;
    defer if (client_open) client.close();
    try waitStreams(&server, 1, 5000);
    client.close();
    client_open = false;
    const deadline = nowNs() +% 5 * std.time.ns_per_s;
    while (server.accounting.active_streams.load(.acquire) != 0 and nowNs() < deadline) {
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    server.requestShutdown();
    serve_handle.join() catch {};
}

/// Runs a second runtime on its own OS thread. That thread drives the
/// runtime's executor 0 while it waits in join.
fn runtimeBThread(gpa: std.mem.Allocator, b: *ServerB) void {
    b.runtime_thread = std.Thread.getCurrentId();
    const rt = testRuntime(gpa, 2) catch |err| {
        b.err = err;
        return;
    };
    defer rt.deinit();
    var h = rt.spawnInto(.{ .executor = 0 }, serveB, .{ rt, gpa, b }) catch |err| {
        b.err = err;
        return;
    };
    h.join() catch |err| {
        b.err = err;
    };
}

fn runTwoRuntimes(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetBalance();
    defer resetBalance();
    conn_mod.conn_balance = true;
    conn_mod.conn_balance_executors = 2;

    var spot: HandlerSpot = .{};
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = &spot, .runFn = hangSse } } },
    };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    var client = try openH2cSse(gpa, server.localAddress(0).getPort());
    var client_open = true;
    defer if (client_open) client.close();
    // Runtime A: one live handler on executor 0.
    try waitStreams(&server, 1, 5000);

    var b: ServerB = .{};
    const thread = try std.Thread.spawn(.{}, runtimeBThread, .{ gpa, &b });
    thread.join();

    client.close();
    client_open = false;
    const deadline = nowNs() +% 5 * std.time.ns_per_s;
    while (server.accounting.active_streams.load(.acquire) != 0 and nowNs() < deadline) {
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    server.requestShutdown();
    serve_handle.join() catch {};

    if (b.err) |err| return err;
    const on_exec0 = b.spot.thread.load(.acquire) == b.runtime_thread;
    std.debug.print("two runtimes: runtime B is empty; its first connection ran on its executor {s}\n", .{if (on_exec0) "0" else "1 (steered by runtime A's load)"});
    // Runtime B has no load, so the stated tie-break (lowest index) is
    // executor 0.
    try std.testing.expect(on_exec0);
}

test "balance: load on one runtime does not steer placement on another" {
    if (!pinned) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try testRuntime(gpa, 2);
    defer rt.deinit();
    var h = try rt.spawnInto(.{ .executor = 0 }, runTwoRuntimes, .{ rt, gpa });
    try h.join();
}
