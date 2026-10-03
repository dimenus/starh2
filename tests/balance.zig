//! Gates for load-aware connection placement (src/edge/balancer.zig).
//!
//! Each test states a behaviour the balancer must keep. They began as the
//! reproductions of the defects the t-2502 reviews found (all six failed on
//! 25b7192's experiment); they run in `test-placement`, which `ci` runs.
//! "control: serve waits for connections when balancing is off" proves the
//! shutdown instrument can tell the two states apart.
//!
//! Needs `-Dzio-scheduling=pinned`: a Balancer refuses work stealing, and
//! every test skips there.
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

const Balancer = starh2.Balancer;

/// A balancer with a chosen width, for the tests that exercise the pick
/// itself or a width the runtime does not have. `Balancer.init` always takes
/// the width from the runtime.
fn widthBalancer(gpa: std.mem.Allocator, n: usize) !Balancer {
    const conns = try gpa.alloc(std.atomic.Value(u32), n);
    const handlers = try gpa.alloc(std.atomic.Value(u32), n);
    for (conns) |*c| c.* = .init(0);
    for (handlers) |*h| h.* = .init(0);
    return .{ .conns = conns, .handlers = handlers };
}

fn sum(xs: []std.atomic.Value(u32)) u32 {
    var t: u32 = 0;
    for (xs) |*x| t += x.load(.acquire);
    return t;
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
/// connection counts of executors below it, so the balanced connection lands
/// on executor `target`; every placement runs.
fn runShutdownWaits(rt: *zio.Runtime, gpa: std.mem.Allocator, balance: bool, target: u8) !usize {
    var bal = try Balancer.init(gpa, rt);
    defer bal.deinit(gpa);

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
        .balancer = if (balance) &bal else null,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);

    for (bal.conns[0..target]) |*c| _ = c.fetchAdd(1, .acq_rel);
    var client = try openH2cSse(gpa, server.localAddress(0).getPort());
    defer client.close();
    try waitStreams(&server, 1, 5000);
    for (bal.conns[0..target]) |*c| _ = c.fetchSub(1, .acq_rel);

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
    const gpa = std.testing.allocator;
    var bal = try widthBalancer(gpa, 24);
    defer bal.deinit(gpa);
    // Executors 0..15 each carry one connection; 16..23 are empty.
    for (bal.conns[0..16]) |*c| c.store(1, .release);
    const pick = bal.reserve().?;
    std.debug.print("width: 24 executors, 0..15 busy, pick={d}\n", .{pick});
    bal.releaseConn(pick);
    for (bal.conns[0..16]) |*c| c.store(0, .release);
    try std.testing.expect(pick >= 16);
}

test "balance: the width is the runtime's" {
    if (!pinned) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try testRuntime(gpa, 3);
    defer rt.deinit();
    var bal = try Balancer.init(gpa, rt);
    defer bal.deinit(gpa);
    try std.testing.expectEqual(rt.executors.items.len, bal.conns.len);
    try std.testing.expectEqual(rt.executors.items.len, bal.handlers.len);
}

// ---------------------------------------------------------------------------
// Count: a configured width wider than the runtime must not close sockets.
// This is the single_executor build (zio resolves any width to one) and any
// caller that sets conn_balance_executors by hand.
// ---------------------------------------------------------------------------

/// `bal` is two wide on a one-executor runtime, so the second connection's
/// placement is refused by zio (InvalidPlacement): the server must fall back
/// to its normal spawn and serve it.
fn runNarrowRuntime(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var bal = try widthBalancer(gpa, 2);
    defer bal.deinit(gpa);

    var spot: HandlerSpot = .{};
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = &spot, .runFn = hangSse } } },
    };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
        .balancer = &bal,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();

    var c1 = try openH2cSse(gpa, port);
    var c1_open = true;
    defer if (c1_open) c1.close();
    try waitStreams(&server, 1, 5000);
    // Executor 0 now has a connection, so the pick for the next one is
    // executor 1, which this runtime does not have.
    var c2 = try openH2cSse(gpa, port);
    var c2_open = true;
    defer if (c2_open) c2.close();
    const second = waitStreams(&server, 2, 3000);
    std.debug.print("refused placement: runtime executors=1, balancer width=2, streams open={d} (want 2)\n", .{server.accounting.active_streams.load(.acquire)});

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

test "balance: a refused placement falls back and still serves the connection" {
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

/// One idle SSE handler on executor 0, then 100 inline-only connections:
/// returns |c0 - c1| after they are placed under `rank`.
fn pileUpSpread(gpa: std.mem.Allocator, rank: Balancer.Rank) !u32 {
    var bal = try widthBalancer(gpa, 2);
    defer bal.deinit(gpa);
    bal.rank = rank;
    bal.handlers[0].store(1, .release);
    bal.conns[0].store(1, .release);
    for (0..100) |_| _ = bal.reserve().?;
    const c0 = bal.conns[0].load(.acquire);
    const c1 = bal.conns[1].load(.acquire);
    for (bal.conns) |*c| c.store(0, .release);
    bal.handlers[0].store(0, .release);
    std.debug.print("inline pile-up, rank={s}: connections per executor = [{d}, {d}]\n", .{ @tagName(rank), c0, c1 });
    return if (c0 > c1) c0 - c1 else c1 - c0;
}

/// Executor 0 holds one heavy connection (250 live handlers); executor 1
/// holds two light ones (20 handlers). Returns the executor a new connection
/// is placed on under `rank`.
fn heavyPick(gpa: std.mem.Allocator, rank: Balancer.Rank) !zio.ExecutorId {
    var bal = try widthBalancer(gpa, 2);
    defer bal.deinit(gpa);
    bal.rank = rank;
    bal.conns[0].store(1, .release);
    bal.handlers[0].store(250, .release);
    bal.conns[1].store(2, .release);
    bal.handlers[1].store(20, .release);
    const pick = bal.reserve().?;
    for (bal.conns) |*c| c.store(0, .release);
    for (bal.handlers) |*h| h.store(0, .release);
    std.debug.print("heavy executor, rank={s}: new connection placed on executor {d} (0 holds 1 conn / 250 handlers, 1 holds 2 conns / 20 handlers)\n", .{ @tagName(rank), pick });
    return pick;
}

// The two rank gates run for every rank and print each result, so one run
// shows which ranks pass both; each asserts only for the default rank.

test "balance: inline-only connections do not pile behind one idle task handler" {
    if (!pinned) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    for (std.enums.values(Balancer.Rank)) |r| _ = try pileUpSpread(gpa, r);
    const default_rank = (Balancer{ .conns = &.{}, .handlers = &.{} }).rank;
    try std.testing.expect(try pileUpSpread(gpa, default_rank) <= 10);
}

test "balance: an executor with one heavy connection is not picked as the emptiest" {
    if (!pinned) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    for (std.enums.values(Balancer.Rank)) |r| _ = try heavyPick(gpa, r);
    const default_rank = (Balancer{ .conns = &.{}, .handlers = &.{} }).rank;
    try std.testing.expectEqual(@as(zio.ExecutorId, 1), try heavyPick(gpa, default_rank));
}

// ---------------------------------------------------------------------------
// HTTP/1.1: an H1 task handler must count toward its executor's load.
// ---------------------------------------------------------------------------

fn runH1Counted(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var bal = try Balancer.init(gpa, rt);
    defer bal.deinit(gpa);

    var spot: HandlerSpot = .{};
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = &spot, .runFn = hangSse } } },
    };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h1c = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
        .balancer = &bal,
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
    const counted = sum(bal.handlers);
    const conns = sum(bal.conns);
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
    var bal = try Balancer.init(gpa, rt);
    defer bal.deinit(gpa);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = &b.spot, .runFn = hangSse } } },
    };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
        .balancer = &bal,
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
    var bal = try Balancer.init(gpa, rt);
    defer bal.deinit(gpa);

    var spot: HandlerSpot = .{};
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = &spot, .runFn = hangSse } } },
    };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
        .balancer = &bal,
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
