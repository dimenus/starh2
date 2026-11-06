//! `Server.stats()` (t-3759 step 9b), through a real server.
//!
//! The three counters that step added, each moved by its own event:
//! - `stream_bound_refusals`: a request when the server holds
//!   `max_streams_per_server` (HTTP/1.1 answers 503);
//! - `task_bound_refusals`: a task request when the server holds
//!   `max_task_handlers` (503);
//! - `connection_bound_waits`: the server reaching `max_connections` (the
//!   accept loop waits for room before it accepts; a client that connects
//!   meanwhile waits in the kernel's queue and is served once the first
//!   connection ends).
//!
//! And two counters that existed before, read through `stats()`: a
//! client-share refusal (a third connection from 127.0.0.1 under pressure)
//! and a route-share refusal (a third task request on a route over its
//! share). Each test checks the exact delta it caused, and a gauge while the
//! resource is held.
//!
//! HTTP/1.1 over zio streams from the test's own tasks: a held request is a
//! `/hold` whose handler waits for `release`.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const serve_deadline = @import("serve_deadline.zig");
const route_share = @import("route_share.zig");

const accounts = starh2.edge.accounts;
const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

var release: std.atomic.Value(bool) = .init(false);
var started: std.atomic.Value(usize) = .init(0);

fn hold(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = started.fetchAdd(1, .acq_rel);
    while (!release.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    try resp.send(200, &.{}, "held");
}

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/hold", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hold } } },
    .{ .method = .GET, .path = "/hold2", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hold } } },
    .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
};

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

const Rig = struct {
    gpa: std.mem.Allocator,
    server: starh2.Server,
    handle: zio.JoinHandle(starh2.ServeError!void),
    routes: [routes.len]starh2.Route = routes,

    /// `whole_server`: every task route may hold the whole server, so no
    /// route share refuses before a server bound (`tests/route_share.zig`).
    fn start(self: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator, limits: starh2.Limits, identity: starh2.edge.server.ClientIdentity, whole_server: bool) !void {
        release.store(false, .release);
        started.store(0, .release);
        self.* = .{ .gpa = gpa, .server = undefined, .handle = undefined };
        if (whole_server) self.routes = route_share.wholeServer(routes.len, routes, limits);
        self.server = try starh2.Server.init(gpa, rt.io(), .{
            .endpoints = &.{.{ .h1c = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), .client_identity = identity } }},
            .routes = &self.routes,
            .tls = null,
            .limits = limits,
        });
        self.handle = try rt.spawn(starh2.Server.serve, .{ &self.server, gpa });
        try self.server.waitUntilListening(5 * std.time.ns_per_s);
    }

    fn stop(self: *Rig) void {
        release.store(true, .release);
        self.server.requestShutdown();
        serve_deadline.join(&self.server, &self.handle);
        self.server.deinit(self.gpa);
    }

    fn connect(self: *Rig) !zio.net.Stream {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", self.server.localAddress(0).getPort());
        return peer.connect(.{});
    }
};

fn sendGet(stream: zio.net.Stream, path: []const u8) !void {
    var buf: [128]u8 = undefined;
    const req = try std.fmt.bufPrint(&buf, "GET {s} HTTP/1.1\r\nhost: localhost\r\n\r\n", .{path});
    var off: usize = 0;
    while (off < req.len) off += try stream.write(req[off..], .none);
}

/// The status of the next response on `stream`, or null when the server
/// closed it first.
fn readStatus(stream: zio.net.Stream, timeout_ms: u64) !?u16 {
    var buf: [512]u8 = undefined;
    var n: usize = 0;
    const deadline = nowNs() + timeout_ms * ms;
    while (n < 12) {
        if (nowNs() >= deadline) return error.NoResponse;
        const got = stream.read(buf[n..], .{ .duration = .fromMilliseconds(10) }) catch |err| switch (err) {
            error.Timeout => continue,
            error.ConnectionResetByPeer => return null,
            else => return err,
        };
        if (got == 0) return null;
        n += got;
    }
    if (!std.mem.startsWith(u8, buf[0..n], "HTTP/1.1 ")) return error.NotHttp;
    return try std.fmt.parseInt(u16, buf[9..12], 10);
}

fn waitStarted(want: usize) !void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (started.load(.acquire) < want) {
        if (nowNs() >= deadline) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

fn expectDelta(comptime what: []const u8, before: u64, after: u64, want: u64) !void {
    if (after - before != want) {
        std.debug.print("stats: {s}: moved by {d}, want {d}\n", .{ what, after - before, want });
        return error.WrongDelta;
    }
}

// ---------------------------------------------------------------------------
// The three new counters.

fn streamBound(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    limits.max_streams_per_server = 1;
    limits.max_streams_per_connection = 1;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .none, true);
    defer rig.stop();
    const before = rig.server.stats();

    const first = try rig.connect();
    defer first.close();
    try sendGet(first, "/hold");
    try waitStarted(1);
    try std.testing.expectEqual(@as(usize, 1), rig.server.stats().active_streams);

    const second = try rig.connect();
    defer second.close();
    try sendGet(second, "/hello");
    try std.testing.expectEqual(@as(?u16, 503), try readStatus(second, 5000));
    const after = rig.server.stats();
    try expectDelta("stream_bound_refusals", before.stream_bound_refusals, after.stream_bound_refusals, 1);
    try expectDelta("task_bound_refusals", before.task_bound_refusals, after.task_bound_refusals, 0);

    release.store(true, .release);
    try std.testing.expectEqual(@as(?u16, 200), try readStatus(first, 5000));
    std.debug.print("stats: stream bound: 503, stream_bound_refusals {d} -> {d}\n", .{ before.stream_bound_refusals, after.stream_bound_refusals });
}

fn taskBound(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    // Two task routes, each allowed the whole server: the server's bound is
    // reached only by both together. (One route holding everything is
    // refused by its own share first, which is charged before the server.)
    limits.max_task_handlers = 2;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .none, true);
    defer rig.stop();
    const before = rig.server.stats();

    var held: [2]zio.net.Stream = undefined;
    for (&held, [_][]const u8{ "/hold", "/hold2" }, 0..) |*s, path, i| {
        s.* = try rig.connect();
        try sendGet(s.*, path);
        try waitStarted(i + 1);
    }
    defer for (held) |s| s.close();
    try std.testing.expectEqual(@as(usize, 2), rig.server.stats().task_handlers);

    const third = try rig.connect();
    defer third.close();
    try sendGet(third, "/hold");
    try std.testing.expectEqual(@as(?u16, 503), try readStatus(third, 5000));
    const after = rig.server.stats();
    const rr = @backingInt(accounts.RouteResource.task_handlers);
    try expectDelta("task_bound_refusals", before.task_bound_refusals, after.task_bound_refusals, 1);
    try expectDelta("route_share_refusals[task_handlers]", before.route_share_refusals[rr], after.route_share_refusals[rr], 0);
    try expectDelta("stream_bound_refusals", before.stream_bound_refusals, after.stream_bound_refusals, 0);

    release.store(true, .release);
    for (held) |s| try std.testing.expectEqual(@as(?u16, 200), try readStatus(s, 5000));
    std.debug.print("stats: task bound: 503, task_bound_refusals {d} -> {d}\n", .{ before.task_bound_refusals, after.task_bound_refusals });
}

fn connectionBound(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    limits.max_connections = 1;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .none, true);
    defer rig.stop();
    const before = rig.server.stats();

    // The first connection fills the server: the accept loop begins to wait
    // for room (it checks before it accepts), with no client waiting yet.
    const first = try rig.connect();
    try sendGet(first, "/hold");
    try waitStarted(1);
    try waitStat("one accept loop waiting", &rig, 1);
    var now = rig.server.stats();
    try std.testing.expectEqual(@as(usize, 1), now.connections);
    try expectDelta("connection_bound_waits (server full)", before.connection_bound_waits, now.connection_bound_waits, 1);

    // The kernel completes this connect; it waits in the listen queue.
    const second = try rig.connect();
    defer second.close();
    try sendGet(second, "/hello");
    zio.sleep(.fromMilliseconds(100)) catch {};
    now = rig.server.stats();
    try std.testing.expectEqual(@as(usize, 1), now.connections);
    try expectDelta("connection_bound_waits (a client queued, the same wait)", before.connection_bound_waits, now.connection_bound_waits, 1);

    release.store(true, .release);
    try std.testing.expectEqual(@as(?u16, 200), try readStatus(first, 5000));
    first.close();
    // The first connection gone, the queued one is accepted and served, and
    // it fills the server again: a second wait.
    try std.testing.expectEqual(@as(?u16, 200), try readStatus(second, 5000));
    try waitStat("the loop waiting again", &rig, 1);
    const after = rig.server.stats();
    try expectDelta("connection_bound_waits (full again)", before.connection_bound_waits, after.connection_bound_waits, 2);
    std.debug.print("stats: connection bound: the server full twice, the queued connection served; connection_bound_waits {d} -> {d}\n", .{ before.connection_bound_waits, after.connection_bound_waits });
}

/// Until `accept_loops_waiting` is `want`.
fn waitStat(comptime what: []const u8, rig: *Rig, want: usize) !void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (rig.server.stats().accept_loops_waiting != want) {
        if (nowNs() >= deadline) {
            std.debug.print("stats: {s}: accept_loops_waiting {d}, want {d}\n", .{ what, rig.server.stats().accept_loops_waiting, want });
            return error.StateNotReached;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

// ---------------------------------------------------------------------------
// Counters that existed before, through `stats()`.

fn clientShare(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    // Mark 2, a client's share 1: 127.0.0.1 holds two, its third is refused.
    limits.max_connections = 8;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .peer, true);
    defer rig.stop();
    const res = @backingInt(accounts.Resource.connections);
    const before = rig.server.stats();

    var held: [2]zio.net.Stream = undefined;
    for (&held, 0..) |*s, i| {
        s.* = try rig.connect();
        try sendGet(s.*, "/hold");
        try waitStarted(i + 1);
    }
    defer for (held) |s| s.close();
    try std.testing.expectEqual(@as(usize, 2), rig.server.stats().connections);
    const third = try rig.connect();
    defer third.close();
    try sendGet(third, "/hello");
    // Refused at accept: closed unanswered.
    try std.testing.expectEqual(@as(?u16, null), try readStatus(third, 5000));
    const after = rig.server.stats();
    try expectDelta("client_share_refusals[connections]", before.client_share_refusals[res], after.client_share_refusals[res], 1);
    release.store(true, .release);
    for (held) |s| try std.testing.expectEqual(@as(?u16, 200), try readStatus(s, 5000));
    std.debug.print("stats: client share: third connection closed, client_share_refusals[connections] {d} -> {d}\n", .{ before.client_share_refusals[res], after.client_share_refusals[res] });
}

fn routeShare(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    // Mark 1; two task routes, so each route's share is 2.
    limits.max_task_handlers = 4;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .none, false);
    defer rig.stop();
    const res = @backingInt(accounts.RouteResource.task_handlers);
    const before = rig.server.stats();

    var held: [2]zio.net.Stream = undefined;
    for (&held, 0..) |*s, i| {
        s.* = try rig.connect();
        try sendGet(s.*, "/hold");
        try waitStarted(i + 1);
    }
    defer for (held) |s| s.close();
    const third = try rig.connect();
    defer third.close();
    try sendGet(third, "/hold");
    try std.testing.expectEqual(@as(?u16, 503), try readStatus(third, 5000));
    const after = rig.server.stats();
    try expectDelta("route_share_refusals[task_handlers]", before.route_share_refusals[res], after.route_share_refusals[res], 1);
    try expectDelta("task_bound_refusals", before.task_bound_refusals, after.task_bound_refusals, 0);
    release.store(true, .release);
    for (held) |s| try std.testing.expectEqual(@as(?u16, 200), try readStatus(s, 5000));
    std.debug.print("stats: route share: third /hold 503, route_share_refusals[task_handlers] {d} -> {d}\n", .{ before.route_share_refusals[res], after.route_share_refusals[res] });
}

fn runIn(comptime f: anytype) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(f, .{ rt, gpa });
    try h.join();
}

test "stats: a request past the server's stream bound counts stream_bound_refusals (t-3759 step 9b)" {
    try runIn(streamBound);
}

test "stats: a task request past the server's task-handler bound counts task_bound_refusals (t-3759 step 9b)" {
    try runIn(taskBound);
}

test "stats: each time the server reaches max_connections counts connection_bound_waits, and a queued connection is served later (t-3759 step 9b)" {
    try runIn(connectionBound);
}

test "stats: a client-share refusal shows in client_share_refusals (t-3759 step 9b)" {
    try runIn(clientShare);
}

test "stats: a route-share refusal shows in route_share_refusals (t-3759 step 9b)" {
    try runIn(routeShare);
}
