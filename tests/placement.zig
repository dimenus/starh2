//! Spawn placement gates.
//!
//! Without task migration (`-Dzio-scheduling=pinned`), every task a connection
//! spawns for itself must run on the connection's own executor. The check is
//! `connection.notePlacement`: each per-connection task compares its thread
//! with the thread its actor recorded, and counts both the comparisons and the
//! mismatches. A mismatch count of zero means nothing unless the comparison
//! count is not zero, so every test asserts both.
//!
//! Under work_stealing the same scenarios run, but the placement is `.auto` by
//! design and the check is compiled out, so those builds assert that instead.
const std = @import("std");
const zio = @import("zio");
const serve_deadline = @import("serve_deadline.zig");
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

fn hangSse(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    while (true) {
        try zio.sleep(.fromMilliseconds(50));
        if (body.terminalCause() != null) return error.Canceled;
    }
}

fn runH2cSsePlacement(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const sse_streams = 4;
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangSse } } },
    };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_deadline.join(&server, &serve_handle);
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);

    conn_mod.test_placement_checks.store(0, .release);
    conn_mod.test_placement_mismatches.store(0, .release);

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());
    var stream = try peer.connect(.{});
    defer stream.close();
    var wire = try h2c.buildClientPrefaceAndSettings(gpa);
    defer wire.deinit(gpa);
    var i: usize = 0;
    while (i < sse_streams) : (i += 1) {
        try h2c.appendHeaders(gpa, &wire, @intCast(1 + 2 * i), "/sse", true);
    }
    try writeAll(stream, wire.items);

    const deadline = nowNs() +% 5 * std.time.ns_per_s;
    while (server.accounting.active_streams.load(.acquire) < sse_streams) {
        if (nowNs() >= deadline) return error.SseStreamsNotOpen;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }

    const checks = conn_mod.test_placement_checks.load(.acquire);
    const mismatches = conn_mod.test_placement_mismatches.load(.acquire);
    std.debug.print("h2c placement: scheduling={s} placement={s} checks={d} mismatches={d}\n", .{
        @tagName(conn_mod.zio_scheduling),
        @tagName(conn_mod.connPlacement()),
        checks,
        mismatches,
    });
    if (conn_mod.placement_check) {
        // The two h2c pumps plus one task per SSE stream.
        try std.testing.expect(checks >= 2 + sse_streams);
        try std.testing.expectEqual(@as(usize, 0), mismatches);
    } else {
        try std.testing.expectEqual(@as(usize, 0), checks);
    }
}

fn spawnLocalProbe() void {}

fn assertWorkStealingRefusesLocal() !void {
    if (conn_mod.zio_scheduling != .work_stealing) return;
    try std.testing.expectEqual(zio.Placement.auto, conn_mod.conn_placement);
    try std.testing.expect(!conn_mod.placement_check);
    try std.testing.expectError(error.InvalidPlacement, zio.spawnInto(.local, spawnLocalProbe, .{}));
}

test "placement: an h2c connection's pumps and SSE handlers run on the actor's executor" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
    defer rt.deinit();
    var handle = try rt.spawn(runH2cSsePlacement, .{ rt, gpa });
    try handle.join();
    var probe = try rt.spawn(assertWorkStealingRefusesLocal, .{});
    try probe.join();
}

// ---------------------------------------------------------------------------
// HTTP/1.1 handler jobs and the connection's end, across executors.
//
// An HTTP/1.1 task handler spawns with `connPlacement()`. When the client
// goes away, the connection asks the handler to cancel (a request, never a
// wait) and ends; the handler, which may run on another executor, sees the
// cancel there and frees its link last.
// ---------------------------------------------------------------------------

var h1_handler_thread: std.atomic.Value(std.Thread.Id) = .init(0);
var h1_handler_started: std.atomic.Value(bool) = .init(false);
var h1_handler_saw_cancel: std.atomic.Value(bool) = .init(false);
var h1_handler_ended: std.atomic.Value(bool) = .init(false);

/// Ignores the terminal cause on purpose, so only the cancel the
/// connection's end requests can stop it.
fn parkUntilCanceled(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    defer h1_handler_ended.store(true, .release);
    h1_handler_thread.store(std.Thread.getCurrentId(), .release);
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    h1_handler_started.store(true, .release);
    while (true) {
        zio.sleep(.fromSeconds(1)) catch |err| {
            if (err == error.Canceled) h1_handler_saw_cancel.store(true, .release);
            return err;
        };
    }
}

fn quickTask(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try resp.send(200, &.{}, "quick");
}

fn readUntilSuffix(stream: zio.net.Stream, buf: []u8, suffix: []const u8) !void {
    var n: usize = 0;
    while (!std.mem.endsWith(u8, buf[0..n], suffix)) {
        if (n == buf.len) return error.ResponseTooLong;
        const got = try stream.read(buf[n..], .none);
        if (got == 0) return error.ConnectionClosed;
        n += got;
    }
}

fn runH1HandlerPlacement(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const io = rt.io();
    const conn_at: zio.Placement = if (pinned) .{ .executor = 0 } else .auto;

    var slabs = try starh2.edge.slab_pool.SlabPool.init(gpa, io, starh2.Limits.defaults.outbound_bytes_per_stream, 4);
    defer slabs.deinit(gpa);
    var requests = try starh2.edge.request_pool.RequestPool.init(starh2.Limits.defaults.requestPoolConfig());
    defer requests.deinit();

    const bind = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try bind.listen(io, .{ .reuse_address = true });
    const port = listener.socket.address.getPort();
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var client = try peer.connect(.{});
    var client_open = true;
    defer if (client_open) client.close();
    const accepted = listener.accept(io) catch |err| {
        listener.socket.close(io);
        return err;
    };
    listener.socket.close(io);

    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/park", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = parkUntilCanceled } } },
        .{ .method = .GET, .path = "/quick", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = quickTask } } },
    };
    const config: conn_mod.ConnConfig = .{
        .io = io,
        .mode = .h1c,
        .limits = starh2.Limits.defaults,
        .router = .{ .routes = &routes },
        .gpa = gpa,
        .slab_pool = &slabs,
        .request_pool = &requests,
    };

    h1_handler_thread.store(0, .release);
    h1_handler_started.store(false, .release);
    h1_handler_saw_cancel.store(false, .release);
    h1_handler_ended.store(false, .release);
    conn_mod.test_placement_checks.store(0, .release);
    conn_mod.test_placement_mismatches.store(0, .release);

    var serve = try rt.spawnInto(conn_at, starh2.edge.h1.serve, .{ accepted, config, null, &.{} });
    // Two handler spawns on one connection. An `.auto` placement homes them
    // round-robin, so over two executors one of them must land away from the
    // connection; with only one spawn, `.auto` can match by luck.
    var quick_buf: [512]u8 = undefined;
    try writeAll(client, "GET /quick HTTP/1.1\r\nHost: t\r\n\r\n");
    try readUntilSuffix(client, &quick_buf, "quick");
    try writeAll(client, "GET /park HTTP/1.1\r\nHost: t\r\n\r\n");

    var deadline = nowNs() +% 5 * std.time.ns_per_s;
    while (!h1_handler_started.load(.acquire)) {
        if (nowNs() >= deadline) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    client.close();
    client_open = false;

    deadline = nowNs() +% 5 * std.time.ns_per_s;
    while (!serve.hasResult()) {
        if (nowNs() >= deadline) {
            serve.cancel();
            return error.ConnectionNeverFinished;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    try serve.join();
    // The handler's link lives in the request pool: it must be back before
    // the pool goes.
    deadline = nowNs() +% 5 * std.time.ns_per_s;
    while (!h1_handler_ended.load(.acquire) or requests.held_bytes.load(.acquire) != 0) {
        if (nowNs() >= deadline) return error.HandlerNeverEnded;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }

    std.debug.print("h1 placement: scheduling={s} saw_cancel={} handler_thread={d} checks={d} mismatches={d}\n", .{
        @tagName(conn_mod.zio_scheduling),
        h1_handler_saw_cancel.load(.acquire),
        h1_handler_thread.load(.acquire),
        conn_mod.test_placement_checks.load(.acquire),
        conn_mod.test_placement_mismatches.load(.acquire),
    });
    try std.testing.expect(h1_handler_saw_cancel.load(.acquire));
    if (conn_mod.placement_check) {
        try std.testing.expectEqual(@as(usize, 2), conn_mod.test_placement_checks.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), conn_mod.test_placement_mismatches.load(.acquire));
    }
}

test "placement: HTTP/1.1 handler jobs spawn on the connection's placement, and the connection's end cancels them" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
    defer rt.deinit();
    const main_at: zio.Placement = if (pinned) .{ .executor = 0 } else .auto;
    var handle = try rt.spawnInto(main_at, runH1HandlerPlacement, .{ rt, gpa });
    try handle.join();
}
