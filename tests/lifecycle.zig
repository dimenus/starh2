//! Lifecycle stress under DebugAllocator — must fail on leaks / nonzero counters.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const dummy: u8 = 0;

fn hello(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    try resp.send(200, &.{}, "ok");
}

fn hangSse(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    // Stay open until cancelled / connection teardown — no further writes so a
    // single-connection 100-stream stress does not require a client drain task.
    while (true) {
        zio.sleep(.fromMilliseconds(50)) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            return err;
        };
        if (body.terminalCause() != null) return error.Canceled;
    }
}

fn delayedHello(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    const t0 = zio.Timestamp.now(.monotonic).toNanoseconds();
    try resp.send(200, &.{}, "ok");
    const elapsed = zio.Timestamp.now(.monotonic).toNanoseconds() -% t0;
    // Writer delay is 40ms — send must not return before flush completes.
    if (elapsed < 30 * std.time.ns_per_ms) return error.ReturnedBeforeFlush;
}

fn completeHello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

/// Carries its call site, so a failure names WHICH of the scenarios broke.
/// A bare error here says only that some write failed, and an unnamed
/// nondeterministic failure is what gets written off instead of root-caused.
fn writeAllStreamAt(stream: zio.net.Stream, bytes: []const u8, src: std.builtin.SourceLocation) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = stream.write(bytes[off..], .none) catch |err| {
            std.debug.print("write failed at {s}:{d}: {s}\n", .{ src.file, src.line, @errorName(err) });
            return err;
        };
        off += n;
    }
}

fn readExact(stream: zio.net.Stream, out: []u8) !void {
    var off: usize = 0;
    while (off < out.len) {
        const n = try stream.read(out[off..], .none);
        if (n == 0) return error.ConnectionClosed;
        off += n;
    }
}

fn buildMultiSseOpen(gpa: std.mem.Allocator, n: usize) ![]u8 {
    var wire = try h2c.buildClientPrefaceAndSettings(gpa);
    errdefer wire.deinit(gpa);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const sid: u31 = @intCast(1 + 2 * i);
        try h2c.appendHeaders(gpa, &wire, sid, "/sse", true);
    }
    return try wire.toOwnedSlice(gpa);
}

fn buildRstAll(gpa: std.mem.Allocator, n: usize) ![]u8 {
    const frame = starh2.core.frame;
    var wire: std.ArrayList(u8) = .empty;
    errdefer wire.deinit(gpa);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const sid: u31 = @intCast(1 + 2 * i);
        var buf: [13]u8 = undefined;
        const rn = try frame.Serializer.rstStream(&buf, sid, .cancel);
        try wire.appendSlice(gpa, buf[0..rn]);
    }
    return try wire.toOwnedSlice(gpa);
}

fn waitAccountingZero(server: *starh2.Server, timeout_ms: u64) !void {
    return waitAccountingZeroAt(server, timeout_ms, 0);
}

fn waitAccountingZeroAt(server: *starh2.Server, timeout_ms: u64, caller_line: usize) !void {
    const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% timeout_ms * std.time.ns_per_ms;
    while (true) {
        const streams = server.accounting.active_streams.load(.acquire);
        const reaper = server.accounting.reaper_reserved.load(.acquire);
        const outbound = server.accounting.outbound_bytes.load(.acquire);
        const request = server.accounting.request_bytes.load(.acquire);
        const live = starh2.edge.connection.test_observed_live_handlers.load(.acquire);
        const slots = starh2.edge.connection.test_observed_slots_in_use.load(.acquire);
        if (streams == 0 and reaper == 0 and outbound == 0 and request == 0 and live == 0 and slots == 0) return;
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) {
            std.debug.print("accounting not zero (called from line {d}): streams={d} reaper={d} outbound={d} request={d} live={d} slots={d}\n", .{ caller_line, streams, reaper, outbound, request, live, slots });
            return error.AccountingNotZero;
        }
        zio.sleep(.fromMilliseconds(10)) catch {};
    }
}

fn waitStreamsAtLeast(server: *starh2.Server, want: usize, timeout_ms: u64) !void {
    const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% timeout_ms * std.time.ns_per_ms;
    while (true) {
        const streams = server.accounting.active_streams.load(.acquire);
        const live = starh2.edge.connection.test_observed_live_handlers.load(.acquire);
        if (streams >= want and live >= want) return;
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) {
            std.debug.print("streams short: streams={d} live={d} want={d}\n", .{ streams, live, want });
            return error.StreamsShort;
        }
        zio.sleep(.fromMilliseconds(10)) catch {};
    }
}

fn runLifecycleStress(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/hello", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangSse } } },
        .{ .method = .GET, .path = "/delayed", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = delayedHello } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 64;
    limits.max_streams_per_connection = 128;
    limits.max_streams_per_server = 256;
    limits.cancellation_reaper_jobs = 256;
    limits.cancellation_reaper_tasks = 4;
    limits.graceful_drain_timeout_ns = 200 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;
    limits.inbound_wire_chunks_per_connection = 512;
    limits.outbound_bytes_per_stream = 64 * 1024;
    limits.outbound_bytes_per_connection = 8 * 1024 * 1024;

    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);

    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }

    // `waitUntilListening`, never a sleep: `.listening` publishes only after
    // every endpoint is bound AND its accept loop is spawned, and it reports
    // BindFailed instead of timing out. A sleep here is both slower than it
    // needs to be and wrong under load — and `localAddress` reads uninitialized
    // storage until the bind lands.
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();

    // --- Real-path forced spawn failure: slot/live/reaper must return to zero. ---
    starh2.edge.connection.test_force_spawn_fail = true;
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const wire = try h2c.buildClientHello(gpa, "/hello");
        defer gpa.free(wire);
        try writeAllStreamAt(stream, wire, @src());
        try waitAccountingZeroAt(&server, 2000, 161);
    }
    starh2.edge.connection.test_force_spawn_fail = false;

    // --- Completion-vs-RST: hold completion drain while RST arrives, then release. ---
    starh2.edge.connection.test_hold_completion_drain.store(true, .release);
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const open = try h2c.buildClientHello(gpa, "/hello");
        defer gpa.free(open);
        try writeAllStreamAt(stream, open, @src());
        // Wait until a slot is observed in use (handler admitted).
        const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% 2 * std.time.ns_per_s;
        while (starh2.edge.connection.test_observed_slots_in_use.load(.acquire) == 0) {
            if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) return error.SlotNeverUsed;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        // Peer RST while completion drain is held.
        var rst: [13]u8 = undefined;
        const rn = try starh2.core.frame.Serializer.rstStream(&rst, 1, .cancel);
        try writeAllStreamAt(stream, rst[0..rn], @src());
        zio.sleep(.fromMilliseconds(50)) catch {};
        // Slot may still be in_use while completion is held.
        starh2.edge.connection.test_hold_completion_drain.store(false, .release);
        // The completion's actor wake was consumed WHILE the drain was held (the
        // held drainCompletions is a no-op), and releasing the hold re-enables
        // the drain without re-waking the idle actor — the t-544 flake: the
        // slot then sits until an incidental timer wake or the gate's timeout,
        // whichever comes first. Production always pairs post+wake, so the
        // faithful fix is to wake the actor the way a real peer would: any
        // inbound frame. PING is the cheapest.
        var ping_buf: [17]u8 = undefined;
        const ping_n = try starh2.core.frame.Serializer.ping(&ping_buf, false, &[_]u8{0} ** 8);
        try writeAllStreamAt(stream, ping_buf[0..ping_n], @src());
        try waitAccountingZeroAt(&server, 2000, 187);
    }

    // --- Single-connection 100 concurrent SSE: open → RST. ---
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const open = try buildMultiSseOpen(gpa, 100);
        defer gpa.free(open);
        try writeAllStreamAt(stream, open, @src());
        try waitStreamsAtLeast(&server, 100, 5000);
        try std.testing.expectEqual(@as(usize, 100), server.accounting.active_streams.load(.acquire));
        try std.testing.expect(starh2.edge.connection.test_observed_live_handlers.load(.acquire) >= 100);
        try std.testing.expect(starh2.edge.connection.test_observed_slots_in_use.load(.acquire) >= 100);

        const rsts = try buildRstAll(gpa, 100);
        defer gpa.free(rsts);
        try writeAllStreamAt(stream, rsts, @src());
        try waitAccountingZeroAt(&server, 5000, 206);
    }

    // --- Single-connection 100 concurrent SSE → abrupt socket close. ---
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        const open = try buildMultiSseOpen(gpa, 100);
        defer gpa.free(open);
        try writeAllStreamAt(stream, open, @src());
        try waitStreamsAtLeast(&server, 100, 5000);
        stream.close();
        try waitAccountingZeroAt(&server, 5000, 218);
    }

    // --- Delayed writer: flush wait must not return early. ---
    starh2.edge.connection.test_write_delay_ms = 40;
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const wire = try h2c.buildClientHello(gpa, "/delayed");
        defer gpa.free(wire);
        try writeAllStreamAt(stream, wire, @src());
        zio.sleep(.fromMilliseconds(200)) catch {};
    }
    starh2.edge.connection.test_write_delay_ms = 0;

    // --- 100 concurrent SSE then server shutdown (DebugAllocator process coverage). ---
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const open = try buildMultiSseOpen(gpa, 100);
        defer gpa.free(open);
        try writeAllStreamAt(stream, open, @src());
        try waitStreamsAtLeast(&server, 100, 5000);
    }

    server.requestShutdown();
    serve_handle.join() catch {};

    try std.testing.expectEqual(@as(usize, 0), server.active_connections.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.active_streams.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.reaper_reserved.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.outbound_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.request_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.active_handshakes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_observed_live_handlers.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_observed_slots_in_use.load(.acquire));
}

fn runCompleteReceiptPipeline(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/complete", .handler = .{ .complete = .{
            .ptr = @constCast(&dummy),
            .runFn = completeHello,
        } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 16;
    limits.max_streams_per_server = 32;
    limits.cancellation_reaper_jobs = 32;
    limits.cancellation_reaper_tasks = 2;

    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
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
    const port = server.localAddress(0).getPort();

    const c = starh2.edge.connection;
    const io = rt.io();
    c.test_release_complete_receipt_ack.reset();
    c.test_complete_receipt_ack_held.store(false, .release);
    c.test_hold_complete_receipt_ack.store(true, .release);
    defer {
        c.test_hold_complete_receipt_ack.store(false, .release);
        c.test_release_complete_receipt_ack.set(io);
        c.test_complete_receipt_ack_held.store(false, .release);
    }

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    var stream_open = true;
    defer if (stream_open) stream.close();

    var first = try h2c.buildClientPrefaceAndSettings(gpa);
    defer first.deinit(gpa);
    try h2c.appendHeaders(gpa, &first, 1, "/complete", true);
    try writeAllStreamAt(stream, first.items, @src());

    var deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% 2 * std.time.ns_per_s;
    while (c.test_observed_live_handlers.load(.acquire) < 1) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) {
            return error.FirstCompleteNotDispatched;
        }
        zio.sleep(.fromMilliseconds(1)) catch {};
    }
    while (!c.test_complete_receipt_ack_held.load(.acquire)) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) {
            return error.CompleteReceiptAckNotHeld;
        }
        zio.sleep(.fromMilliseconds(1)) catch {};
    }

    var second: std.ArrayList(u8) = .empty;
    defer second.deinit(gpa);
    try h2c.appendHeaders(gpa, &second, 3, "/complete", true);
    try writeAllStreamAt(stream, second.items, @src());

    deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% 250 * std.time.ns_per_ms;
    while (c.test_observed_live_handlers.load(.acquire) < 2) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) {
            std.debug.print("complete receipt pipeline did not dispatch B while A ack was held: live={d} slots={d}\n", .{
                c.test_observed_live_handlers.load(.acquire),
                c.test_observed_slots_in_use.load(.acquire),
            });
            return error.SecondCompleteBlockedByFirstAck;
        }
        zio.sleep(.fromMilliseconds(1)) catch {};
    }

    c.test_hold_complete_receipt_ack.store(false, .release);
    c.test_release_complete_receipt_ack.set(io);
    try waitAccountingZeroAt(&server, 2_000, @src().line);
    stream.close();
    stream_open = false;

    server.requestShutdown();
    serve_handle.join() catch {};

    try std.testing.expectEqual(@as(usize, 0), server.active_connections.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.active_streams.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.reaper_reserved.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.outbound_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.request_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), c.test_observed_live_handlers.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), c.test_observed_slots_in_use.load(.acquire));
}

test "lifecycle: DebugAllocator clean under live SSE reset/shutdown" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) {
            @panic("DebugAllocator reported leak/UAF after lifecycle stress");
        }
    }
    const gpa = dbg.allocator();

    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 64 },
        // 100 live SSE tasks plus pumps: two executors is the hang that
        // inlined a write-ack hold into this test. Keep this suite off that
        // trap; the dedicated complete-receipt test is the hold coverage.
        .executors = .exact(4),
        .enable_task_migration = true,
    });
    defer rt.deinit();

    var handle = try rt.spawn(runLifecycleStress, .{ rt, gpa });
    // Name the error. Without this the failure is a bare return trace through
    // `join`, which says a stress run failed but not how — and an unnamed
    // nondeterministic failure is what gets written off as a flake instead of
    // root-caused. Same reason t-544 added caller lines to waitAccountingZero.
    handle.join() catch |err| {
        std.debug.print("lifecycle stress failed: {s}\n", .{@errorName(err)});
        return err;
    };
}

test "complete receipt pipeline ingests B while A write ack is held" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in complete receipt pipeline");
    }
    const gpa = dbg.allocator();

    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
        .enable_task_migration = false,
    });
    defer rt.deinit();

    var handle = try rt.spawn(runCompleteReceiptPipeline, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("complete receipt pipeline failed: {s}\n", .{@errorName(err)});
        return err;
    };
}

fn recordHandlerErr(err: anyerror) void {
    const c = starh2.edge.connection;
    const code: u8 = switch (err) {
        error.WriteFailed => 1,
        error.ConnectionClosed => 2,
        error.PeerReset => 3,
        error.SlowConsumer => 4,
        else => 5,
    };
    c.test_last_handler_err.store(code, .release);
}

fn writeFailHello(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    starh2.edge.connection.test_last_handler_err.store(0, .release);
    // Large body so the handler is still blocked on flow/flush when the pump fails.
    var body: [70 * 1024]u8 = undefined;
    @memset(&body, 'W');
    resp.send(200, &.{}, &body) catch |err| {
        recordHandlerErr(err);
        if (err == error.WriteFailed) return;
        return err;
    };
    return error.ExpectedWriteFailed;
}

fn largeBodyHello(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    starh2.edge.connection.test_last_handler_err.store(0, .release);
    // >64KiB one-shot — blocks on small peer window until WINDOW_UPDATE.
    var body: [70 * 1024]u8 = undefined;
    @memset(&body, 'B');
    resp.send(200, &.{}, &body) catch |err| {
        recordHandlerErr(err);
        return err;
    };
}

fn peerResetWhileBlocked(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    starh2.edge.connection.test_last_handler_err.store(0, .release);
    var body: [70 * 1024]u8 = undefined;
    @memset(&body, 'R');
    resp.send(200, &.{}, &body) catch |err| {
        recordHandlerErr(err);
        if (err == error.PeerReset) {
            if (resp.slotTerminal()) |c| switch (c) {
                .peer_reset => |code| starh2.edge.connection.test_last_peer_reset_code.store(@intFromEnum(code), .release),
                else => {},
            };
        }
        return; // expected PeerReset
    };
    return error.ExpectedPeerReset;
}

fn runWriteFailStress(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/wf", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = writeFailHello } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 8;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    limits.graceful_drain_timeout_ns = 200 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);

    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    // `waitUntilListening`, never a sleep: `.listening` publishes only after
    // every endpoint is bound AND its accept loop is spawned, and it reports
    // BindFailed instead of timing out. A sleep here is both slower than it
    // needs to be and wrong under load — and `localAddress` reads uninitialized
    // storage until the bind lands.
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();

    var round: usize = 0;
    while (round < 100) : (round += 1) {
        starh2.edge.connection.test_write_fail_after = 0;
        starh2.edge.wire_pump.test_fail_next_write.store(false, .release);
        starh2.edge.connection.test_observed_writer_fail_handled.store(false, .release);
        starh2.edge.connection.test_queue_wire_bypass.store(0, .release);
        starh2.edge.connection.test_last_handler_err.store(0, .release);
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const wire = try h2c.buildClientHelloWindow(gpa, "/wf", 1, 256, .defaults);
        defer gpa.free(wire);
        try writeAllStreamAt(stream, wire, @src());
        var waited: u64 = 0;
        while (waited < 1000) : (waited += 5) {
            if (starh2.edge.connection.test_observed_live_handlers.load(.acquire) > 0) break;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        // Fail the next transport write while the handler is mid-send.
        starh2.edge.wire_pump.test_fail_next_write.store(true, .release);
        var boost: std.ArrayList(u8) = .empty;
        defer boost.deinit(gpa);
        try h2c.appendWindowUpdate(gpa, &boost, 1, 64 * 1024);
        try h2c.appendWindowUpdate(gpa, &boost, 0, 64 * 1024);
        writeAllStreamAt(stream, boost.items, @src()) catch {};
        try waitAccountingZeroAt(&server, 2_000, 392);
        try std.testing.expectEqual(@as(usize, 0), server.accounting.active_streams.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), server.accounting.reaper_reserved.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), server.accounting.outbound_bytes.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), server.accounting.request_bytes.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_observed_live_handlers.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_observed_slots_in_use.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_queue_wire_bypass.load(.acquire));
        const herr = starh2.edge.connection.test_last_handler_err.load(.acquire);
        try std.testing.expectEqual(@as(u8, 1), herr);
    }
    server.requestShutdown();
    zio.sleep(.fromMilliseconds(200)) catch {};
}

test "waitForStreamSpace: cancel while blocked is lock-balanced under DebugAllocator" {
    // Mechanical: per-stream space_sem + lockUncancelable reacquire. Prove cancel
    // while waiting does not double-unlock (DebugAllocator / ASan would trap).
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator leak/UAF after space-wait cancel");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 8, .prewarm = 8 },
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();

    const Ctx = struct {
        sem: zio.Semaphore = .{ .permits = 0 },
        mu: zio.Mutex = .init,
        done: std.atomic.Value(bool) = .init(false),
    };
    var ctx: Ctx = .{};

    const waiter = struct {
        fn f(c: *Ctx) void {
            c.mu.lockUncancelable();
            // Mirror waitForStreamSpace ownership: unlock, wait, always reacquire.
            c.mu.unlock();
            _ = c.sem.wait() catch {};
            c.mu.lockUncancelable();
            c.mu.unlock();
            c.done.store(true, .release);
        }
    }.f;

    var h = try rt.spawn(waiter, .{&ctx});
    zio.sleep(.fromMilliseconds(20)) catch {};
    h.cancel();
    h.join();
    // If cancel raced before wait returned, done may be false — still must not UAF.
    _ = ctx.done.load(.acquire);
}

test "lifecycle: 100x write-fail ticket wake (no hang)" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator leak after write-fail stress");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var handle = try rt.spawn(runWriteFailStress, .{ rt, gpa });
    try handle.join();
}

test "lifecycle: DebugAllocator canary detects intentional leak" {
    // Separate expected-failure mechanism from the stress test's DebugAllocator:
    // a counting allocator that fails closed without std.log.err (which would
    // fail the whole test step even when the canary assertion passes).
    const Canary = struct {
        parent: std.mem.Allocator,
        live: usize = 0,

        fn allocator(self: *@This()) std.mem.Allocator {
            return .{
                .ptr = self,
                .vtable = &.{
                    .alloc = alloc,
                    .resize = resize,
                    .remap = remap,
                    .free = free,
                },
            };
        }

        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const p = self.parent.rawAlloc(len, alignment, ret_addr) orelse return null;
            self.live += 1;
            return p;
        }
        fn resize(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.parent.rawResize(buf, alignment, new_len, ret_addr);
        }
        fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.parent.rawRemap(buf, alignment, new_len, ret_addr);
        }
        fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.parent.rawFree(buf, alignment, ret_addr);
            if (self.live > 0) self.live -= 1;
        }
    };

    var canary: Canary = .{ .parent = std.heap.page_allocator };
    const gpa = canary.allocator();
    const leaked = try gpa.alloc(u8, 32);
    try std.testing.expect(canary.live != 0); // detector fires
    gpa.free(leaked); // reclaim after proving the counter tripped
}

test "slot terminal survives job destroy (no resp pointer)" {
    var slot: starh2.http.response.SlotTerminal = .{};
    slot.setCause(.{ .peer_reset = .cancel });
    try std.testing.expect(slot.getCause() != null);
    try std.testing.expect(slot.cancel_flag.load(.acquire));
    const gen = slot.currentGeneration();
    slot.setCause(.server_shutdown); // first-writer wins on kind
    try std.testing.expectEqual(gen + 1, slot.currentGeneration());
}

fn runGlobalCapStorm(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangSse } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 8;
    limits.max_streams_per_connection = 32;
    limits.max_streams_per_server = 4;
    limits.cancellation_reaper_jobs = 4;
    limits.cancellation_reaper_tasks = 2;
    limits.graceful_drain_timeout_ns = 200 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);

    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    // `waitUntilListening`, never a sleep: `.listening` publishes only after
    // every endpoint is bound AND its accept loop is spawned, and it reports
    // BindFailed instead of timing out. A sleep here is both slower than it
    // needs to be and wrong under load — and `localAddress` reads uninitialized
    // storage until the bind lands.
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();

    // Open more connections than global stream cap; each tries one SSE.
    var i: usize = 0;
    var streams: [8]?zio.net.Stream = .{null} ** 8;
    defer {
        for (&streams) |*s| {
            if (s.*) |st| st.close();
        }
    }
    while (i < 8) : (i += 1) {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        streams[i] = try peer.connect(.{});
        const wire = try h2c.buildClientHello(gpa, "/sse");
        defer gpa.free(wire);
        try writeAllStreamAt(streams[i].?, wire, @src());
    }
    zio.sleep(.fromMilliseconds(150)) catch {};
    try std.testing.expect(server.accounting.active_streams.load(.acquire) <= 4);

    // Cancellation storm: close all sockets.
    for (&streams) |*s| {
        if (s.*) |st| {
            st.close();
            s.* = null;
        }
    }
    zio.sleep(.fromMilliseconds(200)) catch {};

    server.requestShutdown();
    serve_handle.join() catch {};
    try std.testing.expectEqual(@as(usize, 0), server.accounting.active_streams.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.reaper_reserved.load(.acquire));
}

test "lifecycle: global stream cap and cancellation storm" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator leak after global cap storm");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 64 },
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var handle = try rt.spawn(runGlobalCapStorm, .{ rt, gpa });
    try handle.join();
}

const FailIndexOutcome = struct {
    allocations: usize,
    induced: bool,
    response_ok: bool,
};

const fail_index_body = "fail-index-request-body";

fn failIndexHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    if (!std.mem.eql(u8, req.body, fail_index_body)) return error.InvalidRequestBody;
    try resp.send(200, &.{}, "ok");
}

fn runFailIndexLifecycleCase(rt: *zio.Runtime, fail_index: usize) !FailIndexOutcome {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
    const gpa = failing.allocator();
    const limits = starh2.Limits{
        .max_connections = 1,
        .max_streams_per_connection = 2,
        .max_streams_per_server = 2,
        .cancellation_reaper_jobs = 2,
        .cancellation_reaper_tasks = 1,
        .inbound_wire_chunks_per_connection = 2,
        .outbound_bytes_per_stream = 4 * 1024,
        .outbound_bytes_per_connection = 16 * 1024,
        .outbound_bytes_per_server = 16 * 1024,
        .request_bytes_per_connection = 16 * 1024,
        .request_bytes_per_server = 16 * 1024,
        // Leave room above the 4 KiB terminal reserve for ordinary SETTINGS.
        .control_bytes_per_connection = 8 * 1024,
        .control_entries_per_connection = 32,
        .stream_tombstones = 8,
        .concurrent_tls_handshakes = 1,
        .tls_handshake_scratch_bytes = 4 * 1024,
        .max_endpoints = 1,
        .max_routes = 2,
        .max_route_path_bytes = 128,
        .graceful_drain_timeout_ns = 100 * std.time.ns_per_ms,
        .preface_timeout_ns = 300 * std.time.ns_per_ms,
    };
    const routes = [_]starh2.Route{.{
        .method = .POST,
        .path = "/fail-index",
        .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = failIndexHandler } },
    }};
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var server = starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    }) catch {
        try std.testing.expect(failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        return .{
            .allocations = failing.alloc_index,
            .induced = true,
            .response_ok = false,
        };
    };
    var server_live = true;
    defer if (server_live) server.deinit(gpa);

    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serve_live = true;
    defer if (serve_live) {
        server.requestShutdown();
        serve_handle.cancel();
    };
    zio.sleep(.fromMilliseconds(20)) catch {};

    const request_bytes = try h2c.buildClientPost(std.testing.allocator, "/fail-index", fail_index_body);
    defer std.testing.allocator.free(request_bytes);
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());
    var response_ok = false;
    if (peer.connect(.{})) |stream_value| {
        var stream = stream_value;
        if (writeAllStreamAt(stream, request_bytes, @src())) |_| {
            var response_buf: [4096]u8 = undefined;
            var reads: usize = 0;
            while (reads < 20) : (reads += 1) {
                const n = stream.read(&response_buf, .{ .duration = .fromMilliseconds(100) }) catch |err| switch (err) {
                    error.Timeout => continue,
                    else => break,
                };
                if (n == 0) break;
                if (std.mem.indexOf(u8, response_buf[0..n], "ok") != null) {
                    response_ok = true;
                    break;
                }
            }
        } else |_| {}
        stream.close();
    } else |_| {}

    server.requestShutdown();
    serve_handle.join() catch {};
    serve_live = false;
    server.deinit(gpa);
    server_live = false;
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    return .{
        .allocations = failing.alloc_index,
        .induced = failing.has_induced_failure,
        .response_ok = response_ok,
    };
}

test "lifecycle: every public Server allocation failure unwinds cleanly" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        // FailingAllocator is deliberately not thread-safe. One executor makes
        // fail-index ordering deterministic while still exercising task unwind.
        .executors = .exact(1),
        .enable_task_migration = false,
    });
    defer rt.deinit();

    const baseline = try runFailIndexLifecycleCase(rt, std.math.maxInt(usize));
    try std.testing.expect(!baseline.induced);
    try std.testing.expect(baseline.response_ok);
    try std.testing.expect(baseline.allocations > 0);

    var fail_index: usize = 0;
    var reached: usize = 0;
    while (fail_index < baseline.allocations) : (fail_index += 1) {
        const outcome = try runFailIndexLifecycleCase(rt, fail_index);
        // A run that never REACHED this index has no allocation to fail, and
        // demanding one asserts something the run cannot satisfy.
        //
        // The count is not fixed: the case drives a live client — connect, a
        // 20ms settle, a read loop with 100ms timeouts — so how far it gets
        // varies, and with cooperative scheduling the ORDER of allocations
        // across tasks varies with it. `baseline.allocations` is therefore one
        // sample of a distribution, not a constant, and a sweep bounded by it
        // will sometimes run past the end of a shorter run. That is what made
        // this test fail roughly 1 run in 8 on master.
        //
        // The teeth are unchanged: every allocation that IS reached must induce
        // a failure, and `runFailIndexLifecycleCase` asserts allocated == freed
        // on every case, so an unwind that leaks still fails here.
        if (outcome.induced) {
            reached += 1;
        } else {
            try std.testing.expect(outcome.allocations <= fail_index);
        }
    }
    // Make the scope observable. Measured coverage is 68/68, 67/67, 68/68 —
    // full, with the baseline itself varying by one. Anything less means runs
    // are getting shorter and the sweep is quietly testing less than it used
    // to, which is invisible from a passing result.
    //
    // Warn on ANY shortfall, fail only on a collapse. A tight percentage here
    // would just reintroduce the flakiness this change removed, because the
    // bound is a sample rather than a constant.
    if (reached < baseline.allocations) {
        std.debug.print(
            "fail-index sweep covered {d} of {d} indices (runs are getting shorter)\n",
            .{ reached, baseline.allocations },
        );
    }
    try std.testing.expect(reached > baseline.allocations / 2);
}

test "regression: stream map drops closed streams (bounded history)" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    // free intents
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| std.testing.allocator.free(f.payload),
            else => {},
        };
        // intent_drain scratch — do not free slice
    }

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/hello" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(block);

    var sid: u31 = 1;
    var n: usize = 0;
    while (n < 2000) : (n += 1) {
        var wire: std.ArrayList(u8) = .empty;
        defer wire.deinit(std.testing.allocator);
        if (n == 0) {
            try wire.appendSlice(std.testing.allocator, frame.CLIENT_PREFACE);
            var sbuf: [9]u8 = undefined;
            const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
            try wire.appendSlice(std.testing.allocator, sbuf[0..sn]);
        }
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = true },
            .stream_id = sid,
        };
        fh.encode(&hdr_buf);
        try wire.appendSlice(std.testing.allocator, &hdr_buf);
        try wire.appendSlice(std.testing.allocator, block);
        try session.ingest(wire.items);
        const intents = session.drainIntents();
        var respond_sid: ?u31 = null;
        for (intents) |*it| {
            switch (it.*) {
                .dispatch_request => |d| {
                    respond_sid = d.stream_id;
                    starh2.core.hpack.HeaderField.freeOwnedSlice(std.testing.allocator, d.headers);
                    std.testing.allocator.free(d.headers);
                    starh2.core.hpack.HeaderField.freeOwnedSlice(std.testing.allocator, d.trailers);
                    if (d.trailers.len != 0) std.testing.allocator.free(d.trailers);
                    if (d.body.len != 0) std.testing.allocator.free(d.body);
                },
                .outbound_frame => |f| std.testing.allocator.free(f.payload),
                else => {},
            }
        }
        if (respond_sid) |rsid| {
            session.applyCommand(.{ .respond_headers = .{
                .stream_id = rsid,
                .status = 200,
                .headers = &.{},
                .end_stream = true,
            } }) catch {};
        }
        {
            const more = session.drainIntents();
            for (more) |*it| switch (it.*) {
                .outbound_frame => |f| std.testing.allocator.free(f.payload),
                else => {},
            };
        }
        try std.testing.expect(session.streams.count() <= session.limits.max_streams_per_connection);
        try std.testing.expect(session.tombstones.items.len <= session.limits.stream_tombstones);
        sid += 2;
    }
}

test "rates: SETTINGS flood trips ENHANCE_YOUR_CALM" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| std.testing.allocator.free(f.payload),
            else => {},
        };
        // intent_drain scratch — do not free slice
    }
    var rates: starh2.core.rates.RateLimiter = .{};
    // Tiny SETTINGS budget so a flood trips immediately.
    rates.settings = .init(2, 0);
    session.rate_limiter = &rates;
    session.edge_now_ns = 1;

    try session.ingest(frame.CLIENT_PREFACE);
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        var sbuf: [9]u8 = undefined;
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
        try session.ingest(sbuf[0..sn]);
        if (session.terminal != .none) break;
    }
    try std.testing.expect(session.terminal != .none);
    switch (session.terminal) {
        .goaway => |g| try std.testing.expectEqual(frame.ErrorCode.enhance_your_calm, g.code),
        else => return error.ExpectedGoaway,
    }
    const intents = session.drainIntents();
    for (intents) |*it| switch (it.*) {
        .outbound_frame => |f| std.testing.allocator.free(f.payload),
        else => {},
    };
    // intent_drain scratch — do not free slice
}

test "rates: HEADERS flood trips ENHANCE_YOUR_CALM" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    const gpa = std.testing.allocator;
    var session = try session_mod.Session.init(gpa, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }
    var rates: starh2.core.rates.RateLimiter = .{};
    rates.headers = .init(2, 0);
    session.rate_limiter = &rates;
    session.edge_now_ns = 1;

    try session.ingest(frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try session.ingest(sbuf[0..sn]);
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(gpa, &fields);
    defer gpa.free(block);
    var i: u31 = 1;
    while (i < 11) : (i += 2) {
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = true },
            .stream_id = i,
        };
        fh.encode(&hdr_buf);
        var wire: [64]u8 = undefined;
        @memcpy(wire[0..hdr_buf.len], &hdr_buf);
        @memcpy(wire[hdr_buf.len..][0..block.len], block);
        try session.ingest(wire[0 .. hdr_buf.len + block.len]);
        {
            const intents = session.drainIntents();
            for (intents) |*it| switch (it.*) {
                .outbound_frame => |f| gpa.free(f.payload),
                .dispatch_request => |d| {
                    starh2.core.hpack.HeaderField.freeOwnedSlice(gpa, d.headers);
                    gpa.free(d.headers);
                    if (d.trailers.len != 0) {
                        starh2.core.hpack.HeaderField.freeOwnedSlice(gpa, d.trailers);
                        gpa.free(d.trailers);
                    }
                    if (d.body.len != 0) gpa.free(d.body);
                },
                else => {},
            };
        }
        if (session.terminal != .none) break;
    }
    try std.testing.expect(session.terminal != .none);
    switch (session.terminal) {
        .goaway => |g| try std.testing.expectEqual(frame.ErrorCode.enhance_your_calm, g.code),
        else => return error.ExpectedGoaway,
    }
    const more = session.drainIntents();
    for (more) |*it| switch (it.*) {
        .outbound_frame => |f| gpa.free(f.payload),
        .dispatch_request => |d| {
            starh2.core.hpack.HeaderField.freeOwnedSlice(gpa, d.headers);
            gpa.free(d.headers);
            if (d.trailers.len != 0) {
                starh2.core.hpack.HeaderField.freeOwnedSlice(gpa, d.trailers);
                gpa.free(d.trailers);
            }
            if (d.body.len != 0) gpa.free(d.body);
        },
        else => {},
    };
}

fn runLargeBodyWindowGate(rt: *zio.Runtime, gpa: std.mem.Allocator, stream_id: u31) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = largeBodyHello } } },
        .{ .method = .GET, .path = "/rstblock", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = peerResetWhileBlocked } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    limits.outbound_bytes_per_stream = 128 * 1024;
    limits.outbound_bytes_per_connection = 512 * 1024;
    limits.slow_consumer_timeout_ns = 30 * std.time.ns_per_s;
    limits.graceful_drain_timeout_ns = 500 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    // `waitUntilListening`, never a sleep: `.listening` publishes only after
    // every endpoint is bound AND its accept loop is spawned, and it reports
    // BindFailed instead of timing out. A sleep here is both slower than it
    // needs to be and wrong under load — and `localAddress` reads uninitialized
    // storage until the bind lands.
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();

    // --- Small window: handler blocks; WINDOW_UPDATE resumes; body completes. ---
    {
        starh2.edge.connection.test_write_delay_ms = 20;
        defer starh2.edge.connection.test_write_delay_ms = 0;
        starh2.edge.connection.test_last_handler_err.store(0, .release);
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const open = try h2c.buildClientHelloWindow(gpa, "/big", stream_id, 1024, .defaults);
        defer gpa.free(open);
        try writeAllStreamAt(stream, open, @src());
        // Handler should still be blocked (no full body yet).
        zio.sleep(.fromMilliseconds(50)) catch {};
        try std.testing.expectEqual(@as(u8, 0), starh2.edge.connection.test_last_handler_err.load(.acquire));

        // Feed stream + connection credit in chunks until body can finish.
        var boost: std.ArrayList(u8) = .empty;
        defer boost.deinit(gpa);
        var k: usize = 0;
        while (k < 80) : (k += 1) {
            try h2c.appendWindowUpdate(gpa, &boost, stream_id, 2048);
            try h2c.appendWindowUpdate(gpa, &boost, 0, 2048);
        }
        try writeAllStreamAt(stream, boost.items, @src());
        // Drain response bytes; delayed writer requires ack before send returns.
        var drain_buf: [16 * 1024]u8 = undefined;
        var reads: usize = 0;
        while (reads < 80) : (reads += 1) {
            _ = stream.read(drain_buf[0..], .none) catch {};
            zio.sleep(.fromMilliseconds(10)) catch {};
            if (starh2.edge.connection.test_observed_live_handlers.load(.acquire) == 0 and
                server.accounting.outbound_bytes.load(.acquire) == 0)
            {
                break;
            }
        }
        // defer stream.close() tears down; wait for accounting after block ends.
    }
    try waitAccountingZeroAt(&server, 5_000, 944);
    try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_queue_wire_bypass.load(.acquire));

    // --- RST while blocked on sparse/dense stream. ---
    {
        starh2.edge.connection.test_last_handler_err.store(0, .release);
        starh2.edge.connection.test_last_peer_reset_code.store(0, .release);
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        const open = try h2c.buildClientHelloWindow(gpa, "/rstblock", stream_id, 512, .defaults);
        defer gpa.free(open);
        try writeAllStreamAt(stream, open, @src());
        var waited: u64 = 0;
        while (waited < 2000) : (waited += 10) {
            if (starh2.edge.connection.test_observed_live_handlers.load(.acquire) > 0) break;
            zio.sleep(.fromMilliseconds(10)) catch {};
        }
        var rst: std.ArrayList(u8) = .empty;
        defer rst.deinit(gpa);
        try h2c.appendRst(gpa, &rst, stream_id);
        try writeAllStreamAt(stream, rst.items, @src());
        waited = 0;
        while (waited < 3000) : (waited += 10) {
            if (starh2.edge.connection.test_last_handler_err.load(.acquire) == 3) break;
            zio.sleep(.fromMilliseconds(10)) catch {};
        }
        stream.close();
    }
    try waitAccountingZeroAt(&server, 5_000, 972);
    const herr = starh2.edge.connection.test_last_handler_err.load(.acquire);
    try std.testing.expectEqual(@as(u8, 3), herr); // PeerReset only
    const frame = starh2.core.frame;
    try std.testing.expectEqual(@as(u32, @intFromEnum(frame.ErrorCode.cancel)), starh2.edge.connection.test_last_peer_reset_code.load(.acquire));
}

test "lifecycle: >64KiB body under small window + RST (stream 1)" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        if (dbg.deinit() != .ok) @panic("DebugAllocator leak after large-body gate");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var h = try rt.spawn(runLargeBodyWindowGate, .{ rt, gpa, @as(u31, 1) });
    try h.join();
}

test "lifecycle: >64KiB body under small window + RST (sparse stream id)" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        if (dbg.deinit() != .ok) @panic("DebugAllocator leak after sparse large-body gate");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    // Sparse odd client stream id — must use handler-slot space waiter, not (id-1)/2.
    var h = try rt.spawn(runLargeBodyWindowGate, .{ rt, gpa, @as(u31, 1_000_001) });
    try h.join();
}

// One resp.send whose body EXCEEDS outbound_bytes_per_stream. The handler must
// chunk through the cap as the actor drains — never park forever waiting for
// space that only its own drain can create. Static so the task stack stays small.
const cap_crossing_body = [_]u8{'B'} ** (1024 * 1024);

fn capCrossingHello(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    starh2.edge.connection.test_last_handler_err.store(0, .release);
    resp.send(200, &.{}, &cap_crossing_body) catch |err| {
        recordHandlerErr(err);
        return err;
    };
}

fn runCapCrossingBody(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = capCrossingHello } } },
    };
    // qmdsync's limitsFor shape (t-482): streams capped at handler_limit,
    // slow-consumer deadline at the SSE send timeout. outbound_bytes_per_stream
    // stays the 64 KiB default, and the page is ~1 MiB.
    var limits = starh2.Limits.defaults;
    limits.max_streams_per_server = 32;
    limits.max_streams_per_connection = 32;
    // Deliberately NOT the 2s slow-consumer deadline qmdsync also sets: a
    // loaded test host can starve this client past any short deadline, and a
    // kill of a genuinely slow reader is correct behaviour, not a regression.
    // The two regressions this gate guards are completion and ordering.
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
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
    const port = server.localAddress(0).getPort();

    // t-537 closed: a delivered response reports success even when the peer
    // closes in the ack's wake-to-run gap (awaiting_receipt skip-cancel +
    // completed-ok wins in TicketTable.wait). One attempt, hard asserts.
    try capCrossingAttempt(gpa, &server, port);
    // The body reaching the peer is not the whole contract: resp.send must
    // RETURN. A handler parked forever on the final flush ack is the t-482
    // /ui/tasks hang, and only the accounting can see it from out here.
    try waitAccountingZeroAt(&server, 3_000, 1063);
    try std.testing.expectEqual(@as(u8, 0), starh2.edge.connection.test_last_handler_err.load(.acquire));
}

/// One connection's fetch of /big with curl-shaped flow control: DEFAULT
/// 64 KiB windows, credit granted per received DATA payload — never a big
/// up-front grant. Errors when the body does not complete.
fn capCrossingAttempt(gpa: std.mem.Allocator, server: *starh2.Server, port: u16) !void {
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    defer stream.close();

    const open = try h2c.buildClientHelloWindow(gpa, "/big", 1, 65_535, .defaults);
    defer gpa.free(open);
    try writeAllStreamAt(stream, open, @src());

    const frame = starh2.core.frame;
    var total: usize = 0;
    var ended = false;
    var got_headers = false;
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    var payload_buf: [frame.DEFAULT_MAX_FRAME_SIZE]u8 = undefined;
    const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% 10 * std.time.ns_per_s;
    while (!ended) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) break;
        readExact(stream, hdr_buf[0..]) catch break;
        const hdr = frame.FrameHeader.decode(&hdr_buf);
        if (hdr.length > payload_buf.len) return error.FrameTooLarge;
        if (hdr.length != 0) readExact(stream, payload_buf[0..hdr.length]) catch break;
        switch (hdr.type) {
            .settings => {
                if (!hdr.flags.ack()) {
                    var ack: [frame.FRAME_HEADER_LEN]u8 = undefined;
                    const an = try frame.Serializer.settingsFrame(&ack, true, &.{});
                    try writeAllStreamAt(stream, ack[0..an], @src());
                }
            },
            .data => {
                if (hdr.stream_id == 1) {
                    // The regression this gate exists for: a cap-crossing body
                    // whose DATA reaches the wire while HEADERS never do.
                    if (!got_headers) return error.DataBeforeHeaders;
                    total += hdr.length;
                    if (hdr.flags.end_stream) {
                        ended = true;
                    } else if (hdr.length != 0) {
                        var credit: std.ArrayList(u8) = .empty;
                        defer credit.deinit(gpa);
                        try h2c.appendWindowUpdate(gpa, &credit, 1, @intCast(hdr.length));
                        try h2c.appendWindowUpdate(gpa, &credit, 0, @intCast(hdr.length));
                        try writeAllStreamAt(stream, credit.items, @src());
                    }
                }
            },
            .headers => {
                if (hdr.stream_id == 1) {
                    got_headers = true;
                    if (hdr.flags.end_stream) ended = true;
                }
            },
            .rst_stream, .goaway => break,
            else => {},
        }
    }
    if (total < cap_crossing_body.len) {
        std.debug.print(
            "cap-crossing short: data_total={d} ended={} waiting_for_space_stream={d} outbound_acct={d} live_handlers={d}\n",
            .{
                total,
                ended,
                starh2.edge.connection.test_waiting_for_space.load(.acquire),
                server.accounting.outbound_bytes.load(.acquire),
                starh2.edge.connection.test_observed_live_handlers.load(.acquire),
            },
        );
        return error.ShortDelivery;
    }
}

test "lifecycle: a single send bigger than outbound_bytes_per_stream completes (t-482)" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        if (dbg.deinit() != .ok) @panic("DebugAllocator leak after cap-crossing gate");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var h = try rt.spawn(runCapCrossingBody, .{ rt, gpa });
    h.join() catch |err| {
        std.debug.print("cap-crossing run failed with error.{s}\n", .{@errorName(err)});
        return err;
    };
}

/// The server's FIRST frame must be its own (non-ack) SETTINGS — RFC 7540 3.5.
/// When the whole client flight (preface + SETTINGS + HEADERS) lands in one
/// read, the server queues its preface SETTINGS and the client-SETTINGS ack in
/// the same intent batch before the first drain; ack frames classified as
/// terminal-class used to jump the queue and reach the wire first (t-538 —
/// nghttp2 rejects the connection with "expected SETTINGS").
fn runPrefaceFirstFrame(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/hello", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
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
        serve_handle.join() catch {};
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();
    const frame = starh2.core.frame;

    // Several rounds: the reorder was a race against the first drain.
    var round: usize = 0;
    while (round < 8) : (round += 1) {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        // The ENTIRE client flight in one write, like a pipelined TLS peer.
        const wire = try h2c.buildClientHello(gpa, "/hello");
        defer gpa.free(wire);
        try writeAllStreamAt(stream, wire, @src());
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        try readExact(stream, hdr_buf[0..]);
        const hdr = frame.FrameHeader.decode(&hdr_buf);
        try std.testing.expectEqual(frame.FrameType.settings, hdr.type);
        try std.testing.expect(!hdr.flags.ack());
    }
}

test "lifecycle: the first wire frame is the server's own SETTINGS, never an ack (t-538)" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        if (dbg.deinit() != .ok) @panic("DebugAllocator leak after preface-first gate");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var h = try rt.spawn(runPrefaceFirstFrame, .{ rt, gpa });
    try h.join();
}

fn runListeningReadiness(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/hello", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    };

    // --- Success: .listening must mean connectable, with no sleep to cover a race. ---
    const any_port = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = any_port }},
        .routes = &routes,
        .tls = null,
    });
    defer server.deinit(gpa);
    try std.testing.expectEqual(starh2.BindState.pending, server.bind_state.load(.acquire));

    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);

    const port = server.localAddress(0).getPort();
    try std.testing.expect(port != 0);
    {
        // Connect immediately: if readiness were published before accept was live
        // this is what would fail.
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const wire = try h2c.buildClientHello(gpa, "/hello");
        defer gpa.free(wire);
        try writeAllStreamAt(stream, wire, @src());
        try waitAccountingZeroAt(&server, 5_000, 1250);
    }

    // A second wait on an already-listening server returns immediately.
    try server.waitUntilListening(0);

    server.requestShutdown();
    try serve_handle.join();

    // --- Failure: an occupied port must report .failed, not time out. ---
    const taken = try starh2.EndpointAddress.parseIp4("127.0.0.1", port);
    var squatter = try taken.listen(rt.io(), .{ .reuse_address = false });
    defer squatter.deinit(rt.io());
    const squatted = squatter.socket.address;

    var doomed = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = squatted }},
        .routes = &routes,
        .tls = null,
    });
    defer doomed.deinit(gpa);
    var doomed_handle = try rt.spawn(starh2.Server.serve, .{ &doomed, gpa });
    try std.testing.expectError(error.BindFailed, doomed.waitUntilListening(5 * std.time.ns_per_s));
    try std.testing.expectError(error.ListenFailed, doomed_handle.join());
}

test "lifecycle: waitUntilListening is authoritative for bind success and failure" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        if (dbg.deinit() != .ok) @panic("DebugAllocator leak after readiness gate");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var h = try rt.spawn(runListeningReadiness, .{ rt, gpa });
    try h.join();
}
