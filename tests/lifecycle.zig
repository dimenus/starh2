//! Lifecycle stress under DebugAllocator — must fail on leaks / nonzero counters.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const dummy: u8 = 0;

/// Axis G: never_unmap so a use-after-free or double free is the allocator's
/// trap, not a silent reuse of the same page.
const LifecycleDbg = std.heap.DebugAllocator(.{
    .never_unmap = true,
    .retain_metadata = true,
    .safety = true,
});

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

/// Answers, then keeps running: a handler may do work after its response,
/// and its stream is closed for the peer the moment END_STREAM arrives.
var linger_runs: std.atomic.Value(usize) = .init(0);

fn helloThenLinger(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    _ = linger_runs.fetchAdd(1, .acq_rel);
    try resp.send(200, &.{}, "ok");
    zio.sleep(.fromMilliseconds(300)) catch {};
}

const big_request_bytes = 1024 * 1024;

fn bigArenaTask(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const scratch = try req.arena.alloc(u8, big_request_bytes);
    @memset(scratch, 0x5a);
    try resp.send(200, &.{}, "ok");
}

fn bigArenaComplete(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    const scratch = try req.arena.alloc(u8, big_request_bytes);
    @memset(scratch, 0x5a);
    try resp.send(200, &.{}, "ok");
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
        const ping_n = try starh2.core.frame.Serializer.ping(&ping_buf, false, &@as([8]u8, @splat(0)));
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
    c.test_release_complete_receipt_ack.reset();
    c.test_complete_receipt_ack_held.store(false, .release);
    c.test_hold_complete_receipt_ack.store(true, .release);
    defer {
        c.test_hold_complete_receipt_ack.store(false, .release);
        c.test_release_complete_receipt_ack.set();
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

    // A held receipt ack proves A dispatched, ran and was written.
    const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% 2 * std.time.ns_per_s;
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

    // A's job finished at the end of its drain turn, so a handler count no
    // longer shows B overlapping A. B's END_STREAM on the wire while A's
    // receipt ack is still held does: the actor ingested, ran and emitted B
    // without waiting for A's write.
    const b_outcome = try awaitStreamOutcome(stream, 3, 250);
    if (b_outcome != .ended) {
        std.debug.print("complete receipt pipeline did not answer B while A ack was held: {s} held={}\n", .{
            @tagName(b_outcome),
            c.test_complete_receipt_ack_held.load(.acquire),
        });
        return error.SecondCompleteBlockedByFirstAck;
    }
    if (!c.test_complete_receipt_ack_held.load(.acquire)) return error.CompleteReceiptAckNotHeld;

    c.test_hold_complete_receipt_ack.store(false, .release);
    c.test_release_complete_receipt_ack.set();
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
    var dbg = LifecycleDbg.init;
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
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in complete receipt pipeline");
    }
    const gpa = dbg.allocator();

    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();

    var handle = try rt.spawn(runCompleteReceiptPipeline, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("complete receipt pipeline failed: {s}\n", .{@errorName(err)});
        return err;
    };
}

const StreamOutcome = enum { ended, reset, timed_out };

/// Read frames until every stream in `sids` ends (HEADERS or DATA with
/// END_STREAM) or is reset, or the deadline passes. A stream still open at the
/// deadline reads `.timed_out`. Frames for other streams are skipped, so wait
/// for every stream you care about in one call.
fn awaitStreamOutcomes(stream: zio.net.Stream, sids: []const u31, out: []StreamOutcome, timeout_ms: u64) !void {
    std.debug.assert(sids.len == out.len);
    @memset(out, .timed_out);
    var open = sids.len;
    const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% timeout_ms * std.time.ns_per_ms;
    var hdr: [9]u8 = undefined;
    var payload: [64 * 1024]u8 = undefined;
    while (open > 0) {
        const now = zio.Timestamp.now(.monotonic).toNanoseconds();
        if (now >= deadline) return;
        const n = stream.read(hdr[0..1], .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
        if (n == 0) return error.ConnectionClosed;
        try readExact(stream, hdr[1..]);
        const len = (@as(usize, hdr[0]) << 16) | (@as(usize, hdr[1]) << 8) | hdr[2];
        const typ = hdr[3];
        const flags = hdr[4];
        const frame_sid: u31 = @intCast(std.mem.readInt(u32, hdr[5..9], .big) & 0x7fff_ffff);
        std.debug.assert(len <= payload.len);
        try readExact(stream, payload[0..len]);
        for (sids, out) |sid, *o| {
            if (sid != frame_sid or o.* != .timed_out) continue;
            if (typ == 0x3) { // RST_STREAM
                o.* = .reset;
                open -= 1;
            } else if ((typ == 0x0 or typ == 0x1) and flags & 0x1 != 0) {
                o.* = .ended;
                open -= 1;
            }
        }
    }
}

fn awaitStreamOutcome(stream: zio.net.Stream, sid: u31, timeout_ms: u64) !StreamOutcome {
    var out: [1]StreamOutcome = undefined;
    try awaitStreamOutcomes(stream, &.{sid}, &out, timeout_ms);
    return out[0];
}

/// A stream is closed for the peer once both sides sent END_STREAM, so a new
/// stream after that is inside the advertised limit even while the server
/// still holds the old stream's handler slot for its write receipt. The new
/// stream must wait for the slot, never be refused (t-3491).
fn runSlotHeldByReceipt(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/complete", .handler = .{ .complete = .{
            .ptr = @constCast(&dummy),
            .runFn = completeHello,
        } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 1;
    limits.max_streams_per_server = 8;
    limits.cancellation_reaper_jobs = 8;
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
    c.test_release_complete_receipt_ack.reset();
    c.test_complete_receipt_ack_held.store(false, .release);
    c.test_hold_complete_receipt_ack.store(true, .release);
    defer {
        c.test_hold_complete_receipt_ack.store(false, .release);
        c.test_release_complete_receipt_ack.set();
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
    // Stream 1's response reaches us while its write ack is held, so its slot
    // stays occupied although the stream is closed.
    try std.testing.expectEqual(StreamOutcome.ended, try awaitStreamOutcome(stream, 1, 2_000));
    const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% 2 * std.time.ns_per_s;
    while (!c.test_complete_receipt_ack_held.load(.acquire)) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) return error.CompleteReceiptAckNotHeld;
        zio.sleep(.fromMilliseconds(1)) catch {};
    }

    var second: std.ArrayList(u8) = .empty;
    defer second.deinit(gpa);
    try h2c.appendHeaders(gpa, &second, 3, "/complete", true);
    try writeAllStreamAt(stream, second.items, @src());
    const while_held = try awaitStreamOutcome(stream, 3, 300);
    if (while_held == .reset) return error.RefusedInsideAdvertisedLimit;

    c.test_hold_complete_receipt_ack.store(false, .release);
    c.test_release_complete_receipt_ack.set();
    if (while_held != .ended) {
        try std.testing.expectEqual(StreamOutcome.ended, try awaitStreamOutcome(stream, 3, 2_000));
    }
    try waitAccountingZeroAt(&server, 2_000, @src().line);
    stream.close();
    stream_open = false;
}

test "a stream inside the advertised limit waits for a receipt-held slot instead of being refused" {
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in slot-held-by-receipt test");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();
    var handle = try rt.spawn(runSlotHeldByReceipt, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("slot held by receipt: {s}\n", .{@errorName(err)});
        return err;
    };
}

/// Stream 1's task handler has answered but not returned, so its slot is busy
/// while the peer may already open stream 3 inside the advertised limit of
/// one. Stream 3 must wait for the slot, never be refused (t-3494). The
/// no-slot path once also leaked a server-wide reaper token per stream, so
/// the accounting must come back to zero after it.
fn runTaskSlotHeldAfterResponse(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/linger", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = helloThenLinger } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 1;
    limits.max_streams_per_server = 8;
    limits.cancellation_reaper_jobs = 8;
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

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    var stream_open = true;
    defer if (stream_open) stream.close();

    var first = try h2c.buildClientPrefaceAndSettings(gpa);
    defer first.deinit(gpa);
    try h2c.appendHeaders(gpa, &first, 1, "/linger", true);
    try writeAllStreamAt(stream, first.items, @src());
    try std.testing.expectEqual(StreamOutcome.ended, try awaitStreamOutcome(stream, 1, 2_000));

    var second: std.ArrayList(u8) = .empty;
    defer second.deinit(gpa);
    try h2c.appendHeaders(gpa, &second, 3, "/linger", true);
    try writeAllStreamAt(stream, second.items, @src());
    // helloThenLinger keeps stream 1's slot for 300 ms after it answered.
    const while_busy = try awaitStreamOutcome(stream, 3, 100);
    if (while_busy == .reset) return error.RefusedInsideAdvertisedLimit;
    // Answered while the only slot was still busy: nothing waited.
    if (while_busy == .ended) return error.SlotNotHeldByLingeringHandler;
    try std.testing.expectEqual(StreamOutcome.ended, try awaitStreamOutcome(stream, 3, 2_000));
    try waitAccountingZeroAt(&server, 2_000, @src().line);
    stream.close();
    stream_open = false;
}

test "a stream inside the advertised limit waits for a task handler that answered but has not returned" {
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in task-slot test");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();
    var handle = try rt.spawn(runTaskSlotHeldAfterResponse, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("task slot held after response: {s}\n", .{@errorName(err)});
        return err;
    };
}

/// Larger than the 64 KiB stream slab, so the handler must wait for the
/// actor to drain before it can queue the rest.
const big_task_body_bytes = 512 * 1024;

fn bigTaskBody(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const body = try req.arena.alloc(u8, big_task_body_bytes);
    @memset(body, 't');
    try resp.send(200, &.{}, body);
}

/// Set by `slowComplete`: stream 1's task handler was parked in the stream
/// space wait while this complete handler held the actor.
var slow_complete_saw_space_wait: std.atomic.Value(bool) = .init(false);

/// A complete handler runs on the actor, so its sleep keeps the actor inside
/// the inline batch, as a slow batch would.
fn slowComplete(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    zio.sleep(.fromMilliseconds(300)) catch {};
    slow_complete_saw_space_wait.store(starh2.edge.connection.test_waiting_for_space.load(.acquire) == 1, .release);
    try resp.send(200, &.{}, "ok");
}

/// A task handler whose stream slab is full waits for the actor to drain it.
/// While the actor runs a complete batch on the same connection, that wait
/// used to return WriteFailed, because the check asked whether the
/// connection was running an inline batch instead of whether this handler
/// was the inline one. The task stream was reset with INTERNAL_ERROR
/// (t-3509).
fn runTaskWaitDuringCompleteBatch(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = bigTaskBody } } },
        .{ .method = .GET, .path = "/slow", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = slowComplete } } },
    };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = starh2.Limits.defaults,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();

    starh2.edge.connection.test_waiting_for_space.store(0, .release);
    slow_complete_saw_space_wait.store(false, .release);
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    var stream_open = true;
    defer if (stream_open) stream.close();

    var wire = try h2c.buildClientPrefaceAndSettings(gpa);
    defer wire.deinit(gpa);
    try h2c.appendWindowUpdate(gpa, &wire, 0, 1 << 30);
    try h2c.appendHeaders(gpa, &wire, 1, "/big", true);
    try h2c.appendHeaders(gpa, &wire, 3, "/slow", true);
    try writeAllStreamAt(stream, wire.items, @src());

    var outcomes: [2]StreamOutcome = undefined;
    try awaitStreamOutcomes(stream, &.{ 1, 3 }, &outcomes, 5_000);
    if (outcomes[0] == .reset) return error.TaskStreamResetDuringCompleteBatch;
    try std.testing.expectEqual(StreamOutcome.ended, outcomes[0]);
    try std.testing.expectEqual(StreamOutcome.ended, outcomes[1]);
    // The run proves its own shape: without the overlap it tests nothing.
    if (!slow_complete_saw_space_wait.load(.acquire)) return error.TaskNeverWaitedDuringCompleteBatch;
    try waitAccountingZeroAt(&server, 2_000, @src().line);
    stream.close();
    stream_open = false;
}

test "a task handler waits for stream space while the actor runs a complete batch" {
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in task-wait-during-complete-batch test");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();
    var handle = try rt.spawn(runTaskWaitDuringCompleteBatch, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("task wait during complete batch: {s}\n", .{@errorName(err)});
        return err;
    };
}

const ParkedEnd = enum { peer_reset, server_shutdown, peer_close };

/// How a parked request ends when it never gets its slot the ordinary way.
/// Stream 1's handler holds the only slot for 300 ms after answering; stream
/// 3 parks behind it.
fn runParkedEnd(rt: *zio.Runtime, gpa: std.mem.Allocator, how: ParkedEnd) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/linger", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = helloThenLinger } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 1;
    limits.max_streams_per_server = 8;
    limits.cancellation_reaper_jobs = 8;
    limits.cancellation_reaper_tasks = 2;

    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();
    linger_runs.store(0, .release);

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    var stream_open = true;
    defer if (stream_open) stream.close();

    var first = try h2c.buildClientPrefaceAndSettings(gpa);
    defer first.deinit(gpa);
    try h2c.appendHeaders(gpa, &first, 1, "/linger", true);
    try writeAllStreamAt(stream, first.items, @src());
    try std.testing.expectEqual(StreamOutcome.ended, try awaitStreamOutcome(stream, 1, 2_000));

    var second: std.ArrayList(u8) = .empty;
    defer second.deinit(gpa);
    try h2c.appendHeaders(gpa, &second, 3, "/linger", true);
    try writeAllStreamAt(stream, second.items, @src());
    try std.testing.expectEqual(StreamOutcome.timed_out, try awaitStreamOutcome(stream, 3, 50));

    switch (how) {
        .peer_reset => {
            var rst: std.ArrayList(u8) = .empty;
            defer rst.deinit(gpa);
            try h2c.appendRst(gpa, &rst, 3);
            try writeAllStreamAt(stream, rst.items, @src());
            // Past stream 1's linger: the slot is free again, and stream 3
            // must not have run on it.
            zio.sleep(.fromMilliseconds(400)) catch {};
            try std.testing.expectEqual(@as(usize, 1), linger_runs.load(.acquire));
            var third: std.ArrayList(u8) = .empty;
            defer third.deinit(gpa);
            try h2c.appendHeaders(gpa, &third, 5, "/linger", true);
            try writeAllStreamAt(stream, third.items, @src());
            try std.testing.expectEqual(StreamOutcome.ended, try awaitStreamOutcome(stream, 5, 2_000));
            try waitAccountingZeroAt(&server, 2_000, @src().line);
            try std.testing.expectEqual(@as(usize, 2), linger_runs.load(.acquire));
        },
        .server_shutdown => {
            // Hold completions so stream 1's slot stays busy after its handler
            // returns: no handler is live, nothing is left to write, and a
            // request still waits. Graceful finish must not drop it.
            const c = starh2.edge.connection;
            c.test_hold_completion_drain.store(true, .release);
            defer c.test_hold_completion_drain.store(false, .release);
            server.requestShutdown();
            // Past the 1 s phase-1 deadline (this client never acks the
            // grace PING), so the finish check runs in phase 2.
            try std.testing.expectEqual(StreamOutcome.timed_out, try awaitStreamOutcome(stream, 3, 1_500));
            try std.testing.expectEqual(@as(usize, 1), linger_runs.load(.acquire));
            c.test_hold_completion_drain.store(false, .release);
            // A held completion does not wake the actor; a PING does.
            const ping = [_]u8{ 0, 0, 8, 0x6, 0, 0, 0, 0, 0 } ++ [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
            try writeAllStreamAt(stream, &ping, @src());
            // Graceful: a stream the server accepted before GOAWAY is served.
            try std.testing.expectEqual(StreamOutcome.ended, try awaitStreamOutcome(stream, 3, 2_000));
            serve_handle.join() catch {};
            serving = false;
            try std.testing.expectEqual(@as(usize, 0), server.accounting.active_streams.load(.acquire));
            try std.testing.expectEqual(@as(usize, 0), server.accounting.reaper_reserved.load(.acquire));
        },
        .peer_close => {
            stream.close();
            stream_open = false;
            try waitAccountingZeroAt(&server, 2_000, @src().line);
        },
    }
}

fn runParkedEndArm(how: ParkedEnd) !void {
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in parked-end test");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();
    var handle = try rt.spawn(runParkedEnd, .{ rt, gpa, how });
    handle.join() catch |err| {
        std.debug.print("parked end {s}: {s}\n", .{ @tagName(how), @errorName(err) });
        return err;
    };
}

test "a parked stream the peer resets is dropped without running its handler" {
    try runParkedEndArm(.peer_reset);
}

test "a parked stream is still served when the server shuts down gracefully" {
    try runParkedEndArm(.server_shutdown);
}

test "a parked stream is released when the peer closes the connection" {
    try runParkedEndArm(.peer_close);
}

/// Every complete response holds one ordinary control entry (its HEADERS)
/// until WritePump acks the write. With slots no longer held to that ack, the
/// streams in flight can outnumber the entries, and a complete batch must wait
/// for an entry instead of failing the stream with RST_STREAM (t-3491).
fn runCompleteWaitsForControlEntry(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/complete", .handler = .{ .complete = .{
            .ptr = @constCast(&dummy),
            .runFn = completeHello,
        } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    // Four ordinary entries for eight streams. With two, the connection
    // closed before stream 1 was answered; not traced yet (t-3492).
    limits.control_entries_per_connection = starh2.core.limits.TERMINAL_CONTROL_RESERVE_ENTRIES + 4;

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
    // Every write acks late, so each queued HEADERS keeps its entry while
    // the rest of the batch runs.
    c.test_write_delay_ms = 100;
    defer c.test_write_delay_ms = 0;
    const cut_before = c.test_observed_inline_ctrl_full.load(.acquire);

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    var stream_open = true;
    defer if (stream_open) stream.close();

    var wire = try h2c.buildClientPrefaceAndSettings(gpa);
    defer wire.deinit(gpa);
    const sids = [_]u31{ 1, 3, 5, 7, 9, 11, 13, 15 };
    for (sids) |sid| try h2c.appendHeaders(gpa, &wire, sid, "/complete", true);
    try writeAllStreamAt(stream, wire.items, @src());

    var outcomes: [sids.len]StreamOutcome = undefined;
    try awaitStreamOutcomes(stream, &sids, &outcomes, 5_000);
    for (sids, outcomes) |sid, o| {
        if (o != .ended) {
            std.debug.print("control entry wait: stream {d} {s}\n", .{ sid, @tagName(o) });
            return error.StreamNotAnswered;
        }
    }
    // The batch really stopped on a full pool: otherwise this test passes
    // without exercising the wait.
    if (c.test_observed_inline_ctrl_full.load(.acquire) == cut_before) return error.ControlEntriesNotExhausted;
    try waitAccountingZeroAt(&server, 2_000, @src().line);
    stream.close();
    stream_open = false;
}

test "a complete batch waits for a control entry instead of resetting the stream" {
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in control-entry wait test");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();
    var handle = try rt.spawn(runCompleteWaitsForControlEntry, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("control entry wait: {s}\n", .{@errorName(err)});
        return err;
    };
}

/// Read frames until a HEADERS frame has arrived for every stream in `sids`.
fn awaitResponseHeaders(stream: zio.net.Stream, sids: []const u31, timeout_ms: u64) !void {
    var seen: [16]bool = @splat(false);
    std.debug.assert(sids.len <= seen.len);
    var left = sids.len;
    const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% timeout_ms * std.time.ns_per_ms;
    var hdr: [9]u8 = undefined;
    var payload: [64 * 1024]u8 = undefined;
    while (left > 0) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) return error.NoResponseHeaders;
        const n = stream.read(hdr[0..1], .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
        if (n == 0) return error.ConnectionClosed;
        try readExact(stream, hdr[1..]);
        const len = (@as(usize, hdr[0]) << 16) | (@as(usize, hdr[1]) << 8) | hdr[2];
        const frame_sid: u31 = @intCast(std.mem.readInt(u32, hdr[5..9], .big) & 0x7fff_ffff);
        std.debug.assert(len <= payload.len);
        try readExact(stream, payload[0..len]);
        if (hdr[3] == 0x7) return error.GoAway;
        if (hdr[3] != 0x1) continue;
        for (sids, 0..) |sid, i| {
            if (sid == frame_sid and !seen[i]) {
                seen[i] = true;
                left -= 1;
            }
        }
    }
}

/// The scheduler remembers each stream whose response HEADERS went out
/// until that stream ends, and a reset is an end. One connection opens a
/// full set of streams, gets their HEADERS, resets them all, and does it
/// again. If a reset did not end that state, the set would be full on the
/// second round and the connection would fail (t-3514).
fn runResetsEndHeadersState(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangSse } } },
        .{ .method = .GET, .path = "/hello", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 4;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
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

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    defer stream.close();

    var wire = try h2c.buildClientPrefaceAndSettings(gpa);
    defer wire.deinit(gpa);
    var next: u31 = 1;
    var round: usize = 0;
    while (round < 3) : (round += 1) {
        var sids: [4]u31 = undefined;
        for (&sids) |*sid| {
            sid.* = next;
            next += 2;
            try h2c.appendHeaders(gpa, &wire, sid.*, "/sse", true);
        }
        try writeAllStreamAt(stream, wire.items, @src());
        wire.clearRetainingCapacity();
        awaitResponseHeaders(stream, &sids, 5_000) catch |err| {
            std.debug.print("resets end HEADERS state: round {d}: {s}\n", .{ round, @errorName(err) });
            return err;
        };
        for (sids) |sid| try h2c.appendRst(gpa, &wire, sid);
        try writeAllStreamAt(stream, wire.items, @src());
        wire.clearRetainingCapacity();
        try waitAccountingZeroAt(&server, 2_000, @src().line);
    }
    try h2c.appendHeaders(gpa, &wire, next, "/hello", true);
    try writeAllStreamAt(stream, wire.items, @src());
    if (try awaitStreamOutcome(stream, next, 5_000) != .ended) return error.StreamNotAnswered;
}

test "a reset ends the scheduler's HEADERS state for that stream (t-3514)" {
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in resets-end-HEADERS-state test");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();
    var handle = try rt.spawn(runResetsEndHeadersState, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("resets end HEADERS state: {s}\n", .{@errorName(err)});
        return err;
    };
}

var sse_entered: std.atomic.Value(usize) = .init(0);

fn countedSse(ctx: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = sse_entered.fetchAdd(1, .acq_rel);
    return hangSse(ctx, req, resp);
}

/// An SSE start queues HEADERS with the ticket its handler waits on, and the
/// actor drains it later. A peer reset that arrives first must drop that
/// HEADERS unsent and fail its ticket, so the handler wakes and the stream's
/// scheduler state ends; the connection then goes on serving (t-3514).
fn runResetDropsQueuedHeaders(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = countedSse } } },
        .{ .method = .GET, .path = "/hello", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 4;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
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
    const fs = starh2.edge.fair_scheduler;
    const dropped_before = fs.test_dropped_ticketed_headers.load(.acquire);
    const entered_before = sse_entered.load(.acquire);
    c.test_hold_emit.store(true, .release);
    defer c.test_hold_emit.store(false, .release);

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    defer stream.close();
    var wire = try h2c.buildClientPrefaceAndSettings(gpa);
    defer wire.deinit(gpa);
    try h2c.appendHeaders(gpa, &wire, 1, "/sse", true);
    try writeAllStreamAt(stream, wire.items, @src());
    wire.clearRetainingCapacity();

    // The handler runs, queues its HEADERS and parks on their ticket. Its
    // entry is visible; the queueing right after it is not, so allow it a
    // moment. If the reset still wins, nothing is dropped and the check
    // below fails instead of passing without exercising the drop.
    const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% 2 * std.time.ns_per_s;
    while (sse_entered.load(.acquire) == entered_before) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) return error.HandlerNeverRan;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
    zio.sleep(.fromMilliseconds(50)) catch {};
    try h2c.appendRst(gpa, &wire, 1);
    try writeAllStreamAt(stream, wire.items, @src());
    wire.clearRetainingCapacity();
    try waitAccountingZeroAt(&server, 2_000, @src().line);
    if (fs.test_dropped_ticketed_headers.load(.acquire) != dropped_before + 1) return error.QueuedHeadersNotDropped;

    c.test_hold_emit.store(false, .release);
    try h2c.appendHeaders(gpa, &wire, 3, "/hello", true);
    try writeAllStreamAt(stream, wire.items, @src());
    // Stream 1's HEADERS never reaches the wire; stream 3 is answered.
    var hdr: [9]u8 = undefined;
    var payload: [64 * 1024]u8 = undefined;
    const end = zio.Timestamp.now(.monotonic).toNanoseconds() +% 5 * std.time.ns_per_s;
    while (true) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() >= end) return error.StreamNotAnswered;
        const n = stream.read(hdr[0..1], .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
        if (n == 0) return error.ConnectionClosed;
        try readExact(stream, hdr[1..]);
        const len = (@as(usize, hdr[0]) << 16) | (@as(usize, hdr[1]) << 8) | hdr[2];
        const sid: u31 = @intCast(std.mem.readInt(u32, hdr[5..9], .big) & 0x7fff_ffff);
        try readExact(stream, payload[0..len]);
        if (sid == 1 and hdr[3] == 0x1) return error.ResetStreamGotHeaders;
        if (sid == 3 and (hdr[3] == 0x0 or hdr[3] == 0x1) and hdr[4] & 0x1 != 0) break;
    }
}

test "a reset drops a stream's queued HEADERS and fails their ticket (t-3514)" {
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in reset-drops-queued-HEADERS test");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();
    var handle = try rt.spawn(runResetDropsQueuedHeaders, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("reset drops queued HEADERS: {s}\n", .{@errorName(err)});
        return err;
    };
}

/// A stream that gets no request memory is refused, and the connection goes
/// on: Session still decoded its block, so the HPACK table stays in step.
fn runRefusedWithoutRequestMemory(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/complete", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = completeHello } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
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
    c.test_no_request_lease.store(true, .release);
    defer c.test_no_request_lease.store(false, .release);

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    defer stream.close();

    var first = try h2c.buildClientPrefaceAndSettings(gpa);
    defer first.deinit(gpa);
    try h2c.appendHeaders(gpa, &first, 1, "/complete", true);
    try writeAllStreamAt(stream, first.items, @src());
    try std.testing.expectEqual(StreamOutcome.reset, try awaitStreamOutcome(stream, 1, 2_000));

    c.test_no_request_lease.store(false, .release);
    var second: std.ArrayList(u8) = .empty;
    defer second.deinit(gpa);
    try h2c.appendHeaders(gpa, &second, 3, "/complete", true);
    try writeAllStreamAt(stream, second.items, @src());
    try std.testing.expectEqual(StreamOutcome.ended, try awaitStreamOutcome(stream, 3, 2_000));
    try waitAccountingZeroAt(&server, 2_000, @src().line);
}

test "a stream with no request memory is refused and the connection goes on" {
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in no-request-memory test");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();
    var handle = try rt.spawn(runRefusedWithoutRequestMemory, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("no request memory: {s}\n", .{@errorName(err)});
        return err;
    };
}

const CountingDbg = std.heap.DebugAllocator(.{
    .never_unmap = true,
    .retain_metadata = true,
    .safety = true,
    .enable_memory_limit = true,
});

fn liveBytes(dbg: *CountingDbg) usize {
    return @atomicLoad(usize, &dbg.total_requested_bytes, .monotonic);
}

/// Request memory lives as long as the request. A handler's `req.arena` used
/// to be its slot's arena, reset with `.retain_capacity`, so every slot kept
/// its largest request's memory until the connection closed.
fn runRequestMemoryFreedAtFinish(rt: *zio.Runtime, gpa: std.mem.Allocator, dbg: *CountingDbg) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/hello", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
        .{ .method = .GET, .path = "/big-task", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = bigArenaTask } } },
        .{ .method = .GET, .path = "/big-complete", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = bigArenaComplete } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
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

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    defer stream.close();

    var wire = try h2c.buildClientPrefaceAndSettings(gpa);
    defer wire.deinit(gpa);
    try h2c.appendHeaders(gpa, &wire, 1, "/hello", true);
    try writeAllStreamAt(stream, wire.items, @src());
    try std.testing.expectEqual(StreamOutcome.ended, try awaitStreamOutcome(stream, 1, 2_000));
    try waitAccountingZeroAt(&server, 2_000, @src().line);
    // The connection is up and idle: everything it holds for its own life is
    // counted here.
    const baseline = liveBytes(dbg);

    const paths = [_][]const u8{ "/big-task", "/big-complete" };
    for (paths, 0..) |path, k| {
        const sid: u31 = @intCast(3 + 2 * k);
        var req: std.ArrayList(u8) = .empty;
        defer req.deinit(gpa);
        try h2c.appendHeaders(gpa, &req, sid, path, true);
        try writeAllStreamAt(stream, req.items, @src());
        try std.testing.expectEqual(StreamOutcome.ended, try awaitStreamOutcome(stream, sid, 2_000));
        try waitAccountingZeroAt(&server, 2_000, @src().line);
        const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% 2 * std.time.ns_per_s;
        while (liveBytes(dbg) > baseline + big_request_bytes / 4) {
            if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) {
                std.debug.print("{s}: live bytes {d} after the request, {d} before it, with the connection still open\n", .{ path, liveBytes(dbg), baseline });
                return error.RequestMemoryOutlivedRequest;
            }
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
    }
}

test "request memory is freed when its handler finishes, not when the connection closes" {
    var dbg = CountingDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator reported leak/UAF in request-memory test");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();
    var handle = try rt.spawn(runRequestMemoryFreedAtFinish, .{ rt, gpa, &dbg });
    handle.join() catch |err| {
        std.debug.print("request memory: {s}\n", .{@errorName(err)});
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
        starh2.edge.connection.test_waiting_for_space.store(0, .release);
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const wire = try h2c.buildClientHelloWindow(gpa, "/wf", 1, 256, .defaults);
        defer gpa.free(wire);
        try writeAllStreamAt(stream, wire, @src());
        var waited: u64 = 0;
        while (waited < 1000) : (waited += 5) {
            // The 70 KiB body parks in waitForStreamSpace. live_handlers > 0 is
            // admission, not a waiter: a fail before send starts leaves herr=0.
            if (starh2.edge.connection.test_waiting_for_space.load(.acquire) != 0) break;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        try std.testing.expect(starh2.edge.connection.test_waiting_for_space.load(.acquire) != 0);
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
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator leak/UAF after space-wait cancel");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 8, .prewarm = 8 },
        .executors = .exact(2),
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
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator leak after write-fail stress");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
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
    var streams: [8]?zio.net.Stream = @splat(null);
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
    var dbg = LifecycleDbg.init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator leak after global cap storm");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 64 },
        .executors = .exact(2),
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
        .inbound_wire_chunks_per_connection = starh2.core.limits.H2C_MIN_WRITE_QUEUE_CHUNKS,
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
    var session = try session_mod.Session.init(std.testing.allocator, std.testing.allocator, .defaults);
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
    var session = try session_mod.Session.init(std.testing.allocator, std.testing.allocator, .defaults);
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

fn drainSessionIntents(gpa: std.mem.Allocator, session: *starh2.core.session.Session) void {
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

test "rates: completed HEADERS do not trip ENHANCE_YOUR_CALM" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    const gpa = std.testing.allocator;
    var session = try session_mod.Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    drainSessionIntents(gpa, &session);
    var rates: starh2.core.rates.RateLimiter = .{};
    session.rate_limiter = &rates;
    session.edge_now_ns = 1;

    try session.ingest(frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try session.ingest(sbuf[0..sn]);
    drainSessionIntents(gpa, &session);

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(gpa, &fields);
    defer gpa.free(block);
    // Past the old 100k HEADERS burst. A lifetime pot would GOAWAY here.
    var i: u31 = 1;
    while (i < 201) : (i += 2) {
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
        drainSessionIntents(gpa, &session);
        try std.testing.expectEqual(.none, session.terminal);
    }
}

test "rates: RST flood trips ENHANCE_YOUR_CALM" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    const gpa = std.testing.allocator;
    var session = try session_mod.Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    drainSessionIntents(gpa, &session);
    var rates: starh2.core.rates.RateLimiter = .{};
    rates.rst = .init(2, 0);
    session.rate_limiter = &rates;
    session.edge_now_ns = 1;

    try session.ingest(frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try session.ingest(sbuf[0..sn]);
    drainSessionIntents(gpa, &session);

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
            .flags = .{ .end_headers = true },
            .stream_id = i,
        };
        fh.encode(&hdr_buf);
        var wire: [64]u8 = undefined;
        @memcpy(wire[0..hdr_buf.len], &hdr_buf);
        @memcpy(wire[hdr_buf.len..][0..block.len], block);
        try session.ingest(wire[0 .. hdr_buf.len + block.len]);
        drainSessionIntents(gpa, &session);
        var rst: [13]u8 = undefined;
        const rn = try frame.Serializer.rstStream(&rst, i, .cancel);
        try session.ingest(rst[0..rn]);
        drainSessionIntents(gpa, &session);
        if (session.terminal != .none) break;
    }
    try std.testing.expect(session.terminal != .none);
    switch (session.terminal) {
        .goaway => |g| try std.testing.expectEqual(frame.ErrorCode.enhance_your_calm, g.code),
        else => return error.ExpectedGoaway,
    }
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
    var dbg = LifecycleDbg.init;
    defer {
        if (dbg.deinit() != .ok) @panic("DebugAllocator leak after large-body gate");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
    });
    defer rt.deinit();
    var h = try rt.spawn(runLargeBodyWindowGate, .{ rt, gpa, @as(u31, 1) });
    try h.join();
}

test "lifecycle: >64KiB body under small window + RST (sparse stream id)" {
    var dbg = LifecycleDbg.init;
    defer {
        if (dbg.deinit() != .ok) @panic("DebugAllocator leak after sparse large-body gate");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
    });
    defer rt.deinit();
    // Sparse odd client stream id — must use handler-slot space waiter, not (id-1)/2.
    var h = try rt.spawn(runLargeBodyWindowGate, .{ rt, gpa, @as(u31, 1_000_001) });
    try h.join();
}

// One resp.send whose body EXCEEDS outbound_bytes_per_stream. The handler must
// chunk through the cap as the actor drains — never park forever waiting for
// space that only its own drain can create. Static so the task stack stays small.
const cap_crossing_body: [1024 * 1024]u8 = @splat('B');

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
    var dbg = LifecycleDbg.init;
    defer {
        if (dbg.deinit() != .ok) @panic("DebugAllocator leak after cap-crossing gate");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
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
    const checks_before = starh2.edge.connection.test_preface_checks.load(.acquire);
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
    // The first-frame check in `queueWire` ran for every h2c connection.
    const checks = starh2.edge.connection.test_preface_checks.load(.acquire) - checks_before;
    try std.testing.expect(checks >= 8);
}

test "lifecycle: the first wire frame is the server's own SETTINGS, never an ack (t-538)" {
    var dbg = LifecycleDbg.init;
    defer {
        if (dbg.deinit() != .ok) @panic("DebugAllocator leak after preface-first gate");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
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
    var dbg = LifecycleDbg.init;
    defer {
        if (dbg.deinit() != .ok) @panic("DebugAllocator leak after readiness gate");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
    });
    defer rt.deinit();
    var h = try rt.spawn(runListeningReadiness, .{ rt, gpa });
    try h.join();
}
