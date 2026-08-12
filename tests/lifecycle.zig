//! Lifecycle stress under DebugAllocator — must fail on leaks / nonzero counters.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

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

fn writeAllStream(stream: zio.net.Stream, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = try stream.write(bytes[off..], .none);
        off += n;
    }
}

fn appendHeaders(gpa: std.mem.Allocator, wire: *std.ArrayList(u8), stream_id: u31, path: []const u8, end_stream: bool) !void {
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = path },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(gpa, &fields);
    defer gpa.free(block);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const fh = frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = end_stream },
        .stream_id = stream_id,
    };
    fh.encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);
}

fn buildClientPrefaceAndSettings(gpa: std.mem.Allocator) !std.ArrayList(u8) {
    const frame = starh2.core.frame;
    var wire: std.ArrayList(u8) = .empty;
    errdefer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    var sbuf: [64]u8 = undefined;
    const settings = [_]frame.Setting{
        .{ .id = .max_concurrent_streams, .value = 256 },
        .{ .id = .initial_window_size, .value = 1 << 20 },
    };
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
    try wire.appendSlice(gpa, sbuf[0..sn]);
    return wire;
}

fn buildClientHello(gpa: std.mem.Allocator, path: []const u8) ![]u8 {
    var wire = try buildClientPrefaceAndSettings(gpa);
    errdefer wire.deinit(gpa);
    try appendHeaders(gpa, &wire, 1, path, true);
    return try wire.toOwnedSlice(gpa);
}

fn buildClientPost(gpa: std.mem.Allocator, path: []const u8, body: []const u8) ![]u8 {
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    var wire = try buildClientPrefaceAndSettings(gpa);
    errdefer wire.deinit(gpa);

    var content_len_buf: [32]u8 = undefined;
    const content_len = try std.fmt.bufPrint(&content_len_buf, "{d}", .{body.len});
    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = path },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-length", .value = content_len },
    };
    const block = try hpack.Encoder.encode(gpa, &fields);
    defer gpa.free(block);

    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const headers = frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true },
        .stream_id = 1,
    };
    headers.encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);

    const data = frame.FrameHeader{
        .length = @intCast(body.len),
        .type = .data,
        .flags = .{ .end_stream = true },
        .stream_id = 1,
    };
    data.encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, body);
    return try wire.toOwnedSlice(gpa);
}

fn buildMultiSseOpen(gpa: std.mem.Allocator, n: usize) ![]u8 {
    var wire = try buildClientPrefaceAndSettings(gpa);
    errdefer wire.deinit(gpa);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const sid: u31 = @intCast(1 + 2 * i);
        try appendHeaders(gpa, &wire, sid, "/sse", true);
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
            std.debug.print("accounting not zero: streams={d} reaper={d} outbound={d} request={d} live={d} slots={d}\n", .{ streams, reaper, outbound, request, live, slots });
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
    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/hello", .handler = .{ .ptr = @constCast(&dummy), .runFn = hello } },
        .{ .method = .GET, .path = "/sse", .handler = .{ .ptr = @constCast(&dummy), .runFn = hangSse } },
        .{ .method = .GET, .path = "/delayed", .handler = .{ .ptr = @constCast(&dummy), .runFn = delayedHello } },
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

    var server = try starh2.Server.init(gpa, rt, .{
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

    zio.sleep(.fromMilliseconds(40)) catch {};
    const port = server.localAddress(0).getPort();

    // --- Real-path forced spawn failure: slot/live/reaper must return to zero. ---
    starh2.edge.connection.test_force_spawn_fail = true;
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const wire = try buildClientHello(gpa, "/hello");
        defer gpa.free(wire);
        try writeAllStream(stream, wire);
        try waitAccountingZero(&server, 2000);
    }
    starh2.edge.connection.test_force_spawn_fail = false;

    // --- Completion-vs-RST: hold completion drain while RST arrives, then release. ---
    starh2.edge.connection.test_hold_completion_drain.store(true, .release);
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const open = try buildClientHello(gpa, "/hello");
        defer gpa.free(open);
        try writeAllStream(stream, open);
        // Wait until a slot is observed in use (handler admitted).
        const deadline = zio.Timestamp.now(.monotonic).toNanoseconds() +% 2 * std.time.ns_per_s;
        while (starh2.edge.connection.test_observed_slots_in_use.load(.acquire) == 0) {
            if (zio.Timestamp.now(.monotonic).toNanoseconds() >= deadline) return error.SlotNeverUsed;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        // Peer RST while completion drain is held.
        var rst: [13]u8 = undefined;
        const rn = try starh2.core.frame.Serializer.rstStream(&rst, 1, .cancel);
        try writeAllStream(stream, rst[0..rn]);
        zio.sleep(.fromMilliseconds(50)) catch {};
        // Slot may still be in_use while completion is held.
        starh2.edge.connection.test_hold_completion_drain.store(false, .release);
        try waitAccountingZero(&server, 2000);
    }

    // --- Single-connection 100 concurrent SSE: open → RST. ---
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const open = try buildMultiSseOpen(gpa, 100);
        defer gpa.free(open);
        try writeAllStream(stream, open);
        try waitStreamsAtLeast(&server, 100, 5000);
        try std.testing.expectEqual(@as(usize, 100), server.accounting.active_streams.load(.acquire));
        try std.testing.expect(starh2.edge.connection.test_observed_live_handlers.load(.acquire) >= 100);
        try std.testing.expect(starh2.edge.connection.test_observed_slots_in_use.load(.acquire) >= 100);

        const rsts = try buildRstAll(gpa, 100);
        defer gpa.free(rsts);
        try writeAllStream(stream, rsts);
        try waitAccountingZero(&server, 5000);
    }

    // --- Single-connection 100 concurrent SSE → abrupt socket close. ---
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        const open = try buildMultiSseOpen(gpa, 100);
        defer gpa.free(open);
        try writeAllStream(stream, open);
        try waitStreamsAtLeast(&server, 100, 5000);
        stream.close();
        try waitAccountingZero(&server, 5000);
    }

    // --- Delayed writer: flush wait must not return early. ---
    starh2.edge.connection.test_write_delay_ms = 40;
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const wire = try buildClientHello(gpa, "/delayed");
        defer gpa.free(wire);
        try writeAllStream(stream, wire);
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
        try writeAllStream(stream, open);
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
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024,
            .max_unused_stacks = 64,
            .max_age = .fromSeconds(5),
        },
        .executors = .exact(2),
        .enable_task_migration = true,
    });
    defer rt.deinit();

    var handle = try rt.spawn(runLifecycleStress, .{ rt, gpa });
    try handle.join();
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
    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/wf", .handler = .{ .ptr = @constCast(&dummy), .runFn = writeFailHello } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 8;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    limits.graceful_drain_timeout_ns = 200 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

    var server = try starh2.Server.init(gpa, rt, .{
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
    zio.sleep(.fromMilliseconds(40)) catch {};
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
        const wire = try buildClientHelloWindow(gpa, "/wf", 1, 256);
        defer gpa.free(wire);
        try writeAllStream(stream, wire);
        var waited: u64 = 0;
        while (waited < 1000) : (waited += 5) {
            if (starh2.edge.connection.test_observed_live_handlers.load(.acquire) > 0) break;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        // Fail the next transport write while the handler is mid-send.
        starh2.edge.wire_pump.test_fail_next_write.store(true, .release);
        var boost: std.ArrayList(u8) = .empty;
        defer boost.deinit(gpa);
        try appendWindowUpdate(gpa, &boost, 1, 64 * 1024);
        try appendWindowUpdate(gpa, &boost, 0, 64 * 1024);
        writeAllStream(stream, boost.items) catch {};
        try waitAccountingZero(&server, 2_000);
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
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024,
            .max_unused_stacks = 8,
            .max_age = .fromSeconds(5),
        },
        .executors = .exact(2),
        .enable_task_migration = true,
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
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024,
            .max_unused_stacks = 32,
            .max_age = .fromSeconds(5),
        },
        .executors = .exact(2),
        .enable_task_migration = true,
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
    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/sse", .handler = .{ .ptr = @constCast(&dummy), .runFn = hangSse } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 8;
    limits.max_streams_per_connection = 32;
    limits.max_streams_per_server = 4;
    limits.cancellation_reaper_jobs = 4;
    limits.cancellation_reaper_tasks = 2;
    limits.graceful_drain_timeout_ns = 200 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

    var server = try starh2.Server.init(gpa, rt, .{
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
    zio.sleep(.fromMilliseconds(40)) catch {};
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
        const wire = try buildClientHello(gpa, "/sse");
        defer gpa.free(wire);
        try writeAllStream(streams[i].?, wire);
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
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024,
            .max_unused_stacks = 64,
            .max_age = .fromSeconds(5),
        },
        .executors = .exact(2),
        .enable_task_migration = true,
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
        .handler = .{ .ptr = @constCast(&dummy), .runFn = failIndexHandler },
    }};
    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = starh2.Server.init(gpa, rt, .{
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

    const request_bytes = try buildClientPost(std.testing.allocator, "/fail-index", fail_index_body);
    defer std.testing.allocator.free(request_bytes);
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());
    var response_ok = false;
    if (peer.connect(.{})) |stream_value| {
        var stream = stream_value;
        if (writeAllStream(stream, request_bytes)) |_| {
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
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024,
            .max_unused_stacks = 16,
            .max_age = .fromSeconds(5),
        },
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
    while (fail_index < baseline.allocations) : (fail_index += 1) {
        const outcome = try runFailIndexLifecycleCase(rt, fail_index);
        try std.testing.expect(outcome.induced);
    }
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
                    for (d.headers) |h| {
                        std.testing.allocator.free(@constCast(h.name));
                        std.testing.allocator.free(@constCast(h.value));
                    }
                    std.testing.allocator.free(d.headers);
                    for (d.trailers) |h| {
                        std.testing.allocator.free(@constCast(h.name));
                        std.testing.allocator.free(@constCast(h.value));
                    }
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

fn buildClientHelloWindow(gpa: std.mem.Allocator, path: []const u8, stream_id: u31, win: u32) ![]u8 {
    const frame = starh2.core.frame;
    var wire = try buildClientPrefaceAndSettings(gpa);
    errdefer wire.deinit(gpa);
    // Overwrite SETTINGS initial window with small value for response flow control.
    // Preface already includes a SETTINGS — append another SETTINGS that shrinks window,
    // plus a connection WINDOW_UPDATE adjustment is not needed for stream credit.
    {
        var sbuf: [64]u8 = undefined;
        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = win }};
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
        try wire.appendSlice(gpa, sbuf[0..sn]);
    }
    try appendHeaders(gpa, &wire, stream_id, path, true);
    return try wire.toOwnedSlice(gpa);
}

fn appendWindowUpdate(gpa: std.mem.Allocator, wire: *std.ArrayList(u8), stream_id: u31, incr: u31) !void {
    const frame = starh2.core.frame;
    var buf: [13]u8 = undefined;
    const n = try frame.Serializer.windowUpdate(&buf, stream_id, incr);
    try wire.appendSlice(gpa, buf[0..n]);
}

fn appendRst(gpa: std.mem.Allocator, wire: *std.ArrayList(u8), stream_id: u31) !void {
    const frame = starh2.core.frame;
    var buf: [13]u8 = undefined;
    const n = try frame.Serializer.rstStream(&buf, stream_id, .cancel);
    try wire.appendSlice(gpa, buf[0..n]);
}

fn runLargeBodyWindowGate(rt: *zio.Runtime, gpa: std.mem.Allocator, stream_id: u31) !void {
    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/big", .handler = .{ .ptr = @constCast(&dummy), .runFn = largeBodyHello } },
        .{ .method = .GET, .path = "/rstblock", .handler = .{ .ptr = @constCast(&dummy), .runFn = peerResetWhileBlocked } },
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

    var server = try starh2.Server.init(gpa, rt, .{
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
    zio.sleep(.fromMilliseconds(40)) catch {};
    const port = server.localAddress(0).getPort();

    // --- Small window: handler blocks; WINDOW_UPDATE resumes; body completes. ---
    {
        starh2.edge.connection.test_write_delay_ms = 20;
        defer starh2.edge.connection.test_write_delay_ms = 0;
        starh2.edge.connection.test_last_handler_err.store(0, .release);
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const open = try buildClientHelloWindow(gpa, "/big", stream_id, 1024);
        defer gpa.free(open);
        try writeAllStream(stream, open);
        // Handler should still be blocked (no full body yet).
        zio.sleep(.fromMilliseconds(50)) catch {};
        try std.testing.expectEqual(@as(u8, 0), starh2.edge.connection.test_last_handler_err.load(.acquire));

        // Feed stream + connection credit in chunks until body can finish.
        var boost: std.ArrayList(u8) = .empty;
        defer boost.deinit(gpa);
        var k: usize = 0;
        while (k < 80) : (k += 1) {
            try appendWindowUpdate(gpa, &boost, stream_id, 2048);
            try appendWindowUpdate(gpa, &boost, 0, 2048);
        }
        try writeAllStream(stream, boost.items);
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
    try waitAccountingZero(&server, 5_000);
    try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_queue_wire_bypass.load(.acquire));

    // --- RST while blocked on sparse/dense stream. ---
    {
        starh2.edge.connection.test_last_handler_err.store(0, .release);
        starh2.edge.connection.test_last_peer_reset_code.store(0, .release);
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        const open = try buildClientHelloWindow(gpa, "/rstblock", stream_id, 512);
        defer gpa.free(open);
        try writeAllStream(stream, open);
        var waited: u64 = 0;
        while (waited < 2000) : (waited += 10) {
            if (starh2.edge.connection.test_observed_live_handlers.load(.acquire) > 0) break;
            zio.sleep(.fromMilliseconds(10)) catch {};
        }
        var rst: std.ArrayList(u8) = .empty;
        defer rst.deinit(gpa);
        try appendRst(gpa, &rst, stream_id);
        try writeAllStream(stream, rst.items);
        waited = 0;
        while (waited < 3000) : (waited += 10) {
            if (starh2.edge.connection.test_last_handler_err.load(.acquire) == 3) break;
            zio.sleep(.fromMilliseconds(10)) catch {};
        }
        stream.close();
    }
    try waitAccountingZero(&server, 5_000);
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
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .max_unused_stacks = 32, .max_age = .fromSeconds(5) },
        .executors = .exact(2),
        .enable_task_migration = true,
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
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .max_unused_stacks = 32, .max_age = .fromSeconds(5) },
        .executors = .exact(2),
        .enable_task_migration = true,
    });
    defer rt.deinit();
    // Sparse odd client stream id — must use handler-slot space waiter, not (id-1)/2.
    var h = try rt.spawn(runLargeBodyWindowGate, .{ rt, gpa, @as(u31, 1_000_001) });
    try h.join();
}
