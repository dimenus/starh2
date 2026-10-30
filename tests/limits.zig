const std = @import("std");
const starh2 = @import("starh2");
const zio = @import("zio");

test "resource upper bound positive" {
    const b = try starh2.Limits.defaults.resourceUpperBound();
    try std.testing.expect(b.allocator_bytes > 1024 * 1024);
    try std.testing.expect(b.virtual_stack_bytes >= b.committed_stack_bytes);
}

test "invalid zero connections" {
    var lim = starh2.Limits.defaults;
    lim.max_connections = 0;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "control capacity must be above zero, and the terminal room is added on top" {
    var lim = starh2.Limits.defaults;
    lim.control_bytes_per_connection = 0;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());

    lim = starh2.Limits.defaults;
    lim.control_entries_per_connection = 0;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());

    // One ordinary entry is a legal config: the terminal frames do not live in it.
    lim = starh2.Limits.defaults;
    lim.control_entries_per_connection = 1;
    lim.control_bytes_per_connection = 1;
    _ = try lim.resourceUpperBound();
    try std.testing.expectEqual(lim.max_streams_per_connection + 3, try lim.terminalFrames());
    try std.testing.expectEqual(lim.control_entries_per_connection + lim.max_streams_per_connection + 3, try lim.controlPoolEntries());
}

test "the connection budget must hold the whole write pool and one stream's cap (t-3538)" {
    // A full budget parks the writer; that is safe only if the wire side alone
    // can never be the whole budget, because the wire side is what an ack
    // frees. The wire side is the write pool, so the budget holds the pool and
    // a stream's cap. One byte short is refused, exactly enough is accepted.
    var lim = starh2.Limits.defaults;
    lim.inbound_wire_chunks_per_connection = starh2.core.limits.H2C_MIN_WRITE_QUEUE_CHUNKS;
    lim.outbound_bytes_per_stream = 64 * 1024;
    const pool_bytes = lim.inbound_wire_chunks_per_connection * starh2.core.limits.WIRE_CHUNK_SIZE;
    const enough = pool_bytes + lim.outbound_bytes_per_stream;
    lim.outbound_bytes_per_connection = enough;
    lim.outbound_bytes_per_server = enough;
    _ = try lim.resourceUpperBound();
    lim.outbound_bytes_per_connection = enough - 1;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
    // The default write pool at the default budget is far inside it.
    _ = try starh2.Limits.defaults.resourceUpperBound();
    // A bigger pool needs a bigger budget.
    lim.outbound_bytes_per_connection = enough;
    lim.inbound_wire_chunks_per_connection += 1;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "bound terms use concrete sizes and capacities" {
    const lim = starh2.Limits.defaults;
    const b = try lim.resourceUpperBound();
    try std.testing.expect(b.terms.slot_boot > 0);
    try std.testing.expect(b.terms.on_demand_conn > 0);
    try std.testing.expect(b.terms.on_demand_server > 0);
    try std.testing.expect(b.terms.reaper_jobs > 0);
    // Every slot object and the pool's two lists are in the bound, not only
    // the boot buffer each slot owns.
    try std.testing.expect(b.terms.conn_slots >= lim.max_connections * @sizeOf(starh2.edge.conn_slot.ConnSlot));
    var fewer = lim;
    fewer.max_connections = lim.max_connections / 2;
    const bf = try fewer.resourceUpperBound();
    try std.testing.expect(bf.terms.conn_slots < b.terms.conn_slots);
    try std.testing.expect(b.terms.routes > 0);
    try std.testing.expect(b.terms.certs > 0);
    try std.testing.expect(b.terms.tls_scratch > 0);
    try std.testing.expect(b.terms.endpoints_listeners > 0);
    try std.testing.expect(b.terms.tasks > 0);
    try std.testing.expect(b.terms.h1_head > 0);
    try std.testing.expect(b.terms.h1_body > 0);

    var no_chunks = lim;
    no_chunks.inbound_wire_chunks_per_connection = starh2.core.limits.MIN_WRITE_POOL_CHUNKS;
    const b2 = try no_chunks.resourceUpperBound();
    try std.testing.expect(b.terms.slot_boot > b2.terms.slot_boot);
    try std.testing.expect(b.allocator_bytes > b2.allocator_bytes);

    var tiny_streams = lim;
    tiny_streams.max_streams_per_connection = 1;
    tiny_streams.max_streams_per_server = 64;
    tiny_streams.cancellation_reaper_jobs = 64;
    const b3 = try tiny_streams.resourceUpperBound();
    try std.testing.expect(b.terms.slot_boot > b3.terms.slot_boot);

    var no_tls = lim;
    no_tls.concurrent_tls_handshakes = 1;
    no_tls.tls_handshake_scratch_bytes = 1024;
    const b4 = try no_tls.resourceUpperBound();
    try std.testing.expect(b.terms.tls_scratch > b4.terms.tls_scratch);

    try std.testing.expectEqual(@sizeOf(starh2.edge.connection.ReaperJob), starh2.core.limits.REAPER_JOB_SIZE);
}

test "counting allocator: Server.init under published ceiling" {
    const lim = starh2.Limits{
        .max_connections = 2,
        .max_streams_per_connection = 4,
        .max_streams_per_server = 8,
        .cancellation_reaper_jobs = 8,
        .cancellation_reaper_tasks = 2,
        .inbound_wire_chunks_per_connection = starh2.core.limits.H2C_MIN_WRITE_QUEUE_CHUNKS,
        .outbound_bytes_per_stream = 4 * 1024,
        // The write pool (3 chunks) and one stream's cap must fit the budget.
        .outbound_bytes_per_connection = 64 * 1024,
        .outbound_bytes_per_server = 128 * 1024,
        .request_bytes_per_connection = 128 * 1024,
        .request_bytes_per_server = 256 * 1024,
        .request_arena_bytes = 256 * 1024,
        .request_pool_retain_bytes = 256 * 1024,
        .request_body_bytes = 1024,
        .control_bytes_per_connection = 8 * 1024,
        .control_entries_per_connection = 32,
        .stream_tombstones = 16,
        .concurrent_tls_handshakes = 1,
        .tls_handshake_scratch_bytes = 4 * 1024,
        .max_endpoints = 1,
        .max_routes = 4,
        .max_route_path_bytes = 1024,
    };
    const ceiling = try lim.resourceUpperBound();

    // FailingAllocator with unreachable fail_index doubles as a counting allocator.
    var counting = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const gpa = counting.allocator();
    const rt = try zio.Runtime.init(std.testing.allocator, .{
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(1),
    });
    defer rt.deinit();

    const dummy: u8 = 0;
    const routes = [_]starh2.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = struct {
            fn f(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
                try resp.send(200, &.{}, "ok");
            }
        }.f } },
    }};
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = lim,
    });
    defer server.deinit(gpa);

    try std.testing.expect(counting.allocated_bytes <= ceiling.allocator_bytes);
    try std.testing.expect(counting.allocated_bytes > 0);
}

test "fail-index iteration: Server.init fails closed" {
    const lim = starh2.Limits{
        .max_connections = 1,
        .max_streams_per_connection = 2,
        .max_streams_per_server = 4,
        .cancellation_reaper_jobs = 4,
        .cancellation_reaper_tasks = 1,
        .inbound_wire_chunks_per_connection = starh2.core.limits.H2C_MIN_WRITE_QUEUE_CHUNKS,
        .outbound_bytes_per_stream = 1024,
        .outbound_bytes_per_connection = 64 * 1024,
        .outbound_bytes_per_server = 128 * 1024,
        .request_bytes_per_connection = 128 * 1024,
        .request_bytes_per_server = 256 * 1024,
        .request_arena_bytes = 256 * 1024,
        .request_pool_retain_bytes = 256 * 1024,
        .request_body_bytes = 1024,
        .control_bytes_per_connection = 8 * 1024,
        .control_entries_per_connection = 32,
        .stream_tombstones = 4,
        .concurrent_tls_handshakes = 1,
        .tls_handshake_scratch_bytes = 1024,
        .max_endpoints = 1,
        .max_routes = 2,
        .max_route_path_bytes = 256,
    };
    var saw_fail = false;
    var fail_i: usize = 0;
    while (fail_i < 64) : (fail_i += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_i });
        const gpa = failing.allocator();
        const rt = zio.Runtime.init(gpa, .{
            .stack_pool = .{
                .maximum_size = 1024 * 1024,
                .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 8, .prewarm = 8 },
            .executors = .exact(1),
        }) catch {
            saw_fail = true;
            continue;
        };
        defer rt.deinit();
        const dummy: u8 = 0;
        const routes = [_]starh2.Route{.{
            .method = .GET,
            .path = "/",
            .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = struct {
                fn f(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
                    try resp.send(200, &.{}, "ok");
                }
            }.f } },
        }};
        const addr = starh2.EndpointAddress.parseIp4("127.0.0.1", 0) catch {
            saw_fail = true;
            continue;
        };
        var server = starh2.Server.init(gpa, rt.io(), .{
            .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
            .routes = &routes,
            .tls = null,
            .limits = lim,
        }) catch {
            saw_fail = true;
            continue;
        };
        server.deinit(gpa);
        break;
    }
    try std.testing.expect(saw_fail);
}

test "counting allocator peak under live hello stays under ceiling" {
    const lim = starh2.Limits{
        .max_connections = 2,
        .max_streams_per_connection = 4,
        .max_streams_per_server = 8,
        .cancellation_reaper_jobs = 8,
        .cancellation_reaper_tasks = 2,
        .inbound_wire_chunks_per_connection = starh2.core.limits.H2C_MIN_WRITE_QUEUE_CHUNKS,
        .outbound_bytes_per_stream = 4 * 1024,
        // The write pool (3 chunks) and one stream's cap must fit the budget.
        .outbound_bytes_per_connection = 64 * 1024,
        .outbound_bytes_per_server = 128 * 1024,
        .request_bytes_per_connection = 128 * 1024,
        .request_bytes_per_server = 256 * 1024,
        .request_arena_bytes = 256 * 1024,
        .request_pool_retain_bytes = 256 * 1024,
        .request_body_bytes = 1024,
        .control_bytes_per_connection = 8 * 1024,
        .control_entries_per_connection = 32,
        .stream_tombstones = 16,
        .concurrent_tls_handshakes = 1,
        .tls_handshake_scratch_bytes = 4 * 1024,
        .max_endpoints = 1,
        .max_routes = 4,
        .max_route_path_bytes = 1024,
        .graceful_drain_timeout_ns = 200 * std.time.ns_per_ms,
        .preface_timeout_ns = 500 * std.time.ns_per_ms,
    };
    const ceiling = try lim.resourceUpperBound();

    // Server allocations are counted; runtime/client allocations stay separate.
    var counting = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
    const gpa = counting.allocator();
    const rt = try zio.Runtime.init(std.testing.allocator, .{
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        // FailingAllocator is not thread-safe; serialize the counting path.
        .executors = .exact(1),
    });
    defer rt.deinit();

    const dummy: u8 = 0;
    const routes = [_]starh2.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = struct {
            fn f(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
                try resp.send(200, &.{}, "ok");
            }
        }.f } },
    }};
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = lim,
    });
    var server_live = true;
    defer if (server_live) server.deinit(gpa);

    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serve_live = true;
    defer if (serve_live) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    zio.sleep(.fromMilliseconds(30)) catch {};
    const port = server.localAddress(0).getPort();

    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    const client_gpa = std.testing.allocator;
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(client_gpa);
    try wire.appendSlice(client_gpa, frame.CLIENT_PREFACE);
    {
        var sbuf: [64]u8 = undefined;
        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 64 * 1024 }};
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
        try wire.appendSlice(client_gpa, sbuf[0..sn]);
    }
    {
        const fields = [_]hpack.HeaderField{
            .{ .name = ":method", .value = "GET" },
            .{ .name = ":scheme", .value = "http" },
            .{ .name = ":path", .value = "/" },
            .{ .name = ":authority", .value = "localhost" },
        };
        const block = try hpack.Encoder.encode(client_gpa, &fields);
        defer client_gpa.free(block);
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = true },
            .stream_id = 1,
        };
        fh.encode(&hdr_buf);
        try wire.appendSlice(client_gpa, &hdr_buf);
        try wire.appendSlice(client_gpa, block);
    }
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        var off: usize = 0;
        while (off < wire.items.len) {
            off += try stream.write(wire.items[off..], .none);
        }
        var response: [4096]u8 = undefined;
        var response_ok = false;
        var reads: usize = 0;
        while (reads < 20) : (reads += 1) {
            const n = stream.read(&response, .{ .duration = .fromMilliseconds(100) }) catch |err| switch (err) {
                error.Timeout => continue,
                else => break,
            };
            if (n == 0) break;
            if (std.mem.indexOf(u8, response[0..n], "ok") != null) {
                response_ok = true;
                break;
            }
        }
        try std.testing.expect(response_ok);
    }

    server.requestShutdown();
    try serve_handle.join();
    serve_live = false;
    server.deinit(gpa);
    server_live = false;
    const peak = counting.allocated_bytes;
    try std.testing.expect(peak <= ceiling.allocator_bytes);
    try std.testing.expect(peak > 0);
}

// Raised body-cap cost card (t-480), plus the H1 reservation.
//
// Which caps a consumer may raise, and what each costs in resourceUpperBound:
// - `request_body_bytes` — 413 threshold, and H1 per-connection body_buf plus
//   carry suffix. A new value moves `terms.h1_body` and `allocator_bytes`.
//   An H2 body is buffered in the request's arena, so the arena cap
//   (`request_arena_bytes`) must be large enough for it, which boot checks
//   (`Limits.bodyArenaBytes`).
// - `request_bytes_per_server` — the request pool: the most RESIDENT bytes it
//   holds, counted once in `terms.request_pool`. It must be at least the
//   arena cap. Raising it is what moves the bound.
// - `request_bytes_per_connection` — one connection's share of that pool. It
//   costs nothing in the bound, because the pool is already counted once.
// - `max_streams_per_connection` — multiplies handler/ticket/map terms.
test "raised request_body_bytes: publish concurrent body memory cost" {
    const B: usize = 2 * 1024 * 1024; // 2 MiB — qmdsync-class body
    var lim = starh2.Limits.defaults;
    const base = try lim.resourceUpperBound();

    lim.request_body_bytes = B;
    // The default arena (4 MiB) cannot buffer a 2 MiB body: the last growth
    // step of its list holds the first block and an old and a new block, 12 MiB.
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
    lim.request_arena_bytes = try lim.bodyArenaBytes();
    try std.testing.expectEqual(@as(usize, 16 * 1024 * 1024), lim.request_arena_bytes);
    const body_only = try lim.resourceUpperBound();
    try std.testing.expect(body_only.terms.h1_body > base.terms.h1_body);
    try std.testing.expect(body_only.allocator_bytes > base.allocator_bytes);
    // The pool is unchanged: the same resident cap.
    try std.testing.expectEqual(base.terms.request_pool, body_only.terms.request_pool);

    // A larger per-connection share is free: it is a slice of the pool.
    const S = lim.max_streams_per_connection;
    lim.request_bytes_per_connection = B * S; // 512 MiB
    lim.request_bytes_per_server = @max(lim.request_bytes_per_server, lim.request_bytes_per_connection);
    const full = try lim.resourceUpperBound();
    // Only the pool term moved, by exactly the cap that was raised.
    try std.testing.expectEqual(
        lim.request_bytes_per_server + try lim.requestPoolConfig().bitmapBytes(),
        full.terms.request_pool,
    );
    try std.testing.expect(full.terms.request_pool > body_only.terms.request_pool);
    var share_only = lim;
    share_only.request_bytes_per_connection = lim.request_bytes_per_connection / 2;
    const half = try share_only.resourceUpperBound();
    try std.testing.expectEqual(full.allocator_bytes, half.allocator_bytes);
}

test "h2c: a write queue smaller than one pop's reservation is rejected at boot (t-3520)" {
    // One pop pushes up to `pushes_per_pop` (2) chunks and the drain's tail one
    // more: 3. The h2c write queue is `inbound_wire_chunks_per_connection` slots
    // with no extra, so a smaller queue never has room for a pop.
    const min_slots = starh2.core.limits.H2C_MIN_WRITE_QUEUE_CHUNKS;
    try std.testing.expectEqual(@as(usize, 3), min_slots);
    const gpa = std.testing.allocator;
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var lim = starh2.Limits.defaults;

    lim.inbound_wire_chunks_per_connection = min_slots - 1;
    try std.testing.expectError(error.InvalidConfig, starh2.Server.init(gpa, std.testing.io, .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &.{},
        .tls = null,
        .limits = lim,
    }));

    lim.inbound_wire_chunks_per_connection = min_slots;
    var server = try starh2.Server.init(gpa, std.testing.io, .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &.{},
        .tls = null,
        .limits = lim,
    });
    server.deinit(gpa);
}

test "frame pool: settings that cannot hold the preface and a terminal frame are InvalidConfig, the smallest that can are accepted" {
    const frame = starh2.core.frame;
    var lim = starh2.Limits.defaults;
    // The biggest control frame is the preface SETTINGS, 45 bytes: judged by
    // the encoded frame, not by the 9-byte header the old check used.
    try std.testing.expectEqual(@as(usize, 45), frame.max_control_frame_bytes);

    lim.frame_slab_bytes = frame.FRAME_HEADER_LEN;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
    lim.frame_slab_bytes = frame.window_update_frame_bytes;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
    lim.frame_slab_bytes = frame.max_control_frame_bytes - 1;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
    lim.frame_slab_bytes = frame.max_control_frame_bytes;
    _ = try lim.resourceUpperBound();

    // Terminal frames do not use the frame pool; it only has to hold the two
    // preface frames.
    for ([_]usize{ 0, 1 }) |slabs| {
        lim.frame_slabs_per_connection = slabs;
        try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
    }
    lim.frame_slabs_per_connection = 2;
    _ = try lim.resourceUpperBound();
}
