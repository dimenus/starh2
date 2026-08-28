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

test "control capacity must leave ordinary space above terminal reserve" {
    var lim = starh2.Limits.defaults;
    lim.control_bytes_per_connection = starh2.core.limits.TERMINAL_CONTROL_RESERVE_BYTES;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());

    lim = starh2.Limits.defaults;
    lim.control_entries_per_connection = starh2.core.limits.TERMINAL_CONTROL_RESERVE_ENTRIES;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "tls stream buffers cover one connection's Io scratch" {
    var lim = starh2.Limits.defaults;
    lim.tls_stream_bytes = starh2.core.limits.TLS_CONN_BUFFER_BYTES - 1;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "bound terms use concrete sizes and capacities" {
    const lim = starh2.Limits.defaults;
    const b = try lim.resourceUpperBound();
    try std.testing.expect(b.terms.handlers > 0);
    try std.testing.expect(b.terms.joins > 0);
    try std.testing.expect(b.terms.tickets > 0);
    try std.testing.expect(b.terms.write_acks > 0);
    try std.testing.expect(b.terms.wire_descs > 0);
    try std.testing.expect(b.terms.read_payload > 0);
    try std.testing.expect(b.terms.stream_maps > 0);
    try std.testing.expect(b.terms.pending_maps > 0);
    try std.testing.expect(b.terms.on_demand_conn > 0);
    try std.testing.expect(b.terms.on_demand_server > 0);
    try std.testing.expect(b.terms.reaper_jobs > 0);
    try std.testing.expectEqual(@as(usize, 0), b.terms.conn_slots);
    try std.testing.expect(b.terms.routes > 0);
    try std.testing.expect(b.terms.certs > 0);
    try std.testing.expect(b.terms.tls_scratch > 0);
    try std.testing.expect(b.terms.tls_cipher_payload > 0);
    try std.testing.expect(b.terms.endpoints_listeners > 0);
    try std.testing.expect(b.terms.tasks > 0);
    try std.testing.expect(b.terms.h1_head > 0);
    try std.testing.expect(b.terms.h1_body > 0);

    var no_chunks = lim;
    no_chunks.inbound_wire_chunks_per_connection = 1;
    const b2 = try no_chunks.resourceUpperBound();
    try std.testing.expect(b.terms.read_payload > b2.terms.read_payload);
    try std.testing.expect(b.allocator_bytes > b2.allocator_bytes);

    var tiny_streams = lim;
    tiny_streams.max_streams_per_connection = 1;
    tiny_streams.max_streams_per_server = 64;
    tiny_streams.cancellation_reaper_jobs = 64;
    const b3 = try tiny_streams.resourceUpperBound();
    try std.testing.expect(b.terms.handlers > b3.terms.handlers);
    try std.testing.expect(b.terms.stream_maps > b3.terms.stream_maps);

    var no_tls = lim;
    no_tls.concurrent_tls_handshakes = 1;
    no_tls.tls_handshake_scratch_bytes = 1024;
    const b4 = try no_tls.resourceUpperBound();
    try std.testing.expect(b.terms.tls_scratch > b4.terms.tls_scratch);

    try std.testing.expectEqual(@sizeOf(starh2.edge.connection.HandlerSlot), starh2.core.limits.HANDLER_SLOT_SIZE);
    try std.testing.expectEqual(@sizeOf(starh2.edge.connection.ReaperJob), starh2.core.limits.REAPER_JOB_SIZE);
    try std.testing.expectEqual(@sizeOf(starh2.edge.wire_pump.WriteCompletion), starh2.core.bound_shapes.WRITE_COMPLETION_SIZE);
    try std.testing.expectEqual(@sizeOf(starh2.edge.wire_pump.WireChunk), starh2.core.bound_shapes.WIRE_CHUNK_DESC_SIZE);
}

test "counting allocator: Server.init under published ceiling" {
    const lim = starh2.Limits{
        .max_connections = 2,
        .max_streams_per_connection = 4,
        .max_streams_per_server = 8,
        .cancellation_reaper_jobs = 8,
        .cancellation_reaper_tasks = 2,
        .inbound_wire_chunks_per_connection = 2,
        .outbound_bytes_per_stream = 4 * 1024,
        .outbound_bytes_per_connection = 16 * 1024,
        .outbound_bytes_per_server = 64 * 1024,
        .request_bytes_per_connection = 16 * 1024,
        .request_bytes_per_server = 64 * 1024,
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
        .inbound_wire_chunks_per_connection = 1,
        .outbound_bytes_per_stream = 1024,
        .outbound_bytes_per_connection = 4 * 1024,
        .outbound_bytes_per_server = 8 * 1024,
        .request_bytes_per_connection = 4 * 1024,
        .request_bytes_per_server = 8 * 1024,
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
        .inbound_wire_chunks_per_connection = 2,
        .outbound_bytes_per_stream = 4 * 1024,
        .outbound_bytes_per_connection = 16 * 1024,
        .outbound_bytes_per_server = 64 * 1024,
        .request_bytes_per_connection = 16 * 1024,
        .request_bytes_per_server = 64 * 1024,
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
        .enable_task_migration = false,
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

test "Intent and ControlEntry sizes feed resourceUpperBound" {
    try std.testing.expect(starh2.core.bound_shapes.INTENT_SIZE >= 16);
    try std.testing.expect(starh2.core.bound_shapes.CONTROL_ENTRY_SIZE >= 16);
    try std.testing.expectEqual(@sizeOf(starh2.edge.fair_scheduler.ControlEntry), starh2.core.bound_shapes.CONTROL_ENTRY_SIZE);
}

// Raised body-cap cost card (t-480), plus the H1 reservation.
//
// Which caps a consumer may raise, and what each costs in resourceUpperBound:
// - `request_body_bytes` — 413 threshold, and H1 per-connection body_buf plus
//   carry suffix. A new value moves `terms.h1_body` and `allocator_bytes`.
// - `request_bytes_per_connection` / `request_bytes_per_server` — H2 concurrent
//   body residency. To admit S streams each holding a full body of size B: set
//   `request_bytes_per_connection >= B*S` and keep `request_bytes_per_server`
//   at least that connection ceiling (else InvalidConfig).
// - `max_streams_per_connection` — multiplies handler/ticket/map terms AND the
//   B*S product above when bodies are saturated.
test "raised request_body_bytes: publish concurrent body memory cost" {
    const B: usize = 2 * 1024 * 1024; // 2 MiB — qmdsync-class body
    var lim = starh2.Limits.defaults;
    const base = try lim.resourceUpperBound();

    lim.request_body_bytes = B;
    const body_only = try lim.resourceUpperBound();
    try std.testing.expect(body_only.terms.h1_body > base.terms.h1_body);
    try std.testing.expect(body_only.allocator_bytes > base.allocator_bytes);

    const S = lim.max_streams_per_connection;
    const need_conn = B * S; // 2 MiB * 256 = 512 MiB
    try std.testing.expectEqual(@as(usize, 512 * 1024 * 1024), need_conn);

    lim.request_bytes_per_connection = need_conn;
    lim.request_bytes_per_server = @max(lim.request_bytes_per_server, need_conn);
    const full = try lim.resourceUpperBound();
    try std.testing.expect(full.allocator_bytes > body_only.allocator_bytes);
    const delta = full.allocator_bytes - body_only.allocator_bytes;
    try std.testing.expect(delta >= need_conn);
}
