const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const dummy: u8 = 0;
const response_body = "backend-ok";

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, response_body);
}

const ReadRace = union(enum) {
    read: std.Io.Reader.Error!usize,
    timeout: std.Io.Cancelable!void,
};

fn readSome(reader: *std.Io.net.Stream.Reader, buffer: []u8) std.Io.Reader.Error!usize {
    var dest: [1][]u8 = .{buffer};
    return reader.interface.readVec(&dest);
}

fn timeout(io: std.Io, timeout_ns: u64) std.Io.Cancelable!void {
    return io.sleep(.fromNanoseconds(timeout_ns), .awake);
}

fn readUntilBody(io: std.Io, reader: *std.Io.net.Stream.Reader, elapsed_limit_ns: u64) !void {
    var response: [16 * 1024]u8 = undefined;
    var used: usize = 0;
    while (used < response.len) {
        var result_buffer: [2]ReadRace = undefined;
        var select = std.Io.Select(ReadRace).init(io, &result_buffer);
        try select.concurrent(.read, readSome, .{ reader, response[used..] });
        try select.concurrent(.timeout, timeout, .{ io, elapsed_limit_ns });
        const selected = try select.await();
        select.cancelDiscard();
        switch (selected) {
            .read => |result| {
                used += try result;
                if (std.mem.indexOf(u8, response[0..used], response_body) != null) return;
            },
            .timeout => |result| {
                try result;
                std.debug.print("response timeout after {d} bytes: {any}\n", .{ used, response[0..used] });
                return error.ResponseTimeout;
            },
        }
    }
    return error.ResponseTooLarge;
}

fn runRealH2(io: std.Io, gpa: std.mem.Allocator, wakeup_gate: bool) !void {
    starh2.edge.connection.test_observed_sched_emits.store(0, .release);
    starh2.edge.connection.test_wire_sends.store(0, .release);
    starh2.edge.connection.test_observed_inline_jobs.store(0, .release);
    const listen_addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{.{
        .method = .GET,
        .path = "/parity",
        .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } },
    }};
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{.{ .h2c_prior_knowledge = listen_addr }},
        .routes = &routes,
        .tls = null,
    });
    defer server.deinit(gpa);

    var serve_future = try io.concurrent(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_future.cancel(io) catch {
            // The test is already failing; cancellation exists only to release server resources.
        };
    };
    try server.waitUntilListening(2 * std.time.ns_per_s);

    const peer = server.localAddress(0);
    const stream = try peer.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var read_buffer: [4096]u8 = undefined;
    var write_buffer: [4096]u8 = undefined;
    var reader = stream.reader(io, &read_buffer);
    var writer = stream.writer(io, &write_buffer);

    if (wakeup_gate) {
        starh2.edge.connection.test_actor_waiting.reset();
        starh2.edge.connection.test_release_actor_wait.reset();
        starh2.edge.connection.test_hold_before_actor_wait.store(true, .release);
        starh2.edge.connection.test_polling_canary_tick_ns.store(std.time.ns_per_s, .release);
        defer starh2.edge.connection.test_polling_canary_tick_ns.store(0, .release);

        var preface = try h2c.buildClientPrefaceAndSettings(gpa);
        defer preface.deinit(gpa);
        try writer.interface.writeAll(preface.items);
        try writer.interface.flush();
        starh2.edge.connection.test_actor_waiting.waitTimeout(io, .{
            .duration = .{ .raw = .fromNanoseconds(2 * std.time.ns_per_s), .clock = .awake },
        }) catch |err| {
            std.debug.print("wakeup setup failed: active={d} boot={} sends={d} live={d}\n", .{
                server.active_connections.load(.acquire),
                starh2.edge.connection.test_boot_ready.load(.acquire),
                starh2.edge.connection.test_wire_sends.load(.acquire),
                starh2.edge.connection.test_observed_live_handlers.load(.acquire),
            });
            return err;
        };

        var headers: std.ArrayList(u8) = .empty;
        defer headers.deinit(gpa);
        try h2c.appendHeaders(gpa, &headers, 1, "/parity", true);
        const started = std.Io.Clock.awake.now(io).nanoseconds;
        starh2.edge.connection.test_release_actor_wait.set(io);
        try writer.interface.writeAll(headers.items);
        try writer.interface.flush();
        try readUntilBody(io, &reader, 250 * std.time.ns_per_ms);
        const elapsed = std.Io.Clock.awake.now(io).nanoseconds - started;
        try std.testing.expect(elapsed < 250 * std.time.ns_per_ms);
    } else {
        const request = try h2c.buildClientHello(gpa, "/parity");
        defer gpa.free(request);
        try writer.interface.writeAll(request);
        try writer.interface.flush();
        readUntilBody(io, &reader, 2 * std.time.ns_per_s) catch |err| {
            std.debug.print("parity read failed: active={d} boot={} sends={d} sched={d} live={d} writer_fail={}\n", .{
                server.active_connections.load(.acquire),
                starh2.edge.connection.test_boot_ready.load(.acquire),
                starh2.edge.connection.test_wire_sends.load(.acquire),
                starh2.edge.connection.test_observed_sched_emits.load(.acquire),
                starh2.edge.connection.test_observed_live_handlers.load(.acquire),
                starh2.edge.connection.test_observed_writer_fail_handled.load(.acquire),
            });
            return err;
        };
    }
    try std.testing.expect(starh2.edge.connection.test_observed_inline_jobs.load(.acquire) >= 1);
    try std.testing.expectEqual(@as(usize, 0), server.accounting.reaper_reserved.load(.acquire));

    server.requestShutdown();
    try serve_future.await(io);
    serving = false;
}

fn runZio(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    try runRealH2(rt.io(), gpa, false);
}

test "backend parity: real h2 request over zio std.Io" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runZio, .{ rt, std.testing.allocator });
    try handle.join();
}

test "backend parity: real h2 request over std.Io.Threaded" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    try runRealH2(threaded.io(), std.testing.allocator, false);
}

test "wakeup gate: inbound request beats one second polling canary" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    try runRealH2(threaded.io(), std.testing.allocator, true);
}
