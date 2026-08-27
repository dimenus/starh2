//! HTTP/1.1 oneshot gates: Content-Length completes without EOF, close closes.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

const dummy: u8 = 0;

fn taskBody(req: *const starh2.Request) []const u8 {
    if (std.mem.eql(u8, req.path, "/v1/tasks/t-7")) return "task-7";
    return "missing";
}

fn handleTask(_: *anyopaque, req: *const starh2.Request, reply: *starh2.http1.Reply) anyerror!void {
    const body = taskBody(req);
    const status: u16 = if (std.mem.eql(u8, body, "task-7")) 200 else 404;
    try reply.send(status, &.{.{ .name = "content-type", .value = "text/plain" }}, body);
}

fn handleEcho(_: *anyopaque, req: *const starh2.Request, reply: *starh2.http1.Reply) anyerror!void {
    try reply.send(200, &.{}, req.body);
}

const ReadRace = union(enum) {
    byte: std.Io.Reader.Error!u8,
    timeout: std.Io.Cancelable!void,
};

fn takeOne(reader: *std.Io.Reader) std.Io.Reader.Error!u8 {
    return reader.takeByte();
}

fn sleepNs(io: std.Io, timeout_ns: u64) std.Io.Cancelable!void {
    return io.sleep(.fromNanoseconds(timeout_ns), .awake);
}

fn expectEofSoon(io: std.Io, reader: *std.Io.Reader, timeout_ns: u64) !void {
    var result_buffer: [2]ReadRace = undefined;
    var select = std.Io.Select(ReadRace).init(io, &result_buffer);
    try select.concurrent(.byte, takeOne, .{reader});
    try select.concurrent(.timeout, sleepNs, .{ io, timeout_ns });
    const selected = try select.await();
    select.cancelDiscard();
    switch (selected) {
        .byte => |result| {
            if (result) |_| return error.ExpectedEof;
            else |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            }
        },
        .timeout => |result| {
            try result;
            return error.CloseDidNotArrive;
        },
    }
}

fn startServer(io: std.Io, gpa: std.mem.Allocator, handler: starh2.http1.Handler) !starh2.http1.Server {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var server = try starh2.http1.Server.init(gpa, io, .{
        .address = addr,
        .handler = handler,
    });
    errdefer server.deinit(gpa);
    return server;
}

fn runContentLengthWithoutEof(io: std.Io, gpa: std.mem.Allocator) !void {
    const bind = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var listener = try bind.listen(io, .{ .reuse_address = true });
    defer listener.socket.close(io);
    const dest = listener.socket.address;

    const stub = struct {
        fn run(lio: std.Io, l: *std.Io.net.Server) std.Io.Cancelable!void {
            const stream = l.accept(lio) catch return;
            defer stream.close(lio);
            var write_buf: [256]u8 = undefined;
            var writer = stream.writer(lio, &write_buf);
            writer.interface.writeAll("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok") catch return;
            writer.interface.flush() catch return;
            // Stay open. A client that waits for EOF hangs here.
            lio.sleep(.fromSeconds(30), .awake) catch {};
        }
    };
    var stub_future = try io.concurrent(stub.run, .{ io, &listener });
    defer stub_future.cancel(io) catch {};

    const started = std.Io.Clock.awake.now(io).nanoseconds;
    var resp = try starh2.http1.get(io, gpa, dest, "127.0.0.1", "/v1/tasks/t-7");
    defer resp.deinit();
    const elapsed = std.Io.Clock.awake.now(io).nanoseconds - started;
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("ok", resp.body);
    try std.testing.expect(elapsed < 500 * std.time.ns_per_ms);
}

fn runMissingLengthFailsClosed(io: std.Io, gpa: std.mem.Allocator) !void {
    const bind = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var listener = try bind.listen(io, .{ .reuse_address = true });
    defer listener.socket.close(io);
    const dest = listener.socket.address;

    const stub = struct {
        fn run(lio: std.Io, l: *std.Io.net.Server) std.Io.Cancelable!void {
            const stream = l.accept(lio) catch return;
            defer stream.close(lio);
            var write_buf: [256]u8 = undefined;
            var writer = stream.writer(lio, &write_buf);
            writer.interface.writeAll("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nhello") catch return;
            writer.interface.flush() catch return;
            lio.sleep(.fromSeconds(30), .awake) catch {};
        }
    };
    var stub_future = try io.concurrent(stub.run, .{ io, &listener });
    defer stub_future.cancel(io) catch {};

    const started = std.Io.Clock.awake.now(io).nanoseconds;
    try std.testing.expectError(error.MissingContentLength, starh2.http1.get(io, gpa, dest, "127.0.0.1", "/"));
    const elapsed = std.Io.Clock.awake.now(io).nanoseconds - started;
    try std.testing.expect(elapsed < 500 * std.time.ns_per_ms);
}

fn runCloseActuallyCloses(io: std.Io, gpa: std.mem.Allocator) !void {
    var server = try startServer(io, gpa, .{ .ptr = @constCast(&dummy), .runFn = handleTask });
    defer server.deinit(gpa);
    var serve_future = try io.concurrent(starh2.http1.Server.serve, .{&server});
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_future.cancel(io) catch {};
    };
    try server.waitUntilListening(2 * std.time.ns_per_s);

    const stream = try server.localAddress().connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var read_buf: [2048]u8 = undefined;
    var write_buf: [512]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);
    try starh2.http1.writeRequest(&writer.interface, "GET", "/v1/tasks/t-7", "127.0.0.1", &.{}, "");
    try writer.interface.flush();

    var resp = try starh2.http1.readResponse(&reader.interface, gpa, .{});
    defer resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("task-7", resp.body);
    try std.testing.expect(resp.connection_close);
    try expectEofSoon(io, &reader.interface, 250 * std.time.ns_per_ms);

    server.requestShutdown();
    try serve_future.await(io);
    serving = false;
}

fn runOneshotGet(io: std.Io, gpa: std.mem.Allocator) !void {
    var server = try startServer(io, gpa, .{ .ptr = @constCast(&dummy), .runFn = handleTask });
    defer server.deinit(gpa);
    var serve_future = try io.concurrent(starh2.http1.Server.serve, .{&server});
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_future.cancel(io) catch {};
    };
    try server.waitUntilListening(2 * std.time.ns_per_s);

    var resp = try starh2.http1.get(io, gpa, server.localAddress(), "127.0.0.1", "/v1/tasks/t-7");
    defer resp.deinit();
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("task-7", resp.body);
    try std.testing.expect(resp.connection_close);

    server.requestShutdown();
    try serve_future.await(io);
    serving = false;
}

fn runHugeHeadersRejected(io: std.Io, gpa: std.mem.Allocator) !void {
    var limits = starh2.http1.Limits{};
    limits.header_bytes = 256;
    var server = try starh2.http1.Server.init(gpa, io, .{
        .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0),
        .handler = .{ .ptr = @constCast(&dummy), .runFn = handleTask },
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_future = try io.concurrent(starh2.http1.Server.serve, .{&server});
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_future.cancel(io) catch {};
    };
    try server.waitUntilListening(2 * std.time.ns_per_s);

    const stream = try server.localAddress().connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var read_buf: [1024]u8 = undefined;
    var write_buf: [1024]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);
    try writer.interface.writeAll("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Big: ");
    var pad: [512]u8 = undefined;
    @memset(&pad, 'a');
    try writer.interface.writeAll(&pad);
    try writer.interface.writeAll("\r\n\r\n");
    try writer.interface.flush();

    var resp = try starh2.http1.readResponse(&reader.interface, gpa, .{});
    defer resp.deinit();
    try std.testing.expectEqual(@as(u16, 431), resp.status);

    server.requestShutdown();
    try serve_future.await(io);
    serving = false;
}

fn runPostMissingLengthRejected(io: std.Io, gpa: std.mem.Allocator) !void {
    var server = try startServer(io, gpa, .{ .ptr = @constCast(&dummy), .runFn = handleEcho });
    defer server.deinit(gpa);
    var serve_future = try io.concurrent(starh2.http1.Server.serve, .{&server});
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_future.cancel(io) catch {};
    };
    try server.waitUntilListening(2 * std.time.ns_per_s);

    const stream = try server.localAddress().connect(io, .{ .mode = .stream });
    defer stream.close(io);
    var read_buf: [1024]u8 = undefined;
    var write_buf: [256]u8 = undefined;
    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);
    try writer.interface.writeAll("POST /echo HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    try writer.interface.flush();

    var resp = try starh2.http1.readResponse(&reader.interface, gpa, .{});
    defer resp.deinit();
    try std.testing.expectEqual(@as(u16, 411), resp.status);

    server.requestShutdown();
    try serve_future.await(io);
    serving = false;
}

fn runZio(rt: *zio.Runtime, gpa: std.mem.Allocator, f: *const fn (std.Io, std.mem.Allocator) anyerror!void) !void {
    try f(rt.io(), gpa);
}

test "client finishes a Content-Length body without waiting for peer close" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runZio, .{ rt, std.testing.allocator, runContentLengthWithoutEof });
    try handle.join();
}

test "client rejects a 200 without Content-Length instead of reading to EOF" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runZio, .{ rt, std.testing.allocator, runMissingLengthFailsClosed });
    try handle.join();
}

test "Connection: close half-closes so a further read sees EOF" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runZio, .{ rt, std.testing.allocator, runCloseActuallyCloses });
    try handle.join();
}

test "oneshot GET /v1/tasks/t-N through server and client" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runZio, .{ rt, std.testing.allocator, runOneshotGet });
    try handle.join();
}

test "huge request headers fail closed with 431" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runZio, .{ rt, std.testing.allocator, runHugeHeadersRejected });
    try handle.join();
}

test "POST without Content-Length is 411" {
    var rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(runZio, .{ rt, std.testing.allocator, runPostMissingLengthRejected });
    try handle.join();
}
