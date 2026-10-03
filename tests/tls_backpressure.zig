//! Gate for t-2655: a TLS actor whose staging buffer is full behind an
//! in-flight send must park until the send completes.
//!
//! The send completion arrives through the pump's CompletionQueue, and only
//! the select in `waitForActivity` reaps it. `driveTlsTurn` used to return
//! without parking whenever an outbound chunk sat in `carried`, even when
//! the staging buffer was full and the chunk could not be written. The actor
//! then went round its loop forever without yielding, its executor never
//! polled again, the completion was never delivered, and every task on that
//! executor stopped for good. On nachos it showed as the zero-event collapse.
//!
//! Shape: ONE pinned executor, so the server and the test share it. A real
//! TLS client on an OS thread has a small receive buffer, asks for a large
//! streamed body, and does not read for 300 ms, so the server's socket fills
//! and a send stays in flight while drainEmit keeps stashing chunks. Then the
//! client reads to END_STREAM. With the spin, the stream never ends, and the
//! client panics at its deadline: a loud failure, never a hang.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const boring = @import("boring");
const sys = boring.boringssl;

const event_bytes = 16 * 1024;
const event_count = 32;
const stream_count = 64;
const client_deadline_ns = 10 * std.time.ns_per_s;
const dummy: u8 = 0;

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

fn bigSse(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    var payload: [event_bytes]u8 = undefined;
    @memset(&payload, 'x');
    for (0..event_count) |_| try body.writeAll(&payload);
    try body.finish();
}

const ClientResult = struct {
    data_bytes: usize = 0,
    ended_n: usize = 0,
    err: ?anyerror = null,
    /// Set last by the client thread; the fields above are read after it.
    done: std.atomic.Value(bool) = .init(false),
};

fn sslWriteAll(ssl: *sys.SSL, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = sys.SSL_write(ssl, bytes[off..].ptr, @intCast(bytes.len - off));
        if (n <= 0) return error.SslWrite;
        off += @intCast(n);
    }
}

/// Blocking TLS h2 client. Plain OS thread and libc sockets on purpose: it
/// must keep running while the server's executor is wedged.
fn clientMain(port: u16, gpa: std.mem.Allocator, out: *ClientResult) void {
    clientRun(port, gpa, out) catch |err| {
        out.err = err;
    };
    out.done.store(true, .release);
}

fn clientRun(port: u16, gpa: std.mem.Allocator, out: *ClientResult) !void {
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.Socket;
    defer _ = std.c.close(fd);
    // Small receive buffer, set before connect so the window is small from
    // the handshake on: the server's socket fills after a few records.
    const rcvbuf: c_int = 4096;
    _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVBUF, std.mem.asBytes(&rcvbuf), @sizeOf(c_int));
    var addr: std.c.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, port),
        .addr = std.mem.nativeToBig(u32, 0x7f000001),
    };
    if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;

    const ctx = sys.SSL_CTX_new(sys.TLS_method()) orelse return error.SslCtx;
    defer sys.SSL_CTX_free(ctx);
    const alpn = "\x02h2";
    if (sys.SSL_CTX_set_alpn_protos(ctx, alpn, alpn.len) != 0) return error.Alpn;
    const ssl = sys.SSL_new(ctx) orelse return error.Ssl;
    defer sys.SSL_free(ssl);
    if (sys.SSL_set_fd(ssl, fd) != 1) return error.SslFd;
    if (sys.SSL_connect(ssl) != 1) return error.Handshake;

    var wire = try h2c.buildClientPreface(gpa, .empty);
    defer wire.deinit(gpa);
    {
        var sbuf: [64]u8 = undefined;
        const settings = [_]starh2.core.frame.Setting{.{ .id = .initial_window_size, .value = 1 << 30 }};
        const sn = try starh2.core.frame.Serializer.settingsFrame(&sbuf, false, &settings);
        try wire.appendSlice(gpa, sbuf[0..sn]);
    }
    try h2c.appendWindowUpdate(gpa, &wire, 0, 1 << 30);
    for (0..stream_count) |i| try h2c.appendHeaders(gpa, &wire, @intCast(1 + 2 * i), "/big", true);
    try sslWriteAll(ssl, wire.items);

    // Stop reading while the server writes the body: its socket fills and a
    // send stays in flight.
    const pause: std.c.timespec = .{ .sec = 0, .nsec = 300 * std.time.ns_per_ms };
    _ = std.c.nanosleep(&pause, null);

    // Read frames until stream 1 ends. A read timeout on the socket keeps a
    // wedged server from blocking this thread past the deadline.
    const tv: std.c.timeval = .{ .sec = 1, .usec = 0 };
    _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVTIMEO, std.mem.asBytes(&tv), @sizeOf(std.c.timeval));
    const deadline = nowNs() + client_deadline_ns;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(gpa);
    var rd: [16384]u8 = undefined;
    while (out.ended_n < stream_count) {
        if (nowNs() >= deadline) {
            std.debug.panic("tls backpressure: stream did not end within {d} s ({d} of {d} body bytes); the actor is not parking", .{
                client_deadline_ns / std.time.ns_per_s, out.data_bytes, stream_count * event_count * event_bytes,
            });
        }
        const n = sys.SSL_read(ssl, &rd, rd.len);
        if (n <= 0) {
            const e = sys.SSL_get_error(ssl, n);
            if (e == sys.SSL_ERROR_SYSCALL or e == sys.SSL_ERROR_WANT_READ) continue;
            return error.SslRead;
        }
        try buf.appendSlice(gpa, rd[0..@intCast(n)]);
        // Consume whole frames.
        var off: usize = 0;
        while (buf.items.len - off >= 9) {
            const h = buf.items[off..];
            const len = (@as(usize, h[0]) << 16) | (@as(usize, h[1]) << 8) | h[2];
            if (buf.items.len - off < 9 + len) break;
            const ftype = h[3];
            const flags = h[4];
            const sid = std.mem.readInt(u32, h[5..9], .big) & 0x7fff_ffff;
            if (sid != 0 and ftype == 0) out.data_bytes += len;
            if (sid != 0 and (ftype == 0 or ftype == 1) and flags & 0x1 != 0) out.ended_n += 1;
            off += 9 + len;
        }
        buf.replaceRangeAssumeCapacity(0, off, &.{});
    }
}

fn runBackpressure(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const io = rt.io();
    const cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert_pem);
    const key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key_pem);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = bigSse } } },
    };
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{.{ .tls = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem },
        .limits = starh2.Limits.defaults,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var result: ClientResult = .{};
    const t = try std.Thread.spawn(.{}, clientMain, .{ server.localAddress(0).getPort(), gpa, &result });
    // Wait inside zio so this executor keeps serving the connection.
    while (true) {
        zio.sleep(.fromMilliseconds(20)) catch {};
        if (result.done.load(.acquire)) break;
    }
    t.join();
    std.debug.print("tls backpressure: ended={d}/{d} body_bytes={d} err={?}\n", .{ result.ended_n, stream_count, result.data_bytes, result.err });
    if (result.err) |err| return err;
    try std.testing.expectEqual(@as(usize, stream_count), result.ended_n);
    // Every byte arrived (writeAll sends the bytes as given, no SSE framing).
    try std.testing.expectEqual(@as(usize, stream_count * event_count * event_bytes), result.data_bytes);
}

test "tls: an actor with staging full behind an in-flight send parks until the send completes" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(1),
    });
    defer rt.deinit();
    var h = try rt.spawn(runBackpressure, .{ rt, gpa });
    try h.join();
}
