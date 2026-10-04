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
//! TLS client (tls.zig's non-blocking API over a raw socket) on an OS thread has a small receive buffer, asks for a large
//! streamed body, and does not read for 300 ms, so the server's socket fills
//! and a send stays in flight while drainEmit keeps stashing chunks. Then the
//! client reads to END_STREAM. With the spin, the stream never ends, and the
//! client panics at its deadline: a loud failure, never a hang.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const tls = @import("tls");

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

/// Blocking TLS h2 client on a raw libc socket. Received ciphertext
/// accumulates in `in` and is decrypted from there; `out` holds one record.
const Client = struct {
    fd: c_int,
    in: [2 * tls.input_buffer_len]u8 = undefined,
    in_len: usize = 0,
    out: [tls.output_buffer_len]u8 = undefined,

    /// Read more ciphertext. `error.WouldBlock` when the receive timeout
    /// fired with nothing to read.
    fn fill(self: *Client) !void {
        if (self.in_len == self.in.len) return error.ClientBufferFull;
        const n = std.c.recv(self.fd, self.in[self.in_len..].ptr, self.in.len - self.in_len, 0);
        if (n == 0) return error.Eof;
        if (n < 0) {
            return switch (std.c.errno(n)) {
                .AGAIN, .INTR => error.WouldBlock,
                else => error.Recv,
            };
        }
        self.in_len += @intCast(n);
    }

    fn consume(self: *Client, n: usize) void {
        std.mem.copyForwards(u8, self.in[0 .. self.in_len - n], self.in[n..self.in_len]);
        self.in_len -= n;
    }

    fn sendAll(self: *Client, bytes: []const u8) !void {
        var off: usize = 0;
        while (off < bytes.len) {
            const n = std.c.send(self.fd, bytes[off..].ptr, bytes.len - off, 0);
            if (n <= 0) return error.Send;
            off += @intCast(n);
        }
    }
};

/// Length of the first record in `bytes` when it is all there.
fn recordLen(bytes: []const u8) ?usize {
    if (bytes.len < 5) return null;
    const len = 5 + @as(usize, std.mem.readInt(u16, bytes[3..5], .big));
    return if (bytes.len < len) null else len;
}

fn realNow() std.Io.Timestamp {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return .fromNanoseconds(@as(i96, ts.sec) * std.time.ns_per_s + ts.nsec);
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

    const client = try gpa.create(Client);
    defer gpa.destroy(client);
    client.* = .{ .fd = fd };
    var prng = std.Random.DefaultPrng.init(@truncate(@as(u128, @bitCast(@as(i128, realNow().nanoseconds)))));
    var hs = tls.nonblock.Client.init(.{
        .rng = prng.random(),
        .now = realNow(),
        .host = "localhost",
        .root_ca = .empty,
        .insecure_skip_verify = true,
        .cipher_suites = tls.config.cipher_suites.tls13,
        .alpn_protocols = &.{"h2"},
    });
    while (!hs.done()) {
        const r = try hs.run(client.in[0..client.in_len], &client.out);
        client.consume(r.recv_pos);
        if (r.send.len > 0) try client.sendAll(r.send);
        if (hs.done()) break;
        if (r.recv_pos == 0 and r.send.len == 0) try client.fill();
    }
    const alpn = hs.inner.alpn_protocol orelse return error.Alpn;
    if (!std.mem.eql(u8, alpn, "h2")) return error.Alpn;
    var conn: tls.nonblock.Connection = .init(hs.cipher().?);

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
    var off: usize = 0;
    while (off < wire.items.len) {
        const r = try conn.encrypt(wire.items[off..][0..@min(wire.items.len - off, 16 * 1024)], &client.out);
        try client.sendAll(r.ciphertext);
        off += r.cleartext_pos;
    }

    // Stop reading while the server writes the body: its socket fills and a
    // send stays in flight.
    const pause: std.c.timespec = .{ .sec = 0, .nsec = 300 * std.time.ns_per_ms };
    _ = std.c.nanosleep(&pause, null);

    // Read frames until every stream ends. A read timeout on the socket keeps
    // a wedged server from blocking this thread past the deadline.
    const tv: std.c.timeval = .{ .sec = 1, .usec = 0 };
    _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVTIMEO, std.mem.asBytes(&tv), @sizeOf(std.c.timeval));
    const deadline = nowNs() + client_deadline_ns;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(gpa);
    // Room for any record payload, and one record per decrypt: a smaller
    // output makes tls.zig keep plaintext inside `client.in`, which
    // `consume` then shifts.
    var rd: [tls.input_buffer_len]u8 = undefined;
    while (out.ended_n < stream_count) {
        if (nowNs() >= deadline) {
            std.debug.panic("tls backpressure: stream did not end within {d} s ({d} of {d} body bytes); the actor is not parking", .{
                client_deadline_ns / std.time.ns_per_s, out.data_bytes, stream_count * event_count * event_bytes,
            });
        }
        const rec_len = recordLen(client.in[0..client.in_len]) orelse 0;
        if (rec_len == 0) {
            client.fill() catch |err| switch (err) {
                error.WouldBlock => continue,
                else => return err,
            };
            continue;
        }
        const res = try conn.decrypt(client.in[0..rec_len], &rd);
        client.consume(rec_len);
        if (res.closed) return error.PeerClosed;
        try buf.appendSlice(gpa, res.cleartext);
        // Consume whole frames.
        var foff: usize = 0;
        while (buf.items.len - foff >= 9) {
            const h = buf.items[foff..];
            const len = (@as(usize, h[0]) << 16) | (@as(usize, h[1]) << 8) | h[2];
            if (buf.items.len - foff < 9 + len) break;
            const ftype = h[3];
            const flags = h[4];
            const sid = std.mem.readInt(u32, h[5..9], .big) & 0x7fff_ffff;
            if (sid != 0 and ftype == 0) out.data_bytes += len;
            if (sid != 0 and (ftype == 0 or ftype == 1) and flags & 0x1 != 0) out.ended_n += 1;
            foff += 9 + len;
        }
        buf.replaceRangeAssumeCapacity(0, foff, &.{});
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
