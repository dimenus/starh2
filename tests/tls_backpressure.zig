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
//!
//! Gate for t-3509: a complete (inline) response must not overflow the TLS
//! write stash. Shape: the smallest legal stash (`MIN_WRITE_POOL_CHUNKS` wire
//! chunks, 3: it was 1 before the drain also waited for write-pool room, and
//! the stash is 2 slots longer than the pool) and complete
//! responses larger than one DATA frame, so one scheduler pop flushes the
//! HEADERS batch and queues a full DATA frame on its own. The drain then
//! pushes the batch receipt after its last pop. A stash with no room for that
//! push fail-closed the connection, and the client saw the TLS stream end.
//!
//! Every shape checks each DATA byte against the byte its stream offset must
//! hold, so bytes that arrive in the wrong order within a stream fail the
//! test, not only bytes that go missing.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const tls = @import("tls");

const event_bytes = 16 * 1024;
const event_count = 32;
const stream_count = 64;
/// Larger than one 16 KiB DATA frame, and inside the 64 KiB stream slab that a
/// complete body must fit.
const complete_body_bytes = 20 * 1024;
const complete_stream_count = 8;
const client_deadline_ns = 10 * std.time.ns_per_s;
const max_client_streams = 64;
const dummy: u8 = 0;

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

/// Byte `off` of a /big body. Each event has its own byte, so a client that
/// gets a stream's DATA out of order sees the wrong byte at that offset.
fn sseByte(off: usize) u8 {
    return 'A' + @as(u8, @intCast((off / event_bytes) % 26));
}

fn completeByte(_: usize) u8 {
    return 'c';
}

fn bigSse(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    var payload: [event_bytes]u8 = undefined;
    for (0..event_count) |i| {
        @memset(&payload, sseByte(i * event_bytes));
        try body.writeAll(&payload);
    }
    try body.finish();
}

fn bigComplete(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    var body: [complete_body_bytes]u8 = undefined;
    @memset(&body, 'c');
    try resp.send(200, &.{}, &body);
}

/// What one client run asks for and how it reads.
const Shape = struct {
    path: []const u8,
    streams: usize,
    body_bytes_per_stream: usize,
    /// The byte every response body must hold at each offset.
    body_byte: *const fn (off: usize) u8,
    /// How long the client waits before it reads anything.
    read_pause_ns: u64,
    /// How long the client waits between its preface and its requests. The
    /// server's SETTINGS and SETTINGS ACK are written in that time, so the
    /// requests meet an empty stash.
    request_delay_ns: u64,
};

const ClientResult = struct {
    data_bytes: usize = 0,
    /// DATA bytes that differ from `Shape.body_byte` at their stream offset.
    wrong_bytes: usize = 0,
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
fn clientMain(port: u16, gpa: std.mem.Allocator, shape: Shape, out: *ClientResult) void {
    clientRun(port, gpa, shape, out) catch |err| {
        out.err = err;
    };
    out.done.store(true, .release);
}

fn sendWire(conn: *tls.nonblock.Connection, client: *Client, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const r = try conn.encrypt(bytes[off..][0..@min(bytes.len - off, 16 * 1024)], &client.out);
        try client.sendAll(r.ciphertext);
        off += r.cleartext_pos;
    }
}

fn clientRun(port: u16, gpa: std.mem.Allocator, shape: Shape, out: *ClientResult) !void {
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
    if (shape.request_delay_ns != 0) {
        try sendWire(&conn, client, wire.items);
        wire.clearRetainingCapacity();
        const delay: std.c.timespec = .{ .sec = 0, .nsec = @intCast(shape.request_delay_ns) };
        _ = std.c.nanosleep(&delay, null);
    }
    for (0..shape.streams) |i| try h2c.appendHeaders(gpa, &wire, @intCast(1 + 2 * i), shape.path, true);
    try sendWire(&conn, client, wire.items);

    // Stop reading while the server writes the body: its socket fills and a
    // send stays in flight.
    if (shape.read_pause_ns != 0) {
        const pause: std.c.timespec = .{ .sec = 0, .nsec = @intCast(shape.read_pause_ns) };
        _ = std.c.nanosleep(&pause, null);
    }

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
    // Body offset per stream, indexed by (stream id - 1) / 2.
    var body_off: [max_client_streams]usize = @splat(0);
    std.debug.assert(shape.streams <= max_client_streams);
    while (out.ended_n < shape.streams) {
        if (nowNs() >= deadline) {
            std.debug.panic("tls backpressure: {s}: stream did not end within {d} s ({d} of {d} body bytes)", .{
                shape.path, client_deadline_ns / std.time.ns_per_s, out.data_bytes, shape.streams * shape.body_bytes_per_stream,
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
            if (sid != 0 and ftype == 0) {
                out.data_bytes += len;
                const off = &body_off[(sid - 1) / 2];
                for (h[9..][0..len]) |b| {
                    if (b != shape.body_byte(off.*)) out.wrong_bytes += 1;
                    off.* += 1;
                }
            }
            if (sid != 0 and (ftype == 0 or ftype == 1) and flags & 0x1 != 0) out.ended_n += 1;
            foff += 9 + len;
        }
        buf.replaceRangeAssumeCapacity(0, foff, &.{});
    }
}

fn runShape(rt: *zio.Runtime, gpa: std.mem.Allocator, routes: []const starh2.Route, limits: starh2.Limits, shape: Shape) !void {
    const io = rt.io();
    const cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert_pem);
    const key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key_pem);
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{.{ .tls = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = routes,
        .tls = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem },
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);

    const overflow_before = starh2.edge.tls_edge.tls_write_overflow.load(.acquire);
    var result: ClientResult = .{};
    const t = try std.Thread.spawn(.{}, clientMain, .{ server.localAddress(0).getPort(), gpa, shape, &result });
    // Wait inside zio so this executor keeps serving the connection.
    while (true) {
        zio.sleep(.fromMilliseconds(20)) catch {};
        if (result.done.load(.acquire)) break;
    }
    t.join();
    const overflows = starh2.edge.tls_edge.tls_write_overflow.load(.acquire) - overflow_before;
    std.debug.print("tls backpressure: {s}: ended={d}/{d} body_bytes={d} wrong_bytes={d} stash_overflows={d} err={?}\n", .{ shape.path, result.ended_n, shape.streams, result.data_bytes, result.wrong_bytes, overflows, result.err });
    if (result.err) |err| return err;
    try std.testing.expectEqual(@as(u64, 0), overflows);
    try std.testing.expectEqual(shape.streams, result.ended_n);
    try std.testing.expectEqual(shape.streams * shape.body_bytes_per_stream, result.data_bytes);
    try std.testing.expectEqual(@as(usize, 0), result.wrong_bytes);
}

fn runBackpressure(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = bigSse } } },
    };
    // Every byte arrives (writeAll sends the bytes as given, no SSE framing).
    try runShape(rt, gpa, &routes, starh2.Limits.defaults, .{
        .path = "/big",
        .streams = stream_count,
        .body_bytes_per_stream = event_count * event_bytes,
        .body_byte = sseByte,
        .read_pause_ns = 300 * std.time.ns_per_ms,
        .request_delay_ns = 0,
    });
}

fn runCompleteStash(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/complete", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = bigComplete } } },
    };
    var limits = starh2.Limits.defaults;
    limits.inbound_wire_chunks_per_connection = starh2.core.limits.MIN_WRITE_POOL_CHUNKS;
    // Two stash states at the batch drain. With the delay, the stash is empty
    // and the drain pops until it is full. Without it, the SETTINGS ACK is
    // still stashed, so the drain starts one slot short.
    for ([_]u64{ 100 * std.time.ns_per_ms, 0 }) |request_delay_ns| {
        try runShape(rt, gpa, &routes, limits, .{
            .path = "/complete",
            .streams = complete_stream_count,
            .body_bytes_per_stream = complete_body_bytes,
            .body_byte = completeByte,
            .read_pause_ns = 0,
            .request_delay_ns = request_delay_ns,
        });
    }
}

fn oneExecutor(gpa: std.mem.Allocator) !*zio.Runtime {
    return zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(1),
    });
}

test "tls: an actor with staging full behind an in-flight send parks until the send completes" {
    const gpa = std.testing.allocator;
    const rt = try oneExecutor(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runBackpressure, .{ rt, gpa });
    try h.join();
}

test "tls: complete responses larger than one DATA frame do not overflow the smallest stash" {
    const gpa = std.testing.allocator;
    const rt = try oneExecutor(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runCompleteStash, .{ rt, gpa });
    try h.join();
}
