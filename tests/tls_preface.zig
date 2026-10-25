//! Gate for t-3511: the server SETTINGS is the first frame on a TLS connection
//! (RFC 9113 section 3.4), whatever the client's first flight makes the server
//! queue.
//!
//! The client sends its TLS Finished and its whole first HTTP/2 flight in ONE
//! socket write, the way curl and browsers pipeline it, so the server reads
//! the flight as handshake leftover plaintext. Two flights:
//!
//! - more streams than `max_streams_per_connection`: Session queues
//!   RST_STREAM REFUSED_STREAM while it ingests the flight;
//! - a DATA frame on stream 0: Session queues GOAWAY PROTOCOL_ERROR. Whether
//!   the GOAWAY reaches the client is t-3520; this test asserts only that
//!   whatever arrives starts with the server SETTINGS.
//!
//! Both frames are in the scheduler's terminal class and used to be emitted
//! before the server SETTINGS. Each shape opens many connections, because
//! the order depended on what the server's first read returned.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const tls = @import("tls");

const frame = starh2.core.frame;

const connections_per_shape = 24;
const stream_limit = 2;
const refused_streams = 4;
const client_deadline_ns = 10 * std.time.ns_per_s;
const max_frames = 32;
const dummy: u8 = 0;

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

fn realNow() std.Io.Timestamp {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return .fromNanoseconds(@as(i96, ts.sec) * std.time.ns_per_s + ts.nsec);
}

const Shape = enum { refused_stream, connection_error };

const Seen = struct {
    ftype: u8,
    flags: u8,
    sid: u32,
    /// Error code of RST_STREAM (payload 0..4) or GOAWAY (payload 4..8).
    code: u32,
};

const ClientResult = struct {
    ok_n: usize = 0,
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
};

/// Blocking TLS client on a raw libc socket, so it keeps running while the
/// server's executor is busy. Received ciphertext accumulates in `in`.
const Client = struct {
    fd: c_int,
    in: [2 * tls.input_buffer_len]u8 = undefined,
    in_len: usize = 0,
    out: [tls.output_buffer_len]u8 = undefined,

    fn fill(self: *Client) !void {
        if (self.in_len == self.in.len) return error.ClientBufferFull;
        const n = std.c.recv(self.fd, self.in[self.in_len..].ptr, self.in.len - self.in_len, 0);
        if (n == 0) return error.Eof;
        if (n < 0) {
            return switch (std.c.errno(n)) {
                .AGAIN, .INTR => error.WouldBlock,
                else => error.Eof,
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

fn recordLen(bytes: []const u8) ?usize {
    if (bytes.len < 5) return null;
    const len = 5 + @as(usize, std.mem.readInt(u16, bytes[3..5], .big));
    return if (bytes.len < len) null else len;
}

fn clientMain(port: u16, gpa: std.mem.Allocator, shape: Shape, out: *ClientResult) void {
    for (0..connections_per_shape) |i| {
        oneConnection(port, gpa, shape) catch |err| {
            std.debug.print("tls preface: {s}: connection {d} failed: {s}\n", .{ @tagName(shape), i, @errorName(err) });
            out.err = err;
            break;
        };
        out.ok_n += 1;
    }
    out.done.store(true, .release);
}

fn oneConnection(port: u16, gpa: std.mem.Allocator, shape: Shape) !void {
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.Socket;
    defer _ = std.c.close(fd);
    var addr: std.c.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, port),
        .addr = std.mem.nativeToBig(u32, 0x7f000001),
    };
    if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
    const tv: std.c.timeval = .{ .sec = 1, .usec = 0 };
    _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVTIMEO, std.mem.asBytes(&tv), @sizeOf(std.c.timeval));

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

    // The last handshake flight (the client Finished) is held back and sent
    // in the same write as the HTTP/2 flight.
    var flight: std.ArrayList(u8) = .empty;
    defer flight.deinit(gpa);
    while (!hs.done()) {
        const r = try hs.run(client.in[0..client.in_len], &client.out);
        client.consume(r.recv_pos);
        if (hs.done()) {
            try flight.appendSlice(gpa, r.send);
            break;
        }
        if (r.send.len > 0) try client.sendAll(r.send);
        if (r.recv_pos == 0 and r.send.len == 0) try client.fill();
    }
    const alpn = hs.inner.alpn_protocol orelse return error.Alpn;
    if (!std.mem.eql(u8, alpn, "h2")) return error.Alpn;
    var conn: tls.nonblock.Connection = .init(hs.cipher().?);

    var wire = try h2c.buildClientPreface(gpa, .empty);
    defer wire.deinit(gpa);
    switch (shape) {
        .refused_stream => for (0..stream_limit + refused_streams) |i|
            try h2c.appendHeaders(gpa, &wire, @intCast(1 + 2 * i), "/hello", true),
        .connection_error => {
            // DATA on stream 0 is a connection error (RFC 9113 section 6.1).
            var hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
            (frame.FrameHeader{ .length = 0, .type = .data, .flags = .{}, .stream_id = 0 }).encode(&hdr);
            try wire.appendSlice(gpa, &hdr);
        },
    }
    var off: usize = 0;
    while (off < wire.items.len) {
        const r = try conn.encrypt(wire.items[off..][0..@min(wire.items.len - off, 16 * 1024)], &client.out);
        try flight.appendSlice(gpa, r.ciphertext);
        off += r.cleartext_pos;
    }
    try client.sendAll(flight.items);

    var seen: [max_frames]Seen = undefined;
    var seen_n: usize = 0;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(gpa);
    var rd: [tls.input_buffer_len]u8 = undefined;
    const deadline = nowNs() + client_deadline_ns;
    var ended = false;
    read: while (!ended) {
        if (nowNs() >= deadline) return error.Deadline;
        const rec_len = recordLen(client.in[0..client.in_len]) orelse {
            client.fill() catch |err| switch (err) {
                error.WouldBlock => continue,
                error.Eof => break :read,
                else => return err,
            };
            continue;
        };
        const res = try conn.decrypt(client.in[0..rec_len], &rd);
        client.consume(rec_len);
        if (res.closed) break;
        try buf.appendSlice(gpa, res.cleartext);
        var foff: usize = 0;
        while (buf.items.len - foff >= frame.FRAME_HEADER_LEN) {
            const h = buf.items[foff..];
            const len = (@as(usize, h[0]) << 16) | (@as(usize, h[1]) << 8) | h[2];
            if (buf.items.len - foff < 9 + len) break;
            if (seen_n == max_frames) return error.TooManyFrames;
            const payload = h[9..][0..len];
            seen[seen_n] = .{
                .ftype = h[3],
                .flags = h[4],
                .sid = std.mem.readInt(u32, h[5..9], .big) & 0x7fff_ffff,
                .code = switch (h[3]) {
                    3 => if (len >= 4) std.mem.readInt(u32, payload[0..4], .big) else 0,
                    7 => if (len >= 8) std.mem.readInt(u32, payload[4..8], .big) else 0,
                    else => 0,
                },
            };
            seen_n += 1;
            foff += 9 + len;
        }
        buf.replaceRangeAssumeCapacity(0, foff, &.{});
        // Stop once the frames under test have arrived.
        switch (shape) {
            .refused_stream => {
                var rsts: usize = 0;
                for (seen[0..seen_n]) |s| {
                    if (s.ftype == 3) rsts += 1;
                }
                ended = rsts >= refused_streams;
            },
            .connection_error => {
                for (seen[0..seen_n]) |s| {
                    if (s.ftype == 7) ended = true;
                }
            },
        }
    }

    try check(shape, seen[0..seen_n]);
}

fn dump(shape: Shape, seen: []const Seen) void {
    std.debug.print("tls preface: {s}: frames in wire order:\n", .{@tagName(shape)});
    for (seen) |s| std.debug.print("  type={d} flags={x} stream={d} code={d}\n", .{ s.ftype, s.flags, s.sid, s.code });
    if (seen.len == 0) std.debug.print("  (none)\n", .{});
}

fn check(shape: Shape, seen: []const Seen) !void {
    errdefer dump(shape, seen);
    // Whatever the server writes starts with its SETTINGS, without ACK. The
    // connection-error shape may receive nothing at all today (see below), so
    // an empty list is allowed there only.
    if (seen.len == 0) {
        if (shape == .connection_error) return;
        return error.NoFrames;
    }
    if (seen[0].ftype != 4 or seen[0].flags & 0x1 != 0 or seen[0].sid != 0) return error.FirstFrameNotServerSettings;
    switch (shape) {
        .refused_stream => {
            var next_refused: u32 = 1 + 2 * stream_limit;
            var n: usize = 0;
            for (seen[1..]) |s| {
                if (s.ftype != 3) continue;
                if (s.code != 7 or s.sid != next_refused) return error.WrongRst;
                next_refused += 2;
                n += 1;
            }
            if (n != refused_streams) return error.MissingRefusal;
        },
        // GOAWAY delivery after a first-flight connection error is t-3520:
        // today teardown drops the stashed GOAWAY. This assertion tightens
        // there to "SETTINGS, then GOAWAY PROTOCOL_ERROR".
        .connection_error => {},
    }
}

fn runShape(rt: *zio.Runtime, gpa: std.mem.Allocator, shape: Shape) !void {
    const io = rt.io();
    const cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert_pem);
    const key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key_pem);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_streams_per_connection = stream_limit;
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{.{ .tls = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
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

    const checks_before = starh2.edge.connection.test_preface_checks.load(.acquire);
    const leftover_before = starh2.edge.connection.test_tls_leftover_connections.load(.acquire);
    var result: ClientResult = .{};
    const t = try std.Thread.spawn(.{}, clientMain, .{ server.localAddress(0).getPort(), gpa, shape, &result });
    // Wait inside zio so this executor keeps serving the connections.
    while (!result.done.load(.acquire)) zio.sleep(.fromMilliseconds(10)) catch {};
    t.join();
    if (result.err) |err| return err;
    try std.testing.expectEqual(@as(usize, connections_per_shape), result.ok_n);
    // Every connection's first wire handoff went through the first-frame check.
    const checks = starh2.edge.connection.test_preface_checks.load(.acquire) - checks_before;
    try std.testing.expectEqual(@as(usize, connections_per_shape), checks);
    // Every connection started with handshake leftover plaintext, so the test
    // reached the path under test and not a later read.
    const leftovers = starh2.edge.connection.test_tls_leftover_connections.load(.acquire) - leftover_before;
    try std.testing.expectEqual(@as(usize, connections_per_shape), leftovers);
}

fn runRefused(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    try runShape(rt, gpa, .refused_stream);
}

fn runConnectionError(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    try runShape(rt, gpa, .connection_error);
}

fn newRuntime(gpa: std.mem.Allocator) !*zio.Runtime {
    return zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
}

test "tls: a pipelined first flight that is refused still sees the server SETTINGS first (t-3511)" {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runRefused, .{ rt, gpa });
    try h.join();
}

test "tls: a pipelined first flight that is a connection error sees the server SETTINGS first (t-3511)" {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runConnectionError, .{ rt, gpa });
    try h.join();
}
