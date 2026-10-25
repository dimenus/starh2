//! Gate for t-3520: a connection error must deliver its GOAWAY (RFC 9113
//! section 5.4.1) before the server closes, on h2c and on TLS, and graceful
//! shutdown must deliver its GOAWAYs and its final RST_STREAMs.
//!
//! The client is a blocking raw-socket client on its own thread, so it keeps
//! running while the server's executors are busy and it sees exactly what the
//! socket carries: a missing frame is a missing frame, not a client bug. Every
//! scenario ends with "read until the server closes", so a frame after the
//! GOAWAY, or a server that never closes, fails too.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const tls = @import("tls");

const frame = starh2.core.frame;
const wire_pump = starh2.edge.wire_pump;

const client_deadline_ns = 10 * std.time.ns_per_s;
const max_frames = 64;
const dummy: u8 = 0;

const Transport = enum { h2c, tls };

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

/// Answers after a delay, so the response lands while a graceful drain is
/// already in phase 2.
fn slow(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try zio.sleep(.fromMilliseconds(slow_handler_ms));
    try resp.send(200, &.{}, "ok");
}

const slow_handler_ms = 150;

/// Holds a stream open until the connection cancels it.
fn hangSse(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    while (true) {
        try zio.sleep(.fromMilliseconds(20));
        if (body.terminalCause() != null) return error.Canceled;
    }
}

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

fn realNow() std.Io.Timestamp {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return .fromNanoseconds(@as(i96, ts.sec) * std.time.ns_per_s + ts.nsec);
}

const Seen = struct {
    ftype: u8,
    flags: u8,
    sid: u32,
    /// Error code of RST_STREAM (payload 0..4) or GOAWAY (payload 4..8).
    code: u32,
    /// Last stream id of a GOAWAY (payload 0..4); 0 for any other frame.
    last_stream_id: u32,
};

const ftype_data = 0;
const ftype_headers = 1;
const ftype_rst_stream = 3;
const ftype_settings = 4;
const ftype_ping = 6;
const ftype_goaway = 7;

/// What one client thread saw. `frames` is in wire order. `closed` is true
/// when the server closed the connection before the deadline.
const Outcome = struct {
    frames: [max_frames]Seen = undefined,
    n: usize = 0,
    closed: bool = false,
    /// When stream 1's END_STREAM arrived, and when the close did (monotonic
    /// ns, 0 until then).
    end_ns: u64 = 0,
    closed_ns: u64 = 0,
    err: ?anyerror = null,
    /// Set by the client when the scenario needs the test body to act (for
    /// example to call `requestShutdown`).
    ready: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),

    fn slice(self: *const Outcome) []const Seen {
        return self.frames[0..self.n];
    }

    fn dump(self: *const Outcome, label: []const u8) void {
        std.debug.print("{s}: frames in wire order (closed={}, err={s}):\n", .{ label, self.closed, if (self.err) |e| @errorName(e) else "none" });
        for (self.slice()) |s| std.debug.print("  type={d} flags={x} stream={d} code={d} last_stream={d}\n", .{ s.ftype, s.flags, s.sid, s.code, s.last_stream_id });
        if (self.n == 0) std.debug.print("  (none)\n", .{});
    }
};

/// Blocking client on a raw libc socket. For TLS, received ciphertext
/// accumulates in `in` and decrypted bytes in `plain`.
const Wire = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    fd: c_int,
    in: [2 * tls.input_buffer_len]u8 = undefined,
    in_len: usize = 0,
    out: [tls.output_buffer_len]u8 = undefined,
    rd: [tls.input_buffer_len]u8 = undefined,
    conn: tls.nonblock.Connection = undefined,
    plain: std.ArrayList(u8) = .empty,

    /// `rcvbuf` shrinks the kernel receive buffer before connecting, so a
    /// client that stops reading stalls the server after a few KiB.
    fn open(gpa: std.mem.Allocator, transport: Transport, port: u16, rcvbuf: ?c_int) !*Wire {
        const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
        if (fd < 0) return error.Socket;
        errdefer _ = std.c.close(fd);
        if (rcvbuf) |n| _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVBUF, std.mem.asBytes(&n), @sizeOf(c_int));
        var addr: std.c.sockaddr.in = .{
            .port = std.mem.nativeToBig(u16, port),
            .addr = std.mem.nativeToBig(u32, 0x7f000001),
        };
        if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
        const tv: std.c.timeval = .{ .sec = 1, .usec = 0 };
        _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVTIMEO, std.mem.asBytes(&tv), @sizeOf(std.c.timeval));

        const self = try gpa.create(Wire);
        errdefer gpa.destroy(self);
        self.* = .{ .gpa = gpa, .transport = transport, .fd = fd };
        if (transport == .tls) try self.handshake();
        return self;
    }

    fn close(self: *Wire) void {
        _ = std.c.close(self.fd);
        self.plain.deinit(self.gpa);
        self.gpa.destroy(self);
    }

    fn handshake(self: *Wire) !void {
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
            const r = try hs.run(self.in[0..self.in_len], &self.out);
            self.consume(r.recv_pos);
            if (r.send.len > 0) try self.sendRaw(r.send);
            if (hs.done()) break;
            if (r.recv_pos == 0 and r.send.len == 0) try self.fill();
        }
        const alpn = hs.inner.alpn_protocol orelse return error.Alpn;
        if (!std.mem.eql(u8, alpn, "h2")) return error.Alpn;
        self.conn = .init(hs.cipher().?);
    }

    fn fill(self: *Wire) !void {
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

    fn consume(self: *Wire, n: usize) void {
        std.mem.copyForwards(u8, self.in[0 .. self.in_len - n], self.in[n..self.in_len]);
        self.in_len -= n;
    }

    fn sendRaw(self: *Wire, bytes: []const u8) !void {
        var off: usize = 0;
        while (off < bytes.len) {
            const n = std.c.send(self.fd, bytes[off..].ptr, bytes.len - off, 0);
            if (n <= 0) return error.Send;
            off += @intCast(n);
        }
    }

    /// Send HTTP/2 bytes, encrypting them for TLS.
    fn send(self: *Wire, bytes: []const u8) !void {
        if (self.transport == .h2c) return self.sendRaw(bytes);
        var off: usize = 0;
        while (off < bytes.len) {
            const r = try self.conn.encrypt(bytes[off..][0..@min(bytes.len - off, 16 * 1024)], &self.out);
            try self.sendRaw(r.ciphertext);
            off += r.cleartext_pos;
        }
    }

    fn recordLen(bytes: []const u8) ?usize {
        if (bytes.len < 5) return null;
        const len = 5 + @as(usize, std.mem.readInt(u16, bytes[3..5], .big));
        return if (bytes.len < len) null else len;
    }

    /// Read more plaintext into `plain`. False when the server closed (EOF or
    /// TLS close_notify). `error.WouldBlock` means the socket timeout passed.
    fn pump(self: *Wire) !bool {
        if (self.transport == .h2c) {
            var buf: [16 * 1024]u8 = undefined;
            const n = std.c.recv(self.fd, &buf, buf.len, 0);
            if (n == 0) return false;
            if (n < 0) {
                return switch (std.c.errno(n)) {
                    .AGAIN, .INTR => error.WouldBlock,
                    .CONNRESET => error.ConnectionReset,
                    else => error.Recv,
                };
            }
            try self.plain.appendSlice(self.gpa, buf[0..@intCast(n)]);
            return true;
        }
        const rec_len = recordLen(self.in[0..self.in_len]) orelse {
            self.fill() catch |err| switch (err) {
                error.Eof => return false,
                else => return err,
            };
            return true;
        };
        const res = try self.conn.decrypt(self.in[0..rec_len], &self.rd);
        self.consume(rec_len);
        if (res.closed) return false;
        try self.plain.appendSlice(self.gpa, res.cleartext);
        return true;
    }

    /// The next frame in wire order, or null when the server closed first.
    fn next(self: *Wire, deadline: u64) !?Seen {
        while (true) {
            if (self.plain.items.len >= frame.FRAME_HEADER_LEN) {
                const h = self.plain.items;
                const len = (@as(usize, h[0]) << 16) | (@as(usize, h[1]) << 8) | h[2];
                if (h.len >= 9 + len) {
                    const payload = h[9..][0..len];
                    const seen: Seen = .{
                        .ftype = h[3],
                        .flags = h[4],
                        .sid = std.mem.readInt(u32, h[5..9], .big) & 0x7fff_ffff,
                        .code = switch (h[3]) {
                            ftype_rst_stream => if (len >= 4) std.mem.readInt(u32, payload[0..4], .big) else 0,
                            ftype_goaway => if (len >= 8) std.mem.readInt(u32, payload[4..8], .big) else 0,
                            else => 0,
                        },
                        .last_stream_id = if (h[3] == ftype_goaway and len >= 8) std.mem.readInt(u32, payload[0..4], .big) & 0x7fff_ffff else 0,
                    };
                    self.plain.replaceRangeAssumeCapacity(0, 9 + len, &.{});
                    return seen;
                }
            }
            if (nowNs() >= deadline) return error.Deadline;
            const more = self.pump() catch |err| switch (err) {
                error.WouldBlock => continue,
                else => return err,
            };
            if (!more) return null;
        }
    }

    /// Read frames into `out` until `stop` says so, or the server closes.
    fn readUntil(self: *Wire, out: *Outcome, deadline: u64, comptime stop: ?fn (*const Outcome) bool) !void {
        while (true) {
            if (stop) |f| if (f(out)) return;
            const s = try self.next(deadline) orelse {
                out.closed = true;
                out.closed_ns = nowNs();
                return;
            };
            if (out.n == max_frames) return error.TooManyFrames;
            out.frames[out.n] = s;
            out.n += 1;
            if (out.end_ns == 0 and streamEnded(out, 1)) out.end_ns = nowNs();
        }
    }
};

/// True once a response on stream `sid` carries END_STREAM.
fn streamEnded(out: *const Outcome, sid: u32) bool {
    for (out.slice()) |s| {
        if ((s.ftype == ftype_data or s.ftype == ftype_headers) and s.sid == sid and s.flags & 0x1 != 0) return true;
    }
    return false;
}

fn stream1Ended(out: *const Outcome) bool {
    return streamEnded(out, 1);
}

fn stream1HasData(out: *const Outcome) bool {
    for (out.slice()) |s| {
        if (s.ftype == ftype_data and s.sid == 1) return true;
    }
    return false;
}

fn appendDataOnStream0(gpa: std.mem.Allocator, wire: *std.ArrayList(u8)) !void {
    var hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{ .length = 0, .type = .data, .flags = .{}, .stream_id = 0 }).encode(&hdr);
    try wire.appendSlice(gpa, &hdr);
}

const Scenario = enum {
    /// A request completes, then the client sends a connection error.
    connection_error,
    /// A request completes, then the server starts a graceful shutdown.
    graceful,
    /// A stream is still open when the graceful drain ends.
    graceful_open_stream,
    /// A stream finishes during phase 2, so the drain ends with nothing left
    /// to write.
    graceful_slow_stream,
    /// h2c only: the client sends something that is not the HTTP/2 preface.
    bad_preface,
};

fn clientMain(port: u16, gpa: std.mem.Allocator, transport: Transport, scenario: Scenario, out: *Outcome) void {
    runClient(port, gpa, transport, scenario, out) catch |err| {
        out.err = err;
    };
    out.done.store(true, .release);
}

fn runClient(port: u16, gpa: std.mem.Allocator, transport: Transport, scenario: Scenario, out: *Outcome) !void {
    const wire = try Wire.open(gpa, transport, port, null);
    defer wire.close();
    const deadline = nowNs() + client_deadline_ns;

    if (scenario == .bad_preface) {
        try wire.send("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
        try wire.readUntil(out, deadline, null);
        return;
    }

    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    switch (scenario) {
        .connection_error, .graceful => try h2c.appendHeaders(gpa, &req, 1, "/hello", true),
        .graceful_open_stream => try h2c.appendHeaders(gpa, &req, 1, "/sse", true),
        .graceful_slow_stream => try h2c.appendHeaders(gpa, &req, 1, "/slow", true),
        .bad_preface => unreachable,
    }
    try wire.send(req.items);
    switch (scenario) {
        .connection_error, .graceful => try wire.readUntil(out, deadline, stream1Ended),
        .graceful_open_stream => try wire.readUntil(out, deadline, stream1HasData),
        // The handler has started by the time the server sees the shutdown.
        .graceful_slow_stream => sleepMs(50),
        .bad_preface => unreachable,
    }
    if (out.closed) return;

    switch (scenario) {
        .bad_preface => unreachable,
        .connection_error => {
            var bad: std.ArrayList(u8) = .empty;
            defer bad.deinit(gpa);
            try appendDataOnStream0(gpa, &bad);
            try wire.send(bad.items);
            try wire.readUntil(out, deadline, null);
        },
        .graceful, .graceful_open_stream, .graceful_slow_stream => {
            out.ready.store(true, .release);
            // Phase 1 sends GOAWAY then a PING. Acking the PING moves the
            // server to phase 2 at once; the server's own guard is 1 s.
            const stop = struct {
                fn f(o: *const Outcome) bool {
                    for (o.slice()) |s| {
                        if (s.ftype == ftype_ping and s.flags & 0x1 == 0) return true;
                    }
                    return false;
                }
            }.f;
            try wire.readUntil(out, deadline, stop);
            if (out.closed) return;
            var ack: [17]u8 = undefined;
            (frame.FrameHeader{ .length = 8, .type = .ping, .flags = .{ .end_stream = true }, .stream_id = 0 }).encode(ack[0..9]);
            @memcpy(ack[9..], "sh2grace");
            try wire.send(&ack);
            try wire.readUntil(out, deadline, null);
        },
    }
}

fn runScenario(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, scenario: Scenario, label: []const u8) !void {
    const io = rt.io();
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangSse } } },
        .{ .method = .GET, .path = "/slow", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = slow } } },
    };
    var limits = starh2.Limits.defaults;
    limits.graceful_drain_timeout_ns = 300 * std.time.ns_per_ms;

    var cert_pem: []u8 = &.{};
    var key_pem: []u8 = &.{};
    defer {
        gpa.free(cert_pem);
        gpa.free(key_pem);
    }
    var endpoint: starh2.EndpointConfig = undefined;
    var tls_cfg: ?starh2.TlsConfig = null;
    switch (transport) {
        .h2c => endpoint = .{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) },
        .tls => {
            cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) };
            tls_cfg = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem };
        },
    }
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{endpoint},
        .routes = &routes,
        .tls = tls_cfg,
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

    var out: Outcome = .{};
    const t = try std.Thread.spawn(.{}, clientMain, .{ server.localAddress(0).getPort(), gpa, transport, scenario, &out });
    const graceful = scenario == .graceful or scenario == .graceful_open_stream or scenario == .graceful_slow_stream;
    if (graceful) {
        while (!out.ready.load(.acquire) and !out.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
        server.requestShutdown();
    }
    // Wait inside zio so this executor keeps serving the connection.
    while (!out.done.load(.acquire)) zio.sleep(.fromMilliseconds(10)) catch {};
    t.join();
    if (graceful) {
        serve_handle.join() catch {};
        serving = false;
    }
    errdefer out.dump(label);
    if (out.err) |err| return err;
    try check(scenario, &out);
}

/// Frame-order contract shared by all scenarios: the server SETTINGS first
/// (without ACK), nothing after the final frame, and a close.
fn check(scenario: Scenario, out: *const Outcome) !void {
    const seen = out.slice();
    if (seen.len == 0) return error.NoFrames;
    if (seen[0].ftype != ftype_settings or seen[0].flags & 0x1 != 0 or seen[0].sid != 0) return error.FirstFrameNotServerSettings;
    if (!out.closed) return error.NoClose;
    switch (scenario) {
        // The response, then GOAWAY PROTOCOL_ERROR naming the one stream the
        // server processed, and nothing after it.
        .connection_error => {
            if (!streamEnded(out, 1)) return error.NoResponse;
            const last = seen[seen.len - 1];
            if (last.ftype != ftype_goaway) return error.GoawayNotLast;
            if (last.code != @backingInt(frame.ErrorCode.protocol_error)) return error.WrongGoawayCode;
            if (last.last_stream_id != 1) return error.WrongLastStreamId;
            for (seen[0 .. seen.len - 1]) |s| {
                if (s.ftype == ftype_goaway) return error.TwoGoaways;
            }
        },
        // The server SETTINGS, then GOAWAY PROTOCOL_ERROR with last stream 0.
        .bad_preface => {
            const last = seen[seen.len - 1];
            if (last.ftype != ftype_goaway) return error.GoawayNotLast;
            if (last.code != @backingInt(frame.ErrorCode.protocol_error)) return error.WrongGoawayCode;
            if (last.last_stream_id != 0) return error.WrongLastStreamId;
        },
        // Phase 1: GOAWAY NO_ERROR with the maximum stream id, then a PING.
        // Phase 2: GOAWAY NO_ERROR naming stream 1. Nothing after it.
        .graceful => {
            var goaways: usize = 0;
            for (seen) |s| {
                if (s.ftype != ftype_goaway) continue;
                if (s.code != 0) return error.WrongGoawayCode;
                if (goaways == 0 and s.last_stream_id != std.math.maxInt(u31)) return error.Phase1WrongLastStreamId;
                if (goaways == 1 and s.last_stream_id != 1) return error.Phase2WrongLastStreamId;
                goaways += 1;
            }
            if (goaways != 2) return error.WrongGoawayCount;
            if (seen[seen.len - 1].ftype != ftype_goaway) return error.GoawayNotLast;
        },
        // Both GOAWAYs, then the slow response, then the close. The drain ends
        // with nothing left to write, so the close must follow the response at
        // once and not wait out `close_flush_timeout_ns` (500 ms): a closing
        // phase that begins with nothing queued has no write completion to wake it.
        .graceful_slow_stream => {
            var goaways: usize = 0;
            for (seen) |s| {
                if (s.ftype == ftype_goaway) goaways += 1;
                if (s.ftype == ftype_rst_stream) return error.UnexpectedRst;
            }
            if (goaways != 2) return error.WrongGoawayCount;
            if (!streamEnded(out, 1)) return error.NoResponse;
            const gap_ms = (out.closed_ns - out.end_ns) / std.time.ns_per_ms;
            std.debug.print("graceful slow stream: closed {d} ms after the response ended\n", .{gap_ms});
            if (gap_ms >= 250) return error.CloseWaitedForDeadline;
        },
        // As above, then the drain ends with the SSE stream still open: the
        // server resets it with CANCEL, and that RST_STREAM is the last frame.
        .graceful_open_stream => {
            var goaways: usize = 0;
            for (seen) |s| {
                if (s.ftype == ftype_goaway) goaways += 1;
            }
            if (goaways != 2) return error.WrongGoawayCount;
            const last = seen[seen.len - 1];
            if (last.ftype != ftype_rst_stream or last.sid != 1) return error.RstNotLast;
            if (last.code != @backingInt(frame.ErrorCode.cancel)) return error.WrongRstCode;
        },
    }
}

fn newRuntime(gpa: std.mem.Allocator) !*zio.Runtime {
    return zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
}

fn runCase(transport: Transport, scenario: Scenario, label: []const u8) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runScenario, .{ rt, gpa, transport, scenario, label });
    try h.join();
}

test "goaway: h2c steady state, a connection error delivers SETTINGS, response, GOAWAY, close (t-3520)" {
    try runCase(.h2c, .connection_error, "h2c connection error");
}

test "goaway: tls steady state, a connection error delivers SETTINGS, response, GOAWAY, close (t-3520)" {
    try runCase(.tls, .connection_error, "tls connection error");
}

test "goaway: h2c graceful shutdown delivers both GOAWAYs and the close (t-3520)" {
    try runCase(.h2c, .graceful, "h2c graceful");
}

test "goaway: tls graceful shutdown delivers both GOAWAYs and the close (t-3520)" {
    try runCase(.tls, .graceful, "tls graceful");
}

test "goaway: h2c graceful drain that ends with an open stream delivers its RST_STREAM (t-3520)" {
    try runCase(.h2c, .graceful_open_stream, "h2c graceful open stream");
}

test "goaway: tls graceful drain that ends with an open stream delivers its RST_STREAM (t-3520)" {
    try runCase(.tls, .graceful_open_stream, "tls graceful open stream");
}

test "goaway: h2c a bad preface delivers its GOAWAY (t-3520)" {
    try runCase(.h2c, .bad_preface, "h2c bad preface");
}

// ---------------------------------------------------------------------------
// A peer or a writer that never makes progress.
// ---------------------------------------------------------------------------

/// How the test stops the server's output.
const Stall = enum {
    /// A real client that asks for an endless stream and never reads. The
    /// server's socket fills and its writes stop. Used on TLS only: on h2c the
    /// actor can park in `write_ch.putOne` BEFORE it reads the connection
    /// error, which holds the connection with no closing phase involved (see
    /// the report for t-3520), so that shape cannot test the deadline.
    peer_never_reads,
    /// As `peer_never_reads`, but the handler outlives the sweep (it ignores the
    /// terminal cause until the task is canceled) and the client keeps sending
    /// PINGs after the connection error. The TLS actor then never parks: every
    /// turn drops inbound plaintext and, with a task handler alive, yields.
    /// That yield is the only cancellation point it reaches, which is what the
    /// cancel test needs (t-3520, round 2).
    peer_floods_input,
    /// The h2c WritePump sleeps before every write (`test_write_delay_ms`), so
    /// no chunk ever completes while the actor stays live.
    writer_never_completes,
    /// As above, after `fill_requests` responses have queued behind the stuck
    /// write, so the h2c write queue is full when the GOAWAY is drained.
    queue_full,
    /// The queue is left with exactly the room one pop is reserved, and one
    /// drain has more to push than that. `overflow_streams` streams hold DATA
    /// behind a zero window while the writer is live; then the writer is held
    /// (`wire_pump.test_hold_writes`), `fill_pings` PINGs fill the queue, and
    /// one client write opens the windows and sends the connection error.
    drain_overflow,
    /// No connection error. The writer is held and the h2c write queue filled
    /// with PING ACKs, then a request for a complete (inline) handler arrives:
    /// its response drain parks the actor in `putOne` from `runPendingInline`.
    /// The test then cancels the server. (t-3520, round 3)
    inline_drain_parked,
    /// As above, but the actor parks in the main loop's `drainEmit`: an endless
    /// SSE stream fills the queue behind the held writer.
    main_drain_parked,
};

/// Responses queued behind the stuck first write for `Stall.queue_full`. The
/// queue holds `inbound_wire_chunks_per_connection` (8) chunks: one SETTINGS
/// ACK, then one chunk per response.
const fill_requests = 7;

/// Slots in the default h2c write queue (`inbound_wire_chunks_per_connection`).
const write_queue_slots = 8;

/// For `Stall.drain_overflow`. The held WritePump has the first PING's ACK in
/// hand, so the other `fill_pings - 1` ACKs queue: 5 of 8, and 3 are free, the
/// room one pop is reserved. The drain then pushes the batch with the GOAWAY
/// and one DATA frame per stream: 5 pushes.
const overflow_streams = 4;
const fill_pings = 6;

/// Bytes the /flood handler has written. It stops growing when the server's
/// socket is full, which is how the test knows the peer has stalled the server.
var flood_bytes: std.atomic.Value(usize) = .init(0);

/// How long `stubborn` keeps running after the connection tells it to stop.
/// Teardown waits for handlers to exit and does not force them, so this is
/// bounded.
const stubborn_ms = 800;

/// Writes until the connection tells it to stop, then ignores that for
/// `stubborn_ms`, like a handler that is slow to notice. Keeps
/// `live_task_handlers` above zero while the closing phase runs.
fn stubborn(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    var chunk: [4096]u8 = @splat('x');
    while (true) {
        body.writeAll(&chunk) catch |err| {
            // The terminal cause is ignored on purpose; a task cancel is not.
            if (err == error.Canceled) return err;
            break;
        };
        _ = flood_bytes.fetchAdd(chunk.len, .monotonic);
    }
    try zio.sleep(.fromMilliseconds(stubborn_ms));
}

fn flood(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    var chunk: [4096]u8 = @splat('x');
    while (true) {
        try body.writeAll(&chunk);
        _ = flood_bytes.fetchAdd(chunk.len, .monotonic);
    }
}

const Stalled = struct {
    /// Client -> test body: the stall is set up from the client's side.
    ready: std.atomic.Value(bool) = .init(false),
    /// Test body -> client: send the connection error now.
    go: std.atomic.Value(bool) = .init(false),
    /// Client -> test body: the connection error is on the wire, at `sent_ns`.
    sent: std.atomic.Value(bool) = .init(false),
    sent_ns: std.atomic.Value(u64) = .init(0),
    /// Test body -> client: close and finish.
    release: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
};

fn sleepMs(ms: u32) void {
    const t: std.c.timespec = .{ .sec = 0, .nsec = @as(isize, ms) * std.time.ns_per_ms };
    _ = std.c.nanosleep(&t, null);
}

fn stalledMain(port: u16, gpa: std.mem.Allocator, transport: Transport, stall: Stall, st: *Stalled) void {
    stalledClient(port, gpa, transport, stall, st) catch |err| {
        st.err = err;
    };
    st.done.store(true, .release);
}

fn stalledClient(port: u16, gpa: std.mem.Allocator, transport: Transport, stall: Stall, st: *Stalled) !void {
    // A small receive buffer makes a non-reading client stall the server
    // after a few KiB.
    const wire = try Wire.open(gpa, transport, port, if (stall == .peer_never_reads or stall == .peer_floods_input) 4096 else null);
    defer wire.close();
    switch (stall) {
        .peer_never_reads, .peer_floods_input => {
            // An endless stream with a huge window, then never read.
            var req = try h2c.buildClientPreface(gpa, .empty);
            defer req.deinit(gpa);
            var sbuf: [64]u8 = undefined;
            const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 1 << 30 }};
            const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
            try req.appendSlice(gpa, sbuf[0..sn]);
            try h2c.appendWindowUpdate(gpa, &req, 0, 1 << 30);
            try h2c.appendHeaders(gpa, &req, 1, if (stall == .peer_floods_input) "/stubborn" else "/flood", true);
            try wire.send(req.items);
        },
        .drain_overflow => {
            // Initial window 0: the responses' DATA waits in the scheduler.
            var req: std.ArrayList(u8) = .empty;
            defer req.deinit(gpa);
            try req.appendSlice(gpa, frame.CLIENT_PREFACE);
            var sbuf: [64]u8 = undefined;
            const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 0 }};
            const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
            try req.appendSlice(gpa, sbuf[0..sn]);
            try wire.send(req.items);
            for (0..overflow_streams) |i| {
                sleepMs(40);
                var one: std.ArrayList(u8) = .empty;
                defer one.deinit(gpa);
                try h2c.appendHeaders(gpa, &one, @intCast(1 + 2 * i), "/sse", true);
                try wire.send(one.items);
            }
            // Let the handlers write their (blocked) DATA, then hold the
            // writer and fill the queue, one PING ACK per chunk.
            sleepMs(300);
            wire_pump.test_hold_writes.store(true, .release);
            for (0..fill_pings) |_| {
                sleepMs(40);
                var ping: [17]u8 = undefined;
                (frame.FrameHeader{ .length = 8, .type = .ping, .flags = .{}, .stream_id = 0 }).encode(ping[0..9]);
                @memcpy(ping[9..], "fillfill");
                try wire.send(&ping);
            }
        },
        .inline_drain_parked => {
            var req = try h2c.buildClientPrefaceAndSettings(gpa);
            defer req.deinit(gpa);
            try wire.send(req.items);
            sleepMs(300);
            wire_pump.test_hold_writes.store(true, .release);
            // The held WritePump keeps the first ACK in hand; the next
            // `write_queue_slots` fill the queue.
            for (0..1 + write_queue_slots) |_| {
                sleepMs(40);
                var ping: [17]u8 = undefined;
                (frame.FrameHeader{ .length = 8, .type = .ping, .flags = .{}, .stream_id = 0 }).encode(ping[0..9]);
                @memcpy(ping[9..], "fillfill");
                try wire.send(&ping);
            }
            sleepMs(40);
            var one: std.ArrayList(u8) = .empty;
            defer one.deinit(gpa);
            try h2c.appendHeaders(gpa, &one, 1, "/hello", true);
            try wire.send(one.items);
        },
        .main_drain_parked => {
            var req = try h2c.buildClientPreface(gpa, .empty);
            defer req.deinit(gpa);
            var sbuf: [64]u8 = undefined;
            const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 1 << 30 }};
            const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
            try req.appendSlice(gpa, sbuf[0..sn]);
            try h2c.appendWindowUpdate(gpa, &req, 0, 1 << 30);
            try h2c.appendHeaders(gpa, &req, 1, "/flood", true);
            try wire.send(req.items);
            // Let the stream run, then hold the writer: the queue fills and the
            // actor parks in its drain.
            sleepMs(300);
            wire_pump.test_hold_writes.store(true, .release);
        },
        .writer_never_completes, .queue_full => {
            var req = try h2c.buildClientPrefaceAndSettings(gpa);
            defer req.deinit(gpa);
            try wire.send(req.items);
            if (stall == .queue_full) {
                // One request at a time, so each response is its own chunk.
                for (0..fill_requests) |i| {
                    sleepMs(40);
                    var one: std.ArrayList(u8) = .empty;
                    defer one.deinit(gpa);
                    try h2c.appendHeaders(gpa, &one, @intCast(1 + 2 * i), "/hello", true);
                    try wire.send(one.items);
                }
            }
        },
    }
    st.ready.store(true, .release);

    if (stall == .inline_drain_parked or stall == .main_drain_parked) {
        // No connection error: the test cancels the server instead.
        while (!st.release.load(.acquire)) sleepMs(5);
        return;
    }
    while (!st.go.load(.acquire)) sleepMs(2);
    var bad: std.ArrayList(u8) = .empty;
    defer bad.deinit(gpa);
    // For `drain_overflow` the window updates come first, in the same write,
    // so the DATA is eligible when the connection error is ingested.
    if (stall == .drain_overflow) {
        for (0..overflow_streams) |i| try h2c.appendWindowUpdate(gpa, &bad, @intCast(1 + 2 * i), 65535);
    }
    try appendDataOnStream0(gpa, &bad);
    try wire.send(bad.items);
    st.sent_ns.store(nowNs(), .release);
    st.sent.store(true, .release);
    if (stall == .peer_floods_input) {
        // Keep the server's inbound side busy; it drops all of it.
        var ping: [17]u8 = undefined;
        (frame.FrameHeader{ .length = 8, .type = .ping, .flags = .{}, .stream_id = 0 }).encode(ping[0..9]);
        @memcpy(ping[9..], "floodinp");
        while (!st.release.load(.acquire)) wire.send(&ping) catch break;
    }
    while (!st.release.load(.acquire)) sleepMs(5);
}

const StalledCheck = enum {
    /// The connection must close at the deadline, not before and not long after.
    deadline,
    /// A cancel during the closing phase ends the connection at once.
    cancel,
};

fn runStalled(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, stall: Stall, check_kind: StalledCheck, label: []const u8) !void {
    const io = rt.io();
    const conn_mod = starh2.edge.connection;
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/flood", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = flood } } },
        .{ .method = .GET, .path = "/stubborn", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = stubborn } } },
        .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangSse } } },
    };
    var limits = starh2.Limits.defaults;
    // The cancel check needs a deadline far enough away that "ended at once"
    // cannot be mistaken for "ended at the deadline".
    const deadline_ns: u64 = switch (check_kind) {
        .deadline => 600 * std.time.ns_per_ms,
        .cancel => 30 * std.time.ns_per_s,
    };
    limits.close_flush_timeout_ns = deadline_ns;

    // Read by the connection when its WritePump starts, so it must be set
    // before the client connects and cleared afterwards.
    conn_mod.test_write_delay_ms = switch (stall) {
        .peer_never_reads, .peer_floods_input, .drain_overflow, .inline_drain_parked, .main_drain_parked => 0,
        .writer_never_completes, .queue_full => 120 * std.time.ms_per_s,
    };
    defer conn_mod.test_write_delay_ms = 0;
    defer wire_pump.test_hold_writes.store(false, .release);

    var cert_pem: []u8 = &.{};
    var key_pem: []u8 = &.{};
    defer {
        gpa.free(cert_pem);
        gpa.free(key_pem);
    }
    var endpoint: starh2.EndpointConfig = undefined;
    var tls_cfg: ?starh2.TlsConfig = null;
    switch (transport) {
        .h2c => endpoint = .{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) },
        .tls => {
            cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) };
            tls_cfg = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem };
        },
    }
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{endpoint},
        .routes = &routes,
        .tls = tls_cfg,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        // A connection stuck in the closing phase ignores the shutdown event;
        // a cancel is what ends it.
        serve_handle.cancel();
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    flood_bytes.store(0, .release);
    const deferred_before = conn_mod.test_closing_emit_deferred.load(.acquire);
    var st: Stalled = .{};
    const t = try std.Thread.spawn(.{}, stalledMain, .{ server.localAddress(0).getPort(), gpa, transport, stall, &st });
    defer {
        st.release.store(true, .release);
        t.join();
    }

    const give_up = nowNs() + client_deadline_ns;
    while (!st.ready.load(.acquire)) {
        if (st.err) |err| return err;
        if (nowNs() >= give_up) return error.ClientNeverReady;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    switch (stall) {
        .peer_never_reads, .peer_floods_input => {
            // The server is stalled when the flood handler has written
            // something and then makes no progress for 500 ms. Without this
            // the connection error could arrive while the socket still has
            // room, and the GOAWAY would be written at once: a pass that
            // proves nothing.
            var last_bytes: usize = 0;
            var still_since: u64 = nowNs();
            while (true) {
                if (st.err) |err| return err;
                if (nowNs() >= give_up) return error.ServerNeverStalled;
                zio.sleep(.fromMilliseconds(50)) catch {};
                const now_bytes = flood_bytes.load(.acquire);
                if (now_bytes != last_bytes) {
                    last_bytes = now_bytes;
                    still_since = nowNs();
                } else if (now_bytes > 0 and nowNs() - still_since >= 500 * std.time.ns_per_ms) break;
            }
            std.debug.print("{s}: server stalled after {d} flood bytes\n", .{ label, last_bytes });
        },
        // Let the server take the requests and queue its responses.
        .writer_never_completes, .queue_full, .drain_overflow => zio.sleep(.fromMilliseconds(300)) catch {},
        .inline_drain_parked, .main_drain_parked => zio.sleep(.fromMilliseconds(600)) catch {},
    }

    if (stall == .inline_drain_parked or stall == .main_drain_parked) {
        // The actor is parked in `putOne` behind a held writer. Cancel the
        // server: `error.Canceled` must reach `Connection.run`, which counts it.
        try std.testing.expectEqual(@as(usize, 1), server.active_connections.load(.acquire));
        const canceled_before = conn_mod.test_run_canceled.load(.acquire);
        const inline_before = conn_mod.test_inline_drain_canceled.load(.acquire);
        const t_cancel = nowNs();
        serve_handle.cancel();
        serving = false;
        const cancel_ms = (nowNs() - t_cancel) / std.time.ns_per_ms;
        const reached_run = conn_mod.test_run_canceled.load(.acquire) - canceled_before;
        const inline_saw = conn_mod.test_inline_drain_canceled.load(.acquire) - inline_before;
        std.debug.print("{s}: canceled connection ended in {d} ms; error.Canceled reached run {d} time(s); runPendingInline saw it {d} time(s)\n", .{ label, cancel_ms, reached_run, inline_saw });
        try std.testing.expectEqual(@as(usize, 0), server.active_connections.load(.acquire));
        try std.testing.expect(cancel_ms <= 2000);
        try std.testing.expectEqual(@as(usize, 1), reached_run);
        // The two tests prove different paths: only the inline one parks the
        // actor in `runPendingInline`'s drain.
        try std.testing.expectEqual(@as(usize, if (stall == .inline_drain_parked) 1 else 0), inline_saw);
        return;
    }

    st.go.store(true, .release);
    while (!st.sent.load(.acquire)) {
        if (st.err) |err| return err;
        zio.sleep(.fromMilliseconds(1)) catch {};
    }
    const t0 = st.sent_ns.load(.acquire);

    switch (check_kind) {
        .deadline => {
            const stop = t0 + deadline_ns + 3 * std.time.ns_per_s;
            while (server.active_connections.load(.acquire) != 0) {
                if (nowNs() >= stop) return error.HeldPastDeadline;
                zio.sleep(.fromMilliseconds(5)) catch {};
            }
            const held_ms = (nowNs() - t0) / std.time.ns_per_ms;
            std.debug.print("{s}: closed {d} ms after the connection error (deadline {d} ms)\n", .{ label, held_ms, deadline_ns / std.time.ns_per_ms });
            // Not before the deadline: the GOAWAY could not be written, so the
            // phase had to wait for it. Not long after: the margin covers the
            // poll step, the handler cancel and the socket teardown.
            try std.testing.expect(held_ms >= deadline_ns / std.time.ns_per_ms);
            try std.testing.expect(held_ms <= deadline_ns / std.time.ns_per_ms + 750);
        },
        .cancel => {
            // Let the phase begin and park, then cancel the connection.
            zio.sleep(.fromMilliseconds(300)) catch {};
            try std.testing.expectEqual(@as(usize, 1), server.active_connections.load(.acquire));
            const t_cancel = nowNs();
            serve_handle.cancel();
            serving = false;
            const cancel_ms = (nowNs() - t_cancel) / std.time.ns_per_ms;
            std.debug.print("{s}: canceled connection ended in {d} ms (deadline {d} ms)\n", .{ label, cancel_ms, deadline_ns / std.time.ns_per_ms });
            try std.testing.expectEqual(@as(usize, 0), server.active_connections.load(.acquire));
            // Far below the 30 s deadline.
            try std.testing.expect(cancel_ms <= 2000);
        },
    }
    if ((stall == .queue_full or stall == .drain_overflow) and check_kind == .deadline) {
        // The hold-back ran: without it the GOAWAY drain would have parked the
        // actor in `putOne` and the deadline could not have fired.
        const deferred = conn_mod.test_closing_emit_deferred.load(.acquire) - deferred_before;
        std.debug.print("{s}: closing drains held back for a full write queue: {d}\n", .{ label, deferred });
        try std.testing.expect(deferred > 0);
    }
}

fn runStalledCase(transport: Transport, stall: Stall, check_kind: StalledCheck, label: []const u8) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runStalled, .{ rt, gpa, transport, stall, check_kind, label });
    try h.join();
}

test "goaway: tls a peer that never reads holds the connection only until the deadline (t-3520)" {
    try runStalledCase(.tls, .peer_never_reads, .deadline, "tls peer never reads");
}

test "goaway: h2c a writer that never completes holds the connection only until the deadline (t-3520)" {
    try runStalledCase(.h2c, .writer_never_completes, .deadline, "h2c writer never completes");
}

test "goaway: h2c a full write queue does not park the closing phase past the deadline (t-3520)" {
    try runStalledCase(.h2c, .queue_full, .deadline, "h2c full write queue");
}

test "goaway: tls a cancel in the closing phase ends the connection at once (t-3520)" {
    try runStalledCase(.tls, .peer_never_reads, .cancel, "tls cancel while closing");
}

test "goaway: h2c a cancel in the closing phase ends the connection at once (t-3520)" {
    try runStalledCase(.h2c, .writer_never_completes, .cancel, "h2c cancel while closing");
}

test "goaway: h2c a graceful drain that ends with nothing queued closes at once (t-3520)" {
    try runCase(.h2c, .graceful_slow_stream, "h2c graceful slow stream");
}

test "goaway: tls a graceful drain that ends with nothing queued closes at once (t-3520)" {
    try runCase(.tls, .graceful_slow_stream, "tls graceful slow stream");
}

test "goaway: h2c one closing drain with more to push than the reserved room does not park past the deadline (t-3520)" {
    try runStalledCase(.h2c, .drain_overflow, .deadline, "h2c drain overflow");
}

test "goaway: tls a cancel reaches the actor through the inbound-batch yield while closing (t-3520)" {
    try runStalledCase(.tls, .peer_floods_input, .cancel, "tls cancel with inbound flood");
}

test "goaway: h2c a cancel while an inline drain is parked in putOne reaches run as error.Canceled (t-3520)" {
    try runStalledCase(.h2c, .inline_drain_parked, .cancel, "h2c cancel in inline drain");
}

test "goaway: h2c a cancel while the main loop drain is parked in putOne reaches run as error.Canceled (t-3520)" {
    try runStalledCase(.h2c, .main_drain_parked, .cancel, "h2c cancel in main drain");
}
