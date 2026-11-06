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
/// Under ThreadSanitizer, allocators that never call `mremap` (t-3837).
const tsan_allocator = @import("tsan_allocator.zig");
const zio = @import("zio");
const serve_deadline = @import("serve_deadline.zig");
const wire_client = @import("wire_client.zig");
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
        return openAlpn(gpa, transport, port, rcvbuf, "h2");
    }

    /// `open`, offering `alpn` on TLS (`http/1.1` for an HTTP/1.1 client).
    fn openAlpn(gpa: std.mem.Allocator, transport: Transport, port: u16, rcvbuf: ?c_int, alpn: []const u8) !*Wire {
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
        if (transport == .tls) try self.handshake(alpn);
        return self;
    }

    fn close(self: *Wire) void {
        _ = std.c.close(self.fd);
        self.plain.deinit(self.gpa);
        self.gpa.destroy(self);
    }

    fn handshake(self: *Wire, want_alpn: []const u8) !void {
        var prng = std.Random.DefaultPrng.init(@truncate(@as(u128, @bitCast(@as(i128, realNow().nanoseconds)))));
        var hs = tls.nonblock.Client.init(.{
            .rng = prng.random(),
            .now = realNow(),
            .host = "localhost",
            .root_ca = .empty,
            .insecure_skip_verify = true,
            .cipher_suites = tls.config.cipher_suites.tls13,
            .alpn_protocols = &.{want_alpn},
        });
        while (!hs.done()) {
            const r = try hs.run(self.in[0..self.in_len], &self.out);
            self.consume(r.recv_pos);
            if (r.send.len > 0) try self.sendRaw(r.send);
            if (hs.done()) break;
            if (r.recv_pos == 0 and r.send.len == 0) try self.fill();
        }
        const alpn = hs.inner.alpn_protocol orelse return error.Alpn;
        if (!std.mem.eql(u8, alpn, want_alpn)) return error.Alpn;
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
    /// The client reads the server's preface and holds back its own, and the
    /// server shuts down meanwhile. The server's SETTINGS already went out,
    /// so this is an HTTP/2 connection: it must get a GOAWAY before the close
    /// (found by DST stage 3, `dst-conn` seed 2).
    shutdown_before_preface,
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
    if (scenario == .shutdown_before_preface) {
        const settings_seen = struct {
            fn f(o: *const Outcome) bool {
                for (o.slice()) |s| {
                    if (s.ftype == ftype_settings and s.flags & 0x1 == 0) return true;
                }
                return false;
            }
        }.f;
        try wire.readUntil(out, deadline, settings_seen);
        if (out.closed) return;
        // The client preface is never sent: the server shuts down while it
        // still waits for it.
        out.ready.store(true, .release);
        try wire.readUntil(out, deadline, null);
        return;
    }

    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    switch (scenario) {
        .connection_error, .graceful => try h2c.appendHeaders(gpa, &req, 1, "/hello", true),
        .graceful_open_stream => try h2c.appendHeaders(gpa, &req, 1, "/sse", true),
        .graceful_slow_stream => try h2c.appendHeaders(gpa, &req, 1, "/slow", true),
        .bad_preface, .shutdown_before_preface => unreachable,
    }
    try wire.send(req.items);
    switch (scenario) {
        .connection_error, .graceful => try wire.readUntil(out, deadline, stream1Ended),
        .graceful_open_stream => try wire.readUntil(out, deadline, stream1HasData),
        // The handler has started by the time the server sees the shutdown.
        .graceful_slow_stream => sleepMs(50),
        .bad_preface, .shutdown_before_preface => unreachable,
    }
    if (out.closed) return;

    switch (scenario) {
        .bad_preface, .shutdown_before_preface => unreachable,
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
        .h2c => endpoint = .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .tls => {
            cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } };
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
        serve_deadline.join(&server, &serve_handle);
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var out: Outcome = .{};
    const t = try std.Thread.spawn(.{}, clientMain, .{ server.localAddress(0).getPort(), gpa, transport, scenario, &out });
    const graceful = scenario == .graceful or scenario == .graceful_open_stream or scenario == .graceful_slow_stream or scenario == .shutdown_before_preface;
    if (graceful) {
        while (!out.ready.load(.acquire) and !out.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
        server.requestShutdown();
    }
    // Wait inside zio so this executor keeps serving the connection.
    while (!out.done.load(.acquire)) zio.sleep(.fromMilliseconds(10)) catch {};
    starh2.test_sleep.joinThread(t);
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
        // The server's preface went out, so the shutdown ends in a GOAWAY
        // NO_ERROR, the last frame, naming stream 0 (no stream was opened).
        .shutdown_before_preface => {
            var goaways: usize = 0;
            for (seen) |s| {
                if (s.ftype != ftype_goaway) continue;
                if (s.code != 0) return error.WrongGoawayCode;
                goaways += 1;
            }
            if (goaways == 0) return error.NoGoaway;
            const last = seen[seen.len - 1];
            if (last.ftype != ftype_goaway) return error.GoawayNotLast;
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
    const gpa = tsan_allocator.allocator();
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

test "goaway: h2c shutdown while the server waits for the client preface delivers a GOAWAY (dst-conn seed 2)" {
    try runCase(.h2c, .shutdown_before_preface, "h2c shutdown before preface");
}

test "goaway: tls shutdown while the server waits for the client preface delivers a GOAWAY (dst-conn seed 2)" {
    try runCase(.tls, .shutdown_before_preface, "tls shutdown before preface");
}

// ---------------------------------------------------------------------------
// A peer or a writer that never makes progress.
// ---------------------------------------------------------------------------

/// How the test stops the server's output.
const Stall = enum {
    /// A real client that asks for an endless stream and never reads. The
    /// server's socket fills and its writes stop, and the actor must stay live
    /// to read what the client sends next. On h2c this needs the room checks
    /// before every pop: when a full write queue parked the actor in `putOne`,
    /// it never read the connection error.
    peer_never_reads,
    /// As `peer_never_reads`, but the handler outlives the sweep (it ignores the
    /// terminal cause until the task is canceled) and the client keeps sending
    /// PINGs after the connection error. The TLS actor then never parks: every
    /// turn drops inbound plaintext and, with a task handler alive, yields.
    /// That yield is the only cancellation point it reaches, which is what the
    /// cancel test needs (t-3520, round 2).
    peer_floods_input,
    /// A real client that asks for an endless stream, grants it no window
    /// (SETTINGS_INITIAL_WINDOW_SIZE 0) and reads nothing. The stream's DATA
    /// waits in the scheduler on the window the client holds, so the
    /// response clock of that stream (`fair_scheduler.Stall`) is what ends
    /// it, and the socket is not stalled: the connection stays. A peer that
    /// stops reading its socket loses its connection instead, at the
    /// write-byte timeout (`tests/hardening.zig`).
    peer_holds_window,
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
    /// its response drain is held back by the write pool (it used to park the
    /// actor in `putOne`; the pool check stops it before a push can wait), and
    /// `runPendingInline` returns. The test then cancels the server, which the
    /// actor sees in its select. (t-3520, round 3)
    inline_drain_parked,
    /// As above, but the held drain is the main loop's `drainEmit`: an endless
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
/// When the /flood handler last finished a write. The stall poll sees a byte
/// change late on a loaded machine, so it cannot time the kill.
var flood_last_write_ns: std.atomic.Value(u64) = .init(0);
/// When the /flood handler's write returned `error.SlowConsumer`; 0 until then.
var flood_killed_ns: std.atomic.Value(u64) = .init(0);
/// The /flood handler returned. A handler that stopped writing for any reason
/// looks like a stalled server to the byte count alone.
var flood_exited: std.atomic.Value(bool) = .init(false);

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
        body.writeAll(&chunk) catch |err| {
            if (err == error.SlowConsumer) flood_killed_ns.store(nowNs(), .release);
            flood_exited.store(true, .release);
            return err;
        };
        flood_last_write_ns.store(nowNs(), .release);
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
    /// Test body -> client: the stream was killed, so read until its reset and
    /// check the connection still answers (`expectResetThenPingAck`).
    read_reset: std.atomic.Value(bool) = .init(false),
    /// Client -> test body: the reset arrived and the PING was answered.
    survived: std.atomic.Value(bool) = .init(false),
    /// Test body -> client: close and finish.
    release: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
};

fn sleepMs(ms: u32) void {
    starh2.test_sleep.threadSleepMs(ms);
}

fn stalledMain(port: u16, gpa: std.mem.Allocator, transport: Transport, stall: Stall, st: *Stalled) void {
    stalledClient(port, gpa, transport, stall, st) catch |err| {
        st.err = err;
    };
    st.done.store(true, .release);
}

/// Reads past the DATA the server had already queued until stream 1's
/// RST_STREAM, then sends a PING: a connection in its closing phase drops
/// inbound frames, so an ACK proves the kill ended the stream and not the
/// connection.
fn expectResetThenPingAck(wire: *Wire) !void {
    const deadline = nowNs() + client_deadline_ns;
    while (true) {
        const s = try wire.next(deadline) orelse return error.ClosedBeforeReset;
        if (s.ftype == ftype_goaway) return error.GoawayBeforeReset;
        if (s.ftype == ftype_rst_stream and s.sid == 1) {
            if (s.code != @backingInt(frame.ErrorCode.cancel)) return error.WrongRstCode;
            break;
        }
    }
    var ping: [17]u8 = undefined;
    (frame.FrameHeader{ .length = 8, .type = .ping, .flags = .{}, .stream_id = 0 }).encode(ping[0..9]);
    @memcpy(ping[9..], "stillup?");
    try wire.send(&ping);
    while (true) {
        const s = try wire.next(deadline) orelse return error.ClosedBeforePingAck;
        if (s.ftype == ftype_goaway) return error.GoawayBeforePingAck;
        if (s.sid == 1) return error.FrameAfterReset;
        if (s.ftype == ftype_ping and s.flags & 0x1 != 0) return;
    }
}

fn stalledClient(port: u16, gpa: std.mem.Allocator, transport: Transport, stall: Stall, st: *Stalled) !void {
    // A small receive buffer makes a non-reading client stall the server
    // after a few KiB.
    const wire = try Wire.open(gpa, transport, port, if (stall == .peer_never_reads or stall == .peer_floods_input or stall == .peer_holds_window) 4096 else null);
    defer wire.close();
    switch (stall) {
        .peer_holds_window => {
            // An endless stream with no window, then never read.
            var req = try h2c.buildClientPreface(gpa, .empty);
            defer req.deinit(gpa);
            var sbuf: [64]u8 = undefined;
            const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 0 }};
            const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
            try req.appendSlice(gpa, sbuf[0..sn]);
            try h2c.appendWindowUpdate(gpa, &req, 0, 1 << 30);
            try h2c.appendHeaders(gpa, &req, 1, "/flood", true);
            try wire.send(req.items);
        },
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
            // Let the stream run, then hold the writer: the pool empties and
            // the drain is held back.
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
    // A check that sends nothing more ends with `release` and no `go`.
    while (!st.go.load(.acquire)) {
        if (st.release.load(.acquire)) return;
        if (st.read_reset.load(.acquire)) {
            try expectResetThenPingAck(wire);
            st.survived.store(true, .release);
            while (!st.release.load(.acquire)) sleepMs(5);
            return;
        }
        sleepMs(2);
    }
    var bad: std.ArrayList(u8) = .empty;
    defer bad.deinit(gpa);
    // For `drain_overflow` the window updates come first, in the same write,
    // so the DATA is eligible when the connection error is ingested.
    if (stall == .drain_overflow) {
        for (0..overflow_streams) |i| try h2c.appendWindowUpdate(gpa, &bad, @intCast(1 + 2 * i), 65535);
    }
    try appendDataOnStream0(gpa, &bad);
    // Before the send: a client descheduled after it would start the clock late,
    // and a close on time would read as early.
    st.sent_ns.store(nowNs(), .release);
    try wire.send(bad.items);
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
    /// The client sends nothing more, so the stream's own deadline is the only
    /// thing that can stop the handler: its write must fail with SlowConsumer.
    slow_consumer,
    /// As `slow_consumer`, and the peer stays silent: with no stream left the
    /// connection idle timeout must end the connection too.
    idle_after_kill,
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
        .deadline, .idle_after_kill => 600 * std.time.ns_per_ms,
        .cancel, .slow_consumer => 30 * std.time.ns_per_s,
    };
    limits.close_flush_timeout_ns = deadline_ns;
    // Above the 500 ms of no progress that says the server has stalled: the
    // handler must still be parked when the stall is seen.
    if (check_kind == .slow_consumer or check_kind == .idle_after_kill) limits.response_min_rate_grace_ns = 1500 * std.time.ns_per_ms;
    if (check_kind == .idle_after_kill) limits.connection_idle_timeout_ns = 1000 * std.time.ns_per_ms;

    // Read by the connection when its WritePump starts, so it must be set
    // before the client connects and cleared afterwards.
    conn_mod.test_write_delay_ms = switch (stall) {
        .peer_never_reads, .peer_floods_input, .peer_holds_window, .drain_overflow, .inline_drain_parked, .main_drain_parked => 0,
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
        .h2c => endpoint = .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .tls => {
            cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } };
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
    flood_last_write_ns.store(0, .release);
    flood_killed_ns.store(0, .release);
    flood_exited.store(false, .release);
    const deferred_before = conn_mod.test_closing_emit_deferred.load(.acquire);
    var st: Stalled = .{};
    const t = try std.Thread.spawn(.{}, stalledMain, .{ server.localAddress(0).getPort(), gpa, transport, stall, &st });
    defer {
        st.release.store(true, .release);
        starh2.test_sleep.joinThread(t);
    }

    const give_up = nowNs() + client_deadline_ns;
    var last_write_ns: u64 = 0;
    while (!st.ready.load(.acquire)) {
        if (st.err) |err| return err;
        if (nowNs() >= give_up) return error.ClientNeverReady;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    switch (stall) {
        .peer_never_reads, .peer_floods_input, .peer_holds_window => {
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
            // Parked in its write, not gone: this is what makes the flat byte
            // count a stalled server.
            if (flood_exited.load(.acquire)) return error.HandlerExitedBeforeStall;
            last_write_ns = flood_last_write_ns.load(.acquire);
            std.debug.print("{s}: server stalled after {d} flood bytes\n", .{ label, last_bytes });
        },
        // Let the server take the requests and queue its responses.
        .writer_never_completes, .queue_full, .drain_overflow => zio.sleep(.fromMilliseconds(300)) catch {},
        .inline_drain_parked, .main_drain_parked => zio.sleep(.fromMilliseconds(600)) catch {},
    }

    if (stall == .inline_drain_parked or stall == .main_drain_parked) {
        // The drain is held back behind a held writer, and the actor waits in
        // its select. Cancel the server: `error.Canceled` must reach
        // `Connection.run`, which counts it.
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
        // No drain parks the actor any more (the write-pool check stops a pop
        // that could push past the queue), so no cancel is delivered inside
        // `runPendingInline`'s drain.
        try std.testing.expectEqual(@as(usize, 0), inline_saw);
        return;
    }

    if (check_kind == .slow_consumer or check_kind == .idle_after_kill) {
        // The margin covers the actor's wake-up on a loaded machine. A deadline
        // at twice the limit does not fit in it.
        const stop = last_write_ns + limits.response_min_rate_grace_ns + 1 * std.time.ns_per_s;
        while (flood_killed_ns.load(.acquire) == 0) {
            if (nowNs() >= stop) return error.NoSlowConsumerKill;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        const killed_ms = (flood_killed_ns.load(.acquire) - last_write_ns) / std.time.ns_per_ms;
        std.debug.print("{s}: handler got SlowConsumer {d} ms after its last write (limit 1500 ms)\n", .{ label, killed_ms });
        // The stream's response clock fired, not something earlier or much
        // later: its DATA waited on the window the peer holds for the grace
        // (`Limits.response_min_rate_grace_ns`), from its first wait, which is
        // close to the handler's stamp, so the floor only allows for a stamp
        // taken late.
        try std.testing.expect(killed_ms >= limits.response_min_rate_grace_ns / std.time.ns_per_ms - 100);
        try std.testing.expect(killed_ms <= (stop - last_write_ns) / std.time.ns_per_ms);
        if (check_kind == .idle_after_kill) {
            // The idle time, then the first GOAWAY's PING that nobody answers,
            // then the close deadline for frames the full socket cannot take.
            const idle_ms = limits.connection_idle_timeout_ns / std.time.ns_per_ms;
            const killed_ns = flood_killed_ns.load(.acquire);
            const gone_by = killed_ns + limits.connection_idle_timeout_ns + 1 * std.time.ns_per_s + deadline_ns + 1 * std.time.ns_per_s;
            while (server.active_connections.load(.acquire) != 0) {
                if (nowNs() >= gone_by) return error.HeldPastIdleTimeout;
                zio.sleep(.fromMilliseconds(5)) catch {};
            }
            const gone_ms = (nowNs() - killed_ns) / std.time.ns_per_ms;
            std.debug.print("{s}: connection gone {d} ms after the kill (idle limit {d} ms)\n", .{ label, gone_ms, idle_ms });
            // The clock starts when the handler sees the kill, which is late on a
            // loaded machine.
            try std.testing.expect(gone_ms + 300 >= idle_ms + 1000 + deadline_ns / std.time.ns_per_ms);
            return;
        }
        // The peer learns of it, and only the stream ended.
        st.read_reset.store(true, .release);
        const read_stop = nowNs() + 2 * client_deadline_ns;
        while (!st.survived.load(.acquire)) {
            // `done` is what orders the read of `err`.
            if (st.done.load(.acquire)) return st.err orelse error.ClientEndedWithoutAck;
            if (nowNs() >= read_stop) return error.ClientNeverFinishedRead;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        try std.testing.expectEqual(@as(usize, 1), server.active_connections.load(.acquire));
        return;
    }

    st.go.store(true, .release);
    while (!st.sent.load(.acquire)) {
        if (st.err) |err| return err;
        zio.sleep(.fromMilliseconds(1)) catch {};
    }
    const t0 = st.sent_ns.load(.acquire);

    switch (check_kind) {
        .slow_consumer, .idle_after_kill => unreachable,
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
    if ((stall == .queue_full or stall == .drain_overflow or (transport == .h2c and stall == .peer_never_reads)) and check_kind == .deadline) {
        const deferred = conn_mod.test_closing_emit_deferred.load(.acquire) - deferred_before;
        std.debug.print("{s}: closing drains held back for no write room: {d}\n", .{ label, deferred });
        // The hold-back ran: without it the GOAWAY drain would have pushed into
        // a full queue and failed the connection before the deadline. Only the
        // stalls that fill the queue on purpose can require it: with a real peer
        // a macOS run can show none held back and still wait out the deadline.
        if (stall != .peer_never_reads) try std.testing.expect(deferred > 0);
    }
}

fn runStalledCase(transport: Transport, stall: Stall, check_kind: StalledCheck, label: []const u8) !void {
    const gpa = tsan_allocator.allocator();
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runStalled, .{ rt, gpa, transport, stall, check_kind, label });
    try h.join();
}

test "goaway: tls a peer that never reads holds the connection only until the deadline (t-3520)" {
    try runStalledCase(.tls, .peer_never_reads, .deadline, "tls peer never reads");
}

test "goaway: h2c a peer that never reads holds the connection only until the deadline (t-3523)" {
    try runStalledCase(.h2c, .peer_never_reads, .deadline, "h2c peer never reads");
}

test "goaway: h2c a peer that holds a stream's window loses that stream at the slow-consumer deadline (t-3521)" {
    try runStalledCase(.h2c, .peer_holds_window, .slow_consumer, "h2c slow consumer");
}

test "goaway: tls a peer that holds a stream's window loses that stream at the slow-consumer deadline (t-3521)" {
    try runStalledCase(.tls, .peer_holds_window, .slow_consumer, "tls slow consumer");
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

test "goaway: h2c a cancel while an inline drain is held back by the write pool reaches run as error.Canceled (t-3520)" {
    try runStalledCase(.h2c, .inline_drain_parked, .cancel, "h2c cancel in inline drain");
}

test "goaway: h2c a cancel while the main loop drain is held back by the write pool reaches run as error.Canceled (t-3520)" {
    try runStalledCase(.h2c, .main_drain_parked, .cancel, "h2c cancel in main drain");
}

// ---------------------------------------------------------------------------
// The connection idle timeout (t-3705)
// ---------------------------------------------------------------------------

const idle_limit_ms = 300;

const IdleCase = enum {
    /// The peer connects and never asks for anything, and answers nothing.
    silent,
    /// The peer sends PINGs and reads the ACKs, and opens no stream.
    pings,
    /// A stream runs for longer than the limit, and then the peer resets it.
    open_stream,
    /// The peer does not end its stream, so it stays open with no handler to wait for.
    unfinished_request,
    /// The peer resets a stream whose handler keeps running.
    handler_outlives_reset,
    /// Requests that each end quickly, over a time longer than the limit.
    requests,
    /// A request sent after the first GOAWAY.
    crosses_goaway,
    /// The limit is 0.
    off,
    /// A header block that takes longer than the limit to arrive.
    unfinished_headers,
    /// A stream sent after the first GOAWAY runs past the drain, so the server
    /// resets it. The server itself is not shutting down.
    cut_after_goaway,
    /// h2c: the writer is held, and a task handler's response drains after the
    /// check, so no write completion wakes the actor, and the handler waits for a
    /// receipt that never comes. It takes a task handler: an inline response ends
    /// its stream before the next check, which then sees the end. Not TLS: there
    /// the hold parks the actor itself, and that is not what a full socket does.
    stream_ends_with_no_wake,
};

const IdleRun = struct {
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
    /// Test body -> client: the server's side is checked, so close.
    release: std.atomic.Value(bool) = .init(false),
};

/// The cause `recordCause` read from its stream's terminal state; null until then.
var idle_cause_seen: std.atomic.Value(bool) = .init(false);
var idle_cause_was_closed: std.atomic.Value(bool) = .init(false);

fn recordCause(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    while (true) {
        // The connection's end also requests a cancel (t-3737), so the sleep
        // can end early: read the cause before the cancel is returned.
        const slept = zio.sleep(.fromMilliseconds(5));
        if (body.terminalCause()) |cause| {
            idle_cause_was_closed.store(cause == .connection_closed, .release);
            idle_cause_seen.store(true, .release);
            return error.Canceled;
        }
        try slept;
    }
}

/// Sends its response, which waits for the wire receipt, then records why that
/// wait ended.
fn sendThenRecordCause(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try zio.sleep(.fromMilliseconds(slow_handler_ms));
    resp.send(200, &.{}, "ok") catch {};
    if (resp.slotTerminal()) |cause| {
        idle_cause_was_closed.store(cause == .connection_closed, .release);
        idle_cause_seen.store(true, .release);
    }
}

fn idleSendPing(wire: *Wire, ack: bool, payload: *const [8]u8) !void {
    var ping: [17]u8 = undefined;
    (frame.FrameHeader{ .length = 8, .type = .ping, .flags = .{ .end_stream = ack }, .stream_id = 0 }).encode(ping[0..9]);
    @memcpy(ping[9..], payload);
    try wire.send(&ping);
}

fn idleSendRst(wire: *Wire, sid: u31) !void {
    var rst: [32]u8 = undefined;
    const n = try frame.Serializer.rstStream(&rst, sid, .cancel);
    try wire.send(rst[0..n]);
}

fn idleSendRequest(gpa: std.mem.Allocator, wire: *Wire, sid: u31, path: []const u8, end_stream: bool) !void {
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(gpa);
    try h2c.appendHeaders(gpa, &req, sid, path, end_stream);
    try wire.send(req.items);
}

/// The first GOAWAY must be phase 1 of a graceful close, and must come when the
/// connection has had nothing to do for `wait_ms` since `from_ns`.
fn idleExpectGoaway(wire: *Wire, from_ns: u64, wait_ms: u64, label: []const u8) !void {
    const deadline = nowNs() + client_deadline_ns;
    while (true) {
        const s = try wire.next(deadline) orelse return error.ClosedBeforeGoaway;
        if (s.ftype != ftype_goaway) continue;
        const ms = (nowNs() - from_ns) / std.time.ns_per_ms;
        std.debug.print("{s}: first GOAWAY after {d} ms (expected about {d} ms)\n", .{ label, ms, wait_ms });
        // The server's clock starts just before the client's on a new connection.
        if (ms + 100 < wait_ms) return error.GoawayTooEarly;
        if (ms > wait_ms + 750) return error.GoawayTooLate;
        if (s.code != @backingInt(frame.ErrorCode.no_error)) return error.WrongGoawayCode;
        if (s.last_stream_id != std.math.maxInt(u31)) return error.NotPhaseOneGoaway;
        return;
    }
}

/// Reads to the close. The last GOAWAY names the last stream the server took.
/// `seen` is a GOAWAY that came while the caller was reading.
fn idleExpectClose(wire: *Wire, last_stream: u32, answer_ping: bool, seen: ?Seen) !void {
    const deadline = nowNs() + client_deadline_ns;
    var last_goaway: ?Seen = seen;
    while (try wire.next(deadline)) |s| {
        if (s.ftype == ftype_goaway) last_goaway = s;
        if (answer_ping and s.ftype == ftype_ping and s.flags & 0x1 == 0) try idleSendPing(wire, true, "sh2grace");
    }
    const g = last_goaway orelse return error.ClosedWithoutSecondGoaway;
    if (g.code != @backingInt(frame.ErrorCode.no_error)) return error.WrongGoawayCode;
    if (g.last_stream_id != last_stream) return error.WrongLastStream;
}

/// Reads until the stream's response ends. A refusal is a failure. A GOAWAY goes
/// in `goaway`, or is a failure with none: the connection was not idle.
fn idleReadResponse(wire: *Wire, sid: u32, goaway: ?*?Seen) !void {
    const deadline = nowNs() + client_deadline_ns;
    while (true) {
        const s = try wire.next(deadline) orelse return error.ClosedBeforeResponse;
        if (s.ftype == ftype_goaway) {
            const out = goaway orelse return error.GoawayWhileBusy;
            out.* = s;
        }
        if (s.ftype == ftype_rst_stream and s.sid == sid) return error.RequestRefused;
        if ((s.ftype == ftype_data or s.ftype == ftype_headers) and s.sid == sid and s.flags & 0x1 != 0) return;
    }
}

fn idleReadFirstData(wire: *Wire, sid: u32) !void {
    const deadline = nowNs() + client_deadline_ns;
    while (true) {
        const s = try wire.next(deadline) orelse return error.ClosedBeforeData;
        if (s.ftype == ftype_goaway) return error.GoawayWhileBusy;
        if (s.ftype == ftype_data and s.sid == sid) return;
    }
}

fn idleMain(port: u16, gpa: std.mem.Allocator, transport: Transport, case: IdleCase, label: []const u8, run: *IdleRun) void {
    idleClient(port, gpa, transport, case, label, run) catch |err| {
        run.err = err;
    };
    run.done.store(true, .release);
}

fn idleClient(port: u16, gpa: std.mem.Allocator, transport: Transport, case: IdleCase, label: []const u8, run: *IdleRun) !void {
    const wire = try Wire.open(gpa, transport, port, null);
    defer wire.close();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try wire.send(req.items);
    const t0 = nowNs();
    switch (case) {
        .silent => {
            try idleExpectGoaway(wire, t0, idle_limit_ms, label);
            try idleExpectClose(wire, 0, false, null);
        },
        .pings => {
            const deadline = t0 + client_deadline_ns;
            var answered: usize = 0;
            outer: while (true) {
                try idleSendPing(wire, false, "idleping");
                while (true) {
                    const s = try wire.next(deadline) orelse return error.ClosedBeforeGoaway;
                    if (s.ftype == ftype_goaway) break :outer;
                    if (s.ftype == ftype_ping and s.flags & 0x1 != 0) break;
                }
                answered += 1;
                sleepMs(50);
            }
            const ms = (nowNs() - t0) / std.time.ns_per_ms;
            std.debug.print("{s}: first GOAWAY after {d} ms, with {d} PINGs answered before it\n", .{ label, ms, answered });
            if (answered < 2) return error.TooFewPingsAnswered;
            if (ms + 100 < idle_limit_ms) return error.GoawayTooEarly;
            if (ms > idle_limit_ms + 750) return error.GoawayTooLate;
            try idleExpectClose(wire, 0, true, null);
        },
        .open_stream => {
            // The idle time has started before the stream opens, on both transports.
            sleepMs(100);
            try idleSendRequest(gpa, wire, 1, "/sse", true);
            try idleReadFirstData(wire, 1);
            sleepMs(3 * idle_limit_ms);
            try idleSendRst(wire, 1);
            try idleExpectGoaway(wire, nowNs(), idle_limit_ms, label);
            try idleExpectClose(wire, 1, true, null);
        },
        .unfinished_request => {
            try idleSendRequest(gpa, wire, 1, "/sse", false);
            sleepMs(3 * idle_limit_ms);
            try idleSendRst(wire, 1);
            try idleExpectGoaway(wire, nowNs(), idle_limit_ms, label);
            try idleExpectClose(wire, 1, true, null);
        },
        .handler_outlives_reset => {
            try idleSendRequest(gpa, wire, 1, "/stubborn", true);
            try idleReadFirstData(wire, 1);
            try idleSendRst(wire, 1);
            // From the reset, not from the handler's end.
            try idleExpectGoaway(wire, nowNs(), idle_limit_ms, label);
            try idleExpectClose(wire, 1, true, null);
        },
        .requests => {
            // The gap leaves a loaded machine room; the run is longer than the limit.
            var sid: u31 = 1;
            while (true) {
                try idleSendRequest(gpa, wire, sid, "/hello", true);
                try idleReadResponse(wire, sid, null);
                if (nowNs() >= t0 + 2 * idle_limit_ms * std.time.ns_per_ms) break;
                sleepMs(50);
                sid += 2;
            }
            try idleExpectGoaway(wire, nowNs(), idle_limit_ms, label);
            try idleExpectClose(wire, sid, true, null);
        },
        .crosses_goaway => {
            try idleExpectGoaway(wire, t0, idle_limit_ms, label);
            try idleSendRequest(gpa, wire, 1, "/hello", true);
            var goaway: ?Seen = null;
            try idleReadResponse(wire, 1, &goaway);
            try idleExpectClose(wire, 1, true, goaway);
        },
        .unfinished_headers => {
            var req1: std.ArrayList(u8) = .empty;
            defer req1.deinit(gpa);
            try h2c.appendHeaders(gpa, &req1, 1, "/hello", true);
            const block = req1.items[frame.FRAME_HEADER_LEN..];
            const half = block.len / 2;
            var hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
            (frame.FrameHeader{ .length = @intCast(half), .type = .headers, .flags = .{ .end_stream = true }, .stream_id = 1 }).encode(&hdr);
            try wire.send(&hdr);
            try wire.send(block[0..half]);
            // Under the default 5 s `field_block_timeout_ns`.
            sleepMs(3 * idle_limit_ms);
            (frame.FrameHeader{ .length = @intCast(block.len - half), .type = .continuation, .flags = .{ .end_headers = true }, .stream_id = 1 }).encode(&hdr);
            try wire.send(&hdr);
            try wire.send(block[half..]);
            try idleReadResponse(wire, 1, null);
            try idleExpectGoaway(wire, nowNs(), idle_limit_ms, label);
            try idleExpectClose(wire, 1, true, null);
        },
        .cut_after_goaway => {
            try idleExpectGoaway(wire, t0, idle_limit_ms, label);
            try idleSendRequest(gpa, wire, 1, "/record", true);
            try idleExpectClose(wire, 1, true, null);
        },
        .stream_ends_with_no_wake => {
            // Let the first writes finish, then hold the writer.
            sleepMs(100);
            wire_pump.test_hold_writes.store(true, .release);
            sleepMs(50);
            try idleSendRequest(gpa, wire, 1, "/send", true);
            while (!run.release.load(.acquire)) sleepMs(5);
        },
        .off => {
            sleepMs(3 * idle_limit_ms);
            try idleSendPing(wire, false, "idleping");
            const deadline = nowNs() + client_deadline_ns;
            while (true) {
                const s = try wire.next(deadline) orelse return error.ClosedWithLimitOff;
                if (s.ftype == ftype_goaway) return error.GoawayWithLimitOff;
                if (s.ftype == ftype_ping and s.flags & 0x1 != 0) return;
            }
        },
    }
}

fn runIdle(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, case: IdleCase, label: []const u8) !void {
    const io = rt.io();
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangSse } } },
        .{ .method = .GET, .path = "/stubborn", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = stubborn } } },
        .{ .method = .GET, .path = "/record", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = recordCause } } },
        .{ .method = .GET, .path = "/send", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendThenRecordCause } } },
    };
    var limits = starh2.Limits.defaults;
    limits.connection_idle_timeout_ns = if (case == .off) 0 else idle_limit_ms * std.time.ns_per_ms;
    limits.graceful_drain_timeout_ns = 300 * std.time.ns_per_ms;
    defer wire_pump.test_hold_writes.store(false, .release);
    idle_cause_seen.store(false, .release);
    idle_cause_was_closed.store(false, .release);

    var cert_pem: []u8 = &.{};
    var key_pem: []u8 = &.{};
    defer {
        gpa.free(cert_pem);
        gpa.free(key_pem);
    }
    var endpoint: starh2.EndpointConfig = undefined;
    var tls_cfg: ?starh2.TlsConfig = null;
    switch (transport) {
        .h2c => endpoint = .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .tls => {
            cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } };
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
    defer {
        server.requestShutdown();
        serve_deadline.join(&server, &serve_handle);
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var run: IdleRun = .{};
    const t = try std.Thread.spawn(.{}, idleMain, .{ server.localAddress(0).getPort(), gpa, transport, case, label, &run });
    defer run.release.store(true, .release);
    var no_wake_gone_ms: u64 = 0;
    if (case == .stream_ends_with_no_wake) {
        // Nothing reaches the client, so the connection count is the only answer:
        // the handler, two limits (`nextDeadlineNs`), the unanswered PING, the
        // drain, the close deadline.
        const t_start = nowNs();
        while (server.active_connections.load(.acquire) == 0) {
            if (nowNs() >= t_start + client_deadline_ns) {
                run.release.store(true, .release);
                t.join();
                return error.ClientNeverConnected;
            }
            zio.sleep(.fromMilliseconds(1)) catch {};
        }
        const stop = t_start + (slow_handler_ms + 2 * idle_limit_ms + 1000 + 300 + 500 + 1000) * std.time.ns_per_ms;
        while (server.active_connections.load(.acquire) != 0) {
            if (nowNs() >= stop) {
                wire_pump.test_hold_writes.store(false, .release);
                run.release.store(true, .release);
                t.join();
                return error.HeldPastIdleTimeout;
            }
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        no_wake_gone_ms = (nowNs() - t_start) / std.time.ns_per_ms;
        std.debug.print("{s}: connection gone {d} ms after it opened\n", .{ label, no_wake_gone_ms });
        wire_pump.test_hold_writes.store(false, .release);
        run.release.store(true, .release);
    }
    // Wait inside zio so this executor keeps serving the connection.
    while (!run.done.load(.acquire)) zio.sleep(.fromMilliseconds(10)) catch {};
    starh2.test_sleep.joinThread(t);
    if (run.err) |err| return err;
    // The client saw the close; the server's slot goes back just after it.
    const stop = nowNs() + 2 * std.time.ns_per_s;
    while (server.active_connections.load(.acquire) != 0) {
        if (nowNs() >= stop) return error.ConnectionStillCounted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    if (case == .stream_ends_with_no_wake) {
        // Through the whole graceful path, not cut: the client's wait, the handler,
        // one limit, the PING, the drain, and the close deadline (the writer is
        // held, so the close never counts as flushed).
        try std.testing.expect(no_wake_gone_ms + 100 >= 150 + slow_handler_ms + idle_limit_ms + 1000 + 300 + 500);
    }
    if (case == .cut_after_goaway or case == .stream_ends_with_no_wake) {
        // The handler stops on its own poll, just after the close.
        const seen_stop = nowNs() + 2 * std.time.ns_per_s;
        while (!idle_cause_seen.load(.acquire)) {
            if (nowNs() >= seen_stop) return error.HandlerNeverSawCause;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        // The connection is closing; the server is not shutting down.
        try std.testing.expect(idle_cause_was_closed.load(.acquire));
    }
}

fn runIdleCase(transport: Transport, case: IdleCase, label: []const u8) !void {
    const gpa = tsan_allocator.allocator();
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runIdle, .{ rt, gpa, transport, case, label });
    try h.join();
}

test "idle: h2c a connection that asks for nothing ends with both GOAWAYs (t-3705)" {
    try runIdleCase(.h2c, .silent, "h2c idle silent");
}
test "idle: tls a connection that asks for nothing ends with both GOAWAYs (t-3705)" {
    try runIdleCase(.tls, .silent, "tls idle silent");
}
test "idle: h2c PINGs do not hold a connection open (t-3705)" {
    try runIdleCase(.h2c, .pings, "h2c idle pings");
}
test "idle: tls PINGs do not hold a connection open (t-3705)" {
    try runIdleCase(.tls, .pings, "tls idle pings");
}
test "idle: h2c a long stream is not idle, and the time starts when it ends (t-3705)" {
    try runIdleCase(.h2c, .open_stream, "h2c idle open stream");
}
test "idle: tls a long stream is not idle, and the time starts when it ends (t-3705)" {
    try runIdleCase(.tls, .open_stream, "tls idle open stream");
}
test "idle: h2c a stream the peer has not ended is not idle (t-3705)" {
    try runIdleCase(.h2c, .unfinished_request, "h2c idle unfinished request");
}
test "idle: tls a stream the peer has not ended is not idle (t-3705)" {
    try runIdleCase(.tls, .unfinished_request, "tls idle unfinished request");
}
test "idle: h2c a handler still running after its reset does not hold the connection (t-3705)" {
    try runIdleCase(.h2c, .handler_outlives_reset, "h2c idle handler outlives reset");
}
test "idle: tls a handler still running after its reset does not hold the connection (t-3705)" {
    try runIdleCase(.tls, .handler_outlives_reset, "tls idle handler outlives reset");
}
test "idle: h2c short requests each restart the time (t-3705)" {
    try runIdleCase(.h2c, .requests, "h2c idle requests");
}
test "idle: tls short requests each restart the time (t-3705)" {
    try runIdleCase(.tls, .requests, "tls idle requests");
}
test "idle: h2c a request that crosses the first GOAWAY is served (t-3705)" {
    try runIdleCase(.h2c, .crosses_goaway, "h2c idle crosses goaway");
}
test "idle: tls a request that crosses the first GOAWAY is served (t-3705)" {
    try runIdleCase(.tls, .crosses_goaway, "tls idle crosses goaway");
}
test "idle: h2c a limit of 0 turns it off (t-3705)" {
    try runIdleCase(.h2c, .off, "h2c idle off");
}
test "idle: tls a limit of 0 turns it off (t-3705)" {
    try runIdleCase(.tls, .off, "tls idle off");
}
test "idle: h2c a header block that takes longer than the limit is not idle (t-3705)" {
    try runIdleCase(.h2c, .unfinished_headers, "h2c idle unfinished headers");
}
test "idle: tls a header block that takes longer than the limit is not idle (t-3705)" {
    try runIdleCase(.tls, .unfinished_headers, "tls idle unfinished headers");
}
test "idle: h2c a handler cut after the first GOAWAY does not hear the server is shutting down (t-3705)" {
    try runIdleCase(.h2c, .cut_after_goaway, "h2c idle cut after goaway");
}
test "idle: tls a handler cut after the first GOAWAY does not hear the server is shutting down (t-3705)" {
    try runIdleCase(.tls, .cut_after_goaway, "tls idle cut after goaway");
}
test "idle: h2c a last stream that ends with nothing to wake the actor still ends the connection (t-3705)" {
    try runIdleCase(.h2c, .stream_ends_with_no_wake, "h2c idle no wake");
}
test "idle: h2c a peer that holds its window and stays silent loses its connection after the kill (t-3705)" {
    try runStalledCase(.h2c, .peer_holds_window, .idle_after_kill, "h2c idle after kill");
}
test "idle: tls a peer that holds its window and stays silent loses its connection after the kill (t-3705)" {
    try runStalledCase(.tls, .peer_holds_window, .idle_after_kill, "tls idle after kill");
}

// ---------------------------------------------------------------------------
// A handler that outlives its connection (t-3737)
// ---------------------------------------------------------------------------

/// How the handler spends the time after its peer has gone.
const OutliveMode = enum {
    /// `try zio.sleep`: an ordinary handler that honours cancellation.
    honours_cancel,
    /// A shielded sleep: it cannot be canceled.
    shielded,
    /// A loop that swallows `error.Canceled`.
    ignores_cancel,
};

var outlive_mode: OutliveMode = .honours_cancel;
var outlive_started: std.atomic.Value(bool) = .init(false);
var outlive_done: std.atomic.Value(bool) = .init(false);
/// Longer than the 5 s teardown watchdog on master, so master panics here.
const outlive_hold_ms = 6_000;

fn outliveHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    defer outlive_done.store(true, .release);
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    outlive_started.store(true, .release);
    const t0 = nowNs();
    switch (outlive_mode) {
        .honours_cancel => try zio.sleep(.fromMilliseconds(outlive_hold_ms)),
        .shielded => {
            zio.beginShield();
            defer zio.endShield();
            zio.sleep(.fromMilliseconds(outlive_hold_ms)) catch {};
        },
        .ignores_cancel => while (nowNs() < t0 + outlive_hold_ms * std.time.ns_per_ms) {
            zio.sleep(.fromMilliseconds(20)) catch {};
        },
    }
    // Still running after the connection has ended: this call must fail
    // cleanly, without touching the connection.
    if (body.writeAll("data: late\n\n")) |_| return error.LateWriteSucceeded else |_| {}
}

fn outliveClient(port: u16, gpa: std.mem.Allocator, transport: Transport, run: *IdleRun) void {
    outliveClientInner(port, gpa, transport) catch |err| {
        run.err = err;
    };
    run.done.store(true, .release);
}

fn outliveClientInner(port: u16, gpa: std.mem.Allocator, transport: Transport) !void {
    const wire = try Wire.open(gpa, transport, port, null);
    defer wire.close();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try wire.send(req.items);
    try idleSendRequest(gpa, wire, 1, "/outlive", true);
    try idleReadFirstData(wire, 1);
    // The peer goes away: `wire.close` on return.
}

fn runOutlive(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    const io = rt.io();
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/outlive", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = outliveHandler } } },
    };
    var cert_pem: []u8 = &.{};
    var key_pem: []u8 = &.{};
    defer {
        gpa.free(cert_pem);
        gpa.free(key_pem);
    }
    var endpoint: starh2.EndpointConfig = undefined;
    var tls_cfg: ?starh2.TlsConfig = null;
    switch (transport) {
        .h2c => endpoint = .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .tls => {
            cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } };
            tls_cfg = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem };
        },
    }
    var server = try starh2.Server.init(gpa, io, .{ .endpoints = &.{endpoint}, .routes = &routes, .tls = tls_cfg, .limits = starh2.Limits.defaults });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_deadline.join(&server, &serve_handle);
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    var run: IdleRun = .{};
    const t = try std.Thread.spawn(.{}, outliveClient, .{ server.localAddress(0).getPort(), gpa, transport, &run });
    while (!run.done.load(.acquire)) zio.sleep(.fromMilliseconds(10)) catch {};
    starh2.test_sleep.joinThread(t);
    if (run.err) |err| return err;
    try std.testing.expect(outlive_started.load(.acquire));
    const t_gone = nowNs();
    // The connection ends, and gives its slot back, without waiting for the
    // handler.
    while (server.active_connections.load(.acquire) != 0) {
        if (nowNs() >= t_gone + 3 * std.time.ns_per_s) return error.ConnectionWaitedForHandler;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    const gone_ms = (nowNs() - t_gone) / std.time.ns_per_ms;
    std.debug.print("outlive {s} {s}: connection gone after {d} ms, handler done={}\n", .{ @tagName(transport), @tagName(outlive_mode), gone_ms, outlive_done.load(.acquire) });
    // .honours_cancel may end early if teardown cancels it; the other two
    // cannot, so the connection really did end first.
    if (outlive_mode != .honours_cancel) try std.testing.expect(!outlive_done.load(.acquire));
    // The handler ends on its own; its memory must come back (the testing
    // allocator reports a leak otherwise).
    while (!outlive_done.load(.acquire)) {
        if (nowNs() >= t_gone + (outlive_hold_ms + 5000) * std.time.ns_per_ms) return error.HandlerNeverEnded;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

fn outliveCase(transport: Transport, mode: OutliveMode) !void {
    outlive_mode = mode;
    outlive_started.store(false, .release);
    outlive_done.store(false, .release);
    const gpa = tsan_allocator.allocator();
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runOutlive, .{ rt, gpa, transport });
    try h.join();
}

test "outlive: h2c a handler that honours cancel does not hold its connection (t-3737)" {
    try outliveCase(.h2c, .honours_cancel);
}
test "outlive: tls a handler that honours cancel does not hold its connection (t-3737)" {
    try outliveCase(.tls, .honours_cancel);
}
test "outlive: h2c a shielded handler does not hold its connection (t-3737)" {
    try outliveCase(.h2c, .shielded);
}
test "outlive: tls a shielded handler does not hold its connection (t-3737)" {
    try outliveCase(.tls, .shielded);
}
test "outlive: h2c a handler that swallows cancel does not hold its connection (t-3737)" {
    try outliveCase(.h2c, .ignores_cancel);
}
test "outlive: tls a handler that swallows cancel does not hold its connection (t-3737)" {
    try outliveCase(.tls, .ignores_cancel);
}

// ---------------------------------------------------------------------------
// An HTTP/1.1 handler that outlives its connection (t-3759 step 7)
//
// The HTTP/1.1 form of the `outlive:` tests above. On h1c and on HTTP/1.1
// over TLS, a handler that honours cancel, a shielded one and one that
// swallows `error.Canceled` hold for `outlive_hold_ms`. At the peer's close,
// or at the server's shutdown, the connection (and, on TLS, its slot) must be
// gone within 3 s while the handler still runs; the handler's late write
// must fail with the cause; the request it reads after its connection ended
// must be intact (the connection poisons its buffers when it ends, in safe
// builds, so a request field left pointing into them reads garbage); `serve`
// must not return until the handler ends; and a second client is served
// while the handler runs.
// ---------------------------------------------------------------------------

const H1Transport = enum { h1c, tls };
const H1Trigger = enum { peer_close, shutdown };

const h1o_path = "/h1outlive";
const h1o_query = "probe=query-value-0123";
const h1o_header_value = "probe-header-value-abcdefghij";
const h1o_body = "probe-body-0123456789abcdefghijklmnopqrstuvwxyz";

/// The HTTP/1.1 modes: `OutliveMode`'s three, and a shielded handler parked
/// inside the response's own `waitUntil`, inside the gate, when its
/// connection ends (round 1 of the step-7 review). Teardown must wake it
/// (`dead`), or the gate drain waits for its deadline.
const H1OutliveMode = enum { honours_cancel, shielded, ignores_cancel, waits_shielded };

var h1o_mode: H1OutliveMode = .honours_cancel;
/// `waits_shielded`: what the `waitUntil` returned (0 not called, 1 ok at its
/// deadline, 2 `ConnectionClosed`, 3 another error), and when, from the
/// handler's start.
var h1o_wait: std.atomic.Value(u8) = .init(0);
var h1o_wait_ms: std.atomic.Value(u64) = .init(0);
var h1o_hold_ms: u64 = outlive_hold_ms;
var h1o_started: std.atomic.Value(bool) = .init(false);
var h1o_done: std.atomic.Value(bool) = .init(false);
/// The late write: 0 not tried, 1 succeeded, 2 failed.
var h1o_late: std.atomic.Value(u8) = .init(0);
var h1o_late_closed: std.atomic.Value(bool) = .init(false);
/// The cause the late write saw: 0 none, else 1 + the `TerminalCause` tag.
var h1o_late_cause: std.atomic.Value(u8) = .init(0);
/// The request read after the hold: 0 not read, 1 intact, 2 not.
var h1o_request: std.atomic.Value(u8) = .init(0);

fn h1CauseCode(c: starh2.TerminalCause) u8 {
    return 1 + @as(u8, @intFromEnum(std.meta.activeTag(c)));
}

fn h1RequestIntact(req: *const starh2.Request) bool {
    if (!std.mem.eql(u8, req.path, h1o_path)) return false;
    if (!std.mem.endsWith(u8, req.query, h1o_query)) return false;
    if (!std.mem.eql(u8, req.body, h1o_body)) return false;
    if (!std.mem.eql(u8, req.authority, "localhost")) return false;
    for (req.headers) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, "x-probe") and std.mem.eql(u8, h.value, h1o_header_value)) return true;
    }
    return false;
}

fn h1OutliveHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    defer h1o_done.store(true, .release);
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    // On h1c an SSE write stays in the connection's socket buffer until a
    // flush (t-3784); the client waits for this event.
    try body.flush();
    h1o_started.store(true, .release);
    const t0 = nowNs();
    switch (h1o_mode) {
        .honours_cancel => try zio.sleep(.fromMilliseconds(h1o_hold_ms)),
        .shielded => {
            zio.beginShield();
            defer zio.endShield();
            zio.sleep(.fromMilliseconds(h1o_hold_ms)) catch {};
        },
        .ignores_cancel => while (nowNs() < t0 + h1o_hold_ms * std.time.ns_per_ms) {
            zio.sleep(.fromMilliseconds(20)) catch {};
        },
        .waits_shielded => {
            zio.beginShield();
            defer zio.endShield();
            const now = std.Io.Clock.awake.now(resp.io).nanoseconds;
            const deadline: std.Io.Timestamp = .{ .nanoseconds = now + @as(i96, h1o_hold_ms) * std.time.ns_per_ms };
            const code: u8 = if (body.waitUntil(deadline)) |_| 1 else |err| if (err == error.ConnectionClosed) 2 else 3;
            h1o_wait_ms.store((nowNs() - t0) / std.time.ns_per_ms, .release);
            h1o_wait.store(code, .release);
            // Then hold, as the shielded mode, so the connection ended first.
            while (nowNs() < t0 + h1o_hold_ms * std.time.ns_per_ms) zio.sleep(.fromMilliseconds(20)) catch {};
        },
    }
    // The connection ended (and poisoned its buffers) a while ago.
    h1o_request.store(if (h1RequestIntact(req)) 1 else 2, .release);
    if (body.writeAll("data: late\n\n")) |_| {
        h1o_late.store(1, .release);
    } else |err| {
        h1o_late_closed.store(err == error.ConnectionClosed, .release);
        const cause: u8 = if (body.terminalCause()) |c| h1CauseCode(c) else 0;
        h1o_late_cause.store(cause, .release);
        h1o_late.store(2, .release);
    }
}

fn h1OkHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

fn h1WireTransport(t: H1Transport) Transport {
    return switch (t) {
        .h1c => .h2c,
        .tls => .tls,
    };
}

fn h1ReadUntil(w: *Wire, needle: []const u8, deadline: u64) !void {
    while (std.mem.indexOf(u8, w.plain.items, needle) == null) {
        if (nowNs() >= deadline) return error.Deadline;
        const more = w.pump() catch |err| switch (err) {
            error.WouldBlock => continue,
            else => {
                std.debug.print("h1 client: {s} after {d} bytes, waiting for \"{s}\": {s}\n", .{ @errorName(err), w.plain.items.len, needle, w.plain.items[0..@min(w.plain.items.len, 200)] });
                return err;
            },
        };
        if (!more) {
            std.debug.print("h1 client: closed after {d} bytes, waiting for \"{s}\": {s}\n", .{ w.plain.items.len, needle, w.plain.items[0..@min(w.plain.items.len, 200)] });
            return error.ClosedBeforeExpected;
        }
    }
}

/// The attacker: asks for the outliving handler, reads its first event,
/// then holds the socket open and reads nothing until released, and closes.
/// Runs on a thread of its own: its socket calls block.
const H1OutliveClient = struct {
    port: u16,
    transport: H1Transport,
    trigger: H1Trigger,
    err: ?anyerror = null,
    ready: std.atomic.Value(bool) = .init(false),
    release: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),

    fn main(c: *H1OutliveClient) void {
        c.run() catch |err| {
            c.err = err;
        };
        c.ready.store(true, .release);
        c.done.store(true, .release);
    }

    fn run(c: *H1OutliveClient) !void {
        const gpa = tsan_allocator.pageAllocator();
        const w = try Wire.openAlpn(gpa, h1WireTransport(c.transport), c.port, null, "http/1.1");
        defer w.close();
        var buf: [512]u8 = undefined;
        const req = try std.fmt.bufPrint(&buf, "POST {s}?{s} HTTP/1.1\r\nHost: localhost\r\nx-probe: {s}\r\ncontent-length: {d}\r\n\r\n{s}", .{ h1o_path, h1o_query, h1o_header_value, h1o_body.len, h1o_body });
        try w.send(req);
        try h1ReadUntil(w, "data: hi", nowNs() + client_deadline_ns);
        c.ready.store(true, .release);
        // Closed (`peer_close`) or let go (`shutdown`) when the test says, so
        // the handler's first event has been acknowledged to it first.
        while (!c.release.load(.acquire)) {
            starh2.test_sleep.threadSleepMs(5);
        }
    }
};

/// A well-behaved client: one `GET /ok`, on a thread of its own.
const H1OkClient = struct {
    port: u16,
    transport: H1Transport,
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),

    fn main(c: *H1OkClient) void {
        c.run() catch |err| {
            c.err = err;
        };
        c.done.store(true, .release);
    }

    fn run(c: *H1OkClient) !void {
        const gpa = tsan_allocator.pageAllocator();
        const w = try Wire.openAlpn(gpa, h1WireTransport(c.transport), c.port, null, "http/1.1");
        defer w.close();
        try w.send("GET /ok HTTP/1.1\r\nHost: localhost\r\nconnection: close\r\n\r\n");
        try h1ReadUntil(w, "\r\n\r\nok", nowNs() + client_deadline_ns);
        if (!std.mem.startsWith(u8, w.plain.items, "HTTP/1.1 200")) return error.NotOk;
    }
};

/// Serve one `GET /ok` from a second client, and fail if it is not served.
fn h1ServedOk(port: u16, transport: H1Transport) !void {
    var c: H1OkClient = .{ .port = port, .transport = transport };
    const t = try std.Thread.spawn(.{}, H1OkClient.main, .{&c});
    while (!c.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    starh2.test_sleep.joinThread(t);
    if (c.err) |err| return err;
}

fn runH1Outlive(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: H1Transport, trigger: H1Trigger) !void {
    const io = rt.io();
    const routes = [_]starh2.Route{
        .{ .method = .POST, .path = h1o_path, .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = h1OutliveHandler } } },
        .{ .method = .GET, .path = "/ok", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = h1OkHandler } } },
    };
    var cert_pem: []u8 = &.{};
    var key_pem: []u8 = &.{};
    defer {
        gpa.free(cert_pem);
        gpa.free(key_pem);
    }
    var endpoint: starh2.EndpointConfig = undefined;
    var tls_cfg: ?starh2.TlsConfig = null;
    switch (transport) {
        .h1c => endpoint = .{ .h1c = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .tls => {
            cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } };
            tls_cfg = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem };
        },
    }
    var server = try starh2.Server.init(gpa, io, .{ .endpoints = &.{endpoint}, .routes = &routes, .tls = tls_cfg, .limits = starh2.Limits.defaults });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var client: H1OutliveClient = .{ .port = 0, .transport = transport, .trigger = trigger };
    var client_thread: ?std.Thread = null;
    defer {
        server.requestShutdown();
        client.release.store(true, .release);
        if (client_thread) |t| starh2.test_sleep.joinThread(t);
        serve_deadline.join(&server, &serve_handle);
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();
    client.port = port;
    const t0 = nowNs();
    client_thread = try std.Thread.spawn(.{}, H1OutliveClient.main, .{&client});
    while (!client.ready.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    if (client.err) |err| return err;
    // The client can read the first event before the handler's write
    // returns (TLS acks it after the record went out).
    while (!h1o_started.load(.acquire)) {
        if (nowNs() >= t0 + 5 * std.time.ns_per_s) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }

    // A second client is served while the handler runs.
    try h1ServedOk(port, transport);

    const t_trigger = nowNs();
    switch (trigger) {
        .peer_close => {
            client.release.store(true, .release);
            while (!client.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
            if (client.err) |err| return err;
        },
        .shutdown => server.requestShutdown(),
    }
    // The connection ends, and gives its slot back, without waiting for the
    // handler.
    while (server.active_connections.load(.acquire) != 0) {
        if (nowNs() >= t_trigger + 3 * std.time.ns_per_s) return error.ConnectionWaitedForHandler;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    const gone_ms = (nowNs() - t_trigger) / std.time.ns_per_ms;
    const done_at_gone = h1o_done.load(.acquire);
    const orphans_at_gone = server.accounting.orphaned_handlers.load(.acquire);
    const slots = server.slots.snapshot();
    std.debug.print("h1 outlive {s} {s} {s}: connection gone after {d} ms, handler done={}, orphans={d}, slots back {d} of {d}\n", .{
        @tagName(transport), @tagName(trigger), @tagName(h1o_mode), gone_ms, done_at_gone, orphans_at_gone, slots.len, slots.created,
    });
    try std.testing.expectEqual(slots.created, slots.len);
    // .honours_cancel may end at the cancel; the other two cannot, so the
    // connection really did end first.
    if (h1o_mode != .honours_cancel) {
        try std.testing.expect(!done_at_gone);
        try std.testing.expectEqual(@as(usize, 1), orphans_at_gone);
    }

    if (trigger == .peer_close) {
        // Still served after the connection is gone, with the handler running.
        try h1ServedOk(port, transport);
        server.requestShutdown();
    }
    // `serve` does not return while the handler runs.
    while (!h1o_done.load(.acquire)) {
        if (serve_handle.hasResult()) return error.ServeReturnedUnderHandler;
        if (nowNs() >= t_trigger + (h1o_hold_ms + 5000) * std.time.ns_per_ms) return error.HandlerNeverEnded;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    if (h1o_mode != .honours_cancel) {
        const want_cause: starh2.TerminalCause = switch (trigger) {
            .peer_close => .connection_closed,
            .shutdown => .server_shutdown,
        };
        std.debug.print("h1 outlive {s} {s} {s}: late write {s}, closed={}, cause={d} (want {d}), request intact={d}\n", .{
            @tagName(transport),                                     @tagName(trigger),
            @tagName(h1o_mode),                                      if (h1o_late.load(.acquire) == 2) "failed" else "did not fail",
            h1o_late_closed.load(.acquire),                          h1o_late_cause.load(.acquire),
            h1CauseCode(want_cause), h1o_request.load(.acquire),
        });
        try std.testing.expectEqual(@as(u8, 2), h1o_late.load(.acquire));
        try std.testing.expect(h1o_late_closed.load(.acquire));
        try std.testing.expectEqual(h1CauseCode(want_cause), h1o_late_cause.load(.acquire));
        try std.testing.expectEqual(@as(u8, 1), h1o_request.load(.acquire));
    }
    if (h1o_mode == .waits_shielded) {
        const waited_ms = h1o_wait_ms.load(.acquire);
        std.debug.print("h1 outlive {s} {s} waits_shielded: waitUntil returned {d} after {d} ms (hold {d} ms)\n", .{
            @tagName(transport), @tagName(trigger), h1o_wait.load(.acquire), waited_ms, h1o_hold_ms,
        });
        // Woken by the connection's end with its cause, not by its deadline.
        try std.testing.expectEqual(@as(u8, 2), h1o_wait.load(.acquire));
        try std.testing.expect(waited_ms < h1o_hold_ms / 2);
    }
}

fn h1OutliveCase(transport: H1Transport, trigger: H1Trigger, mode: H1OutliveMode, hold_ms: u64) !void {
    h1o_mode = mode;
    h1o_wait.store(0, .release);
    h1o_wait_ms.store(0, .release);
    h1o_hold_ms = hold_ms;
    h1o_started.store(false, .release);
    h1o_done.store(false, .release);
    h1o_late.store(0, .release);
    h1o_late_closed.store(false, .release);
    h1o_late_cause.store(0, .release);
    h1o_request.store(0, .release);
    const gpa = tsan_allocator.allocator();
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runH1Outlive, .{ rt, gpa, transport, trigger });
    try h.join();
}

test "outlive: h1c a handler that honours cancel does not hold its connection when the peer closes (t-3759)" {
    try h1OutliveCase(.h1c, .peer_close, .honours_cancel, outlive_hold_ms);
}
test "outlive: h1c a shielded handler does not hold its connection when the peer closes (t-3759)" {
    try h1OutliveCase(.h1c, .peer_close, .shielded, outlive_hold_ms);
}
test "outlive: h1c a handler that swallows cancel does not hold its connection when the peer closes (t-3759)" {
    try h1OutliveCase(.h1c, .peer_close, .ignores_cancel, outlive_hold_ms);
}
test "outlive: h1c a handler that honours cancel does not hold its connection at shutdown (t-3759)" {
    try h1OutliveCase(.h1c, .shutdown, .honours_cancel, outlive_hold_ms);
}
test "outlive: h1c a shielded handler does not hold its connection at shutdown (t-3759)" {
    try h1OutliveCase(.h1c, .shutdown, .shielded, outlive_hold_ms);
}
test "outlive: h1c a handler that swallows cancel does not hold its connection at shutdown (t-3759)" {
    try h1OutliveCase(.h1c, .shutdown, .ignores_cancel, outlive_hold_ms);
}
test "outlive: h1 tls a handler that honours cancel does not hold its connection when the peer closes (t-3759)" {
    try h1OutliveCase(.tls, .peer_close, .honours_cancel, outlive_hold_ms);
}
test "outlive: h1 tls a shielded handler does not hold its connection when the peer closes (t-3759)" {
    try h1OutliveCase(.tls, .peer_close, .shielded, outlive_hold_ms);
}
test "outlive: h1 tls a handler that swallows cancel does not hold its connection when the peer closes (t-3759)" {
    try h1OutliveCase(.tls, .peer_close, .ignores_cancel, outlive_hold_ms);
}
test "outlive: h1 tls a handler that honours cancel does not hold its connection at shutdown (t-3759)" {
    try h1OutliveCase(.tls, .shutdown, .honours_cancel, outlive_hold_ms);
}
test "outlive: h1 tls a shielded handler does not hold its connection at shutdown (t-3759)" {
    try h1OutliveCase(.tls, .shutdown, .shielded, outlive_hold_ms);
}
test "outlive: h1 tls a handler that swallows cancel does not hold its connection at shutdown (t-3759)" {
    try h1OutliveCase(.tls, .shutdown, .ignores_cancel, outlive_hold_ms);
}
test "outlive: h1c a shielded handler in waitUntil does not hold its connection when the peer closes (t-3759)" {
    try h1OutliveCase(.h1c, .peer_close, .waits_shielded, outlive_hold_ms);
}
test "outlive: h1c a shielded handler in waitUntil does not hold its connection at shutdown (t-3759)" {
    try h1OutliveCase(.h1c, .shutdown, .waits_shielded, outlive_hold_ms);
}
test "outlive: h1 tls a shielded handler in waitUntil does not hold its connection when the peer closes (t-3759)" {
    try h1OutliveCase(.tls, .peer_close, .waits_shielded, outlive_hold_ms);
}
test "outlive: h1 tls a shielded handler in waitUntil does not hold its connection at shutdown (t-3759)" {
    try h1OutliveCase(.tls, .shutdown, .waits_shielded, outlive_hold_ms);
}

// A handler blocked INSIDE a write when its connection ends: the peer reads
// nothing, so on h1c the handler's socket write waits for room, and on TLS
// it waits for the actor to drive its record out. It writes under a shield,
// so a cancel cannot free it: only teardown's wakes can (the socket shut,
// the TLS write channels closed). The connection must end within 3 s, far
// inside the 60 s write-byte timeout, and the write must fail with the
// cause.

var h1w_started: std.atomic.Value(bool) = .init(false);
var h1w_done: std.atomic.Value(bool) = .init(false);
var h1w_closed: std.atomic.Value(bool) = .init(false);
var h1w_cause: std.atomic.Value(u8) = .init(0);

fn h1BlockedWriter(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    defer h1w_done.store(true, .release);
    var body = try resp.startSse(&.{});
    h1w_started.store(true, .release);
    var chunk: [16 * 1024]u8 = undefined;
    @memset(&chunk, 'x');
    zio.beginShield();
    defer zio.endShield();
    while (true) {
        body.writeAll(&chunk) catch |err| {
            h1w_closed.store(err == error.ConnectionClosed, .release);
            const cause: u8 = if (body.terminalCause()) |c| h1CauseCode(c) else 0;
            h1w_cause.store(cause, .release);
            return;
        };
    }
}

/// Asks for the endless response with a small receive buffer and reads
/// nothing until released.
const H1StalledClient = struct {
    port: u16,
    transport: H1Transport,
    err: ?anyerror = null,
    ready: std.atomic.Value(bool) = .init(false),
    release: std.atomic.Value(bool) = .init(false),

    fn main(c: *H1StalledClient) void {
        c.run() catch |err| {
            c.err = err;
        };
        c.ready.store(true, .release);
    }

    fn run(c: *H1StalledClient) !void {
        const gpa = tsan_allocator.pageAllocator();
        const w = try Wire.openAlpn(gpa, h1WireTransport(c.transport), c.port, 4096, "http/1.1");
        defer w.close();
        try w.send("GET /flood HTTP/1.1\r\nHost: localhost\r\n\r\n");
        c.ready.store(true, .release);
        while (!c.release.load(.acquire)) {
            starh2.test_sleep.threadSleepMs(5);
        }
    }
};

fn runH1BlockedWrite(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: H1Transport) !void {
    const io = rt.io();
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/flood", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = h1BlockedWriter } } },
    };
    var cert_pem: []u8 = &.{};
    var key_pem: []u8 = &.{};
    defer {
        gpa.free(cert_pem);
        gpa.free(key_pem);
    }
    var endpoint: starh2.EndpointConfig = undefined;
    var tls_cfg: ?starh2.TlsConfig = null;
    switch (transport) {
        .h1c => endpoint = .{ .h1c = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .tls => {
            cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } };
            tls_cfg = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem };
        },
    }
    var server = try starh2.Server.init(gpa, io, .{ .endpoints = &.{endpoint}, .routes = &routes, .tls = tls_cfg, .limits = starh2.Limits.defaults });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var client: H1StalledClient = .{ .port = 0, .transport = transport };
    var client_thread: ?std.Thread = null;
    defer {
        server.requestShutdown();
        client.release.store(true, .release);
        if (client_thread) |t| starh2.test_sleep.joinThread(t);
        serve_deadline.join(&server, &serve_handle);
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    client.port = server.localAddress(0).getPort();
    client_thread = try std.Thread.spawn(.{}, H1StalledClient.main, .{&client});
    while (!client.ready.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    if (client.err) |err| return err;
    const t_start = nowNs();
    while (!h1w_started.load(.acquire)) {
        if (nowNs() >= t_start + 5 * std.time.ns_per_s) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    // Let the writes fill every buffer between the handler and the peer.
    zio.sleep(.fromMilliseconds(500)) catch {};
    try std.testing.expect(!h1w_done.load(.acquire));
    const t_trigger = nowNs();
    server.requestShutdown();
    while (server.active_connections.load(.acquire) != 0) {
        if (nowNs() >= t_trigger + 3 * std.time.ns_per_s) return error.ConnectionWaitedForHandlerWrite;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    const gone_ms = (nowNs() - t_trigger) / std.time.ns_per_ms;
    while (!h1w_done.load(.acquire)) {
        if (nowNs() >= t_trigger + 5 * std.time.ns_per_s) return error.HandlerNeverEnded;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    std.debug.print("h1 blocked write {s}: connection gone after {d} ms; the write failed closed={} cause={d}\n", .{
        @tagName(transport), gone_ms, h1w_closed.load(.acquire), h1w_cause.load(.acquire),
    });
    try std.testing.expect(h1w_closed.load(.acquire));
    try std.testing.expectEqual(h1CauseCode(.server_shutdown), h1w_cause.load(.acquire));
}

fn h1BlockedWriteCase(transport: H1Transport) !void {
    h1w_started.store(false, .release);
    h1w_done.store(false, .release);
    h1w_closed.store(false, .release);
    h1w_cause.store(0, .release);
    const gpa = tsan_allocator.allocator();
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runH1BlockedWrite, .{ rt, gpa, transport });
    try h.join();
}

test "outlive: h1c a handler blocked in a socket write does not hold its connection at shutdown (t-3759)" {
    try h1BlockedWriteCase(.h1c);
}
test "outlive: h1 tls a handler blocked in a TLS write does not hold its connection at shutdown (t-3759)" {
    try h1BlockedWriteCase(.tls);
}

// HTTP/1.1 refusals that answer 503 and close: a task request past the
// server's task-handler bound (`Limits.max_task_handlers`), and a request
// whose copy into a request arena the connection's request memory refuses
// (`Limits.request_bytes_per_connection`, t-3759 step 7: an HTTP/1.1
// request is now pool memory, like an HTTP/2 one).

var h1r_hold: std.atomic.Value(bool) = .init(false);
var h1r_running: std.atomic.Value(usize) = .init(0);

fn h1HoldHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = h1r_running.fetchAdd(1, .acq_rel);
    defer _ = h1r_running.fetchSub(1, .acq_rel);
    while (h1r_hold.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch break;
    try resp.send(200, &.{}, "held");
}

/// One raw HTTP/1.1 exchange on its own thread: sends `req`, reads until the
/// server closes or `want` appears, and keeps what it read.
const H1Exchange = struct {
    port: u16,
    req: []const u8,
    want: []const u8,
    /// Hold the connection open after the answer, until released.
    hold: bool = false,
    got: [512]u8 = undefined,
    got_len: usize = 0,
    err: ?anyerror = null,
    answered: std.atomic.Value(bool) = .init(false),
    release: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),

    fn main(c: *H1Exchange) void {
        c.run() catch |err| {
            c.err = err;
        };
        c.answered.store(true, .release);
        c.done.store(true, .release);
    }

    fn run(c: *H1Exchange) !void {
        const w = try Wire.openAlpn(tsan_allocator.pageAllocator(), .h2c, c.port, null, "http/1.1");
        defer w.close();
        try w.send(c.req);
        h1ReadUntil(w, c.want, nowNs() + client_deadline_ns) catch |err| switch (err) {
            error.ClosedBeforeExpected => {},
            else => return err,
        };
        c.got_len = @min(w.plain.items.len, c.got.len);
        @memcpy(c.got[0..c.got_len], w.plain.items[0..c.got_len]);
        c.answered.store(true, .release);
        while (c.hold and !c.release.load(.acquire)) {
            starh2.test_sleep.threadSleepMs(5);
        }
    }

    fn text(c: *const H1Exchange) []const u8 {
        return c.got[0..c.got_len];
    }
};

/// The 503 cases for request memory on HTTP/1.1 (a request whose copy, or a
/// complete handler whose allocation, the memory refuses) are in
/// `tests/shares.zig`: there the client's share refuses. The connection's own
/// cap can no longer refuse on HTTP/1.1, which runs one request per
/// connection: a request's cap is at most its connection's (`Limits`
/// nesting), so the request's own cap (500) always comes first.
const H1Refusal = enum {
    task_bound,
    /// A complete handler whose allocation passes its request's own cap:
    /// 500.
    complete_request_cap,
};

/// Bytes `h1AllocHandler` asks of its request's arena.
var h1r_alloc: usize = 0;

fn h1AllocHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    const buf = try req.arena.alloc(u8, h1r_alloc);
    @memset(buf, 'a');
    try resp.send(200, &.{}, "allocated");
}

fn runH1Refusal(rt: *zio.Runtime, gpa: std.mem.Allocator, kind: H1Refusal) !void {
    const io = rt.io();
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/hold", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = h1HoldHandler } } },
        .{ .method = .POST, .path = "/hold", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = h1HoldHandler } } },
        .{ .method = .GET, .path = "/alloc", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = h1AllocHandler } } },
    };
    var limits = starh2.Limits.defaults;
    switch (kind) {
        // Compiles against 6ea2fcf too, where the bound was the reaper queue
        // and could not be below the stream bound: there the stream bound
        // refuses instead, with the same answer.
        .task_bound => if (@hasField(starh2.Limits, "max_task_handlers")) {
            @field(limits, "max_task_handlers") = 1;
        } else {
            limits.max_streams_per_connection = 1;
            limits.max_streams_per_server = 1;
            @field(limits, "cancellation_reaper_jobs") = 1;
        },
        .complete_request_cap => {},
    }
    var server = try starh2.Server.init(gpa, io, .{
        // The server's own bounds, from the one loopback client: `.none`, so
        // no client share refuses first (a task-handler bound of 1 is also a
        // share of 1).
        .endpoints = &.{.{ .h1c = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), .client_identity = .none } }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    h1r_hold.store(true, .release);
    defer {
        h1r_hold.store(false, .release);
        server.requestShutdown();
        serve_deadline.join(&server, &serve_handle);
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();

    switch (kind) {
        .task_bound => {
            // The first request takes the only token and holds it.
            var first: H1Exchange = .{ .port = port, .req = "GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n", .want = "held", .hold = true };
            const t1 = try std.Thread.spawn(.{}, H1Exchange.main, .{&first});
            defer {
                h1r_hold.store(false, .release);
                first.release.store(true, .release);
                starh2.test_sleep.joinThread(t1);
            }
            const deadline = nowNs() + 5 * std.time.ns_per_s;
            while (h1r_running.load(.acquire) != 1) {
                if (nowNs() >= deadline) return error.HandlerNeverStarted;
                zio.sleep(.fromMilliseconds(5)) catch {};
            }
            var second: H1Exchange = .{ .port = port, .req = "GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n", .want = "connection: close\r\n\r\n" };
            const t2 = try std.Thread.spawn(.{}, H1Exchange.main, .{&second});
            while (!second.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
            starh2.test_sleep.joinThread(t2);
            if (second.err) |err| return err;
            std.debug.print("h1 refusal task_bound: {s}\n", .{second.text()});
            try std.testing.expect(std.mem.startsWith(u8, second.text(), "HTTP/1.1 503"));
            try std.testing.expect(std.mem.indexOf(u8, second.text(), "connection: close") != null);
        },
        .complete_request_cap => {
            h1r_hold.store(false, .release);
            // Twice the request's own cap: no block class holds it.
            h1r_alloc = 2 * limits.request_arena_bytes;
            var c: H1Exchange = .{ .port = port, .req = "GET /alloc HTTP/1.1\r\nHost: localhost\r\n\r\n", .want = "connection: close\r\n\r\n" };
            const t = try std.Thread.spawn(.{}, H1Exchange.main, .{&c});
            while (!c.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
            starh2.test_sleep.joinThread(t);
            if (c.err) |err| return err;
            std.debug.print("h1 refusal {s}: {s}\n", .{ @tagName(kind), c.text() });
            try std.testing.expect(std.mem.startsWith(u8, c.text(), "HTTP/1.1 500"));
        },
    }
}

fn h1RefusalCase(kind: H1Refusal) !void {
    h1r_running.store(0, .release);
    const gpa = tsan_allocator.allocator();
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runH1Refusal, .{ rt, gpa, kind });
    try h.join();
}

test "h1 refusal: a task request past the task-handler bound is answered 503 and closed (t-3759)" {
    try h1RefusalCase(.task_bound);
}
test "h1 refusal: a complete handler over its request's own cap answers 500 (t-3759 round 1)" {
    try h1RefusalCase(.complete_request_cap);
}

// A TLS handler in the middle of one write larger than a TLS write chunk
// when its connection ends (rounds 1 and 2 of the step-7 review). The
// handler hands the actor one chunk at a time (`tls_req_ch`) and waits for
// its ack (`tls_ok_ch`); teardown closes both. Whichever it meets, the write
// must fail with the connection's cause. Each case runs `h1m_rounds`
// connections, the client reading as fast as it can, and checks every round
// three ways: the client was still reading and the handler still writing
// when the trigger came (so the trigger, not something before it, ended the
// connection), the write failed `ConnectionClosed`, and the cause recorded
// is the trigger's (`connection_closed` for the peer's close,
// `server_shutdown` for a shutdown or a canceled `serve`).
//
// `canceled_serve` (round 2, astra): `serve` fails to start its second
// listener (the step-2 test hook) and cancels every connection task, so the
// actor's wait for the handler returns `error.Canceled` and the request ends
// on `serve`'s exit path (`endOnExit`).

const h1m_rounds = 12;
const H1MultiTrigger = enum { peer_close, shutdown, canceled_serve };
var h1m_started: std.atomic.Value(bool) = .init(false);
var h1m_done: std.atomic.Value(bool) = .init(false);
/// How the handler's last write failed: 0 not yet, 1 `ConnectionClosed`,
/// 2 another error.
var h1m_end: std.atomic.Value(u8) = .init(0);
var h1m_err: std.atomic.Value(u16) = .init(0);
/// The cause the failed write saw (`h1CauseCode`), 0 none.
var h1m_cause: std.atomic.Value(u8) = .init(0);
/// Whole writes that went out, so a round that never wrote (the instrument
/// not firing) is seen.
var h1m_writes: std.atomic.Value(usize) = .init(0);
var h1m_writes_at_end: std.atomic.Value(usize) = .init(0);

fn h1MultiChunkWriter(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    defer h1m_done.store(true, .release);
    var body = try resp.startSse(&.{});
    // Several TLS write chunks per `writeAll` (a chunk is a little over
    // 16 KiB), and below `outbound_bytes_per_stream` (64 KiB), the most one
    // HTTP/1.1 write may hold: a larger one fails at once with
    // `WriteFailed`, whatever the connection does.
    var big: [48 * 1024]u8 = undefined;
    @memset(&big, 'm');
    zio.beginShield();
    defer zio.endShield();
    h1m_started.store(true, .release);
    while (true) {
        body.writeAll(&big) catch |err| {
            h1m_err.store(@intFromError(err), .release);
            h1m_cause.store(if (body.terminalCause()) |c| h1CauseCode(c) else 0, .release);
            h1m_writes_at_end.store(h1m_writes.load(.acquire), .release);
            h1m_end.store(if (err == error.ConnectionClosed) 1 else 2, .release);
            return;
        };
        _ = h1m_writes.fetchAdd(1, .acq_rel);
    }
}

/// Reads and decrypts everything until released, then closes. Any read
/// error or end before `release` is kept (`err`, `ended_early`): the test
/// checks the client was still reading when it pulled the trigger.
const H1Drainer = struct {
    port: u16,
    /// Retry a refused connect for a while: the server is held before it
    /// listens (`canceled_serve`), and only the kernel's accept says when.
    connect_retry: bool = false,
    err: ?anyerror = null,
    /// The server closed before the client was released.
    ended_early: bool = false,
    reading: std.atomic.Value(bool) = .init(false),
    release: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),

    fn main(c: *H1Drainer) void {
        c.run() catch |err| {
            c.err = err;
        };
        c.reading.store(true, .release);
        c.done.store(true, .release);
    }

    fn run(c: *H1Drainer) !void {
        const deadline = nowNs() + 5 * std.time.ns_per_s;
        const w = while (true) {
            break Wire.openAlpn(tsan_allocator.pageAllocator(), .tls, c.port, null, "http/1.1") catch |err| switch (err) {
                error.Connect => {
                    if (!c.connect_retry or nowNs() >= deadline) return err;
                    starh2.test_sleep.threadSleepMs(2);
                    continue;
                },
                else => return err,
            };
        };
        defer w.close();
        try w.send("GET /multi HTTP/1.1\r\nHost: localhost\r\n\r\n");
        c.reading.store(true, .release);
        while (!c.release.load(.acquire)) {
            const more = w.pump() catch |err| switch (err) {
                error.WouldBlock => continue,
                else => return err,
            };
            if (!more) {
                c.ended_early = true;
                return;
            }
            w.plain.clearRetainingCapacity();
        }
    }
};

const H1MultiTally = struct {
    closed: usize = 0,
    other: usize = 0,
    writes: usize = 0,
};

fn runH1MultiChunk(rt: *zio.Runtime, gpa: std.mem.Allocator, trigger: H1MultiTrigger, tally: *H1MultiTally) !void {
    const io = rt.io();
    const server_mod = starh2.edge.server;
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/multi", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = h1MultiChunkWriter } } },
    };
    const cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert_pem);
    const key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key_pem);
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    // `canceled_serve`: a second listener whose accept loop fails to spawn
    // once the test opens the gate. That server is held before it listens,
    // so its `local_addrs` are never published to this task: the first
    // listener's port is the test's own (`wire_client.freeLoopbackPort`),
    // and the client retries until the kernel accepts.
    const held_port: u16 = if (trigger == .canceled_serve) try wire_client.freeLoopbackPort() else 0;
    const held_addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", held_port);
    const endpoints: []const starh2.EndpointConfig = if (trigger == .canceled_serve) &.{ .{ .tls = .{ .address = held_addr } }, .{ .tls = .{ .address = addr } } } else &.{.{ .tls = .{ .address = addr } }};
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = endpoints,
        .routes = &routes,
        .tls = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem },
        .limits = starh2.Limits.defaults,
    });
    defer server.deinit(gpa);
    if (trigger == .canceled_serve) {
        server_mod.test_accept_spawn_gate.store(false, .release);
        server_mod.test_fail_accept_spawn.store(1, .release);
    }
    defer if (trigger == .canceled_serve) {
        server_mod.test_fail_accept_spawn.store(std.math.maxInt(usize), .release);
        server_mod.test_accept_spawn_gate.store(true, .release);
    };
    h1m_started.store(false, .release);
    h1m_done.store(false, .release);
    h1m_end.store(0, .release);
    h1m_cause.store(0, .release);
    h1m_writes.store(0, .release);
    h1m_writes_at_end.store(0, .release);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var client: H1Drainer = .{ .port = 0, .connect_retry = trigger == .canceled_serve };
    var client_thread: ?std.Thread = null;
    defer {
        server.requestShutdown();
        if (trigger == .canceled_serve) server_mod.test_accept_spawn_gate.store(true, .release);
        client.release.store(true, .release);
        if (client_thread) |t| starh2.test_sleep.joinThread(t);
        serve_deadline.join(&server, &serve_handle);
    }
    const t0 = nowNs();
    if (trigger == .canceled_serve) {
        client.port = held_port;
    } else {
        try server.waitUntilListening(5 * std.time.ns_per_s);
        client.port = server.localAddress(0).getPort();
    }
    client_thread = try std.Thread.spawn(.{}, H1Drainer.main, .{&client});
    while (!h1m_started.load(.acquire)) {
        if (client.done.load(.acquire)) return client.err orelse error.ClientEndedBeforeHandler;
        if (nowNs() >= t0 + 5 * std.time.ns_per_s) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
    // Let a few chunks go by, then end the connection under the write.
    zio.sleep(.fromMilliseconds(50)) catch {};
    // The trigger must be what ends it: the client still reads, the handler
    // still writes.
    if (client.done.load(.acquire)) {
        std.debug.print("h1 multi-chunk {s}: the client ended before the trigger: err={?} ended_early={}\n", .{ @tagName(trigger), client.err, client.ended_early });
        return error.ClientEndedBeforeTrigger;
    }
    if (h1m_done.load(.acquire)) return error.HandlerEndedBeforeTrigger;
    switch (trigger) {
        .peer_close => client.release.store(true, .release),
        .shutdown => server.requestShutdown(),
        .canceled_serve => server_mod.test_accept_spawn_gate.store(true, .release),
    }
    while (!h1m_done.load(.acquire)) {
        if (nowNs() >= t0 + 10 * std.time.ns_per_s) return error.HandlerNeverEnded;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
    // The write under test is one after at least one that went out whole.
    if (h1m_writes_at_end.load(.acquire) == 0) return error.NoWriteWentOut;
    tally.writes += h1m_writes_at_end.load(.acquire);
    const want_cause = h1CauseCode(switch (trigger) {
        .peer_close => .connection_closed,
        .shutdown, .canceled_serve => .server_shutdown,
    });
    if (h1m_end.load(.acquire) == 1 and h1m_cause.load(.acquire) == want_cause) {
        tally.closed += 1;
    } else {
        tally.other += 1;
        std.debug.print("h1 multi-chunk {s}: a write failed with {s}, cause {d} (want ConnectionClosed, cause {d})\n", .{
            @tagName(trigger), @errorName(@errorFromInt(h1m_err.load(.acquire))), h1m_cause.load(.acquire), want_cause,
        });
    }
}

fn h1MultiChunkCase(trigger: H1MultiTrigger) !void {
    const gpa = tsan_allocator.allocator();
    var tally: H1MultiTally = .{};
    for (0..h1m_rounds) |_| {
        const rt = try newRuntime(gpa);
        defer rt.deinit();
        var h = try rt.spawn(runH1MultiChunk, .{ rt, gpa, trigger, &tally });
        try h.join();
    }
    std.debug.print("h1 multi-chunk {s}: {d} rounds, {d} whole 48 KiB writes before the end, {d} ended ConnectionClosed with the trigger's cause, {d} otherwise\n", .{ @tagName(trigger), h1m_rounds, tally.writes, tally.closed, tally.other });
    try std.testing.expectEqual(@as(usize, 0), tally.other);
}

test "outlive: h1 tls a handler between the chunks of one write fails with the cause when the peer closes (t-3759 round 1)" {
    try h1MultiChunkCase(.peer_close);
}
test "outlive: h1 tls a handler between the chunks of one write fails with the cause at shutdown (t-3759 round 1)" {
    try h1MultiChunkCase(.shutdown);
}
test "outlive: h1 tls a handler between the chunks of one write fails with the cause when serve is canceled (t-3759 round 2)" {
    try h1MultiChunkCase(.canceled_serve);
}

// A client that resets its connection during a response (round 2, Fable).
// The HTTP/1.1 TLS pump reports every failure as write completions (one for
// `failDrain`, one for the shutdown), and the first version gave it a
// four-slot channel nothing ever read, with a panic when it was full: two
// failures and a `deinit` crashed the whole server. Each round asks for a
// response two TLS write chunks long from a complete handler (it runs on the
// actor, through the actor's own write path) and resets the connection
// (SO_LINGER 0) after the response head, or straight after the request.
// The server must survive every round and still serve a well-behaved
// client.

const h1x_rounds = 40;

fn h1BigHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    var body: [30 * 1024]u8 = undefined;
    @memset(&body, 'r');
    try resp.send(200, &.{}, &body);
}

/// Resets each connection with SO_LINGER 0: the close sends RST.
const H1Resetter = struct {
    port: u16,
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),

    fn main(c: *H1Resetter) void {
        c.run() catch |err| {
            c.err = err;
        };
        c.done.store(true, .release);
    }

    fn run(c: *H1Resetter) !void {
        for (0..h1x_rounds) |i| {
            const w = try Wire.openAlpn(tsan_allocator.pageAllocator(), .tls, c.port, null, "http/1.1");
            defer w.close();
            try w.send("GET /big HTTP/1.1\r\nHost: localhost\r\n\r\n");
            // Even rounds: reset once the head is in, with the body still
            // being written. Odd rounds: reset at once.
            if (i % 2 == 0) h1ReadUntil(w, "\r\n\r\n", nowNs() + client_deadline_ns) catch {};
            const lg: std.c.linger = .{ .onoff = 1, .linger = 0 };
            _ = std.c.setsockopt(w.fd, std.c.SOL.SOCKET, std.c.SO.LINGER, std.mem.asBytes(&lg), @sizeOf(std.c.linger));
        }
    }
};

fn runH1Reset(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const io = rt.io();
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/big", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = h1BigHandler } } },
        .{ .method = .GET, .path = "/ok", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = h1OkHandler } } },
    };
    const cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert_pem);
    const key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key_pem);
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{.{ .tls = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }},
        .routes = &routes,
        .tls = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem },
        .limits = starh2.Limits.defaults,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_deadline.join(&server, &serve_handle);
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();
    const failures_before: u32 = if (@hasDecl(starh2.edge.h1, "test_tls_failures")) blk: {
        @field(starh2.edge.h1, "test_tls_failures_max").store(0, .release);
        break :blk @field(starh2.edge.h1, "test_tls_failures").load(.acquire);
    } else 0;
    var c: H1Resetter = .{ .port = port };
    const t = try std.Thread.spawn(.{}, H1Resetter.main, .{&c});
    while (!c.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    starh2.test_sleep.joinThread(t);
    if (c.err) |err| return err;
    // Every reset connection is gone, and the server still serves.
    const t0 = nowNs();
    while (server.active_connections.load(.acquire) != 0) {
        if (nowNs() >= t0 + 5 * std.time.ns_per_s) return error.ResetConnectionsStayed;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    try h1ServedOk(port, .tls);
    std.debug.print("h1 tls reset: {d} connections reset during or right after the request; the server still serves\n", .{h1x_rounds});
    // Compiles against versions without the counters too.
    const h1 = starh2.edge.h1;
    if (@hasDecl(h1, "test_tls_failures")) {
        const failures = @field(h1, "test_tls_failures").load(.acquire) - failures_before;
        const most = @field(h1, "test_tls_failures_max").load(.acquire);
        std.debug.print("h1 tls reset: {d} TLS pump failures in all, at most {d} on one connection\n", .{ failures, most });
        // The attack reached the pump's failure path at all ...
        try std.testing.expect(failures >= 1);
        // ... and no connection went on writing after its pump failed.
        try std.testing.expectEqual(@as(u32, 1), most);
    }
}

test "h1 tls: clients that reset during a response do not crash the server (t-3759 round 2)" {
    const gpa = tsan_allocator.allocator();
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runH1Reset, .{ rt, gpa });
    try h.join();
}
