//! HTTP/1.1 lingering close (t-3883, RFC 9112 section 9.6). When the server
//! closes a connection that has, or may have, unread input, it half-closes,
//! reads and discards, and closes on the peer's EOF, an error, 500 ms with no
//! data, or 2 s in all. Without that, a close with unread input makes the
//! kernel send a RST, which can destroy the response the client has not read
//! yet. h1c and TLS, real sockets; every client runs on an OS thread of its
//! own (the `Wire` calls block), every wait has a time limit, and the test
//! task waits with `zio.sleep`, never a blocking sleep.
//!
//! A request with a `Content-Length` above the body cap is answered 413
//! before its body is read: the body is the unread input.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const wire_client = @import("wire_client.zig");
const serve_deadline = @import("serve_deadline.zig");

const Transport = wire_client.Transport;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;
const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "hello");
}

fn upload(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    var buf: [24]u8 = undefined;
    try resp.send(200, &.{}, try std.fmt.bufPrint(&buf, "{d}", .{req.body.len}));
}

/// Holds the connection's actor for 150 ms: a complete handler runs inline on
/// it, so for that time nothing the client sends is read. A blocking sleep on
/// purpose, here and nowhere else: it is the only way to leave input in the
/// socket with every buffer of the server empty (the FIONREAD case).
fn busy(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    const t: std.c.timespec = .{ .sec = 0, .nsec = 150 * ms };
    _ = std.c.nanosleep(&t, null);
    try resp.send(200, &.{}, "busy");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    .{ .method = .GET, .path = "/busy", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = busy } } },
    .{ .method = .POST, .path = "/up", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = upload } } },
};

const Rig = struct {
    gpa: std.mem.Allocator,
    server: starh2.Server,
    handle: zio.JoinHandle(starh2.ServeError!void),
    cert: []u8 = &.{},
    key: []u8 = &.{},

    fn start(self: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
        const io = rt.io();
        self.* = .{ .gpa = gpa, .server = undefined, .handle = undefined };
        var tls_cfg: ?starh2.TlsConfig = null;
        if (tr == .tls) {
            self.cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            self.key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            tls_cfg = .{ .certificate_chain_pem = self.cert, .private_key_pem = self.key };
        }
        errdefer {
            if (self.cert.len != 0) gpa.free(self.cert);
            if (self.key.len != 0) gpa.free(self.key);
        }
        const a4 = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
        const ep: starh2.EndpointConfig = switch (tr) {
            .h2c => .{ .h1c = .{ .address = a4 } },
            .tls => .{ .tls = .{ .address = a4 } },
        };
        var limits = starh2.Limits.defaults;
        limits.graceful_drain_timeout_ns = 300 * ms;
        self.server = try starh2.Server.init(gpa, io, .{ .endpoints = &.{ep}, .routes = &routes, .tls = tls_cfg, .limits = limits });
        self.handle = try rt.spawn(starh2.Server.serve, .{ &self.server, gpa });
        try self.server.waitUntilListening(5 * std.time.ns_per_s);
    }

    fn port(self: *Rig) u16 {
        return self.server.localAddress(0).getPort();
    }

    fn connections(self: *Rig) usize {
        return self.server.active_connections.load(.acquire);
    }

    fn stop(self: *Rig) void {
        self.server.requestShutdown();
        serve_deadline.join(&self.server, &self.handle);
        self.server.deinit(self.gpa);
        if (self.cert.len != 0) self.gpa.free(self.cert);
        if (self.key.len != 0) self.gpa.free(self.key);
    }
};

fn runIn(comptime f: anytype, tr: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(f, .{ rt, gpa, tr });
    try h.join();
}

fn blocking(comptime f: anytype, args: anytype) !void {
    var h = try zio.spawnBlocking(f, args);
    return h.join();
}

const h1 = starh2.edge.h1;
const Reason = h1.LingerReason;

/// The process-wide linger counters, read before and after a case.
const Lingers = struct {
    started: u64,
    reasons: [4]u64,

    fn now() Lingers {
        var r: Lingers = .{ .started = h1.linger_started.load(.acquire), .reasons = undefined };
        for (&r.reasons, 0..) |*v, i| v.* = h1.linger_reasons[i].load(.acquire);
        return r;
    }

    fn since(after: Lingers, before: Lingers) Lingers {
        var r: Lingers = .{ .started = after.started - before.started, .reasons = undefined };
        for (&r.reasons, 0..) |*v, i| v.* = after.reasons[i] - before.reasons[i];
        return r;
    }

    fn of(self: Lingers, why: Reason) u64 {
        return self.reasons[@intFromEnum(why)];
    }
};

fn expectLinger(l: Lingers, started: u64, why: ?Reason) !void {
    try expectExactly("lingered closes", l.started, started);
    if (why) |w| try expectExactly("lingered closes for the expected reason", l.of(w), started);
}

fn expectExactly(comptime what: []const u8, got: anytype, want: anytype) !void {
    if (got == want) return;
    std.debug.print("h1 linger: {s} is {d}, want {d}\n", .{ what, got, want });
    return error.WrongCount;
}

fn expectWithin(comptime what: []const u8, got_ms: u64, lo: u64, hi: u64) !void {
    if (got_ms >= lo and got_ms <= hi) return;
    std.debug.print("h1 linger: {s} took {d} ms, want {d} to {d}\n", .{ what, got_ms, lo, hi });
    return error.WrongTime;
}

/// OS-thread sleep, for the client threads only (never on a zio task).
fn osSleepMs(n: u64) void {
    const t: std.c.timespec = .{ .sec = @intCast(n / 1000), .nsec = @intCast((n % 1000) * ms) };
    _ = std.c.nanosleep(&t, null);
}

// ---------------------------------------------------------------------------
// The client.

fn open(gpa: std.mem.Allocator, tr: Transport, port: u16) !*Wire {
    // Sends block: a server that stops reading either drains (the linger)
    // or closes, which fails the send; the linger's 2 s bounds both.
    const w = try Wire.connect(gpa, tr, port);
    errdefer w.close();
    if (tr == .tls) {
        w.alpn = "http/1.1";
        try w.handshake();
    }
    return w;
}

/// Send all of `bytes` (TLS: as records); the errno of a failed send.
fn sendOnce(w: *Wire, bytes: []const u8) ?std.c.E {
    if (w.transport == .h2c) return sendAll(w.fd, bytes);
    var off: usize = 0;
    while (off < bytes.len) {
        const rec = w.conn.encrypt(bytes[off..][0..@min(bytes.len - off, 16 * 1024)], &w.out) catch return .PROTO;
        if (sendAll(w.fd, rec.ciphertext)) |e| return e;
        off += rec.cleartext_pos;
    }
    return null;
}

fn sendAll(fd: c_int, bytes: []const u8) ?std.c.E {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = std.c.send(fd, bytes[off..].ptr, bytes.len - off, 0);
        if (n < 0) {
            const e = std.c.errno(n);
            if (e == .INTR) continue;
            return e;
        }
        off += @intCast(n);
    }
    return null;
}

/// What the client read back and how the connection ended for it.
const Read = struct {
    bytes: usize = 0,
    status: u16 = 0,
    /// The whole response arrived: its head, and a body of its
    /// `content-length`.
    complete: bool = false,
    eof: bool = false,
    reset: bool = false,
    timed_out: bool = false,
    /// When the response was complete (`nowNs`).
    complete_ns: u64 = 0,
    /// Bytes read past the first response before the connection's end.
    after: usize = 0,
};

fn contentLength(head: []const u8) ?usize {
    var it = std.mem.splitSequence(u8, head, "\r\n");
    while (it.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, line[0..colon], " "), "content-length")) continue;
        return std.fmt.parseInt(usize, std.mem.trim(u8, line[colon + 1 ..], " "), 10) catch null;
    }
    return null;
}

/// Read until the response is complete; with `to_end`, go on to the
/// connection's end (EOF or reset). Gives up at `deadline`.
fn readResponse(w: *Wire, r: *Read, deadline: u64, to_end: bool) !void {
    while (nowNs() < deadline) {
        const p = w.plain.items;
        if (!r.complete) {
            if (std.mem.indexOf(u8, p, "\r\n\r\n")) |he| {
                if (p.len >= 12 and std.mem.startsWith(u8, p, "HTTP/1.1 ")) r.status = std.fmt.parseInt(u16, p[9..12], 10) catch 0;
                const cl = contentLength(p[0..he]) orelse 0;
                if (p.len >= he + 4 + cl) {
                    r.complete = true;
                    r.complete_ns = nowNs();
                    r.bytes = he + 4 + cl;
                    if (!to_end) return;
                }
            }
        }
        const more = w.pump() catch |err| switch (err) {
            error.WouldBlock => continue,
            else => {
                r.reset = true;
                return;
            },
        };
        if (!more) {
            if (w.reset) r.reset = true else r.eof = true;
            if (r.complete) r.after = w.plain.items.len - r.bytes;
            return;
        }
    }
    r.timed_out = true;
}

fn soError(w: *Wire) i32 {
    var e: c_int = 0;
    var len: std.c.socklen_t = @sizeOf(c_int);
    _ = std.c.getsockopt(w.fd, std.c.SOL.SOCKET, std.c.SO.ERROR, std.mem.asBytes(&e), &len);
    return e;
}

const too_big = "POST /up HTTP/1.1\r\nHost: localhost\r\nContent-Length: 8388608\r\n\r\n";

// ---------------------------------------------------------------------------
// 1. The RST case.

const Bulk = struct {
    sent: usize = 0,
    send_err: ?std.c.E = null,
    so_error: i32 = 0,
    read: Read = .{},
    /// From the end of the sending to the connection's end, as the client
    /// saw it.
    end_ms: u64 = 0,
    err: ?anyerror = null,
};

fn bulkClient(gpa: std.mem.Allocator, tr: Transport, port: u16, r: *Bulk) void {
    bulkInner(gpa, tr, port, r) catch |err| {
        r.err = err;
    };
}

/// A refused request (413 before its body is read) followed by its body,
/// sent for 300 ms, far more than the socket buffers hold. Then the client
/// reads: the whole 413, then the connection's end.
fn bulkInner(gpa: std.mem.Allocator, tr: Transport, port: u16, r: *Bulk) !void {
    const w = try open(gpa, tr, port);
    defer w.close();
    if (sendOnce(w, too_big)) |e| r.send_err = e;
    var chunk: [16 * 1024]u8 = @splat('b');
    const stop = nowNs() + 300 * ms;
    while (r.send_err == null and nowNs() < stop) {
        if (sendOnce(w, &chunk)) |e| {
            r.send_err = e;
            break;
        }
        r.sent += chunk.len;
    }
    const t = nowNs();
    r.so_error = soError(w);
    try readResponse(w, &r.read, nowNs() + 5 * std.time.ns_per_s, true);
    r.end_ms = (nowNs() - t) / ms;
}

fn rstCase(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr);
    defer rig.stop();
    const l0 = Lingers.now();
    var r: Bulk = .{};
    try blocking(bulkClient, .{ gpa, tr, rig.port(), &r });
    const d = nowNs() + 5 * std.time.ns_per_s;
    while (rig.connections() != 0 and nowNs() < d) zio.sleep(.fromMilliseconds(2)) catch {};
    const l = Lingers.now().since(l0);
    std.debug.print("h1 linger RST case ({s}): body bytes sent {d} in 300 ms, send error {?}; SO_ERROR after {d}; read {d} bytes, status {d}, complete {}; then eof {} reset {} timed out {} after {d} ms; connections now {d}; err {?}\n", .{
        @tagName(tr), r.sent,          r.send_err,       r.so_error,  r.read.bytes, r.read.status, r.read.complete,
        r.read.eof,   r.read.reset,    r.read.timed_out, r.end_ms,    rig.connections(), r.err,
    });
    try std.testing.expect(r.err == null);
    try expectExactly("the status", r.read.status, 413);
    try std.testing.expect(r.read.complete);
    try std.testing.expect(r.read.eof and !r.read.reset);
    // The half-close: the FIN follows the response, so the client sees its
    // EOF at once, not when the linger ends.
    try expectWithin("the EOF after the client stopped sending", r.end_ms, 0, 150);
    try expectExactly("SO_ERROR", r.so_error, 0);
    try std.testing.expect(r.send_err == null);
    // The drain ran: far more than the socket buffers hold went through.
    if (r.sent < 4 * 1024 * 1024) {
        std.debug.print("h1 linger: only {d} body bytes sent in 300 ms; the drain did not run\n", .{r.sent});
        return error.NoDrain;
    }
    try expectLinger(l, 1, .request_unread);
    try expectConserved(&rig);
}

test "a refused request's unread body: the server drains it, the client reads the whole 413 and then EOF, no RST (h1c)" {
    try runIn(rstCase, .h2c);
}
test "a refused request's unread body: the server drains it, the client reads the whole 413 and then EOF, no RST (TLS)" {
    try runIn(rstCase, .tls);
}

// ---------------------------------------------------------------------------
// 2. The bounds, and no linger without unread input.

const After = enum {
    /// One byte every 100 ms after the 413: closed at the total bound.
    trickle,
    /// Nothing after the 413: closed at the idle bound.
    silent,
    /// The client half-closes right after the 413: the linger ends at once.
    client_close,
    /// GET with `Connection: close`, read whole: no unread input, no linger.
    no_unread,
};

const Bound = struct {
    read: Read = .{},
    /// The server's connection count 200 ms into the wait (the slot is held
    /// while it lingers).
    held_at_200ms: usize = 0,
    /// From the complete response to the count reaching 0.
    closed_ms: u64 = 0,
    closed: bool = false,
    err: ?anyerror = null,
};

fn boundClient(gpa: std.mem.Allocator, tr: Transport, rig: *Rig, how: After, r: *Bound) void {
    boundInner(gpa, tr, rig, how, r) catch |err| {
        r.err = err;
    };
}

fn boundInner(gpa: std.mem.Allocator, tr: Transport, rig: *Rig, how: After, r: *Bound) !void {
    const w = try open(gpa, tr, rig.port());
    defer w.close();
    switch (how) {
        .no_unread => _ = sendOnce(w, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
        else => {
            _ = sendOnce(w, too_big);
            const some: [1024]u8 = @splat('b');
            _ = sendOnce(w, &some);
        },
    }
    try readResponse(w, &r.read, nowNs() + 3 * std.time.ns_per_s, false);
    if (!r.read.complete) return error.NoResponse;
    const t0 = r.read.complete_ns;
    if (how == .client_close) _ = std.c.shutdown(w.fd, std.c.SHUT.WR);
    var next_byte = t0 + 100 * ms;
    var sampled = false;
    const deadline = t0 + 5 * std.time.ns_per_s;
    while (nowNs() < deadline) {
        const now = nowNs();
        if (rig.connections() == 0) {
            r.closed = true;
            r.closed_ms = (now - t0) / ms;
            return;
        }
        if (!sampled and now >= t0 + 200 * ms) {
            sampled = true;
            r.held_at_200ms = rig.connections();
        }
        if (how == .trickle and now >= next_byte) {
            _ = sendOnce(w, "b");
            next_byte += 100 * ms;
        }
        osSleepMs(1);
    }
}

fn boundCase(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport, how: After) !void {
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr);
    defer rig.stop();
    const l0 = Lingers.now();
    var r: Bound = .{};
    try blocking(boundClient, .{ gpa, tr, &rig, how, &r });
    const l = Lingers.now().since(l0);
    std.debug.print("h1 linger {s} ({s}): status {d}; slot held 200 ms in: {d}; closed {} {d} ms after the response; lingered {d}; err {?}\n", .{
        @tagName(how), @tagName(tr), r.read.status, r.held_at_200ms, r.closed, r.closed_ms, l.started, r.err,
    });
    try std.testing.expect(r.err == null);
    try std.testing.expect(r.closed);
    switch (how) {
        .trickle => {
            try expectExactly("the status", r.read.status, 413);
            try expectExactly("connections 200 ms in", r.held_at_200ms, 1);
            try expectWithin("a trickling client's close", r.closed_ms, 1800, 2600);
            try expectLinger(l, 1, .request_unread);
        },
        .silent => {
            try expectExactly("the status", r.read.status, 413);
            try expectExactly("connections 200 ms in", r.held_at_200ms, 1);
            try expectWithin("a silent client's close", r.closed_ms, 400, 900);
            try expectLinger(l, 1, .request_unread);
        },
        .client_close => {
            try expectExactly("the status", r.read.status, 413);
            try expectWithin("the close after the client's EOF", r.closed_ms, 0, 150);
        },
        .no_unread => {
            try expectExactly("the status", r.read.status, 200);
            try expectWithin("a close with no unread input", r.closed_ms, 0, 150);
            try expectLinger(l, 0, null);
        },
    }
    try expectConserved(&rig);
}

fn expectConserved(rig: *Rig) !void {
    try expectExactly("connections", rig.connections(), 0);
    try expectExactly("request pool", rig.server.requests.held_bytes.load(.acquire), 0);
    try expectExactly("server underflows", rig.server.accounting.underflows.load(.acquire), 0);
    try expectExactly("client table underflows", rig.server.clients.stats().underflows, 0);
}

fn trickle(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return boundCase(rt, gpa, tr, .trickle);
}
fn silent(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return boundCase(rt, gpa, tr, .silent);
}
fn clientClose(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return boundCase(rt, gpa, tr, .client_close);
}
fn noUnread(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return boundCase(rt, gpa, tr, .no_unread);
}

test "a client that trickles a byte every 100 ms after its 413 is closed at the 2 s total bound (h1c)" {
    try runIn(trickle, .h2c);
}
test "a client that trickles a byte every 100 ms after its 413 is closed at the 2 s total bound (TLS)" {
    try runIn(trickle, .tls);
}
test "a client that goes silent after its 413 is closed at the 500 ms idle bound, its slot held until then (h1c)" {
    try runIn(silent, .h2c);
}
test "a client that goes silent after its 413 is closed at the 500 ms idle bound, its slot held until then (TLS)" {
    try runIn(silent, .tls);
}
test "a client that half-closes after its 413 ends the linger at once (h1c)" {
    try runIn(clientClose, .h2c);
}
test "a client that half-closes after its 413 ends the linger at once (TLS)" {
    try runIn(clientClose, .tls);
}
test "a Connection: close request read whole closes at once, with no linger (h1c)" {
    try runIn(noUnread, .h2c);
}
test "a Connection: close request read whole closes at once, with no linger (TLS)" {
    try runIn(noUnread, .tls);
}

// ---------------------------------------------------------------------------
// 3. The other reasons: a pipelined request, a parse reject, input left in
//    the socket.

const Other = enum {
    /// Two requests in one write, the first with `Connection: close`: the
    /// second is read and never served (bytes read, not consumed).
    pipelined,
    /// A head the parser rejects (400): the rest of the request is unread.
    reject,
    /// `/busy` with `Connection: close`, then 64 KiB more while the actor is
    /// held: on h1c the bytes wait in the socket, read by nothing (FIONREAD).
    queued,
};

const OtherResult = struct {
    read: Read = .{},
    send_err: ?std.c.E = null,
    err: ?anyerror = null,
};

fn otherClient(gpa: std.mem.Allocator, tr: Transport, port: u16, how: Other, r: *OtherResult) void {
    otherInner(gpa, tr, port, how, r) catch |err| {
        r.err = err;
    };
}

fn otherInner(gpa: std.mem.Allocator, tr: Transport, port: u16, how: Other, r: *OtherResult) !void {
    const w = try open(gpa, tr, port);
    defer w.close();
    switch (how) {
        .pipelined => r.send_err = sendOnce(w, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\nGET / HTTP/1.1\r\nHost: localhost\r\n\r\n"),
        .reject => r.send_err = sendOnce(w, "GET / HTTP/1.1\r\nHost: localhost\r\nno colon here\r\n\r\n"),
        .queued => {
            r.send_err = sendOnce(w, "GET /busy HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
            osSleepMs(30);
            const more: [64 * 1024]u8 = @splat('q');
            if (r.send_err == null) r.send_err = sendOnce(w, &more);
        },
    }
    try readResponse(w, &r.read, nowNs() + 5 * std.time.ns_per_s, true);
}

fn otherCase(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport, how: Other) !void {
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr);
    defer rig.stop();
    const l0 = Lingers.now();
    var r: OtherResult = .{};
    try blocking(otherClient, .{ gpa, tr, rig.port(), how, &r });
    const d = nowNs() + 5 * std.time.ns_per_s;
    while (rig.connections() != 0 and nowNs() < d) zio.sleep(.fromMilliseconds(2)) catch {};
    const l = Lingers.now().since(l0);
    std.debug.print("h1 linger {s} ({s}): status {d}, complete {}, bytes {d}; then eof {} reset {}; send error {?}; lingered {d} (request_unread {d}, unconsumed {d}, queued {d}, queued_unknown {d}); err {?}\n", .{
        @tagName(how), @tagName(tr), r.read.status, r.read.complete, r.read.bytes, r.read.eof, r.read.reset, r.send_err, l.started,
        l.of(.request_unread), l.of(.unconsumed), l.of(.queued), l.of(.queued_unknown), r.err,
    });
    try std.testing.expect(r.err == null);
    try std.testing.expect(r.send_err == null);
    try expectExactly("the status", r.read.status, @as(u16, if (how == .reject) 400 else 200));
    try std.testing.expect(r.read.complete);
    try std.testing.expect(r.read.eof and !r.read.reset);
    // One response only: the pipelined second request is never served.
    try expectExactly("bytes after the first response", r.read.after, 0);
    try expectLinger(l, 1, switch (how) {
        .pipelined => .unconsumed,
        .reject => .request_unread,
        // On h1c nothing reads while the actor is held, so the bytes are
        // only in the socket: FIONREAD is the one witness. On TLS, writing
        // the response drives the pump, which takes the waiting ciphertext
        // into its own buffer, so the same input is `unconsumed` there.
        .queued => if (tr == .h2c) .queued else .unconsumed,
    });
    try expectConserved(&rig);
}

fn pipelined(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return otherCase(rt, gpa, tr, .pipelined);
}
fn reject(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return otherCase(rt, gpa, tr, .reject);
}
fn queued(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return otherCase(rt, gpa, tr, .queued);
}

test "a pipelined request behind Connection: close is read, not served, and the close lingers (h1c)" {
    try runIn(pipelined, .h2c);
}
test "a pipelined request behind Connection: close is read, not served, and the close lingers (TLS)" {
    try runIn(pipelined, .tls);
}
test "a parse reject answers 400 and the close lingers over the unread rest (h1c)" {
    try runIn(reject, .h2c);
}
test "a parse reject answers 400 and the close lingers over the unread rest (TLS)" {
    try runIn(reject, .tls);
}
test "input left in the socket while the actor was busy makes the close linger, through FIONREAD (h1c)" {
    try runIn(queued, .h2c);
}
test "input left in the socket while the actor was busy makes the close linger, through FIONREAD (TLS)" {
    try runIn(queued, .tls);
}
