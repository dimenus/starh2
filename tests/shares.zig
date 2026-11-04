//! Shares under pressure (t-3759 step 4): one attack per row of the design's
//! share table, on real sockets.
//!
//! The rule (`accounts.zig` has it in full): for a resource whose server
//! limit is L, the pressure mark is L/4 of the server's use; a client's share
//! is L/8 and an IPv6 aggregate's L/4. While the server is at or above the
//! mark, a charge that would take a client past its share is refused. So one
//! identity alone holds at most its cap, L/4: it reaches the mark itself and
//! is then over its share. A second identity is still admitted up to a full
//! share during that attack.
//!
//! Every row runs the same shape, where its transport applies (h2c, TLS,
//! HTTP/1.1):
//!
//! 1. The attack, serially: one identity (127.0.0.1) takes the resource one
//!    at a time, many times, over many connections where the resource allows
//!    it. It holds exactly its cap; every attempt past it is refused, in the
//!    row's own way (closed at accept, RST_STREAM REFUSED_STREAM, a 503, the
//!    per-stream shed, identity encoding).
//! 2. A second identity (::1) during the attack: admitted up to a full share,
//!    and refused at the next.
//! 3. The attack again, at once: every attempt sent before any is answered.
//!    Admission is check-then-reserve, so concurrent admissions may pass the
//!    mark by the number in flight; the bound printed and checked says so.
//!
//! Identities come from the kernel: 127.0.0.1 against ::1, and PROXY v2
//! headers for the IPv6 /48 aggregate and the NAT case. No test hook makes
//! an identity. Every test prints the server's refusal counter for its
//! resource (`Table.share_refusals`), so an attack that never engaged shows
//! as a zero. Every server ends with `Server.deinit`, which checks that every
//! client account is back to zero (`Table.checkEmpty`) and logs an error
//! otherwise, which fails the test.
//!
//! The file compiles against 42c65d8, the commit before shares, where every
//! attack test fails: `@hasDecl` keeps the counter reads out there.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");
const serve_deadline = @import("serve_deadline.zig");
const route_share = @import("route_share.zig");

const accounts = starh2.edge.accounts;
const proxy = starh2.edge.proxy_protocol;
const hpack = starh2.core.hpack;
const frame = starh2.core.frame;
const Wire = wire_client.Wire;
const Peer = starh2.Peer;

const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

/// Off a zio task (the clients run on blocking threads).
fn osSleepMs(n: u64) void {
    const t: std.c.timespec = .{ .sec = @intCast(n / 1000), .nsec = @intCast((n % 1000) * ms) };
    _ = std.c.nanosleep(&t, null);
}

// ---------------------------------------------------------------------------
// Routes.

/// Set to let every holding handler return.
var release: std.atomic.Value(bool) = .init(false);
var started: std.atomic.Value(usize) = .init(0);
var ended: std.atomic.Value(usize) = .init(0);
/// `/big` handlers whose write the server refused (the per-stream shed).
var big_refused: std.atomic.Value(usize) = .init(0);
var big_written: std.atomic.Value(usize) = .init(0);

fn resetRoutes() void {
    release.store(false, .release);
    started.store(0, .release);
    ended.store(0, .release);
    big_refused.store(0, .release);
    big_written.store(0, .release);
}

fn holdUntilRelease() void {
    while (!release.load(.acquire)) zio.sleep(.fromMilliseconds(2)) catch return;
}

/// A task handler (stream, task-handler token, a first request block) held
/// until `release`.
fn park(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    holdUntilRelease();
    try resp.send(200, &.{}, "ok");
}

/// Holds its first block and a 128 KiB block (100 KiB asked): 192 KiB of
/// request memory, until `release`. Its allocation refused is a 503.
fn hold100k(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = try req.arena.alloc(u8, 100 * 1024);
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    holdUntilRelease();
    try resp.send(200, &.{}, "held");
}

const big_bytes = 32 * 1024;

/// Writes `big_bytes` to a client that opens no flow-control window, so they
/// stay reserved as outbound bytes, then holds until `release`. A refused
/// write is the per-stream shed.
fn big(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    var body = try resp.start(200, &.{});
    const chunk: [big_bytes]u8 = @splat('b');
    body.writeAll(&chunk) catch |err| {
        _ = big_refused.fetchAdd(1, .acq_rel);
        return err;
    };
    _ = big_written.fetchAdd(1, .acq_rel);
    holdUntilRelease();
    try body.finish();
}

/// A compressible streaming response held open: its brotli context (or its
/// identity encoding, when the share refused one) stays until it finishes.
fn brotliHeld(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    var body = try resp.start(200, &.{.{ .name = "content-type", .value = "text/plain" }});
    try body.writeAll("compressible text, held open until the test lets it finish");
    try body.flush();
    holdUntilRelease();
    try body.finish();
}

/// Bytes `allocComplete` asks of its request's arena.
var complete_alloc: usize = 0;

/// A complete handler that allocates `complete_alloc` from its request.
fn allocComplete(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    const buf = try req.arena.alloc(u8, complete_alloc);
    @memset(buf, 'a');
    try resp.send(200, &.{}, "allocated");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/park", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = park } } },
    .{ .method = .GET, .path = "/hold100k", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hold100k } } },
    .{ .method = .POST, .path = "/hold100k", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hold100k } } },
    .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = big } } },
    .{ .method = .GET, .path = "/brotli", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = brotliHeld } } },
    .{ .method = .GET, .path = "/alloc", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = allocComplete } } },
};

// ---------------------------------------------------------------------------
// The server.

const Tr = enum { h2c, tls, h1c };

const Rig = struct {
    gpa: std.mem.Allocator,
    server: starh2.Server,
    handle: zio.JoinHandle(starh2.ServeError!void),
    cert: []u8 = &.{},
    key: []u8 = &.{},

    fn start(self: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator, endpoints: []const starh2.EndpointConfig, limits: starh2.Limits, tls_on: bool) !void {
        const io = rt.io();
        self.* = .{ .gpa = gpa, .server = undefined, .handle = undefined };
        var tls_cfg: ?starh2.TlsConfig = null;
        if (tls_on) {
            self.cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            self.key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            tls_cfg = .{ .certificate_chain_pem = self.cert, .private_key_pem = self.key };
        }
        errdefer {
            if (self.cert.len != 0) gpa.free(self.cert);
            if (self.key.len != 0) gpa.free(self.key);
        }
        // The client shares are under test, not the route shares
        // (`tests/routes.zig`): every route may hold the whole server.
        const whole = route_share.wholeServer(routes.len, routes, limits);
        self.server = try starh2.Server.init(gpa, io, .{
            .endpoints = endpoints,
            .routes = &whole,
            .tls = tls_cfg,
            .limits = limits,
        });
        self.handle = try rt.spawn(starh2.Server.serve, .{ &self.server, gpa });
        try self.server.waitUntilListening(5 * std.time.ns_per_s);
    }

    /// The pair every row uses: endpoint 0 on 127.0.0.1, endpoint 1 on ::1.
    fn startPair(self: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr, limits: starh2.Limits) !void {
        const a4 = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
        const a6 = try starh2.EndpointAddress.parseIp6("::1", 0);
        const eps: [2]starh2.EndpointConfig = switch (tr) {
            .h2c => .{ .{ .h2c_prior_knowledge = .{ .address = a4 } }, .{ .h2c_prior_knowledge = .{ .address = a6 } } },
            .tls => .{ .{ .tls = .{ .address = a4 } }, .{ .tls = .{ .address = a6 } } },
            .h1c => .{ .{ .h1c = .{ .address = a4 } }, .{ .h1c = .{ .address = a6 } } },
        };
        try self.start(rt, gpa, &eps, limits, tr == .tls);
    }

    fn port(self: *Rig, i: usize) u16 {
        return self.server.localAddress(i).getPort();
    }

    fn stop(self: *Rig) void {
        release.store(true, .release);
        self.server.requestShutdown();
        serve_deadline.join(&self.server, &self.handle);
        self.server.deinit(self.gpa);
        if (self.cert.len != 0) self.gpa.free(self.cert);
        if (self.key.len != 0) self.gpa.free(self.key);
    }

    fn count(self: *Rig, k: accounts.Key, res: accounts.Resource) usize {
        const e = self.server.clients.lookup(k) orelse return 0;
        return e.count(res);
    }

    /// Charges refused by share for `res` (0 on 42c65d8, which has none).
    fn refusals(self: *Rig, res: accounts.Resource) u64 {
        if (comptime !@hasDecl(accounts.Table, "shareRefusals")) return 0;
        return self.server.clients.shareRefusals(res);
    }
};

fn v4Key(a: [4]u8) accounts.Key {
    var buf: [accounts.max_refs]accounts.Key = undefined;
    return accounts.keysFor(.{ .ip4 = a }, 48, &buf)[0];
}

fn v6Key(a: [16]u8) accounts.Key {
    var buf: [accounts.max_refs]accounts.Key = undefined;
    return accounts.keysFor(.{ .ip6 = a }, 48, &buf)[0];
}

fn v6AggKey(a: [16]u8) accounts.Key {
    var buf: [accounts.max_refs]accounts.Key = undefined;
    return accounts.keysFor(.{ .ip6 = a }, 48, &buf)[1];
}

fn ip6(text: []const u8) [16]u8 {
    const a = std.Io.net.IpAddress.parseIp6(text, 0) catch unreachable;
    return a.ip6.bytes;
}

const attacker = v4Key(.{ 127, 0, 0, 1 });
const defender = v6Key(ip6("::1"));

/// Set a `Limits` field this commit added, so the file still compiles
/// against 42c65d8, where the field does not exist (and the test fails).
fn setLimit(l: *starh2.Limits, comptime name: []const u8, v: anytype) void {
    if (comptime @hasField(starh2.Limits, name)) @field(l, name) = v;
}

fn testLimits() starh2.Limits {
    var l = starh2.Limits.defaults;
    l.graceful_drain_timeout_ns = 300 * ms;
    return l;
}

fn runIn(comptime f: anytype) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(4) });
    defer rt.deinit();
    var h = try rt.spawn(f, .{ rt, gpa });
    try h.join();
}

/// Run `f(args)` on a blocking thread (the clients block) and wait for it.
fn blocking(comptime f: anytype, args: anytype) !void {
    var h = try zio.spawnBlocking(f, args);
    return h.join();
}

/// Poll from the test's task until `done` holds, or fail naming `what`.
fn waitFor(comptime what: []const u8, ctx: anytype, comptime done: fn (@TypeOf(ctx)) bool, timeout_ns: u64) !void {
    const deadline = nowNs() + timeout_ns;
    while (!done(ctx)) {
        if (nowNs() >= deadline) {
            std.debug.print("shares: timed out waiting for {s}\n", .{what});
            return error.StateNotReached;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

fn tableQuiet(rig: *Rig) bool {
    return rig.server.clients.stats().live == 0;
}

// ---------------------------------------------------------------------------
// Clients, on blocking threads. HTTP/2 over `Wire` (h2c and TLS, from
// 127.0.0.1 or ::1); HTTP/1.1 over a raw socket.

/// One HTTP/2 connection and what it saw, stream by stream (ids 1, 3, 5...).
const H2 = struct {
    w: *Wire,
    dec: hpack.Decoder,
    next_sid: u31 = 1,
    status: [max_streams]?u16 = @splat(null),
    rst: [max_streams]?u32 = @splat(null),
    brotli: [max_streams]bool = @splat(false),
    closed: bool = false,
    settings: bool = false,

    const max_streams = 64;

    /// Connect, and for TLS shake hands. Null when the server refused the
    /// connection at accept (it closes it unread). `window`: the initial
    /// stream window the client advertises (0 holds every response body in
    /// the server).
    fn open(gpa: std.mem.Allocator, tr: Tr, port: u16, v6: bool, window: u32) !?H2 {
        const transport: wire_client.Transport = if (tr == .tls) .tls else .h2c;
        const w = (if (v6) Wire.openIp6(gpa, transport, port) else Wire.open(gpa, transport, port)) catch |err| switch (err) {
            error.Socket, error.Connect, error.OutOfMemory => return err,
            // A TLS handshake the server closed.
            else => return null,
        };
        var self: H2 = .{ .w = w, .dec = try hpack.Decoder.init(gpa) };
        var pre = try h2c.buildClientPrefaceAndSettings(gpa);
        defer pre.deinit(gpa);
        var sbuf: [64]u8 = undefined;
        const s = [_]frame.Setting{.{ .id = .initial_window_size, .value = window }};
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &s);
        try pre.appendSlice(gpa, sbuf[0..sn]);
        // Room on the connection window, so only the stream windows hold.
        try h2c.appendWindowUpdate(gpa, &pre, 0, 1 << 30);
        self.w.sendUnchecked(pre.items) catch {
            self.closed = true;
            return self;
        };
        return self;
    }

    fn deinit(self: *H2) void {
        self.dec.deinit();
        self.w.close();
    }

    fn get(self: *H2, gpa: std.mem.Allocator, path: []const u8, extra: []const hpack.HeaderField) !u31 {
        const sid = self.next_sid;
        self.next_sid += 2;
        var req: std.ArrayList(u8) = .empty;
        defer req.deinit(gpa);
        try h2c.appendHeadersExtra(gpa, &req, sid, "GET", path, true, extra);
        self.w.sendUnchecked(req.items) catch {
            self.closed = true;
        };
        return sid;
    }

    /// Read what arrives for `for_ms`, or until the server closes.
    fn poll(self: *H2, for_ms: u64) !void {
        const deadline = nowNs() + for_ms * ms;
        while (!self.closed) {
            const seen = self.w.next(deadline) catch |err| switch (err) {
                error.Deadline => return,
                else => {
                    self.closed = true;
                    return;
                },
            } orelse {
                self.closed = true;
                return;
            };
            const i: usize = if (seen.sid == 0 or seen.sid % 2 == 0) max_streams else (seen.sid - 1) / 2;
            const payload = self.w.last_payload.items;
            switch (seen.ftype) {
                0x1 => {
                    const decoded = try self.dec.decode(payload, 100, 32 * 1024, 256, 8 * 1024);
                    defer self.dec.freeResult(decoded);
                    if (i < max_streams) {
                        for (decoded.fields) |f| {
                            if (std.mem.eql(u8, f.name, ":status")) self.status[i] = std.fmt.parseInt(u16, f.value, 10) catch null;
                            if (std.mem.eql(u8, f.name, "content-encoding") and std.mem.eql(u8, f.value, "br")) self.brotli[i] = true;
                        }
                    }
                },
                0x3 => if (i < max_streams and payload.len >= 4) {
                    self.rst[i] = std.mem.readInt(u32, payload[0..4], .big);
                },
                0x4 => self.settings = true,
                0x7 => {
                    self.closed = true;
                    return;
                },
                else => {},
            }
        }
    }

    fn slot(sid: u31) usize {
        return (sid - 1) / 2;
    }
};

/// A raw blocking socket to 127.0.0.1 or ::1.
fn rawConnect(v6: bool, port: u16) !std.c.fd_t {
    const fd = std.c.socket(if (v6) std.c.AF.INET6 else std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.Socket;
    errdefer _ = std.c.close(fd);
    if (v6) {
        var addr: std.c.sockaddr.in6 = .{ .port = std.mem.nativeToBig(u16, port), .flowinfo = 0, .addr = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }, .scope_id = 0 };
        if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in6)) != 0) return error.Connect;
    } else {
        var addr: std.c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = std.mem.nativeToBig(u32, 0x7f000001) };
        if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
    }
    return fd;
}

fn rawSend(fd: std.c.fd_t, bytes: []const u8) bool {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = std.c.send(fd, bytes[off..].ptr, bytes.len - off, 0);
        if (n <= 0) return false;
        off += @intCast(n);
    }
    return true;
}

/// What a raw socket saw within `for_ms`: nothing yet, bytes (the first ones
/// kept), or the server's close with nothing before it.
const Raw = struct {
    got: [256]u8 = undefined,
    len: usize = 0,
    closed: bool = false,

    fn text(self: *const Raw) []const u8 {
        return self.got[0..self.len];
    }

    fn statusIs(self: *const Raw, code: []const u8) bool {
        return self.len >= 12 and std.mem.startsWith(u8, self.got[0..self.len], "HTTP/1.1 ") and std.mem.eql(u8, self.got[9..12], code);
    }
};

fn rawRead(fd: std.c.fd_t, for_ms: u64, out: *Raw) void {
    const deadline = nowNs() + for_ms * ms;
    while (!out.closed) {
        const left = deadline -| nowNs();
        if (left == 0) return;
        var pfd = [_]std.c.pollfd{.{ .fd = fd, .events = std.c.POLL.IN, .revents = 0 }};
        const r = std.c.poll(&pfd, 1, @intCast(@max(1, left / ms)));
        if (r <= 0) continue;
        var buf: [4096]u8 = undefined;
        const n = std.c.recv(fd, &buf, buf.len, 0);
        if (n <= 0) {
            out.closed = true;
            return;
        }
        const take = @min(@as(usize, @intCast(n)), out.got.len - out.len);
        @memcpy(out.got[out.len..][0..take], buf[0..take]);
        out.len += take;
        // Enough of a status line to judge.
        if (out.len >= 12) return;
    }
}

fn setLinger0(fd: std.c.fd_t) void {
    const lg: std.c.linger = .{ .onoff = 1, .linger = 0 };
    _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.LINGER, std.mem.asBytes(&lg), @sizeOf(std.c.linger));
}

// ---------------------------------------------------------------------------
// The holders of one attack: every connection a row opened, kept open until
// the row lets go of them.

const max_conns = 96;

const Conns = struct {
    gpa: std.mem.Allocator,
    h2: [max_conns]?H2 = @splat(null),
    raw: [max_conns]std.c.fd_t = @splat(-1),
    n: usize = 0,

    fn addH2(self: *Conns, c: H2) *H2 {
        self.h2[self.n] = c;
        self.n += 1;
        return &self.h2[self.n - 1].?;
    }

    fn addRaw(self: *Conns, fd: std.c.fd_t) void {
        self.raw[self.n] = fd;
        self.n += 1;
    }

    fn closeAll(self: *Conns) void {
        for (self.h2[0..self.n]) |*c| {
            if (c.*) |*h| h.deinit();
            c.* = null;
        }
        for (self.raw[0..self.n]) |*fd| {
            if (fd.* >= 0) _ = std.c.close(fd.*);
            fd.* = -1;
        }
        self.n = 0;
    }
};

/// Let every holder go and wait until the table holds nothing.
fn letGo(rig: *Rig, conns: []const *Conns) !void {
    release.store(true, .release);
    for (conns) |c| try blocking(Conns.closeAll, .{c});
    try waitFor("the table to empty", rig, tableQuiet, 10 * std.time.ns_per_s);
    release.store(false, .release);
    started.store(0, .release);
    ended.store(0, .release);
}

fn expectAtMost(comptime what: []const u8, got: usize, bound: usize) !void {
    if (got <= bound) return;
    std.debug.print("shares: {s} is {d}, the bound is {d}\n", .{ what, got, bound });
    return error.OverBound;
}

fn expectExactly(comptime what: []const u8, got: usize, want: usize) !void {
    if (got == want) return;
    std.debug.print("shares: {s} is {d}, want {d}\n", .{ what, got, want });
    return error.WrongCount;
}

// ---------------------------------------------------------------------------
// Connections: close at accept.

/// One connection attempt that, admitted, holds a `/park` handler. True when
/// the server admitted it (h2c: its SETTINGS arrived; TLS: the handshake
/// completed; HTTP/1.1: no close within the read window).
fn connAttempt(conns: *Conns, tr: Tr, port: u16, v6: bool) !bool {
    switch (tr) {
        .h2c, .tls => {
            const c = try H2.open(conns.gpa, tr, port, v6, 1 << 20) orelse return false;
            const h = conns.addH2(c);
            _ = try h.get(conns.gpa, "/park", &.{});
            try h.poll(300);
            return !h.closed and (tr == .tls or h.settings);
        },
        .h1c => {
            const fd = try rawConnect(v6, port);
            conns.addRaw(fd);
            _ = rawSend(fd, "GET /park HTTP/1.1\r\nHost: localhost\r\n\r\n");
            var r: Raw = .{};
            rawRead(fd, 300, &r);
            return !r.closed and r.len == 0;
        },
    }
}

const Tally = struct {
    admitted: usize = 0,
    refused: usize = 0,
};

fn connSerial(conns: *Conns, tr: Tr, port: u16, v6: bool, n: usize, out: *Tally) void {
    for (0..n) |_| {
        const ok = connAttempt(conns, tr, port, v6) catch false;
        if (ok) out.admitted += 1 else out.refused += 1;
    }
}

/// Every connection opened and its request sent before any is read.
fn connAtOnce(conns: *Conns, tr: Tr, port: u16, v6: bool, n: usize) void {
    for (0..n) |_| {
        switch (tr) {
            .h2c, .tls => {
                // TLS can only shake hands one at a time from here; the
                // server's accept loop admits them one at a time anyway.
                const c = (H2.open(conns.gpa, tr, port, v6, 1 << 20) catch null) orelse continue;
                const h = conns.addH2(c);
                _ = h.get(conns.gpa, "/park", &.{}) catch {};
            },
            .h1c => {
                const fd = rawConnect(v6, port) catch continue;
                conns.addRaw(fd);
                _ = rawSend(fd, "GET /park HTTP/1.1\r\nHost: localhost\r\n\r\n");
            },
        }
    }
}

fn connectionsRow(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    resetRoutes();
    var limits = testLimits();
    // Share 2, mark (and cap) 4.
    limits.max_connections = 16;
    const share = 2;
    const cap = 4;
    var rig: Rig = undefined;
    try rig.startPair(rt, gpa, tr, limits);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    var d: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
        d.closeAll();
    }
    const r0 = rig.refusals(.connections);

    // 1. Serially: ten, one at a time, each kept.
    var serial: Tally = .{};
    try blocking(connSerial, .{ &a, tr, rig.port(0), false, 10, &serial });
    const held_serial = rig.count(attacker, .connections);
    // 2. The second identity, during the attack: a full share, then refused.
    var second: Tally = .{};
    try blocking(connSerial, .{ &d, tr, rig.port(1), true, share + 1, &second });
    const held_second = rig.count(defender, .connections);
    std.debug.print("shares connections {s} serial: 127.0.0.1 admitted {d} refused {d} (holds {d}, cap {d}); ::1 admitted {d} refused {d} (holds {d}, share {d}); refusals {d}\n", .{ @tagName(tr), serial.admitted, serial.refused, held_serial, cap, second.admitted, second.refused, held_second, share, rig.refusals(.connections) - r0 });
    try expectExactly("127.0.0.1 connections, serially", held_serial, cap);
    try expectExactly("127.0.0.1 admitted, serially", serial.admitted, cap);
    try expectExactly("::1 connections during the attack", held_second, share);
    try expectExactly("::1 admitted during the attack", second.admitted, share);
    // One refusal per refused attempt, counted once.
    try expectExactly("connection refusals", rig.refusals(.connections) - r0, serial.refused + second.refused);
    try letGo(&rig, &.{ &a, &d });

    // 3. At once: twelve sent before any is read.
    const r1 = rig.refusals(.connections);
    try blocking(connAtOnce, .{ &a, tr, rig.port(0), false, 12 });
    const Settled = struct {
        rig: *Rig,
        r1: u64,
        fn f(s: @This()) bool {
            return s.rig.count(attacker, .connections) + (s.rig.refusals(.connections) - s.r1) >= 12 or
                (s.rig.count(attacker, .connections) >= 12);
        }
    };
    // On 42c65d8 nothing is refused and all twelve are held.
    waitFor("every attempt admitted or refused", Settled{ .rig = &rig, .r1 = r1 }, Settled.f, 5 * std.time.ns_per_s) catch {};
    const held_once = rig.count(attacker, .connections);
    var second2: Tally = .{};
    try blocking(connSerial, .{ &d, tr, rig.port(1), true, share + 1, &second2 });
    std.debug.print("shares connections {s} at once: 127.0.0.1 holds {d} of 12 (cap {d}, one accept loop admits one at a time); ::1 admitted {d}; refusals {d}\n", .{ @tagName(tr), held_once, cap, second2.admitted, rig.refusals(.connections) - r1 });
    try expectExactly("127.0.0.1 connections, at once", held_once, cap);
    try expectExactly("::1 admitted during the attack, at once", second2.admitted, share);
    try expectExactly("connection refusals, at once", rig.refusals(.connections) - r1, (12 - cap) + (share + 1 - second2.admitted));
    try letGo(&rig, &.{ &a, &d });
}

fn connectionsH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return connectionsRow(rt, gpa, .h2c);
}
fn connectionsTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return connectionsRow(rt, gpa, .tls);
}
fn connectionsH1c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return connectionsRow(rt, gpa, .h1c);
}

test "share connections (h2c): one identity holds its cap; a second gets a full share; the rest close at accept" {
    try runIn(connectionsH2c);
}
test "share connections (TLS): one identity holds its cap; a second gets a full share; the rest close at accept" {
    try runIn(connectionsTls);
}
test "share connections (HTTP/1.1): one identity holds its cap; a second gets a full share; the rest close at accept" {
    try runIn(connectionsH1c);
}

// ---------------------------------------------------------------------------
// TLS handshakes in progress: close at accept.

/// A TCP connection to a TLS endpoint that sends nothing: admitted, it holds
/// a handshake slot until the handshake timeout. True when still open after
/// the read window (refused ones are closed at once).
fn hsAttempt(conns: *Conns, port: u16, v6: bool) !bool {
    const fd = try rawConnect(v6, port);
    conns.addRaw(fd);
    var r: Raw = .{};
    rawRead(fd, 250, &r);
    return !r.closed;
}

fn hsSerial(conns: *Conns, port: u16, v6: bool, n: usize, out: *Tally) void {
    for (0..n) |_| {
        const ok = hsAttempt(conns, port, v6) catch false;
        if (ok) out.admitted += 1 else out.refused += 1;
    }
}

fn hsAtOnce(conns: *Conns, port: u16, v6: bool, n: usize) void {
    for (0..n) |_| {
        const fd = rawConnect(v6, port) catch continue;
        conns.addRaw(fd);
    }
}

fn handshakesTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var limits = testLimits();
    // Share 2, mark (and cap) 4. Long enough that no handshake times out
    // during the test.
    limits.concurrent_tls_handshakes = 16;
    limits.preface_timeout_ns = 20 * std.time.ns_per_s;
    const share = 2;
    const cap = 4;
    var rig: Rig = undefined;
    try rig.startPair(rt, gpa, .tls, limits);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    var d: Conns = .{ .gpa = gpa };
    defer {
        a.closeAll();
        d.closeAll();
    }
    const r0 = rig.refusals(.handshakes);

    var serial: Tally = .{};
    try blocking(hsSerial, .{ &a, rig.port(0), false, 10, &serial });
    const held_serial = rig.count(attacker, .handshakes);
    var second: Tally = .{};
    try blocking(hsSerial, .{ &d, rig.port(1), true, share + 1, &second });
    const held_second = rig.count(defender, .handshakes);
    std.debug.print("shares handshakes TLS serial: 127.0.0.1 admitted {d} refused {d} (holds {d}, cap {d}); ::1 admitted {d} refused {d} (holds {d}, share {d}); server {d}; refusals {d}\n", .{ serial.admitted, serial.refused, held_serial, cap, second.admitted, second.refused, held_second, share, rig.server.accounting.active_handshakes.load(.acquire), rig.refusals(.handshakes) - r0 });
    try expectExactly("127.0.0.1 handshakes, serially", held_serial, cap);
    try expectExactly("127.0.0.1 still open, serially", serial.admitted, cap);
    try expectExactly("::1 handshakes during the attack", held_second, share);
    try expectExactly("handshake refusals", rig.refusals(.handshakes) - r0, serial.refused + second.refused);
    try letGo(&rig, &.{ &a, &d });

    const r1 = rig.refusals(.handshakes);
    try blocking(hsAtOnce, .{ &a, rig.port(0), false, 12 });
    const Settled = struct {
        rig: *Rig,
        r1: u64,
        fn f(s: @This()) bool {
            return s.rig.count(attacker, .handshakes) + (s.rig.refusals(.handshakes) - s.r1) >= 12 or
                s.rig.count(attacker, .handshakes) >= 12;
        }
    };
    waitFor("every attempt admitted or refused", Settled{ .rig = &rig, .r1 = r1 }, Settled.f, 5 * std.time.ns_per_s) catch {};
    const held_once = rig.count(attacker, .handshakes);
    var second2: Tally = .{};
    try blocking(hsSerial, .{ &d, rig.port(1), true, share + 1, &second2 });
    std.debug.print("shares handshakes TLS at once: 127.0.0.1 holds {d} of 12 (cap {d}); ::1 admitted {d}; refusals {d}\n", .{ held_once, cap, second2.admitted, rig.refusals(.handshakes) - r1 });
    try expectExactly("127.0.0.1 handshakes, at once", held_once, cap);
    try expectExactly("::1 handshakes during the attack, at once", rig.count(defender, .handshakes), share);
    try letGo(&rig, &.{ &a, &d });
}

test "share TLS handshakes in progress: one identity holds its cap of open handshakes; a second gets a full share" {
    try runIn(handshakesTls);
}

// ---------------------------------------------------------------------------
// Streams and task handlers: RST_STREAM REFUSED_STREAM on HTTP/2, 503 on
// HTTP/1.1.

const refused_stream: u32 = 0x7;

/// `n` `/park` requests over `per_conn`-stream connections. Serially: one
/// request at a time, each read before the next. Returns how many were
/// refused in the row's way.
const StreamPlan = struct {
    tr: Tr,
    port: u16,
    v6: bool,
    n: usize,
    per_conn: usize,
    serial: bool,
    refused: usize = 0,
    other: usize = 0,
};

fn streamsRun(conns: *Conns, plan: *StreamPlan) void {
    streamsRunInner(conns, plan) catch |err| {
        std.debug.print("shares: stream client failed: {s}\n", .{@errorName(err)});
        plan.other += 1;
    };
}

fn streamsRunInner(conns: *Conns, plan: *StreamPlan) !void {
    switch (plan.tr) {
        .h2c, .tls => {
            const first = conns.n;
            var sent: usize = 0;
            while (sent < plan.n) {
                const c = try H2.open(conns.gpa, plan.tr, plan.port, plan.v6, 1 << 20) orelse return error.ConnectionRefused;
                const h = conns.addH2(c);
                var k: usize = 0;
                while (k < plan.per_conn and sent < plan.n) : (k += 1) {
                    _ = try h.get(conns.gpa, "/park", &.{});
                    sent += 1;
                    if (plan.serial) try h.poll(60);
                }
            }
            for (conns.h2[first..conns.n]) |*c| try c.*.?.poll(300);
            for (conns.h2[first..conns.n]) |*c| {
                const h = &c.*.?;
                for (0..H2.slot(h.next_sid)) |i| {
                    if (h.rst[i]) |code| {
                        if (code == refused_stream) plan.refused += 1 else plan.other += 1;
                    } else if (h.status[i] != null) plan.other += 1;
                }
            }
        },
        .h1c => {
            for (0..plan.n) |_| {
                const fd = try rawConnect(plan.v6, plan.port);
                conns.addRaw(fd);
                _ = rawSend(fd, "GET /park HTTP/1.1\r\nHost: localhost\r\n\r\n");
                if (!plan.serial) continue;
                var r: Raw = .{};
                rawRead(fd, 200, &r);
                if (r.statusIs("503")) plan.refused += 1 else if (r.len != 0 or r.closed) plan.other += 1;
            }
            if (plan.serial) return;
            for (conns.raw[0..conns.n]) |fd| {
                if (fd < 0) continue;
                var r: Raw = .{};
                rawRead(fd, 200, &r);
                if (r.statusIs("503")) plan.refused += 1 else if (r.len != 0 or r.closed) plan.other += 1;
            }
        },
    }
}

fn countRow(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr, res: accounts.Resource) !void {
    resetRoutes();
    var limits = testLimits();
    // Share 8, mark (and cap) 16, of the resource under test; the other one
    // is far from its mark.
    switch (res) {
        .streams => limits.max_streams_per_server = 64,
        .task_handlers => limits.max_task_handlers = 64,
        else => unreachable,
    }
    const share = 8;
    const cap = 16;
    var rig: Rig = undefined;
    try rig.startPair(rt, gpa, tr, limits);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    var d: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
        d.closeAll();
    }
    const per_conn: usize = if (tr == .h1c) 1 else 8;

    // 1. Serially: thirty requests over four connections, one at a time.
    const r0 = rig.refusals(res);
    var serial: StreamPlan = .{ .tr = tr, .port = rig.port(0), .v6 = false, .n = 30, .per_conn = per_conn, .serial = true };
    try blocking(streamsRun, .{ &a, &serial });
    const held_serial = rig.count(attacker, res);
    // 2. The second identity during the attack: a full share, then refused.
    var second: StreamPlan = .{ .tr = tr, .port = rig.port(1), .v6 = true, .n = share + 2, .per_conn = if (tr == .h1c) 1 else share + 2, .serial = true };
    try blocking(streamsRun, .{ &d, &second });
    const held_second = rig.count(defender, res);
    std.debug.print("shares {s} {s} serial: 127.0.0.1 holds {d} of 30 (cap {d}), {d} refused; ::1 holds {d} of {d} (share {d}), {d} refused; other answers {d}; refusals {d}\n", .{ @tagName(res), @tagName(tr), held_serial, cap, serial.refused, held_second, share + 2, share, second.refused, serial.other + second.other, rig.refusals(res) - r0 });
    try expectExactly("127.0.0.1, serially", held_serial, cap);
    try expectExactly("127.0.0.1 refused, serially", serial.refused, 30 - cap);
    try expectExactly("::1 during the attack", held_second, share);
    try expectExactly("::1 refused during the attack", second.refused, 2);
    try expectExactly("answers other than a hold or the refusal", serial.other + second.other, 0);
    try expectExactly("refusals", rig.refusals(res) - r0, serial.refused + second.refused);
    try letGo(&rig, &.{ &a, &d });

    // 3. At once: thirty requests over four connections, sent before any is
    // read. The connections' actors admit concurrently, so the mark can be
    // passed by the admissions in flight: at most one per executor.
    const r1 = rig.refusals(res);
    var once: StreamPlan = .{ .tr = tr, .port = rig.port(0), .v6 = false, .n = 30, .per_conn = per_conn, .serial = false };
    try blocking(streamsRun, .{ &a, &once });
    const held_once = rig.count(attacker, res);
    const in_flight: usize = executors;
    std.debug.print("shares {s} {s} at once: 127.0.0.1 holds {d} of 30 (cap {d}, plus at most {d} in flight), {d} refused; refusals {d}\n", .{ @tagName(res), @tagName(tr), held_once, cap, in_flight, once.refused, rig.refusals(res) - r1 });
    try expectAtMost("127.0.0.1, at once", held_once, cap + in_flight);
    try expectExactly("127.0.0.1 held or refused, at once", held_once + once.refused, 30);
    try expectExactly("refusals, at once", rig.refusals(res) - r1, once.refused);
    try letGo(&rig, &.{ &a, &d });
}

fn streamsH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return countRow(rt, gpa, .h2c, .streams);
}
fn streamsTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return countRow(rt, gpa, .tls, .streams);
}
fn streamsH1c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return countRow(rt, gpa, .h1c, .streams);
}
fn tasksH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return countRow(rt, gpa, .h2c, .task_handlers);
}
fn tasksTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return countRow(rt, gpa, .tls, .task_handlers);
}
fn tasksH1c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return countRow(rt, gpa, .h1c, .task_handlers);
}

test "share streams (h2c): REFUSED_STREAM past the cap; a second identity gets a full share" {
    try runIn(streamsH2c);
}
test "share streams (TLS): REFUSED_STREAM past the cap; a second identity gets a full share" {
    try runIn(streamsTls);
}
test "share streams (HTTP/1.1): 503 past the cap; a second identity gets a full share" {
    try runIn(streamsH1c);
}
test "share task handlers (h2c): REFUSED_STREAM past the cap; a second identity gets a full share" {
    try runIn(tasksH2c);
}
test "share task handlers (TLS): REFUSED_STREAM past the cap; a second identity gets a full share" {
    try runIn(tasksTls);
}
test "share task handlers (HTTP/1.1): 503 past the cap; a second identity gets a full share" {
    try runIn(tasksH1c);
}

// ---------------------------------------------------------------------------
// Request pool bytes: 503.

const kib = 1024;
/// What one `/hold100k` holds: its first block and a 128 KiB block.
const holder_bytes = 192 * kib;

fn memLimits() starh2.Limits {
    var l = testLimits();
    // Share 256 KiB, mark (and cap) 512 KiB; nested, so a connection and a
    // request hold at most a share.
    l.request_bytes_per_server = 2 * 1024 * kib;
    l.request_bytes_per_connection = 256 * kib;
    l.request_arena_bytes = 256 * kib;
    // The largest body that arena can buffer (`Limits.bodyArenaBytes`).
    l.request_body_bytes = 64 * kib - 64;
    l.request_pool_retain_bytes = l.request_bytes_per_server;
    return l;
}

/// One `/hold100k` on a connection of its own. Its status: null while held
/// (no answer yet), 503 when refused.
fn memAttempt(conns: *Conns, tr: Tr, port: u16, v6: bool) !?u16 {
    switch (tr) {
        .h2c, .tls => {
            const c = try H2.open(conns.gpa, tr, port, v6, 1 << 20) orelse return error.ConnectionRefused;
            const h = conns.addH2(c);
            const sid = try h.get(conns.gpa, "/hold100k", &.{});
            try h.poll(250);
            return h.status[H2.slot(sid)];
        },
        .h1c => {
            const fd = try rawConnect(v6, port);
            conns.addRaw(fd);
            _ = rawSend(fd, "GET /hold100k HTTP/1.1\r\nHost: localhost\r\n\r\n");
            var r: Raw = .{};
            rawRead(fd, 250, &r);
            if (r.len == 0) return null;
            return std.fmt.parseInt(u16, r.got[9..12], 10) catch 0;
        },
    }
}

const MemTally = struct {
    held: usize = 0,
    refused: usize = 0,
    other: usize = 0,
};

fn memSerial(conns: *Conns, tr: Tr, port: u16, v6: bool, n: usize, out: *MemTally) void {
    for (0..n) |_| {
        const st = memAttempt(conns, tr, port, v6) catch {
            out.other += 1;
            continue;
        };
        if (st == null) out.held += 1 else if (st.? == 503) out.refused += 1 else out.other += 1;
    }
}

/// Check-then-reserve: an admission reads the server's use, then charges, and
/// a handler does not yield in between, so at most one charge per executor
/// (`runIn` runs 4) can pass the mark at once.
const executors = 4;

/// `n` `/hold100k` requests on `n` connections, every one sent before any
/// is read.
fn memAtOnce(conns: *Conns, tr: Tr, port: u16, n: usize) void {
    for (0..n) |_| {
        switch (tr) {
            .h2c, .tls => {
                const c = (H2.open(conns.gpa, tr, port, false, 1 << 20) catch null) orelse continue;
                const h = conns.addH2(c);
                _ = h.get(conns.gpa, "/hold100k", &.{}) catch {};
            },
            .h1c => {
                const fd = rawConnect(false, port) catch continue;
                conns.addRaw(fd);
                _ = rawSend(fd, "GET /hold100k HTTP/1.1\r\nHost: localhost\r\n\r\n");
            },
        }
    }
}

const Settle = struct {
    rig: *Rig,
    res: accounts.Resource,
    r0: u64,
    n: usize,
    fn f(x: @This()) bool {
        return started.load(.acquire) + (x.rig.refusals(x.res) - x.r0) >= x.n;
    }
};

fn requestBytesRow(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    resetRoutes();
    const limits = memLimits();
    const mark = 512 * kib;
    var rig: Rig = undefined;
    try rig.startPair(rt, gpa, tr, limits);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    var d: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
        d.closeAll();
    }
    const r0 = rig.refusals(.request_bytes);

    // 1. Serially: eight holders, one at a time. Three are admitted below
    // the mark (192, 384, then 448 KiB before the last block: 576 KiB);
    // from then on the server is at the mark and the client over its share.
    var serial: MemTally = .{};
    try blocking(memSerial, .{ &a, tr, rig.port(0), false, 8, &serial });
    const held_serial = rig.count(attacker, .request_bytes);
    // 2. ::1 during the attack: one holder (192 KiB) fits its share
    // (256 KiB); a second takes its first block (the share, exactly) and is
    // refused the next one.
    var second: MemTally = .{};
    try blocking(memSerial, .{ &d, tr, rig.port(1), true, 2, &second });
    // The refused request's first block goes back after its 503 is written
    // (on HTTP/1.1, as its connection closes): let it.
    const Back = struct {
        fn f(r: *Rig) bool {
            return r.count(defender, .request_bytes) <= holder_bytes;
        }
    };
    waitFor("the refused request's block to come back", &rig, Back.f, 2 * std.time.ns_per_s) catch {};
    const held_second = rig.count(defender, .request_bytes);
    std.debug.print("shares request bytes {s} serial: 127.0.0.1 holds {d} KiB in {d} holders ({d} refused, mark {d} KiB, one charge at most 128 KiB past it); ::1 holds {d} KiB ({d} held, {d} refused, share 256 KiB); other answers {d}; refusals {d}\n", .{ @tagName(tr), held_serial / kib, serial.held, serial.refused, mark / kib, held_second / kib, second.held, second.refused, serial.other + second.other, rig.refusals(.request_bytes) - r0 });
    try expectExactly("127.0.0.1 holders, serially", serial.held, 3);
    try expectExactly("127.0.0.1 refused, serially", serial.refused, 5);
    try expectAtMost("127.0.0.1 request bytes, serially", held_serial, mark + 128 * kib);
    try expectExactly("::1 holders during the attack", second.held, 1);
    try expectExactly("::1 refused during the attack", second.refused, 1);
    try expectExactly("::1 request bytes during the attack", held_second, holder_bytes);
    try expectExactly("answers other than a hold or a 503", serial.other + second.other, 0);
    try expectExactly("request byte refusals", rig.refusals(.request_bytes) - r0, serial.refused + second.refused);
    try letGo(&rig, &.{ &a, &d });

    // 3. At once: twelve holders on twelve connections, sent before any is
    // read: 2.3 MiB asked of a 2 MiB pool.
    const r1 = rig.refusals(.request_bytes);
    try blocking(memAtOnce, .{ &a, tr, rig.port(0), 12 });
    // On 42c65d8 nothing is refused by share, so this runs to its timeout.
    waitFor("every holder held or refused", Settle{ .rig = &rig, .res = .request_bytes, .r0 = r1, .n = 12 }, Settle.f, 5 * std.time.ns_per_s) catch {};
    const held_once = rig.count(attacker, .request_bytes);
    std.debug.print("shares request bytes {s} at once: 127.0.0.1 holds {d} KiB from 12 holders sent at once (mark {d} KiB, plus at most {d} charges of 128 KiB in flight); refusals {d}\n", .{ @tagName(tr), held_once / kib, mark / kib, executors, rig.refusals(.request_bytes) - r1 });
    try expectAtMost("127.0.0.1 request bytes, at once", held_once, mark + executors * 128 * kib);
    try letGo(&rig, &.{ &a, &d });
}

fn requestBytesH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return requestBytesRow(rt, gpa, .h2c);
}
fn requestBytesTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return requestBytesRow(rt, gpa, .tls);
}
fn requestBytesH1c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return requestBytesRow(rt, gpa, .h1c);
}

test "share request pool bytes (h2c): 503 past the cap; a second identity gets a full share" {
    try runIn(requestBytesH2c);
}
test "share request pool bytes (TLS): 503 past the cap; a second identity gets a full share" {
    try runIn(requestBytesTls);
}
test "share request pool bytes (HTTP/1.1): 503 past the cap; a second identity gets a full share" {
    try runIn(requestBytesH1c);
}

/// HTTP/1.1, the two places a request's memory can be refused after its
/// first block: the copy of the request at dispatch (a body), and a complete
/// handler's own allocation. Both answer 503 and close, as for any refusal
/// that is not the request's own cap (500). ::1 holders put the pool at its
/// mark; 127.0.0.1 holds 192 KiB of its 256 KiB share, so its next request's
/// first block fits and the 128 KiB after it does not.
fn h1ShareRefusal(rt: *zio.Runtime, gpa: std.mem.Allocator, complete: bool) !void {
    resetRoutes();
    var rig: Rig = undefined;
    try rig.startPair(rt, gpa, .h1c, memLimits());
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    var d: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
        d.closeAll();
    }
    var pressure: MemTally = .{};
    try blocking(memSerial, .{ &d, .h1c, rig.port(1), true, 3, &pressure });
    var mine: MemTally = .{};
    try blocking(memSerial, .{ &a, .h1c, rig.port(0), false, 1, &mine });
    try expectExactly("::1 holders", pressure.held, 3);
    try expectExactly("127.0.0.1 holder", mine.held, 1);
    const r0 = rig.refusals(.request_bytes);
    complete_alloc = 100 * kib;
    const Probe = struct {
        fn run(port: u16, is_complete: bool, out: *Raw) void {
            const fd = rawConnect(false, port) catch return;
            defer _ = std.c.close(fd);
            if (is_complete) {
                _ = rawSend(fd, "GET /alloc HTTP/1.1\r\nHost: localhost\r\n\r\n");
            } else {
                // With the request's fields, more than its first block
                // holds: the copy needs a second block.
                const body_len = 65_000;
                var req: [256 + body_len]u8 = undefined;
                const head = std.fmt.bufPrint(&req, "POST /hold100k HTTP/1.1\r\nHost: localhost\r\ncontent-length: {d}\r\n\r\n", .{body_len}) catch return;
                @memset(req[head.len..][0..body_len], 'b');
                _ = rawSend(fd, req[0 .. head.len + body_len]);
            }
            rawRead(fd, 2000, out);
            // The rest of the head, for the connection: close check.
            var more: Raw = .{};
            rawRead(fd, 200, &more);
            const take = @min(more.len, out.got.len - out.len);
            @memcpy(out.got[out.len..][0..take], more.got[0..take]);
            out.len += take;
        }
    };
    var got: Raw = .{};
    try blocking(Probe.run, .{ rig.port(0), complete, &got });
    std.debug.print("shares h1 {s} refusal: {s}; refusals {d}\n", .{ if (complete) "complete-handler" else "request-copy", got.text(), rig.refusals(.request_bytes) - r0 });
    try std.testing.expect(got.statusIs("503"));
    try std.testing.expect(std.mem.indexOf(u8, got.text(), "connection: close") != null);
    try std.testing.expect(rig.refusals(.request_bytes) - r0 >= 1);
    try letGo(&rig, &.{ &a, &d });
}

fn h1CopyRefusal(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return h1ShareRefusal(rt, gpa, false);
}
fn h1CompleteRefusal(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return h1ShareRefusal(rt, gpa, true);
}

test "share request pool bytes (HTTP/1.1): a request whose copy the client's share refuses is answered 503 and closed" {
    try runIn(h1CopyRefusal);
}
test "share request pool bytes (HTTP/1.1): a complete handler whose allocation the client's share refuses answers 503" {
    try runIn(h1CompleteRefusal);
}

/// A PROXY TLS connection takes its handshake slot after its header, charged
/// to the source the header names: a source over its share of handshakes in
/// progress under pressure is closed there.
fn proxyHandshakes(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var limits = testLimits();
    // Share 2, mark (and cap) 4.
    limits.concurrent_tls_handshakes = 16;
    limits.preface_timeout_ns = 20 * std.time.ns_per_s;
    var rig: Rig = undefined;
    const ep = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    try rig.start(rt, gpa, &.{.{ .tls = .{ .address = ep, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } } }}, limits, true);
    defer rig.stop();
    var c: Conns = .{ .gpa = gpa };
    defer c.closeAll();
    const Run = struct {
        /// `n` PROXY TLS connections naming `src`, each sending its header
        /// and no ClientHello, one at a time; how many are still open after.
        fn run(conns: *Conns, port: u16, src: Peer, n: usize, open: *usize) void {
            for (0..n) |_| {
                const fd = rawConnect(false, port) catch continue;
                conns.addRaw(fd);
                var buf: [proxy.fixed_bytes + 64]u8 = undefined;
                const len = proxy.encode(&buf, .{ .peer = src });
                _ = rawSend(fd, buf[0..len]);
                var r: Raw = .{};
                rawRead(fd, 250, &r);
                if (!r.closed) open.* += 1;
            }
        }
    };
    const s_src: Peer = .{ .ip4 = .{ 198, 51, 100, 20 } };
    const t_src: Peer = .{ .ip4 = .{ 198, 51, 100, 21 } };
    const r0 = rig.refusals(.handshakes);
    var s_open: usize = 0;
    var t_open: usize = 0;
    try blocking(Run.run, .{ &c, rig.port(0), s_src, 8, &s_open });
    try blocking(Run.run, .{ &c, rig.port(0), t_src, 3, &t_open });
    const s_held = rig.count(v4Key(.{ 198, 51, 100, 20 }), .handshakes);
    const t_held = rig.count(v4Key(.{ 198, 51, 100, 21 }), .handshakes);
    std.debug.print("shares handshakes PROXY TLS: 198.51.100.20 holds {d} handshakes, {d} of 8 open (cap 4); 198.51.100.21 holds {d}, {d} of 3 open (share 2); server {d}; refusals {d}\n", .{ s_held, s_open, t_held, t_open, rig.server.accounting.active_handshakes.load(.acquire), rig.refusals(.handshakes) - r0 });
    try expectExactly("one PROXY source's handshakes", s_held, 4);
    try expectExactly("one PROXY source's open connections", s_open, 4);
    try expectExactly("a second PROXY source's handshakes", t_held, 2);
    try expectExactly("a second PROXY source's open connections", t_open, 2);
}

test "share TLS handshakes in progress (PROXY v2): a source over its share is closed after its header" {
    try runIn(proxyHandshakes);
}

// ---------------------------------------------------------------------------
// Outbound bytes: the per-stream shed (the write fails and the stream is
// reset). HTTP/2 only: HTTP/1.1 writes to its socket and holds no outbound
// budget.

fn outboundRun(conns: *Conns, tr: Tr, port: u16, v6: bool, n_conns: usize, per_conn: usize, out: *Tally) void {
    for (0..n_conns) |_| {
        // Window 0: the bodies stay in the server, reserved.
        const c = (H2.open(conns.gpa, tr, port, v6, 0) catch null) orelse {
            out.refused += 1000;
            return;
        };
        const h = conns.addH2(c);
        for (0..per_conn) |_| {
            _ = h.get(conns.gpa, "/big", &.{}) catch {};
            h.poll(80) catch {};
        }
    }
}

fn outboundRow(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    resetRoutes();
    var limits = testLimits();
    // Share 128 KiB, mark (and cap) 256 KiB: 4 and 8 writes of 32 KiB. A
    // connection's budget is a share (nested), 4 writes; the write-pool
    // floor fits under it.
    limits.outbound_bytes_per_server = 1024 * kib;
    limits.outbound_bytes_per_connection = 128 * kib;
    limits.inbound_wire_chunks_per_connection = starh2.core.limits.H2C_MIN_WRITE_QUEUE_CHUNKS;
    // No stream clock may end a held write during the test.
    limits.response_min_rate_grace_ns = 60 * std.time.ns_per_s;
    const mark = 256 * kib;
    const share = 128 * kib;
    var rig: Rig = undefined;
    try rig.startPair(rt, gpa, tr, limits);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    var d: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
        d.closeAll();
    }
    const r0 = rig.refusals(.outbound_bytes);

    // 1. Serially: four connections of three writes each, one at a time.
    var serial: Tally = .{};
    try blocking(outboundRun, .{ &a, tr, rig.port(0), false, 4, 3, &serial });
    const Done = struct {
        fn f(n: usize) bool {
            return big_written.load(.acquire) + big_refused.load(.acquire) >= n;
        }
    };
    try waitFor("every attacker write written or refused", @as(usize, 12), Done.f, 5 * std.time.ns_per_s);
    const held_serial = rig.count(attacker, .outbound_bytes);
    const refused_serial = big_refused.load(.acquire);
    // 2. ::1 during the attack: two connections of three writes. Four fit
    // its share; the rest are refused (the connection's own budget, four
    // writes, is never the limit here).
    var second: Tally = .{};
    try blocking(outboundRun, .{ &d, tr, rig.port(1), true, 2, 3, &second });
    try waitFor("every ::1 write written or refused", @as(usize, 18), Done.f, 5 * std.time.ns_per_s);
    const held_second = rig.count(defender, .outbound_bytes);
    const refused_second = big_refused.load(.acquire) - refused_serial;
    var resets: usize = 0;
    for (a.h2[0..a.n]) |*c| {
        for (c.*.?.rst) |r| resets += @intFromBool(r != null);
    }
    std.debug.print("shares outbound bytes {s} serial: 127.0.0.1 holds {d} KiB, {d} of 12 writes refused, {d} streams reset (mark {d} KiB, one write at most 32 KiB past it); ::1 holds {d} KiB, {d} of 6 refused (share {d} KiB); refusals {d}\n", .{ @tagName(tr), held_serial / kib, refused_serial, resets, mark / kib, held_second / kib, refused_second, share / kib, rig.refusals(.outbound_bytes) - r0 });
    try expectExactly("127.0.0.1 outbound bytes, serially", held_serial, mark);
    try expectExactly("127.0.0.1 writes refused, serially", refused_serial, 4);
    try expectExactly("::1 outbound bytes during the attack", held_second, share);
    try expectExactly("::1 writes refused during the attack", refused_second, 2);
    try expectExactly("outbound refusals", rig.refusals(.outbound_bytes) - r0, refused_serial + refused_second);
    try letGo(&rig, &.{ &a, &d });

    // 3. At once: four connections of four writes, every request sent
    // before any is read.
    const AtOnce = struct {
        fn run(conns: *Conns, t: Tr, port: u16) void {
            for (0..4) |_| {
                const c = (H2.open(conns.gpa, t, port, false, 0) catch null) orelse continue;
                const h = conns.addH2(c);
                for (0..4) |_| _ = h.get(conns.gpa, "/big", &.{}) catch {};
            }
        }
    };
    big_written.store(0, .release);
    big_refused.store(0, .release);
    try blocking(AtOnce.run, .{ &a, tr, rig.port(0) });
    try waitFor("every write written or refused, at once", @as(usize, 16), Done.f, 5 * std.time.ns_per_s);
    const held_once = rig.count(attacker, .outbound_bytes);
    std.debug.print("shares outbound bytes {s} at once: 127.0.0.1 holds {d} KiB from 16 writes sent at once, {d} refused (mark {d} KiB, plus at most {d} writes of 32 KiB in flight)\n", .{ @tagName(tr), held_once / kib, big_refused.load(.acquire), mark / kib, executors });
    try expectAtMost("127.0.0.1 outbound bytes, at once", held_once, mark + executors * big_bytes);
    try letGo(&rig, &.{ &a, &d });
}

fn outboundH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return outboundRow(rt, gpa, .h2c);
}
fn outboundTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return outboundRow(rt, gpa, .tls);
}

test "share outbound bytes (h2c): the per-stream shed past the cap; a second identity gets a full share" {
    try runIn(outboundH2c);
}
test "share outbound bytes (TLS): the per-stream shed past the cap; a second identity gets a full share" {
    try runIn(outboundTls);
}

// ---------------------------------------------------------------------------
// Compression contexts: identity encoding. HTTP/2 only: HTTP/1.1 does not
// compress.

fn brotliRun(conns: *Conns, tr: Tr, port: u16, v6: bool, n: usize, out: *Tally) void {
    const c = (H2.open(conns.gpa, tr, port, v6, 1 << 20) catch null) orelse return;
    const h = conns.addH2(c);
    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
    for (0..n) |_| {
        _ = h.get(conns.gpa, "/brotli", &extra) catch {};
        h.poll(80) catch {};
    }
    h.poll(200) catch {};
    for (0..n) |i| {
        if (h.status[i] == null) continue;
        if (h.brotli[i]) out.admitted += 1 else out.refused += 1;
    }
}

fn compressionRow(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    resetRoutes();
    var limits = testLimits();
    limits.response_compression = true;
    // Share 2, mark (and cap) 4.
    limits.compression_contexts_per_server = 16;
    var rig: Rig = undefined;
    try rig.startPair(rt, gpa, tr, limits);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    var d: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
        d.closeAll();
    }
    const r0 = rig.refusals(.compression);
    var mine: Tally = .{};
    try blocking(brotliRun, .{ &a, tr, rig.port(0), false, 10, &mine });
    const held_a = rig.count(attacker, .compression);
    var second: Tally = .{};
    try blocking(brotliRun, .{ &d, tr, rig.port(1), true, 3, &second });
    const held_d = rig.count(defender, .compression);
    std.debug.print("shares compression {s}: 127.0.0.1 {d} compressed, {d} identity (holds {d}, cap 4); ::1 {d} compressed, {d} identity (holds {d}, share 2); refusals {d}\n", .{ @tagName(tr), mine.admitted, mine.refused, held_a, second.admitted, second.refused, held_d, rig.refusals(.compression) - r0 });
    try expectExactly("127.0.0.1 compressed", mine.admitted, 4);
    try expectExactly("127.0.0.1 identity", mine.refused, 6);
    try expectExactly("127.0.0.1 contexts", held_a, 4);
    try expectExactly("::1 compressed during the attack", second.admitted, 2);
    try expectExactly("::1 identity during the attack", second.refused, 1);
    try expectExactly("compression refusals", rig.refusals(.compression) - r0, mine.refused + second.refused);
    try letGo(&rig, &.{ &a, &d });

    // At once: two connections of six streams, sent before any is read.
    const AtOnce = struct {
        fn run(conns: *Conns, t: Tr, port: u16) void {
            const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
            for (0..2) |_| {
                const c = (H2.open(conns.gpa, t, port, false, 1 << 20) catch null) orelse continue;
                const h = conns.addH2(c);
                for (0..6) |_| _ = h.get(conns.gpa, "/brotli", &extra) catch {};
            }
        }
    };
    try blocking(AtOnce.run, .{ &a, tr, rig.port(0) });
    const Started = struct {
        fn f(n: usize) bool {
            return started.load(.acquire) >= n;
        }
    };
    try waitFor("every response started, at once", @as(usize, 12), Started.f, 5 * std.time.ns_per_s);
    const held_once = rig.count(attacker, .compression);
    std.debug.print("shares compression {s} at once: 127.0.0.1 holds {d} contexts from 12 responses started at once (cap 4, plus at most {d} in flight)\n", .{ @tagName(tr), held_once, executors });
    try expectAtMost("127.0.0.1 contexts, at once", held_once, 4 + executors);
    try letGo(&rig, &.{ &a, &d });
}

fn compressionH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return compressionRow(rt, gpa, .h2c);
}
fn compressionTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return compressionRow(rt, gpa, .tls);
}

test "share compression contexts (h2c): identity encoding past the cap; a second identity gets a full share" {
    try runIn(compressionH2c);
}
test "share compression contexts (TLS): identity encoding past the cap; a second identity gets a full share" {
    try runIn(compressionTls);
}

// ---------------------------------------------------------------------------
// PROXY v2: identities named by the header. The IPv6 /48 aggregate, and the
// NAT case (what the design does not promise).

const trusted_loopback = [_]proxy.Cidr{.{ .addr = .{ .ip4 = .{ 127, 0, 0, 1 } }, .prefix_bits = 32 }};

/// A PROXY connection naming `src`, holding a `/park` handler. True when the
/// server served it past the header (its SETTINGS arrived).
fn proxyAttempt(conns: *Conns, port: u16, src: Peer) !bool {
    const w = try Wire.connect(conns.gpa, .h2c, port);
    var self: H2 = .{ .w = w, .dec = try hpack.Decoder.init(conns.gpa) };
    var buf: [proxy.fixed_bytes + 64]u8 = undefined;
    const n = proxy.encode(&buf, .{ .peer = src });
    var pre = try h2c.buildClientPrefaceAndSettings(conns.gpa);
    defer pre.deinit(conns.gpa);
    try h2c.appendHeaders(conns.gpa, &pre, 1, "/park", true);
    self.next_sid = 3;
    try self.w.sendRaw(buf[0..n]);
    self.w.sendRaw(pre.items) catch {
        self.closed = true;
    };
    const h = conns.addH2(self);
    try h.poll(250);
    return !h.closed and h.settings;
}

fn proxySerial(conns: *Conns, port: u16, src: Peer, n: usize, out: *Tally) void {
    for (0..n) |_| {
        const ok = proxyAttempt(conns, port, src) catch false;
        if (ok) out.admitted += 1 else out.refused += 1;
    }
}

fn proxyRig(rig: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator, max_connections: usize) !void {
    var limits = testLimits();
    limits.max_connections = max_connections;
    const ep = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    try rig.start(rt, gpa, &.{.{ .h2c_prior_knowledge = .{ .address = ep, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } } }}, limits, false);
}

/// One /48 holds what one address may: the aggregate's share is L/4, on the
/// same mark. Six /64s of one /48 attack in turn; another /48 and an IPv4
/// client then each get a full share.
fn aggregateRow(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    // Share 2, aggregate share 4, mark 4.
    try proxyRig(&rig, rt, gpa, 16);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
    }
    const r0 = rig.refusals(.connections);
    var per64: [6]Tally = @splat(.{});
    for (&per64, 0..) |*t, i| {
        var addr = ip6("2001:db8:1::5");
        addr[7] = @intCast(i + 1); // 2001:db8:1:N::5, one /64 each, one /48
        try blocking(proxySerial, .{ &a, rig.port(0), Peer{ .ip6 = addr }, 3, t });
    }
    const agg = rig.count(v6AggKey(ip6("2001:db8:1::5")), .connections);
    var other48: Tally = .{};
    try blocking(proxySerial, .{ &a, rig.port(0), Peer{ .ip6 = ip6("2001:db8:2::5") }, 3, &other48 });
    var v4: Tally = .{};
    try blocking(proxySerial, .{ &a, rig.port(0), Peer{ .ip4 = .{ 192, 0, 2, 40 } }, 3, &v4 });
    var admitted_48: usize = 0;
    for (per64) |t| admitted_48 += t.admitted;
    std.debug.print("shares aggregate: 2001:db8:1::/48 over six /64s admitted {d} of 18 (aggregate holds {d}, cap 4); 2001:db8:2::/48 admitted {d} of 3 (share 2); 192.0.2.40 admitted {d} of 3 (share 2); refusals {d}\n", .{ admitted_48, agg, other48.admitted, v4.admitted, rig.refusals(.connections) - r0 });
    try expectExactly("one /48's aggregate", agg, 4);
    try expectExactly("one /48 admitted over six /64s", admitted_48, 4);
    try expectExactly("another /48 during the attack", other48.admitted, 2);
    try expectExactly("an IPv4 client during the attack", v4.admitted, 2);
    try letGo(&rig, &.{&a});
}

test "share IPv6 aggregate (PROXY v2): one /48 over many /64s holds the cap of one address; others get a full share" {
    try runIn(aggregateRow);
}

/// The NAT case, which the design does not promise to serve: one identity
/// is a crowd. At idle it is admitted up to the mark (a quarter of the
/// server). With 30% of the server held by three other identities, it is
/// held to its share (an eighth) for new work, with 70% of the server free.
fn natRow(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    // Share 5, mark 10.
    try proxyRig(&rig, rt, gpa, 40);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
    }
    const nat: Peer = .{ .ip4 = .{ 198, 51, 100, 7 } };
    var idle: Tally = .{};
    try blocking(proxySerial, .{ &a, rig.port(0), nat, 15, &idle });
    std.debug.print("shares NAT at idle: 198.51.100.7 admitted {d} of 15 (the mark is 10 of 40: 25%)\n", .{idle.admitted});
    try expectExactly("the NAT at idle", idle.admitted, 10);
    try letGo(&rig, &.{&a});

    var others: [3]Tally = @splat(.{});
    for (&others, 0..) |*t, i| {
        try blocking(proxySerial, .{ &a, rig.port(0), Peer{ .ip4 = .{ 192, 0, 2, @intCast(1 + i) } }, 4, t });
        try expectExactly("an identity filling 30%", t.admitted, 4);
    }
    const before = rig.server.active_connections.load(.acquire);
    var busy: Tally = .{};
    try blocking(proxySerial, .{ &a, rig.port(0), nat, 15, &busy });
    std.debug.print("shares NAT at 30% use: {d} of 40 held by three others; 198.51.100.7 admitted {d} of 15 (its share, 5: 12.5%), with {d} of 40 still free after\n", .{ before, busy.admitted, 40 - rig.server.active_connections.load(.acquire) });
    try expectExactly("server use before the NAT", before, 12);
    try expectExactly("the NAT at 30% use", busy.admitted, 5);
    try letGo(&rig, &.{&a});
}

test "share NAT case (PROXY v2): a crowd behind one address gets the cap at idle and only its share at 30% use" {
    try runIn(natRow);
}

test "share connections (PROXY v2): a source over its share is closed after its header" {
    try runIn(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            resetRoutes();
            var rig: Rig = undefined;
            // Share 2, mark (and cap) 4.
            try proxyRig(&rig, rt, gpa, 16);
            defer rig.stop();
            var a: Conns = .{ .gpa = gpa };
            defer {
                release.store(true, .release);
                a.closeAll();
            }
            const r0 = rig.refusals(.connections);
            var src: Tally = .{};
            try blocking(proxySerial, .{ &a, rig.port(0), Peer{ .ip4 = .{ 203, 0, 113, 9 } }, 10, &src });
            var other: Tally = .{};
            try blocking(proxySerial, .{ &a, rig.port(0), Peer{ .ip4 = .{ 203, 0, 113, 10 } }, 3, &other });
            std.debug.print("shares PROXY connections: 203.0.113.9 admitted {d} of 10 (cap 4); 203.0.113.10 admitted {d} of 3 (share 2); refusals {d}; server holds {d}\n", .{ src.admitted, other.admitted, rig.refusals(.connections) - r0, rig.server.active_connections.load(.acquire) });
            try expectExactly("one PROXY source", src.admitted, 4);
            try expectExactly("a second PROXY source", other.admitted, 2);
            try letGo(&rig, &.{&a});
        }
    }.f);
}

// ---------------------------------------------------------------------------
// The start buckets (connection starts on every endpoint, TLS handshake
// starts), and the rate-only records that keep them.

fn rateRefusals(rig: *Rig, kind: u1) u64 {
    if (comptime !@hasField(accounts.Stats, "rate_refusals")) return 0;
    return rig.server.clients.stats().rate_refusals[kind];
}

fn rateOnly(rig: *Rig, k: accounts.Key) bool {
    if (comptime !@hasDecl(accounts.Table, "rateOnly")) return false;
    return rig.server.clients.rateOnly(k);
}

/// Wait, off a zio task, until the server's connections are `n` again (the
/// last one closed and released).
fn waitConnections(server: *starh2.Server, n: usize) void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (server.active_connections.load(.acquire) != n and nowNs() < deadline) osSleepMs(2);
}

const Reconnect = struct {
    admitted: usize = 0,
    refused: usize = 0,
    /// Times the record was found rate-only between two starts: the bucket
    /// outlived the connection.
    rate_only_seen: usize = 0,
};

/// `n` connections from 127.0.0.1, one at a time: each admitted one is
/// closed and released before the next starts.
fn reconnectSerial(rig: *Rig, conns: *Conns, tr: Tr, n: usize, base: usize, out: *Reconnect) void {
    for (0..n) |_| {
        const ok = connAttempt(conns, tr, rig.port(0), false) catch false;
        if (ok) out.admitted += 1 else out.refused += 1;
        conns.closeAll();
        waitConnections(&rig.server, base);
        // Until its handler's link lets go too: then the record has no
        // holder, and only its bucket can keep it.
        const deadline = nowNs() + 2 * std.time.ns_per_s;
        while (rig.server.clients.lookup(attacker) != null and nowNs() < deadline) osSleepMs(2);
        if (rateOnly(rig, attacker)) out.rate_only_seen += 1;
    }
}

fn reconnectRow(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    resetRoutes();
    var limits = testLimits();
    // Connections: share 2, mark 4. A connection-start burst of 4 that
    // refills one a minute, so none comes back during the test.
    limits.max_connections = 16;
    setLimit(&limits, "connection_start_burst", 4);
    setLimit(&limits, "connection_start_refill_ns", 60 * std.time.ns_per_s);
    var rig: Rig = undefined;
    try rig.startPair(rt, gpa, tr, limits);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    var d: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
        d.closeAll();
    }
    // Pressure: ::1 holds its cap, 4, so the server is at the mark.
    var hold: Tally = .{};
    try blocking(connSerial, .{ &d, tr, rig.port(1), true, 4, &hold });
    try expectExactly("::1 holding the mark", hold.admitted, 4);
    const r0 = rateRefusals(&rig, 0);
    // 127.0.0.1 reconnects serially, six times: never more than one
    // connection, always under its share, so only the bucket can refuse.
    var busy: Reconnect = .{};
    try blocking(reconnectSerial, .{ &rig, &a, tr, 6, 4, &busy });
    const refused_busy = rateRefusals(&rig, 0) - r0;
    // No pressure: the empty bucket admits.
    try blocking(Conns.closeAll, .{&d});
    try waitFor("::1 gone", &rig.server, struct {
        fn f(s: *starh2.Server) bool {
            return s.active_connections.load(.acquire) == 0;
        }
    }.f, 5 * std.time.ns_per_s);
    var idle: Reconnect = .{};
    try blocking(reconnectSerial, .{ &rig, &a, tr, 3, 0, &idle });
    std.debug.print("shares connection starts {s}: under pressure 127.0.0.1 reconnected serially 6 times: {d} admitted, {d} closed at accept (burst 4), record rate-only between starts {d} times; then at idle {d} of 3 admitted; rate refusals {d}\n", .{ @tagName(tr), busy.admitted, busy.refused, busy.rate_only_seen, idle.admitted, refused_busy });
    try expectExactly("serial reconnects admitted under pressure", busy.admitted, 4);
    try expectExactly("serial reconnects refused under pressure", busy.refused, 2);
    try expectExactly("rate refusals", refused_busy, 2);
    try expectAtMost("rate-only between starts (at least five)", 5, busy.rate_only_seen);
    try expectExactly("reconnects admitted at idle", idle.admitted, 3);
}

fn reconnectH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return reconnectRow(rt, gpa, .h2c);
}
fn reconnectTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return reconnectRow(rt, gpa, .tls);
}
fn reconnectH1c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return reconnectRow(rt, gpa, .h1c);
}

test "bucket connection starts (h2c): a serial reconnect does not regain its burst; only under pressure" {
    try runIn(reconnectH2c);
}
test "bucket connection starts (TLS): a serial reconnect does not regain its burst; only under pressure" {
    try runIn(reconnectTls);
}
test "bucket connection starts (HTTP/1.1): a serial reconnect does not regain its burst; only under pressure" {
    try runIn(reconnectH1c);
}

/// TLS handshake starts: their own bucket, on the handshakes' pressure. ::1
/// holds four handshakes open (the mark of 16); the connections are far from
/// theirs, so only the handshake bucket can refuse.
fn handshakeStarts(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var limits = testLimits();
    limits.concurrent_tls_handshakes = 16;
    limits.preface_timeout_ns = 20 * std.time.ns_per_s;
    setLimit(&limits, "handshake_start_burst", 4);
    setLimit(&limits, "handshake_start_refill_ns", 60 * std.time.ns_per_s);
    var rig: Rig = undefined;
    try rig.startPair(rt, gpa, .tls, limits);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    var d: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
        d.closeAll();
    }
    var hold: Tally = .{};
    try blocking(hsSerial, .{ &d, rig.port(1), true, 4, &hold });
    try expectExactly("::1 handshakes held", hold.admitted, 4);
    const r0 = rateRefusals(&rig, 1);
    var busy: Reconnect = .{};
    try blocking(reconnectSerial, .{ &rig, &a, .tls, 6, 4, &busy });
    const refused = rateRefusals(&rig, 1) - r0;
    try blocking(Conns.closeAll, .{&d});
    try waitFor("::1 handshakes gone", &rig.server, struct {
        fn f(s: *starh2.Server) bool {
            return s.active_connections.load(.acquire) == 0;
        }
    }.f, 5 * std.time.ns_per_s);
    var idle: Reconnect = .{};
    try blocking(reconnectSerial, .{ &rig, &a, .tls, 3, 0, &idle });
    std.debug.print("shares handshake starts TLS: with 4 handshakes held by ::1, 127.0.0.1 shook hands 6 times serially: {d} admitted, {d} closed at accept (burst 4); then {d} of 3 with none held; handshake-start refusals {d}, connection-start refusals {d}\n", .{ busy.admitted, busy.refused, idle.admitted, refused, rateRefusals(&rig, 0) });
    try expectExactly("handshakes admitted under handshake pressure", busy.admitted, 4);
    try expectExactly("handshake-start refusals", refused, 2);
    try expectExactly("handshakes admitted with none held", idle.admitted, 3);
    try expectExactly("connection-start refusals", @intCast(rateRefusals(&rig, 0)), 0);
}

test "bucket TLS handshake starts: their own bucket, refused only while the handshakes are at their mark" {
    try runIn(handshakeStarts);
}

/// The rate-only records under churn: a table with a reserve of two
/// identities, and twenty identities that each connect once. Rate-only
/// records are evicted, the one that refills soonest first, so the client
/// whose bucket is emptiest keeps it, and no connection is refused at the
/// table. On a PROXY endpoint, so every identity is a header.
fn evictionChurn(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var limits = testLimits();
    // Connections: mark 1, so the one holder below is pressure.
    limits.max_connections = 2;
    limits.concurrent_tls_handshakes = 1;
    limits.max_task_handlers = 1;
    limits.max_endpoints = 1;
    setLimit(&limits, "client_rate_records", 2);
    setLimit(&limits, "connection_start_burst", 2);
    setLimit(&limits, "connection_start_refill_ns", 600 * std.time.ns_per_s);
    var rig: Rig = undefined;
    const ep = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    try rig.start(rt, gpa, &.{.{ .h2c_prior_knowledge = .{ .address = ep, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } } }}, limits, false);
    defer rig.stop();
    var h: Conns = .{ .gpa = gpa };
    var a: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        h.closeAll();
        a.closeAll();
    }
    const Churn = struct {
        fn one(r: *Rig, c: *Conns, src: Peer) bool {
            const ok = proxyAttempt(c, r.port(0), src) catch false;
            c.closeAll();
            waitConnections(&r.server, 1);
            return ok;
        }
        fn run(r: *Rig, c: *Conns, x: Peer, out: *[3]usize) void {
            // X spends its burst of two, then is refused.
            for (0..3) |_| out[0] += @intFromBool(one(r, c, x));
            // Twenty others, once each.
            for (0..20) |i| out[1] += @intFromBool(one(r, c, .{ .ip4 = .{ 192, 0, 2, @intCast(100 + i) } }));
            // X again: its bucket was kept through the churn.
            for (0..2) |_| out[2] += @intFromBool(one(r, c, x));
        }
    };
    var held: Tally = .{};
    try blocking(proxySerial, .{ &h, rig.port(0), Peer{ .ip4 = .{ 203, 0, 113, 1 } }, 1, &held });
    try expectExactly("the holder", held.admitted, 1);
    var got: [3]usize = .{ 0, 0, 0 };
    try blocking(Churn.run, .{ &rig, &a, Peer{ .ip4 = .{ 198, 51, 100, 1 } }, &got });
    const st = rig.server.clients.stats();
    const records = rig.server.clients.records.len;
    const evictions: u64 = if (@hasField(accounts.Stats, "evictions")) st.evictions else 0;
    std.debug.print("shares rate-only churn: a table of {d} records; 198.51.100.1 admitted {d} of 3, twenty others {d} of 20, then 198.51.100.1 {d} of 2; evictions {d}, insert failures {d}\n", .{ records, got[0], got[1], got[2], evictions, st.insert_failures });
    try expectExactly("198.51.100.1 before the churn", got[0], 2);
    try expectExactly("the twenty", got[1], 20);
    try expectExactly("198.51.100.1 after the churn", got[2], 0);
    try expectExactly("insert failures", @intCast(st.insert_failures), 0);
    try std.testing.expect(evictions > 0);
}

test "bucket rate-only records: churn evicts the record that refills soonest, and keeps the emptiest bucket" {
    try runIn(evictionChurn);
}

/// On a PROXY endpoint the connection is counted at accept, before its
/// header names its client, so the start's pressure reading leaves the
/// connection itself out. At three connections held of a mark of four, a
/// source whose bucket is empty is still below the mark and admitted; at
/// four it is refused.
fn proxyStartReading(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var limits = testLimits();
    // Connections: share 2, mark 4. A burst of one.
    limits.max_connections = 16;
    setLimit(&limits, "connection_start_burst", 1);
    setLimit(&limits, "connection_start_refill_ns", 600 * std.time.ns_per_s);
    var rig: Rig = undefined;
    const ep = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    try rig.start(rt, gpa, &.{.{ .h2c_prior_knowledge = .{ .address = ep, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } } }}, limits, false);
    defer rig.stop();
    var h: Conns = .{ .gpa = gpa };
    var x: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        h.closeAll();
        x.closeAll();
    }
    const src: Peer = .{ .ip4 = .{ 198, 51, 100, 30 } };
    const Once = struct {
        fn run(r: *Rig, c: *Conns, p: Peer, held: usize, out: *bool) void {
            out.* = proxyAttempt(c, r.port(0), p) catch false;
            c.closeAll();
            waitConnections(&r.server, held);
        }
    };
    // Three held below the mark (another source, admitted freely).
    var hold: Tally = .{};
    try blocking(proxySerial, .{ &h, rig.port(0), Peer{ .ip4 = .{ 203, 0, 113, 40 } }, 3, &hold });
    try expectExactly("held below the mark", hold.admitted, 3);
    var first = false;
    var second = false;
    var third = false;
    try blocking(Once.run, .{ &rig, &x, src, 3, &first });
    // Its bucket is empty now; the server holds 3, below the mark.
    try blocking(Once.run, .{ &rig, &x, src, 3, &second });
    // A fourth held: at the mark.
    try blocking(proxySerial, .{ &h, rig.port(0), Peer{ .ip4 = .{ 203, 0, 113, 40 } }, 1, &hold });
    try blocking(Once.run, .{ &rig, &x, src, 4, &third });
    std.debug.print("shares PROXY start reading: burst 1; with 3 held 198.51.100.30 admitted {} then {} (empty bucket, below the mark); with 4 held {}; rate refusals {d}\n", .{ first, second, third, rateRefusals(&rig, 0) });
    try std.testing.expect(first);
    try std.testing.expect(second);
    try std.testing.expect(!third);
}

test "bucket connection starts (PROXY v2): the pressure reading leaves out the connection counted at accept" {
    try runIn(proxyStartReading);
}

/// The /48 aggregate has its own start buckets: one /48 starts no faster
/// than one address, however many /64s it rotates through. Under pressure
/// from another identity, five /64s of one /48 start once each, serially
/// (each closed and released before the next): every /64's own bucket is
/// full, so only the aggregate's can refuse the fifth.
const AggStart = enum { connection, handshake };

fn aggregateStarts(rt: *zio.Runtime, gpa: std.mem.Allocator, which: AggStart) !void {
    resetRoutes();
    var limits = testLimits();
    // Connections: mark 4 of 16; handshakes in progress: mark 4 of 16.
    limits.max_connections = 16;
    limits.concurrent_tls_handshakes = 16;
    limits.preface_timeout_ns = 20 * std.time.ns_per_s;
    switch (which) {
        .connection => {
            setLimit(&limits, "connection_start_burst", 4);
            setLimit(&limits, "connection_start_refill_ns", 600 * std.time.ns_per_s);
        },
        .handshake => {
            setLimit(&limits, "handshake_start_burst", 4);
            setLimit(&limits, "handshake_start_refill_ns", 600 * std.time.ns_per_s);
        },
    }
    var rig: Rig = undefined;
    const ep = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const tcp: starh2.EndpointConfig.Tcp = .{ .address = ep, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } };
    const endpoint: starh2.EndpointConfig = if (which == .connection) .{ .h2c_prior_knowledge = tcp } else .{ .tls = tcp };
    try rig.start(rt, gpa, &.{endpoint}, limits, which == .handshake);
    defer rig.stop();
    var h: Conns = .{ .gpa = gpa };
    var x: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        h.closeAll();
        x.closeAll();
    }
    const holder: Peer = .{ .ip4 = .{ 203, 0, 113, 60 } };
    // Pressure from another identity: four connections (connection starts)
    // or four handshakes held open, past their headers with no ClientHello
    // (handshake starts). Each is admitted below the mark.
    switch (which) {
        .connection => {
            var held: Tally = .{};
            try blocking(proxySerial, .{ &h, rig.port(0), holder, 4, &held });
            try expectExactly("connections held by another identity", held.admitted, 4);
        },
        .handshake => {
            const Hold = struct {
                /// A PROXY TLS connection past its header with no ClientHello:
                /// it holds a handshake in progress.
                fn run(c: *Conns, port: u16, src: Peer) void {
                    const fd = rawConnect(false, port) catch return;
                    c.addRaw(fd);
                    var buf: [proxy.fixed_bytes + 64]u8 = undefined;
                    const n = proxy.encode(&buf, .{ .peer = src });
                    _ = rawSend(fd, buf[0..n]);
                }
            };
            for (0..4) |_| try blocking(Hold.run, .{ &h, rig.port(0), holder });
            try waitFor("four handshakes held by another identity", &rig.server, struct {
                fn f(srv: *starh2.Server) bool {
                    return srv.accounting.active_handshakes.load(.acquire) == 4;
                }
            }.f, 3 * std.time.ns_per_s);
        },
    }
    const Starts = struct {
        /// One start from `src`: a PROXY h2c connection served past its header,
        /// or a PROXY TLS connection whose handshake completed. Closed after,
        /// and released before the next.
        fn one(r: *Rig, c: *Conns, w: AggStart, src: Peer) bool {
            const ok = switch (w) {
                .connection => proxyAttempt(c, r.port(0), src) catch false,
                .handshake => tls: {
                    const wire = Wire.connect(c.gpa, .tls, r.port(0)) catch break :tls false;
                    defer wire.close();
                    var buf: [proxy.fixed_bytes + 64]u8 = undefined;
                    const n = proxy.encode(&buf, .{ .peer = src });
                    wire.sendRaw(buf[0..n]) catch break :tls false;
                    wire.handshake() catch break :tls false;
                    break :tls true;
                },
            };
            c.closeAll();
            switch (w) {
                .connection => waitConnections(&r.server, 4),
                .handshake => {
                    const deadline = nowNs() + 3 * std.time.ns_per_s;
                    while ((r.server.active_connections.load(.acquire) != 4 or r.server.accounting.active_handshakes.load(.acquire) != 4) and nowNs() < deadline) osSleepMs(2);
                },
            }
            return ok;
        }
        fn run(r: *Rig, c: *Conns, w: AggStart, out: *[5]bool) void {
            for (out, 0..) |*o, i| {
                var a = ip6("2001:db8:40::9");
                a[7] = @intCast(i + 1); // 2001:db8:40:N::9, one /64 each, one /48
                o.* = one(r, c, w, .{ .ip6 = a });
            }
        }
    };
    const kind: u1 = if (which == .connection) 0 else 1;
    const r0 = rateRefusals(&rig, kind);
    var got: [5]bool = @splat(false);
    try blocking(Starts.run, .{ &rig, &x, which, &got });
    const refused = rateRefusals(&rig, kind) - r0;
    std.debug.print("shares {s} starts, IPv6 aggregate: five /64s of 2001:db8:40::/48 under pressure, burst 4: admitted {any}; rate refusals {d}\n", .{ @tagName(which), got, refused });
    for (got[0..4]) |ok| try std.testing.expect(ok);
    try std.testing.expect(!got[4]);
    try expectExactly("rate refusals (the aggregate's bucket)", @intCast(refused), 1);
    // Conservation: the holders let go, nothing left charged.
    try letGo(&rig, &.{ &h, &x });
    const st = rig.server.clients.stats();
    try expectExactly("underflows", @intCast(st.underflows), 0);
    try expectExactly("stale references", @intCast(st.stale), 0);
    try expectExactly("quarantined records", st.quarantined, 0);
}

fn aggregateConnectionStarts(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return aggregateStarts(rt, gpa, .connection);
}
fn aggregateHandshakeStarts(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return aggregateStarts(rt, gpa, .handshake);
}

test "bucket connection starts (PROXY v2): five /64s of one /48 share the /48's bucket" {
    try runIn(aggregateConnectionStarts);
}
test "bucket TLS handshake starts (PROXY v2): five /64s of one /48 share the /48's bucket" {
    try runIn(aggregateHandshakeStarts);
}

// ---------------------------------------------------------------------------
// The PROXY header waiters: a floor per PROXY endpoint and a shared pool,
// and a header deadline of their own (t-3820).

fn waiterCount(server: *starh2.Server) usize {
    if (comptime @hasDecl(starh2.Server, "proxyWaiters")) return server.proxyWaiters();
    return @field(server, "proxy_waiters").load(.acquire);
}

/// A raw connection to a PROXY endpoint that sends its header (or, with
/// `src` null, only the first bytes of one) and then nothing.
fn proxyRaw(conns: *Conns, port: u16, src: ?Peer) !void {
    const fd = try rawConnect(false, port);
    conns.addRaw(fd);
    var buf: [proxy.fixed_bytes + 64]u8 = undefined;
    if (src) |p| {
        const n = proxy.encode(&buf, .{ .peer = p });
        _ = rawSend(fd, buf[0..n]);
    } else {
        _ = rawSend(fd, proxy.signature[0..6]);
    }
}

fn proxyRawMany(conns: *Conns, port: u16, n: usize, first: u8) void {
    for (0..n) |i| proxyRaw(conns, port, Peer{ .ip4 = .{ 192, 0, 2, first + @as(u8, @intCast(i)) } }) catch {};
}

fn waitWaiters(server: *starh2.Server, n: usize) void {
    const deadline = nowNs() + 3 * std.time.ns_per_s;
    while (waiterCount(server) < n and nowNs() < deadline) osSleepMs(5);
    // Let the rest that could still arrive arrive.
    osSleepMs(150);
}

fn proxyTlsRig(rig: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator, limits: starh2.Limits, endpoints: usize) !void {
    const ep = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const one: starh2.EndpointConfig = .{ .tls = .{ .address = ep, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } } };
    const eps = [_]starh2.EndpointConfig{ one, one };
    try rig.start(rt, gpa, eps[0..endpoints], limits, true);
}

/// Two TLS PROXY endpoints and one handshake slot, held. A TLS PROXY
/// connection keeps its waiter slot until it gets a handshake slot, so every
/// header that arrives on endpoint 0 holds a waiter. With 8 waiters over two
/// endpoints each has a floor of 2 and 4 are shared: endpoint 0 takes its
/// floor and the shared 4 (the pool absorbs its burst), and parks; endpoint
/// 1 still gets its floor. With one cap for all (42c65d8), endpoint 0 takes
/// all 8 and endpoint 1 parks.
fn proxyFloors(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var limits = testLimits();
    limits.proxy_header_waiters = 8;
    limits.concurrent_tls_handshakes = 1;
    limits.preface_timeout_ns = 20 * std.time.ns_per_s;
    if (@hasField(starh2.Limits, "proxy_header_timeout_ns")) @field(limits, "proxy_header_timeout_ns") = 5 * std.time.ns_per_s;
    var rig: Rig = undefined;
    try proxyTlsRig(&rig, rt, gpa, limits, 2);
    defer rig.stop();
    var c: Conns = .{ .gpa = gpa };
    defer c.closeAll();
    // The holder: past its header, it holds the one handshake slot and
    // sends no ClientHello.
    try blocking(proxyRaw, .{ &c, rig.port(0), Peer{ .ip4 = .{ 192, 0, 2, 1 } } });
    try waitFor("the holder's handshake slot", &rig.server, struct {
        fn f(s: *starh2.Server) bool {
            return s.accounting.active_handshakes.load(.acquire) == 1 and waiterCount(s) == 0;
        }
    }.f, 3 * std.time.ns_per_s);
    // Endpoint 0's burst: ten headers.
    try blocking(proxyRawMany, .{ &c, rig.port(0), 10, 10 });
    try blocking(waitWaiters, .{ &rig.server, 6 });
    const after0 = waiterCount(&rig.server);
    // Endpoint 1: three headers.
    try blocking(proxyRawMany, .{ &c, rig.port(1), 3, 40 });
    try blocking(waitWaiters, .{ &rig.server, after0 + 2 });
    const after1 = waiterCount(&rig.server);
    var floor0: usize = 0;
    var floor1: usize = 0;
    var shared: usize = 0;
    if (comptime @hasDecl(starh2.Server, "proxyFloorUsed")) {
        floor0 = rig.server.proxyFloorUsed(0);
        floor1 = rig.server.proxyFloorUsed(1);
        shared = rig.server.proxy_shared_used.load(.acquire);
    }
    std.debug.print("shares PROXY floors: endpoint 0's burst of 10 holds {d} waiters (floor {d}, shared {d}); then endpoint 1 adds {d} (its floor {d}); {d} of 8 in all\n", .{ after0, floor0, shared, after1 - after0, floor1, after1 });
    try expectExactly("endpoint 0's waiters (its floor and the pool)", after0, 6);
    try expectExactly("endpoint 1's waiters during endpoint 0's stall", after1 - after0, 2);
    try expectExactly("endpoint 0's floor", floor0, 2);
    try expectExactly("the shared pool", shared, 4);
    try expectExactly("endpoint 1's floor", floor1, 2);
}

test "PROXY waiters: a stalled TLS PROXY endpoint at its floor does not park a second one; the shared pool absorbs its burst" {
    try runIn(proxyFloors);
}

/// The header deadline is the header's alone. One handshake slot, a header
/// deadline of 600 ms and a preface deadline of 1,500 ms. A header that never
/// completes is closed at the header deadline. W, accepted at t0, sends its
/// complete header at about 500 ms, while H (accepted at 400 ms, its header
/// at once) holds the slot in a handshake that runs to about 1,900 ms. So W
/// waits for the slot past its header deadline (the lower bound: open at
/// 1,000 ms), and is closed at its preface deadline, 1,500 ms from its
/// accept, with nothing left charged: before H lets the slot go, so a wait
/// with no deadline (which would hand W the slot at 1,900 ms and close it at
/// about 3,400) fails.
fn proxyHeaderDeadline(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var limits = testLimits();
    limits.concurrent_tls_handshakes = 1;
    limits.preface_timeout_ns = 1500 * ms;
    setLimit(&limits, "proxy_header_timeout_ns", 600 * ms);
    var rig: Rig = undefined;
    try proxyTlsRig(&rig, rt, gpa, limits, 1);
    defer rig.stop();
    const Stalled = struct {
        fn run(port: u16, closed_ms: *?u64) void {
            const t0 = nowNs();
            const fd = rawConnect(false, port) catch return;
            defer _ = std.c.close(fd);
            _ = rawSend(fd, proxy.signature[0..6]);
            var r: Raw = .{};
            rawRead(fd, 2500, &r);
            if (r.closed) closed_ms.* = (nowNs() - t0) / ms;
        }
    };
    const Waiter = struct {
        server: *starh2.Server,
        port: u16,
        closed_ms: ?u64 = null,
        holder_held: bool = false,
        released_ms: ?u64 = null,

        fn send(fd: std.c.fd_t, src: Peer) void {
            var buf: [proxy.fixed_bytes + 64]u8 = undefined;
            const n = proxy.encode(&buf, .{ .peer = src });
            _ = rawSend(fd, buf[0..n]);
        }

        fn run(self: *@This()) void {
            const t0 = nowNs();
            const w = rawConnect(false, self.port) catch return;
            defer _ = std.c.close(w);
            osSleepMs(400);
            // H: past its header at once, it takes the one slot and sends no
            // ClientHello, so it holds the slot to its own handshake deadline.
            const h = rawConnect(false, self.port) catch return;
            defer _ = std.c.close(h);
            send(h, .{ .ip4 = .{ 192, 0, 2, 1 } });
            const until = nowNs() + 300 * ms;
            while (self.server.accounting.active_handshakes.load(.acquire) != 1 and nowNs() < until) osSleepMs(2);
            self.holder_held = self.server.accounting.active_handshakes.load(.acquire) == 1;
            const at = t0 + 500 * ms;
            if (nowNs() < at) osSleepMs((at - nowNs()) / ms);
            send(w, .{ .ip4 = .{ 192, 0, 2, 2 } });
            var r: Raw = .{};
            rawRead(w, 2500 - (nowNs() - t0) / ms, &r);
            if (!r.closed) return;
            self.closed_ms = (nowNs() - t0) / ms;
            // Nothing of W's left: no waiter slot, no client record.
            const deadline = nowNs() + 500 * ms;
            while (nowNs() < deadline) {
                if (waiterCount(self.server) == 0 and self.server.clients.lookup(v4Key(.{ 192, 0, 2, 2 })) == null) {
                    self.released_ms = (nowNs() - t0) / ms;
                    return;
                }
                osSleepMs(2);
            }
        }
    };
    var stalled: ?u64 = null;
    var waiter: Waiter = .{ .server = &rig.server, .port = rig.port(0) };
    var hs = try zio.spawnBlocking(Stalled.run, .{ rig.port(0), &stalled });
    var hw = try zio.spawnBlocking(Waiter.run, .{&waiter});
    hs.join();
    hw.join();
    std.debug.print("shares PROXY header deadline 600 ms, preface 1500 ms: a stalled header closed at {?d} ms; a complete header waiting for the handshake slot (held by another to about 1,900 ms: {}) closed at {?d} ms from its accept, its waiter slot and client charge gone at {?d} ms\n", .{ stalled, waiter.holder_held, waiter.closed_ms, waiter.released_ms });
    try std.testing.expect(waiter.holder_held);
    try std.testing.expect(stalled != null);
    try expectAtMost("the header deadline before the stalled close (ms)", 600, stalled.? + 5);
    try expectAtMost("the stalled header's close (ms)", stalled.?, 900);
    try std.testing.expect(waiter.closed_ms != null);
    // Open past its header deadline, waiting for the slot...
    try expectAtMost("the waiting header's close, at least (ms)", 1400, waiter.closed_ms.?);
    // ...and closed at its preface deadline, before the holder lets go.
    try expectAtMost("the waiting header's close (ms)", waiter.closed_ms.?, 1800);
    try std.testing.expect(waiter.released_ms != null);
}

test "PROXY waiters: the header deadline is the header's own; the wait for a TLS handshake slot keeps the preface deadline" {
    try runIn(proxyHeaderDeadline);
}

test "PROXY waiters: more PROXY endpoints than waiters, and a header deadline past the preface's, are boot errors" {
    if (comptime !@hasField(starh2.Limits, "proxy_header_timeout_ns")) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(1) });
    defer rt.deinit();
    const ep = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const pp: starh2.EndpointConfig = .{ .h2c_prior_knowledge = .{ .address = ep, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } } };
    var limits = testLimits();
    limits.proxy_header_waiters = 1;
    try std.testing.expectError(error.ProxyWaitersBelowEndpoints, starh2.Server.init(gpa, rt.io(), .{ .endpoints = &.{ pp, pp }, .routes = &routes, .tls = null, .limits = limits }));
    limits = testLimits();
    limits.proxy_header_timeout_ns = limits.preface_timeout_ns + 1;
    try std.testing.expectError(error.InvalidConfig, starh2.Server.init(gpa, rt.io(), .{ .endpoints = &.{pp}, .routes = &routes, .tls = null, .limits = limits }));
    limits.proxy_header_timeout_ns = 0;
    try std.testing.expectError(error.InvalidConfig, starh2.Server.init(gpa, rt.io(), .{ .endpoints = &.{pp}, .routes = &routes, .tls = null, .limits = limits }));
    // Two endpoints and two waiters: a floor of one each, none shared.
    limits = testLimits();
    limits.proxy_header_waiters = 2;
    var s = try starh2.Server.init(gpa, rt.io(), .{ .endpoints = &.{ pp, pp }, .routes = &routes, .tls = null, .limits = limits });
    defer s.deinit(gpa);
    try std.testing.expectEqual(@as(usize, 1), s.proxy_floor);
    try std.testing.expectEqual(@as(usize, 0), s.proxy_shared);
}
