//! Client accounts, client identity and PROXY v2 (t-3759 step 3).
//!
//! What each gate shows, on real sockets:
//!
//! - Identity on the plain path: a connection from 127.0.0.1 and one from
//!   ::1 are two clients; the IPv6 one is charged to its /64 and to its
//!   aggregate. No test hook makes an identity: the kernel's peer address
//!   does.
//! - PROXY v2: the header's source is the client (v4; v6, where two addresses
//!   in one /64 are one client and two /64s in one /48 share the aggregate;
//!   `LOCAL` goes to the endpoint's infrastructure account), on h2c, h1c and
//!   TLS (where the handshake slot is taken after the header). An untrusted
//!   source, an over-size header and a malformed one are closed. A header
//!   that never finishes is held only to the preface deadline and only
//!   inside its own cap.
//! - `.none` on a Unix socket charges no client; `.none` behind a proxy
//!   that sends PROXY v2 strips the header, shows the handler the header's
//!   source, and charges no client; the boot errors.
//! - Orphans stay charged: a handler that outlives its connection keeps its
//!   client, aggregate and server charges until it returns; only the
//!   connection's own charge goes at teardown.
//! - A handshake in progress, outbound bytes past the flow-control window
//!   and a compression context, each held live, are charged to the client
//!   and its aggregate, and released with them.
//! - The table has room for `max_connections` IPv6 clients in different
//!   /48s (two records each), and for the records an accept loop holds while
//!   it waits for admission; a table that is still full refuses the
//!   connection and counts it.
//! - The soak: h2c, TLS, h1c and PROXY connections with random reset, close,
//!   connection-error GOAWAY and shutdown timing, against handlers that
//!   honour and ignore cancel; every account is zero at the end, the same key
//!   is removed and inserted again across rounds, and records are reused with
//!   a new generation.
//!
//! Every server in this file, as in every other test file, ends with
//! `Server.deinit`, which checks that the table is empty (`Table.checkEmpty`)
//! and logs an error otherwise, which fails the test.
//!
//! None of this compiles on d33e615: `Server.clients`, the endpoints'
//! `client_identity` and `proxy_protocol`, and `edge.accounts` are new.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");
const serve_deadline = @import("serve_deadline.zig");

const accounts = starh2.edge.accounts;
const proxy = starh2.edge.proxy_protocol;
const server_mod = starh2.edge.server;
const frame = starh2.core.frame;
const Peer = starh2.Peer;

const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

// ---------------------------------------------------------------------------
// Routes.

/// Set to let every `/park` and `/held` handler return.
var release: std.atomic.Value(bool) = .init(false);
var started: std.atomic.Value(usize) = .init(0);
var ended: std.atomic.Value(usize) = .init(0);
/// The `Request.peer` each handler saw, in start order.
var peers: [64]Peer = undefined;
var peers_n: std.atomic.Value(usize) = .init(0);

fn notePeer(p: Peer) void {
    const i = peers_n.fetchAdd(1, .acq_rel);
    if (i < peers.len) peers[i] = p;
}

fn sawPeer(p: Peer) bool {
    const n = @min(peers_n.load(.acquire), peers.len);
    for (peers[0..n]) |q| {
        if (std.meta.eql(p, q)) return true;
    }
    return false;
}

fn resetRoutes() void {
    release.store(false, .release);
    started.store(0, .release);
    ended.store(0, .release);
    peers_n.store(0, .release);
}

/// Honours cancel: returns when `release` is set or its stream is gone.
fn park(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    notePeer(req.peer);
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    while (!release.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    try resp.send(200, &.{}, "ok");
}

/// Ignores cancel (shielded) until `release`: outlives its connection.
fn held(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    notePeer(req.peer);
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    {
        zio.beginShield();
        defer zio.endShield();
        // Touch request memory while orphaned, so the arena is live.
        const a = req.arena;
        const scratch = a.alloc(u8, 4096) catch null;
        while (!release.load(.acquire)) zio.sleep(.fromMilliseconds(2)) catch {};
        if (scratch) |s| @memset(s, 1);
    }
    resp.send(200, &.{}, "late") catch {};
}

fn queryMs(req: *const starh2.Request) u64 {
    const q = req.query;
    if (!std.mem.startsWith(u8, q, "ms=")) return 0;
    return std.fmt.parseInt(u64, q[3..], 10) catch 0;
}

/// Soak: waits `ms` or until canceled, writing as it goes.
fn honour(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const until = nowNs() + queryMs(req) * ms;
    var body = try resp.start(200, &.{});
    const chunk: [512]u8 = @splat('h');
    while (nowNs() < until) {
        try body.writeAll(&chunk);
        try zio.sleep(.fromMilliseconds(5));
    }
    try body.finish();
}

/// Soak: waits `ms` shielded (ignoring cancel), then tries to answer.
fn ignore(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const until = nowNs() + queryMs(req) * ms;
    {
        zio.beginShield();
        defer zio.endShield();
        _ = req.arena.alloc(u8, 70 * 1024) catch null; // a second block
        while (nowNs() < until) zio.sleep(.fromMilliseconds(3)) catch {};
    }
    resp.send(200, &.{}, "ignored") catch {};
}

/// Writes 256 KiB to a client that reads nothing and opens no window: past
/// the stream's 64 KiB flow-control window, so the rest stays reserved as
/// outbound bytes while it waits. Then waits for `release`.
fn big(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    var body = try resp.start(200, &.{});
    const chunk: [16 * 1024]u8 = @splat('b');
    for (0..16) |_| {
        if (release.load(.acquire)) break;
        try body.writeAll(&chunk);
        try body.flush();
    }
    while (!release.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    try body.finish();
}

/// A compressible streaming response held open: its brotli context stays
/// taken until it finishes.
fn brotliHeld(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    var body = try resp.start(200, &.{.{ .name = "content-type", .value = "text/plain" }});
    try body.writeAll("compressible text, held open until the test lets it finish");
    try body.flush();
    while (!release.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    try body.finish();
}

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/park", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = park } } },
    .{ .method = .GET, .path = "/held", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = held } } },
    .{ .method = .GET, .path = "/honour", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = honour } } },
    .{ .method = .GET, .path = "/ignore", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = ignore } } },
    .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = big } } },
    .{ .method = .GET, .path = "/brotli", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = brotliHeld } } },
};

// ---------------------------------------------------------------------------
// The server.

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
        self.server = try starh2.Server.init(gpa, io, .{
            .endpoints = endpoints,
            .routes = &routes,
            .tls = tls_cfg,
            .limits = limits,
        });
        self.handle = try rt.spawn(starh2.Server.serve, .{ &self.server, gpa });
        try self.server.waitUntilListening(5 * std.time.ns_per_s);
    }

    fn port(self: *Rig, i: usize) u16 {
        return self.server.localAddress(i).getPort();
    }

    /// Shut down, wait for `serve`, and deinit (which checks the table).
    fn stop(self: *Rig) void {
        self.server.requestShutdown();
        serve_deadline.join(&self.server, &self.handle);
        self.server.deinit(self.gpa);
        if (self.cert.len != 0) self.gpa.free(self.cert);
        if (self.key.len != 0) self.gpa.free(self.key);
    }

    fn entry(self: *Rig, k: accounts.Key) ?accounts.Table.Entry {
        return self.server.clients.lookup(k);
    }
};

fn v4Key(a: [4]u8) accounts.Key {
    var buf: [accounts.max_refs]accounts.Key = undefined;
    return accounts.keysFor(.{ .ip4 = a }, 48, &buf)[0];
}

fn v6Keys(a: [16]u8) [2]accounts.Key {
    var buf: [accounts.max_refs]accounts.Key = undefined;
    const k = accounts.keysFor(.{ .ip6 = a }, 48, &buf);
    return .{ k[0], k[1] };
}

fn ip6(text: []const u8) [16]u8 {
    const a = std.Io.net.IpAddress.parseIp6(text, 0) catch unreachable;
    return a.ip6.bytes;
}

fn waitUntil(comptime what: []const u8, ctx: anytype, comptime done: fn (@TypeOf(ctx)) bool, timeout_ns: u64) !void {
    const deadline = nowNs() + timeout_ns;
    while (!done(ctx)) {
        if (nowNs() >= deadline) {
            std.debug.print("accounts: timed out waiting for {s}\n", .{what});
            return error.StateNotReached;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

fn waitAtomic(comptime what: []const u8, v: *const std.atomic.Value(usize), want: usize, timeout_ns: u64) !void {
    const deadline = nowNs() + timeout_ns;
    while (v.load(.acquire) != want) {
        if (nowNs() >= deadline) {
            std.debug.print("accounts: {s} is {d}, waited for {d}\n", .{ what, v.load(.acquire), want });
            return error.StateNotReached;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

fn tableEmpty(rig: *Rig) bool {
    return rig.server.clients.stats().live == 0;
}

fn expectCount(e: accounts.Table.Entry, res: accounts.Resource, want: usize) !void {
    if (e.count(res) == want) return;
    std.debug.print("accounts: {f} {s} is {d}, want {d}\n", .{ accounts.fmtKey(e.key), @tagName(res), e.count(res), want });
    return error.WrongCount;
}

// ---------------------------------------------------------------------------
// Clients. h2c and h1c run on zio tasks (zio sockets park the task, never the
// executor); TLS runs on `spawnBlocking` threads (the `Wire` client blocks).

fn connect(addr: zio.net.IpAddress) !zio.net.Stream {
    return addr.connect(.{});
}

fn writeAll(s: zio.net.Stream, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) off += try s.write(bytes[off..], .none);
}

fn proxyHeader(buf: []u8, src: proxy.Source) []const u8 {
    return buf[0..proxy.encode(buf, src)];
}

/// The h2c preface and SETTINGS, then GET `path` on `streams` streams.
fn h2Requests(gpa: std.mem.Allocator, path: []const u8, streams: u31) !std.ArrayList(u8) {
    var w = try h2c.buildClientPrefaceAndSettings(gpa);
    errdefer w.deinit(gpa);
    var sid: u31 = 1;
    for (0..streams) |_| {
        try h2c.appendHeaders(gpa, &w, sid, path, true);
        sid += 2;
    }
    return w;
}

/// Read until EOF; the milliseconds that took, or an error past `timeout`.
fn readToEof(s: zio.net.Stream, timeout_ns: u64) !u64 {
    const t0 = nowNs();
    var buf: [4096]u8 = undefined;
    while (true) {
        const left = (t0 + timeout_ns) -| nowNs();
        if (left == 0) return error.NoEof;
        const n = s.read(&buf, .{ .duration = .fromNanoseconds(left) }) catch |err| switch (err) {
            error.Timeout => return error.NoEof,
            else => return (nowNs() - t0) / ms,
        };
        if (n == 0) return (nowNs() - t0) / ms;
    }
}

/// Read until `needle` arrives (plaintext).
fn readUntil(s: zio.net.Stream, needle: []const u8, timeout_ns: u64) !void {
    const t0 = nowNs();
    var buf: [8192]u8 = undefined;
    var len: usize = 0;
    while (std.mem.indexOf(u8, buf[0..len], needle) == null) {
        const left = (t0 + timeout_ns) -| nowNs();
        if (left == 0 or len == buf.len) return error.NoResponse;
        const n = try s.read(buf[len..], .{ .duration = .fromNanoseconds(left) });
        if (n == 0) return error.ClosedBeforeResponse;
        len += n;
    }
}

fn resetClose(s: zio.net.Stream) void {
    const lg: std.c.linger = .{ .onoff = 1, .linger = 0 };
    _ = std.c.setsockopt(s.socket.handle, std.c.SOL.SOCKET, std.c.SO.LINGER, std.mem.asBytes(&lg), @sizeOf(std.c.linger));
    s.close();
}

fn testLimits() starh2.Limits {
    var l = starh2.Limits.defaults;
    l.graceful_drain_timeout_ns = 300 * ms;
    return l;
}

fn runIn(comptime f: anytype) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(f, .{ rt, gpa });
    try h.join();
}

// ---------------------------------------------------------------------------
// Identity on the plain path.

fn plainIdentity(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp6("::1", 0) } },
    }, testLimits(), false);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);

    var w4 = try h2Requests(gpa, "/park", 2);
    defer w4.deinit(gpa);
    var w6 = try h2Requests(gpa, "/park", 1);
    defer w6.deinit(gpa);
    const a = try connect(try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(0)));
    try writeAll(a, w4.items);
    const b = try connect(try zio.net.IpAddress.parseIp6("::1", rig.port(1)));
    try writeAll(b, w6.items);
    try waitAtomic("handlers started", &started, 3, 3 * std.time.ns_per_s);

    const k4 = v4Key(.{ 127, 0, 0, 1 });
    const k6 = v6Keys(ip6("::1"));
    const e4 = rig.entry(k4) orelse return error.NoV4Account;
    try expectCount(e4, .connections, 1);
    try expectCount(e4, .streams, 2);
    try expectCount(e4, .task_handlers, 2);
    try std.testing.expect(e4.count(.request_bytes) >= 2 * 64 * 1024);
    // The connection and two links.
    try std.testing.expectEqual(@as(u32, 3), e4.holders);
    for (k6) |k| {
        const e = rig.entry(k) orelse return error.NoV6Account;
        try expectCount(e, .connections, 1);
        try expectCount(e, .streams, 1);
        try expectCount(e, .task_handlers, 1);
        try std.testing.expectEqual(@as(u32, 2), e.holders);
    }
    try std.testing.expectEqual(@as(usize, 3), rig.server.clients.stats().live);
    try std.testing.expect(sawPeer(.{ .ip4 = .{ 127, 0, 0, 1 } }));
    try std.testing.expect(sawPeer(.{ .ip6 = ip6("::1") }));
    std.debug.print("accounts plain: 127.0.0.1 {d} request bytes, ::1/64 {d}, 3 records\n", .{ e4.count(.request_bytes), rig.entry(k6[0]).?.count(.request_bytes) });

    release.store(true, .release);
    try waitAtomic("handlers ended", &ended, 3, 3 * std.time.ns_per_s);
    a.close();
    b.close();
    try waitUntil("table empty", &rig, tableEmpty, 3 * std.time.ns_per_s);
}

test "identity: 127.0.0.1 and ::1 are two clients; the IPv6 one is its /64 and its aggregate" {
    try runIn(plainIdentity);
}

// ---------------------------------------------------------------------------
// PROXY v2 on h2c and h1c.

const trusted_loopback = [_]proxy.Cidr{ .{ .addr = .{ .ip4 = .{ 127, 0, 0, 1 } }, .prefix_bits = 32 } };

/// A TCP endpoint behind a PROXY v2 proxy at one of `trusted`, keyed by the
/// header's source.
fn ppTcp(address: starh2.EndpointAddress, trusted: []const proxy.Cidr) starh2.EndpointConfig.Tcp {
    return .{ .address = address, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = trusted } } };
}

fn proxyH2(addr: zio.net.IpAddress, gpa: std.mem.Allocator, src: proxy.Source, path: []const u8) !zio.net.Stream {
    var hb: [64]u8 = undefined;
    var w = try h2Requests(gpa, path, 1);
    defer w.deinit(gpa);
    const s = try connect(addr);
    errdefer s.close();
    try writeAll(s, proxyHeader(&hb, src));
    try writeAll(s, w.items);
    return s;
}

fn proxyIdentity(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
        .{ .h1c = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
    }, testLimits(), false);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);
    const h2_addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(0));
    const h1_addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(1));

    const v4src: [4]u8 = .{ 203, 0, 113, 7 };
    const a1 = ip6("2001:db8:1:2::5");
    const a2 = ip6("2001:db8:1:2::9");
    const a3 = ip6("2001:db8:1:3::5");
    var conns: [6]zio.net.Stream = undefined;
    var n: usize = 0;
    defer for (conns[0..n]) |c| c.close();
    conns[n] = try proxyH2(h2_addr, gpa, .{ .peer = .{ .ip4 = v4src } }, "/park");
    n += 1;
    conns[n] = try proxyH2(h2_addr, gpa, .{ .peer = .{ .ip6 = a1 } }, "/park");
    n += 1;
    conns[n] = try proxyH2(h2_addr, gpa, .{ .peer = .{ .ip6 = a2 } }, "/park");
    n += 1;
    conns[n] = try proxyH2(h2_addr, gpa, .{ .peer = .{ .ip6 = a3 } }, "/park");
    n += 1;
    conns[n] = try proxyH2(h2_addr, gpa, .local, "/park");
    n += 1;
    // HTTP/1.1 behind the same kind of proxy.
    {
        var hb: [64]u8 = undefined;
        const s = try connect(h1_addr);
        conns[n] = s;
        n += 1;
        try writeAll(s, proxyHeader(&hb, .{ .peer = .{ .ip4 = .{ 198, 51, 100, 4 } } }));
        try writeAll(s, "GET /park HTTP/1.1\r\nHost: localhost\r\n\r\n");
    }
    try waitAtomic("handlers started", &started, 6, 3 * std.time.ns_per_s);

    const e4 = rig.entry(v4Key(v4src)) orelse return error.NoV4Account;
    try expectCount(e4, .connections, 1);
    try expectCount(e4, .streams, 1);
    try std.testing.expect(rig.entry(v4Key(.{ 127, 0, 0, 1 })) == null);
    // Two addresses in one /64: one client.
    const k1 = v6Keys(a1);
    const k3 = v6Keys(a3);
    try std.testing.expect(k1[0].eql(v6Keys(a2)[0]));
    const e64 = rig.entry(k1[0]) orelse return error.NoV6Account;
    try expectCount(e64, .connections, 2);
    try expectCount(e64, .streams, 2);
    const e64b = rig.entry(k3[0]) orelse return error.NoV6Account;
    try expectCount(e64b, .connections, 1);
    // Two /64s in one /48: one aggregate.
    try std.testing.expect(k1[1].eql(k3[1]));
    const agg = rig.entry(k1[1]) orelse return error.NoAggregate;
    try expectCount(agg, .connections, 3);
    try expectCount(agg, .streams, 3);
    try expectCount(agg, .task_handlers, 3);
    const infra = rig.entry(accounts.infrastructureKey(0)) orelse return error.NoInfrastructureAccount;
    try expectCount(infra, .connections, 1);
    try expectCount(infra, .streams, 1);
    const e1 = rig.entry(v4Key(.{ 198, 51, 100, 4 })) orelse return error.NoH1Account;
    try expectCount(e1, .connections, 1);
    try expectCount(e1, .streams, 1);
    try expectCount(e1, .task_handlers, 1);
    try std.testing.expectEqual(@as(usize, 6), rig.server.clients.stats().live);
    // The handler sees the client, not the proxy; LOCAL sees the proxy.
    try std.testing.expect(sawPeer(.{ .ip4 = v4src }));
    try std.testing.expect(sawPeer(.{ .ip6 = a1 }));
    try std.testing.expect(sawPeer(.{ .ip4 = .{ 198, 51, 100, 4 } }));
    try std.testing.expect(sawPeer(.{ .ip4 = .{ 127, 0, 0, 1 } }));
    try std.testing.expectEqual(@as(usize, 0), rig.server.proxyWaiters());
    std.debug.print("accounts proxy: 6 records (v4, two /64, /48, LOCAL, h1)\n", .{});

    release.store(true, .release);
    try waitAtomic("handlers ended", &ended, 6, 3 * std.time.ns_per_s);
    for (conns[0..n]) |c| c.close();
    n = 0;
    try waitUntil("table empty", &rig, tableEmpty, 3 * std.time.ns_per_s);
}

test "PROXY v2: the header's source is the client (v4, one /64, one /48, LOCAL, HTTP/1.1)" {
    try runIn(proxyIdentity);
}

/// TLS behind PROXY: the header first, then the handshake. Blocking client,
/// on a `spawnBlocking` thread.
const TlsProxyClient = struct {
    gpa: std.mem.Allocator,
    port: u16,
    src: [4]u8,
    ok: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,

    fn run(self: *TlsProxyClient) void {
        self.body() catch |e| {
            self.err = e;
        };
        self.done.store(true, .release);
    }

    fn body(self: *TlsProxyClient) !void {
        const w = try wire_client.Wire.connect(self.gpa, .tls, self.port);
        defer w.close();
        var hb: [64]u8 = undefined;
        try w.sendRaw(proxyHeader(&hb, .{ .peer = .{ .ip4 = self.src } }));
        var req = try h2Requests(self.gpa, "/park", 1);
        defer req.deinit(self.gpa);
        try w.handshake();
        try w.sendUnchecked(req.items);
        self.ok.store(true, .release);
        while (!release.load(.acquire)) {
            starh2.test_sleep.threadSleepMs(5);
        }
    }
};

fn proxyTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    var limits = testLimits();
    limits.concurrent_tls_handshakes = 1;
    try rig.start(rt, gpa, &.{
        .{ .tls = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
    }, limits, true);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);
    var c1: TlsProxyClient = .{ .gpa = gpa, .port = rig.port(0), .src = .{ 192, 0, 2, 50 } };
    var c2: TlsProxyClient = .{ .gpa = gpa, .port = rig.port(0), .src = .{ 192, 0, 2, 51 } };
    var h1 = try zio.spawnBlocking(TlsProxyClient.run, .{&c1});
    var h2 = try zio.spawnBlocking(TlsProxyClient.run, .{&c2});
    var joined = false;
    // The client threads wait for `release`: on a failed check they must
    // still end, or the runtime's deinit waits for them for ever.
    defer if (!joined) {
        release.store(true, .release);
        h1.join();
        h2.join();
    };
    try waitAtomic("handlers started", &started, 2, 10 * std.time.ns_per_s);
    if (c1.err) |e| return e;
    if (c2.err) |e| return e;
    for ([_][4]u8{ c1.src, c2.src }) |src| {
        const e = rig.entry(v4Key(src)) orelse return error.NoAccount;
        try expectCount(e, .connections, 1);
        try expectCount(e, .handshakes, 0);
        try expectCount(e, .streams, 1);
    }
    try std.testing.expectEqual(@as(usize, 0), rig.server.accounting.active_handshakes.load(.acquire));
    std.debug.print("accounts proxy tls: two clients behind one handshake slot, both charged\n", .{});
    release.store(true, .release);
    h1.join();
    h2.join();
    joined = true;
    try waitUntil("table empty", &rig, tableEmpty, 3 * std.time.ns_per_s);
}

test "PROXY v2 on TLS: the handshake slot is taken after the header, charged to the client" {
    try runIn(proxyTls);
}

fn proxyRefusals(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    const ten = [_]proxy.Cidr{try proxy.Cidr.parse("10.0.0.0/8")};
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &ten) },
        .{ .h2c_prior_knowledge = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
    }, testLimits(), false);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);
    const untrusted = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(0));
    const trusted = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(1));

    // An untrusted source: closed at accept, never keyed to its address.
    // The close can come before the client's write, which then fails.
    {
        const s = try connect(untrusted);
        defer s.close();
        var hb: [64]u8 = undefined;
        var w = try h2Requests(gpa, "/hello", 1);
        defer w.deinit(gpa);
        writeAll(s, proxyHeader(&hb, .{ .peer = .{ .ip4 = .{ 203, 0, 113, 9 } } })) catch {};
        writeAll(s, w.items) catch {};
        const t = try readToEof(s, 2 * std.time.ns_per_s);
        std.debug.print("accounts proxy: untrusted source closed after {d} ms\n", .{t});
        try std.testing.expectEqual(@as(u64, 1), rig.server.proxy_untrusted.load(.acquire));
    }
    // A header whose length field passes the cap: closed unread.
    {
        const s = try connect(trusted);
        defer s.close();
        var hb: [16]u8 = undefined;
        @memcpy(hb[0..12], &proxy.signature);
        hb[12] = 0x21;
        hb[13] = 0x11;
        std.mem.writeInt(u16, hb[14..16], 1000, .big);
        writeAll(s, &hb) catch {};
        _ = try readToEof(s, 2 * std.time.ns_per_s);
    }
    // PROXY v1 text: malformed for a v2 endpoint.
    {
        const s = try connect(trusted);
        defer s.close();
        writeAll(s, "PROXY TCP4 203.0.113.9 192.0.2.1 40000 443\r\n") catch {};
        _ = try readToEof(s, 2 * std.time.ns_per_s);
    }
    try waitUntil("failures counted", &rig, struct {
        fn f(r: *Rig) bool {
            return r.server.proxy_header_failures.load(.acquire) == 2;
        }
    }.f, 2 * std.time.ns_per_s);
    try std.testing.expect(rig.entry(v4Key(.{ 203, 0, 113, 9 })) == null);
    try std.testing.expect(rig.entry(v4Key(.{ 127, 0, 0, 1 })) == null);
    // The trusted endpoint still serves a good header.
    {
        const s = try proxyH2(trusted, gpa, .{ .peer = .{ .ip4 = .{ 203, 0, 113, 10 } } }, "/hello");
        defer s.close();
        try readUntil(s, "ok", 3 * std.time.ns_per_s);
    }
    try waitUntil("table empty", &rig, tableEmpty, 3 * std.time.ns_per_s);
}

test "PROXY v2: an untrusted source, an over-size header and a v1 header are closed" {
    try runIn(proxyRefusals);
}

/// The waiter count as seen at each 1 ms sample, its maximum.
var waiters_max: std.atomic.Value(usize) = .init(0);

fn sampleWaiters(server: *starh2.Server, stop: *std.atomic.Value(bool)) void {
    while (!stop.load(.acquire)) {
        const n = server.proxyWaiters();
        _ = waiters_max.fetchMax(n, .acq_rel);
        zio.sleep(.fromMilliseconds(1)) catch return;
    }
}

const StalledEnd = struct {
    stream: zio.net.Stream,
    ms: u64 = 0,
    err: ?anyerror = null,

    fn run(self: *StalledEnd, t0: u64) void {
        _ = readToEof(self.stream, 5 * std.time.ns_per_s) catch |e| {
            self.err = e;
            return;
        };
        self.ms = (nowNs() - t0) / ms;
    }
};

fn proxyStalled(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    waiters_max.store(0, .release);
    var rig: Rig = undefined;
    var limits = testLimits();
    limits.preface_timeout_ns = 600 * ms;
    // The PROXY header deadline may not be later (`Limits`).
    limits.proxy_header_timeout_ns = limits.preface_timeout_ns;
    limits.proxy_header_waiters = 2;
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
    }, limits, false);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);
    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(0));

    var stop_sampler: std.atomic.Value(bool) = .init(false);
    var sampler = try zio.spawn(sampleWaiters, .{ &rig.server, &stop_sampler });
    // Three headers that never finish: eight bytes of the signature.
    var ends: [3]StalledEnd = undefined;
    var tasks: [3]zio.JoinHandle(void) = undefined;
    const t0 = nowNs();
    for (&ends, &tasks) |*e, *t| {
        e.* = .{ .stream = try connect(addr) };
        try writeAll(e.stream, proxy.signature[0..8]);
        t.* = try zio.spawn(StalledEnd.run, .{ e, t0 });
    }
    // Only two are inside the cap; the third waits in the listen queue.
    zio.sleep(.fromMilliseconds(300)) catch {};
    try std.testing.expectEqual(@as(usize, 2), rig.server.proxyWaiters());
    try std.testing.expectEqual(@as(usize, 2), rig.server.active_connections.load(.acquire));
    for (&tasks) |*t| t.join();
    stop_sampler.store(true, .release);
    sampler.join();
    for (ends) |e| {
        if (e.err) |err| return err;
        e.stream.close();
    }
    var sorted: [3]u64 = .{ ends[0].ms, ends[1].ms, ends[2].ms };
    std.mem.sort(u64, &sorted, {}, std.sort.asc(u64));
    std.debug.print("accounts proxy stalled: closed at {d}, {d}, {d} ms (deadline 600 ms, cap 2); waiters peaked at {d}\n", .{ sorted[0], sorted[1], sorted[2], waiters_max.load(.acquire) });
    // The first two at their own deadline, the third only after a place
    // in the cap freed and its own deadline then ran.
    try std.testing.expect(sorted[0] >= 550 and sorted[1] < 1200);
    try std.testing.expect(sorted[2] >= 1100 and sorted[2] < 2500);
    try std.testing.expect(waiters_max.load(.acquire) <= 2);
    try std.testing.expectEqual(@as(u64, 3), rig.server.proxy_header_failures.load(.acquire));
    // A good header is served once the stalled ones are gone.
    const s = try proxyH2(addr, gpa, .{ .peer = .{ .ip4 = .{ 203, 0, 113, 11 } } }, "/hello");
    defer s.close();
    try readUntil(s, "ok", 3 * std.time.ns_per_s);
}

test "PROXY v2: a header that never finishes is held only to the preface deadline and inside its own cap" {
    try runIn(proxyStalled);
}

// ---------------------------------------------------------------------------
// `.none`, and the boot errors.

fn noneOnUnix(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var path_buf: [64]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/starh2-acct-{d}.sock", .{std.c.getpid()});
    var rig: Rig = undefined;
    try rig.start(rt, gpa, &.{.{ .h2c_unix = .{ .path = path } }}, testLimits(), false);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);
    var w = try h2Requests(gpa, "/park", 1);
    defer w.deinit(gpa);
    const s = try (try zio.net.UnixAddress.init(path)).connect(.{});
    defer s.close();
    try writeAll(s, w.items);
    try waitAtomic("handler started", &started, 1, 3 * std.time.ns_per_s);
    // Charged to the connection and the server, and to no client.
    try std.testing.expectEqual(@as(usize, 1), rig.server.active_connections.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), rig.server.accounting.task_handlers.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), rig.server.clients.stats().live);
    try std.testing.expectEqual(@as(u64, 0), rig.server.clients.stats().inserts);
    try std.testing.expect(sawPeer(.unknown));
    release.store(true, .release);
    try waitAtomic("handler ended", &ended, 1, 3 * std.time.ns_per_s);
}

test "identity: a Unix socket endpoint is .none by default and charges no client" {
    try runIn(noneOnUnix);
}

/// `.none` behind a pooling L7 proxy that sends PROXY v2: the header is read
/// and stripped, the handler sees the header's source, and no client is
/// charged. The trusted check still runs first.
fn noneBehindProxy(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    const ten = [_]proxy.Cidr{try proxy.Cidr.parse("10.0.0.0/8")};
    const l4 = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = .{ .address = l4, .client_identity = .none, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } } },
        .{ .h1c = .{ .address = l4, .client_identity = .none, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } } },
        .{ .h2c_prior_knowledge = .{ .address = l4, .client_identity = .none, .proxy_protocol = .{ .v2 = .{ .trusted = &ten } } } },
    }, testLimits(), false);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);
    const h2_addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(0));
    const h1_addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(1));
    const untrusted_addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(2));
    const src4: [4]u8 = .{ 203, 0, 113, 9 };
    const src6 = ip6("2001:db8:9::9");

    const a = try proxyH2(h2_addr, gpa, .{ .peer = .{ .ip4 = src4 } }, "/park");
    defer a.close();
    const b = try proxyH2(h2_addr, gpa, .{ .peer = .{ .ip6 = src6 } }, "/hello");
    defer b.close();
    try readUntil(b, "ok", 3 * std.time.ns_per_s);
    const c = try proxyH2(h2_addr, gpa, .local, "/hello");
    defer c.close();
    try readUntil(c, "ok", 3 * std.time.ns_per_s);
    {
        var hb: [64]u8 = undefined;
        const s = try connect(h1_addr);
        defer s.close();
        try writeAll(s, proxyHeader(&hb, .{ .peer = .{ .ip4 = .{ 198, 51, 100, 9 } } }));
        try writeAll(s, "GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n");
        try readUntil(s, "ok", 3 * std.time.ns_per_s);
    }
    try waitAtomic("handler started", &started, 1, 3 * std.time.ns_per_s);
    try std.testing.expect(sawPeer(.{ .ip4 = src4 }));
    // Charged to the connections and the server, and to no client: not the
    // header's source, not the proxy, not LOCAL's infrastructure account.
    const st = rig.server.clients.stats();
    try std.testing.expectEqual(@as(usize, 0), st.live);
    try std.testing.expectEqual(@as(u64, 0), st.inserts);
    try std.testing.expectEqual(@as(usize, 1), rig.server.accounting.task_handlers.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), rig.server.proxy_header_failures.load(.acquire));
    // From a source the endpoint does not trust: closed at accept. The close
    // can come before the client's write, which then fails.
    {
        const u = try connect(untrusted_addr);
        defer u.close();
        var hb: [64]u8 = undefined;
        writeAll(u, proxyHeader(&hb, .{ .peer = .{ .ip4 = src4 } })) catch {};
        _ = try readToEof(u, 3 * std.time.ns_per_s);
    }
    try std.testing.expectEqual(@as(u64, 1), rig.server.proxy_untrusted.load(.acquire));
    std.debug.print("accounts none behind PROXY: h2c (v4, v6, LOCAL) and h1c served, 0 inserts, untrusted closed\n", .{});
    release.store(true, .release);
    try waitAtomic("handler ended", &ended, 1, 3 * std.time.ns_per_s);
}

test "identity: .none behind a proxy that sends PROXY v2 strips the header and charges no client" {
    try runIn(noneBehindProxy);
}

test "identity: the boot errors (PROXY with no trusted source, a PROXY source with no PROXY, .peer behind PROXY)" {
    const gpa = std.testing.allocator;
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const boot = struct {
        fn init(ep: starh2.EndpointConfig) starh2.InitError!starh2.Server {
            return starh2.Server.init(std.testing.allocator, std.testing.io, .{ .endpoints = &.{ep}, .routes = &routes, .tls = null });
        }
    };
    // PROXY v2 on TCP from no trusted source: any client could name any
    // address.
    try std.testing.expectError(error.ProxyProtocolUntrusted, boot.init(.{ .h2c_prior_knowledge = .{ .address = addr, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &.{} } } } }));
    try std.testing.expectError(error.ProxyProtocolUntrusted, boot.init(.{ .h2c_prior_knowledge = .{ .address = addr, .client_identity = .none, .proxy_protocol = .{ .v2 = .{ .trusted = &.{} } } } }));
    // The PROXY source with no PROXY header to read it from.
    try std.testing.expectError(error.ProxySourceWithoutProxyProtocol, boot.init(.{ .h1c = .{ .address = addr, .client_identity = .proxy_source } }));
    try std.testing.expectError(error.ProxySourceWithoutProxyProtocol, boot.init(.{ .h2c_unix = .{ .path = "/tmp/starh2-never-bound.sock", .client_identity = .proxy_source } }));
    // `.peer` behind PROXY: every client would be the proxy.
    try std.testing.expectError(error.PeerIdentityBehindProxyProtocol, boot.init(.{ .h2c_prior_knowledge = .{ .address = addr, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } } }));
    // PROXY on a Unix socket takes no list, with either identity.
    for ([_]starh2.edge.server.UnixClientIdentity{ .proxy_source, .none }) |id| {
        var ok = try boot.init(.{ .h2c_unix = .{ .path = "/tmp/starh2-never-bound.sock", .client_identity = id, .proxy_protocol = .v2 } });
        ok.deinit(gpa);
    }
}

// ---------------------------------------------------------------------------
// Orphans stay charged.

fn orphanCharges(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp6("::1", 0) } },
        .{ .h1c = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
    }, testLimits(), false);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);

    var w = try h2Requests(gpa, "/held", 2);
    defer w.deinit(gpa);
    const a = try connect(try zio.net.IpAddress.parseIp6("::1", rig.port(0)));
    try writeAll(a, w.items);
    const b = try connect(try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(1)));
    try writeAll(b, "GET /held HTTP/1.1\r\nHost: localhost\r\n\r\n");
    try waitAtomic("handlers started", &started, 3, 3 * std.time.ns_per_s);

    const k6 = v6Keys(ip6("::1"));
    const k4 = v4Key(.{ 127, 0, 0, 1 });
    const before6 = rig.entry(k6[0]).?.count(.request_bytes);
    const before4 = rig.entry(k4).?.count(.request_bytes);
    a.close();
    b.close();
    try waitAtomic("orphans", &rig.server.accounting.orphaned_handlers, 3, 3 * std.time.ns_per_s);
    try waitAtomic("connections", &rig.server.active_connections, 0, 3 * std.time.ns_per_s);

    // Only the connections' own charges went.
    for (k6) |k| {
        const e = rig.entry(k) orelse return error.OrphanAccountGone;
        try expectCount(e, .connections, 0);
        try expectCount(e, .streams, 0);
        try expectCount(e, .task_handlers, 2);
        try std.testing.expect(e.count(.request_bytes) >= 2 * 64 * 1024);
        try std.testing.expect(e.count(.request_bytes) >= before6);
        try std.testing.expectEqual(@as(u32, 2), e.holders);
    }
    const e4 = rig.entry(k4) orelse return error.OrphanAccountGone;
    try expectCount(e4, .connections, 0);
    try expectCount(e4, .streams, 0);
    try expectCount(e4, .task_handlers, 1);
    try std.testing.expect(e4.count(.request_bytes) >= before4);
    try std.testing.expectEqual(@as(u32, 1), e4.holders);
    try std.testing.expectEqual(@as(usize, 3), rig.server.accounting.task_handlers.load(.acquire));
    std.debug.print("accounts orphans: 3 orphans keep {d} + {d} request bytes on ::1/64 and 127.0.0.1 after their connections ended\n", .{ rig.entry(k6[0]).?.count(.request_bytes), e4.count(.request_bytes) });

    release.store(true, .release);
    try waitAtomic("orphans ended", &rig.server.accounting.orphaned_handlers, 0, 3 * std.time.ns_per_s);
    try std.testing.expectEqual(@as(usize, 0), rig.server.clients.stats().live);
}

test "orphans: a handler that outlives its connection keeps its client, aggregate and server charges until it returns" {
    try runIn(orphanCharges);
}

// ---------------------------------------------------------------------------
// The resources only a held state shows: a handshake in progress, outbound
// bytes waiting for a window, a compression context.

/// Wait until `k`'s record holds at least `want` of `res`; the count then.
fn waitCount(rig: *Rig, k: accounts.Key, res: accounts.Resource, want: usize, timeout_ns: u64) !usize {
    const deadline = nowNs() + timeout_ns;
    while (true) {
        const v = if (rig.entry(k)) |e| e.count(res) else 0;
        if (v >= want) return v;
        if (nowNs() >= deadline) {
            std.debug.print("accounts: {f} {s} is {d}, waited for at least {d}\n", .{ accounts.fmtKey(k), @tagName(res), v, want });
            return error.StateNotReached;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

fn heldCharges(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    var limits = testLimits();
    limits.response_compression = true;
    try rig.start(rt, gpa, &.{
        .{ .tls = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
        .{ .h2c_prior_knowledge = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
    }, limits, true);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);
    const tls_addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(0));
    const h2_addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(1));
    // One IPv6 client per resource, each in its own /48, so each count is
    // that resource's alone, on the client and on the aggregate.
    const hs_src = ip6("2001:db8:a1::1");
    const out_src = ip6("2001:db8:b2::1");
    const br_src = ip6("2001:db8:c3::1");
    var conns: [3]zio.net.Stream = undefined;
    var n: usize = 0;
    defer for (conns[0..n]) |c| c.close();

    // A TLS handshake in progress: the header, then no ClientHello.
    {
        var hb: [64]u8 = undefined;
        const s = try connect(tls_addr);
        conns[n] = s;
        n += 1;
        try writeAll(s, proxyHeader(&hb, .{ .peer = .{ .ip6 = hs_src } }));
    }
    // Outbound bytes past the window.
    conns[n] = try proxyH2(h2_addr, gpa, .{ .peer = .{ .ip6 = out_src } }, "/big");
    n += 1;
    // A brotli context (HTTP/2 only: HTTP/1.1 responses are not compressed).
    {
        var hb: [64]u8 = undefined;
        const extra = [_]starh2.core.hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
        const wire = try h2c.buildClientHelloExtra(gpa, "/brotli", &extra);
        defer gpa.free(wire);
        const s = try connect(h2_addr);
        conns[n] = s;
        n += 1;
        try writeAll(s, proxyHeader(&hb, .{ .peer = .{ .ip6 = br_src } }));
        try writeAll(s, wire);
    }
    const wait = 3 * std.time.ns_per_s;
    for (v6Keys(hs_src)) |k| {
        _ = try waitCount(&rig, k, .handshakes, 1, wait);
        try expectCount(rig.entry(k).?, .handshakes, 1);
    }
    var out_held: [2]usize = undefined;
    for (v6Keys(out_src), &out_held) |k, *held_bytes| held_bytes.* = try waitCount(&rig, k, .outbound_bytes, 1, wait);
    // The writer is parked on the window, so the count is still: the client,
    // its aggregate and the server hold the same bytes (this is the server's
    // only connection with outbound bytes).
    zio.sleep(.fromMilliseconds(50)) catch {};
    const out_server = rig.server.accounting.outbound_bytes.load(.acquire);
    for (v6Keys(out_src), &out_held) |k, *held_bytes| {
        held_bytes.* = rig.entry(k).?.count(.outbound_bytes);
        try std.testing.expectEqual(out_server, held_bytes.*);
    }
    for (v6Keys(br_src)) |k| {
        _ = try waitCount(&rig, k, .compression, 1, wait);
        try expectCount(rig.entry(k).?, .compression, 1);
    }
    try std.testing.expectEqual(@as(usize, 1), rig.server.accounting.active_handshakes.load(.acquire));
    try std.testing.expect(rig.server.accounting.outbound_bytes.load(.acquire) > 0);
    std.debug.print("accounts held: handshake 1 on the /64 and the /48; outbound {d} and {d} bytes (server {d}); compression 1 and 1\n", .{ out_held[0], out_held[1], out_server });

    // Then the release: every count goes back with its record. The client
    // closes first: `/big` may be parked on a write no window will open.
    release.store(true, .release);
    for (conns[0..n]) |c| c.close();
    n = 0;
    try waitAtomic("handlers ended", &ended, 2, wait);
    try waitUntil("table empty", &rig, tableEmpty, wait);
    const st = rig.server.clients.stats();
    try std.testing.expectEqual(@as(usize, 0), st.quarantined);
    try std.testing.expectEqual(@as(u64, 0), st.underflows);
    try std.testing.expectEqual(@as(u64, 0), st.stale);
    try std.testing.expectEqual(@as(usize, 0), rig.server.accounting.active_handshakes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), rig.server.accounting.outbound_bytes.load(.acquire));
}

test "held charges: a handshake, outbound bytes and a compression context are charged to the client and its aggregate, then released" {
    try runIn(heldCharges);
}

// ---------------------------------------------------------------------------
// A full table.

/// Limits for the table tests: 3 connections, 1 handshake, 1 task handler
/// and 1 endpoint (its accept loop), so 2 x 6 = 12 records.
fn smallTableLimits() starh2.Limits {
    var limits = testLimits();
    limits.max_connections = 3;
    limits.concurrent_tls_handshakes = 1;
    limits.max_task_handlers = 1;
    limits.max_endpoints = 1;
    // No rate-only reserve: these tests count the parties' records.
    limits.client_rate_records = 0;
    return limits;
}

fn tableRoom(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
    }, smallTableLimits(), false);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);
    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(0));
    // max_connections IPv6 clients in three /48s: six records.
    const a = try proxyH2(addr, gpa, .{ .peer = .{ .ip6 = ip6("2001:db8:a::1") } }, "/park");
    defer a.close();
    try waitAtomic("handler started", &started, 1, 3 * std.time.ns_per_s);
    const b = try proxyH2(addr, gpa, .{ .peer = .{ .ip6 = ip6("2001:db8:b::1") } }, "/hello");
    defer b.close();
    try readUntil(b, "ok", 3 * std.time.ns_per_s);
    const c = try proxyH2(addr, gpa, .{ .peer = .{ .ip6 = ip6("2001:db8:c::1") } }, "/hello");
    defer c.close();
    readUntil(c, "ok", 3 * std.time.ns_per_s) catch |err| {
        std.debug.print("accounts table room: the third IPv6 client got no answer ({s}); {d} insert refused\n", .{ @errorName(err), rig.server.clients.stats().insert_failures });
        return err;
    };
    const st = rig.server.clients.stats();
    std.debug.print("accounts table room: {d} live of {d}, {d} insert refused\n", .{ st.live, rig.server.clients.records.len, st.insert_failures });
    try std.testing.expectEqual(@as(u64, 0), st.insert_failures);
    try std.testing.expectEqual(@as(usize, 6), st.live);
    release.store(true, .release);
}

test "table: max_connections IPv6 clients in different /48s are never refused at the table" {
    try runIn(tableRoom);
}

fn tableFull(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
    }, smallTableLimits(), false);
    defer rig.stop();
    // On any exit: let parked and shielded handlers return before the stop.
    defer release.store(true, .release);
    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(0));
    const a = try proxyH2(addr, gpa, .{ .peer = .{ .ip6 = ip6("2001:db8:a::1") } }, "/park");
    defer a.close();
    try waitAtomic("handler started", &started, 1, 3 * std.time.ns_per_s);
    // Admitted parties cannot fill the table (`tableRoom`), so the test takes
    // the rest itself: the records a bug left quarantined, in the server's
    // life. All but one, with `a`'s two: the next IPv6 client's /64 goes in
    // and its /48 does not, and `acquire` must give the /64 back.
    const cap = rig.server.clients.records.len;
    var fill: [16]accounts.Charge = undefined;
    const want = cap - 2 - 1;
    if (want > fill.len) return error.TableTooLargeForTheTest;
    var filled: usize = 0;
    defer for (fill[0..filled]) |f| rig.server.clients.release(f);
    while (filled < want) : (filled += 1) {
        fill[filled] = switch (try rig.server.clients.acquire(&.{v4Key(.{ 10, 99, 0, @intCast(filled) })}, 0, null)) {
            .charge => |ch| ch,
            else => return error.TableFullTooEarly,
        };
    }
    try std.testing.expectEqual(cap - 1, rig.server.clients.stats().live);
    const c = try proxyH2(addr, gpa, .{ .peer = .{ .ip6 = ip6("2001:db8:c::1") } }, "/hello");
    defer c.close();
    _ = try readToEof(c, 3 * std.time.ns_per_s);
    const st = rig.server.clients.stats();
    std.debug.print("accounts table full: {d} live of {d}, {d} insert refused\n", .{ st.live, cap, st.insert_failures });
    try std.testing.expectEqual(@as(u64, 1), st.insert_failures);
    try std.testing.expectEqual(cap - 1, st.live);
    try std.testing.expect(rig.entry(v6Keys(ip6("2001:db8:c::1"))[0]) == null);
    try std.testing.expect(rig.entry(v6Keys(ip6("2001:db8:c::1"))[1]) == null);
    release.store(true, .release);
}

test "table: an insert that finds the table full refuses its connection and counts it" {
    try runIn(tableFull);
}

/// An accept loop that waits for admission holds the records of the
/// connection it accepted (`Server.acceptLoop` keys the peer, then parks in
/// `awaitAdmission`): one per endpoint, beyond `max_connections`. Eight
/// plain endpoints, `max_connections` 1, and eight IPv4 clients from
/// 127.0.0.1 to 127.0.0.8: the first is admitted, and seven accept loops
/// park, holding a record each. Linux routes all of 127/8 to the loopback;
/// macOS answers only on 127.0.0.1 unless an alias is configured, so there
/// the test reports a skip, and the unit test of `clientTableRecords` still
/// covers the formula.
fn acceptLoopHolders(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var limits = testLimits();
    limits.max_connections = 1;
    limits.concurrent_tls_handshakes = 1;
    limits.max_task_handlers = 1;
    limits.max_endpoints = 8;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, &.{ .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }, .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }, .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }, .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }, .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }, .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }, .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }, .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } } }, limits, false);
    defer rig.stop();
    var conns: [8]zio.net.Stream = undefined;
    var n: usize = 0;
    defer for (conns[0..n]) |c| c.close();
    for (0..8) |k| {
        const to = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(k));
        const from = zio.net.IpAddress.initIp4(.{ 127, 0, 0, @intCast(k + 1) }, 0);
        conns[n] = to.connect(.{ .local_address = from }) catch |err| {
            if (k > 0 and err == error.AddressUnavailable) {
                std.debug.print("accounts accept-loop holders: cannot bind 127.0.0.{d} here ({s}); skipped\n", .{ k + 1, @errorName(err) });
                return error.SkipZigTest;
            }
            return err;
        };
        n += 1;
    }
    // Eight clients, one record each, all held: one admitted connection and
    // seven parked accept loops.
    const Live = struct {
        fn eight(r: *Rig) bool {
            const st = r.server.clients.stats();
            return st.live + st.insert_failures >= 8;
        }
    };
    try waitUntil("eight clients keyed or refused", &rig, Live.eight, 3 * std.time.ns_per_s);
    const st = rig.server.clients.stats();
    std.debug.print("accounts accept-loop holders: {d} live of {d}, {d} insert refused\n", .{ st.live, rig.server.clients.records.len, st.insert_failures });
    try std.testing.expectEqual(@as(u64, 0), st.insert_failures);
    try std.testing.expectEqual(@as(usize, 8), st.live);
    for (conns[0..n]) |c| c.close();
    n = 0;
    try waitUntil("table empty", &rig, tableEmpty, 5 * std.time.ns_per_s);
}

test "table: accept loops parked with a client's records are inside the capacity (8 endpoints, 8 IPv4 clients)" {
    try runIn(acceptLoopHolders);
}

// ---------------------------------------------------------------------------
// The soak.

const Transport = enum { h2c, tls, h1c, proxied };

const SoakStats = struct {
    /// The client's own failures by error name: the server closing a
    /// connection under a write (a GOAWAY, a reset, the shutdown) is
    /// expected; anything else would show here.
    error_names: [8][]const u8 = @splat(""),
    error_counts: [8]std.atomic.Value(usize) = @splat(.init(0)),
    error_mu: zio.Mutex = .init,
    connections: std.atomic.Value(usize) = .init(0),
    resets: std.atomic.Value(usize) = .init(0),
    closes: std.atomic.Value(usize) = .init(0),
    goaways: std.atomic.Value(usize) = .init(0),
    stream_resets: std.atomic.Value(usize) = .init(0),
    errors: std.atomic.Value(usize) = .init(0),

    fn noteError(self: *SoakStats, err: anyerror) void {
        _ = self.errors.fetchAdd(1, .monotonic);
        self.error_mu.lockUncancelable();
        defer self.error_mu.unlock();
        const name = @errorName(err);
        for (&self.error_names, &self.error_counts) |*n, *c| {
            if (n.len == 0) n.* = name;
            if (std.mem.eql(u8, n.*, name)) {
                _ = c.fetchAdd(1, .monotonic);
                return;
            }
        }
    }

    fn printErrors(self: *SoakStats) void {
        for (self.error_names, &self.error_counts) |n, *c| {
            if (n.len != 0) std.debug.print("accounts soak: client error {s} x{d}\n", .{ n, c.load(.acquire) });
        }
    }
};

const SoakCtx = struct {
    gpa: std.mem.Allocator,
    ports: [4]u16,
    stats: *SoakStats,
    seed: u64,
    lifetimes: usize,
};

const proxy_sources = [_]Peer{
    .{ .ip4 = .{ 203, 0, 113, 20 } },
    .{ .ip4 = .{ 203, 0, 113, 21 } },
    .{ .ip6 = .{ 0x20, 0x01, 0x0d, 0xb8, 0, 0x10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1 } },
    .{ .ip6 = .{ 0x20, 0x01, 0x0d, 0xb8, 0, 0x10, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1 } },
    .{ .ip6 = .{ 0x20, 0x01, 0x0d, 0xb8, 0, 0x20, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1 } },
};

fn soakPath(buf: []u8, r: std.Random) []const u8 {
    const hold = r.uintLessThan(u64, 120);
    return switch (r.uintLessThan(u8, 3)) {
        0 => std.fmt.bufPrint(buf, "/honour?ms={d}", .{hold}) catch unreachable,
        1 => std.fmt.bufPrint(buf, "/ignore?ms={d}", .{hold}) catch unreachable,
        else => "/hello",
    };
}

/// One h2c, h1c or proxied h2c worker on a zio task.
fn soakPlain(ctx: *SoakCtx, transport: Transport) void {
    var prng = std.Random.DefaultPrng.init(ctx.seed);
    const r = prng.random();
    for (0..ctx.lifetimes) |_| soakOne(ctx, transport, r) catch |err| ctx.stats.noteError(err);
}

fn soakOne(ctx: *SoakCtx, transport: Transport, r: std.Random) !void {
    const gpa = ctx.gpa;
    const port = ctx.ports[@intFromEnum(transport)];
    const s = try connect(try zio.net.IpAddress.parseIp4("127.0.0.1", port));
    _ = ctx.stats.connections.fetchAdd(1, .monotonic);
    var closed = false;
    defer if (!closed) s.close();
    var pb: [64]u8 = undefined;
    if (transport == .h1c) {
        var line: [128]u8 = undefined;
        const req = try std.fmt.bufPrint(&line, "GET {s} HTTP/1.1\r\nHost: localhost\r\n\r\n", .{soakPath(&pb, r)});
        try writeAll(s, req);
    } else {
        if (transport == .proxied) {
            var hb: [64]u8 = undefined;
            try writeAll(s, proxyHeader(&hb, .{ .peer = proxy_sources[r.uintLessThan(usize, proxy_sources.len)] }));
        }
        var w = try h2c.buildClientPrefaceAndSettings(gpa);
        defer w.deinit(gpa);
        const n = 1 + r.uintLessThan(u31, 4);
        var sid: u31 = 1;
        for (0..n) |_| {
            try h2c.appendHeaders(gpa, &w, sid, soakPath(&pb, r), true);
            sid += 2;
        }
        try writeAll(s, w.items);
    }
    zio.sleep(.fromMilliseconds(r.uintLessThan(u64, 80))) catch {};
    switch (r.uintLessThan(u8, 5)) {
        0 => {
            resetClose(s);
            closed = true;
            _ = ctx.stats.resets.fetchAdd(1, .monotonic);
        },
        1 => if (transport != .h1c) {
            // A frame on stream 0 that must not be there: the server ends
            // the connection with GOAWAY PROTOCOL_ERROR.
            var w: std.ArrayList(u8) = .empty;
            defer w.deinit(gpa);
            try h2c.appendData(gpa, &w, 0, "x", false);
            try writeAll(s, w.items);
            _ = readToEof(s, 2 * std.time.ns_per_s) catch {};
            _ = ctx.stats.goaways.fetchAdd(1, .monotonic);
        },
        2 => if (transport != .h1c) {
            var w: std.ArrayList(u8) = .empty;
            defer w.deinit(gpa);
            try h2c.appendRst(gpa, &w, 1);
            try writeAll(s, w.items);
            _ = ctx.stats.stream_resets.fetchAdd(1, .monotonic);
            zio.sleep(.fromMilliseconds(r.uintLessThan(u64, 40))) catch {};
        },
        else => {
            _ = ctx.stats.closes.fetchAdd(1, .monotonic);
        },
    }
}

/// A TLS worker: blocking `Wire`, on a `spawnBlocking` thread.
fn soakTls(ctx: *SoakCtx) void {
    var prng = std.Random.DefaultPrng.init(ctx.seed);
    const r = prng.random();
    for (0..ctx.lifetimes) |_| soakTlsOne(ctx, r) catch |err| ctx.stats.noteError(err);
}

fn soakTlsOne(ctx: *SoakCtx, r: std.Random) !void {
    const gpa = ctx.gpa;
    const w = try wire_client.Wire.connect(gpa, .tls, ctx.ports[@intFromEnum(Transport.tls)]);
    defer w.close();
    _ = ctx.stats.connections.fetchAdd(1, .monotonic);
    try w.handshake();
    var pb: [64]u8 = undefined;
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    const n = 1 + r.uintLessThan(u31, 3);
    var sid: u31 = 1;
    for (0..n) |_| {
        try h2c.appendHeaders(gpa, &req, sid, soakPath(&pb, r), true);
        sid += 2;
    }
    try w.sendUnchecked(req.items);
    starh2.test_sleep.threadSleepMs(r.uintLessThan(u64, 80));
    if (r.boolean()) {
        const lg: std.c.linger = .{ .onoff = 1, .linger = 0 };
        _ = std.c.setsockopt(w.fd, std.c.SOL.SOCKET, std.c.SO.LINGER, std.mem.asBytes(&lg), @sizeOf(std.c.linger));
        _ = ctx.stats.resets.fetchAdd(1, .monotonic);
    } else {
        _ = ctx.stats.closes.fetchAdd(1, .monotonic);
    }
}

fn quiet(rig: *Rig) bool {
    const s = &rig.server;
    return s.active_connections.load(.acquire) == 0 and
        s.accounting.task_handlers.load(.acquire) == 0 and
        s.clients.stats().live == 0;
}

fn soakRound(rt: *zio.Runtime, ctx: *SoakCtx, workers: usize) !void {
    _ = rt;
    var plain: [16]zio.JoinHandle(void) = undefined;
    var tlsh: [4]zio.JoinHandle(void) = undefined;
    var ctxs: [20]SoakCtx = undefined;
    var np: usize = 0;
    var nt: usize = 0;
    for (0..workers) |i| {
        ctxs[i] = ctx.*;
        ctxs[i].seed = ctx.seed *% 31 +% i;
        const t: Transport = @enumFromInt(i % 4);
        if (t == .tls) {
            tlsh[nt] = try zio.spawnBlocking(soakTls, .{&ctxs[i]});
            nt += 1;
        } else {
            plain[np] = try zio.spawn(soakPlain, .{ &ctxs[i], t });
            np += 1;
        }
    }
    for (plain[0..np]) |*h| h.join();
    for (tlsh[0..nt]) |*h| h.join();
}

fn soak(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    var limits = testLimits();
    limits.graceful_drain_timeout_ns = 100 * ms;
    // Buckets that refill at once, so no record stays for its rate state
    // and every key leaves the table between rounds: the same key is removed
    // and inserted again. `soakRefusals` keeps them.
    limits.connection_start_refill_ns = 1;
    limits.handshake_start_refill_ns = 1;
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .{ .tls = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .{ .h1c = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .{ .h2c_prior_knowledge = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
    }, limits, true);
    var stopped = false;
    defer if (!stopped) rig.stop();
    var stats: SoakStats = .{};
    const seed: u64 = @truncate(nowNs());
    std.debug.print("accounts soak: seed {d}\n", .{seed});
    var ctx: SoakCtx = .{ .gpa = gpa, .ports = .{ rig.port(0), rig.port(1), rig.port(2), rig.port(3) }, .stats = &stats, .seed = seed, .lifetimes = 6 };
    const rounds = 5;
    var inserts_by_round: [rounds]u64 = undefined;
    for (0..rounds) |round| {
        ctx.seed = seed +% round;
        try soakRound(rt, &ctx, 12);
        // Quiet between rounds: every key leaves the table, so the next
        // round inserts the same keys again.
        try waitUntil("quiet after a round", &rig, quiet, 10 * std.time.ns_per_s);
        inserts_by_round[round] = rig.server.clients.stats().inserts;
        if (rig.entry(v4Key(.{ 127, 0, 0, 1 })) != null) return error.KeyNotRemoved;
    }
    // Before the shutdown round, every client failure would be the
    // server's doing: there should be none.
    const errors_before_shutdown = stats.errors.load(.acquire);
    std.debug.print("accounts soak: {d} client errors before the shutdown round\n", .{errors_before_shutdown});
    stats.printErrors();
    try std.testing.expectEqual(@as(usize, 0), errors_before_shutdown);
    // The last round ends in a shutdown with traffic in flight.
    ctx.seed = seed +% 99;
    var last = try zio.spawn(soakRound, .{ rt, &ctx, 12 });
    zio.sleep(.fromMilliseconds(60)) catch {};
    rig.server.requestShutdown();
    serve_deadline.join(&rig.server, &rig.handle);
    last.join() catch {};
    const st = rig.server.clients.stats();
    std.debug.print("accounts soak: {d} connections, {d} resets, {d} closes, {d} GOAWAYs, {d} stream resets, {d} client errors; table: {d} inserts ({d} reuses), {d} removals, {d} live, {d} quarantined, {d} underflows, {d} stale, {d} insert failures\n", .{
        stats.connections.load(.acquire), stats.resets.load(.acquire),        stats.closes.load(.acquire), stats.goaways.load(.acquire),
        stats.stream_resets.load(.acquire), stats.errors.load(.acquire),     st.inserts,                  st.reuses,
        st.removals,                       st.live,                           st.quarantined,              st.underflows,
        st.stale,                          st.insert_failures,
    });
    stats.printErrors();
    // The soak did the work.
    try std.testing.expect(stats.connections.load(.acquire) > 300);
    try std.testing.expect(stats.resets.load(.acquire) > 0);
    try std.testing.expect(stats.goaways.load(.acquire) > 0);
    try std.testing.expect(stats.stream_resets.load(.acquire) > 0);
    // Same-key delete and reinsert: 127.0.0.1 went in again every round.
    for (1..rounds) |i| try std.testing.expect(inserts_by_round[i] > inserts_by_round[i - 1]);
    try std.testing.expect(st.reuses > 0);
    // Zero at the end.
    try std.testing.expectEqual(@as(usize, 0), st.live);
    try std.testing.expectEqual(@as(usize, 0), st.quarantined);
    try std.testing.expectEqual(st.inserts, st.removals);
    try std.testing.expectEqual(@as(u64, 0), st.underflows);
    try std.testing.expectEqual(@as(u64, 0), st.stale);
    stopped = true;
    rig.server.deinit(gpa);
    gpa.free(rig.cert);
    gpa.free(rig.key);
}

test "soak: h2c, TLS, HTTP/1.1 and PROXY churn with resets, GOAWAYs and shutdown leaves every account at zero" {
    try runIn(soak);
}

/// The soak again, with limits tight enough that the shares and the start
/// buckets refuse all the time (t-3759 step 4): one loopback client is over
/// its share of connections, streams and task handlers under pressure, and
/// its connection-start bucket runs dry. A refusal is a client error here,
/// so none is counted against the server; what must hold is conservation:
/// nothing live or quarantined, no underflow, no stale reference, and every
/// insert either removed or kept as a rate-only record. The instrument must
/// fire: the run fails if any of those refusals did not happen.
fn soakRefusals(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var rig: Rig = undefined;
    var limits = testLimits();
    limits.graceful_drain_timeout_ns = 100 * ms;
    // Share 2, mark 4.
    limits.max_connections = 16;
    // Share 2, mark 3.
    limits.max_streams_per_server = 24;
    limits.max_task_handlers = 24;
    limits.concurrent_tls_handshakes = 8;
    limits.connection_start_burst = 6;
    limits.connection_start_refill_ns = 40 * ms;
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .{ .tls = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .{ .h1c = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } },
        .{ .h2c_prior_knowledge = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &trusted_loopback) },
    }, limits, true);
    var stopped = false;
    defer if (!stopped) rig.stop();
    var stats: SoakStats = .{};
    const seed: u64 = @truncate(nowNs());
    std.debug.print("accounts refusal soak: seed {d}\n", .{seed});
    var ctx: SoakCtx = .{ .gpa = gpa, .ports = .{ rig.port(0), rig.port(1), rig.port(2), rig.port(3) }, .stats = &stats, .seed = seed, .lifetimes = 6 };
    for (0..3) |round| {
        ctx.seed = seed +% round;
        try soakRound(rt, &ctx, 16);
        try waitUntil("quiet after a round", &rig, quiet, 10 * std.time.ns_per_s);
    }
    ctx.seed = seed +% 99;
    var last = try zio.spawn(soakRound, .{ rt, &ctx, 16 });
    zio.sleep(.fromMilliseconds(60)) catch {};
    rig.server.requestShutdown();
    serve_deadline.join(&rig.server, &rig.handle);
    last.join() catch {};
    const st = rig.server.clients.stats();
    const t = &rig.server.clients;
    std.debug.print("accounts refusal soak: {d} connections, {d} client errors; refused by share: {d} connections, {d} handshakes, {d} streams, {d} task handlers; by bucket: {d} connection starts, {d} handshake starts; table: {d} inserts, {d} removals, {d} rate-only, {d} evictions, {d} live, {d} quarantined, {d} underflows, {d} stale\n", .{
        stats.connections.load(.acquire),  stats.errors.load(.acquire),
        t.shareRefusals(.connections),     t.shareRefusals(.handshakes),
        t.shareRefusals(.streams),         t.shareRefusals(.task_handlers),
        st.rate_refusals[0],               st.rate_refusals[1],
        st.inserts,                        st.removals,
        st.rate_only,                      st.evictions,
        st.live,                           st.quarantined,
        st.underflows,                     st.stale,
    });
    stats.printErrors();
    // The instrument fired: every refusal kind this soak is built to drive.
    try std.testing.expect(stats.connections.load(.acquire) > 300);
    try std.testing.expect(t.shareRefusals(.connections) > 0);
    try std.testing.expect(t.shareRefusals(.streams) + t.shareRefusals(.task_handlers) > 0);
    try std.testing.expect(st.rate_refusals[0] > 0);
    // Conservation.
    try std.testing.expectEqual(@as(usize, 0), st.live);
    try std.testing.expectEqual(@as(usize, 0), st.quarantined);
    try std.testing.expectEqual(st.inserts, st.removals + st.rate_only);
    try std.testing.expectEqual(@as(u64, 0), st.underflows);
    try std.testing.expectEqual(@as(u64, 0), st.stale);
    stopped = true;
    rig.server.deinit(gpa);
    gpa.free(rig.cert);
    gpa.free(rig.key);
}

test "soak: the same churn under shares and start buckets that refuse all the time leaves every account at zero" {
    try runIn(soakRefusals);
}

// ---------------------------------------------------------------------------
// Two accept loops racing for one slot.
//
// An accept loop checks for room before `accept` and takes the slots after
// it (`Server.admitAccepted`). With two endpoints and one free slot, both
// loops can pass the check, accept, and then one finds the slot gone: it
// undoes the charge it took (its client's, then the connection slot) and
// parks holding its stream. Only a race reaches that undo, so each case runs
// cycles until the server's own count (`test_admit_refused`) says it ran,
// and fails if it never did. A leaked undo leaves the loser's client charged,
// which the table reports at the end. The last h2c cycle stops the server
// while the loser is still parked holding its stream, which is the accept
// loop's cancel path (its references go back there).

const RaceKind = enum { connection, handshake, proxy_waiter };

/// A blocking TLS client for the handshake race: connects, handshakes once
/// the server admits it, asks /hello, waits for any answer.
const TlsHello = struct {
    gpa: std.mem.Allocator,
    port: u16,
    /// Connect from `::1` (an endpoint bound there) instead of 127.0.0.1.
    ip6: bool = false,
    ok: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),

    fn run(self: *TlsHello) void {
        self.body() catch {};
        self.done.store(true, .release);
    }

    fn body(self: *TlsHello) !void {
        const w = if (self.ip6) try wire_client.Wire.connectIp6(self.gpa, .tls, self.port) else try wire_client.Wire.connect(self.gpa, .tls, self.port);
        defer w.close();
        // The kernel completed the connect; the server reads the handshake
        // only once it admits the connection, which can be after `Wire`'s
        // 1 s receive timeout.
        const tv: std.c.timeval = .{ .sec = 5, .usec = 0 };
        _ = std.c.setsockopt(w.fd, std.c.SOL.SOCKET, std.c.SO.RCVTIMEO, std.mem.asBytes(&tv), @sizeOf(std.c.timeval));
        try w.handshake();
        var req = try h2Requests(self.gpa, "/hello", 1);
        defer req.deinit(self.gpa);
        try w.sendUnchecked(req.items);
        while (true) {
            const f = (try w.next(nowNs() + 3 * std.time.ns_per_s)) orelse return error.NoResponse;
            if (f.sid == 1 and f.ftype == 1) break;
        }
        self.ok.store(true, .release);
    }
};

const Answer = struct {
    s: zio.net.Stream,
    err: ?anyerror = null,

    fn run(self: *Answer) void {
        readUntil(self.s, "ok", 3 * std.time.ns_per_s) catch |e| {
            self.err = e;
        };
        self.s.close();
    }
};

/// The two contenders come from two clients, 127.0.0.1 on endpoint 0 and
/// ::1 on endpoint 1: one client would be over its share of the one slot
/// under pressure (`accounts.Charge.add`), and its loser closed instead of
/// losing the server's slot, which is the race these tests are about. PROXY
/// endpoints name their sources in the header, so they stay on 127.0.0.1.
fn raceAddrs(kind: RaceKind, ports: [2]u16) ![2]zio.net.IpAddress {
    if (kind == .proxy_waiter) return .{ try zio.net.IpAddress.parseIp4("127.0.0.1", ports[0]), try zio.net.IpAddress.parseIp4("127.0.0.1", ports[1]) };
    return .{ try zio.net.IpAddress.parseIp4("127.0.0.1", ports[0]), try zio.net.IpAddress.parseIp6("::1", ports[1]) };
}

fn raceCycle(kind: RaceKind, gpa: std.mem.Allocator, ports: [2]u16, server: *starh2.Server) !void {
    const addrs = try raceAddrs(kind, ports);
    const a0 = addrs[0];
    const a1 = addrs[1];
    var hb: [64]u8 = undefined;
    var floors: [2]zio.net.Stream = undefined;
    var floors_n: usize = 0;
    defer for (floors[0..floors_n]) |f| f.close();
    // A PROXY waiter slot is taken at accept. Each PROXY endpoint has a floor
    // of one and one slot is shared (`Server.proxy_floor`): stalled headers
    // hold both floors first, so X, the next on endpoint 0, holds the shared
    // one, which Y and Z then race for.
    if (kind == .proxy_waiter) {
        for ([_]zio.net.IpAddress{ a0, a1 }) |addr| {
            const f = try connect(addr);
            floors[floors_n] = f;
            floors_n += 1;
            try writeAll(f, proxy.signature[0..6]);
        }
        try waitUntil("both floors held", server, struct {
            fn f(srv: *starh2.Server) bool {
                return srv.proxyWaiters() == 2;
            }
        }.f, 3 * std.time.ns_per_s);
    }
    // X takes the one slot.
    const x = try connect(a0);
    switch (kind) {
        .connection => {
            var w = try h2Requests(gpa, "/park", 1);
            defer w.deinit(gpa);
            try writeAll(x, w.items);
            try waitAtomic("x parked", &started, 1, 3 * std.time.ns_per_s);
        },
        // A TLS handshake that never gets a ClientHello holds the slot.
        .handshake => {},
        // A header that never finishes holds the waiter slot.
        .proxy_waiter => try writeAll(x, proxy.signature[0..6]),
    }
    zio.sleep(.fromMilliseconds(20)) catch {};
    if (kind == .handshake) {
        var y: TlsHello = .{ .gpa = gpa, .port = ports[0] };
        var z: TlsHello = .{ .gpa = gpa, .port = ports[1], .ip6 = true };
        var hy = try zio.spawnBlocking(TlsHello.run, .{&y});
        var hz = try zio.spawnBlocking(TlsHello.run, .{&z});
        zio.sleep(.fromMilliseconds(30)) catch {};
        x.close();
        hy.join();
        hz.join();
        if (!y.ok.load(.acquire) or !z.ok.load(.acquire)) return error.RaceClientNotServed;
        return;
    }
    // Y and Z wait in the two listen queues; both loops are parked.
    const y = try connect(a0);
    const z = try connect(a1);
    var w = try h2Requests(gpa, "/hello", 1);
    defer w.deinit(gpa);
    for ([_]zio.net.Stream{ y, z }) |s| {
        if (kind == .proxy_waiter) try writeAll(s, proxyHeader(&hb, .{ .peer = .{ .ip4 = .{ 203, 0, 113, 30 } } }));
        try writeAll(s, w.items);
    }
    zio.sleep(.fromMilliseconds(10)) catch {};
    release.store(true, .release);
    x.close();
    // Each is served once the other's connection ends: each closes as soon
    // as its answer arrives.
    var ay: Answer = .{ .s = y };
    var az: Answer = .{ .s = z };
    var hy = try zio.spawn(Answer.run, .{&ay});
    var hz = try zio.spawn(Answer.run, .{&az});
    hy.join();
    hz.join();
    if (ay.err) |e| return e;
    if (az.err) |e| return e;
    // Only the connection race's X ran a handler.
    if (kind == .connection) try waitAtomic("x ended", &ended, 1, 3 * std.time.ns_per_s);
    started.store(0, .release);
    ended.store(0, .release);
    release.store(false, .release);
}

fn admissionRace(rt: *zio.Runtime, gpa: std.mem.Allocator, kind: RaceKind) !void {
    if (!@import("starh2").edge.connection.test_observe) return error.SkipZigTest;
    resetRoutes();
    var rig: Rig = undefined;
    var limits = testLimits();
    limits.preface_timeout_ns = 3 * std.time.ns_per_s;
    // The PROXY header deadline may not be later (`Limits`).
    limits.proxy_header_timeout_ns = limits.preface_timeout_ns;
    const ep4 = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const ep6 = try starh2.EndpointAddress.parseIp6("::1", 0);
    switch (kind) {
        .connection => {
            limits.max_connections = 1;
            try rig.start(rt, gpa, &.{ .{ .h2c_prior_knowledge = .{ .address = ep4 } }, .{ .h2c_prior_knowledge = .{ .address = ep6 } } }, limits, false);
        },
        .handshake => {
            limits.concurrent_tls_handshakes = 1;
            try rig.start(rt, gpa, &.{ .{ .tls = .{ .address = ep4 } }, .{ .tls = .{ .address = ep6 } } }, limits, true);
        },
        .proxy_waiter => {
            // Two PROXY endpoints: a floor of one each, one shared.
            limits.proxy_header_waiters = 3;
            try rig.start(rt, gpa, &.{ .{ .h2c_prior_knowledge = ppTcp(ep4, &trusted_loopback) }, .{ .h2c_prior_knowledge = ppTcp(ep4, &trusted_loopback) } }, limits, false);
        },
    }
    var stopped = false;
    defer if (!stopped) rig.stop();
    defer release.store(true, .release);
    const ports: [2]u16 = .{ rig.port(0), rig.port(1) };
    const before = server_mod.test_admit_refused.load(.acquire);
    var cycles: usize = 0;
    while (cycles < 60 and server_mod.test_admit_refused.load(.acquire) - before < 3) : (cycles += 1) {
        try raceCycle(kind, gpa, ports, &rig.server);
    }
    const refused = server_mod.test_admit_refused.load(.acquire) - before;
    std.debug.print("accounts race {s}: {d} admissions refused and undone in {d} cycles\n", .{ @tagName(kind), refused, cycles });
    if (refused == 0) return error.RaceNeverReached;
    try waitUntil("table empty", &rig, tableEmpty, 3 * std.time.ns_per_s);
    if (kind != .connection) return;
    // The cancel path: stop while a loser is parked holding its stream.
    release.store(false, .release);
    started.store(0, .release);
    const addrs = try raceAddrs(kind, ports);
    const a0 = addrs[0];
    const a1 = addrs[1];
    var parked_losers: usize = 0;
    var tries: usize = 0;
    while (parked_losers == 0 and tries < 60) : (tries += 1) {
        const x = try connect(a0);
        var w = try h2Requests(gpa, "/park", 1);
        defer w.deinit(gpa);
        try writeAll(x, w.items);
        try waitAtomic("x parked", &started, 1, 3 * std.time.ns_per_s);
        const y = try connect(a0);
        const z = try connect(a1);
        // Each asks /park too, so the winner holds the slot.
        try writeAll(y, w.items);
        try writeAll(z, w.items);
        zio.sleep(.fromMilliseconds(10)) catch {};
        const r0 = server_mod.test_admit_refused.load(.acquire);
        x.close();
        // The winner is parked in its handler; the loser waits in its
        // accept loop with its stream.
        try waitAtomic("winner parked", &started, 2, 3 * std.time.ns_per_s);
        zio.sleep(.fromMilliseconds(20)) catch {};
        if (server_mod.test_admit_refused.load(.acquire) != r0) {
            parked_losers += 1;
            std.debug.print("accounts race: stopping with a loser parked in its accept loop\n", .{});
            stopped = true;
            rig.stop();
            y.close();
            z.close();
            return;
        }
        // No race this time: drain and try again.
        release.store(true, .release);
        y.close();
        z.close();
        try waitUntil("table empty", &rig, tableEmpty, 3 * std.time.ns_per_s);
        release.store(false, .release);
        started.store(0, .release);
    }
    return error.RaceNeverReached;
}

fn raceConnection(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return admissionRace(rt, gpa, .connection);
}
fn raceHandshake(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return admissionRace(rt, gpa, .handshake);
}
fn raceWaiter(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return admissionRace(rt, gpa, .proxy_waiter);
}

test "admission race: a connection slot lost to another accept loop undoes its client charge, and a stop finds the loser's references" {
    try runIn(raceConnection);
}

test "admission race: a handshake slot lost to another accept loop undoes the handshake and the connection charges" {
    try runIn(raceHandshake);
}

test "admission race: a PROXY waiter slot lost to another accept loop undoes the connection charge" {
    try runIn(raceWaiter);
}

test "identity: a trusted IPv6 range that holds every IPv4-mapped address is a boot error" {
    const wide = struct {
        fn boot(text: []const u8) ![]const u8 {
            const list = [_]proxy.Cidr{try proxy.Cidr.parse(text)};
            const ep: starh2.EndpointConfig = .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &list } } } };
            if (starh2.Server.init(std.testing.allocator, std.testing.io, .{ .endpoints = &.{ep}, .routes = &routes, .tls = null })) |srv| {
                var s = srv;
                s.deinit(std.testing.allocator);
                return "booted";
            } else |err| return @errorName(err);
        }
    };
    // Ranges that hold ::ffff:0:0/96 without being a mapped range: refused.
    try std.testing.expectEqualStrings("ProxyTrustedRangeHoldsIpv4", try wide.boot("::/0"));
    try std.testing.expectEqualStrings("ProxyTrustedRangeHoldsIpv4", try wide.boot("::ffff:0.0.0.0/95"));
    try std.testing.expectEqualStrings("ProxyTrustedRangeHoldsIpv4", try wide.boot("::/80"));
    // An IPv6 range that does not hold it, the mapped block itself, a mapped
    // range and the IPv4 range: accepted.
    try std.testing.expectEqualStrings("booted", try wide.boot("2001:db8::/32"));
    try std.testing.expectEqualStrings("booted", try wide.boot("::ffff:0.0.0.0/96"));
    try std.testing.expectEqualStrings("booted", try wide.boot("::ffff:10.0.0.0/104"));
    try std.testing.expectEqualStrings("booted", try wide.boot("10.0.0.0/8"));
}

/// A trusted range written as a v4-mapped IPv6 literal (not parsed text)
/// is normalised in the server's copy, so it trusts the IPv4 source it names.
fn mappedLiteralTrusted(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetRoutes();
    var m: [16]u8 = @splat(0);
    m[10] = 0xff;
    m[11] = 0xff;
    m[12] = 127;
    m[15] = 1;
    const lit = [_]proxy.Cidr{.{ .addr = .{ .ip6 = m }, .prefix_bits = 128 }};
    var rig: Rig = undefined;
    try rig.start(rt, gpa, &.{
        .{ .h2c_prior_knowledge = ppTcp(try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), &lit) },
    }, testLimits(), false);
    defer rig.stop();
    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", rig.port(0));
    const s = try proxyH2(addr, gpa, .{ .peer = .{ .ip4 = .{ 203, 0, 113, 40 } } }, "/hello");
    defer s.close();
    try readUntil(s, "ok", 3 * std.time.ns_per_s);
    try std.testing.expectEqual(@as(u64, 0), rig.server.proxy_untrusted.load(.acquire));
}

test "PROXY v2: a trusted range written as a v4-mapped IPv6 literal trusts that IPv4 source" {
    try runIn(mappedLiteralTrusted);
}
