//! `Server.stats()` (t-3759 step 9b), through a real server.
//!
//! The three counters that step added, each moved by its own event:
//! - `stream_bound_refusals`: a request when the server holds
//!   `max_streams_per_server` (HTTP/1.1 answers 503);
//! - `task_bound_refusals`: a task request when the server holds
//!   `max_task_handlers` (503);
//! - `connection_bound_waits`: the server reaching `max_connections` (the
//!   accept loop waits for room before it accepts; a client that connects
//!   meanwhile waits in the kernel's queue and is served once the first
//!   connection ends).
//!
//! And two counters that existed before, read through `stats()`: a
//! client-share refusal (a third connection from 127.0.0.1 under pressure)
//! and a route-share refusal (a third task request on a route over its
//! share). Each test checks the exact delta it caused, and a gauge while the
//! resource is held.
//!
//! The stream and task bounds are tested on HTTP/1.1 and HTTP/2 (h2c): the
//! counters sit in the admission functions both protocols call. A last test
//! makes a PROXY v2 TLS connection, which already holds a connection slot,
//! wait for a handshake slot while the server is at `max_connections`: only
//! the accept loop's wait counts, not that one.
//!
//! Clients are zio streams on the test's own tasks: a held request is a
//! `/hold` whose handler waits for `release`.
//!
//! `close_reasons` (t-3900 item 5): every connection that ends is counted
//! once, at its teardown, with its first reason. One connection ended by
//! each protocol's own path, and the one reason it should count:
//! - an HTTP/2 connection idle past `connection_idle_timeout_ns` (the edge's
//!   `noteServerClose`): `h2_idle`;
//! - an HTTP/2 SETTINGS flood past the session's burst (the session's
//!   GOAWAY): `h2_goaway_enhance_your_calm`;
//! - an HTTP/1.1 request over `request_body_bytes` (`writeError`): `h1_413`.
//! Each test also checks that no other reason moved: a connection counts
//! once, at its first reason, whatever fails after it. A last test ends six
//! connections on one server in six ways, the peer's included (round 2),
//! and checks the table moved by six, one in each expected reason. It can
//! run a mutant armed at run time (`STARH2_CLOSE_MUTANT`, see `mixedEnds`).
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const serve_deadline = @import("serve_deadline.zig");
const route_share = @import("route_share.zig");
const h2c = @import("starh2_h2_client");

const accounts = starh2.edge.accounts;
const proxy = starh2.edge.proxy_protocol;
const hpack = starh2.core.hpack;
const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

var release: std.atomic.Value(bool) = .init(false);
var started: std.atomic.Value(usize) = .init(0);
/// The orphan tests: `hold` then ignores the cancel its connection's end
/// asks for, so it outlives the connection (an orphan). Each canceled sleep
/// is swallowed and the next one waits in full, so it does not spin.
var ignore_cancel: std.atomic.Value(bool) = .init(false);

fn hold(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = started.fetchAdd(1, .acq_rel);
    while (!release.load(.acquire)) {
        if (ignore_cancel.load(.acquire)) {
            zio.sleep(.fromMilliseconds(2)) catch {};
        } else try zio.sleep(.fromMilliseconds(2));
    }
    try resp.send(200, &.{}, "held");
}

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/hold", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hold } } },
    .{ .method = .GET, .path = "/hold2", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hold } } },
    .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
};

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

const Rig = struct {
    gpa: std.mem.Allocator,
    server: starh2.Server,
    handle: zio.JoinHandle(starh2.ServeError!void),
    routes: [routes.len]starh2.Route = routes,
    cert: []u8 = &.{},
    key: []u8 = &.{},

    /// `whole_server`: every task route may hold the whole server, so no
    /// route share refuses before a server bound (`tests/route_share.zig`).
    fn start(self: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator, limits: starh2.Limits, ep: Ep, whole_server: bool) !void {
        release.store(false, .release);
        started.store(0, .release);
        self.* = .{ .gpa = gpa, .server = undefined, .handle = undefined };
        if (whole_server) self.routes = route_share.wholeServer(routes.len, routes, limits);
        const a = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
        var tls_cfg: ?starh2.TlsConfig = null;
        if (ep == .tls_proxy) {
            const io = rt.io();
            self.cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            self.key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            tls_cfg = .{ .certificate_chain_pem = self.cert, .private_key_pem = self.key };
        }
        const endpoint: starh2.EndpointConfig = switch (ep) {
            .h1c_none => .{ .h1c = .{ .address = a, .client_identity = .none } },
            .h1c_peer => .{ .h1c = .{ .address = a, .client_identity = .peer } },
            .h2c => .{ .h2c_prior_knowledge = .{ .address = a, .client_identity = .none } },
            .tls_proxy => .{ .tls = .{ .address = a, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } } },
        };
        self.server = try starh2.Server.init(gpa, rt.io(), .{
            .endpoints = &.{endpoint},
            .routes = &self.routes,
            .tls = tls_cfg,
            .limits = limits,
        });
        self.handle = try rt.spawn(starh2.Server.serve, .{ &self.server, gpa });
        try self.server.waitUntilListening(5 * std.time.ns_per_s);
    }

    fn stop(self: *Rig) void {
        release.store(true, .release);
        self.server.requestShutdown();
        serve_deadline.join(&self.server, &self.handle);
        self.server.deinit(self.gpa);
        if (self.cert.len != 0) self.gpa.free(self.cert);
        if (self.key.len != 0) self.gpa.free(self.key);
    }

    fn connect(self: *Rig) !zio.net.Stream {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", self.server.localAddress(0).getPort());
        return peer.connect(.{});
    }
};

const Ep = enum { h1c_none, h1c_peer, h2c, tls_proxy };
const Proto = enum { h1c, h2c };

const trusted_loopback = [_]proxy.Cidr{.{ .addr = .{ .ip4 = .{ 127, 0, 0, 1 } }, .prefix_bits = 32 }};

fn writeAll(stream: zio.net.Stream, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) off += try stream.write(bytes[off..], .none);
}

/// GET `path` on a new connection's first request (HTTP/2: stream 1).
fn request(gpa: std.mem.Allocator, stream: zio.net.Stream, p: Proto, path: []const u8) !void {
    switch (p) {
        .h1c => try sendGet(stream, path),
        .h2c => {
            var req = try h2c.buildClientPrefaceAndSettings(gpa);
            defer req.deinit(gpa);
            try h2c.appendHeaders(gpa, &req, 1, path, true);
            try writeAll(stream, req.items);
        },
    }
}

/// How the server answered that request.
const Answer = union(enum) { status: u16, rst: u32, closed };

fn answer(gpa: std.mem.Allocator, stream: zio.net.Stream, p: Proto, timeout_ms: u64) !Answer {
    switch (p) {
        .h1c => return if (try readStatus(stream, timeout_ms)) |st| .{ .status = st } else .closed,
        .h2c => return h2Answer(gpa, stream, timeout_ms),
    }
}

/// Stream 1's HEADERS status or RST_STREAM code; other frames are skipped.
fn h2Answer(gpa: std.mem.Allocator, stream: zio.net.Stream, timeout_ms: u64) !Answer {
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var buf: [64 * 1024]u8 = undefined;
    var n: usize = 0;
    const deadline = nowNs() + timeout_ms * ms;
    while (true) {
        while (n >= 9) {
            const len = (@as(usize, buf[0]) << 16) | (@as(usize, buf[1]) << 8) | buf[2];
            if (n < 9 + len) break;
            const ftype = buf[3];
            const sid = std.mem.readInt(u32, buf[5..9], .big) & 0x7fff_ffff;
            const payload = buf[9 .. 9 + len];
            if (sid == 1 and ftype == 0x3 and len >= 4) return .{ .rst = std.mem.readInt(u32, payload[0..4], .big) };
            if (sid == 1 and ftype == 0x1) {
                const decoded = try dec.decode(payload, 100, 32 * 1024, 256, 8 * 1024);
                defer dec.freeResult(decoded);
                for (decoded.fields) |f| {
                    if (std.mem.eql(u8, f.name, ":status")) return .{ .status = try std.fmt.parseInt(u16, f.value, 10) };
                }
                return error.NoStatus;
            }
            std.mem.copyForwards(u8, buf[0 .. n - 9 - len], buf[9 + len .. n]);
            n -= 9 + len;
        }
        if (nowNs() >= deadline) return error.NoResponse;
        if (n == buf.len) return error.FrameTooLarge;
        const got = stream.read(buf[n..], .{ .duration = .fromMilliseconds(10) }) catch |err| switch (err) {
            error.Timeout => continue,
            error.ConnectionResetByPeer => return .closed,
            else => return err,
        };
        if (got == 0) return .closed;
        n += got;
    }
}

/// A refusal of a request by a server bound: 503, or on HTTP/2 a
/// REFUSED_STREAM reset (the session refuses some before any HEADERS).
fn isRefusal(a: Answer) bool {
    return switch (a) {
        .status => |st| st == 503,
        .rst => |code| code == 0x7,
        .closed => false,
    };
}

fn sendGet(stream: zio.net.Stream, path: []const u8) !void {
    var buf: [128]u8 = undefined;
    const req = try std.fmt.bufPrint(&buf, "GET {s} HTTP/1.1\r\nhost: localhost\r\n\r\n", .{path});
    var off: usize = 0;
    while (off < req.len) off += try stream.write(req[off..], .none);
}

/// The status of the next response on `stream`, or null when the server
/// closed it first.
fn readStatus(stream: zio.net.Stream, timeout_ms: u64) !?u16 {
    var buf: [512]u8 = undefined;
    var n: usize = 0;
    const deadline = nowNs() + timeout_ms * ms;
    while (n < 12) {
        if (nowNs() >= deadline) return error.NoResponse;
        const got = stream.read(buf[n..], .{ .duration = .fromMilliseconds(10) }) catch |err| switch (err) {
            error.Timeout => continue,
            error.ConnectionResetByPeer => return null,
            else => return err,
        };
        if (got == 0) return null;
        n += got;
    }
    if (!std.mem.startsWith(u8, buf[0..n], "HTTP/1.1 ")) return error.NotHttp;
    return try std.fmt.parseInt(u16, buf[9..12], 10);
}

fn waitStarted(want: usize) !void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (started.load(.acquire) < want) {
        if (nowNs() >= deadline) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

fn expectDelta(comptime what: []const u8, before: u64, after: u64, want: u64) !void {
    if (after - before != want) {
        std.debug.print("stats: {s}: moved by {d}, want {d}\n", .{ what, after - before, want });
        return error.WrongDelta;
    }
}

// ---------------------------------------------------------------------------
// The three new counters.

fn streamBound(rt: *zio.Runtime, gpa: std.mem.Allocator, p: Proto) !void {
    var limits = starh2.Limits.defaults;
    limits.max_streams_per_server = 1;
    limits.max_streams_per_connection = 1;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, if (p == .h1c) .h1c_none else .h2c, true);
    defer rig.stop();
    const before = rig.server.stats();

    const first = try rig.connect();
    defer first.close();
    try request(gpa, first, p, "/hold");
    try waitStarted(1);
    try std.testing.expectEqual(@as(usize, 1), rig.server.stats().active_streams);

    const second = try rig.connect();
    defer second.close();
    try request(gpa, second, p, "/hello");
    const got = try answer(gpa, second, p, 5000);
    if (!isRefusal(got)) {
        std.debug.print("stats: stream bound ({t}): answered {any}, want 503 or REFUSED_STREAM\n", .{ p, got });
        return error.NotRefused;
    }
    const after = rig.server.stats();
    try expectDelta("stream_bound_refusals", before.stream_bound_refusals, after.stream_bound_refusals, 1);
    try expectDelta("task_bound_refusals", before.task_bound_refusals, after.task_bound_refusals, 0);

    release.store(true, .release);
    try std.testing.expectEqual(Answer{ .status = 200 }, try answer(gpa, first, p, 5000));
    std.debug.print("stats: stream bound ({t}): {any}, stream_bound_refusals {d} -> {d}\n", .{ p, got, before.stream_bound_refusals, after.stream_bound_refusals });
}

fn taskBound(rt: *zio.Runtime, gpa: std.mem.Allocator, p: Proto) !void {
    var limits = starh2.Limits.defaults;
    // Two task routes, each allowed the whole server: the server's bound is
    // reached only by both together. (One route holding everything is
    // refused by its own share first, which is charged before the server.)
    limits.max_task_handlers = 2;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, if (p == .h1c) .h1c_none else .h2c, true);
    defer rig.stop();
    const before = rig.server.stats();

    var held: [2]zio.net.Stream = undefined;
    for (&held, [_][]const u8{ "/hold", "/hold2" }, 0..) |*s, path, i| {
        s.* = try rig.connect();
        try request(gpa, s.*, p, path);
        try waitStarted(i + 1);
    }
    defer for (held) |s| s.close();
    try std.testing.expectEqual(@as(usize, 2), rig.server.stats().task_handlers);

    const third = try rig.connect();
    defer third.close();
    try request(gpa, third, p, "/hold");
    const got = try answer(gpa, third, p, 5000);
    if (!isRefusal(got)) {
        std.debug.print("stats: task bound ({t}): answered {any}, want 503 or REFUSED_STREAM\n", .{ p, got });
        return error.NotRefused;
    }
    const after = rig.server.stats();
    const rr = @backingInt(accounts.RouteResource.task_handlers);
    try expectDelta("task_bound_refusals", before.task_bound_refusals, after.task_bound_refusals, 1);
    try expectDelta("route_share_refusals[task_handlers]", before.route_share_refusals[rr], after.route_share_refusals[rr], 0);
    try expectDelta("stream_bound_refusals", before.stream_bound_refusals, after.stream_bound_refusals, 0);

    release.store(true, .release);
    for (held) |s| try std.testing.expectEqual(Answer{ .status = 200 }, try answer(gpa, s, p, 5000));
    std.debug.print("stats: task bound ({t}): {any}, task_bound_refusals {d} -> {d}\n", .{ p, got, before.task_bound_refusals, after.task_bound_refusals });
}

fn connectionBound(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    limits.max_connections = 1;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .h1c_none, true);
    defer rig.stop();
    const before = rig.server.stats();

    // The first connection fills the server: the accept loop begins to wait
    // for room (it checks before it accepts), with no client waiting yet.
    const first = try rig.connect();
    try sendGet(first, "/hold");
    try waitStarted(1);
    try waitStat("one accept loop waiting", &rig, 1);
    var now = rig.server.stats();
    try std.testing.expectEqual(@as(usize, 1), now.connections);
    try expectDelta("connection_bound_waits (server full)", before.connection_bound_waits, now.connection_bound_waits, 1);

    // The kernel completes this connect; it waits in the listen queue.
    const second = try rig.connect();
    defer second.close();
    try sendGet(second, "/hello");
    zio.sleep(.fromMilliseconds(100)) catch {};
    now = rig.server.stats();
    try std.testing.expectEqual(@as(usize, 1), now.connections);
    try expectDelta("connection_bound_waits (a client queued, the same wait)", before.connection_bound_waits, now.connection_bound_waits, 1);

    release.store(true, .release);
    try std.testing.expectEqual(@as(?u16, 200), try readStatus(first, 5000));
    first.close();
    // The first connection gone, the queued one is accepted and served, and
    // it fills the server again: a second wait.
    try std.testing.expectEqual(@as(?u16, 200), try readStatus(second, 5000));
    try waitStat("the loop waiting again", &rig, 1);
    const after = rig.server.stats();
    try expectDelta("connection_bound_waits (full again)", before.connection_bound_waits, after.connection_bound_waits, 2);
    std.debug.print("stats: connection bound: the server full twice, the queued connection served; connection_bound_waits {d} -> {d}\n", .{ before.connection_bound_waits, after.connection_bound_waits });
}

/// Until `admission_waiters` is `want`.
fn waitStat(comptime what: []const u8, rig: *Rig, want: usize) !void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (rig.server.stats().admission_waiters != want) {
        if (nowNs() >= deadline) {
            std.debug.print("stats: {s}: admission_waiters {d}, want {d}\n", .{ what, rig.server.stats().admission_waiters, want });
            return error.StateNotReached;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

// ---------------------------------------------------------------------------
// Counters that existed before, through `stats()`.

fn clientShare(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    // Mark 2, a client's share 1: 127.0.0.1 holds two, its third is refused.
    limits.max_connections = 8;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .h1c_peer, true);
    defer rig.stop();
    const res = @backingInt(accounts.Resource.connections);
    const before = rig.server.stats();

    var held: [2]zio.net.Stream = undefined;
    for (&held, 0..) |*s, i| {
        s.* = try rig.connect();
        try sendGet(s.*, "/hold");
        try waitStarted(i + 1);
    }
    defer for (held) |s| s.close();
    try std.testing.expectEqual(@as(usize, 2), rig.server.stats().connections);
    const third = try rig.connect();
    defer third.close();
    // Refused at accept: the server may have closed it before this write.
    sendGet(third, "/hello") catch |err| switch (err) {
        error.ConnectionResetByPeer, error.BrokenPipe => {},
        else => return err,
    };
    // Refused at accept: closed unanswered.
    try std.testing.expectEqual(@as(?u16, null), try readStatus(third, 5000));
    const after = rig.server.stats();
    try expectDelta("client_share_refusals[connections]", before.client_share_refusals[res], after.client_share_refusals[res], 1);
    release.store(true, .release);
    for (held) |s| try std.testing.expectEqual(@as(?u16, 200), try readStatus(s, 5000));
    std.debug.print("stats: client share: third connection closed, client_share_refusals[connections] {d} -> {d}\n", .{ before.client_share_refusals[res], after.client_share_refusals[res] });
}

fn routeShare(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    // Mark 1; two task routes, so each route's share is 2.
    limits.max_task_handlers = 4;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .h1c_none, false);
    defer rig.stop();
    const res = @backingInt(accounts.RouteResource.task_handlers);
    const before = rig.server.stats();

    var held: [2]zio.net.Stream = undefined;
    for (&held, 0..) |*s, i| {
        s.* = try rig.connect();
        try sendGet(s.*, "/hold");
        try waitStarted(i + 1);
    }
    defer for (held) |s| s.close();
    const third = try rig.connect();
    defer third.close();
    try sendGet(third, "/hold");
    try std.testing.expectEqual(@as(?u16, 503), try readStatus(third, 5000));
    const after = rig.server.stats();
    try expectDelta("route_share_refusals[task_handlers]", before.route_share_refusals[res], after.route_share_refusals[res], 1);
    try expectDelta("task_bound_refusals", before.task_bound_refusals, after.task_bound_refusals, 0);
    release.store(true, .release);
    for (held) |s| try std.testing.expectEqual(@as(?u16, 200), try readStatus(s, 5000));
    std.debug.print("stats: route share: third /hold 503, route_share_refusals[task_handlers] {d} -> {d}\n", .{ before.route_share_refusals[res], after.route_share_refusals[res] });
}

/// A PROXY v2 TLS connection waits for a handshake slot after its header,
/// holding a connection slot, while the server is at `max_connections`. Only
/// the accept loop's wait (the server full) counts as a connection-bound
/// wait; the handshake wait does not.
fn handshakeWait(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    limits.max_connections = 2;
    limits.concurrent_tls_handshakes = 1;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .tls_proxy, true);
    defer rig.stop();
    const before = rig.server.stats();
    var hb: [64]u8 = undefined;

    // A: its header, then nothing: it holds the only handshake slot.
    const a = try rig.connect();
    defer a.close();
    try writeAll(a, hb[0..proxy.encode(&hb, .{ .peer = .{ .ip4 = .{ 203, 0, 113, 7 } } })]);
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (rig.server.stats().active_handshakes != 1) {
        if (nowNs() >= deadline) return error.HandshakeSlotNeverTaken;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
    // B: admitted (the server now at max_connections), its header read, then
    // it waits for a handshake slot. The accept loop waits for room too.
    const b = try rig.connect();
    defer b.close();
    try writeAll(b, hb[0..proxy.encode(&hb, .{ .peer = .{ .ip4 = .{ 203, 0, 113, 8 } } })]);
    try waitStat("the accept loop and B waiting", &rig, 2);
    zio.sleep(.fromMilliseconds(50)) catch {};
    const after = rig.server.stats();
    try std.testing.expectEqual(@as(usize, 2), after.connections);
    try std.testing.expectEqual(@as(usize, 1), after.active_handshakes);
    // One wait for the connection bound (the accept loop), none for B.
    try expectDelta("connection_bound_waits (the accept loop only, not the handshake wait)", before.connection_bound_waits, after.connection_bound_waits, 1);
    std.debug.print("stats: handshake wait at max_connections: admission_waiters {d}, connection_bound_waits {d} -> {d}\n", .{ after.admission_waiters, before.connection_bound_waits, after.connection_bound_waits });
}

// ---------------------------------------------------------------------------
// Close reasons (t-3900 item 5).

const CloseReason = starh2.edge.connection.CloseReason;

fn closeTotal(st: starh2.Server.Stats) u64 {
    var sum: u64 = 0;
    for (std.enums.values(CloseReason)) |r| sum += st.close_reasons.get(r);
    return sum;
}

/// Reads and drops what the server sends until it closes `stream`.
fn readUntilClosed(stream: zio.net.Stream, timeout_ms: u64) !void {
    var buf: [16 * 1024]u8 = undefined;
    const deadline = nowNs() + timeout_ms * ms;
    while (true) {
        if (nowNs() >= deadline) return error.ServerDidNotClose;
        const got = stream.read(&buf, .{ .duration = .fromMilliseconds(10) }) catch |err| switch (err) {
            error.Timeout => continue,
            error.ConnectionResetByPeer => return,
            else => return err,
        };
        if (got == 0) return;
    }
}

/// Waits until the server holds no connection: a connection is counted at
/// its teardown, which can come just after the client sees the close.
fn waitNoConnections(server: *starh2.Server) !void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (server.stats().connections != 0) {
        if (nowNs() >= deadline) return error.ConnectionsNotReleased;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

/// `reason` moved by exactly one between `before` and now, and nothing else
/// did: the connection was counted once.
fn expectOneClose(rig: *Rig, before: starh2.Server.Stats, comptime reason: CloseReason) !void {
    try waitNoConnections(&rig.server);
    const after = rig.server.stats();
    errdefer for (std.enums.values(CloseReason)) |r| {
        const d = after.close_reasons.get(r) - before.close_reasons.get(r);
        if (d != 0) std.debug.print("stats: close_reasons.{s} moved by {d}\n", .{ @tagName(r), d });
    };
    try expectDelta("close_reasons." ++ @tagName(reason), before.close_reasons.get(reason), after.close_reasons.get(reason), 1);
    try expectDelta("close_reasons, all reasons", closeTotal(before), closeTotal(after), 1);
}

fn idleClose(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    limits.connection_idle_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .h2c, false);
    defer rig.stop();
    const before = rig.server.stats();

    const c = try rig.connect();
    defer c.close();
    var preface = try h2c.buildClientPrefaceAndSettings(gpa);
    defer preface.deinit(gpa);
    try writeAll(c, preface.items);
    // No request: the server ends it at the idle limit.
    try readUntilClosed(c, 5000);
    try expectOneClose(&rig, before, .h2_idle);
}

/// Reads frames until a GOAWAY arrives.
fn readUntilGoaway(stream: zio.net.Stream, timeout_ms: u64) !void {
    var buf: [16 * 1024]u8 = undefined;
    var n: usize = 0;
    const deadline = nowNs() + timeout_ms * ms;
    while (true) {
        while (n >= 9) {
            const len = (@as(usize, buf[0]) << 16) | (@as(usize, buf[1]) << 8) | buf[2];
            if (n < 9 + len) break;
            if (buf[3] == 0x7) return;
            std.mem.copyForwards(u8, buf[0 .. n - 9 - len], buf[9 + len .. n]);
            n -= 9 + len;
        }
        if (nowNs() >= deadline) return error.NoGoaway;
        const got = stream.read(buf[n..], .{ .duration = .fromMilliseconds(10) }) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
        if (got == 0) return error.ClosedBeforeGoaway;
        n += got;
    }
}

/// The idle close starts a graceful close, which still reads: a reset from
/// the client then fails the server's read. That second ending must not be
/// counted: the connection was already counted as idle.
fn idleThenReset(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    limits.connection_idle_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .h2c, false);
    defer rig.stop();
    const before = rig.server.stats();

    const c = try rig.connect();
    var preface = try h2c.buildClientPrefaceAndSettings(gpa);
    defer preface.deinit(gpa);
    try writeAll(c, preface.items);
    try readUntilGoaway(c, 5000);
    const lg: std.c.linger = .{ .onoff = 1, .linger = 0 };
    _ = std.c.setsockopt(c.socket.handle, std.c.SOL.SOCKET, std.c.SO.LINGER, std.mem.asBytes(&lg), @sizeOf(std.c.linger));
    c.close();
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (rig.server.stats().connections != 0) {
        if (nowNs() >= deadline) return error.ConnectionNotReleased;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
    try expectOneClose(&rig, before, .h2_idle);
}

fn settingsFlood(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var rig: Rig = undefined;
    try rig.start(rt, gpa, starh2.Limits.defaults, .h2c, false);
    defer rig.stop();
    const before = rig.server.stats();

    const c = try rig.connect();
    defer c.close();
    var wire = try h2c.buildClientPrefaceAndSettings(gpa);
    defer wire.deinit(gpa);
    // Past the SETTINGS burst at once, faster than its refill.
    for (0..2 * starh2.core.rates.SETTINGS_BURST) |_| {
        var sbuf: [9]u8 = undefined;
        const n = try starh2.core.frame.Serializer.settingsFrame(&sbuf, false, &.{});
        try wire.appendSlice(gpa, sbuf[0..n]);
    }
    try writeAll(c, wire.items);
    try readUntilClosed(c, 5000);
    try expectOneClose(&rig, before, .h2_goaway_enhance_your_calm);
}

fn bodyTooLarge(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    limits.request_body_bytes = 1024;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, .h1c_none, false);
    defer rig.stop();
    const before = rig.server.stats();

    const c = try rig.connect();
    defer c.close();
    try writeAll(c, "POST /hello HTTP/1.1\r\nhost: localhost\r\ncontent-length: 4096\r\n\r\n");
    try std.testing.expectEqual(@as(?u16, 413), try readStatus(c, 5000));
    try readUntilClosed(c, 5000);
    try expectOneClose(&rig, before, .h1_413);
}

/// Six connections on one server (an h1c and an h2c endpoint), each ended a
/// different way, one after another. After each, the table must have moved
/// by exactly one, in that end's reason; in all, by exactly six. A reset is
/// seen by whichever socket call fails first, the read or a write, so it
/// may count either transport reason.
///
/// `STARH2_CLOSE_MUTANT` arms a mutant of the recording at run time
/// (`connection.CloseMutant`): `peer_closed_as_goaway` or
/// `h1_idle_unrecorded`. Each must fail this test. Run the test binary
/// directly to arm one: the variable is not part of the build's cache key.
fn mixedEnds(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    if (std.c.getenv("STARH2_CLOSE_MUTANT")) |raw| {
        const m = std.meta.stringToEnum(starh2.edge.connection.CloseMutant, std.mem.span(raw)) orelse return error.UnknownMutant;
        starh2.edge.connection.test_close_mutant.store(m, .release);
        std.debug.print("stats: close mutant armed: {s}\n", .{@tagName(m)});
    }
    defer starh2.edge.connection.test_close_mutant.store(.none, .release);

    var limits = starh2.Limits.defaults;
    limits.connection_idle_timeout_ns = 300 * ms;
    limits.field_block_timeout_ns = 300 * ms;
    limits.request_body_bytes = 1024;
    var routes_copy = routes;
    const a = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{
            .{ .h1c = .{ .address = a, .client_identity = .none } },
            .{ .h2c_prior_knowledge = .{ .address = a, .client_identity = .none } },
        },
        .routes = &routes_copy,
        .tls = null,
        .limits = limits,
    });
    var handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_deadline.join(&server, &handle);
        server.deinit(gpa);
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const h1_peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());
    const h2_peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(1).getPort());
    const before = server.stats();
    var prev = before;
    var bad = false;
    var preface = try h2c.buildClientPrefaceAndSettings(gpa);
    defer preface.deinit(gpa);

    // 1. HTTP/2, the server's idle close.
    {
        const c = try h2_peer.connect(.{});
        defer c.close();
        try writeAll(c, preface.items);
        try readUntilClosed(c, 5000);
    }
    prev = try expectStep(&server, prev, &.{.h2_idle}, &bad);
    // 2. HTTP/2, the peer's FIN.
    {
        const c = try h2_peer.connect(.{});
        defer c.close();
        try writeAll(c, preface.items);
        try c.shutdown(.send);
        try readUntilClosed(c, 5000);
    }
    prev = try expectStep(&server, prev, &.{.h2_peer_closed}, &bad);
    // 3. HTTP/2, the peer's reset.
    {
        const c = try h2_peer.connect(.{});
        try writeAll(c, preface.items);
        const lg: std.c.linger = .{ .onoff = 1, .linger = 0 };
        _ = std.c.setsockopt(c.socket.handle, std.c.SOL.SOCKET, std.c.SO.LINGER, std.mem.asBytes(&lg), @sizeOf(std.c.linger));
        c.close();
    }
    prev = try expectStep(&server, prev, &.{ .h2_read_failed, .h2_write_failed }, &bad);
    // 4. HTTP/1.1, a 413.
    {
        const c = try h1_peer.connect(.{});
        defer c.close();
        try writeAll(c, "POST /hello HTTP/1.1\r\nhost: localhost\r\ncontent-length: 4096\r\n\r\n");
        try std.testing.expectEqual(@as(?u16, 413), try readStatus(c, 5000));
        try readUntilClosed(c, 5000);
    }
    prev = try expectStep(&server, prev, &.{.h1_413}, &bad);
    // 5. HTTP/1.1, no request head within field_block_timeout_ns.
    {
        const c = try h1_peer.connect(.{});
        defer c.close();
        try readUntilClosed(c, 5000);
    }
    prev = try expectStep(&server, prev, &.{.h1_idle}, &bad);
    // 6. HTTP/1.1, the client's EOF after one answered request.
    {
        const c = try h1_peer.connect(.{});
        defer c.close();
        try sendGet(c, "/hello");
        try std.testing.expectEqual(@as(?u16, 200), try readStatus(c, 5000));
        try c.shutdown(.send);
        try readUntilClosed(c, 5000);
    }
    prev = try expectStep(&server, prev, &.{.h1_peer_closed}, &bad);

    try expectDelta("close_reasons, all reasons (six connections)", closeTotal(before), closeTotal(prev), 6);
    if (bad) return error.WrongCloseReasons;
}

/// After one connection's end: the table moved by exactly one, in one of
/// `allowed`. Prints what moved otherwise and sets `bad`. Returns the new
/// snapshot.
fn expectStep(server: *starh2.Server, prev: starh2.Server.Stats, allowed: []const CloseReason, bad: *bool) !starh2.Server.Stats {
    try waitNoConnections(server);
    const now = server.stats();
    var moved: u64 = 0;
    var in_allowed: u64 = 0;
    for (std.enums.values(CloseReason)) |r| {
        const d = now.close_reasons.get(r) - prev.close_reasons.get(r);
        if (d == 0) continue;
        moved += d;
        if (std.mem.indexOfScalar(CloseReason, allowed, r) != null) in_allowed += d;
        std.debug.print("stats: mixed ends: close_reasons.{s} moved by {d}\n", .{ @tagName(r), d });
    }
    if (moved != 1 or in_allowed != 1) {
        std.debug.print("stats: mixed ends: want one count in {any}, got {d} ({d} of them there)\n", .{ allowed, moved, in_allowed });
        bad.* = true;
    }
    return now;
}

// ---------------------------------------------------------------------------
// Orphans (t-3900): per route, and the oldest one's age.

fn waitOrphans(rig: *Rig, want: usize) !void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (rig.server.stats().orphaned_handlers != want) {
        if (nowNs() >= deadline) {
            std.debug.print("stats: orphans {d}, want {d}\n", .{ rig.server.stats().orphaned_handlers, want });
            return error.OrphansNeverReached;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

/// A handler of `/hold`, then 300 ms later one of `/hold2`, each on its own
/// connection; both connections close, so both handlers are orphans. Then:
/// one orphan on each route, the server's oldest orphan at least 300 ms
/// old, and `/hold`'s orphan about 300 ms older than `/hold2`'s. Once they
/// return: no orphan anywhere, and every age 0.
fn orphans(rt: *zio.Runtime, gpa: std.mem.Allocator, p: Proto) !void {
    var limits = starh2.Limits.defaults;
    limits.graceful_drain_timeout_ns = 200 * ms;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, limits, if (p == .h1c) .h1c_none else .h2c, true);
    defer rig.stop();
    ignore_cancel.store(true, .release);
    defer ignore_cancel.store(false, .release);
    // On any exit: let the orphans return before the stop.
    defer release.store(true, .release);
    try std.testing.expectEqual(@as(usize, 2), rig.server.taskRouteCount());
    try std.testing.expectEqualStrings("GET /hold", rig.server.routeStats(0).label);
    try std.testing.expectEqualStrings("GET /hold2", rig.server.routeStats(1).label);
    try std.testing.expectEqual(@as(u64, 0), rig.server.stats().oldest_orphan_age_ns);

    const a = try rig.connect();
    var a_open = true;
    defer if (a_open) a.close();
    try request(gpa, a, p, "/hold");
    try waitStarted(1);
    zio.sleep(.fromMilliseconds(300)) catch {};
    const b = try rig.connect();
    var b_open = true;
    defer if (b_open) b.close();
    try request(gpa, b, p, "/hold2");
    try waitStarted(2);
    a.close();
    a_open = false;
    b.close();
    b_open = false;
    try waitOrphans(&rig, 2);

    const r0 = rig.server.routeStats(0);
    const r1 = rig.server.routeStats(1);
    const s = rig.server.stats();
    std.debug.print("stats: orphans ({s}): server {d}, oldest {d} ms; {s} {d} (oldest {d} ms), {s} {d} (oldest {d} ms)\n", .{
        @tagName(p),                       s.orphaned_handlers, s.oldest_orphan_age_ns / ms,
        r0.label,                          r0.orphans,          r0.oldest_orphan_age_ns / ms,
        r1.label,                          r1.orphans,          r1.oldest_orphan_age_ns / ms,
    });
    try std.testing.expectEqual(@as(usize, 1), r0.orphans);
    try std.testing.expectEqual(@as(usize, 1), r1.orphans);
    try std.testing.expectEqual(@as(usize, 1), r0.task_handlers);
    try std.testing.expectEqual(@as(usize, 1), r1.task_handlers);
    // Read in the order r0, r1, s: each later read is at least as old.
    if (r0.oldest_orphan_age_ns < 300 * ms) return error.OldestOrphanTooYoung;
    if (r1.oldest_orphan_age_ns == 0) return error.NoOrphanAge;
    if (r0.oldest_orphan_age_ns < r1.oldest_orphan_age_ns + 250 * ms) return error.RouteAgesNotApart;
    if (s.oldest_orphan_age_ns < r0.oldest_orphan_age_ns) return error.ServerOldestNotTheOldest;
    // Not far past it either: the server's oldest is /hold's orphan.
    if (s.oldest_orphan_age_ns > r0.oldest_orphan_age_ns + 250 * ms) return error.ServerOldestTooOld;

    release.store(true, .release);
    try waitOrphans(&rig, 0);
    const after = rig.server.stats();
    try std.testing.expectEqual(@as(u64, 0), after.oldest_orphan_age_ns);
    for (0..2) |i| {
        const r = rig.server.routeStats(i);
        try std.testing.expectEqual(@as(usize, 0), r.orphans);
        try std.testing.expectEqual(@as(u64, 0), r.oldest_orphan_age_ns);
    }
}

fn runProto(comptime f: anytype, p: Proto) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(f, .{ rt, gpa, p });
    try h.join();
}

fn runIn(comptime f: anytype) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(f, .{ rt, gpa });
    try h.join();
}

test "stats: a request past the server's stream bound counts stream_bound_refusals, HTTP/1.1 (t-3759 step 9b)" {
    try runProto(streamBound, .h1c);
}

test "stats: a request past the server's stream bound counts stream_bound_refusals, HTTP/2 (t-3759 step 9b)" {
    try runProto(streamBound, .h2c);
}

test "stats: a task request past the server's task-handler bound counts task_bound_refusals, HTTP/1.1 (t-3759 step 9b)" {
    try runProto(taskBound, .h1c);
}

test "stats: a task request past the server's task-handler bound counts task_bound_refusals, HTTP/2 (t-3759 step 9b)" {
    try runProto(taskBound, .h2c);
}

test "stats: a PROXY TLS connection waiting for a handshake slot at max_connections is not a connection-bound wait (t-3759 step 9b)" {
    try runIn(handshakeWait);
}

test "stats: each time the server reaches max_connections counts connection_bound_waits, and a queued connection is served later (t-3759 step 9b)" {
    try runIn(connectionBound);
}

test "stats: a client-share refusal shows in client_share_refusals (t-3759 step 9b)" {
    try runIn(clientShare);
}

test "stats: a route-share refusal shows in route_share_refusals (t-3759 step 9b)" {
    try runIn(routeShare);
}

test "stats: an HTTP/2 connection idle past its limit counts close_reasons.h2_idle, once (t-3900 item 5)" {
    try runIn(idleClose);
}

test "stats: an HTTP/2 idle close followed by a client reset counts close_reasons.h2_idle only (t-3900 item 5)" {
    try runIn(idleThenReset);
}

test "stats: an HTTP/2 SETTINGS flood counts close_reasons.h2_goaway_enhance_your_calm, once (t-3900 item 5)" {
    try runIn(settingsFlood);
}

test "stats: an HTTP/1.1 request over request_body_bytes counts close_reasons.h1_413, once (t-3900 item 5)" {
    try runIn(bodyTooLarge);
}

test "stats: six connections ended six ways, the peer's included, move close_reasons by six, one in each reason (t-3900 item 5, round 2)" {
    try runIn(mixedEnds);
}

test "stats: orphans per route and the oldest orphan's age, HTTP/1.1 (t-3900)" {
    try runProto(orphans, .h1c);
}

test "stats: orphans per route and the oldest orphan's age, HTTP/2 (t-3900)" {
    try runProto(orphans, .h2c);
}
