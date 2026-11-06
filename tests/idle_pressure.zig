//! Shorter idle timeouts under connection pressure (t-3759 step 8), on real
//! sockets.
//!
//! An HTTP/2 connection reads the server's connection count once, when an
//! idle wait starts: at or above the connection share's pressure mark (L/4 of
//! `max_connections`, the client shares' mark) the wait gets
//! `connection_idle_timeout_under_pressure_ns`, else
//! `connection_idle_timeout_ns`. A wait that started before pressure keeps
//! its deadline. The test limits are short (normal 3 s, under pressure
//! 600 ms) so a run takes seconds. The first GOAWAY is the idle close, timed
//! from the moment the client could have been idle: after its preface, or
//! after its stream's last frame. Each close must land in a window: not
//! before its limit, and at most `late_ms` after it. The two windows do not
//! overlap, so each arm tells the limits apart.
//!
//! Identities: 127.0.0.1 and ::1, on both machines (macOS answers only on
//! 127.0.0.1 of 127/8 unless an alias is set). One identity reaches the
//! mark by itself (its cap under pressure is L/4, the mark), so the table
//! at the mark holds both: three idle connections from 127.0.0.1 and the
//! first from ::1.
//!
//! HTTP/1.1 does not use the pressure limit: its wait for the next request
//! is `field_block_timeout_ns` (5 s by default, below the 10 s default
//! pressure limit). Its arm shows that wait, under pressure, on h1c and TLS.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");
const serve_deadline = @import("serve_deadline.zig");

const accounts = starh2.edge.accounts;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;

const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

const max_connections = 16;
const mark = accounts.Share.of(max_connections).mark;
const normal_ms = 3000;
const short_ms = 600;
const field_block_ms = 1500;
/// How late a close may come after its limit: the actor's wake, the
/// GOAWAY's trip, a loaded ci host.
const late_ms = 1000;

comptime {
    // The windows must not overlap, or an arm could not tell them apart.
    std.debug.assert(short_ms + late_ms < normal_ms);
    std.debug.assert(short_ms < field_block_ms);
    std.debug.assert(mark == 4);
}

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
};

fn testLimits() starh2.Limits {
    var l = starh2.Limits.defaults;
    l.max_connections = max_connections;
    l.connection_idle_timeout_ns = normal_ms * ms;
    l.connection_idle_timeout_under_pressure_ns = short_ms * ms;
    l.field_block_timeout_ns = field_block_ms * ms;
    l.graceful_drain_timeout_ns = 300 * ms;
    return l;
}

const Tr = enum { h2c, tls, h1c, tls_h1 };

// ---------------------------------------------------------------------------
// The client, on an OS thread: its calls block.

const Client = struct {
    gpa: std.mem.Allocator,
    name: []const u8,
    tr: Tr,
    v6: bool,
    port: u16,
    /// HTTP/2: once set, send one request and time the idle wait from its
    /// end instead of from the preface.
    request_on: ?*std.atomic.Value(bool) = null,
    /// The client could be idle from now on (`nowNs`).
    ready: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    /// From the start of its last idle wait to the server's close: the first
    /// GOAWAY (HTTP/2) or the end of the stream (HTTP/1.1).
    elapsed_ms: u64 = 0,
    err: ?anyerror = null,
    thread: std.Thread = undefined,

    fn start(self: *Client) !void {
        self.thread = try std.Thread.spawn(.{}, main, .{self});
    }

    fn main(self: *Client) void {
        self.run() catch |err| {
            std.debug.print("idle-pressure client {s} ({t}) failed: {t}\n", .{ self.name, self.tr, err });
            self.err = err;
        };
        self.done.store(true, .release);
    }

    fn run(self: *Client) !void {
        switch (self.tr) {
            .h2c, .tls => try self.runH2(),
            .h1c, .tls_h1 => try self.runH1(),
        }
    }

    fn open(self: *Client, transport: wire_client.Transport, alpn: []const u8) !*Wire {
        const w = try (if (self.v6) Wire.connectIp6(self.gpa, transport, self.port) else Wire.connect(self.gpa, transport, self.port));
        errdefer w.close();
        w.alpn = alpn;
        try w.handshake();
        return w;
    }

    fn runH2(self: *Client) !void {
        const w = try self.open(if (self.tr == .tls) .tls else .h2c, "h2");
        defer w.close();
        var pre = try h2c.buildClientPrefaceAndSettings(self.gpa);
        defer pre.deinit(self.gpa);
        try w.send(pre.items);
        var t0 = nowNs();
        self.ready.store(true, .release);
        const deadline = t0 + (normal_ms + 5000) * ms;
        if (self.request_on) |on| {
            while (!on.load(.acquire)) {
                if (nowNs() >= deadline) return error.NeverAskedToRequest;
                starh2.test_sleep.threadSleepMs(1);
            }
            var req: std.ArrayList(u8) = .empty;
            defer req.deinit(self.gpa);
            try h2c.appendHeaders(self.gpa, &req, 1, "/hello", true);
            try w.send(req.items);
            while (true) {
                const s = try w.next(deadline) orelse return error.ClosedBeforeResponse;
                if (s.ftype == 7) return error.GoawayBeforeResponse;
                if ((s.ftype == 0 or s.ftype == 1) and s.sid == 1 and s.flags & 1 != 0) break;
            }
            t0 = nowNs();
        }
        while (true) {
            const s = try w.next(deadline) orelse return error.ClosedWithoutGoaway;
            if (s.ftype == 7) {
                self.elapsed_ms = (nowNs() - t0) / ms;
                return;
            }
        }
    }

    fn runH1(self: *Client) !void {
        const w = try self.open(if (self.tr == .tls_h1) .tls else .h2c, "http/1.1");
        defer w.close();
        try w.sendUnchecked("GET /hello HTTP/1.1\r\nhost: localhost\r\n\r\n");
        const deadline = nowNs() + (field_block_ms + 5000) * ms;
        // The response is small and has a length: it ends with its body.
        while (std.mem.indexOf(u8, w.plain.items, "\r\n\r\nok") == null) {
            if (nowNs() >= deadline) return error.NoResponse;
            const more = w.pump() catch |err| switch (err) {
                error.WouldBlock => continue,
                else => return err,
            };
            if (!more) return error.ClosedBeforeResponse;
        }
        const t0 = nowNs();
        self.ready.store(true, .release);
        const got = w.plain.items.len;
        while (true) {
            if (nowNs() >= deadline) return error.NeverClosed;
            const more = w.pump() catch |err| switch (err) {
                error.WouldBlock => continue,
                else => return err,
            };
            if (!more) break;
        }
        if (w.plain.items.len != got) return error.BytesAfterResponse;
        self.elapsed_ms = (nowNs() - t0) / ms;
    }
};

// ---------------------------------------------------------------------------
// The test's side, on a zio task: it waits with zio.sleep.

const Rig = struct {
    gpa: std.mem.Allocator,
    server: starh2.Server,
    handle: zio.JoinHandle(starh2.ServeError!void),
    cert: []u8 = &.{},
    key: []u8 = &.{},

    /// Endpoint 0 on 127.0.0.1, endpoint 1 on ::1.
    fn start(self: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator, ep0: Tr, ep1: Tr) !void {
        const io = rt.io();
        self.* = .{ .gpa = gpa, .server = undefined, .handle = undefined };
        const tls_on = ep0 == .tls or ep0 == .tls_h1 or ep1 == .tls or ep1 == .tls_h1;
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
        const eps = [2]starh2.EndpointConfig{
            endpoint(ep0, try starh2.EndpointAddress.parseIp4("127.0.0.1", 0)),
            endpoint(ep1, try starh2.EndpointAddress.parseIp6("::1", 0)),
        };
        self.server = try starh2.Server.init(gpa, io, .{
            .endpoints = &eps,
            .routes = &routes,
            .tls = tls_cfg,
            .limits = testLimits(),
        });
        self.handle = try rt.spawn(starh2.Server.serve, .{ &self.server, gpa });
        try self.server.waitUntilListening(5 * std.time.ns_per_s);
    }

    fn endpoint(tr: Tr, a: starh2.EndpointAddress) starh2.EndpointConfig {
        return switch (tr) {
            .h2c => .{ .h2c_prior_knowledge = .{ .address = a } },
            .tls, .tls_h1 => .{ .tls = .{ .address = a } },
            .h1c => .{ .h1c = .{ .address = a } },
        };
    }

    fn port(self: *Rig, i: usize) u16 {
        return self.server.localAddress(i).getPort();
    }

    fn connections(self: *Rig) usize {
        return self.server.active_connections.load(.seq_cst);
    }

    fn stop(self: *Rig) void {
        self.server.requestShutdown();
        serve_deadline.join(&self.server, &self.handle);
        self.server.deinit(self.gpa);
        if (self.cert.len != 0) self.gpa.free(self.cert);
        if (self.key.len != 0) self.gpa.free(self.key);
    }

    /// Start `c` and wait until it could be idle and the server counts
    /// `want` connections, then a little longer, so the server's first turn
    /// on it (where its idle wait starts) is done. Returns the count then.
    fn begin(self: *Rig, c: *Client, want: usize) !usize {
        try c.start();
        const deadline = nowNs() + 5 * std.time.ns_per_s;
        while (!c.ready.load(.acquire) or self.connections() < want) {
            if (c.done.load(.acquire)) return c.err orelse error.ClientEndedEarly;
            if (nowNs() >= deadline) {
                std.debug.print("idle-pressure: {s} not ready with {d} connections (want {d})\n", .{ c.name, self.connections(), want });
                return error.StateNotReached;
            }
            zio.sleep(.fromMilliseconds(2)) catch {};
        }
        zio.sleep(.fromMilliseconds(100)) catch {};
        return self.connections();
    }

    fn waitConnections(self: *Rig, want: usize) !void {
        const deadline = nowNs() + 5 * std.time.ns_per_s;
        while (self.connections() != want) {
            if (nowNs() >= deadline) {
                std.debug.print("idle-pressure: {d} connections, want {d}\n", .{ self.connections(), want });
                return error.StateNotReached;
            }
            zio.sleep(.fromMilliseconds(2)) catch {};
        }
    }
};

/// Wait for every client and join it. The first error wins.
fn finish(clients: []const *Client) !void {
    const deadline = nowNs() + (normal_ms + 8000) * ms;
    for (clients) |c| {
        while (!c.done.load(.acquire)) {
            if (nowNs() >= deadline) return error.ClientNeverEnded;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        c.thread.join();
    }
    for (clients) |c| if (c.err) |err| return err;
}

fn expectWindow(tr: Tr, c: *const Client, limit_ms: u64, comptime why: []const u8) !void {
    if (c.elapsed_ms < limit_ms or c.elapsed_ms > limit_ms + late_ms) {
        std.debug.print("idle-pressure {t}: {s} closed after {d} ms, want {d} to {d} ms ({s})\n", .{ tr, c.name, c.elapsed_ms, limit_ms, limit_ms + late_ms, why });
        return error.IdleCloseOutsideWindow;
    }
}

// ---------------------------------------------------------------------------
// HTTP/2: the four arms.

fn h2Run(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, tr);
    defer rig.stop();
    const p4 = rig.port(0);
    const p6 = rig.port(1);

    // Control: alone on the server, never under pressure.
    var ctl: Client = .{ .gpa = gpa, .name = "control", .tr = tr, .v6 = false, .port = p4 };
    const at_ctl = try rig.begin(&ctl, 1);
    try finish(&.{&ctl});
    try rig.waitConnections(0);

    // A1 and A2 go idle below the mark and stay idle: the accepted tail. A3
    // goes idle below the mark too, then makes a request once the server is
    // under pressure, so its next idle wait starts under it.
    var go: std.atomic.Value(bool) = .init(false);
    var a1: Client = .{ .gpa = gpa, .name = "A1 (idle before pressure)", .tr = tr, .v6 = false, .port = p4 };
    var a2: Client = .{ .gpa = gpa, .name = "A2 (idle before pressure)", .tr = tr, .v6 = false, .port = p4 };
    var a3: Client = .{ .gpa = gpa, .name = "A3 (idle again under pressure)", .tr = tr, .v6 = false, .port = p4, .request_on = &go };
    var b1: Client = .{ .gpa = gpa, .name = "B1 (idle under pressure)", .tr = tr, .v6 = true, .port = p6 };
    var b2: Client = .{ .gpa = gpa, .name = "B2 (idle under pressure)", .tr = tr, .v6 = true, .port = p6 };
    var started: [5]*Client = undefined;
    var n: usize = 0;
    // Every started client is joined, also when an arm fails.
    defer finish(started[0..n]) catch {};
    var at: [5]usize = undefined;
    for ([_]*Client{ &a1, &a2, &a3, &b1, &b2 }, 0..) |c, i| {
        at[i] = try rig.begin(c, i + 1);
        started[n] = c;
        n += 1;
    }
    go.store(true, .release);
    const all = started[0..n];
    n = 0;
    try finish(all);

    std.debug.print("idle-pressure {t}: mark {d} of {d}; control {d} ms (connections {d}); before pressure A1 {d} ms, A2 {d} ms (connections {d}, {d}); under pressure B1 {d} ms, B2 {d} ms (connections {d}, {d}); A3 {d} ms after its stream ended under pressure\n", .{ tr, mark, max_connections, ctl.elapsed_ms, at_ctl, a1.elapsed_ms, a2.elapsed_ms, at[0], at[1], b1.elapsed_ms, b2.elapsed_ms, at[3], at[4], a3.elapsed_ms });
    // The precondition: below the mark for the first three, at or above it
    // for the B's. A run that never reached pressure tested nothing.
    if (at_ctl >= mark or at[0] >= mark or at[1] >= mark or at[2] >= mark or at[3] < mark or at[4] < mark) {
        std.debug.print("idle-pressure {t}: pressure not where the arms need it\n", .{tr});
        return error.PressureNotReached;
    }
    try expectWindow(tr, &ctl, normal_ms, "no pressure: the normal limit");
    try expectWindow(tr, &a1, normal_ms, "idle before pressure keeps its deadline");
    try expectWindow(tr, &a2, normal_ms, "idle before pressure keeps its deadline");
    try expectWindow(tr, &b1, short_ms, "idle wait started under pressure");
    try expectWindow(tr, &b2, short_ms, "idle wait started under pressure");
    try expectWindow(tr, &a3, short_ms, "a new idle wait started under pressure");
}

// ---------------------------------------------------------------------------
// HTTP/1.1: the wait for the next request, under pressure.

fn h1Run(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    // HTTP/2 connections from 127.0.0.1 hold the server at the mark; the
    // HTTP/1.1 client is ::1.
    var rig: Rig = undefined;
    try rig.start(rt, gpa, .h2c, tr);
    defer rig.stop();
    var fill: [mark]Client = undefined;
    var started: [mark + 1]*Client = undefined;
    var n: usize = 0;
    defer finish(started[0..n]) catch {};
    for (&fill, 0..) |*f, i| {
        f.* = .{ .gpa = gpa, .name = "filler", .tr = .h2c, .v6 = false, .port = rig.port(0) };
        _ = try rig.begin(f, i + 1);
        started[n] = f;
        n += 1;
    }
    var c: Client = .{ .gpa = gpa, .name = "keep-alive", .tr = tr, .v6 = true, .port = rig.port(1) };
    const at = try rig.begin(&c, mark + 1);
    started[n] = &c;
    n += 1;
    const all = started[0..n];
    n = 0;
    try finish(all);
    std.debug.print("idle-pressure {t}: the wait for the next request closed after {d} ms with {d} connections (mark {d}); field_block_timeout {d} ms, pressure limit {d} ms\n", .{ tr, c.elapsed_ms, at, mark, field_block_ms, short_ms });
    if (at < mark) return error.PressureNotReached;
    try expectWindow(tr, &c, field_block_ms, "HTTP/1.1 waits field_block_timeout_ns for its next request");
}

// ---------------------------------------------------------------------------

fn runCase(comptime f: anytype, tr: Tr) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(4) });
    defer rt.deinit();
    var h = try rt.spawn(f, .{ rt, gpa, tr });
    try h.join();
}

test "idle under pressure (h2c): a wait that starts under pressure gets the short limit; before pressure and without it, the normal one" {
    try runCase(h2Run, .h2c);
}

test "idle under pressure (TLS h2): a wait that starts under pressure gets the short limit; before pressure and without it, the normal one" {
    try runCase(h2Run, .tls);
}

test "idle under pressure (h1c): the wait for the next request is field_block_timeout_ns, under pressure too" {
    try runCase(h1Run, .h1c);
}

test "idle under pressure (TLS HTTP/1.1): the wait for the next request is field_block_timeout_ns, under pressure too" {
    try runCase(h1Run, .tls_h1);
}
