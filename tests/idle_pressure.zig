//! Shorter idle timeouts under connection pressure (t-3759 step 8), on real
//! sockets.
//!
//! An HTTP/2 connection reads the server's connection count once, when an
//! idle wait starts. Under connection pressure (`accounts.underPressure`: the
//! connections other than this one at or above the mark, L/4 of
//! `max_connections`, the rule the client shares use at admission) the wait
//! gets `connection_idle_timeout_under_pressure_ns`, else
//! `connection_idle_timeout_ns`. A wait that started before pressure keeps
//! its deadline. The test limits are short (normal 3 s, under pressure
//! 600 ms) so a run takes seconds.
//!
//! The close is the idle close's first GOAWAY (NO_ERROR, last stream
//! 2^31-1: the graceful phase 1), timed from before the client connected,
//! or from its stream's last frame. Each close must land in a window: at
//! most `early_ms` before its limit (the server's wait can start before the
//! client's clock does, and times are floored to ms) and at most `late_ms`
//! after it. The windows do not overlap, so each arm tells the limits apart.
//!
//! Identities: 127.0.0.1 and ::1, on both machines (macOS answers only on
//! 127.0.0.1 of 127/8 unless an alias is set). One identity fills the
//! connections up to the mark by itself (its cap under pressure is L/4), so
//! the table at the mark holds four idle connections from 127.0.0.1, and
//! the connections from ::1 are the first under pressure.
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
const field_block_ms = 2000;
/// How late a close may come after its limit: the actor's wake, the
/// GOAWAY's trip, a loaded ci host.
const late_ms = 1000;
/// How early: the server's wait can start before the client's clock
/// (HTTP/1.1 and A3 time from the response they read), and ms are floored.
const early_ms = 100;

comptime {
    // The windows must not overlap, or an arm could not tell them apart.
    std.debug.assert(short_ms + late_ms < normal_ms - early_ms);
    std.debug.assert(short_ms + late_ms < field_block_ms - early_ms);
    std.debug.assert(mark == 4);
}

/// The rule under test, written out here rather than called
/// (`accounts.underPressure`), so a defect there cannot move the test's own
/// preconditions with it: `n` connections open in all, the one in question
/// among them, and the others at or above the mark.
fn pressured(n: usize) bool {
    return n -| 1 >= mark;
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
    /// end instead of from the connect.
    request_on: ?*std.atomic.Value(bool) = null,
    /// The server's connection count, read right after that request's
    /// response (`request_on`): the reading its second idle wait starts
    /// with.
    server_connections: ?*const std.atomic.Value(usize) = null,
    connections_after_response: usize = 0,
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
        // Before the connect: the server's idle wait cannot start earlier.
        var t0 = nowNs();
        const deadline = t0 + (normal_ms + 5000) * ms;
        const w = try self.open(if (self.tr == .tls) .tls else .h2c, "h2");
        defer w.close();
        var pre = try h2c.buildClientPrefaceAndSettings(self.gpa);
        defer pre.deinit(self.gpa);
        try w.send(pre.items);
        // The server's SETTINGS leaves in the emit of an actor turn whose
        // idle check (where the wait starts and reads pressure) runs before
        // it (`Connection` turn: `checkConnectionIdle`, then
        // `materializeAndDrain`).
        while (true) {
            const s = try w.next(deadline) orelse return error.ClosedBeforeSettings;
            if (s.ftype == 7) return error.GoawayBeforeSettings;
            if (s.ftype == 4 and s.flags & 1 == 0) break;
        }
        self.ready.store(true, .release);
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
            if (self.server_connections) |c| self.connections_after_response = c.load(.seq_cst);
        }
        while (true) {
            const s = try w.next(deadline) orelse return error.ClosedWithoutGoaway;
            if (s.ftype == 7) {
                self.elapsed_ms = (nowNs() - t0) / ms;
                // The idle close is the graceful path's first GOAWAY
                // (`Session.beginGracefulPhase1`): NO_ERROR, last stream
                // 2^31-1. Any other is some other close.
                const p = w.last_payload.items;
                if (p.len < 8) return error.ShortGoaway;
                const last = std.mem.readInt(u32, p[0..4], .big) & 0x7fff_ffff;
                const code = std.mem.readInt(u32, p[4..8], .big);
                if (last != std.math.maxInt(u31) or code != 0) {
                    std.debug.print("idle-pressure client {s} ({t}): GOAWAY last stream {d} code {d}, not the idle close's (2147483647, NO_ERROR)\n", .{ self.name, self.tr, last, code });
                    return error.NotTheIdleGoaway;
                }
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

    /// Start `c` (registered in `started` before any wait, so it is joined
    /// on every exit) and wait until it could be idle and the server counts
    /// `want` connections, then a little longer as margin. Returns the
    /// count then.
    fn begin(self: *Rig, started: *Started, c: *Client, want: usize) !usize {
        try started.start(c);
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

/// Every client thread a test started. `joinAll` runs on every exit
/// (`defer`): a client's own deadlines bound how long it can run, so the
/// wait ends, and no thread outlives the stack it points into.
const Started = struct {
    list: [8]*Client = undefined,
    n: usize = 0,

    fn start(self: *Started, c: *Client) !void {
        std.debug.assert(self.n < self.list.len);
        try c.start();
        self.list[self.n] = c;
        self.n += 1;
    }

    fn joinAll(self: *Started) void {
        for (self.list[0..self.n]) |c| {
            while (!c.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
            starh2.test_sleep.joinThread(c.thread);
        }
        self.n = 0;
    }

    /// Every client ended, in time, without an error.
    fn finish(self: *Started) !void {
        const deadline = nowNs() + (normal_ms + 8000) * ms;
        for (self.list[0..self.n]) |c| {
            while (!c.done.load(.acquire)) {
                if (nowNs() >= deadline) return error.ClientNeverEnded;
                zio.sleep(.fromMilliseconds(5)) catch {};
            }
        }
        const all = self.list[0..self.n];
        self.joinAll();
        for (all) |c| if (c.err) |err| return err;
    }
};

fn expectWindow(tr: Tr, c: *const Client, limit_ms: u64, comptime why: []const u8) !void {
    if (c.elapsed_ms + early_ms < limit_ms or c.elapsed_ms > limit_ms + late_ms) {
        std.debug.print("idle-pressure {t}: {s} closed after {d} ms, want {d} to {d} ms ({s})\n", .{ tr, c.name, c.elapsed_ms, limit_ms - early_ms, limit_ms + late_ms, why });
        return error.IdleCloseOutsideWindow;
    }
}

fn expectPressure(tr: Tr, comptime what: []const u8, n: usize, want: bool) !void {
    if (pressured(n) != want) {
        std.debug.print("idle-pressure {t}: {s} with {d} connections in all (mark {d} of the others): want {s}\n", .{ tr, what, n, mark, if (want) "under pressure" else "no pressure" });
        return error.PressureNotReached;
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
    var started: Started = .{};
    defer started.joinAll();

    // Control: alone on the server, never under pressure.
    var ctl: Client = .{ .gpa = gpa, .name = "control", .tr = tr, .v6 = false, .port = p4 };
    const at_ctl = try rig.begin(&started, &ctl, 1);
    try started.finish();
    std.debug.print("idle-pressure {t}: control {d} ms ({d} in all)\n", .{ tr, ctl.elapsed_ms, at_ctl });
    try expectPressure(tr, "control", at_ctl, false);
    try expectWindow(tr, &ctl, normal_ms, "no pressure: the normal limit");
    try rig.waitConnections(0);

    // A1, A2 and A4 go idle with fewer others than the mark and stay idle:
    // the accepted tail. A3 goes idle the same way, then makes a request
    // once the server is under pressure, so its next idle wait starts
    // under it. B1 and B2 (::1) are the first connections under pressure.
    var go: std.atomic.Value(bool) = .init(false);
    const conns = &rig.server.active_connections;
    var a1: Client = .{ .gpa = gpa, .name = "A1 (idle before pressure)", .tr = tr, .v6 = false, .port = p4 };
    var a2: Client = .{ .gpa = gpa, .name = "A2 (idle before pressure)", .tr = tr, .v6 = false, .port = p4 };
    var a3: Client = .{ .gpa = gpa, .name = "A3 (idle again under pressure)", .tr = tr, .v6 = false, .port = p4, .request_on = &go, .server_connections = conns };
    var a4: Client = .{ .gpa = gpa, .name = "A4 (idle before pressure)", .tr = tr, .v6 = false, .port = p4 };
    var b1: Client = .{ .gpa = gpa, .name = "B1 (idle under pressure)", .tr = tr, .v6 = true, .port = p6 };
    var b2: Client = .{ .gpa = gpa, .name = "B2 (idle under pressure)", .tr = tr, .v6 = true, .port = p6 };
    var at: [6]usize = undefined;
    for ([_]*Client{ &a1, &a2, &a3, &a4, &b1, &b2 }, 0..) |c, i| at[i] = try rig.begin(&started, c, i + 1);
    go.store(true, .release);
    try started.finish();

    std.debug.print("idle-pressure {t}: mark {d} of {d} (others); before pressure A1 {d} ms, A2 {d} ms, A4 {d} ms ({d}, {d}, {d} in all); under pressure B1 {d} ms, B2 {d} ms ({d}, {d} in all); A3 {d} ms after its stream ended ({d} in all)\n", .{ tr, mark, max_connections, a1.elapsed_ms, a2.elapsed_ms, a4.elapsed_ms, at[0], at[1], at[3], b1.elapsed_ms, b2.elapsed_ms, at[4], at[5], a3.elapsed_ms, a3.connections_after_response });
    // The preconditions: a run that never reached pressure, or reached it
    // too early, tested nothing.
    for ([_]usize{ 0, 1, 2, 3 }) |i| try expectPressure(tr, "an A connection at its start", at[i], false);
    try expectPressure(tr, "B1", at[4], true);
    try expectPressure(tr, "B2", at[5], true);
    try expectPressure(tr, "A3 after its response", a3.connections_after_response, true);
    try expectWindow(tr, &a1, normal_ms, "idle before pressure keeps its deadline");
    try expectWindow(tr, &a2, normal_ms, "idle before pressure keeps its deadline");
    try expectWindow(tr, &a4, normal_ms, "idle before pressure keeps its deadline");
    try expectWindow(tr, &b1, short_ms, "idle wait started under pressure");
    try expectWindow(tr, &b2, short_ms, "idle wait started under pressure");
    try expectWindow(tr, &a3, short_ms, "a new idle wait started under pressure");
}

// ---------------------------------------------------------------------------
// HTTP/1.1: the wait for the next request, under pressure.

fn h1Run(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    // HTTP/2 connections from 127.0.0.1 hold the server at the mark; the
    // HTTP/1.1 client is ::1, the first connection under pressure.
    var rig: Rig = undefined;
    try rig.start(rt, gpa, .h2c, tr);
    defer rig.stop();
    var started: Started = .{};
    defer started.joinAll();
    var fill: [mark]Client = undefined;
    for (&fill, 0..) |*f, i| {
        f.* = .{ .gpa = gpa, .name = "filler", .tr = .h2c, .v6 = false, .port = rig.port(0) };
        _ = try rig.begin(&started, f, i + 1);
    }
    var c: Client = .{ .gpa = gpa, .name = "keep-alive", .tr = tr, .v6 = true, .port = rig.port(1) };
    const at = try rig.begin(&started, &c, mark + 1);
    try started.finish();
    std.debug.print("idle-pressure {t}: the wait for the next request closed after {d} ms with {d} connections in all (mark {d} of the others); field_block_timeout {d} ms, pressure limit {d} ms\n", .{ tr, c.elapsed_ms, at, mark, field_block_ms, short_ms });
    try expectPressure(tr, "the keep-alive connection", at, true);
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
