//! Route shares under pressure (t-3759 step 5), on real sockets.
//!
//! The rule (`accounts.RouteTable` has it in full): a task route holds its
//! task handlers (orphans included), the request-pool bytes they hold and
//! their compression contexts. While the server's use of one of these is at
//! or above that resource's pressure mark (L/4), a charge that would take
//! the route past its share (L over the number of task routes, or the
//! route's override) is refused: the handler or its bytes with a 503, on
//! HTTP/2 and HTTP/1.1 alike, the compression context with identity
//! encoding. Below the mark nothing is refused by a route's share.
//!
//! Every attack comes from several PROXY v2 sources, each below its client
//! share, so no client share binds: what refuses is the route. Each test
//! prints the route's refusal counters and the client share refusals, so an
//! attack that never engaged shows as a zero. Every server ends with
//! `Server.deinit`, which checks that every client account and every route
//! account is back to zero and logs an error otherwise, which fails the
//! test; each test also checks the route counts at zero itself after its
//! orphans returned.
//!
//! The file compiles against 35a2f7b, the commit before route shares, where
//! every attack test fails: `@hasDecl` and `@hasField` keep the route reads
//! out there.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");
const serve_deadline = @import("serve_deadline.zig");

const accounts = starh2.edge.accounts;
const proxy = starh2.edge.proxy_protocol;
const hpack = starh2.core.hpack;
const frame = starh2.core.frame;
const Wire = wire_client.Wire;
const Peer = starh2.Peer;

const ms = std.time.ns_per_ms;
const kib = 1024;
const dummy: u8 = 0;

/// True from this step on; false at 35a2f7b.
const has_routes = @hasDecl(accounts, "RouteTable");

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

// ---------------------------------------------------------------------------
// Handlers.

/// Set to let every held handler return.
var release: std.atomic.Value(bool) = .init(false);
/// Set to make every held `/hold` orphan ask for one more block, once.
var grow: std.atomic.Value(bool) = .init(false);
var started: std.atomic.Value(usize) = .init(0);
/// `/hold` handlers that ran at all, counted before their first allocation.
var entered: std.atomic.Value(usize) = .init(0);
var ended: std.atomic.Value(usize) = .init(0);
/// `/stuck` handlers past `resp.start`: holding their slot until `release`.
var holding: std.atomic.Value(usize) = .init(0);
var grown: std.atomic.Value(usize) = .init(0);
var grow_refused: std.atomic.Value(usize) = .init(0);

// ---------------------------------------------------------------------------
// A per-request timeline (t-3876): what each client saw and what the server
// did, keyed by an `x-id` header the client sends.

const trace_max = 64;
const ClientSaw = enum(u8) { none, status, timeout, eof, err };
const ClientTrace = struct {
    src: Peer = .unknown,
    /// A request to the broken route, `/stuck` (not the defender's `/ok`).
    stuck: bool = false,
    sent_ns: u64 = 0,
    saw: ClientSaw = .none,
    status: u16 = 0,
    errno: i32 = 0,
    bytes: usize = 0,
    rst: ?u32 = null,
    at_ns: u64 = 0,
    /// The route's task handlers right after the answer was read.
    route_then: usize = 0,
};
var trace_t0: u64 = 0;
var trace_next: usize = 0;
var client_trace: [trace_max]ClientTrace = @splat(.{});
var trace_rig: ?*Rig = null;
var srv_entered_ns: [trace_max]std.atomic.Value(u64) = @splat(.init(0));
var srv_start_err: [trace_max]std.atomic.Value(u16) = @splat(.init(0));
var srv_flush_err: [trace_max]std.atomic.Value(u16) = @splat(.init(0));
var srv_ended_ns: [trace_max]std.atomic.Value(u64) = @splat(.init(0));

fn resetTrace() void {
    trace_t0 = nowNs();
    trace_next = 0;
    client_trace = @splat(.{});
    for (0..trace_max) |i| {
        srv_entered_ns[i].store(0, .release);
        srv_start_err[i].store(0, .release);
        srv_flush_err[i].store(0, .release);
        srv_ended_ns[i].store(0, .release);
    }
}

fn traceMs(t: u64) u64 {
    return if (t == 0) 0 else (t -| trace_t0) / ms;
}

fn reqId(req: *const starh2.Request) ?usize {
    for (req.headers) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, "x-id")) {
            const id = std.fmt.parseInt(usize, h.value, 10) catch return null;
            return if (id < trace_max) id else null;
        }
    }
    return null;
}

/// Every request of the run, client view and server view, side by side.
fn printTimeline(label: []const u8) void {
    std.debug.print("routes timeline ({s}): {d} requests\n", .{ label, trace_next });
    for (client_trace[0..trace_next], 0..) |c, i| {
        const start_err = srv_start_err[i].load(.acquire);
        const flush_err = srv_flush_err[i].load(.acquire);
        std.debug.print("  id {d:>2} src {f}: sent {d} ms; client {s} status {d} errno {d} bytes {d} rst {?d} at {d} ms, route then {d}; server entered {d} ms, start {s}, flush {s}, ended {d} ms\n", .{
            i,                                                                  fmtPeer(c.src),                            traceMs(c.sent_ns),
            @tagName(c.saw),                                                    c.status,                                  c.errno,
            c.bytes,                                                            c.rst,                                     traceMs(c.at_ns),
            c.route_then,                                                       traceMs(srv_entered_ns[i].load(.acquire)), if (start_err == 0) "ok" else @errorName(@errorFromInt(start_err)),
            if (flush_err == 0) "ok" else @errorName(@errorFromInt(flush_err)), traceMs(srv_ended_ns[i].load(.acquire)),
        });
    }
}

/// Each `/stuck` request, classified by what happened on both sides: a
/// client's 200 must be a handler that entered, got past `resp.start` and
/// still runs; a 503 must be a request whose handler never entered.
/// Anything else is a mismatch.
const PerRequest = struct {
    stuck: usize = 0,
    held: usize = 0,
    refused: usize = 0,
    start_failed: usize = 0,
    mismatches: usize = 0,
};

fn perRequest() PerRequest {
    var r: PerRequest = .{};
    for (client_trace[0..trace_next], 0..) |c, i| {
        if (!c.stuck) continue;
        r.stuck += 1;
        const entered_ns = srv_entered_ns[i].load(.acquire);
        const start_err = srv_start_err[i].load(.acquire);
        const ended_ns = srv_ended_ns[i].load(.acquire);
        if (start_err != 0) r.start_failed += 1;
        if (c.saw == .status and c.status == 200 and entered_ns != 0 and start_err == 0 and ended_ns == 0) {
            r.held += 1;
        } else if (c.saw == .status and c.status == 503 and entered_ns == 0) {
            r.refused += 1;
        } else r.mismatches += 1;
    }
    return r;
}

/// Before a source's connections close: every request the client saw
/// answered 200 has a handler past `resp.start`. A handler can still be inside
/// `start` after its head reached the client: its socket write is a race of
/// the send and a timeout timer, and the wait ends one loop turn after the
/// send, when the timer's cancel is processed. A close in that turn was seen
/// to end `start` with ConnectionClosed; the handler then returns, its route
/// charge goes back, and the next source is admitted in its place (t-3876).
/// The server is right; the test closed too soon.
///
/// It runs on a zio task, so it waits with `zio.sleep`: a blocking sleep here
/// stops this executor's loop, and a handler whose write waits on that loop
/// does not get past `start` until the sleep ends (t-3878).
fn awaitHolding(admitted: usize) !void {
    const deadline = nowNs() + 2 * std.time.ns_per_s;
    while (holding.load(.acquire) < admitted) {
        if (nowNs() >= deadline) {
            std.debug.print("routes: {d} handlers past resp.start, but the clients saw {d} admitted\n", .{ holding.load(.acquire), admitted });
            printTimeline("awaiting handlers");
            return error.HandlersNotHolding;
        }
        zio.sleep(.fromMilliseconds(1)) catch {};
    }
}

fn resetHandlers() void {
    resetTrace();
    release.store(false, .release);
    grow.store(false, .release);
    started.store(0, .release);
    holding.store(0, .release);
    entered.store(0, .release);
    ended.store(0, .release);
    grown.store(0, .release);
    grow_refused.store(0, .release);
}

/// Shielded, so it ignores cancel: when its connection ends it is an orphan
/// that does not return until `release`. `on_grow` runs once, after `grow`.
fn holdShielded(comptime on_grow: ?fn (*const starh2.Request) void, req: *const starh2.Request) void {
    zio.beginShield();
    defer zio.endShield();
    var did_grow = false;
    while (!release.load(.acquire)) {
        if (on_grow) |g| {
            if (!did_grow and grow.load(.acquire)) {
                did_grow = true;
                g(req);
            }
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

/// The broken route: answers its head at once (so the client can tell it
/// was admitted from a 503), then never returns until `release`.
fn stuck(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const id = reqId(req);
    if (id) |i| srv_entered_ns[i].store(nowNs(), .release);
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    defer if (id) |i| srv_ended_ns[i].store(nowNs(), .release);
    var body = resp.start(200, &.{}) catch |err| {
        if (id) |i| srv_start_err[i].store(@intFromError(err), .release);
        return err;
    };
    _ = holding.fetchAdd(1, .acq_rel);
    body.flush() catch |err| {
        if (id) |i| srv_flush_err[i].store(@intFromError(err), .release);
    };
    holdShielded(null, req);
    body.finish() catch {};
}

/// A healthy route: answers and returns.
fn ok(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

fn growOne(req: *const starh2.Request) void {
    // 30 KiB does not fit what is left of the 128 KiB block, so it takes a
    // new 64 KiB block: one more charge to the route, while orphaned.
    if (req.arena.alloc(u8, 30 * kib)) |b| {
        @memset(b, 'g');
        _ = grown.fetchAdd(1, .acq_rel);
    } else |_| {
        _ = grow_refused.fetchAdd(1, .acq_rel);
    }
}

/// Holds its first block and a 128 KiB block (100 KiB asked): 192 KiB of
/// request memory. A refused allocation fails the handler before it
/// answered, so its fallback answers the refusal's status (503). Then held,
/// shielded; after `grow` it asks for one more 64 KiB block.
fn hold(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = entered.fetchAdd(1, .acq_rel);
    const b = try req.arena.alloc(u8, 100 * kib);
    @memset(b, 'h');
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    var body = try resp.start(200, &.{});
    body.flush() catch {};
    holdShielded(growOne, req);
    body.finish() catch {};
}

/// A compressible streaming response held open: its brotli context (or its
/// identity encoding, when a share refused one) stays until it returns.
fn brotliHeld(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    var body = try resp.start(200, &.{.{ .name = "content-type", .value = "text/plain" }});
    body.writeAll("compressible text, held open until the test lets it finish") catch {};
    body.flush() catch {};
    holdShielded(null, req);
    body.finish() catch {};
}

fn task(comptime f: anytype) starh2.Handler {
    return .{ .task = .{ .ptr = @constCast(&dummy), .runFn = f } };
}

// ---------------------------------------------------------------------------
// The server: one PROXY v2 endpoint on 127.0.0.1, every identity a header.

const Tr = enum { h2c, tls, h1c };

const trusted_loopback = [_]proxy.Cidr{.{ .addr = .{ .ip4 = .{ 127, 0, 0, 1 } }, .prefix_bits = 32 }};

const Rig = struct {
    gpa: std.mem.Allocator,
    server: starh2.Server,
    handle: zio.JoinHandle(starh2.ServeError!void),
    cert: []u8 = &.{},
    key: []u8 = &.{},

    fn start(self: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr, routes: []const starh2.Route, limits: starh2.Limits) !void {
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
        const t: starh2.EndpointConfig.Tcp = .{ .address = a4, .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &trusted_loopback } } };
        const ep: starh2.EndpointConfig = switch (tr) {
            .h2c => .{ .h2c_prior_knowledge = t },
            .tls => .{ .tls = t },
            .h1c => .{ .h1c = t },
        };
        self.server = try starh2.Server.init(gpa, io, .{
            .endpoints = &.{ep},
            .routes = routes,
            .tls = tls_cfg,
            .limits = limits,
        });
        self.handle = try rt.spawn(starh2.Server.serve, .{ &self.server, gpa });
        try self.server.waitUntilListening(5 * std.time.ns_per_s);
    }

    fn port(self: *Rig) u16 {
        return self.server.localAddress(0).getPort();
    }

    fn stop(self: *Rig) void {
        release.store(true, .release);
        self.server.requestShutdown();
        serve_deadline.join(&self.server, &self.handle);
        self.server.deinit(self.gpa);
        if (self.cert.len != 0) self.gpa.free(self.cert);
        if (self.key.len != 0) self.gpa.free(self.key);
    }

    /// What route `route` holds of `res` (0 at 35a2f7b, which has no route
    /// accounts).
    fn routeCount(self: *Rig, route: u32, res: Res) usize {
        if (comptime !has_routes) return 0;
        const rec = self.server.accounting.routes.recordOf(route) orelse return 0;
        return rec.count(@fromBackingInt(@intCast(@backingInt(res))));
    }

    fn routeRefusals(self: *Rig, route: u32, res: Res) u64 {
        if (comptime !has_routes) return 0;
        const rec = self.server.accounting.routes.recordOf(route) orelse return 0;
        return rec.refusals[@backingInt(res)].load(.acquire);
    }

    fn routeShare(self: *Rig, route: u32, res: Res) usize {
        if (comptime !has_routes) return 0;
        const rec = self.server.accounting.routes.recordOf(route) orelse return 0;
        return rec.shares[@backingInt(res)];
    }

    fn clientRefusals(self: *Rig, res: accounts.Resource) u64 {
        return self.server.clients.shareRefusals(res);
    }

    fn routeUnderflows(self: *Rig) u64 {
        if (comptime !has_routes) return 0;
        return self.server.accounting.routes.underflows.load(.acquire);
    }
};

/// The route resources, by `accounts.RouteResource`'s order (which does not
/// exist at 35a2f7b).
const Res = enum(u2) { task_handlers, request_bytes, compression };

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

fn waitFor(comptime what: []const u8, ctx: anytype, comptime done: fn (@TypeOf(ctx)) bool, timeout_ns: u64) !void {
    const deadline = nowNs() + timeout_ns;
    while (!done(ctx)) {
        if (nowNs() >= deadline) {
            std.debug.print("routes: timed out waiting for {s}\n", .{what});
            return error.StateNotReached;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

/// `waitFor` that only waits: the row prints its counters and its checks
/// fail after, so an attack that never engaged (at 35a2f7b) is still shown.
fn settle(ctx: anytype, comptime done: fn (@TypeOf(ctx)) bool, timeout_ns: u64) void {
    const deadline = nowNs() + timeout_ns;
    while (!done(ctx) and nowNs() < deadline) zio.sleep(.fromMilliseconds(2)) catch {};
}

fn expectExactly(comptime what: []const u8, got: anytype, want: anytype) !void {
    if (got == want) return;
    std.debug.print("routes: {s} is {d}, want {d}\n", .{ what, got, want });
    return error.WrongCount;
}

/// A PROXY v2 source: 198.51.100.N.
fn source(n: u8) Peer {
    return .{ .ip4 = .{ 198, 51, 100, n } };
}

const defender: Peer = .{ .ip4 = .{ 203, 0, 113, 1 } };

const no_headers: []const hpack.HeaderField = &.{};

// ---------------------------------------------------------------------------
// Clients, on blocking threads.

/// One HTTP/2 connection past its PROXY header (and for TLS, its handshake),
/// and what it saw, stream by stream (ids 1, 3, 5...).
const H2 = struct {
    w: *Wire,
    dec: hpack.Decoder,
    next_sid: u31 = 1,
    status: [max_streams]?u16 = @splat(null),
    rst: [max_streams]?u32 = @splat(null),
    brotli: [max_streams]bool = @splat(false),
    closed: bool = false,

    const max_streams = 32;

    fn open(gpa: std.mem.Allocator, tr: Tr, port: u16, src: Peer) !H2 {
        std.debug.assert(tr != .h1c);
        const w = try Wire.connect(gpa, if (tr == .tls) .tls else .h2c, port);
        errdefer w.close();
        var buf: [proxy.fixed_bytes + 64]u8 = undefined;
        const n = proxy.encode(&buf, .{ .peer = src });
        try w.sendRaw(buf[0..n]);
        if (tr == .tls) try w.handshake();
        var self: H2 = .{ .w = w, .dec = try hpack.Decoder.init(gpa) };
        var pre = try h2c.buildClientPrefaceAndSettings(gpa);
        defer pre.deinit(gpa);
        try h2c.appendWindowUpdate(gpa, &pre, 0, 1 << 30);
        self.w.send(pre.items) catch {
            self.closed = true;
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
        self.w.send(req.items) catch {
            self.closed = true;
        };
        return sid;
    }

    /// A POST with `body`, in DATA frames of 16 KiB.
    fn post(self: *H2, gpa: std.mem.Allocator, path: []const u8, body: []const u8) !u31 {
        const sid = self.next_sid;
        self.next_sid += 2;
        var req: std.ArrayList(u8) = .empty;
        defer req.deinit(gpa);
        try h2c.appendHeadersExtra(gpa, &req, sid, "POST", path, false, &.{});
        var off: usize = 0;
        while (off < body.len) {
            const take = @min(body.len - off, 16 * kib);
            try h2c.appendData(gpa, &req, sid, body[off..][0..take], off + take == body.len);
            off += take;
        }
        self.w.send(req.items) catch {
            self.closed = true;
        };
        return sid;
    }

    /// Read until every stream of `sids` has a status, or for `for_ms`.
    fn pollFor(self: *H2, sids: []const u31, for_ms: u64) !void {
        const deadline = nowNs() + for_ms * ms;
        while (!self.closed) {
            var all = true;
            for (sids) |s| {
                if (self.status[slot(s)] == null and self.rst[slot(s)] == null) all = false;
            }
            if (all) return;
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
            const i: usize = if (seen.sid == 0 or seen.sid % 2 == 0) max_streams else slot(@intCast(seen.sid));
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

/// A raw blocking socket to 127.0.0.1, its PROXY header sent.
fn rawProxied(port: u16, src: Peer) !std.c.fd_t {
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.Socket;
    errdefer _ = std.c.close(fd);
    var addr: std.c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
    var buf: [proxy.fixed_bytes + 64]u8 = undefined;
    const n = proxy.encode(&buf, .{ .peer = src });
    if (!rawSend(fd, buf[0..n])) return error.Send;
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

/// What the client read on `fd` within `for_ms`: a status line, or the
/// connection's end (EOF, an error), or nothing.
const RawAnswer = struct { saw: ClientSaw, status: u16 = 0, errno: i32 = 0, bytes: usize = 0 };

fn rawAnswer(fd: std.c.fd_t, for_ms: u64) RawAnswer {
    var got: [64]u8 = undefined;
    var len: usize = 0;
    const deadline = nowNs() + for_ms * ms;
    while (len < 12) {
        const left = deadline -| nowNs();
        if (left == 0) return .{ .saw = .timeout, .bytes = len };
        var pfd = [_]std.c.pollfd{.{ .fd = fd, .events = std.c.POLL.IN, .revents = 0 }};
        const r = std.c.poll(&pfd, 1, @intCast(@max(1, left / ms)));
        if (r <= 0) continue;
        const n = std.c.recv(fd, got[len..].ptr, got.len - len, 0);
        if (n == 0) return .{ .saw = .eof, .bytes = len };
        if (n < 0) return .{ .saw = .err, .errno = @backingInt(std.posix.errno(n)), .bytes = len };
        len += @intCast(n);
    }
    if (!std.mem.startsWith(u8, got[0..len], "HTTP/1.1 ")) return .{ .saw = .status, .status = 0, .bytes = len };
    return .{ .saw = .status, .status = std.fmt.parseInt(u16, got[9..12], 10) catch 0, .bytes = len };
}

/// The status of the first response on `fd` within `for_ms`, or null.
fn rawStatus(fd: std.c.fd_t, for_ms: u64) ?u16 {
    var got: [64]u8 = undefined;
    var len: usize = 0;
    const deadline = nowNs() + for_ms * ms;
    while (len < 12) {
        const left = deadline -| nowNs();
        if (left == 0) return null;
        var pfd = [_]std.c.pollfd{.{ .fd = fd, .events = std.c.POLL.IN, .revents = 0 }};
        const r = std.c.poll(&pfd, 1, @intCast(@max(1, left / ms)));
        if (r <= 0) continue;
        const n = std.c.recv(fd, got[len..].ptr, got.len - len, 0);
        if (n <= 0) return null;
        len += @intCast(n);
    }
    if (!std.mem.startsWith(u8, got[0..len], "HTTP/1.1 ")) return 0;
    return std.fmt.parseInt(u16, got[9..12], 10) catch 0;
}

/// What a row's attempts were answered: admitted (the handler's 200), a 503,
/// or anything else (which fails the row).
const Tally = struct {
    admitted: usize = 0,
    refused_503: usize = 0,
    /// HTTP/2 only: an RST_STREAM REFUSED_STREAM, which a route refusal must
    /// never be.
    refused_stream: usize = 0,
    other: usize = 0,

    fn note(self: *Tally, status: ?u16, rst: ?u32) void {
        if (status) |s| {
            if (s == 200) self.admitted += 1 else if (s == 503) self.refused_503 += 1 else self.other += 1;
        } else if (rst) |code| {
            if (code == 0x7) self.refused_stream += 1 else self.other += 1;
        } else self.other += 1;
    }
};

/// Every connection a row opened, kept open until the row lets go of them.
const Conns = struct {
    gpa: std.mem.Allocator,
    h2: [64]?H2 = @splat(null),
    raw: [64]std.c.fd_t = @splat(-1),
    n_h2: usize = 0,
    n_raw: usize = 0,

    fn addH2(self: *Conns, c: H2) *H2 {
        self.h2[self.n_h2] = c;
        self.n_h2 += 1;
        return &self.h2[self.n_h2 - 1].?;
    }

    fn addRaw(self: *Conns, fd: std.c.fd_t) void {
        self.raw[self.n_raw] = fd;
        self.n_raw += 1;
    }

    fn closeAll(self: *Conns) void {
        for (self.h2[0..self.n_h2]) |*c| {
            if (c.*) |*h| h.deinit();
            c.* = null;
        }
        for (self.raw[0..self.n_raw]) |*fd| {
            if (fd.* >= 0) _ = std.c.close(fd.*);
            fd.* = -1;
        }
        self.n_h2 = 0;
        self.n_raw = 0;
    }
};

/// `n` requests to `path` from `src`, every answer read: on HTTP/2 one
/// connection with `n` streams, on HTTP/1.1 one connection each. The
/// connections stay open in `conns`.
fn attempts(conns: *Conns, tr: Tr, port: u16, src: Peer, path: []const u8, extra: []const hpack.HeaderField, n: usize, out: *Tally, brotli: ?*usize) void {
    attemptsInner(conns, tr, port, src, path, extra, n, out, brotli) catch |err| {
        std.debug.print("routes: a client from {f} failed: {s}\n", .{ fmtPeer(src), @errorName(err) });
        out.other += n;
    };
}

fn attemptsInner(conns: *Conns, tr: Tr, port: u16, src: Peer, path: []const u8, extra: []const hpack.HeaderField, n: usize, out: *Tally, brotli: ?*usize) !void {
    switch (tr) {
        .h2c, .tls => {
            const h = conns.addH2(try H2.open(conns.gpa, tr, port, src));
            var sids: [H2.max_streams]u31 = undefined;
            var ids: [H2.max_streams]?usize = @splat(null);
            for (sids[0..n], 0..) |*s, k| {
                var with_id: [16]hpack.HeaderField = undefined;
                var id_buf: [8]u8 = undefined;
                const id = traceBegin(src, path);
                ids[k] = id;
                for (extra, 0..) |f, j| with_id[j] = f;
                var m = extra.len;
                if (id) |i| {
                    with_id[m] = .{ .name = "x-id", .value = try std.fmt.bufPrint(&id_buf, "{d}", .{i}) };
                    m += 1;
                }
                s.* = try h.get(conns.gpa, path, with_id[0..m]);
            }
            try h.pollFor(sids[0..n], 2000);
            for (sids[0..n], 0..) |s, k| {
                if (ids[k]) |i| {
                    const st = h.status[H2.slot(s)];
                    client_trace[i].saw = if (st != null) .status else .none;
                    client_trace[i].status = st orelse 0;
                    client_trace[i].rst = h.rst[H2.slot(s)];
                    client_trace[i].at_ns = nowNs();
                    client_trace[i].route_then = if (trace_rig) |r| r.routeCount(0, .task_handlers) else 0;
                }
                out.note(h.status[H2.slot(s)], h.rst[H2.slot(s)]);
                if (brotli) |b| {
                    if (h.brotli[H2.slot(s)]) b.* += 1;
                }
            }
        },
        .h1c => {
            for (0..n) |_| {
                const fd = try rawProxied(port, src);
                conns.addRaw(fd);
                const id = traceBegin(src, path);
                var req_buf: [256]u8 = undefined;
                const req = if (id) |i|
                    try std.fmt.bufPrint(&req_buf, "GET {s} HTTP/1.1\r\nHost: localhost\r\nx-id: {d}\r\n\r\n", .{ path, i })
                else
                    try std.fmt.bufPrint(&req_buf, "GET {s} HTTP/1.1\r\nHost: localhost\r\n\r\n", .{path});
                _ = rawSend(fd, req);
                const a = rawAnswer(fd, 2000);
                if (id) |i| {
                    client_trace[i].saw = a.saw;
                    client_trace[i].status = a.status;
                    client_trace[i].errno = a.errno;
                    client_trace[i].bytes = a.bytes;
                    client_trace[i].at_ns = nowNs();
                    client_trace[i].route_then = if (trace_rig) |r| r.routeCount(0, .task_handlers) else 0;
                }
                out.note(if (a.saw == .status) a.status else null, null);
            }
        },
    }
}

/// The next request id for `src`, while tracing a run (`trace_rig` set) and
/// ids remain; its send time is now.
fn traceBegin(src: Peer, path: []const u8) ?usize {
    if (trace_rig == null or trace_next >= trace_max) return null;
    const i = trace_next;
    trace_next += 1;
    client_trace[i].src = src;
    client_trace[i].stuck = std.mem.eql(u8, path, "/stuck");
    client_trace[i].sent_ns = nowNs();
    return i;
}

/// One request to `/ok` from the defender on a connection of its own,
/// closed after. True when it was answered 200.
fn served(gpa: std.mem.Allocator, tr: Tr, port: u16) bool {
    var c: Conns = .{ .gpa = gpa };
    defer c.closeAll();
    var t: Tally = .{};
    attempts(&c, tr, port, defender, "/ok", no_headers, 1, &t, null);
    return t.admitted == 1;
}

fn fmtPeer(p: Peer) std.fmt.Alt(Peer, formatPeer) {
    return .{ .data = p };
}

fn formatPeer(p: Peer, w: *std.Io.Writer) std.Io.Writer.Error!void {
    switch (p) {
        .ip4 => |a| try w.print("{d}.{d}.{d}.{d}", .{ a[0], a[1], a[2], a[3] }),
        else => try w.writeAll("(peer)"),
    }
}

fn orphansAre(n: usize) type {
    return struct {
        fn f(srv: *starh2.Server) bool {
            return srv.accounting.orphaned_handlers.load(.acquire) == n;
        }
    };
}

/// Let every handler go, close every connection, and wait until nothing is
/// held: no task handler (orphans included), and no route charge.
fn letGo(rig: *Rig, conns: []const *Conns) !void {
    release.store(true, .release);
    for (conns) |c| try blocking(Conns.closeAll, .{c});
    try waitFor("every handler to return", &rig.server, struct {
        fn f(srv: *starh2.Server) bool {
            return srv.accounting.task_handlers.load(.acquire) == 0 and srv.accounting.orphaned_handlers.load(.acquire) == 0;
        }
    }.f, 10 * std.time.ns_per_s);
    // A link gives the server's token back before its route's
    // (`GlobalAccounting.releaseTaskHandler`: the reverse of the charge
    // order), so the server reading zero does not mean the route does yet.
    // Its arena and its encoder go back earlier (`dropLink`,
    // `finishHandlerJob`), but every count of every route is waited for, so
    // no order of releases can make this read early. Without failing here:
    // `expectRoutesEmpty` then names any count still left.
    settle(rig, routesQuiet, 3 * std.time.ns_per_s);
}

fn routesQuiet(rig: *Rig) bool {
    if (comptime !has_routes) return true;
    const n: u32 = @intCast(rig.server.routes.len);
    for (0..n) |i| {
        inline for (.{ Res.task_handlers, Res.request_bytes, Res.compression }) |res| {
            if (rig.routeCount(@intCast(i), res) != 0) return false;
        }
    }
    return true;
}

/// Every route count back to zero, and no route release refused.
fn expectRoutesEmpty(rig: *Rig, routes: usize) !void {
    for (0..routes) |i| {
        inline for (.{ Res.task_handlers, Res.request_bytes, Res.compression }) |res| {
            const v = rig.routeCount(@intCast(i), res);
            if (v != 0) {
                std.debug.print("routes: route {d} still holds {s} = {d} after its handlers returned\n", .{ i, @tagName(res), v });
                return error.RouteStillCharged;
            }
        }
    }
    try expectExactly("route underflows", rig.routeUnderflows(), 0);
}

// ---------------------------------------------------------------------------
// The broken route: task handlers.

const broken_routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/stuck", .handler = task(stuck) },
    .{ .method = .GET, .path = "/ok", .handler = task(ok) },
};

/// 64 task handlers: the mark is 16, a client's share 8, and each of the two
/// task routes' share 32. Eight sources send five requests each to the
/// broken route, one source at a time; each connection is closed once its
/// answers are read, so every admitted handler is an orphan from then on.
/// No source holds more than 5, below its share of 8. The route passes the
/// mark at 16 and is refused from 32 on: 32 admitted, 8 answered 503. The
/// second route is served after every source, and after the attack.
fn brokenRoute(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    resetHandlers();
    var limits = testLimits();
    limits.max_task_handlers = 64;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, &broken_routes, limits);
    defer rig.stop();
    trace_rig = &rig;
    defer trace_rig = null;
    var a: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
    }
    var tally: Tally = .{};
    var defended: usize = 0;
    for (1..9) |i| {
        try blocking(attempts, .{ &a, tr, rig.port(), source(@intCast(i)), "/stuck", no_headers, 5, &tally, null });
        try awaitHolding(tally.admitted);
        try blocking(Conns.closeAll, .{&a});
        var ok_now = false;
        try blocking(servedInto, .{ gpa, tr, rig.port(), &ok_now });
        if (ok_now) defended += 1;
    }
    settle(&rig.server, orphansAre(32).f, 5 * std.time.ns_per_s);
    var after: usize = 0;
    for (0..3) |_| {
        var ok_now = false;
        try blocking(servedInto, .{ gpa, tr, rig.port(), &ok_now });
        if (ok_now) after += 1;
    }
    const held = rig.routeCount(0, .task_handlers);
    std.debug.print("routes broken route ({s}): 40 requests from 8 sources: {d} admitted, {d} answered 503, {d} REFUSED_STREAM, {d} other; route holds {d} task handlers (share {d}), {d} orphans; route refusals {d}, client share refusals {d}; server task handlers {d}; the second route served {d} of 8 during and {d} of 3 after\n", .{
        @tagName(tr),
        tally.admitted,
        tally.refused_503,
        tally.refused_stream,
        tally.other,
        held,
        rig.routeShare(0, .task_handlers),
        rig.server.accounting.orphaned_handlers.load(.acquire),
        rig.routeRefusals(0, .task_handlers),
        rig.clientRefusals(.task_handlers),
        rig.server.accounting.task_handlers.load(.acquire),
        defended,
        after,
    });
    const pr = perRequest();
    std.debug.print("routes broken route ({s}): per request: {d} to the broken route, {d} answered 200 with a handler past start and still running, {d} answered 503 with no handler, {d} mismatches; handlers whose start failed {d}\n", .{ @tagName(tr), pr.stuck, pr.held, pr.refused, pr.mismatches, pr.start_failed });
    if (pr.mismatches != 0 or tally.admitted != 32 or tally.refused_503 != 8 or tally.other != 0 or held != 32 or rig.routeRefusals(0, .task_handlers) != 8) printTimeline(@tagName(tr));
    try expectExactly("requests whose two sides disagree", pr.mismatches, 0);
    try expectExactly("requests to the broken route", pr.stuck, 40);
    try expectExactly("orphans", rig.server.accounting.orphaned_handlers.load(.acquire), 32);
    try expectExactly("admitted to the broken route", tally.admitted, 32);
    try expectExactly("answered 503", tally.refused_503, 8);
    try expectExactly("answered REFUSED_STREAM", tally.refused_stream, 0);
    try expectExactly("other answers", tally.other, 0);
    try expectExactly("the route's task handlers", held, 32);
    try expectExactly("route refusals", rig.routeRefusals(0, .task_handlers), 8);
    try expectExactly("client share refusals", rig.clientRefusals(.task_handlers), 0);
    try expectExactly("the second route served during the attack", defended, 8);
    try expectExactly("the second route served after the attack", after, 3);
    try letGo(&rig, &.{&a});
    try expectRoutesEmpty(&rig, broken_routes.len);
}

fn brokenH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return brokenRoute(rt, gpa, .h2c);
}
fn brokenTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return brokenRoute(rt, gpa, .tls);
}
fn brokenH1c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return brokenRoute(rt, gpa, .h1c);
}

test "route broken route (h2c): orphans from many sources are refused 503 at the route's share; a second route is served throughout" {
    try runIn(brokenH2c);
}
test "route broken route (TLS): orphans from many sources are refused 503 at the route's share; a second route is served throughout" {
    try runIn(brokenTls);
}
test "route broken route (HTTP/1.1): orphans from many sources are refused 503 at the route's share; a second route is served throughout" {
    try runIn(brokenH1c);
}

/// IPv6 source `i` (1..8): its own /64, two /64s per /48
/// (2001:db8:J:i::5 with J = (i + 1) / 2).
fn source6(i: usize) [16]u8 {
    var a: [16]u8 = .{ 0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5 };
    a[5] = @intCast((i + 1) / 2);
    a[7] = @intCast(i);
    return a;
}

fn clientCount(rig: *Rig, key: accounts.Key) usize {
    const e = rig.server.clients.lookup(key) orelse return 0;
    return e.count(.task_handlers);
}

/// The broken route attacked from IPv6 sources: each charge names a /64 and
/// its /48 aggregate. Eight /64s in four /48s send five requests each, as in
/// `brokenRoute`: 32 admitted, 8 refused by the route. The route's refusal
/// must undo the /64's charge and the /48's alike: after the attack each /64
/// holds exactly what its source was admitted, and each /48 the sum of its
/// two /64s. Neither share binds (a /64 holds at most 5 of its 8, a /48 at
/// most 10 of its 16).
fn brokenRoute6(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    resetHandlers();
    var limits = testLimits();
    limits.max_task_handlers = 64;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, &broken_routes, limits);
    defer rig.stop();
    trace_rig = &rig;
    defer trace_rig = null;
    var a: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
    }
    var per: [8]Tally = @splat(.{});
    var admitted_so_far: usize = 0;
    for (&per, 1..) |*t, i| {
        try blocking(attempts, .{ &a, tr, rig.port(), Peer{ .ip6 = source6(i) }, "/stuck", no_headers, 5, t, null });
        admitted_so_far += t.admitted;
        try awaitHolding(admitted_so_far);
        try blocking(Conns.closeAll, .{&a});
    }
    settle(&rig.server, orphansAre(32).f, 5 * std.time.ns_per_s);
    var admitted: usize = 0;
    var refused: usize = 0;
    var bad: usize = 0;
    for (per, 1..) |t, i| {
        admitted += t.admitted;
        refused += t.refused_503;
        var buf: [accounts.max_refs]accounts.Key = undefined;
        const keys = accounts.keysFor(.{ .ip6 = source6(i) }, 48, &buf);
        const on64 = clientCount(&rig, keys[0]);
        std.debug.print("routes broken route IPv6 ({s}): source {d}: admitted {d}, 503 {d}; its /64 holds {d}", .{ @tagName(tr), i, t.admitted, t.refused_503, on64 });
        if (on64 != t.admitted) bad += 1;
        if (i % 2 == 0) {
            const on48 = clientCount(&rig, keys[1]);
            const want48 = per[i - 2].admitted + t.admitted;
            std.debug.print("; its /48 holds {d} (its two /64s were admitted {d})", .{ on48, want48 });
            if (on48 != want48) bad += 1;
        }
        std.debug.print("\n", .{});
    }
    std.debug.print("routes broken route IPv6 ({s}): {d} admitted, {d} answered 503; route refusals {d}, client share refusals {d}; {d} accounts off\n", .{ @tagName(tr), admitted, refused, rig.routeRefusals(0, .task_handlers), rig.clientRefusals(.task_handlers), bad });
    const pr = perRequest();
    std.debug.print("routes broken route IPv6 ({s}): per request: {d} to the broken route, {d} answered 200 with a handler past start and still running, {d} answered 503 with no handler, {d} mismatches; handlers whose start failed {d}\n", .{ @tagName(tr), pr.stuck, pr.held, pr.refused, pr.mismatches, pr.start_failed });
    if (pr.mismatches != 0 or admitted != 32 or refused != 8 or bad != 0 or rig.routeRefusals(0, .task_handlers) != 8) printTimeline(@tagName(tr));
    try expectExactly("requests whose two sides disagree", pr.mismatches, 0);
    try expectExactly("requests to the broken route", pr.stuck, 40);
    try expectExactly("admitted", admitted, 32);
    try expectExactly("answered 503", refused, 8);
    try expectExactly("route refusals", rig.routeRefusals(0, .task_handlers), 8);
    try expectExactly("client share refusals", rig.clientRefusals(.task_handlers), 0);
    try expectExactly("client and aggregate accounts that do not match what was admitted", bad, 0);
    try letGo(&rig, &.{&a});
    try expectRoutesEmpty(&rig, broken_routes.len);
}

fn broken6H2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return brokenRoute6(rt, gpa, .h2c);
}
fn broken6Tls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return brokenRoute6(rt, gpa, .tls);
}
fn broken6H1c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return brokenRoute6(rt, gpa, .h1c);
}

test "route broken route, IPv6 sources (h2c): a route refusal undoes the /64 and its /48 aggregate" {
    try runIn(broken6H2c);
}
test "route broken route, IPv6 sources (TLS): a route refusal undoes the /64 and its /48 aggregate" {
    try runIn(broken6Tls);
}
test "route broken route, IPv6 sources (HTTP/1.1): a route refusal undoes the /64 and its /48 aggregate" {
    try runIn(broken6H1c);
}

// ---------------------------------------------------------------------------
// The healthy hot route at idle: no refusal below the mark.

/// Eight task routes: each a share of 8 of 64 task handlers; the mark is 16.
const hot_routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/hot", .handler = task(stuck) },
    .{ .method = .GET, .path = "/other", .handler = task(stuck) },
    .{ .method = .GET, .path = "/p2", .handler = task(ok) },
    .{ .method = .GET, .path = "/p3", .handler = task(ok) },
    .{ .method = .GET, .path = "/p4", .handler = task(ok) },
    .{ .method = .GET, .path = "/p5", .handler = task(ok) },
    .{ .method = .GET, .path = "/p6", .handler = task(ok) },
    .{ .method = .GET, .path = "/ok", .handler = task(ok) },
};

/// `/hot` takes 12, half again its share of 8, while the server holds 12 of
/// 64, below the mark of 16: none is refused. Then `/other` takes 4 and the
/// server is at the mark: one more `/hot` is answered 503, and one more
/// `/other`, inside its share, is still admitted.
fn hotAtIdle(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    resetHandlers();
    var limits = testLimits();
    limits.max_task_handlers = 64;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, &hot_routes, limits);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
    }
    var idle: Tally = .{};
    for (1..4) |i| try blocking(attempts, .{ &a, tr, rig.port(), source(@intCast(i)), "/hot", no_headers, 4, &idle, null });
    const idle_held = rig.routeCount(0, .task_handlers);
    const idle_server = rig.server.accounting.task_handlers.load(.acquire);
    var other: Tally = .{};
    try blocking(attempts, .{ &a, tr, rig.port(), source(10), "/other", no_headers, 4, &other, null });
    var hot_busy: Tally = .{};
    try blocking(attempts, .{ &a, tr, rig.port(), source(11), "/hot", no_headers, 1, &hot_busy, null });
    var other_busy: Tally = .{};
    try blocking(attempts, .{ &a, tr, rig.port(), source(12), "/other", no_headers, 1, &other_busy, null });
    std.debug.print("routes hot route at idle ({s}): /hot (share {d}) admitted {d} of 12 with the server at {d} (mark 16), route refusals {d}; then /other admitted {d} of 4; at the mark /hot admitted {d}, answered 503 {d}; /other admitted {d} of 1; route refusals {d}\n", .{
        @tagName(tr),
        rig.routeShare(0, .task_handlers),
        idle.admitted,
        idle_server,
        rig.routeRefusals(0, .task_handlers),
        other.admitted,
        hot_busy.admitted,
        hot_busy.refused_503,
        other_busy.admitted,
        rig.routeRefusals(0, .task_handlers),
    });
    try expectExactly("/hot at idle", idle.admitted, 12);
    try expectExactly("/hot held at idle", idle_held, 12);
    try expectExactly("/other below the mark", other.admitted, 4);
    try expectExactly("/hot at the mark, admitted", hot_busy.admitted, 0);
    try expectExactly("/hot at the mark, 503", hot_busy.refused_503, 1);
    try expectExactly("/other at the mark", other_busy.admitted, 1);
    try expectExactly("route refusals", rig.routeRefusals(0, .task_handlers), 1);
    try expectExactly("client share refusals", rig.clientRefusals(.task_handlers), 0);
    try letGo(&rig, &.{&a});
    try expectRoutesEmpty(&rig, hot_routes.len);
}

fn hotH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return hotAtIdle(rt, gpa, .h2c);
}
fn hotTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return hotAtIdle(rt, gpa, .tls);
}
fn hotH1c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return hotAtIdle(rt, gpa, .h1c);
}

test "route hot route at idle (h2c): a route past its share is not refused while the server is below the mark" {
    try runIn(hotH2c);
}
test "route hot route at idle (TLS): a route past its share is not refused while the server is below the mark" {
    try runIn(hotTls);
}
test "route hot route at idle (HTTP/1.1): a route past its share is not refused while the server is below the mark" {
    try runIn(hotH1c);
}

// ---------------------------------------------------------------------------
// Request bytes.

const bytes_routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/hold", .handler = task(hold) },
    .{ .method = .POST, .path = "/hold", .handler = task(hold) },
    .{ .method = .GET, .path = "/ok", .handler = task(ok) },
};

/// The route index of `GET /hold`, and of `POST /hold` (a route, and an
/// account, of its own).
const hold_route = 0;
const post_route = 1;
/// What one `/hold` holds: its first block and a 128 KiB block.
const holder_bytes = 192 * kib;

fn bytesLimits() starh2.Limits {
    var l = testLimits();
    // The mark 512 KiB, a client's share 256 KiB (one holder, and its one
    // growth block, exactly); three task routes, each a share of 2 MiB / 3,
    // rounded up. Nested, so a connection and a request hold at most a share.
    l.request_bytes_per_server = 2 * 1024 * kib;
    l.request_bytes_per_connection = 256 * kib;
    l.request_arena_bytes = 256 * kib;
    l.request_body_bytes = 64 * kib - 64;
    l.request_pool_retain_bytes = l.request_bytes_per_server;
    return l;
}

/// Three ways the route's bytes are refused, each a 503 (or, for an orphan,
/// a failed allocation), from sources that each hold one holder, below a
/// client's share:
///
/// 1. Growth after dispatch. With a share of 4 holders plus one block (832
///    KiB, an override), eight sources each ask `/hold` once: four hold, and
///    the fifth to eighth are answered 503 when their handler's 128 KiB
///    block is refused (the lease's 64 KiB fit at dispatch).
/// 2. At dispatch. `POST /hold` is a route of its own with a share of one
///    block (64 KiB, an override): a POST whose body (`post_body_bytes`)
///    needs a second block is refused when it is dispatched, a 503. On
///    HTTP/2 the header lease already holds the body, and its bytes are
///    charged to the route at dispatch; on HTTP/1.1 the copy into the
///    request's arena (the link and the body) is, and its second block is
///    refused, so the handler never runs.
/// 3. An orphan's growth. The holders' connections close; each orphan asks
///    one more 64 KiB block: one fits the route, three are refused.
///
/// The second route is served throughout.
fn bytesRow(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    resetHandlers();
    const share = 4 * holder_bytes + 64 * kib;
    var routes = bytes_routes;
    if (comptime @hasField(starh2.Route, "share")) {
        routes[hold_route].share = .{ .request_bytes = share };
        routes[post_route].share = .{ .request_bytes = 64 * kib };
    }
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, &routes, bytesLimits());
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
    }
    // 1. Growth after dispatch.
    var tally: Tally = .{};
    for (1..9) |i| try blocking(attempts, .{ &a, tr, rig.port(), source(@intCast(i)), "/hold", no_headers, 1, &tally, null });
    // A refused request gives its lease back when the actor frees it, a
    // moment after its 503.
    settle(&rig, struct {
        fn f(r: *Rig) bool {
            return r.routeCount(hold_route, .request_bytes) == 4 * holder_bytes;
        }
    }.f, 3 * std.time.ns_per_s);
    const after_growth = rig.routeCount(hold_route, .request_bytes);
    const refused_growth = rig.routeRefusals(hold_route, .request_bytes);
    // 2. At dispatch: a body that needs a second block.
    var post: Tally = .{};
    const entered_before = entered.load(.acquire);
    try blocking(postOnce, .{ &a, tr, rig.port(), source(20), &post });
    const refused_dispatch = rig.routeRefusals(post_route, .request_bytes);
    // A refusal at dispatch: the handler never ran (on HTTP/1.1 a refusal
    // of its own growth would also be a 503, after the handler started).
    const post_started = entered.load(.acquire) - entered_before;
    var ok_during = false;
    try blocking(servedInto, .{ gpa, tr, rig.port(), &ok_during });
    // 3. The orphans' growth.
    try blocking(Conns.closeAll, .{&a});
    settle(&rig.server, orphansAre(4).f, 5 * std.time.ns_per_s);
    const orphans = rig.server.accounting.orphaned_handlers.load(.acquire);
    grow.store(true, .release);
    settle(orphans, struct {
        fn f(n: usize) bool {
            return grown.load(.acquire) + grow_refused.load(.acquire) == n;
        }
    }.f, 5 * std.time.ns_per_s);
    const after_orphans = rig.routeCount(hold_route, .request_bytes);
    var ok_after = false;
    try blocking(servedInto, .{ gpa, tr, rig.port(), &ok_after });
    std.debug.print("routes request bytes ({s}): share {d} KiB; 8 sources: {d} held, {d} answered 503, {d} other; route holds {d} KiB, refusals {d}; a {d}-byte POST: admitted {d}, 503 {d}, route refusals {d}, handlers started {d}; {d} orphans grow: {d} took a block, {d} refused; route holds {d} KiB, refusals {d} in all; client share refusals {d}; the second route served {} and {}\n", .{
        @tagName(tr),
        rig.routeShare(hold_route, .request_bytes) / kib,
        tally.admitted,
        tally.refused_503,
        tally.other,
        after_growth / kib,
        refused_growth,
        post_body_bytes,
        post.admitted,
        post.refused_503,
        refused_dispatch,
        post_started,
        orphans,
        grown.load(.acquire),
        grow_refused.load(.acquire),
        after_orphans / kib,
        rig.routeRefusals(hold_route, .request_bytes),
        rig.clientRefusals(.request_bytes),
        ok_during,
        ok_after,
    });
    try expectExactly("orphans", orphans, 4);
    try expectExactly("held", tally.admitted, 4);
    try expectExactly("answered 503 (growth refused)", tally.refused_503, 4);
    try expectExactly("other answers", tally.other, 0);
    try expectExactly("route bytes after the growth phase", after_growth, 4 * holder_bytes);
    try expectExactly("route refusals of growth", refused_growth, 4);
    try expectExactly("the POST admitted", post.admitted, 0);
    try expectExactly("the POST answered 503", post.refused_503, 1);
    try expectExactly("route refusals at dispatch", refused_dispatch, 1);
    try expectExactly("POST handlers started", post_started, 0);
    try expectExactly("orphans that took a block", grown.load(.acquire), 1);
    try expectExactly("orphans refused a block", grow_refused.load(.acquire), 3);
    try expectExactly("route bytes after the orphans grew", after_orphans, share);
    try expectExactly("route refusals of the orphans' growth", rig.routeRefusals(hold_route, .request_bytes) - refused_growth, 3);
    try expectExactly("client share refusals", rig.clientRefusals(.request_bytes), 0);
    try std.testing.expect(ok_during);
    try std.testing.expect(ok_after);
    try letGo(&rig, &.{&a});
    try expectRoutesEmpty(&rig, routes.len);
}

fn servedInto(g: std.mem.Allocator, t: Tr, p: u16, out: *bool) void {
    out.* = served(g, t, p);
}

/// The largest body `bytesLimits` allows: with the link beside it, the
/// HTTP/1.1 copy does not fit one 64 KiB block.
const post_body_bytes = 64 * kib - 64 - 16;

/// One POST of `post_body_bytes` to `/hold` from `src`.
fn postOnce(conns: *Conns, tr: Tr, port: u16, src: Peer, out: *Tally) void {
    postInner(conns, tr, port, src, out) catch |err| {
        std.debug.print("routes: the POST failed: {s}\n", .{@errorName(err)});
        out.other += 1;
    };
}

fn postInner(conns: *Conns, tr: Tr, port: u16, src: Peer, out: *Tally) !void {
    const body: [post_body_bytes]u8 = @splat('p');
    switch (tr) {
        .h2c, .tls => {
            const h = conns.addH2(try H2.open(conns.gpa, tr, port, src));
            const sid = try h.post(conns.gpa, "/hold", &body);
            try h.pollFor(&.{sid}, 2000);
            out.note(h.status[H2.slot(sid)], h.rst[H2.slot(sid)]);
        },
        .h1c => {
            const fd = try rawProxied(port, src);
            conns.addRaw(fd);
            var head_buf: [128]u8 = undefined;
            const head = try std.fmt.bufPrint(&head_buf, "POST /hold HTTP/1.1\r\nHost: localhost\r\nContent-Length: {d}\r\n\r\n", .{body.len});
            _ = rawSend(fd, head);
            _ = rawSend(fd, &body);
            out.note(rawStatus(fd, 2000), null);
        },
    }
}

fn bytesH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return bytesRow(rt, gpa, .h2c);
}
fn bytesTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return bytesRow(rt, gpa, .tls);
}
fn bytesH1c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return bytesRow(rt, gpa, .h1c);
}

test "route request bytes (h2c): growth, dispatch and orphan growth past the route's share are refused" {
    try runIn(bytesH2c);
}
test "route request bytes (TLS): growth, dispatch and orphan growth past the route's share are refused" {
    try runIn(bytesTls);
}
test "route request bytes (HTTP/1.1): growth, dispatch and orphan growth past the route's share are refused" {
    try runIn(bytesH1c);
}

// ---------------------------------------------------------------------------
// Compression contexts: identity encoding.

const brotli_routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/br", .handler = task(brotliHeld) },
    .{ .method = .GET, .path = "/ok", .handler = task(ok) },
};

/// 16 contexts: the mark 4, a client's share 2, each of two task routes' share
/// 8. Six sources ask `/br` twice each (at their share), and every
/// connection is closed after, so the handlers hold their contexts as
/// orphans: 8 are compressed, 4 fall back to identity, all answered 200.
fn compressionRow(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Tr) !void {
    resetHandlers();
    var limits = testLimits();
    limits.response_compression = true;
    limits.compression_contexts_per_server = 16;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, &brotli_routes, limits);
    defer rig.stop();
    var a: Conns = .{ .gpa = gpa };
    defer {
        release.store(true, .release);
        a.closeAll();
    }
    const accept_br = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
    var tally: Tally = .{};
    var compressed: usize = 0;
    for (1..7) |i| {
        try blocking(attempts, .{ &a, tr, rig.port(), source(@intCast(i)), "/br", accept_br[0..], 2, &tally, &compressed });
        try blocking(Conns.closeAll, .{&a});
    }
    settle(&rig.server, orphansAre(12).f, 5 * std.time.ns_per_s);
    const held = rig.routeCount(0, .compression);
    const pool_live = rig.server.compression_pool.?.liveCount();
    var ok_now = false;
    try blocking(servedInto, .{ gpa, tr, rig.port(), &ok_now });
    std.debug.print("routes compression ({s}): 12 requests from 6 sources: {d} answered 200, {d} compressed, {d} identity; route holds {d} contexts (share {d}), the pool {d}; route refusals {d}, client share refusals {d}; the second route served {}\n", .{
        @tagName(tr),
        tally.admitted,
        compressed,
        tally.admitted - compressed,
        held,
        rig.routeShare(0, .compression),
        pool_live,
        rig.routeRefusals(0, .compression),
        rig.clientRefusals(.compression),
        ok_now,
    });
    try expectExactly("orphans", rig.server.accounting.orphaned_handlers.load(.acquire), 12);
    try expectExactly("answered 200", tally.admitted, 12);
    try expectExactly("compressed", compressed, 8);
    try expectExactly("the route's contexts", held, 8);
    try expectExactly("the pool's live contexts", pool_live, 8);
    try expectExactly("route refusals", rig.routeRefusals(0, .compression), 4);
    try expectExactly("client share refusals", rig.clientRefusals(.compression), 0);
    try std.testing.expect(ok_now);
    try letGo(&rig, &.{&a});
    try expectRoutesEmpty(&rig, brotli_routes.len);
}

fn compressionH2c(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return compressionRow(rt, gpa, .h2c);
}
fn compressionTls(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return compressionRow(rt, gpa, .tls);
}

test "route compression contexts (h2c): identity encoding past the route's share under pressure" {
    try runIn(compressionH2c);
}
test "route compression contexts (TLS): identity encoding past the route's share under pressure" {
    try runIn(compressionTls);
}

// ---------------------------------------------------------------------------
// Boot.

fn bootRoutes(comptime n: usize) [n + 1]starh2.Route {
    var r: [n + 1]starh2.Route = undefined;
    for (r[0..n]) |*x| x.* = .{ .method = .GET, .path = "/t", .handler = task(ok) };
    r[n] = .{ .method = .GET, .path = "/c", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = struct {
        fn f(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
            try resp.send(200, &.{}, "c");
        }
    }.f } } };
    return r;
}

fn bootServer(rt: *zio.Runtime, gpa: std.mem.Allocator, routes: []const starh2.Route) !starh2.Server {
    const a4 = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    return starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = .{ .address = a4 } }},
        .routes = routes,
        .tls = null,
        .limits = .defaults,
    });
}

/// The default share with 1, 2 and 8 task routes (a complete route beside
/// them counts for nothing), read from the server the way a request would
/// find it; an override; and the boot error for an override on a complete
/// route. The warn line and the error for an override above the server's
/// limit log, so they run in a child (`shutdown_child route-share-boot`).
fn bootShares(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    if (comptime !has_routes) return error.NoRouteAccounts;
    const l = starh2.Limits.defaults;
    inline for (.{ 1, 2, 8 }) |n| {
        const routes = bootRoutes(n);
        var s = try bootServer(rt, gpa, &routes);
        defer s.deinit(gpa);
        for (0..n) |i| {
            const rec = s.accounting.routes.recordOf(@intCast(i)).?;
            try expectExactly("task handler share", rec.shares[0], l.max_task_handlers / n);
            try expectExactly("request bytes share", rec.shares[1], l.request_bytes_per_server / n);
            try expectExactly("compression share", rec.shares[2], l.compression_contexts_per_server / n);
        }
        try std.testing.expect(s.accounting.routes.recordOf(n) == null);
        std.debug.print("routes boot: {d} task routes: each a share of {d} task handlers, {d} MiB, {d} contexts\n", .{ n, l.max_task_handlers / n, l.request_bytes_per_server / n / (1024 * kib), l.compression_contexts_per_server / n });
    }
    var over = bootRoutes(2);
    over[0].share = .{ .task_handlers = l.max_task_handlers, .compression = 3 };
    var s = try bootServer(rt, gpa, &over);
    defer s.deinit(gpa);
    try expectExactly("override", s.accounting.routes.recordOf(0).?.shares[0], l.max_task_handlers);
    try expectExactly("override", s.accounting.routes.recordOf(0).?.shares[2], 3);
    try expectExactly("no override", s.accounting.routes.recordOf(0).?.shares[1], l.request_bytes_per_server / 2);
    var on_complete = bootRoutes(1);
    on_complete[1].share = .{ .task_handlers = 1 };
    try std.testing.expectError(error.RouteShareOnCompleteRoute, bootServer(rt, gpa, &on_complete));
}

test "route boot: the default share with 1, 2 and 8 task routes, an override, and no override on a complete route" {
    try runIn(bootShares);
}
