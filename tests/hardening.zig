//! Attack gates for the missing deadlines and the accept loop (t-3759 step 1).
//!
//! Each gate drives a real socket with the attack and, while the attack
//! holds its resource, serves a second, well-behaved client: a server that
//! survives the attack by refusing everyone is not a pass. Each prints the
//! counters that show the defence fired, so an attack that never engaged
//! (no stream held, no error injected) shows as a zero, not as a quiet pass.
//!
//! - Body at HEADERS: streams opened with HEADERS and no END_STREAM, then
//!   silence, are reset by the body clock (h2c and TLS).
//! - Body minimum rate: a body dripped below the rate is ended; an honest
//!   slow upload above it is not (h2c, TLS, HTTP/1.1). The third case of the
//!   brief, an upload the server itself throttles, has no socket-level form
//!   (the server never withholds receive credit except when it owes a
//!   WINDOW_UPDATE it could not queue); `session.zig` tests it on the
//!   Session, where that owed credit can be made.
//! - Response minimum rate: a zero window opened one byte at a time on a
//!   stream with pending bytes is reset; a sparse SSE stream is not (h2c and
//!   TLS).
//! - Accept: a real EMFILE (the process's descriptor limit lowered under the
//!   server) does not end the accept loop, an established connection is
//!   still served meanwhile, and the endpoint accepts again once descriptors
//!   are free.
//! - Boot: a `Server.init` whose `max_connections` needs more descriptors than
//!   the process can get fails with `FileLimitTooLow`.
//!
//! The file compiles against the tree before these defences too (the new
//! `Limits` fields and counters are reached through `@hasField` and
//! `@hasDecl`), so the same gates show the attacks succeed there.
const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");

const frame = starh2.core.frame;
const hpack = starh2.core.hpack;
const session_mod = starh2.core.session;
const conn_mod = starh2.edge.connection;
const h1_mod = starh2.edge.h1;
const server_mod = starh2.edge.server;

const Transport = wire_client.Transport;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;

const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

// ---------------------------------------------------------------------------
// Limits and counters that may not exist in the tree under test.

fn setLimit(limits: *starh2.Limits, comptime name: []const u8, value: anytype) void {
    if (@hasField(starh2.Limits, name)) @field(limits, name) = value;
}

fn counter(comptime Container: type, comptime name: []const u8) u64 {
    if (@hasDecl(Container, name)) return @field(Container, name).load(.acquire);
    return 0;
}

/// The response grace under either name: the old tree calls it
/// `slow_consumer_timeout_ns` and measures any progress.
fn setResponseGrace(limits: *starh2.Limits, value: u64) void {
    if (@hasField(starh2.Limits, "response_min_rate_grace_ns")) {
        @field(limits, "response_min_rate_grace_ns") = value;
    } else {
        @field(limits, "slow_consumer_timeout_ns") = value;
    }
}

// ---------------------------------------------------------------------------
// Routes.

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

var upload_seen: std.atomic.Value(usize) = .init(0);

/// Answers with the body length, so the client sees the whole body arrived.
fn upload(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = upload_seen.fetchAdd(1, .acq_rel);
    var buf: [32]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "{d}", .{req.body.len});
    try resp.send(200, &.{}, text);
}

const big_bytes = 64 * 1024;
/// How many times `big` writes its 64 KiB.
var big_rounds: usize = 1;
var big_error: std.atomic.Value(u8) = .init(0);
var big_done: std.atomic.Value(bool) = .init(false);

const BigEnd = enum(u8) { none, ok, slow_consumer, other };

fn big(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    defer big_done.store(true, .release);
    const pattern: [big_bytes]u8 = @splat('b');
    var body = try resp.start(200, &.{});
    for (0..big_rounds) |_| body.writeAll(&pattern) catch |err| {
        big_error.store(@intFromEnum(if (err == error.SlowConsumer) BigEnd.slow_consumer else BigEnd.other), .release);
        return err;
    };
    body.finish() catch |err| {
        big_error.store(@intFromEnum(if (err == error.SlowConsumer) BigEnd.slow_consumer else BigEnd.other), .release);
        return err;
    };
    big_error.store(@intFromEnum(BigEnd.ok), .release);
}

/// Waits for `queued_gate`, writes 4 KiB, flushes, and stays open: its bytes
/// can leave the scheduler and sit in the writer while the stream lives.
var queued_gate: std.atomic.Value(bool) = .init(false);
var queued_end: std.atomic.Value(u8) = .init(0);
var queued_started: std.atomic.Value(bool) = .init(false);

fn queuedWrite(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    queued_started.store(true, .release);
    while (!queued_gate.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    const chunk: [4096]u8 = @splat('q');
    const res = blk: {
        body.writeAll(&chunk) catch |err| break :blk err;
        body.flush() catch |err| break :blk err;
        while (true) zio.sleep(.fromMilliseconds(20)) catch |err| break :blk err;
    };
    queued_end.store(@intFromEnum(if (res == error.SlowConsumer) BigEnd.slow_consumer else BigEnd.other), .release);
    return res;
}

const sse_events = 4;
const sse_gap_ms = 700;

/// One small event, then a pause longer than the response grace, four times.
fn sparseSse(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{.{ .name = "content-type", .value = "text/event-stream" }});
    for (0..sse_events) |i| {
        if (i != 0) try zio.sleep(.fromMilliseconds(sse_gap_ms));
        try body.writeAll("data: tick\n\n");
        try body.flush();
    }
    try body.finish();
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    .{ .method = .POST, .path = "/upload", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = upload } } },
    .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = big } } },
    .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sparseSse } } },
    .{ .method = .GET, .path = "/queued", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = queuedWrite } } },
    .{ .method = .GET, .path = "/finish", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = finishWrite } } },
    .{ .method = .GET, .path = "/stream", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = streamWrite } } },
    .{ .method = .GET, .path = "/exact-conn", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = exactConnWrite } } },
    .{ .method = .GET, .path = "/exact", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = exactWrite } } },
    .{ .method = .GET, .path = "/flood", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = flood } } },
};

/// Writes the whole initial CONNECTION window (65,535 bytes), flushes, then
/// ends the stream with a bare END_STREAM.
const exact_conn_len = 65535;

fn exactConnWrite(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    const chunk: [4096]u8 = @splat('c');
    const res = blk: {
        var left: usize = exact_conn_len;
        while (left > 0) {
            const n = @min(left, chunk.len);
            body.writeAll(chunk[0..n]) catch |err| break :blk err;
            left -= n;
        }
        body.flush() catch |err| break :blk err;
        body.finish() catch |err| break :blk err;
        break :blk error.Finished;
    };
    exact_end.store(@intFromEnum(if (res == error.Finished) BigEnd.ok else if (res == error.SlowConsumer) BigEnd.slow_consumer else BigEnd.other), .release);
    if (res != error.Finished) return res;
}

/// Writes 16 KiB at a time with no flush, until a write fails: several
/// chunks wait in the writer at once, so it never drains while the reader
/// is slower.
fn streamWrite(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const chunk: [16 * 1024]u8 = @splat('s');
    var body = try resp.start(200, &.{});
    while (true) try body.writeAll(&chunk);
}

/// Writes `exact_len` bytes, flushes, then ends the stream: the END_STREAM
/// goes in a DATA frame of its own, after the body.
const exact_len = 4096;
var exact_end: std.atomic.Value(u8) = .init(0);

fn exactWrite(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    const chunk: [exact_len]u8 = @splat('x');
    const res = blk: {
        body.writeAll(&chunk) catch |err| break :blk err;
        body.flush() catch |err| break :blk err;
        body.finish() catch |err| break :blk err;
        break :blk error.Finished;
    };
    exact_end.store(@intFromEnum(if (res == error.Finished) BigEnd.ok else if (res == error.SlowConsumer) BigEnd.slow_consumer else BigEnd.other), .release);
    if (res != error.Finished) return res;
}

/// Writes 16 KiB and flushes, until a write fails. (HTTP/1.1 holds at most
/// `outbound_bytes_per_stream` unflushed.)
var flood_ended: std.atomic.Value(usize) = .init(0);

/// Flood handlers that ended with `error.SlowConsumer`: the handler is told
/// why it was stopped (astra 5).
var flood_slow: std.atomic.Value(usize) = .init(0);

fn flood(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    defer _ = flood_ended.fetchAdd(1, .acq_rel);
    const chunk: [16 * 1024]u8 = @splat('f');
    var body = try resp.start(200, &.{});
    while (true) {
        body.writeAll(&chunk) catch |err| return floodEnd(err);
        body.flush() catch |err| return floodEnd(err);
    }
}

fn floodEnd(err: anyerror) anyerror {
    if (err == error.SlowConsumer) _ = flood_slow.fetchAdd(1, .acq_rel);
    return err;
}

/// Waits for `queued_gate`, writes 4 KiB and ends the stream: `finish`
/// waits for that last DATA to be written.
var finish_end: std.atomic.Value(u8) = .init(0);

fn finishWrite(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    while (!queued_gate.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    const chunk: [4096]u8 = @splat('e');
    const res = blk: {
        body.writeAll(&chunk) catch |err| break :blk err;
        body.finish() catch |err| break :blk err;
        break :blk error.Finished;
    };
    finish_end.store(@intFromEnum(if (res == error.Finished) BigEnd.ok else if (res == error.SlowConsumer) BigEnd.slow_consumer else BigEnd.other), .release);
    if (res != error.Finished) return res;
}

// ---------------------------------------------------------------------------
// Server.

fn newRuntime(gpa: std.mem.Allocator) !*zio.Runtime {
    return zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
}

const Endpoint = enum { h2c, tls, h1c };

const Running = struct {
    server: starh2.Server,
    handle: zio.JoinHandle(server_mod.ServeError!void) = undefined,
    cert: []u8 = &.{},
    key: []u8 = &.{},
    gpa: std.mem.Allocator,

    fn port(self: *const Running) u16 {
        return self.server.localAddress(0).getPort();
    }

    fn stop(self: *Running) void {
        self.server.requestShutdown();
        self.handle.join() catch {};
        self.server.deinit(self.gpa);
        self.gpa.free(self.cert);
        self.gpa.free(self.key);
    }
};

fn startServer(rt: *zio.Runtime, gpa: std.mem.Allocator, ep: Endpoint, limits: starh2.Limits, run: *Running) !void {
    const io = rt.io();
    run.* = .{ .server = undefined, .gpa = gpa };
    var endpoint: starh2.EndpointConfig = undefined;
    var tls_cfg: ?starh2.TlsConfig = null;
    switch (ep) {
        .h2c => endpoint = .{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) },
        .h1c => endpoint = .{ .h1c = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) },
        .tls => {
            run.cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            run.key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) };
            tls_cfg = .{ .certificate_chain_pem = run.cert, .private_key_pem = run.key };
        },
    }
    run.server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{endpoint},
        .routes = &routes,
        .tls = tls_cfg,
        .limits = limits,
    });
    run.handle = try rt.spawn(starh2.Server.serve, .{ &run.server, gpa });
    try run.server.waitUntilListening(5 * std.time.ns_per_s);
}

/// `startServer`, with h2c and TLS on a unix socket at `path`: its fixed
/// kernel buffers hold a stall, where macOS TCP loopback grows them. h1c
/// has no unix endpoint and stays on TCP.
fn startServerAt(rt: *zio.Runtime, gpa: std.mem.Allocator, ep: Endpoint, limits: starh2.Limits, path: []const u8, run: *Running) !void {
    const io = rt.io();
    run.* = .{ .server = undefined, .gpa = gpa };
    var endpoint: starh2.EndpointConfig = undefined;
    var tls_cfg: ?starh2.TlsConfig = null;
    switch (ep) {
        .h2c => endpoint = .{ .h2c_unix = path },
        .h1c => endpoint = .{ .h1c = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) },
        .tls => {
            run.cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            run.key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls_unix = path };
            tls_cfg = .{ .certificate_chain_pem = run.cert, .private_key_pem = run.key };
        },
    }
    run.server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{endpoint},
        .routes = &routes,
        .tls = tls_cfg,
        .limits = limits,
    });
    run.handle = try rt.spawn(starh2.Server.serve, .{ &run.server, gpa });
    try run.server.waitUntilListening(5 * std.time.ns_per_s);
}

fn waitCount(comptime what: []const u8, value: *const std.atomic.Value(usize), want: usize, timeout_ns: u64) !void {
    const deadline = nowNs() + timeout_ns;
    while (value.load(.acquire) != want) {
        if (nowNs() >= deadline) {
            std.debug.print("hardening: {s} is {d}, waited for {d}\n", .{ what, value.load(.acquire), want });
            return error.StateNotReached;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

// ---------------------------------------------------------------------------
// Clients. Each runs on its own OS thread: `Wire` blocks.

fn threadSleepMs(n: u32) void {
    const t: std.c.timespec = .{ .sec = 0, .nsec = @as(isize, n) * std.time.ns_per_ms };
    _ = std.c.nanosleep(&t, null);
}

/// `Wire` sets a 1 s receive timeout, so `next` can block a full second past
/// its deadline. These clients write on a 100 ms beat while they read, so
/// they look at the socket for 20 ms at a time (after the TLS handshake,
/// which needs the long wait).
fn openShort(gpa: std.mem.Allocator, transport: Transport, port: u16) !*Wire {
    const w = try Wire.open(gpa, transport, port);
    const tv: std.c.timeval = .{ .sec = 0, .usec = 20_000 };
    _ = std.c.setsockopt(w.fd, std.c.SOL.SOCKET, std.c.SO.RCVTIMEO, std.mem.asBytes(&tv), @sizeOf(std.c.timeval));
    return w;
}

fn rstCode(w: *Wire) u32 {
    const p = w.last_payload.items;
    if (p.len < 4) return std.math.maxInt(u32);
    return std.mem.readInt(u32, p[0..4], .big);
}

/// A well-behaved HTTP/2 client: one GET /hello, which must answer 200.
fn goodGet(gpa: std.mem.Allocator, transport: Transport, port: u16) !void {
    const w = try Wire.open(gpa, transport, port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try h2c.appendHeaders(gpa, &req, 1, "/hello", true);
    try w.send(req.items);
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (true) {
        const s = try w.next(deadline) orelse return error.GoodClientClosed;
        if (s.sid != 1) continue;
        if (s.ftype == @intFromEnum(frame.FrameType.rst_stream)) return error.GoodClientReset;
        if (s.ftype == @intFromEnum(frame.FrameType.data) and s.flags & 1 != 0) return;
        if (s.ftype == @intFromEnum(frame.FrameType.headers) and s.flags & 1 != 0) return;
    }
}

const GoodResult = struct {
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
};

fn goodGetMain(gpa: std.mem.Allocator, transport: Transport, port: u16, r: *GoodResult) void {
    goodGet(gpa, transport, port) catch |err| {
        r.err = err;
    };
    r.done.store(true, .release);
}

/// Run the well-behaved client to completion from a zio task (it runs on a
/// thread of its own, and the task waits without blocking its executor).
fn serveGood(gpa: std.mem.Allocator, transport: Transport, port: u16) !void {
    var r: GoodResult = .{};
    const t = try std.Thread.spawn(.{}, goodGetMain, .{ gpa, transport, port, &r });
    while (!r.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    t.join();
    if (r.err) |err| return err;
}

fn postHeaders(gpa: std.mem.Allocator, wire: *std.ArrayList(u8), sid: u31, content_length: []const u8) !void {
    const extra = [_]hpack.HeaderField{.{ .name = "content-length", .value = content_length }};
    try h2c.appendHeadersExtra(gpa, wire, sid, "POST", "/upload", false, &extra);
}

// ---------------------------------------------------------------------------
// 1. HEADERS without END_STREAM, then silence (inventory row 1, the timer).

const silent_streams = 8;
const body_grace_ms = 600;

const SilentResult = struct {
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
    resets: usize = 0,
    wrong_code: usize = 0,
    sent_ns: u64 = 0,
    last_reset_ns: u64 = 0,
    release: std.atomic.Value(bool) = .init(false),
};

fn silentClient(gpa: std.mem.Allocator, transport: Transport, port: u16, r: *SilentResult) !void {
    const w = try Wire.open(gpa, transport, port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    for (0..silent_streams) |i| try postHeaders(gpa, &req, @intCast(1 + 2 * i), "1000");
    try w.send(req.items);
    r.sent_ns = nowNs();
    const deadline = r.sent_ns + 4 * std.time.ns_per_s;
    while (r.resets < silent_streams) {
        const s = w.next(deadline) catch |err| switch (err) {
            error.Deadline => return,
            else => return err,
        } orelse return;
        if (s.ftype == @intFromEnum(frame.FrameType.rst_stream)) {
            r.resets += 1;
            r.last_reset_ns = nowNs();
            if (rstCode(w) != @intFromEnum(frame.ErrorCode.cancel)) r.wrong_code += 1;
        }
    }
}

fn silentMain(gpa: std.mem.Allocator, transport: Transport, port: u16, r: *SilentResult) void {
    silentClient(gpa, transport, port, r) catch |err| {
        r.err = err;
    };
    r.done.store(true, .release);
}

fn runSilent(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    var limits = starh2.Limits.defaults;
    setLimit(&limits, "request_body_min_rate_grace_ns", @as(u64, body_grace_ms * ms));
    var run: Running = undefined;
    try startServer(rt, gpa, if (transport == .tls) .tls else .h2c, limits, &run);
    defer run.stop();
    const rate_before = counter(session_mod, "body_rate_expiries");

    var r: SilentResult = .{};
    const t = try std.Thread.spawn(.{}, silentMain, .{ gpa, transport, run.port(), &r });
    defer t.join();
    // The attack holds its streams: every one took a server stream token.
    try waitCount("active_streams", &run.server.accounting.active_streams, silent_streams, 2 * std.time.ns_per_s);
    // A second client is served while they are held.
    try serveGood(gpa, transport, run.port());
    while (!r.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    if (r.err) |err| return err;
    const rate_fired = counter(session_mod, "body_rate_expiries") - rate_before;
    const after_ms = if (r.last_reset_ns == 0) 0 else (r.last_reset_ns - r.sent_ns) / ms;
    std.debug.print("hardening {s} silent HEADERS: {d} of {d} streams reset (CANCEL: {d}), last {d} ms after HEADERS; body_rate_expiries +{d}; active_streams now {d}\n", .{
        @tagName(transport),                                 r.resets, silent_streams, r.resets - r.wrong_code, after_ms, rate_fired,
        run.server.accounting.active_streams.load(.acquire),
    });
    try std.testing.expectEqual(@as(usize, silent_streams), r.resets);
    try std.testing.expectEqual(@as(usize, 0), r.wrong_code);
    try std.testing.expect(after_ms + 50 >= body_grace_ms);
    try std.testing.expect(after_ms <= body_grace_ms + 1500);
    try waitCount("active_streams", &run.server.accounting.active_streams, 0, 2 * std.time.ns_per_s);
}

test "hardening: h2c streams opened with HEADERS and then silence are reset by the body clock" {
    try runOnRuntime(runSilent, .h2c);
}

test "hardening: tls streams opened with HEADERS and then silence are reset by the body clock" {
    try runOnRuntime(runSilent, .tls);
}

fn runOnRuntime(comptime f: anytype, transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(f, .{ rt, gpa, transport });
    try h.join();
}

// ---------------------------------------------------------------------------
// 2. Request body minimum rate (inventory row 8).

const rate_limit_bps: u32 = 1000;
const drip_grace_ms = 500;

fn rateLimits() starh2.Limits {
    var limits = starh2.Limits.defaults;
    setLimit(&limits, "request_body_min_rate_bytes_per_s", rate_limit_bps);
    setLimit(&limits, "request_body_min_rate_grace_ns", @as(u64, drip_grace_ms * ms));
    return limits;
}

const Upload = struct {
    /// Bytes per write and the pause between writes.
    chunk: usize,
    gap_ms: u64,
    total: usize,
};

/// 50 B every 100 ms: 500 B/s, half the rate.
const drip: Upload = .{ .chunk = 50, .gap_ms = 100, .total = 4000 };
/// 400 B every 100 ms: 4 kB/s, four times the rate, for about a second.
const honest: Upload = .{ .chunk = 400, .gap_ms = 100, .total = 4000 };

const UploadEnd = enum { none, reset, answered, closed };

const UploadResult = struct {
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
    end: UploadEnd = .none,
    status_or_code: u32 = 0,
    sent: usize = 0,
    elapsed_ms: u64 = 0,
};

fn h2Upload(gpa: std.mem.Allocator, transport: Transport, port: u16, up: Upload, r: *UploadResult) !void {
    const w = try openShort(gpa, transport, port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    var cl: [16]u8 = undefined;
    try postHeaders(gpa, &req, 1, try std.fmt.bufPrint(&cl, "{d}", .{up.total}));
    try w.send(req.items);
    const t0 = nowNs();
    const give_up = t0 + 6 * std.time.ns_per_s;
    const chunk: [400]u8 = @splat('u');
    while (true) {
        // Read whatever arrived before the next write.
        while (true) {
            const s = w.next(nowNs() + up.gap_ms * ms) catch |err| switch (err) {
                error.Deadline => break,
                else => return err,
            } orelse {
                r.end = .closed;
                r.elapsed_ms = (nowNs() - t0) / ms;
                return;
            };
            if (s.sid != 1) continue;
            if (s.ftype == @intFromEnum(frame.FrameType.rst_stream)) {
                r.end = .reset;
                r.status_or_code = rstCode(w);
                r.elapsed_ms = (nowNs() - t0) / ms;
                return;
            }
            if ((s.ftype == @intFromEnum(frame.FrameType.data) or s.ftype == @intFromEnum(frame.FrameType.headers)) and s.flags & 1 != 0) {
                r.end = .answered;
                r.elapsed_ms = (nowNs() - t0) / ms;
                return;
            }
        }
        if (nowNs() >= give_up) return;
        if (r.sent < up.total) {
            const n = @min(up.chunk, up.total - r.sent);
            var one: std.ArrayList(u8) = .empty;
            defer one.deinit(gpa);
            try h2c.appendData(gpa, &one, 1, chunk[0..n], r.sent + n == up.total);
            w.send(one.items) catch return;
            r.sent += n;
        }
    }
}

fn h1Upload(port: u16, up: Upload, r: *UploadResult) !void {
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.Socket;
    defer _ = std.c.close(fd);
    var addr: std.c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
    var head: [128]u8 = undefined;
    const h = try std.fmt.bufPrint(&head, "POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: {d}\r\n\r\n", .{up.total});
    if (std.c.send(fd, h.ptr, h.len, 0) != @as(isize, @intCast(h.len))) return error.Send;
    const t0 = nowNs();
    const give_up = t0 + 6 * std.time.ns_per_s;
    const chunk: [400]u8 = @splat('u');
    var got: [512]u8 = undefined;
    var got_n: usize = 0;
    while (nowNs() < give_up) {
        var pfd = [1]std.c.pollfd{.{ .fd = fd, .events = std.c.POLL.IN, .revents = 0 }};
        if (std.c.poll(&pfd, 1, @intCast(up.gap_ms)) > 0) {
            const n = std.c.recv(fd, got[got_n..].ptr, got.len - got_n, 0);
            if (n <= 0) {
                r.end = if (got_n == 0) .closed else .answered;
                break;
            }
            got_n += @intCast(n);
            if (std.mem.indexOf(u8, got[0..got_n], "\r\n\r\n") != null) {
                r.end = .answered;
                break;
            }
            continue;
        }
        if (r.sent < up.total) {
            const n = @min(up.chunk, up.total - r.sent);
            if (std.c.send(fd, &chunk, n, 0) != @as(isize, @intCast(n))) {
                r.end = .closed;
                break;
            }
            r.sent += n;
        }
    }
    r.elapsed_ms = (nowNs() - t0) / ms;
    if (got_n >= 12 and std.mem.startsWith(u8, got[0..got_n], "HTTP/1.1 ")) {
        r.status_or_code = std.fmt.parseInt(u32, got[9..12], 10) catch 0;
    }
}

fn uploadMain(gpa: std.mem.Allocator, ep: Endpoint, port: u16, up: Upload, r: *UploadResult) void {
    const res = switch (ep) {
        .h2c => h2Upload(gpa, .h2c, port, up, r),
        .tls => h2Upload(gpa, .tls, port, up, r),
        .h1c => h1Upload(port, up, r),
    };
    res catch |err| {
        r.err = err;
    };
    r.done.store(true, .release);
}

fn goodH1(port: u16) !void {
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.Socket;
    defer _ = std.c.close(fd);
    var addr: std.c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
    const req = "GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n";
    if (std.c.send(fd, req.ptr, req.len, 0) != req.len) return error.Send;
    var got: [512]u8 = undefined;
    var n: usize = 0;
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (std.mem.indexOf(u8, got[0..n], "\r\n\r\nok") == null) {
        if (nowNs() >= deadline) return error.GoodClientTimeout;
        var pfd = [1]std.c.pollfd{.{ .fd = fd, .events = std.c.POLL.IN, .revents = 0 }};
        if (std.c.poll(&pfd, 1, 100) <= 0) continue;
        const k = std.c.recv(fd, got[n..].ptr, got.len - n, 0);
        if (k <= 0) return error.GoodClientClosed;
        n += @intCast(k);
    }
    if (!std.mem.startsWith(u8, got[0..n], "HTTP/1.1 200")) return error.GoodClientNot200;
}

fn goodAnyMain(gpa: std.mem.Allocator, ep: Endpoint, port: u16, r: *GoodResult) void {
    const res = switch (ep) {
        .h2c => goodGet(gpa, .h2c, port),
        .tls => goodGet(gpa, .tls, port),
        .h1c => goodH1(port),
    };
    res catch |err| {
        r.err = err;
    };
    r.done.store(true, .release);
}

fn runBodyRate(rt: *zio.Runtime, gpa: std.mem.Allocator, ep: Endpoint) !void {
    var run: Running = undefined;
    try startServer(rt, gpa, ep, rateLimits(), &run);
    defer run.stop();
    const h2_rate_before = counter(session_mod, "body_rate_expiries");
    const h1_rate_before = counter(h1_mod, "body_rate_expiries");

    // The drip and the honest upload run at once; the good client is served
    // while the drip holds its stream.
    var dripped: UploadResult = .{};
    var fair: UploadResult = .{};
    const td = try std.Thread.spawn(.{}, uploadMain, .{ gpa, ep, run.port(), drip, &dripped });
    const tf = try std.Thread.spawn(.{}, uploadMain, .{ gpa, ep, run.port(), honest, &fair });
    zio.sleep(.fromMilliseconds(200)) catch {};
    var good: GoodResult = .{};
    const tg = try std.Thread.spawn(.{}, goodAnyMain, .{ gpa, ep, run.port(), &good });
    while (!dripped.done.load(.acquire) or !fair.done.load(.acquire) or !good.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    td.join();
    tf.join();
    tg.join();
    if (dripped.err) |err| return err;
    if (fair.err) |err| return err;
    if (good.err) |err| return err;
    const fired = (counter(session_mod, "body_rate_expiries") - h2_rate_before) + (counter(h1_mod, "body_rate_expiries") - h1_rate_before);
    std.debug.print("hardening {s} body rate: drip ended {s} ({d}) after {d} ms with {d} of {d} bytes sent; honest upload ended {s} ({d}) after {d} ms with {d} bytes; body_rate_expiries +{d}\n", .{
        @tagName(ep), @tagName(dripped.end), dripped.status_or_code, dripped.elapsed_ms, dripped.sent, drip.total,
        @tagName(fair.end), fair.status_or_code, fair.elapsed_ms, fair.sent, fired,
    });
    switch (ep) {
        .h2c, .tls => {
            try std.testing.expectEqual(UploadEnd.reset, dripped.end);
            try std.testing.expectEqual(@as(u32, @intFromEnum(frame.ErrorCode.cancel)), dripped.status_or_code);
        },
        .h1c => {
            // Nothing final was sent before the expiry, so the client is told.
            try std.testing.expectEqual(UploadEnd.answered, dripped.end);
            try std.testing.expectEqual(@as(u32, 408), dripped.status_or_code);
        },
    }
    try std.testing.expect(dripped.sent < drip.total);
    try std.testing.expectEqual(UploadEnd.answered, fair.end);
    try std.testing.expectEqual(honest.total, fair.sent);
    if (ep == .h1c) try std.testing.expectEqual(@as(u32, 200), fair.status_or_code);
    try std.testing.expectEqual(@as(u64, 1), fired);
}

test "hardening: h2c a body dripped below the minimum rate is reset, an honest slow upload is not" {
    try runBodyRateOn(.h2c);
}

test "hardening: tls a body dripped below the minimum rate is reset, an honest slow upload is not" {
    try runBodyRateOn(.tls);
}

test "hardening: h1c a body dripped below the minimum rate gets 408, an honest slow upload is not" {
    try runBodyRateOn(.h1c);
}

fn runBodyRateOn(ep: Endpoint) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runBodyRate, .{ rt, gpa, ep });
    try h.join();
}

// ---------------------------------------------------------------------------
// 3. Response minimum rate (inventory row 9).

const response_grace_ms = 500;

const TrickleResult = struct {
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
    reset: bool = false,
    code: u32 = 0,
    ended: bool = false,
    data_bytes: usize = 0,
    updates: usize = 0,
    elapsed_ms: u64 = 0,
};

/// Which window the client opens one byte at a time.
const Held = enum { stream, connection };

/// `.stream`: stream windows start at zero, the connection window is open.
/// `.connection`: stream windows are huge, the connection window stays at
/// the protocol's 65,535 and the response is larger. GET /big, then a 1-byte
/// WINDOW_UPDATE for the held window every 100 ms.
fn trickleClient(gpa: std.mem.Allocator, transport: Transport, port: u16, held: Held, r: *TrickleResult) !void {
    const w = try openShort(gpa, transport, port);
    defer w.close();
    var req = try h2c.buildClientPreface(gpa, .empty);
    defer req.deinit(gpa);
    var sbuf: [64]u8 = undefined;
    const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = if (held == .stream) 0 else 1 << 30 }};
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
    try req.appendSlice(gpa, sbuf[0..sn]);
    if (held == .stream) try h2c.appendWindowUpdate(gpa, &req, 0, 1 << 30);
    try h2c.appendHeaders(gpa, &req, 1, "/big", true);
    try w.send(req.items);
    const t0 = nowNs();
    const give_up = t0 + 5 * std.time.ns_per_s;
    while (nowNs() < give_up) {
        while (true) {
            const s = w.next(nowNs() + 100 * ms) catch |err| switch (err) {
                error.Deadline => break,
                else => return err,
            } orelse return error.ClosedBeforeReset;
            if (s.sid != 1) continue;
            if (s.ftype == @intFromEnum(frame.FrameType.rst_stream)) {
                r.reset = true;
                r.code = rstCode(w);
                r.elapsed_ms = (nowNs() - t0) / ms;
                return;
            }
            if (s.ftype == @intFromEnum(frame.FrameType.data)) {
                r.data_bytes += w.last_payload.items.len;
                if (s.flags & 1 != 0) {
                    r.ended = true;
                    return;
                }
            }
        }
        // Only once the response has taken what the window allowed.
        if (held == .connection and r.data_bytes < 65_535) continue;
        var one: std.ArrayList(u8) = .empty;
        defer one.deinit(gpa);
        try h2c.appendWindowUpdate(gpa, &one, if (held == .stream) 1 else 0, 1);
        try w.send(one.items);
        r.updates += 1;
    }
    r.elapsed_ms = (nowNs() - t0) / ms;
}

fn trickleMain(gpa: std.mem.Allocator, transport: Transport, port: u16, held: Held, r: *TrickleResult) void {
    trickleClient(gpa, transport, port, held, r) catch |err| {
        r.err = err;
    };
    r.done.store(true, .release);
}

const SseResult = struct {
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
    events: usize = 0,
    ended: bool = false,
    reset: bool = false,
};

/// A client that reads everything: the SSE stream must end on its own.
fn sseClient(gpa: std.mem.Allocator, transport: Transport, port: u16, r: *SseResult) !void {
    const w = try Wire.open(gpa, transport, port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try h2c.appendHeaders(gpa, &req, 1, "/sse", true);
    try w.send(req.items);
    const deadline = nowNs() + 8 * std.time.ns_per_s;
    while (true) {
        const s = try w.next(deadline) orelse return error.SseClosed;
        if (s.sid != 1) continue;
        if (s.ftype == @intFromEnum(frame.FrameType.rst_stream)) {
            r.reset = true;
            return;
        }
        if (s.ftype == @intFromEnum(frame.FrameType.data)) {
            if (std.mem.indexOf(u8, w.last_payload.items, "tick") != null) r.events += 1;
            if (s.flags & 1 != 0) {
                r.ended = true;
                return;
            }
        }
    }
}

fn sseMain(gpa: std.mem.Allocator, transport: Transport, port: u16, r: *SseResult) void {
    sseClient(gpa, transport, port, r) catch |err| {
        r.err = err;
    };
    r.done.store(true, .release);
}

fn runResponseRate(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    return runResponseRateHeld(rt, gpa, transport, .stream);
}

fn runConnectionRate(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    return runResponseRateHeld(rt, gpa, transport, .connection);
}

fn runResponseRateHeld(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, held: Held) !void {
    var limits = starh2.Limits.defaults;
    setResponseGrace(&limits, response_grace_ms * ms);
    setLimit(&limits, "response_min_rate_bytes_per_s", rate_limit_bps);
    var run: Running = undefined;
    try startServer(rt, gpa, if (transport == .tls) .tls else .h2c, limits, &run);
    defer run.stop();
    const resets_before = counter(conn_mod, "response_rate_resets");
    const rule_before = counter(conn_mod, "response_rate_rule_resets");
    big_error.store(0, .release);
    big_done.store(false, .release);
    big_rounds = if (held == .stream) 1 else 4;
    defer big_rounds = 1;

    var trickle: TrickleResult = .{};
    var sse: SseResult = .{};
    const tt = try std.Thread.spawn(.{}, trickleMain, .{ gpa, transport, run.port(), held, &trickle });
    const ts = try std.Thread.spawn(.{}, sseMain, .{ gpa, transport, run.port(), &sse });
    zio.sleep(.fromMilliseconds(200)) catch {};
    try serveGood(gpa, transport, run.port());
    while (!trickle.done.load(.acquire) or !sse.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    tt.join();
    ts.join();
    if (trickle.err) |err| return err;
    if (sse.err) |err| return err;
    const handler_deadline = nowNs() + 2 * std.time.ns_per_s;
    while (!big_done.load(.acquire) and nowNs() < handler_deadline) zio.sleep(.fromMilliseconds(5)) catch {};
    const fired = counter(conn_mod, "response_rate_resets") - resets_before;
    const rule_fired = counter(conn_mod, "response_rate_rule_resets") - rule_before;
    std.debug.print("hardening {s} response rate, {s} window: 1-byte window stream reset={} (code {d}) after {d} ms, {d} bytes taken over {d} updates; handler ended {s}; sparse SSE {d} events ended={} reset={}; response_rate_resets +{d} (rate rule +{d})\n", .{
        @tagName(transport),                                      @tagName(held), trickle.reset, trickle.code, trickle.elapsed_ms, trickle.data_bytes, trickle.updates,
        @tagName(@as(BigEnd, @enumFromInt(big_error.load(.acquire)))), sse.events, sse.ended, sse.reset, fired, rule_fired,
    });
    try std.testing.expect(trickle.reset);
    // Bytes kept moving, one at a time, so it was the rate that ended the
    // stream and not the grace without progress.
    try std.testing.expect(trickle.updates >= 2);
    try std.testing.expect(trickle.data_bytes >= @as(usize, if (held == .stream) 2 else 65_535 + 2));
    try std.testing.expectEqual(@as(u32, @intFromEnum(frame.ErrorCode.cancel)), trickle.code);
    try std.testing.expect(trickle.elapsed_ms + 50 >= response_grace_ms);
    try std.testing.expectEqual(BigEnd.slow_consumer, @as(BigEnd, @enumFromInt(big_error.load(.acquire))));
    try std.testing.expect(sse.ended and !sse.reset);
    try std.testing.expectEqual(@as(usize, sse_events), sse.events);
    try std.testing.expectEqual(@as(u64, 1), fired);
    // The rate rule decided it, not a grace with nothing taken: bytes kept
    // moving, too few of them.
    try std.testing.expectEqual(@as(u64, 1), rule_fired);
}

test "hardening: h2c a zero window opened one byte at a time is reset by the response rate; a sparse SSE stream is not" {
    try runOnRuntime(runResponseRate, .h2c);
}

test "hardening: tls a zero window opened one byte at a time is reset by the response rate; a sparse SSE stream is not" {
    try runOnRuntime(runResponseRate, .tls);
}

test "hardening: h2c a connection window opened one byte at a time resets the stream it holds" {
    try runOnRuntime(runConnectionRate, .h2c);
}

test "hardening: tls a connection window opened one byte at a time resets the stream it holds" {
    try runOnRuntime(runConnectionRate, .tls);
}

/// The bytes left the scheduler (the stream's slot drained) and wait in the
/// writer, which the socket does not drain: the socket's clock follows them
/// and resets their stream. h2c only: the write hold (`test_hold_writes`)
/// holds the h2c WritePump, but on TLS it holds the connection's actor,
/// which runs the clock. The hold stops every pump in the process, so no
/// second client can be served during it.
const QueuedClient = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    err: ?anyerror = null,
    ready: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    release: std.atomic.Value(bool) = .init(false),

    fn main(c: *QueuedClient) void {
        c.run() catch |err| {
            c.err = err;
        };
        c.done.store(true, .release);
    }

    /// Request /queued and /finish, read until both HEADERS, then read
    /// nothing more.
    fn run(c: *QueuedClient) !void {
        const w = try Wire.open(c.gpa, c.transport, c.port);
        defer w.close();
        var req = try h2c.buildClientPrefaceAndSettings(c.gpa);
        defer req.deinit(c.gpa);
        try h2c.appendWindowUpdate(c.gpa, &req, 0, 1 << 30);
        try h2c.appendHeaders(c.gpa, &req, 1, "/queued", true);
        try h2c.appendHeaders(c.gpa, &req, 3, "/finish", true);
        try w.send(req.items);
        const deadline = nowNs() + 3 * std.time.ns_per_s;
        var seen: u8 = 0;
        while (seen != 3) {
            const s = try w.next(deadline) orelse return error.ClosedBeforeHeaders;
            if (s.ftype != @intFromEnum(frame.FrameType.headers)) continue;
            if (s.sid == 1) seen |= 1;
            if (s.sid == 3) seen |= 2;
        }
        c.ready.store(true, .release);
        while (!c.release.load(.acquire)) threadSleepMs(5);
    }
};

/// Bytes that left the scheduler wait in the writer, and the socket takes
/// none of them (the h2c pump is held): one handler waits on its flush, one
/// on `finish`, whose END_STREAM DATA is the last thing it wrote. Neither
/// stream has bytes in the scheduler any more, so only a clock on the socket
/// itself can end the wait: the write-byte timeout closes the connection
/// (`Limits.write_byte_timeout_ns`), and both handlers end. h2c only: on TLS
/// the hold stops the connection's actor, which runs the clock. The hold
/// stops every pump in the process, so no second client is served here; the
/// socket-level tests below do that.
fn runQueuedStall(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    const wire_pump = starh2.edge.wire_pump;
    var limits = starh2.Limits.defaults;
    setLimit(&limits, "write_byte_timeout_ns", @as(u64, write_byte_ms * ms));
    var run: Running = undefined;
    try startServer(rt, gpa, .h2c, limits, &run);
    defer run.stop();
    queued_gate.store(false, .release);
    queued_end.store(0, .release);
    finish_end.store(0, .release);
    queued_started.store(false, .release);
    defer queued_gate.store(true, .release);
    defer wire_pump.test_hold_writes.store(false, .release);

    var c: QueuedClient = .{ .gpa = gpa, .transport = transport, .port = run.port() };
    const t = try std.Thread.spawn(.{}, QueuedClient.main, .{&c});
    defer {
        c.release.store(true, .release);
        t.join();
    }
    while (!c.ready.load(.acquire)) {
        if (c.done.load(.acquire)) return c.err orelse error.QueuedClientEnded;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
    const held_before = wire_pump.test_hold_entered.load(.acquire);
    wire_pump.test_hold_writes.store(true, .release);
    queued_gate.store(true, .release);
    const t0 = nowNs();
    while (wire_pump.test_hold_entered.load(.acquire) == held_before) {
        if (nowNs() - t0 > 3 * std.time.ns_per_s) return error.PumpNeverHeld;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
    const deadline = t0 + write_byte_ms * ms + 3 * std.time.ns_per_s;
    while (nowNs() < deadline) {
        if (run.server.active_connections.load(.acquire) == 0 and queued_end.load(.acquire) != 0 and finish_end.load(.acquire) != 0) break;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    const ended_ms = (nowNs() - t0) / ms;
    const conns = run.server.active_connections.load(.acquire);
    wire_pump.test_hold_writes.store(false, .release);
    std.debug.print("hardening h2c bytes held in the writer: connection gone={} after {d} ms (write-byte timeout {d} ms); flush waiter ended {s}, finish waiter ended {s}\n", .{
        conns == 0,                                                    ended_ms, write_byte_ms,
        @tagName(@as(BigEnd, @enumFromInt(queued_end.load(.acquire)))), @tagName(@as(BigEnd, @enumFromInt(finish_end.load(.acquire)))),
    });
    try std.testing.expectEqual(@as(usize, 0), conns);
    try std.testing.expect(queued_end.load(.acquire) != 0);
    try std.testing.expect(finish_end.load(.acquire) != 0);
    try std.testing.expect(ended_ms + 50 >= write_byte_ms);
}

/// Fable F2: a body that uses the stream window exactly, then a bare
/// END_STREAM. The window is 0 when the END_STREAM is ready, and it needs
/// none (RFC 9113 6.9.1): it must go out, not wait for credit the client
/// never sends. Held, nothing would ever end the stream.
const ExactResult = struct {
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
    data_bytes: usize = 0,
    end_ms: ?u64 = null,
};

/// Which window the body uses exactly: the stream's (the client's initial
/// window is the body length) or the connection's (the stream window is
/// huge, and the client never adds connection credit to the initial 65,535).
const ExactWindow = enum { stream, connection };

fn exactClientMain(gpa: std.mem.Allocator, transport: Transport, port: u16, which: ExactWindow, r: *ExactResult) void {
    exactClient(gpa, transport, port, which, r) catch |err| {
        r.err = err;
    };
    r.done.store(true, .release);
}

fn exactClient(gpa: std.mem.Allocator, transport: Transport, port: u16, which: ExactWindow, r: *ExactResult) !void {
    const w = try Wire.open(gpa, transport, port);
    defer w.close();
    var req = try h2c.buildClientPreface(gpa, .empty);
    defer req.deinit(gpa);
    var sbuf: [64]u8 = undefined;
    const window: u32 = if (which == .stream) exact_len else 1 << 30;
    const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = window }};
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
    try req.appendSlice(gpa, sbuf[0..sn]);
    try h2c.appendHeaders(gpa, &req, 1, if (which == .stream) "/exact" else "/exact-conn", true);
    const t0 = nowNs();
    try w.send(req.items);
    // Reads everything, grants nothing more.
    const deadline = t0 + 3 * std.time.ns_per_s;
    while (nowNs() < deadline) {
        const f = (w.next(deadline) catch |err| switch (err) {
            error.Deadline, error.WouldBlock => continue,
            else => return err,
        }) orelse return error.ServerClosed;
        if (f.sid != 1) continue;
        if (f.ftype == @intFromEnum(frame.FrameType.rst_stream)) return error.StreamReset;
        if (f.ftype == @intFromEnum(frame.FrameType.data)) r.data_bytes += w.last_payload.items.len;
        if (f.flags & 1 != 0 and (f.ftype == @intFromEnum(frame.FrameType.data) or f.ftype == @intFromEnum(frame.FrameType.headers))) {
            r.end_ms = (nowNs() - t0) / ms;
            return;
        }
    }
}

fn runExactWindow(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    return runExact(rt, gpa, transport, .stream);
}

fn runExactConnWindow(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    return runExact(rt, gpa, transport, .connection);
}

fn runExact(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, which: ExactWindow) !void {
    const ep: Endpoint = if (transport == .tls) .tls else .h2c;
    var run: Running = undefined;
    try startServer(rt, gpa, ep, starh2.Limits.defaults, &run);
    defer run.stop();
    exact_end.store(0, .release);
    var r: ExactResult = .{};
    const th = try std.Thread.spawn(.{}, exactClientMain, .{ gpa, if (ep == .tls) Transport.tls else Transport.h2c, run.port(), which, &r });
    while (!r.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    th.join();
    std.debug.print("hardening {s} exact {s} window: {d} body bytes, END_STREAM at {?d} ms, handler ended {s}, err={?}\n", .{
        @tagName(ep), @tagName(which), r.data_bytes, r.end_ms, @tagName(@as(BigEnd, @enumFromInt(exact_end.load(.acquire)))), r.err,
    });
    if (r.err) |err| return err;
    try std.testing.expectEqual(@as(usize, if (which == .stream) exact_len else exact_conn_len), r.data_bytes);
    try std.testing.expect(r.end_ms != null);
    try std.testing.expect(r.end_ms.? < 1000);
}

/// astra 2 / Fable F1: a client that keeps a response moving keeps the
/// writer from draining. A request body on the same connection that used
/// all its credit, then stopped, must still be ended by its body clock: the
/// WINDOW_UPDATE credit queued behind the response counts as the peer's
/// after one body idle period (`Session.unsentDeadlineNs`), not only when
/// the writer drains.
const body_idle_ms = 600;

fn runQueuedCredit(rt: *zio.Runtime, gpa: std.mem.Allocator, _: Transport) !void {
    var limits = starh2.Limits.defaults;
    // Only the body clock is under test: no socket clock, no rate rule.
    setLimit(&limits, "write_byte_timeout_ns", @as(u64, 0));
    setLimit(&limits, "request_body_idle_timeout_ns", @as(u64, body_idle_ms * ms));
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/starh2-hard-{d}-credit.sock", .{std.c.getpid()});
    var run: Running = undefined;
    try wbStart(rt, gpa, .h2c_unix, limits, path, &run);
    defer run.stop();
    var r: CreditResult = .{};
    const th = try std.Thread.spawn(.{}, creditClientMain, .{ gpa, path, &r });
    while (!r.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    th.join();
    std.debug.print("hardening queued credit: body reset after {?d} ms (idle {d} ms); response bytes read meanwhile {d}; err={?}\n", .{ r.reset_ms, body_idle_ms, r.read, r.err });
    if (r.err) |err| return err;
    // The response kept moving the whole time.
    try std.testing.expect(r.read > 0);
    // The cap (one idle period) and the idle rule, with slack.
    try std.testing.expect(r.reset_ms != null);
    try std.testing.expect(r.reset_ms.? < 4 * body_idle_ms);
}

const CreditResult = struct {
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
    reset_ms: ?u64 = null,
    read: usize = 0,
};

fn creditClientMain(gpa: std.mem.Allocator, path: []const u8, r: *CreditResult) void {
    creditClient(gpa, path, r) catch |err| {
        r.err = err;
    };
    r.done.store(true, .release);
}

fn creditClient(gpa: std.mem.Allocator, path: []const u8, r: *CreditResult) !void {
    const w = try Wire.connectUnix(gpa, .h2c, path, wb_rcvbuf);
    defer w.close();
    var req = try h2c.buildClientPreface(gpa, .empty);
    defer req.deinit(gpa);
    var sbuf: [64]u8 = undefined;
    const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 1 << 30 }};
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
    try req.appendSlice(gpa, sbuf[0..sn]);
    try h2c.appendWindowUpdate(gpa, &req, 0, 1 << 30);
    try h2c.appendHeaders(gpa, &req, 1, "/stream", true);
    // The POST uses its whole stream window (and the connection's), then
    // sends nothing more.
    try postHeaders(gpa, &req, 3, "100000");
    const piece: [16384]u8 = @splat('p');
    var left: usize = 65535;
    while (left > 0) {
        const n = @min(left, piece.len);
        try h2c.appendData(gpa, &req, 3, piece[0..n], false);
        left -= n;
    }
    try w.send(req.items);
    const t0 = nowNs();
    const deadline = t0 + 4 * std.time.ns_per_s;
    // Reads one frame every 20 ms: slower than the response is written, so
    // the writer always holds bytes, and the connection keeps moving.
    while (nowNs() < deadline) {
        threadSleepMs(20);
        const f = (w.next(deadline) catch |err| switch (err) {
            error.Deadline, error.WouldBlock => continue,
            else => return err,
        }) orelse return error.ServerClosed;
        if (f.ftype == @intFromEnum(frame.FrameType.data)) r.read += w.last_payload.items.len;
        if (f.sid == 3 and f.ftype == @intFromEnum(frame.FrameType.rst_stream)) {
            r.reset_ms = (nowNs() - t0) / ms;
            return;
        }
    }
}

test "hardening: h2c a body waiting on credit queued behind a moving response is still ended by its clock" {
    try runOnRuntime(runQueuedCredit, .h2c);
}

test "hardening: h2c a bare END_STREAM goes out on a stream window of 0" {
    try runOnRuntime(runExactWindow, .h2c);
}

test "hardening: tls a bare END_STREAM goes out on a stream window of 0" {
    try runOnRuntime(runExactWindow, .tls);
}

test "hardening: h2c a bare END_STREAM goes out on a connection window of 0" {
    try runOnRuntime(runExactConnWindow, .h2c);
}

test "hardening: tls a bare END_STREAM goes out on a connection window of 0" {
    try runOnRuntime(runExactConnWindow, .tls);
}

test "hardening: h2c bytes in the writer that the socket never takes end the connection, flush and finish waits included" {
    try runOnRuntime(runQueuedStall, .h2c);
}

// ---------------------------------------------------------------------------
// The write-byte timeout on real sockets: a peer that stops reading with
// response bytes waiting loses its connection; a slow reader that keeps
// taking bytes keeps it.

const write_byte_ms = 1000;

const Sock = union(enum) { tcp: u16, unix: []const u8 };

fn rawConnect(sock: Sock, rcvbuf: ?c_int) !c_int {
    const fd = switch (sock) {
        .tcp => std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0),
        .unix => std.c.socket(std.c.AF.UNIX, std.c.SOCK.STREAM, 0),
    };
    if (fd < 0) return error.Socket;
    errdefer _ = std.c.close(fd);
    if (rcvbuf) |n| _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVBUF, std.mem.asBytes(&n), @sizeOf(c_int));
    switch (sock) {
        .tcp => |port| {
            var addr: std.c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = std.mem.nativeToBig(u32, 0x7f000001) };
            if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
        },
        .unix => |path| {
            var addr: std.c.sockaddr.un = .{ .path = @splat(0) };
            @memcpy(addr.path[0..path.len], path);
            if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.un)) != 0) return error.Connect;
        },
    }
    return fd;
}

/// Every place a response is written to a socket: HTTP/2 (h2c and TLS) and
/// HTTP/1.1 (cleartext and TLS), over a unix socket and over TCP loopback.
const WbTransport = enum { h2c_unix, h2c_tcp, tls_unix, h1c_tcp, h1_tls_unix };

/// The tests run the timeout at 1 s, not the 60 s default, and scale the
/// slow reader to it. The kernel shows the server a reader's progress in
/// steps, not byte by byte: on a unix socket a whole server write at a time
/// (the sender's buffer stays charged until the reader finishes that
/// write's buffer); on TCP a large part of the send buffer at a time (Linux
/// wakes a writer only once much of the queued bytes is freed, and grows
/// the send buffer to MBs on loopback). A reader survives a timeout only if
/// it takes at least one step per timeout, so the rate a test reader needs
/// is the step divided by the timeout: at the 60 s default the same reader
/// would need a sixtieth of it. These rates show a step several times per
/// second on every transport here, Linux and macOS (`wbRead` gives the
/// rates measured). They
/// still read far slower than the flood is written, so bytes always wait
/// and the clock always runs.
const WbRead = struct { bytes: usize, period_ms: u32 };
fn wbRead(t: WbTransport) WbRead {
    return switch (t) {
        // Measured on nachos at a 1 s timeout: 320 kB/s kept every unix
        // reader, 80 kB/s lost them; 3.2 MB/s kept every TCP reader,
        // 1.3 MB/s lost one of four. These are about ten times the rate
        // that failed.
        .h2c_unix, .tls_unix, .h1_tls_unix => .{ .bytes = 64 * 1024, .period_ms = 20 },
        .h2c_tcp, .h1c_tcp => .{ .bytes = 256 * 1024, .period_ms = 20 },
    };
}
const wb_read_max = 256 * 1024;
/// Unix sockets: a small receive buffer (Linux ignores it there; macOS
/// does not). TCP keeps the kernel's default, as a real client does: a
/// tiny one throttles the reader below the rate it asks for.
const wb_rcvbuf: c_int = 4096;

const WbClient = struct {
    wire: ?*Wire = null,
    fd: c_int = -1,

    fn close(c: *WbClient) void {
        if (c.wire) |w| {
            w.close();
        } else if (c.fd >= 0) {
            _ = std.c.close(c.fd);
        }
        c.* = .{};
    }
};

fn wbStart(rt: *zio.Runtime, gpa: std.mem.Allocator, t: WbTransport, limits: starh2.Limits, path: []const u8, run: *Running) !void {
    const io = rt.io();
    run.* = .{ .server = undefined, .gpa = gpa };
    var tls_cfg: ?starh2.TlsConfig = null;
    if (t == .tls_unix or t == .h1_tls_unix) {
        run.cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
        run.key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
        tls_cfg = .{ .certificate_chain_pem = run.cert, .private_key_pem = run.key };
    }
    const lo = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const endpoint: starh2.EndpointConfig = switch (t) {
        .h2c_unix => .{ .h2c_unix = path },
        .h2c_tcp => .{ .h2c_prior_knowledge = lo },
        .tls_unix, .h1_tls_unix => .{ .tls_unix = path },
        .h1c_tcp => .{ .h1c = lo },
    };
    run.server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{endpoint},
        .routes = &routes,
        .tls = tls_cfg,
        .limits = limits,
    });
    run.handle = try rt.spawn(starh2.Server.serve, .{ &run.server, gpa });
    try run.server.waitUntilListening(5 * std.time.ns_per_s);
}

/// A client that asks for the flood. `attacker` on HTTP/2 is also Fable F1's
/// shape: a POST that sent part of its body and four streams of HEADERS
/// only, so request bodies wait on the connection too (a peer that stops
/// reading must not keep them for ever).
fn wbOpen(gpa: std.mem.Allocator, t: WbTransport, run: *const Running, path: []const u8, attacker: bool) !WbClient {
    switch (t) {
        .h1c_tcp => return .{ .fd = try h1Get(.{ .tcp = run.port() }, null, "GET /flood HTTP/1.1\r\nHost: localhost\r\n\r\n") },
        .h1_tls_unix => {
            const w = try Wire.connectUnix(gpa, .tls, path, wb_rcvbuf);
            errdefer w.close();
            w.alpn = "http/1.1";
            try w.handshake();
            try w.sendUnchecked("GET /flood HTTP/1.1\r\nHost: localhost\r\n\r\n");
            return .{ .wire = w, .fd = w.fd };
        },
        .h2c_unix, .h2c_tcp, .tls_unix => {
            const transport: Transport = if (t == .tls_unix) .tls else .h2c;
            const w = if (t == .h2c_tcp) try Wire.connectRecvBuf(gpa, transport, run.port(), null) else try Wire.connectUnix(gpa, transport, path, wb_rcvbuf);
            errdefer w.close();
            try w.handshake();
            // Windows far larger than any kernel buffer: the flood is held by
            // the socket, never by flow control (a 1 MiB stream window fits
            // in a Linux loopback send buffer, and the stream clock would
            // decide instead).
            var req = try h2c.buildClientPreface(gpa, .empty);
            defer req.deinit(gpa);
            var sbuf: [64]u8 = undefined;
            const settings = [_]frame.Setting{ .{ .id = .max_concurrent_streams, .value = 256 }, .{ .id = .initial_window_size, .value = 1 << 30 } };
            const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
            try req.appendSlice(gpa, sbuf[0..sn]);
            try h2c.appendWindowUpdate(gpa, &req, 0, 1 << 30);
            try h2c.appendHeaders(gpa, &req, 1, "/flood", true);
            if (attacker) {
                try postHeaders(gpa, &req, 3, "100000");
                try h2c.appendData(gpa, &req, 3, &(@as([1000]u8, @splat('p'))), false);
                for (0..4) |i| try postHeaders(gpa, &req, @intCast(5 + 2 * i), "1000");
            }
            try w.send(req.items);
            return .{ .wire = w, .fd = w.fd };
        },
    }
}

/// `wbOpen` on a thread of its own: its connect and TLS handshake block, and
/// a blocking call on a zio executor stalls every connection placed there.
fn wbOpenOffExecutor(gpa: std.mem.Allocator, t: WbTransport, run: *const Running, path: []const u8, attacker: bool) !WbClient {
    const Open = struct {
        client: WbClient = .{},
        err: ?anyerror = null,
        done: std.atomic.Value(bool) = .init(false),
        fn main(o: *@This(), gpa_: std.mem.Allocator, t_: WbTransport, run_: *const Running, path_: []const u8, attacker_: bool) void {
            if (wbOpen(gpa_, t_, run_, path_, attacker_)) |c| {
                o.client = c;
            } else |err| {
                o.err = err;
            }
            o.done.store(true, .release);
        }
    };
    var o: Open = .{};
    const th = try std.Thread.spawn(.{}, Open.main, .{ &o, gpa, t, run, path, attacker });
    while (!o.done.load(.acquire)) zio.sleep(.fromMilliseconds(2)) catch {};
    th.join();
    if (o.err) |err| return err;
    return o.client;
}

const AttackResult = struct {
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
    release: std.atomic.Value(bool) = .init(false),
    /// The slow reader: bytes taken, and whether the server ever closed it.
    slow_bytes: usize = 0,
    slow_closed: bool = false,
};

/// The slow reader: `rd.bytes` every `rd.period_ms`, raw bytes (TLS records
/// are not decrypted: taking them is what counts), until released.
fn slowReader(fd: c_int, r: *AttackResult, rd: WbRead) void {
    var buf: [wb_read_max]u8 = undefined;
    while (!r.release.load(.acquire)) {
        threadSleepMs(rd.period_ms);
        var pfd = [1]std.c.pollfd{.{ .fd = fd, .events = std.c.POLL.IN, .revents = 0 }};
        if (std.c.poll(&pfd, 1, 0) <= 0) continue;
        const k = std.c.recv(fd, &buf, rd.bytes, 0);
        if (k <= 0) {
            r.slow_closed = true;
            break;
        }
        r.slow_bytes += @intCast(k);
    }
    r.done.store(true, .release);
}

fn h1Get(sock: Sock, rcvbuf: ?c_int, req: []const u8) !c_int {
    const fd = try rawConnect(sock, rcvbuf);
    if (std.c.send(fd, req.ptr, req.len, 0) != req.len) {
        _ = std.c.close(fd);
        return error.Send;
    }
    return fd;
}

fn runWriteByte(rt: *zio.Runtime, gpa: std.mem.Allocator, t: WbTransport) !void {
    return runWriteByteRead(rt, gpa, t, wbRead(t));
}

fn runWriteByteRead(rt: *zio.Runtime, gpa: std.mem.Allocator, t: WbTransport, rd: WbRead) !void {
    var limits = starh2.Limits.defaults;
    setLimit(&limits, "write_byte_timeout_ns", @as(u64, write_byte_ms * ms));
    var path_buf: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/starh2-hard-{d}-{s}.sock", .{ std.c.getpid(), @tagName(t) });
    var run: Running = undefined;
    try wbStart(rt, gpa, t, limits, path, &run);
    defer run.stop();
    flood_ended.store(0, .release);
    flood_slow.store(0, .release);

    // The slow reader first: it must keep its connection throughout.
    var slow_client = try wbOpenOffExecutor(gpa, t, &run, path, false);
    defer slow_client.close();
    var slow: AttackResult = .{};
    const ts = try std.Thread.spawn(.{}, slowReader, .{ slow_client.fd, &slow, rd });
    defer {
        slow.release.store(true, .release);
        ts.join();
    }

    // The attacker asks for the flood and never reads. `t0` is before it
    // connects, so no close can be earlier than the timeout after it.
    const t0 = nowNs();
    var attack_client = try wbOpenOffExecutor(gpa, t, &run, path, true);
    defer attack_client.close();
    try waitCount("active_connections", &run.server.active_connections, 2, 3 * std.time.ns_per_s);
    // A well-behaved client is served while the attacker holds its bytes.
    try serveGoodWb(gpa, t, &run, path);
    // The attacker's connection ends within the timeout and some slack.
    const deadline = t0 + write_byte_ms * ms + 4 * std.time.ns_per_s;
    while (run.server.active_connections.load(.acquire) != 1) {
        if (nowNs() >= deadline) break;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    const gone_ms = (nowNs() - t0) / ms;
    const conns = run.server.active_connections.load(.acquire);
    // The slow reader keeps going for two more timeouts.
    const slow_before = slow.slow_bytes;
    zio.sleep(.fromMilliseconds(2 * write_byte_ms)) catch {};
    const slow_after = slow.slow_bytes;
    const conns_after = run.server.active_connections.load(.acquire);
    std.debug.print("hardening {s} write-byte timeout {d} ms: attacker gone={} after {d} ms; slow reader ({d} B every {d} ms) took {d} bytes in the last 2 s, closed={}, connections now {d}; flood handlers ended {d}, {d} of them with SlowConsumer\n", .{
        @tagName(t), write_byte_ms, conns == 1, gone_ms, rd.bytes, rd.period_ms, slow_after - slow_before, slow.slow_closed, conns_after, flood_ended.load(.acquire), flood_slow.load(.acquire),
    });
    try std.testing.expect(!slow.slow_closed);
    try std.testing.expect(slow_after > slow_before);
    try std.testing.expectEqual(@as(usize, 1), conns_after);
    try std.testing.expectEqual(@as(usize, 1), conns);
    try std.testing.expect(gone_ms >= write_byte_ms);
    // Well under twice the timeout: progress is dated when the socket took
    // it, not when the connection looked (astra 4).
    try std.testing.expect(gone_ms < write_byte_ms + 600);
    // The attacker's handler learned why (astra 5): it ended with
    // SlowConsumer, not a bare WriteFailed. (By now, two timeouts later.)
    try std.testing.expectEqual(@as(usize, 1), flood_slow.load(.acquire));
}

fn serveGoodWb(gpa: std.mem.Allocator, t: WbTransport, run: *const Running, path: []const u8) !void {
    var r: GoodResult = .{};
    const th = switch (t) {
        .h2c_tcp => try std.Thread.spawn(.{}, goodAnyMain, .{ gpa, Endpoint.h2c, run.port(), &r }),
        .h1c_tcp => try std.Thread.spawn(.{}, goodAnyMain, .{ std.heap.page_allocator, Endpoint.h1c, run.port(), &r }),
        .h2c_unix => try std.Thread.spawn(.{}, goodUnixMain, .{ gpa, Transport.h2c, path, &r }),
        // The TLS endpoint serves HTTP/2 to a client that offers it.
        .tls_unix, .h1_tls_unix => try std.Thread.spawn(.{}, goodUnixMain, .{ gpa, Transport.tls, path, &r }),
    };
    while (!r.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    th.join();
    if (r.err) |err| return err;
}

fn goodUnixMain(gpa: std.mem.Allocator, transport: Transport, path: []const u8, r: *GoodResult) void {
    goodUnix(gpa, transport, path) catch |err| {
        r.err = err;
    };
    r.done.store(true, .release);
}

fn goodUnix(gpa: std.mem.Allocator, transport: Transport, path: []const u8) !void {
    const w = try Wire.connectUnix(gpa, transport, path, null);
    defer w.close();
    try w.handshake();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try h2c.appendHeaders(gpa, &req, 1, "/hello", true);
    try w.send(req.items);
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (true) {
        const s = try w.next(deadline) orelse return error.GoodClientClosed;
        if (s.sid != 1) continue;
        if (s.ftype == @intFromEnum(frame.FrameType.rst_stream)) return error.GoodClientReset;
        if ((s.ftype == @intFromEnum(frame.FrameType.data) or s.ftype == @intFromEnum(frame.FrameType.headers)) and s.flags & 1 != 0) return;
    }
}

fn runWriteByteOn(t: WbTransport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runWriteByte, .{ rt, gpa, t });
    try h.join();
}

/// Per-byte progress, not per finished write: a reader taking 1 KiB every
/// 100 ms (10 kB/s) takes one 16 KiB write of the flood in 1.6 s, longer
/// than the 1 s timeout, so only a count of each partial write shows it
/// moving. macOS only: a macOS unix socket accepts the part of a write that
/// fits as the reader takes bytes. On Linux the kernel frees a unix
/// sender's buffer one whole write at a time and wakes a TCP writer only
/// after a large share of its queue drains, so there a partial write shows
/// no progress sooner than the finished write: measured on nachos (Linux
/// 7.2), h2c over unix readers at 320 kB/s to 1.3 MB/s kept their
/// connection with and without the count, and at 80 kB/s lost it with it.
fn runWriteBytePartial(rt: *zio.Runtime, gpa: std.mem.Allocator, _: Transport) !void {
    if (builtin.os.tag != .macos) {
        std.debug.print("hardening partial writes: skipped, macOS only (a Linux socket shows a reader's progress one whole write at a time)\n", .{});
        return error.SkipZigTest;
    }
    return runWriteByteRead(rt, gpa, .h2c_unix, .{ .bytes = 1024, .period_ms = 100 });
}

test "hardening: macOS h2c over a unix socket, a reader slower than one write per timeout keeps its connection (progress per partial write)" {
    try runOnRuntime(runWriteBytePartial, .h2c);
}

test "hardening: h2c over a unix socket, a peer that stops reading loses its connection at the write-byte timeout; a slow reader keeps its" {
    try runWriteByteOn(.h2c_unix);
}

test "hardening: h2c over TCP, a peer that stops reading loses its connection at the write-byte timeout; a slow reader keeps its" {
    try runWriteByteOn(.h2c_tcp);
}

test "hardening: tls over a unix socket, a peer that stops reading loses its connection at the write-byte timeout; a slow reader keeps its" {
    try runWriteByteOn(.tls_unix);
}

test "hardening: h1c over TCP, a peer that stops reading loses its connection at the write-byte timeout; a slow reader keeps its" {
    try runWriteByteOn(.h1c_tcp);
}

test "hardening: HTTP/1.1 over TLS, a peer that stops reading loses its connection at the write-byte timeout; a slow reader keeps its" {
    try runWriteByteOn(.h1_tls_unix);
}

// ---------------------------------------------------------------------------
// 4. Accept errors (inventory row 4): a real EMFILE.

fn setSoftLimit(cur: std.posix.rlim_t) !void {
    var lim = try std.posix.getrlimit(.NOFILE);
    lim.cur = cur;
    try std.posix.setrlimit(.NOFILE, lim);
}

/// The lowest descriptor number free now. A soft limit of this plus one
/// leaves exactly one descriptor to open, since every lower one is in use.
fn lowestFreeFd() !c_int {
    const fd = std.c.dup(0);
    if (fd < 0) return error.Dup;
    _ = std.c.close(fd);
    return fd;
}

const RawConn = struct {
    fd: c_int,

    fn connect(port: u16) !RawConn {
        const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
        if (fd < 0) return error.Socket;
        errdefer _ = std.c.close(fd);
        var addr: std.c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = std.mem.nativeToBig(u32, 0x7f000001) };
        if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
        return .{ .fd = fd };
    }

    fn close(self: RawConn) void {
        _ = std.c.close(self.fd);
    }

    /// One HTTP/1.1 GET /hello; true when "200 ... ok" came back in time.
    fn get(self: RawConn, timeout_ms: u64) !bool {
        const req = "GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n";
        if (std.c.send(self.fd, req.ptr, req.len, 0) != req.len) return error.Send;
        var got: [512]u8 = undefined;
        var n: usize = 0;
        const deadline = nowNs() + timeout_ms * ms;
        while (std.mem.indexOf(u8, got[0..n], "\r\n\r\nok") == null) {
            if (nowNs() >= deadline) return false;
            var pfd = [1]std.c.pollfd{.{ .fd = self.fd, .events = std.c.POLL.IN, .revents = 0 }};
            if (std.c.poll(&pfd, 1, 50) <= 0) continue;
            const k = std.c.recv(self.fd, got[n..].ptr, got.len - n, 0);
            if (k <= 0) return false;
            n += @intCast(k);
        }
        return std.mem.startsWith(u8, got[0..n], "HTTP/1.1 200");
    }
};

const AcceptResult = struct {
    /// The server's accept error count, when the tree under test has one.
    errors: ?*const std.atomic.Value(u64),
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
    errors_before: u64 = 0,
    errors_in_window: u64 = 0,
    starved_served: bool = false,
    established_served: bool = false,
    after_served: bool = false,
    queued_served: bool = false,
    /// Descriptors that came free while the limit was down, taken by the
    /// test so the server could not have them.
    absorbed: Absorber = .{},
    go_restore: std.atomic.Value(bool) = .init(false),
    restored: std.atomic.Value(bool) = .init(false),

    fn errorCount(r: *const AcceptResult) u64 {
        return if (r.errors) |e| e.load(.acquire) else 0;
    }
};

/// Holds every descriptor that comes free while the soft limit is down.
/// The limit leaves exactly one descriptor, and the starved client takes
/// it; on Linux one more came free during the error window in every run on
/// nachos (who released it is not known: see `FdSnapshot`), and the
/// server's 10 ms retry took it. Taking it first keeps the error up for the
/// window. The assertions do not depend on winning that race: the error is
/// proven by the server's own count, not by the starved client going
/// unserved.
const Absorber = struct {
    fds: [16]c_int = undefined,
    n: usize = 0,
    /// Every number taken, in order, for the report.
    taken: [16]c_int = undefined,
    taken_n: usize = 0,

    fn absorb(self: *Absorber) void {
        while (self.n < self.fds.len) {
            const fd = std.c.dup(0);
            if (fd < 0) return;
            self.fds[self.n] = fd;
            self.n += 1;
            if (self.taken_n < self.taken.len) {
                self.taken[self.taken_n] = fd;
                self.taken_n += 1;
            }
        }
    }

    fn release(self: *Absorber) void {
        for (self.fds[0..self.n]) |fd| _ = std.c.close(fd);
        self.n = 0;
    }
};

/// Linux: what each descriptor below the limit pointed at just before the
/// limit went down (`/proc/self/fd`), so a descriptor that comes free in the
/// window can be named in the output. Reading a link takes no descriptor.
const FdSnapshot = struct {
    targets: [256][48]u8 = undefined,
    lens: [256]usize = @splat(0),

    fn take(self: *FdSnapshot, below: c_int) void {
        if (builtin.os.tag != .linux) return;
        var fd: c_int = 0;
        while (fd < below and fd < self.lens.len) : (fd += 1) {
            var path: [32]u8 = undefined;
            const p = std.mem.printSentinel(&path, "/proc/self/fd/{d}", .{fd}, 0) catch continue;
            const n = std.c.readlink(p.ptr, &self.targets[@intCast(fd)], self.targets[0].len);
            self.lens[@intCast(fd)] = if (n > 0) @intCast(n) else 0;
        }
    }

    fn target(self: *const FdSnapshot, fd: c_int) []const u8 {
        if (fd < 0 or fd >= self.lens.len) return "?";
        const i: usize = @intCast(fd);
        return if (self.lens[i] == 0) "(none)" else self.targets[i][0..self.lens[i]];
    }
};

/// Send one GET on `c`; then wait up to `timeout_ms` for its answer, taking
/// any descriptor that comes free every 5 ms meanwhile.
fn getAbsorbing(c: RawConn, timeout_ms: u64, absorber: *Absorber) !bool {
    const req = "GET /hello HTTP/1.1\r\nHost: localhost\r\n\r\n";
    if (std.c.send(c.fd, req.ptr, req.len, 0) != req.len) return error.Send;
    return awaitOk(c, timeout_ms, absorber);
}

fn awaitOk(c: RawConn, timeout_ms: u64, absorber: ?*Absorber) !bool {
    var got: [512]u8 = undefined;
    var n: usize = 0;
    const deadline = nowNs() + timeout_ms * ms;
    while (std.mem.indexOf(u8, got[0..n], "\r\n\r\nok") == null) {
        if (absorber) |a| a.absorb();
        if (nowNs() >= deadline) return false;
        var pfd = [1]std.c.pollfd{.{ .fd = c.fd, .events = std.c.POLL.IN, .revents = 0 }};
        if (std.c.poll(&pfd, 1, 5) <= 0) continue;
        const k = std.c.recv(c.fd, got[n..].ptr, got.len - n, 0);
        if (k <= 0) return false;
        n += @intCast(k);
    }
    return std.mem.startsWith(u8, got[0..n], "HTTP/1.1 200");
}

fn acceptClient(port: u16, r: *AcceptResult) !void {
    // An established, keep-alive connection: the well-behaved client.
    const established = try RawConn.connect(port);
    defer established.close();
    if (!try established.get(3000)) return error.EstablishedNotServed;

    const lim0 = try std.posix.getrlimit(.NOFILE);
    // One descriptor left: the client socket below takes it, so the
    // server's accept of that connection fails with EMFILE.
    const free_fd = try lowestFreeFd();
    var snapshot: FdSnapshot = .{};
    snapshot.take(free_fd);
    r.errors_before = r.errorCount();
    try setSoftLimit(@intCast(free_fd + 1));
    var restored = false;
    defer if (!restored) {
        r.absorbed.release();
        setSoftLimit(lim0.cur) catch {};
    };
    const starved = RawConn.connect(port) catch |err| {
        r.absorbed.release();
        setSoftLimit(lim0.cur) catch {};
        restored = true;
        return err;
    };
    defer starved.close();
    // The error happened: the server counts the failed accept. (The tree
    // before these defences has no count; there the window is fixed.)
    const error_deadline = nowNs() + 2 * std.time.ns_per_s;
    while (true) {
        r.absorbed.absorb();
        if (r.errors == null) {
            if (nowNs() >= error_deadline - 1700 * ms) break;
        } else if (r.errorCount() > r.errors_before or nowNs() >= error_deadline) break;
        threadSleepMs(1);
    }
    // While it holds: the starved client is not served (it cannot be, with
    // no descriptor for its accept), and the established one still is.
    r.starved_served = try getAbsorbing(starved, 600, &r.absorbed);
    r.established_served = try getAbsorbing(established, 2000, &r.absorbed);
    r.errors_in_window = r.errorCount();
    // Let the test read the counters while the error holds, then free
    // descriptors again.
    r.go_restore.store(true, .release);
    while (!r.restored.load(.acquire)) {
        r.absorbed.absorb();
        threadSleepMs(1);
    }
    r.absorbed.release();
    try setSoftLimit(lim0.cur);
    restored = true;
    if (builtin.os.tag == .linux and r.absorbed.taken_n != 0) {
        for (r.absorbed.taken[0..r.absorbed.taken_n]) |fd| {
            std.debug.print("hardening accept EMFILE: descriptor {d} came free in the window; before the limit went down it was {s}\n", .{ fd, snapshot.target(fd) });
        }
    }
    // macOS aborts the connection whose accept found no descriptor (XNU
    // `accept_nocancel`: "Don't put this back on the socket like we used to,
    // that results in a busy wait"); Linux leaves it queued, and the loop's
    // retry takes it once descriptors are free. Its GET was sent already.
    r.queued_served = awaitOk(starved, 4000, null) catch false;
    const after = try RawConn.connect(port);
    defer after.close();
    r.after_served = try after.get(4000);
}

fn acceptMain(port: u16, r: *AcceptResult) void {
    acceptClient(port, r) catch |err| {
        r.err = err;
    };
    r.go_restore.store(true, .release);
    r.done.store(true, .release);
}

fn runAcceptError(rt: *zio.Runtime, gpa: std.mem.Allocator, _: Transport) !void {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    var run: Running = undefined;
    try startServer(rt, gpa, .h1c, starh2.Limits.defaults, &run);
    defer run.stop();
    var r: AcceptResult = .{ .errors = if (@hasField(starh2.Server, "accept_errors")) &run.server.accept_errors else null };
    const t = try std.Thread.spawn(.{}, acceptMain, .{ run.port(), &r });
    while (!r.go_restore.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    r.restored.store(true, .release);
    while (!r.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
    t.join();
    if (r.err) |err| {
        std.debug.print("hardening accept EMFILE: client failed with {s}\n", .{@errorName(err)});
        return err;
    }
    const errors = r.errors_in_window - r.errors_before;
    std.debug.print("hardening accept EMFILE: accept errors in the window {d}; descriptors taken by the test in the window {d}; starved client served during the error={}, established client served={}; after descriptors freed: queued client served={}, new client served={}\n", .{
        errors, r.absorbed.taken_n, r.starved_served, r.established_served, r.queued_served, r.after_served,
    });
    // 1. The error happened: the server counted a failed accept.
    try std.testing.expect(errors > 0);
    // 2. The loop survived it: the endpoint kept serving its established
    //    connection, and accepted a new one after the error.
    try std.testing.expect(r.established_served);
    try std.testing.expect(r.after_served);
    // 3. What each OS does with the connection whose accept failed.
    if (builtin.os.tag.isDarwin()) {
        try std.testing.expect(!r.starved_served);
        try std.testing.expect(!r.queued_served);
    }
    if (builtin.os.tag == .linux) try std.testing.expect(r.starved_served or r.queued_served);
}

test "hardening: EMFILE on accept does not end the accept loop, and the endpoint accepts again once it clears" {
    try runOnRuntime(runAcceptError, .h2c);
}

// ---------------------------------------------------------------------------
// 5. The boot check: a server that needs more descriptors than it can get.

test "hardening: Server.init refuses max_connections above what RLIMIT_NOFILE allows" {
    if (builtin.os.tag == .windows) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    const lim = try std.posix.getrlimit(.NOFILE);
    // The most the process could raise its soft limit to. Above every
    // ceiling the server can reach: the hard limit, or on macOS
    // kern.maxfilesperproc when the hard limit says infinity.
    var ceiling: u64 = lim.max;
    if (builtin.os.tag.isDarwin()) {
        var per_proc: c_int = 0;
        var len: usize = @sizeOf(c_int);
        if (std.c.sysctlbyname("kern.maxfilesperproc", &per_proc, &len, null, 0) == 0 and per_proc > 0) {
            ceiling = @min(ceiling, @as(u64, @intCast(per_proc)));
        }
    }
    var limits = starh2.Limits.defaults;
    limits.max_connections = @intCast(@min(ceiling, 1 << 30));
    const got = starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h1c = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    if (got) |s| {
        var server = s;
        server.deinit(gpa);
        std.debug.print("hardening boot: Server.init accepted max_connections={d} with a descriptor ceiling of {d}\n", .{ limits.max_connections, ceiling });
        return error.InitAccepted;
    } else |err| {
        std.debug.print("hardening boot: Server.init with max_connections={d}, ceiling {d}: {s}\n", .{ limits.max_connections, ceiling, @errorName(err) });
        try std.testing.expectEqual(@as(anyerror, error.FileLimitTooLow), err);
    }
}

// ---------------------------------------------------------------------------
// MPTCP: every listener is plain TCP.

/// CVE-2026-98432 is a Linux kernel bug reached through MPTCP listeners, and
/// Go listens with MPTCP by default since 1.24. starh2 listens with
/// `std.Io.net.IpAddress.listen`, whose `protocol` defaults to `.tcp`, and
/// zio passes it through; this gate fails if either default ever changes.
/// Linux only: macOS has no MPTCP listeners and no SO_PROTOCOL to ask.
fn runListenProtocol(rt: *zio.Runtime, gpa: std.mem.Allocator, _: Transport) !void {
    if (builtin.os.tag != .linux) {
        std.debug.print("hardening listen protocol: skipped, Linux only (MPTCP and SO_PROTOCOL are Linux's)\n", .{});
        return error.SkipZigTest;
    }
    const io = rt.io();
    const cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert);
    const key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key);
    // IPv6 when this host has it: a ::1 socket that binds.
    const has_v6 = blk: {
        const fd = std.c.socket(std.c.AF.INET6, std.c.SOCK.STREAM, 0);
        if (fd < 0) break :blk false;
        defer _ = std.c.close(fd);
        var addr: std.c.sockaddr.in6 = .{ .port = 0, .flowinfo = 0, .addr = @splat(0), .scope_id = 0 };
        addr.addr[15] = 1;
        break :blk std.c.bind(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in6)) == 0;
    };
    var endpoints: [4]starh2.EndpointConfig = .{
        .{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) },
        .{ .tls = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) },
        .{ .h1c = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) },
        .{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp6("::1", 0) },
    };
    const n_endpoints: usize = if (has_v6) 4 else 3;
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = endpoints[0..n_endpoints],
        .routes = &routes,
        .tls = .{ .certificate_chain_pem = cert, .private_key_pem = key },
        .limits = .defaults,
    });
    defer server.deinit(gpa);
    var handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        handle.join() catch {};
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    for (server.listeners[0..n_endpoints], 0..) |listener, i| {
        var proto: c_int = -1;
        var len: std.posix.socklen_t = @sizeOf(c_int);
        const rc = std.c.getsockopt(listener.socket.handle, std.c.SOL.SOCKET, std.os.linux.SO.PROTOCOL, @ptrCast(&proto), &len);
        std.debug.print("hardening listen protocol: endpoint {d} ({s}) SO_PROTOCOL={d} (rc {d})\n", .{ i, @tagName(endpoints[i]), proto, rc });
        try std.testing.expectEqual(@as(c_int, 0), rc);
        // IPPROTO_TCP, not IPPROTO_MPTCP (262).
        try std.testing.expectEqual(@as(c_int, 6), proto);
    }
}

test "hardening: every listener is plain TCP, never MPTCP (Linux)" {
    try runOnRuntime(runListenProtocol, .h2c);
}
