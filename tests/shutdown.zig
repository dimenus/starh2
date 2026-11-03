//! The shutdown contract (t-3759 step 2): `serve` never returns while a
//! handler still runs, reports what still runs when it asks for the cancel,
//! and returns as soon as the handlers end.
//!
//! The case that cannot end (a handler that ignores cancel for ever) runs in
//! a child process, `tests/shutdown_child.zig`, which `build.zig` runs and
//! checks; so do `deinit` on a live server and an accounting underflow.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");
const serve_deadline = @import("serve_deadline.zig");

const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;
const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

var running: std.atomic.Value(usize) = .init(0);
var ended_ns: std.atomic.Value(u64) = .init(0);

/// Runs until canceled, then returns at once: a cooperative handler.
fn cooperative(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = running.fetchAdd(1, .acq_rel);
    defer {
        _ = running.fetchSub(1, .acq_rel);
        ended_ns.store(nowNs(), .release);
    }
    var body = try resp.start(200, &.{});
    _ = &body;
    while (true) try zio.sleep(.fromMilliseconds(10));
}

/// Ends 300 ms after it is canceled: still running when the cancel report is
/// due, gone well inside the grace.
fn lingering(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = running.fetchAdd(1, .acq_rel);
    defer {
        _ = running.fetchSub(1, .acq_rel);
        ended_ns.store(nowNs(), .release);
    }
    _ = resp;
    while (true) zio.sleep(.fromMilliseconds(10)) catch break;
    zio.beginShield();
    defer zio.endShield();
    zio.sleep(.fromMilliseconds(300)) catch {};
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/coop", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = cooperative } } },
    .{ .method = .GET, .path = "/linger", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = lingering } } },
};

const handlers = 3;
const drain_ms = 300;
const grace_ms = 1000;

const Client = struct {
    port: u16,
    path: []const u8 = "/coop",
    count: usize = handlers,
    /// Answer the server's PINGs: the first GOAWAY phase then ends at once,
    /// not after its 1 s.
    ack_pings: bool = false,
    /// Retry a refused connect for a while: the server is held before it
    /// listens, and only the kernel's accept says when it does.
    connect_retry: bool = false,
    release: std.atomic.Value(bool) = .init(false),
    sent: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,

    fn main(c: *Client) void {
        c.run() catch |err| {
            c.err = err;
            c.sent.store(true, .release);
        };
    }

    fn run(c: *Client) !void {
        const gpa = std.heap.page_allocator;
        const w = if (c.connect_retry) try openRetry(gpa, c.port) else try Wire.open(gpa, .h2c, c.port);
        defer w.close();
        var req = try h2c.buildClientPrefaceAndSettings(gpa);
        defer req.deinit(gpa);
        for (0..c.count) |i| try h2c.appendHeaders(gpa, &req, @intCast(1 + 2 * i), c.path, true);
        try w.send(req.items);
        c.sent.store(true, .release);
        // Read nothing: the connection stays until the server ends it.
        while (!c.release.load(.acquire)) {
            if (c.ack_pings) {
                const s = w.next(nowNs() + 20 * ms) catch |err| switch (err) {
                    error.Deadline, error.WouldBlock => continue,
                    else => return,
                } orelse return;
                if (s.ftype == @intFromEnum(starh2.core.frame.FrameType.ping) and s.flags & 1 == 0) {
                    var ping: [17]u8 = undefined;
                    (starh2.core.frame.FrameHeader{ .length = 8, .type = .ping, .flags = .{ .end_stream = true }, .stream_id = 0 }).encode(ping[0..9]);
                    @memcpy(ping[9..], w.last_payload.items[0..8]);
                    w.send(&ping) catch return;
                }
                continue;
            }
            const t: std.c.timespec = .{ .sec = 0, .nsec = 5 * ms };
            _ = std.c.nanosleep(&t, null);
        }
    }
};

fn runCooperative(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    return runCooperativeAcking(rt, gpa, false);
}

fn runCooperativeAcking(rt: *zio.Runtime, gpa: std.mem.Allocator, ack_pings: bool) !void {
    var limits = starh2.Limits.defaults;
    limits.graceful_drain_timeout_ns = drain_ms * ms;
    limits.shutdown_cancel_grace_ns = grace_ms * ms;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);

    running.store(0, .release);
    ended_ns.store(0, .release);
    var c: Client = .{ .port = server.localAddress(0).getPort(), .ack_pings = ack_pings };
    const t = try std.Thread.spawn(.{}, Client.main, .{&c});
    defer {
        c.release.store(true, .release);
        t.join();
    }
    const start_deadline = nowNs() + 5 * std.time.ns_per_s;
    while (running.load(.acquire) != handlers) {
        if (c.err) |err| return err;
        if (nowNs() >= start_deadline) return error.HandlersNeverStarted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }

    const t0 = nowNs();
    server.requestShutdown();
    // The drain is the first phase (1 s, or until the PING is answered) and
    // `drain_ms`: until then the handlers run and nothing is reported.
    const first_phase_ms: u64 = if (ack_pings) 0 else 1000;
    zio.sleep(.fromMilliseconds(first_phase_ms + drain_ms - 100)) catch {};
    try std.testing.expectEqual(@as(u32, 0), server.shutdown_reports.load(.acquire));
    try std.testing.expectEqual(@as(usize, handlers), running.load(.acquire));
    try std.testing.expect(!serve_handle.hasResult());

    serve_deadline.joinWithin(&server, &serve_handle, 5 * std.time.ns_per_s);
    const returned_ms = (nowNs() - t0) / ms;
    const ended_ms = (ended_ns.load(.acquire) - t0) / ms;
    std.debug.print("shutdown cooperative: {d} reports, the first counted {d} task handlers asked to cancel; handlers ended {d} ms and serve returned {d} ms after shutdown (drain {d} ms + 1 s, grace {d} ms)\n", .{
        server.shutdown_reports.load(.acquire), server.shutdown_report_asked.load(.acquire), ended_ms, returned_ms, drain_ms, grace_ms,
    });
    // One report, at the cancel, counting every handler asked to cancel.
    try std.testing.expectEqual(@as(u32, 1), server.shutdown_reports.load(.acquire));
    try std.testing.expectEqual(@as(usize, handlers), server.shutdown_report_asked.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), running.load(.acquire));
    // `serve` returned after the handlers, and within the grace.
    try std.testing.expect(returned_ms >= ended_ms);
    try std.testing.expect(returned_ms < first_phase_ms + drain_ms + grace_ms);
}

test "shutdown: a cooperative handler ends within the grace, the cancel report counts it, and serve returns" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(runCooperative, .{ rt, gpa });
    try h.join();
}

test "shutdown: with a client that answers the drain PING, the cancel report still comes when the cancel does" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(runCooperativeAcking, .{ rt, gpa, true });
    try h.join();
}

/// A raw HTTP/1.1 client that asks for `/linger` on `port` and reads
/// nothing. The caller closes the descriptor.
fn openRetry(gpa: std.mem.Allocator, port: u16) !*Wire {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (true) {
        return Wire.open(gpa, .h2c, port) catch |err| switch (err) {
            error.Connect => {
                if (nowNs() >= deadline) return err;
                const t: std.c.timespec = .{ .sec = 0, .nsec = 2 * std.time.ns_per_ms };
                _ = std.c.nanosleep(&t, null);
                continue;
            },
            else => return err,
        };
    }
}

/// `h1Linger` on a thread of its own, retrying a refused connect (the
/// server is held before it listens): the fd, or the error.
const H1LingerRetry = struct {
    port: u16,
    fd: c_int = -1,
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),

    fn main(c: *H1LingerRetry) void {
        c.run() catch |err| {
            c.err = err;
        };
        c.done.store(true, .release);
    }

    fn run(c: *H1LingerRetry) !void {
        const fd = try wire_client.connectRetry(c.port, nowNs() + 5 * std.time.ns_per_s);
        errdefer _ = std.c.close(fd);
        const req = "GET /linger HTTP/1.1\r\nHost: localhost\r\n\r\n";
        if (std.c.send(fd, req.ptr, req.len, 0) != req.len) return error.Send;
        c.fd = fd;
    }
};

fn h1Linger(port: u16) !c_int {
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.Socket;
    errdefer _ = std.c.close(fd);
    var addr: std.c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, port), .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
    const req = "GET /linger HTTP/1.1\r\nHost: localhost\r\n\r\n";
    if (std.c.send(fd, req.ptr, req.len, 0) != req.len) return error.Send;
    return fd;
}

fn waitRunning(want: usize) !void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (running.load(.acquire) != want) {
        if (nowNs() >= deadline) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

/// HTTP/1.1 cancels its handler as soon as the server shuts down; the
/// handler takes 300 ms more. The cancel report must come then, not at the
/// end of an HTTP/2 drain (31 s with the defaults).
fn runH1Cancel(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h1c = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }},
        .routes = &routes,
        .tls = null,
        .limits = .defaults,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    running.store(0, .release);
    ended_ns.store(0, .release);
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.Socket;
    defer _ = std.c.close(fd);
    var addr: std.c.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, server.localAddress(0).getPort()), .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
    const req = "GET /linger HTTP/1.1\r\nHost: localhost\r\n\r\n";
    if (std.c.send(fd, req.ptr, req.len, 0) != req.len) return error.Send;
    const start_deadline = nowNs() + 5 * std.time.ns_per_s;
    while (running.load(.acquire) != 1) {
        if (nowNs() >= start_deadline) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    const t0 = nowNs();
    server.requestShutdown();
    serve_deadline.joinWithin(&server, &serve_handle, 5 * std.time.ns_per_s);
    const returned_ms = (nowNs() - t0) / ms;
    std.debug.print("shutdown h1 cancel: {d} reports, the first counted {d} task handlers; serve returned {d} ms after shutdown\n", .{
        server.shutdown_reports.load(.acquire), server.shutdown_report_handlers.load(.acquire), returned_ms,
    });
    try std.testing.expectEqual(@as(u32, 1), server.shutdown_reports.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), server.shutdown_report_handlers.load(.acquire));
    try std.testing.expect(returned_ms < 1500);
}

test "shutdown: an HTTP/1.1 handler cancelled at shutdown is reported then, not at the end of the drain" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(runH1Cancel, .{ rt, gpa });
    try h.join();
}

/// astra 1: a startup failure after an earlier endpoint already took a
/// connection must not return from `serve` under its running handler. The
/// accept loop of endpoint 1 fails to spawn (a test hook: nothing else can
/// make a spawn fail) once the handler on endpoint 0 runs.
fn runStartupFailure(rt: *zio.Runtime, gpa: std.mem.Allocator, h1: bool) !void {
    const server_mod = starh2.edge.server;
    // Endpoint 0's port is the test's own: the server is held before it
    // listens, so its `local_addrs` are never published to this task
    // (`wire_client.freeLoopbackPort`).
    const port = try wire_client.freeLoopbackPort();
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", port);
    const addr1 = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const endpoints: [2]starh2.EndpointConfig = if (h1) .{ .{ .h1c = .{ .address = addr } }, .{ .h1c = .{ .address = addr1 } } } else .{ .{ .h2c_prior_knowledge = .{ .address = addr } }, .{ .h2c_prior_knowledge = .{ .address = addr1 } } };
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &endpoints,
        .routes = &routes,
        .tls = null,
        .limits = .defaults,
    });
    defer server.deinit(gpa);
    server_mod.test_accept_spawn_gate.store(false, .release);
    server_mod.test_fail_accept_spawn.store(1, .release);
    defer server_mod.test_fail_accept_spawn.store(std.math.maxInt(usize), .release);
    defer server_mod.test_accept_spawn_gate.store(true, .release);
    running.store(0, .release);
    ended_ns.store(0, .release);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var c: Client = .{ .port = port, .path = "/linger", .count = 1, .connect_retry = true };
    var t: ?std.Thread = null;
    var h1_fd: c_int = -1;
    defer {
        c.release.store(true, .release);
        if (t) |th| th.join();
        if (h1_fd >= 0) _ = std.c.close(h1_fd);
    }
    if (h1) {
        var lc: H1LingerRetry = .{ .port = port };
        const lt = try std.Thread.spawn(.{}, H1LingerRetry.main, .{&lc});
        while (!lc.done.load(.acquire)) zio.sleep(.fromMilliseconds(2)) catch {};
        lt.join();
        if (lc.err) |err| return err;
        h1_fd = lc.fd;
    } else {
        t = try std.Thread.spawn(.{}, Client.main, .{&c});
    }
    const start_deadline = nowNs() + 5 * std.time.ns_per_s;
    while (running.load(.acquire) != 1) {
        if (c.err) |err| return err;
        if (nowNs() >= start_deadline) return error.HandlerNeverStarted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    server_mod.test_accept_spawn_gate.store(true, .release);
    const ret_deadline = nowNs() + 10 * std.time.ns_per_s;
    while (!serve_handle.hasResult()) {
        if (nowNs() >= ret_deadline) return error.ServeNeverReturned;
        zio.sleep(.fromMilliseconds(1)) catch {};
    }
    const still_running = running.load(.acquire);
    const result = serve_handle.join();
    std.debug.print("shutdown startup failure ({s}): serve returned {s} with {d} handlers still running; {d} reports\n", .{
        if (h1) "h1c" else "h2c", if (result) |_| "ok" else |err| @errorName(err), still_running, server.shutdown_reports.load(.acquire),
    });
    try std.testing.expectError(error.TransportSetupFailed, result);
    try std.testing.expectEqual(@as(usize, 0), still_running);
}

test "shutdown: a startup failure with a handler already running waits for it like any other exit" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(runStartupFailure, .{ rt, gpa, false });
    try h.join();
}

test "shutdown: an HTTP/1.1 startup failure with a handler already running waits for it" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(runStartupFailure, .{ rt, gpa, true });
    try h.join();
}

/// astra 1 / Fable F3: a canceled `serve` cancels every connection task. A
/// running HTTP/1.1 handler is cut off from its connection (orphaned, t-3759
/// step 7), and `serve` waits for it and reports that wait.
const CancelKind = enum { h2c, h1c, h1_tls };

/// An HTTP/1.1-over-TLS client asking for `/linger`, opened on a thread of
/// its own (the TLS handshake blocks, and a blocking call on a zio executor
/// stalls every connection placed there). Reads nothing.
const H1TlsLinger = struct {
    port: u16,
    wire: ?*Wire = null,
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),

    fn main(c: *H1TlsLinger) void {
        c.open() catch |err| {
            c.err = err;
        };
        c.done.store(true, .release);
    }

    fn open(c: *H1TlsLinger) !void {
        const w = try Wire.connect(std.heap.page_allocator, .tls, c.port);
        errdefer w.close();
        w.alpn = "http/1.1";
        try w.handshake();
        try w.sendUnchecked("GET /linger HTTP/1.1\r\nHost: localhost\r\n\r\n");
        c.wire = w;
    }
};

fn runCanceledServe(rt: *zio.Runtime, gpa: std.mem.Allocator, kind: CancelKind) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer if (cert.len != 0) gpa.free(cert);
    defer if (key.len != 0) gpa.free(key);
    if (kind == .h1_tls) {
        cert = try std.Io.Dir.cwd().readFileAlloc(rt.io(), "testdata/cert.pem", gpa, .limited(64 * 1024));
        key = try std.Io.Dir.cwd().readFileAlloc(rt.io(), "testdata/key.pem", gpa, .limited(64 * 1024));
    }
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{switch (kind) {
            .h1c => .{ .h1c = .{ .address = addr } },
            .h2c => .{ .h2c_prior_knowledge = .{ .address = addr } },
            .h1_tls => .{ .tls = .{ .address = addr } },
        }},
        .routes = &routes,
        .tls = if (kind == .h1_tls) .{ .certificate_chain_pem = cert, .private_key_pem = key } else null,
        .limits = .defaults,
    });
    defer server.deinit(gpa);
    running.store(0, .release);
    ended_ns.store(0, .release);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    var c: Client = .{ .port = server.localAddress(0).getPort(), .path = "/linger", .count = 1 };
    var t: ?std.Thread = null;
    var h1_fd: c_int = -1;
    var tls_client: H1TlsLinger = .{ .port = server.localAddress(0).getPort() };
    defer {
        c.release.store(true, .release);
        if (t) |th| th.join();
        if (h1_fd >= 0) _ = std.c.close(h1_fd);
        if (tls_client.wire) |w| w.close();
    }
    switch (kind) {
        .h1c => h1_fd = try h1Linger(server.localAddress(0).getPort()),
        .h2c => t = try std.Thread.spawn(.{}, Client.main, .{&c}),
        .h1_tls => {
            const th = try std.Thread.spawn(.{}, H1TlsLinger.main, .{&tls_client});
            while (!tls_client.done.load(.acquire)) zio.sleep(.fromMilliseconds(2)) catch {};
            th.join();
            if (tls_client.err) |err| return err;
        },
    }
    try waitRunning(1);
    // Cancel from a task of its own: `cancel` waits for `serve` to return.
    const SH = @TypeOf(serve_handle);
    const Canceller = struct {
        fn run(h: *SH, done: *std.atomic.Value(bool)) void {
            h.cancel();
            done.store(true, .release);
        }
    };
    var done: std.atomic.Value(bool) = .init(false);
    const t0 = nowNs();
    var ch = try rt.spawn(Canceller.run, .{ &serve_handle, &done });
    defer ch.join();
    const deadline = nowNs() + 10 * std.time.ns_per_s;
    while (!done.load(.acquire)) {
        if (nowNs() >= deadline) return error.ServeNeverReturned;
        zio.sleep(.fromMilliseconds(1)) catch {};
    }
    const returned_ms = (nowNs() - t0) / ms;
    const ended_ms = (ended_ns.load(.acquire) -| t0) / ms;
    std.debug.print("shutdown canceled serve ({s}): serve returned {d} ms after the cancel, the handler ended at {d} ms; {d} reports, the first counted {d} task handlers\n", .{
        @tagName(kind), returned_ms, ended_ms, server.shutdown_reports.load(.acquire), server.shutdown_report_handlers.load(.acquire),
    });
    try std.testing.expectEqual(@as(usize, 0), running.load(.acquire));
    try std.testing.expect(ended_ns.load(.acquire) != 0);
    try std.testing.expect(returned_ms >= ended_ms);
    // Reported while the handler still ran.
    try std.testing.expect(server.shutdown_reports.load(.acquire) >= 1);
    try std.testing.expectEqual(@as(usize, 1), server.shutdown_report_handlers.load(.acquire));
}

test "shutdown: a canceled serve waits for a running HTTP/1.1 handler and reports it" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(runCanceledServe, .{ rt, gpa, CancelKind.h1c });
    try h.join();
}

test "shutdown: a canceled serve waits for a running HTTP/2 handler and reports it" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(runCanceledServe, .{ rt, gpa, CancelKind.h2c });
    try h.join();
}

test "shutdown: a canceled serve waits for a running HTTP/1.1-over-TLS handler and reports it" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(runCanceledServe, .{ rt, gpa, CancelKind.h1_tls });
    try h.join();
}

test "shutdown: a server with no handler left returns without a report" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(runIdle, .{ rt, gpa });
    try h.join();
}

fn runIdle(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }},
        .routes = &routes,
        .tls = null,
        .limits = .defaults,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const t0 = nowNs();
    server.requestShutdown();
    serve_deadline.joinWithin(&server, &serve_handle, 2 * std.time.ns_per_s);
    const returned_ms = (nowNs() - t0) / ms;
    try std.testing.expectEqual(@as(u32, 0), server.shutdown_reports.load(.acquire));
    // No drain to wait out with nothing open.
    try std.testing.expect(returned_ms < 500);
}
