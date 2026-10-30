//! Step 1 of the transport seam: does a Unix stream socket hold a server
//! stall, where TCP loopback did not on macOS? Measured with no server hook.
//!
//! The server listens on an `h2c_unix` or `tls_unix` endpoint. The client
//! asks for `/big`, whose handler writes for as long as it can, with HTTP/2
//! credit far beyond what the test moves (so a stall is the transport's, not
//! flow control's), and then reads nothing. The probe watches two numbers:
//! the bytes the client's kernel holds unread (FIONREAD) and the bytes the
//! handler got accepted. A stall holds when both stop and then stay where
//! they are for `hold_ns`. During the hold the client asks for `/tick`, and
//! its handler must run: the server goes on reading while its writer is
//! stuck, which is what the stall tests need. `quiet` sends nothing else
//! during the hold; `active` also sends a PING every 50 ms, to show that
//! client writes do not move the stall. Then the client reads again: the
//! bytes must flow again, and the connection must end clean for the
//! protocol oracle.
//!
//! One line per run (`UNIX-STALL ...`) carries the numbers; the transport
//! seam's scenario sizes come from them, per system.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");

const frame = starh2.core.frame;
const Transport = wire_client.Transport;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;

const dummy: u8 = 0;
const settle_ns = 500 * std.time.ns_per_ms;
const hold_ns = 2 * std.time.ns_per_s;
const sample_ns = 20 * std.time.ns_per_ms;
const stall_deadline_ns = 15 * std.time.ns_per_s;

var big_bytes: std.atomic.Value(usize) = .init(0);
var big_done: std.atomic.Value(bool) = .init(false);
var ticks: std.atomic.Value(usize) = .init(0);

fn big(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    defer big_done.store(true, .release);
    var body = try resp.start(200, &.{});
    const chunk: [16 * 1024]u8 = @splat('b');
    while (true) {
        try body.writeAll(&chunk);
        _ = big_bytes.fetchAdd(chunk.len, .acq_rel);
    }
}

fn tick(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = ticks.fetchAdd(1, .acq_rel);
    try resp.send(204, &.{}, "");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = big } } },
    .{ .method = .GET, .path = "/tick", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = tick } } },
};

const Variant = enum { quiet, active };

/// `unix`: the seam. `tcp`: the control arm, TCP loopback with the same
/// client receive buffer, which round 3 measured NOT holding an h2c stall
/// on macOS. It reports and asserts nothing about the stall: it shows the
/// probe can see a stall that does not hold.
const Socket = enum { unix, tcp };

const Client = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    path: []const u8,
    port: u16 = 0,
    socket: Socket = .unix,
    variant: Variant,
    wire: std.atomic.Value(?*Wire) = .init(null),
    send_tick: std.atomic.Value(bool) = .init(false),
    pinging: std.atomic.Value(bool) = .init(false),
    resume_reading: std.atomic.Value(bool) = .init(false),
    /// Reset `/big` once recovery is proven: its handler writes forever, and
    /// a graceful shutdown would wait out its drain timeout (30 s) for it.
    cancel_big: std.atomic.Value(bool) = .init(false),
    /// After reading resumed: DATA bytes read, and whether the server's
    /// /tick response and EOF came.
    read_after: std.atomic.Value(usize) = .init(0),
    tick_answered: bool = false,
    eof: bool = false,
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
};

fn clientMain(c: *Client) void {
    clientRun(c) catch |err| {
        c.err = err;
    };
    c.done.store(true, .release);
}

fn clientRun(c: *Client) !void {
    const w = switch (c.socket) {
        .unix => try Wire.connectUnix(c.gpa, c.transport, c.path, 4096),
        .tcp => try Wire.connectRecvBuf(c.gpa, c.transport, c.port, 4096),
    };
    defer {
        c.wire.store(null, .release);
        w.close();
    }
    try w.handshake();
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(c.gpa);
    try req.appendSlice(c.gpa, frame.CLIENT_PREFACE);
    var settings: [64]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&settings, false, &.{.{ .id = .initial_window_size, .value = 0x7fff_ffff }});
    try req.appendSlice(c.gpa, settings[0..sn]);
    try h2c.appendWindowUpdate(c.gpa, &req, 0, 0x7fff_ffff - 65_535);
    try h2c.appendHeaders(c.gpa, &req, 1, "/big", true);
    try w.send(req.items);
    c.wire.store(w, .release);

    var tick_sent = false;
    var next_ping = nowNs();
    const deadline = nowNs() + 60 * std.time.ns_per_s;
    while (!c.resume_reading.load(.acquire)) {
        if (nowNs() >= deadline) return error.Deadline;
        if (!tick_sent and c.send_tick.load(.acquire)) {
            var t: std.ArrayList(u8) = .empty;
            defer t.deinit(c.gpa);
            try h2c.appendHeaders(c.gpa, &t, 3, "/tick", true);
            try w.send(t.items);
            tick_sent = true;
        }
        if (c.pinging.load(.acquire) and nowNs() >= next_ping) {
            var ping: [17]u8 = undefined;
            _ = try frame.Serializer.ping(&ping, false, "probe-pg");
            try w.send(&ping);
            next_ping = nowNs() + 50 * std.time.ns_per_ms;
        }
        const nap: std.c.timespec = .{ .sec = 0, .nsec = 5 * std.time.ns_per_ms };
        _ = std.c.nanosleep(&nap, null);
    }
    // Read until the server closes.
    var cancelled = false;
    while (true) {
        if (nowNs() >= deadline) return error.Deadline;
        if (!cancelled and c.cancel_big.load(.acquire)) {
            var rst: std.ArrayList(u8) = .empty;
            defer rst.deinit(c.gpa);
            try h2c.appendRst(c.gpa, &rst, 1);
            try w.send(rst.items);
            cancelled = true;
        }
        const s = w.next(nowNs() + 25 * std.time.ns_per_ms) catch |err| switch (err) {
            error.Deadline => continue,
            else => return err,
        } orelse {
            c.eof = true;
            return;
        };
        if (s.ftype == 0 and s.sid == 1) _ = c.read_after.fetchAdd(w.last_payload.items.len, .acq_rel);
        if (s.ftype == 1 and s.sid == 3) c.tick_answered = true;
    }
}

const Sample = struct { unread: usize, big: usize };

fn sample(c: *Client) ?Sample {
    const w = c.wire.load(.acquire) orelse return null;
    return .{ .unread = w.unreadBytes(), .big = big_bytes.load(.acquire) };
}

/// Wait until both numbers stop moving for `settle_ns`.
fn waitPlateau(c: *Client) !Sample {
    const deadline = nowNs() + stall_deadline_ns;
    var last = sample(c) orelse return error.NoClient;
    var since = nowNs();
    while (nowNs() < deadline) {
        try zio.sleep(.fromNanoseconds(sample_ns));
        const s = sample(c) orelse return error.NoClient;
        if (s.unread != last.unread or s.big != last.big) {
            last = s;
            since = nowNs();
        } else if (s.big > 0 and nowNs() - since >= settle_ns) {
            return s;
        }
    }
    std.debug.print("UNIX-STALL never settled: unread={d} big={d}\n", .{ last.unread, last.big });
    return error.NeverStalled;
}

fn runProbe(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, variant: Variant, socket: Socket) !void {
    big_bytes.store(0, .release);
    big_done.store(false, .release);
    ticks.store(0, .release);
    const io = rt.io();
    var path_buf: [64]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/starh2-probe-{d}-{s}-{s}.sock", .{ std.c.getpid(), @tagName(transport), @tagName(variant) });
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var tls_cfg: ?starh2.TlsConfig = null;
    if (transport == .tls) {
        cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
        key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
        tls_cfg = .{ .certificate_chain_pem = cert, .private_key_pem = key };
    }
    const loopback = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const endpoint: starh2.EndpointConfig = switch (socket) {
        .unix => switch (transport) {
            .h2c => .{ .h2c_unix = path },
            .tls => .{ .tls_unix = path },
        },
        .tcp => switch (transport) {
            .h2c => .{ .h2c_prior_knowledge = loopback },
            .tls => .{ .tls = loopback },
        },
    };
    var server = try starh2.Server.init(gpa, io, .{ .endpoints = &.{endpoint}, .routes = &routes, .tls = tls_cfg, .limits = starh2.Limits.defaults });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var c: Client = .{ .gpa = gpa, .transport = transport, .path = path, .port = server.localAddress(0).getPort(), .socket = socket, .variant = variant };
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});
    var joined = false;
    defer if (!joined) {
        c.resume_reading.store(true, .release);
        server.requestShutdown();
        while (!c.done.load(.acquire)) zio.sleep(.fromMilliseconds(5)) catch {};
        t.join();
    };
    while (c.wire.load(.acquire) == null and !c.done.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    if (c.err) |e| return e;

    const plateau = waitPlateau(&c) catch |err| {
        // The control arm may never settle: that is a stall that does not hold.
        if (socket == .tcp and err == error.NeverStalled) {
            std.debug.print("UNIX-STALL os={s} socket=tcp transport={s} variant={s} never_settled=true\n", .{ @tagName(@import("builtin").os.tag), @tagName(transport), @tagName(variant) });
            return;
        }
        return err;
    };
    // The hold: nothing may move, whether or not the client writes.
    if (variant == .active) c.pinging.store(true, .release);
    c.send_tick.store(true, .release);
    var moved: usize = 0;
    var worst: Sample = plateau;
    const hold_end = nowNs() + hold_ns;
    while (nowNs() < hold_end) {
        try zio.sleep(.fromNanoseconds(sample_ns));
        const s = sample(&c) orelse return error.NoClient;
        if (s.unread != plateau.unread or s.big != plateau.big) {
            moved += 1;
            worst = s;
        }
    }
    c.pinging.store(false, .release);
    const ticked = ticks.load(.acquire);

    // Recovery: read again, then end the connection.
    c.resume_reading.store(true, .release);
    const recover_deadline = nowNs() + 10 * std.time.ns_per_s;
    while (c.read_after.load(.acquire) < plateau.big and nowNs() < recover_deadline) try zio.sleep(.fromMilliseconds(5));
    const recovered = c.read_after.load(.acquire);
    c.cancel_big.store(true, .release);
    const end_deadline = nowNs() + 5 * std.time.ns_per_s;
    while (!big_done.load(.acquire) and nowNs() < end_deadline) try zio.sleep(.fromMilliseconds(2));
    server.requestShutdown();
    while (!c.done.load(.acquire)) try zio.sleep(.fromMilliseconds(5));
    t.join();
    joined = true;
    serve_handle.join() catch {};
    serving = false;

    std.debug.print("UNIX-STALL os={s} socket={s} transport={s} variant={s} plateau_unread={d} plateau_big={d} moved_samples={d} worst_unread={d} worst_big={d} ticked={d} recovered_bytes={d} tick_answered={} eof={} client_err={s}\n", .{
        @tagName(@import("builtin").os.tag), @tagName(socket), @tagName(transport), @tagName(variant), plateau.unread, plateau.big, moved,
        worst.unread,                         worst.big,           ticked,            recovered,      c.tick_answered, c.eof,
        if (c.err) |e| @errorName(e) else "-",
    });
    if (c.err) |e| return e;
    if (socket == .tcp) return;
    // The stall held: neither number moved during the hold.
    try std.testing.expectEqual(@as(usize, 0), moved);
    // The server read the client's request during the stall.
    try std.testing.expectEqual(@as(usize, 1), ticked);
    // It recovered: at least as many bytes as were stuck came through.
    try std.testing.expect(recovered >= plateau.big);
    try std.testing.expect(c.tick_answered);
    try std.testing.expect(c.eof);
}

fn newRuntime(gpa: std.mem.Allocator) !*zio.Runtime {
    return zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
}

fn runTop(transport: Transport, variant: Variant) !void {
    return runSocketTop(transport, variant, .unix);
}

fn runSocketTop(transport: Transport, variant: Variant, socket: Socket) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runProbe, .{ rt, gpa, transport, variant, socket });
    try h.join();
}

test "control arm, tcp loopback, h2c, quiet: reports whether the same stall holds over TCP (asserts nothing)" {
    try runSocketTop(.h2c, .quiet, .tcp);
}

test "unix stall, h2c, quiet: the writer stops and stays stopped, the server still reads, and it recovers" {
    try runTop(.h2c, .quiet);
}

test "unix stall, h2c, active: client PINGs during the hold do not move the stall" {
    try runTop(.h2c, .active);
}

test "unix stall, tls, quiet: the writer stops and stays stopped, the server still reads, and it recovers" {
    try runTop(.tls, .quiet);
}

test "unix stall, tls, active: client PINGs during the hold do not move the stall" {
    try runTop(.tls, .active);
}

test "zz h2 oracle: no protocol violation on any connection in this binary" {
    try @import("h2_oracle.zig").expectNoViolations();
}
