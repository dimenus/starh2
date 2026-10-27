//! t-3562: terminal control frames (RST_STREAM, GOAWAY) have one derived bound,
//! T = `max_streams_per_connection` + 3 (`Limits.terminalFrames`), and a
//! connection past it ends with GOAWAY(ENHANCE_YOUR_CALM), not a failed writer.
//!
//! Every scenario stalls the writer so the scheduler's terminal loop is paused
//! (`pause_control`) and terminal frames wait in the terminal ring. The oracles
//! do not depend on timing:
//!
//! - `test_rst_queued_paused` proves the RSTs were queued while the control
//!   pause was on, so the stall really held them;
//! - `test_ctrl_terminal_full`, the counter of a terminal frame that could not
//!   be queued, does not move;
//! - `test_terminal_ring_high`, the most frames the ring held, is at most T;
//! - what the client reads after the stall: the RSTs for exactly the stream
//!   ids the scenario expects, one each, the GOAWAY(s), then EOF.
//!
//! `server_failures`: M streams start their responses, the writer is held
//! (`wire_pump.test_hold_writes` on h2c, `test_hold_stash` on TLS) and the
//! handlers' DATA fills the write memory, every handler aborts (M server
//! RSTs), and a graceful shutdown queues its GOAWAYs. Before t-3562 the 33rd
//! abort overflowed the 32-entry ring. The holds park the TLS actor, which is
//! fine here: these failures come from handlers, not from reads.
//!
//! The other scenarios need the actor to READ during the stall, so they use
//! the stall a real peer makes: the client stops reading (with a small receive
//! buffer), and the handlers' DATA backs up into the socket and then the write
//! memory, on both transports, while the actor goes on reading.
//!
//! `flood`: the M streams stay open, and the client sends more than M HEADERS
//! for new stream ids in one write. Each is refused with an RST. Past T the
//! connection must queue its final GOAWAY (ENHANCE_YOUR_CALM), keep the ring at
//! T, and close by its closing deadline once the client reads again.
//!
//! Run it with `./zb build test-t3562` (it is also part of `./zb build test`).
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const wire_client = @import("wire_client.zig");

const conn_mod = starh2.edge.connection;
const wire_pump = starh2.edge.wire_pump;
const frame = starh2.core.frame;

const Transport = wire_client.Transport;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;
const client_deadline_ns = wire_client.client_deadline_ns;

const dummy: u8 = 0;

/// Ordinary control entries above M that the setup needs (see `limitsFor`).
const ordinary_slack = 16;
/// The client's receive buffer when it stalls the server by not reading.
const stall_rcvbuf: c_int = 4096;
/// DATA each handler writes when the data gate opens. With the writer held,
/// 2 KiB per stream fills the write memory. With a client that does not read,
/// it must also fill the two socket buffers, so it is larger there, and still
/// far below the connection's outbound budget, so no handler waits in
/// `writeAll` (a handler that waits there would never see the abort gate).
const held_data_bytes = 2 * 1024;
const stalled_data_bytes = 16 * 1024;

var started: std.atomic.Value(usize) = .init(0);
var wrote: std.atomic.Value(usize) = .init(0);
var data_gate: std.atomic.Value(bool) = .init(false);
var data_bytes: std.atomic.Value(usize) = .init(held_data_bytes);
/// TLS only: half the handlers flush with nothing pending instead of writing.
/// A socket the client does not read stops DATA (`pending_n`), but on TLS the
/// control pause is a full stash, and DATA that is not popped takes no stash
/// slot. The bare flush barriers fill the stash (as in t3567_tls_barriers.zig),
/// and the barrier of a handler that waits for room is not taken.
var bare_flush: std.atomic.Value(bool) = .init(false);
var abort_gate: std.atomic.Value(bool) = .init(false);
var aborted: std.atomic.Value(usize) = .init(0);
/// Handlers whose `resp.start` returned an error, and the first such error.
var start_failed: std.atomic.Value(usize) = .init(0);
var first_start_err: std.atomic.Value(u16) = .init(0);
/// Handlers that ran at all.
var entered: std.atomic.Value(usize) = .init(0);

fn resetGlobals() void {
    started.store(0, .release);
    wrote.store(0, .release);
    data_gate.store(false, .release);
    data_bytes.store(held_data_bytes, .release);
    bare_flush.store(false, .release);
    abort_gate.store(false, .release);
    aborted.store(0, .release);
    start_failed.store(0, .release);
    first_start_err.store(0, .release);
    entered.store(0, .release);
}

/// Starts its response (the HEADERS are written before the stall), writes
/// `data_bytes` when the data gate opens (that fills the write memory, which
/// pauses the terminal loop), and aborts when the abort gate opens.
fn stall(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const index = entered.fetchAdd(1, .acq_rel);
    var body = resp.start(200, &.{}) catch |err| {
        _ = start_failed.fetchAdd(1, .acq_rel);
        _ = first_start_err.cmpxchgStrong(0, @intFromError(err), .acq_rel, .acquire);
        return err;
    };
    _ = started.fetchAdd(1, .acq_rel);
    while (!data_gate.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    if (bare_flush.load(.acquire) and index % 2 == 0) {
        // Waits for its barrier to be written, which the stall never does: it
        // returns with an error when the connection ends.
        try body.flush();
    } else {
        const chunk: [stalled_data_bytes]u8 = @splat('x');
        try body.writeAll(chunk[0..data_bytes.load(.acquire)]);
        _ = wrote.fetchAdd(1, .acq_rel);
    }
    // A connection that ends (the flood) tells the handler through its
    // terminal cause, the way `hangSse` in goaway_delivery.zig learns it.
    while (!abort_gate.load(.acquire)) {
        if (body.terminalCause() != null) return error.Canceled;
        try zio.sleep(.fromMilliseconds(2));
    }
    try body.abort();
    _ = aborted.fetchAdd(1, .acq_rel);
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/stall", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = stall } } },
};

fn newRuntime(gpa: std.mem.Allocator) !*zio.Runtime {
    return zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
}

fn serverFor(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, limits: starh2.Limits, cert: *[]u8, key: *[]u8) !starh2.Server {
    const io = rt.io();
    var endpoint: starh2.EndpointConfig = undefined;
    var tls_cfg: ?starh2.TlsConfig = null;
    switch (transport) {
        .h2c => endpoint = .{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) },
        .tls => {
            cert.* = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            key.* = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            endpoint = .{ .tls = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) };
            tls_cfg = .{ .certificate_chain_pem = cert.*, .private_key_pem = key.* };
        },
    }
    return starh2.Server.init(gpa, io, .{
        .endpoints = &.{endpoint},
        .routes = &routes,
        .tls = tls_cfg,
        .limits = limits,
    });
}

// ---------------------------------------------------------------------------
// The client: one connection on its own thread.

const max_sid = 4096;

const Client = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    /// Streams opened at the start, and extra HEADERS (new ids past the
    /// advertised limit) sent when `send_flood` is set.
    streams: usize,
    flood: usize,
    /// Null: read from the start. Otherwise connect with this receive buffer
    /// and read nothing until `read_gate` is set, which stalls the server.
    rcvbuf: ?c_int,
    read_gate: std.atomic.Value(bool) = .init(false),
    send_flood: std.atomic.Value(bool) = .init(false),
    flood_sent: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    rst_per_sid: [max_sid]u8 = @splat(0),
    rst_total: usize = 0,
    goaways: usize = 0,
    goaway_code: u32 = 0,
    /// A GOAWAY(ENHANCE_YOUR_CALM) was read, whichever GOAWAY came last.
    enhance_seen: bool = false,
    /// The server closed the socket (EOF), which is how every scenario ends,
    /// and when the client saw it.
    eof: bool = false,
    eof_ns: u64 = 0,
    /// Frames of each type the client read.
    frames: [16]usize = @splat(0),
};

fn clientMain(c: *Client) void {
    clientRun(c) catch |err| {
        c.err = err;
    };
    c.done.store(true, .release);
}

fn clientRun(c: *Client) !void {
    const w = try Wire.connectRecvBuf(c.gpa, c.transport, c.port, c.rcvbuf);
    defer w.close();
    try w.handshake();
    var req = try h2c.buildClientPrefaceAndSettings(c.gpa);
    defer req.deinit(c.gpa);
    try h2c.appendWindowUpdate(c.gpa, &req, 0, 1 << 30);
    for (0..c.streams) |i| try h2c.appendHeaders(c.gpa, &req, @intCast(1 + 2 * i), "/stall", true);
    try w.send(req.items);

    const deadline = nowNs() + 40 * std.time.ns_per_s;
    var reading = c.rcvbuf == null;
    while (true) {
        if (!c.flood_sent.load(.acquire) and c.send_flood.load(.acquire)) {
            // One write, so the server ingests the whole flood from one read.
            var more: std.ArrayList(u8) = .empty;
            defer more.deinit(c.gpa);
            for (0..c.flood) |i| try h2c.appendHeaders(c.gpa, &more, @intCast(1 + 2 * (c.streams + i)), "/stall", true);
            try w.send(more.items);
            c.flood_sent.store(true, .release);
        }
        if (!reading) {
            if (c.read_gate.load(.acquire)) {
                reading = true;
            } else {
                if (nowNs() >= deadline) return error.Deadline;
                const nap: std.c.timespec = .{ .sec = 0, .nsec = 2 * std.time.ns_per_ms };
                _ = std.c.nanosleep(&nap, null);
                continue;
            }
        }
        // The socket's receive timeout is one second, so a read can block that
        // long and a flag set meanwhile waits for it. A test that needs a flag
        // seen at once uses a client that is not reading yet.
        const s = w.next(nowNs() + 25 * std.time.ns_per_ms) catch |err| switch (err) {
            error.Deadline => {
                if (nowNs() >= deadline) return error.Deadline;
                continue;
            },
            else => return err,
        } orelse {
            c.eof = true;
            c.eof_ns = nowNs();
            return;
        };
        if (s.ftype < c.frames.len) c.frames[s.ftype] += 1;
        switch (s.ftype) {
            3 => {
                c.rst_total += 1;
                if (s.sid < max_sid) c.rst_per_sid[s.sid] +|= 1;
            },
            7 => {
                c.goaways += 1;
                const p = w.last_payload.items;
                if (p.len >= 8) c.goaway_code = std.mem.readInt(u32, p[4..8], .big);
                if (c.goaway_code == @backingInt(frame.ErrorCode.enhance_your_calm)) c.enhance_seen = true;
            },
            6 => if (s.flags & 1 == 0) {
                // Acking the graceful PING moves the server to phase 2.
                var ack: [17]u8 = undefined;
                (frame.FrameHeader{ .length = 8, .type = .ping, .flags = .{ .end_stream = true }, .stream_id = 0 }).encode(ack[0..9]);
                @memcpy(ack[9..], w.last_payload.items[0..8]);
                try w.send(&ack);
            },
            else => {},
        }
    }
}

fn waitFor(comptime what: []const u8, ctx: anytype, comptime pred: fn (@TypeOf(ctx)) bool) !void {
    const deadline = nowNs() + client_deadline_ns;
    while (!pred(ctx)) {
        if (nowNs() >= deadline) {
            std.debug.print("t3562: timed out waiting for {s}\n", .{what});
            return error.Deadline;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

const Count = struct { c: *const std.atomic.Value(usize), n: usize };

fn atLeast(counter: *const std.atomic.Value(usize), n: usize) Count {
    return .{ .c = counter, .n = n };
}

fn reached(x: Count) bool {
    return x.c.load(.acquire) >= x.n;
}

/// Yield until the client thread is done, then join it. A blocking join on
/// the zio task could stop the executor that runs the server.
fn joinClient(c: *Client, t: std.Thread) void {
    const deadline = nowNs() + 45 * std.time.ns_per_s;
    while (!c.done.load(.acquire) and nowNs() < deadline) zio.sleep(.fromMilliseconds(5)) catch {};
    t.join();
}

fn hold(transport: Transport, on: bool) void {
    switch (transport) {
        .h2c => wire_pump.test_hold_writes.store(on, .release),
        .tls => conn_mod.test_hold_stash.store(on, .release),
    }
}

fn holdEntered(transport: Transport) u64 {
    return switch (transport) {
        .h2c => wire_pump.test_hold_entered.load(.acquire),
        .tls => conn_mod.test_hold_stash_entered.load(.acquire),
    };
}

const Scenario = enum { server_failures, flood };

fn limitsFor() starh2.Limits {
    var limits = starh2.Limits.defaults;
    // The flood's final GOAWAY is written after the stall; the closing
    // deadline must outlast the test's own waits.
    limits.close_flush_timeout_ns = 6 * std.time.ns_per_s;
    // The ordinary share holds a frame until its write completes, so M HEADERS
    // in flight plus the server preface, the SETTINGS ack and a WINDOW_UPDATE
    // fill a share of exactly M: with `control_entries_per_connection` = M, one
    // run in about ten got `PoolFull` on a HEADERS and that handler's
    // `resp.start` failed with `WriteFailed` (T3562-setup-timeout line,
    // `ordinary_pool_full=1`). That is a limit of the ordinary share, older
    // than t-3562 (it was 240 at c30c99b), and not what this gate tests, so the
    // share gets slack here. The final assert checks that the slack was enough.
    limits.control_entries_per_connection = limits.max_streams_per_connection + ordinary_slack;
    return limits;
}

/// Wait until every handler has started its response, or say why not.
fn waitStarted(c: *const Client, m: usize) !void {
    waitFor("every handler to start its response", atLeast(&started, m), reached) catch |err| {
        const code = first_start_err.load(.acquire);
        std.debug.print("T3562-setup-timeout m={d} entered={d} started={d} start_failed={d} first_start_err={s} client_done={} client_err={s} ordinary_pool_full={d} ordinary_queue_full={d}\n", .{
            m,
            entered.load(.acquire),
            started.load(.acquire),
            start_failed.load(.acquire),
            if (code == 0) "-" else @errorName(@errorFromInt(code)),
            c.done.load(.acquire),
            if (c.err) |e| @errorName(e) else "-",
            conn_mod.test_ctrl_ordinary_pool_full.load(.monotonic),
            conn_mod.test_ctrl_ordinary_queue_full.load(.monotonic),
        });
        return err;
    };
}

/// RSTs the client read for each of the `n` odd stream ids from `first`.
fn expectRsts(c: *const Client, first: usize, n: usize, want: u8) !void {
    for (0..n) |i| {
        const sid = first + 2 * i;
        if (c.rst_per_sid[sid] != want) {
            std.debug.print("t3562: stream {d} got {d} RST_STREAM, want {d}\n", .{ sid, c.rst_per_sid[sid], want });
            return error.TestUnexpectedResult;
        }
    }
}

fn ringAtLeast(n: usize) bool {
    return conn_mod.test_terminal_ring_high.load(.acquire) >= n;
}

fn runCase(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, scenario: Scenario) !void {
    resetGlobals();
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    const limits = limitsFor();
    const m = limits.max_streams_per_connection;
    const t_frames = try limits.terminalFrames();
    try std.testing.expectEqual(m + 3, t_frames);

    var server = try serverFor(rt, gpa, transport, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    defer hold(transport, false);
    defer data_gate.store(true, .release);
    defer abort_gate.store(true, .release);

    const full_0 = conn_mod.test_ctrl_terminal_full.load(.monotonic);
    const ordinary_full_0 = conn_mod.test_ctrl_ordinary_pool_full.load(.monotonic);
    const rst_paused_0 = conn_mod.test_rst_queued_paused.load(.monotonic);
    conn_mod.test_terminal_ring_high.store(0, .release);

    const stalls_by_reading = scenario != .server_failures;
    var c: Client = .{
        .gpa = gpa,
        .transport = transport,
        .port = server.localAddress(0).getPort(),
        .streams = m,
        .flood = if (scenario == .flood) m + 40 else 0,
        .rcvbuf = if (stalls_by_reading) stall_rcvbuf else null,
    };
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});
    var joined = false;
    defer if (!joined) {
        hold(transport, false);
        c.read_gate.store(true, .release);
        data_gate.store(true, .release);
        abort_gate.store(true, .release);
        joinClient(&c, t);
    };

    // Every response has its HEADERS out before the stall.
    try waitStarted(&c, m);

    if (stalls_by_reading) {
        // The client reads nothing: the DATA fills the socket buffers, then
        // the write memory, and the control pause turns on.
        data_bytes.store(stalled_data_bytes, .release);
        if (transport == .tls) bare_flush.store(true, .release);
        const pauses_0 = conn_mod.test_control_pause_seen.load(.monotonic);
        data_gate.store(true, .release);
        const writers = if (transport == .tls) m / 2 else m;
        try waitFor("every writer to write its DATA", atLeast(&wrote, writers), reached);
        try waitFor("the control pause", pauses_0, struct {
            fn f(p0: u64) bool {
                return conn_mod.test_control_pause_seen.load(.monotonic) > p0;
            }
        }.f);
    } else {
        // Hold the writer, then let the handlers write: the DATA fills the
        // write memory, which pauses the terminal loop.
        const entered_0 = holdEntered(transport);
        hold(transport, true);
        data_gate.store(true, .release);
        const Entered = struct { transport: Transport, at: u64 };
        try waitFor("the writer to enter the hold", Entered{ .transport = transport, .at = entered_0 }, struct {
            fn f(x: Entered) bool {
                return holdEntered(x.transport) != x.at;
            }
        }.f);
        try waitFor("every handler to write its DATA", atLeast(&wrote, m), reached);
    }

    var bound_ns: u64 = 0;
    switch (scenario) {
        .server_failures => {
            // M server RSTs, then the graceful GOAWAY.
            abort_gate.store(true, .release);
            waitFor("every handler to abort", atLeast(&aborted, m), reached) catch |err| {
                std.debug.print("T3562-abort-timeout aborted={d} of {d} terminal_full={d} ring_high={d}\n", .{
                    aborted.load(.acquire),                                  m,
                    conn_mod.test_ctrl_terminal_full.load(.monotonic) - full_0, conn_mod.test_terminal_ring_high.load(.acquire),
                });
                return err;
            };
            server.requestShutdown();
            // On h2c phase 1's GOAWAY is queued in the same stall. On TLS the
            // stash hold parks the actor, which is what starts the graceful
            // shutdown, so the GOAWAY is queued when the hold ends.
            if (transport == .h2c) try waitFor("the graceful GOAWAY in the ring", m + 1, ringAtLeast);
        },
        .flood => {
            c.send_flood.store(true, .release);
            try waitFor("the client to send the flood", &c, struct {
                fn f(x: *Client) bool {
                    return x.flood_sent.load(.acquire);
                }
            }.f);
            // T - 1 refusals, then the final GOAWAY in the slot kept for it.
            waitFor("the terminal ring to reach T", t_frames, ringAtLeast) catch |err| {
                std.debug.print("T3562-flood-timeout ring_high={d} rst_paused={d} terminal_full={d}\n", .{
                    conn_mod.test_terminal_ring_high.load(.acquire),
                    conn_mod.test_rst_queued_paused.load(.monotonic) - rst_paused_0,
                    conn_mod.test_ctrl_terminal_full.load(.monotonic) - full_0,
                });
                return err;
            };
            bound_ns = nowNs();
        },
    }
    const rst_paused = conn_mod.test_rst_queued_paused.load(.monotonic) - rst_paused_0;
    const high_held = conn_mod.test_terminal_ring_high.load(.acquire);
    std.debug.print("T3562-held transport={s} scenario={s} m={d} t={d} ring_high={d} rst_paused={d} terminal_full={d}\n", .{ @tagName(transport), @tagName(scenario), m, t_frames, high_held, rst_paused, conn_mod.test_ctrl_terminal_full.load(.monotonic) - full_0 });

    hold(transport, false);
    c.read_gate.store(true, .release);
    joinClient(&c, t);
    joined = true;

    const full = conn_mod.test_ctrl_terminal_full.load(.monotonic) - full_0;
    std.debug.print("T3562 transport={s} scenario={s} m={d} t={d} ring_high={d} terminal_full={d} rst_paused={d} rst_total={d} goaways={d} goaway_code={d} eof={} close_ms={d} client_err={s} data_frames={d} headers_frames={d}\n", .{
        @tagName(transport),                             @tagName(scenario), m,          t_frames,
        conn_mod.test_terminal_ring_high.load(.acquire), full,               rst_paused, c.rst_total,
        c.goaways,                                       c.goaway_code,      c.eof,      if (bound_ns != 0 and c.eof_ns > bound_ns) (c.eof_ns - bound_ns) / std.time.ns_per_ms else 0,
        if (c.err) |e| @errorName(e) else "-",          c.frames[0],        c.frames[1],
    });

    if (c.err) |e| return e;
    // The slack was enough: no ordinary frame was refused by the pool.
    try std.testing.expectEqual(ordinary_full_0, conn_mod.test_ctrl_ordinary_pool_full.load(.monotonic));
    // The failure is not a terminal frame that could not be queued.
    try std.testing.expectEqual(@as(usize, 0), full);
    try std.testing.expect(conn_mod.test_terminal_ring_high.load(.acquire) <= t_frames);
    try std.testing.expect(c.eof);
    switch (scenario) {
        .server_failures => {
            // Every abort's RST waited in the ring. On TLS the stash takes a
            // few terminal frames before it is full, and those were queued
            // before the pause.
            try std.testing.expect(rst_paused + 16 >= m);
            try std.testing.expect(high_held + 16 >= m);
            try expectRsts(&c, 1, m, 1);
            try std.testing.expectEqual(m, c.rst_total);
            try std.testing.expect(c.goaways >= 2);
            try std.testing.expect(!c.enhance_seen);
            try std.testing.expectEqual(@as(u32, @backingInt(frame.ErrorCode.no_error)), c.goaway_code);
        },
        .flood => {
            // The ring filled to the bound (T - 1 RSTs, then the GOAWAY) and
            // no further, all of it during the stall.
            try std.testing.expectEqual(t_frames, high_held);
            try std.testing.expectEqual(t_frames - 1, rst_paused);
            // The open streams get no RST (the GOAWAY covers them); the first
            // T - 1 flood streams get one each, the rest none.
            try expectRsts(&c, 1, m, 0);
            try expectRsts(&c, 1 + 2 * m, t_frames - 1, 1);
            try expectRsts(&c, 1 + 2 * (m + t_frames - 1), c.flood - (t_frames - 1), 0);
            try std.testing.expectEqual(t_frames - 1, c.rst_total);
            try std.testing.expectEqual(@as(u32, @backingInt(frame.ErrorCode.enhance_your_calm)), c.goaway_code);
            try std.testing.expectEqual(@as(usize, 1), c.goaways);
            // The connection closed by its closing deadline, which began when
            // the GOAWAY was queued, at or before `bound_ns`.
            try std.testing.expect(c.eof_ns - bound_ns <= limits.close_flush_timeout_ns);
        },
    }

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runCaseTop(transport: Transport, scenario: Scenario) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runCase, .{ rt, gpa, transport, scenario });
    try h.join();
}

test "server failures in one stall, h2c: M aborts and the graceful GOAWAY all fit, one RST per stream" {
    try runCaseTop(.h2c, .server_failures);
}

test "server failures in one stall, tls: M aborts and the graceful GOAWAY all fit, one RST per stream" {
    try runCaseTop(.tls, .server_failures);
}

test "flood during a stall, h2c: past T the connection ends with GOAWAY(ENHANCE_YOUR_CALM)" {
    try runCaseTop(.h2c, .flood);
}

test "flood during a stall, tls: past T the connection ends with GOAWAY(ENHANCE_YOUR_CALM)" {
    try runCaseTop(.tls, .flood);
}
