//! t-3562: terminal control frames (RST_STREAM, GOAWAY) have one derived bound,
//! T = `max_streams_per_connection` + 3 (`Limits.terminalFrames`), and a
//! connection past it ends with GOAWAY(ENHANCE_YOUR_CALM), not a failed writer.
//!
//! Both scenarios hold the writer AND fill the write memory with DATA, so the
//! scheduler's terminal loop is paused (`pause_control`) and every terminal
//! frame waits in the terminal ring, on h2c (`wire_pump.test_hold_writes`) and
//! on TLS (`test_hold_stash`). The oracles do not depend on timing:
//!
//! - `test_ctrl_terminal_full`, the counter of a terminal frame that could not
//!   be queued, does not move;
//! - `test_terminal_ring_high`, the most frames the ring held, is at least the
//!   number of frames the scenario means to queue (so the stall really queued
//!   them) and at most T;
//! - what the client reads after the hold is released: one RST per stream,
//!   the GOAWAY, and then EOF.
//!
//! `server_failures`: M streams start their responses, the writer is held,
//! every handler aborts (M server RSTs), and a graceful shutdown queues its
//! GOAWAY. Before t-3562 the 33rd abort overflowed the 32-entry ring.
//!
//! `flood`: the same M streams stay open, and the client sends more than M
//! HEADERS for new stream ids in one write. Each is refused with an RST the
//! peer drives. Past T the connection must queue its final GOAWAY
//! (ENHANCE_YOUR_CALM), keep the ring at T or below, and close by its closing
//! deadline, which is set longer than the hold so the client can read the
//! GOAWAY after the release.
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

var started: std.atomic.Value(usize) = .init(0);
var data_gate: std.atomic.Value(bool) = .init(false);
var abort_gate: std.atomic.Value(bool) = .init(false);
var aborted: std.atomic.Value(usize) = .init(0);

fn resetGlobals() void {
    started.store(0, .release);
    data_gate.store(false, .release);
    abort_gate.store(false, .release);
    aborted.store(0, .release);
}

/// Starts its response (the HEADERS are written before the hold), writes a
/// few KiB when the data gate opens (that fills the write memory, which
/// pauses the terminal loop), and aborts when the abort gate opens.
fn stall(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    _ = started.fetchAdd(1, .acq_rel);
    while (!data_gate.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    const chunk: [2048]u8 = @splat('x');
    try body.writeAll(&chunk);
    // A connection that ends (the flood scenarios) tells the handler through
    // its terminal cause, the way `hangSse` in goaway_delivery.zig learns it.
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
    send_flood: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    rst_per_sid: [max_sid]u8 = @splat(0),
    rst_total: usize = 0,
    goaways: usize = 0,
    goaway_code: u32 = 0,
    /// The server closed the socket (EOF), which is how every scenario ends.
    eof: bool = false,
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
    const w = try Wire.open(c.gpa, c.transport, c.port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(c.gpa);
    defer req.deinit(c.gpa);
    try h2c.appendWindowUpdate(c.gpa, &req, 0, 1 << 30);
    for (0..c.streams) |i| try h2c.appendHeaders(c.gpa, &req, @intCast(1 + 2 * i), "/stall", true);
    try w.send(req.items);

    var flood_sent = false;
    const deadline = nowNs() + 40 * std.time.ns_per_s;
    while (true) {
        if (!flood_sent and c.send_flood.load(.acquire)) {
            // One write, so the server ingests the whole flood from one read.
            var more: std.ArrayList(u8) = .empty;
            defer more.deinit(c.gpa);
            for (0..c.flood) |i| try h2c.appendHeaders(c.gpa, &more, @intCast(1 + 2 * (c.streams + i)), "/stall", true);
            try w.send(more.items);
            flood_sent = true;
        }
        const s = w.next(nowNs() + 25 * std.time.ns_per_ms) catch |err| switch (err) {
            error.Deadline => {
                if (nowNs() >= deadline) return error.Deadline;
                continue;
            },
            else => return err,
        } orelse {
            c.eof = true;
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

/// `flood` meets the terminal bound T. `flood_intents` keeps the default
/// intent queue, whose room runs out first (an RST takes two intents), and must
/// end the same way.
const Scenario = enum { server_failures, flood, flood_intents };

fn runCase(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, scenario: Scenario) !void {
    resetGlobals();
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var limits = starh2.Limits.defaults;
    // The flood's final GOAWAY is written after the release; the closing
    // deadline must be longer than the hold.
    limits.close_flush_timeout_ns = 6 * std.time.ns_per_s;
    // Room for 2 intents per RST, so that T is the limit that fires.
    if (scenario != .flood_intents) limits.intent_entries_per_connection = 2048;
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
    conn_mod.test_terminal_ring_high.store(0, .release);

    var c: Client = .{
        .gpa = gpa,
        .transport = transport,
        .port = server.localAddress(0).getPort(),
        .streams = m,
        .flood = if (scenario == .server_failures) 0 else m + 40,
    };
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});
    var joined = false;
    defer if (!joined) {
        hold(transport, false);
        data_gate.store(true, .release);
        abort_gate.store(true, .release);
        joinClient(&c, t);
    };

    // Every response has its HEADERS written before the hold.
    try waitFor("every handler to start its response", m, struct {
        fn f(x: usize) bool {
            return started.load(.acquire) >= x;
        }
    }.f);

    // Hold the writer, then let the handlers write: the DATA fills the write
    // memory, which pauses the terminal loop.
    const entered_0 = holdEntered(transport);
    hold(transport, true);
    data_gate.store(true, .release);
    const Entered = struct { transport: Transport, at: u64 };
    try waitFor("the writer to enter the hold", Entered{ .transport = transport, .at = entered_0 }, struct {
        fn f(x: Entered) bool {
            return holdEntered(x.transport) != x.at;
        }
    }.f);
    zio.sleep(.fromMilliseconds(300)) catch {};

    switch (scenario) {
        .server_failures => {
            // M server RSTs, then the graceful GOAWAY.
            abort_gate.store(true, .release);
            try waitFor("every handler to abort", m, struct {
                fn f(x: usize) bool {
                    return aborted.load(.acquire) >= x;
                }
            }.f);
            zio.sleep(.fromMilliseconds(300)) catch {};
            server.requestShutdown();
            zio.sleep(.fromMilliseconds(300)) catch {};
        },
        .flood, .flood_intents => {
            c.send_flood.store(true, .release);
            zio.sleep(.fromMilliseconds(500)) catch {};
        },
    }
    const high_held = conn_mod.test_terminal_ring_high.load(.acquire);
    const full_held = conn_mod.test_ctrl_terminal_full.load(.monotonic) - full_0;
    std.debug.print("T3562-held transport={s} scenario={s} m={d} t={d} ring_high={d} terminal_full={d}\n", .{ @tagName(transport), @tagName(scenario), m, t_frames, high_held, full_held });

    hold(transport, false);
    joinClient(&c, t);
    joined = true;

    var streams_with_one_rst: usize = 0;
    var streams_with_more: usize = 0;
    for (0..max_sid) |sid| {
        if (sid % 2 == 0) continue;
        if (c.rst_per_sid[sid] == 1) streams_with_one_rst += 1;
        if (c.rst_per_sid[sid] > 1) streams_with_more += 1;
    }
    const full = conn_mod.test_ctrl_terminal_full.load(.monotonic) - full_0;
    std.debug.print("T3562 transport={s} scenario={s} m={d} t={d} ring_high={d} terminal_full={d} rst_total={d} one_rst={d} dup_rst={d} goaways={d} goaway_code={d} eof={} client_err={s} data_frames={d} headers_frames={d}\n", .{
        @tagName(transport),                             @tagName(scenario),  m, t_frames, conn_mod.test_terminal_ring_high.load(.acquire), full,
        c.rst_total,                                     streams_with_one_rst, streams_with_more,
        c.goaways,                                       c.goaway_code,       c.eof,
        if (c.err) |e| @errorName(e) else "-", c.frames[0], c.frames[1],
    });

    if (c.err) |e| return e;
    // The failure is not a terminal frame that could not be queued.
    try std.testing.expectEqual(@as(usize, 0), full);
    // The instrument ran: the stall queued the frames, and none went past T.
    try std.testing.expect(conn_mod.test_terminal_ring_high.load(.acquire) <= t_frames);
    try std.testing.expectEqual(@as(usize, 0), streams_with_more);
    try std.testing.expect(c.eof);
    try std.testing.expect(c.goaways >= 1);
    switch (scenario) {
        .server_failures => {
            // The TLS stash takes a few terminal frames before it is full.
            try std.testing.expect(high_held + 16 >= m);
            try std.testing.expectEqual(m, streams_with_one_rst);
            try std.testing.expectEqual(m, c.rst_total);
            try std.testing.expectEqual(@as(u32, @backingInt(frame.ErrorCode.no_error)), c.goaway_code);
        },
        .flood => {
            // The ring filled to the bound (T - 1 RSTs, then the GOAWAY) and
            // no further.
            // The actor does not read while the stall lasts, so the flood is
            // ingested when the hold ends, in one chunk: the ring is read
            // after the join, not at the held print.
            try std.testing.expectEqual(t_frames, conn_mod.test_terminal_ring_high.load(.acquire));
            try std.testing.expectEqual(t_frames - 1, c.rst_total);
            try std.testing.expectEqual(@as(u32, @backingInt(frame.ErrorCode.enhance_your_calm)), c.goaway_code);
            try std.testing.expectEqual(@as(usize, 1), c.goaways);
        },
        .flood_intents => {
            // The intent queue ran out before T: still a GOAWAY with the same
            // code, and fewer RSTs.
            const high = conn_mod.test_terminal_ring_high.load(.acquire);
            try std.testing.expect(high > 100);
            try std.testing.expect(high < t_frames);
            try std.testing.expectEqual(@as(u32, @backingInt(frame.ErrorCode.enhance_your_calm)), c.goaway_code);
            try std.testing.expectEqual(@as(usize, 1), c.goaways);
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

test "flood in a stall, h2c: past T the connection ends with GOAWAY(ENHANCE_YOUR_CALM)" {
    try runCaseTop(.h2c, .flood);
}

test "flood past the intent queue in a stall, h2c: the same GOAWAY(ENHANCE_YOUR_CALM)" {
    try runCaseTop(.h2c, .flood_intents);
}

test "flood past the intent queue in a stall, tls: the same GOAWAY(ENHANCE_YOUR_CALM)" {
    try runCaseTop(.tls, .flood_intents);
}

test "flood in a stall, tls: past T the connection ends with GOAWAY(ENHANCE_YOUR_CALM)" {
    try runCaseTop(.tls, .flood);
}
