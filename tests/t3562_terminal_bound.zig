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
//! abort overflowed the 32-entry ring. On h2c all M RSTs and the graceful
//! GOAWAY wait in the ring together. The TLS hold parks the actor: the RSTs
//! (from handlers) still wait, but the first ones reach the stash before it
//! is full, and the GOAWAY (from the actor) is queued after the hold.
//!
//! The other scenarios need the actor to READ during the stall, so they use
//! the stall a real peer makes: the client stops reading (with a small receive
//! buffer), and the handlers' DATA backs up into the socket and then the write
//! memory, on both transports, while the actor goes on reading.
//!
//! `crossed_resets` (h2c): every handler aborts during the stall (M server
//! RSTs wait), then the client resets all M streams itself, so the RSTs cross,
//! and opens M new streams whose handler spawn fails (`test_force_spawn_fail`:
//! M more RSTs, REFUSED_STREAM; a handler cannot start a response during the
//! stall, because `start` waits for its HEADERS to be written). The peer
//! never has more than M streams open in its own view, but without retiring
//! the crossed RSTs the ring would need 2M. Only h2c: on TLS half the handlers
//! wait in a flush (see `bare_flush`) and cannot abort, so the scenario cannot
//! pass T there. The retire is Session and scheduler code, the same on both.
//!
//! `flood`: the M streams stay open, and the client sends more than M HEADERS
//! for new stream ids in one write. Each is refused with an RST. Past T the
//! connection must queue its final GOAWAY (ENHANCE_YOUR_CALM), keep the ring at
//! T, and close by its closing deadline once the client reads again.
//!
//! `refused_end` (both transports): the writer is held with a small ordinary
//! share, and handlers send empty responses (HEADERS with END_STREAM). The
//! HEADERS past the share are refused, and a stream whose final HEADERS was
//! refused must still end for the peer: the client must read, for every
//! stream, either its HEADERS or an RST_STREAM, before a deadline.
//!
//! `flood_unread`: the same flood, but the client never reads again, so the
//! GOAWAY can never be written and only the closing deadline
//! (`close_flush_timeout_ns`, 1 s here) can end the connection. The server's
//! live connection count must reach zero by that deadline, measured from the
//! moment the ring reached T, which is after the GOAWAY was queued.
//!
//! `refused_batch` (h2c): the ordinary share is pinned by responses held in
//! the writer, and the peer sends one chunk of a PING, an early-413 request
//! and new requests. The PING ack, the 413 and the new responses are all
//! refused. Only those frames may be lost: the connection must stay, and every
//! stream must end for the peer (its HEADERS or one RST). TLS is not run: its
//! hold parks the actor, so nothing is read during it.
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
/// The server socket's send buffer in those scenarios (`test_sndbuf_bytes`).
const stall_sndbuf: u32 = 4096;
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
/// `refused_end`: the handlers wait for this, then send an empty response.
var empty_gate: std.atomic.Value(bool) = .init(false);
var empty_entered: std.atomic.Value(usize) = .init(0);
var empty_sent: std.atomic.Value(usize) = .init(0);
var empty_failed: std.atomic.Value(usize) = .init(0);
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
    empty_gate.store(false, .release);
    empty_entered.store(0, .release);
    empty_sent.store(0, .release);
    empty_failed.store(0, .release);
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
        // A barrier pushed before the socket stops moving is written at once,
        // and its flush returns; flush again until one waits. Once the stall
        // holds, a flush waits for good (it returns with an error when the
        // connection ends): for its barrier's write, or for stash room
        // (`test_room_waiting`).
        while (true) {
            try body.flush();
            try zio.sleep(.fromMilliseconds(2));
        }
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

/// Sends a response with no body: one HEADERS frame with END_STREAM. A send
/// the connection refuses returns its error, which is the path that resets the
/// stream (`runHandlerJobBody`).
fn empty(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = empty_entered.fetchAdd(1, .acq_rel);
    while (!empty_gate.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    resp.send(204, &.{}, "") catch |err| {
        _ = empty_failed.fetchAdd(1, .acq_rel);
        return err;
    };
    _ = empty_sent.fetchAdd(1, .acq_rel);
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/empty", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = empty } } },
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
const alive_ping = [8]u8{ 't', '3', '5', '6', '2', 'l', 'i', 'v' };
const batch_ping = [8]u8{ 't', '3', '5', '6', '2', 'b', 'a', 't' };
const nudge_ping = [8]u8{ 't', '3', '5', '6', '2', 'n', 'u', 'd' };

const Client = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    /// Streams opened at the start, and extra HEADERS (new ids past the
    /// advertised limit) sent when `send_flood` is set.
    streams: usize,
    path: []const u8 = "/stall",
    flood: usize,
    /// Null: read from the start. Otherwise connect with this receive buffer
    /// and read nothing until `read_gate` is set, which stalls the server.
    rcvbuf: ?c_int,
    read_gate: std.atomic.Value(bool) = .init(false),
    send_flood: std.atomic.Value(bool) = .init(false),
    flood_sent: std.atomic.Value(bool) = .init(false),
    /// Reset the `streams` first streams and open as many new ones.
    send_cross: std.atomic.Value(bool) = .init(false),
    cross_sent: std.atomic.Value(bool) = .init(false),
    /// Sent when reading starts; its ack proves the connection is alive.
    ping_acked: std.atomic.Value(bool) = .init(false),
    /// `refused_batch`: one write of a PING, an early-413 request and
    /// `batch_streams` new requests; then, on `send_alive`, a PING.
    send_batch: std.atomic.Value(bool) = .init(false),
    batch_sent: std.atomic.Value(bool) = .init(false),
    batch_streams: usize = 0,
    send_alive: std.atomic.Value(bool) = .init(false),
    alive_sent: bool = false,
    /// PINGs the test asked for, and sent: each wakes the server's actor,
    /// whose drain then fills what the blocked writer left of the queue.
    nudges: std.atomic.Value(u32) = .init(0),
    nudged: u32 = 0,
    /// Streams that ended for the client: a HEADERS with END_STREAM or an
    /// RST_STREAM, whichever came first.
    ended_per_sid: [max_sid]u8 = @splat(0),
    ended: std.atomic.Value(usize) = .init(0),
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
    for (0..c.streams) |i| try h2c.appendHeaders(c.gpa, &req, @intCast(1 + 2 * i), c.path, true);
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
        if (!c.batch_sent.load(.acquire) and c.send_batch.load(.acquire)) {
            var more: std.ArrayList(u8) = .empty;
            defer more.deinit(c.gpa);
            var ping: [17]u8 = undefined;
            _ = try frame.Serializer.ping(&ping, false, &batch_ping);
            try more.appendSlice(c.gpa, &ping);
            // A body far over the cap: the session answers 413 at once, with
            // the peer's side still open, and no handler runs.
            try h2c.appendHeadersExtra(c.gpa, &more, @intCast(1 + 2 * c.streams), "POST", "/empty", false, &.{.{ .name = "content-length", .value = "999999999" }});
            for (0..c.batch_streams) |i| try h2c.appendHeaders(c.gpa, &more, @intCast(1 + 2 * (c.streams + 1 + i)), c.path, true);
            try w.send(more.items);
            c.batch_sent.store(true, .release);
        }
        while (c.nudged < c.nudges.load(.acquire)) {
            var ping: [17]u8 = undefined;
            _ = try frame.Serializer.ping(&ping, false, &nudge_ping);
            try w.send(&ping);
            c.nudged += 1;
        }
        if (!c.alive_sent and c.send_alive.load(.acquire)) {
            var ping: [17]u8 = undefined;
            _ = try frame.Serializer.ping(&ping, false, &alive_ping);
            try w.send(&ping);
            c.alive_sent = true;
        }
        if (!c.cross_sent.load(.acquire) and c.send_cross.load(.acquire)) {
            var more: std.ArrayList(u8) = .empty;
            defer more.deinit(c.gpa);
            for (0..c.streams) |i| try h2c.appendRst(c.gpa, &more, @intCast(1 + 2 * i));
            for (0..c.streams) |i| try h2c.appendHeaders(c.gpa, &more, @intCast(1 + 2 * (c.streams + i)), "/stall", true);
            try w.send(more.items);
            c.cross_sent.store(true, .release);
        }
        if (!reading) {
            if (c.read_gate.load(.acquire)) {
                reading = true;
                // In every stall scenario, a peer that writes while the server
                // may already be closing: on Linux, data that reaches a closed
                // socket is answered with a reset, which drops what the client
                // has not read yet, the GOAWAY included (`closeFlushed`).
                var ping: [17]u8 = undefined;
                _ = try frame.Serializer.ping(&ping, false, &alive_ping);
                try w.send(&ping);
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
        if (s.sid != 0 and s.sid < max_sid and (s.ftype == 3 or (s.ftype == 1 and s.flags & 1 != 0))) {
            if (c.ended_per_sid[s.sid] == 0) _ = c.ended.fetchAdd(1, .acq_rel);
            c.ended_per_sid[s.sid] +|= 1;
        }
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
            6 => if (s.flags & 1 != 0) {
                if (std.mem.eql(u8, w.last_payload.items, &alive_ping)) c.ping_acked.store(true, .release);
            } else {
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

const Scenario = enum { server_failures, flood, flood_unread, crossed_resets };

/// `flood_unread`'s closing deadline, and the slack the measurement allows
/// above it: the actor checks the deadline on its own turn.
const unread_close_ns = std.time.ns_per_s;
const unread_close_slack_ns = std.time.ns_per_s;

/// `refused_end`: streams, and the ordinary share their final HEADERS meet.
const refused_end_streams = 32;
const refused_end_share = 8;

fn limitsFor(scenario: Scenario) starh2.Limits {
    var limits = starh2.Limits.defaults;
    // The flood's final GOAWAY is written after the stall; the closing
    // deadline must outlast the test's own waits, except where it is what
    // the scenario measures.
    limits.close_flush_timeout_ns = if (scenario == .flood_unread) unread_close_ns else 6 * std.time.ns_per_s;
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

/// Wait until `test_stalled`, nudging the actor with a client PING every
/// 100 ms while it is not.
fn waitStalled(c: *Client) !void {
    const deadline = nowNs() + client_deadline_ns;
    var next_nudge = nowNs() + 100 * std.time.ns_per_ms;
    while (!conn_mod.test_stalled.load(.acquire)) {
        const now = nowNs();
        if (now >= deadline) {
            std.debug.print("t3562: timed out waiting for the writer blocked with no room (nudges={d} writing={d})\n", .{ c.nudges.load(.acquire), wire_pump.test_writing.load(.acquire) });
            return error.Deadline;
        }
        if (now >= next_nudge) {
            _ = c.nudges.fetchAdd(1, .acq_rel);
            next_nudge = now + 100 * std.time.ns_per_ms;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
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
    const limits = limitsFor(scenario);
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
    conn_mod.test_stalled.store(false, .release);
    conn_mod.test_sndbuf_bytes.store(if (stalls_by_reading) stall_sndbuf else 0, .release);
    defer conn_mod.test_sndbuf_bytes.store(0, .release);
    var c: Client = .{
        .gpa = gpa,
        .transport = transport,
        .port = server.localAddress(0).getPort(),
        .streams = m,
        .flood = if (scenario == .flood or scenario == .flood_unread) m + 40 else 0,
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

    const retired_0 = conn_mod.test_rst_retired.load(.monotonic);
    const ended_blocked_0 = conn_mod.test_closing_ended_deadline_blocked.load(.monotonic);
    const ended_flushed_0 = conn_mod.test_closing_ended_deadline_flushed.load(.monotonic);
    const ended_failed_0 = conn_mod.test_closing_ended_writer_failed.load(.monotonic);
    const ended_eof_0 = conn_mod.test_closing_ended_peer_eof.load(.monotonic);
    // Every response has its HEADERS out before the stall.
    try waitStarted(&c, m);

    if (stalls_by_reading) {
        // The client reads nothing, and the server's socket send buffer is
        // small (`test_sndbuf_bytes`), so the writer blocks after a few KiB
        // and the DATA fills the write memory. The test waits for that state,
        // which cannot end while the client does not read: the writer
        // blocked on the socket with no room left for a pop
        // (`test_stalled`), and on TLS a flush parked for stash room.
        data_bytes.store(stalled_data_bytes, .release);
        if (transport == .tls) bare_flush.store(true, .release);
        data_gate.store(true, .release);
        const writers = if (transport == .tls) m / 2 else m;
        try waitFor("every writer to write its DATA", atLeast(&wrote, writers), reached);
        if (transport == .tls) try waitFor("a flush parked for stash room", atLeast(&conn_mod.test_room_waiting, 1), reached);
        // The gauge is computed by a drain. The last drain can come before
        // the writer blocks; the writer then takes chunks off the queue and
        // blocks in its write, and nothing wakes the actor to fill the queue
        // again. A PING from the client (it can still write) wakes it.
        try waitStalled(&c);
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
    var closed_ns: u64 = 0;
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
        .flood, .flood_unread => {
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
            if (scenario == .flood_unread) {
                // Nothing can be delivered: only the closing deadline ends it.
                const Live = struct { server: *starh2.Server };
                waitFor("the connection to close", Live{ .server = &server }, struct {
                    fn f(x: Live) bool {
                        return x.server.active_connections.load(.acquire) == 0;
                    }
                }.f) catch |err| {
                    std.debug.print("T3562-unread-timeout live={d} after {d} ms\n", .{ server.active_connections.load(.acquire), (nowNs() - bound_ns) / std.time.ns_per_ms });
                    return err;
                };
                closed_ns = nowNs();
            }
        },
        .crossed_resets => {
            abort_gate.store(true, .release);
            try waitFor("every handler to abort", atLeast(&aborted, m), reached);
            try waitFor("the M aborts' RSTs to wait in the ring", atLeast(&conn_mod.test_rst_queued_paused, rst_paused_0 + m), reached);
            conn_mod.test_force_spawn_fail = true;
            defer conn_mod.test_force_spawn_fail = false;
            c.send_cross.store(true, .release);
            try waitFor("the client to reset and reopen", &c, struct {
                fn f(x: *Client) bool {
                    return x.cross_sent.load(.acquire);
                }
            }.f);
            waitFor("the new streams' RSTs to wait in the ring", atLeast(&conn_mod.test_rst_queued_paused, rst_paused_0 + 2 * m), reached) catch |err| {
                std.debug.print("T3562-cross-timeout retired={d} rst_paused={d} ring_high={d} terminal_full={d}\n", .{
                    conn_mod.test_rst_retired.load(.monotonic) - retired_0,
                    conn_mod.test_rst_queued_paused.load(.monotonic) - rst_paused_0,
                    conn_mod.test_terminal_ring_high.load(.acquire),
                    conn_mod.test_ctrl_terminal_full.load(.monotonic) - full_0,
                });
                return err;
            };
        },
    }
    const rst_paused = conn_mod.test_rst_queued_paused.load(.monotonic) - rst_paused_0;
    const high_held = conn_mod.test_terminal_ring_high.load(.acquire);
    std.debug.print("T3562-held transport={s} scenario={s} m={d} t={d} ring_high={d} rst_paused={d} terminal_full={d} unread_close_ms={d}\n", .{ @tagName(transport), @tagName(scenario), m, t_frames, high_held, rst_paused, conn_mod.test_ctrl_terminal_full.load(.monotonic) - full_0, if (closed_ns != 0) (closed_ns - bound_ns) / std.time.ns_per_ms else 0 });

    hold(transport, false);
    c.read_gate.store(true, .release);
    if (scenario == .crossed_resets) {
        // Every RST was queued before the client's PING was read, and
        // terminal frames leave first, so they are all out by its ack.
        try waitFor("the PING ack", &c, struct {
            fn f(x: *Client) bool {
                return x.ping_acked.load(.acquire);
            }
        }.f);
        server.requestShutdown();
    }
    joinClient(&c, t);
    joined = true;

    const full = conn_mod.test_ctrl_terminal_full.load(.monotonic) - full_0;
    std.debug.print("T3562 transport={s} scenario={s} m={d} t={d} ring_high={d} terminal_full={d} rst_paused={d} rst_total={d} goaways={d} goaway_code={d} eof={} close_ms={d} client_err={s} data_frames={d} headers_frames={d}\n", .{
        @tagName(transport),                             @tagName(scenario), m,          t_frames,
        conn_mod.test_terminal_ring_high.load(.acquire), full,               rst_paused, c.rst_total,
        c.goaways,                                       c.goaway_code,      c.eof,      if (bound_ns != 0 and c.eof_ns > bound_ns) (c.eof_ns - bound_ns) / std.time.ns_per_ms else 0,
        if (c.err) |e| @errorName(e) else "-",          c.frames[0],        c.frames[1],
    });

    if (scenario == .flood_unread) {
        // The server closed with bytes the client never read. What the client
        // reads now, if anything, is not the claim here.
        try std.testing.expectEqual(t_frames, high_held);
        try std.testing.expectEqual(t_frames - 1, rst_paused);
        try std.testing.expectEqual(@as(usize, 0), full);
        std.debug.print("T3562-unread-ended blocked={d} flushed={d} writer_failed={d} close_ms={d}\n", .{
            conn_mod.test_closing_ended_deadline_blocked.load(.monotonic) - ended_blocked_0,
            conn_mod.test_closing_ended_deadline_flushed.load(.monotonic) - ended_flushed_0,
            conn_mod.test_closing_ended_writer_failed.load(.monotonic) - ended_failed_0,
            (closed_ns - bound_ns) / std.time.ns_per_ms,
        });
        // It ended because the closing deadline passed, once, and not by a
        // writer failure or a peer EOF. The output was not delivered either
        // way: still in the server (`blocked`) or in its kernel send queue
        // behind a client that never reads (`flushed`, which is what both
        // systems show: the lingering close waits for an EOF that never
        // comes).
        const by_deadline = (conn_mod.test_closing_ended_deadline_blocked.load(.monotonic) - ended_blocked_0) +
            (conn_mod.test_closing_ended_deadline_flushed.load(.monotonic) - ended_flushed_0);
        try std.testing.expectEqual(@as(usize, 1), by_deadline);
        try std.testing.expectEqual(ended_failed_0, conn_mod.test_closing_ended_writer_failed.load(.monotonic));
        try std.testing.expectEqual(ended_eof_0, conn_mod.test_closing_ended_peer_eof.load(.monotonic));
        try std.testing.expect(closed_ns - bound_ns <= unread_close_ns + unread_close_slack_ns);
        server.requestShutdown();
        serve_handle.join() catch {};
        serving = false;
        return;
    }
    if (c.err) |e| return e;
    // The slack was enough: no ordinary frame was refused by the pool.
    try std.testing.expectEqual(ordinary_full_0, conn_mod.test_ctrl_ordinary_pool_full.load(.monotonic));
    // The failure is not a terminal frame that could not be queued.
    try std.testing.expectEqual(@as(usize, 0), full);
    try std.testing.expect(conn_mod.test_terminal_ring_high.load(.acquire) <= t_frames);
    try std.testing.expect(c.eof);
    switch (scenario) {
        .server_failures => {
            if (transport == .h2c) {
                // All M RSTs and the graceful GOAWAY waited in the ring
                // together, during the hold.
                try std.testing.expectEqual(m, rst_paused);
                try std.testing.expectEqual(m + 1, high_held);
            } else {
                // TLS proves less, and its name says so: the drain that
                // filled the stash took the first RSTs into it before the
                // pause (one in every run so far), and the hold parks the
                // actor, which is what queues the graceful GOAWAY, so that
                // GOAWAY is queued after the hold.
                try std.testing.expect(rst_paused + 16 >= m);
                try std.testing.expect(high_held + 16 >= m);
            }
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
        .flood_unread => unreachable,
        .crossed_resets => {
            // All 2M RSTs were queued during the stall, and every crossed one
            // was retired before it was written.
            try std.testing.expectEqual(2 * m, rst_paused);
            try std.testing.expectEqual(m, conn_mod.test_rst_retired.load(.monotonic) - retired_0);
            try expectRsts(&c, 1, m, 0);
            try expectRsts(&c, 1 + 2 * m, m, 1);
            try std.testing.expectEqual(m, c.rst_total);
            try std.testing.expect(c.ping_acked.load(.acquire));
            try std.testing.expect(!c.enhance_seen);
            try std.testing.expect(c.goaways >= 2);
            try std.testing.expectEqual(@as(u32, @backingInt(frame.ErrorCode.no_error)), c.goaway_code);
        },
    }

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runRefusedEnd(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, tombstones: usize) !void {
    resetGlobals();
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var limits = starh2.Limits.defaults;
    // The preface, the SETTINGS ack and the WINDOW_UPDATE are written before
    // the hold, so the whole share is free for the responses' HEADERS.
    limits.control_entries_per_connection = refused_end_share;
    // Zero is a valid config: the RST a refused final HEADERS owes must not
    // depend on the tombstone ring.
    limits.stream_tombstones = tombstones;
    const n = refused_end_streams;

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
    defer empty_gate.store(true, .release);

    const ordinary_full_0 = conn_mod.test_ctrl_ordinary_pool_full.load(.monotonic);
    var c: Client = .{
        .gpa = gpa,
        .transport = transport,
        .port = server.localAddress(0).getPort(),
        .streams = n,
        .path = "/empty",
        .flood = 0,
        .rcvbuf = null,
    };
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});
    var joined = false;
    defer if (!joined) {
        hold(transport, false);
        empty_gate.store(true, .release);
        joinClient(&c, t);
    };

    try waitFor("every handler to run", atLeast(&empty_entered, n), reached);
    // Hold the writer, then let every handler send: the first HEADERS take
    // the ordinary share until their write completes, and the rest are
    // refused. A send waits for its HEADERS to be written, so the handlers
    // whose HEADERS were taken wait out the hold; at most the share's worth.
    hold(transport, true);
    empty_gate.store(true, .release);
    try waitFor("the refused handlers to fail", atLeast(&empty_failed, n - refused_end_share), reached);
    const failed = empty_failed.load(.acquire);
    const refused = conn_mod.test_ctrl_ordinary_pool_full.load(.monotonic) - ordinary_full_0;
    std.debug.print("T3562-held transport={s} scenario=refused_end n={d} sent={d} failed={d} ordinary_pool_full={d}\n", .{ @tagName(transport), n, empty_sent.load(.acquire), failed, refused });
    // The instrument fired: some final HEADERS were refused.
    try std.testing.expect(failed > 0);
    try std.testing.expect(refused > 0);

    hold(transport, false);
    // The oracle: every stream ends for the peer. A stream whose final
    // HEADERS was refused and that gets no RST leaves the client waiting, and
    // this deadline fails.
    waitFor("every stream to end for the client", &c, struct {
        fn f(x: *Client) bool {
            return x.ended.load(.acquire) >= refused_end_streams;
        }
    }.f) catch |err| {
        var missing: usize = 0;
        for (0..n) |i| {
            if (c.ended_per_sid[1 + 2 * i] == 0) missing += 1;
        }
        std.debug.print("T3562-refused-end-timeout transport={s} ended={d} of {d} missing={d} rst_total={d} frames={any} ring_high={d} terminal_full={d} client_err={s} done={}\n", .{ @tagName(transport), c.ended.load(.acquire), n, missing, c.rst_total, c.frames, conn_mod.test_terminal_ring_high.load(.acquire), conn_mod.test_ctrl_terminal_full.load(.monotonic), if (c.err) |e| @errorName(e) else "-", c.done.load(.acquire) });
        return err;
    };
    server.requestShutdown();
    joinClient(&c, t);
    joined = true;
    if (c.err) |e| return e;
    // Each stream ended once: a refused one by its RST, the rest by HEADERS.
    // The count of refusals is read now that every handler is done: under
    // load one more HEADERS can be refused after the wait above.
    try expectEnded(&c, n);
    try std.testing.expectEqual(empty_failed.load(.acquire), c.rst_total);
    try std.testing.expect(c.eof);

    serve_handle.join() catch {};
    serving = false;
}

/// `refused_batch`: the share's frames held, plus two refused to prove it.
const batch_held = refused_end_share + 2;
const batch_new = 4;

fn runRefusedBatch(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetGlobals();
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    var limits = starh2.Limits.defaults;
    limits.control_entries_per_connection = refused_end_share;
    var server = try serverFor(rt, gpa, .h2c, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    defer hold(.h2c, false);
    defer empty_gate.store(true, .release);

    const ordinary_full_0 = conn_mod.test_ctrl_ordinary_pool_full.load(.monotonic);
    var c: Client = .{
        .gpa = gpa,
        .transport = .h2c,
        .port = server.localAddress(0).getPort(),
        .streams = batch_held,
        .path = "/empty",
        .flood = 0,
        .rcvbuf = null,
        .batch_streams = batch_new,
    };
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});
    var joined = false;
    defer if (!joined) {
        hold(.h2c, false);
        empty_gate.store(true, .release);
        joinClient(&c, t);
    };

    try waitFor("every handler to run", atLeast(&empty_entered, batch_held), reached);
    // Pin the ordinary share: the writer holds the first responses' HEADERS,
    // and two more are refused, which proves the share is full.
    hold(.h2c, true);
    empty_gate.store(true, .release);
    try waitFor("the share to be pinned", atLeast(&empty_failed, batch_held - refused_end_share), reached);
    const failed_0 = empty_failed.load(.acquire);

    c.send_batch.store(true, .release);
    // Every new request reaches its handler and is refused too. The read
    // path refused the PING ack and the 413 before that and went on.
    try waitFor("the new handlers to fail", atLeast(&empty_failed, failed_0 + batch_new), reached);
    const refused = conn_mod.test_ctrl_ordinary_pool_full.load(.monotonic) - ordinary_full_0;
    std.debug.print("T3562-held transport=h2c scenario=refused_batch held={d} failed={d} ordinary_pool_full={d}\n", .{ batch_held, empty_failed.load(.acquire), refused });
    // The two pinning refusals, the PING ack, the 413, the new responses.
    try std.testing.expect(refused >= 2 + 1 + 1 + batch_new);

    hold(.h2c, false);
    const total = batch_held + 1 + batch_new;
    const Want = struct { c: *Client, n: usize };
    waitFor("every stream to end for the client", Want{ .c = &c, .n = total }, struct {
        fn f(x: Want) bool {
            return x.c.ended.load(.acquire) >= x.n;
        }
    }.f) catch |err| {
        std.debug.print("T3562-refused-batch-timeout ended={d} of {d} rst_total={d} goaways={d} eof={} client_err={s}\n", .{ c.ended.load(.acquire), total, c.rst_total, c.goaways, c.eof, if (c.err) |e| @errorName(e) else "-" });
        return err;
    };
    c.send_alive.store(true, .release);
    try waitFor("the PING ack", &c, struct {
        fn f(x: *Client) bool {
            return x.ping_acked.load(.acquire);
        }
    }.f);
    try std.testing.expect(!c.eof);
    try std.testing.expectEqual(@as(usize, 0), c.goaways);
    server.requestShutdown();
    joinClient(&c, t);
    joined = true;
    if (c.err) |e| return e;
    try expectEnded(&c, total);
    serve_handle.join() catch {};
    serving = false;
}

fn runRefusedBatchTop() !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runRefusedBatch, .{ rt, gpa });
    try h.join();
}

fn expectEnded(c: *const Client, n: usize) !void {
    for (0..n) |i| {
        const sid = 1 + 2 * i;
        if (c.ended_per_sid[sid] != 1) {
            std.debug.print("t3562: stream {d} ended {d} times, want 1\n", .{ sid, c.ended_per_sid[sid] });
            return error.TestUnexpectedResult;
        }
    }
}

fn runRefusedEndTop(transport: Transport, tombstones: usize) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runRefusedEnd, .{ rt, gpa, transport, tombstones });
    try h.join();
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

test "server failures in one stall, tls: the M aborts' RSTs wait in the ring and the stash, one RST per stream, the graceful GOAWAY after the hold" {
    try runCaseTop(.tls, .server_failures);
}

test "flood during a stall, h2c: past T the connection ends with GOAWAY(ENHANCE_YOUR_CALM)" {
    try runCaseTop(.h2c, .flood);
}

test "flood during a stall, tls: past T the connection ends with GOAWAY(ENHANCE_YOUR_CALM)" {
    try runCaseTop(.tls, .flood);
}

test "flood during a stall that never ends, h2c: the closing deadline ends the connection" {
    try runCaseTop(.h2c, .flood_unread);
}

test "flood during a stall that never ends, tls: the closing deadline ends the connection" {
    try runCaseTop(.tls, .flood_unread);
}

test "crossed resets during a stall, h2c: the peer's RSTs retire the server's unwritten ones, and M new failures fit" {
    try runCaseTop(.h2c, .crossed_resets);
}

test "refused final HEADERS, h2c: the stream still ends for the peer, with an RST" {
    try runRefusedEndTop(.h2c, starh2.Limits.defaults.stream_tombstones);
}

test "refused final HEADERS, tls: the stream still ends for the peer, with an RST" {
    try runRefusedEndTop(.tls, starh2.Limits.defaults.stream_tombstones);
}

test "refused final HEADERS with no tombstones kept, h2c: the stream still ends for the peer, with an RST" {
    try runRefusedEndTop(.h2c, 0);
}

test "ordinary refusals in one read chunk, h2c: only the refused frames are lost, and every stream still ends" {
    try runRefusedBatchTop();
}
