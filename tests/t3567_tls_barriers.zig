//! t-3567: bare flush barriers (a handler's `Body.flush` with nothing
//! pending) against a held writer, on h2c and on TLS.
//!
//! `n` streaming handlers start their responses while the writer is free
//! (a response waits for its HEADERS to be written), then the writer is held
//! (`wire_pump.test_hold_writes`, or on TLS `test_hold_stash` too), one more
//! request puts a chunk in front of the hold, and a gate lets every handler
//! flush once with nothing pending, then finish.
//!
//! On both transports each barrier waits for room outside `session_mu`
//! (`waitForAckRoom`: ack channel and write queue on h2c, the stash on TLS),
//! and every response must end once the hold is released. Before the fix the
//! wait was armed on h2c only: a TLS barrier went straight to `Pump.push`,
//! and a full stash failed it (`tls_write_overflow`). The test asks the same
//! of both transports: no overflow, no failed flush, no RST_STREAM, no server
//! close, every response ended, and the barriers WAITED while held.
//!
//! t-3594 adds two groups to this file, because they ask the same question
//! (does a handler's change wake an idle actor?) of the same server:
//!
//! - An idle table: each `Response` operation on an otherwise idle connection
//!   (send with an empty body, send with a body, start + write + end, a bare
//!   flush, abort), on both transports, each with a deadline. A GET arms no
//!   actor timer, and before each operation every handler waits 150 ms and then
//!   until the actor is observed inside its park select (a deadline that fails
//!   the row), so only a wake can move it.
//!   After the operation the handler stays alive until the client has SEEN its
//!   effect (the next frame, or the reset), within a bound: a handler that
//!   exited would post its completion and wake the actor by that, and hide the
//!   missing wake.
//! - The failure regression: a response refused while it is materialized (the
//!   CONTINUATION of a large header block meets a full control pool) fails the
//!   connection from the handler's task. The handler catches the error and
//!   stays alive. The ACTOR must still reach its teardown by a deadline.
//!
//! Every case prints one `T3567 ...` line with its counters before it
//! asserts, so a rate can be read from repeated runs. Run it with
//! `./zb build test-t3567` (it is also part of `./zb build test`). Every wait
//! has a deadline, so a lost wake fails a case and does not hang the suite.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const wire_client = @import("wire_client.zig");

const conn_mod = starh2.edge.connection;
const wire_pump = starh2.edge.wire_pump;
const hpack = starh2.core.hpack;
const tls_edge = starh2.edge.tls_edge;

const Transport = wire_client.Transport;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;
const client_deadline_ns = wire_client.client_deadline_ns;

const dummy: u8 = 0;
const max_flushers = 64;

// ---------------------------------------------------------------------------
// The handlers.

var flush_gate: std.atomic.Value(bool) = .init(false);
var started: std.atomic.Value(usize) = .init(0);
var flush_attempts: std.atomic.Value(usize) = .init(0);
var flush_ok: std.atomic.Value(usize) = .init(0);
var flush_err: std.atomic.Value(usize) = .init(0);
var first_flush_err: std.atomic.Value(u16) = .init(0);

fn resetGlobals() void {
    flush_gate.store(false, .release);
    started.store(0, .release);
    flush_attempts.store(0, .release);
    flush_ok.store(0, .release);
    flush_err.store(0, .release);
    first_flush_err.store(0, .release);
    obs_headers.store(false, .release);
    obs_data.store(false, .release);
    obs_end.store(false, .release);
    obs_rst.store(false, .release);
    obs_missed.store(0, .release);
    ops_done.store(0, .release);
    obs_status.store(0, .release);
    obs_body_len.store(0, .release);
    park_missed.store(0, .release);
    fail_gate.store(false, .release);
    fail_alive.store(false, .release);
    fail_caught.store(0, .release);
}

/// Starts its response, waits for the gate, flushes once with nothing
/// pending (a bare barrier), then finishes.
fn flusher(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    _ = started.fetchAdd(1, .acq_rel);
    while (!flush_gate.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    _ = flush_attempts.fetchAdd(1, .acq_rel);
    body.flush() catch |err| {
        _ = flush_err.fetchAdd(1, .acq_rel);
        _ = first_flush_err.cmpxchgStrong(0, @intFromError(err), .acq_rel, .acquire);
        return err;
    };
    _ = flush_ok.fetchAdd(1, .acq_rel);
    try body.finish();
}

var spin_stop: std.atomic.Value(bool) = .init(false);
var spin_next: std.atomic.Value(usize) = .init(0);
/// How many handlers keep flushing, and how many of the others are done.
var spin_keepers: std.atomic.Value(usize) = .init(0);
var spin_done: std.atomic.Value(usize) = .init(0);

/// The first `spin_keepers` handlers to start flush with nothing pending again
/// and again until told to stop: while one runs, the TLS driver always finds a
/// barrier to take next. Every other handler flushes ONCE and finishes; those
/// are the ones that park behind a full stash and have to be woken.
fn spinner(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    const me = spin_next.fetchAdd(1, .acq_rel);
    _ = started.fetchAdd(1, .acq_rel);
    while (!flush_gate.load(.acquire)) try zio.sleep(.fromMilliseconds(2));
    if (me < spin_keepers.load(.acquire)) {
        while (!spin_stop.load(.acquire)) try body.flush();
    } else {
        try body.flush();
        _ = spin_done.fetchAdd(1, .acq_rel);
    }
    try body.finish();
}

/// A task handler that answers with no body at all, after the connection has
/// gone quiet: by then the actor has finished its turn and is parked, so the
/// handler's push is the only thing that can wake it.
fn empty204(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try idleSleep();
    try resp.send(204, &.{}, "");
    opDone();
}

/// How long a handler of the idle table waits before each operation. The actor
/// has finished its turn and is parked long before this ends.
const idle_ms = 150;

/// What the client has seen on stream 1. A handler waits on these after an
/// operation so that it does not exit (its exit posts a completion and wakes
/// the actor, which would hide a missing wake).
var obs_headers: std.atomic.Value(bool) = .init(false);
var obs_data: std.atomic.Value(bool) = .init(false);
var obs_end: std.atomic.Value(bool) = .init(false);
var obs_rst: std.atomic.Value(bool) = .init(false);
/// Observations that did not arrive in time.
var obs_missed: std.atomic.Value(usize) = .init(0);
/// Operations the handler was asked to do that returned success. A handler that
/// takes an error path ends in `runHandlerJobBody`'s fallback 500, which the client
/// would read as an END_STREAM; the row compares this count with the one it expects.
var ops_done: std.atomic.Value(usize) = .init(0);
/// The first status the client decoded on stream 1 (0 if none), and the DATA bytes
/// it read there. Written by the client thread, read after it is joined.
var obs_status: std.atomic.Value(u16) = .init(0);
var obs_body: [64]u8 = undefined;
var obs_body_len: std.atomic.Value(usize) = .init(0);

fn opDone() void {
    _ = ops_done.fetchAdd(1, .acq_rel);
}

/// Decode the `:status` of a HEADERS payload on stream 1 (the first header block
/// of the connection, so the dynamic table is empty). Any failure leaves 0.
fn noteStatus(gpa: std.mem.Allocator, block: []const u8) void {
    if (obs_status.load(.acquire) != 0) return;
    var dec = hpack.Decoder.init(gpa) catch return;
    defer dec.deinit();
    const res = dec.decode(block, 100, 64 * 1024, 256, 16 * 1024) catch return;
    defer dec.freeResult(res);
    for (res.fields) |hf| {
        if (std.mem.eql(u8, hf.name, ":status")) {
            const v = std.fmt.parseInt(u16, hf.value, 10) catch return;
            obs_status.store(v, .release);
            return;
        }
    }
}

fn noteBody(bytes: []const u8) void {
    var n = obs_body_len.load(.acquire);
    for (bytes) |b| {
        if (n >= obs_body.len) break;
        obs_body[n] = b;
        n += 1;
    }
    obs_body_len.store(n, .release);
}

fn awaitObs(flag: *std.atomic.Value(bool)) void {
    const deadline = nowNs() + 4 * std.time.ns_per_s;
    while (!flag.load(.acquire)) {
        if (nowNs() >= deadline) {
            _ = obs_missed.fetchAdd(1, .acq_rel);
            return;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

/// Park waits that ran out of time. A row with one is a failure: its operation
/// was not made while the actor was asleep, so it proves nothing.
var park_missed: std.atomic.Value(usize) = .init(0);

/// Wait before an operation: a fixed sleep, then until the actor is observed
/// inside its park select (`test_actors_in_select`, raised just before the
/// select and lowered right after it). A sleep alone is no happens-before with
/// the park: an actor still in its turn would read the handler's change at its
/// next snapshot and the row would pass with the ring removed.
///
/// What this does NOT prove, in plain words. The wait SAMPLES the actor; it does
/// not hold it. The gap is narrow. An unrelated wake could end the select after
/// the sample, and the handler's release could start before the actor reads
/// `releases_at_turn_top` at the top of its next turn. The actor reads that
/// snapshot BEFORE it takes the lock, so the next turn sees the handler's change
/// under the lock anyway. That is a legal schedule and not a lost wake, so a row
/// can pass without the ring and no audit fires. A release that starts AFTER that
/// snapshot and skips its ring is not in the gap: it trips the park check on the
/// same turn. An idle GET has no other wake source, so the gap does not open in
/// practice, and with the ring removed every row failed. The evidence is
/// statistical: do not read a passing row as a proof that the ring is there. The
/// proof is the park audit and the unit tests.
fn idleSleep() !void {
    try zio.sleep(.fromMilliseconds(idle_ms));
    const deadline = nowNs() + 4 * std.time.ns_per_s;
    while (conn_mod.test_actors_in_select.load(.acquire) == 0) {
        if (nowNs() >= deadline) {
            _ = park_missed.fetchAdd(1, .acq_rel);
            return;
        }
        zio.sleep(.fromMilliseconds(1)) catch {};
    }
}

fn idleSendBody(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try idleSleep();
    try resp.send(200, &.{}, "hello");
    opDone();
    awaitObs(&obs_end);
}

fn idleStartWriteEnd(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try idleSleep();
    var body = try resp.start(200, &.{});
    opDone();
    awaitObs(&obs_headers);
    try idleSleep();
    try body.writeAll("x");
    opDone();
    awaitObs(&obs_data);
    try idleSleep();
    try body.finish();
    opDone();
    awaitObs(&obs_end);
}

fn idleBareFlush(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try idleSleep();
    var body = try resp.start(200, &.{});
    opDone();
    awaitObs(&obs_headers);
    try idleSleep();
    try body.flush();
    opDone();
    try idleSleep();
    try body.finish();
    opDone();
    awaitObs(&obs_end);
}

fn idleAbort(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try idleSleep();
    var body = try resp.start(200, &.{});
    opDone();
    awaitObs(&obs_headers);
    try idleSleep();
    try body.abort();
    opDone();
    awaitObs(&obs_rst);
}

const wide_value_len = 7000;
const wide_a: [wide_value_len]u8 = @splat('a');
const wide_b: [wide_value_len]u8 = @splat('b');
const wide_c: [wide_value_len]u8 = @splat('c');

/// The control pool the failure regression sets (`fail_control_bytes`) takes
/// the first fragment of this block (a 16 KiB HEADERS frame) and refuses the
/// CONTINUATION.
var fail_gate: std.atomic.Value(bool) = .init(false);
var fail_alive: std.atomic.Value(bool) = .init(false);
var fail_caught: std.atomic.Value(u16) = .init(0);

/// Sends a response whose header block needs a CONTINUATION, catches the
/// refusal and STAYS alive until the test opens the gate. It swallows the
/// cancel that the actor's teardown delivers, so the connection can only end
/// by the actor getting there on its own.
fn refusedAndAlive(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    fail_alive.store(true, .release);
    defer fail_alive.store(false, .release);
    idleSleep() catch {};
    resp.send(200, &.{
        .{ .name = "x-wide-a", .value = &wide_a },
        .{ .name = "x-wide-b", .value = &wide_b },
        .{ .name = "x-wide-c", .value = &wide_c },
    }, "") catch |err| {
        fail_caught.store(@intFromError(err), .release);
    };
    const cap = nowNs() + 20 * std.time.ns_per_s;
    while (!fail_gate.load(.acquire) and nowNs() < cap) zio.sleep(.fromMilliseconds(2)) catch {};
}

/// The request that puts a chunk in front of the hold.
fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/flusher", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = flusher } } },
    .{ .method = .GET, .path = "/spinner", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = spinner } } },
    .{ .method = .GET, .path = "/empty204", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = empty204 } } },
    .{ .method = .GET, .path = "/idle-send-body", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = idleSendBody } } },
    .{ .method = .GET, .path = "/idle-swe", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = idleStartWriteEnd } } },
    .{ .method = .GET, .path = "/idle-flush", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = idleBareFlush } } },
    .{ .method = .GET, .path = "/idle-abort", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = idleAbort } } },
    .{ .method = .GET, .path = "/refused", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = refusedAndAlive } } },
    .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
};

fn newRuntime(gpa: std.mem.Allocator) !*zio.Runtime {
    return zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
}

fn serverFor(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, cert: *[]u8, key: *[]u8) !starh2.Server {
    return serverForLimits(rt, gpa, transport, starh2.Limits.defaults, cert, key);
}

fn serverForLimits(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, limits: starh2.Limits, cert: *[]u8, key: *[]u8) !starh2.Server {
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
// The client: one connection on its own thread. It sends the flusher
// requests, then the trigger request when the test asks, and reads every
// frame until each stream has ended or been reset, or the server closes.

const Client = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    n: usize,
    path: []const u8 = "/flusher",
    /// How long the reader waits for every stream to resolve.
    deadline_ns: u64 = 3 * client_deadline_ns,
    send_trigger: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    ended: [max_flushers]bool = @splat(false),
    reset: [max_flushers]bool = @splat(false),
    trigger_ended: bool = false,
    resets: usize = 0,
    goaway: bool = false,
    goaway_code: u32 = 0,
    /// The server closed the socket before every stream was resolved.
    closed_early: bool = false,

    fn sid(pos: usize) u31 {
        return @intCast(1 + 2 * pos);
    }

    fn triggerSid(self: *const Client) u31 {
        return sid(self.n);
    }

    fn resolved(self: *const Client) usize {
        var k: usize = 0;
        for (0..self.n) |i| {
            if (self.ended[i] or self.reset[i]) k += 1;
        }
        return k;
    }
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
    for (0..c.n) |pos| try h2c.appendHeaders(c.gpa, &req, Client.sid(pos), c.path, true);
    try w.send(req.items);

    var trigger_sent = false;
    const deadline = nowNs() + c.deadline_ns;
    while (c.resolved() < c.n or (trigger_sent and !c.trigger_ended)) {
        if (!trigger_sent and c.send_trigger.load(.acquire)) {
            var one: std.ArrayList(u8) = .empty;
            defer one.deinit(c.gpa);
            try h2c.appendHeaders(c.gpa, &one, c.triggerSid(), "/hello", true);
            try w.send(one.items);
            trigger_sent = true;
        }
        const s = w.next(nowNs() + 25 * std.time.ns_per_ms) catch |err| switch (err) {
            error.Deadline => {
                if (nowNs() >= deadline) return error.Deadline;
                continue;
            },
            else => return err,
        } orelse {
            c.closed_early = true;
            return;
        };
        if (s.sid == 1) {
            const end = s.flags & 1 != 0;
            switch (s.ftype) {
                1 => {
                    noteStatus(c.gpa, w.last_payload.items);
                    obs_headers.store(true, .release);
                    if (end) obs_end.store(true, .release);
                },
                0 => {
                    noteBody(w.last_payload.items);
                    obs_data.store(true, .release);
                    if (end) obs_end.store(true, .release);
                },
                3 => obs_rst.store(true, .release),
                else => {},
            }
        }
        if (s.sid == c.triggerSid()) {
            // A reset is a reset, whichever stream it is on; only END_STREAM on
            // HEADERS or DATA ends the trigger's response.
            if (s.ftype == 3) {
                c.resets += 1;
            } else if ((s.ftype == 0 or s.ftype == 1) and s.flags & 1 != 0) {
                c.trigger_ended = true;
            }
            continue;
        }
        switch (s.ftype) {
            0, 1 => if (s.sid % 2 == 1 and s.flags & 1 != 0) {
                const pos = (s.sid - 1) / 2;
                if (pos < c.n) c.ended[pos] = true;
            },
            3 => {
                c.resets += 1;
                const pos = (s.sid - 1) / 2;
                if (s.sid % 2 == 1 and pos < c.n) c.reset[pos] = true;
            },
            7 => {
                c.goaway = true;
                const p = w.last_payload.items;
                if (p.len >= 8) c.goaway_code = std.mem.readInt(u32, p[4..8], .big);
            },
            else => {},
        }
    }
}

fn waitFor(comptime what: []const u8, ctx: anytype, comptime pred: fn (@TypeOf(ctx)) bool) !void {
    const deadline = nowNs() + client_deadline_ns;
    while (!pred(ctx)) {
        if (nowNs() >= deadline) {
            std.debug.print("t3567: timed out waiting for {s}\n", .{what});
            return error.Deadline;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

/// Yield until the client thread is done, then join it. A blocking join on
/// the zio task could stop the executor that runs the server.
fn joinClient(c: *Client, t: std.Thread) void {
    const deadline = nowNs() + 4 * client_deadline_ns;
    while (!c.done.load(.acquire) and nowNs() < deadline) zio.sleep(.fromMilliseconds(5)) catch {};
    t.join();
}

// ---------------------------------------------------------------------------
// The scenario.

const Hold = enum { writes, stash };

fn runCase(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, hold: Hold, n: usize) !void {
    std.debug.assert(n <= max_flushers);
    std.debug.assert(!(transport == .h2c and hold == .stash));
    resetGlobals();
    conn_mod.test_tls_barrier_min_room.store(std.math.maxInt(usize), .release);
    conn_mod.close_reason = true;
    defer conn_mod.close_reason = false;
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var server = try serverFor(rt, gpa, transport, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    defer flush_gate.store(true, .release);
    defer wire_pump.test_hold_writes.store(false, .release);
    defer conn_mod.test_hold_stash.store(false, .release);

    const overflow_0 = tls_edge.tls_write_overflow.load(.monotonic);
    const term_full_0 = conn_mod.test_ctrl_terminal_full.load(.monotonic);
    const waits_0 = conn_mod.test_ack_room_waits.load(.monotonic);
    const refusals_0 = conn_mod.test_queue_push_refusals.load(.monotonic);

    var c: Client = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort(), .n = n };
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});
    var joined = false;
    defer if (!joined) {
        wire_pump.test_hold_writes.store(false, .release);
        conn_mod.test_hold_stash.store(false, .release);
        flush_gate.store(true, .release);
        joinClient(&c, t);
    };

    // Every flusher has its HEADERS on the wire before the hold.
    try waitFor("the flushers to start their responses", n, struct {
        fn f(x: usize) bool {
            return started.load(.acquire) >= x;
        }
    }.f);

    // The hold, proved entered: the writer stops with the trigger's chunk.
    const entered_0 = switch (hold) {
        .writes => wire_pump.test_hold_entered.load(.acquire),
        .stash => conn_mod.test_hold_stash_entered.load(.acquire),
    };
    switch (hold) {
        .writes => wire_pump.test_hold_writes.store(true, .release),
        .stash => conn_mod.test_hold_stash.store(true, .release),
    }
    c.send_trigger.store(true, .release);
    const Entered = struct { hold: Hold, at: u64 };
    try waitFor("the writer to enter the hold", Entered{ .hold = hold, .at = entered_0 }, struct {
        fn f(x: Entered) bool {
            return switch (x.hold) {
                .writes => wire_pump.test_hold_entered.load(.acquire) != x.at,
                .stash => conn_mod.test_hold_stash_entered.load(.acquire) != x.at,
            };
        }
    }.f);

    // Every handler flushes once while the writer is held.
    flush_gate.store(true, .release);
    try waitFor("every flusher to try its flush", n, struct {
        fn f(x: usize) bool {
            return flush_attempts.load(.acquire) >= x;
        }
    }.f);
    zio.sleep(.fromMilliseconds(300)) catch {};
    const held_overflow = tls_edge.tls_write_overflow.load(.monotonic) - overflow_0;
    const held_err = flush_err.load(.acquire);
    const held_waits = conn_mod.test_ack_room_waits.load(.monotonic) - waits_0;
    // Printed before the release too: a case can panic after it.
    std.debug.print("T3567-held transport={s} hold={s} n={d} overflow_while_held={d} flush_err_while_held={d} barrier_waits_while_held={d} ctrl_terminal_full={d}\n", .{
        @tagName(transport),                                              @tagName(hold), n, held_overflow, held_err, held_waits,
        conn_mod.test_ctrl_terminal_full.load(.monotonic) - term_full_0,
    });

    // Release, and let every stream resolve (or the connection end).
    wire_pump.test_hold_writes.store(false, .release);
    conn_mod.test_hold_stash.store(false, .release);
    joinClient(&c, t);
    joined = true;
    zio.sleep(.fromMilliseconds(100)) catch {};

    var ended: usize = 0;
    var reset: usize = 0;
    for (0..n) |i| {
        if (c.ended[i]) ended += 1;
        if (c.reset[i]) reset += 1;
    }
    const overflow = tls_edge.tls_write_overflow.load(.monotonic) - overflow_0;
    const term_full = conn_mod.test_ctrl_terminal_full.load(.monotonic) - term_full_0;
    const refusals = conn_mod.test_queue_push_refusals.load(.monotonic) - refusals_0;
    const err_code = first_flush_err.load(.acquire);
    const err_name: []const u8 = if (err_code == 0) "-" else @errorName(@errorFromInt(err_code));
    const server_closed = c.closed_early or c.goaway;
    std.debug.print(
        "T3567 transport={s} hold={s} n={d} overflow={d} overflow_while_held={d} flush_ok={d} flush_err={d} flush_err_while_held={d} first_err={s} barrier_waits_while_held={d} queue_refusals={d} ctrl_terminal_full={d} ended={d} rst={d} unresolved={d} goaway={} goaway_code={d} server_closed={} client_err={s}\n",
        .{
            @tagName(transport),        @tagName(hold),             n,
            overflow,                   held_overflow,              flush_ok.load(.acquire),
            flush_err.load(.acquire),   held_err,                   err_name,
            held_waits,                 refusals,                   term_full,
            ended,                      reset,                      n - ended - reset,
            c.goaway,                   c.goaway_code,              server_closed,
            if (c.err) |e| @errorName(e) else "-",
        },
    );

    if (c.err) |e| return e;
    try std.testing.expectEqual(@as(u64, 0), overflow);
    try std.testing.expectEqual(@as(usize, 0), flush_err.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), reset);
    try std.testing.expectEqual(@as(usize, 0), c.resets);
    try std.testing.expect(!server_closed);
    try std.testing.expectEqual(n, ended);
    try std.testing.expect(c.trigger_ended);
    // The barriers did the work: they really waited for room while the writer
    // was held (a run where none waited proves nothing).
    try std.testing.expect(held_waits > 0);
    // On TLS a barrier is let in only while the stash is not full, so it always
    // leaves the drain's tail push its slot (a one-slot test would let barriers
    // take the last slots).
    if (transport == .tls) {
        // A barrier is admitted at `pushes_per_pop + tail_pushes_per_drain` free
        // slots (not full) and leaves one fewer. A value must have been recorded.
        const min_room = conn_mod.test_tls_barrier_min_room.load(.acquire);
        try std.testing.expect(min_room != std.math.maxInt(usize));
        try std.testing.expect(min_room >= tls_edge.Pump.pushes_per_pop + tls_edge.Pump.tail_pushes_per_drain - 1);
    }
    if (transport == .h2c) try std.testing.expectEqual(@as(usize, 0), refusals);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

/// Barriers that keep coming. A write delay on the TLS pump (2 ms a chunk)
/// makes the driver's take loop slow, so the keepers refill the stash while it
/// runs and the loop never finds the stash empty. The other handlers flush once
/// each, most of them behind a full stash, and every one must get through while
/// the keepers go on: a parked handler has to be woken by a take INSIDE that
/// loop, not after it (which would be never, until the keepers stop). The
/// keepers stop only when every other handler has had its barrier acked.
fn runSpinCase(rt: *zio.Runtime, gpa: std.mem.Allocator, n: usize, keepers: usize) !void {
    std.debug.assert(n <= max_flushers and keepers < n);
    resetGlobals();
    spin_stop.store(false, .release);
    spin_next.store(0, .release);
    spin_keepers.store(keepers, .release);
    spin_done.store(0, .release);
    conn_mod.test_write_delay_ms = 2;
    defer conn_mod.test_write_delay_ms = 0;
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var server = try serverFor(rt, gpa, .tls, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    defer spin_stop.store(true, .release);
    defer flush_gate.store(true, .release);

    const overflow_0 = tls_edge.tls_write_overflow.load(.monotonic);
    const waits_0 = conn_mod.test_ack_room_waits.load(.monotonic);

    var c: Client = .{ .gpa = gpa, .transport = .tls, .port = server.localAddress(0).getPort(), .n = n, .path = "/spinner" };
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});
    var joined = false;
    defer if (!joined) {
        spin_stop.store(true, .release);
        flush_gate.store(true, .release);
        joinClient(&c, t);
    };
    try waitFor("the handlers to start their responses", n, struct {
        fn f(x: usize) bool {
            return started.load(.acquire) >= x;
        }
    }.f);
    flush_gate.store(true, .release);
    errdefer std.debug.print("T3567-spin timed out: {d} of {d} one-shot barriers done; barrier_waits={d} overflow={d}\n", .{
        spin_done.load(.acquire),                                    n - keepers,
        conn_mod.test_ack_room_waits.load(.monotonic) - waits_0, tls_edge.tls_write_overflow.load(.monotonic) - overflow_0,
    });
    try waitFor("every one-shot barrier to get through while the keepers flush", n - keepers, struct {
        fn f(x: usize) bool {
            return spin_done.load(.acquire) >= x;
        }
    }.f);
    const waits = conn_mod.test_ack_room_waits.load(.monotonic) - waits_0;
    std.debug.print("T3567-spin n={d} keepers={d} barrier_waits={d}\n", .{ n, keepers, waits });
    spin_stop.store(true, .release);
    joinClient(&c, t);
    joined = true;

    if (c.err) |e| return e;
    var ended: usize = 0;
    for (0..n) |i| {
        if (c.ended[i]) ended += 1;
    }
    try std.testing.expectEqual(@as(u64, 0), tls_edge.tls_write_overflow.load(.monotonic) - overflow_0);
    try std.testing.expectEqual(@as(usize, 0), c.resets);
    try std.testing.expect(!(c.closed_early or c.goaway));
    try std.testing.expectEqual(n, ended);
    // The stash really filled: handlers waited for room.
    try std.testing.expect(waits > 0);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runSpinCaseTop(n: usize, keepers: usize) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runSpinCase, .{ rt, gpa, n, keepers });
    try h.join();
}

test "tls: a parked barrier is woken while other handlers keep flushing (the wake is inside the take loop)" {
    try runSpinCaseTop(40, 4);
}

const Outcome = enum { ends, reset };

/// What a row must see. The client checks the status and the body the handler's
/// own operations produce, not any END_STREAM: when `resp.start` is refused,
/// `runHandlerJobBody` sends a fallback 500 ("internal error") and the client sees
/// an END_STREAM too. `ops` is how many operations the handler completed.
const Want = struct { outcome: Outcome, status: u16, body: []const u8, ops: usize };
const want_empty204: Want = .{ .outcome = .ends, .status = 204, .body = "", .ops = 1 };
const want_send_body: Want = .{ .outcome = .ends, .status = 200, .body = "hello", .ops = 1 };
const want_swe: Want = .{ .outcome = .ends, .status = 200, .body = "x", .ops = 3 };
const want_flush: Want = .{ .outcome = .ends, .status = 200, .body = "", .ops = 3 };
const want_abort: Want = .{ .outcome = .reset, .status = 200, .body = "", .ops = 2 };

/// One operation on an otherwise idle connection (see the header). The
/// response must reach the client by the client's deadline, and the handler
/// must have seen what the client saw (`obs_missed`), so the wake came from the
/// operation and not from the handler's exit.
///
/// The empty-body `send` is the member t-3567 found: `sendCb` drains on the
/// handler's task and pushes the HEADERS, then the handler parks on the ticket.
/// On TLS the actor is the writer and sleeps unless something rings it.
fn runIdleOp(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, path: []const u8, want: Want) !void {
    resetGlobals();
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var server = try serverFor(rt, gpa, transport, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var c: Client = .{
        .gpa = gpa,
        .transport = transport,
        .port = server.localAddress(0).getPort(),
        .n = 1,
        .path = path,
        .deadline_ns = 5 * std.time.ns_per_s,
    };
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});
    joinClient(&c, t);
    const got_body = obs_body[0..obs_body_len.load(.acquire)];
    std.debug.print("T3594-idle transport={s} path={s} ended={} reset={} status={d} body={s} ops={d} missed={d} client_err={s}\n", .{
        @tagName(transport), path, c.ended[0], c.reset[0], obs_status.load(.acquire), got_body, ops_done.load(.acquire), obs_missed.load(.acquire), if (c.err) |e| @errorName(e) else "-",
    });
    if (c.err) |e| return e;
    try std.testing.expectEqual(want.ops, ops_done.load(.acquire));
    try std.testing.expectEqual(want.status, obs_status.load(.acquire));
    try std.testing.expectEqualStrings(want.body, got_body);
    switch (want.outcome) {
        .ends => {
            try std.testing.expect(c.ended[0]);
            try std.testing.expectEqual(@as(usize, 0), c.resets);
        },
        .reset => {
            try std.testing.expect(c.reset[0]);
            try std.testing.expect(!c.ended[0]);
        },
    }
    try std.testing.expectEqual(@as(usize, 0), obs_missed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), park_missed.load(.acquire));

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runIdleOpTop(transport: Transport, path: []const u8, want: Want) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runIdleOp, .{ rt, gpa, transport, path, want });
    try h.join();
}

test "idle table, tls: a task handler's empty-body send gets its response" {
    try runIdleOpTop(.tls, "/empty204", want_empty204);
}

test "idle table, h2c: a task handler's empty-body send gets its response" {
    try runIdleOpTop(.h2c, "/empty204", want_empty204);
}

test "idle table, tls: a task handler's send with a body gets its response" {
    try runIdleOpTop(.tls, "/idle-send-body", want_send_body);
}

test "idle table, h2c: a task handler's send with a body gets its response" {
    try runIdleOpTop(.h2c, "/idle-send-body", want_send_body);
}

test "idle table, tls: start, write and end each reach the client" {
    try runIdleOpTop(.tls, "/idle-swe", want_swe);
}

test "idle table, h2c: start, write and end each reach the client" {
    try runIdleOpTop(.h2c, "/idle-swe", want_swe);
}

test "idle table, tls: a bare flush on an idle connection completes" {
    try runIdleOpTop(.tls, "/idle-flush", want_flush);
}

test "idle table, h2c: a bare flush on an idle connection completes" {
    try runIdleOpTop(.h2c, "/idle-flush", want_flush);
}

test "idle table, tls: an abort reaches the client as a reset" {
    try runIdleOpTop(.tls, "/idle-abort", want_abort);
}

test "idle table, h2c: an abort reaches the client as a reset" {
    try runIdleOpTop(.h2c, "/idle-abort", want_abort);
}

/// The control pool for the failure regression. The ordinary share is this
/// minus the reserve for terminal frames (4 KiB): 18 KiB, which holds the
/// 16 KiB HEADERS fragment of the wide block and not the CONTINUATION after it.
const fail_control_bytes = 22 * 1024;

/// The third member of the class (t-3594). A response that is refused while it
/// is materialized, for lack of room for a CONTINUATION, fails the connection:
/// `handleWriterFailed` runs on the HANDLER's task, under `session_mu`, and sets
/// every stream's cause. The handler catches the error and stays alive, so
/// nothing posts a completion. The actor has to learn of the failure from the
/// handler's release of the lock, and reach its teardown while the handler is
/// still alive. `test_observed_writer_fail_handled` cannot be the oracle: the
/// handler's own task sets it.
fn runFailedAndAlive(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    resetGlobals();
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var limits = starh2.Limits.defaults;
    limits.control_bytes_per_connection = fail_control_bytes;
    var server = try serverForLimits(rt, gpa, transport, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        fail_gate.store(true, .release);
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    const teardowns_0 = conn_mod.test_actor_teardowns.load(.acquire);
    const term_full_0 = conn_mod.test_ctrl_terminal_full.load(.monotonic);
    var c: Client = .{
        .gpa = gpa,
        .transport = transport,
        .port = server.localAddress(0).getPort(),
        .n = 1,
        .path = "/refused",
        .deadline_ns = 15 * std.time.ns_per_s,
    };
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});

    // The handler has caught the refusal and is waiting on the gate.
    const caught_ok = blk: {
        const deadline = nowNs() + client_deadline_ns;
        while (fail_caught.load(.acquire) == 0) {
            if (nowNs() >= deadline) break :blk false;
            zio.sleep(.fromMilliseconds(2)) catch {};
        }
        break :blk true;
    };
    // The actor reaches its teardown by a deadline, with the handler alive.
    const reached = blk: {
        const deadline = nowNs() + 5 * std.time.ns_per_s;
        while (conn_mod.test_actor_teardowns.load(.acquire) == teardowns_0) {
            if (nowNs() >= deadline) break :blk false;
            zio.sleep(.fromMilliseconds(2)) catch {};
        }
        break :blk true;
    };
    const alive = fail_alive.load(.acquire);
    const caught = fail_caught.load(.acquire);
    // Now let the handler go, so the connection can finish and the client end.
    fail_gate.store(true, .release);
    joinClient(&c, t);
    std.debug.print("T3594-fail transport={s} handler_caught={} err={d} actor_teardown={} handler_alive_then={} terminal_full={d} closed_early={}\n", .{
        @tagName(transport),                                             caught_ok,      caught, reached, alive,
        conn_mod.test_ctrl_terminal_full.load(.monotonic) - term_full_0, c.closed_early,
    });
    try std.testing.expect(caught_ok);
    // The instrument ran: the refusal was the fragment kind, not the terminal kind.
    try std.testing.expect(caught != 0);
    try std.testing.expectEqual(term_full_0, conn_mod.test_ctrl_terminal_full.load(.monotonic));
    try std.testing.expect(reached);
    try std.testing.expect(alive);
    try std.testing.expectEqual(@as(usize, 0), park_missed.load(.acquire));

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runFailedAndAliveTop(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runFailedAndAlive, .{ rt, gpa, transport });
    try h.join();
}

test "failure regression, tls: a refused CONTINUATION fails the connection and the actor tears down while the handler is alive" {
    try runFailedAndAliveTop(.tls);
}

// A CONTROL, not a guard of the class. On h2c `handleWriterFailed` closes
// `write_ch`; the write pump posts `fail_all` and the actor wakes by `.acks`,
// ring or not. This case passes at 58b88c9 too. Do not count it as a guard.
test "failure regression CONTROL, h2c (does not guard the class): a refused CONTINUATION fails the connection and the actor tears down while the handler is alive" {
    try runFailedAndAliveTop(.h2c);
}

fn runCaseTop(transport: Transport, hold: Hold, n: usize) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runCase, .{ rt, gpa, transport, hold, n });
    try h.join();
}

test "h2c control: 60 bare flush barriers against a held writer wait for room and every response ends" {
    try runCaseTop(.h2c, .writes, 60);
}

test "tls: 60 bare flush barriers against a held writer (test_hold_writes) do not overflow the stash" {
    try runCaseTop(.tls, .writes, 60);
}

test "tls: 60 bare flush barriers against a held stash (test_hold_stash) do not overflow the stash" {
    try runCaseTop(.tls, .stash, 60);
}

test "tls: 20 bare flush barriers against a held writer (test_hold_writes) do not overflow the stash" {
    try runCaseTop(.tls, .writes, 20);
}

test "tls: 20 bare flush barriers against a held stash (test_hold_stash) do not overflow the stash" {
    try runCaseTop(.tls, .stash, 20);
}

test "h2c control: 20 bare flush barriers against a held writer wait for room and every response ends" {
    try runCaseTop(.h2c, .writes, 20);
}
