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
    try zio.sleep(.fromMilliseconds(150));
    try resp.send(204, &.{}, "");
}

/// The request that puts a chunk in front of the hold.
fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/flusher", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = flusher } } },
    .{ .method = .GET, .path = "/spinner", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = spinner } } },
    .{ .method = .GET, .path = "/empty204", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = empty204 } } },
    .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
};

fn newRuntime(gpa: std.mem.Allocator) !*zio.Runtime {
    return zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
}

fn serverFor(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, cert: *[]u8, key: *[]u8) !starh2.Server {
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
        .limits = starh2.Limits.defaults,
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

/// A task handler answers with an empty body on an otherwise idle connection.
/// `sendCb` drains on the handler's task and pushes the HEADERS, then the
/// handler parks on the ticket. On TLS the actor is the writer and sleeps
/// unless something rings it; nothing else is going on to wake it (a GET arms
/// no timer). The response must arrive, by a deadline.
fn runIdleSend(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
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
        .path = "/empty204",
        .deadline_ns = 5 * std.time.ns_per_s,
    };
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});
    joinClient(&c, t);
    std.debug.print("T3567-idle transport={s} ended={} client_err={s}\n", .{ @tagName(transport), c.ended[0], if (c.err) |e| @errorName(e) else "-" });
    if (c.err) |e| return e;
    try std.testing.expect(c.ended[0]);
    try std.testing.expectEqual(@as(usize, 0), c.resets);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runIdleSendTop(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runIdleSend, .{ rt, gpa, transport });
    try h.join();
}

test "tls: a task handler's empty-body send on an idle connection gets its response" {
    try runIdleSendTop(.tls);
}

test "h2c control: a task handler's empty-body send on an idle connection gets its response" {
    try runIdleSendTop(.h2c);
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
