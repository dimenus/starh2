//! Gates for the connection outbound budget (t-3538): a full budget PARKS a
//! handler's write, in arrival order, and the write resumes when a write ack
//! returns room. It does not reset the stream.
//!
//! Every scenario holds the write pump (`wire_pump.test_hold_writes`, on h2c
//! and on TLS), so the bytes already written stay in the budget however large
//! the kernel's socket buffers are. A test that needs a full budget cannot
//! depend on that size. The streams are started BEFORE the hold, because a
//! response waits for its HEADERS to be written before its body can flow.
//!
//! - Park and resume: three 64 KiB writes, a budget of two streams. The third
//!   parks, the peer sees no RST_STREAM, every body arrives whole, and the
//!   budget is back to zero. (h2c and TLS)
//! - Order and no barging: writers that need more than the room wait in
//!   arrival order, and a small writer that would fit waits behind them
//!   instead of taking the room. (h2c and TLS)
//! - Exits: a parked writer whose stream the peer resets, and one whose
//!   connection ends, leave the queue and return what they held. (h2c: the TLS
//!   hold stops the connection's actor, which these exits need.)
//! - The server cap: a write the SERVER's byte cap refuses resets its stream;
//!   it is not parked and not a hang. (h2c and TLS)
//! - An inline (complete) handler waits in the SAME queue, behind the task
//!   writers that arrived before it, and runs when it is served, with no reset.
//!   (h2c)
//! - The accounting transition on TLS: a write is counted once while its chunk
//!   is in the stash and again in the driver's batch (never twice), and the
//!   books are zero after the ack.
//! - Streams with no body at all (an empty END_STREAM DATA frame, no payload)
//!   complete through a write pool of three chunks. (h2c and TLS)
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const wire_client = @import("wire_client.zig");

const conn_mod = starh2.edge.connection;
const wire_pump = starh2.edge.wire_pump;
const om = conn_mod.outbound_memory;

const Transport = wire_client.Transport;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;
const client_deadline_ns = wire_client.client_deadline_ns;

const dummy: u8 = 0;
const max_streams = 5;
const stream_cap = 64 * 1024;

/// The body every stream sends: no two neighbouring KiB alike, so a lost,
/// repeated or reordered piece shows up in the compare.
const pattern: [stream_cap]u8 = blk: {
    @setEvalBranchQuota(100_000);
    var p: [stream_cap]u8 = undefined;
    for (&p, 0..) |*b, i| b.* = @intCast((i / 1024) * 37 % 251 + i % 7);
    break :blk p;
};

// ---------------------------------------------------------------------------
// The handlers: start the response (so its HEADERS are on the wire before the
// pump is held), wait for this handler's gate, write `sizes[idx]` bytes.

var sizes: [max_streams]usize = @splat(0);
var gates: [max_streams]std.atomic.Value(bool) = @splat(.init(false));
var started: std.atomic.Value(usize) = .init(0);
/// Set when handler `idx` has returned; `ended_ok[idx]` says whether cleanly.
var finished: [max_streams]std.atomic.Value(bool) = @splat(.init(false));
var ended_ok: [max_streams]std.atomic.Value(bool) = @splat(.init(false));

fn resetGlobals(sz: [max_streams]usize) void {
    sizes = sz;
    started.store(0, .release);
    for (&gates) |*g| g.store(false, .release);
    for (&finished) |*f| f.store(false, .release);
    for (&ended_ok) |*f| f.store(false, .release);
}

fn openAllGates() void {
    for (&gates) |*g| g.store(true, .release);
}

fn waveHandler(comptime idx: usize) *const fn (*anyopaque, *const starh2.Request, *starh2.Response) anyerror!void {
    return struct {
        fn run(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
            defer finished[idx].store(true, .release);
            var body = try resp.start(200, &.{});
            _ = started.fetchAdd(1, .acq_rel);
            while (!gates[idx].load(.acquire)) try zio.sleep(.fromMilliseconds(2));
            try body.writeAll(pattern[0..sizes[idx]]);
            try body.finish();
            ended_ok[idx].store(true, .release);
        }
    }.run;
}

fn inlineHello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "inline");
}

/// Starts and ends at once: an empty END_STREAM DATA frame, no payload.
fn emptyBody(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    try body.finish();
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/s0", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = waveHandler(0) } } },
    .{ .method = .GET, .path = "/s1", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = waveHandler(1) } } },
    .{ .method = .GET, .path = "/s2", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = waveHandler(2) } } },
    .{ .method = .GET, .path = "/s3", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = waveHandler(3) } } },
    .{ .method = .GET, .path = "/s4", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = waveHandler(4) } } },
    .{ .method = .GET, .path = "/inline", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = inlineHello } } },
    .{ .method = .GET, .path = "/empty", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = emptyBody } } },
};

const wave_paths = [max_streams][]const u8{ "/s0", "/s1", "/s2", "/s3", "/s4" };

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

/// The write pool at its smallest, a stream cap of 64 KiB, and the connection
/// budget at `budget`. The boot check (`Limits`) wants the budget to hold the
/// whole write pool and one stream's cap, so two streams' worth (128 KiB) is
/// the smallest budget that has any room beside a stream: the case the first
/// scenario runs.
fn limitsFor(budget: usize, server_budget: usize) starh2.Limits {
    var limits = starh2.Limits.defaults;
    limits.inbound_wire_chunks_per_connection = starh2.core.limits.H2C_MIN_WRITE_QUEUE_CHUNKS;
    limits.outbound_bytes_per_stream = stream_cap;
    limits.outbound_bytes_per_connection = budget;
    limits.outbound_bytes_per_server = server_budget;
    return limits;
}

// ---------------------------------------------------------------------------
// The client: one connection on its own thread. It sends its requests, reads
// every frame in wire order, checks the bodies, and between reads looks at a
// mailbox the test writes (a RST_STREAM to send, a request to send, a hang up).

const Cmd = enum(u8) { none, send_inline, reset_stream_2, hang_up };

const inline_sid: u31 = 11;

const Client = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    /// Handler index of each stream this connection requests, in order; stream
    /// ids are 1, 3, 5, ...
    handlers: [max_streams]u8 = undefined,
    n: usize = 0,
    /// Streams (by position) that must end or be reset before the reader is done.
    expect_end: u8 = 0,
    expect_inline: bool = false,
    cmd: std.atomic.Value(u8) = .init(0),
    requested: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    got: [max_streams]usize = @splat(0),
    ended: [max_streams]bool = @splat(false),
    reset_stream: [max_streams]bool = @splat(false),
    wrong_bytes: usize = 0,
    resets: usize = 0,
    goaways: usize = 0,
    inline_ended: bool = false,

    fn sid(pos: usize) u31 {
        return @intCast(1 + 2 * pos);
    }
};

fn clientMain(c: *Client) void {
    clientRun(c) catch |err| {
        std.debug.print("{s} budget client failed: {s}\n", .{ @tagName(c.transport), @errorName(err) });
        c.err = err;
    };
    c.done.store(true, .release);
}

fn clientRun(c: *Client) !void {
    const w = try Wire.open(c.gpa, c.transport, c.port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(c.gpa);
    defer req.deinit(c.gpa);
    // The connection window is the default 65,535: open it, so the budget is
    // the only thing that stops a writer.
    try h2c.appendWindowUpdate(c.gpa, &req, 0, 1 << 30);
    for (0..c.n) |pos| try h2c.appendHeaders(c.gpa, &req, Client.sid(pos), wave_paths[c.handlers[pos]], true);
    try w.send(req.items);
    c.requested.store(true, .release);

    var want = c.expect_end;
    const deadline = nowNs() + 3 * client_deadline_ns;
    while (want != 0 or (c.expect_inline and !c.inline_ended)) {
        switch (@as(Cmd, @enumFromInt(c.cmd.swap(0, .acq_rel)))) {
            .none => {},
            .send_inline => {
                var one: std.ArrayList(u8) = .empty;
                defer one.deinit(c.gpa);
                try h2c.appendHeaders(c.gpa, &one, inline_sid, "/inline", true);
                try w.send(one.items);
            },
            .reset_stream_2 => {
                var one: std.ArrayList(u8) = .empty;
                defer one.deinit(c.gpa);
                try h2c.appendRst(c.gpa, &one, Client.sid(2));
                try w.send(one.items);
            },
            .hang_up => {
                _ = std.c.shutdown(w.fd, 2);
                return;
            },
        }
        // A short look at the wire, then the mailbox again.
        const s = w.next(nowNs() + 25 * std.time.ns_per_ms) catch |err| switch (err) {
            error.Deadline => {
                if (nowNs() >= deadline) return error.Deadline;
                continue;
            },
            else => return err,
        } orelse return;
        switch (s.ftype) {
            0 => if (s.sid % 2 == 1 and s.sid != inline_sid) {
                const pos = (s.sid - 1) / 2;
                if (pos < c.n) {
                    const data = w.last_payload.items;
                    const at = c.got[pos];
                    if (at + data.len > pattern.len or !std.mem.eql(u8, data, pattern[at..][0..data.len])) c.wrong_bytes += data.len;
                    c.got[pos] += data.len;
                    if (s.flags & 1 != 0) {
                        c.ended[pos] = true;
                        want &= ~(@as(u8, 1) << @intCast(pos));
                    }
                }
            } else if (s.sid == inline_sid and s.flags & 1 != 0) {
                c.inline_ended = true;
            },
            1 => if (s.sid == inline_sid and s.flags & 1 != 0) {
                c.inline_ended = true;
            },
            3 => {
                c.resets += 1;
                const pos = (s.sid - 1) / 2;
                if (s.sid % 2 == 1 and pos < c.n) {
                    c.reset_stream[pos] = true;
                    want &= ~(@as(u8, 1) << @intCast(pos));
                }
            },
            7 => {
                c.goaways += 1;
                return;
            },
            else => {},
        }
    }
}

fn waitFor(comptime what: []const u8, ctx: anytype, comptime pred: fn (@TypeOf(ctx)) bool) !void {
    const deadline = nowNs() + client_deadline_ns;
    while (!pred(ctx)) {
        if (nowNs() >= deadline) {
            std.debug.print("budget test: timed out waiting for {s}\n", .{what});
            return error.Deadline;
        }
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
}

fn joinClient(c: *Client, t: std.Thread) !void {
    const deadline = nowNs() + 3 * client_deadline_ns;
    while (!c.done.load(.acquire)) {
        if (nowNs() >= deadline) {
            t.join();
            return error.Deadline;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    t.join();
    if (c.err) |err| return err;
}

fn reserved(server: *const starh2.Server) usize {
    return server.accounting.outbound_bytes.load(.acquire);
}

fn waitReserved(server: *const starh2.Server, at_least: usize) !void {
    const Ctx = struct { s: *const starh2.Server, n: usize };
    try waitFor("the writes to reserve budget", Ctx{ .s = server, .n = at_least }, struct {
        fn f(x: Ctx) bool {
            return reserved(x.s) >= x.n;
        }
    }.f);
}

fn waitWaits(n: usize) !void {
    try waitFor("a writer to park for budget", n, struct {
        fn f(x: usize) bool {
            return conn_mod.test_budget_waits.load(.monotonic) >= x;
        }
    }.f);
}

fn waitStarted(n: usize) !void {
    try waitFor("the handlers to start their responses", n, struct {
        fn f(x: usize) bool {
            return started.load(.acquire) >= x;
        }
    }.f);
}

fn waitReservedZero(server: *const starh2.Server) !void {
    try waitFor("the budget to return to zero", server, struct {
        fn f(s: *const starh2.Server) bool {
            return reserved(s) == 0;
        }
    }.f);
}

// ---------------------------------------------------------------------------
// Park and resume; order and no barging. One scenario, two sets of numbers.

const Spec = struct {
    n: usize,
    sizes: [max_streams]usize,
    budget: usize,
    /// Streams, in order, whose writes fit the budget at once.
    fits: usize,
    /// Budget bytes those streams hold while the pump is held.
    held: usize,
    /// Stream ids of the writers that park, in the order they must be served.
    order: []const u31,
};

fn runScenario(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, spec: Spec) !void {
    resetGlobals(spec.sizes);
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var server = try serverFor(rt, gpa, transport, limitsFor(spec.budget, starh2.Limits.defaults.outbound_bytes_per_server), &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    defer openAllGates();
    defer wire_pump.test_hold_writes.store(false, .release);

    const waits_0 = conn_mod.test_budget_waits.load(.monotonic);
    const grants_0 = conn_mod.test_budget_grants.load(.monotonic);
    const held_0 = wire_pump.test_hold_entered.load(.acquire);
    const full_0 = conn_mod.test_ctrl_terminal_full.load(.monotonic);

    var c: Client = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort(), .n = spec.n };
    for (0..spec.n) |i| c.handlers[i] = @intCast(i);
    c.expect_end = @intCast((@as(u16, 1) << @intCast(spec.n)) - 1);
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});

    // Every response is started and its HEADERS are out, then the pump stops.
    try waitStarted(spec.n);
    wire_pump.test_hold_writes.store(true, .release);

    // The writes go one at a time, in stream order. The first `fits` take
    // budget; each later one must park, and must not take room that is left.
    var held_expected: usize = 0;
    for (0..spec.n) |k| {
        gates[k].store(true, .release);
        if (k < spec.fits) {
            held_expected += spec.sizes[k];
            try waitReserved(&server, held_expected);
            if (k == 0) {
                // The pump says it is holding chunks, so the bytes written
                // so far will not leave. (The flag alone is not that.)
                const Ctx = struct { n: u64 };
                try waitFor("the pump to hold", Ctx{ .n = held_0 }, struct {
                    fn f(x: Ctx) bool {
                        return wire_pump.test_hold_entered.load(.acquire) != x.n;
                    }
                }.f);
            }
        } else {
            try waitWaits(waits_0 + (k - spec.fits + 1));
        }
    }
    // A parked writer holds nothing, and a small one did not take the room
    // that was left: the budget holds only what fitted.
    zio.sleep(.fromMilliseconds(100)) catch {};
    try std.testing.expectEqual(spec.held, reserved(&server));
    try std.testing.expectEqual(waits_0 + (spec.n - spec.fits), conn_mod.test_budget_waits.load(.monotonic));
    try std.testing.expectEqual(grants_0, conn_mod.test_budget_grants.load(.monotonic));

    // The pump runs: acks return room, and the queue serves its writers.
    wire_pump.test_hold_writes.store(false, .release);
    try joinClient(&c, t);

    try std.testing.expectEqual(@as(usize, 0), c.resets);
    try std.testing.expectEqual(@as(usize, 0), c.goaways);
    try std.testing.expectEqual(@as(usize, 0), c.wrong_bytes);
    // The terminal control queue (32 entries) did not overflow.
    try std.testing.expectEqual(full_0, conn_mod.test_ctrl_terminal_full.load(.monotonic));
    for (0..spec.n) |i| {
        try std.testing.expect(c.ended[i]);
        try std.testing.expectEqual(spec.sizes[i], c.got[i]);
        try std.testing.expect(ended_ok[i].load(.acquire));
    }
    // The writers that parked were served, in arrival order.
    const grants = conn_mod.test_budget_grants.load(.monotonic) - grants_0;
    try std.testing.expectEqual(spec.order.len, grants);
    for (spec.order, 0..) |sid, k| {
        try std.testing.expectEqual(@as(u32, sid), conn_mod.test_budget_grant_log[(grants_0 + k) % conn_mod.test_budget_grant_log.len].load(.monotonic));
    }
    try waitReservedZero(&server);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runScenarioCase(transport: Transport, spec: Spec) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runScenario, .{ rt, gpa, transport, spec });
    try h.join();
}

const park_spec: Spec = .{
    .n = 3,
    .sizes = .{ 64 * 1024, 64 * 1024, 64 * 1024, 0, 0 },
    .budget = 2 * stream_cap,
    .fits = 2,
    .held = 2 * stream_cap,
    .order = &.{5},
};

// Budget 160 KiB. Two 64 KiB writers take 128 KiB and leave 32. The next
// writer needs 48 KiB: it parks. The one after it needs 16 KiB, which fits in
// the 32 KiB that is left, and must STILL wait behind the first. Then a third
// parks. They are served 5, 7, 9.
const order_spec: Spec = .{
    .n = 5,
    .sizes = .{ 64 * 1024, 64 * 1024, 48 * 1024, 16 * 1024, 24 * 1024 },
    .budget = 160 * 1024,
    .fits = 2,
    .held = 2 * stream_cap,
    .order = &.{ 5, 7, 9 },
};

test "park and resume: the third 64 KiB write waits for room, no stream is reset, every body arrives (h2c)" {
    try runScenarioCase(.h2c, park_spec);
}

test "park and resume: the third 64 KiB write waits for room, no stream is reset, every body arrives (tls)" {
    try runScenarioCase(.tls, park_spec);
}

test "order: parked writers are served in arrival order and a small writer does not take the room ahead of them (h2c)" {
    try runScenarioCase(.h2c, order_spec);
}

test "order: parked writers are served in arrival order and a small writer does not take the room ahead of them (tls)" {
    try runScenarioCase(.tls, order_spec);
}

// ---------------------------------------------------------------------------
// Exits.

const Exit = enum { peer_reset, hang_up };

fn runExit(rt: *zio.Runtime, gpa: std.mem.Allocator, exit: Exit) !void {
    resetGlobals(.{ 64 * 1024, 64 * 1024, 64 * 1024, 0, 0 });
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var server = try serverFor(rt, gpa, .h2c, limitsFor(2 * stream_cap, starh2.Limits.defaults.outbound_bytes_per_server), &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    defer openAllGates();
    defer wire_pump.test_hold_writes.store(false, .release);

    const waits_0 = conn_mod.test_budget_waits.load(.monotonic);
    const grants_0 = conn_mod.test_budget_grants.load(.monotonic);
    const full_0 = conn_mod.test_ctrl_terminal_full.load(.monotonic);

    var c: Client = .{ .gpa = gpa, .transport = .h2c, .port = server.localAddress(0).getPort(), .n = 3 };
    for (0..3) |i| c.handlers[i] = @intCast(i);
    // The reset stream is the peer's own doing: the server sends nothing for it.
    c.expect_end = if (exit == .peer_reset) 0b011 else 0b111;
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});

    try waitStarted(3);
    wire_pump.test_hold_writes.store(true, .release);
    gates[0].store(true, .release);
    gates[1].store(true, .release);
    try waitReserved(&server, 2 * stream_cap);
    gates[2].store(true, .release);
    try waitWaits(waits_0 + 1);

    switch (exit) {
        .peer_reset => {
            // The peer gives up on the parked stream. Its writer must leave
            // the queue and the handler return, with nothing reserved for it.
            c.cmd.store(@intFromEnum(Cmd.reset_stream_2), .release);
            try waitFor("the parked handler to return", {}, struct {
                fn f(_: void) bool {
                    return finished[2].load(.acquire);
                }
            }.f);
            try std.testing.expect(!ended_ok[2].load(.acquire));
            try std.testing.expectEqual(2 * stream_cap, reserved(&server));
            wire_pump.test_hold_writes.store(false, .release);
            try joinClient(&c, t);
            // The other two finished; nothing was granted to the one that left.
            try std.testing.expect(c.ended[0] and c.ended[1]);
            try std.testing.expectEqual(grants_0, conn_mod.test_budget_grants.load(.monotonic));
        },
        .hang_up => {
            // The connection ends while the writer waits. Every handler is
            // stopped, and the budget they held goes back with the connection.
            c.cmd.store(@intFromEnum(Cmd.hang_up), .release);
            try joinClient(&c, t);
            const Ctx = struct { s: *const starh2.Server };
            try waitFor("the connection to end", Ctx{ .s = &server }, struct {
                fn f(x: Ctx) bool {
                    return x.s.active_connections.load(.acquire) == 0;
                }
            }.f);
            try std.testing.expect(finished[0].load(.acquire) and finished[1].load(.acquire) and finished[2].load(.acquire));
            try std.testing.expect(!ended_ok[2].load(.acquire));
        },
    }
    try std.testing.expectEqual(full_0, conn_mod.test_ctrl_terminal_full.load(.monotonic));
    try waitReservedZero(&server);
    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runExitCase(exit: Exit) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runExit, .{ rt, gpa, exit });
    try h.join();
}

test "exit: a parked writer whose stream the peer resets leaves the queue and holds nothing (h2c)" {
    try runExitCase(.peer_reset);
}

test "exit: a parked writer whose connection ends leaves the queue and the budget goes back (h2c)" {
    try runExitCase(.hang_up);
}

// ---------------------------------------------------------------------------
// The server cap.

/// Two connections; the server may hold what ONE connection's budget allows.
/// The first connection fills it. The second connection's writer is refused by
/// the server cap: its stream is reset (after the pump runs again), it is not
/// parked and not left hanging.
fn runServerCap(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    resetGlobals(.{ 64 * 1024, 64 * 1024, 64 * 1024, 0, 0 });
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    const budget = 2 * stream_cap;
    var server = try serverFor(rt, gpa, transport, limitsFor(budget, budget), &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    defer openAllGates();
    defer wire_pump.test_hold_writes.store(false, .release);

    const refusals_0 = om.server_cap_refusals.load(.monotonic);
    const waits_0 = conn_mod.test_budget_waits.load(.monotonic);
    const full_0 = conn_mod.test_ctrl_terminal_full.load(.monotonic);

    var first: Client = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort(), .n = 2 };
    first.handlers[0] = 0;
    first.handlers[1] = 1;
    first.expect_end = 0b11;
    var second: Client = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort(), .n = 1 };
    second.handlers[0] = 2;
    second.expect_end = 0b1;
    const t1 = try std.Thread.spawn(.{}, clientMain, .{&first});
    const t2 = try std.Thread.spawn(.{}, clientMain, .{&second});

    try waitStarted(3);
    wire_pump.test_hold_writes.store(true, .release);
    gates[0].store(true, .release);
    gates[1].store(true, .release);
    try waitReserved(&server, budget);
    gates[2].store(true, .release);
    const Ctx = struct { n: u64 };
    try waitFor("the server cap to refuse the second connection's write", Ctx{ .n = refusals_0 }, struct {
        fn f(x: Ctx) bool {
            return om.server_cap_refusals.load(.monotonic) > x.n;
        }
    }.f);
    // Refused, not parked.
    try std.testing.expectEqual(waits_0, conn_mod.test_budget_waits.load(.monotonic));
    try std.testing.expectEqual(budget, reserved(&server));

    wire_pump.test_hold_writes.store(false, .release);
    try joinClient(&first, t1);
    try joinClient(&second, t2);
    try std.testing.expect(first.ended[0] and first.ended[1]);
    try std.testing.expectEqual(@as(usize, 0), first.resets);
    try std.testing.expect(second.reset_stream[0]);
    try std.testing.expectEqual(@as(usize, 0), second.got[0]);
    try std.testing.expect(!ended_ok[2].load(.acquire));
    try std.testing.expectEqual(full_0, conn_mod.test_ctrl_terminal_full.load(.monotonic));
    try waitReservedZero(&server);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runServerCapCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runServerCap, .{ rt, gpa, transport });
    try h.join();
}

test "server cap: a write the server's byte cap refuses resets its stream, it is not parked (h2c)" {
    try runServerCapCase(.h2c);
}

test "server cap: a write the server's byte cap refuses resets its stream, it is not parked (tls)" {
    try runServerCapCase(.tls);
}

// ---------------------------------------------------------------------------
// An inline (complete) handler with the budget full.

fn runInline(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetGlobals(.{ 64 * 1024, 64 * 1024, 64 * 1024, 0, 0 });
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var server = try serverFor(rt, gpa, .h2c, limitsFor(2 * stream_cap, starh2.Limits.defaults.outbound_bytes_per_server), &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    defer openAllGates();
    defer wire_pump.test_hold_writes.store(false, .release);

    const gated_0 = conn_mod.test_inline_budget_gated.load(.monotonic);
    const waits_0 = conn_mod.test_budget_waits.load(.monotonic);
    const grants_0 = conn_mod.test_budget_grants.load(.monotonic);
    const full_0 = conn_mod.test_ctrl_terminal_full.load(.monotonic);
    var c: Client = .{ .gpa = gpa, .transport = .h2c, .port = server.localAddress(0).getPort(), .n = 3 };
    c.handlers[0] = 0;
    c.handlers[1] = 1;
    c.handlers[2] = 2;
    c.expect_end = 0b111;
    c.expect_inline = true;
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});

    try waitStarted(3);
    wire_pump.test_hold_writes.store(true, .release);
    gates[0].store(true, .release);
    gates[1].store(true, .release);
    try waitReserved(&server, 2 * stream_cap);
    // A task writer is already waiting, so the queue is not empty.
    gates[2].store(true, .release);
    try waitWaits(waits_0 + 1);

    // The budget is full. A complete handler takes its place BEHIND the
    // waiting writer: the actor does not run it, and does not reset it.
    c.cmd.store(@intFromEnum(Cmd.send_inline), .release);
    if (comptime conn_mod.test_observe) {
        const Ctx = struct { n: usize };
        try waitFor("the inline gate to hold the job back", Ctx{ .n = gated_0 }, struct {
            fn f(x: Ctx) bool {
                return conn_mod.test_inline_budget_gated.load(.monotonic) > x.n;
            }
        }.f);
    }
    zio.sleep(.fromMilliseconds(100)) catch {};
    try std.testing.expect(!c.inline_ended);
    try std.testing.expectEqual(@as(usize, 2 * stream_cap), reserved(&server));

    // Room comes back with the first acks; the job runs.
    wire_pump.test_hold_writes.store(false, .release);
    try joinClient(&c, t);
    try std.testing.expect(c.inline_ended);
    try std.testing.expectEqual(@as(usize, 0), c.resets);
    try std.testing.expect(c.ended[0] and c.ended[1] and c.ended[2]);
    // Served in arrival order: the task writer (stream 5), then the job.
    try std.testing.expectEqual(grants_0 + 2, conn_mod.test_budget_grants.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 5), conn_mod.test_budget_grant_log[grants_0 % conn_mod.test_budget_grant_log.len].load(.monotonic));
    try std.testing.expectEqual(@as(u32, inline_sid), conn_mod.test_budget_grant_log[(grants_0 + 1) % conn_mod.test_budget_grant_log.len].load(.monotonic));
    try std.testing.expectEqual(full_0, conn_mod.test_ctrl_terminal_full.load(.monotonic));
    try waitReservedZero(&server);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

test "inline gate: with the budget full a complete handler waits on the actor's queue and runs when room returns, with no reset (h2c)" {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runInline, .{ rt, gpa });
    try h.join();
}

// ---------------------------------------------------------------------------
// The accounting transition on TLS: one write, counted once wherever its
// chunk is.

/// One 64 KiB write. Its chunks wait first in the TLS stash (a drain pushed
/// them, the driver has not taken them: `test_hold_stash`), then in the
/// driver's batch (taken, none written: `test_hold_writes`). The server's byte
/// counter must read exactly the payload in both places, and zero after the
/// writes complete. The old rule read the payload plus the frames' bytes once
/// the chunks were handed over.
fn runTlsTransition(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    resetGlobals(.{ 64 * 1024, 0, 0, 0, 0 });
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var server = try serverFor(rt, gpa, .tls, limitsFor(2 * stream_cap, starh2.Limits.defaults.outbound_bytes_per_server), &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    defer openAllGates();
    defer conn_mod.test_hold_stash.store(false, .release);
    defer wire_pump.test_hold_writes.store(false, .release);

    const stash_0 = conn_mod.test_hold_stash_entered.load(.acquire);
    const batch_0 = wire_pump.test_hold_entered.load(.acquire);
    const pushed_0 = om.chunks_to_pump.load(.monotonic);
    const full_0 = conn_mod.test_ctrl_terminal_full.load(.monotonic);

    var c: Client = .{ .gpa = gpa, .transport = .tls, .port = server.localAddress(0).getPort(), .n = 1 };
    c.handlers[0] = 0;
    c.expect_end = 0b1;
    const t = try std.Thread.spawn(.{}, clientMain, .{&c});

    try waitStarted(1);
    // Stage 1: the chunks are pushed and sit in the stash.
    conn_mod.test_hold_stash.store(true, .release);
    gates[0].store(true, .release);
    try waitFor("the driver to stop with chunks in the stash", stash_0, struct {
        fn f(x: u64) bool {
            return conn_mod.test_hold_stash_entered.load(.acquire) != x;
        }
    }.f);
    try waitReserved(&server, 64 * 1024);
    zio.sleep(.fromMilliseconds(50)) catch {};
    try std.testing.expectEqual(@as(usize, 64 * 1024), reserved(&server));
    try std.testing.expect(om.chunks_to_pump.load(.monotonic) > pushed_0);

    // Stage 2: the driver takes them and holds the batch.
    wire_pump.test_hold_writes.store(true, .release);
    conn_mod.test_hold_stash.store(false, .release);
    try waitFor("the driver to hold its batch", batch_0, struct {
        fn f(x: u64) bool {
            return wire_pump.test_hold_entered.load(.acquire) != x;
        }
    }.f);
    zio.sleep(.fromMilliseconds(50)) catch {};
    try std.testing.expectEqual(@as(usize, 64 * 1024), reserved(&server));

    // Stage 3: written and acked.
    wire_pump.test_hold_writes.store(false, .release);
    try joinClient(&c, t);
    try std.testing.expect(c.ended[0]);
    try std.testing.expectEqual(@as(usize, 64 * 1024), c.got[0]);
    try std.testing.expectEqual(@as(usize, 0), c.wrong_bytes);
    try std.testing.expectEqual(full_0, conn_mod.test_ctrl_terminal_full.load(.monotonic));
    try waitReservedZero(&server);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

test "accounting transition (tls): a write is counted once while its chunk is in the stash, then in the driver's batch, and zero after the ack" {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runTlsTransition, .{ rt, gpa });
    try h.join();
}

// ---------------------------------------------------------------------------
// Streams with no payload at all.

const empty_streams = 120;

const Empties = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    ended: usize = 0,
    resets: usize = 0,
    data_bytes: usize = 0,
};

fn emptiesMain(e: *Empties) void {
    emptiesRun(e) catch |err| {
        std.debug.print("{s} empty-body client failed: {s}\n", .{ @tagName(e.transport), @errorName(err) });
        e.err = err;
    };
    e.done.store(true, .release);
}

fn emptiesRun(e: *Empties) !void {
    const w = try Wire.open(e.gpa, e.transport, e.port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(e.gpa);
    defer req.deinit(e.gpa);
    for (0..empty_streams) |i| try h2c.appendHeaders(e.gpa, &req, @intCast(1 + 2 * i), "/empty", true);
    try w.send(req.items);
    const deadline = nowNs() + 3 * client_deadline_ns;
    while (e.ended < empty_streams) {
        const s = try w.next(deadline) orelse return error.ClosedEarly;
        switch (s.ftype) {
            0 => {
                e.data_bytes += w.last_payload.items.len;
                if (s.flags & 1 != 0) e.ended += 1;
            },
            3 => e.resets += 1,
            else => {},
        }
    }
}

fn runEmpties(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var limits = limitsFor(2 * stream_cap, starh2.Limits.defaults.outbound_bytes_per_server);
    limits.max_streams_per_connection = 128;
    limits.max_streams_per_server = 256;
    limits.cancellation_reaper_jobs = 256;
    var server = try serverFor(rt, gpa, transport, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var e: Empties = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort() };
    const t = try std.Thread.spawn(.{}, emptiesMain, .{&e});
    const deadline = nowNs() + 3 * client_deadline_ns;
    while (!e.done.load(.acquire)) {
        if (nowNs() >= deadline) {
            t.join();
            return error.Deadline;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    t.join();
    if (e.err) |err| return err;
    try std.testing.expectEqual(@as(usize, empty_streams), e.ended);
    try std.testing.expectEqual(@as(usize, 0), e.resets);
    try std.testing.expectEqual(@as(usize, 0), e.data_bytes);
    try waitReservedZero(&server);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runEmptiesCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runEmpties, .{ rt, gpa, transport });
    try h.join();
}

test "empty DATA: 120 streams that end with a payload-less END_STREAM frame complete through a write pool of three chunks (h2c)" {
    try runEmptiesCase(.h2c);
}

test "empty DATA: 120 streams that end with a payload-less END_STREAM frame complete through a write pool of three chunks (tls)" {
    try runEmptiesCase(.tls);
}

test "zz h2 oracle: no protocol violation on any connection in this binary" {
    try @import("h2_oracle.zig").expectNoViolations();
}
