//! Gates for pooled connection slots (stage 1: boot memory) and, at the end,
//! for the request memory pool that takes the general allocator off the
//! request path (stage 2).
//!
//! A slot is reused by running `Connection.init` again over rewound memory, so
//! the questions these tests ask are about the seams around that, not about
//! the protocol:
//!
//! - Does a connection that boots into a used slot behave exactly like one
//!   that boots into a fresh slot, whichever way the previous one ended
//!   (normally, by a connection error with GOAWAY, with streams open, by a
//!   cancel)?
//! - Is the slot back on the free list BEFORE the connection's admission is
//!   released? `Server.connEntry` checks it in safe builds, at the moment it
//!   releases admission, so every test that serves a connection checks it. With
//!   one connection allowed, a client that is already waiting is accepted the
//!   instant admission is released; a slot pushed later would make a second
//!   slot, which `created == 1` also catches.
//! - Does boot make any general-allocator call once the slots exist?
//! - Is a slot that fails the clean check kept out of reuse?
//! - Is every slot freed exactly once at shutdown?
//!
//! The client is a blocking raw-socket client on its own thread, as in
//! `goaway_delivery.zig`, so it sees exactly what the socket carries.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const tls = @import("tls");

const frame = starh2.core.frame;
const conn_mod = starh2.edge.connection;

const wire_client = @import("wire_client.zig");
const client_deadline_ns = wire_client.client_deadline_ns;
const dummy: u8 = 0;

const Transport = wire_client.Transport;
const Wire = wire_client.Wire;
const Counts = wire_client.Counts;

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

/// Holds a stream open until the connection cancels it.
fn hangSse(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    while (true) {
        try zio.sleep(.fromMilliseconds(20));
        if (body.terminalCause() != null) return error.Canceled;
    }
}

/// Takes request memory the way a render does: an `ArrayList` grown to 300 KiB
/// from the request's arena. The response is small, so no frame leaves the
/// frame slabs for the general allocator (which stage 3 still allows).
fn renderPool(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    var list: std.ArrayList(u8) = .empty;
    const row: [100]u8 = @splat('r');
    while (list.items.len < 300 * 1024) try list.appendSlice(req.arena, &row);
    try resp.send(200, &.{}, "ok");
}

/// Copies the body out of one arena into the same arena.
fn postPool(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    const copy = try req.arena.dupe(u8, req.body);
    if (copy.len != post_body_len) return error.BodyLengthChanged;
    try resp.send(200, &.{}, "ok");
}

const post_body_len = 40 * 1024;

/// A short event stream: starts (the SSE header list), writes one event, ends.
fn sseShort(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    try body.finish();
}

/// A compressible body, so that with `response_compression` on, the header list
/// needs `vary: accept-encoding` whether or not the client takes brotli.
fn varyBody(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/plain" }}, "hello vary");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangSse } } },
    .{ .method = .GET, .path = "/render", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = renderPool } } },
    .{ .method = .POST, .path = "/post", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = postPool } } },
    .{ .method = .GET, .path = "/sse-short", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseShort } } },
    .{ .method = .GET, .path = "/vary", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = varyBody } } },
};

const nowNs = wire_client.nowNs;
const realNow = wire_client.realNow;

fn newRuntime(gpa: std.mem.Allocator) !*zio.Runtime {
    return zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
}

const ftype_goaway = 7;

/// One request on a fresh connection, read to the end of the response. The
/// counts are what a reused slot is compared on.
fn probe(wire: *Wire, gpa: std.mem.Allocator) !Counts {
    var counts: Counts = @splat(0);
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try h2c.appendHeaders(gpa, &req, 1, "/hello", true);
    try wire.send(req.items);
    if (try wire.readUntil(&counts, .stream1_end)) return error.ClosedEarly;
    return counts;
}

/// How the connection before the probe ends.
const Ending = enum {
    /// A request completes and the client closes.
    normal,
    /// A request completes, then the client sends a connection error and the
    /// server answers GOAWAY and closes.
    connection_error,
    /// An SSE stream is open when the client closes.
    streams_open,
};

const Outcome = struct {
    before: Counts = @splat(0),
    after: Counts = @splat(0),
    saw_goaway: bool = false,
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
};

fn reuseClient(port: u16, gpa: std.mem.Allocator, transport: Transport, ending: Ending, out: *Outcome) void {
    reuseClientRun(port, gpa, transport, ending, out) catch |err| {
        out.err = err;
    };
    out.done.store(true, .release);
}

fn reuseClientRun(port: u16, gpa: std.mem.Allocator, transport: Transport, ending: Ending, out: *Outcome) !void {
    // The fresh slot: nothing has run on the server yet.
    {
        const w = try Wire.open(gpa, transport, port);
        defer w.close();
        out.before = try probe(w, gpa);
    }

    // The connection that dirties the slot.
    const a = try Wire.open(gpa, transport, port);
    var a_open = true;
    defer if (a_open) a.close();
    var scratch: Counts = @splat(0);
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try h2c.appendHeaders(gpa, &req, 1, if (ending == .streams_open) "/sse" else "/hello", true);
    try a.send(req.items);
    if (try a.readUntil(&scratch, if (ending == .streams_open) .stream1_data else .stream1_end)) return error.ClosedEarly;

    // The next client connects BEFORE this one ends, so the server's accept
    // loop is parked on a full limit and is woken by the release. If the slot
    // is pushed after that release, the woken loop finds an empty free list.
    const b = try Wire.connect(gpa, transport, port);
    defer b.close();

    switch (ending) {
        .normal, .streams_open => {
            a.close();
            a_open = false;
        },
        .connection_error => {
            var bad: [frame.FRAME_HEADER_LEN]u8 = undefined;
            (frame.FrameHeader{ .length = 0, .type = .data, .flags = .{}, .stream_id = 0 }).encode(&bad);
            try a.send(&bad);
            var seen: Counts = @splat(0);
            if (!try a.readUntil(&seen, .closed)) return error.ServerDidNotClose;
            out.saw_goaway = seen[ftype_goaway] != 0;
        },
    }

    try b.handshake();
    out.after = try probe(b, gpa);
}

fn tlsMaterial(io: std.Io, gpa: std.mem.Allocator, cert: *[]u8, key: *[]u8) !void {
    cert.* = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    key.* = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
}

fn serverFor(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, limits: starh2.Limits, cert: *[]u8, key: *[]u8) !starh2.Server {
    const io = rt.io();
    var endpoint: starh2.EndpointConfig = undefined;
    var tls_cfg: ?starh2.TlsConfig = null;
    switch (transport) {
        .h2c => endpoint = .{ .h2c_prior_knowledge = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) },
        .tls => {
            try tlsMaterial(io, gpa, cert, key);
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

fn runReuse(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport, ending: Ending) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var limits = starh2.Limits.defaults;
    // One connection at a time, so every connection after the first reuses
    // the one slot, and a second slot can only mean a slot returned late.
    // (The order itself is checked where it happens: `connEntry` panics in a
    // safe build if admission is released before the slot is back.)
    limits.max_connections = 1;
    var server = try serverFor(rt, gpa, transport, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var out: Outcome = .{};
    const t = try std.Thread.spawn(.{}, reuseClient, .{ server.localAddress(0).getPort(), gpa, transport, ending, &out });
    // Wait inside zio so this executor keeps serving the connections.
    while (!out.done.load(.acquire)) zio.sleep(.fromMilliseconds(10)) catch {};
    t.join();
    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;

    if (out.err) |err| return err;
    if (ending == .connection_error) try std.testing.expect(out.saw_goaway);
    // The reused slot delivered what the fresh one did.
    try std.testing.expectEqualSlices(u8, &out.before, &out.after);
    try std.testing.expect(out.before[1] != 0); // a response HEADERS frame
    // Three connections, one slot, none leaked.
    const slots = server.slots.snapshot();
    try std.testing.expectEqual(@as(usize, 1), slots.created);
    try std.testing.expectEqual(@as(usize, 0), slots.leaked);
    try std.testing.expectEqual(slots.created, slots.len);
}

fn runReuseCase(transport: Transport, ending: Ending) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runReuse, .{ rt, gpa, transport, ending });
    h.join() catch |err| {
        std.debug.print("slot reuse {s} {s}: {s}\n", .{ @tagName(transport), @tagName(ending), @errorName(err) });
        return err;
    };
}

test "slot reuse: h2c after a normal end behaves like a fresh slot" {
    try runReuseCase(.h2c, .normal);
}

test "slot reuse: h2c after a connection error with GOAWAY behaves like a fresh slot" {
    try runReuseCase(.h2c, .connection_error);
}

test "slot reuse: h2c after streams left open behaves like a fresh slot" {
    try runReuseCase(.h2c, .streams_open);
}

test "slot reuse: tls after a normal end behaves like a fresh slot" {
    try runReuseCase(.tls, .normal);
}

test "slot reuse: tls after a connection error with GOAWAY behaves like a fresh slot" {
    try runReuseCase(.tls, .connection_error);
}

test "slot reuse: tls after streams left open behaves like a fresh slot" {
    try runReuseCase(.tls, .streams_open);
}

// ---------------------------------------------------------------------------
// A cancel. Only the connection's own task is canceled, so the server is not
// used: a small accept loop of the test's own drives `serveAccepted` with a
// slot from a `SlotPool`, the way `Server.connEntry` does.

const Rig = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    slabs: starh2.edge.slab_pool.SlabPool,
    requests: starh2.edge.request_pool.RequestPool,
    accounting: conn_mod.GlobalAccounting,
    pool: starh2.edge.conn_slot.SlotPool,
    limits: starh2.Limits,

    fn config(self: *Rig) conn_mod.ConnConfig {
        return .{
            .io = self.io,
            .mode = .h2c,
            .limits = self.limits,
            .router = .{ .routes = &routes },
            .gpa = self.gpa,
            .accounting = &self.accounting,
            .slab_pool = &self.slabs,
            .request_pool = &self.requests,
        };
    }
};

const CancelOutcome = struct {
    before: Counts = @splat(0),
    after: Counts = @splat(0),
    err: ?anyerror = null,
    /// Set once the first connection has its response and waits to be canceled.
    a_ready: std.atomic.Value(bool) = .init(false),
    /// Set by the test once the first connection's task has been canceled.
    a_canceled: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
};

fn cancelClient(port: u16, gpa: std.mem.Allocator, out: *CancelOutcome) void {
    cancelClientRun(port, gpa, out) catch |err| {
        out.err = err;
    };
    out.done.store(true, .release);
}

fn cancelClientRun(port: u16, gpa: std.mem.Allocator, out: *CancelOutcome) !void {
    {
        const w = try Wire.open(gpa, .h2c, port);
        defer w.close();
        out.before = try probe(w, gpa);
    }
    const a = try Wire.open(gpa, .h2c, port);
    defer a.close();
    var scratch: Counts = @splat(0);
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try h2c.appendHeaders(gpa, &req, 1, "/sse", true);
    try a.send(req.items);
    if (try a.readUntil(&scratch, .stream1_data)) return error.ClosedEarly;
    out.a_ready.store(true, .release);
    while (!out.a_canceled.load(.acquire)) std.Thread.yield() catch {};
    const b = try Wire.open(gpa, .h2c, port);
    defer b.close();
    out.after = try probe(b, gpa);
}

fn runCancel(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const io = rt.io();
    var rig: Rig = .{
        .gpa = gpa,
        .io = io,
        .slabs = try starh2.edge.slab_pool.SlabPool.init(gpa, io, starh2.Limits.defaults.outbound_bytes_per_stream, 8),
        .requests = try starh2.edge.request_pool.RequestPool.init(starh2.Limits.defaults.requestPoolConfig()),
        .accounting = .{
            .max_streams = starh2.Limits.defaults.max_streams_per_server,
            .reaper_capacity = starh2.Limits.defaults.cancellation_reaper_jobs,
            .max_outbound_bytes = starh2.Limits.defaults.outbound_bytes_per_server,
            .max_handshakes = starh2.Limits.defaults.concurrent_tls_handshakes,
        },
        .pool = undefined,
        .limits = starh2.Limits.defaults,
    };
    defer rig.slabs.deinit(gpa);
    defer rig.requests.deinit();
    rig.pool = try starh2.edge.conn_slot.SlotPool.init(gpa, 1, try rig.limits.slotBootBytes(.h2c));
    defer rig.pool.deinit();

    // The handler needs a reaper to cancel an SSE stream when the connection
    // goes, as the server provides.
    var reaper = try conn_mod.ReaperPool.init(gpa, io, rig.limits.cancellation_reaper_jobs);
    defer reaper.deinit();
    var worker = try rt.spawn(conn_mod.ReaperPool.worker, .{&reaper});
    defer {
        reaper.jobs.close(io);
        worker.join() catch {};
    }

    const bind = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try bind.listen(io, .{ .reuse_address = true });
    defer listener.socket.close(io);

    var out: CancelOutcome = .{};
    const t = try std.Thread.spawn(.{}, cancelClient, .{ listener.socket.address.getPort(), gpa, &out });
    var joined = false;
    defer if (!joined) t.join();

    // Connection 1: the fresh slot, a normal probe.
    {
        const stream = try listener.accept(io);
        const slot = try rig.pool.take();
        var h = try rt.spawn(rigConnectionReaper, .{ &rig, &reaper, stream, slot });
        try h.join();
    }
    // Connection 2: dirties the slot with an open stream, then is canceled.
    var h2 = blk: {
        const stream = try listener.accept(io);
        const slot = try rig.pool.take();
        break :blk try rt.spawn(rigConnectionReaper, .{ &rig, &reaper, stream, slot });
    };
    const give_up = nowNs() + client_deadline_ns;
    while (!out.a_ready.load(.acquire)) {
        if (out.err) |err| return err;
        if (nowNs() >= give_up) return error.ClientNeverReady;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    h2.cancel();
    out.a_canceled.store(true, .release);
    // Connection 3: must reuse the slot the cancel returned.
    {
        const stream = try listener.accept(io);
        const slot = try rig.pool.take();
        var h = try rt.spawn(rigConnectionReaper, .{ &rig, &reaper, stream, slot });
        try h.join();
    }
    while (!out.done.load(.acquire)) zio.sleep(.fromMilliseconds(10)) catch {};
    t.join();
    joined = true;

    if (out.err) |err| return err;
    try std.testing.expectEqualSlices(u8, &out.before, &out.after);
    try std.testing.expect(out.before[1] != 0);
    const pooled = rig.pool.snapshot();
    try std.testing.expectEqual(@as(usize, 1), pooled.created);
    try std.testing.expectEqual(@as(usize, 0), pooled.leaked);
    try std.testing.expectEqual(@as(usize, 1), pooled.len);
    try std.testing.expectEqual(@as(usize, 0), rig.accounting.active_streams.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), rig.accounting.outbound_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), rig.requests.held_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), rig.accounting.reaper_reserved.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), rig.slabs.rentedCount());
}

fn rigConnectionReaper(rig: *Rig, reaper: *conn_mod.ReaperPool, stream: std.Io.net.Stream, slot: *starh2.edge.conn_slot.ConnSlot) std.Io.Cancelable!void {
    defer _ = rig.pool.recycle(slot);
    var cfg = rig.config();
    cfg.reaper = reaper;
    return conn_mod.serveAccepted(stream, cfg, slot);
}

test "slot reuse: h2c after a cancel behaves like a fresh slot" {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runCancel, .{ rt, gpa });
    try h.join();
}

// ---------------------------------------------------------------------------
// Boot makes no general-allocator call once the slots exist.

/// The server's allocator, counting every call. `conn_mod.test_in_boot` is
/// true on a thread only while it is inside `Connection.init`, which never
/// yields, so a call made with it set was made BY boot and not by a request
/// running on another thread.
const CountingGpa = struct {
    child: std.mem.Allocator,
    calls: std.atomic.Value(usize) = .init(0),
    /// Calls for at least `large_call_bytes`: bigger than any control frame,
    /// header table entry or preface frame, so a call this large in a steady
    /// state is a table or a request's memory.
    large_calls: std.atomic.Value(usize) = .init(0),
    boot_calls: std.atomic.Value(usize) = .init(0),
    boot_max_len: std.atomic.Value(usize) = .init(0),

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn allocator(self: *CountingGpa) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const large_call_bytes = 1024;

    fn note(self: *CountingGpa, len: usize) void {
        _ = self.calls.fetchAdd(1, .monotonic);
        if (len >= large_call_bytes) _ = self.large_calls.fetchAdd(1, .monotonic);
        if (conn_mod.test_in_boot) {
            _ = self.boot_calls.fetchAdd(1, .monotonic);
            _ = self.boot_max_len.fetchMax(len, .monotonic);
        }
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *CountingGpa = @ptrCast(@alignCast(ctx));
        self.note(len);
        return self.child.rawAlloc(len, alignment, ra);
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
        const self: *CountingGpa = @ptrCast(@alignCast(ctx));
        self.note(new_len);
        return self.child.rawResize(memory, alignment, new_len, ra);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
        const self: *CountingGpa = @ptrCast(@alignCast(ctx));
        self.note(new_len);
        return self.child.rawRemap(memory, alignment, new_len, ra);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *CountingGpa = @ptrCast(@alignCast(ctx));
        self.child.rawFree(memory, alignment, ra);
    }
};

const Round = struct {
    n: usize,
    /// Requests each connection makes after all are connected. Zero is the
    /// single probe the boot test uses.
    requests: usize = 0,
    /// Run `http2PathScript` on each connection instead, and add the frame
    /// kinds the server sent to `kinds`.
    script: bool = false,
    kinds: std.atomic.Value(u8) = .init(0),
    transport: Transport,
    port: u16,
    gpa: std.mem.Allocator,
    connected: std.atomic.Value(usize) = .init(0),
    served: std.atomic.Value(usize) = .init(0),
    failed: std.atomic.Value(usize) = .init(0),
    done: std.atomic.Value(usize) = .init(0),
};

/// All `n` clients are connected before any of them sends, and none closes
/// before every one has its response, so the server holds `n` connections at
/// once in every round and needs exactly `n` slots.
fn roundClient(r: *Round) void {
    roundClientRun(r) catch |err| {
        std.debug.print("round client failed: {s}\n", .{@errorName(err)});
        _ = r.failed.fetchAdd(1, .monotonic);
    };
    _ = r.done.fetchAdd(1, .release);
}

fn roundClientRun(r: *Round) !void {
    // The script does its own handshake, with its first flight pipelined.
    const w = if (r.script)
        try Wire.connect(r.gpa, r.transport, r.port)
    else
        try Wire.open(r.gpa, r.transport, r.port);
    defer w.close();
    _ = r.connected.fetchAdd(1, .release);
    const deadline = nowNs() + client_deadline_ns;
    while (r.connected.load(.acquire) < r.n) {
        if (nowNs() >= deadline) return error.Deadline;
        std.Thread.yield() catch {};
    }
    if (r.script) {
        try http2PathScript(w, r.gpa, &r.kinds);
    } else if (r.requests == 0) {
        _ = try probe(w, r.gpa);
    } else {
        try requestLoop(w, r.gpa, r.requests);
    }
    _ = r.served.fetchAdd(1, .release);
    while (r.served.load(.acquire) < r.n) {
        if (nowNs() >= deadline) return error.Deadline;
        std.Thread.yield() catch {};
    }
}

fn waitIdle(server: *starh2.Server) !void {
    const give_up = nowNs() + client_deadline_ns;
    while (true) {
        const slots = server.slots.snapshot();
        if (server.active_connections.load(.acquire) == 0 and slots.len + slots.leaked == slots.created) break;
        if (nowNs() >= give_up) return error.ServerNeverIdle;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

fn runRound(server: *starh2.Server, gpa: std.mem.Allocator, transport: Transport, n: usize) !void {
    return runRoundRequests(server, gpa, transport, n, 0);
}

fn runRoundRequests(server: *starh2.Server, gpa: std.mem.Allocator, transport: Transport, n: usize, requests: usize) !void {
    var r: Round = .{ .n = n, .requests = requests, .transport = transport, .port = server.localAddress(0).getPort(), .gpa = gpa };
    return runRoundOf(server, &r);
}

/// One round of `n` connections that each run the HTTP/2 path script. Returns
/// the frame kinds the servers sent, as a `PathKinds` mask.
fn runRoundScript(server: *starh2.Server, gpa: std.mem.Allocator, transport: Transport, n: usize) !u8 {
    var r: Round = .{ .n = n, .script = true, .transport = transport, .port = server.localAddress(0).getPort(), .gpa = gpa };
    try runRoundOf(server, &r);
    return r.kinds.load(.acquire);
}

fn runRoundOf(server: *starh2.Server, r: *Round) !void {
    const n = r.n;
    const gpa = r.gpa;
    const threads = try gpa.alloc(std.Thread, n);
    defer gpa.free(threads);
    for (threads) |*t| t.* = try std.Thread.spawn(.{}, roundClient, .{r});
    while (r.done.load(.acquire) < n) zio.sleep(.fromMilliseconds(5)) catch {};
    for (threads) |t| t.join();
    try std.testing.expectEqual(@as(usize, 0), r.failed.load(.acquire));
    try waitIdle(server);
}

/// What boot is allowed to ask the general allocator for: nothing. The two
/// frames the server's preface queues (SETTINGS and WINDOW_UPDATE) are made in
/// the connection's frame pool, after boot, by `finishBoot`.
const preface_frames_per_connection = 0;

fn runBootCalls(rt: *zio.Runtime, real: std.mem.Allocator, transport: Transport) !void {
    var counting: CountingGpa = .{ .child = real };
    const gpa = counting.allocator();
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        real.free(cert);
        real.free(key);
    }
    const n = 4;
    var server = try serverFor(rt, gpa, transport, starh2.Limits.defaults, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    // Warm-up: the slots are made.
    const boot_0 = counting.boot_calls.load(.monotonic);
    try runRound(&server, real, transport, n);
    try std.testing.expectEqual(@as(usize, n), server.slots.snapshot().created);
    const boot_1 = counting.boot_calls.load(.monotonic);
    const calls_1 = counting.calls.load(.monotonic);

    // Steady state: the same slots serve the same load.
    try runRound(&server, real, transport, n);
    const boot_2 = counting.boot_calls.load(.monotonic);
    const calls_2 = counting.calls.load(.monotonic);
    try std.testing.expectEqual(@as(usize, n), server.slots.snapshot().created);

    std.debug.print("{s} boot calls: warm-up round {d} (for {d} connections), steady round {d}; all calls in the steady round: {d}; largest boot call {d} bytes\n", .{
        @tagName(transport),
        boot_1 - boot_0,
        n,
        boot_2 - boot_1,
        calls_2 - calls_1,
        counting.boot_max_len.load(.monotonic),
    });
    try std.testing.expectEqual(@as(usize, n * preface_frames_per_connection), boot_1 - boot_0);
    try std.testing.expectEqual(@as(usize, n * preface_frames_per_connection), boot_2 - boot_1);
    // Nothing at all: no table and no frame.
    try std.testing.expectEqual(@as(usize, 0), counting.boot_max_len.load(.monotonic));

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runBootCallsCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runBootCalls, .{ rt, gpa, transport });
    try h.join();
}

test "slots: h2c boot makes no general-allocator call after warm-up" {
    try runBootCallsCase(.h2c);
}

test "slots: tls boot makes no general-allocator call after warm-up" {
    try runBootCallsCase(.tls);
}

// ---------------------------------------------------------------------------
// A slot that fails the clean check is not reused.

/// One slot of headroom over the `leaks`, so the server can still serve after
/// every leak: slots in existence, quarantined ones included, never pass
/// `max_connections`.
fn runDirty(rt: *zio.Runtime, gpa: std.mem.Allocator, kind: conn_mod.DirtyKind, leaks: usize) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    var limits = starh2.Limits.defaults;
    limits.max_connections = leaks + 1;
    var server = try serverFor(rt, gpa, .h2c, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    // The next `leaks` connections to end leave one condition set after
    // `quiesce`. Four of the kinds name conditions `quiesce` does not check at
    // all, so these tests are what shows `dirtiness` sees them.
    conn_mod.test_dirty_kind = kind;
    conn_mod.test_dirty_remaining.store(leaks, .release);
    defer conn_mod.test_dirty_remaining.store(0, .release);
    for (1..leaks + 1) |i| {
        try runRound(&server, gpa, .h2c, 1);
        const slots = server.slots.snapshot();
        // Each one is held back, and the next connection gets a new slot.
        try std.testing.expectEqual(i, slots.created);
        try std.testing.expectEqual(i, slots.leaked);
        try std.testing.expectEqual(@as(usize, 0), slots.len);
    }

    // The server goes on serving with every slot it had held back, on the
    // last slot the cap allows.
    try runRound(&server, gpa, .h2c, 1);
    const slots = server.slots.snapshot();
    try std.testing.expectEqual(leaks + 1, slots.created);
    try std.testing.expectEqual(leaks, slots.leaked);
    try std.testing.expectEqual(@as(usize, 1), slots.len);
    try std.testing.expectEqual(@as(usize, 0), slots.refused);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runDirtyCase(kind: conn_mod.DirtyKind, leaks: usize) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runDirty, .{ rt, gpa, kind, leaks });
    h.join() catch |err| {
        std.debug.print("dirty slot {s}: {s}\n", .{ @tagName(kind), @errorName(err) });
        return err;
    };
}

test "slots: several dirty slots are kept and the server serves on the last slot the cap allows" {
    try runDirtyCase(.write_chunk_rented, 3);
}

test "slots: a slot left with a write chunk rented is kept out of reuse" {
    try runDirtyCase(.write_chunk_rented, 1);
}

test "slots: a slot left with request bytes reserved is kept out of reuse" {
    try runDirtyCase(.request_bytes_reserved, 1);
}

test "slots: a slot left with a stream admitted is kept out of reuse" {
    try runDirtyCase(.stream_still_admitted, 1);
}

test "slots: a slot left with a handler join held is kept out of reuse" {
    try runDirtyCase(.handler_join_held, 1);
}

test "slots: a slot left with a completion queued is kept out of reuse" {
    try runDirtyCase(.completion_queued, 1);
}

/// A connection that tries to be served and is closed instead.
const Refused = struct {
    port: u16,
    gpa: std.mem.Allocator,
    transport: Transport,
    /// True when the connection was closed before a response.
    closed: bool = false,
    done: std.atomic.Value(bool) = .init(false),
};

fn refusedClient(r: *Refused) void {
    if (Wire.open(r.gpa, r.transport, r.port)) |w| {
        defer w.close();
        if (probe(w, r.gpa)) |_| {} else |_| r.closed = true;
    } else |_| r.closed = true;
    r.done.store(true, .release);
}

/// Every slot the cap allows has been quarantined. The next connection is
/// refused, and nothing is made past the cap, so the slots in existence stay
/// what `resourceUpperBound` counts.
fn runExhausted(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var limits = starh2.Limits.defaults;
    limits.max_connections = 2;
    var server = try serverFor(rt, gpa, transport, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    conn_mod.test_dirty_kind = .write_chunk_rented;
    conn_mod.test_dirty_remaining.store(2, .release);
    defer conn_mod.test_dirty_remaining.store(0, .release);
    try runRound(&server, gpa, transport, 1);
    try runRound(&server, gpa, transport, 1);
    var slots = server.slots.snapshot();
    try std.testing.expectEqual(@as(usize, 2), slots.created);
    try std.testing.expectEqual(@as(usize, 2), slots.leaked);

    var r: Refused = .{ .port = server.localAddress(0).getPort(), .gpa = gpa, .transport = transport };
    const t = try std.Thread.spawn(.{}, refusedClient, .{&r});
    while (!r.done.load(.acquire)) zio.sleep(.fromMilliseconds(10)) catch {};
    t.join();
    try std.testing.expect(r.closed);
    // The refusal gave back everything it took, handshake slot included.
    const give_up = nowNs() + client_deadline_ns;
    while (server.active_connections.load(.acquire) != 0 or server.accounting.active_handshakes.load(.acquire) != 0) {
        if (nowNs() >= give_up) return error.RefusalNeverReleased;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    slots = server.slots.snapshot();
    try std.testing.expectEqual(@as(usize, 2), slots.created);
    try std.testing.expectEqual(@as(usize, 2), slots.leaked);
    try std.testing.expectEqual(@as(usize, 0), slots.len);
    try std.testing.expect(slots.refused >= 1);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runExhaustedCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runExhausted, .{ rt, gpa, transport });
    h.join() catch |err| {
        std.debug.print("exhausted {s}: {s}\n", .{ @tagName(transport), @errorName(err) });
        return err;
    };
}

test "slots: h2c with every slot quarantined, the next connection is refused and no slot is made" {
    try runExhaustedCase(.h2c);
}

test "slots: tls with every slot quarantined, the next connection is refused and its handshake slot is released" {
    try runExhaustedCase(.tls);
}

// ---------------------------------------------------------------------------
// Shutdown with slots in use and slots free.

const Hold = struct {
    port: u16,
    gpa: std.mem.Allocator,
    connected: std.atomic.Value(usize) = .init(0),
    release_one: std.atomic.Value(bool) = .init(false),
    failed: std.atomic.Value(usize) = .init(0),
    done: std.atomic.Value(usize) = .init(0),
};

/// Connects, finishes a request, then holds the connection open until the
/// server closes it (or, for the one released early, closes it itself).
fn holdClient(h: *Hold, early: bool) void {
    holdClientRun(h, early) catch {
        _ = h.failed.fetchAdd(1, .monotonic);
    };
    _ = h.done.fetchAdd(1, .release);
}

fn holdClientRun(h: *Hold, early: bool) !void {
    const w = try Wire.open(h.gpa, .h2c, h.port);
    defer w.close();
    _ = try probe(w, h.gpa);
    _ = h.connected.fetchAdd(1, .release);
    if (early) {
        const deadline = nowNs() + client_deadline_ns;
        while (!h.release_one.load(.acquire)) {
            if (nowNs() >= deadline) return error.Deadline;
            std.Thread.yield() catch {};
        }
        return;
    }
    var seen: Counts = @splat(0);
    if (!try w.readUntil(&seen, .closed)) return error.ServerDidNotClose;
}

fn runShutdown(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    var limits = starh2.Limits.defaults;
    limits.max_connections = 8;
    limits.graceful_drain_timeout_ns = 300 * std.time.ns_per_ms;
    var server = try serverFor(rt, gpa, .h2c, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var h: Hold = .{ .port = server.localAddress(0).getPort(), .gpa = gpa };
    var threads: [3]std.Thread = undefined;
    for (&threads, 0..) |*t, i| t.* = try std.Thread.spawn(.{}, holdClient, .{ &h, i == 0 });
    const give_up = nowNs() + client_deadline_ns;
    while (h.connected.load(.acquire) < 3) {
        if (nowNs() >= give_up) return error.ClientsNeverConnected;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    // One connection ends, so a slot is free while two are in use.
    h.release_one.store(true, .release);
    while (server.slots.snapshot().len != 1) {
        if (nowNs() >= give_up) return error.SlotNeverReturned;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    try std.testing.expectEqual(@as(usize, 3), server.slots.snapshot().created);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
    while (h.done.load(.acquire) < 3) zio.sleep(.fromMilliseconds(5)) catch {};
    for (threads) |t| t.join();
    try std.testing.expectEqual(@as(usize, 0), h.failed.load(.acquire));

    // Every slot is back. `Server.deinit` (deferred) frees each one once: it
    // asserts the free list holds exactly the slots made, and the testing
    // allocator fails the test on a leak or a double free.
    const done = server.slots.snapshot();
    try std.testing.expectEqual(@as(usize, 3), done.created);
    try std.testing.expectEqual(@as(usize, 3), done.len);
    try std.testing.expectEqual(@as(usize, 0), done.leaked);
}

test "slots: shutdown with slots in use and slots free returns and frees every slot once" {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runShutdown, .{ rt, gpa });
    try h.join();
}

// ---------------------------------------------------------------------------
// Stage 2: request memory is never the general allocator.

/// Read frames until stream `sid` ends (END_STREAM or a reset). True when the
/// server closed first.
fn readStreamEnd(wire: *Wire, sid: u31) !bool {
    const deadline = nowNs() + client_deadline_ns;
    while (true) {
        const s = try wire.next(deadline) orelse return true;
        if (s.sid != sid) continue;
        if (s.ftype == 3) return error.StreamReset;
        if ((s.ftype == 0 or s.ftype == 1) and s.flags & 1 != 0) return false;
    }
}

/// `n` requests on one connection, one after the other: renders that take
/// 300 KiB of request memory, and 40 KiB bodies that are copied.
fn requestLoop(wire: *Wire, gpa: std.mem.Allocator, n: usize) !void {
    const body: [post_body_len]u8 = @splat('p');
    var sid: u31 = 1;
    for (0..n) |i| {
        var req: std.ArrayList(u8) = .empty;
        defer req.deinit(gpa);
        if (i == 0) {
            var preface = try h2c.buildClientPrefaceAndSettings(gpa);
            defer preface.deinit(gpa);
            try req.appendSlice(gpa, preface.items);
        }
        if (i % 2 == 0) {
            try h2c.appendHeaders(gpa, &req, sid, "/render", true);
        } else {
            try h2c.appendHeadersExtra(gpa, &req, sid, "POST", "/post", false, &.{});
            // Frames of at most the default maximum frame size.
            var off: usize = 0;
            while (off < body.len) {
                const take = @min(body.len - off, frame.DEFAULT_MAX_FRAME_SIZE);
                try h2c.appendData(gpa, &req, sid, body[off..][0..take], off + take == body.len);
                off += take;
            }
        }
        try wire.send(req.items);
        if (try readStreamEnd(wire, sid)) return error.ClosedEarly;
        sid += 2;
    }
}

fn runRequestMemoryCalls(rt: *zio.Runtime, real: std.mem.Allocator, transport: Transport) !void {
    var counting: CountingGpa = .{ .child = real };
    const gpa = counting.allocator();
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        real.free(cert);
        real.free(key);
    }
    const n = 4;
    const per_conn = 8;
    var server = try serverFor(rt, gpa, transport, starh2.Limits.defaults, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    // One connection alone, one render and one post: what a single request
    // can hold at its peak, in class bytes. This is the unit the pool's size
    // is judged in below.
    try runRoundRequests(&server, real, transport, 1, 2);
    const one = server.requests.snapshot();
    try std.testing.expectEqual(@as(usize, 0), one.held_bytes);
    const per_request_peak = one.held_high;
    try std.testing.expect(per_request_peak >= 512 * 1024);

    // Warm-up: the slots, and the request blocks the load needs, are made.
    // Its large calls are the control: the counter does see large calls.
    const large_0 = counting.large_calls.load(.monotonic);
    try runRoundRequests(&server, real, transport, n, per_conn);
    const large_1 = counting.large_calls.load(.monotonic);
    const calls_1 = counting.calls.load(.monotonic);
    const pool_1 = server.requests.snapshot();
    try std.testing.expect(large_1 > large_0);

    // Steady state: the same connections' worth of the same requests.
    try runRoundRequests(&server, real, transport, n, per_conn);
    const large_2 = counting.large_calls.load(.monotonic);
    const calls_2 = counting.calls.load(.monotonic);
    const pool_2 = server.requests.snapshot();

    // The requests really took their memory from the pool: 300 KiB renders
    // are 512 KiB blocks, and every request held a first block.
    try std.testing.expect(pool_2.held_high >= 512 * 1024);
    var used: u64 = 0;
    for (pool_2.classes[0..pool_2.class_count]) |c| used += c.made + c.reused;
    try std.testing.expect(used >= n * per_conn * 2);
    // The pool holds what the CONCURRENT requests need and no more: `n`
    // connections run one request each at a time, so no more than `n` times
    // one request's peak is ever resident, however many requests ran. (A count
    // of blocks made would be flatter but also flaky, since how the `n`
    // connections interleave decides the peak.)
    try std.testing.expect(pool_2.resident_high <= n * per_request_peak);
    for (pool_2.refused) |r| try std.testing.expectEqual(@as(u64, 0), r);
    _ = pool_1;
    // Everything is home once the work is done.
    try std.testing.expectEqual(@as(usize, 0), server.requests.held_bytes.load(.acquire));

    // How request memory is told apart from the per-request calls stage 3
    // still has: those are control frames (the first 40 calls of a steady
    // round were 9, 13 and 45 bytes: a frame header, a WINDOW_UPDATE and the
    // two preface frames), all under `large_call_bytes`, while every piece of
    // request memory is at least a 64 KiB block. So a steady state with no
    // large call made no request-memory call. The requests are shaped so
    // that nothing else is large: the responses are two bytes, and the client
    // sends no header that enters the HPACK table.
    std.debug.print("{s} request memory: steady round made {d} large and {d} small general-allocator calls for {d} requests; pool held at most {d} KiB\n", .{
        @tagName(transport),
        large_2 - large_1,
        (calls_2 - calls_1) - (large_2 - large_1),
        n * per_conn,
        pool_2.held_high / 1024,
    });
    try std.testing.expectEqual(@as(usize, 0), large_2 - large_1);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runRequestMemoryCallsCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runRequestMemoryCalls, .{ rt, gpa, transport });
    try h.join();
}

test "request memory: h2c makes no large general-allocator call after warm-up" {
    try runRequestMemoryCallsCase(.h2c);
}

test "request memory: tls makes no large general-allocator call after warm-up" {
    try runRequestMemoryCallsCase(.tls);
}

// ---------------------------------------------------------------------------
// The whole HTTP/2 path makes no general-allocator call (slot pool stage 3).

const hpack = starh2.core.hpack;

/// The frame kinds the server sent that the script is meant to provoke, one
/// bit each. A run that provoked fewer proves less, so the test checks them.
const PathKinds = struct {
    const settings_ack: u8 = 1;
    const ping_ack: u8 = 2;
    const window_update: u8 = 4;
    const rst_stream: u8 = 8;
    const goaway: u8 = 16;
    const all: u8 = 31;
};

const PathStop = union(enum) {
    stream_end: u31,
    /// Every one of these streams ends (in any order).
    streams_end: []const u31,
    stream_rst: u31,
    closed,
};

/// Read frames, noting the kinds of control frame the server sends, until
/// `stop`.
fn readPath(wire: *Wire, kinds: *std.atomic.Value(u8), stop: PathStop) !void {
    const deadline = nowNs() + client_deadline_ns;
    var ended: [8]bool = @splat(false);
    while (true) {
        const s = try wire.next(deadline) orelse switch (stop) {
            .closed => return,
            else => return error.ClosedEarly,
        };
        switch (s.ftype) {
            4 => if (s.flags & 1 != 0) {
                _ = kinds.fetchOr(PathKinds.settings_ack, .acq_rel);
            },
            6 => if (s.flags & 1 != 0) {
                _ = kinds.fetchOr(PathKinds.ping_ack, .acq_rel);
            },
            8 => _ = kinds.fetchOr(PathKinds.window_update, .acq_rel),
            3 => _ = kinds.fetchOr(PathKinds.rst_stream, .acq_rel),
            7 => _ = kinds.fetchOr(PathKinds.goaway, .acq_rel),
            else => {},
        }
        switch (stop) {
            .stream_end => |sid| if (s.sid == sid and (s.ftype == 0 or s.ftype == 1) and s.flags & 1 != 0) return,
            .streams_end => |sids| {
                if ((s.ftype == 0 or s.ftype == 1) and s.flags & 1 != 0) {
                    for (sids, 0..) |sid, k| {
                        if (s.sid == sid) ended[k] = true;
                    }
                }
                for (sids, 0..) |_, k| {
                    if (!ended[k]) break;
                } else return;
            },
            .stream_rst => |sid| if (s.sid == sid and s.ftype == 3) return,
            .closed => {},
        }
    }
}

/// One connection's script. Every control frame the server can send comes up,
/// in the shape a client makes it: the SETTINGS ack, a PING ack, WINDOW_UPDATE
/// (a 40 KiB body is past half the stream window), a RST_STREAM (a request
/// with no :path), and a GOAWAY (DATA on stream 0), after which the server
/// closes.
fn http2PathScript(wire: *Wire, gpa: std.mem.Allocator, kinds: *std.atomic.Value(u8)) !void {
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(gpa);

    // A request, then a PING and a SETTINGS the server must answer.
    var preface = try h2c.buildClientPrefaceAndSettings(gpa);
    defer preface.deinit(gpa);
    try req.appendSlice(gpa, preface.items);
    try h2c.appendHeaders(gpa, &req, 1, "/hello", true);
    var ping_buf: [17]u8 = undefined;
    const ping_n = try frame.Serializer.ping(&ping_buf, false, &.{ 1, 2, 3, 4, 5, 6, 7, 8 });
    try req.appendSlice(gpa, ping_buf[0..ping_n]);
    var settings_buf: [9]u8 = undefined;
    const settings_n = try frame.Serializer.settingsFrame(&settings_buf, false, &.{});
    try req.appendSlice(gpa, settings_buf[0..settings_n]);
    // The whole first flight leaves with the TLS Finished, so on TLS the server
    // reads it as handshake leftover.
    try wire.handshakeWith(req.items);
    try readPath(wire, kinds, .{ .stream_end = 1 });

    // An event stream (the SSE header list) and a response whose header list
    // needs `vary` (compression is on, the client takes no brotli).
    req.clearRetainingCapacity();
    try h2c.appendHeaders(gpa, &req, 3, "/sse-short", true);
    try h2c.appendHeaders(gpa, &req, 5, "/vary", true);
    try wire.send(req.items);
    try readPath(wire, kinds, .{ .streams_end = &.{ 3, 5 } });

    // Incrementally indexed headers: twelve requests each put a 400-byte entry
    // in the connection's HPACK table, which holds 4,096 bytes, so it fills,
    // evicts its oldest entries and compacts. The last request names the
    // newest entry by index.
    req.clearRetainingCapacity();
    var value: [400]u8 = undefined;
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(gpa);
    var sid: u31 = 7;
    for (0..indexed_requests) |k| {
        @memset(&value, @intCast('a' + k));
        raw.clearRetainingCapacity();
        try appendLiteralIndexed(gpa, &raw, "x-trace", &value);
        try appendHelloWithRaw(gpa, &req, sid, raw.items);
        sid += 2;
    }
    try appendHelloWithRaw(gpa, &req, sid, &.{0x80 | 62});
    try wire.send(req.items);
    try readPath(wire, kinds, .{ .stream_end = sid });
    sid += 2;

    // A body past half the stream window: the server credits it back.
    req.clearRetainingCapacity();
    const body: [post_body_len]u8 = @splat('p');
    try h2c.appendHeadersExtra(gpa, &req, sid, "POST", "/post", false, &.{});
    var off: usize = 0;
    while (off < body.len) {
        const take = @min(body.len - off, frame.DEFAULT_MAX_FRAME_SIZE);
        try h2c.appendData(gpa, &req, sid, body[off..][0..take], off + take == body.len);
        off += take;
    }
    try wire.send(req.items);
    try readPath(wire, kinds, .{ .stream_end = sid });
    sid += 2;

    // A request body that stops: the stream is reset when its idle time is up.
    req.clearRetainingCapacity();
    try h2c.appendHeadersExtra(gpa, &req, sid, "POST", "/post", false, &.{});
    try h2c.appendData(gpa, &req, sid, "partial", false);
    try wire.send(req.items);
    try readPath(wire, kinds, .{ .stream_rst = sid });
    sid += 2;

    // A request the server resets: no :path.
    req.clearRetainingCapacity();
    const no_path = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(gpa, &no_path);
    defer gpa.free(block);
    var fh_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = true },
        .stream_id = sid,
    }).encode(&fh_buf);
    try req.appendSlice(gpa, &fh_buf);
    try req.appendSlice(gpa, block);
    try wire.send(req.items);
    try readPath(wire, kinds, .{ .stream_rst = sid });

    // A connection error: the server sends GOAWAY and closes.
    req.clearRetainingCapacity();
    try h2c.appendData(gpa, &req, 0, "x", false);
    try wire.send(req.items);
    try readPath(wire, kinds, .closed);
}

const indexed_requests = 12;

/// `0x40`: a literal field that the peer's table takes in (RFC 7541 section
/// 6.2.1), with a new name. Strings are raw, not Huffman.
fn appendLiteralIndexed(gpa: std.mem.Allocator, out: *std.ArrayList(u8), name: []const u8, value: []const u8) !void {
    try out.append(gpa, 0x40);
    try appendRawString(gpa, out, name);
    try appendRawString(gpa, out, value);
}

fn appendRawString(gpa: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) !void {
    if (s.len < 127) {
        try out.append(gpa, @intCast(s.len));
    } else {
        try out.append(gpa, 127);
        var rest = s.len - 127;
        while (rest >= 128) : (rest /= 128) try out.append(gpa, @intCast(rest % 128 + 128));
        try out.append(gpa, @intCast(rest));
    }
    try out.appendSlice(gpa, s);
}

/// GET /hello on `sid` whose header block is the four pseudo-headers and then
/// `raw`, header block bytes the test wrote by hand.
fn appendHelloWithRaw(gpa: std.mem.Allocator, out: *std.ArrayList(u8), sid: u31, raw: []const u8) !void {
    const pseudo = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/hello" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const lead = try hpack.Encoder.encode(gpa, &pseudo);
    defer gpa.free(lead);
    var fh_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{
        .length = @intCast(lead.len + raw.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = true },
        .stream_id = sid,
    }).encode(&fh_buf);
    try out.appendSlice(gpa, &fh_buf);
    try out.appendSlice(gpa, lead);
    try out.appendSlice(gpa, raw);
}

fn runHttp2PathCalls(rt: *zio.Runtime, real: std.mem.Allocator, transport: Transport) !void {
    var counting: CountingGpa = .{ .child = real };
    const gpa = counting.allocator();
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        real.free(cert);
        real.free(key);
    }
    const n = 4;
    var limits = starh2.Limits.defaults;
    limits.response_compression = true;
    limits.request_body_idle_timeout_ns = 150 * std.time.ns_per_ms;
    var server = try serverFor(rt, gpa, transport, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    // Warm-up: the slots are made (the slot pool at a new peak is the one
    // allocation this path is allowed, and it happens here, not below), and
    // the request blocks the load needs.
    const kinds_1 = try runRoundScript(&server, real, transport, n);
    const calls_1 = counting.calls.load(.monotonic);
    const frames_1 = om.frames_built.load(.monotonic);
    const controls_1 = starh2.core.session.control_frames_built.load(.monotonic);
    const inserts_1 = hpack.table_insertions.load(.monotonic);
    const lists_1 = conn_mod.h2_work.response_header_lists.load(.monotonic);
    const leftovers_1 = conn_mod.h2_work.tls_leftovers.load(.monotonic);
    const expiries_1 = starh2.core.session.body_idle_expiries.load(.monotonic);

    // Steady state: the same slots serve the same script.
    const kinds_2 = try runRoundScript(&server, real, transport, n);
    const calls_2 = counting.calls.load(.monotonic);
    const frames = om.frames_built.load(.monotonic) - frames_1;
    const controls = starh2.core.session.control_frames_built.load(.monotonic) - controls_1;
    const inserts = hpack.table_insertions.load(.monotonic) - inserts_1;
    const lists = conn_mod.h2_work.response_header_lists.load(.monotonic) - lists_1;
    const leftovers = conn_mod.h2_work.tls_leftovers.load(.monotonic) - leftovers_1;
    const expiries = starh2.core.session.body_idle_expiries.load(.monotonic) - expiries_1;
    try std.testing.expectEqual(@as(usize, n), server.slots.snapshot().created);

    std.debug.print("{s} http/2 path: steady round made {d} general-allocator calls of any size; frames built {d}, control frames {d}, hpack insertions {d}, response header lists {d}, tls leftovers {d}, idle-body expiries {d}, frame kinds seen {b:0>5}\n", .{
        @tagName(transport), calls_2 - calls_1, frames, controls, inserts, lists, leftovers, expiries, kinds_2,
    });
    // The counter is alive: the warm-up round made the slots, and that is
    // general-allocator calls.
    try std.testing.expect(calls_1 > 0);
    // The script provoked every kind of control frame, in both rounds, and
    // built frames: a zero below is then a result and not a script that
    // stopped early.
    try std.testing.expectEqual(PathKinds.all, kinds_1);
    try std.testing.expectEqual(PathKinds.all, kinds_2);
    try std.testing.expect(frames >= n * 8);
    try std.testing.expect(controls >= n * 6);
    try std.testing.expect(inserts >= n * indexed_requests);
    // Every response built a header list (the SSE and Vary ones in the arena),
    // every connection's idle body expired, and on TLS the first flight was
    // read as handshake leftover.
    // Per connection: /hello, /sse-short, /vary, the indexed requests, the one
    // that names the newest entry, and the POST.
    try std.testing.expectEqual(n * (indexed_requests + 5), lists);
    try std.testing.expect(expiries >= n);
    if (transport == .tls) try std.testing.expect(leftovers >= 1);
    // And there is no call, of any size, in the steady round.
    try std.testing.expectEqual(@as(usize, 0), calls_2 - calls_1);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runHttp2PathCallsCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runHttp2PathCalls, .{ rt, gpa, transport });
    try h.join();
}

const om = conn_mod.outbound_memory;

test "http/2 path: h2c makes no general-allocator call of any size after warm-up" {
    try runHttp2PathCallsCase(.h2c);
}

test "http/2 path: tls makes no general-allocator call of any size after warm-up" {
    try runHttp2PathCallsCase(.tls);
}

test "zz h2 oracle: no protocol violation on any connection in this binary" {
    try @import("h2_oracle.zig").expectNoViolations();
}
