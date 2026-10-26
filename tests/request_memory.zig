//! Gates for request memory (slot pool stage 2): the server-wide block pool
//! every HTTP/2 request's header list, body and handler allocations come from.
//!
//! These tests run a real server and a client that sends exactly the bytes a
//! scenario needs. They ask three things the unit tests of the pool cannot:
//!
//! - Does each cap, when it refuses, produce the status the design says
//!   (500 for the request's own cap, 503 for the connection's and the
//!   server's before the response is committed; RST_STREAM(INTERNAL_ERROR)
//!   after), and does the connection go on serving?
//! - Does a header block still decode when the pool has no memory for its
//!   request, so a later request that names a table entry decodes correctly?
//! - Does every block come home, whichever way a request ends?
//!
//! The "no general-allocator call" gate for request memory is in
//! `slot_reuse.zig`, beside the boot-call gate it extends.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const frame = starh2.core.frame;
const hpack = starh2.core.hpack;
const rp = starh2.edge.request_pool;
const conn_mod = starh2.edge.connection;

const dummy: u8 = 0;

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

// ---------------------------------------------------------------------------
// Handlers.

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

/// Takes `req.arena` memory and answers. The size is in the path's suffix of
/// the route that owns it: see `routes`.
fn takes1MiB(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    _ = try req.arena.alloc(u8, 1024 * 1024);
    try resp.send(200, &.{}, "ok");
}

fn takes100KiB(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    _ = try req.arena.alloc(u8, 100 * 1024);
    try resp.send(200, &.{}, "ok");
}

/// The bench's render shape: an `ArrayList` grown to 300 KiB.
fn renders300KiB(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    var list: std.ArrayList(u8) = .empty;
    const row: [100]u8 = @splat('r');
    while (list.items.len < 300 * 1024) try list.appendSlice(req.arena, &row);
    try resp.send(200, &.{}, "ok");
}

/// Commits its response (an SSE stream), then asks for memory it cannot have.
fn sseThenTakes1MiB(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    _ = try req.arena.alloc(u8, 1024 * 1024);
}

var hold_started: std.atomic.Value(usize) = .init(0);
var hold_release: std.atomic.Value(bool) = .init(false);

/// Holds `req.arena` memory (100 KiB when the path says so) until released,
/// then answers. A task handler, so it runs beside the actor.
fn holds100KiB(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = try req.arena.alloc(u8, 100 * 1024);
    try holdThenAnswer(resp);
}

fn holdsFirstBlockOnly(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try holdThenAnswer(resp);
}

fn holdThenAnswer(resp: *starh2.Response) anyerror!void {
    _ = hold_started.fetchAdd(1, .acq_rel);
    while (!hold_release.load(.acquire)) try zio.sleep(.fromMilliseconds(5));
    try resp.send(200, &.{}, "ok");
}

/// An SSE stream that holds 100 KiB of request memory until it is canceled.
fn hangsHolding(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = try req.arena.alloc(u8, 100 * 1024);
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    _ = hold_started.fetchAdd(1, .acq_rel);
    while (true) {
        try zio.sleep(.fromMilliseconds(10));
        if (body.terminalCause() != null) return error.Canceled;
    }
}

/// Per connection (the client says which in `x-conn`): the thread its last
/// task handler ran on, and how often a complete handler of the same
/// connection, which runs on the connection's actor, ran on another thread than
/// that.
const conns = 4;
var task_thread: [conns]std.atomic.Value(u64) = @splat(.init(0));
var actor_off_task_thread: std.atomic.Value(usize) = .init(0);
var tasks_run: std.atomic.Value(usize) = .init(0);

fn threadId() u64 {
    return @intCast(std.Thread.getCurrentId());
}

fn connIndex(req: *const starh2.Request) usize {
    for (req.headers) |h| {
        if (std.mem.eql(u8, h.name, "x-conn")) return std.fmt.parseInt(usize, h.value, 10) catch 0;
    }
    return 0;
}

fn onActor(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    const task = task_thread[connIndex(req)].load(.acquire);
    if (task != 0 and threadId() != task) _ = actor_off_task_thread.fetchAdd(1, .acq_rel);
    try resp.send(200, &.{}, "ok");
}

/// A task handler that takes request memory on whatever thread it runs on.
fn taskTakes(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    task_thread[connIndex(req)].store(threadId(), .release);
    _ = tasks_run.fetchAdd(1, .acq_rel);
    var list: std.ArrayList(u8) = .empty;
    const row: [100]u8 = @splat('t');
    while (list.items.len < 200 * 1024) try list.appendSlice(req.arena, &row);
    try resp.send(200, &.{}, "ok");
}

fn echoToken(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    for (req.headers) |h| {
        if (std.mem.eql(u8, h.name, "x-token")) return resp.send(200, &.{}, h.value);
    }
    try resp.send(404, &.{}, "no token");
}

fn postOk(_: *anyopaque, req: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    _ = req;
    try resp.send(200, &.{}, "ok");
}

fn route(method: starh2.Method, path: []const u8, comptime f: anytype) starh2.Route {
    const takes_task = @typeInfo(@TypeOf(f)).@"fn".param_types[2].? == *starh2.Response;
    if (takes_task) {
        return .{ .method = method, .path = path, .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = f } } };
    }
    return .{ .method = method, .path = path, .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = f } } };
}

const routes = [_]starh2.Route{
    route(.GET, "/hello", hello),
    route(.GET, "/big", takes1MiB),
    route(.GET, "/take100k", takes100KiB),
    route(.GET, "/render", renders300KiB),
    route(.GET, "/sse-big", sseThenTakes1MiB),
    route(.GET, "/hold100k", holds100KiB),
    route(.GET, "/hold", holdsFirstBlockOnly),
    route(.GET, "/hang", hangsHolding),
    route(.GET, "/task", taskTakes),
    route(.GET, "/actor", onActor),
    route(.GET, "/echo", echoToken),
    route(.POST, "/post", postOk),
};

// ---------------------------------------------------------------------------
// Client.

const Result = struct {
    status: ?u16 = null,
    /// RST_STREAM error code.
    rst: ?u32 = null,
    ended: bool = false,
    body: [64]u8 = undefined,
    body_len: usize = 0,

    fn bodyText(self: *const Result) []const u8 {
        return self.body[0..self.body_len];
    }
};

const Client = struct {
    gpa: std.mem.Allocator,
    stream: zio.net.Stream,
    inbuf: std.ArrayList(u8) = .empty,
    dec: hpack.Decoder,
    goaway: ?u32 = null,
    closed: bool = false,

    fn open(gpa: std.mem.Allocator, port: u16) !Client {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var self: Client = .{ .gpa = gpa, .stream = try peer.connect(.{}), .dec = hpack.Decoder.init(gpa) };
        errdefer self.deinit();
        var preface = try h2c.buildClientPrefaceAndSettings(gpa);
        defer preface.deinit(gpa);
        try self.send(preface.items);
        return self;
    }

    fn deinit(self: *Client) void {
        self.stream.close();
        self.inbuf.deinit(self.gpa);
        self.dec.deinit();
    }

    fn send(self: *Client, bytes: []const u8) !void {
        var off: usize = 0;
        while (off < bytes.len) off += try self.stream.write(bytes[off..], .none);
    }

    /// Read until stream `sid` ends or is reset, the server closes, or
    /// `timeout_ms` passes. Frames for other streams are decoded and dropped
    /// (their HEADERS too, so a client table would stay in step).
    fn await(self: *Client, sid: u31, timeout_ms: u64) !Result {
        var result: Result = .{};
        const deadline = nowNs() + timeout_ms * std.time.ns_per_ms;
        while (true) {
            while (self.inbuf.items.len >= frame.FRAME_HEADER_LEN) {
                const h = self.inbuf.items;
                const len = (@as(usize, h[0]) << 16) | (@as(usize, h[1]) << 8) | h[2];
                if (h.len < frame.FRAME_HEADER_LEN + len) break;
                const typ = h[3];
                const flags = h[4];
                const fsid: u31 = @intCast(std.mem.readInt(u32, h[5..9], .big) & 0x7fff_ffff);
                const payload = h[frame.FRAME_HEADER_LEN..][0..len];
                switch (typ) {
                    0x0 => if (fsid == sid) {
                        const room = result.body.len - result.body_len;
                        const take = @min(room, payload.len);
                        @memcpy(result.body[result.body_len..][0..take], payload[0..take]);
                        result.body_len += take;
                    },
                    0x1 => {
                        const decoded = try self.dec.decode(payload, 100, 32 * 1024, 256, 8 * 1024);
                        defer self.dec.freeResult(decoded);
                        if (fsid == sid and result.status == null) {
                            for (decoded.fields) |f| {
                                if (std.mem.eql(u8, f.name, ":status")) result.status = try std.fmt.parseInt(u16, f.value, 10);
                            }
                        }
                    },
                    0x3 => if (fsid == sid) {
                        result.rst = std.mem.readInt(u32, payload[0..4], .big);
                    },
                    0x7 => self.goaway = std.mem.readInt(u32, payload[4..8], .big),
                    else => {},
                }
                const finished = fsid == sid and ((typ == 0x0 or typ == 0x1) and flags & 1 != 0 or typ == 0x3);
                if (fsid == sid and (typ == 0x0 or typ == 0x1) and flags & 1 != 0) result.ended = true;
                self.inbuf.replaceRangeAssumeCapacity(0, frame.FRAME_HEADER_LEN + len, &.{});
                if (finished) return result;
            }
            if (self.closed or self.goaway != null) return result;
            if (nowNs() >= deadline) return result;
            var tmp: [16 * 1024]u8 = undefined;
            const n = self.stream.read(&tmp, .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
                error.Timeout => continue,
                else => {
                    self.closed = true;
                    continue;
                },
            };
            if (n == 0) {
                self.closed = true;
                continue;
            }
            try self.inbuf.appendSlice(self.gpa, tmp[0..n]);
        }
    }

    fn get(self: *Client, sid: u31, path: []const u8) !void {
        var req: std.ArrayList(u8) = .empty;
        defer req.deinit(self.gpa);
        try h2c.appendHeaders(self.gpa, &req, sid, path, true);
        try self.send(req.items);
    }

    fn getAndAwait(self: *Client, sid: u31, path: []const u8) !Result {
        try self.get(sid, path);
        return self.await(sid, 5_000);
    }

    /// Open a POST and send `len` bytes of body in frames of at most the
    /// default frame size. END_STREAM ends the last one when `end_stream`.
    fn post(self: *Client, sid: u31, len: usize, end_stream: bool) !void {
        var req: std.ArrayList(u8) = .empty;
        defer req.deinit(self.gpa);
        try h2c.appendHeadersExtra(self.gpa, &req, sid, "POST", "/post", false, &.{});
        const chunk: [frame.DEFAULT_MAX_FRAME_SIZE]u8 = @splat('b');
        var off: usize = 0;
        while (off < len) {
            const take = @min(len - off, chunk.len);
            try h2c.appendData(self.gpa, &req, sid, chunk[0..take], end_stream and off + take == len);
            off += take;
        }
        try self.send(req.items);
    }

    /// A HEADERS frame (END_STREAM, END_HEADERS) with a block built by hand.
    fn rawHeaders(self: *Client, sid: u31, block: []const u8) !void {
        var req: std.ArrayList(u8) = .empty;
        defer req.deinit(self.gpa);
        var hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = true },
            .stream_id = sid,
        };
        fh.encode(&hdr);
        try req.appendSlice(self.gpa, &hdr);
        try req.appendSlice(self.gpa, block);
        try self.send(req.items);
    }
};

// ---------------------------------------------------------------------------
// Server fixture.

fn smallLimits() starh2.Limits {
    var l = starh2.Limits.defaults;
    l.max_connections = 8;
    l.max_streams_per_connection = 16;
    l.max_streams_per_server = 32;
    l.cancellation_reaper_jobs = 32;
    l.cancellation_reaper_tasks = 2;
    l.graceful_drain_timeout_ns = 200 * std.time.ns_per_ms;
    l.preface_timeout_ns = 500 * std.time.ns_per_ms;
    return l;
}

fn Scenario(comptime run: fn (*starh2.Server, std.mem.Allocator, u16) anyerror!void) type {
    return struct {
        fn serve(rt: *zio.Runtime, gpa: std.mem.Allocator, limits: starh2.Limits) !void {
            const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
            var server = try starh2.Server.init(gpa, rt.io(), .{
                .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
                .routes = &routes,
                .tls = null,
                .limits = limits,
            });
            defer server.deinit(gpa);
            var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
            var serving = true;
            defer if (serving) {
                server.requestShutdown();
                serve_handle.join() catch {};
            };
            try server.waitUntilListening(5 * std.time.ns_per_s);
            hold_started.store(0, .release);
            hold_release.store(false, .release);
            try run(&server, gpa, server.localAddress(0).getPort());
            // Every way the scenario ended a request, the server is idle and
            // every block is home before it stops.
            try waitHome(&server);
            server.requestShutdown();
            serve_handle.join() catch {};
            serving = false;
            try std.testing.expectEqual(@as(usize, 0), server.requests.held_bytes.load(.acquire));
        }
    };
}

fn runScenario(comptime run: fn (*starh2.Server, std.mem.Allocator, u16) anyerror!void, scenario_limits: starh2.Limits) !void {
    // These scenarios set small server caps and count blocks, so they keep
    // every returned block resident (retain limit as large as the cap); the
    // retire path has its own unit tests.
    var limits = scenario_limits;
    limits.request_pool_retain_bytes = limits.request_bytes_per_server;
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 64, .prewarm = 16 },
        .executors = .exact(4),
    });
    defer rt.deinit();
    var handle = try rt.spawn(Scenario(run).serve, .{ rt, gpa, limits });
    try handle.join();
}

/// Wait until every block is back in the pool and no handler slot is held.
fn waitHome(server: *starh2.Server) !void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (true) {
        if (server.requests.held_bytes.load(.acquire) == 0 and
            conn_mod.test_observed_slots_in_use.load(.acquire) == 0) return;
        if (nowNs() >= deadline) {
            std.debug.print("request blocks not home: held={d} slots_in_use={d}\n", .{
                server.requests.held_bytes.load(.acquire),
                conn_mod.test_observed_slots_in_use.load(.acquire),
            });
            return error.BlocksNotHome;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

fn refused(server: *starh2.Server, reason: rp.Reason) u64 {
    return server.requests.snapshot().refused[@backingInt(reason)];
}

// ---------------------------------------------------------------------------
// The request's own cap.

fn perRequestCap(server: *starh2.Server, gpa: std.mem.Allocator, port: u16) !void {
    var c = try Client.open(gpa, port);
    defer c.deinit();

    // Before the response is committed: a 500, and the connection goes on.
    const big = try c.getAndAwait(1, "/big");
    try std.testing.expectEqual(@as(?u16, 500), big.status);
    try std.testing.expect(big.ended);
    try std.testing.expectEqual(@as(u64, 1), refused(server, .request_cap));
    const after = try c.getAndAwait(3, "/hello");
    try std.testing.expectEqual(@as(?u16, 200), after.status);

    // After it: the status is gone, so the stream is reset with
    // INTERNAL_ERROR (0x2). The SSE headers did reach the client.
    const sse = try c.getAndAwait(5, "/sse-big");
    try std.testing.expectEqual(@as(?u16, 200), sse.status);
    try std.testing.expectEqual(@as(?u32, 2), sse.rst);
    try std.testing.expectEqual(@as(u64, 2), refused(server, .request_cap));

    const still = try c.getAndAwait(7, "/hello");
    try std.testing.expectEqual(@as(?u16, 200), still.status);
    try std.testing.expectEqual(@as(u64, 0), refused(server, .server_cap));
    try std.testing.expectEqual(@as(u64, 0), refused(server, .connection_cap));
}

test "request cap: 500 before the response is committed, RST(INTERNAL_ERROR) after" {
    var l = smallLimits();
    l.request_body_bytes = 1024;
    l.request_arena_bytes = 256 * 1024;
    l.request_bytes_per_server = 4 * 1024 * 1024;
    l.request_bytes_per_connection = 1024 * 1024;
    try runScenario(perRequestCap, l);
}

// ---------------------------------------------------------------------------
// The server's cap, and a header block decoded without memory.

/// The HEADERS block of `GET /echo` with `x-token: abc123` added to the HPACK
/// table (literal with incremental indexing). Written out byte by byte, because
/// the repo's encoder never indexes.
const echo_indexing_block = [_]u8{
    0x82, // :method GET
    0x86, // :scheme http
    0x04, 0x05, '/', 'e', 'c', 'h', 'o', // :path /echo, literal, name index 4
    0x01, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's',  't', // :authority, name index 1
    0x40, 0x07, 'x', '-', 't', 'o', 'k', 'e', 'n', 0x06, 'a',
    'b',  'c',  '1', '2', '3',
};

/// The same request naming the table entry the block above made (the first
/// dynamic entry is index 62).
const echo_indexed_block = [_]u8{
    0x82,
    0x86,
    0x04,
    0x05,
    '/',
    'e',
    'c',
    'h',
    'o',
    0x01,
    0x09,
    'l',
    'o',
    'c',
    'a',
    'l',
    'h',
    'o',
    's',
    't',
    0x80 | 62,
};

fn serverCap(server: *starh2.Server, gpa: std.mem.Allocator, port: u16) !void {
    // Two connections, so the server's cap and not the connection's is what
    // refuses (the connection cap equals the server cap here).
    var a_conn = try Client.open(gpa, port);
    defer a_conn.deinit();
    var c = try Client.open(gpa, port);
    defer c.deinit();

    // A holds its first block and 100 KiB (a 128 KiB block): 192 KiB of the
    // 256 KiB the pool may have resident.
    try a_conn.get(1, "/hold100k");
    try waitCount(&hold_started, 1);

    // The other connection's first block fits (256 KiB resident, the cap),
    // but the 100 KiB it asks for next does not, and no free resident block
    // can be reclaimed: the handler's allocation fails, before its response: a
    // 503.
    const handler_refused = try c.getAndAwait(1, "/take100k");
    try std.testing.expectEqual(@as(?u16, 503), handler_refused.status);
    try std.testing.expectEqual(@as(u64, 1), refused(server, .server_cap));
    try std.testing.expectEqual(@as(u64, 0), refused(server, .connection_cap));

    // B takes the 64 KiB block that request gave back, and holds it. Now
    // every resident block is in use.
    try c.get(3, "/hold");
    try waitCount(&hold_started, 2);
    try std.testing.expectEqual(@as(usize, 256 * 1024), server.requests.snapshot().resident_bytes);

    // No block for a first block at all. The header block is still decoded,
    // into the connection's scratch, and it adds `x-token` to the table.
    try c.rawHeaders(5, &echo_indexing_block);
    const no_first_block = try c.await(5, 5_000);
    try std.testing.expectEqual(@as(?u16, 503), no_first_block.status);
    try std.testing.expect(no_first_block.ended);
    try std.testing.expectEqual(@as(u64, 2), refused(server, .server_cap));

    // A and B finish; their blocks come home.
    hold_release.store(true, .release);
    const a = try a_conn.await(1, 5_000);
    const b = try c.await(3, 5_000);
    try std.testing.expectEqual(@as(?u16, 200), a.status);
    try std.testing.expectEqual(@as(?u16, 200), b.status);

    // The refused request's block advanced the table: this request names the
    // entry, and the handler sees the right value. If the table had been left
    // behind, this would be a compression error and a closed connection.
    try c.rawHeaders(7, &echo_indexed_block);
    const echoed = try c.await(7, 5_000);
    try std.testing.expectEqual(@as(?u16, 200), echoed.status);
    try std.testing.expectEqualStrings("abc123", echoed.bodyText());
    try std.testing.expect(c.goaway == null);
}

test "server cap: a refused first block still decodes its header block, and answers 503" {
    var l = smallLimits();
    l.request_body_bytes = 1024;
    l.request_arena_bytes = 256 * 1024;
    l.request_bytes_per_server = 256 * 1024;
    l.request_bytes_per_connection = 256 * 1024;
    try runScenario(serverCap, l);
}

fn waitCount(counter: *std.atomic.Value(usize), n: usize) !void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (counter.load(.acquire) < n) {
        if (nowNs() >= deadline) return error.HandlerNeverStarted;
        try zio.sleep(.fromMilliseconds(5));
    }
}

// ---------------------------------------------------------------------------
// The connection's cap, on a body.

fn connectionCap(server: *starh2.Server, gpa: std.mem.Allocator, port: u16) !void {
    var c = try Client.open(gpa, port);
    defer c.deinit();

    // The connection may hold one 64 KiB block. A request's first block takes
    // all of it, and a 64,000-byte body does not fit what is left of that
    // block, so the append that needs a second block is refused. The stream is
    // answered; nothing is torn down.
    try c.post(1, 64_000, true);
    const refused_body = try c.await(1, 5_000);
    try std.testing.expectEqual(@as(?u16, 503), refused_body.status);
    try std.testing.expect(refused_body.ended);
    try std.testing.expectEqual(@as(u64, 1), refused(server, .connection_cap));
    try std.testing.expect(c.goaway == null);

    // The connection serves its next request, which takes the block again.
    const next = try c.getAndAwait(3, "/hello");
    try std.testing.expectEqual(@as(?u16, 200), next.status);
    // And a body that fits one block is accepted.
    try c.post(5, 20_000, true);
    const small = try c.await(5, 5_000);
    try std.testing.expectEqual(@as(?u16, 200), small.status);
    try std.testing.expectEqual(@as(u64, 1), refused(server, .connection_cap));
}

test "connection cap: a refused body append answers the stream and the connection serves the next" {
    var l = smallLimits();
    l.request_body_bytes = 64 * 1024;
    l.request_arena_bytes = 512 * 1024;
    l.request_bytes_per_server = 4 * 1024 * 1024;
    l.request_bytes_per_connection = 64 * 1024;
    try runScenario(connectionCap, l);
}

// ---------------------------------------------------------------------------
// The memory that builds a handler's job runs out.

fn dispatchRefused(server: *starh2.Server, gpa: std.mem.Allocator, port: u16) !void {
    var c = try Client.open(gpa, port);
    defer c.deinit();
    conn_mod.test_fail_dispatch_alloc.store(true, .release);
    defer conn_mod.test_fail_dispatch_alloc.store(false, .release);
    // Both handler kinds: the complete handler never spawns, the task handler
    // would. Each gets an answer and nothing is left held.
    const complete = try c.getAndAwait(1, "/hello");
    try std.testing.expectEqual(@as(?u16, 503), complete.status);
    const task = try c.getAndAwait(3, "/task");
    try std.testing.expectEqual(@as(?u16, 503), task.status);
    try waitHeldZero(server, "refused dispatch");
    conn_mod.test_fail_dispatch_alloc.store(false, .release);
    // The connection is intact and its slots were given back.
    for (0..40) |i| {
        const ok = try c.getAndAwait(@intCast(5 + 2 * i), "/hello");
        try std.testing.expectEqual(@as(?u16, 200), ok.status);
    }
    try std.testing.expect(c.goaway == null);
}

test "a request whose job cannot be built is answered 503 and its slot comes back" {
    var l = smallLimits();
    l.request_body_bytes = 1024;
    try runScenario(dispatchRefused, l);
}

// ---------------------------------------------------------------------------
// A header block that decodes to far more than the limits allow.

/// A table entry of `value_len` bytes, then `refs` one-byte references to it,
/// as one HEADERS block. The references decode to `refs * value_len` bytes.
fn bombBlocks(gpa: std.mem.Allocator, value_len: usize, refs: usize) !struct { seed: []u8, bomb: []u8 } {
    var seed: std.ArrayList(u8) = .empty;
    errdefer seed.deinit(gpa);
    try seed.appendSlice(gpa, &.{ 0x82, 0x86, 0x04, 0x01, '/', 0x01, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't' });
    try seed.appendSlice(gpa, &.{ 0x40, 0x04, 'x', '-', 'b', 'b' });
    // Value length, 7-bit prefix integer.
    var n = value_len;
    if (n < 127) {
        try seed.append(gpa, @intCast(n));
    } else {
        try seed.append(gpa, 127);
        n -= 127;
        while (n >= 128) : (n /= 128) try seed.append(gpa, @intCast(n % 128 + 128));
        try seed.append(gpa, @intCast(n));
    }
    try seed.appendNTimes(gpa, 'v', value_len);

    var bomb: std.ArrayList(u8) = .empty;
    errdefer bomb.deinit(gpa);
    try bomb.appendSlice(gpa, &.{ 0x82, 0x86, 0x04, 0x01, '/', 0x01, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't' });
    try bomb.appendNTimes(gpa, 0x80 | 62, refs);
    return .{ .seed = try seed.toOwnedSlice(gpa), .bomb = try bomb.toOwnedSlice(gpa) };
}

fn hpackBomb(server: *starh2.Server, gpa: std.mem.Allocator, port: u16) !void {
    var c = try Client.open(gpa, port);
    defer c.deinit();

    const blocks = try bombBlocks(gpa, 3000, 15_000);
    defer gpa.free(blocks.seed);
    defer gpa.free(blocks.bomb);
    try c.rawHeaders(1, blocks.seed);
    const seeded = try c.await(1, 5_000);
    try std.testing.expectEqual(@as(?u16, 404), seeded.status);

    // 15,000 references to a 3,000-byte entry: 45 MB if decoded in full. The
    // block is sent as HEADERS plus CONTINUATION frames, since it is larger
    // than one frame, within the 64 KiB the limits allow.
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    var off: usize = 0;
    var first = true;
    while (off < blocks.bomb.len) {
        const take = @min(blocks.bomb.len - off, frame.DEFAULT_MAX_FRAME_SIZE);
        const last = off + take == blocks.bomb.len;
        var hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(take),
            .type = if (first) .headers else .continuation,
            .flags = .{ .end_headers = last, .end_stream = first },
            .stream_id = 3,
        };
        fh.encode(&hdr);
        try wire.appendSlice(gpa, &hdr);
        try wire.appendSlice(gpa, blocks.bomb[off..][0..take]);
        off += take;
        first = false;
    }
    try c.send(wire.items);
    const bombed = try c.await(3, 5_000);
    // Refused as too large, the connection and its table intact.
    try std.testing.expectEqual(@as(?u16, 431), bombed.status);
    try std.testing.expect(c.goaway == null);
    const next = try c.getAndAwait(5, "/hello");
    try std.testing.expectEqual(@as(?u16, 200), next.status);
    // The decode never needed more than the first block: nothing was refused.
    try std.testing.expectEqual(@as(u64, 0), refused(server, .server_cap));
    try std.testing.expectEqual(@as(u64, 0), refused(server, .request_cap));
    try std.testing.expectEqual(@as(u64, 0), refused(server, .connection_cap));
}

test "a header block that decodes to tens of megabytes is a 431 and takes no more than a first block" {
    var l = smallLimits();
    l.request_body_bytes = 1024;
    l.request_arena_bytes = 256 * 1024;
    l.request_bytes_per_server = 1024 * 1024;
    l.request_bytes_per_connection = 256 * 1024;
    try runScenario(hpackBomb, l);
}

// ---------------------------------------------------------------------------
// Blocks come home.

fn waitHeldZero(server: *starh2.Server, what: []const u8) !void {
    const deadline = nowNs() + 5 * std.time.ns_per_s;
    while (server.requests.held_bytes.load(.acquire) != 0) {
        if (nowNs() >= deadline) {
            std.debug.print("{s}: {d} request bytes still held\n", .{ what, server.requests.held_bytes.load(.acquire) });
            return error.BlocksNotHome;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

fn blocksComeHome(server: *starh2.Server, gpa: std.mem.Allocator, port: u16) !void {
    // A normal end. The connection stays open for the next three cases, so
    // the blocks are shown home while it is, not because it closed.
    var c = try Client.open(gpa, port);
    defer c.deinit();
    const normal = try c.getAndAwait(1, "/render");
    try std.testing.expectEqual(@as(?u16, 200), normal.status);
    try waitHeldZero(server, "normal end");
    const used = server.requests.snapshot().held_high;
    try std.testing.expect(used >= 512 * 1024);

    // A reset in the middle of a body: HEADERS and two DATA frames, no
    // END_STREAM, then RST_STREAM. The request never dispatched.
    try c.post(3, 32 * 1024, false);
    {
        var rst: std.ArrayList(u8) = .empty;
        defer rst.deinit(gpa);
        try h2c.appendRst(gpa, &rst, 3);
        try zio.sleep(.fromMilliseconds(50));
        try c.send(rst.items);
    }
    try waitHeldZero(server, "reset mid-body");

    // A cancel: a task handler holds 100 KiB, then its stream is reset.
    try c.get(5, "/hang");
    try waitCount(&hold_started, 1);
    try std.testing.expect(server.requests.held_bytes.load(.acquire) >= 100 * 1024);
    {
        var rst: std.ArrayList(u8) = .empty;
        defer rst.deinit(gpa);
        try h2c.appendRst(gpa, &rst, 5);
        try c.send(rst.items);
    }
    try waitHeldZero(server, "cancel");

    // The connection is still the same one and still serves.
    const alive = try c.getAndAwait(7, "/hello");
    try std.testing.expectEqual(@as(?u16, 200), alive.status);

    // A connection error with requests in flight: two task handlers hold
    // memory and a body is half sent, then a frame no connection may receive.
    hold_started.store(0, .release);
    try c.get(9, "/hang");
    try c.get(11, "/hang");
    try waitCount(&hold_started, 2);
    try c.post(13, 20 * 1024, false);
    {
        // DATA on stream 0.
        var bad: std.ArrayList(u8) = .empty;
        defer bad.deinit(gpa);
        try h2c.appendData(gpa, &bad, 0, "x", false);
        try c.send(bad.items);
    }
    _ = try c.await(15, 5_000);
    try std.testing.expect(c.goaway != null);
    try waitHeldZero(server, "connection error");
}

test "request blocks come home after a normal end, a reset mid-body, a cancel and a connection error" {
    var l = smallLimits();
    l.request_body_bytes = 64 * 1024;
    try runScenario(blocksComeHome, l);
}

fn otherExecutor(server: *starh2.Server, gpa: std.mem.Allocator, port: u16) !void {
    for (&task_thread) |*t| t.store(0, .release);
    actor_off_task_thread.store(0, .release);
    tasks_run.store(0, .release);
    var clients: [conns]Client = undefined;
    var opened: usize = 0;
    defer for (clients[0..opened]) |*c| c.deinit();
    for (&clients) |*c| {
        c.* = try Client.open(gpa, port);
        opened += 1;
    }
    // Four connections, each alternating a task handler (spawned, on any
    // executor) with a complete handler (run on the connection's actor), so
    // every pair shows whether the handler that took the request memory ran on
    // another thread than the actor that gives it back.
    var sid: u31 = 1;
    const rounds = 24;
    for (0..rounds) |_| {
        for ([_][]const u8{ "/task", "/actor" }) |path| {
            for (&clients, 0..) |*c, ci| {
                var idx: [8]u8 = undefined;
                const extra = [_]hpack.HeaderField{.{ .name = "x-conn", .value = std.fmt.bufPrint(&idx, "{d}", .{ci}) catch unreachable }};
                var req: std.ArrayList(u8) = .empty;
                defer req.deinit(gpa);
                try h2c.appendHeadersExtra(gpa, &req, sid, "GET", path, true, &extra);
                try c.send(req.items);
            }
            for (&clients) |*c| {
                const r = try c.await(sid, 5_000);
                try std.testing.expectEqual(@as(?u16, 200), r.status);
            }
            sid += 2;
        }
    }
    try std.testing.expectEqual(@as(usize, rounds * conns), tasks_run.load(.acquire));
    // The scenario really crossed threads: this is a count of pairs whose
    // handler thread and actor thread differ. (Under work stealing the actor
    // may move between the two requests of a pair, so this is evidence that
    // the cross-thread case ran, not a measure of how often a handler does.)
    const crossed = actor_off_task_thread.load(.acquire);
    std.debug.print("task handlers and their connection's actor on different threads: {d} of {d} pairs\n", .{ crossed, rounds * conns });
    try std.testing.expect(crossed > 0);
    try waitHeldZero(server, "task handler on another executor");
    try std.testing.expect(server.requests.snapshot().held_high >= 256 * 1024);
}

test "request blocks come home after a task handler ran on another thread than its actor" {
    var l = smallLimits();
    l.request_body_bytes = 1024;
    try runScenario(otherExecutor, l);
}
