//! Gates for outbound memory (slot pool stage 3): the frame slabs and write
//! chunks every HTTP/2 frame is built in, with no general-allocator fallback.
//!
//! - A tiny write pool and many streams: every response completes, the drain
//!   was held back for lack of a chunk (a run where it never was proves
//!   nothing), and the held-back drain does not spin.
//! - A response header block above the 16 KiB frame size leaves as a HEADERS
//!   frame and CONTINUATION frames, with nothing between them, and decodes to
//!   the headers the handler set.
//!
//! - Many small responses on one connection: several frames share a write
//!   chunk (a frame of its own per chunk was the h2c kb=4 regression), and
//!   every response still arrives whole, HEADERS before DATA, in order.
//!
//! All run on h2c and on TLS, against a real server and a client that reads
//! the frames in wire order.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const wire_client = @import("wire_client.zig");

const frame = starh2.core.frame;
const hpack = starh2.core.hpack;
const conn_mod = starh2.edge.connection;
const wire_pump = starh2.edge.wire_pump;
const om = conn_mod.outbound_memory;

const Transport = wire_client.Transport;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;
const client_deadline_ns = wire_client.client_deadline_ns;

const dummy: u8 = 0;

const body_len = 60 * 1024;
const body_bytes: [body_len]u8 = @splat('b');

fn big(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, &body_bytes);
}

const packed_body_len = 4096;
const packed_body: [packed_body_len]u8 = @splat('p');

fn packedBody(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, &packed_body);
}

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const wide_value_len = 7000;
const wide_a: [wide_value_len]u8 = @splat('a');
const wide_b: [wide_value_len]u8 = @splat('b');
const wide_c: [wide_value_len]u8 = @splat('c');

/// Three headers of 7,000 bytes: a block of about 21 KiB, so one frame cannot
/// hold it.
fn wideHeaders(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{
        .{ .name = "x-wide-a", .value = &wide_a },
        .{ .name = "x-wide-b", .value = &wide_b },
        .{ .name = "x-wide-c", .value = &wide_c },
    }, "ok");
}

fn headers4(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{
        .{ .name = "x-1", .value = "a" },
        .{ .name = "x-2", .value = "b" },
        .{ .name = "x-3", .value = "c" },
        .{ .name = "x-4", .value = "d" },
    }, "four");
}

fn headers5(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{
        .{ .name = "x-1", .value = "a" },
        .{ .name = "x-2", .value = "b" },
        .{ .name = "x-3", .value = "c" },
        .{ .name = "x-4", .value = "d" },
        .{ .name = "x-5", .value = "e" },
    }, "five");
}

fn varyOrigin(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{
        .{ .name = "content-type", .value = "text/plain" },
        .{ .name = "vary", .value = "origin" },
    }, "hello vary");
}

fn sseOnce(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hi\n\n");
    try body.finish();
}

/// Fills the pump: writes until the connection stops it. The peer reads
/// nothing, so the socket fills and the write queue with it.
var bulk_started: std.atomic.Value(bool) = .init(false);

fn bulk(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    // HEADERS are out and no DATA exists yet: hold the pump here, from the
    // handler, so no body byte reaches a socket first. (Set from the test
    // thread, the bulk stream filled a small macOS socket before the flag was
    // set, and the pump sat in the kernel write, not in the hold.)
    wire_pump.test_hold_writes.store(true, .release);
    bulk_started.store(true, .release);
    const chunk: [16 * 1024]u8 = @splat('x');
    while (true) try body.writeAll(&chunk);
}

var flush_gate: std.atomic.Value(bool) = .init(false);
var flushers_started: std.atomic.Value(usize) = .init(0);

/// Starts its response, waits for the gate, then flushes with nothing pending:
/// each flush is a bare barrier, a write-queue slot with no pool chunk.
fn flusher(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    _ = flushers_started.fetchAdd(1, .acq_rel);
    while (!flush_gate.load(.acquire)) try zio.sleep(.fromMilliseconds(5));
    try body.flush();
    try body.flush();
    try body.finish();
}

/// `n` application headers, `x-0` ... , for the default field bound.
var many_headers: [100]starh2.Header = undefined;
var many_names: [100][8]u8 = undefined;
var many_names_len: [100]u8 = undefined;
var many_ready: std.atomic.Value(bool) = .init(false);

fn manyHeadersInit() void {
    if (many_ready.load(.acquire)) return;
    for (&many_headers, 0..) |*h, i| {
        const name = std.fmt.bufPrint(&many_names[i], "x-{d}", .{i}) catch unreachable;
        many_names_len[i] = @intCast(name.len);
        h.* = .{ .name = many_names[i][0..name.len], .value = "v" };
    }
    many_ready.store(true, .release);
}

fn headersN(comptime n: usize) fn (*anyopaque, *const starh2.Request, *starh2.CompleteResponse) anyerror!void {
    return struct {
        fn f(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
            manyHeadersInit();
            try resp.send(200, many_headers[0..n], "many");
        }
    }.f;
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/n33", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = headersN(33) } } },
    .{ .method = .GET, .path = "/n99", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = headersN(99) } } },
    .{ .method = .GET, .path = "/n100", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = headersN(100) } } },
    .{ .method = .GET, .path = "/bulk", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = bulk } } },
    .{ .method = .GET, .path = "/flusher", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = flusher } } },
    .{ .method = .GET, .path = "/h4", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = headers4 } } },
    .{ .method = .GET, .path = "/h5", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = headers5 } } },
    .{ .method = .GET, .path = "/vary", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = varyOrigin } } },
    .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseOnce } } },
    .{ .method = .GET, .path = "/big", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = big } } },
    .{ .method = .GET, .path = "/kb4", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = packedBody } } },
    .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    .{ .method = .GET, .path = "/wide", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = wideHeaders } } },
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
// Backpressure.

const streams = 64;

const Flood = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    /// Client -> test body: every request is on the wire, and the client now
    /// reads nothing until `read_now`.
    requested: std.atomic.Value(bool) = .init(false),
    read_now: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    /// Per request: DATA bytes received, and whether END_STREAM arrived.
    received: [streams]usize = @splat(0),
    ended: [streams]bool = @splat(false),
};

fn floodMain(f: *Flood) void {
    floodRun(f) catch |err| {
        std.debug.print("{s} backpressure client failed: {s}\n", .{ @tagName(f.transport), @errorName(err) });
        f.err = err;
    };
    f.done.store(true, .release);
}

fn floodRun(f: *Flood) !void {
    const w = try Wire.connectRecvBuf(f.gpa, f.transport, f.port, 4096);
    defer w.close();
    try w.handshake();

    var req = try h2c.buildClientPrefaceAndSettings(f.gpa);
    defer req.deinit(f.gpa);
    // The connection window is the default 65,535 bytes; open it for 64
    // bodies of 60 KiB so only the server's memory limits what it sends.
    try h2c.appendWindowUpdate(f.gpa, &req, 0, 1 << 24);
    for (0..streams) |i| try h2c.appendHeaders(f.gpa, &req, @intCast(1 + 2 * i), "/big", true);
    try w.send(req.items);
    f.requested.store(true, .release);

    // The peer reads nothing: the socket fills, the writer blocks, and the
    // write pool stays rented.
    const deadline = nowNs() + client_deadline_ns;
    while (!f.read_now.load(.acquire)) {
        if (nowNs() >= deadline) return error.Deadline;
        std.Thread.yield() catch {};
        sleepMs(1);
    }

    var open: usize = streams;
    while (open > 0) {
        const s = try w.next(nowNs() + client_deadline_ns) orelse return error.ClosedEarly;
        if (s.ftype == 3 or s.ftype == 7) return error.StreamResetOrGoaway;
        if (s.ftype != 0 and s.ftype != 1) continue;
        const idx = (s.sid - 1) / 2;
        if (idx >= streams) return error.UnknownStream;
        if (s.ftype == 0) f.received[idx] += w.last_payload.items.len;
        if (s.flags & 1 != 0) {
            if (f.ended[idx]) return error.EndedTwice;
            f.ended[idx] = true;
            open -= 1;
        }
    }
}

fn sleepMs(ms: u32) void {
    const t: std.c.timespec = .{ .sec = 0, .nsec = @as(isize, ms) * std.time.ns_per_ms };
    _ = std.c.nanosleep(&t, null);
}

fn runBackpressure(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var limits = starh2.Limits.defaults;
    // The smallest write pool the limits allow: one pop needs two free chunks
    // and a queued frame may take one only while more than two are free.
    limits.inbound_wire_chunks_per_connection = starh2.core.limits.MIN_WRITE_POOL_CHUNKS;
    var server = try serverFor(rt, gpa, transport, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    const pauses_0 = om.drain_memory_pauses.load(.monotonic);
    const refused_frames_0 = om.frames_refused.load(.monotonic);
    const refused_chunks_0 = om.chunks_refused.load(.monotonic);
    const built_0 = om.frames_built.load(.monotonic);
    const leased_0 = om.chunks_leased.load(.monotonic);

    var f: Flood = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort() };
    const t = try std.Thread.spawn(.{}, floodMain, .{&f});
    var joined = false;
    defer if (!joined) t.join();

    while (!f.requested.load(.acquire)) {
        if (f.err) |err| return err;
        zio.sleep(.fromMilliseconds(2)) catch {};
    }
    // Let the server fill the socket and stop, then watch it hold still.
    zio.sleep(.fromMilliseconds(300)) catch {};
    const stalled_turns_0 = om.drain_turns.load(.monotonic);
    const stalled_pauses_0 = om.drain_memory_pauses.load(.monotonic);
    zio.sleep(.fromMilliseconds(400)) catch {};
    const stalled_turns = om.drain_turns.load(.monotonic) - stalled_turns_0;
    const stalled_pauses = om.drain_memory_pauses.load(.monotonic) - stalled_pauses_0;

    // The drain was already held back while the peer read nothing: the stall
    // is a memory stall, not a socket that took everything.
    const pauses_while_stalled = om.drain_memory_pauses.load(.monotonic) - pauses_0;
    f.read_now.store(true, .release);
    const deadline = nowNs() + client_deadline_ns;
    while (!f.done.load(.acquire)) {
        if (nowNs() >= deadline) return error.Deadline;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    t.join();
    joined = true;
    if (f.err) |err| return err;

    const pauses = om.drain_memory_pauses.load(.monotonic) - pauses_0;
    const built = om.frames_built.load(.monotonic) - built_0;
    const leased = om.chunks_leased.load(.monotonic) - leased_0;
    std.debug.print("{s} backpressure: {d} streams of {d} bytes; frames built {d}, chunks leased {d}, pops held back {d}; while the peer read nothing for 400 ms: {d} drain turns, {d} hold-backs\n", .{
        @tagName(transport), streams, body_len, built, leased, pauses, stalled_turns, stalled_pauses,
    });
    for (f.received, f.ended) |got, ended| {
        try std.testing.expect(ended);
        try std.testing.expectEqual(@as(usize, body_len), got);
    }
    // The instrument ran: frames were built and chunks leased, and the drain
    // really was held back for lack of a chunk. Without that the rest proves
    // only that a server with a big pool answers.
    try std.testing.expect(built >= streams * 4);
    try std.testing.expect(leased > 0);
    try std.testing.expect(pauses > 0);
    try std.testing.expect(pauses_while_stalled > 0);
    // Nothing fell back and nothing was refused.
    try std.testing.expectEqual(@as(u64, 0), om.frames_refused.load(.monotonic) - refused_frames_0);
    try std.testing.expectEqual(@as(u64, 0), om.chunks_refused.load(.monotonic) - refused_chunks_0);
    // A drain held back for memory waits for a write completion. While the
    // peer read nothing there was none, so the actor turned a few times (its
    // deadlines) and did not spin: a spin is millions of turns.
    try std.testing.expect(stalled_turns <= 64);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runBackpressureCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runBackpressure, .{ rt, gpa, transport });
    try h.join();
}

test "backpressure: h2c with a three-chunk write pool, 64 streams all complete and the held-back drain does not spin" {
    try runBackpressureCase(.h2c);
}

test "backpressure: tls with a three-chunk write pool, 64 streams all complete and the held-back drain does not spin" {
    try runBackpressureCase(.tls);
}

// ---------------------------------------------------------------------------
// CONTINUATION.

const Continuation = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    /// What the wide response's block looked like on the wire.
    headers_frames: usize = 0,
    continuation_frames: usize = 0,
    block_len: usize = 0,
    decoded_wide: usize = 0,
    /// Streams of the small requests sent beside it that completed.
    small_done: usize = 0,
};

const small_streams = 24;

fn continuationMain(c: *Continuation) void {
    continuationRun(c) catch |err| {
        std.debug.print("{s} continuation client failed: {s}\n", .{ @tagName(c.transport), @errorName(err) });
        c.err = err;
    };
    c.done.store(true, .release);
}

fn continuationRun(c: *Continuation) !void {
    const w = try Wire.open(c.gpa, c.transport, c.port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(c.gpa);
    defer req.deinit(c.gpa);
    // Stream 1 is the wide response; the others are small ones that the
    // scheduler has every reason to slip between its fragments.
    try h2c.appendHeaders(c.gpa, &req, 1, "/wide", true);
    for (0..small_streams) |i| try h2c.appendHeaders(c.gpa, &req, @intCast(3 + 2 * i), "/hello", true);
    try w.send(req.items);

    var block: std.ArrayList(u8) = .empty;
    defer block.deinit(c.gpa);
    // The stream whose header block is open, or 0.
    var open_block: u32 = 0;
    var wide_done = false;
    var small_left: usize = small_streams;
    while (!wide_done or small_left > 0) {
        const s = try w.next(nowNs() + client_deadline_ns) orelse return error.ClosedEarly;
        if (open_block != 0) {
            // RFC 9113 section 6.10: only CONTINUATION of that stream.
            if (s.ftype != 9 or s.sid != open_block) return error.FrameInsideHeaderBlock;
        }
        switch (s.ftype) {
            3, 7 => return error.StreamResetOrGoaway,
            1 => {
                if (s.sid == 1) {
                    c.headers_frames += 1;
                    try block.appendSlice(c.gpa, w.last_payload.items);
                    if (s.flags & 4 == 0) open_block = 1;
                }
            },
            9 => {
                if (s.sid != 1) return error.ContinuationOnWrongStream;
                c.continuation_frames += 1;
                try block.appendSlice(c.gpa, w.last_payload.items);
                if (s.flags & 4 != 0) open_block = 0;
            },
            0 => if (s.flags & 1 != 0) {
                if (s.sid == 1) wide_done = true else small_left -= 1;
            },
            else => {},
        }
    }
    c.block_len = block.items.len;
    c.small_done = small_streams - small_left;

    var dec = try hpack.Decoder.init(c.gpa);
    defer dec.deinit();
    const res = try dec.decode(block.items, 100, 64 * 1024, 256, 16 * 1024);
    defer dec.freeResult(res);
    for (res.fields) |hf| {
        if (std.mem.eql(u8, hf.name, "x-wide-a") and std.mem.eql(u8, hf.value, &wide_a)) c.decoded_wide += 1;
        if (std.mem.eql(u8, hf.name, "x-wide-b") and std.mem.eql(u8, hf.value, &wide_b)) c.decoded_wide += 1;
        if (std.mem.eql(u8, hf.name, "x-wide-c") and std.mem.eql(u8, hf.value, &wide_c)) c.decoded_wide += 1;
    }
}

fn runContinuation(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var server = try serverFor(rt, gpa, transport, starh2.Limits.defaults, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var c: Continuation = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort() };
    const t = try std.Thread.spawn(.{}, continuationMain, .{&c});
    const deadline = nowNs() + client_deadline_ns;
    while (!c.done.load(.acquire)) {
        if (nowNs() >= deadline) {
            t.join();
            return error.Deadline;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    t.join();
    if (c.err) |err| return err;
    std.debug.print("{s} continuation: block {d} bytes in 1 HEADERS + {d} CONTINUATION frames; {d} of 3 wide headers decoded; {d} small responses beside it\n", .{
        @tagName(transport), c.block_len, c.continuation_frames, c.decoded_wide, c.small_done,
    });
    try std.testing.expectEqual(@as(usize, 1), c.headers_frames);
    // 21 KiB of headers need a second frame; the instrument ran only if one
    // CONTINUATION was seen.
    try std.testing.expect(c.block_len > frame.DEFAULT_MAX_FRAME_SIZE);
    try std.testing.expectEqual((c.block_len + frame.DEFAULT_MAX_FRAME_SIZE - 1) / frame.DEFAULT_MAX_FRAME_SIZE, 1 + c.continuation_frames);
    try std.testing.expectEqual(@as(usize, 3), c.decoded_wide);
    try std.testing.expectEqual(@as(usize, small_streams), c.small_done);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runContinuationCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runContinuation, .{ rt, gpa, transport });
    try h.join();
}

test "continuation: h2c a response header block above 16 KiB leaves as HEADERS and CONTINUATION with nothing between" {
    try runContinuationCase(.h2c);
}

test "continuation: tls a response header block above 16 KiB leaves as HEADERS and CONTINUATION with nothing between" {
    try runContinuationCase(.tls);
}

// ---------------------------------------------------------------------------
// The response header bound, and the header lists the edge builds.

const bound_paths = [_][]const u8{ "/h4", "/h5", "/vary", "/sse", "/h4" };

const Answer = struct {
    status: u16 = 0,
    ended: bool = false,
    vary: [64]u8 = undefined,
    vary_len: usize = 0,
    content_type: [32]u8 = undefined,
    content_type_len: usize = 0,
    fields: usize = 0,
    body: [32]u8 = undefined,
    body_len: usize = 0,

    fn varyValue(self: *const Answer) []const u8 {
        return self.vary[0..self.vary_len];
    }
    fn contentType(self: *const Answer) []const u8 {
        return self.content_type[0..self.content_type_len];
    }
    fn bodyValue(self: *const Answer) []const u8 {
        return self.body[0..self.body_len];
    }
};

const Bound = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    answers: [bound_paths.len]Answer = @splat(.{}),
};

fn boundMain(b: *Bound) void {
    boundRun(b) catch |err| {
        std.debug.print("{s} header bound client failed: {s}\n", .{ @tagName(b.transport), @errorName(err) });
        b.err = err;
    };
    b.done.store(true, .release);
}

fn boundRun(b: *Bound) !void {
    const w = try Wire.open(b.gpa, b.transport, b.port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(b.gpa);
    defer req.deinit(b.gpa);
    for (bound_paths, 0..) |path, i| try h2c.appendHeaders(b.gpa, &req, @intCast(1 + 2 * i), path, true);
    try w.send(req.items);

    var dec = try hpack.Decoder.init(b.gpa);
    defer dec.deinit();
    var open: usize = bound_paths.len;
    while (open > 0) {
        const s = try w.next(nowNs() + client_deadline_ns) orelse return error.ClosedEarly;
        if (s.ftype == 3 or s.ftype == 7) return error.StreamResetOrGoaway;
        if (s.ftype != 0 and s.ftype != 1) continue;
        const idx = (s.sid - 1) / 2;
        if (idx >= bound_paths.len) return error.UnknownStream;
        const a = &b.answers[idx];
        if (s.ftype == 1) {
            if (s.flags & 4 == 0) return error.UnexpectedContinuation;
            const res = try dec.decode(w.last_payload.items, 100, 64 * 1024, 256, 16 * 1024);
            defer dec.freeResult(res);
            a.fields = res.fields.len;
            for (res.fields) |hf| {
                if (std.mem.eql(u8, hf.name, ":status")) a.status = try std.fmt.parseInt(u16, hf.value, 10);
                if (std.mem.eql(u8, hf.name, "vary")) {
                    a.vary_len = @min(hf.value.len, a.vary.len);
                    @memcpy(a.vary[0..a.vary_len], hf.value[0..a.vary_len]);
                }
                if (std.mem.eql(u8, hf.name, "content-type")) {
                    a.content_type_len = @min(hf.value.len, a.content_type.len);
                    @memcpy(a.content_type[0..a.content_type_len], hf.value[0..a.content_type_len]);
                }
            }
        } else {
            const room = a.body.len - a.body_len;
            const take = @min(room, w.last_payload.items.len);
            @memcpy(a.body[a.body_len..][0..take], w.last_payload.items[0..take]);
            a.body_len += take;
        }
        if (s.flags & 1 != 0 and !a.ended) {
            a.ended = true;
            open -= 1;
        }
    }
}

fn runBound(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var limits = starh2.Limits.defaults;
    // `:status` and four more: /h4 is at the bound and /h5 is one over.
    limits.response_header_fields = 5;
    limits.response_compression = true;
    var server = try serverFor(rt, gpa, transport, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    const lists_0 = conn_mod.h2_work.response_header_lists.load(.monotonic);
    var b: Bound = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort() };
    const t = try std.Thread.spawn(.{}, boundMain, .{&b});
    const deadline = nowNs() + client_deadline_ns;
    while (!b.done.load(.acquire)) {
        if (nowNs() >= deadline) {
            t.join();
            return error.Deadline;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    t.join();
    if (b.err) |err| return err;

    const lists = conn_mod.h2_work.response_header_lists.load(.monotonic) - lists_0;
    std.debug.print("{s} header bound: /h4 {d} ({d} fields), /h5 {d} ({s}), /vary '{s}', /sse {s}, /h4 again {d}; header lists prepared {d}\n", .{
        @tagName(transport),
        b.answers[0].status,
        b.answers[0].fields,
        b.answers[1].status,
        b.answers[1].bodyValue(),
        b.answers[2].varyValue(),
        b.answers[3].contentType(),
        b.answers[4].status,
        lists,
    });
    // At the bound: sent, with its five fields.
    try std.testing.expectEqual(@as(u16, 200), b.answers[0].status);
    try std.testing.expectEqual(@as(usize, 5), b.answers[0].fields);
    try std.testing.expectEqualStrings("four", b.answers[0].bodyValue());
    // One over: refused before anything was queued, so a plain 500 answers it.
    try std.testing.expectEqual(@as(u16, 500), b.answers[1].status);
    try std.testing.expectEqualStrings("internal error", b.answers[1].bodyValue());
    // The list the edge builds for compression-eligible responses: the
    // handler's Vary merged with accept-encoding, and the SSE fields.
    try std.testing.expectEqual(@as(u16, 200), b.answers[2].status);
    try std.testing.expectEqualStrings("origin, accept-encoding", b.answers[2].varyValue());
    try std.testing.expectEqual(@as(u16, 200), b.answers[3].status);
    try std.testing.expectEqualStrings("text/event-stream", b.answers[3].contentType());
    try std.testing.expectEqualStrings("accept-encoding", b.answers[3].varyValue());
    // And the connection served on.
    try std.testing.expectEqual(@as(u16, 200), b.answers[4].status);
    // Five responses built a list each, and the refused one built a second for
    // its 500.
    try std.testing.expectEqual(@as(u64, bound_paths.len + 1), lists);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runBoundCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runBound, .{ rt, gpa, transport });
    try h.join();
}

test "response header bound: a response at response_header_fields is sent and one field over is a 500 before commit (h2c)" {
    try runBoundCase(.h2c);
}

test "response header bound: a response at response_header_fields is sent and one field over is a 500 before commit (tls)" {
    try runBoundCase(.tls);
}

// ---------------------------------------------------------------------------
// Bare flush barriers against a peer that reads nothing.

const flusher_streams = 12;

/// A bare flush barrier takes a write-queue slot and no pool chunk. The pump
/// is stuck, the queue fills, and flushes with nothing pending arrive: each
/// must wait for its slot outside `session_mu` (a push that waited in `putOne`
/// held the lock, and the actor could not run a deadline or end the connection
/// gracefully). The tests asks for both: the slow-consumer deadline resets the
/// stuck bulk stream, and a graceful shutdown ends the connection.
///
/// The pump is held by `wire_pump.test_hold_writes`, not by a full socket. A
/// socket fills at a size the kernel picks (a Linux loopback pair takes
/// megabytes, macOS a few KiB), and a test that needs the pump stuck cannot
/// depend on that: on Linux the 64 KiB bulk stream fitted into the socket
/// buffers, the pump never blocked, the barriers never waited, and the test
/// failed at its result. Here the test asserts the stuck pump itself, before
/// the first barrier (the pump acknowledges it is in the hold), and fails
/// there on any OS where it did not happen.
fn runBarrierQueue(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var limits = starh2.Limits.defaults;
    limits.slow_consumer_timeout_ns = 600 * std.time.ns_per_ms;
    limits.graceful_drain_timeout_ns = 500 * std.time.ns_per_ms;
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    var server = try serverFor(rt, gpa, .h2c, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    flush_gate.store(false, .release);
    flushers_started.store(0, .release);
    defer flush_gate.store(true, .release);
    defer wire_pump.test_hold_writes.store(false, .release);
    const waits_0 = if (comptime conn_mod.test_observe and @hasDecl(conn_mod, "test_ack_room_waits")) conn_mod.test_ack_room_waits.load(.monotonic) else 0;
    const held_0 = wire_pump.test_hold_entered.load(.acquire);
    bulk_started.store(false, .release);

    const w = try Wire.connectRecvBuf(gpa, .h2c, server.localAddress(0).getPort(), 4096);
    var wire_open = true;
    defer if (wire_open) w.close();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    // The connection window is the default 65,535: open it, so only the pump
    // stops the bulk stream.
    try h2c.appendWindowUpdate(gpa, &req, 0, 1 << 30);
    // The flushers go first and start their responses while the pump is free;
    // their HEADERS then need no pool chunk the bulk stream has taken.
    for (0..flusher_streams) |i| try h2c.appendHeaders(gpa, &req, @intCast(1 + 2 * i), "/flusher", true);
    try w.send(req.items);
    const started_deadline = nowNs() + client_deadline_ns;
    while (flushers_started.load(.acquire) < flusher_streams) {
        if (nowNs() >= started_deadline) return error.FlushersNeverStarted;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }

    // The bulk stream starts while the pump is free (a response waits for its
    // HEADERS to be written before it sends a body); its handler then holds
    // the pump (see `bulk`) before writing a byte, and the stream fills the
    // pool and the queue behind it. Its stream window is opened too (65,535
    // bytes by default would stop it before it filled anything).
    const pauses_before_bulk = om.drain_memory_pauses.load(.monotonic);
    const bulk_sid: u31 = 1 + 2 * flusher_streams;
    var bulk_req: std.ArrayList(u8) = .empty;
    defer bulk_req.deinit(gpa);
    try h2c.appendHeaders(gpa, &bulk_req, bulk_sid, "/bulk", true);
    try h2c.appendWindowUpdate(gpa, &bulk_req, bulk_sid, 1 << 30);
    try w.send(bulk_req.items);
    const bulk_deadline = nowNs() + client_deadline_ns;
    while (!bulk_started.load(.acquire)) {
        if (nowNs() >= bulk_deadline) return error.BulkNeverStarted;
        zio.sleep(.fromMilliseconds(1)) catch {};
    }

    // The precondition, asserted before any barrier. First, the pump says it
    // is in the hold: it has taken chunks and writes none. The flag alone is
    // not that (the pump may read it after the barriers have drained), and
    // "no write finished for a while" is not that either. Then the bulk
    // stream has filled the pool and the queue (a drain was held back for
    // room after the bulk stream began), and still nothing finishes while we watch.
    const hold_deadline = nowNs() + client_deadline_ns;
    while (wire_pump.test_hold_entered.load(.acquire) == held_0) {
        if (nowNs() >= hold_deadline) return error.PumpNeverHeld;
        zio.sleep(.fromMilliseconds(1)) catch {};
    }
    const deadline = nowNs() + client_deadline_ns;
    while (om.drain_memory_pauses.load(.monotonic) == pauses_before_bulk) {
        if (nowNs() >= deadline) return error.ConnectionNeverFilled;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    const finished_held = wire_pump.diag_acks.posted_release.load(.monotonic);
    zio.sleep(.fromMilliseconds(200)) catch {};
    if (wire_pump.diag_acks.posted_release.load(.monotonic) != finished_held) return error.PumpNotHeld;
    const streams_before = server.accounting.active_streams.load(.acquire);
    try std.testing.expectEqual(@as(usize, flusher_streams + 1), streams_before);

    flush_gate.store(true, .release);
    // The deadline: the actor runs and resets the bulk stream, which the peer
    // is not reading. (The flushers wait on tickets the stuck pump cannot
    // complete, and stay.)
    const slow_deadline = nowNs() + 5 * std.time.ns_per_s;
    var actor_alive = false;
    while (nowNs() < slow_deadline) {
        if (server.accounting.active_streams.load(.acquire) < streams_before) {
            actor_alive = true;
            break;
        }
        zio.sleep(.fromMilliseconds(20)) catch {};
    }
    if (!actor_alive) {
        // Let the stuck server go before reporting, or cleanup would hang.
        w.close();
        wire_open = false;
        return error.ActorStuckBehindABarrier;
    }
    // Only the bulk stream was reset: the connection is alive and its flushers
    // are waiting. A connection that failed closed would also free its
    // streams, and would not prove the actor was free.
    zio.sleep(.fromMilliseconds(150)) catch {};
    try std.testing.expectEqual(@as(usize, 1), server.active_connections.load(.acquire));
    try std.testing.expectEqual(@as(usize, flusher_streams), server.accounting.active_streams.load(.acquire));
    if (comptime conn_mod.test_observe and @hasDecl(conn_mod, "test_ack_room_waits")) {
        // The barriers really waited: without a wait there was nothing to test.
        try std.testing.expect(conn_mod.test_ack_room_waits.load(.monotonic) > waits_0);
    }
    if (comptime conn_mod.test_observe and @hasDecl(conn_mod, "test_queue_push_refusals")) {
        try std.testing.expectEqual(@as(usize, 0), conn_mod.test_queue_push_refusals.load(.monotonic));
    }

    // And a graceful shutdown ends the connection although the peer reads
    // nothing.
    server.requestShutdown();
    const shutdown_deadline = nowNs() + 8 * std.time.ns_per_s;
    while (server.active_connections.load(.acquire) != 0) {
        if (nowNs() >= shutdown_deadline) {
            w.close();
            wire_open = false;
            return error.ShutdownNeverEnded;
        }
        zio.sleep(.fromMilliseconds(20)) catch {};
    }
    serve_handle.join() catch {};
    serving = false;
}

test "bare flush barriers against a non-reading peer wait outside the lock: a deadline and a graceful shutdown still complete" {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runBarrierQueue, .{ rt, gpa });
    h.join() catch |err| {
        std.debug.print("barrier queue test: {s}\n", .{@errorName(err)});
        return err;
    };
}

// ---------------------------------------------------------------------------
// The smallest frame pool that validates also boots.

const Smallest = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    saw_settings: bool = false,
    saw_hello: bool = false,
    saw_goaway: bool = false,
};

fn smallestMain(c: *Smallest) void {
    smallestRun(c) catch |err| {
        c.err = err;
    };
    c.done.store(true, .release);
}

fn smallestRun(c: *Smallest) !void {
    const w = try Wire.open(c.gpa, c.transport, c.port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(c.gpa);
    defer req.deinit(c.gpa);
    try h2c.appendHeaders(c.gpa, &req, 1, "/hello", true);
    try w.send(req.items);
    const deadline = nowNs() + client_deadline_ns;
    while (!c.saw_hello) {
        const s = try w.next(deadline) orelse return error.ClosedEarly;
        if (s.ftype == 4 and s.flags & 1 == 0) c.saw_settings = true;
        if (s.ftype == 0 and s.sid == 1 and s.flags & 1 != 0) c.saw_hello = true;
    }
    // DATA on stream 0 is a connection error: a GOAWAY, a terminal frame that
    // may take the reserve slab, and then the close.
    var bad: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{ .length = 0, .type = .data, .flags = .{}, .stream_id = 0 }).encode(&bad);
    try w.send(&bad);
    while (true) {
        const s = try w.next(deadline) orelse break;
        if (s.ftype == 7) c.saw_goaway = true;
    }
}

fn runSmallestPools(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var limits = starh2.Limits.defaults;
    limits.frame_slab_bytes = frame.max_control_frame_bytes;
    limits.frame_slabs_per_connection = 4;
    limits.inbound_wire_chunks_per_connection = starh2.core.limits.MIN_WRITE_POOL_CHUNKS;
    var server = try serverFor(rt, gpa, transport, limits, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var c: Smallest = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort() };
    const t = try std.Thread.spawn(.{}, smallestMain, .{&c});
    const deadline = nowNs() + client_deadline_ns;
    while (!c.done.load(.acquire)) {
        if (nowNs() >= deadline) {
            t.join();
            return error.Deadline;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    t.join();
    if (c.err) |err| return err;
    try std.testing.expect(c.saw_settings);
    try std.testing.expect(c.saw_hello);
    try std.testing.expect(c.saw_goaway);
    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runSmallestPoolsCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runSmallestPools, .{ rt, gpa, transport });
    h.join() catch |err| {
        std.debug.print("{s} smallest pools: {s}\n", .{ @tagName(transport), @errorName(err) });
        return err;
    };
}

test "smallest frame pool (45-byte slabs, 4 of them, 3 chunks): a connection boots, answers, and ends with a GOAWAY (h2c)" {
    try runSmallestPoolsCase(.h2c);
}

test "smallest frame pool (45-byte slabs, 4 of them, 3 chunks): a connection boots, answers, and ends with a GOAWAY (tls)" {
    try runSmallestPoolsCase(.tls);
}

// ---------------------------------------------------------------------------
// The default response header bound: 100 fields, `:status` included.

const many_paths = [_][]const u8{ "/n33", "/n99", "/n100" };

const Many = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    status: [many_paths.len]u16 = @splat(0),
    fields: [many_paths.len]usize = @splat(0),
    body: [many_paths.len][16]u8 = undefined,
    body_len: [many_paths.len]usize = @splat(0),
};

fn manyMain(m: *Many) void {
    manyRun(m) catch |err| {
        std.debug.print("{s} many headers client failed: {s}\n", .{ @tagName(m.transport), @errorName(err) });
        m.err = err;
    };
    m.done.store(true, .release);
}

fn manyRun(m: *Many) !void {
    const w = try Wire.open(m.gpa, m.transport, m.port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(m.gpa);
    defer req.deinit(m.gpa);
    for (many_paths, 0..) |path, i| try h2c.appendHeaders(m.gpa, &req, @intCast(1 + 2 * i), path, true);
    try w.send(req.items);
    var dec = try hpack.Decoder.init(m.gpa);
    defer dec.deinit();
    var open: usize = many_paths.len;
    var ended: [many_paths.len]bool = @splat(false);
    while (open > 0) {
        const s = try w.next(nowNs() + client_deadline_ns) orelse return error.ClosedEarly;
        if (s.ftype == 3 or s.ftype == 7) return error.StreamResetOrGoaway;
        if (s.ftype != 0 and s.ftype != 1) continue;
        const idx = (s.sid - 1) / 2;
        if (idx >= many_paths.len) return error.UnknownStream;
        if (s.ftype == 1) {
            if (s.flags & 4 == 0) return error.UnexpectedContinuation;
            const res = try dec.decode(w.last_payload.items, 256, 64 * 1024, 256, 16 * 1024);
            defer dec.freeResult(res);
            m.fields[idx] = res.fields.len;
            for (res.fields) |hf| {
                if (std.mem.eql(u8, hf.name, ":status")) m.status[idx] = try std.fmt.parseInt(u16, hf.value, 10);
            }
        } else {
            const take = @min(m.body[idx].len - m.body_len[idx], w.last_payload.items.len);
            @memcpy(m.body[idx][m.body_len[idx]..][0..take], w.last_payload.items[0..take]);
            m.body_len[idx] += take;
        }
        if (s.flags & 1 != 0 and !ended[idx]) {
            ended[idx] = true;
            open -= 1;
        }
    }
}

fn runMany(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    // The DEFAULT limits: 100 fields. The old stack of 32 refused 33.
    var server = try serverFor(rt, gpa, transport, starh2.Limits.defaults, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    var m: Many = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort() };
    const t = try std.Thread.spawn(.{}, manyMain, .{&m});
    const deadline = nowNs() + client_deadline_ns;
    while (!m.done.load(.acquire)) {
        if (nowNs() >= deadline) {
            t.join();
            return error.Deadline;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    t.join();
    if (m.err) |err| return err;
    std.debug.print("{s} many headers: 33 -> {d} ({d} fields), 99 -> {d} ({d} fields), 100 -> {d} ({s})\n", .{
        @tagName(transport), m.status[0], m.fields[0], m.status[1], m.fields[1], m.status[2], m.body[2][0..m.body_len[2]],
    });
    // 33 and 99 application headers (34 and 100 fields with :status): sent.
    try std.testing.expectEqual(@as(u16, 200), m.status[0]);
    try std.testing.expectEqual(@as(usize, 34), m.fields[0]);
    try std.testing.expectEqual(@as(u16, 200), m.status[1]);
    try std.testing.expectEqual(@as(usize, 100), m.fields[1]);
    // 100 application headers are 101 fields: over the bound, refused before
    // anything was queued, so a plain 500 answers it.
    try std.testing.expectEqual(@as(u16, 500), m.status[2]);
    try std.testing.expectEqualStrings("internal error", m.body[2][0..m.body_len[2]]);
    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runManyCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runMany, .{ rt, gpa, transport });
    try h.join();
}

test "default response header bound: 33 and 99 application headers are sent, 100 (101 fields) is a 500 (h2c)" {
    try runManyCase(.h2c);
}

test "default response header bound: 33 and 99 application headers are sent, 100 (101 fields) is a 500 (tls)" {
    try runManyCase(.tls);
}

// ---------------------------------------------------------------------------
// Small frames share write chunks.

const pack_waves = 3;
const pack_streams = 100;

const Pack = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    port: u16,
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
    data_frames: usize = 0,
};

fn packMain(p: *Pack) void {
    packRun(p) catch |err| {
        std.debug.print("{s} packing client failed: {s}\n", .{ @tagName(p.transport), @errorName(err) });
        p.err = err;
    };
    p.done.store(true, .release);
}

fn packRun(p: *Pack) !void {
    // A small receive buffer and a pause before each wave is read: the socket
    // fills, the write pool runs dry, and the responses queue up in the
    // scheduler. They then leave from the main loop's drain, with no receipt
    // to carry: the unticketed DATA that used to take a chunk each.
    const w = try Wire.connectRecvBuf(p.gpa, p.transport, p.port, 4096);
    defer w.close();
    try w.handshake();
    var preface = try h2c.buildClientPrefaceAndSettings(p.gpa);
    defer preface.deinit(p.gpa);
    // The default connection window is 65,535 bytes, 16 of these bodies. Open
    // it so only the server's memory limits what it sends.
    try h2c.appendWindowUpdate(p.gpa, &preface, 0, 1 << 24);
    try w.send(preface.items);

    for (0..pack_waves) |wave| {
        var req: std.ArrayList(u8) = .empty;
        defer req.deinit(p.gpa);
        const first: u31 = @intCast(1 + 2 * wave * pack_streams);
        for (0..pack_streams) |i| try h2c.appendHeaders(p.gpa, &req, first + @as(u31, @intCast(2 * i)), "/kb4", true);
        try w.send(req.items);
        sleepMs(400);

        var headers_at: [pack_streams]?usize = @splat(null);
        var data_at: [pack_streams]?usize = @splat(null);
        var got: [pack_streams]usize = @splat(0);
        var seen: usize = 0;
        var open: usize = pack_streams;
        while (open > 0) {
            const s = try w.next(nowNs() + client_deadline_ns) orelse return error.ClosedEarly;
            if (s.ftype == 3 or s.ftype == 7) return error.StreamResetOrGoaway;
            if (s.ftype != 0 and s.ftype != 1) continue;
            if (s.sid < first or (s.sid - first) % 2 != 0) return error.UnknownStream;
            const idx = (s.sid - first) / 2;
            if (idx >= pack_streams) return error.UnknownStream;
            if (s.ftype == 1) {
                if (headers_at[idx] != null) return error.HeadersTwice;
                headers_at[idx] = seen;
            } else {
                // HEADERS first, then the body in whole, ending the stream.
                if (headers_at[idx] == null) return error.DataBeforeHeaders;
                if (data_at[idx] != null) return error.DataTwice;
                if (w.last_payload.items.len != 0) p.data_frames += 1;
                if (got[idx] + w.last_payload.items.len > packed_body_len) return error.LongBody;
                if (!std.mem.eql(u8, w.last_payload.items, packed_body[got[idx]..][0..w.last_payload.items.len])) return error.WrongBody;
                got[idx] += w.last_payload.items.len;
                if (s.flags & 1 != 0) {
                    if (got[idx] != packed_body_len) return error.ShortBody;
                    data_at[idx] = seen;
                    open -= 1;
                }
            }
            seen += 1;
        }
        // A complete handler answers in request order, and packing keeps the
        // drain's order inside a chunk and from chunk to chunk.
        for (1..pack_streams) |i| {
            if (headers_at[i].? < headers_at[i - 1].?) return error.HeadersOutOfOrder;
            if (data_at[i].? < data_at[i - 1].?) return error.DataOutOfOrder;
        }
    }
}

fn runPack(rt: *zio.Runtime, gpa: std.mem.Allocator, transport: Transport) !void {
    var cert: []u8 = &.{};
    var key: []u8 = &.{};
    defer {
        gpa.free(cert);
        gpa.free(key);
    }
    var server = try serverFor(rt, gpa, transport, starh2.Limits.defaults, &cert, &key);
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);

    // `chunks_to_pump` counts only under `test_observe` (debug builds): in a
    // build without it this stays 0 and the `chunks > 0` check below fails.
    const chunks_0 = om.chunks_to_pump.load(.monotonic);
    const pauses_0 = om.drain_memory_pauses.load(.monotonic);
    var p: Pack = .{ .gpa = gpa, .transport = transport, .port = server.localAddress(0).getPort() };
    const t = try std.Thread.spawn(.{}, packMain, .{&p});
    const deadline = nowNs() + client_deadline_ns;
    while (!p.done.load(.acquire)) {
        if (nowNs() >= deadline) {
            t.join();
            return error.Deadline;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    t.join();
    if (p.err) |err| return err;

    const chunks = om.chunks_to_pump.load(.monotonic) - chunks_0;
    const pauses = om.drain_memory_pauses.load(.monotonic) - pauses_0;
    std.debug.print("{s} packing: {d} DATA frames of {d} bytes (and as many HEADERS) in {d} write chunks; {d} pops held back\n", .{
        @tagName(transport), p.data_frames, packed_body_len, chunks, pauses,
    });
    try std.testing.expectEqual(@as(usize, pack_waves * pack_streams), p.data_frames);
    // The instrument ran: the write pool really did run dry, so the responses
    // queued up and left in drain turns of many frames.
    try std.testing.expect(pauses > 0);
    try std.testing.expect(chunks > 0);
    // A frame of its own per chunk is one chunk or more per DATA frame (and
    // HEADERS besides). Two DATA frames to a chunk is the least that packing
    // must reach; three fit in 16 KiB.
    try std.testing.expect(chunks * 2 <= p.data_frames);

    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
}

fn runPackCase(transport: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try newRuntime(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runPack, .{ rt, gpa, transport });
    try h.join();
}

test "packing: h2c many small responses on one connection share write chunks and arrive whole and in order" {
    try runPackCase(.h2c);
}

test "packing: tls many small responses on one connection share write chunks and arrive whole and in order" {
    try runPackCase(.tls);
}

test "zz h2 oracle: no protocol violation on any connection in this binary" {
    try @import("h2_oracle.zig").expectNoViolations();
}
