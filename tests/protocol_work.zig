//! Protocol work that holds nothing (t-3759 step 6), on real sockets, h2c and
//! TLS: DATA that delivers no body to a live stream is charged to the non-DATA
//! frame bucket; a 1-byte DATA flood and what bounds it; a browser-like load;
//! padding and the body minimum rate. A parked request whose stream is reset
//! gives its lease back at once.
//!
//! Each test prints the counters that show its defence fired, so an attack
//! that never engaged shows as a zero, not as a quiet pass. Every server ends
//! with `Server.deinit`, which checks that every client and route account is
//! back to zero (steps 3 and 5) and logs an error otherwise, which fails the
//! test.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");
const serve_deadline = @import("serve_deadline.zig");

const frame = starh2.core.frame;
const hpack = starh2.core.hpack;
const accounts = starh2.edge.accounts;
const conn_mod = starh2.edge.connection;

const Transport = wire_client.Transport;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;

const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

// ---------------------------------------------------------------------------
// Handlers.

var release: std.atomic.Value(bool) = .init(false);
var started: std.atomic.Value(usize) = .init(0);
var ended: std.atomic.Value(usize) = .init(0);

fn resetHandlers() void {
    release.store(false, .release);
    started.store(0, .release);
    ended.store(0, .release);
}

/// Reads its body (the whole of it: dispatch is at END_STREAM) and answers.
fn upload(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var buf: [24]u8 = undefined;
    try resp.send(200, &.{}, try std.fmt.bufPrint(&buf, "{d}", .{req.body.len}));
}

/// Ignores cancel (shielded) until `release`: a handler that keeps its slot
/// after its stream ended.
fn stuck(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = started.fetchAdd(1, .acq_rel);
    defer _ = ended.fetchAdd(1, .acq_rel);
    {
        zio.beginShield();
        defer zio.endShield();
        while (!release.load(.acquire)) zio.sleep(.fromMilliseconds(2)) catch {};
    }
    resp.send(200, &.{}, "late") catch {};
}

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "hello");
}

fn task(comptime f: anytype) starh2.Handler {
    return .{ .task = .{ .ptr = @constCast(&dummy), .runFn = f } };
}

const routes = [_]starh2.Route{
    .{ .method = .POST, .path = "/up", .handler = task(upload) },
    .{ .method = .GET, .path = "/stuck", .handler = task(stuck) },
    .{ .method = .GET, .path = "/", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
};

// ---------------------------------------------------------------------------
// The server.

const Rig = struct {
    gpa: std.mem.Allocator,
    server: starh2.Server,
    handle: zio.JoinHandle(starh2.ServeError!void),
    cert: []u8 = &.{},
    key: []u8 = &.{},

    fn start(self: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport, limits: starh2.Limits) !void {
        const io = rt.io();
        self.* = .{ .gpa = gpa, .server = undefined, .handle = undefined };
        var tls_cfg: ?starh2.TlsConfig = null;
        if (tr == .tls) {
            self.cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
            self.key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
            tls_cfg = .{ .certificate_chain_pem = self.cert, .private_key_pem = self.key };
        }
        errdefer {
            if (self.cert.len != 0) gpa.free(self.cert);
            if (self.key.len != 0) gpa.free(self.key);
        }
        const a4 = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
        const ep: starh2.EndpointConfig = switch (tr) {
            .h2c => .{ .h2c_prior_knowledge = .{ .address = a4 } },
            .tls => .{ .tls = .{ .address = a4 } },
        };
        self.server = try starh2.Server.init(gpa, io, .{ .endpoints = &.{ep}, .routes = &routes, .tls = tls_cfg, .limits = limits });
        self.handle = try rt.spawn(starh2.Server.serve, .{ &self.server, gpa });
        try self.server.waitUntilListening(5 * std.time.ns_per_s);
    }

    fn port(self: *Rig) u16 {
        return self.server.localAddress(0).getPort();
    }

    fn stop(self: *Rig) void {
        release.store(true, .release);
        self.server.requestShutdown();
        serve_deadline.join(&self.server, &self.handle);
        self.server.deinit(self.gpa);
        if (self.cert.len != 0) self.gpa.free(self.cert);
        if (self.key.len != 0) self.gpa.free(self.key);
    }
};

fn testLimits() starh2.Limits {
    var l = starh2.Limits.defaults;
    l.graceful_drain_timeout_ns = 300 * ms;
    return l;
}

fn runIn(comptime f: anytype, executors: u8, tr: Transport) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(executors) });
    defer rt.deinit();
    var h = try rt.spawn(f, .{ rt, gpa, tr });
    try h.join();
}

fn blocking(comptime f: anytype, args: anytype) !void {
    var h = try zio.spawnBlocking(f, args);
    return h.join();
}

fn expectExactly(comptime what: []const u8, got: anytype, want: anytype) !void {
    if (got == want) return;
    std.debug.print("protocol work: {s} is {d}, want {d}\n", .{ what, got, want });
    return error.WrongCount;
}

// ---------------------------------------------------------------------------
// One HTTP/2 client connection, on a blocking thread.

/// A connection whose reads give up after 1 ms (`Wire` sets 1 s), so a
/// client that reads between its writes is not paced by its own reads.
fn openQuick(gpa: std.mem.Allocator, tr: Transport, port: u16) !*Wire {
    const w = try Wire.open(gpa, tr, port);
    const tv: std.c.timeval = .{ .sec = 0, .usec = 1000 };
    _ = std.c.setsockopt(w.fd, std.c.SOL.SOCKET, std.c.SO.RCVTIMEO, std.mem.asBytes(&tv), @sizeOf(std.c.timeval));
    return w;
}

/// What the server did to a flooding connection.
const End = enum { none, goaway, rst, answered, closed };

const Flood = struct {
    end: End = .none,
    code: u32 = 0,
    frames: usize = 0,
    body_bytes: usize = 0,
    elapsed_ms: u64 = 0,
    answered_frames: usize = 0,
    answered_ms: u64 = 0,
    /// The `:status` of the answer on the flooded stream.
    answered_status: ?u16 = null,
    /// `one_byte_on`: the stream's RST, which does not stop the flood.
    rst_code: ?u32 = null,
    /// RST_STREAM frames on the flooded stream.
    rst_n: usize = 0,
    rst_ms: u64 = 0,
    rst_frames: usize = 0,
    rst_after_answer: bool = false,
    err: ?anyerror = null,
};

const FloodKind = enum {
    /// Zero-length DATA on a live stream.
    empty,
    /// DATA on a stream the client reset first.
    closed_stream,
    /// 1-byte DATA on a live stream, as fast as flow control allows.
    one_byte,
    /// DATA that is all padding (the pad length byte, no padding, no body) on
    /// a live stream: it pays one byte of flow control and delivers nothing.
    all_padding,
    /// `one_byte`, going on after the server answers the stream and after it
    /// resets it (then within the connection's credit only, as on a closed
    /// stream).
    one_byte_on,
};

/// The open stream's credit as the server grants it.
const Credit = struct {
    conn: i64 = 65_535,
    stream: i64 = 65_535,
};

fn readSettingsWindow(w: *Wire, cr: *Credit) void {
    const p = w.last_payload.items;
    var i: usize = 0;
    while (i + 6 <= p.len) : (i += 6) {
        const id = std.mem.readInt(u16, p[i..][0..2], .big);
        const v = std.mem.readInt(u32, p[i + 2 ..][0..4], .big);
        if (id == 0x4) cr.stream = v;
    }
}

/// Flood one POST stream with `kind` DATA frames, batch by batch, reading
/// what the server sends between batches, until the server ends the stream
/// or the connection, or `limit_ms` pass.
fn floodClient(gpa: std.mem.Allocator, tr: Transport, port: u16, kind: FloodKind, limit_ms: u64, r: *Flood) void {
    floodInner(gpa, tr, port, kind, limit_ms, r) catch |err| {
        r.err = err;
    };
}

fn floodInner(gpa: std.mem.Allocator, tr: Transport, port: u16, kind: FloodKind, limit_ms: u64, r: *Flood) !void {
    const w = try openQuick(gpa, tr, port);
    defer w.close();
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var pre = try h2c.buildClientPrefaceAndSettings(gpa);
    defer pre.deinit(gpa);
    try h2c.appendHeadersExtra(gpa, &pre, 1, "POST", "/up", false, &.{});
    if (kind == .closed_stream) try h2c.appendRst(gpa, &pre, 1);
    try w.sendUnchecked(pre.items);
    var cr: Credit = .{};
    const t0 = nowNs();
    var batch: std.ArrayList(u8) = .empty;
    defer batch.deinit(gpa);
    const one: [1]u8 = .{'x'};
    while (true) {
        // Read what is there, briefly.
        while (true) {
            const s = w.next(nowNs() + 1 * ms) catch |err| switch (err) {
                error.Deadline, error.WouldBlock => break,
                else => {
                    r.end = .closed;
                    r.elapsed_ms = (nowNs() - t0) / ms;
                    return;
                },
            } orelse {
                r.end = .closed;
                r.elapsed_ms = (nowNs() - t0) / ms;
                return;
            };
            const p = w.last_payload.items;
            switch (s.ftype) {
                0x4 => if (s.flags & 1 == 0) readSettingsWindow(w, &cr),
                0x8 => if (p.len >= 4) {
                    const inc = std.mem.readInt(u32, p[0..4], .big) & 0x7fff_ffff;
                    if (s.sid == 0) cr.conn += inc else if (s.sid == 1) cr.stream += inc;
                },
                0x7 => {
                    r.end = .goaway;
                    r.code = if (p.len >= 8) std.mem.readInt(u32, p[4..8], .big) else 0;
                    r.elapsed_ms = (nowNs() - t0) / ms;
                    return;
                },
                0x3 => if (s.sid == 1 and kind == .one_byte_on) {
                    r.rst_n += 1;
                    if (r.rst_code == null) {
                        r.rst_code = if (p.len >= 4) std.mem.readInt(u32, p[0..4], .big) else 0;
                        r.rst_ms = (nowNs() - t0) / ms;
                        r.rst_frames = r.frames;
                        r.rst_after_answer = r.end == .answered;
                    }
                } else if (s.sid == 1 and kind != .closed_stream) {
                    r.end = .rst;
                    r.code = if (p.len >= 4) std.mem.readInt(u32, p[0..4], .big) else 0;
                    r.elapsed_ms = (nowNs() - t0) / ms;
                    return;
                },
                0x1 => if (s.sid == 1 and kind != .closed_stream and s.flags & 1 != 0) {
                    if (r.answered_status == null) r.answered_status = try statusOf(&dec, p);
                    if (kind == .one_byte_on) {
                        if (r.end == .none) {
                            r.end = .answered;
                            r.answered_frames = r.frames;
                            r.answered_ms = (nowNs() - t0) / ms;
                        }
                    } else {
                        r.end = .answered;
                        r.elapsed_ms = (nowNs() - t0) / ms;
                        return;
                    }
                },
                else => {},
            }
        }
        if ((nowNs() - t0) / ms >= limit_ms) {
            r.elapsed_ms = (nowNs() - t0) / ms;
            return;
        }
        batch.clearRetainingCapacity();
        var n: usize = 0;
        while (n < 2048) : (n += 1) {
            switch (kind) {
                .empty => try h2c.appendData(gpa, &batch, 1, &.{}, false),
                .all_padding => {
                    if (cr.conn < 1 or cr.stream < 1) break;
                    try appendFrame(gpa, &batch, 0x0, PADDED, 1, &.{0});
                    cr.conn -= 1;
                    cr.stream -= 1;
                },
                .closed_stream, .one_byte, .one_byte_on => {
                    const stream_closed = kind == .closed_stream or r.rst_code != null;
                    if (cr.conn < 1 or (!stream_closed and cr.stream < 1)) break;
                    try h2c.appendData(gpa, &batch, 1, &one, false);
                    cr.conn -= 1;
                    cr.stream -= 1;
                    r.body_bytes += 1;
                },
            }
        }
        if (n == 0) continue;
        w.sendUnchecked(batch.items) catch {
            r.end = .closed;
            r.elapsed_ms = (nowNs() - t0) / ms;
            return;
        };
        r.frames += n;
    }
}

// ---------------------------------------------------------------------------
// 1. DATA that delivers no body: floods.

/// Zero-length DATA on a live stream, and DATA on a stream the client reset,
/// as fast as the client can write them: each is charged to the non-DATA
/// bucket (burst 10,000), so the connection ends with GOAWAY
/// ENHANCE_YOUR_CALM within the burst plus what was in flight. A well-behaved
/// client is served before and after.
fn bodylessFloods(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, testLimits());
    defer rig.stop();
    inline for (.{ FloodKind.empty, FloodKind.all_padding, FloodKind.closed_stream }) |kind| {
        var r: Flood = .{};
        try blocking(floodClient, .{ gpa, tr, rig.port(), kind, 5_000, &r });
        var ok = false;
        try blocking(goodGet, .{ gpa, tr, rig.port(), &ok });
        std.debug.print("protocol work flood ({s}) {s}: ended {s} code {d} after {d} ms and {d} frames sent; a second client served 200: {}; err {?}\n", .{ @tagName(tr), @tagName(kind), @tagName(r.end), r.code, r.elapsed_ms, r.frames, ok, r.err });
        try std.testing.expect(r.err == null);
        try expectExactly("the flood's end (goaway)", @intFromEnum(r.end), @intFromEnum(End.goaway));
        try expectExactly("the GOAWAY code (ENHANCE_YOUR_CALM)", r.code, @intFromEnum(frame.ErrorCode.enhance_your_calm));
        // The burst is 10,000; what is past it was in flight when the GOAWAY
        // was written (the client writes 2,048 a batch).
        try std.testing.expect(r.frames >= 10_000);
        try std.testing.expect(ok);
    }
}

fn floodsH2c(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return bodylessFloods(rt, gpa, tr);
}

test "flood of zero-length DATA and of DATA on a closed stream ends with GOAWAY ENHANCE_YOUR_CALM (h2c)" {
    try runIn(floodsH2c, 2, .h2c);
}
test "flood of zero-length DATA and of DATA on a closed stream ends with GOAWAY ENHANCE_YOUR_CALM (TLS)" {
    try runIn(floodsH2c, 2, .tls);
}

/// 1-byte DATA as fast as flow control allows, on one executor. It is not
/// charged to the bucket: each frame pays flow control and delivers a body
/// byte. What ends it is the stream's body cap (`request_body_bytes`): a 413,
/// after as many frames as the cap has bytes. Printed: what that costs. The
/// rate rule (240 B/s) does not end it: the flood delivers far more.
fn oneByteFlood(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    var rig: Rig = undefined;
    const limits = testLimits();
    try rig.start(rt, gpa, tr, limits);
    defer rig.stop();
    var r: Flood = .{};
    try blocking(floodClient, .{ gpa, tr, rig.port(), FloodKind.one_byte, 20_000, &r });
    const per_frame_ns = if (r.frames == 0) 0 else r.elapsed_ms * ms / r.frames;
    std.debug.print("protocol work 1-byte flood ({s}, one executor): ended {s} after {d} ms; {d} frames ({d} body bytes) sent before the end was read, about {d} ns a frame; the body cap is {d} bytes; err {?}\n", .{ @tagName(tr), @tagName(r.end), r.elapsed_ms, r.frames, r.body_bytes, per_frame_ns, limits.request_body_bytes, r.err });
    try std.testing.expect(r.err == null);
    try expectExactly("the stream's end (answered: a 413)", @intFromEnum(r.end), @intFromEnum(End.answered));
    try expectExactly("the answer's status", r.answered_status orelse 0, 413);
    try std.testing.expect(r.body_bytes > limits.request_body_bytes);
}

test "a 1-byte DATA flood on one executor ends at the stream's body cap, and what it costs (h2c)" {
    try runIn(oneByteFlood, 1, .h2c);
}
test "a 1-byte DATA flood on one executor ends at the stream's body cap, and what it costs (TLS)" {
    try runIn(oneByteFlood, 1, .tls);
}

/// The 1-byte flood, going on after the 413. The server resets the stream
/// with NO_ERROR once the 413 is queued (RFC 9113 section 8.1), so what the
/// client sends after it is DATA on a closed stream, charged to the non-DATA
/// bucket: the connection ends with GOAWAY ENHANCE_YOUR_CALM. Printed: the
/// frames and the time from the RST to the GOAWAY.
fn floodAfterAnswer(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    var rig: Rig = undefined;
    const limits = testLimits();
    try rig.start(rt, gpa, tr, limits);
    defer rig.stop();
    var r: Flood = .{};
    try blocking(floodClient, .{ gpa, tr, rig.port(), FloodKind.one_byte_on, 5_000, &r });
    std.debug.print("protocol work flood after the 413 ({s}, one executor): answered after {d} ms and {d} frames; stream reset code {?d} after {d} ms and {d} frames (after the answer: {}); ended {s} code {d} after {d} ms and {d} frames, {d} ms and {d} frames after the reset; err {?}\n", .{
        @tagName(tr), r.answered_ms, r.answered_frames, r.rst_code, r.rst_ms, r.rst_frames, r.rst_after_answer, @tagName(r.end), r.code, r.elapsed_ms, r.frames, r.elapsed_ms -| r.rst_ms, r.frames -| r.rst_frames, r.err,
    });
    try std.testing.expect(r.err == null);
    try std.testing.expect(r.answered_frames != 0);
    try expectExactly("the answer's status", r.answered_status orelse 0, 413);
    try expectExactly("the stream's reset code (NO_ERROR)", r.rst_code orelse 99, @intFromEnum(frame.ErrorCode.no_error));
    try expectExactly("the stream's RSTs", r.rst_n, 1);
    try std.testing.expect(r.rst_after_answer);
    try expectExactly("the flood's end (goaway)", @intFromEnum(r.end), @intFromEnum(End.goaway));
    try expectExactly("the GOAWAY code (ENHANCE_YOUR_CALM)", r.code, @intFromEnum(frame.ErrorCode.enhance_your_calm));
}

test "a 1-byte DATA flood that goes on after its 413 meets RST_STREAM NO_ERROR, then GOAWAY ENHANCE_YOUR_CALM (h2c)" {
    try runIn(floodAfterAnswer, 1, .h2c);
}
test "a 1-byte DATA flood that goes on after its 413 meets RST_STREAM NO_ERROR, then GOAWAY ENHANCE_YOUR_CALM (TLS)" {
    try runIn(floodAfterAnswer, 1, .tls);
}

const RefusedResult = struct {
    seen: Seen = .{},
    /// `heldRefusedInner`: the upload's answer and RSTs read while the
    /// server's drain was held (none, if the hold engaged).
    held_headers: u8 = 0,
    held_rst: u8 = 0,
    err: ?anyerror = null,
};

fn refusedClient(gpa: std.mem.Allocator, tr: Transport, port: u16, r: *RefusedResult) void {
    refusedInner(gpa, tr, port, r) catch |err| {
        r.err = err;
    };
}

/// An upload over the body cap, in two DATA frames without END_STREAM; the
/// client reads its 413 and the RST, stops sending, and asks for "/" on the
/// same connection.
fn refusedInner(gpa: std.mem.Allocator, tr: Transport, port: u16, r: *RefusedResult) !void {
    const w = try openQuick(gpa, tr, port);
    defer w.close();
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try h2c.appendHeadersExtra(gpa, &req, 1, "POST", "/up", false, &.{});
    const chunk: [16_000]u8 = @splat('x');
    try h2c.appendData(gpa, &req, 1, &chunk, false);
    try h2c.appendData(gpa, &req, 1, &chunk, false);
    try w.sendUnchecked(req.items);
    const deadline = nowNs() + 2 * std.time.ns_per_s;
    while (nowNs() < deadline and (r.seen.status[0] == null or r.seen.rst[0] == null) and !r.seen.closed) try readFor(w, &dec, &r.seen, 20);
    req.clearRetainingCapacity();
    try h2c.appendHeaders(gpa, &req, 3, "/", true);
    try w.sendUnchecked(req.items);
    while (nowNs() < deadline + std.time.ns_per_s and r.seen.status[1] == null and !r.seen.closed) try readFor(w, &dec, &r.seen, 20);
    try readFor(w, &dec, &r.seen, 100);
}

/// A well-behaved client whose upload is refused: it gets the 413, then
/// RST_STREAM NO_ERROR, and its connection serves the next request.
fn refusedUpload(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    var rig: Rig = undefined;
    var limits = testLimits();
    limits.request_body_bytes = 16 * kib;
    try rig.start(rt, gpa, tr, limits);
    defer rig.stop();
    var r: RefusedResult = .{};
    try blocking(refusedClient, .{ gpa, tr, rig.port(), &r });
    std.debug.print("protocol work refused upload ({s}): 32000 bytes against a 16 KiB cap: status {?d}, then rst {?d}; next request on the connection: status {?d}; GOAWAY {} closed {}; err {?}\n", .{
        @tagName(tr), r.seen.status[0], r.seen.rst[0], r.seen.status[1], r.seen.goaway, r.seen.closed, r.err,
    });
    try std.testing.expect(r.err == null);
    try expectExactly("the upload's status", r.seen.status[0] orelse 0, 413);
    try expectExactly("the upload's answers", r.seen.headers[0], 1);
    try expectExactly("the upload's reset code (NO_ERROR)", r.seen.rst[0] orelse 99, @intFromEnum(frame.ErrorCode.no_error));
    try expectExactly("the upload's RSTs", r.seen.rst_n[0], 1);
    try std.testing.expect(r.seen.rst_after_headers[0]);
    try expectExactly("the next request's status", r.seen.status[1] orelse 0, 200);
    try std.testing.expect(!r.seen.goaway and !r.seen.closed);
}

test "a refused upload that stops sending gets its 413 and RST_STREAM NO_ERROR, and its connection serves the next request (h2c)" {
    try runIn(refusedUpload, 2, .h2c);
}
test "a refused upload that stops sending gets its 413 and RST_STREAM NO_ERROR, and its connection serves the next request (TLS)" {
    try runIn(refusedUpload, 2, .tls);
}

fn holdEmit(on: bool) void {
    if (comptime @hasDecl(conn_mod, "test_hold_emit") and @TypeOf(conn_mod.test_hold_emit) != void) conn_mod.test_hold_emit.store(on, .release);
}

/// `refusedInner`, with the server's drain held while the upload is refused,
/// so the 413 is still in the scheduler when the turn that queued it ends.
/// The reset must wait for it: a local reset drops a stream's queued
/// HEADERS, and the RST would go first.
fn heldRefusedInner(gpa: std.mem.Allocator, tr: Transport, port: u16, r: *RefusedResult) !void {
    const w = try openQuick(gpa, tr, port);
    defer w.close();
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try w.sendUnchecked(req.items);
    // The server's preface first, so the hold below holds only the 413.
    try readFor(w, &dec, &r.seen, 100);
    req.clearRetainingCapacity();
    holdEmit(true);
    try h2c.appendHeadersExtra(gpa, &req, 1, "POST", "/up", false, &.{});
    const chunk: [16_000]u8 = @splat('x');
    try h2c.appendData(gpa, &req, 1, &chunk, false);
    try h2c.appendData(gpa, &req, 1, &chunk, false);
    try w.sendUnchecked(req.items);
    // While the drain is held the server ingests the upload and queues its
    // 413; nothing of it may reach the client.
    try readFor(w, &dec, &r.seen, 200);
    r.held_headers = r.seen.headers[0];
    r.held_rst = r.seen.rst_n[0];
    holdEmit(false);
    // Something to wake the actor: the next request.
    req.clearRetainingCapacity();
    try h2c.appendHeaders(gpa, &req, 3, "/", true);
    try w.sendUnchecked(req.items);
    const deadline = nowNs() + 2 * std.time.ns_per_s;
    while (nowNs() < deadline and r.seen.status[1] == null and !r.seen.closed) try readFor(w, &dec, &r.seen, 20);
    try readFor(w, &dec, &r.seen, 100);
}

fn heldRefusedClient(gpa: std.mem.Allocator, tr: Transport, port: u16, r: *RefusedResult) void {
    heldRefusedInner(gpa, tr, port, r) catch |err| {
        r.err = err;
    };
}

fn holdCompiledIn() bool {
    return @hasDecl(conn_mod, "test_hold_emit") and @TypeOf(conn_mod.test_hold_emit) != void;
}

/// Turns in which the reset sweep found a 413 still queued (observe builds).
fn resetWaited() usize {
    if (comptime !@hasDecl(conn_mod, "test_reset_waited_on_response") or @TypeOf(conn_mod.test_reset_waited_on_response) == void) return 0;
    return conn_mod.test_reset_waited_on_response.load(.acquire);
}

fn heldRefusedUpload(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    // Without the hold this would be `refusedUpload` again, passing without
    // testing the case it names.
    if (comptime !holdCompiledIn()) {
        std.debug.print("protocol work refused upload, drain held ({s}): SKIPPED: connection.test_hold_emit is compiled out (build without -Dobserve)\n", .{@tagName(tr)});
        return error.SkipZigTest;
    }
    const waited0 = resetWaited();
    var rig: Rig = undefined;
    var limits = testLimits();
    limits.request_body_bytes = 16 * kib;
    try rig.start(rt, gpa, tr, limits);
    defer rig.stop();
    defer holdEmit(false);
    var r: RefusedResult = .{};
    try blocking(heldRefusedClient, .{ gpa, tr, rig.port(), &r });
    const waited = resetWaited() - waited0;
    std.debug.print("protocol work refused upload, drain held ({s}): during the hold HEADERS {d} RST {d}, reset sweeps that found the 413 queued {d}; then status {?d} (HEADERS {d}), rst {?d} x{d}; next request: status {?d}; GOAWAY {} closed {}; err {?}\n", .{
        @tagName(tr), r.held_headers, r.held_rst, waited, r.seen.status[0], r.seen.headers[0], r.seen.rst[0], r.seen.rst_n[0], r.seen.status[1], r.seen.goaway, r.seen.closed, r.err,
    });
    try std.testing.expect(r.err == null);
    // The hold engaged: nothing came while it held, and the sweep saw the
    // 413 still in the scheduler at least once.
    try expectExactly("the upload's HEADERS during the hold", r.held_headers, 0);
    try expectExactly("the upload's RSTs during the hold", r.held_rst, 0);
    if (waited == 0) {
        std.debug.print("protocol work refused upload, drain held: the reset sweep never found the 413 queued\n", .{});
        return error.HoldNotEngaged;
    }
    try expectExactly("the upload's status", r.seen.status[0] orelse 0, 413);
    try expectExactly("the upload's reset code (NO_ERROR)", r.seen.rst[0] orelse 99, @intFromEnum(frame.ErrorCode.no_error));
    try expectExactly("the upload's RSTs", r.seen.rst_n[0], 1);
    try std.testing.expect(r.seen.rst_after_headers[0]);
    try expectExactly("the next request's status", r.seen.status[1] orelse 0, 200);
}

test "a 413 still queued when its turn ends goes out before its RST_STREAM NO_ERROR (h2c)" {
    try runIn(heldRefusedUpload, 2, .h2c);
}
test "a 413 still queued when its turn ends goes out before its RST_STREAM NO_ERROR (TLS)" {
    try runIn(heldRefusedUpload, 2, .tls);
}

/// A well-behaved GET on a connection of its own: true when answered 200.
fn goodGet(gpa: std.mem.Allocator, tr: Transport, port: u16, ok: *bool) void {
    ok.* = goodGetInner(gpa, tr, port) catch false;
}

fn goodGetInner(gpa: std.mem.Allocator, tr: Transport, port: u16) !bool {
    const w = try Wire.open(gpa, tr, port);
    defer w.close();
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var pre = try h2c.buildClientPrefaceAndSettings(gpa);
    defer pre.deinit(gpa);
    try h2c.appendHeaders(gpa, &pre, 1, "/", true);
    try w.send(pre.items);
    const deadline = nowNs() + 3 * std.time.ns_per_s;
    var status: ?u16 = null;
    while (true) {
        const s = (w.next(deadline) catch return false) orelse return false;
        if (s.sid != 1) continue;
        if (s.ftype == 0x1) status = try statusOf(&dec, w.last_payload.items);
        // Served is a 200 that ENDS: its HEADERS, then "hello" with
        // END_STREAM. A response that stalls or loses its body is not.
        if ((s.ftype == 0x0 or s.ftype == 0x1) and s.flags & 1 != 0) return status == 200;
    }
}

/// The `:status` of one response HEADERS (the server's encoder keeps no
/// dynamic table, so each block decodes alone).
fn statusOf(dec: *hpack.Decoder, header_block: []const u8) !?u16 {
    const decoded = try dec.decode(header_block, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(decoded);
    for (decoded.fields) |f| {
        if (std.mem.eql(u8, f.name, ":status")) return std.fmt.parseInt(u16, f.value, 10) catch null;
    }
    return null;
}

// ---------------------------------------------------------------------------
// Requests that end their body with an empty END_STREAM DATA, sustained.

const run_requests = 20_000;
const run_batch = 100;

const RunResult = struct {
    sent: usize = 0,
    ok: usize = 0,
    not_ok: usize = 0,
    goaway: bool = false,
    goaway_code: u32 = 0,
    elapsed_ms: u64 = 0,
    err: ?anyerror = null,
};

fn runClient(gpa: std.mem.Allocator, tr: Transport, port: u16, r: *RunResult) void {
    runInner(gpa, tr, port, r) catch |err| {
        r.err = err;
    };
}

/// POST /up with an empty body: HEADERS without END_STREAM, then a
/// zero-length DATA with END_STREAM, as several real clients end every
/// request. Batches of 100 on one connection, each answered before the next.
fn runInner(gpa: std.mem.Allocator, tr: Transport, port: u16, r: *RunResult) !void {
    const w = try Wire.open(gpa, tr, port);
    defer w.close();
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var pre = try h2c.buildClientPrefaceAndSettings(gpa);
    defer pre.deinit(gpa);
    try w.send(pre.items);
    const t0 = nowNs();
    var sid: u31 = 1;
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(gpa);
    while (r.sent < run_requests) {
        req.clearRetainingCapacity();
        const first = sid;
        for (0..run_batch) |_| {
            try h2c.appendHeadersExtra(gpa, &req, sid, "POST", "/up", false, &.{});
            try h2c.appendData(gpa, &req, sid, &.{}, true);
            sid += 2;
        }
        try w.send(req.items);
        r.sent += run_batch;
        var done: usize = 0;
        const deadline = nowNs() + 5 * std.time.ns_per_s;
        while (done < run_batch) {
            const s = (try w.next(deadline)) orelse return error.Closed;
            if (s.ftype == 0x7) {
                r.goaway = true;
                const p = w.last_payload.items;
                r.goaway_code = if (p.len >= 8) std.mem.readInt(u32, p[4..8], .big) else 0;
                r.elapsed_ms = (nowNs() - t0) / ms;
                return;
            }
            if (s.sid >= first and s.ftype == 0x1) {
                done += 1;
                if ((try statusOf(&dec, w.last_payload.items)) == 200) r.ok += 1 else r.not_ok += 1;
            }
        }
    }
    r.elapsed_ms = (nowNs() - t0) / ms;
}

/// 20,000 such requests on one connection, as fast as they are answered:
/// above the bucket's 1,000 a second and past its 10,000 burst. An empty
/// END_STREAM DATA on a live stream is not charged, so none meets the bucket.
fn endStreamRun(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, testLimits());
    defer rig.stop();
    var r: RunResult = .{};
    try blocking(runClient, .{ gpa, tr, rig.port(), &r });
    const per_s = if (r.elapsed_ms == 0) 0 else r.ok * 1000 / r.elapsed_ms;
    std.debug.print("protocol work empty END_STREAM run ({s}): {d} requests sent on one connection, {d} answered 200, {d} otherwise, in {d} ms ({d} a second); GOAWAY {} code {d}; err {?}\n", .{ @tagName(tr), r.sent, r.ok, r.not_ok, r.elapsed_ms, per_s, r.goaway, r.goaway_code, r.err });
    try std.testing.expect(r.err == null);
    try std.testing.expect(!r.goaway);
    try expectExactly("requests answered 200", r.ok, run_requests);
    // The run must be one the old rule refuses: more than the burst plus
    // what the bucket refills (1,000 a second) while it runs.
    if (r.ok <= 10_000 + 1_000 * r.elapsed_ms / 1000) {
        std.debug.print("protocol work empty END_STREAM run: too slow to test the rule ({d} a second)\n", .{per_s});
        return error.RunTooSlow;
    }
}

test "20,000 requests ending in an empty END_STREAM DATA, well above 1,000 a second on one connection, get no GOAWAY (h2c)" {
    try runIn(endStreamRun, 2, .h2c);
}
test "20,000 requests ending in an empty END_STREAM DATA, well above 1,000 a second on one connection, get no GOAWAY (TLS)" {
    try runIn(endStreamRun, 2, .tls);
}

// ---------------------------------------------------------------------------
// A browser loading a Datastar page, again and again, on one connection.

const page_streams = 40;
const page_loads = 50;

const PageResult = struct {
    answered: usize = 0,
    empty_data: usize = 0,
    goaway: bool = false,
    err: ?anyerror = null,
};

/// Each load: 25 GETs (HEADERS with END_STREAM), 10 POSTs with a small body
/// (one DATA with END_STREAM), 5 POSTs whose body ends with an empty DATA
/// carrying END_STREAM (as fetch() does for a streamed body). Every stream
/// must be answered and no GOAWAY sent: 250 empty DATA are far inside the
/// bucket.
fn pageLoads(gpa: std.mem.Allocator, tr: Transport, port: u16, r: *PageResult) void {
    pageInner(gpa, tr, port, r) catch |err| {
        r.err = err;
    };
}

fn pageInner(gpa: std.mem.Allocator, tr: Transport, port: u16, r: *PageResult) !void {
    const w = try Wire.open(gpa, tr, port);
    defer w.close();
    var pre = try h2c.buildClientPrefaceAndSettings(gpa);
    defer pre.deinit(gpa);
    try w.send(pre.items);
    var sid: u31 = 1;
    for (0..page_loads) |_| {
        var req: std.ArrayList(u8) = .empty;
        defer req.deinit(gpa);
        const first = sid;
        for (0..page_streams) |i| {
            if (i < 25) {
                try h2c.appendHeaders(gpa, &req, sid, "/", true);
            } else if (i < 35) {
                try h2c.appendHeadersExtra(gpa, &req, sid, "POST", "/up", false, &.{});
                try h2c.appendData(gpa, &req, sid, "{\"signal\":1}", true);
            } else {
                try h2c.appendHeadersExtra(gpa, &req, sid, "POST", "/up", false, &.{});
                try h2c.appendData(gpa, &req, sid, "{\"signal\":2}", false);
                try h2c.appendData(gpa, &req, sid, &.{}, true);
                r.empty_data += 1;
            }
            sid += 2;
        }
        try w.send(req.items);
        var done: usize = 0;
        const deadline = nowNs() + 5 * std.time.ns_per_s;
        while (done < page_streams) {
            const s = (try w.next(deadline)) orelse return error.Closed;
            if (s.ftype == 0x7) {
                r.goaway = true;
                return;
            }
            if (s.sid >= first and (s.ftype == 0x0 or s.ftype == 0x1) and s.flags & 1 != 0) done += 1;
        }
        r.answered += done;
    }
}

fn browserLoad(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, testLimits());
    defer rig.stop();
    var r: PageResult = .{};
    try blocking(pageLoads, .{ gpa, tr, rig.port(), &r });
    std.debug.print("protocol work browser load ({s}): {d} page loads of {d} streams on one connection: {d} answered, {d} empty END_STREAM DATA sent, GOAWAY {}; err {?}\n", .{ @tagName(tr), page_loads, page_streams, r.answered, r.empty_data, r.goaway, r.err });
    try std.testing.expect(r.err == null);
    try std.testing.expect(!r.goaway);
    try expectExactly("streams answered", r.answered, page_loads * page_streams);
}

test "a browser loading a Datastar page fifty times on one connection never meets the bucket (h2c)" {
    try runIn(browserLoad, 2, .h2c);
}
test "a browser loading a Datastar page fifty times on one connection never meets the bucket (TLS)" {
    try runIn(browserLoad, 2, .tls);
}

// ---------------------------------------------------------------------------
// Padding is not body progress.

const PadResult = struct {
    end: End = .none,
    code: u32 = 0,
    elapsed_ms: u64 = 0,
    wire_body_bytes: usize = 0,
    err: ?anyerror = null,
};

/// Every 100 ms one DATA of 251 payload bytes: padded (1 body byte, a pad
/// length byte and 249 bytes of padding) or not (251 body bytes). For up to
/// `limit_ms`, then END_STREAM.
fn padClient(gpa: std.mem.Allocator, tr: Transport, port: u16, padded: bool, limit_ms: u64, r: *PadResult) void {
    padInner(gpa, tr, port, padded, limit_ms, r) catch |err| {
        r.err = err;
    };
}

fn padInner(gpa: std.mem.Allocator, tr: Transport, port: u16, padded: bool, limit_ms: u64, r: *PadResult) !void {
    const w = try openQuick(gpa, tr, port);
    defer w.close();
    var pre = try h2c.buildClientPrefaceAndSettings(gpa);
    defer pre.deinit(gpa);
    try h2c.appendHeadersExtra(gpa, &pre, 1, "POST", "/up", false, &.{});
    try w.send(pre.items);
    const t0 = nowNs();
    var payload: [251]u8 = @splat('p');
    if (padded) payload[0] = 249;
    while (true) {
        const until = nowNs() + 100 * ms;
        while (nowNs() < until) {
            const s = w.next(until) catch |err| switch (err) {
                error.Deadline, error.WouldBlock => continue,
                else => return err,
            } orelse {
                r.end = .closed;
                r.elapsed_ms = (nowNs() - t0) / ms;
                return;
            };
            if (s.sid != 1) continue;
            const p = w.last_payload.items;
            if (s.ftype == 0x3) {
                r.end = .rst;
                r.code = if (p.len >= 4) std.mem.readInt(u32, p[0..4], .big) else 0;
                r.elapsed_ms = (nowNs() - t0) / ms;
                return;
            }
            if ((s.ftype == 0x0 or s.ftype == 0x1) and s.flags & 1 != 0) {
                r.end = .answered;
                r.elapsed_ms = (nowNs() - t0) / ms;
                return;
            }
        }
        const last = (nowNs() - t0) / ms >= limit_ms;
        var hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
        (frame.FrameHeader{ .length = payload.len, .type = .data, .flags = .{ .padded = padded, .end_stream = last }, .stream_id = 1 }).encode(&hdr);
        var one: std.ArrayList(u8) = .empty;
        defer one.deinit(gpa);
        try one.appendSlice(gpa, &hdr);
        try one.appendSlice(gpa, &payload);
        try w.send(one.items);
        r.wire_body_bytes += payload.len;
    }
}

fn padding(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    var limits = testLimits();
    limits.request_body_min_rate_grace_ns = 1 * std.time.ns_per_s;
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, limits);
    defer rig.stop();
    var pad: PadResult = .{};
    var plain: PadResult = .{};
    try blocking(padClient, .{ gpa, tr, rig.port(), true, 3_000, &pad });
    try blocking(padClient, .{ gpa, tr, rig.port(), false, 3_000, &plain });
    std.debug.print("protocol work padding ({s}): 251 bytes of DATA payload every 100 ms, rate 240 B/s after a 1 s grace: padded (1 body byte each) ended {s} code {d} at {d} ms; unpadded (251 body bytes each) ended {s} at {d} ms; errs {?} {?}\n", .{ @tagName(tr), @tagName(pad.end), pad.code, pad.elapsed_ms, @tagName(plain.end), plain.elapsed_ms, pad.err, plain.err });
    try std.testing.expect(pad.err == null and plain.err == null);
    try expectExactly("the padded body's end (reset)", @intFromEnum(pad.end), @intFromEnum(End.rst));
    try expectExactly("the reset code (CANCEL)", pad.code, @intFromEnum(frame.ErrorCode.cancel));
    try std.testing.expect(pad.elapsed_ms < 2_500);
    try expectExactly("the unpadded body's end (answered)", @intFromEnum(plain.end), @intFromEnum(End.answered));
    try std.testing.expect(plain.elapsed_ms >= 3_000);
}

test "a padded body does not satisfy the body minimum rate; the same bytes as body do (h2c)" {
    try runIn(padding, 2, .h2c);
}
test "a padded body does not satisfy the body minimum rate; the same bytes as body do (TLS)" {
    try runIn(padding, 2, .tls);
}

// ---------------------------------------------------------------------------
// 3. Parked leases released on RST.

fn parkedReleased() u64 {
    if (comptime !@hasDecl(conn_mod, "parked_released_on_rst")) return 0;
    return conn_mod.parked_released_on_rst.load(.acquire);
}


fn waitStarted(n: usize, timeout_ms: u64) bool {
    const deadline = nowNs() + timeout_ms * ms;
    while (started.load(.acquire) < n) {
        if (nowNs() >= deadline) return false;
        osSleepMs(1);
    }
    return true;
}

fn osSleepMs(n: u64) void {
    starh2.test_sleep.threadSleepMs(n);
}

/// What one connection saw, frame by frame.
const Seen = struct {
    goaway: bool = false,
    goaway_code: u32 = 0,
    closed: bool = false,
    /// Final answers by stream id (odd ids up to 63).
    status: [32]?u16 = @splat(null),
    rst: [32]?u32 = @splat(null),
    /// HEADERS frames by stream id: one answer is 1.
    headers: [32]u8 = @splat(0),
    /// The stream's RST came after a HEADERS on it.
    rst_after_headers: [32]bool = @splat(false),
    /// RST_STREAM frames by stream id: `rst` keeps only the last code.
    rst_n: [32]u8 = @splat(0),
};

fn readFor(w: *Wire, dec: *hpack.Decoder, seen: *Seen, for_ms: u64) !void {
    const deadline = nowNs() + for_ms * ms;
    while (!seen.closed and nowNs() < deadline) {
        const s = w.next(deadline) catch |err| switch (err) {
            error.Deadline, error.WouldBlock => return,
            else => {
                seen.closed = true;
                return;
            },
        } orelse {
            seen.closed = true;
            return;
        };
        const p = w.last_payload.items;
        const i: usize = if (s.sid == 0) 0 else (s.sid - 1) / 2;
        switch (s.ftype) {
            0x1 => {
                if (i < seen.headers.len) seen.headers[i] +|= 1;
                const decoded = try dec.decode(p, 100, 32 * 1024, 256, 8 * 1024);
                defer dec.freeResult(decoded);
                for (decoded.fields) |f| {
                    if (i < seen.status.len and std.mem.eql(u8, f.name, ":status")) seen.status[i] = std.fmt.parseInt(u16, f.value, 10) catch null;
                }
            },
            0x3 => if (i < seen.rst.len and p.len >= 4) {
                seen.rst[i] = std.mem.readInt(u32, p[0..4], .big);
                seen.rst_n[i] +|= 1;
                seen.rst_after_headers[i] = seen.headers[i] != 0;
            },
            0x7 => {
                seen.goaway = true;
                seen.goaway_code = if (p.len >= 8) std.mem.readInt(u32, p[4..8], .big) else 0;
            },
            else => {},
        }
    }
}

fn sendGets(gpa: std.mem.Allocator, w: *Wire, sids: []const u31) !void {
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(gpa);
    for (sids) |sid| try h2c.appendHeaders(gpa, &req, sid, "/stuck", true);
    try w.sendUnchecked(req.items);
}

fn sendRsts(gpa: std.mem.Allocator, w: *Wire, sids: []const u31) !void {
    var req: std.ArrayList(u8) = .empty;
    defer req.deinit(gpa);
    for (sids) |sid| try h2c.appendRst(gpa, &req, sid);
    try w.sendUnchecked(req.items);
}

const kib = 1024;
const block = 64 * kib;

/// 3 slots, and a connection whose request memory holds 5 first blocks: 3
/// running requests and 2 parked fill it.
fn parkLimits() starh2.Limits {
    var l = testLimits();
    l.max_streams_per_connection = 3;
    l.request_bytes_per_server = 4 * 1024 * kib;
    l.request_bytes_per_connection = 5 * block;
    l.request_arena_bytes = 4 * block;
    l.request_body_bytes = 64 * kib - 64;
    l.request_pool_retain_bytes = l.request_bytes_per_server;
    return l;
}

const ParkResult = struct {
    /// Request-pool bytes: 3 running and 2 parked (7, then 9).
    held_full: usize = 0,
    /// Just after 9, parked behind 7 (which is still waiting), was reset;
    /// no slot has freed.
    held_after_rst: usize = 0,
    ms_to_release: u64 = 0,
    client_after_rst: usize = 0,
    route_after_rst: usize = 0,
    ended_before_release: usize = 0,
    /// After 11 parked in 9's place; after 11 was reset too; after 13 parked.
    held_11: usize = 0,
    held_after_11: usize = 0,
    held_13: usize = 0,
    late: Seen = .{},
    err: ?anyerror = null,
};

fn parkClient(gpa: std.mem.Allocator, tr: Transport, rig: *Rig, r: *ParkResult) void {
    parkInner(gpa, tr, rig, r) catch |err| {
        r.err = err;
    };
}

fn heldIs(rig: *Rig, want: usize, timeout_ms: u64) bool {
    const deadline = nowNs() + timeout_ms * ms;
    while (rig.server.requests.held_bytes.load(.acquire) != want) {
        if (nowNs() >= deadline) return false;
        osSleepMs(1);
    }
    return true;
}

fn clientBytes(rig: *Rig) usize {
    var buf: [accounts.max_refs]accounts.Key = undefined;
    const k = accounts.keysFor(.{ .ip4 = .{ 127, 0, 0, 1 } }, 48, &buf)[0];
    const e = rig.server.clients.lookup(k) orelse return 0;
    return e.count(.request_bytes);
}

fn routeBytes(rig: *Rig) usize {
    if (comptime !@hasDecl(accounts, "RouteTable")) return 0;
    const rec = rig.server.accounting.routes.recordOf(1) orelse return 0;
    return rec.count(.request_bytes);
}

/// Three slots, all held by handlers that ignore cancel; two of their
/// streams reset, so two streams are free and no slot is. 7 and 9 park.
/// Reset 9, which waits behind 7 (the actor's per-turn sweep of the queue,
/// `dispatchParked`, stops at the first request still waiting, so before this
/// step 9's memory stayed until a slot freed): its lease must come back at
/// once. Then 11 parks in its place and is reset too, and 13 parks: the
/// queue (one place per slot) must have room, so neither reset request may
/// still be in it. Then the slots free: 5, 7 and 13 are answered, 9 and 11
/// never.
fn parkInner(gpa: std.mem.Allocator, tr: Transport, rig: *Rig, r: *ParkResult) !void {
    const w = try openQuick(gpa, tr, rig.port());
    defer w.close();
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var pre = try h2c.buildClientPrefaceAndSettings(gpa);
    defer pre.deinit(gpa);
    try w.sendUnchecked(pre.items);
    try sendGets(gpa, w, &.{ 1, 3, 5 });
    if (!waitStarted(3, 3_000)) return error.HandlersNotStarted;
    try sendRsts(gpa, w, &.{ 1, 3 });
    try sendGets(gpa, w, &.{ 7, 9 });
    if (!heldIs(rig, 5 * block, 2_000)) {
        std.debug.print("protocol work parked lease: the pool holds {d} bytes with 3 running and 2 parked, want {d}\n", .{ rig.server.requests.held_bytes.load(.acquire), 5 * block });
        return error.ParkedNotLeased;
    }
    r.held_full = rig.server.requests.held_bytes.load(.acquire);
    const t0 = nowNs();
    try sendRsts(gpa, w, &.{9});
    _ = heldIs(rig, 4 * block, 1_000);
    r.ms_to_release = (nowNs() - t0) / ms;
    r.held_after_rst = rig.server.requests.held_bytes.load(.acquire);
    r.client_after_rst = clientBytes(rig);
    r.route_after_rst = routeBytes(rig);
    r.ended_before_release = ended.load(.acquire);
    // The connection's own budget has room again: 11 parks, not refused.
    try sendGets(gpa, w, &.{11});
    _ = heldIs(rig, 5 * block, 1_000);
    r.held_11 = rig.server.requests.held_bytes.load(.acquire);
    try sendRsts(gpa, w, &.{11});
    _ = heldIs(rig, 4 * block, 1_000);
    r.held_after_11 = rig.server.requests.held_bytes.load(.acquire);
    try sendGets(gpa, w, &.{13});
    _ = heldIs(rig, 5 * block, 1_000);
    r.held_13 = rig.server.requests.held_bytes.load(.acquire);
    try readFor(w, &dec, &r.late, 200);
    // Now free the slots: the running handlers return, and the parked ones
    // run (and return at once).
    release.store(true, .release);
    const deadline = nowNs() + 3 * std.time.ns_per_s;
    while (nowNs() < deadline and (r.late.status[2] == null or r.late.status[3] == null or r.late.status[6] == null)) try readFor(w, &dec, &r.late, 50);
}

fn parkedLease(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    resetHandlers();
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, parkLimits());
    defer rig.stop();
    const p0 = parkedReleased();
    var r: ParkResult = .{};
    try blocking(parkClient, .{ gpa, tr, &rig, &r });
    const d = nowNs() + 3 * std.time.ns_per_s;
    while (rig.server.accounting.task_handlers.load(.acquire) != 0 and nowNs() < d) zio.sleep(.fromMilliseconds(2)) catch {};
    std.debug.print("protocol work parked lease ({s}): 3 running (2 of their streams reset) + 7 and 9 parked hold {d} KiB; 9 reset behind 7: {d} KiB {d} ms later, with {d} handlers ended (client {d} KiB, route {d} KiB); 11 parks: {d} KiB; 11 reset: {d} KiB; 13 parks: {d} KiB; released on RST {d}; answers 5:{?d} 7:{?d} 13:{?d} (9 and 11 never: {?d} {?d}, resets {?d} {?d}); err {?}\n", .{
        @tagName(tr),             r.held_full / kib,       r.held_after_rst / kib, r.ms_to_release, r.ended_before_release,
        r.client_after_rst / kib, r.route_after_rst / kib, r.held_11 / kib,        r.held_after_11 / kib,
        r.held_13 / kib,          parkedReleased() - p0,   r.late.status[2],       r.late.status[3],
        r.late.status[6],         r.late.status[4],        r.late.status[5],       r.late.rst[4],
        r.late.rst[6],            r.err,
    });
    try std.testing.expect(r.err == null);
    try expectExactly("request pool with 3 running and 2 parked", r.held_full, 5 * block);
    try expectExactly("request pool after 9 was reset", r.held_after_rst, 4 * block);
    try expectExactly("client bytes after the reset", r.client_after_rst, 4 * block);
    try expectExactly("route bytes after the reset (the running ones only)", r.route_after_rst, 3 * block);
    try expectExactly("handlers ended before the release", r.ended_before_release, 0);
    try expectExactly("request pool after 11 parked", r.held_11, 5 * block);
    try expectExactly("request pool after 11 was reset", r.held_after_11, 4 * block);
    try expectExactly("request pool after 13 parked", r.held_13, 5 * block);
    try expectExactly("parked leases released on RST", parkedReleased() - p0, 2);
    for ([_]usize{ 2, 3, 6 }) |i| try expectExactly("an answer", r.late.status[i] orelse 0, 200);
    try std.testing.expect(r.late.status[4] == null and r.late.status[5] == null);
    // Nothing released twice.
    try expectExactly("task handlers", rig.server.accounting.task_handlers.load(.acquire), 0);
    try expectExactly("server underflows", rig.server.accounting.underflows.load(.acquire), 0);
    try expectExactly("client table underflows", rig.server.clients.stats().underflows, 0);
    if (comptime @hasDecl(accounts, "RouteTable")) try expectExactly("route underflows", rig.server.accounting.routes.underflows.load(.acquire), 0);
}

test "a parked request whose stream is reset gives its lease back at once, before any slot frees, and only once (h2c)" {
    try runIn(parkedLease, 2, .h2c);
}
test "a parked request whose stream is reset gives its lease back at once, before any slot frees, and only once (TLS)" {
    try runIn(parkedLease, 2, .tls);
}

// ---------------------------------------------------------------------------
// 4. A parked request that no slot frees for.

fn parkedExpired() u64 {
    if (comptime !@hasDecl(conn_mod, "parked_expired")) return 0;
    return conn_mod.parked_expired.load(.acquire);
}

const wait_limit_ms = 500;

/// `parkLimits`, with a short idle timeout: the longest a request may wait
/// for a slot.
fn waitLimits() starh2.Limits {
    var l = parkLimits();
    l.connection_idle_timeout_ns = wait_limit_ms * ms;
    l.connection_idle_timeout_under_pressure_ns = l.connection_idle_timeout_ns;
    return l;
}

const WaitResult = struct {
    held_parked: usize = 0,
    /// From 7's HEADERS to its answer, or to giving up.
    ms_to_answer: u64 = 0,
    held_after: usize = 0,
    client_after: usize = 0,
    route_after: usize = 0,
    ended_before_release: usize = 0,
    seen: Seen = .{},
    err: ?anyerror = null,
};

fn waitClient(gpa: std.mem.Allocator, tr: Transport, rig: *Rig, r: *WaitResult) void {
    waitInner(gpa, tr, rig, r) catch |err| {
        r.err = err;
    };
}

/// Three slots, all held by handlers that ignore cancel, and all three of
/// their streams reset: nothing is open. 7 parks, and nothing else happens.
/// It must be answered 503 once it has waited the limit, not before and not
/// a limit later, and its lease must come back; then the slots free, and 7 is
/// not answered again.
fn waitInner(gpa: std.mem.Allocator, tr: Transport, rig: *Rig, r: *WaitResult) !void {
    const w = try openQuick(gpa, tr, rig.port());
    defer w.close();
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var pre = try h2c.buildClientPrefaceAndSettings(gpa);
    defer pre.deinit(gpa);
    try w.sendUnchecked(pre.items);
    try sendGets(gpa, w, &.{ 1, 3, 5 });
    if (!waitStarted(3, 3_000)) return error.HandlersNotStarted;
    try sendRsts(gpa, w, &.{ 1, 3, 5 });
    try sendGets(gpa, w, &.{7});
    const t0 = nowNs();
    if (!heldIs(rig, 4 * block, 2_000)) {
        std.debug.print("protocol work parked wait: the pool holds {d} bytes with 3 running and 1 parked, want {d}\n", .{ rig.server.requests.held_bytes.load(.acquire), 4 * block });
        return error.ParkedNotLeased;
    }
    r.held_parked = rig.server.requests.held_bytes.load(.acquire);
    const deadline = t0 + 4 * wait_limit_ms * ms;
    while (nowNs() < deadline and r.seen.headers[3] == 0 and r.seen.rst[3] == null and !r.seen.closed) try readFor(w, &dec, &r.seen, 20);
    r.ms_to_answer = (nowNs() - t0) / ms;
    _ = heldIs(rig, 3 * block, 1_000);
    r.held_after = rig.server.requests.held_bytes.load(.acquire);
    r.client_after = clientBytes(rig);
    r.route_after = routeBytes(rig);
    r.ended_before_release = ended.load(.acquire);
    // Free the slots: nothing may run 7 now, or release it again.
    release.store(true, .release);
    _ = heldIs(rig, 0, 2_000);
    try readFor(w, &dec, &r.seen, 200);
}

fn parkedWait(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    resetHandlers();
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, waitLimits());
    defer rig.stop();
    const e0 = parkedExpired();
    const p0 = parkedReleased();
    var r: WaitResult = .{};
    try blocking(waitClient, .{ gpa, tr, &rig, &r });
    const d = nowNs() + 3 * std.time.ns_per_s;
    while (rig.server.accounting.task_handlers.load(.acquire) != 0 and nowNs() < d) zio.sleep(.fromMilliseconds(2)) catch {};
    std.debug.print("protocol work parked wait ({s}): 3 slots held by handlers that ignore cancel, every stream reset, 7 parked ({d} KiB held), wait limit {d} ms: 7 got status {?d} (HEADERS {d}, rst {?d}) {d} ms after it was sent; then {d} KiB held (client {d} KiB, route {d} KiB), {d} handlers ended; expired {d}, released on RST {d}; err {?}\n", .{
        @tagName(tr),     r.held_parked / kib, wait_limit_ms,          r.seen.status[3],            r.seen.headers[3],
        r.seen.rst[3],    r.ms_to_answer,      r.held_after / kib,     r.client_after / kib,        r.route_after / kib,
        r.ended_before_release, parkedExpired() - e0, parkedReleased() - p0, r.err,
    });
    try std.testing.expect(r.err == null);
    try expectExactly("request pool with 3 running and 1 parked", r.held_parked, 4 * block);
    try expectExactly("7's status", r.seen.status[3] orelse 0, 503);
    try expectExactly("7's answers", r.seen.headers[3], 1);
    try std.testing.expect(r.seen.rst[3] == null);
    if (r.ms_to_answer < wait_limit_ms or r.ms_to_answer > wait_limit_ms + 300) {
        std.debug.print("protocol work parked wait: answered after {d} ms, want {d} to {d}\n", .{ r.ms_to_answer, wait_limit_ms, wait_limit_ms + 300 });
        return error.WrongWait;
    }
    try expectExactly("request pool after the 503", r.held_after, 3 * block);
    try expectExactly("client bytes after the 503", r.client_after, 3 * block);
    try expectExactly("route bytes after the 503 (the running ones only)", r.route_after, 3 * block);
    try expectExactly("handlers ended before the release", r.ended_before_release, 0);
    try expectExactly("parked requests expired", parkedExpired() - e0, 1);
    try expectExactly("parked leases released on RST", parkedReleased() - p0, 0);
    try expectExactly("task handlers", rig.server.accounting.task_handlers.load(.acquire), 0);
    try expectExactly("request pool at the end", rig.server.requests.held_bytes.load(.acquire), 0);
    try expectExactly("server underflows", rig.server.accounting.underflows.load(.acquire), 0);
    try expectExactly("client table underflows", rig.server.clients.stats().underflows, 0);
    if (comptime @hasDecl(accounts, "RouteTable")) try expectExactly("route underflows", rig.server.accounting.routes.underflows.load(.acquire), 0);
}

test "a parked request that no slot frees for is answered 503 after the wait limit, and its lease released once (h2c)" {
    try runIn(parkedWait, 2, .h2c);
}
test "a parked request that no slot frees for is answered 503 after the wait limit, and its lease released once (TLS)" {
    try runIn(parkedWait, 2, .tls);
}

// ---------------------------------------------------------------------------
// 5. Frames that cross our own RST_STREAM.

/// One raw frame.
fn appendFrame(gpa: std.mem.Allocator, buf: *std.ArrayList(u8), typ: u8, flags: u8, sid: u31, payload: []const u8) !void {
    var h: [9]u8 = undefined;
    std.mem.writeInt(u24, h[0..3], @intCast(payload.len), .big);
    h[3] = typ;
    h[4] = flags;
    std.mem.writeInt(u32, h[5..9], sid, .big);
    try buf.appendSlice(gpa, &h);
    try buf.appendSlice(gpa, payload);
}

/// A trailer block, `x-trailer: one`, as a literal WITH incremental indexing:
/// it adds entry 62 to the server's HPACK table, so a server that drops the
/// block without decoding it is out of step at the next block that uses 62.
const trailer_block = "\x40\x09x-trailer\x03one";
/// GET / that refers to entry 62: `:method GET`, `:scheme http`, `:path /`,
/// `:authority localhost` (literal, not indexed), then index 62.
const get_using_62 = "\x82\x86\x84\x01\x09localhost\xbe";

const END_STREAM: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
const PADDED: u8 = 0x8;

const Cross = enum {
    /// The trailers go after the client has read the 413 and the RST: they
    /// crossed the RST on the wire.
    after_rst,
    /// The same, the trailers in a HEADERS and a CONTINUATION.
    after_rst_fragmented,
    /// The trailer HEADERS (no END_HEADERS) goes with the body, so the
    /// server resets the stream while its header block is open; the
    /// CONTINUATION follows the RST.
    reset_mid_block,
};

fn crossClient(gpa: std.mem.Allocator, tr: Transport, port: u16, how: Cross, r: *RefusedResult) void {
    crossInner(gpa, tr, port, how, r) catch |err| {
        r.err = err;
    };
}

fn crossInner(gpa: std.mem.Allocator, tr: Transport, port: u16, how: Cross, r: *RefusedResult) !void {
    const w = try openQuick(gpa, tr, port);
    defer w.close();
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try h2c.appendHeadersExtra(gpa, &req, 1, "POST", "/up", false, &.{});
    const chunk: [16_000]u8 = @splat('x');
    try h2c.appendData(gpa, &req, 1, &chunk, false);
    try h2c.appendData(gpa, &req, 1, &chunk, false);
    const split = 5;
    if (how == .reset_mid_block) try appendFrame(gpa, &req, 0x1, END_STREAM, 1, trailer_block[0..split]);
    try w.sendUnchecked(req.items);
    const deadline = nowNs() + 2 * std.time.ns_per_s;
    while (nowNs() < deadline and (r.seen.status[0] == null or r.seen.rst[0] == null) and !r.seen.closed) try readFor(w, &dec, &r.seen, 20);
    req.clearRetainingCapacity();
    switch (how) {
        .after_rst => try appendFrame(gpa, &req, 0x1, END_STREAM | END_HEADERS, 1, trailer_block),
        .after_rst_fragmented => {
            try appendFrame(gpa, &req, 0x1, END_STREAM, 1, trailer_block[0..split]);
            try appendFrame(gpa, &req, 0x9, END_HEADERS, 1, trailer_block[split..]);
        },
        .reset_mid_block => try appendFrame(gpa, &req, 0x9, END_HEADERS, 1, trailer_block[split..]),
    }
    try appendFrame(gpa, &req, 0x1, END_STREAM | END_HEADERS, 3, get_using_62);
    try w.sendUnchecked(req.items);
    while (nowNs() < deadline + std.time.ns_per_s and r.seen.status[1] == null and !r.seen.closed and !r.seen.goaway) try readFor(w, &dec, &r.seen, 20);
    try readFor(w, &dec, &r.seen, 100);
}

/// Trailers that cross the RST_STREAM NO_ERROR the server sends after a 413
/// (RFC 9113 section 5.1: frames on a stream this side reset are discarded,
/// not a connection error). They are decoded (the next request uses the
/// table entry they add) and dropped: the next request is answered 200, the
/// upload's stream gets one 413 and one RST, and the connection stays up.
fn crossRst(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport, how: Cross) !void {
    var rig: Rig = undefined;
    var limits = testLimits();
    limits.request_body_bytes = 16 * kib;
    try rig.start(rt, gpa, tr, limits);
    defer rig.stop();
    var r: RefusedResult = .{};
    try blocking(crossClient, .{ gpa, tr, rig.port(), how, &r });
    std.debug.print("protocol work trailers crossing the RST ({s}, {s}): upload status {?d} (HEADERS {d}), rst {?d} x{d}; next request (uses the trailers' HPACK entry): status {?d}; GOAWAY {} code {d}, closed {}; err {?}\n", .{
        @tagName(tr), @tagName(how), r.seen.status[0], r.seen.headers[0], r.seen.rst[0], r.seen.rst_n[0], r.seen.status[1], r.seen.goaway, r.seen.goaway_code, r.seen.closed, r.err,
    });
    try std.testing.expect(r.err == null);
    try expectExactly("the upload's status", r.seen.status[0] orelse 0, 413);
    try expectExactly("the upload's answers", r.seen.headers[0], 1);
    try expectExactly("the upload's RSTs", r.seen.rst_n[0], 1);
    try expectExactly("the upload's reset code (NO_ERROR)", r.seen.rst[0] orelse 99, @intFromEnum(frame.ErrorCode.no_error));
    try expectExactly("the next request's status", r.seen.status[1] orelse 0, 200);
    try std.testing.expect(!r.seen.goaway and !r.seen.closed);
}

fn crossAfterRst(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return crossRst(rt, gpa, tr, .after_rst);
}
fn crossAfterRstFragmented(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return crossRst(rt, gpa, tr, .after_rst_fragmented);
}
fn crossResetMidBlock(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return crossRst(rt, gpa, tr, .reset_mid_block);
}

test "trailers that cross the RST after a 413 are decoded and dropped; the connection stays up (h2c)" {
    try runIn(crossAfterRst, 2, .h2c);
}
test "trailers that cross the RST after a 413 are decoded and dropped; the connection stays up (TLS)" {
    try runIn(crossAfterRst, 2, .tls);
}
test "fragmented trailers that cross the RST after a 413 are decoded and dropped (h2c)" {
    try runIn(crossAfterRstFragmented, 2, .h2c);
}
test "fragmented trailers that cross the RST after a 413 are decoded and dropped (TLS)" {
    try runIn(crossAfterRstFragmented, 2, .tls);
}
test "a stream reset inside its open trailer block gets one RST, and the block is decoded and dropped (h2c)" {
    try runIn(crossResetMidBlock, 2, .h2c);
}
test "a stream reset inside its open trailer block gets one RST, and the block is decoded and dropped (TLS)" {
    try runIn(crossResetMidBlock, 2, .tls);
}

const PeerClosed = enum {
    /// The client reset stream 1 itself, then sends HEADERS on it.
    peer_reset,
    /// Stream 1 ended both ways (a 200), then the client sends HEADERS on it.
    ended,
};

fn peerClosedClient(gpa: std.mem.Allocator, tr: Transport, port: u16, how: PeerClosed, r: *RefusedResult) void {
    peerClosedInner(gpa, tr, port, how, r) catch |err| {
        r.err = err;
    };
}

fn peerClosedInner(gpa: std.mem.Allocator, tr: Transport, port: u16, how: PeerClosed, r: *RefusedResult) !void {
    const w = try openQuick(gpa, tr, port);
    defer w.close();
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    const deadline = nowNs() + 2 * std.time.ns_per_s;
    switch (how) {
        .peer_reset => {
            try h2c.appendHeadersExtra(gpa, &req, 1, "POST", "/up", false, &.{});
            try h2c.appendRst(gpa, &req, 1);
            try w.sendUnchecked(req.items);
            try readFor(w, &dec, &r.seen, 100);
        },
        .ended => {
            try h2c.appendHeaders(gpa, &req, 1, "/", true);
            try w.sendUnchecked(req.items);
            while (nowNs() < deadline and r.seen.status[0] == null and !r.seen.closed) try readFor(w, &dec, &r.seen, 20);
        },
    }
    req.clearRetainingCapacity();
    try appendFrame(gpa, &req, 0x1, END_STREAM | END_HEADERS, 1, trailer_block);
    try w.sendUnchecked(req.items);
    while (nowNs() < deadline + std.time.ns_per_s and !r.seen.goaway and !r.seen.closed) try readFor(w, &dec, &r.seen, 20);
}

/// HEADERS on a stream the PEER closed (its own RST, or both ends ended) is
/// still a connection error: STREAM_CLOSED (RFC 9113 section 5.1, h2spec
/// 5.1/12). Only a stream this side reset discards it.
fn peerClosedHeaders(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, testLimits());
    defer rig.stop();
    inline for (.{ PeerClosed.peer_reset, PeerClosed.ended }) |how| {
        var r: RefusedResult = .{};
        try blocking(peerClosedClient, .{ gpa, tr, rig.port(), how, &r });
        std.debug.print("protocol work HEADERS on a stream the peer closed ({s}, {s}): GOAWAY {} code {d}; err {?}\n", .{ @tagName(tr), @tagName(how), r.seen.goaway, r.seen.goaway_code, r.err });
        try std.testing.expect(r.err == null);
        try std.testing.expect(r.seen.goaway);
        try expectExactly("the GOAWAY code (STREAM_CLOSED)", r.seen.goaway_code, @intFromEnum(frame.ErrorCode.stream_closed));
    }
}

test "HEADERS on a stream the peer reset or ended is still a connection error STREAM_CLOSED (h2c)" {
    try runIn(peerClosedHeaders, 2, .h2c);
}
test "HEADERS on a stream the peer reset or ended is still a connection error STREAM_CLOSED (TLS)" {
    try runIn(peerClosedHeaders, 2, .tls);
}

// ---------------------------------------------------------------------------
// 6. DATA on a stream whose request has ended (half-closed remote).

const LateData = enum { empty, body };

const LateResult = struct {
    seen: Seen = .{},
    /// Handlers ended before the client let them go.
    ended_before_release: usize = 0,
    err: ?anyerror = null,
};

fn lateClient(gpa: std.mem.Allocator, tr: Transport, rig: *Rig, how: LateData, r: *LateResult) void {
    lateInner(gpa, tr, rig, how, r) catch |err| {
        r.err = err;
    };
}

/// GET /stuck with END_STREAM: the request is dispatched and its handler
/// holds the response. Then a DATA on that stream, which RFC 9113 section
/// 5.1 makes a stream error STREAM_CLOSED. Then a GET / on stream 3.
fn lateInner(gpa: std.mem.Allocator, tr: Transport, rig: *Rig, how: LateData, r: *LateResult) !void {
    const w = try openQuick(gpa, tr, rig.port());
    defer w.close();
    var dec = try hpack.Decoder.init(gpa);
    defer dec.deinit();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try h2c.appendHeaders(gpa, &req, 1, "/stuck", true);
    try w.sendUnchecked(req.items);
    if (!waitStarted(1, 3_000)) return error.HandlerNotStarted;
    req.clearRetainingCapacity();
    switch (how) {
        .empty => try h2c.appendData(gpa, &req, 1, &.{}, false),
        .body => try h2c.appendData(gpa, &req, 1, "late body", false),
    }
    try w.sendUnchecked(req.items);
    try readFor(w, &dec, &r.seen, 200);
    req.clearRetainingCapacity();
    try h2c.appendHeaders(gpa, &req, 3, "/", true);
    try w.sendUnchecked(req.items);
    const deadline = nowNs() + 2 * std.time.ns_per_s;
    while (nowNs() < deadline and r.seen.status[1] == null and !r.seen.closed and !r.seen.goaway) try readFor(w, &dec, &r.seen, 20);
    r.ended_before_release = ended.load(.acquire);
}

fn lateData(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport, how: LateData) !void {
    resetHandlers();
    var rig: Rig = undefined;
    try rig.start(rt, gpa, tr, testLimits());
    defer rig.stop();
    var r: LateResult = .{};
    try blocking(lateClient, .{ gpa, tr, &rig, how, &r });
    // Let the held handler go; it must return on its own.
    release.store(true, .release);
    const d = nowNs() + 3 * std.time.ns_per_s;
    while (rig.server.accounting.task_handlers.load(.acquire) != 0 and nowNs() < d) zio.sleep(.fromMilliseconds(2)) catch {};
    std.debug.print("protocol work late DATA ({s}, {s}): stream 1 rst {?d} x{d}, HEADERS {d}; handlers ended before release {d}, after {d}; next request status {?d}; GOAWAY {} code {d} closed {}; task handlers {d}, pool {d}; err {?}\n", .{
        @tagName(tr),        @tagName(how),     r.seen.rst[0],                                         r.seen.rst_n[0],        r.seen.headers[0],
        r.ended_before_release, ended.load(.acquire), r.seen.status[1], r.seen.goaway, r.seen.goaway_code, r.seen.closed, rig.server.accounting.task_handlers.load(.acquire), rig.server.requests.held_bytes.load(.acquire), r.err,
    });
    try std.testing.expect(r.err == null);
    try expectExactly("stream 1's RSTs", r.seen.rst_n[0], 1);
    try expectExactly("stream 1's reset code (STREAM_CLOSED)", r.seen.rst[0] orelse 99, @intFromEnum(frame.ErrorCode.stream_closed));
    try expectExactly("handlers ended before the release", r.ended_before_release, 0);
    try expectExactly("handlers ended after the release", ended.load(.acquire), 1);
    try expectExactly("the next request's status", r.seen.status[1] orelse 0, 200);
    try std.testing.expect(!r.seen.goaway and !r.seen.closed);
    try expectExactly("task handlers", rig.server.accounting.task_handlers.load(.acquire), 0);
    try expectExactly("request pool", rig.server.requests.held_bytes.load(.acquire), 0);
    try expectExactly("server underflows", rig.server.accounting.underflows.load(.acquire), 0);
    try expectExactly("client table underflows", rig.server.clients.stats().underflows, 0);
    if (comptime @hasDecl(accounts, "RouteTable")) try expectExactly("route underflows", rig.server.accounting.routes.underflows.load(.acquire), 0);
}

fn lateEmpty(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return lateData(rt, gpa, tr, .empty);
}
fn lateBody(rt: *zio.Runtime, gpa: std.mem.Allocator, tr: Transport) !void {
    return lateData(rt, gpa, tr, .body);
}

test "an empty DATA on a dispatched request's stream is RST STREAM_CLOSED, and nothing else is harmed (h2c)" {
    try runIn(lateEmpty, 2, .h2c);
}
test "an empty DATA on a dispatched request's stream is RST STREAM_CLOSED, and nothing else is harmed (TLS)" {
    try runIn(lateEmpty, 2, .tls);
}
test "a DATA with a body on a dispatched request's stream is RST STREAM_CLOSED, and nothing else is harmed (h2c)" {
    try runIn(lateBody, 2, .h2c);
}
test "a DATA with a body on a dispatched request's stream is RST STREAM_CLOSED, and nothing else is harmed (TLS)" {
    try runIn(lateBody, 2, .tls);
}
