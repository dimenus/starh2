//! Protocol work that holds nothing (t-3759 step 6), on real sockets, h2c and
//! TLS: DATA that delivers no body to a live stream is charged to the non-DATA
//! frame bucket; a 1-byte DATA flood and what bounds it; a browser-like load;
//! padding and the body minimum rate.
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
    err: ?anyerror = null,
};

const FloodKind = enum {
    /// Zero-length DATA on a live stream.
    empty,
    /// DATA on a stream the client reset first.
    closed_stream,
    /// 1-byte DATA on a live stream, as fast as flow control allows.
    one_byte,
    /// `one_byte`, going on after the server answers the stream.
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
                0x3 => if (s.sid == 1 and kind != .closed_stream) {
                    r.end = .rst;
                    r.code = if (p.len >= 4) std.mem.readInt(u32, p[0..4], .big) else 0;
                    r.elapsed_ms = (nowNs() - t0) / ms;
                    return;
                },
                0x1 => if (s.sid == 1 and kind != .closed_stream and s.flags & 1 != 0) {
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
                .closed_stream, .one_byte, .one_byte_on => {
                    if (cr.conn < 1 or (kind != .closed_stream and cr.stream < 1)) break;
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
    inline for (.{ FloodKind.empty, FloodKind.closed_stream }) |kind| {
        var r: Flood = .{};
        try blocking(floodClient, .{ gpa, tr, rig.port(), kind, 5_000, &r });
        var ok = false;
        try blocking(goodGet, .{ gpa, tr, rig.port(), &ok });
        std.debug.print("protocol work flood ({s}) {s}: ended {s} code {d} after {d} ms and {d} frames sent; a second client served: {}; err {?}\n", .{ @tagName(tr), @tagName(kind), @tagName(r.end), r.code, r.elapsed_ms, r.frames, ok, r.err });
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
    try std.testing.expect(r.body_bytes > limits.request_body_bytes);
}

test "a 1-byte DATA flood on one executor ends at the stream's body cap, and what it costs (h2c)" {
    try runIn(oneByteFlood, 1, .h2c);
}
test "a 1-byte DATA flood on one executor ends at the stream's body cap, and what it costs (TLS)" {
    try runIn(oneByteFlood, 1, .tls);
}

/// A well-behaved GET on a connection of its own: true when answered 200.
fn goodGet(gpa: std.mem.Allocator, tr: Transport, port: u16, ok: *bool) void {
    ok.* = goodGetInner(gpa, tr, port) catch false;
}

fn goodGetInner(gpa: std.mem.Allocator, tr: Transport, port: u16) !bool {
    const w = try Wire.open(gpa, tr, port);
    defer w.close();
    var pre = try h2c.buildClientPrefaceAndSettings(gpa);
    defer pre.deinit(gpa);
    try h2c.appendHeaders(gpa, &pre, 1, "/", true);
    try w.send(pre.items);
    const deadline = nowNs() + 3 * std.time.ns_per_s;
    while (true) {
        const s = (w.next(deadline) catch return false) orelse return false;
        if (s.sid == 1 and (s.ftype == 0x0 or s.ftype == 0x1) and s.flags & 1 != 0) return true;
    }
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
