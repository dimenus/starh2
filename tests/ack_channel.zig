//! Gate for t-3561: an h2c connection's write-ack channel must never overflow.
//!
//! The WritePump posts one completion per chunk into `write_ack_ch` with
//! `trySend`, and a full channel is a panic that ends the process. Nothing on
//! the producer side was bounded by the channel's size: the pump frees a
//! chunk's queue slot and its pool chunk BEFORE it posts, and a chunk holds
//! only its own wire bytes until the actor applies the completion. So when
//! the one receiver (the actor) fell behind the drains that feed the pump,
//! the channel filled (`h2load -c 1 -m 200` over 64 KiB task responses).
//!
//! The test makes the receiver slow on purpose: `test_hold_ack_receive`
//! keeps the actor from taking completions while it still ingests frames
//! and drains responses. Holding completions also holds what they release
//! (wire bytes, control-pool entries), so the chunks must be many and small,
//! or the control pool fills first and the server resets streams instead.
//! The client advertises a 100-byte stream window and returns credit for
//! every DATA frame it reads, so each 2,000-byte response leaves as 20
//! chunks of 100 bytes, one per WINDOW_UPDATE: about 630 completions per
//! wave of 30 streams, whatever the timing. Without a bound the pump panics
//! on the first wave and the process aborts. With the ack-room pause the
//! drain stops short of the channel's capacity and responses stop; the test
//! sees them stop, clears the hold, has the client wake the actor with a
//! PING, and every response arrives.
//!
//! The third case covers the one push a handler makes outside a drain: a
//! `Body.flush()` with nothing pending is a bare flush barrier, pushed
//! directly. With the channel already at the pause, 40 such barriers must
//! wait for room (`waitForAckRoom`); pushed directly they pass it.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const conn_mod = starh2.edge.connection;
const frame = starh2.core.frame;
const dummy: u8 = 0;

const body_bytes = 2000;
const stream_window = 100;

fn body(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const bytes: [body_bytes]u8 = @splat('b');
    try resp.send(200, &.{}, &bytes);
}

/// A handler that flushes with nothing pending, twice: each `flush` is a bare
/// flush barrier, the one push a handler makes outside a drain. It starts its
/// response, then waits for `flush_gate`: `start` waits for its HEADERS to be
/// written, so the response must start before completions are held, and the
/// flushes must come after the channel is full.
var flush_gate: std.atomic.Value(bool) = .init(false);
var flushers_started: std.atomic.Value(usize) = .init(0);

fn flusher(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var b = try resp.start(200, &.{});
    _ = flushers_started.fetchAdd(1, .acq_rel);
    while (!flush_gate.load(.acquire)) try zio.sleep(.fromMilliseconds(5));
    try b.flush();
    try b.flush();
    try b.finish();
}

/// One wave is 30 x (2,000 / 100 + 1) = 630 chunks, more than the default
/// channel holds (512), with 30 control-pool entries and about 70 KB of wire.
const wave = 30;
const body_waves = 4;
/// Flushing streams, more than the room the pause leaves above DATA, so
/// their barriers pass it unless they wait.
const flushers = 40;
const client_deadline_ns = 30 * std.time.ns_per_s;

const ftype_data = 0;
const ftype_headers = 1;
const ftype_rst_stream = 3;
const ftype_settings = 4;
const ftype_goaway = 7;

const Client = struct {
    port: u16,
    /// `/body` requests, sent in waves of `wave` once `body_go` is set.
    body_total: usize,
    body_go: std.atomic.Value(bool),
    /// `/flush` requests, sent at once when `flush_go` is set.
    flush_total: usize,
    flush_go: std.atomic.Value(bool),
    body_answered: std.atomic.Value(usize) = .init(0),
    flush_answered: std.atomic.Value(usize) = .init(0),
    /// DATA frames read. Fair scheduling moves every stream together, so
    /// while completions are held no stream may finish; frames are what
    /// show the hold let traffic through before it stopped it.
    data_frames: std.atomic.Value(usize) = .init(0),
    /// Set by the test after it clears the hold: the client sends one PING,
    /// the way a peer's next frame would wake an idle actor.
    ping: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,

    fn answered(c: *const Client) usize {
        return c.body_answered.load(.acquire) + c.flush_answered.load(.acquire);
    }
};

fn runClient(c: *Client) void {
    clientBody(c) catch |err| {
        c.err = err;
    };
    c.done.store(true, .release);
}

fn sendAll(fd: c_int, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = std.c.send(fd, bytes[off..].ptr, bytes.len - off, 0);
        if (n <= 0) return error.Send;
        off += @intCast(n);
    }
}

fn nowNs() u64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

/// Sends the `/body` waves (the next one once the last is answered) and the
/// `/flush` streams when told to, answers the server's SETTINGS, returns
/// credit for every DATA frame, and counts every stream that ends. A GOAWAY,
/// a RST_STREAM or a close is an error.
fn clientBody(c: *Client) !void {
    const gpa = std.heap.page_allocator;
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.Socket;
    defer _ = std.c.close(fd);
    var addr: std.c.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, c.port),
        .addr = std.mem.nativeToBig(u32, 0x7f000001),
    };
    if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
    const tv: std.c.timeval = .{ .sec = 0, .usec = 50 * 1000 };
    _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVTIMEO, std.mem.asBytes(&tv), @sizeOf(std.c.timeval));

    {
        var pre: [frame.CLIENT_PREFACE.len + 64]u8 = undefined;
        @memcpy(pre[0..frame.CLIENT_PREFACE.len], frame.CLIENT_PREFACE);
        const settings = [_]frame.Setting{
            .{ .id = .max_concurrent_streams, .value = 256 },
            .{ .id = .initial_window_size, .value = stream_window },
        };
        const sn = try frame.Serializer.settingsFrame(pre[frame.CLIENT_PREFACE.len..], false, &settings);
        try sendAll(fd, pre[0 .. frame.CLIENT_PREFACE.len + sn]);
    }

    var in: std.ArrayList(u8) = .empty;
    defer in.deinit(gpa);
    var next_sid: u31 = 1;
    var body_sent: usize = 0;
    var flush_sent = false;
    // The `/flush` streams take consecutive ids from here.
    var flush_lo: u31 = 0;
    const deadline = nowNs() + client_deadline_ns;
    while (c.body_answered.load(.acquire) < c.body_total or c.flush_answered.load(.acquire) < c.flush_total) {
        if (nowNs() >= deadline) return error.ClientTimeout;
        if (c.flush_total != 0 and !flush_sent and c.flush_go.load(.acquire)) {
            var w: std.ArrayList(u8) = .empty;
            defer w.deinit(gpa);
            flush_lo = next_sid;
            for (0..c.flush_total) |_| {
                try h2c.appendHeaders(gpa, &w, next_sid, "/flush", true);
                next_sid += 2;
            }
            try sendAll(fd, w.items);
            flush_sent = true;
        }
        if (c.body_go.load(.acquire) and body_sent < c.body_total and body_sent == c.body_answered.load(.acquire)) {
            var w: std.ArrayList(u8) = .empty;
            defer w.deinit(gpa);
            const n = @min(wave, c.body_total - body_sent);
            for (0..n) |_| {
                try h2c.appendHeaders(gpa, &w, next_sid, "/body", true);
                next_sid += 2;
            }
            try sendAll(fd, w.items);
            body_sent += n;
        }
        if (c.ping.swap(false, .acq_rel)) {
            var pbuf: [17]u8 = undefined;
            const pn = try frame.Serializer.ping(&pbuf, false, &@as([8]u8, @splat(0)));
            try sendAll(fd, pbuf[0..pn]);
        }
        var buf: [16 * 1024]u8 = undefined;
        const got = std.c.recv(fd, &buf, buf.len, 0);
        if (got == 0) return error.ServerClosed;
        if (got < 0) switch (std.c.errno(got)) {
            .AGAIN, .INTR => continue,
            else => return error.Recv,
        };
        try in.appendSlice(gpa, buf[0..@intCast(got)]);
        var credit: std.ArrayList(u8) = .empty;
        defer credit.deinit(gpa);
        var off: usize = 0;
        while (in.items.len - off >= frame.FRAME_HEADER_LEN) {
            const hdr = frame.FrameHeader.decode(in.items[off..][0..frame.FRAME_HEADER_LEN]);
            const len = frame.FRAME_HEADER_LEN + @as(usize, hdr.length);
            if (in.items.len - off < len) break;
            const ftype = @intFromEnum(hdr.type);
            const flags = in.items[off + 4];
            switch (ftype) {
                ftype_settings => if (flags & 0x1 == 0) {
                    var abuf: [frame.FRAME_HEADER_LEN]u8 = undefined;
                    const an = try frame.Serializer.settingsFrame(&abuf, true, &.{});
                    try sendAll(fd, abuf[0..an]);
                },
                ftype_data, ftype_headers => {
                    const ended = flags & 0x1 != 0;
                    if (ftype == ftype_data) _ = c.data_frames.fetchAdd(1, .acq_rel);
                    // Return the credit at once, one frame at a time: the
                    // next DATA frame on this stream is then one more chunk.
                    if (ftype == ftype_data and hdr.length != 0) {
                        const incr: u31 = @intCast(hdr.length);
                        try h2c.appendWindowUpdate(gpa, &credit, 0, incr);
                        if (!ended) try h2c.appendWindowUpdate(gpa, &credit, hdr.stream_id, incr);
                    }
                    if (ended and hdr.stream_id != 0) {
                        const is_flush = flush_sent and hdr.stream_id >= flush_lo and hdr.stream_id < flush_lo + 2 * c.flush_total;
                        if (is_flush) {
                            _ = c.flush_answered.fetchAdd(1, .acq_rel);
                        } else {
                            _ = c.body_answered.fetchAdd(1, .acq_rel);
                        }
                    }
                },
                ftype_rst_stream => {
                    const code = std.mem.readInt(u32, in.items[off + frame.FRAME_HEADER_LEN ..][0..4], .big);
                    std.debug.print("ack channel client: RST_STREAM on stream {d}, code {d}\n", .{ hdr.stream_id, code });
                    return error.StreamReset;
                },
                ftype_goaway => return error.GoAway,
                else => {},
            }
            off += len;
        }
        if (credit.items.len != 0) try sendAll(fd, credit.items);
        const rest = in.items.len - off;
        std.mem.copyForwards(u8, in.items[0..rest], in.items[off..]);
        in.shrinkRetainingCapacity(rest);
    }
}

const Case = enum { body_waves, flush_barriers };

/// Wait, by yielding, until DATA has not moved for 300 ms: with completions
/// held, the pause is what stops it.
fn waitDataStill(client: *const Client) void {
    var last = client.data_frames.load(.acquire);
    var still_since = nowNs();
    const deadline = nowNs() + 10 * std.time.ns_per_s;
    while (!client.done.load(.acquire) and nowNs() < deadline) {
        zio.sleep(.fromMilliseconds(10)) catch {};
        const now_frames = client.data_frames.load(.acquire);
        if (now_frames != last) {
            last = now_frames;
            still_since = nowNs();
        } else if (nowNs() - still_since >= 300 * std.time.ns_per_ms) return;
    }
}

fn runCase(rt: *zio.Runtime, gpa: std.mem.Allocator, outbound_bytes: usize, case: Case) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/body", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = body } } },
        .{ .method = .GET, .path = "/flush", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = flusher } } },
    };
    var limits = starh2.Limits.defaults;
    limits.outbound_bytes_per_connection = outbound_bytes;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);

    conn_mod.test_ack_room_pauses.store(0, .release);
    if (comptime @hasDecl(conn_mod, "test_ack_room_waits")) conn_mod.test_ack_room_waits.store(0, .release);
    if (comptime @hasDecl(conn_mod, "test_ack_bound_refusals")) conn_mod.test_ack_bound_refusals.store(0, .release);
    flush_gate.store(false, .release);
    flushers_started.store(0, .release);
    defer flush_gate.store(true, .release);
    defer conn_mod.test_hold_ack_receive.store(false, .release);

    const flushing = case == .flush_barriers;
    var client: Client = .{
        .port = server.localAddress(0).getPort(),
        .body_total = if (flushing) wave else body_waves * wave,
        .body_go = .init(!flushing),
        .flush_total = if (flushing) flushers else 0,
        .flush_go = .init(flushing),
    };
    if (!flushing) conn_mod.test_hold_ack_receive.store(true, .release);
    const thread = try std.Thread.spawn(.{}, runClient, .{&client});

    if (flushing) {
        // Start every flushing response before completions are held: `start`
        // waits for its HEADERS to be written.
        const deadline = nowNs() + 5 * std.time.ns_per_s;
        while (flushers_started.load(.acquire) < flushers and !client.done.load(.acquire) and nowNs() < deadline) {
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        conn_mod.test_hold_ack_receive.store(true, .release);
        client.body_go.store(true, .release);
    }
    waitDataStill(&client);
    const frames_while_held = client.data_frames.load(.acquire);
    if (flushing) {
        // The channel is at the pause. Each flushing stream's barriers must
        // now wait for room instead of being pushed past it.
        flush_gate.store(true, .release);
        const settle = nowNs() + 300 * std.time.ns_per_ms;
        while (nowNs() < settle and !client.done.load(.acquire)) zio.sleep(.fromMilliseconds(10)) catch {};
    }
    const answered_while_held = client.answered();
    const client_ended_early = client.done.load(.acquire);
    const started = flushers_started.load(.acquire);

    conn_mod.test_hold_ack_receive.store(false, .release);
    client.ping.store(true, .release);
    // Wait by yielding, and join only once the client is done: a blocking
    // join holds this executor's thread, and the connection's actor and
    // pumps may be placed on it.
    while (!client.done.load(.acquire)) zio.sleep(.fromMilliseconds(10)) catch {};
    thread.join();

    const want = client.body_total + client.flush_total;
    const waits: usize = if (comptime @hasDecl(conn_mod, "test_ack_room_waits")) conn_mod.test_ack_room_waits.load(.acquire) else 0;
    const refusals: usize = if (comptime @hasDecl(conn_mod, "test_ack_bound_refusals")) conn_mod.test_ack_bound_refusals.load(.acquire) else 0;
    std.debug.print("ack channel {s}: outbound={d} answered {d} of {d}, {d} DATA frames while completions were held, {d} held pops, {d} barrier waits, {d} refused pushes\n", .{ @tagName(case), outbound_bytes, client.answered(), want, frames_while_held, conn_mod.test_ack_room_pauses.load(.acquire), waits, refusals });
    if (client.err) |err| {
        std.debug.print("ack channel client failed: {s} (answered {d} of {d}, {d} while held)\n", .{ @errorName(err), client.answered(), want, answered_while_held });
        return err;
    }
    // The hold must have stopped responses short of the total, and only
    // after traffic flowed through it: a client that finished while acks
    // were held, or a stall before half a channel's worth of DATA arrived,
    // means the hold proved nothing.
    const capacity = limits.control_entries_per_connection + limits.max_streams_per_connection;
    try std.testing.expect(!client_ended_early);
    if (flushing) try std.testing.expectEqual(@as(usize, flushers), started);
    try std.testing.expect(frames_while_held >= capacity / 2);
    try std.testing.expect(answered_while_held < want);
    try std.testing.expectEqual(want, client.answered());
    if (comptime conn_mod.test_observe) {
        try std.testing.expect(conn_mod.test_ack_room_pauses.load(.acquire) > 0);
        // A refused push fails the connection closed, so the client would
        // have failed already; this names the cause.
        try std.testing.expectEqual(@as(usize, 0), refusals);
        if (flushing) try std.testing.expect(waits > 0);
    }
}

fn runInRuntime(outbound_bytes: usize, case: Case) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(4) });
    defer rt.deinit();
    var handle = try rt.spawn(runCase, .{ rt, gpa, outbound_bytes, case });
    try handle.join();
}

test "h2c write-ack channel never overflows while the actor is slow to take completions (default outbound budget)" {
    try runInRuntime(starh2.Limits.defaults.outbound_bytes_per_connection, .body_waves);
}

test "h2c write-ack channel never overflows while the actor is slow to take completions (doubled outbound budget)" {
    try runInRuntime(2 * starh2.Limits.defaults.outbound_bytes_per_connection, .body_waves);
}

test "h2c bare flush barriers wait for write-ack room instead of passing the pause (default limits)" {
    try runInRuntime(starh2.Limits.defaults.outbound_bytes_per_connection, .flush_barriers);
}
