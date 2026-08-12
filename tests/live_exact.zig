//! Exact live gates — H2 frame parser, bounded result channel, cause taxonomy.
//! No vacuous ors; >cap capacity wait; credit-exact WINDOW_UPDATE.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const dummy: u8 = 0;
const BODY_70K = 70 * 1024;
const BODY_192K = 192 * 1024;

const HandlerEvt = struct {
    code: u8 = 0, // 0=ok 1=WriteFailed 2=ConnectionClosed 3=PeerReset 4=SlowConsumer 5=other
    peer_code: u32 = 0,
    entered_ns: u64 = 0,
    returned_ns: u64 = 0,
};

/// Bounded result channel — no global last-value races across rounds.
const ResultCh = struct {
    slots: [8]HandlerEvt = [_]HandlerEvt{.{}} ** 8,
    w: std.atomic.Value(u32) = .init(0),
    r: std.atomic.Value(u32) = .init(0),

    fn reset(self: *ResultCh) void {
        self.w.store(0, .release);
        self.r.store(0, .release);
    }

    fn push(self: *ResultCh, e: HandlerEvt) void {
        const i = self.w.fetchAdd(1, .acq_rel);
        self.slots[i % self.slots.len] = e;
    }

    fn tryPop(self: *ResultCh) ?HandlerEvt {
        const r = self.r.load(.acquire);
        const w = self.w.load(.acquire);
        if (r >= w) return null;
        if (self.r.cmpxchgStrong(r, r + 1, .acq_rel, .acquire) != null) return null;
        return self.slots[r % self.slots.len];
    }

    fn waitOne(self: *ResultCh, timeout_ms: u64) !HandlerEvt {
        var waited: u64 = 0;
        while (waited < timeout_ms) : (waited += 5) {
            if (self.tryPop()) |e| return e;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        return error.Timeout;
    }
};

var result_ch: ResultCh = .{};
var handler_entered: std.atomic.Value(bool) = .init(false);

fn codeOf(err: anyerror) u8 {
    return switch (err) {
        error.WriteFailed => 1,
        error.ConnectionClosed => 2,
        error.PeerReset => 3,
        error.SlowConsumer => 4,
        else => 5,
    };
}

fn writeAllStream(stream: zio.net.Stream, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = try stream.write(bytes[off..], .none);
        off += n;
    }
}

fn readAvailable(stream: zio.net.Stream, bytes: []u8) !usize {
    return stream.read(bytes, .{ .duration = .fromMilliseconds(100) }) catch |err| switch (err) {
        error.Timeout => 0,
        else => return err,
    };
}

/// Incremental client-side H2 frame accumulator for live gates.
const ClientH2 = struct {
    gpa: std.mem.Allocator,
    parser: starh2.core.frame.Parser,
    body: std.ArrayList(u8) = .empty,
    end_stream_count: usize = 0,
    data_frames: usize = 0,
    saw_rst: bool = false,
    saw_goaway: bool = false,
    stream_credit: i32,
    conn_credit: i32,
    target_sid: u31,
    writer_done_ns: u64 = 0,
    settings_ack: std.ArrayList(u8) = .empty,

    fn init(gpa: std.mem.Allocator, sid: u31, initial_win: i32) !ClientH2 {
        var parser = starh2.core.frame.Parser.init(gpa, starh2.core.frame.DEFAULT_MAX_FRAME_SIZE);
        parser.skipPreface();
        return .{
            .gpa = gpa,
            .parser = parser,
            .stream_credit = initial_win,
            .conn_credit = 1 << 30,
            .target_sid = sid,
        };
    }

    fn deinit(self: *ClientH2) void {
        self.body.deinit(self.gpa);
        self.settings_ack.deinit(self.gpa);
        self.parser.deinit();
    }

    fn ingest(self: *ClientH2, bytes: []const u8) !void {
        const frame = starh2.core.frame;
        var rem = bytes;
        while (rem.len > 0) {
            const maybe = try self.parser.ingestOne(rem);
            if (maybe) |r| {
                defer if (r.event.payload.len != 0) self.gpa.free(r.event.payload);
                rem = rem[r.consumed..];
                const hdr = r.event.header;
                switch (hdr.type) {
                    .data => {
                        if (hdr.stream_id != self.target_sid) continue;
                        const n: i32 = @intCast(r.event.payload.len);
                        if (n > self.stream_credit or n > self.conn_credit) return error.CreditExceeded;
                        self.stream_credit -= n;
                        self.conn_credit -= n;
                        try self.body.appendSlice(self.gpa, r.event.payload);
                        self.data_frames += 1;
                        if (hdr.flags.end_stream) {
                            self.end_stream_count += 1;
                            self.writer_done_ns = zio.Timestamp.now(.monotonic).toNanoseconds();
                        }
                    },
                    .settings => {
                        if (!hdr.flags.end_stream) {
                            // Non-ACK SETTINGS → reply with ACK (end_stream flag used as ACK in this codebase).
                            var sbuf: [9]u8 = undefined;
                            const sn = try frame.Serializer.settingsFrame(&sbuf, true, &.{});
                            try self.settings_ack.appendSlice(self.gpa, sbuf[0..sn]);
                        }
                    },
                    .rst_stream => self.saw_rst = true,
                    .goaway => self.saw_goaway = true,
                    .window_update, .headers, .ping, .priority, .push_promise, .continuation => {},
                    _ => {},
                }
            } else break;
        }
    }

    fn takeSettingsAcks(self: *ClientH2) []u8 {
        return self.settings_ack.toOwnedSlice(self.gpa) catch &.{};
    }

    fn noteWindowUpdate(self: *ClientH2, stream_id: u31, incr: u31) void {
        if (stream_id == 0) self.conn_credit += @intCast(incr) else if (stream_id == self.target_sid) self.stream_credit += @intCast(incr);
    }
};

fn bigBodyHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    handler_entered.store(true, .release);
    const entered = zio.Timestamp.now(.monotonic).toNanoseconds();
    var body: [BODY_70K]u8 = undefined;
    @memset(&body, 'B');
    resp.send(200, &.{}, &body) catch |err| {
        result_ch.push(.{ .code = codeOf(err), .entered_ns = entered, .returned_ns = zio.Timestamp.now(.monotonic).toNanoseconds() });
        return err;
    };
    result_ch.push(.{ .code = 0, .entered_ns = entered, .returned_ns = zio.Timestamp.now(.monotonic).toNanoseconds() });
}

fn capWaitBodyHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    handler_entered.store(true, .release);
    const entered = zio.Timestamp.now(.monotonic).toNanoseconds();
    var body: [BODY_192K]u8 = undefined;
    @memset(&body, 'C');
    resp.send(200, &.{}, &body) catch |err| {
        result_ch.push(.{ .code = codeOf(err), .entered_ns = entered, .returned_ns = zio.Timestamp.now(.monotonic).toNanoseconds() });
        return err;
    };
    result_ch.push(.{ .code = 0, .entered_ns = entered, .returned_ns = zio.Timestamp.now(.monotonic).toNanoseconds() });
}

fn rstBlockHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    handler_entered.store(true, .release);
    const entered = zio.Timestamp.now(.monotonic).toNanoseconds();
    var body: [BODY_70K]u8 = undefined;
    @memset(&body, 'R');
    resp.send(200, &.{}, &body) catch |err| {
        var peer: u32 = 0;
        if (err == error.PeerReset) {
            if (resp.slotTerminal()) |c| switch (c) {
                .peer_reset => |code| peer = @intFromEnum(code),
                else => {},
            };
        }
        result_ch.push(.{ .code = codeOf(err), .peer_code = peer, .entered_ns = entered, .returned_ns = zio.Timestamp.now(.monotonic).toNanoseconds() });
        if (err == error.PeerReset) return;
        return err;
    };
    result_ch.push(.{ .code = 5, .entered_ns = entered, .returned_ns = zio.Timestamp.now(.monotonic).toNanoseconds() });
    return error.ExpectedPeerReset;
}

fn writeFailHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    handler_entered.store(true, .release);
    const entered = zio.Timestamp.now(.monotonic).toNanoseconds();
    var body: [BODY_70K]u8 = undefined;
    @memset(&body, 'W');
    resp.send(200, &.{}, &body) catch |err| {
        result_ch.push(.{ .code = codeOf(err), .entered_ns = entered, .returned_ns = zio.Timestamp.now(.monotonic).toNanoseconds() });
        if (err == error.WriteFailed) return;
        return err;
    };
    result_ch.push(.{ .code = 0, .entered_ns = entered, .returned_ns = zio.Timestamp.now(.monotonic).toNanoseconds() });
    return error.ExpectedWriteFailed;
}

fn slowBlockHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    handler_entered.store(true, .release);
    const entered = zio.Timestamp.now(.monotonic).toNanoseconds();
    var body: [BODY_70K]u8 = undefined;
    @memset(&body, 'S');
    resp.send(200, &.{}, &body) catch |err| {
        result_ch.push(.{ .code = codeOf(err), .entered_ns = entered, .returned_ns = zio.Timestamp.now(.monotonic).toNanoseconds() });
        if (err == error.SlowConsumer) return;
        return err;
    };
    result_ch.push(.{ .code = 0, .entered_ns = entered, .returned_ns = zio.Timestamp.now(.monotonic).toNanoseconds() });
    return error.ExpectedSlowConsumer;
}

fn okProgressHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    try resp.send(200, &.{}, "ok");
}

fn waitAccountingZero(server: *starh2.Server, timeout_ms: u64) !void {
    var waited: u64 = 0;
    while (waited < timeout_ms) : (waited += 10) {
        // Slots release strictly after handlers exit, so both must reach zero:
        // live_handlers alone would pass while the slot is still held.
        if (server.accounting.active_streams.load(.acquire) == 0 and
            server.accounting.outbound_bytes.load(.acquire) == 0 and
            server.accounting.request_bytes.load(.acquire) == 0 and
            starh2.edge.connection.test_observed_live_handlers.load(.acquire) == 0 and
            starh2.edge.connection.test_observed_slots_in_use.load(.acquire) == 0)
        {
            return;
        }
        zio.sleep(.fromMilliseconds(10)) catch {};
    }
    std.debug.print("accounting stuck: active_streams={d} outbound_bytes={d} request_bytes={d} live_handlers={d} slots_in_use={d}\n", .{
        server.accounting.active_streams.load(.acquire),
        server.accounting.outbound_bytes.load(.acquire),
        server.accounting.request_bytes.load(.acquire),
        starh2.edge.connection.test_observed_live_handlers.load(.acquire),
        starh2.edge.connection.test_observed_slots_in_use.load(.acquire),
    });
    return error.AccountingTimeout;
}

fn runExactBigBody(rt: *zio.Runtime, gpa: std.mem.Allocator, stream_id: u31) !void {
    result_ch.reset();
    handler_entered.store(false, .release);
    starh2.edge.connection.test_waiting_for_space.store(0, .release);
    starh2.edge.connection.test_boot_ready.store(false, .release);
    starh2.edge.connection.test_queue_wire_bypass.store(0, .release);

    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/big", .handler = .{ .ptr = @constCast(&dummy), .runFn = bigBodyHandler } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    limits.outbound_bytes_per_stream = 128 * 1024;
    limits.outbound_bytes_per_connection = 512 * 1024;
    limits.graceful_drain_timeout_ns = 500 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

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
    zio.sleep(.fromMilliseconds(40)) catch {};
    const port = server.localAddress(0).getPort();

    const initial_win: u32 = 1024;
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    defer stream.close();

    starh2.edge.wire_pump.test_last_ticket_ok_ns.store(0, .release);
    starh2.edge.wire_pump.test_last_ticket_ok_id.store(0, .release);
    starh2.edge.connection.test_write_delay_ms = 15;
    defer starh2.edge.connection.test_write_delay_ms = 0;

    const open = try h2c.buildClientHelloWindow(gpa, "/big", stream_id, initial_win, .empty);
    defer gpa.free(open);
    try writeAllStream(stream, open);

    // Wait until handler entered and still blocked (no result yet).
    var waited: u64 = 0;
    while (waited < 2000) : (waited += 10) {
        if (handler_entered.load(.acquire) and result_ch.tryPop() == null) break;
        zio.sleep(.fromMilliseconds(10)) catch {};
    }
    try std.testing.expect(handler_entered.load(.acquire));
    // Still blocked before WINDOW_UPDATE — no successful return.
    if (result_ch.tryPop()) |early| {
        std.debug.print("early result code={d}\n", .{early.code});
        return error.TestUnexpectedResult;
    }

    var client = try ClientH2.init(gpa, stream_id, @intCast(initial_win));
    defer client.deinit();
    client.conn_credit = 1 << 30;

    // Drain any early SETTINGS/HEADERS/DATA under initial credit.
    var rbuf: [32 * 1024]u8 = undefined;
    {
        const n = try readAvailable(stream, rbuf[0..]);
        if (n > 0) client.ingest(rbuf[0..n]) catch |err| {
            return err;
        };
        const acks = client.takeSettingsAcks();
        defer if (acks.len != 0) gpa.free(acks);
        if (acks.len != 0) try writeAllStream(stream, acks);
    }

    const need: u32 = BODY_70K - initial_win;
    const step: u31 = 2048;
    var sent: u32 = 0;
    while (sent < need) {
        const incr: u31 = @intCast(@min(step, need - sent));
        var boost: std.ArrayList(u8) = .empty;
        defer boost.deinit(gpa);
        try h2c.appendWindowUpdate(gpa, &boost, stream_id, incr);
        try h2c.appendWindowUpdate(gpa, &boost, 0, incr);
        client.noteWindowUpdate(stream_id, incr);
        client.noteWindowUpdate(0, incr);
        try writeAllStream(stream, boost.items);
        sent += incr;

        const n = try readAvailable(stream, rbuf[0..]);
        if (n > 0) {
            client.ingest(rbuf[0..n]) catch |err| {
                return err;
            };
            const acks = client.takeSettingsAcks();
            defer if (acks.len != 0) gpa.free(acks);
            if (acks.len != 0) try writeAllStream(stream, acks);
        }
        // Delayed writer: handler must not return before final ticket completion.
        if (result_ch.w.load(.acquire) > result_ch.r.load(.acquire) and
            starh2.edge.wire_pump.test_last_ticket_ok_ns.load(.acquire) == 0)
        {
            return error.ResultBeforeTicketAck;
        }
    }

    // Finish draining until END_STREAM and handler returns.
    var rounds: usize = 0;
    while (rounds < 200) : (rounds += 1) {
        const n = try readAvailable(stream, rbuf[0..]);
        if (n > 0) client.ingest(rbuf[0..n]) catch |err| {
            return err;
        };
        if (client.end_stream_count >= 1 and result_ch.w.load(.acquire) > result_ch.r.load(.acquire)) break;
        zio.sleep(.fromMilliseconds(10)) catch {};
    }

    try std.testing.expect(!client.saw_rst);
    try std.testing.expect(!client.saw_goaway);
    try std.testing.expectEqual(@as(usize, 1), client.end_stream_count);
    try std.testing.expectEqual(@as(usize, BODY_70K), client.body.items.len);
    for (client.body.items) |b| try std.testing.expectEqual(@as(u8, 'B'), b);

    const evt = try result_ch.waitOne(2000);
    try std.testing.expectEqual(@as(u8, 0), evt.code);
    const ack_ns = starh2.edge.wire_pump.test_last_ticket_ok_ns.load(.acquire);
    try std.testing.expect(ack_ns != 0);
    try std.testing.expect(starh2.edge.wire_pump.test_last_ticket_ok_id.load(.acquire) != 0);
    // Handler return is after WritePump recorded successful final-ticket completion.
    try std.testing.expect(evt.returned_ns >= ack_ns);
    try std.testing.expect(evt.entered_ns > 0);
    try std.testing.expect(evt.entered_ns <= ack_ns);
    try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_queue_wire_bypass.load(.acquire));
    try waitAccountingZero(&server, 5_000);
}

fn runExactCapWait(rt: *zio.Runtime, gpa: std.mem.Allocator, stream_id: u31) !void {
    result_ch.reset();
    handler_entered.store(false, .release);
    starh2.edge.connection.test_waiting_for_space.store(0, .release);
    starh2.edge.connection.test_queue_wire_bypass.store(0, .release);

    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/cap", .handler = .{ .ptr = @constCast(&dummy), .runFn = capWaitBodyHandler } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    // Body 192KiB > per-stream cap 64KiB → waitForStreamSpace must block; actor reuses slab.
    limits.outbound_bytes_per_stream = 64 * 1024;
    limits.outbound_bytes_per_connection = 512 * 1024;
    limits.graceful_drain_timeout_ns = 500 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

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
    zio.sleep(.fromMilliseconds(40)) catch {};
    const port = server.localAddress(0).getPort();

    // Large peer window so local per-stream cap (64KiB) is the binding constraint.
    const initial_win: u32 = 1024 * 1024;
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    defer stream.close();
    starh2.edge.connection.test_write_delay_ms = 5;
    defer starh2.edge.connection.test_write_delay_ms = 0;
    const open = try h2c.buildClientHelloWindow(gpa, "/cap", stream_id, initial_win, .empty);
    defer gpa.free(open);
    try writeAllStream(stream, open);
    // SETTINGS enlarges only the per-stream send window. Raise the connection
    // window as well so the 192KiB response can drain after its first 65,535B.
    var conn_boost: std.ArrayList(u8) = .empty;
    defer conn_boost.deinit(gpa);
    try h2c.appendWindowUpdate(gpa, &conn_boost, 0, BODY_192K);
    try writeAllStream(stream, conn_boost.items);

    // Wait until sparse capacity waiter is observed for this stream.
    var waited: u64 = 0;
    while (waited < 3000) : (waited += 10) {
        if (starh2.edge.connection.test_waiting_for_space.load(.acquire) == stream_id) break;
        zio.sleep(.fromMilliseconds(10)) catch {};
    }
    try std.testing.expectEqual(@as(u32, stream_id), starh2.edge.connection.test_waiting_for_space.load(.acquire));

    var client = try ClientH2.init(gpa, stream_id, @intCast(initial_win));
    defer client.deinit();
    client.conn_credit = 1 << 30;
    var rbuf: [32 * 1024]u8 = undefined;
    var rounds: usize = 0;
    while (rounds < 400) : (rounds += 1) {
        const n = try readAvailable(stream, rbuf[0..]);
        if (n > 0) {
            client.ingest(rbuf[0..n]) catch |err| return err;
            const acks = client.takeSettingsAcks();
            defer if (acks.len != 0) gpa.free(acks);
            if (acks.len != 0) try writeAllStream(stream, acks);
        }
        if (client.end_stream_count >= 1 and result_ch.w.load(.acquire) > result_ch.r.load(.acquire)) break;
        zio.sleep(.fromMilliseconds(10)) catch {};
    }

    try std.testing.expect(!client.saw_rst);
    try std.testing.expect(!client.saw_goaway);
    try std.testing.expectEqual(@as(usize, 1), client.end_stream_count);
    try std.testing.expectEqual(@as(usize, BODY_192K), client.body.items.len);
    for (client.body.items) |b| try std.testing.expectEqual(@as(u8, 'C'), b);
    const evt = try result_ch.waitOne(2000);
    try std.testing.expectEqual(@as(u8, 0), evt.code);
    try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_queue_wire_bypass.load(.acquire));
    try waitAccountingZero(&server, 5_000);
}

fn runExactRstWhileCapWait(rt: *zio.Runtime, gpa: std.mem.Allocator, stream_id: u31) !void {
    result_ch.reset();
    handler_entered.store(false, .release);
    starh2.edge.connection.test_waiting_for_space.store(0, .release);

    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/cap", .handler = .{ .ptr = @constCast(&dummy), .runFn = rstBlockHandler } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    limits.outbound_bytes_per_stream = 64 * 1024;
    limits.outbound_bytes_per_connection = 512 * 1024;
    limits.graceful_drain_timeout_ns = 500 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

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
    zio.sleep(.fromMilliseconds(40)) catch {};
    const port = server.localAddress(0).getPort();

    const initial_win: u32 = 1024 * 1024;
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    defer stream.close();
    starh2.edge.connection.test_write_delay_ms = 50;
    defer starh2.edge.connection.test_write_delay_ms = 0;
    const open = try h2c.buildClientHelloWindow(gpa, "/cap", stream_id, initial_win, .empty);
    defer gpa.free(open);
    try writeAllStream(stream, open);

    var waited: u64 = 0;
    while (waited < 3000) : (waited += 10) {
        if (starh2.edge.connection.test_waiting_for_space.load(.acquire) == stream_id) break;
        zio.sleep(.fromMilliseconds(10)) catch {};
    }
    try std.testing.expectEqual(@as(u32, stream_id), starh2.edge.connection.test_waiting_for_space.load(.acquire));

    var rst: std.ArrayList(u8) = .empty;
    defer rst.deinit(gpa);
    try h2c.appendRst(gpa, &rst, stream_id);
    try writeAllStream(stream, rst.items);

    const evt = try result_ch.waitOne(3000);
    try std.testing.expectEqual(@as(u8, 3), evt.code); // PeerReset while capacity-waiting
    const frame = starh2.core.frame;
    try std.testing.expectEqual(@as(u32, @intFromEnum(frame.ErrorCode.cancel)), evt.peer_code);
    try waitAccountingZero(&server, 5_000);
}

fn runExactRst(rt: *zio.Runtime, gpa: std.mem.Allocator, stream_id: u31) !void {
    result_ch.reset();
    handler_entered.store(false, .release);

    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/rstblock", .handler = .{ .ptr = @constCast(&dummy), .runFn = rstBlockHandler } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    limits.outbound_bytes_per_stream = 128 * 1024;
    limits.graceful_drain_timeout_ns = 500 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

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
    zio.sleep(.fromMilliseconds(40)) catch {};
    const port = server.localAddress(0).getPort();

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    // Keep socket open until result received (defer close after wait).
    const open = try h2c.buildClientHelloWindow(gpa, "/rstblock", stream_id, 256, .empty);
    defer gpa.free(open);
    try writeAllStream(stream, open);

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 10) {
        if (handler_entered.load(.acquire) and starh2.edge.connection.test_observed_live_handlers.load(.acquire) > 0) break;
        zio.sleep(.fromMilliseconds(10)) catch {};
    }
    try std.testing.expect(handler_entered.load(.acquire));

    var rst: std.ArrayList(u8) = .empty;
    defer rst.deinit(gpa);
    try h2c.appendRst(gpa, &rst, stream_id);
    try writeAllStream(stream, rst.items);

    const evt = try result_ch.waitOne(3000);
    stream.close();
    try std.testing.expectEqual(@as(u8, 3), evt.code); // PeerReset only
    const frame = starh2.core.frame;
    try std.testing.expectEqual(@as(u32, @intFromEnum(frame.ErrorCode.cancel)), evt.peer_code);
    try waitAccountingZero(&server, 5_000);
}

fn runExactWriteFail(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/wf", .handler = .{ .ptr = @constCast(&dummy), .runFn = writeFailHandler } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 8;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    limits.graceful_drain_timeout_ns = 200 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

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
    zio.sleep(.fromMilliseconds(40)) catch {};
    const port = server.localAddress(0).getPort();

    var round: usize = 0;
    while (round < 100) : (round += 1) {
        result_ch.reset();
        handler_entered.store(false, .release);
        starh2.edge.connection.test_wire_sends.store(0, .release);
        starh2.edge.connection.test_force_wire_fail_after.store(0, .release);
        starh2.edge.connection.test_write_fail_after = 0;
        starh2.edge.connection.test_observed_writer_fail_handled.store(false, .release);
        starh2.edge.connection.test_queue_wire_bypass.store(0, .release);

        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        // Tiny stream window so handler blocks inside send after HEADERS/DATA start.
        const open = try h2c.buildClientHelloWindow(gpa, "/wf", 1, 256, .empty);
        defer gpa.free(open);
        try writeAllStream(stream, open);

        var waited: u64 = 0;
        while (waited < 2000) : (waited += 5) {
            if (handler_entered.load(.acquire)) break;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        try std.testing.expect(handler_entered.load(.acquire));
        // Fail the next transport write while the handler is mid-send.
        starh2.edge.wire_pump.test_fail_next_write.store(true, .release);
        var boost: std.ArrayList(u8) = .empty;
        defer boost.deinit(gpa);
        try h2c.appendWindowUpdate(gpa, &boost, 1, 64 * 1024);
        try h2c.appendWindowUpdate(gpa, &boost, 0, 64 * 1024);
        writeAllStream(stream, boost.items) catch {};

        const evt = result_ch.waitOne(2000) catch |err| {
            return err;
        };
        try std.testing.expectEqual(@as(u8, 1), evt.code); // WriteFailed every round — never 0/2
        try waitAccountingZero(&server, 2_000);
        try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_queue_wire_bypass.load(.acquire));
    }
}

fn runExactSlowConsumer(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    result_ch.reset();
    handler_entered.store(false, .release);

    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/slow", .handler = .{ .ptr = @constCast(&dummy), .runFn = slowBlockHandler } },
        .{ .method = .GET, .path = "/ok", .handler = .{ .ptr = @constCast(&dummy), .runFn = okProgressHandler } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    limits.outbound_bytes_per_stream = 128 * 1024;
    limits.slow_consumer_timeout_ns = 80 * std.time.ns_per_ms;
    limits.graceful_drain_timeout_ns = 500 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

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
    zio.sleep(.fromMilliseconds(40)) catch {};
    const port = server.localAddress(0).getPort();

    // Stream 1: blocked with tiny window, no WINDOW_UPDATE → SlowConsumer.
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const open = try h2c.buildClientHelloWindow(gpa, "/slow", 1, 256, .empty);
        defer gpa.free(open);
        try writeAllStream(stream, open);
        const evt = try result_ch.waitOne(3000);
        try std.testing.expectEqual(@as(u8, 4), evt.code);
    }
    try waitAccountingZero(&server, 5_000);
    try std.testing.expectEqual(@as(usize, 0), server.accounting.outbound_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.active_streams.load(.acquire));

    // Unaffected stream progresses on a fresh connection.
    {
        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
        var stream = try peer.connect(.{});
        defer stream.close();
        const open = try h2c.buildClientHelloWindow(gpa, "/ok", 1, 64 * 1024, .empty);
        defer gpa.free(open);
        try writeAllStream(stream, open);
        var buf: [4096]u8 = undefined;
        var got = false;
        var i: usize = 0;
        while (i < 50) : (i += 1) {
            const n = try readAvailable(stream, buf[0..]);
            if (n > 0) {
                got = true;
                break;
            }
            zio.sleep(.fromMilliseconds(20)) catch {};
        }
        try std.testing.expect(got);
    }
    try waitAccountingZero(&server, 5_000);
}

fn runExactShutdownBlocked(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    result_ch.reset();
    handler_entered.store(false, .release);

    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/big", .handler = .{ .ptr = @constCast(&dummy), .runFn = bigBodyHandler } },
    };
    var limits = starh2.Limits.defaults;
    limits.max_connections = 4;
    limits.max_streams_per_connection = 8;
    limits.max_streams_per_server = 16;
    limits.cancellation_reaper_jobs = 16;
    limits.cancellation_reaper_tasks = 2;
    limits.outbound_bytes_per_stream = 128 * 1024;
    limits.graceful_drain_timeout_ns = 100 * std.time.ns_per_ms;
    limits.preface_timeout_ns = 500 * std.time.ns_per_ms;

    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    zio.sleep(.fromMilliseconds(40)) catch {};
    const port = server.localAddress(0).getPort();

    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
    var stream = try peer.connect(.{});
    defer stream.close();
    const open = try h2c.buildClientHelloWindow(gpa, "/big", 1, 256, .empty);
    defer gpa.free(open);
    try writeAllStream(stream, open);

    var waited: u64 = 0;
    while (waited < 2000) : (waited += 10) {
        if (handler_entered.load(.acquire)) break;
        zio.sleep(.fromMilliseconds(10)) catch {};
    }
    try std.testing.expect(handler_entered.load(.acquire));
    server.requestShutdown();
    const evt = try result_ch.waitOne(3000);
    try std.testing.expectEqual(@as(u8, 2), evt.code); // ConnectionClosed
    serve_handle.join() catch {};
    try std.testing.expectEqual(@as(usize, 0), server.active_connections.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.active_streams.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.reaper_reserved.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.outbound_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.request_bytes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.accounting.active_handshakes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), starh2.edge.connection.test_observed_live_handlers.load(.acquire));
}

fn spawnRt(gpa: std.mem.Allocator) !*zio.Runtime {
    return try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024 , .shrink_interval = .fromSeconds(5), .slab_slots = 32, .prewarm = 32 },
        .executors = .exact(2),
        .enable_task_migration = true,
    });
}

test "exact: >64KiB body byte-identical + END_STREAM (stream 1)" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer if (dbg.deinit() != .ok) @panic("leak after exact big body");
    const gpa = dbg.allocator();
    const rt = try spawnRt(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runExactBigBody, .{ rt, gpa, @as(u31, 1) });
    try h.join();
}

test "exact: capacity wait then full 192KiB resume (sparse 1000001)" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer if (dbg.deinit() != .ok) @panic("leak after exact cap wait");
    const gpa = dbg.allocator();
    const rt = try spawnRt(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runExactCapWait, .{ rt, gpa, @as(u31, 1_000_001) });
    try h.join();
}

test "exact: PeerReset while capacity-waiting (sparse)" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer if (dbg.deinit() != .ok) @panic("leak after exact rst-while-cap");
    const gpa = dbg.allocator();
    const rt = try spawnRt(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runExactRstWhileCapWait, .{ rt, gpa, @as(u31, 1_000_001) });
    try h.join();
}

test "exact: 100x WriteFailed taxonomy" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer if (dbg.deinit() != .ok) @panic("leak after exact write-fail");
    const gpa = dbg.allocator();
    const rt = try spawnRt(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runExactWriteFail, .{ rt, gpa });
    try h.join();
}

test "exact: SlowConsumer + unaffected progress" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer if (dbg.deinit() != .ok) @panic("leak after exact slow");
    const gpa = dbg.allocator();
    const rt = try spawnRt(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runExactSlowConsumer, .{ rt, gpa });
    try h.join();
}

test "exact: shutdown while blocked → ConnectionClosed" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer if (dbg.deinit() != .ok) @panic("leak after exact shutdown");
    const gpa = dbg.allocator();
    const rt = try spawnRt(gpa);
    defer rt.deinit();
    var h = try rt.spawn(runExactShutdownBlocked, .{ rt, gpa });
    try h.join();
}
