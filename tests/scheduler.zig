//! Real fair-scheduler gates — production FairScheduler emit-order accounting.
const std = @import("std");
const starh2 = @import("starh2");

fn testPool(gpa: std.mem.Allocator, slab_bytes: usize, capacity: usize) !starh2.edge.slab_pool.SlabPool {
    return starh2.edge.slab_pool.SlabPool.init(gpa, std.testing.io, slab_bytes, capacity);
}

const SinkState = struct {
    gpa: std.mem.Allocator,
    emitted: std.ArrayList(struct { control: bool, n: usize }) = .empty,

    fn sink(
        ctx: *anyopaque,
        payload: []u8,
        flush: bool,
        ticket: u64,
        ticket_slot: u32,
        control_n: usize,
        control_entry: bool,
    ) anyerror!void {
        _ = flush;
        _ = ticket;
        _ = ticket_slot;
        _ = control_n;
        const self: *SinkState = @ptrCast(@alignCast(ctx));
        try self.emitted.append(self.gpa, .{ .control = control_entry, .n = payload.len });
        self.gpa.free(payload);
    }
};

fn selfPtr(p: anytype) *anyopaque {
    return @ptrCast(p);
}

test "prefill max ordinary queue: emit gap <= 64 when DATA eligible" {
    const gpa = std.testing.allocator;
    const fs = starh2.edge.fair_scheduler;
    const cp = starh2.edge.control_pool;
    // Ordinary capacity = 256 - 16 terminal reserve = 240
    var pool = try testPool(gpa, 1024 * 1024, 16);
    defer pool.deinit(gpa);
    var sched = try fs.FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 16, 256, 8, &pool);
    defer sched.deinit();

    // Eligible DATA before controls (prefilled into boot slab).
    var big: [16 * 1024]u8 = undefined;
    @memset(&big, 'd');
    var i: usize = 0;
    while (i < 64) : (i += 1) {
        try sched.enqueueDataBytes(1, &big, false, 0, 0);
    }

    const caps = sched.ctrl.ordinaryCaps();
    i = 0;
    while (i < caps.entries) : (i += 1) {
        const p = try gpa.alloc(u8, 8);
        @memset(p, @truncate(i));
        try sched.enqueueControl(p, .ordinary, 0, 0);
    }
    // Prefill must NOT have incremented emitted-since-DATA.
    try std.testing.expectEqual(@as(usize, 0), sched.emitted_controls_since_data);

    var sink_state: SinkState = .{ .gpa = gpa };
    defer sink_state.emitted.deinit(gpa);
    const win = struct {
        fn stream(_: *anyopaque, _: u31) i32 {
            return 1 << 30;
        }
        fn conn(_: *anyopaque) i32 {
            return 1 << 30;
        }
        fn build(ctx: *anyopaque, _: u31, bytes: []const u8, end: bool) anyerror![]u8 {
            _ = end;
            const self: *SinkState = @ptrCast(@alignCast(ctx));
            return try self.gpa.dupe(u8, bytes);
        }
    };

    try sched.drain(selfPtr(&sink_state), SinkState.sink, selfPtr(&sink_state), win.stream, win.conn, win.build, selfPtr(&sink_state));
    for (sink_state.emitted.items) |e| {
        if (e.control) sched.ctrl.release(e.n, true);
    }

    try std.testing.expect(sched.maxEligibleGapInKinds() <= cp.CONTROL_BEFORE_DATA);
    try std.testing.expect(sched.maxControlGap() <= cp.CONTROL_BEFORE_DATA);
    try std.testing.expect(sched.data_quanta >= caps.entries / cp.CONTROL_BEFORE_DATA);
    // Every emit must be sink-originated (no bypass counter equivalent at unit level).
    try std.testing.expectEqual(sched.emits_total, sched.sink_origin_emits);
}

test "fair scheduler: 10k controls interleaved drain gaps <= 64" {
    const gpa = std.testing.allocator;
    const fs = starh2.edge.fair_scheduler;
    // Boot slab holds enough to keep DATA eligible across 10k control drains.
    var pool = try testPool(gpa, 8 * 1024 * 1024, 16);
    defer pool.deinit(gpa);
    var sched = try fs.FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 16, 256, 8, &pool);
    defer sched.deinit();

    var big: [16 * 1024]u8 = undefined;
    @memset(&big, 'y');
    var i: usize = 0;
    while (i < 512) : (i += 1) {
        try sched.enqueueDataBytes(1, &big, false, 0, 0);
    }

    var sink_state: SinkState = .{ .gpa = gpa };
    defer sink_state.emitted.deinit(gpa);
    const win = struct {
        fn stream(_: *anyopaque, _: u31) i32 {
            return 1 << 30;
        }
        fn conn(_: *anyopaque) i32 {
            return 1 << 30;
        }
        fn build(ctx: *anyopaque, _: u31, bytes: []const u8, end: bool) anyerror![]u8 {
            _ = end;
            const self: *SinkState = @ptrCast(@alignCast(ctx));
            return try self.gpa.dupe(u8, bytes);
        }
    };

    i = 0;
    while (i < 10_000) : (i += 1) {
        while (true) {
            const p = try gpa.alloc(u8, 8);
            @memset(p, @truncate(i));
            sched.enqueueControl(p, .ordinary, 0, 0) catch {
                // enqueueControl frees p on failure — drain and retry with a new buffer.
                try sched.drain(selfPtr(&sink_state), SinkState.sink, selfPtr(&sink_state), win.stream, win.conn, win.build, selfPtr(&sink_state));
                for (sink_state.emitted.items) |e| {
                    if (e.control) sched.ctrl.release(e.n, true);
                }
                sink_state.emitted.clearRetainingCapacity();
                continue;
            };
            break;
        }
        if (sched.shouldForceDataNow()) {
            try sched.drain(selfPtr(&sink_state), SinkState.sink, selfPtr(&sink_state), win.stream, win.conn, win.build, selfPtr(&sink_state));
            for (sink_state.emitted.items) |e| {
                if (e.control) sched.ctrl.release(e.n, true);
            }
            sink_state.emitted.clearRetainingCapacity();
        }
    }
    try sched.drain(selfPtr(&sink_state), SinkState.sink, selfPtr(&sink_state), win.stream, win.conn, win.build, selfPtr(&sink_state));
    for (sink_state.emitted.items) |e| {
        if (e.control) sched.ctrl.release(e.n, true);
    }

    const cp = starh2.edge.control_pool;
    try std.testing.expect(sched.maxEligibleGapInKinds() <= cp.CONTROL_BEFORE_DATA);
    try std.testing.expectEqual(@as(usize, 0), sched.ctrl.entries_held.load(.acquire));
}

test "fair scheduler: terminal when ordinary full; zero windows" {
    const gpa = std.testing.allocator;
    const fs = starh2.edge.fair_scheduler;
    var pool = try testPool(gpa, 64 * 1024, 16);
    defer pool.deinit(gpa);
    var sched = try fs.FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 16, 256, 8, &pool);
    defer sched.deinit();
    const caps = sched.ctrl.ordinaryCaps();
    var i: usize = 0;
    while (i < caps.entries) : (i += 1) {
        const p = try gpa.alloc(u8, 1);
        p[0] = 1;
        try sched.enqueueControl(p, .ordinary, 0, 0);
    }
    {
        const p = try gpa.alloc(u8, 1);
        p[0] = 2;
        try std.testing.expectError(error.PoolFull, sched.enqueueControl(p, .ordinary, 0, 0));
    }
    {
        const p = try gpa.alloc(u8, 64);
        @memset(p, 3);
        try sched.enqueueControl(p, .terminal, 0, 0);
    }

    var pool2 = try testPool(gpa, 10_000, 16);
    defer pool2.deinit(gpa);
    var s2 = try fs.FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 16, 256, 8, &pool2);
    defer s2.deinit();
    try s2.enqueueDataBytes(1, &[_]u8{1} ** 100, false, 0, 0);
    try s2.enqueueDataBytes(3, &[_]u8{3} ** 100, true, 0, 0);
    var sink_state: SinkState = .{ .gpa = gpa };
    defer sink_state.emitted.deinit(gpa);
    const win = struct {
        fn stream(_: *anyopaque, sid: u31) i32 {
            return if (sid == 1) 0 else 1 << 20;
        }
        fn conn(_: *anyopaque) i32 {
            return 1 << 20;
        }
        fn build(ctx: *anyopaque, sid: u31, bytes: []const u8, end: bool) anyerror![]u8 {
            _ = sid;
            _ = end;
            const self: *SinkState = @ptrCast(@alignCast(ctx));
            return try self.gpa.dupe(u8, bytes);
        }
    };
    const progressed = try s2.emitOneDataPublic(selfPtr(&sink_state), SinkState.sink, selfPtr(&sink_state), win.stream, win.conn, win.build, selfPtr(&sink_state));
    try std.testing.expect(progressed);

    var pool3 = try testPool(gpa, 10_000, 16);
    defer pool3.deinit(gpa);
    var s3 = try fs.FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 16, 256, 4, &pool3);
    defer s3.deinit();
    try s3.enqueueDataBytes(1, &[_]u8{9} ** 100, true, 0, 0);
    const p = try gpa.alloc(u8, 16);
    @memset(p, 8);
    try s3.enqueueControl(p, .ordinary, 0, 0);
    const win0 = struct {
        fn stream(_: *anyopaque, _: u31) i32 {
            return 0;
        }
        fn conn(_: *anyopaque) i32 {
            return 0;
        }
        fn build(_: *anyopaque, _: u31, _: []const u8, _: bool) anyerror![]u8 {
            return error.WriteFailed;
        }
    };
    var sink3: SinkState = .{ .gpa = gpa };
    defer sink3.emitted.deinit(gpa);
    try s3.drain(selfPtr(&sink3), SinkState.sink, selfPtr(&sink3), win0.stream, win0.conn, win0.build, selfPtr(&sink3));
    try std.testing.expect(s3.ctrl.entries_held.load(.acquire) >= 1 or sink3.emitted.items.len >= 1);
}

test "connection ControlPool constants match FairScheduler" {
    const cp = starh2.edge.control_pool;
    try std.testing.expectEqual(@as(usize, 64), cp.CONTROL_BEFORE_DATA);
    try std.testing.expectEqual(@sizeOf(starh2.edge.connection.HandlerSlot), starh2.core.limits.HANDLER_SLOT_SIZE);
}

test "zero stream window does not block other stream or control (session)" {
    const gpa = std.testing.allocator;
    const Session = starh2.Session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    var session = try Session.init(gpa, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
        // intent_drain scratch — do not free slice
    }

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    {
        var sbuf: [64]u8 = undefined;
        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 64 }};
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
        try wire.appendSlice(gpa, sbuf[0..sn]);
    }
    for ([_]u31{ 1, 3 }) |sid| {
        const fields = [_]hpack.HeaderField{
            .{ .name = ":method", .value = "GET" },
            .{ .name = ":scheme", .value = "http" },
            .{ .name = ":path", .value = "/x" },
            .{ .name = ":authority", .value = "localhost" },
        };
        const block = try hpack.Encoder.encode(gpa, &fields);
        defer gpa.free(block);
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = true },
            .stream_id = sid,
        };
        fh.encode(&hdr_buf);
        try wire.appendSlice(gpa, &hdr_buf);
        try wire.appendSlice(gpa, block);
    }
    try session.ingest(wire.items);
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            .dispatch_request => |d| {
                starh2.core.hpack.HeaderField.freeOwnedSlice(gpa, d.headers);
                gpa.free(d.headers);
                if (d.trailers.len != 0) {
                    starh2.core.hpack.HeaderField.freeOwnedSlice(gpa, d.trailers);
                    gpa.free(d.trailers);
                }
                if (d.body.len != 0) gpa.free(d.body);
            },
            else => {},
        };
    }
    try session.applyCommand(.{ .respond_headers = .{ .stream_id = 1, .status = 200, .headers = &.{}, .end_stream = false } });
    try session.applyCommand(.{ .respond_headers = .{ .stream_id = 3, .status = 200, .headers = &.{}, .end_stream = false } });
    {
        const more = session.drainIntents();
        for (more) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }
    const chunk = [_]u8{'a'} ** 64;
    try session.applyCommand(.{ .respond_data = .{ .stream_id = 1, .data = &chunk, .end_stream = false } });
    {
        const out = session.drainIntents();
        for (out) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
        // intent_drain scratch
    }
    try std.testing.expectError(error.FlowBlocked, session.applyCommand(.{ .respond_data = .{
        .stream_id = 1,
        .data = "x",
        .end_stream = false,
    } }));
    try session.applyCommand(.{ .respond_data = .{ .stream_id = 3, .data = "ok", .end_stream = true } });
    {
        var pbuf: [17]u8 = undefined;
        const opaque_data = [_]u8{0} ** 8;
        const pn = try frame.Serializer.ping(&pbuf, false, &opaque_data);
        try session.ingest(pbuf[0..pn]);
    }
    const out = session.drainIntents();
    defer {
        for (out) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
        // intent_drain scratch
    }
    var saw_data3 = false;
    var saw_ping_ack = false;
    for (out) |it| switch (it) {
        .outbound_frame => |f| {
            if (f.typ == .data and f.stream_id == 3) saw_data3 = true;
            if (f.typ == .ping and f.flags.end_stream) saw_ping_ack = true;
        },
        else => {},
    };
    try std.testing.expect(saw_data3);
    try std.testing.expect(saw_ping_ack);
}

test "drain emits pending DATA after Session dropped the stream" {
    // TLS stall snapshot: FairScheduler still holds the body, Session already
    // dropped the map entry, connection window is huge. streamSendAvailable
    // used to return 0 for a missing stream, so drain skipped forever and the
    // actor parked until slow-consumer. Revert that and this test fails.
    const gpa = std.testing.allocator;
    const Session = starh2.Session;
    const flow = starh2.core.flow;
    const fs = starh2.edge.fair_scheduler;
    var session = try Session.init(gpa, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }

    try session.streams.put(1, .{
        .id = 1,
        .state = .half_closed_remote,
        .window = .{ .send = flow.INITIAL_WINDOW },
    });
    session.active_streams = 1;
    const closer = try session.makeDataFrame(1, &.{}, true);
    defer gpa.free(closer);
    try std.testing.expect(session.streams.get(1) == null);
    try std.testing.expect(session.streamSendAvailable(1) > 0);

    var pool = try testPool(gpa, 1024, 4);
    defer pool.deinit(gpa);
    var sched = try fs.FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 2, &pool);
    defer sched.deinit();
    const body = "Hello, World!";
    try sched.enqueueDataBytes(1, body, true, 0, 0);

    const DrainCtx = struct {
        gpa: std.mem.Allocator,
        session: *Session,
        data_frames: usize = 0,
        fn sink(
            ctx: *anyopaque,
            payload: []u8,
            flush: bool,
            ticket: u64,
            ticket_slot: u32,
            control_n: usize,
            control_entry: bool,
        ) anyerror!void {
            _ = flush;
            _ = ticket;
            _ = ticket_slot;
            _ = control_n;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!control_entry) self.data_frames += 1;
            self.gpa.free(payload);
        }
        fn streamWin(ctx: *anyopaque, stream_id: u31) i32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.session.streamSendAvailable(stream_id);
        }
        fn connWin(ctx: *anyopaque) i32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.session.connectionSendAvailable();
        }
        fn build(ctx: *anyopaque, stream_id: u31, bytes: []const u8, end: bool) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.session.makeDataFrame(stream_id, bytes, end);
        }
    };
    var ctx: DrainCtx = .{ .gpa = gpa, .session = &session };
    try sched.drain(
        @ptrCast(&ctx),
        DrainCtx.sink,
        @ptrCast(&ctx),
        DrainCtx.streamWin,
        DrainCtx.connWin,
        DrainCtx.build,
        @ptrCast(&ctx),
    );
    try std.testing.expectEqual(@as(usize, 1), ctx.data_frames);
    try std.testing.expectEqual(@as(usize, 0), sched.pendingCount());
}

test "drain does not emit pending DATA after a RST tombstone" {
    // The stall snapshot: pending body, stream map gone, RST tombstone.
    // streamSendAvailable is 0, so drain must skip. Progress is cancelHandler
    // dropping the pending (or fail-closed), not framing DATA after RST.
    const gpa = std.testing.allocator;
    const Session = starh2.Session;
    const flow = starh2.core.flow;
    const fs = starh2.edge.fair_scheduler;
    var session = try Session.init(gpa, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }
    try session.streams.put(1, .{
        .id = 1,
        .state = .half_closed_remote,
        .window = .{ .send = flow.INITIAL_WINDOW },
    });
    session.active_streams = 1;
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 1, .code = .internal_error } });
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }
    try std.testing.expect(session.streams.get(1) == null);
    try std.testing.expectEqual(@as(i32, 0), session.streamSendAvailable(1));

    var pool = try testPool(gpa, 1024, 4);
    defer pool.deinit(gpa);
    var sched = try fs.FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 2, &pool);
    defer sched.deinit();
    try sched.enqueueDataBytes(1, "Hello, World!", true, 0, 0);

    const DrainCtx = struct {
        gpa: std.mem.Allocator,
        session: *Session,
        data_frames: usize = 0,
        fn sink(
            ctx: *anyopaque,
            payload: []u8,
            flush: bool,
            ticket: u64,
            ticket_slot: u32,
            control_n: usize,
            control_entry: bool,
        ) anyerror!void {
            _ = flush;
            _ = ticket;
            _ = ticket_slot;
            _ = control_n;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!control_entry) self.data_frames += 1;
            self.gpa.free(payload);
        }
        fn streamWin(ctx: *anyopaque, stream_id: u31) i32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.session.streamSendAvailable(stream_id);
        }
        fn connWin(ctx: *anyopaque) i32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.session.connectionSendAvailable();
        }
        fn build(ctx: *anyopaque, stream_id: u31, bytes: []const u8, end: bool) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.session.makeDataFrame(stream_id, bytes, end);
        }
    };
    var ctx: DrainCtx = .{ .gpa = gpa, .session = &session };
    try sched.drain(
        @ptrCast(&ctx),
        DrainCtx.sink,
        @ptrCast(&ctx),
        DrainCtx.streamWin,
        DrainCtx.connWin,
        DrainCtx.build,
        @ptrCast(&ctx),
    );
    try std.testing.expectEqual(@as(usize, 0), ctx.data_frames);
    try std.testing.expectEqual(@as(usize, 1), sched.pendingCount());
}
