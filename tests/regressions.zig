const std = @import("std");
const starh2 = @import("starh2");

fn freeIntents(intents: []starh2.core.session.Intent) void {
    for (intents) |*it| {
        switch (it.*) {
            .outbound_frame => |f| std.testing.allocator.free(f.payload),
            .dispatch_request => |d| {
                for (d.headers) |h| {
                    std.testing.allocator.free(@constCast(h.name));
                    std.testing.allocator.free(@constCast(h.value));
                }
                std.testing.allocator.free(d.headers);
                for (d.trailers) |h| {
                    std.testing.allocator.free(@constCast(h.name));
                    std.testing.allocator.free(@constCast(h.value));
                }
                if (d.trailers.len != 0) std.testing.allocator.free(d.trailers);
                if (d.body.len != 0) std.testing.allocator.free(d.body);
            },
            else => {},
        }
    }
    // drainIntents returns boot scratch — do not free the slice
}

fn prefaceSettings(wire: *std.ArrayList(u8)) !void {
    const frame = starh2.core.frame;
    try wire.appendSlice(std.testing.allocator, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(std.testing.allocator, sbuf[0..sn]);
}

test "regression: idle DATA is connection PROTOCOL_ERROR" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try prefaceSettings(&wire);
    // DATA on stream 1 while idle
    var dbuf: [frame.FRAME_HEADER_LEN + 1]u8 = undefined;
    const fh = frame.FrameHeader{ .length = 1, .type = .data, .flags = .{}, .stream_id = 1 };
    fh.encode(dbuf[0..frame.FRAME_HEADER_LEN]);
    dbuf[frame.FRAME_HEADER_LEN] = 'x';
    try wire.appendSlice(std.testing.allocator, &dbuf);
    try session.ingest(wire.items);
    try std.testing.expect(session.terminal == .goaway);
    if (session.terminal == .goaway) {
        try std.testing.expectEqual(frame.ErrorCode.protocol_error, session.terminal.goaway.code);
    }
    freeIntents(session.drainIntents());
}

test "regression: path query limit+1 yields early 414" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    var long_q: [16 * 1024 + 1]u8 = undefined;
    @memset(&long_q, 'a');
    var path_buf: [16 * 1024 + 20]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/x?{s}", .{long_q[0..]});

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = path },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(block);

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try prefaceSettings(&wire);

    // Split oversized block across HEADERS + CONTINUATION (max frame 16KiB).
    const first_len = @min(block.len, 16 * 1024);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const fh = frame.FrameHeader{
        .length = @intCast(first_len),
        .type = .headers,
        .flags = .{ .end_headers = first_len == block.len, .end_stream = true },
        .stream_id = 1,
    };
    fh.encode(&hdr_buf);
    try wire.appendSlice(std.testing.allocator, &hdr_buf);
    try wire.appendSlice(std.testing.allocator, block[0..first_len]);
    var off = first_len;
    while (off < block.len) {
        const take = @min(block.len - off, 16 * 1024);
        const ch = frame.FrameHeader{
            .length = @intCast(take),
            .type = .continuation,
            .flags = .{ .end_headers = off + take == block.len },
            .stream_id = 1,
        };
        ch.encode(&hdr_buf);
        try wire.appendSlice(std.testing.allocator, &hdr_buf);
        try wire.appendSlice(std.testing.allocator, block[off .. off + take]);
        off += take;
    }
    try session.ingest(wire.items);
    const intents = session.drainIntents();
    defer freeIntents(intents);
    var saw_414 = false;
    for (intents) |it| switch (it) {
        .early_reject => |e| {
            if (e.status == 414) saw_414 = true;
        },
        else => {},
    };
    try std.testing.expect(saw_414);
    try std.testing.expect(session.terminal == .none);
}

test "regression: body limit+1 yields early 413 not dispatch" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/x" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(block);

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try prefaceSettings(&wire);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const fh = frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true },
        .stream_id = 1,
    };
    fh.encode(&hdr_buf);
    try wire.appendSlice(std.testing.allocator, &hdr_buf);
    try wire.appendSlice(std.testing.allocator, block);

    // Multiple DATA frames totaling 256KiB+1 with final END_STREAM
    var remaining: usize = 256 * 1024 + 1;
    while (remaining > 0) {
        const take = @min(remaining, 16 * 1024);
        remaining -= take;
        var data = try std.testing.allocator.alloc(u8, frame.FRAME_HEADER_LEN + take);
        defer std.testing.allocator.free(data);
        const dh = frame.FrameHeader{
            .length = @intCast(take),
            .type = .data,
            .flags = .{ .end_stream = remaining == 0 },
            .stream_id = 1,
        };
        dh.encode(data[0..frame.FRAME_HEADER_LEN]);
        @memset(data[frame.FRAME_HEADER_LEN..], 'b');
        try wire.appendSlice(std.testing.allocator, data);
    }

    try session.ingest(wire.items);
    const intents = session.drainIntents();
    defer freeIntents(intents);
    var saw_413 = false;
    var saw_dispatch = false;
    for (intents) |it| switch (it) {
        .early_reject => |e| {
            if (e.status == 413) saw_413 = true;
        },
        .dispatch_request => saw_dispatch = true,
        else => {},
    };
    try std.testing.expect(saw_413);
    try std.testing.expect(!saw_dispatch);
}

test "regression: half-closed-remote HEADERS is STREAM_CLOSED" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/hello" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(block);

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try prefaceSettings(&wire);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const fh = frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = true },
        .stream_id = 1,
    };
    fh.encode(&hdr_buf);
    try wire.appendSlice(std.testing.allocator, &hdr_buf);
    try wire.appendSlice(std.testing.allocator, block);
    try session.ingest(wire.items);
    freeIntents(session.drainIntents());

    // Second HEADERS on half-closed-remote
    try wire.resize(std.testing.allocator, 0);
    fh.encode(&hdr_buf);
    try wire.appendSlice(std.testing.allocator, &hdr_buf);
    try wire.appendSlice(std.testing.allocator, block);
    try session.ingest(wire.items);
    const intents = session.drainIntents();
    defer freeIntents(intents);
    var saw_rst = false;
    for (intents) |it| switch (it) {
        .stream_reset => |r| {
            if (r.code == .stream_closed) saw_rst = true;
        },
        .outbound_frame => |f| {
            if (f.typ == .rst_stream) saw_rst = true;
        },
        else => {},
    };
    try std.testing.expect(saw_rst);
}

test "regression: 414 immediate without remote END_STREAM" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    var long_q: [16 * 1024 + 1]u8 = undefined;
    @memset(&long_q, 'q');
    var path_buf: [16 * 1024 + 20]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/x?{s}", .{long_q[0..]});
    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = path },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(block);

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try prefaceSettings(&wire);
    const first_len = @min(block.len, 16 * 1024);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    // No END_STREAM on initial headers.
    const fh = frame.FrameHeader{
        .length = @intCast(first_len),
        .type = .headers,
        .flags = .{ .end_headers = first_len == block.len, .end_stream = false },
        .stream_id = 1,
    };
    fh.encode(&hdr_buf);
    try wire.appendSlice(std.testing.allocator, &hdr_buf);
    try wire.appendSlice(std.testing.allocator, block[0..first_len]);
    var off = first_len;
    while (off < block.len) {
        const take = @min(block.len - off, 16 * 1024);
        const ch = frame.FrameHeader{
            .length = @intCast(take),
            .type = .continuation,
            .flags = .{ .end_headers = off + take == block.len },
            .stream_id = 1,
        };
        ch.encode(&hdr_buf);
        try wire.appendSlice(std.testing.allocator, &hdr_buf);
        try wire.appendSlice(std.testing.allocator, block[off .. off + take]);
        off += take;
    }
    try session.ingest(wire.items);
    const intents = session.drainIntents();
    defer freeIntents(intents);
    var saw_414: usize = 0;
    for (intents) |it| switch (it) {
        .early_reject => |e| {
            if (e.status == 414) saw_414 += 1;
        },
        .dispatch_request => try std.testing.expect(false),
        else => {},
    };
    try std.testing.expectEqual(@as(usize, 1), saw_414);
    try std.testing.expect(session.streams.getPtr(1).?.early_response_sent);
}

test "regression: 413 immediate on DATA byte before END_STREAM" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    var limits = starh2.Limits.defaults;
    limits.request_body_bytes = 64;
    var session = try session_mod.Session.init(std.testing.allocator, limits);
    defer session.deinit();
    freeIntents(session.drainIntents());

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/x" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(block);

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try prefaceSettings(&wire);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const fh = frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true },
        .stream_id = 1,
    };
    fh.encode(&hdr_buf);
    try wire.appendSlice(std.testing.allocator, &hdr_buf);
    try wire.appendSlice(std.testing.allocator, block);
    // 65 bytes without END_STREAM → immediate 413
    var data: [frame.FRAME_HEADER_LEN + 65]u8 = undefined;
    const dh = frame.FrameHeader{ .length = 65, .type = .data, .flags = .{}, .stream_id = 1 };
    dh.encode(data[0..frame.FRAME_HEADER_LEN]);
    @memset(data[frame.FRAME_HEADER_LEN..], 'z');
    try wire.appendSlice(std.testing.allocator, &data);
    try session.ingest(wire.items);
    const intents = session.drainIntents();
    defer freeIntents(intents);
    var saw_413: usize = 0;
    for (intents) |it| switch (it) {
        .early_reject => |e| {
            if (e.status == 413) saw_413 += 1;
        },
        .dispatch_request => try std.testing.expect(false),
        else => {},
    };
    try std.testing.expectEqual(@as(usize, 1), saw_413);
}

test "regression: early-reject stream ending in trailers still gets one response" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    var limits = starh2.Limits.defaults;
    limits.request_body_bytes = 8;
    var session = try session_mod.Session.init(std.testing.allocator, limits);
    defer session.deinit();
    freeIntents(session.drainIntents());

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/x" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-length", .value = "100" }, // proves overflow → 413
    };
    const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(block);
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try prefaceSettings(&wire);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const fh = frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true },
        .stream_id = 1,
    };
    fh.encode(&hdr_buf);
    try wire.appendSlice(std.testing.allocator, &hdr_buf);
    try wire.appendSlice(std.testing.allocator, block);
    try session.ingest(wire.items);
    var intents = session.drainIntents();
    freeIntents(intents);
    try std.testing.expect(session.streams.getPtr(1).?.early_response_sent);

    // Trailers with END_STREAM — must not emit a second response.
    const trail = [_]hpack.HeaderField{.{ .name = "x-t", .value = "1" }};
    const tblock = try hpack.Encoder.encode(std.testing.allocator, &trail);
    defer std.testing.allocator.free(tblock);
    try wire.resize(std.testing.allocator, 0);
    const th = frame.FrameHeader{
        .length = @intCast(tblock.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = true },
        .stream_id = 1,
    };
    th.encode(&hdr_buf);
    try wire.appendSlice(std.testing.allocator, &hdr_buf);
    try wire.appendSlice(std.testing.allocator, tblock);
    try session.ingest(wire.items);
    intents = session.drainIntents();
    defer freeIntents(intents);
    var early_count: usize = 0;
    for (intents) |it| switch (it) {
        .early_reject => early_count += 1,
        .dispatch_request => try std.testing.expect(false),
        else => {},
    };
    try std.testing.expectEqual(@as(usize, 0), early_count);
}

test "regression: >256 sequential streams reuse concurrency on same session" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/hello" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(block);

    var sid: u31 = 1;
    var opened: usize = 0;
    while (opened < 300) : (opened += 1) {
        var wire: std.ArrayList(u8) = .empty;
        defer wire.deinit(std.testing.allocator);
        if (opened == 0) try prefaceSettings(&wire);
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = true },
            .stream_id = sid,
        };
        fh.encode(&hdr_buf);
        try wire.appendSlice(std.testing.allocator, &hdr_buf);
        try wire.appendSlice(std.testing.allocator, block);
        try session.ingest(wire.items);
        const intents = session.drainIntents();
        var respond_sid: ?u31 = null;
        for (intents) |*it| {
            switch (it.*) {
                .dispatch_request => |d| {
                    respond_sid = d.stream_id;
                    for (d.headers) |h| {
                        std.testing.allocator.free(@constCast(h.name));
                        std.testing.allocator.free(@constCast(h.value));
                    }
                    std.testing.allocator.free(d.headers);
                    for (d.trailers) |h| {
                        std.testing.allocator.free(@constCast(h.name));
                        std.testing.allocator.free(@constCast(h.value));
                    }
                    if (d.trailers.len != 0) std.testing.allocator.free(d.trailers);
                    if (d.body.len != 0) std.testing.allocator.free(d.body);
                },
                .outbound_frame => |f| std.testing.allocator.free(f.payload),
                else => {},
            }
        }
        if (respond_sid) |rsid| {
            session.applyCommand(.{ .respond_headers = .{
                .stream_id = rsid,
                .status = 200,
                .headers = &.{},
                .end_stream = true,
            } }) catch {};
        }
        freeIntents(session.drainIntents());
        try std.testing.expect(session.active_streams <= session.limits.max_streams_per_connection);
        sid += 2;
    }
    try std.testing.expect(sid > 512);
}

test "hpack insert OOM is OutOfMemory not CompressionError" {
    const hpack = starh2.core.hpack;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var dec = hpack.Decoder.init(failing.allocator());
    defer dec.deinit();
    // Indexed name + literal value with indexing forces insert dupes.
    // 0x40 = literal with incremental indexing, new name — will allocate.
    const block = [_]u8{ 0x40, 0x01, 'a', 0x01, 'b' };
    const result = dec.decode(&block, 10, 1000, 100, 100);
    try std.testing.expectError(error.OutOfMemory, result);
}


test "raised request_body_bytes: 413 on content-length and on cap+1" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    const raised: usize = 2 * 1024 * 1024;
    var limits = starh2.Limits.defaults;
    limits.request_body_bytes = raised;

    // Trigger A: content-length proves overflow before any DATA.
    {
        var session = try session_mod.Session.init(std.testing.allocator, limits);
        defer session.deinit();
        freeIntents(session.drainIntents());
        var cl_buf: [32]u8 = undefined;
        const cl = try std.fmt.bufPrint(&cl_buf, "{d}", .{raised + 1});
        const fields = [_]hpack.HeaderField{
            .{ .name = ":method", .value = "POST" },
            .{ .name = ":scheme", .value = "http" },
            .{ .name = ":path", .value = "/x" },
            .{ .name = ":authority", .value = "localhost" },
            .{ .name = "content-length", .value = cl },
        };
        const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
        defer std.testing.allocator.free(block);
        var wire: std.ArrayList(u8) = .empty;
        defer wire.deinit(std.testing.allocator);
        try prefaceSettings(&wire);
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true },
            .stream_id = 1,
        };
        fh.encode(&hdr_buf);
        try wire.appendSlice(std.testing.allocator, &hdr_buf);
        try wire.appendSlice(std.testing.allocator, block);
        try session.ingest(wire.items);
        const intents = session.drainIntents();
        defer freeIntents(intents);
        var saw_413 = false;
        var saw_dispatch = false;
        for (intents) |it| switch (it) {
            .early_reject => |e| {
                if (e.status == 413) saw_413 = true;
            },
            .dispatch_request => saw_dispatch = true,
            else => {},
        };
        try std.testing.expect(saw_413);
        try std.testing.expect(!saw_dispatch);
    }

    // Trigger B: no content-length; byte cap+1 on DATA emits 413 immediately.
    {
        var session = try session_mod.Session.init(std.testing.allocator, limits);
        defer session.deinit();
        freeIntents(session.drainIntents());
        const fields = [_]hpack.HeaderField{
            .{ .name = ":method", .value = "POST" },
            .{ .name = ":scheme", .value = "http" },
            .{ .name = ":path", .value = "/x" },
            .{ .name = ":authority", .value = "localhost" },
        };
        const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
        defer std.testing.allocator.free(block);
        var wire: std.ArrayList(u8) = .empty;
        defer wire.deinit(std.testing.allocator);
        try prefaceSettings(&wire);
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true },
            .stream_id = 1,
        };
        fh.encode(&hdr_buf);
        try wire.appendSlice(std.testing.allocator, &hdr_buf);
        try wire.appendSlice(std.testing.allocator, block);

        var remaining: usize = raised + 1;
        while (remaining > 0) {
            const take = @min(remaining, 16 * 1024);
            remaining -= take;
            var data = try std.testing.allocator.alloc(u8, frame.FRAME_HEADER_LEN + take);
            defer std.testing.allocator.free(data);
            const dh = frame.FrameHeader{
                .length = @intCast(take),
                .type = .data,
                .flags = .{ .end_stream = remaining == 0 },
                .stream_id = 1,
            };
            dh.encode(data[0..frame.FRAME_HEADER_LEN]);
            @memset(data[frame.FRAME_HEADER_LEN..], 'b');
            try wire.appendSlice(std.testing.allocator, data);
        }
        try session.ingest(wire.items);
        const intents = session.drainIntents();
        defer freeIntents(intents);
        var saw_413 = false;
        var saw_dispatch = false;
        for (intents) |it| switch (it) {
            .early_reject => |e| {
                if (e.status == 413) saw_413 = true;
            },
            .dispatch_request => saw_dispatch = true,
            else => {},
        };
        try std.testing.expect(saw_413);
        try std.testing.expect(!saw_dispatch);
    }
}
