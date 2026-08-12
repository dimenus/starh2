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
                if (d.body.len != 0) std.testing.allocator.free(d.body);
            },
            else => {},
        }
    }
    // drainIntents returns boot scratch — do not free the slice
}

test "frame corpus reaches types" {
    const frame = starh2.core.frame;
    var parser = frame.Parser.init(std.testing.allocator, frame.DEFAULT_MAX_FRAME_SIZE);
    defer parser.deinit();
    parser.skipPreface();

    var buf: [32]u8 = undefined;
    const opaque_data = [_]u8{0} ** 8;
    const n = try frame.Serializer.ping(&buf, false, &opaque_data);
    const r = try parser.ingestOne(buf[0..n]);
    try std.testing.expect(r != null);
    defer std.testing.allocator.free(r.?.event.payload);
    try std.testing.expectEqual(frame.FrameType.ping, r.?.event.header.type);
}

test "session fragmentation invariance" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try wire.appendSlice(std.testing.allocator, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(std.testing.allocator, sbuf[0..sn]);

    var s1 = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer s1.deinit();
    freeIntents(s1.drainIntents());
    try s1.ingest(wire.items);
    freeIntents(s1.drainIntents());

    var s2 = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer s2.deinit();
    freeIntents(s2.drainIntents());
    for (wire.items) |b| {
        try s2.ingest(&[_]u8{b});
    }
    freeIntents(s2.drainIntents());
    try std.testing.expectEqual(s1.terminal, s2.terminal);
    try std.testing.expectEqual(s1.last_processed_stream, s2.last_processed_stream);
}

test "error corpus: push_promise is connection PROTOCOL_ERROR" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try wire.appendSlice(std.testing.allocator, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(std.testing.allocator, sbuf[0..sn]);
    // PUSH_PROMISE on stream 1
    const pp = [_]u8{ 0, 0, 4, 0x5, 0x4, 0, 0, 0, 1, 0, 0, 0, 3 };
    try wire.appendSlice(std.testing.allocator, &pp);
    try session.ingest(wire.items);
    try std.testing.expect(session.terminal == .goaway);
    freeIntents(session.drainIntents());
}

test "graceful two-phase GOAWAY" {
    const session_mod = starh2.core.session;
    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());
    try session.applyCommand(.graceful_phase1);
    const phase1 = session.drainIntents();
    var saw_goaway = false;
    var saw_ping = false;
    for (phase1) |it| switch (it) {
        .outbound_frame => |f| {
            if (f.typ == .goaway) saw_goaway = true;
            if (f.typ == .ping) saw_ping = true;
        },
        else => {},
    };
    freeIntents(phase1);
    try std.testing.expect(saw_goaway and saw_ping);
    try std.testing.expect(session.grace_phase == .phase1);
    try session.applyCommand(.graceful_phase2);
    const phase2 = session.drainIntents();
    defer freeIntents(phase2);
    try std.testing.expect(session.grace_phase == .phase2);
    try std.testing.expect(session.goaway_ceiling != null);
}

test "session handles GET /hello headers end_stream" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;

    var session = try session_mod.Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try wire.appendSlice(std.testing.allocator, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(std.testing.allocator, sbuf[0..sn]);

    // HEADERS for GET /hello https
    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":path", .value = "/hello" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(block);
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
    const intents = session.drainIntents();
    defer freeIntents(intents);
    var saw_dispatch = false;
    var saw_settings_ack = false;
    for (intents) |it| {
        switch (it) {
            .dispatch_request => |d| {
                saw_dispatch = true;
                try std.testing.expectEqualStrings("/hello", d.path);
            },
            .outbound_frame => |f| {
                if (f.typ == .settings and f.payload.len >= 5 and f.payload[4] == 1) saw_settings_ack = true;
            },
            else => {},
        }
    }
    try std.testing.expect(saw_dispatch);
    try std.testing.expect(saw_settings_ack);
}
