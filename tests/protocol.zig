const std = @import("std");
const starh2 = @import("starh2");

fn freeIntents(intents: []starh2.core.session.Intent) void {
    for (intents) |*it| {
        switch (it.*) {
            .outbound_frame => |f| std.testing.allocator.free(f.payload),
            .dispatch_request => |d| {
                starh2.core.hpack.HeaderField.freeOwnedSlice(std.testing.allocator, d.headers);
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

test "RST_STREAM on a stream the server already closed is tolerated (RFC 9113 5.1)" {
    // A peer may send RST_STREAM before it has learned the stream closed: the
    // reset and the server's END_STREAM cross on the wire. RFC 9113 section 5.1
    // requires an endpoint to tolerate that on a CLOSED stream — only an IDLE
    // stream, one never opened, is a connection error.
    //
    // Treating the crossing case as a connection error kills every other stream
    // on the connection, and it happens whenever a browser cancels a fetch just
    // as the response completes.
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    const gpa = std.testing.allocator;

    var session = try session_mod.Session.init(gpa, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(gpa, sbuf[0..sn]);

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/hello" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(gpa, &fields);
    defer gpa.free(block);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = true },
        .stream_id = 1,
    }).encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);

    try session.ingest(wire.items);
    freeIntents(session.drainIntents());

    // The server completes the response, which closes stream 1 and drops it
    // from the live map.
    try session.applyCommand(.{ .respond_headers = .{
        .stream_id = 1,
        .status = 200,
        .headers = &.{},
        .end_stream = true,
    } });
    freeIntents(session.drainIntents());
    try std.testing.expect(session.streams.get(1) == null);
    try std.testing.expect(session.terminal == .none);

    // Now the peer's in-flight RST arrives for that closed stream.
    var rst: [13]u8 = undefined;
    const rn = try frame.Serializer.rstStream(&rst, 1, .cancel);
    try session.ingest(rst[0..rn]);
    freeIntents(session.drainIntents());

    // It must be ignored. The connection carries other streams.
    try std.testing.expect(session.terminal == .none);
}

test "RST_STREAM on a never-opened stream is still a connection error" {
    // The other half of RFC 9113 section 5.1: an IDLE stream has no state to
    // reset, so a reset for one is a protocol violation. Without this the fix
    // above would swallow a genuinely malformed peer.
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const gpa = std.testing.allocator;

    var session = try session_mod.Session.init(gpa, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(gpa, sbuf[0..sn]);
    try session.ingest(wire.items);
    freeIntents(session.drainIntents());

    var rst: [13]u8 = undefined;
    const rn = try frame.Serializer.rstStream(&rst, 99, .cancel);
    try session.ingest(rst[0..rn]);
    freeIntents(session.drainIntents());

    try std.testing.expect(session.terminal == .goaway);
}

test "WINDOW_UPDATE on a stream the server already closed is tolerated (RFC 9113 6.9)" {
    // The exact companion of the RST_STREAM case, and RFC 9113 section 6.9 is
    // explicit about it: "A receiver MUST NOT treat this as an error" when a
    // WINDOW_UPDATE arrives for a stream that has already ended, because the
    // peer sent the credit before it processed the frame closing the stream.
    //
    // This is the ordinary flow-control conversation, not an edge case: a
    // client grants credit for the DATA it just received while the server is
    // finishing the body. Failing the connection here kills every other stream
    // on it — which is how the t-482 gate saw BrokenPipe on its credit write.
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    const gpa = std.testing.allocator;

    var session = try session_mod.Session.init(gpa, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(gpa, sbuf[0..sn]);

    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/hello" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(gpa, &fields);
    defer gpa.free(block);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = true },
        .stream_id = 1,
    }).encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);
    try session.ingest(wire.items);
    freeIntents(session.drainIntents());

    try session.applyCommand(.{ .respond_headers = .{
        .stream_id = 1,
        .status = 200,
        .headers = &.{},
        .end_stream = true,
    } });
    freeIntents(session.drainIntents());
    try std.testing.expect(session.streams.get(1) == null);

    // The peer's in-flight credit for the stream that just ended.
    var wu: [13]u8 = undefined;
    const wn = try frame.Serializer.windowUpdate(&wu, 1, 4096);
    try session.ingest(wu[0..wn]);
    freeIntents(session.drainIntents());

    try std.testing.expect(session.terminal == .none);
}

test "WINDOW_UPDATE on a never-opened stream is still a connection error" {
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const gpa = std.testing.allocator;

    var session = try session_mod.Session.init(gpa, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(gpa, sbuf[0..sn]);
    try session.ingest(wire.items);
    freeIntents(session.drainIntents());

    var wu: [13]u8 = undefined;
    const wn = try frame.Serializer.windowUpdate(&wu, 77, 4096);
    try session.ingest(wu[0..wn]);
    freeIntents(session.drainIntents());

    try std.testing.expect(session.terminal == .goaway);
}

test "DATA on a closed stream is a STREAM error, not a connection error (RFC 9113 6.1)" {
    // Reachable whenever the server ends a stream while the client is still
    // uploading: an abort, a slow-consumer reset, or a handler that responds
    // early. RFC 9113 section 6.1 makes DATA for a non-open stream a STREAM
    // error of type STREAM_CLOSED. Killing the connection instead takes every
    // other stream with it.
    //
    // The connection flow-control window must still be debited either way
    // (section 6.9.1), or the two sides disagree about credit for the rest of
    // the connection.
    const session_mod = starh2.core.session;
    const frame = starh2.core.frame;
    const hpack = starh2.core.hpack;
    const gpa = std.testing.allocator;

    var session = try session_mod.Session.init(gpa, .defaults);
    defer session.deinit();
    freeIntents(session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(gpa, sbuf[0..sn]);

    // Open stream 1 WITHOUT end_stream: the client intends to send a body.
    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/upload" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(gpa, &fields);
    defer gpa.free(block);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true },
        .stream_id = 1,
    }).encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);
    try session.ingest(wire.items);
    freeIntents(session.drainIntents());

    // The server resets the stream — an abort, or a slow-consumer kill.
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 1, .code = .cancel } });
    freeIntents(session.drainIntents());
    try std.testing.expect(session.streams.get(1) == null);
    try std.testing.expect(session.terminal == .none);

    // The client's in-flight body arrives for the stream that just died.
    const conn_before = session.windows.conn_recv;
    var data_hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const body = "hello body";
    (frame.FrameHeader{
        .length = body.len,
        .type = .data,
        .flags = .{},
        .stream_id = 1,
    }).encode(&data_hdr);
    var data_wire: std.ArrayList(u8) = .empty;
    defer data_wire.deinit(gpa);
    try data_wire.appendSlice(gpa, &data_hdr);
    try data_wire.appendSlice(gpa, body);
    try session.ingest(data_wire.items);
    freeIntents(session.drainIntents());

    // The connection survives, and the credit was accounted for.
    try std.testing.expect(session.terminal == .none);
    try std.testing.expect(session.windows.conn_recv < conn_before);
}
