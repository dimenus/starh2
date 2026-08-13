//! Shared HTTP/2 client frame builders for tests and dependent packages.
//! Zig 0.16 std.http.Client is HTTP/1.1-only; these are the h2 client bytes.
//!
//! A test needs a client that sends EXACTLY the bytes it means to send,
//! including the malformed and adversarial ones a real client would never
//! produce. So these helpers build wire bytes and hold no connection state.
//! A test drives the socket itself.
//!
//! This is exported rather than kept in `tests/`, because a dependent package
//! has the same need and would otherwise reimplement it — and a second copy of
//! a frame builder is a second place for the two to disagree about what a
//! correct request looks like.
const std = @import("std");
const starh2 = @import("starh2");

const frame = starh2.core.frame;
const hpack = starh2.core.hpack;

pub const PrefaceKind = enum {
    /// SETTINGS: max_concurrent_streams=256, initial_window_size=1<<20
    defaults,
    /// Empty SETTINGS frame only
    empty,
};

pub fn appendHeaders(
    gpa: std.mem.Allocator,
    wire: *std.ArrayList(u8),
    stream_id: u31,
    path: []const u8,
    end_stream: bool,
) !void {
    try appendHeadersExtra(gpa, wire, stream_id, "GET", path, end_stream, &.{});
}

pub fn appendHeadersExtra(
    gpa: std.mem.Allocator,
    wire: *std.ArrayList(u8),
    stream_id: u31,
    method: []const u8,
    path: []const u8,
    end_stream: bool,
    extra: []const hpack.HeaderField,
) !void {
    var fields: std.ArrayList(hpack.HeaderField) = .empty;
    defer fields.deinit(gpa);
    try fields.append(gpa, .{ .name = ":method", .value = method });
    try fields.append(gpa, .{ .name = ":scheme", .value = "http" });
    try fields.append(gpa, .{ .name = ":path", .value = path });
    try fields.append(gpa, .{ .name = ":authority", .value = "localhost" });
    for (extra) |f| try fields.append(gpa, f);
    const block = try hpack.Encoder.encode(gpa, fields.items);
    defer gpa.free(block);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const fh = frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = end_stream },
        .stream_id = stream_id,
    };
    fh.encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);
}

pub fn buildClientHelloExtra(
    gpa: std.mem.Allocator,
    path: []const u8,
    extra: []const hpack.HeaderField,
) ![]u8 {
    var wire = try buildClientPrefaceAndSettings(gpa);
    errdefer wire.deinit(gpa);
    try appendHeadersExtra(gpa, &wire, 1, "GET", path, true, extra);
    return try wire.toOwnedSlice(gpa);
}

pub fn buildClientPrefaceAndSettings(gpa: std.mem.Allocator) !std.ArrayList(u8) {
    var wire: std.ArrayList(u8) = .empty;
    errdefer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    var sbuf: [64]u8 = undefined;
    const settings = [_]frame.Setting{
        .{ .id = .max_concurrent_streams, .value = 256 },
        .{ .id = .initial_window_size, .value = 1 << 20 },
    };
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
    try wire.appendSlice(gpa, sbuf[0..sn]);
    return wire;
}

pub fn buildClientPreface(gpa: std.mem.Allocator, kind: PrefaceKind) !std.ArrayList(u8) {
    return switch (kind) {
        .defaults => buildClientPrefaceAndSettings(gpa),
        .empty => blk: {
            var wire: std.ArrayList(u8) = .empty;
            errdefer wire.deinit(gpa);
            try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
            var sbuf: [64]u8 = undefined;
            const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
            try wire.appendSlice(gpa, sbuf[0..sn]);
            break :blk wire;
        },
    };
}

pub fn buildClientHello(gpa: std.mem.Allocator, path: []const u8) ![]u8 {
    var wire = try buildClientPrefaceAndSettings(gpa);
    errdefer wire.deinit(gpa);
    try appendHeaders(gpa, &wire, 1, path, true);
    return try wire.toOwnedSlice(gpa);
}

pub fn buildClientPost(gpa: std.mem.Allocator, path: []const u8, body: []const u8) ![]u8 {
    var wire = try buildClientPrefaceAndSettings(gpa);
    errdefer wire.deinit(gpa);

    var content_len_buf: [32]u8 = undefined;
    const content_len = try std.fmt.bufPrint(&content_len_buf, "{d}", .{body.len});
    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = path },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-length", .value = content_len },
    };
    const block = try hpack.Encoder.encode(gpa, &fields);
    defer gpa.free(block);

    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const headers = frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true },
        .stream_id = 1,
    };
    headers.encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);

    const data = frame.FrameHeader{
        .length = @intCast(body.len),
        .type = .data,
        .flags = .{ .end_stream = true },
        .stream_id = 1,
    };
    data.encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, body);
    return try wire.toOwnedSlice(gpa);
}

pub fn buildClientHelloWindow(
    gpa: std.mem.Allocator,
    path: []const u8,
    stream_id: u31,
    win: u32,
    preface: PrefaceKind,
) ![]u8 {
    var wire = try buildClientPreface(gpa, preface);
    errdefer wire.deinit(gpa);
    {
        var sbuf: [64]u8 = undefined;
        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = win }};
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
        try wire.appendSlice(gpa, sbuf[0..sn]);
    }
    try appendHeaders(gpa, &wire, stream_id, path, true);
    return try wire.toOwnedSlice(gpa);
}

pub fn appendWindowUpdate(gpa: std.mem.Allocator, wire: *std.ArrayList(u8), stream_id: u31, incr: u31) !void {
    var buf: [13]u8 = undefined;
    const n = try frame.Serializer.windowUpdate(&buf, stream_id, incr);
    try wire.appendSlice(gpa, buf[0..n]);
}

pub fn appendRst(gpa: std.mem.Allocator, wire: *std.ArrayList(u8), stream_id: u31) !void {
    var buf: [13]u8 = undefined;
    const n = try frame.Serializer.rstStream(&buf, stream_id, .cancel);
    try wire.appendSlice(gpa, buf[0..n]);
}
