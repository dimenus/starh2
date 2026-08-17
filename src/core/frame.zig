//! Bounded HTTP/2 frame header/payload parsing and serialization (RFC 9113).
//!
//! This module is the lowest layer of the inbound flow:
//! socket bytes -> `Parser` -> `FrameEvent` -> `core.session` -> `Intent`.
//! It knows the wire format only. It applies no protocol semantics, keeps no
//! stream state, and reads no clock. `core.session` owns all of that. The split
//! exists so that a malformed frame becomes a typed `ParseError` at one place,
//! and so that the parser stays testable without a socket.
//!
//! A complete frame already contiguous in the ingest chunk is a borrow of that
//! chunk. `Session.handleFrame` must finish before the chunk is recycled; the
//! next `ingestOne` on the remainder is allowed only after that. A frame split
//! across chunks is copied into boot-reserved scratch (production) or a `gpa`
//! buffer (tests). Copying a complete in-chunk payload into scratch is a second
//! copy the lifetime contract does not require.
//!
//! `FramePayload`'s tag is the ownership; `FrameEvent.deinit` is the only free
//! path — a no-op for an unowned view.
const std = @import("std");
const assert = std.debug.assert;

pub const FRAME_HEADER_LEN: usize = 9;
pub const DEFAULT_MAX_FRAME_SIZE: u32 = 16_384;
pub const CLIENT_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub const FrameType = enum(u8) {
    data = 0x0,
    headers = 0x1,
    priority = 0x2,
    rst_stream = 0x3,
    settings = 0x4,
    push_promise = 0x5,
    ping = 0x6,
    goaway = 0x7,
    window_update = 0x8,
    continuation = 0x9,
    _,
};

pub const ErrorCode = enum(u32) {
    no_error = 0x0,
    protocol_error = 0x1,
    internal_error = 0x2,
    flow_control_error = 0x3,
    settings_timeout = 0x4,
    stream_closed = 0x5,
    frame_size_error = 0x6,
    refused_stream = 0x7,
    cancel = 0x8,
    compression_error = 0x9,
    connect_error = 0xa,
    enhance_your_calm = 0xb,
    inadequate_security = 0xc,
    http_1_1_required = 0xd,
    _,
};

/// The flag byte is positional, and one bit carries two meanings. Bit 0 is
/// END_STREAM on DATA and HEADERS, and it is ACK on SETTINGS and PING. The
/// struct keeps the single bit and gives it two readers (`end_stream` and
/// `ack`), because a second field would let the two names disagree.
pub const FrameFlags = packed struct(u8) {
    end_stream: bool = false, // bit 0 / ACK for SETTINGS/PING
    unused1: bool = false,
    end_headers: bool = false, // bit 2
    padded: bool = false, // bit 3
    unused4: bool = false,
    priority: bool = false, // bit 5
    unused6: bool = false,
    unused7: bool = false,

    pub fn fromByte(b: u8) FrameFlags {
        return @bitCast(b);
    }
    pub fn toByte(self: FrameFlags) u8 {
        return @bitCast(self);
    }
    pub fn ack(self: FrameFlags) bool {
        return self.end_stream;
    }
};

pub const FrameHeader = struct {
    length: u24,
    type: FrameType,
    flags: FrameFlags,
    stream_id: u31,

    pub fn encode(self: FrameHeader, out: *[FRAME_HEADER_LEN]u8) void {
        std.mem.writeInt(u24, out[0..3], self.length, .big);
        out[3] = @intFromEnum(self.type);
        out[4] = self.flags.toByte();
        std.mem.writeInt(u32, out[5..9], @as(u32, self.stream_id), .big);
    }

    pub fn decode(bytes: *const [FRAME_HEADER_LEN]u8) FrameHeader {
        const length = std.mem.readInt(u24, bytes[0..3], .big);
        const typ: FrameType = @enumFromInt(bytes[3]);
        const flags = FrameFlags.fromByte(bytes[4]);
        const stream_id: u31 = @truncate(std.mem.readInt(u32, bytes[5..9], .big) & 0x7fff_ffff);
        return .{
            .length = length,
            .type = typ,
            .flags = flags,
            .stream_id = stream_id,
        };
    }
};

/// On-wire byte count for a complete HTTP/2 frame sitting in `payload`.
///
/// A boot-reserved frame slab is larger than the frame. Callers that copy or
/// reserve occupancy must use this, not `payload.len`, or they will copy slab
/// tail bytes onto the wire. A buffer too short to hold a header, or whose
/// header length overruns the buffer, is treated as opaque: return `payload.len`
/// so tests that enqueue dummy bytes keep working.
pub fn encodedOnWireLen(payload: []const u8) usize {
    if (payload.len < FRAME_HEADER_LEN) return payload.len;
    const hdr_len = std.mem.readInt(u24, payload[0..3], .big);
    const total = FRAME_HEADER_LEN + @as(usize, hdr_len);
    if (total > payload.len) return payload.len;
    return total;
}

/// Borrow of the on-wire prefix. Not an ownership transfer: the caller still
/// frees `payload` as the original slice (full slab or GPA allocation).
pub fn encodedOnWire(payload: []u8) []u8 {
    return payload[0..encodedOnWireLen(payload)];
}

pub const ParseError = error{
    NeedMore,
    FrameSizeError,
    ProtocolError,
    EnhanceYourCalm,
};

/// How long inbound payload bytes live, and who frees them.
///
/// The tag is the ownership. `bytes` is a read view; `deinit(gpa)` is the only
/// free path. A bool named `owned` collapsed two meanings and let a caller free
/// a scratch borrow.
pub const PayloadLifetime = enum {
    /// Zero-length. Nothing to free.
    empty,
    /// Unowned view: either the ingest chunk (complete in-chunk frame) or the
    /// parser's boot-reserved scratch (split frame). Valid only until the
    /// ingest handler returns. Do not free.
    scratch,
    /// Heap buffer the parser transferred. `FrameEvent.deinit` frees it with `gpa`.
    gpa,
};

pub const FramePayload = union(PayloadLifetime) {
    empty,
    scratch: []const u8,
    gpa: []u8,

    pub fn bytes(self: FramePayload) []const u8 {
        return switch (self) {
            .empty => &.{},
            .scratch => |s| s,
            .gpa => |s| s,
        };
    }

    pub fn deinit(self: FramePayload, gpa: std.mem.Allocator) void {
        switch (self) {
            .gpa => |buf| if (buf.len != 0) gpa.free(buf),
            .empty, .scratch => {},
        }
    }
};

pub const FrameEvent = struct {
    header: FrameHeader,
    payload: FramePayload,

    /// Release a transferred `gpa` payload. Scratch and empty are no-ops, so a
    /// reserved parser's `deinit` remains the owner of the scratch buffer.
    pub fn deinit(self: FrameEvent, gpa: std.mem.Allocator) void {
        self.payload.deinit(gpa);
    }
};

/// Incremental frame parser. A TCP read gives an arbitrary byte count, so a
/// frame header, a payload, or both can arrive in pieces across many chunks.
/// The parser holds that partial state between calls and only reports a frame
/// when it is complete.
///
/// Two payload strategies exist for frames that arrive in pieces:
/// - `initReserved` (production) reserves one max-size scratch buffer at
///   connection boot. Split frames copy into that scratch; `Session` copies
///   anything it must retain past `handleFrame`. The hot path therefore never
///   grows a buffer, which is what keeps the connection inside
///   `Limits.resourceUpperBound`.
/// - `init` (tests) allocates each split payload from `gpa` and transfers it
///   on the event. A complete in-chunk frame still borrows `input`.
pub const Parser = struct {
    max_frame_size: u32 = DEFAULT_MAX_FRAME_SIZE,
    header_buf: [FRAME_HEADER_LEN]u8 = undefined,
    header_filled: usize = 0,
    header: ?FrameHeader = null,
    payload_filled: usize = 0,
    /// Fill window for the in-progress frame. Prefix of `scratch`, or a `gpa`
    /// allocation this parser still holds when `payload_kind` is `.gpa`.
    payload_buf: []u8 = &.{},
    /// What `payload_buf` is while a frame is being assembled.
    payload_kind: enum { none, scratch, gpa } = .none,
    /// Boot-reserved payload window. Empty means this parser allocates per frame.
    scratch: []u8 = &.{},
    /// Owns `scratch` and any not-yet-transferred `init` payload.
    gpa: std.mem.Allocator,
    preface_remaining: usize = CLIENT_PREFACE.len,
    expecting_preface: bool = true,

    pub fn init(gpa: std.mem.Allocator, max_frame_size: u32) Parser {
        return .{
            .gpa = gpa,
            .max_frame_size = max_frame_size,
        };
    }

    /// Boot-reserved payload scratch. Completed events borrow it until the
    /// ingest handler returns; the caller copies anything it must retain.
    pub fn initReserved(gpa: std.mem.Allocator, max_frame_size: u32) !Parser {
        const scratch = try gpa.alloc(u8, max_frame_size);
        return .{
            .gpa = gpa,
            .max_frame_size = max_frame_size,
            .payload_buf = scratch,
            .scratch = scratch,
        };
    }

    pub fn deinit(self: *Parser) void {
        if (self.scratch.len != 0) {
            self.gpa.free(self.scratch);
        } else if (self.payload_kind == .gpa and self.payload_buf.len != 0) {
            self.gpa.free(self.payload_buf);
        }
        self.* = undefined;
    }

    pub fn skipPreface(self: *Parser) void {
        self.expecting_preface = false;
        self.preface_remaining = 0;
    }

    /// A frame larger than the scratch buffer is a FRAME_SIZE_ERROR, not an
    /// allocation. The peer agreed to `max_frame_size` in SETTINGS, so a bigger
    /// frame is a peer fault. An allocation here would let a peer choose the
    /// server's memory use.
    fn preparePayloadBuf(self: *Parser, length: u32) ParseError!void {
        self.payload_filled = 0;
        if (length == 0) {
            self.payload_buf = &.{};
            self.payload_kind = .none;
            return;
        }
        if (self.scratch.len != 0) {
            if (length > self.scratch.len) return error.FrameSizeError;
            self.payload_buf = self.scratch[0..length];
            self.payload_kind = .scratch;
            return;
        }
        self.payload_buf = self.gpa.alloc(u8, length) catch return error.EnhanceYourCalm;
        self.payload_kind = .gpa;
    }

    fn takeCompletedPayload(self: *Parser, hdr: FrameHeader) FramePayload {
        if (hdr.length == 0) {
            self.payload_kind = .none;
            return .empty;
        }
        if (self.payload_kind == .scratch) {
            const view = self.scratch[0..hdr.length];
            self.payload_kind = .none;
            return .{ .scratch = view };
        }
        assert(self.payload_kind == .gpa);
        const owned = self.payload_buf;
        self.payload_buf = &.{};
        self.payload_kind = .none;
        return .{ .gpa = owned };
    }

    /// Ingest a chunk. Returns the first completed frame or null (NeedMore).
    /// Remainder is re-fed via `ingestOne`. A complete in-chunk frame borrows
    /// `input` until the handler returns.
    pub fn ingest(self: *Parser, input: []const u8) ParseError!?FrameEvent {
        const r = try self.ingestOne(input) orelse return null;
        return r.event;
    }

    /// Ingest all complete frames from `input`, calling `handler` for each.
    /// `handler` must `event.deinit(gpa)` before it returns; that is a no-op
    /// when the payload is a scratch borrow.
    pub fn ingestAll(
        self: *Parser,
        input: []const u8,
        context: anytype,
        comptime handler: fn (@TypeOf(context), FrameEvent) anyerror!void,
    ) ParseError!void {
        var remaining = input;
        while (remaining.len > 0) {
            const before_len = remaining.len;
            // Feed one byte at a time through a sliding approach: try whole remaining
            const maybe = try self.ingestOne(remaining);
            if (maybe) |result| {
                try handler(context, result.event);
                remaining = remaining[result.consumed..];
            } else {
                // consumed all remaining into partial state
                return;
            }
            if (remaining.len == before_len) return error.ProtocolError; // no progress
        }
    }

    const IngestOneResult = struct { event: FrameEvent, consumed: usize };

    pub fn ingestOne(self: *Parser, input: []const u8) ParseError!?IngestOneResult {
        var offset: usize = 0;
        if (self.expecting_preface) {
            const need = self.preface_remaining;
            const take = @min(need, input.len - offset);
            if (take == 0) return null;
            const expected = CLIENT_PREFACE[CLIENT_PREFACE.len - need ..][0..take];
            if (!std.mem.eql(u8, input[offset..][0..take], expected)) {
                return error.ProtocolError;
            }
            offset += take;
            self.preface_remaining -= take;
            if (self.preface_remaining == 0) self.expecting_preface = false;
            if (offset == input.len and self.header == null and self.header_filled == 0) return null;
        }

        // Complete frame already in this chunk: borrow the payload. Split
        // frames still copy into scratch / gpa below.
        if (self.header == null and self.header_filled == 0 and self.payload_filled == 0 and
            input.len - offset >= FRAME_HEADER_LEN)
        {
            const hdr_bytes: *const [FRAME_HEADER_LEN]u8 = @ptrCast(input[offset..].ptr);
            const hdr = FrameHeader.decode(hdr_bytes);
            if (hdr.length > self.max_frame_size) return error.FrameSizeError;
            const total = FRAME_HEADER_LEN + @as(usize, hdr.length);
            if (input.len - offset >= total) {
                const payload: FramePayload = if (hdr.length == 0)
                    .empty
                else
                    .{ .scratch = input[offset + FRAME_HEADER_LEN ..][0..hdr.length] };
                return .{
                    .event = .{ .header = hdr, .payload = payload },
                    .consumed = offset + total,
                };
            }
        }

        while (true) {
            if (self.header == null) {
                const need = FRAME_HEADER_LEN - self.header_filled;
                const take = @min(need, input.len - offset);
                if (take == 0 and need > 0) return null;
                @memcpy(self.header_buf[self.header_filled..][0..take], input[offset..][0..take]);
                self.header_filled += take;
                offset += take;
                if (self.header_filled < FRAME_HEADER_LEN) return null;
                const hdr = FrameHeader.decode(self.header_buf[0..FRAME_HEADER_LEN]);
                if (hdr.length > self.max_frame_size) return error.FrameSizeError;
                self.header = hdr;
                self.header_filled = 0;
                try self.preparePayloadBuf(hdr.length);
            }

            const hdr = self.header.?;
            if (hdr.length > 0) {
                const need = @as(usize, hdr.length) - self.payload_filled;
                const take = @min(need, input.len - offset);
                if (take == 0 and need > 0) return null;
                @memcpy(self.payload_buf[self.payload_filled..][0..take], input[offset..][0..take]);
                self.payload_filled += take;
                offset += take;
                if (self.payload_filled < hdr.length) return null;
            }

            const event = FrameEvent{
                .header = hdr,
                .payload = self.takeCompletedPayload(hdr),
            };
            self.header = null;
            self.payload_filled = 0;
            return .{ .event = event, .consumed = offset };
        }
    }
};

pub const Serializer = struct {
    pub fn writeFrame(writer: anytype, typ: FrameType, flags: FrameFlags, stream_id: u31, payload: []const u8) !void {
        if (payload.len > DEFAULT_MAX_FRAME_SIZE) return error.FrameSizeError;
        var hdr_buf: [FRAME_HEADER_LEN]u8 = undefined;
        const hdr = FrameHeader{
            .length = @intCast(payload.len),
            .type = typ,
            .flags = flags,
            .stream_id = stream_id,
        };
        hdr.encode(&hdr_buf);
        try writer.writeAll(&hdr_buf);
        try writer.writeAll(payload);
    }

    pub fn settingsFrame(buf: []u8, ack: bool, settings: []const Setting) error{BufferTooSmall}!usize {
        if (ack) {
            if (buf.len < FRAME_HEADER_LEN) return error.BufferTooSmall;
            const hdr = FrameHeader{
                .length = 0,
                .type = .settings,
                .flags = .{ .end_stream = true },
                .stream_id = 0,
            };
            hdr.encode(buf[0..FRAME_HEADER_LEN]);
            return FRAME_HEADER_LEN;
        }
        const payload_len = settings.len * 6;
        if (buf.len < FRAME_HEADER_LEN + payload_len) return error.BufferTooSmall;
        const hdr = FrameHeader{
            .length = @intCast(payload_len),
            .type = .settings,
            .flags = .{},
            .stream_id = 0,
        };
        hdr.encode(buf[0..FRAME_HEADER_LEN]);
        var i: usize = FRAME_HEADER_LEN;
        for (settings) |s| {
            std.mem.writeInt(u16, buf[i..][0..2], @intFromEnum(s.id), .big);
            std.mem.writeInt(u32, buf[i + 2 ..][0..4], s.value, .big);
            i += 6;
        }
        return i;
    }

    pub fn windowUpdate(buf: []u8, stream_id: u31, increment: u31) error{BufferTooSmall}!usize {
        if (buf.len < FRAME_HEADER_LEN + 4) return error.BufferTooSmall;
        const hdr = FrameHeader{
            .length = 4,
            .type = .window_update,
            .flags = .{},
            .stream_id = stream_id,
        };
        hdr.encode(buf[0..FRAME_HEADER_LEN]);
        std.mem.writeInt(u32, buf[FRAME_HEADER_LEN..][0..4], @as(u32, increment), .big);
        return FRAME_HEADER_LEN + 4;
    }

    pub fn ping(buf: []u8, ack: bool, opaque_data: *const [8]u8) error{BufferTooSmall}!usize {
        if (buf.len < FRAME_HEADER_LEN + 8) return error.BufferTooSmall;
        const hdr = FrameHeader{
            .length = 8,
            .type = .ping,
            .flags = .{ .end_stream = ack },
            .stream_id = 0,
        };
        hdr.encode(buf[0..FRAME_HEADER_LEN]);
        @memcpy(buf[FRAME_HEADER_LEN..][0..8], opaque_data);
        return FRAME_HEADER_LEN + 8;
    }

    pub fn goaway(buf: []u8, last_stream_id: u31, error_code: ErrorCode, debug: []const u8) error{BufferTooSmall}!usize {
        const payload_len = 8 + debug.len;
        if (buf.len < FRAME_HEADER_LEN + payload_len) return error.BufferTooSmall;
        const hdr = FrameHeader{
            .length = @intCast(payload_len),
            .type = .goaway,
            .flags = .{},
            .stream_id = 0,
        };
        hdr.encode(buf[0..FRAME_HEADER_LEN]);
        std.mem.writeInt(u32, buf[FRAME_HEADER_LEN..][0..4], @as(u32, last_stream_id), .big);
        std.mem.writeInt(u32, buf[FRAME_HEADER_LEN + 4 ..][0..4], @intFromEnum(error_code), .big);
        @memcpy(buf[FRAME_HEADER_LEN + 8 ..][0..debug.len], debug);
        return FRAME_HEADER_LEN + payload_len;
    }

    pub fn rstStream(buf: []u8, stream_id: u31, error_code: ErrorCode) error{BufferTooSmall}!usize {
        if (buf.len < FRAME_HEADER_LEN + 4) return error.BufferTooSmall;
        const hdr = FrameHeader{
            .length = 4,
            .type = .rst_stream,
            .flags = .{},
            .stream_id = stream_id,
        };
        hdr.encode(buf[0..FRAME_HEADER_LEN]);
        std.mem.writeInt(u32, buf[FRAME_HEADER_LEN..][0..4], @intFromEnum(error_code), .big);
        return FRAME_HEADER_LEN + 4;
    }
};

pub const SettingId = enum(u16) {
    header_table_size = 0x1,
    enable_push = 0x2,
    max_concurrent_streams = 0x3,
    initial_window_size = 0x4,
    max_frame_size = 0x5,
    max_header_list_size = 0x6,
    _,
};

pub const Setting = struct {
    id: SettingId,
    value: u32,
};

/// The server sends these SETTINGS as the first frame of every connection.
/// `enable_push = 0` is load-bearing: this stack never implements PUSH_PROMISE,
/// so it refuses the capability at the handshake instead of at frame receipt.
/// The other values match the profile that the rest of the stack is sized for.
/// `max_concurrent_streams` mirrors `Limits.max_streams_per_connection`, and
/// `max_frame_size` mirrors the parser's boot-reserved scratch buffer.
pub const serverPrefaceSettings = [_]Setting{
    .{ .id = .header_table_size, .value = 4096 },
    .{ .id = .enable_push, .value = 0 },
    .{ .id = .max_concurrent_streams, .value = 256 },
    .{ .id = .initial_window_size, .value = 65_535 },
    .{ .id = .max_frame_size, .value = 16_384 },
    .{ .id = .max_header_list_size, .value = 32_768 },
};

test "encodedOnWireLen reads the header, not the slab tail" {
    var slab: [64]u8 = undefined;
    @memset(&slab, 0xAA);
    const hdr = FrameHeader{
        .length = 4,
        .type = .window_update,
        .flags = .{},
        .stream_id = 0,
    };
    hdr.encode(slab[0..FRAME_HEADER_LEN]);
    try std.testing.expectEqual(@as(usize, FRAME_HEADER_LEN + 4), encodedOnWireLen(&slab));
    try std.testing.expectEqual(@as(usize, 3), encodedOnWireLen("abc"));
}

test "frame header roundtrip" {
    var buf: [9]u8 = undefined;
    const hdr = FrameHeader{
        .length = 100,
        .type = .data,
        .flags = .{ .end_stream = true, .padded = true },
        .stream_id = 7,
    };
    hdr.encode(&buf);
    const decoded = FrameHeader.decode(&buf);
    try std.testing.expectEqual(hdr.length, decoded.length);
    try std.testing.expectEqual(hdr.type, decoded.type);
    try std.testing.expectEqual(hdr.stream_id, decoded.stream_id);
    try std.testing.expect(decoded.flags.end_stream);
    try std.testing.expect(decoded.flags.padded);
}

test "preface rejection" {
    var parser = Parser.init(std.testing.allocator, DEFAULT_MAX_FRAME_SIZE);
    defer parser.deinit();
    try std.testing.expectError(error.ProtocolError, parser.ingestOne("GET / HTTP/1.1\r\n"));
}

test "settings serialize" {
    var buf: [128]u8 = undefined;
    const n = try Serializer.settingsFrame(&buf, false, &serverPrefaceSettings);
    try std.testing.expect(n > FRAME_HEADER_LEN);
    const hdr = FrameHeader.decode(buf[0..FRAME_HEADER_LEN]);
    try std.testing.expectEqual(FrameType.settings, hdr.type);
    try std.testing.expectEqual(@as(u31, 0), hdr.stream_id);
}

test "fragmented frame ingest" {
    var parser = Parser.init(std.testing.allocator, DEFAULT_MAX_FRAME_SIZE);
    defer parser.deinit();
    parser.skipPreface();

    var frame_buf: [32]u8 = undefined;
    const opaque_data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const n = try Serializer.ping(&frame_buf, false, &opaque_data);

    // Feed one byte at a time
    var got: ?FrameEvent = null;
    for (0..n) |i| {
        const r = try parser.ingestOne(frame_buf[i .. i + 1]);
        if (r) |res| {
            got = res.event;
            break;
        }
    }
    try std.testing.expect(got != null);
    defer got.?.deinit(std.testing.allocator);
    try std.testing.expectEqual(.gpa, std.meta.activeTag(got.?.payload));
    try std.testing.expectEqual(FrameType.ping, got.?.header.type);
    try std.testing.expectEqualSlices(u8, &opaque_data, got.?.payload.bytes());
}

test "complete in-chunk frame borrows the input, not scratch" {
    var parser = try Parser.initReserved(std.testing.allocator, DEFAULT_MAX_FRAME_SIZE);
    defer parser.deinit();
    parser.skipPreface();

    var frame_buf: [32]u8 = undefined;
    const opaque_data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const n = try Serializer.ping(&frame_buf, false, &opaque_data);
    const r = try parser.ingestOne(frame_buf[0..n]);
    try std.testing.expect(r != null);
    try std.testing.expectEqual(.scratch, std.meta.activeTag(r.?.event.payload));
    try std.testing.expectEqual(frame_buf[FRAME_HEADER_LEN..].ptr, r.?.event.payload.bytes().ptr);
    try std.testing.expectEqualSlices(u8, &opaque_data, r.?.event.payload.bytes());
    r.?.event.deinit(std.testing.allocator);

    var empty_buf: [FRAME_HEADER_LEN]u8 = undefined;
    (FrameHeader{
        .length = 0,
        .type = .settings,
        .flags = .{ .end_stream = true },
        .stream_id = 0,
    }).encode(&empty_buf);
    const empty = try parser.ingestOne(&empty_buf);
    try std.testing.expectEqual(.empty, std.meta.activeTag(empty.?.event.payload));
    empty.?.event.deinit(std.testing.allocator);
}

test "two complete frames in one chunk are independent borrows" {
    var parser = try Parser.initReserved(std.testing.allocator, DEFAULT_MAX_FRAME_SIZE);
    defer parser.deinit();
    parser.skipPreface();

    var buf: [64]u8 = undefined;
    const a = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const b = [_]u8{ 8, 7, 6, 5, 4, 3, 2, 1 };
    const n1 = try Serializer.ping(buf[0..], false, &a);
    const n2 = try Serializer.ping(buf[n1..], false, &b);
    const wire = buf[0 .. n1 + n2];

    const r1 = (try parser.ingestOne(wire)) orelse return error.IncompleteFrame;
    try std.testing.expectEqual(wire[FRAME_HEADER_LEN..].ptr, r1.event.payload.bytes().ptr);
    try std.testing.expectEqualSlices(u8, &a, r1.event.payload.bytes());
    const rest = wire[r1.consumed..];
    const r2 = (try parser.ingestOne(rest)) orelse return error.IncompleteFrame;
    try std.testing.expectEqual(rest[FRAME_HEADER_LEN..].ptr, r2.event.payload.bytes().ptr);
    try std.testing.expectEqualSlices(u8, &b, r2.event.payload.bytes());
    try std.testing.expect(r1.event.payload.bytes().ptr != r2.event.payload.bytes().ptr);
}

test "gpa parser borrows a complete in-chunk frame and allocates only when split" {
    var parser = Parser.init(std.testing.allocator, DEFAULT_MAX_FRAME_SIZE);
    defer parser.deinit();
    parser.skipPreface();

    var frame_buf: [32]u8 = undefined;
    const opaque_data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const n = try Serializer.ping(&frame_buf, false, &opaque_data);
    const whole = (try parser.ingestOne(frame_buf[0..n])) orelse return error.IncompleteFrame;
    try std.testing.expectEqual(.scratch, std.meta.activeTag(whole.event.payload));
    try std.testing.expectEqual(frame_buf[FRAME_HEADER_LEN..].ptr, whole.event.payload.bytes().ptr);
    whole.event.deinit(std.testing.allocator);
}
