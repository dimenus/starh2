//! Per-connection flight recorder for HTTP/2 over TLS (t-3514).
//!
//! It keeps the last `capacity` frame headers each side sent, plus TLS
//! events, and prints them only when the peer ends the connection before the
//! server did and without a GOAWAY(NO_ERROR). A load client that drops one
//! connection prints no reason of its own, so the last frames on the server
//! side are the cheapest evidence of what it reacted to.
//!
//! How to use: build with `-Dwire-ring=true`. Every peer close prints one
//! `RING close` line to stderr; an abnormal close also prints the ring as
//! `RING ent` lines, oldest first. Plain builds compile the field to `void`
//! (`tls.Conn.ring`), so they pay nothing.
//!
//! The taps sit at the cipher, so they see the plaintext in exact wire
//! order: `inbound` on every decrypted record, `outbound` on every encrypted
//! record. Frame headers are re-parsed from that byte stream, so the ring
//! does not trust the session's own idea of what it sent. Single owner: the
//! actor owns the cipher and therefore this ring; no atomics.
const std = @import("std");
const frame = @import("../core/frame.zig");

pub const enabled = @import("build_options").wire_ring;

pub const capacity = 96;

pub const Event = enum(u8) {
    /// One TLS record decrypted; `len` is its plaintext size.
    record_in,
    /// The peer's close_notify was decrypted.
    close_notify_in,
    /// recv returned 0 (TCP FIN).
    socket_eof,
    /// recv failed; the error name is in the close line.
    socket_error,
    /// tls.zig refused a record; the error name is in the close line.
    decrypt_error,
};

pub const Entry = struct {
    t_us: u64,
    dir: Dir,
    /// Frame type, or `Event` for `dir == .event`.
    typ: u8,
    flags: u8 = 0,
    stream: u32 = 0,
    len: u32 = 0,
    /// RST_STREAM / GOAWAY: the error code. WINDOW_UPDATE: the increment.
    /// SETTINGS: the first setting as id<<16 | low 16 bits of value.
    a: u32 = 0,
    /// GOAWAY: last stream id.
    b: u32 = 0,
};

pub const Dir = enum(u8) { in, out, event };

/// Re-parses frame headers out of one direction's plaintext byte stream.
pub const Tracker = struct {
    /// Inbound starts with the 24-byte client preface; outbound does not.
    preface_left: usize,
    hdr: [frame.FRAME_HEADER_LEN]u8 = undefined,
    hdr_have: usize = 0,
    payload_left: usize = 0,
    /// The first bytes of the current payload, for the codes above.
    cap: [8]u8 = undefined,
    cap_have: usize = 0,
    cur: Entry = undefined,
    frames: u64 = 0,
    bytes: u64 = 0,
    /// Highest odd stream id seen in a HEADERS frame.
    max_stream: u32 = 0,
    /// Count of frames that carried END_STREAM.
    end_streams: u64 = 0,
};

pub const Ring = struct {
    entries: [capacity]Entry = undefined,
    count: u64 = 0,
    t0_ns: u64 = 0,
    in: Tracker = .{ .preface_left = frame.CLIENT_PREFACE.len },
    out: Tracker = .{ .preface_left = 0 },
    /// GOAWAY error codes seen per direction, null if none was seen.
    goaway_in: ?u32 = null,
    goaway_out: ?u32 = null,
    /// GOAWAY debug data from the peer, truncated.
    debug_in: [64]u8 = undefined,
    debug_in_len: usize = 0,
    dumped: bool = false,

    pub fn start(self: *Ring) void {
        self.* = .{};
        self.t0_ns = nowNs();
    }

    pub fn inbound(self: *Ring, bytes: []const u8) void {
        self.push(.{ .t_us = self.age(), .dir = .event, .typ = @backingInt(Event.record_in), .len = @intCast(bytes.len) });
        self.feed(.in, bytes);
    }

    pub fn outbound(self: *Ring, bytes: []const u8) void {
        self.feed(.out, bytes);
    }

    pub fn event(self: *Ring, ev: Event) void {
        self.push(.{ .t_us = self.age(), .dir = .event, .typ = @backingInt(ev) });
    }

    /// The peer ended the connection. One summary line always; the whole
    /// ring only when the end was not a clean GOAWAY(NO_ERROR) from the peer
    /// and the server had not sent its own GOAWAY first.
    pub fn peerClosed(self: *Ring, conn_id: usize, reason: []const u8) void {
        if (self.dumped) return;
        self.dumped = true;
        const clean = (self.goaway_in != null and self.goaway_in.? == 0) or self.goaway_out != null;
        var buf: [768]u8 = undefined;
        var w: std.Io.Writer = .fixed(&buf);
        w.print("RING close conn={x} reason={s} age_ms={d} dump={} goaway_in={?d} goaway_out={?d} in_frames={d} in_bytes={d} in_max_stream={d} in_end_streams={d} out_frames={d} out_bytes={d} out_max_stream={d} out_end_streams={d} in_partial={d}/{d} out_partial={d}/{d}", .{
            conn_id,             reason,                self.age() / 1000, !clean,
            self.goaway_in,      self.goaway_out,       self.in.frames,    self.in.bytes,
            self.in.max_stream,  self.in.end_streams,   self.out.frames,   self.out.bytes,
            self.out.max_stream, self.out.end_streams,  self.in.hdr_have,  self.in.payload_left,
            self.out.hdr_have,   self.out.payload_left,
        }) catch {};
        if (self.debug_in_len > 0) w.print(" debug_in=\"{s}\"", .{self.debug_in[0..self.debug_in_len]}) catch {};
        w.writeAll("\n") catch {};
        writeStderr(w.buffered());
        if (clean) return;
        const n: u64 = @min(self.count, capacity);
        var i: u64 = self.count - n;
        while (i < self.count) : (i += 1) {
            const e = self.entries[@intCast(i % capacity)];
            var lb: [256]u8 = undefined;
            var lw: std.Io.Writer = .fixed(&lb);
            switch (e.dir) {
                .event => lw.print("RING ent conn={x} t_us={d} ! {s} len={d}\n", .{
                    conn_id, e.t_us, @tagName(@as(Event, @fromBackingInt(@intCast(e.typ)))), e.len,
                }) catch {},
                .in, .out => lw.print("RING ent conn={x} t_us={d} {s} {s} flags=0x{x:0>2} stream={d} len={d} a={d} b={d}\n", .{
                    conn_id,         e.t_us,  if (e.dir == .in) "<" else ">",
                    typeName(e.typ), e.flags, e.stream,
                    e.len,           e.a,     e.b,
                }) catch {},
            }
            writeStderr(lw.buffered());
        }
    }

    fn age(self: *const Ring) u64 {
        return (nowNs() -| self.t0_ns) / 1000;
    }

    fn push(self: *Ring, e: Entry) void {
        self.entries[@intCast(self.count % capacity)] = e;
        self.count += 1;
    }

    fn feed(self: *Ring, dir: Dir, bytes_in: []const u8) void {
        const t = if (dir == .in) &self.in else &self.out;
        t.bytes += bytes_in.len;
        var bytes = bytes_in;
        if (t.preface_left > 0) {
            const k = @min(t.preface_left, bytes.len);
            t.preface_left -= k;
            bytes = bytes[k..];
        }
        while (bytes.len > 0) {
            if (t.payload_left > 0) {
                const k = @min(t.payload_left, bytes.len);
                const want = @min(t.cap.len - t.cap_have, k);
                @memcpy(t.cap[t.cap_have..][0..want], bytes[0..want]);
                t.cap_have += want;
                if (dir == .in and t.cur.typ == @backingInt(frame.FrameType.goaway)) self.captureDebug(bytes[0..k], t);
                t.payload_left -= k;
                bytes = bytes[k..];
                if (t.payload_left == 0) self.finishFrame(dir, t);
                continue;
            }
            const k = @min(frame.FRAME_HEADER_LEN - t.hdr_have, bytes.len);
            @memcpy(t.hdr[t.hdr_have..][0..k], bytes[0..k]);
            t.hdr_have += k;
            bytes = bytes[k..];
            if (t.hdr_have < frame.FRAME_HEADER_LEN) break;
            t.hdr_have = 0;
            const len = std.mem.readInt(u24, t.hdr[0..3], .big);
            t.cur = .{
                .t_us = self.age(),
                .dir = dir,
                .typ = t.hdr[3],
                .flags = t.hdr[4],
                .stream = std.mem.readInt(u32, t.hdr[5..9], .big) & 0x7fff_ffff,
                .len = len,
            };
            t.cap_have = 0;
            t.payload_left = len;
            if (len == 0) self.finishFrame(dir, t);
        }
    }

    /// GOAWAY debug data starts after the 8 fixed payload bytes. `chunk` is
    /// the part of the payload in this feed; `t.payload_left` does not yet
    /// count it.
    fn captureDebug(self: *Ring, chunk: []const u8, t: *const Tracker) void {
        const consumed_before = t.cur.len - t.payload_left;
        var i: usize = 0;
        while (i < chunk.len) : (i += 1) {
            if (consumed_before + i < 8) continue;
            if (self.debug_in_len == self.debug_in.len) return;
            const c = chunk[i];
            self.debug_in[self.debug_in_len] = if (c >= 0x20 and c < 0x7f and c != '"') c else '.';
            self.debug_in_len += 1;
        }
    }

    fn finishFrame(self: *Ring, dir: Dir, t: *Tracker) void {
        var e = t.cur;
        const cap = t.cap[0..t.cap_have];
        switch (@as(frame.FrameType, @fromBackingInt(@intCast(e.typ)))) {
            .rst_stream => if (cap.len >= 4) {
                e.a = std.mem.readInt(u32, cap[0..4], .big);
            },
            .window_update => if (cap.len >= 4) {
                e.a = std.mem.readInt(u32, cap[0..4], .big) & 0x7fff_ffff;
            },
            .settings => if (cap.len >= 6) {
                e.a = @as(u32, std.mem.readInt(u16, cap[0..2], .big)) << 16 | (std.mem.readInt(u32, cap[2..6], .big) & 0xffff);
            },
            .goaway => if (cap.len >= 8) {
                e.b = std.mem.readInt(u32, cap[0..4], .big) & 0x7fff_ffff;
                e.a = std.mem.readInt(u32, cap[4..8], .big);
                if (dir == .in) self.goaway_in = e.a else self.goaway_out = e.a;
            },
            .headers => if (e.stream > t.max_stream) {
                t.max_stream = e.stream;
            },
            else => {},
        }
        const end_stream_bit = e.flags & 0x1 != 0;
        if (end_stream_bit and (e.typ == @backingInt(frame.FrameType.data) or e.typ == @backingInt(frame.FrameType.headers))) t.end_streams += 1;
        t.frames += 1;
        self.push(e);
    }
};

fn typeName(t: u8) []const u8 {
    return switch (@as(frame.FrameType, @fromBackingInt(@intCast(t)))) {
        .data => "DATA",
        .headers => "HEADERS",
        .priority => "PRIORITY",
        .rst_stream => "RST_STREAM",
        .settings => "SETTINGS",
        .push_promise => "PUSH_PROMISE",
        .ping => "PING",
        .goaway => "GOAWAY",
        .window_update => "WINDOW_UPDATE",
        .continuation => "CONTINUATION",
        _ => "UNKNOWN",
    };
}

fn nowNs() u64 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) *% 1_000_000_000 +% @as(u64, @intCast(ts.nsec));
}

fn writeStderr(bytes: []const u8) void {
    _ = std.c.write(2, bytes.ptr, bytes.len);
}

test "tracker re-parses frames split at every byte boundary" {
    var r: Ring = .{};
    r.start();
    var wire: [128]u8 = undefined;
    var n: usize = 0;
    @memcpy(wire[0..frame.CLIENT_PREFACE.len], frame.CLIENT_PREFACE);
    n += frame.CLIENT_PREFACE.len;
    // HEADERS stream 3, END_STREAM|END_HEADERS, 2 payload bytes.
    const h = [_]u8{ 0, 0, 2, 0x1, 0x5, 0, 0, 0, 3, 0xaa, 0xbb };
    @memcpy(wire[n..][0..h.len], &h);
    n += h.len;
    // GOAWAY last=3 code=FLOW_CONTROL_ERROR, debug "hi".
    const g = [_]u8{ 0, 0, 10, 0x7, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 3, 'h', 'i' };
    @memcpy(wire[n..][0..g.len], &g);
    n += g.len;
    for (wire[0..n]) |b| r.feed(.in, &.{b});
    try std.testing.expectEqual(@as(u64, 2), r.in.frames);
    try std.testing.expectEqual(@as(u32, 3), r.in.max_stream);
    try std.testing.expectEqual(@as(u64, 1), r.in.end_streams);
    try std.testing.expectEqual(@as(?u32, 3), r.goaway_in);
    try std.testing.expectEqualStrings("hi", r.debug_in[0..r.debug_in_len]);
    const last = r.entries[@intCast((r.count - 1) % capacity)];
    try std.testing.expectEqual(@as(u32, 3), last.b);
}
