//! A peer-side check of what RFC 9113 owes the client, run on every
//! connection the shared test client (`wire_client.Wire`) opens. It shares no
//! code with the server: it sees only the frames on the wire, the client's
//! own and the server's.
//!
//! The class it targets is an obligation dropped on a rare path: a stream
//! the client opened that never ends, a PING or SETTINGS that is never
//! acked, a close with no GOAWAY, a TLS close with no close_notify. Each such
//! defect used to need a test written for it; this checks all of them on
//! every connection of every test that uses the client.
//!
//! Checked as frames arrive:
//! - no HEADERS or DATA on a stream the client never opened, or after the
//!   server ended it (END_STREAM or RST_STREAM);
//! - at most one RST_STREAM per stream, and none for a stream the client
//!   never opened;
//! - a PING ack carries a payload the client sent, a SETTINGS ack answers a
//!   SETTINGS the client sent.
//! Checked when the server closes (the client read its EOF):
//! - a GOAWAY came first, and the server did not reset the connection;
//! - on TLS, close_notify came first;
//! - after a final GOAWAY with NO_ERROR (a graceful end): every stream the
//!   client opened at or below its last stream id has ended, and every PING
//!   and SETTINGS the client sent before it read a GOAWAY was acked. After a
//!   GOAWAY with an error code the connection error ends the streams, and
//!   frames the server had not read yet are dropped, so neither is owed.
//!
//! The end checks apply once the server sent a frame (a connection refused
//! before the server's preface never was HTTP/2), and not to a connection
//! whose test declares that its client stops reading (`waive`).
//!
//! A violation is printed (`H2ORACLE`) and counted in `violations`. Each test
//! binary that uses the client ends with a test that fails when the count is
//! not zero (`expectNoViolations`), so no test has to call the oracle itself.
const std = @import("std");

pub var violations: std.atomic.Value(usize) = .init(0);

/// The last test of a binary that uses the client.
pub fn expectNoViolations() !void {
    const n = violations.load(.acquire);
    if (n != 0) {
        std.debug.print("H2ORACLE: {d} violation(s) in this binary; each is printed above with H2ORACLE\n", .{n});
        return error.ProtocolOracle;
    }
}

const preface_len = 24;
const frame_header_len = 9;

const StreamView = struct {
    client_end: bool = false,
    client_rst: bool = false,
    server_end: bool = false,
    server_rsts: u8 = 0,
};

const Pending = struct {
    payload: [8]u8 = @splat(0),
    /// Sent before the client read any GOAWAY: then it is owed.
    before_goaway: bool,
};

pub const EndOfStream = enum { eof, reset, close_notify };

pub const Oracle = struct {
    gpa: std.mem.Allocator,
    tls: bool,
    /// Bytes of an outbound frame not complete yet (a client may write a
    /// frame in pieces), after the client preface.
    out: std.ArrayList(u8) = .empty,
    preface_left: usize = preface_len,
    streams: std.AutoHashMapUnmanaged(u31, StreamView) = .empty,
    pings: std.ArrayList(Pending) = .empty,
    settings: std.ArrayList(Pending) = .empty,
    goaway_seen: bool = false,
    goaway_code: u32 = 0,
    goaway_last: u31 = 0,
    ended: bool = false,
    /// The server sent at least one frame: HTTP/2 started. A connection the
    /// server refuses before its preface (no slot, for example) owes nothing
    /// at its end: it never was an HTTP/2 connection.
    any_frame: bool = false,
    /// Set by a test whose client stops reading on purpose, so delivery at
    /// the end cannot be owed (`waive`); the reason is printed.
    waived: ?[]const u8 = null,

    pub fn init(gpa: std.mem.Allocator, tls: bool) Oracle {
        return .{ .gpa = gpa, .tls = tls };
    }

    pub fn deinit(self: *Oracle) void {
        self.out.deinit(self.gpa);
        self.streams.deinit(self.gpa);
        self.pings.deinit(self.gpa);
        self.settings.deinit(self.gpa);
    }

    fn violation(self: *Oracle, comptime fmt: []const u8, args: anytype) void {
        _ = self;
        _ = violations.fetchAdd(1, .acq_rel);
        std.debug.print("H2ORACLE: " ++ fmt ++ "\n", args);
    }

    /// Plaintext the client is about to send.
    pub fn onSend(self: *Oracle, bytes: []const u8) void {
        var rest = bytes;
        if (self.preface_left > 0) {
            const skip = @min(self.preface_left, rest.len);
            self.preface_left -= skip;
            rest = rest[skip..];
        }
        self.out.appendSlice(self.gpa, rest) catch @panic("oracle: out of memory");
        while (self.out.items.len >= frame_header_len) {
            const h = self.out.items;
            const len = (@as(usize, h[0]) << 16) | (@as(usize, h[1]) << 8) | h[2];
            if (h.len < frame_header_len + len) break;
            self.outbound(h[3], h[4], @intCast(std.mem.readInt(u32, h[5..9], .big) & 0x7fff_ffff), h[frame_header_len..][0..len]);
            self.out.replaceRangeAssumeCapacity(0, frame_header_len + len, &.{});
        }
    }

    fn outbound(self: *Oracle, ftype: u8, flags: u8, sid: u31, payload: []const u8) void {
        switch (ftype) {
            0 => if (flags & 1 != 0) {
                if (self.streams.getPtr(sid)) |s| s.client_end = true;
            },
            1 => {
                const e = self.streams.getOrPut(self.gpa, sid) catch @panic("oracle: out of memory");
                if (!e.found_existing) e.value_ptr.* = .{};
                if (flags & 1 != 0) e.value_ptr.client_end = true;
            },
            3 => if (self.streams.getPtr(sid)) |s| {
                s.client_rst = true;
            },
            4 => if (flags & 1 == 0) {
                self.settings.append(self.gpa, .{ .before_goaway = !self.goaway_seen }) catch @panic("oracle: out of memory");
            },
            6 => if (flags & 1 == 0 and payload.len == 8) {
                self.pings.append(self.gpa, .{ .payload = payload[0..8].*, .before_goaway = !self.goaway_seen }) catch @panic("oracle: out of memory");
            },
            else => {},
        }
    }

    /// A frame the client read.
    pub fn onFrame(self: *Oracle, ftype: u8, flags: u8, sid: u32, payload: []const u8) void {
        self.any_frame = true;
        const id: u31 = @intCast(sid & 0x7fff_ffff);
        switch (ftype) {
            0, 1 => {
                const s = self.streams.getPtr(id) orelse {
                    self.violation("{s} on stream {d}, which the client never opened", .{ if (ftype == 0) "DATA" else "HEADERS", id });
                    return;
                };
                if (s.server_end or s.server_rsts != 0) self.violation("{s} on stream {d} after the server ended it", .{ if (ftype == 0) "DATA" else "HEADERS", id });
                if (flags & 1 != 0) s.server_end = true;
            },
            3 => {
                const s = self.streams.getPtr(id) orelse {
                    self.violation("RST_STREAM for stream {d}, which the client never opened", .{id});
                    return;
                };
                s.server_rsts +|= 1;
                if (s.server_rsts == 2) self.violation("a second RST_STREAM for stream {d}", .{id});
            },
            4 => if (flags & 1 != 0) {
                if (self.settings.items.len == 0) {
                    self.violation("a SETTINGS ack with no SETTINGS waiting for one", .{});
                } else {
                    _ = self.settings.orderedRemove(0);
                }
            },
            6 => if (flags & 1 != 0 and payload.len == 8) {
                for (self.pings.items, 0..) |p, i| {
                    if (std.mem.eql(u8, &p.payload, payload[0..8])) {
                        _ = self.pings.orderedRemove(i);
                        break;
                    }
                } else self.violation("a PING ack with a payload the client never sent", .{});
            },
            7 => if (payload.len >= 8) {
                self.goaway_seen = true;
                self.goaway_last = @intCast(std.mem.readInt(u32, payload[0..4], .big) & 0x7fff_ffff);
                self.goaway_code = std.mem.readInt(u32, payload[4..8], .big);
            },
            else => {},
        }
    }

    /// The checks at the end do not apply to this connection: its client
    /// stops reading on purpose, so the server can deliver nothing more and
    /// closes at its deadline. Say why; the checks as frames arrive still run.
    pub fn waive(self: *Oracle, reason: []const u8) void {
        self.waived = reason;
    }

    /// The server closed: what the client read ended here.
    pub fn onEnd(self: *Oracle, how: EndOfStream) void {
        if (self.ended) return;
        self.ended = true;
        if (self.waived) |why| {
            std.debug.print("H2ORACLE-waived: end checks skipped: {s}\n", .{why});
            return;
        }
        if (!self.any_frame) return;
        if (how == .reset) self.violation("the server reset the connection (the client may have lost unread frames)", .{});
        if (!self.goaway_seen) {
            self.violation("the server closed without a GOAWAY", .{});
            return;
        }
        if (self.tls and how != .close_notify) self.violation("the TLS connection closed without close_notify", .{});
        // An error GOAWAY ends every stream and drops what was not read.
        if (self.goaway_code != 0) return;
        var it = self.streams.iterator();
        while (it.next()) |e| {
            const s = e.value_ptr.*;
            if (e.key_ptr.* > self.goaway_last or s.client_rst) continue;
            if (!s.server_end and s.server_rsts == 0) self.violation("stream {d} never ended after a graceful GOAWAY (last stream {d})", .{ e.key_ptr.*, self.goaway_last });
        }
        for (self.pings.items) |p| {
            if (p.before_goaway) self.violation("a PING ({any}) sent before the GOAWAY was never acked", .{&p.payload});
        }
        for (self.settings.items) |p| {
            if (p.before_goaway) self.violation("a SETTINGS sent before the GOAWAY was never acked", .{});
        }
    }
};
