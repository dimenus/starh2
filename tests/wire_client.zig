//! A blocking HTTP/2 test client on a raw libc socket, h2c or TLS (the tls.zig
//! client). Shared by the gates that need to see the server's frames in wire
//! order: `slot_reuse.zig` and `outbound_memory.zig`.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const tls = @import("tls");

const frame = starh2.core.frame;
const h2_oracle = @import("h2_oracle.zig");

pub const client_deadline_ns = 10 * std.time.ns_per_s;

/// FIONREAD: std.c has no macOS entry for it. `_IOR('f', 127, int)` there.
const fionread = switch (@import("builtin").os.tag) {
    .linux => std.os.linux.T.FIONREAD,
    else => 0x4004667f,
};

pub const Transport = enum { h2c, tls };

pub fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

pub fn realNow() std.Io.Timestamp {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    return .fromNanoseconds(@as(i96, ts.sec) * std.time.ns_per_s + ts.nsec);
}

/// How many frames of each type a connection delivered, up to the point a
/// scenario stops reading. A reused slot must deliver the same as a fresh one.
pub const Counts = [16]u8;

/// Blocking client on a raw libc socket. For TLS, received ciphertext
/// accumulates in `in` and decrypted bytes in `plain`.
pub const Wire = struct {
    gpa: std.mem.Allocator,
    transport: Transport,
    fd: c_int,
    in: [2 * tls.input_buffer_len]u8 = undefined,
    in_len: usize = 0,
    out: [tls.output_buffer_len]u8 = undefined,
    rd: [tls.input_buffer_len]u8 = undefined,
    conn: tls.nonblock.Connection = undefined,
    plain: std.ArrayList(u8) = .empty,
    /// The payload of the frame `next` returned last.
    last_payload: std.ArrayList(u8) = .empty,
    /// What the server owes this client, checked on every frame and at the
    /// server's EOF (`h2_oracle`).
    oracle: h2_oracle.Oracle,
    /// TLS: the server's close_notify ended the reading.
    close_notify: bool = false,
    /// The read failed with a connection reset, not an EOF.
    reset: bool = false,

    pub const Seen = struct { ftype: u8, flags: u8, sid: u32 };

    /// TCP connect only. The kernel completes it while the server's accept
    /// loop is still parked, so a TLS handshake must wait for `handshake`.
    pub fn connect(gpa: std.mem.Allocator, transport: Transport, port: u16) !*Wire {
        return connectRecvBuf(gpa, transport, port, null);
    }

    /// `connect` with a receive buffer of `rcvbuf` bytes, set before the
    /// connection so the window it advertises is small: a client that stops
    /// reading then stalls the server after a few KiB.
    pub fn connectRecvBuf(gpa: std.mem.Allocator, transport: Transport, port: u16, rcvbuf: ?c_int) !*Wire {
        const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
        if (fd < 0) return error.Socket;
        errdefer _ = std.c.close(fd);
        if (rcvbuf) |n| _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVBUF, std.mem.asBytes(&n), @sizeOf(c_int));
        var addr: std.c.sockaddr.in = .{
            .port = std.mem.nativeToBig(u16, port),
            .addr = std.mem.nativeToBig(u32, 0x7f000001),
        };
        if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.Connect;
        const tv: std.c.timeval = .{ .sec = 1, .usec = 0 };
        _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVTIMEO, std.mem.asBytes(&tv), @sizeOf(std.c.timeval));
        const self = try gpa.create(Wire);
        self.* = .{ .gpa = gpa, .transport = transport, .fd = fd, .oracle = .init(gpa, transport == .tls) };
        return self;
    }

    /// `connectRecvBuf` over a Unix stream socket at `path` (a server's
    /// `h2c_unix` / `tls_unix` endpoint). The receive buffer is set before
    /// the connection; on Linux a Unix stream is limited by the SENDER's
    /// buffer, not this one, so a test must not count on it there.
    pub fn connectUnix(gpa: std.mem.Allocator, transport: Transport, path: []const u8, rcvbuf: ?c_int) !*Wire {
        const fd = std.c.socket(std.c.AF.UNIX, std.c.SOCK.STREAM, 0);
        if (fd < 0) return error.Socket;
        errdefer _ = std.c.close(fd);
        if (rcvbuf) |n| _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVBUF, std.mem.asBytes(&n), @sizeOf(c_int));
        var addr: std.c.sockaddr.un = .{ .path = @splat(0) };
        if (path.len >= addr.path.len) return error.NameTooLong;
        @memcpy(addr.path[0..path.len], path);
        if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.un)) != 0) return error.Connect;
        const tv: std.c.timeval = .{ .sec = 1, .usec = 0 };
        _ = std.c.setsockopt(fd, std.c.SOL.SOCKET, std.c.SO.RCVTIMEO, std.mem.asBytes(&tv), @sizeOf(std.c.timeval));
        const self = try gpa.create(Wire);
        self.* = .{ .gpa = gpa, .transport = transport, .fd = fd, .oracle = .init(gpa, transport == .tls) };
        return self;
    }

    /// Bytes the kernel holds for this client that it has not read yet
    /// (FIONREAD), read without consuming them.
    pub fn unreadBytes(self: *const Wire) usize {
        var n: c_int = 0;
        if (std.c.ioctl(self.fd, fionread, &n) != 0) return 0;
        return @intCast(n);
    }

    pub fn open(gpa: std.mem.Allocator, transport: Transport, port: u16) !*Wire {
        const self = try connect(gpa, transport, port);
        errdefer self.close();
        try self.handshake();
        return self;
    }

    pub fn close(self: *Wire) void {
        _ = std.c.close(self.fd);
        self.plain.deinit(self.gpa);
        self.last_payload.deinit(self.gpa);
        self.oracle.deinit();
        self.gpa.destroy(self);
    }

    pub fn handshake(self: *Wire) !void {
        return self.handshakeImpl(null, false);
    }

    /// Open the connection with `flight` (plaintext HTTP/2) already sent. On
    /// TLS the last client handshake flight, the Finished, leaves in the same
    /// write as the encrypted `flight`, the way curl and browsers pipeline it,
    /// so the server reads the flight as handshake leftover. On h2c it is
    /// the first write.
    pub fn handshakeWith(self: *Wire, flight: []const u8) !void {
        return self.handshakeImpl(flight, false);
    }

    /// TLS only: as `handshakeWith`, with this client's close_notify after
    /// the flight in the same write, and the socket left open. The server
    /// decrypts all of it as handshake leftover.
    pub fn handshakeWithThenCloseNotify(self: *Wire, flight: []const u8) !void {
        std.debug.assert(self.transport == .tls);
        return self.handshakeImpl(flight, true);
    }

    fn handshakeImpl(self: *Wire, flight: ?[]const u8, close_after: bool) !void {
        if (flight) |f| self.oracle.onSend(f);
        if (self.transport == .h2c) {
            if (flight) |f| try self.sendRaw(f);
            return;
        }
        var prng = std.Random.DefaultPrng.init(@truncate(@as(u128, @bitCast(@as(i128, realNow().nanoseconds)))));
        var hs = tls.nonblock.Client.init(.{
            .rng = prng.random(),
            .now = realNow(),
            .host = "localhost",
            .root_ca = .empty,
            .insecure_skip_verify = true,
            .cipher_suites = tls.config.cipher_suites.tls13,
            .alpn_protocols = &.{"h2"},
        });
        var last: std.ArrayList(u8) = .empty;
        defer last.deinit(self.gpa);
        while (!hs.done()) {
            const r = try hs.run(self.in[0..self.in_len], &self.out);
            self.consume(r.recv_pos);
            if (hs.done() and flight != null) {
                try last.appendSlice(self.gpa, r.send);
                break;
            }
            if (r.send.len > 0) try self.sendRaw(r.send);
            if (hs.done()) break;
            if (r.recv_pos == 0 and r.send.len == 0) try self.fill();
        }
        const alpn = hs.inner.alpn_protocol orelse return error.Alpn;
        if (!std.mem.eql(u8, alpn, "h2")) return error.Alpn;
        self.conn = .init(hs.cipher().?);
        if (flight) |f| {
            var off: usize = 0;
            while (off < f.len) {
                const r = try self.conn.encrypt(f[off..][0..@min(f.len - off, 16 * 1024)], &self.out);
                try last.appendSlice(self.gpa, r.ciphertext);
                off += r.cleartext_pos;
            }
            if (close_after) try last.appendSlice(self.gpa, try self.conn.close(&self.out));
            try self.sendRaw(last.items);
        }
    }

    pub fn fill(self: *Wire) !void {
        if (self.in_len == self.in.len) return error.ClientBufferFull;
        const n = std.c.recv(self.fd, self.in[self.in_len..].ptr, self.in.len - self.in_len, 0);
        if (n == 0) return error.Eof;
        if (n < 0) {
            return switch (std.c.errno(n)) {
                .AGAIN, .INTR => error.WouldBlock,
                .CONNRESET => {
                    self.reset = true;
                    return error.Eof;
                },
                else => error.Eof,
            };
        }
        self.in_len += @intCast(n);
    }

    pub fn consume(self: *Wire, n: usize) void {
        std.mem.copyForwards(u8, self.in[0 .. self.in_len - n], self.in[n..self.in_len]);
        self.in_len -= n;
    }

    pub fn sendRaw(self: *Wire, bytes: []const u8) !void {
        var off: usize = 0;
        while (off < bytes.len) {
            const n = std.c.send(self.fd, bytes[off..].ptr, bytes.len - off, 0);
            if (n <= 0) return error.Send;
            off += @intCast(n);
        }
    }

    pub fn send(self: *Wire, bytes: []const u8) !void {
        self.oracle.onSend(bytes);
        if (self.transport == .h2c) return self.sendRaw(bytes);
        var off: usize = 0;
        while (off < bytes.len) {
            const r = try self.conn.encrypt(bytes[off..][0..@min(bytes.len - off, 16 * 1024)], &self.out);
            try self.sendRaw(r.ciphertext);
            off += r.cleartext_pos;
        }
    }

    /// TLS only: send `frames`, then this client's close_notify, in ONE
    /// write, without closing the socket. `record_after`: one more complete
    /// record after the close_notify (its payload is not valid ciphertext; a
    /// peer must ignore anything after close_notify, RFC 8446 section 6.1).
    pub fn sendThenCloseNotify(self: *Wire, frames: []const u8, record_after: bool) !void {
        std.debug.assert(self.transport == .tls);
        self.oracle.onSend(frames);
        var flight: std.ArrayList(u8) = .empty;
        defer flight.deinit(self.gpa);
        var off: usize = 0;
        while (off < frames.len) {
            const r = try self.conn.encrypt(frames[off..][0..@min(frames.len - off, 16 * 1024)], &self.out);
            try flight.appendSlice(self.gpa, r.ciphertext);
            off += r.cleartext_pos;
        }
        try flight.appendSlice(self.gpa, try self.conn.close(&self.out));
        if (record_after) {
            const junk_len = 20;
            try flight.appendSlice(self.gpa, &.{ 0x17, 0x03, 0x03, 0x00, junk_len });
            try flight.appendNTimes(self.gpa, 0xa5, junk_len);
        }
        try self.sendRaw(flight.items);
    }

    pub fn recordLen(bytes: []const u8) ?usize {
        if (bytes.len < 5) return null;
        const len = 5 + @as(usize, std.mem.readInt(u16, bytes[3..5], .big));
        return if (bytes.len < len) null else len;
    }

    /// Read more plaintext. False when the server closed.
    pub fn pump(self: *Wire) !bool {
        if (self.transport == .h2c) {
            var buf: [16 * 1024]u8 = undefined;
            const n = std.c.recv(self.fd, &buf, buf.len, 0);
            if (n == 0) return false;
            if (n < 0) {
                return switch (std.c.errno(n)) {
                    .AGAIN, .INTR => error.WouldBlock,
                    .CONNRESET => {
                        self.reset = true;
                        return false;
                    },
                    else => error.Recv,
                };
            }
            try self.plain.appendSlice(self.gpa, buf[0..@intCast(n)]);
            return true;
        }
        const rec_len = recordLen(self.in[0..self.in_len]) orelse {
            self.fill() catch |err| switch (err) {
                error.Eof => return false,
                else => return err,
            };
            return true;
        };
        const res = try self.conn.decrypt(self.in[0..rec_len], &self.rd);
        self.consume(rec_len);
        if (res.closed) {
            self.close_notify = true;
            return false;
        }
        try self.plain.appendSlice(self.gpa, res.cleartext);
        return true;
    }

    /// The next frame in wire order, or null when the server closed first.
    pub fn next(self: *Wire, deadline: u64) !?Seen {
        while (true) {
            if (self.plain.items.len >= frame.FRAME_HEADER_LEN) {
                const h = self.plain.items;
                const len = (@as(usize, h[0]) << 16) | (@as(usize, h[1]) << 8) | h[2];
                if (h.len >= 9 + len) {
                    const seen: Seen = .{
                        .ftype = h[3],
                        .flags = h[4],
                        .sid = std.mem.readInt(u32, h[5..9], .big) & 0x7fff_ffff,
                    };
                    self.last_payload.clearRetainingCapacity();
                    try self.last_payload.appendSlice(self.gpa, h[9 .. 9 + len]);
                    self.oracle.onFrame(seen.ftype, seen.flags, seen.sid, self.last_payload.items);
                    self.plain.replaceRangeAssumeCapacity(0, 9 + len, &.{});
                    return seen;
                }
            }
            if (nowNs() >= deadline) return error.Deadline;
            const more = self.pump() catch |err| switch (err) {
                error.WouldBlock => continue,
                else => return err,
            };
            if (!more) {
                self.oracle.onEnd(if (self.reset) .reset else if (self.close_notify) .close_notify else .eof);
                return null;
            }
        }
    }

    pub const Stop = enum { stream1_end, stream1_data, closed };

    /// Read frames, counting each type, until `stop` holds. `closed` also
    /// returns when the server closes first. Returns whether it closed.
    pub fn readUntil(self: *Wire, counts: *Counts, stop: Stop) !bool {
        const deadline = nowNs() + client_deadline_ns;
        while (true) {
            const s = try self.next(deadline) orelse return true;
            if (s.ftype < counts.len) counts[s.ftype] += 1;
            switch (stop) {
                .stream1_end => if ((s.ftype == 0 or s.ftype == 1) and s.sid == 1 and s.flags & 1 != 0) return false,
                .stream1_data => if (s.ftype == 0 and s.sid == 1) return false,
                .closed => {},
            }
        }
    }
};

