//! TLS through tls.zig's non-blocking API. HTTP/2 never sees ciphertext.
//!
//! The cipher state (`tls.nonblock.Connection`) has exactly one owner: the
//! actor task, which drives `Pump` methods on its own stack. Encrypt and
//! decrypt are plain function calls over byte buffers this file owns. There
//! is no BIO, and `decryptInto` never lets tls.zig hold plaintext between
//! calls, so every unread byte is in `Conn.in_buf`, where the park predicate
//! (`pendingInbound`) sees it.
//!
//! The pump is a `zio.CompletionQueue` driver owned by the actor. The socket
//! read is a raw `ev.NetRecv` completion the actor submits and re-submits
//! after each arrival. Outbound frames stash on `queueWire` and are encrypted
//! on `driveTlsTurn` via `writeChunks` (not on the `drainEmit` stack: that
//! overflowed the coroutine). Records are encrypted straight into the send
//! staging buffer, which arms `ev.NetSend`. Acks apply locally in `post`.
//! The actor's idle wait is one `select` that includes `.io = &cq` and
//! loses `.reads` and `.acks`. There is no wake Event, no dirty flag, no
//! reset-then-recheck list: the lost-wake class of the old protocol is
//! unsayable here, because nothing is ever reset.
//!
//! A peer that stops reading must not stop the actor: `pending_n > 0`
//! pauses FairScheduler DATA drain only, so WINDOW_UPDATE still emits into
//! the stash. A full stash (`carried` plus `write_ch`) pauses controls too:
//! that is the 200-HEADERS burst bound, and it is not the same as pausing
//! every tier on `pending_n` (that blew stall p99 to 11 ms). The actor's
//! `waitForActivity` still serves handler completions, deadlines, the
//! doorbell and the slow-consumer kill. It parks on the CQ, never the socket.
//!
//! The server speaks TLS 1.3 only (tls.zig has no TLS 1.2 server), offers no
//! session resumption, and negotiates x25519, P-256 or P-384.
//!
//! This file takes a DIRECT zio dependency (the CQ, the channel, the raw
//! completion). That is deliberate and open: an interface over the CQ would
//! be two implementations of one contract. The std.Io purity of src/edge
//! ends here; the h2c pumps remain std.Io-pure.
const std = @import("std");
const tls = @import("tls");
const zio = @import("zio");
const limits_mod = @import("../core/wire_const.zig");
const io_queue = @import("io_queue.zig");
const wire_pump = @import("wire_pump.zig");

/// Same gate as `connection.test_observe`: Debug (the suite) and
/// `-Dobserve=true` ReleaseFast. Plain ReleaseFast compiles the increments
/// and the `STARH2_PUMPTRACE` print path out.
pub const observe = @import("build_options").observe;

/// Quiet-turn counters for TlsPump. Process-global like `test_observed_*`
/// so `/trace` can read them without a pump pointer. One TLS connection is
/// the bench shape; overlapping pumps would share the totals.
///
/// `select` / `select_write` / `select_peek` stay in the schema so a
/// build that still Selects is visible: they must collapse to
/// zero. Bump sites for those three are gone. `dirty_skip_wait` joins them
/// with the CQ driver: the dirty-flag protocol is deleted, so a non-zero
/// value would mean the old wake machinery came back.
pub const pump_trace = struct {
    pub var turns: std.atomic.Value(u64) = .init(0);
    pub var select: std.atomic.Value(u64) = .init(0);
    pub var select_write: std.atomic.Value(u64) = .init(0);
    pub var select_peek: std.atomic.Value(u64) = .init(0);
    pub var tryget_write: std.atomic.Value(u64) = .init(0);
    /// Send completions the driver handled (one per armed send).
    pub var send_complete: std.atomic.Value(u64) = .init(0);
    pub var read_one: std.atomic.Value(u64) = .init(0);
    pub var want_read: std.atomic.Value(u64) = .init(0);
    pub var read_free_empty_yield: std.atomic.Value(u64) = .init(0);
    pub var live_handler_yield: std.atomic.Value(u64) = .init(0);
    pub var pending_read_retry: std.atomic.Value(u64) = .init(0);
    pub var write_chunks: std.atomic.Value(u64) = .init(0);
    pub var write_chunk_sum: std.atomic.Value(u64) = .init(0);
    pub var work_get: std.atomic.Value(u64) = .init(0);
    pub var cipher_chunks: std.atomic.Value(u64) = .init(0);
    pub var dirty_skip_wait: std.atomic.Value(u64) = .init(0);
    /// Stale CQ wakes absorbed at the select (see the .io arm comment).
    pub var cq_spurious_wake: std.atomic.Value(u64) = .init(0);

    inline fn bump(counter: *std.atomic.Value(u64)) void {
        if (comptime observe) _ = counter.fetchAdd(1, .monotonic);
    }

    inline fn add(counter: *std.atomic.Value(u64), n: u64) void {
        if (comptime observe) _ = counter.fetchAdd(n, .monotonic);
    }

    /// Extra `/trace` fields. The `STARH2_PUMPTRACE` token is the strings
    /// canary: present in `-Dobserve=true`, absent from plain ReleaseFast.
    pub fn writeJson(w: *std.Io.Writer) !void {
        if (comptime !observe) return;
        try w.print(
            ",\"STARH2_PUMPTRACE\":1," ++
                "\"pump_turns\":{d},\"pump_select\":{d}," ++
                "\"pump_select_write\":{d},\"pump_select_peek\":{d}," ++
                "\"pump_tryget_write\":{d},\"pump_read_one\":{d}," ++
                "\"pump_want_read\":{d},\"pump_read_free_yield\":{d}," ++
                "\"pump_live_handler_yield\":{d},\"pump_pending_read_retry\":{d}," ++
                "\"pump_write_chunks\":{d},\"pump_write_chunk_sum\":{d}," ++
                "\"pump_work_get\":{d},\"pump_cipher_chunks\":{d}," ++
                "\"pump_dirty_skip_wait\":{d}," ++
                "\"pump_cq_spurious_wake\":{d}",
            .{
                turns.load(.acquire),
                select.load(.acquire),
                select_write.load(.acquire),
                select_peek.load(.acquire),
                tryget_write.load(.acquire),
                read_one.load(.acquire),
                want_read.load(.acquire),
                read_free_empty_yield.load(.acquire),
                live_handler_yield.load(.acquire),
                pending_read_retry.load(.acquire),
                write_chunks.load(.acquire),
                write_chunk_sum.load(.acquire),
                work_get.load(.acquire),
                cipher_chunks.load(.acquire),
                dirty_skip_wait.load(.acquire),
                cq_spurious_wake.load(.acquire),
            },
        );
    }
};

/// Always-on TLS write-fail counters for `/trace`. Not gated on `observe` or
/// `trace.enabled`: a ReleaseFast bench server must name a silent fail-close.
pub var tls_write_overflow: std.atomic.Value(u64) = .init(0);
pub var tls_stage_failed: std.atomic.Value(u64) = .init(0);

pub fn writeFailJson(w: *std.Io.Writer) !void {
    try w.print(
        ",\"tls_write_overflow\":{d},\"tls_stage_failed\":{d}",
        .{
            tls_write_overflow.load(.acquire),
            tls_stage_failed.load(.acquire),
        },
    );
}

/// Bench `--diag`: print one line when the pump parks. Off unless the
/// benchmark server sets it. Independent of `observe` so a ReleaseFast
/// capture binary can dump queue occupancy without `-Dobserve=true`.
pub var diag_wait: bool = false;
var diag_last_wait_ns: u64 = 0;

/// Same raw timestamped fd-2 write as `connection.diagRawPrint`; kept local
/// because tls.zig does not import connection.zig. File order is time order
/// only for lines written this way.
fn rawPrint(comptime fmt: []const u8, args: anytype) void {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const ms: u64 = @as(u64, @intCast(ts.sec)) *% 1000 +% @as(u64, @intCast(ts.nsec)) / 1_000_000;
    var buf: [512]u8 = undefined;
    const line = std.fmt.bufPrint(&buf, "[{d}] " ++ fmt, .{ms % 1_000_000} ++ args) catch return;
    _ = std.c.write(2, line.ptr, line.len);
}

pub const alpn_h2 = "h2";
pub const alpn_http11 = "http/1.1";
pub const stream_buffer_size: usize = limits_mod.TLS_STREAM_BUFFER_SIZE;
pub const cipher_chunk_size: usize = limits_mod.TLS_CIPHER_CHUNK_SIZE;
/// One maximum TLS 1.3 record on the wire. Both `Conn` buffers are this
/// size: a full inbound buffer then always holds a complete record, and an
/// empty outbound buffer always takes a full-size record.
pub const record_buffer_size: usize = limits_mod.TLS_RECORD_BUFFER_SIZE;
/// TLS caps a record's plaintext at 2^14 bytes.
pub const max_record_plaintext: usize = 16 * 1024;
pub const MaxHandshakeIterations: u32 = 4096;

comptime {
    std.debug.assert(stream_buffer_size >= 16 * 1024);
    std.debug.assert(record_buffer_size == tls.input_buffer_len);
    std.debug.assert(record_buffer_size >= tls.output_buffer_len);
    std.debug.assert(conn_buffer_bytes == limits_mod.TLS_CONN_BUFFER_BYTES);
    std.debug.assert(cipher_chunk_size == stream_buffer_size);
    // The pump decrypts into one wire chunk, which must take a whole
    // record payload or decrypt cannot make progress (see `decryptInto`).
    std.debug.assert(limits_mod.WIRE_CHUNK_SIZE >= record_buffer_size - 5);
}

/// `Conn.in_buf` + `Conn.out_buf`.
pub const conn_buffer_bytes: usize = record_buffer_size * 2;

/// Server preference order: `h2`, then `http/1.1`. A client that sends no
/// ALPN gets no selection and is served HTTP/1.1 (`isHttp11Alpn(null)`). A
/// client that offers only other protocols fails the handshake
/// (no_application_protocol).
const server_alpn = [_][]const u8{ alpn_h2, alpn_http11 };
const max_alpn_len = 32;

pub fn isHttp2Alpn(selected: ?[]const u8) bool {
    const proto = selected orelse return false;
    return std.mem.eql(u8, proto, alpn_h2);
}

pub fn isHttp11Alpn(selected: ?[]const u8) bool {
    const proto = selected orelse return true;
    return std.mem.eql(u8, proto, alpn_http11);
}

/// Server-wide certificate and key. Borrowed by every TLS connection.
pub const Acceptor = struct {
    auth: tls.config.CertKeyPair,
    gpa: std.mem.Allocator,

    /// The chain is sent in PEM order, so the leaf must come first. tls.zig
    /// drops certificates that are not valid now, so an expired-only chain
    /// is an error here rather than a handshake that sends no certificate.
    pub fn initFromPem(gpa: std.mem.Allocator, io: std.Io, certificate_chain_pem: []const u8, private_key_pem: []const u8) !Acceptor {
        var auth = tls.config.CertKeyPair.fromSlice(gpa, io, certificate_chain_pem, private_key_pem) catch
            return error.InvalidCertificate;
        errdefer auth.deinit(gpa);
        if (auth.bundle.bytes.items.len == 0) return error.InvalidCertificate;
        return .{ .auth = auth, .gpa = gpa };
    }

    pub fn deinit(self: *Acceptor) void {
        self.auth.deinit(self.gpa);
    }
};

/// Loopback client settings for tests and the bench server's
/// `--self-drive-oneshots`: no certificate verification, `alpn_protocols`
/// offered in order. Do not use this against a real peer.
pub const ClientConnector = struct {
    alpn_protocols: []const []const u8,
};

pub fn loopbackClientConnector() ClientConnector {
    return .{ .alpn_protocols = &.{alpn_h2} };
}

/// Loopback HTTP/1.1 client.
pub fn loopbackH1ClientConnector() ClientConnector {
    return .{ .alpn_protocols = &.{alpn_http11} };
}

/// Client offers `h2` then `http/1.1`. The server must select `h2`.
pub fn loopbackBothAlpnClientConnector() ClientConnector {
    return .{ .alpn_protocols = &.{ alpn_h2, alpn_http11 } };
}

/// Client sends no ALPN. The server must select `http/1.1`.
pub fn loopbackNoAlpnClientConnector() ClientConnector {
    return .{ .alpn_protocols = &.{} };
}

/// Byte length of the first TLS record in `bytes` when all of it is
/// present, else null. The record header is 5 bytes: type, version, and a
/// big-endian u16 payload length.
fn completeRecordLen(bytes: []const u8) ?usize {
    if (bytes.len < 5) return null;
    const len = 5 + @as(usize, std.mem.readInt(u16, bytes[3..5], .big));
    if (bytes.len < len) return null;
    return len;
}

/// Per-connection TLS state. Heap-allocated and never moved.
///
/// Production: the server handshake reads the socket on the connection's
/// own task (`feedFromSocket`); after it, `Pump` (the CQ driver) is the sole
/// socket reader (via its `ev.NetRecv` completion), the sole owner of
/// `cipher`, and the sole socket writer. The client loopback path uses both
/// directions on one task.
///
/// Inbound ciphertext accumulates in `in_buf[in_head..in_fill]` and is
/// decrypted from there. Outbound records are encrypted into `out_buf`:
/// handshake flights first, then the Pump's send staging.
///
/// Bind reader/writer on the task that will park on them: a Reader built on
/// one task is not another task's wait context.
pub const Conn = struct {
    tcp_stream: std.Io.net.Stream = undefined,
    tcp_reader: std.Io.net.Stream.Reader = undefined,
    tcp_writer: std.Io.net.Stream.Writer = undefined,
    out_buf: [record_buffer_size]u8 = undefined,
    in_buf: [record_buffer_size]u8 = undefined,
    in_head: usize = 0,
    in_fill: usize = 0,
    /// Client loopback only: plaintext `readPlain` has not returned yet.
    /// The client decrypts in place, so it sits inside `in_buf`, before
    /// `in_head`. The server path never sets these.
    plain_head: usize = 0,
    plain_end: usize = 0,
    cipher: tls.nonblock.Connection = undefined,
    /// Set by `setupAccept`: the handshake signs with its key.
    acceptor: ?*Acceptor = null,
    alpn_buf: [max_alpn_len]u8 = undefined,
    alpn_len: ?usize = null,
    /// The peer sent close_notify.
    peer_closed: bool = false,
    state: enum { empty, tcp, accepting, open } = .empty,

    pub fn initTcp(self: *Conn, stream: std.Io.net.Stream) void {
        self.* = .{};
        self.tcp_stream = stream;
        self.state = .tcp;
    }

    /// Unbuffered on both sides. After the handshake the CQ driver reads the
    /// raw socket, so a buffered reader could strand prefetched ciphertext
    /// in a buffer nothing drains again; every read lands in `in_buf`.
    pub fn bindIo(self: *Conn, io: std.Io) void {
        self.tcp_reader = self.tcp_stream.reader(io, &.{});
        self.tcp_writer = self.tcp_stream.writer(io, &.{});
    }

    pub fn setupAccept(self: *Conn, acceptor: *Acceptor) void {
        std.debug.assert(self.state == .tcp);
        self.acceptor = acceptor;
        self.state = .accepting;
    }

    /// The protocol ALPN selected, or null when the client sent no ALPN.
    pub fn selectedAlpn(self: *const Conn) ?[]const u8 {
        const n = self.alpn_len orelse return null;
        return self.alpn_buf[0..n];
    }

    fn setAlpn(self: *Conn, proto: ?[]const u8) error{TlsHandshakeFailed}!void {
        const p = proto orelse {
            self.alpn_len = null;
            return;
        };
        if (p.len > self.alpn_buf.len) return error.TlsHandshakeFailed;
        @memcpy(self.alpn_buf[0..p.len], p);
        self.alpn_len = p.len;
    }

    /// Server handshake, on the connection's own task. It reads the socket
    /// directly (`feedFromSocket`); the CQ driver does not exist yet.
    /// Ciphertext beyond the handshake (a pipelined preface) stays in
    /// `in_buf` and is decrypted by `drainLeftoverPlain` or the pump.
    ///
    /// Returns `error.Canceled` when a socket wait was canceled, so a caller
    /// under `zio.withTimeout` gets `error.Timeout` for its own deadline.
    /// Every other failure is `error.TlsHandshakeFailed`.
    pub fn handshake(self: *Conn, io: std.Io) error{ Canceled, TlsHandshakeFailed }!void {
        std.debug.assert(self.state == .accepting);
        self.bindIo(io);
        // TLS 1.3 only: the cipher does not keep the RNG after the
        // handshake, so it may live on this frame.
        var rng: std.Random.IoSource = .{ .io = io };
        var hs = tls.nonblock.Server.init(.{
            .rng = rng.interface(),
            .auth = &self.acceptor.?.auth,
            .alpn_protocols = &server_alpn,
            .now = std.Io.Clock.real.now(io),
        });
        var iterations: u32 = 0;
        while (!hs.done()) {
            iterations += 1;
            if (iterations > MaxHandshakeIterations) return error.TlsHandshakeFailed;
            const res = hs.run(self.in_buf[self.in_head..self.in_fill], &self.out_buf) catch
                return error.TlsHandshakeFailed;
            self.in_head += res.recv_pos;
            if (res.send.len > 0) {
                self.tcp_writer.interface.writeAll(res.send) catch return self.handshakeIoError();
            }
            if (hs.done()) break;
            if (res.recv_pos == 0 and res.send.len == 0) {
                self.feedFromSocket() catch return self.handshakeIoError();
            }
        }
        try self.setAlpn(hs.alpnProtocol());
        self.cipher = .init(hs.cipher().?);
        self.state = .open;
    }

    /// The std.Io stream reader and writer report every socket error as one
    /// generic failure and keep the cause in `err`. Read it back, because a
    /// canceled wait must stay `error.Canceled` for `zio.withTimeout` to
    /// tell its own deadline from a peer that broke the handshake.
    fn handshakeIoError(self: *Conn) error{ Canceled, TlsHandshakeFailed } {
        if (self.tcp_reader.err) |e| {
            if (e == error.Canceled) return error.Canceled;
        }
        if (self.tcp_writer.err) |e| {
            if (e == error.Canceled) return error.Canceled;
        }
        return error.TlsHandshakeFailed;
    }

    /// Drain leftover plaintext after handshake (a pipelined request). Stops
    /// when no complete record is buffered or `buf` cannot take the next
    /// record; anything left stays in `in_buf` for the pump.
    pub fn drainLeftoverPlain(self: *Conn, buf: []u8) usize {
        var n: usize = 0;
        while (n < buf.len) {
            const got = self.decryptInto(buf[n..]) catch break;
            if (got == 0) break;
            n += got;
        }
        return n;
    }

    /// Single-task client handshake: this task is the ciphertext source.
    pub fn handshakeClient(self: *Conn, connector: *const ClientConnector, io: std.Io) !void {
        try self.handshakeClientAny(connector, io);
        if (!isHttp2Alpn(self.selectedAlpn())) return error.TlsHandshakeFailed;
    }

    /// Single-task HTTP/1.1 client handshake.
    pub fn handshakeClientH1(self: *Conn, connector: *const ClientConnector, io: std.Io) !void {
        try self.handshakeClientAny(connector, io);
        if (!isHttp11Alpn(self.selectedAlpn())) return error.TlsHandshakeFailed;
    }

    /// Handshake with no ALPN assertion. The caller checks `selectedAlpn`.
    pub fn handshakeClientAny(self: *Conn, connector: *const ClientConnector, io: std.Io) !void {
        std.debug.assert(self.state == .tcp);
        self.bindIo(io);
        var rng: std.Random.IoSource = .{ .io = io };
        var hs = tls.nonblock.Client.init(.{
            .rng = rng.interface(),
            .now = std.Io.Clock.real.now(io),
            .host = "localhost",
            .root_ca = .empty,
            .insecure_skip_verify = true,
            // TLS 1.3 only, like the server; a TLS 1.2 cipher would keep a
            // pointer to `rng` past this frame.
            .cipher_suites = tls.config.cipher_suites.tls13,
            .alpn_protocols = connector.alpn_protocols,
        });
        var iterations: u32 = 0;
        while (!hs.done()) {
            iterations += 1;
            if (iterations > MaxHandshakeIterations) return error.TlsHandshakeFailed;
            const res = hs.run(self.in_buf[self.in_head..self.in_fill], &self.out_buf) catch
                return error.TlsHandshakeFailed;
            self.in_head += res.recv_pos;
            if (res.send.len > 0) {
                self.tcp_writer.interface.writeAll(res.send) catch return error.TlsHandshakeFailed;
            }
            if (hs.done()) break;
            if (res.recv_pos == 0 and res.send.len == 0) {
                self.feedFromSocket() catch return error.TlsHandshakeFailed;
            }
        }
        try self.setAlpn(hs.inner.alpn_protocol);
        self.cipher = .init(hs.cipher().?);
        self.state = .open;
    }

    pub fn deinit(self: *Conn) void {
        self.state = .empty;
    }

    /// Blocking plaintext read for the loopback clients. Returns 0 at end of
    /// stream (close_notify or socket EOF).
    pub fn readPlain(self: *Conn, output: []u8) !usize {
        std.debug.assert(self.state == .open);
        if (output.len == 0) return 0;
        var iterations: u32 = 0;
        while (self.plain_head == self.plain_end) {
            iterations += 1;
            if (iterations > MaxHandshakeIterations) return error.TlsReadFailed;
            if (self.peer_closed) return 0;
            // In place: tls.zig allows the cleartext buffer to be the
            // ciphertext buffer. The plaintext lands at `in_head`, inside
            // the bytes this call consumes.
            const region = self.in_buf[self.in_head..self.in_fill];
            const res = self.cipher.decrypt(region, region) catch return error.TlsReadFailed;
            std.debug.assert(self.cipher.inner.cleartext_buf.len == 0);
            if (res.closed) self.peer_closed = true;
            if (res.ciphertext_pos > 0) {
                self.plain_head = self.in_head;
                self.plain_end = self.in_head + res.cleartext.len;
                self.in_head += res.ciphertext_pos;
                continue;
            }
            self.feedFromSocket() catch return 0;
        }
        const n = @min(output.len, self.plain_end - self.plain_head);
        @memcpy(output[0..n], self.in_buf[self.plain_head..][0..n]);
        self.plain_head += n;
        return n;
    }

    pub fn writePlain(self: *Conn, input: []const u8) !void {
        std.debug.assert(self.state == .open);
        var off: usize = 0;
        while (off < input.len) {
            const chunk = @min(input.len - off, max_record_plaintext);
            const res = self.cipher.encrypt(input[off..][0..chunk], &self.out_buf) catch
                return error.TlsWriteFailed;
            if (res.cleartext_pos == 0) return error.TlsWriteFailed;
            self.tcp_writer.interface.writeAll(res.ciphertext) catch return error.TlsWriteFailed;
            off += res.cleartext_pos;
        }
    }

    /// Inbound ciphertext not decrypted yet, complete records or not. Diag.
    pub fn pendingInboundCiphertext(self: *const Conn) usize {
        return self.in_fill - self.in_head;
    }

    /// The pump's park predicate for the inbound direction: a complete
    /// record is buffered, so a decrypt can make progress. A partial record
    /// is not work: the pump must park until the socket brings the rest.
    /// One implementation for every pump gate site, so they cannot drift;
    /// the in-process record test pins this exact function.
    pub fn pendingInbound(self: *const Conn) bool {
        return completeRecordLen(self.in_buf[self.in_head..self.in_fill]) != null;
    }

    /// Decrypt complete buffered records into `out`, one record per call into
    /// tls.zig. 0 means no complete record is buffered, `out` cannot take the
    /// next record's payload, or the peer closed (`peer_closed`). Records
    /// that carry no application data (a KeyUpdate) are consumed without
    /// adding bytes.
    ///
    /// One record at a time, and only into room for its whole payload: when
    /// the output is smaller than the payload, tls.zig decrypts in place in
    /// the ciphertext buffer and keeps a reference to that plaintext for the
    /// next call. `in_buf` is compacted and refilled, so that plaintext
    /// would be overwritten before it was read.
    fn decryptInto(self: *Conn, out: []u8) !usize {
        var n: usize = 0;
        while (!self.peer_closed) {
            const rec_len = completeRecordLen(self.in_buf[self.in_head..self.in_fill]) orelse break;
            if (out.len - n < rec_len - 5) break;
            const res = try self.cipher.decrypt(self.in_buf[self.in_head..][0..rec_len], out[n..]);
            std.debug.assert(res.ciphertext_pos == rec_len);
            std.debug.assert(res.cleartext.ptr == out[n..].ptr);
            std.debug.assert(self.cipher.inner.cleartext_buf.len == 0);
            self.in_head += rec_len;
            n += res.cleartext.len;
            if (res.closed) self.peer_closed = true;
        }
        return n;
    }

    /// Copy up to `bytes.len` of received ciphertext into `in_buf`. Returns
    /// how much fit; 0 when `in_buf` is full.
    fn acceptCipher(self: *Conn, bytes: []const u8) usize {
        if (self.in_buf.len - self.in_fill < bytes.len) self.compactIn();
        const n = @min(bytes.len, self.in_buf.len - self.in_fill);
        @memcpy(self.in_buf[self.in_fill..][0..n], bytes[0..n]);
        self.in_fill += n;
        return n;
    }

    /// Move the undecrypted tail to the front of `in_buf`.
    fn compactIn(self: *Conn) void {
        std.debug.assert(self.plain_head == self.plain_end);
        const len = self.in_fill - self.in_head;
        std.mem.copyForwards(u8, self.in_buf[0..len], self.in_buf[self.in_head..self.in_fill]);
        self.in_head = 0;
        self.in_fill = len;
        self.plain_head = 0;
        self.plain_end = 0;
    }

    fn feedFromSocket(self: *Conn) !void {
        self.compactIn();
        if (self.in_fill == self.in_buf.len) return error.TlsReadFailed;
        var dest: [1][]u8 = .{self.in_buf[self.in_fill..]};
        const n = self.tcp_reader.interface.readVec(&dest) catch return error.TlsReadFailed;
        if (n == 0) return error.TlsReadFailed;
        self.in_fill += n;
    }
};


/// One task owns encrypt and decrypt. The cipher state is not thread-safe;
/// this is the share-nothing owner.
///
/// The pump is a `zio.CompletionQueue` driver. Inbound ciphertext is a raw
/// `ev.NetRecv` completion into ONE read buffer, re-submitted only when the
/// buffer is fully copied into `Conn.in_buf` (that buffer's bound plus this
/// one is the inbound backpressure). Outbound frames arrive on a
/// `zio.Channel(WireChunk)`. The idle wait is one select over both; nothing
/// is reset, so no publish can be missed. Socket writes stay on the driver
/// so the SSE event path does not gain a hop.
///
/// The socket write is a raw `ev.NetSend` completion on the SAME queue, not
/// a blocking writer: the driver encrypts records straight into `send_buf`
/// and submits it; the completion is one more CQ wake. The driver therefore
/// never parks on the socket. When `send_buf` cannot take the next record
/// behind an in-flight send, the unfinished write batch stays in
/// `pending_writes` (with the plaintext offset of the partial chunk) and the
/// driver waits on the CQ only; the send completion compacts, re-arms, and
/// the batch resumes. That is the backpressure path (t-2655), exactly where
/// the old `writeAll` used to block. A chunk is acked once its records are
/// in the staging buffer.
///
/// Ownership: the recv completion lives in this struct and is submitted
/// only by the driver itself, so it never needs the heap. No other task
/// submits to the CQ (the actor uses the channel), which is what makes
/// shutdown a local `close` + `cancelAll(.keep)` + drain in `shutdownCq`.
pub const Pump = struct {
    io: std.Io,
    conn: *Conn,
    /// Unused for TLS inbound after M2: plaintext stashes in `pending_read`.
    /// Kept so construction and failDrain stay one shape.
    to_actor: *zio.Channel(wire_pump.WireChunk),
    /// Unused as the TLS write path after M3. failDrain still drains it
    /// so a leftover overflow chunk is not dropped.
    write_ch: *zio.Channel(wire_pump.WireChunk),
    /// Raw socket handle for the driver's `ev.NetRecv` completion.
    sock: zio.ev.Backend.NetHandle,
    /// The driver's single ciphertext read buffer (replaces the cipher
    /// chunk pool). `pending_cipher` aliases its tail while a suffix is
    /// still unconsumed, and the recv op is only re-armed once it is free.
    recv_buf: []u8,
    /// Diag: where this pump currently is. 0=not started, 1=running,
    /// 2=select wait, 3=writeChunks, 4=cipher ingest, 5=exited. The actor's
    /// park snapshot prints it, so a wedge names the pump's blocking site
    /// without a coroutine stack.
    site: *std.atomic.Value(u8),
    /// Diag: this pump's opaque zio task handle, published at run() start.
    task_h: ?*std.atomic.Value(usize) = null,
    task_handle_fn: ?*const fn () usize = null,
    /// A zio channel: unused for TLS acks after M3 (acks apply locally).
    /// Kept so `failDrain` can still drain a closed overflow channel.
    completions: *zio.Channel(wire_pump.WriteCompletion),
    /// When set, `post` applies the ack on this task instead of a channel hop.
    ack_apply: ?*const fn (*anyopaque, wire_pump.WriteCompletion) void = null,
    ack_ctx: *anyopaque = undefined,
    gpa: std.mem.Allocator,
    chunk_storage: []u8,
    n_chunks: u32,
    read_free: *std.Io.Queue(u32),
    write_free: ?*std.Io.Queue(u32) = null,
    live_task_handlers: *std.atomic.Value(usize),
    stopped: std.atomic.Value(bool) = .init(false),
    test_delay_ms: u64 = 0,
    test_fail_after: u64 = 0,
    writes_done: u64 = 0,
    /// Empty flush/sentinel pulled off the write queue while gathering a
    /// DATA batch. WritePump parks the same item in `carried`; dropping it
    /// here leaked tickets and, for a flush with outbound_release, held bytes.
    carried: ?wire_pump.WireChunk = null,
    /// Plaintext already decrypted, waiting for the actor to ingest. Aliases
    /// `plain_buf`. Must not drop: the cipher has advanced.
    pending_read: ?wire_pump.WireChunk = null,
    /// Unconsumed suffix of `recv_buf`, stashed when `Conn.in_buf` was full
    /// and `pending_read` blocked decrypt. The recv op is not re-armed
    /// while this is set, so the bytes cannot be overwritten.
    pending_cipher: ?[]const u8 = null,
    /// Decrypt destination. Aliases the first `WIRE_CHUNK_SIZE` of
    /// `chunk_storage` (already in `resourceUpperBound`); no new buffer.
    /// `pending_read` always points here, never a pool lease or a GPA chunk.
    plain_buf: []u8 = &.{},
    /// Actor-visible inbound EOF (replaces the empty sentinel on `to_actor`).
    inbound_eof: bool = false,
    /// Driver-owned CQ state. Wired in `run()` at the struct's final
    /// address, never in an init that returns by value (init-move hazard:
    /// the ReadBuf points into `recv_iov`).
    cq: zio.CompletionQueue = undefined,
    recv_op: zio.ev.NetRecv = undefined,
    recv_iov: [1]zio.os.iovec = undefined,
    recv_armed: bool = false,
    /// Outbound ciphertext staging for the driver's raw `ev.NetSend`. Aliases
    /// `conn.out_buf`: the handshake flights are done once `run()` starts, so
    /// the buffer costs nothing new. `[send_head..send_fill)` is staged; the in-flight send
    /// covers a prefix of it. `send_op` is rebuilt per submit (the slice
    /// changes); it is only touched after the CQ handed it back.
    send_op: zio.ev.NetSend = undefined,
    send_iov: [1]zio.os.iovec_const = undefined,
    send_buf: []u8 = &.{},
    send_head: usize = 0,
    send_fill: usize = 0,
    send_armed: bool = false,
    /// A write batch that could not finish because the staging buffer is full
    /// behind an in-flight send. `partial_off` is the plaintext offset already
    /// encrypted for `pending_writes[0]` (the chunk bytes do not move). Nothing new is
    /// taken from `write_ch` while this is non-empty.
    pending_writes: [max_write_batch]wire_pump.WireChunk = undefined,
    pending_n: usize = 0,
    partial_off: usize = 0,

    pub const max_write_batch = 16;

    fn post(self: *Pump, c: wire_pump.WriteCompletion) void {
        _ = wire_pump.diag_acks.posted_release.fetchAdd(c.outbound_release, .monotonic);
        if (self.ack_apply) |f| {
            f(self.ack_ctx, c);
            return;
        }
        self.completions.trySend(c) catch |err| switch (err) {
            error.WouldBlock => @panic("write ack channel over proven capacity"),
            error.Closed => {},
        };
    }

    fn returnReadIndex(self: *Pump, idx: u32) void {
        self.read_free.putOneUncancelable(self.io, idx) catch {};
    }

    fn postEof(self: *Pump) void {
        self.inbound_eof = true;
    }

    fn releaseChunk(self: *Pump, chunk: wire_pump.WireChunk, ok: bool, fail_all: bool) void {
        if (chunk.pool_index) |idx| {
            const q = self.write_free orelse unreachable;
            if (!io_queue.tryPut(u32, q, self.io, idx)) {
                std.debug.assert(false);
            }
        } else if (chunk.bytes.len != 0) {
            self.gpa.free(chunk.bytes);
        }
        const has_ticket = chunk.ticket_count != 0 or chunk.ticket != 0;
        const has_acct = chunk.outbound_release != 0 or chunk.control_entries != 0;
        if (has_ticket or has_acct or fail_all) {
            if (comptime observe) {
                if (chunk.ticket != 0) {
                    _ = wire_pump.diag_acks.posted_ticket.fetchAdd(1, .monotonic);
                    if (chunk.complete_batch_receipt) {
                        _ = wire_pump.diag_acks.posted_receipt.fetchAdd(1, .monotonic);
                    }
                }
            }
            var written_ns: u64 = 0;
            if (ok and chunk.ticket != 0) {
                written_ns = nowNs(self.io);
                wire_pump.test_last_ticket_ok_ns.store(written_ns, .release);
                wire_pump.test_last_ticket_ok_id.store(chunk.ticket, .release);
            }
            self.post(.{
                .ticket = chunk.ticket,
                .ticket_slot = chunk.ticket_slot,
                .ticket_count = if (chunk.ticket_count != 0) chunk.ticket_count else if (chunk.ticket != 0) 1 else 0,
                .ok = ok,
                .outbound_release = chunk.outbound_release,
                .written_ns = written_ns,
                .control_release = chunk.control_release,
                .control_entries = chunk.control_entries,
                .fail_all = fail_all,
                .complete_batch_receipt = chunk.complete_batch_receipt,
            });
        }
    }

    fn isSentinel(chunk: wire_pump.WireChunk) bool {
        return chunk.len == 0 and chunk.bytes.len == 0 and !chunk.flush_barrier;
    }

    pub fn failDrain(self: *Pump) void {
        if (self.carried) |chunk| {
            self.carried = null;
            if (!isSentinel(chunk)) self.releaseChunk(chunk, false, false);
        }
        // pending_read aliases plain_buf; nothing to free.
        self.pending_read = null;
        // The stashed suffix aliases recv_buf; nothing to release.
        self.pending_cipher = null;
        for (self.pending_writes[0..self.pending_n]) |chunk| {
            if (!isSentinel(chunk)) self.releaseChunk(chunk, false, false);
        }
        self.pending_n = 0;
        self.partial_off = 0;
        while (true) {
            const chunk = self.write_ch.tryReceive() catch break;
            if (isSentinel(chunk)) continue;
            self.releaseChunk(chunk, false, false);
        }
        self.post(.{ .fail_all = true });
    }

    /// Records are encrypted straight into `send_buf`, so staging is only
    /// arming the send for what is there. False only when the CQ is already
    /// closed (teardown).
    fn stageOrExit(self: *Pump) bool {
        return self.armSend();
    }

    fn compactSend(self: *Pump) void {
        std.debug.assert(!self.send_armed);
        const len = self.send_fill - self.send_head;
        std.mem.copyForwards(u8, self.send_buf[0..len], self.send_buf[self.send_head..self.send_fill]);
        self.send_head = 0;
        self.send_fill = len;
    }

    /// Submit the staged-but-unsent bytes as one send completion. False only
    /// when the CQ is already closed (local shutdown owns the exit).
    fn armSend(self: *Pump) bool {
        if (self.send_armed or self.send_fill == self.send_head) return true;
        self.send_op = zio.ev.NetSend.init(
            self.sock,
            zio.ev.WriteBuf.fromSlice(self.send_buf[self.send_head..self.send_fill], &self.send_iov),
            .{},
        );
        @import("connection.zig").probeNote(.tls_send_submit);
        self.cq.submit(&self.send_op.c) catch |err| switch (err) {
            error.Closed => return false,
            error.InvalidCompletion => unreachable,
        };
        self.send_armed = true;
        return true;
    }

    /// The send completion fired: advance, compact, re-arm for what is still
    /// staged. A short send is just a smaller advance. A batch parked on a
    /// full `send_buf` resumes on the actor's next turn (`drivePending`).
    fn onSendComplete(self: *Pump) RecvOutcome {
        self.send_armed = false;
        const n = self.send_op.getResult() catch |err| switch (err) {
            // Only shutdownCq cancels the op; teardown owns the exit.
            error.Canceled => return .exit,
            else => {
                if (diag_wait) rawPrint("TLSERR send_complete {s}\n", .{@errorName(err)});
                _ = tls_stage_failed.fetchAdd(1, .monotonic);
                self.failDrain();
                self.post(.{ .fail_all = true, .shutdown = true });
                return .exit;
            },
        };
        pump_trace.bump(&pump_trace.send_complete);
        self.send_head += n;
        std.debug.assert(self.send_head <= self.send_fill);
        if (self.send_head == self.send_fill) {
            self.send_head = 0;
            self.send_fill = 0;
        } else {
            self.compactSend();
        }
        return if (self.armSend()) .ok else .exit;
    }

    const Feed = enum { done, blocked, eof };

    /// Copy ciphertext into `Conn.in_buf`. `consumed` is updated on every
    /// path. `.blocked` means inbound cannot make progress (pending plaintext
    /// the actor has not taken, or no read-pool index). Caller must stash the
    /// rest and service `write_ch` — spinning here to MaxHandshakeIterations
    /// failDrains the connection while SETTINGS still sit on the write queue.
    fn feedCipher(self: *Pump, bytes: []const u8, consumed: *usize) Feed {
        consumed.* = 0;
        var spins: u32 = 0;
        while (consumed.* < bytes.len) {
            spins += 1;
            if (spins > MaxHandshakeIterations) {
                if (diag_wait) rawPrint("TLSERR feedcipher_spins\n", .{});
                self.failDrain();
                self.post(.{ .shutdown = true });
                return .eof;
            }
            const n = self.conn.acceptCipher(bytes[consumed.*..]);
            if (n > 0) {
                consumed.* += n;
                continue;
            }
            // `in_buf` is full. It holds one maximum record, so a full buffer
            // without a complete record is a record longer than TLS allows.
            if (!self.conn.pendingInbound()) {
                if (diag_wait) rawPrint("TLSERR oversized_record\n", .{});
                self.failDrain();
                self.post(.{ .shutdown = true });
                return .eof;
            }
            if (self.pending_read != null) return .blocked;
            switch (self.readOne()) {
                .eof => return .eof,
                .ok => {
                    if (self.pending_read != null) return .blocked;
                },
                .stuck => return .blocked,
                .want => {},
            }
            if (!self.stageOrExit()) return .eof;
        }
        return .done;
    }

    pub fn tryTakeWrite(self: *Pump) ?wire_pump.WireChunk {
        const chunk = self.write_ch.tryReceive() catch return null;
        pump_trace.bump(&pump_trace.tryget_write);
        return chunk;
    }

    /// The most chunks one scheduler pop can push before the scheduler asks
    /// `pause`/`pause_control` again. The drain sink flushes a non-empty
    /// packed batch and then queues an unbatchable frame (unticketed DATA)
    /// on its own: two pushes for one pop. A pause that fires only at zero
    /// room lets that second push overflow and fail-close the connection.
    pub const pushes_per_pop: usize = 2;

    /// True when the stash cannot take `pushes_per_pop` more chunks, so the
    /// scheduler must stop popping. `carried` holds one chunk; `write_ch`
    /// holds the rest.
    pub fn stashFull(self: *Pump) bool {
        const cap = 1 + self.write_ch.impl.capacity;
        const used = @as(usize, if (self.carried != null) 1 else 0) + io_queue.chanLen(wire_pump.WireChunk, self.write_ch);
        return cap - used < pushes_per_pop;
    }

    fn stealWrite(self: *Pump) void {
        if (self.carried != null) return;
        self.carried = self.tryTakeWrite();
    }

    const WriteSome = enum { done, full, exit };

    /// Encrypt `bytes` from `self.partial_off` on, one full-size record at a
    /// time, straight into `send_buf`. `.full`: `send_buf` cannot take the
    /// next record behind an in-flight send and `partial_off` holds the
    /// progress; the caller keeps the chunk pending and parks on the CQ
    /// (t-2655). A record is only encrypted when all of it fits, so record
    /// sizes follow the write chunks (the packing in `emit_batch`), never
    /// the space left in the buffer.
    fn encryptSome(self: *Pump, bytes: []const u8) WriteSome {
        var spins: u32 = 0;
        while (self.partial_off < bytes.len) {
            spins += 1;
            if (spins > MaxHandshakeIterations) {
                _ = tls_stage_failed.fetchAdd(1, .monotonic);
                if (diag_wait) rawPrint("TLSERR encrypt_spins\n", .{});
                self.failDrain();
                self.post(.{ .shutdown = true });
                return .exit;
            }
            const chunk = bytes[self.partial_off..][0..@min(bytes.len - self.partial_off, max_record_plaintext)];
            const need = self.conn.cipher.encryptedLength(chunk.len);
            if (self.send_buf.len - self.send_fill < need) {
                if (!self.send_armed and self.send_head > 0) {
                    self.compactSend();
                    continue;
                }
                // Whatever is staged must be in flight, or nothing would
                // ever free the buffer and the park would never end.
                if (!self.armSend()) return .exit;
                return .full;
            }
            const res = self.conn.cipher.encrypt(chunk, self.send_buf[self.send_fill..]) catch |err| {
                if (diag_wait) rawPrint("TLSERR encrypt {s}\n", .{@errorName(err)});
                _ = tls_stage_failed.fetchAdd(1, .monotonic);
                self.failDrain();
                self.post(.{ .shutdown = true });
                return .exit;
            };
            std.debug.assert(res.cleartext_pos == chunk.len);
            self.send_fill += res.ciphertext.len;
            self.partial_off += chunk.len;
        }
        self.partial_off = 0;
        return .done;
    }

    /// Take a batch off `write_ch` (first already taken) into `pending_writes`
    /// and drive it. Returns false on exit; true otherwise, including the
    /// parked case (`pending_n > 0`, waiting for a send completion).
    pub fn writeChunks(self: *Pump, first: wire_pump.WireChunk) bool {
        std.debug.assert(self.pending_n == 0);
        if (first.len == 0 and first.bytes.len == 0 and !first.flush_barrier) {
            _ = tls_stage_failed.fetchAdd(1, .monotonic);
            if (diag_wait) rawPrint("TLSERR sentinel\n", .{});
            self.failDrain();
            self.post(.{ .shutdown = true });
            return false;
        }
        self.pending_writes[0] = first;
        self.pending_n = 1;
        self.partial_off = 0;
        if (!first.flush_barrier and self.test_delay_ms == 0 and self.test_fail_after == 0) {
            while (self.pending_n < max_write_batch) {
                if (self.pending_cipher != null) break;
                const next = self.tryTakeWrite() orelse break;
                if (next.len == 0 and next.bytes.len == 0) {
                    self.carried = next;
                    break;
                }
                self.pending_writes[self.pending_n] = next;
                self.pending_n += 1;
            }
        }
        if (self.test_delay_ms > 0) {
            self.io.sleep(.fromMilliseconds(@intCast(self.test_delay_ms)), .awake) catch {
                self.failPending();
                self.failDrain();
                self.post(.{ .shutdown = true });
                return false;
            };
        }
        const fail_next = wire_pump.test_fail_next_write.swap(false, .acq_rel);
        if (fail_next or (self.test_fail_after > 0 and self.writes_done >= self.test_fail_after)) {
            _ = tls_stage_failed.fetchAdd(1, .monotonic);
            self.failPending();
            self.failDrain();
            self.post(.{ .shutdown = true });
            return false;
        }
        pump_trace.bump(&pump_trace.write_chunks);
        pump_trace.add(&pump_trace.write_chunk_sum, self.pending_n);
        if (wire_pump.write_trace.enabled) wire_pump.write_trace.note(self.pending_n);
        return self.drivePending();
    }

    fn failPending(self: *Pump) void {
        for (self.pending_writes[0..self.pending_n]) |c| self.releaseChunk(c, false, false);
        self.pending_n = 0;
        self.partial_off = 0;
    }

    fn popPending(self: *Pump) void {
        std.debug.assert(self.pending_n > 0);
        std.mem.copyForwards(
            wire_pump.WireChunk,
            self.pending_writes[0 .. self.pending_n - 1],
            self.pending_writes[1..self.pending_n],
        );
        self.pending_n -= 1;
        self.partial_off = 0;
    }

    /// Encrypt the pending batch in order; ack each chunk once its records
    /// are staged; stop (keep the rest pending) when the staging buffer is full
    /// behind an in-flight send. Returns false on exit.
    pub fn drivePending(self: *Pump) bool {
        while (self.pending_n > 0) {
            const chunk = self.pending_writes[0];
            if (chunk.len == 0 and chunk.bytes.len == 0) {
                // A flush barrier: everything before it is already staged;
                // make sure it is on its way.
                std.debug.assert(chunk.flush_barrier);
                if (!self.armSend()) {
                    self.failPending();
                    return false;
                }
                self.popPending();
                self.releaseChunk(chunk, true, false);
                continue;
            }
            switch (self.encryptSome(chunk.bytes[0..chunk.len])) {
                .exit => {
                    self.failPending();
                    return false;
                },
                .full => return true,
                .done => {
                    self.popPending();
                    self.writes_done += 1;
                    self.releaseChunk(chunk, true, false);
                },
            }
        }
        // Batch end: send what is staged.
        return self.armSend();
    }

    pub const ReadOutcome = enum { ok, eof, want, stuck };

    pub fn readOne(self: *Pump) ReadOutcome {
        pump_trace.bump(&pump_trace.read_one);
        if (self.pending_read != null) return .stuck;
        const buf = self.plain_buf;
        if (buf.len == 0 or self.n_chunks == 0) {
            pump_trace.bump(&pump_trace.read_free_empty_yield);
            return .stuck;
        }
        const n = self.conn.decryptInto(buf) catch |err| {
            self.conn.tcp_stream.shutdown(self.io, .send) catch {};
            if (diag_wait) rawPrint("TLSERR recv2 {s}\n", .{@errorName(err)});
            self.failDrain();
            self.postEof();
            return .eof;
        };
        if (n == 0) {
            if (!self.conn.peer_closed) {
                pump_trace.bump(&pump_trace.want_read);
                return .want;
            }
            // close_notify: the same end of stream as a socket EOF.
            self.conn.tcp_stream.shutdown(self.io, .send) catch {};
            if (diag_wait) rawPrint("TLSERR recv2_eof\n", .{});
            self.failDrain();
            self.postEof();
            return .eof;
        }
        self.pending_read = .{ .bytes = buf, .len = n };
        return .ok;
    }

    pub fn ingestCipher(self: *Pump, bytes: []const u8) bool {
        pump_trace.bump(&pump_trace.cipher_chunks);
        var consumed: usize = 0;
        switch (self.feedCipher(bytes, &consumed)) {
            .eof => return false,
            .blocked => {
                if (consumed < bytes.len) {
                    std.debug.assert(self.pending_cipher == null);
                    self.pending_cipher = bytes[consumed..];
                    return true;
                }
                // Fully in `in_buf`; the read buffer is free again, but
                // inbound cannot advance right now — re-arm only.
                return self.rearmRecv();
            },
            .done => {},
        }
        if (!self.rearmRecv()) return false;
        if (self.pending_read != null) return true;
        switch (self.readOne()) {
            .eof => return false,
            .ok, .want, .stuck => return true,
        }
    }

    /// (Re)submit the socket read once `recv_buf` is free. False only when
    /// the CQ is already closed (local shutdown owns the exit).
    fn rearmRecv(self: *Pump) bool {
        if (self.recv_armed) return true;
        std.debug.assert(self.pending_cipher == null);
        @import("connection.zig").probeNote(.tls_recv_submit);
        self.cq.submit(&self.recv_op.c) catch |err| switch (err) {
            error.Closed => return false,
            // The op is ours alone: never grouped, never rearm-flagged.
            error.InvalidCompletion => unreachable,
        };
        self.recv_armed = true;
        return true;
    }

    pub const RecvPoll = enum { none, progress, exit };

    /// Take one completion (recv or send) off the CQ without blocking.
    pub fn pollRecv(self: *Pump) RecvPoll {
        const c = self.cq.next() orelse return .none;
        return switch (self.onCqComplete(c)) {
            .ok => .progress,
            .exit => .exit,
        };
    }

    /// Dispatch one CQ completion: the recv op or the send op, nothing else
    /// is ever submitted to this queue.
    pub fn onCqComplete(self: *Pump, c: *zio.ev.Completion) RecvOutcome {
        if (c == &self.recv_op.c) return self.onRecvComplete();
        std.debug.assert(c == &self.send_op.c);
        return self.onSendComplete();
    }

    pub const RecvOutcome = enum { ok, exit };

    /// The recv completion fired: ingest the bytes and re-arm, or report
    /// EOF / error to the actor exactly like the old read task did.
    fn onRecvComplete(self: *Pump) RecvOutcome {
        self.recv_armed = false;
        pump_trace.bump(&pump_trace.work_get);
        const n = self.recv_op.getResult() catch |err| switch (err) {
            // Only shutdownCq cancels the op; teardown owns the exit.
            error.Canceled => return .exit,
            else => {
                self.conn.tcp_stream.shutdown(self.io, .send) catch {};
                if (diag_wait) rawPrint("TLSERR cqrecv {s}\n", .{@errorName(err)});
                self.failDrain();
                self.postEof();
                self.post(.{ .shutdown = true });
                return .exit;
            },
        };
        if (n == 0) {
            self.conn.tcp_stream.shutdown(self.io, .send) catch {};
            if (diag_wait) rawPrint("TLSERR cqrecv_eof\n", .{});
            self.failDrain();
            self.postEof();
            self.post(.{ .shutdown = true });
            return .exit;
        }
        if (!self.ingestCipher(self.recv_buf[0..n])) return .exit;
        return .ok;
    }

    /// Wire the CQ and the recv op at this struct's final address, then arm
    /// the first recv. The recv ReadBuf points into `recv_iov`, so this must
    /// not run in an init that returns by value (init-move hazard). False
    /// only when the CQ is already closed.
    pub fn start(self: *Pump) bool {
        self.send_buf = self.conn.out_buf[0..];
        self.send_head = 0;
        self.send_fill = 0;
        self.send_armed = false;
        self.pending_n = 0;
        self.partial_off = 0;
        self.inbound_eof = false;
        const chunk_size = limits_mod.WIRE_CHUNK_SIZE;
        self.plain_buf = if (self.chunk_storage.len >= chunk_size)
            self.chunk_storage[0..chunk_size]
        else
            &.{};
        self.cq = zio.CompletionQueue.init();
        self.recv_op = zio.ev.NetRecv.init(
            self.sock,
            zio.ev.ReadBuf.fromSlice(self.recv_buf, &self.recv_iov),
            .{},
        );
        self.recv_armed = false;
        return self.rearmRecv();
    }

    /// Local CQ shutdown: close, cancel in-flight ops with results kept,
    /// and drain. The ops live in this struct, so there is nothing to free;
    /// the drain is what guarantees the loop no longer touches them.
    pub fn shutdownCq(self: *Pump) void {
        self.cq.close();
        self.cq.cancelAll(.keep);
        while (self.cq.next()) |_| {}
    }

};


fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

// In-source loopback fixture pair for the record tests. Deliberately NOT
// testdata/*.pem: those are machine-local by convention (gitignored), and a
// unit test must build on a fresh checkout. Throwaway self-signed
// CN=localhost material, public by nature.
pub const fixture_cert_pem =
    \\-----BEGIN CERTIFICATE-----
    \\MIIBfDCCASOgAwIBAgIUNrYfW/94JO0I8Ly5JLgu+e2Z+vcwCgYIKoZIzj0EAwIw
    \\FDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDgyMDEzNTIyNloXDTM2MDgxNzEz
    \\NTIyNlowFDESMBAGA1UEAwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0D
    \\AQcDQgAEP4wKMqBqZx54+J7kJy9dMcm+Lsx6cit54Sd4eCAzr6uxolWi8M1p3OpO
    \\m7OKRMunkzOTYbPCQjT9NBR0sW89EKNTMFEwHQYDVR0OBBYEFPgT4WNVaveWv1TV
    \\YxhSBrSNhzXfMB8GA1UdIwQYMBaAFPgT4WNVaveWv1TVYxhSBrSNhzXfMA8GA1Ud
    \\EwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDRwAwRAIgdLWKYMUmeqYLwrVPIgJGLxQZ
    \\p0uJdNZ1LnWS2JPowPwCIEGkXAU3QDok+T9Sj0GOGEq6Nhnv3nchWxg24ZqUJ6CR
    \\-----END CERTIFICATE-----
;
pub const fixture_key_pem =
    \\-----BEGIN PRIVATE KEY-----
    \\MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgjpKRKa9ZDVtHcAgb
    \\EwexTKPP66fnsfBsAyqcQoT4aKmhRANCAAQ/jAoyoGpnHnj4nuQnL10xyb4uzHpy
    \\K3nhJ3h4IDOvq7GiVaLwzWnc6k6bs4pEy6eTM5Nhs8JCNP00FHSxbz0Q
    \\-----END PRIVATE KEY-----
;

/// Server and client cipher states after a handshake run entirely in
/// memory, with the ALPN the server selected.
const MemPair = struct {
    server: tls.nonblock.Connection,
    client: tls.nonblock.Connection,
    alpn: ?[]const u8,
};

/// Run a full handshake between tls.zig's non-blocking client and the
/// server configuration `Conn.handshake` uses, shuttling bytes in memory.
fn memHandshake(acceptor: *Acceptor, client_alpn: []const []const u8) !MemPair {
    var prng = std.Random.DefaultPrng.init(0x5747_4832);
    const now = std.Io.Clock.real.now(std.testing.io);
    var srv = tls.nonblock.Server.init(.{
        .rng = prng.random(),
        .auth = &acceptor.auth,
        .alpn_protocols = &server_alpn,
        .now = now,
    });
    var cli = tls.nonblock.Client.init(.{
        .rng = prng.random(),
        .now = now,
        .host = "localhost",
        .root_ca = .empty,
        .insecure_skip_verify = true,
        .cipher_suites = tls.config.cipher_suites.tls13,
        .alpn_protocols = client_alpn,
    });
    var c2s: [2 * record_buffer_size]u8 = undefined;
    var c2s_len: usize = 0;
    var s2c: [2 * record_buffer_size]u8 = undefined;
    var s2c_len: usize = 0;
    var scratch: [record_buffer_size]u8 = undefined;
    var iterations: u32 = 0;
    while (!(cli.done() and srv.done())) {
        iterations += 1;
        if (iterations > 64) return error.HandshakeStalled;
        const cr = try cli.run(s2c[0..s2c_len], &scratch);
        std.mem.copyForwards(u8, s2c[0 .. s2c_len - cr.recv_pos], s2c[cr.recv_pos..s2c_len]);
        s2c_len -= cr.recv_pos;
        @memcpy(c2s[c2s_len..][0..cr.send.len], cr.send);
        c2s_len += cr.send.len;
        const sr = try srv.run(c2s[0..c2s_len], &scratch);
        std.mem.copyForwards(u8, c2s[0 .. c2s_len - sr.recv_pos], c2s[sr.recv_pos..c2s_len]);
        c2s_len -= sr.recv_pos;
        @memcpy(s2c[s2c_len..][0..sr.send.len], sr.send);
        s2c_len += sr.send.len;
    }
    return .{ .server = .init(srv.cipher().?), .client = .init(cli.cipher().?), .alpn = srv.alpnProtocol() };
}

// The t-866 regression test, on tls.zig: two application records arrive in
// ONE chunk. After the first decrypt the second is still in `in_buf`, and the
// pump's park predicate (pendingInbound) must report work, or the pump parks
// on top of a buried request and the connection wedges for good. The other
// half: a PARTIAL record is not work. If it counted, the pump would spin on
// a record it cannot decrypt instead of parking until the socket brings the
// rest. Removing either condition from pendingInbound fails this test.
test "a second record in one chunk is work; a partial record is not" {
    var acceptor = try Acceptor.initFromPem(std.testing.allocator, std.testing.io, fixture_cert_pem, fixture_key_pem);
    defer acceptor.deinit();
    var pair = try memHandshake(&acceptor, &.{alpn_h2});

    var server: Conn = .{};
    server.state = .open;
    server.cipher = pair.server;

    const record_a = "first-record-payload";
    const record_b = "second-record-payload";
    var wire: [512]u8 = undefined;
    const a = try pair.client.encrypt(record_a, &wire);
    const a_len = a.ciphertext.len;
    const b = try pair.client.encrypt(record_b, wire[a_len..]);
    const b_len = b.ciphertext.len;
    try std.testing.expectEqual(a_len + b_len, server.acceptCipher(wire[0 .. a_len + b_len]));

    // An output with room for record A's payload only: B's does not fit
    // after A's plaintext, so B stays buffered.
    var plain_storage: [64]u8 = undefined;
    const plain = plain_storage[0 .. a_len - 5];
    try std.testing.expectEqual(record_a.len, try server.decryptInto(plain));
    try std.testing.expectEqualStrings(record_a, plain[0..record_a.len]);

    // The wedge's exact state: a whole unread record is buffered.
    try std.testing.expect(server.pendingInbound());

    var plain_b: [64]u8 = undefined;
    const got_b = try server.decryptInto(&plain_b);
    try std.testing.expectEqualStrings(record_b, plain_b[0..got_b]);
    try std.testing.expect(!server.pendingInbound());

    // Half a record: buffered bytes, but no work until the rest arrives.
    const c = try pair.client.encrypt("third-record-payload", &wire);
    const half = c.ciphertext.len / 2;
    try std.testing.expectEqual(half, server.acceptCipher(wire[0..half]));
    try std.testing.expect(server.pendingInboundCiphertext() > 0);
    try std.testing.expect(!server.pendingInbound());
    try std.testing.expectEqual(@as(usize, 0), try server.decryptInto(&plain_b));
    try std.testing.expectEqual(c.ciphertext.len - half, server.acceptCipher(wire[half..c.ciphertext.len]));
    try std.testing.expect(server.pendingInbound());
}

test "h2 ALPN matcher" {
    try std.testing.expect(isHttp2Alpn("h2"));
    try std.testing.expect(!isHttp2Alpn("http/1.1"));
    try std.testing.expect(!isHttp2Alpn(null));
    try std.testing.expect(isHttp11Alpn(null));
    try std.testing.expect(isHttp11Alpn("http/1.1"));
    try std.testing.expect(!isHttp11Alpn("h2"));
}

test "ALPN select prefers h2, falls back to http/1.1, none selects nothing, unknown-only fails" {
    var acceptor = try Acceptor.initFromPem(std.testing.allocator, std.testing.io, fixture_cert_pem, fixture_key_pem);
    defer acceptor.deinit();

    const both = try memHandshake(&acceptor, &.{ alpn_http11, alpn_h2 });
    try std.testing.expectEqualStrings(alpn_h2, both.alpn.?);

    const h1_only = try memHandshake(&acceptor, &.{alpn_http11});
    try std.testing.expectEqualStrings(alpn_http11, h1_only.alpn.?);

    // No ALPN extension: no selection, which the server serves as HTTP/1.1.
    const none = try memHandshake(&acceptor, &.{});
    try std.testing.expect(none.alpn == null);
    try std.testing.expect(isHttp11Alpn(none.alpn));

    try std.testing.expectError(error.TlsNoApplicationProtocol, memHandshake(&acceptor, &.{"spdy/3"}));
}

// tls.zig issue #36: the server cannot reassemble a ClientHello that the
// client split across several records. This pins what that costs starh2:
// the handshake FAILS AT ONCE with an error on the first flight. It does not
// wait for bytes that will never come, so the connection closes right away
// instead of holding a handshake slot until the timeout. The control half
// proves the probe: the same ClientHello in one record is accepted. If
// tls.zig fixes #36, the first expectation fails, and this test should then
// require a completed handshake instead.
test "a ClientHello split across records fails at once (tls.zig #36)" {
    var acceptor = try Acceptor.initFromPem(std.testing.allocator, std.testing.io, fixture_cert_pem, fixture_key_pem);
    defer acceptor.deinit();
    var prng = std.Random.DefaultPrng.init(36);
    const now = std.Io.Clock.real.now(std.testing.io);
    var cli = tls.nonblock.Client.init(.{
        .rng = prng.random(),
        .now = now,
        .host = "localhost",
        .root_ca = .empty,
        .insecure_skip_verify = true,
        .cipher_suites = tls.config.cipher_suites.tls13,
        .alpn_protocols = &.{alpn_h2},
    });
    var scratch: [record_buffer_size]u8 = undefined;
    const hello = (try cli.run(&.{}, &scratch)).send;
    try std.testing.expect(hello.len > 5 + 512);

    // The same handshake payload, re-framed as 512-byte records.
    var frag: [2 * record_buffer_size]u8 = undefined;
    var frag_len: usize = 0;
    var off: usize = 5;
    while (off < hello.len) {
        const n = @min(512, hello.len - off);
        @memcpy(frag[frag_len..][0..3], hello[0..3]);
        std.mem.writeInt(u16, frag[frag_len + 3 ..][0..2], @intCast(n), .big);
        @memcpy(frag[frag_len + 5 ..][0..n], hello[off..][0..n]);
        frag_len += 5 + n;
        off += n;
    }

    var out: [record_buffer_size]u8 = undefined;
    var split_srv = tls.nonblock.Server.init(.{ .rng = prng.random(), .auth = &acceptor.auth, .alpn_protocols = &server_alpn, .now = now });
    try std.testing.expectError(error.TlsDecodeError, split_srv.run(frag[0..frag_len], &out));

    var whole_srv = tls.nonblock.Server.init(.{ .rng = prng.random(), .auth = &acceptor.auth, .alpn_protocols = &server_alpn, .now = now });
    const accepted = try whole_srv.run(hello, &out);
    try std.testing.expectEqual(hello.len, accepted.recv_pos);
    try std.testing.expect(accepted.send.len > 0);
}

test "pump_trace moves on read_free-empty yield" {
    var free_storage: [1]u32 = undefined;
    var read_free = std.Io.Queue(u32).init(&free_storage);
    var pump: Pump = undefined;
    pump.io = std.testing.io;
    pump.read_free = &read_free;
    pump.pending_read = null;
    pump.n_chunks = 0;

    const y0 = pump_trace.read_free_empty_yield.load(.acquire);
    const r0 = pump_trace.read_one.load(.acquire);
    try std.testing.expectEqual(Pump.ReadOutcome.stuck, pump.readOne());
    if (comptime observe) {
        try std.testing.expect(pump_trace.read_one.load(.acquire) >= r0 + 1);
        try std.testing.expect(pump_trace.read_free_empty_yield.load(.acquire) >= y0 + 1);
    }
}
