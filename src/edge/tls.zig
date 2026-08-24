//! TLS via memory BIOs. HTTP/2 never sees ciphertext.
//!
//! The SSL object has exactly one owner: the actor task, which drives `Pump`
//! methods on its own stack. Its BIOs are a bounded `BIO_new_bio_pair`, not
//! socket-coupled callbacks. SSL_read and SSL_write never run on a second
//! task — BoringSSL's SSL object is not thread-safe, so a write-only pump
//! beside an actor-owned recv would be a data race.
//!
//! The pump is a `zio.CompletionQueue` driver owned by the actor. The socket
//! read is a raw `ev.NetRecv` completion the actor submits and re-submits
//! after each arrival. Outbound frames stash on `queueWire` and SSL_write
//! on `driveTlsTurn` via `writeChunks` (not on the `drainEmit` stack: that
//! overflowed the coroutine). Ciphertext stages into the handshake
//! writer's buffer and arms `ev.NetSend`. Acks apply locally in `post`.
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
//! This file takes a DIRECT zio dependency (the CQ, the channel, the raw
//! completion). That is deliberate and open: an interface over the CQ would
//! be two implementations of one contract. The std.Io purity of src/edge
//! ends here; the h2c pumps remain std.Io-pure.
const std = @import("std");
const boring = @import("boring");
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
/// memory-BIO build that still Selects is visible: they must collapse to
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
pub const stream_buffer_size: usize = limits_mod.TLS_STREAM_BUFFER_SIZE;
pub const cipher_chunk_size: usize = limits_mod.TLS_CIPHER_CHUNK_SIZE;
pub const MaxHandshakeIterations: u32 = 4096;

comptime {
    std.debug.assert(stream_buffer_size >= 16 * 1024);
    std.debug.assert(stream_buffer_size == limits_mod.TLS_STREAM_BUFFER_SIZE);
    std.debug.assert(conn_buffer_bytes == limits_mod.TLS_CONN_BUFFER_BYTES);
    std.debug.assert(cipher_chunk_size == stream_buffer_size);
}

var alpn_select_ctx: AlpnCtx = .{};

/// tcp in + tcp out + BioPair (ssl write buf + transport write buf).
pub const conn_buffer_bytes: usize = stream_buffer_size * 4;

pub fn isHttp2Alpn(selected: ?[]const u8) bool {
    const proto = selected orelse return false;
    return std.mem.eql(u8, proto, alpn_h2);
}

const AlpnCtx = struct {};

fn selectH2Only(_: *AlpnCtx, _: *boring.ssl.SslRef, input: []const u8) boring.ssl.AlpnSelectResult {
    var index: usize = 0;
    while (index < input.len) {
        const protocol_len = input[index];
        const start = index + 1;
        const next = start + protocol_len;
        if (next > input.len) break;
        const proto = input[start..next];
        if (std.mem.eql(u8, proto, alpn_h2)) return .{ .selected = proto };
        index = next;
    }
    return .alertFatal;
}

/// Server-wide SSL_CTX plus certificate. Borrowed by every TLS connection.
pub const Acceptor = struct {
    tls_context: boring.ssl.Context,

    pub fn initFromPem(certificate_chain_pem: []const u8, private_key_pem: []const u8) !Acceptor {
        boring.init();
        var builder = try boring.ssl.ContextBuilder.init(boring.ssl.Method.tls());
        errdefer builder.deinit();

        try loadCertificateChain(&builder, certificate_chain_pem);
        var key = boring.pkey.PKey.fromPem(private_key_pem) catch return error.InvalidCertificate;
        defer key.deinit();
        builder.usePrivateKey(&key) catch return error.InvalidCertificate;
        builder.checkPrivateKey() catch return error.InvalidCertificate;
        builder.setVerify(boring.ssl.VerifyMode.none);
        builder.setAlpnSelectCallback(AlpnCtx, &alpn_select_ctx, selectH2Only) catch
            return error.InvalidCertificate;

        return .{ .tls_context = builder.build() };
    }

    pub fn deinit(self: *Acceptor) void {
        self.tls_context.deinit();
    }
};

/// Bench-server `--self-drive-oneshots` client: verify none, ALPN `h2` only.
/// A separate SSL_CTX from the acceptor, so TlsPump stays the sole owner of
/// the server SSL object. Do not use this on a production connection.
pub const ClientConnector = struct {
    tls_context: boring.ssl.Context,

    pub fn initWithBuilder(builder: *boring.ssl.ContextBuilder) ClientConnector {
        return .{ .tls_context = builder.build() };
    }

    pub fn deinit(self: *ClientConnector) void {
        self.tls_context.deinit();
    }
};

pub fn loopbackClientConnector() !ClientConnector {
    boring.init();
    var builder = try boring.ssl.ContextBuilder.init(boring.ssl.Method.tls());
    errdefer builder.deinit();
    builder.setVerify(boring.ssl.VerifyMode.none);
    try builder.setClientAlpnProtocol(alpn_h2);
    return .initWithBuilder(&builder);
}

fn loadCertificateChain(builder: *boring.ssl.ContextBuilder, pem: []const u8) !void {
    var stack = boring.x509.X509.stackFromPem(pem) catch return error.InvalidCertificate;
    defer stack.deinit();
    const n = stack.len() catch return error.InvalidCertificate;
    if (n == 0) return error.InvalidCertificate;

    const leaf_ref = (stack.get(0) catch return error.InvalidCertificate) orelse
        return error.InvalidCertificate;
    var leaf: boring.x509.X509 = .{ .ptr = leaf_ref.ptr };
    builder.useCertificate(&leaf) catch return error.InvalidCertificate;
    leaf.ptr = null;

    var i: usize = 1;
    while (i < n) : (i += 1) {
        const extra_ref = (stack.get(i) catch return error.InvalidCertificate) orelse
            return error.InvalidCertificate;
        var extra: boring.x509.X509 = .{ .ptr = extra_ref.ptr };
        builder.add1ChainCert(&extra) catch return error.InvalidCertificate;
        extra.ptr = null;
    }
}

/// Per-connection TLS stream. Heap-allocated and never moved.
///
/// Production: the server handshake reads the socket on the handshake
/// subtask (`feedFromSocket`); after it, `Pump` (the CQ driver) is the sole
/// socket reader (via its `ev.NetRecv` completion), the sole SSL owner, and
/// the sole socket writer. The client loopback path uses both directions on
/// one task.
///
/// Bind reader/writer on the task that will park on them: a Reader built on
/// one task is not another task's wait context.
pub const Conn = struct {
    tcp_stream: std.Io.net.Stream = undefined,
    tcp_reader: std.Io.net.Stream.Reader = undefined,
    tcp_writer: std.Io.net.Stream.Writer = undefined,
    tcp_reader_buffer: [stream_buffer_size]u8 = undefined,
    tcp_writer_buffer: [stream_buffer_size]u8 = undefined,
    ssl: boring.ssl.Ssl = .{ .ptr = null },
    pair: boring.ssl.BioPair = .{ .ssl_bio = null, .transport_bio = null },
    /// Alias of the SSL-side BIO so unread INBOUND ciphertext is countable
    /// (BIO_ctrl_pending). Load-bearing: the pump's park condition depends
    /// on it, because SSL_pending counts processed-record plaintext only and
    /// is blind to whole unread records in the pair. Not owned; never deinit.
    ssl_in_bio: boring.ssl.BioPair = .{ .ssl_bio = null, .transport_bio = null },
    state: enum { empty, tcp, tls } = .empty,

    pub fn initTcp(self: *Conn, stream: std.Io.net.Stream) void {
        self.* = .{};
        self.tcp_stream = stream;
        self.state = .tcp;
    }

    /// Client loopback: this task is the ciphertext source. Production
    /// never calls this — the CQ driver reads the raw socket.
    pub fn bindIo(self: *Conn, io: std.Io) void {
        self.bindReader(io);
        self.bindWriter(io);
    }

    pub fn bindReader(self: *Conn, io: std.Io) void {
        self.tcp_reader = self.tcp_stream.reader(io, &self.tcp_reader_buffer);
    }

    pub fn bindWriter(self: *Conn, io: std.Io) void {
        self.tcp_writer = self.tcp_stream.writer(io, &self.tcp_writer_buffer);
    }

    fn attachSsl(self: *Conn, ssl: boring.ssl.Ssl) !void {
        var owned = ssl;
        errdefer owned.deinit();
        var pair = try boring.ssl.BioPair.init(stream_buffer_size);
        errdefer pair.deinit();
        const ssl_bio = pair.ssl_bio orelse return error.TlsHandshakeFailed;
        owned.setBio(ssl_bio);
        self.ssl_in_bio = .{ .ssl_bio = null, .transport_bio = ssl_bio };
        pair.ssl_bio = null;
        self.ssl = owned;
        self.pair = pair;
        self.state = .tls;
    }

    pub fn setupAccept(self: *Conn, acceptor: *Acceptor) !void {
        std.debug.assert(self.state == .tcp);
        var ssl = try acceptor.tls_context.createSsl();
        ssl.setAcceptState();
        try self.attachSsl(ssl);
    }

    pub fn setupConnect(self: *Conn, connector: *ClientConnector) !void {
        std.debug.assert(self.state == .tcp);
        var ssl = try connector.tls_context.createSsl();
        try ssl.setConnectHostname("localhost");
        ssl.setConnectState();
        try self.attachSsl(ssl);
    }

    /// Server handshake. This task reads the socket directly
    /// (`feedFromSocket`); no read task exists yet. The reader is UNBUFFERED
    /// on purpose: after the handshake the CQ driver reads the raw socket,
    /// so a buffered Reader here could strand prefetched ciphertext in a
    /// buffer nothing drains again. Ciphertext beyond the handshake (a
    /// pipelined preface) lands in the BIO pair and is SSL_read by
    /// `drainTlsLeftover` on the actor, as before.
    pub fn handshake(self: *Conn, io: std.Io) !void {
        std.debug.assert(self.state == .tls);
        self.tcp_reader = self.tcp_stream.reader(io, &.{});
        self.bindWriter(io);
        var iterations: u32 = 0;
        while (!self.ssl.isHandshakeComplete()) {
            iterations += 1;
            if (iterations > MaxHandshakeIterations) return error.TlsHandshakeFailed;
            self.drainToSocket() catch return error.TlsHandshakeFailed;
            self.ssl.doHandshake() catch |err| switch (err) {
                error.WantRead => {
                    // doHandshake may have produced a flight (ServerHello).
                    // Drain it before parking or the peer never replies.
                    self.drainToSocket() catch return error.TlsHandshakeFailed;
                    if (self.ssl.isHandshakeComplete()) break;
                    self.feedFromSocket() catch return error.TlsHandshakeFailed;
                },
                error.WantWrite => {},
                else => return error.TlsHandshakeFailed,
            };
        }
        self.drainToSocket() catch return error.TlsHandshakeFailed;
        if (!isHttp2Alpn(self.ssl.selectedAlpn())) return error.TlsHandshakeFailed;
    }

    /// Single-task client handshake: this task is the ciphertext source.
    pub fn handshakeClient(self: *Conn, connector: *ClientConnector, io: std.Io) !void {
        self.bindIo(io);
        try self.setupConnect(connector);
        var iterations: u32 = 0;
        while (!self.ssl.isHandshakeComplete()) {
            iterations += 1;
            if (iterations > MaxHandshakeIterations) return error.TlsHandshakeFailed;
            self.drainToSocket() catch return error.TlsHandshakeFailed;
            self.ssl.doHandshake() catch |err| switch (err) {
                error.WantRead => {
                    self.drainToSocket() catch return error.TlsHandshakeFailed;
                    if (self.ssl.isHandshakeComplete()) break;
                    self.feedFromSocket() catch return error.TlsHandshakeFailed;
                },
                error.WantWrite => {},
                else => return error.TlsHandshakeFailed,
            };
        }
        self.drainToSocket() catch return error.TlsHandshakeFailed;
        if (!isHttp2Alpn(self.ssl.selectedAlpn())) return error.TlsHandshakeFailed;
    }

    pub fn deinit(self: *Conn) void {
        switch (self.state) {
            .empty, .tcp => {},
            .tls => {
                self.ssl.deinit();
                self.pair.deinit();
            },
        }
        self.state = .empty;
    }

    /// Blocking plaintext read for the bench-server loopback client.
    pub fn readPlain(self: *Conn, output: []u8) !usize {
        std.debug.assert(self.state == .tls);
        if (output.len == 0) return 0;
        var iterations: u32 = 0;
        while (true) {
            iterations += 1;
            if (iterations > MaxHandshakeIterations) return error.TlsReadFailed;
            const n = self.ssl.read(output) catch |err| switch (err) {
                error.WantRead => {
                    self.drainToSocket() catch return error.TlsReadFailed;
                    self.feedFromSocket() catch |feed_err| switch (feed_err) {
                        error.TlsReadFailed => return 0,
                        else => return error.TlsReadFailed,
                    };
                    continue;
                },
                error.WantWrite => {
                    self.drainToSocket() catch return error.TlsReadFailed;
                    continue;
                },
                error.ZeroReturn => return 0,
                else => return error.TlsReadFailed,
            };
            return n;
        }
    }

    pub fn writePlain(self: *Conn, input: []const u8) !void {
        std.debug.assert(self.state == .tls);
        var off: usize = 0;
        var iterations: u32 = 0;
        while (off < input.len) {
            iterations += 1;
            if (iterations > MaxHandshakeIterations) return error.TlsWriteFailed;
            const n = self.ssl.write(input[off..]) catch |err| switch (err) {
                error.WantRead => {
                    self.drainToSocket() catch return error.TlsWriteFailed;
                    self.feedFromSocket() catch return error.TlsWriteFailed;
                    continue;
                },
                error.WantWrite => {
                    self.drainToSocket() catch return error.TlsWriteFailed;
                    continue;
                },
                else => return error.TlsWriteFailed,
            };
            if (n == 0) return error.TlsWriteFailed;
            off += n;
        }
        self.drainToSocket() catch return error.TlsWriteFailed;
    }

    /// Inbound ciphertext written into the pair that SSL has not consumed
    /// yet. Invisible to `pendingPlaintext` (SSL_pending counts
    /// processed-record plaintext only). The t-866 wedge was the pump
    /// parking while this was non-zero: the client's next pipelined
    /// requests sat as unread records in the pair, nothing ever re-woke the
    /// pump for them, and the connection stopped for good.
    pub fn pendingInboundCiphertext(self: *Conn) usize {
        return self.ssl_in_bio.pending() catch 0;
    }

    /// Diag: outbound ciphertext SSL wrote that has not been drained to the
    /// socket.
    pub fn pendingOutboundCiphertext(self: *Conn) usize {
        return self.pair.pending() catch 0;
    }

    /// The pump's park predicate for the inbound direction: work exists if
    /// SSL holds decrypted bytes OR the pair holds unread records. One
    /// implementation for both pump gate sites, so the two cannot drift;
    /// the in-process record test pins this exact function.
    pub fn pendingInbound(self: *Conn) bool {
        return self.pendingPlaintext() > 0 or self.pendingInboundCiphertext() > 0;
    }

    pub fn pendingPlaintext(self: *Conn) usize {
        std.debug.assert(self.state == .tls);
        const ref = self.ssl.ref() catch return 0;
        return ref.pending();
    }

    pub fn drainToSocket(self: *Conn) !void {
        var buf: [stream_buffer_size]u8 = undefined;
        while (true) {
            const n = self.pair.readEncrypted(&buf) catch |err| switch (err) {
                error.WantRead => break,
                else => return error.TlsWriteFailed,
            };
            if (n == 0) break;
            self.tcp_writer.interface.writeAll(buf[0..n]) catch return error.TlsWriteFailed;
        }
        self.tcp_writer.interface.flush() catch return error.TlsWriteFailed;
    }

    fn feedFromSocket(self: *Conn) !void {
        var dest_buf: [stream_buffer_size]u8 = undefined;
        var dest: [1][]u8 = .{&dest_buf};
        const n = self.tcp_reader.interface.readVec(&dest) catch return error.TlsReadFailed;
        if (n == 0) return error.TlsReadFailed;
        var off: usize = 0;
        while (off < n) {
            const w = self.pair.writeEncrypted(dest_buf[off..n]) catch |err| switch (err) {
                error.WantWrite => {
                    self.drainToSocket() catch return error.TlsWriteFailed;
                    continue;
                },
                else => return error.TlsReadFailed,
            };
            if (w == 0) return error.TlsReadFailed;
            off += w;
        }
    }
};

/// One task owns SSL_read and SSL_write. Concurrent pumps on one SSL object
/// are a data race; this is the share-nothing owner.
///
/// The pump is a `zio.CompletionQueue` driver. Inbound ciphertext is a raw
/// `ev.NetRecv` completion into ONE read buffer, re-submitted only when the
/// buffer is fully fed into the BIO pair (the pair's bound plus this buffer
/// is the inbound backpressure). Outbound frames arrive on a
/// `zio.Channel(WireChunk)`. The idle wait is one select over both; nothing
/// is reset, so no publish can be missed. Socket writes stay on the driver
/// so the SSE event path does not gain a hop.
///
/// The socket write is a raw `ev.NetSend` completion on the SAME queue, not
/// a blocking writer: the driver stages ciphertext from the BIO pair into
/// `send_buf` and submits it; the completion is one more CQ wake. The driver
/// therefore never parks on the socket. When the staging buffer is full
/// behind an in-flight send, the unfinished write batch stays in
/// `pending_writes` (with the SSL_write offset of the partial chunk) and the
/// driver waits on the CQ only; the send completion compacts, re-arms, and
/// the batch resumes. That is the backpressure path, exactly where the old
/// `writeAll` used to block. A chunk is acked once its record is written
/// (in the pair or the staging buffer): the same hand-off semantics as
/// before, one 16 KB buffer earlier.
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
    /// Plaintext already SSL_read, waiting for the actor to ingest. Aliases
    /// `plain_buf`. Must not drop: the cipher has advanced.
    pending_read: ?wire_pump.WireChunk = null,
    /// Unconsumed suffix of `recv_buf`, stashed when the BIO pair was full
    /// and `pending_read` blocked SSL_read. The recv op is not re-armed
    /// while this is set, so the bytes cannot be overwritten.
    pending_cipher: ?[]const u8 = null,
    /// SSL_read destination. Aliases the first `WIRE_CHUNK_SIZE` of
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
    /// `conn.tcp_writer_buffer`: the blocking writer is handshake-only once
    /// `run()` starts, so the buffer costs nothing new and the memory ceiling
    /// is unchanged. `[send_head..send_fill)` is staged; the in-flight send
    /// covers a prefix of it. `send_op` is rebuilt per submit (the slice
    /// changes); it is only touched after the CQ handed it back.
    send_op: zio.ev.NetSend = undefined,
    send_iov: [1]zio.os.iovec_const = undefined,
    send_buf: []u8 = &.{},
    send_head: usize = 0,
    send_fill: usize = 0,
    send_armed: bool = false,
    /// A write batch that could not finish because the staging buffer is full
    /// behind an in-flight send. `partial_off` is the plaintext offset SSL_write
    /// already accepted for `pending_writes[0]` (SSL_write retries must pass the
    /// same buffer, and they do: the chunk bytes do not move). Nothing new is
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

    const Stage = enum { ok, full, exit };

    /// Move ciphertext from the BIO pair into the staging buffer and (re)arm
    /// the send completion. `.full`: the pair still holds records and the
    /// buffer cannot take them until the in-flight send completes; the caller
    /// parks on the CQ, never on the socket. `.exit`: CQ closed (teardown).
    fn stageOutbound(self: *Pump) Stage {
        while (self.conn.pendingOutboundCiphertext() > 0) {
            if (self.send_fill == self.send_buf.len) {
                if (self.send_head > 0 and !self.send_armed) {
                    self.compactSend();
                } else break;
            }
            const n = self.conn.pair.readEncrypted(self.send_buf[self.send_fill..]) catch |err| switch (err) {
                error.WantRead => break,
                else => {
                    if (diag_wait) rawPrint("TLSERR ssl_write {s}\n", .{@errorName(err)});
                    _ = tls_stage_failed.fetchAdd(1, .monotonic);
                    self.failDrain();
                    self.post(.{ .shutdown = true });
                    return .exit;
                },
            };
            if (n == 0) break;
            self.send_fill += n;
        }
        if (!self.armSend()) return .exit;
        if (self.conn.pendingOutboundCiphertext() > 0 and self.send_fill == self.send_buf.len) return .full;
        return .ok;
    }

    /// Like `stageOutbound` for callers that only need to know whether the
    /// connection is still alive; `.full` is tolerated (the records stay in
    /// the pair and the next send completion stages them).
    fn stageOrExit(self: *Pump) bool {
        return self.stageOutbound() != .exit;
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
        self.cq.submit(&self.send_op.c) catch |err| switch (err) {
            error.Closed => return false,
            error.InvalidCompletion => unreachable,
        };
        self.send_armed = true;
        return true;
    }

    /// The send completion fired: advance, compact, stage what the pair still
    /// holds, re-arm. A short send is just a smaller advance.
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
        return switch (self.stageOutbound()) {
            .exit => .exit,
            .ok, .full => .ok,
        };
    }

    const Feed = enum { done, blocked, eof };

    /// Write ciphertext into the pair. `consumed` is updated on every path.
    /// `.blocked` means inbound cannot make progress (pending plaintext the
    /// actor has not taken, or no read-pool index). Caller must stash the
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
            const n = self.conn.pair.writeEncrypted(bytes[consumed.*..]) catch |err| switch (err) {
                error.WantWrite => {
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
                    continue;
                },
                else => {
                    if (diag_wait) rawPrint("TLSERR recv {s}\n", .{@errorName(err)});
                    self.failDrain();
                    self.post(.{ .shutdown = true });
                    return .eof;
                },
            };
            if (n == 0) {
                if (diag_wait) rawPrint("TLSERR recv_eof\n", .{});
                self.failDrain();
                self.post(.{ .shutdown = true });
                return .eof;
            }
            consumed.* += n;
        }
        return .done;
    }

    pub fn tryTakeWrite(self: *Pump) ?wire_pump.WireChunk {
        const chunk = self.write_ch.tryReceive() catch return null;
        pump_trace.bump(&pump_trace.tryget_write);
        return chunk;
    }

    /// True when one more `pushWriteChunk` would return WriteFailed.
    /// `carried` holds the first overflow chunk; `write_ch` holds the rest.
    pub fn stashFull(self: *Pump) bool {
        return self.carried != null and self.write_ch.isFull();
    }

    fn stealWrite(self: *Pump) void {
        if (self.carried != null) return;
        self.carried = self.tryTakeWrite();
    }

    const WriteSome = enum { done, full, exit };

    /// SSL_write `bytes` from `self.partial_off` on. `.full`: the staging
    /// buffer is full behind an in-flight send and `partial_off` holds the
    /// progress; the caller keeps the chunk pending and parks on the CQ.
    fn sslWriteSome(self: *Pump, bytes: []const u8) WriteSome {
        var spins: u32 = 0;
        while (self.partial_off < bytes.len) {
            spins += 1;
            if (spins > MaxHandshakeIterations) {
                _ = tls_stage_failed.fetchAdd(1, .monotonic);
                if (diag_wait) rawPrint("TLSERR sslwrite_spins\n", .{});
                self.failDrain();
                self.post(.{ .shutdown = true });
                return .exit;
            }
            const n = self.conn.ssl.write(bytes[self.partial_off..]) catch |err| switch (err) {
                error.WantWrite => {
                    switch (self.stageOutbound()) {
                        .exit => return .exit,
                        .full => return .full,
                        .ok => continue,
                    }
                },
                error.WantRead => {
                    switch (self.readOne()) {
                        .eof => return .exit,
                        .ok => continue,
                        .stuck => return .full,
                        .want => {
                            if (self.pending_cipher != null) {
                                if (!self.consumeInbound()) return .exit;
                                continue;
                            }
                            switch (self.pollRecv()) {
                                .progress => continue,
                                .exit => return .exit,
                                .none => return .full,
                            }
                        },
                    }
                },
                else => {
                    if (diag_wait) rawPrint("TLSERR stage {s}\n", .{@errorName(err)});
                    _ = tls_stage_failed.fetchAdd(1, .monotonic);
                    self.failDrain();
                    self.post(.{ .shutdown = true });
                    return .exit;
                },
            };
            self.partial_off += n;
        }
        self.partial_off = 0;
        return .done;
    }

    fn consumeInbound(self: *Pump) bool {
        if (self.pending_cipher) |bytes| {
            self.pending_cipher = null;
            return self.ingestCipher(bytes);
        }
        switch (self.pollRecv()) {
            .progress => return true,
            .exit => return false,
            .none => {},
        }
        return true;
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

    /// SSL_write the pending batch in order; ack each chunk as its record is
    /// written; stop (keep the rest pending) when the staging buffer is full
    /// behind an in-flight send. Returns false on exit.
    pub fn drivePending(self: *Pump) bool {
        while (self.pending_n > 0) {
            const chunk = self.pending_writes[0];
            if (chunk.len == 0 and chunk.bytes.len == 0) {
                // A flush barrier: everything before it must be staged.
                std.debug.assert(chunk.flush_barrier);
                switch (self.stageOutbound()) {
                    .exit => {
                        self.failPending();
                        return false;
                    },
                    .full => return true,
                    .ok => {},
                }
                self.popPending();
                self.releaseChunk(chunk, true, false);
                continue;
            }
            switch (self.sslWriteSome(chunk.bytes[0..chunk.len])) {
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
        // Batch end: move the records into the staging buffer and arm the
        // send. `.full` here is fine: the rest stages on the next completion.
        return self.stageOutbound() != .exit;
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
        const n = self.conn.ssl.read(buf) catch |err| switch (err) {
            error.WantRead => {
                pump_trace.bump(&pump_trace.want_read);
                return .want;
            },
            error.WantWrite => {
                if (!self.stageOrExit()) return .eof;
                return .want;
            },
            else => {
                self.conn.tcp_stream.shutdown(self.io, .send) catch {};
                if (diag_wait) rawPrint("TLSERR recv2 {s}\n", .{@errorName(err)});
                self.failDrain();
                self.postEof();
                return .eof;
            },
        };
        if (n == 0) {
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
                // Fully in the pair; the read buffer is free again, but
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
        self.send_buf = self.conn.tcp_writer_buffer[0..];
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

/// Move ciphertext `from` one pair `to` the other until the source is dry
/// or the destination is full. Returns bytes moved.
fn shuttle(from: *boring.ssl.BioPair, to: *boring.ssl.BioPair) usize {
    var moved: usize = 0;
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = from.readEncrypted(&buf) catch break;
        if (n == 0) break;
        var off: usize = 0;
        while (off < n) {
            const w = to.writeEncrypted(buf[off..n]) catch return moved;
            if (w == 0) return moved;
            off += w;
        }
        moved += n;
    }
    return moved;
}

fn stepHandshake(ssl: *boring.ssl.Ssl) !void {
    ssl.doHandshake() catch |err| switch (err) {
        error.WantRead, error.WantWrite => {},
        else => return err,
    };
}

// The t-866 regression test: two application records fed in ONE chunk.
// SSL_pending is blind to the second record after the first read - only
// BIO_ctrl_pending sees it. The pump's park predicate (pendingInbound)
// must report work, or the pump parks on top of a buried request and the
// connection wedges for good. Removing the ciphertext term from
// pendingInbound fails this test.
// In-source loopback fixture pair for the record tests. Deliberately NOT
// testdata/*.pem: those are machine-local by convention (gitignored), and a
// unit test must build on a fresh checkout. Throwaway self-signed
// CN=localhost material, public by nature.
const fixture_cert_pem =
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
const fixture_key_pem =
    \\-----BEGIN PRIVATE KEY-----
    \\MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgjpKRKa9ZDVtHcAgb
    \\EwexTKPP66fnsfBsAyqcQoT4aKmhRANCAAQ/jAoyoGpnHnj4nuQnL10xyb4uzHpy
    \\K3nhJ3h4IDOvq7GiVaLwzWnc6k6bs4pEy6eTM5Nhs8JCNP00FHSxbz0Q
    \\-----END PRIVATE KEY-----
;

test "a second record in one chunk is invisible to SSL_pending but pendingInbound sees it" {
    boring.init();

    var acceptor = try Acceptor.initFromPem(fixture_cert_pem, fixture_key_pem);
    defer acceptor.deinit();
    var connector = try loopbackClientConnector();
    defer connector.deinit();

    var server: Conn = .{};
    server.state = .tcp;
    try server.setupAccept(&acceptor);
    defer server.ssl.deinit();
    defer server.pair.deinit();

    var client: Conn = .{};
    client.state = .tcp;
    try client.setupConnect(&connector);
    defer client.ssl.deinit();
    defer client.pair.deinit();

    // In-memory handshake: alternate handshake steps and ciphertext
    // shuttling between the two pairs. No sockets anywhere.
    var iterations: u32 = 0;
    while (!(server.ssl.isHandshakeComplete() and client.ssl.isHandshakeComplete())) {
        iterations += 1;
        try std.testing.expect(iterations <= MaxHandshakeIterations);
        try stepHandshake(&client.ssl);
        _ = shuttle(&client.pair, &server.pair);
        try stepHandshake(&server.ssl);
        _ = shuttle(&server.pair, &client.pair);
    }
    try std.testing.expect(isHttp2Alpn(server.ssl.selectedAlpn()));

    // The client writes TWO application records; their ciphertext arrives
    // at the server as ONE chunk, like a burst read off the socket.
    const record_a = "first-record-payload";
    const record_b = "second-record-payload";
    try std.testing.expectEqual(record_a.len, try client.ssl.write(record_a));
    try std.testing.expectEqual(record_b.len, try client.ssl.write(record_b));
    try std.testing.expect(shuttle(&client.pair, &server.pair) > 0);

    // One read consumes record A only.
    var plain: [256]u8 = undefined;
    const got_a = try server.ssl.read(&plain);
    try std.testing.expectEqualStrings(record_a, plain[0..got_a]);

    // The wedge's exact state: SSL_pending reports nothing at the record
    // boundary while a whole unread record sits in the pair. The park
    // predicate must still report inbound work.
    try std.testing.expectEqual(@as(usize, 0), server.pendingPlaintext());
    try std.testing.expect(server.pendingInboundCiphertext() > 0);
    try std.testing.expect(server.pendingInbound());

    // The second read drains it; only then may the pump park.
    const got_b = try server.ssl.read(&plain);
    try std.testing.expectEqualStrings(record_b, plain[0..got_b]);
    try std.testing.expect(!server.pendingInbound());
}

test "h2 ALPN matcher" {
    try std.testing.expect(isHttp2Alpn("h2"));
    try std.testing.expect(!isHttp2Alpn("http/1.1"));
    try std.testing.expect(!isHttp2Alpn(null));
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

test "memory BIO pair is bounded and opposite directions do not mix" {
    var pair = try boring.ssl.BioPair.init(256);
    defer pair.deinit();
    const wrote = try pair.writeEncrypted("hello");
    try std.testing.expectEqual(@as(usize, 5), wrote);
    // writeEncrypted feeds the SSL half; readEncrypted is the SSL-outbound
    // half and stays empty until SSL_write.
    var buf: [8]u8 = undefined;
    try std.testing.expectError(error.WantRead, pair.readEncrypted(&buf));
}
