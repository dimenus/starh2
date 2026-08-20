//! TLS via memory BIOs. HTTP/2 never sees ciphertext.
//!
//! The SSL object has exactly one owner (`Pump`). Its BIOs are a bounded
//! `BIO_new_bio_pair`, not socket-coupled callbacks: ciphertext bytes move
//! through a queue, not through a readiness edge. A dedicated `CipherRead`
//! task does blocking zio socket reads and posts chunks to the cipher
//! queue. Outbound frames use `write_ch`. The pump `tryGet`s both, then
//! waits on `wake` — no `std.Io.Select`. CipherRead and the actor store a
//! dirty flag before `wake.set`; the pump swaps it after `reset`. An event
//! edge does not survive reset; the flag does. It must service both sides every
//! turn: writes-only would starve oneshot HEADERS under live SSE, and a
//! `continue` past a blocked cipher ingest would skip SETTINGS still on
//! `write_ch` while the actor is parked on `putOne`.
//!
//! Socket writes stay on the pump. A third write task would add a hop on the
//! SSE event path (handler → scheduler → pump SSL_write → socket), which the
//! brief forbids. nats.zig's two-task count is the reference; BoringSSL cannot
//! split the cipher, so the pump owns both directions of SSL and the socket
//! write, matching h2c's task count (read + write).
const std = @import("std");
const boring = @import("boring");
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
/// zero. Bump sites for those three are gone.
pub const pump_trace = struct {
    pub var turns: std.atomic.Value(u64) = .init(0);
    pub var select: std.atomic.Value(u64) = .init(0);
    pub var select_write: std.atomic.Value(u64) = .init(0);
    pub var select_peek: std.atomic.Value(u64) = .init(0);
    pub var tryget_write: std.atomic.Value(u64) = .init(0);
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
                "\"pump_dirty_skip_wait\":{d}",
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
            },
        );
    }
};

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

/// Ciphertext chunk posted by `CipherRead`. The slice is a view into the
/// connection's cipher pool; ownership of `pool_index` moves with the item.
pub const CipherChunk = struct {
    bytes: []u8 = &.{},
    len: usize = 0,
    pool_index: u32 = 0,
};

/// Cipher-queue item. `CipherRead` posts `.cipher` / `.eof`. Outbound
/// frames go on `write_ch`, not here: a unified FIFO HOL-blocked writes
/// behind inbound (oneshot-only rps=0). `.wire` remains so fail-drain
/// and tests can still name the variant.
pub const PumpWork = union(enum) {
    cipher: CipherChunk,
    wire: wire_pump.WireChunk,
    eof,
};

/// Per-connection TLS stream. Heap-allocated and never moved.
///
/// Production: `CipherRead` is the sole socket reader; `Pump` is the sole
/// SSL owner and the sole socket writer. The client loopback path uses both
/// directions on one task.
///
/// Do not build a `Reader` on the actor. A Reader constructed there
/// registers EVFILT_READ on the actor's wait context; Darwin kqueue EV_CLEAR
/// then starves `CipherRead` (the ClientHello never arrives, handshake
/// times out). Bind reader/writer on the task that will park on them.
pub const Conn = struct {
    tcp_stream: std.Io.net.Stream = undefined,
    tcp_reader: std.Io.net.Stream.Reader = undefined,
    tcp_writer: std.Io.net.Stream.Writer = undefined,
    tcp_reader_buffer: [stream_buffer_size]u8 = undefined,
    tcp_writer_buffer: [stream_buffer_size]u8 = undefined,
    ssl: boring.ssl.Ssl = .{ .ptr = null },
    pair: boring.ssl.BioPair = .{ .ssl_bio = null, .transport_bio = null },
    state: enum { empty, tcp, tls } = .empty,

    pub fn initTcp(self: *Conn, stream: std.Io.net.Stream) void {
        self.* = .{};
        self.tcp_stream = stream;
        self.state = .tcp;
    }

    /// Client loopback: this task is the ciphertext source. Production
    /// never calls this — `CipherRead` owns the reader.
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

    /// Server handshake. Ciphertext comes from `work` (the read task is
    /// already running). Recycles cipher indices onto `cipher_free`.
    pub fn handshake(
        self: *Conn,
        io: std.Io,
        work: *std.Io.Queue(PumpWork),
        cipher_free: *std.Io.Queue(u32),
    ) !void {
        std.debug.assert(self.state == .tls);
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
                    const item = work.getOne(io) catch return error.TlsHandshakeFailed;
                    switch (item) {
                        .cipher => |chunk| {
                            defer recycleCipher(io, cipher_free, chunk.pool_index);
                            self.feedCipher(chunk.bytes[0..chunk.len]) catch
                                return error.TlsHandshakeFailed;
                        },
                        .eof, .wire => return error.TlsHandshakeFailed,
                    }
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

    fn feedCipher(self: *Conn, bytes: []const u8) !void {
        var off: usize = 0;
        var spins: u32 = 0;
        while (off < bytes.len) {
            spins += 1;
            if (spins > MaxHandshakeIterations) return error.TlsHandshakeFailed;
            const n = self.pair.writeEncrypted(bytes[off..]) catch |err| switch (err) {
                error.WantWrite => {
                    // Pair full of inbound ciphertext SSL has not consumed.
                    self.ssl.doHandshake() catch |hs_err| switch (hs_err) {
                        error.WantRead, error.WantWrite => {},
                        else => return error.TlsHandshakeFailed,
                    };
                    self.drainToSocket() catch return error.TlsHandshakeFailed;
                    continue;
                },
                else => return error.TlsHandshakeFailed,
            };
            if (n == 0) return error.TlsHandshakeFailed;
            off += n;
        }
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

fn recycleCipher(io: std.Io, free: *std.Io.Queue(u32), idx: u32) void {
    free.putOneUncancelable(io, idx) catch {
        // Queue closure means connection teardown owns the backing pool.
    };
}

/// Blocking socket reader. Never touches the SSL object.
///
/// Parks on the cipher free list, then on a zio stream read, then on the
/// work queue put. Backpressure is the free list emptying — same shape as
/// h2c `ReadPump`. The stream reader is created on this task: a Reader
/// built on the actor is not the read-task's wait context.
pub const CipherRead = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
    work: *std.Io.Queue(PumpWork),
    wake: *std.Io.Event,
    /// Survives `wake.reset()`. CipherRead and the actor store true after
    /// publishing to `work` / `write_ch`, then `set`. The pump swaps it after
    /// reset; a true skip-wait is the lost-wakeup that mixed oneshot hit
    /// when the event edge was eaten with a chunk already queued.
    dirty: *std.atomic.Value(bool),
    chunk_storage: []u8,
    n_chunks: u32,
    free_indices: *std.Io.Queue(u32),
    stopped: std.atomic.Value(bool) = .init(false),
    /// Diag: 0=not started, 1=running, 2=waiting for a free index,
    /// 3=in the socket read, 4=posting to the work queue, 5=exited.
    site: ?*std.atomic.Value(u8) = null,
    task_h: ?*std.atomic.Value(usize) = null,
    task_handle_fn: ?*const fn () usize = null,

    fn diagSite(self: *CipherRead, v: u8) void {
        if (self.site) |a| a.store(v, .release);
    }

    fn returnIndex(self: *CipherRead, idx: u32) void {
        recycleCipher(self.io, self.free_indices, idx);
    }

    fn postEof(self: *CipherRead) void {
        self.work.putOneUncancelable(self.io, .eof) catch {
            // A closed work queue means the connection is already tearing down.
        };
        self.dirty.store(true, .release);
        self.wake.set(self.io);
    }

    pub fn run(self: *CipherRead) void {
        if (self.task_handle_fn) |f| {
            if (self.task_h) |h| h.store(f(), .release);
        }
        defer self.diagSite(5);
        var reader = self.stream.reader(self.io, &.{});
        while (!self.stopped.load(.acquire)) {
            self.diagSite(2);
            const idx = self.free_indices.getOne(self.io) catch return;
            const off = @as(usize, idx) * cipher_chunk_size;
            const buf = self.chunk_storage[off..][0..cipher_chunk_size];
            var dest: [1][]u8 = .{buf};
            self.diagSite(3);
            const n = reader.interface.readVec(&dest) catch {
                self.returnIndex(idx);
                self.stream.shutdown(self.io, .send) catch {};
                self.postEof();
                return;
            };
            if (n == 0) {
                self.returnIndex(idx);
                self.stream.shutdown(self.io, .send) catch {};
                self.postEof();
                return;
            }
            self.diagSite(4);
            self.work.putOne(self.io, .{ .cipher = .{
                .bytes = buf,
                .len = n,
                .pool_index = idx,
            } }) catch {
                self.returnIndex(idx);
                return;
            };
            self.dirty.store(true, .release);
            self.wake.set(self.io);
            self.diagSite(1);
        }
    }

    pub fn stop(self: *CipherRead) void {
        self.stopped.store(true, .release);
    }
};

/// One task owns SSL_read and SSL_write. Concurrent pumps on one SSL object
/// are a data race; this is the share-nothing owner.
///
/// Ciphertext and wire use separate queues so inbound cannot HOL-block a
/// complete-handler write. The wait is `wake.wait` after tryGet on both
/// (no Select). Socket writes stay on the pump so the SSE event path does
/// not gain a hop.
pub const Pump = struct {
    io: std.Io,
    conn: *Conn,
    to_actor: *std.Io.Queue(wire_pump.WireChunk),
    from_actor: *std.Io.Queue(wire_pump.WireChunk),
    work: *std.Io.Queue(PumpWork),
    wake: *std.Io.Event,
    /// Same cell CipherRead and `pushWriteChunk` store. Swap after `wake.reset`.
    dirty: *std.atomic.Value(bool),
    /// Diag: where this pump currently is. 0=not started, 1=running,
    /// 2=wake.wait, 3=writeChunks, 4=cipher ingest, 5=exited. The actor's
    /// park snapshot prints it, so a wedge names the pump's blocking site
    /// without a coroutine stack.
    site: *std.atomic.Value(u8),
    /// Diag: this pump's opaque zio task handle, published at run() start.
    task_h: ?*std.atomic.Value(usize) = null,
    task_handle_fn: ?*const fn () usize = null,
    /// Pump stores true after a successful `read_ch` post; the actor swaps
    /// after `actor_wake.reset` so a read chunk cannot sit while the actor waits.
    read_dirty: *std.atomic.Value(bool),
    completions: *std.Io.Queue(wire_pump.WriteCompletion),
    actor_wake: *std.Io.Event,
    gpa: std.mem.Allocator,
    chunk_storage: []u8,
    n_chunks: u32,
    read_free: *std.Io.Queue(u32),
    write_free: ?*std.Io.Queue(u32) = null,
    cipher_free: *std.Io.Queue(u32),
    live_task_handlers: *std.atomic.Value(usize),
    stopped: std.atomic.Value(bool) = .init(false),
    test_delay_ms: u64 = 0,
    test_fail_after: u64 = 0,
    writes_done: u64 = 0,
    /// Empty flush/sentinel pulled off the write queue while gathering a
    /// DATA batch. WritePump parks the same item in `carried`; dropping it
    /// here leaked tickets and, for a flush with outbound_release, held bytes.
    carried: ?wire_pump.WireChunk = null,
    /// Plaintext already SSL_read, waiting for a read_ch slot. Must not drop:
    /// the cipher has advanced.
    pending_read: ?wire_pump.WireChunk = null,
    /// Ciphertext dequeued but not fully BIO_write'd, because inbound was
    /// full and `pending_read` blocked SSL_read. Off the work queue so a
    /// `.wire` behind it can unblock the actor.
    pending_cipher: ?CipherChunk = null,

    fn post(self: *Pump, c: wire_pump.WriteCompletion) void {
        self.completions.putOneUncancelable(self.io, c) catch {};
        self.notifyActor();
    }

    fn notifyActor(self: *Pump) void {
        self.read_dirty.store(true, .release);
        self.actor_wake.set(self.io);
    }

    fn returnReadIndex(self: *Pump, idx: u32) void {
        self.read_free.putOneUncancelable(self.io, idx) catch {};
    }

    fn postEof(self: *Pump) void {
        self.to_actor.putOneUncancelable(self.io, .{ .bytes = &.{}, .len = 0 }) catch {};
        self.notifyActor();
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

    fn failDrain(self: *Pump) void {
        if (self.carried) |chunk| {
            self.carried = null;
            if (!isSentinel(chunk)) self.releaseChunk(chunk, false, false);
        }
        if (self.pending_read) |chunk| {
            self.pending_read = null;
            if (chunk.pool_index) |idx| {
                self.returnReadIndex(idx);
            } else if (chunk.bytes.len != 0) {
                self.gpa.free(chunk.bytes);
            }
        }
        if (self.pending_cipher) |chunk| {
            self.pending_cipher = null;
            recycleCipher(self.io, self.cipher_free, chunk.pool_index);
        }
        while (io_queue.tryGet(PumpWork, self.work, self.io)) |item| {
            switch (item) {
                .wire => |chunk| {
                    if (isSentinel(chunk)) continue;
                    self.releaseChunk(chunk, false, false);
                },
                .cipher => |chunk| recycleCipher(self.io, self.cipher_free, chunk.pool_index),
                .eof => {},
            }
        }
        while (io_queue.tryGet(wire_pump.WireChunk, self.from_actor, self.io)) |chunk| {
            if (isSentinel(chunk)) continue;
            self.releaseChunk(chunk, false, false);
        }
        self.post(.{ .fail_all = true });
    }

    fn drainToSocket(self: *Pump) bool {
        self.conn.drainToSocket() catch {
            self.failDrain();
            self.post(.{ .shutdown = true });
            return false;
        };
        return true;
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
                    if (!self.drainToSocket()) return .eof;
                    continue;
                },
                else => {
                    self.failDrain();
                    self.post(.{ .shutdown = true });
                    return .eof;
                },
            };
            if (n == 0) {
                self.failDrain();
                self.post(.{ .shutdown = true });
                return .eof;
            }
            consumed.* += n;
        }
        return .done;
    }

    fn stealWrite(self: *Pump) void {
        if (self.carried != null) return;
        const chunk = io_queue.tryGet(wire_pump.WireChunk, self.from_actor, self.io) orelse return;
        pump_trace.bump(&pump_trace.tryget_write);
        self.carried = chunk;
    }

    fn sslWriteAll(self: *Pump, bytes: []const u8) bool {
        var off: usize = 0;
        var spins: u32 = 0;
        while (off < bytes.len) {
            spins += 1;
            if (spins > MaxHandshakeIterations) {
                self.failDrain();
                self.post(.{ .shutdown = true });
                return false;
            }
            const n = self.conn.ssl.write(bytes[off..]) catch |err| switch (err) {
                error.WantWrite => {
                    if (!self.drainToSocket()) return false;
                    continue;
                },
                error.WantRead => {
                    switch (self.readOne()) {
                        .eof => return false,
                        .ok => continue,
                        .stuck => {
                            self.stealWrite();
                            self.site.store(9, .release);
                            self.io.sleep(.zero, .awake) catch {};
                            self.site.store(3, .release);
                            continue;
                        },
                        .want => {
                            if (!self.consumeInbound()) return false;
                            continue;
                        },
                    }
                },
                else => {
                    self.failDrain();
                    self.post(.{ .shutdown = true });
                    return false;
                },
            };
            if (n == 0) {
                self.failDrain();
                self.post(.{ .shutdown = true });
                return false;
            }
            off += n;
        }
        return true;
    }

    /// SSL_write wanted inbound records. Cipher queue only. Never park on
    /// `getOne`: the actor may be in `write_ch.putOne` and CipherRead may need
    /// this executor. A miss yields; `sslWriteAll` retries until a chunk
    /// arrives or MaxHandshakeIterations failDrains.
    fn consumeInbound(self: *Pump) bool {
        if (self.pending_cipher) |chunk| {
            self.pending_cipher = null;
            return self.ingestCipher(chunk);
        }
        if (io_queue.tryGet(PumpWork, self.work, self.io)) |item| {
            pump_trace.bump(&pump_trace.work_get);
            return self.ingestWork(item);
        }
        self.stealWrite();
        self.io.sleep(.zero, .awake) catch {};
        return true;
    }

    /// Cipher-queue item while SSL_write wants a record. Never SSL_write here
    /// (OpenSSL rejects a different buffer until the current SSL_write retries).
    fn ingestWork(self: *Pump, item: PumpWork) bool {
        switch (item) {
            .cipher => |chunk| return self.ingestCipher(chunk),
            .wire => |chunk| {
                pump_trace.bump(&pump_trace.tryget_write);
                if (self.carried == null) {
                    self.carried = chunk;
                    return true;
                }
                self.releaseChunk(chunk, false, false);
                self.failDrain();
                self.post(.{ .shutdown = true });
                return false;
            },
            .eof => {
                self.failDrain();
                self.postEof();
                self.post(.{ .shutdown = true });
                return false;
            },
        }
    }

    fn writeChunks(self: *Pump, first: wire_pump.WireChunk) bool {
        if (first.len == 0 and first.bytes.len == 0) {
            if (first.flush_barrier) {
                pump_trace.bump(&pump_trace.write_chunks);
                pump_trace.add(&pump_trace.write_chunk_sum, 1);
                if (!self.drainToSocket()) {
                    self.releaseChunk(first, false, false);
                    return false;
                }
                self.releaseChunk(first, true, false);
                return true;
            }
            self.failDrain();
            self.post(.{ .shutdown = true });
            return false;
        }

        const max_batch = 16;
        var chunks: [max_batch]wire_pump.WireChunk = undefined;
        chunks[0] = first;
        var count: usize = 1;
        if (self.test_delay_ms == 0 and self.test_fail_after == 0) {
            while (count < max_batch) {
                if (self.pending_cipher != null) break;
                const next = io_queue.tryGet(wire_pump.WireChunk, self.from_actor, self.io) orelse break;
                if (next.len == 0 and next.bytes.len == 0) {
                    self.carried = next;
                    break;
                }
                chunks[count] = next;
                count += 1;
            }
        }
        if (self.test_delay_ms > 0) {
            self.io.sleep(.fromMilliseconds(@intCast(self.test_delay_ms)), .awake) catch {
                self.releaseChunk(first, false, false);
                self.failDrain();
                self.post(.{ .shutdown = true });
                return false;
            };
        }
        const fail_next = wire_pump.test_fail_next_write.swap(false, .acq_rel);
        if (fail_next or (self.test_fail_after > 0 and self.writes_done >= self.test_fail_after)) {
            for (chunks[0..count]) |chunk| self.releaseChunk(chunk, false, false);
            self.failDrain();
            self.post(.{ .shutdown = true });
            return false;
        }
        pump_trace.bump(&pump_trace.write_chunks);
        pump_trace.add(&pump_trace.write_chunk_sum, count);
        if (wire_pump.write_trace.enabled) wire_pump.write_trace.note(count);
        for (chunks[0..count]) |chunk| {
            if (!self.sslWriteAll(chunk.bytes[0..chunk.len])) {
                for (chunks[0..count]) |c| self.releaseChunk(c, false, false);
                return false;
            }
        }
        if (!self.drainToSocket()) {
            for (chunks[0..count]) |c| self.releaseChunk(c, false, false);
            return false;
        }
        self.writes_done += count;
        for (chunks[0..count]) |chunk| self.releaseChunk(chunk, true, false);
        return true;
    }

    const ReadOutcome = enum { ok, eof, want, stuck };

    fn readOne(self: *Pump) ReadOutcome {
        pump_trace.bump(&pump_trace.read_one);
        if (self.pending_read != null) return .stuck;
        const chunk_size = limits_mod.WIRE_CHUNK_SIZE;
        // Never park on the actor's queues. TlsPump is the only writer, so a
        // blocking getOne(read_free) / putOne(read_ch) deadlocks mixed SSE:
        // the actor fills the work queue while we wait here, then both sides wait.
        const pooled = io_queue.tryGet(u32, self.read_free, self.io);
        const idx: ?u32 = pooled;
        const buf: []u8 = if (pooled) |i| blk: {
            const off = @as(usize, i) * chunk_size;
            break :blk self.chunk_storage[off..][0..chunk_size];
        } else blk: {
            pump_trace.bump(&pump_trace.read_free_empty_yield);
            if (self.n_chunks == 0) {
                self.io.sleep(.zero, .awake) catch {};
                return .stuck;
            }
            // read_ch capacity equals the pool, so a full queue means no
            // index. SSL_write of SETTINGS still needs SSL_read (TLS 1.3
            // tickets; pipelined HEADERS) to drain the inbound BIO. One
            // GPA chunk, actor-recycled like any non-pooled read. At most
            // one lives in pending_read.
            break :blk self.gpa.alloc(u8, chunk_size) catch {
                self.io.sleep(.zero, .awake) catch {};
                return .stuck;
            };
        };
        const n = self.conn.ssl.read(buf) catch |err| switch (err) {
            error.WantRead => {
                pump_trace.bump(&pump_trace.want_read);
                if (idx) |i| self.returnReadIndex(i) else self.gpa.free(buf);
                return .want;
            },
            error.WantWrite => {
                if (idx) |i| self.returnReadIndex(i) else self.gpa.free(buf);
                if (!self.drainToSocket()) return .eof;
                return .want;
            },
            else => {
                if (idx) |i| self.returnReadIndex(i) else self.gpa.free(buf);
                self.conn.tcp_stream.shutdown(self.io, .send) catch {};
                self.failDrain();
                self.postEof();
                return .eof;
            },
        };
        if (n == 0) {
            if (idx) |i| self.returnReadIndex(i) else self.gpa.free(buf);
            self.conn.tcp_stream.shutdown(self.io, .send) catch {};
            self.failDrain();
            self.postEof();
            return .eof;
        }
        if (!io_queue.tryPut(wire_pump.WireChunk, self.to_actor, self.io, .{
            .bytes = buf,
            .len = n,
            .pool_index = idx,
        })) {
            std.debug.assert(self.pending_read == null);
            self.pending_read = .{ .bytes = buf, .len = n, .pool_index = idx };
            return .ok;
        }
        self.notifyActor();
        if (self.live_task_handlers.load(.acquire) > 0) {
            pump_trace.bump(&pump_trace.live_handler_yield);
            self.site.store(11, .release);
            self.io.sleep(.zero, .awake) catch {};
            self.site.store(1, .release);
        }
        return .ok;
    }

    fn ingestCipher(self: *Pump, chunk: CipherChunk) bool {
        pump_trace.bump(&pump_trace.cipher_chunks);
        const all = chunk.bytes[0..chunk.len];
        var consumed: usize = 0;
        switch (self.feedCipher(all, &consumed)) {
            .eof => {
                recycleCipher(self.io, self.cipher_free, chunk.pool_index);
                return false;
            },
            .blocked => {
                if (consumed == all.len) {
                    recycleCipher(self.io, self.cipher_free, chunk.pool_index);
                    return true;
                }
                std.debug.assert(self.pending_cipher == null);
                self.pending_cipher = .{
                    .bytes = chunk.bytes[consumed..],
                    .len = all.len - consumed,
                    .pool_index = chunk.pool_index,
                };
                return true;
            },
            .done => recycleCipher(self.io, self.cipher_free, chunk.pool_index),
        }
        if (self.pending_read != null) return true;
        switch (self.readOne()) {
            .eof => return false,
            .ok, .want, .stuck => return true,
        }
    }

    pub fn run(self: *Pump) void {
        // Recreate the writer on this task. Handshake bound one on the
        // handshake Select arm; that wait context is gone.
        self.conn.bindWriter(self.io);
        if (self.task_handle_fn) |f| {
            if (self.task_h) |h| h.store(f(), .release);
        }
        defer self.site.store(5, .release);
        while (!self.stopped.load(.acquire)) {
            self.site.store(1, .release);
            pump_trace.bump(&pump_trace.turns);
            var progress = false;

            if (self.carried) |chunk| {
                self.carried = null;
                self.site.store(3, .release);
                if (!self.writeChunks(chunk)) return;
                self.site.store(1, .release);
                progress = true;
            }

            if (self.pending_read) |chunk| {
                if (io_queue.tryPut(wire_pump.WireChunk, self.to_actor, self.io, chunk)) {
                    self.pending_read = null;
                    self.notifyActor();
                    progress = true;
                }
            }

            // Ingest cipher only when SSL_read can store plaintext. Otherwise
            // fall through and take writes: the actor may be parked on putOne
            // of the SETTINGS that unstick the client.
            if (self.pending_cipher != null and self.pending_read == null) {
                const chunk = self.pending_cipher.?;
                self.pending_cipher = null;
                self.site.store(4, .release);
                if (!self.ingestCipher(chunk)) return;
                self.site.store(1, .release);
                progress = true;
            }

            if (self.pending_read == null and self.conn.pendingPlaintext() > 0) {
                switch (self.readOne()) {
                    .eof => return,
                    .ok => progress = true,
                    .want, .stuck => {},
                }
            }

            if (io_queue.tryGet(wire_pump.WireChunk, self.from_actor, self.io)) |chunk| {
                pump_trace.bump(&pump_trace.tryget_write);
                self.site.store(3, .release);
                if (!self.writeChunks(chunk)) return;
                self.site.store(1, .release);
                progress = true;
            }
            if (self.pending_cipher == null) {
                if (io_queue.tryGet(PumpWork, self.work, self.io)) |item| {
                    pump_trace.bump(&pump_trace.work_get);
                    self.site.store(4, .release);
                    if (!self.dispatch(item)) return;
                    self.site.store(1, .release);
                    progress = true;
                }
            }

            if (progress) continue;

            self.wake.reset();
            if (io_queue.tryGet(wire_pump.WireChunk, self.from_actor, self.io)) |chunk| {
                pump_trace.bump(&pump_trace.tryget_write);
                self.site.store(3, .release);
                if (!self.writeChunks(chunk)) return;
                continue;
            }
            if (self.pending_cipher == null) {
                if (io_queue.tryGet(PumpWork, self.work, self.io)) |item| {
                    pump_trace.bump(&pump_trace.work_get);
                    self.site.store(4, .release);
                    if (!self.dispatch(item)) return;
                    self.site.store(1, .release);
                    continue;
                }
            }
            if (self.pending_read != null or self.pending_cipher != null) {
                pump_trace.bump(&pump_trace.pending_read_retry);
                self.site.store(10, .release);
                self.io.sleep(.zero, .awake) catch {};
                self.site.store(1, .release);
                continue;
            }
            if (self.pending_read == null and self.conn.pendingPlaintext() > 0) continue;
            if (self.dirtyForbidsWait()) continue;
            if (diag_wait) self.traceWaitSnapshot();
            self.site.store(2, .release);
            defer self.site.store(1, .release);
            self.wake.wait(self.io) catch {
                self.failDrain();
                self.post(.{ .fail_all = true, .shutdown = true });
                return;
            };
        }
        _ = self.drainToSocket();
        self.failDrain();
        self.post(.{ .shutdown = true });
    }

    fn dispatch(self: *Pump, item: PumpWork) bool {
        switch (item) {
            .cipher => |chunk| return self.ingestCipher(chunk),
            .wire => |chunk| {
                pump_trace.bump(&pump_trace.tryget_write);
                return self.writeChunks(chunk);
            },
            .eof => {
                self.failDrain();
                self.postEof();
                self.post(.{ .shutdown = true });
                return false;
            },
        }
    }

    pub fn stop(self: *Pump) void {
        self.stopped.store(true, .release);
    }

    /// After `wake.reset` and empty tryGets. A producer stores this before
    /// `set`; the event edge does not survive reset, the flag does.
    fn dirtyForbidsWait(self: *Pump) bool {
        if (self.dirty.swap(false, .acq_rel)) {
            pump_trace.bump(&pump_trace.dirty_skip_wait);
            return true;
        }
        return false;
    }

    fn traceWaitSnapshot(self: *Pump) void {
        const now = nowNs(self.io);
        if (now -% diag_last_wait_ns < std.time.ns_per_s) return;
        diag_last_wait_ns = now;
        const write_q = io_queue.queued(wire_pump.WireChunk, self.from_actor, self.io);
        const work_q = io_queue.queued(PumpWork, self.work, self.io);
        const read_q = io_queue.queued(wire_pump.WireChunk, self.to_actor, self.io);
        const read_free_q = io_queue.queued(u32, self.read_free, self.io);
        const cipher_free_q = io_queue.queued(u32, self.cipher_free, self.io);
        const write_free_q: usize = if (self.write_free) |q| io_queue.queued(u32, q, self.io) else 0;
        rawPrint(
            "STARH2_PUMPWAIT write_q={d} work_q={d} read_q={d} read_free={d} write_free={d} cipher_free={d} carried={} pending_read={} pending_cipher={} plaintext={d} wake={s}\n",
            .{
                write_q,
                work_q,
                read_q,
                read_free_q,
                write_free_q,
                cipher_free_q,
                self.carried != null,
                self.pending_read != null,
                self.pending_cipher != null,
                self.conn.pendingPlaintext(),
                @tagName(self.wake.*),
            },
        );
    }
};

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

test "TlsPump dirty flag survives Event.reset and forbids wait" {
    var dirty = std.atomic.Value(bool).init(false);
    var wake: std.Io.Event = .unset;
    const io = std.testing.io;
    dirty.store(true, .release);
    wake.set(io);
    wake.reset();
    try std.testing.expect(!wake.isSet());

    var pump: Pump = undefined;
    pump.dirty = &dirty;
    try std.testing.expect(pump.dirtyForbidsWait());
    try std.testing.expect(!pump.dirtyForbidsWait());
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

test "PumpWork fits a queue slot and is not smaller than a WireChunk" {
    try std.testing.expect(@sizeOf(PumpWork) >= @sizeOf(wire_pump.WireChunk));
}
