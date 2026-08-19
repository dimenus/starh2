//! TLS as a `std.Io` stream (BoringSSL SSL_read / SSL_write).
//!
//! HTTP/2 never sees ciphertext. The acceptor owns the SSL_CTX and key
//! material; each connection owns one `Conn` whose reader and writer are
//! plaintext. `Pump` is the sole task that calls SSL_read/SSL_write, so the
//! cipher is not shared with the actor and is not covered by `session_mu`.
//!
//! A transforming writer whose `flush` is SSL_write is the grain. Do not put
//! a record loop back on the actor.
const std = @import("std");
const boring = @import("boring");
const limits_mod = @import("../core/wire_const.zig");
const io_queue = @import("io_queue.zig");
const tls_async = @import("tls_async.zig");
const wire_pump = @import("wire_pump.zig");

/// Same gate as `connection.test_observe`: Debug (the suite) and
/// `-Dobserve=true` ReleaseFast. Plain ReleaseFast compiles the increments
/// and the `STARH2_PUMPTRACE` print path out.
pub const observe = @import("build_options").observe;

/// Quiet-turn counters for TlsPump. Process-global like `test_observed_*`
/// so `/trace` can read them without a pump pointer. One TLS connection is
/// the bench shape; overlapping pumps would share the totals.
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
                "\"pump_write_chunks\":{d},\"pump_write_chunk_sum\":{d}",
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
            },
        );
    }
};

pub const alpn_h2 = "h2";
pub const stream_buffer_size: usize = tls_async.BioStreamBufferSize;

comptime {
    std.debug.assert(stream_buffer_size >= 16 * 1024);
    std.debug.assert(stream_buffer_size == limits_mod.TLS_STREAM_BUFFER_SIZE);
    std.debug.assert(conn_buffer_bytes == limits_mod.TLS_CONN_BUFFER_BYTES);
}

var alpn_select_ctx: AlpnCtx = .{};

/// Four std.Io buffers on the TLS connection (tcp in/out + tls in/out).
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
    inner: tls_async.AsyncTlsAcceptor,

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

        return .{ .inner = tls_async.AsyncTlsAcceptor.initWithBuilder(&builder) };
    }

    pub fn deinit(self: *Acceptor) void {
        self.inner.deinit();
    }
};

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

/// Per-connection TLS stream. Heap-allocated and never moved: BIO callbacks
/// hold pointers into `tcp_reader` / `tcp_writer`.
pub const Conn = struct {
    tcp_stream: std.Io.net.Stream = undefined,
    tcp_reader: std.Io.net.Stream.Reader = undefined,
    tcp_writer: std.Io.net.Stream.Writer = undefined,
    tls_stream: tls_async.AsyncTlsStream = undefined,
    tls_reader: tls_async.AsyncTlsStream.Reader = undefined,
    tls_writer: tls_async.AsyncTlsStream.Writer = undefined,
    tcp_reader_buffer: [stream_buffer_size]u8 = undefined,
    tcp_writer_buffer: [stream_buffer_size]u8 = undefined,
    tls_reader_buffer: [stream_buffer_size]u8 = undefined,
    tls_writer_buffer: [stream_buffer_size]u8 = undefined,
    state: enum { empty, tcp, tls } = .empty,

    pub fn initTcp(self: *Conn, io: std.Io, stream: std.Io.net.Stream) void {
        self.* = .{};
        self.tcp_stream = stream;
        self.tcp_reader = stream.reader(io, &self.tcp_reader_buffer);
        self.tcp_writer = stream.writer(io, &self.tcp_writer_buffer);
        self.state = .tcp;
    }

    pub fn handshake(self: *Conn, acceptor: *Acceptor, io: std.Io) !void {
        std.debug.assert(self.state == .tcp);
        self.tls_stream = acceptor.inner.accept(
            io,
            &self.tcp_reader.interface,
            &self.tcp_writer.interface,
        ) catch return error.TlsHandshakeFailed;
        self.state = .tls;
        self.tls_reader = self.tls_stream.reader(&self.tls_reader_buffer);
        self.tls_writer = self.tls_stream.writer(&self.tls_writer_buffer);
        if (!isHttp2Alpn(self.tls_stream.selectedAlpn())) return error.TlsHandshakeFailed;
    }

    pub fn deinit(self: *Conn) void {
        switch (self.state) {
            .empty => {},
            .tcp => {},
            .tls => self.tls_stream.deinit(),
        }
        self.state = .empty;
    }

    pub fn reader(self: *Conn) *std.Io.Reader {
        std.debug.assert(self.state == .tls);
        return &self.tls_reader.interface;
    }

    pub fn writer(self: *Conn) *std.Io.Writer {
        std.debug.assert(self.state == .tls);
        return &self.tls_writer.interface;
    }

    pub fn pendingPlaintext(self: *Conn) usize {
        std.debug.assert(self.state == .tls);
        const ref = self.tls_stream.sslRef() catch return 0;
        return ref.pending();
    }

    pub fn ciphertextBuffered(self: *const Conn) usize {
        return self.tcp_reader.interface.bufferedLen();
    }
};

/// One task owns SSL_read and SSL_write. Concurrent pumps on one SSL object
/// are a data race; this is the share-nothing owner.
///
/// Writes are preferred so a quiet peer cannot park SSE behind a blocked
/// SSL_read. BIO_read returns WANT_READ when the TCP buffer is empty, so
/// SSL_read itself does not wait. When both sides are idle the pump Selects
/// the write queue against a socket peek — never against SSL_read — so
/// cancelling the wait cannot leave the cipher mid-record. The write waiter
/// returns a WireChunk; the losing result is recovered with `Select.cancel`,
/// never `cancelDiscard`.
pub const Pump = struct {
    io: std.Io,
    conn: *Conn,
    to_actor: *std.Io.Queue(wire_pump.WireChunk),
    from_actor: *std.Io.Queue(wire_pump.WireChunk),
    completions: *std.Io.Queue(wire_pump.WriteCompletion),
    actor_wake: *std.Io.Event,
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
    /// Plaintext already SSL_read, waiting for a read_ch slot. Must not drop:
    /// the cipher has advanced.
    pending_read: ?wire_pump.WireChunk = null,

    fn post(self: *Pump, c: wire_pump.WriteCompletion) void {
        self.completions.putOneUncancelable(self.io, c) catch {};
        self.actor_wake.set(self.io);
    }

    fn returnReadIndex(self: *Pump, idx: u32) void {
        self.read_free.putOneUncancelable(self.io, idx) catch {};
    }

    fn postEof(self: *Pump) void {
        self.to_actor.putOneUncancelable(self.io, .{ .bytes = &.{}, .len = 0 }) catch {};
        self.actor_wake.set(self.io);
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
            if (chunk.pool_index) |idx| self.returnReadIndex(idx);
        }
        while (io_queue.tryGet(wire_pump.WireChunk, self.from_actor, self.io)) |chunk| {
            if (isSentinel(chunk)) continue;
            self.releaseChunk(chunk, false, false);
        }
        self.post(.{ .fail_all = true });
    }

    fn writeChunks(self: *Pump, writer: *std.Io.Writer, first: wire_pump.WireChunk) bool {
        if (first.len == 0 and first.bytes.len == 0) {
            if (first.flush_barrier) {
                pump_trace.bump(&pump_trace.write_chunks);
                pump_trace.add(&pump_trace.write_chunk_sum, 1);
                writer.flush() catch {
                    self.releaseChunk(first, false, false);
                    self.failDrain();
                    self.post(.{ .shutdown = true });
                    return false;
                };
                self.releaseChunk(first, true, false);
                return true;
            }
            self.failDrain();
            self.post(.{ .shutdown = true });
            return false;
        }

        const max_batch = 16;
        var chunks: [max_batch]wire_pump.WireChunk = undefined;
        var slices: [max_batch][]const u8 = undefined;
        chunks[0] = first;
        var count: usize = 1;
        if (self.test_delay_ms == 0 and self.test_fail_after == 0) {
            while (count < max_batch) {
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
        for (chunks[0..count], 0..) |chunk, i| {
            slices[i] = chunk.bytes[0..chunk.len];
        }
        if (wire_pump.write_trace.enabled) wire_pump.write_trace.note(count);
        const write_result = if (count == 1)
            writer.writeAll(slices[0])
        else
            writer.writeVecAll(slices[0..count]);
        write_result catch {
            for (chunks[0..count]) |chunk| self.releaseChunk(chunk, false, false);
            self.failDrain();
            self.post(.{ .shutdown = true });
            return false;
        };
        writer.flush() catch {
            for (chunks[0..count]) |chunk| self.releaseChunk(chunk, false, false);
            self.failDrain();
            self.post(.{ .shutdown = true });
            return false;
        };
        self.writes_done += count;
        for (chunks[0..count]) |chunk| self.releaseChunk(chunk, true, false);
        return true;
    }

    const ReadOutcome = enum { ok, eof, want };

    fn readOne(self: *Pump) ReadOutcome {
        pump_trace.bump(&pump_trace.read_one);
        const chunk_size = limits_mod.WIRE_CHUNK_SIZE;
        // Never park on the actor's queues. TlsPump is the only writer, so a
        // blocking getOne(read_free) / putOne(read_ch) deadlocks mixed SSE:
        // the actor fills write_ch while we wait here, then both sides wait.
        const idx = io_queue.tryGet(u32, self.read_free, self.io) orelse {
            pump_trace.bump(&pump_trace.read_free_empty_yield);
            self.io.sleep(.zero, .awake) catch {};
            return .ok;
        };
        const off = @as(usize, idx) * chunk_size;
        const buf = self.chunk_storage[off..][0..chunk_size];
        const n = self.conn.tls_stream.read(buf) catch |err| switch (err) {
            error.WantRead => {
                pump_trace.bump(&pump_trace.want_read);
                self.returnReadIndex(idx);
                return .want;
            },
            error.WantWrite => {
                self.returnReadIndex(idx);
                return .want;
            },
            else => {
                self.returnReadIndex(idx);
                self.conn.tcp_stream.shutdown(self.io, .send) catch {};
                self.failDrain();
                self.postEof();
                return .eof;
            },
        };
        if (n == 0) {
            self.returnReadIndex(idx);
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
        self.actor_wake.set(self.io);
        if (self.live_task_handlers.load(.acquire) > 0) {
            pump_trace.bump(&pump_trace.live_handler_yield);
            self.io.sleep(.zero, .awake) catch {};
        }
        return .ok;
    }

    const Wait = union(enum) {
        write: (std.Io.QueueClosedError || std.Io.Cancelable)!wire_pump.WireChunk,
        peek: std.Io.Reader.Error![]u8,
    };

    fn waitWrite(queue: *std.Io.Queue(wire_pump.WireChunk), io: std.Io) (std.Io.QueueClosedError || std.Io.Cancelable)!wire_pump.WireChunk {
        return queue.getOne(io);
    }

    fn waitPeek(reader: *std.Io.Reader) std.Io.Reader.Error![]u8 {
        return reader.peekGreedy(1);
    }

    const SelectOutcome = struct {
        write: ?wire_pump.WireChunk = null,
        write_closed: bool = false,
        peek_err: bool = false,
    };

    /// `Select.cancelDiscard` closes the result queue *before* joining the
    /// waiters. A completed `waitWrite` then hits `error.Closed` and the
    /// WireChunk is dropped — outbound_held stays reserved until deinit
    /// panics. `cancel` joins first, then returns the leftover result.
    fn applyWait(out: *SelectOutcome, wait: Wait) void {
        switch (wait) {
            .write => |result| {
                const chunk = result catch |err| switch (err) {
                    error.Canceled => return,
                    error.Closed => {
                        out.write_closed = true;
                        return;
                    },
                };
                std.debug.assert(out.write == null);
                out.write = chunk;
            },
            .peek => |result| {
                if (result) |_| {} else |_| {
                    out.peek_err = true;
                }
            },
        }
    }

    fn collectSelect(select: *std.Io.Select(Wait), first: ?Wait) SelectOutcome {
        var out: SelectOutcome = .{};
        if (first) |wait| applyWait(&out, wait);
        while (select.cancel()) |wait| applyWait(&out, wait);
        return out;
    }

    pub fn run(self: *Pump) void {
        const writer = self.conn.writer();
        while (!self.stopped.load(.acquire)) {
            pump_trace.bump(&pump_trace.turns);
            if (self.carried) |chunk| {
                self.carried = null;
                if (!self.writeChunks(writer, chunk)) return;
                continue;
            }
            if (self.pending_read) |chunk| {
                if (io_queue.tryPut(wire_pump.WireChunk, self.to_actor, self.io, chunk)) {
                    self.pending_read = null;
                    self.actor_wake.set(self.io);
                } else {
                    if (io_queue.tryGet(wire_pump.WireChunk, self.from_actor, self.io)) |w| {
                        pump_trace.bump(&pump_trace.tryget_write);
                        if (!self.writeChunks(writer, w)) return;
                    } else {
                        pump_trace.bump(&pump_trace.pending_read_retry);
                        self.io.sleep(.zero, .awake) catch {};
                    }
                    continue;
                }
            }
            if (io_queue.tryGet(wire_pump.WireChunk, self.from_actor, self.io)) |chunk| {
                pump_trace.bump(&pump_trace.tryget_write);
                if (!self.writeChunks(writer, chunk)) return;
                continue;
            }
            if (self.conn.pendingPlaintext() > 0 or self.conn.ciphertextBuffered() > 0) {
                switch (self.readOne()) {
                    .eof => return,
                    .ok => continue,
                    // BIO had no byte. Do not retry SSL_read; Select so a
                    // write can run while we wait for the next TCP byte.
                    .want => {},
                }
            }

            pump_trace.bump(&pump_trace.select);
            var result_buf: [2]Wait = undefined;
            var select = std.Io.Select(Wait).init(self.io, &result_buf);
            select.concurrent(.write, waitWrite, .{ self.from_actor, self.io }) catch {
                _ = collectSelect(&select, null);
                self.failDrain();
                self.post(.{ .fail_all = true, .shutdown = true });
                return;
            };
            select.concurrent(.peek, waitPeek, .{&self.conn.tcp_reader.interface}) catch {
                const lost = collectSelect(&select, null);
                if (lost.write) |chunk| self.releaseChunk(chunk, false, false);
                self.failDrain();
                self.post(.{ .fail_all = true, .shutdown = true });
                return;
            };
            const selected = select.await() catch {
                const lost = collectSelect(&select, null);
                if (lost.write) |chunk| self.releaseChunk(chunk, false, false);
                self.failDrain();
                self.post(.{ .shutdown = true });
                return;
            };
            const out = collectSelect(&select, selected);
            // A cancelled peek returns ReadFailed. That is not peer EOF when
            // write won — ignore the loser. When peek won, a recovered write
            // chunk is still preferred so SSE is not parked behind SSL_read.
            switch (selected) {
                .write => {
                    pump_trace.bump(&pump_trace.select_write);
                    if (out.write_closed and out.write == null) {
                        self.failDrain();
                        self.post(.{ .fail_all = true, .shutdown = true });
                        return;
                    }
                    if (out.write) |chunk| {
                        if (!self.writeChunks(writer, chunk)) return;
                    }
                },
                .peek => {
                    pump_trace.bump(&pump_trace.select_peek);
                    if (out.write) |chunk| {
                        if (!self.writeChunks(writer, chunk)) return;
                    }
                    if (out.peek_err) {
                        self.failDrain();
                        self.postEof();
                        self.post(.{ .shutdown = true });
                        return;
                    }
                    switch (self.readOne()) {
                        .eof => return,
                        .ok, .want => {},
                    }
                },
            }
        }
        writer.flush() catch {};
        self.failDrain();
        self.post(.{ .shutdown = true });
    }

    pub fn stop(self: *Pump) void {
        self.stopped.store(true, .release);
    }
};

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
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

    const y0 = pump_trace.read_free_empty_yield.load(.acquire);
    const r0 = pump_trace.read_one.load(.acquire);
    try std.testing.expectEqual(Pump.ReadOutcome.ok, pump.readOne());
    if (comptime observe) {
        try std.testing.expect(pump_trace.read_one.load(.acquire) >= r0 + 1);
        try std.testing.expect(pump_trace.read_free_empty_yield.load(.acquire) >= y0 + 1);
    }
}
