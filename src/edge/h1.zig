//! HTTP/1.1 edge channel. One request at a time, no Session, no FairScheduler.
//!
//! The connection task owns the socket (h1c) or the TLS `Conn` (ALPN `http/1.1`
//! / no ALPN). It accumulates a head into a buffer of its own and parses
//! once, and reads a Content-Length body into another. `TaskHandler.
//! stream_request` starts after the head with an empty body; unread
//! Content-Length forces close so leftover bytes are not the next request.
//! The response uses one framing (Content-Length, chunked, or
//! close-delimited). Keep-alive loops until a close reason.
//!
//! # A handler reaches only its link (t-3759)
//!
//! At dispatch the request (method, authority, path, query, headers, body)
//! is copied into a request arena from the server's `RequestPool`, and the
//! request's link (`H1Link`) is that arena's first allocation. The link
//! embeds `connection.LinkCore`, the core HTTP/2's `StreamLink` uses: the
//! gate, the two references, the terminal cause, the task-handler token and
//! the orphan count. The connection's head, body and scratch buffers are
//! never handed to a handler. Every `Response` call enters the connection
//! through the link's gate; once the gate is closed it fails with the cause.
//!
//! A complete handler runs on the actor. A task handler runs on its own task,
//! and the actor waits in `waitDispatch` for its report, for the peer, for
//! the shutdown, and (TLS) for the handler's writes, which the actor drives.
//!
//! Teardown does not wait for handler code (`endHandler`): it sets the
//! cause, wakes every wait a call inside the gate can be in (the socket is
//! shut, the TLS write channels are closed, `dead` is set), closes the gate,
//! requests a cancel (never waits for it), and waits only for the calls
//! already inside. A handler that did not report by then is orphaned: its
//! link keeps the server-wide token and the orphan count, so `Server.serve`
//! keeps waiting and reporting, and the connection ends at once.
const std = @import("std");
const slab_pool = @import("slab_pool.zig");
const zio = @import("zio");
const request = @import("../http/request.zig");
const response = @import("../http/response.zig");
const router_mod = @import("../http/router.zig");
const hpack = @import("../core/hpack.zig");
const parser = @import("../http1/parser.zig");
const connection = @import("connection.zig");
const tls_edge = @import("tls.zig");
const request_pool = @import("request_pool.zig");
const accounts = @import("accounts.zig");
const test_hook = @import("test_hook.zig");
const wire_pump = @import("wire_pump.zig");
const limits_mod = @import("../core/wire_const.zig");

/// Request bodies ended by the idle rule and by the minimum rate
/// (`Limits.request_body_min_rate_bytes_per_s`), process-wide. A test reads
/// them to show which rule fired.
pub var body_idle_expiries: std.atomic.Value(u64) = .init(0);
pub var body_rate_expiries: std.atomic.Value(u64) = .init(0);

var no_shutdown_event: zio.Event = .init;

/// Bound on the connection's own task for the whole of `serve`: `get() ==
/// conn` tells the actor from a handler's task (`writeRaw`, `flushSink`). A
/// task-local, not a thread-local: tasks move between threads.
var actor_marker: zio.TaskLocal(*H1Conn) = .{};

fn onActor(self: *H1Conn) bool {
    return actor_marker.get() == self;
}

pub const ChannelMutation = enum {
    none,
    m2_no_chunk_end,
    m3_ignore_close,
    m4_keep_after_400,
    m5_skip_validate,
    m6_buffer_sse,
    m7_grow_head,
};

pub var test_channel_mutation: ChannelMutation = .none;

/// Conformance/smoke process: `STARH2_H1_MUTATION=m2` omits the last chunk.
pub fn applyTestMutationFromEnv() void {
    const raw = std.c.getenv("STARH2_H1_MUTATION") orelse return;
    const v = std.mem.span(raw);
    if (std.mem.eql(u8, v, "m2")) test_channel_mutation = .m2_no_chunk_end;
}

/// After connection setup, a counting GPA should see this stay flat for a
/// request whose head fits `h1_head_bytes` (I4). Tests snapshot it.
pub var test_gpa_allocs: std.atomic.Value(usize) = .init(0);

/// Everything an HTTP/1.1 handler can reach between starh2 calls: the shared
/// link core (`connection.LinkCore`: gate, references, arena, terminal
/// cause, token, orphan count), the request copied into the arena, and the
/// response. It is the first allocation in the request's arena. `conn` is
/// valid only between a successful `enter` and its `exit`. `Response.ctx`
/// points at the link itself.
pub const H1Link = struct {
    core: connection.LinkCore,
    conn: *H1Conn,
    method: request.Method,
    req: request.Request,
    resp: response.Response,
    /// The task route's handler; unused for a complete handler.
    handler: router_mod.TaskHandler = undefined,

    fn fromCtx(ctx: *anyopaque) *H1Link {
        return @ptrCast(@alignCast(ctx));
    }

    /// The connection, for a call that has entered the gate; null once the
    /// connection has ended (or is ending) for this handler.
    fn enter(self: *H1Link) ?*H1Conn {
        if (!self.core.gate.enter()) return null;
        return self.conn;
    }

    fn exit(self: *H1Link) void {
        self.core.gate.exit();
    }
};

const Framing = enum { content_length, chunked, close_delimited };

pub const H1Conn = struct {
    config: connection.ConnConfig,
    stream: std.Io.net.Stream,
    tls: ?*tls_edge.Conn,
    io: std.Io,
    gpa: std.mem.Allocator,

    head_buf: []u8,
    body_buf: []u8,
    recv_buf: []u8,
    write_buf: []u8,
    header_storage: []request.Header,
    field_scratch: []hpack.HeaderField,
    name_scratch: []u8,
    path_q_buf: []u8,
    carry_buf: []u8,
    carry_len: usize = 0,

    /// The socket writer, with the write-byte timeout on every call
    /// (`Limits.write_byte_timeout_ns`, `wire_pump.SocketWriter`): a peer
    /// that takes no byte for the timeout fails the write, and the
    /// connection is shut (`noteWriteFailed`).
    writer: wire_pump.SocketWriter = undefined,
    /// Bytes the socket accepted (`SocketWriter.accepted`).
    socket_bytes: std.atomic.Value(u64) = .init(0),
    socket_bytes_ns: std.atomic.Value(u64) = .init(0),
    /// The write-byte clock of the TLS handler write in flight
    /// (`TlsWriteClock`), read by `waitDispatchTls`.
    tls_write_clock: TlsWriteClock = .{ .seen = 0, .progress_ns = 0 },
    reader_bound: bool = false,

    // Cleartext inbound. One `ev.NetRecv` the actor submits and re-submits,
    // never cancels. A canceled WAIT is free; a canceled READ is what lost a
    // pipelined request twice in t-1760, because every std.Io path that fails
    // a read discards what it already took off the socket. Nothing is reset
    // here and nothing is rolled back, so that class is unsayable. Mirrors
    // `tls_edge.Pump`, which is why the TLS path never carried t-1760.
    cq: zio.CompletionQueue = undefined,
    recv_op: zio.ev.NetRecv = undefined,
    recv_iov: [1]zio.os.iovec = undefined,
    recv_armed: bool = false,
    recv_started: bool = false,
    /// Bytes delivered by the last completion that no caller has taken yet.
    /// Always a slice of `recv_buf`.
    recv_pending: []u8 = &.{},
    recv_eof: bool = false,

    /// Server-wide request memory: every dispatched request is copied into
    /// an arena from here (`newLink`).
    pool: *request_pool.RequestPool,
    /// What this connection's requests may hold of the pool at once
    /// (`Limits.request_bytes_per_connection`). Reached by a handler only
    /// through its link's gate (`request_pool.Arena.gate`).
    request_budget: request_pool.ConnBudget,
    /// The dispatched request's link, from `newLink` until `releaseRequest`
    /// or `endHandler`. The actor holds one reference to it.
    link: ?*H1Link = null,
    /// This connection holds a server-wide stream for the dispatched request
    /// (`GlobalAccounting.tryAdmitStream`), released when the request ends:
    /// at the handler's report, or when the connection ends.
    stream_held: bool = false,
    /// Set once, when the connection ends with a request dispatched
    /// (`endHandler`): a call inside the gate that waits (`waitUntilCb`)
    /// selects on it.
    dead: zio.Event = .init,
    /// The task handler's report: one post, from inside its gate.
    completion_buf: [1]u31 = undefined,
    completion_ch: zio.Channel(u31) = undefined,
    /// The task handler's join handle. Joined after its report, detached
    /// when it is orphaned; never cancelled with a wait.
    join: ?zio.JoinHandle(void) = null,
    /// Written once by `serve` before any spawn; read by `connection.notePlacement`.
    actor_thread: std.Thread.Id = 0,

    acc: parser.Accumulator = undefined,
    head: parser.Head = undefined,
    scheme: []const u8 = "http",

    framing: Framing = .content_length,
    pending_storage: []u8,
    pending_len: usize = 0,
    committed: bool = false,
    occupies: bool = false,
    /// This connection asked its handler to cancel because the server is
    /// shutting down (`GlobalAccounting.shutdown_cancelled`).
    shutdown_cancel_counted: bool = false,
    want_close: bool = false,
    /// The connection is ending before it read all the client sent for the
    /// current request: answered at its head (a refusal, a 413 before the
    /// body, a parse reject), a body that timed out, or a streaming route
    /// whose body the handler may not have read. The close lingers
    /// (`lingerNeeded`).
    input_unread: bool = false,
    finished: bool = false,
    no_payload: bool = false,
    resp_status: u16 = 0,
    socket_closed: bool = false,
    dispatch_headers: []request.Header = &.{},

    /// The TLS transport (`H1Tls`): the pump and everything it borrows. The
    /// actor reaches the pump through `tls_t.running()`.
    tls_t: H1Tls = .{},
    tls_req_buf: [1]u32 = undefined,
    tls_req_ch: zio.Channel(u32) = undefined,
    tls_ok_buf: [1]bool = undefined,
    tls_ok_ch: zio.Channel(bool) = undefined,
    tls_write_inflight: bool = false,

    fn deinit(self: *H1Conn) void {
        if (self.shutdown_cancel_counted) {
            if (self.config.accounting) |a| a.saturatingRelease(&a.shutdown_cancelled, 1, "shutdown_cancelled", .acq_rel);
        }
        std.debug.assert(self.link == null);
        std.debug.assert(!self.stream_held);
        // Every request's connection charge came back: at the release of its
        // link, or at teardown (`LinkCore.returnConnCharges`).
        if (self.request_budget.held.load(.acquire) != 0) {
            std.debug.panic("h1: {d} request bytes still charged to the connection at its end", .{self.request_budget.held.load(.acquire)});
        }
        self.tls_t.close();
        if (self.recv_started) {
            self.cq.close();
            self.cq.cancelAll(.keep);
            while (self.cq.next()) |_| {}
            self.recv_started = false;
        }
        if (!self.socket_closed) {
            self.stream.close(self.io);
            self.socket_closed = true;
        }
        // Poison what a handler must never hold, in safe builds: a request
        // field that still pointed here would read garbage, and the outlive
        // tests (`tests/goaway_delivery.zig`) check what a handler reads
        // after its connection ended. After the receive op is cancelled and
        // drained: it writes into `recv_buf`.
        if (std.debug.runtime_safety) {
            @memset(self.head_buf, 0xaa);
            @memset(self.body_buf, 0xaa);
            @memset(self.recv_buf, 0xaa);
            @memset(self.carry_buf, 0xaa);
            @memset(self.name_scratch, 0xaa);
            @memset(self.path_q_buf, 0xaa);
            @memset(std.mem.sliceAsBytes(self.header_storage), 0xaa);
            @memset(std.mem.sliceAsBytes(self.field_scratch), 0xaa);
        }
        // The caller owns the `Conn`'s memory (a connection slot's buffer).
        if (self.tls) |t| t.deinit();
        self.gpa.free(self.head_buf);
        self.gpa.free(self.body_buf);
        self.gpa.free(self.recv_buf);
        self.gpa.free(self.write_buf);
        self.gpa.free(self.header_storage);
        self.gpa.free(self.field_scratch);
        self.gpa.free(self.name_scratch);
        self.gpa.free(self.path_q_buf);
        self.gpa.free(self.carry_buf);
        self.gpa.free(self.pending_storage);
        self.tls_t.deinit(self.gpa);
        self.* = undefined;
    }
};

pub fn serve(
    stream: std.Io.net.Stream,
    config: connection.ConnConfig,
    tls: ?*tls_edge.Conn,
    leftover: []const u8,
) std.Io.Cancelable!void {
    var conn: H1Conn = undefined;
    initConn(&conn, stream, config, tls) catch {
        if (tls) |t| t.deinit();
        stream.close(config.io);
        return;
    };
    defer conn.deinit();
    var marker: @TypeOf(actor_marker).Node = .unset;
    actor_marker.set(&marker, &conn);
    defer actor_marker.clear(&marker);
    // Runs before `deinit`, on every exit: a handler still running is cut
    // off from the connection (orphaned), never waited for.
    var canceled = false;
    defer endOnExit(&conn, canceled);
    if (comptime connection.placement_check) conn.actor_thread = std.Thread.getCurrentId();
    if (tls != null) startTlsPump(&conn) catch {
        return;
    };
    if (leftover.len != 0) {
        stashCarry(&conn, leftover, 0) catch return;
    }
    runLoop(&conn) catch |err| switch (err) {
        error.Canceled => {
            canceled = true;
            return error.Canceled;
        },
        else => {},
    };
}

fn initConn(self: *H1Conn, stream: std.Io.net.Stream, config: connection.ConnConfig, tls: ?*tls_edge.Conn) !void {
    const gpa = config.gpa;
    const pool = config.request_pool;
    const head_buf = try gpa.alloc(u8, config.limits.h1_head_bytes);
    errdefer gpa.free(head_buf);
    const body_buf = try gpa.alloc(u8, config.limits.request_body_bytes);
    errdefer gpa.free(body_buf);
    const recv_buf = try gpa.alloc(u8, 4096);
    errdefer gpa.free(recv_buf);
    const write_buf = try gpa.alloc(u8, 4096);
    errdefer gpa.free(write_buf);
    const header_storage = try gpa.alloc(request.Header, config.limits.header_fields);
    errdefer gpa.free(header_storage);
    const field_scratch = try gpa.alloc(hpack.HeaderField, config.limits.header_fields + 4);
    errdefer gpa.free(field_scratch);
    const name_scratch = try gpa.alloc(u8, config.limits.h1_head_bytes);
    errdefer gpa.free(name_scratch);
    const path_q_buf = try gpa.alloc(u8, config.limits.path_wire_bytes);
    errdefer gpa.free(path_q_buf);
    // Declared bound: one body plus one pipelined head. Do not grow later (I4).
    const carry_buf = try gpa.alloc(u8, config.limits.h1_head_bytes + config.limits.request_body_bytes);
    errdefer gpa.free(carry_buf);
    const pending_storage = try gpa.alloc(u8, config.limits.outbound_bytes_per_stream);
    errdefer gpa.free(pending_storage);

    self.* = .{
        .config = config,
        .stream = stream,
        .tls = tls,
        .io = config.io,
        .gpa = gpa,
        .head_buf = head_buf,
        .body_buf = body_buf,
        .recv_buf = recv_buf,
        .write_buf = write_buf,
        .header_storage = header_storage,
        .field_scratch = field_scratch,
        .name_scratch = name_scratch,
        .path_q_buf = path_q_buf,
        .carry_buf = carry_buf,
        .pending_storage = pending_storage,
        .pool = pool,
        .request_budget = .init(config.limits.request_bytes_per_connection),
        .scheme = if (tls != null) "https" else "http",
    };
    self.acc = parser.Accumulator.init(self.head_buf);
    self.completion_ch = .init(&self.completion_buf);
    if (tls == null) {
        self.writer = .init(config.io, stream.socket.handle, self.write_buf, &self.socket_bytes, &self.socket_bytes_ns, config.limits.write_byte_timeout_ns);
        self.reader_bound = true;
        startRecv(self);
    }
}

/// Test-only (observe builds): pump failures seen by `h1AckSink`, over all
/// connections, and the most on one connection. A test reads the first to
/// know its attack reached a pump failure, and the second to know no
/// connection went on writing after one.
pub var test_tls_failures: test_hook.Hook(std.atomic.Value(u32)) = if (test_hook.on) .init(0) else {};
pub var test_tls_failures_max: test_hook.Hook(std.atomic.Value(u32)) = if (test_hook.on) .init(0) else {};

/// Every write completion the HTTP/1.1 TLS pump posts (`Pump.post`). The
/// pump reports a failure as completions (`failDrain`'s `fail_all`, then a
/// `shutdown`), and a chunk as one only when it carries a ticket or
/// accounting. HTTP/1.1 chunks carry neither: no tickets (a handler's write
/// waits for the actor's `tls_ok_ch` ack), no outbound-budget release, no
/// control-pool entries, and the write-byte clock reads `Pump.sent_bytes`.
/// The actor learns of a failure from the pump's `.exit`, not from these.
/// So there is nothing to apply, and a completion that does carry
/// accounting is a bug in this file: it would be a release dropped here.
/// HTTP/2 applies the same posts in `Connection.takeWriteAck`. Applied on
/// the posting task, as HTTP/2's are, so no channel and no capacity.
fn h1AckSink(ctx: *anyopaque, c: wire_pump.WriteCompletion) void {
    const self: *H1Tls = @ptrCast(@alignCast(ctx));
    if (comptime test_hook.on) self.applied += 1;
    if (c.shutdown) {
        self.failures += 1;
        if (comptime test_hook.on) {
            _ = test_tls_failures.fetchAdd(1, .monotonic);
            _ = test_tls_failures_max.fetchMax(self.failures, .monotonic);
        }
    }
    if (c.ticket != 0 or c.ticket_count != 0 or c.outbound_release != 0 or c.control_entries != 0 or c.control_release != 0) {
        std.debug.panic("h1: a TLS write completion carries accounting HTTP/1.1 never charges (ticket {d}, outbound {d}, control {d})", .{ c.ticket, c.outbound_release, c.control_entries });
    }
}

/// The transport witness's mutant (`./zb build dst-transport`): the shared
/// constructor leaves the ack sink out, so `Pump.post` falls back to the
/// four-slot `acks` channel that nothing reads, which is the first version
/// of t-3759 step 7. Only the witness's own mutant build sets the option,
/// and `test_hook.on` keeps it out of every release build.
pub const mutant_no_ack_sink = test_hook.on and @import("build_options").dst_mutant == .no_h1_ack_sink;

/// The HTTP/1.1 TLS transport: the pump and everything it borrows (the
/// ciphertext read buffer, the decrypt chunk, the channels the pump's shape
/// carries, the one-chunk write pool), wired to `h1AckSink`. `H1Conn`
/// embeds one, and the transport witness (`tests/dst_tls_lifecycle.zig`)
/// builds one through the same calls, so the wiring the witness checks is
/// this code and not a copy of it:
///
/// 1. `init`, at the struct's final address (the channels point into its
///    buffers, the pump into the struct): allocate and wire. Submits
///    nothing.
/// 2. `arm`: `Pump.start`, which submits the first receive. From here
///    until `close`, `running` returns the pump; that is the actor's only
///    way to reach it.
/// 3. `close` when the connection ends, while the socket is still open:
///    it decides whether there is a running pump to tear down, so the
///    witness's teardown and `H1Conn.deinit`'s are one call.
/// 4. `deinit` frees. It refuses a pump that is still running.
///
/// After a failed `init` or `arm`, `close` does nothing and `deinit` frees
/// what was allocated.
pub const H1Tls = struct {
    /// Allocated by `init`. Reach it through `running`.
    pump: ?*tls_edge.Pump = null,
    /// Set by a successful `arm`, cleared by `close`.
    armed: bool = false,
    recv_buf: []u8 = &.{},
    chunk_storage: []u8 = &.{},
    to_actor_buf: []wire_pump.WireChunk = &.{},
    write_ch_buf: []wire_pump.WireChunk = &.{},
    ack_buf: []wire_pump.WriteCompletion = &.{},
    to_actor: zio.Channel(wire_pump.WireChunk) = undefined,
    write_ch: zio.Channel(wire_pump.WireChunk) = undefined,
    /// The pump's completion channel (`Pump.completions`). With the sink
    /// wired nothing reaches it, and nothing reads it: a completion here is
    /// abandoned. The witness fails on the first one (`UnconsumedTlsAck`).
    acks: zio.Channel(wire_pump.WriteCompletion) = undefined,
    /// One write chunk: slab 0 is `out`, and holding it is the token that
    /// says the pump has not yet finished the previous write.
    write_pool: slab_pool.SlabPool = undefined,
    out: []u8 = &.{},
    site: std.atomic.Value(u8) = .init(0),
    live_handlers: std.atomic.Value(usize) = .init(0),
    /// Pump failures on this connection (shutdown completions the pump
    /// posted, `h1AckSink`). After the first the connection submits nothing
    /// more, so it is at most 1.
    failures: u32 = 0,
    /// Observe builds: the completions `h1AckSink` applied. The witness
    /// reads it to know the sink ran.
    applied: test_hook.Hook(u32) = if (test_hook.on) 0 else {},

    pub fn init(self: *H1Tls, gpa: std.mem.Allocator, io: std.Io, conn: *tls_edge.Conn, sock: zio.ev.Backend.NetHandle) !void {
        self.* = .{};
        self.recv_buf = try gpa.alloc(u8, limits_mod.TLS_CIPHER_CHUNK_SIZE);
        self.chunk_storage = try gpa.alloc(u8, limits_mod.WIRE_CHUNK_SIZE);
        self.to_actor_buf = try gpa.alloc(wire_pump.WireChunk, 1);
        self.write_ch_buf = try gpa.alloc(wire_pump.WireChunk, 4);
        self.ack_buf = try gpa.alloc(wire_pump.WriteCompletion, 4);
        self.write_pool = try slab_pool.SlabPool.init(gpa, io, limits_mod.WIRE_CHUNK_SIZE, 1);
        self.out = self.write_pool.slabAt(0);

        self.to_actor = .init(self.to_actor_buf);
        self.write_ch = .init(self.write_ch_buf);
        self.acks = .init(self.ack_buf);

        const pump = try gpa.create(tls_edge.Pump);
        self.pump = pump;
        pump.* = .{
            .io = io,
            .conn = conn,
            .to_actor = &self.to_actor,
            .write_ch = &self.write_ch,
            .sock = sock,
            .recv_buf = self.recv_buf,
            .site = &self.site,
            .completions = &self.acks,
            .ack_apply = if (comptime mutant_no_ack_sink) null else h1AckSink,
            .ack_ctx = self,
            .gpa = gpa,
            .chunk_storage = self.chunk_storage,
            .n_chunks = 1,
            .write_free = &self.write_pool,
            .live_task_handlers = &self.live_handlers,
        };
    }

    /// Start the pump: its first receive is submitted.
    pub fn arm(self: *H1Tls) error{TlsPumpStartFailed}!void {
        const pump = self.pump.?;
        if (!pump.start()) {
            pump.shutdownCq();
            return error.TlsPumpStartFailed;
        }
        self.armed = true;
    }

    /// The pump, from a successful `arm` until `close`; null otherwise.
    pub fn running(self: *H1Tls) ?*tls_edge.Pump {
        return if (self.armed) self.pump.? else null;
    }

    /// The connection ended. If the pump is running: stop the queue
    /// (cancel and drain what is in flight; nothing more reaches the
    /// pump's handlers), then give back the write chunk the pump still
    /// holds (queued behind a full staging buffer): `deinit` checks that it
    /// came home. A connection that ends under a handler's TLS write can
    /// leave one there (round 1 of the step-7 review found it with the
    /// multi-chunk test). The `fail_all` this posts goes to the sink. Call
    /// it before the socket closes: the cancel names the socket.
    pub fn close(self: *H1Tls) void {
        if (!self.armed) return;
        self.armed = false;
        const pump = self.pump.?;
        pump.shutdownCq();
        pump.failDrain();
    }

    pub fn deinit(self: *H1Tls, gpa: std.mem.Allocator) void {
        if (self.armed) std.debug.panic("h1: H1Tls.deinit with the TLS pump still running; close() it first, while the socket is open", .{});
        if (self.pump) |p| gpa.destroy(p);
        if (self.recv_buf.len != 0) gpa.free(self.recv_buf);
        if (self.chunk_storage.len != 0) gpa.free(self.chunk_storage);
        if (self.to_actor_buf.len != 0) gpa.free(self.to_actor_buf);
        if (self.write_ch_buf.len != 0) gpa.free(self.write_ch_buf);
        if (self.ack_buf.len != 0) gpa.free(self.ack_buf);
        // Checks that the write chunk came home.
        if (self.out.len != 0) self.write_pool.deinit(gpa);
        self.* = .{};
    }
};

fn tlsNetHandle(h: std.Io.net.Socket.Handle) zio.ev.Backend.NetHandle {
    return if (@typeInfo(zio.ev.Backend.NetHandle) == .pointer) @ptrCast(h) else h;
}

fn startTlsPump(self: *H1Conn) !void {
    const t = self.tls orelse return;
    self.tls_req_ch = .init(&self.tls_req_buf);
    self.tls_ok_ch = .init(&self.tls_ok_buf);
    try self.tls_t.init(self.gpa, self.io, t, tlsNetHandle(self.stream.socket.handle));
    try self.tls_t.arm();
}

fn runLoop(self: *H1Conn) !void {
    var keep = true;
    while (keep) {
        if (shutdownRequested(self)) break;
        keep = try serveOne(self);
        if (self.occupies) break;
        if (!keep) break;
        self.acc.reset();
        self.committed = false;
        self.finished = false;
        self.no_payload = false;
        self.resp_status = 0;
        self.framing = .content_length;
        self.pending_len = 0;
    }
    std.debug.assert(self.link == null);
    if (!shutdownRequested(self) and lingerNeeded(self)) linger(self);
    self.stream.shutdown(self.io, .both) catch {};
}

/// Lingering close (RFC 9112 section 9.6). A close with unread input in the
/// socket makes the kernel send a RST, and a RST can destroy the response the
/// client has not read yet (Linux: the close above with input left resets the
/// connection; measured, t-3883). So when the client has, or may have, sent
/// more than was read, the connection first half-closes (the response then
/// ends with a FIN), reads and discards, and closes on the client's EOF, an
/// error, `linger_idle_ns` with no data, or `linger_total_ns` in all. It keeps
/// its slot and its charges meanwhile: it is still a connection. Reads during
/// the linger are discarded, never parsed.
///
/// nginx's shape (`ngx_http_set_lingering_close`: read and discard), Go's
/// time scale (`closeWriteAndWait` waits 500 ms), and shorter than nginx's 5 s
/// idle and 30 s total on purpose: a client that trickles bytes must not hold
/// a slot for long. Fixed: no `Limits` field means the same thing
/// (`close_flush_timeout_ns` bounds our own last write, not the peer's input).
pub const linger_idle_ns: u64 = 500 * std.time.ns_per_ms;
pub const linger_total_ns: u64 = 2 * std.time.ns_per_s;

/// Whether the client has, or may have, input this connection did not read:
/// the current request was not read to its end (`input_unread`, a partial
/// head included), bytes read and not consumed (a pipelined request), or bytes waiting
/// in the socket (FIONREAD; ciphertext on TLS). Not after the client's own
/// end of stream: then nothing more comes.
fn lingerNeeded(self: *H1Conn) bool {
    if (self.tls != null) {
        const pump = self.tls_t.running() orelse return false;
        if (pump.inbound_eof) return false;
        if (pump.pending_read != null or pump.pending_cipher != null) return true;
    } else {
        if (self.recv_eof) return false;
        if (self.recv_pending.len != 0) return true;
    }
    if (self.input_unread or self.carry_len != 0) return true;
    var queued: c_int = 0;
    if (std.c.ioctl(self.stream.socket.handle, fionread, &queued) == 0 and queued > 0) return true;
    return false;
}

/// FIONREAD: Linux's from std; Darwin's is `_IOR('f', 127, int)`, which std
/// does not define for it.
const fionread: c_int = switch (@import("builtin").os.tag) {
    .linux => @intCast(std.os.linux.T.FIONREAD),
    .macos, .ios, .tvos, .watchos, .visionos => 0x4004667f,
    else => @compileError("h1 lingering close: FIONREAD for this OS"),
};

/// Half-close, then read and discard until the client's EOF, an error, or a
/// bound. The response is flushed before this runs (every write path flushes
/// before `serveOne` returns), so the FIN comes after it.
fn linger(self: *H1Conn) void {
    _ = linger_started.fetchAdd(1, .monotonic);
    self.stream.shutdown(self.io, .send) catch return;
    self.carry_len = 0;
    const end_ns = nowNs(self.io) +| linger_total_ns;
    var buf: [4096]u8 = undefined;
    while (true) {
        const now = nowNs(self.io);
        if (now >= end_ns) return;
        const n = (readSome(self, &buf, @min(now +| linger_idle_ns, end_ns)) catch return) orelse return;
        if (n == 0) return;
    }
}

/// HTTP/1.1 closes that lingered, process-wide (tests, an operator).
pub var linger_started: std.atomic.Value(u64) = .init(0);

fn shutdownRequested(self: *H1Conn) bool {
    if (self.config.shutdown_flag) |f| {
        if (f.load(.acquire)) return true;
    }
    return false;
}

fn skipRequestBody(matched: router_mod.Match) bool {
    return switch (matched) {
        .found => |f| switch (f.handler) {
            .task => |t| t.stream_request,
            .complete => false,
        },
        else => false,
    };
}

fn requestBodyCap(matched: router_mod.Match, fallback: usize) usize {
    return switch (matched) {
        .found => |f| f.max_request_body_bytes orelse fallback,
        else => fallback,
    };
}

fn serveOne(self: *H1Conn) !bool {
    const parsed = try readHead(self) orelse {
        // A head that never completed: the client may still be sending it.
        if (self.acc.filled != 0) self.input_unread = true;
        return false;
    };
    switch (parsed) {
        .http09 => {
            return false;
        },
        .reject => |st| {
            self.input_unread = true;
            try writeError(self, st);
            if (test_channel_mutation == .m4_keep_after_400 and st == 400) return true;
            return false;
        },
        .ok => |head| {
            self.head = head;
            var close_after = head.connection_close;
            if (test_channel_mutation == .m3_ignore_close) close_after = false;

            const method = request.Method.parse(head.method_raw);
            const matched = self.config.router.match(method, head.path);
            const body_cap = requestBodyCap(matched, self.config.limits.request_body_bytes);
            if (head.content_length > body_cap) {
                self.input_unread = true;
                try writeError(self, 413);
                return false;
            }

            if (head.expect_100) {
                try writeRaw(self, "HTTP/1.1 100 Continue\r\n\r\n");
                try flushSink(self);
            }

            var body: []const u8 = &.{};
            if (skipRequestBody(matched)) {
                // Streaming contract: handler starts after the head.
                if (head.content_length != 0) {
                    self.want_close = true;
                    self.input_unread = true;
                }
            } else {
                body = readBody(self, head) catch |err| switch (err) {
                    error.BodyTimeout => {
                        // Nothing final was sent (a 100 Continue is interim),
                        // so the client can be told why before the close.
                        self.input_unread = true;
                        writeError(self, 408) catch {};
                        return false;
                    },
                    else => return err,
                } orelse {
                    self.input_unread = true;
                    try writeError(self, 413);
                    return false;
                };
                if (body.len > body_cap) {
                    self.input_unread = true;
                    try writeError(self, 413);
                    return false;
                }
            }

            var headers: []const request.Header = head.headers;
            var path = head.path;
            var query = head.query;
            var authority = head.authority;
            if (test_channel_mutation != .m5_skip_validate) {
                const v = parser.validateAsH2Fields(
                    head,
                    self.scheme,
                    self.field_scratch,
                    self.name_scratch,
                    self.path_q_buf,
                );
                if (v.status != 0) {
                    try writeError(self, v.status);
                    return false;
                }
                const regular_n = if (v.field_n > 4) v.field_n - 4 else 0;
                var i: usize = 0;
                while (i < regular_n) : (i += 1) {
                    const f = self.field_scratch[4 + i];
                    self.header_storage[i] = .{ .name = f.name, .value = f.value };
                }
                self.dispatch_headers = self.header_storage[0..regular_n];
                headers = self.dispatch_headers;
                if (v.path.len != 0) path = v.path;
                query = v.query;
                if (v.authority.len != 0) authority = v.authority;
            }

            try dispatch(self, head, matched, body, headers, path, query, authority);
            if (test_channel_mutation == .m3_ignore_close) self.want_close = false;
            if (self.want_close or close_after) return false;
            return true;
        },
    }
}

const HeadOutcome = union(enum) {
    ok: parser.Head,
    reject: u16,
    http09,
};

fn readHead(self: *H1Conn) !?HeadOutcome {
    const deadline_ns = nowNs(self.io) +% self.config.limits.field_block_timeout_ns;
    while (true) {
        if (self.carry_len != 0) {
            const chunk = self.carry_buf[0..self.carry_len];
            const r = feedHead(self, chunk);
            switch (r) {
                .need_more => {
                    self.carry_len = 0;
                },
                .head => |h| {
                    shiftCarry(self, h.consumed);
                    return parseFilled(self);
                },
                .http09 => return .http09,
                .reject => |rj| {
                    shiftCarry(self, rj.consumed);
                    return .{ .reject = rj.status };
                },
            }
        }
        if (nowNs(self.io) >= deadline_ns) return null;
        var tmp: [4096]u8 = undefined;
        const n = try readSome(self, tmp[0..], deadline_ns) orelse return null;
        if (n == 0) return null;
        const r = feedHead(self, tmp[0..n]);
        switch (r) {
            .need_more => {},
            .head => |h| {
                try stashCarry(self, tmp[0..n], h.consumed);
                return parseFilled(self);
            },
            .http09 => return .http09,
            .reject => |rj| {
                try stashCarry(self, tmp[0..n], rj.consumed);
                return .{ .reject = rj.status };
            },
        }
    }
}

fn feedHead(self: *H1Conn, bytes: []const u8) parser.Accumulator.Feed {
    if (test_channel_mutation == .m7_grow_head) {
        if (self.acc.filled + bytes.len > self.acc.buf.len) {
            const new_buf = self.gpa.alloc(u8, self.acc.buf.len + bytes.len + 1024) catch {
                return .{ .reject = .{ .status = 431, .consumed = 0 } };
            };
            _ = test_gpa_allocs.fetchAdd(1, .monotonic);
            @memcpy(new_buf[0..self.acc.filled], self.acc.buf[0..self.acc.filled]);
            const old = self.head_buf;
            self.head_buf = new_buf;
            self.acc.buf = new_buf;
            self.gpa.free(old);
        }
    }
    return self.acc.feed(bytes);
}

fn parseFilled(self: *H1Conn) HeadOutcome {
    const result = parser.parse(
        self.acc.headBytes(),
        self.header_storage,
        self.field_scratch,
        .{
            .head_bytes = if (test_channel_mutation == .m7_grow_head)
                std.math.maxInt(usize)
            else
                self.config.limits.h1_head_bytes,
            .header_fields = self.config.limits.header_fields,
            .body_bytes = self.config.limits.request_body_bytes,
        },
    );
    return switch (result) {
        .ok => |h| .{ .ok = h },
        .reject => |st| .{ .reject = st },
        .http09 => .http09,
    };
}

fn readBody(self: *H1Conn, head: parser.Head) !?[]const u8 {
    const need: usize = std.math.cast(usize, head.content_length) orelse return null;
    if (need > self.body_buf.len) return null;
    var got: usize = 0;
    if (self.carry_len != 0) {
        const take = @min(self.carry_len, need);
        @memcpy(self.body_buf[0..take], self.carry_buf[0..take]);
        shiftCarry(self, take);
        got = take;
    }
    // Two rules, as on HTTP/2 (`session.BodyClock`), from the end of the head:
    // idle (no byte for `request_body_idle_timeout_ns`, renewed by each byte)
    // and the minimum rate (`start + max(grace, bytes / rate)`). The server
    // reads whenever the body is owed and never withholds anything here, so
    // nothing pauses them.
    const limits = self.config.limits;
    const start_ns = nowNs(self.io);
    var idle_deadline_ns = start_ns +| limits.request_body_idle_timeout_ns;
    while (got < need) {
        const rate_deadline_ns: u64 = if (limits.request_body_min_rate_bytes_per_s == 0)
            std.math.maxInt(u64)
        else
            start_ns +| @max(limits.request_body_min_rate_grace_ns, (@as(u64, got) *| std.time.ns_per_s) / limits.request_body_min_rate_bytes_per_s);
        const deadline_ns = @min(idle_deadline_ns, rate_deadline_ns);
        var tmp: [4096]u8 = undefined;
        const read = if (nowNs(self.io) >= deadline_ns) null else try readSome(self, tmp[0..], deadline_ns);
        const n = read orelse {
            if (deadline_ns == idle_deadline_ns) {
                _ = body_idle_expiries.fetchAdd(1, .monotonic);
            } else {
                _ = body_rate_expiries.fetchAdd(1, .monotonic);
            }
            return error.BodyTimeout;
        };
        if (n == 0) return error.ConnectionClosed;
        const take = @min(n, need - got);
        @memcpy(self.body_buf[got .. got + take], tmp[0..take]);
        got += take;
        idle_deadline_ns = nowNs(self.io) +| limits.request_body_idle_timeout_ns;
        if (take < n) try stashCarry(self, tmp[0..n], take);
    }
    return self.body_buf[0..need];
}

fn shiftCarry(self: *H1Conn, consumed: usize) void {
    if (consumed >= self.carry_len) {
        self.carry_len = 0;
        return;
    }
    const remain = self.carry_len - consumed;
    std.mem.copyForwards(u8, self.carry_buf[0..remain], self.carry_buf[consumed..self.carry_len]);
    self.carry_len = remain;
}

fn stashCarry(self: *H1Conn, chunk: []const u8, consumed: usize) !void {
    if (consumed >= chunk.len) {
        return;
    }
    const rest = chunk[consumed..];
    const new_len = self.carry_len + rest.len;
    if (new_len > self.carry_buf.len) return error.CarryOverflow;
    // Append. A replace drops a pipelined prefix already in carry when the
    // mid-handler wait (or a second TLS ingest) delivers the rest of the next
    // request in a later read.
    @memcpy(self.carry_buf[self.carry_len..][0..rest.len], rest);
    self.carry_len = new_len;
}

fn takeTlsPending(pump: *tls_edge.Pump, buf: []u8) ?usize {
    const chunk = pump.pending_read orelse return null;
    const n = @min(buf.len, chunk.len);
    @memcpy(buf[0..n], chunk.bytes[0..n]);
    if (n < chunk.len) {
        pump.pending_read = .{ .bytes = chunk.bytes[n..], .len = chunk.len - n };
    } else {
        pump.pending_read = null;
    }
    return n;
}

fn driveTlsUntilIdle(self: *H1Conn) response.ResponseError!void {
    const pump = self.tls_t.running() orelse return;
    var clock: TlsWriteClock = .start(self, pump);
    while (pump.pending_n > 0 or pump.send_armed) {
        if (clock.expired(self, pump)) return tlsWriteTimedOut(self);
        const out = waitTlsCq(self, clock.deadline()) catch return error.WriteFailed;
        if (out == .exit) return tlsPumpEnded(self);
        if (pump.pending_n > 0) {
            if (!pump.drivePending()) return tlsPumpEnded(self);
        }
    }
}

/// The pump failed (a send or receive error, a closed queue): nothing more
/// can be written on this connection. The actor's own write fails with the
/// cause, and the connection closes; no further send is submitted.
fn tlsPumpEnded(self: *H1Conn) response.ResponseError {
    self.want_close = true;
    if (self.link) |l| l.core.terminal.setCause(.connection_closed);
    return writeFailedCause(self);
}

/// The write-byte timeout (`Limits.write_byte_timeout_ns`) on the TLS pump:
/// progress is ciphertext the socket accepted (`Pump.sent_bytes`), at every
/// send completion, partial ones included, dated when the socket took it
/// (`Pump.sent_ns`). The limit is read when the clock starts.
const TlsWriteClock = struct {
    seen: u64,
    progress_ns: u64,
    timeout_ns: u64 = 0,

    fn start(self: *H1Conn, pump: *tls_edge.Pump) TlsWriteClock {
        return .{
            .seen = pump.sent_bytes.load(.acquire),
            .progress_ns = nowNs(self.io),
            .timeout_ns = self.config.limits.write_byte_timeout_ns,
        };
    }

    /// Null when the timeout is off.
    fn deadline(c: *const TlsWriteClock) ?u64 {
        if (c.timeout_ns == 0) return null;
        return c.progress_ns +| c.timeout_ns;
    }

    fn expired(c: *TlsWriteClock, self: *H1Conn, pump: *tls_edge.Pump) bool {
        const now = nowNs(self.io);
        const sent = pump.sent_bytes.load(.acquire);
        if (sent != c.seen) {
            c.seen = sent;
            const at = pump.sent_ns.load(.acquire);
            c.progress_ns = if (at == 0) now else @max(c.progress_ns, @min(at, now));
        }
        const d = c.deadline() orelse return false;
        return now >= d;
    }
};

fn tlsWriteTimedOut(self: *H1Conn) response.ResponseError {
    shutTimedOut(self);
    return error.SlowConsumer;
}

fn shutTimedOut(self: *H1Conn) void {
    _ = write_byte_expiries.fetchAdd(1, .monotonic);
    self.stream.shutdown(self.io, .both) catch {};
}

const CqOutcome = enum { more, exit };

/// One turn of the TLS pump's completion queue. `.exit`: the pump has ended
/// (`Pump.onCqComplete` said so, or the queue is drained), and the caller
/// must not submit more.
fn waitTlsCq(self: *H1Conn, deadline_ns: ?u64) error{Canceled}!CqOutcome {
    const pump = self.tls_t.running() orelse return .exit;
    const shutdown_ev = self.config.shutdown_event orelse &no_shutdown_event;
    const timeout: zio.Timeout = if (deadline_ns) |d| blk: {
        const now = nowNs(self.io);
        if (d <= now) return .more;
        break :blk .{ .duration = .fromNanoseconds(d - now) };
    } else .none;
    const winner = zio.select(.{
        .io = &pump.cq,
        .timer = timeout,
        .shutdown = shutdown_ev,
    }) catch return error.Canceled;
    switch (winner) {
        .io => |r| {
            const c = r catch return if (pump.cq.isDrained()) .exit else .more;
            return if (pump.onCqComplete(c) == .exit) .exit else .more;
        },
        .timer => return .more,
        .shutdown => return error.Canceled,
    }
}

fn readSomeTls(self: *H1Conn, buf: []u8, deadline_ns: u64) !?usize {
    const pump = self.tls_t.running() orelse return error.TlsReadFailed;
    if (takeTlsPending(pump, buf)) |n| return n;
    if (pump.inbound_eof) return @as(usize, 0);
    while (true) {
        if (shutdownRequested(self)) return error.Canceled;
        if (nowNs(self.io) >= deadline_ns) return null;
        if (pump.pending_cipher != null and pump.pending_read == null) {
            const bytes = pump.pending_cipher.?;
            pump.pending_cipher = null;
            if (!pump.ingestCipher(bytes)) return @as(usize, 0);
        }
        if (pump.pending_read == null and pump.conn.pendingInbound()) {
            switch (pump.readOne()) {
                .eof, .closed => return @as(usize, 0),
                .ok, .want, .stuck => {},
            }
        }
        if (takeTlsPending(pump, buf)) |n| return n;
        if (pump.inbound_eof) return @as(usize, 0);
        switch (pump.pollRecv()) {
            .exit => return @as(usize, 0),
            .progress => continue,
            .none => {},
        }
        const out = waitTlsCq(self, deadline_ns) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
        };
        // The pump ended: nothing more will be read.
        if (out == .exit) return @as(usize, 0);
        if (nowNs(self.io) >= deadline_ns) return null;
    }
}

/// Wire the CQ and the recv op at this connection's final address, then arm the
/// first read. `ReadBuf` points into `recv_iov`, so this must run after the
/// struct is in place, never in an init that returns by value.
fn startRecv(self: *H1Conn) void {
    self.cq = zio.CompletionQueue.init();
    self.recv_op = zio.ev.NetRecv.init(
        tlsNetHandle(self.stream.socket.handle),
        zio.ev.ReadBuf.fromSlice(self.recv_buf, &self.recv_iov),
        .{},
    );
    self.recv_armed = false;
    self.recv_started = true;
    _ = armRecv(self);
}

/// (Re)submit the socket read once `recv_buf` is free. False only when the CQ
/// is already closed, which means teardown owns the exit.
fn armRecv(self: *H1Conn) bool {
    if (self.recv_armed) return true;
    if (self.recv_eof) return false;
    std.debug.assert(self.recv_pending.len == 0);
    self.cq.submit(&self.recv_op.c) catch |err| switch (err) {
        error.Closed => return false,
        // The op is ours alone: never grouped, never rearm-flagged.
        error.InvalidCompletion => unreachable,
    };
    self.recv_armed = true;
    return true;
}

/// The recv completion fired. Publish its bytes in `recv_pending`; a caller
/// takes them when it wants them. Nothing is dropped on any path here, which
/// is the whole point of the shape.
fn onRecvComplete(self: *H1Conn) void {
    std.debug.assert(self.recv_pending.len == 0);
    self.recv_armed = false;
    const n = self.recv_op.getResult() catch {
        // Only teardown cancels this op, and every other errno ends the
        // connection the same way EOF does.
        self.recv_eof = true;
        return;
    };
    if (n == 0) {
        self.recv_eof = true;
        return;
    }
    self.recv_pending = self.recv_buf[0..n];
}

/// Park until the recv completion arrives, the deadline passes, or shutdown is
/// requested. Canceling THIS wait is free: the read stays submitted and its
/// bytes are still delivered to `recv_pending` on a later turn.
fn waitRecvCq(self: *H1Conn, deadline_ns: ?u64) !void {
    const shutdown_ev = self.config.shutdown_event orelse &no_shutdown_event;
    const timeout: zio.Timeout = if (deadline_ns) |d| blk: {
        const now = nowNs(self.io);
        if (d <= now) return;
        break :blk .{ .duration = .fromNanoseconds(d - now) };
    } else .none;
    const winner = zio.select(.{
        .io = &self.cq,
        .timer = timeout,
        .shutdown = shutdown_ev,
    }) catch return error.Canceled;
    switch (winner) {
        .io => |r| {
            const c = r catch {
                if (self.cq.isDrained()) self.recv_eof = true;
                return;
            };
            std.debug.assert(c == &self.recv_op.c);
            onRecvComplete(self);
        },
        .timer => {},
        .shutdown => return error.Canceled,
    }
}

/// Hand out bytes the connection already holds. Returns 0 at end of stream and
/// null when the deadline passed with nothing delivered.
fn takeRecv(self: *H1Conn, buf: []u8) ?usize {
    if (self.recv_pending.len == 0) return null;
    const take = @min(self.recv_pending.len, buf.len);
    @memcpy(buf[0..take], self.recv_pending[0..take]);
    self.recv_pending = self.recv_pending[take..];
    if (self.recv_pending.len == 0) _ = armRecv(self);
    return take;
}

fn readSome(self: *H1Conn, buf: []u8, deadline_ns: u64) !?usize {
    if (self.tls != null) {
        return readSomeTls(self, buf, deadline_ns);
    }
    while (true) {
        if (takeRecv(self, buf)) |n| return n;
        if (self.recv_eof) return @as(usize, 0);
        if (!armRecv(self)) return @as(usize, 0);
        if (nowNs(self.io) >= deadline_ns) return null;
        try waitRecvCq(self, deadline_ns);
        if (self.recv_pending.len == 0 and !self.recv_eof and nowNs(self.io) >= deadline_ns) return null;
    }
}

fn waitTimer(timeout: std.Io.Timeout, io: std.Io) std.Io.Cancelable!void {
    return timeout.sleep(io);
}

fn waitShutdown(ev: *zio.Event, io: std.Io) std.Io.Cancelable!void {
    _ = io;
    ev.wait() catch return error.Canceled;
}

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn suppressPayload(method: request.Method, status: u16) bool {
    return method == .HEAD or status < 200 or status == 204 or status == 304;
}

/// Give back the server-wide stream this connection holds for its request.
fn releaseStream(self: *H1Conn) void {
    if (!self.stream_held) return;
    self.stream_held = false;
    if (self.config.accounting) |a| a.releaseStream(self.config.charge);
}

/// Copy the request into a fresh request arena from the pool, behind its
/// link, the arena's first allocation. Nothing the handler gets points into
/// this connection: `path`, `query`, `authority` and the header strings may
/// be slices of the head buffer or of the validation scratch, and `body` of
/// the body buffer, all of which the next request (or the connection's end)
/// overwrites. Null when the pool refuses: the status to answer, from the
/// cap that refused (`request_pool.Reason.status`).
fn newLink(
    self: *H1Conn,
    charge: accounts.Charge,
    method: request.Method,
    matched: router_mod.Match,
    body: []const u8,
    headers: []const request.Header,
    path: []const u8,
    query: []const u8,
    authority: []const u8,
) union(enum) { link: *H1Link, refused: u16 } {
    // The arena is made at dispatch, when the route is known, so every block
    // is charged to the route as it is taken (`charge` names it).
    var arena = request_pool.Arena.init(self.pool, &self.request_budget, charge);
    const link = arena.allocator().create(H1Link) catch {
        const status = (arena.refusal orelse request_pool.Reason.server_cap).status();
        arena.release();
        return .{ .refused = status };
    };
    link.* = .{
        .core = .{
            .arena = arena,
            .charge = charge,
            .accounting = self.config.accounting,
            .balancer = self.config.balancer,
            .exec_index = self.config.exec_index,
            .actor_thread = self.actor_thread,
        },
        .conn = self,
        .method = method,
        .req = undefined,
        .resp = undefined,
    };
    // From here the arena is the link's (`link.core.arena`); the local copy
    // is stale.
    link.core.arena.gate = &link.core.gate;
    link.core.retainCharge();
    const a = link.core.arena.allocator();
    const remainder = switch (matched) {
        .found => |f| f.path_remainder,
        else => "",
    };
    var total: usize = authority.len + path.len + query.len + remainder.len + body.len;
    for (headers) |h| total += h.name.len + h.value.len;
    const copied = copy: {
        const hdrs = a.alloc(request.Header, headers.len) catch break :copy false;
        const bytes = a.alloc(u8, total) catch break :copy false;
        var at: usize = 0;
        for (headers, hdrs) |h, *out| {
            out.* = .{ .name = takeCopy(bytes, &at, h.name), .value = takeCopy(bytes, &at, h.value) };
        }
        link.req = .{
            .method = method,
            .scheme = self.scheme,
            .authority = takeCopy(bytes, &at, authority),
            .path = takeCopy(bytes, &at, path),
            .path_remainder = takeCopy(bytes, &at, remainder),
            .query = takeCopy(bytes, &at, query),
            .headers = hdrs,
            .body = takeCopy(bytes, &at, body),
            .trailers = &.{},
            .arena = a,
            .peer = self.config.peer,
        };
        std.debug.assert(at == total);
        break :copy true;
    };
    if (!copied) {
        const status = (link.core.arena.refusal orelse request_pool.Reason.server_cap).status();
        connection.dropLink(H1Link, link);
        return .{ .refused = status };
    }
    link.resp = .{
        .stream_id = 1,
        .generation = link.core.terminal.currentGeneration(),
        .terminal = &link.core.terminal,
        .ctx = link,
        .io = self.io,
        .sendFn = sendCb,
        .startFn = startCb,
        .writeFn = writeCb,
        .flushFn = flushCb,
        .abortFn = abortCb,
        .waitUntilFn = waitUntilCb,
    };
    return .{ .link = link };
}

fn takeCopy(bytes: []u8, at: *usize, s: []const u8) []const u8 {
    const out = bytes[at.*..][0..s.len];
    @memcpy(out, s);
    at.* += s.len;
    return out;
}

fn dispatch(
    self: *H1Conn,
    head: parser.Head,
    matched: router_mod.Match,
    body: []const u8,
    headers: []const request.Header,
    path: []const u8,
    query: []const u8,
    authority: []const u8,
) !void {
    std.debug.assert(self.link == null);
    if (self.config.accounting) |a| {
        if (!a.tryAdmitStream(self.config.charge)) {
            try writeError(self, 503);
            self.want_close = true;
            return;
        }
        self.stream_held = true;
    }
    // Until the link exists, an error gives the stream back here; from then
    // on `releaseRequest` or `endHandler` does.
    var link_owns = false;
    errdefer if (!link_owns) releaseStream(self);

    const method = request.Method.parse(head.method_raw);
    const complete_inline = switch (matched) {
        .found => |f| f.handler == .complete,
        else => true,
    };

    // A task route's account (`accounts.RouteTable`), after the client
    // records in the link's charge. A complete route, a 404 and a 405 have
    // none.
    const route: ?accounts.RouteRef = switch (matched) {
        .found => |f| if (f.handler == .task) (if (self.config.accounting) |a| a.routes.ref(self.config.router.routes, f.route) else null) else null,
        else => null,
    };
    const charge = self.config.charge.withRoute(route);

    var token = false;
    if (!complete_inline) {
        if (self.config.accounting) |a| {
            // Every refusal is a 503 here: HTTP/1.1 has no REFUSED_STREAM.
            if (a.tryReserveTaskHandler(charge) != .ok) {
                releaseStream(self);
                try writeError(self, 503);
                self.want_close = true;
                return;
            }
            token = true;
        }
    }
    const link = switch (newLink(self, charge, method, matched, body, headers, path, query, authority)) {
        .link => |l| l,
        .refused => |status| {
            if (token) {
                if (self.config.accounting) |a| a.releaseTaskHandler(charge);
            }
            releaseStream(self);
            try writeError(self, status);
            self.want_close = true;
            return;
        },
    };
    // The link holds the token until it is freed, after the handler ends.
    link.core.task_token = token;
    self.link = link;
    link_owns = true;
    // Preserve unread-body close from serveOne. A bare assignment dropped it,
    // so leftover Content-Length bytes became the next request.
    self.want_close = self.want_close or head.connection_close;
    self.occupies = false;

    if (complete_inline) {
        // On the actor, so nothing can outlive the connection: the link has
        // the actor's reference only, and is freed when this returns.
        defer releaseRequest(self);
        switch (matched) {
            .found => |f| {
                var cr: response.CompleteResponse = .{ .inner = &link.resp };
                f.handler.complete.runFn(f.handler.complete.ptr, &link.req, &cr) catch {
                    // The same rule as a task handler (`runTaskBody`) and
                    // as HTTP/2: a handler that failed because its request's
                    // memory ran out says which cap, its own a 500, the
                    // connection's and the server's a 503.
                    const status: u16 = if (link.core.arena.refusal) |r| r.status() else 500;
                    if (!self.committed) writeError(self, status) catch {};
                    self.want_close = true;
                };
            },
            .not_found => try link.resp.send(404, &.{}, "not found"),
            .method_not_allowed => |ms| {
                _ = ms;
                try link.resp.send(405, &.{.{ .name = "allow", .value = "GET" }}, "");
            },
        }
        if (!link.resp.committed) {
            try link.resp.send(200, &.{}, "");
        }
        return;
    }

    link.handler = matched.found.handler.task;
    // The handler's reference; the actor's is the one `newLink` began with.
    _ = link.core.refs.fetchAdd(1, .monotonic);
    link.core.task_counted = true;
    // Counted before the spawn, so a job that finishes at once still finds
    // its count to release.
    if (self.config.balancer) |b| b.noteHandlerStart(self.config.exec_index.?);
    const handle = zio.spawnInto(connection.connPlacement(), runJob, .{link}) catch {
        if (self.config.balancer) |b| b.noteHandlerEnd(self.config.exec_index.?);
        link.core.task_counted = false;
        _ = link.core.refs.fetchSub(1, .monotonic);
        releaseRequest(self);
        try writeError(self, 503);
        self.want_close = true;
        return;
    };
    self.join = handle;
    try waitDispatch(self);
}

/// The task handler's job. It reaches the connection only inside its link's
/// gate, so it may run on after the connection ended.
fn runJob(link: *H1Link) void {
    connection.notePlacement(link.core.actor_thread);
    var marker: @TypeOf(connection.handler_job_marker).Node = .unset;
    if (comptime connection.test_observe) connection.handler_job_marker.set(&marker, {});
    defer if (comptime connection.test_observe) connection.handler_job_marker.clear(&marker);
    defer finishJob(link);
    runTaskBody(link);
}

/// The handler's report, its job's last act. Inside the gate: post the
/// completion (the actor selects on the channel; it joins this task, then
/// frees the request). With the gate closed the connection ended while this
/// handler ran and orphaned it; it reports nothing. Then, outside the gate:
/// the balancer, and the handler's reference, which frees the link only when
/// the actor dropped first (the orphan case).
fn finishJob(link: *H1Link) void {
    if (link.enter()) |self| {
        defer link.exit();
        self.completion_ch.trySend(1) catch |err| switch (err) {
            // One request at a time: one completion per request, capacity 1.
            error.WouldBlock => @panic("h1: a second handler completion for one request"),
            error.Closed => @panic("h1: completion channel closed with a handler inside its gate"),
        };
    }
    link.core.noteHandlerEnd();
    connection.dropLink(H1Link, link);
}

fn runTaskBody(link: *H1Link) void {
    const t = &link.core.terminal;
    if (t.cancel_flag.load(.acquire)) return;
    if (t.getCause() != null) return;
    const run_res: anyerror!void = link.handler.runFn(link.handler.ptr, &link.req, &link.resp);
    run_res catch {
        if (t.getCause() != null) return;
        if (!link.resp.committed) {
            // A handler that failed because its request's memory ran out
            // says which cap: its own is a 500, the others a 503.
            const status: u16 = if (link.core.arena.refusal) |r| r.status() else 500;
            link.resp.send(status, &.{}, "internal error") catch {};
        } else if (!link.resp.finished) {
            // The framing reaches the connection only through the gate, like
            // every other call.
            const self = link.enter() orelse return;
            defer link.exit();
            finishFraming(self) catch {};
            self.want_close = true;
            link.resp.finished = true;
        }
    };
    if (t.getCause() == null and !link.resp.committed) {
        link.resp.send(500, &.{}, "no response") catch {};
    }
}

fn tlsIdle(pump: *tls_edge.Pump) bool {
    return pump.pending_n == 0 and !pump.send_armed;
}

fn ackTlsWrite(self: *H1Conn, ok: bool) void {
    self.tls_write_inflight = false;
    self.tls_ok_ch.trySend(ok) catch {};
}

/// Drive one chunk of a task handler's write. False: the pump ended (it
/// failed and gave back what it held), and the caller ends the connection;
/// nothing more may be submitted to it.
fn driveHandlerTlsWrite(self: *H1Conn, n: u32) bool {
    const pump = self.tls_t.running() orelse {
        ackTlsWrite(self, false);
        return true;
    };
    const idx = self.tls_t.write_pool.tryRentIndex(self.io) orelse {
        ackTlsWrite(self, false);
        return true;
    };
    var chunk = wire_pump.WireChunk{
        .bytes = self.tls_t.out[0..n],
        .len = n,
        .pool_index = idx,
    };
    pump.stampPush(&chunk);
    if (!pump.writeChunks(chunk)) {
        // The cause first: the failed ack wakes the handler, which reads it.
        if (self.link) |l| l.core.terminal.setCause(.connection_closed);
        ackTlsWrite(self, false);
        return false;
    }
    if (tlsIdle(pump)) {
        ackTlsWrite(self, true);
        return true;
    }
    self.tls_write_clock = .start(self, pump);
    self.tls_write_inflight = true;
    return true;
}

fn ingestTlsPeer(self: *H1Conn) bool {
    const pump = self.tls_t.running() orelse return false;
    if (pump.inbound_eof) return true;
    if (pump.pending_read == null and pump.conn.pendingInbound()) {
        switch (pump.readOne()) {
            .eof, .closed => return true,
            .ok, .want, .stuck => {},
        }
    }
    var tmp: [256]u8 = undefined;
    if (takeTlsPending(pump, tmp[0..])) |n| {
        if (n == 0) return true;
        stashCarry(self, tmp[0..n], 0) catch return true;
    }
    return pump.inbound_eof;
}

/// The actor's wait while a task handler runs, on TLS: the actor also drives
/// the handler's writes (`tls_req_ch`) through the pump. Returns when the
/// handler reported (`releaseRequest`), or when the connection ends under it
/// (`endHandler`, which never waits for handler code).
fn waitDispatchTls(self: *H1Conn) !void {
    const pump = self.tls_t.running().?;
    const shutdown_ev = self.config.shutdown_event orelse &no_shutdown_event;
    while (self.link) |link| {
        if (shutdownRequested(self)) {
            endForShutdown(self);
            break;
        }

        // A handler write in flight runs the write-byte clock: the socket
        // takes none of it for the timeout, and the connection goes.
        var write_timer: zio.Timeout = .none;
        if (self.tls_write_inflight) {
            if (self.tls_write_clock.expired(self, pump)) {
                // The cause first: the handler's failed write reads it
                // (`writeRawTlsFromHandler`) as soon as the ack wakes it.
                link.core.terminal.setCause(.slow_consumer);
                ackTlsWrite(self, false);
                shutTimedOut(self);
                endHandler(self, .slow_consumer);
                break;
            }
            if (self.tls_write_clock.deadline()) |d| {
                // Absolute: a `.duration` arms from a cached clock (zio t-3653).
                write_timer = .{ .deadline = .fromNanoseconds(d) };
            }
        }
        const winner = zio.select(.{
            .io = &pump.cq,
            .comps = self.completion_ch.asyncReceive(),
            .writes = self.tls_req_ch.asyncReceive(),
            .shutdown = shutdown_ev,
            .timer = write_timer,
        }) catch return error.Canceled;
        switch (winner) {
            .timer => {},
            .io => |r| {
                const c = r catch {
                    if (pump.cq.isDrained()) {
                        endHandler(self, .connection_closed);
                        break;
                    }
                    continue;
                };
                if (pump.onCqComplete(c) == .exit) {
                    endHandler(self, .connection_closed);
                    break;
                }
                if (self.tls_write_inflight) {
                    if (pump.pending_n > 0) {
                        if (!pump.drivePending()) {
                            // The cause first: the failed ack wakes the
                            // handler, which reads it (`writeFailedCause`).
                            link.core.terminal.setCause(.connection_closed);
                            ackTlsWrite(self, false);
                            endHandler(self, .connection_closed);
                            break;
                        }
                    }
                    if (tlsIdle(pump)) ackTlsWrite(self, true);
                }
                if (ingestTlsPeer(self)) {
                    endHandler(self, .connection_closed);
                    break;
                }
            },
            .comps => |r| {
                _ = r catch {};
                releaseRequest(self);
                break;
            },
            .writes => |r| {
                const n = r catch continue;
                if (!driveHandlerTlsWrite(self, n)) {
                    endHandler(self, .connection_closed);
                    break;
                }
            },
            .shutdown => {
                endForShutdown(self);
                break;
            },
        }
    }
}

/// The actor's wait while a task handler runs, on h1c: the handler writes
/// the socket itself (inside its gate); the actor keeps reading, so a
/// pipelined request is kept and the peer's close is seen.
fn waitDispatch(self: *H1Conn) !void {
    if (self.tls_t.running() != null) return waitDispatchTls(self);
    const shutdown_ev = self.config.shutdown_event orelse &no_shutdown_event;
    while (self.link != null) {
        if (shutdownRequested(self)) {
            endForShutdown(self);
            break;
        }
        // `zio.select` over the CQ and the completion channel, the same wait the
        // TLS actor uses. Not `std.Io.Select`: that one discards results from
        // tasks that finish during cancelation, and for the completion channel
        // that result is the handler's only report. zio closed the
        // equivalent hole in its own select (lalinsky/zio #700, #701, #706,
        // all in the pinned revision), so a committed item survives a cancel
        // here; a report left in the channel is found by `endHandler`.
        const winner = zio.select(.{
            .io = &self.cq,
            .comps = self.completion_ch.asyncReceive(),
            .shutdown = shutdown_ev,
        }) catch return error.Canceled;
        switch (winner) {
            .io => |r| {
                const c = r catch {
                    if (self.cq.isDrained()) self.recv_eof = true;
                    continue;
                };
                std.debug.assert(c == &self.recv_op.c);
                onRecvComplete(self);
                if (self.recv_pending.len != 0) {
                    stashCarry(self, self.recv_pending, 0) catch {
                        self.want_close = true;
                    };
                    self.recv_pending = &.{};
                    _ = armRecv(self);
                }
                if (self.recv_eof) {
                    endHandler(self, .connection_closed);
                    break;
                }
            },
            .comps => {
                releaseRequest(self);
                break;
            },
            .shutdown => {
                endForShutdown(self);
                break;
            },
        }
    }
}

/// The request ended on the ordinary path: a task handler reported (inside
/// its gate, so it runs no handler code any more), or a complete handler
/// returned on the actor, or the dispatch was undone. Join the task (its
/// last instructions: the balancer and its own reference drop), drop the
/// actor's reference, which is then the last, so the link and the request's
/// memory go back here, with the gate open, and give the stream back.
fn releaseRequest(self: *H1Conn) void {
    const link = self.link orelse return;
    if (self.join) |*h| {
        h.join();
        self.join = null;
    }
    self.link = null;
    connection.dropLink(H1Link, link);
    releaseStream(self);
}

/// The server is shutting down: this connection's cancel step
/// (`noteShutdownCancel`), then its end.
fn endForShutdown(self: *H1Conn) void {
    noteShutdownCancel(self);
    endHandler(self, .server_shutdown);
}

/// The connection ends with a request dispatched. Actor only. It does not
/// wait for handler code, the same steps as HTTP/2's `shutdownHandlers`:
///
/// 1. Publish the end: the cause (the first one set wins), `dead`, the
///    socket shut (a handler blocked in an h1c socket write fails at once),
///    and the TLS write channels closed (a handler waiting for the actor to
///    drive its write fails at once).
/// 2. Close the gate, so no new call enters, and request a cancel through
///    the join handle's awaitable, which never waits.
/// 3. Wait for the calls already inside the gate. Bounded by starh2 code:
///    step 1 woke every wait a call can be in.
/// 4. Give the connection its request bytes back
///    (`LinkCore.returnConnCharges`). A handler whose report is in the
///    channel is done with handler code: join it. One that did not report
///    is orphaned (`LinkCore.orphan`): its join handle is detached, its link
///    keeps the server-wide token and the orphan count, and `Server.serve`
///    waits for it. Either way the actor drops its reference.
///
/// The connection must end after this: `dead` is set for good, the socket is
/// shut, and the channels are closed. `want_close` says so to the loop.
fn endHandler(self: *H1Conn, cause: response.TerminalCause) void {
    const link = self.link orelse return;
    zio.beginShield();
    defer zio.endShield();
    link.core.terminal.setCause(cause);
    self.dead.set();
    if (!self.socket_closed) self.stream.shutdown(self.io, .both) catch {};
    if (self.tls_t.running() != null) {
        self.tls_req_ch.close(.immediate);
        self.tls_ok_ch.close(.immediate);
    }
    link.core.gate.close();
    if (self.join) |h| {
        if (h.awaitable) |aw| aw.cancel();
    }
    link.core.gate.waitDrained();
    link.core.returnConnCharges(&self.request_budget);
    // A complete handler ran on this task and is done; a task handler is
    // done with handler code only if its report is in.
    const reported = if (self.join == null) true else blk: {
        _ = self.completion_ch.tryReceive() catch break :blk false;
        break :blk true;
    };
    if (self.join) |*h| {
        if (reported) h.join() else h.detach();
        self.join = null;
    }
    if (!reported) link.core.orphan();
    // After the drain: a call inside the gate may write it too.
    self.want_close = true;
    self.link = null;
    connection.dropLink(H1Link, link);
    releaseStream(self);
}

/// `serve` is leaving, by any path, with a request maybe still dispatched:
/// the end of its loop, an error, or this task's own cancel (a canceled
/// `serve` cancels every connection). A cancel of this task is the
/// server's, so it is this connection's cancel step.
fn endOnExit(self: *H1Conn, canceled: bool) void {
    if (self.link == null) return;
    if (canceled or shutdownRequested(self)) {
        endForShutdown(self);
        return;
    }
    endHandler(self, .connection_closed);
}

/// The server's shutdown reached this connection's cancel step: count it
/// once, so `Server.serve` knows when every live connection has asked its
/// handlers to cancel (`GlobalAccounting.shutdown_cancelled`).
fn noteShutdownCancel(self: *H1Conn) void {
    if (self.shutdown_cancel_counted) return;
    self.shutdown_cancel_counted = true;
    if (self.config.accounting) |a| {
        if (self.join != null) _ = a.shutdown_cancel_asked.fetchAdd(1, .acq_rel);
        _ = a.shutdown_cancelled.fetchAdd(1, .acq_rel);
    }
}

fn headerLineOk(name: []const u8, value: []const u8) bool {
    for (name) |c| {
        if (c == '\r' or c == '\n' or c == ':') return false;
    }
    for (value) |c| {
        if (c == '\r' or c == '\n') return false;
    }
    return true;
}

// Every callback below enters the connection only through the link's gate,
// and fails with the cause once the gate is closed (`LinkCore.closedError`).

fn sendCb(ctx: *anyopaque, _: u31, status: u16, headers: []const request.Header, body: []const u8) response.ResponseError!void {
    const link = H1Link.fromCtx(ctx);
    const self = link.enter() orelse return link.core.closedError();
    defer link.exit();
    if (self.committed) return error.ResponseCommitted;
    for (headers) |hdr| {
        if (!headerLineOk(hdr.name, hdr.value)) return error.InvalidHeader;
    }
    self.committed = true;
    self.resp_status = status;
    self.no_payload = suppressPayload(link.method, status);
    if (self.head.version == .http10) {
        self.framing = .close_delimited;
        self.want_close = true;
    } else {
        self.framing = .content_length;
    }
    const cl: usize = if (link.method == .HEAD) body.len else if (self.no_payload) 0 else body.len;
    try writeStatus(self, status);
    try writeCommonHeaders(self, headers, self.framing, cl);
    try writeRaw(self, "\r\n");
    if (!self.no_payload and body.len != 0) try writeRaw(self, body);
    try flushSink(self);
    self.finished = true;
}

fn startCb(ctx: *anyopaque, _: u31, status: u16, headers: []const request.Header, sse: bool) response.ResponseError!void {
    const link = H1Link.fromCtx(ctx);
    const self = link.enter() orelse return link.core.closedError();
    defer link.exit();
    if (self.committed) return error.ResponseCommitted;
    for (headers) |hdr| {
        if (!headerLineOk(hdr.name, hdr.value)) return error.InvalidHeader;
    }
    self.committed = true;
    self.resp_status = status;
    self.no_payload = suppressPayload(link.method, status);
    if (sse) self.occupies = true;
    if (self.no_payload) {
        self.framing = .content_length;
    } else if (self.head.version == .http10) {
        self.framing = .close_delimited;
        self.want_close = true;
    } else {
        self.framing = .chunked;
    }
    try writeStatus(self, status);
    if (sse) {
        var saw_ct = false;
        for (headers) |hdr| {
            if (std.ascii.eqlIgnoreCase(hdr.name, "content-type")) saw_ct = true;
        }
        if (!saw_ct) try writeHeaderLine(self, "content-type", "text/event-stream");
    }
    try writeCommonHeaders(self, headers, self.framing, 0);
    try writeRaw(self, "\r\n");
    try flushSink(self);
}

fn writeCb(ctx: *anyopaque, _: u31, bytes: []const u8, end: bool, wait: bool, emit: bool) response.ResponseError!void {
    const link = H1Link.fromCtx(ctx);
    const self = link.enter() orelse return link.core.closedError();
    defer link.exit();
    _ = wait;
    if (self.no_payload) {
        if (end) {
            try flushSink(self);
            self.finished = true;
        }
        return;
    }
    if (bytes.len != 0) {
        try appendPending(self, bytes);
        const buffer_sse = test_channel_mutation == .m6_buffer_sse and self.occupies;
        if (emit and !buffer_sse) try emitPending(self);
    }
    if (end) try finishFraming(self);
}

fn flushCb(ctx: *anyopaque, _: u31) response.ResponseError!void {
    const link = H1Link.fromCtx(ctx);
    const self = link.enter() orelse return link.core.closedError();
    defer link.exit();
    if (test_channel_mutation == .m6_buffer_sse and self.occupies) return;
    try emitPending(self);
    try flushSink(self);
}

fn abortCb(ctx: *anyopaque, _: u31) response.ResponseError!void {
    const link = H1Link.fromCtx(ctx);
    const self = link.enter() orelse return link.core.closedError();
    defer link.exit();
    self.want_close = true;
}

/// Sleeps to the deadline rounded up by `Limits.handler_deadline_slack_ns`,
/// the same rule as the HTTP/2 path. A handler already past its own deadline
/// returns at once. The wait is inside the gate, so it also wakes on the
/// connection's end (`dead`), as HTTP/2's does.
fn waitUntilCb(ctx: *anyopaque, _: u31, deadline: std.Io.Timestamp) response.ResponseError!void {
    const link = H1Link.fromCtx(ctx);
    const self = link.enter() orelse return link.core.closedError();
    defer link.exit();
    const t = &link.core.terminal;
    if (t.getCause()) |c| return response.causeToError(c);
    const deadline_ns = connection.handlerDeadlineNs(deadline);
    if (nowNs(self.io) >= deadline_ns) return;
    const armed_ns = self.config.limits.handlerDeadlineNs(deadline_ns);
    const winner = zio.select(.{
        .dead = &self.dead,
        .timer = zio.Timeout{ .deadline = .fromNanoseconds(armed_ns) },
    }) catch |err| switch (err) {
        error.Canceled => {
            if (t.getCause()) |c| return response.causeToError(c);
            return error.Canceled;
        },
    };
    switch (winner) {
        .dead, .timer => {},
    }
    if (t.getCause()) |c| return response.causeToError(c);
}

fn appendPending(self: *H1Conn, bytes: []const u8) response.ResponseError!void {
    if (self.pending_len + bytes.len > self.pending_storage.len) return error.WriteFailed;
    @memcpy(self.pending_storage[self.pending_len..][0..bytes.len], bytes);
    self.pending_len += bytes.len;
}

fn emitPending(self: *H1Conn) response.ResponseError!void {
    if (self.pending_len == 0 and self.framing != .chunked) {
        try flushSink(self);
        return;
    }
    switch (self.framing) {
        .chunked => {
            if (self.pending_len == 0) {
                try flushSink(self);
                return;
            }
            try writeChunk(self, self.pending_storage[0..self.pending_len]);
            self.pending_len = 0;
        },
        .close_delimited, .content_length => {
            if (self.pending_len != 0) try writeRaw(self, self.pending_storage[0..self.pending_len]);
            self.pending_len = 0;
        },
    }
}

fn finishFraming(self: *H1Conn) response.ResponseError!void {
    try emitPending(self);
    if (self.framing == .chunked and !self.no_payload) {
        if (test_channel_mutation != .m2_no_chunk_end) {
            try writeRaw(self, "0\r\n\r\n");
        }
    }
    try flushSink(self);
    if (self.framing == .close_delimited) self.want_close = true;
    self.finished = true;
}

fn writeChunk(self: *H1Conn, data: []const u8) response.ResponseError!void {
    var hex: [16]u8 = undefined;
    const n = std.fmt.bufPrint(&hex, "{x}\r\n", .{data.len}) catch return error.WriteFailed;
    try writeRaw(self, n);
    if (data.len != 0) try writeRaw(self, data);
    try writeRaw(self, "\r\n");
}

fn writeStatus(self: *H1Conn, status: u16) response.ResponseError!void {
    var line: [64]u8 = undefined;
    const phrase = parser.reasonPhrase(status);
    const s = std.fmt.bufPrint(&line, "HTTP/1.1 {d} {s}\r\n", .{ status, phrase }) catch
        return error.WriteFailed;
    try writeRaw(self, s);
}

fn writeHeaderLine(self: *H1Conn, name: []const u8, value: []const u8) response.ResponseError!void {
    for (name) |c| {
        if (c == '\r' or c == '\n' or c == ':') return error.InvalidHeader;
    }
    for (value) |c| {
        if (c == '\r' or c == '\n') return error.InvalidHeader;
    }
    var buf: [512]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{s}: {s}\r\n", .{ name, value }) catch return error.WriteFailed;
    try writeRaw(self, s);
}

fn writeCommonHeaders(
    self: *H1Conn,
    headers: []const request.Header,
    framing: Framing,
    body_len: usize,
) response.ResponseError!void {
    for (headers) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, "content-length")) continue;
        if (std.ascii.eqlIgnoreCase(h.name, "transfer-encoding")) continue;
        if (std.ascii.eqlIgnoreCase(h.name, "connection")) continue;
        try writeHeaderLine(self, h.name, h.value);
    }
    if (self.resp_status != 0 and self.resp_status < 200) {
        return;
    }
    switch (framing) {
        .content_length => {
            var nbuf: [32]u8 = undefined;
            const n = std.fmt.bufPrint(&nbuf, "{d}", .{body_len}) catch return error.WriteFailed;
            try writeHeaderLine(self, "content-length", n);
            if (self.want_close) try writeHeaderLine(self, "connection", "close");
        },
        .chunked => {
            try writeHeaderLine(self, "transfer-encoding", "chunked");
            if (self.want_close) try writeHeaderLine(self, "connection", "close");
        },
        .close_delimited => {
            try writeHeaderLine(self, "connection", "close");
        },
    }
}

fn writeError(self: *H1Conn, status: u16) !void {
    const phrase = parser.reasonPhrase(status);
    var buf: [256]u8 = undefined;
    const body = phrase;
    const msg = std.fmt.bufPrint(&buf, "HTTP/1.1 {d} {s}\r\ncontent-length: {d}\r\nconnection: close\r\n\r\n{s}", .{
        status,
        phrase,
        body.len,
        body,
    }) catch return;
    try writeRaw(self, msg);
    try flushSink(self);
    self.want_close = true;
    self.committed = true;
}

/// A write failed. If the socket took no byte for the write-byte timeout, the
/// peer stopped reading: shut the socket, so the connection's own wait (for
/// the handler, or for the next request) ends too and the connection goes.
fn noteWriteFailed(self: *H1Conn) response.ResponseError {
    if (self.writer.timed_out) {
        _ = write_byte_expiries.fetchAdd(1, .monotonic);
        self.stream.shutdown(self.io, .both) catch {};
        return error.SlowConsumer;
    }
    // The connection ended under a handler's write (`endHandler` shut the
    // socket): the handler is told why.
    return writeFailedCause(self);
}

/// A handler write the connection failed: the connection's cause, when it
/// set one (the write-byte timeout's `SlowConsumer`), else `WriteFailed`.
fn writeFailedCause(self: *H1Conn) response.ResponseError {
    if (self.link) |l| {
        if (l.core.terminal.getCause()) |c| return response.causeToError(c);
    }
    return error.WriteFailed;
}

/// HTTP/1.1 connections shut by the write-byte timeout, process-wide.
pub var write_byte_expiries: std.atomic.Value(u64) = .init(0);

/// On TLS only the actor drives the pump. The path is chosen by which task
/// calls, not by a flag the actor sets and clears: a task handler's write,
/// even one still inside the gate while teardown runs, always goes to the
/// actor (`writeRawTlsFromHandler`); the actor's own writes (a complete
/// handler, `writeError`) drive the pump (`writeRawTlsOwner`).
fn writeRaw(self: *H1Conn, bytes: []const u8) response.ResponseError!void {
    if (self.tls != null) {
        if (onActor(self)) {
            try writeRawTlsOwner(self, bytes);
        } else {
            try writeRawTlsFromHandler(self, bytes);
        }
        return;
    }
    self.writer.interface.writeAll(bytes) catch return noteWriteFailed(self);
}

fn writeRawTlsFromHandler(self: *H1Conn, bytes: []const u8) response.ResponseError!void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = @min(bytes.len - off, self.tls_t.out.len);
        @memcpy(self.tls_t.out[0..n], bytes[off..][0..n]);
        // Teardown closes this channel too (`endHandler`): a handler that got
        // the ack for one chunk meets it closed at the next.
        self.tls_req_ch.send(@intCast(n)) catch return writeFailedCause(self);
        const ok = self.tls_ok_ch.receive() catch return writeFailedCause(self);
        if (!ok) return writeFailedCause(self);
        off += n;
    }
}

fn writeRawTlsOwner(self: *H1Conn, bytes: []const u8) response.ResponseError!void {
    const pump = self.tls_t.running() orelse return error.WriteFailed;
    var off: usize = 0;
    while (off < bytes.len) {
        const n = @min(bytes.len - off, self.tls_t.out.len);
        @memcpy(self.tls_t.out[0..n], bytes[off..][0..n]);
        const idx = self.tls_t.write_pool.tryRentIndex(self.io) orelse return error.WriteFailed;
        var chunk = wire_pump.WireChunk{
            .bytes = self.tls_t.out[0..n],
            .len = n,
            .pool_index = idx,
        };
        pump.stampPush(&chunk);
        if (!pump.writeChunks(chunk)) return tlsPumpEnded(self);
        try driveTlsUntilIdle(self);
        off += n;
    }
}

fn flushSink(self: *H1Conn) response.ResponseError!void {
    if (self.tls != null) {
        // A handler's chunks are driven by the actor, ack by ack.
        if (!onActor(self)) return;
        try driveTlsUntilIdle(self);
        return;
    }
    self.writer.interface.flush() catch return noteWriteFailed(self);
}

test "an HTTP/1.1 call after its link's gate closed fails without touching the connection (t-3759)" {
    // The connection pointer is poison: a call that enters the connection
    // anyway faults here. No cause is set, which is the window the gate
    // exists for: `Response` checks the cause first, so only the gate stands
    // between a call that raced teardown and freed memory.
    var link: H1Link = .{
        .core = .{ .arena = undefined, .charge = .none },
        .conn = @ptrFromInt(0xdead0000),
        .method = .GET,
        .req = undefined,
        .resp = undefined,
    };
    link.core.gate.close();
    const ctx: *anyopaque = &link;
    try std.testing.expectError(error.ConnectionClosed, writeCb(ctx, 1, "late", false, true, true));
    try std.testing.expectError(error.ConnectionClosed, sendCb(ctx, 1, 200, &.{}, "late"));
    try std.testing.expectError(error.ConnectionClosed, startCb(ctx, 1, 200, &.{}, true));
    try std.testing.expectError(error.ConnectionClosed, flushCb(ctx, 1));
    try std.testing.expectError(error.ConnectionClosed, abortCb(ctx, 1));
    try std.testing.expectError(error.ConnectionClosed, waitUntilCb(ctx, 1, .{ .nanoseconds = 0 }));
    // A cause set before the close is the error the call gets.
    link.core.terminal.setCause(.slow_consumer);
    try std.testing.expectError(error.SlowConsumer, writeCb(ctx, 1, "late", false, true, true));
    try std.testing.expectEqual(@as(u32, 0), link.core.gate.inside());
}

test "a TLS handler write that meets the closed request channel fails with the cause (t-3759 round 1)" {
    // Teardown (`endHandler`) closes `tls_req_ch` and `tls_ok_ch`. A handler
    // that got the ack for one chunk of a write meets the request channel
    // closed at the next one, and must be told why, like one that was
    // waiting for an ack. Only the fields `writeRawTlsFromHandler` and
    // `writeFailedCause` read are set.
    var conn: H1Conn = undefined;
    var out: [64]u8 = undefined;
    var req_buf: [1]u32 = undefined;
    var ok_buf: [1]bool = undefined;
    conn.tls_t.out = &out;
    conn.tls_req_ch = .init(&req_buf);
    conn.tls_ok_ch = .init(&ok_buf);
    var link: H1Link = .{
        .core = .{ .arena = undefined, .charge = .none },
        .conn = &conn,
        .method = .GET,
        .req = undefined,
        .resp = undefined,
    };
    conn.link = &link;
    link.core.terminal.setCause(.server_shutdown);
    conn.tls_req_ch.close(.immediate);
    conn.tls_ok_ch.close(.immediate);
    try std.testing.expectError(error.ConnectionClosed, writeRawTlsFromHandler(&conn, "a chunk"));
}

test "a write from a task that is not the actor never takes the actor's TLS path (t-3759 round 2)" {
    // On TLS only the actor drives the pump (`writeRawTlsOwner`,
    // `driveTlsUntilIdle`); a task handler hands its chunks to the actor.
    // The first version chose by a flag the actor cleared when its wait for
    // the handler was cancelled, before teardown had closed and drained the
    // gate, so a handler still inside could take the actor's path from its
    // own task. Here the flag (where it exists) is in that state, the caller
    // is not the actor (a unit test runs on no zio task), and teardown has
    // closed the channels: the write must go the handler's way and fail with
    // the cause, never touch the pump (there is none: a dangling `tls`, no
    // running pump).
    var conn: H1Conn = undefined;
    if (@hasField(H1Conn, "offload_tls_io")) @field(conn, "offload_tls_io") = false;
    conn.tls = @ptrFromInt(0x10000);
    conn.tls_t = .{};
    var out: [64]u8 = undefined;
    var req_buf: [1]u32 = undefined;
    var ok_buf: [1]bool = undefined;
    conn.tls_t.out = &out;
    conn.tls_req_ch = .init(&req_buf);
    conn.tls_ok_ch = .init(&ok_buf);
    var link: H1Link = .{
        .core = .{ .arena = undefined, .charge = .none },
        .conn = &conn,
        .method = .GET,
        .req = undefined,
        .resp = undefined,
    };
    conn.link = &link;
    link.core.terminal.setCause(.server_shutdown);
    conn.tls_req_ch.close(.immediate);
    conn.tls_ok_ch.close(.immediate);
    try std.testing.expectError(error.ConnectionClosed, writeRaw(&conn, "a chunk"));
}
