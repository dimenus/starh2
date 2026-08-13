//! Connection actor: owns Session, TLS (optional), HPACK, fair write scheduling.
const std = @import("std");
const tls = @import("tls");
const session_mod = @import("../core/session.zig");
const hpack = @import("../core/hpack.zig");
const limits_mod = @import("../core/limits.zig");
const frame = @import("../core/frame.zig");
const request = @import("../http/request.zig");
const response = @import("../http/response.zig");
const router_mod = @import("../http/router.zig");
const wire_pump = @import("wire_pump.zig");
const tls_edge = @import("tls.zig");
const h2c = @import("h2c.zig");
const rates_mod = @import("../core/rates.zig");
const ticket_table = @import("ticket_table.zig");
const control_pool = @import("control_pool.zig");
const fair_scheduler = @import("fair_scheduler.zig");
const io_queue = @import("io_queue.zig");

/// Test-only: when true, handler spawn fails closed into synchronous runHandlerJob.
pub var test_force_spawn_fail: bool = false;
/// Test-only: when true, actor skips draining completion_ch (sticky until cleared).
pub var test_hold_completion_drain: std.atomic.Value(bool) = .init(false);
/// Test-only: delay write pump by N ms.
pub var test_write_delay_ms: u64 = 0;
/// Test-only: fail write pump after N successful writes (0 = never).
pub var test_write_fail_after: u64 = 0;
/// Test-only: handlers running across all connections. Published by the mutation
/// itself, not by the actor loop: an idle actor parks until the next wakeup, so a
/// value refreshed only per iteration stays stale for as long as the connection is
/// quiet and reports handlers that have already exited.
pub var test_observed_live_handlers: std.atomic.Value(usize) = .init(0);
/// Test-only: handler slots held across all connections. Released strictly later
/// than `test_observed_live_handlers` (the actor releases the slot after joining
/// the handler), so a drain gate must require both to reach zero.
pub var test_observed_slots_in_use: std.atomic.Value(usize) = .init(0);
/// Test-only: FairScheduler emits observed by actor (mutation canary).
pub var test_observed_sched_emits: std.atomic.Value(usize) = .init(0);
/// Test-only: true once actor handled writer_failed terminal teardown.
pub var test_observed_writer_fail_handled: std.atomic.Value(bool) = .init(false);
/// Test-only: queueWire calls not from FairScheduler sink (must stay 0).
pub var test_queue_wire_bypass: std.atomic.Value(usize) = .init(0);
/// Test-only: true while FairScheduler sink is executing queueWire.
threadlocal var test_in_sched_sink: bool = false;
/// Test-only: force queueWire/send fail after N successful accounted sends (0=never).
pub var test_force_wire_fail_after: std.atomic.Value(usize) = .init(0);
pub var test_wire_sends: std.atomic.Value(usize) = .init(0);
/// Test-only: last handler ResponseError taxonomy (0=none/success, 1=WriteFailed, 2=ConnectionClosed,
/// 3=PeerReset, 4=SlowConsumer, 5=other).
pub var test_last_handler_err: std.atomic.Value(u8) = .init(0);
pub var test_last_peer_reset_code: std.atomic.Value(u32) = .init(0);
/// Test-only: stream_id currently blocked in waitForStreamSpace (0 = none).
pub var test_waiting_for_space: std.atomic.Value(u32) = .init(0);
/// Test-only: Connection finished boot allocations (pools, sched slabs, session maps).
pub var test_boot_ready: std.atomic.Value(bool) = .init(false);
/// Test-only gate that parks the actor immediately before its event-driven wait.
pub var test_hold_before_actor_wait: std.atomic.Value(bool) = .init(false);
pub var test_actor_waiting: std.Io.Event = .unset;
pub var test_release_actor_wait: std.Io.Event = .unset;
/// Test-only fallback timer raced against actor events by the wakeup canary.
pub var test_polling_canary_tick_ns: std.atomic.Value(u64) = .init(0);

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

pub const Mode = enum { h2c, tls_h2 };

pub const ConnConfig = struct {
    io: std.Io,
    mode: Mode,
    limits: limits_mod.Limits,
    router: router_mod.Router,
    tls_auth: ?*tls.config.CertKeyPair = null,
    gpa: std.mem.Allocator,
    shutdown_flag: ?*std.atomic.Value(bool) = null,
    shutdown_event: ?*std.Io.Event = null,
    reaper: ?*ReaperPool = null,
    /// Server-wide stream + reaper reservation (optional for unit tests).
    accounting: ?*GlobalAccounting = null,
    /// True when accept path reserved a concurrent TLS handshake slot.
    handshake_held: bool = false,
};

/// Shared with Server: global stream/reaper/memory/handshake admission.
pub const GlobalAccounting = struct {
    active_streams: std.atomic.Value(usize) = .init(0),
    max_streams: usize,
    reaper_reserved: std.atomic.Value(usize) = .init(0),
    reaper_capacity: usize,
    outbound_bytes: std.atomic.Value(usize) = .init(0),
    max_outbound_bytes: usize,
    request_bytes: std.atomic.Value(usize) = .init(0),
    max_request_bytes: usize,
    active_handshakes: std.atomic.Value(usize) = .init(0),
    max_handshakes: usize,

    pub fn tryAdmitStream(self: *GlobalAccounting) bool {
        while (true) {
            const cur = self.active_streams.load(.acquire);
            if (cur >= self.max_streams) return false;
            if (self.active_streams.cmpxchgWeak(cur, cur + 1, .acq_rel, .acquire) == null) return true;
        }
    }

    pub fn releaseStream(self: *GlobalAccounting) void {
        const prev = self.active_streams.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
    }

    pub fn tryReserveReaper(self: *GlobalAccounting) bool {
        while (true) {
            const cur = self.reaper_reserved.load(.acquire);
            if (cur >= self.reaper_capacity) return false;
            if (self.reaper_reserved.cmpxchgWeak(cur, cur + 1, .acq_rel, .acquire) == null) return true;
        }
    }

    pub fn releaseReaper(self: *GlobalAccounting) void {
        const prev = self.reaper_reserved.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
    }

    pub fn tryReserveOutbound(self: *GlobalAccounting, n: usize) bool {
        if (n == 0) return true;
        while (true) {
            const cur = self.outbound_bytes.load(.acquire);
            if (cur > self.max_outbound_bytes or self.max_outbound_bytes - cur < n) return false;
            if (self.outbound_bytes.cmpxchgWeak(cur, cur + n, .acq_rel, .acquire) == null) return true;
        }
    }

    pub fn releaseOutbound(self: *GlobalAccounting, n: usize) void {
        if (n == 0) return;
        const prev = self.outbound_bytes.fetchSub(n, .acq_rel);
        std.debug.assert(prev >= n);
    }

    pub fn tryReserveRequest(self: *GlobalAccounting, n: usize) bool {
        if (n == 0) return true;
        while (true) {
            const cur = self.request_bytes.load(.acquire);
            if (cur > self.max_request_bytes or self.max_request_bytes - cur < n) return false;
            if (self.request_bytes.cmpxchgWeak(cur, cur + n, .acq_rel, .acquire) == null) return true;
        }
    }

    pub fn releaseRequest(self: *GlobalAccounting, n: usize) void {
        if (n == 0) return;
        const prev = self.request_bytes.fetchSub(n, .acq_rel);
        std.debug.assert(prev >= n);
    }

    pub fn tryAdmitHandshake(self: *GlobalAccounting) bool {
        while (true) {
            const cur = self.active_handshakes.load(.acquire);
            if (cur >= self.max_handshakes) return false;
            if (self.active_handshakes.cmpxchgWeak(cur, cur + 1, .acq_rel, .acquire) == null) return true;
        }
    }

    pub fn releaseHandshake(self: *GlobalAccounting) void {
        const prev = self.active_handshakes.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
    }
};

pub const HandlerSlot = struct {
    terminal: response.SlotTerminal = .{},
    completion_owner: std.atomic.Value(u8) = .init(2), // start reported/free
    stream_id: u31 = 0,
    in_use: bool = false,
    /// True while a reaper job capacity token is held for this slot.
    reaper_reserved: bool = false,
    /// True while the handler waits on a ticket whose write is already handed
    /// to the WritePump. The pump's exit contract guarantees every queued
    /// write posts an ack (ok, fail, or fail_all), so this wait ALWAYS wakes
    /// without help — and teardown must not cancel it: canceling here is how
    /// a fully delivered response returned ConnectionClosed when the peer
    /// closed in the ack's wake-to-run gap (t-537).
    awaiting_receipt: std.atomic.Value(bool) = .init(false),
};

const live: u8 = 0;
const reaper_owned: u8 = 1;
const reported: u8 = 2;

pub const ReaperJob = struct {
    handle: std.Io.Future(void),
    owner: *std.atomic.Value(u8),
    completion: *std.Io.Queue(u31),
    actor_wake: *std.Io.Event,
    stream_id: u31,
};

comptime {
    if (@sizeOf(HandlerSlot) != limits_mod.HANDLER_SLOT_SIZE) {
        @compileError("HANDLER_SLOT_SIZE must match @sizeOf(HandlerSlot)");
    }
    if (@sizeOf(ReaperJob) != limits_mod.REAPER_JOB_SIZE) {
        @compileError("REAPER_JOB_SIZE must match @sizeOf(ReaperJob)");
    }
}

pub const ReaperPool = struct {
    io: std.Io,
    jobs: std.Io.Queue(ReaperJob),
    job_buf: []ReaperJob,
    gpa: std.mem.Allocator,

    pub fn init(gpa: std.mem.Allocator, io: std.Io, capacity: usize) !ReaperPool {
        const buf = try gpa.alloc(ReaperJob, capacity);
        return .{ .io = io, .jobs = .init(buf), .job_buf = buf, .gpa = gpa };
    }

    pub fn deinit(self: *ReaperPool) void {
        // Workers must already be joined; only free storage.
        self.gpa.free(self.job_buf);
        self.* = undefined;
    }

    pub fn worker(self: *ReaperPool) std.Io.Cancelable!void {
        while (true) {
            var job = self.jobs.getOne(self.io) catch |err| switch (err) {
                error.Closed => return,
                error.Canceled => return error.Canceled,
            };
            job.handle.cancel(self.io);
            const prev = job.owner.swap(reported, .acq_rel);
            if (prev == reaper_owned) {
                job.completion.putOneUncancelable(self.io, job.stream_id) catch {
                    // Connection teardown has already closed completion delivery.
                };
                job.actor_wake.set(self.io);
            }
        }
    }
};

pub fn serveAccepted(stream: std.Io.net.Stream, config: ConnConfig) std.Io.Cancelable!void {
    var conn = Connection.init(stream, config) catch {
        if (config.handshake_held) {
            if (config.accounting) |a| a.releaseHandshake();
        }
        stream.close(config.io);
        return;
    };
    defer conn.deinit();
    if (conn.session.stream_hooks != null) {
        conn.session.stream_hooks.?.ctx = &conn;
    }
    conn.session.rate_limiter = &conn.rates;
    conn.handshake_held = config.handshake_held;
    test_boot_ready.store(true, .release);
    // Seed read-chunk free list after channels are live.
    var i: u32 = 0;
    while (i < conn.read_pool_n) : (i += 1) {
        conn.read_free_ch.putOneUncancelable(config.io, i) catch {
            // Fresh queue capacity exactly matches the number of pool indices.
            unreachable;
        };
    }
    conn.run() catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            // Transport/protocol failure is connection-local and closes below.
        },
    };
}

const Connection = struct {
    stream: std.Io.net.Stream,
    config: ConnConfig,
    session: session_mod.Session,
    read_ch_buf: []wire_pump.WireChunk,
    write_ch_buf: []wire_pump.WireChunk,
    read_ch: std.Io.Queue(wire_pump.WireChunk) = undefined,
    write_ch: std.Io.Queue(wire_pump.WireChunk) = undefined,
    actor_wake: std.Io.Event = .unset,
    tls_server: ?tls.nonblock.Server = null,
    tls_conn: ?tls.nonblock.Connection = null,
    tls_prng: std.Random.DefaultPrng = undefined,
    tls_recv_acc: std.ArrayList(u8) = .empty,
    plaintext_scratch: []u8 = &.{},
    ciphertext_scratch: []u8 = &.{},
    handlers: []HandlerSlot,
    shutting_down: bool = false,
    /// Set by a handler after it refills scheduler slab space; cleared by the
    /// actor at the top of each iteration. Emission happens only in the
    /// actor's drainEmit, so a refill that lands between the actor's drain and
    /// its actor_wake.reset() would otherwise be a lost wakeup: the handler
    /// then waits on space that only emission can free, and the actor sleeps
    /// until an unrelated read or a protocol deadline.
    sched_refilled: std.atomic.Value(bool) = .init(false),
    grace_deadline: ?std.Io.Timestamp = null,
    /// Production fair scheduler — sole emit path for controls + DATA.
    sched: fair_scheduler.FairScheduler = undefined,
    session_mu: std.Io.Mutex = .init,
    live_handlers: std.atomic.Value(usize) = .init(0),
    reaper: ?*ReaperPool = null,
    completion_ch_buf: []u31 = &.{},
    completion_ch: std.Io.Queue(u31) = undefined,
    write_ack_buf: []wire_pump.WriteCompletion = &.{},
    write_ack_ch: std.Io.Queue(wire_pump.WriteCompletion) = undefined,
    ticket_slots: []ticket_table.TicketWait = &.{},
    tickets: ticket_table.TicketTable = undefined,
    /// Indexed parallel to handlers; only valid while slot.in_use.
    handler_joins: []?std.Io.Future(void) = &.{},
    handshake_deadline: ?std.Io.Timestamp = null,
    /// Connection-local outbound/request reservations — AckDrainer + actor under atomics.
    outbound_held: std.atomic.Value(usize) = .init(0),
    pending_outbound_held: std.atomic.Value(usize) = .init(0),
    wire_outbound_held: std.atomic.Value(usize) = .init(0),
    request_held: std.atomic.Value(usize) = .init(0),
    handshake_held: bool = false,
    socket_closed: std.atomic.Value(bool) = .init(false),
    /// Set by AckDrainer on write failure; actor owns handler terminal transition.
    writer_failed: std.atomic.Value(bool) = .init(false),
    writer_fail_handled: bool = false,
    /// Capacity waiters: one std.Io.Event per HandlerSlot (sparse IDs safe).
    space_events: []std.Io.Event = &.{},
    /// Actor-owned intent batch — filled by drainIntentsInto (no nested Session drain).
    intent_batch: []session_mod.Intent = &.{},
    rates: rates_mod.RateLimiter = .{},
    /// Fixed inbound wire chunk pool.
    read_chunk_storage: []u8 = &.{},
    read_free_buf: []u32 = &.{},
    read_free_ch: std.Io.Queue(u32) = undefined,
    read_pool_n: u32 = 0,
    /// Preallocated scratch for stream-id sweeps (no hot-path ArrayList).
    sid_scratch: []u31 = &.{},
    /// Next outbound wire frame (HEADERS/control/DATA) receives this ticket once.
    next_wire_ticket: u64 = 0,
    next_wire_slot: u32 = 0,
    pending_ticket: ?PendingTicketAttach = null,
    /// Quantum bytes to release from handler outbound after successful DATA sink.
    pending_data_outbound_release: usize = 0,
    pending_data_wake_sid: u31 = 0,

    const PendingTicketAttach = struct {
        stream_id: u31,
        ticket: u64,
        slot: u32,
        /// Complete after this many pending bytes for the stream have been written to the wire.
        remain: usize,
        end_stream: bool,
    };

    fn init(stream: std.Io.net.Stream, config: ConnConfig) !Connection {
        const gpa = config.gpa;
        var session = try session_mod.Session.init(gpa, config.limits);
        errdefer session.deinit();
        if (config.accounting != null) {
            session.stream_hooks = .{
                .ctx = undefined, // set after Connection is built — see postInitHooks
                .tryAdmit = struct {
                    fn f(ctx: *anyopaque) bool {
                        const c: *Connection = @ptrCast(@alignCast(ctx));
                        if (c.config.accounting) |a| return a.tryAdmitStream();
                        return true;
                    }
                }.f,
                .onRelease = struct {
                    fn f(ctx: *anyopaque) void {
                        const c: *Connection = @ptrCast(@alignCast(ctx));
                        if (c.config.accounting) |a| a.releaseStream();
                    }
                }.f,
                .tryReserveRequest = struct {
                    fn f(ctx: *anyopaque, n: usize) bool {
                        const c: *Connection = @ptrCast(@alignCast(ctx));
                        return c.tryReserveRequestBytes(n);
                    }
                }.f,
                .releaseRequest = struct {
                    fn f(ctx: *anyopaque, n: usize) void {
                        const c: *Connection = @ptrCast(@alignCast(ctx));
                        c.releaseRequestBytes(n);
                    }
                }.f,
            };
        }

        const read_ch_buf = try gpa.alloc(wire_pump.WireChunk, config.limits.inbound_wire_chunks_per_connection);
        errdefer gpa.free(read_ch_buf);
        const write_ch_buf = try gpa.alloc(wire_pump.WireChunk, config.limits.inbound_wire_chunks_per_connection);
        errdefer gpa.free(write_ch_buf);
        const handlers = try gpa.alloc(HandlerSlot, config.limits.max_streams_per_connection);
        errdefer gpa.free(handlers);
        const handler_joins = try gpa.alloc(?std.Io.Future(void), config.limits.max_streams_per_connection);
        errdefer gpa.free(handler_joins);
        const completion_ch_buf = try gpa.alloc(u31, config.limits.max_streams_per_connection);
        errdefer gpa.free(completion_ch_buf);
        const write_ack_buf = try gpa.alloc(wire_pump.WriteCompletion, config.limits.control_entries_per_connection + config.limits.max_streams_per_connection);
        errdefer gpa.free(write_ack_buf);
        const ticket_slots = try gpa.alloc(ticket_table.TicketWait, config.limits.control_entries_per_connection + config.limits.max_streams_per_connection);
        errdefer gpa.free(ticket_slots);
        const plaintext_scratch = try gpa.alloc(u8, limits_mod.TLS_PLAINTEXT_SCRATCH_SIZE);
        errdefer gpa.free(plaintext_scratch);
        const ciphertext_scratch = try gpa.alloc(u8, limits_mod.WIRE_CHUNK_SIZE);
        errdefer gpa.free(ciphertext_scratch);
        const n_chunks: u32 = @intCast(config.limits.inbound_wire_chunks_per_connection);
        const read_chunk_storage = try gpa.alloc(u8, @as(usize, n_chunks) * limits_mod.WIRE_CHUNK_SIZE);
        errdefer gpa.free(read_chunk_storage);
        const read_free_buf = try gpa.alloc(u32, n_chunks);
        errdefer gpa.free(read_free_buf);
        const sid_scratch = try gpa.alloc(u31, config.limits.max_streams_per_connection);
        errdefer gpa.free(sid_scratch);
        const space_events = try gpa.alloc(std.Io.Event, config.limits.max_streams_per_connection);
        errdefer gpa.free(space_events);
        @memset(space_events, .unset);
        const intent_batch = try gpa.alloc(session_mod.Intent, @max(config.limits.intent_entries_per_connection, 16));
        errdefer gpa.free(intent_batch);
        var tls_recv_acc: std.ArrayList(u8) = .empty;
        errdefer tls_recv_acc.deinit(gpa);
        if (config.mode == .tls_h2) {
            try tls_recv_acc.ensureTotalCapacityPrecise(gpa, config.limits.tls_recv_acc_bytes);
        }

        if (config.mode == .tls_h2) {
            if (config.tls_auth == null) return error.InvalidConfig;
        }

        const term_cap = @max(config.limits.control_entries_per_connection / 8, 16);
        var sched = try fair_scheduler.FairScheduler.init(
            gpa,
            config.io,
            config.limits.control_bytes_per_connection,
            config.limits.control_entries_per_connection,
            term_cap,
            config.limits.control_entries_per_connection,
            config.limits.max_streams_per_connection,
            config.limits.outbound_bytes_per_stream,
        );
        errdefer sched.deinit();

        var self: Connection = .{
            .stream = stream,
            .config = config,
            .session = session,
            .read_ch_buf = read_ch_buf,
            .write_ch_buf = write_ch_buf,
            .handlers = handlers,
            .handler_joins = handler_joins,
            .completion_ch_buf = completion_ch_buf,
            .write_ack_buf = write_ack_buf,
            .ticket_slots = ticket_slots,
            .reaper = config.reaper,
            .plaintext_scratch = plaintext_scratch,
            .ciphertext_scratch = ciphertext_scratch,
            .sched = sched,
            .read_chunk_storage = read_chunk_storage,
            .read_free_buf = read_free_buf,
            .read_pool_n = n_chunks,
            .sid_scratch = sid_scratch,
            .space_events = space_events,
            .intent_batch = intent_batch,
            .tls_recv_acc = tls_recv_acc,
        };
        @memset(self.handlers, .{});
        @memset(self.handler_joins, null);
        self.tickets = ticket_table.TicketTable.init(config.io, self.ticket_slots);
        self.read_ch = .init(self.read_ch_buf);
        self.write_ch = .init(self.write_ch_buf);
        self.completion_ch = .init(self.completion_ch_buf);
        self.write_ack_ch = .init(self.write_ack_buf);
        self.read_free_ch = .init(self.read_free_buf);
        return self;
    }

    fn deinit(self: *Connection) void {
        const io = self.config.io;
        // Must not free while handlers still live.
        std.debug.assert(self.live_handlers.load(.acquire) == 0);
        if (self.handshake_held) {
            if (self.config.accounting) |a| a.releaseHandshake();
            self.handshake_held = false;
        }
        // Drain channels before freeing their backing buffers.
        while (io_queue.tryGet(wire_pump.WireChunk, &self.write_ch, io)) |chunk| {
            if (chunk.bytes.len != 0) self.config.gpa.free(chunk.bytes);
            // Release amounts without AckDrainer (teardown path).
            self.applyOutboundRelease(chunk.outbound_release, .wire);
            if (chunk.control_entry) self.applyControlRelease(chunk.control_release, true);
        }
        while (io_queue.tryGet(wire_pump.WireChunk, &self.read_ch, io)) |chunk| {
            if (chunk.pool_index) |idx| {
                self.read_free_ch.putOneUncancelable(io, idx) catch {
                    // Every queued read chunk owns one removed pool index.
                    unreachable;
                };
            } else if (chunk.bytes.len != 0) {
                self.config.gpa.free(chunk.bytes);
            }
        }
        // Late reaper posts can arrive after shutdownHandlers; never discard without releaseSlot.
        while (io_queue.tryGet(u31, &self.completion_ch, io)) |sid| {
            self.releaseSlot(sid);
        }
        for (self.handlers) |s| std.debug.assert(!s.in_use);
        while (io_queue.tryGet(wire_pump.WriteCompletion, &self.write_ack_ch, io)) |ack| {
            self.applyOutboundRelease(ack.outbound_release, .wire);
            if (ack.control_entry) self.applyControlRelease(ack.control_release, true);
        }

        self.session.deinit();
        const RelCtx = struct {
            c: *Connection,
            fn cb(ctx: *anyopaque, _: u31, pw: *fair_scheduler.DataPending) void {
                const rc: *@This() = @ptrCast(@alignCast(ctx));
                rc.c.applyOutboundRelease(pw.len, .pending);
            }
        };
        var rel: RelCtx = .{ .c = self };
        self.sched.forEachPending(@ptrCast(&rel), RelCtx.cb);
        self.sched.deinit();
        self.tls_recv_acc.deinit(self.config.gpa);

        self.config.gpa.free(self.read_ch_buf);
        self.config.gpa.free(self.write_ch_buf);
        self.config.gpa.free(self.handlers);
        self.config.gpa.free(self.handler_joins);
        self.config.gpa.free(self.completion_ch_buf);
        if (self.write_ack_buf.len != 0) self.config.gpa.free(self.write_ack_buf);
        if (self.ticket_slots.len != 0) self.config.gpa.free(self.ticket_slots);
        if (self.plaintext_scratch.len != 0) self.config.gpa.free(self.plaintext_scratch);
        if (self.ciphertext_scratch.len != 0) self.config.gpa.free(self.ciphertext_scratch);
        if (self.read_chunk_storage.len != 0) self.config.gpa.free(self.read_chunk_storage);
        if (self.read_free_buf.len != 0) self.config.gpa.free(self.read_free_buf);
        if (self.sid_scratch.len != 0) self.config.gpa.free(self.sid_scratch);
        if (self.space_events.len != 0) self.config.gpa.free(self.space_events);
        if (self.intent_batch.len != 0) self.config.gpa.free(self.intent_batch);
        const held = self.outbound_held.load(.acquire);
        const pending_held = self.pending_outbound_held.load(.acquire);
        const wire_held = self.wire_outbound_held.load(.acquire);
        std.debug.assert(held == pending_held + wire_held);
        std.debug.assert(held == 0);
        if (!self.socket_closed.load(.acquire)) {
            self.stream.close(io);
            self.socket_closed.store(true, .release);
        }
    }

    fn tryReserveRequestBytes(self: *Connection, n: usize) bool {
        if (n == 0) return true;
        const lim = self.config.limits.request_bytes_per_connection;
        while (true) {
            const cur = self.request_held.load(.acquire);
            if (cur > lim or lim - cur < n) return false;
            if (self.config.accounting) |a| {
                if (!a.tryReserveRequest(n)) return false;
            }
            if (self.request_held.cmpxchgWeak(cur, cur + n, .acq_rel, .acquire) == null) return true;
            if (self.config.accounting) |a| a.releaseRequest(n);
        }
    }

    fn releaseRequestBytes(self: *Connection, n: usize) void {
        if (n == 0) return;
        const prev = self.request_held.fetchSub(n, .acq_rel);
        std.debug.assert(prev >= n);
        if (self.config.accounting) |a| a.releaseRequest(n);
    }

    const OutboundKind = enum { pending, wire };

    fn tryReserveOutboundBytes(self: *Connection, n: usize, kind: OutboundKind) bool {
        if (n == 0) return true;
        const lim = self.config.limits.outbound_bytes_per_connection;
        while (true) {
            const cur = self.outbound_held.load(.acquire);
            if (cur > lim or lim - cur < n) return false;
            if (self.config.accounting) |a| {
                if (!a.tryReserveOutbound(n)) return false;
            }
            if (self.outbound_held.cmpxchgWeak(cur, cur + n, .acq_rel, .acquire) == null) {
                switch (kind) {
                    .pending => {
                        const kind_prev = self.pending_outbound_held.fetchAdd(n, .acq_rel);
                        std.debug.assert(kind_prev <= lim - n);
                    },
                    .wire => {
                        const kind_prev = self.wire_outbound_held.fetchAdd(n, .acq_rel);
                        std.debug.assert(kind_prev <= lim - n);
                    },
                }
                return true;
            }
            if (self.config.accounting) |a| a.releaseOutbound(n);
        }
    }

    fn applyOutboundRelease(self: *Connection, n: usize, kind: OutboundKind) void {
        if (n == 0) return;
        const kind_prev = switch (kind) {
            .pending => self.pending_outbound_held.fetchSub(n, .acq_rel),
            .wire => self.wire_outbound_held.fetchSub(n, .acq_rel),
        };
        std.debug.assert(kind_prev >= n);
        const prev = self.outbound_held.fetchSub(n, .acq_rel);
        std.debug.assert(prev >= n);
        if (self.config.accounting) |a| a.releaseOutbound(n);
    }

    fn applyControlRelease(self: *Connection, n: usize, entry: bool) void {
        self.sched.ctrl.release(n, entry);
    }

    fn ackDrainer(self: *Connection) void {
        while (true) {
            const ack = self.write_ack_ch.getOne(self.config.io) catch break;
            self.applyOutboundRelease(ack.outbound_release, .wire);
            if (ack.control_entry) self.applyControlRelease(ack.control_release, true);
            if (ack.fail_all) {
                self.writer_failed.store(true, .release);
                self.tickets.failAll();
                // Do NOT touch handlers[] — actor applies connection_closed terminals.
                self.wakeAllSpace();
            } else if (ack.ticket != 0) {
                self.tickets.complete(ack.ticket_slot, ack.ticket, ack.ok);
            }
            if (ack.outbound_release != 0) self.wakeAllSpace();
            self.actor_wake.set(self.config.io);
            if (ack.shutdown) break;
        }
    }

    /// Capacity waiter index = admitted HandlerSlot index (sparse stream IDs safe).
    fn spaceIndex(self: *Connection, stream_id: u31) ?usize {
        return self.slotIndex(stream_id);
    }

    fn wakeStreamSpace(self: *Connection, stream_id: u31) void {
        if (self.spaceIndex(stream_id)) |i| {
            if (i < self.space_events.len) self.space_events[i].set(self.config.io);
        }
    }

    fn wakeAllSpace(self: *Connection) void {
        for (self.space_events) |*event| event.set(self.config.io);
    }

    /// AckDrainer-signaled writer failure: terminate connection, reset every stream,
    /// release reservations exactly once, wake waiters. Idempotent.
    fn handleWriterFailed(self: *Connection) void {
        if (!self.writer_failed.load(.acquire)) return;
        if (self.writer_fail_handled) return;
        self.writer_fail_handled = true;
        test_observed_writer_fail_handled.store(true, .release);

        for (self.handlers) |*slot| {
            if (!slot.in_use) continue;
            // Active pump-write failure → WriteFailed (internal), not ConnectionClosed.
            // Subsequent ops still see writer_failed / closed connection.
            slot.terminal.setCause(.internal);
        }

        // Drop scheduler pending DATA and release outbound once.
        var drop_ids: usize = 0;
        const CollectCtx = struct {
            c: *Connection,
            n: *usize,
            fn cb(ctx: *anyopaque, sid: u31, _: *fair_scheduler.DataPending) void {
                const cc: *@This() = @ptrCast(@alignCast(ctx));
                if (cc.n.* < cc.c.sid_scratch.len) {
                    cc.c.sid_scratch[cc.n.*] = sid;
                    cc.n.* += 1;
                }
            }
        };
        var collect: CollectCtx = .{ .c = self, .n = &drop_ids };
        self.sched.forEachPending(@ptrCast(&collect), CollectCtx.cb);
        var di: usize = 0;
        while (di < drop_ids) : (di += 1) {
            const sid = self.sid_scratch[di];
            if (self.sched.findPending(sid)) |pw| {
                if (pw.flush_ticket != 0) {
                    self.tickets.wake(pw.flush_slot, pw.flush_ticket, false);
                }
                self.applyOutboundRelease(pw.len, .pending);
                _ = self.sched.removePending(sid);
            }
        }
        // Drain control queues without writing (connection is dead).
        while (self.sched.popTerminal()) |e| {
            self.sched.ctrl.release(e.control_n, true);
            self.config.gpa.free(e.payload);
        }
        while (self.sched.popOrdinary()) |e| {
            self.sched.ctrl.release(e.control_n, true);
            self.config.gpa.free(e.payload);
        }

        // Close/reset every Session stream → global stream accounting releases via hooks.
        var sids: usize = 0;
        var sit = self.session.streams.iterator();
        while (sit.next()) |e| {
            if (e.value_ptr.state == .closed or e.value_ptr.state == .idle) continue;
            if (sids < self.sid_scratch.len) {
                self.sid_scratch[sids] = e.key_ptr.*;
                sids += 1;
            }
        }
        var si: usize = 0;
        while (si < sids) : (si += 1) {
            // Teardown continues when a stream was concurrently made terminal.
            self.session.applyCommand(.{ .reset_stream = .{ .stream_id = self.sid_scratch[si], .code = .cancel } }) catch {};
        }
        // Session teardown below is authoritative if GOAWAY cannot be enqueued.
        self.session.applyCommand(.{ .goaway = .{ .last_stream_id = self.session.last_processed_stream, .code = .internal_error } }) catch {};
        // Consume resulting intents into free (no further writes).
        const n = self.session.drainIntentsInto(self.intent_batch);
        for (self.intent_batch[0..n]) |*intent| switch (intent.*) {
            .outbound_frame => |f| self.config.gpa.free(f.payload),
            .dispatch_request => |d| {
                for (d.headers) |h| {
                    self.config.gpa.free(@constCast(h.name));
                    self.config.gpa.free(@constCast(h.value));
                }
                self.config.gpa.free(d.headers);
                if (d.trailers.len != 0) {
                    for (d.trailers) |h| {
                        self.config.gpa.free(@constCast(h.name));
                        self.config.gpa.free(@constCast(h.value));
                    }
                    self.config.gpa.free(d.trailers);
                }
                if (d.body.len != 0) self.config.gpa.free(d.body);
            },
            else => {},
        };

        // Do NOT drain write_ch here. WritePump is the sole consumer while the
        // connection is live; stealing chunks races AckDrainer wire releases
        // (and can double-free payload bytes). Queued wire is released via
        // WritePump failDrain/ack or Connection.deinit after pumps join.
        // Writer queue closure already terminates the failed transport.
        _ = io_queue.tryPut(wire_pump.WireChunk, &self.write_ch, self.config.io, .{
            .bytes = &.{},
            .len = 0,
        });

        self.wakeAllSpace();
        self.session.terminal = .transport;
    }

    fn reserveTicket(self: *Connection) response.ResponseError!struct { u64, u32 } {
        return self.tickets.reserve() catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.WriteFailed => error.WriteFailed,
        };
    }

    fn waitTicket(self: *Connection, slot_i: u32, terminal: *response.SlotTerminal) response.ResponseError!void {
        return self.tickets.wait(slot_i, terminal);
    }

    fn checkSlowConsumers(self: *Connection) !void {
        const limit_ns = self.config.limits.slow_consumer_timeout_ns;
        if (limit_ns == 0) return;
        const now = nowNs(self.config.io);
        var n_stale: usize = 0;
        const StaleCtx = struct {
            c: *Connection,
            now: u64,
            limit_ns: u64,
            n: *usize,
            fn cb(ctx: *anyopaque, sid: u31, pw: *fair_scheduler.DataPending) void {
                const sc: *@This() = @ptrCast(@alignCast(ctx));
                if (pw.len == 0) return;
                if (pw.last_progress_ns == 0) {
                    pw.last_progress_ns = sc.now;
                    return;
                }
                if (sc.now -% pw.last_progress_ns >= sc.limit_ns) {
                    if (sc.n.* < sc.c.sid_scratch.len) {
                        sc.c.sid_scratch[sc.n.*] = sid;
                        sc.n.* += 1;
                    }
                }
            }
        };
        var stale: StaleCtx = .{ .c = self, .now = now, .limit_ns = limit_ns, .n = &n_stale };
        self.sched.forEachPending(@ptrCast(&stale), StaleCtx.cb);
        var i: usize = 0;
        while (i < n_stale) : (i += 1) {
            const sid = self.sid_scratch[i];
            if (self.slotIndex(sid)) |si| {
                self.handlers[si].terminal.setCause(.slow_consumer);
            }
            // Local terminal/accounting cleanup remains required if the stream already closed.
            self.session.applyCommand(.{ .reset_stream = .{ .stream_id = sid, .code = .cancel } }) catch {};
            if (self.sched.findPending(sid)) |pw| {
                // Exact terminal: wake flush waiter so wait maps SlowConsumer via SlotTerminal.
                if (pw.flush_ticket != 0) {
                    self.tickets.wake(pw.flush_slot, pw.flush_ticket, false);
                }
                self.applyOutboundRelease(pw.len, .pending);
                _ = self.sched.removePending(sid);
            }
            self.wakeStreamSpace(sid);
            try self.processIntents();
        }
    }

    fn drainCompletions(self: *Connection) void {
        if (test_hold_completion_drain.load(.acquire)) return;
        while (io_queue.tryGet(u31, &self.completion_ch, self.config.io)) |sid| {
            self.releaseSlot(sid);
        }
    }

    fn nextDeadlineNs(self: *Connection) ?u64 {
        var next = self.session.nextIdleDeadlineNs();
        if (self.sched.nextSlowDeadlineNs(self.config.limits.slow_consumer_timeout_ns)) |deadline| {
            if (next == null or deadline < next.?) next = deadline;
        }
        if (self.grace_deadline) |deadline| {
            const ns: u64 = @intCast(deadline.nanoseconds);
            if (next == null or ns < next.?) next = ns;
        }
        const canary_tick = test_polling_canary_tick_ns.load(.acquire);
        if (canary_tick != 0) {
            const deadline = nowNs(self.config.io) +% canary_tick;
            if (next == null or deadline < next.?) next = deadline;
        }
        return next;
    }

    const Activity = union(enum) {
        actor: std.Io.Cancelable!void,
        shutdown: std.Io.Cancelable!void,
        timer: std.Io.Cancelable!void,
    };

    fn waitEvent(event: *std.Io.Event, io: std.Io) std.Io.Cancelable!void {
        return event.wait(io);
    }

    fn waitTimer(timeout: std.Io.Timeout, io: std.Io) std.Io.Cancelable!void {
        return timeout.sleep(io);
    }

    fn waitForActivity(self: *Connection) !void {
        const io = self.config.io;
        if (test_hold_before_actor_wait.swap(false, .acq_rel)) {
            test_actor_waiting.set(io);
            try test_release_actor_wait.wait(io);
        }
        var result_buf: [3]Activity = undefined;
        var select = std.Io.Select(Activity).init(io, &result_buf);
        errdefer select.cancelDiscard();
        try select.concurrent(.actor, waitEvent, .{ &self.actor_wake, io });
        if (self.config.shutdown_event) |event| {
            try select.concurrent(.shutdown, waitEvent, .{ event, io });
        }
        if (self.nextDeadlineNs()) |deadline_ns| {
            const now = nowNs(io);
            if (deadline_ns <= now) {
                select.cancelDiscard();
                return;
            }
            const timeout: std.Io.Timeout = .{ .deadline = .{
                .raw = .fromNanoseconds(@intCast(deadline_ns)),
                .clock = .awake,
            } };
            try select.concurrent(.timer, waitTimer, .{ timeout, io });
        }
        const selected = try select.await();
        defer select.cancelDiscard();
        switch (selected) {
            inline else => |result| try result,
        }
    }

    fn run(self: *Connection) !void {
        const gpa = self.config.gpa;
        const io = self.config.io;

        var read_pump: wire_pump.ReadPump = .{
            .io = io,
            .stream = self.stream,
            .to_actor = &self.read_ch,
            .actor_wake = &self.actor_wake,
            .chunk_storage = self.read_chunk_storage,
            .n_chunks = self.read_pool_n,
            .free_indices = &self.read_free_ch,
        };
        var write_pump: wire_pump.WritePump = .{
            .io = io,
            .stream = self.stream,
            .from_actor = &self.write_ch,
            .completions = &self.write_ack_ch,
            .actor_wake = &self.actor_wake,
            .gpa = gpa,
            .test_delay_ms = test_write_delay_ms,
            .test_fail_after = test_write_fail_after,
        };
        var read_handle = try io.concurrent(wire_pump.ReadPump.run, .{&read_pump});
        errdefer read_handle.cancel(io);
        var write_handle = try io.concurrent(wire_pump.WritePump.run, .{&write_pump});
        errdefer write_handle.cancel(io);
        var ack_handle = try io.concurrent(ackDrainerEntry, .{self});
        errdefer ack_handle.cancel(io);

        defer {
            read_pump.stop();
            write_pump.stop();
            _ = self.write_ch.putUncancelable(io, &.{.{ .bytes = &.{}, .len = 0 }}, 0) catch {
                // A closed writer queue already terminates the pump.
            };
            self.stream.shutdown(io, .both) catch {
                // Shutdown is best-effort; task cancellation is the authoritative unblock.
            };
            write_handle.cancel(io);
            read_handle.cancel(io);
            // AckDrainer exits on shutdown completion from write pump.
            ack_handle.await(io);
            if (!self.socket_closed.swap(true, .acq_rel)) {
                self.stream.close(io);
            }
        }

        if (self.config.mode == .tls_h2) {
            const auth = self.config.tls_auth orelse return error.InvalidConfig;
            var seed: [8]u8 = undefined;
            io.random(&seed);
            self.tls_prng = std.Random.DefaultPrng.init(@as(u64, @bitCast(seed)));
            self.tls_server = tls.nonblock.Server.init(.{
                .auth = auth,
                .alpn_protocols = &tls_edge.alpn_list,
                .rng = self.tls_prng.random(),
                .now = .zero,
            });
            self.handshake_deadline = std.Io.Timestamp.fromNanoseconds(
                @as(i96, nowNs(io) +% 5 * std.time.ns_per_s),
            );
            try self.tlsHandshakeViaPumps();
            if (self.handshake_held) {
                if (self.config.accounting) |a| a.releaseHandshake();
                self.handshake_held = false;
            }
        }

        try self.flushSessionIntents();

        if (self.config.mode == .h2c) {
            self.handshake_deadline = std.Io.Timestamp.fromNanoseconds(
                @as(i96, nowNs(io) +% self.config.limits.preface_timeout_ns),
            );
            try self.waitH2cPreface();
        }

        while (true) {
            _ = self.sched_refilled.swap(false, .acq_rel);
            self.drainCompletions();

            {
                self.session_mu.lockUncancelable(io);
                defer self.session_mu.unlock(io);
                if (self.writer_failed.load(.acquire)) self.handleWriterFailed();
                if (self.session.terminal != .none) break;
                const now = nowNs(io);
                self.session.edge_now_ns = now;
                try self.session.checkIdleDeadlines(now);
                try self.maybeBeginGraceful();
                try self.checkSlowConsumers();
                self.drainEmit() catch {
                    // Fail-closed: Session may already be debited — terminate connection.
                    self.writer_failed.store(true, .release);
                    self.handleWriterFailed();
                };
            }

            if (self.session.terminal != .none) break;
            if (self.shutting_down and self.session.grace_phase == .phase2 and
                self.sched.pendingCount() == 0 and self.live_handlers.load(.acquire) == 0)
            {
                self.session_mu.lockUncancelable(io);
                defer self.session_mu.unlock(io);
                try self.finishGraceful();
                break;
            }

            var maybe_chunk = io_queue.tryGet(wire_pump.WireChunk, &self.read_ch, io);
            if (maybe_chunk == null) {
                // Reset before rechecking every producer-owned source; this closes
                // the set-before-reset lost-wakeup race.
                self.actor_wake.reset();
                self.drainCompletions();
                maybe_chunk = io_queue.tryGet(wire_pump.WireChunk, &self.read_ch, io);
                if (maybe_chunk == null and
                    !self.sched_refilled.load(.acquire) and
                    !self.writer_failed.load(.acquire) and
                    !(self.config.shutdown_flag != null and self.config.shutdown_flag.?.load(.acquire)))
                {
                    try self.waitForActivity();
                }
                if (maybe_chunk == null) continue;
            }
            const chunk = maybe_chunk.?;
            if (chunk.len == 0 and chunk.bytes.len == 0) break;
            defer {
                if (chunk.pool_index) |idx| {
                    self.read_free_ch.putOneUncancelable(io, idx) catch {
                        // Each consumed pooled chunk returns exactly one index.
                        unreachable;
                    };
                } else if (chunk.bytes.len != 0) {
                    gpa.free(chunk.bytes);
                }
            }

            self.session_mu.lockUncancelable(io);
            defer self.session_mu.unlock(io);
            self.session.edge_now_ns = nowNs(io);
            if (self.config.mode == .tls_h2 and self.tls_conn == null) {
                try self.appendTlsInput(chunk.bytes[0..chunk.len]);
            } else if (self.tls_conn != null) {
                try self.appendTlsInput(chunk.bytes[0..chunk.len]);
                try self.driveDecrypt();
            } else {
                try self.session.ingest(chunk.bytes[0..chunk.len]);
                try self.processIntents();
            }
            if (self.session.terminal != .none) break;
        }

        // shutdownHandlers returns only once every slot went through releaseSlot, so
        // both counters have already been decremented for this connection. Storing 0
        // here would clobber connections still serving on the same process.
        try self.shutdownHandlers();

        if (self.tls_conn) |*tc| {
            const res = tls_edge.connectionClose(tc, self.ciphertext_scratch);
            if (res.ciphertext_len > 0) {
                const out = try gpa.dupe(u8, self.ciphertext_scratch[0..res.ciphertext_len]);
                self.sendAccountedWire(out, true, 0, 0, 0, false) catch {
                    // The connection is already terminal; close-notify is best-effort.
                };
            }
        }
    }

    fn ackDrainerEntry(self: *Connection) void {
        self.ackDrainer();
    }

    const ReceiveResult = union(enum) {
        read: (std.Io.QueueClosedError || std.Io.Cancelable)!wire_pump.WireChunk,
        shutdown: std.Io.Cancelable!void,
        timer: std.Io.Cancelable!void,
    };

    fn receiveRead(queue: *std.Io.Queue(wire_pump.WireChunk), io: std.Io) (std.Io.QueueClosedError || std.Io.Cancelable)!wire_pump.WireChunk {
        return queue.getOne(io);
    }

    fn receiveUntilDeadline(self: *Connection) !wire_pump.WireChunk {
        const io = self.config.io;
        var result_buf: [3]ReceiveResult = undefined;
        var select = std.Io.Select(ReceiveResult).init(io, &result_buf);
        errdefer select.cancelDiscard();
        try select.concurrent(.read, receiveRead, .{ &self.read_ch, io });
        if (self.config.shutdown_event) |event| {
            try select.concurrent(.shutdown, waitEvent, .{ event, io });
        }
        if (self.handshake_deadline) |deadline| {
            if (nowNs(io) >= @as(u64, @intCast(deadline.nanoseconds))) {
                select.cancelDiscard();
                return error.TlsHandshakeTimeout;
            }
            const timeout: std.Io.Timeout = .{ .deadline = .{ .raw = deadline, .clock = .awake } };
            try select.concurrent(.timer, waitTimer, .{ timeout, io });
        }
        const selected = try select.await();
        defer select.cancelDiscard();
        return switch (selected) {
            .read => |result| result catch |err| switch (err) {
                error.Closed => error.ConnectionClosed,
                error.Canceled => error.Canceled,
            },
            .shutdown => |result| blk: {
                try result;
                break :blk error.ConnectionClosed;
            },
            .timer => |result| blk: {
                try result;
                break :blk error.TlsHandshakeTimeout;
            },
        };
    }

    fn recycleReadChunk(self: *Connection, chunk: wire_pump.WireChunk) void {
        if (chunk.pool_index) |idx| {
            self.read_free_ch.putOneUncancelable(self.config.io, idx) catch {
                // A received pooled chunk always owns one free-list vacancy.
                unreachable;
            };
        } else if (chunk.bytes.len != 0) {
            self.config.gpa.free(chunk.bytes);
        }
    }

    fn appendTlsInput(self: *Connection, bytes: []const u8) !void {
        const limit = self.config.limits.tls_recv_acc_bytes;
        const held = self.tls_recv_acc.items.len;
        if (held > limit or bytes.len > limit - held) return error.TlsInputTooLarge;
        self.tls_recv_acc.appendSliceAssumeCapacity(bytes);
    }

    fn waitH2cPreface(self: *Connection) !void {
        while (self.session.parser.expecting_preface) {
            const chunk = self.receiveUntilDeadline() catch |err| {
                if (err == error.TlsHandshakeTimeout) return error.PrefaceTimeout;
                return err;
            };
            defer self.recycleReadChunk(chunk);
            if (chunk.len == 0) return error.ConnectionClosed;
            self.session_mu.lockUncancelable(self.config.io);
            {
                defer self.session_mu.unlock(self.config.io);
                try self.session.ingest(chunk.bytes[0..chunk.len]);
                try self.processIntents();
                if (self.session.terminal != .none) return error.ConnectionClosed;
            }
        }
        self.handshake_deadline = null;
    }

    fn tlsHandshakeViaPumps(self: *Connection) !void {
        const gpa = self.config.gpa;
        const srv = &(self.tls_server orelse return error.InvalidConfig);
        while (true) {
            if (self.handshake_deadline) |dl| {
                if (nowNs(self.config.io) >= @as(u64, @intCast(dl.nanoseconds))) {
                    return error.TlsHandshakeTimeout;
                }
            }
            const drive = tls_edge.serverDrive(srv, self.tls_recv_acc.items, self.ciphertext_scratch);
            if (drive.ciphertext_len > 0) {
                const out = try gpa.dupe(u8, self.ciphertext_scratch[0..drive.ciphertext_len]);
                try self.sendAccountedWire(out, true, 0, 0, 0, false);
            }
            if (drive.consumed > 0) {
                const rest = self.tls_recv_acc.items.len - drive.consumed;
                if (rest > 0) {
                    @memmove(self.tls_recv_acc.items[0..rest], self.tls_recv_acc.items[drive.consumed..][0..rest]);
                }
                self.tls_recv_acc.shrinkRetainingCapacity(rest);
            }
            switch (drive.status) {
                .complete => break,
                .need_input => {
                    const chunk = try self.receiveUntilDeadline();
                    defer self.recycleReadChunk(chunk);
                    if (chunk.len == 0) return error.ConnectionClosed;
                    try self.appendTlsInput(chunk.bytes[0..chunk.len]);
                },
                .peer_closed => return error.ConnectionClosed,
                .tls_error => return error.TlsHandshakeFailed,
            }
        }
        if (!tls_edge.requireH2(srv)) return error.TlsHandshakeFailed;
        const cipher = try tls_edge.serverTakeCipher(srv);
        self.tls_conn = tls.nonblock.Connection.init(cipher);
        self.handshake_deadline = null;
        if (self.tls_recv_acc.items.len > 0) {
            // A client that pipelines its h2 preface and first request into the
            // handshake flight (curl, every browser) gets its handler DISPATCHED
            // by this decrypt. From that instant the handler encrypts its
            // response under session_mu — so this drive must hold the same lock,
            // or two tasks run the one TLS cipher concurrently and the response
            // corrupts (t-538: undefined inner.output mid-encrypt, SIGSEGV).
            self.session_mu.lockUncancelable(self.config.io);
            defer self.session_mu.unlock(self.config.io);
            try self.driveDecrypt();
        }
    }

    fn allocSlot(self: *Connection, stream_id: u31) ?*HandlerSlot {
        for (self.handlers, 0..) |*slot, i| {
            if (!slot.in_use) {
                slot.in_use = true;
                _ = test_observed_slots_in_use.fetchAdd(1, .acq_rel);
                slot.stream_id = stream_id;
                slot.terminal.clear();
                _ = slot.terminal.generation.fetchAdd(1, .acq_rel);
                slot.completion_owner.store(live, .release);
                slot.reaper_reserved = false;
                self.handler_joins[i] = null;
                if (i < self.space_events.len) self.space_events[i].reset();
                return slot;
            }
        }
        return null;
    }

    fn slotIndex(self: *Connection, stream_id: u31) ?usize {
        for (self.handlers, 0..) |slot, i| {
            if (slot.in_use and slot.stream_id == stream_id) return i;
        }
        return null;
    }

    fn releaseSlot(self: *Connection, stream_id: u31) void {
        if (self.slotIndex(stream_id)) |i| {
            const slot = &self.handlers[i];
            if (self.handler_joins[i]) |*handle| handle.await(self.config.io);
            self.handler_joins[i] = null;
            if (slot.reaper_reserved) {
                if (self.config.accounting) |a| a.releaseReaper();
                slot.reaper_reserved = false;
            }
            slot.in_use = false;
            _ = test_observed_slots_in_use.fetchSub(1, .acq_rel);
            slot.terminal.clear();
            slot.completion_owner.store(reported, .release);
            // Event state is reset only when this slot is admitted again; every
            // waiter rechecks capacity and terminal state under session_mu.
        }
    }

    fn enqueueReaperOrFail(self: *Connection, slot: *HandlerSlot, handle: std.Io.Future(void), stream_id: u31) void {
        if (self.reaper) |pool| {
            var owned_handle = handle;
            const queued = io_queue.tryPut(ReaperJob, &pool.jobs, self.config.io, .{
                .handle = owned_handle,
                .owner = &slot.completion_owner,
                .completion = &self.completion_ch,
                .actor_wake = &self.actor_wake,
                .stream_id = stream_id,
            });
            if (!queued) {
                // Invariant: reserved capacity must make this impossible.
                std.debug.assert(false);
                owned_handle.cancel(self.config.io);
                slot.completion_owner.store(reported, .release);
                // This invariant-failure path already owns connection and slot teardown.
                self.session.applyCommand(.{ .goaway = .{ .code = .internal_error, .last_stream_id = self.session.last_processed_stream } }) catch {};
                // No wire recovery is possible after the reserved reaper queue rejected the job.
                self.processIntents() catch {};
                self.releaseSlot(stream_id);
            }
        } else {
            var h = handle;
            h.cancel(self.config.io);
            slot.completion_owner.store(reported, .release);
            self.releaseSlot(stream_id);
        }
    }

    fn cancelHandler(self: *Connection, stream_id: u31, cause: response.TerminalCause) void {
        const i = self.slotIndex(stream_id) orelse return;
        const slot = &self.handlers[i];
        slot.terminal.setCause(cause);
        self.wakeHandlerWaiters(stream_id);
        // A handler waiting on a wire receipt has a GUARANTEED wake: the
        // WritePump acks or fail-drains every queued chunk on every exit path,
        // and wakeHandlerWaiters above failed any ticket still in a pending.
        // Canceling that wait is how a fully delivered response returned
        // ConnectionClosed (t-537) — leave it to finish; its natural return
        // posts the completion.
        if (slot.awaiting_receipt.load(.acquire)) return;
        // Only CAS to reaper_owned when a JoinHandle is present to transfer. Otherwise
        // cooperative cancel via cause; natural return reports the slot.
        // CAS-without-join left ownership stuck: handler exits without posting, reaper
        // never runs, releaseSlot never runs → reaper_reserved leak.
        const handle = self.handler_joins[i] orelse return;
        const prev = slot.completion_owner.cmpxchgStrong(live, reaper_owned, .acq_rel, .acquire);
        if (prev) |_| return;
        self.handler_joins[i] = null;
        self.enqueueReaperOrFail(slot, handle, stream_id);
    }

    fn wakeHandlerWaiters(self: *Connection, stream_id: u31) void {
        if (self.sched.findPending(stream_id)) |pw| {
            if (pw.flush_ticket != 0) {
                const t = pw.flush_ticket;
                const s = pw.flush_slot;
                pw.flush_ticket = 0;
                pw.flush_slot = 0;
                self.tickets.wake(s, t, false);
            }
            // A terminal stream can never emit its queued body. Drop that
            // scheduler ownership now; otherwise accounting remains charged
            // until the whole connection is destroyed.
            const pending_len = pw.len;
            _ = self.sched.removePending(stream_id);
            self.applyOutboundRelease(pending_len, .pending);
        }
        self.wakeStreamSpace(stream_id);
    }

    fn shutdownHandlers(self: *Connection) !void {
        for (self.handlers, 0..) |*slot, i| {
            if (!slot.in_use) continue;
            slot.terminal.setCause(.server_shutdown);
            self.wakeHandlerWaiters(slot.stream_id);
            // See cancelHandler: a receipt wait always wakes on its own — the
            // pump acks or fail-drains every queued chunk — and the natural
            // return posts the completion this loop's tail waits for.
            if (slot.awaiting_receipt.load(.acquire)) continue;
            if (self.handler_joins[i]) |handle| {
                const prev = slot.completion_owner.cmpxchgStrong(live, reaper_owned, .acq_rel, .acquire);
                if (prev == null) {
                    self.handler_joins[i] = null;
                    self.enqueueReaperOrFail(slot, handle, slot.stream_id);
                } else if (prev == @as(?u8, reaper_owned)) {
                    // Stranded: earlier cancel CAS'd without enqueue; join appeared later.
                    self.handler_joins[i] = null;
                    self.enqueueReaperOrFail(slot, handle, slot.stream_id);
                } else {
                    // Already reported — completion is or will be in completion_ch.
                    self.handler_joins[i] = null;
                }
            }
        }
        // Reaper posts completion only AFTER cancel() returns, and cancel waits for the
        // handler — which has already decremented live_handlers. Waiting on live==0 can
        // therefore finish before the completion is posted; draining once then leaving
        // discards the release. Wait until every slot is released via releaseSlot.
        while (true) {
            self.drainCompletions();
            var any_in_use = false;
            for (self.handlers) |s| {
                if (s.in_use) {
                    any_in_use = true;
                    break;
                }
            }
            if (!any_in_use) break;
            const sid = self.completion_ch.getOne(self.config.io) catch return error.Canceled;
            self.releaseSlot(sid);
        }
    }

    fn driveDecrypt(self: *Connection) !void {
        const tc = &(self.tls_conn orelse return);
        while (self.tls_recv_acc.items.len > 0) {
            const res = tls_edge.connectionDecrypt(
                tc,
                tls_edge.firstRecord(self.tls_recv_acc.items),
                self.plaintext_scratch,
                self.ciphertext_scratch,
            );
            if (res.ciphertext_len > 0) {
                const out = try self.config.gpa.dupe(u8, self.ciphertext_scratch[0..res.ciphertext_len]);
                // TLS close-notify is best-effort; sendAccountedWire releases `out` on failure.
                self.sendAccountedWire(out, true, 0, 0, 0, false) catch {};
            }
            if (res.consumed > 0) {
                const rest = self.tls_recv_acc.items.len - res.consumed;
                if (rest > 0) {
                    @memmove(self.tls_recv_acc.items[0..rest], self.tls_recv_acc.items[res.consumed..][0..rest]);
                }
                self.tls_recv_acc.shrinkRetainingCapacity(rest);
            }
            switch (res.status) {
                .need_input => return,
                .complete => {
                    if (res.plaintext_len > 0) {
                        try self.session.ingest(self.plaintext_scratch[0..res.plaintext_len]);
                        try self.processIntents();
                    }
                },
                .peer_closed => return,
                .tls_error => return error.TlsError,
            }
            if (res.consumed == 0 and res.plaintext_len == 0) return;
        }
    }

    fn flushSessionIntents(self: *Connection) !void {
        try self.processIntents();
    }

    fn maybeBeginGraceful(self: *Connection) !void {
        const sf = self.config.shutdown_flag orelse return;
        if (!sf.load(.acquire)) return;
        if (!self.shutting_down) {
            self.shutting_down = true;
            try self.session.applyCommand(.graceful_phase1);
            try self.processIntents();
            const now_ns = nowNs(self.config.io);
            self.grace_deadline = std.Io.Timestamp.fromNanoseconds(@as(i96, now_ns +% std.time.ns_per_s));
            return;
        }
        if (self.session.grace_phase == .phase1) {
            const now = nowNs(self.config.io);
            const due = self.session.gracePingAcked() or
                (self.grace_deadline != null and now >= @as(u64, @intCast(self.grace_deadline.?.nanoseconds)));
            if (due) {
                try self.session.applyCommand(.graceful_phase2);
                try self.processIntents();
                self.grace_deadline = std.Io.Timestamp.fromNanoseconds(
                    @as(i96, now +% self.config.limits.graceful_drain_timeout_ns),
                );
            }
        } else if (self.session.grace_phase == .phase2) {
            if (self.grace_deadline) |dl| {
                if (nowNs(self.config.io) >= @as(u64, @intCast(dl.nanoseconds))) {
                    try self.finishGraceful();
                }
            }
        }
    }

    fn finishGraceful(self: *Connection) !void {
        // Reset any remaining streams with CANCEL.
        var it = self.session.streams.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.state != .closed and e.value_ptr.state != .idle) {
                // A stream may become terminal while the graceful sweep advances.
                self.session.applyCommand(.{ .reset_stream = .{ .stream_id = e.key_ptr.*, .code = .cancel } }) catch {};
            }
        }
        try self.processIntents();
    }

    fn waitForStreamSpace(self: *Connection, stream_id: u31, terminal: *response.SlotTerminal) response.ResponseError!void {
        const cap = self.config.limits.outbound_bytes_per_stream;
        while (true) {
            if (terminal.getCause()) |c| return response.causeToError(c);
            if (self.writer_failed.load(.acquire)) return error.WriteFailed;
            const len = self.sched.pendingByteLen(stream_id);
            if (len < cap) return;
            // Prove sparse/dense capacity wait for live gates (HandlerSlot-indexed sem).
            test_waiting_for_space.store(@as(u32, stream_id), .release);
            // Explicit lock ownership: unlock for wait, always reacquire uncancelable
            // before any return so caller defer unlock stays balanced.
            const event = if (self.spaceIndex(stream_id)) |i| &self.space_events[i] else null;
            if (event) |e| e.reset();
            self.session_mu.unlock(self.config.io);
            const wait_res: anyerror!void = if (event) |e| e.wait(self.config.io) else error.Canceled;
            self.session_mu.lockUncancelable(self.config.io);
            if (terminal.getCause()) |c| return response.causeToError(c);
            if (self.writer_failed.load(.acquire)) return error.WriteFailed;
            wait_res catch {
                if (terminal.getCause()) |c| return response.causeToError(c);
                return error.Canceled;
            };
        }
    }

    /// Bounded streaming enqueue into FairScheduler boot-reserved slabs.
    /// Handler may block cancellably until actor drains capacity. No post-boot GPA.
    fn enqueuePending(
        self: *Connection,
        stream_id: u31,
        bytes: []const u8,
        end: bool,
        flush_ticket: u64,
        flush_slot: u32,
        terminal: *response.SlotTerminal,
    ) response.ResponseError!void {
        const cap = self.config.limits.outbound_bytes_per_stream;
        if (self.sched.pendingCount() >= self.config.limits.max_streams_per_connection) {
            if (!self.sched.contains(stream_id)) return error.OutOfMemory;
        }
        var off: usize = 0;
        while (off < bytes.len) {
            try self.waitForStreamSpace(stream_id, terminal);
            const room = cap - self.sched.pendingByteLen(stream_id);
            if (room == 0) continue;
            const take = @min(bytes.len - off, room);
            if (!self.tryReserveOutboundBytes(take, .pending)) return error.OutOfMemory;
            self.sched.enqueueDataBytes(stream_id, bytes[off..][0..take], false, 0, 0, cap) catch {
                self.applyOutboundRelease(take, .pending);
                return error.OutOfMemory;
            };
            off += take;
            self.sched_refilled.store(true, .release);
            self.actor_wake.set(self.config.io);
        }
        if (bytes.len == 0 or end or flush_ticket != 0) {
            self.sched.enqueueDataBytes(stream_id, &.{}, end, flush_ticket, flush_slot, cap) catch {
                if (flush_ticket != 0) return error.WriteFailed;
                return error.OutOfMemory;
            };
            self.sched_refilled.store(true, .release);
            self.actor_wake.set(self.config.io);
        }
    }

    /// Single FairScheduler drain: terminal → ordinary(+forced DATA) → DRR DATA.
    /// All emits go through queueWire via scheduler sink only.
    fn drainEmit(self: *Connection) !void {
        const SinkCtx = struct {
            fn sink(
                ctx: *anyopaque,
                payload: []u8,
                flush: bool,
                ticket: u64,
                ticket_slot: u32,
                control_n: usize,
                control_entry: bool,
            ) anyerror!void {
                const c: *Connection = @ptrCast(@alignCast(ctx));
                test_in_sched_sink = true;
                defer test_in_sched_sink = false;
                // Body bytes were reserved at enqueuePending; release only after the
                // sink accepted the framed lease. Wire chunk.outbound_release tracks
                // the framed size reserved in sendAccountedWire (acked separately).
                try c.queueWire(payload, flush, ticket, ticket_slot, control_n, control_entry);
                if (!control_entry and c.pending_data_outbound_release != 0) {
                    c.applyOutboundRelease(c.pending_data_outbound_release, .pending);
                    c.wakeStreamSpace(c.pending_data_wake_sid);
                    c.pending_data_outbound_release = 0;
                    c.pending_data_wake_sid = 0;
                }
            }
            fn streamWin(ctx: *anyopaque, stream_id: u31) i32 {
                const c: *Connection = @ptrCast(@alignCast(ctx));
                return c.session.streamSendAvailable(stream_id);
            }
            fn connWin(ctx: *anyopaque) i32 {
                const c: *Connection = @ptrCast(@alignCast(ctx));
                return c.session.connectionSendAvailable();
            }
            fn buildData(ctx: *anyopaque, stream_id: u31, bytes: []const u8, end: bool) anyerror![]u8 {
                const c: *Connection = @ptrCast(@alignCast(ctx));
                // Frame DATA without enqueueing into Session intents (no nested drain).
                const data_payload = c.session.makeDataFrame(stream_id, bytes, end) catch |err| {
                    if (err == error.FlowBlocked) return error.FlowBlocked;
                    return err;
                };
                // Defer pending-outbound release until sink succeeds (fail-closed otherwise).
                c.pending_data_outbound_release = bytes.len;
                c.pending_data_wake_sid = stream_id;
                return data_payload;
            }
        };
        self.sched.drain(
            self,
            SinkCtx.sink,
            self,
            SinkCtx.streamWin,
            SinkCtx.connWin,
            SinkCtx.buildData,
            self,
        ) catch |err| {
            if (self.pending_data_outbound_release != 0) {
                self.applyOutboundRelease(self.pending_data_outbound_release, .pending);
                self.pending_data_outbound_release = 0;
                self.pending_data_wake_sid = 0;
            }
            self.writer_failed.store(true, .release);
            self.handleWriterFailed();
            return err;
        };
        test_observed_sched_emits.store(self.sched.emits_total, .release);
    }

    fn processIntents(self: *Connection) anyerror!void {
        const n = self.session.drainIntentsInto(self.intent_batch);
        var consumed: usize = 0;
        errdefer while (consumed < n) : (consumed += 1) {
            self.session.releaseIntent(&self.intent_batch[consumed]);
        };
        while (consumed < n) {
            const it = &self.intent_batch[consumed];
            // Every processing branch consumes its current intent, including
            // failure paths. Advance first so errdefer owns only untouched tail.
            consumed += 1;
            switch (it.*) {
                .outbound_frame => |f| {
                    if (f.typ == .data) {
                        // No direct queueWire — framed DATA enters FairScheduler queue.
                        var t: u64 = 0;
                        var s: u32 = 0;
                        if (self.next_wire_ticket != 0) {
                            t = self.next_wire_ticket;
                            s = self.next_wire_slot;
                            self.next_wire_ticket = 0;
                            self.next_wire_slot = 0;
                        }
                        try self.sched.enqueueFramedData(f.payload, f.stream_id, f.flags.end_stream, t, s);
                    } else {
                        const class = fair_scheduler.FairScheduler.classifyControl(f.typ, f.flags);
                        var t: u64 = 0;
                        var s: u32 = 0;
                        if (self.next_wire_ticket != 0) {
                            t = self.next_wire_ticket;
                            s = self.next_wire_slot;
                            self.next_wire_slot = 0;
                            self.next_wire_ticket = 0;
                        }
                        self.sched.enqueueControl(f.payload, class, t, s) catch |err| switch (err) {
                            error.PoolFull, error.QueueFull => {
                                // Fail closed: release ownership already consumed by enqueueControl on error.
                                return error.OutOfMemory;
                            },
                        };
                    }
                },
                .dispatch_request => |d| try self.dispatch(d),
                .early_reject => {},
                .stream_reset => |r| {
                    const cause: response.TerminalCause = if (r.from_peer)
                        .{ .peer_reset = r.code }
                    else if (self.shutting_down)
                        .server_shutdown
                    else
                        .{ .peer_reset = r.code };
                    self.cancelHandler(r.stream_id, cause);
                },
                .connection_error => {
                    for (self.handlers) |*slot| {
                        if (!slot.in_use) continue;
                        slot.terminal.setCause(.connection_closed);
                    }
                    self.writer_failed.store(true, .release);
                },
                .connection_closed => {
                    for (self.handlers) |*slot| {
                        if (!slot.in_use) continue;
                        slot.terminal.setCause(.connection_closed);
                    }
                },
            }
        }
        try self.drainEmit();
    }

    fn sendFlushTicket(self: *Connection, ticket: u64, slot: u32) anyerror!void {
        self.write_ch.putOne(self.config.io, .{
            .bytes = &.{},
            .len = 0,
            .flush_barrier = true,
            .ticket = ticket,
            .ticket_slot = slot,
        }) catch return error.WriteFailed;
    }

    fn sendAccountedWire(
        self: *Connection,
        out: []u8,
        flush: bool,
        ticket: u64,
        ticket_slot: u32,
        control_n: usize,
        control_entry: bool,
    ) anyerror!void {
        if (!self.tryReserveOutboundBytes(out.len, .wire)) {
            self.config.gpa.free(out);
            if (control_entry) self.applyControlRelease(control_n, true);
            return error.OutOfMemory;
        }
        self.write_ch.putOne(self.config.io, .{
            .bytes = out,
            .len = out.len,
            .flush_barrier = flush,
            .ticket = ticket,
            .ticket_slot = ticket_slot,
            .outbound_release = out.len,
            .control_release = control_n,
            .control_entry = control_entry,
        }) catch {
            self.applyOutboundRelease(out.len, .wire);
            if (control_entry) self.applyControlRelease(control_n, true);
            self.config.gpa.free(out);
            return error.WriteFailed;
        };
    }

    fn queueWire(
        self: *Connection,
        bytes: []u8,
        flush: bool,
        ticket: u64,
        ticket_slot: u32,
        control_n: usize,
        control_entry: bool,
    ) anyerror!void {
        if (!test_in_sched_sink) {
            _ = test_queue_wire_bypass.fetchAdd(1, .acq_rel);
        }
        const sends = test_wire_sends.fetchAdd(1, .acq_rel);
        const fail_after = test_force_wire_fail_after.load(.acquire);
        if (fail_after != 0 and sends + 1 >= fail_after) {
            self.config.gpa.free(bytes);
            if (control_entry) self.applyControlRelease(control_n, true);
            return error.WriteFailed;
        }
        if (self.tls_conn) |*tc| {
            if (!self.tryReserveOutboundBytes(bytes.len, .wire)) {
                self.config.gpa.free(bytes);
                if (control_entry) self.applyControlRelease(control_n, true);
                return error.OutOfMemory;
            }
            var reserved_plain = bytes.len;
            errdefer self.applyOutboundRelease(reserved_plain, .wire);
            var off: usize = 0;
            while (off < bytes.len) {
                const res = tls_edge.connectionEncrypt(tc, bytes[off..], self.ciphertext_scratch);
                if (res.ciphertext_len > 0) {
                    const out = self.config.gpa.dupe(u8, self.ciphertext_scratch[0..res.ciphertext_len]) catch {
                        self.config.gpa.free(bytes);
                        reserved_plain = 0;
                        if (control_entry) self.applyControlRelease(control_n, true);
                        return error.OutOfMemory;
                    };
                    const next_off = off + res.consumed;
                    const is_last = next_off >= bytes.len;
                    const t: u64 = if (is_last) ticket else 0;
                    const s: u32 = if (is_last) ticket_slot else 0;
                    const cn: usize = if (is_last) control_n else 0;
                    const ce = is_last and control_entry;
                    self.sendAccountedWire(out, flush and is_last, t, s, cn, ce) catch {
                        self.config.gpa.free(bytes);
                        reserved_plain = 0;
                        return error.WriteFailed;
                    };
                }
                if (res.consumed == 0) break;
                off += res.consumed;
            }
            self.applyOutboundRelease(reserved_plain, .wire);
            reserved_plain = 0;
            self.config.gpa.free(bytes);
        } else {
            try self.sendAccountedWire(bytes, flush, ticket, ticket_slot, control_n, control_entry);
        }
    }

    fn dispatch(self: *Connection, d: session_mod.DispatchRequest) anyerror!void {
        const gpa = self.config.gpa;
        defer {
            for (d.headers) |h| {
                gpa.free(@constCast(h.name));
                gpa.free(@constCast(h.value));
            }
            gpa.free(d.headers);
            for (d.trailers) |h| {
                gpa.free(@constCast(h.name));
                gpa.free(@constCast(h.value));
            }
            if (d.trailers.len != 0) gpa.free(d.trailers);
            gpa.free(d.body);
        }
        const job = try gpa.create(HandlerJob);
        errdefer gpa.destroy(job);
        job.* = .{
            .conn = self,
            .arena = std.heap.ArenaAllocator.init(gpa),
            .stream_id = d.stream_id,
            .req = undefined,
            .resp = undefined,
            .matched = .not_found,
        };
        errdefer job.arena.deinit();
        const a = job.arena.allocator();

        const method_str = try a.dupe(u8, d.method);
        const scheme = try a.dupe(u8, d.scheme);
        const authority = try a.dupe(u8, d.authority);
        const path = try a.dupe(u8, d.path);
        const query = try a.dupe(u8, d.query);
        const body = try a.dupe(u8, d.body);

        var headers = try a.alloc(request.Header, d.headers.len);
        for (d.headers, 0..) |h, i| {
            headers[i] = .{ .name = try a.dupe(u8, h.name), .value = try a.dupe(u8, h.value) };
        }

        var trailers = try a.alloc(request.Header, d.trailers.len);
        for (d.trailers, 0..) |h, i| {
            trailers[i] = .{ .name = try a.dupe(u8, h.name), .value = try a.dupe(u8, h.value) };
        }

        job.req = .{
            .method = .parse(method_str),
            .scheme = scheme,
            .authority = authority,
            .path = path,
            .query = query,
            .headers = headers,
            .body = body,
            .trailers = trailers,
            .arena = a,
        };
        job.resp = .{
            .stream_id = d.stream_id,
            .generation = 0,
            .terminal = undefined, // set after slot alloc
            .ctx = undefined, // set to &job.hctx after slot alloc
            .sendFn = sendCb,
            .startFn = startCb,
            .writeFn = writeCb,
            .flushFn = flushCb,
            .abortFn = abortCb,
        };
        // HandlerCtx: Response.ctx points at job-local ctx with stable terminal pointer.
        job.hctx = .{ .conn = self, .terminal = undefined, .slot = undefined, .stream_id = d.stream_id };
        job.matched = self.config.router.match(job.req.method, job.req.path);
        switch (job.matched) {
            .found => |f| job.req.path_remainder = f.path_remainder,
            else => {},
        }

        // Reserve reaper capacity before admitting the handler.
        if (self.config.accounting) |acct| {
            if (!acct.tryReserveReaper()) {
                job.arena.deinit();
                gpa.destroy(job);
                // A terminal connection needs no additional refusal frame.
                self.session.applyCommand(.{ .reset_stream = .{ .stream_id = d.stream_id, .code = .refused_stream } }) catch {};
                try self.processIntents();
                return;
            }
        }

        const slot = self.allocSlot(d.stream_id) orelse {
            if (self.config.accounting) |acct| acct.releaseReaper();
            job.arena.deinit();
            gpa.destroy(job);
            // A terminal connection needs no additional refusal frame.
            self.session.applyCommand(.{ .reset_stream = .{ .stream_id = d.stream_id, .code = .refused_stream } }) catch {};
            try self.processIntents();
            return;
        };
        slot.reaper_reserved = self.config.accounting != null;
        job.resp.terminal = &slot.terminal;
        job.resp.generation = slot.terminal.currentGeneration();
        job.slot = slot;
        job.hctx.terminal = &slot.terminal;
        job.hctx.slot = slot;
        job.resp.ctx = &job.hctx;

        _ = self.live_handlers.fetchAdd(1, .acq_rel);
        _ = test_observed_live_handlers.fetchAdd(1, .acq_rel);
        const handle = if (test_force_spawn_fail)
            error.OutOfMemory
        else
            self.config.io.concurrent(runHandlerJob, .{job});
        const h = handle catch {
            runHandlerJob(job);
            self.drainCompletions();
            return;
        };
        if (self.slotIndex(d.stream_id)) |i| {
            self.handler_joins[i] = h;
        }
    }

    const HandlerCtx = struct {
        conn: *Connection,
        terminal: *response.SlotTerminal,
        slot: *HandlerSlot,
        stream_id: u31,
    };

    const HandlerJob = struct {
        conn: *Connection,
        arena: std.heap.ArenaAllocator,
        stream_id: u31,
        req: request.Request,
        resp: response.Response,
        matched: router_mod.Match,
        slot: *HandlerSlot = undefined,
        hctx: HandlerCtx = undefined,
    };

    fn runHandlerJob(job: *HandlerJob) void {
        const self = job.conn;
        defer {
            const prev = job.slot.completion_owner.cmpxchgStrong(live, reported, .acq_rel, .acquire);
            if (prev == null) {
                self.completion_ch.putOneUncancelable(self.config.io, job.stream_id) catch {
                    // Connection teardown has already assumed completion ownership.
                };
                self.actor_wake.set(self.config.io);
            }
            _ = self.live_handlers.fetchSub(1, .acq_rel);
            _ = test_observed_live_handlers.fetchSub(1, .acq_rel);
            job.arena.deinit();
            self.config.gpa.destroy(job);
        }
        if (job.slot.terminal.cancel_flag.load(.acquire)) return;
        if (job.slot.terminal.getCause() != null) return;
        switch (job.matched) {
            .found => |f| {
                f.handler.runFn(f.handler.ptr, &job.req, &job.resp) catch {
                    if (job.slot.terminal.getCause() != null) return;
                    if (!job.resp.committed) {
                        // The original handler error remains terminal if the fallback cannot write.
                        job.resp.send(500, &.{}, "internal error") catch {};
                    } else if (!job.resp.finished) {
                        self.withSession(struct {
                            fn go(c: *Connection, sid: u31) void {
                                // A concurrently terminal stream needs no second reset.
                                c.session.applyCommand(.{ .reset_stream = .{ .stream_id = sid, .code = .internal_error } }) catch {};
                                // Connection teardown is authoritative when reset output cannot be queued.
                                c.processIntents() catch {};
                            }
                        }.go, job.stream_id);
                    }
                };
                if (job.slot.terminal.getCause() == null and !job.resp.committed) {
                    // Terminal transport state supersedes the synthetic fallback.
                    job.resp.send(500, &.{}, "no response") catch {};
                }
            },
            .not_found => {
                if (job.slot.terminal.getCause() == null) {
                    // Terminal transport state supersedes the synthetic response.
                    job.resp.send(404, &.{}, "not found") catch {};
                }
            },
            .method_not_allowed => {
                if (job.slot.terminal.getCause() == null) {
                    const allow = [_]request.Header{.{ .name = "allow", .value = "GET, POST" }};
                    // Terminal transport state supersedes the synthetic response.
                    job.resp.send(405, &allow, "method not allowed") catch {};
                }
            },
        }
    }

    fn withSession(self: *Connection, comptime f: anytype, arg: anytype) void {
        self.session_mu.lock(self.config.io) catch return;
        defer self.session_mu.unlock(self.config.io);
        f(self, arg);
    }

    fn lockSession(self: *Connection) response.ResponseError!void {
        self.session_mu.lock(self.config.io) catch return error.Canceled;
    }

    fn sendCb(ctx: *anyopaque, stream_id: u31, status: u16, headers: []const request.Header, body: []const u8) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        if (hctx.terminal.getCause()) |c| return response.causeToError(c);
        if (hctx.terminal.cancel_flag.load(.acquire)) return error.Canceled;
        const ticket_pair = self.reserveTicket() catch |err| return err;
        const ticket = ticket_pair[0];
        const slot_i = ticket_pair[1];
        var armed = true;
        defer if (armed) self.tickets.releaseReserved(slot_i);
        {
            try self.lockSession();
            defer self.session_mu.unlock(self.config.io);
            var hlist: std.ArrayList(hpack.HeaderField) = .empty;
            defer hlist.deinit(self.config.gpa);
            for (headers) |h| {
                hlist.append(self.config.gpa, .{ .name = h.name, .value = h.value }) catch return error.OutOfMemory;
            }
            self.session.applyCommand(.{ .respond_headers = .{
                .stream_id = stream_id,
                .status = status,
                .headers = hlist.items,
                .end_stream = body.len == 0,
            } }) catch return error.WriteFailed;
            if (body.len > 0) {
                // Materialize the HEADERS intent onto the wire queue BEFORE any
                // DATA can drain. enqueuePending unlocks the session while it
                // waits for stream space, and the actor emits DRR DATA the
                // moment it wakes — so a body crossing outbound_bytes_per_stream
                // used to reach the peer as DATA with its HEADERS never sent,
                // which a browser reports as a protocol error and curl waits
                // out in silence (t-482, the /ui/tasks page).
                self.processIntents() catch return error.WriteFailed;
                self.enqueuePending(stream_id, body, true, ticket, slot_i, hctx.terminal) catch |err| return err;
                self.processIntents() catch return error.WriteFailed;
            } else {
                self.next_wire_ticket = ticket;
                self.next_wire_slot = slot_i;
                self.processIntents() catch return error.WriteFailed;
            }
        }
        armed = false;
        hctx.slot.awaiting_receipt.store(true, .release);
        defer hctx.slot.awaiting_receipt.store(false, .release);
        try self.waitTicket(slot_i, hctx.terminal);
    }

    fn startCb(ctx: *anyopaque, stream_id: u31, status: u16, headers: []const request.Header, sse: bool) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        if (hctx.terminal.getCause()) |c| return response.causeToError(c);
        if (hctx.terminal.cancel_flag.load(.acquire)) return error.Canceled;
        const ticket_pair = self.reserveTicket() catch |err| return err;
        const ticket = ticket_pair[0];
        const slot_i = ticket_pair[1];
        {
            errdefer self.tickets.releaseReserved(slot_i);
            try self.lockSession();
            defer self.session_mu.unlock(self.config.io);
            var hlist: std.ArrayList(hpack.HeaderField) = .empty;
            defer hlist.deinit(self.config.gpa);
            if (sse) {
                hlist.append(self.config.gpa, .{ .name = "content-type", .value = "text/event-stream" }) catch return error.OutOfMemory;
                hlist.append(self.config.gpa, .{ .name = "cache-control", .value = "no-cache" }) catch return error.OutOfMemory;
                hlist.append(self.config.gpa, .{ .name = "x-accel-buffering", .value = "no" }) catch return error.OutOfMemory;
            }
            for (headers) |h| {
                hlist.append(self.config.gpa, .{ .name = h.name, .value = h.value }) catch return error.OutOfMemory;
            }
            self.session.applyCommand(.{ .respond_headers = .{
                .stream_id = stream_id,
                .status = status,
                .headers = hlist.items,
                .end_stream = false,
            } }) catch return error.WriteFailed;
            self.next_wire_ticket = ticket;
            self.next_wire_slot = slot_i;
            self.processIntents() catch return error.WriteFailed;
        }
        hctx.slot.awaiting_receipt.store(true, .release);
        defer hctx.slot.awaiting_receipt.store(false, .release);
        try self.waitTicket(slot_i, hctx.terminal);
    }

    fn writeCb(ctx: *anyopaque, stream_id: u31, bytes: []const u8, end: bool, flush: bool) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        if (hctx.terminal.getCause()) |c| return response.causeToError(c);
        if (hctx.terminal.cancel_flag.load(.acquire)) return error.Canceled;
        if (!flush and !end and bytes.len == 0) return;
        const need_wait = flush or end;
        var ticket: u64 = 0;
        var slot_i: u32 = 0;
        if (need_wait) {
            const ticket_pair = self.reserveTicket() catch |err| return err;
            ticket = ticket_pair[0];
            slot_i = ticket_pair[1];
        }
        {
            errdefer if (need_wait) self.tickets.releaseReserved(slot_i);
            try self.lockSession();
            defer self.session_mu.unlock(self.config.io);
            self.enqueuePending(stream_id, bytes, end, if (need_wait) ticket else 0, if (need_wait) slot_i else 0, hctx.terminal) catch |err| return err;
            self.processIntents() catch return error.WriteFailed;
        }
        if (need_wait) {
            hctx.slot.awaiting_receipt.store(true, .release);
            defer hctx.slot.awaiting_receipt.store(false, .release);
            try self.waitTicket(slot_i, hctx.terminal);
        }
    }

    fn flushCb(ctx: *anyopaque, stream_id: u31) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        if (hctx.terminal.getCause()) |c| return response.causeToError(c);
        const ticket_pair = self.reserveTicket() catch |err| return err;
        const ticket = ticket_pair[0];
        const slot_i = ticket_pair[1];
        {
            errdefer self.tickets.releaseReserved(slot_i);
            try self.lockSession();
            defer self.session_mu.unlock(self.config.io);
            if (self.sched.findPending(stream_id)) |pw| {
                if (pw.len > 0 or pw.end_stream) {
                    pw.flush_ticket = ticket;
                    pw.flush_slot = slot_i;
                    pw.flush_remain = pw.len;
                } else {
                    self.sendFlushTicket(ticket, slot_i) catch return error.WriteFailed;
                }
            } else {
                self.sendFlushTicket(ticket, slot_i) catch return error.WriteFailed;
            }
            self.processIntents() catch return error.WriteFailed;
        }
        hctx.slot.awaiting_receipt.store(true, .release);
        defer hctx.slot.awaiting_receipt.store(false, .release);
        try self.waitTicket(slot_i, hctx.terminal);
    }

    fn abortCb(ctx: *anyopaque, stream_id: u31) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        try self.lockSession();
        defer self.session_mu.unlock(self.config.io);
        self.session.applyCommand(.{ .reset_stream = .{ .stream_id = stream_id, .code = .cancel } }) catch return error.WriteFailed;
        self.processIntents() catch return error.WriteFailed;
    }
};
