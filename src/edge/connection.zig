//! Connection actor: owns Session, TLS (optional), HPACK, fair write scheduling.
const std = @import("std");
const zio = @import("zio");
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

/// Test-only: when true, handler spawn fails closed into synchronous runHandlerJob.
pub var test_force_spawn_fail: bool = false;
/// Test-only: when true, actor skips draining completion_ch (sticky until cleared).
pub var test_hold_completion_drain: std.atomic.Value(bool) = .init(false);
/// Test-only: delay write pump by N ms.
pub var test_write_delay_ms: u64 = 0;
/// Test-only: fail write pump after N successful writes (0 = never).
pub var test_write_fail_after: u64 = 0;
/// Test-only: last observed live_handlers (updated by actor loop).
pub var test_observed_live_handlers: std.atomic.Value(usize) = .init(0);
/// Test-only: slots currently in_use (updated on alloc/release).
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

pub const Mode = enum { h2c, tls_h2 };

pub const ConnConfig = struct {
    mode: Mode,
    limits: limits_mod.Limits,
    router: router_mod.Router,
    tls_auth: ?*tls.config.CertKeyPair = null,
    gpa: std.mem.Allocator,
    shutdown_flag: ?*std.atomic.Value(bool) = null,
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
};

const live: u8 = 0;
const reaper_owned: u8 = 1;
const reported: u8 = 2;

pub const ReaperJob = struct {
    handle: zio.JoinHandle(void),
    owner: *std.atomic.Value(u8),
    completion: *zio.Channel(u31),
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
    jobs: zio.Channel(ReaperJob),
    job_buf: []ReaperJob,
    gpa: std.mem.Allocator,
    shut: std.atomic.Value(bool) = .init(false),

    pub fn init(gpa: std.mem.Allocator, capacity: usize) !ReaperPool {
        const buf = try gpa.alloc(ReaperJob, capacity);
        return .{ .jobs = .init(buf), .job_buf = buf, .gpa = gpa };
    }

    pub fn deinit(self: *ReaperPool) void {
        // Workers must already be joined; only free storage.
        self.gpa.free(self.job_buf);
        self.* = undefined;
    }

    pub fn worker(self: *ReaperPool) void {
        while (!self.shut.load(.acquire)) {
            var job = self.jobs.receive() catch break;
            job.handle.cancel();
            const prev = job.owner.swap(reported, .acq_rel);
            if (prev == reaper_owned) {
                job.completion.send(job.stream_id) catch {};
            }
        }
        // Drain any remaining jobs after close(.graceful).
        while (self.jobs.tryReceive()) |job_val| {
            var job = job_val;
            job.handle.cancel();
            const prev = job.owner.swap(reported, .acq_rel);
            if (prev == reaper_owned) {
                job.completion.send(job.stream_id) catch {};
            }
        } else |_| {}
    }
};

pub fn serveAccepted(stream: zio.net.Stream, config: ConnConfig) void {
    // TCP_NODELAY before first read — failure closes socket.
    stream.socket.setNoDelay(true) catch {
        if (config.handshake_held) {
            if (config.accounting) |a| a.releaseHandshake();
        }
        stream.close();
        return;
    };
    var conn = Connection.init(stream, config) catch {
        if (config.handshake_held) {
            if (config.accounting) |a| a.releaseHandshake();
        }
        stream.close();
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
        conn.read_free_ch.send(i) catch {};
    }
    conn.run() catch {};
}

const Connection = struct {
    stream: zio.net.Stream,
    config: ConnConfig,
    session: session_mod.Session,
    read_ch_buf: []wire_pump.WireChunk,
    write_ch_buf: []wire_pump.WireChunk,
    read_ch: zio.Channel(wire_pump.WireChunk) = undefined,
    write_ch: zio.Channel(wire_pump.WireChunk) = undefined,
    tls_server: ?tls.nonblock.Server = null,
    tls_conn: ?tls.nonblock.Connection = null,
    tls_prng: std.Random.DefaultPrng = undefined,
    tls_recv_acc: std.ArrayList(u8) = .empty,
    plaintext_scratch: []u8 = &.{},
    ciphertext_scratch: []u8 = &.{},
    handlers: []HandlerSlot,
    shutting_down: bool = false,
    grace_deadline: ?zio.Timestamp = null,
    /// Production fair scheduler — sole emit path for controls + DATA.
    sched: fair_scheduler.FairScheduler = undefined,
    session_mu: zio.Mutex = .init,
    live_handlers: std.atomic.Value(usize) = .init(0),
    reaper: ?*ReaperPool = null,
    completion_ch_buf: []u31 = &.{},
    completion_ch: zio.Channel(u31) = undefined,
    write_ack_buf: []wire_pump.WriteCompletion = &.{},
    write_ack_ch: zio.Channel(wire_pump.WriteCompletion) = undefined,
    ticket_slots: []ticket_table.TicketWait = &.{},
    tickets: ticket_table.TicketTable = undefined,
    /// Indexed parallel to handlers; only valid while slot.in_use.
    handler_joins: []?zio.JoinHandle(void) = &.{},
    handshake_deadline: ?zio.Timestamp = null,
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
    /// Bounded writer-failure event (AckDrainer → actor). Capacity 1.
    writer_fail_buf: [1]u8 = .{0},
    writer_fail_ch: zio.Channel(u8) = undefined,
    /// Capacity waiters: one zio.Semaphore per HandlerSlot (slotIndex(stream_id); sparse IDs safe).
    space_sems: []zio.Semaphore = &.{},
    /// Actor-owned intent batch — filled by drainIntentsInto (no nested Session drain).
    intent_batch: []session_mod.Intent = &.{},
    rates: rates_mod.RateLimiter = .{},
    /// Fixed inbound wire chunk pool.
    read_chunk_storage: []u8 = &.{},
    read_free_buf: []u32 = &.{},
    read_free_ch: zio.Channel(u32) = undefined,
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

    fn init(stream: zio.net.Stream, config: ConnConfig) !Connection {
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
        const handler_joins = try gpa.alloc(?zio.JoinHandle(void), config.limits.max_streams_per_connection);
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
        const space_sems = try gpa.alloc(zio.Semaphore, config.limits.max_streams_per_connection);
        errdefer gpa.free(space_sems);
        @memset(space_sems, .{ .permits = 0 });
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
            .space_sems = space_sems,
            .intent_batch = intent_batch,
            .tls_recv_acc = tls_recv_acc,
        };
        @memset(self.handlers, .{});
        @memset(self.handler_joins, null);
        self.tickets = ticket_table.TicketTable.init(self.ticket_slots);
        self.read_ch = .init(self.read_ch_buf);
        self.write_ch = .init(self.write_ch_buf);
        self.completion_ch = .init(self.completion_ch_buf);
        self.write_ack_ch = .init(self.write_ack_buf);
        self.read_free_ch = .init(self.read_free_buf);
        self.writer_fail_ch = .init(&self.writer_fail_buf);
        return self;
    }

    fn deinit(self: *Connection) void {
        // Must not free while handlers still live.
        std.debug.assert(self.live_handlers.load(.acquire) == 0);
        if (self.handshake_held) {
            if (self.config.accounting) |a| a.releaseHandshake();
            self.handshake_held = false;
        }
        // Drain channels before freeing their backing buffers.
        while (self.write_ch.tryReceive()) |chunk| {
            if (chunk.bytes.len != 0) self.config.gpa.free(chunk.bytes);
            // Release amounts without AckDrainer (teardown path).
            self.applyOutboundRelease(chunk.outbound_release, .wire);
            if (chunk.control_entry) self.applyControlRelease(chunk.control_release, true);
        } else |_| {}
        while (self.read_ch.tryReceive()) |chunk| {
            if (chunk.pool_index) |idx| {
                self.read_free_ch.send(idx) catch {};
            } else if (chunk.bytes.len != 0) {
                self.config.gpa.free(chunk.bytes);
            }
        } else |_| {}
        // Late reaper posts can arrive after shutdownHandlers; never discard without releaseSlot.
        while (self.completion_ch.tryReceive()) |sid| {
            self.releaseSlot(sid);
        } else |_| {}
        for (self.handlers) |s| std.debug.assert(!s.in_use);
        while (self.write_ack_ch.tryReceive()) |ack| {
            self.applyOutboundRelease(ack.outbound_release, .wire);
            if (ack.control_entry) self.applyControlRelease(ack.control_release, true);
        } else |_| {}

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
        if (self.space_sems.len != 0) self.config.gpa.free(self.space_sems);
        if (self.intent_batch.len != 0) self.config.gpa.free(self.intent_batch);
        const held = self.outbound_held.load(.acquire);
        const pending_held = self.pending_outbound_held.load(.acquire);
        const wire_held = self.wire_outbound_held.load(.acquire);
        std.debug.assert(held == pending_held + wire_held);
        std.debug.assert(held == 0);
        if (!self.socket_closed.load(.acquire)) {
            self.stream.close();
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
            const ack = self.write_ack_ch.receive() catch break;
            self.applyOutboundRelease(ack.outbound_release, .wire);
            if (ack.control_entry) self.applyControlRelease(ack.control_release, true);
            if (ack.fail_all) {
                self.writer_failed.store(true, .release);
                self.tickets.failAll();
                // Do NOT touch handlers[] — actor applies connection_closed terminals.
                self.wakeAllSpace();
                self.writer_fail_ch.trySend(1) catch {};
            } else if (ack.ticket != 0) {
                self.tickets.complete(ack.ticket_slot, ack.ticket, ack.ok);
            }
            if (ack.outbound_release != 0) self.wakeAllSpace();
            if (ack.shutdown) break;
        }
    }

    /// Capacity waiter index = admitted HandlerSlot index (sparse stream IDs safe).
    fn spaceIndex(self: *Connection, stream_id: u31) ?usize {
        return self.slotIndex(stream_id);
    }

    fn wakeStreamSpace(self: *Connection, stream_id: u31) void {
        if (self.spaceIndex(stream_id)) |i| {
            if (i < self.space_sems.len) self.space_sems[i].post();
        }
    }

    fn wakeAllSpace(self: *Connection) void {
        for (self.space_sems) |*s| s.post();
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
            self.session.applyCommand(.{ .reset_stream = .{ .stream_id = self.sid_scratch[si], .code = .cancel } }) catch {};
        }
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

        // Release any wire chunks still queued (AckDrainer may not see them if pump died).
        while (self.write_ch.tryReceive()) |chunk| {
            if (chunk.bytes.len != 0) self.config.gpa.free(chunk.bytes);
            self.applyOutboundRelease(chunk.outbound_release, .wire);
            if (chunk.control_entry) self.applyControlRelease(chunk.control_release, true);
        } else |_| {}

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
        const now = zio.Timestamp.now(.monotonic).toNanoseconds();
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

    fn run(self: *Connection) !void {
        const gpa = self.config.gpa;

        var read_pump: wire_pump.ReadPump = .{
            .stream = self.stream,
            .to_actor = &self.read_ch,
            .chunk_storage = self.read_chunk_storage,
            .n_chunks = self.read_pool_n,
            .free_indices = &self.read_free_ch,
        };
        var write_pump: wire_pump.WritePump = .{
            .stream = self.stream,
            .from_actor = &self.write_ch,
            .completions = &self.write_ack_ch,
            .gpa = gpa,
            .test_delay_ms = test_write_delay_ms,
            .test_fail_after = test_write_fail_after,
        };
        var read_handle = try zio.spawn(wire_pump.ReadPump.run, .{&read_pump});
        errdefer read_handle.cancel();
        var write_handle = try zio.spawn(wire_pump.WritePump.run, .{&write_pump});
        errdefer write_handle.cancel();
        var ack_handle = try zio.spawn(ackDrainerEntry, .{self});
        errdefer ack_handle.cancel();

        defer {
            read_pump.stop();
            write_pump.stop();
            self.write_ch.send(.{ .bytes = &.{}, .len = 0 }) catch {};
            write_handle.cancel();
            read_handle.cancel();
            write_handle.join() catch {};
            read_handle.join() catch {};
            // AckDrainer exits on shutdown completion from write pump.
            ack_handle.join();
            if (!self.socket_closed.swap(true, .acq_rel)) {
                self.stream.close();
            }
        }

        if (self.config.mode == .tls_h2) {
            const auth = self.config.tls_auth orelse return error.InvalidConfig;
            var seed: [8]u8 = undefined;
            zio.random(&seed);
            self.tls_prng = std.Random.DefaultPrng.init(@as(u64, @bitCast(seed)));
            self.tls_server = tls.nonblock.Server.init(.{
                .auth = auth,
                .alpn_protocols = &tls_edge.alpn_list,
                .rng = self.tls_prng.random(),
                .now = .zero,
            });
            self.handshake_deadline = zio.Timestamp.fromNanoseconds(
                zio.Timestamp.now(.monotonic).toNanoseconds() +% 5 * std.time.ns_per_s,
            );
            try self.tlsHandshakeViaPumps();
            if (self.handshake_held) {
                if (self.config.accounting) |a| a.releaseHandshake();
                self.handshake_held = false;
            }
        }

        try self.flushSessionIntents();

        if (self.config.mode == .h2c) {
            self.handshake_deadline = zio.Timestamp.fromNanoseconds(
                zio.Timestamp.now(.monotonic).toNanoseconds() +% self.config.limits.preface_timeout_ns,
            );
            try self.waitH2cPreface();
        }

        while (true) {
            test_observed_live_handlers.store(self.live_handlers.load(.acquire), .release);
            var slots_used: usize = 0;
            for (self.handlers) |s| {
                if (s.in_use) slots_used += 1;
            }
            test_observed_slots_in_use.store(slots_used, .release);

            if (!test_hold_completion_drain.load(.acquire)) {
                while (self.completion_ch.tryReceive()) |sid| {
                    self.releaseSlot(sid);
                } else |_| {}
            }

            {
                self.session_mu.lockUncancelable();
                defer self.session_mu.unlock();
                while (self.writer_fail_ch.tryReceive()) |_| {
                    self.handleWriterFailed();
                } else |_| {}
                if (self.writer_failed.load(.acquire)) self.handleWriterFailed();
                if (self.session.terminal != .none) break;
                const now = zio.Timestamp.now(.monotonic).toNanoseconds();
                self.session.edge_now_ns = now;
                self.session.checkIdleDeadlines(now) catch {};
                self.maybeBeginGraceful() catch {};
                self.checkSlowConsumers() catch {};
                self.drainEmit() catch {
                    // Fail-closed: Session may already be debited — terminate connection.
                    self.writer_failed.store(true, .release);
                    self.handleWriterFailed();
                };
            }

            const chunk = self.read_ch.tryReceive() catch |err| switch (err) {
                error.ChannelEmpty => {
                    if (self.session.terminal != .none) break;
                    if (self.shutting_down and self.session.grace_phase == .phase2 and
                        self.sched.pendingCount() == 0 and self.live_handlers.load(.acquire) == 0)
                    {
                        self.session_mu.lockUncancelable();
                        defer self.session_mu.unlock();
                        self.finishGraceful() catch {};
                        break;
                    }
                    zio.sleep(.fromMilliseconds(5)) catch break;
                    continue;
                },
                else => break,
            };
            if (chunk.len == 0 and chunk.bytes.len == 0) break;
            defer {
                if (chunk.pool_index) |idx| {
                    self.read_free_ch.send(idx) catch {};
                } else if (chunk.bytes.len != 0) {
                    gpa.free(chunk.bytes);
                }
            }

            self.session_mu.lockUncancelable();
            defer self.session_mu.unlock();
            self.session.edge_now_ns = zio.Timestamp.now(.monotonic).toNanoseconds();
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

        try self.shutdownHandlers();
        test_observed_live_handlers.store(0, .release);
        test_observed_slots_in_use.store(0, .release);

        if (self.tls_conn) |*tc| {
            const res = tls_edge.connectionClose(tc, self.ciphertext_scratch);
            if (res.ciphertext_len > 0) {
                const out = try gpa.dupe(u8, self.ciphertext_scratch[0..res.ciphertext_len]);
                self.sendAccountedWire(out, true, 0, 0, 0, false) catch {};
            }
        }
    }

    fn ackDrainerEntry(self: *Connection) void {
        self.ackDrainer();
    }

    fn receiveUntilDeadline(self: *Connection) !wire_pump.WireChunk {
        const dl = self.handshake_deadline orelse {
            return self.read_ch.receive() catch return error.ConnectionClosed;
        };
        const now = zio.Timestamp.now(.monotonic).toNanoseconds();
        const dl_ns = dl.toNanoseconds();
        if (now >= dl_ns) return error.TlsHandshakeTimeout;
        var ac = zio.AutoCancel.init;
        defer ac.clear();
        ac.set(zio.Timeout.fromNanoseconds(dl_ns -% now));
        const chunk = self.read_ch.receive() catch |err| {
            if (err == error.Canceled and ac.check(error.Canceled)) return error.TlsHandshakeTimeout;
            return error.ConnectionClosed;
        };
        return chunk;
    }

    fn recycleReadChunk(self: *Connection, chunk: wire_pump.WireChunk) void {
        if (chunk.pool_index) |idx| {
            self.read_free_ch.send(idx) catch {};
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
            self.session_mu.lockUncancelable();
            {
                defer self.session_mu.unlock();
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
                if (zio.Timestamp.now(.monotonic).toNanoseconds() >= dl.toNanoseconds()) {
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
            try self.driveDecrypt();
        }
    }

    fn allocSlot(self: *Connection, stream_id: u31) ?*HandlerSlot {
        for (self.handlers, 0..) |*slot, i| {
            if (!slot.in_use) {
                slot.in_use = true;
                slot.stream_id = stream_id;
                slot.terminal.clear();
                _ = slot.terminal.generation.fetchAdd(1, .acq_rel);
                slot.completion_owner.store(live, .release);
                slot.reaper_reserved = false;
                self.handler_joins[i] = null;
                if (i < self.space_sems.len) self.space_sems[i] = .{ .permits = 0 };
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
            if (slot.reaper_reserved) {
                if (self.config.accounting) |a| a.releaseReaper();
                slot.reaper_reserved = false;
            }
            slot.in_use = false;
            slot.terminal.clear();
            slot.completion_owner.store(reported, .release);
            self.handler_joins[i] = null;
            if (i < self.space_sems.len) self.space_sems[i] = .{ .permits = 0 };
        }
    }

    fn enqueueReaperOrFail(self: *Connection, slot: *HandlerSlot, handle: zio.JoinHandle(void), stream_id: u31) void {
        if (self.reaper) |pool| {
            pool.jobs.trySend(.{
                .handle = handle,
                .owner = &slot.completion_owner,
                .completion = &self.completion_ch,
                .stream_id = stream_id,
            }) catch {
                // Invariant: reserved capacity must make this impossible.
                std.debug.assert(false);
                slot.completion_owner.store(reported, .release);
                self.session.applyCommand(.{ .goaway = .{ .code = .internal_error, .last_stream_id = self.session.last_processed_stream } }) catch {};
                self.processIntents() catch {};
                self.releaseSlot(stream_id);
            };
        } else {
            var h = handle;
            h.cancel();
            slot.completion_owner.store(reported, .release);
            self.releaseSlot(stream_id);
        }
    }

    fn cancelHandler(self: *Connection, stream_id: u31, cause: response.TerminalCause) void {
        const i = self.slotIndex(stream_id) orelse return;
        const slot = &self.handlers[i];
        slot.terminal.setCause(cause);
        self.wakeHandlerWaiters(stream_id);
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
            while (self.completion_ch.tryReceive()) |sid| {
                self.releaseSlot(sid);
            } else |_| {}
            var any_in_use = false;
            for (self.handlers) |s| {
                if (s.in_use) {
                    any_in_use = true;
                    break;
                }
            }
            if (!any_in_use) break;
            zio.sleep(.fromMilliseconds(5)) catch {};
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
            const now_ns = zio.Timestamp.now(.monotonic).toNanoseconds();
            self.grace_deadline = zio.Timestamp.fromNanoseconds(now_ns +% std.time.ns_per_s);
            return;
        }
        if (self.session.grace_phase == .phase1) {
            const now = zio.Timestamp.now(.monotonic);
            const due = self.session.gracePingAcked() or
                (self.grace_deadline != null and now.toNanoseconds() >= self.grace_deadline.?.toNanoseconds());
            if (due) {
                try self.session.applyCommand(.graceful_phase2);
                try self.processIntents();
                self.grace_deadline = zio.Timestamp.fromNanoseconds(
                    now.toNanoseconds() +% self.config.limits.graceful_drain_timeout_ns,
                );
            }
        } else if (self.session.grace_phase == .phase2) {
            if (self.grace_deadline) |dl| {
                if (zio.Timestamp.now(.monotonic).toNanoseconds() >= dl.toNanoseconds()) {
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
            const sem = if (self.spaceIndex(stream_id)) |i| &self.space_sems[i] else null;
            self.session_mu.unlock();
            const wait_res: anyerror!void = if (sem) |s| s.wait() else error.Canceled;
            self.session_mu.lockUncancelable();
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
        }
        if (bytes.len == 0 or end or flush_ticket != 0) {
            self.sched.enqueueDataBytes(stream_id, &.{}, end, flush_ticket, flush_slot, cap) catch {
                if (flush_ticket != 0) return error.WriteFailed;
                return error.OutOfMemory;
            };
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
        self.write_ch.send(.{
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
        self.write_ch.send(.{
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
        job.hctx = .{ .conn = self, .terminal = undefined, .stream_id = d.stream_id };
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
                self.session.applyCommand(.{ .reset_stream = .{ .stream_id = d.stream_id, .code = .refused_stream } }) catch {};
                try self.processIntents();
                return;
            }
        }

        const slot = self.allocSlot(d.stream_id) orelse {
            if (self.config.accounting) |acct| acct.releaseReaper();
            job.arena.deinit();
            gpa.destroy(job);
            self.session.applyCommand(.{ .reset_stream = .{ .stream_id = d.stream_id, .code = .refused_stream } }) catch {};
            try self.processIntents();
            return;
        };
        slot.reaper_reserved = self.config.accounting != null;
        job.resp.terminal = &slot.terminal;
        job.resp.generation = slot.terminal.currentGeneration();
        job.slot = slot;
        job.hctx.terminal = &slot.terminal;
        job.resp.ctx = &job.hctx;

        _ = self.live_handlers.fetchAdd(1, .acq_rel);
        const handle = if (test_force_spawn_fail)
            error.OutOfMemory
        else
            zio.spawn(runHandlerJob, .{job});
        const h = handle catch {
            runHandlerJob(job);
            while (self.completion_ch.tryReceive()) |sid| {
                self.releaseSlot(sid);
            } else |_| {}
            return;
        };
        if (self.slotIndex(d.stream_id)) |i| {
            self.handler_joins[i] = h;
        }
    }

    const HandlerCtx = struct {
        conn: *Connection,
        terminal: *response.SlotTerminal,
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
                self.completion_ch.send(job.stream_id) catch {};
            }
            _ = self.live_handlers.fetchSub(1, .acq_rel);
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
                        job.resp.send(500, &.{}, "internal error") catch {};
                    } else if (!job.resp.finished) {
                        self.withSession(struct {
                            fn go(c: *Connection, sid: u31) void {
                                c.session.applyCommand(.{ .reset_stream = .{ .stream_id = sid, .code = .internal_error } }) catch {};
                                c.processIntents() catch {};
                            }
                        }.go, job.stream_id);
                    }
                };
                if (job.slot.terminal.getCause() == null and !job.resp.committed) {
                    job.resp.send(500, &.{}, "no response") catch {};
                }
            },
            .not_found => {
                if (job.slot.terminal.getCause() == null) {
                    job.resp.send(404, &.{}, "not found") catch {};
                }
            },
            .method_not_allowed => {
                if (job.slot.terminal.getCause() == null) {
                    const allow = [_]request.Header{.{ .name = "allow", .value = "GET, POST" }};
                    job.resp.send(405, &allow, "method not allowed") catch {};
                }
            },
        }
    }

    fn withSession(self: *Connection, comptime f: anytype, arg: anytype) void {
        self.session_mu.lock() catch return;
        defer self.session_mu.unlock();
        f(self, arg);
    }

    fn lockSession(self: *Connection) response.ResponseError!void {
        self.session_mu.lock() catch return error.Canceled;
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
            defer self.session_mu.unlock();
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
                self.enqueuePending(stream_id, body, true, ticket, slot_i, hctx.terminal) catch |err| return err;
                self.processIntents() catch return error.WriteFailed;
            } else {
                self.next_wire_ticket = ticket;
                self.next_wire_slot = slot_i;
                self.processIntents() catch return error.WriteFailed;
            }
        }
        armed = false;
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
            defer self.session_mu.unlock();
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
            defer self.session_mu.unlock();
            self.enqueuePending(stream_id, bytes, end, if (need_wait) ticket else 0, if (need_wait) slot_i else 0, hctx.terminal) catch |err| return err;
            self.processIntents() catch return error.WriteFailed;
        }
        if (need_wait) try self.waitTicket(slot_i, hctx.terminal);
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
            defer self.session_mu.unlock();
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
        try self.waitTicket(slot_i, hctx.terminal);
    }

    fn abortCb(ctx: *anyopaque, stream_id: u31) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        try self.lockSession();
        defer self.session_mu.unlock();
        self.session.applyCommand(.{ .reset_stream = .{ .stream_id = stream_id, .code = .cancel } }) catch return error.WriteFailed;
        self.processIntents() catch return error.WriteFailed;
    }
};
