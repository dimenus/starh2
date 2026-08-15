//! Connection actor: owns Session, TLS (optional), HPACK, fair write scheduling.
//!
//! # Task topology — read this before you change anything here
//!
//! One accepted socket runs SIX kinds of concurrent task. Almost every defect
//! this file has carried came from a wrong assumption about which task owns a
//! thing, so the ownership table is the contract:
//!
//! | task           | count | owns                                          |
//! |----------------|-------|-----------------------------------------------|
//! | actor (`run`)  | 1     | `Session`, TLS cipher, `FairScheduler`, slots |
//! | `ReadPump`     | 1     | the socket READ direction, the chunk pool     |
//! | `WritePump`    | 1     | the socket WRITE direction, queued payloads   |
//! | `AckDrainer`   | 1     | `TicketTable` completions, byte releases      |
//! | handler        | 0..N  | its own arena and request/response objects    |
//! | reaper worker  | pool  | canceling a handler future (server-wide)      |
//!
//! Rules that follow from the table, all of which have been violated at least
//! once:
//!
//! - A pump NEVER touches `Connection` state. A pump exchanges `WireChunk` and
//!   `WriteCompletion` messages that carry integers only. That is why the pumps
//!   need no lock.
//! - Only the actor emits. Every production byte reaches the wire through
//!   `drainEmit` -> `FairScheduler` sink -> `queueWire`. The
//!   `test_queue_wire_bypass` counter is the mutation canary for that rule and
//!   must stay at zero.
//! - Only ONE task may drive the TLS cipher at a time, and `session_mu` is what
//!   guarantees it. An unlocked decrypt once let the read task and a handler
//!   encrypt concurrently, which left `inner.output` undefined mid-write and
//!   crashed the process (t-538).
//!
//! # Lock discipline
//!
//! `session_mu` is the ONLY mutex. It covers `Session`, the `FairScheduler`,
//! and the TLS connection together, because the three are one consistency
//! domain: a frame is built from session state, debited against session flow
//! control, and encrypted, and no other task may observe a partial step.
//!
//! There is exactly one place that releases the lock inside an operation:
//! `waitForStreamSpace`. It must, because the capacity it waits for can only be
//! freed by the actor, which needs the same lock. It always reacquires
//! UNCANCELABLE before it returns, so the caller's `defer unlock` stays
//! balanced on every path, including cancellation.
//!
//! Handler code never holds the lock. `Response` callbacks take it, do their
//! work, release it, and only then wait on a ticket.
//!
//! # The wake protocol
//!
//! The actor is event-driven and parks with no timer when nothing is armed, so
//! a lost wakeup is a hang and not a delay. Two mechanisms close the race:
//!
//! 1. The actor RESETS `actor_wake` first, then re-checks every
//!    producer-owned source, and only then waits. A `set` that lands between
//!    the check and the reset would otherwise be erased.
//! 2. A producer that frees a resource sets a FLAG before it sets the event
//!    (`sched_refilled` is the example). A flag survives a reset; an event
//!    edge does not. This is what closes the refill race by construction
//!    rather than by timing.
//!
//! # Byte accounting: two kinds, released at different moments
//!
//! Outbound bytes are held in two disjoint pools, and the split is deliberate:
//!
//! - PENDING bytes sit in a `FairScheduler` slab and are released when the
//!   scheduler sink accepts the framed output.
//! - WIRE bytes are the framed frame and are released only when the
//!   `WritePump` reports the write through `AckDrainer`.
//!
//! One counter cannot express both, because a byte is briefly present as
//! pending body AND as framed wire. `deinit` asserts that the two parts sum to
//! the total and that both reach zero, which is the leak check.
const std = @import("std");
const tls = @import("tls");
const session_mod = @import("../core/session.zig");
const hpack = @import("../core/hpack.zig");
const limits_mod = @import("../core/limits.zig");
const frame = @import("../core/frame.zig");
const request = @import("../http/request.zig");
const response = @import("../http/response.zig");
const router_mod = @import("../http/router.zig");
const content_coding = @import("../http/content_coding.zig");
const brotli = @import("../http/brotli.zig");
const wire_pump = @import("wire_pump.zig");
const tls_edge = @import("tls.zig");
const h2c = @import("h2c.zig");
const rates_mod = @import("../core/rates.zig");
const ticket_table = @import("ticket_table.zig");
const control_pool = @import("control_pool.zig");
const fair_scheduler = @import("fair_scheduler.zig");
const io_queue = @import("io_queue.zig");
const slab_pool = @import("slab_pool.zig");

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

/// Phase trace for one flushed write, off unless a benchmark turns it on.
///
/// # Why this exists
///
/// A saturated connection delivers ~45k SSE events/s while the process uses
/// 1.7 of 12 cores, and a CPU profile cannot say why: contention on
/// `session_mu`, work done under it, transport handoff, and wake-to-run latency
/// all produce the same futex and scheduler frames. On-CPU sampling cannot see
/// time a task spends blocked or runnable-but-not-scheduled, which is where a
/// serialized pipeline actually spends its time.
///
/// So each sampled event is timestamped at four boundaries and reported as
/// three intervals plus the receipt:
///
/// - `block`   before `lockSession` -> after acquire. Dominant means a convoy on
///             the mutex: tasks queue for the lock itself.
/// - `hold`    after acquire -> after unlock. Dominant means the serial section
///             is genuinely expensive: framing, flow-control debit, encryption,
///             or the blocking `write_ch.putOne` that runs while the lock is
///             held.
/// - `ack`     unlock -> `tickets.complete` on the AckDrainer. Dominant means
///             the transport handoff, not the connection's own work.
/// - `resume`  `tickets.complete` -> `waitTicket` returns. Dominant means
///             wake-to-run scheduler latency, not any of the above.
///
/// One event in `trace_sample_every` is measured, so the instrument cannot
/// become the load. Counters are server-wide and monotonic; a harness reads
/// them before and after a run and divides.
pub const trace = struct {
    /// Set once at boot by a benchmark binary. A plain bool: it is written
    /// before any connection exists and only read afterwards.
    pub var enabled: bool = false;
    pub var sample_every: u64 = 1024;

    pub var writes: std.atomic.Value(u64) = .init(0);
    /// Sampled events refused because another sample was already in flight on
    /// that connection. Printed, because a low sample count with a silent skip
    /// count is how a trace hides that it measured a biased subset.
    pub var skipped: std.atomic.Value(u64) = .init(0);
    pub var samples: std.atomic.Value(u64) = .init(0);
    pub var block_ns: std.atomic.Value(u64) = .init(0);
    pub var hold_ns: std.atomic.Value(u64) = .init(0);
    pub var ack_ns: std.atomic.Value(u64) = .init(0);
    pub var resume_ns: std.atomic.Value(u64) = .init(0);
    /// Worst single sample of each, so a mean cannot hide a stall.
    pub var block_max: std.atomic.Value(u64) = .init(0);
    pub var hold_max: std.atomic.Value(u64) = .init(0);
    pub var ack_max: std.atomic.Value(u64) = .init(0);
    pub var resume_max: std.atomic.Value(u64) = .init(0);

    fn bumpMax(cell: *std.atomic.Value(u64), v: u64) void {
        var cur = cell.load(.monotonic);
        while (v > cur) {
            cur = cell.cmpxchgWeak(cur, v, .monotonic, .monotonic) orelse break;
        }
    }

    pub fn snapshot(out: *[10]u64) void {
        out[0] = samples.load(.acquire);
        out[1] = block_ns.load(.acquire);
        out[2] = hold_ns.load(.acquire);
        out[3] = ack_ns.load(.acquire);
        out[4] = resume_ns.load(.acquire);
        out[5] = block_max.load(.acquire);
        out[6] = hold_max.load(.acquire);
        out[7] = ack_max.load(.acquire);
        out[8] = writes.load(.acquire);
        out[9] = skipped.load(.acquire);
    }
};

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
    /// Server-wide outbound DATA slabs, borrowed for this connection's lifetime.
    slab_pool: *slab_pool.SlabPool,
    /// Server-wide brotli encoder pool when response compression is enabled.
    compression_pool: ?*brotli.Pool = null,
    /// True when accept path reserved a concurrent TLS handshake slot.
    handshake_held: bool = false,
};

/// Shared with Server: global stream/reaper/memory/handshake admission.
///
/// Every counter here is touched by many connections at once, so each one uses
/// a compare-and-swap loop and not a read-then-add. A `fetchAdd` followed by a
/// limit check would admit past the limit whenever two connections raced, and
/// the overshoot is exactly the case the limit exists to prevent.
///
/// The release paths use `fetchSub` with an assert instead. A release is always
/// paired with a successful reserve, so it cannot fail; the assert is there to
/// catch a double release, which is the failure this shape actually has.
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

/// Actor-owned state for one running handler.
///
/// The slot exists because a handler's own memory cannot hold this. A handler
/// job is destroyed when the handler returns, but the actor may still need to
/// record a terminal cause for that stream, so the terminal state has to
/// outlive the job. `Response.terminal` therefore points here and never into
/// the arena.
///
/// `completion_owner` is a three-state race arbiter, and it is what makes the
/// slot release exactly-once. Two tasks can decide that a handler is finished:
/// the handler itself when it returns, and the actor when it cancels. Both
/// attempt a CAS; the winner posts the completion, the loser does nothing.
/// - `live` (0): the handler is running and will report itself.
/// - `reaper_owned` (1): the actor won the race and moved the join handle to a
///   reaper worker, which reports after `cancel` returns.
/// - `reported` (2): the completion is posted or queued. Also the free state.
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

/// Cancellation happens off the actor, and this pool is why.
///
/// `Future.cancel` BLOCKS until the target task actually stops. If the actor
/// called it directly, one handler that is slow to notice cancellation would
/// stall the whole connection — including the reads and the writes that might
/// be what lets the handler finish. So the actor hands the join handle to a
/// worker here and continues at once.
///
/// The reaper capacity is reserved BEFORE a handler is admitted
/// (`tryReserveReaper` in `dispatch`), never at cancel time. Cancellation must
/// not be able to fail for want of a resource, because there is no way to
/// retry it and no other path that would release the slot.
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

    /// The worker's order is fixed and load-bearing:
    ///
    /// 1. `cancel` — waits until the handler task has really stopped. Only then
    ///    is the handler's arena unused and its slot safe to reuse.
    /// 2. `swap(reported)` — claim the right to report. If the handler returned
    ///    naturally in the meantime it already reported, and the previous value
    ///    is not `reaper_owned`; this worker then stays silent.
    /// 3. post the completion, then WAKE the actor. A post without a wake is a
    ///    slot that sits until an unrelated event happens to arrive.
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

/// Entry point for one accepted socket. Owns the `Connection` for its whole
/// life, so `defer conn.deinit()` is the single cleanup point.
///
/// The stream hook context is patched after construction because the hooks need
/// a pointer to the `Connection`, which does not exist while `Session.init`
/// runs. The read-chunk free list is seeded here rather than in `init` for the
/// same reason: the queues are only live once the struct has its final address.
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
    /// Debug proof that `session_mu` is held where Session or the scheduler is
    /// touched. Written only while the mutex is held, so it needs no atomic.
    ///
    /// It proves SOMEBODY holds the mutex, not that this task does. A thread id
    /// would be wrong: zio migrates tasks, and a holder can park inside the
    /// critical section (`sendAccountedWire` can block on `write_ch.putOne`).
    /// That is still enough to catch the class it exists for — a call made with
    /// the mutex not held at all — which is exactly how the two sites in t-731
    /// read Session and scheduler state unsynchronized.
    session_held: bool = false,
    /// Phase-trace handoff for ONE sampled write at a time. The handler stores
    /// the ticket it is about to wait on; the AckDrainer stamps the completion
    /// time for that exact ticket. See `trace`.
    trace_ticket: std.atomic.Value(u64) = .init(0),
    trace_ack_ns: std.atomic.Value(u64) = .init(0),
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
            config.slab_pool,
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

    /// Release everything, in an order that cannot double-free.
    ///
    /// Every channel is drained BEFORE its backing buffer is freed, and each
    /// drain applies the accounting the AckDrainer would have applied — the
    /// drainer has already stopped by now. Queued read chunks return their pool
    /// index; queued write chunks free their payload and release wire bytes.
    ///
    /// The completion drain uses `releaseSlot` and never discards. A reaper can
    /// post after `shutdownHandlers` returned, and a discarded completion would
    /// leak the reaper token.
    ///
    /// The closing asserts are the leak detector: the two outbound parts must
    /// sum to the whole, and both must be zero. A mismatch means some path
    /// released the wrong kind, which no test would otherwise notice.
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

    /// The AckDrainer task. It exists so that the WritePump can stay a pure
    /// byte mover: the pump reports what it did, and this task applies the
    /// consequences to `Connection` state.
    ///
    /// Per completion, in this order:
    /// 1. Release the WIRE bytes and any control-pool occupancy. This must
    ///    precede the wake, or a woken waiter re-checks capacity that is not
    ///    free yet and parks again.
    /// 2. `fail_all` -> mark the writer failed and fail every in-flight ticket.
    ///    Note what it does NOT do: it never touches `handlers[]`. Terminal
    ///    causes are actor-owned, and a second writer of that state would race
    ///    the actor's own teardown.
    /// 3. Otherwise complete the one ticket the chunk carried.
    /// 4. Wake the space waiters, then the actor.
    ///
    /// The loop exits on the `shutdown` completion, which the WritePump posts
    /// as its last act. That is the join point the actor's `defer` waits on.
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
                if (trace.enabled and self.trace_ticket.load(.acquire) == ack.ticket) {
                    self.trace_ack_ns.store(nowNs(self.config.io), .release);
                }
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
    ///
    /// The AckDrainer only SETS the flag; this runs on the actor, under
    /// `session_mu`. That separation is the point: teardown touches `Session`,
    /// the scheduler, and every handler slot, so it must run where those are
    /// owned. `writer_fail_handled` makes it once-only, because both the actor
    /// loop and `drainEmit`'s error path can arrive here.
    ///
    /// Order, and each step has a reason:
    /// 1. Mark every live handler `.internal` (WriteFailed). The write actually
    ///    failed, so this is not a peer-side ConnectionClosed.
    /// 2. Drop pending DATA, wake its flush waiters, release the pending bytes.
    ///    A dead connection can never emit those bytes, so holding the
    ///    reservation would keep server capacity charged until the process ends.
    /// 3. Drain the control queues WITHOUT writing, and free the payloads.
    /// 4. Reset every open stream and emit a GOAWAY through `Session`, so the
    ///    stream hooks release the server-wide stream credit.
    /// 5. Free the resulting intents instead of sending them.
    ///
    /// What it must NOT do: drain `write_ch`. The WritePump is the sole
    /// consumer while the connection lives. Stealing chunks races the
    /// AckDrainer's releases and can double-free payload bytes.
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

    /// Reset streams whose peer stopped reading.
    ///
    /// A peer that opens a stream and then never grants window costs the server
    /// a full per-stream slab for as long as it likes. Flow control cannot help
    /// here, because withholding credit is legal behaviour. So progress is
    /// measured instead: `last_progress_ns` advances whenever bytes enter or
    /// leave the pending slab, and a stream with pending bytes and no progress
    /// inside `slow_consumer_timeout_ns` loses its stream.
    ///
    /// The wake order matters. The flush waiter is woken with `ok = false`
    /// AFTER the terminal cause is set, so `TicketTable.wait` finds the cause
    /// and returns the exact `error.SlowConsumer` rather than a generic
    /// `WriteFailed`. The handler then learns why it was stopped.
    ///
    /// This is a deliberate kill and not a regression. A test host under load
    /// can starve a healthy client, which is why the timeout is a limit and not
    /// a short deadline.
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

    /// Park until something the actor cares about happens.
    ///
    /// Three sources race here: the actor event that every producer sets, the
    /// server shutdown event, and the earliest armed protocol deadline. The
    /// timer branch is added ONLY when a deadline exists, so an idle connection
    /// with no armed timer costs no periodic wakeup at all.
    ///
    /// The caller is responsible for the reset-then-recheck sequence that makes
    /// this safe. See `run`.
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

    /// The actor. One task, one connection, from first byte to close.
    ///
    /// ## Startup sequence
    /// 1. Spawn ReadPump, WritePump, AckDrainer. The pumps must exist before
    ///    the TLS handshake, because the handshake itself exchanges bytes
    ///    through them — there is no separate handshake I/O path.
    /// 2. TLS handshake, if the endpoint is TLS. It ends by draining any
    ///    leftover bytes, and that drive holds `session_mu`. See
    ///    `tlsHandshakeViaPumps`.
    /// 3. Flush the server preface that `Session.init` already queued.
    /// 4. For h2c, wait for the client preface under the preface deadline.
    ///
    /// ## Steady-state iteration
    /// Each pass does the same five things, and the order is the contract:
    /// 1. Clear `sched_refilled`, then drain handler completions. Clearing
    ///    first means a refill that lands during this pass is still seen.
    /// 2. Under `session_mu`: apply a pending writer failure, publish the
    ///    clock, check protocol deadlines, advance graceful shutdown, kill slow
    ///    consumers, then EMIT. Emission is last because every earlier step can
    ///    add to what must be emitted.
    /// 3. Check for the graceful finish condition.
    /// 4. Try to take a read chunk without blocking.
    /// 5. If there is none: RESET the wake event, re-check every producer-owned
    ///    source, and only then park. This reset-then-recheck order is what
    ///    makes a `set` that races the reset harmless — the flag or the queue
    ///    still holds the evidence.
    ///
    /// ## Teardown sequence (the `defer` block, then the tail)
    /// 1. Tell both pumps to stop, and push a sentinel so a WritePump parked on
    ///    an empty queue wakes.
    /// 2. `shutdown` the socket. A read parked in the kernel does not observe a
    ///    flag, so this is what unblocks it; task cancellation is the
    ///    authoritative backstop.
    /// 3. Cancel the write pump, then the read pump, then AWAIT the AckDrainer.
    ///    The drainer must be last, because it is the task that applies the
    ///    releases the pumps emit while they stop.
    /// 4. `shutdownHandlers` runs after the loop and returns only when every
    ///    slot has passed through `releaseSlot`.
    /// 5. Close the socket exactly once, guarded by `socket_closed`.
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

        // Under the lock: the TLS leftover-decrypt path above can already have
        // DISPATCHED a handler (see the comment at `driveDecrypt`'s call in the
        // handshake), and that handler emits through `session_mu`. Draining
        // intents here without the lock races it on Session, the scheduler and
        // the TLS cipher at once.
        {
            self.lockSessionUncancelable(io);
            defer self.unlockSession(io);
            try self.flushSessionIntents();
        }

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
                self.lockSessionUncancelable(io);
                defer self.unlockSession(io);
                if (self.writer_failed.load(.acquire)) self.handleWriterFailed();
                if (self.session.terminal != .none) break;
                const now = nowNs(io);
                self.session.edge_now_ns = now;
                try self.session.checkIdleDeadlines(now);
                try self.maybeBeginGraceful();
                try self.checkSlowConsumers();
                self.drainEmit() catch {
                    // Fail-closed: Session may already be debited — terminate connection.
                    // A frame that failed after its flow-control debit cannot
                    // be retried and cannot be un-debited, so the connection's
                    // window state no longer matches the peer's. The only
                    // correct move left is to close.
                    self.writer_failed.store(true, .release);
                    self.handleWriterFailed();
                };

                // Both checks stay INSIDE this lock. `drainEmit` above can
                // terminate the connection, so the terminal check has to run
                // after it — and `grace_phase`, `terminal` and
                // `sched.pendingCount()` are all plain fields that a handler
                // task mutates under this same mutex. Reading them outside it
                // was a data race, and the `live_handlers` guard did not cover
                // it: `and` evaluates left to right, so `pendingCount()` was
                // read BEFORE the handler count was known to be zero.
                // The assert travels with the checks: move them back out of the
                // lock and this fires, instead of the race returning silently.
                self.assertSessionHeld("graceful finish check");
                if (self.session.terminal != .none) break;
                // Graceful finish needs all three conditions together: phase 2
                // reached, nothing left to write, and no handler still running.
                // Any one alone would cut off work that is still in flight.
                if (self.shutting_down and self.session.grace_phase == .phase2 and
                    self.sched.pendingCount() == 0 and self.live_handlers.load(.acquire) == 0)
                {
                    try self.finishGraceful();
                    break;
                }
            }

            var maybe_chunk = io_queue.tryGet(wire_pump.WireChunk, &self.read_ch, io);
            if (maybe_chunk == null) {
                // Reset before rechecking every producer-owned source; this closes
                // the set-before-reset lost-wakeup race.
                // The recheck list below must name EVERY producer: read chunks,
                // handler completions, scheduler refills, writer failure, and
                // the shutdown flag. A source left out of this list is a hang,
                // not a slow path, because the actor then parks with work
                // already waiting for it.
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

            self.lockSessionUncancelable(io);
            defer self.unlockSession(io);
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

    /// Append ciphertext to the boot-reserved accumulator.
    ///
    /// `appendSliceAssumeCapacity` is required here and not an optimization.
    /// The accumulator is reserved once at connection boot and counted in
    /// `resourceUpperBound`; a growing append would let a peer choose the
    /// server's memory use by never completing a record. The explicit check
    /// above turns that into a refused connection instead.
    fn appendTlsInput(self: *Connection, bytes: []const u8) !void {
        const limit = self.config.limits.tls_recv_acc_bytes;
        const held = self.tls_recv_acc.items.len;
        if (held > limit or bytes.len > limit - held) return error.TlsInputTooLarge;
        // The guard above tests the CONFIGURED limit; the append below relies on
        // the RESERVED capacity. Those are two different numbers that happen to
        // be equal, because `init` reserves exactly `tls_recv_acc_bytes`. Assert
        // the one that actually protects the write, so a future change that
        // reserves less than it admits fails here by name instead of corrupting
        // memory inside `appendSliceAssumeCapacity`.
        std.debug.assert(held + bytes.len <= self.tls_recv_acc.capacity);
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
            self.lockSessionUncancelable(self.config.io);
            {
                defer self.unlockSession(self.config.io);
                try self.session.ingest(chunk.bytes[0..chunk.len]);
                try self.processIntents();
                if (self.session.terminal != .none) return error.ConnectionClosed;
            }
        }
        self.handshake_deadline = null;
    }

    /// Drive the TLS handshake through the same pumps that carry application
    /// data. There is no separate handshake socket path, so the read pump owns
    /// the read direction from the very first byte.
    ///
    /// The loop is a state machine over `serverDrive`: emit whatever ciphertext
    /// it produced, consume whatever input it accepted, then act on the status.
    /// The accumulator is compacted with `memmove` rather than reallocated,
    /// because it is reserved once at boot and must never grow.
    ///
    /// ALPN is checked before the connection is accepted. This stack serves
    /// HTTP/2 only, so a peer that did not agree to `h2` is refused at the
    /// handshake instead of at its first frame.
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
            // `consumed` crosses the boundary from the pinned tls.zig fork. A
            // value larger than the input would underflow `rest` — a usize
            // underflow reports as a slice panic several lines later, and the
            // real cause (a fork or patch that no longer matches the ABI) would
            // not appear anywhere in the message. Name the contract here.
            std.debug.assert(drive.consumed <= self.tls_recv_acc.items.len);
            std.debug.assert(drive.ciphertext_len <= self.ciphertext_scratch.len);
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
            self.lockSessionUncancelable(self.config.io);
            defer self.unlockSession(self.config.io);
            try self.driveDecrypt();
        }
    }

    /// Claim a handler slot. Actor-only, so the linear scan needs no lock and
    /// no free list. The slot count equals `max_streams_per_connection`, so the
    /// scan is bounded by a configured limit and a `null` result means the
    /// connection is genuinely at its stream cap.
    ///
    /// The generation bump is the safety mechanism for slot REUSE. A `Body`
    /// held by an old handler still points at this slot, so a stale write must
    /// be rejected rather than applied to the new stream. The `Body` carries
    /// the generation it was created with, and any mismatch becomes
    /// `error.BodyClosed`.
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

    /// Free a slot after its handler has finished. Actor-only.
    ///
    /// The `await` is the memory-safety barrier and not bookkeeping. The
    /// handler's arena is destroyed when the handler task returns, so the slot
    /// may only be reused after this task confirms it has really stopped.
    ///
    /// The reaper token is released here, at the single point where the slot's
    /// lifetime actually ends. Releasing it earlier — at cancel time — would
    /// hand the capacity to a new stream while the old handler was still
    /// running.
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

    /// Stop a handler because its stream is over — a peer RST, or a local
    /// reset. Actor-only.
    ///
    /// The sequence is ordered so that a handler always has a way out:
    /// 1. Publish the terminal cause. Every `Response` entry point reads it
    ///    first, so a handler that is running notices at its next call.
    /// 2. Wake anything that waits on this stream: the flush ticket and the
    ///    capacity event. A waiter that is not woken here waits forever,
    ///    because the stream will produce no further events.
    /// 3. Skip the join-cancel if the handler is awaiting a wire receipt (see
    ///    `HandlerSlot.awaiting_receipt`).
    /// 4. Otherwise transfer the join handle to a reaper, but ONLY when a
    ///    handle exists to transfer. A CAS without a handle once left the
    ///    ownership stuck at `reaper_owned`: the handler exited without
    ///    reporting, no reaper ever ran, and the slot plus its reaper token
    ///    leaked for the life of the connection.
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

    /// Release everything a terminal stream still holds, and wake whoever waits
    /// on it. This is called before any cancellation decision, so that a
    /// handler which is about to be left alone still has its wake guaranteed.
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

    /// Stop every handler and wait until all slots are released. This is the
    /// last thing the actor does, and it must be exhaustive: `deinit` asserts
    /// that no slot is still in use, and frees the storage the handlers point
    /// into.
    ///
    /// Two passes, and the second is the one that is easy to get wrong. The
    /// first pass sets causes, wakes waiters, and hands off join handles. The
    /// second waits — and it waits on SLOTS, not on `live_handlers`. The
    /// difference is a real race: a reaper posts its completion only after
    /// `cancel` returns, and `cancel` returns after the handler has already
    /// decremented `live_handlers`. A wait on `live_handlers == 0` can
    /// therefore finish before the completion is posted, and the release is
    /// then dropped.
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

    /// Decrypt accumulated TLS input and feed the plaintext to `Session`.
    ///
    /// The CALLER must hold `session_mu`. Both call sites do. The lock is not
    /// taken inside because the other caller already holds it, and a
    /// locked/unlocked variant would hide one site's mistake behind a
    /// parameter (t-538 was exactly that mistake).
    ///
    /// `firstRecord` feeds ONE TLS record per call on purpose. See the note on
    /// that function: a coalesced small record followed by a maximum-size one
    /// can advance the cipher sequence and only then fail for want of output
    /// space, which is unrecoverable.
    ///
    /// The final guard exits when a pass consumed nothing and produced nothing.
    /// Without it a record that can never make progress spins the actor.
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
            // Same ABI contract as the handshake drive above, plus the two
            // output lengths this path slices with.
            std.debug.assert(res.consumed <= self.tls_recv_acc.items.len);
            std.debug.assert(res.ciphertext_len <= self.ciphertext_scratch.len);
            std.debug.assert(res.plaintext_len <= self.plaintext_scratch.len);
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

    /// Two-phase graceful shutdown (RFC 9113 section 6.8).
    ///
    /// One GOAWAY is not enough, because the server and the client can be
    /// choosing stream ids at the same instant. A request already in flight
    /// would be refused for no reason. So:
    ///
    /// - Phase 1 sends GOAWAY with the maximum stream id — "I am going away,
    ///   but nothing is refused yet" — followed by a PING. The PING ack proves
    ///   the peer has processed everything sent before it, which fixes the set
    ///   of streams that were genuinely in flight.
    /// - Phase 2 sends a second GOAWAY naming the highest stream actually
    ///   accepted, and refuses everything above it.
    ///
    /// The one-second deadline between the phases is a liveness guard: a peer
    /// that never acks the PING must not be able to hold the shutdown open.
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

    /// Block a handler until its stream's pending slab has room.
    ///
    /// This is the backpressure boundary: a handler that produces faster than
    /// the peer reads is parked here rather than allowed to grow a buffer. That
    /// is what keeps a single stream inside `outbound_bytes_per_stream` and the
    /// connection inside `resourceUpperBound`.
    ///
    /// This is the ONLY place that releases `session_mu` mid-operation, and it
    /// must, because only the actor can free this capacity and the actor needs
    /// the same lock. The sequence per attempt:
    /// 1. Check the terminal cause and the writer state FIRST, so a dead stream
    ///    returns an exact error instead of parking.
    /// 2. Re-read the pending length; return if there is room.
    /// 3. Reset the capacity event BEFORE releasing the lock, so a wake that
    ///    arrives during the gap is not lost.
    /// 4. Unlock, wait, and reacquire UNCANCELABLE. The uncancelable
    ///    reacquisition is what keeps the caller's `defer unlock` correct even
    ///    when the handler is being canceled.
    /// 5. Re-check the terminal state before trusting the wait result, because
    ///    a cancel and a capacity wake can arrive together.
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
            self.unlockSession(self.config.io);
            const wait_res: anyerror!void = if (event) |e| e.wait(self.config.io) else error.Canceled;
            self.lockSessionUncancelable(self.config.io);
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
    ///
    /// A body of any size is copied in cap-sized pieces, so an arbitrarily
    /// large response never needs an arbitrarily large buffer. Per piece:
    /// reserve the outbound bytes, copy into the slab, then set
    /// `sched_refilled` and wake the actor. The flag is set BEFORE the wake for
    /// the reason given in the module header — an event edge can be erased by
    /// the actor's reset, a flag cannot.
    ///
    /// The trailing zero-length enqueue is not redundant. It carries the
    /// END_STREAM marker or the flush ticket for a body whose bytes are all
    /// already queued, and for an empty body it is the only enqueue there is.
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
            self.sched.enqueueDataBytes(stream_id, bytes[off..][0..take], false, 0, 0) catch {
                self.applyOutboundRelease(take, .pending);
                return error.OutOfMemory;
            };
            off += take;
            self.sched_refilled.store(true, .release);
            self.actor_wake.set(self.config.io);
        }
        if (bytes.len == 0 or end or flush_ticket != 0) {
            self.sched.enqueueDataBytes(stream_id, &.{}, end, flush_ticket, flush_slot) catch {
                if (flush_ticket != 0) return error.WriteFailed;
                return error.OutOfMemory;
            };
            self.sched_refilled.store(true, .release);
            self.actor_wake.set(self.config.io);
        }
    }

    /// Single FairScheduler drain: terminal → ordinary(+forced DATA) → DRR DATA.
    /// All emits go through queueWire via scheduler sink only.
    ///
    /// Actor-only, under `session_mu`. The three callbacks below are how the
    /// scheduler reaches protocol state without knowing what a protocol is: it
    /// asks for the windows, asks for a framed DATA frame, and hands the result
    /// to the sink.
    ///
    /// The `buildData` -> `sink` pair has a strict order. `buildData` DEBITS
    /// flow control, so the pending-byte release is deferred through
    /// `pending_data_outbound_release` and applied only after the sink accepts
    /// the frame. If the sink fails, the debit has already happened and cannot
    /// be undone, so the connection fails closed rather than continuing with
    /// window state the peer does not share.
    fn drainEmit(self: *Connection) !void {
        self.assertSessionHeld("drainEmit");
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

    /// Turn queued `Session` intents into scheduler work, then emit.
    ///
    /// Intents are copied into an actor-owned batch first. Draining into the
    /// session's own storage would break as soon as a branch below pushes a new
    /// intent — which `dispatch` and the reset paths both do — because the
    /// slice would alias storage that is being written.
    ///
    /// `consumed` advances BEFORE each branch runs, so the `errdefer` owns only
    /// the untouched tail. Every branch takes ownership of its own intent on
    /// both the success and the failure path, so a shared cleanup would double
    /// free.
    ///
    /// DATA never goes straight to the wire here. It enters the scheduler, so
    /// that fairness between streams is decided in one place.
    fn processIntents(self: *Connection) anyerror!void {
        self.assertSessionHeld("processIntents");
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

    /// Hand finished wire bytes to the WritePump, with their accounting
    /// attached.
    ///
    /// The chunk carries the release amounts rather than a callback, so the
    /// pump needs no access to `Connection`. The AckDrainer applies them when
    /// the write completes. That is the whole reason the pumps can run without
    /// a lock.
    ///
    /// Both failure paths release the reservation AND free the bytes before
    /// they return. After a successful `putOne` the pump owns the memory, and
    /// this function must not touch it again.
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

    /// The single exit from protocol bytes to transport bytes. Called ONLY by
    /// the `FairScheduler` sink; `test_queue_wire_bypass` proves it.
    ///
    /// For h2c the frame goes straight to the pump. For TLS it is encrypted
    /// here, on the actor, under `session_mu` — the cipher is actor-owned, and
    /// two tasks driving it concurrently is what crashed the process in t-538.
    ///
    /// One plaintext frame can become several TLS records, and the ticket, the
    /// flush flag, and the control-pool release must ride on the LAST record
    /// only. A ticket attached to an earlier record would tell the handler its
    /// write was delivered while a later part of the same frame was still
    /// queued.
    ///
    /// Note the two reservations. The plaintext holds a wire reservation while
    /// it is being encrypted, and each ciphertext record takes its own. The
    /// plaintext reservation is released once the last record is queued, so the
    /// peak accounts for both, which is what actually exists in memory.
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
                // Same ABI contract. An over-large `consumed` would push `off`
                // past the end and panic on the NEXT iteration's slice, which
                // points at the loop rather than at the encrypt that caused it.
                std.debug.assert(res.consumed <= bytes.len - off);
                std.debug.assert(res.ciphertext_len <= self.ciphertext_scratch.len);
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

    /// Build a handler job and spawn it. Actor-only.
    ///
    /// Everything the handler will read is COPIED into a per-job arena, and the
    /// session-owned originals are freed by the `defer` on return. The handler
    /// runs on another task with no lock, so it must not hold a borrow into
    /// session memory that the actor may free or reuse at any moment. One arena
    /// per job also makes cleanup a single call on every exit path.
    ///
    /// Admission order is reserve-then-claim, and it is deliberate:
    /// 1. Reserve reaper capacity. A handler that cannot be canceled must never
    ///    start, because cancellation has no retry path.
    /// 2. Claim a handler slot.
    /// 3. Point `Response` at the SLOT's terminal state, never at job memory.
    /// 4. Increment the live counters, then spawn.
    ///
    /// Each failure step undoes the step before it and answers the peer with
    /// REFUSED_STREAM, which is retryable, rather than dropping the request.
    ///
    /// A spawn failure runs the handler INLINE on the actor. That blocks the
    /// connection for the duration, which is bad, and it is still better than
    /// the alternative: a stream that is admitted and accounted for but never
    /// answered.
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
        // Combine repeated Accept-Encoding lines (RFC 9110) into one value.
        var ae_parts: std.ArrayList([]const u8) = .empty;
        defer ae_parts.deinit(a);
        for (headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, "accept-encoding")) {
                ae_parts.append(a, h.value) catch {};
            }
        }
        const accept_encoding = if (ae_parts.items.len == 0)
            ""
        else
            content_coding.joinAcceptEncoding(a, ae_parts.items) catch "";

        // HandlerCtx: Response.ctx points at job-local ctx with stable terminal pointer.
        job.hctx = .{
            .conn = self,
            .terminal = undefined,
            .slot = undefined,
            .stream_id = d.stream_id,
            .method = job.req.method,
            .accept_encoding = accept_encoding,
        };
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
        method: request.Method = .GET,
        /// Combined Accept-Encoding field value (arena-owned), empty if absent.
        accept_encoding: []const u8 = "",
        encoder: ?*brotli.Encoder = null,
        compressing: bool = false,
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

    /// The handler task entry point.
    ///
    /// The `defer` runs on every exit, including a cancellation, and its order
    /// is the contract with the actor:
    /// 1. CAS `live` -> `reported`. The winner posts the completion. If a
    ///    reaper already claimed the slot, this handler stays silent and the
    ///    reaper reports after its `cancel` returns.
    /// 2. Post the completion, then wake the actor. Without the wake the slot
    ///    sits until an unrelated event arrives.
    /// 3. Decrement the live counter, then destroy the arena and the job.
    ///
    /// The counters are published at the MUTATION sites, not once per actor
    /// iteration. A parked actor republishes nothing, so a per-iteration value
    /// stays stale for as long as the connection is quiet and reports handlers
    /// that have already exited.
    ///
    /// Every fallback response is guarded by a terminal-cause check. A stream
    /// that is already dead must not receive a synthetic 404 or 500: the write
    /// would fail anyway, and the real cause is the one the handler should
    /// report.
    fn runHandlerJob(job: *HandlerJob) void {
        const self = job.conn;
        defer {
            // Encoder contexts are server-wide; release even on cancel/reset so
            // a stranded SSE cannot pin a pool slot past handler death.
            if (job.hctx.encoder) |enc| {
                enc.destroy();
                job.hctx.encoder = null;
                job.hctx.compressing = false;
            }
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
        self.session_held = true;
        defer self.unlockSession(self.config.io);
        f(self, arg);
    }

    fn lockSession(self: *Connection) response.ResponseError!void {
        self.session_mu.lock(self.config.io) catch return error.Canceled;
        self.session_held = true;
    }

    /// Every acquisition and release of `session_mu` goes through these two, so
    /// `session_held` cannot drift from the mutex. Do not call the mutex
    /// directly.
    fn lockSessionUncancelable(self: *Connection, io: std.Io) void {
        self.session_mu.lockUncancelable(io);
        self.session_held = true;
    }

    fn unlockSession(self: *Connection, io: std.Io) void {
        // Cleared while the mutex is still held, or the next holder would see a
        // stale false.
        self.session_held = false;
        self.session_mu.unlock(io);
    }

    /// Called where Session or the scheduler is about to be read or mutated.
    fn assertSessionHeld(self: *const Connection, comptime site: []const u8) void {
        if (std.debug.runtime_safety and !self.session_held) {
            std.debug.panic("session_mu not held at {s}", .{site});
        }
    }

    /// One-shot response: headers plus a complete body.
    ///
    /// Runs on the HANDLER task. The shared shape of all five callbacks below:
    /// 1. Check the terminal cause before doing any work.
    /// 2. Reserve a ticket BEFORE taking the lock. Ticket reservation can fail,
    ///    and it is better to fail without the connection's only mutex held.
    /// 3. Take `session_mu`, apply the command, materialize the intents.
    /// 4. RELEASE the lock.
    /// 5. Only then wait on the ticket.
    ///
    /// Step 5 must be outside the lock. The ticket is completed by the
    /// AckDrainer after the WritePump has written the bytes, and the actor
    /// needs `session_mu` to produce those bytes. A wait inside the lock is a
    /// deadlock.
    ///
    /// The `armed` flag releases the ticket slot on any failure that happens
    /// before the wait begins. Once the wait starts the slot belongs to
    /// `TicketTable.wait`, which releases it in its own `defer`.
    /// Decide whether this response may use brotli, and optionally open an encoder.
    ///
    /// Returns the encoder when compression will actually run. When the response
    /// is compression-eligible by configuration but the pool/init fails, the
    /// identity fallback is counted and null is returned — still with Vary.
    fn prepareCompression(
        self: *Connection,
        hctx: *HandlerCtx,
        status: u16,
        headers: []const request.Header,
        body_len: ?usize, // null = streaming; Some(n) = full-body path
        sse: bool,
    ) struct { compress: bool, add_vary: bool } {
        if (!self.config.limits.response_compression) return .{ .compress = false, .add_vary = false };
        if (hctx.method == .HEAD) return .{ .compress = false, .add_vary = false };
        if (status == 204 or status == 304) return .{ .compress = false, .add_vary = false };
        // Handler already chose a coding — pass through untouched (I5).
        if (content_coding.headerHasContentEncoding(request.Header, headers)) return .{ .compress = false, .add_vary = false };

        const ct = if (sse)
            "text/event-stream"
        else
            content_coding.findHeaderValue(request.Header, headers, "content-type") orelse return .{ .compress = false, .add_vary = false };
        if (!content_coding.isCompressibleContentType(ct)) return .{ .compress = false, .add_vary = false };

        // Eligible by configuration: Vary even if we serve identity.
        const add_vary = true;
        if (!content_coding.acceptsBrotli(if (hctx.accept_encoding.len == 0) null else hctx.accept_encoding)) {
            return .{ .compress = false, .add_vary = add_vary };
        }
        if (body_len) |n| {
            if (n == 0) return .{ .compress = false, .add_vary = add_vary };
            if (n < self.config.limits.compression_min_bytes) return .{ .compress = false, .add_vary = add_vary };
        }
        const pool = self.config.compression_pool orelse {
            return .{ .compress = false, .add_vary = add_vary };
        };
        const enc = pool.tryAcquire() orelse return .{ .compress = false, .add_vary = add_vary };
        hctx.encoder = enc;
        hctx.compressing = true;
        return .{ .compress = true, .add_vary = add_vary };
    }

    fn appendResponseHeaders(
        gpa: std.mem.Allocator,
        hlist: *std.ArrayList(hpack.HeaderField),
        headers: []const request.Header,
        opts: struct {
            sse: bool = false,
            compress: bool = false,
            add_vary: bool = false,
            strip_content_length: bool = false,
        },
        /// Optional gpa-owned merged Vary value; caller frees after applyCommand.
        owned_vary: *?[]u8,
    ) response.ResponseError!void {
        if (opts.sse) {
            hlist.append(gpa, .{ .name = "content-type", .value = "text/event-stream" }) catch return error.OutOfMemory;
            hlist.append(gpa, .{ .name = "cache-control", .value = "no-cache" }) catch return error.OutOfMemory;
            hlist.append(gpa, .{ .name = "x-accel-buffering", .value = "no" }) catch return error.OutOfMemory;
        }
        var handler_vary: ?[]const u8 = null;
        for (headers) |h| {
            if (opts.strip_content_length and std.ascii.eqlIgnoreCase(h.name, "content-length")) continue;
            if (opts.sse and std.ascii.eqlIgnoreCase(h.name, "content-type")) continue;
            if (std.ascii.eqlIgnoreCase(h.name, "vary")) {
                handler_vary = h.value;
                continue;
            }
            hlist.append(gpa, .{ .name = h.name, .value = h.value }) catch return error.OutOfMemory;
        }
        if (opts.compress) {
            hlist.append(gpa, .{ .name = "content-encoding", .value = "br" }) catch return error.OutOfMemory;
        }
        if (opts.add_vary) {
            const merged = content_coding.mergeVaryAcceptEncoding(gpa, handler_vary) catch return error.OutOfMemory;
            owned_vary.* = merged;
            hlist.append(gpa, .{ .name = "vary", .value = merged }) catch return error.OutOfMemory;
        } else if (handler_vary) |v| {
            hlist.append(gpa, .{ .name = "vary", .value = v }) catch return error.OutOfMemory;
        }
    }

    fn compressOrAbort(hctx: *HandlerCtx, stream_id: u31, input: []const u8, op: brotli.Operation, out: *std.ArrayList(u8)) response.ResponseError!void {
        const enc = hctx.encoder orelse return error.WriteFailed;
        const self = hctx.conn;
        enc.compress(input, op, out, self.config.gpa) catch {
            // Budget/encode failure after headers committed: RST, never switch coding.
            if (hctx.encoder) |e| {
                e.destroy();
                hctx.encoder = null;
                hctx.compressing = false;
            }
            self.withSession(struct {
                fn go(c: *Connection, sid: u31) void {
                    c.session.applyCommand(.{ .reset_stream = .{ .stream_id = sid, .code = .internal_error } }) catch {};
                    c.processIntents() catch {};
                }
            }.go, stream_id);
            return error.WriteFailed;
        };
    }

    fn sendCb(ctx: *anyopaque, stream_id: u31, status: u16, headers: []const request.Header, body: []const u8) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        if (hctx.terminal.getCause()) |c| return response.causeToError(c);
        if (hctx.terminal.cancel_flag.load(.acquire)) return error.Canceled;

        const prep = self.prepareCompression(hctx, status, headers, body.len, false);
        // Single full-body path parameterized by coding. Pre-commit compress
        // failure falls back to identity (still with Vary when prep asked for it).
        var compress = prep.compress;
        const add_vary = prep.add_vary;
        var body_out = body;
        var compressed_owned: ?[]u8 = null;
        defer if (compressed_owned) |p| self.config.gpa.free(p);

        if (compress) {
            const enc = hctx.encoder.?;
            if (enc.compressAll(body, self.config.gpa)) |compressed| {
                compressed_owned = compressed;
                body_out = compressed;
            } else |_| {
                // Still before headers on the wire: identity + counted fallback.
                if (self.config.compression_pool) |pool| pool.noteIdentityFallback();
                compress = false;
                body_out = body;
            }
            enc.destroy();
            hctx.encoder = null;
            hctx.compressing = false;
        }

        const ticket_pair = self.reserveTicket() catch |err| return err;
        const ticket = ticket_pair[0];
        const slot_i = ticket_pair[1];
        var armed = true;
        defer if (armed) self.tickets.releaseReserved(slot_i);
        {
            try self.lockSession();
            defer self.unlockSession(self.config.io);
            var hlist: std.ArrayList(hpack.HeaderField) = .empty;
            defer hlist.deinit(self.config.gpa);
            var owned_vary: ?[]u8 = null;
            defer if (owned_vary) |v| self.config.gpa.free(v);
            try appendResponseHeaders(self.config.gpa, &hlist, headers, .{
                .compress = compress,
                .add_vary = add_vary,
                .strip_content_length = compress,
            }, &owned_vary);
            // Compressed bodies never carry content-length — HTTP/2 length is framing.
            self.session.applyCommand(.{ .respond_headers = .{
                .stream_id = stream_id,
                .status = status,
                .headers = hlist.items,
                .end_stream = body_out.len == 0,
            } }) catch return error.WriteFailed;
            if (body_out.len > 0) {
                // Materialize the HEADERS intent onto the wire queue BEFORE any
                // DATA can drain. enqueuePending unlocks the session while it
                // waits for stream space, and the actor emits DRR DATA the
                // moment it wakes — so a body crossing outbound_bytes_per_stream
                // used to reach the peer as DATA with its HEADERS never sent,
                // which a browser reports as a protocol error and curl waits
                // out in silence (t-482, the /ui/tasks page).
                self.processIntents() catch return error.WriteFailed;
                self.enqueuePending(stream_id, body_out, true, ticket, slot_i, hctx.terminal) catch |err| return err;
                self.processIntents() catch return error.WriteFailed;
            } else {
                self.next_wire_ticket = ticket;
                self.next_wire_slot = slot_i;
                self.processIntents() catch return error.WriteFailed;
            }
        }
        armed = false;
        // From here until the wait returns, teardown must NOT cancel this
        // handler. The wait is guaranteed to wake on its own — see
        // `HandlerSlot.awaiting_receipt` and `cancelHandler`.
        hctx.slot.awaiting_receipt.store(true, .release);
        defer hctx.slot.awaiting_receipt.store(false, .release);
        try self.waitTicket(slot_i, hctx.terminal);
    }

    /// Streaming response: headers now, body later. The SSE path adds the three
    /// fields that keep an event stream unbuffered end to end —
    /// `text/event-stream`, `no-cache`, and `x-accel-buffering: no` for
    /// intermediaries that would otherwise hold events until a buffer fills.
    ///
    /// `next_wire_ticket` is how the ticket reaches a frame that this function
    /// does not build. The HEADERS frame is produced inside `Session`, so the
    /// ticket is parked on the connection and `processIntents` attaches it to
    /// the next outbound frame it creates. This is safe only because the whole
    /// sequence runs under `session_mu` on one task.
    fn startCb(ctx: *anyopaque, stream_id: u31, status: u16, headers: []const request.Header, sse: bool) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        if (hctx.terminal.getCause()) |c| return response.causeToError(c);
        if (hctx.terminal.cancel_flag.load(.acquire)) return error.Canceled;
        const prep = self.prepareCompression(hctx, status, headers, null, sse);
        const ticket_pair = self.reserveTicket() catch |err| {
            if (hctx.encoder) |enc| {
                enc.destroy();
                hctx.encoder = null;
                hctx.compressing = false;
            }
            return err;
        };
        const ticket = ticket_pair[0];
        const slot_i = ticket_pair[1];
        {
            errdefer self.tickets.releaseReserved(slot_i);
            try self.lockSession();
            defer self.unlockSession(self.config.io);
            var hlist: std.ArrayList(hpack.HeaderField) = .empty;
            defer hlist.deinit(self.config.gpa);
            var owned_vary: ?[]u8 = null;
            defer if (owned_vary) |v| self.config.gpa.free(v);
            try appendResponseHeaders(self.config.gpa, &hlist, headers, .{
                .sse = sse,
                .compress = prep.compress,
                .add_vary = prep.add_vary,
                .strip_content_length = true, // streaming never sends content-length
            }, &owned_vary);
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

    /// Streaming body write.
    ///
    /// A ticket is taken only when the caller needs a receipt — a flush or an
    /// end-of-stream. A plain write returns as soon as the bytes are queued, so
    /// a handler that writes many small chunks pays one round trip per flush
    /// and not one per chunk. Backpressure still applies through
    /// `enqueuePending`, so this is not an unbounded fire-and-forget.
    fn writeCb(ctx: *anyopaque, stream_id: u31, bytes: []const u8, end: bool, flush: bool) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        if (hctx.terminal.getCause()) |c| return response.causeToError(c);
        if (hctx.terminal.cancel_flag.load(.acquire)) return error.Canceled;
        if (!flush and !end and bytes.len == 0) return;

        var wire_bytes = bytes;
        var owned: ?[]u8 = null;
        defer if (owned) |p| self.config.gpa.free(p);

        if (hctx.compressing) {
            var out: std.ArrayList(u8) = .empty;
            errdefer out.deinit(self.config.gpa);
            const op: brotli.Operation = if (end) .finish else if (flush) .flush else .process;
            try compressOrAbort(hctx, stream_id, bytes, op, &out);
            if (end and hctx.encoder != null) {
                // finish already done in compress; release encoder
                hctx.encoder.?.destroy();
                hctx.encoder = null;
                hctx.compressing = false;
            }
            owned = try out.toOwnedSlice(self.config.gpa);
            wire_bytes = owned.?;
            // A flush/end with no compressed output still needs the receipt path.
            if (!flush and !end and wire_bytes.len == 0) return;
        }

        const need_wait = flush or end;
        var ticket: u64 = 0;
        var slot_i: u32 = 0;
        if (need_wait) {
            const ticket_pair = self.reserveTicket() catch |err| return err;
            ticket = ticket_pair[0];
            slot_i = ticket_pair[1];
        }

        // Sampled phase trace. `traced` is decided once, so every stamp below
        // belongs to the same event.
        // One sample in flight per connection. Claiming the slot with a CAS
        // rather than a store matters: a plain store lets a later sample
        // overwrite an earlier one, and the overwritten event is then dropped
        // at the end — which silently selects for events that finished before
        // the next sample started, i.e. the fast ones. Refusing to start a
        // second overlapping sample removes that bias.
        var traced = trace.enabled and need_wait and
            (trace.writes.fetchAdd(1, .monotonic) % trace.sample_every == 0);
        if (traced) {
            traced = self.trace_ticket.cmpxchgStrong(0, ticket, .acq_rel, .monotonic) == null;
            if (!traced) _ = trace.skipped.fetchAdd(1, .monotonic);
        }
        var t_before: u64 = 0;
        var t_acquired: u64 = 0;
        var t_unlocked: u64 = 0;
        if (traced) {
            self.trace_ack_ns.store(0, .release);
            t_before = nowNs(self.config.io);
        }
        {
            errdefer if (need_wait) self.tickets.releaseReserved(slot_i);
            try self.lockSession();
            if (traced) t_acquired = nowNs(self.config.io);
            defer self.unlockSession(self.config.io);
            // The handler enqueues and leaves. It does NOT drain.
            //
            // `enqueuePending` already sets `sched_refilled` and wakes the
            // actor, and the actor drains under this same lock every turn, so
            // the bytes still reach the wire — one actor turn later instead of
            // inside this critical section.
            //
            // Draining here made every handler the emitter for its own event:
            // 200 streams took 200 separate acquisitions, each framing,
            // debiting, encrypting and pushing one frame. Measured, that
            // critical section is 20.3us wide and 95% of an event's life was
            // spent queueing for it, which capped one connection at 1/20.3us,
            // about 49k events/s. The actor drains every stream that arrived
            // while it was busy in ONE acquisition, so the width stops being
            // paid per event.
            //
            // It also restores the ownership rule this module states at the
            // top: only the actor emits.
            self.enqueuePending(stream_id, wire_bytes, end, if (need_wait) ticket else 0, if (need_wait) slot_i else 0, hctx.terminal) catch |err| return err;
        }
        if (traced) t_unlocked = nowNs(self.config.io);
        if (need_wait) {
            hctx.slot.awaiting_receipt.store(true, .release);
            defer hctx.slot.awaiting_receipt.store(false, .release);
            try self.waitTicket(slot_i, hctx.terminal);
        }
        if (traced) {
            const t_done = nowNs(self.config.io);
            // The AckDrainer stamps this when it completes THIS ticket. Zero
            // means the receipt never went through that path, so the sample is
            // dropped rather than folded in with a bogus split.
            const t_ack = self.trace_ack_ns.load(.acquire);
            self.trace_ticket.store(0, .release);
            if (t_ack >= t_unlocked and t_done >= t_ack) {
                const block = t_acquired - t_before;
                const hold = t_unlocked - t_acquired;
                const ack = t_ack - t_unlocked;
                const res = t_done - t_ack;
                _ = trace.samples.fetchAdd(1, .monotonic);
                _ = trace.block_ns.fetchAdd(block, .monotonic);
                _ = trace.hold_ns.fetchAdd(hold, .monotonic);
                _ = trace.ack_ns.fetchAdd(ack, .monotonic);
                _ = trace.resume_ns.fetchAdd(res, .monotonic);
                trace.bumpMax(&trace.block_max, block);
                trace.bumpMax(&trace.hold_max, hold);
                trace.bumpMax(&trace.ack_max, ack);
                trace.bumpMax(&trace.resume_max, res);
            }
        }
    }

    /// Wait until everything written so far has reached the wire.
    ///
    /// Two cases, and the distinction is what makes a flush mean something:
    /// - Bytes are still pending. The ticket is attached to the pending entry
    ///   with `flush_remain` set to the current length, so the scheduler
    ///   completes it only after that many bytes have actually drained.
    /// - Nothing is pending. A bare flush barrier goes through the write queue
    ///   instead. The pump completes it in FIFO order, so it still proves that
    ///   every earlier write was written.
    fn flushCb(ctx: *anyopaque, stream_id: u31) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        if (hctx.terminal.getCause()) |c| return response.causeToError(c);

        // Emit brotli flush output before the wire barrier so I4 holds: after
        // Body.flush returns, a streaming decoder can read every event so far.
        if (hctx.compressing) {
            var out: std.ArrayList(u8) = .empty;
            defer out.deinit(self.config.gpa);
            try compressOrAbort(hctx, stream_id, &.{}, .flush, &out);
            if (out.items.len > 0) {
                const ticket_pair = self.reserveTicket() catch |err| return err;
                const ticket = ticket_pair[0];
                const slot_i = ticket_pair[1];
                {
                    errdefer self.tickets.releaseReserved(slot_i);
                    try self.lockSession();
                    defer self.unlockSession(self.config.io);
                    self.enqueuePending(stream_id, out.items, false, ticket, slot_i, hctx.terminal) catch |err| return err;
                    self.processIntents() catch return error.WriteFailed;
                }
                hctx.slot.awaiting_receipt.store(true, .release);
                defer hctx.slot.awaiting_receipt.store(false, .release);
                try self.waitTicket(slot_i, hctx.terminal);
            }
        }

        const ticket_pair = self.reserveTicket() catch |err| return err;
        const ticket = ticket_pair[0];
        const slot_i = ticket_pair[1];
        {
            errdefer self.tickets.releaseReserved(slot_i);
            try self.lockSession();
            defer self.unlockSession(self.config.io);
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

    /// Kill one stream without harming the connection. A handler uses this when
    /// it has already committed a response and then discovers it cannot
    /// complete it. A RST_STREAM tells the peer the response is broken, which
    /// is honest; a silent truncation would look like a complete body.
    fn abortCb(ctx: *anyopaque, stream_id: u31) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        try self.lockSession();
        defer self.unlockSession(self.config.io);
        self.session.applyCommand(.{ .reset_stream = .{ .stream_id = stream_id, .code = .cancel } }) catch return error.WriteFailed;
        self.processIntents() catch return error.WriteFailed;
    }
};
