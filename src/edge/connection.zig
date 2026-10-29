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
//! | actor (`run`)  | 1     | `Session`, `FairScheduler`, slots, tickets    |
//! |                |       | WritePump completions, byte releases          |
//! |                |       | TLS: the zio.CompletionQueue driver. Sole     |
//! |                |       | encrypt/decrypt owner, sole socket reader     |
//! |                |       | (raw NetRecv) and socket writer (NetSend).    |
//! | `ReadPump`     | 1     | h2c: socket READ. TLS: not spawned            |
//! | `WritePump`    | 1     | h2c: socket WRITE. TLS: not spawned           |
//! | handler        | 0..N  | arena + request/response. Task handlers spawn; |
//! |                |       | complete oneshots run on the actor, still     |
//! |                |       | emit through the actor. SSE always spawns.    |
//! | reaper worker  | pool  | canceling a handler future (server-wide)      |
//!
//! The edge takes a DIRECT zio dependency, openly: the actor parks in one
//! `zio.select`, its inbound/ack/completion channels are zio channels, and
//! TLS adds the CompletionQueue driver. Nothing in the edge is std.Io-pure
//! any more; a connection requires the zio runtime behind `std.Io`.
//!
//! Rules that follow from the table, all of which have been violated at least
//! once:
//!
//! - A pump NEVER touches `Connection` state. A pump exchanges `WireChunk` and
//!   `WriteCompletion` messages that carry integers only. That is why the pumps
//!   need no lock.
//! - Only the actor emits. Every production byte reaches the writer through
//!   `drainEmit` -> `FairScheduler` sink -> `queueWire`. The
//!   `test_queue_wire_bypass` counter is the mutation canary for that rule and
//!   must stay at zero. On TLS, `queueWire` hands a plaintext chunk to the
//!   actor-driven `Pump.writeChunks`; encrypting it is the actor's flush.
//! - One task drives the TLS cipher. That is the actor, not a second pump.
//!   Sharing the cipher state with a second task is a data race.
//!
//! # Lock discipline
//!
//! `session_mu` is the ONLY mutex. It covers `Session` and the `FairScheduler`
//! together, because a frame is built from session state and debited against
//! session flow control. TLS cipher work now runs on the actor too, but it
//! is not in this domain: HTTP/2 writes plaintext under the lock, and
//! encryption happens on the actor without taking a second mutex. The
//! cipher state has one owner (the actor task).
//!
//! There are three places that release the lock inside an operation:
//! `waitForStreamSpace`, `parkForBudget` and `waitForAckRoom`. They must,
//! because the capacity they wait for can only be freed by the actor, which
//! needs the same lock. Each always reacquires UNCANCELABLE before it returns,
//! so the caller's `defer unlock` stays balanced on every path, including
//! cancellation.
//!
//! Handler code never holds the lock. `Response` callbacks take it and do
//! their work. A non-end `wait` parks inside `waitForStreamSpace` (the lock is
//! dropped and reacquired uncancelable). End-of-stream and `Body.flush()`
//! release the lock and then wait on a ticket.
//!
//! # The wake protocol
//!
//! The actor is event-driven and parks with no timer when nothing is armed, so
//! a lost wakeup is a hang and not a delay. The park is one `zio.select` over
//! sources that all hold PERSISTENT evidence, so there is no reset and no
//! recheck list:
//!
//! 1. Read chunks, write acks, and handler completions are zio channels; a
//!    buffered item survives until the actor takes it, and the select's fast
//!    path observes it before parking.
//! 2. Every other producer sets its flag and then RINGS the doorbell (`ring`,
//!    a capacity-1 channel; a full doorbell already means a ring is pending).
//!    The ring is the producer's LAST act, and `ring` is the only writer of
//!    that channel. A producer that neither sends on a selected channel nor
//!    rings is a hang — which is why no such producer may exist.
//!
//!    A handler's task does not ring by site. It changes shared state only
//!    under `session_mu`, and `unlockSession` rings when the releasing task is
//!    not the actor, as its last act. Only two producers ring by hand: the
//!    actor for work it made after its turn's check (`tryDispatch`, the inline
//!    grant in `grantBudgetLocked`), because the unlock does not ring for it.
//!
//! # Byte accounting: two kinds, released at different moments
//!
//! Outbound bytes are held in two disjoint pools, and the split is deliberate:
//!
//! - PENDING bytes sit in a `FairScheduler` slab and are released when the
//!   scheduler sink accepts the framed output.
//! - WIRE bytes are the framed frame and are released only when the
//!   actor applies the write ack (WritePump completion on h2c; local
//!   `ack_apply` on TLS).
//!
//! One counter cannot express both, because a byte is briefly present as
//! pending body AND as framed wire. `deinit` asserts that the two parts sum to
//! the total and that both reach zero, which is the leak check.
//!
//! # Placement
//!
//! Every task a connection spawns for itself (h2c pumps, task handlers, the
//! HTTP/1.1 handler job) is spawned with `connPlacement()`. Only server-wide
//! tasks (accept loops, `connEntry`, reaper workers) use `.auto`. Without
//! task migration, `.auto` homes each new task round-robin, so one
//! connection's tasks scatter over executors by spawn-time luck and a bad
//! draw lasts for the connection's life. With migration there is no fixed
//! home to choose, and zio refuses any placement but `.auto`.
const std = @import("std");
const test_hook = @import("test_hook.zig");
// Direct zio dependency: the actor parks in one `zio.select`; TLS adds
// the CompletionQueue. The h2c pumps stay std.Io-pure.
const zio = @import("zio");
const session_mod = @import("../core/session.zig");
const hpack = @import("../core/hpack.zig");
const limits_mod = @import("../core/limits.zig");
const wire_const = @import("../core/wire_const.zig");
const boot_plan = @import("../core/boot_plan.zig");
const conn_slot = @import("conn_slot.zig");
const builtin = @import("builtin");
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
const emit_batch = @import("emit_batch.zig");
const fair_scheduler = @import("fair_scheduler.zig");
const io_queue = @import("io_queue.zig");
pub const Balancer = @import("balancer.zig").Balancer;
const slab_pool = @import("slab_pool.zig");
const request_pool = @import("request_pool.zig");
const Touched = @import("touched.zig").Touched;
const flow = @import("../core/flow.zig");

pub const zio_scheduling = @import("build_options").zio_scheduling;

/// `.local` is the connection's own executor, because every per-connection
/// spawn runs on a task of that connection. zio fails `.local` with
/// `error.InvalidPlacement` under work_stealing, so this must be decided at
/// comptime.
pub const conn_placement: zio.Placement = if (zio_scheduling == .work_stealing) .auto else .local;

/// Bench and test override of `conn_placement`, so one pinned binary can run
/// both arms of the placement A/B. Set it before the server starts.
pub var placement_override: ?zio.Placement = null;

pub fn connPlacement() zio.Placement {
    return placement_override orelse conn_placement;
}

/// The placement check needs tasks that never change thread, so it is off
/// under work_stealing. Off in release unless `-Dobserve=true`.
pub const placement_check = test_observe and zio_scheduling != .work_stealing;
/// Per-connection task starts that compared their thread with the actor's.
/// Counted so a zero mismatch count can be told apart from a check that
/// never ran.
pub var test_placement_checks: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Diagnosis counters (always on, one atomic add per task handler): task
/// handlers that reached the spawn, and spawns zio accepted. Compared with a
/// handler's own entry count they tell "never spawned" from "spawned, never
/// ran" (t-2655).
pub var diag_task_spawn_attempts: std.atomic.Value(u64) = .init(0);
pub var diag_task_spawned: std.atomic.Value(u64) = .init(0);
/// Per-connection task starts that ran on a thread other than the actor's.
pub var test_placement_mismatches: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};

/// Called first by every per-connection task. `actor_thread` is the thread
/// the connection's actor recorded when it started.
pub fn notePlacement(actor_thread: std.Thread.Id) void {
    if (comptime !placement_check) return;
    _ = test_placement_checks.fetchAdd(1, .monotonic);
    if (std.Thread.getCurrentId() != actor_thread) {
        _ = test_placement_mismatches.fetchAdd(1, .monotonic);
    }
}

/// Placement probe (observe builds only; compiled out otherwise): counts, per
/// OS thread, how often each kind of server task ran a unit of work there.
/// Threads are keyed by kernel tid in first-seen slots, so a row can be
/// joined with /proc/<pid>/task/<tid>/stat. Linux only: other targets count
/// nothing.
pub const ProbeKind = enum(u8) { accept, conn_entry, actor_start, actor_turn, handler_start, handler_iter, reaper_job, tls_recv_submit, tls_send_submit };
pub const probe_slots = 8;
pub var probe_tids: [probe_slots]std.atomic.Value(i32) = std.mem.zeroes([probe_slots]std.atomic.Value(i32));
pub var probe_counts: [@typeInfo(ProbeKind).@"enum".field_names.len][probe_slots]std.atomic.Value(u64) =
    std.mem.zeroes([@typeInfo(ProbeKind).@"enum".field_names.len][probe_slots]std.atomic.Value(u64));
/// More distinct threads than slots: those notes are dropped, and counted
/// here, so a full table cannot pass as a complete one.
pub var probe_overflow: std.atomic.Value(u64) = .init(0);

pub fn probeTid() i32 {
    if (comptime @import("builtin").os.tag != .linux) return 0;
    return @intCast(std.os.linux.gettid());
}

pub fn probeNote(kind: ProbeKind) void {
    if (comptime !placement_check or @import("builtin").os.tag != .linux) return;
    const tid = probeTid();
    for (&probe_tids, 0..) |*slot, i| {
        var cur = slot.load(.acquire);
        if (cur == 0) cur = slot.cmpxchgStrong(0, tid, .acq_rel, .acquire) orelse tid;
        if (cur == tid) {
            _ = probe_counts[@intFromEnum(kind)][i].fetchAdd(1, .monotonic);
            return;
        }
    }
    _ = probe_overflow.fetchAdd(1, .monotonic);
}

/// Probe-only forcing of the handler spawn, to validate the probe: `same`
/// spawns SSE handlers on the actor's executor, `other` on the other one of
/// a 2-executor runtime. Executor 0 is the process main thread (zio's main
/// executor), so the actor's executor is 0 exactly when its tid is the pid.
pub const ProbeForce = enum { none, same, other };

/// Probe-only placement of whole connections (connEntry): `same` puts every
/// accepted connection on executor 0; `split` alternates executors 0, 1, 0,
/// ... in accept order. `none` keeps the server's normal `.auto` spawn.
pub const ProbeConnForce = enum { none, same, split };
pub var probe_conn_force: ProbeConnForce = .none;
var probe_conn_next: std.atomic.Value(u32) = .init(0);

pub fn probeConnPlacement() ?zio.Placement {
    if (comptime !placement_check) return null;
    return switch (probe_conn_force) {
        .none => null,
        .same => .{ .executor = 0 },
        .split => .{ .executor = @intCast(probe_conn_next.fetchAdd(1, .monotonic) % 2) },
    };
}
pub var probe_handler_force: ProbeForce = .none;

fn probeHandlerPlacement() zio.Placement {
    if (comptime !placement_check or @import("builtin").os.tag != .linux) return connPlacement();
    const actor_exec: zio.ExecutorId = if (probeTid() == std.os.linux.getpid()) 0 else 1;
    return switch (probe_handler_force) {
        .none => connPlacement(),
        .same => .{ .executor = actor_exec },
        .other => .{ .executor = 1 - actor_exec },
    };
}

/// Test-only: when true, task-handler spawn fails closed with REFUSED_STREAM.
/// Complete handlers never spawn, so this flag does not touch them. Do not
/// run the task handler on the actor as a fallback — that parks ingest.
pub var test_force_spawn_fail: test_hook.Hook(bool) = if (test_hook.on) false else {};
/// Test-only: when true, actor skips draining completion_ch (sticky until cleared).
pub var test_hold_completion_drain: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
/// Test-only: when true, the actor takes no write completion off
/// `write_ack_ch` (sticky until cleared), so a test can make the one receiver
/// slower than the drains that feed the pump. A real actor is that slow when
/// one drain turn pushes many chunks. After clearing it, wake the actor the
/// way a peer would (any inbound frame). Observe builds only.
pub var test_hold_ack_receive: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
var hold_dummy_ack_buf: [1]wire_pump.WriteCompletion = undefined;
var hold_dummy_ack_ch: zio.Channel(wire_pump.WriteCompletion) = .init(&hold_dummy_ack_buf);
/// Test-only (observe builds): h2c drains held back because `write_ack_ch`
/// had no room for another pop.
pub var test_ack_room_pauses: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): handler flush barriers that parked for
/// `write_ack_ch` room (`waitForAckRoom`).
pub var test_ack_room_waits: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): the fewest free stash slots a TLS bare flush
/// barrier left after its push (`pushBareBarrier`). Starts at the maximum.
pub var test_tls_barrier_min_room: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(std.math.maxInt(usize)) else {};
/// Test-only (observe builds): handlers that parked for connection outbound
/// budget (`parkForBudget`), and the budget grants made. `test_budget_grant_log`
/// keeps the stream id of each grant, newest at `grants % len`, so a test can
/// read the order the queue served its waiters in.
pub var test_budget_waits: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_budget_grants: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_budget_grant_log: test_hook.Hook([64]std.atomic.Value(u32)) = if (test_hook.on) @splat(.init(0)) else {};
/// Test-only (observe builds): times `runPendingInline` held a complete job
/// back for lack of connection budget.
pub var test_inline_budget_gated: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): doorbell rings made by a budget grant to an
/// inline entry. The grant of the entry being admitted in the same call rings
/// nothing.
pub var test_budget_doorbell_rings: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): terminal control frames the scheduler could not
/// queue (`ctrl_terminal_full`, the reset burst that ends a connection). A test
/// that relies on the terminal queue not overflowing asserts it did not move.
pub var test_ctrl_terminal_full: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only: the most frames the terminal ring has held at once, over every
/// connection since the test last stored 0. A bound test reads it to prove the
/// stall really queued the frames it means to, and that none went past T.
/// Counted in Debug and `-Dobserve=true`.
pub var test_terminal_ring_high: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): RST_STREAM frames the scheduler took while its
/// control pause (`pause_control`) was on, so they waited in the terminal
/// ring. A stall test reads it to prove its RSTs were queued during the
/// stall, not after it.
pub var test_rst_queued_paused: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): how closing phases ended in `closeFlushed`:
/// the writer failed; the deadline passed with output still not written
/// (`blocked`) or with all of it in the kernel and the peer not closing
/// (`flushed`); the peer closed after a lingering close (h2c: on TLS a peer
/// EOF ends the turn before `closeFlushed` runs).
pub var test_closing_ended_writer_failed: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_closing_ended_deadline_blocked: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_closing_ended_deadline_flushed: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_closing_ended_peer_eof: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): closing phases whose output was all flushed
/// and whose send side was then shut, to wait for the peer (the lingering
/// close in `closeFlushed`).
pub var test_closing_write_shut: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): HTTP/2 connections whose socket was closed,
/// and when the last one was (`zio.Timestamp` monotonic ns, the clock the
/// tests read). Written at the close itself, so a test can tell when the
/// server let go of the peer, whatever the closing phase decided.
pub var test_socket_closes: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): TLS closing phases that staged close_notify.
pub var test_close_notify_staged: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): peer EOFs the actor took as an event
/// (`onPeerEof`), before or during the closing phase.
pub var test_peer_eofs: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_socket_close_ns: test_hook.Hook(std.atomic.Value(u64)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): server RST_STREAMs dropped from the terminal
/// ring because the peer reset the same stream first (`retireTerminalRst`).
pub var test_rst_retired: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only: ORDINARY control frames (HEADERS and the like) the scheduler
/// refused, by the pool (`PoolFull`) and by the ring (`QueueFull`). A handler
/// whose response is refused sees `WriteFailed`. Counted in Debug and
/// `-Dobserve=true`.
pub var test_ctrl_ordinary_pool_full: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_ctrl_ordinary_queue_full: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): actors that are inside their park select right
/// now. Raised immediately before `zio.select` in `waitForActivity` and lowered
/// right after it returns. A test whose handler must act while the actor is
/// asleep waits for this to be above zero (a sleep proves nothing about the
/// actor's turn), with a deadline that fails the test. One connection per test.
pub var test_actors_in_select: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): where in `unlockSession` a hook runs, between its
/// two counters and around its ring, so a unit test can put the actor's
/// snapshot at either side of the ring without a second thread.
pub const ReleasePoint = enum { before_ring, after_ring, park_after_check };
pub var test_release_hook: test_hook.Hook(?*const fn (*Connection, ReleasePoint) void) = if (test_hook.on) null else {};
/// Test-only (observe builds): while true, the TLS driver holds the chunks that
/// sit in the stash (pushed by a drain, not yet taken for writing). Beside
/// `wire_pump.test_hold_writes`, which holds a batch the driver already took,
/// it lets a test see a chunk in each place. `test_hold_stash_entered` counts
/// the times the driver stopped there.
pub var test_hold_stash: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
pub var test_hold_stash_entered: test_hook.Hook(std.atomic.Value(u64)) = if (test_hook.on) .init(0) else {};
/// Test-only: lets the full-queue push trap in `pushWriteChunk` be skipped, to
/// test what every other build does there.
pub var test_allow_full_queue_push: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
/// Test-only (observe builds): h2c pushes refused because the write queue was
/// full. Zero in a correct build (`pushWriteChunk`).
pub var test_queue_push_refusals: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): pushes `pushWriteChunk` refused because the
/// channel had no room, each one a connection failed closed. Zero in a
/// correct build.
pub var test_ack_bound_refusals: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only: while true, `drainEmit` emits nothing, so frames a handler
/// queued stay in the scheduler (a reset can then arrive before them).
/// Compiled out unless `test_observe`.
pub var test_hold_emit: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
/// Test-only: stash a complete-batch write ack on the actor instead of
/// applying it. Separate from `test_hold_completion_drain`, which parks
/// `completion_ch`. Must not park the actor on the release event — that
/// would block ingest of the next HEADERS, which is the property the
/// lifecycle hold test checks.
pub var test_hold_complete_receipt_ack: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
/// Test-only: true while a complete-batch write ack is stashed unapplied.
pub var test_complete_receipt_ack_held: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
/// Test-only: wakes a parked actor that has a live complete-batch stash.
/// A zio.Event so the actor's select can wait on it directly.
pub var test_release_complete_receipt_ack: test_hook.Hook(zio.Event) = if (test_hook.on) .init else {};
/// Never set / never sent-to: the select branches point here when their
/// real source is absent or gated, so the select keeps one comptime shape.
var no_shutdown_event: zio.Event = .init;
var no_ack_hold_event: zio.Event = .init;
var hold_dummy_completion_buf: [1]u31 = undefined;
var hold_dummy_completion_ch: zio.Channel(u31) = .init(&hold_dummy_completion_buf);
/// Test-only: when true, `borrowHeaderListMem` finds no free lease, so the
/// next HEADERS takes Session's refuse-without-memory path.
pub var test_no_request_lease: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
/// Test-only: when true, the allocations that build a handler job fail as if
/// the request's memory had run out, so dispatch takes its refusal path.
pub var test_fail_dispatch_alloc: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
/// Test-only: delay write pump by N ms.
pub var test_write_delay_ms: test_hook.Hook(u64) = if (test_hook.on) 0 else {};
/// Test-only: fail write pump after N successful writes (0 = never).
pub var test_write_fail_after: test_hook.Hook(u64) = if (test_hook.on) 0 else {};
/// Hot test counters. On in Debug (the gate builds) and when `-Dobserve=true`.
/// Off in ReleaseFast examples so the oneshot bench does not pay process-global
/// RMWs. Mutation canaries stay in gate builds; they are not the price of the
/// connection split.
pub const test_observe = @import("build_options").observe;

/// Test-only: handlers running across all connections. Published by the mutation
/// itself, not by the actor loop: an idle actor parks until the next wakeup, so a
/// value refreshed only per iteration stays stale for as long as the connection is
/// quiet and reports handlers that have already exited.
pub var test_observed_live_handlers: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only: handler slots held across all connections. Released strictly later
/// than `test_observed_live_handlers` (the actor releases the slot after joining
/// the handler), so a drain gate must require both to reach zero.
pub var test_observed_slots_in_use: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only: FairScheduler emits observed by actor (mutation canary).
pub var test_observed_sched_emits: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only: true once actor handled writer_failed terminal teardown.
pub var test_observed_writer_fail_handled: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
/// Test-only: complete handlers run on the actor (no spawn).
pub var test_observed_inline_jobs: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Times `runPendingInline` stopped a batch because no ordinary control entry was free.
pub var test_observed_inline_ctrl_full: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only: queueWire calls not from FairScheduler sink (must stay 0).
pub var test_queue_wire_bypass: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Counts the first-frame checks `queueWire` ran: one per connection that
/// wrote. A test that expects N connections to be checked proves the check
/// is wired in; deleting the call leaves every other test green.
pub var test_preface_checks: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Counts connections that started with a non-empty `tls_leftover`. The TLS
/// preface tests assert every tested connection took that path, because a
/// server read that returned only the Finished record would skip it.
pub var test_tls_leftover_connections: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Counts the times `error.Canceled` reached `Connection.run`, so a test can
/// tell a cancel that left as `Canceled` from one a `catch` turned into
/// something else. The connection task's result is not visible to a test.
pub var test_run_canceled: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only (observe builds): actor teardowns begun (`teardownExhaustive`). A
/// test whose handler stays alive after the connection failed reads this to
/// prove the ACTOR got to its teardown, which a handler's own task cannot fake.
pub var test_actor_teardowns: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Counts the times `runPendingInline` saw `error.Canceled` from its drain. With
/// `test_run_canceled` it lets a test tell WHICH path carried a cancel: a cancel
/// parked elsewhere (the main-loop drain, a select) reaches `run` without
/// moving this one.
pub var test_inline_drain_canceled: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Counts the pops an ending h2c connection held back because the write pool
/// or the write queue had no room (see `h2cPauseFor`). A test that expects the
/// hold-back proves it ran; removing the hooks leaves the other tests green.
pub var test_closing_emit_deferred: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only: true while FairScheduler sink is executing queueWire.
threadlocal var test_in_sched_sink: test_hook.Hook(bool) = if (test_hook.on) false else {};
/// Test-only: force queueWire/send fail after N successful accounted sends (0=never).
pub var test_force_wire_fail_after: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_wire_sends: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only: last handler ResponseError taxonomy (0=none/success, 1=WriteFailed, 2=ConnectionClosed,
/// 3=PeerReset, 4=SlowConsumer, 5=other).
pub var test_last_handler_err: test_hook.Hook(std.atomic.Value(u8)) = if (test_hook.on) .init(0) else {};
pub var test_last_peer_reset_code: test_hook.Hook(std.atomic.Value(u32)) = if (test_hook.on) .init(0) else {};
/// Test-only: stream_id currently blocked in waitForStreamSpace (0 = none).
pub var test_waiting_for_space: test_hook.Hook(std.atomic.Value(u32)) = if (test_hook.on) .init(0) else {};
/// Test-only: actor-owned handler-deadline heap occupancy (one connection at a time).
pub var test_deadline_waits: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Test-only: Connection finished boot allocations (pools, sched slabs, session maps).
pub var test_boot_ready: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};



/// Per-task binding: the current task is inside `shutdownHandlers`.
///
/// A connection-wide bool cannot answer that. A handler in
/// `waitForStreamSpace` drops `session_mu` and reacquires it; that reacquire
/// is not teardown taking the lock. `TaskLocal` is keyed by this instance and
/// stored in caller-owned `Node`s, so `get` is the current task only and a
/// stray or double `clear` asserts instead of corrupting the chain
/// (`zig-pkg/zio-*/src/task.zig` `TaskLocal.set`/`clear`).
var shutdown_sweep: zio.TaskLocal(void) = .{};

/// Bound, for the whole of `runBody`, to the connection whose actor runs on this
/// task. `get() == self` tells the actor from a handler's task (`pushWriteChunk`
/// rings only for the latter). Same discipline as `shutdown_sweep`.
var actor_marker: zio.TaskLocal(*Connection) = .{};
/// Set on a task handler's task, from the start of `runHandlerJob` to its end
/// (observe builds only). A request's blocks go back to the pool from the
/// actor, after the join; `releaseLease` asserts it is not called from inside
/// a handler's task. A thread-local would be wrong here: a handler yields and
/// can resume on another thread.
var handler_job_marker: zio.TaskLocal(void) = .{};
/// Test-only gate that parks the actor immediately before its event-driven wait.
pub var test_hold_before_actor_wait: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(false) else {};
pub var test_actor_waiting: test_hook.Hook(std.Io.Event) = if (test_hook.on) .unset else {};
pub var test_release_actor_wait: test_hook.Hook(std.Io.Event) = if (test_hook.on) .unset else {};
/// Test-only fallback timer raced against actor events by the wakeup canary.
pub var test_polling_canary_tick_ns: test_hook.Hook(std.atomic.Value(u64)) = if (test_hook.on) .init(0) else {};
/// Bench `--diag`: print one line when the actor parks with scheduler or TLS
/// work still sitting in userspace. Off unless the benchmark server sets it.
pub var diag_park: bool = false;

/// Bench `--close-reason`: one `CLOSE_REASON` line for each connection the
/// server ends, printed at the FIRST site that ends it, with the error that
/// site saw and the outbound budget at that moment. Session protocol errors
/// print as `site=session_goaway` with the GOAWAY code. A connection whose
/// handlers were refused outbound budget also prints one `STREAM_BUDGET`
/// line at teardown, ended or not. A peer close usually prints as
/// `by=write_failed` (`ack_fail_all`): the next socket write fails, and a
/// real write error looks the same from here. Off by default; unlike
/// `--diag` it prints nothing per stream, so it can stay on under a rate run.
///
///   CLOSE_REASON conn=<id> mode=<h2c|tls> by=<server|write_failed> site=<site> err=<error|-> goaway=<code|->
///     held=<b> pending=<b> wire=<b> lim=<b> stream_budget_refusals=<n>
///     wire_refused=<n|-> [at_refusal held=<b> pending=<b> wire=<b> deferred_pending=<b>]
///
/// `wire_refused` is the size of a sink handoff that `sendAccountedWire`
/// could not reserve wire bytes for, and `at_refusal` the budget then:
/// `deferred_pending` is the pending bytes of the frame being handed off,
/// which stay charged until the sink accepts it.
pub var close_reason: bool = false;

/// t-1002 probe: trace the connection-close path of every connection under a
/// full h2spec run. Off unless the conformance server sets it (from
/// `STARH2_CLOSE_PROBE=1`). Prints go through `diagRawPrint`, so the file
/// order is the time order.
pub var close_probe: bool = false;
/// Gauge: connections parked RIGHT NOW in `waitForActivity` with a protocol
/// deadline armed. Every close-probe line prints it, so a slow close
/// correlates (or does not) with parked-on-deadline neighbours. Only
/// maintained while `close_probe` is set.
pub var close_probe_parked: std.atomic.Value(u32) = .init(0);

/// Diag-only peer port, so a probe line maps to the exact h2spec case: the
/// harness prints `using source address 127.0.0.1:PORT` per case. Returns 0
/// when the socket cannot answer; 0 never matches a real ephemeral port.
fn closeProbePeerPort(handle: std.posix.socket_t) u16 {
    var addr: std.posix.sockaddr = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    std.posix.getpeername(handle, &addr, &len) catch return 0;
    if (addr.family != std.posix.AF.INET) return 0;
    const in: *const std.posix.sockaddr.in = @ptrCast(@alignCast(&addr));
    return std.mem.bigToNative(u16, in.port);
}

const mapped_v4_prefix = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff };

/// Connection peer without a port. IPv4-mapped IPv6 becomes IPv4.
///
/// Uses libc getpeername, not std.posix.getpeername. posix maps EINVAL to
/// `unreachable`, and a lifecycle test socket can return that.
pub fn peerFromHandle(handle: std.posix.socket_t) request.Peer {
    var storage: std.posix.sockaddr.storage = std.mem.zeroes(std.posix.sockaddr.storage);
    if (@hasField(std.posix.sockaddr.storage, "len")) {
        storage.len = @sizeOf(std.posix.sockaddr.storage);
    }
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.storage);
    if (std.c.getpeername(handle, @ptrCast(&storage), &len) != 0) return .unknown;
    switch (storage.family) {
        std.posix.AF.INET => {
            const in: *const std.posix.sockaddr.in = @ptrCast(@alignCast(&storage));
            return .{ .ip4 = std.mem.toBytes(in.addr) };
        },
        std.posix.AF.INET6 => {
            const in6: *const std.posix.sockaddr.in6 = @ptrCast(@alignCast(&storage));
            if (std.mem.eql(u8, in6.addr[0..12], &mapped_v4_prefix)) {
                return .{ .ip4 = in6.addr[12..16].* };
            }
            return .{ .ip6 = in6.addr };
        },
        else => return .unknown,
    }
}

/// The print body for `session.close_probe_fn`. Lives here because core is
/// clock-free and I/O-free by contract; the binary wires the two together.
pub fn closeProbeSessionPrint(ev: session_mod.CloseProbeEvent) void {
    // `ErrorCode` is non-exhaustive, so print the wire integer, not a tag name.
    diagRawPrint("t1002 sess={x} {s} code={d} sid={d} parked={d}\n", .{
        ev.session,
        @tagName(ev.site),
        @intFromEnum(ev.code),
        ev.stream_id,
        close_probe_parked.load(.acquire),
    });
}

/// Diag hooks the bench server installs (they need zio, which this module
/// must not import). Handle = opaque task pointer; state byte = packed
/// (tag:u3 | awaken:u1) from zio's AnyTask.
pub var diag_task_handle_fn: ?*const fn () usize = null;
pub var diag_task_state_fn: ?*const fn (usize) u8 = null;
/// Wake-ledger reads: (seq << 8 | branch) of a task's last scheduling hop,
/// and the global sequence counter, from the locally patched zio.
pub var diag_task_mark_fn: ?*const fn (usize) u64 = null;
pub var diag_stamp_now_fn: ?*const fn () u64 = null;

/// Diag-only registry of live TLS connections, so an OS thread (spawned by
/// the bench server, outside the zio scheduler) can sweep for the lost-wake
/// wedge signature and re-drive the pump futex. An in-runtime watchdog task
/// cannot serve here: near the wedge its own timer wakes are not delivered.
var diag_reg_lock: std.atomic.Value(bool) = .init(false);
var diag_reg: [64]?*Connection = @splat(null);

fn diagRegLock() void {
    while (diag_reg_lock.swap(true, .acquire)) std.atomic.spinLoopHint();
}

fn diagRegUnlock() void {
    diag_reg_lock.store(false, .release);
}

fn diagRegister(conn: *Connection) void {
    diagRegLock();
    defer diagRegUnlock();
    for (&diag_reg) |*slot| {
        if (slot.* == null) {
            slot.* = conn;
            return;
        }
    }
}

fn diagDeregister(conn: *Connection) void {
    diagRegLock();
    defer diagRegUnlock();
    for (&diag_reg) |*slot| {
        if (slot.* == conn) slot.* = null;
    }
}

/// Raw fd-2 write for diag lines, from tasks and OS threads alike.
/// `std.debug.print` buffers and writes positionally, so its lines do not
/// interleave chronologically with plain writes; every diag line goes
/// through here so the file order is the time order. The prefix is
/// CLOCK_MONOTONIC milliseconds.
pub fn diagRawPrint(comptime fmt: []const u8, args: anytype) void {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const ms: u64 = @as(u64, @intCast(ts.sec)) *% 1000 +% @as(u64, @intCast(ts.nsec)) / 1_000_000;
    var buf: [512]u8 = undefined;
    const line = std.fmt.bufPrint(&buf, "[{d}] " ++ fmt, .{ms % 1_000_000} ++ args) catch return;
    _ = std.c.write(2, line.ptr, line.len);
}

/// Sweep every registered connection: print the park snapshot and re-fire
/// the ACTOR futex when its event sat set across a whole interval. The old
/// pump-event rekick is gone with the wake protocol: the CQ driver has no
/// Event to re-drive. Called from an OS thread; everything touched here is
/// atomic or the futex bucket table.
pub fn diagRekickSweep() u32 {
    diagRegLock();
    defer diagRegUnlock();
    const kicked: u32 = 0;
    for (diag_reg) |maybe_conn| {
        const conn = maybe_conn orelse continue;
        const site = conn.tls_pump_site.load(.acquire);
        // No queue reads here: io_queue locks a std.Io.Mutex, which an OS
        // thread outside the runtime must never take.
        const pump_state: u8 = if (diag_task_state_fn) |f| f(conn.tls_pump_task_h.load(.acquire)) else 0xff;
        const actor_state: u8 = if (diag_task_state_fn) |f| f(conn.actor_task_h.load(.acquire)) else 0xff;
        const pump_mark: u64 = if (diag_task_mark_fn) |f| f(conn.tls_pump_task_h.load(.acquire)) else 0;
        const actor_mark: u64 = if (diag_task_mark_fn) |f| f(conn.actor_task_h.load(.acquire)) else 0;
        const stamp_now: u64 = if (diag_stamp_now_fn) |f| f() else 0;
        diagRawPrint(
            "STARH2_SWEEP conn={x} site={d} pump_tag={d} pump_awaken={d} actor_tag={d} actor_awaken={d} pump_hop={d}@{d} actor_hop={d}@{d} now={d} actor_site={d} mu_held={}\n",
            .{
                @intFromPtr(conn) & 0xffff,
                site,
                pump_state & 0x7,
                (pump_state >> 3) & 1,
                actor_state & 0x7,
                (actor_state >> 3) & 1,
                pump_mark & 0xff,
                pump_mark >> 8,
                actor_mark & 0xff,
                actor_mark >> 8,
                stamp_now,
                conn.actor_site.load(.acquire),
                @atomicLoad(bool, &conn.session_held, .monotonic),
            },
        );
        diagRawPrint(
            "STARH2_SWEEP2 conn={x} ackp={d} ackr={d} aapp={d} arec={d} tres={d} tcomp={d} tdrop_free={d} tdrop_mm={d} tls_in_cipher={d}\n",
            .{
                @intFromPtr(conn) & 0xffff,
                wire_pump.diag_acks.posted_ticket.load(.acquire),
                wire_pump.diag_acks.posted_receipt.load(.acquire),
                wire_pump.diag_acks.applied_ticket.load(.acquire),
                wire_pump.diag_acks.applied_receipt.load(.acquire),
                ticket_table.diag_reserved.load(.acquire),
                ticket_table.diag_completed.load(.acquire),
                ticket_table.diag_dropped_not_in_use.load(.acquire),
                ticket_table.diag_dropped_mismatch.load(.acquire),
                if (conn.tls) |t| t.pendingInboundCiphertext() else 0,
            },
        );
        // The Event-era re-kick probe is gone with the Event: a published
        // wake now lives in a channel until the actor takes it, so there is
        // no futex edge to re-fire.
    }
    return kicked;
}
pub var diag_rst_handler: std.atomic.Value(u32) = .init(0);
pub var diag_rst_compress: std.atomic.Value(u32) = .init(0);
pub var diag_ctrl_fail: std.atomic.Value(u32) = .init(0);
pub var diag_reset_after_ctrl_fail: std.atomic.Value(u32) = .init(0);
var diag_last_empty_park_ns: u64 = 0;
/// t-1022 probe: stamp of the last drainEmit entry, so the overflow print
/// can show the gap that let the backlog build. One connection at a time in
/// the probe, so a plain var is enough.
var diag_last_drain_ns: u64 = 0;
var diag_prev_drain_gap_ns: u64 = 0;

fn noteTestSocketClose() void {
    test_socket_close_ns.store(zio.Timestamp.now(.monotonic).toNanoseconds(), .release);
    _ = test_socket_closes.fetchAdd(1, .acq_rel);
}

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

/// std.Io socket handle to zio's ev handle (same fd; Windows would need the
/// pointer cast). Mirrors zio's private `stdIoHandleToZio`.
fn tlsNetHandle(h: std.Io.net.Socket.Handle) zio.ev.Backend.NetHandle {
    return if (@typeInfo(zio.ev.Backend.NetHandle) == .pointer) @ptrCast(h) else h;
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
///             is genuinely expensive: framing, flow-control debit, or the
///             blocking `write_ch.putOne` that runs while the lock is held.
/// - `ack`     unlock -> `tickets.complete` when the actor applies the write
///             ack. Dominant means the transport handoff, not the connection's
///             own work.
/// - `resume`  `tickets.complete` -> `waitTicket` returns. Dominant means
///             wake-to-run scheduler latency, not any of the above.
///
/// One-shot requests add three more intervals, sampled at dispatch so a CPU
/// profile cannot hide blocked spawn or job-lifecycle time:
///
/// - `spawn`   `dispatch` decides to sample -> `runHandlerJob` starts. Dominant
///             means the zio task is slow to become runnable.
/// - `to_send` handler start -> `sendCb` entry. Dominant means the application
///             handler, not the stack.
/// - `hpack`   inside `hold`, `applyCommand(.respond_headers)`. Dominant means
///             response header encode + HEADERS frame allocation.
///
/// Those intervals are wall latency on one sampled request. Concurrent streams
/// overlap them, so a large mean is not by itself a throughput cost. Batch
/// counters below are the throughput-shaped instrument: they fire only when
/// `enabled` is set at boot, and they count tickets per `queueWire` handoff
/// and WritePump records. A harness cross-checks ticket deltas against h2load
/// succeeded, not against a second self-reported request counter.
///
/// One event in `trace_sample_every` is measured for the latency intervals, so
/// that instrument cannot become the load. Batch counters are per-handoff, not
/// per-request, and stay off on the official bench path.
/// Outbound frame and write-chunk memory, server-wide. Every counter is one
/// relaxed atomic add. The refusals are the events that were a general-allocator
/// call before stage 3 (the frame and chunk fallbacks), so they must stay 0 in
/// a steady state; the work counters beside them must be positive in the same
/// run, or a zero proves nothing. `/mem` on the bench server prints them.
pub const outbound_memory = struct {
    /// Frames `rentFrame` served, and which pool each came from.
    pub var frames_built: std.atomic.Value(u64) = .init(0);
    pub var frames_from_slab: std.atomic.Value(u64) = .init(0);
    /// ... or from the terminal store (RST_STREAM and GOAWAY).
    pub var frames_from_terminal_store: std.atomic.Value(u64) = .init(0);
    pub var frames_from_chunk: std.atomic.Value(u64) = .init(0);
    /// ... or written straight into the drain-turn scratch (small DATA).
    pub var frames_in_scratch: std.atomic.Value(u64) = .init(0);
    /// Write chunks `leaseWriteChunk` handed to a batch flush or a copied frame.
    pub var chunks_leased: std.atomic.Value(u64) = .init(0);
    /// Write chunks handed to the pump, leased ones and owned frames alike,
    /// counted after the push succeeded. Wire frames per chunk is the packing
    /// measure; `chunks_leased` alone misses a frame that was built in a chunk
    /// and sent as it is. Counted only when `test_observe` is on (debug builds
    /// and `-Dobserve=true`): a release build adds nothing to the handoff, and
    /// the bench `/mem` shows 0 unless it was built with `-Dobserve=true`.
    pub var chunks_to_pump: std.atomic.Value(u64) = .init(0);
    /// `rentFrame` found neither a slab nor a chunk it may take: the frame is
    /// refused (`OutOfMemory` to the session).
    pub var frames_refused: std.atomic.Value(u64) = .init(0);
    /// `leaseWriteChunk` found the write pool empty. The drain checks room
    /// before every pop, so this is a broken bound, not a load result.
    pub var chunks_refused: std.atomic.Value(u64) = .init(0);
    /// `drainEmit` turns, and the pops the scheduler declined for lack of
    /// write-pool room (the drain resumes when a chunk comes back).
    pub var drain_turns: std.atomic.Value(u64) = .init(0);
    pub var drain_memory_pauses: std.atomic.Value(u64) = .init(0);
    /// Handler writes refused because the SERVER's outbound byte cap
    /// (`outbound_bytes_per_server`) was full, and because the server's
    /// outbound slab pool was empty (`SlabsExhausted`). Each resets the
    /// writer's stream. A full CONNECTION budget is not here: that parks the
    /// handler. Server-wide exhaustion is overload shedding, not
    /// backpressure: no release on this connection would make room, so
    /// waiting could not end.
    pub var server_cap_refusals: std.atomic.Value(u64) = .init(0);
    pub var slab_pool_refusals: std.atomic.Value(u64) = .init(0);
};

/// Work counters for the rest of the HTTP/2 path (stage 3 part 4), beside
/// `outbound_memory`: a zero count of general-allocator calls means nothing in
/// a run that did none of this work. Server-wide, relaxed; the bench server's
/// `/mem` prints them.
pub const h2_work = struct {
    /// TLS connections that started with plaintext already read during the
    /// handshake (a pipelined preface), ingested from the handshake task's own
    /// stack with no copy.
    pub var tls_leftovers: std.atomic.Value(u64) = .init(0);
    /// Response header lists prepared (`prepareResponseHeaderFields`,
    /// `appendResponseHeaders`), in the request's arena or on the stack.
    pub var response_header_lists: std.atomic.Value(u64) = .init(0);
};

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
    /// Receipt sub-phases: handler unlock -> scheduler sink, TLS/enqueue, then
    /// queued WritePump work -> actor applies the completion.
    pub var actor_ns: std.atomic.Value(u64) = .init(0);
    pub var queue_ns: std.atomic.Value(u64) = .init(0);
    pub var pump_ns: std.atomic.Value(u64) = .init(0);
    pub var ack_split_samples: std.atomic.Value(u64) = .init(0);
    pub var write_ns: std.atomic.Value(u64) = .init(0);
    pub var drain_ns: std.atomic.Value(u64) = .init(0);
    pub var pump_split_samples: std.atomic.Value(u64) = .init(0);
    /// Worst single sample of each, so a mean cannot hide a stall.
    pub var block_max: std.atomic.Value(u64) = .init(0);
    pub var hold_max: std.atomic.Value(u64) = .init(0);
    pub var ack_max: std.atomic.Value(u64) = .init(0);
    pub var resume_max: std.atomic.Value(u64) = .init(0);

    pub var jobs: std.atomic.Value(u64) = .init(0);
    pub var spawn_ns: std.atomic.Value(u64) = .init(0);
    pub var to_send_ns: std.atomic.Value(u64) = .init(0);
    pub var hpack_ns: std.atomic.Value(u64) = .init(0);
    pub var spawn_max: std.atomic.Value(u64) = .init(0);
    pub var to_send_max: std.atomic.Value(u64) = .init(0);
    pub var hpack_max: std.atomic.Value(u64) = .init(0);
    pub var lifecycle_samples: std.atomic.Value(u64) = .init(0);

    /// Ticketed `queueWire` handoffs and the tickets they carried. Gated by
    /// `enabled`. Histogram buckets are inclusive upper bounds: 1, 2, ≤4, ≤8,
    /// ≤16, ≥17.
    pub var handoffs: std.atomic.Value(u64) = .init(0);
    pub var tickets: std.atomic.Value(u64) = .init(0);
    pub var handoff_bytes: std.atomic.Value(u64) = .init(0);
    pub var handoff_max: std.atomic.Value(u64) = .init(0);
    pub var batch_1: std.atomic.Value(u64) = .init(0);
    pub var batch_2: std.atomic.Value(u64) = .init(0);
    pub var batch_le4: std.atomic.Value(u64) = .init(0);
    pub var batch_le8: std.atomic.Value(u64) = .init(0);
    pub var batch_le16: std.atomic.Value(u64) = .init(0);
    pub var batch_ge17: std.atomic.Value(u64) = .init(0);
    /// WritePump chunks queued. Packing authority for records/response.
    pub var records: std.atomic.Value(u64) = .init(0);
    /// Live TLS transform, gated by `enabled`, every call (not sampled).
    /// Amortize against h2load succeeded. `queue_ns` is a sampled wall wait
    /// across one receipt; these are CPU-shaped clocks around the cipher and
    /// the ciphertext enqueue.
    pub var encrypt_ns: std.atomic.Value(u64) = .init(0);
    pub var encrypt_n: std.atomic.Value(u64) = .init(0);
    pub var encrypt_bytes: std.atomic.Value(u64) = .init(0);
    pub var decrypt_ns: std.atomic.Value(u64) = .init(0);
    pub var decrypt_n: std.atomic.Value(u64) = .init(0);
    pub var decrypt_in: std.atomic.Value(u64) = .init(0);
    pub var decrypt_plain: std.atomic.Value(u64) = .init(0);
    pub var inbound_records: std.atomic.Value(u64) = .init(0);
    pub var decrypt_loop_ns: std.atomic.Value(u64) = .init(0);
    pub var decrypt_loop_n: std.atomic.Value(u64) = .init(0);
    pub var ingest_ns: std.atomic.Value(u64) = .init(0);
    pub var ingest_n: std.atomic.Value(u64) = .init(0);
    pub var intent_ns: std.atomic.Value(u64) = .init(0);
    pub var intent_n: std.atomic.Value(u64) = .init(0);
    pub var send_ns: std.atomic.Value(u64) = .init(0);
    pub var send_n: std.atomic.Value(u64) = .init(0);
    pub var send_bytes: std.atomic.Value(u64) = .init(0);
    /// Unused after TLS-as-stream (decrypt lives in TlsPump). Kept so the
    /// phase-trace printer does not grow a second schema.
    pub var acc_append_ns: std.atomic.Value(u64) = .init(0);
    pub var acc_append_n: std.atomic.Value(u64) = .init(0);
    pub var acc_append_bytes: std.atomic.Value(u64) = .init(0);
    pub var acc_compact_ns: std.atomic.Value(u64) = .init(0);
    pub var acc_compact_n: std.atomic.Value(u64) = .init(0);
    pub var acc_compact_bytes: std.atomic.Value(u64) = .init(0);
    /// `drainEmit` turns that flushed at least one ticket, and tickets in those
    /// turns. Mean tickets/turn is a batching signal distinct from wall waits.
    pub var emit_turns: std.atomic.Value(u64) = .init(0);
    pub var emit_tickets: std.atomic.Value(u64) = .init(0);
    pub var emit_max: std.atomic.Value(u64) = .init(0);
    /// Maximum actor-owned complete-batch receipts concurrently awaiting ack.
    pub var complete_receipt_depth_max: std.atomic.Value(u64) = .init(0);
    /// Depth after each publish/consume. `receipt_depth_2` is how often the
    /// pipeline sat at capacity, not merely that it touched 2 once.
    pub var receipt_depth_0: std.atomic.Value(u64) = .init(0);
    pub var receipt_depth_1: std.atomic.Value(u64) = .init(0);
    pub var receipt_depth_2: std.atomic.Value(u64) = .init(0);
    pub var receipt_depth_3: std.atomic.Value(u64) = .init(0);
    pub var receipt_depth_4: std.atomic.Value(u64) = .init(0);
    /// Wall time the actor held depth == capacity, and how many times it entered.
    pub var receipt_full_ns: std.atomic.Value(u64) = .init(0);
    pub var receipt_full_enter: std.atomic.Value(u64) = .init(0);
    /// `runPendingInline` returned because depth was full while `inline_n > 0`.
    pub var inline_full_n: std.atomic.Value(u64) = .init(0);
    pub var inline_full_sids: std.atomic.Value(u64) = .init(0);
    pub var inline_full_max: std.atomic.Value(u64) = .init(0);
    /// `runPendingInline` cut a batch short: no ordinary control entry left.
    pub var inline_ctrl_full_n: std.atomic.Value(u64) = .init(0);
    /// Complete-batch stamp → actor consumed the receipt (credit reuse).
    pub var credit_reuse_ns: std.atomic.Value(u64) = .init(0);
    pub var credit_reuse_n: std.atomic.Value(u64) = .init(0);
    /// After the actor takes one inbound chunk (including a batch extra), how
    /// many still sit in `read_ch`. `read_left_0` means the next `tryGet` would
    /// have found nothing. Do not use these to size the hot-path batch.
    pub var read_take_n: std.atomic.Value(u64) = .init(0);
    pub var read_left_0: std.atomic.Value(u64) = .init(0);
    pub var read_left_1: std.atomic.Value(u64) = .init(0);
    pub var read_left_2: std.atomic.Value(u64) = .init(0);
    pub var read_left_3: std.atomic.Value(u64) = .init(0);
    pub var read_left_ge4: std.atomic.Value(u64) = .init(0);
    pub var read_left_sum: std.atomic.Value(u64) = .init(0);
    pub var read_left_max: std.atomic.Value(u64) = .init(0);
    /// How many inbound chunks one actor turn ingested (1..=inbound_chunk_batch).
    pub var inbound_batch_1: std.atomic.Value(u64) = .init(0);
    pub var inbound_batch_2: std.atomic.Value(u64) = .init(0);
    pub var inbound_batch_3: std.atomic.Value(u64) = .init(0);
    pub var inbound_batch_4: std.atomic.Value(u64) = .init(0);

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

    fn noteHandoff(ticket_count: u32, nbytes: usize) void {
        if (ticket_count == 0) return;
        _ = handoffs.fetchAdd(1, .monotonic);
        _ = tickets.fetchAdd(ticket_count, .monotonic);
        _ = handoff_bytes.fetchAdd(nbytes, .monotonic);
        bumpMax(&handoff_max, ticket_count);
        if (ticket_count == 1) {
            _ = batch_1.fetchAdd(1, .monotonic);
        } else if (ticket_count == 2) {
            _ = batch_2.fetchAdd(1, .monotonic);
        } else if (ticket_count <= 4) {
            _ = batch_le4.fetchAdd(1, .monotonic);
        } else if (ticket_count <= 8) {
            _ = batch_le8.fetchAdd(1, .monotonic);
        } else if (ticket_count <= 16) {
            _ = batch_le16.fetchAdd(1, .monotonic);
        } else {
            _ = batch_ge17.fetchAdd(1, .monotonic);
        }
    }
};

pub const Mode = limits_mod.ConnMode;

pub const ConnConfig = struct {
    /// Set together when the accept loop placed this connection with
    /// `Balancer.reserve`: the balancer and the executor its connection and
    /// task handlers are counted against.
    balancer: ?*Balancer = null,
    exec_index: ?zio.ExecutorId = null,
    io: std.Io,
    mode: Mode,
    limits: limits_mod.Limits,
    router: router_mod.Router,
    tls_acceptor: ?*tls_edge.Acceptor = null,
    gpa: std.mem.Allocator,
    shutdown_flag: ?*std.atomic.Value(bool) = null,
    /// One-shot: set once at server shutdown, never reset, so selecting on it
    /// is race-free (persistent set, no edge to lose).
    shutdown_event: ?*zio.Event = null,
    reaper: ?*ReaperPool = null,
    /// Server-wide stream + reaper reservation (optional for unit tests).
    accounting: ?*GlobalAccounting = null,
    /// Server-wide outbound DATA slabs, borrowed for this connection's lifetime.
    slab_pool: *slab_pool.SlabPool,
    /// Server-wide request memory. Every HTTP/2 request's header list, body
    /// and handler allocations come from here and nowhere else; there is no
    /// fallback to `gpa`. Required for h2c and TLS (`Connection.init` fails
    /// without it); h1c keeps its own buffers and ignores it.
    request_pool: ?*request_pool.RequestPool = null,
    /// Server-wide brotli encoder pool when response compression is enabled.
    compression_pool: ?*brotli.Pool = null,
    /// True when accept path reserved a concurrent TLS handshake slot.
    handshake_held: bool = false,
    /// Pre-handshaked TLS. When set, `run` skips handshake and owns this Conn.
    tls_ready: ?*tls_edge.Conn = null,
    tls_leftover: []const u8 = &.{},
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
    active_handshakes: std.atomic.Value(usize) = .init(0),
    max_handshakes: usize,
    /// Accept loops parked because the connection or handshake limit is full
    /// (`Server.awaitAdmission`). A release that frees either kind of slot
    /// calls `wakeAdmission`, which costs one load unless a loop is parked.
    ///
    /// The slot counters and this count are read and written `.seq_cst` on
    /// the admission paths. A parked loop bumps this count and then rechecks
    /// the slots; a release frees its slot and then reads this count. Total
    /// order on those four operations means one side always sees the other,
    /// so a release can never slip between the recheck and the wait.
    admission_waiters: std.atomic.Value(usize) = .init(0),
    admission_mu: zio.Mutex = .init,
    admission_cond: zio.Condition = .init,

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

    pub fn tryAdmitHandshake(self: *GlobalAccounting) bool {
        while (true) {
            const cur = self.active_handshakes.load(.seq_cst);
            if (cur >= self.max_handshakes) return false;
            if (self.active_handshakes.cmpxchgWeak(cur, cur + 1, .seq_cst, .seq_cst) == null) return true;
        }
    }

    pub fn releaseHandshake(self: *GlobalAccounting) void {
        const prev = self.active_handshakes.fetchSub(1, .seq_cst);
        std.debug.assert(prev > 0);
        self.wakeAdmission();
    }

    /// Wake every accept loop parked on a full admission limit; each rechecks
    /// its own condition. Called after a connection or handshake slot is
    /// released. Runs on connection tasks only (zio mutex).
    pub fn wakeAdmission(self: *GlobalAccounting) void {
        if (self.admission_waiters.load(.seq_cst) == 0) return;
        self.admission_mu.lockUncancelable();
        defer self.admission_mu.unlock();
        self.admission_cond.broadcast();
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
/// `completion_owner` is a four-state race arbiter, and it is what makes the
/// slot release exactly-once. Two tasks can decide that a handler is finished:
/// the handler itself when it returns, and the actor when it cancels. Both
/// attempt a CAS; the winner posts the completion, the loser does nothing.
/// - `live` (0): the handler is running and will report itself.
/// - `reaper_owned` (1): the join is in the reaper queue; no worker has it yet.
/// - `reported` (2): the completion is posted or queued. Also the free state.
/// - `reaper_running` (3): a worker dequeued the job and is in `JoinHandle.cancel`.
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
    /// Diagnosis (t-2655): where this slot's handler is blocked right now
    /// (`DiagWait`), and since when (monotonic ns). Written by the handler
    /// around each wait; read by `diagHandlerState`.
    diag_wait: std.atomic.Value(u8) = .init(0),
    diag_since_ns: std.atomic.Value(u64) = .init(0),
    /// Incremented as the first act of every `finishHandlerJob`. Exactly 1 at
    /// `releaseSlot` for an admitted job. Not an idempotency flag: a second
    /// increment panics.
    finalize_count: std.atomic.Value(u8) = .init(0),
    /// Set when `live_handlers` is incremented. `errdefer releaseSlot` before
    /// that point is rollback, not a missing finalize.
    admitted: bool = false,
    /// This handler's place in the connection's budget queue
    /// (`Connection.parkForBudget`). Guarded by `Connection.budget_mu`; idle
    /// whenever the handler is not inside that function.
    budget: BudgetWait = .{},
};

/// One handler's wait for connection outbound budget. The entry lives in the
/// handler's slot, so the queue allocates nothing.
///
/// A parked task writer and a gated inline job wait in the SAME queue, in
/// arrival order; only what a grant wakes differs.
///
/// - `queued`: in the queue, owed `need` bytes, not yet reserved.
/// - `granted`: `need` bytes are reserved for this handler (connection and
///   server counters) and the handler has not looked yet. The handler owns
///   them from here: it copies them into its slab, or gives them back.
/// - `refused`: the hand-over hit the SERVER cap, so nothing is reserved.
///   Nothing on this connection would ever retry it, so it is woken to fail.
pub const BudgetWait = struct {
    pub const State = enum(u8) { idle, queued, granted, refused };
    state: State = .idle,
    need: usize = 0,
    /// The waiter is a complete (inline) job, not a parked task writer. The
    /// actor runs it, so a grant rings the actor's doorbell instead of waking
    /// a handler, and the actor takes the outcome (`inlineBudget`).
    is_inline: bool = false,
    /// Slot index of the next waiter, or `budget_none`.
    next: u32 = budget_none,
};

const budget_none: u32 = std.math.maxInt(u32);

/// The blocking point a task handler is parked at, for `diagHandlerState`.
pub const DiagWait = enum(u8) {
    running = 0,
    start_reserve_ticket,
    start_lock,
    start_wait_ticket,
    deadline_wait,
    write_reserve_ticket,
    write_lock,
    write_space_or_enqueue,
    write_wait_ticket,
};

/// Off by default: the stuck-handler diagnosis (`diagWait`, `diagActor`,
/// `diag_actor_turns`) writes atomics on every handler wait and actor turn,
/// which is hot-path cost a measurement must not pay. A diagnosis run sets
/// it before the server starts (bench_server --diag-stuck).
pub var diag_stuck: bool = false;

/// `lk_*` are points inside the actor's `session_mu` section, so a frozen
/// actor that still holds the lock names the call it stopped in. `lk_spawn`
/// is set around zio's spawn, which can yield inside the lock.
pub const DiagActor = enum(u8) { running = 0, loop_lock, select, tls_turn, inline_handlers, lk_checks, lk_materialize, lk_spawn, lk_drain_emit, lk_tail };

fn diagActor(c: *Connection, w: DiagActor) void {
    if (!diag_stuck) return;
    c.diag_actor_where.store(@intFromEnum(w), .monotonic);
}

fn diagWait(hctx: *HandlerCtx, w: DiagWait) void {
    if (!diag_stuck) return;
    hctx.slot.diag_since_ns.store(if (w == .running) 0 else nowNs(hctx.conn.config.io), .monotonic);
    hctx.slot.diag_wait.store(@intFromEnum(w), .monotonic);
}

/// Snapshot for a stuck-handler dump. `wait`/`waited_ms` come from atomics;
/// the connection fields are plain reads of actor-owned state taken without
/// the session lock, so they are a racy hint (good enough to see what a
/// wedged connection holds), never a value to act on.
pub const DiagHandlerState = struct {
    stream_id: u31,
    conn_addr: usize,
    wait: DiagWait,
    waited_ms: u64,
    sched_terminal_len: usize,
    sched_ordinary_len: usize,
    sched_framed_len: usize,
    sched_active_pending: usize,
    sched_emits_total: usize,
    tls_stash_full: ?bool,
    session_held: bool,
    live_task_handlers: usize,
    conn_send_available: i64,
    actor_turns: u64,
    actor_where: DiagActor,
    tls_carried: bool,
    tls_write_ch_len: usize,
    tls_send_armed: bool,
    tls_recv_armed: bool,
    tls_pending_read: bool,
    tls_pending_cipher: bool,
    /// The actor's zio task handle (0 if no hook is installed).
    actor_task: usize,
};

/// `resp_ctx` is a task handler's `Response.ctx`, valid while that handler
/// runs.
pub fn diagHandlerState(resp_ctx: *anyopaque) DiagHandlerState {
    const hctx: *HandlerCtx = @ptrCast(@alignCast(resp_ctx));
    const c = hctx.conn;
    const since = hctx.slot.diag_since_ns.load(.monotonic);
    const now = nowNs(c.config.io);
    return .{
        .stream_id = hctx.stream_id,
        .conn_addr = @intFromPtr(c),
        .wait = @enumFromInt(hctx.slot.diag_wait.load(.monotonic)),
        .waited_ms = if (since == 0 or now < since) 0 else (now - since) / std.time.ns_per_ms,
        .sched_terminal_len = c.sched.terminal_len,
        .sched_ordinary_len = c.sched.ordinary_len,
        .sched_framed_len = c.sched.framed_len,
        .sched_active_pending = c.sched.active_pending,
        .sched_emits_total = c.sched.emits_total,
        .tls_stash_full = if (c.tls_driver) |pump| pump.stashFull() else null,
        .session_held = c.session_held,
        .live_task_handlers = c.live_task_handlers.load(.monotonic),
        .conn_send_available = c.session.connectionSendAvailable(),
        .actor_turns = c.diag_actor_turns.load(.monotonic),
        .actor_where = @enumFromInt(c.diag_actor_where.load(.monotonic)),
        .tls_carried = if (c.tls_driver) |pump| pump.carried != null else false,
        .tls_write_ch_len = if (c.tls_driver) |pump| io_queue.chanLen(wire_pump.WireChunk, pump.write_ch) else 0,
        .tls_send_armed = if (c.tls_driver) |pump| pump.send_armed else false,
        .tls_recv_armed = if (c.tls_driver) |pump| pump.recv_armed else false,
        .tls_pending_read = if (c.tls_driver) |pump| pump.pending_read != null else false,
        .tls_pending_cipher = if (c.tls_driver) |pump| pump.pending_cipher != null else false,
        .actor_task = c.actor_task_h.load(.acquire),
    };
}

const HandlerCtx = struct {
    conn: *Connection,
    terminal: *response.SlotTerminal,
    slot: *HandlerSlot,
    stream_id: u31,
    method: request.Method = .GET,
    /// The request's arena: where a response's header list is built. It is
    /// the handler's own allocator, used only on the handler's task (or on the
    /// actor for a complete handler), never both at once.
    arena: std.mem.Allocator,
    /// Combined Accept-Encoding field value (arena-owned), empty if absent.
    accept_encoding: []const u8 = "",
    encoder: ?*brotli.Encoder = null,
    compressing: bool = false,
    /// One-shot lifecycle sample, decided at dispatch so spawn delay is visible.
    trace_on: bool = false,
    t_dispatch: u64 = 0,
    t_handler: u64 = 0,
    /// True for a complete handler that `runPendingInline` runs on the actor,
    /// for that job's whole life. Such a handler must not park on anything
    /// only the actor wakes (stream space, a ticket), so `sendCb` leaves its
    /// receipt to the batch drain, where multiplexed complete responses share
    /// one TLS record. This is per handler on purpose: a task handler on the
    /// same connection can be writing while the actor runs a complete batch.
    runs_inline: bool = false,
    /// Budget bytes `runPendingInline` reserved for this inline job (reserved
    /// as pending, so a write only spends them; the unused part goes back when
    /// the job ends). The actor must not park, so an inline job never waits for
    /// budget: the gate waits for room BEFORE it runs the job. A complete
    /// handler's whole body fits one stream's slab, so one stream's cap is all
    /// it can write.
    inline_credit: usize = 0,
    /// The gate found the SERVER cap full: the job runs with no credit and its
    /// first body write is refused like any server-cap refusal.
    inline_shed: bool = false,
    /// `1` means encode succeeded. The drain-turn receipt is reserved after
    /// the inline batch, not per response.
    deferred_ticket: u64 = 0,
    /// Set when this send owns the connection's one `trace_ticket` sample.
    send_trace: ?SendTrace = null,
};

/// Timestamps of one sampled `sendCb`, stamped into the trace counters once
/// its write ack lands (`stampSendWait`).
const SendTrace = struct {
    before: u64,
    acquired: u64,
    unlocked: u64,
    hpack: u64,
};

/// Probes `LeaseIndex.find` made (observe builds only): every lease it looked
/// at. A test reads the count to show a lookup does not scan.
pub var test_lease_probes: test_hook.Hook(std.atomic.Value(u64)) = if (test_hook.on) .init(0) else {};

/// How a stream id finds its `RequestLease` in O(1), and how a new request
/// finds a free one. Both are tables sized at boot (`Connection.planBoot`),
/// owned by the actor.
///
/// Chosen over keeping the lease where the stream already lives: the stream is
/// a `core` type (`Session.streams`), and `core` has no knowledge of the edge,
/// so a lease position there would be an edge field in the protocol layer.
/// This is an open-addressing table of lease positions (plus one; zero is
/// empty), keyed by the lease's own `stream_id`, with linear probing and
/// backward-shift deletion, so it has no tombstones and never fills: it has at
/// least two slots per lease. `free` is a stack of the positions of touched
/// leases that hold no request, so taking a lease for a new request does not
/// scan either. `Touched` itself only grows, which is why the free stack is
/// needed.
const LeaseIndex = struct {
    slots: []u32,
    free: []u32,
    free_len: usize = 0,
    bits: u5,

    /// Number of index slots for `leases_n` leases: a power of two of at
    /// least twice as many. Also checks the size fits.
    fn slotCount(leases_n: usize) error{InvalidConfig}!usize {
        const want = try boot_plan.mul(leases_n, 2);
        const n = std.math.ceilPowerOfTwo(usize, @max(want, 2)) catch return error.InvalidConfig;
        if (n > (@as(usize, 1) << 31)) return error.InvalidConfig;
        return n;
    }

    fn init(slots: []u32, free: []u32) LeaseIndex {
        @memset(slots, 0);
        return .{ .slots = slots, .free = free, .bits = @intCast(std.math.log2_int(usize, slots.len)) };
    }

    fn home(self: *const LeaseIndex, stream_id: u31) usize {
        const h: u32 = @as(u32, stream_id) *% 2654435761;
        return h >> @intCast(32 - @as(u6, self.bits));
    }

    fn find(self: *const LeaseIndex, leases: []const RequestLease, stream_id: u31) ?u32 {
        const mask = self.slots.len - 1;
        var i = self.home(stream_id);
        while (true) : (i = (i + 1) & mask) {
            const v = self.slots[i];
            if (v == 0) return null;
            if (comptime test_observe) _ = test_lease_probes.fetchAdd(1, .monotonic);
            if (leases[v - 1].stream_id == stream_id) return v - 1;
        }
    }

    fn insert(self: *LeaseIndex, leases: []const RequestLease, pos: u32) void {
        const mask = self.slots.len - 1;
        var i = self.home(leases[pos].stream_id);
        while (self.slots[i] != 0) i = (i + 1) & mask;
        self.slots[i] = pos + 1;
    }

    /// Remove `stream_id`'s entry; call while the lease still has its id.
    fn remove(self: *LeaseIndex, leases: []const RequestLease, stream_id: u31) void {
        const mask = self.slots.len - 1;
        var i = self.home(stream_id);
        while (true) : (i = (i + 1) & mask) {
            const v = self.slots[i];
            std.debug.assert(v != 0);
            if (leases[v - 1].stream_id == stream_id) break;
        }
        // Backward shift: pull later entries of the probe run into the hole
        // unless they already sit between their home slot and the hole.
        var j = i;
        while (true) {
            j = (j + 1) & mask;
            const v = self.slots[j];
            if (v == 0) break;
            const k = self.home(leases[v - 1].stream_id);
            const between = if (i <= j) (i < k and k <= j) else (i < k or k <= j);
            if (!between) {
                self.slots[i] = v;
                i = j;
            }
        }
        self.slots[i] = 0;
    }

    fn takeFree(self: *LeaseIndex) ?u32 {
        if (self.free_len == 0) return null;
        self.free_len -= 1;
        return self.free[self.free_len];
    }

    fn putFree(self: *LeaseIndex, pos: u32) void {
        self.free[self.free_len] = pos;
        self.free_len += 1;
    }
};

/// A request's memory, keyed by stream. The arena is the request's: HPACK
/// decodes its header block into it, the body is appended to it, and after
/// dispatch it backs `Request.arena` for the handler. The lease is the
/// arena's home and stays put and occupied until `releaseSlot` has joined the
/// handler (`releaseLease`), so the handler's `HandlerJob` holds a pointer and
/// nothing is copied or moved between the actor and the handler. A request
/// that never dispatches gives its blocks back where its lease is released
/// (`releaseHeaderList`, `quiesce`).
const RequestLease = struct {
    stream_id: u31 = 0,
    /// True once dispatch handed the arena to a `HandlerJob`. From then on only
    /// `releaseSlot` gives the lease back, after the handler was joined.
    in_job: bool = false,
    arena: request_pool.Arena = undefined,
};

/// The request's Accept-Encoding lines as one value (RFC 9110 section 5.3
/// lets repeated lines be combined with ", "). One line is used as it is,
/// since it already lives in the request's arena; two or more are joined into
/// one allocation.
fn joinAcceptEncodingLines(a: std.mem.Allocator, headers: []const request.Header) error{OutOfMemory}![]const u8 {
    var count: usize = 0;
    var total: usize = 0;
    var only: []const u8 = "";
    for (headers) |h| {
        if (!std.ascii.eqlIgnoreCase(h.name, "accept-encoding")) continue;
        count += 1;
        total += h.value.len;
        only = h.value;
    }
    if (count == 0) return "";
    if (count == 1) return only;
    const out = try a.alloc(u8, total + 2 * (count - 1));
    var off: usize = 0;
    var seen: usize = 0;
    for (headers) |h| {
        if (!std.ascii.eqlIgnoreCase(h.name, "accept-encoding")) continue;
        @memcpy(out[off..][0..h.value.len], h.value);
        off += h.value.len;
        seen += 1;
        if (seen < count) {
            out[off] = ',';
            out[off + 1] = ' ';
            off += 2;
        }
    }
    std.debug.assert(off == out.len);
    return out;
}

/// Per-slot handler state, reserved at connection boot so dispatch never heap-allocates a job.
const HandlerJob = struct {
    conn: *Connection,
    /// The request's own memory: its lease's arena. Allocated from on the
    /// handler's task, returned by the actor in `releaseSlot`.
    arena: *request_pool.Arena,
    stream_id: u31,
    owned_request: session_mod.DispatchRequest,
    req: request.Request,
    resp: response.Response,
    matched: router_mod.Match,
    slot: *HandlerSlot = undefined,
    hctx: HandlerCtx = undefined,
    /// True after a task handler is spawned. Complete oneshots stay false so
    /// they never increment `live_task_handlers`.
    task_counted: bool = false,
};

/// One optional deadline per live task-handler slot. Actor is the only mutator
/// of the heap (handlers insert under `session_mu`, same shape as `writeFn`).
const live: u8 = 0;
const reaper_owned: u8 = 1;
const reported: u8 = 2;
const reaper_running: u8 = 3;

/// Complete-handler drain turns that may await their wire receipts at once.
/// Independent of the deeper WritePump queue. 2 serialized the 8-worker
/// mixed oneshot pipe (inline skipped while full); 4 is the occupancy ABA.
pub const complete_receipt_capacity: usize = 4;

/// Inbound `read_ch` chunks ingested on one actor turn before one
/// `runPendingInline`. Matches receipt capacity so K requests can share
/// one drain-turn. Do not size this with `io_queue.queued`.
pub const inbound_chunk_batch: usize = 4;

/// Non-end `wait` writes (SSE `writeAll`) park while this stream's pending
/// DATA is at or above this watermark, then enqueue without a ticket.
///
/// Cap 1 was occupancy of one in-flight enqueue: mixed SSE-only hit 16 000,
/// but mixed+oneshot dropped to ~9k because complete handlers keep the actor
/// busy and every stream waited for that drain instead of returning to its
/// 10 ms timer. 8 KiB is several saturating events and still far below the
/// 64 KiB slab, so a producer that outruns the connection self-clocks before
/// queue depth becomes the latency.
const flush_occupancy_cap: usize = 8 * 1024;

/// A complete-handler drain turn whose write is queued but not yet acked.
///
/// It holds no handler slot. The batch's jobs finish at the end of the drain
/// turn that encoded them, because by then nothing references their arenas:
/// HEADERS were HPACK-encoded by Session and the body was copied into the
/// scheduler slab. Holding the slots until this ack refused streams inside
/// the advertised limit: the peer reads END_STREAM, opens its next stream
/// legally (RFC 9113 5.1.2), and found every slot still busy (t-3491).
///
/// What the receipt still does: it bounds how many drain turns run ahead of
/// WritePump (`complete_receipt_capacity`), reclaims the ticket slot, and
/// carries the one sampled send of the batch to `stampSendWait`.
const PendingCompleteReceipt = struct {
    ticket: u64 = 0,
    ticket_slot: u32 = ticket_table.no_completion_slot,
    send_trace: ?SendTrace = null,
};

comptime {
    if (complete_receipt_capacity != 4) @compileError("complete receipt pipeline capacity must stay 4");
    if (inbound_chunk_batch != 4) @compileError("inbound chunk batch must stay 4");
    if (inbound_chunk_batch > complete_receipt_capacity) {
        @compileError("inbound chunk batch cannot exceed the receipt window");
    }
}

pub const ReaperJob = struct {
    handle: zio.JoinHandle(void),
    owner: *std.atomic.Value(u8),
    /// The actor selects on this channel; the post IS the wake.
    completion: *zio.Channel(u31),
    stream_id: u31,
    /// Connection-local count of successful `trySend` publications.
    post_ok: *std.atomic.Value(usize),
};

comptime {
    if (@sizeOf(ReaperJob) != limits_mod.REAPER_JOB_SIZE) {
        @compileError(std.fmt.comptimePrint(
            "REAPER_JOB_SIZE must match @sizeOf(ReaperJob) (got {d})",
            .{@sizeOf(ReaperJob)},
        ));
    }
}

/// Cancellation happens off the actor, and this pool is why.
///
/// `JoinHandle.cancel` BLOCKS until the target task actually stops. If the actor
/// called it directly, one handler that is slow to notice cancellation would
/// stall the whole connection — including the reads and the writes that might
/// be what lets the handler finish. So the actor hands the join handle to a
/// worker here and continues at once.
///
/// The reaper capacity is reserved BEFORE a *task* handler is admitted
/// (`tryReserveReaper` in `dispatch`), never at cancel time. Cancellation must
/// not be able to fail for want of a resource, because there is no way to
/// retry it and no other path that would release the slot. Complete oneshots
/// never spawn and never own a join handle, so they do not take a token.
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
    /// 1. Mark `reaper_running` so a stall dump can tell queued from cancel.
    /// 2. `zio.JoinHandle.cancel` requests cancel, then waits until the task
    ///    actually stops (`waitUntilComplete`). The handler may live on
    ///    another executor; its completion wakes this worker cross-thread.
    ///    A handler in an uncancelable wait (lockUncancelable, or Futex's
    ///    no_cancel handshake after a racing wake) never returns, so this
    ///    worker never posts.
    /// 3. `swap(reported)` — claim the right to report. If the previous value
    ///    is not `reaper_running`, the handler already reported; stay silent.
    /// 4. post the completion. The actor selects on the channel, so the post
    ///    IS the wake.
    pub fn worker(self: *ReaperPool) std.Io.Cancelable!void {
        while (true) {
            var job = self.jobs.getOne(self.io) catch |err| switch (err) {
                error.Closed => return,
                error.Canceled => return error.Canceled,
            };
            probeNote(.reaper_job);
            job.owner.store(reaper_running, .release);
            std.debug.assert(job.owner.load(.acquire) == reaper_running);
            job.handle.cancel();
            const prev = job.owner.swap(reported, .acq_rel);
            if (prev == reaper_running) {
                if (job.completion.trySend(job.stream_id)) |_| {
                    _ = job.post_ok.fetchAdd(1, .acq_rel);
                } else |err| switch (err) {
                    // One completion per slot, capacity max_streams: full is a
                    // broken invariant, never a wait.
                    error.WouldBlock => @panic("completion channel over proven capacity"),
                    error.Closed => {
                        // Connection teardown has already closed completion delivery.
                    },
                }
            }
        }
    }
};

/// The byte layout of a slot's boot buffer for `mode`: the TLS `Conn` first,
/// when there is one, then everything `Connection.init` takes. `mode` must be
/// an HTTP/2 mode. `Limits.slotBootBytes` returns its end.
pub fn planSlotBoot(plan: *boot_plan.Plan, limits: limits_mod.Limits, mode: Mode) error{InvalidConfig}!void {
    std.debug.assert(mode != .h1c);
    if (mode == .tls) try plan.array(tls_edge.Conn, 1);
    try Connection.planBoot(plan, limits, mode);
}

/// What a test leaves behind on purpose after `quiesce`, so it can watch
/// `recycle` refuse the slot. Each names one condition `dirtiness` checks.
/// They are the conditions `quiesce` does NOT already stop on (see the table
/// on `dirtiness`), plus one it does, to show the check itself works.
pub const DirtyKind = enum {
    write_chunk_rented,
    request_bytes_reserved,
    stream_still_admitted,
    handler_join_held,
    completion_queued,
};

/// Test-only: how many of the next connections to end in a slot are dirtied,
/// and how. Counted in Debug and `-Dobserve=true`.
pub var test_dirty_remaining: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_dirty_kind: test_hook.Hook(DirtyKind) = if (test_hook.on) .write_chunk_rented else {};

fn testTakeDirtyTicket() bool {
    var n = test_dirty_remaining.load(.acquire);
    while (n > 0) {
        n = test_dirty_remaining.cmpxchgWeak(n, n - 1, .acq_rel, .acquire) orelse return true;
    }
    return false;
}

/// Test-only, thread-local: true while this thread is inside the boot of a
/// connection. The boot path never yields, so a counting allocator can read
/// this to tell a general-allocator call made by boot from one made by a
/// request. Counted in Debug and `-Dobserve=true`.
pub threadlocal var test_in_boot: test_hook.Hook(bool) = if (test_hook.on) false else {};

/// Entry point for one accepted socket. Boots the `Connection` into `slot`
/// and runs it. The slot owns the connection's memory; this function ends the
/// connection (`quiesce`) and leaves the slot to its caller to `recycle`.
///
/// The stream hook context is patched after construction because the hooks need
/// a pointer to the `Connection`, which does not exist while `Session.init`
/// runs. The read-chunk free list is seeded here rather than in `init` for the
/// same reason: the queues are only live once the struct has its final address.
pub fn serveAccepted(stream: std.Io.net.Stream, config: ConnConfig, slot: *conn_slot.ConnSlot) std.Io.Cancelable!void {
    if (comptime test_observe) test_in_boot = true;
    const booted = Connection.init(slot.bootAllocator(), stream, config);
    if (comptime test_observe) test_in_boot = false;
    slot.conn = booted catch |err| {
        if (close_probe) diagRawPrint("t1002 init_fail {s}\n", .{@errorName(err)});
        if (config.handshake_held) {
            if (config.accounting) |a| a.releaseHandshake();
        }
        if (config.tls_ready) |t| t.deinit();
        stream.close(config.io);
        return;
    };
    slot.state = .booted;
    if (std.debug.runtime_safety) {
        // Every connection, under whatever limits it runs with, checks that
        // `planBoot` still names what `init` took. The exact-fit test covers
        // a few limits; this covers every one a test or a server uses.
        const planned = config.limits.slotBootBytes(config.mode) catch unreachable;
        if (slot.fba.end_index != planned) {
            std.debug.panic("slot boot used {d} bytes, planBoot says {d}: a boot allocation and its plan disagree", .{ slot.fba.end_index, planned });
        }
    }
    const conn = &slot.conn;
    defer {
        conn.quiesce();
        if (comptime test_observe) {
            if (testTakeDirtyTicket()) conn.dirtyOnPurpose(test_dirty_kind);
        }
    }
    conn.finishBoot() catch |err| {
        if (close_probe) diagRawPrint("t1002 finish_boot_fail {s}\n", .{@errorName(err)});
        return;
    };
    if (comptime test_observe) test_boot_ready.store(true, .release);
    conn.run() catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            // Transport/protocol failure is connection-local and closes below.
            // Raw writes, no std.fmt: this path crashed once through
            // Writer.printValue in ReleaseSafe (unreproduced; diag-only).
            if (close_probe) {
                const name = @errorName(err);
                _ = std.c.write(2, "t1002 run_err ", 14);
                _ = std.c.write(2, name.ptr, name.len);
                _ = std.c.write(2, "\n", 1);
            }
        },
    };
}

pub const Connection = struct {
    stream: std.Io.net.Stream,
    config: ConnConfig,
    /// Where `init` took the boot tables below (arrays, both slab pools, the
    /// scheduler's tables, Session's fixed maps). On the live edge it is the
    /// connection slot's buffer, which `ConnSlot.recycle` rewinds, so `quiesce`
    /// frees none of it; `deinit` frees it for a connection built outside a
    /// slot.
    boot: std.mem.Allocator,
    session: session_mod.Session,
    read_ch_buf: []wire_pump.WireChunk,
    write_ch_buf: []wire_pump.WireChunk,
    /// Inbound chunks from the pump. A zio channel the actor selects on;
    /// capacity is the pool size + 1 so the EOF sentinel always fits.
    read_ch: zio.Channel(wire_pump.WireChunk) = undefined,
    write_ch: std.Io.Queue(wire_pump.WireChunk) = undefined,
    /// The flag-producers' wake: capacity 1, `ring` is the only writer.
    /// `error.WouldBlock` on the ring means a wake is already pending. A
    /// buffered ring survives until the actor takes it, so there is no
    /// reset and no set-before-park race.
    doorbell_buf: [1]u8 = undefined,
    doorbell: zio.Channel(u8) = undefined,
    tls: ?*tls_edge.Conn = null,
    /// Drain-turn packing buffer. HTTP/2 frames concat here before one
    /// `queueWire`; on TLS that plaintext is what the pump encrypts.
    plaintext_scratch: []u8 = &.{},
    /// Written on first use: `allocSlot` touches the next slot only when
    /// every touched slot is busy. `handler_joins`, `space_events` and
    /// `deadline_events` are indexed like `handlers` and are valid for the
    /// touched slots only. The touched count is written by the actor under
    /// `session_mu`, and read by the actor or by any task under `session_mu`.
    handlers: Touched(HandlerSlot),
    /// Untouched until a slot is first dispatched: dispatch assigns the whole
    /// job, so nothing is set up at connect.
    handler_jobs: []HandlerJob,
    /// Requests decoded but not yet released: waiting for dispatch, parked,
    /// or running (a lease is released with its handler slot). One more than
    /// twice the stream limit. Session decodes a block before it checks the
    /// stream limit (the HPACK table must advance either way), which is the
    /// one more; and the peer may open a stream as soon as it reads the
    /// previous one's END_STREAM while that stream's handler still holds its
    /// lease, which makes the other half. Written on first use, like
    /// `handlers`. Actor-only.
    request_leases: Touched(RequestLease),
    /// Stream id to lease, and the free leases. See `LeaseIndex`. Actor-only.
    lease_index: LeaseIndex,
    /// Where a header block is decoded when the pool has no memory for its
    /// request: sized for the largest decode (`Limits.hpackDecodeBytes`), so
    /// the decode cannot fail, and the HPACK table advances either way.
    /// Actor-only, under `session_mu`.
    hpack_scratch: std.heap.FixedBufferAllocator,
    /// Requests waiting for a handler slot, oldest first (a ring). The peer
    /// may open a stream as soon as it reads the previous one's END_STREAM,
    /// while that stream's handler is still running, so a slot can be busy
    /// for a stream inside the advertised limit. Such a request waits here
    /// instead of being refused. A parked request is an open stream, so the
    /// stream limit bounds this queue; the slots are never written before
    /// use.
    parked: []session_mod.DispatchRequest,
    parked_head: usize = 0,
    parked_len: usize = 0,
    shutting_down: bool = false,
    /// Set by a handler after it refills scheduler slab space; cleared by the
    /// actor at the top of each iteration. Emission happens only in the
    /// actor's drainEmit. The handler's release of `session_mu` (which rings,
    /// `unlockSession`) comes after the store, and the ring persists until the
    /// actor takes it, so the refill cannot be a lost wakeup.
    sched_refilled: std.atomic.Value(bool) = .init(false),
    /// Observe builds. Releases of `session_mu` by a task that is not the
    /// actor, counted twice by `unlockSession` (see `lostWakeSince`):
    /// `releases_started` after the unlock and BEFORE the ring (each release
    /// takes its own number from it), `releases_rung_max` AFTER the ring (the
    /// highest number that has finished its ring). Both `seq_cst`.
    releases_started: std.atomic.Value(usize) = .init(0),
    releases_rung_max: std.atomic.Value(usize) = .init(0),
    /// Actor only. `releases_started` at the top of the current turn.
    releases_at_turn_top: usize = 0,
    grace_deadline: ?std.Io.Timestamp = null,
    /// Set when the connection has decided to close and is delivering the
    /// frames it already queued: the time after which it stops waiting for the
    /// peer. Null while the connection is open. See `runBody` ("Closing phase").
    closing_deadline_ns: ?u64 = null,
    /// Closing phase, actor only: everything queued has reached the kernel and
    /// the send side is shut (`closeFlushed`), and the peer's EOF has arrived.
    closing_write_shut: bool = false,
    /// TLS: close_notify was staged in the closing phase (`closeFlushed`).
    closing_notify_staged: bool = false,
    /// Set by the first `materializeIntents`; see `unlockSession`.
    intents_materialized: bool = false,
    closing_peer_eof: bool = false,
    /// Production fair scheduler — sole emit path for controls + DATA.
    sched: fair_scheduler.FairScheduler = undefined,
    session_mu: std.Io.Mutex = .init,
    /// One-shot teardown event. Set once, never reset. Handlers select on
    /// this OR their work. Same shape as `ConnConfig.shutdown_event`.
    dead: zio.Event = .init,
    /// Debug proof that `session_mu` is held where Session or the scheduler is
    /// touched. Written only while the mutex is held, so it needs no atomic.
    ///
    /// It proves SOMEBODY holds the mutex, not that this task does. A thread id
    /// would be wrong: zio migrates tasks, and a holder can resume on another
    /// thread inside the critical section (a push that must wait releases the
    /// lock first: `waitForStreamSpace`, `waitForAckRoom`).
    /// That is still enough to catch the class it exists for — a call made with
    /// the mutex not held at all — which is exactly how the two sites in t-731
    /// read Session and scheduler state unsynchronized.
    session_held: bool = false,
    /// Phase-trace handoff for ONE sampled write at a time. The handler stores
    /// the ticket it is about to wait on; the actor stamps the completion
    /// time for that exact ticket when it applies the write ack. See `trace`.
    trace_ticket: std.atomic.Value(u64) = .init(0),
    trace_ack_ns: std.atomic.Value(u64) = .init(0),
    trace_emit_ns: std.atomic.Value(u64) = .init(0),
    trace_queued_ns: std.atomic.Value(u64) = .init(0),
    trace_written_ns: std.atomic.Value(u64) = .init(0),
    live_handlers: std.atomic.Value(usize) = .init(0),
    /// Spawned task handlers only. Complete oneshots stay on `live_handlers`
    /// and must not trip the ReadPump donation.
    live_task_handlers: std.atomic.Value(usize) = .init(0),
    reaper: ?*ReaperPool = null,
    completion_ch_buf: []u31 = &.{},
    completion_ch: zio.Channel(u31) = undefined,
    write_ack_buf: []wire_pump.WriteCompletion = &.{},
    write_ack_ch: zio.Channel(wire_pump.WriteCompletion) = undefined,
    /// h2c only: completions owed room in `write_ack_ch` (see `AckLedger`).
    /// Producers add under `session_mu`; the actor subtracts on receive.
    acks: wire_pump.AckLedger = .{},
    /// Set when the writer starts (the h2c WritePump, or the TLS driver): from
    /// then on a handler's bare flush barrier waits for room in `waitForAckRoom`.
    /// On h2c it also makes `h2cPause` hold a pop back when `write_ack_ch` has no
    /// room for it. A harness without a pump (`BenchHop`) leaves it false.
    ack_room_armed: bool = false,
    /// Handlers parked in `waitForAckRoom`. On h2c the actor wakes them when it
    /// takes a completion, on TLS the driver when it takes chunks off the
    /// stash; see that function for the ordering.
    ack_room_waiters: std.atomic.Value(u32) = .init(0),
    /// Set the first time a drain finds the h2c connection ending; then
    /// `h2cPause` also keeps the drain from parking on a full write queue.
    h2c_ending: bool = false,
    /// h2c: the write queue is being emptied of its meaning. Set, before
    /// anything else, by the two places that put something in or take
    /// something out of the queue behind the ledger's back: the teardown's
    /// sentinel (`stopWritePump`), which is not counted in `acks.owed`, and
    /// `closeWriterQueues`. While it is set `h2cQueueRoom` reads the exact
    /// length instead of trusting `owed`, and a full push is an ending
    /// connection's, not a failed gate. Never cleared.
    write_queue_ending: std.atomic.Value(bool) = .init(false),
    ticket_slots: []ticket_table.TicketWait = &.{},
    tickets: ticket_table.TicketTable = undefined,
    /// Indexed parallel to handlers; only valid while slot.in_use.
    handler_joins: []?zio.JoinHandle(void) = &.{},
    preface_deadline: ?std.Io.Timestamp = null,
    /// Connection-local outbound/request reservations — actor applies write acks under atomics.
    ///
    /// The budget counts DATA payload bytes a handler wrote, once, from the
    /// write until the ack of the chunk that carried them (see
    /// `tryReserveOutboundBytes`). `pending` is the part still waiting in the
    /// scheduler (or being framed in the drain that holds `session_mu`),
    /// `wire` the part handed to the write pump. Always
    /// `outbound_held == pending_outbound_held + wire_outbound_held`.
    outbound_held: std.atomic.Value(usize) = .init(0),
    pending_outbound_held: std.atomic.Value(usize) = .init(0),
    wire_outbound_held: std.atomic.Value(usize) = .init(0),
    /// FIFO of handlers waiting for budget (`BudgetWait`, linked through the
    /// handler slots). Lock order: `session_mu`, then `budget_mu`; the lock is
    /// never held across a wait and never takes `session_mu`, because wire
    /// releases run on the actor with no `session_mu` (h2c `drainWriteAcks`,
    /// TLS acks in the driver turn).
    budget_mu: zio.Mutex = .init,
    budget_head: u32 = budget_none,
    budget_tail: u32 = budget_none,
    /// Entries in the queue. A release reads it to skip `budget_mu` when no
    /// one waits; see `wakeBudgetWaiters` for the ordering.
    budget_waiters: std.atomic.Value(u32) = .init(0),
    /// Largest `outbound_held` seen, for `--close-reason`. Only kept while
    /// `close_reason` is on.
    outbound_high_water: std.atomic.Value(usize) = .init(0),
    /// Times a handler parked for budget, for `--close-reason`.
    budget_parks: std.atomic.Value(u32) = .init(0),
    /// What this connection's requests hold, in class bytes. Atomic because
    /// the handlers run on other executors. Shares `Limits.request_bytes_per_connection`.
    request_budget: request_pool.ConnBudget,
    handshake_held: bool = false,
    /// True once `queueWire` has seen the connection's first wire bytes.
    /// Written under `session_mu`, like every `queueWire` call.
    wire_started: bool = false,
    socket_closed: std.atomic.Value(bool) = .init(false),
    /// Set when a write completion reports failure; actor owns handler terminal transition.
    writer_failed: std.atomic.Value(bool) = .init(false),
    writer_fail_handled: bool = false,
    /// `--close-reason`: set by the first `noteServerClose`, so only the
    /// first site that ends the connection prints.
    close_reason_noted: bool = false,
    /// Handler writes refused for lack of connection outbound budget
    /// (`enqueuePending`). Each refusal fails one stream. Under `session_mu`.
    stream_budget_refusals: u32 = 0,
    /// Capacity waiters: one `zio.Event` per HandlerSlot (sparse IDs safe).
    space_events: []zio.Event = &.{},
    /// Time waiters. Occupancy and time are different waits; do not overload
    /// `space_events`. Cadence is a heap entry, not a handler timer.
    deadline_events: []zio.Event = &.{},
    /// Actor-owned intent batch — filled by drainIntentsInto (no nested Session drain).
    intent_batch: []session_mod.Intent = &.{},
    rates: rates_mod.RateLimiter = .{},
    /// Fixed inbound wire chunk pool.
    read_chunk_storage: []u8 = &.{},
    read_free_buf: []u32 = &.{},
    read_free_ch: std.Io.Queue(u32) = undefined,
    /// Write chunks, claimed lowest-free-bit. A FIFO free list rotated one
    /// in-flight chunk through all of them and faulted every chunk's pages on
    /// a connection that never had more than one write outstanding.
    write_pool: slab_pool.SlabPool = undefined,
    /// TLS-only: the CQ driver's single ciphertext read buffer (replaces the
    /// cipher chunk pool), and the outbound frame channel the driver selects
    /// on. `tls_write_ch` shares the write_ch capacity; only one of the two
    /// carries traffic per mode.
    tls_read_buf: []u8 = &.{},
    tls_write_buf: []wire_pump.WireChunk = &.{},
    tls_write_ch: zio.Channel(wire_pump.WireChunk) = undefined,
    /// Actor-owned TLS driver. Stack-allocated in `run`, pointed here so
    /// `waitForActivity` and `pushWriteChunk` can reach the CQ. Null on h2c
    /// and before `Pump.start`.
    tls_driver: ?*tls_edge.Pump = null,
    /// Diag: the pump's current blocking site (see `tls_edge.Pump.site`).
    tls_pump_site: std.atomic.Value(u8) = .init(0),
    /// Diag: where the ACTOR currently is. 1 loop, 2 waitForActivity,
    /// 3 acquiring session_mu, 4 putOne(write_ch), 5 waitH2cPreface,
    /// 6 shutdownHandlers.
    actor_site: std.atomic.Value(u8) = .init(0),
    /// Diagnosis (t-2655): actor waits entered; read twice, it shows whether
    /// the actor still turns.
    diag_actor_turns: std.atomic.Value(u64) = .init(0),
    /// Diagnosis (t-2655): the actor's current blocking point (`DiagActor`).
    diag_actor_where: std.atomic.Value(u8) = .init(0),
    /// Diagnosis (t-2655): TLS turns in a row that did not park.
    diag_no_park_run: u64 = 0,
    /// Diag: this connection is in `diag_reg` and must deregister on teardown.
    diag_registered: bool = false,
    /// Diag: opaque zio task handles for the actor and the pump.
    actor_task_h: std.atomic.Value(usize) = .init(0),
    tls_pump_task_h: std.atomic.Value(usize) = .init(0),
    /// Written by `run` before its first spawn and never again, so the tasks
    /// it spawns read it without a race. Only `notePlacement` reads it.
    actor_thread: std.Thread.Id = 0,
    frame_pool: slab_pool.SlabPool = undefined,
    /// Where RST_STREAM and GOAWAY frames are built: `Limits.terminalFrames`
    /// (T) fixed slots, each as big as the biggest terminal frame (a GOAWAY
    /// with no debug data). They never take a frame slab or a write chunk, so
    /// no other frame can leave a terminal frame without room. A slot is taken
    /// when the frame is built (`rentTerminal`, which refuses past the bound)
    /// and returned when the frame's bytes are copied out or written
    /// (`releaseWireBytes`).
    terminal_store: slab_pool.SlabPool = undefined,
    /// True while the scheduler's `buildData` callback makes a DATA frame, the
    /// one frame built inside a pop. See `rentFrame`. Written and read under
    /// `session_mu`.
    building_pop_frame: bool = false,
    /// The drain-turn scratch while a drain runs (set by `drainEmit` and
    /// `drainEmitReceipt`, under `session_mu`), so a small DATA frame built
    /// inside a pop is written where it will be sent (`rentFrame`) instead of
    /// into a pool chunk and copied. Null outside a drain.
    pack_scratch: ?*emit_batch.EmitBatch = null,
    /// Bytes `sendAccountedWire` copied into a write chunk. A frame that
    /// already sits in one is handed over instead, so a test reads this to
    /// prove the handover happened. Written under `session_mu`.
    wire_copied_bytes: usize = 0,
    read_pool_n: u32 = 0,
    /// Preallocated scratch for stream-id sweeps (no hot-path ArrayList).
    sid_scratch: []u31 = &.{},
    /// Complete handlers queued to run on the actor after `session_mu` drops.
    /// Pushed under the lock in `dispatch`; popped under the lock in
    /// `runPendingInline`.
    inline_sids: []u31 = &.{},
    inline_n: usize = 0,
    /// Frozen complete-job count from the last owned turn. `runPendingInline`
    /// must not run these: it takes `session_mu`.
    teardown_inline_n: usize = 0,
    /// `claimSlot` increments. Rollback `releaseSlot` increments `slots_rolled_back`.
    /// `finishHandlerJob` increments `slots_finalized`. deinit: claimed ==
    /// finalized + rolled_back.
    slots_claimed: usize = 0,
    slots_rolled_back: usize = 0,
    slots_finalized: std.atomic.Value(usize) = .init(0),
    /// In-use slots at the teardown wait, and how many `releaseSlot` ran.
    /// `released` counts in-use → free transitions only, never a stale SID.
    teardown_wait_expected: usize = 0,
    teardown_wait_released: usize = 0,
    /// Actor-owned drain turns whose receipt is attached but not yet
    /// consumed. Separate from `inline_sids`, which is the ready queue.
    pending_complete_receipts: [complete_receipt_capacity]PendingCompleteReceipt = .{ .{}, .{}, .{}, .{} },
    pending_complete_receipt_n: usize = 0,
    /// Complete-batch write acks stashed while `test_hold_complete_receipt_ack`
    /// is armed. Production never holds; the lifecycle test uses this so ingest
    /// of B proceeds while A's credit stays unpublished.
    held_acks: [complete_receipt_capacity]wire_pump.WriteCompletion = .{ .{}, .{}, .{}, .{} },
    held_ack_n: usize = 0,
    /// Set after `tickets.failAll`; actor may then consume every pending
    /// receipt, waiting via `waitTicketDraining` for any still-unsignaled
    /// slot. Ordinary reclaim does not use a ready-count: the front receipt's
    /// ticket event is the proof (t-900).
    complete_receipts_fail_all: std.atomic.Value(bool) = .init(false),
    /// Actor-only: when depth last reached capacity. Used for `receipt_full_ns`.
    receipt_depth_entered_ns: u64 = 0,
    /// Stamped when a complete-batch ticket is completed; popped on consume.
    receipt_ready_stamp: [complete_receipt_capacity]std.atomic.Value(u64) = .{ .init(0), .init(0), .init(0), .init(0) },
    receipt_ready_stamp_w: std.atomic.Value(usize) = .init(0),
    receipt_ready_stamp_r: usize = 0,
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

    /// The boot allocations of `init`, in its order, after the TLS `Conn` when
    /// the mode has one. See `boot_plan`; the exact-fit test at the end of this
    /// file is what keeps it equal to `init`.
    fn planBoot(plan: *boot_plan.Plan, limits: limits_mod.Limits, mode: Mode) error{InvalidConfig}!void {
        const max_streams = limits.max_streams_per_connection;
        const n_chunks = limits.inbound_wire_chunks_per_connection;
        // `init` narrows both counts to `u32`. The plan refuses a larger one
        // where the count is used: `SlabPool.planBoot` for the chunks and
        // `Plan.hashMap` for the streams.
        // Ticket owners are handlers and ordinary controls. A terminal frame
        // never reserves a ticket; it can only carry a handler's, which that
        // handler already counts.
        const ticket_n = try boot_plan.add(limits.control_entries_per_connection, max_streams);
        const ack_n = try boot_plan.add(limits.control_entries_per_connection, max_streams);
        const read_ch_n = try boot_plan.add(n_chunks, 1);
        const leases_n = try boot_plan.add(try boot_plan.mul(max_streams, 2), 1);
        const read_chunk_bytes = try boot_plan.mul(n_chunks, limits_mod.WIRE_CHUNK_SIZE);
        try session_mod.Session.planBoot(plan, limits);
        try plan.array(wire_pump.WireChunk, read_ch_n); // read_ch_buf
        try plan.array(wire_pump.WireChunk, n_chunks); // write_ch_buf
        try plan.array(HandlerSlot, max_streams);
        try plan.array(HandlerJob, max_streams);
        try plan.array(RequestLease, leases_n);
        try plan.array(u32, try LeaseIndex.slotCount(leases_n)); // lease_index.slots
        try plan.array(u32, leases_n); // lease_index.free
        try plan.array(u8, try limits.hpackScratchBytes()); // hpack_scratch
        try plan.array(session_mod.DispatchRequest, max_streams); // parked
        try plan.array(?zio.JoinHandle(void), max_streams);
        try plan.array(u31, max_streams); // completion_ch_buf
        try plan.array(wire_pump.WriteCompletion, ack_n);
        try plan.array(ticket_table.TicketWait, ticket_n);
        try plan.array(u8, emit_batch.max_plaintext);
        try plan.array(u8, read_chunk_bytes); // read_chunk_storage
        try plan.array(u32, n_chunks); // read_free_buf
        try slab_pool.SlabPool.planBoot(plan, limits_mod.WIRE_CHUNK_SIZE, n_chunks); // write_pool
        if (mode == .tls) {
            try plan.array(u8, limits_mod.TLS_CIPHER_CHUNK_SIZE);
            try plan.array(wire_pump.WireChunk, try boot_plan.add(n_chunks, tls_edge.Pump.extra_stash_slots));
        }
        try slab_pool.SlabPool.planBoot(plan, limits.frame_slab_bytes, limits.frame_slabs_per_connection); // frame_pool
        try slab_pool.SlabPool.planBoot(plan, frame.goaway_frame_bytes, try limits.terminalFrames()); // terminal_store
        try plan.array(u31, max_streams); // sid_scratch
        try plan.array(u31, max_streams); // inline_sids
        try plan.array(zio.Event, max_streams); // space_events
        try plan.array(zio.Event, max_streams); // deadline_events
        try plan.array(session_mod.Intent, @max(limits.intent_entries_per_connection, 16)); // intent_batch
        try fair_scheduler.FairScheduler.planBoot(
            plan,
            try limits.terminalFrames(),
            limits.control_entries_per_connection,
            max_streams,
        );
    }

    /// `boot` serves every allocation that lives as long as the connection and
    /// never changes size; `config.gpa` serves the rest. See the `boot` field.
    fn init(boot: std.mem.Allocator, stream: std.Io.net.Stream, config: ConnConfig) !Connection {
        // `Server.init` checks this at boot; a connection built any other way
        // gets the same answer, not a queue its drain can overrun.
        if (config.mode == .h2c) try config.limits.checkH2cWriteQueue();
        try config.limits.checkFramePool();
        try config.limits.checkIntentQueue();
        if (config.request_pool == null) return error.RequestPoolMissing;
        var session = try session_mod.Session.initBare(boot, config.gpa, config.limits);
        errdefer {
            // Hooks are installed below with `ctx` still unset, and no frame
            // exists yet (the preface is queued by `finishBoot`), so deinit has
            // nothing to give back through `releaseFrame`.
            session.stream_hooks = null;
            session.deinit();
        }
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
            .headerListMem = struct {
                fn f(ctx: *anyopaque, stream_id: u31) session_mod.HeaderListMem {
                    const c: *Connection = @ptrCast(@alignCast(ctx));
                    return c.borrowHeaderListMem(stream_id);
                }
            }.f,
            .requestAlloc = struct {
                fn f(ctx: *anyopaque, stream_id: u31) std.mem.Allocator {
                    const c: *Connection = @ptrCast(@alignCast(ctx));
                    const lease = c.findLease(stream_id) orelse
                        std.debug.panic("body for stream {d} with no request memory", .{stream_id});
                    return lease.arena.allocator();
                }
            }.f,
            .memoryRefusalStatus = struct {
                fn f(ctx: *anyopaque, stream_id: u31) u16 {
                    const c: *Connection = @ptrCast(@alignCast(ctx));
                    const lease = c.findLease(stream_id) orelse return 503;
                    return (lease.arena.refusal orelse request_pool.Reason.server_cap).status();
                }
            }.f,
            .discardHeaderList = struct {
                fn f(ctx: *anyopaque, stream_id: u31) void {
                    const c: *Connection = @ptrCast(@alignCast(ctx));
                    c.releaseHeaderList(stream_id);
                }
            }.f,
            .rentFrame = struct {
                fn f(ctx: *anyopaque, n: usize, class: session_mod.FrameClass) ?[]u8 {
                    const c: *Connection = @ptrCast(@alignCast(ctx));
                    return c.rentFrame(n, class);
                }
            }.f,
            .releaseFrame = struct {
                fn f(ctx: *anyopaque, buf: []u8) void {
                    const c: *Connection = @ptrCast(@alignCast(ctx));
                    c.releaseWireBytes(buf);
                }
            }.f,
            .retireRst = struct {
                fn f(ctx: *anyopaque, stream_id: u31) void {
                    const c: *Connection = @ptrCast(@alignCast(ctx));
                    const dropped = c.sched.retireTerminalRst(stream_id);
                    if (comptime test_observe) {
                        if (dropped) _ = test_rst_retired.fetchAdd(1, .monotonic);
                    }
                }
            }.f,
            .requestBodyCap = struct {
                fn f(ctx: *anyopaque, method: []const u8, path: []const u8) usize {
                    const c: *Connection = @ptrCast(@alignCast(ctx));
                    const matched = c.config.router.match(request.Method.parse(method), path);
                    return switch (matched) {
                        .found => |found| found.max_request_body_bytes orelse c.config.limits.request_body_bytes,
                        else => c.config.limits.request_body_bytes,
                    };
                }
            }.f,
        };
        // + 1: the EOF sentinel must fit even with every pool chunk queued.
        const read_ch_buf = try boot.alloc(wire_pump.WireChunk, config.limits.inbound_wire_chunks_per_connection + 1);
        errdefer boot.free(read_ch_buf);
        const write_ch_buf = try boot.alloc(wire_pump.WireChunk, config.limits.inbound_wire_chunks_per_connection);
        errdefer boot.free(write_ch_buf);
        const handlers = try boot.alloc(HandlerSlot, config.limits.max_streams_per_connection);
        errdefer boot.free(handlers);
        const handler_jobs = try boot.alloc(HandlerJob, config.limits.max_streams_per_connection);
        errdefer boot.free(handler_jobs);
        const request_leases = try boot.alloc(RequestLease, config.limits.max_streams_per_connection * 2 + 1);
        errdefer boot.free(request_leases);
        const lease_slots = try boot.alloc(u32, try LeaseIndex.slotCount(request_leases.len));
        errdefer boot.free(lease_slots);
        const lease_free = try boot.alloc(u32, request_leases.len);
        errdefer boot.free(lease_free);
        const hpack_scratch = try boot.alloc(u8, try config.limits.hpackScratchBytes());
        errdefer boot.free(hpack_scratch);
        const parked = try boot.alloc(session_mod.DispatchRequest, config.limits.max_streams_per_connection);
        errdefer boot.free(parked);
        const handler_joins = try boot.alloc(?zio.JoinHandle(void), config.limits.max_streams_per_connection);
        errdefer boot.free(handler_joins);
        const completion_ch_buf = try boot.alloc(u31, config.limits.max_streams_per_connection);
        errdefer boot.free(completion_ch_buf);
        const write_ack_buf = try boot.alloc(wire_pump.WriteCompletion, config.limits.control_entries_per_connection + config.limits.max_streams_per_connection);
        errdefer boot.free(write_ack_buf);
        const ticket_slots = try boot.alloc(ticket_table.TicketWait, config.limits.control_entries_per_connection + config.limits.max_streams_per_connection);
        errdefer boot.free(ticket_slots);
        const plaintext_scratch = try boot.alloc(u8, emit_batch.max_plaintext);
        errdefer boot.free(plaintext_scratch);
        const n_chunks: u32 = @intCast(config.limits.inbound_wire_chunks_per_connection);
        const read_chunk_storage = try boot.alloc(u8, @as(usize, n_chunks) * limits_mod.WIRE_CHUNK_SIZE);
        errdefer boot.free(read_chunk_storage);
        const read_free_buf = try boot.alloc(u32, n_chunks);
        errdefer boot.free(read_free_buf);
        var write_pool = try slab_pool.SlabPool.init(boot, config.io, limits_mod.WIRE_CHUNK_SIZE, n_chunks);
        errdefer write_pool.deinit(boot);

        var tls_read_buf: []u8 = &.{};
        var tls_write_buf: []wire_pump.WireChunk = &.{};
        if (config.mode == .tls) {
            tls_read_buf = try boot.alloc(u8, limits_mod.TLS_CIPHER_CHUNK_SIZE);
            errdefer boot.free(tls_read_buf);
            tls_write_buf = try boot.alloc(wire_pump.WireChunk, config.limits.inbound_wire_chunks_per_connection + tls_edge.Pump.extra_stash_slots);
            errdefer boot.free(tls_write_buf);
        }
        var frame_pool = try slab_pool.SlabPool.init(
            boot,
            config.io,
            config.limits.frame_slab_bytes,
            config.limits.frame_slabs_per_connection,
        );
        errdefer frame_pool.deinit(boot);
        const terminal_frames = try config.limits.terminalFrames();
        var terminal_store = try slab_pool.SlabPool.init(boot, config.io, frame.goaway_frame_bytes, terminal_frames);
        errdefer terminal_store.deinit(boot);
        const sid_scratch = try boot.alloc(u31, config.limits.max_streams_per_connection);
        errdefer boot.free(sid_scratch);
        const inline_sids = try boot.alloc(u31, config.limits.max_streams_per_connection);
        errdefer boot.free(inline_sids);
        const space_events = try boot.alloc(zio.Event, config.limits.max_streams_per_connection);
        errdefer boot.free(space_events);
        const deadline_events = try boot.alloc(zio.Event, config.limits.max_streams_per_connection);
        errdefer boot.free(deadline_events);
        const intent_batch = try boot.alloc(session_mod.Intent, @max(config.limits.intent_entries_per_connection, 16));
        errdefer boot.free(intent_batch);

        if (config.mode == .tls) {
            if (config.tls_acceptor == null) return error.InvalidConfig;
        }

        var sched = try fair_scheduler.FairScheduler.init(
            boot,
            config.io,
            config.limits.control_bytes_per_connection,
            config.limits.control_entries_per_connection,
            try config.limits.terminalBytes(),
            terminal_frames,
            config.limits.max_streams_per_connection,
            config.slab_pool,
        );
        errdefer sched.deinit();
        // One number sizes all three, and a construction that ever stops doing
        // so must not boot: the bound `Limits.terminalFrames` derives holds
        // only while the ring, the pool's terminal share and the store agree.
        if (sched.terminal_q.len != terminal_frames or sched.ctrl.terminal_entries != terminal_frames or
            terminal_store.capacity != terminal_frames) return error.InvalidConfig;

        var self: Connection = .{
            .stream = stream,
            .config = config,
            .boot = boot,
            .session = session,
            .read_ch_buf = read_ch_buf,
            .write_ch_buf = write_ch_buf,
            .handlers = .init(handlers),
            .handler_jobs = handler_jobs,
            .request_leases = .init(request_leases),
            .lease_index = .init(lease_slots, lease_free),
            .hpack_scratch = .init(hpack_scratch),
            .request_budget = .init(config.limits.request_bytes_per_connection),
            .parked = parked,
            .handler_joins = handler_joins,
            .completion_ch_buf = completion_ch_buf,
            .write_ack_buf = write_ack_buf,
            .ticket_slots = ticket_slots,
            .reaper = config.reaper,
            .plaintext_scratch = plaintext_scratch,
            .sched = sched,
            .read_chunk_storage = read_chunk_storage,
            .read_free_buf = read_free_buf,
            .write_pool = write_pool,
            .tls_read_buf = tls_read_buf,
            .tls_write_buf = tls_write_buf,
            .frame_pool = frame_pool,
            .terminal_store = terminal_store,
            .read_pool_n = n_chunks,
            .sid_scratch = sid_scratch,
            .inline_sids = inline_sids,
            .space_events = space_events,
            .deadline_events = deadline_events,
            .intent_batch = intent_batch,
        };
        self.tickets = ticket_table.TicketTable.init(config.io, self.ticket_slots);
        self.read_ch = .init(self.read_ch_buf);
        self.write_ch = .init(self.write_ch_buf);
        self.completion_ch = .init(self.completion_ch_buf);
        self.write_ack_ch = .init(self.write_ack_buf);
        self.read_free_ch = .init(self.read_free_buf);
        if (self.tls_write_buf.len != 0) {
            self.tls_write_ch = .init(self.tls_write_buf);
        }
        return self;
    }

    /// Hook wiring and free-list seed that `serveAccepted` and `BenchHop.open`
    /// both need after `init`. Channels only have their final address here.
    ///
    /// The server preface is queued here, not in `Session.initBare`, because
    /// its frames come from this connection's frame pool and so need the hooks
    /// that were just bound. Nothing else is queued before it, which keeps the
    /// SETTINGS first on the wire (RFC 9113 section 3.4). It fails only if a
    /// limit leaves the connection without a slab or a chunk for two small
    /// frames at birth, and then the connection is not served.
    fn finishBoot(self: *Connection) !void {
        // The doorbell buffer is an inline field, so the channel must bind it
        // here, at the struct's final address (init returns by value).
        self.doorbell = .init(&self.doorbell_buf);
        // Same init-move hazard: `&self.dead` inside `init` dangles after the
        // return-move. Bind at the final address, like the doorbell.
        self.tickets.dead = &self.dead;
        if (self.session.stream_hooks != null) {
            self.session.stream_hooks.?.ctx = self;
        }
        self.session.rate_limiter = &self.rates;
        self.handshake_held = self.config.handshake_held;
        self.sched.payload_free_ctx = self;
        self.sched.payload_free = struct {
            fn f(ctx: *anyopaque, payload: []u8) void {
                const c: *Connection = @ptrCast(@alignCast(ctx));
                c.releaseWireBytes(payload);
            }
        }.f;
        self.sched.ticket_fail_ctx = self;
        self.sched.ticket_fail = struct {
            fn f(ctx: *anyopaque, ticket: u64, slot: u32) void {
                const c: *Connection = @ptrCast(@alignCast(ctx));
                c.tickets.wake(slot, ticket, false);
            }
        }.f;
        // Seed read/write-chunk free lists after channels are live.
        var i: u32 = 0;
        while (i < self.read_pool_n) : (i += 1) {
            self.read_free_ch.putOneUncancelable(self.config.io, i) catch {
                // Fresh queue capacity exactly matches the number of pool indices.
                unreachable;
            };
        }
        try self.session.emitServerPreface();
    }

    /// Release everything, in an order that cannot double-free: `quiesce`,
    /// then free the boot memory. For a connection built outside a slot; a slot
    /// calls `quiesce` and rewinds its buffer instead.
    fn deinit(self: *Connection) void {
        self.quiesce();
        self.freeBoot();
    }

    /// Everything `deinit` does except free the boot memory: end the
    /// connection's use of the server (handshake slot, streams, slabs, reaper
    /// tokens, socket) and prove the books balance. After it returns the
    /// connection holds nothing outside its own boot memory, and the slot may
    /// rewind that. Do not use the connection afterwards.
    ///
    /// Every channel is drained BEFORE its backing buffer is released. Queued
    /// read chunks return their pool index; queued write chunks return a
    /// write-pool index or free a GPA fallback, then release wire bytes.
    /// Write-ack leftovers are NOT drained here: `drainWriteAcksForced` is
    /// the sole owner (t-894). A second drain absorbed skipping that one.
    ///
    /// The completion drain uses `releaseSlot` and never discards. A reaper can
    /// post after `shutdownHandlers` returned, and a discarded completion would
    /// leak the reaper token.
    ///
    /// The closing asserts are the leak detector: the two outbound parts must
    /// sum to the whole, and both must be zero. A mismatch means some path
    /// released the wrong kind, which no test would otherwise notice.
    fn quiesce(self: *Connection) void {
        const io = self.config.io;
        // Must not release while handlers still live.
        const live_n = self.live_handlers.load(.acquire);
        if (std.debug.runtime_safety and live_n != 0) {
            std.debug.panic("deinit with live_handlers={d}", .{live_n});
        }
        std.debug.assert(self.live_task_handlers.load(.acquire) == 0);
        if (self.handshake_held) {
            if (self.config.accounting) |a| a.releaseHandshake();
            self.handshake_held = false;
        }
        // Drain channels before their backing buffers are released.
        while (io_queue.tryGet(wire_pump.WireChunk, &self.write_ch, io)) |chunk| {
            if (chunk.pool_index) |idx| {
                self.write_pool.releaseIndex(io, idx);
            } else if (chunk.bytes.len != 0) {
                self.config.gpa.free(chunk.bytes);
            }
            // Release amounts without a live write-ack (teardown path).
            self.applyOutboundRelease(chunk.outbound_release, .wire);
            if (chunk.control_entries != 0) self.applyControlRelease(chunk.control_release, chunk.control_entries);
        }
        while (io_queue.tryRecv(wire_pump.WireChunk, &self.read_ch)) |chunk| {
            if (chunk.pool_index) |idx| {
                self.read_free_ch.putOneUncancelable(io, idx) catch {
                    // Every queued read chunk owns one removed pool index.
                    unreachable;
                };
            } else if (chunk.bytes.len != 0) {
                self.config.gpa.free(chunk.bytes);
            }
        }
        if (self.tls_write_buf.len != 0) {
            // Chunks the driver never took out of the channel. Same
            // releases as the write_ch drain above.
            while (true) {
                const chunk = self.tls_write_ch.tryReceive() catch break;
                if (chunk.pool_index) |idx| {
                    self.write_pool.releaseIndex(io, idx);
                } else if (chunk.bytes.len != 0) {
                    self.config.gpa.free(chunk.bytes);
                }
                self.applyOutboundRelease(chunk.outbound_release, .wire);
                if (chunk.control_entries != 0) {
                    self.applyControlRelease(chunk.control_release, chunk.control_entries);
                }
            }
        }
        // Late reaper posts can arrive after shutdownHandlers; never discard without releaseSlot.
        while (io_queue.tryRecv(u31, &self.completion_ch)) |sid| {
            _ = self.releaseSlot(sid);
        }
        if (std.debug.runtime_safety) {
            const claimed = self.slots_claimed;
            const finalized = self.slots_finalized.load(.acquire);
            const rolled = self.slots_rolled_back;
            if (claimed != finalized + rolled) {
                std.debug.panic(
                    "slot conservation: claimed={d} finalized={d} rolled_back={d}",
                    .{ claimed, finalized, rolled },
                );
            }
            if (self.teardown_wait_expected != 0) {
                if (self.teardown_wait_expected != self.teardown_wait_released) {
                    std.debug.panic(
                        "teardown completion conservation: expected={d} released={d}",
                        .{ self.teardown_wait_expected, self.teardown_wait_released },
                    );
                }
            }
        }
        for (self.handlers.slice()) |s| {
            if (s.in_use) {
                std.debug.panic("deinit with slot sid={d} still in_use", .{s.stream_id});
            }
        }
        self.assertWriteAcksDrained();

        self.session.releaseRuntime();
        const RelCtx = struct {
            c: *Connection,
            fn cb(ctx: *anyopaque, _: u31, pw: *fair_scheduler.DataPending) void {
                const rc: *@This() = @ptrCast(@alignCast(ctx));
                rc.c.applyOutboundRelease(pw.len, .pending);
            }
        };
        var rel: RelCtx = .{ .c = self };
        self.sched.forEachPending(@ptrCast(&rel), RelCtx.cb);
        // Snapshot the slot ledger BEFORE `sched.deinit` frees `pending_slots`.
        // The leak detector below used to walk that array after the free, so its
        // numbers were read out of freed memory: that is where the "integer
        // overflow" in the leak panic came from, not from a real slot length.
        var leak_slot_bytes: usize = 0;
        var leak_slot_n: usize = 0;
        var leak_slot_max: usize = 0;
        for (self.sched.pending_slots.slice()) |ps| {
            if (ps.stream_id == 0) continue;
            leak_slot_bytes +|= ps.len;
            if (ps.len > leak_slot_max) leak_slot_max = ps.len;
            leak_slot_n += 1;
        }
        self.sched.releaseHeld();
        self.frame_pool.assertAllReturned();
        self.terminal_store.assertAllReturned();
        self.write_pool.assertAllReturned();
        if (self.tls) |tls_conn| {
            // The slot (or whoever created it) owns the memory.
            tls_conn.deinit();
            self.tls = null;
        }

        // The sweep released every parked request; a request still here would
        // be a teardown path that skipped it.
        std.debug.assert(self.parked_len == 0);
        // Session is gone, so a lease left here is a request it never handed
        // to dispatch nor dropped (a teardown with HEADERS still pending).
        for (self.request_leases.slice()) |*l| {
            if (l.stream_id != 0) self.releaseLease(l);
        }
        // Every waiter leaves the queue before its handler returns, and every
        // handler is gone by now. A name left here is a grant nobody returned.
        if (self.budget_waiters.load(.acquire) != 0 or self.budget_head != budget_none) {
            std.debug.panic("budget waiters at deinit: waiters={d}", .{self.budget_waiters.load(.acquire)});
        }
        const held = self.outbound_held.load(.acquire);
        const pending_held = self.pending_outbound_held.load(.acquire);
        const wire_held = self.wire_outbound_held.load(.acquire);
        if (held != pending_held + wire_held or held != 0) {
            // The ledger, before dying: which side leaked and how much. The slot
            // figures are the snapshot taken above, while `pending_slots` was
            // still allocated. `data_release` is the deferred-quantum side
            // channel: non-zero here means a quantum left the slab and nothing
            // paid it back, which is a different fault from bytes still sitting
            // in slots.
            std.debug.panic(
                "outbound leak at deinit: held={d} pending={d} wire={d} sched_slots={d} sched_bytes={d} slot_max={d} data_release={d} rel_posted={d} rel_applied={d} ack_q={d} held_acks={d} write_q={d} read_q={d}",
                .{
                    held,
                    pending_held,
                    wire_held,
                    leak_slot_n,
                    leak_slot_bytes,
                    leak_slot_max,
                    self.pending_data_outbound_release,
                    wire_pump.diag_acks.posted_release.load(.acquire),
                    wire_pump.diag_acks.applied_release.load(.acquire),
                    io_queue.chanLen(wire_pump.WriteCompletion, &self.write_ack_ch),
                    self.held_ack_n,
                    io_queue.queued(wire_pump.WireChunk, &self.write_ch, self.config.io),
                    io_queue.chanLen(wire_pump.WireChunk, &self.read_ch),
                },
            );
        }
        if (!self.socket_closed.load(.acquire)) {
            self.stream.close(io);
            self.socket_closed.store(true, .release);
            if (comptime test_observe) noteTestSocketClose();
        }
    }

    /// Leave one condition `dirtiness` checks set, for a test.
    fn dirtyOnPurpose(self: *Connection, kind: DirtyKind) void {
        switch (kind) {
            .write_chunk_rented => _ = self.write_pool.tryRentIndex(self.config.io),
            .request_bytes_reserved => _ = self.request_budget.held.fetchAdd(1, .acq_rel),
            .stream_still_admitted => self.session.active_streams += 1,
            // Only the "is it null" of the join is read, never the handle.
            .handler_join_held => self.handler_joins[0] = @as(zio.JoinHandle(void), undefined),
            .completion_queued => self.completion_ch.trySend(1) catch unreachable,
        }
    }

    /// Why this connection, after `quiesce`, still must not have its slot
    /// reused; null when it left nothing behind.
    ///
    /// Judged by what a stranger could still touch, not by what the teardown
    /// meant to do. Most conditions are a second net behind `quiesce`, which
    /// stops on them first; four are not checked by `quiesce` at all, so only
    /// this sees them. For each condition, whether `quiesce` can return with
    /// it set (in a safe build, the only build this runs in):
    ///
    /// - Unreachable, `quiesce` panics or asserts first: live handlers, live
    ///   task handlers, a handler slot in use, outbound bytes reserved (all
    ///   three counters), a rented write-pool chunk, a rented frame-pool slab,
    ///   a parked request, undrained write acks.
    /// - Unreachable, `quiesce` clears or empties it: a held handshake slot
    ///   (released), an open socket (closed last), the read, write and TLS
    ///   write channels (drained first), a queued scheduler frame and a held
    ///   data slab (`releaseHeld`), a request lease (cleared).
    /// - Reachable only through a bookkeeping bug, because `quiesce` does not
    ///   check it: request bytes still reserved (`request_budget`), streams
    ///   still admitted (`session.active_streams`), a handler join still held
    ///   (`handler_joins`), and the completion channel (a reaper can post to
    ///   it from another thread after the drain). No known path sets any of
    ///   them. These are what this check is for, and each has a test that
    ///   leaves it set (`DirtyKind`).
    pub fn dirtiness(self: *Connection) ?[]const u8 {
        if (self.live_handlers.load(.acquire) != 0) return "handler still live";
        if (self.live_task_handlers.load(.acquire) != 0) return "task handler still live";
        for (self.handlers.slice()) |h| {
            if (h.in_use) return "handler slot still in use";
        }
        for (self.handlers.slice(), 0..) |_, i| {
            if (self.handler_joins[i] != null) return "handler join still held";
        }
        if (self.write_pool.rentedCount() != 0) return "write pool chunk still rented";
        if (self.frame_pool.rentedCount() != 0) return "frame pool slab still rented";
        if (self.terminal_store.rentedCount() != 0) return "terminal frame store slot still rented";
        if (self.sched.terminal_len != 0 or self.sched.ordinary_len != 0 or self.sched.framed_len != 0) {
            return "scheduler still holds a queued frame";
        }
        for (self.sched.pending_slots.slice()) |ps| {
            if (ps.slab.len != 0) return "scheduler still holds a data slab";
        }
        if (self.parked_len != 0) return "parked request";
        for (self.request_leases.slice()) |l| {
            if (l.stream_id != 0) return "request lease still held";
        }
        if (self.outbound_held.load(.acquire) != 0 or
            self.pending_outbound_held.load(.acquire) != 0 or
            self.wire_outbound_held.load(.acquire) != 0) return "outbound bytes still reserved";
        if (self.budget_waiters.load(.acquire) != 0 or self.budget_head != budget_none) return "budget waiter still queued";
        if (self.request_budget.held.load(.acquire) != 0) return "request bytes still reserved";
        if (self.session.active_streams != 0) return "streams still admitted";
        if (self.handshake_held) return "handshake slot still held";
        if (!self.socket_closed.load(.acquire)) return "socket still open";
        if (io_queue.chanLen(wire_pump.WireChunk, &self.read_ch) != 0) return "read channel not empty";
        if (io_queue.queued(wire_pump.WireChunk, &self.write_ch, self.config.io) != 0) return "write channel not empty";
        if (self.tls_write_buf.len != 0 and io_queue.chanLen(wire_pump.WireChunk, &self.tls_write_ch) != 0) {
            return "tls write channel not empty";
        }
        if (io_queue.chanLen(u31, &self.completion_ch) != 0) return "completion channel not empty";
        if (io_queue.chanLen(wire_pump.WriteCompletion, &self.write_ack_ch) != 0) return "write ack channel not empty";
        return null;
    }

    /// Free the boot memory one allocation at a time, for a connection that
    /// was not booted into a slot. `quiesce` has run.
    fn freeBoot(self: *Connection) void {
        const boot = self.boot;
        self.sched.deinit();
        self.terminal_store.deinit(boot);
        self.frame_pool.deinit(boot);
        self.session.freeBoot();
        boot.free(self.read_ch_buf);
        boot.free(self.write_ch_buf);
        boot.free(self.handlers.storage);
        boot.free(self.handler_jobs);
        boot.free(self.parked);
        boot.free(self.request_leases.storage);
        boot.free(self.lease_index.slots);
        boot.free(self.lease_index.free);
        boot.free(self.hpack_scratch.buffer);
        boot.free(self.handler_joins);
        boot.free(self.completion_ch_buf);
        if (self.write_ack_buf.len != 0) boot.free(self.write_ack_buf);
        if (self.ticket_slots.len != 0) boot.free(self.ticket_slots);
        if (self.plaintext_scratch.len != 0) boot.free(self.plaintext_scratch);
        if (self.read_chunk_storage.len != 0) boot.free(self.read_chunk_storage);
        if (self.read_free_buf.len != 0) boot.free(self.read_free_buf);
        self.write_pool.deinit(boot);
        if (self.tls_read_buf.len != 0) boot.free(self.tls_read_buf);
        if (self.tls_write_buf.len != 0) boot.free(self.tls_write_buf);
        if (self.sid_scratch.len != 0) boot.free(self.sid_scratch);
        if (self.inline_sids.len != 0) boot.free(self.inline_sids);
        if (self.space_events.len != 0) boot.free(self.space_events);
        if (self.deadline_events.len != 0) boot.free(self.deadline_events);
        if (self.intent_batch.len != 0) boot.free(self.intent_batch);
    }

    const OutboundKind = enum { pending, wire };

    /// Why a reservation was not made. The two differ in what a handler may do
    /// about them: a full connection budget is backpressure (park, a release
    /// will come), the server cap is overload (refuse, nothing on this
    /// connection will ever make room for it).
    const ReserveResult = enum { ok, connection_full, server_full };

    /// The budget counts DATA payload bytes a handler wrote, ONCE. The bytes
    /// are reserved here, at the handler's write (`enqueuePending`), as
    /// `pending`; the drain TRANSFERS them to `wire` when the chunk that
    /// carries them goes to the pump (`transferOutbound`, no reserve); the
    /// ack of that chunk releases them. Frame headers, HEADERS, CONTINUATION
    /// and control frames are not charged: the control pool, the frame pool
    /// and the write pool already bound them.
    ///
    /// A byte is not charged again when its frame moves to the wire. The move
    /// changes neither the total nor the server counter, so it cannot fail,
    /// and another connection cannot take the gap that a "release, then
    /// reserve" move would leave.
    fn tryReserveOutboundBytes(self: *Connection, n: usize, kind: OutboundKind) ReserveResult {
        if (n == 0) return .ok;
        const lim = self.config.limits.outbound_bytes_per_connection;
        while (true) {
            const cur = self.outbound_held.load(.seq_cst);
            if (cur > lim or lim - cur < n) return .connection_full;
            if (self.config.accounting) |a| {
                if (!a.tryReserveOutbound(n)) return .server_full;
            }
            if (self.outbound_held.cmpxchgWeak(cur, cur + n, .seq_cst, .seq_cst) == null) {
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
                if (close_reason) self.noteOutboundHighWater(cur + n);
                return .ok;
            }
            if (self.config.accounting) |a| a.releaseOutbound(n);
        }
    }

    fn noteOutboundHighWater(self: *Connection, held: usize) void {
        var hw = self.outbound_high_water.load(.monotonic);
        while (held > hw) hw = self.outbound_high_water.cmpxchgWeak(hw, held, .monotonic, .monotonic) orelse break;
    }

    /// Move `n` bytes from `pending` to `wire`: the bytes left the scheduler
    /// for a chunk the pump now owns. The total and the server counter do not
    /// change, so this never reserves and never fails.
    fn transferOutbound(self: *Connection, n: usize) void {
        if (n == 0) return;
        const prev = self.pending_outbound_held.fetchSub(n, .acq_rel);
        if (std.debug.runtime_safety and prev < n) {
            std.debug.panic("outbound over-transfer: n={d} pending_before={d} total_held={d}", .{ n, prev, self.outbound_held.load(.acquire) });
        }
        _ = self.wire_outbound_held.fetchAdd(n, .acq_rel);
    }

    fn applyOutboundRelease(self: *Connection, n: usize, kind: OutboundKind) void {
        if (n == 0) return;
        const kind_prev = switch (kind) {
            .pending => self.pending_outbound_held.fetchSub(n, .acq_rel),
            .wire => self.wire_outbound_held.fetchSub(n, .acq_rel),
        };
        // Name the ledger before trapping. A bare assert says only that the
        // counter went negative, which is the one thing already obvious from the
        // crash; the amounts and the kind are what identify WHICH release was
        // doubled. Gated on `runtime_safety` so this stays exactly as absent from
        // ReleaseFast as the assert it precedes.
        if (std.debug.runtime_safety and kind_prev < n) {
            std.debug.panic(
                "outbound over-release: kind={s} n={d} kind_held_before={d} total_held={d}",
                .{ @tagName(kind), n, kind_prev, self.outbound_held.load(.acquire) },
            );
        }
        std.debug.assert(kind_prev >= n);
        const prev = self.outbound_held.fetchSub(n, .seq_cst);
        std.debug.assert(prev >= n);
        if (self.config.accounting) |a| a.releaseOutbound(n);
        // The one function every release passes through (drain failure, ack,
        // slow consumer, reset, writer failure, teardown), so no release site
        // can forget to hand the room to a waiter.
        self.wakeBudgetWaiters();
    }

    /// A release just made room: give it to the waiters, in arrival order.
    ///
    /// No lost wake. The waiter queues itself (`budget_waiters.fetchAdd`) and
    /// then reads `outbound_held` (the grant attempt in `parkForBudget`); the
    /// releaser lowers `outbound_held` and then reads `budget_waiters`. All
    /// four are seq_cst, so they have one total order that keeps each side's
    /// program order: either the waiter's read sees the release (it takes the
    /// room itself) or the releaser's read sees the waiter (it takes
    /// `budget_mu` and hands the room over). The count read is the whole
    /// cost of a release while no one waits.
    fn wakeBudgetWaiters(self: *Connection) void {
        if (self.budget_waiters.load(.seq_cst) == 0) return;
        self.budget_mu.lockUncancelable();
        defer self.budget_mu.unlock();
        self.grantBudgetLocked(budget_none);
    }

    /// Hand reservations to the head of the queue while its whole `need`
    /// fits, FIFO. A grant is a reservation made for the waiter (connection
    /// and server counters), so the woken handler finds its bytes taken and
    /// no one can take them first. Wake only the waiter that got something:
    /// no wake-all, so a release does not start a herd. `budget_mu` held.
    ///
    /// `admitting` is the slot index of an inline entry that is being admitted
    /// by the caller in this same critical section, or `budget_none`. Its
    /// grant is taken by the caller on return, so it does not ring the
    /// doorbell; a grant made later, by a release, does.
    fn grantBudgetLocked(self: *Connection, admitting: u32) void {
        while (self.budget_head != budget_none) {
            const i = self.budget_head;
            const slot = &self.handlers.storage[i];
            std.debug.assert(slot.budget.state == .queued);
            const outcome = self.tryReserveOutboundBytes(slot.budget.need, .pending);
            if (outcome == .connection_full) return;
            // `server_full` fails the head here. The server counter does not
            // move for a release on this connection alone, so leaving the
            // head queued would strand it.
            slot.budget.state = if (outcome == .ok) .granted else .refused;
            self.budget_head = slot.budget.next;
            if (self.budget_head == budget_none) self.budget_tail = budget_none;
            slot.budget.next = budget_none;
            _ = self.budget_waiters.fetchSub(1, .seq_cst);
            if (comptime test_observe) {
                const n = test_budget_grants.fetchAdd(1, .monotonic);
                test_budget_grant_log[n % test_budget_grant_log.len].store(slot.stream_id, .monotonic);
            }
            if (!slot.budget.is_inline) {
                self.space_events[i].set();
            } else if (i != admitting) {
                // KEEP. A release of wire bytes runs on the actor with no
                // `session_mu` (h2c `drainWriteAcks`, TLS `ack_apply`), so no
                // unlock follows it and nothing else tells the actor that an
                // inline entry was granted. A grant made by a handler's release
                // of pending bytes is also followed by that handler's unlock;
                // the ring here covers both, and a second token coalesces.
                if (comptime test_observe) _ = test_budget_doorbell_rings.fetchAdd(1, .monotonic);
                self.ring();
            }
        }
    }

    fn applyControlRelease(self: *Connection, n: usize, entries: u32) void {
        self.sched.ctrl.release(n, @as(usize, entries));
    }

    /// Apply one WritePump completion on the actor.
    ///
    /// WritePump stays a pure byte mover: it reports what it did, and the actor
    /// applies the consequences. Folding this into WritePump delayed the next
    /// `getOne` and dropped TLS throughput; a third task for the same work
    /// overlapped the next write at the cost of a hop before credit reuse.
    ///
    /// Per completion, in this order:
    /// 1. Release the WIRE bytes and any control-pool occupancy. This must
    ///    precede any waiter wake, or a woken waiter re-checks capacity that
    ///    is not free yet and parks again.
    /// 2. `fail_all` -> mark the writer failed and fail every in-flight ticket.
    ///    Note what it does NOT do: it never touches `handlers[]`. Terminal
    ///    causes stay under `session_mu` on a later step of this same turn.
    /// 3. Otherwise complete every linked ticket the chunk carried.
    /// 4. Completing a complete-batch ticket sets its event. That event IS
    ///    the ready signal: `drainPendingCompleteReceipts` probes
    ///    `tickets.isSignaled` on the front pending receipt. There is no
    ///    side-channel count (t-900: a count can exceed truly-applied acks
    ///    and then steer which receipts get popped).
    fn completeAckTickets(self: *Connection, ack: wire_pump.WriteCompletion) bool {
        var remaining = ack.ticket_count;
        if (remaining == 0 and ack.ticket != 0) remaining = 1;
        if (remaining == 0) return true;
        if (remaining > self.ticket_slots.len) return false;

        var slot_i = ack.ticket_slot;
        var first = true;
        const ack_ns = if (trace.enabled) nowNs(self.config.io) else 0;
        while (remaining > 0) : (remaining -= 1) {
            // Snapshot the link BEFORE complete wakes the handler. The handler
            // owns release/reuse after that wake and may reset this slot before
            // the next ticket in the chain is completed.
            const meta = self.tickets.completion(slot_i) orelse return false;
            if (first and meta.ticket != ack.ticket) return false;
            first = false;
            if (trace.enabled and self.trace_ticket.load(.acquire) == meta.ticket) {
                self.trace_written_ns.store(ack.written_ns, .release);
                self.trace_ack_ns.store(ack_ns, .release);
            }
            self.tickets.complete(slot_i, meta.ticket, ack.ok);
            if (remaining > 1) {
                if (meta.next == ticket_table.no_completion_slot) return false;
                slot_i = meta.next;
            } else if (meta.next != ticket_table.no_completion_slot) {
                return false;
            }
        }
        return true;
    }

    /// Every site that ends the connection from this side calls this just
    /// before it sets `writer_failed`; see `close_reason`.
    fn noteServerClose(self: *Connection, comptime site: []const u8, err: ?anyerror) void {
        if (diag_park) diagRawPrint("WFSET " ++ site ++ " conn={x}\n", .{@intFromPtr(self) & 0xffff});
        self.printCloseReason(site, if (err) |e| @errorName(e) else "-", "-");
    }

    fn printCloseReason(self: *Connection, comptime site: []const u8, err: []const u8, goaway: []const u8) void {
        if (!close_reason or self.close_reason_noted) return;
        self.close_reason_noted = true;
        // A failed socket write is how this side first sees most peer
        // closes: h2load hangs up after its last response, and the next
        // write fails. So it is not counted as the server ending it.
        const by = comptime if (std.mem.eql(u8, site, "ack_fail_all")) "write_failed" else "server";
        diagRawPrint("CLOSE_REASON conn={x} mode={s} by=" ++ by ++ " site=" ++ site ++ " err={s} goaway={s} held={d} pending={d} wire={d} lim={d} high_water={d} budget_parks={d} stream_budget_refusals={d}\n", .{
            @intFromPtr(self) & 0xffffff,
            @tagName(self.config.mode),
            err,
            goaway,
            self.outbound_held.load(.acquire),
            self.pending_outbound_held.load(.acquire),
            self.wire_outbound_held.load(.acquire),
            self.config.limits.outbound_bytes_per_connection,
            self.outbound_high_water.load(.acquire),
            self.budget_parks.load(.acquire),
            self.stream_budget_refusals,
        });
    }

    fn applyWriteAck(self: *Connection, ack: wire_pump.WriteCompletion) bool {
        _ = wire_pump.diag_acks.applied_release.fetchAdd(ack.outbound_release, .monotonic);
        self.applyOutboundRelease(ack.outbound_release, .wire);
        if (ack.control_entries != 0) self.applyControlRelease(ack.control_release, ack.control_entries);
        if (ack.fail_all) {
            self.noteServerClose("ack_fail_all", null);
            self.writer_failed.store(true, .release);
            self.tickets.failAll();
            // Ticket events are set before this flag; drainPendingCompleteReceipts
            // then consumes every pending receipt (wait_unsignaled).
            self.complete_receipts_fail_all.store(true, .release);
            // Do NOT touch handlers[] — handleWriterFailed applies terminals.
            self.wakeAllSpace();
            self.wakeAllDeadlines();
        } else if (ack.ticket != 0) {
            if (comptime test_observe) _ = wire_pump.diag_acks.applied_ticket.fetchAdd(1, .monotonic);
            if (!self.completeAckTickets(ack)) {
                // A malformed chain would strand at least one synchronous
                // write forever. Fail every waiter instead of converting
                // internal metadata corruption into a silent hang.
                self.noteServerClose("ack_fail_all2", null);
                self.writer_failed.store(true, .release);
                self.tickets.failAll();
                self.complete_receipts_fail_all.store(true, .release);
                self.wakeAllSpace();
                self.wakeAllDeadlines();
            } else if (ack.complete_batch_receipt) {
                if (trace.enabled) {
                    const idx = self.receipt_ready_stamp_w.fetchAdd(1, .monotonic);
                    self.receipt_ready_stamp[idx % complete_receipt_capacity].store(
                        nowNs(self.config.io),
                        .release,
                    );
                }
                if (comptime test_observe) _ = wire_pump.diag_acks.applied_receipt.fetchAdd(1, .monotonic);
            }
        }
        // A successful WIRE release cannot unblock waitForStreamSpace:
        // handlers park only on their stream's pending-slab length, and
        // that release already wakes the one affected stream in drainEmit.
        return ack.shutdown;
    }

    fn stashCompleteAck(self: *Connection, ack: wire_pump.WriteCompletion) void {
        std.debug.assert(self.held_ack_n < complete_receipt_capacity);
        self.held_acks[self.held_ack_n] = ack;
        self.held_ack_n += 1;
        if (comptime test_observe) test_complete_receipt_ack_held.store(true, .release);
    }

    fn shouldHoldCompleteAck(ack: wire_pump.WriteCompletion) bool {
        if (comptime !test_observe) return false;
        return ack.ticket != 0 and ack.complete_batch_receipt and
            test_hold_complete_receipt_ack.load(.acquire);
    }

    fn takeWriteAck(self: *Connection, ack: wire_pump.WriteCompletion) void {
        // TLS applies its acks here too (`ack_apply`), but they never pass
        // through `write_ack_ch`, so only h2c counts them.
        if (self.config.mode == .h2c and wire_pump.countsChunk(ack)) {
            self.acks.sub();
            self.wakeAckRoomWaiters();
        }
        if (shouldHoldCompleteAck(ack) and self.held_ack_n < complete_receipt_capacity) {
            self.stashCompleteAck(ack);
            return;
        }
        _ = self.applyWriteAck(ack);
    }

    fn flushHeldWriteAcks(self: *Connection) void {
        if (comptime test_observe) {
            if (test_hold_complete_receipt_ack.load(.acquire) and self.held_ack_n > 0) {
                test_complete_receipt_ack_held.store(true, .release);
                return;
            }
        }
        var i: usize = 0;
        while (i < self.held_ack_n) : (i += 1) {
            _ = self.applyWriteAck(self.held_acks[i]);
        }
        if (self.held_ack_n != 0) {
            self.held_ack_n = 0;
            if (comptime test_observe) test_complete_receipt_ack_held.store(false, .release);
        }
    }

    /// The flag-producers' wake. Set the flag FIRST, then ring; the channel
    /// mutex publishes the store. A full doorbell means a wake is already
    /// pending, and the actor drains every flag on every turn, so coalescing
    /// cannot lose work. This is the ONLY writer of `doorbell`.
    fn ring(self: *Connection) void {
        self.doorbell.trySend(0) catch |err| switch (err) {
            error.WouldBlock, error.Closed => {},
        };
    }

    /// Apply every posted WritePump completion without parking. Called at the
    /// top of every actor turn so a complete batch can publish credit and be
    /// consumed on the same turn.
    fn drainWriteAcks(self: *Connection) void {
        self.flushHeldWriteAcks();
        if (ackReceiveHeld()) return;
        while (io_queue.tryRecv(wire_pump.WriteCompletion, &self.write_ack_ch)) |ack| {
            self.takeWriteAck(ack);
        }
    }

    /// The channel the actor selects on for write completions. Under
    /// `test_hold_ack_receive` it is a channel nothing posts to, so the
    /// completions stay in `write_ack_ch`, the way the completion hold leaves
    /// handler completions buffered.
    fn ackReceiveCh(self: *Connection) *zio.Channel(wire_pump.WriteCompletion) {
        if (ackReceiveHeld()) return &hold_dummy_ack_ch;
        return &self.write_ack_ch;
    }

    fn ackReceiveHeld() bool {
        if (comptime !test_observe) return false;
        return test_hold_ack_receive.load(.acquire);
    }

    /// Occupancy Forced has not yet applied. `deinit` panics when this is
    /// nonzero: a second drain here used to absorb skipping Forced (t-894).
    fn leftoverWriteAckCount(self: *Connection) usize {
        return self.held_ack_n + io_queue.chanLen(wire_pump.WriteCompletion, &self.write_ack_ch);
    }

    fn assertWriteAcksDrained(self: *Connection) void {
        const leftover_acks = io_queue.chanLen(wire_pump.WriteCompletion, &self.write_ack_ch);
        if (self.held_ack_n == 0 and leftover_acks == 0) return;
        std.debug.panic(
            "deinit: leftover write acks (ack_q={d} held_acks={d}); drainWriteAcksForced is the sole owner",
            .{ leftover_acks, self.held_ack_n },
        );
    }

    /// Sole owner of leftover write-ack apply on teardown. `run`'s defer
    /// calls this after `JoinHandle.cancel`, which waits until the pump has
    /// exited, so every completion the pump posted is already in the channel.
    /// `deinit` asserts the channel is then empty (t-894); a second drain
    /// there absorbed skipping this one.
    fn drainWriteAcksForced(self: *Connection) void {
        var i: usize = 0;
        while (i < self.held_ack_n) : (i += 1) {
            _ = self.applyWriteAck(self.held_acks[i]);
        }
        self.held_ack_n = 0;
        if (comptime test_observe) test_complete_receipt_ack_held.store(false, .release);
        while (io_queue.tryRecv(wire_pump.WriteCompletion, &self.write_ack_ch)) |ack| {
            if (wire_pump.countsChunk(ack)) {
                self.acks.sub();
                self.wakeAckRoomWaiters();
            }
            _ = self.applyWriteAck(ack);
        }
    }

    /// Capacity waiter index = admitted HandlerSlot index (sparse stream IDs safe).
    fn spaceIndex(self: *Connection, stream_id: u31) ?usize {
        return self.slotIndex(stream_id);
    }

    fn wakeStreamSpace(self: *Connection, stream_id: u31) void {
        if (self.spaceIndex(stream_id)) |i| {
            if (i < self.space_events.len) self.space_events[i].set();
        }
    }

    fn wakeAllSpace(self: *Connection) void {
        for (self.space_events[0..self.handlers.touched]) |*event| event.set();
    }

    fn wakeHandlerDeadline(self: *Connection, stream_id: u31) void {
        const i = self.slotIndex(stream_id) orelse return;
        if (i < self.deadline_events.len) self.deadline_events[i].set();
    }

    fn wakeAllDeadlines(self: *Connection) void {
        for (self.deadline_events[0..self.handlers.touched]) |*event| event.set();
    }

    /// Write-ack-signaled writer failure: terminate connection, reset every stream,
    /// release reservations exactly once, wake waiters. Idempotent.
    ///
    /// `applyWriteAck` only SETS the flag; this runs later on the same actor
    /// turn, under `session_mu`. That separation is the point: teardown touches
    /// `Session`, the scheduler, and every handler slot, so it must run where
    /// those are owned. `writer_fail_handled` makes it once-only, because both
    /// the actor loop and `drainEmit`'s error path can arrive here.
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
    /// write-ack releases and can double-free payload bytes.
    fn handleWriterFailed(self: *Connection) void {
        if (!self.writer_failed.load(.acquire)) return;
        if (self.writer_fail_handled) return;
        self.writer_fail_handled = true;
        if (comptime test_observe) test_observed_writer_fail_handled.store(true, .release);

        for (self.handlers.slice()) |*slot| {
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
            self.sched.freePayload(e.payload);
        }
        while (self.sched.popOrdinary()) |e| {
            self.sched.ctrl.release(e.control_n, 1);
            self.sched.freePayload(e.payload);
        }
        while (self.sched.popFramed()) |e| {
            self.sched.freePayload(e.payload);
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
            .outbound_frame => |f| self.releaseWireBytes(f.payload),
            .dispatch_request => |d| self.releaseDispatchRequest(d),
            else => {},
        };

        // Do NOT drain write_ch here. WritePump is the sole consumer while the
        // connection is live; stealing chunks races write-ack releases
        // (and can double-free payload bytes). Queued wire is released via
        // WritePump failDrain/ack or Connection.deinit after pumps join.
        // Writer queue closure already terminates the failed transport.
        _ = io_queue.tryPut(wire_pump.WireChunk, &self.write_ch, self.config.io, .{
            .bytes = &.{},
            .len = 0,
        });
        self.write_ch.close(self.config.io);
        if (self.tls_write_buf.len != 0) {
            // Graceful: buffered chunks carry pool indices and accounting
            // that the driver's failDrain (or deinit) must release exactly
            // once. An immediate close would drop them with no destructor.
            self.tls_write_ch.close(.graceful);
        }

        self.wakeAllSpace();
        self.wakeAllDeadlines();
        self.session.terminal = .transport;
    }

    fn reserveTicket(self: *Connection) response.ResponseError!struct { u64, u32 } {
        return self.tickets.reserve() catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            error.WriteFailed => error.WriteFailed,
        };
    }

    /// ACTOR-side ticket wait (t-899). The actor is the sole consumer of
    /// `write_ack_ch`, so it must never park on a bare ticket event: the
    /// completion that would set the event can only be applied BY the actor.
    /// The 2026-08-20 live wedge read exactly that state off the process —
    /// actor in `waitTicket`, four acks queued (the ticket's failed ack, a
    /// fail_all, and the pump's shutdown) with nothing left to apply them.
    /// This loop feeds the wait: drain acks, and park only on a select that
    /// includes the ack channel. The final `tickets.wait` then returns without
    /// parking and reclaims the ticket slot. Returns whether the write landed.
    ///
    /// Only the complete-batch receipt waits here, and its handlers have
    /// already finished, so there is no terminal cause to map.
    ///
    /// Both waits here are cancellation points. zio delivers a cancel once, so
    /// `error.Canceled` leaves as itself and is not folded into "the write
    /// did not land" (`false`).
    fn waitTicketDraining(self: *Connection, slot_i: u32) error{Canceled}!bool {
        while (!self.tickets.isSignaled(slot_i)) {
            self.drainWriteAcks();
            if (self.tickets.isSignaled(slot_i)) break;
            if (self.config.mode == .tls) {
                // Acks apply locally in `post`. If a ticket is still dark,
                // the send is in flight: drive the CQ, never park on a
                // channel with no TLS producer.
                const pump = self.tls_driver orelse break;
                const winner = try zio.select(.{
                    .io = &pump.cq,
                    .bell = self.doorbell.asyncReceive(),
                });
                switch (winner) {
                    .io => |r| {
                        const c = r catch break;
                        if (pump.onCqComplete(c) == .exit) break;
                        if (pump.pending_n > 0 and !pump.drivePending()) break;
                    },
                    .bell => |r| {
                        _ = r catch break;
                    },
                }
                continue;
            }
            const winner = try zio.select(.{
                .acks = self.ackReceiveCh().asyncReceive(),
                .bell = self.doorbell.asyncReceive(),
            });
            switch (winner) {
                .acks => |r| {
                    const ack = r catch break;
                    self.takeWriteAck(ack);
                },
                .bell => |r| {
                    _ = r catch break;
                },
            }
        }
        self.tickets.wait(slot_i, null) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            return false;
        };
        return true;
    }

    /// After a failed ticket wait, apply the same terminal mapping as
    /// `refuseIfStreamDead`. Peer RST sets a Session tombstone in `handleFrame`
    /// before the `stream_reset` intent reaches `cancelHandler`, and
    /// `wakeHandlerWaiters` can post `ok = false` in that gap — including the
    /// no-slot path that drops pending without a `SlotTerminal` cause.
    ///
    /// The lock is uncancelable: a cancelable acquire can turn the RST into
    /// `Canceled` (I8 code 5) in the same gap this mapping exists to close.
    fn waitTicket(self: *Connection, stream_id: u31, slot_i: u32, terminal: *response.SlotTerminal) response.ResponseError!void {
        self.tickets.wait(slot_i, terminal) catch |err| {
            if (terminal.getCause()) |c| return response.causeToError(c);
            self.lockSessionUncancelable(self.config.io);
            defer self.unlockSession(self.config.io);
            return response.mapFailedTicketWait(terminal, self.session.tombstoneCode(stream_id), err);
        };
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
                self.handlers.slice()[si].terminal.setCause(.slow_consumer);
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

    fn drainCompletions(self: *Connection) usize {
        if (comptime test_observe) {
            if (test_hold_completion_drain.load(.acquire)) return 0;
        }
        var n: usize = 0;
        while (io_queue.tryRecv(u31, &self.completion_ch)) |sid| {
            if (self.releaseSlot(sid)) n += 1;
        }
        return n;
    }

    /// Trace-only: leftover `read_ch` after this take. Locks the channel
    /// mutex; do not use the count to size the hot-path batch.
    fn noteReadBacklog(self: *Connection) void {
        const left = io_queue.chanLen(wire_pump.WireChunk, &self.read_ch);
        _ = trace.read_take_n.fetchAdd(1, .monotonic);
        _ = trace.read_left_sum.fetchAdd(left, .monotonic);
        trace.bumpMax(&trace.read_left_max, left);
        switch (left) {
            0 => _ = trace.read_left_0.fetchAdd(1, .monotonic),
            1 => _ = trace.read_left_1.fetchAdd(1, .monotonic),
            2 => _ = trace.read_left_2.fetchAdd(1, .monotonic),
            3 => _ = trace.read_left_3.fetchAdd(1, .monotonic),
            else => _ = trace.read_left_ge4.fetchAdd(1, .monotonic),
        }
    }

    fn noteInboundBatch(n: usize) void {
        switch (n) {
            1 => _ = trace.inbound_batch_1.fetchAdd(1, .monotonic),
            2 => _ = trace.inbound_batch_2.fetchAdd(1, .monotonic),
            3 => _ = trace.inbound_batch_3.fetchAdd(1, .monotonic),
            else => _ = trace.inbound_batch_4.fetchAdd(1, .monotonic),
        }
    }

    const InboundBatchStop = enum { go, terminal, failed };

    /// Between inbound extras: apply write acks so receipt credit can publish,
    /// consume ready receipts so the closing `runPendingInline` has a slot,
    /// then stop the batch on terminal or writer failure. Do not `runPendingInline`
    /// here — extras share one drain-turn after the batch.
    fn inboundBatchShouldStop(self: *Connection) error{Canceled}!InboundBatchStop {
        self.drainWriteAcks();
        _ = self.drainCompletions();
        try self.drainPendingCompleteReceipts(false);
        if (self.writer_failed.load(.acquire)) return .failed;
        self.lockSessionUncancelable(self.config.io);
        defer self.unlockSession(self.config.io);
        if (self.session.terminal != .none) return .terminal;
        return .go;
    }

    /// Ingest `bytes` and process the intents, as many rounds as it takes.
    /// `Session.ingestSome` stops at a frame boundary when the intent queue has
    /// less room than one frame can need, so the queue is materialized and
    /// ingest goes on with the rest; one read chunk can then never fill the
    /// queue (`Limits.intentEntriesMin`). Caller holds `session_mu`.
    fn ingestAndProcess(self: *Connection, bytes: []const u8) !void {
        self.assertSessionHeld("ingestAndProcess");
        const io = self.config.io;
        var rest = bytes;
        while (true) {
            const ingest_t0 = if (trace.enabled) nowNs(io) else 0;
            const taken = self.session.ingestSome(rest) catch |err| return self.sessionErrorExit(err, "ingest_error");
            if (trace.enabled) {
                _ = trace.ingest_ns.fetchAdd(nowNs(io) -% ingest_t0, .monotonic);
                _ = trace.ingest_n.fetchAdd(1, .monotonic);
            }
            // The queue is empty on every call (each round below drains it),
            // and boot keeps it at least one frame's room, so a round that
            // takes nothing is a broken invariant, not a slow peer.
            if (taken == 0 and rest.len != 0) std.debug.panic("ingest took nothing from {d} bytes with {d} of {d} intents queued: the intent queue is below Limits.intentEntriesMin", .{ rest.len, self.session.intentLen(), self.session.intentCapacity() });
            rest = rest[taken..];
            const intent_t0 = if (trace.enabled) nowNs(io) else 0;
            try self.processIntents();
            if (trace.enabled) {
                _ = trace.intent_ns.fetchAdd(nowNs(io) -% intent_t0, .monotonic);
                _ = trace.intent_n.fetchAdd(1, .monotonic);
            }
            if (rest.len == 0) return;
        }
    }

    /// The exit for an error from `Session` in a lock section. The section
    /// may have queued intents before the error, and the queue must be empty
    /// when the lock is released (`unlockSession`), so they are materialized
    /// first. If the session went terminal, its GOAWAY is now in the
    /// scheduler and the error does not end the connection here: the caller
    /// goes on, and the closing phase writes it. Otherwise the error is the
    /// connection's end, returned to `run`, whose teardown releases the
    /// scheduler.
    fn sessionErrorExit(self: *Connection, err: anyerror, comptime site: []const u8) anyerror!void {
        self.assertSessionHeld("sessionErrorExit");
        self.materializeIntents() catch self.failClosedOnIntentError();
        if (self.session.terminal != .goaway) return err;
        self.noteServerClose(site, err);
    }

    /// Ingest one already-taken inbound chunk. Caller recycles the chunk.
    /// Returns true when Session became terminal.
    fn ingestWireChunk(self: *Connection, chunk: wire_pump.WireChunk) !bool {
        const io = self.config.io;
        self.lockSessionUncancelable(io);
        defer self.unlockSession(io);
        self.session.edge_now_ns = nowNs(io);
        try self.ingestAndProcess(chunk.bytes[0..chunk.len]);
        // Start closing here, under the hold ingest already has, so the
        // inline queue is frozen before anything can run those handlers.
        self.beginClosingIfTerminal();
        return self.session.terminal != .none;
    }

    fn noteReceiptDepthChange(self: *Connection, old_n: usize, new_n: usize) void {
        if (!trace.enabled) return;
        switch (new_n) {
            0 => _ = trace.receipt_depth_0.fetchAdd(1, .monotonic),
            1 => _ = trace.receipt_depth_1.fetchAdd(1, .monotonic),
            2 => _ = trace.receipt_depth_2.fetchAdd(1, .monotonic),
            3 => _ = trace.receipt_depth_3.fetchAdd(1, .monotonic),
            else => _ = trace.receipt_depth_4.fetchAdd(1, .monotonic),
        }
        const now = nowNs(self.config.io);
        if (old_n == complete_receipt_capacity and new_n < old_n) {
            const entered = self.receipt_depth_entered_ns;
            if (entered != 0 and now >= entered) {
                _ = trace.receipt_full_ns.fetchAdd(now - entered, .monotonic);
            }
            self.receipt_depth_entered_ns = 0;
        }
        if (new_n == complete_receipt_capacity and old_n < new_n) {
            self.receipt_depth_entered_ns = now;
            _ = trace.receipt_full_enter.fetchAdd(1, .monotonic);
        }
    }

    fn noteCreditReuse(self: *Connection) void {
        if (!trace.enabled) return;
        const idx = self.receipt_ready_stamp_r;
        self.receipt_ready_stamp_r += 1;
        const stamped = self.receipt_ready_stamp[idx % complete_receipt_capacity].swap(0, .acq_rel);
        if (stamped == 0) return;
        const now = nowNs(self.config.io);
        if (now >= stamped) {
            _ = trace.credit_reuse_ns.fetchAdd(now - stamped, .monotonic);
            _ = trace.credit_reuse_n.fetchAdd(1, .monotonic);
        }
    }

    /// True when the front pending receipt's ticket event is set. That event
    /// is the only ready proof: `complete` / `failAll` set it, and only after
    /// the matching ack is applied. A side-channel count can exceed
    /// truly-applied acks and then pop the wrong receipt (t-900).
    fn frontCompleteReceiptReady(self: *Connection) bool {
        if (self.pending_complete_receipt_n == 0) return false;
        return self.tickets.isSignaled(self.pending_complete_receipts[0].ticket_slot);
    }

    /// Reclaim complete-handler drain-turn receipts whose tickets this actor
    /// has already completed. Walk from the front while `isSignaled`; stop at
    /// the first unsignaled receipt so a later ack cannot steer which sample
    /// gets stamped. Writer-failure and shutdown (`finalize_all`) still wait
    /// via `waitTicketDraining` so in-flight acks can land. The ordinary path
    /// never starts that wait for an unsignaled front receipt — the loop top
    /// applies acks first, then this drain sees the event.
    fn drainPendingCompleteReceipts(self: *Connection, finalize_all: bool) error{Canceled}!void {
        const wait_unsignaled = finalize_all or self.complete_receipts_fail_all.swap(false, .acq_rel);

        while (self.pending_complete_receipt_n > 0) {
            const receipt = self.pending_complete_receipts[0];
            std.debug.assert(receipt.ticket != 0);
            if (!wait_unsignaled and !self.tickets.isSignaled(receipt.ticket_slot)) break;
            if (std.debug.runtime_safety) {
                const meta = self.tickets.completion(receipt.ticket_slot) orelse
                    std.debug.panic("pending complete receipt lost ticket slot {d}", .{receipt.ticket_slot});
                std.debug.assert(meta.ticket == receipt.ticket);
            }

            const receipt_ok = try self.waitTicketDraining(receipt.ticket_slot);
            if (receipt.send_trace) |t| {
                if (receipt_ok) self.stampSendWait(t) else self.trace_ticket.store(0, .release);
            }
            self.popCompleteReceipt();
        }
    }

    fn popCompleteReceipt(self: *Connection) void {
        std.debug.assert(self.pending_complete_receipt_n > 0);
        var i: usize = 1;
        while (i < self.pending_complete_receipt_n) : (i += 1) {
            self.pending_complete_receipts[i - 1] = self.pending_complete_receipts[i];
        }
        const old_n = self.pending_complete_receipt_n;
        self.pending_complete_receipt_n -= 1;
        self.pending_complete_receipts[self.pending_complete_receipt_n] = .{};
        self.noteCreditReuse();
        self.noteReceiptDepthChange(old_n, self.pending_complete_receipt_n);
    }

    /// Protocol deadlines only (idle, slow-consumer, grace, test canary).
    /// Handler deadlines are handler-owned zio timed waits and never appear
    /// in the actor's park timer.
    fn nextDeadlineNs(self: *Connection) ?u64 {
        // While closing, the other deadlines are not enforced (the loop does
        // not run them), and an overdue one would make `waitForActivity`
        // return at once on every turn.
        if (self.closing_deadline_ns) |deadline| return deadline;
        var next = self.session.nextIdleDeadlineNs();
        if (self.sched.nextSlowDeadlineNs(self.config.limits.slow_consumer_timeout_ns)) |deadline| {
            if (next == null or deadline < next.?) next = deadline;
        }
        if (self.grace_deadline) |deadline| {
            const ns: u64 = @intCast(deadline.nanoseconds);
            if (next == null or ns < next.?) next = ns;
        }
        const canary_tick: u64 = if (comptime test_observe) test_polling_canary_tick_ns.load(.acquire) else 0;
        if (canary_tick != 0) {
            const deadline = nowNs(self.config.io) +% canary_tick;
            if (next == null or deadline < next.?) next = deadline;
        }
        return next;
    }

    fn traceParkSnapshot(self: *Connection) void {
        const acc: usize = 0;
        const complete = false;
        const pending = self.sched.pendingCount();
        const ordinary = self.sched.ordinary_len;
        const framed = self.sched.framed_len;
        if (!complete and pending == 0 and ordinary == 0 and framed == 0) {
            // dropResetPending / refuseResetStream hung here with nothing to
            // print. Throttle empty parks so an infinite hang is still visible.
            const now = nowNs(self.config.io);
            if (now -% diag_last_empty_park_ns < 5 * std.time.ns_per_s) return;
            diag_last_empty_park_ns = now;
        }
        var missing: u32 = 0;
        var zero_win: u32 = 0;
        var eligible: u32 = 0;
        var tomb_noerr: u32 = 0;
        var tomb_rst: u32 = 0;
        var pend_slots: u32 = 0;
        var rst0: u32 = 0;
        var min_win: i32 = std.math.maxInt(i32);
        var pending_bytes: usize = 0;
        for (self.sched.pending_slots.slice()) |s| {
            if (s.stream_id == 0) continue;
            pending_bytes += s.len;
            if (self.slotIndex(s.stream_id) != null) pend_slots += 1;
            const sw = self.session.streamSendAvailable(s.stream_id);
            if (self.session.streams.get(s.stream_id) == null) {
                missing += 1;
                if (self.session.tombstoneCode(s.stream_id)) |c| {
                    if (c == .no_error) tomb_noerr += 1 else tomb_rst += 1;
                    if (rst0 == 0 and c != .no_error) rst0 = @intFromEnum(c);
                }
            } else if (sw <= 0) {
                zero_win += 1;
            } else {
                eligible += 1;
            }
            if (sw < min_win) min_win = sw;
        }
        const write_q = io_queue.queued(wire_pump.WireChunk, &self.write_ch, self.config.io);
        const read_q = io_queue.chanLen(wire_pump.WireChunk, &self.read_ch);
        const read_free_q = io_queue.queued(u32, &self.read_free_ch, self.config.io);
        const write_free_q = self.write_pool.capacity - self.write_pool.rentedCount();
        const tls_write_empty = if (self.tls_write_buf.len != 0) self.tls_write_ch.isEmpty() else true;
        var receipts_ready: usize = 0;
        while (receipts_ready < self.pending_complete_receipt_n) : (receipts_ready += 1) {
            if (!self.tickets.isSignaled(self.pending_complete_receipts[receipts_ready].ticket_slot)) break;
        }
        diagRawPrint(
            "STARH2_PARK acc={d} complete={} pending={d} bytes={d} ordinary={d} framed={d} live={d} refilled={} conn_win={d} eligible={d} missing={d} tomb_noerr={d} tomb_rst={d} pend_slots={d} rst0={d} intents={d} zero_win={d} min_swin={d} active={d} rst_h={d} rst_c={d} ctrl_fail={d} rst_after_fail={d}\n",
            .{
                acc,
                complete,
                pending,
                pending_bytes,
                ordinary,
                framed,
                self.live_handlers.load(.acquire),
                self.sched_refilled.load(.acquire),
                self.session.connectionSendAvailable(),
                eligible,
                missing,
                tomb_noerr,
                tomb_rst,
                pend_slots,
                rst0,
                self.session.intentLen(),
                zero_win,
                min_win,
                self.session.active_streams,
                diag_rst_handler.load(.monotonic),
                diag_rst_compress.load(.monotonic),
                diag_ctrl_fail.load(.monotonic),
                diag_reset_after_ctrl_fail.load(.monotonic),
            },
        );
        diagRawPrint(
            "STARH2_TLSQ conn={x} write_q={d} read_q={d} tls_write_empty={} read_free={d} write_free={d} inline={d} receipts={d} receipts_ready={d} bell={d} acks={d} live_h={d}\n",
            .{
                @intFromPtr(self) & 0xffff,
                write_q,
                read_q,
                tls_write_empty,
                read_free_q,
                write_free_q,
                self.inline_n,
                self.pending_complete_receipt_n,
                receipts_ready,
                io_queue.chanLen(u8, &self.doorbell),
                io_queue.chanLen(wire_pump.WriteCompletion, &self.write_ack_ch),
                self.live_handlers.load(.acquire),
            },
        );
        // t-1022: the scheduler-pause inputs. An eligible pending chunk that
        // never emits while `pause` reads true names the TLS driver state
        // that pinned it; none of the lines above carry these.
        if (self.tls_driver) |p| {
            diagRawPrint(
                "STARH2_TLSP conn={x} pending_n={d} stash_full={} pending_read={} pending_cipher={} recv_armed={} send_armed={}\n",
                .{
                    @intFromPtr(self) & 0xffff,
                    p.pending_n,
                    p.stashFull(),
                    p.pending_read != null,
                    p.pending_cipher != null,
                    p.recv_armed,
                    p.send_armed,
                },
            );
        }
    }

    /// Park in ONE `zio.select` until something the actor cares about
    /// happens. Every branch holds persistent evidence (a buffered channel
    /// item, a set Event), so there is no reset and no recheck list.
    ///
    /// Declaration order is the tie-break when several branches are ready:
    /// reads first (matches the hot turn's take order), then acks and
    /// completions, then the doorbell, then the control branches.
    ///
    /// The timer branch is `.none` when no deadline is armed, which
    /// registers nothing, so an idle connection costs no periodic wakeup.
    /// Test-gated branches point at never-fired dummies when inactive, so
    /// the select keeps one comptime shape.
    ///
    /// Returns the chunk when the reads branch won; null on any other wake
    /// (the loop top re-drains every source each turn).
    ///
    /// TLS loses `.reads` and `.acks`: inbound is a stash the actor ingest
    /// consumes, and write acks apply locally in `Pump.post`.
    /// One TLS actor turn: CQ, outbound, inbound ingest. True means the
    /// connection should tear down (EOF, cipher failure, or empty sentinel).
    ///
    /// A peer that stops reading must not stop the actor: `writeChunks` that
    /// hit a full staging buffer leave `pending_n > 0` and return, and this
    /// turn still parks in `waitForActivity` which serves handler completions,
    /// deadlines, the doorbell and the slow-consumer kill — only `.io` waits
    /// for the send completion, never the socket.
    /// Diagnosis (t-2655): count TLS turns in a row that return without
    /// parking, and print the pump state once the run passes a threshold no
    /// healthy connection reaches. Printed from the actor itself, because a
    /// spinning actor starves every other task on its executor.
    fn diagNoPark(self: *Connection, pump: *tls_edge.Pump, why: u8) void {
        if (!diag_stuck) return;
        self.diag_no_park_run += 1;
        if (self.diag_no_park_run == 100_000 or self.diag_no_park_run == 10_000_000) {
            diagRawPrint("NOPARK conn={x} run={d} why={d} pending_n={d} carried={} write_ch_empty={} send_armed={} recv_armed={} stash_full={} sched_pending={d}\n", .{
                @intFromPtr(self) & 0xffff, self.diag_no_park_run, why, pump.pending_n, pump.carried != null,
                pump.write_ch.isEmpty(),    pump.send_armed,       pump.recv_armed, pump.stashFull(), self.sched.pendingCount(),
            });
        }
    }

    fn driveTlsTurn(self: *Connection) !bool {
        diagActor(self, .tls_turn);
        defer diagActor(self, .running);
        const pump = self.tls_driver.?;

        // Closing phase: a completion taken here can be the last send, which
        // is what ends the phase. Only the lock block decides that, so a turn
        // that took one goes round again instead of parking on the timer.
        var polled_progress = false;
        if (pump.pending_cipher == null) {
            switch (pump.pollRecv()) {
                .exit => return true,
                .progress => polled_progress = true,
                .none => {},
            }
        }

        if (pump.pending_n > 0) {
            if (!pump.drivePending()) return true;
        }
        if (comptime test_observe) {
            if (test_hold_stash.load(.acquire) and !pump.write_ch.isEmpty()) {
                _ = test_hold_stash_entered.fetchAdd(1, .release);
                while (test_hold_stash.load(.acquire)) try self.config.io.sleep(.fromMilliseconds(1), .awake);
            }
        }
        while (pump.pending_n == 0) {
            const chunk = if (pump.carried) |c| blk: {
                pump.carried = null;
                break :blk c;
            } else blk: {
                const taken = pump.tryTakeWrite() orelse break;
                // This take left room in the stash: wake the barriers that wait
                // for it (the TLS actor gets no write-ack to do it). At once, not
                // after the write: a handler that is ready to flush takes the
                // slot while the chunk is being written, and a check after the
                // write would find the stash full again and wake nobody.
                self.wakeBarrierWaitersTls();
                break :blk taken;
            };
            if (!pump.writeChunks(chunk)) return true;
            // Again after the write: `writeChunks` gathers more chunks off the
            // stash for a batch. Inside the loop on purpose: a bare barrier makes
            // no ciphertext, so its chunk leaves `pending_n` at zero and the loop
            // goes on, and handlers that keep flushing can keep it going; a wake
            // after the loop would wait for as long as they do.
            self.wakeBarrierWaitersTls();
        }

        if (pump.pending_cipher != null and pump.pending_read == null) {
            const bytes = pump.pending_cipher.?;
            pump.pending_cipher = null;
            if (!pump.ingestCipher(bytes)) return true;
        }

        var held_n: usize = 0;
        var leave = false;
        // Closing phase: the peer's frames are read and dropped, not ingested.
        // Reading on keeps the pump from stalling on a full inbound buffer and
        // keeps `waitForActivity` from refusing to park on unread plaintext.
        const closing = self.closing_deadline_ns != null;
        while (held_n < inbound_chunk_batch) {
            if (pump.pending_read == null and pump.conn.pendingInbound()) {
                switch (pump.readOne()) {
                    .eof => return true,
                    .ok => {},
                    .closed, .want, .stuck => break,
                }
            }
            const chunk = pump.pending_read orelse break;
            pump.pending_read = null;
            if (chunk.len == 0) return true;
            held_n += 1;
            if (closing) continue;
            leave = try self.ingestWireChunk(chunk) or leave;
            // Session went terminal. If that began the closing phase, the next
            // turn writes the GOAWAY; otherwise the writer failed: leave.
            if (leave) return self.closing_deadline_ns == null;
            if (self.writer_failed.load(.acquire)) break;
        }
        if (held_n > 0) {
            if (pump.pending_n > 0) {
                if (!pump.drivePending()) return true;
            }
            if (self.live_task_handlers.load(.acquire) > 0) {
                // A yield is a cancellation point. zio delivers a cancel once,
                // so it must leave as `error.Canceled`, not be dropped here.
                try self.config.io.sleep(.zero, .awake);
            }
            try self.runPendingInline();
            return false;
        }

        // t-866: never wait while pendingInbound(). Also don't park on a
        // stashed cipher suffix (recv is unarmed until it is fed), or while
        // outbound is only stashed (carried / write_ch) and not yet encrypted.
        if (pump.conn.pendingInbound() or pump.pending_read != null or pump.pending_cipher != null) {
            self.diagNoPark(pump, 1);
            return false;
        }
        // Stashed outbound is a reason to go round again only when the next
        // turn can write it. With `pending_n > 0` the staging buffer is full
        // behind an in-flight send, the loop above did not touch the stash,
        // and only the send completion frees space. That completion arrives
        // through the CQ, which only the select in `waitForActivity` reaps.
        // Returning here instead spun the actor without ever yielding. Under
        // pinned scheduling its executor then never polled again, so the
        // completion was never delivered and every task on that executor
        // stopped for good (t-2655: 10M turns with no park, pending_n=6,
        // carried set).
        if (pump.pending_n == 0 and (pump.carried != null or !pump.write_ch.isEmpty())) {
            self.diagNoPark(pump, 2);
            return false;
        }
        self.diag_no_park_run = 0;
        if (polled_progress and self.closing_deadline_ns != null) return false;

        const maybe_chunk = try self.waitForActivity();
        if (maybe_chunk) |chunk| {
            if (chunk.len == 0 and chunk.bytes.len == 0) return true;
        }
        return false;
    }

    fn waitForActivity(self: *Connection) !?wire_pump.WireChunk {
        const io = self.config.io;
        self.actor_site.store(2, .release);
        defer self.actor_site.store(1, .release);
        if (diag_park) self.traceParkSnapshot();
        if (comptime test_observe) {
            if (test_hold_before_actor_wait.swap(false, .acq_rel)) {
                test_actor_waiting.set(io);
                try test_release_actor_wait.wait(io);
            }
        }
        const timeout: zio.Timeout = if (self.nextDeadlineNs()) |deadline_ns| blk: {
            const now = nowNs(io);
            if (deadline_ns <= now) return null;
            break :blk .{ .duration = .fromNanoseconds(deadline_ns - now) };
        } else .none;
        // t-1002 gauge: this actor is about to park with a protocol deadline
        // armed. The probe lines print the count, so a slow close correlates
        // (or does not) with parked-on-deadline neighbours.
        const probe_deadline_park = close_probe and timeout != .none;
        if (probe_deadline_park) _ = close_probe_parked.fetchAdd(1, .acq_rel);
        defer if (probe_deadline_park) {
            _ = close_probe_parked.fetchSub(1, .acq_rel);
        };
        if (diag_stuck) _ = self.diag_actor_turns.fetchAdd(1, .monotonic);
        // The server's shutdown event stays set once fired. After this actor
        // has seen it (`shutting_down`), selecting on it again wins at once on
        // every turn, so the actor never parks and holds its executor for the
        // whole drain (t-2652). The drain still wakes on its own deadlines
        // (`grace_deadline` is in nextDeadlineNs) and on peer activity.
        const shutdown_ev = if (self.shutting_down or self.closing_deadline_ns != null)
            &no_shutdown_event
        else
            self.config.shutdown_event orelse &no_shutdown_event;
        // The hold leaves completions buffered in the channel, exactly as the
        // gated drain leaves them queued.
        const comps_ch = if ((comptime test_observe) and test_hold_completion_drain.load(.acquire))
            &hold_dummy_completion_ch
        else
            &self.completion_ch;
        const ack_hold_ev = if ((comptime test_observe) and self.held_ack_n > 0)
            &test_release_complete_receipt_ack
        else
            &no_ack_hold_event;
        if (self.config.mode == .tls) {
            const pump = self.tls_driver.?;
            // t-866: never park while inbound plaintext, unread records, or a
            // stashed cipher suffix sit. The recv is unarmed in the last case.
            if (pump.pending_read != null or pump.pending_cipher != null or pump.conn.pendingInbound()) return null;
            // After the peer's EOF nothing is read, and with no send in flight
            // only a timer or the doorbell can wake the actor.
            std.debug.assert(pump.recv_armed or pump.send_armed or pump.inbound_eof);
            self.assertNoLostWake("the TLS select");
            diagActor(self, .select);
            defer diagActor(self, .tls_turn);
            const winner = blk: {
                if (comptime test_observe) _ = test_actors_in_select.fetchAdd(1, .acq_rel);
                defer if (comptime test_observe) {
                    _ = test_actors_in_select.fetchSub(1, .acq_rel);
                };
                break :blk try zio.select(.{
                    .io = &pump.cq,
                    .comps = comps_ch.asyncReceive(),
                    .bell = self.doorbell.asyncReceive(),
                    .ack_hold = ack_hold_ev,
                    .shutdown = shutdown_ev,
                    .timer = timeout,
                });
            };
            switch (winner) {
                .bell => {},
                else => self.auditWake("the TLS select"),
            }
            switch (winner) {
                .io => |r| {
                    const c = r catch |err| {
                        // Under select, `error.Closed` is not proof of the
                        // drained state: a stale futex signal from an arm a
                        // concurrent winner canceled can outlive the select's
                        // signal accounting and win a later select with the
                        // queue empty and open (zio, #709 protocol lineage).
                        // Tearing the connection down on it killed all live
                        // streams silently (t-1022). `isDrained()` is the
                        // CQ's own classifier: false means keep driving (one
                        // wasted turn); true is the terminal state the error
                        // names, and only that falls through to failDrain.
                        // Gating on the predicate rather than on the "actor
                        // never closes mid-loop" invariant keeps a REAL
                        // drained queue loud: without it, a genuine close
                        // would re-poll a permanently-ready arm in a silent
                        // hot spin.
                        if (err == error.Closed and !pump.cq.isDrained()) {
                            if (diag_park) diagRawPrint("spurious cq Closed ignored conn={x}\n", .{@intFromPtr(self) & 0xffff});
                            return null;
                        }
                        if (diag_park) diagRawPrint("TLSERR cq_receive {s} conn={x}\n", .{ @errorName(err), @intFromPtr(self) & 0xffff });
                        pump.failDrain();
                        return .{ .bytes = &.{}, .len = 0 };
                    };
                    if (pump.onCqComplete(c) == .exit) {
                        return .{ .bytes = &.{}, .len = 0 };
                    }
                },
                .comps => |r| {
                    const sid = r catch return null;
                    if (!self.releaseSlot(sid)) {
                        std.debug.panic("completion sid={d} released no slot", .{sid});
                    }
                },
                .bell => |r| {
                    _ = r catch return null;
                },
                .ack_hold => self.drainWriteAcks(),
                .shutdown => {},
                .timer => {},
            }
            return null;
        }
        self.assertNoLostWake("the h2c select");
        const winner = blk: {
            if (comptime test_observe) _ = test_actors_in_select.fetchAdd(1, .acq_rel);
            defer if (comptime test_observe) {
                _ = test_actors_in_select.fetchSub(1, .acq_rel);
            };
            break :blk try zio.select(.{
                .reads = self.read_ch.asyncReceive(),
                .acks = self.ackReceiveCh().asyncReceive(),
                .comps = comps_ch.asyncReceive(),
                .bell = self.doorbell.asyncReceive(),
                .ack_hold = ack_hold_ev,
                .shutdown = shutdown_ev,
                .timer = timeout,
            });
        };
        switch (winner) {
            .bell => {},
            else => self.auditWake("the h2c select"),
        }
        switch (winner) {
            .reads => |r| {
                const chunk = r catch return null;
                return chunk;
            },
            .acks => |r| {
                const ack = r catch return null;
                self.takeWriteAck(ack);
            },
            .comps => |r| {
                const sid = r catch return null;
                if (!self.releaseSlot(sid)) {
                    std.debug.panic("completion sid={d} released no slot", .{sid});
                }
            },
            .bell => |r| {
                // The token is the consumed wake; the flags it covered are
                // drained at the loop top.
                _ = r catch return null;
            },
            .ack_hold => self.drainWriteAcks(),
            .shutdown => {},
            .timer => {},
        }
        return null;
    }

    /// The actor. One task, one connection, from first byte to close.
    ///
    /// ## Startup sequence
    /// 1. TLS, if any: the handshake already ran on this same task, in
    ///    `Server.serveTlsThenBranch`, which hands over `tls_ready` and the
    ///    leftover plaintext (a pipelined preface). Arm the CQ driver: sole
    ///    encrypt/decrypt owner and sole socket reader from here on; flush is
    ///    encrypt into the send staging buffer + NetSend. Arming submits the
    ///    first recv, and the kernel may fill it at once, but no received byte
    ///    reaches Session until the loop below takes it off the queue.
    /// 2. h2c: spawn ReadPump and WritePump on the raw socket.
    /// 3. Flush the server preface that `Session.init` already queued. It
    ///    must leave the scheduler before any client byte is ingested
    ///    (RFC 9113 section 3.4).
    /// 4. TLS only: ingest the leftover plaintext, then flush again. It still
    ///    reaches Session before any byte the pump received. A connection
    ///    error here starts the closing phase; it does not return.
    /// 5. For h2c, wait for the client preface under the preface deadline. A
    ///    bad preface starts the closing phase the same way.
    ///
    /// ## Steady-state iteration
    /// Each pass does the same five things, and the order is the contract:
    /// 1. Clear `sched_refilled`, drain write completions, then drain
    ///    handler completions. Ack drain is first so a complete-batch can
    ///    publish credit and be consumed on this same turn. Clearing the
    ///    refill flag first means a refill that lands during this pass is
    ///    still seen.
    /// 2. Under `session_mu`: apply a pending writer failure, publish the
    ///    clock, check protocol deadlines, advance graceful shutdown, kill slow
    ///    consumers, then EMIT. Emission is last because every earlier step can
    ///    add to what must be emitted.
    /// 3. Check for the graceful finish condition.
    /// 4. Try to take a read chunk without blocking. Then `tryGet` up to
    ///    `inbound_chunk_batch - 1` more, ingesting each (already plaintext)
    ///    before the next take. One `runPendingInline` after the batch.
    /// 5. If there is none: park in `waitForActivity`'s single select. Every
    ///    source holds persistent evidence, so a publish that races the park
    ///    is observed by the select's fast path — no reset, no recheck list.
    ///
    /// ## Closing phase
    /// RFC 9113 section 5.4.1: on a connection error send GOAWAY first, then
    /// close. The connection starts a closing phase, inside the same loop, when
    /// Session becomes terminal with a GOAWAY queued (a connection error, in the
    /// first flight or later) or when a graceful drain ends and its RST_STREAMs
    /// are queued. `beginClosing` sets `closing_deadline_ns` and runs the sweep,
    /// so handlers are told to stop and no inline job runs. Then, each turn:
    /// - The deadline checks, graceful steps, slow-consumer kills and parked
    ///   dispatch are skipped. The materialize and emit steps still run, so a
    ///   frame still in the scheduler reaches the writer.
    /// - Inbound bytes are read and dropped, not ingested. Reading on keeps the
    ///   TLS pump from stalling on a full inbound buffer; dropping means a peer
    ///   that keeps talking cannot restart the work the sweep just stopped.
    /// - Writes are driven the normal way: `driveTlsTurn` on TLS, the WritePump
    ///   on h2c. On h2c, once the connection is ending, the scheduler pause
    ///   hook (`h2cPause`) also stops each pop that lacks room in the write
    ///   queue: a push would park the actor in `putOne`, and
    ///   the deadline could not fire. The frames wait in the scheduler, and a
    ///   write completion wakes the actor to try again.
    /// - `closeFlushed` ends the phase when `outboundFlushed` (every chunk acked,
    ///   and on TLS the socket has accepted all ciphertext), or the deadline
    ///   `Limits.close_flush_timeout_ns` passed, or the writer failed. It is
    ///   the only place that decides this.
    /// A cancel during the phase is not caught: it leaves through `run` like
    /// any other, and `run` re-raises it. After the phase, the teardown below
    /// runs unchanged and releases whatever the phase did not write.
    ///
    /// ## Teardown sequence (the `defer` block, then the tail)
    /// 1. Tell the live pump(s) to stop. h2c: push a sentinel so a writer
    ///    parked on an empty queue wakes. TLS: close the unused overflow
    ///    channel, then `shutdownCq` on this task (no pump task).
    /// 2. `shutdown` the socket. A read parked in the kernel does not observe a
    ///    flag, so this is what unblocks it; task cancellation is the
    ///    authoritative backstop.
    /// 3. Cancel h2c pumps if they were spawned, then `drainWriteAcksForced`
    ///    applies leftover channel completions. TLS acks already applied
    ///    locally; the drain is a no-op. `deinit` asserts the channel empty.
    /// 4. `shutdownHandlers` runs after the loop and returns only when every
    ///    slot has passed through `releaseSlot`.
    /// 5. Close the socket exactly once, guarded by `socket_closed`.
    fn run(self: *Connection) !void {
        var torn_down = false;
        // The error path runs after `runBody`'s own `defer`, the order an
        // `errdefer` at the top of the body gave before Zig 0.17 removed
        // error capture from `errdefer`.
        self.runBody(&torn_down) catch |err| {
            if (comptime test_observe) {
                if (err == error.Canceled) _ = test_run_canceled.fetchAdd(1, .acq_rel);
            }
            if (!torn_down) {
                torn_down = true;
                self.teardownExhaustive();
                if (err == error.Canceled) zio.recancel();
            }
            return err;
        };
    }

    fn runBody(self: *Connection, torn_down: *bool) !void {
        const gpa = self.config.gpa;
        const io = self.config.io;
        var actor_node: @TypeOf(actor_marker).Node = .unset;
        actor_marker.set(&actor_node, self);
        defer actor_marker.clear(&actor_node);
        if (diag_task_handle_fn) |f| self.actor_task_h.store(f(), .release);
        if (comptime placement_check) self.actor_thread = std.Thread.getCurrentId();
        probeNote(.actor_start);

        var read_pump: wire_pump.ReadPump = undefined;
        var write_pump: wire_pump.WritePump = undefined;
        var tls_pump: tls_edge.Pump = undefined;
        var read_handle: ?zio.JoinHandle(void) = null;
        var write_handle: ?zio.JoinHandle(void) = null;
        var tls_started = false;
        var h2c_started = false;

        defer {
            if (self.diag_registered) diagDeregister(self);
            if (h2c_started) read_pump.stop();
            if (self.config.mode == .tls) {
                self.tls_write_ch.close(.graceful);
            } else {
                self.stopWritePump(if (h2c_started) &write_pump else null);
            }
            self.stream.shutdown(io, .both) catch {
                // Shutdown is best-effort; task cancellation is the authoritative unblock.
            };
            if (tls_started) {
                tls_pump.failDrain();
                self.tls_driver = null;
                tls_pump.shutdownCq();
            }
            if (write_handle) |*h| h.cancel();
            if (read_handle) |*h| h.cancel();
            self.drainWriteAcksForced();
            if (!self.socket_closed.swap(true, .acq_rel)) {
                self.stream.close(io);
                if (comptime test_observe) noteTestSocketClose();
            }
        }

        if (self.config.mode == .tls) {
            // One handshake site, `Server.serveTlsThenBranch`: it must pick
            // HTTP/2 or HTTP/1.1 from ALPN before a Connection exists.
            self.tls = self.config.tls_ready orelse
                @panic("tls mode needs tls_ready: the handshake runs in Server.serveTlsThenBranch");
            if (self.handshake_held) {
                if (self.config.accounting) |a| a.releaseHandshake();
                self.handshake_held = false;
            }
            tls_pump = .{
                .io = io,
                .conn = self.tls.?,
                .to_actor = &self.read_ch,
                .write_ch = &self.tls_write_ch,
                .sock = tlsNetHandle(self.stream.socket.handle),
                .recv_buf = self.tls_read_buf,
                .site = &self.tls_pump_site,
                .task_h = &self.tls_pump_task_h,
                .task_handle_fn = diag_task_handle_fn,
                .completions = &self.write_ack_ch,
                .ack_apply = struct {
                    fn f(ctx: *anyopaque, ack: wire_pump.WriteCompletion) void {
                        const c: *Connection = @ptrCast(@alignCast(ctx));
                        c.takeWriteAck(ack);
                    }
                }.f,
                .ack_ctx = self,
                .gpa = gpa,
                .chunk_storage = self.read_chunk_storage,
                .n_chunks = self.read_pool_n,
                .read_free = &self.read_free_ch,
                .write_free = &self.write_pool,
                .live_task_handlers = &self.live_task_handlers,
                .test_delay_ms = if (comptime test_observe) test_write_delay_ms else 0,
                .test_fail_after = if (comptime test_observe) test_write_fail_after else 0,
            };
            if (!tls_pump.start()) return;
            self.tls_driver = &tls_pump;
            tls_started = true;
            self.sched.pause = struct {
                fn f(ctx: *anyopaque) bool {
                    const c: *Connection = @ptrCast(@alignCast(ctx));
                    const p = c.tls_driver orelse return false;
                    return p.pending_n > 0 or p.stashFull() or c.outboundMemoryPaused();
                }
            }.f;
            // Stash-full only. pending_n must not pause WINDOW_UPDATE (stall p99).
            self.sched.pause_control = struct {
                fn f(ctx: *anyopaque) bool {
                    const c: *Connection = @ptrCast(@alignCast(ctx));
                    const p = c.tls_driver orelse return false;
                    return p.stashFull() or c.outboundMemoryPaused();
                }
            }.f;
            self.sched.pause_ctx = self;
            // After `tls_driver` is set: `barrierRoom` reads the stash through it.
            self.ack_room_armed = true;
            if (diag_park) {
                self.diag_registered = true;
                diagRegister(self);
            }
        } else {
            read_pump = .{
                .io = io,
                .stream = self.stream,
                .to_actor = &self.read_ch,
                .chunk_storage = self.read_chunk_storage,
                .n_chunks = self.read_pool_n,
                .free_indices = &self.read_free_ch,
                .live_task_handlers = &self.live_task_handlers,
            };
            write_pump = .{
                .io = io,
                .stream = self.stream,
                .from_actor = &self.write_ch,
                .completions = &self.write_ack_ch,
                .ledger = &self.acks,
                .gpa = gpa,
                .free_indices = &self.write_pool,
                .test_delay_ms = if (comptime test_observe) test_write_delay_ms else 0,
                .test_fail_after = if (comptime test_observe) test_write_fail_after else 0,
            };
            read_handle = try zio.spawnInto(connPlacement(), runReadPump, .{ self, &read_pump });
            write_handle = try zio.spawnInto(connPlacement(), runWritePump, .{ self, &write_pump });
            h2c_started = true;
            self.ack_room_armed = true;
            self.sched.pause = h2cPauseData;
            self.sched.pause_control = h2cPause;
            self.sched.pause_ctx = self;
        }

        // The server preface (SETTINGS, queued by `Session.init`) leaves the
        // scheduler BEFORE anything the client sent is ingested (RFC 9113
        // section 3.4: it must be the first frame). Ingest can queue
        // RST_STREAM or GOAWAY, which the scheduler drains ahead of ordinary
        // controls, so it must not run while SETTINGS is still queued. On TLS
        // the handshake leftover (a pipelined client preface and requests) is
        // the first client bytes; it is ingested after this flush and before
        // the loop first takes a completion off the pump's queue, so it still
        // reaches Session before any byte the pump received. Emit after the CQ is armed so queueWire can
        // encrypt.
        {
            self.lockSessionUncancelable(io);
            defer self.unlockSession(io);
            try self.flushSessionIntents();
            if (self.config.tls_leftover.len != 0) {
                std.debug.assert(self.config.mode == .tls);
                _ = h2_work.tls_leftovers.fetchAdd(1, .monotonic);
                if (comptime test_observe) _ = test_tls_leftover_connections.fetchAdd(1, .acq_rel);
                try self.ingestAndProcess(self.config.tls_leftover);
            }
            // A connection error in the first flight starts the closing phase
            // here, under the lock: its GOAWAY is stashed behind the SETTINGS
            // and the loop below writes both.
            self.beginClosingIfTerminal();
        }
        try self.runPendingInline();

        if (close_probe) diagRawPrint("t1002 conn={x} sess={x} start peer={d} parked={d}\n", .{ @intFromPtr(self), @intFromPtr(&self.session), closeProbePeerPort(self.stream.socket.handle), close_probe_parked.load(.acquire) });
        if (self.config.mode == .h2c) {
            self.preface_deadline = std.Io.Timestamp.fromNanoseconds(
                @as(i96, nowNs(io) +% self.config.limits.preface_timeout_ns),
            );
            try self.waitH2cPreface();
            try self.runPendingInline();
        }

        // An end of stream met among the extra chunks of a read batch.
        var inbound_eof = false;
        // Out-of-lock exits (TLS pump gone, a failed read) set this and
        // continue. In-lock terminal branches freeze and sweep under the
        // hold they already have; they must not drop and reacquire.
        var leaving = false;
        while (true) {
            probeNote(.actor_turn);
            if (comptime test_observe) self.releases_at_turn_top = self.releases_started.load(.seq_cst);
            _ = self.sched_refilled.swap(false, .acq_rel);
            self.drainWriteAcks();
            if (self.writer_failed.load(.acquire)) self.closeWriterQueues();
            _ = self.drainCompletions();
            try self.drainPendingCompleteReceipts(false);
            if (self.writer_failed.load(.acquire)) self.closeWriterQueues();
            diagActor(self, .inline_handlers);
            if (!leaving and !self.writer_failed.load(.acquire)) try self.runPendingInline();

            {
                diagActor(self, .loop_lock);
                self.lockSessionUncancelable(io);
                diagActor(self, .lk_checks);
                defer self.unlockSession(io);
                defer diagActor(self, .running);
                if (self.writer_failed.load(.acquire)) self.handleWriterFailed();
                // `.transport` is the writer-failed terminal: nothing to deliver.
                if (leaving or self.session.terminal == .transport) {
                    self.freezeAndSweepLocked();
                    break;
                }
                self.beginClosingIfTerminal();
                // While closing, none of the checks below run: no deadline is
                // enforced, no graceful step is taken and no parked request is
                // dispatched. Only the emit that follows does, so a frame still
                // in the scheduler reaches the writer.
                if (self.closing_deadline_ns == null) {
                    const now = nowNs(io);
                    self.session.edge_now_ns = now;
                    self.session.checkIdleDeadlines(now) catch |err| try self.sessionErrorExit(err, "idle_deadline_error");
                    const graceful_done = self.maybeBeginGraceful() catch |err| blk: {
                        try self.sessionErrorExit(err, "graceful_error");
                        break :blk false;
                    };
                    if (graceful_done) {
                        // The drain is over; its RST_STREAMs are queued.
                        self.beginClosing();
                    } else {
                        self.checkSlowConsumers() catch |err| try self.sessionErrorExit(err, "slow_consumer_error");
                        // Deadlines and slow-consumer kills push Session intents
                        // (RST_STREAM, stream_reset). drainEmit only walks the
                        // scheduler, so those intents have to be materialized first or
                        // the RST'd streams keep their pending DATA, drain skips them
                        // forever, and the actor parks until slow-consumer. That is
                        // the TLS stall snapshot: pending=10, tomb_rst=10, pend_slots=0.
                        // Slots freed since the last turn go to parked requests before
                        // any new one, so a waiting request is not overtaken.
                        self.dispatchParked() catch |err| try self.sessionErrorExit(err, "dispatch_parked_error");
                    }
                }
                diagActor(self, .lk_materialize);
                self.materializeIntents() catch {
                    // Terminal enqueue failure sets writer_failed inside
                    // materializeIntents. Ordinary PoolFull is per-stream.
                    self.failClosedOnIntentError();
                };
                diagActor(self, .lk_drain_emit);
                self.drainEmit() catch |err| {
                    // A cancel is not a write failure: leave as `error.Canceled`.
                    if (err == error.Canceled) return err;
                    // Fail-closed: Session may already be debited — terminate connection.
                    // A frame that failed after its flow-control debit cannot
                    // be retried and cannot be un-debited, so the connection's
                    // window state no longer matches the peer's. The only
                    // correct move left is to close.
                    self.noteServerClose("loop_drain_catch", err);
                    self.writer_failed.store(true, .release);
                    self.handleWriterFailed();
                };
                if (self.writer_failed.load(.acquire)) self.handleWriterFailed();

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
                diagActor(self, .lk_tail);
                self.assertSessionHeld("graceful finish check");
                if (self.closing_deadline_ns == null) {
                    if (self.session.terminal == .goaway) {
                        // Made terminal this turn (a deadline above), and
                        // `drainEmit` has just queued its GOAWAY.
                        self.beginClosing();
                    } else if (self.session.terminal != .none) {
                        self.freezeAndSweepLocked();
                        break;
                    } else if (self.shutting_down and self.session.grace_phase == .phase2 and
                        // Graceful finish needs all four conditions together: phase 2
                        // reached, nothing left to write, no handler still running,
                        // and no request waiting for a slot. Any one alone would cut
                        // off work that is still in flight.
                        self.sched.pendingCount() == 0 and self.live_handlers.load(.acquire) == 0 and
                        self.parked_len == 0)
                    {
                        try self.finishGraceful();
                        self.beginClosing();
                    }
                }
                // Checked in the turn that began the phase too. If nothing was
                // queued, no write completion will ever wake the actor, and it
                // would wait out the whole deadline.
                if (self.closing_deadline_ns) |deadline| {
                    if (self.closeFlushed(deadline)) {
                        self.freezeAndSweepLocked();
                        break;
                    }
                }
            }
            // The donate-yield that followed fire-due is gone with the heap:
            // a handler deadline parks on its own timed wait, so zio makes
            // the waiter runnable without the actor's help.
            try self.runPendingInline();

            if (self.config.mode == .tls) {
                const leave = try self.driveTlsTurn();
                // An EOF or close_notify the pump saw this turn.
                if (!leave and self.tls_driver.?.inbound_eof) try self.onPeerEof();
                if (leave) self.beginLeave(&leaving);
                continue;
            }

            var maybe_chunk = io_queue.tryRecv(wire_pump.WireChunk, &self.read_ch);
            if (maybe_chunk == null) {
                // Park directly: every producer either sends on a selected
                // channel or rings the doorbell, and both survive until
                // taken, so the select's fast path sees any work that
                // arrived since the drains above.
                maybe_chunk = try self.waitForActivity();
                if (maybe_chunk == null) continue;
            }
            const chunk = maybe_chunk.?;
            // Closing phase: read and drop (an EOF too: the write side may
            // still be open, and the GOAWAY is for a peer that half-closed).
            // The EOF is noted: once the output is flushed it ends the
            // lingering close (`closeFlushed`).
            if (chunk.len == 0 and chunk.bytes.len == 0) {
                // A failed read fails the connection; the peer's EOF is an
                // event (`onPeerEof`), in the closing phase or before it.
                if (read_pump.read_failed.load(.acquire)) {
                    self.noteServerClose("read_failed", null);
                    self.writer_failed.store(true, .release);
                    self.beginLeave(&leaving);
                } else {
                    try self.onPeerEof();
                }
                continue;
            }
            if (self.closing_deadline_ns != null) {
                self.recycleReadChunk(chunk);
                continue;
            }
            if (trace.enabled) self.noteReadBacklog();
            // In-chunk frames borrow the wire buffer until handleFrame copies.
            // Recycle after the drain-turn, same lifetime as the one-chunk defer.
            var held: [inbound_chunk_batch]wire_pump.WireChunk = undefined;
            var held_n: usize = 0;
            defer {
                var i: usize = 0;
                while (i < held_n) : (i += 1) self.recycleReadChunk(held[i]);
            }
            held[0] = chunk;
            held_n = 1;
            var leave = try self.ingestWireChunk(chunk);
            if (!leave) {
                var extra_i: usize = 1;
                while (extra_i < inbound_chunk_batch) : (extra_i += 1) {
                    const extra = io_queue.tryRecv(wire_pump.WireChunk, &self.read_ch) orelse break;
                    if (extra.len == 0 and extra.bytes.len == 0) {
                        // Handled after the batch, like the first chunk's.
                        inbound_eof = true;
                        break;
                    }
                    switch (try self.inboundBatchShouldStop()) {
                        .go => {},
                        .terminal => leave = true,
                        .failed => {},
                    }
                    if (trace.enabled) self.noteReadBacklog();
                    held[held_n] = extra;
                    held_n += 1;
                    leave = try self.ingestWireChunk(extra) or leave;
                    if (leave) break;
                    if (self.writer_failed.load(.acquire)) break;
                }
            }
            if (trace.enabled) noteInboundBatch(held_n);
            // An end of stream behind the batch, after its frames.
            if (inbound_eof) {
                inbound_eof = false;
                if (read_pump.read_failed.load(.acquire)) {
                    self.noteServerClose("read_failed", null);
                    self.writer_failed.store(true, .release);
                } else {
                    try self.onPeerEof();
                }
            }
            // Session went terminal. If that began the closing phase, the lock
            // block above drives it; otherwise the writer failed: leave.
            if (self.writer_failed.load(.acquire) or (leave and self.closing_deadline_ns == null)) {
                self.beginLeave(&leaving);
                continue;
            }
            if (leave) continue;
            try self.runPendingInline();
        }

        self.teardownExhaustive();
        torn_down.* = true;
        // endShield only drops shield_count. A cancel that arrived during
        // the shield is still pending; consume and return it.
        try zio.checkCancel();
        if (close_probe) diagRawPrint("t1002 conn={x} shutdown_done parked={d}\n", .{ @intFromPtr(self), close_probe_parked.load(.acquire) });
    }

    /// Last owned turn, then lock-free wait. Every `run` exit uses this,
    /// including errdefer: a complete job can sit in `inline_sids` with
    /// `teardown_inline_n == 0` if an intent error skipped the loop freeze.
    /// Close write queues before the lock: a handler can park in `putOne`
    /// while holding `session_mu`.
    fn teardownExhaustive(self: *Connection) void {
        std.debug.assert(shutdown_sweep.get() == null);
        if (comptime test_observe) _ = test_actor_teardowns.fetchAdd(1, .release);
        self.closeWriterQueues();
        {
            self.lockSessionUncancelable(self.config.io);
            defer self.unlockSession(self.config.io);
            self.freezeAndSweepLocked();
        }
        zio.beginShield();
        defer zio.endShield();
        if (close_reason and self.config.mode == .h2c) {
            diagRawPrint("ACK_LEDGER conn={x} high_water={d} capacity={d} owed={d}\n", .{ @intFromPtr(self) & 0xffffff, self.acks.high_water.load(.acquire), self.write_ack_buf.len, self.acks.owed.load(.acquire) });
        }
        if (close_reason) {
            // One line per connection, even when nothing was refused: a run
            // whose budget never filled proves nothing about the budget, so
            // the high-water (largest bytes held at once) is printed beside
            // the limit, with how many writes parked and how many were refused.
            diagRawPrint("STREAM_BUDGET conn={x} mode={s} stream_budget_refusals={d} lim={d} high_water={d} budget_parks={d} server_ended={}\n", .{ @intFromPtr(self) & 0xffffff, @tagName(self.config.mode), self.stream_budget_refusals, self.config.limits.outbound_bytes_per_connection, self.outbound_high_water.load(.acquire), self.budget_parks.load(.acquire), self.close_reason_noted });
        }
        if (close_probe) diagRawPrint("t1002 conn={x} loop_exit terminal={s} sched_pending={d} parked={d}\n", .{ @intFromPtr(self), @tagName(self.session.terminal), self.sched.pendingCount(), close_probe_parked.load(.acquire) });
        self.finishTerminalInline(self.teardown_inline_n);
        self.teardown_inline_n = 0;
        self.shutdownHandlers();
        std.debug.assert(self.countInUseSlots() == 0);
    }

    fn receiveUntilDeadline(self: *Connection) !wire_pump.WireChunk {
        const io = self.config.io;
        const timeout: zio.Timeout = if (self.preface_deadline) |deadline| blk: {
            const deadline_ns: u64 = @intCast(deadline.nanoseconds);
            const now = nowNs(io);
            if (now >= deadline_ns) return error.PrefaceTimeout;
            break :blk .{ .duration = .fromNanoseconds(deadline_ns - now) };
        } else .none;
        const shutdown_ev = self.config.shutdown_event orelse &no_shutdown_event;
        const winner = try zio.select(.{
            .read = self.read_ch.asyncReceive(),
            .shutdown = shutdown_ev,
            .timer = timeout,
        });
        return switch (winner) {
            .read => |result| result catch error.ConnectionClosed,
            .shutdown => error.ConnectionClosed,
            .timer => error.PrefaceTimeout,
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

    fn waitH2cPreface(self: *Connection) !void {
        while (self.session.parser.expecting_preface) {
            const chunk = try self.receiveUntilDeadline();
            defer self.recycleReadChunk(chunk);
            if (chunk.len == 0) return error.ConnectionClosed;
            self.lockSessionUncancelable(self.config.io);
            {
                defer self.unlockSession(self.config.io);
                try self.ingestAndProcess(chunk.bytes[0..chunk.len]);
                // A bad preface is a connection error. Return to the main
                // loop, which delivers its GOAWAY.
                if (self.session.terminal != .none) {
                    self.beginClosingIfTerminal();
                    return;
                }
            }
        }
        self.preface_deadline = null;
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
        for (self.handlers.slice(), 0..) |*slot, i| {
            if (!slot.in_use) return self.claimSlot(slot, i, stream_id);
        }
        const i = self.handlers.touched;
        const slot = self.handlers.touchNext(.{}) orelse return null;
        self.space_events[i] = .init;
        self.deadline_events[i] = .init;
        return self.claimSlot(slot, i, stream_id);
    }

    fn claimSlot(self: *Connection, slot: *HandlerSlot, i: usize, stream_id: u31) *HandlerSlot {
        std.debug.assert(!slot.in_use);
        std.debug.assert(i < self.handlers.touched);
        std.debug.assert(stream_id != 0);
        slot.in_use = true;
        self.slots_claimed += 1;
        if (comptime test_observe) _ = test_observed_slots_in_use.fetchAdd(1, .acq_rel);
        slot.stream_id = stream_id;
        slot.terminal.clear();
        _ = slot.terminal.generation.fetchAdd(1, .acq_rel);
        slot.completion_owner.store(live, .release);
        slot.finalize_count.store(0, .release);
        slot.admitted = false;
        slot.reaper_reserved = false;
        self.handler_joins[i] = null;
        if (i < self.space_events.len) self.space_events[i].reset();
        if (i < self.deadline_events.len) self.deadline_events[i].reset();
        std.debug.assert(slot.in_use);
        std.debug.assert(!slot.admitted);
        std.debug.assert(slot.finalize_count.load(.acquire) == 0);
        return slot;
    }

    /// Production admission. Dispatch is the only caller besides the
    /// double-finalize trap, which must use this rather than forging counters.
    fn admitHandler(self: *Connection, slot: *HandlerSlot) void {
        std.debug.assert(slot.in_use);
        std.debug.assert(!slot.admitted);
        slot.admitted = true;
        _ = self.live_handlers.fetchAdd(1, .acq_rel);
        if (comptime test_observe) _ = test_observed_live_handlers.fetchAdd(1, .acq_rel);
        std.debug.assert(slot.admitted);
    }

    /// Test-only: a lease the way dispatch leaves one, already handed to a job.
    fn borrowTestLease(self: *Connection, stream_id: u31) *RequestLease {
        const lease = self.newLease(stream_id) orelse std.debug.panic("no free test lease", .{});
        lease.in_job = true;
        return lease;
    }

    /// A free lease for `stream_id`, entered in the index: from the free stack,
    /// else the next untouched one. Null when all are busy.
    fn newLease(self: *Connection, stream_id: u31) ?*RequestLease {
        const lease = if (self.lease_index.takeFree()) |pos|
            &self.request_leases.storage[pos]
        else
            self.request_leases.touchNext(.{}) orelse return null;
        lease.* = .{ .stream_id = stream_id, .arena = .init(self.config.request_pool.?, &self.request_budget) };
        self.lease_index.insert(self.request_leases.storage, self.leasePos(lease));
        return lease;
    }

    fn leasePos(self: *Connection, lease: *RequestLease) u32 {
        return @intCast(lease - self.request_leases.storage.ptr);
    }

    /// The lease holding `stream_id`'s request memory. Actor-only.
    fn findLease(self: *Connection, stream_id: u31) ?*RequestLease {
        const pos = self.lease_index.find(self.request_leases.storage, stream_id) orelse return null;
        return &self.request_leases.storage[pos];
    }

    /// Where `stream_id`'s header block decodes: its existing lease (trailers),
    /// or a new one. When the pool has no memory for it, or every lease is
    /// busy (which the lease table's size should make unreachable), the block
    /// still has to be decoded, so the HPACK table advances; that is
    /// `hpack_scratch`, and the status the stream is then answered with.
    /// Actor-only.
    ///
    /// Either way the decode cannot run out of memory afterwards: the lease
    /// has been given room for the largest decode there can be
    /// (`Limits.hpackDecodeBytes`) before this returns it, and the scratch
    /// is that size.
    fn borrowHeaderListMem(self: *Connection, stream_id: u31) session_mod.HeaderListMem {
        const need = self.config.limits.hpackDecodeBytes() catch unreachable;
        if (self.findLease(stream_id)) |l| {
            if (l.arena.reserve(need)) |_| {
                return self.requestMem(l);
            } else |_| {
                return self.scratchMem((l.arena.refusal orelse request_pool.Reason.server_cap).status());
            }
        }
        if (comptime test_observe) {
            if (test_no_request_lease.load(.acquire)) return self.scratchMem(503);
        }
        const lease = self.newLease(stream_id) orelse return self.scratchMem(503);
        if (lease.arena.reserve(need)) |_| {
            return self.requestMem(lease);
        } else |_| {
            const status = (lease.arena.refusal orelse request_pool.Reason.server_cap).status();
            self.releaseLease(lease);
            return self.scratchMem(status);
        }
    }

    /// The request's arena for the decoded list, and the slot's scratch for the
    /// decoder's transient room, emptied for this decode.
    fn requestMem(self: *Connection, lease: *RequestLease) session_mod.HeaderListMem {
        self.hpack_scratch.reset();
        return .{ .request = .{ .alloc = lease.arena.allocator(), .scratch = self.hpack_scratch.allocator() } };
    }

    fn scratchMem(self: *Connection, status: u16) session_mod.HeaderListMem {
        self.hpack_scratch.reset();
        return .{ .scratch = .{ .alloc = self.hpack_scratch.allocator(), .status = status } };
    }

    /// Give back `stream_id`'s request memory if no handler holds it: a decode
    /// Session dropped, or a dispatch that refused the stream. A lease that
    /// dispatch handed to a job is left alone; `releaseSlot` returns it after
    /// the handler was joined. Actor-only.
    fn releaseHeaderList(self: *Connection, stream_id: u31) void {
        const lease = self.findLease(stream_id) orelse return;
        if (lease.in_job) return;
        self.releaseLease(lease);
    }

    fn releaseLease(self: *Connection, lease: *RequestLease) void {
        if (comptime test_observe) std.debug.assert(handler_job_marker.get() == null);
        lease.arena.release();
        self.lease_index.remove(self.request_leases.storage, lease.stream_id);
        lease.stream_id = 0;
        lease.in_job = false;
        self.lease_index.putFree(self.leasePos(lease));
    }

    fn slotIndex(self: *Connection, stream_id: u31) ?usize {
        for (self.handlers.slice(), 0..) |slot, i| {
            if (slot.in_use and slot.stream_id == stream_id) return i;
        }
        return null;
    }

    /// Free a slot after its handler has finished. Actor-only.
    ///
    /// The `join` is the memory-safety barrier and not bookkeeping. The
    /// handler's arena is destroyed when the handler task returns, so the slot
    /// may only be reused after this task confirms it has really stopped.
    ///
    /// The reaper token is released here, at the single point where the slot's
    /// lifetime actually ends. Releasing it earlier — at cancel time — would
    /// hand the capacity to a new stream while the old handler was still
    /// running.
    /// Returns true only when an in-use slot became free. A stale or duplicate
    /// SID returns false and must not count as teardown progress.
    fn releaseSlot(self: *Connection, stream_id: u31) bool {
        const i = self.slotIndex(stream_id) orelse return false;
        const slot = &self.handlers.slice()[i];
        std.debug.assert(slot.in_use);
        const n = slot.finalize_count.load(.acquire);
        if (slot.admitted) {
            if (std.debug.runtime_safety) {
                if (n == 0) {
                    std.debug.panic("handler slot sid={d} released with no finalize", .{stream_id});
                }
                if (n != 1) {
                    std.debug.panic("handler slot sid={d} finalize_count={d} at release", .{ stream_id, n });
                }
            }
        } else {
            std.debug.assert(n == 0);
            self.slots_rolled_back += 1;
        }
        if (self.handler_joins[i]) |*handle| handle.join();
        self.handler_joins[i] = null;
        // The handler has stopped, so nothing allocates from the request's
        // arena any more. The actor gives its blocks back, here and nowhere
        // else, so a block is returned by the thread that took it.
        if (self.findLease(stream_id)) |lease| self.releaseLease(lease);
        if (slot.reaper_reserved) {
            if (self.config.accounting) |a| a.releaseReaper();
            slot.reaper_reserved = false;
        }
        self.assertBudgetEntryIdle(slot, i);
        slot.in_use = false;
        slot.admitted = false;
        if (comptime test_observe) _ = test_observed_slots_in_use.fetchSub(1, .acq_rel);
        slot.terminal.clear();
        slot.completion_owner.store(reported, .release);
        std.debug.assert(!slot.in_use);
        return true;
    }

    /// A slot goes back to the pool with its budget entry idle and out of the
    /// queue: a queued entry would leave a freed slot linked, and a granted one
    /// would leak its bytes. This does NOT repair either; it stops, with the
    /// stream and the state, so the path that missed the entry is found. Safe
    /// builds only.
    fn assertBudgetEntryIdle(self: *Connection, slot: *HandlerSlot, index: usize) void {
        if (!std.debug.runtime_safety) return;
        self.budget_mu.lockUncancelable();
        defer self.budget_mu.unlock();
        const linked = self.budget_head == index or self.budget_tail == index or blk: {
            var cur = self.budget_head;
            while (cur != budget_none) : (cur = self.handlers.storage[cur].budget.next) {
                if (cur == index) break :blk true;
            }
            break :blk false;
        };
        if (slot.budget.state != .idle or linked) {
            std.debug.panic("handler slot sid={d} released with a budget entry: state={s} inline={} linked={} need={d}", .{
                slot.stream_id, @tagName(slot.budget.state), slot.budget.is_inline, linked, slot.budget.need,
            });
        }
    }

    /// Stop a handler because its stream is over — a peer RST, or a local
    /// reset. Actor-only.
    ///
    /// Publish the terminal cause and wake every wait this stream owns.
    /// The handler returns on its next Response call or wait, then posts.
    /// There is no JoinHandle.cancel: that path cannot reclaim an uncancelable
    /// wait, and it is the Futex handshake that hung teardown.
    fn cancelHandler(self: *Connection, stream_id: u31, cause: response.TerminalCause) void {
        const i = self.slotIndex(stream_id) orelse {
            // The handler already posted its completion and released the slot.
            // RST still owns any scheduler body queued for this id: leaving it
            // makes `emitOneData` skip forever (RST tombstone ⇒ zero credit)
            // and the actor parks until slow-consumer. That is the TLS stall.
            self.wakeHandlerWaiters(stream_id);
            return;
        };
        const slot = &self.handlers.slice()[i];
        slot.terminal.setCause(cause);
        self.wakeHandlerWaiters(stream_id);
    }

    /// Release everything a terminal stream still holds, and wake whoever waits
    /// on it. This is called before any cancellation decision, so that a
    /// handler which is about to be left alone still has its wake guaranteed.
    fn wakeHandlerWaiters(self: *Connection, stream_id: u31) void {
        self.assertSessionHeld("wakeHandlerWaiters");
        std.debug.assert(stream_id != 0);
        // A complete job waiting for budget (queued, or granted and not yet
        // run) leaves the queue with its stream, and a grant goes back, once.
        // A parked task writer leaves by itself when it wakes below.
        if (self.slotIndex(stream_id)) |i| self.abandonInlineBudget(&self.handlers.storage[i], i);
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
            std.debug.assert(self.sched.findPending(stream_id) == null);
        }
        self.wakeStreamSpace(stream_id);
        self.wakeHandlerDeadline(stream_id);
    }

    /// Set terminal causes and release scheduler pending bytes. The actor
    /// already holds `session_mu` on this turn. `shutdownHandlers` must not
    /// take the mutex: a handler can park inside it on `write_ch.putOne`.
    fn sweepHandlerWaitersLocked(self: *Connection) void {
        self.assertSessionHeld("sweepHandlerWaitersLocked");
        std.debug.assert(self.handlers.capacity() != 0);
        for (self.handlers.slice()) |*slot| {
            if (!slot.in_use) continue;
            std.debug.assert(slot.stream_id != 0);
            slot.terminal.setCause(.server_shutdown);
            self.wakeHandlerWaiters(slot.stream_id);
        }
        var n: usize = 0;
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
        var collect: CollectCtx = .{ .c = self, .n = &n };
        self.sched.forEachPending(@ptrCast(&collect), CollectCtx.cb);
        std.debug.assert(n <= self.sid_scratch.len);
        var i: usize = 0;
        while (i < n) : (i += 1) {
            self.wakeHandlerWaiters(self.sid_scratch[i]);
        }
        std.debug.assert(self.sched.pendingCount() == 0);
    }

    /// Freeze the inline queue and run the cause-and-ledger sweep. The caller
    /// already holds `session_mu`. Do not drop that hold and reacquire.
    fn freezeAndSweepLocked(self: *Connection) void {
        self.assertSessionHeld("freezeAndSweepLocked");
        std.debug.assert(self.inline_n <= self.inline_sids.len);
        // A second freeze (closing phase, then teardown) must find nothing new
        // to freeze, or it would overwrite the first batch and leak its jobs.
        std.debug.assert(self.inline_n == 0 or self.teardown_inline_n == 0);
        if (self.inline_n != 0) {
            self.teardown_inline_n = self.inline_n;
            self.inline_n = 0;
        }
        std.debug.assert(self.inline_n == 0);
        std.debug.assert(self.teardown_inline_n <= self.inline_sids.len);
        self.releaseParked();
        self.sweepHandlerWaitersLocked();
    }

    /// Close the write queue (and the TLS twin), so a push after this fails
    /// instead of landing in a queue nobody drains, before this task waits for
    /// `session_mu`. Close is idempotent.
    fn closeWriterQueues(self: *Connection) void {
        std.debug.assert(self.write_ch_buf.len != 0);
        if (self.config.mode != .tls) {
            std.debug.assert(self.tls_write_buf.len == 0);
        }
        self.write_queue_ending.store(true, .seq_cst);
        self.write_ch.close(self.config.io);
        if (self.tls_write_buf.len != 0) {
            self.tls_write_ch.close(.graceful);
        }
    }

    /// Teardown's first word to the h2c writer: stop, and wake it if it is
    /// parked on an empty queue. The sentinel is a chunk in the queue that no
    /// push counted in `acks.owed`, so the order is the point: the ending
    /// flag first (it makes `h2cQueueRoom` exact and a full push forgivable),
    /// then the pump's stop flag, then the sentinel. A push that finds the
    /// queue full after the sentinel went in therefore finds the flag set.
    /// The sentinel is put without waiting; a full queue already has chunks
    /// that wake the pump.
    fn stopWritePump(self: *Connection, pump: ?*wire_pump.WritePump) void {
        self.write_queue_ending.store(true, .seq_cst);
        if (pump) |p| p.stop();
        _ = self.write_ch.putUncancelable(self.config.io, &.{.{ .bytes = &.{}, .len = 0 }}, 0) catch {
            // A closed writer queue already terminates the pump.
        };
    }

    /// Close write queues and mark the loop to sweep on the next existing
    /// lock. Do not split these: a leave without close waits behind putOne.
    /// This helper does not lock.
    fn beginLeave(self: *Connection, leaving: *bool) void {
        std.debug.assert(shutdown_sweep.get() == null);
        self.closeWriterQueues();
        leaving.* = true;
        std.debug.assert(leaving.*);
    }

    /// Start the closing phase (see `runBody`). The caller holds `session_mu`,
    /// and the last frames the connection will send are already queued: a
    /// terminal GOAWAY, or the RST_STREAMs that end a graceful drain.
    ///
    /// The sweep runs now, not at the end, so that no handler writes more
    /// while the phase waits and no inline job runs against a closing
    /// connection. `teardownExhaustive` runs it again; it is idempotent.
    fn beginClosing(self: *Connection) void {
        self.assertSessionHeld("beginClosing");
        std.debug.assert(self.closing_deadline_ns == null);
        self.closing_deadline_ns = nowNs(self.config.io) +% self.config.limits.close_flush_timeout_ns;
        self.freezeAndSweepLocked();
    }

    /// `.goaway` is the only terminal state with a frame to deliver:
    /// `.transport` means the writer already failed.
    fn beginClosingIfTerminal(self: *Connection) void {
        self.assertSessionHeld("beginClosingIfTerminal");
        if (self.closing_deadline_ns != null) return;
        if (self.session.terminal != .goaway) return;
        self.beginClosing();
    }

    /// The peer closed its side: a socket EOF, or TLS close_notify. An event,
    /// not a failure (a failed read is one, and the caller leaves instead).
    /// The peer may still read, so what is queued for it is delivered: a
    /// connection that is not ending yet ends now, with GOAWAY(NO_ERROR)
    /// (`Session.peerClosed`), and the closing phase writes everything
    /// queued. Its lingering close then ends as soon as the output is
    /// flushed, because the peer's EOF is already here (`closeFlushed`).
    fn onPeerEof(self: *Connection) !void {
        if (self.closing_peer_eof) return;
        self.closing_peer_eof = true;
        if (comptime test_observe) _ = test_peer_eofs.fetchAdd(1, .monotonic);
        const io = self.config.io;
        self.lockSessionUncancelable(io);
        defer self.unlockSession(io);
        if (self.closing_deadline_ns != null) return;
        if (self.session.terminal == .none) {
            self.session.peerClosed() catch |err| try self.sessionErrorExit(err, "peer_eof_error");
        }
        self.materializeIntents() catch self.failClosedOnIntentError();
        self.beginClosingIfTerminal();
    }

    /// The ONE place that decides the closing phase is over: everything queued
    /// was written and the peer closed, or the deadline passed, or the writer
    /// failed.
    ///
    /// Written means handed to the kernel, not delivered: on Linux the send
    /// queue takes hundreds of KiB from a peer that is not reading, GOAWAY
    /// included. Closing the socket then is not enough. If the peer writes
    /// anything after the close (a WINDOW_UPDATE, a PING), the kernel answers
    /// with a TCP RST (`TCPAbortOnData`), and the peer drops everything it has
    /// not read yet, the GOAWAY with it: on nachos the flood test's client got
    /// ECONNRESET with nothing read in about half the runs. So once flushed
    /// the send side is shut (a FIN after the queued bytes), the actor keeps
    /// reading and dropping, and the socket is closed when the peer closes its
    /// side or the closing deadline passes (a lingering close).
    fn closeFlushed(self: *Connection, deadline_ns: u64) bool {
        self.assertSessionHeld("closeFlushed");
        if (self.writer_failed.load(.acquire)) {
            if (comptime test_observe) _ = test_closing_ended_writer_failed.fetchAdd(1, .monotonic);
            return true;
        }
        if (nowNs(self.config.io) >= deadline_ns) {
            if (comptime test_observe) {
                if (self.outboundFlushed()) _ = test_closing_ended_deadline_flushed.fetchAdd(1, .monotonic) else _ = test_closing_ended_deadline_blocked.fetchAdd(1, .monotonic);
            }
            return true;
        }
        if (!self.outboundFlushed()) return false;
        // TLS: close_notify is the last record, staged once everything else
        // is written, and it drains under the same deadline before the send
        // side is shut.
        if (self.tls_driver) |pump| {
            if (!self.closing_notify_staged) {
                self.closing_notify_staged = true;
                if (!pump.stageCloseNotify()) return true;
                if (comptime test_observe) _ = test_close_notify_staged.fetchAdd(1, .monotonic);
                if (!self.outboundFlushed()) return false;
            }
        }
        if (!self.closing_write_shut) {
            self.closing_write_shut = true;
            self.stream.shutdown(self.config.io, .send) catch {};
            if (comptime test_observe) _ = test_closing_write_shut.fetchAdd(1, .monotonic);
        }
        if (comptime test_observe) {
            if (self.closing_peer_eof) _ = test_closing_ended_peer_eof.fetchAdd(1, .monotonic);
        }
        return self.closing_peer_eof;
    }

    /// True when nothing is left between Session and the socket: no intent,
    /// no scheduler entry, no chunk the writer has not acked, and on TLS no
    /// ciphertext the socket has not accepted. h2c acks a chunk after its
    /// flush, so for h2c "acked" already means "written".
    fn outboundFlushed(self: *Connection) bool {
        if (self.session.intentLen() != 0) return false;
        if (!self.sched.idle()) return false;
        // Not `wire_outbound_held`: the budget counts DATA payload bytes only,
        // so a chunk of headers and controls (a GOAWAY is the one that
        // matters here) holds none of it. The transport's own counts say
        // whether everything is written: h2c, the completions still owed for
        // pushed chunks; TLS, the stash, the staged records and the send.
        if (self.config.mode == .tls) return self.tls_driver.?.writesDrained();
        return self.acks.owed.load(.acquire) == 0;
    }

    /// The h2c drain's pause hooks. The scheduler asks before EVERY pop, so a
    /// drain with many entries cannot push past the room it checked, which a
    /// single check before `drain` could not promise. A pop is held back, and stays in the
    /// scheduler for the next drain, when:
    /// - `write_ack_ch` has no room for what the pop and the drain's tail can
    ///   add (`h2cAckRoom`). DATA stops one pop's room earlier than controls
    ///   (`h2cPauseData`), so a RST_STREAM or GOAWAY can still leave while
    ///   DATA waits: controls pack many to a chunk, and a terminal control
    ///   that cannot be queued fails the connection. The WritePump posts a
    ///   completion for every chunk with `trySend`, and a full channel is a
    ///   panic in the pump.
    ///   Nothing else bounds the completions in flight: the pump frees a
    ///   chunk's queue slot and its pool chunk BEFORE it posts, and a chunk
    ///   holds only its own wire bytes (at least 9) until the actor applies
    ///   the completion. The completion that frees room wakes the actor
    ///   (`waitForActivity` selects on the channel), and every actor turn
    ///   drains again, so a held pop is never stranded.
    /// - the write queue has no room for the pop's pushes and the tail
    ///   (`h2cQueueRoom`), so no push waits in `putOne`. This was only the
    ///   ending connection's rule; it is every connection's now, because a
    ///   bare flush barrier takes a queue slot and no pool chunk, so the
    ///   pool alone no longer bounds the queue.
    ///
    /// Stage 3 adds the first condition below to this one rule: the write
    /// pool has fewer than `POP_PUSHES` free chunks (`outboundMemoryPaused`).
    /// The three conditions are tested in the order pool, ack channel, write
    /// queue, and a pop is held if ANY holds. None can starve another: each
    /// holds only until a write completion, and a completion frees all three.
    /// The pump frees the chunk's queue slot when it takes the chunk and
    /// releases the chunk to the pool before it posts the completion; the
    /// actor frees the ack room when it takes the completion, and that same
    /// completion wakes the actor. So a held pop is
    /// retried after the resource that held it has been given back, and a
    /// condition that holds while the others do not never needs them to move.
    /// A frame made outside a pop leaves `POP_PUSHES` chunks free, so the pool
    /// condition cannot hold with nothing in flight to release a chunk.
    fn h2cPause(ctx: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ctx));
        return self.h2cPauseFor(0);
    }

    fn h2cPauseData(ctx: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ctx));
        return self.h2cPauseFor(ack_room_per_pop);
    }

    fn h2cPauseFor(self: *Connection, reserve: usize) bool {
        if (self.outboundMemoryPaused()) {
            if (comptime test_observe) {
                if (self.h2c_ending) _ = test_closing_emit_deferred.fetchAdd(1, .monotonic);
            }
            return true;
        }
        if (self.ack_room_armed and !self.h2cAckRoom(reserve)) {
            if (comptime test_observe) _ = test_ack_room_pauses.fetchAdd(1, .monotonic);
            return true;
        }
        if (self.h2cQueueRoom(h2c_queue_room_per_pop)) return false;
        if (comptime test_observe) {
            if (self.h2c_ending) _ = test_closing_emit_deferred.fetchAdd(1, .monotonic);
        }
        return true;
    }

    /// Queue slots one pop may fill: its pushes and the drain's tail push.
    const h2c_queue_room_per_pop = tls_edge.Pump.pushes_per_pop + tls_edge.Pump.tail_pushes_per_drain;

    /// True when the h2c write queue has `need` free slots. Every push is made
    /// under `session_mu`, which the caller holds, and only the pump takes
    /// from the queue, so a count that is enough stays enough until this task
    /// pushes. That makes the push after a true answer unable to wait.
    ///
    /// A queued chunk is owed a completion (`AckLedger`), so the queue never
    /// holds more than `owed`: when `owed + need` fits, the answer is yes
    /// without taking the queue's lock. The exact count is read only near
    /// the bound.
    fn h2cQueueRoom(self: *Connection, need: usize) bool {
        std.debug.assert(self.config.mode == .h2c);
        const capacity = self.write_ch_buf.len;
        // `owed` bounds the queue only while every entry is counted. The
        // teardown sentinel is not, so once the queue is ending the count is
        // read exactly (the flag is set before the sentinel goes in).
        if (!self.write_queue_ending.load(.acquire) and self.acks.owed.load(.acquire) + need <= capacity) return true;
        return capacity - io_queue.queued(wire_pump.WireChunk, &self.write_ch, self.config.io) >= need;
    }

    /// True when `write_ack_ch` can take the completions of every push one
    /// pop and the drain's tail can make, beside the pump's own terminal
    /// posts. Every push is counted in `acks` before it is made, under
    /// `session_mu`, which every drain holds; the actor subtracts without it,
    /// so a concurrent change only makes this answer more cautious.
    fn h2cAckRoom(self: *Connection, reserve: usize) bool {
        const owed = self.acks.owed.load(.acquire);
        return owed + ack_room_per_pop + reserve <= self.write_ack_buf.len;
    }

    /// Completions one pop may still add while `h2cAckRoom` holds: the pop's
    /// pushes, the drain's tail push, and the pump's terminal posts.
    const ack_room_per_pop = tls_edge.Pump.pushes_per_pop + tls_edge.Pump.tail_pushes_per_drain + wire_pump.terminal_posts;

    fn countInUseSlots(self: *const Connection) usize {
        std.debug.assert(self.handlers.capacity() != 0);
        var n: usize = 0;
        for (self.handlers.slice()) |s| {
            if (s.in_use) n += 1;
        }
        std.debug.assert(n <= self.handlers.touched);
        return n;
    }

    /// Run frozen complete jobs that never reached a drain turn. Causes are
    /// already set, so the body returns without emit. Does not take `session_mu`.
    fn finishTerminalInline(self: *Connection, n: usize) void {
        std.debug.assert(n <= self.inline_sids.len);
        std.debug.assert(shutdown_sweep.get() == null);
        if (n == 0) return;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const sid = self.inline_sids[i];
            const job = if (self.slotIndex(sid)) |idx| &self.handler_jobs[idx] else continue;
            std.debug.assert(job.slot.in_use);
            runHandlerJobBody(job);
            finishHandlerJob(job);
        }
        _ = self.drainCompletions();
    }

    /// Release the ticket slots of drain-turn receipts still in flight. Does
    /// not wait on a ticket: `waitTicket` can take `session_mu`, and an
    /// unsignaled wait is the actor waiting for itself. `failAll` has already
    /// run. The batches' handler slots were freed when their drain turn ended.
    fn finishPendingReceiptsNoWait(self: *Connection) void {
        std.debug.assert(shutdown_sweep.get() != null);
        std.debug.assert(self.pending_complete_receipt_n <= complete_receipt_capacity);
        while (self.pending_complete_receipt_n > 0) {
            const receipt = self.pending_complete_receipts[0];
            std.debug.assert(receipt.ticket_slot != ticket_table.no_completion_slot);
            self.tickets.releaseReserved(receipt.ticket_slot);
            self.popCompleteReceipt();
        }
    }

    fn assertTeardownPosters(self: *const Connection) void {
        std.debug.assert(self.handlers.capacity() != 0);
        for (self.handlers.slice(), 0..) |*slot, i| {
            if (!slot.in_use) continue;
            const owner = slot.completion_owner.load(.acquire);
            if (owner != live and owner != reaper_owned and owner != reaper_running and owner != reported) {
                std.debug.panic("teardown slot sid={d} owner={d} is not live/reaper/reported", .{ slot.stream_id, owner });
            }
            if (owner == reported) continue;
            if (owner == reaper_owned) continue;
            if (owner == reaper_running) continue;
            if (self.handler_joins[i] != null) continue;
            std.debug.panic(
                "teardown slot sid={d} has no poster (owner={d} join=null)",
                .{ slot.stream_id, owner },
            );
        }
    }

    fn eventHasWaiters(e: *const zio.Event) bool {
        return e.wait_queue.hasWaiters();
    }

    fn countTicketInUse(self: *const Connection) usize {
        var n: usize = 0;
        for (self.tickets.slots) |*slot| {
            if (slot.in_use.load(.acquire)) n += 1;
        }
        return n;
    }

    fn countTicketWaiters(self: *const Connection) usize {
        var n: usize = 0;
        for (self.tickets.slots) |*slot| {
            if (slot.in_use.load(.acquire) and slot.event.wait_queue.hasWaiters()) n += 1;
        }
        return n;
    }

    fn panicTeardownWatchdog(self: *const Connection) noreturn {
        std.debug.assert(self.teardown_wait_expected <= self.handlers.touched);
        std.debug.assert(self.teardown_wait_released < self.teardown_wait_expected);
        var in_use: usize = 0;
        var reaper_n: usize = 0;
        var owner_live: usize = 0;
        var reaper_queued: usize = 0;
        var reaper_run: usize = 0;
        var awaiting_n: usize = 0;
        for (self.handlers.slice()) |s| {
            if (!s.in_use) continue;
            in_use += 1;
            if (s.reaper_reserved) reaper_n += 1;
            if (s.awaiting_receipt.load(.acquire)) awaiting_n += 1;
            const owner = s.completion_owner.load(.acquire);
            if (owner == live) owner_live += 1;
            if (owner == reaper_owned) reaper_queued += 1;
            if (owner == reaper_running) reaper_run += 1;
        }
        std.debug.assert(in_use != 0);
        for (self.handlers.slice(), 0..) |s, i| {
            if (!s.in_use) continue;
            const space_wait = i < self.space_events.len and eventHasWaiters(&self.space_events[i]);
            const deadline_wait = i < self.deadline_events.len and eventHasWaiters(&self.deadline_events[i]);
            diagRawPrint(
                "teardown stall sid={d} owner={d} join={d} admitted={d} finalize={d} reaper={d} awaiting_receipt={d} cause={d} space_wait={d} deadline_wait={d} session_held={d}\n",
                .{
                    s.stream_id,
                    s.completion_owner.load(.acquire),
                    @intFromBool(self.handler_joins[i] != null),
                    @intFromBool(s.admitted),
                    s.finalize_count.load(.acquire),
                    @intFromBool(s.reaper_reserved),
                    @intFromBool(s.awaiting_receipt.load(.acquire)),
                    s.terminal.kind.load(.acquire),
                    @intFromBool(space_wait),
                    @intFromBool(deadline_wait),
                    @intFromBool(self.session_held),
                },
            );
        }
        std.debug.panic(
            "teardown wait exceeded 5s no progress: live_handlers={d} slots={d} reaper={d} owner_live={d} expected={d} released={d} reaper_queued={d} reaper_running={d} awaiting_receipt={d} ticket_in_use={d} ticket_wait={d} dead_wait={d} session_held={d}",
            .{
                self.live_handlers.load(.acquire),
                in_use,
                reaper_n,
                owner_live,
                self.teardown_wait_expected,
                self.teardown_wait_released,
                reaper_queued,
                reaper_run,
                awaiting_n,
                self.countTicketInUse(),
                self.countTicketWaiters(),
                @intFromBool(eventHasWaiters(&self.dead)),
                @intFromBool(self.session_held),
            },
        );
    }

    /// Stop every handler and wait until all slots are released. This is the
    /// last thing the actor does, and it must be exhaustive: `deinit` asserts
    /// that no slot is still in use, and frees the storage the handlers point
    /// into.
    ///
    /// The last actor turn already set causes, released pending bytes, and
    /// froze the inline queue. This path does not take `session_mu`. Wakes
    /// run before any wait: a handler in `waitForStreamSpace` cannot post
    /// until space is set.
    fn shutdownHandlers(self: *Connection) void {
        var sweep_node: @TypeOf(shutdown_sweep).Node = .unset;
        shutdown_sweep.set(&sweep_node, {});
        defer shutdown_sweep.clear(&sweep_node);
        if (std.debug.runtime_safety) {
            std.debug.assert(shutdown_sweep.get() != null);
        }

        if (close_probe) {
            // Racy read of the queue's byte count, without its mutex: a diag
            // gauge, never a decision. Nonzero here means the socket shutdown
            // below clips frames the Session already emitted (the GOAWAY race).
            // Unit: ring bytes of queued WireChunk descriptors, not wire bytes.
            const queued = self.write_ch.type_erased.len / @sizeOf(wire_pump.WireChunk);
            diagRawPrint("t1002 conn={x} shutdownHandlers_enter unflushed_chunks={d} parked={d}\n", .{ @intFromPtr(self), queued, close_probe_parked.load(.acquire) });
        }
        // On a `runBody` error, its defer has already closed the socket before
        // `run` reaches here. A shutdown then targets a closed fd number that
        // an accept may already have handed to another connection.
        if (!self.socket_closed.load(.acquire)) self.stream.shutdown(self.config.io, .both) catch {};
        if (diag_park) diagRawPrint("WFSET shutdown_handlers conn={x}\n", .{ @intFromPtr(self) & 0xffff });
        self.writer_failed.store(true, .release);
        std.debug.assert(self.writer_failed.load(.acquire));
        self.closeWriterQueues();
        self.publishTeardownWakes();
        _ = self.drainCompletions();
        self.finishPendingReceiptsNoWait();
        // Handlers in tickets.wait / space / deadline select on `dead` and
        // post. An uncooperative wait does not return; the watchdog names it.
        self.waitTeardownCompletions();
    }

    fn publishTeardownWakes(self: *Connection) void {
        std.debug.assert(shutdown_sweep.get() != null);
        std.debug.assert(self.handlers.capacity() != 0);
        for (self.handlers.slice()) |*slot| {
            if (!slot.in_use) continue;
            slot.terminal.setCause(.server_shutdown);
        }
        self.tickets.failAll();
        self.wakeAllDeadlines();
        self.wakeAllSpace();
        self.dead.set();
    }

    fn waitTeardownCompletions(self: *Connection) void {
        std.debug.assert(shutdown_sweep.get() != null);
        self.teardown_wait_expected = self.countInUseSlots();
        self.teardown_wait_released = 0;
        std.debug.assert(self.teardown_wait_expected <= self.handlers.touched);
        const no_progress_ns: u64 = 5 * std.time.ns_per_s;
        var last_progress_ns = nowNs(self.config.io);
        var waits: usize = 0;
        while (true) {
            const before = self.countInUseSlots();
            const n = self.drainCompletions();
            const after_drain = self.countInUseSlots();
            std.debug.assert(before >= after_drain);
            std.debug.assert(before - after_drain == n);
            if (n != 0) {
                self.teardown_wait_released += n;
                last_progress_ns = nowNs(self.config.io);
            }
            std.debug.assert(self.teardown_wait_released + after_drain == self.teardown_wait_expected);
            if (after_drain == 0) break;
            std.debug.assert(self.teardown_wait_released < self.teardown_wait_expected);
            self.tickets.failAll();
            self.wakeAllSpace();
            self.wakeAllDeadlines();
            if (close_probe) {
                diagRawPrint("t1002 conn={x} shutdown_wait in_use={d} parked={d}\n", .{ @intFromPtr(self), after_drain, close_probe_parked.load(.acquire) });
            }
            waits += 1;
            std.debug.assert(waits <= self.handlers.touched);
            const sid = self.takeTeardownCompletion(last_progress_ns, no_progress_ns);
            if (!self.releaseSlot(sid)) {
                std.debug.panic("teardown completion sid={d} released no slot", .{sid});
            }
            self.teardown_wait_released += 1;
            last_progress_ns = nowNs(self.config.io);
            std.debug.assert(self.teardown_wait_released + self.countInUseSlots() == self.teardown_wait_expected);
        }
        std.debug.assert(self.countInUseSlots() == 0);
    }

    fn takeTeardownCompletion(self: *Connection, wait_started_ns: u64, watchdog_ns: u64) u31 {
        std.debug.assert(watchdog_ns != 0);
        const elapsed = nowNs(self.config.io) -% wait_started_ns;
        if (elapsed >= watchdog_ns) self.panicTeardownWatchdog();
        const winner = zio.select(.{
            .comp = self.completion_ch.asyncReceive(),
            .timer = zio.Timeout.fromNanoseconds(watchdog_ns - elapsed),
        }) catch |err| switch (err) {
            error.Canceled => std.debug.panic("teardown select canceled under shield", .{}),
        };
        return switch (winner) {
            .comp => |r| r catch |err| switch (err) {
                error.Closed => std.debug.panic("completion_ch closed with slots still in use", .{}),
            },
            .timer => self.panicTeardownWatchdog(),
        };
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
    fn maybeBeginGraceful(self: *Connection) !bool {
        const sf = self.config.shutdown_flag orelse return false;
        if (!sf.load(.acquire)) return false;
        if (!self.shutting_down) {
            self.shutting_down = true;
            try self.session.applyCommand(.graceful_phase1);
            try self.processIntents();
            const now_ns = nowNs(self.config.io);
            self.grace_deadline = std.Io.Timestamp.fromNanoseconds(@as(i96, now_ns +% std.time.ns_per_s));
            return false;
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
                    // The remaining handlers are canceled by shutdownHandlers
                    // after the actor loop. Staying in this loop waits for
                    // work whose cancellation cannot begin: a circular hang.
                    return true;
                }
            }
        }
        return false;
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

    /// Block a handler until the writer has room for one more push, the gate a
    /// drain's pop gets from the pause hooks. A handler's bare flush barrier
    /// (`sendFlushTicket` with nothing pending) is the one push made outside a
    /// drain, so it waits here first, on both transports. The room is
    /// `barrierRoom`: on h2c `write_ack_ch` has room for one more completion
    /// AND the write queue a free slot (`h2cPause`'s gates); on TLS the stash is
    /// not full (`Pump.stashFull`, `pause_control`'s test). The rest of this
    /// text is written for h2c; the TLS waker and its argument follow it. It parks the handler, not the barrier: the handler
    /// then waits for the barrier's ticket anyway, and a parked handler leaves
    /// nothing behind if its stream is reset. A barrier takes a queue slot and
    /// no pool chunk, so the queue can be full while the pool is not; without
    /// the queue gate the push itself would wait, in `putOne`, holding
    /// `session_mu`, and the actor could not run a deadline or a graceful
    /// shutdown. After the gate the push cannot wait: every push is made under
    /// `session_mu`, which the caller holds from the check to the push, and
    /// only the pump takes from the queue (`h2cQueueRoom`).
    ///
    /// Caller holds `session_mu`; like `waitForStreamSpace`, this releases
    /// and reacquires it around the wait, on the same per-slot space event.
    ///
    /// No lost wake. Four operations decide whether a parking handler is
    /// woken, and all four are seq_cst:
    ///   handler  W1: `ack_room_waiters.fetchAdd`, then R1: `acks.owed.load`
    ///   actor    W2: `acks.owed.fetchSub` (`AckLedger.sub`), then
    ///            R2: `ack_room_waiters.load` (`wakeAckRoomWaiters`)
    /// seq_cst operations have one total order that keeps each side's
    /// program order (W1 before R1, W2 before R2). If R1 misses W2 (the
    /// handler reads the old `owed` and parks), then R1 comes before W2 in
    /// that order, so W1 < R1 < W2 < R2, and R2 sees W1: the actor finds a
    /// waiter and wakes it. If R2 misses W1, then W2 < R2 < W1 < R1, and R1
    /// sees the subtract. So at least one side sees the other. With acquire
    /// or acq_rel on any of the four this is the store-buffer pattern, and
    /// both may read the old values. The first room check, before W1, is an
    /// ordinary `acquire` read: it only decides whether to register.
    ///
    /// The queue half of the gate has no seq_cst pair of its own. A slot frees
    /// when the pump takes a chunk, which the actor does not see; but that
    /// chunk's completion is posted after the take and received by the actor
    /// after that, and the actor's R2 (and its room check) follow its
    /// subtract. If R1 saw the queue full, then the take came after R1, and
    /// the chain take, post, receive, W2, R2 passes through the queue's mutex
    /// and the channel's, so W1 happens before R2 and R2 sees the waiter. The
    /// actor's room check then reads the queue after the take.
    ///
    /// The event is reset BEFORE W1, so a wake that lands after the reset
    /// leaves it set and the wait returns at once. A re-read that still finds
    /// no room parks with completions still owed; each later completion runs
    /// R2 again. If none comes, the writer failed or the connection is ending:
    /// a failed write (`applyWriteAck`), `handleWriterFailed` and teardown
    /// (`publishTeardownWakes`, which also sets `dead`) all call
    /// `wakeAllSpace`.
    ///
    /// TLS. The actor posts no write acks, so `wakeAckRoomWaiters` never runs
    /// there; the waker is the driver, which wakes after it has taken chunks off
    /// the stash (`wakeBarrierWaitersTls`, from `driveTlsTurn`). The two sides:
    ///   handler  W1: `ack_room_waiters.fetchAdd`, then R1: `stashFull()`
    ///   driver   W2: `tryTakeWrite`, then R2: `ack_room_waiters.load`
    /// R1 and W2 are critical sections of the SAME channel mutex (`stashFull`
    /// counts the stash with `chanLen`, `tryTakeWrite` is a `tryReceive`), so
    /// the mutex orders them. If R1 ran first and saw the stash full, its
    /// release happens before the take's acquire, W1 is before R1 in the
    /// handler's order and R2 is after W2 in the driver's, so W1 happens before
    /// R2 and R2 sees the waiter (a load cannot miss a store that happens
    /// before it): the driver finds a waiter and wakes it. If the take ran
    /// first, R1 counts the stash after it and sees the room. So one side sees
    /// the other, and no seq_cst is needed on this pair. R2 runs after every
    /// take of the turn (the loop in `driveTlsTurn`), so a waiter that parks
    /// behind a full stash is woken by the next take; if no take comes, the
    /// writer is stuck for a reason the other wake sources already cover (a
    /// failed send ends in `applyWriteAck`, `handleWriterFailed`, `dead`).
    /// Progress. The loop in `driveTlsTurn` takes one chunk, runs R2, then
    /// writes it, so every take the LOOP makes is followed by R2 before the next
    /// take, and a loop that keeps going does not leave a waiter behind. The
    /// takes that `writeChunks` makes itself (its batch gather, which runs only
    /// behind a first chunk that is not a barrier) are not each followed by R2:
    /// they share the one check after that call. A waiter needs the stash to be
    /// below the full threshold. If the outer take crossed it, the check before
    /// the write wakes the waiter; if only the gather did, the check after the
    /// call does. Barriers never gather, so a run of barriers is the first case.
    /// The check right after the take matters: the room is there only until a
    /// handler that is ready to flush takes it.
    fn waitForAckRoom(self: *Connection, hctx: *HandlerCtx, stream_id: u31) response.ResponseError!void {
        if (!self.ack_room_armed) return;
        const terminal = hctx.terminal;
        while (true) {
            if (terminal.getCause()) |c| return response.causeToError(c);
            if (self.writer_failed.load(.acquire)) return error.WriteFailed;
            if (self.barrierRoom()) return;
            // The actor is the only receiver; a handler running on it would
            // wait for itself.
            if (hctx.runs_inline) return error.WriteFailed;
            if (comptime test_observe) _ = test_ack_room_waits.fetchAdd(1, .monotonic);
            const event = if (self.spaceIndex(stream_id)) |i| &self.space_events[i] else null;
            if (event) |e| e.reset();
            _ = self.ack_room_waiters.fetchAdd(1, .seq_cst); // W1
            defer _ = self.ack_room_waiters.fetchSub(1, .seq_cst);
            if (self.barrierRoomSeqCst()) return; // R1
            self.unlockSession(self.config.io);
            const wait_res: anyerror!void = if (event) |e| blk: {
                const winner = zio.select(.{
                    .space = e,
                    .dead = &self.dead,
                }) catch break :blk error.Canceled;
                break :blk switch (winner) {
                    .space => {},
                    .dead => error.Canceled,
                };
            } else error.Canceled;
            self.lockSessionUncancelable(self.config.io);
            if (terminal.getCause()) |c| return response.causeToError(c);
            if (self.writer_failed.load(.acquire)) return error.WriteFailed;
            wait_res catch return error.Canceled;
        }
    }

    /// Actor, after taking a completion: wake handlers parked in
    /// `waitForAckRoom` once there is room for them.
    /// The caller has just subtracted from `acks.owed` (W2 in
    /// `waitForAckRoom`'s proof); this load is R2.
    fn wakeAckRoomWaiters(self: *Connection) void {
        if (self.ack_room_waiters.load(.seq_cst) == 0) return;
        if (!self.barrierRoom()) return;
        self.wakeAllSpace();
    }

    /// Room for one bare flush barrier. h2c: its completion in `write_ack_ch`,
    /// and its slot in the write queue. TLS: the stash is not full, the test a
    /// drain asks before a pop, so a barrier pushed after a yes leaves the
    /// drain's tail push its slot (`assertTailRoom`). Called under `session_mu`
    /// before the push, and by the wakers.
    fn barrierRoom(self: *Connection) bool {
        if (self.config.mode == .tls) {
            const pump = self.tls_driver orelse return true;
            return !pump.stashFull();
        }
        return self.h2cAckRoom(0) and self.h2cQueueRoom(1);
    }

    /// `barrierRoom` for R1 in `waitForAckRoom`: the h2c ledger is read
    /// seq_cst (it is half of the store-buffer handshake above). TLS needs no
    /// stronger read; the channel mutex orders it.
    fn barrierRoomSeqCst(self: *Connection) bool {
        if (self.config.mode == .tls) return self.barrierRoom();
        return self.acks.owed.load(.seq_cst) + ack_room_per_pop <= self.write_ack_buf.len and self.h2cQueueRoom(1);
    }

    /// TLS driver, after it took chunks off the stash: wake the handlers parked
    /// for a barrier when the stash has room again. This load is R2 (see
    /// `waitForAckRoom`).
    fn wakeBarrierWaitersTls(self: *Connection) void {
        if (self.ack_room_waiters.load(.seq_cst) == 0) return;
        if (!self.barrierRoom()) return;
        self.wakeAllSpace();
    }

    /// Block a handler until its stream's pending slab is below `cap`.
    ///
    /// `enqueuePending` passes `outbound_bytes_per_stream` (the memory bound).
    /// SSE/`wait` writes pass `flush_occupancy_cap` so they park on pending-slab
    /// occupancy rather than on a TLS receipt.
    ///
    /// This is the backpressure boundary: a handler that produces faster than
    /// the peer reads is parked here rather than allowed to grow a buffer. That
    /// is what keeps a single stream inside `outbound_bytes_per_stream` and the
    /// connection inside `resourceUpperBound`.
    ///
    /// This is one of three places that release `session_mu` mid-operation
    /// (the others are `parkForBudget` and `waitForAckRoom`), and it must,
    /// because only the actor can free this capacity and the actor needs the
    /// same lock. The sequence per attempt:
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
    fn waitForStreamSpace(self: *Connection, hctx: *HandlerCtx, stream_id: u31, cap: usize) response.ResponseError!void {
        std.debug.assert(cap > 0);
        const terminal = hctx.terminal;
        while (true) {
            if (terminal.getCause()) |c| return response.causeToError(c);
            if (self.writer_failed.load(.acquire)) return error.WriteFailed;
            const len = self.sched.pendingByteLen(stream_id);
            if (len < cap) return;
            // A complete handler runs on the actor. This wait is woken by the
            // actor's drain, so parking here would never return. A body that
            // does not fit in the stream slab belongs on a task handler. Ask
            // the caller, not the connection: a task handler on the same
            // connection may wait here while the actor runs a complete batch.
            if (hctx.runs_inline) return error.WriteFailed;
            // Prove sparse/dense capacity wait for live gates. Debug (and
            // -Dobserve=true) only: same gate as the other hot test hooks.
            if (comptime test_observe) test_waiting_for_space.store(@as(u32, stream_id), .release);
            // Explicit lock ownership: unlock for wait, always reacquire uncancelable
            // before any return so caller defer unlock stays balanced.
            const event = if (self.spaceIndex(stream_id)) |i| &self.space_events[i] else null;
            if (event) |e| e.reset();
            // Recheck after reset: a teardown wake can land in the gap and be
            // cleared, after which wait would never return.
            if (terminal.getCause()) |c| return response.causeToError(c);
            if (self.writer_failed.load(.acquire)) return error.WriteFailed;
            self.unlockSession(self.config.io);
            const wait_res: anyerror!void = if (event) |e| blk: {
                const winner = zio.select(.{
                    .space = e,
                    .dead = &self.dead,
                }) catch break :blk error.Canceled;
                break :blk switch (winner) {
                    .space => {},
                    .dead => error.Canceled,
                };
            } else error.Canceled;
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
    /// wait for room in the stream's slab, then for room in the connection's
    /// budget (`reserveBudget`; a full budget parks the handler, it does not
    /// fail the write), copy into the slab, then set `sched_refilled`. The
    /// actor learns of it from the release of `session_mu` (`unlockSession`
    /// rings), which comes after the flag is set, for the reason given in the
    /// module header — an event edge can be erased by the actor's reset, a flag
    /// cannot.
    ///
    /// Slab room comes first, and `take` is what that room allows, so a
    /// waiter never asks the budget for more than one stream's cap. The
    /// order also keeps a parked handler from holding budget while it waits
    /// for something else: it holds none until it is granted, and then it
    /// only copies.
    ///
    /// The trailing zero-length enqueue is not redundant. It carries the
    /// END_STREAM marker or the flush ticket for a body whose bytes are all
    /// already queued, and for an empty body it is the only enqueue there is.
    /// It carries no payload, so it costs no budget and never waits for it.
    fn enqueuePending(
        self: *Connection,
        stream_id: u31,
        bytes: []const u8,
        end: bool,
        flush_ticket: u64,
        flush_slot: u32,
        hctx: *HandlerCtx,
    ) response.ResponseError!void {
        const cap = self.config.limits.outbound_bytes_per_stream;
        if (self.sched.pendingCount() >= self.config.limits.max_streams_per_connection) {
            if (!self.sched.contains(stream_id)) {
                if (diag_park) diagRawPrint("WFAIL pendcap sid={d} sched_pending={d}\n", .{ stream_id, self.sched.pendingCount() });
                return error.OutOfMemory;
            }
        }
        var off: usize = 0;
        while (off < bytes.len) {
            try self.waitForStreamSpace(hctx, stream_id, cap);
            const room = cap - self.sched.pendingByteLen(stream_id);
            if (room == 0) continue;
            const take = @min(bytes.len - off, room);
            try self.reserveBudget(hctx, stream_id, take);
            self.sched.enqueueDataBytes(stream_id, bytes[off..][0..take], false, 0, 0) catch |err| {
                if (diag_park) diagRawPrint("WFAIL slab sid={d} sched_pending={d}\n", .{ stream_id, self.sched.pendingCount() });
                self.applyOutboundRelease(take, .pending);
                // The server's slab pool is empty. Like the server byte cap,
                // that is shed load: no release on this connection brings a
                // slab back, so the write is refused and not parked.
                if (err == error.SlabsExhausted) self.noteHardRefusal(&outbound_memory.slab_pool_refusals);
                return error.OutOfMemory;
            };
            off += take;
            // The wake for this piece is the release of `session_mu`: the next
            // `waitForStreamSpace` (or the caller's unlock) rings.
            self.sched_refilled.store(true, .release);
        }
        if (bytes.len == 0 or end or flush_ticket != 0) {
            self.sched.enqueueDataBytes(stream_id, &.{}, end, flush_ticket, flush_slot) catch {
                if (flush_ticket != 0) return error.WriteFailed;
                return error.OutOfMemory;
            };
            self.sched_refilled.store(true, .release);
        }
    }

    /// A write the server refused for lack of SERVER-wide memory (its byte
    /// cap or its slab pool). Under `session_mu`.
    fn noteHardRefusal(self: *Connection, counter: *std.atomic.Value(u64)) void {
        self.stream_budget_refusals +|= 1;
        _ = counter.fetchAdd(1, .monotonic);
    }

    /// Reserve `need` budget bytes for a handler's write, as pending. Returns
    /// with the bytes reserved and `session_mu` held, or with an error and
    /// nothing reserved. May release `session_mu` while it waits.
    ///
    /// - An inline (complete) handler spends the credit the gate reserved for
    ///   it (`runPendingInline`). It never waits: the actor would be waiting
    ///   for itself.
    /// - A task handler takes the room if no one is queued. A newcomer joins
    ///   the tail whenever the queue is not empty, even if room exists, or a
    ///   stream of small writes could starve a large one for ever. The
    ///   emptiness read is a snapshot: two arrivals at the same instant have
    ///   no order to keep.
    /// - Otherwise it parks (`parkForBudget`).
    fn reserveBudget(self: *Connection, hctx: *HandlerCtx, stream_id: u31, need: usize) response.ResponseError!void {
        if (hctx.runs_inline) {
            if (need <= hctx.inline_credit) {
                hctx.inline_credit -= need;
                return;
            }
            if (hctx.inline_shed) {
                self.noteHardRefusal(&outbound_memory.server_cap_refusals);
                return error.OutOfMemory;
            }
            // The gate reserved a whole stream's cap and a complete handler
            // cannot write more: a gate that let a job through without it.
            return error.WriteFailed;
        }
        if (self.budget_waiters.load(.seq_cst) == 0) {
            switch (self.tryReserveOutboundBytes(need, .pending)) {
                .ok => return,
                .server_full => {
                    self.noteHardRefusal(&outbound_memory.server_cap_refusals);
                    return error.OutOfMemory;
                },
                .connection_full => {},
            }
        }
        return self.parkForBudget(hctx, stream_id, need);
    }

    /// Wait in the budget queue until `need` bytes are reserved for this
    /// handler. The lock protocol is `waitForStreamSpace`'s: check the
    /// terminal cause and the writer, reset the event, unlock `session_mu`,
    /// select on the event and `dead`, relock uncancelably, check again.
    ///
    /// What the queue guarantees:
    /// - FIFO with a hand-over. The releaser reserves the bytes FOR the head
    ///   (`grantBudgetLocked`) before it wakes it, so the woken handler finds
    ///   them taken and no newcomer can take them first. A large write is not
    ///   starved by small ones, and a write that needs more than one release
    ///   to fit is not woken until it can proceed (no herd, no retry loop).
    /// - A large body parks again at the TAIL for each of its pieces, so
    ///   streams take turns (round robin by construction).
    /// - Every exit unlinks the entry and gives an unused grant back, exactly
    ///   once (`abandonBudget`): a cancel, a stream reset, the connection
    ///   ending, a slow-consumer kill, a failed writer.
    ///
    /// Nothing here waits on anything that waits on the budget. The handler
    /// holds no budget while it waits. The budget is held by pending bytes
    /// (the drain moves them on, or the slow-consumer deadline kills their
    /// stream), by wire bytes (bounded by the write pool, see
    /// `Limits.resourceUpperBound`; the ack frees them) and by granted bytes whose
    /// handler only needs `session_mu` to copy them.
    fn parkForBudget(self: *Connection, hctx: *HandlerCtx, stream_id: u31, need: usize) response.ResponseError!void {
        const io = self.config.io;
        const terminal = hctx.terminal;
        if (terminal.getCause()) |c| return response.causeToError(c);
        if (self.writer_failed.load(.acquire)) return error.WriteFailed;
        const i = self.spaceIndex(stream_id) orelse return error.Canceled;
        const slot = &self.handlers.storage[i];
        const event = &self.space_events[i];
        self.queueForBudget(slot, i, need, false);
        var parked = false;
        while (true) {
            const state = self.takeBudgetOutcome(slot, event);
            switch (state) {
                .granted => return self.acceptGrant(terminal, need),
                .refused => {
                    self.noteHardRefusal(&outbound_memory.server_cap_refusals);
                    return error.OutOfMemory;
                },
                .queued => {},
                .idle => unreachable,
            }
            if (terminal.getCause()) |c| {
                self.abandonBudget(slot, i);
                return response.causeToError(c);
            }
            if (self.writer_failed.load(.acquire)) {
                self.abandonBudget(slot, i);
                return error.WriteFailed;
            }
            if (!parked) {
                parked = true;
                _ = self.budget_parks.fetchAdd(1, .monotonic);
                if (comptime test_observe) _ = test_budget_waits.fetchAdd(1, .monotonic);
            }
            self.unlockSession(io);
            const wait_res: anyerror!void = blk: {
                const winner = zio.select(.{
                    .space = event,
                    .dead = &self.dead,
                }) catch break :blk error.Canceled;
                break :blk switch (winner) {
                    .space => {},
                    .dead => error.Canceled,
                };
            };
            self.lockSessionUncancelable(io);
            wait_res catch {
                self.abandonBudget(slot, i);
                if (terminal.getCause()) |c| return response.causeToError(c);
                return error.Canceled;
            };
        }
    }

    /// The handler found `need` bytes reserved for it. A reset or a failed
    /// writer that arrived with the grant wins, and the bytes go back.
    fn acceptGrant(self: *Connection, terminal: *response.SlotTerminal, need: usize) response.ResponseError!void {
        if (terminal.getCause()) |c| {
            self.applyOutboundRelease(need, .pending);
            return response.causeToError(c);
        }
        if (self.writer_failed.load(.acquire)) {
            self.applyOutboundRelease(need, .pending);
            return error.WriteFailed;
        }
    }

    /// Join the tail of the budget queue, then try the hand-over once: a
    /// release may have landed before this entry was visible to it, and the
    /// head may be this entry.
    fn queueForBudget(self: *Connection, slot: *HandlerSlot, index: usize, need: usize, is_inline: bool) void {
        const event = &self.space_events[index];
        self.budget_mu.lockUncancelable();
        defer self.budget_mu.unlock();
        if (!is_inline) event.reset();
        self.linkBudgetLocked(slot, index, need, is_inline);
        self.grantBudgetLocked(budget_none);
    }

    /// Put an idle entry at the tail of the queue. `budget_mu` held.
    fn linkBudgetLocked(self: *Connection, slot: *HandlerSlot, index: usize, need: usize, is_inline: bool) void {
        std.debug.assert(slot.budget.state == .idle);
        slot.budget = .{ .state = .queued, .need = need, .is_inline = is_inline };
        if (self.budget_tail == budget_none) {
            self.budget_head = @intCast(index);
        } else {
            self.handlers.storage[self.budget_tail].budget.next = @intCast(index);
        }
        self.budget_tail = @intCast(index);
        _ = self.budget_waiters.fetchAdd(1, .seq_cst);
    }

    /// What became of this entry. A grant or a refusal is taken (the entry is
    /// idle again, the handler owns the outcome); a still queued entry has
    /// its event reset under the lock a grant sets it under, so a grant after
    /// this call is not erased.
    fn takeBudgetOutcome(self: *Connection, slot: *HandlerSlot, event: ?*zio.Event) @TypeOf(slot.budget.state) {
        self.budget_mu.lockUncancelable();
        defer self.budget_mu.unlock();
        return takeBudgetOutcomeLocked(slot, event);
    }

    fn takeBudgetOutcomeLocked(slot: *HandlerSlot, event: ?*zio.Event) @TypeOf(slot.budget.state) {
        const st = slot.budget.state;
        switch (st) {
            .queued => if (event) |e| e.reset(),
            .granted, .refused => slot.budget.state = .idle,
            .idle => unreachable,
        }
        return st;
    }

    /// Leave the budget queue without the bytes: unlink a queued entry, or
    /// give back a grant the handler no longer wants. Either way the entry is
    /// idle afterwards and the bytes are released once. The release is done
    /// outside `budget_mu` because it hands the room to the next waiter.
    fn abandonBudget(self: *Connection, slot: *HandlerSlot, index: usize) void {
        self.budget_mu.lockUncancelable();
        const give_back = self.abandonBudgetLocked(slot, index);
        self.budget_mu.unlock();
        self.applyOutboundRelease(give_back, .pending);
    }

    /// `abandonBudget` for a complete job's entry only: whether the entry is
    /// one, and in what state, is read under the lock (a parked task writer
    /// owns its entry's fields, and the hand-over writes them from another
    /// thread). A stream's reset and the teardown sweep call this for every
    /// slot.
    fn abandonInlineBudget(self: *Connection, slot: *HandlerSlot, index: usize) void {
        self.budget_mu.lockUncancelable();
        const give_back = if (slot.budget.is_inline and slot.budget.state != .idle)
            self.abandonBudgetLocked(slot, index)
        else
            0;
        self.budget_mu.unlock();
        self.applyOutboundRelease(give_back, .pending);
    }

    /// Unlink a queued entry or take back a grant; the entry is idle after.
    /// Returns the bytes to release once the lock is dropped. `budget_mu` held.
    fn abandonBudgetLocked(self: *Connection, slot: *HandlerSlot, index: usize) usize {
        var give_back: usize = 0;
        switch (slot.budget.state) {
            .queued => self.unlinkBudgetLocked(@intCast(index)),
            .granted => give_back = slot.budget.need,
            .refused, .idle => {},
        }
        slot.budget = .{};
        // The entry that left may have been a head that did not fit and
        // that held back a follower that does. No release is coming to
        // wake that follower (behind closed stream windows the next one
        // can be the slow-consumer deadline), so hand over room now.
        self.grantBudgetLocked(budget_none);
        return give_back;
    }

    fn unlinkBudgetLocked(self: *Connection, index: u32) void {
        var prev: u32 = budget_none;
        var cur = self.budget_head;
        while (cur != budget_none and cur != index) {
            prev = cur;
            cur = self.handlers.storage[cur].budget.next;
        }
        std.debug.assert(cur == index);
        const next = self.handlers.storage[index].budget.next;
        if (prev == budget_none) self.budget_head = next else self.handlers.storage[prev].budget.next = next;
        if (self.budget_tail == index) self.budget_tail = prev;
        _ = self.budget_waiters.fetchSub(1, .seq_cst);
    }

    /// Single FairScheduler drain: terminal → ordinary(+forced DATA) → DRR DATA.
    /// Every control and ticketed DATA from that turn copies into the
    /// boot-counted scratch and becomes one queueWire input, up to 16 KiB.
    /// On TLS that input encrypts as one record batch; on h2c it is one write
    /// chunk. WireChunk.control_entries carries how many control-pool slots
    /// the batch holds, so the actor can release occupancy exactly. A second
    /// HEADERS used to flush the first instead, which is why one-shot TLS
    /// emitted one record per response while SSE (DATA only) packed a turn.
    /// Non-ticketed DATA still takes the immediate per-frame handoff. All emits
    /// still go through queueWire via the scheduler sink only.
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
        if (comptime test_observe) {
            if (test_hold_emit.load(.acquire)) return;
        }
        // The drain does not wait for memory. Pause hooks re-check room before
        // every pop: write-pool chunks on both transports
        // (`outboundMemoryPaused`), the stash on TLS, and on h2c the ack
        // channel (always) and the write queue (once the connection is
        // ending: a push would wait in `putOne` holding `session_mu`, and a
        // writer that is not moving never makes room, so the closing deadline
        // could not fire). The install below covers a connection whose pumps
        // never started. The frames the hooks hold back stay in the
        // scheduler; a write completion wakes the actor and the next drain
        // pops them. Terminal Session counts as ending here because the drain
        // inside `ingestWireChunk` runs before the closing phase has begun.
        _ = outbound_memory.drain_turns.fetchAdd(1, .monotonic);
        if (self.config.mode == .h2c and !self.h2c_ending and
            (self.closing_deadline_ns != null or self.session.terminal == .goaway))
        {
            self.h2c_ending = true;
            self.sched.pause = h2cPauseData;
            self.sched.pause_control = h2cPause;
            self.sched.pause_ctx = self;
        }
        if (diag_park) {
            const now = nowNs(self.config.io);
            if (diag_last_drain_ns != 0) diag_prev_drain_gap_ns = now -% diag_last_drain_ns;
            diag_last_drain_ns = now;
        }
        const SinkCtx = struct {
            c: *Connection,
            emit: emit_batch.EmitBatch,
            tickets_emitted: u32 = 0,

            fn queue(
                batch: *@This(),
                payload: []u8,
                flush: bool,
                ticket: u64,
                ticket_slot: u32,
                ticket_count: u32,
                control_n: usize,
                control_entries: u32,
                credit: usize,
                owns_payload: bool,
            ) anyerror!void {
                // A completed batch is still scheduler-originated output even
                // when it is handed off after `sched.drain` returns.
                if (comptime test_observe) test_in_sched_sink = true;
                defer if (comptime test_observe) {
                    test_in_sched_sink = false;
                };
                if (trace.enabled) batch.tickets_emitted += ticket_count;
                try batch.c.queueWire(
                    payload,
                    flush,
                    ticket,
                    ticket_slot,
                    ticket_count,
                    control_n,
                    control_entries,
                    credit,
                    owns_payload,
                    false,
                );
            }

            fn flushBatch(batch: *@This()) anyerror!void {
                if (batch.emit.empty()) {
                    std.debug.assert(batch.emit.ticket_count == 0);
                    std.debug.assert(batch.emit.control_entries == 0);
                    return;
                }
                const taken = batch.emit.take();
                try batch.queue(
                    taken.bytes,
                    taken.flush,
                    taken.ticket,
                    taken.ticket_slot,
                    taken.ticket_count,
                    taken.control_n,
                    taken.control_entries,
                    taken.payload_credit,
                    false,
                );
            }

            fn sink(
                ctx: *anyopaque,
                payload: []u8,
                flush: bool,
                ticket: u64,
                ticket_slot: u32,
                control_n: usize,
                control_entry: bool,
            ) anyerror!void {
                const batch: *@This() = @ptrCast(@alignCast(ctx));
                const c = batch.c;
                // The frame's DATA payload (`buildData`), still counted as
                // pending. From here this function owns it: it goes to the
                // batch or to `queueWire`, which release it if they fail, and
                // the early returns below release it themselves. The side
                // channel is cleared first so the drain's error path cannot
                // release it a second time.
                const credit = c.pending_data_outbound_release;
                const wake_sid = c.pending_data_wake_sid;
                c.pending_data_outbound_release = 0;
                c.pending_data_wake_sid = 0;
                if (emit_batch.frameIsBatchable(
                    control_entry,
                    ticket,
                    frame.encodedOnWireLen(payload),
                    batch.emit.capacity(),
                )) {
                    const wire = frame.encodedOnWire(payload);
                    if (!batch.emit.wouldFit(wire.len)) {
                        batch.flushBatch() catch |err| {
                            c.releaseWireBytes(payload);
                            c.applyOutboundRelease(credit, .pending);
                            return err;
                        };
                    }
                    if (ticket != 0) {
                        if (batch.emit.ticket_count != 0) {
                            c.tickets.linkCompletion(batch.emit.last_ticket_slot, ticket_slot) catch {
                                if (batch.emit.control_entries != 0) {
                                    c.applyControlRelease(batch.emit.control_n, batch.emit.control_entries);
                                    batch.emit.control_n = 0;
                                    batch.emit.control_entries = 0;
                                }
                                c.releaseWireBytes(payload);
                                c.applyOutboundRelease(credit, .pending);
                                return error.WriteFailed;
                            };
                        }
                        batch.emit.noteTicket(ticket, ticket_slot);
                    }
                    if (c.payloadInScratch(payload)) {
                        // Built here by `rentFrame`, at the end of what the scratch holds.
                        std.debug.assert(payload.ptr == batch.emit.buf.ptr + batch.emit.len);
                        batch.emit.adopt(wire.len, flush, control_n, control_entry, credit);
                    } else {
                        batch.emit.copyFrame(wire, flush, control_n, control_entry, credit);
                        c.releaseWireBytes(payload);
                    }
                } else {
                    // `rentFrame` hands out scratch only for a frame the rule above batches.
                    std.debug.assert(!c.payloadInScratch(payload));
                    batch.flushBatch() catch |err| {
                        c.releaseWireBytes(payload);
                        c.applyOutboundRelease(credit, .pending);
                        return err;
                    };
                    try batch.queue(
                        payload,
                        flush,
                        ticket,
                        ticket_slot,
                        if (ticket != 0) 1 else 0,
                        control_n,
                        if (control_entry) 1 else 0,
                        credit,
                        true,
                    );
                }
                // The frame left its slab, so the stream has room again. This
                // is the stream's wake; the budget's is `applyOutboundRelease`.
                if (credit != 0) c.wakeStreamSpace(wake_sid);
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
                c.building_pop_frame = true;
                defer c.building_pop_frame = false;
                const data_payload = c.session.makeDataFrame(stream_id, bytes, end) catch |err| {
                    if (err == error.FlowBlocked) return error.FlowBlocked;
                    return err;
                };
                c.pending_data_outbound_release = bytes.len;
                c.pending_data_wake_sid = stream_id;
                return data_payload;
            }
        };
        std.debug.assert(self.plaintext_scratch.len == emit_batch.max_plaintext);
        var sink_ctx: SinkCtx = .{
            .c = self,
            .emit = .{ .buf = self.plaintext_scratch },
        };
        self.rearmCredited();
        self.pack_scratch = &sink_ctx.emit;
        defer self.pack_scratch = null;
        self.sched.drain(
            &sink_ctx,
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
            // Frames the batch took before the failure never reached a chunk.
            self.applyOutboundRelease(sink_ctx.emit.payload_credit, .pending);
            sink_ctx.emit.payload_credit = 0;
            self.noteServerClose("data_sink_catch", err);
            self.writer_failed.store(true, .release);
            self.handleWriterFailed();
            return err;
        };
        if (!sink_ctx.emit.empty()) self.assertTailRoom("drain batch");
        sink_ctx.flushBatch() catch |err| {
            self.noteServerClose("data_sink_catch2", err);
            self.writer_failed.store(true, .release);
            self.handleWriterFailed();
            return err;
        };
        if (trace.enabled) {
            if (sink_ctx.tickets_emitted > 0) {
                _ = trace.emit_turns.fetchAdd(1, .monotonic);
                _ = trace.emit_tickets.fetchAdd(sink_ctx.tickets_emitted, .monotonic);
                trace.bumpMax(&trace.emit_max, sink_ctx.tickets_emitted);
            }
        }
        if (comptime test_observe) test_observed_sched_emits.store(self.sched.emits_total, .release);
    }

    /// Complete-handler drain: join unticketed DATA while a drain-turn receipt
    /// is live, and attach that receipt to the last `queueWire`. Do not route
    /// the actor or SSE through this sink — that hung 100-stream shutdown even
    /// with `receipt_ticket == 0`. Actor emit stays on `drainEmit`.
    fn drainEmitReceipt(self: *Connection, receipt_ticket: u64, receipt_slot: u32, attached: *bool) !void {
        self.assertSessionHeld("drainEmitReceipt");
        const ReceiptSink = struct {
            c: *Connection,
            emit: emit_batch.EmitBatch,
            tickets_emitted: u32 = 0,
            receipt_ticket: u64 = 0,
            receipt_slot: u32 = 0,
            receipt_attached: bool = false,
            queued_any: bool = false,

            fn queue(
                batch: *@This(),
                payload: []u8,
                flush: bool,
                ticket: u64,
                ticket_slot: u32,
                ticket_count: u32,
                control_n: usize,
                control_entries: u32,
                credit: usize,
                owns_payload: bool,
                complete_batch_receipt: bool,
            ) anyerror!void {
                // A completed batch is still scheduler-originated output even
                // when it is handed off after `sched.drain` returns.
                if (comptime test_observe) test_in_sched_sink = true;
                defer if (comptime test_observe) {
                    test_in_sched_sink = false;
                };
                if (trace.enabled) batch.tickets_emitted += ticket_count;
                batch.queued_any = true;
                try batch.c.queueWire(
                    payload,
                    flush,
                    ticket,
                    ticket_slot,
                    ticket_count,
                    control_n,
                    control_entries,
                    credit,
                    owns_payload,
                    complete_batch_receipt,
                );
            }

            fn flushBatch(batch: *@This(), complete_batch_receipt: bool) anyerror!void {
                if (batch.emit.empty()) {
                    std.debug.assert(batch.emit.ticket_count == 0);
                    std.debug.assert(batch.emit.control_entries == 0);
                    return;
                }
                const taken = batch.emit.take();
                // queueWire encrypts this borrowed slice synchronously. The
                // buffer is the connection's boot-counted TLS plaintext
                // scratch, so a batch adds no allocation and no new limit.
                try batch.queue(
                    taken.bytes,
                    taken.flush,
                    taken.ticket,
                    taken.ticket_slot,
                    taken.ticket_count,
                    taken.control_n,
                    taken.control_entries,
                    taken.payload_credit,
                    false,
                    complete_batch_receipt,
                );
            }

            fn sink(
                ctx: *anyopaque,
                payload: []u8,
                flush: bool,
                ticket: u64,
                ticket_slot: u32,
                control_n: usize,
                control_entry: bool,
            ) anyerror!void {
                const batch: *@This() = @ptrCast(@alignCast(ctx));
                const c = batch.c;
                // The frame's DATA payload (`buildData`), still counted as
                // pending, and owned from here exactly as in `drainEmit`'s
                // sink. The chunk's own release is this credit, acked
                // separately. Several HEADERS may share one plaintext.
                // Occupancy is still held until the write completes;
                // control_entries is how many reserved slots that completion
                // must return.
                const credit = c.pending_data_outbound_release;
                const wake_sid = c.pending_data_wake_sid;
                c.pending_data_outbound_release = 0;
                c.pending_data_wake_sid = 0;
                if (emit_batch.frameJoinsInProgress(
                    control_entry,
                    ticket,
                    frame.encodedOnWireLen(payload),
                    batch.emit.capacity(),
                    batch.emit.len,
                    batch.receipt_ticket != 0,
                )) {
                    const wire = frame.encodedOnWire(payload);
                    if (!batch.emit.wouldFit(wire.len)) {
                        batch.flushBatch(false) catch |err| {
                            c.releaseWireBytes(payload);
                            c.applyOutboundRelease(credit, .pending);
                            return err;
                        };
                    }
                    if (ticket != 0) {
                        if (batch.emit.ticket_count != 0) {
                            c.tickets.linkCompletion(batch.emit.last_ticket_slot, ticket_slot) catch {
                                if (batch.emit.control_entries != 0) {
                                    c.applyControlRelease(batch.emit.control_n, batch.emit.control_entries);
                                    batch.emit.control_n = 0;
                                    batch.emit.control_entries = 0;
                                }
                                c.releaseWireBytes(payload);
                                c.applyOutboundRelease(credit, .pending);
                                return error.WriteFailed;
                            };
                        }
                        batch.emit.noteTicket(ticket, ticket_slot);
                    }
                    if (c.payloadInScratch(payload)) {
                        // Built here by `rentFrame`, at the end of what the scratch holds.
                        std.debug.assert(payload.ptr == batch.emit.buf.ptr + batch.emit.len);
                        batch.emit.adopt(wire.len, flush, control_n, control_entry, credit);
                    } else {
                        batch.emit.copyFrame(wire, flush, control_n, control_entry, credit);
                        c.releaseWireBytes(payload);
                    }
                } else {
                    // `rentFrame` hands out scratch only for a frame the rule above batches.
                    std.debug.assert(!c.payloadInScratch(payload));
                    batch.flushBatch(false) catch |err| {
                        c.releaseWireBytes(payload);
                        c.applyOutboundRelease(credit, .pending);
                        return err;
                    };
                    try batch.queue(
                        payload,
                        flush,
                        ticket,
                        ticket_slot,
                        if (ticket != 0) 1 else 0,
                        control_n,
                        if (control_entry) 1 else 0,
                        credit,
                        true,
                        false,
                    );
                }
                if (credit != 0) c.wakeStreamSpace(wake_sid);
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
                c.building_pop_frame = true;
                defer c.building_pop_frame = false;
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
        std.debug.assert(self.plaintext_scratch.len == emit_batch.max_plaintext);
        var sink_ctx: ReceiptSink = .{
            .c = self,
            .emit = .{ .buf = self.plaintext_scratch },
            .receipt_ticket = receipt_ticket,
            .receipt_slot = receipt_slot,
        };
        defer attached.* = sink_ctx.receipt_attached;
        self.rearmCredited();
        self.pack_scratch = &sink_ctx.emit;
        defer self.pack_scratch = null;
        self.sched.drain(
            &sink_ctx,
            ReceiptSink.sink,
            self,
            ReceiptSink.streamWin,
            ReceiptSink.connWin,
            ReceiptSink.buildData,
            self,
        ) catch |err| {
            if (self.pending_data_outbound_release != 0) {
                self.applyOutboundRelease(self.pending_data_outbound_release, .pending);
                self.pending_data_outbound_release = 0;
                self.pending_data_wake_sid = 0;
            }
            // Frames the batch took before the failure never reached a chunk.
            self.applyOutboundRelease(sink_ctx.emit.payload_credit, .pending);
            sink_ctx.emit.payload_credit = 0;
            self.noteServerClose("emit_fail1", err);
            self.writer_failed.store(true, .release);
            self.handleWriterFailed();
            return err;
        };
        if (sink_ctx.receipt_ticket != 0 and !sink_ctx.receipt_attached) {
            if (!sink_ctx.emit.empty()) {
                if (sink_ctx.emit.ticket_count != 0) {
                    self.tickets.linkCompletion(sink_ctx.emit.last_ticket_slot, sink_ctx.receipt_slot) catch {
                        self.applyOutboundRelease(sink_ctx.emit.payload_credit, .pending);
                        sink_ctx.emit.payload_credit = 0;
                        self.noteServerClose("emit_fail2", null);
                        self.writer_failed.store(true, .release);
                        self.handleWriterFailed();
                        return error.WriteFailed;
                    };
                }
                sink_ctx.emit.noteTicket(sink_ctx.receipt_ticket, sink_ctx.receipt_slot);
                self.assertTailRoom("receipt batch");
                sink_ctx.flushBatch(true) catch |err| {
                    self.noteServerClose("emit_fail3", err);
                    self.writer_failed.store(true, .release);
                    self.handleWriterFailed();
                    return err;
                };
                sink_ctx.receipt_attached = true;
            } else if (sink_ctx.queued_any) {
                self.assertTailRoom("bare receipt");
                self.sendFlushTicket(sink_ctx.receipt_ticket, sink_ctx.receipt_slot, true) catch |err| {
                    self.noteServerClose("emit_fail4", err);
                    self.writer_failed.store(true, .release);
                    self.handleWriterFailed();
                    return err;
                };
                if (trace.enabled) trace.noteHandoff(1, 0);
                sink_ctx.receipt_attached = true;
            }
        } else {
            if (!sink_ctx.emit.empty()) self.assertTailRoom("receipt drain batch");
            sink_ctx.flushBatch(false) catch |err| {
                self.noteServerClose("emit_fail5", err);
                self.writer_failed.store(true, .release);
                self.handleWriterFailed();
                return err;
            };
        }
        if (trace.enabled) {
            if (sink_ctx.tickets_emitted > 0) {
                _ = trace.emit_turns.fetchAdd(1, .monotonic);
                _ = trace.emit_tickets.fetchAdd(sink_ctx.tickets_emitted, .monotonic);
                trace.bumpMax(&trace.emit_max, sink_ctx.tickets_emitted);
            }
        }
        if (comptime test_observe) test_observed_sched_emits.store(self.sched.emits_total, .release);
    }

    fn applyStreamResetIntent(self: *Connection, r: anytype) void {
        // Every reset, local or from the peer, comes through here, so this is
        // where the scheduler's per-stream HEADERS state ends for it, and its
        // HEADERS still queued are dropped unsent.
        self.sched.forgetStream(r.stream_id);
        const cause: response.TerminalCause = if (r.from_peer)
            .{ .peer_reset = r.code }
        else if (self.shutting_down)
            .server_shutdown
        else
            .{ .peer_reset = r.code };
        self.cancelHandler(r.stream_id, cause);
    }

    /// A RST frame that cannot be queued has already tombstoned the stream.
    /// Dropping the paired `stream_reset` intent leaves FairScheduler DATA that
    /// `emitOneData` will skip forever (TLS stall). Run every remaining reset
    /// locally, then fail the connection so the peer is not left waiting.
    fn finishIntentsAfterEnqueueFail(self: *Connection, rest: []session_mod.Intent) void {
        for (rest) |*it| {
            switch (it.*) {
                .stream_reset => |r| {
                    if (diag_park) _ = diag_reset_after_ctrl_fail.fetchAdd(1, .monotonic);
                    self.applyStreamResetIntent(r);
                },
                .outbound_frame => |f| {
                    // Its waiter must hear that the frame will never go.
                    if (f.ticket.id != 0) self.tickets.wake(f.ticket.slot, f.ticket.id, false);
                    self.session.releaseIntent(it);
                },
                else => self.session.releaseIntent(it),
            }
        }
    }

    fn failClosedOnIntentError(self: *Connection) void {
        // Ordinary control/framed enqueue failure is per-stream WriteFailed.
        // Only a terminal frame that could not be queued fail-closes: that is
        // the RST-without-wire stall. Killing the connection on HEADERS
        // PoolFull dropped one of 50 clients and showed up as not-started.
        if (!self.writer_failed.load(.acquire)) return;
        self.handleWriterFailed();
    }

    /// Turn queued `Session` intents into scheduler work without emitting.
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
    ///
    /// A batch can queue intents of its own: a dispatch that is refused (a
    /// failed handler spawn, a refused reaper reservation, no request memory)
    /// resets or answers its stream. They are drained here, in further rounds,
    /// before this returns, also after a round fails (the first error is
    /// returned at the end), so the session queue is empty whenever a lock
    /// section ends (`unlockSession` asserts it), and the next section starts
    /// its batch from an empty queue (`Limits.intentEntriesMin`). A ticket
    /// does not depend on it: it travels in its own frame's intent
    /// (`OutboundFrame.ticket`). Nothing in a round may materialize the queue
    /// itself: the batch it would drain into is the one being read.
    fn materializeIntents(self: *Connection) anyerror!void {
        self.assertSessionHeld("materializeIntents");
        var first_err: ?anyerror = null;
        self.intents_materialized = true;
        var requeued = false;
        while (true) {
            const n = self.session.drainIntentsInto(self.intent_batch);
            if (n == 0) {
                // Once the queue is empty, what the session owes goes in, up
                // to the room the ordinary share has now, and is materialized
                // in the rounds that follow. Once per call: a frame refused
                // again is owed again, for the next call. Every lock section
                // calls this, the actor's turn after a write completion too,
                // and a completion is what frees the share.
                if (requeued or !self.session.owed.any()) break;
                requeued = true;
                self.session.requeueOwed(self.sched.ctrl.ordinaryFree());
                continue;
            }
            self.materializeBatch(n) catch |err| {
                if (first_err == null) first_err = err;
            };
        }
        if (first_err) |err| return err;
    }

    fn materializeBatch(self: *Connection, n: usize) anyerror!void {
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
                        // Already-framed DATA (`Session.cmdData`, the
                        // `respond_data` command) has no pending reservation,
                        // so it would reach the wire with no budget credit and
                        // the counters would stop meaning what they say. No
                        // production code produces it: handlers write through
                        // `enqueuePending`, and only tests and the session's
                        // own harnesses use `respond_data`. A connection that
                        // meets it stops loudly instead of drifting.
                        std.debug.panic("framed DATA reached the connection scheduler: no budget credit (stream {d}); handlers write through enqueuePending", .{f.stream_id});
                    }
                    const class = fair_scheduler.FairScheduler.classifyControl(f.typ, f.flags);
                    // The frame's own ticket, if a handler waits on it.
                    const t = f.ticket.id;
                    const s = f.ticket.slot;
                    // A CONTINUATION is always taken with its HEADERS below.
                    if (f.typ == .continuation) std.debug.panic("a CONTINUATION for stream {d} reached the batch without its HEADERS", .{f.stream_id});
                    const opens_block = fair_scheduler.leavesBlockOpen(f.payload);
                    // The payload is freed by a refusal; what a refused
                    // WINDOW_UPDATE or PING ack carries is read first.
                    const wu_increment: u31 = if (f.typ == .window_update and f.payload.len >= frame.FRAME_HEADER_LEN + 4)
                        @intCast(std.mem.readInt(u32, f.payload[frame.FRAME_HEADER_LEN..][0..4], .big) & 0x7fff_ffff)
                    else
                        0;
                    const ping_payload: [8]u8 = if (f.typ == .ping and f.payload.len >= frame.FRAME_HEADER_LEN + 8)
                        f.payload[frame.FRAME_HEADER_LEN..][0..8].*
                    else
                        @splat(0);
                    const queued = if (opens_block)
                        self.enqueueHeaderBlock(n, &consumed)
                    else
                        self.sched.enqueueControl(f.payload, class, t, s);
                    queued catch |err| switch (err) {
                        error.PoolFull, error.QueueFull => {
                            // Payload already freed by enqueueControl.
                            if (diag_park) _ = diag_ctrl_fail.fetchAdd(1, .monotonic);
                            if (comptime test_observe) {
                                if (class != .terminal) {
                                    if (err == error.PoolFull) _ = test_ctrl_ordinary_pool_full.fetchAdd(1, .monotonic) else _ = test_ctrl_ordinary_queue_full.fetchAdd(1, .monotonic);
                                }
                            }
                            // A terminal frame that cannot be queued: there
                            // is no way to go on. The rest of the batch runs
                            // its local resets and is released.
                            if (class == .terminal) {
                                self.finishIntentsAfterEnqueueFail(self.intent_batch[consumed..n]);
                                consumed = n;
                                if (comptime test_observe) _ = test_ctrl_terminal_full.fetchAdd(1, .monotonic);
                                self.noteServerClose("ctrl_terminal_full", err);
                                self.writer_failed.store(true, .release);
                                return error.OutOfMemory;
                            }
                            // An ordinary frame, or a whole header block
                            // (`enqueueHeaderBlock` took its fragments and
                            // freed them): only it is dropped. The rest of the
                            // batch is other streams' work, and the
                            // connection goes on: an ordinary refusal is never
                            // a connection error.
                            // The waiter whose ticket rode on it hears now.
                            if (t != 0) self.tickets.wake(s, t, false);
                            switch (f.typ) {
                                // The peer never gets the response's start:
                                // the stream is reset (a handler waiting on
                                // it is woken by the reset). The RST is
                                // queued, and this call's next round sends it.
                                .headers => self.session.resetRefusedHeaders(f.stream_id, f.owes_rst_if_refused) catch {},
                                // What RFC 9113 requires is owed and queued
                                // again when the share has room
                                // (`Session.Owed`, `materializeIntents`). Past
                                // its bound the peer gets ENHANCE_YOUR_CALM,
                                // which this call's next round queues.
                                .window_update => self.session.oweWindowUpdate(f.stream_id, wu_increment),
                                .settings => {
                                    // The server preface goes first, into an
                                    // empty share (`finishBoot`).
                                    if (!f.flags.end_stream) std.debug.panic("the server preface SETTINGS was refused", .{});
                                    try self.session.oweSettingsAck();
                                },
                                .ping => if (f.flags.end_stream) {
                                    try self.session.owePingAck(ping_payload);
                                },
                                // Nothing else is ordinary, but the graceful
                                // PING (non-ack): dropped. Phase 1 ends at its
                                // deadline without the ack.
                                else => {},
                            }
                            continue;
                        },
                    };
                    if (comptime test_observe) {
                        if (f.typ == .rst_stream and self.controlPausedNow()) _ = test_rst_queued_paused.fetchAdd(1, .monotonic);
                        if (class == .terminal) {
                            const len = self.sched.terminal_len;
                            var high = test_terminal_ring_high.load(.monotonic);
                            while (len > high) high = test_terminal_ring_high.cmpxchgWeak(high, len, .monotonic, .monotonic) orelse break;
                        }
                    }
                },
                .dispatch_request => |d| try self.dispatch(d),
                .early_reject => {},
                .stream_reset => |r| self.applyStreamResetIntent(r),
                .connection_error => |ce| {
                    if (close_reason) {
                        var code_buf: [12]u8 = undefined;
                        self.printCloseReason("session_goaway", "-", std.fmt.bufPrint(&code_buf, "{d}", .{@intFromEnum(ce.code)}) catch "?");
                    }
                    if (close_probe) diagRawPrint("t1002 conn={x} intent_connection_error parked={d}\n", .{ @intFromPtr(self), close_probe_parked.load(.acquire) });
                    for (self.handlers.slice()) |*slot| {
                        if (!slot.in_use) continue;
                        slot.terminal.setCause(.connection_closed);
                    }
                    // The writer is NOT failed: the GOAWAY queued just before this
                    // intent is still to be written (closing phase). Handlers
                    // are told the connection is closed by the cause above.
                },
                .connection_closed => {
                    for (self.handlers.slice()) |*slot| {
                        if (!slot.in_use) continue;
                        slot.terminal.setCause(.connection_closed);
                    }
                },
            }
        }
    }

    /// The header block whose HEADERS is `intent_batch[consumed.* - 1]`,
    /// with its CONTINUATIONs right behind it (`Session.cmdHeaders` queues a
    /// block whole, and a drain takes the whole queue), admitted as one unit
    /// (`FairScheduler.enqueueControlBlock`). Advances `consumed` past the
    /// fragments: on success the scheduler owns every payload, on `PoolFull`
    /// every payload is freed, and either way the fragments' intents hold
    /// nothing more to release.
    fn enqueueHeaderBlock(self: *Connection, n: usize, consumed: *usize) error{ PoolFull, QueueFull }!void {
        const first = self.intent_batch[consumed.* - 1].outbound_frame;
        var payloads: [limits_mod.MAX_HEADER_FRAGMENTS][]u8 = undefined;
        payloads[0] = first.payload;
        var k: usize = 1;
        var open = true;
        while (open) : (k += 1) {
            if (consumed.* >= n) std.debug.panic("the header block of stream {d} ends without END_HEADERS in the batch", .{first.stream_id});
            const frag = switch (self.intent_batch[consumed.*]) {
                .outbound_frame => |nf| nf,
                else => std.debug.panic("the header block of stream {d} is broken by another intent", .{first.stream_id}),
            };
            if (frag.typ != .continuation or frag.stream_id != first.stream_id) std.debug.panic("the header block of stream {d} is broken by another frame", .{first.stream_id});
            payloads[k] = frag.payload;
            open = fair_scheduler.leavesBlockOpen(frag.payload);
            consumed.* += 1;
        }
        return self.sched.enqueueControlBlock(payloads[0..k], first.ticket.id, first.ticket.slot);
    }

    /// Materialize every Session intent and emit all scheduler work now.
    fn processIntents(self: *Connection) anyerror!void {
        self.assertSessionHeld("processIntents");
        try self.materializeIntents();
        try self.drainEmit();
    }

    fn processIntentsWithReceipt(self: *Connection, ticket: u64, slot: u32, attached: *bool) anyerror!void {
        self.assertSessionHeld("processIntentsWithReceipt");
        try self.materializeIntents();
        try self.drainEmitReceipt(ticket, slot, attached);
    }

    /// A handler's flush with nothing pending: one bare barrier, pushed only
    /// once the ack channel has room for it (`waitForAckRoom`). Every other
    /// push is a drain's, gated by `h2cPause`. Caller holds `session_mu`.
    fn pushBareBarrier(self: *Connection, hctx: *HandlerCtx, stream_id: u31, ticket: u64, slot: u32) response.ResponseError!void {
        try self.waitForAckRoom(hctx, stream_id);
        self.sendFlushTicket(ticket, slot, false) catch return error.WriteFailed;
        if (comptime test_observe) {
            if (self.tls_driver) |pump| {
                // Under `session_mu`, so nothing was pushed since: the room the
                // barrier left. A drain's pop needs `pushes_per_pop` slots and
                // its tail push `tail_pushes_per_drain`; a barrier is let in only
                // while the stash is not full, so this never falls under the tail.
                const left = pump.freeSlots();
                var min = test_tls_barrier_min_room.load(.monotonic);
                while (left < min) min = test_tls_barrier_min_room.cmpxchgWeak(min, left, .monotonic, .monotonic) orelse break;
            }
        }
    }

    fn sendFlushTicket(self: *Connection, ticket: u64, slot: u32, complete_batch_receipt: bool) anyerror!void {
        try self.pushWriteChunk(.{
            .bytes = &.{},
            .len = 0,
            .flush_barrier = true,
            .ticket = ticket,
            .ticket_slot = slot,
            .ticket_count = 1,
            .complete_batch_receipt = complete_batch_receipt,
        });
    }

    /// A drain's tail push always finds a free stash slot: the pause stopped
    /// every pop while `Pump.tail_pushes_per_drain` slots were still free. A
    /// trap here means one pop pushed more than `Pump.pushes_per_pop`, and in
    /// a build without the check that push would fail the connection closed.
    fn assertTailRoom(self: *Connection, comptime site: []const u8) void {
        if (!std.debug.runtime_safety) return;
        const pump = self.tls_driver orelse return;
        if (pump.hasRoomFor(tls_edge.Pump.tail_pushes_per_drain)) return;
        std.debug.panic("TLS stash has no room for the {s} tail push: write_ch={d}/{d}", .{
            site,
            io_queue.chanLen(wire_pump.WireChunk, pump.write_ch),
            pump.write_ch.impl.capacity,
        });
    }

    /// True when the actor must not park on `write_ch`. WritePump may already
    /// have exited (peer close / socket shutdown), and a blocking put then
    /// waits forever while holding `session_mu`.
    fn writeGoingAway(self: *Connection) bool {
        if (self.writer_failed.load(.acquire) or self.shutting_down) return true;
        const sf = self.config.shutdown_flag orelse return false;
        return sf.load(.acquire);
    }

    /// Hand a chunk to WritePump. Never parks the actor once the connection is
    /// going away: tryPut, shut the socket to unstick a blocked write, retry
    /// once, then fail. A healthy connection still `putOne`s for backpressure.
    fn pushWriteChunk(self: *Connection, chunk: wire_pump.WireChunk) error{ WriteFailed, Canceled }!void {
        if (self.config.mode == .tls) {
            // Encrypt + stage + arm on this task. Acks apply in `post` via
            // `ack_apply` (no write_ack_ch hop). Two things keep the stash from
            // filling: `pause` (DATA) and `pause_control` (HEADERS/WINDOW_UPDATE)
            // stop a drain before a pop, and a handler's bare flush barrier,
            // which is a push outside any drain, waits in `waitForAckRoom` for
            // the same room. So trySend is the last-resort fail-close.
            const pump = self.tls_driver orelse {
                _ = tls_edge.tls_stage_failed.fetchAdd(1, .monotonic);
                return error.WriteFailed;
            };
            // Stash only. Encrypting on this stack under session_mu overflowed
            // the coroutine (the old BoringSSL frame plus drainEmit). driveTlsTurn
            // writes after the lock drops.
            pump.push(chunk) catch {
                _ = tls_edge.tls_write_overflow.fetchAdd(1, .monotonic);
                // t-1022 probe: the fail-close moment, with the backlog this
                // drain turn is trying to move and the gap that built it.
                if (diag_park) {
                    var backlog: usize = 0;
                    for (self.sched.pending_slots.slice()) |s| backlog += s.len;
                    diagRawPrint("OVERFLOW backlog_bytes={d} sched_pending={d} prev_drain_gap_us={d} chunk_len={d}\n", .{ backlog, self.sched.pendingCount(), diag_prev_drain_gap_ns / 1000, chunk.len });
                }
                return error.WriteFailed;
            };
            // The actor is the writer here and does not select on the stash. A
            // chunk pushed from a handler's task (a drain run by `sendCb`,
            // `flushCb`, `abortCb`, the error reset, a bare barrier) would leave
            // an idle actor asleep until something else wakes it. Every such
            // push is made under `session_mu`, and that task's release of it
            // rings (`unlockSession`), so this site rings nothing. The actor's
            // own drains write in the same turn (the hot path). On h2c the pump
            // task waits on the queue.
            return;
        }
        // Counted before the push: the pump may post its completion, and the
        // actor receive it, before the push returns.
        const counted = wire_pump.postsCompletion(chunk);
        if (counted) self.acks.add();
        errdefer if (counted) self.acks.sub();
        // `h2cPause` and `waitForAckRoom` keep this from firing. If a push
        // ever passes the channel's room (one made outside both gates, or a
        // pop that pushed more than `pushes_per_pop`), refuse it in every
        // build: the pump would panic on a completion it cannot post, so the
        // connection fails closed instead.
        if (self.ack_room_armed and self.acks.owed.load(.acquire) + wire_pump.terminal_posts > self.write_ack_buf.len) {
            if (comptime test_observe) _ = test_ack_bound_refusals.fetchAdd(1, .monotonic);
            self.noteServerClose("ack_bound", null);
            self.writer_failed.store(true, .release);
            return error.WriteFailed;
        }
        // The push never waits. `h2cPause` leaves room in the queue for every
        // pop's pushes and the tail, and `waitForAckRoom` for a bare barrier,
        // and both are decided under `session_mu`, which this push holds, with
        // only the pump taking from the queue. So a full queue here is a gate
        // that failed: it traps in a safe build, and in any build the
        // connection fails closed instead of parking the task that holds
        // `session_mu`. A closed queue (the connection is going away) is a
        // failed write, as before.
        if (io_queue.tryPut(wire_pump.WireChunk, &self.write_ch, self.config.io, chunk)) return;
        // A put that fails with room left in the queue is a closed queue.
        if (io_queue.queued(wire_pump.WireChunk, &self.write_ch, self.config.io) < self.write_ch_buf.len) return error.WriteFailed;
        // The teardown sentinel is uncounted, so a full queue after it is the
        // connection ending, not a gate that failed: no trap, no refusal.
        if (self.write_queue_ending.load(.seq_cst)) return error.WriteFailed;
        if (comptime test_observe) _ = test_queue_push_refusals.fetchAdd(1, .monotonic);
        if (std.debug.runtime_safety and !self.writeGoingAway() and !((comptime test_observe) and test_allow_full_queue_push.load(.acquire))) {
            std.debug.panic("h2c write queue is full at a push: a pause hook or the barrier gate let it through", .{});
        }
        self.noteServerClose("queue_full", null);
        self.writer_failed.store(true, .release);
        return error.WriteFailed;
    }

    /// A frame that fits a frame slab takes one. A larger frame (a full DATA
    /// frame is 16,393 bytes) takes a write-pool chunk, because that chunk is
    /// what an unbatched frame becomes: `queueWire` hands it to the pump as
    /// is, with no copy. Small frames stay in `frame_pool`: most of them are
    /// copied into the drain-turn batch, and a 16 KiB chunk for each would
    /// empty the write pool (`inbound_wire_chunks_per_connection` chunks).
    /// A HEADERS block larger than a frame slab also takes a write chunk, and
    /// so does a small frame when every slab is rented.
    ///
    /// A frame made outside a scheduler pop (a handler's HEADERS, a control
    /// frame answering ingest) may take a chunk only while more than
    /// `POP_PUSHES` stay free. Those frames wait in the scheduler until they
    /// are popped, and a pop needs `POP_PUSHES` free chunks
    /// (`outboundMemoryPaused`): if waiting frames could hold every chunk, no
    /// pop could ever run and nothing would give a chunk back. The DATA frame
    /// built inside a pop (`building_pop_frame`) may take any chunk the pop's
    /// room check already counted.
    ///
    /// A terminal frame (RST_STREAM, GOAWAY) never comes from here: it is
    /// built in `terminal_store` (`rentTerminal`), so the slabs and chunks the
    /// other frames take cannot leave it without room.
    ///
    /// Null means refused. There is no general-allocator fallback: the pools
    /// are the bound.
    fn rentFrame(self: *Connection, n: usize, class: session_mod.FrameClass) ?[]u8 {
        if (class != .ordinary) return self.rentTerminal(n, class);
        // A small frame built in a pop is written straight into the drain-turn
        // scratch, past what it already holds: no pool memory, no copy. The
        // sink then only counts it (`EmitBatch.adopt`). `packsWithNeighbours`
        // is the sink's own rule for joining, so the frame joins.
        if (self.building_pop_frame and emit_batch.packsWithNeighbours(n, emit_batch.max_plaintext)) {
            if (self.pack_scratch) |batch| {
                if (batch.tail(n)) |dest| {
                    _ = outbound_memory.frames_built.fetchAdd(1, .monotonic);
                    _ = outbound_memory.frames_in_scratch.fetchAdd(1, .monotonic);
                    return dest;
                }
            }
        }
        if (n <= self.frame_pool.slab_bytes) {
            if (self.frame_pool.tryRent(self.config.io)) |slab| {
                _ = outbound_memory.frames_built.fetchAdd(1, .monotonic);
                _ = outbound_memory.frames_from_slab.fetchAdd(1, .monotonic);
                return slab;
            }
        }
        if (n <= limits_mod.WIRE_CHUNK_SIZE) {
            const keep: usize = if (self.building_pop_frame) 0 else wire_const.POP_PUSHES;
            if (self.write_pool.freeCount() > keep) {
                if (self.write_pool.tryRentIndex(self.config.io)) |idx| {
                    _ = outbound_memory.frames_built.fetchAdd(1, .monotonic);
                    _ = outbound_memory.frames_from_chunk.fetchAdd(1, .monotonic);
                    return self.write_pool.slabAt(idx);
                }
            }
        }
        _ = outbound_memory.frames_refused.fetchAdd(1, .monotonic);
        return null;
    }

    /// Build room for one terminal frame in `terminal_store`, or null when the
    /// peer holds more unwritten terminal frames than a well-behaved peer can
    /// (`Limits.terminalFrames`, T). Runs under `session_mu`, like every frame
    /// build.
    ///
    /// The final GOAWAY (`.final_goaway`) may take the last slot. Every other
    /// terminal frame is admitted only while the final GOAWAY would still have
    /// room. One store slot is enough to say so in all three places it needs:
    /// a frame holds its store slot from the build until it is copied out at
    /// the pop, and holds its terminal ring entry and its terminal pool share
    /// from the queueing until the same pop. So the slots rented are at least
    /// the ring entries and the pool entries held, and a free slot is a free
    /// ring entry and a free pool entry. (A frame with a flush ticket is
    /// written from its slot and keeps it longer, which only makes the check
    /// refuse a little early.) Bytes need no check of their own: at most M
    /// RSTs and three GOAWAYs fit in T entries, and `Limits.terminalBytes` is
    /// exactly that.
    /// A refused final GOAWAY is a broken invariant, and loud in Debug.
    fn rentTerminal(self: *Connection, n: usize, class: session_mod.FrameClass) ?[]u8 {
        std.debug.assert(class != .ordinary);
        std.debug.assert(n <= self.terminal_store.slab_bytes);
        const keep: usize = if (class == .final_goaway) 0 else 1;
        if (self.terminal_store.freeCount() > keep) {
            if (self.terminal_store.tryRent(self.config.io)) |slot| {
                _ = outbound_memory.frames_built.fetchAdd(1, .monotonic);
                _ = outbound_memory.frames_from_terminal_store.fetchAdd(1, .monotonic);
                return slot;
            }
        }
        _ = outbound_memory.frames_refused.fetchAdd(1, .monotonic);
        if (class == .final_goaway and builtin.mode == .debug) {
            std.debug.panic("the final GOAWAY was refused (store free {d}, ring {d}): the terminal bound is broken", .{
                self.terminal_store.freeCount(), self.sched.terminal_len,
            });
        }
        return null;
    }

    /// True when the scheduler must not pop: the pop could push `POP_PUSHES`
    /// chunks (the batch it flushes, and the frame itself when it is
    /// unbatchable) and fewer are free. The chunks come back when the writer
    /// finishes one, which posts a write completion (h2c) or advances the
    /// stash (TLS), and either wakes the actor's select, which drains again.
    /// Nothing waits here and no lock is held across a wait.
    fn outboundMemoryPaused(self: *Connection) bool {
        if (!self.outboundMemoryLow()) return false;
        _ = outbound_memory.drain_memory_pauses.fetchAdd(1, .monotonic);
        return true;
    }

    /// `outboundMemoryPaused` without counting a pause.
    fn outboundMemoryLow(self: *const Connection) bool {
        return self.write_pool.freeCount() < wire_const.POP_PUSHES;
    }

    /// The scheduler's control pause (`pause_control`) as the hooks would
    /// answer it, without what the hooks count (`drain_memory_pauses`, the
    /// test pause counters). For observe-build probes.
    fn controlPausedNow(self: *Connection) bool {
        if (self.tls_driver) |p| return p.stashFull() or self.outboundMemoryLow();
        if (self.config.mode != .h2c or self.sched.pause_control == null) return false;
        if (self.outboundMemoryLow()) return true;
        if (self.ack_room_armed and !self.h2cAckRoom(0)) return true;
        return !self.h2cQueueRoom(h2c_queue_room_per_pop);
    }

    fn payloadInScratch(self: *const Connection, buf: []const u8) bool {
        const base = @intFromPtr(self.plaintext_scratch.ptr);
        const here = @intFromPtr(buf.ptr);
        return here >= base and here < base + self.plaintext_scratch.len;
    }

    /// Return a frame buffer to its pool, `frame_pool` or `write_pool`. `buf`
    /// may be a used prefix; the pointer must be the slab base. A buffer in
    /// no pool is a bug (every frame comes from `rentFrame`), and there
    /// is no general allocator to hand it to.
    fn releaseWireBytes(self: *Connection, buf: []u8) void {
        if (buf.len == 0) return;
        // Built in the drain-turn scratch: it owns no pool memory.
        if (self.payloadInScratch(buf)) return;
        if (self.frame_pool.containsPtr(buf.ptr)) {
            self.frame_pool.release(self.config.io, self.frame_pool.slabFromPtr(buf.ptr));
            return;
        }
        if (self.terminal_store.containsPtr(buf.ptr)) {
            self.terminal_store.release(self.config.io, self.terminal_store.slabFromPtr(buf.ptr));
            return;
        }
        if (self.write_pool.containsPtr(buf.ptr)) {
            self.write_pool.releaseIndex(self.config.io, self.write_pool.indexFromPtr(buf.ptr));
            return;
        }
        std.debug.panic("frame buffer of {d} bytes is in neither the frame pool nor the write pool", .{buf.len});
    }

    /// The write chunk an owned frame buffer already is, or null for a frame
    /// slab, which is too small to be a write chunk and is copied instead.
    fn frameAsWriteLease(self: *Connection, buf: []u8) ?WriteLease {
        if (self.frame_pool.containsPtr(buf.ptr) or self.terminal_store.containsPtr(buf.ptr)) return null;
        std.debug.assert(self.write_pool.containsPtr(buf.ptr));
        const idx = self.write_pool.indexFromPtr(buf.ptr);
        return .{ .bytes = self.write_pool.slabAt(idx), .pool_index = idx };
    }

    const WriteLease = struct {
        bytes: []u8,
        pool_index: u32,
    };

    /// Take a write-pool chunk, or fail. Never blocks and never allocates: the
    /// actor must not park on the write pool (it would miss shutdown, reads,
    /// and writer failure while WritePump may already be idle). The drain
    /// checks the room before every pop (`outboundMemoryPaused`), so the
    /// pops that reach here find a chunk; the refusal counter is the proof,
    /// and a refusal fails the connection like any other write failure.
    fn leaseWriteChunk(self: *Connection, n: usize) error{OutOfMemory}!WriteLease {
        if (n <= limits_mod.WIRE_CHUNK_SIZE) {
            if (self.write_pool.tryRentIndex(self.config.io)) |idx| {
                _ = outbound_memory.chunks_leased.fetchAdd(1, .monotonic);
                return .{
                    .bytes = self.write_pool.slabAt(idx),
                    .pool_index = idx,
                };
            }
        }
        _ = outbound_memory.chunks_refused.fetchAdd(1, .monotonic);
        return error.OutOfMemory;
    }

    fn returnWriteChunk(self: *Connection, lease: WriteLease) void {
        self.write_pool.releaseIndex(self.config.io, lease.pool_index);
    }

    /// Hand `src` to the WritePump with accounting attached. With `owned` null,
    /// copy `src` into a fresh write chunk and leave `src` with the caller.
    /// With `owned` set, `src` is the start of `owned.bytes` and that chunk
    /// goes to the pump as is; this function then owns it on every path.
    ///
    /// `credit` is the DATA payload bytes `src` carries, still counted as
    /// pending. The chunk carries them to the pump (`outbound_release`) and
    /// the ack releases exactly those; the frames' headers and the controls in
    /// `src` are not the budget's bytes. This function owns the credit on
    /// every path: it transfers it to the wire side before the push (the pump
    /// may ack, and the actor apply the ack, before the push returns) and
    /// releases it if the chunk never reaches the pump.
    ///
    /// The chunk carries the release amounts rather than a callback, so the
    /// pump needs no access to `Connection`. The actor applies them when the
    /// write completes. That is the whole reason the pumps can run without
    /// a lock.
    ///
    /// Both failure paths release the reservation AND return the write-chunk
    /// lease before they return. After a successful `putOne` the pump owns the
    /// lease, and this function must not touch it again.
    fn sendAccountedWire(
        self: *Connection,
        src: []const u8,
        flush: bool,
        ticket: u64,
        ticket_slot: u32,
        ticket_count: u32,
        control_n: usize,
        control_entries: u32,
        complete_batch_receipt: bool,
        credit: usize,
        owned: ?WriteLease,
    ) anyerror!void {
        const send_t0 = if (trace.enabled) nowNs(self.config.io) else 0;
        defer if (trace.enabled) {
            _ = trace.send_ns.fetchAdd(nowNs(self.config.io) -% send_t0, .monotonic);
            _ = trace.send_n.fetchAdd(1, .monotonic);
            _ = trace.send_bytes.fetchAdd(src.len, .monotonic);
        };
        if (owned) |o| std.debug.assert(o.bytes.ptr == src.ptr and src.len <= o.bytes.len);
        std.debug.assert(credit <= src.len);
        const lease = if (owned) |o| o else self.leaseWriteChunk(src.len) catch |err| {
            self.applyOutboundRelease(credit, .pending);
            if (control_entries != 0) self.applyControlRelease(control_n, control_entries);
            return err;
        };
        std.debug.assert(src.len <= lease.bytes.len);
        if (owned == null) {
            @memcpy(lease.bytes[0..src.len], src);
            self.wire_copied_bytes += src.len;
        }
        if (trace.enabled) _ = trace.records.fetchAdd(1, .monotonic);
        self.transferOutbound(credit);
        self.pushWriteChunk(.{
            .bytes = lease.bytes,
            .len = src.len,
            .flush_barrier = flush,
            .ticket = ticket,
            .ticket_slot = ticket_slot,
            .ticket_count = ticket_count,
            .complete_batch_receipt = complete_batch_receipt,
            .outbound_release = credit,
            .control_release = control_n,
            .control_entries = control_entries,
            .pool_index = lease.pool_index,
        }) catch |err| {
            self.applyOutboundRelease(credit, .wire);
            if (control_entries != 0) self.applyControlRelease(control_n, control_entries);
            self.returnWriteChunk(lease);
            // `err` is `WriteFailed` or `Canceled`; a cancel must not become a failed write.
            return err;
        };
        if (comptime test_observe) _ = outbound_memory.chunks_to_pump.fetchAdd(1, .monotonic);
    }

    /// The single exit from protocol bytes to transport bytes. Called ONLY by
    /// the `FairScheduler` sink; `test_queue_wire_bypass` proves it.
    ///
    /// HTTP/2 bytes are always plaintext here. h2c copies them into a WritePump
    /// chunk; TLS encrypts the same chunk on this task (`pushWriteChunk` →
    /// `writeChunks`), stages ciphertext, and arms the send. Acks apply in
    /// `post` via `ack_apply` — no `write_ack_ch` hop. `session_mu` is held.
    ///
    /// One input is one frame or a drain-turn concat. The ticket chain, flush
    /// flag, and control-pool release ride with that one handoff.
    fn batchContainsTraceTicket(self: *Connection, ticket_slot: u32, ticket_count: u32) bool {
        const target = self.trace_ticket.load(.acquire);
        if (target == 0 or ticket_count == 0) return false;
        var slot_i = ticket_slot;
        var remaining = ticket_count;
        while (remaining > 0) : (remaining -= 1) {
            const meta = self.tickets.completion(slot_i) orelse return false;
            if (meta.ticket == target) return true;
            if (remaining > 1) {
                if (meta.next == ticket_table.no_completion_slot) return false;
                slot_i = meta.next;
            }
        }
        return false;
    }

    /// RFC 9113 section 3.4: the server connection preface is a SETTINGS
    /// frame, and it must be the first frame the server sends. `plain` is the
    /// first handoff of the connection, so its first frame is the one that
    /// goes first on the wire (h2c and TLS alike). A SETTINGS ACK is not the
    /// preface: the t-538 defect put one first.
    const PrefaceCheck = enum { ok, short, not_settings, ack, on_stream };

    fn checkPrefaceFirst(plain: []const u8) PrefaceCheck {
        if (plain.len < frame.FRAME_HEADER_LEN) return .short;
        const hdr = frame.FrameHeader.decode(plain[0..frame.FRAME_HEADER_LEN]);
        if (hdr.type != .settings) return .not_settings;
        if (hdr.flags.ack()) return .ack;
        if (hdr.stream_id != 0) return .on_stream;
        return .ok;
    }

    /// Panics in safe builds; one predictable branch in ReleaseFast (the
    /// `wire_started` test at the call site).
    fn assertPrefaceFirst(plain: []const u8) void {
        if (comptime test_observe) _ = test_preface_checks.fetchAdd(1, .acq_rel);
        if (comptime !std.debug.runtime_safety) return;
        const check = checkPrefaceFirst(plain);
        if (check != .ok) {
            const type_byte: u8 = if (plain.len > 3) plain[3] else 0xff;
            std.debug.panic("first wire handoff of a connection is not the server SETTINGS: {s} (frame type byte {d}, {d} bytes)", .{ @tagName(check), type_byte, plain.len });
        }
    }

    fn queueWire(
        self: *Connection,
        bytes: []u8,
        flush: bool,
        ticket: u64,
        ticket_slot: u32,
        ticket_count: u32,
        control_n: usize,
        control_entries: u32,
        credit: usize,
        owns_bytes: bool,
        complete_batch_receipt: bool,
    ) anyerror!void {
        if (comptime test_observe) {
            if (!test_in_sched_sink) {
                _ = test_queue_wire_bypass.fetchAdd(1, .acq_rel);
            }
        }
        // `bytes_held` goes false once `sendAccountedWire` owns the buffer.
        var bytes_held = owns_bytes;
        defer if (bytes_held) self.releaseWireBytes(bytes);
        const plain = if (owns_bytes) frame.encodedOnWire(bytes) else bytes;
        if (!self.wire_started) {
            self.wire_started = true;
            assertPrefaceFirst(plain);
        }
        if (trace.enabled) trace.noteHandoff(ticket_count, plain.len);
        const trace_batch = trace.enabled and self.batchContainsTraceTicket(ticket_slot, ticket_count);
        if (trace_batch) self.trace_emit_ns.store(nowNs(self.config.io), .release);
        if (comptime test_observe) {
            const sends = test_wire_sends.fetchAdd(1, .acq_rel);
            const fail_after = test_force_wire_fail_after.load(.acquire);
            if (fail_after != 0 and sends + 1 >= fail_after) {
                if (control_entries != 0) self.applyControlRelease(control_n, control_entries);
                self.applyOutboundRelease(credit, .pending);
                return error.WriteFailed;
            }
        }
        const owned = if (owns_bytes) self.frameAsWriteLease(bytes) else null;
        if (owned != null) bytes_held = false;
        try self.sendAccountedWire(plain, flush, ticket, ticket_slot, ticket_count, control_n, control_entries, complete_batch_receipt, credit, owned);
        if (trace_batch) self.trace_queued_ns.store(nowNs(self.config.io), .release);
    }

    /// Drop a dispatch-intent payload that never became a job: give back the
    /// request's memory, which holds its headers and its body, unless dispatch
    /// has already handed it to a job.
    fn releaseDispatchRequest(self: *Connection, d: session_mod.DispatchRequest) void {
        self.releaseHeaderList(d.stream_id);
    }

    /// Build a handler job. Actor-only, under `session_mu`.
    ///
    /// Ownership of the decoded request moves from the Session intent to the
    /// handler job. The header strings were decoded into the request's own
    /// arena, which moves into the job here; only the lightweight Header
    /// arrays and handler scratch are new allocations in it. The handler
    /// therefore shares nothing with Session, and nothing is copied.
    ///
    /// Admission order is reserve-then-claim, and it is deliberate:
    /// 1. Classify the route. Complete oneshots never spawn, so they skip the
    ///    reaper token. A task handler that cannot be canceled must never start,
    ///    because cancellation has no retry path.
    /// 2. Reserve reaper capacity for a task handler (and 404/405, which spawn).
    /// 3. Claim a handler slot.
    /// 4. Point `Response` at the SLOT's terminal state, never at job memory.
    /// 5. Increment the live counters, then spawn a task handler or queue a
    ///    complete handler to run on the actor after this lock drops.
    ///
    /// Each failure step undoes the step before it and answers the peer with
    /// REFUSED_STREAM, which is retryable, rather than dropping the request.
    ///
    /// A spawn failure for a *task* handler refuses the stream: running it on
    /// the actor would park ingest for as long as that handler blocks (SSE).
    /// Complete handlers are the intentional no-spawn path: they are queued
    /// here and run in `runPendingInline` with the lock released.
    ///
    /// No free slot is not a refusal: the request parks (`parked`) and
    /// `dispatchParked` resumes it when a slot frees. A request never
    /// overtakes one that is already waiting.
    fn dispatch(self: *Connection, d: session_mod.DispatchRequest) anyerror!void {
        // The stream ended later in the same batch (the peer reset it, or a
        // stream error closed it): there is no one to answer, so the request
        // is dropped, as `dispatchParked` drops one that waited. Running it
        // would also let one batch refuse more dispatches than it has live
        // streams, and each refusal queues intents (`Limits.intentEntriesMin`).
        if (self.session.streams.get(d.stream_id) == null) {
            self.releaseDispatchRequest(d);
            return;
        }
        if (self.parked_len == 0 and try self.tryDispatch(d)) return;
        self.park(d);
    }

    fn park(self: *Connection, d: session_mod.DispatchRequest) void {
        if (self.parked_len == self.parked.len) {
            // More parked than open streams can exist: an accounting fault,
            // answered like the old no-slot path rather than dropped.
            self.session.applyCommand(.{ .reset_stream = .{ .stream_id = d.stream_id, .code = .refused_stream } }) catch {};
            self.releaseDispatchRequest(d);
            return;
        }
        self.parked[(self.parked_head + self.parked_len) % self.parked.len] = d;
        self.parked_len += 1;
    }

    /// Dispatch parked requests, oldest first, while slots are free. A stream
    /// the peer reset while it waited is no longer in Session; its request is
    /// dropped instead of run. Actor-only, under `session_mu`.
    fn dispatchParked(self: *Connection) anyerror!void {
        self.assertSessionHeld("dispatchParked");
        while (self.parked_len != 0) {
            const d = self.parked[self.parked_head];
            if (self.session.streams.get(d.stream_id) == null) {
                self.popParked();
                self.releaseDispatchRequest(d);
                continue;
            }
            // Pop first: on an error tryDispatch has already released `d`.
            self.popParked();
            if (!try self.tryDispatch(d)) {
                self.parked_head = (self.parked_head + self.parked.len - 1) % self.parked.len;
                self.parked[self.parked_head] = d;
                self.parked_len += 1;
                return;
            }
        }
    }

    fn popParked(self: *Connection) void {
        std.debug.assert(self.parked_len != 0);
        self.parked_head = (self.parked_head + 1) % self.parked.len;
        self.parked_len -= 1;
    }

    /// Release every parked request without running it. Teardown only.
    fn releaseParked(self: *Connection) void {
        while (self.parked_len != 0) {
            const d = self.parked[self.parked_head];
            self.popParked();
            self.releaseDispatchRequest(d);
        }
    }

    /// The request's memory ran out while its job was being built. Answer the
    /// stream and give the slot, and with it the request's memory, back. The
    /// answer is queued as an intent; the next drain sends it (see the spawn
    /// failure below for why this does not drain here). Returns true: `d` is
    /// consumed.
    fn refuseDispatchForMemory(self: *Connection, d: session_mod.DispatchRequest, lease: *RequestLease) bool {
        const status = (lease.arena.refusal orelse request_pool.Reason.server_cap).status();
        self.session.applyCommand(.{ .respond_headers = .{
            .stream_id = d.stream_id,
            .status = status,
            .headers = &.{},
            .end_stream = true,
            .ticket = .none,
        } }) catch {};
        std.debug.assert(self.releaseSlot(d.stream_id));
        return true;
    }

    /// The body of `dispatch`. Returns false, with `d` still the caller's,
    /// when no handler slot is free; every other outcome (a handler started,
    /// or the stream refused) consumes `d`.
    fn tryDispatch(self: *Connection, d: session_mod.DispatchRequest) anyerror!bool {
        var dispatch_owned = true;
        defer if (dispatch_owned) self.releaseDispatchRequest(d);

        // Classify before reserving: complete oneshots have no join handle.
        // `path_remainder` slices `d.path`, which lives in the request arena
        // the job takes over, so the match stays valid for the handler.
        const classified = self.config.router.match(request.Method.parse(d.method), d.path);
        const complete_inline = switch (classified) {
            .found => |f| f.handler == .complete,
            else => false,
        };

        var reaper_reserved = false;
        if (!complete_inline) {
            if (self.config.accounting) |acct| {
                if (!acct.tryReserveReaper()) {
                    // Queued, not processed: this can run inside
                    // `materializeIntents`, whose batch a nested drain would
                    // overwrite. That loop, or the caller's, sends it.
                    self.session.applyCommand(.{ .reset_stream = .{ .stream_id = d.stream_id, .code = .refused_stream } }) catch {};
                    return true;
                }
                reaper_reserved = true;
            }
        }
        // `defer`, not `errdefer`: the no-slot return below carries no error,
        // and an errdefer leaked one server-wide token per such task stream
        // until every task stream was refused. A parked request reserves a
        // fresh token when it resumes. The slot takes ownership where
        // `slot.reaper_reserved` is set.
        defer if (reaper_reserved) {
            if (self.config.accounting) |acct| acct.releaseReaper();
        };

        const slot = self.allocSlot(d.stream_id) orelse {
            dispatch_owned = false;
            return false;
        };
        errdefer std.debug.assert(self.releaseSlot(d.stream_id));
        const slot_i = self.slotIndex(d.stream_id) orelse unreachable;
        const job = &self.handler_jobs[slot_i];
        // Session decodes every dispatched request into a lease (it refuses
        // the stream when there is none), so a miss here is a broken
        // invariant, and its strings would be memory nobody owns.
        const lease = self.findLease(d.stream_id) orelse
            std.debug.panic("dispatch of stream {d} without request memory", .{d.stream_id});
        lease.in_job = true;
        job.* = .{
            .conn = self,
            .arena = &lease.arena,
            .stream_id = d.stream_id,
            .owned_request = undefined,
            .req = undefined,
            .resp = undefined,
            .matched = .not_found,
        };
        const a = job.arena.allocator();

        // The strings are already in this arena; only the handler-facing
        // arrays are built here. Running out of memory is an answer to the
        // stream, never a connection error: the request is refused (500 when
        // it hit its own cap, 503 for the others) and its memory goes back.
        if (comptime test_observe) {
            if (test_fail_dispatch_alloc.load(.acquire)) return self.refuseDispatchForMemory(d, lease);
        }
        const headers = a.alloc(request.Header, d.headers.len) catch return self.refuseDispatchForMemory(d, lease);
        for (d.headers, 0..) |h, i| headers[i] = .{ .name = h.name, .value = h.value };
        const trailers = a.alloc(request.Header, d.trailers.len) catch return self.refuseDispatchForMemory(d, lease);
        for (d.trailers, 0..) |h, i| trailers[i] = .{ .name = h.name, .value = h.value };
        // Repeated Accept-Encoding lines are one value (RFC 9110).
        const accept_encoding = joinAcceptEncodingLines(a, headers) catch return self.refuseDispatchForMemory(d, lease);

        job.req = .{
            .method = .parse(d.method),
            .scheme = d.scheme,
            .authority = d.authority,
            .path = d.path,
            .query = d.query,
            .headers = headers,
            .body = d.body,
            .trailers = trailers,
            .arena = a,
            .peer = peerFromHandle(self.stream.socket.handle),
        };
        job.resp = .{
            .stream_id = d.stream_id,
            .generation = 0,
            .terminal = undefined, // set after slot alloc
            .ctx = undefined, // set to &job.hctx after slot alloc
            .io = self.config.io,
            .sendFn = sendCb,
            .startFn = startCb,
            .writeFn = writeCb,
            .flushFn = flushCb,
            .abortFn = abortCb,
            .waitUntilFn = waitUntilCb,
        };
        // HandlerCtx: Response.ctx points at job-local ctx with stable terminal pointer.
        job.hctx = .{
            .conn = self,
            .terminal = undefined,
            .slot = undefined,
            .stream_id = d.stream_id,
            .method = job.req.method,
            .arena = a,
            .accept_encoding = accept_encoding,
        };
        job.matched = classified;
        switch (job.matched) {
            .found => |f| job.req.path_remainder = f.path_remainder,
            else => {},
        }

        slot.reaper_reserved = reaper_reserved;
        reaper_reserved = false;
        // From here every exit must `finishHandlerJob` (task `defer`, the end
        // of the complete batch's drain turn, or spawn-fail below). Do not run a task
        // handler synchronously on this path: that is the ingest-park.
        job.owned_request = d;
        dispatch_owned = false;
        job.resp.terminal = &slot.terminal;
        job.resp.generation = slot.terminal.currentGeneration();
        job.slot = slot;
        job.hctx.terminal = &slot.terminal;
        job.hctx.slot = slot;
        job.resp.ctx = &job.hctx;

        self.admitHandler(slot);
        if (trace.enabled) {
            const n = trace.jobs.fetchAdd(1, .monotonic);
            if (n % trace.sample_every == 0) {
                job.hctx.trace_on = true;
                job.hctx.t_dispatch = nowNs(self.config.io);
            }
        }
        const run_inline = switch (job.matched) {
            .found => |f| f.handler == .complete,
            else => false,
        };
        std.debug.assert(run_inline == complete_inline);
        if (run_inline) {
            // Run after this lock drops. sendCb takes session_mu itself, and
            // waitTicket must not hold it. A task handler that cannot spawn
            // is refused below, not queued here: SSE on the actor parks ingest.
            std.debug.assert(self.inline_n < self.inline_sids.len);
            self.inline_sids[self.inline_n] = d.stream_id;
            self.inline_n += 1;
            // A task handler's processIntents can queue a complete job while
            // the actor is parked. Flag-before-ring: inline_n is the state,
            // the ring is the persistent wake.
            //
            // KEEP, and do not suppress it for the actor. This is the actor
            // ringing ITSELF for work it made after this turn's check:
            // `dispatch` also runs on the actor, inside `runPendingInline`, and
            // the release of `session_mu` does not ring for the actor
            // (`unlockSession`). That release ring covers the handler-side
            // callers only.
            self.ring();
            return true;
        }
        // Count before spawn so a handler that finishes before this function
        // stores the join handle still decrements in finishHandlerJob.
        job.task_counted = true;
        _ = self.live_task_handlers.fetchAdd(1, .acq_rel);
        if (self.config.balancer) |b| b.noteHandlerStart(self.config.exec_index.?);
        // With `.local`, a burst of 13+ spawns fills this executor's queue and
        // zio's `registerTask` yields this actor while it holds `session_mu`:
        // a short convoy behind the new handlers, not a deadlock, because the
        // yield reschedules the actor and a handler blocked on the mutex parks.
        _ = diag_task_spawn_attempts.fetchAdd(1, .monotonic);
        const where_before = self.diag_actor_where.load(.monotonic);
        diagActor(self, .lk_spawn);
        const handle = if ((comptime test_observe) and test_force_spawn_fail)
            error.OutOfMemory
        else
            zio.spawnInto(probeHandlerPlacement(), runHandlerJob, .{job});
        diagActor(self, @enumFromInt(where_before));
        if (handle) |_| _ = diag_task_spawned.fetchAdd(1, .monotonic) else |_| {}
        const h = handle catch {
            // Admission already incremented live_handlers and claimed a slot.
            // Refuse rather than run on the actor: a blocking task handler
            // (SSE) would stop ingest, TLS, and FairScheduler on this socket.
            // Do not processIntents here: we are inside materializeIntents and
            // the intent batch is live. Its next round sends the RST.
            self.session.applyCommand(.{ .reset_stream = .{ .stream_id = d.stream_id, .code = .refused_stream } }) catch {};
            finishHandlerJob(job);
            return true;
        };
        if (self.slotIndex(d.stream_id)) |i| {
            self.handler_joins[i] = h;
        }
        return true;
    }

    /// The handler task entry point.
    ///
    /// The `defer` runs on every exit, including a cancellation, and its order
    /// is the contract with the actor:
    /// 1. CAS `live` -> `reported`. The winner posts the completion. If a
    ///    reaper already claimed the slot, this handler stays silent and the
    ///    reaper reports after its `cancel` returns.
    /// 2. Post the completion. The send IS the wake: the actor selects on the
    ///    completion channel, so no ring follows it. Without the post the slot
    ///    sits until an unrelated event arrives.
    /// 3. Decrement the live counter. This frees nothing: the request's blocks
    ///    (headers, body, arena) go back to the pool from the actor, in
    ///    `releaseSlot` after it has joined this task.
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
    fn finishHandlerJob(job: *HandlerJob) void {
        const self = job.conn;
        std.debug.assert(job.slot.in_use);
        std.debug.assert(job.slot.admitted);
        std.debug.assert(job.stream_id == job.slot.stream_id);
        recordFinalize(job.slot);
        _ = self.slots_finalized.fetchAdd(1, .acq_rel);
        // Encoder contexts are server-wide; release even on cancel/reset so
        // a stranded SSE cannot pin a pool slot past handler death.
        if (job.hctx.encoder) |enc| {
            enc.destroy();
            job.hctx.encoder = null;
            job.hctx.compressing = false;
        }
        const prev = job.slot.completion_owner.cmpxchgStrong(live, reported, .acq_rel, .acquire);
        if (prev == null) {
            self.completion_ch.trySend(job.stream_id) catch |err| switch (err) {
                // One completion per slot, capacity max_streams: full is a
                // broken invariant, never a wait.
                error.WouldBlock => @panic("completion channel over proven capacity"),
                error.Closed => {
                    std.debug.assert(shutdown_sweep.get() != null);
                },
            };
        }
        _ = self.live_handlers.fetchSub(1, .acq_rel);
        if (job.task_counted) {
            _ = self.live_task_handlers.fetchSub(1, .acq_rel);
            if (self.config.balancer) |b| b.noteHandlerEnd(self.config.exec_index.?);
        }
        if (comptime test_observe) _ = test_observed_live_handlers.fetchSub(1, .acq_rel);
        // Runs on whichever task finished the job. It frees nothing: the
        // request's memory (headers, body, whatever the handler allocated) is
        // in blocks of the request pool, and the actor returns them in
        // `releaseSlot`, after the join.
    }

    fn runHandlerJob(job: *HandlerJob) void {
        notePlacement(job.conn.actor_thread);
        probeNote(.handler_start);
        var marker: @TypeOf(handler_job_marker).Node = .unset;
        if (comptime test_observe) handler_job_marker.set(&marker, {});
        defer if (comptime test_observe) handler_job_marker.clear(&marker);
        defer finishHandlerJob(job);
        runHandlerJobBody(job);
    }

    // The pumps are thin on purpose and know nothing of the connection, so
    // the placement check lives in these wrappers and not in wire_pump.zig.
    fn runReadPump(self: *Connection, pump: *wire_pump.ReadPump) void {
        notePlacement(self.actor_thread);
        pump.run();
    }

    fn runWritePump(self: *Connection, pump: *wire_pump.WritePump) void {
        notePlacement(self.actor_thread);
        pump.run();
    }

    fn runHandlerJobBody(job: *HandlerJob) void {
        const self = job.conn;
        if (job.hctx.trace_on) job.hctx.t_handler = nowNs(self.config.io);
        if (job.slot.terminal.cancel_flag.load(.acquire)) return;
        if (job.slot.terminal.getCause() != null) return;
        switch (job.matched) {
            .found => |f| {
                const run_res: anyerror!void = switch (f.handler) {
                    .task => |h| h.runFn(h.ptr, &job.req, &job.resp),
                    .complete => |h| blk: {
                        var complete_resp: response.CompleteResponse = .{ .inner = &job.resp };
                        break :blk h.runFn(h.ptr, &job.req, &complete_resp);
                    },
                };
                run_res catch {
                    if (job.slot.terminal.getCause() != null) return;
                    if (!job.resp.committed) {
                        // The original handler error remains terminal if the
                        // fallback cannot write. A handler that failed because
                        // the request's memory ran out says which cap: its own
                        // is a 500, the connection's and the server's a 503.
                        const status: u16 = if (job.arena.refusal) |r| r.status() else 500;
                        job.resp.send(status, &.{}, "internal error") catch {};
                    } else if (!job.resp.finished) {
                        self.withSession(struct {
                            fn go(c: *Connection, at: DelayedReset) void {
                                if (diag_park) _ = diag_rst_handler.fetchAdd(1, .monotonic);
                                // A concurrently terminal stream needs no second reset.
                                if (!at.stillOwed()) return;
                                c.session.applyCommand(.{ .reset_stream = .{ .stream_id = at.stream_id, .code = .internal_error } }) catch {};
                                // Swallowing OOM here left RST-tombstoned DATA in
                                // the scheduler and the connection up — the TLS stall.
                                c.processIntents() catch c.failClosedOnIntentError();
                            }
                        }.go, DelayedReset{ .stream_id = job.stream_id, .terminal = &job.slot.terminal });
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

    /// Stage Session intents onto the scheduler without emitting.
    /// Task handlers leave drain to the actor so this turn can coalesce.
    /// Complete handlers leave drain to `runPendingInline` after the whole
    /// inline batch has encoded — per-job drainEmit is one TLS record per
    /// response (packing oracle exit 9).
    fn emitQueuedResponse(self: *Connection) anyerror!void {
        try self.materializeIntents();
    }

    fn stampSendWait(self: *Connection, send: SendTrace) void {
        defer self.trace_ticket.store(0, .release);
        const t_done = nowNs(self.config.io);
        const t_ack = self.trace_ack_ns.load(.acquire);
        const t_unlocked = send.unlocked;
        if (t_ack >= t_unlocked and t_done >= t_ack) {
            const t_emit = self.trace_emit_ns.load(.acquire);
            const t_queued = self.trace_queued_ns.load(.acquire);
            const t_written = self.trace_written_ns.load(.acquire);
            const block = t_acquiredGap(send.acquired, send.before);
            const hold = t_acquiredGap(t_unlocked, send.acquired);
            const ack = t_ack - t_unlocked;
            const res = t_done - t_ack;
            _ = trace.samples.fetchAdd(1, .monotonic);
            _ = trace.block_ns.fetchAdd(block, .monotonic);
            _ = trace.hold_ns.fetchAdd(hold, .monotonic);
            _ = trace.ack_ns.fetchAdd(ack, .monotonic);
            _ = trace.resume_ns.fetchAdd(res, .monotonic);
            _ = trace.hpack_ns.fetchAdd(send.hpack, .monotonic);
            if (t_emit >= t_unlocked and t_queued >= t_emit and t_ack >= t_queued) {
                _ = trace.ack_split_samples.fetchAdd(1, .monotonic);
                _ = trace.actor_ns.fetchAdd(t_emit - t_unlocked, .monotonic);
                _ = trace.queue_ns.fetchAdd(t_queued - t_emit, .monotonic);
                _ = trace.pump_ns.fetchAdd(t_ack - t_queued, .monotonic);
                if (t_written >= t_queued and t_ack >= t_written) {
                    _ = trace.pump_split_samples.fetchAdd(1, .monotonic);
                    _ = trace.write_ns.fetchAdd(t_written - t_queued, .monotonic);
                    _ = trace.drain_ns.fetchAdd(t_ack - t_written, .monotonic);
                }
            }
            trace.bumpMax(&trace.block_max, block);
            trace.bumpMax(&trace.hold_max, hold);
            trace.bumpMax(&trace.ack_max, ack);
            trace.bumpMax(&trace.resume_max, res);
            trace.bumpMax(&trace.hpack_max, send.hpack);
        }
    }

    fn t_acquiredGap(later: u64, earlier: u64) u64 {
        return if (later >= earlier) later - earlier else 0;
    }

    const InlineBudget = enum { run, shed, wait };

    /// Where a complete job stands in the budget queue. The admission
    /// decision is made under `budget_mu`, like a task writer's: the job joins
    /// the tail and the hand-over runs once, so with an empty queue and room
    /// the job is granted at once, and otherwise it waits its turn. Called by
    /// the actor only. `.wait` leaves the entry queued; the actor asks again
    /// on a later turn (a grant rings the doorbell).
    fn inlineBudget(self: *Connection, slot: *HandlerSlot, index: usize) InlineBudget {
        // One critical section: the state is read, the entry joins the tail
        // if it is idle, the hand-over runs once and the outcome is taken. A
        // grant made to this entry in this very call is taken here and rings
        // nothing (`grantBudgetLocked`).
        self.budget_mu.lockUncancelable();
        defer self.budget_mu.unlock();
        const fresh = slot.budget.state == .idle;
        if (fresh) {
            self.linkBudgetLocked(slot, index, self.config.limits.outbound_bytes_per_stream, true);
            self.grantBudgetLocked(@intCast(index));
        }
        return switch (takeBudgetOutcomeLocked(slot, null)) {
            .granted => .run,
            .refused => .shed,
            .queued => blk: {
                if (fresh) {
                    if (comptime test_observe) _ = test_inline_budget_gated.fetchAdd(1, .monotonic);
                }
                break :blk .wait;
            },
            .idle => unreachable,
        };
    }

    /// Complete handlers skip spawn. They are queued under `session_mu` and
    /// run here with the lock released. Encode one drain-turn, attach one
    /// receipt, finish every job of the batch, return to the actor loop. The
    /// actor consumes that receipt only after `drainWriteAcks` reports it
    /// ready — it does not `waitTicket` here. The jobs do not wait for it:
    /// see `PendingCompleteReceipt` for why their slots free now (t-3491).
    /// SSE and any handler that can wait are still spawned (`connPlacement`).
    ///
    /// The drain can park in `putOne` on a full h2c queue, a cancellation
    /// point. `error.Canceled` leaves as itself, after the batch below is
    /// finished exactly as on any other drain failure: each job once, the
    /// receipt slot released, the inline queue shifted. Every other drain error
    /// still goes to `failClosedOnIntentError`.
    fn runPendingInline(self: *Connection) error{Canceled}!void {
        // Do not assert `!session_held`: that flag means SOMEBODY holds the
        // mutex, often a task handler's sendCb, not this actor. Taking the
        // lock to discover an empty queue would convoy behind that handler
        // on every loop iteration.
        if (self.inline_n == 0) return;
        if (self.pending_complete_receipt_n == complete_receipt_capacity) {
            if (trace.enabled) {
                _ = trace.inline_full_n.fetchAdd(1, .monotonic);
                _ = trace.inline_full_sids.fetchAdd(self.inline_n, .monotonic);
                trace.bumpMax(&trace.inline_full_max, @intCast(self.inline_n));
            }
            return;
        }

        const queued_n: usize = blk: {
            self.lockSessionUncancelable(self.config.io);
            defer self.unlockSession(self.config.io);
            break :blk self.inline_n;
        };
        if (queued_n == 0) return;

        // Each complete response queues one HEADERS frame in the ordinary
        // control pool, and that entry stays held until WritePump acks the
        // write. Stop the batch when no entry is left, or the next HEADERS
        // fails its stream with WriteFailed. The rest stay queued — their
        // streams are still open for the peer — and run once acks free
        // entries. Slots used to bound this by accident: they were held until
        // the same ack (t-3491).
        var i: usize = 0;
        while (i < queued_n) : (i += 1) {
            const sid = self.inline_sids[i];
            const idx = self.slotIndex(sid) orelse continue;
            const job = &self.handler_jobs[idx];
            if (!self.sched.ctrl.hasOrdinaryEntry()) {
                if (trace.enabled) _ = trace.inline_ctrl_full_n.fetchAdd(1, .monotonic);
                if (comptime test_observe) _ = test_observed_inline_ctrl_full.fetchAdd(1, .monotonic);
                break;
            }
            // Budget gate. The actor must not park, so a complete handler
            // never waits for budget on its own task: it takes a place in the
            // budget queue, the same FIFO the parked task writers wait in
            // (`inlineBudget`), and the actor stops the batch at the first job
            // whose entry is not granted. A grant reserves one stream's cap
            // for the job (as pending, so its write only spends it) and rings
            // the doorbell; the actor runs the job on its next turn. A complete
            // body fits one stream's slab, so the cap is all the job can write.
            //
            // One queue is what keeps both properties: a job cannot take room
            // ahead of writers that arrived before it (a room check outside
            // the queue could, in the window between a release and its
            // hand-over), and it cannot wait for ever behind a stream of
            // writers that keep the queue non-empty (it is served in order).
            //
            // A stream that is already over (reset, connection ending) takes no
            // budget: the handler fails at its first check.
            job.hctx.inline_credit = 0;
            job.hctx.inline_shed = false;
            if (job.slot.terminal.getCause() != null) {
                // The entry may be queued, or granted by a release on the way
                // here (`handleWriterFailed` sets the causes and releases
                // pending DATA, and calls nothing that abandons it). Whatever
                // its state, it ends here, before the job runs.
                self.abandonInlineBudget(job.slot, idx);
            } else {
                switch (self.inlineBudget(job.slot, idx)) {
                    .run => job.hctx.inline_credit = self.config.limits.outbound_bytes_per_stream,
                    // The server cap is not waited for: no release on this
                    // connection makes room, so the job runs with no credit and
                    // its body write is refused (`inline_shed`), like a task
                    // handler's.
                    .shed => job.hctx.inline_shed = true,
                    .wait => break,
                }
            }
            if (comptime test_observe) _ = test_observed_inline_jobs.fetchAdd(1, .monotonic);
            job.hctx.runs_inline = true;
            runHandlerJobBody(job);
            // Give back what the job did not write.
            const unused = job.hctx.inline_credit;
            job.hctx.inline_credit = 0;
            self.applyOutboundRelease(unused, .pending);
        }
        const batch_n = i;
        if (batch_n == 0) return;

        var receipt_ticket: u64 = 0;
        var receipt_slot: u32 = ticket_table.no_completion_slot;
        var receipt_attached = false;
        var wait_job: ?*HandlerJob = null;
        var reservation_failed = false;
        // Set when the drain was canceled; returned once the batch is finished.
        var canceled = false;
        var receipt_trace: ?SendTrace = null;
        {
            self.lockSessionUncancelable(self.config.io);
            defer self.unlockSession(self.config.io);

            i = 0;
            while (i < batch_n) : (i += 1) {
                const sid = self.inline_sids[i];
                const job = if (self.slotIndex(sid)) |idx| &self.handler_jobs[idx] else continue;
                if (job.hctx.deferred_ticket != 0 and wait_job == null) {
                    wait_job = job;
                }
            }

            if (wait_job != null) {
                if (self.reserveTicket()) |pair| {
                    receipt_ticket = pair[0];
                    receipt_slot = pair[1];
                    i = 0;
                    while (i < batch_n) : (i += 1) {
                        const sid = self.inline_sids[i];
                        const job = if (self.slotIndex(sid)) |idx| &self.handler_jobs[idx] else continue;
                        const t = job.hctx.send_trace orelse continue;
                        if (self.trace_ticket.cmpxchgStrong(0, pair[0], .acq_rel, .monotonic) == null) {
                            self.trace_ack_ns.store(0, .release);
                            self.trace_emit_ns.store(0, .release);
                            self.trace_queued_ns.store(0, .release);
                            self.trace_written_ns.store(0, .release);
                            receipt_trace = t;
                        } else {
                            _ = trace.skipped.fetchAdd(1, .monotonic);
                        }
                    }
                } else |_| {
                    // The complete response is already encoded into Session
                    // intents. Emitting it without a receipt would skip the
                    // lifecycle boundary, so fail this connection closed.
                    reservation_failed = true;
                    self.noteServerClose("inline_fail", null);
                    self.writer_failed.store(true, .release);
                    self.handleWriterFailed();
                }
            }

            if (!reservation_failed) {
                self.processIntentsWithReceipt(receipt_ticket, receipt_slot, &receipt_attached) catch |err| {
                    if (err == error.Canceled) {
                        if (comptime test_observe) _ = test_inline_drain_canceled.fetchAdd(1, .acq_rel);
                        canceled = true;
                    } else {
                        self.failClosedOnIntentError();
                    }
                };
            }
        }

        if (receipt_ticket != 0 and receipt_attached) {
            self.pending_complete_receipts[self.pending_complete_receipt_n] = .{
                .ticket = receipt_ticket,
                .ticket_slot = receipt_slot,
                .send_trace = receipt_trace,
            };
            const old_n = self.pending_complete_receipt_n;
            self.pending_complete_receipt_n += 1;
            if (trace.enabled) {
                trace.bumpMax(
                    &trace.complete_receipt_depth_max,
                    @intCast(self.pending_complete_receipt_n),
                );
                self.noteReceiptDepthChange(old_n, self.pending_complete_receipt_n);
            }
        } else if (receipt_ticket != 0) {
            self.tickets.releaseReserved(receipt_slot);
            if (receipt_trace != null) self.trace_ticket.store(0, .release);
        }

        i = 0;
        while (i < batch_n) : (i += 1) {
            const sid = self.inline_sids[i];
            const job = if (self.slotIndex(sid)) |idx| &self.handler_jobs[idx] else continue;
            finishHandlerJob(job);
        }
        _ = self.drainCompletions();

        {
            self.lockSessionUncancelable(self.config.io);
            defer self.unlockSession(self.config.io);
            std.debug.assert(self.inline_n >= batch_n);
            const rest = self.inline_n - batch_n;
            if (rest != 0) {
                std.mem.copyForwards(u31, self.inline_sids[0..rest], self.inline_sids[batch_n..self.inline_n]);
            }
            self.inline_n = rest;
        }
        if (canceled) return error.Canceled;
    }

    /// A handler's reset of its own stream after a failure, which reaches
    /// `session_mu` late: the stream may have ended meanwhile (the peer reset
    /// it, the slow-consumer sweep did, the connection is closing), and its
    /// tombstone may since have been evicted, so Session's state alone cannot
    /// say whether the peer still counts it open (`Session.rstOwed` refuses an
    /// absent id). The slot's terminal cause, read under the lock, can: any
    /// cause means the stream already ended or is ending.
    const DelayedReset = struct {
        stream_id: u31,
        terminal: *const response.SlotTerminal,

        fn stillOwed(self: DelayedReset) bool {
            return self.terminal.getCause() == null;
        }
    };

    fn withSession(self: *Connection, comptime f: anytype, arg: anytype) void {
        self.lockSession() catch return;
        defer self.unlockSession(self.config.io);
        f(self, arg);
    }

    fn lockSession(self: *Connection) response.ResponseError!void {
        assertNotInShutdownSweep();
        self.session_mu.lock(self.config.io) catch return error.Canceled;
        self.session_held = true;
    }

    /// Every acquisition and release of `session_mu` goes through `lockSession`,
    /// this and `unlockSession`, so `session_held` cannot drift from the mutex
    /// and every release by a task that is not the actor rings it. Do not call
    /// the mutex directly: `./zb build session-mu-gate` fails the build.
    fn lockSessionUncancelable(self: *Connection, io: std.Io) void {
        assertNotInShutdownSweep();
        if (diag_park) {
            self.actor_site.store(3, .release);
            self.session_mu.lockUncancelable(io);
            self.actor_site.store(1, .release);
        } else {
            self.session_mu.lockUncancelable(io);
        }
        self.session_held = true;
    }

    /// Axis B. `shutdown_sweep.get` is this task only (`TaskLocal.get` walks
    /// `task.tls_head`). A handler reacquire is a different task and is unbound.
    fn assertNotInShutdownSweep() void {
        if (!std.debug.runtime_safety) return;
        const bound = shutdown_sweep.get();
        if (bound != null) {
            std.debug.panic("session lock acquired during shutdownHandlers", .{});
        }
        std.debug.assert(bound == null);
    }

    /// First act of every finalizer. A second call panics before the arena reset.
    fn recordFinalize(slot: *HandlerSlot) void {
        std.debug.assert(slot.in_use);
        const prev = slot.finalize_count.fetchAdd(1, .acq_rel);
        if (std.debug.runtime_safety and prev != 0) {
            std.debug.panic("handler slot finalized {d} times", .{prev +% 1});
        }
        std.debug.assert(slot.finalize_count.load(.acquire) == 1);
    }

    /// The release of `session_mu` is how a handler's task tells the actor that
    /// shared state changed. Every handler-side change to what the actor acts on
    /// (a queued frame, a pushed stash chunk, a failure published by
    /// `handleWriterFailed`, a flag) is made under this lock, so a task that is
    /// not the actor rings the doorbell as the LAST act of its release. A new
    /// handler path cannot forget the wake: it cannot change that state without
    /// releasing the lock.
    ///
    /// - AFTER the unlock. A ring before it could wake the actor into a park on
    ///   the mutex that the handler still holds.
    /// - Not for the actor itself. It drains every input in the same turn, and a
    ///   ring would only cost it a spare turn.
    /// - Also on the unlocks inside the wait loops (`waitForStreamSpace`,
    ///   `parkForBudget`, `waitForAckRoom`). That is by design: `enqueuePending`
    ///   copies piece k, then parks in `waitForStreamSpace` for piece k + 1, and
    ///   only that unlock can tell the actor about piece k.
    /// - The ring coalesces. A full doorbell already means a wake is pending,
    ///   so a burst of releases costs one token.
    ///
    /// Not covered by this ring, so those sites keep their own: the actor
    /// ringing itself for work it made after its turn's check (`tryDispatch`'s
    /// `inline_n`, the inline entry in `grantBudgetLocked`), and changes made
    /// with no `session_mu` at all (wire releases on the actor).
    fn unlockSession(self: *Connection, io: std.Io) void {
        // Every lock section materializes what it queued before it lets go,
        // so the next one starts its batch from an empty queue: the intent
        // bound (`Limits.intentEntriesMin`) counts one batch at a time.
        // (From the first materialization on: until then the preface that
        // `Session.init` queued waits for the boot flush.)
        if (std.debug.runtime_safety and self.intents_materialized and self.session.intentLen() != 0) {
            std.debug.panic("the session lock was released with {d} intents queued", .{self.session.intentLen()});
        }
        // Cleared while the mutex is still held, or the next holder would see a
        // stale false.
        self.session_held = false;
        self.session_mu.unlock(io);
        if (actor_marker.get() != self) {
            // Two counters, because one cannot be right (`lostWakeSince`).
            const seq = if (comptime test_observe) self.releaseStarted() else 0;
            if (comptime test_observe) {
                if (test_release_hook) |h| h(self, .before_ring);
            }
            self.ring();
            if (comptime test_observe) {
                if (test_release_hook) |h| h(self, .after_ring);
                self.releaseRung(seq);
            }
        }
    }

    /// Observe builds. A release by a task that is not the actor takes its own
    /// number here, after its unlock and before its ring.
    fn releaseStarted(self: *Connection) usize {
        return self.releases_started.fetchAdd(1, .seq_cst) + 1;
    }

    /// Observe builds. The same release, after its ring: publish its number. The
    /// maximum, not a count, so a release that is slow to get here cannot hide
    /// another one that did not ring (`lostWakeSince`).
    fn releaseRung(self: *Connection, seq: usize) void {
        _ = self.releases_rung_max.fetchMax(seq, .seq_cst);
    }

    /// Observe builds. True when a release by a task that is not the actor
    /// STARTED after `snapshot` (a value of `releases_started` that the actor
    /// read), has finished its ring, and the doorbell is empty. That is a lost
    /// wake.
    ///
    /// Why it is sound. On the paths this is called from, the actor takes a
    /// token only in a `.bell` arm, and that is before the top of its turn,
    /// where it reads `releases_started` as `snapshot`. (`waitTicketDraining`
    /// also has a `.bell` arm and takes tokens, but it cannot reach a checked
    /// park: it runs its select only for a ticket that is still dark. The
    /// ordinary path calls it for a ticket that is already signaled, so the loop
    /// body does not run. A dark ticket gets there only through `finalize_all`,
    /// which is teardown, or `complete_receipts_fail_all`, which is set together
    /// with `writer_failed`; the turn that sees `writer_failed` leaves the loop
    /// for teardown in its lock block, before either check.)
    ///
    /// Each release takes its own number from `releases_started` (a `fetchAdd`
    /// after its unlock), and after its ring it publishes that number with
    /// `fetchMax` into `releases_rung_max`. Numbers 1 to `snapshot` were taken
    /// before the actor read the snapshot, so those releases unlocked before it.
    /// `releases_rung_max > snapshot` means a release whose number is above the
    /// snapshot, so one that started AFTER it, has finished its ring. Its ring
    /// came after the actor's last take, so its token is still there, and the
    /// doorbell is empty only if some ring never happened. A release that started
    /// before the snapshot did its unlock before it, so the turn that follows
    /// reads its state under the lock, with or without a token.
    ///
    /// Why a maximum and not a count. Release A (number 1) rings and is slow to
    /// publish; the actor takes A's token and snapshots 1; release B (number 2)
    /// skips its ring and publishes. A count of published releases is 1, not
    /// above the snapshot, and A's late publish would only make it 2 after the
    /// fact. The maximum is 2 at once.
    ///
    /// Why two counters. `trySend` can hand the token straight to the parked
    /// actor before the handler's next instruction. With one counter bumped
    /// AFTER the ring, the actor takes `.bell`, snapshots the old count, the
    /// bump lands, and the check sees "count moved, doorbell empty" on a legal
    /// schedule. With one counter bumped BEFORE the ring, the actor can look
    /// between the bump and the ring. `releases_started` is read by the actor
    /// and moves before the ring; `releases_rung_max` is compared and moves
    /// after.
    fn lostWakeSince(self: *Connection, snapshot: usize) bool {
        if (comptime !test_observe) return false;
        if (self.releases_rung_max.load(.seq_cst) <= snapshot) return false;
        const empty = io_queue.chanLen(u8, &self.doorbell) == 0;
        return empty;
    }

    /// Observe builds. Called just before the actor parks in `waitForActivity`.
    /// The actor may sleep only if every release of `session_mu` by another task
    /// since the top of its turn has left a token (`lostWakeSince`). It panics at
    /// the park, which names the bug, and a hang would not.
    ///
    /// Not checked: whether a queued complete job (`inline_n`) has a token. The
    /// states that hold one back on purpose have their own wakes, and the check
    /// panicked on a legal TLS schedule (`runPendingInline` runs before
    /// `driveTlsTurn`, and the send completion that frees a control entry is the
    /// next wake). The two rings that guard that case have their own unit tests.
    fn assertNoLostWake(self: *Connection, comptime where: []const u8) void {
        if (comptime !test_observe) return;
        if (self.parkCheckFailed()) {
            std.debug.panic("actor parks in {s} with no wake token waiting after a handler released the session lock since its turn began (a lost wake)", .{where});
        }
    }

    /// The check, as a value. The hook runs after it and before the select: a
    /// release there is the schedule that a snapshot taken at the park would have
    /// absorbed (round 2 loaded `releases_at_park` here), so the wake audit
    /// compares against the turn-top snapshot instead.
    fn parkCheckFailed(self: *Connection) bool {
        if (comptime !test_observe) return false;
        if (self.lostWakeSince(self.releases_at_turn_top)) return true;
        if (test_release_hook) |h| h(self, .park_after_check);
        return false;
    }

    /// The wake audit, as a value. The turn-top snapshot, not one taken at the
    /// park: no token is taken between the top of the turn and the select on
    /// the audited path, so every release that started after the top and rang
    /// leaves a token, whenever in the turn it started.
    fn wakeAuditFailed(self: *Connection) bool {
        return self.lostWakeSince(self.releases_at_turn_top);
    }

    /// Observe builds. Called when the actor wakes from a park by anything but
    /// the doorbell. `assertNoLostWake` cannot catch a wake that was lost while
    /// the actor was already asleep: it runs before the park, and the release
    /// came after. This runs at the first wake that does come (a timer, a
    /// completion, the shutdown event at the end of a test), and names the
    /// cause instead of leaving a deadline to fail. Same argument as
    /// `lostWakeSince`, with the turn-top snapshot (`wakeAuditFailed`): the
    /// doorbell has one consumer, this task, and the wake was not a `.bell` arm.
    fn auditWake(self: *Connection, comptime where: []const u8) void {
        if (comptime !test_observe) return;
        if (self.wakeAuditFailed()) {
            std.debug.panic("actor woke in {s} by something other than the bell after a handler released the session lock while it slept, and no wake token is waiting (a lost wake)", .{where});
        }
    }

    /// Called where Session or the scheduler is about to be read or mutated.
    fn assertSessionHeld(self: *const Connection, comptime site: []const u8) void {
        if (std.debug.runtime_safety and !self.session_held) {
            std.debug.panic("session lock not held at {s}", .{site});
        }
    }

    /// One-shot response: headers plus a complete body.
    ///
    /// Task handlers: reserve a ticket before the lock, encode, release the
    /// lock, then wait. Complete handlers skip the per-response ticket;
    /// `runPendingInline` publishes one drain-turn receipt after the packed
    /// write, and the actor consumes it only after `drainWriteAcks` reports it
    /// ready. A wait inside `session_mu` is a deadlock: the actor completes the
    /// ticket only after dropping the mutex to apply write acks, and it needs
    /// the mutex to produce bytes.
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

    /// The response's header list, in the request's arena: the handler's
    /// headers, and for SSE, compression and Vary the fields the edge adds.
    /// Nothing here is the general allocator, and nothing is freed one by one:
    /// the arena goes home with the request. A failure is `ResponseRefused`,
    /// because nothing has been queued yet and the stream can still be
    /// answered (the arena says which cap it hit, so the answer is 500 or 503).
    fn appendResponseHeaders(
        arena: std.mem.Allocator,
        hlist: *std.ArrayList(hpack.HeaderField),
        headers: []const request.Header,
        opts: struct {
            sse: bool = false,
            compress: bool = false,
            add_vary: bool = false,
            strip_content_length: bool = false,
        },
    ) response.ResponseError!void {
        // The three SSE fields, one for compression, one for Vary.
        hlist.ensureTotalCapacityPrecise(arena, headers.len + 5) catch return error.ResponseRefused;
        if (opts.sse) {
            hlist.appendAssumeCapacity(.{ .name = "content-type", .value = "text/event-stream" });
            hlist.appendAssumeCapacity(.{ .name = "cache-control", .value = "no-cache" });
            hlist.appendAssumeCapacity(.{ .name = "x-accel-buffering", .value = "no" });
        }
        var handler_vary: ?[]const u8 = null;
        for (headers) |h| {
            if (opts.strip_content_length and std.ascii.eqlIgnoreCase(h.name, "content-length")) continue;
            if (opts.sse and std.ascii.eqlIgnoreCase(h.name, "content-type")) continue;
            if (std.ascii.eqlIgnoreCase(h.name, "vary")) {
                handler_vary = h.value;
                continue;
            }
            hlist.appendAssumeCapacity(.{ .name = h.name, .value = h.value });
        }
        if (opts.compress) {
            hlist.appendAssumeCapacity(.{ .name = "content-encoding", .value = "br" });
        }
        if (opts.add_vary) {
            const merged = content_coding.mergeVaryAcceptEncoding(arena, handler_vary) catch return error.ResponseRefused;
            hlist.appendAssumeCapacity(.{ .name = "vary", .value = merged });
        } else if (handler_vary) |v| {
            hlist.appendAssumeCapacity(.{ .name = "vary", .value = v });
        }
        _ = h2_work.response_header_lists.fetchAdd(1, .monotonic);
    }

    /// `stack` serves the common response (the handler's headers and nothing the
    /// edge adds); anything else is built in the arena. Either way the caller
    /// frees nothing.
    fn prepareResponseHeaderFields(
        arena: std.mem.Allocator,
        stack: []hpack.HeaderField,
        headers: []const request.Header,
        compress: bool,
        add_vary: bool,
        strip_content_length: bool,
        sse: bool,
    ) response.ResponseError![]const hpack.HeaderField {
        // A list the stack cannot hold (the handler's headers and a `vary`) is
        // built in the arena like the edge-added kinds; the stack is the
        // small-list fast path and is no bound. The bound is
        // `Limits.response_header_fields`, checked in `Session.cmdHeaders`.
        if (sse or compress or add_vary or headers.len >= stack.len) {
            var hlist: std.ArrayList(hpack.HeaderField) = .empty;
            try appendResponseHeaders(arena, &hlist, headers, .{
                .sse = sse,
                .compress = compress,
                .add_vary = add_vary,
                .strip_content_length = strip_content_length,
            });
            return hlist.items;
        }
        var handler_vary: ?[]const u8 = null;
        var n: usize = 0;
        for (headers) |h| {
            if (strip_content_length and std.ascii.eqlIgnoreCase(h.name, "content-length")) continue;
            if (std.ascii.eqlIgnoreCase(h.name, "vary")) {
                handler_vary = h.value;
                continue;
            }
            // `headers.len < stack.len` above: the stack holds them and one more.
            stack[n] = .{ .name = h.name, .value = h.value };
            n += 1;
        }
        if (handler_vary) |v| {
            stack[n] = .{ .name = "vary", .value = v };
            n += 1;
        }
        _ = h2_work.response_header_lists.fetchAdd(1, .monotonic);
        return stack[0..n];
    }

    /// What `respond_headers` failing means to the handler. `HeadersTooLarge`
    /// and `OutOfMemory` leave nothing queued (`Session.cmdHeaders` is all or
    /// none), so the response is refused and the stream can still be answered.
    fn headersRefusal(err: anyerror) response.ResponseError {
        return switch (err) {
            error.HeadersTooLarge, error.OutOfMemory => error.ResponseRefused,
            else => error.WriteFailed,
        };
    }

    /// Give back the brotli encoder `prepareCompression` took, for a response
    /// that was refused before it started.
    fn dropEncoder(hctx: *HandlerCtx) void {
        if (hctx.encoder) |enc| {
            enc.destroy();
            hctx.encoder = null;
            hctx.compressing = false;
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
                fn go(c: *Connection, at: DelayedReset) void {
                    if (diag_park) _ = diag_rst_compress.fetchAdd(1, .monotonic);
                    if (!at.stillOwed()) return;
                    c.session.applyCommand(.{ .reset_stream = .{ .stream_id = at.stream_id, .code = .internal_error } }) catch {};
                    c.processIntents() catch c.failClosedOnIntentError();
                }
            }.go, DelayedReset{ .stream_id = stream_id, .terminal = hctx.terminal });
            return error.WriteFailed;
        };
    }

    /// Recheck after taking `session_mu`. The pre-lock `getCause()` is stale:
    /// the actor can RST and `cancelHandler` between that load and this
    /// acquisition, and `cmdHeaders` used to encode HEADERS for a missing
    /// stream anyway. Enqueueing DATA after a RST tombstone is the TLS stall.
    fn refuseIfStreamDead(
        self: *Connection,
        stream_id: u31,
        terminal: *response.SlotTerminal,
    ) response.ResponseError!void {
        self.assertSessionHeld("refuseIfStreamDead");
        if (terminal.getCause()) |c| return response.causeToError(c);
        if (self.writer_failed.load(.acquire)) return error.WriteFailed;
        // A live entry means the id was never tombstoned (releaseConcurrency
        // tombstones and removes together), so the tombstone scan is only
        // needed once the stream is gone.
        if (self.session.streams.contains(stream_id)) return;
        if (self.session.tombstoneCode(stream_id)) |code| {
            if (code != .no_error) return error.PeerReset;
        }
        return error.WriteFailed;
    }

    /// Hand the streams whose send window grew to the scheduler before it
    /// drains. Runs before BOTH drain entry points; a drain without it leaves a
    /// credited stream on the blocked list until the slow-consumer sweep
    /// resets it. The journal is cleared only here, so a turn that skips the
    /// drain keeps it for the next one.
    fn rearmCredited(self: *Connection) void {
        self.assertSessionHeld("rearmCredited");
        const credit = self.session.takeSendCredit();
        self.sched.rearm(credit[0], credit[1]);
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

        var ticket: u64 = 0;
        var slot_i: u32 = 0;
        var armed = false;
        if (!hctx.runs_inline) {
            const ticket_pair = self.reserveTicket() catch |err| return err;
            ticket = ticket_pair[0];
            slot_i = ticket_pair[1];
            armed = true;
        }
        defer if (armed) self.tickets.releaseReserved(slot_i);

        // One-shot responses use the same ticket-correlated phase trace as
        // streaming writes. Without this arm the one-shot benchmark reports
        // zero samples and the instrument silently measures only SSE.
        // A dispatch-sampled job also traces spawn/to_send on this same request.
        const lifecycle = hctx.trace_on;
        var traced = lifecycle or (trace.enabled and
            (trace.writes.fetchAdd(1, .monotonic) % trace.sample_every == 0));
        if (lifecycle) _ = trace.writes.fetchAdd(1, .monotonic);
        if (traced and !hctx.runs_inline) {
            traced = self.trace_ticket.cmpxchgStrong(0, ticket, .acq_rel, .monotonic) == null;
            if (!traced) _ = trace.skipped.fetchAdd(1, .monotonic);
        }
        var defer_wait = false;
        defer if (traced and !defer_wait and !hctx.runs_inline) self.trace_ticket.store(0, .release);
        var t_before: u64 = 0;
        var t_acquired: u64 = 0;
        var t_hpack: u64 = 0;
        var t_unlocked: u64 = 0;
        if (traced or lifecycle) {
            if (traced and !hctx.runs_inline) {
                self.trace_ack_ns.store(0, .release);
                self.trace_emit_ns.store(0, .release);
                self.trace_queued_ns.store(0, .release);
                self.trace_written_ns.store(0, .release);
            }
            t_before = nowNs(self.config.io);
        }
        if (lifecycle and hctx.t_handler >= hctx.t_dispatch and t_before >= hctx.t_handler) {
            const spawn = hctx.t_handler - hctx.t_dispatch;
            const to_send = t_before - hctx.t_handler;
            _ = trace.lifecycle_samples.fetchAdd(1, .monotonic);
            _ = trace.spawn_ns.fetchAdd(spawn, .monotonic);
            _ = trace.to_send_ns.fetchAdd(to_send, .monotonic);
            trace.bumpMax(&trace.spawn_max, spawn);
            trace.bumpMax(&trace.to_send_max, to_send);
        }
        {
            try self.lockSession();
            if (traced) t_acquired = nowNs(self.config.io);
            defer self.unlockSession(self.config.io);
            try self.refuseIfStreamDead(stream_id, hctx.terminal);
            var stack_hpack: [32]hpack.HeaderField = undefined;
            const fields = try prepareResponseHeaderFields(hctx.arena, &stack_hpack, headers, compress, add_vary, compress, false);
            // Compressed bodies never carry content-length — HTTP/2 length is framing.
            const t_hpack0 = if (traced) nowNs(self.config.io) else 0;
            // With a body, the ticket goes with the last DATA
            // (`enqueuePending`); without one, with these HEADERS.
            self.session.applyCommand(.{ .respond_headers = .{
                .stream_id = stream_id,
                .status = status,
                .headers = fields,
                .end_stream = body_out.len == 0,
                .ticket = if (body_out.len == 0) .{ .id = ticket, .slot = slot_i } else .none,
            } }) catch |err| return headersRefusal(err);
            if (traced) t_hpack = nowNs(self.config.io) - t_hpack0;
            if (body_out.len > 0) {
                const pending_len = self.sched.pendingByteLen(stream_id);
                const fits_without_wait = pending_len < self.config.limits.outbound_bytes_per_stream and
                    body_out.len <= self.config.limits.outbound_bytes_per_stream - pending_len;
                if (hctx.runs_inline or fits_without_wait) {
                    // Place HEADERS in the ordinary control queue, then stage
                    // DATA (the scheduler holds DATA until its HEADERS left,
                    // t-482). Drain is left to the actor (task) or to
                    // `runPendingInline` after the whole complete batch
                    // encodes. Complete handlers carry no per-response ticket:
                    // the drain-turn receipt is reserved after this encode
                    // loop. HEADERS first, because `enqueuePending` may
                    // release `session_mu` (stream space, connection budget),
                    // and no lock section may end with intents queued
                    // (`unlockSession`).
                    self.emitQueuedResponse() catch {
                        self.failClosedOnIntentError();
                        return error.WriteFailed;
                    };
                    self.enqueuePending(stream_id, body_out, true, ticket, slot_i, hctx) catch |err| return err;
                } else {
                    // A body that can fill the stream slab may release this
                    // lock while waiting for capacity. Materialize HEADERS
                    // first so the actor cannot emit DATA ahead of them (t-482).
                    self.processIntents() catch {
                        self.failClosedOnIntentError();
                        return error.WriteFailed;
                    };
                    self.enqueuePending(stream_id, body_out, true, ticket, slot_i, hctx) catch |err| return err;
                    self.processIntents() catch {
                        self.failClosedOnIntentError();
                        return error.WriteFailed;
                    };
                }
            } else {
                if (hctx.runs_inline) {
                    self.emitQueuedResponse() catch {
                        self.failClosedOnIntentError();
                        return error.WriteFailed;
                    };
                } else {
                    self.processIntents() catch {
                        self.failClosedOnIntentError();
                        return error.WriteFailed;
                    };
                }
            }
        }
        if (traced) t_unlocked = nowNs(self.config.io);
        armed = false;
        if (hctx.runs_inline) {
            // Encode now; the actor consumes the drain-turn receipt after applying the write ack.
            defer_wait = true;
            hctx.deferred_ticket = 1;
            if (traced) hctx.send_trace = .{
                .before = t_before,
                .acquired = t_acquired,
                .unlocked = t_unlocked,
                .hpack = t_hpack,
            };
            return;
        }
        // From here until the wait returns, teardown must NOT cancel this
        // handler. The wait is guaranteed to wake on its own — see
        // `HandlerSlot.awaiting_receipt` and `cancelHandler`.
        hctx.slot.awaiting_receipt.store(true, .release);
        defer hctx.slot.awaiting_receipt.store(false, .release);
        try self.waitTicket(stream_id, slot_i, hctx.terminal);
        if (traced) {
            self.stampSendWait(.{
                .before = t_before,
                .acquired = t_acquired,
                .unlocked = t_unlocked,
                .hpack = t_hpack,
            });
        }
    }

    /// Streaming response: headers now, body later. The SSE path adds the three
    /// fields that keep an event stream unbuffered end to end —
    /// `text/event-stream`, `no-cache`, and `x-accel-buffering: no` for
    /// intermediaries that would otherwise hold events until a buffer fills.
    ///
    /// The HEADERS frame is built inside `Session`, so the ticket goes in the
    /// `respond_headers` command and travels on that frame's intent
    /// (`OutboundFrame.ticket`): it can only complete, or fail, with this
    /// response's HEADERS.
    fn startCb(ctx: *anyopaque, stream_id: u31, status: u16, headers: []const request.Header, sse: bool) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        if (hctx.terminal.getCause()) |c| return response.causeToError(c);
        if (hctx.terminal.cancel_flag.load(.acquire)) return error.Canceled;
        const prep = self.prepareCompression(hctx, status, headers, null, sse);
        diagWait(hctx, .start_reserve_ticket);
        defer diagWait(hctx, .running);
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
            diagWait(hctx, .start_lock);
            try self.lockSession();
            diagWait(hctx, .running);
            defer self.unlockSession(self.config.io);
            try self.refuseIfStreamDead(stream_id, hctx.terminal);
            var hlist: std.ArrayList(hpack.HeaderField) = .empty;
            appendResponseHeaders(hctx.arena, &hlist, headers, .{
                .sse = sse,
                .compress = prep.compress,
                .add_vary = prep.add_vary,
                .strip_content_length = true, // streaming never sends content-length
            }) catch |err| {
                dropEncoder(hctx);
                return err;
            };
            self.session.applyCommand(.{ .respond_headers = .{
                .stream_id = stream_id,
                .status = status,
                .headers = hlist.items,
                .end_stream = false,
                .ticket = .{ .id = ticket, .slot = slot_i },
            } }) catch |err| {
                const refused = headersRefusal(err);
                if (refused == error.ResponseRefused) dropEncoder(hctx);
                return refused;
            };
            // Enqueue and leave. Do NOT drain. writeCb already learned this:
            // 200 task handlers each processIntents/drainEmit is one HEADERS
            // chunk apiece into carried+tls_write_ch (cap 8). The tenth
            // trySend is WriteFailed and fail-closes the connection. The actor
            // packs those HEADERS in one drainEmit (emit_batch) and the
            // stash-full pause_control bound keeps a leftover processIntents
            // path from overflowing.
            self.emitQueuedResponse() catch {
                self.failClosedOnIntentError();
                return error.WriteFailed;
            };
            // The actor is woken by the release of `session_mu` at the end of
            // this block (`unlockSession`).
        }
        hctx.slot.awaiting_receipt.store(true, .release);
        defer hctx.slot.awaiting_receipt.store(false, .release);
        diagWait(hctx, .start_wait_ticket);
        try self.waitTicket(stream_id, slot_i, hctx.terminal);
    }

    /// Streaming body cadence wait (`Body.waitUntil`), handler-owned.
    ///
    /// One per-slot Event raced against a timer: the timer winning IS the
    /// deadline. zio arms it on the waiter's home executor, so it fires when that
    /// executor next polls. This is a cooperative scheduler: the actor must
    /// reach a suspension point for the timer to fire, and it does, because
    /// every turn parks in `zio.select`. `T1` in `tests/deadlines.zig` is the
    /// gate — a 20 ms deadline while the client floods one-shots on ONE
    /// executor with migration off.
    ///
    /// Do NOT move timers to a thread of their own to make a non-yielding
    /// actor work. That was tried, upstream refused it twice, and it adds
    /// latency on hundreds of concurrent SSE streams at the same delivered
    /// count and the same CPU per event. The mechanism:
    /// such a patch does not only ARM the timer elsewhere, it runs the woken
    /// waiter there too (`wakeFromTimer` marks the wake, and `scheduleTask`
    /// then forces local scheduling on the timer executor). Every expired
    /// deadline therefore runs on ONE thread and then contends for
    /// `session_mu` in `writeCb`. That one thread saturates at some stream
    /// count, so the penalty is a knee rather than a slope.
    ///
    /// The timer is armed at an absolute deadline on the awake clock, the
    /// clock `deadline` is on, never as a duration: a duration is added to
    /// the loop's cached `now`, which can be older than ours. With
    /// `Limits.handler_deadline_slack_ns` the armed deadline is rounded up
    /// to a multiple of the slack, so streams that tick on their own timers
    /// share timer passes. The early return below compares against the
    /// caller's own deadline, so a handler that is already late is never
    /// held back to the next multiple.
    ///
    /// A normal return is an early wake from
    /// `wakeHandlerDeadline` (RST, terminal, writer failure, teardown); the
    /// loop then rechecks the cause. The actor plays no role in handler
    /// timing: no heap, no fire-due turn work, no park-timer contribution.
    ///
    /// Ordering discipline with the waker: the waker sets the terminal cause
    /// (or writer_failed) BEFORE it sets the event; the waiter resets the
    /// event BEFORE it rechecks those conditions. A wake that races the
    /// reset therefore always leaves its evidence visible to the recheck.
    fn waitUntilCb(ctx: *anyopaque, stream_id: u31, deadline: std.Io.Timestamp) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        const io = self.config.io;
        const deadline_ns: u64 = @intCast(deadline.nanoseconds);
        const armed_ns = self.config.limits.handlerDeadlineNs(deadline_ns);

        if (comptime test_observe) _ = test_deadline_waits.fetchAdd(1, .acq_rel);
        defer if (comptime test_observe) {
            _ = test_deadline_waits.fetchSub(1, .acq_rel);
        };

        while (true) {
            const slot_i = self.slotIndex(stream_id) orelse return error.Canceled;
            const event = &self.deadline_events[slot_i];
            event.reset();
            if (hctx.terminal.getCause()) |c| return response.causeToError(c);
            if (hctx.terminal.cancel_flag.load(.acquire)) return error.Canceled;
            if (self.writer_failed.load(.acquire)) return error.WriteFailed;
            if (nowNs(io) >= deadline_ns) return;
            diagWait(hctx, .deadline_wait);
            defer diagWait(hctx, .running);
            const winner = zio.select(.{
                .ev = event,
                .dead = &self.dead,
                .timer = zio.Timeout{ .deadline = .fromNanoseconds(armed_ns) },
            }) catch |err| switch (err) {
                error.Canceled => {
                    if (hctx.terminal.getCause()) |c| return response.causeToError(c);
                    return error.Canceled;
                },
            };
            switch (winner) {
                .ev => {},
                .dead => {
                    if (hctx.terminal.getCause()) |c| return response.causeToError(c);
                    return error.Canceled;
                },
                .timer => return,
            }
            // Early wake: loop, recheck cause, re-arm the remaining time.
        }
    }

    /// A ticket is taken only when the caller needs a wire receipt — today that
    /// is end-of-stream. A plain write returns as soon as the bytes are queued,
    /// so a handler that writes many small chunks pays one round trip per flush
    /// and not one per chunk. Backpressure still applies through
    /// `enqueuePending`, so this is not an unbounded fire-and-forget.
    /// `flush` means EMIT NOW (the compressor is told to flush and the bytes are
    /// queued). `wait` means APPLY BACKPRESSURE: a non-end wait parks while this
    /// stream still has pending DATA, then returns once the bytes are queued.
    /// `end` still reserves a ticket and waits for the wire. They remain
    /// separate so callers can choose either contract; `Body.flush()` still
    /// waits for a receipt via `flushCb`.
    fn writeCb(ctx: *anyopaque, stream_id: u31, bytes: []const u8, end: bool, flush: bool, wait: bool) response.ResponseError!void {
        const hctx: *HandlerCtx = @ptrCast(@alignCast(ctx));
        const self = hctx.conn;
        // `CompleteResponse` has only `send`. A streaming write from an inline
        // handler would wait on a ticket that only the actor (busy running it)
        // can complete.
        std.debug.assert(!hctx.runs_inline);
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
            // A non-final zero-byte receipt becomes a write-queue barrier below:
            // the DATA scheduler cannot emit a frame for it.
            if (!flush and !end and wire_bytes.len == 0) return;
        }

        // Finish waits for the wire. A non-end `wait` is occupancy-capped:
        // park while this stream's pending DATA is at the flush watermark,
        // then enqueue without a ticket. Mixed cadence finds room and returns
        // to its timer; a saturating producer self-clocks on drainEmit's
        // `wakeStreamSpace` instead of on WritePump completion.
        const need_ticket = end;
        var ticket: u64 = 0;
        var slot_i: u32 = 0;
        defer diagWait(hctx, .running);
        if (need_ticket) {
            diagWait(hctx, .write_reserve_ticket);
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
        var traced = trace.enabled and need_ticket and
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
            self.trace_emit_ns.store(0, .release);
            self.trace_queued_ns.store(0, .release);
            self.trace_written_ns.store(0, .release);
            t_before = nowNs(self.config.io);
        }
        {
            errdefer if (need_ticket) self.tickets.releaseReserved(slot_i);
            diagWait(hctx, .write_lock);
            try self.lockSession();
            diagWait(hctx, .write_space_or_enqueue);
            if (traced) t_acquired = nowNs(self.config.io);
            defer self.unlockSession(self.config.io);
            try self.refuseIfStreamDead(stream_id, hctx.terminal);
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
            if (wait and !end) {
                try self.waitForStreamSpace(hctx, stream_id, flush_occupancy_cap);
            }
            if (wire_bytes.len == 0 and need_ticket and !end) {
                // A zero-length, non-final DataPending is ineligible forever:
                // there is no DATA frame to carry its ticket. Preserve FIFO
                // receipt semantics by attaching to earlier pending bytes, or
                // by queueing a bare pump barrier when no DATA remains.
                if (self.sched.findPending(stream_id)) |pw| {
                    if (pw.len > 0 or pw.end_stream) {
                        pw.flush_ticket = ticket;
                        pw.flush_slot = slot_i;
                        pw.flush_remain = pw.len;
                    } else {
                        try self.pushBareBarrier(hctx, stream_id, ticket, slot_i);
                    }
                } else {
                    try self.pushBareBarrier(hctx, stream_id, ticket, slot_i);
                }
            } else if (wire_bytes.len != 0 or end) {
                self.enqueuePending(stream_id, wire_bytes, end, if (need_ticket) ticket else 0, if (need_ticket) slot_i else 0, hctx) catch |err| return err;
            }
        }
        if (traced) t_unlocked = nowNs(self.config.io);
        if (need_ticket) {
            hctx.slot.awaiting_receipt.store(true, .release);
            defer hctx.slot.awaiting_receipt.store(false, .release);
            diagWait(hctx, .write_wait_ticket);
            try self.waitTicket(stream_id, slot_i, hctx.terminal);
        }
        if (traced) {
            const t_done = nowNs(self.config.io);
            // The actor stamps this when it completes THIS ticket. Zero
            // means the receipt never went through that path, so the sample is
            // dropped rather than folded in with a bogus split.
            const t_ack = self.trace_ack_ns.load(.acquire);
            self.trace_ticket.store(0, .release);
            if (t_ack >= t_unlocked and t_done >= t_ack) {
                const t_emit = self.trace_emit_ns.load(.acquire);
                const t_queued = self.trace_queued_ns.load(.acquire);
                const t_written = self.trace_written_ns.load(.acquire);
                const block = t_acquired - t_before;
                const hold = t_unlocked - t_acquired;
                const ack = t_ack - t_unlocked;
                const res = t_done - t_ack;
                _ = trace.samples.fetchAdd(1, .monotonic);
                _ = trace.block_ns.fetchAdd(block, .monotonic);
                _ = trace.hold_ns.fetchAdd(hold, .monotonic);
                _ = trace.ack_ns.fetchAdd(ack, .monotonic);
                _ = trace.resume_ns.fetchAdd(res, .monotonic);
                if (t_emit >= t_unlocked and t_queued >= t_emit and t_ack >= t_queued) {
                    _ = trace.ack_split_samples.fetchAdd(1, .monotonic);
                    _ = trace.actor_ns.fetchAdd(t_emit - t_unlocked, .monotonic);
                    _ = trace.queue_ns.fetchAdd(t_queued - t_emit, .monotonic);
                    _ = trace.pump_ns.fetchAdd(t_ack - t_queued, .monotonic);
                    if (t_written >= t_queued and t_ack >= t_written) {
                        _ = trace.pump_split_samples.fetchAdd(1, .monotonic);
                        _ = trace.write_ns.fetchAdd(t_written - t_queued, .monotonic);
                        _ = trace.drain_ns.fetchAdd(t_ack - t_written, .monotonic);
                    }
                }
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
                    try self.refuseIfStreamDead(stream_id, hctx.terminal);
                    self.enqueuePending(stream_id, out.items, false, ticket, slot_i, hctx) catch |err| return err;
                    self.processIntents() catch {
                        self.failClosedOnIntentError();
                        return error.WriteFailed;
                    };
                }
                hctx.slot.awaiting_receipt.store(true, .release);
                defer hctx.slot.awaiting_receipt.store(false, .release);
                try self.waitTicket(stream_id, slot_i, hctx.terminal);
            }
        }

        const ticket_pair = self.reserveTicket() catch |err| return err;
        const ticket = ticket_pair[0];
        const slot_i = ticket_pair[1];
        {
            errdefer self.tickets.releaseReserved(slot_i);
            try self.lockSession();
            defer self.unlockSession(self.config.io);
            try self.refuseIfStreamDead(stream_id, hctx.terminal);
            if (self.sched.findPending(stream_id)) |pw| {
                if (pw.len > 0 or pw.end_stream) {
                    pw.flush_ticket = ticket;
                    pw.flush_slot = slot_i;
                    pw.flush_remain = pw.len;
                } else {
                    try self.pushBareBarrier(hctx, stream_id, ticket, slot_i);
                }
            } else {
                try self.pushBareBarrier(hctx, stream_id, ticket, slot_i);
            }
            self.processIntents() catch {
                self.failClosedOnIntentError();
                return error.WriteFailed;
            };
        }
        hctx.slot.awaiting_receipt.store(true, .release);
        defer hctx.slot.awaiting_receipt.store(false, .release);
        try self.waitTicket(stream_id, slot_i, hctx.terminal);
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

/// Isolated Connection hop for `pipeline_bench`: Session + scheduler + inline
/// encode + local ticket complete. No WritePump, no socket write. Production
/// bytes still pass through FairScheduler → `queueWire`.
pub const BenchHop = struct {
    conn: Connection,

    /// Initialize into `self`. `finishBoot` takes pointers to this Connection,
    /// so this must not return a by-value hop — a move would dangle the hooks.
    pub fn open(self: *BenchHop, stream: std.Io.net.Stream, config: ConnConfig) !void {
        self.conn = Connection.init(config.gpa, stream, config) catch |err| {
            stream.close(config.io);
            return err;
        };
        self.conn.finishBoot() catch |err| {
            self.conn.deinit();
            return err;
        };
    }

    pub fn deinit(self: *BenchHop) void {
        self.conn.deinit();
    }

    /// CLIENT_PREFACE + empty SETTINGS, then drain the boot SETTINGS/ACK
    /// that `processIntents` queued. Not part of the timed hop.
    pub fn preparePreface(self: *BenchHop) !void {
        {
            self.conn.lockSessionUncancelable(self.conn.config.io);
            defer self.conn.unlockSession(self.conn.config.io);
            try self.conn.session.ingest(frame.CLIENT_PREFACE);
            var settings_header: [frame.FRAME_HEADER_LEN]u8 = undefined;
            (frame.FrameHeader{
                .length = 0,
                .type = .settings,
                .flags = .{},
                .stream_id = 0,
            }).encode(&settings_header);
            try self.conn.session.ingest(&settings_header);
            try self.conn.processIntents();
        }
        self.drainLocalWrites();
    }

    /// One complete GET oneshot: ingest HEADERS, run the inline handler,
    /// emit through the scheduler, complete the receipt locally.
    pub fn completeOneshot(self: *BenchHop, headers_frame: []const u8) !usize {
        {
            self.conn.lockSessionUncancelable(self.conn.config.io);
            defer self.conn.unlockSession(self.conn.config.io);
            self.conn.session.windows.conn_send = flow.INITIAL_WINDOW;
            try self.conn.session.ingest(headers_frame);
            try self.conn.processIntents();
        }
        self.drainLocalWrites();
        try self.conn.runPendingInline();
        self.drainLocalWrites();
        if (!self.conn.frontCompleteReceiptReady()) return error.HopNoReceipt;
        try self.conn.drainPendingCompleteReceipts(false);
        if (self.conn.writer_failed.load(.acquire)) return error.HopWriterFailed;
        if (self.conn.pending_complete_receipt_n != 0) return error.HopReceiptStuck;
        if (self.conn.inline_n != 0) return error.HopInlineStuck;
        return headers_frame.len;
    }

    /// AckDrainer's job without a WritePump: release wire/control occupancy
    /// and complete tickets so the actor path can `waitTicket` on an
    /// already-signaled slot. Readiness is the ticket event, not a count.
    fn drainLocalWrites(self: *BenchHop) void {
        while (io_queue.tryGet(wire_pump.WireChunk, &self.conn.write_ch, self.conn.config.io)) |chunk| {
            // Applied here, with no pump and no channel: the completion is
            // received the moment the chunk is.
            if (wire_pump.postsCompletion(chunk)) self.conn.acks.sub();
            self.completeLocalWrite(chunk);
        }
    }

    /// One chunk of `drainLocalWrites`, for a test that reads the chunk first.
    fn completeLocalWrite(self: *BenchHop, chunk: wire_pump.WireChunk) void {
        const io = self.conn.config.io;
        if (chunk.pool_index) |idx| {
            self.conn.write_pool.releaseIndex(io, idx);
        } else if (chunk.bytes.len != 0) {
            self.conn.config.gpa.free(chunk.bytes);
        }
        self.conn.applyOutboundRelease(chunk.outbound_release, .wire);
        if (chunk.control_entries != 0) {
            self.conn.applyControlRelease(chunk.control_release, chunk.control_entries);
        }
        if (chunk.ticket != 0) {
            const ack = wire_pump.WriteCompletion{
                .ticket = chunk.ticket,
                .ticket_slot = chunk.ticket_slot,
                .ticket_count = chunk.ticket_count,
                .ok = true,
                .complete_batch_receipt = chunk.complete_batch_receipt,
            };
            if (!self.conn.completeAckTickets(ack)) {
                self.conn.noteServerClose("local_writes_fail", null);
                self.conn.writer_failed.store(true, .release);
                self.conn.tickets.failAll();
                self.conn.complete_receipts_fail_all.store(true, .release);
            }
        }
    }
};

test "the first-frame check accepts only a SETTINGS frame without ACK on stream 0 (t-3511)" {
    var hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const encode = struct {
        fn f(out: *[frame.FRAME_HEADER_LEN]u8, t: frame.FrameType, ack: bool, sid: u31) void {
            (frame.FrameHeader{ .length = 0, .type = t, .flags = .{ .end_stream = ack }, .stream_id = sid }).encode(out);
        }
    }.f;
    encode(&hdr, .settings, false, 0);
    try std.testing.expectEqual(Connection.PrefaceCheck.ok, Connection.checkPrefaceFirst(&hdr));
    encode(&hdr, .settings, true, 0);
    try std.testing.expectEqual(Connection.PrefaceCheck.ack, Connection.checkPrefaceFirst(&hdr));
    encode(&hdr, .rst_stream, false, 1);
    try std.testing.expectEqual(Connection.PrefaceCheck.not_settings, Connection.checkPrefaceFirst(&hdr));
    encode(&hdr, .goaway, false, 0);
    try std.testing.expectEqual(Connection.PrefaceCheck.not_settings, Connection.checkPrefaceFirst(&hdr));
    encode(&hdr, .settings, false, 1);
    try std.testing.expectEqual(Connection.PrefaceCheck.on_stream, Connection.checkPrefaceFirst(&hdr));
    try std.testing.expectEqual(Connection.PrefaceCheck.short, Connection.checkPrefaceFirst(hdr[0..8]));
}

test "t-894 leftover write acks: skip Forced is visible, Forced then deinit is quiet" {
    // Mutation-c (t-882) skipped only drainWriteAcksForced and deinit's
    // second drain absorbed it. This test is that mutation: leftovers in
    // BOTH the held stash and the channel make leftoverWriteAckCount != 0
    // (the deinit panic arm). Forced applies them; deinit then stays quiet.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const limits = limits_mod.Limits.defaults;
    var slabs = try slab_pool.SlabPool.init(gpa, io, limits.outbound_bytes_per_stream, 8);
    defer slabs.deinit(gpa);
    var requests = try testRequestPool();
    defer requests.deinit();

    const bind = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try bind.listen(io, .{ .reuse_address = true });
    const dest = listener.socket.address;
    const peer = dest.connect(io, .{ .mode = .stream }) catch |err| {
        listener.socket.close(io);
        return err;
    };
    const accepted = listener.accept(io) catch |err| {
        peer.close(io);
        listener.socket.close(io);
        return err;
    };
    listener.socket.close(io);

    var hop: BenchHop = undefined;
    hop.open(accepted, .{
        .io = io,
        .mode = .h2c,
        .limits = limits,
        .router = .{ .routes = &.{} },
        .gpa = gpa,
        .slab_pool = &slabs,
        .request_pool = &requests,
    }) catch |err| {
        peer.close(io);
        return err;
    };
    defer {
        hop.conn.drainWriteAcksForced();
        hop.deinit();
        peer.close(io);
    }

    hop.conn.held_acks[0] = .{};
    hop.conn.held_ack_n = 1;
    hop.conn.write_ack_ch.trySend(.{}) catch unreachable;
    try std.testing.expect(hop.conn.leftoverWriteAckCount() != 0);

    hop.conn.drainWriteAcksForced();
    try std.testing.expectEqual(@as(usize, 0), hop.conn.leftoverWriteAckCount());
    hop.conn.assertWriteAcksDrained();
}

fn bigBodyByte(i: usize) u8 {
    return @intCast('a' + (i / 1000) % 26);
}

fn hopBigBody(_: *anyopaque, _: *const request.Request, resp: *response.CompleteResponse) anyerror!void {
    var body: [48 * 1024]u8 = undefined;
    for (&body, 0..) |*b, i| b.* = bigBodyByte(i);
    try resp.send(200, &.{}, &body);
}

/// The same push, through the accounting wrapper every production frame uses.
fn sendSettingsAck(conn: *Connection) anyerror!void {
    var settings: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const n = frame.Serializer.settingsFrame(&settings, true, &.{}) catch unreachable;
    return conn.sendAccountedWire(settings[0..n], false, 0, 0, 0, 0, 0, false, 0, null);
}

test "teardown's sentinel fills the h2c write queue while a handler flushes: the room check sees it and the push is an ending connection's, not a trap" {
    // The sentinel is a chunk in the queue that no push counted in
    // `acks.owed`. With the queue one short of full and nothing owed, the
    // fast path of `h2cQueueRoom` (`owed + need <= capacity`) said "room", the
    // sentinel took the last slot, and the handler's bare flush barrier found
    // the queue full: a trap in a safe build, a failed connection in any
    // other. Here the queue holds `capacity - 1` chunks that were never
    // counted (what the sentinel is), and teardown's own `stopWritePump`
    // supplies the sentinel.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, io);
    defer trap.close();
    const conn = &trap.hop.conn;
    const capacity = conn.write_ch_buf.len;
    const barrier: wire_pump.WireChunk = .{ .bytes = &.{}, .len = 0, .flush_barrier = true };
    for (0..capacity - 1) |_| try std.testing.expect(io_queue.tryPut(wire_pump.WireChunk, &conn.write_ch, io, barrier));
    defer while (io_queue.tryGet(wire_pump.WireChunk, &conn.write_ch, io)) |_| {};

    // Before teardown: the queue has one slot and the ledger owes nothing, so
    // the gate says yes, and a barrier would fit.
    try std.testing.expect(conn.h2cQueueRoom(1));

    // The handler checked the gate under the lock and has not pushed yet;
    // teardown runs now.
    conn.stopWritePump(null);
    try std.testing.expectEqual(capacity, io_queue.queued(wire_pump.WireChunk, &conn.write_ch, io));
    try std.testing.expect(conn.write_queue_ending.load(.acquire));
    // The gate is exact now, so a new check does not let a barrier through...
    try std.testing.expect(!conn.h2cQueueRoom(1));
    try std.testing.expect(!conn.barrierRoom());
    // ...and the one that passed before the sentinel gets a failed write, not
    // a trap, and not a refusal: the connection was already ending.
    const refused_before = test_queue_push_refusals.load(.acquire);
    try std.testing.expectError(error.WriteFailed, conn.pushWriteChunk(barrier));
    try std.testing.expectEqual(refused_before, test_queue_push_refusals.load(.acquire));
    try std.testing.expect(!conn.writer_failed.load(.acquire));
}

test "a push into a full h2c write queue fails the connection closed and never waits (t-3564)" {
    // The push used to wait in `putOne` holding `session_mu` (and a cancel
    // there had its own test, t-3520). The gates leave it room, so a full
    // queue is a gate that failed, and it must not park the task that holds
    // the lock: in a safe build it traps; here the trap is off to see the
    // behavior of every other build.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, io);
    defer trap.close();
    const conn = &trap.hop.conn;
    test_allow_full_queue_push.store(true, .release);
    defer test_allow_full_queue_push.store(false, .release);
    const barrier: wire_pump.WireChunk = .{ .bytes = &.{}, .len = 0, .flush_barrier = true };
    while (io_queue.tryPut(wire_pump.WireChunk, &conn.write_ch, io, barrier)) {}
    defer while (io_queue.tryGet(wire_pump.WireChunk, &conn.write_ch, io)) |_| {};

    const refused_before = test_queue_push_refusals.load(.acquire);
    try std.testing.expectError(error.WriteFailed, conn.pushWriteChunk(barrier));
    try std.testing.expect(conn.writer_failed.load(.acquire));
    try std.testing.expectEqual(refused_before + 1, test_queue_push_refusals.load(.acquire));
    // Through `sendAccountedWire`: the failure releases the reservation it took.
    conn.writer_failed.store(false, .release);
    try std.testing.expectError(error.WriteFailed, sendSettingsAck(conn));
    try std.testing.expectEqual(@as(usize, 0), conn.wire_outbound_held.load(.acquire));
    conn.writer_failed.store(false, .release);
}

test "an h2c connection refuses a write queue smaller than one pop's reservation (t-3520)" {
    // `Server.init` rejects it at boot; a connection built any other way gets
    // the same answer from `Connection.init`.
    var trap: TrapHop = undefined;
    var limits = limits_mod.Limits.defaults;
    limits.inbound_wire_chunks_per_connection = limits_mod.H2C_MIN_WRITE_QUEUE_CHUNKS - 1;
    try std.testing.expectError(error.InvalidConfig, openTrapHopLimits(&trap, std.testing.allocator, std.testing.io, &.{}, limits));
}

test "a full DATA frame is built in a write-pool chunk and reaches the pump without a copy" {
    // Before, a 16,393-byte DATA frame did not fit a 512-byte frame slab, so
    // every one was a GPA alloc, a copy into a write chunk, and a free. That
    // churn was most of the server's 16-32 KiB allocations under load. Here
    // the pool is idle, so every full frame must arrive as a write-pool chunk
    // whose bytes are the frame itself.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopBigBody } },
    }};
    var trap: TrapHop = undefined;
    try openTrapHopRoutes(&trap, gpa, io, &routes);
    defer trap.close();
    const hop = &trap.hop;
    try hop.preparePreface();

    const refused_before = outbound_memory.frames_refused.load(.monotonic);
    const copied_before = hop.conn.wire_copied_bytes;
    const block = [_]u8{ 0x82, 0x87, 0x84 }; // :method GET, :scheme https, :path /
    var headers: [frame.FRAME_HEADER_LEN + block.len]u8 = undefined;
    (frame.FrameHeader{
        .length = block.len,
        .type = .headers,
        .flags = .{ .end_stream = true, .end_headers = true },
        .stream_id = 1,
    }).encode(headers[0..frame.FRAME_HEADER_LEN]);
    @memcpy(headers[frame.FRAME_HEADER_LEN..], &block);
    {
        hop.conn.lockSessionUncancelable(io);
        defer hop.conn.unlockSession(io);
        hop.conn.session.windows.conn_send = flow.INITIAL_WINDOW;
        try hop.conn.session.ingest(&headers);
        try hop.conn.processIntents();
    }
    hop.drainLocalWrites();
    try hop.conn.runPendingInline();

    var full_frames: usize = 0;
    var body_off: usize = 0;
    while (io_queue.tryGet(wire_pump.WireChunk, &hop.conn.write_ch, io)) |chunk| {
        defer hop.completeLocalWrite(chunk);
        var off: usize = 0;
        while (off < chunk.len) {
            const fh = frame.FrameHeader.decode(chunk.bytes[off..][0..frame.FRAME_HEADER_LEN]);
            const payload = chunk.bytes[off + frame.FRAME_HEADER_LEN ..][0..fh.length];
            if (fh.type == .data) {
                for (payload, body_off..) |b, i| try std.testing.expectEqual(bigBodyByte(i), b);
                body_off += payload.len;
                if (payload.len == 16 * 1024) {
                    full_frames += 1;
                    // The chunk is the frame: a write-pool slab, starting at it.
                    try std.testing.expectEqual(@as(usize, 0), off);
                    try std.testing.expectEqual(chunk.len, frame.FRAME_HEADER_LEN + payload.len);
                    const idx = chunk.pool_index orelse return error.FullFrameNotInWritePool;
                    try std.testing.expectEqual(hop.conn.write_pool.slabAt(idx).ptr, chunk.bytes.ptr);
                }
            }
            off += frame.FRAME_HEADER_LEN + fh.length;
        }
    }
    try std.testing.expectEqual(@as(usize, 48 * 1024), body_off);
    try std.testing.expectEqual(@as(usize, 3), full_frames);
    try std.testing.expectEqual(refused_before, outbound_memory.frames_refused.load(.monotonic));
    // HEADERS and the short last DATA frame may be copied; a full frame not.
    try std.testing.expect(hop.conn.wire_copied_bytes - copied_before < 16 * 1024);
    try hop.conn.drainPendingCompleteReceipts(false);
    try std.testing.expect(!hop.conn.writer_failed.load(.acquire));
}

test "a write-pool frame the write queue refuses goes back to the write pool, and its budget credit with it" {
    // `queueWire` hands an owned write-pool frame to `sendAccountedWire`,
    // which then owns it on every path, and the credit it carries too. The
    // push is the exit that can still fail once the credit moved to the wire
    // side, so the chunk must be returned and the credit released from there.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, io);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();

    const rented_before = conn.write_pool.rentedCount();
    const full = conn.rentFrame(frame.FRAME_HEADER_LEN + 16 * 1024, .ordinary) orelse return error.NoWriteChunk;
    try std.testing.expect(conn.write_pool.containsPtr(full.ptr));
    (frame.FrameHeader{ .length = 16 * 1024, .type = .data, .flags = .{}, .stream_id = 1 }).encode(full[0..frame.FRAME_HEADER_LEN]);

    const credit: usize = 16 * 1024;
    const held_before = conn.outbound_held.load(.acquire);
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(credit, .pending));
    conn.closeWriterQueues();
    test_in_sched_sink = true;
    defer test_in_sched_sink = false;
    try std.testing.expectError(error.WriteFailed, conn.queueWire(full, false, 0, 0, 0, 0, 0, credit, true, false));
    try std.testing.expectEqual(rented_before, conn.write_pool.rentedCount());
    try std.testing.expectEqual(held_before, conn.outbound_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), conn.pending_outbound_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), conn.wire_outbound_held.load(.acquire));
}

/// One request for `/` on `sid`: HEADERS with END_STREAM and END_HEADERS.
fn hopRequestFrame(out: *[frame.FRAME_HEADER_LEN + 3]u8, sid: u31) []const u8 {
    const block = [_]u8{ 0x82, 0x87, 0x84 }; // :method GET, :scheme https, :path /
    (frame.FrameHeader{
        .length = block.len,
        .type = .headers,
        .flags = .{ .end_stream = true, .end_headers = true },
        .stream_id = sid,
    }).encode(out[0..frame.FRAME_HEADER_LEN]);
    @memcpy(out[frame.FRAME_HEADER_LEN..], &block);
    return out;
}

fn hopHundredBytes(_: *anyopaque, _: *const request.Request, resp: *response.CompleteResponse) anyerror!void {
    const body: [100]u8 = @splat('h');
    try resp.send(200, &.{}, &body);
}

/// Everything the pump was handed, read before any of it is acked.
const HopChunks = struct {
    chunks: [16]wire_pump.WireChunk = undefined,
    n: usize = 0,
    credit: usize = 0,
    wire_bytes: usize = 0,

    fn take(hop: *BenchHop) HopChunks {
        var out: HopChunks = .{};
        while (io_queue.tryGet(wire_pump.WireChunk, &hop.conn.write_ch, hop.conn.config.io)) |chunk| {
            out.chunks[out.n] = chunk;
            out.n += 1;
            out.credit += chunk.outbound_release;
            out.wire_bytes += chunk.len;
        }
        return out;
    }

    /// The pump's side of it: every chunk written and its completion applied.
    fn ackAll(self: *HopChunks, hop: *BenchHop) void {
        for (self.chunks[0..self.n]) |chunk| {
            if (wire_pump.postsCompletion(chunk)) hop.conn.acks.sub();
            hop.completeLocalWrite(chunk);
        }
        self.n = 0;
    }
};

fn expectBudget(conn: *Connection, pending: usize, wire: usize) !void {
    try std.testing.expectEqual(pending + wire, conn.outbound_held.load(.acquire));
    try std.testing.expectEqual(pending, conn.pending_outbound_held.load(.acquire));
    try std.testing.expectEqual(wire, conn.wire_outbound_held.load(.acquire));
}

test "budget: a DATA frame with no payload carries no credit and still posts a completion" {
    // An empty END_STREAM DATA frame is nine bytes of header and nothing for
    // the budget. The completion is not about the budget: it returns the
    // write-pool chunk to a drain that waits for one, so the pump must post it
    // and the ledger must count it, whatever the chunk's credit is.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, io);
    defer trap.close();
    const hop = &trap.hop;
    const conn = &hop.conn;
    try hop.preparePreface();

    var empty_data: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{ .length = 0, .type = .data, .flags = .{ .end_stream = true }, .stream_id = 1 }).encode(&empty_data);
    try std.testing.expectEqual(@as(usize, 0), conn.acks.owed.load(.acquire));
    try conn.sendAccountedWire(&empty_data, false, 0, 0, 0, 0, 0, false, 0, null);
    try std.testing.expectEqual(@as(usize, 1), conn.acks.owed.load(.acquire));
    try expectBudget(conn, 0, 0);

    var got = HopChunks.take(hop);
    try std.testing.expectEqual(@as(usize, 1), got.n);
    try std.testing.expectEqual(@as(usize, 0), got.chunks[0].outbound_release);
    try std.testing.expect(wire_pump.postsCompletion(got.chunks[0]));
    got.ackAll(hop);
    try std.testing.expectEqual(@as(usize, 0), conn.acks.owed.load(.acquire));

    // The pump's terminal posts and the shutdown sentinel are not chunks'.
    try std.testing.expect(!wire_pump.postsCompletion(.{}));
    try std.testing.expect(!wire_pump.countsChunk(.{ .fail_all = true }));
    try std.testing.expect(wire_pump.countsChunk(.{ .wire_len = frame.FRAME_HEADER_LEN }));
}

test "budget: a DATA frame is counted once, pending until the pump has it, wire until its ack, never both" {
    // The old rule charged a frame again when it moved to the wire, so the
    // total briefly held 48 KiB plus the frames' headers on top of the 48 KiB
    // still pending. The total must not move at all when the drain hands the
    // frames over, and the chunks must carry the payload, not the frame bytes.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopBigBody } },
    }};
    var trap: TrapHop = undefined;
    try openTrapHopRoutes(&trap, gpa, io, &routes);
    defer trap.close();
    const hop = &trap.hop;
    try hop.preparePreface();

    var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
    {
        hop.conn.lockSessionUncancelable(io);
        defer hop.conn.unlockSession(io);
        hop.conn.session.windows.conn_send = flow.INITIAL_WINDOW;
        try hop.conn.session.ingest(hopRequestFrame(&req, 1));
        try hop.conn.processIntents();
    }
    try expectBudget(&hop.conn, 0, 0);
    try hop.conn.runPendingInline();

    const payload: usize = 48 * 1024;
    // The gate reserved a whole stream's cap for the job and took back the
    // 16 KiB it did not write; the 48 KiB it wrote went to the pump.
    try expectBudget(&hop.conn, 0, payload);
    var got = HopChunks.take(hop);
    try std.testing.expectEqual(payload, got.credit);
    // Frame headers, HEADERS and the rest are on the wire and not in the budget.
    try std.testing.expect(got.wire_bytes > payload + 3 * frame.FRAME_HEADER_LEN);
    try expectBudget(&hop.conn, 0, payload);
    got.ackAll(hop);
    try expectBudget(&hop.conn, 0, 0);
    try hop.conn.drainPendingCompleteReceipts(false);
}

test "budget: a packed batch of small DATA frames and controls releases exactly its payload" {
    // Three responses share one drain turn, so HEADERS and DATA frames of
    // three streams leave in the same chunk. The ack must return the 300
    // payload bytes: returning the chunk's length (what the old rule did)
    // over-releases, and the safe build traps on it.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopHundredBytes } },
    }};
    var trap: TrapHop = undefined;
    try openTrapHopRoutes(&trap, gpa, io, &routes);
    defer trap.close();
    const hop = &trap.hop;
    try hop.preparePreface();

    {
        hop.conn.lockSessionUncancelable(io);
        defer hop.conn.unlockSession(io);
        hop.conn.session.windows.conn_send = flow.INITIAL_WINDOW;
        var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
        for ([_]u31{ 1, 3, 5 }) |sid| try hop.conn.session.ingest(hopRequestFrame(&req, sid));
        try hop.conn.processIntents();
    }
    try hop.conn.runPendingInline();

    try expectBudget(&hop.conn, 0, 300);
    var got = HopChunks.take(hop);
    try std.testing.expectEqual(@as(usize, 300), got.credit);
    // Fewer chunks than frames: they were packed.
    try std.testing.expect(got.n < 6);
    try std.testing.expect(got.wire_bytes > 300 + 6 * frame.FRAME_HEADER_LEN);
    got.ackAll(hop);
    try expectBudget(&hop.conn, 0, 0);
    try hop.conn.drainPendingCompleteReceipts(false);
}

test "budget: an inline job waits for room and runs when it comes back, with no reset" {
    // The actor never waits, so `runPendingInline` holds a complete job back
    // until one stream's cap is free, and the job then runs. A full budget
    // used to refuse its write and reset the stream.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopHundredBytes } },
    }};
    var trap: TrapHop = undefined;
    try openTrapHopRoutes(&trap, gpa, io, &routes);
    defer trap.close();
    const hop = &trap.hop;
    try hop.preparePreface();
    const conn = &hop.conn;

    // Fill the budget but for 1,000 bytes: less than one stream's cap.
    const lim = conn.config.limits.outbound_bytes_per_connection;
    const filler = lim - 1000;
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(filler, .pending));

    {
        conn.lockSessionUncancelable(io);
        defer conn.unlockSession(io);
        conn.session.windows.conn_send = flow.INITIAL_WINDOW;
        var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
        try conn.session.ingest(hopRequestFrame(&req, 1));
        try conn.processIntents();
    }
    const gated_before = test_inline_budget_gated.load(.monotonic);
    try conn.runPendingInline();
    // Still queued, nothing reserved for it, nothing sent, nothing reset.
    try std.testing.expectEqual(@as(usize, 1), conn.inline_n);
    try expectBudget(conn, filler, 0);
    try std.testing.expect(test_inline_budget_gated.load(.monotonic) > gated_before);
    try std.testing.expectEqual(@as(usize, 0), io_queue.queued(wire_pump.WireChunk, &conn.write_ch, io));

    // Room returns: the job runs and its 100 bytes are the whole charge.
    conn.applyOutboundRelease(filler, .pending);
    try conn.runPendingInline();
    try std.testing.expectEqual(@as(usize, 0), conn.inline_n);
    try expectBudget(conn, 0, 100);
    var got = HopChunks.take(hop);
    try std.testing.expectEqual(@as(usize, 100), got.credit);
    got.ackAll(hop);
    try expectBudget(conn, 0, 0);
    try conn.drainPendingCompleteReceipts(false);
    try std.testing.expect(!conn.writer_failed.load(.acquire));
}

/// A handler slot that exists only to wait in the budget queue: no task, no
/// stream, just the entry and its event.
fn fakeBudgetWaiter(conn: *Connection, stream_id: u31) usize {
    _ = conn.handlers.touchNext(.{ .stream_id = stream_id, .in_use = true }).?;
    const i = conn.handlers.touched - 1;
    // What `claimSlot` does for a real handler.
    conn.space_events[i] = .init;
    return i;
}

fn endFakeWaiters(conn: *Connection) void {
    for (conn.handlers.slice()) |*slot| slot.in_use = false;
}

test "budget queue: waiters are served in arrival order, and room too small for the head wakes no one" {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, std.testing.io);
    defer trap.close();
    const conn = &trap.hop.conn;
    defer endFakeWaiters(conn);
    const lim = conn.config.limits.outbound_bytes_per_connection;
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(lim, .pending));

    const a = fakeBudgetWaiter(conn, 1);
    const b = fakeBudgetWaiter(conn, 3);
    const c = fakeBudgetWaiter(conn, 5);
    const grants_before = test_budget_grants.load(.monotonic);
    conn.queueForBudget(&conn.handlers.storage[a], a, 30, false);
    conn.queueForBudget(&conn.handlers.storage[b], b, 10, false);
    conn.queueForBudget(&conn.handlers.storage[c], c, 20, false);
    try std.testing.expectEqual(@as(u32, 3), conn.budget_waiters.load(.acquire));

    // 10 bytes come back. `b` would fit, `a` (the head) would not: `b` must
    // wait behind it, and nothing is reserved.
    conn.applyOutboundRelease(10, .pending);
    try std.testing.expectEqual(BudgetWait.State.queued, conn.handlers.storage[a].budget.state);
    try std.testing.expectEqual(BudgetWait.State.queued, conn.handlers.storage[b].budget.state);
    try std.testing.expectEqual(lim - 10, conn.outbound_held.load(.acquire));
    try std.testing.expectEqual(grants_before, test_budget_grants.load(.monotonic));

    // 20 more: the head fits, and only the head is served (the room is gone).
    conn.applyOutboundRelease(20, .pending);
    try std.testing.expectEqual(BudgetWait.State.granted, conn.handlers.storage[a].budget.state);
    try std.testing.expectEqual(BudgetWait.State.queued, conn.handlers.storage[b].budget.state);
    try std.testing.expectEqual(lim, conn.outbound_held.load(.acquire));

    conn.applyOutboundRelease(10, .pending);
    try std.testing.expectEqual(BudgetWait.State.granted, conn.handlers.storage[b].budget.state);
    try std.testing.expectEqual(BudgetWait.State.queued, conn.handlers.storage[c].budget.state);
    conn.applyOutboundRelease(20, .pending);
    try std.testing.expectEqual(BudgetWait.State.granted, conn.handlers.storage[c].budget.state);
    try std.testing.expectEqual(@as(u32, 0), conn.budget_waiters.load(.acquire));
    try std.testing.expectEqual(budget_none, conn.budget_head);
    try std.testing.expectEqual(budget_none, conn.budget_tail);

    // The grant order is the arrival order.
    try std.testing.expectEqual(grants_before + 3, test_budget_grants.load(.monotonic));
    for ([_]u32{ 1, 3, 5 }, 0..) |sid, k| {
        try std.testing.expectEqual(sid, test_budget_grant_log[(grants_before + k) % test_budget_grant_log.len].load(.monotonic));
    }

    // Each takes its grant, and the books balance: 60 granted, `lim - 60`
    // still held by the filler.
    for ([_]usize{ a, b, c }) |i| {
        const ev = &conn.space_events[i];
        try std.testing.expectEqual(BudgetWait.State.granted, conn.takeBudgetOutcome(&conn.handlers.storage[i], ev));
    }
    conn.applyOutboundRelease(lim, .pending);
    try expectBudget(conn, 0, 0);
}

test "budget queue: a head refused by the SERVER cap is woken to fail, not left queued" {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, std.testing.io);
    defer trap.close();
    const conn = &trap.hop.conn;
    defer endFakeWaiters(conn);
    var acct: GlobalAccounting = .{ .max_streams = 8, .reaper_capacity = 8, .max_outbound_bytes = 160, .max_handshakes = 1 };
    conn.config.accounting = &acct;
    conn.config.limits.outbound_bytes_per_connection = 100;

    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(100, .pending));
    const w = fakeBudgetWaiter(conn, 1);
    conn.queueForBudget(&conn.handlers.storage[w], w, 40, false);
    try std.testing.expectEqual(BudgetWait.State.queued, conn.handlers.storage[w].budget.state);
    // Other connections take the server's room while this one waits: 60 more
    // bytes, then the cap is all used up.
    try std.testing.expect(acct.tryReserveOutbound(60));
    acct.max_outbound_bytes = 140;

    // 50 bytes come back. The connection has room for the head (50 >= 40);
    // the server has 140 - 110 = 30. No release on this connection will ever
    // change that, so the head is woken with a refusal and the queue is empty.
    conn.applyOutboundRelease(50, .pending);
    try std.testing.expectEqual(BudgetWait.State.refused, conn.handlers.storage[w].budget.state);
    try std.testing.expectEqual(@as(u32, 0), conn.budget_waiters.load(.acquire));
    try std.testing.expectEqual(budget_none, conn.budget_head);
    // Nothing was reserved for it.
    try expectBudget(conn, 50, 0);
    try std.testing.expectEqual(@as(usize, 110), acct.outbound_bytes.load(.acquire));
    try std.testing.expectEqual(BudgetWait.State.refused, conn.takeBudgetOutcome(&conn.handlers.storage[w], &conn.space_events[w]));
    conn.applyOutboundRelease(50, .pending);
    acct.releaseOutbound(60);
    try std.testing.expectEqual(@as(usize, 0), acct.outbound_bytes.load(.acquire));
}

test "budget queue: leaving the queue returns an unused grant exactly once" {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, std.testing.io);
    defer trap.close();
    const conn = &trap.hop.conn;
    defer endFakeWaiters(conn);
    const lim = conn.config.limits.outbound_bytes_per_connection;
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(lim, .pending));

    const a = fakeBudgetWaiter(conn, 1);
    const b = fakeBudgetWaiter(conn, 3);
    const c = fakeBudgetWaiter(conn, 5);
    conn.queueForBudget(&conn.handlers.storage[a], a, 10, false);
    conn.queueForBudget(&conn.handlers.storage[b], b, 10, false);
    conn.queueForBudget(&conn.handlers.storage[c], c, 10, false);

    // A queued waiter in the middle leaves: the others keep their order.
    conn.abandonBudget(&conn.handlers.storage[b], b);
    try std.testing.expectEqual(BudgetWait.State.idle, conn.handlers.storage[b].budget.state);
    try std.testing.expectEqual(@as(u32, 2), conn.budget_waiters.load(.acquire));
    try std.testing.expectEqual(@as(u32, @intCast(a)), conn.budget_head);
    try std.testing.expectEqual(@as(u32, @intCast(c)), conn.budget_tail);
    try std.testing.expectEqual(@as(u32, @intCast(c)), conn.handlers.storage[a].budget.next);

    // The head is granted, then leaves without the bytes: they go back once,
    // and the next waiter gets them.
    conn.applyOutboundRelease(10, .pending);
    try std.testing.expectEqual(BudgetWait.State.granted, conn.handlers.storage[a].budget.state);
    try std.testing.expectEqual(BudgetWait.State.queued, conn.handlers.storage[c].budget.state);
    conn.abandonBudget(&conn.handlers.storage[a], a);
    try std.testing.expectEqual(BudgetWait.State.idle, conn.handlers.storage[a].budget.state);
    try std.testing.expectEqual(BudgetWait.State.granted, conn.handlers.storage[c].budget.state);
    try std.testing.expectEqual(lim, conn.outbound_held.load(.acquire));
    // A second exit finds nothing to give back.
    conn.abandonBudget(&conn.handlers.storage[a], a);
    try std.testing.expectEqual(lim, conn.outbound_held.load(.acquire));

    // The tail leaves queued, then the last grant is returned.
    conn.abandonBudget(&conn.handlers.storage[c], c);
    try std.testing.expectEqual(lim - 10, conn.outbound_held.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), conn.budget_waiters.load(.acquire));
    try std.testing.expectEqual(budget_none, conn.budget_head);
    try std.testing.expectEqual(budget_none, conn.budget_tail);
    conn.applyOutboundRelease(lim - 10, .pending);
    try expectBudget(conn, 0, 0);
}

test "budget queue: a waiter that leaves hands its room to a follower that fits" {
    // The head needs 30 and 10 are free; the follower needs 10 and fits, but
    // waits behind the head. When the head leaves, no release is coming: the
    // follower must be served by the exit itself.
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, std.testing.io);
    defer trap.close();
    const conn = &trap.hop.conn;
    defer endFakeWaiters(conn);
    const lim = conn.config.limits.outbound_bytes_per_connection;
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(lim, .pending));

    const head = fakeBudgetWaiter(conn, 1);
    const follower = fakeBudgetWaiter(conn, 3);
    conn.queueForBudget(&conn.handlers.storage[head], head, 30, false);
    conn.queueForBudget(&conn.handlers.storage[follower], follower, 10, false);
    conn.applyOutboundRelease(10, .pending);
    try std.testing.expectEqual(BudgetWait.State.queued, conn.handlers.storage[follower].budget.state);
    try std.testing.expectEqual(lim - 10, conn.outbound_held.load(.acquire));

    conn.abandonBudget(&conn.handlers.storage[head], head);
    try std.testing.expectEqual(BudgetWait.State.granted, conn.handlers.storage[follower].budget.state);
    try std.testing.expectEqual(lim, conn.outbound_held.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), conn.budget_waiters.load(.acquire));
    try std.testing.expectEqual(BudgetWait.State.granted, conn.takeBudgetOutcome(&conn.handlers.storage[follower], &conn.space_events[follower]));
    conn.applyOutboundRelease(lim, .pending);
    try expectBudget(conn, 0, 0);
}

test "budget: a grant that arrives with a reset or a failed writer goes back" {
    // The granted branch of `parkForBudget`: the bytes are reserved for the
    // handler, which finds its stream over (or the writer dead) when it runs.
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, std.testing.io);
    defer trap.close();
    const conn = &trap.hop.conn;
    defer endFakeWaiters(conn);
    const a = fakeBudgetWaiter(conn, 1);
    const b = fakeBudgetWaiter(conn, 3);
    const c = fakeBudgetWaiter(conn, 5);

    // A terminal cause set after the grant.
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(50, .pending));
    conn.handlers.storage[a].terminal.setCause(.server_shutdown);
    try std.testing.expectError(response.causeToError(.server_shutdown), conn.acceptGrant(&conn.handlers.storage[a].terminal, 50));
    try expectBudget(conn, 0, 0);

    // The writer failed after the grant.
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(50, .pending));
    conn.writer_failed.store(true, .release);
    try std.testing.expectError(error.WriteFailed, conn.acceptGrant(&conn.handlers.storage[b].terminal, 50));
    conn.writer_failed.store(false, .release);
    try expectBudget(conn, 0, 0);

    // Neither: the handler keeps its bytes.
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(50, .pending));
    try conn.acceptGrant(&conn.handlers.storage[c].terminal, 50);
    try expectBudget(conn, 50, 0);
    conn.applyOutboundRelease(50, .pending);
}

fn parkedTaskMain(conn: *Connection, hctx: *HandlerCtx, out: *?anyerror) void {
    conn.lockSessionUncancelable(conn.config.io);
    defer conn.unlockSession(conn.config.io);
    conn.parkForBudget(hctx, hctx.stream_id, 100) catch |err| {
        out.* = err;
    };
}

fn cancelParkedScenario(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, rt.io());
    defer trap.close();
    const conn = &trap.hop.conn;
    defer endFakeWaiters(conn);
    const lim = conn.config.limits.outbound_bytes_per_connection;
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(lim, .pending));

    const i = fakeBudgetWaiter(conn, 1);
    const slot = &conn.handlers.storage[i];
    var hctx: HandlerCtx = .{ .conn = conn, .terminal = &slot.terminal, .slot = slot, .stream_id = 1, .arena = gpa };
    var out: ?anyerror = null;
    var task = try rt.spawn(parkedTaskMain, .{ conn, &hctx, &out });
    const deadline = nowNs(conn.config.io) + 5 * std.time.ns_per_s;
    while (conn.budget_parks.load(.acquire) == 0) {
        if (nowNs(conn.config.io) >= deadline) return error.NeverParked;
        try zio.sleep(.fromMilliseconds(1));
    }
    try std.testing.expectEqual(@as(u32, 1), conn.budget_waiters.load(.acquire));

    // The task is cancelled while it waits (the reaper's way of stopping a
    // handler that does not come out by itself).
    task.cancel();
    try std.testing.expectEqual(@as(?anyerror, error.Canceled), out);
    try std.testing.expectEqual(@as(u32, 0), conn.budget_waiters.load(.acquire));
    try std.testing.expectEqual(budget_none, conn.budget_head);
    try std.testing.expectEqual(BudgetWait.State.idle, slot.budget.state);
    try std.testing.expectEqual(lim, conn.outbound_held.load(.acquire));
    conn.applyOutboundRelease(lim, .pending);
    try expectBudget(conn, 0, 0);
}

test "budget: a task handler cancelled while it waits leaves the queue and reserves nothing" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(1),
    });
    defer rt.deinit();
    var h = try rt.spawn(cancelParkedScenario, .{ rt, gpa });
    try h.join();
}

test "budget queue: a gated inline job is served in arrival order behind task writers, then runs" {
    // Task writers keep the queue non-empty. A complete job that arrives
    // takes its place at the tail (it does not take room from outside the
    // queue, and it is not left waiting for a moment when the room is a whole
    // cap): it is granted third, after the two writers before it, and then runs.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopHundredBytes } },
    }};
    var trap: TrapHop = undefined;
    try openTrapHopRoutes(&trap, gpa, io, &routes);
    defer trap.close();
    const hop = &trap.hop;
    try hop.preparePreface();
    const conn = &hop.conn;
    const lim = conn.config.limits.outbound_bytes_per_connection;
    const cap = conn.config.limits.outbound_bytes_per_stream;
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(lim, .pending));

    {
        conn.lockSessionUncancelable(io);
        defer conn.unlockSession(io);
        conn.session.windows.conn_send = flow.INITIAL_WINDOW;
        var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
        try conn.session.ingest(hopRequestFrame(&req, 1));
        try conn.processIntents();
    }
    const w1 = fakeBudgetWaiter(conn, 101);
    const w2 = fakeBudgetWaiter(conn, 103);
    defer {
        conn.handlers.storage[w1].in_use = false;
        conn.handlers.storage[w2].in_use = false;
    }
    conn.queueForBudget(&conn.handlers.storage[w1], w1, 100, false);
    conn.queueForBudget(&conn.handlers.storage[w2], w2, 100, false);

    const grants_before = test_budget_grants.load(.monotonic);
    const gated_before = test_inline_budget_gated.load(.monotonic);
    try conn.runPendingInline();
    // Queued behind both, nothing reserved for it, the job not run.
    try std.testing.expectEqual(@as(usize, 1), conn.inline_n);
    try std.testing.expectEqual(@as(u32, 3), conn.budget_waiters.load(.acquire));
    try std.testing.expect(test_inline_budget_gated.load(.monotonic) > gated_before);

    conn.applyOutboundRelease(100, .pending);
    try std.testing.expectEqual(BudgetWait.State.granted, conn.handlers.storage[w1].budget.state);
    conn.applyOutboundRelease(100, .pending);
    try std.testing.expectEqual(BudgetWait.State.granted, conn.handlers.storage[w2].budget.state);
    try conn.runPendingInline();
    try std.testing.expectEqual(@as(usize, 1), conn.inline_n);

    conn.applyOutboundRelease(cap, .pending);
    try conn.runPendingInline();
    try std.testing.expectEqual(@as(usize, 0), conn.inline_n);

    // The order of the grants: both writers, then the job (stream 1).
    try std.testing.expectEqual(grants_before + 3, test_budget_grants.load(.monotonic));
    for ([_]u32{ 101, 103, 1 }, 0..) |sid, k| {
        try std.testing.expectEqual(sid, test_budget_grant_log[(grants_before + k) % test_budget_grant_log.len].load(.monotonic));
    }

    var got = HopChunks.take(hop);
    try std.testing.expectEqual(@as(usize, 100), got.credit);
    got.ackAll(hop);
    try hop.conn.drainPendingCompleteReceipts(false);
    for ([_]usize{ w1, w2 }) |i| {
        try std.testing.expectEqual(BudgetWait.State.granted, conn.takeBudgetOutcome(&conn.handlers.storage[i], &conn.space_events[i]));
    }
    conn.applyOutboundRelease(conn.pending_outbound_held.load(.acquire), .pending);
    try expectBudget(conn, 0, 0);
}

test "budget queue: a complete job whose stream is reset while it waits leaves the queue and a grant goes back" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopHundredBytes } },
    }};
    var trap: TrapHop = undefined;
    try openTrapHopRoutes(&trap, gpa, io, &routes);
    defer trap.close();
    const hop = &trap.hop;
    try hop.preparePreface();
    const conn = &hop.conn;
    const lim = conn.config.limits.outbound_bytes_per_connection;
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(lim, .pending));
    {
        conn.lockSessionUncancelable(io);
        defer conn.unlockSession(io);
        conn.session.windows.conn_send = flow.INITIAL_WINDOW;
        var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
        try conn.session.ingest(hopRequestFrame(&req, 1));
        try conn.processIntents();
    }
    try conn.runPendingInline();
    try std.testing.expectEqual(@as(u32, 1), conn.budget_waiters.load(.acquire));

    // Reset while queued: it leaves, nothing is owed.
    {
        conn.lockSessionUncancelable(io);
        defer conn.unlockSession(io);
        conn.handlers.storage[0].terminal.setCause(.server_shutdown);
        conn.wakeHandlerWaiters(1);
    }
    try std.testing.expectEqual(@as(u32, 0), conn.budget_waiters.load(.acquire));
    try std.testing.expectEqual(BudgetWait.State.idle, conn.handlers.storage[0].budget.state);
    try std.testing.expectEqual(lim, conn.outbound_held.load(.acquire));

    // The job still runs (to fail at its first check) and takes no budget.
    try conn.runPendingInline();
    try std.testing.expectEqual(@as(usize, 0), conn.inline_n);
    try std.testing.expectEqual(lim, conn.outbound_held.load(.acquire));
    conn.applyOutboundRelease(lim, .pending);
    // Whatever the failed job queued (an RST) is drained and acked.
    hop.drainLocalWrites();
    try expectBudget(conn, 0, 0);
}

/// A hop with one complete route, the budget full, and one request (stream 1)
/// ingested: its job sits in `inline_sids`. For the tests of a complete job's
/// budget entry.
fn inlineEntryHop(trap: *TrapHop, gpa: std.mem.Allocator, io: std.Io, routes: []const router_mod.Route) !void {
    try openTrapHopRoutes(trap, gpa, io, routes);
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    const lim = conn.config.limits.outbound_bytes_per_connection;
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(lim, .pending));
    conn.lockSessionUncancelable(io);
    defer conn.unlockSession(io);
    conn.session.windows.conn_send = flow.INITIAL_WINDOW;
    var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
    try conn.session.ingest(hopRequestFrame(&req, 1));
    try conn.processIntents();
}

test "budget queue: a GRANTED complete job whose stream is reset gives its cap back at once, and once" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopHundredBytes } },
    }};
    var trap: TrapHop = undefined;
    try inlineEntryHop(&trap, gpa, io, &routes);
    defer trap.close();
    const hop = &trap.hop;
    const conn = &hop.conn;
    const lim = conn.config.limits.outbound_bytes_per_connection;
    const cap = conn.config.limits.outbound_bytes_per_stream;

    try conn.runPendingInline();
    try std.testing.expectEqual(BudgetWait.State.queued, conn.handlers.storage[0].budget.state);
    // A release grants it: the cap is reserved for the job, which has not run.
    conn.applyOutboundRelease(lim, .pending);
    try std.testing.expectEqual(BudgetWait.State.granted, conn.handlers.storage[0].budget.state);
    try expectBudget(conn, cap, 0);

    // The stream is reset before the actor runs it: the cap goes back now.
    {
        conn.lockSessionUncancelable(io);
        defer conn.unlockSession(io);
        conn.handlers.storage[0].terminal.setCause(.server_shutdown);
        conn.wakeHandlerWaiters(1);
    }
    try expectBudget(conn, 0, 0);
    try std.testing.expectEqual(BudgetWait.State.idle, conn.handlers.storage[0].budget.state);

    // The job then ends without a second return (an over-release would trap).
    try conn.runPendingInline();
    try std.testing.expectEqual(@as(usize, 0), conn.inline_n);
    hop.drainLocalWrites();
    try expectBudget(conn, 0, 0);
}

test "budget queue: a writer failure grants a queued complete job and ends its stream; the batch ends it cleanly" {
    // `handleWriterFailed` sets every slot's cause and releases the pending
    // DATA of the scheduler, which grants the queued entry, and it never
    // abandons the entry. The actor then runs the batch: the terminal job must
    // give its entry up before it runs, or the slot is released with the entry
    // granted. The budget here is held by scheduler-owned DATA, so the release
    // that grants the entry is `handleWriterFailed`'s own: no release is made
    // by the test.
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopHundredBytes } },
    }};
    var trap: TrapHop = undefined;
    try inlineEntryHop(&trap, gpa, io, &routes);
    defer trap.close();
    const hop = &trap.hop;
    const conn = &hop.conn;
    const cap = conn.config.limits.outbound_bytes_per_stream;

    // Swap the helper's direct reservation for two streams' worth of DATA that
    // sits in the scheduler (the hop's slab pool holds only a few slabs, so the
    // budget is lowered to what they can fill).
    conn.applyOutboundRelease(conn.config.limits.outbound_bytes_per_connection, .pending);
    conn.config.limits.outbound_bytes_per_connection = 2 * cap;
    const chunk: [limits_mod.Limits.defaults.outbound_bytes_per_stream]u8 = @splat('d');
    try std.testing.expectEqual(cap, chunk.len);
    for ([_]u31{ 101, 103 }) |sid| {
        try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(cap, .pending));
        try conn.sched.enqueueDataBytes(sid, &chunk, false, 0, 0);
    }
    try expectBudget(conn, 2 * cap, 0);

    try conn.runPendingInline();
    try std.testing.expectEqual(BudgetWait.State.queued, conn.handlers.storage[0].budget.state);
    try expectBudget(conn, 2 * cap, 0);

    conn.writer_failed.store(true, .release);
    {
        conn.lockSessionUncancelable(io);
        defer conn.unlockSession(io);
        conn.handleWriterFailed();
    }
    // `handleWriterFailed` released the scheduler's DATA and that granted the
    // entry; the stream is terminal. Nothing else released anything.
    try std.testing.expectEqual(BudgetWait.State.granted, conn.handlers.storage[0].budget.state);
    try std.testing.expect(conn.handlers.storage[0].terminal.getCause() != null);
    try expectBudget(conn, cap, 0);

    // The batch: the job is terminal, so its entry ends before it runs, the
    // slot is released with an idle entry, and the books are zero.
    try conn.runPendingInline();
    try std.testing.expectEqual(@as(usize, 0), conn.inline_n);
    try expectBudget(conn, 0, 0);
    try std.testing.expectEqual(@as(u32, 0), conn.budget_waiters.load(.acquire));
    try std.testing.expectEqual(budget_none, conn.budget_head);
    hop.drainLocalWrites();
    conn.writer_failed.store(false, .release);
}

test "budget queue: admitting a complete job to an empty queue rings no bell; a later grant does" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopHundredBytes } },
    }};
    var trap: TrapHop = undefined;
    try openTrapHopRoutes(&trap, gpa, io, &routes);
    defer trap.close();
    const hop = &trap.hop;
    const conn = &hop.conn;
    try hop.preparePreface();
    {
        conn.lockSessionUncancelable(io);
        defer conn.unlockSession(io);
        conn.session.windows.conn_send = flow.INITIAL_WINDOW;
        var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
        try conn.session.ingest(hopRequestFrame(&req, 1));
        try conn.processIntents();
    }
    // Empty queue, room: granted in its own admission and taken there.
    const rings_before = test_budget_doorbell_rings.load(.monotonic);
    try conn.runPendingInline();
    try std.testing.expectEqual(@as(usize, 0), conn.inline_n);
    try std.testing.expectEqual(rings_before, test_budget_doorbell_rings.load(.monotonic));
    var got = HopChunks.take(hop);
    got.ackAll(hop);
    try conn.drainPendingCompleteReceipts(false);
    try expectBudget(conn, 0, 0);

    // A job that has to wait is granted by a release, and that rings.
    const lim = conn.config.limits.outbound_bytes_per_connection;
    try std.testing.expectEqual(Connection.ReserveResult.ok, conn.tryReserveOutboundBytes(lim, .pending));
    {
        conn.lockSessionUncancelable(io);
        defer conn.unlockSession(io);
        var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
        try conn.session.ingest(hopRequestFrame(&req, 3));
        try conn.processIntents();
    }
    try conn.runPendingInline();
    try std.testing.expectEqual(@as(usize, 1), conn.inline_n);
    try std.testing.expectEqual(rings_before, test_budget_doorbell_rings.load(.monotonic));
    // The release is a wire release's kind: no `session_mu`, so no unlock rings
    // after it, and the token below can only be the grant's own (the ring in
    // `grantBudgetLocked` is kept for this, `unlockSession` does not replace it).
    drainDoorbell(conn);
    conn.applyOutboundRelease(lim, .pending);
    try std.testing.expectEqual(rings_before + 1, test_budget_doorbell_rings.load(.monotonic));
    try std.testing.expect(io_queue.tryRecv(u8, &conn.doorbell) != null);
    try conn.runPendingInline();
    var got2 = HopChunks.take(hop);
    got2.ackAll(hop);
    try conn.drainPendingCompleteReceipts(false);
    try expectBudget(conn, 0, 0);
}

/// Give back the two preface frames the connection queued at boot, so a test
/// that counts slabs and chunks starts from full pools.
fn dropQueuedPreface(conn: *Connection) void {
    for (conn.session.drainIntents()) |*it| conn.session.releaseIntent(it);
}

test "rentFrame: a queued frame leaves POP_PUSHES chunks free, a frame built in a pop may take them, and an empty pool refuses" {
    var lim = limits_mod.Limits.defaults;
    lim.frame_slab_bytes = 64;
    lim.frame_slabs_per_connection = 4;
    lim.inbound_wire_chunks_per_connection = wire_const.POP_PUSHES + 2;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, std.testing.allocator, std.testing.io, &.{}, lim);
    defer trap.close();
    const conn = &trap.hop.conn;
    dropQueuedPreface(conn);
    const pushes = wire_const.POP_PUSHES;
    try std.testing.expectEqual(pushes + 2, conn.write_pool.freeCount());

    const refused_before = outbound_memory.frames_refused.load(.monotonic);
    var held: [8][]u8 = undefined;
    var n: usize = 0;
    defer for (held[0..n]) |buf| conn.releaseWireBytes(buf);

    // Every slab (terminal frames have a store of their own), then chunks, down to POP_PUSHES free.
    while (conn.frame_pool.freeCount() > 0) {
        held[n] = conn.rentFrame(20, .ordinary) orelse return error.NoSlab;
        try std.testing.expect(conn.frame_pool.containsPtr(held[n].ptr));
        n += 1;
    }
    try std.testing.expect(n == 4);
    while (conn.write_pool.freeCount() > pushes) {
        held[n] = conn.rentFrame(20, .ordinary) orelse return error.NoChunkToSpare;
        try std.testing.expect(conn.write_pool.containsPtr(held[n].ptr));
        n += 1;
    }
    // A queued frame is refused here, small or large, with chunks still free.
    try std.testing.expectEqual(pushes, conn.write_pool.freeCount());
    try std.testing.expect(conn.rentFrame(20, .ordinary) == null);
    try std.testing.expect(conn.rentFrame(limits_mod.WIRE_CHUNK_SIZE, .ordinary) == null);
    try std.testing.expectEqual(refused_before + 2, outbound_memory.frames_refused.load(.monotonic));
    // The scheduler would pop here: the room is exactly what a pop needs.
    try std.testing.expect(!conn.outboundMemoryPaused());

    // The DATA frame of a pop takes what the room check counted, and then the
    // pool is empty and the next rent is refused. Nothing came from `gpa`:
    // the test allocator would report a leak for it.
    conn.building_pop_frame = true;
    defer conn.building_pop_frame = false;
    while (conn.write_pool.freeCount() > 0) {
        held[n] = conn.rentFrame(limits_mod.WIRE_CHUNK_SIZE, .ordinary) orelse return error.NoChunkForPop;
        n += 1;
    }
    try std.testing.expect(conn.rentFrame(20, .ordinary) == null);
    // A frame above the write chunk is refused whatever is free.
    try std.testing.expect(conn.rentFrame(limits_mod.WIRE_CHUNK_SIZE + 1, .ordinary) == null);
    // And the drain is held back until a chunk comes home.
    try std.testing.expect(conn.outboundMemoryPaused());
    conn.releaseWireBytes(held[n - 1]);
    n -= 1;
    try std.testing.expect(conn.outboundMemoryPaused());
    conn.releaseWireBytes(held[n - 1]);
    n -= 1;
    try std.testing.expect(!conn.outboundMemoryPaused());
}

test "rentFrame: a terminal frame never needs a frame slab or a write chunk, and the last slot is the final GOAWAY's" {
    var lim = limits_mod.Limits.defaults;
    lim.frame_slab_bytes = 64;
    lim.frame_slabs_per_connection = 8;
    lim.max_streams_per_connection = 4;
    lim.max_streams_per_server = 16;
    lim.cancellation_reaper_jobs = 16;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, std.testing.allocator, std.testing.io, &.{}, lim);
    defer trap.close();
    const conn = &trap.hop.conn;
    dropQueuedPreface(conn);
    const t = try lim.terminalFrames();
    try std.testing.expectEqual(@as(usize, 7), t);
    try std.testing.expectEqual(t, conn.terminal_store.capacity);

    // Take every chunk and every slab away: ordinary traffic has used all of them.
    var chunks: [16]u32 = undefined;
    var n_chunks: usize = 0;
    defer for (chunks[0..n_chunks]) |idx| conn.write_pool.releaseIndex(std.testing.io, idx);
    while (conn.write_pool.tryRentIndex(std.testing.io)) |idx| {
        chunks[n_chunks] = idx;
        n_chunks += 1;
    }
    var slabs: [8][]u8 = undefined;
    var n_slabs: usize = 0;
    defer for (slabs[0..n_slabs]) |buf| conn.releaseWireBytes(buf);
    while (conn.rentFrame(17, .ordinary)) |buf| {
        slabs[n_slabs] = buf;
        n_slabs += 1;
    }
    try std.testing.expect(conn.rentFrame(17, .ordinary) == null);

    var held: [8][]u8 = undefined;
    var n: usize = 0;
    defer for (held[0..n]) |buf| conn.releaseWireBytes(buf);
    // T - 1 RSTs and graceful GOAWAYs fit, each from the store.
    while (n < t - 1) : (n += 1) {
        held[n] = conn.rentFrame(if (n % 2 == 0) 13 else 17, .terminal) orelse return error.TerminalFrameRefused;
        try std.testing.expect(conn.terminal_store.containsPtr(held[n].ptr));
    }
    // The last slot is kept for the GOAWAY that ends the connection.
    try std.testing.expect(conn.rentFrame(13, .terminal) == null);
    held[n] = conn.rentFrame(17, .final_goaway) orelse return error.FinalGoawayRefused;
    n += 1;
    try std.testing.expectEqual(@as(usize, 0), conn.terminal_store.freeCount());
}

test "the terminal ring, the pool's terminal share and the frame store all hold T = M + 3 (t-3562)" {
    for ([_]usize{ 1, 256, 1024 }) |m| {
        var lim = limits_mod.Limits.defaults;
        lim.max_streams_per_connection = m;
        lim.max_streams_per_server = @max(lim.max_streams_per_server, m);
        lim.cancellation_reaper_jobs = @max(lim.cancellation_reaper_jobs, lim.max_streams_per_server);
        lim.outbound_slabs_per_server = @max(lim.outbound_slabs_per_server, lim.max_streams_per_server);
        lim.intent_entries_per_connection = @max(lim.intent_entries_per_connection, try lim.intentEntriesMin());
        var trap: TrapHop = undefined;
        try openTrapHopLimits(&trap, std.testing.allocator, std.testing.io, &.{}, lim);
        defer trap.close();
        const conn = &trap.hop.conn;
        try std.testing.expectEqual(m + 3, try lim.terminalFrames());
        try std.testing.expectEqual(m + 3, conn.sched.terminal_q.len);
        try std.testing.expectEqual(m + 3, conn.sched.ctrl.terminal_entries);
        try std.testing.expectEqual(m + 3, conn.terminal_store.capacity);
        // Bytes: M RSTs and three GOAWAYs, nothing else.
        try std.testing.expectEqual(m * frame.rst_stream_frame_bytes + 3 * frame.goaway_frame_bytes, conn.sched.ctrl.terminal_bytes);
        try std.testing.expectEqual(try lim.terminalBytes(), conn.sched.ctrl.terminal_bytes);
        // The ordinary share is the config value, whatever T is.
        try std.testing.expectEqual(lim.control_entries_per_connection, conn.sched.ctrl.max_entries);
        try std.testing.expectEqual(lim.control_bytes_per_connection, conn.sched.ctrl.max_bytes);
    }
}

/// Limits for M streams per connection, with the server-wide caps and the
/// intent queue raised to what boot asks for M. At the default M they are the
/// defaults, untouched, so the tests check the default intent queue too.
fn limitsForStreams(m: usize) !limits_mod.Limits {
    var lim = limits_mod.Limits.defaults;
    if (m == lim.max_streams_per_connection) return lim;
    lim.max_streams_per_connection = m;
    lim.max_streams_per_server = @max(lim.max_streams_per_server, m);
    lim.cancellation_reaper_jobs = @max(lim.cancellation_reaper_jobs, lim.max_streams_per_server);
    lim.outbound_slabs_per_server = @max(lim.outbound_slabs_per_server, lim.max_streams_per_server);
    lim.intent_entries_per_connection = @max(lim.intent_entries_per_connection, try lim.intentEntriesMin());
    return lim;
}

/// Feed `bytes` through the edge's own ingest loop, as one read chunk.
fn trapIngest(conn: *Connection, bytes: []const u8) !void {
    conn.lockSessionUncancelable(conn.config.io);
    defer conn.unlockSession(conn.config.io);
    try conn.ingestAndProcess(bytes);
}

/// What the server wrote, frame by frame, after the actor's turns so far.
const TrapWire = struct {
    rst_per_sid: std.AutoHashMap(u31, usize),
    rst: usize = 0,
    goaway_codes: [4]u32 = @splat(0),
    goaways: usize = 0,
    settings_acks: usize = 0,
    ping_acks: [4][8]u8 = undefined,
    ping_ack_n: usize = 0,
    conn_credit: u64 = 0,

    fn deinit(self: *TrapWire) void {
        self.rst_per_sid.deinit();
    }
};

/// Emit everything the scheduler holds and read it off the write queue, with
/// no WritePump: drain, parse, complete, until nothing more comes.
fn trapCollect(trap: *TrapHop, out: *TrapWire) !void {
    const conn = &trap.hop.conn;
    while (true) {
        {
            conn.lockSessionUncancelable(conn.config.io);
            defer conn.unlockSession(conn.config.io);
            try conn.drainEmit();
        }
        var took = false;
        while (io_queue.tryGet(wire_pump.WireChunk, &conn.write_ch, conn.config.io)) |chunk| {
            took = true;
            var rest = chunk.bytes[0..chunk.len];
            while (rest.len >= frame.FRAME_HEADER_LEN) {
                const h = frame.FrameHeader.decode(rest[0..frame.FRAME_HEADER_LEN]);
                const body = rest[frame.FRAME_HEADER_LEN..][0..h.length];
                switch (h.type) {
                    .rst_stream => {
                        out.rst += 1;
                        const e = try out.rst_per_sid.getOrPutValue(h.stream_id, 0);
                        e.value_ptr.* += 1;
                    },
                    .goaway => {
                        if (out.goaways < out.goaway_codes.len) out.goaway_codes[out.goaways] = std.mem.readInt(u32, body[4..8], .big);
                        out.goaways += 1;
                    },
                    .settings => if (h.flags.end_stream) {
                        out.settings_acks += 1;
                    },
                    .ping => if (h.flags.end_stream) {
                        if (out.ping_ack_n < out.ping_acks.len) out.ping_acks[out.ping_ack_n] = body[0..8].*;
                        out.ping_ack_n += 1;
                    },
                    .window_update => if (h.stream_id == 0) {
                        out.conn_credit += std.mem.readInt(u32, body[0..4], .big) & 0x7fff_ffff;
                    },
                    else => {},
                }
                rest = rest[frame.FRAME_HEADER_LEN + h.length ..];
            }
            if (wire_pump.postsCompletion(chunk)) conn.acks.sub();
            trap.hop.completeLocalWrite(chunk);
        }
        if (!took) return;
    }
}

fn refuseEveryAdmit(_: *anyopaque) bool {
    return false;
}

/// M streams opened in one read chunk, every one refused by the server-wide
/// admission (`tryAdmit`): M RSTs, 2M intents from one chunk.
fn refusedBurstCase(m: usize) !void {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, try limitsForStreams(m));
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    conn.session.stream_hooks.?.tryAdmit = refuseEveryAdmit;

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    for (0..m) |i| {
        var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
        try wire.appendSlice(gpa, hopRequestFrame(&req, @intCast(1 + 2 * i)));
    }
    try trapIngest(conn, wire.items);
    try std.testing.expect(conn.session.terminal == .none);
    try std.testing.expect(!conn.writer_failed.load(.acquire));

    var seen: TrapWire = .{ .rst_per_sid = .init(gpa) };
    defer seen.deinit();
    try trapCollect(&trap, &seen);
    try std.testing.expectEqual(m, seen.rst);
    for (0..m) |i| try std.testing.expectEqual(@as(?usize, 1), seen.rst_per_sid.get(@intCast(1 + 2 * i)));
    try std.testing.expectEqual(@as(usize, 0), seen.goaways);
}

test "M streams refused by tryAdmit in one read chunk keep the connection: one RST each, no GOAWAY (t-3562)" {
    try refusedBurstCase(limits_mod.Limits.defaults.max_streams_per_connection);
}

test "the same at M = 1024, with the intent queue boot asks for (t-3562)" {
    try refusedBurstCase(1024);
}

test "a rapid reset under the rate limit in one read chunk keeps the connection: ingest stops and drains (t-3562)" {
    // Every pair is a request and the peer's RST_STREAM for it: two intents
    // (the dispatch and the local reset) for 25 bytes, so one 16 KiB chunk
    // holds far more intents than the queue. The rate limit allows 1,000
    // RST_STREAMs at once; 600 stay under it.
    const gpa = std.testing.allocator;
    // A complete handler runs inline, on the actor, so this needs no executor.
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopHundredBytes } },
    }};
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &routes, limits_mod.Limits.defaults);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    const pairs = 600;
    try std.testing.expect(2 * pairs > conn.session.intentCapacity());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    for (0..pairs) |i| {
        const sid: u31 = @intCast(1 + 2 * i);
        var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
        try wire.appendSlice(gpa, hopRequestFrame(&req, sid));
        var rst: [frame.rst_stream_frame_bytes]u8 = undefined;
        _ = try frame.Serializer.rstStream(&rst, sid, .cancel);
        try wire.appendSlice(gpa, &rst);
    }
    try std.testing.expect(wire.items.len <= limits_mod.WIRE_CHUNK_SIZE);
    try trapIngest(conn, wire.items);
    try std.testing.expect(conn.session.terminal == .none);
    try std.testing.expect(!conn.writer_failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), conn.session.intentLen());
    try std.testing.expectEqual(@as(usize, 0), conn.session.active_streams);
    // Requests the ingest stop let through before their reset run inline;
    // let them finish so the connection closes clean.
    try conn.runPendingInline();
    var seen: TrapWire = .{ .rst_per_sid = .init(gpa) };
    defer seen.deinit();
    try trapCollect(&trap, &seen);
    // A peer's reset is never answered with one.
    try std.testing.expectEqual(@as(usize, 0), seen.rst);
    try conn.drainPendingCompleteReceipts(false);
}

/// A graceful shutdown that ends with M streams still open: the finish resets
/// every one of them in one batch (2M intents) after the two graceful GOAWAYs.
fn gracefulFinishCase(m: usize) !void {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, try limitsForStreams(m));
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    {
        conn.lockSessionUncancelable(conn.config.io);
        defer conn.unlockSession(conn.config.io);
        // Open streams whose handlers have not answered (no dispatch: the
        // finish walks the session's map, which is what this needs).
        for (0..m) |i| {
            const sid: u31 = @intCast(1 + 2 * i);
            try conn.session.streams.put(sid, .{ .id = sid, .state = .half_closed_remote, .window = .{ .send = flow.INITIAL_WINDOW } });
            conn.session.active_streams += 1;
            conn.session.highest_peer_stream = sid;
        }
        try conn.session.applyCommand(.graceful_phase1);
        try conn.processIntents();
        try conn.session.applyCommand(.graceful_phase2);
        try conn.processIntents();
        try conn.finishGraceful();
        try std.testing.expect(conn.session.terminal == .none);
    }
    try std.testing.expect(!conn.writer_failed.load(.acquire));
    var seen: TrapWire = .{ .rst_per_sid = .init(gpa) };
    defer seen.deinit();
    try trapCollect(&trap, &seen);
    try std.testing.expectEqual(m, seen.rst);
    for (0..m) |i| try std.testing.expectEqual(@as(?usize, 1), seen.rst_per_sid.get(@intCast(1 + 2 * i)));
    try std.testing.expectEqual(@as(usize, 2), seen.goaways);
    for (seen.goaway_codes[0..2]) |code| try std.testing.expectEqual(@as(u32, @backingInt(frame.ErrorCode.no_error)), code);
}

test "a graceful shutdown with M live streams sends M RSTs and no ENHANCE_YOUR_CALM (t-3562)" {
    try gracefulFinishCase(limits_mod.Limits.defaults.max_streams_per_connection);
    try gracefulFinishCase(1024);
}

test "a ticket travels with its own frame: a leftover HEADERS does not take the next handler's ticket (t-3562)" {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, limits_mod.Limits.defaults);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    const a = try conn.reserveTicket();
    const b = try conn.reserveTicket();
    {
        conn.lockSessionUncancelable(conn.config.io);
        defer conn.unlockSession(conn.config.io);
        for ([_]u31{ 1, 3 }) |sid| {
            try conn.session.streams.put(sid, .{ .id = sid, .state = .half_closed_remote, .window = .{ .send = flow.INITIAL_WINDOW } });
            conn.session.active_streams += 1;
            conn.session.highest_peer_stream = sid;
        }
        // Handler A's HEADERS, left queued (what a park in the middle of a
        // command used to do), then handler B's empty response.
        try conn.session.applyCommand(.{ .respond_headers = .{ .stream_id = 1, .status = 200, .headers = &.{}, .end_stream = false, .ticket = .{ .id = a[0], .slot = a[1] } } });
        try conn.session.applyCommand(.{ .respond_headers = .{ .stream_id = 3, .status = 204, .headers = &.{}, .end_stream = true, .ticket = .{ .id = b[0], .slot = b[1] } } });
        try conn.materializeIntents();
        // Each scheduler entry carries its own stream's ticket.
        var seen: usize = 0;
        var k: usize = 0;
        while (k < conn.sched.ordinary_len) : (k += 1) {
            const e = conn.sched.ordinary_q[(conn.sched.ordinary_head + k) % conn.sched.ordinary_q.len];
            const sid = std.mem.readInt(u32, e.payload[5..9], .big) & 0x7fff_ffff;
            if (sid == 1) {
                try std.testing.expectEqual(a[0], e.ticket);
                seen += 1;
            }
            if (sid == 3) {
                try std.testing.expectEqual(b[0], e.ticket);
                seen += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 2), seen);
    }
    // Written: both tickets complete.
    var wire: TrapWire = .{ .rst_per_sid = .init(gpa) };
    defer wire.deinit();
    try trapCollect(&trap, &wire);
    try std.testing.expect(conn.tickets.isSignaled(a[1]));
    try std.testing.expect(conn.tickets.isSignaled(b[1]));
    try conn.drainPendingCompleteReceipts(false);
}

test "a header block is admitted whole: one ordinary entry left and a two-fragment response refuse the block, reset its stream once, and keep the connection (t-3562)" {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, limits_mod.Limits.defaults);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    // Pin the ordinary share to one free entry.
    const caps = conn.sched.ctrl.ordinaryCaps();
    var pinned: usize = 0;
    while (conn.sched.ctrl.entries_held.load(.acquire) + 1 < caps.entries) : (pinned += 1) {
        try std.testing.expect(conn.sched.ctrl.tryReserveOrdinary(1));
    }
    // A response header block over one frame: HEADERS plus a CONTINUATION.
    const wide: [20 * 1024]u8 = @splat('w');
    const fields_wide = [_]hpack.HeaderField{.{ .name = "x-wide", .value = &wide }};
    {
        conn.lockSessionUncancelable(conn.config.io);
        defer conn.unlockSession(conn.config.io);
        try conn.session.streams.put(1, .{ .id = 1, .state = .half_closed_remote, .window = .{ .send = flow.INITIAL_WINDOW } });
        conn.session.active_streams += 1;
        conn.session.highest_peer_stream = 1;
        try conn.session.applyCommand(.{ .respond_headers = .{ .stream_id = 1, .status = 200, .headers = &fields_wide, .end_stream = false, .ticket = .none } });
        try std.testing.expectEqual(@as(usize, 2), conn.session.intentLen());
        try conn.materializeIntents();
        // No fragment went in: the one free entry is still free.
        try std.testing.expectEqual(@as(usize, 0), conn.sched.ordinary_len);
        try std.testing.expectEqual(caps.entries - 1, conn.sched.ctrl.entries_held.load(.acquire));
        try std.testing.expect(!conn.sched.block_open);
    }
    try std.testing.expect(!conn.writer_failed.load(.acquire));
    try std.testing.expect(conn.session.terminal == .none);
    conn.sched.ctrl.release(pinned, pinned);
    var seen: TrapWire = .{ .rst_per_sid = .init(gpa) };
    defer seen.deinit();
    try trapCollect(&trap, &seen);
    try std.testing.expectEqual(@as(usize, 1), seen.rst);
    try std.testing.expectEqual(@as(?usize, 1), seen.rst_per_sid.get(1));
    try std.testing.expectEqual(@as(usize, 0), seen.goaways);
}

test "acks the edge refuses are owed and sent once the share has room (t-3562)" {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, limits_mod.Limits.defaults);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    // The whole ordinary share is held, as by a peer that does not read.
    const caps = conn.sched.ctrl.ordinaryCaps();
    const pinned = caps.entries - conn.sched.ctrl.entries_held.load(.acquire);
    for (0..pinned) |_| try std.testing.expect(conn.sched.ctrl.tryReserveOrdinary(1));
    const refused_0 = test_ctrl_ordinary_pool_full.load(.monotonic);

    var wire: [9 + 17 + 17]u8 = undefined;
    (frame.FrameHeader{ .length = 0, .type = .settings, .flags = .{}, .stream_id = 0 }).encode(wire[0..9]);
    _ = try frame.Serializer.ping(wire[9..26], false, "owed-p-1");
    _ = try frame.Serializer.ping(wire[26..43], false, "owed-p-2");
    try trapIngest(conn, &wire);
    // All three acks were refused, and the connection goes on.
    try std.testing.expectEqual(refused_0 + 3, test_ctrl_ordinary_pool_full.load(.monotonic));
    try std.testing.expect(!conn.writer_failed.load(.acquire));
    try std.testing.expect(conn.session.terminal == .none);
    try std.testing.expectEqual(@as(usize, 1), conn.session.owed.settings_acks);
    try std.testing.expectEqual(@as(usize, 2), conn.session.owed.ping_len);

    // The share frees (a write completion), and the actor's next section
    // queues what is owed.
    conn.sched.ctrl.release(pinned, pinned);
    {
        conn.lockSessionUncancelable(conn.config.io);
        defer conn.unlockSession(conn.config.io);
        try conn.materializeIntents();
    }
    try std.testing.expect(!conn.session.owed.any());
    var seen: TrapWire = .{ .rst_per_sid = .init(gpa) };
    defer seen.deinit();
    try trapCollect(&trap, &seen);
    try std.testing.expectEqual(@as(usize, 1), seen.settings_acks);
    try std.testing.expectEqual(@as(usize, 2), seen.ping_ack_n);
    try std.testing.expectEqualSlices(u8, "owed-p-1", &seen.ping_acks[0]);
    try std.testing.expectEqualSlices(u8, "owed-p-2", &seen.ping_acks[1]);
    try std.testing.expectEqual(@as(usize, 0), seen.goaways);
}

/// Frame hooks for the error-exit tests: refuse every ordinary frame, or
/// only the final GOAWAY (a broken invariant, forced), and build the rest
/// the usual way.
fn rentNoOrdinary(ctx: *anyopaque, n: usize, class: session_mod.FrameClass) ?[]u8 {
    if (class == .ordinary) return null;
    const c: *Connection = @ptrCast(@alignCast(ctx));
    return c.rentFrame(n, class);
}

fn rentNoFinalGoaway(ctx: *anyopaque, n: usize, class: session_mod.FrameClass) ?[]u8 {
    if (class == .final_goaway) return null;
    const c: *Connection = @ptrCast(@alignCast(ctx));
    return c.rentFrame(n, class);
}

/// Materialize and collect until nothing is owed: the actor's turns after
/// write completions.
fn trapSettle(trap: *TrapHop, out: *TrapWire) !void {
    const conn = &trap.hop.conn;
    var turns: usize = 0;
    while (true) : (turns += 1) {
        if (turns > 64) return error.NeverSettled;
        {
            conn.lockSessionUncancelable(conn.config.io);
            defer conn.unlockSession(conn.config.io);
            try conn.materializeIntents();
        }
        try trapCollect(trap, out);
        if (!conn.session.owed.any()) return;
    }
}

test "nine PINGs in one read with two frame slabs: the acks that find no frame are owed, not an error, and all nine arrive in order (t-3562)" {
    const gpa = std.testing.allocator;
    var lim = limits_mod.Limits.defaults;
    lim.frame_slabs_per_connection = 2;
    lim.inbound_wire_chunks_per_connection = wire_const.POP_PUSHES + 2;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, lim);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    const refused_0 = outbound_memory.frames_refused.load(.monotonic);
    var wire: [9 * 17]u8 = undefined;
    for (0..9) |i| {
        var payload: [8]u8 = "ping-0-x".*;
        payload[5] = '0' + @as(u8, @intCast(i));
        _ = try frame.Serializer.ping(wire[i * 17 ..][0..17], false, &payload);
    }
    try trapIngest(conn, &wire);
    // The instrument fired: some acks found no frame buffer.
    try std.testing.expect(outbound_memory.frames_refused.load(.monotonic) > refused_0);
    try std.testing.expect(conn.session.owed.ping_len > 0);
    try std.testing.expect(conn.session.terminal == .none);
    try std.testing.expect(!conn.writer_failed.load(.acquire));
    var seen: TrapWire = .{ .rst_per_sid = .init(gpa) };
    defer seen.deinit();
    try trapSettle(&trap, &seen);
    try std.testing.expectEqual(@as(usize, 9), seen.ping_ack_n);
    // TrapWire keeps the first four payloads: in the order the PINGs came.
    for (0..4) |i| try std.testing.expectEqual('0' + @as(u8, @intCast(i)), seen.ping_acks[i][5]);
    try std.testing.expectEqual(@as(usize, 0), seen.goaways);
}

test "graceful phase 1 with no ordinary frame left: the GOAWAY goes, the PING is dropped, and nothing is left queued (t-3562)" {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, limits_mod.Limits.defaults);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    conn.session.stream_hooks.?.rentFrame = rentNoOrdinary;
    {
        conn.lockSessionUncancelable(conn.config.io);
        defer conn.unlockSession(conn.config.io);
        try conn.session.applyCommand(.graceful_phase1);
        try conn.processIntents();
    }
    try std.testing.expect(conn.session.grace_phase == .phase1);
    try std.testing.expect(conn.session.terminal == .none);
    var seen: TrapWire = .{ .rst_per_sid = .init(gpa) };
    defer seen.deinit();
    try trapCollect(&trap, &seen);
    try std.testing.expectEqual(@as(usize, 1), seen.goaways);
    try std.testing.expectEqual(@as(u32, @backingInt(frame.ErrorCode.no_error)), seen.goaway_codes[0]);
}

test "an early 413 with no frame for its response resets the stream instead of failing the read (t-3562)" {
    const gpa = std.testing.allocator;
    var lim = limits_mod.Limits.defaults;
    lim.request_body_bytes = 1024;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, lim);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    conn.session.stream_hooks.?.rentFrame = rentNoOrdinary;
    var block: [64]u8 = undefined;
    const fields_post = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "content-length", .value = "4096" },
    };
    const parts = [_][]const hpack.HeaderField{&fields_post};
    const block_len = try hpack.Encoder.encodedLen(&fields_post);
    std.debug.assert(block_len <= block.len);
    _ = hpack.Encoder.encodeRange(block[0..block_len], &parts, 0);
    var wire: [frame.FRAME_HEADER_LEN + 64]u8 = undefined;
    (frame.FrameHeader{ .length = @intCast(block_len), .type = .headers, .flags = .{ .end_headers = true }, .stream_id = 1 }).encode(wire[0..frame.FRAME_HEADER_LEN]);
    @memcpy(wire[frame.FRAME_HEADER_LEN..][0..block_len], block[0..block_len]);
    try trapIngest(conn, wire[0 .. frame.FRAME_HEADER_LEN + block_len]);
    try std.testing.expect(conn.session.terminal == .none);
    var seen: TrapWire = .{ .rst_per_sid = .init(gpa) };
    defer seen.deinit();
    try trapCollect(&trap, &seen);
    try std.testing.expectEqual(@as(?usize, 1), seen.rst_per_sid.get(1));
    try std.testing.expectEqual(@as(usize, 0), seen.goaways);
}

test "an error exit from ingest materializes what the read queued before the unlock (t-3562)" {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, limits_mod.Limits.defaults);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    // A PING (its ack is queued), then DATA on stream 0, a connection error
    // whose final GOAWAY is refused: `ingestSome` returns an error with the
    // ack still in the session queue. Forced: the store keeps a slot for that
    // GOAWAY, so this is the path of any error, not a reachable refusal.
    conn.session.stream_hooks.?.rentFrame = rentNoFinalGoaway;
    var wire: [17 + frame.FRAME_HEADER_LEN]u8 = undefined;
    _ = try frame.Serializer.ping(wire[0..17], false, "err-exit");
    (frame.FrameHeader{ .length = 0, .type = .data, .flags = .{}, .stream_id = 0 }).encode(wire[17..][0..frame.FRAME_HEADER_LEN]);
    const ordinary_0 = conn.sched.ordinary_len;
    // The unlock in `trapIngest` panics in a safe build if the ack is left.
    try std.testing.expectError(error.OutOfMemory, trapIngest(conn, &wire));
    try std.testing.expectEqual(@as(usize, 0), conn.session.intentLen());
    try std.testing.expectEqual(ordinary_0 + 1, conn.sched.ordinary_len);
}

test "a failed round does not stop the next: intents a refusal queued are materialized before the unlock (t-3562)" {
    const gpa = std.testing.allocator;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, limits_mod.Limits.defaults);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    // Both shares held. The terminal one is forced: the terminal store, ring
    // and share hold the same T, so a terminal frame the store built always
    // finds a share entry; holding the share alone stands for any round that
    // fails while the session queue has more to give.
    const caps = conn.sched.ctrl.ordinaryCaps();
    const pinned = caps.entries - conn.sched.ctrl.entries_held.load(.acquire);
    for (0..pinned) |_| try std.testing.expect(conn.sched.ctrl.tryReserveOrdinary(1));
    var t_pinned: usize = 0;
    while (conn.sched.ctrl.tryReserveTerminal(1)) t_pinned += 1;
    defer {
        conn.sched.ctrl.release(pinned, pinned);
        for (0..t_pinned) |_| conn.sched.ctrl.releaseTerminal(1);
    }
    {
        conn.lockSessionUncancelable(conn.config.io);
        // The unlock panics in a safe build if a round was skipped.
        defer conn.unlockSession(conn.config.io);
        for ([_]u31{ 1, 3 }) |sid| {
            try conn.session.streams.put(sid, .{ .id = sid, .state = .half_closed_remote, .window = .{ .send = flow.INITIAL_WINDOW } });
            conn.session.active_streams += 1;
            conn.session.highest_peer_stream = sid;
        }
        // Round 1: stream 1's final HEADERS is refused (its RST goes to the
        // session queue, for round 2), then stream 3's RST fails the round.
        try conn.session.applyCommand(.{ .respond_headers = .{ .stream_id = 1, .status = 204, .headers = &.{}, .end_stream = true, .ticket = .none } });
        try conn.session.applyCommand(.{ .reset_stream = .{ .stream_id = 3, .code = .cancel } });
        try std.testing.expectError(error.OutOfMemory, conn.materializeIntents());
        try std.testing.expectEqual(@as(usize, 0), conn.session.intentLen());
    }
    try std.testing.expect(conn.writer_failed.load(.acquire));
}

/// M server RSTs wait in the terminal ring (the drain is held), then one
/// read chunk carries the peer's RSTs for 3 of those streams and 3 new
/// streams that `tryAdmit` refuses. The crossed RSTs must free their slots
/// during the ingest, or the 3rd refusal finds none.
fn crossedInOneChunkCase(tombstones: usize) !void {
    const gpa = std.testing.allocator;
    var lim = limits_mod.Limits.defaults;
    lim.stream_tombstones = tombstones;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, lim);
    defer trap.close();
    const conn = &trap.hop.conn;
    try trap.hop.preparePreface();
    test_hold_emit.store(true, .release);
    defer test_hold_emit.store(false, .release);
    const m = lim.max_streams_per_connection;
    {
        conn.lockSessionUncancelable(conn.config.io);
        defer conn.unlockSession(conn.config.io);
        for (0..m) |i| {
            const sid: u31 = @intCast(1 + 2 * i);
            try conn.session.streams.put(sid, .{ .id = sid, .state = .half_closed_remote, .window = .{ .send = flow.INITIAL_WINDOW } });
            conn.session.active_streams += 1;
            conn.session.highest_peer_stream = sid;
        }
        for (0..m) |i| try conn.session.applyCommand(.{ .reset_stream = .{ .stream_id = @intCast(1 + 2 * i), .code = .cancel } });
        try conn.materializeIntents();
    }
    try std.testing.expectEqual(m, conn.sched.terminal_len);
    conn.session.stream_hooks.?.tryAdmit = refuseEveryAdmit;

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    for ([_]u31{ 1, 3, 5 }) |sid| {
        var rst: [frame.rst_stream_frame_bytes]u8 = undefined;
        _ = try frame.Serializer.rstStream(&rst, sid, .cancel);
        try wire.appendSlice(gpa, &rst);
    }
    for (0..3) |i| {
        var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
        try wire.appendSlice(gpa, hopRequestFrame(&req, @intCast(1 + 2 * (m + i))));
    }
    try trapIngest(conn, wire.items);
    try std.testing.expect(conn.session.terminal == .none);
    // M - 3 old RSTs and the 3 refusals.
    try std.testing.expectEqual(m, conn.sched.terminal_len);
    for ([_]u31{ 1, 3, 5 }) |sid| {
        var k: usize = 0;
        while (k < conn.sched.terminal_len) : (k += 1) {
            const e = conn.sched.terminal_q[(conn.sched.terminal_head + k) % conn.sched.terminal_q.len];
            try std.testing.expect(std.mem.readInt(u32, e.payload[5..9], .big) & 0x7fff_ffff != sid);
        }
    }
}

test "crossed RSTs free their terminal slots during the ingest of the same read chunk (t-3562)" {
    try crossedInOneChunkCase(limits_mod.Limits.defaults.stream_tombstones);
}

test "the same when the streams' tombstones were evicted (t-3562)" {
    try crossedInOneChunkCase(2);
}

test "a limit that breaks the terminal bound is InvalidConfig, not a small wrong store (t-3562)" {
    const top = std.math.maxInt(usize);
    for ([_]usize{ top, top - 1, top - 2 }) |m| {
        var lim = limits_mod.Limits.defaults;
        lim.max_streams_per_connection = m;
        try std.testing.expectError(error.InvalidConfig, lim.terminalFrames());
        try std.testing.expectError(error.InvalidConfig, lim.slotBootBytes(.h2c));
        try std.testing.expectError(error.InvalidConfig, lim.slotBootBytes(.tls));
        try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
    }
    // M * 13 overflows while M + 3 does not.
    var lim = limits_mod.Limits.defaults;
    lim.max_streams_per_connection = top / 13 + 1;
    _ = try lim.terminalFrames();
    try std.testing.expectError(error.InvalidConfig, lim.terminalBytes());
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "leaseWriteChunk refuses an empty pool and an oversized chunk, and counts the refusal" {
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, std.testing.allocator, std.testing.io);
    defer trap.close();
    const conn = &trap.hop.conn;
    const refused_before = outbound_memory.chunks_refused.load(.monotonic);
    const leased_before = outbound_memory.chunks_leased.load(.monotonic);
    var leases: [64]Connection.WriteLease = undefined;
    var n: usize = 0;
    defer for (leases[0..n]) |l| conn.returnWriteChunk(l);
    while (conn.write_pool.freeCount() > 0) {
        leases[n] = try conn.leaseWriteChunk(16);
        n += 1;
    }
    try std.testing.expectEqual(leased_before + n, outbound_memory.chunks_leased.load(.monotonic));
    try std.testing.expectError(error.OutOfMemory, conn.leaseWriteChunk(16));
    try std.testing.expectEqual(refused_before + 1, outbound_memory.chunks_refused.load(.monotonic));
    conn.returnWriteChunk(leases[n - 1]);
    n -= 1;
    try std.testing.expectError(error.OutOfMemory, conn.leaseWriteChunk(limits_mod.WIRE_CHUNK_SIZE + 1));
}

test "recordFinalize first call stores 1" {
    var slot: HandlerSlot = .{};
    slot.in_use = true;
    Connection.recordFinalize(&slot);
    try std.testing.expectEqual(@as(u8, 1), slot.finalize_count.load(.acquire));
}

/// A small request pool for the tests that need a Connection and make no
/// requests of their own.
fn testRequestPool() !request_pool.RequestPool {
    return request_pool.RequestPool.init(.{ .server_bytes = 16 * 1024 * 1024, .arena_bytes = 4 * 1024 * 1024, .retain_bytes = 16 * 1024 * 1024 });
}

const TrapHop = struct {
    hop: BenchHop,
    peer: std.Io.net.Stream,
    slabs: slab_pool.SlabPool,
    requests: request_pool.RequestPool,
    gpa: std.mem.Allocator,
    io: std.Io,

    fn close(self: *TrapHop) void {
        self.hop.deinit();
        self.peer.close(self.io);
        self.slabs.deinit(self.gpa);
        self.requests.deinit();
    }
};

fn openTrapHop(out: *TrapHop, gpa: std.mem.Allocator, io: std.Io) !void {
    return openTrapHopRoutes(out, gpa, io, &.{});
}

fn openTrapHopRoutes(out: *TrapHop, gpa: std.mem.Allocator, io: std.Io, routes: []const router_mod.Route) !void {
    return openTrapHopLimits(out, gpa, io, routes, limits_mod.Limits.defaults);
}

fn openTrapHopLimits(out: *TrapHop, gpa: std.mem.Allocator, io: std.Io, routes: []const router_mod.Route, limits: limits_mod.Limits) !void {
    out.gpa = gpa;
    out.io = io;
    out.slabs = try slab_pool.SlabPool.init(gpa, io, limits_mod.Limits.defaults.outbound_bytes_per_stream, 8);
    errdefer out.slabs.deinit(gpa);
    out.requests = try testRequestPool();
    errdefer out.requests.deinit();
    const bind = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try bind.listen(io, .{ .reuse_address = true });
    const dest = listener.socket.address;
    const peer = dest.connect(io, .{ .mode = .stream }) catch |err| {
        listener.socket.close(io);
        return err;
    };
    const accepted = listener.accept(io) catch |err| {
        peer.close(io);
        listener.socket.close(io);
        return err;
    };
    listener.socket.close(io);
    out.peer = peer;
    out.hop.open(accepted, .{
        .io = io,
        .mode = .h2c,
        .limits = limits,
        .router = .{ .routes = routes },
        .gpa = gpa,
        .slab_pool = &out.slabs,
        .request_pool = &out.requests,
    }) catch |err| {
        peer.close(io);
        return err;
    };
}

test "lease lookup does not scan: after a burst that touched every lease, a find costs a probe or two" {
    const gpa = std.testing.allocator;
    var lim = limits_mod.Limits.defaults;
    lim.max_streams_per_connection = 8;
    var trap: TrapHop = undefined;
    try openTrapHopLimits(&trap, gpa, std.testing.io, &.{}, lim);
    defer trap.close();
    const conn = &trap.hop.conn;
    const n = conn.request_leases.capacity();
    try std.testing.expectEqual(@as(usize, 2 * 8 + 1), n);

    // The burst: every lease busy, then every lease released.
    var sid: u31 = 1;
    for (0..n) |_| {
        _ = conn.newLease(sid) orelse return error.LeaseTableTooSmall;
        sid += 2;
    }
    try std.testing.expectEqual(n, conn.request_leases.touched);
    try std.testing.expect(conn.newLease(sid) == null);
    var id: u31 = 1;
    for (0..n) |_| {
        conn.releaseLease(conn.findLease(id).?);
        id += 2;
    }
    // Free leases come from the stack: nothing more is touched.
    try std.testing.expectEqual(n, conn.lease_index.free_len);

    // Now a few requests are live among the touched leases, against a model.
    var model = std.AutoHashMap(u31, void).init(gpa);
    defer model.deinit();
    var rng = std.Random.DefaultPrng.init(0x1ea5e);
    var next: u31 = 1001;
    var lookups: u64 = 0;
    test_lease_probes.store(0, .monotonic);
    for (0..4000) |_| {
        if (model.count() < n and (model.count() == 0 or rng.random().boolean())) {
            _ = conn.newLease(next) orelse return error.LeaseTableTooSmall;
            try model.put(next, {});
            next += 2;
        } else {
            var it = model.keyIterator();
            var pick = rng.random().uintLessThan(usize, model.count());
            while (it.next()) |k| : (pick -|= 1) {
                if (pick == 0) {
                    const k_id = k.*;
                    conn.releaseLease(conn.findLease(k_id).?);
                    _ = model.remove(k_id);
                    break;
                }
            }
        }
        // Every live id is found, and a released one is not.
        var alive = model.keyIterator();
        while (alive.next()) |k| {
            lookups += 1;
            try std.testing.expectEqual(k.*, conn.findLease(k.*).?.stream_id);
        }
        lookups += 1;
        try std.testing.expect(conn.findLease(next + 2) == null);
    }
    try std.testing.expectEqual(n, conn.request_leases.touched);
    const probes = test_lease_probes.load(.monotonic);
    std.debug.print("lease lookups: {d} lookups made {d} probes over {d} leases (touched {d})\n", .{ lookups, probes, n, conn.request_leases.touched });
    // A scan of the touched leases would cost about n / 2 per hit; the table
    // costs about one. Allow three.
    try std.testing.expect(probes <= 3 * lookups);
    var left = model.keyIterator();
    while (left.next()) |k| conn.releaseLease(conn.findLease(k.*).?);
}

/// Axis E arm: two `finishHandlerJob` calls on one slot admitted through
/// `admitHandler`. The second must panic in `recordFinalize`.
pub fn testTrapDoubleFinalize(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    defer trap.close();
    const slot = trap.hop.conn.allocSlot(1) orelse std.debug.panic("trap allocSlot failed", .{});
    const i = trap.hop.conn.slotIndex(1) orelse std.debug.panic("trap slotIndex failed", .{});
    const job = &trap.hop.conn.handler_jobs[i];
    job.conn = &trap.hop.conn;
    job.arena = &trap.hop.conn.borrowTestLease(1).arena;
    job.task_counted = false;
    job.stream_id = 1;
    job.slot = slot;
    const body = job.arena.allocator().dupe(u8, "trap-body") catch std.debug.panic("trap body dupe failed", .{});
    job.owned_request = .{
        .stream_id = 1,
        .method = "GET",
        .scheme = "http",
        .authority = "",
        .path = "/",
        .query = "",
        .headers = &.{},
        .body = body,
        .trailers = &.{},
    };
    job.hctx.encoder = null;
    trap.hop.conn.admitHandler(slot);
    Connection.finishHandlerJob(job);
    Connection.finishHandlerJob(job);
}

/// Axis B arm: bind the sweep TaskLocal, then `lockSessionUncancelable`.
pub fn testTrapLockDuringShutdownSweep(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    defer trap.close();
    var node: @TypeOf(shutdown_sweep).Node = .unset;
    shutdown_sweep.set(&node, {});
    defer shutdown_sweep.clear(&node);
    trap.hop.conn.lockSessionUncancelable(trap.hop.conn.config.io);
}

/// Axis B arm: bind the sweep TaskLocal, then `lockSession`.
pub fn testTrapLockSessionDuringShutdownSweep(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    defer trap.close();
    var node: @TypeOf(shutdown_sweep).Node = .unset;
    shutdown_sweep.set(&node, {});
    defer shutdown_sweep.clear(&node);
    _ = trap.hop.conn.lockSession() catch unreachable;
}

/// Axis A arm: `wakeHandlerWaiters` with `session_mu` not held.
pub fn testTrapUnlockedWakeHandlerWaiters(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    defer trap.close();
    std.debug.assert(!trap.hop.conn.session_held);
    trap.hop.conn.wakeHandlerWaiters(1);
}

fn trapAdmitJob(conn: *Connection, sid: u31) *HandlerJob {
    const slot = conn.allocSlot(sid) orelse std.debug.panic("trap allocSlot failed", .{});
    std.debug.assert(slot.in_use);
    std.debug.assert(!slot.admitted);
    const i = conn.slotIndex(sid) orelse std.debug.panic("trap slotIndex failed", .{});
    const job = &conn.handler_jobs[i];
    job.conn = conn;
    job.arena = &conn.borrowTestLease(sid).arena;
    job.task_counted = false;
    job.stream_id = sid;
    job.slot = slot;
    job.owned_request = .{
        .stream_id = sid,
        .method = "GET",
        .scheme = "http",
        .authority = "",
        .path = "/",
        .query = "",
        .headers = &.{},
        .body = &.{},
        .trailers = &.{},
    };
    job.hctx = .{
        .conn = conn,
        .terminal = &slot.terminal,
        .slot = slot,
        .stream_id = sid,
        .arena = job.arena.allocator(),
    };
    conn.admitHandler(slot);
    std.debug.assert(slot.admitted);
    return job;
}

/// Axis D arm: one admitted slot never posts. `waitTeardownCompletions` is
/// the production wait; `shutdownHandlers` would hit `assertTeardownPosters`
/// first on a join-null slot and would not arm the watchdog.
pub fn testTrapWatchdogNoProgress(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    const slot = trap.hop.conn.allocSlot(1) orelse std.debug.panic("trap allocSlot failed", .{});
    trap.hop.conn.admitHandler(slot);
    std.debug.assert(slot.in_use);
    std.debug.assert(trap.hop.conn.live_handlers.load(.acquire) == 1);
    var node: @TypeOf(shutdown_sweep).Node = .unset;
    shutdown_sweep.set(&node, {});
    defer shutdown_sweep.clear(&node);
    std.debug.assert(shutdown_sweep.get() != null);
    trap.hop.conn.waitTeardownCompletions();
    std.debug.panic("watchdog did not fire", .{});
}

fn trapParkForever() void {
    while (true) {
        // Swallow Canceled so JoinHandle.cancel cannot finish: the next sleep is
        // not a cancel point (zio consumes one Canceled per request).
        zio.sleep(.fromSeconds(30)) catch {};
    }
}

/// Axis D arm: a running reaper whose `JoinHandle.cancel` never returns. A worker
/// must be live: a pool with nobody home only arms `reaper_queued`, not
/// `reaper_running`.
pub fn testTrapWatchdogReaperNoPost(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    var pool = ReaperPool.init(gpa, io, 1) catch std.debug.panic("trap reaper pool failed", .{});
    trap.hop.conn.reaper = &pool;
    _ = zio.spawn(ReaperPool.worker, .{&pool}) catch std.debug.panic("trap reaper worker spawn failed", .{});
    const slot = trap.hop.conn.allocSlot(1) orelse std.debug.panic("trap allocSlot failed", .{});
    trap.hop.conn.admitHandler(slot);
    slot.reaper_reserved = true;
    const i = trap.hop.conn.slotIndex(1) orelse std.debug.panic("trap slotIndex failed", .{});
    std.debug.assert(trap.hop.conn.handler_joins[i] == null);
    const handle = zio.spawnInto(connPlacement(), trapParkForever, .{}) catch std.debug.panic("trap park spawn failed", .{});
    trap.hop.conn.handler_joins[i] = handle;
    std.debug.assert(slot.completion_owner.load(.acquire) == live);
    std.debug.assert(trap.hop.conn.live_handlers.load(.acquire) == 1);
    trap.hop.conn.shutdownHandlers();
    std.debug.panic("reaper watchdog did not fire", .{});
}

var trap_r158_ticket_slot: std.atomic.Value(u32) = .init(ticket_table.no_completion_slot);

fn trapR158ParkedHolder(job: *HandlerJob) void {
    defer Connection.finishHandlerJob(job);
    const conn = job.conn;
    const pair = conn.tickets.reserve() catch return;
    trap_r158_ticket_slot.store(pair[1], .release);
    job.slot.awaiting_receipt.store(true, .release);
    defer job.slot.awaiting_receipt.store(false, .release);
    conn.waitTicket(job.stream_id, pair[1], &job.slot.terminal) catch {};
}

fn trapAssertSweepClean(conn: *Connection) void {
    var live_n: usize = 0;
    var running_n: usize = 0;
    for (conn.handlers.slice()) |s| {
        if (!s.in_use) continue;
        const owner = s.completion_owner.load(.acquire);
        if (owner == live) live_n += 1;
        if (owner == reaper_running) running_n += 1;
    }
    if (live_n != 0) {
        std.debug.panic("r158 parked leftover owner_live={d}", .{live_n});
    }
    if (running_n != 0) {
        std.debug.panic("reaper-stuck leftover reaper_running={d}", .{running_n});
    }
    std.debug.assert(conn.countInUseSlots() == 0);
}

/// A2 gate: a handler parked in `tickets.wait` returns with no JoinHandle.cancel.
/// Ready signal is `TicketTable.isWaiting`. Handshake hang is t-1946, not armed.
pub fn testTrapR158Parked(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    defer trap.close();
    trap_r158_ticket_slot.store(ticket_table.no_completion_slot, .release);
    var pool = ReaperPool.init(gpa, io, 1) catch std.debug.panic("trap reaper pool failed", .{});
    trap.hop.conn.reaper = &pool;
    _ = zio.spawn(ReaperPool.worker, .{&pool}) catch std.debug.panic("trap reaper worker spawn failed", .{});
    const job = trapAdmitJob(&trap.hop.conn, 1);
    job.slot.reaper_reserved = true;
    const i = trap.hop.conn.slotIndex(1) orelse std.debug.panic("trap slotIndex failed", .{});
    const handle = zio.spawnInto(connPlacement(), trapR158ParkedHolder, .{job}) catch std.debug.panic("trap holder spawn failed", .{});
    trap.hop.conn.handler_joins[i] = handle;
    std.debug.assert(job.slot.completion_owner.load(.acquire) == live);
    var spins: usize = 0;
    while (true) {
        const slot_i = trap_r158_ticket_slot.load(.acquire);
        if (slot_i != ticket_table.no_completion_slot and
            trap.hop.conn.tickets.isWaiting(slot_i) and
            trap.hop.conn.countTicketWaiters() == 1 and
            Connection.eventHasWaiters(&trap.hop.conn.dead)) break;
        spins += 1;
        std.debug.assert(spins < 1_000_000);
        zio.yield() catch std.debug.panic("trap yield canceled before wait", .{});
    }
    std.debug.assert(job.slot.awaiting_receipt.load(.acquire));
    std.debug.assert(trap.hop.conn.tickets.isWaiting(trap_r158_ticket_slot.load(.acquire)));
    std.debug.assert(trap.hop.conn.countTicketWaiters() == 1);
    std.debug.assert(Connection.eventHasWaiters(&trap.hop.conn.dead));
    trap.hop.conn.shutdownHandlers();
    std.debug.assert(trap.hop.conn.countTicketWaiters() == 0);
    std.debug.assert(!Connection.eventHasWaiters(&trap.hop.conn.dead));
    trapAssertSweepClean(&trap.hop.conn);
    std.debug.print("r158 parked enroll ok\n", .{});
    std.debug.print("reaper-stuck absent\n", .{});
}

fn trapWatchdogHealthyPoster(jobs: [3]*HandlerJob) void {
    std.debug.assert(jobs.len == 3);
    for (jobs) |job| {
        std.debug.assert(job.slot.in_use);
        std.debug.assert(job.slot.admitted);
        zio.sleep(.fromSeconds(3)) catch std.debug.panic("watchdog healthy poster sleep canceled", .{});
        Connection.finishHandlerJob(job);
    }
}

/// Axis D arm: three completions 3s apart. Total past the old 5s deadline;
/// each gap under the 5s no-progress clock. Teardown must return.
pub fn testTrapWatchdogHealthyProgress(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    defer trap.close();
    const sids = [_]u31{ 1, 3, 5 };
    var jobs: [3]*HandlerJob = undefined;
    for (sids, 0..) |sid, i| {
        jobs[i] = trapAdmitJob(&trap.hop.conn, sid);
    }
    std.debug.assert(trap.hop.conn.live_handlers.load(.acquire) == 3);
    var poster = zio.spawn(trapWatchdogHealthyPoster, .{jobs}) catch std.debug.panic("trap poster spawn failed", .{});
    var node: @TypeOf(shutdown_sweep).Node = .unset;
    shutdown_sweep.set(&node, {});
    defer shutdown_sweep.clear(&node);
    const t0 = nowNs(io);
    trap.hop.conn.waitTeardownCompletions();
    const elapsed = nowNs(io) -% t0;
    std.debug.assert(elapsed > 5 * std.time.ns_per_s);
    _ = poster.join();
    std.debug.assert(trap.hop.conn.countInUseSlots() == 0);
    std.debug.print("teardown no-progress healthy elapsed_ns={d}\n", .{elapsed});
}

/// Axis E/F arm: `claimSlot` with no admit and no rollback. deinit must
/// panic conservation, not a generic `in_use` assert.
pub fn testTrapConservation(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    _ = trap.hop.conn.allocSlot(1) orelse std.debug.panic("trap allocSlot failed", .{});
    std.debug.assert(trap.hop.conn.slots_claimed == 1);
    std.debug.assert(trap.hop.conn.slots_finalized.load(.acquire) == 0);
    std.debug.assert(trap.hop.conn.slots_rolled_back == 0);
    std.debug.assert(trap.hop.conn.live_handlers.load(.acquire) == 0);
    trap.hop.conn.deinit();
    std.debug.panic("conservation did not fire", .{});
}

/// Axis F arm: admitted slot still live at deinit.
pub fn testTrapDeinitLive(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    const slot = trap.hop.conn.allocSlot(1) orelse std.debug.panic("trap allocSlot failed", .{});
    trap.hop.conn.admitHandler(slot);
    std.debug.assert(trap.hop.conn.live_handlers.load(.acquire) == 1);
    trap.hop.conn.deinit();
    std.debug.panic("deinit live_handlers did not fire", .{});
}

fn trapStaleSidPoster(conn: *Connection) void {
    std.debug.assert(conn.handlers.capacity() != 0);
    zio.sleep(.fromMilliseconds(300)) catch std.debug.panic("stale sid poster sleep canceled", .{});
    conn.completion_ch.trySend(1) catch |err| switch (err) {
        error.WouldBlock => std.debug.panic("stale sid trySend WouldBlock", .{}),
        error.Closed => std.debug.panic("stale sid trySend Closed", .{}),
    };
}

/// Finding 2 arm: a duplicate SID after the real release must panic, not
/// refresh last_progress_ns.
pub fn testTrapStaleTeardownSid(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    const job1 = trapAdmitJob(&trap.hop.conn, 1);
    _ = trapAdmitJob(&trap.hop.conn, 3);
    Connection.finishHandlerJob(job1);
    var poster = zio.spawn(trapStaleSidPoster, .{&trap.hop.conn}) catch std.debug.panic("stale sid poster spawn failed", .{});
    var node: @TypeOf(shutdown_sweep).Node = .unset;
    shutdown_sweep.set(&node, {});
    defer shutdown_sweep.clear(&node);
    trap.hop.conn.waitTeardownCompletions();
    _ = poster.join();
    std.debug.panic("stale sid did not fire", .{});
}

/// Finding 5 arm: a complete job in `inline_sids` with `teardown_inline_n == 0`.
/// `teardownExhaustive` must freeze before the lock-free wait.
pub fn testTrapErrorPathFreeze(io: std.Io) void {
    const gpa = std.heap.page_allocator;
    var trap: TrapHop = undefined;
    openTrapHop(&trap, gpa, io) catch std.debug.panic("trap hop open failed", .{});
    defer trap.close();
    _ = trapAdmitJob(&trap.hop.conn, 1);
    std.debug.assert(trap.hop.conn.inline_sids.len != 0);
    trap.hop.conn.inline_sids[0] = 1;
    trap.hop.conn.inline_n = 1;
    std.debug.assert(trap.hop.conn.teardown_inline_n == 0);
    trap.hop.conn.teardownExhaustive();
    std.debug.assert(trap.hop.conn.countInUseSlots() == 0);
    std.debug.print("error-path freeze ok\n", .{});
}

/// A connected socket pair on loopback, for a test that boots a connection
/// without serving it.
const SocketPair = struct {
    accepted: std.Io.net.Stream,
    peer: std.Io.net.Stream,

    fn open(io: std.Io) !SocketPair {
        const bind = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
        var listener = try bind.listen(io, .{ .reuse_address = true });
        defer listener.socket.close(io);
        const peer = try listener.socket.address.connect(io, .{ .mode = .stream });
        errdefer peer.close(io);
        const accepted = try listener.accept(io);
        return .{ .accepted = accepted, .peer = peer };
    }
};

/// Boot one connection into a buffer of exactly `bytes` and end it. Returns
/// how much of the buffer the boot used, or the boot's error.
fn bootIntoBuffer(io: std.Io, lim: limits_mod.Limits, mode: Mode, bytes: usize) !usize {
    const gpa = std.testing.allocator;
    const buffer = try gpa.alignedAlloc(u8, .fromByteUnits(boot_plan.slot_align), bytes);
    defer gpa.free(buffer);
    var fba: std.heap.FixedBufferAllocator = .init(buffer);
    const boot = fba.allocator();

    var slabs = try slab_pool.SlabPool.init(gpa, io, lim.outbound_bytes_per_stream, 8);
    defer slabs.deinit(gpa);
    var requests = try testRequestPool();
    defer requests.deinit();
    // `Connection.init` only checks that a TLS connection has an acceptor.
    var acceptor: tls_edge.Acceptor = undefined;
    const pair = try SocketPair.open(io);
    defer pair.peer.close(io);
    errdefer pair.accepted.close(io);

    // A TLS slot holds the handshake `Conn` first, as `ConnSlot.createTlsConn` does.
    if (mode == .tls) _ = try boot.create(tls_edge.Conn);
    var conn = try Connection.init(boot, pair.accepted, .{
        .io = io,
        .mode = mode,
        .limits = lim,
        .router = .{ .routes = &.{} },
        .tls_acceptor = if (mode == .tls) &acceptor else null,
        .gpa = gpa,
        .slab_pool = &slabs,
        .request_pool = &requests,
    });
    const used = fba.end_index;
    try conn.finishBoot();
    conn.quiesce();
    return used;
}

fn fitLimits(which: enum { defaults, small, large, header_block_below_frame }) limits_mod.Limits {
    var lim = limits_mod.Limits.defaults;
    switch (which) {
        .defaults => {},
        .small => {
            lim.max_streams_per_connection = 4;
            lim.inbound_wire_chunks_per_connection = limits_mod.H2C_MIN_WRITE_QUEUE_CHUNKS;
            lim.frame_slab_bytes = 64;
            lim.frame_slabs_per_connection = 4;
            lim.control_bytes_per_connection = 8 * 1024;
            lim.control_entries_per_connection = 32;
            lim.intent_entries_per_connection = 20;
            lim.stream_tombstones = 3;
        },
        // Sizes that are not powers of two, so the hash maps round up and the
        // alignment padding between tables differs from the defaults.
        .large => {
            lim.max_streams_per_connection = 1000;
            lim.inbound_wire_chunks_per_connection = 24;
            lim.frame_slab_bytes = 700;
            lim.frame_slabs_per_connection = 333;
            lim.control_entries_per_connection = 1999;
            lim.intent_entries_per_connection = 3001;
            lim.stream_tombstones = 5001;
            lim.compressed_header_bytes = 100_001;
        },
        // `compressed_header_bytes` under one frame: the block is still sized
        // for a full first fragment.
        .header_block_below_frame => lim.compressed_header_bytes = 1000,
    }
    return lim;
}

test "slot boot buffer: slotBootBytes is an exact fit for every mode and limits" {
    const io = std.testing.io;
    inline for (.{ .defaults, .small, .large, .header_block_below_frame }) |which| {
        const lim = fitLimits(which);
        inline for (.{ Mode.h2c, Mode.tls }) |mode| {
            const need = try lim.slotBootBytes(mode);
            std.debug.print("slot fit {s} {s}: {d} bytes\n", .{ @tagName(which), @tagName(mode), need });
            // Exactly the planned size boots, and the boot uses all of it.
            try std.testing.expectEqual(need, try bootIntoBuffer(io, lim, mode, need));
            // One byte less cannot hold the last allocation.
            try std.testing.expectError(error.OutOfMemory, bootIntoBuffer(io, lim, mode, need - 1));
        }
    }
}

// ---------------------------------------------------------------------------
// t-3594: a task that is not the actor wakes it by releasing `session_mu`.
//
// These tests are about the boundary, not about any named path. The task below
// changes a value that no ring site writes and calls no `ring`, and the parked
// stand-in for the actor must still wake. A handler path written next year
// passes or fails here exactly as the paths of today do.

/// What the "handler" changes under the lock.
var release_probe: std.atomic.Value(usize) = .init(0);
var stand_in_parked: std.atomic.Value(bool) = .init(false);

const ReleaseWay = enum { cancelable, uncancelable, error_return };

fn releaseThenFail(conn: *Connection) !void {
    try conn.lockSession();
    defer conn.unlockSession(conn.config.io);
    release_probe.store(42, .release);
    return error.Boom;
}

/// The only thing it does to tell the actor is to release the lock.
fn releasingTask(conn: *Connection, way: ReleaseWay) void {
    switch (way) {
        .cancelable => {
            conn.lockSession() catch return;
            defer conn.unlockSession(conn.config.io);
            release_probe.store(42, .release);
        },
        .uncancelable => {
            conn.lockSessionUncancelable(conn.config.io);
            defer conn.unlockSession(conn.config.io);
            release_probe.store(42, .release);
        },
        .error_return => releaseThenFail(conn) catch {},
    }
}

/// A select on the doorbell and a timeout. The timeout is a failure.
fn parkedStandIn(conn: *Connection) !void {
    const limit: zio.Timeout = .{ .duration = .fromMilliseconds(5000) };
    stand_in_parked.store(true, .release);
    const winner = try zio.select(.{
        .bell = conn.doorbell.asyncReceive(),
        .timer = limit,
    });
    switch (winner) {
        .bell => |r| _ = try r,
        .timer => return error.LostWake,
    }
    try std.testing.expectEqual(@as(usize, 42), release_probe.load(.acquire));
}

fn drainDoorbell(conn: *Connection) void {
    while (io_queue.tryRecv(u8, &conn.doorbell)) |_| {}
}

fn releaseWakesScenario(rt: *zio.Runtime, gpa: std.mem.Allocator, way: ReleaseWay) !void {
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, rt.io());
    defer trap.close();
    const conn = &trap.hop.conn;
    drainDoorbell(conn);
    release_probe.store(0, .release);
    stand_in_parked.store(false, .release);

    var actor = try rt.spawn(parkedStandIn, .{conn});
    while (!stand_in_parked.load(.acquire)) try zio.sleep(.fromMilliseconds(1));
    // The stand-in is on its way into the select; give it time to be in it, so
    // that the wake has to reach a parked task. (A ring that came before the
    // park would pass too: the doorbell keeps its token. That is the point of
    // it, and not what this waits for.)
    try zio.sleep(.fromMilliseconds(20));
    var handler = try rt.spawn(releasingTask, .{ conn, way });
    handler.join();
    try actor.join();
}

fn releaseWakesCase(way: ReleaseWay) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(2),
    });
    defer rt.deinit();
    var h = try rt.spawn(releaseWakesScenario, .{ rt, gpa, way });
    try h.join();
}

test "release wake: a release by a task that is not the actor leaves a wake that a parked or parking actor receives (lockSession)" {
    try releaseWakesCase(.cancelable);
}

test "release wake: a release by a task that is not the actor leaves a wake that a parked or parking actor receives (lockSessionUncancelable)" {
    try releaseWakesCase(.uncancelable);
}

test "release wake: a deferred unlock on an error return leaves a wake that a parked or parking actor receives" {
    try releaseWakesCase(.error_return);
}

fn markerScenario(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var trap_a: TrapHop = undefined;
    try openTrapHop(&trap_a, gpa, rt.io());
    defer trap_a.close();
    var trap_b: TrapHop = undefined;
    try openTrapHop(&trap_b, gpa, rt.io());
    defer trap_b.close();
    const a = &trap_a.hop.conn;
    const b = &trap_b.hop.conn;
    const io = rt.io();
    drainDoorbell(a);
    drainDoorbell(b);
    const count_0 = a.releases_rung_max.load(.seq_cst);

    var node: @TypeOf(actor_marker).Node = .unset;

    // Bound to this connection: the task is its actor, and an unlock rings
    // nothing.
    actor_marker.set(&node, a);
    a.lockSessionUncancelable(io);
    a.unlockSession(io);
    actor_marker.clear(&node);
    try std.testing.expectEqual(@as(usize, 0), io_queue.chanLen(u8, &a.doorbell));
    try std.testing.expectEqual(count_0, a.releases_rung_max.load(.seq_cst));

    // Bound to ANOTHER connection: still not this one's actor, so it rings.
    actor_marker.set(&node, b);
    a.lockSessionUncancelable(io);
    a.unlockSession(io);
    actor_marker.clear(&node);
    try std.testing.expectEqual(@as(usize, 1), io_queue.chanLen(u8, &a.doorbell));
    try std.testing.expectEqual(count_0 + 1, a.releases_rung_max.load(.seq_cst));
    // And it rang only a: b was neither locked nor rung.
    try std.testing.expectEqual(@as(usize, 0), io_queue.chanLen(u8, &b.doorbell));
}

test "release wake: an unlock by the actor's own task rings nothing; bound to another connection it rings" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(1),
    });
    defer rt.deinit();
    var h = try rt.spawn(markerScenario, .{ rt, gpa });
    try h.join();
}

fn coalesceScenario(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, rt.io());
    defer trap.close();
    const conn = &trap.hop.conn;
    const io = rt.io();
    drainDoorbell(conn);

    // Repeated releases with the doorbell full return (a ring that blocked
    // would hang this test) and leave one token.
    for (0..5) |_| {
        conn.lockSessionUncancelable(io);
        conn.unlockSession(io);
    }
    try std.testing.expectEqual(@as(usize, 1), io_queue.chanLen(u8, &conn.doorbell));
    // After the token is consumed, the next release makes a fresh one.
    try std.testing.expect(io_queue.tryRecv(u8, &conn.doorbell) != null);
    try std.testing.expectEqual(@as(usize, 0), io_queue.chanLen(u8, &conn.doorbell));
    conn.lockSessionUncancelable(io);
    conn.unlockSession(io);
    try std.testing.expectEqual(@as(usize, 1), io_queue.chanLen(u8, &conn.doorbell));
}

test "release wake: releases coalesce on a full bell and make a fresh token once it is consumed" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(1),
    });
    defer rt.deinit();
    var h = try rt.spawn(coalesceScenario, .{ rt, gpa });
    try h.join();
}

fn selfDispatchScenario(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const routes = [_]router_mod.Route{.{
        .method = .GET,
        .path = "/",
        .handler = .{ .complete = .{ .ptr = undefined, .runFn = hopHundredBytes } },
    }};
    var trap: TrapHop = undefined;
    try openTrapHopRoutes(&trap, gpa, rt.io(), &routes);
    defer trap.close();
    const hop = &trap.hop;
    const conn = &hop.conn;
    const io = rt.io();
    try hop.preparePreface();

    // This task is the actor, as far as `unlockSession` can tell, so its unlock
    // rings nothing. The token below can only be the ring in `tryDispatch`,
    // which is kept for exactly this case: the actor queues a complete job
    // after its turn's check, and nothing else tells it.
    var node: @TypeOf(actor_marker).Node = .unset;
    actor_marker.set(&node, conn);
    defer actor_marker.clear(&node);
    drainDoorbell(conn);
    {
        conn.lockSessionUncancelable(io);
        defer conn.unlockSession(io);
        conn.session.windows.conn_send = flow.INITIAL_WINDOW;
        var req: [frame.FRAME_HEADER_LEN + 3]u8 = undefined;
        try conn.session.ingest(hopRequestFrame(&req, 1));
        try conn.processIntents();
    }
    try std.testing.expectEqual(@as(usize, 1), conn.inline_n);
    try std.testing.expectEqual(@as(usize, 1), io_queue.chanLen(u8, &conn.doorbell));

    try conn.runPendingInline();
    var got = HopChunks.take(hop);
    got.ackAll(hop);
    try conn.drainPendingCompleteReceipts(false);
    try expectBudget(conn, 0, 0);
}

test "release wake: the actor's own dispatch of a complete job rings its bell (the ring unlockSession does not replace)" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(1),
    });
    defer rt.deinit();
    var h = try rt.spawn(selfDispatchScenario, .{ rt, gpa });
    try h.join();
}

// The park audit and the ring/count race (t-3594, round 2). One thread, and the
// two interleavings that matter are made by the hook `unlockSession` calls on
// each side of its ring.

var hook_target: ?*Connection = null;
var hook_false_alarm = false;
/// How many times each hook point ran for the target. The cases assert them:
/// a case that passes with its hook never called proves nothing.
var hook_entries: [3]usize = @splat(0);

/// The actor was parked. The ring has handed it the token: it takes it (only a
/// `.bell` arm takes one) and starts a new turn, reading the count. The
/// handler's last counter lands AFTER this.
fn actorTakesTokenAfterRing(conn: *Connection, point: ReleasePoint) void {
    if (conn != hook_target or point != .after_ring) return;
    hook_entries[@intFromEnum(point)] += 1;
    std.debug.assert(io_queue.tryRecv(u8, &conn.doorbell) != null);
    conn.releases_at_turn_top = conn.releases_started.load(.seq_cst);
}

/// The actor looks for a lost wake after the release started and before its
/// ring.
fn actorLooksBeforeRing(conn: *Connection, point: ReleasePoint) void {
    if (conn != hook_target or point != .before_ring) return;
    hook_entries[@intFromEnum(point)] += 1;
    if (conn.lostWakeSince(conn.releases_at_turn_top)) hook_false_alarm = true;
}

/// Between the pre-park check and the select, a release starts and never rings.
fn releaseWithoutRingAfterCheck(conn: *Connection, point: ReleasePoint) void {
    if (conn != hook_target or point != .park_after_check) return;
    hook_entries[@intFromEnum(point)] += 1;
    conn.releaseRung(conn.releaseStarted());
}

/// Release A rings and is slow to publish. The actor takes A's token and
/// snapshots. Release B starts, skips its ring and publishes. The check runs now,
/// before A's publish.
var overlap_caught = false;
fn overlapHook(conn: *Connection, point: ReleasePoint) void {
    if (conn != hook_target or point != .after_ring) return;
    hook_entries[@intFromEnum(point)] += 1;
    std.debug.assert(io_queue.tryRecv(u8, &conn.doorbell) != null);
    conn.releases_at_turn_top = conn.releases_started.load(.seq_cst);
    conn.releaseRung(conn.releaseStarted());
    overlap_caught = conn.lostWakeSince(conn.releases_at_turn_top);
}

fn interleaveScenario(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var trap: TrapHop = undefined;
    try openTrapHop(&trap, gpa, rt.io());
    defer trap.close();
    const conn = &trap.hop.conn;
    const io = rt.io();
    hook_target = conn;
    defer {
        test_release_hook = null;
        hook_target = null;
    }

    // 1. The ring hands the token to the actor, which snapshots; then the
    //    handler's counter lands. Legal, and must not look like a lost wake.
    hook_entries = @splat(0);
    drainDoorbell(conn);
    conn.releases_at_turn_top = conn.releases_started.load(.seq_cst);
    test_release_hook = actorTakesTokenAfterRing;
    conn.lockSessionUncancelable(io);
    conn.unlockSession(io);
    test_release_hook = null;
    // The forced schedule ran: the hook was entered once, the actor took the
    // token (the doorbell is empty), and the release still counted.
    try std.testing.expectEqual(@as(usize, 1), hook_entries[@intFromEnum(ReleasePoint.after_ring)]);
    try std.testing.expectEqual(@as(usize, 0), io_queue.chanLen(u8, &conn.doorbell));
    try std.testing.expectEqual(conn.releases_started.load(.seq_cst), conn.releases_rung_max.load(.seq_cst));
    try std.testing.expect(!conn.wakeAuditFailed());
    try std.testing.expect(!conn.parkCheckFailed());

    // 2. The actor looks between the release's start and its ring. Legal.
    drainDoorbell(conn);
    hook_false_alarm = false;
    conn.releases_at_turn_top = conn.releases_started.load(.seq_cst);
    test_release_hook = actorLooksBeforeRing;
    conn.lockSessionUncancelable(io);
    conn.unlockSession(io);
    test_release_hook = null;
    // The hook ran once, between the start and the ring, and the ring then made
    // the token (the doorbell holds one).
    try std.testing.expectEqual(@as(usize, 1), hook_entries[@intFromEnum(ReleasePoint.before_ring)]);
    try std.testing.expectEqual(@as(usize, 1), io_queue.chanLen(u8, &conn.doorbell));
    try std.testing.expect(!hook_false_alarm);
    try std.testing.expect(!conn.wakeAuditFailed());

    // 3. The converse. A release after the snapshot whose ring never happens
    //    (counted without the ring, through the same counters the unlock uses)
    //    is a lost wake, and the audit returns it instead of crashing.
    drainDoorbell(conn);
    const snapshot = conn.releases_started.load(.seq_cst);
    conn.releaseRung(conn.releaseStarted());
    try std.testing.expect(conn.lostWakeSince(snapshot));
    // The same release with its ring is not.
    conn.ring();
    try std.testing.expect(!conn.lostWakeSince(snapshot));
    // A release that started BEFORE the snapshot is never one, ring or no ring.
    drainDoorbell(conn);
    try std.testing.expect(!conn.lostWakeSince(conn.releases_started.load(.seq_cst)));

    // 4. The schedule a snapshot taken at the park absorbs. The pre-park check
    //    passes; THEN a release starts and never rings; then the actor wakes by
    //    something that is not the doorbell. The wake audit must see it. (With
    //    the park snapshot loaded after the check, as in round 2, it is inside
    //    the snapshot and this fails.)
    hook_entries = @splat(0);
    drainDoorbell(conn);
    conn.releases_at_turn_top = conn.releases_started.load(.seq_cst);
    test_release_hook = releaseWithoutRingAfterCheck;
    const check_failed = conn.parkCheckFailed();
    test_release_hook = null;
    try std.testing.expectEqual(@as(usize, 1), hook_entries[@intFromEnum(ReleasePoint.park_after_check)]);
    try std.testing.expect(!check_failed);
    try std.testing.expect(conn.wakeAuditFailed());

    // 5. Two releases overlap. A rings and is slow to publish; the actor takes
    //    A's token and snapshots; B skips its ring and publishes; the check runs
    //    BEFORE A's publish lands. B must be seen then, and after A's publish.
    //    (An anonymous count of published releases is 1 here, not above the
    //    snapshot, and misses it.)
    hook_entries = @splat(0);
    overlap_caught = false;
    drainDoorbell(conn);
    test_release_hook = overlapHook;
    conn.lockSessionUncancelable(io);
    conn.unlockSession(io);
    test_release_hook = null;
    try std.testing.expectEqual(@as(usize, 1), hook_entries[@intFromEnum(ReleasePoint.after_ring)]);
    try std.testing.expect(overlap_caught);
    try std.testing.expect(conn.wakeAuditFailed());
}

test "park audit: the ring and the count race both ways without a false alarm, and a skipped ring is returned" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(5), .slab_slots = 16, .prewarm = 16 },
        .executors = .exact(1),
    });
    defer rt.deinit();
    var h = try rt.spawn(interleaveScenario, .{ rt, gpa });
    try h.join();
}
