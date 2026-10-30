//! Production fair write scheduler — owned by Connection actor.
//! Terminal/required ACK → ordinary controls+HEADERS → 16KiB DRR DATA.
//! A stream's DATA is never eligible before its response HEADERS has been
//! handed to the sink (see "HEADERS before DATA" below).
//! Pending DATA uses boot-reserved per-stream slabs (no post-boot GPA growth).
//! Nonterminal controls counted only when emitted.
//!
//! # The problem this solves
//!
//! HTTP/2 multiplexes many streams over one socket, so something must choose
//! which stream's bytes go next. A naive queue gives whoever wrote first the
//! whole connection, which is what makes one large download stall an entire
//! browser tab's worth of requests. That is precisely the Datastar shape this
//! stack serves: long-lived SSE streams beside one-shot page loads on the same
//! connection.
//!
//! # The drain order, and why each tier is where it is
//!
//! 1. TERMINAL controls — GOAWAY and RST_STREAM. Teardown must not queue behind
//!    the data it is canceling.
//! 2. ORDINARY controls — SETTINGS, PING, acks, WINDOW_UPDATE, HEADERS. These
//!    are small, and they unblock the peer. A WINDOW_UPDATE stuck behind a
//!    megabyte of DATA deadlocks the very flow control it grants.
//! 3. DATA — round robin over streams in a fixed 16 KiB quantum.
//!
//! The strict tier order would let controls starve DATA forever, so tier 2
//! yields after `CONTROL_BEFORE_DATA` frames whenever DATA is eligible. That
//! turns starvation into a bounded delay.
//!
//! Within tier 3 the contract is round robin, not an order of stream ids: no
//! stream with DATA ready gets a second quantum before every other ready stream
//! had one. The ready list delivers it (pop head, one quantum, push tail), so
//! picking the next stream costs O(1) however many are open. A fixed quantum
//! caps how long any one stream holds the wire. Streams blocked on their own
//! window wait on a separate list and cost nothing until `rearm`.
//!
//! # HEADERS before DATA
//!
//! A response starts with HEADERS (RFC 9113 section 8.1). DATA on a stream
//! whose HEADERS the peer has not seen is a connection error at the peer, so
//! one misordered frame kills every stream on the connection (t-3514).
//!
//! Tier order alone does not give this. The anti-starvation yield emits DATA
//! while controls are still queued, and a drain on a handler task can see the
//! control pause and the DATA pause disagree, because the actor empties the
//! TLS stash without `session_mu`. A complete handler also stages DATA before
//! its HEADERS reaches the ordinary queue. So the rule is per stream, by
//! construction: `DataPending.headers_out` is false until that stream's
//! HEADERS goes to the sink, and `arm` puts only a slot with it set on the
//! ready list. A slot waiting for its HEADERS stays idle: it is visited by
//! nothing, keeps its newcomer place, and `drain` returns when nothing else is
//! eligible, as for a window-blocked stream.
//!
//! A slot is freed every time it drains (an SSE stream between events), so
//! the fact outlives slots in `started`: the streams whose HEADERS went out
//! and whose local side has not ended. An entry ends when END_STREAM goes to
//! the sink (on DATA, or on a HEADERS such as trailers) or when the connection
//! reports the stream reset (`forgetStream`). A new slot reads it.
//!
//! A reset also drops the stream's HEADERS that are still queued. Sent later,
//! it would be a frame on a closed stream (RFC 9113 section 5.1); after a
//! local reset the RST_STREAM, a terminal control, would even have gone out
//! first. And it would put the stream back into `started` with nothing left
//! to end it. The encoder keeps no dynamic table (`hpack.Encoder`), so a
//! dropped block cannot desynchronize the peer's decoder.
//!
//! # No allocation on the write path
//!
//! Pending DATA lives in per-stream slabs carved from ONE boot allocation, and
//! a slot is a fixed array entry. The id index and the lists over the slots
//! are fixed arrays sized at `init` too. A handler write is a
//! `memcpy` into an existing slab; when the slab is full the handler parks in
//! `Connection.waitForStreamSpace`. That is the mechanism behind the promise
//! that a connection stays inside `Limits.resourceUpperBound` under any load.
const std = @import("std");
const test_hook = @import("test_hook.zig");
const control_pool = @import("control_pool.zig");
const slab_pool = @import("slab_pool.zig");
const Touched = @import("touched.zig").Touched;
const frame = @import("../core/frame.zig");
const boot_plan = @import("../core/boot_plan.zig");
const wire_ring = @import("wire_ring.zig");
const observe = @import("build_options").observe;

/// Test-only (`observe` builds): HEADERS dropped by `forgetStream` that
/// carried a ticket, so an integration test can prove that path ran.
pub var test_dropped_ticketed_headers: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};

/// One round-robin turn. It matches the HTTP/2 default max frame size, so a
/// quantum is exactly one DATA frame and the scheduler never has to split a
/// turn across frames.
pub const DATA_QUANTUM: usize = 16 * 1024;

pub const ControlClass = enum { terminal, ordinary };
pub const EmitKind = enum { terminal_control, ordinary_control, data };

pub const ControlEntry = struct {
    payload: []u8,
    class: ControlClass,
    ticket: u64 = 0,
    ticket_slot: u32 = 0,
    control_n: usize = 0,
};

pub const FramedDataEntry = struct {
    payload: []u8,
    stream_id: u31,
    end_stream: bool,
    ticket: u64 = 0,
    ticket_slot: u32 = 0,
};

/// Fixed per-stream pending DATA — slab leased at FairScheduler.init.
///
/// `stream_id == 0` marks a free slot. Zero is never a real stream id in
/// HTTP/2, so no sentinel field is needed and a slot cannot be misread as live.
///
/// `flush_remain` is what makes a flush ticket honest. It records how many
/// bytes were outstanding when the handler asked, and the ticket completes only
/// after that many have drained. Completing on the next quantum would tell the
/// handler its data reached the peer while most of it was still in the slab.
pub const DataPending = struct {
    stream_id: u31 = 0, // 0 = free slot
    slab: []u8 = &.{},
    len: usize = 0,
    end_stream: bool = false,
    last_progress_ns: u64 = 0,
    flush_ticket: u64 = 0,
    flush_slot: u32 = 0,
    flush_remain: usize = 0,
    /// Which tier-3 list the slot is on. `idle` is a live slot on neither:
    /// nothing to send yet, or popped and being served.
    place: Place = .free,
    /// Set after the first quantum. An unserved slot enters the ready list in
    /// the front segment, so a new response is not queued behind a full pass
    /// of other streams' DATA.
    served: bool = false,
    next: u32 = none,
    prev: u32 = none,
    /// Position in `FairScheduler.live`, so a free is a swap-remove.
    live_pos: u32 = none,
    /// This stream's HEADERS has been handed to the sink. Until then the
    /// slot is never armed (see "HEADERS before DATA").
    headers_out: bool = false,

    pub const Place = enum { free, idle, ready, blocked };

    pub fn bytes(self: *const DataPending) []const u8 {
        return self.slab[0..self.len];
    }

    pub fn clear(self: *DataPending) void {
        self.stream_id = 0;
        self.len = 0;
        self.end_stream = false;
        self.flush_ticket = 0;
        self.flush_slot = 0;
        self.flush_remain = 0;
        self.last_progress_ns = 0;
        self.place = .free;
        self.served = false;
        self.next = none;
        self.prev = none;
        self.live_pos = none;
        self.headers_out = false;
    }
};

const none: u32 = std.math.maxInt(u32);

/// Slots in the stream-id index: a power of two at least twice the slot
/// count, so the load stays at or below one half and a miss stops at an
/// empty entry within a short probe. Exported for `bound_shapes`.
/// The stream of a HEADERS frame payload; null for any other frame, and for
/// stream 0, which no HEADERS may use.
fn headersStream(payload: []const u8) ?u31 {
    if (payload.len < frame.FRAME_HEADER_LEN or payload[3] != @backingInt(frame.FrameType.headers)) return null;
    const sid: u31 = @intCast(std.mem.readInt(u32, payload[5..9], .big) & 0x7fff_ffff);
    if (sid == 0) return null;
    return sid;
}

/// True for a HEADERS or CONTINUATION frame that does not end its header
/// block (no END_HEADERS): the next frame on the wire must be the next
/// fragment of the same block, so nothing else may be emitted before it
/// (RFC 9113 section 6.10).
pub fn leavesBlockOpen(payload: []const u8) bool {
    if (payload.len < frame.FRAME_HEADER_LEN) return false;
    const typ = payload[3];
    if (typ != @backingInt(frame.FrameType.headers) and typ != @backingInt(frame.FrameType.continuation)) return false;
    return !frame.FrameFlags.fromByte(payload[4]).end_headers;
}

/// The stream of an RST_STREAM frame payload; null for any other frame.
fn rstStream(payload: []const u8) ?u31 {
    if (payload.len < frame.FRAME_HEADER_LEN or payload[3] != @backingInt(frame.FrameType.rst_stream)) return null;
    return @intCast(std.mem.readInt(u32, payload[5..9], .big) & 0x7fff_ffff);
}

/// The stream of a CONTINUATION frame payload; null for any other frame.
fn continuationStream(payload: []const u8) ?u31 {
    if (payload.len < frame.FRAME_HEADER_LEN or payload[3] != @backingInt(frame.FrameType.continuation)) return null;
    return @intCast(std.mem.readInt(u32, payload[5..9], .big) & 0x7fff_ffff);
}

/// Open-addressed `u31` keys, linear probing, key 0 empty. The table is at
/// most half full (`indexCapacity`), so a probe always meets an empty key.
fn probeHome(shift: u5, stream_id: u31) usize {
    return (@as(u32, stream_id) *% 0x9E3779B1) >> shift;
}

fn probeFind(keys: []const u31, shift: u5, stream_id: u31) ?usize {
    std.debug.assert(stream_id != 0);
    const mask = keys.len - 1;
    var i = probeHome(shift, stream_id);
    while (true) : (i = (i + 1) & mask) {
        const k = keys[i];
        if (k == 0) return null;
        if (k == stream_id) return i;
    }
}

fn probeInsert(keys: []u31, shift: u5, stream_id: u31) usize {
    const mask = keys.len - 1;
    var i = probeHome(shift, stream_id);
    while (keys[i] != 0) : (i = (i + 1) & mask) {
        std.debug.assert(keys[i] != stream_id);
    }
    keys[i] = stream_id;
    return i;
}

/// Backward-shift delete. Each later key in the cluster moves into the
/// hole unless its home lies cyclically in (hole, its position], in which
/// case moving it would put it before its own home and a lookup would
/// stop at the hole. A plain clear (or a tombstone) is what this replaces.
/// `vals`, when given, moves with its key.
fn probeRemove(keys: []u31, vals: ?[]u32, shift: u5, stream_id: u31) void {
    const mask = keys.len - 1;
    var hole = probeHome(shift, stream_id);
    while (keys[hole] != stream_id) : (hole = (hole + 1) & mask) {
        std.debug.assert(keys[hole] != 0);
    }
    var j = hole;
    while (true) {
        j = (j + 1) & mask;
        const k = keys[j];
        if (k == 0) break;
        const home = probeHome(shift, k);
        const stays = if (hole <= j) (hole < home and home <= j) else (hole < home or home <= j);
        if (stays) continue;
        keys[hole] = k;
        if (vals) |v| v[hole] = v[j];
        hole = j;
    }
    keys[hole] = 0;
}

pub fn indexCapacity(max_streams: usize) usize {
    return std.math.ceilPowerOfTwoAssert(usize, @max(2, max_streams * 2));
}

pub const EmitSink = *const fn (
    ctx: *anyopaque,
    payload: []u8,
    flush: bool,
    ticket: u64,
    ticket_slot: u32,
    control_n: usize,
    control_entry: bool,
) anyerror!void;

pub const WindowQuery = *const fn (ctx: *anyopaque, stream_id: u31) i32;
pub const ConnWindowQuery = *const fn (ctx: *anyopaque) i32;
/// Build framed DATA. Must be infallible once called after capacity reservation,
/// or fail before Session debit. Returns owned wire bytes.
pub const BuildDataFrame = *const fn (*anyopaque, u31, []const u8, bool) anyerror![]u8;

pub const FairScheduler = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    ctrl: control_pool.ControlPool,
    terminal_q: []ControlEntry,
    terminal_head: usize = 0,
    terminal_tail: usize = 0,
    terminal_len: usize = 0,
    ordinary_q: []ControlEntry,
    ordinary_head: usize = 0,
    ordinary_tail: usize = 0,
    ordinary_len: usize = 0,
    framed_data_q: []FramedDataEntry,
    framed_head: usize = 0,
    framed_tail: usize = 0,
    framed_len: usize = 0,
    /// One per max concurrent stream, written on first use. A connection
    /// touches only as many as it ever had streams with DATA pending at once.
    pending_slots: Touched(DataPending),
    /// Borrowed, server-owned. Slabs are rented when a stream first queues
    /// DATA and returned the moment it has none, so an idle stream — and an
    /// idle connection — holds none at all.
    pool: *slab_pool.SlabPool,
    /// When set, control/framed payloads may be frame-pool slabs. Null (tests)
    /// means every payload is a GPA allocation of the on-wire size.
    payload_free_ctx: ?*anyopaque = null,
    payload_free: ?*const fn (*anyopaque, []u8) void = null,
    /// Fails the ticket of a control that `forgetStream` drops unsent, so its
    /// waiter wakes. Required once a dropped control can carry a ticket.
    ticket_fail_ctx: ?*anyopaque = null,
    ticket_fail: ?*const fn (*anyopaque, u64, u32) void = null,
    active_pending: usize = 0,
    /// Open-addressed `stream_id -> slot index`, linear probing, deleted by
    /// backward shift so no tombstones accumulate under stream churn. Key 0 is
    /// empty (stream id 0 is never a stream).
    idx_keys: []u31,
    idx_vals: []u32,
    idx_shift: u5,
    /// Streams whose HEADERS went to the sink and whose local side has not
    /// ended (see "HEADERS before DATA"). Same capacity and probing as the
    /// index, keys only. At most `max_streams` entries: the server advertises
    /// that as SETTINGS_MAX_CONCURRENT_STREAMS and every entry is an open
    /// stream, so a full set is a broken invariant, reported as StreamCap.
    started_keys: []u31,
    started_len: usize = 0,
    /// Every occupied slot, dense, for the walks that must see blocked
    /// streams too (`forEachPending`, `nextSlowDeadlineNs`).
    live: []u32,
    /// Touched slots that are free again. A slot comes from here first, and
    /// is touched fresh only when this is empty.
    free_stack: []u32,
    free_len: usize = 0,
    /// Streams with DATA to send that were not seen blocked on their own
    /// window. Pop the head, serve one quantum, push the tail. `fresh_tail`
    /// ends the front segment of never-served slots (none when empty).
    ready_head: u32 = none,
    ready_tail: u32 = none,
    fresh_tail: u32 = none,
    ready_len: usize = 0,
    /// Streams seen with `stream_win <= 0` while the connection had credit:
    /// their own window, or an RST tombstone. Never visited on the emit path;
    /// only `rearm` moves them back.
    blocked_head: u32 = none,
    blocked_tail: u32 = none,
    blocked_len: usize = 0,
    /// A `forEachPending` callback must not free a slot: `live` is a dense
    /// array and a swap-remove would skip a neighbour.
    iterating: bool = false,
    data_quanta: usize = 0,
    emitted_controls_since_data: usize = 0,
    control_gaps: [128]usize = @splat(0),
    gap_i: usize = 0,
    emits_total: usize = 0,
    emit_kinds: [512]EmitKind = undefined,
    emit_kinds_n: usize = 0,
    sink_origin_emits: usize = 0,
    /// When this returns true, `drain` stops before popping the next DATA
    /// frame so the TLS actor can wait on a send completion without fail-closing.
    /// Fires while an in-flight send holds `pending_n` OR the TLS stash is full.
    pause: ?*const fn (*anyopaque) bool = null,
    pause_ctx: *anyopaque = undefined,
    /// When this returns true, `drain` stops before popping the next control
    /// (terminal or ordinary) so a full TLS stash cannot fail-close. Distinct
    /// from `pause`: a slow peer (`pending_n > 0` with stash room) must still
    /// emit WINDOW_UPDATE. Only stash-full should set this.
    pause_control: ?*const fn (*anyopaque) bool = null,

    /// A HEADERS or CONTINUATION frame went to the sink without END_HEADERS,
    /// so the next frame out must be the next fragment of its block. While
    /// set, `drain` pops no terminal control, forces no DATA and emits no
    /// DATA: an RST_STREAM or a DATA frame inside a header block is a
    /// connection error to the peer. A pause may still stop the drain inside
    /// a block; the next drain finishes it first. The fragments are queued
    /// back to back by one `cmdHeaders`, so the next ordinary entry is the
    /// next fragment.
    block_open: bool = false,

    /// `-Dwire-ring=true` only (t-3514): which `drain` site asked for DATA,
    /// and whether the ordinary loop last stopped on `isControlPaused`.
    diag_path: enum { forced, tail } = .tail,
    diag_ctrl_break: bool = false,

    /// `-Dwire-ring=true` only: a HEADERS frame for `sid` still waits in the
    /// ordinary queue. DATA emitted now reaches the peer before the stream's
    /// HEADERS, which a client treats as a connection error.
    fn diagHeadersQueued(self: *const FairScheduler, sid: u31) bool {
        var k: usize = 0;
        while (k < self.ordinary_len) : (k += 1) {
            const p = self.ordinary_q[(self.ordinary_head + k) % self.ordinary_q.len].payload;
            if (p.len < frame.FRAME_HEADER_LEN or p[3] != @backingInt(frame.FrameType.headers)) continue;
            if (std.mem.readInt(u32, p[5..9], .big) & 0x7fff_ffff == sid) return true;
        }
        return false;
    }

    fn diagDataBeforeHeaders(self: *FairScheduler, sid: u31, site: []const u8) void {
        if (!self.diagHeadersQueued(sid)) return;
        var buf: [256]u8 = undefined;
        const line = std.fmt.bufPrint(&buf, "SCHED data_before_headers sid={d} site={s} path={s} ctrl_break={} paused_now={} ctrl_paused_now={} ordinary_len={d} since_data={d}\n", .{
            sid,             site,                   @tagName(self.diag_path), self.diag_ctrl_break,
            self.isPaused(), self.isControlPaused(), self.ordinary_len,        self.emitted_controls_since_data,
        }) catch return;
        _ = std.c.write(2, line.ptr, line.len);
    }

    fn isPaused(self: *FairScheduler) bool {
        const f = self.pause orelse return false;
        return f(self.pause_ctx);
    }

    fn isControlPaused(self: *FairScheduler) bool {
        const f = self.pause_control orelse return false;
        return f(self.pause_ctx);
    }

    /// The allocations of `init`, in its order. See `boot_plan`.
    pub fn planBoot(
        plan: *boot_plan.Plan,
        terminal_cap: usize,
        ordinary_cap: usize,
        max_streams: usize,
    ) error{InvalidConfig}!void {
        // `init` asserts this, and `indexCapacity` doubles it.
        if (max_streams >= none) return error.InvalidConfig;
        try plan.array(ControlEntry, @max(terminal_cap, 1));
        try plan.array(ControlEntry, @max(ordinary_cap, 1));
        try plan.array(FramedDataEntry, @max(ordinary_cap, 1));
        try plan.array(DataPending, max_streams);
        const idx_cap = indexCapacity(max_streams);
        try plan.array(u31, idx_cap);
        try plan.array(u32, idx_cap);
        // `started_keys`, keys only, same capacity as the index.
        try plan.array(u31, idx_cap);
        try plan.array(u32, max_streams);
        try plan.array(u32, max_streams);
    }

    pub fn init(
        gpa: std.mem.Allocator,
        io: std.Io,
        ordinary_bytes: usize,
        ordinary_entries: usize,
        terminal_bytes: usize,
        terminal_entries: usize,
        max_streams: usize,
        pool: *slab_pool.SlabPool,
    ) !FairScheduler {
        // The terminal ring and the pool's terminal share are one argument, so
        // they cannot disagree (`Limits.terminalFrames`).
        const tq = try gpa.alloc(ControlEntry, @max(terminal_entries, 1));
        errdefer gpa.free(tq);
        const oq = try gpa.alloc(ControlEntry, @max(ordinary_entries, 1));
        errdefer gpa.free(oq);
        const fq = try gpa.alloc(FramedDataEntry, @max(ordinary_entries, 1));
        errdefer gpa.free(fq);
        std.debug.assert(max_streams < none);
        const slots = try gpa.alloc(DataPending, max_streams);
        errdefer gpa.free(slots);
        const idx_cap = indexCapacity(max_streams);
        const keys = try gpa.alloc(u31, idx_cap);
        errdefer gpa.free(keys);
        @memset(keys, 0);
        const vals = try gpa.alloc(u32, idx_cap);
        errdefer gpa.free(vals);
        const started = try gpa.alloc(u31, idx_cap);
        errdefer gpa.free(started);
        @memset(started, 0);
        const live = try gpa.alloc(u32, max_streams);
        errdefer gpa.free(live);
        const free_stack = try gpa.alloc(u32, max_streams);
        errdefer gpa.free(free_stack);
        return .{
            .gpa = gpa,
            .io = io,
            .ctrl = control_pool.ControlPool.init(ordinary_bytes, ordinary_entries, terminal_bytes, terminal_entries),
            .terminal_q = tq,
            .ordinary_q = oq,
            .framed_data_q = fq,
            .pending_slots = .init(slots),
            .pool = pool,
            .idx_keys = keys,
            .idx_vals = vals,
            .idx_shift = @intCast(32 - std.math.log2_int(usize, idx_cap)),
            .started_keys = started,
            .live = live,
            .free_stack = free_stack,
        };
    }

    pub fn freePayload(self: *FairScheduler, payload: []u8) void {
        if (payload.len == 0) return;
        if (self.payload_free) |f| {
            f(self.payload_free_ctx.?, payload);
            return;
        }
        self.gpa.free(payload);
    }

    /// Release what the scheduler still holds outside its own tables: queued
    /// payloads (through `freePayload`) and the slabs of pending DATA, which
    /// go back to the shared pool. A connection slot calls this and rewinds
    /// the tables' memory; `deinit` is this plus the frees.
    pub fn releaseHeld(self: *FairScheduler) void {
        while (self.popTerminal()) |e| self.freePayload(e.payload);
        while (self.popOrdinary()) |e| self.freePayload(e.payload);
        while (self.popFramed()) |e| self.freePayload(e.payload);
        // A slot still holding a slab here means some teardown path missed it.
        // Return them rather than leaking into the shared pool, where the loss
        // would starve every other connection instead of just this one.
        for (self.pending_slots.slice()) |*slot| self.returnSlab(slot);
    }

    pub fn deinit(self: *FairScheduler) void {
        self.releaseHeld();
        self.gpa.free(self.terminal_q);
        self.gpa.free(self.ordinary_q);
        self.gpa.free(self.framed_data_q);
        self.gpa.free(self.pending_slots.storage);
        self.gpa.free(self.idx_keys);
        self.gpa.free(self.idx_vals);
        self.gpa.free(self.started_keys);
        self.gpa.free(self.live);
        self.gpa.free(self.free_stack);
    }

    fn pushRing(comptime T: type, q: []T, head: *usize, tail: *usize, len: *usize, e: T) error{QueueFull}!void {
        _ = head;
        if (len.* >= q.len) return error.QueueFull;
        q[tail.*] = e;
        tail.* = (tail.* + 1) % q.len;
        len.* += 1;
    }

    fn popRing(comptime T: type, q: []T, head: *usize, len: *usize) ?T {
        if (len.* == 0) return null;
        const e = q[head.*];
        head.* = (head.* + 1) % q.len;
        len.* -= 1;
        return e;
    }

    /// A terminal frame holds its pool share until it leaves the ring
    /// (`ControlPool`), so every pop gives the share back, the drain's and the
    /// teardown's alike. The entry's `control_n` is not the sink's to release.
    pub fn popTerminal(self: *FairScheduler) ?ControlEntry {
        const e = popRing(ControlEntry, self.terminal_q, &self.terminal_head, &self.terminal_len) orelse return null;
        self.ctrl.releaseTerminal(e.control_n);
        return e;
    }
    pub fn popOrdinary(self: *FairScheduler) ?ControlEntry {
        return popRing(ControlEntry, self.ordinary_q, &self.ordinary_head, &self.ordinary_len);
    }
    pub fn popFramed(self: *FairScheduler) ?FramedDataEntry {
        return popRing(FramedDataEntry, self.framed_data_q, &self.framed_head, &self.framed_len);
    }

    fn idxHome(self: *const FairScheduler, stream_id: u31) usize {
        return probeHome(self.idx_shift, stream_id);
    }

    fn idxFind(self: *const FairScheduler, stream_id: u31) ?u32 {
        const at = probeFind(self.idx_keys, self.idx_shift, stream_id) orelse return null;
        return self.idx_vals[at];
    }

    fn idxInsert(self: *FairScheduler, stream_id: u31, slot: u32) void {
        self.idx_vals[probeInsert(self.idx_keys, self.idx_shift, stream_id)] = slot;
    }

    fn idxRemove(self: *FairScheduler, stream_id: u31) void {
        probeRemove(self.idx_keys, self.idx_vals, self.idx_shift, stream_id);
    }

    fn startedContains(self: *const FairScheduler, stream_id: u31) bool {
        return probeFind(self.started_keys, self.idx_shift, stream_id) != null;
    }

    fn startedInsert(self: *FairScheduler, stream_id: u31) error{StreamCap}!void {
        if (self.startedContains(stream_id)) return;
        if (self.started_len == self.pending_slots.storage.len) return error.StreamCap;
        _ = probeInsert(self.started_keys, self.idx_shift, stream_id);
        self.started_len += 1;
    }

    fn startedRemove(self: *FairScheduler, stream_id: u31) void {
        if (!self.startedContains(stream_id)) return;
        probeRemove(self.started_keys, null, self.idx_shift, stream_id);
        self.started_len -= 1;
    }

    /// True once `stream_id`'s HEADERS has been handed to the sink.
    fn headersOut(self: *FairScheduler, stream_id: u31) bool {
        if (self.findPending(stream_id)) |pw| return pw.headers_out;
        return self.startedContains(stream_id);
    }

    /// A HEADERS frame is about to go to the sink: its stream's DATA becomes
    /// eligible. END_STREAM on it (a response with no body, or trailers) ends
    /// the local side, so the stream leaves `started`. A slot that already
    /// waits is armed; DATA after an END_STREAM HEADERS would be a caller
    /// bug, and arming it makes that bug a loud peer error, not a silent stall.
    ///
    /// `drain` is the production caller, for every ordinary control it hands
    /// to the sink. Public so a test that drives `emitOneDataPublic` directly
    /// can open streams the same way.
    pub fn noteHeadersOut(self: *FairScheduler, payload: []const u8) error{StreamCap}!void {
        const sid = headersStream(payload) orelse return;
        if (payload[4] & 0x1 != 0) self.startedRemove(sid) else try self.startedInsert(sid);
        if (self.findPending(sid)) |pw| {
            pw.headers_out = true;
            self.arm(pw);
        }
    }

    /// The connection saw this stream reset, by either side: it will send
    /// no more frames, so its HEADERS state ends and its still-queued HEADERS
    /// are dropped with their control-pool occupancy and tickets (see "HEADERS
    /// before DATA"). Its DATA slot, if any, is the caller's to remove
    /// (`removePending`), because the caller owns that byte accounting.
    pub fn forgetStream(self: *FairScheduler, stream_id: u31) void {
        if (stream_id == 0) return;
        self.startedRemove(stream_id);
        self.dropQueuedHeaders(stream_id);
    }

    /// Only HEADERS: they are what opens a response, and the only control a
    /// handler queues for its stream. RST_STREAM is terminal-class and must
    /// still go. Order of everything kept is unchanged.
    fn dropQueuedHeaders(self: *FairScheduler, stream_id: u31) void {
        const n = self.ordinary_len;
        if (n == 0) return;
        const cap = self.ordinary_q.len;
        var kept: usize = 0;
        var k: usize = 0;
        // The CONTINUATION fragments queued behind a dropped HEADERS go with
        // it. Ones whose HEADERS already left stay: the peer is waiting for
        // them.
        var dropping_block = false;
        while (k < n) : (k += 1) {
            const e = self.ordinary_q[(self.ordinary_head + k) % cap];
            const is_headers = headersStream(e.payload) == stream_id;
            const is_fragment = dropping_block and continuationStream(e.payload) == stream_id;
            if (is_headers or is_fragment) {
                dropping_block = leavesBlockOpen(e.payload);
                self.ctrl.release(e.control_n, 1);
                if (e.ticket != 0) {
                    if (comptime observe) _ = test_dropped_ticketed_headers.fetchAdd(1, .acq_rel);
                    const fail = self.ticket_fail orelse
                        std.debug.panic("dropped HEADERS for stream {d} carries ticket {d} and no ticket_fail is set", .{ stream_id, e.ticket });
                    fail(self.ticket_fail_ctx.?, e.ticket, e.ticket_slot);
                }
                self.freePayload(e.payload);
                continue;
            }
            self.ordinary_q[(self.ordinary_head + kept) % cap] = e;
            kept += 1;
        }
        self.ordinary_len = kept;
        self.ordinary_tail = (self.ordinary_head + kept) % cap;
    }

    /// Drop the RST_STREAM for `stream_id` if it still waits in the terminal
    /// ring, with its pool share, its payload and its ticket. The peer reset
    /// the stream itself, so it already counts the stream closed, and an
    /// unwritten RST for it would only hold one of the T terminal slots a
    /// well-behaved peer's own open streams need (`Limits.terminalFrames`).
    /// An RST that already left the ring is in the write memory and goes out
    /// as it is. Returns whether one was dropped. Order of everything kept is
    /// unchanged. O(T), and a peer RST is rate limited.
    pub fn retireTerminalRst(self: *FairScheduler, stream_id: u31) bool {
        const n = self.terminal_len;
        const cap = self.terminal_q.len;
        var kept: usize = 0;
        var k: usize = 0;
        var dropped = false;
        while (k < n) : (k += 1) {
            const e = self.terminal_q[(self.terminal_head + k) % cap];
            if (!dropped and rstStream(e.payload) == stream_id) {
                dropped = true;
                self.ctrl.releaseTerminal(e.control_n);
                if (e.ticket != 0) {
                    const fail = self.ticket_fail orelse
                        std.debug.panic("retired RST_STREAM for stream {d} carries ticket {d} and no ticket_fail is set", .{ stream_id, e.ticket });
                    fail(self.ticket_fail_ctx.?, e.ticket, e.ticket_slot);
                }
                self.freePayload(e.payload);
                continue;
            }
            self.terminal_q[(self.terminal_head + kept) % cap] = e;
            kept += 1;
        }
        self.terminal_len = kept;
        self.terminal_tail = (self.terminal_head + kept) % cap;
        return dropped;
    }

    fn slotIndex(self: *const FairScheduler, pw: *const DataPending) u32 {
        return @intCast((@intFromPtr(pw) - @intFromPtr(self.pending_slots.storage.ptr)) / @sizeOf(DataPending));
    }

    fn listPushTail(self: *FairScheduler, head: *u32, tail: *u32, i: u32) void {
        const s = &self.pending_slots.slice()[i];
        s.next = none;
        s.prev = tail.*;
        if (tail.* != none) self.pending_slots.slice()[tail.*].next = i else head.* = i;
        tail.* = i;
    }

    fn listPushHead(self: *FairScheduler, head: *u32, tail: *u32, i: u32) void {
        const s = &self.pending_slots.slice()[i];
        s.prev = none;
        s.next = head.*;
        if (head.* != none) self.pending_slots.slice()[head.*].prev = i else tail.* = i;
        head.* = i;
    }

    fn listUnlink(self: *FairScheduler, head: *u32, tail: *u32, i: u32) void {
        const s = &self.pending_slots.slice()[i];
        if (s.prev != none) self.pending_slots.slice()[s.prev].next = s.next else head.* = s.next;
        if (s.next != none) self.pending_slots.slice()[s.next].prev = s.prev else tail.* = s.prev;
        s.next = none;
        s.prev = none;
    }

    /// Ready list, front segment for never-served slots. The segment is a
    /// prefix ending at `fresh_tail`; newcomers append to it in arrival order.
    fn readyInsert(self: *FairScheduler, i: u32) void {
        const s = &self.pending_slots.slice()[i];
        std.debug.assert(s.place == .idle);
        std.debug.assert(s.headers_out);
        if (!s.served) {
            if (self.fresh_tail == none) {
                self.listPushHead(&self.ready_head, &self.ready_tail, i);
            } else {
                const after = self.fresh_tail;
                const nxt = self.pending_slots.slice()[after].next;
                s.prev = after;
                s.next = nxt;
                self.pending_slots.slice()[after].next = i;
                if (nxt != none) self.pending_slots.slice()[nxt].prev = i else self.ready_tail = i;
            }
            self.fresh_tail = i;
        } else {
            self.listPushTail(&self.ready_head, &self.ready_tail, i);
        }
        s.place = .ready;
        self.ready_len += 1;
    }

    fn readyUnlink(self: *FairScheduler, i: u32) void {
        // The front segment is a prefix, so the previous slot is still in it.
        if (self.fresh_tail == i) self.fresh_tail = self.pending_slots.slice()[i].prev;
        self.listUnlink(&self.ready_head, &self.ready_tail, i);
        self.pending_slots.slice()[i].place = .idle;
        self.ready_len -= 1;
    }

    fn blockedPush(self: *FairScheduler, i: u32) void {
        std.debug.assert(self.pending_slots.slice()[i].place == .idle);
        self.listPushTail(&self.blocked_head, &self.blocked_tail, i);
        self.pending_slots.slice()[i].place = .blocked;
        self.blocked_len += 1;
    }

    fn blockedUnlink(self: *FairScheduler, i: u32) void {
        self.listUnlink(&self.blocked_head, &self.blocked_tail, i);
        self.pending_slots.slice()[i].place = .idle;
        self.blocked_len -= 1;
    }

    /// Put a live slot that has a DATA frame to send on the ready list, unless
    /// it is already on a list. A blocked slot stays blocked: more bytes do not
    /// change its window.
    fn arm(self: *FairScheduler, pw: *DataPending) void {
        if (pw.place != .idle) return;
        if (pw.len == 0 and !pw.end_stream) return;
        if (!pw.headers_out) return;
        self.readyInsert(self.slotIndex(pw));
    }

    pub fn findPending(self: *FairScheduler, stream_id: u31) ?*DataPending {
        if (stream_id == 0) return null;
        const i = self.idxFind(stream_id) orelse return null;
        return &self.pending_slots.slice()[i];
    }

    fn allocPending(self: *FairScheduler, stream_id: u31) error{StreamCap}!*DataPending {
        if (self.findPending(stream_id)) |s| return s;
        const i: u32 = if (self.free_len != 0) blk: {
            self.free_len -= 1;
            break :blk self.free_stack[self.free_len];
        } else blk: {
            const fresh: u32 = @intCast(self.pending_slots.touched);
            _ = self.pending_slots.touchNext(.{}) orelse return error.StreamCap;
            break :blk fresh;
        };
        const s = &self.pending_slots.slice()[i];
        std.debug.assert(s.stream_id == 0 and s.place == .free);
        s.clear();
        s.stream_id = stream_id;
        s.place = .idle;
        s.headers_out = self.startedContains(stream_id);
        s.live_pos = @intCast(self.active_pending);
        self.live[self.active_pending] = i;
        self.active_pending += 1;
        self.idxInsert(stream_id, i);
        return s;
    }

    pub fn pendingByteLen(self: *FairScheduler, stream_id: u31) usize {
        if (self.findPending(stream_id)) |pw| return pw.len;
        return 0;
    }

    pub fn pendingCount(self: *const FairScheduler) usize {
        return self.active_pending;
    }

    /// True when no control frame, framed DATA or pending DATA waits to be
    /// emitted.
    pub fn idle(self: *const FairScheduler) bool {
        return self.terminal_len == 0 and self.ordinary_len == 0 and
            self.framed_len == 0 and self.active_pending == 0;
    }

    pub fn nextSlowDeadlineNs(self: *const FairScheduler, timeout_ns: u64) ?u64 {
        if (timeout_ns == 0) return null;
        var next: ?u64 = null;
        for (self.live[0..self.active_pending]) |i| {
            const slot = self.pending_slots.slice()[i];
            if (slot.len == 0 or slot.last_progress_ns == 0) continue;
            const deadline = slot.last_progress_ns +% timeout_ns;
            if (next == null or deadline < next.?) next = deadline;
        }
        return next;
    }

    pub fn contains(self: *FairScheduler, stream_id: u31) bool {
        return self.findPending(stream_id) != null;
    }

    /// The ONLY place a slab goes back. Every teardown path routes through
    /// `freePending`, so exactly one function has to be right — a second
    /// release site is how a shared pool gets double returns.
    fn returnSlab(self: *FairScheduler, pw: *DataPending) void {
        if (pw.slab.len == 0) return;
        const slab = pw.slab;
        pw.slab = &.{};
        self.pool.release(self.io, slab);
    }

    /// The one exit from every list and the index. A second unlink path is
    /// how a slot ends up in the ready list after its stream is gone.
    fn freePending(self: *FairScheduler, pw: *DataPending) void {
        std.debug.assert(!self.iterating);
        self.returnSlab(pw);
        if (pw.stream_id != 0) {
            const i = self.slotIndex(pw);
            switch (pw.place) {
                .ready => self.readyUnlink(i),
                .blocked => self.blockedUnlink(i),
                .idle => {},
                .free => unreachable,
            }
            self.idxRemove(pw.stream_id);
            std.debug.assert(self.active_pending > 0);
            self.active_pending -= 1;
            const last = self.live[self.active_pending];
            self.live[pw.live_pos] = last;
            self.pending_slots.slice()[last].live_pos = pw.live_pos;
            self.free_stack[self.free_len] = i;
            self.free_len += 1;
        }
        pw.clear();
    }

    fn recordEmitKind(self: *FairScheduler, kind: EmitKind) void {
        if (self.emit_kinds_n < self.emit_kinds.len) {
            self.emit_kinds[self.emit_kinds_n] = kind;
            self.emit_kinds_n += 1;
        }
    }

    pub fn enqueueControl(
        self: *FairScheduler,
        payload: []u8,
        class: ControlClass,
        ticket: u64,
        ticket_slot: u32,
    ) error{ PoolFull, QueueFull }!void {
        const n = frame.encodedOnWireLen(payload);
        if (class == .terminal) {
            if (!self.ctrl.tryReserveTerminal(n)) {
                self.freePayload(payload);
                return error.PoolFull;
            }
            pushRing(ControlEntry, self.terminal_q, &self.terminal_head, &self.terminal_tail, &self.terminal_len, .{
                .payload = payload,
                .class = .terminal,
                .ticket = ticket,
                .ticket_slot = ticket_slot,
                .control_n = n,
            }) catch {
                self.ctrl.releaseTerminal(n);
                self.freePayload(payload);
                return error.QueueFull;
            };
        } else {
            if (!self.ctrl.tryReserveOrdinary(n)) {
                self.freePayload(payload);
                return error.PoolFull;
            }
            pushRing(ControlEntry, self.ordinary_q, &self.ordinary_head, &self.ordinary_tail, &self.ordinary_len, .{
                .payload = payload,
                .class = .ordinary,
                .ticket = ticket,
                .ticket_slot = ticket_slot,
                .control_n = n,
            }) catch {
                self.ctrl.release(n, 1);
                self.freePayload(payload);
                return error.QueueFull;
            };
        }
    }

    /// A header block of more than one frame (HEADERS, then CONTINUATIONs),
    /// admitted as one unit: the share's room for all its entries and bytes
    /// is taken before the first fragment is queued, or none is, and every
    /// payload is freed (`PoolFull`). A block admitted fragment by fragment
    /// could be refused after its HEADERS went in, and the peer would then
    /// wait for the rest of a block that never comes (the scheduler holds the
    /// connection to it, `block_open`). The ticket rides on the first frame.
    pub fn enqueueControlBlock(
        self: *FairScheduler,
        payloads: []const []u8,
        ticket: u64,
        ticket_slot: u32,
    ) error{PoolFull}!void {
        std.debug.assert(payloads.len >= 2);
        var total: usize = 0;
        for (payloads) |p| total += frame.encodedOnWireLen(p);
        if (!self.ctrl.tryReserveOrdinaryBlock(total, payloads.len)) {
            for (payloads) |p| self.freePayload(p);
            return error.PoolFull;
        }
        for (payloads, 0..) |p, k| {
            // The ring holds as many entries as the share, and a popped entry
            // gives its ring slot back before its share, so a reserved entry
            // always has a slot.
            pushRing(ControlEntry, self.ordinary_q, &self.ordinary_head, &self.ordinary_tail, &self.ordinary_len, .{
                .payload = p,
                .class = .ordinary,
                .ticket = if (k == 0) ticket else 0,
                .ticket_slot = if (k == 0) ticket_slot else 0,
                .control_n = frame.encodedOnWireLen(p),
            }) catch unreachable;
        }
    }

    pub fn enqueueFramedData(
        self: *FairScheduler,
        payload: []u8,
        stream_id: u31,
        end_stream: bool,
        ticket: u64,
        ticket_slot: u32,
    ) error{QueueFull}!void {
        pushRing(FramedDataEntry, self.framed_data_q, &self.framed_head, &self.framed_tail, &self.framed_len, .{
            .payload = payload,
            .stream_id = stream_id,
            .end_stream = end_stream,
            .ticket = ticket,
            .ticket_slot = ticket_slot,
        }) catch {
            self.freePayload(payload);
            return error.QueueFull;
        };
    }

    /// Append into boot-reserved slab — never grows, never calls GPA.
    ///
    /// The per-stream cap is the SLAB LENGTH, fixed at `init` from
    /// `Limits.outbound_bytes_per_stream`. There is deliberately no cap
    /// argument: a caller that could pass one would appear to choose the limit
    /// while the slab decided, and the two would drift.
    ///
    /// `error.OutOfMemory` here means "this stream's slab is full", not that
    /// the process is out of memory. It is the backpressure signal that parks
    /// the handler in `Connection.waitForStreamSpace`.
    pub fn enqueueDataBytes(
        self: *FairScheduler,
        stream_id: u31,
        bytes: []const u8,
        end: bool,
        flush_ticket: u64,
        flush_slot: u32,
    ) error{ OutOfMemory, StreamCap, SlabsExhausted, TicketWithoutFrame }!void {
        // dataEligible deliberately ignores empty, non-final entries: there is
        // no legal DATA frame to emit. Refuse a receipt that could never reach
        // the WritePump instead of turning an internal mistake into a hang.
        if (bytes.len == 0 and !end and flush_ticket != 0) {
            const pending = self.findPending(stream_id);
            if (pending == null or (pending.?.len == 0 and !pending.?.end_stream)) {
                return error.TicketWithoutFrame;
            }
        }
        // Nothing to frame and nothing to carry: do not occupy a slot that the
        // admission cap in `Connection.enqueuePending` would count.
        if (bytes.len == 0 and !end and flush_ticket == 0) return;
        const pw = try self.allocPending(stream_id);
        if (bytes.len != 0 and pw.slab.len == 0) {
            // First bytes for this stream: take a slab now. Exhaustion is
            // BACKPRESSURE and gets its own error, because the caller must park
            // and retry — mapping it onto OutOfMemory would fail the write for
            // a condition that clears on its own.
            pw.slab = self.pool.tryRent(self.io) orelse {
                if (pw.len == 0 and !pw.end_stream and flush_ticket == 0) self.freePending(pw);
                return error.SlabsExhausted;
            };
        }
        if (pw.len + bytes.len > pw.slab.len) return error.OutOfMemory;
        // Restate the invariant the guard above establishes, immediately before
        // the copy that depends on it. Without this line a weakened guard is
        // caught only by the bounds check INSIDE `@memcpy`, which aborts at a
        // line that says nothing about slabs — so the failure names the symptom
        // and a reader has to bisect back to the cause. The assert costs
        // nothing in a release build and names the cause directly.
        std.debug.assert(pw.len + bytes.len <= pw.slab.len);
        @memcpy(pw.slab[pw.len..][0..bytes.len], bytes);
        pw.len += bytes.len;
        if (end) pw.end_stream = true;
        pw.last_progress_ns = nowNs(self.io);
        if (flush_ticket != 0) {
            pw.flush_ticket = flush_ticket;
            pw.flush_slot = flush_slot;
            pw.flush_remain = pw.len;
        }
        self.arm(pw);
    }

    fn recordGap(self: *FairScheduler) void {
        self.control_gaps[self.gap_i % self.control_gaps.len] = self.emitted_controls_since_data;
        self.gap_i += 1;
        self.data_quanta += 1;
        self.emitted_controls_since_data = 0;
        self.ctrl.noteDataQuantum();
    }

    pub fn maxControlGap(self: *const FairScheduler) usize {
        var m: usize = 0;
        const n = @min(self.gap_i, self.control_gaps.len);
        var i: usize = 0;
        while (i < n) : (i += 1) {
            if (self.control_gaps[i] > m) m = self.control_gaps[i];
        }
        if (self.emitted_controls_since_data > m) m = self.emitted_controls_since_data;
        return m;
    }

    fn attachTicket(pw: *DataPending, quantum: usize) struct { u64, u32 } {
        if (pw.flush_ticket == 0) return .{ 0, 0 };
        if (pw.flush_remain > quantum) {
            pw.flush_remain -= quantum;
            return .{ 0, 0 };
        }
        const t = pw.flush_ticket;
        const s = pw.flush_slot;
        pw.flush_ticket = 0;
        pw.flush_slot = 0;
        pw.flush_remain = 0;
        return .{ t, s };
    }

    /// A ready slot may turn out blocked when it is popped (its window has not
    /// been probed since it was armed). Then `emitOneData` moves it to blocked
    /// and returns false, and the ordinary loop in `drain` pops a control
    /// instead: one wasted probe, never a spin.
    fn dataEligible(self: *FairScheduler, win_ctx: *anyopaque, conn_win: ConnWindowQuery) bool {
        if (conn_win(win_ctx) <= 0) return false;
        return self.framed_len > 0 or self.ready_len > 0;
    }

    /// Move streams whose send credit grew back to the ready tail. `ids` are
    /// stream WINDOW_UPDATEs; `all` is a positive SETTINGS_INITIAL_WINDOW_SIZE
    /// change or an overflowed journal (`Session.noteSendCredit`). It does not
    /// probe windows: the next pop does, and a still-blocked stream goes back.
    /// A connection WINDOW_UPDATE needs no call, because connection credit
    /// never puts a stream on the blocked list (see `emitOneData`).
    pub fn rearm(self: *FairScheduler, ids: []const u31, all: bool) void {
        if (all) {
            while (self.blocked_head != none) {
                const i = self.blocked_head;
                self.blockedUnlink(i);
                self.readyInsert(i);
            }
            return;
        }
        for (ids) |sid| {
            const i = self.idxFind(sid) orelse continue;
            if (self.pending_slots.slice()[i].place != .blocked) continue;
            self.blockedUnlink(i);
            self.readyInsert(i);
        }
    }

    pub fn shouldForceDataNow(self: *const FairScheduler) bool {
        return self.emitted_controls_since_data >= control_pool.CONTROL_BEFORE_DATA;
    }

    /// Emit as much as the windows and the queues allow, in tier order.
    ///
    /// Called only from `Connection.drainEmit`, on the actor, under
    /// `session_mu`. It runs to exhaustion rather than emitting one frame,
    /// because the actor drains once per iteration and anything left behind
    /// waits for the next wakeup.
    ///
    /// The `shouldForceDataNow` check inside the ordinary loop is the
    /// anti-starvation yield. It fires only when DATA is actually eligible, so
    /// a window-blocked stream cannot make the loop spin.
    pub fn drain(
        self: *FairScheduler,
        sink_ctx: *anyopaque,
        sink: EmitSink,
        win_ctx: *anyopaque,
        stream_win: WindowQuery,
        conn_win: ConnWindowQuery,
        build_data_frame: BuildDataFrame,
        build_ctx: *anyopaque,
    ) anyerror!void {
        // Terminal controls first, then ordinary ones. A header block that
        // closes inside the ordinary loop lets the terminals that waited for
        // it go in this same drain: nothing else would wake the actor for them.
        while (true) {
            while (self.terminal_len > 0 and !self.block_open) {
                if (self.isControlPaused()) break;
                const e = self.popTerminal() orelse break;
                self.emits_total += 1;
                self.sink_origin_emits += 1;
                self.recordEmitKind(.terminal_control);
                // Not a counted control entry: its share was given back by the pop.
                try sink(sink_ctx, e.payload, true, e.ticket, e.ticket_slot, 0, false);
            }
            if (comptime wire_ring.enabled) self.diag_ctrl_break = false;
            while (self.ordinary_len > 0) {
                if (self.isControlPaused()) {
                    if (comptime wire_ring.enabled) self.diag_ctrl_break = true;
                    break;
                }
                if (!self.block_open and !self.isPaused() and self.shouldForceDataNow() and self.dataEligible(win_ctx, conn_win)) {
                    if (comptime wire_ring.enabled) self.diag_path = .forced;
                    if (try self.emitOneData(sink_ctx, sink, win_ctx, stream_win, conn_win, build_data_frame, build_ctx)) {
                        continue;
                    }
                }
                const e = self.popOrdinary() orelse break;
                self.block_open = leavesBlockOpen(e.payload);
                self.emits_total += 1;
                self.sink_origin_emits += 1;
                self.recordEmitKind(.ordinary_control);
                self.emitted_controls_since_data += 1;
                self.ctrl.noteControl();
                self.noteHeadersOut(e.payload) catch |err| {
                    self.freePayload(e.payload);
                    return err;
                };
                try sink(sink_ctx, e.payload, true, e.ticket, e.ticket_slot, e.control_n, true);
            }
            if (self.terminal_len > 0 and !self.block_open and !self.isControlPaused()) continue;
            break;
        }
        if (comptime wire_ring.enabled) self.diag_path = .tail;
        while (true) {
            if (self.block_open or self.isPaused()) return;
            if (!try self.emitOneData(sink_ctx, sink, win_ctx, stream_win, conn_win, build_data_frame, build_ctx)) break;
        }
    }

    pub fn emitOneDataPublic(
        self: *FairScheduler,
        sink_ctx: *anyopaque,
        sink: EmitSink,
        win_ctx: *anyopaque,
        stream_win: WindowQuery,
        conn_win: ConnWindowQuery,
        build_data_frame: BuildDataFrame,
        build_ctx: *anyopaque,
    ) anyerror!bool {
        return self.emitOneData(sink_ctx, sink, win_ctx, stream_win, conn_win, build_data_frame, build_ctx);
    }

    /// Emit at most one DATA frame. Returns false when nothing is eligible.
    ///
    /// Order of decisions:
    /// 1. The connection window gates everything. No credit, no DATA, and the
    ///    ready list is left exactly as it is.
    /// 2. Already-framed DATA goes first. Those bytes are past `Session`, so
    ///    holding them would reorder frames that the protocol layer has already
    ///    committed to.
    /// 3. Otherwise pop the ready head. A head whose own window is empty moves
    ///    to the blocked list and the next head is tried.
    ///
    /// Tier-3 fairness is round robin: a served stream with bytes left goes to
    /// the TAIL, so no stream gets a second quantum while another ready stream
    /// waits for its first. Never-served streams enter a front segment instead
    /// (see `readyInsert`), which is what today's newcomers got from the id
    /// cursor: a new response is not queued behind a full pass of bulk DATA.
    ///
    /// Blocked means the STREAM's window, never the connection's. `stream_win`
    /// is `min(stream, connection)` credit (`flow.availableSend`), so it is
    /// read only before the build and only after step 1 saw connection credit;
    /// a build that spends the last connection credit puts the remainder on
    /// the ready tail without re-reading it. Reading it after the debit would
    /// park a stream that still has its own credit on the blocked list, where
    /// a connection WINDOW_UPDATE never reaches it.
    ///
    /// The builder must not call back into this scheduler: the slot is popped,
    /// built, then shrunk, and the assert after the build is what a re-entrant
    /// builder would trip.
    fn emitOneData(
        self: *FairScheduler,
        sink_ctx: *anyopaque,
        sink: EmitSink,
        win_ctx: *anyopaque,
        stream_win: WindowQuery,
        conn_win: ConnWindowQuery,
        build_data_frame: BuildDataFrame,
        build_ctx: *anyopaque,
    ) anyerror!bool {
        if (self.block_open or conn_win(win_ctx) <= 0) return false;

        if (self.framed_len > 0) {
            const head = self.framed_data_q[self.framed_head];
            if (self.headersOut(head.stream_id) and (stream_win(win_ctx, head.stream_id) > 0 or self.active_pending == 0)) {
                if (comptime wire_ring.enabled) self.diagDataBeforeHeaders(head.stream_id, "framed");
                const e = self.popFramed() orelse return false;
                if (e.end_stream) self.startedRemove(e.stream_id);
                self.emits_total += 1;
                self.sink_origin_emits += 1;
                self.recordEmitKind(.data);
                self.recordGap();
                try sink(sink_ctx, e.payload, e.end_stream or e.ticket != 0, e.ticket, e.ticket_slot, 0, false);
                return true;
            }
        }

        while (self.ready_head != none) {
            const i = self.ready_head;
            const in_front = self.fresh_tail != none;
            self.readyUnlink(i);
            const pw = &self.pending_slots.slice()[i];
            const sid = pw.stream_id;
            // `arm` only lists a slot with a frame to send and its HEADERS
            // out, and every path that empties one frees it.
            std.debug.assert(pw.len > 0 or pw.end_stream);
            std.debug.assert(pw.headers_out);
            const avail = stream_win(win_ctx, sid);
            if (avail <= 0) {
                self.blockedPush(i);
                continue;
            }
            const quantum = if (pw.len == 0) 0 else @min(pw.len, DATA_QUANTUM, @as(usize, @intCast(avail)));
            const end = pw.end_stream and quantum == pw.len;
            if (comptime wire_ring.enabled) self.diagDataBeforeHeaders(sid, "slot");
            const payload = build_data_frame(build_ctx, sid, pw.slab[0..quantum], end) catch |err| {
                if (err != error.FlowBlocked) return err;
                if (conn_win(win_ctx) <= 0) {
                    // The connection ran dry, not this stream: keep its place.
                    self.listPushHead(&self.ready_head, &self.ready_tail, i);
                    pw.place = .ready;
                    self.ready_len += 1;
                    if (in_front and self.fresh_tail == none) self.fresh_tail = i;
                    return false;
                }
                self.blockedPush(i);
                continue;
            };
            std.debug.assert(pw.stream_id == sid);
            const attach = attachTicket(pw, quantum);
            const rest = pw.len - quantum;
            // The class invariant `pw.len <= pw.slab.len` is what makes this
            // compaction in-bounds. `enqueueDataBytes` is the only writer
            // that can break it, so state it at the reader too.
            std.debug.assert(pw.len <= pw.slab.len);
            if (rest > 0) @memmove(pw.slab[0..rest], pw.slab[quantum..][0..rest]);
            pw.len = rest;
            pw.last_progress_ns = nowNs(self.io);
            if (end) self.startedRemove(sid);
            if (end or rest == 0) {
                // Drained: free the slot (and its slab) at once. A long-lived
                // SSE stream is idle between events, and holding a slab or an
                // admission slot across that idle time is the cost this
                // design exists to remove. Its next event is a newcomer again.
                self.freePending(pw);
            } else {
                pw.served = true;
                self.readyInsert(i);
            }
            self.recordGap();
            self.emits_total += 1;
            self.sink_origin_emits += 1;
            self.recordEmitKind(.data);
            try sink(sink_ctx, payload, end or attach[0] != 0, attach[0], attach[1], 0, false);
            return true;
        }
        return false;
    }

    pub fn classifyControl(typ: frame.FrameType, flags: frame.FrameFlags) ControlClass {
        _ = flags;
        // Only teardown frames may jump the queue. PING and SETTINGS acks used
        // to be terminal-class too, and that reordered them AHEAD of the
        // server preface SETTINGS whenever a client pipelined its preface +
        // SETTINGS into the TLS handshake flight: both intents were queued
        // before the first drain, the ack won by class, and the peer saw a
        // SETTINGS ACK as the connection's first frame — a protocol violation
        // (t-538; nghttp2 reports it as "expected SETTINGS"). Acks in source
        // order still precede all DATA, because ordinary-class drains first.
        const terminal = typ == .goaway or typ == .rst_stream;
        if (terminal) return .terminal;
        return .ordinary;
    }

    test "acks never outrank the server preface SETTINGS (t-538)" {
        // The classifier IS the fix: ack frames must be ordinary-class so the
        // FIFO ordinary queue preserves source order (preface SETTINGS first).
        const ack_flags: frame.FrameFlags = .{ .end_stream = true };
        try std.testing.expectEqual(ControlClass.ordinary, classifyControl(.settings, ack_flags));
        try std.testing.expectEqual(ControlClass.ordinary, classifyControl(.ping, ack_flags));
        try std.testing.expectEqual(ControlClass.ordinary, classifyControl(.settings, .{}));
        try std.testing.expectEqual(ControlClass.terminal, classifyControl(.goaway, .{}));
        try std.testing.expectEqual(ControlClass.terminal, classifyControl(.rst_stream, .{}));
    }

    pub fn maxEligibleGapInKinds(self: *const FairScheduler) usize {
        var gap: usize = 0;
        var max_gap: usize = 0;
        var i: usize = 0;
        while (i < self.emit_kinds_n) : (i += 1) {
            switch (self.emit_kinds[i]) {
                .ordinary_control => {
                    gap += 1;
                    if (gap > max_gap) max_gap = gap;
                },
                .data => gap = 0,
                .terminal_control => {},
            }
        }
        return max_gap;
    }

    /// Iterate every live slot, blocked ones included (Connection's sweeps).
    /// The callback may read and write the slot but must not remove it:
    /// collect ids and call `removePending` after this returns.
    pub fn forEachPending(self: *FairScheduler, ctx: *anyopaque, cb: *const fn (*anyopaque, u31, *DataPending) void) void {
        self.iterating = true;
        defer self.iterating = false;
        for (self.live[0..self.active_pending]) |i| {
            const s = &self.pending_slots.slice()[i];
            cb(ctx, s.stream_id, s);
        }
    }

    pub fn removePending(self: *FairScheduler, stream_id: u31) bool {
        const pw = self.findPending(stream_id) orelse return false;
        self.freePending(pw);
        return true;
    }
};

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn testPool(gpa: std.mem.Allocator, slab_bytes: usize, capacity: usize) !slab_pool.SlabPool {
    return slab_pool.SlabPool.init(gpa, std.testing.io, slab_bytes, capacity);
}

test "retireTerminalRst drops the stream's waiting RST with its share and payload, and keeps the order of the rest (t-3562)" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 16, 13 * 4, 4, 8, &pool);
    defer sched.deinit();
    var rst: [13]u8 = undefined;
    for ([_]u31{ 1, 3, 5 }) |sid| {
        _ = try frame.Serializer.rstStream(&rst, sid, .cancel);
        try sched.enqueueControl(try gpa.dupe(u8, &rst), .terminal, 0, 0);
    }
    try std.testing.expectEqual(@as(usize, 3), sched.ctrl.terminal_entries_held.load(.acquire));
    try std.testing.expect(sched.retireTerminalRst(3));
    try std.testing.expect(!sched.retireTerminalRst(3));
    try std.testing.expect(!sched.retireTerminalRst(7));
    try std.testing.expectEqual(@as(usize, 2), sched.terminal_len);
    try std.testing.expectEqual(@as(usize, 2), sched.ctrl.terminal_entries_held.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2 * 13), sched.ctrl.terminal_bytes_held.load(.acquire));
    for ([_]u31{ 1, 5 }) |want| {
        const e = sched.popTerminal().?;
        try std.testing.expectEqual(@as(?u31, want), rstStream(e.payload));
        sched.freePayload(e.payload);
    }
}

test "terminal frames fit their whole share with the ordinary share pinned, and not one more (t-3562)" {
    // The pool half of the bound, with no timing: frames already in flight pin
    // the ordinary share (a `forgetStream` cannot give them back), and the
    // terminal share must still take T frames. The terminal ring and the pool
    // are one argument, so the first refusal past T is whichever fills first.
    const gpa = std.testing.allocator;
    const terminal = 7;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 16, 13 * terminal, terminal, 8, &pool);
    defer sched.deinit();
    var i: usize = 0;
    while (i < 16) : (i += 1) try std.testing.expect(sched.ctrl.tryReserveOrdinary(10));
    try std.testing.expect(!sched.ctrl.tryReserveOrdinary(10));

    var rst: [13]u8 = undefined;
    _ = try frame.Serializer.rstStream(&rst, 1, .cancel);
    i = 0;
    while (i < terminal) : (i += 1) {
        try sched.enqueueControl(try gpa.dupe(u8, &rst), .terminal, 0, 0);
    }
    try std.testing.expectEqual(terminal, sched.terminal_len);
    // One more is refused, by the pool share and the ring alike.
    const extra = try gpa.dupe(u8, &rst);
    try std.testing.expectError(error.PoolFull, sched.enqueueControl(extra, .terminal, 0, 0));
    // The ordinary share is untouched by terminal frames.
    try std.testing.expectEqual(@as(usize, 16), sched.ctrl.entries_held.load(.acquire));
    // A pop gives its share back, so the next terminal frame fits.
    const popped = sched.popTerminal().?;
    sched.freePayload(popped.payload);
    try sched.enqueueControl(try gpa.dupe(u8, &rst), .terminal, 0, 0);
}

test "scheduler queues own payloads and hold control until complete" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, 8, &pool);
    defer sched.deinit();
    const p = try gpa.dupe(u8, "helloctl");
    try sched.enqueueControl(p, .ordinary, 0, 0);
    try std.testing.expectEqual(@as(usize, 1), sched.ordinary_len);
    try std.testing.expectEqual(@as(usize, 0), sched.emitted_controls_since_data);
    try std.testing.expectEqual(@as(usize, 1), sched.ctrl.entries_held.load(.acquire));
}

test "a stream rents one slab on its first bytes, and none before" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 1024, 4);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 2, &pool);
    defer sched.deinit();

    // An idle scheduler holds nothing. This is the whole point of the pool.
    try std.testing.expectEqual(@as(usize, 0), pool.rentedCount());

    try sched.enqueueDataBytes(1, "hello", false, 0, 0);
    try std.testing.expectEqual(@as(usize, 1), pool.rentedCount());
    // More bytes for the same stream reuse the slab it already holds.
    try sched.enqueueDataBytes(1, "world", false, 0, 0);
    try std.testing.expectEqual(@as(usize, 1), pool.rentedCount());
    try std.testing.expectEqual(@as(usize, 10), sched.pendingByteLen(1));

    // A second stream takes its own.
    try sched.enqueueDataBytes(3, "x", false, 0, 0);
    try std.testing.expectEqual(@as(usize, 2), pool.rentedCount());
}

test "the per-stream cap is the pool's slab length" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64, 4);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 2, &pool);
    defer sched.deinit();

    try sched.enqueueDataBytes(1, &@as([40]u8, @splat('a')), false, 0, 0);
    try std.testing.expectEqual(@as(usize, 40), sched.pendingByteLen(1));
    // Past the slab: OutOfMemory, which is the stream's own cap, distinct from
    // pool exhaustion.
    try std.testing.expectError(error.OutOfMemory, sched.enqueueDataBytes(1, &@as([40]u8, @splat('b')), false, 0, 0));
    try std.testing.expectEqual(@as(usize, 40), sched.pendingByteLen(1));
}

test "ticket requires an existing DATA frame instead of stranding" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64, 2);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 2, &pool);
    defer sched.deinit();

    try std.testing.expectError(
        error.TicketWithoutFrame,
        sched.enqueueDataBytes(1, &.{}, false, 1, 0),
    );
    try std.testing.expect(!sched.contains(1));

    // enqueuePending appends bytes first, then attaches the receipt with a
    // zero-byte call. That split is valid because the existing DATA carries it.
    try sched.enqueueDataBytes(1, "x", false, 0, 0);
    try sched.enqueueDataBytes(1, &.{}, false, 2, 1);
    const pending = sched.findPending(1).?;
    try std.testing.expectEqual(@as(u64, 2), pending.flush_ticket);
    try std.testing.expectEqual(@as(usize, 1), pending.flush_remain);
}

test "pool exhaustion is its own error, and strands no slot" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64, 2);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 8, &pool);
    defer sched.deinit();

    try sched.enqueueDataBytes(1, "a", false, 0, 0);
    try sched.enqueueDataBytes(3, "b", false, 0, 0);
    try std.testing.expectEqual(@as(usize, 2), pool.rentedCount());

    // The third stream cannot rent. It must be told so, and must not be left
    // occupying a pending slot it cannot use.
    try std.testing.expectError(error.SlabsExhausted, sched.enqueueDataBytes(5, "c", false, 0, 0));
    try std.testing.expectEqual(@as(usize, 2), pool.rentedCount());
    try std.testing.expect(!sched.contains(5));
    try std.testing.expectEqual(@as(usize, 2), sched.pendingCount());
}

test "removePending returns the slab" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64, 2);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 4, &pool);
    defer sched.deinit();

    try sched.enqueueDataBytes(1, "abc", false, 0, 0);
    try std.testing.expectEqual(@as(usize, 1), pool.rentedCount());
    try std.testing.expect(sched.removePending(1));
    try std.testing.expectEqual(@as(usize, 0), pool.rentedCount());

    // And the returned slab is usable again, so the release was real.
    try sched.enqueueDataBytes(7, "z", false, 0, 0);
    try std.testing.expectEqual(@as(usize, 1), pool.rentedCount());
    try std.testing.expect(sched.removePending(7));
}

test "deinit returns every slab it still holds" {
    // The teardown-path gate: a connection destroyed with data still queued
    // must not take slabs with it. SlabPool.deinit asserts rented == 0, so a
    // miss here fails loudly rather than starving later connections.
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64, 4);
    defer pool.deinit(gpa);
    {
        var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 4, &pool);
        defer sched.deinit();
        try sched.enqueueDataBytes(1, "abc", false, 0, 0);
        try sched.enqueueDataBytes(3, "def", false, 0, 0);
        try std.testing.expectEqual(@as(usize, 2), pool.rentedCount());
    }
    try std.testing.expectEqual(@as(usize, 0), pool.rentedCount());
}

/// A wire for tier-3 tests: per-stream and connection windows that the builder
/// debits the way `Session.makeDataFrame` does, and the order streams went out.
const TestWire = struct {
    gpa: std.mem.Allocator,
    conn: i32 = 1 << 30,
    win: [64]i32 = @splat(1 << 30),
    order: std.ArrayList(u31) = .empty,
    stream_queries: usize = 0,

    fn streamWin(ctx: *anyopaque, sid: u31) i32 {
        const t: *TestWire = @ptrCast(@alignCast(ctx));
        t.stream_queries += 1;
        const w = t.win[sid];
        if (w <= 0 or t.conn <= 0) return 0;
        return @min(w, t.conn);
    }
    fn connWin(ctx: *anyopaque) i32 {
        const t: *TestWire = @ptrCast(@alignCast(ctx));
        return t.conn;
    }
    fn build(ctx: *anyopaque, sid: u31, bytes: []const u8, end: bool) anyerror![]u8 {
        _ = end;
        const t: *TestWire = @ptrCast(@alignCast(ctx));
        if (bytes.len > 0 and (t.win[sid] <= 0 or t.conn <= 0)) return error.FlowBlocked;
        t.win[sid] -= @intCast(bytes.len);
        t.conn -= @intCast(bytes.len);
        try t.order.append(t.gpa, sid);
        return t.gpa.dupe(u8, bytes);
    }
    fn sink(ctx: *anyopaque, payload: []u8, flush: bool, ticket: u64, ticket_slot: u32, control_n: usize, control_entry: bool) anyerror!void {
        _ = .{ flush, ticket, ticket_slot, control_n, control_entry };
        const t: *TestWire = @ptrCast(@alignCast(ctx));
        t.gpa.free(payload);
    }
    fn emit(t: *TestWire, sched: *FairScheduler) !bool {
        return sched.emitOneDataPublic(t, sink, t, streamWin, connWin, build, t);
    }
    fn deinit(t: *TestWire) void {
        t.order.deinit(t.gpa);
    }
};

/// Hand HEADERS for `sids` out the way `drain` does, so a tier-3 test that
/// drives `emitOneData` directly has streams whose DATA may go.
fn testHeadersOut(sched: *FairScheduler, sids: []const u31) !void {
    for (sids) |sid| {
        var h = [_]u8{ 0, 0, 0, @backingInt(frame.FrameType.headers), 0x4, 0, 0, 0, 0 };
        std.mem.writeInt(u32, h[5..9], sid, .big);
        try sched.noteHeadersOut(&h);
    }
}

test "tier 3 serves ready streams round robin, newcomers in arrival order, not by id" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 8, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa };
    defer w.deinit();

    const body: [3 * DATA_QUANTUM]u8 = @splat('x');
    try testHeadersOut(&sched, &.{ 11, 1, 9 });
    // Arrival order is deliberately not id order.
    try sched.enqueueDataBytes(11, &body, true, 0, 0);
    try sched.enqueueDataBytes(1, &body, true, 0, 0);
    try sched.enqueueDataBytes(9, &body, true, 0, 0);
    while (try w.emit(&sched)) {}

    try std.testing.expectEqualSlices(u31, &.{ 11, 1, 9, 11, 1, 9, 11, 1, 9 }, w.order.items);
    try std.testing.expectEqual(@as(usize, 0), sched.pendingCount());
}

test "a newcomer goes out before a running stream's next quantum, and newcomers keep arrival order" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 8, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa };
    defer w.deinit();

    const bulk: [3 * DATA_QUANTUM]u8 = @splat('b');
    try testHeadersOut(&sched, &.{ 1, 3, 5 });
    try sched.enqueueDataBytes(1, &bulk, true, 0, 0);
    try std.testing.expect(try w.emit(&sched));
    // Stream 1 is now served and back on the ready list. Two short responses
    // arrive; each must go out before stream 1's second quantum, 3 before 5.
    try sched.enqueueDataBytes(3, "page", true, 0, 0);
    try sched.enqueueDataBytes(5, "page", true, 0, 0);
    while (try w.emit(&sched)) {}

    try std.testing.expectEqualSlices(u31, &.{ 1, 3, 5, 1, 1 }, w.order.items);
}

test "a quantum that spends the last connection credit does not strand its stream" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 8, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa, .conn = DATA_QUANTUM };
    defer w.deinit();

    const body: [2 * DATA_QUANTUM]u8 = @splat('c');
    try testHeadersOut(&sched, &.{1});
    try sched.enqueueDataBytes(1, &body, true, 0, 0);
    try std.testing.expect(try w.emit(&sched));
    try std.testing.expect(!(try w.emit(&sched)));
    // Out of connection credit, not stream credit: still ready, not blocked.
    try std.testing.expectEqual(@as(usize, 0), sched.blocked_len);
    try std.testing.expectEqual(DataPending.Place.ready, sched.findPending(1).?.place);

    // A connection WINDOW_UPDATE has no rearm; the stream must go out anyway.
    w.conn += 1 << 20;
    try std.testing.expect(try w.emit(&sched));
    try std.testing.expectEqualSlices(u31, &.{ 1, 1 }, w.order.items);
}

test "a stream blocked on its own window waits for rearm, and only rearm" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 8, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa };
    defer w.deinit();

    w.win[1] = 0;
    w.win[3] = 0;
    try testHeadersOut(&sched, &.{ 1, 3 });
    try sched.enqueueDataBytes(1, "one", true, 0, 0);
    try sched.enqueueDataBytes(3, "three", true, 0, 0);
    try std.testing.expect(!(try w.emit(&sched)));
    try std.testing.expectEqual(@as(usize, 2), sched.blocked_len);

    w.win[1] = 1 << 20;
    w.win[3] = 1 << 20;
    // Credit alone does not move them: the emit path never looks at blocked.
    try std.testing.expect(!(try w.emit(&sched)));
    sched.rearm(&.{1}, false);
    try std.testing.expect(try w.emit(&sched));
    try std.testing.expect(!(try w.emit(&sched)));
    sched.rearm(&.{}, true);
    try std.testing.expect(try w.emit(&sched));
    try std.testing.expectEqualSlices(u31, &.{ 1, 3 }, w.order.items);
    try std.testing.expectEqual(@as(usize, 0), sched.pendingCount());
}

test "blocked streams cost no window query on the emit path" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 16);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 16, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa };
    defer w.deinit();

    try testHeadersOut(&sched, &.{ 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 41 });
    var sid: u31 = 1;
    while (sid < 20) : (sid += 2) {
        w.win[sid] = 0;
        try sched.enqueueDataBytes(sid, "z", true, 0, 0);
    }
    const body: [3 * DATA_QUANTUM]u8 = @splat('r');
    try sched.enqueueDataBytes(41, &body, true, 0, 0);
    // The first emit pops the ten blocked heads once each, then serves 41.
    try std.testing.expect(try w.emit(&sched));
    try std.testing.expectEqual(@as(usize, 10), sched.blocked_len);
    w.stream_queries = 0;
    try std.testing.expect(try w.emit(&sched));
    try std.testing.expect(try w.emit(&sched));
    try std.testing.expectEqual(@as(usize, 2), w.stream_queries);
}

test "index delete keeps a colliding key reachable" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 256, 4, 4, &pool);
    defer sched.deinit();

    // Three odd ids with the same home slot, so they share one probe cluster.
    var ids: [3]u31 = undefined;
    var n: usize = 0;
    const home = sched.idxHome(1);
    var cand: u31 = 1;
    while (n < ids.len) : (cand += 2) {
        if (sched.idxHome(cand) == home) {
            ids[n] = cand;
            n += 1;
        }
    }
    for (ids) |id| try sched.enqueueDataBytes(id, "k", false, 0, 0);
    // Removing the first leaves a hole where the cluster starts. Without the
    // backward shift a lookup for the others stops at that hole.
    try std.testing.expect(sched.removePending(ids[0]));
    try std.testing.expectEqual(ids[1], sched.findPending(ids[1]).?.stream_id);
    try std.testing.expectEqual(ids[2], sched.findPending(ids[2]).?.stream_id);
    try std.testing.expect(sched.findPending(ids[0]) == null);
    try std.testing.expect(sched.removePending(ids[2]));
    try std.testing.expect(sched.removePending(ids[1]));
    try std.testing.expectEqual(@as(usize, 0), sched.pendingCount());
}

/// Records what `drain` emits, in order: a control by the stream id in its
/// frame header, DATA by the stream the builder framed.
const OrderWire = struct {
    gpa: std.mem.Allocator,
    events: std.ArrayList(struct { headers: bool, sid: u31 }) = .empty,
    ctrl_pause_calls: usize = 0,

    fn streamWin(_: *anyopaque, _: u31) i32 {
        return 1 << 30;
    }
    fn connWin(_: *anyopaque) i32 {
        return 1 << 30;
    }
    fn build(ctx: *anyopaque, sid: u31, bytes: []const u8, _: bool) anyerror![]u8 {
        const t: *OrderWire = @ptrCast(@alignCast(ctx));
        try t.events.append(t.gpa, .{ .headers = false, .sid = sid });
        return t.gpa.dupe(u8, bytes);
    }
    fn sink(ctx: *anyopaque, payload: []u8, _: bool, _: u64, _: u32, _: usize, control_entry: bool) anyerror!void {
        const t: *OrderWire = @ptrCast(@alignCast(ctx));
        defer t.gpa.free(payload);
        if (!control_entry) return;
        const sid: u31 = @intCast(std.mem.readInt(u32, payload[5..9], .big) & 0x7fff_ffff);
        try t.events.append(t.gpa, .{ .headers = true, .sid = sid });
    }
    /// The stash is full when the control loop asks, and has room by the
    /// time the DATA loop asks: the actor emptied it in between, without
    /// `session_mu`, while a handler task held the lock and drained.
    fn ctrlPausedOnce(ctx: *anyopaque) bool {
        const t: *OrderWire = @ptrCast(@alignCast(ctx));
        t.ctrl_pause_calls += 1;
        return t.ctrl_pause_calls == 1;
    }
    fn neverPaused(_: *anyopaque) bool {
        return false;
    }
};

test "DATA never leaves before its stream's HEADERS when the stash frees between the pause checks (t-3514)" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, 8, &pool);
    defer sched.deinit();
    var w: OrderWire = .{ .gpa = gpa };
    defer w.events.deinit(gpa);
    sched.pause = OrderWire.neverPaused;
    sched.pause_control = OrderWire.ctrlPausedOnce;
    sched.pause_ctx = &w;

    // A task handler's response: HEADERS (ordinary control) and its body.
    const hdr = [_]u8{ 0, 0, 1, @backingInt(frame.FrameType.headers), 0x4, 0, 0, 0, 7, 0x88 };
    try sched.enqueueControl(try gpa.dupe(u8, &hdr), .ordinary, 0, 0);
    try sched.enqueueDataBytes(7, "body", true, 0, 0);

    try sched.drain(&w, OrderWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);
    try sched.drain(&w, OrderWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);

    try std.testing.expectEqual(@as(usize, 2), w.events.items.len);
    try std.testing.expect(w.events.items[0].headers);
    try std.testing.expectEqual(@as(u31, 7), w.events.items[0].sid);
    try std.testing.expect(!w.events.items[1].headers);
    try std.testing.expectEqual(@as(u31, 7), w.events.items[1].sid);
}

/// Records every frame `drain` emits as (type, stream, END_HEADERS).
const FrameWire = struct {
    gpa: std.mem.Allocator,
    seen: std.ArrayList(Seen) = .empty,
    /// `pause_control` answers true for this many calls after `pause_after`
    /// calls, then false.
    ctrl_calls: usize = 0,
    pause_after: usize = 0,
    pause_for: usize = 0,

    const Seen = struct { typ: u8, sid: u31, end_headers: bool };

    fn streamWin(_: *anyopaque, _: u31) i32 {
        return 1 << 30;
    }
    fn connWin(_: *anyopaque) i32 {
        return 1 << 30;
    }
    fn build(ctx: *anyopaque, sid: u31, bytes: []const u8, _: bool) anyerror![]u8 {
        const t: *FrameWire = @ptrCast(@alignCast(ctx));
        var f = try t.gpa.alloc(u8, frame.FRAME_HEADER_LEN + bytes.len);
        @memset(f[0..frame.FRAME_HEADER_LEN], 0);
        f[3] = @backingInt(frame.FrameType.data);
        std.mem.writeInt(u32, f[5..9], sid, .big);
        @memcpy(f[frame.FRAME_HEADER_LEN..], bytes);
        return f;
    }
    fn sink(ctx: *anyopaque, payload: []u8, _: bool, _: u64, _: u32, _: usize, _: bool) anyerror!void {
        const t: *FrameWire = @ptrCast(@alignCast(ctx));
        defer t.gpa.free(payload);
        try t.seen.append(t.gpa, .{
            .typ = payload[3],
            .sid = @intCast(std.mem.readInt(u32, payload[5..9], .big) & 0x7fff_ffff),
            .end_headers = payload[4] & 0x4 != 0,
        });
    }
    fn ctrlPaused(ctx: *anyopaque) bool {
        const t: *FrameWire = @ptrCast(@alignCast(ctx));
        t.ctrl_calls += 1;
        return t.ctrl_calls > t.pause_after and t.ctrl_calls <= t.pause_after + t.pause_for;
    }
    fn neverPaused(_: *anyopaque) bool {
        return false;
    }
};

fn blockFrame(gpa: std.mem.Allocator, typ: frame.FrameType, sid: u31, end_headers: bool) ![]u8 {
    var h = [_]u8{ 0, 0, 1, @backingInt(typ), if (end_headers) 0x4 else 0, 0, 0, 0, 0, 0x88 };
    std.mem.writeInt(u32, h[5..9], sid, .big);
    return gpa.dupe(u8, &h);
}

fn rstFrame(gpa: std.mem.Allocator, sid: u31) ![]u8 {
    var h = [_]u8{ 0, 0, 4, @backingInt(frame.FrameType.rst_stream), 0, 0, 0, 0, 0, 0, 0, 0, 8 };
    std.mem.writeInt(u32, h[5..9], sid, .big);
    return gpa.dupe(u8, &h);
}

test "nothing leaves inside a header block: a terminal control, forced DATA and fresh DATA wait for its last fragment" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, 8, &pool);
    defer sched.deinit();
    var w: FrameWire = .{ .gpa = gpa };
    defer w.seen.deinit(gpa);
    sched.pause = FrameWire.neverPaused;
    sched.pause_control = FrameWire.ctrlPaused;
    sched.pause_ctx = &w;

    // Stream 9 has its HEADERS out and a body waiting: DATA is eligible the
    // whole time. Stream 7's block is HEADERS, CONTINUATION, CONTINUATION.
    try sched.enqueueControl(try blockFrame(gpa, .headers, 9, true), .ordinary, 0, 0);
    try sched.drain(&w, FrameWire.sink, &w, FrameWire.streamWin, FrameWire.connWin, FrameWire.build, &w);
    w.seen.clearRetainingCapacity();
    const bulk: [4 * DATA_QUANTUM]u8 = @splat('b');
    try sched.enqueueDataBytes(9, &bulk, true, 0, 0);
    try sched.enqueueControl(try blockFrame(gpa, .headers, 7, false), .ordinary, 0, 0);
    try sched.enqueueControl(try blockFrame(gpa, .continuation, 7, false), .ordinary, 0, 0);
    try sched.enqueueControl(try blockFrame(gpa, .continuation, 7, true), .ordinary, 0, 0);

    // The control loop's pause check is call 1 for the HEADERS pop, 2 for the
    // first CONTINUATION: pause there, and again at the second, so the drain
    // stops twice inside the block, and a terminal control arrives meanwhile.
    w.ctrl_calls = 0;
    w.pause_after = 1;
    w.pause_for = 1;
    try sched.drain(&w, FrameWire.sink, &w, FrameWire.streamWin, FrameWire.connWin, FrameWire.build, &w);
    try std.testing.expectEqual(@as(usize, 1), w.seen.items.len);
    try std.testing.expectEqual(@backingInt(frame.FrameType.headers), w.seen.items[0].typ);
    try std.testing.expect(sched.block_open);
    try sched.enqueueControl(try rstFrame(gpa, 5), .terminal, 0, 0);

    // The next drain finishes the block before anything else, then the
    // terminal control, then DATA.
    w.pause_for = 0;
    try sched.drain(&w, FrameWire.sink, &w, FrameWire.streamWin, FrameWire.connWin, FrameWire.build, &w);
    try std.testing.expect(!sched.block_open);
    const typs = w.seen.items;
    try std.testing.expectEqual(@backingInt(frame.FrameType.continuation), typs[1].typ);
    try std.testing.expectEqual(@backingInt(frame.FrameType.continuation), typs[2].typ);
    try std.testing.expect(typs[2].end_headers);
    try std.testing.expectEqual(@backingInt(frame.FrameType.rst_stream), typs[3].typ);
    try std.testing.expectEqual(@backingInt(frame.FrameType.data), typs[4].typ);
    for (typs[0..3]) |t| try std.testing.expectEqual(@as(u31, 7), t.sid);
    try std.testing.expect(sched.terminal_len == 0);
}

test "a stream reset drops the queued fragments of its header block with its HEADERS, and keeps a block already started" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, 8, &pool);
    defer sched.deinit();
    var w: FrameWire = .{ .gpa = gpa };
    defer w.seen.deinit(gpa);
    sched.pause = FrameWire.neverPaused;
    sched.pause_control = FrameWire.neverPaused;
    sched.pause_ctx = &w;

    try sched.enqueueControl(try blockFrame(gpa, .headers, 7, false), .ordinary, 0, 0);
    try sched.enqueueControl(try blockFrame(gpa, .continuation, 7, true), .ordinary, 0, 0);
    try sched.enqueueControl(try blockFrame(gpa, .headers, 9, true), .ordinary, 0, 0);
    const held_before = sched.ctrl.entries_held.load(.acquire);
    sched.forgetStream(7);
    try std.testing.expectEqual(held_before - 2, sched.ctrl.entries_held.load(.acquire));
    try sched.drain(&w, FrameWire.sink, &w, FrameWire.streamWin, FrameWire.connWin, FrameWire.build, &w);
    try std.testing.expectEqual(@as(usize, 1), w.seen.items.len);
    try std.testing.expectEqual(@as(u31, 9), w.seen.items[0].sid);

    // HEADERS already out: the CONTINUATION behind it is the peer's due.
    w.seen.clearRetainingCapacity();
    w.ctrl_calls = 0;
    w.pause_after = 1;
    w.pause_for = 1;
    sched.pause_control = FrameWire.ctrlPaused;
    try sched.enqueueControl(try blockFrame(gpa, .headers, 11, false), .ordinary, 0, 0);
    try sched.enqueueControl(try blockFrame(gpa, .continuation, 11, true), .ordinary, 0, 0);
    try sched.drain(&w, FrameWire.sink, &w, FrameWire.streamWin, FrameWire.connWin, FrameWire.build, &w);
    try std.testing.expectEqual(@as(usize, 1), w.seen.items.len);
    sched.forgetStream(11);
    sched.pause_control = FrameWire.neverPaused;
    try sched.drain(&w, FrameWire.sink, &w, FrameWire.streamWin, FrameWire.connWin, FrameWire.build, &w);
    try std.testing.expectEqual(@as(usize, 2), w.seen.items.len);
    try std.testing.expectEqual(@backingInt(frame.FrameType.continuation), w.seen.items[1].typ);
    try std.testing.expect(!sched.block_open);
}

fn orderHeaders(gpa: std.mem.Allocator, sid: u31, end_stream: bool) ![]u8 {
    var h = [_]u8{ 0, 0, 1, @backingInt(frame.FrameType.headers), 0x4, 0, 0, 0, 0, 0x88 };
    if (end_stream) h[4] |= 0x1;
    std.mem.writeInt(u32, h[5..9], sid, .big);
    return gpa.dupe(u8, &h);
}

/// Every DATA in `events` follows a HEADERS for its stream.
fn expectHeadersBeforeData(events: anytype) !void {
    var seen: [64]bool = @splat(false);
    for (events) |e| {
        if (e.headers) seen[e.sid] = true else try std.testing.expect(seen[e.sid]);
    }
}

test "the anti-starvation yield does not send DATA before its stream's HEADERS" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, 8, &pool);
    defer sched.deinit();
    var w: OrderWire = .{ .gpa = gpa };
    defer w.events.deinit(gpa);

    // HEADERS 9 first, then 64 other controls, then HEADERS and a body for
    // stream 7. The yield fires after the 64th control, while HEADERS 7
    // still waits. Stream 9's body is queued after 7's, so 7 would be the
    // ready head if it were eligible. With 7 held back the yield serves 9,
    // which shows the yield did fire.
    try sched.enqueueControl(try orderHeaders(gpa, 9, false), .ordinary, 0, 0);
    var k: usize = 0;
    while (k < control_pool.CONTROL_BEFORE_DATA) : (k += 1) {
        try sched.enqueueControl(try orderHeaders(gpa, 61, true), .ordinary, 0, 0);
    }
    try sched.enqueueControl(try orderHeaders(gpa, 7, false), .ordinary, 0, 0);
    try sched.enqueueDataBytes(7, "body", true, 0, 0);
    const bulk: [4 * DATA_QUANTUM]u8 = @splat('b');
    try sched.enqueueDataBytes(9, &bulk, true, 0, 0);

    try sched.drain(&w, OrderWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);

    try expectHeadersBeforeData(w.events.items);
    var data9_before_h7 = false;
    for (w.events.items) |e| {
        if (e.headers and e.sid == 7) break;
        if (!e.headers and e.sid == 9) data9_before_h7 = true;
    }
    try std.testing.expect(data9_before_h7);
    try std.testing.expectEqual(@as(usize, 0), sched.pendingCount());
    try std.testing.expectEqual(@as(usize, 0), sched.started_len);
}

test "a stream waiting for its HEADERS is not visited, and is served once they leave" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, 8, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa };
    defer w.deinit();

    // Only streams whose HEADERS has not gone out. A complete handler stages
    // DATA before its HEADERS reaches the queue, so this is a real state.
    try sched.enqueueDataBytes(7, "body", true, 0, 0);
    try sched.enqueueDataBytes(9, "body", true, 0, 0);
    try std.testing.expect(!(try w.emit(&sched)));
    try std.testing.expect(!(try w.emit(&sched)));
    // Not spinning: no window was asked, nothing moved to the blocked list.
    try std.testing.expectEqual(@as(usize, 0), w.stream_queries);
    try std.testing.expectEqual(@as(usize, 0), sched.blocked_len);
    try std.testing.expectEqual(@as(usize, 0), sched.ready_len);
    try std.testing.expectEqual(@as(usize, 2), sched.pendingCount());

    // HEADERS 9 leaves first, then 7: they go out in that order.
    try testHeadersOut(&sched, &.{ 9, 7 });
    while (try w.emit(&sched)) {}
    try std.testing.expectEqualSlices(u31, &.{ 9, 7 }, w.order.items);
    try std.testing.expectEqual(@as(usize, 0), sched.started_len);
}

test "HEADERS state outlives a slot freed between writes, and ends at END_STREAM" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, 8, &pool);
    defer sched.deinit();
    var w: OrderWire = .{ .gpa = gpa };
    defer w.events.deinit(gpa);

    // An SSE-shaped stream: HEADERS, one event, idle, another event.
    try sched.enqueueControl(try orderHeaders(gpa, 7, false), .ordinary, 0, 0);
    try sched.enqueueDataBytes(7, "event one", false, 0, 0);
    try sched.drain(&w, OrderWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);
    try std.testing.expectEqual(@as(usize, 0), sched.pendingCount());
    try std.testing.expectEqual(@as(usize, 1), sched.started_len);

    try sched.enqueueDataBytes(7, "event two", true, 0, 0);
    try sched.drain(&w, OrderWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);
    try std.testing.expectEqual(@as(usize, 3), w.events.items.len);
    try std.testing.expect(!w.events.items[2].headers);
    try expectHeadersBeforeData(w.events.items);
    try std.testing.expectEqual(@as(usize, 0), sched.started_len);
}

test "a reset ends the HEADERS state; trailers do not gate and end it too" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, 8, &pool);
    defer sched.deinit();
    var w: OrderWire = .{ .gpa = gpa };
    defer w.events.deinit(gpa);

    try testHeadersOut(&sched, &.{ 5, 7 });
    try std.testing.expectEqual(@as(usize, 2), sched.started_len);
    sched.forgetStream(5);
    try std.testing.expectEqual(@as(usize, 1), sched.started_len);

    // Trailers: DATA without END_STREAM, then an END_STREAM HEADERS.
    try sched.enqueueDataBytes(7, "body", false, 0, 0);
    try sched.drain(&w, OrderWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);
    try sched.enqueueControl(try orderHeaders(gpa, 7, true), .ordinary, 0, 0);
    try sched.drain(&w, OrderWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);
    try std.testing.expectEqual(@as(usize, 2), w.events.items.len);
    try std.testing.expect(!w.events.items[0].headers);
    try std.testing.expect(w.events.items[1].headers);
    try std.testing.expectEqual(@as(usize, 0), sched.started_len);
}

test "framed DATA waits for its stream's HEADERS too" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, 8, &pool);
    defer sched.deinit();
    var w: OrderWire = .{ .gpa = gpa };
    defer w.events.deinit(gpa);
    sched.pause = OrderWire.neverPaused;
    sched.pause_control = OrderWire.ctrlPausedOnce;
    sched.pause_ctx = &w;

    // Framed DATA is already a frame; the sink sees it as non-control.
    var d = [_]u8{ 0, 0, 1, @backingInt(frame.FrameType.data), 0x1, 0, 0, 0, 7, 'x' };
    try sched.enqueueControl(try orderHeaders(gpa, 7, false), .ordinary, 0, 0);
    try sched.enqueueFramedData(try gpa.dupe(u8, &d), 7, true, 0, 0);

    try sched.drain(&w, FramedWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);
    try std.testing.expectEqual(@as(usize, 0), w.events.items.len);
    try sched.drain(&w, FramedWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);
    try std.testing.expectEqual(@as(usize, 2), w.events.items.len);
    try expectHeadersBeforeData(w.events.items);
    try std.testing.expectEqual(@as(usize, 0), sched.started_len);
}

/// `OrderWire` for framed DATA: a non-control payload is a whole frame, so
/// its stream id is in the header rather than known to the builder.
const FramedWire = struct {
    fn sink(ctx: *anyopaque, payload: []u8, _: bool, _: u64, _: u32, _: usize, control_entry: bool) anyerror!void {
        const t: *OrderWire = @ptrCast(@alignCast(ctx));
        defer t.gpa.free(payload);
        const sid: u31 = @intCast(std.mem.readInt(u32, payload[5..9], .big) & 0x7fff_ffff);
        try t.events.append(t.gpa, .{ .headers = control_entry, .sid = sid });
    }
};

test "a reset before its queued HEADERS reaches the sink leaves no started entry, past capacity" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    const max_streams = 8;
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, max_streams, &pool);
    defer sched.deinit();
    var w: OrderWire = .{ .gpa = gpa };
    defer w.events.deinit(gpa);

    // Each round is a task handler that queued its HEADERS without a drain,
    // then the peer's RST: the connection forgets the stream before the next
    // drain reaches that HEADERS. Twice the set's capacity.
    var sid: u31 = 1;
    var round: usize = 0;
    while (round < 2 * max_streams) : (round += 1) {
        try sched.enqueueControl(try orderHeaders(gpa, sid, false), .ordinary, 0, 0);
        sched.forgetStream(sid);
        try sched.drain(&w, OrderWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);
        sid += 2;
    }
    try std.testing.expectEqual(@as(usize, 0), sched.started_len);
    // Nothing for a reset stream reached the wire.
    try std.testing.expectEqual(@as(usize, 0), w.events.items.len);
    try std.testing.expectEqual(@as(usize, 0), sched.ordinary_len);
    try std.testing.expectEqual(@as(usize, 0), sched.ctrl.entries_held.load(.acquire));
}

test "a reset drops only that stream's queued HEADERS, with its ticket and control entry" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 4 * 1024, 16, 8, &pool);
    defer sched.deinit();
    var w: OrderWire = .{ .gpa = gpa };
    defer w.events.deinit(gpa);
    const Failed = struct {
        ticket: u64 = 0,
        slot: u32 = 0,
        calls: usize = 0,
        fn f(ctx: *anyopaque, ticket: u64, slot: u32) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            self.ticket = ticket;
            self.slot = slot;
        }
    };
    var failed: Failed = .{};
    sched.ticket_fail_ctx = &failed;
    sched.ticket_fail = Failed.f;

    try sched.enqueueControl(try orderHeaders(gpa, 1, false), .ordinary, 0, 0);
    // An SSE start: its HEADERS carries the ticket the handler waits on.
    try sched.enqueueControl(try orderHeaders(gpa, 3, false), .ordinary, 41, 7);
    try sched.enqueueControl(try orderHeaders(gpa, 5, false), .ordinary, 0, 0);
    try std.testing.expectEqual(@as(usize, 3), sched.ctrl.entries_held.load(.acquire));

    sched.forgetStream(3);
    try std.testing.expectEqual(@as(usize, 1), failed.calls);
    try std.testing.expectEqual(@as(u64, 41), failed.ticket);
    try std.testing.expectEqual(@as(u32, 7), failed.slot);
    try std.testing.expectEqual(@as(usize, 2), sched.ordinary_len);
    try std.testing.expectEqual(@as(usize, 2), sched.ctrl.entries_held.load(.acquire));

    try sched.drain(&w, OrderWire.sink, &w, OrderWire.streamWin, OrderWire.connWin, OrderWire.build, &w);
    try std.testing.expectEqual(@as(usize, 2), w.events.items.len);
    try std.testing.expectEqual(@as(u31, 1), w.events.items[0].sid);
    try std.testing.expectEqual(@as(u31, 5), w.events.items[1].sid);
    sched.forgetStream(1);
    sched.forgetStream(5);
    try std.testing.expectEqual(@as(usize, 0), sched.started_len);
}
