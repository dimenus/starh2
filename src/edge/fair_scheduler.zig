//! Production fair write scheduler — owned by Connection actor.
//! Terminal/required ACK → ordinary controls+HEADERS → 16KiB DRR DATA.
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
//! # No allocation on the write path
//!
//! Pending DATA lives in per-stream slabs carved from ONE boot allocation, and
//! a slot is a fixed array entry. The id index and the lists over the slots
//! are fixed arrays sized at `init` too. A handler write is a
//! `memcpy` into an existing slab; when the slab is full the handler parks in
//! `Connection.waitForStreamSpace`. That is the mechanism behind the promise
//! that a connection stays inside `Limits.resourceUpperBound` under any load.
const std = @import("std");
const control_pool = @import("control_pool.zig");
const slab_pool = @import("slab_pool.zig");
const frame = @import("../core/frame.zig");

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
    }
};

const none: u32 = std.math.maxInt(u32);

/// Slots in the stream-id index: a power of two at least twice the slot
/// count, so the load stays at or below one half and a miss stops at an
/// empty entry within a short probe. Exported for `bound_shapes`.
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
    /// Fixed slots — one per max concurrent stream; slab storage contiguous.
    pending_slots: []DataPending,
    /// Borrowed, server-owned. Slabs are rented when a stream first queues
    /// DATA and returned the moment it has none, so an idle stream — and an
    /// idle connection — holds none at all.
    pool: *slab_pool.SlabPool,
    /// When set, control/framed payloads may be frame-pool slabs. Null (tests)
    /// means every payload is a GPA allocation of the on-wire size.
    payload_free_ctx: ?*anyopaque = null,
    payload_free: ?*const fn (*anyopaque, []u8) void = null,
    active_pending: usize = 0,
    /// Open-addressed `stream_id -> slot index`, linear probing, deleted by
    /// backward shift so no tombstones accumulate under stream churn. Key 0 is
    /// empty (stream id 0 is never a stream).
    idx_keys: []u31,
    idx_vals: []u32,
    idx_shift: u5,
    /// Every occupied slot, dense, for the walks that must see blocked
    /// streams too (`forEachPending`, `nextSlowDeadlineNs`).
    live: []u32,
    free_stack: []u32,
    free_len: usize,
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

    fn isPaused(self: *FairScheduler) bool {
        const f = self.pause orelse return false;
        return f(self.pause_ctx);
    }

    fn isControlPaused(self: *FairScheduler) bool {
        const f = self.pause_control orelse return false;
        return f(self.pause_ctx);
    }

    pub fn init(
        gpa: std.mem.Allocator,
        io: std.Io,
        ctrl_bytes: usize,
        ctrl_entries: usize,
        terminal_cap: usize,
        ordinary_cap: usize,
        max_streams: usize,
        pool: *slab_pool.SlabPool,
    ) !FairScheduler {
        const tq = try gpa.alloc(ControlEntry, @max(terminal_cap, 1));
        errdefer gpa.free(tq);
        const oq = try gpa.alloc(ControlEntry, @max(ordinary_cap, 1));
        errdefer gpa.free(oq);
        const fq = try gpa.alloc(FramedDataEntry, @max(ordinary_cap, 1));
        errdefer gpa.free(fq);
        std.debug.assert(max_streams < none);
        const slots = try gpa.alloc(DataPending, max_streams);
        errdefer gpa.free(slots);
        @memset(slots, .{});
        const idx_cap = indexCapacity(max_streams);
        const keys = try gpa.alloc(u31, idx_cap);
        errdefer gpa.free(keys);
        @memset(keys, 0);
        const vals = try gpa.alloc(u32, idx_cap);
        errdefer gpa.free(vals);
        const live = try gpa.alloc(u32, max_streams);
        errdefer gpa.free(live);
        const free_stack = try gpa.alloc(u32, max_streams);
        errdefer gpa.free(free_stack);
        // Pop order hands out slot 0 first, which only matters for diagnostics.
        for (free_stack, 0..) |*f, i| f.* = @intCast(max_streams - 1 - i);
        return .{
            .gpa = gpa,
            .io = io,
            .ctrl = control_pool.ControlPool.init(ctrl_bytes, ctrl_entries),
            .terminal_q = tq,
            .ordinary_q = oq,
            .framed_data_q = fq,
            .pending_slots = slots,
            .pool = pool,
            .idx_keys = keys,
            .idx_vals = vals,
            .idx_shift = @intCast(32 - std.math.log2_int(usize, idx_cap)),
            .live = live,
            .free_stack = free_stack,
            .free_len = max_streams,
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

    pub fn deinit(self: *FairScheduler) void {
        while (self.popTerminal()) |e| self.freePayload(e.payload);
        while (self.popOrdinary()) |e| self.freePayload(e.payload);
        while (self.popFramed()) |e| self.freePayload(e.payload);
        self.gpa.free(self.terminal_q);
        self.gpa.free(self.ordinary_q);
        self.gpa.free(self.framed_data_q);
        // A slot still holding a slab here means some teardown path missed it.
        // Return them rather than leaking into the shared pool, where the loss
        // would starve every other connection instead of just this one.
        for (self.pending_slots) |*slot| self.returnSlab(slot);
        self.gpa.free(self.pending_slots);
        self.gpa.free(self.idx_keys);
        self.gpa.free(self.idx_vals);
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

    pub fn popTerminal(self: *FairScheduler) ?ControlEntry {
        return popRing(ControlEntry, self.terminal_q, &self.terminal_head, &self.terminal_len);
    }
    pub fn popOrdinary(self: *FairScheduler) ?ControlEntry {
        return popRing(ControlEntry, self.ordinary_q, &self.ordinary_head, &self.ordinary_len);
    }
    pub fn popFramed(self: *FairScheduler) ?FramedDataEntry {
        return popRing(FramedDataEntry, self.framed_data_q, &self.framed_head, &self.framed_len);
    }

    fn idxHome(self: *const FairScheduler, stream_id: u31) usize {
        return (@as(u32, stream_id) *% 0x9E3779B1) >> self.idx_shift;
    }

    fn idxFind(self: *const FairScheduler, stream_id: u31) ?u32 {
        std.debug.assert(stream_id != 0);
        const mask = self.idx_keys.len - 1;
        var i = self.idxHome(stream_id);
        // Terminates: the table is at most half full, so an empty key exists.
        while (true) : (i = (i + 1) & mask) {
            const k = self.idx_keys[i];
            if (k == 0) return null;
            if (k == stream_id) return self.idx_vals[i];
        }
    }

    fn idxInsert(self: *FairScheduler, stream_id: u31, slot: u32) void {
        const mask = self.idx_keys.len - 1;
        var i = self.idxHome(stream_id);
        while (self.idx_keys[i] != 0) : (i = (i + 1) & mask) {
            std.debug.assert(self.idx_keys[i] != stream_id);
        }
        self.idx_keys[i] = stream_id;
        self.idx_vals[i] = slot;
    }

    /// Backward-shift delete. Each later key in the cluster moves into the
    /// hole unless its home lies cyclically in (hole, its position], in which
    /// case moving it would put it before its own home and a lookup would
    /// stop at the hole. A plain clear (or a tombstone) is what this replaces.
    fn idxRemove(self: *FairScheduler, stream_id: u31) void {
        const mask = self.idx_keys.len - 1;
        var hole = self.idxHome(stream_id);
        while (self.idx_keys[hole] != stream_id) : (hole = (hole + 1) & mask) {
            std.debug.assert(self.idx_keys[hole] != 0);
        }
        var j = hole;
        while (true) {
            j = (j + 1) & mask;
            const k = self.idx_keys[j];
            if (k == 0) break;
            const home = self.idxHome(k);
            const stays = if (hole <= j) (hole < home and home <= j) else (hole < home or home <= j);
            if (stays) continue;
            self.idx_keys[hole] = k;
            self.idx_vals[hole] = self.idx_vals[j];
            hole = j;
        }
        self.idx_keys[hole] = 0;
    }

    fn slotIndex(self: *const FairScheduler, pw: *const DataPending) u32 {
        return @intCast((@intFromPtr(pw) - @intFromPtr(self.pending_slots.ptr)) / @sizeOf(DataPending));
    }

    fn listPushTail(self: *FairScheduler, head: *u32, tail: *u32, i: u32) void {
        const s = &self.pending_slots[i];
        s.next = none;
        s.prev = tail.*;
        if (tail.* != none) self.pending_slots[tail.*].next = i else head.* = i;
        tail.* = i;
    }

    fn listPushHead(self: *FairScheduler, head: *u32, tail: *u32, i: u32) void {
        const s = &self.pending_slots[i];
        s.prev = none;
        s.next = head.*;
        if (head.* != none) self.pending_slots[head.*].prev = i else tail.* = i;
        head.* = i;
    }

    fn listUnlink(self: *FairScheduler, head: *u32, tail: *u32, i: u32) void {
        const s = &self.pending_slots[i];
        if (s.prev != none) self.pending_slots[s.prev].next = s.next else head.* = s.next;
        if (s.next != none) self.pending_slots[s.next].prev = s.prev else tail.* = s.prev;
        s.next = none;
        s.prev = none;
    }

    /// Ready list, front segment for never-served slots. The segment is a
    /// prefix ending at `fresh_tail`; newcomers append to it in arrival order.
    fn readyInsert(self: *FairScheduler, i: u32) void {
        const s = &self.pending_slots[i];
        std.debug.assert(s.place == .idle);
        if (!s.served) {
            if (self.fresh_tail == none) {
                self.listPushHead(&self.ready_head, &self.ready_tail, i);
            } else {
                const after = self.fresh_tail;
                const nxt = self.pending_slots[after].next;
                s.prev = after;
                s.next = nxt;
                self.pending_slots[after].next = i;
                if (nxt != none) self.pending_slots[nxt].prev = i else self.ready_tail = i;
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
        if (self.fresh_tail == i) self.fresh_tail = self.pending_slots[i].prev;
        self.listUnlink(&self.ready_head, &self.ready_tail, i);
        self.pending_slots[i].place = .idle;
        self.ready_len -= 1;
    }

    fn blockedPush(self: *FairScheduler, i: u32) void {
        std.debug.assert(self.pending_slots[i].place == .idle);
        self.listPushTail(&self.blocked_head, &self.blocked_tail, i);
        self.pending_slots[i].place = .blocked;
        self.blocked_len += 1;
    }

    fn blockedUnlink(self: *FairScheduler, i: u32) void {
        self.listUnlink(&self.blocked_head, &self.blocked_tail, i);
        self.pending_slots[i].place = .idle;
        self.blocked_len -= 1;
    }

    /// Put a live slot that has a DATA frame to send on the ready list, unless
    /// it is already on a list. A blocked slot stays blocked: more bytes do not
    /// change its window.
    fn arm(self: *FairScheduler, pw: *DataPending) void {
        if (pw.place != .idle) return;
        if (pw.len == 0 and !pw.end_stream) return;
        self.readyInsert(self.slotIndex(pw));
    }

    pub fn findPending(self: *FairScheduler, stream_id: u31) ?*DataPending {
        if (stream_id == 0) return null;
        const i = self.idxFind(stream_id) orelse return null;
        return &self.pending_slots[i];
    }

    fn allocPending(self: *FairScheduler, stream_id: u31) error{StreamCap}!*DataPending {
        if (self.findPending(stream_id)) |s| return s;
        if (self.free_len == 0) return error.StreamCap;
        self.free_len -= 1;
        const i = self.free_stack[self.free_len];
        const s = &self.pending_slots[i];
        std.debug.assert(s.stream_id == 0 and s.place == .free);
        s.clear();
        s.stream_id = stream_id;
        s.place = .idle;
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

    pub fn nextSlowDeadlineNs(self: *const FairScheduler, timeout_ns: u64) ?u64 {
        if (timeout_ns == 0) return null;
        var next: ?u64 = null;
        for (self.live[0..self.active_pending]) |i| {
            const slot = self.pending_slots[i];
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
            self.pending_slots[last].live_pos = pw.live_pos;
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
                self.ctrl.release(n, 1);
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
            if (self.pending_slots[i].place != .blocked) continue;
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
        while (self.terminal_len > 0) {
            if (self.isControlPaused()) break;
            const e = self.popTerminal() orelse break;
            self.emits_total += 1;
            self.sink_origin_emits += 1;
            self.recordEmitKind(.terminal_control);
            try sink(sink_ctx, e.payload, true, e.ticket, e.ticket_slot, e.control_n, true);
        }
        while (self.ordinary_len > 0) {
            if (self.isControlPaused()) break;
            if (!self.isPaused() and self.shouldForceDataNow() and self.dataEligible(win_ctx, conn_win)) {
                if (try self.emitOneData(sink_ctx, sink, win_ctx, stream_win, conn_win, build_data_frame, build_ctx)) {
                    continue;
                }
            }
            const e = self.popOrdinary() orelse break;
            self.emits_total += 1;
            self.sink_origin_emits += 1;
            self.recordEmitKind(.ordinary_control);
            self.emitted_controls_since_data += 1;
            self.ctrl.noteControl();
            try sink(sink_ctx, e.payload, true, e.ticket, e.ticket_slot, e.control_n, true);
        }
        while (true) {
            if (self.isPaused()) return;
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
        if (conn_win(win_ctx) <= 0) return false;

        if (self.framed_len > 0) {
            const head = self.framed_data_q[self.framed_head];
            if (stream_win(win_ctx, head.stream_id) > 0 or self.active_pending == 0) {
                const e = self.popFramed() orelse return false;
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
            const pw = &self.pending_slots[i];
            const sid = pw.stream_id;
            // `arm` only lists a slot with a frame to send, and every path that
            // empties one frees it.
            std.debug.assert(pw.len > 0 or pw.end_stream);
            const avail = stream_win(win_ctx, sid);
            if (avail <= 0) {
                self.blockedPush(i);
                continue;
            }
            const quantum = if (pw.len == 0) 0 else @min(pw.len, DATA_QUANTUM, @as(usize, @intCast(avail)));
            const end = pw.end_stream and quantum == pw.len;
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
            const s = &self.pending_slots[i];
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

test "scheduler queues own payloads and hold control until complete" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 16, 256, 8, &pool);
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
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 2, &pool);
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
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 2, &pool);
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
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 2, &pool);
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
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 8, &pool);
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
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 4, &pool);
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
        var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 4, &pool);
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

test "tier 3 serves ready streams round robin, newcomers in arrival order, not by id" {
    const gpa = std.testing.allocator;
    var pool = try testPool(gpa, 64 * 1024, 8);
    defer pool.deinit(gpa);
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 8, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa };
    defer w.deinit();

    const body: [3 * DATA_QUANTUM]u8 = @splat('x');
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
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 8, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa };
    defer w.deinit();

    const bulk: [3 * DATA_QUANTUM]u8 = @splat('b');
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
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 8, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa, .conn = DATA_QUANTUM };
    defer w.deinit();

    const body: [2 * DATA_QUANTUM]u8 = @splat('c');
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
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 8, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa };
    defer w.deinit();

    w.win[1] = 0;
    w.win[3] = 0;
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
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 16, &pool);
    defer sched.deinit();
    var w: TestWire = .{ .gpa = gpa };
    defer w.deinit();

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
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 4, &pool);
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
