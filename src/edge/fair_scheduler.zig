//! Production fair write scheduler — owned by Connection actor.
//! Terminal/required ACK → ordinary controls+HEADERS → 16KiB DRR DATA.
//! Pending DATA uses boot-reserved per-stream slabs (no post-boot GPA growth).
//! Nonterminal controls counted only when emitted.
const std = @import("std");
const control_pool = @import("control_pool.zig");
const frame = @import("../core/frame.zig");

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
pub const DataPending = struct {
    stream_id: u31 = 0, // 0 = free slot
    slab: []u8 = &.{},
    len: usize = 0,
    end_stream: bool = false,
    last_progress_ns: u64 = 0,
    flush_ticket: u64 = 0,
    flush_slot: u32 = 0,
    flush_remain: usize = 0,

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
    }
};

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
    slab_storage: []u8,
    slab_bytes_per_stream: usize,
    active_pending: usize = 0,
    drr_cursor: u31 = 1,
    data_quanta: usize = 0,
    emitted_controls_since_data: usize = 0,
    control_gaps: [128]usize = .{0} ** 128,
    gap_i: usize = 0,
    sid_scratch: []u31,
    emits_total: usize = 0,
    emit_kinds: [512]EmitKind = undefined,
    emit_kinds_n: usize = 0,
    sink_origin_emits: usize = 0,

    pub fn init(
        gpa: std.mem.Allocator,
        io: std.Io,
        ctrl_bytes: usize,
        ctrl_entries: usize,
        terminal_cap: usize,
        ordinary_cap: usize,
        max_streams: usize,
        slab_bytes_per_stream: usize,
    ) !FairScheduler {
        const tq = try gpa.alloc(ControlEntry, @max(terminal_cap, 1));
        errdefer gpa.free(tq);
        const oq = try gpa.alloc(ControlEntry, @max(ordinary_cap, 1));
        errdefer gpa.free(oq);
        const fq = try gpa.alloc(FramedDataEntry, @max(ordinary_cap, 1));
        errdefer gpa.free(fq);
        const scratch = try gpa.alloc(u31, max_streams);
        errdefer gpa.free(scratch);
        const slots = try gpa.alloc(DataPending, max_streams);
        errdefer gpa.free(slots);
        const slab_total = max_streams * @max(slab_bytes_per_stream, 1);
        const slabs = try gpa.alloc(u8, slab_total);
        errdefer gpa.free(slabs);
        @memset(slots, .{});
        var i: usize = 0;
        while (i < max_streams) : (i += 1) {
            const off = i * @max(slab_bytes_per_stream, 1);
            slots[i].slab = slabs[off..][0..@max(slab_bytes_per_stream, 1)];
        }
        return .{
            .gpa = gpa,
            .io = io,
            .ctrl = control_pool.ControlPool.init(ctrl_bytes, ctrl_entries),
            .terminal_q = tq,
            .ordinary_q = oq,
            .framed_data_q = fq,
            .pending_slots = slots,
            .slab_storage = slabs,
            .slab_bytes_per_stream = @max(slab_bytes_per_stream, 1),
            .sid_scratch = scratch,
        };
    }

    pub fn deinit(self: *FairScheduler) void {
        while (self.popTerminal()) |e| self.gpa.free(e.payload);
        while (self.popOrdinary()) |e| self.gpa.free(e.payload);
        while (self.popFramed()) |e| self.gpa.free(e.payload);
        self.gpa.free(self.terminal_q);
        self.gpa.free(self.ordinary_q);
        self.gpa.free(self.framed_data_q);
        self.gpa.free(self.pending_slots);
        self.gpa.free(self.slab_storage);
        self.gpa.free(self.sid_scratch);
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
    fn popFramed(self: *FairScheduler) ?FramedDataEntry {
        return popRing(FramedDataEntry, self.framed_data_q, &self.framed_head, &self.framed_len);
    }

    pub fn findPending(self: *FairScheduler, stream_id: u31) ?*DataPending {
        for (self.pending_slots) |*s| {
            if (s.stream_id == stream_id) return s;
        }
        return null;
    }

    fn allocPending(self: *FairScheduler, stream_id: u31) error{StreamCap}!*DataPending {
        if (self.findPending(stream_id)) |s| return s;
        for (self.pending_slots) |*s| {
            if (s.stream_id == 0) {
                s.clear();
                s.stream_id = stream_id;
                self.active_pending += 1;
                return s;
            }
        }
        return error.StreamCap;
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
        for (self.pending_slots) |slot| {
            if (slot.stream_id == 0 or slot.len == 0 or slot.last_progress_ns == 0) continue;
            const deadline = slot.last_progress_ns +% timeout_ns;
            if (next == null or deadline < next.?) next = deadline;
        }
        return next;
    }

    pub fn contains(self: *FairScheduler, stream_id: u31) bool {
        return self.findPending(stream_id) != null;
    }

    fn freePending(self: *FairScheduler, pw: *DataPending) void {
        if (pw.stream_id != 0) {
            std.debug.assert(self.active_pending > 0);
            self.active_pending -= 1;
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
        if (class == .terminal) {
            if (!self.ctrl.tryReserveTerminal(payload.len)) {
                self.gpa.free(payload);
                return error.PoolFull;
            }
            pushRing(ControlEntry, self.terminal_q, &self.terminal_head, &self.terminal_tail, &self.terminal_len, .{
                .payload = payload,
                .class = .terminal,
                .ticket = ticket,
                .ticket_slot = ticket_slot,
                .control_n = payload.len,
            }) catch {
                self.ctrl.release(payload.len, true);
                self.gpa.free(payload);
                return error.QueueFull;
            };
        } else {
            if (!self.ctrl.tryReserveOrdinary(payload.len)) {
                self.gpa.free(payload);
                return error.PoolFull;
            }
            pushRing(ControlEntry, self.ordinary_q, &self.ordinary_head, &self.ordinary_tail, &self.ordinary_len, .{
                .payload = payload,
                .class = .ordinary,
                .ticket = ticket,
                .ticket_slot = ticket_slot,
                .control_n = payload.len,
            }) catch {
                self.ctrl.release(payload.len, true);
                self.gpa.free(payload);
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
            self.gpa.free(payload);
            return error.QueueFull;
        };
    }

    /// Append into boot-reserved slab — never grows, never calls GPA.
    pub fn enqueueDataBytes(
        self: *FairScheduler,
        stream_id: u31,
        bytes: []const u8,
        end: bool,
        flush_ticket: u64,
        flush_slot: u32,
        max_per_stream: usize,
    ) error{ OutOfMemory, StreamCap }!void {
        _ = max_per_stream;
        const pw = try self.allocPending(stream_id);
        if (pw.len + bytes.len > pw.slab.len) return error.OutOfMemory;
        @memcpy(pw.slab[pw.len..][0..bytes.len], bytes);
        pw.len += bytes.len;
        if (end) pw.end_stream = true;
        pw.last_progress_ns = nowNs(self.io);
        if (flush_ticket != 0) {
            pw.flush_ticket = flush_ticket;
            pw.flush_slot = flush_slot;
            pw.flush_remain = pw.len;
        }
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

    fn dataEligible(
        self: *FairScheduler,
        win_ctx: *anyopaque,
        stream_win: WindowQuery,
        conn_win: ConnWindowQuery,
    ) bool {
        if (conn_win(win_ctx) <= 0) return false;
        if (self.framed_len > 0) return true;
        for (self.pending_slots) |*s| {
            if (s.stream_id == 0) continue;
            if (stream_win(win_ctx, s.stream_id) > 0 and (s.len > 0 or s.end_stream)) return true;
        }
        return false;
    }

    pub fn shouldForceDataNow(self: *const FairScheduler) bool {
        return self.emitted_controls_since_data >= control_pool.CONTROL_BEFORE_DATA;
    }

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
        while (self.popTerminal()) |e| {
            self.emits_total += 1;
            self.sink_origin_emits += 1;
            self.recordEmitKind(.terminal_control);
            try sink(sink_ctx, e.payload, true, e.ticket, e.ticket_slot, e.control_n, true);
        }
        while (self.ordinary_len > 0) {
            if (self.shouldForceDataNow() and self.dataEligible(win_ctx, stream_win, conn_win)) {
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
        while (try self.emitOneData(sink_ctx, sink, win_ctx, stream_win, conn_win, build_data_frame, build_ctx)) {}
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

        if (self.active_pending == 0) return false;
        var n_ids: usize = 0;
        for (self.pending_slots) |*s| {
            if (s.stream_id == 0) continue;
            if (n_ids >= self.sid_scratch.len) break;
            self.sid_scratch[n_ids] = s.stream_id;
            n_ids += 1;
        }
        std.mem.sort(u31, self.sid_scratch[0..n_ids], {}, comptime std.sort.asc(u31));
        var start: usize = 0;
        for (self.sid_scratch[0..n_ids], 0..) |id, i| {
            if (id >= self.drr_cursor) {
                start = i;
                break;
            }
        }
        var i: usize = 0;
        while (i < n_ids) : (i += 1) {
            const sid = self.sid_scratch[(start + i) % n_ids];
            const avail = stream_win(win_ctx, sid);
            if (avail <= 0) continue;
            const pw = self.findPending(sid) orelse continue;
            if (pw.len == 0 and !pw.end_stream) {
                self.freePending(pw);
                continue;
            }
            const quantum = if (pw.len == 0) 0 else @min(pw.len, DATA_QUANTUM, @as(usize, @intCast(avail)));
            if (pw.len > 0 and quantum == 0) continue;
            const end = pw.end_stream and quantum == pw.len;
            const slice = pw.slab[0..quantum];
            // Build frame first (Session debit). On sink failure Connection fail-closes;
            // pending may also be cleared by nested cancel during build — shrink only if still ours.
            // Comment historically said “commit shrink only after sink succeeds”; Session debit
            // must precede sink, so we shrink after successful build and fail-close on sink error.
            const payload = build_data_frame(build_ctx, sid, slice, end) catch |err| {
                if (err == error.FlowBlocked) continue;
                return err;
            };
            var attach: struct { u64, u32 } = .{ 0, 0 };
            if (pw.stream_id == sid) {
                attach = attachTicket(pw, quantum);
                const take = @min(quantum, pw.len);
                const rest = pw.len - take;
                if (rest > 0) @memmove(pw.slab[0..rest], pw.slab[take..][0..rest]);
                pw.len = rest;
                pw.last_progress_ns = nowNs(self.io);
                if (end or (pw.len == 0 and pw.end_stream)) {
                    self.freePending(pw);
                }
            }
            self.drr_cursor = sid +% 2;
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

    /// Iterate active pending for slow-consumer sweeps (Connection).
    pub fn forEachPending(self: *FairScheduler, ctx: *anyopaque, cb: *const fn (*anyopaque, u31, *DataPending) void) void {
        for (self.pending_slots) |*s| {
            if (s.stream_id == 0) continue;
            cb(ctx, s.stream_id, s);
        }
    }

    pub fn takePending(self: *FairScheduler, stream_id: u31) ?DataPending {
        const pw = self.findPending(stream_id) orelse return null;
        var copy = pw.*;
        // Detach slab ownership for caller cleanup of logical state only — slab stays in pool.
        copy.slab = pw.slab; // same backing; caller must not free
        self.freePending(pw);
        // Return a snapshot with len/tickets; bytes still in shared slab until overwritten.
        return copy;
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

test "scheduler queues own payloads and hold control until complete" {
    const gpa = std.testing.allocator;
    var sched = try FairScheduler.init(gpa, std.testing.io, 64 * 1024, 256, 16, 256, 8, 64 * 1024);
    defer sched.deinit();
    const p = try gpa.dupe(u8, "helloctl");
    try sched.enqueueControl(p, .ordinary, 0, 0);
    try std.testing.expectEqual(@as(usize, 1), sched.ordinary_len);
    try std.testing.expectEqual(@as(usize, 0), sched.emitted_controls_since_data);
    try std.testing.expectEqual(@as(usize, 1), sched.ctrl.entries_held.load(.acquire));
}

test "pending DATA uses boot slab without GPA growth" {
    const gpa = std.testing.allocator;
    var sched = try FairScheduler.init(gpa, std.testing.io, 1024, 32, 4, 32, 2, 1024);
    defer sched.deinit();
    try sched.enqueueDataBytes(1, "hello", false, 0, 0, 1024);
    try std.testing.expectEqual(@as(usize, 5), sched.pendingByteLen(1));
    try std.testing.expectError(error.OutOfMemory, sched.enqueueDataBytes(1, &[_]u8{'x'} ** 2000, false, 0, 0, 1024));
}
