//! Deterministic HTTP/2 connection state machine. No I/O, no clocks.
//!
//! `Session` is the protocol authority for one connection. It is the only place
//! that decides what HTTP/2 means, and the edge is the only place that touches
//! a socket or a clock. The split is the central design choice of this stack,
//! and it buys three things:
//!
//! - A protocol decision is reproducible. The same byte sequence produces the
//!   same intents on every run, so a conformance failure is a test case and not
//!   a timing story.
//! - No lock lives in here. `Connection` serializes every entry point behind
//!   `session_mu`, so `Session` may assume single-threaded access.
//! - The edge cannot invent protocol. It may only apply `AppCommand` values and
//!   execute `Intent` values.
//!
//! ## The flow
//!
//! Inbound:  wire bytes -> `ingest` -> `frame.Parser` -> `handleFrame`
//!           -> stream state -> `Intent`.
//! Outbound: handler -> `AppCommand` -> `applyCommand` -> `Intent`.
//!
//! `Session` never writes. It appends an `Intent`, and the edge drains the
//! intent ring and turns each entry into wire bytes or into a handler dispatch.
//! Two consequences follow, and both have cost real defects:
//!
//! - An intent is queued, not sent. A caller that needs bytes on the wire in a
//!   given ORDER must materialize the intents at the right moment. See the
//!   HEADERS-before-DATA note in `edge.connection.sendCb` (t-482).
//! - `edge_now_ns` is a value the edge stores before each call, and not a clock
//!   this module reads. Every deadline below compares against that stored
//!   value.
//!
//! ## Ownership
//!
//! An inbound `FrameEvent` is released by `handleFrame` through `deinit`: a
//! scratch borrow is a no-op there, a test `gpa` payload is freed there.
//! An `Intent` carries owned memory: a frame payload, or a decoded request.
//! Header-list bytes on the live edge are arena-backed (the handler-job arena
//! leased at decode); isolated Session tests still GPA-own them. `pushIntent`
//! transfers that ownership only when it returns success, so an
//! `error.PoolExhausted` leaves the caller still owning the payload. Whoever
//! drains an intent must release it with `releaseIntent` exactly once.
const std = @import("std");
const frame = @import("frame.zig");
const hpack = @import("hpack.zig");
const fields = @import("fields.zig");
const stream_mod = @import("stream.zig");
const flow = @import("flow.zig");
const limits_mod = @import("limits.zig");
const rates_mod = @import("rates.zig");

/// Work that the edge must perform. `Session` decides; the edge executes.
/// An intent never contains a callback or a pointer back into edge state, so
/// the protocol layer cannot reach into the actor and cannot be re-entered.
pub const Intent = union(enum) {
    outbound_frame: OutboundFrame,
    dispatch_request: DispatchRequest,
    stream_reset: struct { stream_id: u31, code: frame.ErrorCode, from_peer: bool = false },
    connection_error: struct { code: frame.ErrorCode, last_stream_id: u31 },
    connection_closed,
    /// Exactly one early HTTP response (431/414/413); no handler dispatch.
    early_reject: struct { stream_id: u31, status: u16 },
};

pub const OutboundFrame = struct {
    typ: frame.FrameType,
    flags: frame.FrameFlags = .{},
    stream_id: u31 = 0,
    payload: []u8, // session-owned; released after write
};

pub const DispatchRequest = struct {
    stream_id: u31,
    method: []const u8,
    scheme: []const u8,
    authority: []const u8,
    path: []const u8,
    query: []const u8,
    headers: []hpack.HeaderField,
    body: []const u8,
    trailers: []hpack.HeaderField,
};

/// The handler-facing direction: everything an application may ask of the
/// connection. The set is closed on purpose. A handler cannot build a frame,
/// so it cannot violate the protocol, and every response path is auditable
/// from this one union.
pub const AppCommand = union(enum) {
    respond_headers: struct {
        stream_id: u31,
        status: u16,
        headers: []const hpack.HeaderField,
        end_stream: bool,
    },
    respond_data: struct {
        stream_id: u31,
        data: []const u8,
        end_stream: bool,
    },
    reset_stream: struct { stream_id: u31, code: frame.ErrorCode },
    ping: [8]u8,
    goaway: struct { last_stream_id: u31, code: frame.ErrorCode },
    /// Start graceful shutdown phase 1: GOAWAY(max) + PING (edge waits for ACK/timeout).
    graceful_phase1,
    /// Phase 2: GOAWAY(highest accepted); refuse later streams.
    graceful_phase2,
};

pub const Terminal = union(enum) {
    none,
    goaway: struct { code: frame.ErrorCode, last_stream_id: u31 },
    transport,
};

const Tombstone = struct {
    id: u31,
    code: frame.ErrorCode,
};

fn encodeDataFrameInto(out: []u8, stream_id: u31, data: []const u8, end_stream: bool) usize {
    const total = frame.FRAME_HEADER_LEN + data.len;
    std.debug.assert(out.len >= total);
    const fh = frame.FrameHeader{
        .length = @intCast(data.len),
        .type = .data,
        .flags = .{ .end_stream = end_stream },
        .stream_id = stream_id,
    };
    fh.encode(out[0..frame.FRAME_HEADER_LEN]);
    if (data.len > 0) @memcpy(out[frame.FRAME_HEADER_LEN..][0..data.len], data);
    return total;
}

/// t-1002 probe hook. Core stays clock-free and I/O-free, so the print body
/// lives at the edge; a binary installs `edge.connection.closeProbeSessionPrint`
/// here. Fired on the HEADERS-on-closed-stream branches and on
/// `failConnectionWith`, so a full-suite h2spec run shows whether the Session
/// DECIDED to close, separately from whether the edge materialized the close.
pub const CloseProbeSite = enum { headers_on_tombstone, headers_on_closed, fail_connection, data_on_closed_ignored, headers_seen, hpack_decode_fail };
pub const CloseProbeEvent = struct {
    session: usize,
    site: CloseProbeSite,
    code: frame.ErrorCode,
    stream_id: u32,
};
pub var close_probe_fn: ?*const fn (ev: CloseProbeEvent) void = null;

pub const Session = struct {
    /// Heap that outlives any one ingest or command.
    gpa: std.mem.Allocator,
    limits: limits_mod.Limits,
    parser: frame.Parser,
    decoder: hpack.Decoder,
    windows: flow.Windows = .{},
    streams: std.AutoHashMap(u31, stream_mod.Stream),
    tombstones: std.ArrayList(Tombstone),
    tombstone_next: usize = 0,
    /// Fixed-capacity intent ring — enqueue fails before ownership transfer when full.
    intent_storage: []Intent = &.{},
    intent_len: usize = 0,
    /// Slots reserved for GOAWAY / connection_error so PoolExhausted can still fail-close.
    /// A connection that fills its intent ring must still be able to say WHY it
    /// is closing. Without the reserve the failure path needs the resource it
    /// just ran out of, so the connection would die silently and the peer would
    /// see only a dropped socket.
    terminal_reserve: usize = 2,
    highest_peer_stream: u31 = 0,
    last_processed_stream: u31 = 0,
    active_streams: usize = 0,
    peer_max_frame_size: u32 = frame.DEFAULT_MAX_FRAME_SIZE,
    peer_initial_window: i32 = flow.INITIAL_WINDOW,
    peer_header_table_size: u32 = 4096,
    received_peer_settings: bool = false,
    settings_acked: bool = false,
    sent_preface: bool = false,
    expect_continuation: ?u31 = null,
    header_block: std.ArrayList(u8) = .empty,
    continuation_count: usize = 0,
    header_end_stream: bool = false,
    terminal: Terminal = .none,
    request_storage_used: usize = 0,
    // Pending assembled requests awaiting dispatch (owned)
    pending_bodies: std.AutoHashMap(u31, std.ArrayList(u8)),
    pending_headers: std.AutoHashMap(u31, []hpack.HeaderField),
    pending_trailers: std.AutoHashMap(u31, []hpack.HeaderField),
    /// After graceful phase 2 (or protocol GOAWAY), refuse peer stream IDs above this.
    goaway_ceiling: ?u31 = null,
    peer_goaway: bool = false,
    grace_phase: enum { none, phase1, phase2 } = .none,
    grace_ping: [8]u8 = .{ 0x73, 0x68, 0x32, 0x67, 0x72, 0x61, 0x63, 0x65 }, // "sh2grace"
    grace_ping_acked: bool = false,
    control_frames_since_data: usize = 0,
    /// Optional server-wide stream admission (global max_streams_per_server).
    /// A per-connection limit alone does not bound a server: N connections at
    /// their own limits still exceed any global budget. The hooks let the edge
    /// enforce the server-wide counters at the exact moment `Session` admits a
    /// stream or accepts request bytes, while `Session` stays free of any
    /// knowledge about other connections. `Session` calls `onRelease` exactly
    /// once per successful `tryAdmit`, including on the teardown path in
    /// `deinit`; a missed release leaks server capacity for the process
    /// lifetime.
    stream_hooks: ?StreamHooks = null,
    /// Edge-owned rate limiter + clock snapshot. Null → rates disabled.
    rate_limiter: ?*rates_mod.RateLimiter = null,
    edge_now_ns: u64 = 0,
    /// Wall-clock start of unfinished field block (0 = idle).
    field_block_started_ns: u64 = 0,
    /// Last request-body DATA progress per stream (0 = unset).
    body_idle_ns: std.AutoHashMap(u31, u64) = undefined,

    pub const StreamHooks = struct {
        ctx: *anyopaque,
        tryAdmit: *const fn (*anyopaque) bool,
        onRelease: *const fn (*anyopaque) void,
        tryReserveRequest: ?*const fn (*anyopaque, usize) bool = null,
        releaseRequest: ?*const fn (*anyopaque, usize) void = null,
        /// Route body cap for this method and path. Null hook uses `Limits.request_body_bytes`.
        requestBodyCap: ?*const fn (*anyopaque, method: []const u8, path: []const u8) usize = null,
        /// Allocator for one stream's decoded request-list (not the dynamic table).
        headerListAlloc: ?*const fn (*anyopaque, u31) std.mem.Allocator = null;
        /// Reset that allocator after a decode that will not be dispatched.
        discardHeaderList: ?*const fn (*anyopaque, u31) void = null,
        /// Rent a boot-reserved outbound frame slab. `n` is the on-wire size.
        /// Returns a full slab (`len == slab_bytes >= n`) or null (too large or
        /// exhausted — caller GPA-falls-back). Never a used prefix.
        rentFrame: ?*const fn (*anyopaque, usize) ?[]u8 = null,
        /// Return a buffer from `rentFrame` or a GPA fallback. Pointer-range
        /// decides which; a prefix of a slab is reconstructed to the full lease.
        releaseFrame: ?*const fn (*anyopaque, []u8) void = null,
    };

    fn routeBodyCap(self: *const Session, method: []const u8, path: []const u8) usize {
        const global = self.limits.request_body_bytes;
        if (self.stream_hooks) |h| {
            if (h.requestBodyCap) |f| return @min(f(h.ctx, method, path), global);
        }
        return global;
    }

    fn headerListAllocator(self: *Session, stream_id: u31) std.mem.Allocator {
        if (self.stream_hooks) |h| {
            if (h.headerListAlloc) |f| return f(h.ctx, stream_id);
        }
        return self.gpa;
    }

    fn headerListIsGpa(self: *const Session) bool {
        if (self.stream_hooks) |h| return h.headerListAlloc == null;
        return true;
    }

    fn discardHeaderList(self: *Session, stream_id: u31) void {
        if (self.stream_hooks) |h| {
            if (h.discardHeaderList) |f| f(h.ctx, stream_id);
        }
    }

    fn allocFrame(self: *Session, n: usize) error{OutOfMemory}![]u8 {
        if (self.stream_hooks) |h| {
            if (h.rentFrame) |f| {
                if (f(h.ctx, n)) |slab| return slab;
            }
        }
        return self.gpa.alloc(u8, n);
    }

    fn freeFrame(self: *Session, buf: []u8) void {
        if (buf.len == 0) return;
        if (self.stream_hooks) |h| {
            if (h.releaseFrame) |f| {
                f(h.ctx, buf);
                return;
            }
        }
        self.gpa.free(buf);
    }

    fn dropDecoded(self: *Session, stream_id: u31, decoded: hpack.Decoder.DecodeResult) void {
        if (self.headerListIsGpa()) {
            self.decoder.freeResult(decoded);
            return;
        }
        self.discardHeaderList(stream_id);
    }

    fn dropPendingRequest(self: *Session, stream_id: u31) void {
        const had_headers = self.pending_headers.fetchRemove(stream_id);
        const had_trailers = self.pending_trailers.fetchRemove(stream_id);
        if (self.headerListIsGpa()) {
            if (had_headers) |e| {
                hpack.HeaderField.freeOwnedSlice(self.gpa, e.value);
                self.gpa.free(e.value);
            }
            if (had_trailers) |e| {
                hpack.HeaderField.freeOwnedSlice(self.gpa, e.value);
                if (e.value.len != 0) self.gpa.free(e.value);
            }
        } else if (had_headers != null or had_trailers != null) {
            self.discardHeaderList(stream_id);
        }
        if (self.pending_bodies.fetchRemove(stream_id)) |e| {
            var list = e.value;
            if (self.stream_hooks) |h| {
                if (h.releaseRequest) |rel| rel(h.ctx, list.items.len);
            }
            list.deinit(self.gpa);
        }
    }

    pub fn init(gpa: std.mem.Allocator, limits: limits_mod.Limits) !Session {
        const intent_cap = @max(limits.intent_entries_per_connection, 16);
        const intent_storage = try gpa.alloc(Intent, intent_cap);
        var intent_storage_owned = true;
        errdefer if (intent_storage_owned) gpa.free(intent_storage);
        var s: Session = .{
            .gpa = gpa,
            .limits = limits,
            .parser = try frame.Parser.initReserved(gpa, frame.DEFAULT_MAX_FRAME_SIZE),
            .decoder = hpack.Decoder.init(gpa),
            .streams = std.AutoHashMap(u31, stream_mod.Stream).init(gpa),
            .tombstones = .empty,
            .intent_storage = intent_storage,
            .intent_len = 0,
            .terminal_reserve = 2,
            .pending_bodies = std.AutoHashMap(u31, std.ArrayList(u8)).init(gpa),
            .pending_headers = std.AutoHashMap(u31, []hpack.HeaderField).init(gpa),
            .pending_trailers = std.AutoHashMap(u31, []hpack.HeaderField).init(gpa),
            .body_idle_ns = std.AutoHashMap(u31, u64).init(gpa),
        };
        intent_storage_owned = false;
        errdefer s.deinit();
        try s.streams.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.tombstones.ensureTotalCapacity(gpa, limits.stream_tombstones);
        try s.pending_bodies.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.pending_headers.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.pending_trailers.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.body_idle_ns.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.header_block.ensureTotalCapacity(gpa, frame.DEFAULT_MAX_FRAME_SIZE);
        // The preface is queued at construction, before the peer has said
        // anything. RFC 9113 requires the server's SETTINGS to be the first
        // frame it sends, and the only way to guarantee that is to make it the
        // first intent that can possibly exist. See
        // `edge.fair_scheduler.classifyControl` for the ordering defect that
        // appeared when a later ack could overtake this frame (t-538).
        try s.emitServerPreface();
        return s;
    }

    pub fn deinit(self: *Session) void {
        // Connection teardown must release every still-admitted stream exactly once.
        var sit = self.streams.iterator();
        while (sit.next()) |e| {
            if (!e.value_ptr.concurrency_released) {
                e.value_ptr.concurrency_released = true;
                if (self.active_streams > 0) self.active_streams -= 1;
                if (self.stream_hooks) |h| h.onRelease(h.ctx);
            }
        }
        self.parser.deinit();
        self.decoder.deinit();
        self.clearIntents();
        if (self.intent_storage.len != 0) self.gpa.free(self.intent_storage);
        self.tombstones.deinit(self.gpa);
        self.header_block.deinit(self.gpa);
        var bit = self.pending_bodies.iterator();
        while (bit.next()) |e| {
            if (self.stream_hooks) |h| {
                if (h.releaseRequest) |rel| rel(h.ctx, e.value_ptr.items.len);
            }
            e.value_ptr.deinit(self.gpa);
        }
        self.pending_bodies.deinit();
        var hit = self.pending_headers.iterator();
        while (hit.next()) |e| {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, e.value_ptr.*);
                self.gpa.free(e.value_ptr.*);
            }
        }
        self.pending_headers.deinit();
        var tit = self.pending_trailers.iterator();
        while (tit.next()) |e| {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, e.value_ptr.*);
                self.gpa.free(e.value_ptr.*);
            }
        }
        self.pending_trailers.deinit();
        self.body_idle_ns.deinit();
        self.streams.deinit();
    }

    fn clearIntents(self: *Session) void {
        var i: usize = 0;
        while (i < self.intent_len) : (i += 1) {
            self.releaseIntent(&self.intent_storage[i]);
        }
        self.intent_len = 0;
    }

    pub fn releaseIntent(self: *Session, it: *Intent) void {
        switch (it.*) {
            .outbound_frame => |*f| self.freeFrame(f.payload),
            .dispatch_request => |*d| {
                if (self.headerListIsGpa()) {
                    hpack.HeaderField.freeOwnedSlice(self.gpa, d.headers);
                    self.gpa.free(d.headers);
                    hpack.HeaderField.freeOwnedSlice(self.gpa, d.trailers);
                    if (d.trailers.len != 0) self.gpa.free(d.trailers);
                } else {
                    self.discardHeaderList(d.stream_id);
                }
                self.gpa.free(d.body);
            },
            else => {},
        }
    }

    fn intentIsTerminal(intent: Intent) bool {
        return switch (intent) {
            .outbound_frame => |f| f.typ == .goaway,
            .connection_error, .connection_closed => true,
            else => false,
        };
    }

    /// Enqueue before ownership transfer completes — on PoolExhausted caller still owns payloads.
    pub fn pushIntent(self: *Session, intent: Intent) error{PoolExhausted}!void {
        const limit = if (intentIsTerminal(intent))
            self.intent_storage.len
        else
            self.intent_storage.len -| self.terminal_reserve;
        if (self.intent_len >= limit) return error.PoolExhausted;
        self.intent_storage[self.intent_len] = intent;
        self.intent_len += 1;
    }

    fn pushIntentOrFailClosed(self: *Session, intent: Intent) !void {
        self.pushIntent(intent) catch {
            self.releaseIntent(@constCast(&intent));
            try self.failClosedInternal();
            return error.PoolExhausted;
        };
    }

    /// Last resort when an intent cannot be queued. The connection is already
    /// unable to express itself, so this path may not allocate a new intent
    /// slot through the normal route — it spends the terminal reserve directly
    /// and must never recurse through `pushIntentOrFailClosed`.
    ///
    /// Every step degrades rather than gives up: if even the reserved GOAWAY
    /// cannot be queued, `terminal` is still set, so the actor loop breaks and
    /// the edge tears the connection down. The rule is that a broken connection
    /// closes; it never continues in an unknown protocol state.
    pub fn failClosedInternal(self: *Session) !void {
        if (self.terminal != .none) return;
        // Use reserved terminal capacity — must not recurse through pushIntentOrFailClosed.
        const last = self.last_processed_stream;
        var buf: [17]u8 = undefined;
        const n = try frame.Serializer.goaway(&buf, last, .internal_error, &.{});
        const p = self.gpa.dupe(u8, buf[0..n]) catch return error.OutOfMemory;
        self.pushIntent(.{ .outbound_frame = .{ .typ = .goaway, .payload = p } }) catch {
            self.gpa.free(p);
            self.terminal = .{ .goaway = .{ .code = .internal_error, .last_stream_id = last } };
            return;
        };
        self.pushIntent(.{ .connection_error = .{ .code = .internal_error, .last_stream_id = last } }) catch {
            self.terminal = .{ .goaway = .{ .code = .internal_error, .last_stream_id = last } };
            return;
        };
        self.terminal = .{ .goaway = .{ .code = .internal_error, .last_stream_id = last } };
    }

    /// Copy pending intents into caller-owned batch (same capacity). Never drops silently.
    pub fn drainIntentsInto(self: *Session, out: []Intent) usize {
        const n = self.intent_len;
        std.debug.assert(n <= out.len);
        if (n != 0) @memcpy(out[0..n], self.intent_storage[0..n]);
        self.intent_len = 0;
        return n;
    }

    /// Test/compat: drain into storage view. Do not pushIntent until finished with the slice.
    pub fn drainIntents(self: *Session) []Intent {
        const n = self.intent_len;
        self.intent_len = 0;
        return self.intent_storage[0..n];
    }

    pub fn releaseDrainedIntents(_: *Session, _: []Intent) void {}

    pub fn intentCapacity(self: *const Session) usize {
        return self.intent_storage.len;
    }

    pub fn intentLen(self: *const Session) usize {
        return self.intent_len;
    }

    /// Build framed DATA without touching the intent queue (avoids nested drain).
    ///
    /// The scheduler calls this while it drains, so the bytes must NOT become
    /// an intent: the edge would then drain an intent ring that it is already
    /// inside. Flow-control credit is debited here, at the moment the frame
    /// exists, so the debit and the frame cannot separate. A caller that gets
    /// `error.FlowBlocked` has caused no debit and may retry the same bytes
    /// unchanged after a WINDOW_UPDATE.
    pub fn makeDataFrame(self: *Session, stream_id: u31, data: []const u8, end_stream: bool) ![]u8 {
        const s = self.streams.getPtr(stream_id) orelse {
            // Scheduler still holds the body after releaseConcurrency dropped
            // the map entry. Treating that as StreamClosed/zero credit makes
            // emitOneData skip forever and the actor parks until slow-consumer.
            // A non-no_error tombstone is a RST: do not emit DATA after it.
            if (data.len > 16 * 1024) return error.FlowBlocked;
            if (data.len > 0 and self.windows.conn_send <= 0) return error.FlowBlocked;
            if (self.tombstoneCode(stream_id)) |code| {
                if (code != .no_error) return error.FlowBlocked;
            }
            const out = try self.allocFrame(frame.FRAME_HEADER_LEN + data.len);
            _ = encodeDataFrameInto(out, stream_id, data, end_stream);
            if (data.len > 0) self.windows.conn_send -= @intCast(data.len);
            return out;
        };
        const avail = s.window.availableSend(self.windows.conn_send);
        if (data.len == 0) {
            if (!end_stream) return error.FlowBlocked;
        } else if (avail <= 0) {
            return error.FlowBlocked;
        }
        const take: usize = if (data.len == 0)
            0
        else
            @min(data.len, @as(usize, @intCast(avail)), 16 * 1024);
        if (data.len > 0 and take < data.len) return error.FlowBlocked;
        const out = try self.allocFrame(frame.FRAME_HEADER_LEN + take);
        _ = encodeDataFrameInto(out, stream_id, data[0..take], end_stream);
        if (take > 0) {
            s.window.send -= @intCast(take);
            self.windows.conn_send -= @intCast(take);
        }
        if (end_stream) {
            s.localEndStream();
            self.releaseConcurrency(stream_id, .no_error);
        }
        return out;
    }

    fn emitServerPreface(self: *Session) !void {
        var buf: [256]u8 = undefined;
        const n = try frame.Serializer.settingsFrame(&buf, false, &frame.serverPrefaceSettings);
        const payload = try self.gpa.dupe(u8, buf[0..n]);
        // Store as raw bytes frame — edge writes verbatim. Use typ=settings with full frame in payload.
        // Simpler: outbound is complete wire bytes in a synthetic frame.
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{
            .typ = .settings,
            .payload = payload,
            .stream_id = 0,
            .flags = .{},
        } });
        // Connection window raise to 4MiB
        const incr: u31 = @intCast(flow.CONNECTION_RECV_TARGET - flow.INITIAL_WINDOW);
        var wbuf: [13]u8 = undefined;
        const wn = try frame.Serializer.windowUpdate(&wbuf, 0, incr);
        const wp = try self.gpa.dupe(u8, wbuf[0..wn]);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{
            .typ = .window_update,
            .payload = wp,
            .stream_id = 0,
        } });
        self.windows.conn_recv = flow.CONNECTION_RECV_TARGET;
        self.sent_preface = true;
    }

    pub fn ingest(self: *Session, chunk: []const u8) !void {
        if (self.terminal != .none) return;
        var remaining = chunk;
        while (remaining.len > 0) {
            const maybe = self.parser.ingestOne(remaining) catch |err| {
                const code: frame.ErrorCode = switch (err) {
                    error.ProtocolError => .protocol_error,
                    error.FrameSizeError => .frame_size_error,
                    error.EnhanceYourCalm => .enhance_your_calm,
                    error.NeedMore => unreachable,
                };
                try self.failConnection(code);
                return;
            };
            if (maybe) |res| {
                try self.handleFrame(res.event);
                remaining = remaining[res.consumed..];
            } else break;
        }
    }

    pub fn applyCommand(self: *Session, cmd: AppCommand) !void {
        if (self.terminal != .none) return;
        switch (cmd) {
            .respond_headers => |r| try self.cmdHeaders(r.stream_id, r.status, r.headers, r.end_stream),
            .respond_data => |r| try self.cmdData(r.stream_id, r.data, r.end_stream),
            .reset_stream => |r| try self.emitRst(r.stream_id, r.code),
            .ping => |opaque_data| {
                var buf: [17]u8 = undefined;
                const n = try frame.Serializer.ping(&buf, false, &opaque_data);
                const p = try self.gpa.dupe(u8, buf[0..n]);
                try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .ping, .payload = p } });
            },
            .goaway => |g| try self.failConnectionWith(g.code, g.last_stream_id),
            .graceful_phase1 => try self.beginGracefulPhase1(),
            .graceful_phase2 => try self.beginGracefulPhase2(),
        }
    }

    fn emitGoawayNoTerminal(self: *Session, last: u31, code: frame.ErrorCode) !void {
        var buf: [32]u8 = undefined;
        const n = try frame.Serializer.goaway(&buf, last, code, &.{});
        const p = try self.gpa.dupe(u8, buf[0..n]);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .goaway, .payload = p } });
    }

    fn beginGracefulPhase1(self: *Session) !void {
        if (self.grace_phase != .none or self.terminal != .none) return;
        // Graceful shutdown: GOAWAY NO_ERROR with last-stream-ID 2^31-1, then PING.
        try self.emitGoawayNoTerminal(std.math.maxInt(u31), .no_error);
        var buf: [17]u8 = undefined;
        const n = try frame.Serializer.ping(&buf, false, &self.grace_ping);
        const p = try self.gpa.dupe(u8, buf[0..n]);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .ping, .payload = p } });
        self.grace_phase = .phase1;
        self.grace_ping_acked = false;
    }

    fn beginGracefulPhase2(self: *Session) !void {
        if (self.terminal != .none) return;
        const last = self.highest_peer_stream;
        try self.emitGoawayNoTerminal(last, .no_error);
        self.goaway_ceiling = last;
        self.grace_phase = .phase2;
    }

    pub fn gracePingAcked(self: *const Session) bool {
        return self.grace_ping_acked;
    }

    fn failConnection(self: *Session, code: frame.ErrorCode) !void {
        try self.failConnectionWith(code, self.last_processed_stream);
    }

    fn failConnectionWith(self: *Session, code: frame.ErrorCode, last: u31) !void {
        if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .fail_connection, .code = code, .stream_id = last });
        var buf: [32]u8 = undefined;
        const n = try frame.Serializer.goaway(&buf, last, code, &.{});
        const p = try self.gpa.dupe(u8, buf[0..n]);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .goaway, .payload = p } });
        try self.pushIntentOrFailClosed(.{ .connection_error = .{ .code = code, .last_stream_id = last } });
        self.terminal = .{ .goaway = .{ .code = code, .last_stream_id = last } };
    }

    /// Return the stream's concurrency credit and drop its map entry.
    ///
    /// The map entry cannot live for the whole connection, or a long-lived
    /// connection accumulates one entry per request it ever served. A dropped
    /// entry, however, makes a late frame for that stream look like a frame for
    /// an IDLE stream, and the protocol treats those two cases very
    /// differently. The bounded tombstone list closes that gap: it remembers
    /// that the id was used and how it ended, at 8 bytes per entry instead of a
    /// whole `Stream`.
    ///
    /// `concurrency_released` makes the release idempotent. Several paths can
    /// reach a closed stream — a peer RST, a local RST, END_STREAM in both
    /// directions — and a double release would corrupt the server-wide stream
    /// count.
    fn releaseConcurrency(self: *Session, stream_id: u31, tombstone_code: frame.ErrorCode) void {
        const s = self.streams.getPtr(stream_id) orelse return;
        if (s.concurrency_released) return;
        if (s.state != .closed) return;
        s.concurrency_released = true;
        if (self.active_streams > 0) self.active_streams -= 1;
        if (self.stream_hooks) |h| h.onRelease(h.ctx);
        // Drop map entry; late frames classify via bounded tombstones.
        // The live-stream entry proves this ID has not yet been tombstoned:
        // stream IDs cannot be reused, and every close removes the entry below.
        // Passing the terminal code here avoids a linear duplicate scan on
        // every normal response while preserving the peer/local RST code.
        self.addTombstone(stream_id, tombstone_code) catch {};
        self.dropPendingRequest(stream_id);
        _ = self.streams.remove(stream_id);
    }

    fn emitRst(self: *Session, stream_id: u31, code: frame.ErrorCode) !void {
        var buf: [13]u8 = undefined;
        const n = try frame.Serializer.rstStream(&buf, stream_id, code);
        const p = try self.gpa.dupe(u8, buf[0..n]);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .rst_stream, .payload = p, .stream_id = stream_id } });
        try self.pushIntentOrFailClosed(.{ .stream_reset = .{ .stream_id = stream_id, .code = code, .from_peer = false } });
        if (self.streams.getPtr(stream_id)) |s| {
            s.onRst();
        }
        self.releaseConcurrency(stream_id, code);
    }

    /// Oldest-first ring overwrite, because a peer decides how many streams it opens.
    /// An unbounded list would grow with the connection's lifetime, which is a
    /// memory target. Losing the oldest tombstone is safe: a frame for a stream
    /// that closed that long ago falls back to the generic closed-stream rules.
    fn addTombstone(self: *Session, id: u31, code: frame.ErrorCode) !void {
        const capacity = self.limits.stream_tombstones;
        if (capacity == 0) return;
        if (self.tombstones.items.len < capacity) {
            try self.tombstones.append(self.gpa, .{ .id = id, .code = code });
            return;
        }
        self.tombstones.items[self.tombstone_next] = .{ .id = id, .code = code };
        self.tombstone_next = (self.tombstone_next + 1) % capacity;
    }

    /// One frame, fully validated, in the order the RFC requires the checks.
    ///
    /// The three gates below run BEFORE the type switch, and the order matters:
    ///
    /// 1. The peer's first frame must be a non-ack SETTINGS. Anything else means
    ///    the peer is not speaking HTTP/2, so no later state is trustworthy.
    /// 2. An unfinished field block accepts only a CONTINUATION for the same
    ///    stream. HPACK state spans the fragments, so any other frame in the
    ///    gap would leave the decoder unable to read the rest of the connection.
    /// 3. `already_terminal` is captured first so that a protocol error wins
    ///    over a rate-limit error raised by the same frame. The peer must learn
    ///    the more specific reason.
    fn handleFrame(self: *Session, ev: frame.FrameEvent) !void {
        defer ev.deinit(self.gpa);
        const hdr = ev.header;
        const payload = ev.payload.bytes();
        const already_terminal = self.terminal != .none;

        if (!self.received_peer_settings) {
            if (hdr.type != .settings or hdr.flags.ack()) {
                try self.failConnection(.protocol_error);
                return;
            }
            self.received_peer_settings = true;
        }

        if (self.expect_continuation) |cid| {
            if (hdr.type != .continuation or hdr.stream_id != cid) {
                try self.failConnection(.protocol_error);
                return;
            }
            try self.consumeContinuation(hdr, payload);
            try self.chargeRate(hdr.type, already_terminal);
            return;
        }

        switch (hdr.type) {
            .data => try self.onData(hdr, payload),
            .headers => try self.onHeaders(hdr, payload),
            .priority => {
                if (hdr.stream_id == 0 or hdr.length != 5) {
                    try self.failConnection(if (hdr.length != 5) .frame_size_error else .protocol_error);
                }
            },
            .rst_stream => {
                if (hdr.stream_id == 0 or hdr.length != 4) {
                    try self.failConnection(if (hdr.length != 4) .frame_size_error else .protocol_error);
                    return;
                }
                const code: frame.ErrorCode = @enumFromInt(std.mem.readInt(u32, payload[0..4], .big));
                if (self.streams.getPtr(hdr.stream_id)) |s| {
                    s.onRst();
                    self.releaseConcurrency(hdr.stream_id, code);
                    // Cancel handler; never RST in response to peer RST.
                    // A reset answered by a reset is an infinite exchange
                    // between two conforming peers (RFC 9113 section 5.4.2).
                    try self.pushIntentOrFailClosed(.{ .stream_reset = .{
                        .stream_id = hdr.stream_id,
                        .code = code,
                        .from_peer = true,
                    } });
                } else if (hdr.stream_id <= self.highest_peer_stream) {
                    // CLOSED, not idle. The peer sent this before it learned the
                    // stream was over — its RST and our END_STREAM crossed on
                    // the wire. RFC 9113 section 5.1 requires tolerating that,
                    // and it is not a rare case: it happens every time a browser
                    // cancels a fetch as the response completes. Failing the
                    // connection here would kill every OTHER stream on it, which
                    // is how one canceled request became a dead connection.
                    //
                    // There is nothing to do. The stream is already gone, its
                    // concurrency credit already released, and the handler (if
                    // any) already canceled when the stream closed.
                } else {
                    // IDLE: a stream id the peer has never opened has no state
                    // to reset, so this is a genuine protocol violation
                    // (RFC 9113 section 5.1 / 5.4.1).
                    try self.failConnection(.protocol_error);
                    return;
                }
            },
            .settings => try self.onSettings(hdr, payload),
            .push_promise => try self.failConnection(.protocol_error),
            .ping => try self.onPing(hdr, payload),
            .goaway => {
                if (hdr.stream_id != 0 or hdr.length < 8) {
                    try self.failConnection(.frame_size_error);
                    return;
                }
                const last: u31 = @intCast(std.mem.readInt(u32, payload[0..4], .big) & 0x7fff_ffff);
                self.goaway_ceiling = last;
                self.peer_goaway = true;
                // Existing streams at or below last may finish; do not tear down transport yet.
                // A peer GOAWAY announces an intent to stop, not an immediate
                // close. An instant teardown here would abort in-flight
                // responses that the peer is still waiting to receive.
            },
            .window_update => try self.onWindowUpdate(hdr, payload),
            .continuation => try self.failConnection(.protocol_error),
            _ => {}, // unknown types ignored after generic validation
        }
        if (hdr.stream_id > self.last_processed_stream and hdr.stream_id % 2 == 1) {
            self.last_processed_stream = hdr.stream_id;
        }
        try self.chargeRate(hdr.type, already_terminal);
    }

    fn chargeRate(self: *Session, typ: frame.FrameType, already_terminal: bool) !void {
        // Protocol errors win over simultaneous rate errors.
        if (already_terminal) return;
        if (self.terminal != .none) return;
        if (self.rate_limiter) |rl| {
            if (!rl.admit(typ, self.edge_now_ns)) {
                try self.failConnection(.enhance_your_calm);
            }
        }
    }

    /// Edge clock: field-block / request-body idle deadlines.
    ///
    /// Both deadlines answer the same attack: a peer that opens work and then
    /// stops, so the server holds state while the peer spends nothing. A
    /// half-sent field block pins the HPACK buffer and blocks every other frame
    /// on the connection, so it costs the CONNECTION. A stalled request body
    /// pins only its own stream, so it costs that stream a RST_STREAM.
    ///
    /// The edge passes `now_ns` rather than this module reading a clock. That
    /// keeps the timeout behaviour deterministic under test.
    pub fn checkIdleDeadlines(self: *Session, now_ns: u64) !void {
        if (self.terminal != .none) return;
        if (self.expect_continuation != null and self.field_block_started_ns != 0) {
            if (now_ns -% self.field_block_started_ns >= self.limits.field_block_timeout_ns) {
                try self.failConnection(.enhance_your_calm);
                return;
            }
        }
        // Collect first, act second. `emitRst` removes map entries, and a
        // mutation during iteration invalidates the iterator.
        var stale: std.ArrayList(u31) = .empty;
        defer stale.deinit(self.gpa);
        var it = self.body_idle_ns.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.* == 0) continue;
            if (now_ns -% e.value_ptr.* >= self.limits.request_body_idle_timeout_ns) {
                try stale.append(self.gpa, e.key_ptr.*);
            }
        }
        for (stale.items) |sid| {
            _ = self.body_idle_ns.remove(sid);
            try self.emitRst(sid, .cancel);
        }
    }

    /// Earliest edge-owned idle deadline, or null when no protocol timer is armed.
    /// This only derives protocol state; the edge chooses how to wait for it.
    ///
    /// The actor is event-driven and parks with no timer when nothing is armed.
    /// Without this query it would have to poll to notice a deadline, which
    /// costs a wakeup per tick on every idle connection. A `null` result is
    /// therefore a real answer: no protocol timer exists, so the actor may
    /// sleep until a peer or a handler wakes it.
    pub fn nextIdleDeadlineNs(self: *Session) ?u64 {
        var next: ?u64 = null;
        if (self.expect_continuation != null and self.field_block_started_ns != 0) {
            next = self.field_block_started_ns +% self.limits.field_block_timeout_ns;
        }
        var it = self.body_idle_ns.iterator();
        while (it.next()) |entry| {
            const started = entry.value_ptr.*;
            if (started == 0) continue;
            const deadline = started +% self.limits.request_body_idle_timeout_ns;
            if (next == null or deadline < next.?) next = deadline;
        }
        return next;
    }

    fn onSettings(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        if (hdr.stream_id != 0) {
            try self.failConnection(.protocol_error);
            return;
        }
        if (hdr.flags.ack()) {
            if (hdr.length != 0) try self.failConnection(.frame_size_error);
            self.settings_acked = true;
            return;
        }
        if (hdr.length % 6 != 0) {
            try self.failConnection(.frame_size_error);
            return;
        }
        var i: usize = 0;
        while (i + 6 <= payload.len) : (i += 6) {
            const id: frame.SettingId = @enumFromInt(std.mem.readInt(u16, payload[i..][0..2], .big));
            const value = std.mem.readInt(u32, payload[i + 2 ..][0..4], .big);
            switch (id) {
                .enable_push => if (value > 1) {
                    try self.failConnection(.protocol_error);
                    return;
                },
                // A new initial window retunes every OPEN stream by the
                // difference. This is the one setting that reaches back into
                // existing state, and it is why a stream send window may go
                // negative. See `flow.applyInitialWindowDelta`.
                .initial_window_size => {
                    if (value > 0x7fffffff) {
                        try self.failConnection(.flow_control_error);
                        return;
                    }
                    const new_iw: i32 = @intCast(value);
                    const delta = new_iw - self.peer_initial_window;
                    var it = self.streams.iterator();
                    while (it.next()) |e| {
                        e.value_ptr.window.applyInitialWindowDelta(delta) catch {
                            try self.failConnection(.flow_control_error);
                            return;
                        };
                    }
                    self.peer_initial_window = new_iw;
                },
                .max_frame_size => {
                    if (value < 16384 or value > 16_777_215) {
                        try self.failConnection(.protocol_error);
                        return;
                    }
                    // Accept the peer's larger offer, then ignore it. The
                    // outbound frame size stays at the 16 KiB default, because
                    // that is what the boot-reserved scratch buffers are sized
                    // for. A bigger frame would need an allocation that
                    // `resourceUpperBound` never counted.
                    self.peer_max_frame_size = @min(value, frame.DEFAULT_MAX_FRAME_SIZE);
                },
                .header_table_size => self.peer_header_table_size = value,
                else => {},
            }
        }
        // ACK
        var buf: [9]u8 = undefined;
        const n = try frame.Serializer.settingsFrame(&buf, true, &.{});
        const p = try self.gpa.dupe(u8, buf[0..n]);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .settings, .payload = p, .flags = .{ .end_stream = true } } });
    }

    fn onPing(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        if (hdr.stream_id != 0 or hdr.length != 8) {
            try self.failConnection(.frame_size_error);
            return;
        }
        if (hdr.flags.ack()) {
            if (self.grace_phase == .phase1 and std.mem.eql(u8, payload[0..8], &self.grace_ping)) {
                self.grace_ping_acked = true;
            }
            return;
        }
        var opaque_data: [8]u8 = undefined;
        @memcpy(&opaque_data, payload[0..8]);
        var buf: [17]u8 = undefined;
        const n = try frame.Serializer.ping(&buf, true, &opaque_data);
        const p = try self.gpa.dupe(u8, buf[0..n]);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .ping, .payload = p, .flags = .{ .end_stream = true } } });
    }

    fn onWindowUpdate(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        if (hdr.length != 4) {
            try self.failConnection(.frame_size_error);
            return;
        }
        const incr_raw = std.mem.readInt(u32, payload[0..4], .big) & 0x7fff_ffff;
        const incr: u31 = @intCast(incr_raw);
        if (hdr.stream_id == 0) {
            self.windows.applyConnWindowUpdate(incr) catch |err| {
                const code: frame.ErrorCode = if (err == error.ProtocolError) .protocol_error else .flow_control_error;
                try self.failConnection(code);
            };
        } else if (self.streams.getPtr(hdr.stream_id)) |s| {
            s.window.applySendUpdate(incr) catch |err| {
                const code: frame.ErrorCode = if (err == error.ProtocolError) .protocol_error else .flow_control_error;
                try self.emitRst(hdr.stream_id, code);
            };
        } else if (hdr.stream_id <= self.highest_peer_stream) {
            // CLOSED, not idle. RFC 9113 section 6.9 is explicit: a peer may
            // send WINDOW_UPDATE for a stream it has not yet learned is over,
            // and "a receiver MUST NOT treat this as an error". This is the
            // ordinary flow-control conversation — a client credits the DATA it
            // just received while the server finishes the body — so failing the
            // connection here kills every other stream over a normal race.
            //
            // Nothing to apply: the stream is gone, and its send window with it.
        } else {
            // IDLE: the peer has never opened this stream, so there is no
            // window to credit (RFC 9113 section 5.1).
            try self.failConnection(.protocol_error);
        }
    }

    fn onData(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        if (hdr.stream_id == 0) {
            try self.failConnection(.protocol_error);
            return;
        }
        var data = payload;
        var pad: usize = 0;
        if (hdr.flags.padded) {
            if (payload.len < 1) {
                try self.failConnection(.protocol_error);
                return;
            }
            pad = payload[0];
            if (1 + pad > payload.len) {
                try self.failConnection(.protocol_error);
                return;
            }
            data = payload[1 .. payload.len - pad];
        }
        // Flow control charges the WHOLE payload, padding included. Only the
        // unpadded slice reaches the request body. A peer that pays for padding
        // must not gain free window by hiding bytes in it.
        const total: i32 = @intCast(payload.len);
        self.windows.debitConnRecv(total) catch {
            try self.failConnection(.flow_control_error);
            return;
        };
        const s = self.streams.getPtr(hdr.stream_id) orelse {
            if (hdr.stream_id <= self.highest_peer_stream) {
                // CLOSED, not idle: the client is still uploading a body for a
                // stream we ended — an abort, a slow-consumer kill, or an early
                // response. RFC 9113 section 6.4 says an endpoint that has SENT
                // RST_STREAM must IGNORE later frames on that stream, so this
                // does not answer with another reset; a reset per DATA frame
                // would be an amplification against a client that is simply
                // behind.
                //
                // The connection window was already debited above, and must be
                // (section 6.9.1) — the peer counted these bytes, so dropping
                // the accounting would desynchronize credit for the rest of the
                // connection. Replenish at the usual threshold so a large body
                // sent into a dead stream cannot strand the whole window.
                if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .data_on_closed_ignored, .code = .no_error, .stream_id = hdr.stream_id });
                if (self.windows.needsConnWindowUpdate()) {
                    const inc = self.windows.connWindowUpdateIncrement();
                    if (inc > 0) {
                        self.windows.creditConnRecv(inc);
                        var buf: [13]u8 = undefined;
                        const n = try frame.Serializer.windowUpdate(&buf, 0, inc);
                        const p = try self.gpa.dupe(u8, buf[0..n]);
                        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .window_update, .payload = p } });
                    }
                }
                return;
            }
            // IDLE: DATA for a stream never opened (RFC 9113 section 5.1).
            try self.failConnection(.protocol_error);
            return;
        };
        if (s.state == .idle or s.state == .closed) {
            try self.failConnection(.protocol_error);
            return;
        }
        s.window.debitRecv(total) catch {
            try self.emitRst(hdr.stream_id, .flow_control_error);
            return;
        };

        // An early-rejected stream (413/414/431) keeps consuming and discarding
        // its DATA until the peer sends END_STREAM. The server cannot stop the
        // peer mid-body without a RST, and a RST would race the 413 response
        // that explains the refusal. So the bytes are charged to flow control,
        // counted, and dropped — never retained and never dispatched.
        if (s.early_status == null and !s.refused_before_dispatch and s.headers_done) {
            var body = self.pending_bodies.getPtr(hdr.stream_id) orelse blk: {
                try self.pending_bodies.put(hdr.stream_id, .empty);
                break :blk self.pending_bodies.getPtr(hdr.stream_id).?;
            };
            const cap = s.request_body_cap orelse self.limits.request_body_bytes;
            if (body.items.len + data.len > cap) {
                s.refused_before_dispatch = true;
                s.early_status = 413;
                // Discard overflow bytes; do not retain or dispatch.
                // 413 immediately on byte cap+1 — continue discard through remote END_STREAM.
                try self.emitEarlyReject(hdr.stream_id, 413);
            } else {
                if (self.stream_hooks) |h| {
                    if (h.tryReserveRequest) |tr| {
                        if (!tr(h.ctx, data.len)) {
                            s.refused_before_dispatch = true;
                            s.early_status = 413;
                            try self.emitEarlyReject(hdr.stream_id, 413);
                        } else {
                            errdefer if (h.releaseRequest) |rel| rel(h.ctx, data.len);
                            try body.appendSlice(self.gpa, data);
                        }
                    } else {
                        try body.appendSlice(self.gpa, data);
                    }
                } else {
                    try body.appendSlice(self.gpa, data);
                }
            }
        }

        s.onData(data.len, hdr.flags.end_stream) catch |err| {
            try self.emitRst(hdr.stream_id, stream_mod.Stream.toErrorCode(err));
            return;
        };

        if (!hdr.flags.end_stream) {
            try self.body_idle_ns.put(hdr.stream_id, self.edge_now_ns);
        } else {
            _ = self.body_idle_ns.remove(hdr.stream_id);
        }

        if (hdr.flags.end_stream) {
            if (s.early_status) |st| {
                try self.emitEarlyReject(hdr.stream_id, st);
            } else {
                try self.maybeDispatch(hdr.stream_id);
            }
        }
        self.releaseConcurrency(hdr.stream_id, .no_error);

        // WINDOW_UPDATE at half
        if (s.window.needsWindowUpdate()) {
            const inc = s.window.windowUpdateIncrement();
            if (inc > 0) {
                s.window.creditRecv(inc);
                var buf: [13]u8 = undefined;
                const n = try frame.Serializer.windowUpdate(&buf, hdr.stream_id, inc);
                const p = try self.gpa.dupe(u8, buf[0..n]);
                try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .window_update, .payload = p, .stream_id = hdr.stream_id } });
            }
        }
        if (self.windows.needsConnWindowUpdate()) {
            const inc = self.windows.connWindowUpdateIncrement();
            if (inc > 0) {
                self.windows.creditConnRecv(inc);
                var buf: [13]u8 = undefined;
                const n = try frame.Serializer.windowUpdate(&buf, 0, inc);
                const p = try self.gpa.dupe(u8, buf[0..n]);
                try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .window_update, .payload = p } });
            }
        }
    }

    /// Emit exactly one HTTP response for a request that never reaches a
    /// handler. Several paths can decide the same refusal — an oversized header
    /// block, an oversized declared body, an oversized observed body, a late
    /// trailer block — and each may fire more than once as further frames
    /// arrive. `early_response_sent` makes the frameset exactly-once, because a
    /// second HEADERS on the same stream is a protocol violation that would
    /// turn a clean 413 into a broken connection.
    fn emitEarlyReject(self: *Session, stream_id: u31, status: u16) !void {
        const s = self.streams.getPtr(stream_id) orelse return;
        if (s.early_response_sent) return;
        s.early_response_sent = true;
        s.refused_before_dispatch = true;
        s.early_status = status;
        try self.pushIntentOrFailClosed(.{ .early_reject = .{ .stream_id = stream_id, .status = status } });
        // Also emit the HTTP response frames now so core stays self-contained for tests.
        try self.cmdHeaders(stream_id, status, &.{}, true);
    }

    fn isTombstoned(self: *const Session, id: u31) bool {
        for (self.tombstones.items) |t| {
            if (t.id == id) return true;
        }
        return false;
    }

    fn onHeaders(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .headers_seen, .code = .no_error, .stream_id = hdr.stream_id });
        if (hdr.stream_id == 0 or hdr.stream_id % 2 == 0) {
            try self.failConnection(.protocol_error);
            return;
        }
        // A stream ID above the highest one ever opened cannot be a closed
        // stream. Check the watermark first so the normal monotonic request
        // path does not scan the bounded tombstone ring.
        if (hdr.stream_id <= self.highest_peer_stream and self.isTombstoned(hdr.stream_id)) {
            // HEADERS on a closed stream: connection STREAM_CLOSED (RFC 9113 §5.1 / h2spec 5.1/12).
            if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .headers_on_tombstone, .code = .stream_closed, .stream_id = hdr.stream_id });
            try self.failConnection(.stream_closed);
            return;
        }
        if (self.streams.getPtr(hdr.stream_id)) |existing| {
            if (existing.state == .closed) {
                if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .headers_on_closed, .code = .stream_closed, .stream_id = hdr.stream_id });
                try self.failConnection(.stream_closed);
                return;
            }
            if (existing.state == .half_closed_remote) {
                try self.emitRst(hdr.stream_id, .stream_closed);
                return;
            }
        }
        if (hdr.stream_id <= self.highest_peer_stream) {
            if (self.streams.get(hdr.stream_id) == null) {
                try self.failConnection(.protocol_error);
                return;
            }
        } else {
            if (self.goaway_ceiling) |ceil| {
                if (hdr.stream_id > ceil) {
                    try self.emitRst(hdr.stream_id, .refused_stream);
                    return;
                }
            }
            self.highest_peer_stream = hdr.stream_id;
        }

        var fragment = payload;
        if (hdr.flags.padded) {
            if (payload.len < 1) {
                try self.failConnection(.protocol_error);
                return;
            }
            const pad = payload[0];
            var skip: usize = 1;
            if (hdr.flags.priority) skip += 5;
            if (skip + pad > payload.len) {
                try self.failConnection(.protocol_error);
                return;
            }
            fragment = payload[skip .. payload.len - pad];
        } else if (hdr.flags.priority) {
            if (payload.len < 5) {
                try self.failConnection(.protocol_error);
                return;
            }
            fragment = payload[5..];
        }

        self.header_block.clearRetainingCapacity();
        self.continuation_count = 0;
        self.header_end_stream = hdr.flags.end_stream;
        if (!hdr.flags.end_headers) {
            try self.header_block.appendSlice(self.gpa, fragment);
            self.expect_continuation = hdr.stream_id;
            if (self.field_block_started_ns == 0) self.field_block_started_ns = self.edge_now_ns;
            return;
        }
        self.field_block_started_ns = 0;
        try self.finishHeaderBlock(hdr.stream_id, fragment);
    }

    fn consumeContinuation(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        self.continuation_count += 1;
        if (self.continuation_count > self.limits.continuation_frames or
            self.header_block.items.len + payload.len > self.limits.compressed_header_bytes)
        {
            try self.failConnection(.enhance_your_calm);
            return;
        }
        try self.header_block.appendSlice(self.gpa, payload);
        if (!hdr.flags.end_headers) return;
        const sid = self.expect_continuation.?;
        self.expect_continuation = null;
        self.field_block_started_ns = 0;
        try self.finishHeaderBlock(sid, self.header_block.items);
        self.header_block.clearRetainingCapacity();
    }

    /// A complete field block arrived. This is where a request becomes real.
    ///
    /// The sequence, and each step depends on the one before it:
    ///
    /// 1. Decode. A compression fault kills the CONNECTION, never one stream,
    ///    because the shared HPACK table is now out of step with the peer.
    /// 2. Admit, if the stream is new. The per-connection cap is checked first,
    ///    then the server-wide hook. A refusal is a RST_STREAM, not a
    ///    connection error: the peer may simply retry later.
    /// 3. Classify. A block on a stream that already has headers is a TRAILER
    ///    block and follows different rules.
    /// 4. Validate the pseudo-headers, then apply the size policies. A policy
    ///    breach becomes an `early_status` and not an exception, so the peer
    ///    receives a real HTTP status instead of a bare reset.
    /// 5. Transfer a complete header-only request directly to its dispatch
    ///    intent. Requests with bodies store fields until DATA END_STREAM.
    ///
    /// Ownership: `block` is the HEADERS payload for a single-frame END_HEADERS
    /// request, or the assembled `header_block` after CONTINUATION. The
    /// `decoded.fields` slice is owned from step 1 onward.
    /// Each field's strings are independently owned or borrowed. GPA results
    /// are freed with `freeResult`; edge-arena results are discarded via the
    /// header-list hook. Success hands the slice to an intent or to
    /// `pending_headers` / `pending_trailers`.
    fn finishHeaderBlock(self: *Session, stream_id: u31, block: []const u8) !void {
        const req_alloc = self.headerListAllocator(stream_id);
        const decoded = self.decoder.decodeInto(
            req_alloc,
            block,
            self.limits.header_fields,
            self.limits.decoded_header_bytes,
            256,
            self.limits.regular_field_value_bytes,
        ) catch |err| {
            self.discardHeaderList(stream_id);
            if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .hpack_decode_fail, .code = if (err == error.OutOfMemory) .internal_error else .compression_error, .stream_id = stream_id });
            if (err == error.OutOfMemory) {
                try self.failConnection(.internal_error);
            } else {
                try self.failConnection(.compression_error);
            }
            return;
        };

        var s_ptr = self.streams.getPtr(stream_id);
        if (s_ptr == null) {
            if (self.active_streams >= self.limits.max_streams_per_connection) {
                self.dropDecoded(stream_id, decoded);
                try self.emitRst(stream_id, .refused_stream);
                return;
            }
            if (self.stream_hooks) |h| {
                if (!h.tryAdmit(h.ctx)) {
                    self.dropDecoded(stream_id, decoded);
                    try self.emitRst(stream_id, .refused_stream);
                    return;
                }
            }
            var credit_held = self.stream_hooks != null;
            errdefer if (credit_held) {
                if (self.stream_hooks) |h| h.onRelease(h.ctx);
            };
            try self.streams.put(stream_id, .{ .id = stream_id, .window = .{ .send = self.peer_initial_window } });
            credit_held = false;
            self.active_streams += 1;
            s_ptr = self.streams.getPtr(stream_id);
        }
        const s = s_ptr.?;

        if (s.headers_done) {
            // Trailers only legal while remote half is still open, and MUST carry END_STREAM.
            if (!self.header_end_stream) {
                self.dropDecoded(stream_id, decoded);
                try self.emitRst(stream_id, .protocol_error);
                return;
            }
            if (s.end_stream_remote or s.state == .half_closed_remote or s.state == .closed) {
                self.dropDecoded(stream_id, decoded);
                try self.emitRst(stream_id, .stream_closed);
                return;
            }
            for (decoded.fields) |f| {
                if (f.name.len > 0 and f.name[0] == ':') {
                    self.dropDecoded(stream_id, decoded);
                    try self.emitRst(stream_id, .protocol_error);
                    return;
                }
            }
            // Early-rejected streams: trailers close remote; ensure the one HTTP response exists.
            if (s.early_status) |st| {
                self.dropDecoded(stream_id, decoded);
                // The early-reject state is already terminal and owns the response.
                s.onHeaders(true) catch {};
                try self.emitEarlyReject(stream_id, st);
                self.releaseConcurrency(stream_id, .no_error);
                return;
            }
            s.onHeaders(true) catch |err| {
                self.dropDecoded(stream_id, decoded);
                try self.emitRst(stream_id, stream_mod.Stream.toErrorCode(err));
                return;
            };
            try self.pending_trailers.put(stream_id, decoded.fields);
            try self.maybeDispatch(stream_id);
            self.releaseConcurrency(stream_id, .no_error);
            return;
        }

        const parsed = fields.validateRequestFields(decoded.fields) catch |err| {
            self.dropDecoded(stream_id, decoded);
            if (err == error.PathTooLong) {
                s.refused_before_dispatch = true;
                s.early_status = 414;
                // The early-reject state is already terminal and owns the response.
                s.onHeaders(self.header_end_stream) catch {};
                try self.emitEarlyReject(stream_id, 414);
                self.releaseConcurrency(stream_id, .no_error);
                return;
            }
            try self.emitRst(stream_id, .protocol_error);
            return;
        };
        if (decoded.policy_exceeded) {
            s.refused_before_dispatch = true;
            s.early_status = 431;
        }
        s.request_body_cap = self.routeBodyCap(parsed.method, parsed.path);
        if (parsed.content_length) |cl| {
            const cap = s.request_body_cap orelse self.limits.request_body_bytes;
            if (cl > cap) {
                s.refused_before_dispatch = true;
                s.early_status = 413;
            }
        }

        s.content_length = parsed.content_length;
        const early = s.early_status;
        s.onHeaders(self.header_end_stream) catch |err| {
            self.dropDecoded(stream_id, decoded);
            try self.emitRst(stream_id, stream_mod.Stream.toErrorCode(err));
            return;
        };
        if (early) |st| {
            self.dropDecoded(stream_id, decoded);
            try self.emitEarlyReject(stream_id, st);
            self.releaseConcurrency(stream_id, .no_error);
        } else {
            if (self.header_end_stream) {
                // The common one-shot path already has a complete request and
                // the validated pseudo-header slices. Do not put/fetch the
                // fields through a hash map and validate them a second time.
                try self.dispatchOwnedRequest(stream_id, parsed, decoded.fields, &.{}, &.{});
            } else {
                try self.pending_headers.put(stream_id, decoded.fields);
            }
            self.releaseConcurrency(stream_id, .no_error);
        }
    }

    /// Hand a complete request to the edge, exactly once.
    ///
    /// Several frames can reach this point for one stream — HEADERS with
    /// END_STREAM, the last DATA, or a trailer block — so the three guards are
    /// the real contract: the remote half must be closed, the stream must not
    /// have been refused, and no handler may have started yet. `handler_started`
    /// is what makes a request one request and not one per triggering frame.
    ///
    /// Headers, body, and trailers are moved out of the pending maps, then
    /// `dispatchOwnedRequest` transfers them to the intent.
    fn maybeDispatch(self: *Session, stream_id: u31) !void {
        const s = self.streams.getPtr(stream_id) orelse return;
        if (!s.end_stream_remote or s.refused_before_dispatch or s.handler_started) return;
        const hdrs = self.pending_headers.fetchRemove(stream_id) orelse return;
        var headers_owned = true;
        errdefer if (headers_owned) {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, hdrs.value);
                self.gpa.free(hdrs.value);
            } else {
                self.discardHeaderList(stream_id);
            }
        };
        const body_entry = self.pending_bodies.fetchRemove(stream_id);
        const body = blk: {
            if (body_entry) |e| {
                var list = e.value;
                if (self.stream_hooks) |h| {
                    if (h.releaseRequest) |rel| rel(h.ctx, list.items.len);
                }
                break :blk list.toOwnedSlice(self.gpa) catch |err| {
                    list.deinit(self.gpa);
                    // Preserve the original allocation error if fail-closed signaling also allocates.
                    self.failClosedInternal() catch {};
                    return err;
                };
            }
            break :blk &.{};
        };
        var body_owned = body.len != 0;
        errdefer if (body_owned) self.gpa.free(body);

        const parsed = fields.validateRequestFields(hdrs.value) catch {
            try self.emitRst(stream_id, .protocol_error);
            return;
        };

        const trailers_entry = self.pending_trailers.fetchRemove(stream_id);
        const trailers: []hpack.HeaderField = if (trailers_entry) |e| e.value else &.{};
        var trailers_owned = trailers.len != 0;
        errdefer if (trailers_owned) {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, trailers);
                self.gpa.free(trailers);
            }
        };

        // The helper owns every slice from this point, including its failures.
        headers_owned = false;
        body_owned = false;
        trailers_owned = false;
        try self.dispatchOwnedRequest(stream_id, parsed, hdrs.value, body, trailers);
    }

    /// Ownership transfer point for a complete request. On entry this function
    /// owns all three slices; on success the intent owns them. Failures before
    /// transfer free exactly once, while `pushIntentOrFailClosed` owns and
    /// releases an intent it accepted before failing closed.
    fn dispatchOwnedRequest(
        self: *Session,
        stream_id: u31,
        parsed: fields.ValidatedRequestFields,
        headers: []hpack.HeaderField,
        body: []const u8,
        trailers: []hpack.HeaderField,
    ) !void {
        var headers_owned = true;
        errdefer if (headers_owned) {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, headers);
                self.gpa.free(headers);
            } else {
                self.discardHeaderList(stream_id);
            }
        };
        var body_owned = body.len != 0;
        errdefer if (body_owned) self.gpa.free(body);
        var trailers_owned = trailers.len != 0;
        errdefer if (trailers_owned) {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, trailers);
                self.gpa.free(trailers);
            }
        };

        const s = self.streams.getPtr(stream_id) orelse return error.StreamClosed;
        if (!s.end_stream_remote or s.refused_before_dispatch or s.handler_started) {
            return error.StreamClosed;
        }
        // Past this line the intent owns everything. Clear the flags BEFORE the
        // push, so that a `PoolExhausted` inside `pushIntentOrFailClosed` — which
        // releases the intent itself — cannot make the errdefer blocks free the
        // same memory a second time.
        s.handler_started = true;
        headers_owned = false;
        body_owned = false;
        trailers_owned = false;
        try self.pushIntentOrFailClosed(.{ .dispatch_request = .{
            .stream_id = stream_id,
            .method = parsed.method,
            .scheme = parsed.scheme,
            .authority = parsed.authority,
            .path = parsed.path,
            .query = parsed.query,
            .headers = headers,
            .body = body,
            .trailers = trailers,
        } });
    }

    fn cmdHeaders(self: *Session, stream_id: u31, status: u16, headers: []const hpack.HeaderField, end_stream: bool) !void {
        // A RST tombstone already released the map entry. Emitting HEADERS
        // after that is how a handler that took `session_mu` late queued a
        // body the scheduler can never emit (TLS stall: pending DATA, tomb_rst).
        if (self.streams.get(stream_id) == null) return error.StreamClosed;
        var status_buf: [3]u8 = undefined;
        const status_str = try std.fmt.bufPrint(&status_buf, "{d}", .{status});
        const status_field = [_]hpack.HeaderField{.{ .name = ":status", .value = status_str }};
        const status_len = hpack.Encoder.encodedLen(&status_field) catch return error.OutOfMemory;
        const headers_len = hpack.Encoder.encodedLen(headers) catch return error.OutOfMemory;
        const block_len = std.math.add(usize, status_len, headers_len) catch return error.OutOfMemory;
        if (block_len > std.math.maxInt(u24)) return error.OutOfMemory;
        const total = std.math.add(usize, frame.FRAME_HEADER_LEN, block_len) catch return error.OutOfMemory;

        const out = try self.allocFrame(total);
        var out_owned = true;
        errdefer if (out_owned) self.freeFrame(out);

        const fh = frame.FrameHeader{
            .length = @intCast(block_len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = end_stream },
            .stream_id = stream_id,
        };
        fh.encode(out[0..frame.FRAME_HEADER_LEN]);
        const n1 = hpack.Encoder.encodeInto(out[frame.FRAME_HEADER_LEN..], &status_field) catch return error.OutOfMemory;
        const n2 = hpack.Encoder.encodeInto(out[frame.FRAME_HEADER_LEN + n1 ..], headers) catch return error.OutOfMemory;
        std.debug.assert(n1 + n2 == block_len);

        out_owned = false;
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{
            .typ = .headers,
            .payload = out,
            .stream_id = stream_id,
            .flags = fh.flags,
        } });
        if (self.streams.getPtr(stream_id)) |s| {
            if (end_stream) {
                s.localEndStream();
                self.releaseConcurrency(stream_id, .no_error);
            }
        }
    }

    fn cmdData(self: *Session, stream_id: u31, data: []const u8, end_stream: bool) !void {
        if (data.len == 0 and !end_stream) return;
        const out = try self.makeDataFrame(stream_id, data, end_stream);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{
            .typ = .data,
            .payload = out,
            .stream_id = stream_id,
            .flags = .{ .end_stream = end_stream },
        } });
    }

    /// Outbound DATA credit for a stream (0 when negative/exhausted).
    ///
    /// A missing map entry is not the same as a blocked window. The TLS stall
    /// parked with scheduler slabs still holding the response body after
    /// `releaseConcurrency` had dropped the stream; treating that as zero
    /// credit made `FairScheduler.drain` skip those streams forever.
    pub fn streamSendAvailable(self: *const Session, stream_id: u31) i32 {
        if (self.streams.get(stream_id)) |s| {
            return s.window.availableSend(self.windows.conn_send);
        }
        if (self.windows.conn_send <= 0) return 0;
        if (self.tombstoneCode(stream_id)) |code| {
            if (code != .no_error) return 0;
        }
        return self.windows.conn_send;
    }

    pub fn tombstoneCode(self: *const Session, id: u31) ?frame.ErrorCode {
        for (self.tombstones.items) |t| {
            if (t.id == id) return t.code;
        }
        return null;
    }

    pub fn connectionSendAvailable(self: *const Session) i32 {
        if (self.windows.conn_send <= 0) return 0;
        return self.windows.conn_send;
    }
};

test "tombstones overwrite oldest in place" {
    var session = try Session.init(std.testing.allocator, .{ .stream_tombstones = 2 });
    defer session.deinit();

    try session.addTombstone(1, .cancel);
    try session.addTombstone(3, .stream_closed);
    try session.addTombstone(5, .no_error);

    try std.testing.expectEqual(@as(usize, 2), session.tombstones.items.len);
    try std.testing.expect(!session.isTombstoned(1));
    try std.testing.expect(session.isTombstoned(3));
    try std.testing.expect(session.isTombstoned(5));
    try std.testing.expectEqual(@as(u31, 5), session.tombstones.items[0].id);
    try std.testing.expectEqual(@as(u31, 3), session.tombstones.items[1].id);
}

test "makeDataFrame still frames DATA after the stream map entry is gone" {
    var session = try Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    const body = "Hello, World!";
    try session.streams.put(1, .{
        .id = 1,
        .state = .half_closed_remote,
        .window = .{ .send = flow.INITIAL_WINDOW },
    });
    session.active_streams = 1;
    const live = try session.makeDataFrame(1, body, true);
    defer std.testing.allocator.free(live);
    try std.testing.expectEqual(@as(usize, frame.FRAME_HEADER_LEN + body.len), live.len);
    try std.testing.expect(session.streams.get(1) == null);

    try session.addTombstone(3, .no_error);
    try std.testing.expect(session.streamSendAvailable(3) > 0);
    const orphan = try session.makeDataFrame(3, body, true);
    defer std.testing.allocator.free(orphan);
    try std.testing.expectEqual(@as(usize, frame.FRAME_HEADER_LEN + body.len), orphan.len);
    try std.testing.expectEqual(@as(u8, 1), orphan[4] & 1);

    try session.addTombstone(5, .cancel);
    try std.testing.expectEqual(@as(i32, 0), session.streamSendAvailable(5));
}

test "respond_headers after RST is StreamClosed" {
    var session = try Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| std.testing.allocator.free(f.payload),
            else => {},
        };
    }
    try session.streams.put(1, .{
        .id = 1,
        .state = .half_closed_remote,
        .window = .{ .send = flow.INITIAL_WINDOW },
    });
    session.active_streams = 1;
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 1, .code = .internal_error } });
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| std.testing.allocator.free(f.payload),
            else => {},
        };
    }
    try std.testing.expect(session.streams.get(1) == null);
    try std.testing.expectError(error.StreamClosed, session.applyCommand(.{ .respond_headers = .{
        .stream_id = 1,
        .status = 200,
        .headers = &.{},
        .end_stream = true,
    } }));
}

test "session preface and settings ack" {
    var session = try Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    const intents = session.drainIntents();
    defer {
        for (intents) |*it| {
            switch (it.*) {
                .outbound_frame => |f| std.testing.allocator.free(f.payload),
                else => {},
            }
        }
        // intent_drain scratch — do not free slice
    }
    try std.testing.expect(intents.len >= 2);
}

test "bad preface" {
    var session = try Session.init(std.testing.allocator, .defaults);
    defer session.deinit();
    const intents = session.drainIntents();
    for (intents) |*it| {
        switch (it.*) {
            .outbound_frame => |f| std.testing.allocator.free(f.payload),
            else => {},
        }
    }
    session.releaseDrainedIntents(intents);
    try session.ingest("GET / HTTP/1.1\r\n");
    try std.testing.expect(session.terminal == .goaway);
    // Drain connection-error intents produced by failure
    const more = session.drainIntents();
    defer {
        for (more) |*it| {
            switch (it.*) {
                .outbound_frame => |f| std.testing.allocator.free(f.payload),
                else => {},
            }
        }
        session.releaseDrainedIntents(more);
    }
}

test "SETTINGS_INITIAL_WINDOW_SIZE negative send window holds queued DATA (real ingest)" {
    const gpa = std.testing.allocator;
    const hpack_mod = @import("hpack.zig");
    var session = try Session.init(gpa, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
        // intent_drain scratch — do not free slice
    }

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    {
        var sbuf: [64]u8 = undefined;
        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 128 }};
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
        try wire.appendSlice(gpa, sbuf[0..sn]);
    }
    const hdr_fields = [_]hpack_mod.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/x" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack_mod.Encoder.encode(gpa, &hdr_fields);
    defer gpa.free(block);
    {
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = true },
            .stream_id = 1,
        };
        fh.encode(&hdr_buf);
        try wire.appendSlice(gpa, &hdr_buf);
        try wire.appendSlice(gpa, block);
    }
    try session.ingest(wire.items);
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            .dispatch_request => |d| {
                hpack_mod.HeaderField.freeOwnedSlice(gpa, d.headers);
                gpa.free(d.headers);
                if (d.trailers.len != 0) {
                    hpack_mod.HeaderField.freeOwnedSlice(gpa, d.trailers);
                    gpa.free(d.trailers);
                }
                if (d.body.len != 0) gpa.free(d.body);
            },
            else => {},
        };
    }
    try session.applyCommand(.{ .respond_headers = .{
        .stream_id = 1,
        .status = 200,
        .headers = &.{},
        .end_stream = false,
    } });
    {
        const more = session.drainIntents();
        for (more) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }

    // Simulate connection scheduler pending: emit first quantum that fits, hold the rest.
    const payload = [_]u8{'Q'} ** 200;
    const first = payload[0..128];
    const rest = payload[128..];
    try session.applyCommand(.{ .respond_data = .{
        .stream_id = 1,
        .data = first,
        .end_stream = false,
    } });
    {
        const out = session.drainIntents();
        defer {
            for (out) |*it| switch (it.*) {
                .outbound_frame => |f| gpa.free(f.payload),
                else => {},
            };
            // intent_drain scratch
        }
        var saw = false;
        for (out) |it| switch (it) {
            .outbound_frame => |f| if (f.typ == .data) {
                saw = true;
                try std.testing.expectEqualSlices(u8, first, f.payload[9..][0..first.len]);
            },
            else => {},
        };
        try std.testing.expect(saw);
    }
    var pending: std.ArrayList(u8) = .empty;
    defer pending.deinit(gpa);
    try pending.appendSlice(gpa, rest);
    const pending_end = true;

    // Real SETTINGS_INITIAL_WINDOW_SIZE reduction via ingest (makes send negative).
    {
        var sbuf: [64]u8 = undefined;
        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 1 }};
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
        try session.ingest(sbuf[0..sn]);
    }
    {
        const ack_out = session.drainIntents();
        for (ack_out) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
        // intent_drain scratch
    }
    const s = session.streams.getPtr(1).?;
    try std.testing.expect(s.window.send < 0);
    try std.testing.expectEqual(@as(i32, 0), session.streamSendAvailable(1));
    try std.testing.expectError(error.FlowBlocked, session.applyCommand(.{ .respond_data = .{
        .stream_id = 1,
        .data = pending.items,
        .end_stream = pending_end,
    } }));
    // Queued scheduler bytes retained byte-identical.
    try std.testing.expectEqualSlices(u8, rest, pending.items);

    // WINDOW_UPDATE restores credit; resume pending → DATA+END_STREAM identical.
    {
        var wbuf: [13]u8 = undefined;
        const wn = try frame.Serializer.windowUpdate(&wbuf, 1, 10_000);
        try session.ingest(wbuf[0..wn]);
    }
    try session.windows.applyConnWindowUpdate(10_000);
    try session.applyCommand(.{ .respond_data = .{
        .stream_id = 1,
        .data = pending.items,
        .end_stream = pending_end,
    } });
    const out = session.drainIntents();
    defer {
        for (out) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
        // intent_drain scratch
    }
    var saw_data = false;
    for (out) |it| switch (it) {
        .outbound_frame => |f| {
            if (f.typ == .data) {
                saw_data = true;
                try std.testing.expect(f.flags.end_stream);
                try std.testing.expectEqualSlices(u8, pending.items, f.payload[9..][0..pending.items.len]);
            }
        },
        else => {},
    };
    try std.testing.expect(saw_data);
}

test "intent capacity: flood fails closed without silent drop" {
    const gpa = std.testing.allocator;
    var limits = limits_mod.Limits.defaults;
    limits.intent_entries_per_connection = 8; // 6 usable + 2 terminal reserve
    var session = try Session.init(gpa, limits);
    defer session.deinit();
    {
        const boot = session.drainIntents();
        for (boot) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }

    // Mutation canary: each accepted intent carries unique payload bytes.
    var accepted: usize = 0;
    var i: usize = 0;
    while (i < 64) : (i += 1) {
        var buf: [17]u8 = undefined;
        // Synthetic PING frames as cheap owned payloads.
        const ping_data = [_]u8{ @truncate(i), 0, 0, 0, 0, 0, 0, 0 };
        const n = try frame.Serializer.ping(&buf, false, &ping_data);
        const p = try gpa.dupe(u8, buf[0..n]);
        const intent: Intent = .{ .outbound_frame = .{ .typ = .ping, .payload = p } };
        session.pushIntent(intent) catch |err| {
            try std.testing.expectEqual(error.PoolExhausted, err);
            gpa.free(p); // ownership never transferred
            break;
        };
        accepted += 1;
    }
    try std.testing.expect(accepted > 0);
    try std.testing.expect(accepted <= session.intentCapacity() - session.terminal_reserve);
    try std.testing.expectEqual(accepted, session.intentLen());

    // Drain must emit every accepted intent (no silent drop).
    var batch: [16]Intent = undefined;
    const n = session.drainIntentsInto(&batch);
    try std.testing.expectEqual(accepted, n);
    var seen: usize = 0;
    for (batch[0..n]) |*it| switch (it.*) {
        .outbound_frame => |f| {
            try std.testing.expectEqual(frame.FrameType.ping, f.typ);
            seen += 1;
            gpa.free(f.payload);
        },
        else => return error.UnexpectedIntent,
    };
    try std.testing.expectEqual(accepted, seen);
    try std.testing.expectEqual(@as(usize, 0), session.intentLen());

    // Terminal reserve still accepts GOAWAY fail-close after capacity exhaustion.
    i = 0;
    while (i < session.intentCapacity() - session.terminal_reserve) : (i += 1) {
        var buf: [17]u8 = undefined;
        const ping_data = [_]u8{0} ** 8;
        const pn = try frame.Serializer.ping(&buf, false, &ping_data);
        const p = try gpa.dupe(u8, buf[0..pn]);
        try session.pushIntent(.{ .outbound_frame = .{ .typ = .ping, .payload = p } });
    }
    {
        var buf: [17]u8 = undefined;
        const ping_data = [_]u8{0} ** 8;
        const pn = try frame.Serializer.ping(&buf, false, &ping_data);
        const p = try gpa.dupe(u8, buf[0..pn]);
        try std.testing.expectError(error.PoolExhausted, session.pushIntent(.{ .outbound_frame = .{ .typ = .ping, .payload = p } }));
        gpa.free(p);
    }
    try session.failClosedInternal();
    try std.testing.expect(session.terminal != .none);
    {
        const drained = session.drainIntents();
        for (drained) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }
}

test "respond HEADERS allocates only the final frame" {
    var counting = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const gpa = counting.allocator();
    const hpack_mod = @import("hpack.zig");
    var session = try Session.init(gpa, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    {
        var sbuf: [9]u8 = undefined;
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
        try wire.appendSlice(gpa, sbuf[0..sn]);
    }
    const req_fields = [_]hpack_mod.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack_mod.Encoder.encode(gpa, &req_fields);
    defer gpa.free(block);
    {
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = true },
            .stream_id = 1,
        };
        fh.encode(&hdr_buf);
        try wire.appendSlice(gpa, &hdr_buf);
        try wire.appendSlice(gpa, block);
    }
    try session.ingest(wire.items);
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            .dispatch_request => |d| {
                hpack_mod.HeaderField.freeOwnedSlice(gpa, d.headers);
                gpa.free(d.headers);
                if (d.trailers.len != 0) {
                    hpack_mod.HeaderField.freeOwnedSlice(gpa, d.trailers);
                    gpa.free(d.trailers);
                }
                if (d.body.len != 0) gpa.free(d.body);
            },
            else => {},
        };
    }

    const extra = [_]hpack_mod.HeaderField{.{ .name = "content-type", .value = "text/plain" }};
    const before = counting.allocations;
    try session.applyCommand(.{ .respond_headers = .{
        .stream_id = 1,
        .status = 200,
        .headers = &extra,
        .end_stream = false,
    } });
    try std.testing.expectEqual(@as(usize, 1), counting.allocations - before);
    {
        const more = session.drainIntents();
        try std.testing.expectEqual(@as(usize, 1), more.len);
        switch (more[0]) {
            .outbound_frame => |f| {
                try std.testing.expectEqual(frame.FrameType.headers, f.typ);
                gpa.free(f.payload);
            },
            else => return error.TestUnexpectedResult,
        }
    }
}

fn freeTestIntents(gpa: std.mem.Allocator, intents: []Intent) void {
    for (intents) |*it| switch (it.*) {
        .outbound_frame => |f| gpa.free(f.payload),
        .dispatch_request => |d| {
            hpack.HeaderField.freeOwnedSlice(gpa, d.headers);
            gpa.free(d.headers);
            if (d.trailers.len != 0) {
                hpack.HeaderField.freeOwnedSlice(gpa, d.trailers);
                gpa.free(d.trailers);
            }
            if (d.body.len != 0) gpa.free(d.body);
        },
        else => {},
    };
}

fn ingestPrefaceAndSettings(session: *Session, gpa: std.mem.Allocator) !void {
    freeTestIntents(gpa, session.drainIntents());
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(gpa, sbuf[0..sn]);
    try session.ingest(wire.items);
    freeTestIntents(gpa, session.drainIntents());
}

test "single-frame END_HEADERS does not copy into header_block" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, .defaults);
    defer session.deinit();
    try ingestPrefaceAndSettings(&session, gpa);

    const hdr_fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":path", .value = "/hello" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "x-custom", .value = "literal-value" },
    };
    const block = try hpack.Encoder.encode(gpa, &hdr_fields);
    defer gpa.free(block);
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = true },
        .stream_id = 1,
    }).encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);

    try session.ingest(wire.items);
    try std.testing.expectEqual(@as(usize, 0), session.header_block.items.len);

    @memset(wire.items, 0xAA);
    const intents = session.drainIntents();
    defer freeTestIntents(gpa, intents);
    var saw = false;
    for (intents) |it| switch (it) {
        .dispatch_request => |d| {
            saw = true;
            try std.testing.expectEqualStrings("/hello", d.path);
            var found_custom = false;
            for (d.headers) |f| {
                if (std.mem.eql(u8, f.name, "x-custom")) {
                    found_custom = true;
                    try std.testing.expectEqualStrings("literal-value", f.value);
                }
            }
            try std.testing.expect(found_custom);
        },
        else => {},
    };
    try std.testing.expect(saw);
}

test "HEADERS without END_HEADERS still assembles in header_block" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, .defaults);
    defer session.deinit();
    try ingestPrefaceAndSettings(&session, gpa);

    const hdr_fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(gpa, &hdr_fields);
    defer gpa.free(block);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = false },
        .stream_id = 1,
    }).encode(&hdr_buf);
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);
    try session.ingest(wire.items);
    try std.testing.expectEqual(block.len, session.header_block.items.len);
    try std.testing.expectEqual(@as(?u31, 1), session.expect_continuation);
}
