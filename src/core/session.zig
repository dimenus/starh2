//! Deterministic HTTP/2 connection state machine. No I/O, no clocks.
const std = @import("std");
const frame = @import("frame.zig");
const hpack = @import("hpack.zig");
const fields = @import("fields.zig");
const stream_mod = @import("stream.zig");
const flow = @import("flow.zig");
const limits_mod = @import("limits.zig");
const rates_mod = @import("rates.zig");

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

pub const Session = struct {
    allocator: std.mem.Allocator,
    limits: limits_mod.Limits,
    parser: frame.Parser,
    decoder: hpack.Decoder,
    windows: flow.Windows = .{},
    streams: std.AutoHashMap(u31, stream_mod.Stream),
    tombstones: std.ArrayList(Tombstone),
    /// Fixed-capacity intent ring — enqueue fails before ownership transfer when full.
    intent_storage: []Intent = &.{},
    intent_len: usize = 0,
    /// Slots reserved for GOAWAY / connection_error so PoolExhausted can still fail-close.
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
    };

    pub fn init(allocator: std.mem.Allocator, limits: limits_mod.Limits) !Session {
        const intent_cap = @max(limits.intent_entries_per_connection, 16);
        const intent_storage = try allocator.alloc(Intent, intent_cap);
        var intent_storage_owned = true;
        errdefer if (intent_storage_owned) allocator.free(intent_storage);
        var s: Session = .{
            .allocator = allocator,
            .limits = limits,
            .parser = try frame.Parser.initReserved(allocator, frame.DEFAULT_MAX_FRAME_SIZE),
            .decoder = hpack.Decoder.init(allocator),
            .streams = std.AutoHashMap(u31, stream_mod.Stream).init(allocator),
            .tombstones = .empty,
            .intent_storage = intent_storage,
            .intent_len = 0,
            .terminal_reserve = 2,
            .pending_bodies = std.AutoHashMap(u31, std.ArrayList(u8)).init(allocator),
            .pending_headers = std.AutoHashMap(u31, []hpack.HeaderField).init(allocator),
            .pending_trailers = std.AutoHashMap(u31, []hpack.HeaderField).init(allocator),
            .body_idle_ns = std.AutoHashMap(u31, u64).init(allocator),
        };
        intent_storage_owned = false;
        errdefer s.deinit();
        try s.streams.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.tombstones.ensureTotalCapacity(allocator, limits.stream_tombstones);
        try s.pending_bodies.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.pending_headers.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.pending_trailers.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.body_idle_ns.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.header_block.ensureTotalCapacity(allocator, frame.DEFAULT_MAX_FRAME_SIZE);
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
        if (self.intent_storage.len != 0) self.allocator.free(self.intent_storage);
        self.tombstones.deinit(self.allocator);
        self.header_block.deinit(self.allocator);
        var bit = self.pending_bodies.iterator();
        while (bit.next()) |e| {
            if (self.stream_hooks) |h| {
                if (h.releaseRequest) |rel| rel(h.ctx, e.value_ptr.items.len);
            }
            e.value_ptr.deinit(self.allocator);
        }
        self.pending_bodies.deinit();
        var hit = self.pending_headers.iterator();
        while (hit.next()) |e| {
            for (e.value_ptr.*) |f| {
                self.allocator.free(@constCast(f.name));
                self.allocator.free(@constCast(f.value));
            }
            self.allocator.free(e.value_ptr.*);
        }
        self.pending_headers.deinit();
        var tit = self.pending_trailers.iterator();
        while (tit.next()) |e| {
            for (e.value_ptr.*) |f| {
                self.allocator.free(@constCast(f.name));
                self.allocator.free(@constCast(f.value));
            }
            self.allocator.free(e.value_ptr.*);
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
            .outbound_frame => |*f| self.allocator.free(f.payload),
            .dispatch_request => |*d| {
                for (d.headers) |f| {
                    self.allocator.free(@constCast(f.name));
                    self.allocator.free(@constCast(f.value));
                }
                self.allocator.free(d.headers);
                for (d.trailers) |f| {
                    self.allocator.free(@constCast(f.name));
                    self.allocator.free(@constCast(f.value));
                }
                if (d.trailers.len != 0) self.allocator.free(d.trailers);
                self.allocator.free(d.body);
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

    pub fn failClosedInternal(self: *Session) !void {
        if (self.terminal != .none) return;
        // Use reserved terminal capacity — must not recurse through pushIntentOrFailClosed.
        const last = self.last_processed_stream;
        var buf: [17]u8 = undefined;
        const n = try frame.Serializer.goaway(&buf, last, .internal_error, &.{});
        const p = self.allocator.dupe(u8, buf[0..n]) catch return error.OutOfMemory;
        self.pushIntent(.{ .outbound_frame = .{ .typ = .goaway, .payload = p } }) catch {
            self.allocator.free(p);
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
    pub fn makeDataFrame(self: *Session, stream_id: u31, data: []const u8, end_stream: bool) ![]u8 {
        const s = self.streams.getPtr(stream_id) orelse return error.StreamClosed;
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
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(take),
            .type = .data,
            .flags = .{ .end_stream = end_stream },
            .stream_id = stream_id,
        };
        fh.encode(&hdr_buf);
        const out = try self.allocator.alloc(u8, frame.FRAME_HEADER_LEN + take);
        @memcpy(out[0..frame.FRAME_HEADER_LEN], &hdr_buf);
        if (take > 0) @memcpy(out[frame.FRAME_HEADER_LEN..], data[0..take]);
        if (take > 0) {
            s.window.send -= @intCast(take);
            self.windows.conn_send -= @intCast(take);
        }
        if (end_stream) {
            s.localEndStream();
            self.releaseConcurrency(stream_id);
        }
        return out;
    }

    fn emitServerPreface(self: *Session) !void {
        var buf: [256]u8 = undefined;
        const n = try frame.Serializer.settingsFrame(&buf, false, &frame.serverPrefaceSettings);
        const payload = try self.allocator.dupe(u8, buf[0..n]);
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
        const wp = try self.allocator.dupe(u8, wbuf[0..wn]);
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
                const p = try self.allocator.dupe(u8, buf[0..n]);
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
        const p = try self.allocator.dupe(u8, buf[0..n]);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .goaway, .payload = p } });
    }

    fn beginGracefulPhase1(self: *Session) !void {
        if (self.grace_phase != .none or self.terminal != .none) return;
        // Graceful shutdown: GOAWAY NO_ERROR with last-stream-ID 2^31-1, then PING.
        try self.emitGoawayNoTerminal(std.math.maxInt(u31), .no_error);
        var buf: [17]u8 = undefined;
        const n = try frame.Serializer.ping(&buf, false, &self.grace_ping);
        const p = try self.allocator.dupe(u8, buf[0..n]);
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
        var buf: [32]u8 = undefined;
        const n = try frame.Serializer.goaway(&buf, last, code, &.{});
        const p = try self.allocator.dupe(u8, buf[0..n]);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .goaway, .payload = p } });
        try self.pushIntentOrFailClosed(.{ .connection_error = .{ .code = code, .last_stream_id = last } });
        self.terminal = .{ .goaway = .{ .code = code, .last_stream_id = last } };
    }

    fn releaseConcurrency(self: *Session, stream_id: u31) void {
        const s = self.streams.getPtr(stream_id) orelse return;
        if (s.concurrency_released) return;
        if (s.state != .closed) return;
        s.concurrency_released = true;
        if (self.active_streams > 0) self.active_streams -= 1;
        if (self.stream_hooks) |h| h.onRelease(h.ctx);
        // Drop map entry; late frames classify via bounded tombstones.
        if (!self.isTombstoned(stream_id)) {
            self.addTombstone(stream_id, .no_error) catch {};
        }
        _ = self.streams.remove(stream_id);
    }

    fn emitRst(self: *Session, stream_id: u31, code: frame.ErrorCode) !void {
        var buf: [13]u8 = undefined;
        const n = try frame.Serializer.rstStream(&buf, stream_id, code);
        const p = try self.allocator.dupe(u8, buf[0..n]);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .rst_stream, .payload = p, .stream_id = stream_id } });
        try self.pushIntentOrFailClosed(.{ .stream_reset = .{ .stream_id = stream_id, .code = code, .from_peer = false } });
        if (self.streams.getPtr(stream_id)) |s| {
            s.onRst();
        }
        try self.addTombstone(stream_id, code);
        self.releaseConcurrency(stream_id);
    }

    fn addTombstone(self: *Session, id: u31, code: frame.ErrorCode) !void {
        if (self.tombstones.items.len >= self.limits.stream_tombstones) {
            _ = self.tombstones.orderedRemove(0);
        }
        try self.tombstones.append(self.allocator, .{ .id = id, .code = code });
    }

    fn handleFrame(self: *Session, ev: frame.FrameEvent) !void {
        defer if (ev.payload.len != 0) self.allocator.free(ev.payload);
        const hdr = ev.header;
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
            try self.consumeContinuation(hdr, ev.payload);
            try self.chargeRate(hdr.type, already_terminal);
            return;
        }

        switch (hdr.type) {
            .data => try self.onData(hdr, ev.payload),
            .headers => try self.onHeaders(hdr, ev.payload),
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
                const code: frame.ErrorCode = @enumFromInt(std.mem.readInt(u32, ev.payload[0..4], .big));
                if (self.streams.getPtr(hdr.stream_id)) |s| {
                    s.onRst();
                    try self.addTombstone(hdr.stream_id, code);
                    self.releaseConcurrency(hdr.stream_id);
                    // Cancel handler; never RST in response to peer RST.
                    try self.pushIntentOrFailClosed(.{ .stream_reset = .{
                        .stream_id = hdr.stream_id,
                        .code = code,
                        .from_peer = true,
                    } });
                } else {
                    // RST_STREAM on idle / unknown stream is a connection error (RFC 9113 §5.1 / §5.4.1).
                    try self.failConnection(.protocol_error);
                    return;
                }
            },
            .settings => try self.onSettings(hdr, ev.payload),
            .push_promise => try self.failConnection(.protocol_error),
            .ping => try self.onPing(hdr, ev.payload),
            .goaway => {
                if (hdr.stream_id != 0 or hdr.length < 8) {
                    try self.failConnection(.frame_size_error);
                    return;
                }
                const last: u31 = @intCast(std.mem.readInt(u32, ev.payload[0..4], .big) & 0x7fff_ffff);
                self.goaway_ceiling = last;
                self.peer_goaway = true;
                // Existing streams at or below last may finish; do not tear down transport yet.
            },
            .window_update => try self.onWindowUpdate(hdr, ev.payload),
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
    pub fn checkIdleDeadlines(self: *Session, now_ns: u64) !void {
        if (self.terminal != .none) return;
        if (self.expect_continuation != null and self.field_block_started_ns != 0) {
            if (now_ns -% self.field_block_started_ns >= self.limits.field_block_timeout_ns) {
                try self.failConnection(.enhance_your_calm);
                return;
            }
        }
        var stale: std.ArrayList(u31) = .empty;
        defer stale.deinit(self.allocator);
        var it = self.body_idle_ns.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.* == 0) continue;
            if (now_ns -% e.value_ptr.* >= self.limits.request_body_idle_timeout_ns) {
                try stale.append(self.allocator, e.key_ptr.*);
            }
        }
        for (stale.items) |sid| {
            _ = self.body_idle_ns.remove(sid);
            try self.emitRst(sid, .cancel);
        }
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
                    self.peer_max_frame_size = @min(value, frame.DEFAULT_MAX_FRAME_SIZE);
                },
                .header_table_size => self.peer_header_table_size = value,
                else => {},
            }
        }
        // ACK
        var buf: [9]u8 = undefined;
        const n = try frame.Serializer.settingsFrame(&buf, true, &.{});
        const p = try self.allocator.dupe(u8, buf[0..n]);
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
        const p = try self.allocator.dupe(u8, buf[0..n]);
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
        } else {
            // WINDOW_UPDATE on idle stream → connection PROTOCOL_ERROR
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
        const total: i32 = @intCast(payload.len);
        self.windows.debitConnRecv(total) catch {
            try self.failConnection(.flow_control_error);
            return;
        };
        const s = self.streams.getPtr(hdr.stream_id) orelse {
            // DATA on idle / unknown stream → connection PROTOCOL_ERROR (RFC 9113 §5.1).
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

        // Early-reject streams still consume/discard DATA through remote END_STREAM.
        if (s.early_status == null and !s.refused_before_dispatch and s.headers_done) {
            var body = self.pending_bodies.getPtr(hdr.stream_id) orelse blk: {
                try self.pending_bodies.put(hdr.stream_id, .empty);
                break :blk self.pending_bodies.getPtr(hdr.stream_id).?;
            };
            if (body.items.len + data.len > self.limits.request_body_bytes) {
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
                            try body.appendSlice(self.allocator, data);
                        }
                    } else {
                        try body.appendSlice(self.allocator, data);
                    }
                } else {
                    try body.appendSlice(self.allocator, data);
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
        self.releaseConcurrency(hdr.stream_id);

        // WINDOW_UPDATE at half
        if (s.window.needsWindowUpdate()) {
            const inc = s.window.windowUpdateIncrement();
            if (inc > 0) {
                s.window.creditRecv(inc);
                var buf: [13]u8 = undefined;
                const n = try frame.Serializer.windowUpdate(&buf, hdr.stream_id, inc);
                const p = try self.allocator.dupe(u8, buf[0..n]);
                try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .window_update, .payload = p, .stream_id = hdr.stream_id } });
            }
        }
        if (self.windows.needsConnWindowUpdate()) {
            const inc = self.windows.connWindowUpdateIncrement();
            if (inc > 0) {
                self.windows.creditConnRecv(inc);
                var buf: [13]u8 = undefined;
                const n = try frame.Serializer.windowUpdate(&buf, 0, inc);
                const p = try self.allocator.dupe(u8, buf[0..n]);
                try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .window_update, .payload = p } });
            }
        }
    }

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
        if (hdr.stream_id == 0 or hdr.stream_id % 2 == 0) {
            try self.failConnection(.protocol_error);
            return;
        }
        if (self.isTombstoned(hdr.stream_id)) {
            // HEADERS on a closed stream: connection STREAM_CLOSED (RFC 9113 §5.1 / h2spec 5.1/12).
            try self.failConnection(.stream_closed);
            return;
        }
        if (self.streams.getPtr(hdr.stream_id)) |existing| {
            if (existing.state == .closed) {
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
        try self.header_block.appendSlice(self.allocator, fragment);
        self.continuation_count = 0;
        self.header_end_stream = hdr.flags.end_stream;
        if (!hdr.flags.end_headers) {
            self.expect_continuation = hdr.stream_id;
            if (self.field_block_started_ns == 0) self.field_block_started_ns = self.edge_now_ns;
            return;
        }
        self.field_block_started_ns = 0;
        try self.finishHeaderBlock(hdr.stream_id);
    }

    fn consumeContinuation(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        self.continuation_count += 1;
        if (self.continuation_count > self.limits.continuation_frames or
            self.header_block.items.len + payload.len > self.limits.compressed_header_bytes)
        {
            try self.failConnection(.enhance_your_calm);
            return;
        }
        try self.header_block.appendSlice(self.allocator, payload);
        if (!hdr.flags.end_headers) return;
        const sid = self.expect_continuation.?;
        self.expect_continuation = null;
        self.field_block_started_ns = 0;
        try self.finishHeaderBlock(sid);
    }

    fn finishHeaderBlock(self: *Session, stream_id: u31) !void {
        const decoded = self.decoder.decode(
            self.header_block.items,
            self.limits.header_fields,
            self.limits.decoded_header_bytes,
            256,
            self.limits.regular_field_value_bytes,
        ) catch |err| {
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
                try self.emitRst(stream_id, .refused_stream);
                self.decoder.freeResult(decoded);
                return;
            }
            if (self.stream_hooks) |h| {
                if (!h.tryAdmit(h.ctx)) {
                    try self.emitRst(stream_id, .refused_stream);
                    self.decoder.freeResult(decoded);
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
                self.decoder.freeResult(decoded);
                try self.emitRst(stream_id, .protocol_error);
                return;
            }
            if (s.end_stream_remote or s.state == .half_closed_remote or s.state == .closed) {
                self.decoder.freeResult(decoded);
                try self.emitRst(stream_id, .stream_closed);
                return;
            }
            for (decoded.fields) |f| {
                if (f.name.len > 0 and f.name[0] == ':') {
                    self.decoder.freeResult(decoded);
                    try self.emitRst(stream_id, .protocol_error);
                    return;
                }
            }
            // Early-rejected streams: trailers close remote; ensure the one HTTP response exists.
            if (s.early_status) |st| {
                self.decoder.freeResult(decoded);
                s.onHeaders(true) catch {};
                try self.emitEarlyReject(stream_id, st);
                self.releaseConcurrency(stream_id);
                return;
            }
            s.onHeaders(true) catch |err| {
                self.decoder.freeResult(decoded);
                try self.emitRst(stream_id, stream_mod.Stream.toErrorCode(err));
                return;
            };
            try self.pending_trailers.put(stream_id, decoded.fields);
            try self.maybeDispatch(stream_id);
            self.releaseConcurrency(stream_id);
            return;
        }

        const parsed = fields.validateRequestFields(decoded.fields) catch |err| {
            self.decoder.freeResult(decoded);
            if (err == error.PathTooLong) {
                s.refused_before_dispatch = true;
                s.early_status = 414;
                s.onHeaders(self.header_end_stream) catch {};
                try self.emitEarlyReject(stream_id, 414);
                self.releaseConcurrency(stream_id);
                return;
            }
            try self.emitRst(stream_id, .protocol_error);
            return;
        };
        if (decoded.policy_exceeded) {
            s.refused_before_dispatch = true;
            s.early_status = 431;
        }
        if (parsed.content_length) |cl| {
            if (cl > self.limits.request_body_bytes) {
                s.refused_before_dispatch = true;
                s.early_status = 413;
            }
        }

        s.content_length = parsed.content_length;
        const early = s.early_status;
        s.onHeaders(self.header_end_stream) catch |err| {
            self.decoder.freeResult(decoded);
            try self.emitRst(stream_id, stream_mod.Stream.toErrorCode(err));
            return;
        };
        if (early) |st| {
            self.decoder.freeResult(decoded);
            try self.emitEarlyReject(stream_id, st);
            self.releaseConcurrency(stream_id);
        } else {
            try self.pending_headers.put(stream_id, decoded.fields);
            if (self.header_end_stream) {
                try self.maybeDispatch(stream_id);
            }
            self.releaseConcurrency(stream_id);
        }
    }

    fn maybeDispatch(self: *Session, stream_id: u31) !void {
        const s = self.streams.getPtr(stream_id) orelse return;
        if (!s.end_stream_remote or s.refused_before_dispatch or s.handler_started) return;
        const hdrs = self.pending_headers.fetchRemove(stream_id) orelse return;
        var headers_owned = true;
        errdefer if (headers_owned) {
            for (hdrs.value) |f| {
                self.allocator.free(@constCast(f.name));
                self.allocator.free(@constCast(f.value));
            }
            self.allocator.free(hdrs.value);
        };
        const body_entry = self.pending_bodies.fetchRemove(stream_id);
        const body = blk: {
            if (body_entry) |e| {
                var list = e.value;
                if (self.stream_hooks) |h| {
                    if (h.releaseRequest) |rel| rel(h.ctx, list.items.len);
                }
                break :blk list.toOwnedSlice(self.allocator) catch |err| {
                    list.deinit(self.allocator);
                    self.failClosedInternal() catch {};
                    return err;
                };
            }
            break :blk &.{};
        };
        var body_owned = body.len != 0;
        errdefer if (body_owned) self.allocator.free(body);

        const parsed = fields.validateRequestFields(hdrs.value) catch {
            for (hdrs.value) |f| {
                self.allocator.free(@constCast(f.name));
                self.allocator.free(@constCast(f.value));
            }
            self.allocator.free(hdrs.value);
            headers_owned = false;
            if (body.len != 0) self.allocator.free(body);
            body_owned = false;
            try self.emitRst(stream_id, .protocol_error);
            return;
        };

        const trailers_entry = self.pending_trailers.fetchRemove(stream_id);
        const trailers: []hpack.HeaderField = if (trailers_entry) |e| e.value else try self.allocator.alloc(hpack.HeaderField, 0);
        var trailers_owned = trailers.len != 0;
        errdefer if (trailers_owned) {
            for (trailers) |f| {
                self.allocator.free(@constCast(f.name));
                self.allocator.free(@constCast(f.value));
            }
            self.allocator.free(trailers);
        };

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
            .headers = hdrs.value,
            .body = body,
            .trailers = trailers,
        } });
    }

    fn cmdHeaders(self: *Session, stream_id: u31, status: u16, headers: []const hpack.HeaderField, end_stream: bool) !void {
        var list: std.ArrayList(hpack.HeaderField) = .empty;
        defer list.deinit(self.allocator);
        var status_buf: [3]u8 = undefined;
        const status_str = try std.fmt.bufPrint(&status_buf, "{d}", .{status});
        try list.append(self.allocator, .{ .name = ":status", .value = status_str });
        for (headers) |h| try list.append(self.allocator, h);
        const block = try hpack.Encoder.encode(self.allocator, list.items);
        defer self.allocator.free(block);

        // Build HEADERS frame bytes
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = end_stream },
            .stream_id = stream_id,
        };
        fh.encode(&hdr_buf);
        const out = try self.allocator.alloc(u8, frame.FRAME_HEADER_LEN + block.len);
        @memcpy(out[0..frame.FRAME_HEADER_LEN], &hdr_buf);
        @memcpy(out[frame.FRAME_HEADER_LEN..], block);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{
            .typ = .headers,
            .payload = out,
            .stream_id = stream_id,
            .flags = fh.flags,
        } });
        if (self.streams.getPtr(stream_id)) |s| {
            if (end_stream) {
                s.localEndStream();
                self.releaseConcurrency(stream_id);
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
    pub fn streamSendAvailable(self: *const Session, stream_id: u31) i32 {
        const s = self.streams.get(stream_id) orelse return 0;
        return s.window.availableSend(self.windows.conn_send);
    }

    pub fn connectionSendAvailable(self: *const Session) i32 {
        if (self.windows.conn_send <= 0) return 0;
        return self.windows.conn_send;
    }
};

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
                for (d.headers) |h| {
                    gpa.free(@constCast(h.name));
                    gpa.free(@constCast(h.value));
                }
                gpa.free(d.headers);
                if (d.trailers.len != 0) {
                    for (d.trailers) |h| {
                        gpa.free(@constCast(h.name));
                        gpa.free(@constCast(h.value));
                    }
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
