//! HTTP/1.1 edge channel. One stream, no Session, no FairScheduler.
//!
//! The connection task owns the socket (h1c) or the TLS `Conn` (ALPN `http/1.1`
//! / no ALPN). It accumulates a head into a boot-reserved buffer and parses
//! once. A complete handler reads the Content-Length body, then runs on the
//! actor. A task handler reads the same bounded body, then runs on its own
//! task (`Request.body` holds those bytes). `TaskHandler.stream_request`
//! starts after the head with an empty body; unread Content-Length forces
//! close so leftover bytes are not the next request. The response uses one
//! framing (Content-Length, chunked, or close-delimited). Keep-alive loops
//! until a close reason.
const std = @import("std");
const zio = @import("zio");
const request = @import("../http/request.zig");
const response = @import("../http/response.zig");
const router_mod = @import("../http/router.zig");
const hpack = @import("../core/hpack.zig");
const parser = @import("../http1/parser.zig");
const connection = @import("connection.zig");
const tls_edge = @import("tls.zig");
const io_queue = @import("io_queue.zig");
const wire_pump = @import("wire_pump.zig");
const limits_mod = @import("../core/wire_const.zig");

const live: u8 = 0;
const reaper_owned: u8 = 1;
const reported: u8 = 2;

var no_shutdown_event: zio.ResetEvent = .init;

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

const Hctx = struct {
    conn: *H1Conn,
    terminal: *response.SlotTerminal,
    slot: *connection.HandlerSlot,
    method: request.Method,
};

const Framing = enum { content_length, chunked, close_delimited };

const H1Conn = struct {
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

    reader: std.Io.net.Stream.Reader = undefined,
    writer: std.Io.net.Stream.Writer = undefined,
    reader_bound: bool = false,

    slot: connection.HandlerSlot = .{},
    arena: std.heap.ArenaAllocator,
    completion_buf: [1]u31 = undefined,
    completion_ch: zio.Channel(u31) = undefined,
    join: ?std.Io.Future(void) = null,

    acc: parser.Accumulator = undefined,
    head: parser.Head = undefined,
    scheme: []const u8 = "http",

    framing: Framing = .content_length,
    pending_storage: []u8,
    pending_len: usize = 0,
    job_req: request.Request = undefined,
    job_resp: response.Response = undefined,
    job_hctx: Hctx = undefined,
    job_handler: router_mod.TaskHandler = undefined,
    committed: bool = false,
    occupies: bool = false,
    want_close: bool = false,
    finished: bool = false,
    no_payload: bool = false,
    resp_status: u16 = 0,
    request_held: usize = 0,
    socket_closed: bool = false,
    dispatch_headers: []request.Header = &.{},

    tls_pump: ?*tls_edge.Pump = null,
    tls_pump_storage: []u8 = &.{},
    tls_recv_buf: []u8 = &.{},
    tls_chunk_storage: []u8 = &.{},
    tls_to_actor_buf: []wire_pump.WireChunk = &.{},
    tls_write_ch_buf: []wire_pump.WireChunk = &.{},
    tls_ack_buf: []wire_pump.WriteCompletion = &.{},
    tls_read_free_buf: []u32 = &.{},
    tls_to_actor: zio.Channel(wire_pump.WireChunk) = undefined,
    tls_write_ch: zio.Channel(wire_pump.WireChunk) = undefined,
    tls_acks: zio.Channel(wire_pump.WriteCompletion) = undefined,
    tls_read_free: std.Io.Queue(u32) = undefined,
    tls_write_free_buf: []u32 = &.{},
    tls_write_free: std.Io.Queue(u32) = undefined,
    tls_req_buf: [1]u32 = undefined,
    tls_req_ch: zio.Channel(u32) = undefined,
    tls_ok_buf: [1]bool = undefined,
    tls_ok_ch: zio.Channel(bool) = undefined,
    tls_site: std.atomic.Value(u8) = .init(0),
    tls_live_handlers: std.atomic.Value(usize) = .init(0),
    tls_out: []u8 = &.{},
    offload_tls_io: bool = false,
    tls_write_inflight: bool = false,

    fn deinit(self: *H1Conn) void {
        std.debug.assert(!self.slot.in_use);
        std.debug.assert(self.request_held == 0);
        if (self.tls_pump) |p| {
            p.shutdownCq();
        }
        if (!self.socket_closed) {
            self.stream.close(self.io);
            self.socket_closed = true;
        }
        if (self.tls) |t| {
            t.deinit();
            self.gpa.destroy(t);
        }
        self.arena.deinit();
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
        if (self.tls_pump) |p| {
            self.gpa.destroy(p);
            self.tls_pump = null;
        }
        if (self.tls_recv_buf.len != 0) self.gpa.free(self.tls_recv_buf);
        if (self.tls_chunk_storage.len != 0) self.gpa.free(self.tls_chunk_storage);
        if (self.tls_to_actor_buf.len != 0) self.gpa.free(self.tls_to_actor_buf);
        if (self.tls_write_ch_buf.len != 0) self.gpa.free(self.tls_write_ch_buf);
        if (self.tls_ack_buf.len != 0) self.gpa.free(self.tls_ack_buf);
        if (self.tls_read_free_buf.len != 0) self.gpa.free(self.tls_read_free_buf);
        if (self.tls_write_free_buf.len != 0) self.gpa.free(self.tls_write_free_buf);
        if (self.tls_out.len != 0) self.gpa.free(self.tls_out);
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
        if (tls) |t| {
            t.deinit();
            config.gpa.destroy(t);
        }
        stream.close(config.io);
        return;
    };
    defer conn.deinit();
    if (tls != null) startTlsPump(&conn) catch {
        return;
    };
    if (leftover.len != 0) {
        stashCarry(&conn, leftover, 0) catch return;
    }
    runLoop(&conn) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {},
    };
}

fn initConn(self: *H1Conn, stream: std.Io.net.Stream, config: connection.ConnConfig, tls: ?*tls_edge.Conn) !void {
    const gpa = config.gpa;
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
        .arena = std.heap.ArenaAllocator.init(gpa),
        .scheme = if (tls != null) "https" else "http",
    };
    self.acc = parser.Accumulator.init(self.head_buf);
    self.completion_ch = .init(&self.completion_buf);
    if (tls == null) {
        self.reader = stream.reader(config.io, self.recv_buf);
        self.writer = stream.writer(config.io, self.write_buf);
        self.reader_bound = true;
    }
}

fn tlsNetHandle(h: std.Io.net.Socket.Handle) zio.ev.Backend.NetHandle {
    return if (@typeInfo(zio.ev.Backend.NetHandle) == .pointer) @ptrCast(h) else h;
}

fn startTlsPump(self: *H1Conn) !void {
    const t = self.tls orelse return;
    const gpa = self.gpa;
    self.tls_recv_buf = try gpa.alloc(u8, limits_mod.TLS_CIPHER_CHUNK_SIZE);
    self.tls_chunk_storage = try gpa.alloc(u8, limits_mod.WIRE_CHUNK_SIZE);
    self.tls_to_actor_buf = try gpa.alloc(wire_pump.WireChunk, 1);
    self.tls_write_ch_buf = try gpa.alloc(wire_pump.WireChunk, 4);
    self.tls_ack_buf = try gpa.alloc(wire_pump.WriteCompletion, 4);
    self.tls_read_free_buf = try gpa.alloc(u32, 1);
    self.tls_write_free_buf = try gpa.alloc(u32, 1);
    self.tls_out = try gpa.alloc(u8, limits_mod.WIRE_CHUNK_SIZE);

    self.tls_to_actor = .init(self.tls_to_actor_buf);
    self.tls_write_ch = .init(self.tls_write_ch_buf);
    self.tls_acks = .init(self.tls_ack_buf);
    self.tls_read_free = .init(self.tls_read_free_buf);
    self.tls_write_free = .init(self.tls_write_free_buf);
    self.tls_req_ch = .init(&self.tls_req_buf);
    self.tls_ok_ch = .init(&self.tls_ok_buf);
    self.tls_write_free.putOneUncancelable(self.io, 0) catch unreachable;

    const pump = try gpa.create(tls_edge.Pump);
    errdefer gpa.destroy(pump);
    pump.* = .{
        .io = self.io,
        .conn = t,
        .to_actor = &self.tls_to_actor,
        .write_ch = &self.tls_write_ch,
        .sock = tlsNetHandle(self.stream.socket.handle),
        .recv_buf = self.tls_recv_buf,
        .site = &self.tls_site,
        .completions = &self.tls_acks,
        .gpa = gpa,
        .chunk_storage = self.tls_chunk_storage,
        .n_chunks = 1,
        .read_free = &self.tls_read_free,
        .write_free = &self.tls_write_free,
        .live_task_handlers = &self.tls_live_handlers,
    };
    if (!pump.start()) {
        pump.shutdownCq();
        return error.TlsPumpStartFailed;
    }
    self.tls_pump = pump;
}

fn runLoop(self: *H1Conn) !void {
    var keep = true;
    while (keep) {
        if (shutdownRequested(self)) break;
        keep = try serveOne(self);
        if (self.occupies) break;
        if (!keep) break;
        _ = self.arena.reset(.retain_capacity);
        self.acc.reset();
        self.committed = false;
        self.finished = false;
        self.no_payload = false;
        self.resp_status = 0;
        self.framing = .content_length;
        self.pending_len = 0;
    }
    try shutdownHandlers(self);
    self.stream.shutdown(self.io, .both) catch {};
}

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
    const parsed = try readHead(self) orelse return false;
    switch (parsed) {
        .http09 => {
            return false;
        },
        .reject => |st| {
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
                if (head.content_length != 0) self.want_close = true;
            } else {
                body = try readBody(self, head) orelse {
                    try writeError(self, 413);
                    return false;
                };
                if (body.len > body_cap) {
                    try writeError(self, 413);
                    return false;
                }
                if (self.config.accounting) |a| {
                    if (!a.tryReserveRequest(body.len)) {
                        try writeError(self, 413);
                        return false;
                    }
                    self.request_held = body.len;
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
                    releaseRequest(self);
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

            try dispatch(self, head, body, headers, path, query, authority);
            releaseRequest(self);
            if (test_channel_mutation == .m3_ignore_close) self.want_close = false;
            if (self.want_close or close_after) return false;
            return true;
        },
    }
}

fn releaseRequest(self: *H1Conn) void {
    if (self.request_held != 0) {
        if (self.config.accounting) |a| a.releaseRequest(self.request_held);
        self.request_held = 0;
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
    // Idle, not total: re-arm on each byte of progress so a slow-but-moving
    // body is not killed by time spent on earlier chunks.
    var idle_deadline_ns = nowNs(self.io) +% self.config.limits.request_body_idle_timeout_ns;
    while (got < need) {
        if (nowNs(self.io) >= idle_deadline_ns) return error.IdleTimeout;
        var tmp: [4096]u8 = undefined;
        const n = try readSome(self, tmp[0..], idle_deadline_ns) orelse return error.IdleTimeout;
        if (n == 0) return error.ConnectionClosed;
        const take = @min(n, need - got);
        @memcpy(self.body_buf[got .. got + take], tmp[0..take]);
        got += take;
        idle_deadline_ns = nowNs(self.io) +% self.config.limits.request_body_idle_timeout_ns;
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
    // Append. A replace drops a pipelined prefix already in carry when
    // waitPeerByte (or a second TLS ingest) delivers the rest of the next
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
    const pump = self.tls_pump orelse return;
    while (pump.pending_n > 0 or pump.send_armed) {
        waitTlsCq(self, null) catch return error.WriteFailed;
        if (pump.pending_n > 0) {
            if (!pump.drivePending()) return error.WriteFailed;
        }
    }
}

fn waitTlsCq(self: *H1Conn, deadline_ns: ?u64) !void {
    const pump = self.tls_pump orelse return;
    const shutdown_ev = self.config.shutdown_event orelse &no_shutdown_event;
    const timeout: zio.Timeout = if (deadline_ns) |d| blk: {
        const now = nowNs(self.io);
        if (d <= now) return;
        break :blk .{ .duration = .fromNanoseconds(d - now) };
    } else .none;
    const winner = zio.select(.{
        .io = &pump.cq,
        .timer = timeout,
        .shutdown = shutdown_ev,
    }) catch return error.Canceled;
    switch (winner) {
        .io => |r| {
            const c = r catch {
                if (pump.cq.isDrained()) return;
                return;
            };
            _ = pump.onCqComplete(c);
        },
        .timer => {},
        .shutdown => return error.Canceled,
    }
}

fn readSomeTls(self: *H1Conn, buf: []u8, deadline_ns: u64) !?usize {
    const pump = self.tls_pump orelse return error.TlsReadFailed;
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
                .eof => return @as(usize, 0),
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
        waitTlsCq(self, deadline_ns) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
        };
        if (nowNs(self.io) >= deadline_ns) return null;
    }
}

fn readSome(self: *H1Conn, buf: []u8, deadline_ns: u64) !?usize {
    if (self.tls != null) {
        return readSomeTls(self, buf, deadline_ns);
    }
    var dest: [1][]u8 = .{buf};
    const Read = union(enum) {
        data: std.Io.Reader.Error!usize,
        timer: std.Io.Cancelable!void,
        shutdown: std.Io.Cancelable!void,
    };
    var result_buf: [3]Read = undefined;
    var select = std.Io.Select(Read).init(self.io, &result_buf);
    errdefer select.cancelDiscard();
    const ReadFn = struct {
        fn run(reader: *std.Io.Reader, d: [][]u8) std.Io.Reader.Error!usize {
            return reader.readVec(d);
        }
    };
    try select.concurrent(.data, ReadFn.run, .{ &self.reader.interface, dest[0..] });
    const timeout: std.Io.Timeout = .{ .deadline = .{
        .raw = std.Io.Timestamp.fromNanoseconds(@intCast(deadline_ns)),
        .clock = .awake,
    } };
    try select.concurrent(.timer, waitTimer, .{ timeout, self.io });
    if (self.config.shutdown_event) |ev| {
        try select.concurrent(.shutdown, waitShutdown, .{ ev, self.io });
    }
    const selected = try select.await();
    defer select.cancelDiscard();
    return switch (selected) {
        .data => |r| r catch |err| switch (err) {
            error.EndOfStream => @as(usize, 0),
            else => return err,
        },
        .timer => null,
        .shutdown => error.Canceled,
    };
}

fn waitTimer(timeout: std.Io.Timeout, io: std.Io) std.Io.Cancelable!void {
    return timeout.sleep(io);
}

fn waitShutdown(ev: *zio.ResetEvent, io: std.Io) std.Io.Cancelable!void {
    _ = io;
    ev.wait() catch return error.Canceled;
}

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn suppressPayload(method: request.Method, status: u16) bool {
    return method == .HEAD or status < 200 or status == 204 or status == 304;
}

fn dispatch(
    self: *H1Conn,
    head: parser.Head,
    body: []const u8,
    headers: []const request.Header,
    path: []const u8,
    query: []const u8,
    authority: []const u8,
) !void {
    if (self.config.accounting) |a| {
        if (!a.tryAdmitStream()) {
            try writeError(self, 503);
            self.want_close = true;
            return;
        }
    }
    errdefer if (self.config.accounting) |a| a.releaseStream();

    const method = request.Method.parse(head.method_raw);
    const matched = self.config.router.match(method, head.path);
    const complete_inline = switch (matched) {
        .found => |f| f.handler == .complete,
        else => true,
    };

    var reaper_reserved = false;
    if (!complete_inline) {
        if (self.config.accounting) |a| {
            if (!a.tryReserveReaper()) {
                try writeError(self, 503);
                self.want_close = true;
                return;
            }
            reaper_reserved = true;
        }
    }
    errdefer if (reaper_reserved) {
        if (self.config.accounting) |a| a.releaseReaper();
    };

    self.slot.in_use = true;
    self.slot.stream_id = 1;
    self.slot.terminal.clear();
    _ = self.slot.terminal.generation.fetchAdd(1, .acq_rel);
    self.slot.completion_owner.store(live, .release);
    self.slot.reaper_reserved = reaper_reserved;
    self.join = null;
    // Preserve unread-body close from serveOne. A bare assignment dropped it,
    // so leftover Content-Length bytes became the next request.
    self.want_close = self.want_close or head.connection_close;
    self.occupies = false;

    const req: request.Request = .{
        .method = method,
        .scheme = self.scheme,
        .authority = authority,
        .path = path,
        .path_remainder = switch (matched) {
            .found => |f| f.path_remainder,
            else => "",
        },
        .query = query,
        .headers = headers,
        .body = body,
        .trailers = &.{},
        .arena = self.arena.allocator(),
        .peer = connection.peerFromHandle(self.stream.socket.handle),
    };

    self.job_hctx = .{
        .conn = self,
        .terminal = &self.slot.terminal,
        .slot = &self.slot,
        .method = method,
    };
    self.job_req = req;
    self.job_resp = .{
        .stream_id = 1,
        .generation = self.slot.terminal.currentGeneration(),
        .terminal = &self.slot.terminal,
        .ctx = &self.job_hctx,
        .io = self.io,
        .sendFn = sendCb,
        .startFn = startCb,
        .writeFn = writeCb,
        .flushFn = flushCb,
        .abortFn = abortCb,
        .waitUntilFn = waitUntilCb,
    };

    if (complete_inline) {
        switch (matched) {
            .found => |f| {
                var cr: response.CompleteResponse = .{ .inner = &self.job_resp };
                f.handler.complete.runFn(f.handler.complete.ptr, &self.job_req, &cr) catch {
                    if (!self.committed) writeError(self, 500) catch {};
                    self.want_close = true;
                };
            },
            .not_found => try self.job_resp.send(404, &.{}, "not found"),
            .method_not_allowed => |ms| {
                _ = ms;
                try self.job_resp.send(405, &.{.{ .name = "allow", .value = "GET" }}, "");
            },
        }
        if (!self.job_resp.committed) {
            try self.job_resp.send(200, &.{}, "");
        }
        finishSlot(self);
        return;
    }

    self.job_handler = matched.found.handler.task;
    const Job = struct {
        fn run(conn: *H1Conn) void {
            defer finishSlotFromTask(conn);
            runTaskBody(conn);
        }
    };
    self.offload_tls_io = self.tls_pump != null;
    const handle = self.io.concurrent(Job.run, .{self}) catch {
        self.offload_tls_io = false;
        if (reaper_reserved) {
            if (self.config.accounting) |a| a.releaseReaper();
            self.slot.reaper_reserved = false;
        }
        try writeError(self, 503);
        self.want_close = true;
        finishSlot(self);
        return;
    };
    self.join = handle;
    waitDispatch(self) catch |err| {
        self.offload_tls_io = false;
        return err;
    };
    self.offload_tls_io = false;
}

fn runTaskBody(self: *H1Conn) void {
    if (self.slot.terminal.cancel_flag.load(.acquire)) return;
    if (self.slot.terminal.getCause() != null) return;
    const run_res: anyerror!void = self.job_handler.runFn(self.job_handler.ptr, &self.job_req, &self.job_resp);
    run_res catch {
        if (self.slot.terminal.getCause() != null) return;
        if (!self.job_resp.committed) {
            self.job_resp.send(500, &.{}, "internal error") catch {};
        } else if (!self.job_resp.finished) {
            finishFraming(self) catch {};
            self.want_close = true;
            self.job_resp.finished = true;
        }
    };
    if (self.slot.terminal.getCause() == null and !self.job_resp.committed) {
        self.job_resp.send(500, &.{}, "no response") catch {};
    }
}

fn waitPeerByte(self: *H1Conn) anyerror!?usize {
    var tmp: [256]u8 = undefined;
    const far = nowNs(self.io) +% (365 * std.time.ns_per_s);
    const n = try readSome(self, tmp[0..], far) orelse return @as(usize, 0);
    if (n != 0) try stashCarry(self, tmp[0..n], 0);
    return n;
}

fn tlsIdle(pump: *tls_edge.Pump) bool {
    return pump.pending_n == 0 and !pump.send_armed;
}

fn ackTlsWrite(self: *H1Conn, ok: bool) void {
    self.tls_write_inflight = false;
    self.tls_ok_ch.trySend(ok) catch {};
}

fn driveHandlerTlsWrite(self: *H1Conn, n: u32) void {
    const pump = self.tls_pump orelse {
        ackTlsWrite(self, false);
        return;
    };
    const idx = io_queue.tryGet(u32, &self.tls_write_free, self.io) orelse {
        ackTlsWrite(self, false);
        return;
    };
    const chunk = wire_pump.WireChunk{
        .bytes = self.tls_out[0..n],
        .len = n,
        .pool_index = idx,
    };
    if (!pump.writeChunks(chunk)) {
        ackTlsWrite(self, false);
        return;
    }
    if (tlsIdle(pump)) {
        ackTlsWrite(self, true);
        return;
    }
    self.tls_write_inflight = true;
}

fn ingestTlsPeer(self: *H1Conn) bool {
    const pump = self.tls_pump orelse return false;
    if (pump.inbound_eof) return true;
    if (pump.pending_read == null and pump.conn.pendingInbound()) {
        switch (pump.readOne()) {
            .eof => return true,
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

fn waitDispatchTls(self: *H1Conn) !void {
    var reaping = false;
    const pump = self.tls_pump.?;
    const shutdown_ev = self.config.shutdown_event orelse &no_shutdown_event;
    while (self.slot.in_use) {
        if (shutdownRequested(self) and !reaping) {
            self.slot.terminal.setCause(.server_shutdown);
            if (!cancelJoin(self)) {
                finishSlot(self);
                break;
            }
            reaping = true;
        }
        if (reaping) {
            const sid = self.completion_ch.receive() catch return error.Canceled;
            _ = sid;
            finishSlot(self);
            break;
        }

        const winner = zio.select(.{
            .io = &pump.cq,
            .comps = self.completion_ch.asyncReceive(),
            .writes = self.tls_req_ch.asyncReceive(),
            .shutdown = shutdown_ev,
        }) catch return error.Canceled;
        switch (winner) {
            .io => |r| {
                const c = r catch {
                    if (pump.cq.isDrained()) {
                        self.slot.terminal.setCause(.connection_closed);
                        if (!cancelJoin(self)) {
                            finishSlot(self);
                            break;
                        }
                        reaping = true;
                    }
                    continue;
                };
                if (pump.onCqComplete(c) == .exit) {
                    self.slot.terminal.setCause(.connection_closed);
                    if (!cancelJoin(self)) {
                        finishSlot(self);
                        break;
                    }
                    reaping = true;
                    continue;
                }
                if (self.tls_write_inflight) {
                    if (pump.pending_n > 0) {
                        if (!pump.drivePending()) {
                            ackTlsWrite(self, false);
                            self.slot.terminal.setCause(.connection_closed);
                            if (!cancelJoin(self)) {
                                finishSlot(self);
                                break;
                            }
                            reaping = true;
                            continue;
                        }
                    }
                    if (tlsIdle(pump)) ackTlsWrite(self, true);
                }
                if (ingestTlsPeer(self)) {
                    self.slot.terminal.setCause(.connection_closed);
                    if (!cancelJoin(self)) {
                        finishSlot(self);
                        break;
                    }
                    reaping = true;
                }
            },
            .comps => |r| {
                _ = r catch {};
                finishSlot(self);
                break;
            },
            .writes => |r| {
                const n = r catch continue;
                driveHandlerTlsWrite(self, n);
            },
            .shutdown => {
                self.slot.terminal.setCause(.server_shutdown);
                if (!cancelJoin(self)) {
                    finishSlot(self);
                    break;
                }
                reaping = true;
            },
        }
    }
}

fn waitDispatch(self: *H1Conn) !void {
    if (self.tls_pump != null) return waitDispatchTls(self);
    var reaping = false;
    while (self.slot.in_use) {
        if (shutdownRequested(self) and !reaping) {
            self.slot.terminal.setCause(.server_shutdown);
            if (!cancelJoin(self)) {
                finishSlot(self);
                break;
            }
            reaping = true;
        }
        if (reaping) {
            const sid = self.completion_ch.receive() catch return error.Canceled;
            _ = sid;
            finishSlot(self);
            break;
        }

        const Race = union(enum) {
            done: anyerror!u31,
            shutdown: std.Io.Cancelable!void,
            peer: anyerror!?usize,
        };
        var result_buf: [3]Race = undefined;
        var select = std.Io.Select(Race).init(self.io, &result_buf);
        errdefer select.cancelDiscard();
        const Recv = struct {
            fn run(ch: *zio.Channel(u31)) anyerror!u31 {
                return ch.receive();
            }
        };
        try select.concurrent(.done, Recv.run, .{&self.completion_ch});
        if (self.config.shutdown_event) |ev| {
            try select.concurrent(.shutdown, waitShutdown, .{ ev, self.io });
        }
        try select.concurrent(.peer, waitPeerByte, .{self});
        const selected = try select.await();
        defer select.cancelDiscard();
        switch (selected) {
            .done => |r| {
                _ = r catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    else => {},
                };
                finishSlot(self);
                break;
            },
            .shutdown => {
                self.slot.terminal.setCause(.server_shutdown);
                if (!cancelJoin(self)) {
                    finishSlot(self);
                    break;
                }
                reaping = true;
            },
            .peer => |r| {
                const n = r catch |err| switch (err) {
                    error.Canceled => return error.Canceled,
                    else => @as(?usize, 0),
                };
                if (n == null or n == 0) {
                    self.slot.terminal.setCause(.connection_closed);
                    if (!cancelJoin(self)) {
                        finishSlot(self);
                        break;
                    }
                    reaping = true;
                }
            },
        }
    }
}

fn finishSlotFromTask(self: *H1Conn) void {
    const prev = self.slot.completion_owner.cmpxchgStrong(live, reported, .acq_rel, .acquire);
    if (prev == null) {
        self.completion_ch.trySend(1) catch {};
    }
}

fn finishSlot(self: *H1Conn) void {
    if (!self.slot.in_use) return;
    if (self.join) |*h| {
        h.await(self.io);
        self.join = null;
    }
    if (self.slot.reaper_reserved) {
        if (self.config.accounting) |a| a.releaseReaper();
        self.slot.reaper_reserved = false;
    }
    self.slot.in_use = false;
    self.slot.completion_owner.store(reported, .release);
    if (self.config.accounting) |a| a.releaseStream();
}

fn cancelJoin(self: *H1Conn) bool {
    if (self.join) |handle| {
        const prev = self.slot.completion_owner.cmpxchgStrong(live, reaper_owned, .acq_rel, .acquire);
        if (prev == null) {
            self.join = null;
            if (self.config.reaper) |pool| {
                const queued = io_queue.tryPut(connection.ReaperJob, &pool.jobs, self.io, .{
                    .handle = handle,
                    .owner = &self.slot.completion_owner,
                    .completion = &self.completion_ch,
                    .stream_id = 1,
                });
                if (queued) return true;
            }
            var h = handle;
            h.cancel(self.io);
            self.slot.completion_owner.store(reported, .release);
            return false;
        }
    }
    return false;
}

fn shutdownHandlers(self: *H1Conn) !void {
    if (!self.slot.in_use) return;
    self.slot.terminal.setCause(.server_shutdown);
    if (!cancelJoin(self)) {
        finishSlot(self);
        return;
    }
    while (self.slot.in_use) {
        const sid = self.completion_ch.receive() catch return error.Canceled;
        _ = sid;
        finishSlot(self);
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

fn sendCb(ctx: *anyopaque, _: u31, status: u16, headers: []const request.Header, body: []const u8) response.ResponseError!void {
    const h: *Hctx = @ptrCast(@alignCast(ctx));
    const self = h.conn;
    if (self.committed) return error.ResponseCommitted;
    for (headers) |hdr| {
        if (!headerLineOk(hdr.name, hdr.value)) return error.InvalidHeader;
    }
    self.committed = true;
    self.resp_status = status;
    self.no_payload = suppressPayload(h.method, status);
    if (self.head.version == .http10) {
        self.framing = .close_delimited;
        self.want_close = true;
    } else {
        self.framing = .content_length;
    }
    const cl: usize = if (h.method == .HEAD) body.len else if (self.no_payload) 0 else body.len;
    try writeStatus(self, status);
    try writeCommonHeaders(self, headers, self.framing, cl);
    try writeRaw(self, "\r\n");
    if (!self.no_payload and body.len != 0) try writeRaw(self, body);
    try flushSink(self);
    self.finished = true;
}

fn startCb(ctx: *anyopaque, _: u31, status: u16, headers: []const request.Header, sse: bool) response.ResponseError!void {
    const h: *Hctx = @ptrCast(@alignCast(ctx));
    const self = h.conn;
    if (self.committed) return error.ResponseCommitted;
    for (headers) |hdr| {
        if (!headerLineOk(hdr.name, hdr.value)) return error.InvalidHeader;
    }
    self.committed = true;
    self.resp_status = status;
    self.no_payload = suppressPayload(h.method, status);
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
    const h: *Hctx = @ptrCast(@alignCast(ctx));
    const self = h.conn;
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
    const h: *Hctx = @ptrCast(@alignCast(ctx));
    const self = h.conn;
    if (test_channel_mutation == .m6_buffer_sse and self.occupies) return;
    try emitPending(self);
    try flushSink(self);
}

fn abortCb(ctx: *anyopaque, _: u31) response.ResponseError!void {
    const h: *Hctx = @ptrCast(@alignCast(ctx));
    h.conn.want_close = true;
}

fn waitUntilCb(ctx: *anyopaque, _: u31, deadline: std.Io.Timestamp) response.ResponseError!void {
    const h: *Hctx = @ptrCast(@alignCast(ctx));
    if (h.terminal.getCause()) |c| return response.causeToError(c);
    const timeout: std.Io.Timeout = .{ .deadline = .{ .raw = deadline, .clock = .awake } };
    timeout.sleep(h.conn.io) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
    };
    if (h.terminal.getCause()) |c| return response.causeToError(c);
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

fn writeRaw(self: *H1Conn, bytes: []const u8) response.ResponseError!void {
    if (self.tls != null) {
        if (self.offload_tls_io) {
            try writeRawTlsFromHandler(self, bytes);
        } else {
            try writeRawTlsOwner(self, bytes);
        }
        return;
    }
    self.writer.interface.writeAll(bytes) catch return error.WriteFailed;
}

fn writeRawTlsFromHandler(self: *H1Conn, bytes: []const u8) response.ResponseError!void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = @min(bytes.len - off, self.tls_out.len);
        @memcpy(self.tls_out[0..n], bytes[off..][0..n]);
        self.tls_req_ch.send(@intCast(n)) catch return error.WriteFailed;
        const ok = self.tls_ok_ch.receive() catch return error.WriteFailed;
        if (!ok) return error.WriteFailed;
        off += n;
    }
}

fn writeRawTlsOwner(self: *H1Conn, bytes: []const u8) response.ResponseError!void {
    const pump = self.tls_pump orelse return error.WriteFailed;
    var off: usize = 0;
    while (off < bytes.len) {
        const n = @min(bytes.len - off, self.tls_out.len);
        @memcpy(self.tls_out[0..n], bytes[off..][0..n]);
        const idx = io_queue.tryGet(u32, &self.tls_write_free, self.io) orelse return error.WriteFailed;
        const chunk = wire_pump.WireChunk{
            .bytes = self.tls_out[0..n],
            .len = n,
            .pool_index = idx,
        };
        if (!pump.writeChunks(chunk)) return error.WriteFailed;
        try driveTlsUntilIdle(self);
        off += n;
    }
}

fn flushSink(self: *H1Conn) response.ResponseError!void {
    if (self.tls != null) {
        if (self.offload_tls_io) return;
        try driveTlsUntilIdle(self);
        return;
    }
    self.writer.interface.flush() catch return error.WriteFailed;
}
