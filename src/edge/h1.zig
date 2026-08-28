//! HTTP/1.1 edge channel. One stream, no Session, no FairScheduler.
//!
//! The connection task owns the socket (h1c) or the TLS `Conn` (ALPN `http/1.1`
//! / no ALPN). It accumulates a head into a boot-reserved buffer, parses once,
//! reads a Content-Length body into a reserved buffer, dispatches into the
//! existing `Router` / `Response` surface, and writes one framing (Content-Length,
//! chunked, or close-delimited). Keep-alive loops until a close reason.
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

const live: u8 = 0;
const reaper_owned: u8 = 1;
const reported: u8 = 2;

pub const ChannelMutation = enum {
    none,
    m2_no_chunk_end,
    m3_ignore_close,
    m4_keep_after_400,
    m6_buffer_sse,
    m7_grow_head,
};

pub var test_channel_mutation: ChannelMutation = .none;

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
    request_held: usize = 0,
    socket_closed: bool = false,

    fn deinit(self: *H1Conn) void {
        std.debug.assert(!self.slot.in_use);
        std.debug.assert(self.request_held == 0);
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
        self.* = undefined;
    }
};

pub fn serve(
    stream: std.Io.net.Stream,
    config: connection.ConnConfig,
    tls: ?*tls_edge.Conn,
    leftover: []const u8,
) std.Io.Cancelable!void {
    var conn = initConn(stream, config, tls) catch {
        if (tls) |t| {
            t.deinit();
            config.gpa.destroy(t);
        }
        stream.close(config.io);
        return;
    };
    defer conn.deinit();
    if (tls) |t| t.bindIo(config.io);
    if (leftover.len != 0) {
        const n = @min(leftover.len, conn.carry_buf.len);
        @memcpy(conn.carry_buf[0..n], leftover[0..n]);
        conn.carry_len = n;
    }
    runLoop(&conn) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {},
    };
}

fn initConn(stream: std.Io.net.Stream, config: connection.ConnConfig, tls: ?*tls_edge.Conn) !H1Conn {
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
    const carry_buf = try gpa.alloc(u8, config.limits.h1_head_bytes);
    errdefer gpa.free(carry_buf);
    const pending_storage = try gpa.alloc(u8, config.limits.outbound_bytes_per_stream);
    errdefer gpa.free(pending_storage);

    var self: H1Conn = .{
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
    return self;
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

            if (head.expect_100) {
                try writeRaw(self, "HTTP/1.1 100 Continue\r\n\r\n");
                try flushSink(self);
            }

            const body = try readBody(self, head) orelse {
                try writeError(self, 413);
                return false;
            };
            if (body.len > self.config.limits.request_body_bytes) {
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

            const st = parser.validateAsH2Fields(
                head,
                self.scheme,
                self.field_scratch,
                self.name_scratch,
                self.path_q_buf,
            );
            if (st != 0) {
                releaseRequest(self);
                try writeError(self, st);
                return false;
            }

            try dispatch(self, head, body);
            releaseRequest(self);
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
                stashCarry(self, tmp[0..n], h.consumed);
                return parseFilled(self);
            },
            .http09 => return .http09,
            .reject => |rj| {
                stashCarry(self, tmp[0..n], rj.consumed);
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
            .head_bytes = self.config.limits.h1_head_bytes,
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
    const deadline_ns = nowNs(self.io) +% self.config.limits.request_body_idle_timeout_ns;
    while (got < need) {
        if (nowNs(self.io) >= deadline_ns) return error.IdleTimeout;
        var tmp: [4096]u8 = undefined;
        const n = try readSome(self, tmp[0..], deadline_ns) orelse return error.IdleTimeout;
        if (n == 0) return error.ConnectionClosed;
        const take = @min(n, need - got);
        @memcpy(self.body_buf[got .. got + take], tmp[0..take]);
        got += take;
        if (take < n) stashCarry(self, tmp[0..n], take);
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

fn stashCarry(self: *H1Conn, chunk: []const u8, consumed: usize) void {
    if (consumed >= chunk.len) {
        return;
    }
    const rest = chunk[consumed..];
    const n = @min(rest.len, self.carry_buf.len);
    @memcpy(self.carry_buf[0..n], rest[0..n]);
    self.carry_len = n;
}

fn readSome(self: *H1Conn, buf: []u8, deadline_ns: u64) !?usize {
    if (self.tls) |t| {
        const Read = union(enum) {
            data: anyerror!usize,
            timer: std.Io.Cancelable!void,
            shutdown: std.Io.Cancelable!void,
        };
        var result_buf: [3]Read = undefined;
        var select = std.Io.Select(Read).init(self.io, &result_buf);
        errdefer select.cancelDiscard();
        const ReadFn = struct {
            fn run(conn: *tls_edge.Conn, out: []u8) anyerror!usize {
                return conn.readPlain(out);
            }
        };
        try select.concurrent(.data, ReadFn.run, .{ t, buf });
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
                error.TlsReadFailed => @as(usize, 0),
                else => return err,
            },
            .timer => null,
            .shutdown => error.Canceled,
        };
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

fn dispatch(self: *H1Conn, head: parser.Head, body: []const u8) !void {
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
    self.want_close = head.connection_close;
    self.occupies = false;

    const req: request.Request = .{
        .method = method,
        .scheme = self.scheme,
        .authority = head.authority,
        .path = head.path,
        .path_remainder = switch (matched) {
            .found => |f| f.path_remainder,
            else => "",
        },
        .query = head.query,
        .headers = head.headers,
        .body = body,
        .trailers = &.{},
        .arena = self.arena.allocator(),
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
                    if (!self.job_resp.committed) writeError(self, 500) catch {};
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
            conn.job_handler.runFn(conn.job_handler.ptr, &conn.job_req, &conn.job_resp) catch {};
        }
    };
    const handle = self.io.concurrent(Job.run, .{self}) catch {
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

    while (self.slot.in_use) {
        if (shutdownRequested(self)) {
            self.slot.terminal.setCause(.server_shutdown);
            cancelJoin(self);
        }
        const Race = union(enum) {
            done: anyerror!u31,
            shutdown: std.Io.Cancelable!void,
        };
        var result_buf: [2]Race = undefined;
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
                cancelJoin(self);
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

fn cancelJoin(self: *H1Conn) void {
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
                if (!queued) {
                    var h = handle;
                    h.cancel(self.io);
                    self.slot.completion_owner.store(reported, .release);
                }
            } else {
                var h = handle;
                h.cancel(self.io);
                self.slot.completion_owner.store(reported, .release);
            }
        }
    }
}

fn shutdownHandlers(self: *H1Conn) !void {
    if (!self.slot.in_use) return;
    self.slot.terminal.setCause(.server_shutdown);
    cancelJoin(self);
    while (self.slot.in_use) {
        const sid = self.completion_ch.receive() catch return error.Canceled;
        _ = sid;
        finishSlot(self);
    }
}

fn sendCb(ctx: *anyopaque, _: u31, status: u16, headers: []const request.Header, body: []const u8) response.ResponseError!void {
    const h: *Hctx = @ptrCast(@alignCast(ctx));
    const self = h.conn;
    if (self.committed) return error.ResponseCommitted;
    self.committed = true;
    self.framing = if (self.head.version == .http10) .close_delimited else .content_length;
    if (self.head.version == .http10) self.want_close = true;
    try writeStatus(self, status);
    try writeCommonHeaders(self, headers, self.framing, body.len);
    try writeRaw(self, "\r\n");
    if (h.method != .HEAD and body.len != 0) try writeRaw(self, body);
    try flushSink(self);
}

fn startCb(ctx: *anyopaque, _: u31, status: u16, headers: []const request.Header, sse: bool) response.ResponseError!void {
    const h: *Hctx = @ptrCast(@alignCast(ctx));
    const self = h.conn;
    if (self.committed) return error.ResponseCommitted;
    self.committed = true;
    if (sse) self.occupies = true;
    if (self.head.version == .http10) {
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
    if (h.method == .HEAD) {
        if (end) try finishFraming(self);
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
    if (self.framing == .chunked) {
        if (test_channel_mutation != .m2_no_chunk_end) {
            try writeRaw(self, "0\r\n\r\n");
        }
    }
    try flushSink(self);
    if (self.framing == .close_delimited) self.want_close = true;
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
    if (self.tls) |t| {
        t.writePlain(bytes) catch return error.WriteFailed;
        return;
    }
    self.writer.interface.writeAll(bytes) catch return error.WriteFailed;
}

fn flushSink(self: *H1Conn) response.ResponseError!void {
    if (self.tls != null) return;
    self.writer.interface.flush() catch return error.WriteFailed;
}
