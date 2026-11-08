//! DST stage 3: one real h2c `Connection` inside the zio simulator.
//!
//! The connection is starh2's own (`connection.serveAccepted`: the real
//! actor, read and write pumps, ticket table and handlers) on one end of a
//! sim pipe (`zio.sim.pipePair`). The peer is two zio tasks in the same
//! simulation on the other end: a reader that checks every frame with
//! `tests/h2_oracle.zig` and decodes response headers, and a controller that
//! writes requests and reacts to frames. One seed decides the scenario and,
//! through the simulator, every task pick, I/O delay and timer order, so a
//! failing seed replays.
//!
//! A run has two phases (TigerBeetle VOPR shape). Safety: the scenario's
//! faults (peer resets, half-close, close, `requestShutdown`, short writes
//! and a write error at the server's pipe end, starved flow-control windows)
//! happen. Liveness: faults stop, cooperative handlers are released, the
//! peer opens every window, a graceful shutdown follows, and everything must
//! retire before a simulated deadline.
//!
//! Judged after the run settles, per stream (`judge`):
//! - clean run (no write error, peer reset, half-close, close, or shutdown
//!   during the safety phase): the handler's call returned ok and the peer
//!   read the handler's whole response with END_STREAM. Short writes and
//!   window starvation are not faults here: they are legal transport and
//!   flow-control behaviour and must not change an outcome.
//! - error with the handler's complete response at the peer: forbidden,
//!   except when an injected write error fell inside that stream's window
//!   (request sent .. handler returned): the h2c pump fails the whole batch
//!   that held it (`wire_pump.zig`; task t-3794 will remove this exception).
//! - every error is explained by a fault or a terminal cause (`explained`).
//! - "not returned" is never an error; a handler still not returned at the
//!   deadline is a stall.
//! Plus, after every run: the h2 oracle, framing and HPACK at the peer, the
//! connection's own `deinit` conservation checks, the slot, slab,
//! request-pool and admission counters back at zero, and the simulator's
//! deadlock detection.
//!
//!   zig-out/bin/dst-conn --seed N [--verbose]      one seed (replay)
//!   zig-out/bin/dst-conn --ci --seeds-file F --commit HEX --budget-ms B
//!   zig-out/bin/dst-conn-<mutant> --expect-mutant  validity
//!   --preempt-one-in N: one non-blocking channel operation in N preempts
//!   its task for a seeded 50 us .. 50 ms (`zio.sim.Preempt`); 0, the
//!   default, is off. With --seed, --survey and --expect-mutant it applies to
//!   every seed. With --ci the budget is split: the first half runs as
//!   without it (preemption off: the regression seeds, which were recorded
//!   off, the commit seeds and the range), the second half runs the range
//!   again from seed 1 at one in N. A replay needs the same N, so every
//!   replay line prints the N its run used.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("h2_client");
const Oracle = @import("h2_oracle").Oracle;
const harness_options = @import("harness_options");

const conn_mod = starh2.edge.connection;
const ticket_table = starh2.edge.ticket_table;
const hpack = starh2.core.hpack;
const frame = starh2.core.frame;
const CompleteResponse = starh2.http.response.CompleteResponse;
const ConnSlot = starh2.edge.conn_slot.ConnSlot;
const SlotPool = starh2.edge.conn_slot.SlotPool;

const max_streams = 8;
const max_frame_payload = 16 * 1024;
const body_small = 13;
const body_multi = 40_000;
/// Liveness bound, from the start of the liveness phase. Above the server's
/// graceful drain (30 s) plus its cancel grace (1 s) and every response
/// deadline, so a run whose faults explain a slow end still retires.
const liveness_deadline_ns: u64 = 120 * std.time.ns_per_s;
const tick: zio.Duration = .fromMilliseconds(1);

/// The panic handler prints the seed, the build and the replay command, so a
/// panic anywhere in the server or zio (a conservation check, a sim protocol
/// panic, a deadlock) names its run.
pub const panic = std.debug.FullPanic(panicFn);

var current_seed: ?u64 = null;

fn panicFn(msg: []const u8, first_trace_addr: ?usize) noreturn {
    if (current_seed) |s| {
        std.debug.print("DSTCONN FAIL seed={d} build={s} invariant=panic: {s}\nreplay: {s}\n", .{ s, buildName(), msg, replayCmd(s) });
    }
    std.debug.defaultPanic(msg, first_trace_addr);
}

fn buildName() []const u8 {
    return if (comptime std.mem.eql(u8, harness_options.mutant, "none")) "dst-conn" else "dst-conn-" ++ harness_options.mutant;
}

var replay_buf: [224]u8 = undefined;
fn replayCmd(seed: u64) []const u8 {
    return std.fmt.bufPrint(&replay_buf, "./zb build dst-conn-install && zig-out/bin/{s} --seed {d} --preempt-one-in {d} --verbose", .{ buildName(), seed, zio.sim.preemptSetting().one_in }) catch "?";
}

/// The preemption range (`zio.sim.Preempt`), log-uniform. Above 2 ms the
/// window must stay open past the harness's 1 ms tick for a parked handler
/// to wake and act in it (the t-3931 survey: 0 finds at 0 and 1 ms, finds
/// from 2 ms); 50 us .. 50 ms puts about half the preemptions above 2 ms
/// and keeps short ones, as an OS time slice does.
const preempt_min_ns: u64 = 50 * std.time.ns_per_us;
const preempt_max_ns: u64 = 50 * std.time.ns_per_ms;

fn setPreempt(one_in: u32) void {
    zio.sim.setPreempt(.{ .one_in = one_in, .min_ns = preempt_min_ns, .max_ns = preempt_max_ns });
}

// ---------------------------------------------------------------------------
// Scenario: what one seed asks for. Swarm style: the seed first picks which
// fault families are on, then their parameters.

const HandlerKind = enum { complete, task_send, task_stream };
const BodyKind = enum { empty, small, multi };
/// When the peer resets a stream, in the safety phase.
const RstPoint = enum { none, after_request, after_headers, after_data };
/// Per-stream window credit the peer grants per DATA frame, safety phase.
const WinPolicy = enum { full, one, small, none };

const StreamSpec = struct {
    kind: HandlerKind,
    body: BodyKind,
    /// Task handlers only: wait for the liveness phase before the last call.
    coop: bool,
    rst: RstPoint,
    win: WinPolicy,
};

const Scenario = struct {
    n: u8,
    streams: [max_streams]StreamSpec,
    initial_window: u32,
    /// Safety phase: no connection-level WINDOW_UPDATE.
    conn_starve: bool,
    half_close_after_frames: ?u32,
    close_after_frames: ?u32,
    shutdown_at_ns: ?u64,
    short_max: u32,
    fail_at_byte: ?u64,
    safety_ns: u64,

    fn generate(seed: u64) Scenario {
        var prng = std.Random.DefaultPrng.init(seed ^ 0x5eed_c0de_d575_0003);
        const r = prng.random();
        const resets = r.boolean();
        const windows = r.boolean();
        const half_close = r.uintLessThan(u8, 4) == 0;
        const close = r.uintLessThan(u8, 4) == 0;
        const shutdown = r.uintLessThan(u8, 3) == 0;
        const shorts = r.boolean();
        const write_error = r.uintLessThan(u8, 4) == 0;
        const coops = r.boolean();
        var sc: Scenario = .{
            .n = 1 + r.uintLessThan(u8, max_streams),
            .streams = undefined,
            .initial_window = 65_535,
            .conn_starve = windows and r.uintLessThan(u8, 3) == 0,
            .half_close_after_frames = if (half_close) 1 + r.uintLessThan(u32, 12) else null,
            .close_after_frames = if (close) 1 + r.uintLessThan(u32, 16) else null,
            .shutdown_at_ns = null,
            .short_max = if (shorts) 1 + r.uintLessThan(u32, 600) else 0,
            .fail_at_byte = if (write_error) 64 + r.uintLessThan(u64, 60_000) else null,
            .safety_ns = (20 + r.uintLessThan(u64, 180)) * std.time.ns_per_ms,
        };
        if (windows) {
            sc.initial_window = switch (r.uintLessThan(u8, 4)) {
                0 => 0,
                1 => 1,
                2 => 1 + r.uintLessThan(u32, 200),
                else => 65_535,
            };
        }
        if (shutdown) sc.shutdown_at_ns = r.uintLessThan(u64, sc.safety_ns);
        for (sc.streams[0..sc.n]) |*s| {
            s.* = .{
                .kind = r.enumValue(HandlerKind),
                .body = r.enumValue(BodyKind),
                .coop = false,
                .rst = if (resets and r.uintLessThan(u8, 3) == 0) switch (r.uintLessThan(u8, 3)) {
                    0 => .after_request,
                    1 => .after_headers,
                    else => .after_data,
                } else .none,
                .win = if (windows) r.enumValue(WinPolicy) else .full,
            };
            if (coops and s.kind != .complete) s.coop = r.boolean();
        }
        return sc;
    }
};

// ---------------------------------------------------------------------------
// One run's state. The simulation runs every task on one OS thread, so plain
// fields are exact.

const not_returned: u32 = 0;
const returned_ok: u32 = 1;
const err_base: u32 = 2;
const ErrorInt = @TypeOf(@intFromError(@as(anyerror, error.Unexpected)));

const Record = struct {
    started: bool = false,
    /// The handler's terminal call's result, stored at the instruction after
    /// it returns, before the handler can suspend again.
    word: u32 = not_returned,
    ret_ns: u64 = 0,

    fn name(self: Record) []const u8 {
        return switch (self.word) {
            not_returned => "not_returned",
            returned_ok => "ok",
            else => @errorName(self.err()),
        };
    }

    fn err(self: Record) anyerror {
        return @errorFromInt(@as(ErrorInt, @intCast(self.word - err_base)));
    }
};

const StreamObs = struct {
    req_sent_ns: ?u64 = null,
    rst_sent_ns: ?u64 = null,
    status: u16 = 0,
    body_len: usize = 0,
    body_ok: bool = true,
    end_stream: bool = false,
    server_rst: bool = false,
    got_headers: bool = false,
    hdr_block: std.ArrayList(u8) = .empty,
    /// Credit this stream has at the server as the peer granted it: initial
    /// window + WINDOW_UPDATEs - DATA received.
    window: i64 = 0,

    fn complete(self: StreamObs, want_len: usize) bool {
        return self.status == 200 and self.end_stream and !self.server_rst and self.body_ok and self.body_len == want_len;
    }

    fn settled(self: StreamObs) bool {
        return self.end_stream or self.server_rst or self.rst_sent_ns != null;
    }
};

const PeerEnd = enum { running, eof, reset, closed_by_peer, peer_error };

const FrameEv = struct { ftype: u8, flags: u8, sid: u31, len: u32, ping: [8]u8 };
const Event = union(enum) { frame: FrameEv, end };

pub const Marks = struct {
    ok_with_cancel: u64 = 0,
    cancel_while_waiting: u64 = 0,
    reset_raced_response: u64 = 0,
    teardown_with_handler_running: u64 = 0,
    /// Reported only (not a floor): see `ticket_table.Marks`.
    task_cancel_while_waiting: u64 = 0,

    fn add(self: *Marks, o: Marks) void {
        inline for (comptime std.meta.fieldNames(Marks)) |name| @field(self, name) += @field(o, name);
    }
};

pub const Faults = struct {
    short_writes: u64 = 0,
    write_errors: u64 = 0,
    peer_resets: u64 = 0,
    half_closes: u64 = 0,
    closes: u64 = 0,
    shutdowns: u64 = 0,
    starved_windows: u64 = 0,

    fn add(self: *Faults, o: Faults) void {
        inline for (comptime std.meta.fieldNames(Faults)) |name| @field(self, name) += @field(o, name);
    }
};

const Run = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    seed: u64,
    verbose: bool,
    sc: Scenario,
    records: [max_streams]Record = @splat(.{}),
    obs: [max_streams]StreamObs = @splat(.{}),
    release: zio.Event = .init,
    phase2: zio.Event = .init,
    liveness: bool = false,
    peer_fd: i32 = -1,
    server_fd: i32 = -1,
    peer_end: PeerEnd = .running,
    reader_done: bool = false,
    controller_done: bool = false,
    conn_done: bool = false,
    conn_done_ns: u64 = 0,
    shutdown_flag: std.atomic.Value(bool) = .init(false),
    shutdown_event: zio.Event = .init,
    shutdown_requested: bool = false,
    shutdown_in_safety: bool = false,
    half_closed: bool = false,
    closed: bool = false,
    frames_read: u32 = 0,
    writes_dead: bool = false,
    peer_error: ?[]const u8 = null,
    oracle: Oracle,
    decoder: hpack.Decoder,
    decoder_arena: std.heap.ArenaAllocator,
    events_buf: [1024]Event = undefined,
    events: zio.Channel(Event) = undefined,
    conn_window: i64 = 65_535,
    marks: Marks = .{},
    faults: Faults = .{},

    fn now() u64 {
        return zio.sim.clockNs();
    }

    fn streamIndex(self: *const Run, sid: u31) ?usize {
        if (sid == 0 or sid % 2 == 0) return null;
        const k: usize = (sid - 1) / 2;
        return if (k < self.sc.n) k else null;
    }

    fn body(self: *const Run, k: usize) []const u8 {
        return switch (self.sc.streams[k].body) {
            .empty => "",
            .small => body_pattern[k * 3 ..][0..body_small],
            .multi => body_pattern[k * 3 ..][0..body_multi],
        };
    }

    fn requestShutdown(self: *Run) void {
        if (self.shutdown_requested) return;
        self.shutdown_requested = true;
        if (!self.liveness) {
            self.shutdown_in_safety = true;
            self.faults.shutdowns += 1;
        }
        // What `Server.requestShutdown` does.
        self.shutdown_flag.store(true, .release);
        self.shutdown_event.set();
    }

    // -- peer: reading --------------------------------------------------

    fn endPeer(self: *Run, err: anyerror) void {
        if (self.closed) {
            self.peer_end = .closed_by_peer;
            self.oracle.waive("the peer closed its end on purpose");
            self.oracle.onEnd(.eof);
        } else if (err == error.EndOfStream) {
            self.peer_end = .eof;
            self.oracle.onEnd(.eof);
        } else {
            self.peer_end = .reset;
            if (zio.sim.faultStats(self.server_fd).write_errors > 0) {
                self.oracle.waive("an injected write error reset the pipe");
            }
            self.oracle.onEnd(.reset);
        }
    }

    fn peerError(self: *Run, what: []const u8) void {
        if (self.peer_error == null) self.peer_error = what;
        self.peer_end = .peer_error;
    }

    fn noteRst(self: *Run, k: usize) void {
        self.obs[k].rst_sent_ns = now();
        self.faults.peer_resets += 1;
        if (self.records[k].started and self.records[k].word == not_returned) self.marks.reset_raced_response += 1;
    }

    fn observeFrame(self: *Run, ftype: u8, flags: u8, sid: u31, payload: []const u8) void {
        if (ftype == 0) self.conn_window -= @intCast(payload.len);
        const k = self.streamIndex(sid) orelse return;
        const o = &self.obs[k];
        switch (ftype) {
            0 => {
                var data = payload;
                if (flags & 0x8 != 0) {
                    // PADDED: a pad length byte, the data, then the padding.
                    if (data.len == 0 or data[0] >= data.len) return self.peerError("bad DATA padding");
                    data = data[1 .. data.len - data[0]];
                }
                const want = self.body(k);
                if (o.body_len + data.len > want.len or !std.mem.eql(u8, want[o.body_len..][0..data.len], data)) o.body_ok = false;
                o.body_len += data.len;
                o.window -= @intCast(payload.len);
                if (flags & 1 != 0) o.end_stream = true;
            },
            1, 9 => {
                var block = payload;
                if (ftype == 1) {
                    if (flags & 0x8 != 0) {
                        if (block.len == 0 or block[0] >= block.len) return self.peerError("bad HEADERS padding");
                        block = block[1 .. block.len - block[0]];
                    }
                    if (flags & 0x20 != 0) {
                        if (block.len < 5) return self.peerError("short HEADERS priority");
                        block = block[5..];
                    }
                    if (flags & 1 != 0) o.end_stream = true;
                }
                o.hdr_block.appendSlice(self.gpa, block) catch @panic("oom");
                if (flags & 0x4 != 0) {
                    const res = self.decoder.decode(o.hdr_block.items, 64, 16 * 1024, 4096, 8192) catch {
                        return self.peerError("HPACK decode failed");
                    };
                    for (res.fields) |f| {
                        if (std.mem.eql(u8, f.name, ":status") and !o.got_headers) {
                            o.status = std.fmt.parseInt(u16, f.value, 10) catch 0;
                        }
                    }
                    o.got_headers = true;
                    o.hdr_block.clearRetainingCapacity();
                }
            },
            3 => o.server_rst = true,
            else => {},
        }
    }

    // -- peer: writing --------------------------------------------------

    fn flush(self: *Run, w: *std.Io.net.Stream.Writer, wire: *std.ArrayList(u8)) void {
        defer wire.clearRetainingCapacity();
        if (wire.items.len == 0 or self.writes_dead or self.closed or self.half_closed) return;
        self.oracle.onSend(wire.items);
        w.interface.writeAll(wire.items) catch {
            self.writes_dead = true;
            return;
        };
        w.interface.flush() catch {
            self.writes_dead = true;
        };
    }

    fn react(self: *Run, f: FrameEv, wire: *std.ArrayList(u8)) void {
        const gpa = self.gpa;
        switch (f.ftype) {
            4 => if (f.flags & 1 == 0) {
                wire.appendSlice(gpa, &[_]u8{ 0, 0, 0, 4, 1, 0, 0, 0, 0 }) catch @panic("oom");
            },
            6 => if (f.flags & 1 == 0) {
                wire.appendSlice(gpa, &[_]u8{ 0, 0, 8, 6, 1, 0, 0, 0, 0 }) catch @panic("oom");
                wire.appendSlice(gpa, &f.ping) catch @panic("oom");
            },
            0 => if (f.len > 0) {
                if (!self.liveness and self.sc.conn_starve) {
                    self.faults.starved_windows += 1;
                } else {
                    h2c.appendWindowUpdate(gpa, wire, 0, @intCast(f.len)) catch @panic("oom");
                    self.conn_window += f.len;
                }
                if (self.streamIndex(f.sid)) |k| {
                    const o = &self.obs[k];
                    if (!o.end_stream and o.rst_sent_ns == null and !o.server_rst) {
                        const incr: u32 = if (self.liveness) f.len else switch (self.sc.streams[k].win) {
                            .full => f.len,
                            .one => 1,
                            .small => @min(f.len, 7),
                            .none => 0,
                        };
                        if (incr < f.len) self.faults.starved_windows += 1;
                        if (incr > 0) {
                            h2c.appendWindowUpdate(gpa, wire, f.sid, @intCast(incr)) catch @panic("oom");
                            o.window += incr;
                        }
                    }
                }
            },
            else => {},
        }
        if (self.liveness) return;
        // The safety phase's scripted resets.
        if (self.streamIndex(f.sid)) |k| {
            const o = &self.obs[k];
            const point = self.sc.streams[k].rst;
            const fire = o.rst_sent_ns == null and !o.end_stream and !o.server_rst and
                ((point == .after_headers and f.ftype == 1) or (point == .after_data and f.ftype == 0));
            if (fire) {
                h2c.appendRst(gpa, wire, f.sid) catch @panic("oom");
                self.noteRst(k);
            }
        }
    }

    /// Liveness phase: every open stream, and the connection, gets credit up
    /// to a large window. Tracks what was granted so no window overflows.
    fn openWindows(self: *Run, wire: *std.ArrayList(u8)) void {
        const target: i64 = 1 << 24;
        if (self.conn_window < target) {
            h2c.appendWindowUpdate(self.gpa, wire, 0, @intCast(target - self.conn_window)) catch @panic("oom");
            self.conn_window = target;
        }
        for (self.obs[0..self.sc.n], 0..) |*o, k| {
            if (o.settled()) continue;
            if (o.window < target) {
                h2c.appendWindowUpdate(self.gpa, wire, @intCast(2 * k + 1), @intCast(target - o.window)) catch @panic("oom");
                o.window = target;
            }
        }
    }

    /// Safety phase: the scripted half-close and close, by frames read.
    fn scriptedEnds(self: *Run) void {
        if (self.liveness) return;
        if (self.sc.half_close_after_frames) |n| if (!self.half_closed and !self.closed and self.frames_read >= n) {
            self.half_closed = true;
            self.faults.half_closes += 1;
            simStream(self.peer_fd).shutdown(self.io, .send) catch {};
        };
        if (self.sc.close_after_frames) |n| if (!self.closed and self.frames_read >= n) {
            self.closed = true;
            self.faults.closes += 1;
            simStream(self.peer_fd).close(self.io);
        };
    }
};

var body_pattern: [body_multi + 3 * max_streams]u8 = blk: {
    @setEvalBranchQuota(200_000);
    var b: [body_multi + 3 * max_streams]u8 = undefined;
    for (&b, 0..) |*c, i| c.* = 'a' + @as(u8, @intCast(i % 26));
    break :blk b;
};

/// The run in progress; handlers reach it from a route.
var R: *Run = undefined;

// ---------------------------------------------------------------------------
// Handlers. Each records the result of its terminal call at return.

var dummy: u8 = 0;
const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/c/", .prefix = true, .handler = .{ .complete = .{ .ptr = &dummy, .runFn = completeHandler } } },
    .{ .method = .GET, .path = "/t/", .prefix = true, .handler = .{ .task = .{ .ptr = &dummy, .runFn = taskHandler } } },
};

fn pathIndex(rem: []const u8) usize {
    const k = std.fmt.parseInt(usize, rem, 10) catch std.debug.panic("dst-conn: bad path remainder {s}", .{rem});
    if (k >= max_streams) std.debug.panic("dst-conn: stream index {d}", .{k});
    return k;
}

fn record(k: usize, result: anyerror!void) anyerror!void {
    R.records[k].word = if (result) |_| returned_ok else |err| err_base + @as(u32, @intFromError(err));
    R.records[k].ret_ns = Run.now();
    return result;
}

fn completeHandler(_: *anyopaque, req: *const starh2.Request, resp: *CompleteResponse) anyerror!void {
    const k = pathIndex(req.path_remainder);
    R.records[k].started = true;
    return record(k, resp.send(200, &.{}, R.body(k)));
}

fn taskHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const k = pathIndex(req.path_remainder);
    R.records[k].started = true;
    return record(k, taskOps(k, resp));
}

fn taskOps(k: usize, resp: *starh2.Response) anyerror!void {
    const spec = R.sc.streams[k];
    const bytes = R.body(k);
    switch (spec.kind) {
        .complete => unreachable,
        .task_send => {
            if (spec.coop) try R.release.wait();
            try resp.send(200, &.{}, bytes);
        },
        .task_stream => {
            var b = try resp.start(200, &.{});
            const third = bytes.len / 3;
            try b.writeAll(bytes[0..third]);
            try b.flush();
            try b.writeAll(bytes[third .. 2 * third]);
            if (spec.coop) try R.release.wait();
            try b.writeAll(bytes[2 * third ..]);
            try b.finish();
        },
    }
}

// ---------------------------------------------------------------------------
// Tasks.

const Rig = struct {
    gpa: std.mem.Allocator,
    io: std.Io,
    slabs: starh2.edge.slab_pool.SlabPool,
    requests: starh2.edge.request_pool.RequestPool,
    accounting: conn_mod.GlobalAccounting,
    pool: SlotPool,
    limits: starh2.Limits,
    returned: ?SlotPool.Returned = null,

    fn config(self: *Rig) conn_mod.ConnConfig {
        return .{
            .io = self.io,
            // Below the accept loop, as `tests/slot_reuse.zig` serves: no
            // client record to charge, and no peer address on a sim pipe.
            .charge = .none,
            .peer = .unknown,
            .mode = .h2c,
            .limits = self.limits,
            .router = .{ .routes = &routes },
            .gpa = self.gpa,
            .accounting = &self.accounting,
            .slab_pool = &self.slabs,
            .request_pool = &self.requests,
            .shutdown_flag = &R.shutdown_flag,
            .shutdown_event = &R.shutdown_event,
        };
    }
};

fn connTask(rig: *Rig, slot: *ConnSlot) void {
    conn_mod.serveAccepted(simStream(R.server_fd), rig.config(), slot) catch {};
    rig.returned = rig.pool.recycle(slot);
    R.conn_done = true;
    R.conn_done_ns = Run.now();
    for (R.records[0..R.sc.n]) |rec| {
        if (rec.started and rec.word == not_returned) {
            R.marks.teardown_with_handler_running += 1;
            break;
        }
    }
}

fn simStream(fd: i32) std.Io.net.Stream {
    return .{ .socket = .{ .handle = fd, .address = .{ .ip4 = .loopback(0) } } };
}

fn peerReader(run: *Run) void {
    defer {
        run.reader_done = true;
        run.events.trySend(.end) catch {};
    }
    var rbuf: [4096]u8 = undefined;
    var reader = simStream(run.peer_fd).reader(run.io, &rbuf);
    var payload: [max_frame_payload]u8 = undefined;
    while (true) {
        var hdr: [9]u8 = undefined;
        reader.interface.readSliceAll(&hdr) catch |err| {
            if (run.verbose) std.debug.print("  peer read end @{d}ns: {s}\n", .{ Run.now(), @errorName(err) });
            return run.endPeer(err);
        };
        const len: u32 = (@as(u32, hdr[0]) << 16) | (@as(u32, hdr[1]) << 8) | hdr[2];
        const ftype = hdr[3];
        const flags = hdr[4];
        const sid: u31 = @intCast(std.mem.readInt(u32, hdr[5..9], .big) & 0x7fff_ffff);
        if (len > max_frame_payload) return run.peerError("a frame longer than SETTINGS_MAX_FRAME_SIZE (16384)");
        reader.interface.readSliceAll(payload[0..len]) catch |err| return run.endPeer(err);
        run.frames_read += 1;
        if (run.verbose) std.debug.print("  peer read @{d}ns: type={d} flags=0x{x} sid={d} len={d}\n", .{ Run.now(), ftype, flags, sid, len });
        run.oracle.onFrame(ftype, flags, sid, payload[0..len]);
        run.observeFrame(ftype, flags, sid, payload[0..len]);
        if (run.peer_error != null) return;
        var ping: [8]u8 = @splat(0);
        if (ftype == 6 and len == 8) ping = payload[0..8].*;
        run.events.send(.{ .frame = .{ .ftype = ftype, .flags = flags, .sid = sid, .len = len, .ping = ping } }) catch return;
    }
}

fn peerController(run: *Run) void {
    defer run.controller_done = true;
    var wbuf: [4096]u8 = undefined;
    var writer = simStream(run.peer_fd).writer(run.io, &wbuf);
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(run.gpa);

    // The preface, SETTINGS with the scenario's initial window, every request.
    wire.appendSlice(run.gpa, frame.CLIENT_PREFACE) catch @panic("oom");
    {
        var sbuf: [64]u8 = undefined;
        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = run.sc.initial_window }};
        const n = frame.Serializer.settingsFrame(&sbuf, false, &settings) catch unreachable;
        wire.appendSlice(run.gpa, sbuf[0..n]) catch @panic("oom");
    }
    for (0..run.sc.n) |k| {
        var path_buf: [16]u8 = undefined;
        const prefix = if (run.sc.streams[k].kind == .complete) "/c/" else "/t/";
        const path = std.fmt.bufPrint(&path_buf, "{s}{d}", .{ prefix, k }) catch unreachable;
        const sid: u31 = @intCast(2 * k + 1);
        h2c.appendHeaders(run.gpa, &wire, sid, path, true) catch @panic("oom");
        run.obs[k].window = run.sc.initial_window;
        run.obs[k].req_sent_ns = Run.now();
        if (run.sc.streams[k].rst == .after_request) {
            h2c.appendRst(run.gpa, &wire, sid) catch @panic("oom");
            run.noteRst(k);
        }
    }
    run.flush(&writer, &wire);

    var phase2_seen = false;
    while (true) {
        var ev: Event = undefined;
        if (!phase2_seen) {
            var recv = run.events.asyncReceive();
            const which = zio.select(.{ .ev = &recv, .phase2 = &run.phase2 }) catch return;
            switch (which) {
                .ev => |r| ev = r catch return,
                .phase2 => {
                    phase2_seen = true;
                    run.openWindows(&wire);
                    run.flush(&writer, &wire);
                    continue;
                },
            }
        } else {
            ev = run.events.receive() catch return;
        }
        switch (ev) {
            .end => return,
            .frame => |f| run.react(f, &wire),
        }
        run.flush(&writer, &wire);
        run.scriptedEnds();
        if (run.closed) return;
    }
}

// ---------------------------------------------------------------------------
// One run.

pub const Outcome = struct {
    seed: u64,
    failure: ?[]const u8 = null,
    /// The failure is one of the per-stream rules (what the ticket mutant
    /// must trip), as opposed to the frame oracle, framing or conservation.
    per_stream: bool = false,
    detail: [256]u8 = undefined,
    detail_len: usize = 0,
    trace: u64 = 0,
    state: u64 = 0,
    events: u64 = 0,
    clock: u64 = 0,
    marks: Marks = .{},
    faults: Faults = .{},
    clean: bool = false,

    fn fail(self: *Outcome, per_stream: bool, invariant: []const u8, comptime fmt: []const u8, args: anytype) void {
        if (self.failure != null) return;
        self.failure = invariant;
        self.per_stream = per_stream;
        const s = std.fmt.bufPrint(&self.detail, fmt, args) catch self.detail[0..0];
        self.detail_len = s.len;
    }

    fn detailText(self: *const Outcome) []const u8 {
        return self.detail[0..self.detail_len];
    }
};

fn runSeed(gpa: std.mem.Allocator, seed: u64, verbose: bool) !Outcome {
    current_seed = seed;
    defer current_seed = null;
    var out: Outcome = .{ .seed = seed };
    const sc = Scenario.generate(seed);

    zio.sim.begin(seed);
    defer zio.sim.end();
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(2),
        .enable_main_executor = true,
        .thread_pool = .{ .max_threads = 0 },
        .stack_pool = .{ .maximum_size = 1024 * 1024, .committed_size = 64 * 1024, .shrink_interval = .zero },
    });
    defer rt.deinit();
    const io = rt.io();

    var run: Run = .{
        .gpa = gpa,
        .io = io,
        .seed = seed,
        .verbose = verbose,
        .sc = sc,
        .oracle = Oracle.init(gpa, false),
        .decoder = undefined,
        .decoder_arena = std.heap.ArenaAllocator.init(gpa),
    };
    // `global` stays on so violations print; the harness judges `found`.
    defer run.oracle.deinit();
    defer run.decoder_arena.deinit();
    defer for (&run.obs) |*o| o.hdr_block.deinit(gpa);
    run.decoder = try hpack.Decoder.init(run.decoder_arena.allocator());
    run.events = zio.Channel(Event).init(&run.events_buf);
    R = &run;

    const limits = starh2.Limits.defaults;
    var rig: Rig = .{
        .gpa = gpa,
        .io = io,
        .slabs = try starh2.edge.slab_pool.SlabPool.init(gpa, io, limits.outbound_bytes_per_stream, 16),
        .requests = try starh2.edge.request_pool.RequestPool.init(limits.requestPoolConfig()),
        .accounting = .{
            .max_streams = limits.max_streams_per_server,
            .task_handler_capacity = limits.max_task_handlers,
            .max_outbound_bytes = limits.outbound_bytes_per_server,
            .max_handshakes = limits.concurrent_tls_handshakes,
            .routes = try starh2.edge.accounts.RouteTable.init(gpa, &routes, limits.routeLimits()),
        },
        .pool = try SlotPool.init(gpa, 1, try limits.slotBootBytes(.h2c)),
        .limits = limits,
    };
    defer rig.pool.deinit();
    defer rig.requests.deinit();
    defer rig.slabs.deinit(gpa);
    defer rig.accounting.routes.deinit(gpa);

    const fds = zio.sim.pipePair();
    run.server_fd = fds[0];
    run.peer_fd = fds[1];
    zio.sim.setSendFaults(run.server_fd, .{ .short_max = sc.short_max, .fail_at_byte = sc.fail_at_byte });
    // A window below the default starves the server from the start, before
    // any DATA could show it.
    if (sc.initial_window < 65_535) run.faults.starved_windows += 1;

    const marks_before = ticketMarks();
    const slot = try rig.pool.take();
    // The connection where the accept loop would place it; the peer on the
    // other executor, so the two meet only through the pipe and the picks.
    var conn_h = try rt.spawnInto(.{ .executor = 0 }, connTask, .{ &rig, slot });
    var reader_h = try rt.spawnInto(.{ .executor = 1 }, peerReader, .{&run});
    var ctrl_h = try rt.spawnInto(.{ .executor = 1 }, peerController, .{&run});

    // Safety phase.
    const t0 = Run.now();
    while (Run.now() - t0 < sc.safety_ns and !(run.conn_done and run.reader_done)) {
        if (sc.shutdown_at_ns) |at| if (Run.now() - t0 >= at) run.requestShutdown();
        try zio.sleep(tick);
    }

    // Liveness phase: faults stop, handlers are released, windows open.
    run.liveness = true;
    zio.sim.clearSendFaults(run.server_fd);
    run.release.set();
    run.phase2.set();
    const t1 = Run.now();
    const deadline = t1 + liveness_deadline_ns;
    while (Run.now() < deadline and !run.conn_done) {
        var all_settled = true;
        for (run.obs[0..sc.n]) |o| all_settled = all_settled and o.settled();
        if (all_settled or run.reader_done) run.requestShutdown();
        try zio.sleep(tick);
    }
    while (Run.now() < deadline and !(run.reader_done and run.controller_done and handlersDone(&run) and rig.accounting.orphaned_handlers.load(.acquire) == 0)) {
        try zio.sleep(tick);
    }
    if (!run.conn_done or !run.reader_done or !run.controller_done or !handlersDone(&run)) {
        var buf: [96]u8 = undefined;
        var w: std.Io.Writer = .fixed(&buf);
        for (run.records[0..sc.n], 0..) |rec, k| if (rec.started and rec.word == not_returned) w.print("{d} ", .{k}) catch {};
        out.fail(false, "liveness", "nothing retired by the deadline: conn_done={} reader_done={} controller_done={} handlers_not_returned=[{s}] peer_end={s}", .{ run.conn_done, run.reader_done, run.controller_done, w.buffered(), @tagName(run.peer_end) });
        printOutcome(&out, &run);
        std.process.exit(1);
    }
    conn_h.join();
    reader_h.join();
    ctrl_h.join();

    // Faults that fired.
    const fs = zio.sim.faultStats(run.server_fd);
    run.faults.short_writes += fs.short_writes;
    run.faults.write_errors += fs.write_errors;
    const marks_after = ticketMarks();
    run.marks.ok_with_cancel += marks_after.ok_with_cancel - marks_before.ok_with_cancel;
    run.marks.cancel_while_waiting += marks_after.cancel_while_waiting - marks_before.cancel_while_waiting;
    run.marks.task_cancel_while_waiting += marks_after.task_cancel_while_waiting - marks_before.task_cancel_while_waiting;

    judge(&run, &rig, fs, &out);

    out.trace = zio.sim.traceHash();
    out.state = zio.sim.stateDigest();
    out.events = zio.sim.eventCount();
    out.clock = zio.sim.clockNs();
    out.marks = run.marks;
    out.faults = run.faults;
    if (verbose or out.failure != null) printOutcome(&out, &run);
    return out;
}

fn handlersDone(run: *const Run) bool {
    for (run.records[0..run.sc.n]) |rec| if (rec.started and rec.word == not_returned) return false;
    return true;
}

fn ticketMarks() struct { ok_with_cancel: u64, cancel_while_waiting: u64, task_cancel_while_waiting: u64 } {
    return .{
        .ok_with_cancel = ticket_table.test_marks.ok_with_cancel.load(.monotonic),
        .cancel_while_waiting = ticket_table.test_marks.cancel_while_waiting.load(.monotonic),
        .task_cancel_while_waiting = ticket_table.test_marks.task_cancel_while_waiting.load(.monotonic),
    };
}

/// Whether a fault or terminal cause explains `err` on stream `k`, by the
/// server's own naming: a peer end of stream (half-close or close) makes
/// `Session.peerClosed` reset every open stream locally, and outside a
/// shutdown a local reset reaches the handler as `.peer_reset`
/// (`Connection.applyStreamResetIntent`), so PeerReset follows a peer EOF
/// or a server RST as well as the peer's own RST_STREAM.
fn explained(run: *const Run, k: usize, err: anyerror, send_failed: bool) bool {
    const o = run.obs[k];
    const spec = run.sc.streams[k];
    const peer_eof = run.half_closed or run.closed;
    const conn_cause = send_failed or peer_eof or run.shutdown_requested or
        (run.conn_done and run.conn_done_ns <= run.records[k].ret_ns) or run.peer_end != .running;
    return switch (err) {
        error.PeerReset => o.rst_sent_ns != null or peer_eof or o.server_rst,
        error.ConnectionClosed, error.Canceled, error.WriteFailed => conn_cause or o.rst_sent_ns != null,
        error.SlowConsumer => spec.win != .full or run.sc.conn_starve or run.sc.initial_window < 65_535 or run.half_closed or run.closed,
        else => false,
    };
}

fn judge(run: *Run, rig: *Rig, fs: zio.sim.FaultStats, out: *Outcome) void {
    const write_error = fs.write_errors > 0;
    const send_failed = fs.send_failures > 0;
    const clean = !write_error and run.faults.peer_resets == 0 and !run.half_closed and !run.closed and !run.shutdown_in_safety;
    out.clean = clean;

    if (run.peer_error) |e| out.fail(false, "peer-framing", "the peer could not read the server's bytes: {s}", .{e});
    if (run.oracle.found != 0) out.fail(false, "h2-oracle", "{d} frame-level violation(s), printed above as H2ORACLE", .{run.oracle.found});

    for (run.records[0..run.sc.n], run.obs[0..run.sc.n], 0..) |rec, o, k| {
        const want = run.body(k).len;
        const complete = o.complete(want);
        if (clean and !(rec.started and rec.word == returned_ok and complete)) {
            out.fail(true, "clean-stream", "stream {d} ({s}, {s} body): handler={s} peer: status={d} body={d}/{d} end_stream={} body_ok={} server_rst={}", .{ k, @tagName(run.sc.streams[k].kind), @tagName(run.sc.streams[k].body), rec.name(), o.status, o.body_len, want, o.end_stream, o.body_ok, o.server_rst });
        }
        if (rec.word >= err_base) {
            if (complete) {
                // The batch exception (t-3794): a server write failed (the
                // injected error, or a send into a pipe the peer closed or
                // reset) before the handler returned, so the h2c pump may have
                // failed the batch that held this complete response.
                const batch_failed = if (fs.first_send_failure_ns) |t| t <= rec.ret_ns else false;
                if (!batch_failed) out.fail(true, "error-with-complete-response", "stream {d}: handler returned {s} at {d} ns, but the peer read its complete response (status {d}, {d} bytes, END_STREAM) and no server write failed before that", .{ k, rec.name(), rec.ret_ns, o.status, o.body_len });
            }
            if (!explained(run, k, rec.err(), send_failed)) {
                out.fail(true, "unexplained-error", "stream {d}: handler returned {s}; no fault or terminal cause explains it (rst_sent={} half_closed={} closed={} shutdown={} write_error={} peer_end={s})", .{ k, rec.name(), o.rst_sent_ns != null, run.half_closed, run.closed, run.shutdown_requested, write_error, @tagName(run.peer_end) });
            }
        }
    }

    // Conservation, beyond the connection's own deinit checks.
    const a = &rig.accounting;
    const counters = [_]struct { []const u8, usize }{
        .{ "active_streams", a.active_streams.load(.acquire) },
        .{ "task_handlers", a.task_handlers.load(.acquire) },
        .{ "outbound_bytes", a.outbound_bytes.load(.acquire) },
        .{ "orphaned_handlers", a.orphaned_handlers.load(.acquire) },
        .{ "underflows", @intCast(a.underflows.load(.acquire)) },
        .{ "rented_slabs", rig.slabs.rentedCount() },
        .{ "request_pool_held_bytes", rig.requests.held_bytes.load(.acquire) },
    };
    for (counters) |c| if (c[1] != 0) out.fail(false, "conservation", "{s}={d} after the run", .{ c[0], c[1] });
    if (rig.returned) |r| {
        if (r != .free) out.fail(false, "conservation", "the slot came back {s}", .{@tagName(r)});
    } else out.fail(false, "conservation", "the slot never came back", .{});
}

fn printOutcome(out: *const Outcome, run: *const Run) void {
    const sc = run.sc;
    std.debug.print("SEED={d} build={s} TRACE_HASH={x:0>16} STATE_DIGEST={x:0>16} events={d} clock_ns={d} clean={}\n", .{ out.seed, buildName(), out.trace, out.state, out.events, out.clock, out.clean });
    std.debug.print("  scenario: n={d} initial_window={d} conn_starve={} half_close_after={?d} close_after={?d} shutdown_at_ns={?d} short_max={d} fail_at_byte={?d} safety_ns={d}\n", .{ sc.n, sc.initial_window, sc.conn_starve, sc.half_close_after_frames, sc.close_after_frames, sc.shutdown_at_ns, sc.short_max, sc.fail_at_byte, sc.safety_ns });
    for (run.records[0..sc.n], run.obs[0..sc.n], 0..) |rec, o, k| {
        const s = sc.streams[k];
        std.debug.print("  stream {d}: {s} {s} coop={} rst={s} win={s} | handler={s} ret_ns={d} | peer status={d} body={d} end={} rst_in={} rst_out={}\n", .{ k, @tagName(s.kind), @tagName(s.body), s.coop, @tagName(s.rst), @tagName(s.win), rec.name(), rec.ret_ns, o.status, o.body_len, o.end_stream, o.server_rst, o.rst_sent_ns != null });
    }
    std.debug.print("  peer_end={s} conn_done_ns={d} faults={any} marks={any}\n", .{ @tagName(run.peer_end), run.conn_done_ns, out.faults, out.marks });
    if (out.failure) |f| {
        std.debug.print("DSTCONN FAIL seed={d} build={s} invariant={s} TRACE_HASH={x:0>16}: {s}\nreplay: {s}\n", .{ out.seed, buildName(), f, out.trace, out.detailText(), replayCmd(out.seed) });
    }
}

// ---------------------------------------------------------------------------
// Modes.

fn printScope() void {
    std.debug.print("SCOPE simulated: one h2c Connection (real actor, read/write pumps, ticket table, complete and task handlers) on a zio sim pipe; peer as zio tasks (h2 oracle, HPACK); clock, task picks, I/O delays, timer order (zio sim); faults: peer RST_STREAM, half-close, close, requestShutdown, short writes, write error, flow-control windows 0/1/small/starved; with --preempt-one-in, a task preempted after a non-blocking channel operation\n", .{});
    std.debug.print("SCOPE unsimulated: real OS threads (one thread runs 2 logical executors; the connection's tasks share executor 0, placement .local), work stealing (sim cannot run work_stealing), TLS, HTTP/1.1, the accept loop and admission, getpeername (a real syscall that fails on a sim fd), more than one connection\n", .{});
}

fn readSeedsFile(gpa: std.mem.Allocator, io: std.Io, path: []const u8) ![]u64 {
    const text = try std.Io.Dir.cwd().readFileAlloc(io, path, gpa, .limited(1 << 20));
    defer gpa.free(text);
    var seeds: std.ArrayList(u64) = .empty;
    errdefer seeds.deinit(gpa);
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        const t = std.mem.trim(u8, line, " \t\r");
        if (t.len == 0 or t[0] == '#') continue;
        const word = t[0 .. std.mem.indexOfAny(u8, t, " \t#") orelse t.len];
        try seeds.append(gpa, try std.fmt.parseInt(u64, word, 10));
    }
    return seeds.toOwnedSlice(gpa);
}

const Sweep = struct {
    completed: u64 = 0,
    clean: u64 = 0,
    marks: Marks = .{},
    /// Runs in which each fault fired at least once.
    fault_runs: Faults = .{},

    fn add(self: *Sweep, o: Outcome) void {
        self.completed += 1;
        if (o.clean) self.clean += 1;
        self.marks.add(o.marks);
        inline for (comptime std.meta.fieldNames(Faults)) |name| {
            if (@field(o.faults, name) > 0) @field(self.fault_runs, name) += 1;
        }
    }
};

fn wallNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.now(.awake, io).nanoseconds);
}

fn ciMode(gpa: std.mem.Allocator, io: std.Io, seeds_file: []const u8, commit: []const u8, budget_ms: u64, preempt_one_in: u32) !void {
    printScope();
    std.debug.print("dst-conn: build={s} commit={s} budget_ms={d} preempt_one_in={d} (second half)\n", .{ buildName(), commit, budget_ms, preempt_one_in });
    setPreempt(0);
    const start = wallNs(io);
    const budget_ns = budget_ms * std.time.ns_per_ms;
    // With preemption the second half of the budget; without, all of it.
    const off_ns = if (preempt_one_in == 0) budget_ns else budget_ns / 2;
    var sweep: Sweep = .{};
    var det_checked: u64 = 0;

    const regression = try readSeedsFile(gpa, io, seeds_file);
    defer gpa.free(regression);
    std.debug.print("dst-conn: {d} regression seeds from {s}\n", .{ regression.len, seeds_file });
    for (regression) |s| try ciSeed(gpa, s, &sweep, &det_checked, true);

    var commit_seeds: u64 = 0;
    if (std.mem.eql(u8, commit, "none")) {
        std.debug.print("dst-conn: COMMIT_SEED none (not a git checkout): the commit-derived seeds are skipped\n", .{});
    } else {
        const base = std.hash.Wyhash.hash(0xd5c0_33, commit);
        std.debug.print("dst-conn: commit seeds from {s}: base {d}\n", .{ commit, base });
        while (commit_seeds < 64 and wallNs(io) - start < off_ns / 4) : (commit_seeds += 1) {
            try ciSeed(gpa, base +% commit_seeds, &sweep, &det_checked, false);
        }
    }
    var s: u64 = 1;
    while (wallNs(io) - start < off_ns) : (s += 1) try ciSeed(gpa, s, &sweep, &det_checked, false);
    const off_completed = sweep.completed;

    // The second half: the same range, every seed preempted at one in N.
    var p: u64 = 1;
    if (preempt_one_in != 0) {
        setPreempt(preempt_one_in);
        while (wallNs(io) - start < budget_ns) : (p += 1) try ciSeed(gpa, p, &sweep, &det_checked, false);
        setPreempt(0);
    }

    const wall_ms = (wallNs(io) - start) / std.time.ns_per_ms;
    std.debug.print("dst-conn: completed={d} (regression {d}, commit {d}, range 1..{d}; preempted at one in {d}: range 1..{d}, {d} seeds) clean={d} determinism_checked={d} wall_ms={d}\n", .{ sweep.completed, regression.len, commit_seeds, s - 1, preempt_one_in, p - 1, sweep.completed - off_completed, sweep.clean, det_checked, wall_ms });
    std.debug.print("dst-conn: marks {any}\n", .{sweep.marks});
    std.debug.print("dst-conn: runs with each fault {any}\n", .{sweep.fault_runs});

    // Floors: a sweep that ran too few seeds, or in which a configured fault
    // or a required mark never fired, did not test what it claims.
    const min_completed = 300;
    var failed = false;
    if (sweep.completed < min_completed) {
        std.debug.print("DSTCONN FAIL invariant=seed-floor: completed {d} < {d} in the budget\n", .{ sweep.completed, min_completed });
        failed = true;
    }
    if (sweep.clean == 0) {
        std.debug.print("DSTCONN FAIL invariant=fault-floor: no clean run\n", .{});
        failed = true;
    }
    inline for (comptime std.meta.fieldNames(Faults)) |name| if (@field(sweep.fault_runs, name) == 0) {
        std.debug.print("DSTCONN FAIL invariant=fault-floor: fault {s} never fired\n", .{name});
        failed = true;
    };
    inline for (comptime std.meta.fieldNames(Marks)) |name| if (comptime !std.mem.eql(u8, name, "task_cancel_while_waiting")) if (@field(sweep.marks, name) == 0) {
        std.debug.print("DSTCONN FAIL invariant=mark-floor: mark {s} never reached\n", .{name});
        failed = true;
    };
    if (failed) std.process.exit(1);
    std.debug.print("dst-conn: PASS\n", .{});
}

fn ciSeed(gpa: std.mem.Allocator, seed: u64, sweep: *Sweep, det_checked: *u64, always_twice: bool) !void {
    const a = try runSeed(gpa, seed, false);
    if (a.failure != null) std.process.exit(1);
    // Determinism: a sample of seeds runs twice and must hash the same.
    if (always_twice or seed % 16 == 0) {
        const b = try runSeed(gpa, seed, false);
        det_checked.* += 1;
        if (a.trace != b.trace or a.state != b.state) {
            std.debug.print("DSTCONN FAIL seed={d} build={s} invariant=determinism TRACE_HASH={x:0>16}/{x:0>16} STATE={x:0>16}/{x:0>16}\nreplay: {s}\n", .{ seed, buildName(), a.trace, b.trace, a.state, b.state, replayCmd(seed) });
            std.process.exit(1);
        }
    }
    sweep.add(a);
}

/// Validity: this build carries a mutant; some seed must fail a rule, and a
/// replay of that seed must fail the same way with the same trace.
fn mutantMode(gpa: std.mem.Allocator, io: std.Io) !void {
    const want_per_stream = comptime std.mem.eql(u8, harness_options.mutant, "ticket_cause_first");
    std.debug.print("== VALIDITY mutant {s} (needs a {s} failure) preempt_one_in={d}\n", .{ harness_options.mutant, if (want_per_stream) "per-stream rule" else "rule or oracle", zio.sim.preemptSetting().one_in });
    const start = wallNs(io);
    var seed: u64 = 1;
    while (seed <= 20_000 and wallNs(io) - start < 45 * std.time.ns_per_s) : (seed += 1) {
        const a = try runSeed(gpa, seed, false);
        const f = a.failure orelse continue;
        if (want_per_stream and !a.per_stream) {
            std.debug.print("MUTANT seed={d} failed {s}, not a per-stream rule; continuing\n", .{ seed, f });
            continue;
        }
        const b = try runSeed(gpa, seed, false);
        const same = b.failure != null and std.mem.eql(u8, b.failure.?, f) and a.trace == b.trace and
            std.mem.eql(u8, a.detailText(), b.detailText());
        if (!same) {
            std.debug.print("MUTANT_REPLAY_DIFFERS seed={d}: first {s} {x:0>16}, replay {?s} {x:0>16}\n", .{ seed, f, a.trace, b.failure, b.trace });
            std.process.exit(1);
        }
        std.debug.print("MUTANT_FOUND seed={d} invariant={s} TRACE_HASH={x:0>16} replay identical\n", .{ seed, f, a.trace });
        return;
    }
    std.debug.print("MUTANT_NOT_FOUND after {d} seeds (a clean sweep is a fail)\n", .{seed - 1});
    std.process.exit(1);
}

/// Triage: run seeds 1..n, print every failure, keep going. A panic still
/// ends it (and names its seed).
fn surveyMode(gpa: std.mem.Allocator, n: u64) !void {
    var sweep: Sweep = .{};
    var failed: u64 = 0;
    var seed: u64 = 1;
    while (seed <= n) : (seed += 1) {
        const o = try runSeed(gpa, seed, false);
        if (o.failure) |f| {
            failed += 1;
            std.debug.print("SURVEY seed={d} invariant={s}: {s}\n", .{ seed, f, o.detailText() });
        } else sweep.add(o);
    }
    std.debug.print("survey: seeds={d} failed={d} clean_passing={d}\nsurvey: marks {any}\nsurvey: runs with each fault {any}\n", .{ n, failed, sweep.clean, sweep.marks, sweep.fault_runs });
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args.deinit();
    _ = args.next();

    var mode: enum { none, seed, ci, mutant, survey } = .none;
    var survey_n: u64 = 0;
    var seed: u64 = 1;
    var verbose = false;
    var seeds_file: []const u8 = "tools/dst-conn/regression-seeds.txt";
    var commit: []const u8 = "none";
    var budget_ms: u64 = 45_000;
    var preempt_one_in: u32 = 0;
    while (args.next()) |a| {
        if (std.mem.eql(u8, a, "--seed")) {
            mode = .seed;
            seed = try std.fmt.parseInt(u64, args.next() orelse return error.MissingSeed, 10);
        } else if (std.mem.eql(u8, a, "--verbose")) {
            verbose = true;
        } else if (std.mem.eql(u8, a, "--ci")) {
            mode = .ci;
        } else if (std.mem.eql(u8, a, "--seeds-file")) {
            seeds_file = args.next() orelse return error.MissingPath;
        } else if (std.mem.eql(u8, a, "--commit")) {
            commit = args.next() orelse return error.MissingCommit;
        } else if (std.mem.eql(u8, a, "--budget-ms")) {
            budget_ms = try std.fmt.parseInt(u64, args.next() orelse return error.MissingBudget, 10);
        } else if (std.mem.eql(u8, a, "--survey")) {
            mode = .survey;
            survey_n = try std.fmt.parseInt(u64, args.next() orelse return error.MissingCount, 10);
        } else if (std.mem.eql(u8, a, "--expect-mutant")) {
            mode = .mutant;
        } else if (std.mem.eql(u8, a, "--preempt-one-in")) {
            preempt_one_in = try std.fmt.parseInt(u32, args.next() orelse return error.MissingRate, 10);
        } else {
            std.debug.print("unknown arg {s}\n", .{a});
            std.process.exit(2);
        }
    }
    setPreempt(preempt_one_in);
    switch (mode) {
        .seed => {
            printScope();
            const o = try runSeed(gpa, seed, verbose);
            if (o.failure != null) std.process.exit(1);
        },
        .ci => try ciMode(gpa, io, seeds_file, commit, budget_ms, preempt_one_in),
        .mutant => try mutantMode(gpa, io),
        .survey => try surveyMode(gpa, survey_n),
        .none => {
            std.debug.print("usage: --seed N [--verbose] | --ci --seeds-file F --commit HEX --budget-ms B | --expect-mutant\n", .{});
            std.process.exit(2);
        },
    }
}
