//! The HTTP/1.1 TLS transport witness (DST stage 1).
//!
//! What it proves: the HTTP/1.1 TLS transport cannot regain an unread
//! failure channel. The first version of t-3759 step 7 gave the HTTP/1.1
//! TLS pump a four-slot completion channel that nothing read, and
//! `Pump.post` panics when it is full: one resetting client crashed the
//! server. The fix wired `h1AckSink`. This witness builds the transport with
//! the production constructor (`h1.H1Tls`, the same calls `H1Conn` makes),
//! drives the real pump's completion handlers (`Pump.onCqComplete`) with
//! modelled kernel results, and checks the oracle after every step. It does
//! not reproduce the HTTPS crash end to end, and it says nothing about
//! HTTP/2, the scheduler, or real threads.
//!
//! # The model
//!
//! The pump's two submissions (`armSend`, `rearmRecv`) go to `Kernel`
//! through `Pump.test_io` (observe builds only), so no operation reaches a
//! kernel and the socket handle is -1. The driver plays the HTTP/1.1 actor
//! and the kernel, one action per step, single-threaded:
//!
//! - `Ws`/`Wb`: the actor writes one small chunk / one full chunk, as
//!   `writeRawTlsOwner` does (rent the one slab, `stampPush`, `writeChunks`).
//!   Legal only when the pump is idle (`tlsIdle`: nothing pending, no send
//!   armed) and the slab is free, which is when HTTP/1.1 writes. A full chunk
//!   is two records, and the second does not fit behind the first while it
//!   is in flight: the pump holds the chunk (one borrowed chunk). Also
//!   offered once after `.exit`, which HTTP/1.1 does not do: the pump must
//!   refuse it, give the chunk back, and submit nothing (`Pump.stopped`).
//! - `So`/`Sh`/`Se`: the armed send completes with all its bytes / half of
//!   them / `error.ConnectionResetByPeer`. After an ok, the actor drives a
//!   held chunk (`drivePending`), as `driveTlsUntilIdle` does.
//! - `Re`/`Rf`: the armed receive completes with
//!   `error.ConnectionResetByPeer` / 0 bytes (EOF).
//! - `Rx`: the armed receive delivers one record with a valid header and a
//!   junk payload. `readOne`'s decrypt error runs `failDrain` and posts the
//!   EOF but no shutdown, the one failure path without one (t-3807).
//!   tls.zig then refuses to encrypt (the connection is
//!   `fatal_alert_pending`).
//! - `Ra`: the armed receive delivers a fatal alert from the peer, correctly
//!   encrypted. The same `readOne` path runs, but tls.zig queues no alert of
//!   its own for a peer's alert, so the cipher stays open: only the pump can
//!   refuse a later write.
//! - `T`: teardown, last in every case: the kernel drops what is in flight
//!   (what `shutdownCq`'s cancel and drain do to it), then `H1Tls.close`,
//!   the same call `H1Conn.deinit` makes, which decides itself whether a
//!   running pump needs tearing down.
//!
//! After `.exit` no completion is driven (`h1.zig` `tlsPumpEnded`); only
//! writes and `T` are legal. Every case starts with `A` (`H1Tls.arm`, which
//! submits the first receive).
//!
//! # The oracle, after every step
//!
//! - `UnconsumedTlsAck`: a completion sits in `H1Tls.acks`, which nothing
//!   reads. Reported at the first one, not at overflow.
//! - `ChunkAccounting`: the slab pool's rented count differs from the chunks
//!   the pump holds (`pending_n` plus `carried`).
//! - `ArmedMismatch`: the pump's `send_armed`/`recv_armed` disagree with what
//!   the kernel holds; `DoubleSubmit`: an op submitted while in flight;
//!   `SubmitAfterExit`: an op submitted after the pump exited. A write
//!   after `.exit` must also be refused (`writeChunks` returns false,
//!   `WriteAcceptedAfterExit`) before anything is staged (staged bytes and
//!   `writes_done` do not grow, `StagedAfterExit`).
//! - `RepeatedPumpFailure`: more than one shutdown completion.
//! - At the end: `ChunkNotReturned` (the slab is not home after `finish`),
//!   `TeardownDidNoWork` (the sink applied nothing), `TerminalNotReached`
//!   (a `Se`/`Re` did not end the pump with one failure, or `Rf` did not
//!   post the EOF), `LeakedMemory`.
//! - Replays: every seeded walk runs twice and must give the same trace hash
//!   (`ReplayDiverged`).
//! - Floors, over the whole corpus: every fault kind and every coverage mark
//!   fired at least once, the case counts are at least the minimum, and the
//!   run fits the time limit (twice the 30 s target). A floor failure
//!   exits 4.
//!
//! # Running it
//!
//! `./zb build dst-transport` builds a control and a mutant (the shared
//! constructor without the sink) and runs `tools/dst/transport.sh`. By hand:
//!
//!     <binary>                    the corpus: every enumerated case, then
//!                                 seeds 0-63 and 16 commit-derived seeds,
//!                                 each twice
//!     <binary> --replay <case>    one case, with its full trace
//!
//! A case id is `enum:<choices>` (the index chosen among the legal actions
//! at each step, dot-separated; `enum:` with nothing after it is a case
//! that failed before its first choice) or `seed:<n>`. Exit codes: 0 pass,
//! 2 usage or no commit identity, 3 an invariant failed, 4 a floor failed,
//! and a panic aborts. A failure, a panic included, prints `INVARIANT=`,
//! `CASE=`, `SEED=`, `STEP=`, `BUILD=`, `TRACE=`, `TRACE_HASH=`, `DETAIL=`
//! and a `REPLAY:` line that is the command to run. A panic also prints
//! `PENDING=`, the action it happened in, which is the last token of its
//! trace (`<token>:panic`).
//!
//! The witness needs runtime safety: the slab pool's double-release
//! assert is part of its oracle. `build.zig` builds it in Debug whatever
//! `-Doptimize` says, and a build without safety does not compile.
const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const starh2 = @import("starh2");
const dst_options = @import("dst_options");

const h1 = starh2.edge.h1;
const tls_edge = starh2.edge.tls_edge;
const wire_pump = starh2.edge.wire_pump;
const Pump = tls_edge.Pump;

pub const panic = std.debug.FullPanic(dstPanic);

comptime {
    if (!std.debug.runtime_safety) @compileError("dst-transport needs runtime safety: the slab pool's double-release assert is part of its oracle");
}

/// Most actions before the forced `T` in an enumerated case.
const enum_depth = 9;
/// Most events in a seeded walk, `A` and `T` included.
const walk_events = 32;
const fixed_seeds = 64;
const commit_seeds = 16;
/// The enumerated corpus at `enum_depth`, measured; fewer cases means the
/// legality rules or the pump changed what is reachable. Re-measure (the
/// pass line prints the count) when either changes on purpose.
const min_enum_cases = 10255;
/// The step targets 30 s without compilation (the corpus ran in about 6 s
/// alone on an M-series Mac in Debug when this was set). The hard limit is
/// twice that, so a loaded `ci` machine does not fail on time alone, while
/// a run that slows down by a large factor still fails.
const target_ns: u64 = 30 * std.time.ns_per_s;
const budget_ns: u64 = 2 * target_ns;
const max_trace = 4096;

const small_write = 64;
const injected_error = error.ConnectionResetByPeer;

const exit_invariant: u8 = 3;
const exit_floor: u8 = 4;
const exit_usage: u8 = 2;

const Action = enum {
    write_small,
    write_big,
    send_ok,
    send_half,
    send_err,
    recv_err,
    recv_eof,
    recv_corrupt,
    recv_alert,
    teardown,

    fn token(a: Action) []const u8 {
        return switch (a) {
            .write_small => "Ws",
            .write_big => "Wb",
            .send_ok => "So",
            .send_half => "Sh",
            .send_err => "Se",
            .recv_err => "Re",
            .recv_eof => "Rf",
            .recv_corrupt => "Rx",
            .recv_alert => "Ra",
            .teardown => "T",
        };
    }
};

const Invariant = enum {
    UnconsumedTlsAck,
    ChunkAccounting,
    ArmedMismatch,
    DoubleSubmit,
    SubmitAfterExit,
    WriteAcceptedAfterExit,
    StagedAfterExit,
    RepeatedPumpFailure,
    ChunkNotReturned,
    TeardownDidNoWork,
    TerminalNotReached,
    ReplayDiverged,
    LeakedMemory,
};

/// Fault kinds the corpus must inject, and states it must reach. A zero
/// at the end fails the run: a fault that never fired tested nothing.
const Mark = enum {
    send_err,
    send_half,
    recv_err,
    recv_eof,
    recv_corrupt,
    corrupt_with_chunk_held,
    recv_alert,
    alert_with_chunk_held,
    fail_with_chunk_held,
    teardown_with_chunk_held,
    teardown_with_send_in_flight,
    teardown_after_exit,
    write_after_eof,
    write_after_exit,
};

const Counts = struct {
    marks: std.EnumArray(Mark, u64) = .initFill(0),
    enum_cases: u64 = 0,
    walk_runs: u64 = 0,
    steps: u64 = 0,
};

/// The kernel the pump submits to. It holds at most one receive and one
/// send, as the pump does.
const Kernel = struct {
    recv: ?*zio.ev.Completion = null,
    send: ?*zio.ev.Completion = null,
    send_len: usize = 0,
    closed: bool = false,
    double_submit: bool = false,
    /// Set by the driver once the pump has exited.
    after_exit: bool = false,
    submit_after_exit: bool = false,

    fn submit(ctx: *anyopaque, c: *zio.ev.Completion) error{Closed}!void {
        const k: *Kernel = @ptrCast(@alignCast(ctx));
        if (k.closed) return error.Closed;
        if (k.after_exit) k.submit_after_exit = true;
        switch (c.op) {
            .net_recv => {
                if (k.recv != null) k.double_submit = true;
                k.recv = c;
            },
            .net_send => {
                if (k.send != null) k.double_submit = true;
                k.send = c;
                var len: usize = 0;
                for (c.cast(zio.ev.NetSend).buffer.iovecs) |v| len += v.len;
                k.send_len = len;
            },
            else => std.debug.panic("dst: the pump submitted a {t}, which it never does", .{c.op}),
        }
    }
};

/// Where the next choice comes from: the enumerator, a seeded PRNG, or a
/// replayed choice list.
const Chooser = union(enum) {
    enumerate: *Enumerator,
    walk: std.Random,
    fixed: struct { choices: []const u8, pos: usize = 0 },
};

/// Exhaustigen-style enumeration (TigerBeetle `testing/exhaustigen.zig`):
/// each run replays the recorded choices and appends a 0 for every new
/// decision; `next` advances the last choice like an odometer. Every
/// choice sequence within the bounds runs exactly once.
const Enumerator = struct {
    vals: [enum_depth + 2]u8 = undefined,
    bounds: [enum_depth + 2]u8 = undefined,
    len: usize = 0,
    pos: usize = 0,
    started: bool = false,

    fn next(self: *Enumerator) bool {
        self.pos = 0;
        if (!self.started) {
            self.started = true;
            return true;
        }
        while (self.len > 0) {
            const i = self.len - 1;
            if (self.vals[i] + 1 < self.bounds[i]) {
                self.vals[i] += 1;
                return true;
            }
            self.len -= 1;
        }
        return false;
    }

    /// Null when the recorded run had a different number of legal actions
    /// at this point: the same prefix reached a different state.
    fn choose(self: *Enumerator, n: u8) ?u8 {
        if (self.pos < self.len) {
            if (self.bounds[self.pos] != n) return null;
            const v = self.vals[self.pos];
            self.pos += 1;
            return v;
        }
        self.vals[self.len] = 0;
        self.bounds[self.len] = n;
        self.len += 1;
        self.pos += 1;
        return 0;
    }
};

/// A failed check. It owns its detail: a failure outlives the case that
/// produced it (it is reported after `runCase` returned).
const Failure = struct {
    invariant: Invariant,
    step: usize,
    detail_buf: [256]u8 = undefined,
    detail_len: usize = 0,

    fn init(inv: Invariant, step: usize, comptime fmt: []const u8, args: anytype) Failure {
        var f: Failure = .{ .invariant = inv, .step = step };
        const d = std.fmt.bufPrint(&f.detail_buf, fmt, args) catch f.detail_buf[0..];
        f.detail_len = d.len;
        return f;
    }

    fn detail(f: *const Failure) []const u8 {
        return f.detail_buf[0..f.detail_len];
    }
};

const Globals = struct {
    io: std.Io,
    cipher: @FieldType(tls_edge.Conn, "cipher"),
    /// The peer's half of the same handshake, for records the peer sends.
    client_cipher: @FieldType(tls_edge.Conn, "cipher"),
    argv0: []const u8,
    commit: []const u8,
    counts: Counts = .{},
};

/// One case: a fresh transport, the kernel, the trace.
const Run = struct {
    g: *Globals,
    gpa: std.mem.Allocator,
    conn: *tls_edge.Conn,
    t: h1.H1Tls = .{},
    k: Kernel = .{},
    test_io: Pump.TestIo = undefined,
    steps: usize = 0,
    max_actions: usize,
    exited: bool = false,
    last: Action = .teardown,
    trace: [max_trace]u8 = undefined,
    trace_len: usize = 0,
    choices: [walk_events + 2]u8 = undefined,
    n_choices: usize = 0,
    /// The walk's seed; null for an enumerated or replayed choice list.
    seed: ?u64,
    /// The action being executed, recorded with its choice and its trace
    /// token before it runs, so a panic inside it can name it and replay.
    pending: ?Action = null,
    wrote_after_exit: bool = false,
    torn_down: bool = false,

    fn pump(self: *Run) *Pump {
        return self.t.pump.?;
    }

    fn note(self: *Run, comptime fmt: []const u8, args: anytype) void {
        const out = std.fmt.bufPrint(self.trace[self.trace_len..], fmt, args) catch {
            std.debug.panic("dst: trace over {d} bytes", .{max_trace});
        };
        self.trace_len += out.len;
    }

    fn traceText(self: *const Run) []const u8 {
        return self.trace[0..self.trace_len];
    }

    fn fail(self: *Run, inv: Invariant, comptime fmt: []const u8, args: anytype) Failure {
        return .init(inv, self.steps, fmt, args);
    }

    /// The action's token, before it runs (`<token>:`).
    fn begin(self: *Run, act: []const u8) void {
        const sep: []const u8 = if (self.trace_len == 0) "" else ">";
        self.note("{s}{s}:", .{ sep, act });
    }

    /// The step's outcome and the transport's state after it, appended to
    /// the trace after the action's token (so the trace hash covers what
    /// the pump did, not only what the driver asked).
    fn observe(self: *Run, outcome: []const u8) void {
        const p = self.pump();
        const applied: u32 = if (comptime @TypeOf(self.t.applied) == void) 0 else self.t.applied;
        self.note("{s}/p{d}s{d}r{d}a{d}f{d}u{d}", .{
            outcome,
            p.pending_n,
            if (self.k.send != null) self.k.send_len else 0,
            @intFromBool(self.k.recv != null),
            applied,
            self.t.failures,
            self.t.write_pool.rented.load(.acquire),
        });
    }

    fn check(self: *Run) ?Failure {
        const p = self.pump();
        if (!self.t.acks.isEmpty()) {
            const c = self.t.acks.tryReceive() catch unreachable;
            return self.fail(.UnconsumedTlsAck, "the pump posted a completion (fail_all={} shutdown={} ok={}) into the acks channel, which nothing reads", .{ c.fail_all, c.shutdown, c.ok });
        }
        if (self.k.submit_after_exit) return self.fail(.SubmitAfterExit, "the pump submitted an operation after it exited", .{});
        if (self.k.double_submit) return self.fail(.DoubleSubmit, "the pump submitted an operation that was still in flight", .{});
        if (!self.torn_down) {
            if (p.send_armed != (self.k.send != null) or p.recv_armed != (self.k.recv != null)) {
                return self.fail(.ArmedMismatch, "pump send_armed={} recv_armed={}, kernel holds send={} recv={}", .{ p.send_armed, p.recv_armed, self.k.send != null, self.k.recv != null });
            }
        }
        const held = p.pending_n + @intFromBool(p.carried != null);
        const rented = self.t.write_pool.rented.load(.acquire);
        if (rented != held) return self.fail(.ChunkAccounting, "{d} slab(s) rented, the pump holds {d} chunk(s)", .{ rented, held });
        if (self.t.failures > 1) return self.fail(.RepeatedPumpFailure, "{d} shutdown completions on one connection", .{self.t.failures});
        return null;
    }

    fn legal(self: *Run, out: *[8]Action) []const Action {
        var n: usize = 0;
        // After `.exit`, one write: it must be refused, and a second refused
        // write is the same case again.
        if (self.steps < self.max_actions and !(self.exited and self.wrote_after_exit)) {
            const p = self.pump();
            const idle = p.pending_n == 0 and !p.send_armed and self.t.write_pool.rented.load(.acquire) == 0;
            if (idle) {
                out[n] = .write_small;
                n += 1;
                out[n] = .write_big;
                n += 1;
            }
        }
        if (!self.exited and self.steps < self.max_actions) {
            if (self.k.send != null) {
                out[n] = .send_ok;
                n += 1;
                if (self.k.send_len >= 2) {
                    out[n] = .send_half;
                    n += 1;
                }
                out[n] = .send_err;
                n += 1;
            }
            if (self.k.recv != null) {
                out[n] = .recv_err;
                n += 1;
                out[n] = .recv_eof;
                n += 1;
                out[n] = .recv_corrupt;
                n += 1;
                out[n] = .recv_alert;
                n += 1;
            }
        }
        out[n] = .teardown;
        n += 1;
        return out[0..n];
    }

    fn pick(self: *Run, chooser: *Chooser, opts: []const Action) ?Action {
        const n: u8 = @intCast(opts.len);
        const i: u8 = switch (chooser.*) {
            .enumerate => |e| e.choose(n) orelse return null,
            .walk => |r| blk: {
                // Teardown rarely, so walks run long; it is forced at the end.
                if (n == 1 or r.uintLessThan(u8, 16) == 0) break :blk n - 1;
                break :blk r.uintLessThan(u8, n - 1);
            },
            .fixed => |*f| blk: {
                if (f.pos >= f.choices.len) return null;
                const v = f.choices[f.pos];
                f.pos += 1;
                if (v >= n) return null;
                break :blk v;
            },
        };
        self.choices[self.n_choices] = i;
        self.n_choices += 1;
        return opts[i];
    }

    fn write(self: *Run, len: usize) []const u8 {
        const p = self.pump();
        const idx = self.t.write_pool.tryRentIndex(self.g.io) orelse std.debug.panic("dst: a write was legal with the slab rented", .{});
        @memset(self.t.out[0..len], 'x');
        var chunk = wire_pump.WireChunk{ .bytes = self.t.out[0..len], .len = len, .pool_index = idx };
        p.stampPush(&chunk);
        if (!p.writeChunks(chunk)) {
            self.exited = true;
            return "exit";
        }
        return "ok";
    }

    fn completeSend(self: *Run, how: Action) []const u8 {
        const c = self.k.send orelse unreachable;
        const len = self.k.send_len;
        self.k.send = null;
        self.k.send_len = 0;
        c.reset();
        switch (how) {
            .send_ok => c.setResult(.net_send, len),
            .send_half => c.setResult(.net_send, len / 2),
            .send_err => c.setError(injected_error),
            else => unreachable,
        }
        return self.afterCompletion(c);
    }

    fn completeRecv(self: *Run, how: Action) []const u8 {
        const c = self.k.recv orelse unreachable;
        self.k.recv = null;
        c.reset();
        switch (how) {
            .recv_err => c.setError(injected_error),
            .recv_eof => c.setResult(.net_recv, 0),
            .recv_corrupt => {
                // The kernel delivers one application-data record with a
                // valid header and a payload that is not ciphertext under
                // this connection's keys: `readOne`'s decrypt error.
                var junk: [5 + 64]u8 = undefined;
                junk[0..5].* = .{ 0x17, 0x03, 0x03, 0x00, 64 };
                @memset(junk[5..], 0xa5);
                const iov = c.cast(zio.ev.NetRecv).buffers.iovecs[0];
                if (iov.len < junk.len) std.debug.panic("dst: receive buffer of {d} bytes", .{iov.len});
                @memcpy(iov.base[0..junk.len], &junk);
                c.setResult(.net_recv, junk.len);
            },
            .recv_alert => {
                // The peer sends a fatal alert, correctly encrypted: the
                // first record this case receives, so it is sealed with the
                // peer's first sequence number. tls.zig returns the alert as
                // an error and, since it came from the peer, queues nothing,
                // so the cipher stays open for writing.
                var peer = self.g.client_cipher;
                peer.inner.pending_alert = .handshake_failure;
                peer.inner.state = .fatal_alert_pending;
                var buf: [64]u8 = undefined;
                const rec = peer.close(&buf) catch |err| std.debug.panic("dst: encoding the peer's alert: {t}", .{err});
                const iov = c.cast(zio.ev.NetRecv).buffers.iovecs[0];
                if (iov.len < rec.len) std.debug.panic("dst: receive buffer of {d} bytes", .{iov.len});
                @memcpy(iov.base[0..rec.len], rec);
                c.setResult(.net_recv, rec.len);
            },
            else => unreachable,
        }
        return self.afterCompletion(c);
    }

    /// `driveTlsUntilIdle`'s turn: one completion, then a held chunk.
    fn afterCompletion(self: *Run, c: *zio.ev.Completion) []const u8 {
        const p = self.pump();
        if (p.onCqComplete(c) == .exit) {
            self.exited = true;
            return "exit";
        }
        if (p.pending_n > 0) {
            if (!p.drivePending()) {
                self.exited = true;
                return "exit";
            }
        }
        return "ok";
    }

    fn mark(self: *Run, m: Mark) void {
        self.g.counts.marks.getPtr(m).* += 1;
    }

    fn step(self: *Run, a: Action) ?Failure {
        const p = self.pump();
        const held_before = p.pending_n > 0;
        const failures_before = self.t.failures;
        const eof_before = p.inbound_eof;
        const exited_before = self.exited;
        const staged_before = p.send_fill - p.send_head;
        const written_before = p.writes_done;
        self.pending = a;
        self.begin(a.token());
        const outcome: []const u8 = switch (a) {
            .write_small => self.write(small_write),
            .write_big => self.write(self.t.out.len),
            .send_ok, .send_half, .send_err => self.completeSend(a),
            .recv_err, .recv_eof, .recv_corrupt, .recv_alert => self.completeRecv(a),
            .teardown => blk: {
                if (held_before) self.mark(.teardown_with_chunk_held);
                if (self.k.send != null) self.mark(.teardown_with_send_in_flight);
                if (self.exited) self.mark(.teardown_after_exit);
                self.k.closed = true;
                self.k.send = null;
                self.k.recv = null;
                self.t.close();
                self.torn_down = true;
                break :blk "fin";
            },
        };
        self.steps += 1;
        self.last = a;
        self.g.counts.steps += 1;
        switch (a) {
            .send_err => self.mark(.send_err),
            .send_half => self.mark(.send_half),
            .recv_err => self.mark(.recv_err),
            .recv_eof => self.mark(.recv_eof),
            .recv_corrupt => {
                self.mark(.recv_corrupt);
                if (held_before) self.mark(.corrupt_with_chunk_held);
            },
            .recv_alert => {
                self.mark(.recv_alert);
                if (held_before) self.mark(.alert_with_chunk_held);
            },
            .write_small, .write_big => {
                if (eof_before) self.mark(.write_after_eof);
                if (exited_before) {
                    self.mark(.write_after_exit);
                    self.wrote_after_exit = true;
                }
            },
            else => {},
        }
        if ((a == .send_err or a == .recv_err) and held_before) self.mark(.fail_with_chunk_held);
        if (self.exited) self.k.after_exit = true;
        self.pending = null;
        self.observe(outcome);
        if (self.check()) |f| return f;
        if (exited_before and (a == .write_small or a == .write_big)) {
            const staged = p.send_fill - p.send_head;
            if (staged > staged_before or p.writes_done != written_before) {
                return self.fail(.StagedAfterExit, "a write after the pump exited was staged: {d} -> {d} staged bytes, writes_done {d} -> {d}", .{ staged_before, staged, written_before, p.writes_done });
            }
            if (!std.mem.eql(u8, outcome, "exit")) {
                return self.fail(.WriteAcceptedAfterExit, "writeChunks accepted a write after the pump exited (outcome {s})", .{outcome});
            }
        }
        switch (a) {
            .send_err, .recv_err => {
                if (!self.exited or self.t.failures != failures_before + 1) {
                    return self.fail(.TerminalNotReached, "{s} did not end the pump with one failure (exited={} failures {d} -> {d})", .{ a.token(), self.exited, failures_before, self.t.failures });
                }
            },
            .recv_eof => if (!p.inbound_eof) return self.fail(.TerminalNotReached, "Rf did not post the EOF", .{}),
            // The decrypt error ends the pump with the EOF and no shutdown
            // post (`readOne`): the one failure path that posts none.
            .recv_corrupt, .recv_alert => if (!self.exited or !p.inbound_eof or self.t.failures != failures_before) {
                return self.fail(.TerminalNotReached, "{s} did not end the pump with the EOF and no shutdown post (exited={} eof={} failures {d} -> {d})", .{ a.token(), self.exited, p.inbound_eof, failures_before, self.t.failures });
            },
            else => {},
        }
        return null;
    }

    fn finalCheck(self: *Run) ?Failure {
        const p = self.pump();
        if (self.t.write_pool.rented.load(.acquire) != 0 or p.pending_n != 0 or p.carried != null) {
            return self.fail(.ChunkNotReturned, "after finish: {d} slab(s) rented, pending_n={d}, carried={}", .{ self.t.write_pool.rented.load(.acquire), p.pending_n, p.carried != null });
        }
        const applied: u32 = if (comptime @TypeOf(self.t.applied) == void) 0 else self.t.applied;
        if (applied == 0) return self.fail(.TeardownDidNoWork, "the ack sink applied nothing, not even the teardown's fail_all", .{});
        return null;
    }
};

const Outcome = struct {
    failure: ?Failure,
    hash: u64,
    trace: [max_trace]u8,
    trace_len: usize,
    choices: [walk_events + 2]u8,
    n_choices: usize,
};

/// Run one case from a fresh transport. The trace and its hash come back
/// with the verdict.
fn runCase(g: *Globals, chooser: *Chooser, max_actions: usize, seed: ?u64) !Outcome {
    var dbg: std.heap.DebugAllocator(.{}) = .init;
    const gpa = dbg.allocator();
    const conn = try gpa.create(tls_edge.Conn);
    conn.* = .{};
    conn.state = .open;
    conn.cipher = g.cipher;
    var run: Run = .{ .g = g, .gpa = gpa, .conn = conn, .max_actions = max_actions, .seed = seed };
    current = &run;
    defer current = null;
    run.test_io = .{ .ctx = &run.k, .submit = Kernel.submit };

    const no_socket: zio.ev.Backend.NetHandle = -1;
    try run.t.init(gpa, g.io, conn, no_socket);
    run.pump().test_io = &run.test_io;
    run.begin("A");
    run.t.arm() catch std.debug.panic("dst: Pump.start refused with no CQ involved", .{});
    run.observe("ok");

    var failure: ?Failure = run.check();
    var opts_buf: [8]Action = undefined;
    while (failure == null and !run.torn_down) {
        const opts = run.legal(&opts_buf);
        const a = run.pick(chooser, opts) orelse {
            if (chooser.* != .enumerate) return error.BadReplayCase;
            failure = run.fail(.ReplayDiverged, "the legal actions differed from the recorded run's at the same prefix", .{});
            break;
        };
        failure = run.step(a);
    }
    if (failure == null) failure = run.finalCheck();
    // A failed case is reported and the process exits: its transport is
    // left as it failed, because `deinit` asserts on a chunk not home and
    // would replace the named invariant with a panic.
    if (failure == null) {
        run.t.deinit(gpa);
        gpa.destroy(conn);
        if (dbg.deinit() == .leak) failure = run.fail(.LeakedMemory, "the case leaked memory", .{});
    }
    var out: Outcome = .{
        .failure = failure,
        .hash = std.hash.Wyhash.hash(0, run.traceText()),
        .trace = undefined,
        .trace_len = run.trace_len,
        .choices = undefined,
        .n_choices = run.n_choices,
    };
    @memcpy(out.trace[0..run.trace_len], run.traceText());
    @memcpy(out.choices[0..run.n_choices], run.choices[0..run.n_choices]);
    return out;
}

/// The case being run, for the panic handler.
var current: ?*Run = null;

/// A panic inside a case is a failure like the others: it prints the same
/// lines, so it can be replayed. Every choice, including the one for the
/// action that panicked, was recorded before that action ran (`pick`,
/// `step`), so the case id replays up to the panic.
fn dstPanic(msg: []const u8, first_trace_addr: ?usize) noreturn {
    if (current) |r| {
        // The trace ends with the pending action's `<token>:`, or with the
        // state after the last step when nothing was running.
        const tail: []const u8 = if (r.pending != null) "panic" else ">panic";
        const trace = r.traceText();
        var h = std.hash.Wyhash.init(0);
        h.update(trace);
        h.update(tail);
        var id_buf: [128]u8 = undefined;
        const case = caseId(&id_buf, r.seed, r.choices[0..r.n_choices]);
        std.debug.print("DST-TRANSPORT FAIL\nINVARIANT=Panic\nCASE={s}\n", .{case});
        if (r.seed) |sd| std.debug.print("SEED={d}\n", .{sd}) else std.debug.print("SEED=none\n", .{});
        std.debug.print("STEP={d}\nPENDING={s}\n", .{ r.steps, if (r.pending) |a| a.token() else "none" });
        buildLine(r.g);
        std.debug.print("TRACE={s}{s}\nTRACE_HASH=0x{x:0>16}\nDETAIL={s}\nREPLAY: {s} --replay {s}\n", .{ trace, tail, h.final(), msg, r.g.argv0, case });
    }
    std.debug.defaultPanic(msg, first_trace_addr);
}

fn buildLine(g: *const Globals) void {
    std.debug.print("BUILD=commit={s} mutant={s} zig={s} mode={t} target={t}-{t}\n", .{
        g.commit,
        if (h1.mutant_no_ack_sink) "no-h1-ack-sink" else "none",
        builtin.zig_version_string,
        builtin.mode,
        builtin.cpu.arch,
        builtin.os.tag,
    });
}

/// `seed:<n>` for a walk, else `enum:` and the choices made, which may be
/// none.
fn caseId(buf: []u8, seed: ?u64, choices: []const u8) []const u8 {
    if (seed) |sd| return std.fmt.bufPrint(buf, "seed:{d}", .{sd}) catch unreachable;
    var w: usize = (std.fmt.bufPrint(buf, "enum:", .{}) catch unreachable).len;
    for (choices, 0..) |c, i| {
        const s = std.fmt.bufPrint(buf[w..], "{s}{d}", .{ if (i == 0) "" else ".", c }) catch unreachable;
        w += s.len;
    }
    return buf[0..w];
}

fn report(g: *const Globals, f: Failure, case: []const u8, seed: ?u64, out: *const Outcome) noreturn {
    std.debug.print("DST-TRANSPORT FAIL\nINVARIANT={t}\nCASE={s}\n", .{ f.invariant, case });
    if (seed) |s| std.debug.print("SEED={d}\n", .{s}) else std.debug.print("SEED=none\n", .{});
    std.debug.print("STEP={d}\n", .{f.step});
    buildLine(g);
    std.debug.print("TRACE={s}\nTRACE_HASH=0x{x:0>16}\nDETAIL={s}\nREPLAY: {s} --replay {s}\n", .{ out.trace[0..out.trace_len], out.hash, f.detail(), g.argv0, case });
    std.process.exit(exit_invariant);
}

/// Seeds from the commit hash alone, so a build with local edits walks
/// the same seeds as its commit.
fn walkSeed(commit: []const u8, i: u64) u64 {
    return std.hash.Wyhash.hash(i, commit[0..40]);
}

fn runWalk(g: *Globals, seed: u64) !Outcome {
    var prng = std.Random.DefaultPrng.init(seed);
    var chooser: Chooser = .{ .walk = prng.random() };
    return runCase(g, &chooser, walk_events - 2, seed);
}

fn corpus(g: *Globals) !void {
    const start = nowNs(g.io);
    var id_buf: [128]u8 = undefined;

    var e: Enumerator = .{};
    while (e.next()) {
        var chooser: Chooser = .{ .enumerate = &e };
        const out = try runCase(g, &chooser, enum_depth, null);
        if (out.failure) |f| report(g, f, caseId(&id_buf, null, out.choices[0..out.n_choices]), null, &out);
        g.counts.enum_cases += 1;
    }

    var seeds: [fixed_seeds + commit_seeds]u64 = undefined;
    for (0..fixed_seeds) |i| seeds[i] = i;
    for (0..commit_seeds) |i| seeds[fixed_seeds + i] = walkSeed(g.commit, i);
    for (seeds) |seed| {
        const a = try runWalk(g, seed);
        const id = caseId(&id_buf, seed, &.{});
        if (a.failure) |f| report(g, f, id, seed, &a);
        const b = try runWalk(g, seed);
        if (b.failure) |f| report(g, f, id, seed, &b);
        if (a.hash != b.hash) {
            report(g, .init(.ReplayDiverged, 0, "the same seed gave two different traces", .{}), id, seed, &b);
        }
        g.counts.walk_runs += 2;
    }

    const elapsed = nowNs(g.io) - start;
    var floor_failed = false;
    std.debug.print("dst-transport: faults and marks:", .{});
    var it = g.counts.marks.iterator();
    while (it.next()) |entry| {
        std.debug.print(" {t}={d}", .{ entry.key, entry.value.* });
        if (entry.value.* == 0) floor_failed = true;
    }
    std.debug.print("\n", .{});
    var it2 = g.counts.marks.iterator();
    while (it2.next()) |entry| {
        if (entry.value.* == 0) std.debug.print("FLOOR=FaultNeverFired {t}\n", .{entry.key});
    }
    if (g.counts.enum_cases < min_enum_cases) {
        std.debug.print("FLOOR=UnderCorpusMinimum enumerated {d} cases, the minimum is {d}\n", .{ g.counts.enum_cases, min_enum_cases });
        floor_failed = true;
    }
    if (g.counts.walk_runs != 2 * seeds.len) {
        std.debug.print("FLOOR=UnderCorpusMinimum {d} walk runs, want {d}\n", .{ g.counts.walk_runs, 2 * seeds.len });
        floor_failed = true;
    }
    if (elapsed > budget_ns) {
        std.debug.print("FLOOR=OverBudget {d} ms, the budget is {d} ms\n", .{ elapsed / std.time.ns_per_ms, budget_ns / std.time.ns_per_ms });
        floor_failed = true;
    }
    if (floor_failed) {
        buildLine(g);
        std.debug.print("DST-TRANSPORT FAIL (floor)\n", .{});
        std.process.exit(exit_floor);
    }
    buildLine(g);
    std.debug.print("DST-TRANSPORT PASS completed={d} (enumerated {d} at depth {d}, {d} walk runs of {d} seeds) steps={d} elapsed_ms={d} target_ms={d}\n", .{
        g.counts.enum_cases + g.counts.walk_runs,
        g.counts.enum_cases,
        enum_depth,
        g.counts.walk_runs,
        seeds.len,
        g.counts.steps,
        elapsed / std.time.ns_per_ms,
        target_ns / std.time.ns_per_ms,
    });
}

fn replay(g: *Globals, case: []const u8) !void {
    var id_buf: [128]u8 = undefined;
    var out: Outcome = undefined;
    var seed: ?u64 = null;
    if (std.mem.startsWith(u8, case, "seed:")) {
        const s = std.fmt.parseInt(u64, case["seed:".len..], 10) catch return error.BadReplayCase;
        seed = s;
        out = try runWalk(g, s);
    } else if (std.mem.startsWith(u8, case, "enum:")) {
        var choices: [walk_events + 2]u8 = undefined;
        var n: usize = 0;
        const list = case["enum:".len..];
        // `enum:` alone: the case failed before its first choice.
        var it = std.mem.splitScalar(u8, list, '.');
        while (list.len != 0) {
            const part = it.next() orelse break;
            if (n == choices.len) return error.BadReplayCase;
            choices[n] = std.fmt.parseInt(u8, part, 10) catch return error.BadReplayCase;
            n += 1;
        }
        var chooser: Chooser = .{ .fixed = .{ .choices = choices[0..n] } };
        out = try runCase(g, &chooser, enum_depth, null);
    } else return error.BadReplayCase;
    const id = if (seed) |sd| caseId(&id_buf, sd, &.{}) else case;
    if (out.failure) |f| report(g, f, id, seed, &out);
    buildLine(g);
    std.debug.print("CASE={s}\nTRACE={s}\nTRACE_HASH=0x{x:0>16}\nDST-TRANSPORT REPLAY PASS\n", .{ id, out.trace[0..out.trace_len], out.hash });
}

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn isCommit(s: []const u8) bool {
    if (s.len < 40) return false;
    for (s[0..40]) |c| {
        if (!std.ascii.isHex(c)) return false;
    }
    return true;
}

/// The commit the seeds derive from. A `git archive` export substitutes
/// it into `dst_commit.txt` (`.gitattributes` `export-subst`); a checkout
/// gets it from `git rev-parse` at configure time (`build.zig`).
fn commitIdentity() ?[]const u8 {
    const embedded = std.mem.trim(u8, @embedFile("dst_commit.txt"), " \r\n\t");
    if (isCommit(embedded)) return embedded;
    if (isCommit(dst_options.git_commit)) return dst_options.git_commit;
    return null;
}

fn runMain(io: std.Io, argv0: []const u8, mode_replay: ?[]const u8) !void {
    const commit = commitIdentity() orelse {
        std.debug.print("dst-transport: no commit identity: tests/dst_commit.txt is not substituted and git gave none ({s})\n", .{dst_options.git_commit});
        std.process.exit(exit_usage);
    };
    var acceptor = try tls_edge.Acceptor.initFromPem(std.heap.page_allocator, io, tls_edge.fixture_cert_pem, tls_edge.fixture_key_pem);
    defer acceptor.deinit();
    const pair = try tls_edge.memHandshake(&acceptor, &.{tls_edge.alpn_http11}, std.Io.Clock.real.now(io));
    var g: Globals = .{ .io = io, .cipher = pair.server, .client_cipher = pair.client, .argv0 = argv0, .commit = commit };
    if (mode_replay) |case| return replay(&g, case);
    std.debug.print("dst-transport: scope: this witness proves the HTTP/1.1 TLS transport cannot regain an unread failure channel. It does not reproduce the HTTPS crash end to end, and it says nothing about HTTP/2, the scheduler, or real threads.\n", .{});
    try corpus(&g);
}

pub fn main(init: std.process.Init.Minimal) !void {
    var it = std.process.Args.Iterator.init(init.args);
    const argv0 = it.next() orelse "dst-transport";
    var mode_replay: ?[]const u8 = null;
    if (it.next()) |arg| {
        if (!std.mem.eql(u8, arg, "--replay")) {
            std.debug.print("usage: {s} [--replay enum:<choices>|seed:<n>]\n", .{argv0});
            std.process.exit(exit_usage);
        }
        mode_replay = it.next() orelse {
            std.debug.print("usage: {s} [--replay enum:<choices>|seed:<n>]\n", .{argv0});
            std.process.exit(exit_usage);
        };
    }
    // One executor: the witness is single-threaded by design; the model
    // decides every order.
    const rt = try zio.Runtime.init(std.heap.page_allocator, .{ .executors = .exact(1) });
    defer rt.deinit();
    var h = try rt.spawn(runMain, .{ rt.io(), argv0, mode_replay });
    h.join() catch |err| switch (err) {
        error.BadReplayCase => {
            std.debug.print("dst-transport: not a case id this build can replay\n", .{});
            std.process.exit(exit_usage);
        },
        else => return err,
    };
}
