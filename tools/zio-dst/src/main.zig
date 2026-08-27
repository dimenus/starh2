//! DST harness for zio CQ / select / timer (t-1166).
//!
//! Builds zio with `-Dsim=true`. Does not replace `tools/zio-pin-gate`.
//!
//!   ./zb build run -- --d1 --d2 --d4 --d5
//!   ./zb build run -Dsim-mutant=arm_timer_stale -- --arm-mutant

const std = @import("std");
const zio = @import("zio");

const Fixture = enum {
    same_tick_race,
    select_park,
    select_generations,
    select_task_cancel,
    select_channel,
    select_clobber,
    zero_timer,
    remote_submit,
    close_inflight,
    overflow_1k,
    setclock_real,
    io_pipe,
    io_timeout_race,
    io_close_inflight,
    io_mid_sleep,
};

const RunOut = struct {
    trace: u64,
    state: u64,
    events: u64,
    clock: u64,
};

fn runtimeOpts() zio.RuntimeOptions {
    return .{
        .executors = .exact(2),
        .enable_main_executor = true,
        .enable_task_migration = false,
        .thread_pool = .{ .max_threads = 0 },
        .stack_pool = .{
            .maximum_size = 8 * 1024 * 1024,
            .committed_size = 256 * 1024,
            .shrink_interval = .zero,
        },
    };
}

fn runFixture(gpa: std.mem.Allocator, seed: u64, fixture: Fixture, print_scope: bool) !RunOut {
    zio.sim.begin(seed);
    defer zio.sim.end();
    if (print_scope) zio.sim.printScope();

    const rt = try zio.Runtime.init(gpa, runtimeOpts());
    defer rt.deinit();

    switch (fixture) {
        .same_tick_race => try sameTickRace(rt),
        .select_park => try selectPark(rt),
        .select_generations => try selectGenerations(rt),
        .select_task_cancel => try selectTaskCancel(rt),
        .select_channel => try selectChannel(rt),
        .select_clobber => try selectClobber(rt),
        .zero_timer => try zeroTimer(rt),
        .remote_submit => try remoteSubmit(rt),
        .close_inflight => try closeInflight(rt),
        .overflow_1k => try overflow1k(rt),
        .setclock_real => try setclockReal(rt),
        .io_pipe => try ioPipe(rt),
        .io_timeout_race => try ioTimeoutRace(rt),
        .io_close_inflight => try ioCloseInflight(rt),
        .io_mid_sleep => try ioMidSleep(rt),
    }

    return .{
        .trace = zio.sim.traceHash(),
        .state = zio.sim.stateDigest(),
        .events = zio.sim.eventCount(),
        .clock = zio.sim.clockNs(),
    };
}

const Race = struct {
    cq: zio.CompletionQueue,
    timer: zio.ev.Timer,
};

fn raceDriver(r: *Race) !void {
    const c = r.cq.timedWait(.{ .duration = .fromMilliseconds(10) }) catch |err| switch (err) {
        error.Timeout, error.Closed, error.Canceled => return,
    };
    _ = c;
}

fn racePoster(r: *Race) !void {
    try r.cq.submit(&r.timer.c);
}

fn sameTickRace(rt: *zio.Runtime) !void {
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        var race: Race = .{
            .cq = zio.CompletionQueue.init(),
            .timer = zio.ev.Timer.init(.{ .duration = .fromMilliseconds(10) }),
        };
        // Spawn both before anyone runs so getNextTask sees n>=2. A yield
        // between the spawns serializes them and D2 collapses to one hash.
        var d = try rt.spawn(raceDriver, .{&race});
        var p = try rt.spawn(racePoster, .{&race});
        p.join() catch {};
        d.join() catch {};
        race.cq.cancelAll(.discard);
    }
}

/// Parks `zio.select` with a CQ arm, a timer arm, and a cancellation arm.
/// Without this, select/asyncWait never sits on the queue and select-class
/// bugs cannot fire no matter the seed count.
const SelectPark = struct {
    cq: zio.CompletionQueue,
    io_timer: zio.ev.Timer,
    cancel: zio.ResetEvent,
};

fn selectDriver(p: *SelectPark) !void {
    const result = zio.select(.{
        .io = &p.cq,
        .timer = zio.Timeout.fromMilliseconds(10),
        .cancel = &p.cancel,
    }) catch |err| switch (err) {
        error.Canceled => return,
    };
    switch (result) {
        .io => |r| _ = r catch {},
        .timer => {},
        .cancel => {},
    }
}

fn selectPoster(p: *SelectPark) !void {
    try p.cq.submit(&p.io_timer.c);
}

fn selectCanceler(p: *SelectPark) !void {
    p.cancel.set();
}

fn selectPark(rt: *zio.Runtime) !void {
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        var park: SelectPark = .{
            .cq = zio.CompletionQueue.init(),
            .io_timer = zio.ev.Timer.init(.{ .duration = .fromMilliseconds(10) }),
            .cancel = .init,
        };
        // Park the select first: CQ asyncWait, Timeout arm, ResetEvent arm.
        // Poster and canceler spawn after that so the wait is on the queue
        // rather than winning on the registration sweep.
        var d = try rt.spawn(selectDriver, .{&park});
        try zio.yield();
        var p = try rt.spawn(selectPoster, .{&park});
        var c = try rt.spawn(selectCanceler, .{&park});
        p.join() catch {};
        c.join() catch {};
        d.join() catch {};
        park.cq.cancelAll(.discard);
    }
}

/// Two select generations on one queue. After the first result, `next`,
/// then select again while a poster keeps submitting. A one-select-per-queue
/// lifetime cannot see a stale wake from generation 1 claim generation 2.
const SelectGen = struct {
    cq: zio.CompletionQueue,
    timers: [8]zio.ev.Timer,
};

fn genDriver(g: *SelectGen) !void {
    var gen: usize = 0;
    while (gen < 2) : (gen += 1) {
        const result = zio.select(.{
            .io = &g.cq,
            .timer = zio.Timeout.fromMilliseconds(50),
        }) catch |err| switch (err) {
            error.Canceled => return,
        };
        switch (result) {
            .io => |r| _ = r catch {},
            .timer => {},
        }
        _ = g.cq.next();
    }
}

fn genPoster(g: *SelectGen) !void {
    var i: usize = 0;
    while (i < g.timers.len) : (i += 1) {
        try g.cq.submit(&g.timers[i].c);
        try zio.yield();
    }
}

fn selectGenerations(rt: *zio.Runtime) !void {
    var g: SelectGen = .{
        .cq = zio.CompletionQueue.init(),
        .timers = undefined,
    };
    var i: usize = 0;
    while (i < g.timers.len) : (i += 1) {
        g.timers[i] = zio.ev.Timer.init(.{ .duration = .fromMilliseconds(10) });
    }
    var d = try rt.spawn(genDriver, .{&g});
    try zio.yield();
    var p = try rt.spawn(genPoster, .{&g});
    p.join() catch {};
    d.join() catch {};
    g.cq.cancelAll(.discard);
}

/// Task cancel of the selecting task, not a ResetEvent arm win. A poster
/// races a claim while `JoinHandle.cancel` runs.
const SelectCancel = struct {
    cq: zio.CompletionQueue,
    io_timer: zio.ev.Timer,
};

fn selectCancelDriver(p: *SelectCancel) !void {
    const result = zio.select(.{
        .io = &p.cq,
        .timer = zio.Timeout.fromMilliseconds(100),
    }) catch |err| switch (err) {
        error.Canceled => return,
    };
    switch (result) {
        .io => |r| _ = r catch {},
        .timer => {},
    }
}

fn selectCancelPoster(p: *SelectCancel) !void {
    try p.cq.submit(&p.io_timer.c);
}

fn selectTaskCancel(rt: *zio.Runtime) !void {
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        var park: SelectCancel = .{
            .cq = zio.CompletionQueue.init(),
            .io_timer = zio.ev.Timer.init(.{ .duration = .fromNanoseconds(0) }),
        };
        var d = try rt.spawn(selectCancelDriver, .{&park});
        try zio.yield();
        var p = try rt.spawn(selectCancelPoster, .{&park});
        d.cancel();
        p.join() catch {};
        park.cq.cancelAll(.discard);
    }
}

/// Select with a channel recv arm. A poster send races `JoinHandle.cancel`
/// so a claim can land on the channel while cancel deregisters.
const SelectCh = struct {
    buf: [1]u32 = undefined,
    ch: zio.Channel(u32) = undefined,
};

fn selectChannelDriver(s: *SelectCh) !void {
    var recv = s.ch.asyncReceive();
    const result = zio.select(.{
        .recv = &recv,
        .timer = zio.Timeout.fromMilliseconds(50),
    }) catch |err| switch (err) {
        error.Canceled => return,
    };
    switch (result) {
        .recv => |r| _ = r catch {},
        .timer => {},
    }
}

fn selectChannelPoster(s: *SelectCh) !void {
    s.ch.send(1) catch {};
}

fn selectChannel(rt: *zio.Runtime) !void {
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        var s: SelectCh = .{};
        s.ch = zio.Channel(u32).init(&s.buf);
        var d = try rt.spawn(selectChannelDriver, .{&s});
        try zio.yield();
        var p = try rt.spawn(selectChannelPoster, .{&s});
        d.cancel();
        p.join() catch {};
    }
}

/// One arm is already ready at sweep (pre-buffered channel). A sender
/// races the earlier-registered empty arm. Sweep coop-yield after the
/// empty arm queues, so the send can claim it before the ready arm wins.
const Clobber = struct {
    empty_buf: [1]u32 = undefined,
    ready_buf: [1]u32 = undefined,
    empty: zio.Channel(u32) = undefined,
    ready: zio.Channel(u32) = undefined,
};

fn clobberDriver(c: *Clobber) !void {
    var recv_empty = c.empty.asyncReceive();
    var recv_ready = c.ready.asyncReceive();
    const result = zio.select(.{
        .empty = &recv_empty,
        .ready = &recv_ready,
    }) catch |err| switch (err) {
        error.Canceled => return,
    };
    switch (result) {
        .empty => |r| _ = r catch {},
        .ready => |r| _ = r catch {},
    }
}

fn clobberPoster(c: *Clobber) !void {
    c.empty.send(2) catch {};
}

fn selectClobber(rt: *zio.Runtime) !void {
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        var c: Clobber = .{};
        c.empty = zio.Channel(u32).init(&c.empty_buf);
        c.ready = zio.Channel(u32).init(&c.ready_buf);
        c.ready.send(1) catch {};
        var d = try rt.spawn(clobberDriver, .{&c});
        var p = try rt.spawn(clobberPoster, .{&c});
        p.join() catch {};
        d.join() catch {};
    }
}

fn zeroTimer(rt: *zio.Runtime) !void {
    _ = rt;
    var cq = zio.CompletionQueue.init();
    var timer = zio.ev.Timer.init(.{ .duration = .fromNanoseconds(0) });
    try cq.submit(&timer.c);
    const c = try cq.wait();
    try timer.getResult();
    _ = c;
    cq.cancelAll(.discard);
}

fn remoteSubmit(rt: *zio.Runtime) !void {
    var race: Race = .{
        .cq = zio.CompletionQueue.init(),
        .timer = zio.ev.Timer.init(.{ .duration = .fromNanoseconds(0) }),
    };
    var d = try rt.spawn(raceDriverLong, .{&race});
    try zio.yield();
    try race.cq.submit(&race.timer.c);
    d.join() catch {};
    race.cq.cancelAll(.discard);
}

fn raceDriverLong(r: *Race) !void {
    const c = r.cq.timedWait(.{ .duration = .fromMilliseconds(100) }) catch |err| switch (err) {
        error.Timeout => return error.UnexpectedTimeout,
        error.Closed, error.Canceled => return err,
    };
    _ = c;
}

fn closeInflight(rt: *zio.Runtime) !void {
    _ = rt;
    var cq = zio.CompletionQueue.init();
    var timer = zio.ev.Timer.init(.{ .duration = .fromMilliseconds(10) });
    try cq.submit(&timer.c);
    cq.close();
    const c = try cq.wait();
    try timer.getResult();
    _ = c;
    if (cq.wait()) |_| {
        return error.ExpectedClosed;
    } else |err| switch (err) {
        error.Closed => {},
        else => return err,
    }
}

/// `AutoCancel.setClock` (#717). A `.real` deadline on the awake heap
/// never fires (unix epoch vs monotonic). Under sim, `.real` lives in a
/// distinct epoch so this fixture deadlocks if `setClock` does not assign
/// `timer.clock`.
fn expectAutoCancel(rt: *zio.Runtime, timeout: *zio.AutoCancel) !void {
    const before = zio.sim.clockNs();
    if (rt.sleep(.fromMilliseconds(500))) |_| {
        return error.DidNotCancel;
    } else |err| switch (err) {
        error.Canceled => {
            if (!timeout.check(err)) return error.NotAutoCancel;
        },
    }
    const elapsed = zio.sim.clockNs() - before;
    if (elapsed < 10_000_000) return error.ClockDidNotAdvance;
    if (elapsed >= 1_000_000_000) return error.OversleptWallTimer;
}

fn setclockReal(rt: *zio.Runtime) !void {
    {
        var timeout: zio.AutoCancel = .init;
        defer timeout.clear();
        timeout.setClock(.fromMilliseconds(10), .real);
        try expectAutoCancel(rt, &timeout);
    }
    {
        var timeout: zio.AutoCancel = .init;
        defer timeout.clear();
        const deadline = zio.Timestamp.now(.real).addDuration(.fromMilliseconds(10));
        timeout.setClock(.{ .deadline = deadline }, .real);
        try expectAutoCancel(rt, &timeout);
    }
    {
        var timeout: zio.AutoCancel = .init;
        defer timeout.clear();
        timeout.setClock(.fromSeconds(60), .real);
        timeout.setClock(.fromMilliseconds(10), .awake);
        try expectAutoCancel(rt, &timeout);
    }
}

const IoRace = struct {
    cq: zio.CompletionQueue,
    send: zio.ev.NetSend,
    recv: zio.ev.NetRecv,
    out: [8]u8,
};

fn ioPipe(rt: *zio.Runtime) !void {
    _ = rt;
    const fds = zio.sim.pipePair();
    const payload = "hello";
    var out: [8]u8 = @splat(0);
    var send_store: [1]zio.os.iovec_const = undefined;
    var recv_store: [1]zio.os.iovec = undefined;
    var send_op = zio.ev.NetSend.init(fds[1], zio.ev.WriteBuf.fromSlice(payload, &send_store), .{});
    var recv_op = zio.ev.NetRecv.init(fds[0], zio.ev.ReadBuf.fromSlice(&out, &recv_store), .{});
    var cq = zio.CompletionQueue.init();
    try cq.submit(&recv_op.c);
    try cq.submit(&send_op.c);
    var got_recv: bool = false;
    var got_send: bool = false;
    var i: usize = 0;
    while (i < 2) : (i += 1) {
        const c = try cq.wait();
        if (c == &recv_op.c) {
            got_recv = true;
            const n = try recv_op.getResult();
            if (n != payload.len) return error.BadRecvLen;
            if (!std.mem.eql(u8, out[0..n], payload)) return error.BadRecvData;
        } else if (c == &send_op.c) {
            got_send = true;
            const n = try send_op.getResult();
            if (n != payload.len) return error.BadSendLen;
        } else return error.UnknownCompletion;
    }
    if (!got_recv or !got_send) return error.MissingCompletion;
    var close_op = zio.ev.NetClose.init(fds[0]);
    try cq.submit(&close_op.c);
    _ = try cq.wait();
}

fn ioRecvDriver(r: *IoRace) !void {
    try r.cq.submit(&r.recv.c);
    const c = r.cq.timedWait(.{ .duration = .fromMilliseconds(10) }) catch |err| switch (err) {
        error.Timeout, error.Closed, error.Canceled => return,
    };
    _ = c;
}

fn ioSendPoster(r: *IoRace) !void {
    try zio.sleep(.fromMilliseconds(10));
    try r.cq.submit(&r.send.c);
}

fn ioTimeoutRace(rt: *zio.Runtime) !void {
    const fds = zio.sim.pipePair();
    const payload = "xy";
    var send_store: [1]zio.os.iovec_const = undefined;
    var recv_store: [1]zio.os.iovec = undefined;
    var race: IoRace = .{
        .cq = zio.CompletionQueue.init(),
        .out = @splat(0),
        .send = undefined,
        .recv = undefined,
    };
    race.send = zio.ev.NetSend.init(fds[1], zio.ev.WriteBuf.fromSlice(payload, &send_store), .{});
    race.recv = zio.ev.NetRecv.init(fds[0], zio.ev.ReadBuf.fromSlice(&race.out, &recv_store), .{});
    var d = try rt.spawn(ioRecvDriver, .{&race});
    var p = try rt.spawn(ioSendPoster, .{&race});
    p.join() catch {};
    d.join() catch {};
    race.cq.cancelAll(.discard);
}

fn ioCloseInflight(rt: *zio.Runtime) !void {
    _ = rt;
    const fds = zio.sim.pipePair();
    var out: [8]u8 = @splat(0);
    var recv_store: [1]zio.os.iovec = undefined;
    var recv_op = zio.ev.NetRecv.init(fds[0], zio.ev.ReadBuf.fromSlice(&out, &recv_store), .{});
    var cq = zio.CompletionQueue.init();
    try cq.submit(&recv_op.c);
    var close_op = zio.ev.NetClose.init(fds[1]);
    try cq.submit(&close_op.c);
    var saw_recv = false;
    var i: usize = 0;
    while (i < 2) : (i += 1) {
        const c = try cq.wait();
        if (c == &recv_op.c) {
            saw_recv = true;
            const n = try recv_op.getResult();
            if (n != 0) return error.ExpectedEof;
        }
    }
    if (!saw_recv) return error.RecvNotCompleted;
}

fn sleep50ms() !void {
    try zio.sleep(.fromMilliseconds(50));
}

/// Recv+send on a pipe while a 50 ms timer is also armed. Delayed I/O must
/// wake `waitForEvents` at the due-I/O time (`timed_out=false`). Advancing
/// the full timer would oversleep.
fn ioMidSleep(rt: *zio.Runtime) !void {
    var sleeper = try rt.spawn(sleep50ms, .{});
    const fds = zio.sim.pipePair();
    const payload = "hi";
    var out: [8]u8 = @splat(0);
    var send_store: [1]zio.os.iovec_const = undefined;
    var recv_store: [1]zio.os.iovec = undefined;
    var send_op = zio.ev.NetSend.init(fds[1], zio.ev.WriteBuf.fromSlice(payload, &send_store), .{});
    var recv_op = zio.ev.NetRecv.init(fds[0], zio.ev.ReadBuf.fromSlice(&out, &recv_store), .{});
    var cq = zio.CompletionQueue.init();
    try cq.submit(&recv_op.c);
    try cq.submit(&send_op.c);
    var i: usize = 0;
    while (i < 2) : (i += 1) {
        _ = try cq.wait();
    }
    // After a mid-sleep I/O wake (`timed_out=false`), arm a duration
    // timer. That is the #711 consumer: the snapshot after an I/O wake,
    // not after a timer timeout (checkTimers already refreshed).
    var later = zio.ev.Timer.init(.{ .duration = .fromMilliseconds(10) });
    try cq.submit(&later.c);
    _ = try cq.wait();
    sleeper.cancel();
    if (zio.sim.clockNs() >= 50_000_000) return error.Overslept;
}

fn overflowInc(counter: *usize) void {
    counter.* += 1;
}

fn overflow1k(rt: *zio.Runtime) !void {
    _ = rt;
    var counter: usize = 0;
    var group: zio.Group = .init;
    defer group.cancel();
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        try group.spawn(overflowInc, .{&counter});
    }
    try group.wait();
    if (counter != 1000) {
        std.debug.panic("overflow_1k: counter={d} want 1000", .{counter});
    }
}

fn printRun(seed: u64, fixture: Fixture, out: RunOut) void {
    std.debug.print(
        "SEED={d} fixture={s} TRACE_HASH={x:0>16} STATE_DIGEST={x:0>16} events={d} clock_ns={d}\n",
        .{ seed, @tagName(fixture), out.trace, out.state, out.events, out.clock },
    );
}

fn checkD1(gpa: std.mem.Allocator) !void {
    std.debug.print("== D1 DETERMINISM (20 seeds x 2, same_tick_race)\n", .{});
    var seed: u64 = 1;
    while (seed <= 20) : (seed += 1) {
        const a = try runFixture(gpa, seed, .same_tick_race, seed == 1);
        const b = try runFixture(gpa, seed, .same_tick_race, false);
        printRun(seed, .same_tick_race, a);
        if (a.trace != b.trace or a.state != b.state) {
            std.debug.print(
                "D1 FAIL SEED={d} run1 TRACE_HASH={x} STATE_DIGEST={x} run2 TRACE_HASH={x} STATE_DIGEST={x}\n",
                .{ seed, a.trace, a.state, b.trace, b.state },
            );
            std.process.exit(1);
        }
        if (a.events == 0) {
            std.debug.print("D1 FAIL SEED={d} zero events (the run did not happen)\n", .{seed});
            std.process.exit(1);
        }
    }
    std.debug.print("D1 PASS\n", .{});
}

fn checkD2(gpa: std.mem.Allocator) !void {
    std.debug.print("== D2 EXPLORATION (1000 seeds, same_tick_race, >=100 distinct traces)\n", .{});
    var seen: std.AutoHashMap(u64, void) = .init(gpa);
    defer seen.deinit();
    var seed: u64 = 1;
    while (seed <= 1000) : (seed += 1) {
        const out = try runFixture(gpa, seed, .same_tick_race, seed == 1);
        try seen.put(out.trace, {});
        if (seed <= 5 or seed == 1000) printRun(seed, .same_tick_race, out);
    }
    const n = seen.count();
    std.debug.print("DISTINCT_HASHES={d} SEEDS=1000\n", .{n});
    if (n < 100) {
        std.debug.print("D2 FAIL: distinct traces {d} < 100\n", .{n});
        std.process.exit(1);
    }
    if (n == 0) {
        std.debug.print("D2 FAIL: zero hashes (the run did not happen)\n", .{});
        std.process.exit(1);
    }
    std.debug.print("D2 PASS\n", .{});
}

fn checkD4(gpa: std.mem.Allocator, io: std.Io, exe: []const u8) !void {
    std.debug.print("== D4 NO WALL CLOCK (arm clock and futex)\n", .{});
    try expectChildPanic(gpa, io, exe, &.{ "--arm-d4-inner", "clock" }, "sim: real clock_gettime");
    try expectChildPanic(gpa, io, exe, &.{ "--arm-d4-inner", "futex" }, "sim: real kernel futex");
    try expectChildPanic(gpa, io, exe, &.{ "--arm-d4-inner", "io" }, "sim: real backend.poll");
    std.debug.print("D4 PASS\n", .{});
}

fn checkD5(gpa: std.mem.Allocator, io: std.Io, exe: []const u8) !void {
    std.debug.print("== D5 SCOPE HONESTY\n", .{});
    const res = try std.process.run(gpa, io, .{
        .argv = &.{ exe, "--run", "zero_timer", "--seed", "1" },
    });
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    const text = if (res.stderr.len > 0) res.stderr else res.stdout;
    const has_sim = std.mem.indexOf(u8, text, "SCOPE simulated:") != null;
    const has_real = std.mem.indexOf(u8, text, "SCOPE real:") != null;
    const unsim = std.mem.indexOf(u8, text, "SCOPE unsimulated:");
    if (!has_sim or !has_real or unsim == null) {
        std.debug.print("D5 FAIL: missing SCOPE lines\n{s}\n", .{text});
        std.process.exit(1);
    }
    const rest = text[unsim.? + "SCOPE unsimulated:".len ..];
    const line_end = std.mem.indexOfScalar(u8, rest, '\n') orelse rest.len;
    const named = std.mem.trim(u8, rest[0..line_end], " \t");
    if (named.len == 0) {
        std.debug.print("D5 FAIL: unsimulated line is empty (silent real source)\n", .{});
        std.process.exit(1);
    }
    if (std.mem.indexOf(u8, text, "net_pipe") == null) {
        std.debug.print("D5 FAIL: simulated line does not name net_pipe\n{s}\n", .{text});
        std.process.exit(1);
    }
    if (std.mem.indexOf(u8, text, "extra_logical_executors") == null) {
        std.debug.print("D5 FAIL: simulated line does not name extra_logical_executors\n{s}\n", .{text});
        std.process.exit(1);
    }
    std.debug.print("{s}", .{text});
    std.debug.print("D5 PASS\n", .{});
}

fn expectChildPanic(
    gpa: std.mem.Allocator,
    io: std.Io,
    exe: []const u8,
    extra: []const []const u8,
    needle: []const u8,
) !void {
    var argv = std.ArrayList([]const u8).empty;
    defer argv.deinit(gpa);
    try argv.append(gpa, exe);
    try argv.appendSlice(gpa, extra);
    const res = try std.process.run(gpa, io, .{ .argv = argv.items });
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    const hit = std.mem.indexOf(u8, res.stderr, needle) != null or
        std.mem.indexOf(u8, res.stdout, needle) != null;
    if (!hit) {
        std.debug.print("D4 FAIL: child did not print `{s}`\nterm={any}\nstderr={s}\nstdout={s}\n", .{
            needle,
            res.term,
            res.stderr,
            res.stdout,
        });
        std.process.exit(1);
    }
    switch (res.term) {
        .exited => |c| if (c == 0) {
            std.debug.print("D4 FAIL: child exited 0 after printing the panic needle\n", .{});
            std.process.exit(1);
        },
        else => {},
    }
    std.debug.print("  armed `{s}`: fired\n", .{needle});
}

fn parseTraceHash(text: []const u8) ?u64 {
    const key = "TRACE_HASH=";
    const i = std.mem.indexOf(u8, text, key) orelse return null;
    const rest = text[i + key.len ..];
    var n: usize = 0;
    while (n < rest.len and n < 16) : (n += 1) {
        const c = rest[n];
        const hex = (c >= '0' and c <= '9') or (c >= 'a' and c <= 'f') or (c >= 'A' and c <= 'F');
        if (!hex) break;
    }
    if (n == 0) return null;
    return std.fmt.parseInt(u64, rest[0..n], 16) catch null;
}

fn armMutant(gpa: std.mem.Allocator, io: std.Io, exe: []const u8) !void {
    const stale = zio.sim.mutantArmTimerStale();
    const omit = zio.sim.mutantOmitTimeoutRecheck();
    const needle: []const u8 = if (stale)
        "armTimer backdated"
    else if (omit)
        "timedWait Timeout with a ready completion"
    else {
        std.debug.print("MUTANT_NOT_COMPILED: rebuild with -Dsim-mutant=arm_timer_stale or omit_timeout_recheck\n", .{});
        std.process.exit(1);
    };
    std.debug.print("== VALIDITY mutant {s}\n", .{if (stale) "arm_timer_stale" else "omit_timeout_recheck"});
    var seed: u64 = 1;
    var buf: [32]u8 = undefined;
    while (seed <= 1000) : (seed += 1) {
        const seed_s = std.fmt.bufPrint(&buf, "{d}", .{seed}) catch unreachable;
        const res = try std.process.run(gpa, io, .{
            .argv = &.{ exe, "--run", "same_tick_race", "--seed", seed_s },
        });
        defer gpa.free(res.stdout);
        defer gpa.free(res.stderr);
        const hit = std.mem.indexOf(u8, res.stderr, needle) != null or
            std.mem.indexOf(u8, res.stdout, needle) != null;
        if (!hit) continue;

        const h1 = parseTraceHash(res.stderr) orelse parseTraceHash(res.stdout);
        if (h1 == null) {
            std.debug.print("D3 FAIL: SEED={d} panic had no TRACE_HASH\nstderr={s}\nstdout={s}\n", .{
                seed,
                res.stderr,
                res.stdout,
            });
            std.process.exit(1);
        }

        std.debug.print("MUTANT_FIRED SEED={d} TRACE_HASH={x:0>16}\n", .{ seed, h1.? });
        const res2 = try std.process.run(gpa, io, .{
            .argv = &.{ exe, "--run", "same_tick_race", "--seed", seed_s },
        });
        defer gpa.free(res2.stdout);
        defer gpa.free(res2.stderr);
        const hit2 = std.mem.indexOf(u8, res2.stderr, needle) != null or
            std.mem.indexOf(u8, res2.stdout, needle) != null;
        const h2 = parseTraceHash(res2.stderr) orelse parseTraceHash(res2.stdout);
        if (!hit2) {
            std.debug.print("D3 FAIL: replay of SEED={d} did not reproduce the panic\nstderr={s}\n", .{
                seed,
                res2.stderr,
            });
            std.process.exit(1);
        }
        if (h2 == null or h2.? != h1.?) {
            std.debug.print("D3 FAIL: replay TRACE_HASH mismatch SEED={d} first={x} second={x}\n", .{
                seed,
                h1.?,
                h2 orelse 0,
            });
            std.process.exit(1);
        }
        std.debug.print("D3 PASS (replay SEED={d} same panic and TRACE_HASH)\n", .{seed});
        return;
    }
    std.debug.print("MUTANT_NOT_FOUND after 1000 seeds (a clean sweep at this count is a fail)\n", .{});
    std.process.exit(1);
}

fn armD4Inner(kind: []const u8) noreturn {
    zio.sim.begin(1);
    zio.sim.printScope();
    if (std.mem.eql(u8, kind, "clock")) {
        zio.sim.setClockRouted(false);
        _ = zio.time.Timestamp.now(.awake);
        std.debug.print("D4 FAIL: Timestamp.now returned in sim without the clock route\n", .{});
        std.process.exit(1);
    } else if (std.mem.eql(u8, kind, "futex")) {
        var word: std.atomic.Value(u32) = .init(0);
        zio.os.thread.Futex.wait(&word, 0);
        std.debug.print("D4 FAIL: kernel futex wait returned in sim mode\n", .{});
        std.process.exit(1);
    } else if (std.mem.eql(u8, kind, "io")) {
        const rt = zio.Runtime.init(std.heap.page_allocator, runtimeOpts()) catch {
            std.debug.print("D4 FAIL: Runtime.init for backend.poll trip\n", .{});
            std.process.exit(1);
        };
        const exec = zio.getCurrentExecutor();
        _ = exec.loop.backend.poll(&exec.loop.state, .zero) catch {};
        rt.deinit();
        std.debug.print("D4 FAIL: backend.poll returned in sim mode\n", .{});
        std.process.exit(1);
    } else {
        std.debug.print("unknown --arm-d4-inner {s}\n", .{kind});
        std.process.exit(2);
    }
}

fn parseFixture(name: []const u8) ?Fixture {
    inline for (std.meta.tags(Fixture)) |t| {
        if (std.mem.eql(u8, name, @tagName(t))) return t;
    }
    return null;
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args.deinit();
    const exe = args.next() orelse return error.NoExe;

    var do_d1 = false;
    var do_d2 = false;
    var do_d4 = false;
    var do_d5 = false;
    var do_arm_mutant = false;
    var do_all_fixtures = false;
    var run_one: ?Fixture = null;
    var seed: u64 = 1;
    var arm_d4_inner: ?[]const u8 = null;

    while (args.next()) |a| {
        if (std.mem.eql(u8, a, "--d1")) {
            do_d1 = true;
        } else if (std.mem.eql(u8, a, "--d2")) {
            do_d2 = true;
        } else if (std.mem.eql(u8, a, "--d4")) {
            do_d4 = true;
        } else if (std.mem.eql(u8, a, "--d5")) {
            do_d5 = true;
        } else if (std.mem.eql(u8, a, "--arm-mutant")) {
            do_arm_mutant = true;
        } else if (std.mem.eql(u8, a, "--all-fixtures")) {
            do_all_fixtures = true;
        } else if (std.mem.eql(u8, a, "--run")) {
            const name = args.next() orelse {
                std.debug.print("--run needs a fixture name\n", .{});
                std.process.exit(2);
            };
            run_one = parseFixture(name) orelse {
                std.debug.print("unknown fixture {s}\n", .{name});
                std.process.exit(2);
            };
        } else if (std.mem.eql(u8, a, "--seed")) {
            const s = args.next() orelse {
                std.debug.print("--seed needs a number\n", .{});
                std.process.exit(2);
            };
            seed = std.fmt.parseInt(u64, s, 10) catch {
                std.debug.print("bad --seed {s}\n", .{s});
                std.process.exit(2);
            };
        } else if (std.mem.eql(u8, a, "--arm-d4-inner")) {
            arm_d4_inner = args.next() orelse {
                std.debug.print("--arm-d4-inner needs clock|futex|io\n", .{});
                std.process.exit(2);
            };
        } else if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            std.debug.print(
                \\zio-dst — DST harness for zio CQ/select/timer/sim I/O
                \\  --d1 --d2 --d4 --d5   mechanical checks
                \\  --all-fixtures        run every shape-axis fixture once
                \\  --run <fixture> --seed N
                \\  --arm-mutant          validity (needs -Dsim-mutant=arm_timer_stale)
                \\  --arm-d4-inner clock|futex|io
                \\
            , .{});
            return;
        } else {
            std.debug.print("unknown arg {s}\n", .{a});
            std.process.exit(2);
        }
    }

    if (arm_d4_inner) |kind| armD4Inner(kind);

    if (run_one) |fix| {
        std.debug.print("SEED={d}\n", .{seed});
        const out = try runFixture(gpa, seed, fix, true);
        printRun(seed, fix, out);
        return;
    }

    if (do_arm_mutant) {
        try armMutant(gpa, io, exe);
        return;
    }

    if (!do_d1 and !do_d2 and !do_d4 and !do_d5 and !do_all_fixtures) {
        do_d1 = true;
        do_d2 = true;
        do_d4 = true;
        do_d5 = true;
        do_all_fixtures = true;
    }

    if (do_d5) try checkD5(gpa, io, exe);
    if (do_all_fixtures) {
        std.debug.print("== fixtures\n", .{});
        inline for (std.meta.tags(Fixture)) |fix| {
            const out = try runFixture(gpa, 7, fix, false);
            printRun(7, fix, out);
            if (out.events == 0 and fix != .overflow_1k) {
                // overflow_1k may still emit task_switch events; a true zero
                // on a timer fixture means the instrument did not fire.
                if (fix == .zero_timer or fix == .same_tick_race or fix == .select_park or
                    fix == .select_generations or fix == .select_task_cancel or
                    fix == .select_channel or fix == .select_clobber or fix == .io_mid_sleep)
                {
                    std.debug.print("FAIL fixture {s}: zero events\n", .{@tagName(fix)});
                    std.process.exit(1);
                }
            }
        }
    }
    if (do_d1) try checkD1(gpa);
    if (do_d2) try checkD2(gpa);
    if (do_d4) try checkD4(gpa, io, exe);

    std.debug.print("DST checks finished\n", .{});
}
