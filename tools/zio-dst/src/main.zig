//! DST harness for zio CQ / select / timer (t-1166 M1).
//!
//! Builds zio with `-Dsim=true`. Does not replace `tools/zio-pin-gate`.
//!
//!   ./zb build run -- --d1 --d2 --d4 --d5
//!   ./zb build run -Dsim-mutant=arm_timer_stale -- --arm-mutant

const std = @import("std");
const zio = @import("zio");

const Fixture = enum {
    same_tick_race,
    zero_timer,
    remote_submit,
    close_inflight,
    overflow_1k,
};

const RunOut = struct {
    trace: u64,
    state: u64,
    events: u64,
    clock: u64,
};

fn runtimeOpts() zio.RuntimeOptions {
    return .{
        .executors = .exact(1),
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
        .zero_timer => try zeroTimer(rt),
        .remote_submit => try remoteSubmit(rt),
        .close_inflight => try closeInflight(rt),
        .overflow_1k => try overflow1k(rt),
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

fn armMutant(gpa: std.mem.Allocator, io: std.Io, exe: []const u8) !void {
    std.debug.print("== VALIDITY mutant arm_timer_stale\n", .{});
    if (!zio.sim.mutantArmTimerStale()) {
        std.debug.print("MUTANT_NOT_COMPILED: rebuild with -Dsim-mutant=arm_timer_stale\n", .{});
        std.process.exit(1);
    }
    var seed: u64 = 1;
    var buf: [32]u8 = undefined;
    while (seed <= 1000) : (seed += 1) {
        const seed_s = std.fmt.bufPrint(&buf, "{d}", .{seed}) catch unreachable;
        const res = try std.process.run(gpa, io, .{
            .argv = &.{ exe, "--run", "same_tick_race", "--seed", seed_s },
        });
        defer gpa.free(res.stdout);
        defer gpa.free(res.stderr);
        const needle = "armTimer backdated";
        const hit = std.mem.indexOf(u8, res.stderr, needle) != null or
            std.mem.indexOf(u8, res.stdout, needle) != null;
        if (!hit) continue;

        std.debug.print("MUTANT_FIRED SEED={d}\n", .{seed});
        const res2 = try std.process.run(gpa, io, .{
            .argv = &.{ exe, "--run", "same_tick_race", "--seed", seed_s },
        });
        defer gpa.free(res2.stdout);
        defer gpa.free(res2.stderr);
        const hit2 = std.mem.indexOf(u8, res2.stderr, needle) != null or
            std.mem.indexOf(u8, res2.stdout, needle) != null;
        if (!hit2) {
            std.debug.print("D3 FAIL: replay of SEED={d} did not reproduce the panic\nstderr={s}\n", .{
                seed,
                res2.stderr,
            });
            std.process.exit(1);
        }
        std.debug.print("D3 PASS (replay SEED={d} same panic)\n", .{seed});
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
                std.debug.print("--arm-d4-inner needs clock|futex\n", .{});
                std.process.exit(2);
            };
        } else if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            std.debug.print(
                \\zio-dst — M1 DST harness for zio CQ/select/timer
                \\  --d1 --d2 --d4 --d5   mechanical checks
                \\  --all-fixtures        run every shape-axis fixture once
                \\  --run <fixture> --seed N
                \\  --arm-mutant          validity (needs -Dsim-mutant=arm_timer_stale)
                \\  --arm-d4-inner clock|futex
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
                if (fix == .zero_timer or fix == .same_tick_race) {
                    std.debug.print("FAIL fixture {s}: zero events\n", .{@tagName(fix)});
                    std.process.exit(1);
                }
            }
        }
    }
    if (do_d1) try checkD1(gpa);
    if (do_d2) try checkD2(gpa);
    if (do_d4) try checkD4(gpa, io, exe);

    std.debug.print("M1 checks finished\n", .{});
}
