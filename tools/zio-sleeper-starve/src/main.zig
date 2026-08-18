//! Two zio executors, a 10 ms sleeper, and tasks that never wait.
//!
//! Mixed SSE missed its cadence because a runnable task never returns to
//! Executor.run, so that executor never polls its timer. This program is that
//! shape without HTTP. The calling thread is not an executor
//! (`enable_main_executor = false`): a wall sleep on main would otherwise pin
//! executor 0.
//!
//! Cases:
//!   spin1  — one hog. Starve iff it lands on the executor that owns the
//!            sleeper's timer (that loop never polls). Idle executor has no
//!            timer to fire. Race: this run is ok or STARVE.
//!   spin2  — two hogs; both loops busy; always starve after settle
//!   yield2 — two hogs that maybeYield (zio's documented escape)
//!   emptyq — two hogs looping Queue get min=0 on empty buffers (no peer wake)
//!   hotq2  — two hogs get/put the same queue (they wake each other; not a hog)
//!
//! Expected ticks in 1 s at 10 ms: ~100. Starve is 0 besides the settle window.

const std = @import("std");
const zio = @import("zio");

extern "c" fn nanosleep(req: *const std.c.timespec, rem: ?*std.c.timespec) c_int;

const Case = enum { spin1, spin2, yield2, emptyq, hotq2, all };

const interval_ms: u64 = 10;
const run_ms: u64 = 1000;
const expected_ticks: u64 = run_ms / interval_ms;

const State = struct {
    stop: std.atomic.Value(bool) = .init(false),
    ticks: std.atomic.Value(u64) = .init(0),
    hog_iters: std.atomic.Value(u64) = .init(0),
};

fn wallSleep(ns: u64) void {
    var req = std.c.timespec{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    while (true) {
        var rem: std.c.timespec = undefined;
        const rc = nanosleep(&req, &rem);
        if (rc == 0) return;
        req = rem;
    }
}

fn sleeper(state: *State) void {
    while (true) {
        zio.sleep(.fromMilliseconds(interval_ms)) catch break;
        if (state.stop.load(.acquire)) break;
        _ = state.ticks.fetchAdd(1, .monotonic);
    }
}

fn spinHog(state: *State) void {
    var n: u64 = 0;
    while (!state.stop.load(.monotonic)) {
        n +%= 1;
        std.mem.doNotOptimizeAway(&n);
    }
    _ = state.hog_iters.fetchAdd(n, .monotonic);
}

fn yieldHog(state: *State) void {
    var n: u64 = 0;
    while (!state.stop.load(.monotonic)) {
        n +%= 1;
        zio.maybeYield() catch break;
    }
    _ = state.hog_iters.fetchAdd(n, .monotonic);
}

const Hot = struct {
    state: *State,
    io: std.Io,
    queue: *std.Io.Queue(u8),
};

fn hotPut(h: *Hot) void {
    var n: u64 = 0;
    var item: [1]u8 = .{1};
    while (!h.state.stop.load(.monotonic)) {
        _ = h.queue.putUncancelable(h.io, &item, 0) catch {};
        n +%= 1;
    }
    _ = h.state.hog_iters.fetchAdd(n, .monotonic);
}

fn hotGet(h: *Hot) void {
    var n: u64 = 0;
    var item: [1]u8 = undefined;
    while (!h.state.stop.load(.monotonic)) {
        _ = h.queue.getUncancelable(h.io, &item, 0) catch {};
        n +%= 1;
    }
    _ = h.state.hog_iters.fetchAdd(n, .monotonic);
}

fn emptyGet(h: *Hot) void {
    var n: u64 = 0;
    var item: [1]u8 = undefined;
    while (!h.state.stop.load(.monotonic)) {
        _ = h.queue.getUncancelable(h.io, &item, 0) catch {};
        n +%= 1;
    }
    _ = h.state.hog_iters.fetchAdd(n, .monotonic);
}

fn runCase(gpa: std.mem.Allocator, case: Case) !void {
    const hog_n: u8 = if (case == .spin1) 1 else 2;
    const runtime = try zio.Runtime.init(gpa, .{
        .executors = .exact(2),
        .enable_main_executor = false,
        .enable_task_migration = true,
    });
    defer runtime.deinit();

    var state: State = .{};
    var sleeper_h = try runtime.spawn(sleeper, .{&state});
    // Let the sleeper arm its first timer before the hogs take the workers.
    wallSleep(50 * std.time.ns_per_ms);

    var buf: [8]u8 = undefined;
    var buf_a: [8]u8 = undefined;
    var buf_b: [8]u8 = undefined;
    var queue = std.Io.Queue(u8).init(&buf);
    var empty_a = std.Io.Queue(u8).init(&buf_a);
    var empty_b = std.Io.Queue(u8).init(&buf_b);
    var hot_put_ctx: Hot = .{ .state = &state, .io = runtime.io(), .queue = &queue };
    var hot_get_ctx: Hot = .{ .state = &state, .io = runtime.io(), .queue = &queue };
    var empty_a_ctx: Hot = .{ .state = &state, .io = runtime.io(), .queue = &empty_a };
    var empty_b_ctx: Hot = .{ .state = &state, .io = runtime.io(), .queue = &empty_b };

    var hog1 = switch (case) {
        .spin1, .spin2 => try runtime.spawn(spinHog, .{&state}),
        .yield2 => try runtime.spawn(yieldHog, .{&state}),
        .hotq2 => try runtime.spawn(hotPut, .{&hot_put_ctx}),
        .emptyq => try runtime.spawn(emptyGet, .{&empty_a_ctx}),
        .all => unreachable,
    };
    var hog2: ?@TypeOf(hog1) = null;
    if (hog_n == 2) {
        hog2 = switch (case) {
            .spin2 => try runtime.spawn(spinHog, .{&state}),
            .yield2 => try runtime.spawn(yieldHog, .{&state}),
            .hotq2 => try runtime.spawn(hotGet, .{&hot_get_ctx}),
            .emptyq => try runtime.spawn(emptyGet, .{&empty_b_ctx}),
            .spin1, .all => unreachable,
        };
    }

    wallSleep(run_ms * std.time.ns_per_ms);
    state.stop.store(true, .release);

    hog1.join();
    if (hog2) |*h| h.join();
    sleeper_h.join();

    const ticks = state.ticks.load(.acquire);
    const iters = state.hog_iters.load(.acquire);
    const starved = ticks < expected_ticks / 2;
    std.debug.print(
        "{s:8} executors=2 hogs={d} ticks={d} expected≈{d} hog_iters={d} {s}\n",
        .{
            @tagName(case),
            hog_n,
            ticks,
            expected_ticks,
            iters,
            if (starved) "STARVE" else "ok",
        },
    );
}

fn parseCase(arg: []const u8) !Case {
    if (std.mem.eql(u8, arg, "spin1")) return .spin1;
    if (std.mem.eql(u8, arg, "spin2")) return .spin2;
    if (std.mem.eql(u8, arg, "yield2")) return .yield2;
    if (std.mem.eql(u8, arg, "emptyq")) return .emptyq;
    if (std.mem.eql(u8, arg, "hotq2")) return .hotq2;
    if (std.mem.eql(u8, arg, "all")) return .all;
    return error.UnknownArgument;
}

pub fn main(init: std.process.Init) !void {
    var want: Case = .all;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    if (args.next()) |arg| want = try parseCase(arg);

    std.debug.print(
        "zio sleeper starve: interval={d}ms window={d}ms pin=489f31f\n",
        .{ interval_ms, run_ms },
    );
    if (want == .all) {
        try runCase(init.gpa, .spin1);
        try runCase(init.gpa, .spin2);
        try runCase(init.gpa, .yield2);
        try runCase(init.gpa, .emptyq);
        try runCase(init.gpa, .hotq2);
        return;
    }
    try runCase(init.gpa, want);
}
