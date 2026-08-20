//! Probe: does a zio timed wait fire on time while its home executor is
//! hogged by a task that never yields?
//!
//! This is the t-824 mechanism test, re-armed for t-883. The deadline heap
//! exists because zio timers used to arm on the sleeper's HOME loop:
//! `tick_target_ns` only forces a poll once `coro.step()` returns, so an
//! actor that stays hot froze every handler timer on its executor. The
//! fork's dedicated timer executor (53504b4) claims to retire that. This
//! probe measures it instead of trusting the changelog.
//!
//! Shape: exact(1) executor, migration off — the T1/t-824 configuration.
//! Task H spins without yielding for HOG_MS. Task W arms a 50 ms timed
//! wait on a never-set event before the hog ends. If timer delivery still
//! depends on the home loop, W fires only after H stops (~HOG_MS); with
//! the dedicated timer executor it fires at ~50 ms.
//!
//! Exit 1 when the observed latency is more than 4x the armed timeout.
const std = @import("std");
const zio = @import("zio");

const hog_ms: u64 = 500;
const wait_ms: u64 = 50;

var never_set: zio.ResetEvent = .init;
var hog_running = std.atomic.Value(bool).init(false);
var waiter_armed = std.atomic.Value(bool).init(false);
var fired_at_ns = std.atomic.Value(u64).init(0);

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

fn hog() !void {
    hog_running.store(true, .release);
    // Wait until the waiter is armed. This wait must YIELD: the probe runs
    // one cooperative executor, so a spin here would starve the waiter
    // before the experiment starts.
    while (!waiter_armed.load(.acquire)) {
        try zio.yield();
    }
    const until = nowNs() + hog_ms * std.time.ns_per_ms;
    var x: u64 = 0;
    while (nowNs() < until) {
        x +%= 1;
        std.atomic.spinLoopHint();
    }
    std.mem.doNotOptimizeAway(&x);
}

fn waiter() !void {
    while (!hog_running.load(.acquire)) {
        try zio.yield();
    }
    const armed = nowNs();
    waiter_armed.store(true, .release);
    never_set.timedWait(.{ .duration = .fromMilliseconds(wait_ms) }) catch |err| switch (err) {
        error.Timeout => {
            fired_at_ns.store(nowNs() - armed, .release);
            return;
        },
        else => return err,
    };
}

fn run(rt: *zio.Runtime) !void {
    var w = try rt.spawn(waiter, .{});
    var h = try rt.spawn(hog, .{});
    try w.join();
    try h.join();
}

// Second scenario: the same shape through std.Io.Event.waitTimeout (the
// futex path), because 53504b4 names sleep/select/AutoCancel and the
// per-slot deadline events are std.Io.Event today.
var std_event: std.Io.Event = .unset;
var fired2_at_ns = std.atomic.Value(u64).init(0);
var waiter2_armed = std.atomic.Value(bool).init(false);
var hog2_running = std.atomic.Value(bool).init(false);

fn hog2() !void {
    hog2_running.store(true, .release);
    while (!waiter2_armed.load(.acquire)) {
        try zio.yield();
    }
    const until = nowNs() + hog_ms * std.time.ns_per_ms;
    var x: u64 = 0;
    while (nowNs() < until) {
        x +%= 1;
        std.atomic.spinLoopHint();
    }
    std.mem.doNotOptimizeAway(&x);
}

fn waiter2(io: std.Io) !void {
    while (!hog2_running.load(.acquire)) {
        try zio.yield();
    }
    const armed = nowNs();
    waiter2_armed.store(true, .release);
    std_event.waitTimeout(io, .{ .duration = .{
        .raw = .fromNanoseconds(wait_ms * std.time.ns_per_ms),
        .clock = .awake,
    } }) catch |err| switch (err) {
        error.Timeout => {
            fired2_at_ns.store(nowNs() - armed, .release);
            return;
        },
        else => return err,
    };
}

fn run2(rt: *zio.Runtime) !void {
    var w = try rt.spawn(waiter2, .{rt.io()});
    var h = try rt.spawn(hog2, .{});
    try w.join();
    try h.join();
}

pub fn main() !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(1),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var handle = try rt.spawn(run, .{rt});
    try handle.join();

    const fired = fired_at_ns.load(.acquire);
    const fired_ms = fired / std.time.ns_per_ms;
    std.debug.print(
        "zio.ResetEvent.timedWait: armed {d} ms fired after {d} ms (hog {d} ms, 1 executor, migration off)\n",
        .{ wait_ms, fired_ms, hog_ms },
    );

    var handle2 = try rt.spawn(run2, .{rt});
    try handle2.join();
    const fired2 = fired2_at_ns.load(.acquire);
    const fired2_ms = fired2 / std.time.ns_per_ms;
    std.debug.print(
        "std.Io.Event.waitTimeout:   armed {d} ms fired after {d} ms (same shape)\n",
        .{ wait_ms, fired2_ms },
    );

    var bad = false;
    if (fired == 0 or fired_ms > wait_ms * 4) {
        std.debug.print("FAIL: zio.ResetEvent timer starved behind the hog\n", .{});
        bad = true;
    }
    if (fired2 == 0 or fired2_ms > wait_ms * 4) {
        std.debug.print("FAIL: std.Io.Event timer starved behind the hog\n", .{});
        bad = true;
    }
    if (bad) std.process.exit(1);
}
