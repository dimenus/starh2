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
        "timed wait armed at {d} ms fired after {d} ms (hog {d} ms, 1 executor, migration off)\n",
        .{ wait_ms, fired_ms, hog_ms },
    );
    if (fired == 0) {
        std.debug.print("FAIL: the wait never timed out\n", .{});
        std.process.exit(1);
    }
    if (fired_ms > wait_ms * 4) {
        std.debug.print("FAIL: timer starved behind the hog\n", .{});
        std.process.exit(1);
    }
}
