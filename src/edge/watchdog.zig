//! The executor watchdog (t-3760): one OS thread per runtime that reports an
//! executor making no scheduler progress, and never preempts it.
//!
//! zio publishes, per executor, a word that changes at every task switch and
//! at every park entry and exit, with a parked bit, and the label of the task
//! it runs (`Executor.readActivity`). Handlers run labelled with their server
//! and route (`label`, set around every handler call). Every
//! `Limits.executor_stall_report_ns / 2` this thread reads each executor. An
//! executor that is not parked, and whose word two reads at least the limit
//! apart both saw, is reported once per stall:
//!
//!     executor 1 made no scheduler progress for at least 1500 ms; route GET /spin
//!
//! "No scheduler progress" is what was observed: one task, or the scheduler
//! itself, held the executor's thread the whole time. It may be a loop, a
//! blocking call, or very long CPU work; the report does not say which. The
//! label is read from the executor and mapped to the route table here; no
//! task, request or handler pointer is ever read. A task a handler spawns
//! (`zio.spawn`) does not inherit the handler's label: it reports "no label".
//!
//! One watchdog per runtime: every watchdog would see every executor, so a
//! second `Server` serving on the same runtime starts none and logs that.
//! The first one reports a stall in the second server's handler as that
//! server's (its tag and route index), never as one of its own routes. When
//! the first server's `serve` returns, the runtime is unwatched until a new
//! `serve` starts.
//!
//! Lifetime: `start` runs in `Server.serve` before any accept loop, and
//! `stop` (which joins) runs when `serve` returns, after every handler has
//! ended, so the thread never outlives the route table or the runtime.
const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const router = @import("../http/router.zig");
const test_hook = @import("test_hook.zig");

const log = std.log.scoped(.starh2_watchdog);

/// zio's executor type, which zio does not export by name.
const Executor = @typeInfo(@typeInfo(@TypeOf(zio.getCurrentExecutor)).@"fn".return_type.?).pointer.child;
const Activity = Executor.Activity;

/// Reports made, and the label of the last one (0: no label). Tests read
/// them; the log line is the product.
pub var test_reports: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_last_label: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// Watchdogs that did not start because their runtime had one.
pub var test_second_refused: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
/// In a test binary (`builtin.is_test`) with the test hooks on (every Debug
/// test), a report is logged as an error, and the test runner fails the test
/// it happens in: a test that holds an executor for the watchdog's limit (a
/// join, a blocking read or sleep on an executor thread, t-3899) fails,
/// wherever it is, with no grep to keep up to date. A test that stalls an
/// executor on purpose (`tests/watchdog.zig`) sets this false. Everything
/// else, a Debug application included, logs a warning.
pub var test_reports_fail: test_hook.Hook(std.atomic.Value(bool)) = if (test_hook.on) .init(true) else {};

// ---------------------------------------------------------------------------
// Labels: a server tag in the high half of the word, the route index plus
// one in the low half. 0 is no label.

const half = @bitSizeOf(usize) / 2;
const low_mask: usize = (@as(usize, 1) << half) - 1;

var next_tag: std.atomic.Value(usize) = .init(1);

/// A tag for a new `Server`, unique in the process while tags fit the high
/// half of a label.
pub fn newServerTag() usize {
    return next_tag.fetchAdd(1, .monotonic) & low_mask;
}

/// The label a handler runs under: its server's tag and its route's index
/// in that server's route table.
pub fn label(server_tag: usize, route: u32) usize {
    return (server_tag << half) | ((@as(usize, route) + 1) & low_mask);
}

fn labelTag(l: usize) usize {
    return l >> half;
}

fn labelRoute(l: usize) ?usize {
    const r = l & low_mask;
    return if (r == 0) null else r - 1;
}

// ---------------------------------------------------------------------------
// One watchdog per runtime: a small table of the runtimes that have one,
// under a spin lock held for a few loads and stores, by `start` and `stop`
// only.

const max_runtimes = 16;
var owners_lock: std.atomic.Value(bool) = .init(false);
var owners: [max_runtimes]?*const anyopaque = @splat(null);

fn lockOwners() void {
    while (owners_lock.swap(true, .acquire)) std.atomic.spinLoopHint();
}

fn unlockOwners() void {
    owners_lock.store(false, .release);
}

const Claim = enum { claimed, taken, full };

fn claimRuntime(rt: *const anyopaque) Claim {
    lockOwners();
    defer unlockOwners();
    for (owners) |o| if (o == rt) return .taken;
    for (&owners) |*o| if (o.* == null) {
        o.* = rt;
        return .claimed;
    };
    return .full;
}

fn releaseRuntime(rt: *const anyopaque) void {
    lockOwners();
    defer unlockOwners();
    for (&owners) |*o| if (o.* == rt) {
        o.* = null;
    };
}

// ---------------------------------------------------------------------------

/// What the thread last saw of one executor.
pub const Seen = struct {
    word: ?Word = null,
    /// When the first read that saw this word, not parked, returned.
    since_ns: u64 = 0,
    /// This stall was reported already.
    reported: bool = false,

    const Word = struct { generation: usize, parked: bool };
};

pub const Stall = struct { still_ns: u64, label: usize };

/// One read of one executor, at `now_ns` (taken right after the read). A
/// stall when this read and an earlier one at least `limit_ns` before it saw
/// the same word, not parked, and it was not reported yet.
pub fn observe(seen: *Seen, a: ?Activity, now_ns: u64, limit_ns: u64) ?Stall {
    const act = a orelse {
        // Transitions on every try: plainly making progress.
        seen.word = null;
        return null;
    };
    const word: Seen.Word = .{ .generation = act.generation, .parked = act.parked };
    if (act.parked or seen.word == null or !std.meta.eql(seen.word.?, word)) {
        seen.* = .{ .word = word, .since_ns = now_ns };
        return null;
    }
    const still_ns = now_ns -| seen.since_ns;
    if (seen.reported or still_ns < limit_ns) return null;
    seen.reported = true;
    return .{ .still_ns = still_ns, .label = act.label };
}

/// One pass: each executor is read, then the clock, so a slow pass (the
/// thread descheduled, a log write blocked) cannot backdate a later
/// executor's first sighting. `ctx` has `now() u64` and
/// `report(index, Stall)`.
pub fn scan(executors: anytype, seen: []Seen, limit_ns: u64, ctx: anytype) void {
    for (executors, seen, 0..) |exec, *s, i| {
        const a = readSettled(exec);
        const now_ns = ctx.now();
        if (observe(s, a, now_ns, limit_ns)) |stall| ctx.report(i, stall);
    }
}

/// A few tries: a read fails only while a transition happens during it.
fn readSettled(exec: anytype) ?Activity {
    for (0..8) |_| if (exec.readActivity()) |a| return a;
    return null;
}

pub const Watchdog = struct {
    /// Set to 1 to stop the thread; it waits on this word, so `stop` also
    /// wakes it.
    stop_flag: std.atomic.Value(u32) = .init(0),
    thread: ?std.Thread = null,
    seen: []Seen = &.{},
    runtime: ?*const anyopaque = null,

    /// Start the thread, unless `limit_ns` is 0 (off) or the calling
    /// executor's runtime already has one. Every executor of the runtime is
    /// watched.
    pub fn start(self: *Watchdog, gpa: std.mem.Allocator, limit_ns: u64, server_tag: usize, routes: []const router.Route) !void {
        if (limit_ns == 0) return;
        const rt = zio.getCurrentExecutor().runtime;
        switch (claimRuntime(rt)) {
            .claimed => {},
            .taken => {
                log.info("another server's watchdog already watches this runtime; it reports this server's stalls as server {d}, by route index", .{server_tag});
                if (comptime test_hook.on) _ = test_second_refused.fetchAdd(1, .acq_rel);
                return;
            },
            .full => {
                log.warn("{d} runtimes already have a watchdog; this runtime is not watched", .{max_runtimes});
                return;
            },
        }
        errdefer releaseRuntime(rt);
        const executors = rt.executors.items;
        self.seen = try gpa.alloc(Seen, executors.len);
        errdefer {
            gpa.free(self.seen);
            self.seen = &.{};
        }
        @memset(self.seen, .{});
        self.stop_flag.store(0, .release);
        self.thread = try std.Thread.spawn(.{}, run, .{ self, limit_ns, executors, Reporter{ .server_tag = server_tag, .routes = routes } });
        self.runtime = rt;
    }

    /// Stop and join the thread. Safe when it never started.
    pub fn stop(self: *Watchdog, gpa: std.mem.Allocator) void {
        const t = self.thread orelse return;
        self.stop_flag.store(1, .release);
        zio.os.Futex.wake(&self.stop_flag, .all);
        t.join();
        self.thread = null;
        gpa.free(self.seen);
        self.seen = &.{};
        releaseRuntime(self.runtime.?);
        self.runtime = null;
    }

    fn run(self: *Watchdog, limit_ns: u64, executors: []const *Executor, reporter: Reporter) void {
        const interval: zio.Duration = .fromNanoseconds(@max(limit_ns / 2, 1));
        while (self.stop_flag.load(.acquire) == 0) {
            zio.os.Futex.timedWait(&self.stop_flag, 0, interval) catch {};
            if (self.stop_flag.load(.acquire) != 0) return;
            scan(executors, self.seen, limit_ns, reporter);
        }
    }
};

const Reporter = struct {
    server_tag: usize,
    routes: []const router.Route,

    pub fn now(_: Reporter) u64 {
        return zio.Timestamp.now(.monotonic).toNanoseconds();
    }

    pub fn report(self: Reporter, index: usize, stall: Stall) void {
        const ms = stall.still_ns / std.time.ns_per_ms;
        const l = stall.label;
        const route = labelRoute(l);
        if (l == 0) {
            say("executor {d} made no scheduler progress for at least {d} ms; no label", .{ index, ms });
        } else if (labelTag(l) != self.server_tag) {
            say("executor {d} made no scheduler progress for at least {d} ms; a handler of server {d}, route index {?d}", .{ index, ms, labelTag(l), route });
        } else if (route != null and route.? < self.routes.len) {
            const r = self.routes[route.?];
            say("executor {d} made no scheduler progress for at least {d} ms; route {s} {s}", .{ index, ms, @tagName(r.method), r.path });
        } else {
            say("executor {d} made no scheduler progress for at least {d} ms; label {d}, not a route of this server", .{ index, ms, l });
        }
        if (comptime test_hook.on) {
            test_last_label.store(l, .release);
            _ = test_reports.fetchAdd(1, .acq_rel);
        }
    }
};

/// A warning; an error in a test binary that has not opted out
/// (`test_reports_fail`). `test_hook.on` alone is not enough: it is also on
/// in a Debug build of an application, which must not log its stalls as
/// errors.
fn say(comptime fmt: []const u8, args: anytype) void {
    if (comptime builtin.is_test and test_hook.on) {
        if (test_reports_fail.load(.acquire)) return log.err(fmt ++ " (an error in a test binary: a test must not hold an executor)", args);
    }
    log.warn(fmt, args);
}

test "a slow pass cannot backdate a stall: each executor's time is read after its own read (t-3760)" {
    // Two executors. The first one's read takes 900 ms (the watchdog thread
    // descheduled); the second shows the same word, not parked, in both
    // passes, which are 200 ms of real observation apart.
    const Fake = struct {
        clock: *u64,
        slow_ns: u64,
        act: Activity,
        fn readActivity(self: @This()) ?Activity {
            self.clock.* += self.slow_ns;
            return self.act;
        }
    };
    const Ctx = struct {
        clock: *u64,
        reports: *usize,
        pub fn now(self: @This()) u64 {
            return self.clock.*;
        }
        pub fn report(self: @This(), _: usize, _: Stall) void {
            self.reports.* += 1;
        }
    };
    var clock: u64 = 0;
    var reports: usize = 0;
    const busy: Activity = .{ .generation = 7, .parked = false, .label = 3 };
    var execs = [_]Fake{
        .{ .clock = &clock, .slow_ns = 900 * std.time.ns_per_ms, .act = .{ .generation = 1, .parked = true, .label = 0 } },
        .{ .clock = &clock, .slow_ns = 0, .act = busy },
    };
    var seen: [2]Seen = @splat(.{});
    const limit = 1000 * std.time.ns_per_ms;
    const ctx: Ctx = .{ .clock = &clock, .reports = &reports };
    scan(&execs, &seen, limit, ctx); // pass 1 starts at 0, sees executor 1 at 900 ms
    execs[0].slow_ns = 0;
    clock = 1100 * std.time.ns_per_ms; // pass 2: 200 ms after that sighting
    scan(&execs, &seen, limit, ctx);
    try std.testing.expectEqual(@as(usize, 0), reports);
    // A real stall is still reported, once.
    clock = 1900 * std.time.ns_per_ms;
    scan(&execs, &seen, limit, ctx);
    try std.testing.expectEqual(@as(usize, 1), reports);
    clock = 3000 * std.time.ns_per_ms;
    scan(&execs, &seen, limit, ctx);
    try std.testing.expectEqual(@as(usize, 1), reports);
}

test "labels carry the server tag and the route; 0 is no label (t-3760)" {
    const l = label(5, 0);
    try std.testing.expectEqual(@as(usize, 5), labelTag(l));
    try std.testing.expectEqual(@as(?usize, 0), labelRoute(l));
    try std.testing.expectEqual(@as(?usize, 41), labelRoute(label(5, 41)));
    try std.testing.expect(label(1, 0) != label(2, 0));
    try std.testing.expectEqual(@as(?usize, null), labelRoute(0));
}
