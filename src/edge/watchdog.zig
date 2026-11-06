//! The executor watchdog (t-3760): one OS thread per serving `Server` that
//! reports an executor making no scheduler progress, and never preempts it.
//!
//! zio publishes, per executor, a word that changes at every task switch and
//! at every park entry and exit, with a parked bit, and the label of the task
//! it runs (`Executor.readActivity`). Handlers run labelled with their route
//! (`setTaskLabel(route + 1)` around every handler call). Every
//! `Limits.executor_stall_report_ns / 2` this thread reads each executor. An
//! executor that is not parked, and whose word has not changed across at
//! least the limit, is reported once per stall:
//!
//!     executor 1 made no scheduler progress for at least 1500 ms; route GET /spin
//!
//! "No scheduler progress" is what was observed: one task, or the scheduler
//! itself, held the executor's thread the whole time. It may be a loop, a
//! blocking call, or very long CPU work; the report does not say which. The
//! label is read from the executor and mapped to the route table here; no
//! task, request or handler pointer is ever read.
//!
//! Lifetime: `start` runs in `Server.serve` before any accept loop, and
//! `stop` (which joins) runs when `serve` returns, after every handler has
//! ended, so the thread never outlives the route table or the runtime.
const std = @import("std");
const zio = @import("zio");
const router = @import("../http/router.zig");
const test_hook = @import("test_hook.zig");

const log = std.log.scoped(.starh2_watchdog);

/// zio's executor type, which zio does not export by name.
const Executor = @typeInfo(@typeInfo(@TypeOf(zio.getCurrentExecutor)).@"fn".return_type.?).pointer.child;

/// Reports made, and the label of the last one (0: no label). Tests read
/// them; the log line is the product.
pub var test_reports: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};
pub var test_last_label: test_hook.Hook(std.atomic.Value(usize)) = if (test_hook.on) .init(0) else {};

pub const Watchdog = struct {
    /// Set to 1 to stop the thread; it waits on this word, so `stop` also
    /// wakes it.
    stop_flag: std.atomic.Value(u32) = .init(0),
    thread: ?std.Thread = null,
    seen: []Seen = &.{},

    /// What the thread last saw of one executor.
    const Seen = struct {
        word: ?Word = null,
        /// When this word was first seen, not parked.
        since_ns: u64 = 0,
        /// This stall was reported already.
        reported: bool = false,
    };

    const Word = struct { generation: usize, parked: bool };

    /// Start the thread, unless `limit_ns` is 0 (off). On the calling
    /// executor's runtime: every executor of it is watched.
    pub fn start(self: *Watchdog, gpa: std.mem.Allocator, limit_ns: u64, routes: []const router.Route) !void {
        if (limit_ns == 0) return;
        const executors = zio.getCurrentExecutor().runtime.executors.items;
        self.seen = try gpa.alloc(Seen, executors.len);
        errdefer {
            gpa.free(self.seen);
            self.seen = &.{};
        }
        @memset(self.seen, .{});
        self.stop_flag.store(0, .release);
        self.thread = try std.Thread.spawn(.{}, run, .{ self, limit_ns, executors, routes });
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
    }

    fn run(self: *Watchdog, limit_ns: u64, executors: []const *Executor, routes: []const router.Route) void {
        const interval: zio.Duration = .fromNanoseconds(@max(limit_ns / 2, 1));
        while (self.stop_flag.load(.acquire) == 0) {
            zio.os.Futex.timedWait(&self.stop_flag, 0, interval) catch {};
            if (self.stop_flag.load(.acquire) != 0) return;
            const now = monoNs();
            for (executors, self.seen, 0..) |exec, *seen, i| check(exec, seen, i, now, limit_ns, routes);
        }
    }

    fn check(exec: *const Executor, seen: *Seen, index: usize, now: u64, limit_ns: u64, routes: []const router.Route) void {
        const a = readSettled(exec) orelse {
            // Transitions on every try: plainly making progress.
            seen.word = null;
            return;
        };
        const word: Word = .{ .generation = a.generation, .parked = a.parked };
        if (a.parked or seen.word == null or !std.meta.eql(seen.word.?, word)) {
            seen.* = .{ .word = word, .since_ns = now };
            return;
        }
        const still_ns = now -| seen.since_ns;
        if (seen.reported or still_ns < limit_ns) return;
        seen.reported = true;
        report(index, still_ns, a.label, routes);
    }

    /// A few tries: a read fails only while a transition happens during it.
    fn readSettled(exec: *const Executor) ?Executor.Activity {
        for (0..8) |_| if (exec.readActivity()) |a| return a;
        return null;
    }

    fn report(index: usize, still_ns: u64, label: usize, routes: []const router.Route) void {
        const ms = still_ns / std.time.ns_per_ms;
        if (label == 0) {
            log.warn("executor {d} made no scheduler progress for at least {d} ms; no label", .{ index, ms });
        } else if (label - 1 < routes.len) {
            const r = routes[label - 1];
            log.warn("executor {d} made no scheduler progress for at least {d} ms; route {s} {s}", .{ index, ms, @tagName(r.method), r.path });
        } else {
            log.warn("executor {d} made no scheduler progress for at least {d} ms; label {d}, not a route of this server", .{ index, ms, label });
        }
        if (comptime test_hook.on) {
            test_last_label.store(label, .release);
            _ = test_reports.fetchAdd(1, .acq_rel);
        }
    }
};

/// The label a handler runs under: its route's index in the server's route
/// table, plus one (0 is no label).
pub fn routeLabel(route: u32) usize {
    return @as(usize, route) + 1;
}

fn monoNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}
