//! The only blocking sleeps test code may use (t-3885). `tools/std-io-gate`
//! fails the build on any other `nanosleep`, `clock_nanosleep`, `usleep` or
//! `Thread.sleep` under `src/` and `tests/`.
//!
//! A blocking sleep on a zio executor thread stops that executor's event loop
//! for its whole length: every task placed there waits, and so does any I/O
//! completion only that loop drives. t-3878 was exactly that: a test's
//! `nanosleep` on a task held a send's timer-cancel completion for 10 s and
//! looked like a server stall. So:
//!
//! - `threadSleepMs` / `threadSleepNs`: a wait on an OS thread (a client in a
//!   `std.Thread`, `zio.spawnBlocking` code, a test with no runtime). They
//!   panic on an executor thread, which names the defect where it is made. A
//!   task waits with `zio.sleep` instead.
//! - `holdExecutorMs`: the deliberate case, a test that must keep an
//!   executor busy (a slow inline handler, so input stays unread in the
//!   socket). It panics OFF an executor thread, so it cannot stand in for an
//!   ordinary sleep.
//!
//! An executor thread is one whose `Executor.current_DO_NOT_ACCESS_DIRECTLY`
//! is set: zio sets it when an executor starts on that thread (each worker,
//! and the thread that called `Runtime.init` for the main executor, until
//! `deinit`) and nowhere else, so `spawnBlocking` pool threads and plain
//! `std.Thread`s read null. zio reads it through `getCurrentExecutorOrNull`,
//! a `noinline` accessor it does not export; `onExecutorThread` is the same
//! read in a `noinline` function of ours, so the thread-local is looked up on
//! the thread that calls, never cached across a coroutine switch.
const std = @import("std");
const zio = @import("zio");

const Executor = @typeInfo(@typeInfo(@TypeOf(zio.getCurrentExecutor)).@"fn".return_type.?).pointer.child;

/// True on a thread that runs a zio executor now.
pub noinline fn onExecutorThread() bool {
    return Executor.current_DO_NOT_ACCESS_DIRECTLY != null;
}

/// Block this OS thread for `ms` milliseconds. Panics on an executor thread.
pub fn threadSleepMs(ms: u64) void {
    threadSleepNs(ms * std.time.ns_per_ms);
}

/// Block this OS thread for `ns` nanoseconds. Panics on an executor thread.
pub fn threadSleepNs(ns: u64) void {
    if (onExecutorThread()) @panic("test_sleep: a blocking sleep on a zio executor thread stops its event loop; a task waits with zio.sleep, or the wait moves to an OS thread (zio.spawnBlocking, std.Thread)");
    rawSleep(ns);
}

/// Keep this executor thread busy for `ms` milliseconds, on purpose. Panics
/// off an executor thread: there it would only be an ordinary sleep.
pub fn holdExecutorMs(ms: u64) void {
    if (!onExecutorThread()) @panic("test_sleep.holdExecutorMs: not on a zio executor thread; an OS thread waits with threadSleepMs");
    rawSleep(ms * std.time.ns_per_ms);
}

fn rawSleep(ns: u64) void {
    var t: std.c.timespec = .{ .sec = @intCast(ns / std.time.ns_per_s), .nsec = @intCast(ns % std.time.ns_per_s) };
    while (true) {
        var rem: std.c.timespec = undefined;
        const rc = std.c.nanosleep(&t, &rem);
        if (rc == 0 or std.c.errno(rc) != .INTR) return;
        t = rem;
    }
}
