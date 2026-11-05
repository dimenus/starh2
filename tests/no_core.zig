//! A test process case that is MEANT to die (a panic, an abort: what the
//! build step checks is its stderr) calls `expectDeath` before it runs, so
//! its death writes no core dump. On nachos every core dump opens a crash
//! agent's window, and these deaths are the test's purpose (t-3882).
//!
//! Only the dying case calls it: the same binary's other cases keep the
//! limits they were started with, so a real crash in them still leaves its
//! core. A case left off a binary's list dumps (noise), it is never silenced.
//!
//! `tools/zio-dst` imports this file too (its `build.zig`): its D4 children
//! and the `--run` children of `--arm-mutant` die on purpose.
const std = @import("std");
const builtin = @import("builtin");

pub fn expectDeath() void {
    // Every POSIX system: a core of at most 0 bytes is no core.
    std.posix.setrlimit(.CORE, .{ .cur = 0, .max = 0 }) catch |err| {
        std.debug.print("no_core: setrlimit(RLIMIT_CORE, 0) failed ({s}); this death may write a core\n", .{@errorName(err)});
    };
    // Linux: with `core_pattern` piped to systemd-coredump the kernel still
    // hands the dump to the helper, which records it; a process that is not
    // dumpable is not dumped at all.
    if (builtin.os.tag == .linux) {
        const rc = std.os.linux.prctl(@intFromEnum(std.os.linux.PR.SET_DUMPABLE), 0, 0, 0, 0);
        if (std.os.linux.errno(rc) != .SUCCESS) std.debug.print("no_core: prctl(PR_SET_DUMPABLE, 0) failed; this death may write a core\n", .{});
    }
}
