//! Isolated panic arms for t-1802. A panic inside `zig test` is a failed test,
//! so these run as a standalone process the build step expects to abort.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const no_core = @import("no_core.zig");

pub fn main(init: std.process.Init.Minimal) void {
    var it = std.process.Args.Iterator.init(init.args);
    _ = it.next();
    const which = it.next() orelse {
        std.debug.print("usage: teardown-traps double-finalize | sweep-lock | sweep-lock-session | unlocked-sweep | conservation | deinit-live | error-path-freeze | r158-parked\n", .{});
        std.process.exit(2);
    };
    // The cases that end in a panic, which is what `build.zig` checks (their
    // stderr, no exit code): they write no core. The others exit 0.
    const dying = [_][]const u8{ "double-finalize", "sweep-lock", "sweep-lock-session", "unlocked-sweep", "conservation", "deinit-live" };
    for (dying) |d| {
        if (std.mem.eql(u8, which, d)) no_core.expectDeath();
    }
    const gpa = std.heap.page_allocator;
    const rt = zio.Runtime.init(gpa, .{}) catch std.process.exit(3);
    if (std.mem.eql(u8, which, "double-finalize")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapDoubleFinalize, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    if (std.mem.eql(u8, which, "sweep-lock")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapLockDuringShutdownSweep, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    if (std.mem.eql(u8, which, "sweep-lock-session")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapLockSessionDuringShutdownSweep, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    if (std.mem.eql(u8, which, "unlocked-sweep")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapUnlockedWakeHandlerWaiters, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    if (std.mem.eql(u8, which, "conservation")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapConservation, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    if (std.mem.eql(u8, which, "deinit-live")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapDeinitLive, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    if (std.mem.eql(u8, which, "error-path-freeze")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapErrorPathFreeze, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    if (std.mem.eql(u8, which, "r158-parked")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapR158Parked, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    std.debug.print("unknown trap {s}\n", .{which});
    std.process.exit(2);
}
