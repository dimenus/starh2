//! Isolated panic arms for t-1802. A panic inside `zig test` is a failed test,
//! so these run as a standalone process the build step expects to abort.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

pub fn main(init: std.process.Init.Minimal) void {
    var it = std.process.Args.Iterator.init(init.args);
    _ = it.next();
    const which = it.next() orelse {
        std.debug.print("usage: teardown-traps double-finalize | sweep-lock | sweep-lock-session | unlocked-sweep | watchdog-stall | watchdog-healthy | conservation | deinit-live | stale-sid | error-path-freeze\n", .{});
        std.process.exit(2);
    };
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
    if (std.mem.eql(u8, which, "watchdog-stall")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapWatchdogNoProgress, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    if (std.mem.eql(u8, which, "watchdog-healthy")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapWatchdogHealthyProgress, .{rt.io()}) catch std.process.exit(3);
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
    if (std.mem.eql(u8, which, "stale-sid")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapStaleTeardownSid, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    if (std.mem.eql(u8, which, "error-path-freeze")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapErrorPathFreeze, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    std.debug.print("unknown trap {s}\n", .{which});
    std.process.exit(2);
}
