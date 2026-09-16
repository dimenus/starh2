//! Isolated panic arms for t-1802. A panic inside `zig test` is a failed test,
//! so these run as a standalone process the build step expects to abort.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

pub fn main(init: std.process.Init.Minimal) void {
    var it = std.process.Args.Iterator.init(init.args);
    _ = it.next();
    const which = it.next() orelse {
        std.debug.print("usage: teardown-traps double-finalize | sweep-lock | unlocked-sweep\n", .{});
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
    if (std.mem.eql(u8, which, "unlocked-sweep")) {
        var handle = rt.spawn(starh2.edge.connection.testTrapUnlockedWakeHandlerWaiters, .{rt.io()}) catch std.process.exit(3);
        handle.join();
        std.process.exit(0);
    }
    std.debug.print("unknown trap {s}\n", .{which});
    std.process.exit(2);
}
