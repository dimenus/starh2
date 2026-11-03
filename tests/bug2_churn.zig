//! Bug 2's zio-only churn test, run by the TSan lane (`./zb build tsan`).
//!
//! zio `registerTask` once read `task.runtime` after `Executor.scheduleTask`
//! had published the task (lalinsky/zio#875). A group task holds no reference
//! for the spawner, so another executor could run it to the end and free it
//! in between. dimenus/zio `2f727e3` fixed it; this test guards the fix.
//!
//! One spawner task puts `per_round` group tasks on a 2-executor runtime.
//! Each task's context is 640 bytes (the size of starh2's `connEntry`
//! arguments), so its closure is larger than `TaskPool.pool_item_size` and
//! comes from the runtime allocator, as a connection task's does. Each task
//! returns at once.
//!
//! Without TSan the read of freed memory almost never faults, because the
//! freed bucket usually stays mapped. Under TSan the read conflicts with the
//! Debug-mode memset in `std.mem.Allocator.free`, so the bad order is
//! reported every time it happens, crash or not. Against zio `182b0f6` (before
//! the fix) TSan reports it in every run of this test; against `2f727e3` it
//! does not.
const std = @import("std");
const zio = @import("zio");

const per_round = 200_000;
const rounds = 5;

const Big = struct { pad: [640]u8 };
var finished: std.atomic.Value(u64) = .init(0);

fn quick(b: Big) void {
    std.mem.doNotOptimizeAway(b.pad[0]);
    _ = finished.fetchAdd(1, .monotonic);
}

fn churn(n: usize) !void {
    var group: zio.Group = .init;
    defer group.cancel();
    var big: Big = undefined;
    @memset(&big.pad, 7);
    for (0..n) |_| try group.spawn(quick, .{big});
    try group.wait();
}

test "bug2: group tasks that finish on the other executor while the spawner is in registerTask" {
    const gpa = std.testing.allocator;
    for (0..rounds) |r| {
        const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
        defer rt.deinit();
        const before = finished.load(.monotonic);
        var h = try rt.spawn(churn, .{per_round});
        try h.join();
        std.debug.print("bug2 churn round {d}: {d} group tasks spawned and finished\n", .{ r, finished.load(.monotonic) - before });
    }
}
