//! Reproducer: `zio.select` over a channel drops the claimed item when the
//! selecting task is canceled while a sender's claim is in flight.
//!
//! The conservation law under test: after the waiter task ends, the sent
//! item is EITHER still in the channel OR recorded as received by the
//! waiter. A vanished item is the drop.
//!
//! `Channel.receive` itself keeps the law: on cancellation it absorbs the
//! in-flight signal and RETURNS THE ITEM instead of `error.Canceled`.
//! `zio.select`'s cleanup absorbs the same signal and then returns the
//! error anyway, so the copied item dies on the dead select frame.
//!
//! Exit 1 with a DROP line on the first vanished item; exit 0 after the
//! iteration budget with every item accounted for.
const std = @import("std");
const zio = @import("zio");

var never_set: zio.ResetEvent = .init;

const Shared = struct {
    ch: *zio.Channel(u64),
    received: std.atomic.Value(u64) = .init(0),
    canceled: std.atomic.Value(bool) = .init(false),
    parked: std.atomic.Value(bool) = .init(false),
};

fn waiter(sh: *Shared) !void {
    sh.parked.store(true, .release);
    const winner = zio.select(.{
        .item = sh.ch.asyncReceive(),
        .never = &never_set,
    }) catch {
        sh.canceled.store(true, .release);
        return;
    };
    switch (winner) {
        .item => |r| {
            const v = r catch return;
            sh.received.store(v, .release);
        },
        .never => unreachable,
    }
}

fn sender(sh: *Shared) !void {
    sh.ch.trySend(1) catch {};
}

fn run(rt: *zio.Runtime, iterations: u64) !void {
    var buf: [4]u64 = undefined;
    var i: u64 = 0;
    var drops: u64 = 0;
    while (i < iterations) : (i += 1) {
        var ch = zio.Channel(u64).init(&buf);
        var sh = Shared{ .ch = &ch };

        var w = try rt.spawn(waiter, .{&sh});
        // Let the waiter reach the select and park.
        while (!sh.parked.load(.acquire)) try zio.yield();
        try zio.yield();

        // Race the send against the cancel from two sides.
        var s = try rt.spawn(sender, .{&sh});
        w.cancel();
        w.join() catch {};
        s.join() catch {};

        const in_channel: u64 = ch.tryReceive() catch 0;
        const got = sh.received.load(.acquire);
        if (in_channel == 0 and got == 0) {
            drops += 1;
            std.debug.print(
                "DROP at iteration {d}: item neither in channel nor received (waiter canceled={})\n",
                .{ i, sh.canceled.load(.acquire) },
            );
            std.process.exit(1);
        }
    }
    std.debug.print("no drop in {d} iterations\n", .{iterations});
}

pub fn main() !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    const iterations: u64 = 200_000;

    const rt = try zio.Runtime.init(gpa, .{});
    defer rt.deinit();
    var handle = try rt.spawn(run, .{ rt, iterations });
    try handle.join();
}
