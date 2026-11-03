//! The TSan lane's canary. The lane (`./zb build tsan`) runs this binary
//! first and requires TSan to report the planted race below, naming
//! `tsan_canary.racer`. If it does not, the lane stops: a build that lost its
//! instrumentation (a module without `sanitize_thread`, a runtime that is not
//! linked, a suppression that matches everything) reports nothing, and
//! nothing looks exactly like clean.
//!
//! The race is between two zio tasks on two executor threads, so it also
//! checks that zio's own code paths run instrumented, not only this file.
//!
//! How the two writes are kept unordered: task 0 writes `shared` and raises
//! its flag; task 1 waits for that flag, writes `shared`, and raises its own;
//! task 0 waits for task 1's flag before it returns. The flags are
//! `.monotonic`, which gives TSan no happens-before edge, and no zio call runs
//! between the handshake steps. Each task waits on the other at some point, so
//! they cannot run one after the other on one executor (a fiber switch there
//! would order them); under work stealing the idle executor steals the second
//! task. The spins are bounded; if the second task never starts (one
//! executor, or no stealing), the test fails with `error.CanaryNoOverlap`
//! instead of hanging.
//!
//! Task 1 writes only after it has seen task 0's flag, so task 0's write is
//! already in TSan's shadow memory when task 1's write checks it. An earlier
//! version let both tasks write at once and TSan missed the race in 12 of 30
//! runs on nachos. The likely cause (not proven): two threads that update one
//! shadow cell at the same instant can each miss the other's record.
const std = @import("std");
const zio = @import("zio");

/// The planted race: two tasks write this with nothing ordering them.
var shared: u64 = 0;
var arrived: [2]std.atomic.Value(bool) = .{ .init(false), .init(false) };

/// About 10 s of spinning under TSan on nachos; a run that needs more than
/// that never overlapped.
const spin_limit: u64 = 200_000_000;

fn racer(me: usize) !void {
    if (me == 0) {
        shared = 1;
        arrived[0].store(true, .monotonic);
        try spinUntil(&arrived[1]);
    } else {
        try spinUntil(&arrived[0]);
        shared = 2;
        arrived[1].store(true, .monotonic);
    }
}

fn spinUntil(flag: *std.atomic.Value(bool)) !void {
    var spins: u64 = 0;
    while (!flag.load(.monotonic)) : (spins += 1) {
        if (spins == spin_limit) return error.CanaryNoOverlap;
        std.atomic.spinLoopHint();
    }
}

test "tsan canary: two zio tasks on two executors write one variable unordered" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var a = try rt.spawn(racer, .{0});
    var b = try rt.spawn(racer, .{1});
    const ra = a.join();
    const rb = b.join();
    try ra;
    try rb;
    std.debug.print("tsan canary: both racers wrote; shared={d}\n", .{shared});
}
