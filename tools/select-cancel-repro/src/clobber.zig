//! Reproducer: `zio.select`'s registration fast path drops a concurrently
//! claimed item on an earlier-registered branch.
//!
//! Shape: branch `.a` (channel A, empty) registers first; branch `.b`
//! (channel B, item ALREADY buffered) hits the fast path and does
//! `winner.store(b)` — a blind store. A sender that claims the `.a` waiter
//! in that window (claim CAS succeeds while winner is still NO_WINNER)
//! has already copied its item into the select's stack context; the store
//! clobbers the claim and the item vanishes.
//!
//! Conservation law: after the select returns, the item sent to A is
//! EITHER still in channel A OR was returned by the select. Exit 1 with a
//! DROP line on the first vanished item.
const std = @import("std");
const zio = @import("zio");

const Shared = struct {
    ch_a: *zio.Channel(u64),
    go: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
};

fn sender(sh: *Shared) !void {
    while (!sh.go.load(.acquire)) {
        std.atomic.spinLoopHint();
    }
    sh.ch_a.trySend(1) catch {};
    sh.done.store(true, .release);
}

fn run(rt: *zio.Runtime, iterations: u64) !void {
    var buf_a: [4]u64 = undefined;
    var buf_b: [4]u64 = undefined;
    var i: u64 = 0;
    while (i < iterations) : (i += 1) {
        var ch_a = zio.Channel(u64).init(&buf_a);
        var ch_b = zio.Channel(u64).init(&buf_b);
        // Branch .b is ready before the select starts: the fast path fires.
        try ch_b.trySend(7);
        var sh = Shared{ .ch_a = &ch_a };

        var s = try rt.spawn(sender, .{&sh});
        // Release the sender as close to the select's registration window
        // as possible.
        sh.go.store(true, .release);
        var got_a: u64 = 0;
        const winner = try zio.select(.{
            .a = ch_a.asyncReceive(),
            .b = ch_b.asyncReceive(),
        });
        switch (winner) {
            .a => |r| {
                got_a = r catch 0;
            },
            .b => |r| {
                _ = r catch 0;
            },
        }
        s.join() catch {};

        if (sh.done.load(.acquire)) {
            const in_a: u64 = ch_a.tryReceive() catch 0;
            if (in_a == 0 and got_a == 0) {
                std.debug.print("DROP at iteration {d}: A's item neither in channel nor returned\n", .{i});
                std.process.exit(1);
            }
        }
    }
    std.debug.print("no drop in {d} iterations\n", .{iterations});
}

pub fn main() !void {
    var gpa_state: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa_state.deinit();
    const gpa = gpa_state.allocator();

    const rt = try zio.Runtime.init(gpa, .{});
    defer rt.deinit();
    var handle = try rt.spawn(run, .{ rt, 2_000_000 });
    try handle.join();
}
