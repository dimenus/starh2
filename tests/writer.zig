//! Writer ticket ownership, flush semantics, and failure/reuse loops.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

test "ticket table: 100 reserve/fail/release reuse cycles" {
    const ticket_table = starh2.edge.ticket_table;
    var storage: [8]ticket_table.TicketWait = undefined;
    var table = ticket_table.TicketTable.init(&storage);
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        const a = try table.reserve();
        const b = try table.reserve();
        table.complete(a[1], a[0], i % 2 == 0);
        if (i % 2 == 0) {
            try table.wait(a[1], null);
        } else {
            try std.testing.expectError(error.WriteFailed, table.wait(a[1], null));
        }
        table.releaseReserved(b[1]);
    }
    var n: usize = 0;
    while (n < storage.len) : (n += 1) {
        _ = try table.reserve();
    }
    try std.testing.expectError(error.OutOfMemory, table.reserve());
}

test "ticket table: failAll wakes every waiter" {
    const ticket_table = starh2.edge.ticket_table;
    var storage: [4]ticket_table.TicketWait = undefined;
    var table = ticket_table.TicketTable.init(&storage);
    const a = try table.reserve();
    const b = try table.reserve();
    table.failAll();
    try std.testing.expectError(error.WriteFailed, table.wait(a[1], null));
    try std.testing.expectError(error.WriteFailed, table.wait(b[1], null));
    try std.testing.expectError(error.WriteFailed, table.reserve());
}

test "WriteCompletion is integer-only payload" {
    const W = starh2.edge.wire_pump.WriteCompletion;
    try std.testing.expect(@sizeOf(W) <= 64);
    try std.testing.expectEqual(u64, @TypeOf(@as(W, undefined).ticket));
    try std.testing.expectEqual(u32, @TypeOf(@as(W, undefined).ticket_slot));
    try std.testing.expectEqual(usize, @TypeOf(@as(W, undefined).outbound_release));
}

test "100x failAll vs reserve never hangs" {
    const ticket_table = starh2.edge.ticket_table;
    var storage: [8]ticket_table.TicketWait = undefined;
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        // Fresh table each round — never reset live atomics with .{} concurrently.
        var table = ticket_table.TicketTable.init(&storage);
        const a = table.reserve() catch continue;
        table.failAll();
        _ = table.reserve() catch {};
        table.wait(a[1], null) catch {};
    }
}

const BarrierCtx = struct {
    table: *starh2.edge.ticket_table.TicketTable,
    barrier: *starh2.edge.ticket_table.TestReserveBarrier,
    result: std.atomic.Value(u8) = .init(0), // 1=WriteFailed, 2=ok, 3=other
};

fn reserveTask(ctx: *BarrierCtx) void {
    if (ctx.table.reserve()) |_| {
        ctx.result.store(2, .release);
    } else |err| switch (err) {
        error.WriteFailed => ctx.result.store(1, .release),
        error.OutOfMemory => ctx.result.store(3, .release),
    }
}

fn adversaryTask(ctx: *BarrierCtx) void {
    // Wait until reserve passed precheck, then failAll at adversarial point, release gates.
    ctx.barrier.after_precheck.wait() catch {};
    ctx.table.failAll();
    ctx.barrier.go_claim.post();
    // Always release finish — reserve may return WriteFailed before claim (no after_claim).
    ctx.barrier.go_finish.post();
}

fn runTwoTaskBarrier(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    _ = gpa;
    const ticket_table = starh2.edge.ticket_table;
    var storage: [2]ticket_table.TicketWait = undefined;
    var table = ticket_table.TicketTable.init(&storage);
    var barrier: ticket_table.TestReserveBarrier = .{};
    ticket_table.test_reserve_barrier = &barrier;
    defer ticket_table.test_reserve_barrier = null;

    var ctx: BarrierCtx = .{ .table = &table, .barrier = &barrier };
    var h1 = try rt.spawn(reserveTask, .{&ctx});
    var h2 = try rt.spawn(adversaryTask, .{&ctx});
    h1.join();
    h2.join();

    try std.testing.expectEqual(@as(u8, 1), ctx.result.load(.acquire));
    // Bounded completion: late reserve also WriteFailed.
    try std.testing.expectError(error.WriteFailed, table.reserve());
}

test "two-task barrier: failAll at preclaim/postclaim returns WriteFailed" {
    var dbg = std.heap.DebugAllocator(.{}).init;
    defer {
        const status = dbg.deinit();
        if (status != .ok) @panic("DebugAllocator leak in ticket barrier test");
    }
    const gpa = dbg.allocator();
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024,
            .max_unused_stacks = 8,
            .max_age = .fromSeconds(5),
        },
        .executors = .exact(2),
        .enable_task_migration = true,
    });
    defer rt.deinit();
    var handle = try rt.spawn(runTwoTaskBarrier, .{ rt, gpa });
    try handle.join();
}
