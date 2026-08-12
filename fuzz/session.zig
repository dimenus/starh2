const std = @import("std");
const starh2 = @import("starh2");

fn freeIntents(session: *starh2.core.session.Session, intents: []starh2.core.session.Intent) void {
    for (intents) |*it| {
        session.releaseIntent(it);
    }
    // drainIntents returns boot scratch — do not free the slice
}

fn expectGoaway(session: *const starh2.core.session.Session, code: starh2.core.frame.ErrorCode) !void {
    switch (session.terminal) {
        .goaway => |g| try std.testing.expectEqual(code, g.code),
        else => return error.ExpectedGoaway,
    }
}

fn testOne(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    var buf: [4096]u8 = undefined;
    const n = smith.slice(&buf);
    var session = try starh2.core.session.Session.init(std.testing.allocator, .defaults);
    defer {
        freeIntents(&session, session.drainIntents());
        session.deinit();
    }
    freeIntents(&session, session.drainIntents());
    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(std.testing.allocator);
    try input.appendSlice(std.testing.allocator, starh2.core.frame.CLIENT_PREFACE);
    // Keep arbitrary bytes in the post-preface state; first-frame enforcement
    // has a dedicated mutation canary below.
    try input.appendSlice(std.testing.allocator, &[_]u8{ 0, 0, 0, 0x4, 0, 0, 0, 0, 0 });
    try input.appendSlice(std.testing.allocator, buf[0..n]);
    var steps: usize = 0;
    const max = 2 * input.items.len + 1024;
    var off: usize = 0;
    while (off < input.items.len and steps < max) : (steps += 1) {
        const take = @min(1 + (steps % 17), input.items.len - off);
        try session.ingest(input.items[off .. off + take]);
        freeIntents(&session, session.drainIntents());
        off += take;
        if (session.terminal != .none) break;
    }
}

test "fuzz session" {
    try std.testing.fuzz({}, testOne, .{
        .corpus = &.{
            &[_]u8{ 0, 0, 0, 0x4, 0, 0, 0, 0, 0 },
            &[_]u8{ 0, 0, 8, 0x6, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8 },
            &[_]u8{ 0, 0, 4, 0x3, 0, 0, 0, 0, 1, 0, 0, 0, 8 },
            &[_]u8{ 0, 0, 4, 0x8, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
            &[_]u8{ 0, 0, 4, 0x1, 0x4, 0, 0, 0, 1, 0x82, 0x86, 0x84, 0x81, 0, 0, 1, 0x0, 0x1, 0, 0, 0, 1, 'x' },
            &[_]u8{ 0, 0, 8, 0x7, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0 },
        },
    });
}

test "mutation canary illegal stream" {
    var session = try starh2.core.session.Session.init(std.testing.allocator, .defaults);
    defer {
        freeIntents(&session, session.drainIntents());
        session.deinit();
    }
    freeIntents(&session, session.drainIntents());
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try wire.appendSlice(std.testing.allocator, starh2.core.frame.CLIENT_PREFACE);
    try wire.appendSlice(std.testing.allocator, &[_]u8{ 0, 0, 0, 0x4, 0, 0, 0, 0, 0 });
    try wire.appendSlice(std.testing.allocator, &[_]u8{ 0, 0, 0, 0x0, 0x1, 0, 0, 0, 0 });
    try session.ingest(wire.items);
    try std.testing.expect(session.terminal == .goaway);
}

test "mutation canary requires SETTINGS as first peer frame" {
    var session = try starh2.core.session.Session.init(std.testing.allocator, .defaults);
    defer {
        freeIntents(&session, session.drainIntents());
        session.deinit();
    }
    freeIntents(&session, session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try wire.appendSlice(std.testing.allocator, starh2.core.frame.CLIENT_PREFACE);
    try wire.appendSlice(std.testing.allocator, &[_]u8{ 0, 0, 0, 0x1, 0x5, 0, 0, 0, 1 });
    try session.ingest(wire.items);
    try expectGoaway(&session, .protocol_error);
}

test "mutation canary requires CONTINUATION without interleaving" {
    var session = try starh2.core.session.Session.init(std.testing.allocator, .defaults);
    defer {
        freeIntents(&session, session.drainIntents());
        session.deinit();
    }
    freeIntents(&session, session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try wire.appendSlice(std.testing.allocator, starh2.core.frame.CLIENT_PREFACE);
    try wire.appendSlice(std.testing.allocator, &[_]u8{ 0, 0, 0, 0x4, 0, 0, 0, 0, 0 });
    try wire.appendSlice(std.testing.allocator, &[_]u8{ 0, 0, 1, 0x1, 0x1, 0, 0, 0, 1, 0x82 });
    try wire.appendSlice(std.testing.allocator, &[_]u8{ 0, 0, 8, 0x6, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8 });
    try session.ingest(wire.items);
    try expectGoaway(&session, .protocol_error);
}

test "mutation canary rejects connection flow-window overflow" {
    var session = try starh2.core.session.Session.init(std.testing.allocator, .defaults);
    defer {
        freeIntents(&session, session.drainIntents());
        session.deinit();
    }
    freeIntents(&session, session.drainIntents());

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try wire.appendSlice(std.testing.allocator, starh2.core.frame.CLIENT_PREFACE);
    try wire.appendSlice(std.testing.allocator, &[_]u8{ 0, 0, 0, 0x4, 0, 0, 0, 0, 0 });
    try wire.appendSlice(std.testing.allocator, &[_]u8{ 0, 0, 4, 0x8, 0, 0, 0, 0, 0, 0x7f, 0xff, 0xff, 0xff });
    try session.ingest(wire.items);
    try expectGoaway(&session, .flow_control_error);
}
