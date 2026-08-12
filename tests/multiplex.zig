const std = @import("std");
const starh2 = @import("starh2");

test "flow windows isolate streams" {
    const flow = starh2.core.flow;
    var a: flow.StreamWindow = .{};
    const b: flow.StreamWindow = .{};
    try a.debitRecv(1000);
    try std.testing.expect(b.recv == flow.INITIAL_WINDOW);
}
