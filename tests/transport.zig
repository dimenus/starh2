const std = @import("std");
const starh2 = @import("starh2");

test "h2c preface matcher" {
    const h2c = starh2.edge.h2c;
    var m: h2c.PrefaceMatcher = .{};
    const preface = starh2.core.frame.CLIENT_PREFACE;
    const r1 = try m.feed(preface[0..5]);
    try std.testing.expect(!r1.done);
    const r2 = try m.feed(preface[5..]);
    try std.testing.expect(r2.done);
}

test "tls abi marker present" {
    try std.testing.expectEqual(@as(u32, 1), starh2.edge.tls_edge.starh2_nonblock_abi);
}

test "tls plaintext scratch includes TLS 1.3 inner content type" {
    try std.testing.expectEqual(
        @as(usize, 16 * 1024 + 1),
        starh2.core.wire_const.TLS_PLAINTEXT_SCRATCH_SIZE,
    );
}

test "tls decrypt drive presents at most one record" {
    const records = [_]u8{
        23, 3, 3, 0, 3, 1, 2, 3,
        23, 3, 3, 0, 2, 4, 5,
    };
    try std.testing.expectEqualSlices(u8, records[0..8], starh2.edge.tls_edge.firstRecord(&records));
    try std.testing.expectEqualSlices(u8, records[0..4], starh2.edge.tls_edge.firstRecord(records[0..4]));
    try std.testing.expectEqualSlices(u8, records[0..6], starh2.edge.tls_edge.firstRecord(records[0..6]));
}
