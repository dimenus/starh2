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
    // Drain-turn packing uses every byte except that content-type slot.
    try std.testing.expectEqual(
        starh2.core.wire_const.TLS_PLAINTEXT_SCRATCH_SIZE - 1,
        starh2.edge.emit_batch.max_plaintext,
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

test "wire completion can release several control-pool entries" {
    // One TLS plaintext can carry several HEADERS. A bool here would under-release
    // the pool and stall later ordinary controls on a live connection.
    const chunk = starh2.edge.wire_pump.WireChunk{
        .control_release = 90,
        .control_entries = 3,
    };
    const ack = starh2.edge.wire_pump.WriteCompletion{
        .control_release = chunk.control_release,
        .control_entries = chunk.control_entries,
    };
    try std.testing.expectEqual(@as(u32, 3), ack.control_entries);
    try std.testing.expectEqual(@as(usize, 90), ack.control_release);
}

test "emit batch packing is the production drain-turn rule" {
    // tests/transport.zig is the external consumer of the public edge surface.
    // The adversarial cases live next to EmitBatch so a nested sink rewrite
    // cannot hide them; this import is the canary that they still run in ci.
    const batch = starh2.edge.emit_batch;
    try std.testing.expect(!batch.frameIsBatchable(false, 0, 40, batch.max_plaintext));
    try std.testing.expect(!batch.frameJoinsInProgress(false, 0, 40, batch.max_plaintext, 0, false));
    try std.testing.expect(!batch.frameJoinsInProgress(false, 0, 40, batch.max_plaintext, 10, false));
    try std.testing.expect(batch.frameJoinsInProgress(false, 0, 40, batch.max_plaintext, 0, true));
    var storage: [64]u8 = undefined;
    var emit: batch.EmitBatch = .{ .buf = &storage };
    emit.copyFrame("H1", true, 8, true);
    emit.copyFrame("D1", true, 0, false);
    emit.copyFrame("H2", true, 8, true);
    try std.testing.expectEqual(@as(u32, 2), emit.control_entries);
    try std.testing.expectEqualStrings("H1D1H2", emit.buf[0..emit.len]);
}
