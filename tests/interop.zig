const std = @import("std");
const starh2 = @import("starh2");

test "huffman table is 257 entries with RFC EOS" {
    try std.testing.expectEqual(@as(usize, 257), starh2.core.hpack.huffmanTableLen());
    const eos = starh2.core.hpack.huffmanEos();
    try std.testing.expectEqual(@as(u32, 0x3fffffff), eos.code);
    try std.testing.expectEqual(@as(u8, 30), eos.bits);
    // RFC 7541 Appendix B fixed points
    const zero = starh2.core.hpack.huffmanPair('0');
    try std.testing.expectEqual(@as(u32, 0x0), zero.code);
    try std.testing.expectEqual(@as(u8, 5), zero.bits);
    const sp = starh2.core.hpack.huffmanPair(' ');
    try std.testing.expectEqual(@as(u32, 0x14), sp.code);
    try std.testing.expectEqual(@as(u8, 6), sp.bits);
}

test "decode every Go x/net Huffman single-byte encoding" {
    const blob = @embedFile("go_huffman_bytes.txt");
    var it = std.mem.splitScalar(u8, blob, '\n');
    const count_line = it.next() orelse return error.BadOracle;
    try std.testing.expectEqualStrings("257", count_line);
    var sym: usize = 0;
    while (sym < 256) : (sym += 1) {
        const hex = it.next() orelse return error.BadOracle;
        const nbytes = hex.len / 2;
        const buf = try std.testing.allocator.alloc(u8, nbytes);
        defer std.testing.allocator.free(buf);
        _ = try std.fmt.hexToBytes(buf, hex);
        const out = try starh2.core.hpack.decodeHuffman(std.testing.allocator, buf);
        defer std.testing.allocator.free(out);
        try std.testing.expectEqual(@as(usize, 1), out.len);
        try std.testing.expectEqual(@as(u8, @intCast(sym)), out[0]);
    }
}

test "frame parser rejects oversized length" {
    var parser = starh2.core.frame.Parser.init(std.testing.allocator, 16_384);
    defer parser.deinit();
    parser.skipPreface();
    const bad = [_]u8{ 0x00, 0x40, 0x01, 0x0, 0, 0, 0, 0, 1 };
    try std.testing.expectError(error.FrameSizeError, parser.ingestOne(&bad));
}
