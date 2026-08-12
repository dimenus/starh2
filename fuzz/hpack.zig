const std = @import("std");
const starh2 = @import("starh2");

fn testOne(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    var buf: [4096]u8 = undefined;
    const n = smith.slice(&buf);
    var dec = starh2.core.hpack.Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const res = dec.decode(buf[0..n], 100, 32 * 1024, 256, 8 * 1024) catch return;
    dec.freeResult(res);
}

test "fuzz hpack" {
    try std.testing.fuzz({}, testOne, .{
        .corpus = &.{
            &[_]u8{0x82}, // :method GET
            &[_]u8{ 0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79, 0x0c, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72 },
            &[_]u8{ 0x40, 0x01, 'x', 0x01, 'y', 0xbe }, // insert then dynamic index 62
            &[_]u8{ 0x00, 0x01, 'n', 0x01, 'v' }, // literal without indexing
            &[_]u8{ 0x10, 0x01, 's', 0x01, 'v' }, // literal never indexed
            &[_]u8{ 0x20, 0x40, 0x01, 'x', 0x01, 'y' }, // size zero then non-retained insert
            &[_]u8{ 0x01, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff }, // RFC Huffman
            &[_]u8{0xff}, // bad index
            &[_]u8{ 0x00, 0x81, 0xff }, // bad huffman
        },
    });
}

test "mutation canary invalid index" {
    var dec = starh2.core.hpack.Decoder.init(std.testing.allocator);
    defer dec.deinit();
    try std.testing.expectError(error.CompressionError, dec.decode(&[_]u8{0x80}, 100, 32 * 1024, 256, 8 * 1024));
}

test "mutation canary invalid Huffman padding" {
    try std.testing.expectError(
        error.CompressionError,
        starh2.core.hpack.decodeHuffman(std.testing.allocator, &[_]u8{0xff}),
    );
}
