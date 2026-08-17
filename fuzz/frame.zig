const std = @import("std");
const starh2 = @import("starh2");

fn testOne(_: void, smith: *std.testing.Smith) !void {
    @disableInstrumentation();
    var generated: [4096]u8 = undefined;
    const n = smith.slice(&generated);
    var input: [starh2.core.frame.CLIENT_PREFACE.len + generated.len]u8 = undefined;
    @memcpy(input[0..starh2.core.frame.CLIENT_PREFACE.len], starh2.core.frame.CLIENT_PREFACE);
    @memcpy(input[starh2.core.frame.CLIENT_PREFACE.len..][0..n], generated[0..n]);
    var parser = starh2.core.frame.Parser.init(std.testing.allocator, starh2.core.frame.DEFAULT_MAX_FRAME_SIZE);
    defer parser.deinit();
    var steps: usize = 0;
    var remaining: []const u8 = input[0 .. starh2.core.frame.CLIENT_PREFACE.len + n];
    const max_steps = 2 * remaining.len + 1024;
    while (remaining.len > 0 and steps < max_steps) : (steps += 1) {
        const before = remaining.len;
        const r = parser.ingestOne(remaining) catch {
            return; // classified error
        };
        if (r) |res| {
            if (res.event.payload_owned) std.testing.allocator.free(res.event.payload);
            remaining = remaining[res.consumed..];
        } else return;
        if (remaining.len == before) return;
    }
}

test "fuzz frame" {
    try std.testing.fuzz({}, testOne, .{
        .corpus = &.{
            &[_]u8{ 0, 0, 0, 0x0, 0x1, 0, 0, 0, 1 }, // DATA
            &[_]u8{ 0, 0, 0, 0x1, 0x4, 0, 0, 0, 1 }, // HEADERS
            &[_]u8{ 0, 0, 5, 0x2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 16 }, // PRIORITY
            &[_]u8{ 0, 0, 4, 0x3, 0, 0, 0, 0, 1, 0, 0, 0, 8 }, // RST_STREAM
            &[_]u8{ 0, 0, 0, 0x4, 1, 0, 0, 0, 0 }, // SETTINGS ACK
            &[_]u8{ 0, 0, 4, 0x5, 0x4, 0, 0, 0, 1, 0, 0, 0, 2 }, // PUSH_PROMISE
            &[_]u8{ 0, 0, 8, 0x6, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8 }, // PING
            &[_]u8{ 0, 0, 8, 0x7, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0 }, // GOAWAY
            &[_]u8{ 0, 0, 4, 0x8, 0, 0, 0, 0, 0, 0, 0, 0, 1 }, // WINDOW_UPDATE
            &[_]u8{ 0, 0, 0, 0x9, 0x4, 0, 0, 0, 1 }, // CONTINUATION
            &[_]u8{ 0, 0, 0, 0xfe, 0, 0, 0, 0, 0 }, // unknown extension
            &[_]u8{ 0, 0, 1, 0x0 }, // partial header
            &[_]u8{ 0, 0, 2, 0x0, 0, 0, 0, 0, 1, 0xaa }, // partial payload
            &[_]u8{ 0xff, 0xff, 0xff, 0x0, 0, 0, 0, 0, 1 }, // oversized length
        },
    });
}

test "mutation canary frame length" {
    // Oversized length must classify as FrameSizeError
    var parser = starh2.core.frame.Parser.init(std.testing.allocator, 16_384);
    defer parser.deinit();
    parser.skipPreface();
    const bad = [_]u8{ 0x00, 0x40, 0x01, 0x0, 0, 0, 0, 0, 1 }; // length > max
    try std.testing.expectError(error.FrameSizeError, parser.ingestOne(&bad));
}
