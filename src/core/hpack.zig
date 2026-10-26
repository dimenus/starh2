//! HPACK decoder/encoder (RFC 7541). Encoder: static indices + literal never/without indexing only.
//!
//! The two directions are deliberately asymmetric.
//!
//! The DECODER must be complete, because the peer chooses the encoding. It
//! keeps a dynamic table and decodes Huffman, or it cannot read a browser. The
//! table is `DynTable`: fixed storage the owner gives it, no allocation after.
//! The dynamic table is connection state and not stream state: entry N of one
//! header block can name a string that an earlier block on another stream
//! inserted. That is why a compression fault is always a CONNECTION error. The
//! decoder can no longer track the peer's table, so every later block on the
//! connection is undecodable.
//!
//! The ENCODER is minimal on purpose. It emits static-table indices and
//! literals, and it inserts nothing into a dynamic table. The server therefore
//! keeps no encoder-side table. The cost is a few bytes on each response. The
//! gain is that responses cannot leak across streams through shared compression
//! state, which is the HPACK form of the CRIME attack, and that the encoder has
//! no state a bug could desynchronize.
const std = @import("std");
const boot_plan = @import("boot_plan.zig");

pub const MAX_DYNAMIC_TABLE: usize = 4096;
pub const CompressionError = error{CompressionError};

pub const HeaderField = struct {
    name: []const u8,
    value: []const u8,
    never_index: bool = false,
    /// Decoder results set this. Encoder/response literals stay `false` so a
    /// `.{ .name = "...", .value = "..." }` is a borrow of the source, not an
    /// allocation the caller must free.
    name_owned: bool = false,
    value_owned: bool = false,

    pub fn freeOwned(self: HeaderField, allocator: std.mem.Allocator) void {
        if (self.name_owned) allocator.free(self.name);
        if (self.value_owned) allocator.free(self.value);
    }

    pub fn freeOwnedSlice(allocator: std.mem.Allocator, fields: []const HeaderField) void {
        for (fields) |f| f.freeOwned(allocator);
    }
};

const StaticEntry = struct { name: []const u8, value: []const u8 };

pub const static_table = [_]StaticEntry{
    .{ .name = ":authority", .value = "" },
    .{ .name = ":method", .value = "GET" },
    .{ .name = ":method", .value = "POST" },
    .{ .name = ":path", .value = "/" },
    .{ .name = ":path", .value = "/index.html" },
    .{ .name = ":scheme", .value = "http" },
    .{ .name = ":scheme", .value = "https" },
    .{ .name = ":status", .value = "200" },
    .{ .name = ":status", .value = "204" },
    .{ .name = ":status", .value = "206" },
    .{ .name = ":status", .value = "304" },
    .{ .name = ":status", .value = "400" },
    .{ .name = ":status", .value = "404" },
    .{ .name = ":status", .value = "500" },
    .{ .name = "accept-charset", .value = "" },
    .{ .name = "accept-encoding", .value = "gzip, deflate" },
    .{ .name = "accept-language", .value = "" },
    .{ .name = "accept-ranges", .value = "" },
    .{ .name = "accept", .value = "" },
    .{ .name = "access-control-allow-origin", .value = "" },
    .{ .name = "age", .value = "" },
    .{ .name = "allow", .value = "" },
    .{ .name = "authorization", .value = "" },
    .{ .name = "cache-control", .value = "" },
    .{ .name = "content-disposition", .value = "" },
    .{ .name = "content-encoding", .value = "" },
    .{ .name = "content-language", .value = "" },
    .{ .name = "content-length", .value = "" },
    .{ .name = "content-location", .value = "" },
    .{ .name = "content-range", .value = "" },
    .{ .name = "content-type", .value = "" },
    .{ .name = "cookie", .value = "" },
    .{ .name = "date", .value = "" },
    .{ .name = "etag", .value = "" },
    .{ .name = "expect", .value = "" },
    .{ .name = "expires", .value = "" },
    .{ .name = "from", .value = "" },
    .{ .name = "host", .value = "" },
    .{ .name = "if-match", .value = "" },
    .{ .name = "if-modified-since", .value = "" },
    .{ .name = "if-none-match", .value = "" },
    .{ .name = "if-range", .value = "" },
    .{ .name = "if-unmodified-since", .value = "" },
    .{ .name = "last-modified", .value = "" },
    .{ .name = "link", .value = "" },
    .{ .name = "location", .value = "" },
    .{ .name = "max-forwards", .value = "" },
    .{ .name = "proxy-authenticate", .value = "" },
    .{ .name = "proxy-authorization", .value = "" },
    .{ .name = "range", .value = "" },
    .{ .name = "referer", .value = "" },
    .{ .name = "refresh", .value = "" },
    .{ .name = "retry-after", .value = "" },
    .{ .name = "server", .value = "" },
    .{ .name = "set-cookie", .value = "" },
    .{ .name = "strict-transport-security", .value = "" },
    .{ .name = "transfer-encoding", .value = "" },
    .{ .name = "user-agent", .value = "" },
    .{ .name = "vary", .value = "" },
    .{ .name = "via", .value = "" },
    .{ .name = "www-authenticate", .value = "" },
};

/// RFC 7541 Appendix B Huffman codes are in `huff_pairs` (exact Go x/net table).
const HuffPair = struct { code: u32, bits: u8 };
const huff_pairs: [257]HuffPair = load: {
    @setEvalBranchQuota(100_000);
    // Canonical pairs from RFC 7541 Appendix B (verified subset used by decode).
    // Full table embedded as (code, bits); EOS at 256.
    var pairs: [257]HuffPair = undefined;
    const raw = [_]struct { u32, u8 }{
        .{ 0x1ff8, 13 },    .{ 0x7fffd8, 23 },   .{ 0xfffffe2, 28 },  .{ 0xfffffe3, 28 },
        .{ 0xfffffe4, 28 }, .{ 0xfffffe5, 28 },  .{ 0xfffffe6, 28 },  .{ 0xfffffe7, 28 },
        .{ 0xfffffe8, 28 }, .{ 0xffffea, 24 },   .{ 0x3ffffffc, 30 }, .{ 0xfffffe9, 28 },
        .{ 0xfffffea, 28 }, .{ 0x3ffffffd, 30 }, .{ 0xfffffeb, 28 },  .{ 0xfffffec, 28 },
        .{ 0xfffffed, 28 }, .{ 0xfffffee, 28 },  .{ 0xfffffef, 28 },  .{ 0xffffff0, 28 },
        .{ 0xffffff1, 28 }, .{ 0xffffff2, 28 },  .{ 0x3ffffffe, 30 }, .{ 0xffffff3, 28 },
        .{ 0xffffff4, 28 }, .{ 0xffffff5, 28 },  .{ 0xffffff6, 28 },  .{ 0xffffff7, 28 },
        .{ 0xffffff8, 28 }, .{ 0xffffff9, 28 },  .{ 0xffffffa, 28 },  .{ 0xffffffb, 28 },
        .{ 0x14, 6 },       .{ 0x3f8, 10 },      .{ 0x3f9, 10 },      .{ 0xffa, 12 },
        .{ 0x1ff9, 13 },    .{ 0x15, 6 },        .{ 0xf8, 8 },        .{ 0x7fa, 11 },
        .{ 0x3fa, 10 },     .{ 0x3fb, 10 },      .{ 0xf9, 8 },        .{ 0x7fb, 11 },
        .{ 0xfa, 8 },       .{ 0x16, 6 },        .{ 0x17, 6 },        .{ 0x18, 6 },
        .{ 0x0, 5 },        .{ 0x1, 5 },         .{ 0x2, 5 },         .{ 0x19, 6 },
        .{ 0x1a, 6 },       .{ 0x1b, 6 },        .{ 0x1c, 6 },        .{ 0x1d, 6 },
        .{ 0x1e, 6 },       .{ 0x1f, 6 },        .{ 0x5c, 7 },        .{ 0xfb, 8 },
        .{ 0x7ffc, 15 },    .{ 0x20, 6 },        .{ 0xffb, 12 },      .{ 0x3fc, 10 },
        .{ 0x1ffa, 13 },    .{ 0x21, 6 },        .{ 0x5d, 7 },        .{ 0x5e, 7 },
        .{ 0x5f, 7 },       .{ 0x60, 7 },        .{ 0x61, 7 },        .{ 0x62, 7 },
        .{ 0x63, 7 },       .{ 0x64, 7 },        .{ 0x65, 7 },        .{ 0x66, 7 },
        .{ 0x67, 7 },       .{ 0x68, 7 },        .{ 0x69, 7 },        .{ 0x6a, 7 },
        .{ 0x6b, 7 },       .{ 0x6c, 7 },        .{ 0x6d, 7 },        .{ 0x6e, 7 },
        .{ 0x6f, 7 },       .{ 0x70, 7 },        .{ 0x71, 7 },        .{ 0x72, 7 },
        .{ 0xfc, 8 },       .{ 0x73, 7 },        .{ 0xfd, 8 },        .{ 0x1ffb, 13 },
        .{ 0x7fff0, 19 },   .{ 0x1ffc, 13 },     .{ 0x3ffc, 14 },     .{ 0x22, 6 },
        .{ 0x7ffd, 15 },    .{ 0x3, 5 },         .{ 0x23, 6 },        .{ 0x4, 5 },
        .{ 0x24, 6 },       .{ 0x5, 5 },         .{ 0x25, 6 },        .{ 0x26, 6 },
        .{ 0x27, 6 },       .{ 0x6, 5 },         .{ 0x74, 7 },        .{ 0x75, 7 },
        .{ 0x28, 6 },       .{ 0x29, 6 },        .{ 0x2a, 6 },        .{ 0x7, 5 },
        .{ 0x2b, 6 },       .{ 0x76, 7 },        .{ 0x2c, 6 },        .{ 0x8, 5 },
        .{ 0x9, 5 },        .{ 0x2d, 6 },        .{ 0x77, 7 },        .{ 0x78, 7 },
        .{ 0x79, 7 },       .{ 0x7a, 7 },        .{ 0x7b, 7 },        .{ 0x7ffe, 15 },
        .{ 0x7fc, 11 },     .{ 0x3ffd, 14 },     .{ 0x1ffd, 13 },     .{ 0xffffffc, 28 },
        .{ 0xfffe6, 20 },   .{ 0x3fffd2, 22 },   .{ 0xfffe7, 20 },    .{ 0xfffe8, 20 },
        .{ 0x3fffd3, 22 },  .{ 0x3fffd4, 22 },   .{ 0x3fffd5, 22 },   .{ 0x7fffd9, 23 },
        .{ 0x3fffd6, 22 },  .{ 0x7fffda, 23 },   .{ 0x7fffdb, 23 },   .{ 0x7fffdc, 23 },
        .{ 0x7fffdd, 23 },  .{ 0x7fffde, 23 },   .{ 0xffffeb, 24 },   .{ 0x7fffdf, 23 },
        .{ 0xffffec, 24 },  .{ 0xffffed, 24 },   .{ 0x3fffd7, 22 },   .{ 0x7fffe0, 23 },
        .{ 0xffffee, 24 },  .{ 0x7fffe1, 23 },   .{ 0x7fffe2, 23 },   .{ 0x7fffe3, 23 },
        .{ 0x7fffe4, 23 },  .{ 0x1fffdc, 21 },   .{ 0x3fffd8, 22 },   .{ 0x7fffe5, 23 },
        .{ 0x3fffd9, 22 },  .{ 0x7fffe6, 23 },   .{ 0x7fffe7, 23 },   .{ 0xffffef, 24 },
        .{ 0x3fffda, 22 },  .{ 0x1fffdd, 21 },   .{ 0xfffe9, 20 },    .{ 0x3fffdb, 22 },
        .{ 0x3fffdc, 22 },  .{ 0x7fffe8, 23 },   .{ 0x7fffe9, 23 },   .{ 0x1fffde, 21 },
        .{ 0x7fffea, 23 },  .{ 0x3fffdd, 22 },   .{ 0x3fffde, 22 },   .{ 0xfffff0, 24 },
        .{ 0x1fffdf, 21 },  .{ 0x3fffdf, 22 },   .{ 0x7fffeb, 23 },   .{ 0x7fffec, 23 },
        .{ 0x1fffe0, 21 },  .{ 0x1fffe1, 21 },   .{ 0x3fffe0, 22 },   .{ 0x1fffe2, 21 },
        .{ 0x7fffed, 23 },  .{ 0x3fffe1, 22 },   .{ 0x7fffee, 23 },   .{ 0x7fffef, 23 },
        .{ 0xfffea, 20 },   .{ 0x3fffe2, 22 },   .{ 0x3fffe3, 22 },   .{ 0x3fffe4, 22 },
        .{ 0x7ffff0, 23 },  .{ 0x3fffe5, 22 },   .{ 0x3fffe6, 22 },   .{ 0x7ffff1, 23 },
        .{ 0x3ffffe0, 26 }, .{ 0x3ffffe1, 26 },  .{ 0xfffeb, 20 },    .{ 0x7fff1, 19 },
        .{ 0x3fffe7, 22 },  .{ 0x7ffff2, 23 },   .{ 0x3fffe8, 22 },   .{ 0x1ffffec, 25 },
        .{ 0x3ffffe2, 26 }, .{ 0x3ffffe3, 26 },  .{ 0x3ffffe4, 26 },  .{ 0x7ffffde, 27 },
        .{ 0x7ffffdf, 27 }, .{ 0x3ffffe5, 26 },  .{ 0xfffff1, 24 },   .{ 0x1ffffed, 25 },
        .{ 0x7fff2, 19 },   .{ 0x1fffe3, 21 },   .{ 0x3ffffe6, 26 },  .{ 0x7ffffe0, 27 },
        .{ 0x7ffffe1, 27 }, .{ 0x3ffffe7, 26 },  .{ 0x7ffffe2, 27 },  .{ 0xfffff2, 24 },
        .{ 0x1fffe4, 21 },  .{ 0x1fffe5, 21 },   .{ 0x3ffffe8, 26 },  .{ 0x3ffffe9, 26 },
        .{ 0xffffffd, 28 }, .{ 0x7ffffe3, 27 },  .{ 0x7ffffe4, 27 },  .{ 0x7ffffe5, 27 },
        .{ 0xfffec, 20 },   .{ 0xfffff3, 24 },   .{ 0xfffed, 20 },    .{ 0x1fffe6, 21 },
        .{ 0x3fffe9, 22 },  .{ 0x1fffe7, 21 },   .{ 0x1fffe8, 21 },   .{ 0x7ffff3, 23 },
        .{ 0x3fffea, 22 },  .{ 0x3fffeb, 22 },   .{ 0x1ffffee, 25 },  .{ 0x1ffffef, 25 },
        .{ 0xfffff4, 24 },  .{ 0xfffff5, 24 },   .{ 0x3ffffea, 26 },  .{ 0x7ffff4, 23 },
        .{ 0x3ffffeb, 26 }, .{ 0x7ffffe6, 27 },  .{ 0x3ffffec, 26 },  .{ 0x3ffffed, 26 },
        .{ 0x7ffffe7, 27 }, .{ 0x7ffffe8, 27 },  .{ 0x7ffffe9, 27 },  .{ 0x7ffffea, 27 },
        .{ 0x7ffffeb, 27 }, .{ 0xffffffe, 28 },  .{ 0x7ffffec, 27 },  .{ 0x7ffffed, 27 },
        .{ 0x7ffffee, 27 }, .{ 0x7ffffef, 27 },  .{ 0x7fffff0, 27 },  .{ 0x3ffffee, 26 },
        .{ 0x3fffffff, 30 }, // EOS
    };
    // Canonical RFC 7541 Appendix B / Go x/net hpack table (257 incl. EOS).
    for (&pairs, 0..) |*p, i| {
        p.* = .{ .code = raw[i][0], .bits = raw[i][1] };
    }
    break :load pairs;
};

const huff_decode_missing = std.math.maxInt(u16);
const huff_decode_none: u16 = 257;

const HuffDecodeNode = struct {
    child: [2]u16 = .{ huff_decode_missing, huff_decode_missing },
    /// 0–255 byte, 256 EOS, 257 not a leaf.
    symbol: u16 = huff_decode_none,
};

const huff_decode_trie = build: {
    @setEvalBranchQuota(200_000);
    var storage: [1024]HuffDecodeNode = @splat(.{});
    var count: u16 = 1;
    for (huff_pairs, 0..) |p, sym| {
        var idx: u16 = 0;
        var bit_i: u8 = 0;
        while (bit_i < p.bits) : (bit_i += 1) {
            const shift: u5 = @intCast(p.bits - 1 - bit_i);
            const bit: u1 = @intCast((p.code >> shift) & 1);
            if (storage[idx].child[bit] == huff_decode_missing) {
                if (count >= storage.len) unreachable;
                storage[idx].child[bit] = count;
                count += 1;
            }
            idx = storage[idx].child[bit];
        }
        if (storage[idx].symbol != huff_decode_none) unreachable;
        storage[idx].symbol = @intCast(sym);
    }
    var exact: [count]HuffDecodeNode = undefined;
    @memcpy(&exact, storage[0..count]);
    break :build exact;
};

pub fn decodeHuffman(allocator: std.mem.Allocator, encoded: []const u8) (CompressionError || error{OutOfMemory})![]u8 {
    return huffDecode(allocator, encoded);
}

/// Exposed for mechanical table checks against Go x/net / RFC 7541 Appendix B.
pub fn huffmanTableLen() usize {
    return huff_pairs.len;
}

pub fn huffmanEos() struct { code: u32, bits: u8 } {
    return .{ .code = huff_pairs[256].code, .bits = huff_pairs[256].bits };
}

pub fn huffmanPair(sym: u8) struct { code: u32, bits: u8 } {
    return .{ .code = huff_pairs[sym].code, .bits = huff_pairs[sym].bits };
}

/// Walk a Huffman string. Returns its decoded length; writes the bytes to
/// `out` when given one. `out` must hold `huffUpper(encoded.len)` bytes.
fn huffWalk(encoded: []const u8, out: ?[]u8) CompressionError!usize {
    var n: usize = 0;
    var node: u16 = 0;
    var leftover: u32 = 0;
    var leftover_bits: u8 = 0;
    for (encoded) |byte| {
        var bit_i: u4 = 0;
        while (bit_i < 8) : (bit_i += 1) {
            const bit: u1 = @intCast((byte >> @intCast(7 - bit_i)) & 1);
            const next = huff_decode_trie[node].child[bit];
            if (next == huff_decode_missing) return error.CompressionError;
            node = next;
            leftover = (leftover << 1) | bit;
            leftover_bits += 1;
            const sym = huff_decode_trie[node].symbol;
            if (sym == huff_decode_none) continue;
            // EOS inside a string is a compression error (RFC 7541 §5.2).
            if (sym == 256) return error.CompressionError;
            if (out) |o| o[n] = @intCast(sym);
            n += 1;
            node = 0;
            leftover = 0;
            leftover_bits = 0;
        }
    }
    // RFC 7541 §5.2: leftover bits are EOS-prefix padding. More than 7 bits
    // means a whole symbol was dropped. Padding that is not all-ones is a
    // second encoding of the same string, which lets a peer smuggle a value
    // past a byte comparison somewhere downstream.
    if (leftover_bits >= 8) return error.CompressionError;
    if (leftover_bits > 0) {
        const pad_mask: u32 = (@as(u32, 1) << @intCast(leftover_bits)) - 1;
        if (leftover != pad_mask) return error.CompressionError;
    }
    return n;
}

/// Appendix B's shortest code is 5 bits, so a Huffman string of `encoded_len`
/// bytes decodes to at most this many bytes.
fn huffUpper(encoded_len: usize) usize {
    return encoded_len * 8 / 5;
}

fn huffDecode(allocator: std.mem.Allocator, encoded: []const u8) (CompressionError || error{OutOfMemory})![]u8 {
    if (encoded.len == 0) return try allocator.alloc(u8, 0);
    const buf = try allocator.alloc(u8, huffUpper(encoded.len));
    errdefer allocator.free(buf);
    const n = try huffWalk(encoded, buf);
    if (n == buf.len) return buf;
    // The allocation was the upper bound. Shrinking in place keeps the bytes
    // that were not used available to a request arena, whose tail this is.
    if (allocator.remap(buf, n)) |shrunk| return shrunk;
    const exact = try allocator.dupe(u8, buf[0..n]);
    allocator.free(buf);
    return exact;
}

/// A string literal located in a header block and not yet decoded.
const StrRef = struct {
    huffman: bool,
    bytes: []const u8,

    /// The most bytes the decoded string can take. Exact for a raw string.
    fn upper(self: StrRef) usize {
        return if (self.huffman) huffUpper(self.bytes.len) else self.bytes.len;
    }

    fn materialize(self: StrRef, allocator: std.mem.Allocator) (CompressionError || error{OutOfMemory})![]u8 {
        if (self.huffman) return huffDecode(allocator, self.bytes);
        return try allocator.dupe(u8, self.bytes);
    }

    /// The decoded length, exactly, with no allocation. Validates the string
    /// the way decoding does (padding, EOS), so a bad string fails here.
    fn exactLen(self: StrRef) CompressionError!usize {
        if (!self.huffman) return self.bytes.len;
        return huffWalk(self.bytes, null);
    }

    /// Decode into `out`, whose length is `exactLen`.
    fn decodeExact(self: StrRef, out: []u8) CompressionError!void {
        if (self.huffman) {
            const n = try huffWalk(self.bytes, out);
            std.debug.assert(n == out.len);
        } else {
            std.debug.assert(self.bytes.len == out.len);
            @memcpy(out, self.bytes);
        }
    }

    /// A copy of exactly `n` bytes (the string's `exactLen`) in `allocator`.
    fn materializeExact(self: StrRef, allocator: std.mem.Allocator, n: usize) (CompressionError || error{OutOfMemory})![]u8 {
        const buf = try allocator.alloc(u8, n);
        errdefer allocator.free(buf);
        try self.decodeExact(buf);
        return buf;
    }
};

fn peekString(data: []const u8, start: *usize) CompressionError!StrRef {
    if (start.* >= data.len) return error.CompressionError;
    const huffman = (data[start.*] & 0x80) != 0;
    const len = try decodeInt(data, 7, start);
    if (len > data.len - start.*) return error.CompressionError;
    const slice = data[start.* .. start.* + len];
    start.* += len;
    return .{ .huffman = huffman, .bytes = slice };
}

fn decodeString(allocator: std.mem.Allocator, data: []const u8, start: *usize) (CompressionError || error{OutOfMemory})![]u8 {
    const ref = try peekString(data, start);
    return ref.materialize(allocator);
}

fn decodeInt(data: []const u8, prefix_bits: u3, start: *usize) CompressionError!usize {
    if (start.* >= data.len) return error.CompressionError;
    const mask: u8 = @as(u8, 0xff) >> @as(u3, @intCast(8 - @as(u8, prefix_bits)));
    var value: usize = data[start.*] & mask;
    start.* += 1;
    if (value < mask) return value;
    // The continuation bytes are bounded twice. `m > 28` caps the shift, and the
    // overflow check caps the sum. Without both, a peer sends a long run of
    // continuation bytes and chooses either an integer overflow or a huge length
    // that a later allocation would honour.
    var m: u6 = 0;
    while (true) {
        if (start.* >= data.len) return error.CompressionError;
        const b = data[start.*];
        start.* += 1;
        const add = @as(usize, b & 0x7f) << m;
        const sum = @addWithOverflow(value, add);
        if (sum[1] == 1) return error.CompressionError;
        value = sum[0];
        m += 7;
        if (m > 28) return error.CompressionError;
        if ((b & 0x80) == 0) break;
    }
    return value;
}

/// Entries put into a dynamic table, in every decoder of the process. The work
/// counter that goes with "the table takes no general-allocator call": a zero
/// count of calls means nothing in a run that inserted nothing. The bench
/// server prints it on `/mem`.
pub var table_insertions: std.atomic.Value(u64) = .init(0);

/// The most entries a table of `MAX_DYNAMIC_TABLE` bytes can hold: every entry
/// is accounted at its name, its value and 32 bytes (RFC 7541 section 4.1), so
/// none is smaller than 32.
pub const MAX_DYNAMIC_ENTRIES: usize = MAX_DYNAMIC_TABLE / 32;

/// The decoder's dynamic table, in storage it is given and never grows.
///
/// `data` holds each entry's name then its value, packed, oldest entry first,
/// from offset 0. The RFC's `+ 32` per entry is only in the size accounting
/// (`size`, against `max_size`) and takes no byte of `data`; the table must
/// evict at the moment the peer's does, and it has room for it because the
/// bytes used (`used`) are always fewer than `size`. `entries` is a ring of
/// descriptors, oldest at `first`.
///
/// Evicting an entry only moves `first`. The next insert then compacts: it
/// moves what is left down to offset 0 and shifts the descriptors with it, so
/// every name and value is one contiguous slice and the free room is one run
/// at the tail. No call allocates; the storage came from the caller at `init`.
///
/// A slice returned by `name`/`value` points into `data` and is valid until the
/// next `insert`, `clear` or `setMaxSize`. A caller that keeps a field copies it.
pub const DynTable = struct {
    data: []u8,
    entries: []Entry,
    first: usize = 0,
    len: usize = 0,
    used: usize = 0,
    size: usize = 0,
    max_size: usize = MAX_DYNAMIC_TABLE,

    pub const Entry = struct {
        off: u16,
        name_len: u16,
        value_len: u16,
    };

    fn entryAt(self: *const DynTable, newest_first: usize) *const Entry {
        std.debug.assert(newest_first < self.len);
        return &self.entries[(self.first + self.len - 1 - newest_first) % self.entries.len];
    }

    /// Entry `i`, newest first: dynamic index 0 is the most recently inserted.
    pub fn name(self: *const DynTable, i: usize) []const u8 {
        const e = self.entryAt(i);
        return self.data[e.off..][0..e.name_len];
    }

    pub fn value(self: *const DynTable, i: usize) []const u8 {
        const e = self.entryAt(i);
        return self.data[@as(usize, e.off) + e.name_len ..][0..e.value_len];
    }

    pub fn clear(self: *DynTable) void {
        self.first = 0;
        self.len = 0;
        self.used = 0;
        self.size = 0;
    }

    fn evictOldest(self: *DynTable) void {
        std.debug.assert(self.len > 0);
        const e = self.entries[self.first];
        const bytes = @as(usize, e.name_len) + e.value_len;
        self.size -= bytes + 32;
        self.used -= bytes;
        self.first = (self.first + 1) % self.entries.len;
        self.len -= 1;
        if (self.len == 0) {
            self.first = 0;
            std.debug.assert(self.used == 0 and self.size == 0);
        }
    }

    /// Move the entries to the start of `data`. Evictions leave a gap there.
    fn compact(self: *DynTable) void {
        if (self.len == 0) return;
        const gap = self.entries[self.first].off;
        if (gap == 0) return;
        std.mem.copyForwards(u8, self.data[0..self.used], self.data[gap..][0..self.used]);
        var k: usize = 0;
        while (k < self.len) : (k += 1) {
            const e = &self.entries[(self.first + k) % self.entries.len];
            e.off -= gap;
        }
    }

    /// Resize the table's limit, evicting from the oldest end to fit. The caller
    /// has checked `new_size <= MAX_DYNAMIC_TABLE`.
    pub fn setMaxSize(self: *DynTable, new_size: usize) void {
        std.debug.assert(new_size <= MAX_DYNAMIC_TABLE);
        self.max_size = new_size;
        while (self.size > self.max_size) self.evictOldest();
    }

    /// Add an entry as the newest, copying `entry_name` and `entry_value` in.
    /// Neither may point into this table.
    ///
    /// The `+ 32` is RFC 7541 section 4.1. It is not a guess: the accounted
    /// size of an entry must match the peer's accounting exactly, or the two
    /// sides evict at different moments and every later index disagrees.
    ///
    /// An entry larger than the whole table empties the table and is not
    /// stored. That is the RFC's own rule, and it is why the eviction loop
    /// below cannot spin.
    pub fn insert(self: *DynTable, entry_name: []const u8, entry_value: []const u8) void {
        std.debug.assert(!overlaps(self.data, entry_name) and !overlaps(self.data, entry_value));
        const bytes = entry_name.len + entry_value.len;
        const entry_size = bytes + 32;
        if (entry_size > self.max_size) {
            self.clear();
            return;
        }
        while (self.size + entry_size > self.max_size) self.evictOldest();
        self.compact();
        // `size <= max_size <= MAX_DYNAMIC_TABLE` and `size > used + 32 * len`,
        // so the bytes and the descriptor both fit.
        std.debug.assert(self.used + bytes <= self.data.len);
        std.debug.assert(self.len < self.entries.len);
        const off = self.used;
        @memcpy(self.data[off..][0..entry_name.len], entry_name);
        @memcpy(self.data[off + entry_name.len ..][0..entry_value.len], entry_value);
        self.entries[(self.first + self.len) % self.entries.len] = .{
            .off = @intCast(off),
            .name_len = @intCast(entry_name.len),
            .value_len = @intCast(entry_value.len),
        };
        self.len += 1;
        self.used += bytes;
        self.size += entry_size;
        _ = table_insertions.fetchAdd(1, .monotonic);
    }

    fn overlaps(haystack: []const u8, needle: []const u8) bool {
        if (needle.len == 0 or haystack.len == 0) return false;
        const h0 = @intFromPtr(haystack.ptr);
        const n0 = @intFromPtr(needle.ptr);
        return n0 < h0 + haystack.len and h0 < n0 + needle.len;
    }
};

pub const Decoder = struct {
    /// Where `table`'s storage came from, and goes back to in `deinit`.
    storage: std.mem.Allocator,
    table: DynTable,

    pub const DecodeResult = struct {
        /// The decoded fields. With `truncated` this is a prefix of the block's
        /// fields and not all of them.
        fields: []HeaderField,
        decoded_size: usize,
        policy_exceeded: bool = false,
        /// The block held more fields, or more decoded bytes, than the limits
        /// allow, so only a prefix was materialized. The decoder still read
        /// the whole block, because its table must stay in step with the
        /// peer's. A caller has no use for the fields of a truncated result:
        /// it is a request to refuse.
        truncated: bool = false,
    };

    /// The boot allocations of `init`, in its order. See `boot_plan`.
    pub fn planBoot(plan: *boot_plan.Plan) error{InvalidConfig}!void {
        try plan.array(u8, MAX_DYNAMIC_TABLE);
        try plan.array(DynTable.Entry, MAX_DYNAMIC_ENTRIES);
    }

    /// A decoder whose table is two fixed arrays from `storage`. On the live
    /// edge that is the connection slot's boot buffer, which is rewound when
    /// the slot is reused; nothing allocates again after this.
    pub fn init(storage: std.mem.Allocator) error{OutOfMemory}!Decoder {
        const data = try storage.alloc(u8, MAX_DYNAMIC_TABLE);
        errdefer storage.free(data);
        const entries = try storage.alloc(DynTable.Entry, MAX_DYNAMIC_ENTRIES);
        return .{ .storage = storage, .table = .{ .data = data, .entries = entries } };
    }

    pub fn deinit(self: *Decoder) void {
        self.storage.free(self.table.entries);
        self.storage.free(self.table.data);
    }

    fn setMaxSize(self: *Decoder, new_size: usize) CompressionError!void {
        if (new_size > MAX_DYNAMIC_TABLE) return error.CompressionError;
        self.table.setMaxSize(new_size);
    }

    const Indexed = struct {
        name: []const u8,
        value: []const u8,
        static: bool,
    };

    fn lookup(self: *const Decoder, index: usize) CompressionError!Indexed {
        if (index == 0) return error.CompressionError;
        if (index <= static_table.len) {
            const e = static_table[index - 1];
            return .{ .name = e.name, .value = e.value, .static = true };
        }
        const dyn_index = index - static_table.len - 1;
        if (dyn_index >= self.table.len) return error.CompressionError;
        return .{ .name = self.table.name(dyn_index), .value = self.table.value(dyn_index), .static = false };
    }

    /// Static-table strings are immutable process-lifetime data. Dynamic-table
    /// strings are not: a later block can evict the entry while a request still
    /// holds the decoded field, so those must be independently owned.
    ///
    /// `request_alloc` owns the copy. The table's strings are never handed
    /// out (see `indexIntoTable`): a reset of the request's arena must not
    /// reach the table, and a later insert must not change a field.
    fn copyIndexedString(src: []const u8, from_static: bool, request_alloc: std.mem.Allocator) error{OutOfMemory}!struct { []const u8, bool } {
        if (from_static) return .{ src, false };
        return .{ try request_alloc.dupe(u8, src), true };
    }

    fn decodeName(self: *Decoder, request_alloc: std.mem.Allocator, block: []const u8, idx: usize, i: *usize) (CompressionError || error{OutOfMemory})!struct { []const u8, bool } {
        if (idx == 0) return .{ try decodeString(request_alloc, block, i), true };
        const pair = try self.lookup(idx);
        return copyIndexedString(pair.name, pair.static, request_alloc);
    }

    fn takeIndexed(pair: Indexed, request_alloc: std.mem.Allocator) error{OutOfMemory}!HeaderField {
        const name, const name_owned = try copyIndexedString(pair.name, pair.static, request_alloc);
        errdefer if (name_owned) request_alloc.free(name);
        const value, const value_owned = try copyIndexedString(pair.value, pair.static, request_alloc);
        return .{
            .name = name,
            .value = value,
            .name_owned = name_owned,
            .value_owned = value_owned,
        };
    }

    fn takeLiteral(self: *Decoder, request_alloc: std.mem.Allocator, block: []const u8, idx: usize, i: *usize, never: bool) (CompressionError || error{OutOfMemory})!HeaderField {
        const name, const name_owned = try self.decodeName(request_alloc, block, idx, i);
        errdefer if (name_owned) request_alloc.free(name);
        const value = try decodeString(request_alloc, block, i);
        return .{
            .name = name,
            .value = value,
            .never_index = never,
            .name_owned = name_owned,
            .value_owned = true,
        };
    }

    /// Copies `name`/`value` into the dynamic table. The result field keeps its
    /// own ownership; the table must not alias it.
    fn indexIntoTable(self: *Decoder, name: []const u8, value: []const u8) void {
        self.table.insert(name, value);
    }

    /// Decode a complete header block into `self.storage`, which then also
    /// serves as the transient allocator. For tests and tools: the edge calls
    /// `decodeInto` with the request's arena and the slot's scratch.
    pub fn decode(
        self: *Decoder,
        block: []const u8,
        max_fields: usize,
        max_decoded_size: usize,
        max_name: usize,
        max_value: usize,
    ) (CompressionError || error{OutOfMemory})!DecodeResult {
        return self.decodeInto(self.storage, self.storage, block, max_fields, max_decoded_size, max_name, max_value);
    }

    /// The most bytes `decodeInto` takes from `request_alloc`, whatever the
    /// block says: the field array at full capacity plus the strings, which
    /// are held to `max_decoded_size`. An allocator with this much room cannot
    /// make `decodeInto` fail. `alignment_slack` covers the field array's
    /// alignment inside a block that is not empty.
    pub fn requestBytesBound(max_fields: usize, max_decoded_size: usize) error{Overflow}!usize {
        const alignment_slack = @alignOf(HeaderField);
        const array = try std.math.mul(usize, max_fields, @sizeOf(HeaderField));
        return try std.math.add(usize, try std.math.add(usize, array, max_decoded_size), alignment_slack);
    }

    const DecodeState = struct {
        fields: std.ArrayList(HeaderField) = .empty,
        decoded_size: usize = 0,
        /// Fields in the block so far, whether or not they were materialized.
        seen: usize = 0,
        policy: bool = false,
        truncated: bool = false,
        max_fields: usize,
        max_decoded_size: usize,
        max_name: usize,
        max_value: usize,

        /// Count a field against the policy limits.
        fn account(st: *DecodeState, name_len: usize, value_len: usize) void {
            st.decoded_size +|= name_len +| value_len +| 32;
            if (st.seen >= st.max_fields or st.decoded_size > st.max_decoded_size or name_len > st.max_name or value_len > st.max_value) st.policy = true;
            st.seen += 1;
        }

        /// Whether the field just counted may be materialized.
        fn fits(st: *const DecodeState) bool {
            return !st.truncated and st.fields.items.len < st.fields.capacity and st.decoded_size <= st.max_decoded_size;
        }
    };

    /// Decode a complete header block.
    ///
    /// `request_alloc` owns the result field slice and every non-static string
    /// in it. The connection's dynamic table is `self.table`, in the storage it
    /// was made with, and takes nothing from any allocator after `init`.
    /// `scratch` is the transient room of one decode: at most
    /// `MAX_DYNAMIC_TABLE` bytes, only for a field that is past the request's
    /// budget and enters the table (see `literalField`), and given back before
    /// this returns. On the live edge it is the slot's `hpack_scratch`.
    ///
    /// A policy breach is NOT an error here. A field block that exceeds
    /// `max_fields`, `max_decoded_size`, `max_name`, or `max_value` still
    /// decodes in full, and the result carries `policy_exceeded = true`.
    /// The reason is the dynamic table: an early return would leave the
    /// decoder's table out of step with the peer's, which corrupts every later
    /// block on the connection. So the decoder always finishes its work, and
    /// `Session` turns the flag into a 431 response for that one stream while
    /// the connection survives.
    ///
    /// What it takes from `request_alloc` is bounded, whatever the block says
    /// (`requestBytesBound`). A block can be 64 KiB of one-byte references to
    /// a 4 KiB table entry, which decodes to 256 MiB; the request's memory
    /// must not grow with that, because the table has to be advanced and the
    /// request refused even when the request cannot get any memory. So the
    /// field array is sized once, strings are materialized only while the
    /// running decoded size stays within `max_decoded_size`, and past that the
    /// result is `truncated` and later fields are only run through the table.
    /// A field near the edge is decoded on the decoder's allocator first
    /// (`literalField`), so an oversized string never touches `request_alloc`.
    pub fn decodeInto(
        self: *Decoder,
        request_alloc: std.mem.Allocator,
        scratch: std.mem.Allocator,
        block: []const u8,
        max_fields: usize,
        max_decoded_size: usize,
        max_name: usize,
        max_value: usize,
    ) (CompressionError || error{OutOfMemory})!DecodeResult {
        var st: DecodeState = .{
            .max_fields = max_fields,
            .max_decoded_size = max_decoded_size,
            .max_name = max_name,
            .max_value = max_value,
        };
        errdefer {
            HeaderField.freeOwnedSlice(request_alloc, st.fields.items);
            st.fields.deinit(request_alloc);
        }
        // Each field takes at least one byte of the block, so a block never
        // has more fields than bytes.
        try st.fields.ensureTotalCapacityPrecise(request_alloc, @min(max_fields, block.len));
        var i: usize = 0;
        var first = true;
        while (i < block.len) {
            const b = block[i];
            if ((b & 0x80) != 0) {
                const idx = try decodeInt(block, 7, &i);
                const pair = try self.lookup(idx);
                st.account(pair.name.len, pair.value.len);
                if (st.fits()) {
                    st.fields.appendAssumeCapacity(try takeIndexed(pair, request_alloc));
                } else {
                    st.truncated = true;
                }
                first = false;
            } else if ((b & 0xc0) == 0x40) {
                const idx = try decodeInt(block, 6, &i);
                try self.literalField(request_alloc, scratch, block, idx, &i, false, true, &st);
                first = false;
            } else if ((b & 0xe0) == 0x20) {
                // A dynamic table size update is legal only at the start of a
                // block (RFC 7541 section 4.2). Mid-block it would resize the
                // table between two indices that the peer chose under one size.
                if (!first) return error.CompressionError;
                const new_size = try decodeInt(block, 5, &i);
                try self.setMaxSize(new_size);
            } else {
                const never = (b & 0xf0) == 0x10;
                const idx = try decodeInt(block, 4, &i);
                try self.literalField(request_alloc, scratch, block, idx, &i, never, false, &st);
                first = false;
            }
        }
        return .{
            .fields = try st.fields.toOwnedSlice(request_alloc),
            .decoded_size = st.decoded_size,
            .policy_exceeded = st.policy,
            .truncated = st.truncated,
        };
    }

    /// One literal representation: count it, add it to the table when it is
    /// an incremental-indexing one, and materialize it into `request_alloc`
    /// if the budget has room.
    ///
    /// The room is judged BEFORE anything is decoded, from the strings'
    /// upper bounds: a field that cannot overrun what is left of the budget is
    /// decoded straight into `request_alloc`, and the table copies from there.
    ///
    /// Any other field (past the budget's edge) is measured first: the exact
    /// decoded lengths cost a pass over the Huffman data and no memory. Those
    /// lengths are what the policy counts and what the table's accounting
    /// needs. The field is then built in `request_alloc` only if it fits, and
    /// the table is changed after that, from the built field. A field that does
    /// not fit but enters the table is decoded into `scratch` for the copy:
    /// it is no more than `MAX_DYNAMIC_TABLE - 32` bytes, because a larger
    /// entry is never stored (it empties the table).
    fn literalField(
        self: *Decoder,
        request_alloc: std.mem.Allocator,
        scratch: std.mem.Allocator,
        block: []const u8,
        idx: usize,
        i: *usize,
        never: bool,
        index_it: bool,
        st: *DecodeState,
    ) (CompressionError || error{OutOfMemory})!void {
        const start = i.*;
        var pos = start;
        const name_upper = if (idx == 0) (try peekString(block, &pos)).upper() else (try self.lookup(idx)).name.len;
        const value_upper = (try peekString(block, &pos)).upper();
        const room = st.max_decoded_size -| st.decoded_size;
        const sure_fit = !st.truncated and st.fields.items.len < st.fields.capacity and
            name_upper +| value_upper +| 32 <= room;
        if (sure_fit) {
            const field = try self.takeLiteral(request_alloc, block, idx, i, never);
            st.account(field.name.len, field.value.len);
            if (index_it) self.indexIntoTable(field.name, field.value);
            std.debug.assert(st.fits());
            st.fields.appendAssumeCapacity(field);
            return;
        }

        // Past the budget's edge. Locate both strings, measure them exactly.
        var p = start;
        const name_ref: ?StrRef = if (idx == 0) try peekString(block, &p) else null;
        const indexed_name: ?Indexed = if (idx == 0) null else try self.lookup(idx);
        const value_ref = try peekString(block, &p);
        i.* = p;
        const name_len = if (name_ref) |r| try r.exactLen() else indexed_name.?.name.len;
        const value_len = try value_ref.exactLen();
        st.account(name_len, value_len);

        if (st.fits()) {
            // It fits after all (the upper bounds were pessimistic).
            var name_owned = false;
            const name: []const u8 = blk: {
                if (name_ref) |r| {
                    name_owned = true;
                    break :blk try r.materializeExact(request_alloc, name_len);
                }
                const pair = try copyIndexedString(indexed_name.?.name, indexed_name.?.static, request_alloc);
                name_owned = pair[1];
                break :blk pair[0];
            };
            errdefer if (name_owned) request_alloc.free(name);
            const value = try value_ref.materializeExact(request_alloc, value_len);
            st.fields.appendAssumeCapacity(.{
                .name = name,
                .value = value,
                .never_index = never,
                .name_owned = name_owned,
                .value_owned = true,
            });
            if (index_it) {
                const f = st.fields.items[st.fields.items.len - 1];
                self.indexIntoTable(f.name, f.value);
            }
            return;
        }

        st.truncated = true;
        if (!index_it) return;
        if (name_len + value_len + 32 > self.table.max_size) {
            // Larger than the table: it empties the table and is not stored.
            self.table.clear();
            return;
        }
        // The field is not kept, only the table's copy of it. The name is
        // copied out of the table (or the block) first: the insert moves the
        // table's bytes.
        const buf = try scratch.alloc(u8, name_len + value_len);
        defer scratch.free(buf);
        if (name_ref) |r| {
            try r.decodeExact(buf[0..name_len]);
        } else {
            @memcpy(buf[0..name_len], indexed_name.?.name);
        }
        try value_ref.decodeExact(buf[name_len..]);
        self.indexIntoTable(buf[0..name_len], buf[name_len..]);
    }

    pub fn freeResult(self: *Decoder, result: DecodeResult) void {
        self.freeResultInto(self.storage, result);
    }

    pub fn freeResultInto(_: *Decoder, request_alloc: std.mem.Allocator, result: DecodeResult) void {
        HeaderField.freeOwnedSlice(request_alloc, result.fields);
        request_alloc.free(result.fields);
    }
};

fn encodedIntLen(value: usize, prefix_bits: u3) usize {
    const mask: usize = (@as(usize, 1) << prefix_bits) - 1;
    if (value < mask) return 1;
    var len: usize = 1;
    var v = value - mask;
    while (v >= 128) {
        len += 1;
        v /= 128;
    }
    return len + 1;
}

fn encodeIntInto(out: []u8, pos: *usize, value: usize, prefix_bits: u3, first_byte_hi: u8) error{NoSpace}!void {
    const mask: usize = (@as(usize, 1) << prefix_bits) - 1;
    if (value < mask) {
        if (pos.* >= out.len) return error.NoSpace;
        out[pos.*] = first_byte_hi | @as(u8, @intCast(value));
        pos.* += 1;
        return;
    }
    if (pos.* >= out.len) return error.NoSpace;
    out[pos.*] = first_byte_hi | @as(u8, @intCast(mask));
    pos.* += 1;
    var v = value - mask;
    while (v >= 128) {
        if (pos.* >= out.len) return error.NoSpace;
        out[pos.*] = @as(u8, @intCast((v % 128) + 128));
        pos.* += 1;
        v /= 128;
    }
    if (pos.* >= out.len) return error.NoSpace;
    out[pos.*] = @intCast(v);
    pos.* += 1;
}

fn encodedStringLen(s: []const u8) error{Overflow}!usize {
    return std.math.add(usize, encodedIntLen(s.len, 7), s.len);
}

fn encodeStringInto(out: []u8, pos: *usize, s: []const u8) error{NoSpace}!void {
    try encodeIntInto(out, pos, s.len, 7, 0);
    if (s.len > out.len - pos.*) return error.NoSpace;
    @memcpy(out[pos.*..][0..s.len], s);
    pos.* += s.len;
}

fn writeByte(out: []u8, pos: *usize, b: u8) error{NoSpace}!void {
    if (pos.* >= out.len) return error.NoSpace;
    out[pos.*] = b;
    pos.* += 1;
}

fn encodedFieldLen(f: HeaderField) error{Overflow}!usize {
    const full, const name_idx = findStatic(f.name, f.value);
    if (full) |idx| return encodedIntLen(idx, 7);
    var n: usize = 0;
    if (name_idx) |ni| {
        n = encodedIntLen(ni, 4);
    } else {
        n = 1;
        n = try std.math.add(usize, n, try encodedStringLen(f.name));
    }
    return std.math.add(usize, n, try encodedStringLen(f.value));
}

fn encodeFieldInto(out: []u8, pos: *usize, f: HeaderField) error{NoSpace}!void {
    const full, const name_idx = findStatic(f.name, f.value);
    if (full) |idx| {
        try encodeIntInto(out, pos, idx, 7, 0x80);
        return;
    }
    const never = f.never_index or neverIndexName(f.name);
    if (never) {
        if (name_idx) |ni| {
            try encodeIntInto(out, pos, ni, 4, 0x10);
        } else {
            try writeByte(out, pos, 0x10);
            try encodeStringInto(out, pos, f.name);
        }
        try encodeStringInto(out, pos, f.value);
    } else {
        if (name_idx) |ni| {
            try encodeIntInto(out, pos, ni, 4, 0x00);
        } else {
            try writeByte(out, pos, 0x00);
            try encodeStringInto(out, pos, f.name);
        }
        try encodeStringInto(out, pos, f.value);
    }
}

fn findStatic(name: []const u8, value: []const u8) struct { ?usize, ?usize } {
    var name_only: ?usize = null;
    for (static_table, 0..) |e, i| {
        if (std.mem.eql(u8, e.name, name)) {
            if (name_only == null) name_only = i + 1;
            if (std.mem.eql(u8, e.value, value)) return .{ i + 1, i + 1 };
        }
    }
    return .{ null, name_only };
}

/// These fields carry credentials or session identity. The encoder marks them
/// "literal never indexed" so that no intermediary may place them in a shared
/// compression table. A value in a shared table is recoverable by a
/// compression-ratio oracle, which is how CRIME recovered session cookies.
fn neverIndexName(name: []const u8) bool {
    return std.mem.eql(u8, name, "authorization") or
        std.mem.eql(u8, name, "proxy-authorization") or
        std.mem.eql(u8, name, "cookie") or
        std.mem.eql(u8, name, "set-cookie");
}

/// Writes a window of a header block: `skip` bytes are dropped first, then
/// bytes are kept until `out` is full. The rest is counted nowhere, so a
/// caller that knows the block's length reads the window it wants.
const RangeSink = struct {
    out: []u8,
    skip: usize,
    written: usize = 0,

    fn put(self: *RangeSink, bytes: []const u8) void {
        var b = bytes;
        if (self.skip >= b.len) {
            self.skip -= b.len;
            return;
        }
        b = b[self.skip..];
        self.skip = 0;
        const n = @min(b.len, self.out.len - self.written);
        @memcpy(self.out[self.written..][0..n], b[0..n]);
        self.written += n;
    }

    fn putInt(self: *RangeSink, value: usize, prefix_bits: u3, first_byte_hi: u8) void {
        var tmp: [16]u8 = undefined;
        var pos: usize = 0;
        // A usize needs at most 11 bytes at a 4-bit prefix.
        encodeIntInto(&tmp, &pos, value, prefix_bits, first_byte_hi) catch unreachable;
        self.put(tmp[0..pos]);
    }

    fn putString(self: *RangeSink, s: []const u8) void {
        self.putInt(s.len, 7, 0);
        self.put(s);
    }
};

/// `encodeFieldInto`, written through a window. The two must stay the same
/// encoding: a test compares them over a field list.
fn encodeFieldRange(sink: *RangeSink, f: HeaderField) void {
    const full, const name_idx = findStatic(f.name, f.value);
    if (full) |idx| {
        sink.putInt(idx, 7, 0x80);
        return;
    }
    const hi: u8 = if (f.never_index or neverIndexName(f.name)) 0x10 else 0x00;
    if (name_idx) |ni| {
        sink.putInt(ni, 4, hi);
    } else {
        sink.put(&.{hi});
        sink.putString(f.name);
    }
    sink.putString(f.value);
}

pub const Encoder = struct {
    pub fn encodedLen(fields: []const HeaderField) error{Overflow}!usize {
        var n: usize = 0;
        for (fields) |f| {
            n = try std.math.add(usize, n, try encodedFieldLen(f));
        }
        return n;
    }

    pub fn encodeInto(out: []u8, fields: []const HeaderField) error{NoSpace}!usize {
        var pos: usize = 0;
        for (fields) |f| try encodeFieldInto(out, &pos, f);
        return pos;
    }

    /// The bytes of the block `parts[0] ++ parts[1] ++ ...` from byte `skip`
    /// on, as many as fit `out`. This is how a block longer than one frame
    /// is cut into a HEADERS frame and its CONTINUATION frames with no
    /// buffer for the whole block. Returns the bytes written.
    pub fn encodeRange(out: []u8, parts: []const []const HeaderField, skip: usize) usize {
        var sink: RangeSink = .{ .out = out, .skip = skip };
        for (parts) |fields| {
            for (fields) |f| {
                if (sink.written == out.len) return sink.written;
                encodeFieldRange(&sink, f);
            }
        }
        return sink.written;
    }

    pub fn encode(allocator: std.mem.Allocator, fields: []const HeaderField) (error{ OutOfMemory, Overflow })![]u8 {
        const n = try encodedLen(fields);
        const out = try allocator.alloc(u8, n);
        errdefer allocator.free(out);
        const wrote = encodeInto(out, fields) catch unreachable;
        std.debug.assert(wrote == n);
        return out;
    }
};

test "RFC 7541 C.2.1 literal with indexing" {
    // Literal with incremental indexing — name "custom-key", value "custom-header"
    const block = [_]u8{ 0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79, 0x0d, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72 };
    var dec = try Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const res = try dec.decode(&block, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(res);
    try std.testing.expectEqual(@as(usize, 1), res.fields.len);
    try std.testing.expectEqualStrings("custom-key", res.fields[0].name);
    try std.testing.expectEqualStrings("custom-header", res.fields[0].value);
}

test "RFC 7541 C.2.2 literal without indexing" {
    const block = [_]u8{ 0x04, 0x0c, 0x2f, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2f, 0x70, 0x61, 0x74, 0x68 };
    var dec = try Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const res = try dec.decode(&block, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(res);
    try std.testing.expectEqualStrings(":path", res.fields[0].name);
    try std.testing.expectEqualStrings("/sample/path", res.fields[0].value);
}

test "indexed static :method GET" {
    const block = [_]u8{0x82};
    var dec = try Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const res = try dec.decode(&block, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(res);
    try std.testing.expectEqualStrings(":method", res.fields[0].name);
    try std.testing.expectEqualStrings("GET", res.fields[0].value);
}

test "encodeRange cuts the block the way encodeInto writes it, at every window size" {
    const gpa = std.testing.allocator;
    const long: [700]u8 = @splat('v');
    const lead = [_]HeaderField{.{ .name = ":status", .value = "200" }};
    const rest = [_]HeaderField{
        .{ .name = "content-type", .value = "text/plain" },
        .{ .name = "x-custom-name-that-is-not-in-the-static-table", .value = &long },
        .{ .name = "set-cookie", .value = "a=b" },
        .{ .name = ":status", .value = "404" },
        .{ .name = "x-long", .value = &long },
    };
    const parts = [_][]const HeaderField{ &lead, &rest };
    var whole: [4096]u8 = undefined;
    var n = try Encoder.encodeInto(&whole, &lead);
    n += try Encoder.encodeInto(whole[n..], &rest);
    const block = whole[0..n];
    var window: usize = 1;
    while (window < n + 3) : (window += 37) {
        const got = try gpa.alloc(u8, n);
        defer gpa.free(got);
        var off: usize = 0;
        while (off < n) {
            const take = Encoder.encodeRange(got[off..][0..@min(window, n - off)], &parts, off);
            try std.testing.expectEqual(@min(window, n - off), take);
            off += take;
        }
        try std.testing.expectEqualSlices(u8, block, got);
    }
    var past: [8]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), Encoder.encodeRange(&past, &parts, n));
}

test "encoder roundtrip never-index cookie" {
    const fields = [_]HeaderField{.{ .name = "cookie", .value = "a=b" }};
    const enc = try Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(enc);
    try std.testing.expect((enc[0] & 0xf0) == 0x10);
    var dec = try Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const res = try dec.decode(enc, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(res);
    try std.testing.expectEqualStrings("cookie", res.fields[0].name);
}

test "invalid index is compression error" {
    const block = [_]u8{0xff}; // indexed with large value likely OOB after decode
    var dec = try Decoder.init(std.testing.allocator);
    defer dec.deinit();
    // 0xff = indexed 127 — may be OOB
    const result = dec.decode(&block, 100, 32 * 1024, 256, 8 * 1024);
    try std.testing.expectError(error.CompressionError, result);
}

test "indexed static fields are borrowed and freeResult does not free them" {
    // Eight fully-indexed static fields. Copying each name and value would
    // allocate 16 strings plus the result slice; borrowing allocates only the
    // slice (and at most a shrink). FailingAllocator.allocations is the GPA
    // count, not a decoder-reported counter.
    const block = [_]u8{ 0x82, 0x86, 0x84, 0x81, 0x82, 0x86, 0x84, 0x81 };
    var counting = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var dec = try Decoder.init(counting.allocator());
    defer dec.deinit();
    const res = try dec.decode(&block, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(res);
    try std.testing.expectEqual(@as(usize, 8), res.fields.len);
    try std.testing.expect(!res.fields[0].name_owned);
    try std.testing.expect(!res.fields[0].value_owned);
    try std.testing.expectEqual(static_table[1].name.ptr, res.fields[0].name.ptr);
    try std.testing.expectEqual(static_table[1].value.ptr, res.fields[0].value.ptr);
    try std.testing.expectEqual(static_table[5].name.ptr, res.fields[1].name.ptr);
    try std.testing.expectEqual(static_table[5].value.ptr, res.fields[1].value.ptr);
    // Two allocations are the table's own storage, made by `init`.
    try std.testing.expect(counting.allocations <= 4);
}

test "dynamic-indexed result strings survive table eviction" {
    const insert = [_]u8{ 0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79, 0x0d, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72 };
    var dec = try Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const first = try dec.decode(&insert, 100, 32 * 1024, 256, 8 * 1024);
    dec.freeResult(first);

    const indexed = [_]u8{0xbe}; // dynamic index 62
    const res = try dec.decode(&indexed, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(res);
    try std.testing.expectEqual(@as(usize, 1), res.fields.len);
    try std.testing.expect(res.fields[0].name_owned);
    try std.testing.expect(res.fields[0].value_owned);

    const resize = [_]u8{0x20}; // dynamic table size update to 0
    const emptied = try dec.decode(&resize, 100, 32 * 1024, 256, 8 * 1024);
    dec.freeResult(emptied);
    try std.testing.expectEqual(@as(usize, 0), dec.table.len);
    try std.testing.expectEqualStrings("custom-key", res.fields[0].name);
    try std.testing.expectEqualStrings("custom-header", res.fields[0].value);
}

test "request-list allocator is independent of the dynamic table" {
    var arena_inst = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_inst.deinit();
    const arena = arena_inst.allocator();
    var dec = try Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const insert = [_]u8{ 0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79, 0x0d, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72 };
    const res = try dec.decodeInto(arena, arena, &insert, 100, 32 * 1024, 256, 8 * 1024);
    try std.testing.expectEqual(@as(usize, 1), dec.table.len);
    try std.testing.expect(res.fields[0].name.ptr != dec.table.name(0).ptr);
    const emptied = try dec.decodeInto(arena, arena, &[_]u8{0x20}, 100, 32 * 1024, 256, 8 * 1024);
    try std.testing.expectEqual(@as(usize, 0), emptied.fields.len);
    try std.testing.expectEqual(@as(usize, 0), dec.table.len);
    try std.testing.expectEqualStrings("custom-key", res.fields[0].name);
    try std.testing.expectEqualStrings("custom-header", res.fields[0].value);
}

test "literal indexed-name and literal-name ownership" {
    // C.2.2: name is static index 4 (:path), value is a literal.
    const indexed_name = [_]u8{ 0x04, 0x0c, 0x2f, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2f, 0x70, 0x61, 0x74, 0x68 };
    var dec = try Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const borrowed_name = try dec.decode(&indexed_name, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(borrowed_name);
    try std.testing.expect(!borrowed_name.fields[0].name_owned);
    try std.testing.expect(borrowed_name.fields[0].value_owned);
    try std.testing.expectEqual(static_table[3].name.ptr, borrowed_name.fields[0].name.ptr);
    try std.testing.expectEqualStrings("/sample/path", borrowed_name.fields[0].value);

    // New name and value are both literals, so both owned. Encoder literals
    // stay borrowed by default (name_owned/value_owned omitted).
    var both_literal = [_]u8{ 0x00, 0x01, 'n', 0x01, 'v' };
    const owned_both = try dec.decode(&both_literal, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(owned_both);
    try std.testing.expect(owned_both.fields[0].name_owned);
    try std.testing.expect(owned_both.fields[0].value_owned);
    @memset(&both_literal, 0xAA);
    try std.testing.expectEqualStrings("n", owned_both.fields[0].name);
    try std.testing.expectEqualStrings("v", owned_both.fields[0].value);

    const enc_fields = [_]HeaderField{.{ .name = "cookie", .value = "a=b" }};
    try std.testing.expect(!enc_fields[0].name_owned);
    try std.testing.expect(!enc_fields[0].value_owned);
}

test "Huffman decode is owned plaintext, not a view of the encoded slice" {
    // RFC 7541 C.4.1 `:authority` www.example.com
    var enc = [_]u8{ 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff };
    const out = try decodeHuffman(std.testing.allocator, &enc);
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("www.example.com", out);
    try std.testing.expect(@intFromPtr(out.ptr) != @intFromPtr(&enc));
    @memset(&enc, 0);
    try std.testing.expectEqualStrings("www.example.com", out);
}

test "Huffman padding is EOS prefix and EOS itself is a compression error" {
    // '0' is 5 zero bits; three trailing ones fill the byte (RFC 7541 §5.2).
    const padded_zero = try decodeHuffman(std.testing.allocator, &[_]u8{0x07});
    defer std.testing.allocator.free(padded_zero);
    try std.testing.expectEqualSlices(u8, "0", padded_zero);

    try std.testing.expectError(error.CompressionError, decodeHuffman(std.testing.allocator, &[_]u8{0x00}));
    try std.testing.expectError(error.CompressionError, decodeHuffman(std.testing.allocator, &[_]u8{ 0xff, 0xff, 0xff, 0xff }));
}

fn expectEncodeMatch(fields: []const HeaderField, expected: []const u8) !void {
    try std.testing.expectEqual(expected.len, try Encoder.encodedLen(fields));
    var into_buf: [256]u8 = undefined;
    try std.testing.expect(expected.len <= into_buf.len);
    const n = try Encoder.encodeInto(into_buf[0..], fields);
    try std.testing.expectEqualSlices(u8, expected, into_buf[0..n]);
    const alloced = try Encoder.encode(std.testing.allocator, fields);
    defer std.testing.allocator.free(alloced);
    try std.testing.expectEqualSlices(u8, expected, alloced);
}

test "encodeInto matches encode and RFC bytes for each encoding class" {
    try expectEncodeMatch(&[_]HeaderField{.{ .name = ":method", .value = "GET" }}, &[_]u8{0x82});

    const sample_path = [_]u8{ 0x04, 0x0c, 0x2f, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2f, 0x70, 0x61, 0x74, 0x68 };
    try expectEncodeMatch(&[_]HeaderField{.{ .name = ":path", .value = "/sample/path" }}, &sample_path);

    const custom = [_]u8{
        0x00, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79,
        0x0d, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72,
    };
    try expectEncodeMatch(&[_]HeaderField{.{ .name = "custom-key", .value = "custom-header" }}, &custom);

    const cookie = [_]u8{ 0x1f, 0x11, 0x03, 'a', '=', 'b' };
    try expectEncodeMatch(&[_]HeaderField{.{ .name = "cookie", .value = "a=b" }}, &cookie);

    var long_val: [127]u8 = undefined;
    @memset(&long_val, 'x');
    const long_fields = [_]HeaderField{.{ .name = ":path", .value = &long_val }};
    var long_expected: [130]u8 = undefined;
    long_expected[0] = 0x04;
    long_expected[1] = 0x7f;
    long_expected[2] = 0x00;
    @memset(long_expected[3..], 'x');
    try expectEncodeMatch(&long_fields, &long_expected);
}

test "encodeInto undersized output is NoSpace without writing past the slice" {
    const fields = [_]HeaderField{.{ .name = ":method", .value = "GET" }};
    const exact = try Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(exact);
    try std.testing.expect(exact.len >= 1);
    const short = try std.testing.allocator.alloc(u8, exact.len - 1);
    defer std.testing.allocator.free(short);
    try std.testing.expectError(error.NoSpace, Encoder.encodeInto(short, &fields));
    try std.testing.expectError(error.NoSpace, Encoder.encodeInto(&.{}, &fields));
}

test "Encoder.encode allocates exactly once" {
    const fields = [_]HeaderField{
        .{ .name = ":status", .value = "200" },
        .{ .name = "content-type", .value = "text/plain" },
        .{ .name = "custom-key", .value = "custom-header" },
        .{ .name = "cookie", .value = "a=b" },
    };
    var counting = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const enc = try Encoder.encode(counting.allocator(), &fields);
    defer counting.allocator().free(enc);
    try std.testing.expectEqual(@as(usize, 1), counting.allocations);
    try std.testing.expectEqual(try Encoder.encodedLen(&fields), enc.len);
}

/// Encode `s` as a Huffman string literal body (RFC 7541 section 5.2), for
/// the tests below. Returns the encoded bytes.
fn testHuffmanEncode(allocator: std.mem.Allocator, s: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var acc: u64 = 0;
    var bits: u8 = 0;
    for (s) |c| {
        const pair = huffmanPair(c);
        acc = (acc << @intCast(pair.bits)) | pair.code;
        bits += pair.bits;
        while (bits >= 8) {
            bits -= 8;
            try out.append(allocator, @intCast((acc >> @intCast(bits)) & 0xff));
        }
    }
    if (bits > 0) {
        const pad: u8 = 8 - bits;
        acc = (acc << @intCast(pad)) | ((@as(u64, 1) << @intCast(pad)) - 1);
        try out.append(allocator, @intCast(acc & 0xff));
    }
    return out.toOwnedSlice(allocator);
}

/// `0x40`, a new name and a value, raw, as a literal with incremental indexing.
fn testLiteral(list: *std.ArrayList(u8), allocator: std.mem.Allocator, prefix: u8, name: []const u8, value: []const u8, value_huffman: bool) !void {
    try list.append(allocator, prefix);
    try testAppendString(list, allocator, name, false);
    try testAppendString(list, allocator, value, value_huffman);
}

fn testAppendString(list: *std.ArrayList(u8), allocator: std.mem.Allocator, s: []const u8, huffman: bool) !void {
    const body = if (huffman) try testHuffmanEncode(allocator, s) else s;
    defer if (huffman) allocator.free(body);
    // 7-bit prefix integer.
    var len = body.len;
    const flag: u8 = if (huffman) 0x80 else 0;
    if (len < 127) {
        try list.append(allocator, flag | @as(u8, @intCast(len)));
    } else {
        try list.append(allocator, flag | 127);
        len -= 127;
        while (len >= 128) : (len /= 128) try list.append(allocator, @intCast(len % 128 + 128));
        try list.append(allocator, @intCast(len));
    }
    try list.appendSlice(allocator, body);
}

/// What the table must be, written the plain way: entries newest first, each
/// its own pair of strings, evicted from the end while the RFC's size is over
/// the limit.
const ModelTable = struct {
    gpa: std.mem.Allocator,
    entries: std.ArrayList(struct { name: []u8, value: []u8 }) = .empty,
    max_size: usize = MAX_DYNAMIC_TABLE,

    fn deinit(self: *ModelTable) void {
        self.clear();
        self.entries.deinit(self.gpa);
    }

    fn clear(self: *ModelTable) void {
        for (self.entries.items) |e| {
            self.gpa.free(e.name);
            self.gpa.free(e.value);
        }
        self.entries.clearRetainingCapacity();
    }

    fn size(self: *const ModelTable) usize {
        var n: usize = 0;
        for (self.entries.items) |e| n += e.name.len + e.value.len + 32;
        return n;
    }

    fn evictToFit(self: *ModelTable, limit: usize) void {
        while (self.size() > limit) {
            const e = self.entries.pop().?;
            self.gpa.free(e.name);
            self.gpa.free(e.value);
        }
    }

    fn insert(self: *ModelTable, name: []const u8, value: []const u8) !void {
        const entry_size = name.len + value.len + 32;
        if (entry_size > self.max_size) return self.clear();
        self.evictToFit(self.max_size - entry_size);
        const n = try self.gpa.dupe(u8, name);
        errdefer self.gpa.free(n);
        const v = try self.gpa.dupe(u8, value);
        errdefer self.gpa.free(v);
        try self.entries.insert(self.gpa, 0, .{ .name = n, .value = v });
    }

    fn setMaxSize(self: *ModelTable, new_size: usize) void {
        self.max_size = new_size;
        self.evictToFit(new_size);
    }
};

/// `from_start`: the entries begin at offset 0. True after an insert, which
/// compacts; an eviction on its own (a size change) leaves the gap until then.
fn expectTableMatches(table: *const DynTable, model: *const ModelTable, from_start: bool) !void {
    try std.testing.expectEqual(model.entries.items.len, table.len);
    try std.testing.expectEqual(model.size(), table.size);
    var bytes: usize = 0;
    for (model.entries.items, 0..) |e, i| {
        try std.testing.expectEqualStrings(e.name, table.name(i));
        try std.testing.expectEqualStrings(e.value, table.value(i));
        bytes += e.name.len + e.value.len;
    }
    // The bytes are packed: `used` is exactly the strings, with no +32 in
    // them, and the entries follow each other from the start of the storage.
    try std.testing.expectEqual(bytes, table.used);
    try std.testing.expect(table.size <= table.max_size);
    if (table.len == 0) return;
    var at: usize = table.entryAt(table.len - 1).off;
    if (from_start) try std.testing.expectEqual(@as(usize, 0), at);
    var k: usize = table.len;
    while (k > 0) {
        k -= 1; // oldest first
        const e = table.entryAt(k).*;
        try std.testing.expectEqual(at, @as(usize, e.off));
        at += @as(usize, e.name_len) + e.value_len;
    }
    try std.testing.expect(at <= table.data.len);
}

test "DynTable: random inserts, evictions and size changes match a plain model, and the bytes stay packed" {
    const gpa = std.testing.allocator;
    var dec = try Decoder.init(gpa);
    defer dec.deinit();
    var model: ModelTable = .{ .gpa = gpa };
    defer model.deinit();
    var rng = std.Random.DefaultPrng.init(0x7ab1e);
    const r = rng.random();
    var bytes_buf: [MAX_DYNAMIC_TABLE + 64]u8 = undefined;
    r.bytes(&bytes_buf);
    var inserted: usize = 0;
    var evicting_inserts: usize = 0;
    var cleared_by_big: usize = 0;
    for (0..20_000) |step| {
        var from_start = false;
        switch (r.uintLessThan(u8, 100)) {
            // Most inserts are small, some fill the table, a few are bigger
            // than it and must empty it.
            0...89 => {
                const cap: usize = switch (r.uintLessThan(u8, 10)) {
                    0 => MAX_DYNAMIC_TABLE + 8,
                    1, 2 => 1500,
                    else => 90,
                };
                const name_len = r.uintLessThan(usize, cap / 3 + 1);
                const value_len = r.uintLessThan(usize, cap + 1);
                const name = bytes_buf[r.uintLessThan(usize, 32)..][0..name_len];
                const value = bytes_buf[r.uintLessThan(usize, 32)..][0..value_len];
                const before = model.entries.items.len;
                const was_full = model.size() + name_len + value_len + 32 > model.max_size;
                if (name_len + value_len + 32 > model.max_size and before > 0) cleared_by_big += 1;
                if (was_full and name_len + value_len + 32 <= model.max_size) evicting_inserts += 1;
                try model.insert(name, value);
                dec.table.insert(name, value);
                inserted += 1;
                from_start = true;
            },
            90...94 => {
                const new_size = switch (r.uintLessThan(u8, 4)) {
                    0 => 0,
                    1 => MAX_DYNAMIC_TABLE,
                    else => r.uintLessThan(usize, MAX_DYNAMIC_TABLE + 1),
                };
                model.setMaxSize(new_size);
                dec.table.setMaxSize(new_size);
            },
            else => {
                // A size update through the decoder, as a block carries it.
                const new_size = r.uintLessThan(usize, MAX_DYNAMIC_TABLE + 1);
                var upd: [4]u8 = undefined;
                var pos: usize = 0;
                try encodeIntInto(&upd, &pos, new_size, 5, 0x20);
                const res = try dec.decode(upd[0..pos], 10, 1000, 100, 100);
                dec.freeResult(res);
                model.setMaxSize(new_size);
            },
        }
        expectTableMatches(&dec.table, &model, from_start) catch |err| {
            std.debug.print("table differs from the model at step {d}\n", .{step});
            return err;
        };
    }
    // The sequence did what its name says: it evicted, and it emptied the
    // table with an entry larger than it.
    try std.testing.expect(inserted > 10_000);
    try std.testing.expect(evicting_inserts > 500);
    try std.testing.expect(cleared_by_big > 20);
}

test "DynTable: fill to the limit, evict the oldest, compact, and the survivors keep their bytes" {
    const gpa = std.testing.allocator;
    var dec = try Decoder.init(gpa);
    defer dec.deinit();
    const t = &dec.table;
    // Three entries of 1,300 accounted bytes each (1,268 of storage: a
    // 4-byte name and a 1,264-byte value) fill 3,900 of the 4,096.
    var value: [1268 - 4]u8 = undefined;
    for (0..3) |k| {
        @memset(&value, @intCast('a' + k));
        t.insert("name", &value);
    }
    try std.testing.expectEqual(@as(usize, 3), t.len);
    try std.testing.expectEqual(@as(usize, 3 * 1300), t.size);
    try std.testing.expectEqual(@as(usize, 3 * 1268), t.used);
    // A fourth does not fit: the oldest ('a') goes, the rest move to the front.
    @memset(&value, 'd');
    t.insert("name", &value);
    try std.testing.expectEqual(@as(usize, 3), t.len);
    try std.testing.expectEqual(@as(u8, 'd'), t.value(0)[0]);
    try std.testing.expectEqual(@as(u8, 'c'), t.value(1)[0]);
    try std.testing.expectEqual(@as(u8, 'b'), t.value(2)[0]);
    try std.testing.expectEqual(@as(usize, 0), t.entryAt(2).off);
    try std.testing.expectEqual(@as(usize, 3 * 1268), t.used);
    for (0..t.len) |i| {
        const want: u8 = @intCast('d' - i);
        for (t.value(i)) |b| try std.testing.expectEqual(want, b);
        try std.testing.expectEqualStrings("name", t.name(i));
    }
}

test "DynTable: a size update to 0 empties the table; one above the limit is a compression error" {
    const gpa = std.testing.allocator;
    var dec = try Decoder.init(gpa);
    defer dec.deinit();
    dec.table.insert("a", "b");
    dec.table.insert("c", "d");
    try std.testing.expectEqual(@as(usize, 2), dec.table.len);

    const to_zero = try dec.decode(&[_]u8{0x20}, 10, 1000, 100, 100);
    dec.freeResult(to_zero);
    try std.testing.expectEqual(@as(usize, 0), dec.table.len);
    try std.testing.expectEqual(@as(usize, 0), dec.table.size);
    // And nothing fits until the size is raised again.
    dec.table.insert("a", "b");
    try std.testing.expectEqual(@as(usize, 0), dec.table.len);

    // 4096 is the limit and is accepted; 4097 is not. (5-bit prefix: 31 and
    // the rest in 7-bit groups.)
    const at_limit = try dec.decode(&[_]u8{ 0x3f, 0xe1, 0x1f }, 10, 1000, 100, 100);
    dec.freeResult(at_limit);
    try std.testing.expectEqual(MAX_DYNAMIC_TABLE, dec.table.max_size);
    try std.testing.expectError(error.CompressionError, dec.decode(&[_]u8{ 0x3f, 0xe2, 0x1f }, 10, 1000, 100, 100));
    // A size update after the first field of a block is also an error.
    try std.testing.expectError(error.CompressionError, dec.decode(&[_]u8{ 0x82, 0x20 }, 10, 1000, 100, 100));
}

test "a field past the request's budget still enters the table, whole, from the scratch" {
    // A budget of one field: the second literal (incrementally indexed) is
    // past it, so it is truncated, and the table must still get it exactly.
    const gpa = std.testing.allocator;
    var dec = try Decoder.init(gpa);
    defer dec.deinit();
    var block: std.ArrayList(u8) = .empty;
    defer block.deinit(gpa);
    var long_value: [900]u8 = undefined;
    for (&long_value, 0..) |*b, i| b.* = @intCast('a' + i % 26);
    try testLiteral(&block, gpa, 0x40, "x-first", "1", false);
    // A Huffman value, a raw name; and a name by (static) index with a Huffman value.
    try testLiteral(&block, gpa, 0x40, "x-second", &long_value, true);
    try testLiteral(&block, gpa, 0x40, "x-third", "3", false);

    var scratch_buf: [MAX_DYNAMIC_TABLE]u8 = undefined;
    var scratch = std.heap.FixedBufferAllocator.init(&scratch_buf);
    var arena_inst = std.heap.ArenaAllocator.init(gpa);
    defer arena_inst.deinit();
    const res = try dec.decodeInto(arena_inst.allocator(), scratch.allocator(), block.items, 1, 32 * 1024, 256, 8 * 1024);
    try std.testing.expect(res.truncated);
    try std.testing.expectEqual(@as(usize, 1), res.fields.len);
    // Everything was given back to the scratch.
    try std.testing.expectEqual(@as(usize, 0), scratch.end_index);

    try std.testing.expectEqual(@as(usize, 3), dec.table.len);
    try std.testing.expectEqualStrings("x-third", dec.table.name(0));
    try std.testing.expectEqualStrings("x-second", dec.table.name(1));
    try std.testing.expectEqualSlices(u8, &long_value, dec.table.value(1));
    try std.testing.expectEqualStrings("x-first", dec.table.name(2));

    // And an entry bigger than the table, past the budget, empties it.
    var huge: [MAX_DYNAMIC_TABLE]u8 = @splat('h');
    var big_block: std.ArrayList(u8) = .empty;
    defer big_block.deinit(gpa);
    try testLiteral(&big_block, gpa, 0x40, "x-one", "1", false);
    try testLiteral(&big_block, gpa, 0x40, "x-huge", &huge, false);
    const res2 = try dec.decodeInto(arena_inst.allocator(), scratch.allocator(), big_block.items, 1, 32 * 1024, 256, 8 * 1024);
    try std.testing.expect(res2.truncated or res2.policy_exceeded);
    try std.testing.expectEqual(@as(usize, 0), dec.table.len);
}

const bomb_limits = struct {
    const max_fields = 100;
    const max_decoded = 32 * 1024;
    const max_name = 256;
    const max_value = 8 * 1024;
};

test "decode takes at most requestBytesBound from the request allocator: a table bomb" {
    // One 4000-byte table entry, then 64 KiB of one-byte references to it:
    // 65,000 fields and about 260 MB if every one were materialized.
    const gpa = std.testing.allocator;
    var dec = try Decoder.init(gpa);
    defer dec.deinit();
    var big: [4000 - 32 - 1]u8 = undefined;
    @memset(&big, 'v');
    var insert: std.ArrayList(u8) = .empty;
    defer insert.deinit(gpa);
    try testLiteral(&insert, gpa, 0x40, "x", &big, false);
    const seeded = try dec.decode(insert.items, 100, 32 * 1024, 256, 8 * 1024);
    dec.freeResult(seeded);

    const block = try gpa.alloc(u8, 64 * 1024);
    defer gpa.free(block);
    @memset(block, 0xbe); // indexed, 62 + 1 + 0x3e... the first dynamic entry
    block[0] = 0x80 | 62;
    @memset(block[1..], 0x80 | 62);

    const bound = try Decoder.requestBytesBound(bomb_limits.max_fields, bomb_limits.max_decoded);
    const buf = try gpa.alloc(u8, bound);
    defer gpa.free(buf);
    var fba = std.heap.FixedBufferAllocator.init(buf);
    const res = try dec.decodeInto(fba.allocator(), std.testing.allocator, block, bomb_limits.max_fields, bomb_limits.max_decoded, bomb_limits.max_name, bomb_limits.max_value);
    try std.testing.expect(res.truncated);
    try std.testing.expect(res.policy_exceeded);
    try std.testing.expect(fba.end_index <= bound);
    // 4000 + 1 + 32 bytes per field: eight fields fit 32 KiB.
    try std.testing.expect(res.fields.len >= 1 and res.fields.len <= 9);
    try std.testing.expect(res.decoded_size > 32 * 1024);
    // The table is intact: the same reference still decodes.
    const again = try dec.decode(&[_]u8{0x80 | 62}, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(again);
    try std.testing.expectEqual(@as(usize, 4000 - 32 - 1), again.fields[0].value.len);
}

test "decode takes at most requestBytesBound: a string past the budget never reaches the request allocator" {
    const gpa = std.testing.allocator;
    var dec = try Decoder.init(gpa);
    defer dec.deinit();
    const bound = try Decoder.requestBytesBound(bomb_limits.max_fields, bomb_limits.max_decoded);
    const buf = try gpa.alloc(u8, bound);
    defer gpa.free(buf);

    // A 60 KB raw value, and a 60 KB value in Huffman form (about 45 KB
    // encoded, 60 KB decoded): both larger than the 32 KiB budget.
    var huge: [60 * 1024]u8 = undefined;
    @memset(&huge, 'a');
    for ([_]bool{ false, true }) |huffman| {
        for ([_]u8{ 0x40, 0x00, 0x10 }) |prefix| {
            var block: std.ArrayList(u8) = .empty;
            defer block.deinit(gpa);
            try testLiteral(&block, gpa, prefix, "x-big", &huge, huffman);
            var fba = std.heap.FixedBufferAllocator.init(buf);
            const res = try dec.decodeInto(fba.allocator(), std.testing.allocator, block.items, 100, bomb_limits.max_decoded, 256, 8 * 1024);
            try std.testing.expect(res.truncated);
            try std.testing.expect(res.policy_exceeded);
            try std.testing.expectEqual(@as(usize, 0), res.fields.len);
            // Only the (empty) field array was taken.
            try std.testing.expect(fba.end_index <= 100 * @sizeOf(HeaderField));
        }
    }
}

test "a Huffman field whose upper bound passes the budget but whose real size fits is kept" {
    // 20,000 'e' characters Huffman-encode to 20,000 * 5 / 8 = 12,500 bytes,
    // upper bound 20,000. Put them after 20,000 bytes of other fields so that
    // upper + used passes 32 KiB while the real size does not.
    const gpa = std.testing.allocator;
    var dec = try Decoder.init(gpa);
    defer dec.deinit();
    var plain: [20000]u8 = undefined;
    @memset(&plain, 'p');
    var e_run: [12000]u8 = undefined;
    @memset(&e_run, 'e');
    var block: std.ArrayList(u8) = .empty;
    defer block.deinit(gpa);
    try testLiteral(&block, gpa, 0x00, "x-first", &plain, false);
    try testLiteral(&block, gpa, 0x00, "x-second", &e_run, true);
    // 20000 + 7 + 32 + 12000 + 8 + 32 < 32768; the upper bound of the second
    // is 12000 (raw) vs the Huffman string's 7500 encoded bytes -> upper 12000.
    // Make it fail the cheap test by adding a third field first.
    var block2: std.ArrayList(u8) = .empty;
    defer block2.deinit(gpa);
    try testLiteral(&block2, gpa, 0x00, "x-first", &plain, false);
    var filler: [600]u8 = undefined;
    @memset(&filler, 'f');
    try testLiteral(&block2, gpa, 0x00, "x-filler", &filler, false);
    try testLiteral(&block2, gpa, 0x00, "x-second", &e_run, true);

    const bound = try Decoder.requestBytesBound(100, 32 * 1024);
    const buf = try gpa.alloc(u8, bound);
    defer gpa.free(buf);
    for ([_][]const u8{ block.items, block2.items }) |b| {
        var fba = std.heap.FixedBufferAllocator.init(buf);
        const res = try dec.decodeInto(fba.allocator(), std.testing.allocator, b, 100, 32 * 1024, 256, 8 * 1024);
        try std.testing.expect(!res.truncated);
        const last = res.fields[res.fields.len - 1];
        try std.testing.expectEqualStrings("x-second", last.name);
        try std.testing.expectEqualSlices(u8, &e_run, last.value);
        try std.testing.expect(fba.end_index <= bound);
    }
}

test "a random block never makes decode fail on a bounded request allocator" {
    // Not a proof, an adversarial sample: blocks of random bytes and blocks
    // built from valid pieces, against a decoder whose table already holds
    // large entries, each decoded in an allocator of exactly the bound.
    const gpa = std.testing.allocator;
    var dec = try Decoder.init(gpa);
    defer dec.deinit();
    var seed_block: std.ArrayList(u8) = .empty;
    defer seed_block.deinit(gpa);
    var v: [1500]u8 = undefined;
    @memset(&v, 'z');
    try testLiteral(&seed_block, gpa, 0x40, "alpha", &v, false);
    try testLiteral(&seed_block, gpa, 0x40, "beta", &v, true);
    const seeded = try dec.decode(seed_block.items, 100, 32 * 1024, 256, 8 * 1024);
    dec.freeResult(seeded);

    const bound = try Decoder.requestBytesBound(100, 32 * 1024);
    const buf = try gpa.alloc(u8, bound);
    defer gpa.free(buf);
    var rng = std.Random.DefaultPrng.init(0x5eed);
    var value_buf: [3000]u8 = undefined;
    @memset(&value_buf, 'q');
    var decoded_ok: usize = 0;
    var max_used: usize = 0;
    for (0..4000) |_| {
        var built: std.ArrayList(u8) = .empty;
        defer built.deinit(gpa);
        const pieces = rng.random().uintLessThan(usize, 60);
        for (0..pieces) |_| {
            switch (rng.random().uintLessThan(u8, 5)) {
                0 => try built.append(gpa, 0x80 | (62 + rng.random().uintLessThan(u8, 2))),
                1 => try built.append(gpa, 0x80 | (1 + rng.random().uintLessThan(u8, 61))),
                2 => try testLiteral(&built, gpa, 0x40, "n", value_buf[0..rng.random().uintLessThan(usize, 3000)], rng.random().boolean()),
                3 => try testLiteral(&built, gpa, 0x00, "x-name", value_buf[0..rng.random().uintLessThan(usize, 3000)], rng.random().boolean()),
                else => try built.append(gpa, rng.random().int(u8)),
            }
        }
        const block = built.items;
        var fba = std.heap.FixedBufferAllocator.init(buf);
        if (dec.decodeInto(fba.allocator(), std.testing.allocator, block, 100, 32 * 1024, 256, 8 * 1024)) |res| {
            decoded_ok += 1;
            HeaderField.freeOwnedSlice(fba.allocator(), res.fields);
        } else |err| {
            // A compression error is the peer's fault. Running out of room is ours.
            try std.testing.expect(err == error.CompressionError);
        }
        try std.testing.expect(fba.end_index <= bound);
        max_used = @max(max_used, fba.end_index);
        // A bad block can leave the table in any state; start over when it
        // grew past what the seed block built.
        if (dec.table.len > 6) {
            dec.table.clear();
            const again = try dec.decode(seed_block.items, 100, 32 * 1024, 256, 8 * 1024);
            dec.freeResult(again);
        }
    }
    try std.testing.expect(decoded_ok > 100);
    // The sample reached the budget, or it proved nothing about it.
    try std.testing.expect(max_used > 32 * 1024 / 2);
}
