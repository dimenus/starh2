//! HPACK decoder/encoder (RFC 7541). Encoder: static indices + literal never/without indexing only.
const std = @import("std");

pub const MAX_DYNAMIC_TABLE: usize = 4096;
pub const CompressionError = error{CompressionError};

pub const HeaderField = struct {
    name: []const u8,
    value: []const u8,
    never_index: bool = false,
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

fn huffDecode(allocator: std.mem.Allocator, encoded: []const u8) (CompressionError || error{OutOfMemory})![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var acc: u64 = 0;
    var bits: u8 = 0;
    for (encoded) |byte| {
        acc = (acc << 8) | byte;
        bits += 8;
        decode_loop: while (bits > 0) {
            var found = false;
            var sym: usize = 0;
            while (sym < 256) : (sym += 1) {
                const p = huff_pairs[sym];
                if (p.bits == 0 or p.bits > bits) continue;
                const shift: u6 = @intCast(bits - p.bits);
                const got: u32 = @truncate(acc >> shift);
                const mask: u32 = if (p.bits == 32) 0xffff_ffff else (@as(u32, 1) << @intCast(p.bits)) - 1;
                if ((got & mask) == p.code) {
                    try out.append(allocator, @intCast(sym));
                    bits -= p.bits;
                    acc &= (@as(u64, 1) << @intCast(bits)) - 1;
                    found = true;
                    continue :decode_loop;
                }
            }
            const eos = huff_pairs[256];
            if (eos.bits != 0 and eos.bits <= bits) {
                const shift: u6 = @intCast(bits - eos.bits);
                const got: u32 = @truncate(acc >> shift);
                if (got == eos.code) return error.CompressionError;
            }
            if (!found) break;
        }
    }
    if (bits >= 8) return error.CompressionError;
    if (bits > 0) {
        const pad_mask: u64 = (@as(u64, 1) << @intCast(bits)) - 1;
        if ((acc & pad_mask) != pad_mask) return error.CompressionError;
    }
    return try out.toOwnedSlice(allocator);
}

fn decodeString(allocator: std.mem.Allocator, data: []const u8, start: *usize) (CompressionError || error{OutOfMemory})![]u8 {
    if (start.* >= data.len) return error.CompressionError;
    const huffman = (data[start.*] & 0x80) != 0;
    const len = try decodeInt(data, 7, start);
    if (start.* + len > data.len) return error.CompressionError;
    const slice = data[start.* .. start.* + len];
    start.* += len;
    if (huffman) return huffDecode(allocator, slice);
    return try allocator.dupe(u8, slice);
}

fn decodeInt(data: []const u8, prefix_bits: u3, start: *usize) CompressionError!usize {
    if (start.* >= data.len) return error.CompressionError;
    const mask: u8 = @as(u8, 0xff) >> @as(u3, @intCast(8 - @as(u8, prefix_bits)));
    var value: usize = data[start.*] & mask;
    start.* += 1;
    if (value < mask) return value;
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

const DynEntry = struct {
    name: []u8,
    value: []u8,
    size: usize,
};

pub const Decoder = struct {
    allocator: std.mem.Allocator,
    dyn: std.ArrayList(DynEntry) = .empty,
    dyn_size: usize = 0,
    max_size: usize = MAX_DYNAMIC_TABLE,

    pub const DecodeResult = struct {
        fields: []HeaderField,
        decoded_size: usize,
        policy_exceeded: bool = false,
    };

    pub fn init(allocator: std.mem.Allocator) Decoder {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Decoder) void {
        self.clearDyn();
        self.dyn.deinit(self.allocator);
    }

    fn clearDyn(self: *Decoder) void {
        for (self.dyn.items) |e| {
            self.allocator.free(e.name);
            self.allocator.free(e.value);
        }
        self.dyn.clearRetainingCapacity();
        self.dyn_size = 0;
    }

    fn setMaxSize(self: *Decoder, new_size: usize) CompressionError!void {
        if (new_size > MAX_DYNAMIC_TABLE) return error.CompressionError;
        self.max_size = new_size;
        while (self.dyn_size > self.max_size and self.dyn.items.len > 0) {
            const e = self.dyn.orderedRemove(self.dyn.items.len - 1);
            self.dyn_size -= e.size;
            self.allocator.free(e.name);
            self.allocator.free(e.value);
        }
    }

    fn insert(self: *Decoder, name: []u8, value: []u8) (CompressionError || error{OutOfMemory})!void {
        const size = name.len + value.len + 32;
        if (size > self.max_size) {
            self.allocator.free(name);
            self.allocator.free(value);
            self.clearDyn();
            return;
        }
        while (self.dyn_size + size > self.max_size and self.dyn.items.len > 0) {
            const e = self.dyn.orderedRemove(self.dyn.items.len - 1);
            self.dyn_size -= e.size;
            self.allocator.free(e.name);
            self.allocator.free(e.value);
        }
        self.dyn.insert(self.allocator, 0, .{ .name = name, .value = value, .size = size }) catch {
            self.allocator.free(name);
            self.allocator.free(value);
            return error.OutOfMemory;
        };
        self.dyn_size += size;
    }

    fn lookup(self: *const Decoder, index: usize) CompressionError!struct { []const u8, []const u8 } {
        if (index == 0) return error.CompressionError;
        if (index <= static_table.len) {
            const e = static_table[index - 1];
            return .{ e.name, e.value };
        }
        const dyn_index = index - static_table.len - 1;
        if (dyn_index >= self.dyn.items.len) return error.CompressionError;
        const e = self.dyn.items[dyn_index];
        return .{ e.name, e.value };
    }

    pub fn decode(
        self: *Decoder,
        block: []const u8,
        max_fields: usize,
        max_decoded_size: usize,
        max_name: usize,
        max_value: usize,
    ) (CompressionError || error{OutOfMemory})!DecodeResult {
        var fields: std.ArrayList(HeaderField) = .empty;
        errdefer {
            for (fields.items) |f| {
                self.allocator.free(@constCast(f.name));
                self.allocator.free(@constCast(f.value));
            }
            fields.deinit(self.allocator);
        }
        var decoded_size: usize = 0;
        var policy = false;
        var i: usize = 0;
        var first = true;
        while (i < block.len) {
            const b = block[i];
            if ((b & 0x80) != 0) {
                const idx = try decodeInt(block, 7, &i);
                const pair = try self.lookup(idx);
                const name = try self.allocator.dupe(u8, pair[0]);
                errdefer self.allocator.free(name);
                const value = try self.allocator.dupe(u8, pair[1]);
                errdefer self.allocator.free(value);
                decoded_size += name.len + value.len + 32;
                if (fields.items.len >= max_fields or decoded_size > max_decoded_size or name.len > max_name or value.len > max_value) policy = true;
                try fields.append(self.allocator, .{ .name = name, .value = value });
                first = false;
            } else if ((b & 0xc0) == 0x40) {
                const idx = try decodeInt(block, 6, &i);
                const name = if (idx == 0)
                    try decodeString(self.allocator, block, &i)
                else blk: {
                    const pair = try self.lookup(idx);
                    break :blk (try self.allocator.dupe(u8, pair[0]));
                };
                errdefer self.allocator.free(name);
                const value = try decodeString(self.allocator, block, &i);
                errdefer self.allocator.free(value);
                decoded_size += name.len + value.len + 32;
                if (fields.items.len >= max_fields or decoded_size > max_decoded_size or name.len > max_name or value.len > max_value) policy = true;
                const n2 = try self.allocator.dupe(u8, name);
                errdefer self.allocator.free(n2);
                const v2 = try self.allocator.dupe(u8, value);
                errdefer self.allocator.free(v2);
                try self.insert(n2, v2);
                try fields.append(self.allocator, .{ .name = name, .value = value });
                first = false;
            } else if ((b & 0xe0) == 0x20) {
                if (!first) return error.CompressionError;
                const new_size = try decodeInt(block, 5, &i);
                try self.setMaxSize(new_size);
            } else {
                const never = (b & 0xf0) == 0x10;
                const idx = try decodeInt(block, 4, &i);
                const name = if (idx == 0)
                    try decodeString(self.allocator, block, &i)
                else blk: {
                    const pair = try self.lookup(idx);
                    break :blk (try self.allocator.dupe(u8, pair[0]));
                };
                errdefer self.allocator.free(name);
                const value = try decodeString(self.allocator, block, &i);
                errdefer self.allocator.free(value);
                decoded_size += name.len + value.len + 32;
                if (fields.items.len >= max_fields or decoded_size > max_decoded_size or name.len > max_name or value.len > max_value) policy = true;
                try fields.append(self.allocator, .{ .name = name, .value = value, .never_index = never });
                first = false;
            }
        }
        return .{
            .fields = try fields.toOwnedSlice(self.allocator),
            .decoded_size = decoded_size,
            .policy_exceeded = policy,
        };
    }

    pub fn freeResult(self: *Decoder, result: DecodeResult) void {
        for (result.fields) |f| {
            self.allocator.free(@constCast(f.name));
            self.allocator.free(@constCast(f.value));
        }
        self.allocator.free(result.fields);
    }
};

fn encodeInt(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, value: usize, prefix_bits: u3, first_byte_hi: u8) !void {
    const mask: usize = (@as(usize, 1) << prefix_bits) - 1;
    if (value < mask) {
        try buf.append(allocator, first_byte_hi | @as(u8, @intCast(value)));
        return;
    }
    try buf.append(allocator, first_byte_hi | @as(u8, @intCast(mask)));
    var v = value - mask;
    while (v >= 128) {
        try buf.append(allocator, @as(u8, @intCast((v % 128) + 128)));
        v /= 128;
    }
    try buf.append(allocator, @intCast(v));
}

fn encodeStringLiteral(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    try encodeInt(buf, allocator, s.len, 7, 0);
    try buf.appendSlice(allocator, s);
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

fn neverIndexName(name: []const u8) bool {
    return std.mem.eql(u8, name, "authorization") or
        std.mem.eql(u8, name, "proxy-authorization") or
        std.mem.eql(u8, name, "cookie") or
        std.mem.eql(u8, name, "set-cookie");
}

pub const Encoder = struct {
    pub fn encode(allocator: std.mem.Allocator, fields: []const HeaderField) ![]u8 {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(allocator);
        for (fields) |f| {
            const full, const name_idx = findStatic(f.name, f.value);
            if (full) |idx| {
                try encodeInt(&buf, allocator, idx, 7, 0x80);
                continue;
            }
            const never = f.never_index or neverIndexName(f.name);
            if (never) {
                if (name_idx) |ni| {
                    try encodeInt(&buf, allocator, ni, 4, 0x10);
                } else {
                    try buf.append(allocator, 0x10);
                    try encodeStringLiteral(&buf, allocator, f.name);
                }
                try encodeStringLiteral(&buf, allocator, f.value);
            } else {
                if (name_idx) |ni| {
                    try encodeInt(&buf, allocator, ni, 4, 0x00);
                } else {
                    try buf.append(allocator, 0x00);
                    try encodeStringLiteral(&buf, allocator, f.name);
                }
                try encodeStringLiteral(&buf, allocator, f.value);
            }
        }
        return buf.toOwnedSlice(allocator);
    }
};

test "RFC 7541 C.2.1 literal with indexing" {
    // Literal with incremental indexing — name "custom-key", value "custom-header"
    const block = [_]u8{ 0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79, 0x0d, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72 };
    var dec = Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const res = try dec.decode(&block, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(res);
    try std.testing.expectEqual(@as(usize, 1), res.fields.len);
    try std.testing.expectEqualStrings("custom-key", res.fields[0].name);
    try std.testing.expectEqualStrings("custom-header", res.fields[0].value);
}

test "RFC 7541 C.2.2 literal without indexing" {
    const block = [_]u8{ 0x04, 0x0c, 0x2f, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2f, 0x70, 0x61, 0x74, 0x68 };
    var dec = Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const res = try dec.decode(&block, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(res);
    try std.testing.expectEqualStrings(":path", res.fields[0].name);
    try std.testing.expectEqualStrings("/sample/path", res.fields[0].value);
}

test "indexed static :method GET" {
    const block = [_]u8{0x82};
    var dec = Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const res = try dec.decode(&block, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(res);
    try std.testing.expectEqualStrings(":method", res.fields[0].name);
    try std.testing.expectEqualStrings("GET", res.fields[0].value);
}

test "encoder roundtrip never-index cookie" {
    const fields = [_]HeaderField{.{ .name = "cookie", .value = "a=b" }};
    const enc = try Encoder.encode(std.testing.allocator, &fields);
    defer std.testing.allocator.free(enc);
    try std.testing.expect((enc[0] & 0xf0) == 0x10);
    var dec = Decoder.init(std.testing.allocator);
    defer dec.deinit();
    const res = try dec.decode(enc, 100, 32 * 1024, 256, 8 * 1024);
    defer dec.freeResult(res);
    try std.testing.expectEqualStrings("cookie", res.fields[0].name);
}

test "invalid index is compression error" {
    const block = [_]u8{0xff}; // indexed with large value likely OOB after decode
    var dec = Decoder.init(std.testing.allocator);
    defer dec.deinit();
    // 0xff = indexed 127 — may be OOB
    const result = dec.decode(&block, 100, 32 * 1024, 256, 8 * 1024);
    try std.testing.expectError(error.CompressionError, result);
}
