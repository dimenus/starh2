//! Brotli encoder/decoder bindings with a per-context byte budget.
//!
//! Every brotli allocation goes through custom alloc/free hooks into a Zig
//! allocator that enforces `compression_context_bytes`. The server-wide `Pool`
//! caps concurrent live encoder contexts at `compression_contexts_per_server`.
//! Init failure before headers commit is a counted identity fallback; budget
//! exhaustion after commit is the caller's problem (abort the stream).
const std = @import("std");
const c = @cImport({
    @cInclude("brotli/encode.h");
    @cInclude("brotli/decode.h");
});

pub const EncodeError = error{
    OutOfMemory,
    EncoderFailed,
};

pub const DecodeError = error{
    OutOfMemory,
    DecoderFailed,
    NeedMoreInput,
};

/// Tracks live bytes under one encoder (or decoder) context.
///
/// Brotli's C core assumes malloc alignment. Zig's `alloc(u8, n)` is only
/// 1-byte aligned, so every budget block is 16-byte aligned with a 16-byte
/// size prefix (payload starts at a 16-byte boundary too).
pub const Budget = struct {
    parent: std.mem.Allocator,
    limit: usize,
    used: usize = 0,

    const prefix_len: usize = 16;

    pub fn alloc(self: *Budget, size: usize) ?[*]u8 {
        if (size == 0) return null;
        const total = std.math.add(usize, size, prefix_len) catch return null;
        if (self.used > self.limit or self.limit - self.used < total) return null;
        const block = self.parent.alignedAlloc(u8, .fromByteUnits(16), total) catch return null;
        @as(*usize, @ptrCast(@alignCast(block.ptr))).* = size;
        self.used += total;
        return block[prefix_len..].ptr;
    }

    pub fn free(self: *Budget, ptr: ?[*]u8) void {
        const p = ptr orelse return;
        const base: [*]align(16) u8 = @ptrCast(@alignCast(p - prefix_len));
        const size = @as(*usize, @ptrCast(base)).*;
        const total = size + prefix_len;
        std.debug.assert(self.used >= total);
        self.used -= total;
        self.parent.free(base[0..total]);
    }
};

fn cAlloc(opaque_ptr: ?*anyopaque, size: usize) callconv(.c) ?*anyopaque {
    const budget: *Budget = @ptrCast(@alignCast(opaque_ptr orelse return null));
    // brotli may request size 0; malloc(0) is implementation-defined but must not
    // look like OOM. Hand back a unique non-null slab of 1 payload byte.
    const want = if (size == 0) @as(usize, 1) else size;
    const p = budget.alloc(want) orelse return null;
    return p;
}

fn cFree(opaque_ptr: ?*anyopaque, address: ?*anyopaque) callconv(.c) void {
    const budget: *Budget = @ptrCast(@alignCast(opaque_ptr orelse return));
    budget.free(@ptrCast(address));
}

pub const Operation = enum {
    process,
    flush,
    finish,

    fn toC(self: Operation) c.BrotliEncoderOperation {
        return switch (self) {
            .process => c.BROTLI_OPERATION_PROCESS,
            .flush => c.BROTLI_OPERATION_FLUSH,
            .finish => c.BROTLI_OPERATION_FINISH,
        };
    }
};

pub const Encoder = struct {
    state: *c.BrotliEncoderState,
    budget: *Budget,
    pool: ?*Pool,
    finished: bool = false,

    pub fn create(parent: std.mem.Allocator, limit: usize, quality: u8, window_bits: u8) EncodeError!*Encoder {
        // Refuse a budget that cannot cover peak for this quality/window.
        // libbrotli may hang if a custom alloc returns NULL mid-stream; failing
        // here keeps OOM on the identity-fallback / init path instead.
        if (window_bits < 10 or window_bits > 24 or quality > 11) return error.EncoderFailed;
        const window_bytes: usize = @as(usize, 1) << @intCast(window_bits);
        const min_limit = window_bytes * 4 + @as(usize, quality) * 128 * 1024 + 1024 * 1024;
        if (limit < min_limit) return error.OutOfMemory;

        const budget = try parent.create(Budget);
        errdefer parent.destroy(budget);
        budget.* = .{ .parent = parent, .limit = limit };

        const self = try parent.create(Encoder);
        errdefer parent.destroy(self);

        const state = c.BrotliEncoderCreateInstance(cAlloc, cFree, budget) orelse {
            return error.OutOfMemory;
        };
        errdefer c.BrotliEncoderDestroyInstance(state);

        if (c.BrotliEncoderSetParameter(state, c.BROTLI_PARAM_QUALITY, quality) == 0) {
            return error.EncoderFailed;
        }
        if (c.BrotliEncoderSetParameter(state, c.BROTLI_PARAM_LGWIN, window_bits) == 0) {
            return error.EncoderFailed;
        }
        // Prefer text mode; harmless for non-text and helps HTML/SSE.
        _ = c.BrotliEncoderSetParameter(state, c.BROTLI_PARAM_MODE, c.BROTLI_MODE_TEXT);

        self.* = .{
            .state = state,
            .budget = budget,
            .pool = null,
        };
        return self;
    }

    pub fn destroy(self: *Encoder) void {
        const parent = self.budget.parent;
        const budget = self.budget;
        const pool = self.pool;
        c.BrotliEncoderDestroyInstance(self.state);
        // After DestroyInstance, budget.used should be 0.
        std.debug.assert(budget.used == 0);
        parent.destroy(budget);
        parent.destroy(self);
        if (pool) |p| p.releaseSlot();
    }

    /// Compress `input` under `op`, appending output bytes to `out`.
    pub fn compress(self: *Encoder, input: []const u8, op: Operation, out: *std.ArrayList(u8), gpa: std.mem.Allocator) EncodeError!void {
        if (self.finished) return error.EncoderFailed;
        var next_in: [*c]const u8 = if (input.len == 0) null else input.ptr;
        var available_in: usize = input.len;
        var out_buf: [64 * 1024]u8 = undefined;
        // Guard against a stuck encoder spinning forever (defensive; brotli
        // should always make progress or fail when output space is available).
        var spins: usize = 0;

        while (true) {
            spins += 1;
            if (spins > 1_000_000) return error.EncoderFailed;
            var next_out: [*c]u8 = &out_buf;
            var available_out: usize = out_buf.len;
            const ok = c.BrotliEncoderCompressStream(
                self.state,
                op.toC(),
                &available_in,
                &next_in,
                &available_out,
                &next_out,
                null,
            );
            if (ok == 0) return error.EncoderFailed;
            const produced = out_buf.len - available_out;
            if (produced > 0) {
                try out.appendSlice(gpa, out_buf[0..produced]);
            }
            const has_more = c.BrotliEncoderHasMoreOutput(self.state) != 0;
            if (available_in == 0 and !has_more) {
                if (op == .finish) {
                    if (c.BrotliEncoderIsFinished(self.state) != 0) {
                        self.finished = true;
                        return;
                    }
                    // Keep pumping FINISH until finished.
                    continue;
                }
                // PROCESS and FLUSH are done once input is consumed and output drained.
                return;
            }
        }
    }

    pub fn compressAll(self: *Encoder, input: []const u8, gpa: std.mem.Allocator) EncodeError![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(gpa);
        try self.compress(input, .finish, &out, gpa);
        return try out.toOwnedSlice(gpa);
    }
};

pub const Decoder = struct {
    state: *c.BrotliDecoderState,
    budget: *Budget,

    pub fn create(parent: std.mem.Allocator, limit: usize) DecodeError!*Decoder {
        const budget = try parent.create(Budget);
        errdefer parent.destroy(budget);
        budget.* = .{ .parent = parent, .limit = limit };

        const self = try parent.create(Decoder);
        errdefer parent.destroy(self);

        const state = c.BrotliDecoderCreateInstance(cAlloc, cFree, budget) orelse {
            return error.OutOfMemory;
        };
        self.* = .{ .state = state, .budget = budget };
        return self;
    }

    pub fn destroy(self: *Decoder) void {
        const parent = self.budget.parent;
        const budget = self.budget;
        c.BrotliDecoderDestroyInstance(self.state);
        std.debug.assert(budget.used == 0);
        parent.destroy(budget);
        parent.destroy(self);
    }

    /// Feed compressed bytes; append any newly decoded plaintext to `out`.
    /// Returns true when the stream is finished.
    pub fn decompress(self: *Decoder, input: []const u8, out: *std.ArrayList(u8), gpa: std.mem.Allocator) DecodeError!bool {
        var next_in: [*c]const u8 = if (input.len == 0) null else input.ptr;
        var available_in: usize = input.len;
        var out_buf: [64 * 1024]u8 = undefined;

        while (true) {
            var next_out: [*c]u8 = &out_buf;
            var available_out: usize = out_buf.len;
            const rc = c.BrotliDecoderDecompressStream(
                self.state,
                &available_in,
                &next_in,
                &available_out,
                &next_out,
                null,
            );
            const produced = out_buf.len - available_out;
            if (produced > 0) {
                try out.appendSlice(gpa, out_buf[0..produced]);
            }
            switch (rc) {
                c.BROTLI_DECODER_RESULT_SUCCESS => return true,
                c.BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT => return false,
                c.BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT => continue,
                else => return error.DecoderFailed,
            }
        }
    }

    pub fn decompressAll(gpa: std.mem.Allocator, limit: usize, input: []const u8) DecodeError![]u8 {
        const dec = try Decoder.create(gpa, limit);
        defer dec.destroy();
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(gpa);
        const done = try dec.decompress(input, &out, gpa);
        if (!done) return error.NeedMoreInput;
        return try out.toOwnedSlice(gpa);
    }
};

/// Server-wide concurrent encoder context table.
pub const Pool = struct {
    gpa: std.mem.Allocator,
    max_contexts: usize,
    context_bytes: usize,
    quality: u8,
    window_bits: u8,
    live: std.atomic.Value(usize) = .init(0),
    /// I6: identity fallbacks because the pool was full or encoder init failed
    /// before headers committed. Observable without reading logs.
    identity_fallbacks: std.atomic.Value(usize) = .init(0),

    pub fn init(
        gpa: std.mem.Allocator,
        max_contexts: usize,
        context_bytes: usize,
        quality: u8,
        window_bits: u8,
    ) Pool {
        return .{
            .gpa = gpa,
            .max_contexts = max_contexts,
            .context_bytes = context_bytes,
            .quality = quality,
            .window_bits = window_bits,
        };
    }

    pub fn noteIdentityFallback(self: *Pool) void {
        _ = self.identity_fallbacks.fetchAdd(1, .acq_rel);
    }

    /// Try to open a new encoder. Null means serve identity (and count it).
    pub fn tryAcquire(self: *Pool) ?*Encoder {
        if (self.max_contexts == 0) {
            self.noteIdentityFallback();
            return null;
        }
        while (true) {
            const cur = self.live.load(.acquire);
            if (cur >= self.max_contexts) {
                self.noteIdentityFallback();
                return null;
            }
            if (self.live.cmpxchgWeak(cur, cur + 1, .acq_rel, .acquire) == null) break;
        }
        const enc = Encoder.create(self.gpa, self.context_bytes, self.quality, self.window_bits) catch {
            _ = self.live.fetchSub(1, .acq_rel);
            self.noteIdentityFallback();
            return null;
        };
        enc.pool = self;
        return enc;
    }

    fn releaseSlot(self: *Pool) void {
        const prev = self.live.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
    }

    pub fn liveCount(self: *const Pool) usize {
        return self.live.load(.acquire);
    }
};

const test_budget: usize = 4 * 1024 * 1024;

test "encoder round-trip identity" {
    const gpa = std.testing.allocator;
    const plain = "hello brotli " ** 32;
    const enc = try Encoder.create(gpa, test_budget, 5, 19);
    defer enc.destroy();
    const compressed = try enc.compressAll(plain, gpa);
    defer gpa.free(compressed);
    try std.testing.expect(compressed.len > 0);
    try std.testing.expect(compressed.len < plain.len);
    const decoded = try Decoder.decompressAll(gpa, test_budget, compressed);
    defer gpa.free(decoded);
    try std.testing.expectEqualStrings(plain, decoded);
}

test "encoder flush yields streaming-decodable prefix" {
    const gpa = std.testing.allocator;
    const enc = try Encoder.create(gpa, test_budget, 5, 19);
    defer enc.destroy();
    const dec = try Decoder.create(gpa, test_budget);
    defer dec.destroy();

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    var plain_out: std.ArrayList(u8) = .empty;
    defer plain_out.deinit(gpa);

    const e1 = "data: event-one\n\n";
    try enc.compress(e1, .flush, &wire, gpa);
    const d1 = try dec.decompress(wire.items, &plain_out, gpa);
    try std.testing.expect(!d1);
    try std.testing.expectEqualStrings(e1, plain_out.items);

    const e2 = "data: event-two-repeats-html-ish\n\n";
    const before = wire.items.len;
    try enc.compress(e2, .flush, &wire, gpa);
    const d2 = try dec.decompress(wire.items[before..], &plain_out, gpa);
    try std.testing.expect(!d2);
    try std.testing.expectEqualStrings(e1 ++ e2, plain_out.items);

    const before_fin = wire.items.len;
    try enc.compress(&.{}, .finish, &wire, gpa);
    const d3 = try dec.decompress(wire.items[before_fin..], &plain_out, gpa);
    try std.testing.expect(d3);
    try std.testing.expectEqualStrings(e1 ++ e2, plain_out.items);
}

test "compressible body shrinks below 25 percent" {
    const gpa = std.testing.allocator;
    // ≥ 64 KiB repetitive HTML-like text.
    var plain: std.ArrayList(u8) = .empty;
    defer plain.deinit(gpa);
    const chunk = "<div class=\"row\"><span>item</span><p>lorem ipsum dolor sit amet</p></div>\n";
    while (plain.items.len < 64 * 1024) {
        try plain.appendSlice(gpa, chunk);
    }
    const enc = try Encoder.create(gpa, test_budget, 5, 19);
    defer enc.destroy();
    const compressed = try enc.compressAll(plain.items, gpa);
    defer gpa.free(compressed);
    try std.testing.expect(compressed.len * 4 <= plain.items.len);
    const decoded = try Decoder.decompressAll(gpa, test_budget, compressed);
    defer gpa.free(decoded);
    try std.testing.expectEqualSlices(u8, plain.items, decoded);
}

test "pool caps concurrent contexts and counts identity fallback" {
    const gpa = std.testing.allocator;
    var pool = Pool.init(gpa, 1, test_budget, 5, 19);
    const a = pool.tryAcquire() orelse return error.TestUnexpectedResult;
    defer a.destroy();
    try std.testing.expect(pool.tryAcquire() == null);
    try std.testing.expectEqual(@as(usize, 1), pool.identity_fallbacks.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), pool.liveCount());
}

test "budget rejects undersized context before create" {
    const gpa = std.testing.allocator;
    // Below min budget for q5/w19: must fail at create (not hang mid-stream).
    try std.testing.expectError(error.OutOfMemory, Encoder.create(gpa, 256, 5, 19));
    try std.testing.expectError(error.OutOfMemory, Encoder.create(gpa, 2 * 1024 * 1024, 5, 19));
}
