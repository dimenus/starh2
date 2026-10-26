//! The byte layout of a connection slot's boot buffer, computed without
//! allocating.
//!
//! A slot hands every boot allocation out of one `std.heap.FixedBufferAllocator`
//! over one buffer. `Limits.slotBootBytes` has to name that buffer's size
//! exactly, because the buffer is the connection's whole boot footprint: a
//! number that is too small makes `Connection.init` fail with OutOfMemory, and
//! a number that is too large is memory every slot pays for and never uses.
//!
//! `Plan` replays the allocator's own arithmetic. A bump allocator places each
//! allocation at the next offset that is a multiple of its alignment, so the
//! end of the buffer after a sequence of allocations depends only on the
//! sizes, the alignments and the order. It does NOT depend on the buffer's
//! address as long as the buffer starts on a `slot_align` boundary, which is
//! why the slot allocates it that way and why `add` refuses a type that needs
//! more.
//!
//! Every component that allocates at boot has a `planBoot` function that makes
//! the same calls, in the same order, as its `init`. Nothing checks the two
//! against each other by reading: the exact-fit test in `edge/connection.zig`
//! boots a real connection into a buffer of exactly the planned size and into
//! one byte less, so a reordered or forgotten allocation fails a test.
const std = @import("std");

/// Alignment of the slot buffer's first byte. Every boot allocation must need
/// no more than this.
pub const slot_align: usize = 64;

/// Checked arithmetic for the sizes a plan is built from. A limit comes from
/// the operator, so `a + b` on two of them can overflow, and an overflow in a
/// plan must come out as `InvalidConfig`, never as a panic or a small wrong
/// size.
pub fn add(a: usize, b: usize) error{InvalidConfig}!usize {
    return std.math.add(usize, a, b) catch error.InvalidConfig;
}

pub fn mul(a: usize, b: usize) error{InvalidConfig}!usize {
    return std.math.mul(usize, a, b) catch error.InvalidConfig;
}

/// `std.mem.alignForward` asserts when `x + alignment - 1` overflows, so a
/// plan whose end is within an alignment of the top of `usize` would panic.
/// This adds first, checked, and rounds down after.
pub fn alignForward(x: usize, alignment: usize) error{Overflow}!usize {
    std.debug.assert(std.math.isPowerOfTwo(alignment));
    const bumped = std.math.add(usize, x, alignment - 1) catch return error.Overflow;
    return std.mem.alignBackward(usize, bumped, alignment);
}

/// The largest count `Connection.init` and `std.hash_map` can take: both
/// narrow a count to `u32`.
pub const max_count: usize = std.math.maxInt(u32);

pub const Plan = struct {
    end: usize = 0,

    /// Same as `allocator.alloc(T, n)`.
    pub fn array(self: *Plan, comptime T: type, n: usize) error{InvalidConfig}!void {
        if (@sizeOf(T) == 0) return;
        const bytes = std.math.mul(usize, n, @sizeOf(T)) catch return error.InvalidConfig;
        return self.block(bytes, @alignOf(T));
    }

    /// The one allocation `HashMap.ensureTotalCapacity(n)` makes, for the
    /// std layout described on `hashMapBytes`.
    pub fn hashMap(self: *Plan, comptime K: type, comptime V: type, n: usize) error{InvalidConfig}!void {
        const cap = hashMapCapacityFor(n) catch return error.InvalidConfig;
        const total = hashMapBytesForCapacity(K, V, cap) catch return error.InvalidConfig;
        return self.block(total, hashMapAlign(K, V));
    }

    /// An allocation of `n` bytes that asks for `alignment`, as
    /// `allocator.alignedAlloc(u8, alignment, n)` does. A zero-length
    /// allocation never reaches the allocator, so it moves nothing.
    pub fn block(self: *Plan, n: usize, alignment: usize) error{InvalidConfig}!void {
        std.debug.assert(std.math.isPowerOfTwo(alignment));
        if (alignment > slot_align) return error.InvalidConfig;
        if (n == 0) return;
        const start = alignForward(self.end, alignment) catch return error.InvalidConfig;
        self.end = std.math.add(usize, start, n) catch return error.InvalidConfig;
    }
};

/// Zig `default_max_load_percentage` — must match std.hash_map.
pub const HASH_MAP_MAX_LOAD_PERCENTAGE: u32 = 80;
/// Zig HashMapUnmanaged.minimal_capacity.
pub const HASH_MAP_MINIMAL_CAPACITY: u32 = 8;

/// Exact `capacityForSize` from std.hash_map.HashMapUnmanaged, plus the
/// `grow` floor. Zero entries allocate nothing.
pub fn hashMapCapacityFor(n: usize) error{Overflow}!usize {
    if (n == 0) return 0;
    // std takes the count as a `u32`; checking first also keeps the `* 100`
    // below from overflowing.
    if (n > max_count) return error.Overflow;
    const load: u64 = HASH_MAP_MAX_LOAD_PERCENTAGE;
    const need64 = (@as(u64, n) * 100) / load + 1;
    if (need64 > std.math.maxInt(u32)) return error.Overflow;
    var new_cap: u32 = @intCast(need64);
    new_cap = std.math.ceilPowerOfTwo(u32, new_cap) catch return error.Overflow;
    new_cap = @max(new_cap, HASH_MAP_MINIMAL_CAPACITY);
    return new_cap;
}

/// The alignment `HashMapUnmanaged.allocate` asks for.
pub fn hashMapAlign(comptime K: type, comptime V: type) usize {
    const Header = struct {
        values: [*]V,
        keys: [*]K,
        capacity: u32,
    };
    const key_align = if (@sizeOf(K) == 0) 1 else @alignOf(K);
    const val_align = if (@sizeOf(V) == 0) 1 else @alignOf(V);
    return @max(@alignOf(Header), @max(key_align, val_align));
}

/// Exact single-allocation size from std.hash_map `allocate` for capacity
/// `cap`: a header, one metadata byte per slot, the keys, the values, with
/// alignment padding between the regions. A naive `n * (key + value)` misses
/// every one of those. This copies standard-library internals, so it can drift
/// when Zig changes `std.hash_map`; the planned-map test below, and the exact-fit test in
/// `edge/connection.zig`, fail when it does. Fix the copy; do not widen
/// the bound to hide the difference.
pub fn hashMapBytesForCapacity(comptime K: type, comptime V: type, cap: usize) error{Overflow}!usize {
    if (cap == 0) return 0;
    const Header = struct {
        values: [*]V,
        keys: [*]K,
        capacity: u32,
    };
    const Metadata = packed struct {
        fingerprint: u7 = 0,
        used: u1 = 0,
    };
    comptime {
        if (@sizeOf(Metadata) != 1) @compileError("HashMap Metadata must be 1 byte");
    }
    const key_align = if (@sizeOf(K) == 0) 1 else @alignOf(K);
    const val_align = if (@sizeOf(V) == 0) 1 else @alignOf(V);
    const max_align = hashMapAlign(K, V);

    const meta_size = std.math.add(usize, @sizeOf(Header), std.math.mul(usize, cap, @sizeOf(Metadata)) catch return error.Overflow) catch return error.Overflow;
    const keys_start = try alignForward(meta_size, key_align);
    const keys_end = std.math.add(usize, keys_start, std.math.mul(usize, cap, @sizeOf(K)) catch return error.Overflow) catch return error.Overflow;
    const vals_start = try alignForward(keys_end, val_align);
    const vals_end = std.math.add(usize, vals_start, std.math.mul(usize, cap, @sizeOf(V)) catch return error.Overflow) catch return error.Overflow;
    return alignForward(vals_end, max_align);
}

test "hash map capacity matches std formula" {
    try std.testing.expectEqual(@as(usize, 0), try hashMapCapacityFor(0));
    try std.testing.expectEqual(@as(usize, 8), try hashMapCapacityFor(1));
    try std.testing.expectEqual(@as(usize, 8), try hashMapCapacityFor(5));
    // size=7 -> (700)/80+1 = 9 -> ceilPow2 -> 16
    try std.testing.expectEqual(@as(usize, 16), try hashMapCapacityFor(7));
}

test "a planned hash map matches what std allocates" {
    // The real allocation, measured, for sizes either side of a power-of-two
    // capacity step and for a value type with a wide alignment.
    const Wide = struct { a: u64, b: [3]u8 };
    inline for (.{ u64, Wide }) |V| {
        for ([_]usize{ 1, 7, 12, 13, 100, 256 }) |n| {
            var backing: [64 * 1024]u8 align(slot_align) = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&backing);
            var map = std.AutoHashMap(u31, V).init(fba.allocator());
            try map.ensureTotalCapacity(@intCast(n));
            var p: Plan = .{};
            try p.hashMap(u31, V, n);
            try std.testing.expectEqual(fba.end_index, p.end);
        }
    }
}

test "an allocation starts at the next multiple of its alignment" {
    var p: Plan = .{};
    try p.array(u8, 3);
    try p.array(u64, 2);
    // 3 bytes, pad to 8, then 16 bytes.
    try std.testing.expectEqual(@as(usize, 24), p.end);
}

test "a zero-length allocation moves nothing" {
    var p: Plan = .{};
    try p.array(u8, 3);
    try p.array(u64, 0);
    try std.testing.expectEqual(@as(usize, 3), p.end);
}

test "the plan matches a FixedBufferAllocator over an aligned buffer" {
    var backing: [4096]u8 align(slot_align) = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&backing);
    const a = fba.allocator();
    var p: Plan = .{};

    _ = try a.alloc(u8, 5);
    try p.array(u8, 5);
    _ = try a.alloc(u32, 7);
    try p.array(u32, 7);
    _ = try a.alloc(u8, 1);
    try p.array(u8, 1);
    _ = try a.alloc(u64, 3);
    try p.array(u64, 3);
    _ = try a.alignedAlloc(u8, .@"16", 33);
    try p.block(33, 16);

    try std.testing.expectEqual(fba.end_index, p.end);
}

test "an alignment above the slot's is refused" {
    var p: Plan = .{};
    try std.testing.expectError(error.InvalidConfig, p.block(8, slot_align * 2));
}

test "a plan end within an alignment of the top of usize is InvalidConfig, not an assert" {
    const top = std.math.maxInt(usize);
    var p: Plan = .{ .end = top - 2 };
    try std.testing.expectError(error.InvalidConfig, p.block(8, 8));
    p = .{ .end = top };
    try std.testing.expectError(error.InvalidConfig, p.block(1, 2));
    // The same for the hash-map layout. One byte per key and per value makes
    // the layout 3 bytes per slot, so scanning the capacities around
    // `top / 3` puts the end of the values at every distance from the top.
    var cap: usize = top / 3 - 40;
    while (cap < top / 3 + 40) : (cap += 1) {
        if (hashMapBytesForCapacity(u8, u8, cap)) |n| {
            try std.testing.expect(n >= cap);
        } else |err| {
            try std.testing.expectEqual(error.Overflow, err);
        }
    }
}
