//! Concrete allocation shapes for Limits.resourceUpperBound — no magic estimates.
//! HashMap bytes use Zig 0.16 `std.hash_map` allocate layout (Header + Metadata + keys + values).
//!
//! The hash-map functions reproduce the standard library's own layout instead
//! of approximating it, because a memory bound that undercounts is worse than
//! none: it still returns a confident number. A HashMap does not allocate
//! `n * (key + value)`. It rounds the capacity up to a power of two above the
//! load factor, then packs a header, a metadata byte per slot, the keys, and
//! the values into ONE allocation with alignment padding between the regions.
//! Every one of those steps adds bytes that a naive product misses.
//!
//! This is a copy of standard-library internals, so it can drift when Zig
//! changes `std.hash_map`. Two guards catch that: the constants below are
//! named and comparable against the source, and the unit tests pin the
//! capacity formula at known inputs. If a Zig upgrade changes the layout, fix
//! it here — do not widen the bound to hide the difference.
const std = @import("std");
const ticket_table = @import("../edge/ticket_table.zig");
const wire_pump = @import("../edge/wire_pump.zig");
const stream_mod = @import("stream.zig");
const frame = @import("frame.zig");
const hpack = @import("hpack.zig");
const session_mod = @import("session.zig");
const fair_scheduler = @import("../edge/fair_scheduler.zig");
const router_mod = @import("../http/router.zig");

/// Zig `default_max_load_percentage` — must match std.hash_map.
pub const HASH_MAP_MAX_LOAD_PERCENTAGE: u32 = 80;
/// Zig HashMapUnmanaged.minimal_capacity.
pub const HASH_MAP_MINIMAL_CAPACITY: u32 = 8;

/// Exact `capacityForSize` from std.hash_map.HashMapUnmanaged.
pub fn hashMapCapacityFor(n: usize) error{Overflow}!usize {
    if (n == 0) return 0;
    const load: u64 = HASH_MAP_MAX_LOAD_PERCENTAGE;
    const need64 = (@as(u64, n) * 100) / load + 1;
    if (need64 > std.math.maxInt(u32)) return error.Overflow;
    var new_cap: u32 = @intCast(need64);
    new_cap = std.math.ceilPowerOfTwo(u32, new_cap) catch return error.Overflow;
    new_cap = @max(new_cap, HASH_MAP_MINIMAL_CAPACITY);
    return new_cap;
}

/// Exact single-allocation size from std.hash_map allocate() for capacity `cap`.
/// Never undercounts: uses real Header/Metadata shapes + alignment forward.
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
    const header_align = @alignOf(Header);
    const key_align = if (@sizeOf(K) == 0) 1 else @alignOf(K);
    const val_align = if (@sizeOf(V) == 0) 1 else @alignOf(V);
    const max_align = @max(header_align, @max(key_align, val_align));

    const meta_size = std.math.add(usize, @sizeOf(Header), std.math.mul(usize, cap, @sizeOf(Metadata)) catch return error.Overflow) catch return error.Overflow;
    const keys_start = std.mem.alignForward(usize, meta_size, key_align);
    const keys_end = std.math.add(usize, keys_start, std.math.mul(usize, cap, @sizeOf(K)) catch return error.Overflow) catch return error.Overflow;
    const vals_start = std.mem.alignForward(usize, keys_end, val_align);
    const vals_end = std.math.add(usize, vals_start, std.math.mul(usize, cap, @sizeOf(V)) catch return error.Overflow) catch return error.Overflow;
    return std.mem.alignForward(usize, vals_end, max_align);
}

/// Upper bound on unmanaged HashMap bytes for `n` live entries.
pub fn hashMapBytes(comptime K: type, comptime V: type, n: usize) error{Overflow}!usize {
    const cap = try hashMapCapacityFor(n);
    return hashMapBytesForCapacity(K, V, cap);
}

pub const JOIN_HANDLE_SIZE: usize = @sizeOf(std.Io.Future(void));
pub const WIRE_CHUNK_DESC_SIZE: usize = @sizeOf(wire_pump.WireChunk);
pub const WRITE_COMPLETION_SIZE: usize = @sizeOf(wire_pump.WriteCompletion);
pub const TICKET_WAIT_SIZE: usize = @sizeOf(ticket_table.TicketWait);
pub const STREAM_SIZE: usize = @sizeOf(stream_mod.Stream);
pub const U31_SIZE: usize = @sizeOf(u31);
pub const INTENT_SIZE: usize = @sizeOf(session_mod.Intent);
pub const DATA_PENDING_SIZE: usize = @sizeOf(fair_scheduler.DataPending);
pub const CONTROL_ENTRY_SIZE: usize = @sizeOf(fair_scheduler.ControlEntry);
pub const FramedDataEntryShape = fair_scheduler.FramedDataEntry;
pub const EVENT_SIZE: usize = @sizeOf(std.Io.Event);
pub const ROUTE_SIZE: usize = @sizeOf(router_mod.Route);
pub const ENDPOINT_CONFIG_SIZE: usize = @sizeOf(union(enum) {
    tls: std.Io.net.IpAddress,
    h2c_prior_knowledge: std.Io.net.IpAddress,
    h1c: std.Io.net.IpAddress,
});
pub const LISTENER_SIZE: usize = @sizeOf(std.Io.net.Server);
pub const ENDPOINT_ADDRESS_SIZE: usize = @sizeOf(std.Io.net.IpAddress);

/// Per-stream map entry bound (key u31 + Stream value) under HashMap policy.
pub fn streamMapBytes(max_streams: usize) error{Overflow}!usize {
    return hashMapBytes(u31, stream_mod.Stream, max_streams);
}

/// Pending outbound DATA uses a fixed slot array and not a HashMap. The
/// scheduler must add a pending entry on a handler write, and a HashMap can
/// resize on insert. A resize after boot would allocate on the write path,
/// which breaks the promise that `resourceUpperBound` is an upper bound.
pub fn pendingWriteMapBytes(max_streams: usize) error{Overflow}!usize {
    // Fixed DataPending slots (no HashMap) — slabs counted separately in Limits.
    return std.math.mul(usize, max_streams, @sizeOf(fair_scheduler.DataPending));
}

pub fn pendingSlabBytes(max_streams: usize, bytes_per_stream: usize) error{Overflow}!usize {
    const storage = try std.math.mul(usize, max_streams, bytes_per_stream);
    const rounded = try std.math.add(usize, max_streams, 63);
    const words = rounded / 64;
    const bitmap = try std.math.mul(usize, words, @sizeOf(std.atomic.Value(u64)));
    return std.math.add(usize, storage, bitmap);
}

test "hash map capacity matches std formula" {
    try std.testing.expectEqual(@as(usize, 8), try hashMapCapacityFor(1));
    try std.testing.expectEqual(@as(usize, 8), try hashMapCapacityFor(5));
    // size=7 → (700)/80+1 = 9 → ceilPow2 → 16
    try std.testing.expectEqual(@as(usize, 16), try hashMapCapacityFor(7));
}

test "hash map bytes never undercount alignment" {
    const bare = @sizeOf(u31) + @sizeOf(stream_mod.Stream);
    const bound = try hashMapBytes(u31, stream_mod.Stream, 16);
    try std.testing.expect(bound > bare * 16);
    try std.testing.expect(bound >= try hashMapBytesForCapacity(u31, stream_mod.Stream, 8));
}

test "concrete sizes nonzero" {
    try std.testing.expect(JOIN_HANDLE_SIZE > 0);
    try std.testing.expect(WIRE_CHUNK_DESC_SIZE >= @sizeOf(usize));
    try std.testing.expect(WRITE_COMPLETION_SIZE <= 64);
    try std.testing.expect(STREAM_SIZE > 0);
    try std.testing.expect(INTENT_SIZE > 0);
    try std.testing.expect(DATA_PENDING_SIZE > 0);
}
