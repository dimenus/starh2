//! Concrete sizes for the server-wide terms of `Limits.resourceUpperBound` —
//! no magic estimates.
//!
//! What a connection allocates at boot is no longer here: it is one term,
//! `Limits.slotBootBytes`, computed by replaying the allocations themselves
//! (`boot_plan`). This file keeps the sizes of the server-wide structures that
//! are not part of a slot.
const std = @import("std");
const router_mod = @import("../http/router.zig");

pub const ROUTE_SIZE: usize = @sizeOf(router_mod.Route);
pub const ENDPOINT_CONFIG_SIZE: usize = @sizeOf(union(enum) {
    tls: std.Io.net.IpAddress,
    h2c_prior_knowledge: std.Io.net.IpAddress,
    h1c: std.Io.net.IpAddress,
});
pub const LISTENER_SIZE: usize = @sizeOf(std.Io.net.Server);
pub const ENDPOINT_ADDRESS_SIZE: usize = @sizeOf(std.Io.net.IpAddress);

pub fn pendingSlabBytes(max_streams: usize, bytes_per_stream: usize) error{Overflow}!usize {
    const storage = try std.math.mul(usize, max_streams, bytes_per_stream);
    const rounded = try std.math.add(usize, max_streams, 63);
    const words = rounded / 64;
    const bitmap = try std.math.mul(usize, words, @sizeOf(std.atomic.Value(u64)));
    return std.math.add(usize, storage, bitmap);
}

test "concrete sizes nonzero" {
    try std.testing.expect(ROUTE_SIZE > 0);
    try std.testing.expect(LISTENER_SIZE > 0);
}
