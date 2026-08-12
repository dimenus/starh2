//! Small nonblocking helpers over bounded `std.Io.Queue`.
const std = @import("std");

pub fn tryGet(comptime T: type, queue: *std.Io.Queue(T), io: std.Io) ?T {
    var one: [1]T = undefined;
    const n = queue.getUncancelable(io, &one, 0) catch {
        // A closed and drained queue has no immediately available element.
        return null;
    };
    return if (n == 1) one[0] else null;
}

pub fn tryPut(comptime T: type, queue: *std.Io.Queue(T), io: std.Io, value: T) bool {
    return queue.putUncancelable(io, &.{value}, 0) catch {
        // A closed queue rejects ownership without blocking.
        return false;
    } == 1;
}
