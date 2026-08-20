//! Small nonblocking helpers over bounded `std.Io.Queue`.
//!
//! The actor must poll several queues without committing to any of them: if it
//! blocked on one, it could not notice work waiting on another. These wrappers
//! give a zero-timeout attempt with an option result.
//!
//! The helpers are UNCANCELABLE by design. They are used on teardown paths that
//! must drain a queue in full, and a cancellation there would abandon queued
//! items that own memory or hold accounting. A closed queue is reported as
//! "nothing available" rather than as an error, because to a poller the two are
//! the same fact.
const std = @import("std");
const zio = @import("zio");

/// Nonblocking take from a zio channel. Empty and closed-and-drained report
/// the same fact to a poller: nothing available.
pub fn tryRecv(comptime T: type, ch: *zio.Channel(T)) ?T {
    return ch.tryReceive() catch null;
}

/// Occupancy of a zio channel, in elements. Locks the channel mutex.
/// Trace-only: do not use this to decide a hot-path batch size.
pub fn chanLen(comptime T: type, ch: *zio.Channel(T)) usize {
    ch.impl.mutex.lockUncancelable();
    defer ch.impl.mutex.unlock();
    return ch.impl.count;
}

pub fn tryGet(comptime T: type, queue: *std.Io.Queue(T), io: std.Io) ?T {
    var one: [1]T = undefined;
    const n = queue.getUncancelable(io, &one, 0) catch {
        // A closed and drained queue has no immediately available element.
        return null;
    };
    return if (n == 1) one[0] else null;
}

/// Occupancy of a typed queue, in elements. Locks the same mutex as get/put.
/// Trace-only: do not use this to decide a hot-path batch size.
pub fn queued(comptime T: type, queue: *std.Io.Queue(T), io: std.Io) usize {
    queue.type_erased.mutex.lockUncancelable(io);
    defer queue.type_erased.mutex.unlock(io);
    std.debug.assert(queue.type_erased.len % @sizeOf(T) == 0);
    return queue.type_erased.len / @sizeOf(T);
}

pub fn tryPut(comptime T: type, queue: *std.Io.Queue(T), io: std.Io, value: T) bool {
    return queue.putUncancelable(io, &.{value}, 0) catch {
        // A closed queue rejects ownership without blocking.
        return false;
    } == 1;
}
