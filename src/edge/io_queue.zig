//! Small nonblocking helpers over bounded `zio.Channel`s.
//!
//! The actor must poll several channels without committing to any of them: if
//! it blocked on one, it could not notice work waiting on another. `tryRecv`
//! gives a zero-timeout attempt with an option result.
//!
//! The helpers are UNCANCELABLE by design (the channel's `tryReceive` and its
//! mutex take no cancel). They are used on teardown paths that must drain a
//! channel in full, and a cancellation there would abandon queued items that
//! own memory or hold accounting. A closed channel is reported as "nothing
//! available" rather than as an error, because to a poller the two are the
//! same fact.
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
