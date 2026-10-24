//! A fixed-capacity array whose elements are written only when first needed.
//!
//! A connection sizes its per-stream state for `max_streams_per_connection`
//! streams at connect, but it only ever uses as many elements as it has
//! streams at once. Writing every element at connect makes every page
//! resident, so an idle connection pays for streams it never opens.
//!
//! `slice()` is the prefix that has been written. A caller scans it for a free
//! element first, and calls `touchNext` only when every touched element is
//! busy. The rest of `storage` has never been written, so it costs no RSS.
//!
//! `storage` is public so the owner can free it and do pointer arithmetic on
//! it. Do not read elements through it: past `touched` they hold whatever the
//! allocator left there.
const std = @import("std");

pub fn Touched(comptime T: type) type {
    return struct {
        const Self = @This();

        storage: []T,
        touched: usize = 0,

        pub fn init(storage: []T) Self {
            return .{ .storage = storage };
        }

        pub fn slice(self: Self) []T {
            return self.storage[0..self.touched];
        }

        pub fn capacity(self: Self) usize {
            return self.storage.len;
        }

        /// Write `value` into the first untouched element and return it.
        /// Null when every element is touched.
        pub fn touchNext(self: *Self, value: T) ?*T {
            if (self.touched == self.storage.len) return null;
            const e = &self.storage[self.touched];
            e.* = value;
            self.touched += 1;
            return e;
        }
    };
}

test "touchNext writes elements in order and stops at capacity" {
    var storage: [3]u32 = undefined;
    var t: Touched(u32) = .init(&storage);
    try std.testing.expectEqual(@as(usize, 0), t.slice().len);
    try std.testing.expectEqual(@as(u32, 7), t.touchNext(7).?.*);
    try std.testing.expectEqual(@as(u32, 8), t.touchNext(8).?.*);
    try std.testing.expectEqualSlices(u32, &.{ 7, 8 }, t.slice());
    _ = t.touchNext(9).?;
    try std.testing.expectEqual(@as(?*u32, null), t.touchNext(10));
    try std.testing.expectEqualSlices(u32, &.{ 7, 8, 9 }, t.slice());
    try std.testing.expectEqual(@as(usize, 3), t.capacity());
}
