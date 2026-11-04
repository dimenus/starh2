//! The allocators the TSan lane's test binaries use in place of
//! `std.testing.allocator` and `std.heap.page_allocator` (`./zb build tsan`,
//! the binaries listed in `build.zig`'s `tsan_bins`).
//!
//! Without `sanitize_thread` each function returns the std allocator itself,
//! so the normal suite runs exactly as before.
//!
//! Under `sanitize_thread` each returns a wrapper whose `remap` and `resize`
//! always refuse, so a caller that grows or shrinks falls back to alloc, copy
//! and free. The reason: `std.heap.PageAllocator.remap` moves a large
//! allocation with `mremap(MREMAP_MAYMOVE)`, and ThreadSanitizer intercepts
//! `mmap` and `munmap` but not `mremap`. TSan then keeps the old accesses for
//! the range a move left, and when the kernel hands that range to another
//! thread's next `mremap`, TSan reports a data race between two threads that
//! never shared the memory (t-3837: `slot_reuse`'s client threads, in
//! `SafeAllocator.remap` and `AllocFooter.populate`). The kernel orders the
//! two `mremap` calls, but TSan cannot see that edge. A suppression would have
//! to match `array_list` and `SafeAllocator` frames, which would also hide
//! real races. With the wrappers, large allocations come and go only through
//! `mmap` and `munmap`, which TSan sees.
//!
//! Alloc and free go to the wrapped allocator, so the testing allocator still
//! reports leaks and double frees.
const std = @import("std");
const builtin = @import("builtin");

pub fn allocator() std.mem.Allocator {
    if (!builtin.sanitize_thread) return std.testing.allocator;
    return NoRemap(testingAllocator).allocator();
}

pub fn pageAllocator() std.mem.Allocator {
    if (!builtin.sanitize_thread) return std.heap.page_allocator;
    return NoRemap(pageAllocatorChild).allocator();
}

fn testingAllocator() std.mem.Allocator {
    return std.testing.allocator;
}

fn pageAllocatorChild() std.mem.Allocator {
    return std.heap.page_allocator;
}

/// An allocator over `child()` that never grows or shrinks in place. The
/// child is read on every call: the test runner re-creates the testing
/// allocator's state before each test.
fn NoRemap(comptime child: fn () std.mem.Allocator) type {
    return struct {
        var anchor: u8 = 0;

        const vtable: std.mem.Allocator.VTable = .{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        };

        fn allocator() std.mem.Allocator {
            return .{ .ptr = &anchor, .vtable = &vtable };
        }

        fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            return child().rawAlloc(len, alignment, ra);
        }

        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }

        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }

        fn free(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
            child().rawFree(memory, alignment, ra);
        }
    };
}
