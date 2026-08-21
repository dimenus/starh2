//! Physical core count for zio executor `.auto`.
//!
//! zio's `ExecutorCount.auto` uses `std.Thread.getCpuCount`, which is the
//! affinity mask (logical/SMT siblings). SMT halves share execution units;
//! HTTP/2 connection actors scheduled onto both halves of one core fight.
//! The 50-conn nachos run made that visible: 24 logical executors burned
//! ~3× the CPU of 2 and lost rps.
//!
//! The detector follows challenge01 `src/calibrate.zig` `detectPhysicalCores`
//! (unique `/sys/.../topology/core_id`), with `physical_package_id` paired
//! so two sockets do not collapse to one package's core ids. Darwin uses
//! `hw.physicalcpu`. The result is clamped to the process affinity count.
const std = @import("std");
const builtin = @import("builtin");

/// Physical cores visible to this process, at least 1.
pub fn count() usize {
    const logical = std.Thread.getCpuCount() catch 1;
    const physical = switch (builtin.os.tag) {
        .linux => linux_count.run() catch logical,
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => darwin_count.run() catch logical,
        else => logical,
    };
    if (physical == 0) return logical;
    return @min(physical, logical);
}

/// zio `ExecutorCount.exact` argument. Matches `Executor.max_executors` (u6+1 = 64).
pub fn executorCount() u8 {
    const n = count();
    if (n == 0) return 1;
    return @intCast(@min(n, 64));
}

const darwin_count = if (builtin.os.tag.isDarwin()) struct {
    fn run() error{Unexpected}!usize {
        var n: c_int = 0;
        var len: usize = @sizeOf(c_int);
        switch (std.posix.errno(std.posix.system.sysctlbyname("hw.physicalcpu", &n, &len, null, 0))) {
            .SUCCESS => {
                if (n <= 0) return error.Unexpected;
                return @intCast(n);
            },
            else => return error.Unexpected,
        }
    }
} else struct {
    fn run() error{Unexpected}!usize {
        return error.Unexpected;
    }
};

const linux_count = if (builtin.os.tag == .linux) struct {
    fn run() error{Unexpected}!usize {
        const set = std.posix.sched_getaffinity(0) catch return error.Unexpected;
        var seen: [128]u32 = undefined;
        var n_seen: usize = 0;
        var cpu: usize = 0;
        while (cpu < 1024) : (cpu += 1) {
            if (!affinityHas(set, cpu)) continue;
            const key = coreKey(cpu) orelse continue;
            var found = false;
            for (seen[0..n_seen]) |k| {
                if (k == key) {
                    found = true;
                    break;
                }
            }
            if (found) continue;
            if (n_seen == seen.len) break;
            seen[n_seen] = key;
            n_seen += 1;
        }
        if (n_seen == 0) return error.Unexpected;
        return n_seen;
    }

    fn affinityHas(set: std.posix.cpu_set_t, cpu: usize) bool {
        const word_bits = @bitSizeOf(usize);
        const i = cpu / word_bits;
        const b = cpu % word_bits;
        if (i >= set.len) return false;
        return ((set[i] >> @intCast(b)) & 1) != 0;
    }

    fn coreKey(cpu: usize) ?u32 {
        var path_buf: [128]u8 = undefined;
        const pkg = readSysfsInt(sysfsPath(&path_buf, cpu, "physical_package_id")) orelse 0;
        const core = readSysfsInt(sysfsPath(&path_buf, cpu, "core_id")) orelse return null;
        return (@as(u32, pkg) << 16) | core;
    }

    fn sysfsPath(buf: *[128]u8, cpu: usize, leaf: []const u8) [:0]const u8 {
        return std.fmt.bufPrintZ(buf, "/sys/devices/system/cpu/cpu{d}/topology/{s}", .{ cpu, leaf }) catch {
            buf[0] = 0;
            return buf[0..0 :0];
        };
    }

    fn readSysfsInt(path: [:0]const u8) ?u16 {
        if (path.len == 0) return null;
        const fd = std.posix.openatZ(std.posix.AT.FDCWD, path, .{}, 0) catch return null;
        defer _ = std.os.linux.close(fd);
        var buf: [32]u8 = undefined;
        const n = std.posix.read(fd, &buf) catch return null;
        if (n == 0) return null;
        const line = std.mem.trim(u8, buf[0..n], " \n\t\r");
        return std.fmt.parseInt(u16, line, 10) catch null;
    }
} else struct {
    fn run() error{Unexpected}!usize {
        return error.Unexpected;
    }
};

test "physical count is at least one and not above logical" {
    const physical = count();
    try std.testing.expect(physical >= 1);
    const logical = std.Thread.getCpuCount() catch physical;
    try std.testing.expect(physical <= logical);
    try std.testing.expect(executorCount() >= 1);
}
