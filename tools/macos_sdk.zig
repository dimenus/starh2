//! Select a macOS SDK that zig's bundled libcxx can compile.
//!
//! Mechanism (qmdsync t-1749, confirmed here): SDK 27 `math.h` defines
//! INFINITY only when `__has_feature(modules)` is off, and otherwise defines
//! `__need_infinity_nan` and hands the job to the compiler's `float.h`. clang
//! reports that feature for every C++20-or-later translation unit. zig builds
//! its own libc++ at `-std=c++23`, so the branch fires with no `-fmodules`
//! anywhere. zig 0.16.0's bundled clang headers predate LLVM's protocol
//! (llvm/llvm-project PR #164348, merged upstream; zig 0.16.0 predates it).
//!
//! Measured here: `zig c++ -std=c++17 -c` has `__has_feature(modules)` off;
//! `-std=c++20` and `-std=c++23` have it on. A default `zig c++ -c` of
//! `<random>` compiles clean because the default standard is below C++20.
//! zig 0.16.0 `lib/include` has `float.h` but not `__float_float.h`,
//! `__float_header_macro.h`, or `__float_infinity_nan.h`. A zig that carries
//! the protocol ships those three. Nothing is filed on zig's tracker.
//!
//! `__need_infinity_nan` is the discriminator: it agrees with
//! `zig build-exe -lc++` on every SDK path on this machine.
//!
//! No process spawn lives in this file. `build.zig` calls `xcrun` the same
//! way `std.zig.system.darwin` does, then uses these predicates and the scan.
const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;

/// Token in `usr/include/math.h` that marks the SDK shape zig's libcxx cannot compile.
pub const workaround_token = "__need_infinity_nan";

pub const math_h_limit: Io.Limit = .limited(1024 * 1024);

/// Well-known macOS SDK search roots. No versioned SDK name lives here.
pub const default_search_roots = [_][]const u8{
    "/Library/Developer/CommandLineTools/SDKs",
    "/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs",
};

pub const Version = struct {
    major: u32 = 0,
    minor: u32 = 0,
    patch: u32 = 0,

    pub fn order(a: Version, b: Version) std.math.Order {
        if (a.major != b.major) return std.math.order(a.major, b.major);
        if (a.minor != b.minor) return std.math.order(a.minor, b.minor);
        return std.math.order(a.patch, b.patch);
    }
};

pub const Verdict = enum {
    usable,
    needs_workaround,
    missing_math_h,
    unreadable,
    math_h_too_large,
    inspect_failed,
    /// realpath/open failed with FileNotFound or NotDir. Nothing is there.
    absent,
};

pub const Candidate = struct {
    /// Sentinel slice. Free this value, not a reslice.
    resolved_path: [:0]const u8,
    version: Version,
    verdict: Verdict,
};

/// `ran == false` is the zero value: the scan never ran.
/// `scan` always returns `ran == true`, even when `candidates` is empty.
pub const ScanResult = struct {
    ran: bool = false,
    candidates: []Candidate = &.{},
    /// Roots whose directory iterator failed before it ended.
    truncated_roots: []const []const u8 = &.{},
    /// Roots that are not there (`FileNotFound` or `NotDir`). Not a blocker.
    absent_roots: []const []const u8 = &.{},
    /// Roots that exist but `openDirAbsolute` failed (AccessDenied above all).
    unreadable_roots: []const []const u8 = &.{},

    pub fn deinit(self: ScanResult, gpa: Allocator) void {
        for (self.candidates) |c| gpa.free(c.resolved_path);
        gpa.free(self.candidates);
        for (self.truncated_roots) |r| gpa.free(r);
        gpa.free(self.truncated_roots);
        for (self.absent_roots) |r| gpa.free(r);
        gpa.free(self.absent_roots);
        for (self.unreadable_roots) |r| gpa.free(r);
        gpa.free(self.unreadable_roots);
    }
};

/// Why a chosen SDK is not safe to use. Only AMBIGUOUS evidence: a higher
/// SDK we could not rule out, a truncated listing, or a present-but-unreadable
/// root. Absence is definitive and is not a blocker.
pub const ChoiceBlocker = union(enum) {
    unclear_higher: Candidate,
    truncated_root: []const u8,
    unreadable_root: []const u8,
};

/// True when `math.h` contains the token that zig's libcxx cannot compile.
pub fn sdkNeedsWorkaround(math_h_bytes: []const u8) bool {
    return std.mem.indexOf(u8, math_h_bytes, workaround_token) != null;
}

/// Parse `MacOSX<major>[.<minor>[.<patch>]].sdk`. `MacOSX.sdk` returns null.
pub fn parseVersionFromSdkName(name: []const u8) ?Version {
    const prefix = "MacOSX";
    const suffix = ".sdk";
    if (!std.mem.startsWith(u8, name, prefix)) return null;
    if (!std.mem.endsWith(u8, name, suffix)) return null;
    const mid = name[prefix.len .. name.len - suffix.len];
    if (mid.len == 0) return null;

    var it = std.mem.splitScalar(u8, mid, '.');
    const major_s = it.next() orelse return null;
    const major = std.fmt.parseInt(u32, major_s, 10) catch return null;
    var minor: u32 = 0;
    var patch: u32 = 0;
    if (it.next()) |s| {
        minor = std.fmt.parseInt(u32, s, 10) catch return null;
    }
    if (it.next()) |s| {
        patch = std.fmt.parseInt(u32, s, 10) catch return null;
    }
    if (it.next() != null) return null;
    return .{ .major = major, .minor = minor, .patch = patch };
}

fn isSdkDirName(name: []const u8) bool {
    return std.mem.startsWith(u8, name, "MacOSX") and std.mem.endsWith(u8, name, ".sdk");
}

fn isAbsentErr(err: anyerror) bool {
    return switch (err) {
        error.FileNotFound, error.NotDir => true,
        else => false,
    };
}

fn verdictFromOpenErr(err: anyerror) Verdict {
    return switch (err) {
        error.FileNotFound, error.NotDir => .absent,
        error.AccessDenied, error.PermissionDenied => .unreadable,
        else => .inspect_failed,
    };
}

fn verdictFromReadErr(err: anyerror) Verdict {
    return switch (err) {
        error.FileNotFound => .missing_math_h,
        error.StreamTooLong => .math_h_too_large,
        error.AccessDenied, error.PermissionDenied => .unreadable,
        else => .unreadable,
    };
}

/// Read one SDK root. Failures become verdicts. Only OOM is an error.
pub fn inspectSdk(gpa: Allocator, io: Io, sdk_root: []const u8) error{OutOfMemory}!Candidate {
    const version_from_input = parseVersionFromSdkName(std.fs.path.basename(sdk_root)) orelse Version{};

    const resolved = Io.Dir.cwd().realPathFileAlloc(io, sdk_root, gpa) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return .{
            .resolved_path = try gpa.dupeZ(u8, sdk_root),
            .version = version_from_input,
            .verdict = if (isAbsentErr(err)) .absent else .inspect_failed,
        };
    };
    errdefer gpa.free(resolved);

    const version = parseVersionFromSdkName(std.fs.path.basename(resolved)) orelse version_from_input;

    var sdk_dir = Io.Dir.openDirAbsolute(io, resolved, .{}) catch |err| {
        return .{
            .resolved_path = resolved,
            .version = version,
            .verdict = verdictFromOpenErr(err),
        };
    };
    defer sdk_dir.close(io);

    const math_h = sdk_dir.readFileAlloc(io, "usr/include/math.h", gpa, math_h_limit) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return .{
            .resolved_path = resolved,
            .version = version,
            .verdict = verdictFromReadErr(err),
        };
    };
    defer gpa.free(math_h);

    const verdict: Verdict = if (sdkNeedsWorkaround(math_h)) .needs_workaround else .usable;
    return .{
        .resolved_path = resolved,
        .version = version,
        .verdict = verdict,
    };
}

/// Enumerate `MacOSX*.sdk` under each root. Resolve, de-duplicate, read math.h.
/// An absent root (`FileNotFound` / `NotDir`) does not fail the scan. A root
/// that exists but cannot be read is recorded as unreadable. The result always
/// has `ran == true`. Scan does not sort: `choose` owns version order.
/// Scratch lives in an arena. The returned slices are copied onto `gpa`.
pub fn scan(gpa: Allocator, io: Io, roots: []const []const u8) error{OutOfMemory}!ScanResult {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var list: std.ArrayList(Candidate) = .empty;
    var truncated_list: std.ArrayList([]const u8) = .empty;
    var absent_list: std.ArrayList([]const u8) = .empty;
    var unreadable_list: std.ArrayList([]const u8) = .empty;
    var seen: std.StringHashMap(usize) = .init(arena);

    for (roots) |root| {
        var root_dir = Io.Dir.openDirAbsolute(io, root, .{ .iterate = true }) catch |err| {
            const copied = try arena.dupe(u8, root);
            if (isAbsentErr(err)) {
                try absent_list.append(arena, copied);
            } else {
                try unreadable_list.append(arena, copied);
            }
            continue;
        };
        defer root_dir.close(io);

        var it = root_dir.iterate();
        var truncated = false;
        while (true) {
            const entry = it.next(io) catch {
                truncated = true;
                break;
            } orelse break;
            if (!isSdkDirName(entry.name)) continue;

            const entry_path = try std.fs.path.join(arena, &.{ root, entry.name });
            const cand = try inspectSdk(arena, io, entry_path);
            const gop = try seen.getOrPut(cand.resolved_path);
            if (gop.found_existing) {
                const prev = &list.items[gop.value_ptr.*];
                if (higherThan(cand.version, prev.version)) {
                    prev.version = cand.version;
                }
                continue;
            }
            gop.value_ptr.* = list.items.len;
            try list.append(arena, cand);
        }
        if (truncated) {
            try truncated_list.append(arena, try arena.dupe(u8, root));
        }
    }

    const candidates = try gpa.alloc(Candidate, list.items.len);
    var copied: usize = 0;
    errdefer {
        for (candidates[0..copied]) |c| gpa.free(c.resolved_path);
        gpa.free(candidates);
    }
    for (candidates, list.items) |*dst, src| {
        dst.version = src.version;
        dst.verdict = src.verdict;
        dst.resolved_path = try gpa.dupeZ(u8, src.resolved_path);
        copied += 1;
    }

    const truncated_roots = try dupePathSlice(gpa, truncated_list.items);
    errdefer {
        for (truncated_roots) |r| gpa.free(r);
        gpa.free(truncated_roots);
    }
    const absent_roots = try dupePathSlice(gpa, absent_list.items);
    errdefer {
        for (absent_roots) |r| gpa.free(r);
        gpa.free(absent_roots);
    }
    const unreadable_roots = try dupePathSlice(gpa, unreadable_list.items);

    return .{
        .ran = true,
        .candidates = candidates,
        .truncated_roots = truncated_roots,
        .absent_roots = absent_roots,
        .unreadable_roots = unreadable_roots,
    };
}

fn dupePathSlice(gpa: Allocator, src: []const []const u8) error{OutOfMemory}![]const []const u8 {
    const out = try gpa.alloc([]const u8, src.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |p| gpa.free(p);
        gpa.free(out);
    }
    for (out, src) |*dst, s| {
        dst.* = try gpa.dupe(u8, s);
        filled += 1;
    }
    return out;
}

fn higherThan(a: Version, b: Version) bool {
    return a.order(b) == .gt;
}

fn chooseMatching(result: ScanResult, verdict: Verdict) ?Candidate {
    if (!result.ran) return null;
    var best: ?Candidate = null;
    for (result.candidates) |c| {
        if (c.verdict != verdict) continue;
        if (best == null or higherThan(c.version, best.?.version)) {
            best = c;
        }
    }
    return best;
}

/// Highest-version candidate with verdict `usable`. Null if the scan never ran
/// or if no candidate is usable. This is the sole owner of "which version is
/// highest" for selection.
pub fn choose(result: ScanResult) ?Candidate {
    return chooseMatching(result, .usable);
}

/// Highest-version candidate the predicate rejected (`needs_workaround`).
pub fn chooseHighestRejected(result: ScanResult) ?Candidate {
    return chooseMatching(result, .needs_workaround);
}

/// True when `c` is a rejected SDK (token present) with a version above `chosen`.
pub fn isRejectedHigher(c: Candidate, chosen: Candidate) bool {
    return c.verdict == .needs_workaround and higherThan(c.version, chosen.version);
}

/// Block only on AMBIGUOUS evidence: `unreadable`, `math_h_too_large`,
/// `inspect_failed`, a truncated listing, or a present-but-unreadable root.
/// `absent` and `missing_math_h` are definitive: nothing is hidden.
pub fn firstChoiceBlocker(result: ScanResult, chosen: ?Candidate) ?ChoiceBlocker {
    if (result.unreadable_roots.len != 0) {
        return .{ .unreadable_root = result.unreadable_roots[0] };
    }
    if (result.truncated_roots.len != 0) {
        return .{ .truncated_root = result.truncated_roots[0] };
    }
    const ch = chosen orelse return null;
    for (result.candidates) |c| {
        if (!higherThan(c.version, ch.version)) continue;
        switch (c.verdict) {
            .usable, .needs_workaround, .absent, .missing_math_h => {},
            .unreadable, .math_h_too_large, .inspect_failed => return .{ .unclear_higher = c },
        }
    }
    return null;
}

pub fn formatChoiceBlocker(gpa: Allocator, blocker: ChoiceBlocker) ![]u8 {
    return switch (blocker) {
        .unclear_higher => |c| std.fmt.allocPrint(gpa, "macOS SDK workaround: {s} version={d}.{d}.{d} verdict={s} is higher than the chosen SDK and was not ruled out", .{
            c.resolved_path,
            c.version.major,
            c.version.minor,
            c.version.patch,
            @tagName(c.verdict),
        }),
        .truncated_root => |root| std.fmt.allocPrint(gpa, "macOS SDK workaround: search root {s} was truncated; a higher SDK may be hidden", .{root}),
        .unreadable_root => |root| std.fmt.allocPrint(gpa, "macOS SDK workaround: search root {s} is present but unreadable", .{root}),
    };
}

/// Six-key libc file. On macOS only `include_dir` and `sys_include_dir` hold a path.
pub fn formatLibcFile(gpa: Allocator, include_dir: []const u8) ![]u8 {
    return std.fmt.allocPrint(gpa,
        \\include_dir={s}
        \\sys_include_dir={s}
        \\crt_dir=
        \\msvc_lib_dir=
        \\kernel32_lib_dir=
        \\gcc_dir=
        \\
    , .{ include_dir, include_dir });
}

/// Fatal-message body: every search root, every candidate, each verdict.
/// `scan_ran` makes a zero-candidate scan distinct from a scan that never ran.
/// A forced SDK outside the search roots still prints its own verdict.
pub fn formatScanReport(
    gpa: Allocator,
    roots: []const []const u8,
    result: ScanResult,
    forced: ?Candidate,
) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);

    try out.appendSlice(gpa, "macOS SDK workaround: zig's bundled libcxx cannot compile this SDK ");
    try out.appendSlice(gpa, "(math.h uses ");
    try out.appendSlice(gpa, workaround_token);
    try out.appendSlice(gpa, ").\n");

    if (forced) |c| {
        try out.print(gpa, "forced_sdk={s}\n", .{c.resolved_path});
        try out.print(gpa, "forced_verdict={s}\n", .{@tagName(c.verdict)});
    }

    try out.appendSlice(gpa, "search_roots:\n");
    if (roots.len == 0) {
        try out.appendSlice(gpa, "  (none)\n");
    } else {
        for (roots) |root| {
            try out.print(gpa, "  {s}\n", .{root});
        }
    }

    try out.print(gpa, "scan_ran={s}\n", .{if (result.ran) "true" else "false"});
    try out.print(gpa, "candidate_count={d}\n", .{result.candidates.len});
    for (result.candidates) |c| {
        try out.print(gpa, "candidate path={s} version={d}.{d}.{d} verdict={s}\n", .{
            c.resolved_path,
            c.version.major,
            c.version.minor,
            c.version.patch,
            @tagName(c.verdict),
        });
    }
    if (result.truncated_roots.len != 0) {
        try out.appendSlice(gpa, "truncated_roots:\n");
        for (result.truncated_roots) |root| {
            try out.print(gpa, "  {s}\n", .{root});
        }
    }
    if (result.absent_roots.len != 0) {
        try out.appendSlice(gpa, "absent_roots:\n");
        for (result.absent_roots) |root| {
            try out.print(gpa, "  {s}\n", .{root});
        }
    }
    if (result.unreadable_roots.len != 0) {
        try out.appendSlice(gpa, "unreadable_roots:\n");
        for (result.unreadable_roots) |root| {
            try out.print(gpa, "  {s}\n", .{root});
        }
    }

    return out.toOwnedSlice(gpa);
}

fn plantSdk(
    gpa: Allocator,
    dir: Io.Dir,
    io: Io,
    major: u32,
    minor: u32,
    math_h: []const u8,
) !void {
    const name = try std.fmt.allocPrint(gpa, "MacOSX{d}.{d}.sdk", .{ major, minor });
    defer gpa.free(name);
    const include_rel = try std.fmt.allocPrint(gpa, "{s}/usr/include", .{name});
    defer gpa.free(include_rel);
    try dir.createDirPath(io, include_rel);
    const math_rel = try std.fmt.allocPrint(gpa, "{s}/usr/include/math.h", .{name});
    defer gpa.free(math_rel);
    try dir.writeFile(io, .{ .sub_path = math_rel, .data = math_h });
}

fn plantLink(
    gpa: Allocator,
    dir: Io.Dir,
    io: Io,
    target_major: u32,
    target_minor: u32,
    link_name: []const u8,
) !void {
    const target = try std.fmt.allocPrint(gpa, "MacOSX{d}.{d}.sdk", .{ target_major, target_minor });
    defer gpa.free(target);
    try dir.symLink(io, target, link_name, .{ .is_directory = true });
}

fn tmpRootAbs(gpa: Allocator, io: Io, tmp: std.testing.TmpDir) ![:0]u8 {
    return tmp.parent_dir.realPathFileAlloc(io, &tmp.sub_path, gpa);
}

fn findByVersion(result: ScanResult, major: u32, minor: u32) ?Candidate {
    for (result.candidates) |c| {
        if (c.version.major == major and c.version.minor == minor) return c;
    }
    return null;
}

test "sdkNeedsWorkaround is true only when the token is present" {
    try std.testing.expect(!sdkNeedsWorkaround("#define INFINITY __builtin_inff()\n"));
    try std.testing.expect(!sdkNeedsWorkaround(""));
    try std.testing.expect(sdkNeedsWorkaround("__need_infinity_nan"));
    try std.testing.expect(sdkNeedsWorkaround("/* other text */\n#define __need_infinity_nan 1\n"));
}

test "parseVersionFromSdkName reads the resolved basename" {
    var buf: [32]u8 = undefined;
    const name_265 = std.fmt.bufPrint(&buf, "MacOSX{d}.{d}.sdk", .{ 26, 5 }) catch unreachable;
    const v26 = parseVersionFromSdkName(name_265) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 26), v26.major);
    try std.testing.expectEqual(@as(u32, 5), v26.minor);
    try std.testing.expectEqual(@as(u32, 0), v26.patch);

    const name_270 = std.fmt.bufPrint(&buf, "MacOSX{d}.{d}.sdk", .{ 27, 0 }) catch unreachable;
    const v27 = parseVersionFromSdkName(name_270) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 27), v27.major);
    try std.testing.expectEqual(@as(u32, 0), v27.minor);

    const name_26 = std.fmt.bufPrint(&buf, "MacOSX{d}.sdk", .{26}) catch unreachable;
    const v26_only = parseVersionFromSdkName(name_26) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 26), v26_only.major);
    try std.testing.expectEqual(@as(u32, 0), v26_only.minor);

    try std.testing.expect(parseVersionFromSdkName("MacOSX.sdk") == null);
    try std.testing.expect(parseVersionFromSdkName("not-an-sdk") == null);
}

test "scan enumerates every unique SDK and collapses symlink duplicates" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const good = "#define INFINITY 1\n";
    const bad = "#define __need_infinity_nan\n";
    try plantSdk(gpa, tmp.dir, io, 26, 5, good);
    try plantSdk(gpa, tmp.dir, io, 27, 0, bad);
    var link_buf: [32]u8 = undefined;
    const link26 = std.fmt.bufPrint(&link_buf, "MacOSX{d}.sdk", .{26}) catch unreachable;
    try plantLink(gpa, tmp.dir, io, 26, 5, link26);
    const link27 = std.fmt.bufPrint(&link_buf, "MacOSX{d}.sdk", .{27}) catch unreachable;
    try plantLink(gpa, tmp.dir, io, 27, 0, link27);
    try plantLink(gpa, tmp.dir, io, 27, 0, "MacOSX.sdk");

    const root_abs = try tmpRootAbs(gpa, io, tmp);
    defer gpa.free(root_abs);

    const result = try scan(gpa, io, &.{root_abs});
    defer result.deinit(gpa);

    try std.testing.expect(result.ran);
    try std.testing.expectEqual(@as(usize, 2), result.candidates.len);
    try std.testing.expect(findByVersion(result, 26, 5) != null);
    try std.testing.expect(findByVersion(result, 27, 0) != null);
    try std.testing.expectEqual(Verdict.usable, findByVersion(result, 26, 5).?.verdict);
    try std.testing.expectEqual(Verdict.needs_workaround, findByVersion(result, 27, 0).?.verdict);
}

test "chooser picks the highest usable version from ascending input" {
    const gpa = std.testing.allocator;
    const p15 = try gpa.dupeZ(u8, "/sdk/low");
    const p26 = try gpa.dupeZ(u8, "/sdk/mid");
    const p27 = try gpa.dupeZ(u8, "/sdk/high");
    defer gpa.free(p15);
    defer gpa.free(p26);
    defer gpa.free(p27);

    var cands = [_]Candidate{
        .{ .resolved_path = p15, .version = .{ .major = 15, .minor = 2 }, .verdict = .usable },
        .{ .resolved_path = p26, .version = .{ .major = 26, .minor = 5 }, .verdict = .usable },
        .{ .resolved_path = p27, .version = .{ .major = 27, .minor = 0 }, .verdict = .needs_workaround },
    };
    const result: ScanResult = .{ .ran = true, .candidates = &cands };

    const chosen = choose(result) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 26), chosen.version.major);
    try std.testing.expectEqual(@as(u32, 5), chosen.version.minor);
    try std.testing.expectEqual(Verdict.usable, chosen.verdict);

    const rejected = chooseHighestRejected(result) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 27), rejected.version.major);
    try std.testing.expect(isRejectedHigher(rejected, chosen));
    try std.testing.expect(firstChoiceBlocker(result, chosen) == null);
}

test "isRejectedHigher matches every rejected SDK above chosen" {
    const p26 = try std.testing.allocator.dupeZ(u8, "/sdk/a");
    const p270 = try std.testing.allocator.dupeZ(u8, "/sdk/b");
    const p271 = try std.testing.allocator.dupeZ(u8, "/sdk/c");
    defer std.testing.allocator.free(p26);
    defer std.testing.allocator.free(p270);
    defer std.testing.allocator.free(p271);
    const chosen: Candidate = .{ .resolved_path = p26, .version = .{ .major = 26, .minor = 5 }, .verdict = .usable };
    const r0: Candidate = .{ .resolved_path = p270, .version = .{ .major = 27, .minor = 0 }, .verdict = .needs_workaround };
    const r1: Candidate = .{ .resolved_path = p271, .version = .{ .major = 27, .minor = 1 }, .verdict = .needs_workaround };
    try std.testing.expect(isRejectedHigher(r0, chosen));
    try std.testing.expect(isRejectedHigher(r1, chosen));
    try std.testing.expect(!isRejectedHigher(chosen, chosen));
}

test "scan of a root with no SDK returns zero candidates and is distinct from not-run" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(io, .{ .sub_path = "readme.txt", .data = "not an sdk\n" });

    const root_abs = try tmpRootAbs(gpa, io, tmp);
    defer gpa.free(root_abs);

    const result = try scan(gpa, io, &.{root_abs});
    defer result.deinit(gpa);

    try std.testing.expect(result.ran);
    try std.testing.expectEqual(@as(usize, 0), result.candidates.len);
    try std.testing.expect(choose(result) == null);

    const never: ScanResult = .{};
    try std.testing.expect(!never.ran);
    try std.testing.expectEqual(@as(usize, 0), never.candidates.len);
    try std.testing.expect(choose(never) == null);

    const empty_report = try formatScanReport(gpa, &.{root_abs}, result, null);
    defer gpa.free(empty_report);
    try std.testing.expect(std.mem.indexOf(u8, empty_report, "scan_ran=true") != null);
    try std.testing.expect(std.mem.indexOf(u8, empty_report, "candidate_count=0") != null);

    const never_report = try formatScanReport(gpa, &.{root_abs}, never, null);
    defer gpa.free(never_report);
    try std.testing.expect(std.mem.indexOf(u8, never_report, "scan_ran=false") != null);

    const trunc_root = try gpa.dupe(u8, root_abs);
    defer gpa.free(trunc_root);
    const trunc: ScanResult = .{
        .ran = true,
        .candidates = &.{},
        .truncated_roots = &.{trunc_root},
    };
    const trunc_report = try formatScanReport(gpa, &.{root_abs}, trunc, null);
    defer gpa.free(trunc_report);
    try std.testing.expect(std.mem.indexOf(u8, trunc_report, "truncated_roots:") != null);
    try std.testing.expect(std.mem.indexOf(u8, trunc_report, root_abs) != null);
}

test "absent search root does not block a choice" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try plantSdk(gpa, tmp.dir, io, 26, 5, "#define INFINITY 1\n");
    const root_abs = try tmpRootAbs(gpa, io, tmp);
    defer gpa.free(root_abs);
    const missing = "/no/such/macos-sdk-root";
    const result = try scan(gpa, io, &.{ root_abs, missing });
    defer result.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 1), result.absent_roots.len);
    try std.testing.expectEqualStrings(missing, result.absent_roots[0]);
    try std.testing.expectEqual(@as(usize, 0), result.unreadable_roots.len);
    const chosen = choose(result) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 26), chosen.version.major);
    try std.testing.expect(firstChoiceBlocker(result, chosen) == null);
}

test "SDK without math.h is not usable" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const name = try std.fmt.allocPrint(gpa, "MacOSX{d}.{d}.sdk", .{ 10, 0 });
    defer gpa.free(name);
    try tmp.dir.createDirPath(io, name);

    const root_abs = try tmpRootAbs(gpa, io, tmp);
    defer gpa.free(root_abs);

    const result = try scan(gpa, io, &.{root_abs});
    defer result.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 1), result.candidates.len);
    try std.testing.expectEqual(Verdict.missing_math_h, result.candidates[0].verdict);
    try std.testing.expect(choose(result) == null);
}

test "dangling higher SDK symlink is absent and does not block" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try plantSdk(gpa, tmp.dir, io, 26, 5, "#define INFINITY 1\n");
    var name_buf: [32]u8 = undefined;
    const dangling = std.fmt.bufPrint(&name_buf, "MacOSX{d}.sdk", .{28}) catch unreachable;
    try tmp.dir.symLink(io, "does-not-exist.sdk", dangling, .{ .is_directory = true });

    const root_abs = try tmpRootAbs(gpa, io, tmp);
    defer gpa.free(root_abs);
    const result = try scan(gpa, io, &.{root_abs});
    defer result.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 2), result.candidates.len);
    try std.testing.expectEqual(Verdict.absent, findByVersion(result, 28, 0).?.verdict);
    const chosen = choose(result) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 26), chosen.version.major);
    try std.testing.expect(firstChoiceBlocker(result, chosen) == null);
}

test "higher SDK directory with no math.h does not block" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try plantSdk(gpa, tmp.dir, io, 26, 5, "#define INFINITY 1\n");
    const empty = try std.fmt.allocPrint(gpa, "MacOSX{d}.{d}.sdk", .{ 28, 0 });
    defer gpa.free(empty);
    try tmp.dir.createDirPath(io, empty);

    const root_abs = try tmpRootAbs(gpa, io, tmp);
    defer gpa.free(root_abs);
    const result = try scan(gpa, io, &.{root_abs});
    defer result.deinit(gpa);

    try std.testing.expectEqual(Verdict.missing_math_h, findByVersion(result, 28, 0).?.verdict);
    const chosen = choose(result) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u32, 26), chosen.version.major);
    try std.testing.expect(firstChoiceBlocker(result, chosen) == null);
}

test "higher inspect_failed still blocks" {
    const gpa = std.testing.allocator;
    const p26 = try gpa.dupeZ(u8, "/sdk/mid");
    const p28 = try gpa.dupeZ(u8, "/sdk/high-io");
    defer gpa.free(p26);
    defer gpa.free(p28);
    var cands = [_]Candidate{
        .{ .resolved_path = p26, .version = .{ .major = 26, .minor = 5 }, .verdict = .usable },
        .{ .resolved_path = p28, .version = .{ .major = 28, .minor = 0 }, .verdict = .inspect_failed },
    };
    const result: ScanResult = .{ .ran = true, .candidates = &cands };
    const block = firstChoiceBlocker(result, cands[0]) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(Verdict.inspect_failed, block.unclear_higher.verdict);
}

test "duplicate aliases keep the highest version" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "MacOSX.sdk/usr/include");
    try tmp.dir.writeFile(io, .{ .sub_path = "MacOSX.sdk/usr/include/math.h", .data = "#define __need_infinity_nan\n" });
    var ver_buf: [32]u8 = undefined;
    const ver_name = std.fmt.bufPrint(&ver_buf, "MacOSX{d}.{d}.sdk", .{ 27, 0 }) catch unreachable;
    try tmp.dir.symLink(io, "MacOSX.sdk", ver_name, .{ .is_directory = true });

    const root_abs = try tmpRootAbs(gpa, io, tmp);
    defer gpa.free(root_abs);
    const result = try scan(gpa, io, &.{root_abs});
    defer result.deinit(gpa);

    try std.testing.expectEqual(@as(usize, 1), result.candidates.len);
    try std.testing.expectEqual(@as(u32, 27), result.candidates[0].version.major);
    try std.testing.expectEqual(@as(u32, 0), result.candidates[0].version.minor);
    try std.testing.expectEqual(Verdict.needs_workaround, result.candidates[0].verdict);
}

test "truncated root is a choice blocker" {
    const gpa = std.testing.allocator;
    const p26 = try gpa.dupeZ(u8, "/sdk/mid");
    defer gpa.free(p26);
    var cands = [_]Candidate{
        .{ .resolved_path = p26, .version = .{ .major = 26, .minor = 5 }, .verdict = .usable },
    };
    const trunc = "/sdk-root-truncated";
    const result: ScanResult = .{
        .ran = true,
        .candidates = &cands,
        .truncated_roots = &.{trunc},
    };
    const block = firstChoiceBlocker(result, cands[0]) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(trunc, block.truncated_root);
    const msg = try formatChoiceBlocker(gpa, block);
    defer gpa.free(msg);
    try std.testing.expect(std.mem.indexOf(u8, msg, trunc) != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "truncated") != null);
}

test "math.h over the size limit is math_h_too_large" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const name = try std.fmt.allocPrint(gpa, "MacOSX{d}.{d}.sdk", .{ 9, 0 });
    defer gpa.free(name);
    const include_rel = try std.fmt.allocPrint(gpa, "{s}/usr/include", .{name});
    defer gpa.free(include_rel);
    try tmp.dir.createDirPath(io, include_rel);
    const math_rel = try std.fmt.allocPrint(gpa, "{s}/usr/include/math.h", .{name});
    defer gpa.free(math_rel);
    const big = try gpa.alloc(u8, 1024 * 1024 + 1);
    defer gpa.free(big);
    @memset(big, 'x');
    try tmp.dir.writeFile(io, .{ .sub_path = math_rel, .data = big });

    const root_abs = try tmpRootAbs(gpa, io, tmp);
    defer gpa.free(root_abs);
    const sdk_abs = try std.fs.path.join(gpa, &.{ root_abs, name });
    defer gpa.free(sdk_abs);

    const cand = try inspectSdk(gpa, io, sdk_abs);
    defer gpa.free(cand.resolved_path);
    try std.testing.expectEqual(Verdict.math_h_too_large, cand.verdict);
}

test "unreadable math.h is not missing_math_h" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try plantSdk(gpa, tmp.dir, io, 8, 0, "#define INFINITY 1\n");
    const name = try std.fmt.allocPrint(gpa, "MacOSX{d}.{d}.sdk", .{ 8, 0 });
    defer gpa.free(name);
    const math_rel = try std.fmt.allocPrint(gpa, "{s}/usr/include/math.h", .{name});
    defer gpa.free(math_rel);

    var file = try tmp.dir.openFile(io, math_rel, .{ .mode = .read_write });
    defer {
        file.setPermissions(io, Io.File.Permissions.fromMode(0o644)) catch {};
        file.close(io);
    }
    try file.setPermissions(io, Io.File.Permissions.fromMode(0));

    const root_abs = try tmpRootAbs(gpa, io, tmp);
    defer gpa.free(root_abs);
    const sdk_abs = try std.fs.path.join(gpa, &.{ root_abs, name });
    defer gpa.free(sdk_abs);

    const cand = try inspectSdk(gpa, io, sdk_abs);
    defer gpa.free(cand.resolved_path);
    try std.testing.expectEqual(Verdict.unreadable, cand.verdict);
}

test "formatLibcFile writes the six keys and leaves crt_dir empty" {
    const gpa = std.testing.allocator;
    const bytes = try formatLibcFile(gpa, "/sdk/usr/include");
    defer gpa.free(bytes);

    try std.testing.expect(std.mem.indexOf(u8, bytes, "include_dir=/sdk/usr/include\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "sys_include_dir=/sdk/usr/include\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "crt_dir=\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "msvc_lib_dir=\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "kernel32_lib_dir=\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "gcc_dir=\n") != null);
}

test "formatScanReport names every root and every candidate verdict" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try plantSdk(gpa, tmp.dir, io, 27, 0, "#define __need_infinity_nan\n");

    const root_abs = try tmpRootAbs(gpa, io, tmp);
    defer gpa.free(root_abs);

    const result = try scan(gpa, io, &.{root_abs});
    defer result.deinit(gpa);

    const forced_path = try gpa.dupeZ(u8, "/outside/search/roots.sdk");
    defer gpa.free(forced_path);
    const forced: Candidate = .{
        .resolved_path = forced_path,
        .version = .{ .major = 99 },
        .verdict = .needs_workaround,
    };
    const report = try formatScanReport(gpa, &.{root_abs}, result, forced);
    defer gpa.free(report);

    try std.testing.expect(std.mem.indexOf(u8, report, workaround_token) != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "forced_sdk=/outside/search/roots.sdk") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "forced_verdict=needs_workaround") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, root_abs) != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "scan_ran=true") != null);
    try std.testing.expect(std.mem.indexOf(u8, report, "verdict=needs_workaround") != null);
}
