//! The ThreadSanitizer lane's runner (`./zb build tsan`). Not in `ci`.
//!
//!   tsan-runner --suppressions <file> --log-dir <dir> --timeout-s <n>
//!               --runs <n> --canary <bin> (--bin <name> <path>)...
//!
//! build.zig passes the binaries; they are test binaries built with
//! `sanitize_thread` on their root module (zio, std and starh2 inherit it).
//!
//! What it does, in order, and what fails the lane (exit 1):
//!
//! 1. Checks the suppressions file: every entry needs a `#` reason directly
//!    above it, or the runner aborts. TSan matches a `race:` entry against
//!    every frame of every stack in a report (both accesses and both thread
//!    creation stacks), so one broad entry can hide a whole subsystem. The
//!    file's own header carries the rule for adding one.
//! 2. Runs the canary (`tests/tsan_canary.zig`) once. It must exit 66 (TSan's
//!    report exit code) with a data race that names `tsan_canary.racer`.
//!    Otherwise the build cannot see a planted race and the runner stops
//!    before calling anything clean.
//! 3. Runs every `--bin` `--runs` times, each run under a wall-clock limit of
//!    `--timeout-s` (SIGKILL after it). A run is clean only if it exited 0,
//!    printed no TSan report, and its test runner printed a passed count
//!    above zero (a binary that ran no test did no work).
//!
//! Each run writes stdout and stderr, TSan reports included, to
//! `<log-dir>/<unix seconds>/<name>.run<i>.log`. The summary at the end lists
//! every distinct report signature (the first non-interceptor frame of each
//! of the two accesses) with the binaries it came from, so a report seen in
//! eight binaries is triaged once.
//!
//! TSan is Linux first. Zig builds its runtime from source for the target;
//! see the lane's commit message for which systems were measured.
const std = @import("std");

const tsan_report_exit: u8 = 66;

fn abort(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("tsan-runner ABORT: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

const Bin = struct { name: []const u8, path: []const u8 };

const RunResult = struct {
    name: []const u8,
    run: usize,
    secs: f64,
    term: std.process.Child.Term,
    timed_out: bool,
    reports: usize,
    passed: ?usize,
    log_path: []const u8,
    signatures: []const []const u8,

    fn clean(r: RunResult) bool {
        const exited_zero = switch (r.term) {
            .exited => |c| c == 0,
            else => false,
        };
        return exited_zero and !r.timed_out and r.reports == 0 and (r.passed orelse 0) > 0;
    }
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.arena.allocator();
    const io = init.io;

    var suppressions: []const u8 = "";
    var log_root: []const u8 = "";
    var timeout_s: u64 = 0;
    var runs: usize = 0;
    var canary: []const u8 = "";
    var bins: std.ArrayList(Bin) = .empty;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |a| {
        if (std.mem.eql(u8, a, "--suppressions")) {
            suppressions = args.next() orelse abort("--suppressions needs a value", .{});
        } else if (std.mem.eql(u8, a, "--log-dir")) {
            log_root = args.next() orelse abort("--log-dir needs a value", .{});
        } else if (std.mem.eql(u8, a, "--timeout-s")) {
            const v = args.next() orelse abort("--timeout-s needs a value", .{});
            timeout_s = std.fmt.parseInt(u64, v, 10) catch abort("--timeout-s {s} is not a number", .{v});
        } else if (std.mem.eql(u8, a, "--runs")) {
            const v = args.next() orelse abort("--runs needs a value", .{});
            runs = std.fmt.parseInt(usize, v, 10) catch abort("--runs {s} is not a number", .{v});
        } else if (std.mem.eql(u8, a, "--canary")) {
            canary = args.next() orelse abort("--canary needs a value", .{});
        } else if (std.mem.eql(u8, a, "--bin")) {
            const name = args.next() orelse abort("--bin needs a name and a path", .{});
            const path = args.next() orelse abort("--bin {s} needs a path", .{name});
            try bins.append(gpa, .{ .name = name, .path = path });
        } else abort("unknown argument {s}", .{a});
    }
    if (suppressions.len == 0) abort("--suppressions is required", .{});
    if (log_root.len == 0) abort("--log-dir is required", .{});
    if (timeout_s == 0) abort("--timeout-s must be above zero", .{});
    if (runs == 0) abort("--runs must be above zero", .{});
    if (canary.len == 0) abort("--canary is required: a lane that cannot show it sees a planted race cannot call anything clean", .{});
    if (bins.items.len == 0) abort("no --bin given", .{});

    const cwd = std.Io.Dir.cwd();
    const supp_abs = cwd.realPathFileAlloc(io, suppressions, gpa) catch |err|
        abort("suppressions file {s}: {t}", .{ suppressions, err });
    const entries = checkSuppressions(gpa, io, supp_abs);

    const stamp: i96 = @divTrunc(std.Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_s);
    const log_dir = try std.fmt.allocPrint(gpa, "{s}/{d}", .{ log_root, stamp });
    cwd.createDirPath(io, log_dir) catch |err| abort("could not create {s}: {t}", .{ log_dir, err });
    const log_dir_abs = try cwd.realPathFileAlloc(io, log_dir, gpa);

    // abort_on_error=0: its default is 1 on Darwin, where a report then ends
    // the process with SIGABRT instead of `exitcode`; Linux defaults to 0.
    const tsan_options = try std.fmt.allocPrint(
        gpa,
        "halt_on_error=0 abort_on_error=0 exitcode={d} history_size=7 print_suppressions=1 suppressions={s}",
        .{ tsan_report_exit, supp_abs },
    );
    var env = try init.environ_map.clone(gpa);
    try env.put("TSAN_OPTIONS", tsan_options);

    std.debug.print(
        "tsan-runner: {d} binaries x {d} runs, limit {d} s per run, {d} suppression entries\n" ++
            "tsan-runner: TSAN_OPTIONS={s}\ntsan-runner: logs in {s}\n",
        .{ bins.items.len, runs, timeout_s, entries, tsan_options, log_dir_abs },
    );

    const lane_start = std.Io.Timestamp.now(io, .awake);

    const c = try runOne(gpa, io, &env, log_dir_abs, .{ .name = "tsan_canary", .path = canary }, 1, timeout_s);
    const canary_exit_ok = switch (c.term) {
        .exited => |code| code == tsan_report_exit,
        else => false,
    };
    const canary_log = try cwd.readFileAlloc(io, c.log_path, gpa, .limited(256 << 20));
    const canary_named = std.mem.indexOf(u8, canary_log, "tsan_canary.racer") != null;
    if (!canary_exit_ok or c.reports == 0 or !canary_named or (c.passed orelse 0) == 0) {
        abort(
            "the canary did not fire (term {any}, reports {d}, names tsan_canary.racer: {}, passed {?d}); see {s}. " ++
                "This build cannot see a planted race, so no other result from it means anything.",
            .{ c.term, c.reports, canary_named, c.passed, c.log_path },
        );
    }
    std.debug.print("tsan-runner: canary fired ({d} reports, exit {d}, {d:.1} s)\n", .{ c.reports, tsan_report_exit, c.secs });

    var results: std.ArrayList(RunResult) = .empty;
    for (bins.items) |bin| {
        for (1..runs + 1) |i| {
            const r = try runOne(gpa, io, &env, log_dir_abs, bin, i, timeout_s);
            std.debug.print("tsan-runner: {s} run {d}: {s}, term {any}, reports {d}, passed {?d}, {d:.1} s\n", .{
                r.name, r.run,     if (r.clean()) "clean" else if (r.timed_out) "TIMEOUT" else "DIRTY",
                r.term, r.reports, r.passed,
                r.secs,
            });
            try results.append(gpa, r);
        }
    }
    const lane_secs = nsToSecs(lane_start.durationTo(std.Io.Timestamp.now(io, .awake)).nanoseconds);

    // Distinct signatures, each with the binaries it was seen in.
    var sig_bins: std.StringArrayHashMapUnmanaged(std.ArrayList([]const u8)) = .empty;
    var sig_count: std.StringArrayHashMapUnmanaged(usize) = .empty;
    var dirty: usize = 0;
    for (results.items) |r| {
        if (!r.clean()) dirty += 1;
        for (r.signatures) |s| {
            const gop = try sig_bins.getOrPut(gpa, s);
            if (!gop.found_existing) gop.value_ptr.* = .empty;
            var have = false;
            for (gop.value_ptr.items) |n| have = have or std.mem.eql(u8, n, r.name);
            if (!have) try gop.value_ptr.append(gpa, r.name);
            const cnt = try sig_count.getOrPut(gpa, s);
            if (!cnt.found_existing) cnt.value_ptr.* = 0;
            cnt.value_ptr.* += 1;
        }
    }

    std.debug.print("\ntsan-runner: {d} binaries, {d} runs, {d} dirty, lane wall time {d:.1} s (canary included)\n", .{
        bins.items.len, results.items.len, dirty, lane_secs,
    });
    if (sig_bins.count() != 0) {
        std.debug.print("tsan-runner: {d} distinct report signatures (reports, binaries):\n", .{sig_bins.count()});
        for (sig_bins.keys(), sig_bins.values()) |s, names| {
            std.debug.print("  [{d}x] {s}\n        in:", .{ sig_count.get(s).?, s });
            for (names.items) |n| std.debug.print(" {s}", .{n});
            std.debug.print("\n", .{});
        }
    }
    for (results.items) |r| {
        if (!r.clean()) std.debug.print("tsan-runner: DIRTY {s} run {d}: {s}\n", .{ r.name, r.run, r.log_path });
    }
    if (dirty != 0) std.process.exit(1);
    std.debug.print("tsan-runner: every run clean, canary fired\n", .{});
}

fn nsToSecs(ns: i96) f64 {
    return @as(f64, @floatFromInt(ns)) / std.time.ns_per_s;
}

/// Aborts unless every entry has a `#` reason line directly above it, and
/// returns the number of entries. An entry is any line that is not blank and
/// not a comment.
fn checkSuppressions(gpa: std.mem.Allocator, io: std.Io, path: []const u8) usize {
    const text = std.Io.Dir.cwd().readFileAlloc(io, path, gpa, .limited(1 << 20)) catch |err|
        abort("read {s}: {t}", .{ path, err });
    var entries: usize = 0;
    var prev_is_reason = false;
    var line_no: usize = 0;
    var it = std.mem.splitScalar(u8, text, '\n');
    while (it.next()) |raw| {
        line_no += 1;
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0) {
            prev_is_reason = false;
            continue;
        }
        if (line[0] == '#') {
            prev_is_reason = line.len > 1;
            continue;
        }
        if (!prev_is_reason) abort("{s}:{d}: suppression `{s}` has no `#` reason directly above it", .{ path, line_no, line });
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse
            abort("{s}:{d}: `{s}` is not `<type>:<pattern>`", .{ path, line_no, line });
        if (colon + 1 >= line.len) abort("{s}:{d}: `{s}` has an empty pattern", .{ path, line_no, line });
        entries += 1;
        // Several entries may share one reason block only if each is listed
        // right under it; a blank line ends the block.
    }
    return entries;
}

fn runOne(
    gpa: std.mem.Allocator,
    io: std.Io,
    env: *const std.process.Environ.Map,
    log_dir: []const u8,
    bin: Bin,
    run: usize,
    timeout_s: u64,
) !RunResult {
    const log_path = try std.fmt.allocPrint(gpa, "{s}/{s}.run{d}.log", .{ log_dir, bin.name, run });
    const cwd = std.Io.Dir.cwd();
    const log_file = cwd.createFile(io, log_path, .{}) catch |err| abort("create {s}: {t}", .{ log_path, err });

    const start = std.Io.Timestamp.now(io, .awake);
    var child = std.process.spawn(io, .{
        .argv = &.{bin.path},
        .environ_map = env,
        .stdin = .ignore,
        .stdout = .{ .file = log_file },
        .stderr = .{ .file = log_file },
    }) catch |err| abort("spawn {s} ({s}): {t}", .{ bin.name, bin.path, err });
    const pid = child.id orelse abort("spawn {s}: no pid", .{bin.name});

    var timed_out: std.atomic.Value(bool) = .init(false);
    const Wd = struct {
        fn watch(p: std.process.Child.Id, inner: std.Io, ns: u64, flag: *std.atomic.Value(bool)) void {
            inner.sleep(.fromNanoseconds(ns), .awake) catch return;
            flag.store(true, .release);
            std.posix.kill(p, std.posix.SIG.KILL) catch {};
        }
    };
    var wd = io.concurrent(Wd.watch, .{ pid, io, timeout_s * std.time.ns_per_s, &timed_out }) catch
        abort("watchdog spawn failed; an unbounded run is not a result", .{});
    const term = child.wait(io) catch |err| abort("wait {s}: {t}", .{ bin.name, err });
    wd.cancel(io);
    log_file.close(io);
    const secs = nsToSecs(start.durationTo(std.Io.Timestamp.now(io, .awake)).nanoseconds);

    const log = try cwd.readFileAlloc(io, log_path, gpa, .limited(256 << 20));
    return .{
        .name = bin.name,
        .run = run,
        .secs = secs,
        .term = term,
        .timed_out = timed_out.load(.acquire),
        .reports = std.mem.count(u8, log, "WARNING: ThreadSanitizer:"),
        .passed = passedCount(log),
        .log_path = log_path,
        .signatures = try signatures(gpa, log),
    };
}

/// The test runner's passed count: "All N tests passed." or
/// "N passed; M skipped; K failed.". Null if neither line is there (the
/// process died before the summary).
fn passedCount(log: []const u8) ?usize {
    if (std.mem.lastIndexOf(u8, log, "All ")) |i| {
        const rest = log[i + 4 ..];
        const end = std.mem.indexOfScalar(u8, rest, ' ') orelse return null;
        if (std.mem.startsWith(u8, rest[end..], " tests passed.")) {
            return std.fmt.parseInt(usize, rest[0..end], 10) catch null;
        }
    }
    if (std.mem.lastIndexOf(u8, log, " passed; ")) |i| {
        var s = i;
        while (s > 0 and std.ascii.isDigit(log[s - 1])) s -= 1;
        if (s == i) return null;
        return std.fmt.parseInt(usize, log[s..i], 10) catch null;
    }
    return null;
}

/// One signature per report: its kind, then the first frame outside the TSan
/// runtime under each access header ("Read of size", "Previous write of
/// size", ...), as `function file:line:col`. The frames are sorted, and
/// Zig's `__func_N` instance suffixes dropped, so one pair of sites is one
/// signature whichever side TSan saw first and whichever generic instance
/// ran.
fn signatures(gpa: std.mem.Allocator, log: []const u8) ![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    var lines = std.mem.splitScalar(u8, log, '\n');
    var kind: ?[]const u8 = null;
    var frames: std.ArrayList([]const u8) = .empty;
    var want_frame = false;
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, "WARNING: ThreadSanitizer: ")) |i| {
            const k = std.mem.trim(u8, line[i + "WARNING: ThreadSanitizer: ".len ..], " ");
            const paren = std.mem.indexOf(u8, k, " (pid=") orelse k.len;
            kind = k[0..paren];
            frames.clearRetainingCapacity();
            want_frame = false;
            continue;
        }
        if (kind == null) continue;
        if (std.mem.startsWith(u8, line, "SUMMARY: ThreadSanitizer")) {
            std.mem.sort([]const u8, frames.items, {}, lessThan);
            var sig: std.ArrayList(u8) = .empty;
            try sig.appendSlice(gpa, kind.?);
            for (frames.items) |f| {
                try sig.appendSlice(gpa, " | ");
                try sig.appendSlice(gpa, f);
            }
            try out.append(gpa, try sig.toOwnedSlice(gpa));
            kind = null;
            continue;
        }
        if (isAccessHeader(line)) {
            want_frame = true;
            continue;
        }
        if (want_frame) {
            const t = std.mem.trimStart(u8, line, " ");
            if (t.len == 0 or t[0] != '#') {
                want_frame = false;
                continue;
            }
            if (std.mem.indexOf(u8, t, "libtsan/") != null or std.mem.indexOf(u8, t, " __tsan_") != null) continue;
            try frames.append(gpa, try frameString(gpa, t));
            want_frame = false;
        }
    }
    return out.toOwnedSlice(gpa);
}

fn lessThan(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}

fn isAccessHeader(line: []const u8) bool {
    const t = std.mem.trimStart(u8, line, " ");
    const heads = [_][]const u8{ "Read of size", "Write of size", "Previous read", "Previous write", "Atomic read", "Atomic write", "Previous atomic" };
    for (heads) |h| if (std.mem.startsWith(u8, t, h)) return true;
    return false;
}

/// "#1 fn.name__func_12 /abs/.../zig-pkg/zio-0.19.0-xHb.../src/ev/loop.zig:674:26 (bin+0x1)"
/// -> "fn.name zio/src/ev/loop.zig:674:26". The path is cut so one signature
/// is the same on every machine: a package is named by its name, std by
/// `std/`, and this repo's files from `src/`, `tests/` or `tools/`.
fn frameString(gpa: std.mem.Allocator, t: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    const sp = std.mem.indexOfScalar(u8, t, ' ') orelse return gpa.dupe(u8, t);
    var rest = t[sp + 1 ..];
    if (std.mem.indexOf(u8, rest, " (")) |p| rest = rest[0..p];
    const sp2 = std.mem.indexOfScalar(u8, rest, ' ') orelse return gpa.dupe(u8, rest);
    var func = rest[0..sp2];
    if (std.mem.indexOf(u8, func, "__func_")) |i| func = func[0..i];
    const path = rest[sp2 + 1 ..];
    try out.appendSlice(gpa, func);
    try out.append(gpa, ' ');
    if (std.mem.indexOf(u8, path, "/zig-pkg/")) |i| {
        const pkg = path[i + "/zig-pkg/".len ..];
        if (std.mem.indexOfScalar(u8, pkg, '/')) |slash| {
            const dash = std.mem.indexOfScalar(u8, pkg[0..slash], '-') orelse slash;
            try out.appendSlice(gpa, pkg[0..dash]);
            try out.appendSlice(gpa, pkg[slash..]);
        } else try out.appendSlice(gpa, pkg);
    } else if (std.mem.indexOf(u8, path, "/lib/std/")) |i| {
        try out.appendSlice(gpa, path[i + "/lib/".len ..]);
    } else for ([_][]const u8{ "/src/", "/tests/", "/tools/" }) |d| {
        if (std.mem.lastIndexOf(u8, path, d)) |i| {
            try out.appendSlice(gpa, path[i + 1 ..]);
            break;
        }
    } else try out.appendSlice(gpa, path);
    return out.toOwnedSlice(gpa);
}
