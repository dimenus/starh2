//! One-shot HTTP/2 throughput, measured against another implementation.
//!
//! # The rule this harness exists to enforce
//!
//! **h2load is single-threaded by default, and it saturates before a fast
//! server does.** Measured here on a 12-core M3 Pro: an opponent server read
//! 371k req/s at `-t 1`, 466k at `-t 2`, and 483k at `-t 4`. The first number
//! is not a property of that server. It is a property of the client. Nothing
//! in the output says so — the run completes, every request succeeds, and the
//! number looks precise.
//!
//! So this harness never reports a single number. For every arm it runs at
//! `-t` and at `2*t`, and if the faster client produces more than
//! `client_limit_ratio` more requests per second, that arm is reported as
//! CLIENT-LIMITED with a lower bound, not as a result. A benchmark that can
//! silently measure its own client is worse than no benchmark, because it
//! answers.
//!
//! # What it compares, and what it refuses to compare
//!
//! - `starh2 tls` and `starh2 h2c` are the same binary and the same handler.
//!   testdata must be ECDSA P-256 (`tools/README.md`). rsa:2048 made a 100k
//!   run measure an RSA handshake (~17 ms user on the old tls.zig pin), not
//!   the record layer.
//!   The harness prints user vs sys CPU so that cannot hide in one number.
//! - `--opponent <binary>` adds a third arm. It is optional, and its absence
//!   is PRINTED rather than assumed, because a table with a missing row reads
//!   like a complete table.
//! - `--opponent-name` and `--opponent-revision` make that arm attributable.
//!   A number for an unnamed binary at an unknown commit cannot be reproduced.
//! - Every arm must serve the same number of body bytes. The harness reads
//!   each arm's body length first and REFUSES to run when they differ. A
//!   throughput ratio between two different payloads is not a ratio.
//!
//! # Rules
//!
//! - An arm that does not answer 200 aborts the run. It is never skipped.
//! - The starting arm rotates across rounds, so drift and background load reach
//!   every arm rather than consistently favoring one position.
//! - Rounds are reported individually, not only as a mean. A mean hides the
//!   spread, and the spread is how a reader judges whether to trust the gap.
const std = @import("std");
const builtin = @import("builtin");

/// The point at which a rise between `-t` and `-t*2` means the client, not the
/// server, set the number. 10% is well above run-to-run noise here, which sits
/// near 5%.
const client_limit_ratio: f64 = 1.10;

/// Below this fraction of the mean, the doubled-threads run did not measure the
/// server — it failed. Observed: an arm read 3,311 req/s against a mean of
/// 224,682, a 68x collapse, because a leftover process still held the port. The
/// client-limited check above only looks for a RISE, so it called that run
/// "server-bound" and the tool exited 0. A benchmark that reports a broken run
/// as a good one is worse than one that crashes.
const collapse_ratio: f64 = 0.5;

const Config = struct {
    server: []const u8 = "",
    opponent: []const u8 = "",
    opponent_name: []const u8 = "opponent",
    opponent_revision: []const u8 = "",
    opponent_only: bool = false,
    /// Its own default port, so the opponent runs unmodified and unconfigured.
    opponent_url: []const u8 = "https://127.0.0.1:8443/",
    /// Most servers load a certificate relative to their own working
    /// directory, so the opponent gets its own cwd rather than this repo's.
    opponent_cwd: []const u8 = "",
    requests: usize = 100_000,
    connections: usize = 50,
    streams: usize = 10,
    threads: usize = 4,
    rounds: usize = 3,
};

/// Every exit that is not the end of a clean run comes through here, so it
/// is also where spawned servers die. `std.process.exit` runs no defers: an
/// abort that only printed left the servers running, still bound to their
/// ports (the opponent on a fixed one, 18444 in the tls.zig captures), and
/// the next run then probed or benchmarked the leftover instead of its own
/// arm.
fn abort(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("bench: " ++ fmt ++ "\n", args);
    stopSpawned();
    std.process.exit(1);
}

/// Process groups of the servers this harness spawned and has not stopped
/// yet; 0 is an empty slot. Each server runs in its own group (`pgid = 0`
/// at spawn), so stopping the group also stops whatever a wrapper script
/// started: `--opponent` is often a script around the real server, and
/// signalling only the script's pid orphans that server on its port.
var spawned: [3]std.atomic.Value(std.posix.pid_t) = @splat(.init(0));

/// How long a server gets to exit on SIGTERM before it gets SIGKILL.
const stop_grace_ns: u64 = 3 * std.time.ns_per_s;

fn spawnServer(io: std.Io, options: std.process.SpawnOptions) std.process.SpawnError!std.process.Child {
    var opts = options;
    opts.pgid = 0;
    const child = try std.process.spawn(io, opts);
    const pid = child.id orelse return child;
    for (&spawned) |*slot| {
        if (slot.cmpxchgStrong(0, pid, .acq_rel, .acquire) == null) return child;
    }
    unreachable; // three servers at most: tls, h2c, opponent
}

fn untrack(pid: std.posix.pid_t) void {
    for (&spawned) |*slot| _ = slot.cmpxchgStrong(pid, 0, .acq_rel, .acquire);
}

fn signalGroup(pid: std.posix.pid_t, sig: std.posix.SIG) void {
    std.posix.kill(-pid, sig) catch std.posix.kill(pid, sig) catch {};
}

/// Reap `pid` if it has exited. True once it is gone.
fn reaped(pid: std.posix.pid_t) bool {
    var status: c_int = 0;
    const rc = std.posix.system.waitpid(pid, &status, std.posix.W.NOHANG);
    return switch (std.posix.errno(rc)) {
        .SUCCESS => rc != 0,
        .INTR => false,
        else => true, // ECHILD: not ours, or already reaped
    };
}

/// SIGTERM every tracked server group, wait up to `stop_grace_ns` for the
/// leaders to exit, then SIGKILL what is left. The abort path only: it reaps
/// with waitpid directly, behind std.process.Child's back, so nothing may use
/// those Child values afterwards.
fn stopSpawned() void {
    switch (builtin.os.tag) {
        .windows, .wasi => return,
        else => {},
    }
    var pids: [spawned.len]std.posix.pid_t = undefined;
    for (&spawned, &pids) |*slot, *pid| {
        pid.* = slot.swap(0, .acq_rel);
        if (pid.* != 0) signalGroup(pid.*, .TERM);
    }
    var waited_ns: u64 = 0;
    const step_ns: u64 = 20 * std.time.ns_per_ms;
    while (waited_ns < stop_grace_ns) : (waited_ns += step_ns) {
        var live = false;
        for (&pids) |*pid| {
            if (pid.* != 0 and reaped(pid.*)) pid.* = 0;
            if (pid.* != 0) live = true;
        }
        if (!live) break;
        const ts: std.posix.timespec = .{ .sec = 0, .nsec = @intCast(step_ns) };
        _ = std.posix.system.nanosleep(&ts, null);
    }
    for (pids) |pid| {
        if (pid == 0) continue;
        std.debug.print("bench: pid {d} ignored SIGTERM for {d} s; killing its group\n", .{ pid, stop_grace_ns / std.time.ns_per_s });
        signalGroup(pid, .KILL);
        var status: c_int = 0;
        _ = std.posix.system.waitpid(pid, &status, 0);
    }
}

/// Ctrl-C reaches only this process: the servers are in their own groups,
/// out of the terminal's foreground group. Pass the signal on to them, then
/// die of it with the default action.
fn onInterrupt(sig: std.posix.SIG) callconv(.c) void {
    for (&spawned) |*slot| {
        const pid = slot.load(.acquire);
        if (pid != 0) signalGroup(pid, .TERM);
    }
    const dfl: std.posix.Sigaction = .{
        .handler = .{ .handler = std.posix.SIG.DFL },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(sig, &dfl, null);
    std.posix.raise(sig) catch {};
}

fn installInterruptHandlers() void {
    switch (builtin.os.tag) {
        .windows, .wasi => return,
        else => {},
    }
    const action: std.posix.Sigaction = .{
        .handler = .{ .handler = onInterrupt },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.INT, &action, null);
    std.posix.sigaction(.TERM, &action, null);
    std.posix.sigaction(.HUP, &action, null);
}

const Arm = struct {
    name: []const u8,
    url: []const u8,
    child: ?*std.process.Child = null,
    body_bytes: usize = 0,
    /// Per round, at the base thread count.
    rps: [8]f64 = @splat(0),
    p50_us: [8]f64 = @splat(0),
    p99_us: [8]f64 = @splat(0),
    /// One run at double the threads, to catch a client-limited arm.
    rps_double: f64 = 0,
    cpu_seconds: f64 = 0,
    cpu_user_seconds: f64 = 0,
    cpu_sys_seconds: f64 = 0,
    nvcsw: i64 = 0,
    nivcsw: i64 = 0,
    peak_rss_bytes: usize = 0,
    retained_rss_bytes: usize = 0,
};

fn parseArgs(gpa: std.mem.Allocator, process_args: std.process.Args) !Config {
    var cfg: Config = .{};
    var args = try std.process.Args.Iterator.initAllocator(process_args, gpa);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |a| {
        const val = struct {
            fn next(it: *@TypeOf(args), name: []const u8) []const u8 {
                return it.next() orelse abort("{s} needs a value", .{name});
            }
        };
        if (std.mem.eql(u8, a, "--server")) {
            cfg.server = try gpa.dupe(u8, val.next(&args, "--server"));
        } else if (std.mem.eql(u8, a, "--opponent")) {
            cfg.opponent = try gpa.dupe(u8, val.next(&args, "--opponent"));
        } else if (std.mem.eql(u8, a, "--opponent-name")) {
            cfg.opponent_name = try gpa.dupe(u8, val.next(&args, "--opponent-name"));
        } else if (std.mem.eql(u8, a, "--opponent-revision")) {
            cfg.opponent_revision = try gpa.dupe(u8, val.next(&args, "--opponent-revision"));
        } else if (std.mem.eql(u8, a, "--opponent-only")) {
            cfg.opponent_only = true;
        } else if (std.mem.eql(u8, a, "--opponent-url")) {
            cfg.opponent_url = try gpa.dupe(u8, val.next(&args, "--opponent-url"));
        } else if (std.mem.eql(u8, a, "--opponent-cwd")) {
            cfg.opponent_cwd = try gpa.dupe(u8, val.next(&args, "--opponent-cwd"));
        } else if (std.mem.eql(u8, a, "-n")) {
            cfg.requests = std.fmt.parseInt(usize, val.next(&args, "-n"), 10) catch abort("-n needs a number", .{});
        } else if (std.mem.eql(u8, a, "-c")) {
            cfg.connections = std.fmt.parseInt(usize, val.next(&args, "-c"), 10) catch abort("-c needs a number", .{});
        } else if (std.mem.eql(u8, a, "-m")) {
            cfg.streams = std.fmt.parseInt(usize, val.next(&args, "-m"), 10) catch abort("-m needs a number", .{});
        } else if (std.mem.eql(u8, a, "-t")) {
            cfg.threads = std.fmt.parseInt(usize, val.next(&args, "-t"), 10) catch abort("-t needs a number", .{});
        } else if (std.mem.eql(u8, a, "--rounds")) {
            cfg.rounds = std.fmt.parseInt(usize, val.next(&args, "--rounds"), 10) catch abort("--rounds needs a number", .{});
        } else {
            abort("unknown argument {s}", .{a});
        }
    }
    if (cfg.server.len == 0) abort("--server <starh2-bench-server> is required", .{});
    if (cfg.opponent_only and cfg.opponent.len == 0) abort("--opponent-only requires --opponent", .{});
    if (cfg.rounds == 0 or cfg.rounds > 8) abort("--rounds must be 1..8; zero rounds is not a result", .{});
    return cfg;
}

/// Run h2load once and return (req/s, succeeded, failed, body bytes).
const Sample = struct {
    rps: f64,
    succeeded: usize,
    failed: usize,
    data_bytes: usize,
    p50_us: f64,
    p99_us: f64,
};

fn durationUs(text: []const u8) ?f64 {
    const scale: f64, const number = if (std.mem.endsWith(u8, text, "ns"))
        .{ 0.001, text[0 .. text.len - 2] }
    else if (std.mem.endsWith(u8, text, "us"))
        .{ 1.0, text[0 .. text.len - 2] }
    else if (std.mem.endsWith(u8, text, "ms"))
        .{ 1_000.0, text[0 .. text.len - 2] }
    else if (std.mem.endsWith(u8, text, "s"))
        .{ 1_000_000.0, text[0 .. text.len - 1] }
    else
        return null;
    return (std.fmt.parseFloat(f64, number) catch return null) * scale;
}

fn h2load(
    gpa: std.mem.Allocator,
    io: std.Io,
    url: []const u8,
    n: usize,
    c: usize,
    m: usize,
    t: usize,
) !Sample {
    const n_s = try std.fmt.allocPrint(gpa, "{d}", .{n});
    defer gpa.free(n_s);
    const c_s = try std.fmt.allocPrint(gpa, "{d}", .{c});
    defer gpa.free(c_s);
    const m_s = try std.fmt.allocPrint(gpa, "{d}", .{m});
    defer gpa.free(m_s);
    const t_s = try std.fmt.allocPrint(gpa, "{d}", .{t});
    defer gpa.free(t_s);

    const res = std.process.run(gpa, io, .{
        .argv = &.{ "h2load", "-n", n_s, "-c", c_s, "-m", m_s, "-t", t_s, url },
    }) catch abort("cannot run h2load — install nghttp2", .{});
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    switch (res.term) {
        .exited => |code| if (code != 0) abort("h2load exited {d}: {s}", .{ code, res.stderr }),
        else => abort("h2load did not exit normally: {any}", .{res.term}),
    }

    var out: Sample = .{
        .rps = 0,
        .succeeded = 0,
        .failed = 0,
        .data_bytes = 0,
        .p50_us = 0,
        .p99_us = 0,
    };
    var it = std.mem.splitScalar(u8, res.stdout, '\n');
    while (it.next()) |line| {
        if (std.mem.startsWith(u8, line, "finished in")) {
            // "finished in 1.23s, 123456.78 req/s, 1.23MB/s"
            if (std.mem.indexOf(u8, line, " req/s")) |at| {
                const head = line[0..at];
                const start = (std.mem.lastIndexOfScalar(u8, head, ' ') orelse 0) + 1;
                out.rps = std.fmt.parseFloat(f64, head[start..]) catch 0;
            }
        } else if (std.mem.startsWith(u8, line, "requests:")) {
            out.succeeded = fieldBefore(line, " succeeded") orelse 0;
            out.failed = fieldBefore(line, " failed") orelse 0;
        } else if (std.mem.startsWith(u8, line, "traffic:")) {
            // "traffic: 4.13KB (4230) total, 590B (590) headers ..., 1.27KB (1300) data"
            if (std.mem.lastIndexOf(u8, line, ") data")) |close| {
                const head = line[0..close];
                const open = std.mem.lastIndexOfScalar(u8, head, '(') orelse 0;
                out.data_bytes = std.fmt.parseInt(usize, head[open + 1 ..], 10) catch 0;
            }
        } else if (std.mem.startsWith(u8, line, "request")) {
            var fields = std.mem.tokenizeAny(u8, line, " \t");
            if (!std.mem.eql(u8, fields.next() orelse continue, "request")) continue;
            if (!std.mem.eql(u8, fields.next() orelse continue, ":")) continue;
            _ = fields.next(); // min
            _ = fields.next(); // max
            out.p50_us = durationUs(fields.next() orelse continue) orelse 0;
            _ = fields.next(); // p95
            out.p99_us = durationUs(fields.next() orelse continue) orelse 0;
        }
    }
    if (out.p50_us == 0 or out.p99_us == 0) {
        abort("could not parse h2load request percentiles", .{});
    }
    return out;
}

/// The integer immediately before `suffix` on this line.
fn fieldBefore(line: []const u8, suffix: []const u8) ?usize {
    const at = std.mem.indexOf(u8, line, suffix) orelse return null;
    const head = line[0..at];
    const start = (std.mem.lastIndexOfScalar(u8, head, ' ') orelse return null) + 1;
    return std.fmt.parseInt(usize, head[start..], 10) catch null;
}

fn mean(values: []const f64) f64 {
    var sum: f64 = 0;
    for (values) |v| sum += v;
    return sum / @as(f64, @floatFromInt(values.len));
}

fn timevalSeconds(tv: anytype) f64 {
    return @as(f64, @floatFromInt(tv.sec)) +
        @as(f64, @floatFromInt(tv.usec)) / 1_000_000.0;
}

fn childRusage(child: *const std.process.Child, arm: *Arm) void {
    const ru = switch (builtin.os.tag) {
        .dragonfly,
        .freebsd,
        .netbsd,
        .openbsd,
        .illumos,
        .linux,
        .serenity,
        .driverkit,
        .ios,
        .maccatalyst,
        .macos,
        .tvos,
        .visionos,
        .watchos,
        => child.resource_usage_statistics.rusage,
        else => null,
    };
    const usage = ru orelse return;
    arm.cpu_user_seconds = timevalSeconds(usage.utime);
    arm.cpu_sys_seconds = timevalSeconds(usage.stime);
    arm.cpu_seconds = arm.cpu_user_seconds + arm.cpu_sys_seconds;
    arm.nvcsw = @intCast(usage.nvcsw);
    arm.nivcsw = @intCast(usage.nivcsw);
}

fn currentRssBytes(gpa: std.mem.Allocator, io: std.Io, child: *const std.process.Child) ?usize {
    switch (builtin.os.tag) {
        .windows, .wasi => return null,
        else => {},
    }
    const pid = child.id orelse return null;
    const pid_s = std.fmt.allocPrint(gpa, "{d}", .{pid}) catch return null;
    defer gpa.free(pid_s);
    const res = std.process.run(gpa, io, .{
        .argv = &.{ "ps", "-o", "rss=", "-p", pid_s },
    }) catch return null;
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    if (res.term != .exited or res.term.exited != 0) return null;
    const kb = std.fmt.parseInt(usize, std.mem.trim(u8, res.stdout, " \t\r\n"), 10) catch return null;
    return kb * 1024;
}

/// Read the bench server's one JSON ready line and return the port it bound.
/// The server publishes this only after every endpoint is listening, so
/// reading it is what removes the need for a sleep here.
fn awaitReadyPort(io: std.Io, child: *std.process.Child) !u16 {
    const out = child.stdout orelse abort("server was spawned without a stdout pipe", .{});
    var buf: [4096]u8 = undefined;
    var used: usize = 0;
    while (used < buf.len) {
        var dest: [1][]u8 = .{buf[used..]};
        const n = out.readStreaming(io, &dest) catch
            abort("server closed stdout before printing its ready line", .{});
        if (n == 0) abort("server exited before printing its ready line", .{});
        used += n;
        const line_end = std.mem.indexOfScalar(u8, buf[0..used], '\n') orelse continue;
        const line = buf[0..line_end];
        const key = "\"port\":";
        const at = std.mem.indexOf(u8, line, key) orelse
            abort("ready line has no port field: {s}", .{line});
        var i = at + key.len;
        var port: u32 = 0;
        var digits: usize = 0;
        while (i < line.len and line[i] >= '0' and line[i] <= '9') : (i += 1) {
            port = port * 10 + (line[i] - '0');
            digits += 1;
        }
        if (digits == 0 or port == 0 or port > 65535) abort("ready line has a bad port: {s}", .{line});
        return @intCast(port);
    }
    abort("server ready line exceeded {d} bytes", .{buf.len});
}

/// Clean-run shutdown: SIGTERM the server's group and wait for the leader,
/// so its rusage is collected. Signalling the group, not the pid, also stops
/// a server that a wrapper script started.
fn stopAndCollect(child: *std.process.Child, io: std.Io) void {
    switch (builtin.os.tag) {
        .windows, .wasi => {
            child.kill(io);
            return;
        },
        else => {},
    }
    const pid = child.id orelse return;
    untrack(pid);
    signalGroup(pid, .TERM);
    _ = child.wait(io) catch {
        signalGroup(pid, .KILL);
        child.kill(io);
        return;
    };
}

/// The opponent's address, from `--opponent-url`. Only IP literals and
/// `localhost`: the harness runs every arm on this machine.
fn urlAddress(url: []const u8) ?std.Io.net.IpAddress {
    const after_scheme = if (std.mem.indexOf(u8, url, "://")) |i| url[i + 3 ..] else url;
    const authority = after_scheme[0 .. std.mem.indexOfScalar(u8, after_scheme, '/') orelse after_scheme.len];
    const colon = std.mem.lastIndexOfScalar(u8, authority, ':') orelse return null;
    var host = authority[0..colon];
    const port = std.fmt.parseInt(u16, authority[colon + 1 ..], 10) catch return null;
    if (std.mem.eql(u8, host, "localhost")) host = "127.0.0.1";
    if (host.len >= 2 and host[0] == '[' and host[host.len - 1] == ']') host = host[1 .. host.len - 1];
    return std.Io.net.IpAddress.parse(host, port) catch null;
}

fn accepts(io: std.Io, addr: std.Io.net.IpAddress) bool {
    const stream = addr.connect(io, .{ .mode = .stream }) catch return false;
    stream.close(io);
    return true;
}

/// How long the opponent gets to start accepting connections.
const opponent_listen_timeout_ns: u64 = 15 * std.time.ns_per_s;

/// The opponent prints no ready line, so the harness used to probe it the
/// moment it was spawned and abort on h2load's empty output ("could not
/// parse h2load request percentiles"). Wait until its port accepts a TCP
/// connection instead, bounded, and fail by name if it never does.
fn awaitOpponentListening(io: std.Io, addr: std.Io.net.IpAddress, url: []const u8) !void {
    var waited_ns: u64 = 0;
    const step_ns: u64 = 50 * std.time.ns_per_ms;
    while (!accepts(io, addr)) : (waited_ns += step_ns) {
        if (waited_ns >= opponent_listen_timeout_ns) {
            abort("the opponent never accepted a connection at {s} within {d} s", .{ url, opponent_listen_timeout_ns / std.time.ns_per_s });
        }
        try io.sleep(.fromNanoseconds(step_ns), .awake);
    }
}

pub fn main(init: std.process.Init) void {
    installInterruptHandlers();
    run(init) catch |err| abort("{s}", .{@errorName(err)});
    // Anything still tracked (the starh2 arms under --opponent-only).
    stopSpawned();
}

fn run(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const cfg = try parseArgs(arena, init.minimal.args);

    // Before anything is spawned: a server left on the opponent's port by
    // an earlier run would answer every probe in place of the real opponent.
    const opponent_addr: ?std.Io.net.IpAddress = if (cfg.opponent.len == 0) null else urlAddress(cfg.opponent_url) orelse
        abort("--opponent-url {s} needs an IP literal or localhost, and a port", .{cfg.opponent_url});
    if (opponent_addr) |addr| {
        if (accepts(io, addr)) abort("something already accepts connections at {s}, before the opponent was started; a server left by an earlier run?", .{cfg.opponent_url});
    }

    var tls_child = spawnServer(io, .{
        .argv = &.{ cfg.server, "--mode", "tls", "--port", "0" },
        .stdout = .pipe,
        .stderr = .ignore,
        .request_resource_usage_statistics = true,
    }) catch abort("cannot spawn {s}", .{cfg.server});
    const tls_port = try awaitReadyPort(io, &tls_child);
    const tls_url = try std.fmt.allocPrint(arena, "https://127.0.0.1:{d}/", .{tls_port});

    var h2c_child = spawnServer(io, .{
        .argv = &.{ cfg.server, "--mode", "h2c", "--port", "0" },
        .stdout = .pipe,
        .stderr = .ignore,
        .request_resource_usage_statistics = true,
    }) catch abort("cannot spawn {s}", .{cfg.server});
    const h2c_port = try awaitReadyPort(io, &h2c_child);
    const h2c_url = try std.fmt.allocPrint(arena, "http://127.0.0.1:{d}/", .{h2c_port});

    var opponent_child: ?std.process.Child = null;
    if (cfg.opponent.len != 0) {
        opponent_child = spawnServer(io, .{
            .argv = &.{cfg.opponent},
            .cwd = if (cfg.opponent_cwd.len != 0) .{ .path = cfg.opponent_cwd } else .inherit,
            .stdout = .ignore,
            .stderr = .ignore,
            .request_resource_usage_statistics = true,
        }) catch abort("cannot spawn the opponent at {s}", .{cfg.opponent});
        try awaitOpponentListening(io, opponent_addr.?, cfg.opponent_url);
    }

    var arms: std.ArrayList(Arm) = .empty;
    if (!cfg.opponent_only) {
        try arms.append(arena, .{ .name = "starh2 tls", .url = tls_url, .child = &tls_child });
        try arms.append(arena, .{ .name = "starh2 h2c", .url = h2c_url, .child = &h2c_child });
    }
    if (cfg.opponent.len != 0) {
        try arms.append(arena, .{
            .name = cfg.opponent_name,
            .url = cfg.opponent_url,
            .child = if (opponent_child) |*child| child else unreachable,
        });
        if (cfg.opponent_revision.len != 0) {
            std.debug.print("bench: opponent {s} revision {s}\n", .{
                cfg.opponent_name,
                cfg.opponent_revision,
            });
        }
    } else {
        std.debug.print("bench: NO OPPONENT. Only the starh2 arms ran; pass --opponent <binary>.\n", .{});
    }

    // Reachability and equal payload, before any number is taken.
    for (arms.items) |*arm| {
        const probe = try h2load(arena, io, arm.url, 1, 1, 1, 1);
        if (probe.succeeded != 1) abort("{s} did not answer 200 at {s}", .{ arm.name, arm.url });
        arm.body_bytes = probe.data_bytes;
    }
    for (arms.items[1..]) |arm| {
        if (arm.body_bytes != arms.items[0].body_bytes) {
            abort(
                "arms serve different payloads ({s}={d}B, {s}={d}B). A ratio between unequal bodies is not a ratio.",
                .{ arms.items[0].name, arms.items[0].body_bytes, arm.name, arm.body_bytes },
            );
        }
    }
    std.debug.print(
        "bench: n={d} c={d} m={d} t={d}, {d} rounds, body={d}B on every arm\n",
        .{ cfg.requests, cfg.connections, cfg.streams, cfg.threads, cfg.rounds, arms.items[0].body_bytes },
    );

    var round: usize = 0;
    while (round < cfg.rounds) : (round += 1) {
        // Rotate the first arm each round. With three arms and three rounds,
        // each server runs first, middle, and last once, so thermal/background
        // drift cannot consistently favor one implementation.
        for (0..arms.items.len) |offset| {
            const arm = &arms.items[(round + offset) % arms.items.len];
            const s = try h2load(arena, io, arm.url, cfg.requests, cfg.connections, cfg.streams, cfg.threads);
            if (s.failed != 0) abort("{s} failed {d} requests — the number is meaningless", .{ arm.name, s.failed });
            arm.rps[round] = s.rps;
            arm.p50_us[round] = s.p50_us;
            arm.p99_us[round] = s.p99_us;
            std.debug.print("  round {d}  {s}  {d:.0} req/s  p50={d:.1}us p99={d:.1}us\n", .{
                round + 1,
                arm.name,
                s.rps,
                s.p50_us,
                s.p99_us,
            });
        }
    }

    // The client-limited check, once per arm, at double the client threads.
    for (arms.items) |*arm| {
        const s = try h2load(arena, io, arm.url, cfg.requests, cfg.connections, cfg.streams, cfg.threads * 2);
        arm.rps_double = s.rps;
    }

    for (arms.items) |*arm| {
        const child = arm.child.?;
        arm.retained_rss_bytes = currentRssBytes(arena, io, child) orelse 0;
        stopAndCollect(child, io);
        childRusage(child, arm);
        arm.peak_rss_bytes = child.resource_usage_statistics.getMaxRss() orelse 0;
    }

    const requests_per_arm = cfg.requests * (cfg.rounds + 1) + 1;
    std.debug.print("\n  {s:<14} {s:>12} {s:>14} {s:>11} {s:>10} {s:>10} {s:>10}\n", .{
        "arm",
        "mean req/s",
        "at 2x threads",
        "CPU/req",
        "CPU total",
        "peak RSS",
        "verdict",
    });
    var limited_any = false;
    var collapsed_any = false;
    for (arms.items) |arm| {
        const m = mean(arm.rps[0..cfg.rounds]);
        const limited = arm.rps_double > m * client_limit_ratio;
        const collapsed = arm.rps_double < m * collapse_ratio;
        const cpu_ns_per_request = arm.cpu_seconds * 1_000_000_000.0 /
            @as(f64, @floatFromInt(requests_per_arm));
        if (limited) limited_any = true;
        if (collapsed) collapsed_any = true;
        std.debug.print("  {s:<14} {d:>12.0} {d:>14.0} {d:>8.0}ns {d:>8.2}s {d:>7.1}MiB {s:>10}\n", .{
            arm.name,
            m,
            arm.rps_double,
            cpu_ns_per_request,
            arm.cpu_seconds,
            @as(f64, @floatFromInt(arm.peak_rss_bytes)) / (1024.0 * 1024.0),
            if (collapsed) "BROKEN RUN" else if (limited) "CLIENT-LIMITED" else "server-bound",
        });
        std.debug.print("    CPU user={d:.2}s ({d:.0}ns/req) sys={d:.2}s ({d:.0}ns/req) nvcsw={d} nivcsw={d}\n", .{
            arm.cpu_user_seconds,
            arm.cpu_user_seconds * 1_000_000_000.0 / @as(f64, @floatFromInt(requests_per_arm)),
            arm.cpu_sys_seconds,
            arm.cpu_sys_seconds * 1_000_000_000.0 / @as(f64, @floatFromInt(requests_per_arm)),
            arm.nvcsw,
            arm.nivcsw,
        });
        std.debug.print("    retained RSS before shutdown: {d:.1}MiB\n", .{
            @as(f64, @floatFromInt(arm.retained_rss_bytes)) / (1024.0 * 1024.0),
        });
    }

    if (collapsed_any) {
        std.debug.print(
            "\nbench: an arm fell below {d:.0}% of its own mean when h2load got twice the\n" ++
                "threads. That is not a slower server, it is a run that did not happen —\n" ++
                "a port still held by a previous process is the usual cause. Re-run.\n",
            .{collapse_ratio * 100},
        );
        std.process.exit(3);
    }
    if (limited_any) {
        std.debug.print(
            "\nbench: at least one arm rose more than {d:.0}% when h2load got twice the threads.\n" ++
                "That arm's mean is a LOWER BOUND on the server, not a measurement of it.\n" ++
                "Raise -t until the arm reports server-bound before quoting its number.\n",
            .{(client_limit_ratio - 1) * 100},
        );
        std.process.exit(2);
    }
    std.debug.print("\nbench: every arm is server-bound at t={d}.\n", .{cfg.threads});
}
