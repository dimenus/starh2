//! HTTP/1.1 Go net/http oracle: keep-alive reuse and Expect 100-continue.
//!
//! The oracle is Go's stdlib client because it shares no parser with this
//! stack or with curl. A missing `go` ABORTS. A skip is not a pass.
const std = @import("std");

var g_pid: ?std.process.Child.Id = null;

fn abort(comptime fmt: []const u8, args: anytype) noreturn {
    var buf: [1024]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "h1-go-smoke ABORT: " ++ fmt ++ "\n", args) catch "h1-go-smoke ABORT\n";
    std.debug.print("{s}", .{msg});
    if (g_pid) |pid| {
        g_pid = null;
        std.posix.kill(pid, std.posix.SIG.KILL) catch {};
    }
    std.process.exit(1);
}

fn requireGo(gpa: std.mem.Allocator, io: std.Io) !void {
    const res = std.process.run(gpa, io, .{ .argv = &.{ "go", "version" } }) catch
        abort("go is not runnable on this machine — the H1 Go oracle cannot run", .{});
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    switch (res.term) {
        .exited => |c| if (c != 0) abort("go version failed", .{}),
        else => abort("go version failed", .{}),
    }
}

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
        if (digits == 0 or port > 65535) abort("ready line port unreadable: {s}", .{line});
        return @intCast(port);
    }
    abort("ready line did not arrive", .{});
}

fn waitChildBounded(child: *std.process.Child, io: std.Io, timeout_ns: u64) !std.process.Child.Term {
    const pid = child.id orelse return error.AlreadyGone;
    const Wd = struct {
        fn run(p: std.process.Child.Id, inner: std.Io, ns: u64) void {
            inner.sleep(.fromNanoseconds(ns), .awake) catch return;
            std.posix.kill(p, std.posix.SIG.KILL) catch {};
        }
    };
    var wd = io.concurrent(Wd.run, .{ pid, io, timeout_ns }) catch
        abort("watchdog spawn failed — unbounded wait is not a result", .{});
    const term = try child.wait(io);
    wd.cancel(io);
    return term;
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;
    try requireGo(gpa, io);

    var bin: []const u8 = "";
    var go_src: []const u8 = "tools/h1_go_client.go";
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |a| {
        if (std.mem.eql(u8, a, "--bin")) {
            bin = args.next() orelse abort("--bin needs a value", .{});
        } else if (std.mem.eql(u8, a, "--go")) {
            go_src = args.next() orelse abort("--go needs a value", .{});
        }
    }
    if (bin.len == 0) abort("--bin <conformance-server> is required", .{});

    var child = std.process.spawn(io, .{
        .argv = &.{ bin, "--mode", "h1c", "--bind", "127.0.0.1:0" },
        .stdout = .pipe,
        .stderr = .ignore,
    }) catch abort("could not spawn h1c conformance server", .{});
    var reaped = false;
    defer if (!reaped) child.kill(io);
    g_pid = child.id;
    const port = try awaitReadyPort(io, &child);
    std.debug.print("h1-go-smoke: h1c on 127.0.0.1:{d}\n", .{port});

    var base_buf: [64]u8 = undefined;
    const base = std.fmt.bufPrint(&base_buf, "http://127.0.0.1:{d}", .{port}) catch abort("url", .{});

    var go = std.process.spawn(io, .{
        .argv = &.{ "go", "run", go_src, "--base", base },
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch abort("go run failed to start", .{});
    const go_term = waitChildBounded(&go, io, 30 * std.time.ns_per_s) catch abort("go run wait failed", .{});
    switch (go_term) {
        .exited => |c| if (c != 0) abort("go client exit {d}", .{c}),
        .signal => |s| {
            if (s == std.posix.SIG.KILL) abort("go client hung >30s", .{});
            abort("go client signal {any}", .{s});
        },
        else => abort("go client term {any}", .{go_term}),
    }

    if (child.id) |pid| std.posix.kill(pid, std.posix.SIG.TERM) catch {};
    _ = waitChildBounded(&child, io, 10 * std.time.ns_per_s) catch {};
    reaped = true;
    g_pid = null;
}
