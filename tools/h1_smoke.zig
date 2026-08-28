//! HTTP/1.1 edge gate: curl against `tls` (ALPN fallback) and `h1c`.
//!
//! The oracle is curl because it shares no code with this stack. A missing
//! curl ABORTS. Zero connections is an error. SIGTERM with an open SSE stream
//! must stop the server.
const std = @import("std");

var g_pid: ?std.process.Child.Id = null;

fn abort(comptime fmt: []const u8, args: anytype) noreturn {
    var buf: [1024]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "h1-smoke ABORT: " ++ fmt ++ "\n", args) catch "h1-smoke ABORT\n";
    std.debug.print("{s}", .{msg});
    if (g_pid) |pid| {
        g_pid = null;
        std.posix.kill(pid, std.posix.SIG.KILL) catch {};
    }
    std.process.exit(1);
}

fn requireCurl(gpa: std.mem.Allocator, io: std.Io) !void {
    const res = std.process.run(gpa, io, .{ .argv = &.{ "curl", "--version" } }) catch
        abort("curl is not runnable on this machine — the H1 oracle cannot run", .{});
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    switch (res.term) {
        .exited => |c| if (c != 0) abort("curl --version failed", .{}),
        else => abort("curl --version failed", .{}),
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

fn curl(gpa: std.mem.Allocator, io: std.Io, argv: []const []const u8) !std.process.RunResult {
    return std.process.run(gpa, io, .{ .argv = argv }) catch
        abort("curl failed to start", .{});
}

/// Two URLs, one curl process. `%{num_connects}` is per transfer: the first
/// request is `n=1`, a reused connection is `n=0`. A server that closes after
/// the first response makes the second transfer `n=1` as well.
fn assertCurlReuse(stdout: []const u8, site: []const u8) void {
    if (std.mem.indexOf(u8, stdout, "n=1") == null) {
        abort("{s} keep-alive first transfer did not connect: {s}", .{ site, stdout });
    }
    if (std.mem.indexOf(u8, stdout, "n=0") == null) {
        abort("{s} keep-alive second transfer opened a new connection: {s}", .{ site, stdout });
    }
    if (std.mem.indexOf(u8, stdout, "c=200") == null) {
        abort("{s} keep-alive did not return 200: {s}", .{ site, stdout });
    }
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
    try requireCurl(gpa, io);

    var bin: []const u8 = "";
    var cert: []const u8 = "testdata/cert.pem";
    var key: []const u8 = "testdata/key.pem";
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |a| {
        if (std.mem.eql(u8, a, "--bin")) {
            bin = args.next() orelse abort("--bin needs a value", .{});
        } else if (std.mem.eql(u8, a, "--cert")) {
            cert = args.next() orelse abort("--cert needs a value", .{});
        } else if (std.mem.eql(u8, a, "--key")) {
            key = args.next() orelse abort("--key needs a value", .{});
        }
    }
    if (bin.len == 0) abort("--bin <conformance-server> is required", .{});

    var connections: usize = 0;

    // --- tls endpoint, curl --http1.1 (ALPN fallback) ---
    {
        var child = std.process.spawn(io, .{
            .argv = &.{ bin, "--mode", "tls", "--bind", "127.0.0.1:0", "--cert", cert, "--key", key },
            .stdout = .pipe,
            .stderr = .ignore,
        }) catch abort("could not spawn tls conformance server", .{});
        var reaped = false;
        defer if (!reaped) child.kill(io);
        g_pid = child.id;
        const port = try awaitReadyPort(io, &child);
        std.debug.print("h1-smoke: tls on 127.0.0.1:{d}\n", .{port});
        var url_buf: [128]u8 = undefined;
        const url = std.fmt.bufPrint(&url_buf, "https://127.0.0.1:{d}/hello", .{port}) catch abort("url", .{});
        const res = try curl(gpa, io, &.{ "curl", "-sk", "--http1.1", "--max-time", "5", "-w", "%{http_code} %{http_version}", "-o", "/dev/null", url });
        defer gpa.free(res.stdout);
        defer gpa.free(res.stderr);
        if (std.mem.indexOf(u8, res.stdout, "200") == null) abort("tls --http1.1: {s}", .{res.stdout});
        if (std.mem.indexOf(u8, res.stdout, "1.1") == null) abort("tls did not negotiate HTTP/1.1: {s}", .{res.stdout});
        connections += 1;
        std.debug.print("h1-smoke: tls --http1.1 /hello 200\n", .{});

        const ka_tls = try curl(gpa, io, &.{
            "curl", "-sk", "--http1.1",
            "-o", "/dev/null", "-o", "/dev/null",
            "-w", "n=%{num_connects} c=%{http_code}\n", url, url,
        });
        defer gpa.free(ka_tls.stdout);
        defer gpa.free(ka_tls.stderr);
        std.debug.print("h1-smoke: tls keep-alive curl {s}\n", .{ka_tls.stdout});
        assertCurlReuse(ka_tls.stdout, "tls");
        connections += 2;

        var once_url_buf: [128]u8 = undefined;
        const once_url = std.fmt.bufPrint(&once_url_buf, "https://127.0.0.1:{d}/h1-once", .{port}) catch abort("url", .{});
        const once = try curl(gpa, io, &.{ "curl", "-sk", "--http1.1", "--max-time", "3", once_url });
        defer gpa.free(once.stdout);
        defer gpa.free(once.stderr);
        switch (once.term) {
            .exited => |c| if (c != 0) abort("tls /h1-once curl exit {d} stdout={s}", .{ c, once.stdout }),
            else => abort("tls /h1-once curl term={any}", .{once.term}),
        }
        if (std.mem.indexOf(u8, once.stdout, "chunk-ok") == null) {
            abort("tls /h1-once did not complete: stdout={s}", .{once.stdout});
        }

        // SIGTERM with SSE open.
        var sse_url_buf: [128]u8 = undefined;
        const sse_url = std.fmt.bufPrint(&sse_url_buf, "https://127.0.0.1:{d}/h1-sse", .{port}) catch abort("url", .{});
        var sse = std.process.spawn(io, .{
            .argv = &.{ "curl", "-sk", "--http1.1", "-N", "--max-time", "8", sse_url },
            .stdout = .pipe,
            .stderr = .ignore,
        }) catch abort("could not spawn curl -N", .{});
        // Wait briefly for an event.
        io.sleep(.fromMilliseconds(500), .awake) catch {};
        const pid = child.id orelse abort("server was already gone before SIGTERM", .{});
        std.posix.kill(pid, std.posix.SIG.TERM) catch abort("SIGTERM failed", .{});
        const term = waitChildBounded(&child, io, 10 * std.time.ns_per_s) catch abort("wait after SIGTERM failed", .{});
        reaped = true;
        g_pid = null;
        if (sse.id != null) sse.kill(io);
        switch (term) {
            .exited => {},
            .signal => |s| {
                if (s == std.posix.SIG.KILL) abort("server hung >10s after SIGTERM with /h1-sse open", .{});
            },
            else => abort("server did not exit after SIGTERM with SSE open: {any}", .{term}),
        }
    }

    // --- h1c endpoint ---
    {
        var child = std.process.spawn(io, .{
            .argv = &.{ bin, "--mode", "h1c", "--bind", "127.0.0.1:0" },
            .stdout = .pipe,
            .stderr = .ignore,
        }) catch abort("could not spawn h1c conformance server", .{});
        var reaped = false;
        defer if (!reaped) child.kill(io);
        g_pid = child.id;
        const port = try awaitReadyPort(io, &child);
        std.debug.print("h1-smoke: h1c on 127.0.0.1:{d}\n", .{port});
        var url_buf: [128]u8 = undefined;
        const url = std.fmt.bufPrint(&url_buf, "http://127.0.0.1:{d}/hello", .{port}) catch abort("url", .{});
        const res = try curl(gpa, io, &.{ "curl", "-s", "-w", "%{http_code}", "-o", "/dev/null", url });
        defer gpa.free(res.stdout);
        defer gpa.free(res.stderr);
        if (std.mem.indexOf(u8, res.stdout, "200") == null) abort("h1c /hello: {s}", .{res.stdout});
        connections += 1;

        // Two URLs in one invocation: keep-alive reuse.
        var url2_buf: [128]u8 = undefined;
        const url2 = std.fmt.bufPrint(&url2_buf, "http://127.0.0.1:{d}/hello", .{port}) catch abort("url", .{});
        const ka = try curl(gpa, io, &.{
            "curl", "-s",
            "-o", "/dev/null", "-o", "/dev/null",
            "-w", "n=%{num_connects} c=%{http_code}\n", url, url2,
        });
        defer gpa.free(ka.stdout);
        defer gpa.free(ka.stderr);
        std.debug.print("h1-smoke: keep-alive curl {s}\n", .{ka.stdout});
        assertCurlReuse(ka.stdout, "h1c");
        connections += 2;

        // SSE event within 2s.
        var sse_url_buf: [128]u8 = undefined;
        const sse_url = std.fmt.bufPrint(&sse_url_buf, "http://127.0.0.1:{d}/h1-sse", .{port}) catch abort("url", .{});
        const sse = try curl(gpa, io, &.{ "curl", "-s", "-N", "--max-time", "2", sse_url });
        defer gpa.free(sse.stdout);
        defer gpa.free(sse.stderr);
        if (std.mem.indexOf(u8, sse.stdout, "data: ping") == null) {
            abort("h1c SSE did not deliver an event within 2s: {s}", .{sse.stdout});
        }

        var once_url_buf: [128]u8 = undefined;
        const once_url = std.fmt.bufPrint(&once_url_buf, "http://127.0.0.1:{d}/h1-once", .{port}) catch abort("url", .{});
        const once = try curl(gpa, io, &.{ "curl", "-s", "--max-time", "3", once_url });
        defer gpa.free(once.stdout);
        defer gpa.free(once.stderr);
        switch (once.term) {
            .exited => |c| if (c != 0) abort("h1c /h1-once curl exit {d} stdout={s}", .{ c, once.stdout }),
            else => abort("h1c /h1-once curl term={any}", .{once.term}),
        }
        if (std.mem.indexOf(u8, once.stdout, "chunk-ok") == null) {
            abort("h1c /h1-once did not complete: stdout={s}", .{once.stdout});
        }

        if (child.id) |pid| std.posix.kill(pid, std.posix.SIG.TERM) catch {};
        _ = waitChildBounded(&child, io, 10 * std.time.ns_per_s) catch {};
        reaped = true;
        g_pid = null;
    }

    if (connections == 0) abort("zero connections — that is not a result", .{});
    std.debug.print("h1-smoke PASS connections={d}\n", .{connections});
}
