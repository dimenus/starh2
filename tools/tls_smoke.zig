//! TLS-edge gate: real curl connections against the conformance server.
//!
//! # Why this exists, and why it is not a Zig test
//!
//! `zig build test` never executes the TLS edge. No test binds a `tls_h2`
//! endpoint, so `tlsHandshakeViaPumps`, `driveDecrypt`, and the TLS branch of
//! `queueWire` are unreachable from the suite. That was proven, not assumed: an
//! always-false assert placed inside `driveDecrypt` left the suite at 126/126
//! green, and aborted the process on the first curl request.
//!
//! # Why curl, and not a Zig client
//!
//! The oracle must share no code with the thing under test. A gate written
//! against the pinned `tls.zig` fork would test that fork against itself, with
//! the same assumptions and the same blind spots. curl brings nghttp2, an
//! independent implementation that is strict about frame ORDER — which is what
//! makes it able to see a defect like a SETTINGS ACK emitted before the server
//! preface. It also pipelines its h2 preface into the TLS handshake flight,
//! which is the client shape both t-538 defects required and no h2c gate could
//! produce.
//!
//! # What each request proves
//!
//! - `/hello` — a fresh TLS handshake, ALPN to h2, and one small response.
//!   Each round is a NEW process, so it is a new connection every time; a
//!   reused connection would skip the handshake this gate exists to exercise.
//! - `/big` — a body far past `outbound_bytes_per_stream` (64 KiB default), so
//!   the response crosses the pending-slab cap and drains through the DRR path
//!   under encryption. This is the shape that found t-482's frame-ordering bug.
//! - The process is checked ALIVE at the end. t-538 killed the server outright,
//!   and a gate that only reads response codes would score a dead server as a
//!   failed request rather than as a crash.
//!
//! # Rules this harness follows
//!
//! - A missing dependency ABORTS; it never skips. A skipped TLS gate reads as a
//!   pass in a build log, which is the failure mode that leaves the edge
//!   untested for months.
//! - Zero connections is an ERROR, not a clean run. A harness that did no work
//!   has produced no result.
//! - The body length is not hardcoded. The first response defines it, every
//!   later round must match it, and it must exceed the 64 KiB cap. A hardcoded
//!   size would silently stop testing the cap crossing if the route changed.
//!
//! No certificate is minted here. `testdata/cert.pem` is used with `curl -k`,
//! which ignores validity, so the gate cannot rot when that certificate
//! expires. This gate therefore proves the TLS DATA path, not certificate
//! verification.
const std = @import("std");

/// The outbound per-stream cap this gate must cross to be meaningful. It
/// mirrors `Limits.outbound_bytes_per_stream`.
const OUTBOUND_CAP_BYTES: usize = 64 * 1024;

const Config = struct {
    bin: []const u8 = "",
    cert: []const u8 = "testdata/cert.pem",
    key: []const u8 = "testdata/key.pem",
    rounds: usize = 8,
    big_rounds: usize = 3,
};

/// The spawned server, so `abort` can stop it. `std.process.exit` does NOT run
/// deferred cleanup, so without this every failing gate run leaked a live
/// server: it kept a port and burned CPU for the rest of the session, and a
/// handful of them made unrelated lifecycle tests flaky. A harness that leaks a
/// process on failure poisons every run after it.
var g_pid: ?std.process.Child.Id = null;

fn abort(comptime fmt: []const u8, args: anytype) noreturn {
    var buf: [1024]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "tls-smoke ABORT: " ++ fmt ++ "\n", args) catch "tls-smoke ABORT\n";
    std.debug.print("{s}", .{msg});
    // SIGKILL the pid directly rather than `Child.kill`, which needs a running
    // io loop this path cannot rely on. The server under test may also be
    // wedged precisely because it cannot shut down gracefully, so asking
    // politely is the one thing that certainly will not work.
    if (g_pid) |pid| {
        g_pid = null;
        std.posix.kill(pid, std.posix.SIG.KILL) catch {};
    }
    std.process.exit(1);
}

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
        if (std.mem.eql(u8, a, "--bin")) {
            cfg.bin = try gpa.dupe(u8, val.next(&args, "--bin"));
        } else if (std.mem.eql(u8, a, "--cert")) {
            cfg.cert = try gpa.dupe(u8, val.next(&args, "--cert"));
        } else if (std.mem.eql(u8, a, "--key")) {
            cfg.key = try gpa.dupe(u8, val.next(&args, "--key"));
        } else if (std.mem.eql(u8, a, "--rounds")) {
            cfg.rounds = std.fmt.parseInt(usize, val.next(&args, "--rounds"), 10) catch
                abort("--rounds needs a number", .{});
        } else if (std.mem.eql(u8, a, "--big-rounds")) {
            cfg.big_rounds = std.fmt.parseInt(usize, val.next(&args, "--big-rounds"), 10) catch
                abort("--big-rounds needs a number", .{});
        }
    }
    if (cfg.bin.len == 0) abort("--bin <conformance-server> is required", .{});
    // A run with no connections proves nothing, so refuse it up front rather
    // than printing a green line for zero work.
    if (cfg.rounds == 0 and cfg.big_rounds == 0) abort("zero rounds requested — that is not a result", .{});
    return cfg;
}

/// The dependency check. It aborts rather than skipping, because a skipped TLS
/// gate is indistinguishable from a passing one in a build log.
fn requireCurlWithHttp2(gpa: std.mem.Allocator, io: std.Io) !void {
    const res = std.process.run(gpa, io, .{ .argv = &.{ "curl", "--version" } }) catch
        abort("curl is not runnable on this machine — the TLS oracle cannot run", .{});
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    if (std.mem.indexOf(u8, res.stdout, "HTTP2") == null) {
        abort("curl reports no HTTP2 feature — the TLS oracle cannot run. Install a curl built with nghttp2.", .{});
    }
}

/// Read the conformance server's one JSON ready line and return the port it
/// bound. The server publishes this only after every endpoint is listening, so
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

/// How long a healthy server may take to stop after SIGTERM. Graceful shutdown
/// is bounded by `graceful_drain_timeout_ns` (30 s default), but every stream
/// here is finished, so a healthy server stops at once. This bound exists to
/// turn a HANG into a failure, so it is generous without being useless.
const SHUTDOWN_BOUND_NS: u64 = 10 * std.time.ns_per_s;

const Shutdown = union(enum) {
    exited: std.process.Child.WaitError!std.process.Child.Term,
    timer: std.Io.Cancelable!void,
};

fn waitChild(child: *std.process.Child, io: std.Io) std.process.Child.WaitError!std.process.Child.Term {
    return child.wait(io);
}

fn waitTimer(timeout: std.Io.Timeout, io: std.Io) std.Io.Cancelable!void {
    return timeout.sleep(io);
}

/// Require the server to STOP, not merely to answer.
///
/// This is the half of the contract that a client-side check cannot see. A
/// defect that strands a handler — a ticket that never completes, a slot never
/// released — leaves every response looking perfect on the wire while the
/// server can no longer shut down, because `shutdownHandlers` waits for every
/// slot to be released. Without this check that whole class is invisible here,
/// and shows up only as a build that hangs, which names nothing.
fn requireCleanShutdown(io: std.Io, child: *std.process.Child, reaped: *bool) void {
    const pid = child.id orelse abort("server was already gone before shutdown was requested", .{});
    std.posix.kill(pid, std.posix.SIG.TERM) catch
        abort("could not signal the server to stop", .{});

    var buf: [2]Shutdown = undefined;
    var select = std.Io.Select(Shutdown).init(io, &buf);
    select.concurrent(.exited, waitChild, .{ child, io }) catch
        abort("could not wait on the server process", .{});
    const timeout: std.Io.Timeout = .{ .duration = .{
        .raw = .fromNanoseconds(@intCast(SHUTDOWN_BOUND_NS)),
        .clock = .awake,
    } };
    select.concurrent(.timer, waitTimer, .{ timeout, io }) catch
        abort("could not arm the shutdown deadline", .{});

    const selected = select.await() catch abort("shutdown wait was canceled", .{});
    // Cancel the losing branch BEFORE anything else. On the timeout path the
    // loser is `waitChild`, still blocked inside `child.wait`, and `abort`
    // kills the child — killing a process another task is waiting on, from a
    // context that still holds the select, deadlocks. Cancel, then abort.
    select.cancelDiscard();
    switch (selected) {
        .exited => |result| {
            const term = result catch abort("waiting on the server failed", .{});
            reaped.* = true;
            switch (term) {
                .exited => |code| if (code != 0) {
                    abort("server exited {d} during shutdown — it did not stop cleanly", .{code});
                },
                else => abort("server did not exit cleanly on SIGTERM: {any}", .{term}),
            }
        },
        .timer => abort(
            "server did not stop within {d}s of SIGTERM — a handler is stranded, so shutdown cannot complete",
            .{SHUTDOWN_BOUND_NS / std.time.ns_per_s},
        ),
    }
}

const Fetch = struct { http_version: []const u8, code: u32, size: usize };

/// One fresh curl process, so one fresh TLS connection and handshake.
fn fetch(gpa: std.mem.Allocator, io: std.Io, port: u16, path: []const u8) !Fetch {
    var url_buf: [128]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "https://127.0.0.1:{d}{s}", .{ port, path });
    const res = try std.process.run(gpa, io, .{ .argv = &.{
        "curl",     "-s",  "-k",  "--http2",
        "--max-time", "30",
        url,
        "-o",       "/dev/null",
        "-w",       "%{http_version} %{http_code} %{size_download}",
    } });
    defer gpa.free(res.stdout);
    defer gpa.free(res.stderr);
    switch (res.term) {
        .exited => |c| if (c != 0) return error.CurlFailed,
        else => return error.CurlFailed,
    }
    var it = std.mem.tokenizeScalar(u8, res.stdout, ' ');
    const ver = it.next() orelse return error.CurlOutputUnparsable;
    const code_s = it.next() orelse return error.CurlOutputUnparsable;
    const size_s = it.next() orelse return error.CurlOutputUnparsable;
    return .{
        .http_version = try gpa.dupe(u8, ver),
        .code = std.fmt.parseInt(u32, code_s, 10) catch return error.CurlOutputUnparsable,
        .size = std.fmt.parseInt(usize, size_s, 10) catch return error.CurlOutputUnparsable,
    };
}

fn expectOk(gpa: std.mem.Allocator, f: Fetch, path: []const u8, round: usize) void {
    defer gpa.free(f.http_version);
    // "2" is curl's rendering of HTTP/2. Anything else means ALPN did not reach
    // h2, which is a protocol failure even when the body arrives.
    if (!std.mem.eql(u8, f.http_version, "2")) {
        abort("{s} round {d}: negotiated HTTP/{s}, expected HTTP/2", .{ path, round, f.http_version });
    }
    if (f.code != 200) abort("{s} round {d}: HTTP {d}, expected 200", .{ path, round, f.code });
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    // Config strings live for the whole run, so an arena frees them in one
    // step and leaves the string-literal defaults untouched. Without it the
    // DebugAllocator prints a leak report at exit, and that noise would sit
    // directly beside the PASS line where a real failure has to be readable.
    var cfg_arena = std.heap.ArenaAllocator.init(gpa);
    defer cfg_arena.deinit();

    const cfg = try parseArgs(cfg_arena.allocator(), init.minimal.args);
    try requireCurlWithHttp2(gpa, io);

    // Bind port 0 and read back the real port, so concurrent runs on one
    // machine cannot collide on a fixed port.
    var child = std.process.spawn(io, .{
        .argv = &.{
            cfg.bin, "--mode", "tls", "--bind", "127.0.0.1:0",
            "--cert", cfg.cert, "--key", cfg.key,
        },
        .stdout = .pipe,
        .stderr = .ignore,
    }) catch abort("could not spawn the conformance server at {s}", .{cfg.bin});
    // `kill` is the fallback for every failure path. It is skipped once the
    // shutdown check has reaped the process, because `wait` has already
    // released the child's resources. `abort` cannot rely on this defer —
    // `std.process.exit` skips it — so it kills through `g_child` instead.
    var reaped = false;
    defer if (!reaped) child.kill(io);
    g_pid = child.id;
    defer g_pid = null;

    const port = try awaitReadyPort(io, &child);
    std.debug.print("tls-smoke: server on 127.0.0.1:{d}, {d} small + {d} big fresh TLS connections\n", .{ port, cfg.rounds, cfg.big_rounds });

    var connections: usize = 0;

    var i: usize = 0;
    while (i < cfg.rounds) : (i += 1) {
        const f = fetch(gpa, io, port, "/hello") catch
            abort("/hello round {d}: curl failed — the server may have died mid-handshake", .{i});
        expectOk(gpa, f, "/hello", i);
        connections += 1;
    }

    // The body length is discovered, never hardcoded, so this keeps testing the
    // cap crossing even if the route's size changes.
    var big_len: ?usize = null;
    i = 0;
    while (i < cfg.big_rounds) : (i += 1) {
        const f = fetch(gpa, io, port, "/big") catch
            abort("/big round {d}: curl failed — a cap-crossing body did not complete", .{i});
        expectOk(gpa, f, "/big", i);
        if (big_len) |want| {
            if (f.size != want) abort("/big round {d}: got {d} bytes, earlier rounds got {d} — a short delivery", .{ i, f.size, want });
        } else {
            if (f.size <= OUTBOUND_CAP_BYTES) {
                abort("/big returned {d} bytes, which does not exceed the {d} byte outbound cap — this gate would prove nothing", .{ f.size, OUTBOUND_CAP_BYTES });
            }
            big_len = f.size;
        }
        connections += 1;
    }

    // The server must still be serving. A crash after the last response would
    // otherwise pass, and killing the process is exactly what t-538 did.
    const alive = fetch(gpa, io, port, "/hello") catch
        abort("the server did not survive: it stopped answering after {d} connections", .{connections});
    expectOk(gpa, alive, "/hello (liveness)", connections);
    connections += 1;

    if (connections == 0) abort("zero connections were made — this is not a result", .{});

    requireCleanShutdown(io, &child, &reaped);

    std.debug.print("tls-smoke PASS: {d} fresh TLS connections, /big={d} bytes, clean shutdown\n", .{ connections, big_len orelse 0 });
}
