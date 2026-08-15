//! The starh2 arm of the one-shot throughput benchmark.
//!
//! It is deliberately the smallest server this stack can express: one route,
//! one fixed body, no formatting, no per-request allocation. The conformance
//! server is the wrong arm for a benchmark, because it carries six routes, a
//! nonce format on every hello, and compression enabled — measuring it against
//! another project's purpose-built benchmark binary would compare harnesses,
//! not stacks.
//!
//! The body is `Hello, World!` because that is byte-for-byte what
//! hendriknielaender/http2.zig serves from `/` in its own benchmark server. A
//! throughput number is not comparable unless the bytes are.
//!
//! `--mode h2c` runs the identical handler, router and framing path with the
//! TLS record layer removed. That arm exists to answer one question with a
//! measurement instead of a hypothesis: how much of the number is crypto?
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

const dummy: u8 = 0;

const BODY = "Hello, World!";

fn helloHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/plain" }}, BODY);
}

/// The SSE arm: one event every `--sse-interval-ms`, each carrying the
/// monotonic nanosecond count at the moment the server wrote it.
///
/// The timestamp is the whole point. A client that only counts events measures
/// throughput, and throughput is not the property that matters for a long-lived
/// stream — delivery latency under many concurrent streams is. Both ends run on
/// one machine and read the same clock, so the subtraction is valid.
///
/// The stream runs forever. The client closes it, because a server-side event
/// budget would end the measurement early on exactly the slow streams that the
/// benchmark is looking for.
var g_sse_interval_ms: u64 = 100;

fn sseHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    var buf: [64]u8 = undefined;

    // Sleep to a DEADLINE, never for a duration. `sleep(interval)` in a loop
    // makes the period interval plus the work, so the emitted rate drifts below
    // the requested rate and the arm looks like it dropped events when it only
    // ticked slowly. That is a defect in a benchmark, not a measurement of the
    // server. Go's time.Ticker keeps a schedule, so the arms must match.
    const interval_ns: i128 = @as(i128, @intCast(g_sse_interval_ms)) * std.time.ns_per_ms;
    var next: i128 = zio.Timestamp.now(.realtime).toNanoseconds() + interval_ns;
    while (true) {
        const now_ns: i128 = zio.Timestamp.now(.realtime).toNanoseconds();
        if (next > now_ns) {
            zio.sleep(.fromNanoseconds(@intCast(next - now_ns))) catch |err| {
                if (err == error.Canceled) return error.Canceled;
                return err;
            };
        }
        next += interval_ns;
        const sent: i128 = zio.Timestamp.now(.realtime).toNanoseconds();
        const ev = try std.fmt.bufPrint(&buf, "data: {d}\n\n", .{sent});
        body.writeAll(ev) catch break;
        try zio.maybeYield();
    }
    try body.finish();
}

// The broadcast arm. One ticker wakes every stream, instead of every stream
// holding its own timer.
//
// This exists to separate two explanations of the per-connection ceiling that
// the profile cannot tell apart: the connection's serialized write path, or the
// benchmark's own shape of one armed timer per stream. A real hypermedia app
// pushes when data changes, which is this shape, not a per-viewer tick.
//
// If this arm lifts the per-connection number, the timers were the cost. If it
// does not move, the serialization is.
var g_io: std.Io = undefined;
var g_bc_mutex: std.Io.Mutex = .init;
var g_bc_cond: std.Io.Condition = .init;
var g_bc_seq: u64 = 0;
var g_bc_ts: i128 = 0;

fn tickerTask() !void {
    const interval_ns: i128 = @as(i128, @intCast(g_sse_interval_ms)) * std.time.ns_per_ms;
    var next: i128 = zio.Timestamp.now(.realtime).toNanoseconds() + interval_ns;
    while (true) {
        const now_ns: i128 = zio.Timestamp.now(.realtime).toNanoseconds();
        if (next > now_ns) {
            try zio.sleep(.fromNanoseconds(@intCast(next - now_ns)));
        }
        next += interval_ns;
        try g_bc_mutex.lock(g_io);
        g_bc_seq += 1;
        g_bc_ts = zio.Timestamp.now(.realtime).toNanoseconds();
        g_bc_mutex.unlock(g_io);
        g_bc_cond.broadcast(g_io);
    }
}

fn sseBroadcastHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    var buf: [64]u8 = undefined;

    try g_bc_mutex.lock(g_io);
    var seen: u64 = g_bc_seq;
    g_bc_mutex.unlock(g_io);

    while (true) {
        try g_bc_mutex.lock(g_io);
        while (g_bc_seq == seen) {
            g_bc_cond.wait(g_io, &g_bc_mutex) catch {
                g_bc_mutex.unlock(g_io);
                return error.Canceled;
            };
        }
        seen = g_bc_seq;
        const ts = g_bc_ts;
        g_bc_mutex.unlock(g_io);

        const ev = try std.fmt.bufPrint(&buf, "data: {d}\n\n", .{ts});
        body.writeAll(ev) catch break;
        try zio.maybeYield();
    }
    try body.finish();
}

const Args = struct {
    port: u16 = 0,
    tls: bool = true,
    cert: []const u8 = "testdata/cert.pem",
    key: []const u8 = "testdata/key.pem",
    sse_interval_ms: u64 = 100,
};

fn parseArgs(gpa: std.mem.Allocator, process_args: std.process.Args) !Args {
    var out: Args = .{};
    var args = try std.process.Args.Iterator.initAllocator(process_args, gpa);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |a| {
        if (std.mem.eql(u8, a, "--port")) {
            out.port = try std.fmt.parseInt(u16, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, a, "--mode")) {
            out.tls = std.mem.eql(u8, args.next() orelse return error.MissingValue, "tls");
        } else if (std.mem.eql(u8, a, "--cert")) {
            out.cert = try gpa.dupe(u8, args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, a, "--key")) {
            out.key = try gpa.dupe(u8, args.next() orelse return error.MissingValue);
        } else if (std.mem.eql(u8, a, "--sse-interval-ms")) {
            out.sse_interval_ms = try std.fmt.parseInt(u64, args.next() orelse return error.MissingValue, 10);
        } else {
            return error.UnknownArgument;
        }
    }
    return out;
}

fn serveMain(rt: *zio.Runtime, gpa: std.mem.Allocator, process_args: std.process.Args, io: std.Io) !void {
    const args = try parseArgs(gpa, process_args);
    g_sse_interval_ms = args.sse_interval_ms;
    g_io = rt.io();
    var ticker_handle = try rt.spawn(tickerTask, .{});
    defer ticker_handle.cancel();
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", args.port);

    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/", .handler = .{ .ptr = @constCast(&dummy), .runFn = helloHandler } },
        .{ .method = .GET, .path = "/sse", .handler = .{ .ptr = @constCast(&dummy), .runFn = sseHandler } },
        .{ .method = .GET, .path = "/sse-broadcast", .handler = .{ .ptr = @constCast(&dummy), .runFn = sseBroadcastHandler } },
    };

    var cert_pem: []u8 = &.{};
    var key_pem: []u8 = &.{};
    defer if (cert_pem.len != 0) gpa.free(cert_pem);
    defer if (key_pem.len != 0) gpa.free(key_pem);
    var tls_cfg: ?starh2.TlsConfig = null;
    const ep: starh2.EndpointConfig = if (args.tls) blk: {
        cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, args.cert, gpa, .limited(64 * 1024));
        key_pem = try std.Io.Dir.cwd().readFileAlloc(io, args.key, gpa, .limited(16 * 1024));
        tls_cfg = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem };
        break :blk .{ .tls_h2 = addr };
    } else .{ .h2c_prior_knowledge = addr };

    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{ep},
        .routes = &routes,
        .tls = tls_cfg,
    });
    defer server.deinit(gpa);

    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);

    // Same ready-line contract as the conformance server: a harness connects
    // the moment it reads this, so it must never be printed on a timer. Port 0
    // binds a free port, and this line is how the harness learns which.
    const port = server.localAddress(0).getPort();
    const ready = try std.fmt.allocPrint(
        gpa,
        "{{\"ready\":true,\"mode\":\"{s}\",\"port\":{d}}}\n",
        .{ if (args.tls) "tls" else "h2c", port },
    );
    defer gpa.free(ready);
    var out = zio.stdout().writer(&.{});
    try out.interface.writeAll(ready);
    try out.interface.flush();

    try serve_handle.join();
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024,
            .shrink_interval = .fromSeconds(30),
            .slab_slots = 256,
            .prewarm = 256,
        },
        .executors = .auto,
        .enable_task_migration = true,
    });
    defer rt.deinit();

    var handle = try rt.spawn(serveMain, .{ rt, gpa, init.minimal.args, init.io });
    try handle.join();
}
