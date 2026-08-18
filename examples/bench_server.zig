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

const trace = starh2.edge.connection.trace;
const write_trace = starh2.edge.wire_pump.write_trace;

/// Bench-only counting wrapper. Installed on the server GPA when `--trace` is
/// set, so the official bench path (no `--trace`) does not pay atomic increments
/// or extra vtable hops. Sites are hashed by caller return address; collisions
/// overflow rather than merge distinct callers.
///
/// `alloc_ns` / `free_ns` time the parent `rawAlloc` / `rawFree` plus two
/// `Clock.awake` reads, so they overstate allocator cost. `sample(1)` is 1ms
/// and this reactor's bursts are tens of microseconds, so it will not resolve
/// malloc; treat these counters as the allocator-time number, not a CPU %.
const AllocTrace = struct {
    parent: std.mem.Allocator,
    io: std.Io,
    allocs: std.atomic.Value(u64) = .init(0),
    bytes: std.atomic.Value(u64) = .init(0),
    alloc_ns: std.atomic.Value(u64) = .init(0),
    frees: std.atomic.Value(u64) = .init(0),
    free_bytes: std.atomic.Value(u64) = .init(0),
    free_ns: std.atomic.Value(u64) = .init(0),
    overflow: std.atomic.Value(u64) = .init(0),
    sites: [128]Site = @splat(.{}),

    const Site = struct {
        addr: std.atomic.Value(usize) = .init(0),
        count: std.atomic.Value(u64) = .init(0),
        bytes: std.atomic.Value(u64) = .init(0),
        ns: std.atomic.Value(u64) = .init(0),
    };

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn allocator(self: *AllocTrace) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn note(self: *AllocTrace, addr: usize, n: usize, dt: u64) void {
        if (addr == 0) {
            _ = self.overflow.fetchAdd(1, .monotonic);
            return;
        }
        const idx = addr *% 11400714819323198485;
        var i: usize = 0;
        while (i < 8) : (i += 1) {
            const slot = &self.sites[(idx +% i) % self.sites.len];
            const cur = slot.addr.load(.monotonic);
            if (cur == addr) {
                _ = slot.count.fetchAdd(1, .monotonic);
                _ = slot.bytes.fetchAdd(n, .monotonic);
                _ = slot.ns.fetchAdd(dt, .monotonic);
                return;
            }
            if (cur == 0) {
                if (slot.addr.cmpxchgStrong(0, addr, .monotonic, .monotonic) == null or
                    slot.addr.load(.monotonic) == addr)
                {
                    _ = slot.count.fetchAdd(1, .monotonic);
                    _ = slot.bytes.fetchAdd(n, .monotonic);
                    _ = slot.ns.fetchAdd(dt, .monotonic);
                    return;
                }
            }
        }
        _ = self.overflow.fetchAdd(1, .monotonic);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *AllocTrace = @ptrCast(@alignCast(ctx));
        const t0 = nowNs(self.io);
        const p = self.parent.rawAlloc(len, alignment, ret_addr) orelse return null;
        const dt = nowNs(self.io) - t0;
        _ = self.allocs.fetchAdd(1, .monotonic);
        _ = self.bytes.fetchAdd(len, .monotonic);
        _ = self.alloc_ns.fetchAdd(dt, .monotonic);
        self.note(ret_addr, len, dt);
        return p;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *AllocTrace = @ptrCast(@alignCast(ctx));
        return self.parent.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *AllocTrace = @ptrCast(@alignCast(ctx));
        return self.parent.rawRemap(memory, alignment, new_len, ret_addr);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *AllocTrace = @ptrCast(@alignCast(ctx));
        const t0 = nowNs(self.io);
        self.parent.rawFree(memory, alignment, ret_addr);
        const dt = nowNs(self.io) - t0;
        _ = self.frees.fetchAdd(1, .monotonic);
        _ = self.free_bytes.fetchAdd(memory.len, .monotonic);
        _ = self.free_ns.fetchAdd(dt, .monotonic);
    }
};

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

var g_alloc_trace: ?*AllocTrace = null;

/// GET /trace returns the phase-trace counters as JSON. The harness reads it
/// before and after a run and divides, so the numbers are deltas over a known
/// window rather than lifetime averages. Allocation fields are zero unless the
/// process was started with `--trace` (counting allocator installed).
fn traceHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var snap: [10]u64 = undefined;
    trace.snapshot(&snap);
    var allocs: u64 = 0;
    var alloc_bytes: u64 = 0;
    var alloc_ns: u64 = 0;
    var frees: u64 = 0;
    var free_bytes: u64 = 0;
    var free_ns: u64 = 0;
    var overflow: u64 = 0;
    const Top = struct { addr: usize, n: u64, bytes: u64, ns: u64 };
    var top: [8]Top = @splat(.{ .addr = 0, .n = 0, .bytes = 0, .ns = 0 });
    if (g_alloc_trace) |t| {
        allocs = t.allocs.load(.acquire);
        alloc_bytes = t.bytes.load(.acquire);
        alloc_ns = t.alloc_ns.load(.acquire);
        frees = t.frees.load(.acquire);
        free_bytes = t.free_bytes.load(.acquire);
        free_ns = t.free_ns.load(.acquire);
        overflow = t.overflow.load(.acquire);
        for (&t.sites) |*s| {
            const n = s.count.load(.acquire);
            if (n == 0) continue;
            const cand = Top{
                .addr = s.addr.load(.acquire),
                .n = n,
                .bytes = s.bytes.load(.acquire),
                .ns = s.ns.load(.acquire),
            };
            var i: usize = 0;
            while (i < top.len and top[i].n >= cand.n) i += 1;
            if (i < top.len) {
                var j: usize = top.len - 1;
                while (j > i) : (j -= 1) top[j] = top[j - 1];
                top[i] = cand;
            }
        }
    }
    var sites_buf: [768]u8 = undefined;
    var sites_len: usize = 0;
    sites_buf[sites_len] = '[';
    sites_len += 1;
    var first = true;
    for (top) |s| {
        if (s.n == 0) continue;
        const piece = std.fmt.bufPrint(
            sites_buf[sites_len..],
            "{s}[{d},{d},{d},{d}]",
            .{ if (first) "" else ",", s.addr, s.n, s.bytes, s.ns },
        ) catch break;
        sites_len += piece.len;
        first = false;
    }
    if (sites_len < sites_buf.len) {
        sites_buf[sites_len] = ']';
        sites_len += 1;
    }
    var buf: [4096]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try w.print(
        "{{\"samples\":{d},\"block_ns\":{d},\"hold_ns\":{d},\"ack_ns\":{d},\"resume_ns\":{d}," ++
            "\"actor_ns\":{d},\"queue_ns\":{d},\"pump_ns\":{d},\"ack_split_samples\":{d}," ++
            "\"write_ns\":{d},\"drain_ns\":{d},\"pump_split_samples\":{d}," ++
            "\"block_max\":{d},\"hold_max\":{d},\"ack_max\":{d},\"writes\":{d},\"skipped\":{d}," ++
            "\"lifecycle\":{d},\"spawn_ns\":{d},\"to_send_ns\":{d},\"hpack_ns\":{d}," ++
            "\"spawn_max\":{d},\"to_send_max\":{d},\"hpack_max\":{d},\"jobs\":{d},",
        .{
            snap[0],                       snap[1],                                 snap[2],                                snap[3],                                snap[4],
            trace.actor_ns.load(.acquire), trace.queue_ns.load(.acquire),           trace.pump_ns.load(.acquire),           trace.ack_split_samples.load(.acquire), trace.write_ns.load(.acquire),
            trace.drain_ns.load(.acquire), trace.pump_split_samples.load(.acquire), snap[5],                                snap[6],                                snap[7],
            snap[8],                       snap[9],                                 trace.lifecycle_samples.load(.acquire), trace.spawn_ns.load(.acquire),          trace.to_send_ns.load(.acquire),
            trace.hpack_ns.load(.acquire), trace.spawn_max.load(.acquire),          trace.to_send_max.load(.acquire),       trace.hpack_max.load(.acquire),         trace.jobs.load(.acquire),
        },
    );
    try w.print(
        "\"handoffs\":{d},\"tickets\":{d},\"handoff_bytes\":{d},\"handoff_max\":{d}," ++
            "\"batch_1\":{d},\"batch_2\":{d},\"batch_le4\":{d},\"batch_le8\":{d}," ++
            "\"batch_le16\":{d},\"batch_ge17\":{d},\"records\":{d}," ++
            "\"emit_turns\":{d},\"emit_tickets\":{d},\"emit_max\":{d}," ++
            "\"write_calls\":{d},\"write_chunks\":{d},\"write_max\":{d},",
        .{
            trace.handoffs.load(.acquire),
            trace.tickets.load(.acquire),
            trace.handoff_bytes.load(.acquire),
            trace.handoff_max.load(.acquire),
            trace.batch_1.load(.acquire),
            trace.batch_2.load(.acquire),
            trace.batch_le4.load(.acquire),
            trace.batch_le8.load(.acquire),
            trace.batch_le16.load(.acquire),
            trace.batch_ge17.load(.acquire),
            trace.records.load(.acquire),
            trace.emit_turns.load(.acquire),
            trace.emit_tickets.load(.acquire),
            trace.emit_max.load(.acquire),
            write_trace.calls.load(.acquire),
            write_trace.chunks.load(.acquire),
            write_trace.max_chunks.load(.acquire),
        },
    );
    try w.print(
        "\"encrypt_ns\":{d},\"encrypt_n\":{d},\"encrypt_bytes\":{d}," ++
            "\"decrypt_ns\":{d},\"decrypt_n\":{d},\"decrypt_in\":{d},\"decrypt_plain\":{d}," ++
            "\"inbound_records\":{d},\"decrypt_loop_ns\":{d},\"decrypt_loop_n\":{d}," ++
            "\"ingest_ns\":{d},\"ingest_n\":{d},\"intent_ns\":{d},\"intent_n\":{d}," ++
            "\"send_ns\":{d},\"send_n\":{d},\"send_bytes\":{d},",
        .{
            trace.encrypt_ns.load(.acquire),
            trace.encrypt_n.load(.acquire),
            trace.encrypt_bytes.load(.acquire),
            trace.decrypt_ns.load(.acquire),
            trace.decrypt_n.load(.acquire),
            trace.decrypt_in.load(.acquire),
            trace.decrypt_plain.load(.acquire),
            trace.inbound_records.load(.acquire),
            trace.decrypt_loop_ns.load(.acquire),
            trace.decrypt_loop_n.load(.acquire),
            trace.ingest_ns.load(.acquire),
            trace.ingest_n.load(.acquire),
            trace.intent_ns.load(.acquire),
            trace.intent_n.load(.acquire),
            trace.send_ns.load(.acquire),
            trace.send_n.load(.acquire),
            trace.send_bytes.load(.acquire),
        },
    );
    try w.print(
        "\"allocs\":{d},\"alloc_bytes\":{d},\"alloc_ns\":{d}," ++
            "\"frees\":{d},\"free_bytes\":{d},\"free_ns\":{d}," ++
            "\"alloc_overflow\":{d},\"sites\":{s}}}\n",
        .{ allocs, alloc_bytes, alloc_ns, frees, free_bytes, free_ns, overflow, sites_buf[0..sites_len] },
    );
    try resp.send(200, &.{.{ .name = "content-type", .value = "application/json" }}, w.buffered());
}

const BODY = "Hello, World!";

fn helloHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
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
    trace: bool = false,
    trace_every: u64 = 1024,
    executors: ?u8 = null,
    task_migration: bool = false,
    diag: bool = false,
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
        } else if (std.mem.eql(u8, a, "--trace")) {
            out.trace = true;
        } else if (std.mem.eql(u8, a, "--trace-every")) {
            out.trace_every = try std.fmt.parseInt(u64, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, a, "--executors")) {
            out.executors = try std.fmt.parseInt(u8, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, a, "--task-migration")) {
            out.task_migration = true;
        } else if (std.mem.eql(u8, a, "--diag")) {
            out.diag = true;
        } else {
            return error.UnknownArgument;
        }
    }
    return out;
}

const RuntimeArgs = struct {
    executors: ?u8 = null,
    task_migration: bool = false,
};

fn parseRuntimeArgs(gpa: std.mem.Allocator, process_args: std.process.Args) !RuntimeArgs {
    var out: RuntimeArgs = .{};
    var args = try std.process.Args.Iterator.initAllocator(process_args, gpa);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |a| {
        if (std.mem.eql(u8, a, "--executors")) {
            const n = try std.fmt.parseInt(u8, args.next() orelse return error.MissingValue, 10);
            if (n == 0) return error.InvalidExecutorCount;
            out.executors = n;
        } else if (std.mem.eql(u8, a, "--task-migration")) {
            out.task_migration = true;
        }
    }
    return out;
}

fn serveMain(rt: *zio.Runtime, gpa: std.mem.Allocator, process_args: std.process.Args, io: std.Io) !void {
    const args = try parseArgs(gpa, process_args);
    g_sse_interval_ms = args.sse_interval_ms;
    starh2.edge.connection.diag_park = args.diag;
    trace.enabled = args.trace;
    trace.sample_every = args.trace_every;
    write_trace.enabled = args.trace;
    var alloc_trace_storage: AllocTrace = .{ .parent = gpa, .io = io };
    const server_gpa: std.mem.Allocator = if (args.trace) blk: {
        g_alloc_trace = &alloc_trace_storage;
        break :blk alloc_trace_storage.allocator();
    } else gpa;
    g_io = rt.io();
    var ticker_handle = try rt.spawn(tickerTask, .{});
    defer ticker_handle.cancel();
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", args.port);

    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = helloHandler } } },
        .{ .method = .GET, .path = "/sse", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseHandler } } },
        .{ .method = .GET, .path = "/sse-broadcast", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseBroadcastHandler } } },
        .{ .method = .GET, .path = "/trace", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = traceHandler } } },
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

    var server = try starh2.Server.init(server_gpa, rt.io(), .{
        .endpoints = &.{ep},
        .routes = &routes,
        .tls = tls_cfg,
    });
    defer server.deinit(server_gpa);

    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, server_gpa });
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
    const runtime_args = try parseRuntimeArgs(gpa, init.minimal.args);
    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 16 * 1024,
            .shrink_interval = .fromSeconds(30),
            .slab_slots = 256,
            .prewarm = 256,
        },
        .executors = if (runtime_args.executors) |n| .exact(n) else .auto,
        // zio a2b134a can strand a migrated socket task while both directions
        // have queued kernel data. Keep I/O tasks on their home executor; the
        // opt-in flag exists only to preserve the upstream reproducer.
        .enable_task_migration = runtime_args.task_migration,
    });
    defer rt.deinit();

    var handle = try rt.spawn(serveMain, .{ rt, gpa, init.minimal.args, init.io });
    try handle.join();
}
