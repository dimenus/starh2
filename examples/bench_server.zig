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
    var buf: [6144]u8 = undefined;
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
            "\"emit_turns\":{d},\"emit_tickets\":{d},\"emit_max\":{d},\"complete_receipt_depth_max\":{d}," ++
            "\"receipt_depth_0\":{d},\"receipt_depth_1\":{d},\"receipt_depth_2\":{d}," ++
            "\"receipt_depth_3\":{d},\"receipt_depth_4\":{d}," ++
            "\"receipt_full_ns\":{d},\"receipt_full_enter\":{d}," ++
            "\"inline_full_n\":{d},\"inline_full_sids\":{d},\"inline_full_max\":{d}," ++
            "\"credit_reuse_ns\":{d},\"credit_reuse_n\":{d},",
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
            trace.complete_receipt_depth_max.load(.acquire),
            trace.receipt_depth_0.load(.acquire),
            trace.receipt_depth_1.load(.acquire),
            trace.receipt_depth_2.load(.acquire),
            trace.receipt_depth_3.load(.acquire),
            trace.receipt_depth_4.load(.acquire),
            trace.receipt_full_ns.load(.acquire),
            trace.receipt_full_enter.load(.acquire),
            trace.inline_full_n.load(.acquire),
            trace.inline_full_sids.load(.acquire),
            trace.inline_full_max.load(.acquire),
            trace.credit_reuse_ns.load(.acquire),
            trace.credit_reuse_n.load(.acquire),
        },
    );
    try w.print(
        "\"read_take_n\":{d},\"read_left_0\":{d},\"read_left_1\":{d},\"read_left_2\":{d}," ++
            "\"read_left_3\":{d},\"read_left_ge4\":{d},\"read_left_sum\":{d},\"read_left_max\":{d}," ++
            "\"inbound_batch_1\":{d},\"inbound_batch_2\":{d},\"inbound_batch_3\":{d},\"inbound_batch_4\":{d}," ++
            "\"write_calls\":{d},\"write_chunks\":{d},\"write_max\":{d},",
        .{
            trace.read_take_n.load(.acquire),
            trace.read_left_0.load(.acquire),
            trace.read_left_1.load(.acquire),
            trace.read_left_2.load(.acquire),
            trace.read_left_3.load(.acquire),
            trace.read_left_ge4.load(.acquire),
            trace.read_left_sum.load(.acquire),
            trace.read_left_max.load(.acquire),
            trace.inbound_batch_1.load(.acquire),
            trace.inbound_batch_2.load(.acquire),
            trace.inbound_batch_3.load(.acquire),
            trace.inbound_batch_4.load(.acquire),
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
            "\"send_ns\":{d},\"send_n\":{d},\"send_bytes\":{d}," ++
            "\"acc_append_ns\":{d},\"acc_append_n\":{d},\"acc_append_bytes\":{d}," ++
            "\"acc_compact_ns\":{d},\"acc_compact_n\":{d},\"acc_compact_bytes\":{d},",
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
            trace.acc_append_ns.load(.acquire),
            trace.acc_append_n.load(.acquire),
            trace.acc_append_bytes.load(.acquire),
            trace.acc_compact_ns.load(.acquire),
            trace.acc_compact_n.load(.acquire),
            trace.acc_compact_bytes.load(.acquire),
        },
    );
    try w.print(
        "\"allocs\":{d},\"alloc_bytes\":{d},\"alloc_ns\":{d}," ++
            "\"frees\":{d},\"free_bytes\":{d},\"free_ns\":{d}," ++
            "\"alloc_overflow\":{d},\"sites\":{s}",
        .{ allocs, alloc_bytes, alloc_ns, frees, free_bytes, free_ns, overflow, sites_buf[0..sites_len] },
    );
    if (comptime starh2.edge.tls_edge.observe) {
        try starh2.edge.tls_edge.pump_trace.writeJson(&w);
    }
    try w.writeAll("}\n");
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

/// Bench-only: where `/sse` loop time goes. Reset via GET /sse-cadence-reset.
/// `loops` is handler iterations; `writes` is successful `writeAll`s. If
/// `loops` matches the client event count, the ticks were never produced.
/// If `loops` is ~16k and the client saw ~9k, they were produced and not
/// delivered. `skipped` is arriving at the deadline already late (no sleep).
/// `late_*` is sleeping toward a deadline and waking after it.
const Cadence = struct {
    loops: std.atomic.Value(u64) = .init(0),
    sleeps: std.atomic.Value(u64) = .init(0),
    skipped: std.atomic.Value(u64) = .init(0),
    writes: std.atomic.Value(u64) = .init(0),
    first_n: std.atomic.Value(u64) = .init(0),
    inter_n: std.atomic.Value(u64) = .init(0),
    late_n: std.atomic.Value(u64) = .init(0),
    late_ge1ms: std.atomic.Value(u64) = .init(0),
    sleep_req_ns: std.atomic.Value(u64) = .init(0),
    sleep_ns: std.atomic.Value(u64) = .init(0),
    late_ns: std.atomic.Value(u64) = .init(0),
    write_ns: std.atomic.Value(u64) = .init(0),
    yield_ns: std.atomic.Value(u64) = .init(0),
    skip_behind_ns: std.atomic.Value(u64) = .init(0),
    first_ns: std.atomic.Value(u64) = .init(0),
    inter_ns: std.atomic.Value(u64) = .init(0),
    start_sse_n: std.atomic.Value(u64) = .init(0),
    start_sse_ns: std.atomic.Value(u64) = .init(0),
    start_sse_max: std.atomic.Value(u64) = .init(0),

    fn reset(self: *Cadence) void {
        inline for (.{
            &self.loops,         &self.sleeps,      &self.skipped,
            &self.writes,        &self.first_n,     &self.inter_n,
            &self.late_n,        &self.late_ge1ms,
            &self.sleep_req_ns,  &self.sleep_ns,    &self.late_ns,
            &self.write_ns,      &self.yield_ns,    &self.skip_behind_ns,
            &self.first_ns,      &self.inter_ns,
            &self.start_sse_n,   &self.start_sse_ns, &self.start_sse_max,
        }) |f| f.store(0, .release);
    }
};

var g_cadence: Cadence = .{};

fn sseHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const t_enter = zio.Timestamp.now(.realtime).toNanoseconds();
    var body = try resp.startSse(&.{});
    const t_ready = zio.Timestamp.now(.realtime).toNanoseconds();
    if (t_ready >= t_enter) {
        const dt: u64 = @intCast(t_ready - t_enter);
        _ = g_cadence.start_sse_n.fetchAdd(1, .monotonic);
        _ = g_cadence.start_sse_ns.fetchAdd(dt, .monotonic);
        var max = g_cadence.start_sse_max.load(.monotonic);
        while (dt > max) {
            if (g_cadence.start_sse_max.cmpxchgWeak(max, dt, .monotonic, .monotonic)) |cur| {
                max = cur;
            } else break;
        }
    }
    var buf: [64]u8 = undefined;

    // Sleep to a DEADLINE, never for a duration. `sleep(interval)` in a loop
    // makes the period interval plus the work, so the emitted rate drifts below
    // the requested rate and the arm looks like it dropped events when it only
    // ticked slowly. That is a defect in a benchmark, not a measurement of the
    // server. Go's time.Ticker keeps a schedule, so the arms must match.
    const interval_ns: i128 = @as(i128, @intCast(g_sse_interval_ms)) * std.time.ns_per_ms;
    var prev_write: ?i128 = null;
    const t_start: i128 = zio.Timestamp.now(.realtime).toNanoseconds();
    // Schedule on Clock.awake: waitUntil fires against the actor's nowNs.
    var next: i128 = std.Io.Clock.awake.now(g_io).nanoseconds + interval_ns;
    while (true) {
        _ = g_cadence.loops.fetchAdd(1, .monotonic);
        const now_ns: i128 = std.Io.Clock.awake.now(g_io).nanoseconds;
        const deadline = next;
        if (deadline > now_ns) {
            _ = g_cadence.sleeps.fetchAdd(1, .monotonic);
            // Cadence is a heap entry, not a handler timer. waitUntil parks
            // on the actor-owned deadline heap; waitForActivity is the idle
            // arm of the same heap.
            const ts = std.Io.Timestamp.fromNanoseconds(@intCast(deadline));
            body.waitUntil(ts) catch |err| {
                if (err == error.Canceled) return error.Canceled;
                return err;
            };
        } else {
            _ = g_cadence.skipped.fetchAdd(1, .monotonic);
            _ = g_cadence.skip_behind_ns.fetchAdd(@intCast(now_ns - deadline), .monotonic);
        }
        next += interval_ns;
        const t_write = zio.Timestamp.now(.realtime).toNanoseconds();
        const t_write_awake: i128 = std.Io.Clock.awake.now(g_io).nanoseconds;
        if (deadline < t_write_awake) {
            const late: u64 = @intCast(t_write_awake - deadline);
            _ = g_cadence.late_n.fetchAdd(1, .monotonic);
            _ = g_cadence.late_ns.fetchAdd(late, .monotonic);
            if (late >= std.time.ns_per_ms) _ = g_cadence.late_ge1ms.fetchAdd(1, .monotonic);
        }
        const ev = try std.fmt.bufPrint(&buf, "data: {d}\n\n", .{t_write});
        body.writeAll(ev) catch break;
        _ = g_cadence.writes.fetchAdd(1, .monotonic);
        if (prev_write) |p| {
            if (t_write >= p) {
                _ = g_cadence.inter_n.fetchAdd(1, .monotonic);
                _ = g_cadence.inter_ns.fetchAdd(@intCast(t_write - p), .monotonic);
            }
        } else {
            _ = g_cadence.first_n.fetchAdd(1, .monotonic);
            if (t_write >= t_start) {
                _ = g_cadence.first_ns.fetchAdd(@intCast(t_write - t_start), .monotonic);
            }
        }
        prev_write = t_write;
    }
    try body.finish();
}

fn cadenceJson(w: *std.Io.Writer) !void {
    try w.print(
        "{{\"loops\":{d},\"sleeps\":{d},\"skipped\":{d},\"writes\":{d}," ++
            "\"first_n\":{d},\"inter_n\":{d}," ++
            "\"late_n\":{d},\"late_ge1ms\":{d}," ++
            "\"sleep_req_ns\":{d},\"sleep_ns\":{d},\"late_ns\":{d}," ++
            "\"write_ns\":{d},\"yield_ns\":{d},\"skip_behind_ns\":{d}," ++
            "\"first_ns\":{d},\"inter_ns\":{d}," ++
            "\"start_sse_n\":{d},\"start_sse_ns\":{d},\"start_sse_max\":{d}}}\n",
        .{
            g_cadence.loops.load(.acquire),
            g_cadence.sleeps.load(.acquire),
            g_cadence.skipped.load(.acquire),
            g_cadence.writes.load(.acquire),
            g_cadence.first_n.load(.acquire),
            g_cadence.inter_n.load(.acquire),
            g_cadence.late_n.load(.acquire),
            g_cadence.late_ge1ms.load(.acquire),
            g_cadence.sleep_req_ns.load(.acquire),
            g_cadence.sleep_ns.load(.acquire),
            g_cadence.late_ns.load(.acquire),
            g_cadence.write_ns.load(.acquire),
            g_cadence.yield_ns.load(.acquire),
            g_cadence.skip_behind_ns.load(.acquire),
            g_cadence.first_ns.load(.acquire),
            g_cadence.inter_ns.load(.acquire),
            g_cadence.start_sse_n.load(.acquire),
            g_cadence.start_sse_ns.load(.acquire),
            g_cadence.start_sse_max.load(.acquire),
        },
    );
}

fn cadenceHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    var buf: [768]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try cadenceJson(&w);
    try resp.send(200, &.{.{ .name = "content-type", .value = "application/json" }}, w.buffered());
}

fn cadenceResetHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    g_cadence.reset();
    try resp.send(200, &.{.{ .name = "content-type", .value = "application/json" }}, "{\"reset\":true}\n");
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
    // Default ON since the two-OS t-853 gate: both 60-round stall runs were
    // clean with migration on, and migration off is the home of the bimodal
    // placement bands. --no-task-migration keeps the A/B arm reachable.
    task_migration: bool = true,
    diag: bool = false,
    /// When set, connect to this process's listener over real TCP, drive N
    /// oneshots on one connection, then shut down. Hyperfine/poop measure the
    /// whole program. Zero is refused: a no-op self-drive is not a result.
    self_drive_oneshots: ?usize = null,
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
        } else if (std.mem.eql(u8, a, "--no-task-migration")) {
            out.task_migration = false;
        } else if (std.mem.eql(u8, a, "--diag")) {
            out.diag = true;
        } else if (std.mem.eql(u8, a, "--self-drive-oneshots")) {
            const n = try std.fmt.parseInt(usize, args.next() orelse return error.MissingValue, 10);
            if (n == 0) return error.InvalidSelfDriveCount;
            out.self_drive_oneshots = n;
        } else {
            return error.UnknownArgument;
        }
    }
    return out;
}

const RuntimeArgs = struct {
    executors: ?u8 = null,
    task_migration: bool = true,
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
        } else if (std.mem.eql(u8, a, "--no-task-migration")) {
            out.task_migration = false;
        }
    }
    return out;
}

const frame = starh2.core.frame;
const hpack = starh2.core.hpack;
const tls_edge = starh2.edge.tls_edge;

const self_drive_in_flight: usize = 10;

const BytePipe = union(enum) {
    io: struct { reader: *std.Io.Reader, writer: *std.Io.Writer },
    tls: *tls_edge.Conn,

    fn readExact(self: BytePipe, buf: []u8) !void {
        switch (self) {
            .io => |p| {
                var got: usize = 0;
                while (got < buf.len) {
                    var dest: [1][]u8 = .{buf[got..]};
                    const n = try p.reader.readVec(&dest);
                    if (n == 0) return error.ConnectionClosed;
                    got += n;
                }
            },
            .tls => |conn| {
                var got: usize = 0;
                while (got < buf.len) {
                    const n = try conn.readPlain(buf[got..]);
                    if (n == 0) return error.ConnectionClosed;
                    got += n;
                }
            },
        }
    }

    fn writeAll(self: BytePipe, buf: []const u8) !void {
        switch (self) {
            .io => |p| {
                try p.writer.writeAll(buf);
                try p.writer.flush();
            },
            .tls => |conn| try conn.writePlain(buf),
        }
    }
};

fn encodeGetSlash(gpa: std.mem.Allocator) ![]u8 {
    const fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":authority", .value = "localhost" },
    };
    return hpack.Encoder.encode(gpa, &fields);
}

fn appendHeadersFrame(out: *std.ArrayList(u8), gpa: std.mem.Allocator, stream_id: u31, block: []const u8) !void {
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    const fh = frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = true },
        .stream_id = stream_id,
    };
    fh.encode(&hdr_buf);
    try out.appendSlice(gpa, &hdr_buf);
    try out.appendSlice(gpa, block);
}

fn driveH2(gpa: std.mem.Allocator, pipe: BytePipe, n: usize) !void {
    const block = try encodeGetSlash(gpa);
    defer gpa.free(block);

    var hello: std.ArrayList(u8) = .empty;
    defer hello.deinit(gpa);
    try hello.appendSlice(gpa, frame.CLIENT_PREFACE);
    var sbuf: [64]u8 = undefined;
    const settings = [_]frame.Setting{
        .{ .id = .max_concurrent_streams, .value = 256 },
        .{ .id = .initial_window_size, .value = 1 << 20 },
    };
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
    try hello.appendSlice(gpa, sbuf[0..sn]);
    try pipe.writeAll(hello.items);

    var sent: usize = 0;
    var done: usize = 0;
    var in_flight: usize = 0;
    var next_sid: u31 = 1;
    var conn_consumed: u32 = 0;
    var payload_buf: [frame.DEFAULT_MAX_FRAME_SIZE]u8 = undefined;
    var write_buf: std.ArrayList(u8) = .empty;
    defer write_buf.deinit(gpa);

    while (done < n) {
        write_buf.clearRetainingCapacity();
        while (in_flight < self_drive_in_flight and sent < n) {
            try appendHeadersFrame(&write_buf, gpa, next_sid, block);
            next_sid += 2;
            sent += 1;
            in_flight += 1;
        }
        if (write_buf.items.len != 0) try pipe.writeAll(write_buf.items);

        try pipe.readExact(payload_buf[0..frame.FRAME_HEADER_LEN]);
        const hdr = frame.FrameHeader.decode(payload_buf[0..frame.FRAME_HEADER_LEN]);
        if (hdr.length > payload_buf.len) return error.FrameTooLarge;
        if (hdr.length > 0) try pipe.readExact(payload_buf[0..hdr.length]);

        switch (hdr.type) {
            .settings => {
                if (!hdr.flags.ack()) {
                    const an = try frame.Serializer.settingsFrame(&sbuf, true, &.{});
                    try pipe.writeAll(sbuf[0..an]);
                }
            },
            .ping => {
                if (!hdr.flags.ack()) {
                    var ping_opaque: [8]u8 = undefined;
                    if (hdr.length != 8) return error.Protocol;
                    @memcpy(&ping_opaque, payload_buf[0..8]);
                    var ping_buf: [17]u8 = undefined;
                    const pn = try frame.Serializer.ping(&ping_buf, true, &ping_opaque);
                    try pipe.writeAll(ping_buf[0..pn]);
                }
            },
            .window_update => {},
            .headers, .data => {
                if (hdr.type == .data and hdr.length > 0) {
                    conn_consumed += hdr.length;
                    if (conn_consumed >= 16 * 1024) {
                        var wu: [13]u8 = undefined;
                        const wn = try frame.Serializer.windowUpdate(&wu, 0, @intCast(conn_consumed));
                        try pipe.writeAll(wu[0..wn]);
                        conn_consumed = 0;
                    }
                }
                if (hdr.flags.end_stream) {
                    if (in_flight == 0) return error.SelfDriveUnexpectedEnd;
                    in_flight -= 1;
                    done += 1;
                }
            },
            .goaway => return error.SelfDriveGoaway,
            .rst_stream => return error.SelfDriveRst,
            else => {},
        }
    }
}

fn driveOneshots(gpa: std.mem.Allocator, io: std.Io, tls: bool, peer: starh2.EndpointAddress, n: usize) !void {
    const stream = try peer.connect(io, .{ .mode = .stream });
    if (tls) {
        var conn: tls_edge.Conn = undefined;
        conn.initTcp(stream);
        conn.bindIo(io);
        defer {
            conn.deinit();
            stream.close(io);
        }
        var connector = try tls_edge.loopbackClientConnector();
        defer connector.deinit();
        try conn.handshakeClient(&connector, io);
        try driveH2(gpa, .{ .tls = &conn }, n);
    } else {
        var read_buf: [tls_edge.stream_buffer_size]u8 = undefined;
        var write_buf: [tls_edge.stream_buffer_size]u8 = undefined;
        var reader = stream.reader(io, &read_buf);
        var writer = stream.writer(io, &write_buf);
        defer stream.close(io);
        try driveH2(gpa, .{ .io = .{ .reader = &reader.interface, .writer = &writer.interface } }, n);
    }
}

extern "c" fn nanosleep(req: *const std.c.timespec, rem: ?*std.c.timespec) c_int;

fn diagSweeperMain() void {
    starh2.edge.connection.diagRawPrint("STARH2_SWEEPER up\n", .{});
    var sweeps: u64 = 0;
    while (true) {
        var req = std.c.timespec{ .sec = 0, .nsec = 500 * std.time.ns_per_ms };
        _ = nanosleep(&req, null);
        sweeps += 1;
        const kicked = starh2.edge.connection.diagRekickSweep();
        if (kicked != 0 or sweeps % 20 == 0) {
            starh2.edge.connection.diagRawPrint("STARH2_SWEEPER sweeps={d} kicked={d}\n", .{ sweeps, kicked });
        }
    }
}

fn serveMain(rt: *zio.Runtime, gpa: std.mem.Allocator, process_args: std.process.Args, io: std.Io) !void {
    const args = try parseArgs(gpa, process_args);
    g_sse_interval_ms = args.sse_interval_ms;
    starh2.edge.connection.diag_park = args.diag;
    starh2.edge.tls_edge.diag_wait = args.diag;
    if (args.diag) {
        // These exports exist only in a locally patched zio (zig-pkg is
        // machine-local and gitignored); a pristine pin builds without them
        // and the sweep prints 0xff-derived task tags instead.
        if (comptime @hasDecl(zio, "debugCurrentTaskHandle")) {
            starh2.edge.connection.diag_task_handle_fn = &zio.debugCurrentTaskHandle;
            starh2.edge.connection.diag_task_state_fn = &zio.debugTaskStateByte;
        }
        if (comptime @hasDecl(zio, "debugTaskMark")) {
            starh2.edge.connection.diag_task_mark_fn = &zio.debugTaskMark;
            starh2.edge.connection.diag_stamp_now_fn = &zio.debugStampNow;
        }
        // OS thread, outside the zio scheduler on purpose: near the wedge the
        // scheduler stops delivering wakes, so an in-runtime watchdog task is
        // itself a victim. This sweep re-drives a wedged pump futex directly.
        starh2.edge.connection.diagRawPrint("STARH2_DIAG armed\n", .{});
        const sweeper = try std.Thread.spawn(.{}, diagSweeperMain, .{});
        sweeper.detach();
    }
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
        .{ .method = .GET, .path = "/sse-cadence", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = cadenceHandler } } },
        .{ .method = .GET, .path = "/sse-cadence-reset", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = cadenceResetHandler } } },
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

    if (args.self_drive_oneshots) |n| {
        // The listener lives on rt.io(); init.io would block this task
        // and starve accept.
        try driveOneshots(gpa, rt.io(), args.tls, server.localAddress(0), n);
        server.requestShutdown();
    }

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
