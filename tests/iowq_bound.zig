//! A client cannot hold io_uring io-wq kernel threads past a bound (t-3887).
//!
//! The kernel defect (t-3667; it lasts until the io_uring poll fix that
//! t-3863 tracks reaches the kernels we run): a socket send that io_uring
//! polls for a full send buffer, to a peer that has half-closed (FIN), sees
//! EPOLLRDHUP on every re-arm. After 128 re-arms the kernel hands the send to
//! an io-wq worker thread, where a plain IORING_OP_SEND blocks the thread
//! until the peer reads. The zio fork's -EAGAIN bridge covers SENDMSG only,
//! and starh2's TLS path sends with plain SEND.
//!
//! What bounds it in starh2 is not zio. The connection's own read sees the
//! client's EOF and the HTTP/2 closing phase starts; nothing more can be
//! flushed to a peer that does not read, so `close_flush_timeout_ns` ends the
//! connection, and its teardown frees the worker. Two checks hold that:
//! - every connection has ended by the end of the watch (a connection that
//!   ignores the EOF lives on to `write_byte_timeout_ns`, held by a client
//!   that will never read);
//! - no worker is still blocked `bound_ns` after the half-close (a closing
//!   phase that is allowed to last longer holds the thread that long).
//!
//! The probe: 32 TLS HTTP/2 connections with a 4 KiB receive buffer each ask
//! for 8 MiB (8 streams of 1 MiB, the windows opened), which fills the
//! server's socket buffer and parks its sends. Then each half-closes and never
//! reads. A thread samples this process's `iou-wrk` threads every 2 ms and
//! counts the ones whose `wchan` is not the idle wait or running (`0`):
//! those are blocked, in practice in `wait_woken` under the socket send. The
//! idle wait is the worker's main function: `io_wq_worker` since Linux 6.4,
//! `io_wqe_worker` before it (renamed by "io-wq: Drop struct io_wqe",
//! eb47943f2238). Every distinct `wchan` seen is printed, so a name this list
//! does not know shows up as blocked and in the line.
//!
//! The bound check must see the defect fire, or it proves nothing:
//! - zio's Linux `auto` backend falls back to epoll when ring setup fails.
//!   epoll has no io-wq, so the test FAILS then, saying so, before the probe.
//! - On io_uring, no `iou-wrk` thread at all also FAILS. The sampler may be
//!   blind, and a blind sampler must not pass; if instead the kernel no longer
//!   hands these sends to io-wq (the fix t-3863 tracks), this test has done
//!   its job and should be retired, which a failure makes someone do.
//! - On io_uring, workers seen but none ever blocked SKIPS and says why: a
//!   kernel that still uses io-wq for other work but not for these sends.
//!
//! Linux only: io-wq is io_uring's.
const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");
const serve_deadline = @import("serve_deadline.zig");

const frame = starh2.core.frame;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;
const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

const connections = 32;
const streams_per_connection = 8;
/// The bound held, from the last half-close to the last blocked worker seen.
/// What sets it is `close_flush_timeout_ns` (500 ms) plus teardown. 2 s
/// leaves room for a Debug build on a loaded ci machine, and still fails a
/// change that leaves the worker to `write_byte_timeout_ns` (60 s).
const bound_ns = 2000 * ms;
/// How long the sampler watches after the half-close.
const watch_ns = 4000 * ms;

var big: [1 << 20]u8 = @splat('x');

fn bigHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try resp.send(200, &.{.{ .name = "content-type", .value = "application/octet-stream" }}, &big);
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = bigHandler } } },
};

/// What the sampler saw.
const Seen = struct {
    /// Most `iou-wrk` threads blocked in one sample, before and after the
    /// half-close.
    blocked_before: usize = 0,
    blocked_max: usize = 0,
    workers_max: usize = 0,
    /// Last sample with a blocked worker, ns after the half-close; null if none.
    last_blocked_ns: ?u64 = null,
    samples: usize = 0,
    /// Every distinct `wchan` of an `iou-wrk` thread, in any sample.
    wchans: [max_wchans][32]u8 = undefined,
    wchan_lens: [max_wchans]u8 = undefined,
    wchan_n: usize = 0,
    /// A distinct value past `max_wchans`, or one longer than 32 bytes: the
    /// list is not complete.
    wchan_more: bool = false,

    const max_wchans = 8;

    fn noteWchan(self: *Seen, w: []const u8) void {
        for (self.wchans[0..self.wchan_n], self.wchan_lens[0..self.wchan_n]) |*have, len| {
            if (std.mem.eql(u8, have[0..len], w)) return;
        }
        if (self.wchan_n == max_wchans or w.len > 32) {
            self.wchan_more = true;
            return;
        }
        @memcpy(self.wchans[self.wchan_n][0..w.len], w);
        self.wchan_lens[self.wchan_n] = @intCast(w.len);
        self.wchan_n += 1;
    }

    fn printWchans(self: *const Seen) void {
        for (self.wchans[0..self.wchan_n], self.wchan_lens[0..self.wchan_n], 0..) |*have, len, i| {
            std.debug.print("{s}{s}", .{ if (i == 0) "" else ",", have[0..len] });
        }
        if (self.wchan_more) std.debug.print(",(more)", .{});
    }
};

/// The idle wait of an io-wq worker (see the file comment), or running.
fn workerNotBlocked(wchan: []const u8) bool {
    return std.mem.eql(u8, wchan, "io_wq_worker") or
        std.mem.eql(u8, wchan, "io_wqe_worker") or
        std.mem.eql(u8, wchan, "0");
}

/// Blocked `iou-wrk` threads of this process now, and all of them. Each
/// worker's `wchan` is noted in `seen`.
fn sampleWorkers(seen: *Seen) struct { workers: usize, blocked: usize } {
    var out: @TypeOf(sampleWorkers(seen)) = .{ .workers = 0, .blocked = 0 };
    const dir = std.c.opendir("/proc/self/task") orelse return out;
    defer _ = std.c.closedir(dir);
    var path: [64]u8 = undefined;
    var buf: [64]u8 = undefined;
    while (std.c.readdir(dir)) |ent| {
        const name = std.mem.sliceTo(@as([*:0]const u8, @ptrCast(&ent.name)), 0);
        if (name[0] == '.') continue;
        const comm = readSmall(&path, &buf, name, "comm") orelse continue;
        if (!std.mem.startsWith(u8, comm, "iou-wrk")) continue;
        out.workers += 1;
        const wchan = readSmall(&path, &buf, name, "wchan") orelse continue;
        seen.noteWchan(wchan);
        if (!workerNotBlocked(wchan)) out.blocked += 1;
    }
    return out;
}

fn readSmall(path: *[64]u8, buf: *[64]u8, tid: []const u8, file: []const u8) ?[]const u8 {
    const p = std.fmt.bufPrintSentinel(path, "/proc/self/task/{s}/{s}", .{ tid, file }, 0) catch return null;
    const fd = std.c.open(p, .{ .ACCMODE = .RDONLY });
    if (fd < 0) return null;
    defer _ = std.c.close(fd);
    const n = std.c.read(fd, buf, buf.len);
    if (n <= 0) return null;
    return std.mem.trimEnd(u8, buf[0..@intCast(n)], "\n");
}

fn osSleepMs(n: u64) void {
    starh2.test_sleep.threadSleepMs(n);
}

/// The client side, on a blocking-pool thread (it sleeps and blocks; a zio
/// executor thread must not).
fn attack(gpa: std.mem.Allocator, port: u16, seen: *Seen) !void {
    var wires: [connections]*Wire = undefined;
    var opened: usize = 0;
    defer for (wires[0..opened]) |w| w.close();
    for (&wires) |*slot| {
        const w = try Wire.connectRecvBuf(gpa, .tls, port, 4096);
        errdefer w.close();
        try w.handshake();
        var out = try h2c.buildClientPrefaceAndSettings(gpa);
        defer out.deinit(gpa);
        try h2c.appendWindowUpdate(gpa, &out, 0, 1 << 24);
        try w.send(out.items);
        slot.* = w;
        opened += 1;
    }
    // The server's SETTINGS arrived during the handshake round trips; ack it
    // (a missing ack could end the connection on its own), then ask.
    osSleepMs(50);
    for (wires) |w| {
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(gpa);
        var ack: [frame.FRAME_HEADER_LEN]u8 = undefined;
        _ = try frame.Serializer.settingsFrame(&ack, true, &.{});
        try out.appendSlice(gpa, &ack);
        for (0..streams_per_connection) |i| try h2c.appendHeaders(gpa, &out, @intCast(1 + 2 * i), "/big", true);
        try w.send(out.items);
    }
    // Every client's 4 KiB buffer fills, then the server's socket send
    // buffer fills behind it (8 MiB is more than it grows to on loopback)
    // and its sends park.
    const fill_deadline = nowNs() + 10 * std.time.ns_per_s;
    for (wires) |w| while (w.unreadBytes() == 0) {
        if (nowNs() > fill_deadline) return error.ResponsesNeverStarted;
        osSleepMs(5);
    };
    var t = nowNs() + 1500 * ms;
    while (nowNs() < t) {
        seen.blocked_before = @max(seen.blocked_before, sampleWorkers(seen).blocked);
        osSleepMs(2);
    }

    // The attack: a TCP half-close (no close_notify), then never read.
    for (wires) |w| _ = std.c.shutdown(w.fd, std.c.SHUT.WR);
    const t0 = nowNs();
    t = t0 + watch_ns;
    while (nowNs() < t) {
        const s = sampleWorkers(seen);
        seen.samples += 1;
        seen.workers_max = @max(seen.workers_max, s.workers);
        seen.blocked_max = @max(seen.blocked_max, s.blocked);
        if (s.blocked > 0) seen.last_blocked_ns = nowNs() - t0;
        osSleepMs(2);
    }
}

fn run(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const io = rt.io();
    const cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert);
    const key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key);
    const a4 = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var limits = starh2.Limits.defaults;
    limits.graceful_drain_timeout_ns = 300 * ms;
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{.{ .tls = .{ .address = a4 } }},
        .routes = &routes,
        .tls = .{ .certificate_chain_pem = cert, .private_key_pem = key },
        .limits = limits,
    });
    var handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_deadline.join(&server, &handle);
        server.deinit(gpa);
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);

    // The engine zio's Linux backend chose at run time: `auto` falls back to
    // epoll when io_uring ring setup fails, and epoll has no io-wq.
    const engine = rt.main_executor.loop.backend.selectedEngine();
    if (engine != .io_uring) {
        std.debug.print("T3887 FAIL: zio runs the {s} engine, not io_uring (ring setup failed on this host, or a forced backend); this test needs io_uring to see io-wq at all\n", .{@tagName(engine)});
        return error.NotIoUring;
    }

    var seen: Seen = .{};
    var h = try zio.spawnBlocking(attack, .{ gpa, server.localAddress(0).getPort(), &seen });
    try h.join();
    const live_after = server.active_connections.load(.acquire);

    std.debug.print("T3887 engine={s} connections={d} samples={d} workers_max={d} blocked_before={d} blocked_max={d} last_blocked_ms={?d} live_after_watch={d} wchans=", .{
        @tagName(engine),                               connections, seen.samples, seen.workers_max, seen.blocked_before, seen.blocked_max,
        if (seen.last_blocked_ns) |n| n / ms else null, live_after,
    });
    seen.printWchans();
    std.debug.print("\n", .{});
    // A send blocks in io-wq only after the peer's FIN; before it, none may.
    try std.testing.expectEqual(@as(usize, 0), seen.blocked_before);
    // The server ended every half-closed connection, whatever the kernel did.
    if (live_after != 0) {
        std.debug.print("T3887 FAIL: {d} of {d} half-closed connections still live {d} ms after the half-close\n", .{ live_after, connections, watch_ns / ms });
        return error.HalfClosedConnectionsLive;
    }
    if (seen.workers_max == 0) {
        std.debug.print("T3887 FAIL: io_uring, but no iou-wrk thread in any sample: the sampler is blind, or this kernel no longer hands these sends to io-wq (then retire this test, t-3863)\n", .{});
        return error.NoIoWqWorkerSeen;
    }
    if (seen.blocked_max == 0) {
        std.debug.print("T3887 SKIP: io_uring with io-wq workers, but none ever blocked: this kernel does not hand these sends to io-wq, so the bound is untested here\n", .{});
        return error.SkipZigTest;
    }
    if (seen.last_blocked_ns.? > bound_ns) {
        std.debug.print("T3887 FAIL: a worker was still blocked {d} ms after the half-close (bound {d} ms)\n", .{ seen.last_blocked_ns.? / ms, bound_ns / ms });
        return error.WorkerHeldPastBound;
    }
}

test "io-wq: half-closed TLS HTTP/2 clients that never read hold no kernel thread past the bound" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(run, .{ rt, gpa });
    try h.join();
}
