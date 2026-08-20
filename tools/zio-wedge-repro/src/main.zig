//! Minimal reproducer for the t-866 lost-wake wedge, outside starh2.
//!
//! It reruns starh2's exact wake protocol with no TLS and no HTTP: per
//! connection, a reader task does blocking socket reads and posts chunks
//! (put, then dirty flag, then Event.set); a pump task services two queues
//! with tryGet and parks on its Event only after reset + re-tryGet + a
//! dirty-flag swap; an actor task consumes decoded requests, enqueues
//! responses back to the pump, and parks the same way on its own Event.
//! `tools/zio-migration-repro` uses blocking getOne everywhere and never
//! reproduced; this protocol is the ingredient it lacks.
//!
//! An OS-thread watchdog (never a zio task: near the wedge, in-runtime
//! watchdogs are victims too) checks reply progress every 500 ms. On a
//! stall it dumps every connection's sites, Event states, dirty flags and
//! task state tags, then exits 2. The two oracle states it looks for:
//!   - a task tag .waiting while its Event reads .is_set (lost futex wake)
//!   - a reader parked in the socket read while the peer has bytes
//!     outstanding (lost loop completion)
const std = @import("std");
const zio = @import("zio");

extern "c" fn nanosleep(req: *const std.c.timespec, rem: ?*std.c.timespec) c_int;

const Config = struct {
    rounds: usize = 20,
    connections: usize = 1,
    requests_per_connection: usize = 30_000,
    pipeline: usize = 8,
    executors: u8 = 2,
    migration: bool = true,
    stall_ticks: u32 = 6, // 500 ms each
};

const chunk_slots = 8;
const chunk_size = 4096;

const Chunk = struct {
    slot: u8 = 0,
    len: usize = 0,
};

/// One byte of work per request, like the probe's oneshot bytes.
const request_byte: u8 = 0x5a;

const Conn = struct {
    io: std.Io,
    stream: std.Io.net.Stream,

    // reader -> pump
    work: std.Io.Queue(Chunk) = undefined,
    // pump -> actor (request counts)
    inbound: std.Io.Queue(usize) = undefined,
    // actor -> pump (response counts)
    outbound: std.Io.Queue(usize) = undefined,
    free_slots: std.Io.Queue(u8) = undefined,

    pump_wake: std.Io.Event = .unset,
    actor_wake: std.Io.Event = .unset,
    pump_dirty: std.atomic.Value(bool) = .init(false),
    actor_dirty: std.atomic.Value(bool) = .init(false),

    // Diag: 0 not started, 1 running, 2 event wait, 3 socket read,
    // 4 blocking put, 5 exited.
    reader_site: std.atomic.Value(u8) = .init(0),
    pump_site: std.atomic.Value(u8) = .init(0),
    actor_site: std.atomic.Value(u8) = .init(0),
    reader_task: std.atomic.Value(usize) = .init(0),
    pump_task: std.atomic.Value(usize) = .init(0),
    actor_task: std.atomic.Value(usize) = .init(0),

    work_buf: [chunk_slots]Chunk = undefined,
    inbound_buf: [chunk_slots]usize = undefined,
    outbound_buf: [chunk_slots]usize = undefined,
    free_buf: [chunk_slots]u8 = undefined,
    storage: [chunk_slots][chunk_size]u8 = undefined,
};

fn taskHandle() usize {
    if (comptime @hasDecl(zio, "debugCurrentTaskHandle")) {
        return zio.debugCurrentTaskHandle();
    }
    return 0;
}

fn taskTag(h: usize) u8 {
    if (comptime @hasDecl(zio, "debugTaskStateByte")) {
        if (h != 0) return zio.debugTaskStateByte(h);
    }
    return 0xff;
}

fn tryGet(comptime T: type, q: *std.Io.Queue(T), io: std.Io) ?T {
    var one: [1]T = undefined;
    const n = q.getUncancelable(io, &one, 0) catch return null;
    return if (n == 1) one[0] else null;
}

fn readerTask(conn: *Conn) std.Io.Cancelable!void {
    const io = conn.io;
    conn.reader_task.store(taskHandle(), .release);
    defer conn.reader_site.store(5, .release);
    var reader = conn.stream.reader(io, &.{});
    while (true) {
        conn.reader_site.store(1, .release);
        const slot = tryGet(u8, &conn.free_slots, io) orelse blk: {
            conn.reader_site.store(4, .release);
            break :blk conn.free_slots.getOne(io) catch return;
        };
        var dest: [1][]u8 = .{&conn.storage[slot]};
        conn.reader_site.store(3, .release);
        const n = reader.interface.readVec(&dest) catch {
            conn.free_slots.putOneUncancelable(io, slot) catch {};
            conn.work.putOneUncancelable(io, .{}) catch {};
            conn.pump_dirty.store(true, .release);
            conn.pump_wake.set(io);
            return;
        };
        conn.reader_site.store(4, .release);
        conn.work.putOne(io, .{ .slot = slot, .len = n }) catch return;
        conn.pump_dirty.store(true, .release);
        conn.pump_wake.set(io);
    }
}

fn pumpTask(conn: *Conn) std.Io.Cancelable!void {
    const io = conn.io;
    conn.pump_task.store(taskHandle(), .release);
    defer conn.pump_site.store(5, .release);
    var writer = conn.stream.writer(io, &.{});
    var response: [chunk_size]u8 = undefined;
    @memset(&response, request_byte);
    while (true) {
        conn.pump_site.store(1, .release);
        var progress = false;

        if (tryGet(Chunk, &conn.work, io)) |chunk| {
            if (chunk.len == 0) return; // reader EOF
            // Count request bytes, hand the count to the actor.
            conn.pump_site.store(4, .release);
            conn.inbound.putOne(io, chunk.len) catch return;
            conn.actor_dirty.store(true, .release);
            conn.actor_wake.set(io);
            conn.free_slots.putOneUncancelable(io, chunk.slot) catch {};
            progress = true;
        }
        if (tryGet(usize, &conn.outbound, io)) |count| {
            // Write `count` response bytes to the socket.
            conn.pump_site.store(3, .release);
            writer.interface.writeAll(response[0..count]) catch return;
            writer.interface.flush() catch return;
            progress = true;
        }
        if (progress) continue;

        conn.pump_wake.reset();
        if (tryGet(Chunk, &conn.work, io)) |chunk| {
            if (chunk.len == 0) return;
            conn.pump_site.store(4, .release);
            conn.inbound.putOne(io, chunk.len) catch return;
            conn.actor_dirty.store(true, .release);
            conn.actor_wake.set(io);
            conn.free_slots.putOneUncancelable(io, chunk.slot) catch {};
            continue;
        }
        if (tryGet(usize, &conn.outbound, io)) |count| {
            conn.pump_site.store(3, .release);
            writer.interface.writeAll(response[0..count]) catch return;
            writer.interface.flush() catch return;
            continue;
        }
        if (conn.pump_dirty.swap(false, .acq_rel)) continue;
        conn.pump_site.store(2, .release);
        try conn.pump_wake.wait(io);
    }
}

const Activity = union(enum) {
    actor: std.Io.Cancelable!void,
    timer: std.Io.Cancelable!void,
};

fn waitEvent(event: *std.Io.Event, io: std.Io) std.Io.Cancelable!void {
    return event.wait(io);
}

fn waitTimer(timeout: std.Io.Timeout, io: std.Io) std.Io.Cancelable!void {
    return timeout.sleep(io);
}

/// starh2's `waitForActivity` shape: park through a Select whose arms are
/// a concurrent Event waiter and a timer, not a bare `Event.wait`. The
/// wedged actor sat parked exactly here.
fn actorPark(conn: *Conn) (std.Io.ConcurrentError || std.Io.Cancelable)!void {
    const io = conn.io;
    var result_buf: [2]Activity = undefined;
    var select = std.Io.Select(Activity).init(io, &result_buf);
    errdefer select.cancelDiscard();
    try select.concurrent(.actor, waitEvent, .{ &conn.actor_wake, io });
    const timeout: std.Io.Timeout = .{ .deadline = .{
        .raw = .fromNanoseconds(@intCast(nowNs(io) +% 5 * std.time.ns_per_s)),
        .clock = .awake,
    } };
    try select.concurrent(.timer, waitTimer, .{ timeout, io });
    const selected = try select.await();
    defer select.cancelDiscard();
    switch (selected) {
        .actor, .timer => |result| try result,
    }
}

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn actorTask(conn: *Conn) (std.Io.ConcurrentError || std.Io.Cancelable)!void {
    const io = conn.io;
    conn.actor_task.store(taskHandle(), .release);
    defer conn.actor_site.store(5, .release);
    while (true) {
        conn.actor_site.store(1, .release);
        var maybe = tryGet(usize, &conn.inbound, io);
        if (maybe == null) {
            conn.actor_wake.reset();
            maybe = tryGet(usize, &conn.inbound, io);
            if (maybe == null and !conn.actor_dirty.swap(false, .acq_rel)) {
                conn.actor_site.store(2, .release);
                try actorPark(conn);
                continue;
            }
            if (maybe == null) continue;
        }
        const count = maybe.?;
        // "Handle" the batch: respond with the same number of bytes.
        conn.actor_site.store(4, .release);
        conn.outbound.putOne(io, count) catch return;
        conn.pump_dirty.store(true, .release);
        conn.pump_wake.set(io);
    }
}

const Progress = struct {
    replies: std.atomic.Value(u64) = .init(0),
    done: std.atomic.Value(bool) = .init(false),
};

var g_conns_lock: std.atomic.Value(bool) = .init(false);
var g_conns: [64]?*Conn = @splat(null);
var g_progress: Progress = .{};
var g_stall_ticks: u32 = 6;
var g_listening: std.atomic.Value(bool) = .init(false);
var g_accepts: std.atomic.Value(u64) = .init(0);
var g_client_connects: std.atomic.Value(u64) = .init(0);

fn connsLock() void {
    while (g_conns_lock.swap(true, .acquire)) std.atomic.spinLoopHint();
}
fn connsUnlock() void {
    g_conns_lock.store(false, .release);
}

fn registerConn(c: *Conn) void {
    connsLock();
    defer connsUnlock();
    for (&g_conns) |*slot| {
        if (slot.* == null) {
            slot.* = c;
            return;
        }
    }
}

fn deregisterConn(c: *Conn) void {
    connsLock();
    defer connsUnlock();
    for (&g_conns) |*slot| {
        if (slot.* == c) slot.* = null;
    }
}

fn rawPrint(comptime fmt: []const u8, args: anytype) void {
    var buf: [512]u8 = undefined;
    const line = std.fmt.bufPrint(&buf, fmt, args) catch return;
    _ = std.c.write(2, line.ptr, line.len);
}

/// OS thread. Detects a reply stall and dumps every live connection.
fn watchdogMain() void {
    var last: u64 = 0;
    var quiet: u32 = 0;
    while (!g_progress.done.load(.acquire)) {
        var req = std.c.timespec{ .sec = 0, .nsec = 500 * std.time.ns_per_ms };
        _ = nanosleep(&req, null);
        const now = g_progress.replies.load(.acquire);
        if (now != last) {
            last = now;
            quiet = 0;
            continue;
        }
        quiet += 1;
        if (quiet < g_stall_ticks) continue;
        rawPrint(
            "\nSTALL: no replies for {d} ticks (completed={d} listening={} accepts={d} client_connects={d})\n",
            .{
                quiet,
                now,
                g_listening.load(.acquire),
                g_accepts.load(.acquire),
                g_client_connects.load(.acquire),
            },
        );
        connsLock();
        for (g_conns) |maybe| {
            const c = maybe orelse continue;
            rawPrint(
                "conn={x} reader(site={d},tag={d}) pump(site={d},tag={d},wake={s},dirty={}) actor(site={d},tag={d},wake={s},dirty={})\n",
                .{
                    @intFromPtr(c) & 0xffff,
                    c.reader_site.load(.acquire),
                    taskTag(c.reader_task.load(.acquire)) & 0xf,
                    c.pump_site.load(.acquire),
                    taskTag(c.pump_task.load(.acquire)) & 0xf,
                    @tagName(@atomicLoad(std.Io.Event, &c.pump_wake, .acquire)),
                    c.pump_dirty.load(.acquire),
                    c.actor_site.load(.acquire),
                    taskTag(c.actor_task.load(.acquire)) & 0xf,
                    @tagName(@atomicLoad(std.Io.Event, &c.actor_wake, .acquire)),
                    c.actor_dirty.load(.acquire),
                },
            );
        }
        connsUnlock();
        std.process.exit(2);
    }
}

fn serveConnection(io: std.Io, stream: std.Io.net.Stream) !void {
    defer stream.close(io);
    var conn: Conn = .{ .io = io, .stream = stream };
    conn.work = .init(&conn.work_buf);
    conn.inbound = .init(&conn.inbound_buf);
    conn.outbound = .init(&conn.outbound_buf);
    conn.free_slots = .init(&conn.free_buf);
    for (0..chunk_slots) |slot| try conn.free_slots.putOne(io, @intCast(slot));
    registerConn(&conn);
    defer deregisterConn(&conn);

    var reader = try io.concurrent(readerTask, .{&conn});
    defer reader.cancel(io) catch {};
    var actor = try io.concurrent(actorTask, .{&conn});
    defer actor.cancel(io) catch {};
    // The pump runs on this task, like starh2's pump owns its lifetime.
    try pumpTask(&conn);
}

fn serveConnectionEntry(io: std.Io, stream: std.Io.net.Stream) std.Io.Cancelable!void {
    serveConnection(io, stream) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => rawPrint("connection: {s}\n", .{@errorName(err)}),
    };
}

fn serverTask(io: std.Io, port_out: *zio.Channel(u16), total_connections: usize) !void {
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try address.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    try port_out.send(server.socket.address.getPort());
    g_listening.store(true, .release);

    var connections: std.Io.Group = .init;
    defer connections.cancel(io);
    for (0..total_connections) |_| {
        const stream = try server.accept(io);
        _ = g_accepts.fetchAdd(1, .monotonic);
        try connections.concurrent(io, serveConnectionEntry, .{ io, stream });
    }
    try connections.await(io);
}

/// Clients run on RAW OS THREADS with blocking BSD sockets, never as zio
/// tasks. In-process zio clients keep the executors busy, and the lost-wake
/// window opens when an executor parks in kevent; an out-of-process-style
/// client is what lets the server runtime go idle between bursts, which is
/// the starh2 shape (Go client / h2load).
fn clientThread(port: u16, cfg: Config) void {
    clientThreadInner(port, cfg) catch |err| {
        rawPrint("client: {s}\n", .{@errorName(err)});
        std.process.exit(3);
    };
}

fn clientThreadInner(port: u16, cfg: Config) !void {
    const fd = std.c.socket(std.c.AF.INET, std.c.SOCK.STREAM, 0);
    if (fd < 0) return error.SocketFailed;
    defer _ = std.c.close(fd);
    var addr: std.c.sockaddr.in = .{
        .family = std.c.AF.INET,
        .port = std.mem.nativeToBig(u16, port),
        .addr = std.mem.nativeToBig(u32, 0x7f000001),
        .zero = @splat(0),
    };
    if (std.c.connect(fd, @ptrCast(&addr), @sizeOf(std.c.sockaddr.in)) != 0) return error.ConnectFailed;
    _ = g_client_connects.fetchAdd(1, .monotonic);

    var request: [256]u8 = undefined;
    var response: [256]u8 = undefined;
    @memset(&request, request_byte);

    var remaining = cfg.requests_per_connection;
    while (remaining != 0) {
        const n = @min(remaining, cfg.pipeline);
        var sent: usize = 0;
        while (sent < n) {
            const w = std.c.write(fd, request[sent..].ptr, n - sent);
            if (w <= 0) return error.WriteFailed;
            sent += @intCast(w);
        }
        var got: usize = 0;
        while (got < n) {
            const r = std.c.read(fd, response[got..].ptr, n - got);
            if (r <= 0) return error.ReadFailed;
            got += @intCast(r);
        }
        _ = g_progress.replies.fetchAdd(n, .monotonic);
        remaining -= n;
    }
}

fn run(io: std.Io, cfg: Config) !void {
    var port_buf: [1]u16 = undefined;
    var port_out = zio.Channel(u16).init(&port_buf);
    const total_connections = try std.math.mul(usize, cfg.rounds, cfg.connections);

    var server = try zio.spawn(serverTask, .{ io, &port_out, total_connections });
    defer server.cancel();

    const port = try port_out.receive();
    for (0..cfg.rounds) |round| {
        var threads: [64]?std.Thread = @splat(null);
        for (0..@min(cfg.connections, threads.len)) |i| {
            threads[i] = std.Thread.spawn(.{}, clientThread, .{ port, cfg }) catch null;
        }
        for (&threads) |*t| {
            if (t.*) |th| th.join();
            t.* = null;
        }
        rawPrint(".", .{});
        if ((round + 1) % 20 == 0) rawPrint(" {d} rounds\n", .{round + 1});
    }
    try server.join();
    g_progress.done.store(true, .release);
    rawPrint(
        "\nPASS: {d} replies, migration={any}\n",
        .{ g_progress.replies.load(.acquire), cfg.migration },
    );
}

fn parseArgs(gpa: std.mem.Allocator, process_args: std.process.Args) !Config {
    var cfg: Config = .{};
    var args = try std.process.Args.Iterator.initAllocator(process_args, gpa);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--no-migration")) {
            cfg.migration = false;
        } else if (std.mem.eql(u8, arg, "--rounds")) {
            cfg.rounds = try std.fmt.parseInt(usize, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--connections")) {
            cfg.connections = try std.fmt.parseInt(usize, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--requests")) {
            cfg.requests_per_connection = try std.fmt.parseInt(usize, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--pipeline")) {
            cfg.pipeline = try std.fmt.parseInt(usize, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--executors")) {
            cfg.executors = try std.fmt.parseInt(u8, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--stall-ticks")) {
            cfg.stall_ticks = try std.fmt.parseInt(u32, args.next() orelse return error.MissingValue, 10);
        } else {
            return error.UnknownArgument;
        }
    }
    return cfg;
}

pub fn main(init: std.process.Init) !void {
    const cfg = try parseArgs(init.gpa, init.minimal.args);
    g_stall_ticks = cfg.stall_ticks;
    const runtime = try zio.Runtime.init(init.gpa, .{
        .executors = .exact(cfg.executors),
        .enable_task_migration = cfg.migration,
    });
    defer runtime.deinit();

    rawPrint(
        "zio wedge repro: rounds={d} connections={d} requests={d} pipeline={d} executors={d} migration={any} task_tags={any}\n",
        .{
            cfg.rounds,
            cfg.connections,
            cfg.requests_per_connection,
            cfg.pipeline,
            runtime.executors.items.len,
            cfg.migration,
            comptime @hasDecl(zio, "debugCurrentTaskHandle"),
        },
    );
    const watchdog = try std.Thread.spawn(.{}, watchdogMain, .{});
    watchdog.detach();
    var main_task = try runtime.spawn(run, .{ runtime.io(), cfg });
    try main_task.join();
}
