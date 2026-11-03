//! The shutdown cases that cannot run inside the test process: each ends in a
//! state a test binary cannot leave behind. `build.zig` runs this with one
//! argument and checks what it prints on stderr.
//!
//! - `stuck`: a handler that never ends. `serve` must keep waiting and keep
//!   reporting; after the cancel report, the grace report and two interval
//!   reports this process exits on its own with `serve` still running (the
//!   test's "kill"), printing whether `serve` had returned.
//! - `deinit-live`: `deinit` on a server that still counts a task handler
//!   must report it and free nothing.
//! - `deinit-idle`: `deinit` on a server whose `serve` still runs with
//!   nothing charged (no connection yet) must free nothing either: the
//!   server then still accepts and serves a request.
//! - `underflow`: a release of a counter at zero is refused, counted and
//!   logged, and the counter stays at zero.
//! - `accounts-underflow`: a client account released twice is refused,
//!   counted (`underflows`) and logged; a record reference given back twice
//!   is refused, counted (`stale`: its record's generation moved when it was
//!   freed) and logged; the record is freed once, not twice (the free list
//!   would overflow its array), and a reference that outlived its record does
//!   not touch the record's next key. On d33e615 this case does not exist (no
//!   client accounts).
//! - `accounts-release-race`: two releases of one reference both take the
//!   locked path, and an acquire reuses the record between them; the second
//!   is refused (`stale`) and the new key's record stays, with its holder.
//!   On 8eb220f the second removed the new key's live record. Then a release
//!   and a retain through the same stale reference, on the lock-free paths
//!   (the new key has two holders), are refused too.
//! - `accounts-quarantine`: a record whose last reference goes with two counts
//!   not zero is quarantined once, and one log line names both counts. On
//!   8eb220f it counted two quarantines.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");

const dummy: u8 = 0;
var started: std.atomic.Value(bool) = .init(false);

/// Ignores cancel: each canceled sleep is swallowed and the next one waits
/// in full, so it never ends and never spins.
fn stuck(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    _ = &body;
    started.store(true, .release);
    while (true) zio.sleep(.fromMilliseconds(20)) catch {};
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/stuck", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = stuck } } },
};

const drain_ms = 200;
const grace_ms = 300;
const interval_ms = 300;

fn clientMain(port: u16, release: *std.atomic.Value(bool)) void {
    const gpa = std.heap.page_allocator;
    const w = wire_client.Wire.open(gpa, .h2c, port) catch return;
    defer w.close();
    var req = h2c.buildClientPrefaceAndSettings(gpa) catch return;
    defer req.deinit(gpa);
    h2c.appendHeaders(gpa, &req, 1, "/stuck", true) catch return;
    w.send(req.items) catch return;
    while (!release.load(.acquire)) {
        const t: std.c.timespec = .{ .sec = 0, .nsec = 5 * std.time.ns_per_ms };
        _ = std.c.nanosleep(&t, null);
    }
}

fn runStuck(rt: *zio.Runtime) anyerror!void {
    // The page allocator: this process exits with the server live, on purpose.
    const gpa = std.heap.page_allocator;
    var limits = starh2.Limits.defaults;
    limits.graceful_drain_timeout_ns = drain_ms * std.time.ns_per_ms;
    limits.shutdown_cancel_grace_ns = grace_ms * std.time.ns_per_ms;
    limits.shutdown_report_interval_ns = interval_ms * std.time.ns_per_ms;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }},
        .routes = &routes,
        .tls = null,
        .limits = limits,
    });
    var handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    var release: std.atomic.Value(bool) = .init(false);
    const t = try std.Thread.spawn(.{}, clientMain, .{ server.localAddress(0).getPort(), &release });
    t.detach();
    while (!started.load(.acquire)) try zio.sleep(.fromMilliseconds(5));
    server.requestShutdown();
    const t0 = zio.Timestamp.now(.monotonic).toNanoseconds();
    // The first phase (1 s), the drain, the grace, two intervals, and room.
    const wait_ms: u64 = 1000 + drain_ms + grace_ms + 2 * interval_ms + 250;
    while (server.shutdown_reports.load(.acquire) < 4) {
        if (zio.Timestamp.now(.monotonic).toNanoseconds() - t0 > (wait_ms + 2000) * std.time.ns_per_ms) break;
        try zio.sleep(.fromMilliseconds(10));
    }
    const ms = (zio.Timestamp.now(.monotonic).toNanoseconds() - t0) / std.time.ns_per_ms;
    std.debug.print("child: {d} ms after shutdown, serve returned={}, reports={d}, last report counted {d} task handlers\n", .{
        ms, handle.hasResult(), server.shutdown_reports.load(.acquire), server.shutdown_report_handlers.load(.acquire),
    });
    if (!handle.hasResult() and server.shutdown_reports.load(.acquire) >= 4 and server.shutdown_report_handlers.load(.acquire) == 1) {
        std.debug.print("child: serve still waiting for the stuck handler\n", .{});
    }
    // The kill: leave with `serve` and the handler running.
    std.process.exit(0);
}

/// A handler that holds request memory and keeps writing to it.
var held_touches: std.atomic.Value(u64) = .init(0);

fn holder(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    _ = &body;
    // From the request pool, through the request's arena.
    const mem = try req.arena.alloc(u8, 256 * 1024);
    started.store(true, .release);
    var i: u8 = 0;
    while (true) {
        @memset(mem, i);
        i +%= 1;
        _ = held_touches.fetchAdd(1, .monotonic);
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

const holder_routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/stuck", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = holder } } },
};

/// `deinit` called while `serve` runs, with a handler that holds 256 KiB of
/// request-pool memory and keeps writing to it. `deinit` must free nothing:
/// the handler goes on writing, and the process ends cleanly.
fn runDeinitLive(rt: *zio.Runtime) anyerror!void {
    const gpa = std.heap.page_allocator;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }},
        .routes = &holder_routes,
        .tls = null,
        .limits = .defaults,
    });
    _ = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    var release: std.atomic.Value(bool) = .init(false);
    const t = try std.Thread.spawn(.{}, clientMain, .{ server.localAddress(0).getPort(), &release });
    t.detach();
    while (!started.load(.acquire)) try zio.sleep(.fromMilliseconds(5));
    const handlers = server.accounting.task_handlers.load(.acquire);
    server.deinit(gpa);
    // The handler still writes its request memory after `deinit`.
    const before = held_touches.load(.acquire);
    try zio.sleep(.fromMilliseconds(200));
    const after = held_touches.load(.acquire);
    std.debug.print("child: deinit returned with {d} task handlers live; the handler wrote its request memory {d} times after it\n", .{ handlers, after - before });
    if (after > before) std.debug.print("child: request memory still live after deinit\n", .{});
    std.process.exit(0);
}

/// astra 6: `deinit` while `serve` runs and no counter is charged. The
/// listeners and the accept loops are still in use, so
/// `deinit` must free nothing; the server then serves a request.
fn runDeinitIdle(rt: *zio.Runtime) anyerror!void {
    const gpa = std.heap.page_allocator;
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }},
        .routes = &ok_routes,
        .tls = null,
        .limits = .defaults,
    });
    _ = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();
    server.deinit(gpa);
    std.debug.print("child: deinit returned with serve running and nothing charged\n", .{});
    var ok: std.atomic.Value(bool) = .init(false);
    var done: std.atomic.Value(bool) = .init(false);
    const t = try std.Thread.spawn(.{}, okClient, .{ port, &ok, &done });
    // Poll, never a blocking join here: it would stall this zio executor
    // and every connection placed on it.
    while (!done.load(.acquire)) try zio.sleep(.fromMilliseconds(5));
    t.join();
    if (ok.load(.acquire)) std.debug.print("child: served a request after deinit\n", .{});
    std.process.exit(0);
}

fn okHandler(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const ok_routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/ok", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = okHandler } } },
};

fn okClient(port: u16, ok: *std.atomic.Value(bool), done: *std.atomic.Value(bool)) void {
    defer done.store(true, .release);
    const gpa = std.heap.page_allocator;
    const w = wire_client.Wire.open(gpa, .h2c, port) catch return;
    defer w.close();
    var req = h2c.buildClientPrefaceAndSettings(gpa) catch return;
    defer req.deinit(gpa);
    h2c.appendHeaders(gpa, &req, 1, "/ok", true) catch return;
    w.send(req.items) catch return;
    const deadline = wire_client.nowNs() + 3 * std.time.ns_per_s;
    while (true) {
        const f = (w.next(deadline) catch return) orelse return;
        if (f.sid == 1 and f.flags & 1 != 0) {
            ok.store(true, .release);
            return;
        }
    }
}

fn runUnderflow(_: *zio.Runtime) anyerror!void {
    var a: starh2.edge.connection.GlobalAccounting = .{ .max_streams = 1, .task_handler_capacity = 1, .max_outbound_bytes = 1, .max_handshakes = 1 };
    a.releaseStream(.none);
    a.releaseOutbound(.none, 7);
    std.debug.print("child: active_streams={d} outbound_bytes={d} underflows={d}\n", .{ a.active_streams.load(.acquire), a.outbound_bytes.load(.acquire), a.underflows.load(.acquire) });
}

fn runAccountsUnderflow(_: *zio.Runtime) anyerror!void {
    const accounts = starh2.edge.accounts;
    const gpa = std.heap.page_allocator;
    var t = try accounts.Table.init(gpa, 2, 7);
    defer t.deinit(gpa);
    var kb: [accounts.max_refs]accounts.Key = undefined;
    const a = (try t.acquire(accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 1 } }, 48, &kb))).?;
    a.add(.streams, 1);
    a.sub(.streams, 1);
    // The double release of the charge.
    a.sub(.streams, 1);
    t.release(a);
    // The double release of the reference, before the record is reused.
    t.release(a);
    // Both records are still there to take: freed once, not twice.
    var k2: [accounts.max_refs]accounts.Key = undefined;
    var k3: [accounts.max_refs]accounts.Key = undefined;
    const b = (try t.acquire(accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 2 } }, 48, &k2))).?;
    const c = (try t.acquire(accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 3 } }, 48, &k3))).?;
    var k4: [accounts.max_refs]accounts.Key = undefined;
    const full = (try t.acquire(accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 4 } }, 48, &k4))) == null;
    b.add(.streams, 1);
    // `a`'s record now holds 10.0.0.2 (or 10.0.0.3): a release through the
    // stale reference is refused and leaves the new key's counts alone.
    a.sub(.streams, 1);
    t.release(a);
    const eb = t.lookup(k2[0]).?;
    const s = t.stats();
    std.debug.print("child: accounts underflows={d} stale={d} removals={d} full_after_two={} b.streams={d} b.holders={d}\n", .{ s.underflows, s.stale, s.removals, full, eb.count(.streams), eb.holders });
    b.sub(.streams, 1);
    t.release(b);
    t.release(c);
}

fn runAccountsReleaseRace(_: *zio.Runtime) anyerror!void {
    const accounts = starh2.edge.accounts;
    const gpa = std.heap.page_allocator;
    var t = try accounts.Table.init(gpa, 2, 7);
    defer t.deinit(gpa);
    var ka: [accounts.max_refs]accounts.Key = undefined;
    const a = (try t.acquire(accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 1 } }, 48, &ka))).?;
    // Two releases of `a`'s one reference race: both read one holder, so
    // both take the locked path (`Table.release`). The first wins the lock
    // and removes the record; an acquire then takes the lock before the
    // second and reuses the record for a new key; then the second runs its
    // locked step. The steps, in that order:
    t.release(a);
    var kb: [accounts.max_refs]accounts.Key = undefined;
    const b = (try t.acquire(accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 2 } }, 48, &kb))).?;
    const same_record = b.refs[0].index == a.refs[0].index;
    t.releaseLast(a.refs[0]);
    const after_locked = if (t.lookup(kb[0])) |e| e.holders else 0;
    // The lock-free paths, through the same stale reference: with two
    // holders on the new key, a release of `a` is a lock-free decrement and
    // must be refused too; so must a retain.
    if (after_locked == 1) t.retain(b);
    t.release(a);
    t.retain(a);
    const eb = t.lookup(kb[0]);
    const s = t.stats();
    std.debug.print("child: release race same_record={} after_locked={d} b.live={} b.holders={d} stale={d} underflows={d} removals={d}\n", .{ same_record, after_locked, eb != null, if (eb) |e| e.holders else 0, s.stale, s.underflows, s.removals });
    if (eb) |e| {
        for (0..e.holders) |_| t.release(b);
    }
}

fn runAccountsQuarantine(_: *zio.Runtime) anyerror!void {
    const accounts = starh2.edge.accounts;
    const gpa = std.heap.page_allocator;
    var t = try accounts.Table.init(gpa, 2, 7);
    defer t.deinit(gpa);
    var ka: [accounts.max_refs]accounts.Key = undefined;
    const a = (try t.acquire(accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 1 } }, 48, &ka))).?;
    // Two counts a path forgot to release: one record, quarantined once.
    a.add(.streams, 1);
    a.add(.connections, 1);
    t.release(a);
    const s = t.stats();
    std.debug.print("child: quarantine live={d} quarantined={d} removals={d}\n", .{ s.live, s.quarantined, s.removals });
}

fn runProxyTrustedRange(rt: *zio.Runtime) anyerror!void {
    const gpa = std.heap.page_allocator;
    const wide = [_]starh2.edge.proxy_protocol.Cidr{try starh2.edge.proxy_protocol.Cidr.parse("::/0")};
    const ep: starh2.EndpointConfig = .{ .h2c_prior_knowledge = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0), .client_identity = .proxy_source, .proxy_protocol = .{ .v2 = .{ .trusted = &wide } } } };
    if (starh2.Server.init(gpa, rt.io(), .{ .endpoints = &.{ep}, .routes = &.{}, .tls = null })) |srv| {
        var s = srv;
        s.deinit(gpa);
        std.debug.print("child: trusted range ::/0 booted\n", .{});
    } else |err| std.debug.print("child: trusted range ::/0 {s}\n", .{@errorName(err)});
}

fn runAccountsChargeRace(_: *zio.Runtime) anyerror!void {
    const accounts = starh2.edge.accounts;
    const gpa = std.heap.page_allocator;
    var t = try accounts.Table.init(gpa, 1, 7);
    defer t.deinit(gpa);
    var ka: [accounts.max_refs]accounts.Key = undefined;
    const a = (try t.acquire(accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 1 } }, 48, &ka))).?;
    // A charge through `a` checks its record's generation (`Charge.add` and
    // `sub` do that first) while `a` still holds it. Then `a` is released,
    // and an acquire reuses the record for a new key, before the charge's
    // add and sub land (`Table.addAt`, `subAt`). The steps, in that order:
    const rec = t.record(a.refs[0]).?;
    t.release(a);
    var kb: [accounts.max_refs]accounts.Key = undefined;
    const b = (try t.acquire(accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 2 } }, 48, &kb))).?;
    const same_record = b.refs[0].index == a.refs[0].index;
    b.add(.request_bytes, 10);
    t.addAt(rec, a.refs[0], .streams, 5);
    t.subAt(rec, a.refs[0], .request_bytes, 4);
    const eb = t.lookup(kb[0]).?;
    const s = t.stats();
    std.debug.print("child: charge race same_record={} b.streams={d} b.request_bytes={d} stale={d} underflows={d}\n", .{ same_record, eb.count(.streams), eb.count(.request_bytes), s.stale, s.underflows });
    // Leave `b` clean, whichever code ran.
    if (eb.count(.streams) != 0) b.sub(.streams, eb.count(.streams));
    b.sub(.request_bytes, t.lookup(kb[0]).?.count(.request_bytes));
    t.release(b);
}

fn runAccountsGenerationWrap(_: *zio.Runtime) anyerror!void {
    const accounts = starh2.edge.accounts;
    const gpa = std.heap.page_allocator;
    var t = try accounts.Table.init(gpa, 1, 7);
    defer t.deinit(gpa);
    var k1: [accounts.max_refs]accounts.Key = undefined;
    const key1 = accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 1 } }, 48, &k1);
    // The oldest reference to the one record: generation 0.
    const a0 = (try t.acquire(key1)).?;
    t.release(a0);
    // Two removals short of 2^32: then cycle the record until it can no
    // longer be taken, at most three times.
    t.forceGenerationForTest(0, std.math.maxInt(u32) - 2);
    var cycles: usize = 0;
    while (cycles < 3) : (cycles += 1) {
        const c = (try t.acquire(key1)) orelse break;
        t.release(c);
    }
    var k2: [accounts.max_refs]accounts.Key = undefined;
    const key2 = accounts.keysFor(.{ .ip4 = .{ 10, 0, 0, 2 } }, 48, &k2);
    const b = try t.acquire(key2);
    // The generation-0 reference, given back again: it must never match.
    t.release(a0);
    const s = t.stats();
    std.debug.print("child: generation wrap cycles={d} b.acquired={} b.live={} stale={d} insert_failures={d}\n", .{ cycles, b != null, t.lookup(k2[0]) != null, s.stale, s.insert_failures });
    if (t.lookup(k2[0]) != null) t.release(b.?);
}

pub fn main(init: std.process.Init.Minimal) !void {
    var it = std.process.Args.Iterator.init(init.args);
    _ = it.next();
    const case = it.next() orelse return error.NoCase;
    const rt = try zio.Runtime.init(std.heap.page_allocator, .{ .executors = .exact(2) });
    var h = if (std.mem.eql(u8, case, "stuck"))
        try rt.spawn(runStuck, .{rt})
    else if (std.mem.eql(u8, case, "deinit-live"))
        try rt.spawn(runDeinitLive, .{rt})
    else if (std.mem.eql(u8, case, "deinit-idle"))
        try rt.spawn(runDeinitIdle, .{rt})
    else if (std.mem.eql(u8, case, "underflow"))
        try rt.spawn(runUnderflow, .{rt})
    else if (std.mem.eql(u8, case, "accounts-underflow"))
        try rt.spawn(runAccountsUnderflow, .{rt})
    else if (std.mem.eql(u8, case, "accounts-release-race"))
        try rt.spawn(runAccountsReleaseRace, .{rt})
    else if (std.mem.eql(u8, case, "accounts-quarantine"))
        try rt.spawn(runAccountsQuarantine, .{rt})
    else if (std.mem.eql(u8, case, "proxy-trusted-range"))
        try rt.spawn(runProxyTrustedRange, .{rt})
    else if (std.mem.eql(u8, case, "accounts-charge-race"))
        try rt.spawn(runAccountsChargeRace, .{rt})
    else if (std.mem.eql(u8, case, "accounts-generation-wrap"))
        try rt.spawn(runAccountsGenerationWrap, .{rt})
    else
        return error.UnknownCase;
    try h.join();
}
