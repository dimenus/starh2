//! The executor watchdog (t-3760), on one executor and real sockets.
//!
//! The limit is 200 ms here (the thread samples every 100 ms). A handler
//! that spins without yielding holds the only executor until the client
//! thread sees the watchdog's report counter move, then the client releases
//! it (5 s at most: past that the arm fails, naming what it waited for). So
//! a slow host delays the report instead of losing it. The watchdog must
//! report exactly once (once per stall), with that handler's route. One that
//! loops for three limits calling `zio.yield()` makes scheduler progress and
//! must not be reported, and neither must an idle server (three limits).
//!
//! The spinning handler runs as a task over HTTP/2 and over HTTP/1.1, and as
//! an HTTP/1.1 complete handler, which runs inline on the connection's actor:
//! the three places a handler is labelled. A fourth first naps (it yields;
//! the executor parks), and only after it says so does `/quick` run on
//! another connection and end, leaving no label; then it spins. The executor
//! must show its label again when it resumes (zio republishes it at every
//! switch), so its stall is still named.
//!
//! A second test runs two servers on one runtime: one watchdog (the first
//! server's), the second server's start counted as refused, and a stall in
//! the second server's handler reported once, as that server's.
//!
//! This file needs the test hooks (`watchdog.test_reports`): its build step
//! compiles starh2 with them on in every optimize mode, and it does not
//! compile without them.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");
const serve_deadline = @import("serve_deadline.zig");

const watchdog = starh2.edge.watchdog;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;
const ms = std.time.ns_per_ms;
const dummy: u8 = 0;

comptime {
    if (@TypeOf(watchdog.test_reports) == void) @compileError("tests/watchdog.zig needs starh2's test hooks (observe); build it with `test-watchdog`");
}

const limit_ms = 200;
/// The negative arms run this long: three limits.
const quiet_ms = 3 * limit_ms;
/// The most a positive arm waits for its report.
const report_deadline_ms = 5000;

/// Set by a client once it has seen its report; the spinning handler
/// returns then.
var release: std.atomic.Value(bool) = .init(false);
/// `/nap-spin` is napping; `/quick` has run.
var napping: std.atomic.Value(bool) = .init(false);
var quick_ran: std.atomic.Value(bool) = .init(false);

fn resetFlags() void {
    release.store(false, .release);
    napping.store(false, .release);
    quick_ran.store(false, .release);
}

/// Spin, never yielding, until released (or a safety bound far past the
/// client's own deadline, so a broken watchdog fails the test instead of
/// hanging it).
fn spinUntilReleased() void {
    const bound = nowNs() + (report_deadline_ms + 5000) * ms;
    while (!release.load(.acquire) and nowNs() < bound) std.atomic.spinLoopHint();
}

fn spin(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    spinUntilReleased();
    try resp.send(200, &.{}, "spun");
}

fn yielding(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const until = nowNs() + quiet_ms * ms;
    while (nowNs() < until) try zio.yield();
    try resp.send(200, &.{}, "yielded");
}

fn spinComplete(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    spinUntilReleased();
    try resp.send(200, &.{}, "spun");
}

/// Says it is napping, naps until `/quick` has run, then spins.
fn napSpin(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    napping.store(true, .release);
    const bound = nowNs() + report_deadline_ms * ms;
    while (!quick_ran.load(.acquire)) {
        if (nowNs() >= bound) return error.QuickNeverRan;
        try zio.sleep(.fromMilliseconds(1));
    }
    spinUntilReleased();
    try resp.send(200, &.{}, "spun");
}

fn quick(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try resp.send(200, &.{}, "spun");
    quick_ran.store(true, .release);
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/spin", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = spin } } },
    .{ .method = .GET, .path = "/yield", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = yielding } } },
    .{ .method = .GET, .path = "/spin-complete", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = spinComplete } } },
    .{ .method = .GET, .path = "/nap-spin", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = napSpin } } },
    .{ .method = .GET, .path = "/quick", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = quick } } },
};
const spin_route = 0;
const spin_complete_route = 2;
const nap_spin_route = 3;

const Proto = enum { h2c, h1c };

/// One request on its own connection, from an OS thread: the only executor
/// is busy with the handler while it spins. With `release_on_report`, the
/// client waits for the report counter to pass `reports_before`, then
/// releases the spinning handler, then reads the response.
const Client = struct {
    gpa: std.mem.Allocator,
    proto: Proto,
    port: u16,
    path: []const u8,
    release_on_report: bool = false,
    reports_before: usize = 0,
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,

    fn main(self: *Client) void {
        self.run() catch |err| {
            std.debug.print("watchdog client {s} ({t}) failed: {t}\n", .{ self.path, self.proto, err });
            self.err = err;
            // Never leave a handler spinning.
            release.store(true, .release);
        };
        self.done.store(true, .release);
    }

    fn run(self: *Client) !void {
        const w = try Wire.connect(self.gpa, .h2c, self.port);
        defer w.close();
        const deadline = nowNs() + (report_deadline_ms + 10_000) * ms;
        switch (self.proto) {
            .h2c => {
                var req = try h2c.buildClientPrefaceAndSettings(self.gpa);
                defer req.deinit(self.gpa);
                try h2c.appendHeaders(self.gpa, &req, 1, self.path, true);
                try w.send(req.items);
            },
            .h1c => {
                var buf: [128]u8 = undefined;
                const line = try std.fmt.bufPrint(&buf, "GET {s} HTTP/1.1\r\nhost: localhost\r\n\r\n", .{self.path});
                try w.sendUnchecked(line);
            },
        }
        if (self.release_on_report) {
            const until = nowNs() + report_deadline_ms * ms;
            while (watchdog.test_reports.load(.acquire) == self.reports_before) {
                if (nowNs() >= until) {
                    std.debug.print("watchdog client {s} ({t}): no report within {d} ms of the request\n", .{ self.path, self.proto, report_deadline_ms });
                    return error.NoReport;
                }
                starh2.test_sleep.threadSleepMs(1);
            }
            release.store(true, .release);
        }
        switch (self.proto) {
            .h2c => while (true) {
                const s = try w.next(deadline) orelse return error.ClosedBeforeResponse;
                if ((s.ftype == 0 or s.ftype == 1) and s.sid == 1 and s.flags & 1 != 0) return;
                if (s.ftype == 3 or s.ftype == 7) return error.ResetOrGoaway;
            },
            .h1c => while (std.mem.indexOf(u8, w.plain.items, "\r\n\r\n") == null or !std.mem.endsWith(u8, w.plain.items, "spun") and !std.mem.endsWith(u8, w.plain.items, "yielded")) {
                if (nowNs() >= deadline) return error.NoResponse;
                const more = w.pump() catch |err| switch (err) {
                    error.WouldBlock => continue,
                    else => return err,
                };
                if (!more) return error.ClosedBeforeResponse;
            },
        }
    }
};

const Rig = struct {
    gpa: std.mem.Allocator,
    server: starh2.Server,
    handle: zio.JoinHandle(starh2.ServeError!void),

    fn start(self: *Rig, rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
        self.* = .{ .gpa = gpa, .server = undefined, .handle = undefined };
        var limits = starh2.Limits.defaults;
        limits.executor_stall_report_ns = limit_ms * ms;
        limits.graceful_drain_timeout_ns = 300 * ms;
        const a = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
        self.server = try starh2.Server.init(gpa, rt.io(), .{
            .endpoints = &.{ .{ .h2c_prior_knowledge = .{ .address = a } }, .{ .h1c = .{ .address = a } } },
            .routes = &routes,
            .tls = null,
            .limits = limits,
        });
        self.handle = try rt.spawn(starh2.Server.serve, .{ &self.server, gpa });
        try self.server.waitUntilListening(5 * std.time.ns_per_s);
    }

    fn stop(self: *Rig) void {
        release.store(true, .release);
        self.server.requestShutdown();
        serve_deadline.join(&self.server, &self.handle);
        self.server.deinit(self.gpa);
    }

    fn port(self: *Rig, proto: Proto) u16 {
        return self.server.localAddress(switch (proto) {
            .h2c => 0,
            .h1c => 1,
        }).getPort();
    }

    /// One request; waits for it with zio sleeps (the executor must stay
    /// free between handlers), then joins the client thread.
    fn request(self: *Rig, proto: Proto, path: []const u8, release_on_report: bool) !void {
        resetFlags();
        var c: Client = .{ .gpa = self.gpa, .proto = proto, .port = self.port(proto), .path = path, .release_on_report = release_on_report, .reports_before = reports() };
        const t = try std.Thread.spawn(.{}, Client.main, .{&c});
        defer starh2.test_sleep.joinThread(t);
        try waitClient(&c);
    }

    /// `/nap-spin`, and `/quick` on another connection once it naps.
    fn napThenQuick(self: *Rig) !void {
        resetFlags();
        var a: Client = .{ .gpa = self.gpa, .proto = .h2c, .port = self.port(.h2c), .path = "/nap-spin", .release_on_report = true, .reports_before = reports() };
        const ta = try std.Thread.spawn(.{}, Client.main, .{&a});
        defer starh2.test_sleep.joinThread(ta);
        const until = nowNs() + report_deadline_ms * ms;
        while (!napping.load(.acquire)) {
            if (nowNs() >= until) {
                release.store(true, .release);
                return error.NeverNapped;
            }
            zio.sleep(.fromMilliseconds(1)) catch {};
        }
        var b: Client = .{ .gpa = self.gpa, .proto = .h2c, .port = self.port(.h2c), .path = "/quick" };
        const tb = try std.Thread.spawn(.{}, Client.main, .{&b});
        defer starh2.test_sleep.joinThread(tb);
        try waitClient(&b);
        try waitClient(&a);
    }
};

fn waitClient(c: *Client) !void {
    const deadline = nowNs() + (report_deadline_ms + 15_000) * ms;
    while (!c.done.load(.acquire)) {
        if (nowNs() >= deadline) return error.ClientNeverEnded;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
    if (c.err) |err| return err;
}

fn reports() usize {
    return watchdog.test_reports.load(.acquire);
}

fn expectReports(comptime what: []const u8, before: usize, want: usize) !void {
    const got = reports() - before;
    if (got != want) {
        std.debug.print("watchdog: {s}: {d} reports, want {d} (last label {d})\n", .{ what, got, want, watchdog.test_last_label.load(.acquire) });
        return error.WrongReportCount;
    }
}

fn expectLabel(comptime what: []const u8, want: usize) !void {
    const got = watchdog.test_last_label.load(.acquire);
    if (got != want) {
        std.debug.print("watchdog: {s}: reported label {d}, want {d}\n", .{ what, got, want });
        return error.WrongLabel;
    }
}

fn runAll(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    var rig: Rig = undefined;
    try rig.start(rt, gpa);
    defer rig.stop();
    const tag = rig.server.watch_tag;

    // Idle: the executor parks, nothing is reported.
    var before = reports();
    zio.sleep(.fromMilliseconds(quiet_ms)) catch {};
    try expectReports("idle server", before, 0);

    // Busy but yielding: scheduler progress all along.
    for ([_]Proto{ .h2c, .h1c }) |proto| {
        before = reports();
        try rig.request(proto, "/yield", false);
        try expectReports("a handler that yields (/yield)", before, 0);
    }

    // Spinning until the report: one report per stall, naming the route.
    for ([_]Proto{ .h2c, .h1c }) |proto| {
        before = reports();
        try rig.request(proto, "/spin", true);
        try expectReports("a task handler that spins (/spin)", before, 1);
        try expectLabel("a task handler that spins (/spin)", watchdog.label(tag, spin_route));
    }
    before = reports();
    try rig.request(.h1c, "/spin-complete", true);
    try expectReports("a complete handler that spins inline on the actor (/spin-complete)", before, 1);
    try expectLabel("a complete handler that spins inline on the actor (/spin-complete)", watchdog.label(tag, spin_complete_route));
    before = reports();
    try rig.napThenQuick();
    try expectReports("a handler that naps while /quick runs, then spins (/nap-spin)", before, 1);
    try expectLabel("a handler that naps while /quick runs, then spins (/nap-spin)", watchdog.label(tag, nap_spin_route));

    // Idle again after the stalls: nothing more.
    before = reports();
    zio.sleep(.fromMilliseconds(quiet_ms)) catch {};
    try expectReports("idle after the stalls", before, 0);
    std.debug.print("watchdog: limit {d} ms: spinning task (h2c, h1c), inline complete (h1c) and nap-then-spin (h2c) reported once each with their route; yielding handlers and the idle server ({d} ms each) not reported\n", .{ limit_ms, quiet_ms });
}

test "executor watchdog: a spinning handler is reported once with its route; a yielding handler and an idle server are not (t-3760)" {
    // These stalls are on purpose: reported as warnings, not test errors.
    watchdog.test_reports_fail.store(false, .release);
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(1) });
    defer rt.deinit();
    var h = try rt.spawn(runAll, .{ rt, gpa });
    try h.join();
}

fn runTwo(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const refused_before = watchdog.test_second_refused.load(.acquire);
    var first: Rig = undefined;
    try first.start(rt, gpa);
    defer first.stop();
    var second: Rig = undefined;
    try second.start(rt, gpa);
    defer second.stop();
    try std.testing.expect(first.server.watch_tag != second.server.watch_tag);
    // The first server's watchdog watches the runtime; the second started none.
    try std.testing.expectEqual(refused_before + 1, watchdog.test_second_refused.load(.acquire));

    // A stall in the second server's handler: one report (no duplicate), as
    // the second server's route 0, never as the first server's route 0.
    const before = reports();
    try second.request(.h2c, "/spin", true);
    // Past the report, a little longer: a second watchdog would report too.
    zio.sleep(.fromMilliseconds(quiet_ms)) catch {};
    try expectReports("a stall in the second server's handler", before, 1);
    try expectLabel("a stall in the second server's handler", watchdog.label(second.server.watch_tag, spin_route));
    std.debug.print("watchdog: two servers on one runtime: one watchdog, the second server's stall reported once as server {d}\n", .{second.server.watch_tag});
}

test "executor watchdog: two servers on one runtime have one watchdog, and a stall is reported once, as its own server's (t-3760)" {
    // These stalls are on purpose: reported as warnings, not test errors.
    watchdog.test_reports_fail.store(false, .release);
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(1) });
    defer rt.deinit();
    var h = try rt.spawn(runTwo, .{ rt, gpa });
    try h.join();
}
