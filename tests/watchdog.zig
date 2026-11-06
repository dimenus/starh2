//! The executor watchdog (t-3760), on one executor and real sockets.
//!
//! The limit is 200 ms here (the thread samples every 100 ms). A handler
//! that spins without yielding for `hold_ms` holds the only executor that
//! long, so the watchdog must report it exactly once (once per stall), with
//! that handler's route. One that loops for the same time but calls
//! `zio.yield()` makes scheduler progress and must not be reported, and
//! neither must an idle server. The spinning handler runs as a task over
//! HTTP/2 and over HTTP/1.1, and as an HTTP/1.1 complete handler, which runs
//! inline on the connection's actor: the three places a handler is labelled.
//!
//! A fourth spinning handler first naps (it yields; the executor parks),
//! and while it naps another handler runs and ends, leaving no label. When
//! the napping task resumes, the executor must show its label again (zio
//! republishes it at every switch), so its stall is still named.
//!
//! The counters (`watchdog.test_reports`, `test_last_label`) are the
//! watchdog's own: it bumps them where it logs the report.
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

const limit_ms = 200;
/// How long each busy handler runs: four limits, so a loaded host still
/// sees one report.
const hold_ms = 4 * limit_ms;

fn spinFor(n_ms: u64) void {
    const until = nowNs() + n_ms * ms;
    while (nowNs() < until) std.atomic.spinLoopHint();
}

fn spin(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    spinFor(hold_ms);
    try resp.send(200, &.{}, "spun");
}

fn yielding(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const until = nowNs() + hold_ms * ms;
    while (nowNs() < until) try zio.yield();
    try resp.send(200, &.{}, "yielded");
}

/// Naps while `/quick` runs on another connection, then spins.
fn napSpin(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try zio.sleep(.fromMilliseconds(nap_ms));
    spinFor(hold_ms);
    try resp.send(200, &.{}, "spun");
}

fn quick(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    try resp.send(200, &.{}, "spun");
}

fn spinComplete(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    spinFor(hold_ms);
    try resp.send(200, &.{}, "spun");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/spin", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = spin } } },
    .{ .method = .GET, .path = "/yield", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = yielding } } },
    .{ .method = .GET, .path = "/spin-complete", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = spinComplete } } },
    .{ .method = .GET, .path = "/nap-spin", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = napSpin } } },
    .{ .method = .GET, .path = "/quick", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = quick } } },
};
const spin_label = 0 + 1;
const spin_complete_label = 2 + 1;
const nap_spin_label = 3 + 1;
/// How long `/nap-spin` naps before it spins; `/quick` is sent and answered
/// inside it.
const nap_ms = 300;

const Proto = enum { h2c, h1c };

/// One request on its own connection, from an OS thread: the only executor
/// is busy with the handler while it runs.
const Client = struct {
    gpa: std.mem.Allocator,
    proto: Proto,
    port: u16,
    path: []const u8,
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,

    fn main(self: *Client) void {
        self.run() catch |err| {
            std.debug.print("watchdog client {s} ({t}) failed: {t}\n", .{ self.path, self.proto, err });
            self.err = err;
        };
        self.done.store(true, .release);
    }

    fn run(self: *Client) !void {
        const w = try Wire.connect(self.gpa, .h2c, self.port);
        defer w.close();
        const deadline = nowNs() + (hold_ms + 10_000) * ms;
        switch (self.proto) {
            .h2c => {
                var req = try h2c.buildClientPrefaceAndSettings(self.gpa);
                defer req.deinit(self.gpa);
                try h2c.appendHeaders(self.gpa, &req, 1, self.path, true);
                try w.send(req.items);
                while (true) {
                    const s = try w.next(deadline) orelse return error.ClosedBeforeResponse;
                    if ((s.ftype == 0 or s.ftype == 1) and s.sid == 1 and s.flags & 1 != 0) return;
                    if (s.ftype == 3 or s.ftype == 7) return error.ResetOrGoaway;
                }
            },
            .h1c => {
                var buf: [128]u8 = undefined;
                const line = try std.fmt.bufPrint(&buf, "GET {s} HTTP/1.1\r\nhost: localhost\r\n\r\n", .{self.path});
                try w.sendUnchecked(line);
                while (std.mem.indexOf(u8, w.plain.items, "\r\n\r\n") == null or !std.mem.endsWith(u8, w.plain.items, "spun") and !std.mem.endsWith(u8, w.plain.items, "yielded")) {
                    if (nowNs() >= deadline) return error.NoResponse;
                    const more = w.pump() catch |err| switch (err) {
                        error.WouldBlock => continue,
                        else => return err,
                    };
                    if (!more) return error.ClosedBeforeResponse;
                }
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
    fn request(self: *Rig, proto: Proto, path: []const u8) !void {
        var c: Client = .{ .gpa = self.gpa, .proto = proto, .port = self.port(proto), .path = path };
        const t = try std.Thread.spawn(.{}, Client.main, .{&c});
        defer t.join();
        try waitClient(&c);
    }

    /// `/nap-spin`, and `/quick` on another connection while it naps.
    fn napThenQuick(self: *Rig) !void {
        var a: Client = .{ .gpa = self.gpa, .proto = .h2c, .port = self.port(.h2c), .path = "/nap-spin" };
        const ta = try std.Thread.spawn(.{}, Client.main, .{&a});
        defer ta.join();
        zio.sleep(.fromMilliseconds(nap_ms / 3)) catch {};
        var b: Client = .{ .gpa = self.gpa, .proto = .h2c, .port = self.port(.h2c), .path = "/quick" };
        const tb = try std.Thread.spawn(.{}, Client.main, .{&b});
        defer tb.join();
        try waitClient(&b);
        try waitClient(&a);
    }

    fn waitClient(c: *Client) !void {
        const deadline = nowNs() + (hold_ms + 15_000) * ms;
        while (!c.done.load(.acquire)) {
            if (nowNs() >= deadline) return error.ClientNeverEnded;
            zio.sleep(.fromMilliseconds(5)) catch {};
        }
        if (c.err) |err| return err;
    }
};

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

    // Idle: the executor parks, nothing is reported.
    var before = reports();
    zio.sleep(.fromMilliseconds(hold_ms)) catch {};
    try expectReports("idle server", before, 0);

    // Busy but yielding: scheduler progress all along.
    for ([_]Proto{ .h2c, .h1c }) |proto| {
        before = reports();
        try rig.request(proto, "/yield");
        try expectReports("a handler that yields (" ++ "/yield)", before, 0);
    }

    // Spinning: one report per stall, naming the route.
    for ([_]Proto{ .h2c, .h1c }) |proto| {
        before = reports();
        try rig.request(proto, "/spin");
        try expectReports("a task handler that spins (/spin)", before, 1);
        try expectLabel("a task handler that spins (/spin)", spin_label);
    }
    before = reports();
    try rig.request(.h1c, "/spin-complete");
    try expectReports("a complete handler that spins inline on the actor (/spin-complete)", before, 1);
    try expectLabel("a complete handler that spins inline on the actor (/spin-complete)", spin_complete_label);
    before = reports();
    try rig.napThenQuick();
    try expectReports("a handler that naps while /quick runs, then spins (/nap-spin)", before, 1);
    try expectLabel("a handler that naps while /quick runs, then spins (/nap-spin)", nap_spin_label);

    // Idle again after the stalls: nothing more.
    before = reports();
    zio.sleep(.fromMilliseconds(hold_ms)) catch {};
    try expectReports("idle after the stalls", before, 0);
    std.debug.print("watchdog: limit {d} ms, handlers held {d} ms: spinning task (h2c, h1c), inline complete (h1c) and nap-then-spin (h2c) reported once each with their route; yielding handlers and the idle server not reported\n", .{ limit_ms, hold_ms });
}

test "executor watchdog: a spinning handler is reported once with its route; a yielding handler and an idle server are not (t-3760)" {
    // The counters exist only in observe builds (`test_hook`).
    if (@TypeOf(watchdog.test_reports) == void) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(1) });
    defer rt.deinit();
    var h = try rt.spawn(runAll, .{ rt, gpa });
    try h.join();
}
