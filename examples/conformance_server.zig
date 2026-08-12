//! Fixed conformance-server CLI: --mode tls|h2c, --bind host:port,
//! optional --cert/--key; one JSON ready line on stdout; routes /hello /sse /morph /signals.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

const Signals = struct {
    nonce: []const u8 = "",
    sequence: u64 = 0,
};

var g_shutdown: std.atomic.Value(bool) = .init(false);

fn handleSignal(_: std.posix.SIG) callconv(.c) void {
    g_shutdown.store(true, .release);
}

fn installSignalHandlers() void {
    const action: std.posix.Sigaction = .{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.TERM, &action, null);
    std.posix.sigaction(.INT, &action, null);
}

fn helloHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var nonce: []const u8 = "missing";
    for (req.headers) |h| {
        if (std.mem.eql(u8, h.name, "x-grader-nonce")) {
            nonce = h.value;
            break;
        }
    }
    var buf: [256]u8 = undefined;
    const body = try std.fmt.bufPrint(&buf, "hello:{s}", .{nonce});
    const headers = [_]starh2.Header{.{ .name = "content-type", .value = "text/plain" }};
    try resp.send(200, &headers, body);
}

fn sseHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const signals = starh2.datastar.readSignalsFromQuery(Signals, req) catch {
        try resp.send(400, &.{}, "bad signals");
        return;
    };
    var body = try resp.startSse(&.{});
    // Immediate nonce/sequence patch
    var arena = std.heap.ArenaAllocator.init(req.arena);
    defer arena.deinit();
    const patch = try starh2.datastar.patchElementsFmt(
        arena.allocator(),
        "<div id=\"n\">{s}-{d}</div>",
        .{ signals.nonce, signals.sequence },
        .{},
    );
    try body.writeAll(patch);
    // Capacity retention is an optimization; either reset outcome is usable.
    _ = arena.reset(.retain_capacity);

    // Optional window-fill for the multiplex grader's single stalled stream.
    // Must exceed common client SETTINGS_INITIAL_WINDOW_SIZE (often 4 MiB).
    const exhaust = std.mem.indexOf(u8, req.query, "exhaust=1") != null;
    if (exhaust) {
        var flood: [2048]u8 = undefined;
        const flood_line = try std.fmt.bufPrint(&flood, "event: datastar-patch-elements\ndata: <div id=\"pad\">{s}</div>\n\n", .{signals.nonce});
        var filled: usize = 0;
        const target: usize = 5 * 1024 * 1024;
        while (filled < target) {
            body.writeAll(flood_line) catch break;
            filled += flood_line.len;
            try zio.maybeYield();
        }
    }

    var seq: u64 = signals.sequence;
    while (!g_shutdown.load(.acquire)) {
        zio.sleep(.fromMilliseconds(100)) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            return err;
        };
        seq += 1;
        const ev = try starh2.datastar.patchElementsFmt(
            arena.allocator(),
            "<div id=\"tick\">{s}-{d}</div>",
            .{ signals.nonce, seq },
            .{},
        );
        try body.writeAll(ev);
        // Capacity retention is an optimization; either reset outcome is usable.
        _ = arena.reset(.retain_capacity);
        try zio.maybeYield();
    }
    try body.finish();
}

fn morphHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const signals = starh2.datastar.readSignalsFromBody(Signals, req) catch {
        try resp.send(400, &.{}, "bad signals");
        return;
    };
    var arena = std.heap.ArenaAllocator.init(req.arena);
    defer arena.deinit();
    const patch = try starh2.datastar.patchElementsFmt(
        arena.allocator(),
        "<div id=\"morph\">{s}</div>",
        .{signals.nonce},
        .{},
    );
    const headers = [_]starh2.Header{.{ .name = "content-type", .value = "text/event-stream" }};
    try resp.send(200, &headers, patch);
}

fn signalsGetHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const signals = starh2.datastar.readSignalsFromQuery(Signals, req) catch {
        try resp.send(400, &.{}, "bad signals");
        return;
    };
    var arena = std.heap.ArenaAllocator.init(req.arena);
    defer arena.deinit();
    const patch = try starh2.datastar.patchSignals(arena.allocator(), signals, .{});
    const headers = [_]starh2.Header{.{ .name = "content-type", .value = "text/event-stream" }};
    try resp.send(200, &headers, patch);
}

fn signalsPostHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const signals = starh2.datastar.readSignalsFromBody(Signals, req) catch {
        try resp.send(400, &.{}, "bad signals");
        return;
    };
    var arena = std.heap.ArenaAllocator.init(req.arena);
    defer arena.deinit();
    const patch = try starh2.datastar.patchSignals(arena.allocator(), signals, .{});
    const headers = [_]starh2.Header{.{ .name = "content-type", .value = "text/event-stream" }};
    try resp.send(200, &headers, patch);
}

const dummy: u8 = 0;

fn parseArgs(gpa: std.mem.Allocator, process_args: std.process.Args) !struct {
    mode: []const u8,
    host: []const u8,
    port: u16,
    cert: ?[]const u8,
    key: ?[]const u8,
} {
    var args = try std.process.Args.Iterator.initAllocator(process_args, gpa);
    defer args.deinit();
    _ = args.next();
    var mode: []const u8 = "h2c";
    var bind: []const u8 = "127.0.0.1:0";
    var cert: ?[]const u8 = null;
    var key: ?[]const u8 = null;
    while (args.next()) |a| {
        if (std.mem.eql(u8, a, "--mode")) {
            mode = args.next() orelse return error.InvalidArgs;
        } else if (std.mem.eql(u8, a, "--bind")) {
            bind = args.next() orelse return error.InvalidArgs;
        } else if (std.mem.eql(u8, a, "--cert")) {
            cert = args.next();
        } else if (std.mem.eql(u8, a, "--key")) {
            key = args.next();
        }
    }
    const colon = std.mem.lastIndexOfScalar(u8, bind, ':') orelse return error.InvalidArgs;
    const host = bind[0..colon];
    const port = try std.fmt.parseInt(u16, bind[colon + 1 ..], 10);
    return .{ .mode = mode, .host = host, .port = port, .cert = cert, .key = key };
}

fn serveMain(rt: *zio.Runtime, gpa: std.mem.Allocator, process_args: std.process.Args, io: std.Io) !void {
    const args = try parseArgs(gpa, process_args);
    const addr = try starh2.EndpointAddress.parseIp4(args.host, args.port);

    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/hello", .handler = .{ .ptr = @constCast(&dummy), .runFn = helloHandler } },
        .{ .method = .GET, .path = "/sse", .handler = .{ .ptr = @constCast(&dummy), .runFn = sseHandler } },
        .{ .method = .POST, .path = "/morph", .handler = .{ .ptr = @constCast(&dummy), .runFn = morphHandler } },
        .{ .method = .GET, .path = "/signals", .handler = .{ .ptr = @constCast(&dummy), .runFn = signalsGetHandler } },
        .{ .method = .POST, .path = "/signals", .handler = .{ .ptr = @constCast(&dummy), .runFn = signalsPostHandler } },
    };

    var tls_cfg: ?starh2.TlsConfig = null;
    var cert_pem: []u8 = &.{};
    var key_pem: []u8 = &.{};
    defer if (cert_pem.len != 0) gpa.free(cert_pem);
    defer if (key_pem.len != 0) gpa.free(key_pem);

    const ep: starh2.EndpointConfig = if (std.mem.eql(u8, args.mode, "tls")) blk: {
        const cpath = args.cert orelse return error.MissingCert;
        const kpath = args.key orelse return error.MissingKey;
        cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, cpath, gpa, .limited(64 * 1024));
        key_pem = try std.Io.Dir.cwd().readFileAlloc(io, kpath, gpa, .limited(16 * 1024));
        tls_cfg = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem };
        break :blk .{ .tls_h2 = addr };
    } else .{ .h2c_prior_knowledge = addr };

    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{ep},
        .routes = &routes,
        .tls = tls_cfg,
    });
    defer server.deinit(gpa);

    // Start serve in background-ish: we need localAddress after bind.
    // serve() binds then loops — spawn it and wait for it to publish .listening.
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    // Cleared once serve() has already returned on its own; cancelling a handle
    // we have joined would consume it twice.
    var serve_running = true;
    errdefer if (serve_running) {
        server.requestShutdown();
        serve_handle.cancel();
    };

    // The ready line is a contract: harnesses connect the moment they read it,
    // so it must not be printed on a timer. On a bind failure, surface serve()'s
    // real error rather than the wait's generic BindFailed.
    server.waitUntilListening(5 * std.time.ns_per_s) catch |err| {
        if (err == error.BindFailed) {
            serve_running = false;
            try serve_handle.join();
        }
        return err;
    };
    const local = server.localAddress(0);
    const port = local.getPort();

    const protocol = if (std.mem.eql(u8, args.mode, "tls")) "h2" else "h2c";
    const ready = try std.fmt.allocPrint(gpa, "{{\"ready\":true,\"mode\":\"{s}\",\"protocol\":\"{s}\",\"port\":{d}}}\n", .{ args.mode, protocol, port });
    defer gpa.free(ready);
    var out = zio.stdout().writer(&.{});
    try out.interface.writeAll(ready);
    try out.interface.flush();

    // Wait for SIGTERM / shutdown
    while (!g_shutdown.load(.acquire)) {
        zio.sleep(.fromMilliseconds(100)) catch break;
    }
    server.requestShutdown();
    try serve_handle.join();
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    g_shutdown.store(false, .release);
    installSignalHandlers();

    const rt = try zio.Runtime.init(gpa, .{
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024, .shrink_interval = .fromSeconds(30), .slab_slots = 256, .prewarm = 256 },
        .executors = .auto,
        .enable_task_migration = true,
    });
    defer rt.deinit();

    var handle = try rt.spawn(serveMain, .{ rt, gpa, init.minimal.args, init.io });
    try handle.join();
}
