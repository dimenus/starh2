//! TLS handshake timeout gate.
//!
//! The server runs the handshake on the connection's own task under
//! `zio.withTimeout`. A client that connects and then stalls must be closed
//! once `Limits.preface_timeout_ns` passes, and the connection must release
//! everything it took: the connection slot, the handshake slot, and every
//! handler slot. Nothing else in the suite drives a handshake past its
//! deadline, so without this test a timeout that never fires stays green.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

const conn_mod = starh2.edge.connection;
const dummy: u8 = 0;
const handshake_timeout_ns: u64 = 300 * std.time.ns_per_ms;

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

fn writeAll(stream: zio.net.Stream, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        off += try stream.write(bytes[off..], .none);
    }
}

/// Reads until the server closes. Returns how long that took. A read that
/// outlives several timeouts means the server never closed.
fn waitServerClose(stream: zio.net.Stream) !u64 {
    const t0 = nowNs();
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = stream.read(&buf, .{ .duration = .fromSeconds(3) }) catch |err| switch (err) {
            error.Timeout => return error.ServerNeverClosed,
            error.ConnectionResetByPeer => return nowNs() -% t0,
            else => return err,
        };
        if (n == 0) return nowNs() -% t0;
    }
}

fn waitReleased(server: *starh2.Server) !void {
    const deadline = nowNs() +% 2 * std.time.ns_per_s;
    while (true) {
        const conns = server.active_connections.load(.acquire);
        const handshakes = server.accounting.active_handshakes.load(.acquire);
        const slots = conn_mod.test_observed_slots_in_use.load(.acquire);
        const live = conn_mod.test_observed_live_handlers.load(.acquire);
        if (conns == 0 and handshakes == 0 and slots == 0 and live == 0) return;
        if (nowNs() >= deadline) {
            std.debug.print("handshake timeout left state: connections={d} handshakes={d} slots={d} live={d}\n", .{ conns, handshakes, slots, live });
            return error.NotReleased;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

fn runHandshakeTimeout(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const io = rt.io();
    const cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert_pem);
    const key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key_pem);
    const routes = [_]starh2.Route{
        .{ .method = .GET, .path = "/", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
    };
    var limits = starh2.Limits.defaults;
    limits.preface_timeout_ns = handshake_timeout_ns;
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{.{ .tls = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = .{ .certificate_chain_pem = cert_pem, .private_key_pem = key_pem },
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());

    // Two stalls: a client that sends nothing, and one that sends the start
    // of a ClientHello record and stops, so the server has read ciphertext
    // and is parked waiting for the rest of it.
    const stalls = [_][]const u8{
        "",
        &.{ 0x16, 0x03, 0x01, 0x00, 0xc8, 0x01, 0x00, 0x00 },
    };
    const timeouts = &starh2.edge.server.test_handshake_timeouts;
    for (stalls) |sent| {
        const timeouts_before = timeouts.load(.acquire);
        var stream = try peer.connect(.{});
        defer stream.close();
        try writeAll(stream, sent);
        const elapsed = try waitServerClose(stream);
        try waitReleased(&server);
        std.debug.print("handshake timeout: sent={d} closed_after_ms={d} timeouts={d}\n", .{
            sent.len,
            elapsed / std.time.ns_per_ms,
            timeouts.load(.acquire),
        });
        // The server closed because the deadline passed, not for another
        // reason: it held the socket for most of the timeout, and it counted
        // a timeout rather than a failed handshake.
        try std.testing.expect(elapsed >= handshake_timeout_ns / 2);
        if (conn_mod.test_observe) {
            try std.testing.expectEqual(timeouts_before + 1, timeouts.load(.acquire));
        }
    }
}

test "handshake: a stalled TLS client is closed at the timeout and releases every slot" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var handle = try rt.spawn(runHandshakeTimeout, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("handshake timeout gate failed: {s}\n", .{@errorName(err)});
        return err;
    };
}
