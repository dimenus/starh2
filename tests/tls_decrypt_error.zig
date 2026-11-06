//! t-3807: a peer's record that fails to decrypt, while the TLS pump holds a
//! write chunk, on both protocols the TLS endpoint serves.
//!
//! The decrypt error is the one `failDrain` in `tls.zig` that posts no
//! shutdown (`Pump.readOne`): it gives back the chunk the pump holds without
//! moving the stash order on. A later write that staged again would trip
//! `popPending`'s order check (a panic in safe builds) and would send on a
//! connection whose peer is broken. This test drives that state over a real
//! socket, with no hook in the actor:
//!
//! 1. A client with a 4 KiB receive buffer asks for `/bulk`, whose handler
//!    writes until the connection stops it, and then reads nothing. The
//!    socket fills and the pump parks a chunk behind the send in flight.
//! 2. Once the handler stops making progress, the client sends one TLS
//!    application-data record with a valid header and a junk payload (even
//!    rounds), or a correctly encrypted fatal alert (odd rounds).
//! 3. The server must close the connection and stay healthy: a new
//!    connection gets its `/hello`, and shutdown finishes.
//!
//! `tls.test_decrypt_failures` must count exactly one decrypt error per
//! round, so a run proves it reached the decrypt-error `failDrain`. On h2
//! every round must also find a chunk held (`test_decrypt_failures_holding`):
//! a batch of chunks fills the staging buffer. HTTP/1.1 has one write in
//! flight, and its second record waits only until the socket takes the first
//! byte of the first, so a stalled HTTP/1.1 write rarely holds a chunk; that
//! arm requires a send in flight instead (`test_decrypt_failures_sending`),
//! and the transport witness (`tests/dst_tls_lifecycle.zig`) drives the
//! held case for HTTP/1.1 exactly. Each test runs `rounds` rounds on one
//! server.
const std = @import("std");
const zio = @import("zio");
const serve_deadline = @import("serve_deadline.zig");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");
const wire_client = @import("wire_client.zig");

const tls_edge = starh2.edge.tls_edge;
const Wire = wire_client.Wire;
const nowNs = wire_client.nowNs;
const client_deadline_ns = wire_client.client_deadline_ns;

const dummy: u8 = 0;
const rounds = 20;
const client_rcvbuf: c_int = 4096;

/// Bytes `/bulk` handlers got accepted, over all connections. The test reads
/// it to know the handler stopped making progress (the socket is full).
var bulk_bytes: std.atomic.Value(usize) = .init(0);

/// 64 KiB per write: HTTP/1.1 hands a write to the pump in chunks of
/// `WIRE_CHUNK_SIZE`, and a full one is two records, so the second waits
/// behind the first in flight and the pump holds the chunk. A 16 KiB write
/// is one record, which fits the staging buffer, so nothing would be held.
fn bulk(_: *anyopaque, _: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.start(200, &.{});
    const chunk: [64 * 1024]u8 = @splat('b');
    while (true) {
        try body.writeAll(&chunk);
        try body.flush();
        _ = bulk_bytes.fetchAdd(chunk.len, .acq_rel);
    }
}

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/bulk", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = bulk } } },
    .{ .method = .GET, .path = "/hello", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
};

const Proto = enum { h2, http11 };

/// What the client sends instead of a valid record. Both reach the same
/// `readOne` error path: a record that does not decrypt, or a fatal alert
/// from the peer, correctly encrypted (tls.zig returns it as an error).
const Bad = enum { junk_record, peer_alert };

const Client = struct {
    gpa: std.mem.Allocator,
    proto: Proto,
    bad: Bad,
    port: u16,
    requested: std.atomic.Value(bool) = .init(false),
    corrupt_now: std.atomic.Value(bool) = .init(false),
    /// The server closed the connection after the corrupt record (EOF or a
    /// reset), with this many bytes read after it.
    closed: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    err: ?anyerror = null,
};

fn clientMain(c: *Client) void {
    clientRun(c) catch |err| {
        std.debug.print("decrypt-error client ({t}) failed: {t}\n", .{ c.proto, err });
        c.err = err;
    };
    c.done.store(true, .release);
}

fn clientRun(c: *Client) !void {
    const w = try Wire.connectRecvBuf(c.gpa, .tls, c.port, client_rcvbuf);
    defer w.close();
    w.alpn = switch (c.proto) {
        .h2 => "h2",
        .http11 => "http/1.1",
    };
    try w.handshake();
    switch (c.proto) {
        .h2 => {
            var req = try h2c.buildClientPrefaceAndSettings(c.gpa);
            defer req.deinit(c.gpa);
            // Credit far beyond what the socket holds, on the connection
            // and on the stream, so the stall is the socket's, not flow
            // control's. With only the connection's, stream 1 stops at the
            // default 65,535 bytes, which Linux loopback buffers absorb: the
            // pump then sits idle and the round tests nothing (nachos).
            try h2c.appendWindowUpdate(c.gpa, &req, 0, 1 << 30);
            try h2c.appendHeaders(c.gpa, &req, 1, "/bulk", true);
            try h2c.appendWindowUpdate(c.gpa, &req, 1, 1 << 30);
            try w.send(req.items);
        },
        .http11 => try w.sendUnchecked("GET /bulk HTTP/1.1\r\nhost: localhost\r\n\r\n"),
    }
    c.requested.store(true, .release);

    const deadline = nowNs() + client_deadline_ns;
    while (!c.corrupt_now.load(.acquire)) {
        if (nowNs() >= deadline) return error.Deadline;
        sleepMs(1);
    }
    switch (c.bad) {
        .junk_record => {
            // One application-data record: a valid header, a payload that
            // is not ciphertext under this connection's keys.
            var junk: [5 + 64]u8 = undefined;
            junk[0..5].* = .{ 0x17, 0x03, 0x03, 0x00, 64 };
            @memset(junk[5..], 0xa5);
            try w.sendRaw(&junk);
        },
        .peer_alert => {
            // A fatal handshake_failure alert, sealed with this client's
            // next sequence number, as a peer that gives up would send.
            w.conn.inner.pending_alert = .handshake_failure;
            w.conn.inner.state = .fatal_alert_pending;
            const rec = try w.conn.close(&w.out);
            try w.sendRaw(rec);
        },
    }

    // Read and drop until the server closes. The receive timeout is 1 s
    // (`connectRecvBuf`); a server that never closes runs out the deadline.
    var buf: [16 * 1024]u8 = undefined;
    while (true) {
        if (nowNs() >= deadline) return error.ServerNeverClosed;
        const n = std.c.recv(w.fd, &buf, buf.len, 0);
        if (n == 0) break;
        if (n < 0) switch (std.c.errno(n)) {
            .AGAIN, .INTR => continue,
            .CONNRESET => break,
            else => return error.Recv,
        };
    }
    c.closed.store(true, .release);
}

fn sleepMs(ms: u32) void {
    starh2.test_sleep.threadSleepMs(ms);
}

/// A fresh HTTP/2 connection gets `/hello`: the server survived. On an OS
/// thread of its own, like the client: the `Wire` calls block, and a zio
/// executor blocked in one, or in a thread join, cannot serve the server's
/// tasks. So the wait for it is a zio sleep, and the join comes after.
fn helloWorks(gpa: std.mem.Allocator, port: u16) !void {
    var result: anyerror!void = {};
    var done: std.atomic.Value(bool) = .init(false);
    const t = try std.Thread.spawn(.{}, helloThread, .{ gpa, port, &result, &done });
    while (!done.load(.acquire)) zio.sleep(.fromMilliseconds(2)) catch {};
    t.join();
    return result;
}

fn helloThread(gpa: std.mem.Allocator, port: u16, result: *anyerror!void, done: *std.atomic.Value(bool)) void {
    result.* = helloRun(gpa, port);
    done.store(true, .release);
}

fn helloRun(gpa: std.mem.Allocator, port: u16) !void {
    const w = try Wire.open(gpa, .tls, port);
    defer w.close();
    var req = try h2c.buildClientPrefaceAndSettings(gpa);
    defer req.deinit(gpa);
    try h2c.appendHeaders(gpa, &req, 1, "/hello", true);
    try w.send(req.items);
    const deadline = nowNs() + client_deadline_ns;
    while (true) {
        const s = try w.next(deadline) orelse return error.HelloClosed;
        if (s.ftype == 0 and s.sid == 1) {
            try std.testing.expectEqualStrings("ok", w.last_payload.items);
            if (s.flags & 1 != 0) return;
        }
        if (s.ftype == 3 or s.ftype == 7) return error.HelloResetOrGoaway;
    }
}

fn runRounds(rt: *zio.Runtime, gpa: std.mem.Allocator, proto: Proto) !void {
    const io = rt.io();
    const cert = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert);
    const key = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key);
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{.{ .tls = .{ .address = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) } }},
        .routes = &routes,
        .tls = .{ .certificate_chain_pem = cert, .private_key_pem = key },
        .limits = starh2.Limits.defaults,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_deadline.join(&server, &serve_handle);
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const port = server.localAddress(0).getPort();

    var holding_rounds: usize = 0;
    var sending_rounds: usize = 0;
    for (0..rounds) |round| {
        const failures_0 = tls_edge.test_decrypt_failures.load(.acquire);
        const holding_0 = tls_edge.test_decrypt_failures_holding.load(.acquire);
        const sending_0 = tls_edge.test_decrypt_failures_sending.load(.acquire);
        const bad: Bad = if (round % 2 == 0) .junk_record else .peer_alert;
        var c: Client = .{ .gpa = gpa, .proto = proto, .bad = bad, .port = port };
        const t = try std.Thread.spawn(.{}, clientMain, .{&c});
        var joined = false;
        defer if (!joined) {
            c.corrupt_now.store(true, .release);
            t.join();
        };
        const deadline = nowNs() + client_deadline_ns;
        while (!c.requested.load(.acquire)) {
            if (c.err) |err| return err;
            if (nowNs() >= deadline) return error.Deadline;
            zio.sleep(.fromMilliseconds(2)) catch {};
        }
        // The handler has written something and then stopped for 200 ms:
        // the socket is full and the pump parks what it holds.
        var last = bulk_bytes.load(.acquire);
        var still: u32 = 0;
        var moved = false;
        while (still < 100) {
            if (nowNs() >= deadline) return error.NeverStalled;
            zio.sleep(.fromMilliseconds(2)) catch {};
            const now = bulk_bytes.load(.acquire);
            if (now != last) {
                moved = true;
                still = 0;
            } else if (moved) still += 1;
            last = now;
        }
        c.corrupt_now.store(true, .release);
        while (!c.done.load(.acquire)) {
            if (nowNs() >= deadline) return error.Deadline;
            zio.sleep(.fromMilliseconds(2)) catch {};
        }
        t.join();
        joined = true;
        if (c.err) |err| return err;
        try std.testing.expect(c.closed.load(.acquire));

        const failures = tls_edge.test_decrypt_failures.load(.acquire) - failures_0;
        const holding = tls_edge.test_decrypt_failures_holding.load(.acquire) - holding_0;
        const sending = tls_edge.test_decrypt_failures_sending.load(.acquire) - sending_0;
        if (failures != 1) {
            std.debug.print("decrypt-error {t} round {d} ({t}): {d} decrypt errors, want 1\n", .{ proto, round, bad, failures });
            return error.DecryptErrorNotReached;
        }
        holding_rounds += holding;
        sending_rounds += sending;
        helloWorks(gpa, port) catch |err| {
            std.debug.print("decrypt-error {t} round {d}: the next connection's /hello failed: {t}\n", .{ proto, round, err });
            return err;
        };
    }
    std.debug.print("decrypt-error {t}: {d} rounds, {d} decrypt errors: {d} with a chunk held, {d} with a send in flight; no panic, every connection closed, the server kept serving\n", .{ proto, rounds, rounds, holding_rounds, sending_rounds });
    // The state each arm is about. A run that did not reach it tested
    // nothing.
    switch (proto) {
        .h2 => try std.testing.expectEqual(@as(usize, rounds), holding_rounds),
        .http11 => try std.testing.expectEqual(@as(usize, rounds), sending_rounds),
    }

    server.requestShutdown();
    serve_deadline.join(&server, &serve_handle);
    serving = false;
}

fn runCase(proto: Proto) !void {
    if (comptime !starh2.edge.tls_edge.observe) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var h = try rt.spawn(runRounds, .{ rt, gpa, proto });
    try h.join();
}

test "h2 over TLS: a record that fails to decrypt while the pump holds a chunk closes the connection and nothing panics (t-3807)" {
    try runCase(.h2);
}

test "HTTP/1.1 over TLS: a record that fails to decrypt while the pump holds a chunk closes the connection and nothing panics (t-3807)" {
    try runCase(.http11);
}
