//! Admission gate: a full server waits, it does not refuse.
//!
//! When the connection limit, or the TLS handshake limit, is full, the accept
//! loop stops calling accept. A new connection waits in the kernel's listen
//! queue and is accepted and served once a slot is released. It used to be
//! accepted and closed at once, so a burst larger than the limits lost
//! everything past them (128 of 2000 at the default handshake limit).
//!
//! Each gate fills one limit with a client that holds its slot, connects a
//! second client, and requires that the second is neither served nor closed
//! while the slot is held, and is served after the first client leaves. The
//! third gate stops the server while the accept loop is parked.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const tls = @import("tls");

const dummy: u8 = 0;
const server_mod = starh2.edge.server;
const conn_mod = starh2.edge.connection;

/// How long the queued client must see nothing at all.
const held_ns: u64 = 400 * std.time.ns_per_ms;

fn hello(_: *anyopaque, _: *const starh2.Request, resp: *starh2.CompleteResponse) anyerror!void {
    try resp.send(200, &.{}, "ok");
}

const routes = [_]starh2.Route{
    .{ .method = .GET, .path = "/", .handler = .{ .complete = .{ .ptr = @constCast(&dummy), .runFn = hello } } },
};

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

fn writeAll(stream: zio.net.Stream, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) off += try stream.write(bytes[off..], .none);
}

/// The queued client must get no byte and no close for `held_ns`.
fn expectHeld(stream: zio.net.Stream) !void {
    var buf: [256]u8 = undefined;
    const n = stream.read(&buf, .{ .duration = .fromNanoseconds(held_ns) }) catch |err| switch (err) {
        error.Timeout => return,
        else => {
            std.debug.print("admission: queued client saw {s} while the slot was held\n", .{@errorName(err)});
            return error.QueuedClientClosed;
        },
    };
    std.debug.print("admission: queued client read {d} bytes while the slot was held\n", .{n});
    return if (n == 0) error.QueuedClientClosed else error.QueuedClientServed;
}

/// Read plaintext HTTP until "ok" arrives. Error on close or timeout.
fn expectOk(stream: zio.net.Stream) !void {
    var buf: [4096]u8 = undefined;
    var len: usize = 0;
    const deadline = nowNs() +% 3 * std.time.ns_per_s;
    while (std.mem.indexOf(u8, buf[0..len], "\r\n\r\nok") == null) {
        if (nowNs() >= deadline) return error.NoResponse;
        if (len == buf.len) return error.ResponseTooLarge;
        const n = try stream.read(buf[len..], .{ .duration = .fromSeconds(3) });
        if (n == 0) return error.ClosedBeforeResponse;
        len += n;
    }
}

fn waitUntil(comptime what: []const u8, value: *const std.atomic.Value(usize), want: usize) !void {
    const deadline = nowNs() +% 2 * std.time.ns_per_s;
    while (value.load(.acquire) != want) {
        if (nowNs() >= deadline) {
            std.debug.print("admission: {s} is {d}, waited for {d}\n", .{ what, value.load(.acquire), want });
            return error.StateNotReached;
        }
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

/// The accept loop has parked at least `n` times since `before`.
fn waitParked(before: usize, n: usize) !void {
    if (!conn_mod.test_observe) {
        zio.sleep(.fromMilliseconds(100)) catch {};
        return;
    }
    const deadline = nowNs() +% 2 * std.time.ns_per_s;
    while (server_mod.test_admission_waits.load(.acquire) < before + n) {
        if (nowNs() >= deadline) return error.AcceptLoopNeverParked;
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

const http_get = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";

/// A TLS 1.3 client over a zio stream, enough for one HTTP/1.1 exchange.
const TlsClient = struct {
    stream: zio.net.Stream,
    hs: tls.nonblock.Client,
    in: [2 * tls.input_buffer_len]u8 = undefined,
    in_len: usize = 0,
    out: [tls.output_buffer_len]u8 = undefined,

    fn fill(self: *TlsClient) !void {
        if (self.in_len == self.in.len) return error.ClientBufferFull;
        const n = try self.stream.read(self.in[self.in_len..], .{ .duration = .fromSeconds(3) });
        if (n == 0) return error.ClosedDuringHandshake;
        self.in_len += n;
    }

    fn consume(self: *TlsClient, n: usize) void {
        std.mem.copyForwards(u8, self.in[0 .. self.in_len - n], self.in[n..self.in_len]);
        self.in_len -= n;
    }

    /// Send the ClientHello only.
    fn start(self: *TlsClient) !void {
        const r = try self.hs.run(&.{}, &self.out);
        try writeAll(self.stream, r.send);
    }

    /// Finish the handshake, send one request, and read "ok" back.
    fn finishAndGet(self: *TlsClient) !void {
        while (!self.hs.done()) {
            const r = try self.hs.run(self.in[0..self.in_len], &self.out);
            self.consume(r.recv_pos);
            if (r.send.len > 0) try writeAll(self.stream, r.send);
            if (self.hs.done()) break;
            if (r.recv_pos == 0 and r.send.len == 0) try self.fill();
        }
        var conn: tls.nonblock.Connection = .init(self.hs.cipher().?);
        const req = try conn.encrypt(http_get, &self.out);
        try writeAll(self.stream, req.ciphertext);

        var plain: [4096]u8 = undefined;
        var plain_len: usize = 0;
        var rd: [tls.input_buffer_len]u8 = undefined;
        const deadline = nowNs() +% 3 * std.time.ns_per_s;
        while (std.mem.indexOf(u8, plain[0..plain_len], "\r\n\r\nok") == null) {
            if (nowNs() >= deadline) return error.NoResponse;
            const rec_len = recordLen(self.in[0..self.in_len]) orelse {
                try self.fill();
                continue;
            };
            const res = try conn.decrypt(self.in[0..rec_len], &rd);
            self.consume(rec_len);
            if (res.closed) return error.ClosedBeforeResponse;
            if (plain_len + res.cleartext.len > plain.len) return error.ResponseTooLarge;
            @memcpy(plain[plain_len..][0..res.cleartext.len], res.cleartext);
            plain_len += res.cleartext.len;
        }
    }
};

fn recordLen(bytes: []const u8) ?usize {
    if (bytes.len < 5) return null;
    const len = 5 + @as(usize, std.mem.readInt(u16, bytes[3..5], .big));
    return if (bytes.len < len) null else len;
}

fn runTlsQueued(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const io = rt.io();
    const cert_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/cert.pem", gpa, .limited(64 * 1024));
    defer gpa.free(cert_pem);
    const key_pem = try std.Io.Dir.cwd().readFileAlloc(io, "testdata/key.pem", gpa, .limited(64 * 1024));
    defer gpa.free(key_pem);
    var limits = starh2.Limits.defaults;
    limits.concurrent_tls_handshakes = 1;
    // Long enough that the held slot is freed by the client leaving, never
    // by the handshake timeout.
    limits.preface_timeout_ns = 20 * std.time.ns_per_s;
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

    // Client A takes the only handshake slot and sends nothing.
    const parked_before = server_mod.test_admission_waits.load(.acquire);
    var a = try peer.connect(.{});
    var a_open = true;
    defer if (a_open) a.close();
    try waitUntil("active_handshakes", &server.accounting.active_handshakes, 1);

    // Client B connects and sends a real ClientHello while the gate is full.
    var prng = std.Random.DefaultPrng.init(0xad31);
    const b = try gpa.create(TlsClient);
    defer gpa.destroy(b);
    b.* = .{
        .stream = try peer.connect(.{}),
        .hs = tls.nonblock.Client.init(.{
            .rng = prng.random(),
            .now = std.Io.Clock.real.now(io),
            .host = "localhost",
            .root_ca = .empty,
            .insecure_skip_verify = true,
            .cipher_suites = tls.config.cipher_suites.tls13,
            .alpn_protocols = &.{"http/1.1"},
        }),
    };
    defer b.stream.close();
    try b.start();
    try expectHeld(b.stream);
    // B is still in the listen queue, not accepted and parked somewhere, and
    // the accept loop is parked on the limit.
    try std.testing.expectEqual(@as(usize, 1), server.active_connections.load(.acquire));
    try waitParked(parked_before, 1);

    // A leaves: its handshake fails on EOF and releases the slot.
    const t0 = nowNs();
    a.close();
    a_open = false;
    try b.finishAndGet();
    std.debug.print("admission tls: queued client served {d} ms after the slot freed\n", .{(nowNs() -% t0) / std.time.ns_per_ms});
}

test "admission: a TLS client queued behind a full handshake limit is served when a slot frees" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var handle = try rt.spawn(runTlsQueued, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("admission tls gate failed: {s}\n", .{@errorName(err)});
        return err;
    };
}


fn h1Limits() starh2.Limits {
    var limits = starh2.Limits.defaults;
    limits.max_connections = 1;
    limits.graceful_drain_timeout_ns = 200 * std.time.ns_per_ms;
    return limits;
}

fn runH1Queued(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const io = rt.io();
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{.{ .h1c = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
        .limits = h1Limits(),
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());

    // Client A takes the only connection slot and keeps it (keep-alive).
    const parked_before = server_mod.test_admission_waits.load(.acquire);
    var a = try peer.connect(.{});
    var a_open = true;
    defer if (a_open) a.close();
    try writeAll(a, http_get);
    try expectOk(a);

    // Client B's request waits in the listen queue.
    var b = try peer.connect(.{});
    defer b.close();
    try writeAll(b, http_get);
    try expectHeld(b);
    try std.testing.expectEqual(@as(usize, 1), server.active_connections.load(.acquire));
    try waitParked(parked_before, 1);

    const t0 = nowNs();
    a.close();
    a_open = false;
    try expectOk(b);
    std.debug.print("admission h1c: queued client served {d} ms after the slot freed\n", .{(nowNs() -% t0) / std.time.ns_per_ms});
}

test "admission: an h1c client queued behind a full connection limit is served when a slot frees" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var handle = try rt.spawn(runH1Queued, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("admission h1c gate failed: {s}\n", .{@errorName(err)});
        return err;
    };
}

fn runStopWhileParked(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
    const io = rt.io();
    var server = try starh2.Server.init(gpa, io, .{
        .endpoints = &.{.{ .h1c = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0) }},
        .routes = &routes,
        .tls = null,
        .limits = h1Limits(),
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    var serving = true;
    defer if (serving) {
        server.requestShutdown();
        serve_handle.join() catch {};
    };
    try server.waitUntilListening(5 * std.time.ns_per_s);
    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", server.localAddress(0).getPort());

    const parked_before = server_mod.test_admission_waits.load(.acquire);
    var a = try peer.connect(.{});
    defer a.close();
    try writeAll(a, http_get);
    try expectOk(a);
    var b = try peer.connect(.{});
    defer b.close();
    try writeAll(b, http_get);
    try waitParked(parked_before, 1);

    // Stop with the loop parked and A still connected. If the parked loop
    // did not see the stop, serve would never return: it cancels the accept
    // group and waits for it before draining connections.
    const t0 = nowNs();
    server.requestShutdown();
    serve_handle.join() catch {};
    serving = false;
    const stop_ms = (nowNs() -% t0) / std.time.ns_per_ms;
    std.debug.print("admission stop: serve returned {d} ms after requestShutdown with the accept loop parked\n", .{stop_ms});
    try std.testing.expect(stop_ms < 2_000);

    // B was never accepted, so it was never served: the listener closed
    // under it.
    var buf: [256]u8 = undefined;
    const n = b.read(&buf, .{ .duration = .fromSeconds(2) }) catch |err| switch (err) {
        error.ConnectionResetByPeer => 0,
        else => return err,
    };
    try std.testing.expect(std.mem.indexOf(u8, buf[0..n], "ok") == null);
}

test "admission: stopping the server while the accept loop waits for a slot returns promptly" {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(2) });
    defer rt.deinit();
    var handle = try rt.spawn(runStopWhileParked, .{ rt, gpa });
    handle.join() catch |err| {
        std.debug.print("admission stop gate failed: {s}\n", .{@errorName(err)});
        return err;
    };
}
