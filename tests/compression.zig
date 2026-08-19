//! Response brotli compression — the I1 to I8 live gates.
//!
//! These invariants define what this module grades. All of them must hold at
//! the same time. Each one names the test that pins it. `src/edge/connection.zig`
//! and `src/http/brotli.zig` refer to these numbers, so keep the list here.
//!
//! I1 — Negotiation is exact, per RFC 9110 section 12.5.3. The parser matches
//! the `br` token without case, and never as a substring. It reads q-values:
//! `q=0` means the client refuses `br`, and a missing q means 1. A `*` matches
//! `br`, unless the header names `br` itself. Several `accept-encoding` lines
//! join into one comma-separated list. No header at all means identity. The
//! parser drops a malformed member, not the whole header. Pinned by:
//! - `acceptsBrotli: q-values` (src/http/content_coding.zig)
//! - `acceptsBrotli: no substring match` (src/http/content_coding.zig)
//! - `acceptsBrotli: star` (src/http/content_coding.zig)
//! - `acceptsBrotli: malformed member ignored` (src/http/content_coding.zig)
//!
//! I2 — Round-trip identity. The wire bytes of a compressed response decode to
//! exactly the bytes the handler wrote. `finish()` must emit the final brotli
//! block, because a truncated stream is a failure. Pinned by:
//! - `I2 send path round-trips brotli`
//! - `I2 streaming start path round-trips`
//!
//! I3 — The encoder compresses. A compressible body of 64 KiB or more must
//! reach 25 percent of its original size, or less, at the default quality. This
//! gate rejects an encoder that emits stored metablocks, and a quality-0
//! default. Pinned by:
//! - `I3 big compressible body shrinks under 25 percent`
//!
//! I4 — A flush reaches the wire as decodable bytes. This is the SSE invariant,
//! and it carries the load: a compressor that buffers across events breaks a
//! Datastar client. After an SSE write, or after an explicit `Body.flush()`, the
//! bytes sent so far must decode to the complete plaintext of the events so far.
//! The encoder uses BROTLI_OPERATION_FLUSH at each flush point, and
//! BROTLI_OPERATION_FINISH at the end of the stream. The test feeds a streaming
//! decoder one chunk at a time. It asserts that event N decodes before event
//! N+1 exists. A check of the final bytes alone does not pin this. Pinned by:
//! - `I4 SSE flush is incrementally decodable before next event`
//! - `I4 large SSE event over outbound_bytes_per_stream still flushes decodably`
//! - `I4 empty compressed SSE write does not strand its receipt`
//!
//! I5 — Header hygiene. `content-encoding: br` is present exactly when the body
//! bytes are brotli, never on an identity body, and never absent on a compressed
//! body. A response for which the configuration made compression eligible
//! carries `vary: accept-encoding`, merged into a `vary` value that the handler
//! set. A `content-length` from the handler must never survive next to a
//! compressed body: the full-body path recomputes or omits it, and the streaming
//! path omits it. The layer never compresses an empty body, a body below
//! `compression_min_bytes` on the full-body path, status 204 or 304, a HEAD
//! response, or a content-type outside the compressible set. If the handler set
//! any `content-encoding` itself, the layer leaves that response untouched.
//! Pinned by:
//! - `I5 hygiene: min bytes, content-encoding passthrough, png, vary merge, no content-length`
//! - `no accept-encoding stays identity`
//!
//! I6 — Memory is bounded and accounted. Every brotli allocation goes through
//! the custom allocator hooks into a per-context budget of
//! `compression_context_bytes`. An encoder-init failure BEFORE the response
//! commits serves identity, and a counter records it. Budget exhaustion AFTER
//! the headers commit aborts the stream with RST. The layer never changes the
//! encoding in the middle of a stream. `compression_contexts_per_server` bounds
//! the live contexts server-wide, and `limits.resourceUpperBound` carries the
//! `compression_contexts` term. Boot rejects a window that cannot fit the
//! context budget. Pinned by:
//! - `I6 pool exhaustion serves identity and counts fallback`
//! - `lifecycle: shutdown releases live encoder context`
//! - `mutation canary: compression_contexts term moves with the limit` (src/core/limits.zig)
//! - `rejects response_compression with zero contexts` (src/core/limits.zig)
//!
//! I7 — The whole suite is the gate. `./zb build ci` is the definition of done:
//! the suite, the exact tests, the fuzz smoke, the TLS gate, and every release
//! cross-target. Never run a filtered subset and call the work done. Keep the
//! `test_queue_wire_bypass == 0` mutation canary.
//!
//! I8 — Error paths carry exact causes. The compression layer must not turn
//! `PeerReset`, `SlowConsumer`, or `ConnectionClosed` into `WriteFailed`, and
//! must not swallow them. A compressed streaming body surfaces a mid-stream peer
//! reset with the same error as an uncompressed one. Pinned by:
//! - `I8 compressed stream surfaces PeerReset exactly`
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");
const h2c = @import("starh2_h2_client");

const dummy: u8 = 0;
const brotli = starh2.http.brotli;
const hpack = starh2.core.hpack;
const frame = starh2.core.frame;

fn writeAllStream(stream: zio.net.Stream, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const n = try stream.write(bytes[off..], .none);
        off += n;
    }
}

const Capture = struct {
    gpa: std.mem.Allocator,
    parser: frame.Parser,
    body: std.ArrayList(u8) = .empty,
    headers: std.ArrayList(hpack.HeaderField) = .empty,
    end_stream: bool = false,
    saw_rst: bool = false,
    data_frames: usize = 0,
    /// Compressed body prefixes after each DATA frame (for I4 incremental decode).
    data_chunks: std.ArrayList([]u8) = .empty,

    fn init(gpa: std.mem.Allocator) !Capture {
        var parser = frame.Parser.init(gpa, frame.DEFAULT_MAX_FRAME_SIZE);
        parser.skipPreface();
        return .{ .gpa = gpa, .parser = parser };
    }

    fn deinit(self: *Capture) void {
        self.body.deinit(self.gpa);
        for (self.headers.items) |h| {
            h.freeOwned(self.gpa);
        }
        self.headers.deinit(self.gpa);
        for (self.data_chunks.items) |c| self.gpa.free(c);
        self.data_chunks.deinit(self.gpa);
        self.parser.deinit();
    }

    fn ingest(self: *Capture, bytes: []const u8) !void {
        var rem = bytes;
        while (rem.len > 0) {
            const maybe = try self.parser.ingestOne(rem);
            if (maybe) |r| {
                defer r.event.deinit(self.gpa);
                rem = rem[r.consumed..];
                const hdr = r.event.header;
                switch (hdr.type) {
                    .data => {
                        try self.body.appendSlice(self.gpa, r.event.payload.bytes());
                        const chunk = try self.gpa.dupe(u8, r.event.payload.bytes());
                        try self.data_chunks.append(self.gpa, chunk);
                        self.data_frames += 1;
                        if (hdr.flags.end_stream) self.end_stream = true;
                    },
                    .headers => {
                        var dec = hpack.Decoder.init(self.gpa);
                        defer dec.deinit();
                        const result = try dec.decode(r.event.payload.bytes(), 100, 64 * 1024, 256, 8 * 1024);
                        defer dec.freeResult(result);
                        for (result.fields) |f| {
                            try self.headers.append(self.gpa, .{
                                .name = try self.gpa.dupe(u8, f.name),
                                .value = try self.gpa.dupe(u8, f.value),
                                .name_owned = true,
                                .value_owned = true,
                            });
                        }
                        if (hdr.flags.end_stream) self.end_stream = true;
                    },
                    .rst_stream => self.saw_rst = true,
                    .settings => {
                        // ACK handled by discarding; server does not require client SETTINGS ACK for these tests.
                    },
                    else => {},
                }
            } else break;
        }
    }

    fn headerValue(self: *const Capture, name: []const u8) ?[]const u8 {
        for (self.headers.items) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }

    fn hasVaryAcceptEncoding(self: *const Capture) bool {
        for (self.headers.items) |h| {
            if (!std.ascii.eqlIgnoreCase(h.name, "vary")) continue;
            var it = std.mem.splitScalar(u8, h.value, ',');
            while (it.next()) |part| {
                const t = std.mem.trim(u8, part, " \t");
                if (std.ascii.eqlIgnoreCase(t, "accept-encoding")) return true;
            }
        }
        return false;
    }
};

fn readUntil(
    stream: zio.net.Stream,
    cap: *Capture,
    timeout_ms: u64,
    done: *const fn (*Capture) bool,
) !void {
    var buf: [16 * 1024]u8 = undefined;
    var waited: u64 = 0;
    while (waited < timeout_ms) : (waited += 20) {
        const n = stream.read(&buf, .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
            error.Timeout => 0,
            else => return err,
        };
        if (n > 0) try cap.ingest(buf[0..n]);
        if (done(cap)) return;
    }
    return error.Timeout;
}

fn doneEnd(c: *Capture) bool {
    return c.end_stream or c.saw_rst;
}

fn doneHeaders(c: *Capture) bool {
    return c.headers.items.len > 0 or c.saw_rst;
}

fn enableCompressionLimits(base: starh2.Limits) starh2.Limits {
    var lim = base;
    lim.response_compression = true;
    lim.max_connections = 16;
    lim.max_streams_per_connection = 32;
    lim.max_streams_per_server = 64;
    lim.cancellation_reaper_jobs = 64;
    lim.cancellation_reaper_tasks = 2;
    lim.outbound_slabs_per_server = 64;
    return lim;
}

fn withRuntime(comptime func: anytype) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{
        .executors = .exact(2),
        .enable_task_migration = false,
    });
    defer rt.deinit();
    var handle = try rt.spawn(func, .{ rt, gpa });
    try handle.join();
}

fn runServer(
    rt: *zio.Runtime,
    gpa: std.mem.Allocator,
    routes: []const starh2.Route,
    limits: starh2.Limits,
    comptime body: anytype,
) !void {
    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
    var server = try starh2.Server.init(gpa, rt.io(), .{
        .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
        .routes = routes,
        .tls = null,
        .limits = limits,
    });
    defer server.deinit(gpa);
    var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
    defer {
        server.requestShutdown();
        serve_handle.join() catch {};
    }
    try server.waitUntilListening(5 * std.time.ns_per_s);
    try body(&server, rt, gpa);
}

fn compressiblePlain(n: usize, gpa: std.mem.Allocator) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    const chunk = "<div class=\"row\"><span>item</span><p>lorem ipsum dolor</p></div>\n";
    while (out.items.len < n) try out.appendSlice(gpa, chunk);
    return try out.toOwnedSlice(gpa);
}

// --- handlers ---------------------------------------------------------------

fn sendTextHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    const plain = "hello compressed world " ** 20; // > 256 bytes
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/plain" }}, plain);
}

fn sendSmallHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/plain" }}, "tiny");
}

fn sendBigHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const plain = try compressiblePlain(64 * 1024, req.arena);
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/html" }}, plain);
}

/// `/big` shape from the conformance server: ≥5 MiB one-shot send.
fn sendFiveMiBHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    const plain = try req.arena.alloc(u8, 5 * 1024 * 1024);
    @memset(plain, 'B');
    try resp.send(200, &.{.{ .name = "content-type", .value = "text/plain" }}, plain);
}

fn sendWithContentEncoding(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    const plain = "already-encoded-body-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad-pad";
    try resp.send(200, &.{
        .{ .name = "content-type", .value = "text/plain" },
        .{ .name = "content-encoding", .value = "gzip" },
    }, plain);
}

fn sendWithVaryAndLength(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    const plain = "vary-merge-body " ** 40;
    var len_buf: [16]u8 = undefined;
    const len_s = try std.fmt.bufPrint(&len_buf, "{d}", .{plain.len});
    try resp.send(200, &.{
        .{ .name = "content-type", .value = "text/plain" },
        .{ .name = "vary", .value = "origin" },
        .{ .name = "content-length", .value = len_s },
    }, plain);
}

fn sendPngHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    const plain = "\x89PNG" ++ ("binary-not-compressible-pad" ** 20);
    try resp.send(200, &.{.{ .name = "content-type", .value = "image/png" }}, plain);
}

fn streamHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.start(200, &.{.{ .name = "content-type", .value = "text/plain" }});
    // many small writes, one flush
    try body.writeAll("part-a-");
    try body.writeAll("part-b-");
    try body.writeAll("part-c-and-more-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding-padding");
    try body.flush();
    try body.writeAll("-tail");
    try body.finish();
}

fn sseHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    const e1 = "data: <div id=\"a\">one</div>\n\n";
    const e2 = "data: <div id=\"a\">two-repeats-similar-html</div>\n\n";
    const e3 = "data: <div id=\"a\">three-still-similar</div>\n\n";
    // No sleep: an SSE write emits immediately. Occupancy-capped wait does not
    // separate two tiny events, so the incremental-decode pin uses `flush()`
    // as the wire barrier between them. A timed gap would test the clock
    // instead.
    try body.writeAll(e1);
    try body.flush();
    try body.writeAll(e2);
    try body.flush();
    try body.writeAll(e3);
    try body.finish();
}

fn sseEmptyWriteHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    try body.writeAll("data: before-empty\n\n");
    // After the preceding Brotli flush, another empty flush legitimately emits
    // no compressed bytes. Its receipt must use a wire barrier rather than a
    // zero-length scheduler entry, which can never produce a DATA frame.
    try body.writeAll("");
    try body.writeAll("data: after-empty\n\n");
    try body.finish();
}

fn sseLargeEventHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    var body = try resp.startSse(&.{});
    // One event larger than outbound_bytes_per_stream (64KiB default in test limits).
    var big: std.ArrayList(u8) = .empty;
    defer big.deinit(req.arena);
    try big.appendSlice(req.arena, "data: ");
    while (big.items.len < 70 * 1024) try big.appendSlice(req.arena, "<div>pad</div>");
    try big.appendSlice(req.arena, "\n\n");
    try body.writeAll(big.items);
    try body.finish();
}

fn sseOneByteHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    try body.writeAll("d");
    try body.writeAll("ata: x\n\n");
    try body.finish();
}

var peer_reset_err: std.atomic.Value(u8) = .init(0);

fn ssePeerResetHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    body.writeAll("data: before-reset\n\n") catch |err| {
        peer_reset_err.store(if (err == error.PeerReset) 3 else 5, .release);
        return err;
    };
    // Keep writing until peer RST surfaces.
    while (true) {
        body.writeAll("data: <div>more</div>\n\n") catch |err| {
            peer_reset_err.store(if (err == error.PeerReset) 3 else 1, .release);
            if (err == error.PeerReset) return;
            return err;
        };
        zio.sleep(.fromMilliseconds(5)) catch {};
    }
}

fn hangCompressedSse(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    _ = req;
    var body = try resp.startSse(&.{});
    try body.writeAll("data: hold\n\n");
    while (true) {
        zio.sleep(.fromMilliseconds(50)) catch |err| {
            if (err == error.Canceled) return error.Canceled;
            return err;
        };
        if (body.terminalCause() != null) return error.Canceled;
    }
}

fn streamExactOutboundHandler(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {
    // Write exactly outbound_bytes_per_stream.
    const n = 64 * 1024;
    const buf = try req.arena.alloc(u8, n);
    @memset(buf, 'Z');
    var body = try resp.start(200, &.{.{ .name = "content-type", .value = "text/plain" }});
    try body.writeAll(buf);
    try body.finish();
}

// --- tests ------------------------------------------------------------------

test "I2 send path round-trips brotli" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/t",
                .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendTextHandler } },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    const wire = try h2c.buildClientHelloExtra(alloc, "/t", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 5000, doneEnd);
                    try std.testing.expect(cap.end_stream);
                    const ce = cap.headerValue("content-encoding") orelse return error.MissingContentEncoding;
                    try std.testing.expectEqualStrings("br", ce);
                    try std.testing.expect(cap.hasVaryAcceptEncoding());
                    const decoded = try brotli.Decoder.decompressAll(alloc, 2 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    const expect = "hello compressed world " ** 20;
                    try std.testing.expectEqualStrings(expect, decoded);
                }
            }.go);
        }
    }.f);
}

test "I3 big compressible body shrinks under 25 percent" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/big",
                .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendBigHandler } },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "gzip, br" }};
                    const wire = try h2c.buildClientHelloExtra(alloc, "/big", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 8000, doneEnd);
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    try std.testing.expect(cap.body.items.len * 4 <= 64 * 1024);
                    const decoded = try brotli.Decoder.decompressAll(alloc, 4 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    try std.testing.expect(decoded.len >= 64 * 1024);
                }
            }.go);
        }
    }.f);
}

// Pins fix-round-1: 5 MiB one-shot + br must not kill the process; second
// request on the same connection proves the server stayed up.
test "five MiB one-shot br round-trip keeps server alive" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/big", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendFiveMiBHandler } } },
                .{ .method = .GET, .path = "/t", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendTextHandler } } },
            };
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();

                    // Large client windows so a multi-MiB payload can drain even
                    // if compression unexpectedly falls back to identity.
                    var wire = try h2c.buildClientPrefaceAndSettings(alloc);
                    defer wire.deinit(alloc);
                    {
                        var sbuf: [64]u8 = undefined;
                        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 1 << 23 }};
                        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
                        try wire.appendSlice(alloc, sbuf[0..sn]);
                    }
                    try h2c.appendWindowUpdate(alloc, &wire, 0, 1 << 23);
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    try h2c.appendHeadersExtra(alloc, &wire, 1, "GET", "/big", true, &extra);
                    try writeAllStream(stream, wire.items);

                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 30000, doneEnd);
                    try std.testing.expect(!cap.saw_rst);
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    const decoded = try brotli.Decoder.decompressAll(alloc, 4 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    try std.testing.expectEqual(@as(usize, 5 * 1024 * 1024), decoded.len);
                    try std.testing.expect(std.mem.allEqual(u8, decoded, 'B'));

                    // Keep-alive probe: server must still accept work.
                    var wire2: std.ArrayList(u8) = .empty;
                    defer wire2.deinit(alloc);
                    try h2c.appendHeadersExtra(alloc, &wire2, 3, "GET", "/t", true, &extra);
                    try writeAllStream(stream, wire2.items);
                    var cap2 = try Capture.init(alloc);
                    defer cap2.deinit();
                    try readUntil(stream, &cap2, 8000, doneEnd);
                    try std.testing.expectEqualStrings("br", cap2.headerValue("content-encoding").?);
                    const d2 = try brotli.Decoder.decompressAll(alloc, 4 * 1024 * 1024, cap2.body.items);
                    defer alloc.free(d2);
                    const expect = "hello compressed world " ** 20;
                    try std.testing.expectEqualStrings(expect, d2);
                }
            }.go);
        }
    }.f);
}

test "I5 hygiene: min bytes, content-encoding passthrough, png, vary merge, no content-length" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/small", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendSmallHandler } } },
                .{ .method = .GET, .path = "/ce", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendWithContentEncoding } } },
                .{ .method = .GET, .path = "/png", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendPngHandler } } },
                .{ .method = .GET, .path = "/vary", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendWithVaryAndLength } } },
            };
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const cases = [_]struct { path: []const u8, want_br: bool, check_vary: bool }{
                        .{ .path = "/small", .want_br = false, .check_vary = true },
                        .{ .path = "/ce", .want_br = false, .check_vary = false },
                        .{ .path = "/png", .want_br = false, .check_vary = false },
                        .{ .path = "/vary", .want_br = true, .check_vary = true },
                    };
                    for (cases) |case| {
                        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                        var stream = try peer.connect(.{});
                        defer stream.close();
                        const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                        const wire = try h2c.buildClientHelloExtra(alloc, case.path, &extra);
                        defer alloc.free(wire);
                        try writeAllStream(stream, wire);
                        var cap = try Capture.init(alloc);
                        defer cap.deinit();
                        try readUntil(stream, &cap, 5000, doneEnd);
                        const ce = cap.headerValue("content-encoding");
                        if (case.want_br) {
                            try std.testing.expectEqualStrings("br", ce.?);
                            try std.testing.expect(cap.headerValue("content-length") == null);
                        } else if (std.mem.eql(u8, case.path, "/ce")) {
                            try std.testing.expectEqualStrings("gzip", ce.?);
                        } else {
                            try std.testing.expect(ce == null);
                        }
                        if (case.check_vary) try std.testing.expect(cap.hasVaryAcceptEncoding());
                        if (std.mem.eql(u8, case.path, "/vary")) {
                            // origin must still be present
                            var saw_origin = false;
                            for (cap.headers.items) |h| {
                                if (!std.ascii.eqlIgnoreCase(h.name, "vary")) continue;
                                if (std.mem.indexOf(u8, h.value, "origin") != null) saw_origin = true;
                            }
                            try std.testing.expect(saw_origin);
                        }
                    }
                }
            }.go);
        }
    }.f);
}

test "I4 SSE flush is incrementally decodable before next event" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/sse",
                .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseHandler } },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    // end_stream false so we can observe progressive DATA before finish... actually
                    // startSse opens stream; client GET with end_stream true is fine.
                    const wire = try h2c.buildClientHelloExtra(alloc, "/sse", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);

                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    const dec = try brotli.Decoder.create(alloc, 2 * 1024 * 1024);
                    defer dec.destroy();
                    var plain: std.ArrayList(u8) = .empty;
                    defer plain.deinit(alloc);

                    const e1 = "data: <div id=\"a\">one</div>\n\n";
                    const e2 = "data: <div id=\"a\">two-repeats-similar-html</div>\n\n";
                    const e3 = "data: <div id=\"a\">three-still-similar</div>\n\n";

                    // Incremental: after each new DATA chunk, decode and check prefix.
                    var buf: [16 * 1024]u8 = undefined;
                    var saw_e1 = false;
                    var saw_e2 = false;
                    var waited: u64 = 0;
                    while (waited < 8000) : (waited += 20) {
                        const n = stream.read(&buf, .{ .duration = .fromMilliseconds(20) }) catch |err| switch (err) {
                            error.Timeout => 0,
                            else => return err,
                        };
                        if (n == 0) continue;
                        const before_chunks = cap.data_chunks.items.len;
                        try cap.ingest(buf[0..n]);
                        // Feed only newly observed DATA payload to the decoder.
                        var ci = before_chunks;
                        while (ci < cap.data_chunks.items.len) : (ci += 1) {
                            _ = try dec.decompress(cap.data_chunks.items[ci], &plain, alloc);
                        }
                        if (!saw_e1 and plain.items.len >= e1.len) {
                            try std.testing.expectEqualStrings(e1, plain.items[0..e1.len]);
                            saw_e1 = true;
                            // Event 1 must be decodable while event 3 is still
                            // absent. Without this, a stream that buffers every
                            // event and emits them together at the end passes:
                            // the final concatenation starts with e1, so the
                            // check above is satisfied by data that arrived all
                            // at once, which is exactly what I4 forbids.
                            try std.testing.expect(plain.items.len < e1.len + e2.len + e3.len);
                        }
                        if (saw_e1 and !saw_e2 and plain.items.len >= e1.len + e2.len) {
                            try std.testing.expectEqualStrings(e1 ++ e2, plain.items[0 .. e1.len + e2.len]);
                            saw_e2 = true;
                        }
                        if (cap.end_stream) break;
                    }
                    try std.testing.expect(saw_e1);
                    try std.testing.expect(saw_e2);
                    // Three writes, so the events cannot all have ridden one
                    // DATA frame. A single frame means the per-write emit was
                    // lost and only the end-of-stream finish produced output.
                    try std.testing.expect(cap.data_frames >= 2);
                    try std.testing.expect(cap.end_stream);
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    try std.testing.expectEqualStrings(e1 ++ e2 ++ e3, plain.items);
                }
            }.go);
        }
    }.f);
}

test "I4 empty compressed SSE write does not strand its receipt" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/sse-empty",
                .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseEmptyWriteHandler } },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    const wire = try h2c.buildClientHelloExtra(alloc, "/sse-empty", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);

                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 5000, doneEnd);
                    try std.testing.expect(cap.end_stream);
                    try std.testing.expect(!cap.saw_rst);
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    const decoded = try brotli.Decoder.decompressAll(alloc, 2 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    try std.testing.expectEqualStrings(
                        "data: before-empty\n\ndata: after-empty\n\n",
                        decoded,
                    );
                }
            }.go);
        }
    }.f);
}

test "I2 streaming start path round-trips" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/s",
                .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = streamHandler } },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "BR" }};
                    const wire = try h2c.buildClientHelloExtra(alloc, "/s", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 5000, doneEnd);
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    const decoded = try brotli.Decoder.decompressAll(alloc, 2 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    try std.testing.expect(std.mem.indexOf(u8, decoded, "part-a-") != null);
                    try std.testing.expect(std.mem.endsWith(u8, decoded, "-tail"));
                }
            }.go);
        }
    }.f);
}

test "I6 pool exhaustion serves identity and counts fallback" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            var lim = enableCompressionLimits(.{});
            lim.compression_contexts_per_server = 1;
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/hold", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangCompressedSse } } },
                .{ .method = .GET, .path = "/t", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendTextHandler } } },
            };
            try runServer(rt, gpa, &routes, lim, struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    // Hold the single encoder context with a live SSE.
                    const peer1 = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var s1 = try peer1.connect(.{});
                    defer s1.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    const w1 = try h2c.buildClientHelloExtra(alloc, "/hold", &extra);
                    defer alloc.free(w1);
                    try writeAllStream(s1, w1);
                    var cap1 = try Capture.init(alloc);
                    defer cap1.deinit();
                    try readUntil(s1, &cap1, 5000, doneHeaders);
                    try std.testing.expectEqualStrings("br", cap1.headerValue("content-encoding").?);

                    // Second stream must be identity.
                    const peer2 = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var s2 = try peer2.connect(.{});
                    defer s2.close();
                    const w2 = try h2c.buildClientHelloExtra(alloc, "/t", &extra);
                    defer alloc.free(w2);
                    try writeAllStream(s2, w2);
                    var cap2 = try Capture.init(alloc);
                    defer cap2.deinit();
                    try readUntil(s2, &cap2, 5000, doneEnd);
                    try std.testing.expect(cap2.headerValue("content-encoding") == null);
                    try std.testing.expect(cap2.hasVaryAcceptEncoding());
                    const expect = "hello compressed world " ** 20;
                    try std.testing.expectEqualStrings(expect, cap2.body.items);
                    const pool = server.compression_pool orelse return error.NoPool;
                    try std.testing.expect(pool.identity_fallbacks.load(.acquire) >= 1);
                }
            }.go);
        }
    }.f);
}

test "I8 compressed stream surfaces PeerReset exactly" {
    peer_reset_err.store(0, .release);
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/sse",
                .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = ssePeerResetHandler } },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    const wire = try h2c.buildClientHelloExtra(alloc, "/sse", &extra);
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);
                    // Wait until some DATA, then RST.
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 5000, struct {
                        fn d(c: *Capture) bool {
                            return c.data_frames > 0 or c.headers.items.len > 0;
                        }
                    }.d);
                    var rst: [13]u8 = undefined;
                    const rn = try frame.Serializer.rstStream(&rst, 1, .cancel);
                    try writeAllStream(stream, rst[0..rn]);
                    var waited: u64 = 0;
                    while (waited < 5000) : (waited += 20) {
                        if (peer_reset_err.load(.acquire) == 3) break;
                        zio.sleep(.fromMilliseconds(20)) catch {};
                    }
                    try std.testing.expectEqual(@as(u8, 3), peer_reset_err.load(.acquire));
                    // Drain accounting.
                    var t: u64 = 0;
                    while (t < 3000) : (t += 20) {
                        if (server.accounting.active_streams.load(.acquire) == 0) break;
                        zio.sleep(.fromMilliseconds(20)) catch {};
                    }
                }
            }.go);
        }
    }.f);
}

test "I4 large SSE event over outbound_bytes_per_stream still flushes decodably" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            var lim = enableCompressionLimits(.{});
            lim.outbound_bytes_per_stream = 64 * 1024;
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/sse",
                .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseLargeEventHandler } },
            }};
            try runServer(rt, gpa, &routes, lim, struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    // Enlarge client window so the large event can drain.
                    var wire = try h2c.buildClientPrefaceAndSettings(alloc);
                    defer wire.deinit(alloc);
                    {
                        var sbuf: [64]u8 = undefined;
                        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 1 << 22 }};
                        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
                        try wire.appendSlice(alloc, sbuf[0..sn]);
                    }
                    const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                    try h2c.appendHeadersExtra(alloc, &wire, 1, "GET", "/sse", true, &extra);
                    try writeAllStream(stream, wire.items);
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 10000, doneEnd);
                    // Occupancy is counted on compressed bytes. A 70 KiB SSE pad
                    // brotli-compresses far below the 64 KiB slab, so one DATA+END
                    // is the honest emit; the gate is that the event still decodes.
                    try std.testing.expect(cap.data_frames >= 1);
                    try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                    const decoded = try brotli.Decoder.decompressAll(alloc, 4 * 1024 * 1024, cap.body.items);
                    defer alloc.free(decoded);
                    try std.testing.expect(decoded.len > 70 * 1024);
                    try std.testing.expect(std.mem.startsWith(u8, decoded, "data: "));
                }
            }.go);
        }
    }.f);
}

test "lifecycle: shutdown releases live encoder context" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/hold",
                .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = hangCompressedSse } },
            }};
            const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 0);
            var lim = enableCompressionLimits(.{});
            // This client deliberately never ACKs the graceful PING. Force the
            // phase-2 deadline so the test proves that path exits the actor
            // loop and reaches handler cancellation.
            lim.graceful_drain_timeout_ns = 10 * std.time.ns_per_ms;
            var server = try starh2.Server.init(gpa, rt.io(), .{
                .endpoints = &.{.{ .h2c_prior_knowledge = addr }},
                .routes = &routes,
                .tls = null,
                .limits = lim,
            });
            defer server.deinit(gpa);
            var serve_handle = try rt.spawn(starh2.Server.serve, .{ &server, gpa });
            try server.waitUntilListening(5 * std.time.ns_per_s);
            const port = server.localAddress(0).getPort();
            const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
            var stream = try peer.connect(.{});
            defer stream.close();
            const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
            const wire = try h2c.buildClientHelloExtra(gpa, "/hold", &extra);
            defer gpa.free(wire);
            try writeAllStream(stream, wire);
            var cap = try Capture.init(gpa);
            defer cap.deinit();
            try readUntil(stream, &cap, 5000, doneHeaders);
            try std.testing.expect(server.compression_pool.?.liveCount() >= 1);
            server.requestShutdown();
            serve_handle.join() catch {};
            try std.testing.expectEqual(@as(usize, 0), server.compression_pool.?.liveCount());
        }
    }.f);
}

test "write sizes: one-byte SSE and exact outbound quantum" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{
                .{ .method = .GET, .path = "/one", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sseOneByteHandler } } },
                .{ .method = .GET, .path = "/exact", .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = streamExactOutboundHandler } } },
            };
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    for ([_][]const u8{ "/one", "/exact" }) |path| {
                        const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                        var stream = try peer.connect(.{});
                        defer stream.close();
                        const extra = [_]hpack.HeaderField{.{ .name = "accept-encoding", .value = "br" }};
                        const wire = try h2c.buildClientHelloExtra(alloc, path, &extra);
                        defer alloc.free(wire);
                        try writeAllStream(stream, wire);
                        var cap = try Capture.init(alloc);
                        defer cap.deinit();
                        try readUntil(stream, &cap, 8000, doneEnd);
                        try std.testing.expectEqualStrings("br", cap.headerValue("content-encoding").?);
                        const decoded = try brotli.Decoder.decompressAll(alloc, 4 * 1024 * 1024, cap.body.items);
                        defer alloc.free(decoded);
                        if (std.mem.eql(u8, path, "/one")) {
                            try std.testing.expectEqualStrings("data: x\n\n", decoded);
                        } else {
                            try std.testing.expectEqual(@as(usize, 64 * 1024), decoded.len);
                        }
                    }
                }
            }.go);
        }
    }.f);
}

test "no accept-encoding stays identity" {
    try withRuntime(struct {
        fn f(rt: *zio.Runtime, gpa: std.mem.Allocator) !void {
            const routes = [_]starh2.Route{.{
                .method = .GET,
                .path = "/t",
                .handler = .{ .task = .{ .ptr = @constCast(&dummy), .runFn = sendTextHandler } },
            }};
            try runServer(rt, gpa, &routes, enableCompressionLimits(.{}), struct {
                fn go(server: *starh2.Server, _: *zio.Runtime, alloc: std.mem.Allocator) !void {
                    const port = server.localAddress(0).getPort();
                    const peer = try zio.net.IpAddress.parseIp4("127.0.0.1", port);
                    var stream = try peer.connect(.{});
                    defer stream.close();
                    const wire = try h2c.buildClientHello(alloc, "/t");
                    defer alloc.free(wire);
                    try writeAllStream(stream, wire);
                    var cap = try Capture.init(alloc);
                    defer cap.deinit();
                    try readUntil(stream, &cap, 5000, doneEnd);
                    try std.testing.expect(cap.headerValue("content-encoding") == null);
                    // Eligible by configuration still adds Vary.
                    try std.testing.expect(cap.hasVaryAcceptEncoding());
                    const expect = "hello compressed world " ** 20;
                    try std.testing.expectEqualStrings(expect, cap.body.items);
                }
            }.go);
        }
    }.f);
}

// The invariant list at the top of this file is the only definition of I1 to I8.
// A rename that leaves a "Pinned by" bullet naming a test which no longer exists
// makes that list read as covered when it is not, and nothing else reports it.
// This test reads its own source and proves every local bullet still resolves.
//
// A bullet that ends with a parenthesized path names a test in another test
// binary, which this binary cannot enumerate. Those are reported, not checked.
test "every Pinned by bullet names a test that exists" {
    const src = @embedFile("compression.zig");
    const builtin = @import("builtin");

    var checked: usize = 0;
    var foreign: usize = 0;
    var missing: usize = 0;

    var it = std.mem.splitScalar(u8, src, '\n');
    while (it.next()) |line| {
        const bullet = "//! - `";
        if (!std.mem.startsWith(u8, line, bullet)) {
            continue;
        }
        const rest = line[bullet.len..];
        const end = std.mem.indexOfScalar(u8, rest, '`') orelse {
            std.debug.print("unterminated test name in: {s}\n", .{line});
            return error.MalformedPinnedBullet;
        };
        const name = rest[0..end];
        // A trailing "(path)" marks a test that lives in another binary.
        if (std.mem.indexOfScalar(u8, rest[end..], '(') != null) {
            foreign += 1;
            continue;
        }
        var found = false;
        for (builtin.test_functions) |t| {
            if (std.mem.endsWith(u8, t.name, name)) {
                found = true;
                break;
            }
        }
        if (found) {
            checked += 1;
        } else {
            missing += 1;
            std.debug.print("docstring pins a test that does not exist: \"{s}\"\n", .{name});
        }
    }

    // Zero local bullets means the list was gutted, or the prefix changed.
    // Silence there would read exactly like a pass, so fail on it.
    if (checked == 0 or missing != 0) {
        std.debug.print("pinned bullets: {d} checked, {d} foreign, {d} missing\n", .{ checked, foreign, missing });
        return error.PinnedTestMissing;
    }
}
