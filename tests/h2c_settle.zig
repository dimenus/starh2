//! t-3794: the h2c `WritePump` settles each chunk of a failed batch by the
//! bytes the socket accepted.
//!
//! The real `WritePump` and `SocketWriter` write to one end of a Unix stream
//! socketpair. The other end never reads, and both ends have small kernel
//! buffers, so the batch (far larger than the socket holds) stops partway
//! with a prefix accepted. Then the peer closes, and the write fails with a
//! real socket error. No hook: the outcome is fixed by the sizes, not by
//! timing, because the batch cannot finish while the peer reads nothing.
//!
//! The first chunk is a small "response" with a ticket. The socket accepts
//! it in full before anything fails, so its ack must be ok: a `send` waiting
//! on that ticket returns ok, because a completed-ok ticket wins over the
//! `fail_all` that follows (`ticket_table.zig`, first settlement wins). The
//! chunk the failure cut through and every later one must fail, and the
//! pump's terminal posts (`fail_all`, then `shutdown`) follow in the old
//! order.
//!
//! Two arms: the batch is bigger than the writer's buffer, so the failure is
//! in `writeVecAll`; or it fits the buffer and is bigger than the socket, so
//! `writeVecAll` only copies and the failure is in `flush`.
const std = @import("std");
const zio = @import("zio");
const starh2 = @import("starh2");

const wire_pump = starh2.edge.wire_pump;
const WireChunk = wire_pump.WireChunk;
const WriteCompletion = wire_pump.WriteCompletion;
const wire_chunk_size = starh2.core.wire_const.WIRE_CHUNK_SIZE;

/// The response the peer receives in full before the failure.
const first_len = 64;
/// Small chunks after it, so more than one chunk lies inside the accepted
/// prefix and the cut is not always the second chunk.
const small_n = 4;
const small_len = 256;
/// Small kernel buffers on both ends, so the socket holds a few KiB at most.
const socket_buffer_bytes: c_int = 2048;

const Arm = enum {
    /// Then 11 chunks of 16 KiB: bigger than the writer's buffer, so
    /// `writeVecAll` drains to the socket and fails there.
    write_fails,
    /// Then 2 chunks of 4 KiB: inside the writer's buffer, so `writeVecAll`
    /// only copies and `flush` fails.
    flush_fails,

    fn rest(arm: Arm) struct { n: usize, len: usize } {
        return switch (arm) {
            .write_fails => .{ .n = 11, .len = 16 * 1024 },
            .flush_fails => .{ .n = 2, .len = 4 * 1024 },
        };
    }
};

const max_chunks = 16;

fn setBuffers(fd: std.c.fd_t) !void {
    inline for (.{ std.c.SO.SNDBUF, std.c.SO.RCVBUF }) |opt| {
        if (std.c.setsockopt(fd, std.c.SOL.SOCKET, opt, &socket_buffer_bytes, @sizeOf(c_int)) != 0) return error.SetSockOptFailed;
    }
}

fn runArm(gpa: std.mem.Allocator, io: std.Io, arm: Arm) !void {
    const fds = try zio.os.net.socketpair(.unix, .stream, .ip, .{ .nonblocking = true });
    var peer_open = true;
    defer if (peer_open) zio.os.net.close(fds[1]);
    defer zio.os.net.close(fds[0]);
    try setBuffers(fds[0]);
    try setBuffers(fds[1]);

    const rest = arm.rest();
    const n_chunks = 1 + small_n + rest.n;
    std.debug.assert(n_chunks <= max_chunks);
    var lens: [max_chunks]usize = undefined;
    var total: usize = 0;
    for (0..n_chunks) |i| {
        lens[i] = if (i == 0) first_len else if (i <= small_n) small_len else rest.len;
        total += lens[i];
    }
    switch (arm) {
        .write_fails => try std.testing.expect(total > wire_chunk_size),
        .flush_fails => try std.testing.expect(total < wire_chunk_size),
    }

    // Every chunk is queued before the pump starts, so they are one batch
    // (the pump gathers what is queued, up to 16). The zero chunk after them
    // is the shutdown sentinel: it ends the pump if nothing failed.
    var q_buf: [max_chunks + 1]WireChunk = undefined;
    var queue: zio.Channel(WireChunk) = .init(&q_buf);
    for (0..n_chunks) |i| {
        const bytes = try gpa.alloc(u8, lens[i]);
        @memset(bytes, @intCast('a' + i));
        try queue.trySend(.{
            .bytes = bytes,
            .len = lens[i],
            .ticket = i + 1,
            .ticket_slot = @intCast(i),
            .outbound_release = lens[i] - 9,
        });
    }
    try queue.trySend(.{});

    var c_buf: [max_chunks + 4]WriteCompletion = undefined;
    var completions: zio.Channel(WriteCompletion) = .init(&c_buf);
    var accepted: std.atomic.Value(u64) = .init(0);
    var accepted_ns: std.atomic.Value(u64) = .init(0);
    var pump: wire_pump.WritePump = .{
        .io = io,
        .stream = .{ .socket = .{ .handle = fds[0], .address = undefined } },
        .from_actor = &queue,
        .accepted = &accepted,
        .accepted_ns = &accepted_ns,
        .completions = &completions,
        .gpa = gpa,
    };
    var pump_task = try zio.spawn(wire_pump.WritePump.run, .{&pump});

    // Wait until the socket has taken the response and the small chunks, and then stopped
    // taking bytes (the pump is parked on a full socket), then close the
    // peer. Bounded, and every wait is on a count the socket moved.
    var stable: u32 = 0;
    var last: u64 = 0;
    var waited: u32 = 0;
    while (stable < 25) : (waited += 1) {
        if (waited > 2500) return error.SocketNeverStopped;
        try zio.sleep(.fromMilliseconds(2));
        const now = accepted.load(.acquire);
        if (now >= first_len + small_n * small_len and now == last) stable += 1 else stable = 0;
        last = now;
    }
    try std.testing.expect(last < total);
    zio.os.net.close(fds[1]);
    peer_open = false;
    pump_task.join();

    // The socket's count after the failure: the bytes it accepted from this
    // batch (the pump wrote nothing before it).
    const taken = accepted.load(.acquire);
    std.debug.print("h2c settle {t}: batch {d} bytes in {d} chunks, socket accepted {d}\n", .{ arm, total, n_chunks, taken });
    try std.testing.expect(taken >= first_len + small_n * small_len);
    try std.testing.expect(taken < total);

    var end: usize = 0;
    var oks: usize = 0;
    for (0..n_chunks) |i| {
        const c = completions.tryReceive() catch return error.MissingCompletion;
        end += lens[i];
        try std.testing.expectEqual(@as(u64, i + 1), c.ticket);
        try std.testing.expectEqual(@as(u32, @intCast(lens[i])), c.wire_len);
        try std.testing.expectEqual(lens[i] - 9, c.outbound_release);
        try std.testing.expect(!c.fail_all and !c.shutdown);
        const want_ok = end <= taken;
        if (c.ok != want_ok) {
            std.debug.print("h2c settle {t}: chunk {d} ends at byte {d}, the socket accepted {d}: ok={} but should be {}\n", .{ arm, i, end, taken, c.ok, want_ok });
            return error.WrongSettlement;
        }
        if (c.ok) oks += 1;
    }
    // The response the peer got in full settles ok; the failure cut at least
    // one chunk, which fails, as does everything after it.
    if (oks < 1 + small_n) return error.FullyAcceptedResponseFailed;
    try std.testing.expect(oks < n_chunks);
    const fail_all = completions.tryReceive() catch return error.MissingFailAll;
    try std.testing.expect(fail_all.fail_all and !fail_all.shutdown and fail_all.ticket == 0);
    const shutdown = completions.tryReceive() catch return error.MissingShutdown;
    try std.testing.expect(shutdown.shutdown and shutdown.ticket == 0);
    try std.testing.expectError(error.WouldBlock, completions.tryReceive());
}

fn runInRuntime(arm: Arm) !void {
    const gpa = std.testing.allocator;
    const rt = try zio.Runtime.init(gpa, .{ .executors = .exact(1) });
    defer rt.deinit();
    var h = try rt.spawn(runArm, .{ gpa, rt.io(), arm });
    try h.join();
}

test "h2c: a write that fails mid-batch acks ok the chunks the socket accepted in full (t-3794)" {
    try runInRuntime(.write_fails);
}

test "h2c: a flush that fails mid-batch acks ok the chunks the socket accepted in full (t-3794)" {
    try runInRuntime(.flush_fails);
}
