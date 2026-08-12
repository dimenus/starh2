//! Sole owners of the raw std.Io stream Reader and Writer directions.
//! Pumps never mutate Connection state — they only exchange WireChunk / WriteCompletion
//! messages carrying integer tickets and release amounts.
const std = @import("std");
const limits_mod = @import("../core/wire_const.zig");
const io_queue = @import("io_queue.zig");

pub const WriteCompletion = struct {
    /// Nonzero: complete this ticket wait slot.
    ticket: u64 = 0,
    ticket_slot: u32 = 0,
    ok: bool = true,
    /// Connection-local outbound bytes to release (AckDrainer applies).
    outbound_release: usize = 0,
    /// Control-pool bytes/entry to release (AckDrainer applies).
    control_release: usize = 0,
    control_entry: bool = false,
    /// Fail every in-flight ticket (write pump transport failure).
    fail_all: bool = false,
    /// Shut down AckDrainer.
    shutdown: bool = false,
};

pub const WireChunk = struct {
    bytes: []u8 = &.{},
    len: usize = 0,
    flush_barrier: bool = false,
    ticket: u64 = 0,
    ticket_slot: u32 = 0,
    /// Amounts echoed into WriteCompletion after write/free (no Connection callback).
    outbound_release: usize = 0,
    control_release: usize = 0,
    control_entry: bool = false,
    /// Read-pool lease index; null = heap-owned (should not happen for reads after boot).
    pool_index: ?u32 = null,
};

pub const ReadPump = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
    to_actor: *std.Io.Queue(WireChunk),
    actor_wake: *std.Io.Event,
    /// Contiguous storage: n_chunks * WIRE_CHUNK_SIZE.
    chunk_storage: []u8,
    n_chunks: u32,
    /// Free pool indices (actor returns after consume).
    free_indices: *std.Io.Queue(u32),
    stopped: std.atomic.Value(bool) = .init(false),

    fn returnIndex(self: *ReadPump, idx: u32) void {
        self.free_indices.putOneUncancelable(self.io, idx) catch {
            // Queue closure means connection teardown owns the backing pool.
        };
    }

    fn postEof(self: *ReadPump) void {
        self.to_actor.putOneUncancelable(self.io, .{ .bytes = &.{}, .len = 0 }) catch {
            // A closed actor queue means the connection is already tearing down.
        };
        self.actor_wake.set(self.io);
    }

    pub fn run(self: *ReadPump) void {
        const chunk_size = limits_mod.WIRE_CHUNK_SIZE;
        var reader = self.stream.reader(self.io, &.{});
        while (!self.stopped.load(.acquire)) {
            const idx = self.free_indices.getOne(self.io) catch return;
            const off = @as(usize, idx) * chunk_size;
            const buf = self.chunk_storage[off..][0..chunk_size];
            var dest: [1][]u8 = .{buf};
            const n = reader.interface.readVec(&dest) catch {
                self.returnIndex(idx);
                self.postEof();
                return;
            };
            if (n == 0) {
                self.returnIndex(idx);
                self.postEof();
                return;
            }
            self.to_actor.putOne(self.io, .{
                .bytes = buf,
                .len = n,
                .pool_index = idx,
            }) catch {
                self.returnIndex(idx);
                return;
            };
            self.actor_wake.set(self.io);
        }
    }

    pub fn stop(self: *ReadPump) void {
        self.stopped.store(true, .release);
    }
};

/// Test-only: when true, the next WritePump write fails and the flag clears.
pub var test_fail_next_write: std.atomic.Value(bool) = .init(false);
/// Test-only: monotonic ns of last successful WritePump completion that carried a ticket.
pub var test_last_ticket_ok_ns: std.atomic.Value(u64) = .init(0);
/// Test-only: ticket id of that completion (0 = none).
pub var test_last_ticket_ok_id: std.atomic.Value(u64) = .init(0);

pub const WritePump = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
    from_actor: *std.Io.Queue(WireChunk),
    completions: *std.Io.Queue(WriteCompletion),
    actor_wake: *std.Io.Event,
    gpa: std.mem.Allocator,
    stopped: std.atomic.Value(bool) = .init(false),
    test_delay_ms: u64 = 0,
    test_fail_after: u64 = 0,
    writes_done: u64 = 0,

    fn post(self: *WritePump, c: WriteCompletion) void {
        self.completions.putOneUncancelable(self.io, c) catch {
            // Ack queue closure means connection teardown will release queued ownership.
        };
        self.actor_wake.set(self.io);
    }

    fn releaseChunk(self: *WritePump, chunk: WireChunk, ok: bool, fail_all: bool) void {
        if (chunk.bytes.len != 0) {
            self.gpa.free(chunk.bytes);
        }
        const has_ticket = chunk.ticket != 0;
        const has_acct = chunk.outbound_release != 0 or chunk.control_entry;
        if (has_ticket or has_acct or fail_all) {
            if (ok and chunk.ticket != 0) {
                test_last_ticket_ok_ns.store(nowNs(self.io), .release);
                test_last_ticket_ok_id.store(chunk.ticket, .release);
            }
            self.post(.{
                .ticket = chunk.ticket,
                .ticket_slot = chunk.ticket_slot,
                .ok = ok,
                .outbound_release = chunk.outbound_release,
                .control_release = chunk.control_release,
                .control_entry = chunk.control_entry,
                .fail_all = fail_all,
            });
        }
    }

    fn failDrain(self: *WritePump) void {
        // Transport failed: every remaining queued chunk must free + fail its ticket.
        while (io_queue.tryGet(WireChunk, self.from_actor, self.io)) |chunk| {
            if (chunk.len == 0 and chunk.bytes.len == 0 and !chunk.flush_barrier) {
                self.post(.{ .fail_all = true, .shutdown = false });
                return;
            }
            self.releaseChunk(chunk, false, false);
        }
        self.post(.{ .fail_all = true });
    }

    pub fn run(self: *WritePump) void {
        var writer = self.stream.writer(self.io, &.{});
        while (!self.stopped.load(.acquire)) {
            const chunk = self.from_actor.getOne(self.io) catch {
                self.post(.{ .fail_all = true, .shutdown = true });
                return;
            };
            if (chunk.len == 0 and chunk.bytes.len == 0) {
                if (chunk.flush_barrier) {
                    // Empty flush barrier: success after prior writes already completed.
                    self.releaseChunk(chunk, true, false);
                    continue;
                }
                // Shutdown sentinel.
                self.post(.{ .shutdown = true });
                return;
            }
            if (self.test_delay_ms > 0) {
                self.io.sleep(.fromMilliseconds(@intCast(self.test_delay_ms)), .awake) catch {
                    // Cancellation terminates this pump; queued chunks are failed below.
                    self.releaseChunk(chunk, false, false);
                    self.failDrain();
                    self.post(.{ .shutdown = true });
                    return;
                };
            }
            const fail_next = test_fail_next_write.swap(false, .acq_rel);
            if (fail_next or (self.test_fail_after > 0 and self.writes_done >= self.test_fail_after)) {
                self.releaseChunk(chunk, false, false);
                self.failDrain();
                self.post(.{ .shutdown = true });
                return;
            }
            writer.interface.writeAll(chunk.bytes[0..chunk.len]) catch {
                self.releaseChunk(chunk, false, false);
                self.failDrain();
                self.post(.{ .shutdown = true });
                return;
            };
            self.writes_done += 1;
            self.releaseChunk(chunk, true, false);
        }
        self.post(.{ .shutdown = true });
    }

    pub fn stop(self: *WritePump) void {
        self.stopped.store(true, .release);
    }
};

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

test "WriteCompletion is integer-only payload" {
    try std.testing.expect(@sizeOf(WriteCompletion) <= 64);
}
