//! Sole owners of the raw zio stream Reader and Writer directions.
//! Pumps never mutate Connection state — they only exchange WireChunk / WriteCompletion
//! messages carrying integer tickets and release amounts.
const std = @import("std");
const zio = @import("zio");
const limits_mod = @import("../core/wire_const.zig");

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
    stream: zio.net.Stream,
    to_actor: *zio.Channel(WireChunk),
    /// Contiguous storage: n_chunks * WIRE_CHUNK_SIZE.
    chunk_storage: []u8,
    n_chunks: u32,
    /// Free pool indices (actor returns after consume).
    free_indices: *zio.Channel(u32),
    stopped: std.atomic.Value(bool) = .init(false),

    pub fn run(self: *ReadPump) !void {
        const chunk_size = limits_mod.WIRE_CHUNK_SIZE;
        while (!self.stopped.load(.acquire)) {
            const idx = self.free_indices.receive() catch return;
            const off = @as(usize, idx) * chunk_size;
            const buf = self.chunk_storage[off..][0..chunk_size];
            const n = self.stream.read(buf, .none) catch |err| {
                self.free_indices.send(idx) catch {};
                if (err == error.Canceled) return;
                self.to_actor.send(.{ .bytes = &.{}, .len = 0 }) catch {};
                return;
            };
            if (n == 0) {
                self.free_indices.send(idx) catch {};
                self.to_actor.send(.{ .bytes = &.{}, .len = 0 }) catch {};
                return;
            }
            self.to_actor.send(.{
                .bytes = buf,
                .len = n,
                .pool_index = idx,
            }) catch {
                self.free_indices.send(idx) catch {};
                return;
            };
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
    stream: zio.net.Stream,
    from_actor: *zio.Channel(WireChunk),
    completions: *zio.Channel(WriteCompletion),
    gpa: std.mem.Allocator,
    stopped: std.atomic.Value(bool) = .init(false),
    test_delay_ms: u64 = 0,
    test_fail_after: u64 = 0,
    writes_done: u64 = 0,

    fn post(self: *WritePump, c: WriteCompletion) void {
        self.completions.send(c) catch {};
    }

    fn releaseChunk(self: *WritePump, chunk: WireChunk, ok: bool, fail_all: bool) void {
        if (chunk.bytes.len != 0) {
            self.gpa.free(chunk.bytes);
        }
        const has_ticket = chunk.ticket != 0;
        const has_acct = chunk.outbound_release != 0 or chunk.control_entry;
        if (has_ticket or has_acct or fail_all) {
            if (ok and chunk.ticket != 0) {
                test_last_ticket_ok_ns.store(zio.Timestamp.now(.monotonic).toNanoseconds(), .release);
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
        while (self.from_actor.tryReceive()) |chunk| {
            if (chunk.len == 0 and chunk.bytes.len == 0 and !chunk.flush_barrier) {
                self.post(.{ .fail_all = true, .shutdown = false });
                return;
            }
            self.releaseChunk(chunk, false, false);
        } else |_| {}
        self.post(.{ .fail_all = true });
    }

    pub fn run(self: *WritePump) !void {
        while (!self.stopped.load(.acquire)) {
            const chunk = self.from_actor.receive() catch {
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
                zio.sleep(.fromMilliseconds(self.test_delay_ms)) catch {};
            }
            const fail_next = test_fail_next_write.swap(false, .acq_rel);
            if (fail_next or (self.test_fail_after > 0 and self.writes_done >= self.test_fail_after)) {
                self.releaseChunk(chunk, false, false);
                self.failDrain();
                self.post(.{ .shutdown = true });
                return;
            }
            self.stream.writeAll(chunk.bytes[0..chunk.len], .none) catch {
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

test "WriteCompletion is integer-only payload" {
    try std.testing.expect(@sizeOf(WriteCompletion) <= 64);
}
