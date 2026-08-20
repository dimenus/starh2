//! Sole owners of the raw std.Io stream Reader and Writer directions.
//! Pumps never mutate Connection state — they only exchange WireChunk / WriteCompletion
//! messages carrying integer tickets and release amounts.
//!
//! Why the pumps exist as separate tasks: a socket read and a socket write both
//! block. If the actor performed either one, a peer that stops reading would
//! stop the actor, and the actor is what must notice that fact and react. So
//! the blocking calls live in tasks that own nothing else, and the actor only
//! ever exchanges messages with them.
//!
//! Why they hold no pointer into `Connection`: message passing is what removes
//! the need for a lock on this path. A `WireChunk` carries integers and one
//! slice, so a pump can be canceled at any point without leaving shared state
//! half-written. Every consequence of a write — released bytes, a completed
//! ticket, a control-pool entry — travels back as data in a `WriteCompletion`
//! and is applied by the actor.
//!
//! ## The WritePump exit contract
//!
//! Every queued chunk gets exactly one outcome on every exit path: an ok ack, a
//! failed ack, or a `fail_all`. Nothing is ever dropped silently. That contract
//! is what lets a handler wait on a ticket with no timeout, and it is why
//! teardown may leave a waiting handler alone instead of canceling it. If a
//! future change adds an exit path that skips `failDrain`, handlers will hang.
const std = @import("std");
const zio = @import("zio");
const limits_mod = @import("../core/wire_const.zig");
const io_queue = @import("io_queue.zig");

pub const WriteCompletion = struct {
    /// Nonzero: complete this ticket wait slot.
    ticket: u64 = 0,
    ticket_slot: u32 = 0,
    /// Number of ticket slots linked from `ticket_slot` for this one write.
    ticket_count: u32 = 0,
    ok: bool = true,
    /// Connection-local outbound bytes to release (actor applies).
    outbound_release: usize = 0,
    /// Successful socket-write completion time for trace attribution.
    written_ns: u64 = 0,
    /// Control-pool bytes and entry count to release (actor applies).
    control_release: usize = 0,
    control_entries: u32 = 0,
    /// Fail every in-flight ticket (write pump transport failure).
    fail_all: bool = false,
    /// Last completion the WritePump posts; actor treats remaining acks as done.
    shutdown: bool = false,
    /// This write carries a complete-handler drain-turn receipt. The actor
    /// publishes credit only for these, not for ordinary/SSE tickets.
    complete_batch_receipt: bool = false,
};

pub const WireChunk = struct {
    bytes: []u8 = &.{},
    len: usize = 0,
    flush_barrier: bool = false,
    ticket: u64 = 0,
    ticket_slot: u32 = 0,
    /// Number of ticket slots linked from `ticket_slot` for this one write.
    ticket_count: u32 = 0,
    /// Amounts echoed into WriteCompletion after write/free (no Connection callback).
    outbound_release: usize = 0,
    control_release: usize = 0,
    control_entries: u32 = 0,
    /// Read-pool lease index; null = heap-owned (should not happen for reads after boot).
    pool_index: ?u32 = null,
    /// Echoed into WriteCompletion: this chunk's ticket is a complete-batch receipt.
    complete_batch_receipt: bool = false,
};

/// Reads the socket into pooled chunks and hands them to the actor.
///
/// The chunk pool inverts the usual flow-control problem. The pump must LEASE a
/// free index before it reads, so when the actor falls behind, the free queue
/// empties and the pump simply blocks. The kernel receive buffer then fills and
/// TCP applies backpressure to the peer. No allocation and no unbounded queue
/// exist anywhere on this path.
///
/// Ownership of an index moves: free queue -> pump -> actor -> free queue. The
/// actor returns it in a `defer` immediately after it consumes the bytes. An
/// index that is not returned is a permanent loss of read capacity, and enough
/// of them stall the connection.
///
/// End of stream is reported as a zero-length chunk rather than by closing the
/// queue, so the actor sees it in the same place as every other read and can
/// finish its normal teardown.
pub const ReadPump = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
    /// A zio channel: the actor selects on it, so a post IS the wake.
    to_actor: *zio.Channel(WireChunk),
    /// Contiguous storage: n_chunks * WIRE_CHUNK_SIZE.
    chunk_storage: []u8,
    n_chunks: u32,
    /// Free pool indices (actor returns after consume).
    free_indices: *std.Io.Queue(u32),
    /// Connection-owned count of spawned task handlers. Yield only while this
    /// is non-zero — oneshot-only must not donate on every read.
    live_task_handlers: *std.atomic.Value(usize),
    stopped: std.atomic.Value(bool) = .init(false),

    fn returnIndex(self: *ReadPump, idx: u32) void {
        self.free_indices.putOneUncancelable(self.io, idx) catch {
            // Queue closure means connection teardown owns the backing pool.
        };
    }

    fn postEof(self: *ReadPump) void {
        // The channel holds capacity n_chunks + 1: the extra slot is this
        // sentinel, so the post cannot find the channel full.
        self.to_actor.trySend(.{ .bytes = &.{}, .len = 0 }) catch {
            // A closed actor channel means the connection is already tearing down.
        };
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
                // Unblock a write parked on a peer that already closed. The
                // actor may be in `write_ch.putOne` holding `session_mu` and
                // cannot observe this EOF until that put returns.
                self.stream.shutdown(self.io, .send) catch {};
                self.postEof();
                return;
            };
            if (n == 0) {
                self.returnIndex(idx);
                self.stream.shutdown(self.io, .send) catch {};
                self.postEof();
                return;
            }
            self.to_actor.trySend(.{
                .bytes = buf,
                .len = n,
                .pool_index = idx,
            }) catch |err| switch (err) {
                // The leased index is the capacity proof: at most n_chunks
                // pooled chunks exist, and the channel holds n_chunks + 1.
                error.WouldBlock => unreachable,
                error.Closed => {
                    self.returnIndex(idx);
                    return;
                },
            };
            if (self.live_task_handlers.load(.acquire) > 0) {
                self.io.sleep(.zero, .awake) catch {};
            }
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

/// t-866 ticket-ledger diagnostics (process-global, diag reads only).
pub const diag_acks = struct {
    /// WriteCompletions posted by pumps with ticket != 0.
    pub var posted_ticket: std.atomic.Value(u64) = .init(0);
    /// Of those, carrying complete_batch_receipt.
    pub var posted_receipt: std.atomic.Value(u64) = .init(0);
    /// Acks with ticket != 0 applied by actors.
    pub var applied_ticket: std.atomic.Value(u64) = .init(0);
    /// Of those, credited as complete-batch receipts.
    pub var applied_receipt: std.atomic.Value(u64) = .init(0);
    /// Tickets reserved / completed / failed in ticket tables.
    pub var reserved: std.atomic.Value(u64) = .init(0);
    pub var completed: std.atomic.Value(u64) = .init(0);
    /// Outbound-release BYTES posted by pumps / applied by actors — the
    /// conservation ledger for the wire-held accounting.
    pub var posted_release: std.atomic.Value(u64) = .init(0);
    pub var applied_release: std.atomic.Value(u64) = .init(0);
};

pub const write_trace = struct {
    pub var enabled: bool = false;
    pub var calls: std.atomic.Value(u64) = .init(0);
    pub var chunks: std.atomic.Value(u64) = .init(0);
    pub var max_chunks: std.atomic.Value(u64) = .init(0);

    pub fn note(count: usize) void {
        const count_u64: u64 = @intCast(count);
        _ = calls.fetchAdd(1, .monotonic);
        _ = chunks.fetchAdd(count_u64, .monotonic);
        var cur = max_chunks.load(.monotonic);
        while (count_u64 > cur) {
            cur = max_chunks.cmpxchgWeak(cur, count_u64, .monotonic, .monotonic) orelse break;
        }
    }
};

pub const WritePump = struct {
    io: std.Io,
    stream: std.Io.net.Stream,
    from_actor: *std.Io.Queue(WireChunk),
    /// A zio channel: the actor selects on it, so a post IS the wake. The
    /// capacity is the proven outstanding-ack bound; `post` never parks.
    completions: *zio.Channel(WriteCompletion),
    gpa: std.mem.Allocator,
    /// When set, `WireChunk.pool_index` is returned here instead of `gpa.free`.
    /// Tests that feed GPA-owned chunks leave this null.
    free_indices: ?*std.Io.Queue(u32) = null,
    stopped: std.atomic.Value(bool) = .init(false),
    test_delay_ms: u64 = 0,
    test_fail_after: u64 = 0,
    writes_done: u64 = 0,

    fn post(self: *WritePump, c: WriteCompletion) void {
        _ = diag_acks.posted_release.fetchAdd(c.outbound_release, .monotonic);
        self.completions.trySend(c) catch |err| switch (err) {
            // A dropped completion is a hung handler; a full channel means the
            // capacity proof is wrong. Fail loudly, never silently.
            error.WouldBlock => @panic("write ack channel over proven capacity"),
            error.Closed => {
                // Ack channel closure means connection teardown will release queued ownership.
            },
        };
    }

    fn releaseChunk(self: *WritePump, chunk: WireChunk, ok: bool, fail_all: bool) void {
        if (chunk.pool_index) |idx| {
            const q = self.free_indices orelse unreachable;
            // Never block: the actor is the only consumer of this queue and may
            // already be parked on write_ch. An uncancelable put into a full
            // free list deadlocks shutdown (pump cannot fail-drain, actor
            // cannot take an index). A failed put is a duplicate return; leak
            // the index and let later sends GPA-fallback.
            if (!io_queue.tryPut(u32, q, self.io, idx)) {
                std.debug.assert(false);
            }
        } else if (chunk.bytes.len != 0) {
            self.gpa.free(chunk.bytes);
        }
        const has_ticket = chunk.ticket_count != 0 or chunk.ticket != 0;
        const has_acct = chunk.outbound_release != 0 or chunk.control_entries != 0;
        if (has_ticket or has_acct or fail_all) {
            var written_ns: u64 = 0;
            if (ok and chunk.ticket != 0) {
                written_ns = nowNs(self.io);
                test_last_ticket_ok_ns.store(written_ns, .release);
                test_last_ticket_ok_id.store(chunk.ticket, .release);
            }
            self.post(.{
                .ticket = chunk.ticket,
                .ticket_slot = chunk.ticket_slot,
                .ticket_count = if (chunk.ticket_count != 0) chunk.ticket_count else if (chunk.ticket != 0) 1 else 0,
                .ok = ok,
                .outbound_release = chunk.outbound_release,
                .written_ns = written_ns,
                .control_release = chunk.control_release,
                .control_entries = chunk.control_entries,
                .fail_all = fail_all,
                .complete_batch_receipt = chunk.complete_batch_receipt,
            });
        }
    }

    /// The transport is dead. Every chunk still queued must be freed and its
    /// ticket failed, or the handlers waiting on those tickets never wake.
    ///
    /// The drain stops early if it meets the shutdown sentinel, because nothing
    /// after it was queued by a live writer. `fail_all` is posted in both exits,
    /// so `TicketTable.failAll` also catches tickets that were reserved but not
    /// yet attached to any chunk.
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

    /// Write chunks in FIFO order, gathering already-queued chunks into the
    /// buffered writer. Completions fire only after an explicit flush, so a
    /// ticket still means the bytes reached the socket — not merely this
    /// buffer. Frame ORDER is decided upstream by the `FairScheduler`.
    ///
    /// Two zero-length chunks mean different things, and the flag tells them
    /// apart. With `flush_barrier` it is a flush that had no bytes of its own:
    /// drain the buffer, then complete. Without it, it is the shutdown sentinel.
    pub fn run(self: *WritePump) void {
        var write_buf: [limits_mod.WIRE_CHUNK_SIZE]u8 = undefined;
        var writer = self.stream.writer(self.io, &write_buf);
        var carried: ?WireChunk = null;
        while (!self.stopped.load(.acquire)) {
            const first = if (carried) |chunk| blk: {
                carried = null;
                break :blk chunk;
            } else self.from_actor.getOne(self.io) catch {
                self.failDrain();
                self.post(.{ .fail_all = true, .shutdown = true });
                return;
            };
            if (first.len == 0 and first.bytes.len == 0) {
                if (first.flush_barrier) {
                    // Empty flush barrier: drain the buffer, then complete.
                    writer.interface.flush() catch {
                        self.releaseChunk(first, false, false);
                        self.failDrain();
                        self.post(.{ .shutdown = true });
                        return;
                    };
                    self.releaseChunk(first, true, false);
                    continue;
                }
                // Shutdown sentinel. Drain anything still queued (a writer
                // that raced the sentinel) so those tickets still fail.
                self.failDrain();
                self.post(.{ .shutdown = true });
                return;
            }

            const max_batch = 16;
            var chunks: [max_batch]WireChunk = undefined;
            var slices: [max_batch][]const u8 = undefined;
            chunks[0] = first;
            var count: usize = 1;
            // Keep fault-injection timing exact: those tests describe one
            // attempted chunk per configured delay/failure boundary.
            if (self.test_delay_ms == 0 and self.test_fail_after == 0) {
                while (count < max_batch) {
                    const next = io_queue.tryGet(WireChunk, self.from_actor, self.io) orelse break;
                    if (next.len == 0 and next.bytes.len == 0) {
                        carried = next;
                        break;
                    }
                    chunks[count] = next;
                    count += 1;
                }
            }
            if (self.test_delay_ms > 0) {
                self.io.sleep(.fromMilliseconds(@intCast(self.test_delay_ms)), .awake) catch {
                    // Cancellation terminates this pump; queued chunks are failed below.
                    self.releaseChunk(first, false, false);
                    self.failDrain();
                    self.post(.{ .shutdown = true });
                    return;
                };
            }
            const fail_next = test_fail_next_write.swap(false, .acq_rel);
            if (fail_next or (self.test_fail_after > 0 and self.writes_done >= self.test_fail_after)) {
                for (chunks[0..count]) |chunk| self.releaseChunk(chunk, false, false);
                self.failDrain();
                self.post(.{ .shutdown = true });
                return;
            }
            for (chunks[0..count], 0..) |chunk, i| {
                slices[i] = chunk.bytes[0..chunk.len];
            }
            if (write_trace.enabled) write_trace.note(count);
            const write_result = if (count == 1)
                writer.interface.writeAll(slices[0])
            else
                writer.interface.writeVecAll(slices[0..count]);
            write_result catch {
                for (chunks[0..count]) |chunk| self.releaseChunk(chunk, false, false);
                self.failDrain();
                self.post(.{ .shutdown = true });
                return;
            };
            // Ticket completions stay behind this flush. writeAll into the
            // buffer is not delivery; a cancelled pump must not ack a receipt
            // whose bytes are still sitting here.
            writer.interface.flush() catch {
                for (chunks[0..count]) |chunk| self.releaseChunk(chunk, false, false);
                self.failDrain();
                self.post(.{ .shutdown = true });
                return;
            };
            self.writes_done += count;
            for (chunks[0..count]) |chunk| self.releaseChunk(chunk, true, false);
        }
        writer.interface.flush() catch {};
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
