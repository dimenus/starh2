//! Ticket wait table owned exclusively by AckDrainer for completion posts.
//! Handlers reserve a slot (CAS), wait on its semaphore, then clear the slot.
//! Handlers reserve/release, the actor links receipts that share one wire
//! chunk, and the AckDrainer completes/fails. Cross-task link fields are atomic.
//!
//! # What a ticket is for
//!
//! A handler needs to know that its bytes REACHED THE WIRE, not merely that
//! they were queued. Streaming needs it for backpressure, and `resp.send` needs
//! it to return an honest result. The write happens on another task, so a
//! ticket is the rendezvous between the handler that waits and the AckDrainer
//! that learns the outcome.
//!
//! # Lifecycle
//!
//! 1. `reserve` — the HANDLER claims a slot by CAS and gets a unique id.
//! 2. The id rides on a `WireChunk`, or on a pending scheduler entry.
//! 3. The WritePump writes, then posts a `WriteCompletion` carrying the id.
//! 4. `complete` — the ACK DRAINER sets `ok` and signals the event.
//! 5. `wait` — the handler wakes, reads the result, and frees the slot.
//!
//! The slot index accompanies the id everywhere, so a completion is an O(1)
//! index rather than a scan. The id is still compared, because the slot may
//! already have been reused by another handler; a stale completion must be
//! ignored, not applied to the new owner.
//!
//! # The two races this table exists to survive
//!
//! `reserve` re-checks `write_failed` THREE times: before the claim, after the
//! claim, and after the id is published. `failAll` walks the slots once, so it
//! can pass a slot that is claimed a moment later. Without the re-checks that
//! handler waits for a completion nobody will ever post.
//!
//! `wait` checks `ok` BEFORE the terminal cause, and that order is the whole
//! point of `wait`'s doc comment below.
const std = @import("std");
const response = @import("../http/response.zig");

pub const no_completion_slot = std.math.maxInt(u32);

pub const TicketWait = struct {
    event: std.Io.Event = .unset,
    ok: bool = false,
    in_use: std.atomic.Value(bool) = .init(false),
    ticket: std.atomic.Value(u64) = .init(0),
    /// Actor-written, AckDrainer-read link for tickets sharing one wire chunk.
    completion_next: std.atomic.Value(u32) = .init(no_completion_slot),
};

/// Test-only two-task barrier around reserve preclaim/postclaim.
pub const TestReserveBarrier = struct {
    /// Posted by reserve after first write_failed check (before claim).
    after_precheck: std.Io.Event = .unset,
    /// Waited by reserve before claiming — test posts after failAll.
    go_claim: std.Io.Event = .unset,
    /// Posted by reserve after successful claim, before postclaim recheck.
    after_claim: std.Io.Event = .unset,
    /// Waited by reserve before finishing — test may failAll again here.
    go_finish: std.Io.Event = .unset,
};

pub var test_reserve_barrier: ?*TestReserveBarrier = null;

pub const TicketTable = struct {
    io: std.Io,
    slots: []TicketWait,
    next_ticket: std.atomic.Value(u64) = .init(1),
    write_failed: std.atomic.Value(bool) = .init(false),

    pub fn init(io: std.Io, slots: []TicketWait) TicketTable {
        for (slots) |*s| s.* = .{};
        return .{ .io = io, .slots = slots };
    }

    /// Returns ticket + slot index. Caller must waitTicket or releaseReserved on error before wait.
    pub fn reserve(self: *TicketTable) error{ OutOfMemory, WriteFailed }!struct { u64, u32 } {
        if (self.write_failed.load(.acquire)) return error.WriteFailed;
        if (test_reserve_barrier) |b| {
            b.after_precheck.set(self.io);
            b.go_claim.wait(self.io) catch return error.WriteFailed;
        }
        if (self.write_failed.load(.acquire)) return error.WriteFailed;
        const ticket = self.next_ticket.fetchAdd(1, .acq_rel);
        if (ticket == 0) return error.OutOfMemory;
        for (self.slots, 0..) |*slot, i| {
            if (slot.in_use.cmpxchgStrong(false, true, .acq_rel, .acquire) == null) {
                if (test_reserve_barrier) |b| {
                    b.after_claim.set(self.io);
                    b.go_finish.wait(self.io) catch return error.WriteFailed;
                }
                // Claim won — recheck failure. failAll may have scanned past this free slot
                // before our claim; without this check we wait forever.
                if (self.write_failed.load(.acquire)) {
                    slot.ticket.store(0, .release);
                    slot.in_use.store(false, .release);
                    return error.WriteFailed;
                }
                slot.ok = false;
                slot.event.reset();
                slot.completion_next.store(no_completion_slot, .release);
                slot.ticket.store(ticket, .release);
                // Recheck again after publishing ticket (failAll may race between checks).
                if (self.write_failed.load(.acquire)) {
                    slot.ok = false;
                    slot.event.set(self.io);
                    slot.ticket.store(0, .release);
                    slot.in_use.store(false, .release);
                    return error.WriteFailed;
                }
                return .{ ticket, @intCast(i) };
            }
        }
        return error.OutOfMemory;
    }

    /// Link two reserved tickets that will complete on the same wire write.
    ///
    /// The actor publishes the link before queueing the chunk. The AckDrainer
    /// reads it before waking the current slot, because that wake lets the
    /// handler release and immediately reuse the slot.
    pub fn linkCompletion(self: *TicketTable, from_slot: u32, to_slot: u32) error{InvalidSlot}!void {
        if (from_slot >= self.slots.len or to_slot >= self.slots.len) return error.InvalidSlot;
        const from = &self.slots[from_slot];
        const to = &self.slots[to_slot];
        if (!from.in_use.load(.acquire) or !to.in_use.load(.acquire)) return error.InvalidSlot;
        from.completion_next.store(to_slot, .release);
    }

    /// AckDrainer-only snapshot. Call before `complete`, whose event wake lets
    /// the handler release and reuse this slot.
    pub fn completion(self: *TicketTable, slot_i: u32) ?struct { ticket: u64, next: u32 } {
        if (slot_i >= self.slots.len) return null;
        const slot = &self.slots[slot_i];
        if (!slot.in_use.load(.acquire)) return null;
        const ticket = slot.ticket.load(.acquire);
        if (ticket == 0) return null;
        return .{
            .ticket = ticket,
            .next = slot.completion_next.load(.acquire),
        };
    }

    pub fn releaseReserved(self: *TicketTable, slot_i: u32) void {
        if (slot_i >= self.slots.len) return;
        const slot = &self.slots[slot_i];
        slot.completion_next.store(no_completion_slot, .release);
        slot.ticket.store(0, .release);
        slot.in_use.store(false, .release);
    }

    /// AckDrainer-only. Both guards reject a STALE completion for a slot that
    /// has already been released and reused: a completion applied to the new
    /// owner would wake a handler whose write is still in flight.
    pub fn complete(self: *TicketTable, slot_i: u32, ticket: u64, ok: bool) void {
        if (slot_i >= self.slots.len) return;
        const slot = &self.slots[slot_i];
        if (!slot.in_use.load(.acquire)) return;
        if (slot.ticket.load(.acquire) != ticket) return;
        slot.ok = ok;
        slot.event.set(self.io);
    }

    /// Wake a reserved waiter without matching ticket (pending removed / terminal cause set).
    pub fn wake(self: *TicketTable, slot_i: u32, ticket: u64, ok: bool) void {
        self.complete(slot_i, ticket, ok);
    }

    /// The transport died. Wake every waiter and refuse every future reserve.
    ///
    /// The flag is stored BEFORE the sweep. In the other order, a handler could
    /// pass its pre-claim check, claim a slot the sweep had already walked past,
    /// and then wait forever. With the flag first, that handler's post-claim
    /// re-check sees it and returns `error.WriteFailed`.
    pub fn failAll(self: *TicketTable) void {
        self.write_failed.store(true, .release);
        for (self.slots) |*slot| {
            if (!slot.in_use.load(.acquire)) continue;
            slot.ok = false;
            slot.event.set(self.io);
        }
    }

    /// A completed-ok ticket is a fact about the past: the wire accepted these
    /// bytes before any teardown could matter to THIS write, so a terminal
    /// cause that lands after the ack must not rewrite the result. (t-537:
    /// resp.send returned ConnectionClosed for a fully delivered response
    /// whenever the peer closed in the wake-to-run gap — the ack had already
    /// posted ok, and the old order checked the cause first.) `ok` is set true
    /// only by complete(ok=true); failAll and teardown wakes set false, so on
    /// a failed wake the cause mapping below still names the reason.
    pub fn wait(self: *TicketTable, slot_i: u32, terminal: ?*response.SlotTerminal) response.ResponseError!void {
        if (slot_i >= self.slots.len) return error.OutOfMemory;
        const slot = &self.slots[slot_i];
        defer self.releaseReserved(slot_i);
        slot.event.wait(self.io) catch {
            if (terminal) |t| if (t.getCause()) |c| return response.causeToError(c);
            return error.Canceled;
        };
        if (slot.ok) return;
        if (terminal) |t| if (t.getCause()) |c| return response.causeToError(c);
        return error.WriteFailed;
    }
};

test "ticket reserve wait complete reuse" {
    var storage: [4]TicketWait = undefined;
    var table = TicketTable.init(std.testing.io, &storage);
    const a = try table.reserve();
    const b = try table.reserve();
    try std.testing.expect(a[0] != b[0]);
    table.complete(a[1], a[0], true);
    try table.wait(a[1], null);
    const c = try table.reserve();
    _ = c;
    table.complete(b[1], b[0], false);
    try std.testing.expectError(error.WriteFailed, table.wait(b[1], null));
}

test "completion links survive until every batched ticket is snapped" {
    var storage: [4]TicketWait = undefined;
    var table = TicketTable.init(std.testing.io, &storage);
    const a = try table.reserve();
    const b = try table.reserve();
    const c = try table.reserve();
    try table.linkCompletion(a[1], b[1]);
    try table.linkCompletion(b[1], c[1]);

    const a_meta = table.completion(a[1]).?;
    try std.testing.expectEqual(a[0], a_meta.ticket);
    try std.testing.expectEqual(b[1], a_meta.next);
    table.complete(a[1], a_meta.ticket, true);

    const b_meta = table.completion(a_meta.next).?;
    try std.testing.expectEqual(b[0], b_meta.ticket);
    try std.testing.expectEqual(c[1], b_meta.next);
    table.complete(b[1], b_meta.ticket, true);

    const c_meta = table.completion(b_meta.next).?;
    try std.testing.expectEqual(c[0], c_meta.ticket);
    try std.testing.expectEqual(no_completion_slot, c_meta.next);
    table.complete(c[1], c_meta.ticket, true);

    try table.wait(a[1], null);
    try table.wait(b[1], null);
    try table.wait(c[1], null);
}

test "ticket releaseReserved on abandon" {
    var storage: [2]TicketWait = undefined;
    var table = TicketTable.init(std.testing.io, &storage);
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        const t = try table.reserve();
        table.releaseReserved(t[1]);
    }
    _ = try table.reserve();
}

test "reserve after failAll returns WriteFailed" {
    var storage: [2]TicketWait = undefined;
    var table = TicketTable.init(std.testing.io, &storage);
    table.failAll();
    try std.testing.expectError(error.WriteFailed, table.reserve());
}

test "a completed-ok ticket survives a terminal cause that lands after the ack (t-537)" {
    var storage: [1]TicketWait = undefined;
    var table = TicketTable.init(std.testing.io, &storage);
    var term: response.SlotTerminal = .{};
    const a = try table.reserve();
    // The wire acked the write, THEN the peer closed (the wake-to-run gap).
    table.complete(a[1], a[0], true);
    term.setCause(.connection_closed);
    // The delivered write must report success; the cause belongs to the NEXT operation.
    try table.wait(a[1], &term);

    // Same for a global writer failure that lands after this ack.
    const b = try table.reserve();
    table.complete(b[1], b[0], true);
    table.write_failed.store(true, .release);
    try table.wait(b[1], null);
    table.write_failed.store(false, .release);
}

test "wait maps SlotTerminal slow_consumer over WriteFailed" {
    var storage: [1]TicketWait = undefined;
    var table = TicketTable.init(std.testing.io, &storage);
    var term: response.SlotTerminal = .{};
    const a = try table.reserve();
    term.setCause(.slow_consumer);
    table.complete(a[1], a[0], false);
    try std.testing.expectError(error.SlowConsumer, table.wait(a[1], &term));
}

test "wait maps SlotTerminal connection_closed over WriteFailed" {
    var storage: [1]TicketWait = undefined;
    var table = TicketTable.init(std.testing.io, &storage);
    var term: response.SlotTerminal = .{};
    const a = try table.reserve();
    term.setCause(.server_shutdown);
    table.complete(a[1], a[0], false);
    try std.testing.expectError(error.ConnectionClosed, table.wait(a[1], &term));
}
