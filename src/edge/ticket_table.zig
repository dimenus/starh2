//! Ticket wait table owned exclusively by AckDrainer for completion posts.
//! Handlers reserve a slot (CAS), wait on its semaphore, then clear the slot.
//! No other task mutates slots except reserve (handler) and complete/failAll (AckDrainer).
const std = @import("std");
const zio = @import("zio");
const response = @import("../http/response.zig");

pub const TicketWait = struct {
    sem: zio.Semaphore = .{ .permits = 0 },
    ok: bool = false,
    in_use: std.atomic.Value(bool) = .init(false),
    ticket: std.atomic.Value(u64) = .init(0),
};

/// Test-only two-task barrier around reserve preclaim/postclaim.
pub const TestReserveBarrier = struct {
    /// Posted by reserve after first write_failed check (before claim).
    after_precheck: zio.Semaphore = .{ .permits = 0 },
    /// Waited by reserve before claiming — test posts after failAll.
    go_claim: zio.Semaphore = .{ .permits = 0 },
    /// Posted by reserve after successful claim, before postclaim recheck.
    after_claim: zio.Semaphore = .{ .permits = 0 },
    /// Waited by reserve before finishing — test may failAll again here.
    go_finish: zio.Semaphore = .{ .permits = 0 },
};

pub var test_reserve_barrier: ?*TestReserveBarrier = null;

pub const TicketTable = struct {
    slots: []TicketWait,
    next_ticket: std.atomic.Value(u64) = .init(1),
    write_failed: std.atomic.Value(bool) = .init(false),

    pub fn init(slots: []TicketWait) TicketTable {
        for (slots) |*s| s.* = .{};
        return .{ .slots = slots };
    }

    /// Returns ticket + slot index. Caller must waitTicket or releaseReserved on error before wait.
    pub fn reserve(self: *TicketTable) error{ OutOfMemory, WriteFailed }!struct { u64, u32 } {
        if (self.write_failed.load(.acquire)) return error.WriteFailed;
        if (test_reserve_barrier) |b| {
            b.after_precheck.post();
            b.go_claim.wait() catch {};
        }
        if (self.write_failed.load(.acquire)) return error.WriteFailed;
        const ticket = self.next_ticket.fetchAdd(1, .acq_rel);
        if (ticket == 0) return error.OutOfMemory;
        for (self.slots, 0..) |*slot, i| {
            if (slot.in_use.cmpxchgStrong(false, true, .acq_rel, .acquire) == null) {
                if (test_reserve_barrier) |b| {
                    b.after_claim.post();
                    b.go_finish.wait() catch {};
                }
                // Claim won — recheck failure. failAll may have scanned past this free slot
                // before our claim; without this check we wait forever.
                if (self.write_failed.load(.acquire)) {
                    slot.ticket.store(0, .release);
                    slot.in_use.store(false, .release);
                    return error.WriteFailed;
                }
                slot.ok = false;
                slot.ticket.store(ticket, .release);
                slot.sem = .{ .permits = 0 };
                // Recheck again after publishing ticket (failAll may race between checks).
                if (self.write_failed.load(.acquire)) {
                    slot.ok = false;
                    slot.sem.post(); // if failAll already posted, extra post is harmless for wait
                    slot.ticket.store(0, .release);
                    slot.in_use.store(false, .release);
                    return error.WriteFailed;
                }
                return .{ ticket, @intCast(i) };
            }
        }
        return error.OutOfMemory;
    }

    pub fn releaseReserved(self: *TicketTable, slot_i: u32) void {
        if (slot_i >= self.slots.len) return;
        const slot = &self.slots[slot_i];
        slot.ticket.store(0, .release);
        slot.in_use.store(false, .release);
    }

    pub fn complete(self: *TicketTable, slot_i: u32, ticket: u64, ok: bool) void {
        if (slot_i >= self.slots.len) return;
        const slot = &self.slots[slot_i];
        if (!slot.in_use.load(.acquire)) return;
        if (slot.ticket.load(.acquire) != ticket) return;
        slot.ok = ok;
        slot.sem.post();
    }

    /// Wake a reserved waiter without matching ticket (pending removed / terminal cause set).
    pub fn wake(self: *TicketTable, slot_i: u32, ticket: u64, ok: bool) void {
        self.complete(slot_i, ticket, ok);
    }

    pub fn failAll(self: *TicketTable) void {
        self.write_failed.store(true, .release);
        for (self.slots) |*slot| {
            if (!slot.in_use.load(.acquire)) continue;
            slot.ok = false;
            slot.sem.post();
        }
    }

    /// After wake: PeerReset/SlowConsumer win; writer fail → WriteFailed even if slot also closed.
    pub fn wait(self: *TicketTable, slot_i: u32, terminal: ?*response.SlotTerminal) response.ResponseError!void {
        if (slot_i >= self.slots.len) return error.OutOfMemory;
        const slot = &self.slots[slot_i];
        defer self.releaseReserved(slot_i);
        slot.sem.wait() catch {
            if (terminal) |t| if (t.getCause()) |c| return response.causeToError(c);
            return error.Canceled;
        };
        if (terminal) |t| if (t.getCause()) |c| {
            switch (c) {
                .peer_reset, .slow_consumer, .connection_closed, .server_shutdown => return response.causeToError(c),
                else => {},
            }
        };
        if (self.write_failed.load(.acquire) or !slot.ok) return error.WriteFailed;
        if (terminal) |t| if (t.getCause()) |c| return response.causeToError(c);
    }
};

test "ticket reserve wait complete reuse" {
    var storage: [4]TicketWait = undefined;
    var table = TicketTable.init(&storage);
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

test "ticket releaseReserved on abandon" {
    var storage: [2]TicketWait = undefined;
    var table = TicketTable.init(&storage);
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        const t = try table.reserve();
        table.releaseReserved(t[1]);
    }
    _ = try table.reserve();
}

test "reserve after failAll returns WriteFailed" {
    var storage: [2]TicketWait = undefined;
    var table = TicketTable.init(&storage);
    table.failAll();
    try std.testing.expectError(error.WriteFailed, table.reserve());
}

test "wait maps SlotTerminal slow_consumer over WriteFailed" {
    var storage: [1]TicketWait = undefined;
    var table = TicketTable.init(&storage);
    var term: response.SlotTerminal = .{};
    const a = try table.reserve();
    term.setCause(.slow_consumer);
    table.complete(a[1], a[0], false);
    try std.testing.expectError(error.SlowConsumer, table.wait(a[1], &term));
}

test "wait maps SlotTerminal connection_closed over WriteFailed" {
    var storage: [1]TicketWait = undefined;
    var table = TicketTable.init(&storage);
    var term: response.SlotTerminal = .{};
    const a = try table.reserve();
    term.setCause(.server_shutdown);
    table.complete(a[1], a[0], false);
    try std.testing.expectError(error.ConnectionClosed, table.wait(a[1], &term));
}
