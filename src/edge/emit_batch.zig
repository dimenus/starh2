//! Drain-turn wire accumulator.
//!
//! Connection copies every batchable frame from one FairScheduler drain into
//! one scratch buffer and hands it to queueWire as one input. On TLS that
//! input is one SSL_write; on h2c it is one socket write. The rules here are
//! the ones that used to live only inside `drainEmit`'s nested sink, which is
//! why a second HEADERS silently started a new write: nothing could name the
//! invariant without standing up a cipher.
//!
//! Occupancy of the control pool stays held until that write completes. The
//! batch therefore counts entries, not a bool. Releasing 1 after N HEADERS
//! leaks N-1 slots for the life of the connection.
const std = @import("std");
const wire_const = @import("../core/wire_const.zig");

/// TLS 1.3 application plaintext cap. HTTP/2 packing stops here; SSL_write is
/// TlsPump's flush, not a second record loop on the actor.
pub const max_plaintext = wire_const.TLS_PLAINTEXT_SCRATCH_SIZE;

pub const WireMeta = struct {
    ticket: u64 = 0,
    ticket_slot: u32 = 0,
    ticket_count: u32 = 0,
    control_n: usize = 0,
    control_entries: u32 = 0,
};

/// Ticket, flush, and control-pool release ride on the LAST TLS record of one
/// plaintext input. A receipt on an earlier record would tell handlers their
/// writes were delivered while later bytes from the same input were still queued.
pub fn lastRecordMeta(is_last: bool, meta: WireMeta) WireMeta {
    if (is_last) return meta;
    return .{};
}

/// An earlier record of a split encrypt must not flush the TLS layer: the
/// later records of the same plaintext have not been queued yet.
pub fn lastRecordFlush(is_last: bool, flush: bool) bool {
    return is_last and flush;
}

/// Unticketed DATA keeps the immediate per-frame handoff when the scratch is
/// empty. A single frame larger than the scratch cannot join and is also
/// immediate. Control frames and ticketed DATA concat into one queueWire, on
/// h2c and TLS alike.
pub fn frameIsBatchable(
    control_entry: bool,
    ticket: u64,
    payload_len: usize,
    capacity: usize,
) bool {
    return (control_entry or ticket != 0) and payload_len <= capacity;
}

/// Whether this frame joins the drain-turn already in `batch_len`.
///
/// `frameIsBatchable` is the empty-scratch rule: unticketed DATA does not
/// *start* a batch, so an SSE body write without a wait stays an immediate
/// handoff. Complete oneshots skip a per-response ticket; their DATA would
/// miss the packed write under that rule. `hold_unticketed` is the drain-turn
/// receipt: those DATA join, including as the first byte of a new scratch
/// after a 16 KiB overflow, so the receipt can ride the last `queueWire`.
///
/// Do not join unticketed DATA merely because the scratch is non-empty.
/// SSE `writeAll` puts the receipt on a trailing empty DATA after an
/// unticketed body; joining that body onto someone else's HEADERS was a
/// shutdown hang (lifecycle 100-stream SSE).
pub fn frameJoinsInProgress(
    control_entry: bool,
    ticket: u64,
    payload_len: usize,
    capacity: usize,
    batch_len: usize,
    hold_unticketed: bool,
) bool {
    _ = batch_len;
    if (payload_len > capacity) return false;
    if (control_entry or ticket != 0) return true;
    return hold_unticketed;
}

pub const EmitBatch = struct {
    buf: []u8,
    len: usize = 0,
    first_ticket: u64 = 0,
    first_ticket_slot: u32 = 0,
    last_ticket_slot: u32 = 0,
    ticket_count: u32 = 0,
    needs_flush: bool = false,
    control_n: usize = 0,
    control_entries: u32 = 0,

    pub fn capacity(self: *const EmitBatch) usize {
        return self.buf.len;
    }

    pub fn empty(self: *const EmitBatch) bool {
        return self.len == 0;
    }

    pub fn wouldFit(self: *const EmitBatch, payload_len: usize) bool {
        return self.len + payload_len <= self.buf.len;
    }

    pub fn noteTicket(self: *EmitBatch, ticket: u64, ticket_slot: u32) void {
        if (self.ticket_count == 0) {
            self.first_ticket = ticket;
            self.first_ticket_slot = ticket_slot;
        }
        self.last_ticket_slot = ticket_slot;
        self.ticket_count += 1;
    }

    /// Copy one frame into the scratch. Several HEADERS are supposed to join;
    /// a flush-on-second-control here is how one-shot TLS paid one record per
    /// response. DATA is not a control and must not increment `control_entries`.
    pub fn copyFrame(
        self: *EmitBatch,
        payload: []const u8,
        flush: bool,
        control_n: usize,
        control_entry: bool,
    ) void {
        std.debug.assert(self.wouldFit(payload.len));
        @memcpy(self.buf[self.len..][0..payload.len], payload);
        self.len += payload.len;
        self.needs_flush = self.needs_flush or flush;
        if (control_entry) {
            self.control_n += control_n;
            self.control_entries += 1;
        }
    }

    pub const Taken = struct {
        bytes: []u8,
        flush: bool,
        ticket: u64,
        ticket_slot: u32,
        ticket_count: u32,
        control_n: usize,
        control_entries: u32,
    };

    pub fn take(self: *EmitBatch) Taken {
        std.debug.assert(!self.empty());
        // DATA-only leftover after a 16 KiB overflow has neither a ticket nor
        // a control entry: the drain-turn receipt rides the last queueWire.
        const out: Taken = .{
            .bytes = self.buf[0..self.len],
            .flush = self.needs_flush,
            .ticket = self.first_ticket,
            .ticket_slot = self.first_ticket_slot,
            .ticket_count = self.ticket_count,
            .control_n = self.control_n,
            .control_entries = self.control_entries,
        };
        self.clear();
        return out;
    }

    pub fn clear(self: *EmitBatch) void {
        self.len = 0;
        self.first_ticket = 0;
        self.first_ticket_slot = 0;
        self.last_ticket_slot = 0;
        self.ticket_count = 0;
        self.needs_flush = false;
        self.control_n = 0;
        self.control_entries = 0;
    }
};

test "unticketed DATA is not batchable" {
    try std.testing.expect(!frameIsBatchable(false, 0, 40, max_plaintext));
    try std.testing.expect(frameIsBatchable(true, 0, 40, max_plaintext));
    try std.testing.expect(frameIsBatchable(true, 1, 40, max_plaintext));
    try std.testing.expect(frameIsBatchable(false, 7, 40, max_plaintext));
    try std.testing.expect(!frameIsBatchable(true, 0, 50, 40));
}

test "unticketed DATA joins a drain-turn already in progress" {
    // Empty scratch, no drain-turn receipt: SSE/body writes without a wait
    // stay immediate. A non-empty scratch without a receipt is the same —
    // joining here hung lifecycle SSE shutdown.
    try std.testing.expect(!frameJoinsInProgress(false, 0, 40, max_plaintext, 0, false));
    try std.testing.expect(!frameJoinsInProgress(false, 0, 40, max_plaintext, 10, false));
    // Drain-turn receipt: oneshot DATA joins, including after overflow.
    try std.testing.expect(frameJoinsInProgress(false, 0, 40, max_plaintext, 0, true));
    try std.testing.expect(frameJoinsInProgress(false, 0, 40, max_plaintext, 10, true));
    try std.testing.expect(frameJoinsInProgress(true, 0, 40, max_plaintext, 0, false));
    try std.testing.expect(frameJoinsInProgress(false, 7, 40, max_plaintext, 0, false));
    try std.testing.expect(!frameJoinsInProgress(false, 0, 50, 40, 10, true));
}

test "unticketed DATA overflow can take before the receipt is attached" {
    var storage: [5]u8 = undefined;
    var batch: EmitBatch = .{ .buf = &storage };
    batch.copyFrame("aa", true, 2, true);
    try std.testing.expect(!batch.wouldFit(4));
    const first = batch.take();
    try std.testing.expectEqualStrings("aa", first.bytes);
    try std.testing.expectEqual(@as(u32, 1), first.control_entries);
    batch.copyFrame("bbb", true, 0, false);
    const mid = batch.take();
    try std.testing.expectEqualStrings("bbb", mid.bytes);
    try std.testing.expectEqual(@as(u32, 0), mid.ticket_count);
    try std.testing.expectEqual(@as(u32, 0), mid.control_entries);
    batch.copyFrame("cc", true, 0, false);
    batch.noteTicket(9, 1);
    const last = batch.take();
    try std.testing.expectEqualStrings("cc", last.bytes);
    try std.testing.expectEqual(@as(u64, 9), last.ticket);
}

test "several HEADERS join one batch and sum control occupancy" {
    // Revert to flush-on-second-control and this is two takes, entries=1 each.
    var storage: [256]u8 = undefined;
    var batch: EmitBatch = .{ .buf = &storage };
    batch.copyFrame("AAA", true, 10, true);
    batch.copyFrame("BB", true, 20, true);
    batch.copyFrame("C", true, 30, true);
    try std.testing.expectEqual(@as(u32, 3), batch.control_entries);
    try std.testing.expectEqual(@as(usize, 60), batch.control_n);
    try std.testing.expectEqualStrings("AAABBC", batch.buf[0..batch.len]);
    const taken = batch.take();
    try std.testing.expectEqual(@as(u32, 3), taken.control_entries);
    try std.testing.expectEqual(@as(usize, 60), taken.control_n);
    try std.testing.expectEqualStrings("AAABBC", taken.bytes);
    try std.testing.expect(batch.empty());
}

test "ticketed DATA joins HEADERS without counting as a control" {
    var storage: [256]u8 = undefined;
    var batch: EmitBatch = .{ .buf = &storage };
    batch.copyFrame("HDR", true, 24, true);
    batch.noteTicket(1, 10);
    batch.copyFrame("BODY", true, 0, false);
    batch.noteTicket(2, 11);
    batch.copyFrame("HDR", true, 24, true);
    batch.copyFrame("BODY", true, 0, false);
    batch.noteTicket(3, 12);
    try std.testing.expectEqual(@as(u32, 2), batch.control_entries);
    try std.testing.expectEqual(@as(usize, 48), batch.control_n);
    try std.testing.expectEqual(@as(u32, 3), batch.ticket_count);
    try std.testing.expectEqual(@as(u32, 10), batch.first_ticket_slot);
    try std.testing.expectEqual(@as(u32, 12), batch.last_ticket_slot);
    try std.testing.expectEqualStrings("HDRBODYHDRBODY", batch.buf[0..batch.len]);
}

test "a frame that does not fit leaves the batch untouched" {
    var storage: [5]u8 = undefined;
    var batch: EmitBatch = .{ .buf = &storage };
    batch.copyFrame("aa", true, 2, true);
    try std.testing.expect(!batch.wouldFit(4));
    try std.testing.expect(batch.wouldFit(3));
    try std.testing.expectEqual(@as(usize, 2), batch.len);
    try std.testing.expectEqual(@as(u32, 1), batch.control_entries);
}

test "overflow is a flush then a new batch, not a concat past capacity" {
    var storage: [5]u8 = undefined;
    var batch: EmitBatch = .{ .buf = &storage };
    batch.copyFrame("aa", true, 2, true);
    try std.testing.expect(!batch.wouldFit(4));
    const first = batch.take();
    try std.testing.expectEqualStrings("aa", first.bytes);
    try std.testing.expectEqual(@as(u32, 1), first.control_entries);
    try std.testing.expect(batch.wouldFit(4));
    batch.copyFrame("bbbb", true, 4, true);
    const second = batch.take();
    try std.testing.expectEqualStrings("bbbb", second.bytes);
    try std.testing.expectEqual(@as(u32, 1), second.control_entries);
}

test "receipts attach only to the last TLS record of one plaintext" {
    const meta: WireMeta = .{
        .ticket = 9,
        .ticket_slot = 3,
        .ticket_count = 6,
        .control_n = 120,
        .control_entries = 6,
    };
    const early = lastRecordMeta(false, meta);
    try std.testing.expectEqual(@as(u64, 0), early.ticket);
    try std.testing.expectEqual(@as(u32, 0), early.ticket_count);
    try std.testing.expectEqual(@as(u32, 0), early.control_entries);
    try std.testing.expectEqual(@as(usize, 0), early.control_n);
    try std.testing.expect(!lastRecordFlush(false, true));
    const last = lastRecordMeta(true, meta);
    try std.testing.expectEqual(meta.ticket, last.ticket);
    try std.testing.expectEqual(meta.ticket_count, last.ticket_count);
    try std.testing.expectEqual(meta.control_entries, last.control_entries);
    try std.testing.expectEqual(meta.control_n, last.control_n);
    try std.testing.expect(lastRecordFlush(true, true));
    try std.testing.expect(!lastRecordFlush(true, false));
}

test "needs_flush stays set after a later non-flush frame" {
    var storage: [64]u8 = undefined;
    var batch: EmitBatch = .{ .buf = &storage };
    batch.copyFrame("aa", false, 2, true);
    try std.testing.expect(!batch.needs_flush);
    batch.copyFrame("bb", true, 2, true);
    batch.copyFrame("cc", false, 2, true);
    try std.testing.expect(batch.needs_flush);
    const taken = batch.take();
    try std.testing.expect(taken.flush);
}

test "ticketed DATA alone is a valid batch with no control occupancy" {
    var storage: [64]u8 = undefined;
    var batch: EmitBatch = .{ .buf = &storage };
    batch.copyFrame("BODY", true, 0, false);
    batch.noteTicket(7, 3);
    try std.testing.expectEqual(@as(u32, 0), batch.control_entries);
    try std.testing.expectEqual(@as(usize, 0), batch.control_n);
    const taken = batch.take();
    try std.testing.expectEqual(@as(u32, 1), taken.ticket_count);
    try std.testing.expectEqual(@as(u64, 7), taken.ticket);
    try std.testing.expectEqual(@as(u32, 0), taken.control_entries);
}

test "frames fill the TLS plaintext cap without overflowing" {
    var storage: [max_plaintext]u8 = undefined;
    var batch: EmitBatch = .{ .buf = &storage };
    const payload = [_]u8{'x'} ** 64;
    while (batch.wouldFit(payload.len)) {
        batch.copyFrame(&payload, true, payload.len, true);
    }
    try std.testing.expect(batch.len + payload.len > max_plaintext);
    try std.testing.expect(batch.len <= max_plaintext);
    try std.testing.expect(batch.len % payload.len == 0);
    try std.testing.expect(batch.control_entries > 1);
    try std.testing.expectEqual(batch.len, @as(usize, batch.control_entries) * payload.len);
}

test "a frame the size of the scratch is batchable; one byte over is not" {
    try std.testing.expect(frameIsBatchable(true, 0, max_plaintext, max_plaintext));
    try std.testing.expect(!frameIsBatchable(true, 0, max_plaintext + 1, max_plaintext));
}
