//! Client accounts: what each client identity holds of every server-wide
//! resource (t-3759 step 3).
//!
//! # The rule
//!
//! A resource is charged from the moment it is taken until it is really
//! released, to every party that holds it. The parties of one charge are a
//! small fixed list taken at admission, in a fixed order: the connection, the
//! client, the client's aggregate (IPv6 only), then the server. The
//! connection's and the server's accounts are the counters that already exist
//! (`ConnBudget`, the connection's outbound budget, the Session's stream count,
//! `GlobalAccounting`); this file holds the other two, as records in one
//! table, and a `Charge` is the list of record references a holder carries.
//!
//! - A charge walks its list in order and the server's counter last; when the
//!   server refuses, the walk is undone in reverse (`Charge.add` then
//!   `Charge.sub`). A client account never refuses in this step: shares under
//!   pressure are a later step, and they go in `Charge.add`, which then
//!   returns whether it took the charge.
//! - Release walks the same list. When a connection ends, its own charge is
//!   released and the rest stay: an orphan handler's request bytes, its
//!   task-handler token and its compression context stay charged to its
//!   client and aggregate until it returns, through the link's own references
//!   (`LinkCore.charge`).
//!
//! # The table
//!
//! - **Stable records.** A fixed slab of records sized at boot, and an
//!   open-addressing index (linear probing, backward-shift deletion, no
//!   tombstones; the hash is keyed with a random seed drawn at boot) that maps
//!   a key to a record index. Only index entries move; records never do, so a
//!   charge's cached index stays valid for as long as it holds its reference.
//!   Each record has a generation, moved when the record leaves the index
//!   (freed, quarantined or retired), in the same atomic step as its last
//!   holder; a `Ref` carries the generation it was taken at, so a reference
//!   that outlived its record never matches again, whether the record is
//!   free, quarantined or holding the next key. A charge or release whose
//!   generation does not match is a bug, refused and counted (`stale`).
//! - **No generation is used twice.** A record whose next generation would be
//!   `retired_generation` (2^32 - 1) is retired instead of freed: out of the
//!   index, never reused, counted (`retired`) and logged. No `Ref` ever
//!   carries that value, so a reference from any earlier generation cannot
//!   match a record again after 2^32 removals of its slot. The cost is one
//!   record of capacity per 2^32 removals of one slot.
//! - **Counts are checked after they move.** `Charge.add` and `sub` check the
//!   generation, change the count, and read the generation again
//!   (`Table.addAt`, `subAt`). A charge whose record left the index in
//!   between (a holder that used a charge after giving its reference back)
//!   has moved some other key's count for that instant; it is taken back and
//!   counted (`stale`), never kept.
//! - **References.** The holder count counts the parties that hold the
//!   record: each connection keyed to it, and each request link dispatched on
//!   such a connection, until it is freed. Whoever holds a record index holds
//!   a reference, so a count of zero means nobody can reach the record. The
//!   only way to get a reference without one is the locked lookup
//!   (`acquire`); a party that already holds one takes another for a new
//!   holder with a lock-free increment (`retain`).
//! - **One word for both.** The generation and the holder count are one
//!   atomic word (`Record.word`), so every change to the count is a
//!   compare-and-swap that also compares the generation. A release through a
//!   reference that outlived its record (a double release racing a reuse)
//!   then fails the compare, on the lock-free path and under the lock alike,
//!   and can never move the count of the record's next key.
//! - **Concurrency.** One `zio.Mutex` guards lookup-or-insert together with the
//!   record's first reference, and the release of the last reference together
//!   with removal; the release re-reads the word under the lock, so a lookup
//!   that took a reference in between keeps the record. Every other charge and
//!   release is a lock-free atomic on the record. The lock is taken at the
//!   accept rate only.
//! - **Removal checks conservation.** When the last reference goes, every
//!   count must be zero. One that is not is a release some path forgot: the
//!   record is logged and quarantined (out of the index, never reused), so it
//!   is still there for `checkEmpty` at `Server.deinit` to report.
//! - **Underflow.** A release that would take a count, or `holders`, below
//!   zero is refused (the count stays), counted in `underflows` and logged,
//!   and nothing is removed on the strength of it.
//!
//! # Capacity
//!
//! `Limits.clientTableRecords`. An insert that finds the slab full fails; the
//! caller refuses that connection and counts it (`insert_failures`). Nothing
//! is ever admitted uncounted.
const std = @import("std");
const zio = @import("zio");
const request = @import("../http/request.zig");

pub const Resource = enum(u8) {
    connections,
    /// TLS handshakes in progress.
    handshakes,
    /// HTTP/2 streams the protocol holds open, and the HTTP/1.1 request in
    /// progress.
    streams,
    /// Task handlers, orphans included.
    task_handlers,
    /// Request-pool bytes, by block class.
    request_bytes,
    /// Outbound DATA bytes reserved and not yet acked.
    outbound_bytes,
    /// Brotli encoder contexts.
    compression,
};

pub const resource_count = @typeInfo(Resource).@"enum".field_names.len;

/// What a record is keyed by. 17 bytes, no padding, hashed as bytes.
pub const Key = extern struct {
    kind: Kind,
    /// The address bytes the kind uses, the rest zero: 4 for `ip4`, the first
    /// 8 for `ip6_64`, the prefix for `ip6_aggregate` (the bits past it
    /// zero), the endpoint index (little endian u16) for `infrastructure`,
    /// none for `unknown`.
    bytes: [16]u8,

    pub const Kind = enum(u8) {
        ip4 = 1,
        /// An IPv6 client: its /64 (privacy addresses rotate inside one /64).
        ip6_64,
        /// The aggregate account of IPv6 clients that share a prefix
        /// (`Limits.client_ipv6_aggregate_prefix_bits`, /48 by default).
        ip6_aggregate,
        /// Every connection whose peer is not known (`Request.Peer.unknown`):
        /// one shared identity, not an exemption.
        unknown,
        /// PROXY v2 `LOCAL` connections (a proxy's health checks) on one
        /// endpoint: one account per listener.
        infrastructure,
    };

    pub fn eql(a: Key, b: Key) bool {
        return a.kind == b.kind and std.mem.eql(u8, &a.bytes, &b.bytes);
    }

    comptime {
        std.debug.assert(@sizeOf(Key) == 17);
        std.debug.assert(std.meta.hasUniqueRepresentation(Key));
    }
};

/// The keys a connection from `peer` is charged to, client first: one for
/// IPv4 (per address) and for an unknown peer, two for IPv6 (the /64, then
/// the aggregate prefix). `peer` is already normalised: an IPv4-mapped IPv6
/// address is `.ip4` (`connection.peerFromHandle`, `proxy_protocol.parse`),
/// so a dual-stack socket keys IPv4 clients by address, not all of them by
/// one /64 of `::ffff:0:0/96`.
pub fn keysFor(peer: request.Peer, aggregate_bits: u8, out: *[max_refs]Key) []const Key {
    std.debug.assert(aggregate_bits >= 1 and aggregate_bits < 64);
    switch (peer) {
        .ip4 => |a| {
            out[0] = .{ .kind = .ip4, .bytes = @splat(0) };
            @memcpy(out[0].bytes[0..4], &a);
            return out[0..1];
        },
        .ip6 => |a| {
            out[0] = .{ .kind = .ip6_64, .bytes = @splat(0) };
            @memcpy(out[0].bytes[0..8], a[0..8]);
            out[1] = .{ .kind = .ip6_aggregate, .bytes = @splat(0) };
            const whole = aggregate_bits / 8;
            @memcpy(out[1].bytes[0..whole], a[0..whole]);
            const rest: u3 = @intCast(aggregate_bits % 8);
            if (rest != 0) out[1].bytes[whole] = a[whole] & ~(@as(u8, 0xff) >> rest);
            return out[0..2];
        },
        .unknown => {
            out[0] = .{ .kind = .unknown, .bytes = @splat(0) };
            return out[0..1];
        },
    }
}

pub fn infrastructureKey(endpoint_index: u16) Key {
    var k: Key = .{ .kind = .infrastructure, .bytes = @splat(0) };
    std.mem.writeInt(u16, k.bytes[0..2], endpoint_index, .little);
    return k;
}

/// Client, then aggregate. A route account is a later step.
pub const max_refs = 2;

/// One record held by one party: its index in the slab, and the generation
/// it had when the reference was taken.
pub const Ref = struct {
    index: u32,
    generation: u32,
};

/// The client-table part of one holder's charge list (the connection's and
/// the server's accounts are walked by the caller). A copy is a plain value:
/// the references it names are held by whoever made the copy's holder
/// (`Table.acquire` for a connection, `Table.retain` for a link), and the
/// copy is valid exactly as long as that holder keeps them.
pub const Charge = struct {
    table: ?*Table = null,
    refs: [max_refs]Ref = undefined,
    len: u8 = 0,

    /// No client account: `.none` endpoints, and the unit tests that build a
    /// connection by hand.
    pub const none: Charge = .{};

    pub fn slice(self: *const Charge) []const Ref {
        return self.refs[0..self.len];
    }

    /// Charge `n` of `res` to every account on the list, in order. Cannot
    /// refuse in this step (see the file header).
    pub fn add(self: Charge, res: Resource, n: usize) void {
        if (self.len == 0 or n == 0) return;
        const t = self.table.?;
        for (self.refs[0..self.len]) |r| {
            const rec = t.record(r) orelse continue;
            t.addAt(rec, r, res, n);
        }
    }

    /// Release `n` of `res` from every account on the list, in reverse.
    /// A release below zero is refused for that account (`Table.underflows`).
    pub fn sub(self: Charge, res: Resource, n: usize) void {
        if (self.len == 0 or n == 0) return;
        const t = self.table.?;
        var i = self.len;
        while (i > 0) {
            i -= 1;
            const rec = t.record(self.refs[i]) orelse continue;
            t.subAt(rec, self.refs[i], res, n);
        }
    }
};

pub const Record = struct {
    counts: [resource_count]std.atomic.Value(usize) = @splat(.init(0)),
    /// The generation in the high 32 bits, the holder count in the low 32
    /// (see the file header). The generation is bumped under the lock when
    /// the record leaves the index, and only then, when the count is zero.
    word: std.atomic.Value(u64) = .init(0),
    /// Under the lock.
    key: Key = undefined,
    state: State = .free,

    pub const State = enum(u8) {
        free,
        /// In the index.
        live,
        /// Its last reference went while a count was not zero: out of the
        /// index and never reused, so `checkEmpty` reports it.
        quarantined,
        /// Its generation reached `retired_generation`: out of the index and
        /// never reused, so no generation is handed out twice.
        retired,
    };

    fn generationOf(w: u64) u32 {
        return @intCast(w >> 32);
    }

    fn holdersOf(w: u64) u32 {
        return @truncate(w);
    }

    pub fn generation(self: *const Record) u32 {
        return generationOf(self.word.load(.acquire));
    }

    pub fn holders(self: *const Record) u32 {
        return holdersOf(self.word.load(.acquire));
    }
};

/// What a test or an operator reads from the table (`Table.snapshot`).
pub const Stats = struct {
    live: usize,
    /// Records, not counts: one record left with several counts not zero is
    /// one.
    quarantined: usize,
    /// Records whose generation would have wrapped (`retired_generation`).
    retired: usize,
    inserts: u64,
    /// Inserts into a record that had held another key before (generation
    /// above zero).
    reuses: u64,
    removals: u64,
    insert_failures: u64,
    underflows: u64,
    stale: u64,
};

/// The one generation no `Ref` carries: a record that reaches it is retired.
pub const retired_generation: u32 = std.math.maxInt(u32);

/// The word after one holder leaves (`Table.dropLocked`). The last holder's
/// leaving moves the generation in the same step, so there is no instant at
/// which a reference to the old generation matches a record with no holder.
fn dropWord(cur: u64) u64 {
    std.debug.assert(Record.holdersOf(cur) != 0);
    if (Record.holdersOf(cur) != 1) return cur - 1;
    const g = Record.generationOf(cur);
    std.debug.assert(g < retired_generation);
    return @as(u64, g + 1) << 32;
}

pub const Table = struct {
    mu: zio.Mutex = .init,
    records: []Record,
    /// Open addressing over record indices: 0 is empty, else index + 1.
    index: []u32,
    mask: usize,
    /// Free record indices, a stack.
    free: []u32,
    free_len: usize,
    seed: u64,
    /// Under the lock.
    live: usize = 0,
    quarantined: usize = 0,
    retired: usize = 0,
    inserts: u64 = 0,
    reuses: u64 = 0,
    removals: u64 = 0,
    /// Atomic, not under the lock.
    insert_failures: std.atomic.Value(u64) = .init(0),
    underflows: std.atomic.Value(u64) = .init(0),
    stale: std.atomic.Value(u64) = .init(0),

    /// `capacity` records, an index of at least twice as many entries, and a
    /// hash keyed by `seed` (the server draws it at boot, so a client cannot
    /// pick keys that collide in a known pattern).
    pub fn init(gpa: std.mem.Allocator, capacity: usize, seed: u64) error{OutOfMemory}!Table {
        std.debug.assert(capacity > 0 and capacity < std.math.maxInt(u32));
        const records = try gpa.alloc(Record, capacity);
        errdefer gpa.free(records);
        for (records) |*r| r.* = .{};
        const index_len = std.math.ceilPowerOfTwo(usize, capacity * 2) catch return error.OutOfMemory;
        const index = try gpa.alloc(u32, index_len);
        errdefer gpa.free(index);
        @memset(index, 0);
        const free = try gpa.alloc(u32, capacity);
        // Pushed so the lowest index is popped first.
        for (free, 0..) |*f, i| f.* = @intCast(capacity - 1 - i);
        return .{
            .records = records,
            .index = index,
            .mask = index_len - 1,
            .free = free,
            .free_len = capacity,
            .seed = seed,
        };
    }

    pub fn deinit(self: *Table, gpa: std.mem.Allocator) void {
        gpa.free(self.records);
        gpa.free(self.index);
        gpa.free(self.free);
        self.* = undefined;
    }

    /// The bytes `init` allocates for `capacity` records, for the resource
    /// bound.
    pub fn bootBytes(capacity: usize) error{Overflow}!usize {
        const index_len = std.math.ceilPowerOfTwo(usize, try std.math.mul(usize, capacity, 2)) catch return error.Overflow;
        var total = try std.math.mul(usize, capacity, @sizeOf(Record));
        total = try std.math.add(usize, total, try std.math.mul(usize, index_len, @sizeOf(u32)));
        return std.math.add(usize, total, try std.math.mul(usize, capacity, @sizeOf(u32)));
    }

    /// Look up or insert every key in `keys` and take one reference on each,
    /// under one hold of the lock. Null when the slab cannot hold a new key:
    /// the references already taken are given back, the failure is counted,
    /// and the caller refuses the connection. Cancelable: the accept loop
    /// calls it, and a cancel there is the server stopping.
    pub fn acquire(self: *Table, keys: []const Key) std.Io.Cancelable!?Charge {
        std.debug.assert(keys.len <= max_refs);
        var c: Charge = .{ .table = self };
        if (keys.len == 0) return .none;
        try self.mu.lock();
        defer self.mu.unlock();
        for (keys) |k| {
            const i = self.find(k) orelse self.insert(k) orelse {
                // Undo in reverse, under the same hold of the lock.
                while (c.len > 0) {
                    c.len -= 1;
                    self.dropLocked(c.refs[c.len]);
                }
                _ = self.insert_failures.fetchAdd(1, .monotonic);
                log.warn("client table full ({d} records): connection refused", .{self.records.len});
                return null;
            };
            const rec = &self.records[i];
            // Under the lock the generation cannot change (it changes only
            // at removal, under the lock), so a plain add is a reference on
            // this generation.
            const w = rec.word.fetchAdd(1, .acq_rel);
            std.debug.assert(Record.holdersOf(w) < std.math.maxInt(u32));
            std.debug.assert(Record.generationOf(w) != retired_generation);
            c.refs[c.len] = .{ .index = i, .generation = Record.generationOf(w) };
            c.len += 1;
        }
        return c;
    }

    /// One more reference on every record of `c`, for a new holder (a request
    /// link), taken by a party that already holds `c`'s references, so no
    /// record can be removed meanwhile. Lock-free. A record found with no
    /// holder is a bug: refused, counted (`underflows`), logged.
    pub fn retain(self: *Table, c: Charge) void {
        for (c.slice()) |r| {
            const rec = &self.records[r.index];
            var cur = rec.word.load(.monotonic);
            while (true) {
                if (Record.generationOf(cur) != r.generation) {
                    self.noteStale(r, Record.generationOf(cur));
                    break;
                }
                if (Record.holdersOf(cur) == 0) {
                    self.noteUnderflow("holders (retain of an unheld record)", 0, 1);
                    break;
                }
                std.debug.assert(Record.holdersOf(cur) < std.math.maxInt(u32));
                cur = rec.word.cmpxchgWeak(cur, cur + 1, .monotonic, .monotonic) orelse break;
            }
        }
    }

    /// Give back one reference on every record of `c`, in reverse. A
    /// reference that is not the last is a lock-free decrement; the last
    /// takes the lock (`releaseLast`). Never waits for anything but the lock,
    /// and ignores cancel: a release must happen.
    pub fn release(self: *Table, c: Charge) void {
        var i = c.len;
        while (i > 0) {
            i -= 1;
            const r = c.refs[i];
            const rec = &self.records[r.index];
            var cur = rec.word.load(.monotonic);
            while (true) {
                if (Record.generationOf(cur) != r.generation) {
                    self.noteStale(r, Record.generationOf(cur));
                    break;
                }
                if (Record.holdersOf(cur) <= 1) {
                    self.releaseLast(r);
                    break;
                }
                cur = rec.word.cmpxchgWeak(cur, cur - 1, .acq_rel, .monotonic) orelse break;
            }
        }
    }

    /// The locked step of `release`, for a reference that read one holder (or
    /// none): give it back under the lock, re-reading the word, and remove
    /// the record if it was the last. Two releases of one reference can both
    /// get here, and an `acquire` can take the lock between them and reuse
    /// the record; the second then finds a new generation and is refused
    /// (`stale`). Public so a test can order those steps.
    pub fn releaseLast(self: *Table, r: Ref) void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        self.dropLocked(r);
    }

    /// Under the lock: give back one reference to `r`, and remove the record
    /// if it was the last.
    fn dropLocked(self: *Table, r: Ref) void {
        const rec = &self.records[r.index];
        // A read-modify-write even under the lock: a holder that is not the
        // last decrements without it (`release`).
        var cur = rec.word.load(.acquire);
        while (true) {
            if (Record.generationOf(cur) != r.generation) {
                self.noteStale(r, Record.generationOf(cur));
                return;
            }
            if (Record.holdersOf(cur) == 0) {
                self.noteUnderflow("holders", 0, 1);
                return;
            }
            // The last holder's leaving moves the generation in this same
            // step (`dropWord`): leaving the index, no reference to this
            // generation may match again, and only the lock adds a holder
            // from zero.
            cur = rec.word.cmpxchgWeak(cur, dropWord(cur), .seq_cst, .acquire) orelse break;
        }
        if (Record.holdersOf(cur) != 1) return;
        // The generation matched and a holder was there, so the record is in
        // the index: every exit from it moves the generation.
        std.debug.assert(rec.state == .live);
        self.removeIndex(r.index);
        self.live -= 1;
        self.removals += 1;
        // `seq_cst` with the word above and with `addAt`'s add and re-read:
        // a stale charge either lands before this read (and quarantines the
        // record, loudly) or reads the moved generation and takes itself
        // back.
        var left: usize = 0;
        for (&rec.counts) |*count| {
            if (count.load(.seq_cst) != 0) left += 1;
        }
        if (left != 0) {
            // A release some path forgot: keep the evidence. One record, one
            // quarantine, whatever the number of counts left.
            rec.state = .quarantined;
            self.quarantined += 1;
            var buf: [resource_count * 40]u8 = undefined;
            var w: std.Io.Writer = .fixed(&buf);
            var first = true;
            for (&rec.counts, 0..) |*count, ri| {
                const v = count.load(.acquire);
                if (v == 0) continue;
                w.print("{s}{s} = {d}", .{ if (first) "" else ", ", @tagName(@as(Resource, @enumFromInt(ri))), v }) catch {};
                first = false;
            }
            log.err("client account conservation: the last reference to {f} went with {s}; record quarantined", .{ fmtKey(rec.key), w.buffered() });
            return;
        }
        if (Record.generationOf(dropWord(cur)) == retired_generation) {
            rec.state = .retired;
            self.retired += 1;
            log.warn("client table: record {d} retired: its generation would wrap ({d} retired, {d} of {d} records left to use)", .{ r.index, self.retired, self.records.len - self.retired - self.quarantined, self.records.len });
            return;
        }
        rec.state = .free;
        self.free[self.free_len] = r.index;
        self.free_len += 1;
    }

    /// The record `r` names, or null when its generation is not the record's:
    /// a reference that outlived its record, which is a bug. Counted
    /// (`stale`) and logged, and the caller skips the record.
    /// The step of `Charge.add` after the generation check: add, then read
    /// the generation again. If the record left the index in between, the
    /// add landed on whatever the record holds now: it is taken back and
    /// counted (`stale`). Public so a test can order an acquire between the
    /// check and this step.
    pub fn addAt(self: *Table, rec: *Record, r: Ref, res: Resource, n: usize) void {
        const c = &rec.counts[@intFromEnum(res)];
        _ = c.fetchAdd(n, .seq_cst);
        const g = Record.generationOf(rec.word.load(.seq_cst));
        if (g == r.generation) return;
        _ = c.fetchSub(n, .seq_cst);
        self.noteStale(r, g);
    }

    /// `Charge.sub`'s step after the check: subtract unless it would go below
    /// zero, then read the generation again. A stale subtraction is put back
    /// and counted (`stale`); an underflow is counted only when the record
    /// is still this reference's, so it is never charged to another key.
    pub fn subAt(self: *Table, rec: *Record, r: Ref, res: Resource, n: usize) void {
        const c = &rec.counts[@intFromEnum(res)];
        var cur = c.load(.seq_cst);
        const took = while (true) {
            if (cur < n) break false;
            cur = c.cmpxchgWeak(cur, cur - n, .seq_cst, .seq_cst) orelse break true;
        };
        const g = Record.generationOf(rec.word.load(.seq_cst));
        if (g != r.generation) {
            if (took) _ = c.fetchAdd(n, .seq_cst);
            self.noteStale(r, g);
            return;
        }
        if (!took) self.noteUnderflow(@tagName(res), cur, n);
    }

    /// Test only: give the free record `index` generation `g`, to test the
    /// boundary at 2^32 removals without making them.
    pub fn forceGenerationForTest(self: *Table, index: u32, g: u32) void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        const rec = &self.records[index];
        std.debug.assert(rec.state == .free and rec.holders() == 0);
        std.debug.assert(g < retired_generation);
        rec.word.store(@as(u64, g) << 32, .release);
    }

    pub fn record(self: *Table, r: Ref) ?*Record {
        const rec = &self.records[r.index];
        const g = rec.generation();
        if (g == r.generation) return rec;
        self.noteStale(r, g);
        return null;
    }

    fn noteStale(self: *Table, r: Ref, found: u32) void {
        const total = self.stale.fetchAdd(1, .monotonic) + 1;
        log.err("client account: a reference to record {d} generation {d} found generation {d}; skipped ({d} so far)", .{ r.index, r.generation, found, total });
    }

    fn noteUnderflow(self: *Table, name: []const u8, held: usize, n: usize) void {
        const total = self.underflows.fetchAdd(1, .monotonic) + 1;
        log.err("client account underflow: a release of {d} from {s} found {d}; refused, count kept ({d} refused so far)", .{ n, name, held, total });
    }

    fn home(self: *const Table, k: Key) usize {
        return @as(usize, @truncate(std.hash.Wyhash.hash(self.seed, std.mem.asBytes(&k)))) & self.mask;
    }

    fn find(self: *const Table, k: Key) ?u32 {
        var p = self.home(k);
        while (true) : (p = (p + 1) & self.mask) {
            const e = self.index[p];
            if (e == 0) return null;
            if (self.records[e - 1].key.eql(k)) return e - 1;
        }
    }

    /// A free record for `k`, in the index; null when none is free.
    fn insert(self: *Table, k: Key) ?u32 {
        if (self.free_len == 0) return null;
        self.free_len -= 1;
        const i = self.free[self.free_len];
        const rec = &self.records[i];
        std.debug.assert(rec.state == .free);
        std.debug.assert(rec.holders() == 0);
        for (&rec.counts) |*count| std.debug.assert(count.load(.monotonic) == 0);
        const g = rec.generation();
        std.debug.assert(g != retired_generation);
        rec.key = k;
        rec.state = .live;
        var p = self.home(k);
        while (self.index[p] != 0) p = (p + 1) & self.mask;
        self.index[p] = i + 1;
        self.live += 1;
        self.inserts += 1;
        if (g > 0) self.reuses += 1;
        return i;
    }

    /// Take record `i` out of the index with a backward shift: every entry
    /// after the hole that may sit there (its home is not between the hole
    /// and it) moves back, so a lookup never meets a gap before its key.
    fn removeIndex(self: *Table, i: u32) void {
        var hole = self.home(self.records[i].key);
        while (self.index[hole] != i + 1) {
            std.debug.assert(self.index[hole] != 0);
            hole = (hole + 1) & self.mask;
        }
        var j = hole;
        while (true) {
            j = (j + 1) & self.mask;
            const e = self.index[j];
            if (e == 0) break;
            const h = self.home(self.records[e - 1].key);
            // `e` may move to `hole` unless its home lies in (hole, j].
            const stays = if (hole <= j) (h > hole and h <= j) else (h > hole or h <= j);
            if (stays) continue;
            self.index[hole] = e;
            hole = j;
        }
        self.index[hole] = 0;
    }

    /// Every record free and every count zero, or log what is not. True when
    /// clean. `Server.deinit` calls it, after `serve` returned, so in a test
    /// suite every test that ends a server checks conservation.
    pub fn checkEmpty(self: *Table) bool {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        var clean = true;
        for (self.records, 0..) |*rec, i| {
            // A retired record holds nothing: it is only never reused.
            if (rec.state == .free or rec.state == .retired) continue;
            clean = false;
            log.err("client account still {s} at deinit: record {d} {f}, {d} holders", .{ @tagName(rec.state), i, fmtKey(rec.key), rec.holders() });
            for (&rec.counts, 0..) |*count, ri| {
                const v = count.load(.acquire);
                if (v != 0) log.err("  {s} = {d}", .{ @tagName(@as(Resource, @enumFromInt(ri))), v });
            }
        }
        for (self.index) |e| {
            if (e != 0) clean = false;
        }
        if (!clean) log.err("client table not empty at deinit: {d} live, {d} quarantined", .{ self.live, self.quarantined });
        if (self.underflows.load(.acquire) != 0) {
            clean = false;
            log.err("client table: {d} refused releases over the server's life", .{self.underflows.load(.acquire)});
        }
        if (self.stale.load(.acquire) != 0) {
            clean = false;
            log.err("client table: {d} stale references over the server's life", .{self.stale.load(.acquire)});
        }
        return clean;
    }

    pub fn stats(self: *Table) Stats {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        return .{
            .live = self.live,
            .quarantined = self.quarantined,
            .retired = self.retired,
            .inserts = self.inserts,
            .reuses = self.reuses,
            .removals = self.removals,
            .insert_failures = self.insert_failures.load(.acquire),
            .underflows = self.underflows.load(.acquire),
            .stale = self.stale.load(.acquire),
        };
    }

    /// One live record as `snapshot` reports it.
    pub const Entry = struct {
        key: Key,
        index: u32,
        generation: u32,
        holders: u32,
        counts: [resource_count]usize,

        pub fn count(self: Entry, res: Resource) usize {
            return self.counts[@intFromEnum(res)];
        }
    };

    /// The live and quarantined records, at most `out.len`, by scanning the
    /// slab under the lock. Counts are read one by one, so concurrent charges
    /// may be half seen.
    pub fn snapshot(self: *Table, out: []Entry) []Entry {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        var n: usize = 0;
        for (self.records, 0..) |*rec, i| {
            if (rec.state == .free or rec.state == .retired) continue;
            if (n == out.len) break;
            out[n] = .{
                .key = rec.key,
                .index = @intCast(i),
                .generation = rec.generation(),
                .holders = rec.holders(),
                .counts = undefined,
            };
            for (&rec.counts, &out[n].counts) |*c, *o| o.* = c.load(.acquire);
            n += 1;
        }
        return out[0..n];
    }

    /// The live record for `k`, if any (`snapshot` filtered by key).
    pub fn lookup(self: *Table, k: Key) ?Entry {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        const i = self.find(k) orelse return null;
        const rec = &self.records[i];
        var e: Entry = .{
            .key = rec.key,
            .index = i,
            .generation = rec.generation(),
            .holders = rec.holders(),
            .counts = undefined,
        };
        for (&rec.counts, &e.counts) |*c, *o| o.* = c.load(.acquire);
        return e;
    }
};

const log = std.log.scoped(.starh2);

pub fn fmtKey(k: Key) std.fmt.Alt(Key, formatKey) {
    return .{ .data = k };
}

fn formatKey(k: Key, w: *std.Io.Writer) std.Io.Writer.Error!void {
    switch (k.kind) {
        .ip4 => try w.print("{d}.{d}.{d}.{d}", .{ k.bytes[0], k.bytes[1], k.bytes[2], k.bytes[3] }),
        .ip6_64, .ip6_aggregate => {
            const groups: usize = if (k.kind == .ip6_64) 4 else 8;
            var last_nonzero: usize = 0;
            for (0..groups) |g| {
                if (k.bytes[2 * g] != 0 or k.bytes[2 * g + 1] != 0) last_nonzero = g;
            }
            for (0..last_nonzero + 1) |g| {
                if (g != 0) try w.writeByte(':');
                try w.print("{x}", .{std.mem.readInt(u16, k.bytes[2 * g ..][0..2], .big)});
            }
            try w.writeAll(if (k.kind == .ip6_64) "::/64" else "::/agg");
        },
        .unknown => try w.writeAll("(unknown peer)"),
        .infrastructure => try w.print("(PROXY LOCAL, endpoint {d})", .{std.mem.readInt(u16, k.bytes[0..2], .little)}),
    }
}

test "keysFor: IPv4 per address, IPv6 by /64 plus the aggregate prefix" {
    var out: [max_refs]Key = undefined;
    const k4 = keysFor(.{ .ip4 = .{ 10, 0, 0, 5 } }, 48, &out);
    try std.testing.expectEqual(@as(usize, 1), k4.len);
    try std.testing.expectEqual(Key.Kind.ip4, k4[0].kind);
    try std.testing.expectEqualSlices(u8, &.{ 10, 0, 0, 5 }, k4[0].bytes[0..4]);

    const a: [16]u8 = .{ 0x20, 0x01, 0x0d, 0xb8, 0x12, 0x34, 0x56, 0x78, 1, 2, 3, 4, 5, 6, 7, 8 };
    var b = a;
    b[15] = 99; // same /64
    var c = a;
    c[7] = 0x79; // another /64, same /48
    var d = a;
    d[5] = 0x35; // another /48
    var ka: [max_refs]Key = undefined;
    var kb: [max_refs]Key = undefined;
    var kc: [max_refs]Key = undefined;
    var kd: [max_refs]Key = undefined;
    const sa = keysFor(.{ .ip6 = a }, 48, &ka);
    const sb = keysFor(.{ .ip6 = b }, 48, &kb);
    const sc = keysFor(.{ .ip6 = c }, 48, &kc);
    const sd = keysFor(.{ .ip6 = d }, 48, &kd);
    try std.testing.expect(sa[0].eql(sb[0]));
    try std.testing.expect(!sa[0].eql(sc[0]));
    try std.testing.expect(sa[1].eql(sc[1]));
    try std.testing.expect(!sa[1].eql(sd[1]));
    // A prefix that is not a whole byte keeps only its own bits.
    var ke: [max_refs]Key = undefined;
    const se = keysFor(.{ .ip6 = a }, 44, &ke);
    try std.testing.expectEqual(@as(u8, 0x30), se[1].bytes[5]);
    try std.testing.expectEqual(@as(u8, 0), se[1].bytes[6]);
}

test "Table: one record per key, removed with its last reference, reused with a new generation" {
    const gpa = std.testing.allocator;
    var t = try Table.init(gpa, 4, 0x1234);
    defer t.deinit(gpa);
    var out: [max_refs]Key = undefined;
    const k = keysFor(.{ .ip4 = .{ 1, 2, 3, 4 } }, 48, &out);
    const c1 = (try t.acquire(k)).?;
    const c2 = (try t.acquire(k)).?;
    try std.testing.expectEqual(c1.refs[0].index, c2.refs[0].index);
    try std.testing.expectEqual(@as(u32, 2), t.lookup(k[0]).?.holders);
    t.retain(c1);
    try std.testing.expectEqual(@as(u32, 3), t.lookup(k[0]).?.holders);
    c1.add(.streams, 3);
    c2.sub(.streams, 3);
    t.release(c1);
    t.release(c2);
    try std.testing.expect(t.lookup(k[0]) != null);
    t.release(c1);
    try std.testing.expect(t.lookup(k[0]) == null);
    const g1 = c1.refs[0].generation;
    // The same record comes back for the next key, one generation on.
    var out2: [max_refs]Key = undefined;
    const k2 = keysFor(.{ .ip4 = .{ 5, 6, 7, 8 } }, 48, &out2);
    const c3 = (try t.acquire(k2)).?;
    try std.testing.expectEqual(c1.refs[0].index, c3.refs[0].index);
    try std.testing.expectEqual(g1 + 1, c3.refs[0].generation);
    t.release(c3);
    const s = t.stats();
    try std.testing.expectEqual(@as(u64, 2), s.inserts);
    try std.testing.expectEqual(@as(u64, 1), s.reuses);
    try std.testing.expectEqual(@as(u64, 2), s.removals);
    try std.testing.expect(t.checkEmpty());
}

test "Table: a full slab refuses the insert and gives back what it took" {
    const gpa = std.testing.allocator;
    var t = try Table.init(gpa, 2, 7);
    defer t.deinit(gpa);
    var o1: [max_refs]Key = undefined;
    const a: [16]u8 = .{ 0x20, 0x01, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1 };
    // Two records: the /64 and the /48.
    const c1 = (try t.acquire(keysFor(.{ .ip6 = a }, 48, &o1))).?;
    var o2: [max_refs]Key = undefined;
    var b = a;
    b[7] = 2; // same /48, a new /64: the slab is full
    try std.testing.expect((try t.acquire(keysFor(.{ .ip6 = b }, 48, &o2))) == null);
    try std.testing.expectEqual(@as(u32, 1), t.lookup(o1[1]).?.holders);
    try std.testing.expectEqual(@as(u64, 1), t.stats().insert_failures);
    t.release(c1);
    try std.testing.expect(t.checkEmpty());
}

test "Table: backward-shift deletion keeps every remaining key reachable" {
    const gpa = std.testing.allocator;
    // A small index and many keys, so probe chains wrap and overlap.
    var t = try Table.init(gpa, 24, 99);
    defer t.deinit(gpa);
    var charges: [24]Charge = undefined;
    var keys: [24]Key = undefined;
    for (&charges, &keys, 0..) |*c, *k, i| {
        var o: [max_refs]Key = undefined;
        k.* = keysFor(.{ .ip4 = .{ 10, 0, @intCast(i / 256), @intCast(i % 256) } }, 48, &o)[0];
        c.* = (try t.acquire(&.{k.*})).?;
    }
    // Remove every third, then check every other is still found.
    var gone: [24]bool = @splat(false);
    var i: usize = 0;
    while (i < 24) : (i += 3) {
        t.release(charges[i]);
        gone[i] = true;
    }
    for (keys, gone) |k, g| {
        try std.testing.expectEqual(!g, t.lookup(k) != null);
    }
    for (charges, gone) |c, g| {
        if (!g) t.release(c);
    }
    try std.testing.expect(t.checkEmpty());
}

test "Table: the last holder's release moves the generation in the same step" {
    const w = struct {
        fn of(g: u32, h: u32) u64 {
            return (@as(u64, g) << 32) | h;
        }
    }.of;
    // A holder that is not the last: one fewer, the same generation.
    try std.testing.expectEqual(w(5, 2), dropWord(w(5, 3)));
    // The last: no holder, and the next generation in the same word, so no
    // reference to generation 5 can match it even for an instant.
    try std.testing.expectEqual(w(6, 0), dropWord(w(5, 1)));
}
