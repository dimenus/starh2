//! Client accounts: what each client identity holds of every server-wide
//! resource (t-3759 step 3).
//!
//! # The rule
//!
//! A resource is charged from the moment it is taken until it is really
//! released, to every party that holds it. The parties of one charge are a
//! small fixed list taken at admission, in a fixed order: the connection, the
//! client, the client's aggregate (IPv6 only), the task route (a request
//! link's charge only, `RouteTable`), then the server. The
//! connection's and the server's accounts are the counters that already exist
//! (`ConnBudget`, the connection's outbound budget, the Session's stream count,
//! `GlobalAccounting`); this file holds the other two, as records in one
//! table, and a `Charge` is the list of record references a holder carries.
//!
//! - A charge walks its list in order and the server's counter last; when the
//!   server refuses, the walk is undone in reverse (`Charge.add` then
//!   `Charge.sub`). A client account refuses too (shares, below): then
//!   `Charge.add` undoes its own earlier entries and returns false, and the
//!   caller undoes the connection's.
//!
//! # Shares under pressure
//!
//! For every resource the server has a limit L (`Shares.init`). Its pressure
//! mark is L/4 of the server's use, read from the server's own counter, which
//! the caller passes to `Charge.add` (`server_use`, read before the server's
//! own charge). A client's share is L/8, an IPv6 aggregate's L/4; both round
//! up, so no share is zero. Below the mark nothing is refused by share. At or
//! above it, a charge that would take a client (or an aggregate) past its
//! share is refused: the count is a bounded compare-and-swap, so the share
//! itself is exact; only the mark is check-then-reserve (concurrent
//! admissions can pass it by the number in flight, a byte charge by its own
//! size).
//!
//! So one identity alone holds at most its cap, L/4 (it reaches the mark
//! itself, and is then over its share), and a second identity is still
//! admitted up to a full share. What this does not do, said plainly: it does
//! not take back what an identity holds, and once one identity has pushed
//! the server to the mark every other identity is held to its share for new
//! work, even with 3/4 of the server free (a crowd behind one NAT is refused
//! at that point). A refusal is counted per resource
//! (`Table.share_refusals`) and logged at most once a second, naming the
//! identity, its part of the server's use and the endpoint's mode
//! (`Table.noteRefused`).
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
//! # Start buckets and rate-only records
//!
//! Two token buckets per record, both kept as a theoretical arrival time
//! (GCRA): connection starts, on every endpoint, and TLS handshake starts, on
//! TLS endpoints (`Rules.connection_start`, `handshake_start`: burst 128,
//! 16 a second by default). The connection-start bucket covers h2c and
//! HTTP/1.1 too, because the per-connection frame buckets renew with each new
//! connection. A start always takes a token when there is one; an empty
//! bucket refuses only while the resource's server use is at its pressure
//! mark (connections, or handshakes in progress), as a share does. The client
//! and, for IPv6, the aggregate each have both buckets, so one /48 starts
//! connections no faster than one address. Buckets change only under the
//! lock, in `acquire`, where every start takes its references.
//!
//! A bucket must outlive its client's charges: if the record went with its
//! last holder, a client that reconnects serially would get its burst back
//! every time. So a record whose last holder leaves while a bucket is not yet
//! full stays in the index as a rate-only record: no holder, no charge, its
//! generation moved as for any removal (no old reference matches it), its
//! buckets kept. A start that finds it takes it back. A rate-only record is
//! evictable: when the slab is full, the one whose buckets are full soonest
//! (the oldest refill time) is taken for the new key (`evictions`).
//!
//! # Capacity
//!
//! `Limits.clientTableRecords`, which includes a reserve for rate-only
//! records. An insert that finds the slab full with no rate-only record to
//! evict fails; the caller refuses that connection and counts it
//! (`insert_failures`). Nothing is ever admitted uncounted.
//!
//! # Route accounts
//!
//! A task route is an account too, between the aggregate and the server in
//! the charge order (`RouteTable`, below). It holds three resources: the
//! route's task handlers, orphans included, the request-pool bytes they
//! hold, and their compression contexts. Routes are known at boot, so their
//! records are a fixed array sized from the task routes: no index, no lock,
//! no generation, no holder count. A link's `Charge` names its route
//! (`Charge.route`), so `add` and `sub` walk it with the client records.
const std = @import("std");
const zio = @import("zio");
const request = @import("../http/request.zig");
const router = @import("../http/router.zig");

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

/// Client, then aggregate. The route account is not a table record: a
/// charge names it apart (`Charge.route`).
pub const max_refs = 2;

/// One resource's share rule (see the file header).
pub const Share = struct {
    /// The most one client may hold of the resource while the server's use
    /// is at or above `mark`: L/8, rounded up.
    client: usize,
    /// The same for an IPv6 aggregate: L/4, rounded up, so one /48 caps at
    /// what one address caps at.
    aggregate: usize,
    /// The server's use at which shares apply: L/4, rounded up.
    mark: usize,

    pub fn of(server_limit: usize) Share {
        return .{
            .client = std.math.divCeil(usize, server_limit, 8) catch unreachable,
            .aggregate = std.math.divCeil(usize, server_limit, 4) catch unreachable,
            .mark = std.math.divCeil(usize, server_limit, 4) catch unreachable,
        };
    }
};

/// The share of every resource, from the server's limits (`Limits.shares`).
pub const Shares = [resource_count]Share;

/// The one definition of pressure on a resource: the server's use of it,
/// counted BEFORE the charge or the connection in question, is at or above
/// the resource's `mark`. The client shares (`Charge.add`), the route shares
/// (`RouteRef.add`) and the HTTP/2 idle limit (`Connection.idleLimitNs`,
/// which passes its own connection's count minus itself) all ask here, so
/// they turn on at the same connection.
pub fn underPressure(use_before: usize, mark: usize) bool {
    return use_before >= mark;
}

/// Shares whose mark no use reaches: nothing is refused by share. For the
/// unit tests and the child-process cases that build a table by hand.
pub const unlimited: Shares = @splat(Share.of(std.math.maxInt(usize)));

/// A start bucket: `burst` starts at once, then one every `interval_ns`.
pub const Bucket = struct {
    burst: u64,
    interval_ns: u64,

    /// The arrival time a start at `now` leaves, when the bucket has a token
    /// for it (GCRA: the bucket is full again at `tat`, and a start
    /// conforms while `tat` stays within `burst` intervals of now).
    fn take(self: Bucket, tat: u64, now: u64) ?u64 {
        const next = @max(tat, now) + self.interval_ns;
        if (next - now > self.burst * self.interval_ns) return null;
        return next;
    }
};

pub const BucketKind = enum(u1) { connection_start, handshake_start };

/// Everything the table refuses by: the shares, and the two start buckets
/// (`Limits.accountRules`).
pub const Rules = struct {
    shares: Shares,
    connection_start: Bucket,
    handshake_start: Bucket,

    /// Nothing refused: for the unit tests and the child-process cases.
    pub const unlimited: Rules = .{
        .shares = accounts_unlimited,
        .connection_start = .{ .burst = std.math.maxInt(u32), .interval_ns = 1 },
        .handshake_start = .{ .burst = std.math.maxInt(u32), .interval_ns = 1 },
    };

    fn bucket(self: *const Rules, kind: BucketKind) Bucket {
        return switch (kind) {
            .connection_start => self.connection_start,
            .handshake_start => self.handshake_start,
        };
    }
};

const accounts_unlimited = unlimited;

/// A connection start, for `Table.acquire`: when, and which buckets refuse
/// now. A key acquired without one (a test) touches no bucket.
pub const Start = struct {
    now_ns: u64,
    /// The server's connections, before this one: the pressure reading for
    /// the connection-start bucket, and the refusal line's.
    connections: usize,
    /// TLS endpoints only: the server's handshakes in progress, before this
    /// one. Null where no handshake starts.
    handshakes: ?usize,
};

pub const Acquired = union(enum) {
    charge: Charge,
    /// The slab holds no free record and no rate-only one (counted in
    /// `insert_failures`).
    table_full,
    /// A start bucket was empty under pressure (counted in `rate_refusals`,
    /// logged).
    rate_limited,
};

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
    /// The client first, then (IPv6) its aggregate: `keysFor`'s order, which
    /// is how `add` tells which share applies.
    refs: [max_refs]Ref = undefined,
    len: u8 = 0,
    /// The endpoint the holder came in on, for the refusal log line.
    endpoint: u16 = 0,
    /// The task route a request link was dispatched to, after the client
    /// records in the charge order. Only a link's charge names one
    /// (`withRoute`, at dispatch): a connection's charge has no route.
    route: ?RouteRef = null,

    /// No client account: `.none` endpoints, and the unit tests that build a
    /// connection by hand.
    pub const none: Charge = .{};

    pub fn slice(self: *const Charge) []const Ref {
        return self.refs[0..self.len];
    }

    /// This charge with its route: a request link's, at dispatch.
    pub fn withRoute(self: Charge, route: ?RouteRef) Charge {
        std.debug.assert(self.route == null);
        var c = self;
        c.route = route;
        return c;
    }

    /// Who refused a charge (`add`). The caller answers by it: a route
    /// refusal is a 503 on both protocols, never REFUSED_STREAM, since a
    /// client may retry that by itself and the route is still broken.
    pub const Outcome = enum { took, client_refused, route_refused };

    /// Charge `n` of `res` to every account on the list, in order (the
    /// client, the aggregate, the route), each a bounded compare-and-swap.
    /// `server_use` is the server's own count of `res` now, before its own
    /// charge: at or above the resource's mark, an account that would pass
    /// its share refuses. Then the entries this call already charged are
    /// undone in reverse, the refusal is counted and logged, and the result
    /// says who refused: the caller undoes the connection's account and
    /// refuses. `.took` when every account took it.
    pub fn add(self: Charge, res: Resource, n: usize, server_use: usize) Outcome {
        if (n == 0) return .took;
        // Which entries this call charged, so the undo skips one a stale
        // reference made it skip.
        var took: [max_refs]bool = @splat(false);
        if (self.len != 0) {
            const t = self.table.?;
            const share = t.shares[@intFromEnum(res)];
            const pressure = underPressure(server_use, share.mark);
            for (self.refs[0..self.len], 0..) |r, i| {
                const rec = t.record(r) orelse continue;
                const bound: ?usize = if (!pressure) null else if (i == 0) share.client else share.aggregate;
                switch (t.addAt(rec, r, res, n, bound)) {
                    .took => took[i] = true,
                    .stale => {},
                    .refused => |held| {
                        self.undoClients(&took, i, res, n);
                        t.noteRefused(res, rec, held, server_use, self.endpoint);
                        return .client_refused;
                    },
                }
            }
        }
        if (self.route) |rt| {
            if (!rt.add(res, n, server_use)) {
                self.undoClients(&took, self.len, res, n);
                return .route_refused;
            }
        }
        return .took;
    }

    /// The undo of `add`: the client entries before `upto` that it charged,
    /// in reverse.
    fn undoClients(self: Charge, took: *const [max_refs]bool, upto: usize, res: Resource, n: usize) void {
        const t = self.table orelse return;
        var j = upto;
        while (j > 0) {
            j -= 1;
            if (!took[j]) continue;
            const back = t.record(self.refs[j]) orelse continue;
            t.subAt(back, self.refs[j], res, n);
        }
    }

    /// Release `n` of `res` from every account on the list, in reverse: the
    /// route, then the aggregate and the client. A release below zero is
    /// refused for that account (`Table.underflows`, `RouteTable.underflows`).
    pub fn sub(self: Charge, res: Resource, n: usize) void {
        if (n == 0) return;
        if (self.route) |rt| rt.sub(res, n);
        if (self.len == 0) return;
        const t = self.table.?;
        var i = self.len;
        while (i > 0) {
            i -= 1;
            const rec = t.record(self.refs[i]) orelse continue;
            t.subAt(rec, self.refs[i], res, n);
        }
    }
};

/// The resources a route account holds, a subset of `Resource`.
pub const RouteResource = enum(u2) {
    task_handlers,
    request_bytes,
    compression,

    pub const count = 3;

    /// The route resource `res` is, or null: a route holds nothing else
    /// (connections, handshakes, streams and outbound bytes are a
    /// connection's).
    pub fn of(res: Resource) ?RouteResource {
        return switch (res) {
            .task_handlers => .task_handlers,
            .request_bytes => .request_bytes,
            .compression => .compression,
            .connections, .handshakes, .streams, .outbound_bytes => null,
        };
    }

    pub fn resource(self: RouteResource) Resource {
        return switch (self) {
            .task_handlers => .task_handlers,
            .request_bytes => .request_bytes,
            .compression => .compression,
        };
    }
};

/// The server's limit of each route resource, by `RouteResource`
/// (`Limits.routeLimits`).
pub const RouteLimits = [RouteResource.count]usize;

/// One task route's account. Its counts are charged at dispatch and held
/// until the handler returns, orphan included.
pub const RouteRecord = struct {
    counts: [RouteResource.count]std.atomic.Value(usize) = @splat(.init(0)),
    /// The most the route may hold of each resource while the server's use of
    /// it is at or above its mark. Read only after `init`.
    shares: [RouteResource.count]usize = @splat(0),
    /// Charges refused by this route's share, by resource.
    refusals: [RouteResource.count]std.atomic.Value(u64) = @splat(.init(0)),
    /// "GET /path" (a prefix route ends with `*`), for the refusal line.
    label: Table.Label = .{},
    /// This route's handlers still running after their connection ended:
    /// up from `GlobalAccounting.addOrphan`, down when the orphan's link is
    /// freed (`dropLink`). Its task handlers count includes them.
    orphans: std.atomic.Value(usize) = .init(0),

    pub fn count(self: *const RouteRecord, res: RouteResource) usize {
        return self.counts[@intFromEnum(res)].load(.acquire);
    }
};

/// A link's route account: the table and the record (`RouteTable.ref`).
pub const RouteRef = struct {
    table: *RouteTable,
    index: u32,

    pub fn rec(self: RouteRef) *RouteRecord {
        return &self.table.records[self.index];
    }

    /// The route's step of `Charge.add`, and the whole of it for bytes a
    /// request already holds when it is dispatched (its header lease): those
    /// are charged to its client and the server already, and to the route
    /// only once the route is known. A bounded compare-and-swap at or above
    /// the resource's mark, so the share is exact; the mark is
    /// check-then-reserve, as for a client. False when the share refused:
    /// counted and logged here. True for a resource a route does not hold.
    pub fn add(self: RouteRef, res: Resource, n: usize, server_use: usize) bool {
        const rr = RouteResource.of(res) orelse return true;
        if (n == 0) return true;
        const t = self.table;
        const r = self.rec();
        const c = &r.counts[@intFromEnum(rr)];
        if (!underPressure(server_use, t.marks[@intFromEnum(rr)])) {
            _ = c.fetchAdd(n, .acq_rel);
            return true;
        }
        const share = r.shares[@intFromEnum(rr)];
        var cur = c.load(.acquire);
        while (true) {
            if (cur > share or share - cur < n) {
                t.noteRefused(self.index, rr, server_use);
                return false;
            }
            cur = c.cmpxchgWeak(cur, cur + n, .acq_rel, .acquire) orelse return true;
        }
    }

    /// Give back `n` of `res`. Below zero is refused: the count stays, and
    /// the event is counted (`RouteTable.underflows`) and logged.
    pub fn sub(self: RouteRef, res: Resource, n: usize) void {
        const rr = RouteResource.of(res) orelse return;
        if (n == 0) return;
        const c = &self.rec().counts[@intFromEnum(rr)];
        var cur = c.load(.acquire);
        while (true) {
            if (cur < n) {
                const total = self.table.underflows.fetchAdd(1, .monotonic) + 1;
                log.err("route account underflow: a release of {d} from {s} of {s} found {d}; refused, count kept ({d} refused so far)", .{ n, @tagName(rr), self.rec().label.text(), cur, total });
                return;
            }
            cur = c.cmpxchgWeak(cur, cur - n, .acq_rel, .acquire) orelse return;
        }
    }
};

/// The task routes' accounts (t-3759 step 5), part of the server's
/// `GlobalAccounting`.
///
/// # The rule
///
/// A task route holds, from dispatch until its handler returns (an orphan
/// included): one task handler, the request-pool bytes the request's arena
/// holds (the header lease's bytes charged at dispatch, every later block as
/// it is taken), and the compression context its response uses. While the
/// server's use of one of these is at or above that resource's pressure mark
/// (1/4 of the server limit, the same mark the client shares use), a charge
/// that would take the route past its share is refused: the task handler or
/// its bytes with a 503 on HTTP/2 and HTTP/1.1 alike, the compression context
/// with identity encoding. Below the mark nothing is refused by a route's
/// share: a healthy busy route is never capped while the server has room. So
/// a broken route can reach about the mark before refusal starts, and the
/// rest of the server stays for every other route.
///
/// # Shares
///
/// A route's share of each resource is the server's limit over the number
/// of task routes, rounded up, or the route's own override
/// (`router.Route.share`). An override of 0, or one above the server's
/// limit, is a boot error; a share below a client's share is logged at boot (a client alone
/// could then hold more of that resource than the route it calls may).
///
/// # The server-wide token stays
///
/// The route charge sits beside the server's own limits, it does not replace
/// them: `GlobalAccounting.task_handlers` is still the hard ceiling of live
/// task handlers, orphans included, and the request pool's cap still bounds
/// its bytes.
pub const RouteTable = struct {
    /// One per task route, in route order.
    records: []RouteRecord = &.{},
    /// By route index (`router.Found.route`): the route's record, or
    /// `no_record` for a complete route, which has no account.
    of_route: []u32 = &.{},
    /// The server's pressure mark of each route resource. Read only after
    /// `init`.
    marks: [RouteResource.count]usize = @splat(std.math.maxInt(usize)),
    /// The route list this table was built from: `ref` takes the list the
    /// caller's route index came from and refuses any other, so a router and
    /// a table built from two lists cannot meet silently, whatever their
    /// lengths or layouts. Borrowed: the owner of the list (`Server`, a test
    /// rig) keeps it for as long as the table.
    built_from: []const router.Route = &.{},
    underflows: std.atomic.Value(u64) = .init(0),
    refusal_log: RateLimitedLine = .{},

    pub const no_record = std.math.maxInt(u32);

    /// No routes: a test connection built by hand with no task route.
    pub const empty: RouteTable = .{};

    pub const InitError = error{ OutOfMemory, RouteShareZero, RouteShareAboveServerLimit, RouteShareOnCompleteRoute };

    /// One record per task route of `routes`, with its share of every route
    /// resource of `limits`; see the type's doc for the rule and the boot
    /// checks.
    pub fn init(gpa: std.mem.Allocator, routes: []const router.Route, limits: RouteLimits) InitError!RouteTable {
        var task_routes: usize = 0;
        for (routes) |r| {
            switch (r.handler) {
                .task => task_routes += 1,
                .complete => if (!r.share.isDefault()) return error.RouteShareOnCompleteRoute,
            }
        }
        const of_route = try gpa.alloc(u32, routes.len);
        errdefer gpa.free(of_route);
        const records = try gpa.alloc(RouteRecord, task_routes);
        errdefer gpa.free(records);
        var marks: [RouteResource.count]usize = undefined;
        for (&marks, limits) |*m, l| m.* = Share.of(l).mark;
        var next: u32 = 0;
        for (routes, of_route) |r, *slot| {
            if (r.handler != .task) {
                slot.* = no_record;
                continue;
            }
            slot.* = next;
            const rec = &records[next];
            rec.* = .{};
            const l = &rec.label;
            const written: []u8 = std.fmt.bufPrint(&l.buf, "{s} {s}{s}", .{ @tagName(r.method), r.path, if (r.prefix) "*" else "" }) catch @as([]u8, &l.buf);
            l.len = @intCast(written.len);
            const overrides: [RouteResource.count]?usize = .{ r.share.task_handlers, r.share.request_bytes, r.share.compression };
            for (overrides, limits, 0..) |o, server_limit, ri| {
                // An override of 0 would refuse every charge of the route
                // whenever the server is at its mark, which is a route turned
                // off under pressure, not a share; the default is never 0.
                if (o) |v| {
                    if (v == 0) {
                        log.err("route {s}: its {s} share is 0: the route would be refused everything under pressure", .{ l.text(), @tagName(@as(RouteResource, @enumFromInt(ri))) });
                        return error.RouteShareZero;
                    }
                }
                const share = o orelse defaultShare(server_limit, task_routes);
                if (share > server_limit) {
                    log.err("route {s}: its {s} share {d} is above the server's limit {d}", .{ l.text(), @tagName(@as(RouteResource, @enumFromInt(ri))), share, server_limit });
                    return error.RouteShareAboveServerLimit;
                }
                const client = Share.of(server_limit).client;
                if (share < client) log.warn("route {s}: its {s} share {d} is below a client's share {d}: one client may hold more of it than this route", .{ l.text(), @tagName(@as(RouteResource, @enumFromInt(ri))), share, client });
                rec.shares[ri] = share;
            }
            next += 1;
        }
        return .{ .records = records, .of_route = of_route, .marks = marks, .built_from = routes };
    }

    /// The default share: the server's limit over the task routes, rounded
    /// up, so no share is zero.
    pub fn defaultShare(server_limit: usize, task_routes: usize) usize {
        std.debug.assert(task_routes > 0);
        return std.math.divCeil(usize, server_limit, task_routes) catch unreachable;
    }

    pub fn deinit(self: *RouteTable, gpa: std.mem.Allocator) void {
        gpa.free(self.records);
        gpa.free(self.of_route);
        self.* = undefined;
    }

    /// Whether `routes` is the list this table was built from: the same
    /// slice, not an equal one.
    pub fn builtFrom(self: *const RouteTable, routes: []const router.Route) bool {
        return routes.ptr == self.built_from.ptr and routes.len == self.built_from.len;
    }

    /// The account of task route `route` (`router.Found.route`) of `routes`,
    /// the router's list. A list this table was not built from, or a route
    /// of it that is not a task route, is a broken invariant (the router and
    /// the table come from one list, and only a task route is charged), so
    /// it panics: two lists of one length but another layout would
    /// otherwise charge the wrong route, or none, silently.
    pub fn ref(self: *RouteTable, routes: []const router.Route, route: u32) RouteRef {
        if (!self.builtFrom(routes)) std.debug.panic("route accounts: a route of a list of {d} routes, but the table was built from another list of {d}", .{ routes.len, self.built_from.len });
        if (route >= routes.len or routes[route].handler != .task) std.debug.panic("route accounts: route {d} of {d} is not a task route", .{ route, routes.len });
        const i = self.of_route[route];
        std.debug.assert(i != no_record);
        return .{ .table = self, .index = i };
    }

    /// The record of route `route`, or null for a complete route: for tests
    /// and the shutdown report.
    pub fn recordOf(self: *RouteTable, route: u32) ?*RouteRecord {
        if (route >= self.of_route.len) return null;
        const i = self.of_route[route];
        if (i == no_record) return null;
        return &self.records[i];
    }

    fn noteRefused(self: *RouteTable, index: u32, res: RouteResource, server_use: usize) void {
        const r = &self.records[index];
        _ = r.refusals[@intFromEnum(res)].fetchAdd(1, .monotonic);
        self.refusal_log.note("refused route {s}: {s} over its share under pressure; route holds task_handlers {d} of {d}, request_bytes {d} of {d}, compression {d} of {d}; the server's {s} {d} (mark {d})", .{
            r.label.text(),
            @tagName(res),
            r.count(.task_handlers),
            r.shares[@intFromEnum(RouteResource.task_handlers)],
            r.count(.request_bytes),
            r.shares[@intFromEnum(RouteResource.request_bytes)],
            r.count(.compression),
            r.shares[@intFromEnum(RouteResource.compression)],
            @tagName(res),
            server_use,
            self.marks[@intFromEnum(res)],
        });
    }

    /// "GET /stuck 3, POST /upload 1": every task route that holds a task
    /// handler now, with its count, or "none". Cut, never refused, when `w`
    /// is full: it is text for a log line.
    pub fn writeHeld(self: *const RouteTable, w: *std.Io.Writer) void {
        var any = false;
        for (self.records) |*r| {
            const n = r.count(.task_handlers);
            if (n == 0) continue;
            w.print("{s}{s} {d}", .{ if (any) ", " else "", r.label.text(), n }) catch return;
            any = true;
        }
        if (!any) w.writeAll("none") catch {};
    }

    /// One orphan of record `index` ended (`dropLink`). Below zero is
    /// refused, counted in `underflows` and logged, like `RouteRef.sub`.
    pub fn releaseOrphan(self: *RouteTable, index: u32) void {
        const r = &self.records[index];
        var cur = r.orphans.load(.acquire);
        while (true) {
            if (cur == 0) {
                const total = self.underflows.fetchAdd(1, .monotonic) + 1;
                log.err("route account underflow: an orphan of {s} ended with its orphan count at 0; refused ({d} refused so far)", .{ r.label.text(), total });
                return;
            }
            cur = r.orphans.cmpxchgWeak(cur, cur - 1, .acq_rel, .acquire) orelse return;
        }
    }

    /// Every route count zero and no underflow over the server's life, or
    /// log what is not. `Server.deinit` calls it after `serve` returned, so
    /// every test that ends a server checks the route accounts too, orphans
    /// included (`serve` waits for them).
    pub fn checkEmpty(self: *const RouteTable) bool {
        var clean = true;
        for (self.records) |*r| {
            for (&r.counts, 0..) |*c, ri| {
                const v = c.load(.acquire);
                if (v == 0) continue;
                clean = false;
                log.err("route account still charged at deinit: {s} {s} = {d}", .{ r.label.text(), @tagName(@as(RouteResource, @enumFromInt(ri))), v });
            }
            const o = r.orphans.load(.acquire);
            if (o != 0) {
                clean = false;
                log.err("route account still charged at deinit: {s} orphans = {d}", .{ r.label.text(), o });
            }
        }
        const u = self.underflows.load(.acquire);
        if (u != 0) {
            clean = false;
            log.err("route accounts: {d} refused releases over the server's life", .{u});
        }
        return clean;
    }
};

/// The live orphans' dispatch times: the age of the oldest orphan,
/// server-wide (`Server.stats`) and per task route (`Server.routeStats`).
///
/// An orphan is a task handler still running after its connection ended
/// (`connection.LinkCore.orphan`). Its age is now minus its dispatch time
/// (`LinkCore.start_ns`), so the oldest needs the smallest start among the
/// orphans alive now. One "oldest start" kept per route cannot follow the
/// orphans as they end: when the oldest ends, the next one is unknown
/// without a list of them. So each orphan holds one slot here, from when it
/// is orphaned until its link is freed, and a reader scans the slots.
///
/// - One slot per task-handler token (`Limits.max_task_handlers`). Every
///   orphan holds a token until its link is freed and gives its slot back
///   before its token (`dropLink`), so the slots in use are never more than
///   the tokens held, and a new orphan, which holds a token, has a free one.
/// - `add` makes one pass and never waits. A pass that finds no free slot
///   is a miss: counted (`misses`, `Server.Stats.orphan_slot_misses`) and
///   logged at error level, at most a line a second
///   (`GlobalAccounting.addOrphan`). The orphan is still counted everywhere
///   else; only its age is left out of the oldest orphan's age. A miss
///   means the invariant above broke, or, at full capacity, the pass raced
///   with frees behind it.
/// - No lock. `add` claims a free slot with a compare-and-swap, writes the
///   route, then publishes the start; `remove` stores 0. Only orphaning and
///   the orphan's end touch this, and orphaning is rare (a connection ended
///   under a running handler). Dispatch only writes its start into its own
///   link.
/// - A reader (`oldestStart`) loads every slot: `max_task_handlers` loads
///   (4096 by default), only when asked.
/// - A `GlobalAccounting` built by hand in a unit test has no slots
///   (`capacity` 0): `add` then records nothing, and says so (null).
pub const Orphans = struct {
    /// The dispatch time (monotonic ns, never 0) of the orphan in the slot;
    /// 0 for a free slot, `claimed` while `add` fills it.
    starts: []std.atomic.Value(u64) = &.{},
    /// The orphan's task route (`RouteRef.index`) + 1, or 0 for none (an
    /// HTTP/2 404 or 405 runs as a task with no route account). Written
    /// before the start is published.
    routes: []std.atomic.Value(u32) = &.{},
    /// One line at most a second when a handler is orphaned
    /// (`GlobalAccounting.addOrphan`).
    log: RateLimitedLine = .{},
    /// Orphans that found no free slot (`add`): their age is not in
    /// `oldestStart`. Only grows.
    misses: std.atomic.Value(u64) = .init(0),
    /// The miss line, at error level: a miss is a broken invariant.
    miss_log: RateLimitedLine = .{ .level = .err },

    const claimed = std.math.maxInt(u64);

    pub fn init(gpa: std.mem.Allocator, capacity: usize) error{OutOfMemory}!Orphans {
        const starts = try gpa.alloc(std.atomic.Value(u64), capacity);
        errdefer gpa.free(starts);
        const routes = try gpa.alloc(std.atomic.Value(u32), capacity);
        @memset(starts, .init(0));
        @memset(routes, .init(0));
        return .{ .starts = starts, .routes = routes };
    }

    pub fn deinit(self: *Orphans, gpa: std.mem.Allocator) void {
        gpa.free(self.starts);
        gpa.free(self.routes);
        self.* = undefined;
    }

    /// Take a slot for an orphan dispatched at `start_ns`, of route record
    /// `route` (null for none): its index. Null when this registry has no
    /// slots (a hand-built test accounting), or when one pass found none
    /// free: then the miss is counted in `misses` (the caller logs it).
    pub fn add(self: *Orphans, start_ns: u64, route: ?u32) ?u32 {
        if (self.starts.len == 0) return null;
        const start = @min(@max(start_ns, 1), claimed - 1);
        for (self.starts, 0..) |*slot, i| {
            if (slot.load(.monotonic) != 0) continue;
            if (slot.cmpxchgStrong(0, claimed, .acquire, .monotonic) != null) continue;
            self.routes[i].store(if (route) |r| r + 1 else 0, .monotonic);
            slot.store(start, .release);
            return @intCast(i);
        }
        _ = self.misses.fetchAdd(1, .monotonic);
        return null;
    }

    /// The orphan in slot `i` ended.
    pub fn remove(self: *Orphans, i: u32) void {
        std.debug.assert(self.starts[i].load(.monotonic) != 0);
        self.starts[i].store(0, .release);
    }

    /// The smallest start among the live orphans of route record `route`, or
    /// of every route when it is null; 0 when there is none. A slot being
    /// filled or reused while it is read is skipped: that orphan is
    /// arriving or gone.
    pub fn oldestStart(self: *const Orphans, route: ?u32) u64 {
        var oldest: u64 = 0;
        for (self.starts, self.routes) |*slot, *r| {
            const st = slot.load(.acquire);
            if (st == 0 or st == claimed) continue;
            if (route) |want| {
                const got = r.load(.monotonic);
                // The route belongs to this start only if the slot still
                // holds it.
                if (slot.load(.acquire) != st) continue;
                if (got != want + 1) continue;
            }
            if (oldest == 0 or st < oldest) oldest = st;
        }
        return oldest;
    }

    /// Slots in use: 0 once every orphan ended (`Server.deinit` checks it).
    pub fn inUse(self: *const Orphans) usize {
        var n: usize = 0;
        for (self.starts) |*slot| {
            if (slot.load(.acquire) != 0) n += 1;
        }
        return n;
    }

    /// Now on the clock `LinkCore.start_ns` and `oldestStart` use.
    pub fn now() u64 {
        return nowNs();
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
    /// The start buckets' theoretical arrival times (`Bucket.take`), by
    /// `BucketKind`. Under the lock. Reset when the record is freed.
    tat: [2]u64 = .{ 0, 0 },

    pub const State = enum(u8) {
        free,
        /// In the index.
        live,
        /// In the index with no holder and no charge: only its start buckets
        /// are not yet full again. Taken back by the next start of its key,
        /// or evicted for another key when the slab is full.
        rate_only,
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

    /// When both buckets are full again.
    fn refilledAt(self: *const Record) u64 {
        return @max(self.tat[0], self.tat[1]);
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
    /// Records kept only for their start buckets.
    rate_only: usize,
    /// Rate-only records taken for another key because the slab was full.
    evictions: u64,
    /// Starts refused by an empty bucket under pressure, by `BucketKind`.
    rate_refusals: [2]u64,
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
    rate_only: usize = 0,
    inserts: u64 = 0,
    reuses: u64 = 0,
    /// Records that left the index (a rate-only record when it is evicted).
    removals: u64 = 0,
    evictions: u64 = 0,
    /// Atomic, not under the lock.
    insert_failures: std.atomic.Value(u64) = .init(0),
    underflows: std.atomic.Value(u64) = .init(0),
    stale: std.atomic.Value(u64) = .init(0),
    /// Read only after `init`.
    shares: Shares,
    rules: Rules,
    /// Charges refused by share under pressure, per resource (`Charge.add`).
    share_refusals: [resource_count]std.atomic.Value(u64) = @splat(.init(0)),
    /// Starts refused by a bucket, by `BucketKind`. Atomic for readers; only
    /// written under the lock.
    rate_refusals: [2]std.atomic.Value(u64) = @splat(.init(0)),
    /// What the refusal line says about each endpoint (`setEndpointLabel`).
    labels: [max_labels]Label = @splat(.{}),
    refusal_log: RateLimitedLine = .{},

    pub const max_labels = 64;

    /// "endpoint 0 (h2c_prior_knowledge) client_identity=.peer", written by
    /// the server at boot.
    pub const Label = struct {
        buf: [80]u8 = undefined,
        len: u8 = 0,

        pub fn text(self: *const Label) []const u8 {
            return self.buf[0..self.len];
        }
    };

    /// `capacity` records, an index of at least twice as many entries, and a
    /// hash keyed by `seed` (the server draws it at boot, so a client cannot
    /// pick keys that collide in a known pattern). `rules` from the
    /// server's limits (`Limits.accountRules`).
    pub fn init(gpa: std.mem.Allocator, capacity: usize, seed: u64, rules: Rules) error{OutOfMemory}!Table {
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
            .shares = rules.shares,
            .rules = rules,
        };
    }

    /// The text the refusal line uses for endpoint `i`. Boot only, before
    /// any connection.
    pub fn setEndpointLabel(self: *Table, i: usize, comptime fmt: []const u8, args: anytype) void {
        std.debug.assert(i < max_labels);
        const l = &self.labels[i];
        // A label too long for the buffer is cut, never refused: it is text
        // for a log line.
        const written: []u8 = std.fmt.bufPrint(&l.buf, fmt, args) catch @as([]u8, &l.buf);
        l.len = @intCast(written.len);
    }

    /// A charge refused by share (`Charge.add`): counted, and one warn line
    /// at most a second for the whole server, naming the identity, what it
    /// holds as a part of the server's use, and the endpoint. Lock-free: the
    /// record is the refused holder's own, so its key does not change.
    pub fn noteRefused(self: *Table, res: Resource, rec: *const Record, held: usize, server_use: usize, endpoint: u16) void {
        _ = self.share_refusals[@intFromEnum(res)].fetchAdd(1, .monotonic);
        const pct = if (server_use == 0) 0 else held * 100 / server_use;
        const label: []const u8 = if (endpoint < max_labels) self.labels[endpoint].text() else "endpoint ?";
        self.refusal_log.note("refused {f}: {d}% of {s} ({d} of the server's {d}, over its share under pressure), {s}", .{ fmtKey(rec.key), pct, @tagName(res), held, server_use, label });
    }

    pub fn shareRefusals(self: *const Table, res: Resource) u64 {
        return self.share_refusals[@intFromEnum(res)].load(.acquire);
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
    /// under one hold of the lock. `.table_full` when the slab cannot hold a
    /// new key: the references already taken are given back, the failure is
    /// counted, and the caller refuses the connection. Cancelable: the accept
    /// loop calls it, and a cancel there is the server stopping. `endpoint`
    /// is the index of the endpoint the connection came in on (the refusal
    /// line names it).
    ///
    /// `start`, for a connection start: every record's start buckets take a
    /// token for it, all or none. A bucket with none refuses while its
    /// resource is at its pressure mark: then the references go back (a
    /// record that had no other holder goes back to rate-only, its buckets
    /// unchanged), the refusal is counted and logged, and the result is
    /// `.rate_limited`; the caller closes the connection. With no pressure an
    /// empty bucket admits and stays empty. Null: no bucket is touched.
    pub fn acquire(self: *Table, keys: []const Key, endpoint: u16, start: ?Start) std.Io.Cancelable!Acquired {
        std.debug.assert(keys.len <= max_refs);
        var c: Charge = .{ .table = self, .endpoint = endpoint };
        if (keys.len == 0) return .{ .charge = .none };
        try self.mu.lock();
        defer self.mu.unlock();
        for (keys) |k| {
            const i = self.find(k) orelse self.insert(k, keys) orelse {
                self.dropAllLocked(&c);
                _ = self.insert_failures.fetchAdd(1, .monotonic);
                log.warn("client table full ({d} records): connection refused", .{self.records.len});
                return .table_full;
            };
            const rec = &self.records[i];
            // Under the lock the generation cannot change (it changes only
            // at removal, under the lock), so a plain add is a reference on
            // this generation.
            const w = rec.word.fetchAdd(1, .acq_rel);
            std.debug.assert(Record.holdersOf(w) < std.math.maxInt(u32));
            std.debug.assert(Record.generationOf(w) != retired_generation);
            if (rec.state == .rate_only) {
                std.debug.assert(Record.holdersOf(w) == 0);
                rec.state = .live;
                self.rate_only -= 1;
                self.live += 1;
            }
            c.refs[c.len] = .{ .index = i, .generation = Record.generationOf(w) };
            c.len += 1;
        }
        const st = start orelse return .{ .charge = c };
        // Every bucket of every record, before any moves: all or none.
        var next: [max_refs][2]?u64 = @splat(.{ null, null });
        for (c.slice(), 0..) |r, ri| {
            const rec = &self.records[r.index];
            inline for (.{ BucketKind.connection_start, BucketKind.handshake_start }) |kind| {
                const used: ?usize = switch (kind) {
                    .connection_start => st.connections,
                    .handshake_start => st.handshakes,
                };
                if (used) |u| {
                    const res: Resource = if (kind == .connection_start) .connections else .handshakes;
                    const t = self.rules.bucket(kind).take(rec.tat[@intFromEnum(kind)], st.now_ns);
                    if (t == null and u >= self.shares[@intFromEnum(res)].mark) {
                        _ = self.rate_refusals[@intFromEnum(kind)].fetchAdd(1, .monotonic);
                        self.noteRateRefused(kind, rec, u, endpoint);
                        self.dropAllLocked(&c);
                        return .rate_limited;
                    }
                    next[ri][@intFromEnum(kind)] = t;
                }
            }
        }
        for (c.slice(), 0..) |r, ri| {
            const rec = &self.records[r.index];
            for (next[ri], 0..) |t, k| {
                if (t) |v| rec.tat[k] = v;
            }
        }
        return .{ .charge = c };
    }

    /// Under the lock: give back every reference `c` holds, in reverse.
    fn dropAllLocked(self: *Table, c: *Charge) void {
        while (c.len > 0) {
            c.len -= 1;
            self.dropLocked(c.refs[c.len]);
        }
    }

    /// Under the lock, so the key is stable: a start refused by its bucket.
    fn noteRateRefused(self: *Table, kind: BucketKind, rec: *const Record, server_use: usize, endpoint: u16) void {
        const b = self.rules.bucket(kind);
        const res: Resource = if (kind == .connection_start) .connections else .handshakes;
        const held = rec.counts[@intFromEnum(res)].load(.monotonic);
        const pct = if (server_use == 0) 0 else held * 100 / server_use;
        const label: []const u8 = if (endpoint < max_labels) self.labels[endpoint].text() else "endpoint ?";
        self.refusal_log.note("refused {f}: {s}s over its rate (burst {d}, one every {d} ms) under pressure, {d}% of {s} ({d} of the server's {d}), {s}", .{ fmtKey(rec.key), @tagName(kind), b.burst, b.interval_ns / std.time.ns_per_ms, pct, @tagName(res), held, server_use, label });
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
        self.live -= 1;
        // `seq_cst` with the word above and with `addAt`'s add and re-read:
        // a stale charge either lands before this read (and quarantines the
        // record, loudly) or reads the moved generation and takes itself
        // back.
        var left: usize = 0;
        for (&rec.counts) |*count| {
            if (count.load(.seq_cst) != 0) left += 1;
        }
        // Nothing charged, a generation still to hand out, and a start
        // bucket not yet full: keep it for its buckets (the file header).
        if (left == 0 and Record.generationOf(dropWord(cur)) != retired_generation and rec.refilledAt() > nowNs()) {
            rec.state = .rate_only;
            self.rate_only += 1;
            return;
        }
        self.removeIndex(r.index);
        self.removals += 1;
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
        self.freeLocked(r.index);
    }

    fn freeLocked(self: *Table, i: u32) void {
        const rec = &self.records[i];
        rec.state = .free;
        rec.tat = .{ 0, 0 };
        self.free[self.free_len] = i;
        self.free_len += 1;
    }

    /// Under the lock, with no free record: the rate-only record whose
    /// buckets are full soonest leaves the index and is freed. False when
    /// there is none. A scan of the slab, made only when it is full.
    ///
    /// Never one of `keep`, the keys of the charge being acquired: a new
    /// /64's insert would otherwise evict its own /48's rate-only record,
    /// and the /48 would come back with a fresh bucket, so a /48 could
    /// regain its burst by rotating /64s.
    fn evictLocked(self: *Table, keep: []const Key) bool {
        var best: ?u32 = null;
        var best_at: u64 = std.math.maxInt(u64);
        scan: for (self.records, 0..) |*rec, i| {
            if (rec.state != .rate_only) continue;
            for (keep) |k| {
                if (rec.key.eql(k)) continue :scan;
            }
            if (rec.refilledAt() < best_at) {
                best_at = rec.refilledAt();
                best = @intCast(i);
            }
        }
        const i = best orelse return false;
        std.debug.assert(self.records[i].holders() == 0);
        self.removeIndex(i);
        self.rate_only -= 1;
        self.removals += 1;
        self.evictions += 1;
        self.freeLocked(i);
        return true;
    }

    /// The record `r` names, or null when its generation is not the record's:
    /// a reference that outlived its record, which is a bug. Counted
    /// (`stale`) and logged, and the caller skips the record.
    /// The step of `Charge.add` after the generation check: add, then read
    /// the generation again. If the record left the index in between, the
    /// add landed on whatever the record holds now: it is taken back and
    /// counted (`stale`). Public so a test can order an acquire between the
    /// check and this step.
    ///
    /// `bound`, when not null, is the share: the add is a compare-and-swap
    /// that refuses when the count would pass it, and returns the count it
    /// found. A refusal is checked against the generation too, so a stale
    /// reference is never refused on another key's count.
    pub fn addAt(self: *Table, rec: *Record, r: Ref, res: Resource, n: usize, bound: ?usize) AddResult {
        const c = &rec.counts[@intFromEnum(res)];
        if (bound) |b| {
            var cur = c.load(.seq_cst);
            while (true) {
                if (cur > b or b - cur < n) {
                    const g = Record.generationOf(rec.word.load(.seq_cst));
                    if (g == r.generation) return .{ .refused = cur };
                    self.noteStale(r, g);
                    return .stale;
                }
                cur = c.cmpxchgWeak(cur, cur + n, .seq_cst, .seq_cst) orelse break;
            }
        } else {
            _ = c.fetchAdd(n, .seq_cst);
        }
        const g = Record.generationOf(rec.word.load(.seq_cst));
        if (g == r.generation) return .took;
        _ = c.fetchSub(n, .seq_cst);
        self.noteStale(r, g);
        return .stale;
    }

    pub const AddResult = union(enum) {
        took,
        /// The reference was stale (counted): nothing is charged, and the
        /// caller skips this record, as for a failed `record`.
        stale,
        /// Over the share under pressure: the count found.
        refused: usize,
    };

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

    /// A free record for `k`, in the index; null when none is free and no
    /// rate-only record other than one of `keep` (the charge's own keys) can
    /// be evicted.
    fn insert(self: *Table, k: Key, keep: []const Key) ?u32 {
        if (self.free_len == 0 and !self.evictLocked(keep)) return null;
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
            // A retired record holds nothing: it is only never reused. A
            // rate-only one holds no charge and no holder, only its buckets.
            if (rec.state == .free or rec.state == .retired or rec.state == .rate_only) continue;
            clean = false;
            log.err("client account still {s} at deinit: record {d} {f}, {d} holders", .{ @tagName(rec.state), i, fmtKey(rec.key), rec.holders() });
            for (&rec.counts, 0..) |*count, ri| {
                const v = count.load(.acquire);
                if (v != 0) log.err("  {s} = {d}", .{ @tagName(@as(Resource, @enumFromInt(ri))), v });
            }
        }
        for (self.index) |e| {
            if (e != 0 and self.records[e - 1].state != .rate_only) clean = false;
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
            .rate_only = self.rate_only,
            .evictions = self.evictions,
            .rate_refusals = .{ self.rate_refusals[0].load(.acquire), self.rate_refusals[1].load(.acquire) },
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
            if (rec.state == .free or rec.state == .retired or rec.state == .rate_only) continue;
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

    /// The live record for `k`, if any (`snapshot` filtered by key). A
    /// rate-only record holds nothing, so it is not one (`rateOnly`).
    pub fn lookup(self: *Table, k: Key) ?Entry {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        const i = self.find(k) orelse return null;
        const rec = &self.records[i];
        if (rec.state != .live) return null;
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

    /// Whether `k` has a rate-only record: no holder, no charge, only start
    /// buckets not yet full again.
    pub fn rateOnly(self: *Table, k: Key) bool {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        const i = self.find(k) orelse return false;
        return self.records[i].state == .rate_only;
    }
};

const log = std.log.scoped(.starh2);

fn nowNs() u64 {
    return zio.Timestamp.now(.monotonic).toNanoseconds();
}

/// One warn line at most per `interval_ns` (a second by default), with the
/// count of the ones held back: a client that is refused under an attack
/// must not become a flood of lines.
///
/// The held count is printed by the next line, or by `flush` when no line
/// comes: `Server.serve` flushes every such line of the server once a second
/// and once when it ends (`Server.flushLines`). Without the flush, the last
/// burst inside an interval reached no output at all.
pub const RateLimitedLine = struct {
    last_ns: std.atomic.Value(u64) = .init(0),
    held: std.atomic.Value(u64) = .init(0),
    interval_ns: u64 = std.time.ns_per_s,
    /// `err` for a line that reports a broken invariant.
    level: Level = .warn,

    pub const Level = enum { warn, err };

    fn emit(self: *const RateLimitedLine, comptime fmt: []const u8, args: anytype) void {
        switch (self.level) {
            .warn => log.warn(fmt, args),
            .err => log.err(fmt, args),
        }
    }

    pub fn note(self: *RateLimitedLine, comptime fmt: []const u8, args: anytype) void {
        const now = zio.Timestamp.now(.monotonic).toNanoseconds();
        const last = self.last_ns.load(.monotonic);
        if (last != 0 and now -% last < self.interval_ns) {
            _ = self.held.fetchAdd(1, .monotonic);
            return;
        }
        if (self.last_ns.cmpxchgStrong(last, now, .monotonic, .monotonic) != null) {
            _ = self.held.fetchAdd(1, .monotonic);
            return;
        }
        const held = self.held.swap(0, .monotonic);
        self.emit(fmt ++ " ({d} more since the last report)", args ++ .{held});
    }

    /// Print the held count, if any, once the interval since the last line
    /// passed, or at once with `force` (the server's last flush). The line
    /// has only `what` and the count: the held lines' own details were not
    /// kept. It takes the interval like a line of its own, so one line per
    /// interval stays the bound.
    pub fn flush(self: *RateLimitedLine, comptime what: []const u8, force: bool) void {
        if (self.held.load(.monotonic) == 0) return;
        const now = zio.Timestamp.now(.monotonic).toNanoseconds();
        const last = self.last_ns.load(.monotonic);
        if (!force and now -% last < self.interval_ns) return;
        // A line printed meanwhile carries the held count itself.
        if (self.last_ns.cmpxchgStrong(last, now, .monotonic, .monotonic) != null) return;
        const held = self.held.swap(0, .monotonic);
        if (held == 0) return;
        self.emit(what ++ ": {d} more since the last report", .{held});
    }
};

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
    var t = try Table.init(gpa, 4, 0x1234, Rules.unlimited);
    defer t.deinit(gpa);
    var out: [max_refs]Key = undefined;
    const k = keysFor(.{ .ip4 = .{ 1, 2, 3, 4 } }, 48, &out);
    const c1 = (try t.acquire(k, 0, null)).charge;
    const c2 = (try t.acquire(k, 0, null)).charge;
    try std.testing.expectEqual(c1.refs[0].index, c2.refs[0].index);
    try std.testing.expectEqual(@as(u32, 2), t.lookup(k[0]).?.holders);
    t.retain(c1);
    try std.testing.expectEqual(@as(u32, 3), t.lookup(k[0]).?.holders);
    try std.testing.expectEqual(Charge.Outcome.took, c1.add(.streams, 3, 0));
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
    const c3 = (try t.acquire(k2, 0, null)).charge;
    try std.testing.expectEqual(c1.refs[0].index, c3.refs[0].index);
    try std.testing.expectEqual(g1 + 1, c3.refs[0].generation);
    t.release(c3);
    const s = t.stats();
    try std.testing.expectEqual(@as(u64, 2), s.inserts);
    try std.testing.expectEqual(@as(u64, 1), s.reuses);
    try std.testing.expectEqual(@as(u64, 2), s.removals);
    try std.testing.expect(t.checkEmpty());
}

test "Charge.add: shares apply only at the mark; an aggregate's refusal undoes the client's charge" {
    const gpa = std.testing.allocator;
    var shares: Shares = unlimited;
    // L = 16: client share 2, aggregate share 4, mark 4.
    shares[@intFromEnum(Resource.streams)] = .of(16);
    var t = try Table.init(gpa, 8, 3, .{ .shares = shares, .connection_start = Rules.unlimited.connection_start, .handshake_start = Rules.unlimited.handshake_start });
    defer t.deinit(gpa);
    var o1: [max_refs]Key = undefined;
    var o2: [max_refs]Key = undefined;
    const a: [16]u8 = .{ 0x20, 0x01, 0x0d, 0xb8, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1 };
    var b = a;
    b[7] = 2; // another /64 in the same /48
    const ca = (try t.acquire(keysFor(.{ .ip6 = a }, 48, &o1), 0, null)).charge;
    const cb = (try t.acquire(keysFor(.{ .ip6 = b }, 48, &o2), 0, null)).charge;
    // Below the mark nothing is refused, past any share.
    for (0..3) |_| try std.testing.expectEqual(Charge.Outcome.took, ca.add(.streams, 1, 0));
    // At the mark: /64 a holds 3 > its share.
    try std.testing.expectEqual(Charge.Outcome.client_refused, ca.add(.streams, 1, 4));
    // /64 b has room in its share, but the aggregate holds 3 of 4: one more
    // fits, the next is refused by the aggregate and b's charge is undone.
    try std.testing.expectEqual(Charge.Outcome.took, cb.add(.streams, 1, 4));
    try std.testing.expectEqual(Charge.Outcome.client_refused, cb.add(.streams, 1, 5));
    try std.testing.expectEqual(@as(usize, 1), t.lookup(o2[0]).?.count(.streams));
    try std.testing.expectEqual(@as(usize, 4), t.lookup(o1[1]).?.count(.streams));
    try std.testing.expectEqual(@as(u64, 2), t.shareRefusals(.streams));
    // A byte-sized charge is refused when it would pass the share, not
    // only when the share is already passed.
    try std.testing.expectEqual(Charge.Outcome.client_refused, cb.add(.streams, 2, 4));
    for (0..3) |_| ca.sub(.streams, 1);
    cb.sub(.streams, 1);
    t.release(ca);
    t.release(cb);
    try std.testing.expect(t.checkEmpty());
}

fn bucketRules(burst: u64, interval_ns: u64) Rules {
    var r = Rules.unlimited;
    // L = 4: the connections' mark is 1.
    r.shares[@intFromEnum(Resource.connections)] = .of(4);
    r.shares[@intFromEnum(Resource.handshakes)] = .of(4);
    r.connection_start = .{ .burst = burst, .interval_ns = interval_ns };
    r.handshake_start = .{ .burst = burst, .interval_ns = interval_ns };
    return r;
}

test "start bucket: a serial reconnect keeps its bucket in a rate-only record and does not regain its burst" {
    const gpa = std.testing.allocator;
    var t = try Table.init(gpa, 4, 5, bucketRules(3, std.time.ns_per_s));
    defer t.deinit(gpa);
    var o: [max_refs]Key = undefined;
    const k = keysFor(.{ .ip4 = .{ 10, 1, 1, 1 } }, 48, &o);
    // The release path reads the real clock: the starts are dated on it.
    const now = nowNs();
    // Under pressure (one connection held, the mark is 1): three starts,
    // each released before the next, then refused.
    for (0..3) |i| {
        const got = try t.acquire(k, 0, .{ .now_ns = now + i, .connections = 1, .handshakes = null });
        t.release(got.charge);
        try std.testing.expect(t.rateOnly(k[0]));
        try std.testing.expect(t.lookup(k[0]) == null);
    }
    try std.testing.expect((try t.acquire(k, 0, .{ .now_ns = now + 3, .connections = 1, .handshakes = null })) == .rate_limited);
    // With no pressure an empty bucket admits.
    const idle = try t.acquire(k, 0, .{ .now_ns = now + 4, .connections = 0, .handshakes = null });
    t.release(idle.charge);
    // One interval later one token is back, and only one.
    const later = try t.acquire(k, 0, .{ .now_ns = now + std.time.ns_per_s + 5, .connections = 1, .handshakes = null });
    t.release(later.charge);
    try std.testing.expect((try t.acquire(k, 0, .{ .now_ns = now + std.time.ns_per_s + 6, .connections = 1, .handshakes = null })) == .rate_limited);
    // The handshake bucket is its own, on its own pressure.
    const tls = try t.acquire(k, 0, .{ .now_ns = now + std.time.ns_per_s + 7, .connections = 0, .handshakes = 1 });
    t.release(tls.charge);
    const s = t.stats();
    try std.testing.expectEqual(@as(u64, 2), s.rate_refusals[@intFromEnum(BucketKind.connection_start)]);
    try std.testing.expectEqual(@as(u64, 1), s.inserts);
    try std.testing.expectEqual(@as(usize, 1), s.rate_only);
    try std.testing.expectEqual(@as(usize, 0), s.live);
    // A rate-only record is not a leak.
    try std.testing.expect(t.checkEmpty());
}

test "start bucket: a full slab evicts the rate-only record that refills soonest, never one with a holder" {
    const gpa = std.testing.allocator;
    var t = try Table.init(gpa, 3, 6, bucketRules(4, std.time.ns_per_s));
    defer t.deinit(gpa);
    const now = nowNs();
    var keys: [5][max_refs]Key = undefined;
    var k: [5][]const Key = undefined;
    for (&keys, &k, 0..) |*o, *kk, i| kk.* = keysFor(.{ .ip4 = .{ 10, 2, 0, @intCast(i) } }, 48, o);
    // 0 starts three times (refills last), 1 once (refills first), 2 holds.
    for (0..3) |i| t.release((try t.acquire(k[0], 0, .{ .now_ns = now + i, .connections = 0, .handshakes = null })).charge);
    t.release((try t.acquire(k[1], 0, .{ .now_ns = now + 3, .connections = 0, .handshakes = null })).charge);
    const held = (try t.acquire(k[2], 0, .{ .now_ns = now + 4, .connections = 0, .handshakes = null })).charge;
    // The slab is full: 3 takes 1's record, then 4 takes 0's.
    const c3 = (try t.acquire(k[3], 0, .{ .now_ns = now + 5, .connections = 0, .handshakes = null })).charge;
    try std.testing.expect(t.rateOnly(k[0][0]));
    try std.testing.expect(!t.rateOnly(k[1][0]));
    const c4 = (try t.acquire(k[4], 0, .{ .now_ns = now + 6, .connections = 0, .handshakes = null })).charge;
    try std.testing.expect(!t.rateOnly(k[0][0]));
    // Nothing left to evict: every record has a holder.
    var o5: [max_refs]Key = undefined;
    try std.testing.expect((try t.acquire(keysFor(.{ .ip4 = .{ 10, 2, 0, 9 } }, 48, &o5), 0, null)) == .table_full);
    const s = t.stats();
    try std.testing.expectEqual(@as(u64, 2), s.evictions);
    try std.testing.expectEqual(@as(u64, 1), s.insert_failures);
    t.release(held);
    t.release(c3);
    t.release(c4);
    try std.testing.expect(t.checkEmpty());
}

test "start bucket: a record evicted for a new key gives the new key a full bucket" {
    const gpa = std.testing.allocator;
    var t = try Table.init(gpa, 1, 8, bucketRules(1, std.time.ns_per_s));
    defer t.deinit(gpa);
    const now = nowNs();
    var oa: [max_refs]Key = undefined;
    var ob: [max_refs]Key = undefined;
    const ka = keysFor(.{ .ip4 = .{ 10, 3, 0, 1 } }, 48, &oa);
    const kb = keysFor(.{ .ip4 = .{ 10, 3, 0, 2 } }, 48, &ob);
    // A spends its one token and leaves a rate-only record, the only one.
    t.release((try t.acquire(ka, 0, .{ .now_ns = now, .connections = 0, .handshakes = null })).charge);
    try std.testing.expect(t.rateOnly(ka[0]));
    // B takes that record, under pressure: its own bucket is full.
    const got = try t.acquire(kb, 0, .{ .now_ns = now + 1, .connections = 1, .handshakes = null });
    try std.testing.expect(got == .charge);
    try std.testing.expectEqual(@as(u64, 1), t.stats().evictions);
    t.release(got.charge);
    try std.testing.expect(t.checkEmpty());
}

test "start bucket: a new /64 never evicts its own /48's rate-only record (a /48 cannot rotate /64s for a fresh burst)" {
    const gpa = std.testing.allocator;
    // Three records; a burst of one that does not refill during the test.
    var t = try Table.init(gpa, 3, 9, bucketRules(1, 600 * std.time.ns_per_s));
    defer t.deinit(gpa);
    const now = nowNs();
    const idle: Start = .{ .now_ns = now, .connections = 0, .handshakes = null };
    var ox: [max_refs]Key = undefined;
    var oy: [max_refs]Key = undefined;
    var o1: [max_refs]Key = undefined;
    var o2: [max_refs]Key = undefined;
    const g1: [16]u8 = .{ 0x20, 0x01, 0x0d, 0xb8, 0, 0x30, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1 };
    var g2 = g1;
    g2[7] = 2; // another /64 of the same /48
    // X holds a record for the whole test, so the slab stays full.
    const x = (try t.acquire(keysFor(.{ .ip4 = .{ 10, 4, 0, 1 } }, 48, &ox), 0, idle)).charge;
    // The first /64 starts once: its /64 and its /48 spend their one token
    // and stay rate-only.
    t.release((try t.acquire(keysFor(.{ .ip6 = g1 }, 48, &o1), 0, idle)).charge);
    // Y needs a record: the slab is full, and the first /64's record (it
    // refills no later than the /48's, and comes first) is evicted. Y then
    // leaves its own rate-only record, refilling after the /48's.
    t.release((try t.acquire(keysFor(.{ .ip4 = .{ 10, 4, 0, 2 } }, 48, &oy), 0, .{ .now_ns = now + 1, .connections = 0, .handshakes = null })).charge);
    try std.testing.expect(t.rateOnly(o1[1]));
    // A second /64 of the /48, under pressure. Its /64 needs a record, and
    // the /48's rate-only record refills soonest: evicting it would give the
    // /48 a fresh bucket. Its token is spent: refused.
    const got = try t.acquire(keysFor(.{ .ip6 = g2 }, 48, &o2), 0, .{ .now_ns = now + 2, .connections = 1, .handshakes = null });
    try std.testing.expect(got == .rate_limited);
    try std.testing.expect(t.rateOnly(o1[1]));
    t.release(x);
    try std.testing.expect(t.checkEmpty());
}

test "Table: a full slab refuses the insert and gives back what it took" {
    const gpa = std.testing.allocator;
    var t = try Table.init(gpa, 2, 7, Rules.unlimited);
    defer t.deinit(gpa);
    var o1: [max_refs]Key = undefined;
    const a: [16]u8 = .{ 0x20, 0x01, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1 };
    // Two records: the /64 and the /48.
    const c1 = (try t.acquire(keysFor(.{ .ip6 = a }, 48, &o1), 0, null)).charge;
    var o2: [max_refs]Key = undefined;
    var b = a;
    b[7] = 2; // same /48, a new /64: the slab is full
    try std.testing.expect((try t.acquire(keysFor(.{ .ip6 = b }, 48, &o2), 0, null)) == .table_full);
    try std.testing.expectEqual(@as(u32, 1), t.lookup(o1[1]).?.holders);
    try std.testing.expectEqual(@as(u64, 1), t.stats().insert_failures);
    t.release(c1);
    try std.testing.expect(t.checkEmpty());
}

test "Table: backward-shift deletion keeps every remaining key reachable" {
    const gpa = std.testing.allocator;
    // A small index and many keys, so probe chains wrap and overlap.
    var t = try Table.init(gpa, 24, 99, Rules.unlimited);
    defer t.deinit(gpa);
    var charges: [24]Charge = undefined;
    var keys: [24]Key = undefined;
    for (&charges, &keys, 0..) |*c, *k, i| {
        var o: [max_refs]Key = undefined;
        k.* = keysFor(.{ .ip4 = .{ 10, 0, @intCast(i / 256), @intCast(i % 256) } }, 48, &o)[0];
        c.* = (try t.acquire(&.{k.*}, 0, null)).charge;
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

fn testTaskRoutes(comptime n: usize, out: *[n + 1]router.Route) []const router.Route {
    const H = struct {
        fn task(_: *anyopaque, _: *const request.Request, _: *@import("../http/response.zig").Response) anyerror!void {}
        fn complete(_: *anyopaque, _: *const request.Request, _: *@import("../http/response.zig").CompleteResponse) anyerror!void {}
        var dummy: u8 = 0;
    };
    for (out[0..n]) |*r| r.* = .{ .method = .GET, .path = "/t", .handler = .{ .task = .{ .ptr = &H.dummy, .runFn = H.task } } };
    // A complete route counts for nothing.
    out[n] = .{ .method = .GET, .path = "/c", .handler = .{ .complete = .{ .ptr = &H.dummy, .runFn = H.complete } } };
    return out;
}

test "RouteTable: the default share is the server's limit over the task routes, rounded up (1, 2 and 8 task routes)" {
    const gpa = std.testing.allocator;
    const limits: RouteLimits = .{ 4096, 256 * 1024 * 1024, 128 };
    inline for (.{ 1, 2, 8 }) |n| {
        var buf: [n + 1]router.Route = undefined;
        const routes = testTaskRoutes(n, &buf);
        var t = try RouteTable.init(gpa, routes, limits);
        defer t.deinit(gpa);
        try std.testing.expectEqual(@as(usize, n), t.records.len);
        for (0..n) |i| {
            const rec = t.recordOf(@intCast(i)).?;
            try std.testing.expectEqual(@as(usize, 4096 / n), rec.shares[@intFromEnum(RouteResource.task_handlers)]);
            try std.testing.expectEqual(@as(usize, 256 * 1024 * 1024 / n), rec.shares[@intFromEnum(RouteResource.request_bytes)]);
            try std.testing.expectEqual(@as(usize, 128 / n), rec.shares[@intFromEnum(RouteResource.compression)]);
        }
        // The complete route has no account.
        try std.testing.expect(t.recordOf(@intCast(n)) == null);
        try std.testing.expect(t.builtFrom(routes));
    }
    // Rounded up: 3 task routes of 128 contexts get 43 each, never 42.
    var buf3: [4]router.Route = undefined;
    var t3 = try RouteTable.init(gpa, testTaskRoutes(3, &buf3), limits);
    defer t3.deinit(gpa);
    try std.testing.expectEqual(@as(usize, 43), t3.recordOf(0).?.shares[@intFromEnum(RouteResource.compression)]);
    try std.testing.expectEqual(@as(usize, 1366), t3.recordOf(0).?.shares[@intFromEnum(RouteResource.task_handlers)]);
}

test "RouteTable: an override replaces the default for that resource only" {
    const gpa = std.testing.allocator;
    var buf: [3]router.Route = undefined;
    const routes = testTaskRoutes(2, &buf);
    buf[0].share = .{ .task_handlers = 4096 };
    var t = try RouteTable.init(gpa, routes, .{ 4096, 1024, 128 });
    defer t.deinit(gpa);
    try std.testing.expectEqual(@as(usize, 4096), t.recordOf(0).?.shares[@intFromEnum(RouteResource.task_handlers)]);
    try std.testing.expectEqual(@as(usize, 512), t.recordOf(0).?.shares[@intFromEnum(RouteResource.request_bytes)]);
    try std.testing.expectEqual(@as(usize, 2048), t.recordOf(1).?.shares[@intFromEnum(RouteResource.task_handlers)]);
    // An override on a complete route cannot mean anything.
    buf[2].share = .{ .compression = 1 };
    try std.testing.expectError(error.RouteShareOnCompleteRoute, RouteTable.init(gpa, routes, .{ 4096, 1024, 128 }));
}

test "Charge.add: a route over its share is refused only at the mark, and its refusal undoes the client's charge" {
    const gpa = std.testing.allocator;
    var buf: [3]router.Route = undefined;
    // L = 16 task handlers, two task routes: each a share of 8, the mark 4.
    const routes_list = testTaskRoutes(2, &buf);
    var rt = try RouteTable.init(gpa, routes_list, .{ 16, 1024, 16 });
    defer rt.deinit(gpa);
    var t = try Table.init(gpa, 4, 7, Rules.unlimited);
    defer t.deinit(gpa);
    var o: [max_refs]Key = undefined;
    const k = keysFor(.{ .ip4 = .{ 10, 0, 0, 1 } }, 48, &o);
    const conn = (try t.acquire(k, 0, null)).charge;
    const c = conn.withRoute(rt.ref(routes_list, 0));
    // Below the mark: past the route's share, nothing is refused.
    for (0..10) |_| try std.testing.expectEqual(Charge.Outcome.took, c.add(.task_handlers, 1, 0));
    try std.testing.expectEqual(@as(usize, 10), rt.recordOf(0).?.count(.task_handlers));
    // At the mark the route holds 10 > 8: refused, and the client's charge of
    // this call is undone.
    try std.testing.expectEqual(Charge.Outcome.route_refused, c.add(.task_handlers, 1, 4));
    try std.testing.expectEqual(@as(usize, 10), t.lookup(k[0]).?.count(.task_handlers));
    try std.testing.expectEqual(@as(usize, 10), rt.recordOf(0).?.count(.task_handlers));
    try std.testing.expectEqual(@as(u64, 1), rt.recordOf(0).?.refusals[@intFromEnum(RouteResource.task_handlers)].load(.acquire));
    // The other route has its whole share at the mark.
    const c1 = conn.withRoute(rt.ref(routes_list, 1));
    for (0..8) |_| try std.testing.expectEqual(Charge.Outcome.took, c1.add(.task_handlers, 1, 4));
    try std.testing.expectEqual(Charge.Outcome.route_refused, c1.add(.task_handlers, 1, 4));
    // A byte charge is refused when it would pass the share.
    try std.testing.expectEqual(Charge.Outcome.took, c1.add(.request_bytes, 500, 300));
    try std.testing.expectEqual(Charge.Outcome.route_refused, c1.add(.request_bytes, 13, 300));
    // A resource a route does not hold passes through it.
    try std.testing.expectEqual(Charge.Outcome.took, c1.add(.streams, 1, 1 << 30));
    c1.sub(.streams, 1);
    // Release walks the route too.
    for (0..10) |_| c.sub(.task_handlers, 1);
    for (0..8) |_| c1.sub(.task_handlers, 1);
    c1.sub(.request_bytes, 500);
    try std.testing.expect(rt.checkEmpty());
    t.release(conn);
    try std.testing.expect(t.checkEmpty());
}

test "Charge.add: a route refusal from an IPv6 client undoes both the /64 and the /48 aggregate" {
    const gpa = std.testing.allocator;
    var buf: [2]router.Route = undefined;
    // L = 16 task handlers, one task route: its share is 16 by default, so
    // an override of 2 binds first; the mark is 4. The client's share (2)
    // and the aggregate's (4) are never reached here.
    const routes = testTaskRoutes(1, &buf);
    buf[0].share = .{ .task_handlers = 2 };
    var rt = try RouteTable.init(gpa, routes, .{ 16, 1024, 16 });
    defer rt.deinit(gpa);
    var shares: Shares = unlimited;
    shares[@intFromEnum(Resource.task_handlers)] = .of(16);
    var t = try Table.init(gpa, 8, 9, .{ .shares = shares, .connection_start = Rules.unlimited.connection_start, .handshake_start = Rules.unlimited.handshake_start });
    defer t.deinit(gpa);
    const a: [16]u8 = .{ 0x20, 0x01, 0x0d, 0xb8, 0, 7, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1 };
    var b = a;
    b[7] = 2; // another /64 of the same /48
    var oa: [max_refs]Key = undefined;
    var ob: [max_refs]Key = undefined;
    const ka = keysFor(.{ .ip6 = a }, 48, &oa);
    const kb = keysFor(.{ .ip6 = b }, 48, &ob);
    const ca = ((try t.acquire(ka, 0, null)).charge).withRoute(rt.ref(routes, 0));
    const cb = ((try t.acquire(kb, 0, null)).charge).withRoute(rt.ref(routes, 0));
    // Each /64 takes one: the route holds its share, 2.
    try std.testing.expectEqual(Charge.Outcome.took, ca.add(.task_handlers, 1, 4));
    try std.testing.expectEqual(Charge.Outcome.took, cb.add(.task_handlers, 1, 4));
    // At the mark the route refuses the next, from /64 b: the /64 and the
    // aggregate it charged first are both undone.
    try std.testing.expectEqual(Charge.Outcome.route_refused, cb.add(.task_handlers, 1, 4));
    try std.testing.expectEqual(@as(usize, 1), t.lookup(kb[0]).?.count(.task_handlers));
    try std.testing.expectEqual(@as(usize, 2), t.lookup(kb[1]).?.count(.task_handlers));
    try std.testing.expectEqual(@as(usize, 2), rt.recordOf(0).?.count(.task_handlers));
    ca.sub(.task_handlers, 1);
    cb.sub(.task_handlers, 1);
    try std.testing.expect(rt.checkEmpty());
    t.release(ca);
    t.release(cb);
    try std.testing.expect(t.checkEmpty());
}

test "RouteTable: ref takes only the list the table was built from, not another of the same length" {
    const gpa = std.testing.allocator;
    var a: [3]router.Route = undefined;
    var b: [3]router.Route = undefined;
    const la = testTaskRoutes(2, &a);
    // The same length, another layout: the complete route first.
    const lb = testTaskRoutes(2, &b);
    std.mem.swap(router.Route, &b[0], &b[2]);
    var t = try RouteTable.init(gpa, la, .{ 16, 1024, 16 });
    defer t.deinit(gpa);
    try std.testing.expect(t.builtFrom(la));
    try std.testing.expect(!t.builtFrom(lb));
    // An equal copy is another list too: what is checked is the list itself.
    var c = a;
    try std.testing.expect(!t.builtFrom(&c));
    try std.testing.expectEqual(@as(u32, 1), t.ref(la, 1).index);
    // (`ref` with `lb`, or with a complete route of `la`, panics:
    // `shutdown_child route-table-mismatch`.)
}

test "Orphans.add: a full registry is one pass and a counted miss, and the missed orphan is left out of the age (t-3900)" {
    const gpa = std.testing.allocator;
    var o = try Orphans.init(gpa, 3);
    defer o.deinit(gpa);
    try std.testing.expectEqual(@as(?u32, 0), o.add(100, 0));
    try std.testing.expectEqual(@as(?u32, 1), o.add(200, 1));
    try std.testing.expectEqual(@as(?u32, 2), o.add(300, null));
    // Full. The new orphan is older than all three, so had it been counted
    // it would be the oldest.
    try std.testing.expectEqual(@as(?u32, null), o.add(50, 0));
    try std.testing.expectEqual(@as(u64, 1), o.misses.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 100), o.oldestStart(null));
    try std.testing.expectEqual(@as(u64, 100), o.oldestStart(0));
    try std.testing.expectEqual(@as(u64, 200), o.oldestStart(1));
    try std.testing.expectEqual(@as(usize, 3), o.inUse());
    // A freed slot is found again, and a miss is not repeated.
    o.remove(0);
    try std.testing.expectEqual(@as(?u32, 0), o.add(60, 0));
    try std.testing.expectEqual(@as(u64, 60), o.oldestStart(null));
    try std.testing.expectEqual(@as(u64, 1), o.misses.load(.monotonic));
    // No slots at all (a hand-built test accounting): null, and no miss.
    var none: Orphans = .{};
    try std.testing.expectEqual(@as(?u32, null), none.add(10, null));
    try std.testing.expectEqual(@as(u64, 0), none.misses.load(.monotonic));
}
