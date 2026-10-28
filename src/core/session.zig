//! Deterministic HTTP/2 connection state machine. No I/O, no clocks.
//!
//! `Session` is the protocol authority for one connection. It is the only place
//! that decides what HTTP/2 means, and the edge is the only place that touches
//! a socket or a clock. The split is the central design choice of this stack,
//! and it buys three things:
//!
//! - A protocol decision is reproducible. The same byte sequence produces the
//!   same intents on every run, so a conformance failure is a test case and not
//!   a timing story.
//! - No lock lives in here. `Connection` serializes every entry point behind
//!   `session_mu`, so `Session` may assume single-threaded access.
//! - The edge cannot invent protocol. It may only apply `AppCommand` values and
//!   execute `Intent` values.
//!
//! ## The flow
//!
//! Inbound:  wire bytes -> `ingest` -> `frame.Parser` -> `handleFrame`
//!           -> stream state -> `Intent`.
//! Outbound: handler -> `AppCommand` -> `applyCommand` -> `Intent`.
//!
//! `Session` never writes. It appends an `Intent`, and the edge drains the
//! intent ring and turns each entry into wire bytes or into a handler dispatch.
//! Two consequences follow, and both have cost real defects:
//!
//! - An intent is queued, not sent. A caller that needs bytes on the wire in a
//!   given ORDER must materialize the intents at the right moment. See the
//!   HEADERS-before-DATA note in `edge.connection.sendCb` (t-482).
//! - `edge_now_ns` is a value the edge stores before each call, and not a clock
//!   this module reads. Every deadline below compares against that stored
//!   value.
//!
//! ## Ownership
//!
//! An inbound `FrameEvent` is released by `handleFrame` through `deinit`: a
//! scratch borrow is a no-op there, a test `gpa` payload is freed there.
//! An `Intent` carries owned memory: a frame payload, or a decoded request.
//! A request's memory (its header list and its body) is, on the live edge, one
//! arena of server-wide pool blocks, borrowed through `StreamHooks`: the
//! decode and every DATA append go into it, the dispatch intent points into
//! it, and nothing in it is freed item by item. When the edge has none to give
//! (`HeaderListMem.scratch`) the stream is answered 500/503 and the request is
//! never dispatched. Only an isolated Session in a test (no `headerListMem`
//! hook) GPA-owns request memory item by item. `pushIntent`
//! transfers that ownership only when it returns success, so an
//! `error.PoolExhausted` leaves the caller still owning the payload. Whoever
//! drains an intent must release it with `releaseIntent` exactly once.
const std = @import("std");
const frame = @import("frame.zig");
const hpack = @import("hpack.zig");
const fields = @import("fields.zig");
const stream_mod = @import("stream.zig");
const flow = @import("flow.zig");
const limits_mod = @import("limits.zig");
const rates_mod = @import("rates.zig");
const boot_plan = @import("boot_plan.zig");

/// Work that the edge must perform. `Session` decides; the edge executes.
/// An intent never contains a callback or a pointer back into edge state, so
/// the protocol layer cannot reach into the actor and cannot be re-entered.
pub const Intent = union(enum) {
    outbound_frame: OutboundFrame,
    dispatch_request: DispatchRequest,
    stream_reset: struct { stream_id: u31, code: frame.ErrorCode, from_peer: bool = false },
    connection_error: struct { code: frame.ErrorCode, last_stream_id: u31 },
    connection_closed,
    /// Exactly one early HTTP response (431/414/413); no handler dispatch.
    early_reject: struct { stream_id: u31, status: u16 },
};

pub const OutboundFrame = struct {
    typ: frame.FrameType,
    flags: frame.FrameFlags = .{},
    stream_id: u31 = 0,
    payload: []u8, // session-owned; released after write
    /// The write ticket a handler waits on, carried by the frame it belongs
    /// to: only `respond_headers` sets it, on the first frame of its own
    /// block. The edge hands it to the scheduler with this frame, or fails it
    /// if the frame is refused.
    ticket: WireTicket = .none,
    /// Set on every final HEADERS (END_STREAM, `cmdHeaders`): if the edge
    /// refuses it, the stream is reset instead (`resetRefusedHeaders`). It is
    /// set whether or not the stream left the map when it was queued, because
    /// the peer can close its own side later in the same read (an early 413,
    /// then the peer's DATA or trailers with END_STREAM), and the stream then
    /// leaves the map before the edge takes the frame. Cleared while the frame
    /// is still queued by a peer RST for the stream (`retireUnwrittenRst`: the
    /// peer counts it closed) and by a server RST queued after it
    /// (`queueRst`: that RST already ends it, and a second one is not owed).
    /// The obligation lives on the frame, not in the tombstone ring, which a
    /// connection may keep empty and always overwrites.
    owes_rst_if_refused: bool = false,
};

/// A handler's write ticket and its table slot (`Connection.tickets`). `id`
/// 0 is no ticket.
pub const WireTicket = struct {
    id: u64,
    slot: u32,

    pub const none: WireTicket = .{ .id = 0, .slot = 0 };
};

pub const DispatchRequest = struct {
    stream_id: u31,
    method: []const u8,
    scheme: []const u8,
    authority: []const u8,
    path: []const u8,
    query: []const u8,
    headers: []hpack.HeaderField,
    body: []const u8,
    trailers: []hpack.HeaderField,
};

/// The handler-facing direction: everything an application may ask of the
/// connection. The set is closed on purpose. A handler cannot build a frame,
/// so it cannot violate the protocol, and every response path is auditable
/// from this one union.
pub const AppCommand = union(enum) {
    respond_headers: struct {
        stream_id: u31,
        status: u16,
        headers: []const hpack.HeaderField,
        end_stream: bool,
        /// The ticket the handler waits on for these HEADERS, or `.none`. It
        /// travels on the block's first frame, so it can only complete or
        /// fail with this response.
        ticket: WireTicket,
    },
    respond_data: struct {
        stream_id: u31,
        data: []const u8,
        end_stream: bool,
    },
    reset_stream: struct { stream_id: u31, code: frame.ErrorCode },
    goaway: struct { last_stream_id: u31, code: frame.ErrorCode },
    /// Start graceful shutdown phase 1: GOAWAY(max) + PING (edge waits for ACK/timeout).
    graceful_phase1,
    /// Phase 2: GOAWAY(highest accepted); refuse later streams.
    graceful_phase2,
};

pub const Terminal = union(enum) {
    none,
    goaway: struct { code: frame.ErrorCode, last_stream_id: u31 },
    transport,
};

const Tombstone = struct {
    id: u31,
    code: frame.ErrorCode,
};

fn encodeDataFrameInto(out: []u8, stream_id: u31, data: []const u8, end_stream: bool) usize {
    const total = frame.FRAME_HEADER_LEN + data.len;
    std.debug.assert(out.len >= total);
    const fh = frame.FrameHeader{
        .length = @intCast(data.len),
        .type = .data,
        .flags = .{ .end_stream = end_stream },
        .stream_id = stream_id,
    };
    fh.encode(out[0..frame.FRAME_HEADER_LEN]);
    if (data.len > 0) @memcpy(out[frame.FRAME_HEADER_LEN..][0..data.len], data);
    return total;
}

/// t-1002 probe hook. Core stays clock-free and I/O-free, so the print body
/// lives at the edge; a binary installs `edge.connection.closeProbeSessionPrint`
/// here. Fired on the HEADERS-on-closed-stream branches and on
/// `failConnectionWith`, so a full-suite h2spec run shows whether the Session
/// DECIDED to close, separately from whether the edge materialized the close.
pub const CloseProbeSite = enum { headers_on_tombstone, headers_on_closed, fail_connection, data_on_closed_ignored, headers_seen, hpack_decode_fail };
pub const CloseProbeEvent = struct {
    session: usize,
    site: CloseProbeSite,
    code: frame.ErrorCode,
    stream_id: u32,
};
pub var close_probe_fn: ?*const fn (ev: CloseProbeEvent) void = null;

/// Control frames made by `Session.controlFrame`, in every Session of the
/// process. The work counter that goes with "no general-allocator call": a zero
/// count of calls means nothing in a run that made no control frame. The bench
/// server prints it on `/mem`.
pub var control_frames_built: std.atomic.Value(u64) = .init(0);

/// Streams reset because their request body went idle, in every Session of the
/// process: the work counter for the idle-sweep list. The bench server prints it.
pub var body_idle_expiries: std.atomic.Value(u64) = .init(0);

/// What a frame is for, when the edge decides which buffer it gets.
///
/// An RST_STREAM and a graceful GOAWAY are `terminal`; the GOAWAY that ends
/// the connection is `final_goaway`. Neither is built in a frame slab: the
/// edge has a store of `Limits.terminalFrames` fixed slots for them, and
/// admits a `terminal` frame only while one slot, one terminal ring entry and
/// one pool entry stay free for the final GOAWAY. So the frame that ends a
/// flood is never refused for the room the flood already took.
///
/// A refused `terminal` frame means the peer holds more unwritten terminal
/// frames than a well-behaved peer can (`Limits.terminalFrames`); the session
/// answers it with `failConnection(.enhance_your_calm)`. A refused
/// `final_goaway` is a broken invariant.
pub const FrameClass = enum { ordinary, terminal, final_goaway };

/// Where a header block is decoded.
pub const HeaderListMem = union(enum) {
    /// The request's own memory, which lives as long as the request, and the
    /// scratch the decoder may use for the length of one decode (the slot's
    /// `hpack_scratch`; see `Decoder.decodeInto`).
    request: struct { alloc: std.mem.Allocator, scratch: std.mem.Allocator },
    /// The edge could not give the request any memory (the pool refused its
    /// first block, or it has no lease for it). The block is decoded here
    /// only so the HPACK table stays in step with the peer's, the decoded
    /// fields are read to validate the request and then forgotten, and the
    /// stream is answered with `status`. The edge resets this allocator at
    /// its next call, so the fields must not outlive the call that decoded
    /// them. The decoder's transient room comes from it too.
    scratch: struct { alloc: std.mem.Allocator, status: u16 },
};

/// What the connection owes the peer and could not queue yet: the frame
/// buffer was not there (`controlFrame`), or the edge refused the frame
/// because the ordinary control share was full (a peer that does not read).
/// RFC 9113 requires each of these: a SETTINGS ack (a peer may end the
/// connection with SETTINGS_TIMEOUT), a PING ack with the PING's payload (Go
/// and gRPC close a connection that misses one), and WINDOW_UPDATE credit (a
/// peer whose window is at zero sends no DATA, so nothing else would ever
/// make a new one). They are kept here, coalesced, and queued again by
/// `Session.requeueOwed`, which the edge calls once per materialization,
/// with the room its ordinary share has.
///
/// While anything of a kind is owed, a new one of that kind joins it instead
/// of going ahead, so the acks keep their order and the credit is one frame.
/// A SETTINGS ack is owed only after its SETTINGS was applied (`onSettings`
/// applies before it acks), so a later ack never runs ahead of its settings.
///
/// Bounded: past `rates.SETTINGS_BURST` SETTINGS acks or `rates.PING_BURST`
/// PING acks owed at once, the peer goes on sending past the burst its rate
/// limit allows while it reads nothing, and the connection ends with
/// ENHANCE_YOUR_CALM. The window credit needs no bound: it is a sum, and the
/// peer can only send what it was granted.
///
/// The graceful shutdown's PING is not here: it may be dropped, because its
/// one-second deadline moves the shutdown on without its ack.
pub const Owed = struct {
    settings_acks: usize = 0,
    /// PING ack payloads, oldest first, in a ring.
    pings: [rates_mod.PING_BURST][8]u8 = undefined,
    ping_head: usize = 0,
    ping_len: usize = 0,
    /// Connection credit granted (`Windows.conn_recv` holds it) and not yet
    /// sent. Each stream's is `Stream.window_owed`; `streams` says whether
    /// any stream may have some.
    conn_window: u31 = 0,
    streams: bool = false,

    pub fn any(self: *const Owed) bool {
        return self.settings_acks != 0 or self.ping_len != 0 or self.conn_window != 0 or self.streams;
    }
};

pub const Session = struct {
    /// Heap that outlives any one ingest or command.
    gpa: std.mem.Allocator,
    /// Where `init` took the tables that exist for the connection's whole life
    /// and never change size: the intent ring, the parser scratch, the stream
    /// and pending maps, the tombstone ring, the header block. On the live edge
    /// this is the connection slot's buffer, which is rewound when the slot is
    /// reused, so nothing may grow a table through `gpa` or through this
    /// allocator after `init` (`putBounded`, `appendHeaderBlock`, `addTombstone`
    /// each prove why they cannot). In a test it is the same allocator as `gpa`.
    boot: std.mem.Allocator,
    limits: limits_mod.Limits,
    parser: frame.Parser,
    decoder: hpack.Decoder,
    windows: flow.Windows = .{},
    streams: std.AutoHashMap(u31, stream_mod.Stream),
    /// `streams.remove` calls since the last `streams.rehash`. std's HashMap
    /// leaves a tombstone per remove and only clears them when it grows, and
    /// it never grows here: every request puts one fresh stream id and later
    /// removes it, so the size stays flat. The tombstones then fill every
    /// free slot, and a lookup of a new id probes the whole table before it
    /// misses. `ingest` rehashes once this reaches half the capacity.
    stream_removes: usize = 0,
    /// Streams whose send window grew since `Connection` last consumed this
    /// journal, for `FairScheduler.rearm`. Fixed at `init`
    /// (`max_streams_per_connection`); on overflow, or on a positive
    /// SETTINGS_INITIAL_WINDOW_SIZE change, `credit_all` covers every stream.
    /// A connection WINDOW_UPDATE is not recorded: connection credit never
    /// blocks a single stream in the scheduler.
    credit_ids: []u31 = &.{},
    /// Scratch for `checkIdleDeadlines`: the streams whose body went idle, one
    /// sweep at a time. `body_idle_ns` never holds more entries than there are
    /// streams (`putBounded`), so `max_streams_per_connection` ids always fit.
    stale_ids: []u31 = &.{},
    credit_len: usize = 0,
    credit_all: bool = false,
    tombstones: std.ArrayList(Tombstone),
    tombstone_next: usize = 0,
    /// Fixed-capacity intent ring — enqueue fails before ownership transfer when full.
    intent_storage: []Intent = &.{},
    intent_len: usize = 0,
    /// Slots reserved for GOAWAY / connection_error so PoolExhausted can still fail-close.
    /// A connection that fills its intent ring must still be able to say WHY it
    /// is closing. Without the reserve the failure path needs the resource it
    /// just ran out of, so the connection would die silently and the peer would
    /// see only a dropped socket.
    terminal_reserve: usize = limits_mod.TERMINAL_INTENT_RESERVE,
    highest_peer_stream: u31 = 0,
    last_processed_stream: u31 = 0,
    active_streams: usize = 0,
    peer_max_frame_size: u32 = frame.DEFAULT_MAX_FRAME_SIZE,
    peer_initial_window: i32 = flow.INITIAL_WINDOW,
    peer_header_table_size: u32 = 4096,
    received_peer_settings: bool = false,
    settings_acked: bool = false,
    sent_preface: bool = false,
    expect_continuation: ?u31 = null,
    header_block: std.ArrayList(u8) = .empty,
    continuation_count: usize = 0,
    header_end_stream: bool = false,
    terminal: Terminal = .none,
    request_storage_used: usize = 0,
    // Pending assembled requests awaiting dispatch (owned)
    pending_bodies: std.AutoHashMap(u31, std.ArrayList(u8)),
    pending_headers: std.AutoHashMap(u31, []hpack.HeaderField),
    pending_trailers: std.AutoHashMap(u31, []hpack.HeaderField),
    /// After graceful phase 2 (or protocol GOAWAY), refuse peer stream IDs above this.
    goaway_ceiling: ?u31 = null,
    peer_goaway: bool = false,
    grace_phase: enum { none, phase1, phase2 } = .none,
    grace_ping: [8]u8 = .{ 0x73, 0x68, 0x32, 0x67, 0x72, 0x61, 0x63, 0x65 }, // "sh2grace"
    grace_ping_acked: bool = false,
    /// Connection-level frames RFC 9113 requires and the edge could not queue
    /// yet (`Owed`).
    owed: Owed = .{},
    control_frames_since_data: usize = 0,
    /// Optional server-wide stream admission (global max_streams_per_server).
    /// A per-connection limit alone does not bound a server: N connections at
    /// their own limits still exceed any global budget. The hooks let the edge
    /// enforce the server-wide counters at the exact moment `Session` admits a
    /// stream or accepts request bytes, while `Session` stays free of any
    /// knowledge about other connections. `Session` calls `onRelease` exactly
    /// once per successful `tryAdmit`, including on the teardown path in
    /// `deinit`; a missed release leaks server capacity for the process
    /// lifetime.
    stream_hooks: ?StreamHooks = null,
    /// Edge-owned rate limiter + clock snapshot. Null → rates disabled.
    rate_limiter: ?*rates_mod.RateLimiter = null,
    edge_now_ns: u64 = 0,
    /// Wall-clock start of unfinished field block (0 = idle).
    field_block_started_ns: u64 = 0,
    /// Last request-body DATA progress per stream (0 = unset).
    body_idle_ns: std.AutoHashMap(u31, u64) = undefined,

    pub const StreamHooks = struct {
        ctx: *anyopaque,
        tryAdmit: *const fn (*anyopaque) bool,
        onRelease: *const fn (*anyopaque) void,
        /// Route body cap for this method and path. Null hook uses `Limits.request_body_bytes`.
        requestBodyCap: ?*const fn (*anyopaque, method: []const u8, path: []const u8) usize = null,
        /// Where one stream's decoded request list goes (not the dynamic
        /// table). Null means the request's memory is `Session.gpa`, which only
        /// an isolated Session in a test does: on the live edge the hook is
        /// always set, and request memory is never the general allocator.
        headerListMem: ?*const fn (*anyopaque, u31) HeaderListMem = null,
        /// The allocator of the request memory `stream_id` already has, which
        /// is true from the moment its headers were decoded: the body is
        /// appended into it.
        requestAlloc: ?*const fn (*anyopaque, u31) std.mem.Allocator = null,
        /// The status a stream gets when its request memory ran out (500 when
        /// the request itself hit its cap, 503 for the other caps). Null means
        /// 503.
        memoryRefusalStatus: ?*const fn (*anyopaque, u31) u16 = null,
        /// Give back `stream_id`'s request memory: a decode Session dropped, or
        /// a request that will never be dispatched.
        discardHeaderList: ?*const fn (*anyopaque, u31) void = null,
        /// Rent a boot-reserved outbound frame buffer. `n` is the on-wire size.
        /// Returns a full slab of whichever pool the edge picks for `n`
        /// (`len >= n`), or null when no pool has one: the frame is then
        /// refused with `OutOfMemory`. Never a used prefix.
        rentFrame: ?*const fn (*anyopaque, usize, FrameClass) ?[]u8 = null,
        /// Return a buffer from `rentFrame`. Pointer-range decides which
        /// pool; a prefix of a slab is reconstructed to the full lease.
        releaseFrame: ?*const fn (*anyopaque, []u8) void = null,
        /// Drop the unwritten RST_STREAM for this stream from the edge's
        /// terminal ring, if one waits there (`retireUnwrittenRst`).
        retireRst: ?*const fn (*anyopaque, u31) void = null,
    };

    /// Insert into one of the maps that `init` sized once, in the boot buffer.
    ///
    /// std grows a map when an insert of a NEW key finds `available == 0`, and
    /// `available` is the map's load limit minus its live entries; a remove
    /// gives its slot back at once, so tombstones never cause a grow. Growth is
    /// therefore reachable only with more live entries than `init` made room
    /// for, which is `max_streams_per_connection`. Every map here is keyed by
    /// a stream that is in `streams`, and `streams` takes a new key only below
    /// that limit (`finishHeaderBlock` checks `active_streams` first), and
    /// `releaseConcurrency` removes the stream from all of them together. A
    /// grow would reallocate through the boot allocator, which a connection
    /// slot rewinds, so a violation is a panic and not an allocation.
    fn putBounded(map: anytype, key: u31, value: anytype) void {
        if (map.unmanaged.available == 0 and !map.contains(key)) {
            @panic("session map insert past the capacity it was sized for");
        }
        map.putAssumeCapacity(key, value);
    }

    /// Append to the header block without growing it. `init` sized the block
    /// to the larger of one frame and `compressed_header_bytes`: the first
    /// fragment is at most one frame (the parser refuses a bigger one), and
    /// `consumeContinuation` rejects any fragment that would take the block
    /// past `compressed_header_bytes` before it calls this.
    fn appendHeaderBlock(self: *Session, bytes: []const u8) void {
        if (self.header_block.items.len + bytes.len > self.header_block.capacity) {
            @panic("header block append past the capacity it was sized for");
        }
        self.header_block.appendSliceAssumeCapacity(bytes);
    }

    fn routeBodyCap(self: *const Session, method: []const u8, path: []const u8) usize {
        const global = self.limits.request_body_bytes;
        if (self.stream_hooks) |h| {
            if (h.requestBodyCap) |f| return @min(f(h.ctx, method, path), global);
        }
        return global;
    }

    fn headerListMem(self: *Session, stream_id: u31) HeaderListMem {
        if (self.stream_hooks) |h| {
            if (h.headerListMem) |f| return f(h.ctx, stream_id);
        }
        return .{ .request = .{ .alloc = self.gpa, .scratch = self.gpa } };
    }

    /// True for an isolated Session: request memory (header lists and bodies)
    /// is then owned item by item and freed with `gpa`. On the live edge it
    /// belongs to the request's arena, and nothing in it is freed one by one.
    fn headerListIsGpa(self: *const Session) bool {
        if (self.stream_hooks) |h| return h.headerListMem == null;
        return true;
    }

    /// The allocator a body is appended to.
    fn bodyAllocator(self: *Session, stream_id: u31) std.mem.Allocator {
        if (self.stream_hooks) |h| {
            if (h.requestAlloc) |f| return f(h.ctx, stream_id);
        }
        return self.gpa;
    }

    fn memoryRefusalStatus(self: *Session, stream_id: u31) u16 {
        if (self.stream_hooks) |h| {
            if (h.memoryRefusalStatus) |f| return f(h.ctx, stream_id);
        }
        return 503;
    }

    fn discardHeaderList(self: *Session, stream_id: u31) void {
        if (self.stream_hooks) |h| {
            if (h.discardHeaderList) |f| f(h.ctx, stream_id);
        }
    }

    /// A buffer for one frame of `n` bytes on the wire. Where the edge has
    /// installed `rentFrame`, the frame comes from the connection's own pools
    /// and a refusal is `OutOfMemory`: there is no fallback to `gpa`, because
    /// a path with a bound must fail at the bound. A Session with no hook is an
    /// isolated one in a test, and its frames are `gpa` allocations.
    fn allocFrame(self: *Session, n: usize, class: FrameClass) error{OutOfMemory}![]u8 {
        if (self.stream_hooks) |h| {
            if (h.rentFrame) |f| return f(h.ctx, n, class) orelse error.OutOfMemory;
        }
        return self.gpa.alloc(u8, n);
    }

    /// A serialized control frame in a frame buffer, ready to become an
    /// intent. Every control frame goes through here: SETTINGS (the preface and
    /// the ack), PING, WINDOW_UPDATE, RST_STREAM and GOAWAY. The caller owns the
    /// result (`freeFrame`, or an intent that is released).
    fn controlFrame(self: *Session, bytes: []const u8, class: FrameClass) error{OutOfMemory}![]u8 {
        const out = try self.allocFrame(bytes.len, class);
        @memcpy(out[0..bytes.len], bytes);
        _ = control_frames_built.fetchAdd(1, .monotonic);
        return out;
    }

    fn freeFrame(self: *Session, buf: []u8) void {
        if (buf.len == 0) return;
        if (self.stream_hooks) |h| {
            if (h.releaseFrame) |f| {
                f(h.ctx, buf);
                return;
            }
        }
        self.gpa.free(buf);
    }

    /// `scratch`: the block was decoded into the edge's scratch, which owns
    /// nothing and is reset by itself.
    fn dropDecoded(self: *Session, stream_id: u31, decoded: hpack.Decoder.DecodeResult, scratch: bool) void {
        if (scratch) return;
        if (self.headerListIsGpa()) {
            self.decoder.freeResultInto(self.gpa, decoded);
            return;
        }
        self.discardHeaderList(stream_id);
    }

    fn dropPendingRequest(self: *Session, stream_id: u31) void {
        const had_headers = self.pending_headers.fetchRemove(stream_id);
        const had_trailers = self.pending_trailers.fetchRemove(stream_id);
        if (self.headerListIsGpa()) {
            if (had_headers) |e| {
                hpack.HeaderField.freeOwnedSlice(self.gpa, e.value);
                self.gpa.free(e.value);
            }
            if (had_trailers) |e| {
                hpack.HeaderField.freeOwnedSlice(self.gpa, e.value);
                if (e.value.len != 0) self.gpa.free(e.value);
            }
        } else if (had_headers != null or had_trailers != null) {
            self.discardHeaderList(stream_id);
        }
        if (self.pending_bodies.fetchRemove(stream_id)) |e| {
            var list = e.value;
            // On the edge the body is in the request's arena, which was
            // given back with the header list above (or is, by whoever holds
            // the lease).
            if (self.headerListIsGpa()) list.deinit(self.gpa);
        }
    }

    fn intentCapacityFor(limits: limits_mod.Limits) usize {
        return @max(limits.intent_entries_per_connection, 16);
    }

    /// The largest the header block can get. The first HEADERS fragment is at
    /// most one frame (the parser refuses a bigger frame), and
    /// `consumeContinuation` refuses to take the block past
    /// `compressed_header_bytes`. So the block never needs more than the
    /// larger of the two, and `appendHeaderBlock` never has to grow it.
    fn headerBlockCapacityFor(limits: limits_mod.Limits) usize {
        return @max(@as(usize, frame.DEFAULT_MAX_FRAME_SIZE), limits.compressed_header_bytes);
    }

    /// The boot allocations of `init`, in its order. See `boot_plan`.
    pub fn planBoot(plan: *boot_plan.Plan, limits: limits_mod.Limits) error{InvalidConfig}!void {
        const max_streams = limits.max_streams_per_connection;
        try plan.array(Intent, intentCapacityFor(limits));
        try plan.array(u8, frame.DEFAULT_MAX_FRAME_SIZE); // parser scratch
        try hpack.Decoder.planBoot(plan); // the dynamic table: bytes, then descriptors
        try plan.hashMap(u31, stream_mod.Stream, max_streams);
        try plan.array(u31, @max(max_streams, 1)); // credit_ids
        try plan.array(u31, @max(max_streams, 1)); // stale_ids
        try plan.array(Tombstone, limits.stream_tombstones);
        try plan.hashMap(u31, std.ArrayList(u8), max_streams);
        try plan.hashMap(u31, []hpack.HeaderField, max_streams);
        try plan.hashMap(u31, []hpack.HeaderField, max_streams);
        try plan.hashMap(u31, u64, max_streams);
        try plan.array(u8, headerBlockCapacityFor(limits));
    }

    /// `boot` holds the fixed tables (see the field); `gpa` holds everything a
    /// request or a response allocates, including the two preface frames
    /// queued here. On the live edge `boot` is a slot's buffer and `gpa` the
    /// server allocator.
    pub fn init(boot: std.mem.Allocator, gpa: std.mem.Allocator, limits: limits_mod.Limits) !Session {
        var s = try initBare(boot, gpa, limits);
        errdefer s.deinit();
        try s.emitServerPreface();
        return s;
    }

    /// `init` without the server preface. The edge uses this one: the preface
    /// frames come from the connection's frame pool, so they can only be made
    /// once `stream_hooks` is installed, and it calls `emitServerPreface` then.
    /// RFC 9113 requires the server's SETTINGS to be the first frame it sends,
    /// and the only way to guarantee that is to make it the first intent that
    /// can possibly exist, so nothing may be queued between `initBare` and
    /// `emitServerPreface` (see `edge.fair_scheduler.classifyControl` for the
    /// ordering defect that appeared when a later ack could overtake it, t-538).
    pub fn initBare(boot: std.mem.Allocator, gpa: std.mem.Allocator, limits: limits_mod.Limits) !Session {
        const intent_storage = try boot.alloc(Intent, intentCapacityFor(limits));
        var intent_storage_owned = true;
        errdefer if (intent_storage_owned) boot.free(intent_storage);
        var parser = try frame.Parser.initReserved(boot, frame.DEFAULT_MAX_FRAME_SIZE);
        var parser_owned = true;
        errdefer if (parser_owned) parser.deinit();
        var decoder = try hpack.Decoder.init(boot);
        var decoder_owned = true;
        errdefer if (decoder_owned) decoder.deinit();
        var s: Session = .{
            .gpa = gpa,
            .boot = boot,
            .limits = limits,
            .parser = parser,
            .decoder = decoder,
            .streams = std.AutoHashMap(u31, stream_mod.Stream).init(boot),
            .tombstones = .empty,
            .intent_storage = intent_storage,
            .intent_len = 0,
            .terminal_reserve = limits_mod.TERMINAL_INTENT_RESERVE,
            .pending_bodies = std.AutoHashMap(u31, std.ArrayList(u8)).init(boot),
            .pending_headers = std.AutoHashMap(u31, []hpack.HeaderField).init(boot),
            .pending_trailers = std.AutoHashMap(u31, []hpack.HeaderField).init(boot),
            .body_idle_ns = std.AutoHashMap(u31, u64).init(boot),
        };
        intent_storage_owned = false;
        parser_owned = false;
        decoder_owned = false;
        errdefer s.deinit();
        try s.streams.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        s.credit_ids = try boot.alloc(u31, @max(limits.max_streams_per_connection, 1));
        s.stale_ids = try boot.alloc(u31, @max(limits.max_streams_per_connection, 1));
        try s.tombstones.ensureTotalCapacityPrecise(boot, limits.stream_tombstones);
        try s.pending_bodies.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.pending_headers.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.pending_trailers.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.body_idle_ns.ensureTotalCapacity(@intCast(limits.max_streams_per_connection));
        try s.header_block.ensureTotalCapacityPrecise(boot, headerBlockCapacityFor(limits));
        return s;
    }

    /// Give back everything this session took from `gpa`, and the admissions
    /// it still holds, but none of the fixed tables from `boot`. A connection
    /// slot calls this and then rewinds its buffer, so after this returns the
    /// session must not be used again; `deinit` is this plus the boot frees.
    pub fn releaseRuntime(self: *Session) void {
        // Connection teardown must release every still-admitted stream exactly once.
        var sit = self.streams.iterator();
        while (sit.next()) |e| {
            if (!e.value_ptr.concurrency_released) {
                e.value_ptr.concurrency_released = true;
                if (self.active_streams > 0) self.active_streams -= 1;
                if (self.stream_hooks) |h| h.onRelease(h.ctx);
            }
        }
        self.clearIntents();
        var bit = self.pending_bodies.iterator();
        while (bit.next()) |e| {
            if (self.headerListIsGpa()) e.value_ptr.deinit(self.gpa);
        }
        var hit = self.pending_headers.iterator();
        while (hit.next()) |e| {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, e.value_ptr.*);
                self.gpa.free(e.value_ptr.*);
            }
        }
        var tit = self.pending_trailers.iterator();
        while (tit.next()) |e| {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, e.value_ptr.*);
                self.gpa.free(e.value_ptr.*);
            }
        }
        // Forget the entries just released, so a second pass could not free
        // them again.
        self.pending_bodies.clearRetainingCapacity();
        self.pending_headers.clearRetainingCapacity();
        self.pending_trailers.clearRetainingCapacity();
        self.streams.clearRetainingCapacity();
    }

    pub fn deinit(self: *Session) void {
        self.releaseRuntime();
        self.freeBoot();
    }

    /// Free the fixed tables one at a time, for a session that was not booted
    /// into a slot. `releaseRuntime` has run. The HPACK table is one of them.
    pub fn freeBoot(self: *Session) void {
        self.decoder.deinit();
        self.parser.deinit();
        if (self.intent_storage.len != 0) self.boot.free(self.intent_storage);
        if (self.stale_ids.len != 0) self.boot.free(self.stale_ids);
        if (self.credit_ids.len != 0) self.boot.free(self.credit_ids);
        self.tombstones.deinit(self.boot);
        self.header_block.deinit(self.boot);
        self.pending_bodies.deinit();
        self.pending_headers.deinit();
        self.pending_trailers.deinit();
        self.body_idle_ns.deinit();
        self.streams.deinit();
    }

    fn clearIntents(self: *Session) void {
        var i: usize = 0;
        while (i < self.intent_len) : (i += 1) {
            self.releaseIntent(&self.intent_storage[i]);
        }
        self.intent_len = 0;
    }

    pub fn releaseIntent(self: *Session, it: *Intent) void {
        switch (it.*) {
            .outbound_frame => |*f| self.freeFrame(f.payload),
            .dispatch_request => |*d| {
                if (self.headerListIsGpa()) {
                    hpack.HeaderField.freeOwnedSlice(self.gpa, d.headers);
                    self.gpa.free(d.headers);
                    hpack.HeaderField.freeOwnedSlice(self.gpa, d.trailers);
                    if (d.trailers.len != 0) self.gpa.free(d.trailers);
                } else {
                    self.discardHeaderList(d.stream_id);
                }
                if (self.headerListIsGpa()) self.gpa.free(d.body);
            },
            else => {},
        }
    }

    fn intentIsTerminal(intent: Intent) bool {
        return switch (intent) {
            .outbound_frame => |f| f.typ == .goaway,
            .connection_error, .connection_closed => true,
            else => false,
        };
    }

    /// Enqueue before ownership transfer completes — on PoolExhausted caller still owns payloads.
    pub fn pushIntent(self: *Session, intent: Intent) error{PoolExhausted}!void {
        const limit = if (intentIsTerminal(intent))
            self.intent_storage.len
        else
            self.intent_storage.len -| self.terminal_reserve;
        if (self.intent_len >= limit) return error.PoolExhausted;
        self.intent_storage[self.intent_len] = intent;
        self.intent_len += 1;
    }

    /// Free intent slots for an ordinary (non-terminal) intent.
    fn ordinaryIntentRoom(self: *const Session) usize {
        return self.intent_storage.len -| self.terminal_reserve -| self.intent_len;
    }

    fn pushIntentOrFailClosed(self: *Session, intent: Intent) !void {
        self.pushIntent(intent) catch {
            self.releaseIntent(@constCast(&intent));
            try self.failClosedInternal();
            return error.PoolExhausted;
        };
    }

    /// Last resort when an intent cannot be queued. The connection is already
    /// unable to express itself, so this path may not allocate a new intent
    /// slot through the normal route — it spends the terminal reserve directly
    /// and must never recurse through `pushIntentOrFailClosed`.
    ///
    /// Every step degrades rather than gives up: if even the reserved GOAWAY
    /// cannot be queued, `terminal` is still set, so the actor starts its
    /// closing phase, which has nothing to wait for, and the edge tears the
    /// connection down. The rule is that a broken connection
    /// closes; it never continues in an unknown protocol state.
    pub fn failClosedInternal(self: *Session) !void {
        if (self.terminal != .none) return;
        // Use reserved terminal capacity — must not recurse through pushIntentOrFailClosed.
        const last = self.last_processed_stream;
        var buf: [17]u8 = undefined;
        const n = try frame.Serializer.goaway(&buf, last, .internal_error, &.{});
        const p = self.controlFrame(buf[0..n], .final_goaway) catch return error.OutOfMemory;
        self.pushIntent(.{ .outbound_frame = .{ .typ = .goaway, .payload = p } }) catch {
            self.freeFrame(p);
            self.terminal = .{ .goaway = .{ .code = .internal_error, .last_stream_id = last } };
            return;
        };
        self.pushIntent(.{ .connection_error = .{ .code = .internal_error, .last_stream_id = last } }) catch {
            self.terminal = .{ .goaway = .{ .code = .internal_error, .last_stream_id = last } };
            return;
        };
        self.terminal = .{ .goaway = .{ .code = .internal_error, .last_stream_id = last } };
    }

    /// Copy pending intents into caller-owned batch (same capacity). Never drops silently.
    pub fn drainIntentsInto(self: *Session, out: []Intent) usize {
        const n = self.intent_len;
        std.debug.assert(n <= out.len);
        if (n != 0) @memcpy(out[0..n], self.intent_storage[0..n]);
        self.intent_len = 0;
        return n;
    }

    /// Test/compat: drain into storage view. Do not pushIntent until finished with the slice.
    pub fn drainIntents(self: *Session) []Intent {
        const n = self.intent_len;
        self.intent_len = 0;
        return self.intent_storage[0..n];
    }

    pub fn releaseDrainedIntents(_: *Session, _: []Intent) void {}

    pub fn intentCapacity(self: *const Session) usize {
        return self.intent_storage.len;
    }

    pub fn intentLen(self: *const Session) usize {
        return self.intent_len;
    }

    /// Build framed DATA without touching the intent queue (avoids nested drain).
    ///
    /// The scheduler calls this while it drains, so the bytes must NOT become
    /// an intent: the edge would then drain an intent ring that it is already
    /// inside. Flow-control credit is debited here, at the moment the frame
    /// exists, so the debit and the frame cannot separate. A caller that gets
    /// `error.FlowBlocked` has caused no debit and may retry the same bytes
    /// unchanged after a WINDOW_UPDATE.
    pub fn makeDataFrame(self: *Session, stream_id: u31, data: []const u8, end_stream: bool) ![]u8 {
        const s = self.streams.getPtr(stream_id) orelse {
            // Scheduler still holds the body after releaseConcurrency dropped
            // the map entry. Treating that as StreamClosed/zero credit makes
            // emitOneData skip forever and the actor parks until slow-consumer.
            // A non-no_error tombstone is a RST: do not emit DATA after it.
            if (data.len > 16 * 1024) return error.FlowBlocked;
            if (data.len > 0 and self.windows.conn_send <= 0) return error.FlowBlocked;
            if (self.tombstoneCode(stream_id)) |code| {
                if (code != .no_error) return error.FlowBlocked;
            }
            const out = try self.allocFrame(frame.FRAME_HEADER_LEN + data.len, .ordinary);
            _ = encodeDataFrameInto(out, stream_id, data, end_stream);
            if (data.len > 0) self.windows.conn_send -= @intCast(data.len);
            return out;
        };
        const avail = s.window.availableSend(self.windows.conn_send);
        if (data.len == 0) {
            if (!end_stream) return error.FlowBlocked;
        } else if (avail <= 0) {
            return error.FlowBlocked;
        }
        const take: usize = if (data.len == 0)
            0
        else
            @min(data.len, @as(usize, @intCast(avail)), 16 * 1024);
        if (data.len > 0 and take < data.len) return error.FlowBlocked;
        const out = try self.allocFrame(frame.FRAME_HEADER_LEN + take, .ordinary);
        _ = encodeDataFrameInto(out, stream_id, data[0..take], end_stream);
        if (take > 0) {
            s.window.send -= @intCast(take);
            self.windows.conn_send -= @intCast(take);
        }
        if (end_stream) {
            s.localEndStream();
            self.releaseConcurrency(stream_id, .no_error);
        }
        return out;
    }

    /// Queue the server preface: SETTINGS, then the connection WINDOW_UPDATE.
    /// Once per session, before anything else is queued (see `initBare`).
    pub fn emitServerPreface(self: *Session) !void {
        std.debug.assert(!self.sent_preface and self.intent_len == 0);
        var buf: [256]u8 = undefined;
        const settings = frame.serverPrefaceSettings(@intCast(self.limits.max_streams_per_connection));
        const n = try frame.Serializer.settingsFrame(&buf, false, &settings);
        const payload = try self.controlFrame(buf[0..n], .ordinary);
        // Store as raw bytes frame — edge writes verbatim. Use typ=settings with full frame in payload.
        // Simpler: outbound is complete wire bytes in a synthetic frame.
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{
            .typ = .settings,
            .payload = payload,
            .stream_id = 0,
            .flags = .{},
        } });
        // Connection window raise to 4MiB
        const incr: u31 = @intCast(flow.CONNECTION_RECV_TARGET - flow.INITIAL_WINDOW);
        var wbuf: [13]u8 = undefined;
        const wn = try frame.Serializer.windowUpdate(&wbuf, 0, incr);
        const wp = try self.controlFrame(wbuf[0..wn], .ordinary);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{
            .typ = .window_update,
            .payload = wp,
            .stream_id = 0,
        } });
        self.windows.conn_recv = flow.CONNECTION_RECV_TARGET;
        self.sent_preface = true;
    }

    /// Ingest the whole of `chunk`, for a caller that drains the intents only
    /// at the end. A chunk whose frames would fill the intent queue is
    /// `error.IntentQueueFull` here, not a dropped tail; the edge uses
    /// `ingestSome`, which stops and lets it drain.
    pub fn ingest(self: *Session, chunk: []const u8) !void {
        if (try self.ingestSome(chunk) != chunk.len) return error.IntentQueueFull;
    }

    /// Ingest frames from `chunk` and return how many bytes were taken.
    ///
    /// Fewer than `chunk.len` means the intent queue has less free room than
    /// one frame can need (`limits.INGEST_FRAME_ROOM`), and the rest of the
    /// chunk starts at a frame boundary: the caller materializes the intents,
    /// then calls again with the rest. So no read chunk fills the queue,
    /// whatever frames it holds; without the stop, a chunk of HEADERS and
    /// RST_STREAM pairs (a rapid reset under the rate limit) or of refused
    /// streams queued more intents than the queue holds, and the connection
    /// of a peer within every limit failed closed or got ENHANCE_YOUR_CALM.
    /// A partial frame at the end is kept by the parser and counts as taken.
    /// Once the connection is ending the rest of the chunk is dropped and
    /// counts as taken too.
    pub fn ingestSome(self: *Session, chunk: []const u8) !usize {
        if (self.terminal != .none) return chunk.len;
        // Rehash moves entries, so it runs here, before any `*Stream` from
        // `streams.getPtr` exists. O(capacity) once per capacity/2 removes.
        if (self.stream_removes * 2 >= self.streams.capacity()) {
            self.streams.rehash();
            self.stream_removes = 0;
        }
        var remaining = chunk;
        while (remaining.len > 0) {
            if (self.ordinaryIntentRoom() < limits_mod.INGEST_FRAME_ROOM) return chunk.len - remaining.len;
            const queued = self.intent_len;
            const maybe = self.parser.ingestOne(remaining) catch |err| {
                const code: frame.ErrorCode = switch (err) {
                    error.ProtocolError => .protocol_error,
                    error.FrameSizeError => .frame_size_error,
                    error.EnhanceYourCalm => .enhance_your_calm,
                    error.NeedMore => unreachable,
                };
                try self.failConnection(code);
                return chunk.len;
            };
            const res = maybe orelse return chunk.len;
            try self.handleFrame(res.event);
            // Saturating: a crossed peer RST can remove a queued RST (`retireUnwrittenRst`).
            std.debug.assert(self.intent_len -| queued <= limits_mod.INTENTS_PER_FRAME_MAX);
            remaining = remaining[res.consumed..];
            // The connection is ending and its GOAWAY is queued. The rest
            // of the chunk would only add intents (a flood of refused
            // streams would fill the intent queue before the GOAWAY
            // could be written).
            if (self.terminal != .none) return chunk.len;
        }
        return chunk.len;
    }

    pub fn applyCommand(self: *Session, cmd: AppCommand) !void {
        if (self.terminal != .none) return;
        switch (cmd) {
            .respond_headers => |r| try self.cmdHeaders(r.stream_id, r.status, r.headers, r.end_stream, r.ticket),
            .respond_data => |r| try self.cmdData(r.stream_id, r.data, r.end_stream),
            .reset_stream => |r| try self.emitRst(r.stream_id, r.code),
            .goaway => |g| try self.failConnectionWith(g.code, g.last_stream_id),
            .graceful_phase1 => try self.beginGracefulPhase1(),
            .graceful_phase2 => try self.beginGracefulPhase2(),
        }
    }

    /// A graceful GOAWAY (phase 1 or 2). The edge refuses it only when the peer
    /// has too many unwritten terminal frames, and then the connection ends
    /// with ENHANCE_YOUR_CALM instead.
    fn emitGoawayNoTerminal(self: *Session, last: u31, code: frame.ErrorCode) !void {
        var buf: [32]u8 = undefined;
        const n = try frame.Serializer.goaway(&buf, last, code, &.{});
        const p = self.controlFrame(buf[0..n], .terminal) catch |err| switch (err) {
            error.OutOfMemory => {
                try self.failConnection(.enhance_your_calm);
                return;
            },
        };
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .goaway, .payload = p } });
    }

    fn beginGracefulPhase1(self: *Session) !void {
        if (self.grace_phase != .none or self.terminal != .none) return;
        // Graceful shutdown: GOAWAY NO_ERROR with last-stream-ID 2^31-1, then PING.
        try self.emitGoawayNoTerminal(std.math.maxInt(u31), .no_error);
        // The GOAWAY may have ended the connection instead (flood).
        if (self.terminal != .none) return;
        var buf: [17]u8 = undefined;
        const n = try frame.Serializer.ping(&buf, false, &self.grace_ping);
        // The PING may be dropped (no frame buffer here, or the edge refuses
        // it): phase 1 still ends at its deadline, without the ack.
        if (self.controlFrame(buf[0..n], .ordinary)) |p| {
            try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .ping, .payload = p } });
        } else |err| switch (err) {
            error.OutOfMemory => {},
        }
        self.grace_phase = .phase1;
        self.grace_ping_acked = false;
    }

    /// At most once: phase 2 is one of the three GOAWAYs `Limits.terminalFrames`
    /// counts.
    fn beginGracefulPhase2(self: *Session) !void {
        if (self.terminal != .none or self.grace_phase == .phase2) return;
        const last = self.highest_peer_stream;
        try self.emitGoawayNoTerminal(last, .no_error);
        if (self.terminal != .none) return;
        self.goaway_ceiling = last;
        self.grace_phase = .phase2;
    }

    pub fn gracePingAcked(self: *const Session) bool {
        return self.grace_ping_acked;
    }

    /// The peer closed its side of the transport (a TCP EOF, or TLS
    /// close_notify): it will send nothing more, so no request can finish and
    /// no WINDOW_UPDATE can come. What is already queued still goes out (the
    /// peer may still be reading), then every stream still open ends with
    /// RST_STREAM(CANCEL), then the connection with GOAWAY(NO_ERROR) naming
    /// the last stream processed. A NO_ERROR GOAWAY tells the peer the streams
    /// at or below it were handled, so none may be left open behind it; the
    /// graceful finish ends its streams the same way. At most one RST per
    /// open stream: M, which the terminal bound and the intent queue hold
    /// (`Limits.terminalFrames`, `intentEntriesMin`).
    pub fn peerClosed(self: *Session) !void {
        // The ids first: a reset takes its stream out of the map.
        var n: usize = 0;
        var it = self.streams.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.state == .closed or e.value_ptr.state == .idle) continue;
            self.stale_ids[n] = e.key_ptr.*;
            n += 1;
        }
        for (self.stale_ids[0..n]) |sid| try self.emitRst(sid, .cancel);
        try self.failConnectionWith(.no_error, self.last_processed_stream);
    }

    fn failConnection(self: *Session, code: frame.ErrorCode) !void {
        try self.failConnectionWith(code, self.last_processed_stream);
    }

    /// The GOAWAY that ends the connection, at most once: a second failure in
    /// the same read chunk finds `terminal` set and queues nothing, because the
    /// first GOAWAY already said why. This is the third of the GOAWAYs
    /// `Limits.terminalFrames` counts, and the edge keeps a slot, a ring entry
    /// and a pool entry free for it (`FrameClass.final_goaway`).
    fn failConnectionWith(self: *Session, code: frame.ErrorCode, last: u31) !void {
        if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .fail_connection, .code = code, .stream_id = last });
        if (self.terminal != .none) return;
        var buf: [32]u8 = undefined;
        const n = try frame.Serializer.goaway(&buf, last, code, &.{});
        const p = try self.controlFrame(buf[0..n], .final_goaway);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .goaway, .payload = p } });
        try self.pushIntentOrFailClosed(.{ .connection_error = .{ .code = code, .last_stream_id = last } });
        self.terminal = .{ .goaway = .{ .code = code, .last_stream_id = last } };
    }

    /// Return the stream's concurrency credit and drop its map entry.
    ///
    /// The map entry cannot live for the whole connection, or a long-lived
    /// connection accumulates one entry per request it ever served. A dropped
    /// entry, however, makes a late frame for that stream look like a frame for
    /// an IDLE stream, and the protocol treats those two cases very
    /// differently. The bounded tombstone list closes that gap: it remembers
    /// that the id was used and how it ended, at 8 bytes per entry instead of a
    /// whole `Stream`.
    ///
    /// `concurrency_released` makes the release idempotent. Several paths can
    /// reach a closed stream — a peer RST, a local RST, END_STREAM in both
    /// directions — and a double release would corrupt the server-wide stream
    /// count.
    fn releaseConcurrency(self: *Session, stream_id: u31, tombstone_code: frame.ErrorCode) void {
        const s = self.streams.getPtr(stream_id) orelse return;
        if (s.concurrency_released) return;
        if (s.state != .closed) return;
        s.concurrency_released = true;
        if (self.active_streams > 0) self.active_streams -= 1;
        if (self.stream_hooks) |h| h.onRelease(h.ctx);
        // Drop map entry; late frames classify via bounded tombstones.
        // The live-stream entry proves this ID has not yet been tombstoned:
        // stream IDs cannot be reused, and every close removes the entry below.
        // Passing the terminal code here avoids a linear duplicate scan on
        // every normal response while preserving the peer/local RST code.
        self.addTombstone(stream_id, tombstone_code) catch {};
        self.dropPendingRequest(stream_id);
        // A stream that ends mid-body (RST, early reject) leaves its idle
        // clock behind otherwise, and a peer that opens and resets streams
        // would fill a map that is sized for the live ones (`putBounded`).
        _ = self.body_idle_ns.remove(stream_id);
        _ = self.streams.remove(stream_id);
        self.stream_removes += 1;
    }

    /// Whether an RST_STREAM for `stream_id` still has something to tell the
    /// peer: the peer counts the stream open, no RST for it is queued yet, and
    /// the connection is not already ending.
    ///
    /// This is what makes `Limits.terminalFrames` true. Every RST that waits
    /// unwritten is for a DISTINCT stream the peer cannot have closed, because
    /// the RST is what closes it, so a well-behaved peer (at most
    /// `max_streams_per_connection` streams open in its view) never has more
    /// than M of them. The cases:
    /// - A stream in the map is open for the peer unless it is `.closed`.
    /// - A stream that left the map ended. Only the caller can say that the
    ///   peer still counts it open: a NEW id it is refusing before it was
    ///   admitted (`target` = `.new`), or a stream whose final HEADERS the
    ///   edge refused (`.refused_end`, from that frame's obligation). Any other
    ///   absent id is closed for the peer, tombstone or not (a delayed reset of
    ///   a stream that ended long ago). The RST of a refusal tombstones the id,
    ///   and a second refusal of it is a connection error in `onHeaders`.
    /// - Once `terminal` is set the GOAWAY already covers every stream, and
    ///   more RSTs would only use the room the final GOAWAY was kept.
    fn rstOwed(self: *Session, stream_id: u31, target: RstTarget) bool {
        if (self.terminal != .none) return false;
        if (self.streams.getPtr(stream_id)) |s| return s.state != .closed;
        return switch (target) {
            .known => false,
            // The same new id refused twice queues one RST.
            .new => !self.isTombstoned(stream_id),
            .refused_end => true,
        };
    }

    /// What the caller of `queueRst` knows about the stream it resets.
    const RstTarget = enum {
        /// A stream this connection admitted: in the map, or ended.
        known,
        /// A new id the peer just opened, refused before it was admitted.
        new,
        /// A stream that ended with a final HEADERS the edge then refused.
        refused_end,
    };

    /// The edge refused a HEADERS frame for `stream_id` (and dropped the rest
    /// of its block): the peer never got the response's start. A stream still
    /// in the map (a response with a body, or a final HEADERS while the
    /// peer's side is still open, which an early 413 is) is reset the usual
    /// way. One that left the map ended with that HEADERS, or after it (the
    /// peer's END_STREAM in the same read), and is reset only if the frame
    /// still owed it (`OutboundFrame.owes_rst_if_refused`): not if the peer
    /// reset the stream, or the server queued an RST for it, while the frame
    /// was queued.
    pub fn resetRefusedHeaders(self: *Session, stream_id: u31, owes_rst: bool) !void {
        if (self.streams.contains(stream_id)) return self.emitRst(stream_id, .internal_error);
        if (owes_rst) return self.queueRst(stream_id, .internal_error, .refused_end);
    }

    /// Queue an RST_STREAM at most once per stream (`rstOwed`), and always run
    /// the local half: the `stream_reset` intent (scheduler cleanup, waking the
    /// handler) and the concurrency release happen whether or not a frame was
    /// queued, because the slow-consumer sweep and the handler paths need them
    /// for a stream that is already closed on the wire.
    ///
    /// A frame the edge refuses (`controlFrame` OutOfMemory) means the peer
    /// holds more unwritten terminal frames than a well-behaved peer can
    /// (`Limits.terminalFrames`): the connection ends with ENHANCE_YOUR_CALM.
    fn emitRst(self: *Session, stream_id: u31, code: frame.ErrorCode) !void {
        return self.queueRst(stream_id, code, .known);
    }

    /// Refuse a stream the peer just opened, before it is admitted: there is
    /// no map entry and no tombstone, and the RST is owed (`rstOwed`).
    fn refuseStream(self: *Session, stream_id: u31) !void {
        return self.queueRst(stream_id, .refused_stream, .new);
    }

    fn queueRst(self: *Session, stream_id: u31, code: frame.ErrorCode, target: RstTarget) !void {
        // An RST takes two intents (the frame and the local reset). The
        // connection error that ends a flood takes the two terminal intents
        // (GOAWAY and connection_error, which sit in the reserve and move the
        // ordinary limit's room down by two) and then the local reset, so it
        // needs 3 of the room that is left: below `RST_INTENT_ROOM` an RST
        // would leave less than that. The queue holds every batch a
        // well-behaved peer can cause (`Limits.intentEntriesMin`), and a read
        // chunk stops before it fills it (`ingestSome`), so a queue this full
        // means a peer past the bound, answered like any other flood.
        var queued = false;
        if (self.ordinaryIntentRoom() < limits_mod.RST_INTENT_ROOM) {
            if (self.rstOwed(stream_id, target)) try self.failConnection(.enhance_your_calm);
        } else if (self.rstOwed(stream_id, target)) {
            var buf: [13]u8 = undefined;
            const n = try frame.Serializer.rstStream(&buf, stream_id, code);
            if (self.controlFrame(buf[0..n], .terminal)) |p| {
                try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .rst_stream, .payload = p, .stream_id = stream_id } });
                queued = true;
                // This RST ends the stream for the peer. A final HEADERS
                // still queued ahead of it owes nothing more if refused.
                self.clearRefusalObligation(stream_id);
            } else |err| switch (err) {
                error.OutOfMemory => try self.failConnection(.enhance_your_calm),
            }
        }
        try self.pushIntentOrFailClosed(.{ .stream_reset = .{ .stream_id = stream_id, .code = code, .from_peer = false } });
        if (self.streams.getPtr(stream_id)) |s| {
            s.onRst();
            self.releaseConcurrency(stream_id, code);
        } else if (queued) {
            if (self.tombstonePtr(stream_id)) |t| {
                // A refused final HEADERS: the RST now ends the stream.
                t.code = code;
            } else {
                // A refused stream never had a map entry: the tombstone is
                // what makes a second refusal of the same id find it.
                try self.addTombstone(stream_id, code);
            }
        }
    }

    /// Oldest-first ring overwrite, because a peer decides how many streams it opens.
    /// An unbounded list would grow with the connection's lifetime, which is a
    /// memory target. Losing the oldest tombstone is safe: a frame for a stream
    /// that closed that long ago falls back to the generic closed-stream rules.
    fn addTombstone(self: *Session, id: u31, code: frame.ErrorCode) !void {
        const capacity = self.limits.stream_tombstones;
        if (capacity == 0) return;
        if (self.tombstones.items.len < capacity) {
            // `init` sized the list to exactly `stream_tombstones`, so this
            // append cannot allocate: a table in the boot buffer never grows.
            self.tombstones.appendAssumeCapacity(.{ .id = id, .code = code });
            return;
        }
        self.tombstones.items[self.tombstone_next] = .{ .id = id, .code = code };
        self.tombstone_next = (self.tombstone_next + 1) % capacity;
    }

    /// One frame, fully validated, in the order the RFC requires the checks.
    ///
    /// The three gates below run BEFORE the type switch, and the order matters:
    ///
    /// 1. The peer's first frame must be a non-ack SETTINGS. Anything else means
    ///    the peer is not speaking HTTP/2, so no later state is trustworthy.
    /// 2. An unfinished field block accepts only a CONTINUATION for the same
    ///    stream. HPACK state spans the fragments, so any other frame in the
    ///    gap would leave the decoder unable to read the rest of the connection.
    /// 3. `already_terminal` is captured first so that a protocol error wins
    ///    over a rate-limit error raised by the same frame. The peer must learn
    ///    the more specific reason.
    fn handleFrame(self: *Session, ev: frame.FrameEvent) !void {
        defer ev.deinit(self.gpa);
        const hdr = ev.header;
        const payload = ev.payload.bytes();
        const already_terminal = self.terminal != .none;

        if (!self.received_peer_settings) {
            if (hdr.type != .settings or hdr.flags.ack()) {
                try self.failConnection(.protocol_error);
                return;
            }
            self.received_peer_settings = true;
        }

        if (self.expect_continuation) |cid| {
            if (hdr.type != .continuation or hdr.stream_id != cid) {
                try self.failConnection(.protocol_error);
                return;
            }
            try self.consumeContinuation(hdr, payload);
            try self.chargeRate(hdr.type, already_terminal);
            return;
        }

        switch (hdr.type) {
            .data => try self.onData(hdr, payload),
            .headers => try self.onHeaders(hdr, payload),
            .priority => {
                if (hdr.stream_id == 0 or hdr.length != 5) {
                    try self.failConnection(if (hdr.length != 5) .frame_size_error else .protocol_error);
                }
            },
            .rst_stream => {
                if (hdr.stream_id == 0 or hdr.length != 4) {
                    try self.failConnection(if (hdr.length != 4) .frame_size_error else .protocol_error);
                    return;
                }
                const code: frame.ErrorCode = @enumFromInt(std.mem.readInt(u32, payload[0..4], .big));
                if (self.streams.getPtr(hdr.stream_id)) |s| {
                    s.onRst();
                    self.releaseConcurrency(hdr.stream_id, code);
                    // Cancel handler; never RST in response to peer RST.
                    // A reset answered by a reset is an infinite exchange
                    // between two conforming peers (RFC 9113 section 5.4.2).
                    try self.pushIntentOrFailClosed(.{ .stream_reset = .{
                        .stream_id = hdr.stream_id,
                        .code = code,
                        .from_peer = true,
                    } });
                } else if (hdr.stream_id <= self.highest_peer_stream) {
                    // CLOSED, not idle. The peer sent this before it learned the
                    // stream was over — its RST and our END_STREAM crossed on
                    // the wire. RFC 9113 section 5.1 requires tolerating that,
                    // and it is not a rare case: it happens every time a browser
                    // cancels a fetch as the response completes. Failing the
                    // connection here would kill every OTHER stream on it, which
                    // is how one canceled request became a dead connection.
                    //
                    // The stream is already gone, its concurrency credit
                    // already released, and the handler (if any) already
                    // canceled when the stream closed. If it closed because
                    // this server reset it, the two RSTs crossed.
                    self.retireUnwrittenRst(hdr.stream_id);
                } else {
                    // IDLE: a stream id the peer has never opened has no state
                    // to reset, so this is a genuine protocol violation
                    // (RFC 9113 section 5.1 / 5.4.1).
                    try self.failConnection(.protocol_error);
                    return;
                }
            },
            .settings => try self.onSettings(hdr, payload),
            .push_promise => try self.failConnection(.protocol_error),
            .ping => try self.onPing(hdr, payload),
            .goaway => {
                if (hdr.stream_id != 0 or hdr.length < 8) {
                    try self.failConnection(.frame_size_error);
                    return;
                }
                const last: u31 = @intCast(std.mem.readInt(u32, payload[0..4], .big) & 0x7fff_ffff);
                self.goaway_ceiling = last;
                self.peer_goaway = true;
                // Existing streams at or below last may finish; do not tear down transport yet.
                // A peer GOAWAY announces an intent to stop, not an immediate
                // close. An instant teardown here would abort in-flight
                // responses that the peer is still waiting to receive.
            },
            .window_update => try self.onWindowUpdate(hdr, payload),
            .continuation => try self.failConnection(.protocol_error),
            _ => {}, // unknown types ignored after generic validation
        }
        if (hdr.stream_id > self.last_processed_stream and hdr.stream_id % 2 == 1) {
            self.last_processed_stream = hdr.stream_id;
        }
        try self.chargeRate(hdr.type, already_terminal);
    }

    fn chargeRate(self: *Session, typ: frame.FrameType, already_terminal: bool) !void {
        // Protocol errors win over simultaneous rate errors.
        if (already_terminal) return;
        if (self.terminal != .none) return;
        if (self.rate_limiter) |rl| {
            if (!rl.admit(typ, self.edge_now_ns)) {
                try self.failConnection(.enhance_your_calm);
            }
        }
    }

    /// Edge clock: field-block / request-body idle deadlines.
    ///
    /// Both deadlines answer the same attack: a peer that opens work and then
    /// stops, so the server holds state while the peer spends nothing. A
    /// half-sent field block pins the HPACK buffer and blocks every other frame
    /// on the connection, so it costs the CONNECTION. A stalled request body
    /// pins only its own stream, so it costs that stream a RST_STREAM.
    ///
    /// The edge passes `now_ns` rather than this module reading a clock. That
    /// keeps the timeout behaviour deterministic under test.
    pub fn checkIdleDeadlines(self: *Session, now_ns: u64) !void {
        if (self.terminal != .none) return;
        if (self.expect_continuation != null and self.field_block_started_ns != 0) {
            if (now_ns -% self.field_block_started_ns >= self.limits.field_block_timeout_ns) {
                try self.failConnection(.enhance_your_calm);
                return;
            }
        }
        // Collect first, act second. `emitRst` removes map entries, and a
        // mutation during iteration invalidates the iterator. The ids go in
        // `stale_ids`, which holds one per stream the connection may have.
        var n_stale: usize = 0;
        var it = self.body_idle_ns.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.* == 0) continue;
            if (now_ns -% e.value_ptr.* >= self.limits.request_body_idle_timeout_ns) {
                self.stale_ids[n_stale] = e.key_ptr.*;
                n_stale += 1;
            }
        }
        for (self.stale_ids[0..n_stale]) |sid| {
            _ = self.body_idle_ns.remove(sid);
            _ = body_idle_expiries.fetchAdd(1, .monotonic);
            try self.emitRst(sid, .cancel);
        }
    }

    /// Earliest edge-owned idle deadline, or null when no protocol timer is armed.
    /// This only derives protocol state; the edge chooses how to wait for it.
    ///
    /// The actor is event-driven and parks with no timer when nothing is armed.
    /// Without this query it would have to poll to notice a deadline, which
    /// costs a wakeup per tick on every idle connection. A `null` result is
    /// therefore a real answer: no protocol timer exists, so the actor may
    /// sleep until a peer or a handler wakes it.
    pub fn nextIdleDeadlineNs(self: *Session) ?u64 {
        var next: ?u64 = null;
        if (self.expect_continuation != null and self.field_block_started_ns != 0) {
            next = self.field_block_started_ns +% self.limits.field_block_timeout_ns;
        }
        var it = self.body_idle_ns.iterator();
        while (it.next()) |entry| {
            const started = entry.value_ptr.*;
            if (started == 0) continue;
            const deadline = started +% self.limits.request_body_idle_timeout_ns;
            if (next == null or deadline < next.?) next = deadline;
        }
        return next;
    }

    fn onSettings(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        if (hdr.stream_id != 0) {
            try self.failConnection(.protocol_error);
            return;
        }
        if (hdr.flags.ack()) {
            if (hdr.length != 0) try self.failConnection(.frame_size_error);
            self.settings_acked = true;
            return;
        }
        if (hdr.length % 6 != 0) {
            try self.failConnection(.frame_size_error);
            return;
        }
        var i: usize = 0;
        while (i + 6 <= payload.len) : (i += 6) {
            const id: frame.SettingId = @enumFromInt(std.mem.readInt(u16, payload[i..][0..2], .big));
            const value = std.mem.readInt(u32, payload[i + 2 ..][0..4], .big);
            switch (id) {
                .enable_push => if (value > 1) {
                    try self.failConnection(.protocol_error);
                    return;
                },
                // A new initial window retunes every OPEN stream by the
                // difference. This is the one setting that reaches back into
                // existing state, and it is why a stream send window may go
                // negative. See `flow.applyInitialWindowDelta`.
                .initial_window_size => {
                    if (value > 0x7fffffff) {
                        try self.failConnection(.flow_control_error);
                        return;
                    }
                    const new_iw: i32 = @intCast(value);
                    const delta = new_iw - self.peer_initial_window;
                    var it = self.streams.iterator();
                    while (it.next()) |e| {
                        e.value_ptr.window.applyInitialWindowDelta(delta) catch {
                            try self.failConnection(.flow_control_error);
                            return;
                        };
                    }
                    self.peer_initial_window = new_iw;
                    // A decrease cannot unblock anything; the scheduler finds
                    // streams it made negative when it next pops them.
                    if (delta > 0) self.credit_all = true;
                },
                .max_frame_size => {
                    if (value < 16384 or value > 16_777_215) {
                        try self.failConnection(.protocol_error);
                        return;
                    }
                    // Accept the peer's larger offer, then ignore it. The
                    // outbound frame size stays at the 16 KiB default, because
                    // that is what the boot-reserved scratch buffers are sized
                    // for. A bigger frame would need an allocation that
                    // `resourceUpperBound` never counted.
                    self.peer_max_frame_size = @min(value, frame.DEFAULT_MAX_FRAME_SIZE);
                },
                .header_table_size => self.peer_header_table_size = value,
                else => {},
            }
        }
        // Applied above; now the ack.
        try self.queueSettingsAck();
    }

    fn onPing(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        if (hdr.stream_id != 0 or hdr.length != 8) {
            try self.failConnection(.frame_size_error);
            return;
        }
        if (hdr.flags.ack()) {
            if (self.grace_phase == .phase1 and std.mem.eql(u8, payload[0..8], &self.grace_ping)) {
                self.grace_ping_acked = true;
            }
            return;
        }
        try self.queuePingAck(payload[0..8].*);
    }

    fn onWindowUpdate(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        if (hdr.length != 4) {
            try self.failConnection(.frame_size_error);
            return;
        }
        const incr_raw = std.mem.readInt(u32, payload[0..4], .big) & 0x7fff_ffff;
        const incr: u31 = @intCast(incr_raw);
        if (hdr.stream_id == 0) {
            self.windows.applyConnWindowUpdate(incr) catch |err| {
                const code: frame.ErrorCode = if (err == error.ProtocolError) .protocol_error else .flow_control_error;
                try self.failConnection(code);
            };
        } else if (self.streams.getPtr(hdr.stream_id)) |s| {
            if (s.window.applySendUpdate(incr)) |_| {
                self.noteSendCredit(hdr.stream_id);
            } else |err| {
                const code: frame.ErrorCode = if (err == error.ProtocolError) .protocol_error else .flow_control_error;
                try self.emitRst(hdr.stream_id, code);
            }
        } else if (hdr.stream_id <= self.highest_peer_stream) {
            // CLOSED, not idle. RFC 9113 section 6.9 is explicit: a peer may
            // send WINDOW_UPDATE for a stream it has not yet learned is over,
            // and "a receiver MUST NOT treat this as an error". This is the
            // ordinary flow-control conversation — a client credits the DATA it
            // just received while the server finishes the body — so failing the
            // connection here kills every other stream over a normal race.
            //
            // Nothing to apply: the stream is gone, and its send window with it.
        } else {
            // IDLE: the peer has never opened this stream, so there is no
            // window to credit (RFC 9113 section 5.1).
            try self.failConnection(.protocol_error);
        }
    }

    fn onData(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        if (hdr.stream_id == 0) {
            try self.failConnection(.protocol_error);
            return;
        }
        var data = payload;
        var pad: usize = 0;
        if (hdr.flags.padded) {
            if (payload.len < 1) {
                try self.failConnection(.protocol_error);
                return;
            }
            pad = payload[0];
            if (1 + pad > payload.len) {
                try self.failConnection(.protocol_error);
                return;
            }
            data = payload[1 .. payload.len - pad];
        }
        // Flow control charges the WHOLE payload, padding included. Only the
        // unpadded slice reaches the request body. A peer that pays for padding
        // must not gain free window by hiding bytes in it.
        const total: i32 = @intCast(payload.len);
        // Checked against what the peer was told: the window less the credit
        // still owed (`grantWindow`).
        if (self.windows.conn_recv - @as(i32, self.owed.conn_window) < total) {
            try self.failConnection(.flow_control_error);
            return;
        }
        self.windows.debitConnRecv(total) catch unreachable;
        const s = self.streams.getPtr(hdr.stream_id) orelse {
            if (hdr.stream_id <= self.highest_peer_stream) {
                // CLOSED, not idle: the client is still uploading a body for a
                // stream we ended — an abort, a slow-consumer kill, or an early
                // response. RFC 9113 section 6.4 says an endpoint that has SENT
                // RST_STREAM must IGNORE later frames on that stream, so this
                // does not answer with another reset; a reset per DATA frame
                // would be an amplification against a client that is simply
                // behind.
                //
                // The connection window was already debited above, and must be
                // (section 6.9.1) — the peer counted these bytes, so dropping
                // the accounting would desynchronize credit for the rest of the
                // connection. Replenish at the usual threshold so a large body
                // sent into a dead stream cannot strand the whole window.
                if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .data_on_closed_ignored, .code = .no_error, .stream_id = hdr.stream_id });
                try self.replenishConnWindow();
                return;
            }
            // IDLE: DATA for a stream never opened (RFC 9113 section 5.1).
            try self.failConnection(.protocol_error);
            return;
        };
        if (s.state == .idle or s.state == .closed) {
            try self.failConnection(.protocol_error);
            return;
        }
        if (s.window.recv - @as(i32, s.window_owed) < total) {
            try self.emitRst(hdr.stream_id, .flow_control_error);
            return;
        }
        s.window.debitRecv(total) catch unreachable;

        // An early-rejected stream (413/414/431) keeps consuming and discarding
        // its DATA until the peer sends END_STREAM. The server cannot stop the
        // peer mid-body without a RST, and a RST would race the 413 response
        // that explains the refusal. So the bytes are charged to flow control,
        // counted, and dropped — never retained and never dispatched.
        if (s.early_status == null and !s.refused_before_dispatch and s.headers_done) {
            var body = self.pending_bodies.getPtr(hdr.stream_id) orelse blk: {
                putBounded(&self.pending_bodies, hdr.stream_id, std.ArrayList(u8).empty);
                break :blk self.pending_bodies.getPtr(hdr.stream_id).?;
            };
            const cap = s.request_body_cap orelse self.limits.request_body_bytes;
            if (body.items.len + data.len > cap) {
                s.refused_before_dispatch = true;
                s.early_status = 413;
                // Discard overflow bytes; do not retain or dispatch.
                // 413 immediately on byte cap+1 — continue discard through remote END_STREAM.
                try self.emitEarlyReject(hdr.stream_id, 413);
            } else if (body.appendSlice(self.bodyAllocator(hdr.stream_id), data)) |_| {} else |err| {
                // The request's memory ran out. An isolated Session has only
                // the allocator's error to give. On the edge the stream is
                // answered (500 for its own cap, 503 for the others) and
                // everything the request held is given back at once; the
                // connection goes on to serve its other streams.
                if (self.headerListIsGpa()) return err;
                const status = self.memoryRefusalStatus(hdr.stream_id);
                s.refused_before_dispatch = true;
                s.early_status = status;
                self.dropPendingRequest(hdr.stream_id);
                try self.emitEarlyReject(hdr.stream_id, status);
            }
        }

        s.onData(data.len, hdr.flags.end_stream) catch |err| {
            try self.emitRst(hdr.stream_id, stream_mod.Stream.toErrorCode(err));
            return;
        };

        if (!hdr.flags.end_stream) {
            putBounded(&self.body_idle_ns, hdr.stream_id, self.edge_now_ns);
        } else {
            _ = self.body_idle_ns.remove(hdr.stream_id);
        }

        if (hdr.flags.end_stream) {
            if (s.early_status) |st| {
                try self.emitEarlyReject(hdr.stream_id, st);
            } else {
                try self.maybeDispatch(hdr.stream_id);
            }
        }
        self.releaseConcurrency(hdr.stream_id, .no_error);

        // WINDOW_UPDATE at half
        if (s.window.needsWindowUpdate()) {
            const inc = s.window.windowUpdateIncrement();
            if (inc > 0) {
                s.window.creditRecv(inc);
                try self.grantWindow(hdr.stream_id, inc);
            }
        }
        try self.replenishConnWindow();
    }

    fn replenishConnWindow(self: *Session) !void {
        if (!self.windows.needsConnWindowUpdate()) return;
        const inc = self.windows.connWindowUpdateIncrement();
        if (inc == 0) return;
        self.windows.creditConnRecv(inc);
        try self.grantWindow(0, inc);
    }

    /// Tell the peer about `inc` of window credit already added to the
    /// window (`stream_id` 0: the connection's). The window holds what this
    /// server granted; the owed sum (`Owed`) is the part the peer has not been
    /// told yet. DATA is checked against the difference, which is what the
    /// peer knows (`onData`), so the owed sum never exceeds the window.
    fn grantWindow(self: *Session, stream_id: u31, inc: u31) !void {
        // Credit already owed for this target: join it, one frame later.
        if (self.windowOwedPtr(stream_id)) |owed| {
            if (owed.* != 0) {
                self.oweWindowUpdate(stream_id, inc);
                return;
            }
        }
        var buf: [13]u8 = undefined;
        const n = try frame.Serializer.windowUpdate(&buf, stream_id, inc);
        const p = self.controlFrame(buf[0..n], .ordinary) catch |err| switch (err) {
            error.OutOfMemory => {
                self.oweWindowUpdate(stream_id, inc);
                return;
            },
        };
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .window_update, .payload = p, .stream_id = stream_id } });
    }

    /// The owed credit for `stream_id` (0: the connection), or null for a
    /// stream that is gone: its window went with it, and nothing is owed.
    fn windowOwedPtr(self: *Session, stream_id: u31) ?*u31 {
        if (stream_id == 0) return &self.owed.conn_window;
        const s = self.streams.getPtr(stream_id) orelse return null;
        return &s.window_owed;
    }

    /// `inc` of credit, granted and not sent: kept until `requeueOwed`.
    /// Called by the edge for a WINDOW_UPDATE it refused, too.
    pub fn oweWindowUpdate(self: *Session, stream_id: u31, inc: u31) void {
        const owed = self.windowOwedPtr(stream_id) orelse return;
        // It stays within the window, which a u31 holds: the credit was just
        // added to the window, and DATA past what the peer was told is a
        // flow-control error (`onData`).
        owed.* += inc;
        if (stream_id != 0) self.owed.streams = true;
    }

    fn queueSettingsAck(self: *Session) !void {
        if (self.owed.settings_acks == 0) {
            if (self.settingsAckFrame()) |p| {
                return self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .settings, .payload = p, .flags = .{ .end_stream = true } } });
            } else |err| switch (err) {
                error.OutOfMemory => {},
            }
        }
        try self.oweSettingsAck();
    }

    fn settingsAckFrame(self: *Session) error{OutOfMemory}![]u8 {
        var buf: [9]u8 = undefined;
        const n = frame.Serializer.settingsFrame(&buf, true, &.{}) catch unreachable;
        return self.controlFrame(buf[0..n], .ordinary);
    }

    /// One more SETTINGS ack owed: by `queueSettingsAck`, or by the edge for
    /// one it refused. Past the bound, ENHANCE_YOUR_CALM (`Owed`).
    pub fn oweSettingsAck(self: *Session) !void {
        if (self.owed.settings_acks == rates_mod.SETTINGS_BURST) return self.failConnection(.enhance_your_calm);
        self.owed.settings_acks += 1;
    }

    fn queuePingAck(self: *Session, payload: [8]u8) !void {
        if (self.owed.ping_len == 0) {
            if (self.pingAckFrame(payload)) |p| {
                return self.pushIntentOrFailClosed(.{ .outbound_frame = .{ .typ = .ping, .payload = p, .flags = .{ .end_stream = true } } });
            } else |err| switch (err) {
                error.OutOfMemory => {},
            }
        }
        try self.owePingAck(payload);
    }

    fn pingAckFrame(self: *Session, payload: [8]u8) error{OutOfMemory}![]u8 {
        var buf: [17]u8 = undefined;
        const n = frame.Serializer.ping(&buf, true, &payload) catch unreachable;
        return self.controlFrame(buf[0..n], .ordinary);
    }

    /// One more PING ack owed, with the PING's payload: by `queuePingAck`, or
    /// by the edge for one it refused. Past the bound, ENHANCE_YOUR_CALM.
    pub fn owePingAck(self: *Session, payload: [8]u8) !void {
        const o = &self.owed;
        if (o.ping_len == o.pings.len) return self.failConnection(.enhance_your_calm);
        o.pings[(o.ping_head + o.ping_len) % o.pings.len] = payload;
        o.ping_len += 1;
    }

    /// Queue what is owed (`Owed`), at most `room` frames: the room the
    /// edge's ordinary control share has now. SETTINGS acks first, then the
    /// PING acks in the order their PINGs came, then the connection's credit,
    /// then each stream's. A frame buffer that is not there stops it, and the
    /// rest stays owed. Nothing once the connection is ending: its GOAWAY
    /// already ends every stream, and the peer reads no further acks.
    pub fn requeueOwed(self: *Session, room: usize) void {
        if (self.terminal != .none) return;
        var left = room;
        const o = &self.owed;
        while (o.settings_acks != 0 and self.owedRoom(left)) {
            const p = self.settingsAckFrame() catch return;
            self.pushIntent(.{ .outbound_frame = .{ .typ = .settings, .payload = p, .flags = .{ .end_stream = true } } }) catch unreachable;
            o.settings_acks -= 1;
            left -= 1;
        }
        while (o.ping_len != 0 and self.owedRoom(left)) {
            const p = self.pingAckFrame(o.pings[o.ping_head]) catch return;
            self.pushIntent(.{ .outbound_frame = .{ .typ = .ping, .payload = p, .flags = .{ .end_stream = true } } }) catch unreachable;
            o.ping_head = (o.ping_head + 1) % o.pings.len;
            o.ping_len -= 1;
            left -= 1;
        }
        if (o.conn_window != 0 and self.owedRoom(left)) {
            if (!self.requeueWindow(0, &o.conn_window)) return;
            left -= 1;
        }
        if (!o.streams) return;
        var it = self.streams.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.window_owed == 0) continue;
            if (!self.owedRoom(left)) return;
            if (!self.requeueWindow(e.key_ptr.*, &e.value_ptr.window_owed)) return;
            left -= 1;
        }
        o.streams = false;
    }

    /// Room for one more owed frame: the edge's share has a free entry, and
    /// the intent queue keeps the room an RST needs.
    fn owedRoom(self: *const Session, left: usize) bool {
        return left != 0 and self.ordinaryIntentRoom() > limits_mod.RST_INTENT_ROOM;
    }

    fn requeueWindow(self: *Session, stream_id: u31, owed: *u31) bool {
        var buf: [13]u8 = undefined;
        const n = frame.Serializer.windowUpdate(&buf, stream_id, owed.*) catch unreachable;
        const p = self.controlFrame(buf[0..n], .ordinary) catch return false;
        self.pushIntent(.{ .outbound_frame = .{ .typ = .window_update, .payload = p, .stream_id = stream_id } }) catch unreachable;
        owed.* = 0;
        return true;
    }

    /// Emit exactly one HTTP response for a request that never reaches a
    /// handler. Several paths can decide the same refusal — an oversized header
    /// block, an oversized declared body, an oversized observed body, a late
    /// trailer block — and each may fire more than once as further frames
    /// arrive. `early_response_sent` makes the frameset exactly-once, because a
    /// second HEADERS on the same stream is a protocol violation that would
    /// turn a clean 413 into a broken connection.
    fn emitEarlyReject(self: *Session, stream_id: u31, status: u16) !void {
        const s = self.streams.getPtr(stream_id) orelse return;
        if (s.early_response_sent) return;
        s.early_response_sent = true;
        s.refused_before_dispatch = true;
        s.early_status = status;
        try self.pushIntentOrFailClosed(.{ .early_reject = .{ .stream_id = stream_id, .status = status } });
        // Also emit the HTTP response frames now so core stays self-contained for tests.
        self.cmdHeaders(stream_id, status, &.{}, true, .none) catch |err| switch (err) {
            // No frame buffer for the response: the stream is reset instead,
            // as when the edge refuses it (`resetRefusedHeaders`). An RST
            // always has room (`Limits.terminalFrames`).
            error.OutOfMemory => try self.emitRst(stream_id, .internal_error),
            else => return err,
        };
    }

    fn isTombstoned(self: *const Session, id: u31) bool {
        for (self.tombstones.items) |t| {
            if (t.id == id) return true;
        }
        return false;
    }

    fn onHeaders(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .headers_seen, .code = .no_error, .stream_id = hdr.stream_id });
        if (hdr.stream_id == 0 or hdr.stream_id % 2 == 0) {
            try self.failConnection(.protocol_error);
            return;
        }
        // A stream ID above the highest one ever opened cannot be a closed
        // stream. Check the watermark first so the normal monotonic request
        // path does not scan the bounded tombstone ring.
        if (hdr.stream_id <= self.highest_peer_stream and self.isTombstoned(hdr.stream_id)) {
            // HEADERS on a closed stream: connection STREAM_CLOSED (RFC 9113 §5.1 / h2spec 5.1/12).
            if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .headers_on_tombstone, .code = .stream_closed, .stream_id = hdr.stream_id });
            try self.failConnection(.stream_closed);
            return;
        }
        if (self.streams.getPtr(hdr.stream_id)) |existing| {
            if (existing.state == .closed) {
                if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .headers_on_closed, .code = .stream_closed, .stream_id = hdr.stream_id });
                try self.failConnection(.stream_closed);
                return;
            }
            if (existing.state == .half_closed_remote) {
                try self.emitRst(hdr.stream_id, .stream_closed);
                return;
            }
        }
        if (hdr.stream_id <= self.highest_peer_stream) {
            if (self.streams.get(hdr.stream_id) == null) {
                try self.failConnection(.protocol_error);
                return;
            }
        } else {
            if (self.goaway_ceiling) |ceil| {
                if (hdr.stream_id > ceil) {
                    try self.refuseStream(hdr.stream_id);
                    return;
                }
            }
            self.highest_peer_stream = hdr.stream_id;
        }

        var fragment = payload;
        if (hdr.flags.padded) {
            if (payload.len < 1) {
                try self.failConnection(.protocol_error);
                return;
            }
            const pad = payload[0];
            var skip: usize = 1;
            if (hdr.flags.priority) skip += 5;
            if (skip + pad > payload.len) {
                try self.failConnection(.protocol_error);
                return;
            }
            fragment = payload[skip .. payload.len - pad];
        } else if (hdr.flags.priority) {
            if (payload.len < 5) {
                try self.failConnection(.protocol_error);
                return;
            }
            fragment = payload[5..];
        }

        self.header_block.clearRetainingCapacity();
        self.continuation_count = 0;
        self.header_end_stream = hdr.flags.end_stream;
        if (!hdr.flags.end_headers) {
            self.appendHeaderBlock(fragment);
            self.expect_continuation = hdr.stream_id;
            if (self.field_block_started_ns == 0) self.field_block_started_ns = self.edge_now_ns;
            return;
        }
        self.field_block_started_ns = 0;
        try self.finishHeaderBlock(hdr.stream_id, fragment);
    }

    fn consumeContinuation(self: *Session, hdr: frame.FrameHeader, payload: []const u8) !void {
        self.continuation_count += 1;
        if (self.continuation_count > self.limits.continuation_frames or
            self.header_block.items.len + payload.len > self.limits.compressed_header_bytes)
        {
            try self.failConnection(.enhance_your_calm);
            return;
        }
        self.appendHeaderBlock(payload);
        if (!hdr.flags.end_headers) return;
        const sid = self.expect_continuation.?;
        self.expect_continuation = null;
        self.field_block_started_ns = 0;
        try self.finishHeaderBlock(sid, self.header_block.items);
        self.header_block.clearRetainingCapacity();
    }

    /// A complete field block arrived. This is where a request becomes real.
    ///
    /// The sequence, and each step depends on the one before it:
    ///
    /// 1. Decode. A compression fault kills the CONNECTION, never one stream,
    ///    because the shared HPACK table is now out of step with the peer.
    /// 2. Admit, if the stream is new. The per-connection cap is checked first,
    ///    then the server-wide hook. A refusal is a RST_STREAM, not a
    ///    connection error: the peer may simply retry later.
    /// 3. Classify. A block on a stream that already has headers is a TRAILER
    ///    block and follows different rules.
    /// 4. Validate the pseudo-headers, then apply the size policies. A policy
    ///    breach becomes an `early_status` and not an exception, so the peer
    ///    receives a real HTTP status instead of a bare reset.
    /// 5. Transfer a complete header-only request directly to its dispatch
    ///    intent. Requests with bodies store fields until DATA END_STREAM.
    ///
    /// Ownership: `block` is the HEADERS payload for a single-frame END_HEADERS
    /// request, or the assembled `header_block` after CONTINUATION. The
    /// `decoded.fields` slice is owned from step 1 onward.
    /// Each field's strings are independently owned or borrowed. GPA results
    /// are freed with `freeResult`; edge-arena results are discarded via the
    /// header-list hook. Success hands the slice to an intent or to
    /// `pending_headers` / `pending_trailers`.
    fn finishHeaderBlock(self: *Session, stream_id: u31, block: []const u8) !void {
        const mem = self.headerListMem(stream_id);
        const req_alloc = switch (mem) {
            .request => |r| r.alloc,
            .scratch => |sc| sc.alloc,
        };
        const transient = switch (mem) {
            .request => |r| r.scratch,
            .scratch => |sc| sc.alloc,
        };
        // Set when the edge had no memory for this request. The block is
        // decoded anyway (the table must advance), and the stream is answered
        // with this status instead of being dispatched.
        const scratch_status: ?u16 = switch (mem) {
            .request => null,
            .scratch => |sc| sc.status,
        };
        const decoded = self.decoder.decodeInto(
            req_alloc,
            transient,
            block,
            self.limits.header_fields,
            self.limits.decoded_header_bytes,
            256,
            self.limits.regular_field_value_bytes,
        ) catch |err| {
            if (scratch_status == null) self.discardHeaderList(stream_id);
            if (close_probe_fn) |probe| probe(.{ .session = @intFromPtr(self), .site = .hpack_decode_fail, .code = if (err == error.OutOfMemory) .internal_error else .compression_error, .stream_id = stream_id });
            if (err == error.OutOfMemory) {
                try self.failConnection(.internal_error);
            } else {
                try self.failConnection(.compression_error);
            }
            return;
        };
        var s_ptr = self.streams.getPtr(stream_id);
        if (s_ptr == null) {
            if (self.active_streams >= self.limits.max_streams_per_connection) {
                self.dropDecoded(stream_id, decoded, scratch_status != null);
                try self.refuseStream(stream_id);
                return;
            }
            if (self.stream_hooks) |h| {
                if (!h.tryAdmit(h.ctx)) {
                    self.dropDecoded(stream_id, decoded, scratch_status != null);
                    try self.refuseStream(stream_id);
                    return;
                }
            }
            var credit_held = self.stream_hooks != null;
            errdefer if (credit_held) {
                if (self.stream_hooks) |h| h.onRelease(h.ctx);
            };
            putBounded(&self.streams, stream_id, stream_mod.Stream{ .id = stream_id, .window = .{ .send = self.peer_initial_window } });
            credit_held = false;
            self.active_streams += 1;
            s_ptr = self.streams.getPtr(stream_id);
        }
        const s = s_ptr.?;

        if (s.headers_done) {
            // Trailers only legal while remote half is still open, and MUST carry END_STREAM.
            if (!self.header_end_stream) {
                self.dropDecoded(stream_id, decoded, scratch_status != null);
                try self.emitRst(stream_id, .protocol_error);
                return;
            }
            if (s.end_stream_remote or s.state == .half_closed_remote or s.state == .closed) {
                self.dropDecoded(stream_id, decoded, scratch_status != null);
                try self.emitRst(stream_id, .stream_closed);
                return;
            }
            // Trailers too large to keep, or no memory to keep them in: the
            // request cannot be dispatched whole, so it is answered and
            // everything it held (headers and body too) goes back now.
            if (decoded.truncated or scratch_status != null) {
                const st = scratch_status orelse 431;
                self.dropDecoded(stream_id, decoded, scratch_status != null);
                self.dropPendingRequest(stream_id);
                s.refused_before_dispatch = true;
                s.early_status = st;
                s.onHeaders(true) catch {};
                try self.emitEarlyReject(stream_id, st);
                self.releaseConcurrency(stream_id, .no_error);
                return;
            }
            for (decoded.fields) |f| {
                if (f.name.len > 0 and f.name[0] == ':') {
                    self.dropDecoded(stream_id, decoded, scratch_status != null);
                    try self.emitRst(stream_id, .protocol_error);
                    return;
                }
            }
            // Early-rejected streams: trailers close remote; ensure the one HTTP response exists.
            if (s.early_status) |st| {
                self.dropDecoded(stream_id, decoded, scratch_status != null);
                // The early-reject state is already terminal and owns the response.
                s.onHeaders(true) catch {};
                try self.emitEarlyReject(stream_id, st);
                self.releaseConcurrency(stream_id, .no_error);
                return;
            }
            s.onHeaders(true) catch |err| {
                self.dropDecoded(stream_id, decoded, scratch_status != null);
                try self.emitRst(stream_id, stream_mod.Stream.toErrorCode(err));
                return;
            };
            putBounded(&self.pending_trailers, stream_id, decoded.fields);
            try self.maybeDispatch(stream_id);
            self.releaseConcurrency(stream_id, .no_error);
            return;
        }

        // More fields or decoded bytes than the limits allow: only a prefix of
        // the block was kept, and a prefix cannot be validated. It is a 431.
        if (decoded.truncated) {
            self.dropDecoded(stream_id, decoded, scratch_status != null);
            s.refused_before_dispatch = true;
            s.early_status = 431;
            s.onHeaders(self.header_end_stream) catch {};
            try self.emitEarlyReject(stream_id, 431);
            self.releaseConcurrency(stream_id, .no_error);
            return;
        }

        const parsed = fields.validateRequestFields(decoded.fields) catch |err| {
            self.dropDecoded(stream_id, decoded, scratch_status != null);
            if (err == error.PathTooLong) {
                s.refused_before_dispatch = true;
                s.early_status = 414;
                // The early-reject state is already terminal and owns the response.
                s.onHeaders(self.header_end_stream) catch {};
                try self.emitEarlyReject(stream_id, 414);
                self.releaseConcurrency(stream_id, .no_error);
                return;
            }
            try self.emitRst(stream_id, .protocol_error);
            return;
        };
        if (decoded.policy_exceeded) {
            s.refused_before_dispatch = true;
            s.early_status = 431;
        }
        s.request_body_cap = self.routeBodyCap(parsed.method, parsed.path);
        if (parsed.content_length) |cl| {
            const cap = s.request_body_cap orelse self.limits.request_body_bytes;
            if (cl > cap) {
                s.refused_before_dispatch = true;
                s.early_status = 413;
            }
        }

        // No memory for the request: the other refusals above say more about
        // the request than this does, so they win.
        if (s.early_status == null) {
            if (scratch_status) |st| {
                s.refused_before_dispatch = true;
                s.early_status = st;
            }
        }

        s.content_length = parsed.content_length;
        const early = s.early_status;
        s.onHeaders(self.header_end_stream) catch |err| {
            self.dropDecoded(stream_id, decoded, scratch_status != null);
            try self.emitRst(stream_id, stream_mod.Stream.toErrorCode(err));
            return;
        };
        if (early) |st| {
            self.dropDecoded(stream_id, decoded, scratch_status != null);
            try self.emitEarlyReject(stream_id, st);
            self.releaseConcurrency(stream_id, .no_error);
        } else {
            if (self.header_end_stream) {
                // The common one-shot path already has a complete request and
                // the validated pseudo-header slices. Do not put/fetch the
                // fields through a hash map and validate them a second time.
                try self.dispatchOwnedRequest(stream_id, parsed, decoded.fields, &.{}, &.{});
            } else {
                putBounded(&self.pending_headers, stream_id, decoded.fields);
            }
            self.releaseConcurrency(stream_id, .no_error);
        }
    }

    /// Hand a complete request to the edge, exactly once.
    ///
    /// Several frames can reach this point for one stream — HEADERS with
    /// END_STREAM, the last DATA, or a trailer block — so the three guards are
    /// the real contract: the remote half must be closed, the stream must not
    /// have been refused, and no handler may have started yet. `handler_started`
    /// is what makes a request one request and not one per triggering frame.
    ///
    /// Headers, body, and trailers are moved out of the pending maps, then
    /// `dispatchOwnedRequest` transfers them to the intent.
    fn maybeDispatch(self: *Session, stream_id: u31) !void {
        const s = self.streams.getPtr(stream_id) orelse return;
        if (!s.end_stream_remote or s.refused_before_dispatch or s.handler_started) return;
        const hdrs = self.pending_headers.fetchRemove(stream_id) orelse return;
        var headers_owned = true;
        errdefer if (headers_owned) {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, hdrs.value);
                self.gpa.free(hdrs.value);
            } else {
                self.discardHeaderList(stream_id);
            }
        };
        const body_entry = self.pending_bodies.fetchRemove(stream_id);
        const body = blk: {
            if (body_entry) |e| {
                var list = e.value;
                // On the edge the body stays where it was appended: in the
                // request's arena, which the dispatch carries.
                if (!self.headerListIsGpa()) break :blk list.items;
                break :blk list.toOwnedSlice(self.gpa) catch |err| {
                    list.deinit(self.gpa);
                    // Preserve the original allocation error if fail-closed signaling also allocates.
                    self.failClosedInternal() catch {};
                    return err;
                };
            }
            break :blk &.{};
        };
        var body_owned = body.len != 0 and self.headerListIsGpa();
        errdefer if (body_owned) self.gpa.free(body);

        const parsed = fields.validateRequestFields(hdrs.value) catch {
            try self.emitRst(stream_id, .protocol_error);
            return;
        };

        const trailers_entry = self.pending_trailers.fetchRemove(stream_id);
        const trailers: []hpack.HeaderField = if (trailers_entry) |e| e.value else &.{};
        var trailers_owned = trailers.len != 0;
        errdefer if (trailers_owned) {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, trailers);
                self.gpa.free(trailers);
            }
        };

        // The helper owns every slice from this point, including its failures.
        headers_owned = false;
        body_owned = false;
        trailers_owned = false;
        try self.dispatchOwnedRequest(stream_id, parsed, hdrs.value, body, trailers);
    }

    /// Ownership transfer point for a complete request. On entry this function
    /// owns all three slices; on success the intent owns them. Failures before
    /// transfer free exactly once, while `pushIntentOrFailClosed` owns and
    /// releases an intent it accepted before failing closed.
    fn dispatchOwnedRequest(
        self: *Session,
        stream_id: u31,
        parsed: fields.ValidatedRequestFields,
        headers: []hpack.HeaderField,
        body: []const u8,
        trailers: []hpack.HeaderField,
    ) !void {
        var headers_owned = true;
        errdefer if (headers_owned) {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, headers);
                self.gpa.free(headers);
            } else {
                self.discardHeaderList(stream_id);
            }
        };
        var body_owned = body.len != 0 and self.headerListIsGpa();
        errdefer if (body_owned) self.gpa.free(body);
        var trailers_owned = trailers.len != 0;
        errdefer if (trailers_owned) {
            if (self.headerListIsGpa()) {
                hpack.HeaderField.freeOwnedSlice(self.gpa, trailers);
                self.gpa.free(trailers);
            }
        };

        const s = self.streams.getPtr(stream_id) orelse return error.StreamClosed;
        if (!s.end_stream_remote or s.refused_before_dispatch or s.handler_started) {
            return error.StreamClosed;
        }
        // Past this line the intent owns everything. Clear the flags BEFORE the
        // push, so that a `PoolExhausted` inside `pushIntentOrFailClosed` — which
        // releases the intent itself — cannot make the errdefer blocks free the
        // same memory a second time.
        s.handler_started = true;
        headers_owned = false;
        body_owned = false;
        trailers_owned = false;
        try self.pushIntentOrFailClosed(.{ .dispatch_request = .{
            .stream_id = stream_id,
            .method = parsed.method,
            .scheme = parsed.scheme,
            .authority = parsed.authority,
            .path = parsed.path,
            .query = parsed.query,
            .headers = headers,
            .body = body,
            .trailers = trailers,
        } });
    }

    /// Queue a response's HEADERS. A block that does not fit one frame of the
    /// default maximum size (16 KiB, which is also the most this server ever
    /// sends in one frame) is cut into a HEADERS frame and CONTINUATION
    /// frames, each its own intent and its own buffer. RFC 9113 section 6.10
    /// forbids any other frame between them: the scheduler keeps them
    /// together (`FairScheduler.block_open`). END_STREAM goes on the HEADERS
    /// frame and END_HEADERS on the last one.
    ///
    /// The response is bounded before anything is encoded or allocated:
    /// `Limits.response_header_fields` fields (`:status` counts) and
    /// `response_header_bytes` of encoded block, and a response at the bound is
    /// accepted. Over it, `HeadersTooLarge`.
    ///
    /// All or none: every frame's buffer and every intent slot is taken before
    /// the first intent is queued, so a refusal (`HeadersTooLarge` or
    /// `OutOfMemory`) leaves nothing queued and the caller may still answer
    /// the stream with something else. A half-queued block would be one the
    /// peer waits on forever.
    fn cmdHeaders(self: *Session, stream_id: u31, status: u16, headers: []const hpack.HeaderField, end_stream: bool, ticket: WireTicket) !void {
        // A RST tombstone already released the map entry. Emitting HEADERS
        // after that is how a handler that took `session_mu` late queued a
        // body the scheduler can never emit (TLS stall: pending DATA, tomb_rst).
        if (self.streams.get(stream_id) == null) return error.StreamClosed;
        var status_buf: [3]u8 = undefined;
        const status_str = try std.fmt.bufPrint(&status_buf, "{d}", .{status});
        const status_field = [_]hpack.HeaderField{.{ .name = ":status", .value = status_str }};
        const parts = [_][]const hpack.HeaderField{ &status_field, headers };
        if (headers.len >= self.limits.response_header_fields) return error.HeadersTooLarge;
        const status_len = hpack.Encoder.encodedLen(&status_field) catch return error.HeadersTooLarge;
        const headers_len = hpack.Encoder.encodedLen(headers) catch return error.HeadersTooLarge;
        const block_len = std.math.add(usize, status_len, headers_len) catch return error.HeadersTooLarge;
        if (block_len > self.limits.response_header_bytes) return error.HeadersTooLarge;

        const max_payload: usize = frame.DEFAULT_MAX_FRAME_SIZE;
        const n_frames = @max(1, (block_len + max_payload - 1) / max_payload);
        // `Limits.check` keeps the bound within `MAX_HEADER_FRAGMENTS` frames.
        std.debug.assert(n_frames <= limits_mod.MAX_HEADER_FRAGMENTS);
        if (self.ordinaryIntentRoom() < n_frames) return error.OutOfMemory;

        var bufs: [limits_mod.MAX_HEADER_FRAGMENTS][]u8 = undefined;
        var built: usize = 0;
        errdefer for (bufs[0..built]) |buf| self.freeFrame(buf);
        var offset: usize = 0;
        while (built < n_frames) {
            const take = @min(max_payload, block_len - offset);
            const first = built == 0;
            const last = built + 1 == n_frames;
            const out = try self.allocFrame(frame.FRAME_HEADER_LEN + take, .ordinary);
            bufs[built] = out;
            built += 1;
            const fh = frame.FrameHeader{
                .length = @intCast(take),
                .type = if (first) .headers else .continuation,
                .flags = .{ .end_headers = last, .end_stream = first and end_stream },
                .stream_id = stream_id,
            };
            fh.encode(out[0..frame.FRAME_HEADER_LEN]);
            const wrote = hpack.Encoder.encodeRange(out[frame.FRAME_HEADER_LEN..][0..take], &parts, offset);
            std.debug.assert(wrote == take);
            offset += take;
        }
        std.debug.assert(offset == block_len);

        // From here the frames belong to the intents. The room was checked
        // above, so a push cannot fail.
        for (bufs[0..n_frames], 0..) |out, k| {
            const flags = frame.FrameHeader.decode(out[0..frame.FRAME_HEADER_LEN]).flags;
            self.pushIntent(.{ .outbound_frame = .{
                .typ = if (k == 0) .headers else .continuation,
                .payload = out,
                .stream_id = stream_id,
                .flags = flags,
                .ticket = if (k == 0) ticket else .none,
                // A final HEADERS owes an RST if the edge refuses it.
                .owes_rst_if_refused = k == 0 and end_stream,
            } }) catch unreachable;
        }
        built = 0;
        if (self.streams.getPtr(stream_id)) |s| {
            if (end_stream) {
                s.localEndStream();
                self.releaseConcurrency(stream_id, .no_error);
            }
        }
    }

    fn cmdData(self: *Session, stream_id: u31, data: []const u8, end_stream: bool) !void {
        if (data.len == 0 and !end_stream) return;
        const out = try self.makeDataFrame(stream_id, data, end_stream);
        try self.pushIntentOrFailClosed(.{ .outbound_frame = .{
            .typ = .data,
            .payload = out,
            .stream_id = stream_id,
            .flags = .{ .end_stream = end_stream },
        } });
    }

    /// Outbound DATA credit for a stream (0 when negative/exhausted).
    ///
    /// A missing map entry is not the same as a blocked window. The TLS stall
    /// parked with scheduler slabs still holding the response body after
    /// `releaseConcurrency` had dropped the stream; treating that as zero
    /// credit made `FairScheduler.drain` skip those streams forever.
    pub fn streamSendAvailable(self: *const Session, stream_id: u31) i32 {
        if (self.streams.get(stream_id)) |s| {
            return s.window.availableSend(self.windows.conn_send);
        }
        if (self.windows.conn_send <= 0) return 0;
        if (self.tombstoneCode(stream_id)) |code| {
            if (code != .no_error) return 0;
        }
        return self.windows.conn_send;
    }

    fn noteSendCredit(self: *Session, stream_id: u31) void {
        if (self.credit_all) return;
        if (self.credit_len == self.credit_ids.len) {
            self.credit_all = true;
            return;
        }
        self.credit_ids[self.credit_len] = stream_id;
        self.credit_len += 1;
    }

    /// For the consumer: the streams credited since the last call, and
    /// whether to treat every stream as credited. Clears the journal.
    pub fn takeSendCredit(self: *Session) struct { []const u31, bool } {
        const out = .{ self.credit_ids[0..self.credit_len], self.credit_all };
        self.credit_len = 0;
        self.credit_all = false;
        return out;
    }

    /// The peer reset `stream_id`, which this connection no longer has: the
    /// peer counts it closed. An RST_STREAM this server queued for it and has
    /// not written tells the peer nothing and holds one of the
    /// `Limits.terminalFrames` slots, so it is dropped now, during ingest,
    /// before a refusal later in the same read chunk needs the slot: from the
    /// intent queue if it is still there, and from the edge's terminal ring
    /// (`StreamHooks.retireRst`). Whether this server reset the stream is not
    /// looked up in the tombstones, which a busy connection overwrites: the
    /// search by id answers it. O(intents + T), and a peer RST is rate
    /// limited.
    fn retireUnwrittenRst(self: *Session, stream_id: u31) void {
        // A final HEADERS still queued: the peer has closed the stream, so a
        // refusal of it owes nothing now.
        self.clearRefusalObligation(stream_id);
        var kept: usize = 0;
        for (self.intent_storage[0..self.intent_len]) |*it| {
            const drop = switch (it.*) {
                .outbound_frame => |f| f.typ == .rst_stream and f.stream_id == stream_id,
                else => false,
            };
            if (drop) {
                self.releaseIntent(it);
                continue;
            }
            self.intent_storage[kept] = it.*;
            kept += 1;
        }
        self.intent_len = kept;
        if (self.stream_hooks) |h| {
            if (h.retireRst) |f| f(h.ctx, stream_id);
        }
    }

    /// The stream ended for the peer some other way: a final HEADERS for it
    /// still in the intent queue no longer owes an RST if refused
    /// (`OutboundFrame.owes_rst_if_refused`). O(intents), on an RST only.
    fn clearRefusalObligation(self: *Session, stream_id: u31) void {
        for (self.intent_storage[0..self.intent_len]) |*it| switch (it.*) {
            .outbound_frame => |*f| if (f.typ == .headers and f.stream_id == stream_id) {
                f.owes_rst_if_refused = false;
            },
            else => {},
        };
    }

    fn tombstonePtr(self: *Session, id: u31) ?*Tombstone {
        for (self.tombstones.items) |*t| {
            if (t.id == id) return t;
        }
        return null;
    }

    pub fn tombstoneCode(self: *const Session, id: u31) ?frame.ErrorCode {
        for (self.tombstones.items) |t| {
            if (t.id == id) return t.code;
        }
        return null;
    }

    pub fn connectionSendAvailable(self: *const Session) i32 {
        if (self.windows.conn_send <= 0) return 0;
        return self.windows.conn_send;
    }
};

test "tombstones overwrite oldest in place" {
    var session = try Session.init(std.testing.allocator, std.testing.allocator, .{ .stream_tombstones = 2 });
    defer session.deinit();

    try session.addTombstone(1, .cancel);
    try session.addTombstone(3, .stream_closed);
    try session.addTombstone(5, .no_error);

    try std.testing.expectEqual(@as(usize, 2), session.tombstones.items.len);
    try std.testing.expect(!session.isTombstoned(1));
    try std.testing.expect(session.isTombstoned(3));
    try std.testing.expect(session.isTombstoned(5));
    try std.testing.expectEqual(@as(u31, 5), session.tombstones.items[0].id);
    try std.testing.expectEqual(@as(u31, 3), session.tombstones.items[1].id);
}

test "makeDataFrame still frames DATA after the stream map entry is gone" {
    var session = try Session.init(std.testing.allocator, std.testing.allocator, .defaults);
    defer session.deinit();
    const body = "Hello, World!";
    try session.streams.put(1, .{
        .id = 1,
        .state = .half_closed_remote,
        .window = .{ .send = flow.INITIAL_WINDOW },
    });
    session.active_streams = 1;
    const live = try session.makeDataFrame(1, body, true);
    defer std.testing.allocator.free(live);
    try std.testing.expectEqual(@as(usize, frame.FRAME_HEADER_LEN + body.len), live.len);
    try std.testing.expect(session.streams.get(1) == null);

    try session.addTombstone(3, .no_error);
    try std.testing.expect(session.streamSendAvailable(3) > 0);
    const orphan = try session.makeDataFrame(3, body, true);
    defer std.testing.allocator.free(orphan);
    try std.testing.expectEqual(@as(usize, frame.FRAME_HEADER_LEN + body.len), orphan.len);
    try std.testing.expectEqual(@as(u8, 1), orphan[4] & 1);

    try session.addTombstone(5, .cancel);
    try std.testing.expectEqual(@as(i32, 0), session.streamSendAvailable(5));
}

/// An isolated Session with stream 1 open for a response, its preface frames
/// already given back.
fn headerBoundSession(gpa: std.mem.Allocator, boot: std.mem.Allocator, limits: limits_mod.Limits) !Session {
    var session = try Session.init(boot, gpa, limits);
    errdefer session.deinit();
    freeTestIntents(gpa, session.drainIntents());
    try session.streams.put(1, .{
        .id = 1,
        .state = .half_closed_remote,
        .window = .{ .send = flow.INITIAL_WINDOW },
    });
    session.active_streams = 1;
    return session;
}

test "response header bound: a block of exactly response_header_bytes is queued, one byte more is refused with nothing queued" {
    const gpa = std.testing.allocator;
    // One header: a new name and a value of `n` bytes. The block is `:status`
    // (one byte, static index) and the literal: 1 + name (1 + 5) + value (1 or
    // 2 length bytes + n).
    var value: [200]u8 = @splat('v');
    const fits = [_]hpack.HeaderField{.{ .name = "x-hdr", .value = value[0..120] }};
    const over = [_]hpack.HeaderField{.{ .name = "x-hdr", .value = value[0..121] }};
    const exact = (try hpack.Encoder.encodedLen(&.{.{ .name = ":status", .value = "200" }})) + try hpack.Encoder.encodedLen(&fits);
    try std.testing.expectEqual(exact + 1, (try hpack.Encoder.encodedLen(&.{.{ .name = ":status", .value = "200" }})) + try hpack.Encoder.encodedLen(&over));

    var limits = limits_mod.Limits.defaults;
    limits.response_header_bytes = exact;
    var session = try headerBoundSession(gpa, gpa, limits);
    defer session.deinit();

    try std.testing.expectError(error.HeadersTooLarge, session.applyCommand(.{ .respond_headers = .{ .ticket = .none,
        .stream_id = 1,
        .status = 200,
        .headers = &over,
        .end_stream = true,
    } }));
    try std.testing.expectEqual(@as(usize, 0), session.intentLen());
    // The stream is still open: the caller may answer it another way.
    try std.testing.expect(session.streams.get(1) != null);

    try session.applyCommand(.{ .respond_headers = .{ .ticket = .none, .stream_id = 1, .status = 200, .headers = &fits, .end_stream = true } });
    try std.testing.expectEqual(@as(usize, 1), session.intentLen());
    freeTestIntents(gpa, session.drainIntents());
}

test "response header bound: response_header_fields counts :status, and a response at the bound is queued" {
    const gpa = std.testing.allocator;
    var limits = limits_mod.Limits.defaults;
    limits.response_header_fields = 3;
    var session = try headerBoundSession(gpa, gpa, limits);
    defer session.deinit();
    const two = [_]hpack.HeaderField{ .{ .name = "x-a", .value = "1" }, .{ .name = "x-b", .value = "2" } };
    const three = [_]hpack.HeaderField{ .{ .name = "x-a", .value = "1" }, .{ .name = "x-b", .value = "2" }, .{ .name = "x-c", .value = "3" } };
    try std.testing.expectError(error.HeadersTooLarge, session.applyCommand(.{ .respond_headers = .{ .ticket = .none, .stream_id = 1, .status = 200, .headers = &three, .end_stream = false } }));
    try std.testing.expectEqual(@as(usize, 0), session.intentLen());
    // :status and two more is three fields: at the bound.
    try session.applyCommand(.{ .respond_headers = .{ .ticket = .none, .stream_id = 1, .status = 200, .headers = &two, .end_stream = false } });
    try std.testing.expectEqual(@as(usize, 1), session.intentLen());
    freeTestIntents(gpa, session.drainIntents());
}

test "a header block above one frame is cut into HEADERS and CONTINUATION frames of at most 16 KiB, or queued not at all" {
    const gpa = std.testing.allocator;
    var limits = limits_mod.Limits.defaults;
    limits.response_header_bytes = 3 * frame.DEFAULT_MAX_FRAME_SIZE;
    var big: [frame.DEFAULT_MAX_FRAME_SIZE + 1000]u8 = @splat('h');
    const wide = [_]hpack.HeaderField{
        .{ .name = "x-one", .value = &big },
        .{ .name = "x-two", .value = big[0 .. big.len - 100] },
    };
    const block_len = (try hpack.Encoder.encodedLen(&.{.{ .name = ":status", .value = "200" }})) + try hpack.Encoder.encodedLen(&wide);
    try std.testing.expect(block_len > 2 * frame.DEFAULT_MAX_FRAME_SIZE);
    try std.testing.expect(block_len <= limits.response_header_bytes);

    {
        var session = try headerBoundSession(gpa, gpa, limits);
        defer session.deinit();
        try session.applyCommand(.{ .respond_headers = .{ .ticket = .none, .stream_id = 1, .status = 200, .headers = &wide, .end_stream = true } });
        const intents = session.drainIntents();
        defer freeTestIntents(gpa, intents);
        try std.testing.expectEqual(@as(usize, 3), intents.len);
        var total: usize = 0;
        for (intents, 0..) |it, k| {
            const f = it.outbound_frame;
            const hdr = frame.FrameHeader.decode(f.payload[0..frame.FRAME_HEADER_LEN]);
            try std.testing.expect(hdr.length <= frame.DEFAULT_MAX_FRAME_SIZE);
            try std.testing.expectEqual(if (k == 0) frame.FrameType.headers else frame.FrameType.continuation, hdr.type);
            try std.testing.expectEqual(k == 2, hdr.flags.end_headers);
            // END_STREAM rides the HEADERS frame only.
            try std.testing.expectEqual(k == 0, hdr.flags.end_stream);
            total += hdr.length;
        }
        try std.testing.expectEqual(block_len, total);
    }

    // The third frame cannot be had: the first two are given back and the
    // stream has no half-queued block.
    var failing = std.testing.FailingAllocator.init(gpa, .{});
    var session = try headerBoundSession(failing.allocator(), gpa, limits);
    defer session.deinit();
    failing.fail_index = failing.alloc_index + 2;
    try std.testing.expectError(error.OutOfMemory, session.applyCommand(.{ .respond_headers = .{ .ticket = .none, .stream_id = 1, .status = 200, .headers = &wide, .end_stream = true } }));
    try std.testing.expectEqual(@as(usize, 0), session.intentLen());
    try std.testing.expect(session.streams.get(1) != null);
}

test "respond_headers after RST is StreamClosed" {
    var session = try Session.init(std.testing.allocator, std.testing.allocator, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| std.testing.allocator.free(f.payload),
            else => {},
        };
    }
    try session.streams.put(1, .{
        .id = 1,
        .state = .half_closed_remote,
        .window = .{ .send = flow.INITIAL_WINDOW },
    });
    session.active_streams = 1;
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 1, .code = .internal_error } });
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| std.testing.allocator.free(f.payload),
            else => {},
        };
    }
    try std.testing.expect(session.streams.get(1) == null);
    try std.testing.expectError(error.StreamClosed, session.applyCommand(.{ .respond_headers = .{ .ticket = .none,
        .stream_id = 1,
        .status = 200,
        .headers = &.{},
        .end_stream = true,
    } }));
}

/// Counts of what a drained batch holds, then frees it.
const TerminalCounts = struct { rst: usize = 0, goaway: usize = 0, stream_reset: usize = 0, connection_error: usize = 0 };

fn countTerminalIntents(gpa: std.mem.Allocator, session: *Session) TerminalCounts {
    const intents = session.drainIntents();
    defer freeTestIntents(gpa, intents);
    var c: TerminalCounts = .{};
    for (intents) |it| switch (it) {
        .outbound_frame => |f| switch (f.typ) {
            .rst_stream => c.rst += 1,
            .goaway => c.goaway += 1,
            else => {},
        },
        .stream_reset => c.stream_reset += 1,
        .connection_error => c.connection_error += 1,
        else => {},
    };
    return c;
}

fn openTestStream(session: *Session, id: u31) !void {
    try session.streams.put(id, .{ .id = id, .state = .half_closed_remote, .window = .{ .send = flow.INITIAL_WINDOW } });
    session.active_streams += 1;
}

test "a stream is reset on the wire once: a second reset runs its local half and queues no frame (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    _ = countTerminalIntents(gpa, &session);
    try openTestStream(&session, 1);

    // The handler's reset, then the slow-consumer sweep's, for the same stream.
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 1, .code = .internal_error } });
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 1, .code = .cancel } });
    const c = countTerminalIntents(gpa, &session);
    try std.testing.expectEqual(@as(usize, 1), c.rst);
    // The local half ran both times: the scheduler and the handler hear of both.
    try std.testing.expectEqual(@as(usize, 2), c.stream_reset);
    try std.testing.expectEqual(@as(usize, 0), session.active_streams);
}

test "a reset of a stream closed in both directions queues no frame, and its local cleanup still runs (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    _ = countTerminalIntents(gpa, &session);
    // Stream 3 ended normally: it has a tombstone and no map entry.
    try session.addTombstone(3, .no_error);
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 3, .code = .cancel } });
    const c = countTerminalIntents(gpa, &session);
    try std.testing.expectEqual(@as(usize, 0), c.rst);
    try std.testing.expectEqual(@as(usize, 1), c.stream_reset);

    // The same for an entry still in the map but closed.
    try session.streams.put(5, .{ .id = 5, .state = .closed, .window = .{ .send = flow.INITIAL_WINDOW } });
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 5, .code = .cancel } });
    const d = countTerminalIntents(gpa, &session);
    try std.testing.expectEqual(@as(usize, 0), d.rst);
    try std.testing.expectEqual(@as(usize, 1), d.stream_reset);
}

test "a refused stream has no map entry, and is reset once because the RST tombstones it (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    _ = countTerminalIntents(gpa, &session);
    try session.refuseStream(7);
    try session.refuseStream(7);
    const c = countTerminalIntents(gpa, &session);
    try std.testing.expectEqual(@as(usize, 1), c.rst);
    try std.testing.expectEqual(@as(usize, 2), c.stream_reset);
    try std.testing.expect(session.isTombstoned(7));
}

test "a delayed reset of a stream whose tombstone was evicted queues no RST, and its local half still runs (t-3562)" {
    const gpa = std.testing.allocator;
    var limits = limits_mod.Limits.defaults;
    limits.stream_tombstones = 2;
    var session = try Session.init(gpa, gpa, limits);
    defer session.deinit();
    _ = countTerminalIntents(gpa, &session);
    // Three streams end normally; the ring keeps the last two.
    for ([_]u31{ 1, 3, 5 }) |sid| {
        try openTestStream(&session, sid);
        try session.applyCommand(.{ .respond_headers = .{ .ticket = .none, .stream_id = sid, .status = 204, .headers = &.{}, .end_stream = true } });
    }
    try std.testing.expect(!session.isTombstoned(1));
    _ = countTerminalIntents(gpa, &session);
    // A handler's reset that arrives late, for the evicted stream 1: the peer
    // counts it closed, and absence is not a refusal.
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 1, .code = .internal_error } });
    const c = countTerminalIntents(gpa, &session);
    try std.testing.expectEqual(@as(usize, 0), c.rst);
    try std.testing.expectEqual(@as(usize, 1), c.stream_reset);
}

test "each GOAWAY role is queued at most once (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    _ = countTerminalIntents(gpa, &session);

    // Graceful phase 1 and phase 2, each asked for twice.
    try session.applyCommand(.graceful_phase1);
    try session.applyCommand(.graceful_phase1);
    try session.applyCommand(.graceful_phase2);
    try session.applyCommand(.graceful_phase2);
    const graceful = countTerminalIntents(gpa, &session);
    try std.testing.expectEqual(@as(usize, 2), graceful.goaway);

    // The connection error, raised twice in one read chunk.
    try session.failConnection(.protocol_error);
    try session.failConnection(.compression_error);
    const final = countTerminalIntents(gpa, &session);
    try std.testing.expectEqual(@as(usize, 1), final.goaway);
    try std.testing.expectEqual(@as(usize, 1), final.connection_error);
    try std.testing.expectEqual(frame.ErrorCode.protocol_error, session.terminal.goaway.code);
}

test "after the connection error an RST is not queued, and its local half still runs (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    _ = countTerminalIntents(gpa, &session);
    try openTestStream(&session, 1);
    try session.failConnection(.protocol_error);
    _ = countTerminalIntents(gpa, &session);
    try session.emitRst(1, .cancel);
    const c = countTerminalIntents(gpa, &session);
    try std.testing.expectEqual(@as(usize, 0), c.rst);
    try std.testing.expectEqual(@as(usize, 1), c.stream_reset);
}

test "stream churn keeps free slots in the stream map" {
    // Every request puts a fresh id and removes it later, so without a rehash
    // std's HashMap fills its free slots with tombstones, and each new-id
    // lookup then probes the whole table before it misses.
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    const live = 64;
    var next: u31 = 1;
    var oldest: u31 = 1;
    var i: usize = 0;
    while (i < 4096) : (i += 1) {
        try session.streams.put(next, .{ .id = next, .state = .half_closed_remote, .window = .{ .send = flow.INITIAL_WINDOW } });
        session.active_streams += 1;
        next += 2;
        if (session.streams.count() > live) {
            try session.applyCommand(.{ .reset_stream = .{ .stream_id = oldest, .code = .internal_error } });
            oldest += 2;
        }
        const intents = session.drainIntents();
        freeTestIntents(gpa, intents);
        try session.ingest(&.{});
        const map = &session.streams.unmanaged;
        var free: usize = 0;
        for (map.metadata.?[0..map.capacity()]) |m| {
            if (m.isFree()) free += 1;
        }
        try std.testing.expect(free > 0);
    }
}

test "session preface and settings ack" {
    var session = try Session.init(std.testing.allocator, std.testing.allocator, .defaults);
    defer session.deinit();
    const intents = session.drainIntents();
    defer {
        for (intents) |*it| {
            switch (it.*) {
                .outbound_frame => |f| std.testing.allocator.free(f.payload),
                else => {},
            }
        }
        // intent_drain scratch — do not free slice
    }
    try std.testing.expect(intents.len >= 2);
}

test "the preface advertises the configured stream limit" {
    var limits = limits_mod.Limits.defaults;
    limits.max_streams_per_connection = 37;
    var session = try Session.init(std.testing.allocator, std.testing.allocator, limits);
    defer session.deinit();
    const intents = session.drainIntents();
    defer freeTestIntents(std.testing.allocator, intents);
    var advertised: ?u32 = null;
    for (intents) |it| switch (it) {
        .outbound_frame => |f| if (f.typ == .settings) {
            // Full wire frame: 9-byte header, then 6-byte (id, value) entries.
            var off: usize = frame.FRAME_HEADER_LEN;
            while (off + 6 <= f.payload.len) : (off += 6) {
                const id = std.mem.readInt(u16, f.payload[off..][0..2], .big);
                if (id == @intFromEnum(frame.SettingId.max_concurrent_streams)) {
                    advertised = std.mem.readInt(u32, f.payload[off + 2 ..][0..4], .big);
                }
            }
        },
        else => {},
    };
    try std.testing.expectEqual(@as(?u32, 37), advertised);
}

test "bad preface" {
    var session = try Session.init(std.testing.allocator, std.testing.allocator, .defaults);
    defer session.deinit();
    const intents = session.drainIntents();
    for (intents) |*it| {
        switch (it.*) {
            .outbound_frame => |f| std.testing.allocator.free(f.payload),
            else => {},
        }
    }
    session.releaseDrainedIntents(intents);
    try session.ingest("GET / HTTP/1.1\r\n");
    try std.testing.expect(session.terminal == .goaway);
    // Drain connection-error intents produced by failure
    const more = session.drainIntents();
    defer {
        for (more) |*it| {
            switch (it.*) {
                .outbound_frame => |f| std.testing.allocator.free(f.payload),
                else => {},
            }
        }
        session.releaseDrainedIntents(more);
    }
}

test "SETTINGS_INITIAL_WINDOW_SIZE negative send window holds queued DATA (real ingest)" {
    const gpa = std.testing.allocator;
    const hpack_mod = @import("hpack.zig");
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
        // intent_drain scratch — do not free slice
    }

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    {
        var sbuf: [64]u8 = undefined;
        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 128 }};
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
        try wire.appendSlice(gpa, sbuf[0..sn]);
    }
    const hdr_fields = [_]hpack_mod.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/x" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack_mod.Encoder.encode(gpa, &hdr_fields);
    defer gpa.free(block);
    {
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = true },
            .stream_id = 1,
        };
        fh.encode(&hdr_buf);
        try wire.appendSlice(gpa, &hdr_buf);
        try wire.appendSlice(gpa, block);
    }
    try session.ingest(wire.items);
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            .dispatch_request => |d| {
                hpack_mod.HeaderField.freeOwnedSlice(gpa, d.headers);
                gpa.free(d.headers);
                if (d.trailers.len != 0) {
                    hpack_mod.HeaderField.freeOwnedSlice(gpa, d.trailers);
                    gpa.free(d.trailers);
                }
                if (d.body.len != 0) gpa.free(d.body);
            },
            else => {},
        };
    }
    try session.applyCommand(.{ .respond_headers = .{ .ticket = .none,
        .stream_id = 1,
        .status = 200,
        .headers = &.{},
        .end_stream = false,
    } });
    {
        const more = session.drainIntents();
        for (more) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }

    // Simulate connection scheduler pending: emit first quantum that fits, hold the rest.
    const payload: [200]u8 = @splat('Q');
    const first = payload[0..128];
    const rest = payload[128..];
    try session.applyCommand(.{ .respond_data = .{
        .stream_id = 1,
        .data = first,
        .end_stream = false,
    } });
    {
        const out = session.drainIntents();
        defer {
            for (out) |*it| switch (it.*) {
                .outbound_frame => |f| gpa.free(f.payload),
                else => {},
            };
            // intent_drain scratch
        }
        var saw = false;
        for (out) |it| switch (it) {
            .outbound_frame => |f| if (f.typ == .data) {
                saw = true;
                try std.testing.expectEqualSlices(u8, first, f.payload[9..][0..first.len]);
            },
            else => {},
        };
        try std.testing.expect(saw);
    }
    var pending: std.ArrayList(u8) = .empty;
    defer pending.deinit(gpa);
    try pending.appendSlice(gpa, rest);
    const pending_end = true;

    // Real SETTINGS_INITIAL_WINDOW_SIZE reduction via ingest (makes send negative).
    {
        var sbuf: [64]u8 = undefined;
        const settings = [_]frame.Setting{.{ .id = .initial_window_size, .value = 1 }};
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &settings);
        try session.ingest(sbuf[0..sn]);
    }
    {
        const ack_out = session.drainIntents();
        for (ack_out) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
        // intent_drain scratch
    }
    const s = session.streams.getPtr(1).?;
    try std.testing.expect(s.window.send < 0);
    try std.testing.expectEqual(@as(i32, 0), session.streamSendAvailable(1));
    try std.testing.expectError(error.FlowBlocked, session.applyCommand(.{ .respond_data = .{
        .stream_id = 1,
        .data = pending.items,
        .end_stream = pending_end,
    } }));
    // Queued scheduler bytes retained byte-identical.
    try std.testing.expectEqualSlices(u8, rest, pending.items);

    // WINDOW_UPDATE restores credit; resume pending → DATA+END_STREAM identical.
    {
        var wbuf: [13]u8 = undefined;
        const wn = try frame.Serializer.windowUpdate(&wbuf, 1, 10_000);
        try session.ingest(wbuf[0..wn]);
    }
    try session.windows.applyConnWindowUpdate(10_000);
    try session.applyCommand(.{ .respond_data = .{
        .stream_id = 1,
        .data = pending.items,
        .end_stream = pending_end,
    } });
    const out = session.drainIntents();
    defer {
        for (out) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
        // intent_drain scratch
    }
    var saw_data = false;
    for (out) |it| switch (it) {
        .outbound_frame => |f| {
            if (f.typ == .data) {
                saw_data = true;
                try std.testing.expect(f.flags.end_stream);
                try std.testing.expectEqualSlices(u8, pending.items, f.payload[9..][0..pending.items.len]);
            }
        },
        else => {},
    };
    try std.testing.expect(saw_data);
}

test "intent capacity: flood fails closed without silent drop" {
    const gpa = std.testing.allocator;
    var limits = limits_mod.Limits.defaults;
    limits.intent_entries_per_connection = 8; // 6 usable + 2 terminal reserve
    var session = try Session.init(gpa, gpa, limits);
    defer session.deinit();
    {
        const boot = session.drainIntents();
        for (boot) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }

    // Mutation canary: each accepted intent carries unique payload bytes.
    var accepted: usize = 0;
    var i: usize = 0;
    while (i < 64) : (i += 1) {
        var buf: [17]u8 = undefined;
        // Synthetic PING frames as cheap owned payloads.
        const ping_data = [_]u8{ @truncate(i), 0, 0, 0, 0, 0, 0, 0 };
        const n = try frame.Serializer.ping(&buf, false, &ping_data);
        const p = try gpa.dupe(u8, buf[0..n]);
        const intent: Intent = .{ .outbound_frame = .{ .typ = .ping, .payload = p } };
        session.pushIntent(intent) catch |err| {
            try std.testing.expectEqual(error.PoolExhausted, err);
            gpa.free(p); // ownership never transferred
            break;
        };
        accepted += 1;
    }
    try std.testing.expect(accepted > 0);
    try std.testing.expect(accepted <= session.intentCapacity() - session.terminal_reserve);
    try std.testing.expectEqual(accepted, session.intentLen());

    // Drain must emit every accepted intent (no silent drop).
    var batch: [16]Intent = undefined;
    const n = session.drainIntentsInto(&batch);
    try std.testing.expectEqual(accepted, n);
    var seen: usize = 0;
    for (batch[0..n]) |*it| switch (it.*) {
        .outbound_frame => |f| {
            try std.testing.expectEqual(frame.FrameType.ping, f.typ);
            seen += 1;
            gpa.free(f.payload);
        },
        else => return error.UnexpectedIntent,
    };
    try std.testing.expectEqual(accepted, seen);
    try std.testing.expectEqual(@as(usize, 0), session.intentLen());

    // Terminal reserve still accepts GOAWAY fail-close after capacity exhaustion.
    i = 0;
    while (i < session.intentCapacity() - session.terminal_reserve) : (i += 1) {
        var buf: [17]u8 = undefined;
        const ping_data: [8]u8 = @splat(0);
        const pn = try frame.Serializer.ping(&buf, false, &ping_data);
        const p = try gpa.dupe(u8, buf[0..pn]);
        try session.pushIntent(.{ .outbound_frame = .{ .typ = .ping, .payload = p } });
    }
    {
        var buf: [17]u8 = undefined;
        const ping_data: [8]u8 = @splat(0);
        const pn = try frame.Serializer.ping(&buf, false, &ping_data);
        const p = try gpa.dupe(u8, buf[0..pn]);
        try std.testing.expectError(error.PoolExhausted, session.pushIntent(.{ .outbound_frame = .{ .typ = .ping, .payload = p } }));
        gpa.free(p);
    }
    try session.failClosedInternal();
    try std.testing.expect(session.terminal != .none);
    {
        const drained = session.drainIntents();
        for (drained) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }
}

test "respond HEADERS allocates only the final frame" {
    var counting = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const gpa = counting.allocator();
    const hpack_mod = @import("hpack.zig");
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            else => {},
        };
    }

    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    {
        var sbuf: [9]u8 = undefined;
        const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
        try wire.appendSlice(gpa, sbuf[0..sn]);
    }
    const req_fields = [_]hpack_mod.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack_mod.Encoder.encode(gpa, &req_fields);
    defer gpa.free(block);
    {
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        const fh = frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_headers = true, .end_stream = true },
            .stream_id = 1,
        };
        fh.encode(&hdr_buf);
        try wire.appendSlice(gpa, &hdr_buf);
        try wire.appendSlice(gpa, block);
    }
    try session.ingest(wire.items);
    {
        const intents = session.drainIntents();
        for (intents) |*it| switch (it.*) {
            .outbound_frame => |f| gpa.free(f.payload),
            .dispatch_request => |d| {
                hpack_mod.HeaderField.freeOwnedSlice(gpa, d.headers);
                gpa.free(d.headers);
                if (d.trailers.len != 0) {
                    hpack_mod.HeaderField.freeOwnedSlice(gpa, d.trailers);
                    gpa.free(d.trailers);
                }
                if (d.body.len != 0) gpa.free(d.body);
            },
            else => {},
        };
    }

    const extra = [_]hpack_mod.HeaderField{.{ .name = "content-type", .value = "text/plain" }};
    const before = counting.allocations;
    try session.applyCommand(.{ .respond_headers = .{ .ticket = .none,
        .stream_id = 1,
        .status = 200,
        .headers = &extra,
        .end_stream = false,
    } });
    try std.testing.expectEqual(@as(usize, 1), counting.allocations - before);
    {
        const more = session.drainIntents();
        try std.testing.expectEqual(@as(usize, 1), more.len);
        switch (more[0]) {
            .outbound_frame => |f| {
                try std.testing.expectEqual(frame.FrameType.headers, f.typ);
                gpa.free(f.payload);
            },
            else => return error.TestUnexpectedResult,
        }
    }
}

fn freeTestIntents(gpa: std.mem.Allocator, intents: []Intent) void {
    for (intents) |*it| switch (it.*) {
        .outbound_frame => |f| gpa.free(f.payload),
        .dispatch_request => |d| {
            hpack.HeaderField.freeOwnedSlice(gpa, d.headers);
            gpa.free(d.headers);
            if (d.trailers.len != 0) {
                hpack.HeaderField.freeOwnedSlice(gpa, d.trailers);
                gpa.free(d.trailers);
            }
            if (d.body.len != 0) gpa.free(d.body);
        },
        else => {},
    };
}

fn ingestPrefaceAndSettings(session: *Session, gpa: std.mem.Allocator) !void {
    freeTestIntents(gpa, session.drainIntents());
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, frame.CLIENT_PREFACE);
    var sbuf: [9]u8 = undefined;
    const sn = try frame.Serializer.settingsFrame(&sbuf, false, &.{});
    try wire.appendSlice(gpa, sbuf[0..sn]);
    try session.ingest(wire.items);
    freeTestIntents(gpa, session.drainIntents());
}

var test_retired_ids: [4]u31 = undefined;
var test_retired_n: usize = 0;

fn testRetireRst(_: *anyopaque, sid: u31) void {
    test_retired_ids[test_retired_n] = sid;
    test_retired_n += 1;
}

fn testAdmitAll(_: *anyopaque) bool {
    return true;
}

fn testNoRelease(_: *anyopaque) void {}

test "a crossed peer RST drops this server's unwritten RST at once, from the intent queue and through the edge, with no tombstone (t-3562)" {
    const gpa = std.testing.allocator;
    var limits = limits_mod.Limits.defaults;
    limits.stream_tombstones = 0;
    var session = try Session.init(gpa, gpa, limits);
    defer session.deinit();
    var ctx: u8 = 0;
    session.stream_hooks = .{ .ctx = &ctx, .tryAdmit = testAdmitAll, .onRelease = testNoRelease, .retireRst = testRetireRst };
    test_retired_n = 0;
    try ingestPrefaceAndSettings(&session, gpa);
    try openTestStream(&session, 1);
    session.highest_peer_stream = 3;
    // The server resets stream 1; its RST is still in the intent queue.
    try session.emitRst(1, .cancel);
    // The peer resets 1 (crossed), then 3 (a stream that ended long ago:
    // with no tombstones kept, nothing remembers it).
    var peer: [frame.rst_stream_frame_bytes]u8 = undefined;
    _ = try frame.Serializer.rstStream(&peer, 1, .cancel);
    try session.ingest(&peer);
    _ = try frame.Serializer.rstStream(&peer, 3, .cancel);
    try session.ingest(&peer);
    const c = countTerminalIntents(gpa, &session);
    // The queued RST is gone; its local reset stays.
    try std.testing.expectEqual(@as(usize, 0), c.rst);
    try std.testing.expectEqual(@as(usize, 1), c.stream_reset);
    // The edge is asked for both ids: it searches its ring by id.
    try std.testing.expectEqual(@as(usize, 2), test_retired_n);
    try std.testing.expectEqual(@as(u31, 1), test_retired_ids[0]);
    try std.testing.expectEqual(@as(u31, 3), test_retired_ids[1]);
    try std.testing.expect(session.terminal == .none);
}

/// The obligation a final HEADERS carries, read off the drained intents.
fn finalHeadersOwes(gpa: std.mem.Allocator, session: *Session, stream_id: u31) !bool {
    const intents = session.drainIntents();
    defer freeTestIntents(gpa, intents);
    for (intents) |it| switch (it) {
        .outbound_frame => |f| if (f.typ == .headers and f.stream_id == stream_id) return f.owes_rst_if_refused,
        else => {},
    };
    return error.NoFinalHeaders;
}

test "a refused final HEADERS is reset from its own obligation, with no tombstones kept (t-3562)" {
    const gpa = std.testing.allocator;
    var limits = limits_mod.Limits.defaults;
    limits.stream_tombstones = 0;
    var session = try Session.init(gpa, gpa, limits);
    defer session.deinit();
    _ = countTerminalIntents(gpa, &session);
    try openTestStream(&session, 1);
    // An empty response: HEADERS with END_STREAM closes the stream here.
    try session.applyCommand(.{ .respond_headers = .{ .ticket = .none, .stream_id = 1, .status = 204, .headers = &.{}, .end_stream = true } });
    try std.testing.expect(!session.streams.contains(1));
    const owes = try finalHeadersOwes(gpa, &session, 1);
    try std.testing.expect(owes);
    // The handler's reset of a stream that ended normally queues no frame.
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 1, .code = .internal_error } });
    try std.testing.expectEqual(@as(usize, 0), countTerminalIntents(gpa, &session).rst);
    // The edge refused that HEADERS: the stream is reset, once (the edge
    // calls this once, for the one frame that carried the obligation).
    try session.resetRefusedHeaders(1, owes);
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 1, .code = .internal_error } });
    const c = countTerminalIntents(gpa, &session);
    try std.testing.expectEqual(@as(usize, 1), c.rst);
    try std.testing.expect(session.terminal == .none);
}

test "a peer RST while the final HEADERS is still queued clears its obligation (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    try ingestPrefaceAndSettings(&session, gpa);
    try openTestStream(&session, 1);
    session.highest_peer_stream = 1;
    try session.applyCommand(.{ .respond_headers = .{ .ticket = .none, .stream_id = 1, .status = 204, .headers = &.{}, .end_stream = true } });
    // The peer cancels the stream before the edge takes the HEADERS.
    var peer: [frame.rst_stream_frame_bytes]u8 = undefined;
    _ = try frame.Serializer.rstStream(&peer, 1, .cancel);
    try session.ingest(&peer);
    const owes = try finalHeadersOwes(gpa, &session, 1);
    try std.testing.expect(!owes);
    // A refusal of it now queues no RST: the peer counts the stream closed.
    try session.resetRefusedHeaders(1, owes);
    try std.testing.expectEqual(@as(usize, 0), countTerminalIntents(gpa, &session).rst);
}

/// A client HEADERS frame (one fragment, END_HEADERS) for the session tests.
fn appendTestRequest(gpa: std.mem.Allocator, wire: *std.ArrayList(u8), stream_id: u31, fields_in: []const hpack.HeaderField, end_stream: bool) !void {
    const block = try hpack.Encoder.encode(gpa, fields_in);
    defer gpa.free(block);
    var hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = end_stream },
        .stream_id = stream_id,
    }).encode(&hdr);
    try wire.appendSlice(gpa, &hdr);
    try wire.appendSlice(gpa, block);
}

/// A POST that declares 16 bytes against a body cap of 8 (`early_413_limits`):
/// answered 413 at its HEADERS, while the peer's side is open. The body it
/// then sends matches what it declared, so its END_STREAM is no error.
const too_large_post = [_]hpack.HeaderField{
    .{ .name = ":method", .value = "POST" },
    .{ .name = ":scheme", .value = "http" },
    .{ .name = ":path", .value = "/" },
    .{ .name = ":authority", .value = "localhost" },
    .{ .name = "content-length", .value = "16" },
};
const too_large_body = "0123456789abcdef";

/// An early 413 is queued while the peer's side is open; the peer's own
/// END_STREAM (`last`, DATA or trailers) in the same read closes the stream
/// and it leaves the map before the edge takes the 413's HEADERS. A refusal
/// of that HEADERS must still reset the stream.
fn expectEarly413OwesAfterPeerEnd(last: enum { data, trailers }) !void {
    const gpa = std.testing.allocator;
    var limits = limits_mod.Limits.defaults;
    limits.request_body_bytes = 8;
    var session = try Session.init(gpa, gpa, limits);
    defer session.deinit();
    try ingestPrefaceAndSettings(&session, gpa);
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try appendTestRequest(gpa, &wire, 1, &too_large_post, false);
    var hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{ .length = too_large_body.len, .type = .data, .flags = .{ .end_stream = last == .data }, .stream_id = 1 }).encode(&hdr);
    try wire.appendSlice(gpa, &hdr);
    try wire.appendSlice(gpa, too_large_body);
    if (last == .trailers) try appendTestRequest(gpa, &wire, 1, &.{.{ .name = "x-trailer", .value = "1" }}, true);
    try session.ingest(wire.items);
    try std.testing.expect(!session.streams.contains(1));
    try std.testing.expect(session.terminal == .none);
    const owes = try finalHeadersOwes(gpa, &session, 1);
    try std.testing.expect(owes);
    try session.resetRefusedHeaders(1, owes);
    try std.testing.expectEqual(@as(usize, 1), countTerminalIntents(gpa, &session).rst);
}

test "an early 413 refused after the peer's DATA(END_STREAM) closed the stream still owes its RST (t-3562)" {
    try expectEarly413OwesAfterPeerEnd(.data);
}

test "an early 413 refused after the peer's trailers closed the stream still owes its RST (t-3562)" {
    try expectEarly413OwesAfterPeerEnd(.trailers);
}

test "a server RST queued after a final HEADERS clears its obligation: one RST, not two (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    _ = countTerminalIntents(gpa, &session);
    // The peer's side is open, so the final HEADERS leaves the stream in the map.
    try session.streams.put(1, .{ .id = 1, .state = .open, .window = .{ .send = flow.INITIAL_WINDOW } });
    session.active_streams += 1;
    try session.applyCommand(.{ .respond_headers = .{ .ticket = .none, .stream_id = 1, .status = 413, .headers = &.{}, .end_stream = true } });
    try std.testing.expect(session.streams.contains(1));
    // The server resets the stream while that HEADERS is still queued.
    try session.applyCommand(.{ .reset_stream = .{ .stream_id = 1, .code = .cancel } });
    try std.testing.expect(!session.streams.contains(1));
    const owes = try finalHeadersOwes(gpa, &session, 1);
    try std.testing.expect(!owes);
    // The refusal of the HEADERS adds nothing to the RST already queued.
    try session.resetRefusedHeaders(1, owes);
    try std.testing.expectEqual(@as(usize, 0), countTerminalIntents(gpa, &session).rst);
}

/// The frames a drained batch holds, in order, as (type, ack, stream,
/// payload) — then frees it.
const SeenFrame = struct { typ: frame.FrameType, ack: bool, sid: u31, body: [8]u8 = @splat(0), inc: u31 = 0 };

fn drainFrames(gpa: std.mem.Allocator, session: *Session, out: []SeenFrame) usize {
    const intents = session.drainIntents();
    defer freeTestIntents(gpa, intents);
    var n: usize = 0;
    for (intents) |it| switch (it) {
        .outbound_frame => |f| {
            var seen: SeenFrame = .{ .typ = f.typ, .ack = f.flags.end_stream, .sid = f.stream_id };
            const body = f.payload[frame.FRAME_HEADER_LEN..];
            if (f.typ == .ping) seen.body = body[0..8].*;
            if (f.typ == .window_update) seen.inc = @intCast(std.mem.readInt(u32, body[0..4], .big) & 0x7fff_ffff);
            out[n] = seen;
            n += 1;
        },
        else => {},
    };
    return n;
}

/// `len` bytes of DATA on `stream_id`, cut into frames of at most 16 KiB.
fn appendTestData(gpa: std.mem.Allocator, wire: *std.ArrayList(u8), stream_id: u31, len: usize) !void {
    var left = len;
    while (left > 0) {
        const take = @min(left, frame.DEFAULT_MAX_FRAME_SIZE);
        var hdr: [frame.FRAME_HEADER_LEN]u8 = undefined;
        (frame.FrameHeader{ .length = @intCast(take), .type = .data, .flags = .{}, .stream_id = stream_id }).encode(&hdr);
        try wire.appendSlice(gpa, &hdr);
        try wire.appendNTimes(gpa, 'd', take);
        left -= take;
    }
}

fn pingFrame(buf: *[17]u8, payload: *const [8]u8) []const u8 {
    _ = frame.Serializer.ping(buf, false, payload) catch unreachable;
    return buf;
}

test "owed acks and credit are queued again in order, coalesced, as room allows (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    try ingestPrefaceAndSettings(&session, gpa);
    // What the edge does with refused frames: two SETTINGS acks, two PING
    // acks, connection credit in two parts.
    try session.oweSettingsAck();
    try session.oweSettingsAck();
    try session.owePingAck("first-pg".*);
    try session.owePingAck("second-p".*);
    session.oweWindowUpdate(0, 1000);
    session.oweWindowUpdate(0, 500);
    // A PING that arrives now joins the owed ones, behind them.
    var pb: [17]u8 = undefined;
    try session.ingest(pingFrame(&pb, "third-pg"));
    var seen: [16]SeenFrame = undefined;
    try std.testing.expectEqual(@as(usize, 0), drainFrames(gpa, &session, &seen));
    try std.testing.expectEqual(@as(usize, 3), session.owed.ping_len);

    // Room for three: the SETTINGS acks first, then the oldest PING ack.
    session.requeueOwed(3);
    var n = drainFrames(gpa, &session, &seen);
    try std.testing.expectEqual(@as(usize, 3), n);
    try std.testing.expect(seen[0].typ == .settings and seen[0].ack);
    try std.testing.expect(seen[1].typ == .settings and seen[1].ack);
    try std.testing.expect(seen[2].typ == .ping and seen[2].ack);
    try std.testing.expectEqualSlices(u8, "first-pg", &seen[2].body);
    try std.testing.expect(session.owed.any());

    // The rest: the PING acks in order, then the credit as one frame.
    session.requeueOwed(16);
    n = drainFrames(gpa, &session, &seen);
    try std.testing.expectEqual(@as(usize, 3), n);
    try std.testing.expectEqualSlices(u8, "second-p", &seen[0].body);
    try std.testing.expectEqualSlices(u8, "third-pg", &seen[1].body);
    try std.testing.expect(seen[2].typ == .window_update and seen[2].sid == 0);
    try std.testing.expectEqual(@as(u31, 1500), seen[2].inc);
    try std.testing.expect(!session.owed.any());
}

test "owed PING acks past the burst end the connection with ENHANCE_YOUR_CALM (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    try ingestPrefaceAndSettings(&session, gpa);
    for (0..rates_mod.PING_BURST) |_| try session.owePingAck("burst-pg".*);
    try std.testing.expect(session.terminal == .none);
    try session.owePingAck("one-more".*);
    try std.testing.expectEqual(frame.ErrorCode.enhance_your_calm, session.terminal.goaway.code);
    // Ending, nothing more is queued for the peer.
    _ = countTerminalIntents(gpa, &session);
    session.requeueOwed(16);
    try std.testing.expectEqual(@as(usize, 0), session.intentLen());
}

test "owed SETTINGS acks past the burst end the connection with ENHANCE_YOUR_CALM (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    try ingestPrefaceAndSettings(&session, gpa);
    for (0..rates_mod.SETTINGS_BURST) |_| try session.oweSettingsAck();
    try std.testing.expect(session.terminal == .none);
    try session.oweSettingsAck();
    try std.testing.expectEqual(frame.ErrorCode.enhance_your_calm, session.terminal.goaway.code);
}

test "owed window credit: DATA is held to what the peer was told, and a stream's owed credit goes with the stream (t-3562)" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    try ingestPrefaceAndSettings(&session, gpa);
    const post = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "http" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":authority", .value = "localhost" },
    };
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try appendTestRequest(gpa, &wire, 1, &post, false);
    try appendTestRequest(gpa, &wire, 3, &post, false);
    try session.ingest(wire.items);
    var seen: [16]SeenFrame = undefined;
    _ = drainFrames(gpa, &session, &seen);

    // Stream 1's half-window WINDOW_UPDATE is refused by the edge.
    const half: usize = flow.INITIAL_WINDOW / 2 + 1;
    wire.clearRetainingCapacity();
    try appendTestData(gpa, &wire, 1, half);
    try session.ingest(wire.items);
    var n = drainFrames(gpa, &session, &seen);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expect(seen[0].typ == .window_update and seen[0].sid == 1);
    session.oweWindowUpdate(1, seen[0].inc);
    // The peer was told INITIAL_WINDOW - half: one byte more on stream 1 is
    // a flow-control error for that stream, even though the server's own
    // window has the credit.
    const rest_len: usize = flow.INITIAL_WINDOW - half;
    wire.clearRetainingCapacity();
    try appendTestData(gpa, &wire, 1, rest_len + 1);
    try session.ingest(wire.items);
    const c = countTerminalIntents(gpa, &session);
    try std.testing.expectEqual(@as(usize, 1), c.rst);
    try std.testing.expect(session.terminal == .none);
    // Stream 1 is gone, and its owed credit with it.
    try std.testing.expect(!session.streams.contains(1));

    // Stream 3: its credit is owed, and the peer sends all it was told. A
    // stream cannot reach a second update while one is owed (that takes more
    // than the peer was told), so nothing new is queued; the requeue sends
    // the owed credit, which reopens the peer's window.
    wire.clearRetainingCapacity();
    try appendTestData(gpa, &wire, 3, half);
    try session.ingest(wire.items);
    n = drainFrames(gpa, &session, &seen);
    try std.testing.expectEqual(@as(usize, 1), n);
    const first_inc = seen[0].inc;
    session.oweWindowUpdate(3, first_inc);
    wire.clearRetainingCapacity();
    try appendTestData(gpa, &wire, 3, rest_len);
    try session.ingest(wire.items);
    n = drainFrames(gpa, &session, &seen);
    for (seen[0..n]) |f| try std.testing.expect(!(f.typ == .window_update and f.sid == 3));
    session.requeueOwed(16);
    n = drainFrames(gpa, &session, &seen);
    var stream3: u31 = 0;
    for (seen[0..n]) |f| {
        if (f.typ == .window_update and f.sid == 3) stream3 += f.inc;
        try std.testing.expect(!(f.typ == .window_update and f.sid == 1));
    }
    try std.testing.expectEqual(first_inc, stream3);
    try std.testing.expect(session.streams.contains(3));
    try std.testing.expectEqual(@as(i32, flow.INITIAL_WINDOW) - @as(i32, @intCast(half + rest_len)) + first_inc, session.streams.get(3).?.window.recv);
    try std.testing.expectEqual(@as(u31, 0), session.streams.get(3).?.window_owed);
}

test "single-frame END_HEADERS does not copy into header_block" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    try ingestPrefaceAndSettings(&session, gpa);

    const hdr_fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":path", .value = "/hello" },
        .{ .name = ":authority", .value = "localhost" },
        .{ .name = "x-custom", .value = "literal-value" },
    };
    const block = try hpack.Encoder.encode(gpa, &hdr_fields);
    defer gpa.free(block);
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = true, .end_stream = true },
        .stream_id = 1,
    }).encode(&hdr_buf);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);

    try session.ingest(wire.items);
    try std.testing.expectEqual(@as(usize, 0), session.header_block.items.len);

    @memset(wire.items, 0xAA);
    const intents = session.drainIntents();
    defer freeTestIntents(gpa, intents);
    var saw = false;
    for (intents) |it| switch (it) {
        .dispatch_request => |d| {
            saw = true;
            try std.testing.expectEqualStrings("/hello", d.path);
            var found_custom = false;
            for (d.headers) |f| {
                if (std.mem.eql(u8, f.name, "x-custom")) {
                    found_custom = true;
                    try std.testing.expectEqualStrings("literal-value", f.value);
                }
            }
            try std.testing.expect(found_custom);
        },
        else => {},
    };
    try std.testing.expect(saw);
}

test "HEADERS without END_HEADERS still assembles in header_block" {
    const gpa = std.testing.allocator;
    var session = try Session.init(gpa, gpa, .defaults);
    defer session.deinit();
    try ingestPrefaceAndSettings(&session, gpa);

    const hdr_fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(gpa, &hdr_fields);
    defer gpa.free(block);
    var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
    (frame.FrameHeader{
        .length = @intCast(block.len),
        .type = .headers,
        .flags = .{ .end_headers = false },
        .stream_id = 1,
    }).encode(&hdr_buf);
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(gpa);
    try wire.appendSlice(gpa, &hdr_buf);
    try wire.appendSlice(gpa, block);
    try session.ingest(wire.items);
    try std.testing.expectEqual(block.len, session.header_block.items.len);
    try std.testing.expectEqual(@as(?u31, 1), session.expect_continuation);
}

test "streams reset mid-body leave no table entry, so the boot-sized tables never fill" {
    // The tables live in a buffer sized to exactly what `planBoot` names, so a
    // table that tried to grow could not: `putBounded` would panic first. A
    // peer that opens a stream, sends body bytes without END_STREAM and resets
    // it does this once per stream id, with no limit on the ids. Before
    // `releaseConcurrency` removed the idle clock, `body_idle_ns` kept one
    // entry per such stream and filled up after a handful.
    const gpa = std.testing.allocator;
    var limits: limits_mod.Limits = .defaults;
    limits.max_streams_per_connection = 4;
    var plan: boot_plan.Plan = .{};
    try Session.planBoot(&plan, limits);
    const buffer = try gpa.alignedAlloc(u8, .fromByteUnits(boot_plan.slot_align), plan.end);
    defer gpa.free(buffer);
    var fba: std.heap.FixedBufferAllocator = .init(buffer);
    var session = try Session.init(fba.allocator(), gpa, limits);
    defer session.releaseRuntime();
    try std.testing.expectEqual(plan.end, fba.end_index);
    try ingestPrefaceAndSettings(&session, gpa);

    const hdr_fields = [_]hpack.HeaderField{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":path", .value = "/upload" },
        .{ .name = ":authority", .value = "localhost" },
    };
    const block = try hpack.Encoder.encode(gpa, &hdr_fields);
    defer gpa.free(block);

    // Many times the capacity of any of the tables.
    var i: u31 = 0;
    while (i < 100) : (i += 1) {
        const sid: u31 = 1 + 2 * i;
        var wire: std.ArrayList(u8) = .empty;
        defer wire.deinit(gpa);
        var hdr_buf: [frame.FRAME_HEADER_LEN]u8 = undefined;
        (frame.FrameHeader{ .length = @intCast(block.len), .type = .headers, .flags = .{ .end_headers = true }, .stream_id = sid }).encode(&hdr_buf);
        try wire.appendSlice(gpa, &hdr_buf);
        try wire.appendSlice(gpa, block);
        (frame.FrameHeader{ .length = 1, .type = .data, .flags = .{}, .stream_id = sid }).encode(&hdr_buf);
        try wire.appendSlice(gpa, &hdr_buf);
        try wire.append(gpa, 'x');
        var rst: [13]u8 = undefined;
        const rn = try frame.Serializer.rstStream(&rst, sid, .cancel);
        try wire.appendSlice(gpa, rst[0..rn]);
        session.edge_now_ns = 1_000_000 + i;
        try session.ingest(wire.items);
        freeTestIntents(gpa, session.drainIntents());

        try std.testing.expectEqual(@as(u32, 0), session.streams.count());
        try std.testing.expectEqual(@as(u32, 0), session.pending_bodies.count());
        try std.testing.expectEqual(@as(u32, 0), session.pending_headers.count());
        try std.testing.expectEqual(@as(u32, 0), session.body_idle_ns.count());
    }
}
