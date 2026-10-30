//! Checked Limits.resourceUpperBound from concrete `@sizeOf` shapes + configured capacities.
//!
//! Why this file exists at all: an operator must be able to answer "how much
//! memory can this configuration use?" BEFORE the process serves a request, and
//! must get the answer from the code rather than from a measurement of one run.
//! `resourceUpperBound` is that answer. `Server.init` calls it first and refuses
//! to start on `error.InvalidConfig`, so an inconsistent configuration fails at
//! boot instead of under load.
//!
//! Two properties keep the number honest:
//!
//! - Every term uses a real `@sizeOf` of a real type, never an estimate. The
//!   comptime block below fails the BUILD when a struct grows and its recorded
//!   size no longer matches. A term that silently undercounts is worse than no
//!   bound, because it still answers.
//! - The checked arithmetic is not defensive style. A large configured limit
//!   multiplied by a struct size can overflow `usize`, and a wrapped product is
//!   a small, plausible, wrong bound.
//!
//! `Terms` is public so a test can assert that a term moves when the limit that
//! feeds it moves. That is the mutation canary against a term that gets dropped
//! from the sum during a refactor: the total would still look reasonable.
const std = @import("std");
const bound = @import("bound_shapes.zig");
const wire_const = @import("wire_const.zig");
const boot_plan = @import("boot_plan.zig");
const connection = @import("../edge/connection.zig");
const conn_slot = @import("../edge/conn_slot.zig");
const request_pool = @import("../edge/request_pool.zig");
const hpack = @import("hpack.zig");
const frame = @import("frame.zig");

/// How an accepted socket is served. `h1c` is plain HTTP/1.1 and takes no
/// connection slot.
pub const ConnMode = enum { h2c, tls, h1c };

pub const ResourceUpperBound = struct {
    allocator_bytes: usize,
    /// Task execution storage belongs to the application-selected std.Io backend.
    committed_stack_bytes: usize,
    /// Task execution storage belongs to the application-selected std.Io backend.
    virtual_stack_bytes: usize,
    /// Exposed for mutation canaries / term accounting.
    terms: Terms = .{},
};

pub const Terms = struct {
    /// One connection slot's boot buffer, `slotBootBytes`, for the larger of
    /// the two HTTP/2 modes. Everything a connection allocates at boot is in
    /// this one term: the arrays, both slab pools, the scheduler and Session
    /// tables, the TLS buffers.
    slot_boot: usize = 0,
    on_demand_conn: usize = 0,
    on_demand_server: usize = 0,
    /// The request memory pool, counted once: the most RESIDENT bytes it can
    /// hold (`request_bytes_per_server`; a block that is decommitted is not
    /// resident) plus its bitmaps. The address space it reserves is about
    /// `class_count` times that, and is not counted: untouched pages are not
    /// memory. The per-connection share, `request_bytes_per_connection`, is a
    /// slice of this term and adds nothing to it.
    request_pool: usize = 0,
    reaper_jobs: usize = 0,
    /// The slot objects themselves (each holds a whole `Connection`) plus the
    /// two lists the pool keeps, the free list and the quarantine, each sized
    /// to `max_connections`. The boot buffer of each slot is `slot_boot`.
    conn_slots: usize = 0,
    routes: usize = 0,
    certs: usize = 0,
    tls_scratch: usize = 0,
    endpoints_listeners: usize = 0,
    /// The server-wide outbound slab pool, counted once.
    outbound_slabs: usize = 0,
    /// Concurrent brotli contexts × per-context budget, counted once server-wide.
    compression_contexts: usize = 0,
    tasks: usize = 0,
    h1_head: usize = 0,
    h1_body: usize = 0,
};

/// Must match `edge.connection.ReaperJob` — comptime-asserted in connection.zig.
pub const REAPER_JOB_SIZE: usize = 40;

pub const WIRE_CHUNK_SIZE = wire_const.WIRE_CHUNK_SIZE;
pub const TLS_PLAINTEXT_SCRATCH_SIZE = wire_const.TLS_PLAINTEXT_SCRATCH_SIZE;
pub const TLS_CONN_BUFFER_BYTES = wire_const.TLS_CONN_BUFFER_BYTES;
pub const TLS_CIPHER_CHUNK_SIZE = wire_const.TLS_CIPHER_CHUNK_SIZE;
/// Smallest h2c write queue: the room one scheduler pop is reserved, a pop's
/// pushes plus the drain's tail push. The h2c queue is
/// `inbound_wire_chunks_per_connection` slots with no extra; TLS adds
/// `TLS_STASH_EXTRA_SLOTS` to its stash, so a 1-chunk TLS setting is legal and
/// this bound does not apply to it.
pub const H2C_MIN_WRITE_QUEUE_CHUNKS: usize = wire_const.POP_PUSHES + wire_const.TLS_STASH_TAIL_SLOTS;
/// Smallest write pool. A scheduler pop needs `POP_PUSHES` free chunks
/// (`Connection.outboundMemoryPaused`), and a frame queued by a handler or by
/// ingest may take a chunk only while more than that many are free, so a pool
/// of exactly `POP_PUSHES` would never lend one and a smaller pool would never
/// pop. Every mode: the pool is `inbound_wire_chunks_per_connection` chunks.
pub const MIN_WRITE_POOL_CHUNKS: usize = wire_const.POP_PUSHES + 1;
/// The most frames one response header block may be cut into. `cmdHeaders`
/// builds them all or none, so it keeps their buffers in a fixed array.
pub const MAX_HEADER_FRAGMENTS: usize = 8;
/// The GOAWAYs one connection may queue: graceful phase 1, graceful phase 2,
/// and the final one that ends the connection (`Session.beginGracefulPhase1`,
/// `beginGracefulPhase2`, `failConnectionWith`). Each is queued at most once.
pub const TERMINAL_GOAWAYS: usize = 3;
/// Intent slots only a GOAWAY or the connection error may take
/// (`Session.pushIntent`), so a connection that fills its intent queue can
/// still say why it closes.
pub const TERMINAL_INTENT_RESERVE: usize = 2;
/// The free ordinary intent slots `Session.emitRst` wants before it queues an
/// RST: its own two intents (the frame and the local reset), then the local
/// reset of the connection error that ends a flood. Below this, the queue is
/// full because the peer is past the bound, and the RST is answered with
/// ENHANCE_YOUR_CALM instead.
pub const RST_INTENT_ROOM: usize = 5;
/// The most intents one inbound frame adds, the connection error it may end
/// with included. The largest case is a DATA frame that takes its stream past
/// the body cap: the `early_reject` intent and its response HEADERS (at most
/// `MAX_HEADER_FRAGMENTS` frames), then the stream and the connection
/// WINDOW_UPDATE, then the two terminal intents of a connection error (the
/// rate limit is charged after the frame is handled). Every other frame adds
/// less: a refused or reset stream two, an admitted request one, a SETTINGS or
/// PING its ack, a peer RST_STREAM its local reset. `Session.ingestSome`
/// asserts the bound after every frame.
pub const INTENTS_PER_FRAME_MAX: usize = 1 + MAX_HEADER_FRAGMENTS + 2 + TERMINAL_INTENT_RESERVE;
/// The free ordinary intent slots `Session.ingestSome` wants before it takes
/// the next frame: one frame's worth, and after it the room `emitRst` checks
/// for. With less, ingest stops at the frame boundary and the edge
/// materializes the queue before it goes on, so a read chunk never fills the
/// queue, whatever it holds.
pub const INGEST_FRAME_ROOM: usize = INTENTS_PER_FRAME_MAX + RST_INTENT_ROOM;

/// Lower bound on `compression_context_bytes` for a quality/window pair.
///
/// With chunked `compressAll` (≤64 KiB PROCESS steps), encoder memory is
/// window-bounded: ring ≈ 2^lgwin plus quality-scaled hashers and multi-flush
/// residency. It must NOT scale with body size. Kept in limits so boot
/// validation and operators share one floor.
pub fn minBrotliContextBytes(window_bits: u8, quality: u8) usize {
    const window_bytes: usize = @as(usize, 1) << @intCast(window_bits);
    // 4× window covers ring + copy + headroom; quality adds hasher tables;
    // 1 MiB fixed slack covers multi-flush SSE residency (q5/w19 ~2.2 MiB peak).
    const q: usize = quality;
    return window_bytes * 4 + q * 128 * 1024 + 1024 * 1024;
}

comptime {
    // `terminalBytes` counts a GOAWAY as `goaway_frame_bytes`, which has no
    // debug data. If the server ever sends debug data, T's bytes and the
    // terminal store's slots are too small and this must change with it.
    if (frame.goaway_frame_bytes != frame.FRAME_HEADER_LEN + 8) @compileError("GOAWAY carries debug data: terminal byte room and the terminal store slot size are derived without it");
}

pub const Limits = struct {
    max_endpoints: usize = 8,
    max_routes: usize = 256,
    max_route_path_bytes: usize = 64 * 1024,
    max_connections: usize = 1_024,
    max_streams_per_connection: usize = 256,
    max_streams_per_server: usize = 4_096,
    inbound_wire_chunks_per_connection: usize = 8,
    /// Boot-reserved outbound frame slabs per connection. A frame larger than
    /// `frame_slab_bytes` takes a write chunk instead (so does a small one when
    /// every slab is rented), and when no chunk can be spared the frame is
    /// refused.
    frame_slab_bytes: usize = 512,
    frame_slabs_per_connection: usize = 256,
    /// DATA payload bytes one stream may have waiting in the scheduler. A
    /// handler that writes more waits for the stream's slab to drain.
    outbound_bytes_per_stream: usize = 64 * 1024,
    /// DATA payload bytes one connection holds, counted once, from a handler's
    /// write until the write ack of the chunk that carried them. Frame
    /// headers, HEADERS and control frames are not counted: the control pool,
    /// the frame pool and the write pool bound them. A write that finds this
    /// full WAITS (backpressure, first come first served) and resumes when an
    /// ack returns room; it is not refused. A complete (inline) handler takes
    /// its place in the same queue, for one stream's cap, and the actor runs it
    /// when it is served. The budget must hold the
    /// whole write pool and one stream's cap (checked at boot), so the bytes
    /// on the wire alone can never fill it.
    outbound_bytes_per_connection: usize = 4 * 1024 * 1024,
    /// The same bytes over every connection. Unlike the connection budget this
    /// one REFUSES: when it is exhausted no release on the writer's own
    /// connection makes room, so waiting could not end. The write fails and its
    /// stream is reset (overload shedding). The server's slab pool, which holds
    /// the bytes waiting in the scheduler, is refused the same way.
    outbound_bytes_per_server: usize = 256 * 1024 * 1024,
    /// The most CLASS bytes (see `request_pool`) one connection's requests
    /// hold at once, headers and bodies and what handlers allocate. One
    /// connection must not be able to take the whole pool. 64 MiB lets a
    /// connection run 16 concurrent requests that each render 1 MiB bodies
    /// (about 3 MiB each) with room to spare.
    request_bytes_per_connection: usize = 64 * 1024 * 1024,
    /// The most RESIDENT bytes the request pool holds, in use or free. See
    /// `request_pool.RequestPool` for what counts and what is reclaimed.
    request_bytes_per_server: usize = 256 * 1024 * 1024,
    /// The most class bytes ONE request holds: its headers, its body and
    /// everything its handler allocates from `Request.arena`. Over it, the
    /// allocation fails with `OutOfMemory` and the request gets a 500 (an
    /// RST_STREAM once its response is committed). A power of two, at least
    /// 64 KiB, and large enough to buffer a body of `request_body_bytes`
    /// (`bodyArenaBytes`). It is also the largest block class.
    request_arena_bytes: usize = 4 * 1024 * 1024,
    /// The most FREE resident bytes the request pool keeps (resident minus
    /// held). A block returned while the pool is over this is decommitted at
    /// once, so a burst of large renders does not leave its blocks resident
    /// until the next burst. Above the limit a request pays one madvise and
    /// fresh page faults for a block, which is what `munmap` cost for every
    /// buffer above 32 KiB before the pool; below it, reuse is free. At most
    /// `request_bytes_per_server`.
    request_pool_retain_bytes: usize = 64 * 1024 * 1024,
    /// Reserved HTTP/1.1 request-head size per connection. A head over this
    /// bound is 431; the buffer never grows. Each connection allocates this
    /// size three times: `head_buf`, `name_scratch`, and the head prefix of
    /// `carry_buf`.
    h1_head_bytes: usize = 16 * 1024,
    /// The ORDINARY control-frame share of one connection (SETTINGS acks, PING
    /// acks, WINDOW_UPDATE, HEADERS). Terminal frames (RST_STREAM, GOAWAY)
    /// have a room of their own on top of it, derived from
    /// `max_streams_per_connection` (`terminalFrames`, `terminalBytes`), so
    /// these two numbers never shrink the room a connection has to end itself.
    control_bytes_per_connection: usize = 64 * 1024,
    control_entries_per_connection: usize = 256,
    stream_tombstones: usize = 1_024,
    concurrent_tls_handshakes: usize = 128,
    /// Kernel accept-queue length requested from listen(2) for every endpoint.
    /// zio and std.Io both default to 128, which a connection burst overruns
    /// before the accept loop runs: the kernel drops handshake packets for
    /// a full queue, so the client waits out a TCP retransmit (1 s and up),
    /// and a client with a connect deadline gives up. The kernel silently caps
    /// the request at its own limit (Linux `net.core.somaxconn`, macOS
    /// `kern.ipc.somaxconn`), so asking for more than it allows is not an
    /// error; it only means the operator's sysctl is the ceiling.
    ///
    /// A longer queue only helps when the admission limits can take what it
    /// holds: the accept loop closes a connection over `max_connections` or
    /// `concurrent_tls_handshakes` instead of waiting for a slot. A TLS burst
    /// bigger than `concurrent_tls_handshakes` needs that limit raised too.
    ///
    /// Not part of `resourceUpperBound`: the queue is kernel memory, and every
    /// queued connection still passes `max_connections` and
    /// `concurrent_tls_handshakes` when it is accepted.
    listen_backlog: u31 = 4_096,
    certificate_chain_bytes: usize = 64 * 1024,
    private_key_bytes: usize = 16 * 1024,
    tls_handshake_scratch_bytes: usize = 256 * 1024,
    cancellation_reaper_tasks: usize = 64,
    cancellation_reaper_jobs: usize = 4_096,
    decoded_header_bytes: usize = 32 * 1024,
    compressed_header_bytes: usize = 64 * 1024,
    header_fields: usize = 100,
    /// The most header fields a response may carry, `:status` included, and the
    /// most bytes of its encoded header block. `Session.cmdHeaders` checks both
    /// before it encodes anything; a response over either is refused before a
    /// byte is queued, and the handler's request is answered 500. The block is
    /// cut into frames of 16 KiB, each in a write chunk, so the bytes bound is
    /// at most `MAX_HEADER_FRAGMENTS` of them. Whether a block that size can
    /// be sent at a given moment is up to the write pool (a frame it cannot lend
    /// a chunk for refuses the response the same way).
    response_header_fields: usize = 100,
    response_header_bytes: usize = 32 * 1024,
    continuation_frames: usize = 16,
    regular_field_value_bytes: usize = 8 * 1024,
    path_wire_bytes: usize = 24 * 1024,
    path_component_bytes: usize = 16 * 1024,
    query_component_bytes: usize = 16 * 1024,
    /// Per-stream decoded body cap (413). Each H1 connection reserves this
    /// size twice (`body_buf` and the body suffix of `carry_buf`). A new
    /// value moves `terms.h1_body` and `allocator_bytes`. An H2 body lives in
    /// the request's arena and counts against `request_arena_bytes`, which
    /// must be large enough for it.
    request_body_bytes: usize = 256 * 1024,
    /// Outbound DATA slabs for the WHOLE SERVER, each `outbound_bytes_per_stream`
    /// bytes. Slabs are rented when a stream first queues DATA and returned when
    /// it drains, so this bounds CONCURRENT WRITERS rather than connections.
    /// The old per-connection reservation cost `max_streams_per_connection`
    /// slabs on every connection — 16 MiB each at the defaults — whether the
    /// connection wrote one byte or saturated every stream.
    outbound_slabs_per_server: usize = 4_096,
    /// Intent queue entries per connection (session). At least
    /// `intentEntriesMin`, which boot checks.
    intent_entries_per_connection: usize = 520,
    /// Concurrent live brotli encoder contexts server-wide. Each context is
    /// rented for the lifetime of one compressed response (including an SSE
    /// stream) and returned when that response finishes or aborts.
    compression_contexts_per_server: usize = 128,
    /// Per-encoder allocation budget routed through brotli's custom allocator
    /// hooks. Exhaustion before headers commit → identity; after commit → RST.
    ///
    /// Default is 4 MiB (brief sketched 2 MiB). With chunked PROCESS steps,
    /// peak is window-bounded (~2.2 MiB multi-flush at q5/w19), not body-sized.
    /// Built with `-DBROTLI_ENCODER_CLEANUP_ON_OOM` so a null alloc returns
    /// BROTLI_FALSE instead of exit(1). Boot rejects a budget below
    /// `minBrotliContextBytes`.
    compression_context_bytes: usize = 4 * 1024 * 1024,
    /// Full-body path only: bodies shorter than this stay identity.
    compression_min_bytes: usize = 256,
    /// Brotli quality 0–11. Rejected at boot if out of range.
    compression_quality: u8 = 5,
    /// Brotli lgwin 10–24. Rejected at boot if out of range or if the window
    /// cannot fit inside `compression_context_bytes`.
    compression_window_bits: u8 = 19,
    /// Master switch. Default off so a consumer that never asked for compression
    /// does not pay for encoder contexts or Vary surface area.
    response_compression: bool = false,
    preface_timeout_ns: u64 = 5 * std.time.ns_per_s,
    field_block_timeout_ns: u64 = 5 * std.time.ns_per_s,
    request_body_idle_timeout_ns: u64 = 15 * std.time.ns_per_s,
    slow_consumer_timeout_ns: u64 = 30 * std.time.ns_per_s,
    graceful_drain_timeout_ns: u64 = 30 * std.time.ns_per_s,
    /// How long a connection that has decided to close (a connection error, or
    /// the end of a graceful drain) waits for its last queued frames, the
    /// GOAWAY above all, to reach the socket (RFC 9113 section 5.4.1). A frame
    /// counts as written when the socket accepted it, not when the peer read it,
    /// so a peer that reads at all finishes this in well under a millisecond on
    /// a LAN. The deadline only matters when the socket buffer is full because
    /// the peer stopped reading: it then bounds how long that peer can hold the
    /// connection after it has already been told to go away. 500 ms is long
    /// enough for a few round trips to a slow reader and short enough that
    /// provoking a connection error from a peer that never reads is not a way
    /// to hold a connection slot. Holds no memory: the frames were already
    /// counted when they were queued. Must be non-zero.
    close_flush_timeout_ns: u64 = 500 * std.time.ns_per_ms,
    /// How late a handler's `waitUntil` may return. 0, the default, wakes
    /// each handler at its own deadline. A non-zero value rounds every
    /// handler deadline UP to the next multiple of it on the awake clock, so
    /// handlers whose deadlines fall in the same window wake on one timer
    /// pass instead of one wake each. That is the trade for many streams
    /// that each tick on their own timer (SSE): fewer wakes per event, for up
    /// to this much added latency on each timed wait, plus the time the
    /// handlers woken together spend waiting for each other.
    ///
    /// Only the timed wait moves. A handler woken by I/O or by another task
    /// is not affected, and a handler already past its own deadline returns
    /// at once instead of waiting for the next multiple.
    handler_deadline_slack_ns: u64 = 0,

    pub const defaults: Limits = .{};

    /// The deadline a handler's timed wait arms: `deadline_ns` rounded up to
    /// the next multiple of `handler_deadline_slack_ns`. Saturates instead of
    /// wrapping, so a deadline near the end of the clock never wraps to an
    /// early one.
    pub fn handlerDeadlineNs(self: Limits, deadline_ns: u64) u64 {
        const slack = self.handler_deadline_slack_ns;
        if (slack == 0) return deadline_ns;
        const over = deadline_ns % slack;
        if (over == 0) return deadline_ns;
        return deadline_ns +| (slack - over);
    }

    fn checkedMul(a: usize, b: usize) error{InvalidConfig}!usize {
        const r = @mulWithOverflow(a, b);
        if (r[1] != 0) return error.InvalidConfig;
        return r[0];
    }

    fn checkedAdd(a: usize, b: usize) error{InvalidConfig}!usize {
        const r = @addWithOverflow(a, b);
        if (r[1] != 0) return error.InvalidConfig;
        return r[0];
    }

    /// Boot check for a server or connection that runs h2c. An h2c connection
    /// holds a drain back before every pop that lacks the room one pop is
    /// reserved (`H2C_MIN_WRITE_QUEUE_CHUNKS`). A queue smaller than that never
    /// has the room, so no pop could ever run. Not part of `resourceUpperBound`
    /// because that is mode-blind and TLS legally uses a smaller value.
    pub fn checkH2cWriteQueue(self: Limits) error{InvalidConfig}!void {
        if (self.inbound_wire_chunks_per_connection < H2C_MIN_WRITE_QUEUE_CHUNKS) return error.InvalidConfig;
    }

    /// T: the most terminal frames (RST_STREAM and GOAWAY) a well-behaved
    /// peer's connection can need unwritten at once.
    ///
    /// A well-behaved peer never has more than `max_streams_per_connection`
    /// (M) streams open in its own view, and an unwritten RST_STREAM is for a
    /// stream the peer still counts as open, because the RST is what closes
    /// it. `Session.emitRst` queues at most one RST per stream, so M RSTs are
    /// the most that can wait. Add the `TERMINAL_GOAWAYS` GOAWAYs and
    /// T = M + 3. This is the one number: the terminal ring, the control
    /// pool's terminal share and the terminal frame store all hold T, and
    /// `Connection.init` refuses to boot if they differ. More than T is a peer
    /// that is not well-behaved.
    pub fn terminalFrames(self: Limits) error{InvalidConfig}!usize {
        return checkedAdd(self.max_streams_per_connection, TERMINAL_GOAWAYS);
    }

    /// The control pool bytes for T terminal frames: M RST_STREAMs and three
    /// GOAWAYs. A GOAWAY carries no debug data in this server, which the
    /// comptime check below keeps true.
    pub fn terminalBytes(self: Limits) error{InvalidConfig}!usize {
        return checkedAdd(
            try checkedMul(self.max_streams_per_connection, frame.rst_stream_frame_bytes),
            TERMINAL_GOAWAYS * frame.goaway_frame_bytes,
        );
    }

    /// The control pool's whole room: the ordinary share plus the terminal
    /// share. Ordinary frames may use only the first part.
    pub fn controlPoolEntries(self: Limits) error{InvalidConfig}!usize {
        return checkedAdd(self.control_entries_per_connection, try self.terminalFrames());
    }

    pub fn controlPoolBytes(self: Limits) error{InvalidConfig}!usize {
        return checkedAdd(self.control_bytes_per_connection, try self.terminalBytes());
    }

    /// The fewest intents the session queue must hold, so that a well-behaved
    /// peer is never taken for a flood (`Session.emitRst` answers a nearly
    /// full queue with ENHANCE_YOUR_CALM) and never fails closed on a full
    /// queue.
    ///
    /// The queue fills between two drains, and the edge drains at the end of
    /// every lock section that pushes, so it must hold the largest single
    /// batch, from empty:
    ///
    /// - A read chunk does not count, whatever it holds: `Session.ingestSome`
    ///   stops at a frame boundary while fewer than `INGEST_FRAME_ROOM` slots
    ///   are free, and the edge drains before it goes on. The queue only has
    ///   to take one frame and the RST room after it.
    /// - A reset of every live stream at once: the graceful finish
    ///   (`Connection.finishGraceful`), the request body idle sweep
    ///   (`Session.checkIdleDeadlines`), or the dispatch refusals of a batch
    ///   (a failed handler spawn, a refused reaper reservation), which
    ///   `Connection.materializeIntents` drains in the same call. Each stream
    ///   is reset once in such a batch, because a reset takes it out of the
    ///   map and every one of these walks the map (a dispatch of a stream
    ///   that has already left it is dropped, not refused). Two intents per
    ///   stream (the RST and its local reset): 2M. The last RST still wants
    ///   `RST_INTENT_ROOM` free, and the terminal reserve sits on top.
    /// - A handler's command: at most one response header block
    ///   (`MAX_HEADER_FRAGMENTS`), less than one frame's worth.
    pub fn intentEntriesMin(self: Limits) error{InvalidConfig}!usize {
        const resets = try checkedAdd(try checkedMul(self.max_streams_per_connection, 2), RST_INTENT_ROOM);
        return checkedAdd(@max(resets, INGEST_FRAME_ROOM), TERMINAL_INTENT_RESERVE);
    }

    /// Boot check for the intent queue (`intentEntriesMin`).
    pub fn checkIntentQueue(self: Limits) error{InvalidConfig}!void {
        if (self.intent_entries_per_connection < try self.intentEntriesMin()) return error.InvalidConfig;
    }

    /// Boot check for the frame pool. A connection queues its two preface
    /// frames (SETTINGS, WINDOW_UPDATE) at birth, and a slab holds the biggest
    /// control frame (the preface SETTINGS, 45 bytes; sized by encoded frames,
    /// not by their 9-byte header), because a control frame that does not fit
    /// a slab needs a write chunk, and the chunks are not reserved for them.
    /// Terminal frames do not use this pool (`Connection.terminal_store`).
    pub fn checkFramePool(self: Limits) error{InvalidConfig}!void {
        if (self.frame_slab_bytes < frame.max_control_frame_bytes) return error.InvalidConfig;
        if (self.frame_slabs_per_connection < 2) return error.InvalidConfig;
    }

    /// The size of a connection slot's boot buffer for `mode`: exactly the
    /// bytes `Connection.init` takes from it (and, for TLS, the `Conn` taken
    /// before it), including alignment padding. One byte less and the boot
    /// fails with OutOfMemory; one byte more is memory every slot pays and
    /// never uses. The exact-fit test in `edge/connection.zig` boots into both. `resourceUpperBound`
    /// counts the slot from this same function.
    pub fn slotBootBytes(self: Limits, mode: ConnMode) error{InvalidConfig}!usize {
        if (mode == .h1c) return 0;
        var plan: boot_plan.Plan = .{};
        try connection.planSlotBoot(&plan, self, mode);
        return plan.end;
    }

    /// The most a header-block decode takes from a request's arena (see
    /// `hpack.Decoder.requestBytesBound`). A request that holds this much free
    /// room cannot fail to decode.
    pub fn hpackDecodeBytes(self: Limits) error{InvalidConfig}!usize {
        return hpack.Decoder.requestBytesBound(self.header_fields, self.decoded_header_bytes) catch error.InvalidConfig;
    }

    /// The slot's HPACK scratch: room for the largest decode as the whole
    /// request memory of a request the pool refused (`hpackDecodeBytes`), and
    /// beside it the decoder's transient room for one field that enters the
    /// table past the request's budget (at most `MAX_DYNAMIC_TABLE` bytes).
    pub fn hpackScratchBytes(self: Limits) error{InvalidConfig}!usize {
        return checkedAdd(try self.hpackDecodeBytes(), hpack.MAX_DYNAMIC_TABLE);
    }

    /// The size of a request's first block: the smallest class that holds the
    /// block header and one decode (`hpackDecodeBytes`).
    pub fn firstBlockBytes(self: Limits) error{InvalidConfig}!usize {
        const need = try checkedAdd(request_pool.header_bytes, try self.hpackDecodeBytes());
        const rounded = std.math.ceilPowerOfTwo(usize, need) catch return error.InvalidConfig;
        return @max(rounded, request_pool.min_block_bytes);
    }

    /// The smallest `request_arena_bytes` that can buffer a body of exactly
    /// `request_body_bytes`.
    ///
    /// The body is an `ArrayList` appended to as DATA arrives. At its last
    /// growth step the request holds its first block, the block the list is
    /// leaving and the block it is entering. The list grows by a half plus 64
    /// bytes and a block takes a 64-byte header, so with `p` the class of
    /// `body + header`, the block left is at most `p` and the block entered at
    /// most `2p`. A test (`the largest legal body fits the arena bound`)
    /// appends a body of exactly that size and checks the peak.
    pub fn bodyArenaBytes(self: Limits) error{InvalidConfig}!usize {
        const p = std.math.ceilPowerOfTwo(usize, try checkedAdd(self.request_body_bytes, request_pool.header_bytes)) catch return error.InvalidConfig;
        const need = try checkedAdd(try self.firstBlockBytes(), try checkedMul(p, 3));
        return std.math.ceilPowerOfTwo(usize, need) catch error.InvalidConfig;
    }

    pub fn requestPoolConfig(self: Limits) request_pool.Config {
        return .{ .server_bytes = self.request_bytes_per_server, .arena_bytes = self.request_arena_bytes, .retain_bytes = self.request_pool_retain_bytes };
    }

    pub fn resourceUpperBound(self: Limits) error{InvalidConfig}!ResourceUpperBound {
        if (self.max_connections == 0 or self.max_streams_per_connection == 0) return error.InvalidConfig;
        if (self.max_streams_per_server == 0) return error.InvalidConfig;
        if (self.cancellation_reaper_tasks == 0 or self.cancellation_reaper_jobs == 0) return error.InvalidConfig;
        // Every live *task* stream may need a reaper job at once, because a peer
        // may reset all of them in one flight. Complete oneshots have no join
        // handle and do not take a token; the bound is still max_streams because
        // a config cannot promise the mix. A smaller reaper queue would reject a
        // job that the cancel path has no way to retry, which strands the
        // handler slot and its capacity token.
        if (self.cancellation_reaper_jobs < self.max_streams_per_server) return error.InvalidConfig;
        if (self.inbound_wire_chunks_per_connection < MIN_WRITE_POOL_CHUNKS) return error.InvalidConfig;
        if (self.response_header_fields == 0 or self.response_header_bytes == 0) return error.InvalidConfig;
        if (self.response_header_bytes > MAX_HEADER_FRAGMENTS * frame.DEFAULT_MAX_FRAME_SIZE) return error.InvalidConfig;
        if (self.outbound_bytes_per_stream == 0 or self.outbound_bytes_per_connection == 0) return error.InvalidConfig;
        // The three ceilings must nest: stream <= connection <= server. An
        // inverted pair makes the inner limit unreachable, so a handler would
        // park forever on capacity that the outer limit already refused.
        if (self.outbound_bytes_per_stream > self.outbound_bytes_per_connection) return error.InvalidConfig;
        if (self.outbound_bytes_per_connection > self.outbound_bytes_per_server) return error.InvalidConfig;
        // A handler whose write finds the connection budget full PARKS; it
        // does not fail. That is only safe if a release can always come, and
        // a release comes from a write ack or a slow-consumer kill. Wire bytes
        // are acked, and they are bounded by the write pool: no more than
        // `inbound_wire_chunks_per_connection` chunks of `WIRE_CHUNK_SIZE`
        // exist, so the wire side cannot be the whole budget. The pending side
        // has the slow-consumer deadline. The budget therefore has to hold the
        // whole write pool AND one stream's cap that a parked writer needs,
        // or "all of the budget is on the wire, and the wire is waiting for
        // the writer" would be a state, and nothing would end it.
        const write_pool_bytes = try checkedMul(self.inbound_wire_chunks_per_connection, WIRE_CHUNK_SIZE);
        if (try checkedAdd(write_pool_bytes, self.outbound_bytes_per_stream) > self.outbound_bytes_per_connection) return error.InvalidConfig;
        if (self.request_bytes_per_connection > self.request_bytes_per_server) return error.InvalidConfig;
        // The request pool's own shape: the arena cap is a power of two of at
        // least one block, and the server cap holds at least one request.
        try self.requestPoolConfig().check();
        // A request must be able to hold its first block, and a body of the
        // largest legal size, or the limits promise what the pool refuses.
        const first_block = try self.firstBlockBytes();
        if (self.request_arena_bytes < try self.bodyArenaBytes()) return error.InvalidConfig;
        // A connection that cannot hold one first block refuses every request.
        if (self.request_bytes_per_connection < first_block) return error.InvalidConfig;
        if (first_block > self.request_arena_bytes) return error.InvalidConfig;
        // Ordinary SETTINGS/HEADERS must fit without consuming the terminal
        // GOAWAY/RST room, which is derived (`terminalFrames`) and added on top
        // of these two numbers, so they only have to be above zero.
        if (self.control_bytes_per_connection == 0) return error.InvalidConfig;
        if (self.control_entries_per_connection == 0) return error.InvalidConfig;
        _ = try self.controlPoolEntries();
        _ = try self.controlPoolBytes();
        // Zero would close before the GOAWAY is written, the defect this limit exists to remove.
        if (self.close_flush_timeout_ns == 0) return error.InvalidConfig;
        if (self.concurrent_tls_handshakes == 0) return error.InvalidConfig;
        if (self.listen_backlog == 0) return error.InvalidConfig;
        try self.checkFramePool();
        try self.checkIntentQueue();
        // A server with no slabs can never write DATA.
        if (self.outbound_slabs_per_server == 0) return error.InvalidConfig;
        // Every admitted stream can hold at most one slab, and global stream
        // admission already caps concurrent streams. Requiring at least that
        // many slabs makes pool exhaustion UNREACHABLE for a valid config, and
        // that is what removes the deadlock this design would otherwise have:
        // a writer that cannot rent would have to park until some OTHER
        // connection released, which nothing on this connection can wake.
        // Rung 4 — the dangerous configuration cannot be expressed.
        if (self.outbound_slabs_per_server < self.max_streams_per_server) return error.InvalidConfig;
        if (self.compression_quality > 11) return error.InvalidConfig;
        if (self.compression_window_bits < 10 or self.compression_window_bits > 24) return error.InvalidConfig;
        // Reject budgets that cannot cover window-bounded peak for this quality/window.
        // CLEANUP_ON_OOM turns a null alloc into EncoderFailed, but a config that
        // cannot open a context for a normal response would identity-fallback
        // every request — fail that at boot instead.
        if (self.compression_context_bytes < minBrotliContextBytes(self.compression_window_bits, self.compression_quality)) {
            return error.InvalidConfig;
        }
        if (self.response_compression and self.compression_contexts_per_server == 0) return error.InvalidConfig;
        if (self.h1_head_bytes == 0) return error.InvalidConfig;

        var terms: Terms = .{};

        // One slot per connection, sized for whichever HTTP/2 mode is larger
        // because the bound does not know which endpoints are TLS. h1c takes
        // no slot (its buffers are the `h1_*` terms below).
        const slot_h2c = try self.slotBootBytes(.h2c);
        const slot_tls = try self.slotBootBytes(.tls);
        terms.slot_boot = @max(slot_h2c, slot_tls);
        // Runtime ceilings that are not part of the slot: the per-connection
        // request, outbound and control budgets.
        // The request budget is a share of the server pool and costs nothing
        // extra.
        terms.on_demand_conn = try checkedAdd(self.outbound_bytes_per_connection, try self.controlPoolBytes());
        // head_buf, name_scratch, and the head prefix of carry_buf (init in edge/h1.zig).
        terms.h1_head = try checkedMul(self.max_connections, try checkedMul(self.h1_head_bytes, 3));
        // body_buf plus the body-sized suffix of carry_buf (init in edge/h1.zig).
        terms.h1_body = try checkedMul(self.max_connections, try checkedMul(self.request_body_bytes, 2));
        const h1_per_conn = try checkedAdd(try checkedMul(self.h1_head_bytes, 3), try checkedMul(self.request_body_bytes, 2));
        const per_conn = try checkedAdd(terms.slot_boot, try checkedAdd(terms.on_demand_conn, h1_per_conn));

        terms.routes = try checkedAdd(
            self.max_route_path_bytes,
            try checkedMul(self.max_routes, bound.ROUTE_SIZE),
        );
        terms.certs = try checkedAdd(self.certificate_chain_bytes, self.private_key_bytes);
        terms.reaper_jobs = try checkedMul(self.cancellation_reaper_jobs, REAPER_JOB_SIZE);
        terms.conn_slots = try checkedMul(
            self.max_connections,
            try checkedAdd(@sizeOf(conn_slot.ConnSlot), 2 * @sizeOf(*conn_slot.ConnSlot)),
        );
        terms.tls_scratch = try checkedMul(self.concurrent_tls_handshakes, self.tls_handshake_scratch_bytes);
        const endpoint_slot = try checkedAdd(
            bound.ENDPOINT_CONFIG_SIZE,
            try checkedAdd(bound.LISTENER_SIZE, bound.ENDPOINT_ADDRESS_SIZE),
        );
        terms.endpoints_listeners = try checkedMul(self.max_endpoints, endpoint_slot);
        const server_static = try checkedAdd(terms.routes, try checkedAdd(terms.certs, try checkedAdd(terms.reaper_jobs, try checkedAdd(terms.conn_slots, terms.endpoints_listeners))));

        const all_conns = try checkedMul(self.max_connections, per_conn);
        // Global on-demand ceilings counted once (admission counters + payload pools).
        const slab_bytes = bound.pendingSlabBytes(self.outbound_slabs_per_server, self.outbound_bytes_per_stream) catch return error.InvalidConfig;
        terms.outbound_slabs = slab_bytes;
        terms.compression_contexts = try checkedMul(self.compression_contexts_per_server, self.compression_context_bytes);
        terms.on_demand_server = try checkedAdd(slab_bytes, self.outbound_bytes_per_server);
        terms.request_pool = try checkedAdd(self.request_bytes_per_server, try self.requestPoolConfig().bitmapBytes());
        const allocator_bytes = try checkedAdd(
            server_static,
            try checkedAdd(terms.tls_scratch, try checkedAdd(all_conns, try checkedAdd(terms.on_demand_server, try checkedAdd(terms.request_pool, terms.compression_contexts)))),
        );

        // Maximum std.Io task topology: accept loops + per-connection
        // actor/read/write/ack and three Select wait branches + handlers + reapers.
        const per_conn_tasks: usize = 7;
        terms.tasks = try checkedAdd(
            try checkedAdd(self.max_endpoints, try checkedMul(self.max_connections, per_conn_tasks)),
            try checkedAdd(self.max_streams_per_server, self.cancellation_reaper_tasks),
        );
        // Backend task storage is intentionally outside starh2's allocator bound.
        const committed_stack_bytes: usize = 0;
        const virtual_stack_bytes: usize = 0;

        return .{
            .allocator_bytes = allocator_bytes,
            .committed_stack_bytes = committed_stack_bytes,
            .virtual_stack_bytes = virtual_stack_bytes,
            .terms = terms,
        };
    }
};

test "mutation canary: the slot term moves with each limit that sizes a boot table" {
    const base = try Limits.defaults.resourceUpperBound();
    try std.testing.expect(base.terms.slot_boot > 0);
    // Every limit below sizes something `Connection.init` allocates at boot.
    // A table that dropped out of the plan would leave its limit unable to
    // move the slot, and the bound would still look reasonable.
    inline for (.{
        "frame_slabs_per_connection",
        "frame_slab_bytes",
        "inbound_wire_chunks_per_connection",
        "max_streams_per_connection",
        "control_entries_per_connection",
        "intent_entries_per_connection",
        "stream_tombstones",
        "compressed_header_bytes",
    }) |name| {
        var more = Limits.defaults;
        @field(more, name) = @field(Limits.defaults, name) * 2;
        // The reaper queue must stay at least the global stream cap, and the
        // global cap at least the per-connection one.
        more.max_streams_per_server = @max(more.max_streams_per_server, more.max_streams_per_connection);
        more.cancellation_reaper_jobs = @max(more.cancellation_reaper_jobs, more.max_streams_per_server);
        more.outbound_slabs_per_server = @max(more.outbound_slabs_per_server, more.max_streams_per_server);
        // The intent queue must hold a reset of every live stream.
        more.intent_entries_per_connection = @max(more.intent_entries_per_connection, try more.intentEntriesMin());
        const bigger = try more.resourceUpperBound();
        try std.testing.expect(bigger.terms.slot_boot > base.terms.slot_boot);
        try std.testing.expect(bigger.allocator_bytes > base.allocator_bytes);
    }
}

test "the intent queue holds a reset of every live stream, and boot refuses less (t-3562)" {
    var lim = Limits.defaults;
    try lim.checkIntentQueue();
    // 2 per stream, the room the last RST checks for, and the terminal reserve.
    try std.testing.expectEqual(2 * lim.max_streams_per_connection + RST_INTENT_ROOM + TERMINAL_INTENT_RESERVE, try lim.intentEntriesMin());
    lim.intent_entries_per_connection = (try lim.intentEntriesMin()) - 1;
    try std.testing.expectError(error.InvalidConfig, lim.checkIntentQueue());
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
    // M = 1024 does not fit the default queue.
    lim = Limits.defaults;
    lim.max_streams_per_connection = 1024;
    try std.testing.expectError(error.InvalidConfig, lim.checkIntentQueue());
    // A tiny M still needs one frame's room for ingest to make progress.
    lim = Limits.defaults;
    lim.max_streams_per_connection = 1;
    try std.testing.expectEqual(INGEST_FRAME_ROOM + TERMINAL_INTENT_RESERVE, try lim.intentEntriesMin());
}

test "handler deadline slack rounds up to its multiple and never wraps" {
    var lim = Limits.defaults;
    try std.testing.expectEqual(@as(u64, 1_234_567), lim.handlerDeadlineNs(1_234_567));

    lim.handler_deadline_slack_ns = 1_000_000;
    try std.testing.expectEqual(@as(u64, 2_000_000), lim.handlerDeadlineNs(1_000_001));
    try std.testing.expectEqual(@as(u64, 2_000_000), lim.handlerDeadlineNs(1_999_999));
    try std.testing.expectEqual(@as(u64, 2_000_000), lim.handlerDeadlineNs(2_000_000));
    try std.testing.expectEqual(@as(u64, 0), lim.handlerDeadlineNs(0));
    const max = std.math.maxInt(u64);
    try std.testing.expectEqual(max, lim.handlerDeadlineNs(max - 1));
}

test "defaults resource bound" {
    const b = try Limits.defaults.resourceUpperBound();
    try std.testing.expect(b.allocator_bytes > 0);
    try std.testing.expect(b.terms.slot_boot > 0);
    try std.testing.expect(b.terms.tasks > 0);
}

test "rejects inconsistent outbound hierarchy" {
    var lim = Limits.defaults;
    lim.outbound_bytes_per_stream = 8 * 1024 * 1024;
    lim.outbound_bytes_per_connection = 4 * 1024 * 1024;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "rejects a slab pool smaller than the global stream cap" {
    // Without this the pool can be exhausted, and an exhausted writer has
    // nothing on its own connection that can wake it.
    var lim = Limits.defaults;
    lim.outbound_slabs_per_server = lim.max_streams_per_server - 1;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());

    lim.outbound_slabs_per_server = 0;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "the slab pool is counted once for the server, not per connection" {
    // The regression this whole change exists to prevent: if slabs are counted
    // per connection, halving max_connections halves the slab cost, which means
    // the term is in the wrong place.
    const wide = try Limits.defaults.resourceUpperBound();
    var half = Limits.defaults;
    half.max_connections = Limits.defaults.max_connections / 2;
    const narrow = try half.resourceUpperBound();
    try std.testing.expectEqual(wide.terms.outbound_slabs, narrow.terms.outbound_slabs);
    try std.testing.expect(wide.terms.outbound_slabs > 0);
    // And it does track the slab count itself.
    var more = Limits.defaults;
    more.outbound_slabs_per_server = Limits.defaults.outbound_slabs_per_server * 2;
    const bigger = try more.resourceUpperBound();
    try std.testing.expect(bigger.terms.outbound_slabs > wide.terms.outbound_slabs);
}

test "rejects reaper jobs below global streams" {
    var lim = Limits.defaults;
    lim.max_streams_per_server = 100;
    lim.cancellation_reaper_jobs = 50;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "mutation canary: dropping the slot term shrinks the bound" {
    const b = try Limits.defaults.resourceUpperBound();
    try std.testing.expect(b.terms.slot_boot > 0);
    try std.testing.expect(b.terms.on_demand_server > 0);
    var tiny = Limits.defaults;
    tiny.max_streams_per_connection = 1;
    tiny.max_streams_per_server = 64;
    tiny.cancellation_reaper_jobs = 64;
    const b2 = try tiny.resourceUpperBound();
    try std.testing.expect(b.terms.slot_boot > b2.terms.slot_boot);
    try std.testing.expect(b.allocator_bytes > b2.allocator_bytes);
}

test "the slot is counted per connection, so halving connections halves it" {
    const wide = try Limits.defaults.resourceUpperBound();
    var half = Limits.defaults;
    half.max_connections = Limits.defaults.max_connections / 2;
    const narrow = try half.resourceUpperBound();
    try std.testing.expectEqual(wide.terms.slot_boot, narrow.terms.slot_boot);
    try std.testing.expect(wide.allocator_bytes > narrow.allocator_bytes);
}

test "mutation canary: compression_contexts term moves with the limit" {
    const base = try Limits.defaults.resourceUpperBound();
    try std.testing.expect(base.terms.compression_contexts > 0);
    var more = Limits.defaults;
    more.compression_contexts_per_server = Limits.defaults.compression_contexts_per_server * 2;
    const bigger = try more.resourceUpperBound();
    try std.testing.expect(bigger.terms.compression_contexts > base.terms.compression_contexts);
    try std.testing.expect(bigger.allocator_bytes > base.allocator_bytes);
    var less = Limits.defaults;
    // Stay above minBrotliContextBytes(default window, default quality) (~3.6 MiB).
    less.compression_context_bytes = Limits.defaults.compression_context_bytes - 64 * 1024;
    const smaller = try less.resourceUpperBound();
    try std.testing.expect(smaller.terms.compression_contexts < base.terms.compression_contexts);
}

test "rejects compression quality/window out of range" {
    var lim = Limits.defaults;
    lim.compression_quality = 12;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
    lim = Limits.defaults;
    lim.compression_window_bits = 9;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
    lim = Limits.defaults;
    lim.compression_window_bits = 25;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "rejects window larger than compression context budget" {
    var lim = Limits.defaults;
    lim.compression_window_bits = 22; // 4 MiB window
    lim.compression_context_bytes = 1 * 1024 * 1024;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "rejects response_compression with zero contexts" {
    var lim = Limits.defaults;
    lim.response_compression = true;
    lim.compression_contexts_per_server = 0;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "h1_head term counts three head-sized regions" {
    const per = Limits.defaults.h1_head_bytes;
    const n = Limits.defaults.max_connections;
    const base = try Limits.defaults.resourceUpperBound();
    try std.testing.expectEqual(n * per * 3, base.terms.h1_head);
    var more = Limits.defaults;
    more.h1_head_bytes = per * 2;
    const bigger = try more.resourceUpperBound();
    try std.testing.expectEqual(n * per * 2 * 3, bigger.terms.h1_head);
    try std.testing.expectEqual(base.allocator_bytes + n * per * 3, bigger.allocator_bytes);
}

test "h1_body term moves allocator_bytes with request_body_bytes" {
    const base = try Limits.defaults.resourceUpperBound();
    var more = Limits.defaults;
    more.request_body_bytes = Limits.defaults.request_body_bytes * 2;
    const bigger = try more.resourceUpperBound();
    try std.testing.expect(bigger.terms.h1_body > base.terms.h1_body);
    try std.testing.expect(bigger.allocator_bytes > base.allocator_bytes);
}

test "rejects a zero listen backlog" {
    var l = Limits.defaults;
    l.listen_backlog = 0;
    try std.testing.expectError(error.InvalidConfig, l.resourceUpperBound());
}

test "rejects zero h1_head_bytes" {
    var lim = Limits.defaults;
    lim.h1_head_bytes = 0;
    try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
}

test "a TLS slot is larger than an h2c slot by the handshake Conn and the TLS buffers" {
    // The TLS slot holds what h2c does, plus the handshake `Conn` and the
    // driver's read buffer and write stash. The bound takes the larger.
    const lim = Limits.defaults;
    const h2c_bytes = try lim.slotBootBytes(.h2c);
    const tls_bytes = try lim.slotBootBytes(.tls);
    try std.testing.expect(tls_bytes > h2c_bytes);
    try std.testing.expect(tls_bytes - h2c_bytes >= @sizeOf(@import("../edge/tls.zig").Conn) + wire_const.TLS_CIPHER_CHUNK_SIZE);
    try std.testing.expectEqual(@as(usize, 0), try lim.slotBootBytes(.h1c));
    const b = try lim.resourceUpperBound();
    try std.testing.expectEqual(tls_bytes, b.terms.slot_boot);
}

test "extreme limits are InvalidConfig, never a panic or a small wrong size" {
    // Each limit is an operator's number. The plan adds and multiplies them,
    // so a value at the top of `usize` used to overflow inside the plan, which
    // `resourceUpperBound` reaches before it can say InvalidConfig.
    const top = std.math.maxInt(usize);
    const past_u32: usize = std.math.maxInt(u32) + 1;
    inline for (.{
        "inbound_wire_chunks_per_connection",
        "max_streams_per_connection",
        "control_entries_per_connection",
        "intent_entries_per_connection",
        "stream_tombstones",
        "frame_slabs_per_connection",
        "frame_slab_bytes",
        "compressed_header_bytes",
    }) |name| {
        for ([_]usize{ top, top - 1, top / 2 + 1 }) |v| {
            var lim = Limits.defaults;
            @field(lim, name) = v;
            // A size may be huge but must never be small: every unit of every
            // one of these limits is at least a byte of the plan.
            inline for (.{ ConnMode.h2c, ConnMode.tls }) |mode| {
                if (lim.slotBootBytes(mode)) |n| {
                    try std.testing.expect(n >= v);
                } else |err| {
                    try std.testing.expectEqual(error.InvalidConfig, err);
                }
            }
            try std.testing.expectError(error.InvalidConfig, lim.resourceUpperBound());
        }
    }
    // The counts `init` narrows to `u32` are refused one past it, where the
    // byte sizes alone would still fit a `usize`.
    inline for (.{ "inbound_wire_chunks_per_connection", "max_streams_per_connection" }) |name| {
        var lim = Limits.defaults;
        @field(lim, name) = past_u32;
        try std.testing.expectError(error.InvalidConfig, lim.slotBootBytes(.h2c));
        try std.testing.expectError(error.InvalidConfig, lim.slotBootBytes(.tls));
    }
    // A plan whose end lands within an alignment of the top of `usize`: the
    // header block is the last Session table, so sizing it to leave k bytes
    // below the top puts the next table's alignment step on the edge.
    const session_mod = @import("session.zig");
    var probe: boot_plan.Plan = .{};
    var small = Limits.defaults;
    small.compressed_header_bytes = 1;
    try session_mod.Session.planBoot(&probe, small);
    // The block is `max(one frame, compressed_header_bytes)` bytes and has
    // alignment 1, so everything before it ends where the block starts.
    const block_start = probe.end - @import("frame.zig").DEFAULT_MAX_FRAME_SIZE;
    for (0..70) |k| {
        var edge = Limits.defaults;
        edge.compressed_header_bytes = top - block_start - k;
        try std.testing.expectError(error.InvalidConfig, edge.slotBootBytes(.h2c));
        try std.testing.expectError(error.InvalidConfig, edge.slotBootBytes(.tls));
    }
    // A sum of two limits that overflows only as a sum.
    var sum = Limits.defaults;
    sum.control_entries_per_connection = top;
    sum.max_streams_per_connection = 1;
    try std.testing.expectError(error.InvalidConfig, sum.slotBootBytes(.h2c));
}

test "the request pool is counted once, and the per-connection share costs nothing" {
    const base = try Limits.defaults.resourceUpperBound();
    try std.testing.expect(base.terms.request_pool >= Limits.defaults.request_bytes_per_server);

    // Not per connection.
    var half = Limits.defaults;
    half.max_connections = Limits.defaults.max_connections / 2;
    try std.testing.expectEqual(base.terms.request_pool, (try half.resourceUpperBound()).terms.request_pool);

    // It tracks the server cap, by exactly the bytes moved plus the bitmaps.
    var more = Limits.defaults;
    more.request_bytes_per_server = Limits.defaults.request_bytes_per_server * 2;
    const bigger = try more.resourceUpperBound();
    const bitmaps_before = try Limits.defaults.requestPoolConfig().bitmapBytes();
    const bitmaps_after = try more.requestPoolConfig().bitmapBytes();
    try std.testing.expectEqual(
        base.terms.request_pool + Limits.defaults.request_bytes_per_server + (bitmaps_after - bitmaps_before),
        bigger.terms.request_pool,
    );
    try std.testing.expect(bigger.allocator_bytes > base.allocator_bytes);

    // The connection's share is a slice of the pool: it does not move the bound.
    var share = Limits.defaults;
    share.request_bytes_per_connection = Limits.defaults.request_bytes_per_connection / 2;
    try std.testing.expectEqual(base.allocator_bytes, (try share.resourceUpperBound()).allocator_bytes);
}

test "the request pool's limits that cannot work are refused at boot" {
    const d = Limits.defaults;
    var l = d;
    // The arena cap: a power of two of at least one block.
    l.request_arena_bytes = 48 * 1024;
    try std.testing.expectError(error.InvalidConfig, l.resourceUpperBound());
    l.request_arena_bytes = 3 * 1024 * 1024;
    try std.testing.expectError(error.InvalidConfig, l.resourceUpperBound());
    // The server cap holds at least one request.
    l = d;
    l.request_bytes_per_server = d.request_arena_bytes / 2;
    l.request_bytes_per_connection = l.request_bytes_per_server;
    try std.testing.expectError(error.InvalidConfig, l.resourceUpperBound());
    // A connection holds at least one first block.
    l = d;
    l.request_bytes_per_connection = 32 * 1024;
    try std.testing.expectError(error.InvalidConfig, l.resourceUpperBound());
    // The connection's share is part of the server's.
    l = d;
    l.request_bytes_per_connection = d.request_bytes_per_server + 1;
    try std.testing.expectError(error.InvalidConfig, l.resourceUpperBound());
    // A body the arena cannot buffer.
    l = d;
    l.request_body_bytes = 2 * 1024 * 1024;
    try std.testing.expectError(error.InvalidConfig, l.resourceUpperBound());
    l.request_arena_bytes = try l.bodyArenaBytes();
    _ = try l.resourceUpperBound();
    // A decode that cannot fit the largest block.
    l = d;
    l.decoded_header_bytes = 8 * 1024 * 1024;
    try std.testing.expectError(error.InvalidConfig, l.resourceUpperBound());
    // The defaults: one first block is the smallest class.
    try std.testing.expectEqual(@as(usize, 64 * 1024), try d.firstBlockBytes());
}

test "the largest legal body fits the arena bound" {
    // The bound `bodyArenaBytes` rests on a claim about how an ArrayList grows
    // through the arena. Check it by doing it: for several body sizes, take the
    // first block a decode reserves, append the body in DATA-frame sized pieces
    // to exactly `request_body_bytes`, and check nothing was refused and the
    // peak fits.
    for ([_]usize{ 1024, 16 * 1024, 64 * 1024, 100_000, 256 * 1024, 1024 * 1024, 2 * 1024 * 1024 }) |body_bytes| {
        var lim = Limits.defaults;
        lim.request_body_bytes = body_bytes;
        lim.request_arena_bytes = try lim.bodyArenaBytes();
        lim.request_bytes_per_server = @max(lim.request_bytes_per_server, lim.request_arena_bytes);
        lim.request_bytes_per_connection = lim.request_bytes_per_server;
        _ = try lim.resourceUpperBound();
        var pool = try request_pool.RequestPool.init(lim.requestPoolConfig());
        defer pool.deinit();
        var budget = request_pool.ConnBudget.init(lim.request_bytes_per_connection);
        var arena = request_pool.Arena.init(&pool, &budget);
        defer arena.release();
        const al = arena.allocator();
        try arena.reserve(try lim.hpackDecodeBytes());
        // What a decode leaves behind: part of the reserved room used.
        _ = try al.alloc(u8, 3000);
        var body: std.ArrayList(u8) = .empty;
        const piece: [16 * 1024]u8 = @splat('b');
        var left = body_bytes;
        while (left > 0) {
            const take = @min(left, piece.len);
            try body.appendSlice(al, piece[0..take]);
            left -= take;
        }
        try std.testing.expectEqual(body_bytes, body.items.len);
        try std.testing.expectEqual(@as(?request_pool.Reason, null), arena.refusal);
        try std.testing.expect(arena.peak <= lim.request_arena_bytes);
        // The cap is not wildly loose either: half of it is not enough for the
        // larger bodies, or the formula would be paying for nothing.
        if (body_bytes >= 256 * 1024) try std.testing.expect(arena.peak > lim.request_arena_bytes / 4);
    }
}
