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
const ticket_table = @import("../edge/ticket_table.zig");
const wire_pump = @import("../edge/wire_pump.zig");

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
    handlers: usize = 0,
    joins: usize = 0,
    tickets: usize = 0,
    write_acks: usize = 0,
    wire_descs: usize = 0,
    read_payload: usize = 0,
    stream_maps: usize = 0,
    pending_maps: usize = 0,
    on_demand_conn: usize = 0,
    on_demand_server: usize = 0,
    reaper_jobs: usize = 0,
    /// Retained for source compatibility; std.Io.Group removed the heap slot table.
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
};

/// Must match `edge.connection.HandlerSlot` — comptime-asserted in connection.zig.
pub const HANDLER_SLOT_SIZE: usize = 20;
/// Must match `edge.connection.ReaperJob` — comptime-asserted in connection.zig.
pub const REAPER_JOB_SIZE: usize = 40;

pub const WIRE_CHUNK_SIZE = wire_const.WIRE_CHUNK_SIZE;
pub const TLS_PLAINTEXT_SCRATCH_SIZE = wire_const.TLS_PLAINTEXT_SCRATCH_SIZE;
pub const TERMINAL_CONTROL_RESERVE_BYTES: usize = 4 * 1024;
pub const TERMINAL_CONTROL_RESERVE_ENTRIES: usize = 16;

/// Lower bound on `compression_context_bytes` for a quality/window pair.
/// Derived from measured libbrotli peaks (ring ≈ 2^lgwin, plus quality-scaled
/// hash tables and multi-flush residency). Kept in limits so boot validation
/// and the encoder wrapper share one floor.
pub fn minBrotliContextBytes(window_bits: u8, quality: u8) usize {
    const window_bytes: usize = @as(usize, 1) << @intCast(window_bits);
    // 4× window covers ring + copy + headroom; quality adds hasher tables;
    // 1 MiB fixed slack covers streaming flush residency beyond the one-shot
    // estimate (multi-flush q5/w19 measured ~2.2 MiB vs ~1 MiB base).
    const q: usize = quality;
    return window_bytes * 4 + q * 128 * 1024 + 1024 * 1024;
}

pub const Limits = struct {
    max_endpoints: usize = 8,
    max_routes: usize = 256,
    max_route_path_bytes: usize = 64 * 1024,
    max_connections: usize = 1_024,
    max_streams_per_connection: usize = 256,
    max_streams_per_server: usize = 4_096,
    inbound_wire_chunks_per_connection: usize = 8,
    outbound_bytes_per_stream: usize = 64 * 1024,
    outbound_bytes_per_connection: usize = 4 * 1024 * 1024,
    outbound_bytes_per_server: usize = 256 * 1024 * 1024,
    request_bytes_per_connection: usize = 8 * 1024 * 1024,
    request_bytes_per_server: usize = 256 * 1024 * 1024,
    control_bytes_per_connection: usize = 64 * 1024,
    control_entries_per_connection: usize = 256,
    stream_tombstones: usize = 1_024,
    concurrent_tls_handshakes: usize = 128,
    certificate_chain_bytes: usize = 64 * 1024,
    private_key_bytes: usize = 16 * 1024,
    tls_handshake_scratch_bytes: usize = 256 * 1024,
    /// TLS recv accumulator soft cap per connection (on-demand).
    tls_recv_acc_bytes: usize = 256 * 1024,
    cancellation_reaper_tasks: usize = 64,
    cancellation_reaper_jobs: usize = 4_096,
    decoded_header_bytes: usize = 32 * 1024,
    compressed_header_bytes: usize = 64 * 1024,
    header_fields: usize = 100,
    continuation_frames: usize = 16,
    regular_field_value_bytes: usize = 8 * 1024,
    path_wire_bytes: usize = 24 * 1024,
    path_component_bytes: usize = 16 * 1024,
    query_component_bytes: usize = 16 * 1024,
    /// Per-stream decoded body cap (413 threshold). Does not by itself grow
    /// `resourceUpperBound`: concurrent body residency is gated by
    /// `request_bytes_per_connection` / `request_bytes_per_server`. Raising this
    /// to B with S streams requires `request_bytes_per_connection >= B*S` (and a
    /// matching server ceiling) if every stream may hold a full body at once.
    request_body_bytes: usize = 256 * 1024,
    /// Outbound DATA slabs for the WHOLE SERVER, each `outbound_bytes_per_stream`
    /// bytes. Slabs are rented when a stream first queues DATA and returned when
    /// it drains, so this bounds CONCURRENT WRITERS rather than connections.
    /// The old per-connection reservation cost `max_streams_per_connection`
    /// slabs on every connection — 16 MiB each at the defaults — whether the
    /// connection wrote one byte or saturated every stream.
    outbound_slabs_per_server: usize = 4_096,
    /// Intent backlog entries per connection (session).
    intent_entries_per_connection: usize = 512,
    /// Concurrent live brotli encoder contexts server-wide. Each context is
    /// rented for the lifetime of one compressed response (including an SSE
    /// stream) and returned when that response finishes or aborts.
    compression_contexts_per_server: usize = 128,
    /// Per-encoder allocation budget routed through brotli's custom allocator
    /// hooks. Exhaustion before headers commit → identity; after commit → RST.
    ///
    /// Default is 4 MiB, not the brief's 2 MiB sketch: measured peak for
    /// quality 5 / window 19 is ~3.1 MiB on a 64 KiB one-shot and ~2.2 MiB on
    /// multi-flush SSE. libbrotli can spin forever when a custom alloc returns
    /// NULL mid-stream, so the budget must cover peak rather than relying on
    /// OOM as a failure path. Boot rejects a budget below `minBrotliContextBytes`.
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

    pub const defaults: Limits = .{};

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

    pub fn resourceUpperBound(self: Limits) error{InvalidConfig}!ResourceUpperBound {
        if (self.max_connections == 0 or self.max_streams_per_connection == 0) return error.InvalidConfig;
        if (self.max_streams_per_server == 0) return error.InvalidConfig;
        if (self.cancellation_reaper_tasks == 0 or self.cancellation_reaper_jobs == 0) return error.InvalidConfig;
        // Every live stream may need a reaper job at once, because a peer may
        // reset all of them in one flight. A smaller reaper queue would reject a
        // job that the cancel path has no way to retry, which strands the
        // handler slot and its capacity token.
        if (self.cancellation_reaper_jobs < self.max_streams_per_server) return error.InvalidConfig;
        if (self.inbound_wire_chunks_per_connection == 0) return error.InvalidConfig;
        if (self.outbound_bytes_per_stream == 0 or self.outbound_bytes_per_connection == 0) return error.InvalidConfig;
        // The three ceilings must nest: stream <= connection <= server. An
        // inverted pair makes the inner limit unreachable, so a handler would
        // park forever on capacity that the outer limit already refused.
        if (self.outbound_bytes_per_stream > self.outbound_bytes_per_connection) return error.InvalidConfig;
        if (self.outbound_bytes_per_connection > self.outbound_bytes_per_server) return error.InvalidConfig;
        if (self.request_bytes_per_connection > self.request_bytes_per_server) return error.InvalidConfig;
        // Ordinary SETTINGS/HEADERS must fit without consuming the terminal
        // GOAWAY/RST/ACK reserve. Without a reserve, a connection that fills its
        // control pool cannot emit the GOAWAY that says why, so it fails silent
        // instead of fail-closed.
        if (self.control_bytes_per_connection <= TERMINAL_CONTROL_RESERVE_BYTES) return error.InvalidConfig;
        if (self.control_entries_per_connection <= TERMINAL_CONTROL_RESERVE_ENTRIES) return error.InvalidConfig;
        if (self.concurrent_tls_handshakes == 0) return error.InvalidConfig;
        if (self.tls_recv_acc_bytes < WIRE_CHUNK_SIZE) return error.InvalidConfig;
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
        // Reject budgets that cannot cover measured peak for this quality/window.
        // A too-small budget does not fail closed under libbrotli — it can hang
        // inside CompressStream when the custom alloc returns NULL.
        if (self.compression_context_bytes < minBrotliContextBytes(self.compression_window_bits, self.compression_quality)) {
            return error.InvalidConfig;
        }
        if (self.response_compression and self.compression_contexts_per_server == 0) return error.InvalidConfig;

        // Rung-4 enforcement: these sizes are load-bearing inputs to the bound,
        // so a struct that gains a field must break the BUILD and not the
        // number. A drifted size is invisible in a passing test suite.
        comptime {
            if (@sizeOf(ticket_table.TicketWait) != bound.TICKET_WAIT_SIZE) @compileError("TicketWait size drift");
            if (@sizeOf(wire_pump.WriteCompletion) != bound.WRITE_COMPLETION_SIZE) @compileError("WriteCompletion size drift");
            if (@sizeOf(wire_pump.WireChunk) != bound.WIRE_CHUNK_DESC_SIZE) @compileError("WireChunk size drift");
        }

        var terms: Terms = .{};
        const n_chunks = self.inbound_wire_chunks_per_connection;
        const ticket_n = try checkedAdd(self.control_entries_per_connection, self.max_streams_per_connection);

        terms.read_payload = try checkedMul(n_chunks, WIRE_CHUNK_SIZE);
        terms.wire_descs = try checkedMul(try checkedMul(n_chunks, 2), bound.WIRE_CHUNK_DESC_SIZE); // read+write ch
        terms.handlers = try checkedMul(self.max_streams_per_connection, HANDLER_SLOT_SIZE);
        terms.joins = try checkedMul(self.max_streams_per_connection, bound.JOIN_HANDLE_SIZE);
        const completion_ids = try checkedMul(self.max_streams_per_connection, @sizeOf(u31));
        terms.write_acks = try checkedMul(ticket_n, bound.WRITE_COMPLETION_SIZE);
        terms.tickets = try checkedMul(ticket_n, bound.TICKET_WAIT_SIZE);
        const plain_scratch: usize = TLS_PLAINTEXT_SCRATCH_SIZE;
        const cipher_scratch: usize = WIRE_CHUNK_SIZE;
        const sid_scratch = try checkedMul(self.max_streams_per_connection, @sizeOf(u31));
        const read_free = try checkedMul(n_chunks, @sizeOf(u32));
        terms.stream_maps = bound.streamMapBytes(self.max_streams_per_connection) catch return error.InvalidConfig;
        const pending_slots = bound.pendingWriteMapBytes(self.max_streams_per_connection) catch return error.InvalidConfig;
        // Slabs are server-wide now, so they are NOT part of the per-connection
        // term. Counting them here is what made the bound scale with
        // connections instead of with concurrent work.
        terms.pending_maps = pending_slots;
        const tombstones = try checkedMul(self.stream_tombstones, 8);
        const header_maps_one = bound.hashMapBytes(u31, usize, self.max_streams_per_connection) catch return error.InvalidConfig;
        const header_maps = try checkedMul(header_maps_one, 3); // pending hdr/body/trailer maps
        const intents = try checkedMul(self.intent_entries_per_connection, bound.INTENT_SIZE);
        const sched_rings = try checkedAdd(
            try checkedMul(self.control_entries_per_connection, bound.CONTROL_ENTRY_SIZE),
            try checkedAdd(
                try checkedMul(@max(self.control_entries_per_connection / 8, 16), bound.CONTROL_ENTRY_SIZE),
                try checkedMul(self.control_entries_per_connection, @sizeOf(bound.FramedDataEntryShape)),
            ),
        );
        const space_events = try checkedMul(self.max_streams_per_connection, bound.EVENT_SIZE);
        const sched_scratch = try checkedMul(self.max_streams_per_connection, @sizeOf(u31));
        terms.on_demand_conn = try checkedAdd(self.request_bytes_per_connection, try checkedAdd(self.outbound_bytes_per_connection, try checkedAdd(self.control_bytes_per_connection, try checkedAdd(self.tls_recv_acc_bytes, try checkedAdd(header_maps, intents)))));

        const per_conn = try checkedAdd(terms.read_payload, try checkedAdd(terms.wire_descs, try checkedAdd(terms.handlers, try checkedAdd(terms.joins, try checkedAdd(completion_ids, try checkedAdd(terms.write_acks, try checkedAdd(terms.tickets, try checkedAdd(plain_scratch, try checkedAdd(cipher_scratch, try checkedAdd(sid_scratch, try checkedAdd(read_free, try checkedAdd(terms.on_demand_conn, try checkedAdd(terms.stream_maps, try checkedAdd(terms.pending_maps, try checkedAdd(tombstones, try checkedAdd(sched_rings, try checkedAdd(space_events, sched_scratch)))))))))))))))));

        terms.routes = try checkedAdd(
            self.max_route_path_bytes,
            try checkedMul(self.max_routes, bound.ROUTE_SIZE),
        );
        terms.certs = try checkedAdd(self.certificate_chain_bytes, self.private_key_bytes);
        terms.reaper_jobs = try checkedMul(self.cancellation_reaper_jobs, REAPER_JOB_SIZE);
        terms.conn_slots = 0;
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
        terms.on_demand_server = try checkedAdd(slab_bytes, try checkedAdd(self.outbound_bytes_per_server, self.request_bytes_per_server));
        const allocator_bytes = try checkedAdd(
            server_static,
            try checkedAdd(terms.tls_scratch, try checkedAdd(all_conns, try checkedAdd(terms.on_demand_server, terms.compression_contexts))),
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

test "defaults resource bound" {
    const b = try Limits.defaults.resourceUpperBound();
    try std.testing.expect(b.allocator_bytes > 0);
    try std.testing.expect(b.terms.handlers > 0);
    try std.testing.expect(b.terms.tickets > 0);
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

test "mutation canary: dropping handlers term shrinks bound" {
    const b = try Limits.defaults.resourceUpperBound();
    try std.testing.expect(b.terms.handlers > 0);
    try std.testing.expect(b.terms.tickets > 0);
    try std.testing.expect(b.terms.write_acks > 0);
    try std.testing.expect(b.terms.stream_maps > 0);
    try std.testing.expect(b.terms.on_demand_server > 0);
    var tiny = Limits.defaults;
    tiny.max_streams_per_connection = 1;
    tiny.max_streams_per_server = 64;
    tiny.cancellation_reaper_jobs = 64;
    const b2 = try tiny.resourceUpperBound();
    try std.testing.expect(b.terms.handlers > b2.terms.handlers);
    try std.testing.expect(b.allocator_bytes > b2.allocator_bytes);
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
