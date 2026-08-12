//! Checked Limits.resourceUpperBound from concrete `@sizeOf` shapes + configured capacities.
const std = @import("std");
const bound = @import("bound_shapes.zig");
const wire_const = @import("wire_const.zig");
const ticket_table = @import("../edge/ticket_table.zig");
const wire_pump = @import("../edge/wire_pump.zig");

pub const ResourceUpperBound = struct {
    allocator_bytes: usize,
    committed_stack_bytes: usize,
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
    conn_slots: usize = 0,
    routes: usize = 0,
    certs: usize = 0,
    tls_scratch: usize = 0,
    endpoints_listeners: usize = 0,
    tasks: usize = 0,
};

/// Must match `edge.connection.HandlerSlot` — comptime-asserted in connection.zig.
pub const HANDLER_SLOT_SIZE: usize = 20;
/// Must match `edge.connection.ReaperJob` — comptime-asserted in connection.zig.
pub const REAPER_JOB_SIZE: usize = 32;

pub const WIRE_CHUNK_SIZE = wire_const.WIRE_CHUNK_SIZE;
pub const TLS_PLAINTEXT_SCRATCH_SIZE = wire_const.TLS_PLAINTEXT_SCRATCH_SIZE;
pub const TERMINAL_CONTROL_RESERVE_BYTES: usize = 4 * 1024;
pub const TERMINAL_CONTROL_RESERVE_ENTRIES: usize = 16;

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
    /// Intent backlog entries per connection (session).
    intent_entries_per_connection: usize = 512,
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
        if (self.cancellation_reaper_jobs < self.max_streams_per_server) return error.InvalidConfig;
        if (self.inbound_wire_chunks_per_connection == 0) return error.InvalidConfig;
        if (self.outbound_bytes_per_stream == 0 or self.outbound_bytes_per_connection == 0) return error.InvalidConfig;
        if (self.outbound_bytes_per_stream > self.outbound_bytes_per_connection) return error.InvalidConfig;
        if (self.outbound_bytes_per_connection > self.outbound_bytes_per_server) return error.InvalidConfig;
        if (self.request_bytes_per_connection > self.request_bytes_per_server) return error.InvalidConfig;
        // Ordinary SETTINGS/HEADERS must fit without consuming the terminal
        // GOAWAY/RST/ACK reserve.
        if (self.control_bytes_per_connection <= TERMINAL_CONTROL_RESERVE_BYTES) return error.InvalidConfig;
        if (self.control_entries_per_connection <= TERMINAL_CONTROL_RESERVE_ENTRIES) return error.InvalidConfig;
        if (self.concurrent_tls_handshakes == 0) return error.InvalidConfig;
        if (self.tls_recv_acc_bytes < WIRE_CHUNK_SIZE) return error.InvalidConfig;

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
        const pending_slabs = bound.pendingSlabBytes(self.max_streams_per_connection, self.outbound_bytes_per_stream) catch return error.InvalidConfig;
        terms.pending_maps = try checkedAdd(pending_slots, pending_slabs);
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
        const space_sems = try checkedMul(self.max_streams_per_connection, bound.SEMAPHORE_SIZE);
        const sched_scratch = try checkedMul(self.max_streams_per_connection, @sizeOf(u31));
        terms.on_demand_conn = try checkedAdd(self.request_bytes_per_connection, try checkedAdd(self.outbound_bytes_per_connection, try checkedAdd(self.control_bytes_per_connection, try checkedAdd(self.tls_recv_acc_bytes, try checkedAdd(header_maps, intents)))));

        const per_conn = try checkedAdd(terms.read_payload, try checkedAdd(terms.wire_descs, try checkedAdd(terms.handlers, try checkedAdd(terms.joins, try checkedAdd(completion_ids, try checkedAdd(terms.write_acks, try checkedAdd(terms.tickets, try checkedAdd(plain_scratch, try checkedAdd(cipher_scratch, try checkedAdd(sid_scratch, try checkedAdd(read_free, try checkedAdd(terms.on_demand_conn, try checkedAdd(terms.stream_maps, try checkedAdd(terms.pending_maps, try checkedAdd(tombstones, try checkedAdd(sched_rings, try checkedAdd(space_sems, sched_scratch)))))))))))))))));

        terms.routes = self.max_route_path_bytes;
        terms.certs = try checkedAdd(self.certificate_chain_bytes, self.private_key_bytes);
        terms.reaper_jobs = try checkedMul(self.cancellation_reaper_jobs, REAPER_JOB_SIZE);
        terms.conn_slots = try checkedMul(self.max_connections, bound.CONN_SLOT_SIZE);
        terms.tls_scratch = try checkedMul(self.concurrent_tls_handshakes, self.tls_handshake_scratch_bytes);
        // listeners + local_addrs + endpoint configs (concrete pointer/slot shapes)
        const EndpointSlot = struct { a: usize, b: usize, c: usize, d: usize };
        terms.endpoints_listeners = try checkedMul(self.max_endpoints, @sizeOf(EndpointSlot));
        const server_static = try checkedAdd(terms.routes, try checkedAdd(terms.certs, try checkedAdd(terms.reaper_jobs, try checkedAdd(terms.conn_slots, terms.endpoints_listeners))));

        const all_conns = try checkedMul(self.max_connections, per_conn);
        // Global on-demand ceilings counted once (admission counters + payload pools).
        terms.on_demand_server = try checkedAdd(self.outbound_bytes_per_server, self.request_bytes_per_server);
        const allocator_bytes = try checkedAdd(server_static, try checkedAdd(terms.tls_scratch, try checkedAdd(all_conns, terms.on_demand_server)));

        const max_stack: usize = 1024 * 1024;
        const committed_stack: usize = 64 * 1024;
        // Exact topology: accept loops + per-conn (actor+read+write+ack) + handlers + reapers.
        // Cached unused stacks: zio max_unused_stacks default documented as 256 in DESIGN §5.
        const per_conn_tasks: usize = 4;
        terms.tasks = try checkedAdd(
            try checkedAdd(self.max_endpoints, try checkedMul(self.max_connections, per_conn_tasks)),
            try checkedAdd(self.max_streams_per_server, self.cancellation_reaper_tasks),
        );
        const committed_stack_bytes = try checkedMul(terms.tasks, committed_stack);
        const cached_stacks: usize = 256;
        const virtual_stack_bytes = try checkedAdd(try checkedMul(terms.tasks, max_stack), try checkedMul(cached_stacks, max_stack));

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
