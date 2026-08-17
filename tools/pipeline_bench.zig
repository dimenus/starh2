//! Isolated CPU/allocation costs for the one-shot request pipeline.
//!
//! This is intentionally not a throughput benchmark. It removes sockets, TLS,
//! contention, and client scheduling so a large full-server gap can be assigned
//! to concrete local work (or shown not to live there). Each row runs multiple
//! rounds and reports the median plus the observed range.
const std = @import("std");
const starh2 = @import("starh2");
const zio = @import("zio");

const hpack = starh2.core.hpack;
const frame = starh2.core.frame;
const emit_batch = starh2.edge.emit_batch;

const response_fields = [_]hpack.HeaderField{
    .{ .name = ":status", .value = "200" },
    .{ .name = "content-type", .value = "text/plain" },
};
const response_extra_fields = [_]hpack.HeaderField{
    .{ .name = "content-type", .value = "text/plain" },
};

const static_request_block = [_]u8{ 0x82, 0x87, 0x84 };
const dynamic_insert_block = [_]u8{
    0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79,
    0x0d, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64,
    0x65, 0x72,
};
const seed_request_block = [_]u8{ 0x82, 0x87, 0x84 } ++ dynamic_insert_block;
// :method GET, :scheme https, :path /, then two references to dynamic index 62.
const mixed_request_block = [_]u8{ 0x82, 0x87, 0x84, 0xbe, 0xbe };

const Config = struct {
    iterations: usize = 1_000_000,
    rounds: usize = 5,
};

fn abort(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("pipeline-bench: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

fn parseArgs(gpa: std.mem.Allocator, process_args: std.process.Args) !Config {
    var cfg: Config = .{};
    var args = try std.process.Args.Iterator.initAllocator(process_args, gpa);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |arg| {
        const value = args.next() orelse abort("{s} needs a value", .{arg});
        if (std.mem.eql(u8, arg, "-n")) {
            cfg.iterations = std.fmt.parseInt(usize, value, 10) catch
                abort("-n needs a number", .{});
        } else if (std.mem.eql(u8, arg, "--rounds")) {
            cfg.rounds = std.fmt.parseInt(usize, value, 10) catch
                abort("--rounds needs a number", .{});
        } else {
            abort("unknown argument {s}", .{arg});
        }
    }
    if (cfg.iterations == 0) abort("-n must be greater than zero", .{});
    if (cfg.rounds < 3 or cfg.rounds > 9 or cfg.rounds % 2 == 0) {
        abort("--rounds must be an odd number from 3 through 9", .{});
    }
    return cfg;
}

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

const AllocCounter = struct {
    parent: std.mem.Allocator,
    allocs: usize = 0,
    bytes: usize = 0,

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn allocator(self: *AllocCounter) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn reset(self: *AllocCounter) void {
        self.allocs = 0;
        self.bytes = 0;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *AllocCounter = @ptrCast(@alignCast(ctx));
        const ptr = self.parent.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.allocs += 1;
        self.bytes += len;
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *AllocCounter = @ptrCast(@alignCast(ctx));
        return self.parent.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *AllocCounter = @ptrCast(@alignCast(ctx));
        return self.parent.rawRemap(memory, alignment, new_len, ret_addr);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *AllocCounter = @ptrCast(@alignCast(ctx));
        self.parent.rawFree(memory, alignment, ret_addr);
    }
};

const EncodeIntoCtx = struct {
    out: [128]u8 = undefined,
};

fn encodeIntoOp(ctx: *EncodeIntoCtx) !usize {
    const n = try hpack.Encoder.encodeInto(&ctx.out, &response_fields);
    std.mem.doNotOptimizeAway(&ctx.out);
    return n;
}

const EncodeAllocCtx = struct {
    gpa: std.mem.Allocator,
};

fn encodeAllocOp(ctx: *EncodeAllocCtx) !usize {
    const out = try hpack.Encoder.encode(ctx.gpa, &response_fields);
    defer ctx.gpa.free(out);
    std.mem.doNotOptimizeAway(out);
    return out.len;
}

const DecodeCtx = struct {
    decoder: hpack.Decoder,
    block: []const u8,

    fn init(gpa: std.mem.Allocator, block: []const u8, seed_dynamic: bool) !DecodeCtx {
        var decoder = hpack.Decoder.init(gpa);
        errdefer decoder.deinit();
        if (seed_dynamic) {
            const seeded = try decoder.decode(&dynamic_insert_block, 100, 32 * 1024, 256, 8 * 1024);
            decoder.freeResult(seeded);
        }
        return .{ .decoder = decoder, .block = block };
    }

    fn deinit(self: *DecodeCtx) void {
        self.decoder.deinit();
    }
};

fn decodeOp(ctx: *DecodeCtx) !usize {
    const decoded = try ctx.decoder.decode(ctx.block, 100, 32 * 1024, 256, 8 * 1024);
    defer ctx.decoder.freeResult(decoded);
    std.mem.doNotOptimizeAway(decoded.fields);
    return decoded.decoded_size;
}

const ParseCtx = struct {
    gpa: std.mem.Allocator,
    parser: frame.Parser,
    wire: [frame.FRAME_HEADER_LEN + mixed_request_block.len]u8,

    fn init(gpa: std.mem.Allocator) !ParseCtx {
        var parser = try frame.Parser.initReserved(gpa, frame.DEFAULT_MAX_FRAME_SIZE);
        errdefer parser.deinit();
        parser.skipPreface();

        var wire: [frame.FRAME_HEADER_LEN + mixed_request_block.len]u8 = undefined;
        var header: [frame.FRAME_HEADER_LEN]u8 = undefined;
        (frame.FrameHeader{
            .length = mixed_request_block.len,
            .type = .headers,
            .flags = .{ .end_headers = true },
            .stream_id = 1,
        }).encode(&header);
        @memcpy(wire[0..frame.FRAME_HEADER_LEN], &header);
        @memcpy(wire[frame.FRAME_HEADER_LEN..], &mixed_request_block);
        return .{ .gpa = gpa, .parser = parser, .wire = wire };
    }

    fn deinit(self: *ParseCtx) void {
        self.parser.deinit();
    }
};

fn parseOp(ctx: *ParseCtx) !usize {
    const result = (try ctx.parser.ingestOne(&ctx.wire)) orelse return error.IncompleteFrame;
    defer result.event.deinit(ctx.gpa);
    std.mem.doNotOptimizeAway(result.event.payload.bytes());
    return result.consumed;
}

const InboundCtx = struct {
    gpa: std.mem.Allocator,
    parser: frame.Parser,
    decoder: hpack.Decoder,
    wire: [frame.FRAME_HEADER_LEN + mixed_request_block.len]u8,

    fn init(gpa: std.mem.Allocator) !InboundCtx {
        var parse_ctx = try ParseCtx.init(gpa);
        errdefer parse_ctx.deinit();
        var decoder = hpack.Decoder.init(gpa);
        errdefer decoder.deinit();
        const seeded = try decoder.decode(&dynamic_insert_block, 100, 32 * 1024, 256, 8 * 1024);
        decoder.freeResult(seeded);
        const result: InboundCtx = .{
            .gpa = gpa,
            .parser = parse_ctx.parser,
            .decoder = decoder,
            .wire = parse_ctx.wire,
        };
        parse_ctx.parser = undefined;
        return result;
    }

    fn deinit(self: *InboundCtx) void {
        self.decoder.deinit();
        self.parser.deinit();
    }
};

fn inboundOp(ctx: *InboundCtx) !usize {
    const parsed = (try ctx.parser.ingestOne(&ctx.wire)) orelse return error.IncompleteFrame;
    defer parsed.event.deinit(ctx.gpa);
    const decoded = try ctx.decoder.decode(parsed.event.payload.bytes(), 100, 32 * 1024, 256, 8 * 1024);
    defer ctx.decoder.freeResult(decoded);
    std.mem.doNotOptimizeAway(decoded.fields);
    return parsed.consumed + decoded.decoded_size;
}

const SessionCtx = struct {
    session: starh2.Session,
    wire: [frame.FRAME_HEADER_LEN + seed_request_block.len]u8,
    next_stream_id: u31,

    fn init(self: *SessionCtx, gpa: std.mem.Allocator) !void {
        self.session = try starh2.Session.init(gpa, .{});
        errdefer self.session.deinit();
        self.next_stream_id = 1;

        // Construction queues server SETTINGS; exclude boot protocol from the
        // per-request measurement.
        self.releaseIntents();
        try self.session.ingest(frame.CLIENT_PREFACE);
        var settings_header: [frame.FRAME_HEADER_LEN]u8 = undefined;
        (frame.FrameHeader{
            .length = 0,
            .type = .settings,
            .flags = .{},
            .stream_id = 0,
        }).encode(&settings_header);
        try self.session.ingest(&settings_header);
        self.releaseIntents();

        // Seed dynamic index 62 through a complete request on this connection.
        try self.request(&seed_request_block);
        self.next_stream_id = 3;
    }

    fn deinit(self: *SessionCtx) void {
        self.session.deinit();
    }

    fn releaseIntents(self: *SessionCtx) void {
        const intents = self.session.drainIntents();
        for (intents) |*intent| self.session.releaseIntent(intent);
    }

    fn setWire(self: *SessionCtx, block: []const u8, stream_id: u31) []const u8 {
        var header: [frame.FRAME_HEADER_LEN]u8 = undefined;
        (frame.FrameHeader{
            .length = @intCast(block.len),
            .type = .headers,
            .flags = .{ .end_stream = true, .end_headers = true },
            .stream_id = stream_id,
        }).encode(&header);
        @memcpy(self.wire[0..frame.FRAME_HEADER_LEN], &header);
        @memcpy(self.wire[frame.FRAME_HEADER_LEN..][0..block.len], block);
        return self.wire[0 .. frame.FRAME_HEADER_LEN + block.len];
    }

    fn request(self: *SessionCtx, block: []const u8) !void {
        const stream_id = try self.ingress(block);
        try self.respondHeaders(stream_id);
        try self.respondData(stream_id);
    }

    fn ingress(self: *SessionCtx, block: []const u8) !u31 {
        const stream_id = self.next_stream_id;
        self.next_stream_id += 2;
        try self.session.ingest(self.setWire(block, stream_id));

        var dispatches: usize = 0;
        const inbound = self.session.drainIntents();
        for (inbound) |*intent| {
            if (intent.* == .dispatch_request) dispatches += 1;
            self.session.releaseIntent(intent);
        }
        if (dispatches != 1) return error.RequestNotDispatched;
        return stream_id;
    }

    fn respondHeaders(self: *SessionCtx, stream_id: u31) !void {
        // Each isolated operation is flow-control-ready. A real client
        // replenishes this connection window between responses.
        self.session.windows.conn_send = starh2.core.flow.INITIAL_WINDOW;
        try self.session.applyCommand(.{ .respond_headers = .{
            .stream_id = stream_id,
            .status = 200,
            .headers = &response_extra_fields,
            .end_stream = false,
        } });
        self.releaseIntents();
    }

    fn respondData(self: *SessionCtx, stream_id: u31) !void {
        try self.session.applyCommand(.{ .respond_data = .{
            .stream_id = stream_id,
            .data = "Hello, World!",
            .end_stream = true,
        } });
        self.releaseIntents();
    }
};

fn sessionOp(ctx: *SessionCtx) !usize {
    try ctx.request(&mixed_request_block);
    return mixed_request_block.len + "Hello, World!".len;
}

const SessionPhases = struct {
    ingress: Stats,
    headers: Stats,
    data: Stats,
};

fn statsFromSamples(samples: *[9]f64, rounds: usize) Stats {
    for (1..rounds) |i| {
        const value = samples[i];
        var j = i;
        while (j > 0 and samples[j - 1] > value) : (j -= 1) {
            samples[j] = samples[j - 1];
        }
        samples[j] = value;
    }
    return .{
        .median_ns = samples[rounds / 2],
        .min_ns = samples[0],
        .max_ns = samples[rounds - 1],
    };
}

fn profileSessionPhases(
    io: std.Io,
    iterations: usize,
    rounds: usize,
    ctx: *SessionCtx,
) !SessionPhases {
    var ingress_samples: [9]f64 = @splat(0);
    var header_samples: [9]f64 = @splat(0);
    var data_samples: [9]f64 = @splat(0);
    for (0..rounds) |round| {
        var ingress_ns: u64 = 0;
        var header_ns: u64 = 0;
        var data_ns: u64 = 0;
        for (0..iterations) |_| {
            const start = nowNs(io);
            const stream_id = try ctx.ingress(&mixed_request_block);
            const after_ingress = nowNs(io);
            try ctx.respondHeaders(stream_id);
            const after_headers = nowNs(io);
            try ctx.respondData(stream_id);
            const after_data = nowNs(io);
            ingress_ns += after_ingress - start;
            header_ns += after_headers - after_ingress;
            data_ns += after_data - after_headers;
        }
        const denominator = @as(f64, @floatFromInt(iterations));
        ingress_samples[round] = @as(f64, @floatFromInt(ingress_ns)) / denominator;
        header_samples[round] = @as(f64, @floatFromInt(header_ns)) / denominator;
        data_samples[round] = @as(f64, @floatFromInt(data_ns)) / denominator;
    }
    return .{
        .ingress = statsFromSamples(&ingress_samples, rounds),
        .headers = statsFromSamples(&header_samples, rounds),
        .data = statsFromSamples(&data_samples, rounds),
    };
}

const PackCtx = struct {
    storage: [512]u8 = undefined,
};

const pack_headers = "HHHHHHHHHHHHHHHHHHHHHHHH";
const pack_data = "DDDDDDDDDDDDDDDDDDDDDD";

fn packSixOp(ctx: *PackCtx) !usize {
    // One drain turn of six 13-byte responses: HEADERS then DATA, the shape
    // that used to be six TLS records because a second HEADERS flushed.
    var batch: emit_batch.EmitBatch = .{ .buf = &ctx.storage };
    var i: u32 = 0;
    while (i < 6) : (i += 1) {
        batch.copyFrame(pack_headers, true, pack_headers.len, true);
        batch.noteTicket(i + 1, i);
        batch.copyFrame(pack_data, true, 0, false);
    }
    const n = batch.len;
    std.mem.doNotOptimizeAway(batch.take().bytes);
    return n;
}

const TaskCtx = struct {
    io: std.Io,
};

fn emptyTask() void {}

fn taskLifecycleOp(ctx: *TaskCtx) !usize {
    var handle = try ctx.io.concurrent(emptyTask, .{});
    handle.await(ctx.io);
    return 1;
}

const Stats = struct {
    median_ns: f64,
    min_ns: f64,
    max_ns: f64,
};

fn benchmark(
    io: std.Io,
    iterations: usize,
    rounds: usize,
    ctx: anytype,
    comptime op: fn (@TypeOf(ctx)) anyerror!usize,
) !Stats {
    const warmup = @min(iterations, 10_000);
    for (0..warmup) |_| _ = try op(ctx);

    var samples: [9]f64 = @splat(0);
    for (0..rounds) |round| {
        var checksum: usize = 0;
        const start = nowNs(io);
        for (0..iterations) |_| checksum +%= try op(ctx);
        const elapsed = nowNs(io) - start;
        std.mem.doNotOptimizeAway(checksum);
        samples[round] = @as(f64, @floatFromInt(elapsed)) /
            @as(f64, @floatFromInt(iterations));
    }

    return statsFromSamples(&samples, rounds);
}

fn printRow(name: []const u8, iterations: usize, stats: Stats, allocs: ?f64, bytes: ?f64) void {
    if (allocs) |a| {
        std.debug.print(
            "  {s:<28} {d:>10.1} {d:>8.1}..{d:<8.1} {d:>9.2} {d:>10.1} {d:>10}\n",
            .{ name, stats.median_ns, stats.min_ns, stats.max_ns, a, bytes.?, iterations },
        );
    } else {
        std.debug.print(
            "  {s:<28} {d:>10.1} {d:>8.1}..{d:<8.1} {s:>9} {s:>10} {d:>10}\n",
            .{ name, stats.median_ns, stats.min_ns, stats.max_ns, "n/a", "n/a", iterations },
        );
    }
}

fn allocationRate(counter: *AllocCounter, iterations: usize) struct { allocs: f64, bytes: f64 } {
    return .{
        .allocs = @as(f64, @floatFromInt(counter.allocs)) / @as(f64, @floatFromInt(iterations)),
        .bytes = @as(f64, @floatFromInt(counter.bytes)) / @as(f64, @floatFromInt(iterations)),
    };
}

fn runBenchmarks(rt: *zio.Runtime, cfg: Config) !void {
    const io = rt.io();
    const gpa = std.heap.c_allocator;
    const count_n = @min(cfg.iterations, 10_000);

    std.debug.print(
        "pipeline-bench: ReleaseFast isolated costs; median of {d} rounds\n" ++
            "  {s:<28} {s:>10} {s:>18} {s:>9} {s:>10} {s:>10}\n",
        .{ cfg.rounds, "stage", "ns/op", "range", "alloc/op", "bytes/op", "iterations" },
    );

    var encode_into: EncodeIntoCtx = .{};
    const encode_into_stats = try benchmark(io, cfg.iterations, cfg.rounds, &encode_into, encodeIntoOp);
    printRow("HPACK response encodeInto", cfg.iterations, encode_into_stats, 0, 0);

    var encode_alloc: EncodeAllocCtx = .{ .gpa = gpa };
    const encode_alloc_stats = try benchmark(io, cfg.iterations, cfg.rounds, &encode_alloc, encodeAllocOp);
    var encode_counter: AllocCounter = .{ .parent = gpa };
    var counted_encode: EncodeAllocCtx = .{ .gpa = encode_counter.allocator() };
    for (0..count_n) |_| _ = try encodeAllocOp(&counted_encode);
    const encode_rate = allocationRate(&encode_counter, count_n);
    printRow("HPACK response encode alloc", cfg.iterations, encode_alloc_stats, encode_rate.allocs, encode_rate.bytes);

    var static_decode = try DecodeCtx.init(gpa, &static_request_block, false);
    defer static_decode.deinit();
    const static_stats = try benchmark(io, cfg.iterations, cfg.rounds, &static_decode, decodeOp);
    var static_counter: AllocCounter = .{ .parent = gpa };
    var counted_static = try DecodeCtx.init(static_counter.allocator(), &static_request_block, false);
    defer counted_static.deinit();
    static_counter.reset();
    for (0..count_n) |_| _ = try decodeOp(&counted_static);
    const static_rate = allocationRate(&static_counter, count_n);
    printRow("HPACK request static decode", cfg.iterations, static_stats, static_rate.allocs, static_rate.bytes);

    var mixed_decode = try DecodeCtx.init(gpa, &mixed_request_block, true);
    defer mixed_decode.deinit();
    const mixed_stats = try benchmark(io, cfg.iterations, cfg.rounds, &mixed_decode, decodeOp);
    var mixed_counter: AllocCounter = .{ .parent = gpa };
    var counted_mixed = try DecodeCtx.init(mixed_counter.allocator(), &mixed_request_block, true);
    defer counted_mixed.deinit();
    mixed_counter.reset();
    for (0..count_n) |_| _ = try decodeOp(&counted_mixed);
    const mixed_rate = allocationRate(&mixed_counter, count_n);
    printRow("HPACK request mixed decode", cfg.iterations, mixed_stats, mixed_rate.allocs, mixed_rate.bytes);

    var parse = try ParseCtx.init(gpa);
    defer parse.deinit();
    const parse_stats = try benchmark(io, cfg.iterations, cfg.rounds, &parse, parseOp);
    var parse_counter: AllocCounter = .{ .parent = gpa };
    var counted_parse = try ParseCtx.init(parse_counter.allocator());
    defer counted_parse.deinit();
    parse_counter.reset();
    for (0..count_n) |_| _ = try parseOp(&counted_parse);
    const parse_rate = allocationRate(&parse_counter, count_n);
    printRow("HTTP/2 frame parse", cfg.iterations, parse_stats, parse_rate.allocs, parse_rate.bytes);

    var inbound = try InboundCtx.init(gpa);
    defer inbound.deinit();
    const inbound_stats = try benchmark(io, cfg.iterations, cfg.rounds, &inbound, inboundOp);
    var inbound_counter: AllocCounter = .{ .parent = gpa };
    var counted_inbound = try InboundCtx.init(inbound_counter.allocator());
    defer counted_inbound.deinit();
    inbound_counter.reset();
    for (0..count_n) |_| _ = try inboundOp(&counted_inbound);
    const inbound_rate = allocationRate(&inbound_counter, count_n);
    printRow("frame + HPACK request", cfg.iterations, inbound_stats, inbound_rate.allocs, inbound_rate.bytes);

    var session: SessionCtx = undefined;
    try session.init(gpa);
    defer session.deinit();
    const session_stats = try benchmark(io, cfg.iterations, cfg.rounds, &session, sessionOp);
    var session_counter: AllocCounter = .{ .parent = gpa };
    var counted_session: SessionCtx = undefined;
    try counted_session.init(session_counter.allocator());
    defer counted_session.deinit();
    session_counter.reset();
    for (0..count_n) |_| _ = try sessionOp(&counted_session);
    const session_rate = allocationRate(&session_counter, count_n);
    printRow("Session request + response", cfg.iterations, session_stats, session_rate.allocs, session_rate.bytes);

    const phase_iterations = @min(cfg.iterations, 100_000);
    const phases = try profileSessionPhases(io, phase_iterations, cfg.rounds, &session);
    printRow("  Session ingress phase*", phase_iterations, phases.ingress, 6, 211);
    printRow("  Session HEADERS phase*", phase_iterations, phases.headers, 1, 23);
    printRow("  Session DATA phase*", phase_iterations, phases.data, 1, 22);

    var pack: PackCtx = .{};
    const pack_stats = try benchmark(io, cfg.iterations, cfg.rounds, &pack, packSixOp);
    printRow("TLS pack 6 HEADERS+DATA", cfg.iterations, pack_stats, 0, 0);

    const task_iterations = @max(@as(usize, 1_000), cfg.iterations / 100);
    var task: TaskCtx = .{ .io = io };
    const task_stats = try benchmark(io, task_iterations, cfg.rounds, &task, taskLifecycleOp);
    printRow("empty task spawn + await", task_iterations, task_stats, null, null);

    std.debug.print(
        "\npipeline-bench: allocation columns count successful allocator.alloc calls and\n" ++
            "requested bytes; setup, dynamic-table seeding, and warmup are excluded.\n" ++
            "Rows marked * are interleaved phase samples and include one clock read.\n",
        .{},
    );
}

fn runMain(rt: *zio.Runtime, process_args: std.process.Args, gpa: std.mem.Allocator) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const cfg = try parseArgs(arena_state.allocator(), process_args);
    try runBenchmarks(rt, cfg);
}

pub fn main(init: std.process.Init) !void {
    const rt = try zio.Runtime.init(init.gpa, .{
        .stack_pool = .{
            .maximum_size = 1024 * 1024,
            .committed_size = 64 * 1024,
            .shrink_interval = .fromSeconds(30),
            .slab_slots = 256,
            .prewarm = 256,
        },
        .executors = .auto,
        .enable_task_migration = true,
    });
    defer rt.deinit();
    var handle = try rt.spawn(runMain, .{ rt, init.minimal.args, init.gpa });
    try handle.join();
}
