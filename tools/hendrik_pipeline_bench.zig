//! Isolated pipeline costs for hendriknielaender/http2.zig.
//!
//! Compiled out-of-tree by `bench-hendrik-pipeline.sh`; the opponent checkout
//! stays clean. Shapes and output intentionally match `pipeline_bench.zig`.
const std = @import("std");
const http2 = @import("http2");

const Hpack = http2.Hpack;

const response_fields = [_]Hpack.HeaderField{
    .{ .name = ":status", .value = "200" },
    .{ .name = "content-type", .value = "text/plain" },
};

const dynamic_insert_block = [_]u8{
    0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79,
    0x0d, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64,
    0x65, 0x72,
};
const seed_request_block = [_]u8{ 0x82, 0x87, 0x84 } ++ dynamic_insert_block;
const mixed_request_block = [_]u8{ 0x82, 0x87, 0x84, 0xbe, 0xbe };
const wire_header = [_]u8{ 0x00, 0x00, mixed_request_block.len, 0x01, 0x05, 0x00, 0x00, 0x00, 0x01 };

const Config = struct {
    iterations: usize = 1_000_000,
    rounds: usize = 5,
};

fn abort(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("hendrik-pipeline-bench: " ++ fmt ++ "\n", args);
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

const EncodeCtx = struct {
    allocator: std.mem.Allocator,
    table: Hpack.DynamicTable,
    out: std.ArrayList(u8) = .empty,

    fn init(allocator: std.mem.Allocator) !EncodeCtx {
        var ctx: EncodeCtx = .{
            .allocator = allocator,
            .table = Hpack.DynamicTable.init(allocator, 4096),
        };
        errdefer ctx.table.deinit();
        errdefer ctx.out.deinit(allocator);
        try ctx.out.ensureTotalCapacity(allocator, 128);
        // Production responses reuse the connection-level encoder table.
        for (response_fields) |field| {
            try Hpack.encodeHeaderField(field, &ctx.table, &ctx.out, allocator);
        }
        ctx.out.clearRetainingCapacity();
        return ctx;
    }

    fn deinit(self: *EncodeCtx) void {
        self.out.deinit(self.allocator);
        self.table.deinit();
    }
};

fn encodeOp(ctx: *EncodeCtx) !usize {
    ctx.out.clearRetainingCapacity();
    for (response_fields) |field| {
        try Hpack.encodeHeaderField(field, &ctx.table, &ctx.out, ctx.allocator);
    }
    std.mem.doNotOptimizeAway(ctx.out.items);
    return ctx.out.items.len;
}

const DecodeCtx = struct {
    table: Hpack.DynamicTable,

    fn init(allocator: std.mem.Allocator) !DecodeCtx {
        var table = Hpack.DynamicTable.init(allocator, 4096);
        errdefer table.deinit();
        _ = try Hpack.decodeHeaderFieldView(&dynamic_insert_block, &table);
        return .{ .table = table };
    }

    fn deinit(self: *DecodeCtx) void {
        self.table.deinit();
    }
};

fn decodeOp(ctx: *DecodeCtx) !usize {
    var cursor: usize = 0;
    var decoded_bytes: usize = 0;
    while (cursor < mixed_request_block.len) {
        Hpack.resetScratchBuffer();
        const decoded = try Hpack.decodeHeaderFieldView(mixed_request_block[cursor..], &ctx.table);
        cursor += decoded.bytes_consumed;
        decoded_bytes += decoded.header.name.len + decoded.header.value.len;
        std.mem.doNotOptimizeAway(decoded.header);
    }
    return decoded_bytes;
}

fn frameHeaderOp(_: *void) !usize {
    // Exact scalar operations in http2.zig's `parseFrameHeaderLenient`; that
    // declaration is not exported by its root module.
    const length = (@as(u32, wire_header[0]) << 16) |
        (@as(u32, wire_header[1]) << 8) |
        @as(u32, wire_header[2]);
    const stream_id = std.mem.readInt(u32, wire_header[5..9], .big) & 0x7fff_ffff;
    const parsed = .{
        length,
        wire_header[3],
        http2.FrameFlags.init(wire_header[4]),
        stream_id,
    };
    std.mem.doNotOptimizeAway(parsed);
    return length + stream_id;
}

fn benchmarkHandler(ctx: *const http2.Context) !http2.Response {
    if (ctx.method == .get and std.mem.eql(u8, ctx.path, "/")) {
        return ctx.response.text(.ok, "Hello, World!");
    }
    return ctx.response.text(.not_found, "Not Found");
}

const HandlerCtx = struct {
    context: http2.Context,
    dispatcher: http2.RequestDispatcher,
    body_scratch: [128]u8 = undefined,

    fn init(self: *HandlerCtx, allocator: std.mem.Allocator) void {
        self.* = undefined;
        self.context = http2.Context.init(
            allocator,
            .get,
            "/",
            "",
            &.{},
            "",
            &self.body_scratch,
        );
        self.dispatcher = http2.RequestDispatcher.fromHandlerWithoutHeaders(benchmarkHandler);
    }
};

fn handlerOp(ctx: *HandlerCtx) !usize {
    var response = try ctx.dispatcher.call(&ctx.context);
    defer response.deinit();
    std.mem.doNotOptimizeAway(response);
    return response.body.len;
}

const FixedIo = struct {
    reader: std.Io.Reader,
    writer: std.Io.Writer,

    fn init(write_buffer: []u8) FixedIo {
        return .{ .reader = .fixed(&.{}), .writer = .fixed(write_buffer) };
    }

    fn resetWriter(self: *FixedIo, write_buffer: []u8) void {
        self.writer = .fixed(write_buffer);
    }
};

const FullCoreCtx = struct {
    allocator: std.mem.Allocator,
    connection: http2.Connection,
    stream_storage: *http2.Connection.StreamStorage,
    fixed_io: FixedIo,
    output: [4096]u8,
    next_stream_id: u32,

    fn init(self: *FullCoreCtx, allocator: std.mem.Allocator) !void {
        self.allocator = allocator;
        self.stream_storage = try allocator.create(http2.Connection.StreamStorage);
        errdefer allocator.destroy(self.stream_storage);
        self.fixed_io = FixedIo.init(&self.output);
        try http2.Connection.initServerEventDrivenInPlace(
            &self.connection,
            self.stream_storage,
            allocator,
            &self.fixed_io.reader,
            &self.fixed_io.writer,
        );
        errdefer self.connection.deinit();
        self.connection.bindRequestDispatcher(
            http2.RequestDispatcher.fromHandlerWithoutHeaders(benchmarkHandler),
        );
        self.fixed_io.resetWriter(&self.output);

        // Seed the request dynamic table once, exactly as a prior request on the
        // same connection would. It is outside all measured and counted work.
        try self.dispatch(&seed_request_block, 1);
        self.fixed_io.resetWriter(&self.output);
        self.next_stream_id = 3;
    }

    fn deinit(self: *FullCoreCtx) void {
        self.connection.deinit();
        self.allocator.destroy(self.stream_storage);
    }

    fn dispatch(self: *FullCoreCtx, block: []const u8, stream_id: u32) !void {
        // Each operation represents a flow-control-ready request. A live client
        // replenishes the connection window; carrying depletion across isolated
        // iterations would eventually benchmark a blocked stream instead.
        self.connection.send_window_size = 65535;
        const request = http2.Frame{
            .header = .{
                .length = @intCast(block.len),
                .frame_type = .HEADERS,
                .flags = http2.FrameFlags.init(
                    http2.FrameFlags.END_HEADERS | http2.FrameFlags.END_STREAM,
                ),
                .reserved = false,
                .stream_id = stream_id,
            },
            .payload = block,
        };
        try self.connection.dispatchFrameOptimized(request);
        try self.connection.flush_ready_streams();
        if (self.connection.takeCompletedResponses() != 1) {
            return error.ResponseNotCompleted;
        }
    }
};

fn fullCoreOp(ctx: *FullCoreCtx) !usize {
    ctx.fixed_io.resetWriter(&ctx.output);
    const stream_id = ctx.next_stream_id;
    ctx.next_stream_id += 2;
    try ctx.dispatch(&mixed_request_block, stream_id);
    const wrote = ctx.fixed_io.writer.buffered().len;
    std.mem.doNotOptimizeAway(ctx.output[0..wrote]);
    return wrote;
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

fn printRow(name: []const u8, iterations: usize, stats: Stats, allocs: f64, bytes: f64) void {
    std.debug.print(
        "  {s:<28} {d:>10.1} {d:>8.1}..{d:<8.1} {d:>9.2} {d:>10.1} {d:>10}\n",
        .{ name, stats.median_ns, stats.min_ns, stats.max_ns, allocs, bytes, iterations },
    );
}

fn allocationRate(counter: *AllocCounter, iterations: usize) struct { allocs: f64, bytes: f64 } {
    return .{
        .allocs = @as(f64, @floatFromInt(counter.allocs)) / @as(f64, @floatFromInt(iterations)),
        .bytes = @as(f64, @floatFromInt(counter.bytes)) / @as(f64, @floatFromInt(iterations)),
    };
}

pub fn main(init: std.process.Init) !void {
    var arena_state = std.heap.ArenaAllocator.init(init.gpa);
    defer arena_state.deinit();
    const cfg = try parseArgs(arena_state.allocator(), init.minimal.args);
    const io = init.io;
    const allocator = std.heap.c_allocator;
    const count_n = @min(cfg.iterations, 10_000);

    std.debug.print(
        "hendrik-pipeline-bench: ReleaseFast isolated costs; median of {d} rounds\n" ++
            "  {s:<28} {s:>10} {s:>18} {s:>9} {s:>10} {s:>10}\n",
        .{ cfg.rounds, "stage", "ns/op", "range", "alloc/op", "bytes/op", "iterations" },
    );

    var encode = try EncodeCtx.init(allocator);
    defer encode.deinit();
    const encode_stats = try benchmark(io, cfg.iterations, cfg.rounds, &encode, encodeOp);
    var encode_counter: AllocCounter = .{ .parent = allocator };
    var counted_encode = try EncodeCtx.init(encode_counter.allocator());
    defer counted_encode.deinit();
    encode_counter.reset();
    for (0..count_n) |_| _ = try encodeOp(&counted_encode);
    const encode_rate = allocationRate(&encode_counter, count_n);
    printRow("HPACK response steady encode", cfg.iterations, encode_stats, encode_rate.allocs, encode_rate.bytes);

    var decode = try DecodeCtx.init(allocator);
    defer decode.deinit();
    const decode_stats = try benchmark(io, cfg.iterations, cfg.rounds, &decode, decodeOp);
    var decode_counter: AllocCounter = .{ .parent = allocator };
    var counted_decode = try DecodeCtx.init(decode_counter.allocator());
    defer counted_decode.deinit();
    decode_counter.reset();
    for (0..count_n) |_| _ = try decodeOp(&counted_decode);
    const decode_rate = allocationRate(&decode_counter, count_n);
    printRow("HPACK request view decode", cfg.iterations, decode_stats, decode_rate.allocs, decode_rate.bytes);

    var none: void = {};
    const frame_stats = try benchmark(io, cfg.iterations, cfg.rounds, &none, frameHeaderOp);
    printRow("HTTP/2 frame header parse", cfg.iterations, frame_stats, 0, 0);

    var handler: HandlerCtx = undefined;
    handler.init(allocator);
    const handler_stats = try benchmark(io, cfg.iterations, cfg.rounds, &handler, handlerOp);
    printRow("inline handler call", cfg.iterations, handler_stats, 0, 0);

    var full_counter: AllocCounter = .{ .parent = allocator };
    var full: FullCoreCtx = undefined;
    try full.init(full_counter.allocator());
    defer full.deinit();
    full_counter.reset();
    const full_stats = try benchmark(io, cfg.iterations, cfg.rounds, &full, fullCoreOp);
    const full_rate = allocationRate(&full_counter, cfg.iterations * cfg.rounds);
    printRow("inline request core", cfg.iterations, full_stats, full_rate.allocs, full_rate.bytes);

    std.debug.print(
        "\nhendrik-pipeline-bench: inline request core starts from a parsed HEADERS frame\n" ++
            "and includes stream lookup/lifecycle, HPACK decode and validation, direct\n" ++
            "handler dispatch, response HPACK/framing, and writes to a fixed buffer.\n" ++
            "starh2's comparable hop is pipeline-bench 'Connection complete oneshot'\n" ++
            "(Session + scheduler + inline encode + local ack; still no socket).\n",
        .{},
    );
}
