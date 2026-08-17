const std = @import("std");
const zio = @import("zio");

const Config = struct {
    rounds: usize = 100,
    connections: usize = 50,
    requests_per_connection: usize = 2_000,
    pipeline: usize = 10,
    executors: u8 = 0,
    migration: bool = true,
};

const Progress = struct {
    replies: std.atomic.Value(u64) = .init(0),
    done: std.atomic.Value(bool) = .init(false),
};

const read_slots = 16;
const read_chunk_size = 4096;

const ReadChunk = struct {
    slot: u8 = 0,
    len: usize = 0,
};

fn readPump(
    io: std.Io,
    stream: std.Io.net.Stream,
    chunks: *std.Io.Queue(ReadChunk),
    free_slots: *std.Io.Queue(u8),
    storage: *[read_slots][read_chunk_size]u8,
) !void {
    var reader = stream.reader(io, &.{});
    while (true) {
        const slot = try free_slots.getOne(io);
        var dest: [1][]u8 = .{&storage[slot]};
        const n = reader.interface.readVec(&dest) catch |err| switch (err) {
            error.EndOfStream => {
                try free_slots.putOne(io, slot);
                break;
            },
            else => return err,
        };
        try chunks.putOne(io, .{ .slot = slot, .len = n });
    }
    try chunks.putOne(io, .{});
}

fn readPumpEntry(
    io: std.Io,
    stream: std.Io.net.Stream,
    chunks: *std.Io.Queue(ReadChunk),
    free_slots: *std.Io.Queue(u8),
    storage: *[read_slots][read_chunk_size]u8,
) std.Io.Cancelable!void {
    readPump(io, stream, chunks, free_slots, storage) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            std.debug.print("read pump: {s}\n", .{@errorName(err)});
            chunks.close(io);
        },
    };
}

fn writePump(
    io: std.Io,
    stream: std.Io.net.Stream,
    commands: *std.Io.Queue(u8),
    acknowledgments: *std.Io.Queue(u8),
) !void {
    const response = [_]u8{0x5a};
    var writer = stream.writer(io, &.{});
    while (true) {
        const command = try commands.getOne(io);
        if (command == 0) {
            try acknowledgments.putOne(io, 0);
            return;
        }
        try writer.interface.writeAll(&response);
        try writer.interface.flush();
        try acknowledgments.putOne(io, 1);
    }
}

fn writePumpEntry(
    io: std.Io,
    stream: std.Io.net.Stream,
    commands: *std.Io.Queue(u8),
    acknowledgments: *std.Io.Queue(u8),
) std.Io.Cancelable!void {
    writePump(io, stream, commands, acknowledgments) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            std.debug.print("write pump: {s}\n", .{@errorName(err)});
            acknowledgments.close(io);
        },
    };
}

fn ackDrainer(io: std.Io, acknowledgments: *std.Io.Queue(u8)) std.Io.Cancelable!usize {
    var completed: usize = 0;
    while (true) {
        const acknowledgment = acknowledgments.getOne(io) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Closed => return completed,
        };
        if (acknowledgment == 0) return completed;
        completed += acknowledgment;
    }
}

fn serveConnection(io: std.Io, stream: std.Io.net.Stream) !void {
    defer stream.close(io);

    var chunk_storage: [read_slots][read_chunk_size]u8 = undefined;
    var chunk_buf: [read_slots]ReadChunk = undefined;
    var free_slot_buf: [read_slots]u8 = undefined;
    var command_buf: [16]u8 = undefined;
    var acknowledgment_buf: [16]u8 = undefined;
    var chunks = std.Io.Queue(ReadChunk).init(&chunk_buf);
    var free_slots = std.Io.Queue(u8).init(&free_slot_buf);
    var commands = std.Io.Queue(u8).init(&command_buf);
    var acknowledgments = std.Io.Queue(u8).init(&acknowledgment_buf);
    for (0..read_slots) |slot| try free_slots.putOne(io, @intCast(slot));

    var reader = try io.concurrent(readPumpEntry, .{ io, stream, &chunks, &free_slots, &chunk_storage });
    defer reader.cancel(io) catch {};
    var writer = try io.concurrent(writePumpEntry, .{ io, stream, &commands, &acknowledgments });
    defer writer.cancel(io) catch {};
    var acknowledger = try io.concurrent(ackDrainer, .{ io, &acknowledgments });
    defer _ = acknowledger.cancel(io) catch 0;

    var submitted: usize = 0;
    while (true) {
        const chunk = try chunks.getOne(io);
        if (chunk.len == 0) break;
        for (0..chunk.len) |_| try commands.putOne(io, 1);
        submitted += chunk.len;
        try free_slots.putOne(io, chunk.slot);
    }
    try commands.putOne(io, 0);
    try reader.await(io);
    try writer.await(io);
    const completed = try acknowledger.await(io);
    if (completed != submitted) return error.AcknowledgmentMismatch;
}

fn serveConnectionEntry(io: std.Io, stream: std.Io.net.Stream) std.Io.Cancelable!void {
    serveConnection(io, stream) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => std.debug.print("connection: {s}\n", .{@errorName(err)}),
    };
}

fn serverTask(io: std.Io, port_out: *zio.Channel(u16), total_connections: usize) !void {
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try address.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    try port_out.send(server.socket.address.getPort());

    var connections: std.Io.Group = .init;
    defer connections.cancel(io);
    for (0..total_connections) |_| {
        const stream = try server.accept(io);
        try connections.concurrent(io, serveConnectionEntry, .{ io, stream });
    }
    try connections.await(io);
}

fn clientTask(io: std.Io, port: u16, cfg: Config, progress: *Progress) !void {
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
    const stream = try address.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var request: [256]u8 = undefined;
    var response: [256]u8 = undefined;
    var read_buffer: [4096]u8 = undefined;
    var write_buffer: [4096]u8 = undefined;
    var reader = stream.reader(io, &read_buffer);
    var writer = stream.writer(io, &write_buffer);
    @memset(&request, 0x5a);

    var remaining = cfg.requests_per_connection;
    while (remaining != 0) {
        const n = @min(remaining, cfg.pipeline);
        try writer.interface.writeAll(request[0..n]);
        try writer.interface.flush();
        try reader.interface.readSliceAll(response[0..n]);
        for (response[0..n]) |byte| {
            if (byte != 0x5a) return error.DataMismatch;
        }
        _ = progress.replies.fetchAdd(n, .monotonic);
        remaining -= n;
    }
}

fn watchdog(progress: *Progress) void {
    var last = progress.replies.load(.acquire);
    while (!progress.done.load(.acquire)) {
        zio.sleep(.fromSeconds(5)) catch return;
        if (progress.done.load(.acquire)) return;
        const current = progress.replies.load(.acquire);
        if (current == last) {
            std.debug.print(
                "\nSTALL: no replies for 5s (completed={d}). " ++
                    "Re-run with --no-migration as the control.\n",
                .{current},
            );
            std.process.exit(2);
        }
        last = current;
    }
}

fn run(io: std.Io, cfg: Config) !void {
    var port_buf: [1]u16 = undefined;
    var port_out = zio.Channel(u16).init(&port_buf);
    const total_connections = try std.math.mul(usize, cfg.rounds, cfg.connections);

    var progress: Progress = .{};
    var watchdog_handle = try zio.spawn(watchdog, .{&progress});
    defer watchdog_handle.cancel();
    var server = try zio.spawn(serverTask, .{ io, &port_out, total_connections });
    defer server.cancel();

    const port = try port_out.receive();
    for (0..cfg.rounds) |round| {
        var clients: zio.Group = .init;
        defer clients.cancel();
        for (0..cfg.connections) |_| {
            try clients.spawn(clientTask, .{ io, port, cfg, &progress });
        }
        try clients.wait();
        std.debug.print(".", .{});
        if ((round + 1) % 20 == 0) {
            std.debug.print(" {d} rounds\n", .{round + 1});
        }
    }
    try server.join();
    progress.done.store(true, .release);
    watchdog_handle.cancel();
    std.debug.print(
        "\nPASS: {d} replies, migration={any}\n",
        .{ progress.replies.load(.acquire), cfg.migration },
    );
}

fn parseArgs(gpa: std.mem.Allocator, process_args: std.process.Args) !Config {
    var cfg: Config = .{};
    var args = try std.process.Args.Iterator.initAllocator(process_args, gpa);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--no-migration")) {
            cfg.migration = false;
        } else if (std.mem.eql(u8, arg, "--rounds")) {
            cfg.rounds = try std.fmt.parseInt(usize, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--connections")) {
            cfg.connections = try std.fmt.parseInt(usize, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--requests")) {
            cfg.requests_per_connection = try std.fmt.parseInt(usize, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--pipeline")) {
            cfg.pipeline = try std.fmt.parseInt(usize, args.next() orelse return error.MissingValue, 10);
        } else if (std.mem.eql(u8, arg, "--executors")) {
            cfg.executors = try std.fmt.parseInt(u8, args.next() orelse return error.MissingValue, 10);
        } else {
            return error.UnknownArgument;
        }
    }
    if (cfg.rounds == 0 or cfg.connections == 0 or cfg.requests_per_connection == 0 or cfg.pipeline == 0) {
        return error.InvalidArgument;
    }
    return cfg;
}

pub fn main(init: std.process.Init) !void {
    const cfg = try parseArgs(init.gpa, init.minimal.args);
    const runtime = try zio.Runtime.init(init.gpa, .{
        .executors = if (cfg.executors == 0) .auto else .exact(cfg.executors),
        .enable_task_migration = cfg.migration,
    });
    defer runtime.deinit();

    std.debug.print(
        "zio socket migration repro: rounds={d} connections={d} requests={d} " ++
            "pipeline={d} executors={d} migration={any}\n",
        .{
            cfg.rounds,
            cfg.connections,
            cfg.requests_per_connection,
            cfg.pipeline,
            runtime.executors.items.len,
            cfg.migration,
        },
    );
    var main_task = try runtime.spawn(run, .{ runtime.io(), cfg });
    try main_task.join();
}
