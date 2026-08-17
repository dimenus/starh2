//! README doctest: every `zig` block in README.md must compile, as an OUTSIDE
//! consumer compiles it.
//!
//! # Why this exists
//!
//! `./zb build ci` compiles the examples, so an API rename inside the repo
//! fails the build. It never compiles a line of README.md. The Datastar
//! section proved what that costs: commit 23dd119 moved signal reads out of
//! the core module, the README kept telling readers to call
//! `starh2.datastar.readSignalsFromQuery(...)`, and the snippet stayed broken
//! until 4be7eb9. Nothing reported it, because nothing compiled it.
//!
//! # Why an external consumer, and not a test in this repo
//!
//! The defect class is "the reader cannot reach this from outside". An in-repo
//! test imports modules through wiring a consumer does not have, so it proves
//! the API exists, never that the README's own build lines reach it. This gate
//! therefore writes a package that declares starh2 as a path dependency, and
//! wires it with the `addImport` lines COPIED FROM README.md, in document
//! order. A README that forgets to mention a module fails here, which is the
//! whole point. `dep.module("name")` panics at build time for a name that does
//! not exist, so the build lines must RUN, not merely compile.
//!
//! # Blocks are marked, never guessed
//!
//! A README block is a fragment: a handler body, a struct literal, some build
//! lines. Each kind needs a different wrapper, and a guess would silently pick
//! the wrong one. So every ```zig block carries an HTML comment on the line
//! above it, which renders as nothing on GitHub:
//!
//!     <!-- doctest: handler -->
//!
//! An unmarked block is an ERROR, not a skip. A skipped block reads exactly
//! like a checked one in the build log, and that is how coverage rots.
//!
//! # Forcing analysis, which is not what it looks like
//!
//! Zig analyzes a function body when it is called or when codegen needs it.
//! Two idioms that LOOK like they force it do not, both measured on 0.16:
//!
//! - `comptime { _ = &@field(T, name); }` compiles a broken body clean.
//! - `@typeInfo(T).@"struct".decls` lists only PUB decls, so a private `fn`
//!   in a README program is invisible to any decl loop.
//!
//! What works, also measured: store the function pointer of a PUB decl into a
//! runtime variable. So the generated root does that for every pub decl of
//! every generated file, and program blocks get their top-level `fn` raised to
//! `pub fn` on the way in. Raising it changes no semantics; leaving it private
//! would make this gate pass on a program that does not compile.
//!
//! # Rules
//!
//! - Zero blocks is an ERROR. A run that checked nothing has no result.
//! - The counts are printed. A gate that quietly covers less must be visible
//!   in the log, and "PASS" alone cannot say it.
//! - The generated package is left on disk after a failure, so the error text
//!   points at a file that can be opened and read.
const std = @import("std");

const Kind = enum {
    /// Lines for a consumer's build.zig. All of them, in document order, are
    /// spliced into one function, because a reader accumulates them that way.
    build,
    /// A complete program. Becomes its own file.
    program,
    /// Statements inside a handler body: `req`, `resp`, `body`, `chunk` scope.
    handler,
    /// Elements of a `starh2.Route` array. `h` is in scope.
    route,
    /// Fields of a `starh2.ServerConfig`. `addr`, `cert_pem`, `key_pem` scope.
    config,
    /// Statements inside `fn () !void`, with `std` and `starh2` in scope.
    snippet,
};

const Block = struct {
    kind: Kind,
    line: usize,
    text: []const u8,
};

const Config = struct {
    readme: []const u8 = "README.md",
    out: []const u8 = ".zig-cache/readme-doctest",
    zig: []const u8 = "",
    /// Relative path from the generated package to this repo, for the path
    /// dependency. Passed in rather than computed, so the build file that
    /// chooses `out` also states the answer.
    repo_rel: []const u8 = "../..",
};

fn abort(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("readme-doctest: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

fn parseArgs(gpa: std.mem.Allocator, process_args: std.process.Args) !Config {
    var cfg: Config = .{};
    var args = try std.process.Args.Iterator.initAllocator(process_args, gpa);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |a| {
        const val = struct {
            fn next(it: *@TypeOf(args), name: []const u8) []const u8 {
                return it.next() orelse abort("{s} needs a value", .{name});
            }
        };
        if (std.mem.eql(u8, a, "--readme")) {
            cfg.readme = try gpa.dupe(u8, val.next(&args, "--readme"));
        } else if (std.mem.eql(u8, a, "--out")) {
            cfg.out = try gpa.dupe(u8, val.next(&args, "--out"));
        } else if (std.mem.eql(u8, a, "--zig")) {
            cfg.zig = try gpa.dupe(u8, val.next(&args, "--zig"));
        } else if (std.mem.eql(u8, a, "--repo-rel")) {
            cfg.repo_rel = try gpa.dupe(u8, val.next(&args, "--repo-rel"));
        } else {
            abort("unknown argument {s}", .{a});
        }
    }
    if (cfg.zig.len == 0) abort("--zig <zig executable> is required", .{});
    return cfg;
}

const marker_prefix = "<!-- doctest:";
const fence = "```";
const zig_fence = "```zig";

/// Extract every ```zig block, with the kind from the marker above it.
/// A zig block without a marker aborts, naming its line.
fn collect(gpa: std.mem.Allocator, src: []const u8, blocks: *std.ArrayList(Block)) !void {
    var line_no: usize = 0;
    var pending: ?Kind = null;
    var it = std.mem.splitScalar(u8, src, '\n');
    while (it.next()) |line| {
        line_no += 1;
        const trimmed = std.mem.trim(u8, line, " \t\r");

        if (std.mem.startsWith(u8, trimmed, marker_prefix)) {
            const rest = trimmed[marker_prefix.len..];
            const close = std.mem.indexOf(u8, rest, "-->") orelse
                abort("line {d}: doctest marker is not closed", .{line_no});
            const name = std.mem.trim(u8, rest[0..close], " \t");
            pending = std.meta.stringToEnum(Kind, name) orelse
                abort("line {d}: unknown doctest kind \"{s}\"", .{ line_no, name });
            continue;
        }

        if (!std.mem.startsWith(u8, trimmed, fence)) continue;

        // A non-zig fence (```sh) clears any stray marker rather than
        // attaching it to a later block.
        if (!std.mem.eql(u8, trimmed, zig_fence)) {
            pending = null;
            // Skip to the end of that block so its contents cannot be read as
            // markers or fences.
            while (it.next()) |inner| {
                line_no += 1;
                if (std.mem.startsWith(u8, std.mem.trim(u8, inner, " \t\r"), fence)) break;
            }
            continue;
        }

        const kind = pending orelse abort(
            "line {d}: a ```zig block with no `{s} <kind> -->` marker above it. " ++
                "Mark it, or this gate does not cover it.",
            .{ line_no, marker_prefix },
        );
        pending = null;
        const open_line = line_no;

        var body: std.ArrayList(u8) = .empty;
        var closed = false;
        while (it.next()) |inner| {
            line_no += 1;
            if (std.mem.startsWith(u8, std.mem.trim(u8, inner, " \t\r"), fence)) {
                closed = true;
                break;
            }
            try body.appendSlice(gpa, inner);
            try body.append(gpa, '\n');
        }
        if (!closed) abort("line {d}: the zig block is never closed", .{open_line});
        try blocks.append(gpa, .{ .kind = kind, .line = open_line, .text = try body.toOwnedSlice(gpa) });
    }
}

/// Raise every top-level `fn` to `pub fn`, so the generated root can reach it.
/// Only a line that starts at column zero is touched, which is what "top level"
/// means in a Zig file.
fn raiseTopLevelFns(gpa: std.mem.Allocator, text: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    var it = std.mem.splitScalar(u8, text, '\n');
    while (it.next()) |line| {
        if (std.mem.startsWith(u8, line, "fn ")) try out.appendSlice(gpa, "pub ");
        try out.appendSlice(gpa, line);
        try out.append(gpa, '\n');
    }
    return out.toOwnedSlice(gpa);
}

/// Copy one `.name = .{ ... },` entry out of a build.zig.zon, brace-matched.
/// The gate declares exactly the dependencies the README's build lines name,
/// with this repo's pins, so the nested build resolves offline and a README
/// that names a dependency nobody pins fails here.
fn extractZonDep(zon: []const u8, name: []const u8, gpa: std.mem.Allocator) !?[]const u8 {
    const needle = try std.fmt.allocPrint(gpa, ".{s} = .{{", .{name});
    const start = std.mem.indexOf(u8, zon, needle) orelse return null;
    var depth: usize = 0;
    var i = start + needle.len - 1; // at the '{'
    while (i < zon.len) : (i += 1) {
        if (zon[i] == '{') depth += 1;
        if (zon[i] == '}') {
            depth -= 1;
            if (depth == 0) {
                const end = if (i + 1 < zon.len and zon[i + 1] == ',') i + 2 else i + 1;
                return zon[start..end];
            }
        }
    }
    return null;
}

/// Every `b.dependency("name"` named by the README's build lines.
fn namedDependencies(gpa: std.mem.Allocator, build_lines: []const u8, out: *std.ArrayList([]const u8)) !void {
    const call = "b.dependency(\"";
    var rest = build_lines;
    while (std.mem.indexOf(u8, rest, call)) |at| {
        rest = rest[at + call.len ..];
        const close = std.mem.indexOfScalar(u8, rest, '"') orelse break;
        const name = rest[0..close];
        // starh2 itself is the path dependency this gate writes.
        if (std.mem.eql(u8, name, "starh2")) continue;
        var seen = false;
        for (out.items) |o| {
            if (std.mem.eql(u8, o, name)) seen = true;
        }
        if (!seen) try out.append(gpa, name);
    }
}

fn isIdentChar(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

/// Does the block use this identifier, as a whole word?
fn mentionsIdent(block: []const u8, name: []const u8) bool {
    var at: usize = 0;
    while (std.mem.indexOfPos(u8, block, at, name)) |i| {
        const before_ok = i == 0 or !isIdentChar(block[i - 1]);
        const end = i + name.len;
        const after_ok = end >= block.len or !isIdentChar(block[end]);
        if (before_ok and after_ok) return true;
        at = i + 1;
    }
    return false;
}

/// `_ = name;` for each fixture name the block does not use. Zig rejects a
/// discard of a constant that IS used ("pointless discard"), and it rejects an
/// unused one, so the wrapper has to name exactly the unused half.
fn discardsFor(gpa: std.mem.Allocator, block: []const u8, names: []const []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    for (names) |name| {
        if (!mentionsIdent(block, name)) try out.print(gpa, "    _ = {s};\n", .{name});
    }
    return out.toOwnedSlice(gpa);
}

fn mentionsField(block: []const u8, comptime field: []const u8) bool {
    return std.mem.indexOf(u8, block, field ++ " =") != null or
        std.mem.indexOf(u8, block, field ++ "=") != null;
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const cfg = try parseArgs(arena, init.minimal.args);

    const cwd = std.Io.Dir.cwd();
    const src = cwd.readFileAlloc(io, cfg.readme, arena, .limited(1024 * 1024)) catch
        abort("cannot read {s}", .{cfg.readme});

    var blocks: std.ArrayList(Block) = .empty;
    try collect(arena, src, &blocks);
    if (blocks.items.len == 0) {
        abort("no ```zig blocks found in {s} — this gate checked nothing", .{cfg.readme});
    }

    // Generate the package.
    const src_dir = try std.fmt.allocPrint(arena, "{s}/src", .{cfg.out});
    cwd.createDirPath(io, src_dir) catch abort("cannot create {s}", .{src_dir});

    var build_lines: std.ArrayList(u8) = .empty;
    var fixtures: std.ArrayList(u8) = .empty;
    var roots: std.ArrayList(u8) = .empty;
    var counts = std.EnumArray(Kind, usize).initFill(0);

    try fixtures.appendSlice(arena,
        \\// GENERATED by tools/readme_doctest.zig. Do not edit.
        \\const std = @import("std");
        \\const starh2 = @import("starh2");
        \\
        \\const doctest_ctx: u8 = 0;
        \\
        \\pub fn doctestNoopHandler(_: *anyopaque, _: *const starh2.Request, _: *starh2.Response) anyerror!void {}
        \\
        \\
    );
    try roots.appendSlice(arena, "    pub const fixtures = @import(\"fixtures.zig\");\n");

    var program_n: usize = 0;
    for (blocks.items, 0..) |b, i| {
        counts.set(b.kind, counts.get(b.kind) + 1);
        switch (b.kind) {
            .build => {
                // Concatenated at function scope, NOT wrapped in a block each:
                // a reader accumulates these lines in one build.zig, so a name
                // declared by an earlier section is in scope for a later one.
                try build_lines.print(arena, "    // README.md line {d}\n{s}\n", .{ b.line, b.text });
            },
            .program => {
                const name = try std.fmt.allocPrint(arena, "prog_{d}", .{program_n});
                program_n += 1;
                const raised = try raiseTopLevelFns(arena, b.text);
                const path = try std.fmt.allocPrint(arena, "{s}/{s}.zig", .{ src_dir, name });
                const data = try std.fmt.allocPrint(
                    arena,
                    "// GENERATED from README.md line {d}. Do not edit.\n{s}",
                    .{ b.line, raised },
                );
                try cwd.writeFile(io, .{ .sub_path = path, .data = data });
                try roots.print(arena, 
                    "    pub const {s} = @import(\"{s}.zig\");\n",
                    .{ name, name },
                );
            },
            .handler => {
                const discards = try discardsFor(arena, b.text, &.{ "req", "resp", "body", "chunk" });
                try fixtures.print(arena,
                    \\// README.md line {d}
                    \\pub fn doctestHandler{d}(_: *anyopaque, req: *const starh2.Request, resp: *starh2.Response) anyerror!void {{
                    \\    const body: []const u8 = "doctest";
                    \\    const chunk: []const u8 = "doctest";
                    \\{s}{s}}}
                    \\
                    \\
                , .{ b.line, i, discards, b.text });
            },
            .route => {
                const discards = try discardsFor(arena, b.text, &.{"h"});
                try fixtures.print(arena,
                    \\// README.md line {d}
                    \\pub fn doctestRoute{d}() void {{
                    \\    const h: starh2.Handler = .{{ .task = .{{ .ptr = @constCast(&doctest_ctx), .runFn = doctestNoopHandler }} }};
                    \\{s}    const routes = [_]starh2.Route{{
                    \\{s}    }};
                    \\    _ = routes;
                    \\}}
                    \\
                    \\
                , .{ b.line, i, discards, b.text });
            },
            .config => {
                // ServerConfig has three fields with no default. The block
                // shows the ones the section is about; the rest are filled so
                // the literal is complete. A field the block DOES name is not
                // filled, because a duplicate field is a compile error.
                var fill: std.ArrayList(u8) = .empty;
                if (!mentionsField(b.text, ".endpoints")) try fill.appendSlice(arena, "        .endpoints = &.{},\n");
                if (!mentionsField(b.text, ".routes")) try fill.appendSlice(arena, "        .routes = &.{},\n");
                if (!mentionsField(b.text, ".tls")) try fill.appendSlice(arena, "        .tls = null,\n");
                const discards = try discardsFor(arena, b.text, &.{ "addr", "cert_pem", "key_pem" });
                try fixtures.print(arena,
                    \\// README.md line {d}
                    \\pub fn doctestConfig{d}() !void {{
                    \\    const addr = try starh2.EndpointAddress.parseIp4("127.0.0.1", 8080);
                    \\    const cert_pem: []const u8 = "";
                    \\    const key_pem: []const u8 = "";
                    \\{s}    const cfg: starh2.ServerConfig = .{{
                    \\{s}{s}    }};
                    \\    _ = cfg;
                    \\}}
                    \\
                    \\
                , .{ b.line, i, discards, b.text, fill.items });
            },
            .snippet => {
                try fixtures.print(arena, 
                    \\// README.md line {d}
                    \\pub fn doctestSnippet{d}() !void {{
                    \\{s}}}
                    \\
                    \\
                , .{ b.line, i, b.text });
            },
        }
    }

    if (counts.get(.build) == 0) {
        abort("no `build` blocks — then nothing proves a consumer can reach any module", .{});
    }

    const fixtures_path = try std.fmt.allocPrint(arena, "{s}/fixtures.zig", .{src_dir});
    try cwd.writeFile(io, .{ .sub_path = fixtures_path, .data = fixtures.items });

    // The root stores each pub function pointer into a runtime variable. A
    // comptime reference does NOT force Zig to analyze the body; this does.
    const root = try std.fmt.allocPrint(arena,
        \\// GENERATED by tools/readme_doctest.zig. Do not edit.
        \\const std = @import("std");
        \\
        \\const files = struct {{
        \\{s}}};
        \\
        \\var sink: ?*const anyopaque = null;
        \\
        \\pub fn main() void {{
        \\    inline for (@typeInfo(files).@"struct".decls) |file_decl| {{
        \\        const file = @field(files, file_decl.name);
        \\        inline for (@typeInfo(file).@"struct".decls) |decl| {{
        \\            const f = @field(file, decl.name);
        \\            if (@typeInfo(@TypeOf(f)) == .@"fn") sink = @ptrCast(&f);
        \\        }}
        \\    }}
        \\}}
        \\
    , .{roots.items});
    const root_path = try std.fmt.allocPrint(arena, "{s}/root.zig", .{src_dir});
    try cwd.writeFile(io, .{ .sub_path = root_path, .data = root });

    // Whatever the README's build lines name, pinned exactly as this repo
    // pins it, so the nested build resolves from the cache and not the network.
    var wanted: std.ArrayList([]const u8) = .empty;
    try namedDependencies(arena, build_lines.items, &wanted);
    const repo_zon = cwd.readFileAlloc(io, "build.zig.zon", arena, .limited(64 * 1024)) catch
        abort("cannot read build.zig.zon", .{});
    var deps: std.ArrayList(u8) = .empty;
    try deps.print(arena, "        .starh2 = .{{ .path = \"{s}\" }},\n", .{cfg.repo_rel});
    for (wanted.items) |name| {
        const entry = try extractZonDep(repo_zon, name, arena) orelse abort(
            "README.md calls b.dependency(\"{s}\"), which build.zig.zon does not pin",
            .{name},
        );
        try deps.print(arena, "        {s}\n", .{entry});
    }

    // The name and the fingerprint move together: zig validates the low half
    // of the fingerprint against the package name and refuses a mismatch.
    const zon = try std.fmt.allocPrint(arena,
        \\.{{
        \\    .name = .readme_consumer,
        \\    .version = "0.0.0",
        \\    .fingerprint = 0x9d5aafd09d46dee4,
        \\    .minimum_zig_version = "0.16.0",
        \\    .dependencies = .{{
        \\{s}    }},
        \\    .paths = .{{ "build.zig", "build.zig.zon", "src" }},
        \\}}
        \\
    , .{deps.items});
    const zon_path = try std.fmt.allocPrint(arena, "{s}/build.zig.zon", .{cfg.out});
    try cwd.writeFile(io, .{ .sub_path = zon_path, .data = zon });

    const build_zig = try std.fmt.allocPrint(arena,
        \\// GENERATED by tools/readme_doctest.zig. Do not edit.
        \\const std = @import("std");
        \\
        \\pub fn build(b: *std.Build) void {{
        \\    const target = b.standardTargetOptions(.{{}});
        \\    const optimize = b.standardOptimizeOption(.{{}});
        \\    const exe_mod = b.createModule(.{{
        \\        .root_source_file = b.path("src/root.zig"),
        \\        .target = target,
        \\        .optimize = optimize,
        \\    }});
        \\
        \\    // ---- README.md build blocks, verbatim, in document order ----
        \\{s}    // --------------------------------------------------------------
        \\
        \\    const exe = b.addExecutable(.{{ .name = "readme-doctest", .root_module = exe_mod }});
        \\    b.installArtifact(exe);
        \\}}
        \\
    , .{build_lines.items});
    const build_path = try std.fmt.allocPrint(arena, "{s}/build.zig", .{cfg.out});
    try cwd.writeFile(io, .{ .sub_path = build_path, .data = build_zig });

    std.debug.print(
        "readme-doctest: {d} zig blocks from {s} — build:{d} program:{d} handler:{d} route:{d} config:{d} snippet:{d}\n",
        .{
            blocks.items.len,     cfg.readme,           counts.get(.build), counts.get(.program),
            counts.get(.handler), counts.get(.route),   counts.get(.config), counts.get(.snippet),
        },
    );

    const build_file = try std.fmt.allocPrint(arena, "{s}/build.zig", .{cfg.out});
    const prefix = try std.fmt.allocPrint(arena, "{s}/zig-out", .{cfg.out});
    var child = std.process.spawn(io, .{
        .argv = &.{
            cfg.zig,      "build",
            "--build-file", build_file,
            // Share this repo's cache, so starh2 and its C dependencies are not
            // compiled a second time for the gate.
            "--cache-dir", ".zig-cache",
            "--prefix",    prefix,
        },
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch abort("cannot spawn {s}", .{cfg.zig});

    const term = child.wait(io) catch abort("the nested build did not finish", .{});
    switch (term) {
        .exited => |code| if (code != 0) abort(
            "a README snippet does not compile. The generated package is at {s} — open the file the error names.",
            .{cfg.out},
        ),
        else => abort("the nested build was killed: {any}", .{term}),
    }

    std.debug.print("readme-doctest PASS: {d} blocks compile as an external consumer\n", .{blocks.items.len});
}
