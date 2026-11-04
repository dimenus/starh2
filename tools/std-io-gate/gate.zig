//! std-io-gate: this repository does not use the std.Io CONCURRENCY API.
//! zio owns concurrency here. Use the zio primitive, or ask Ryan first.
//!
//!   std-io-gate <repo-root>
//!
//! Exit 0: no finding. Exit 1: at least one finding, each printed as
//! `path:line:col: <rule>: <name>` with the source line under it. Exit 3: the
//! scan itself is broken (no file found, a required directory scanned nothing,
//! an unreadable file), because a scan that read nothing has no result.
//!
//! # What is banned, and why
//!
//! `std.Io` itself is the vtable zio implements, so `io: std.Io`, `self.io`
//! for sockets, files and clocks, `std.Io.Cancelable`, `std.Io.Timeout`,
//! `std.Io.net`, `Reader` and `Writer` are all allowed. Banned is the
//! concurrency part of it (`io_decls` below): the futures, groups, selects,
//! batches, queues, locks, events and futexes, the cancel-protection calls,
//! and the alternative runtimes (`Threaded`, `Evented`, ...). Each primitive
//! differs from the zio one under it in a way that already cost a defect:
//! `Select.cancelDiscard` discarded bytes already read off the socket
//! (t-1760); `Future.cancel` is request PLUS mandatory await, so a task that
//! cancels with it blocks forever on a handler in an uncancelable wait
//! (t-1802); `Event.set` on an already-set event neither wakes nor
//! re-synchronizes a waiter that already returned (t-1802).
//!
//! # How it scans
//!
//! Every `.zig` file under the root, recursively, except inside directories
//! named in `skipped_dirs` (dependency and build caches, not our code). No
//! file list and no allowance: one finding fails the build. The scan works on
//! `std.zig.Tokenizer` tokens, so comments, doc comments and string literals
//! are never code, and a reference split across lines is still one reference.
//!
//! Per file it first collects every alias binding, `const|var N = <chain>;`,
//! where a chain is `ident(.ident)*` or `@import("x")(.ident)*`, and resolves
//! them to a fixpoint, so `const S = @import("std"); const I = S.Io;
//! const J = I;` makes `J` an alias of `std.Io` whatever the declaration
//! order. A name bound more than once keeps every binding, and a use is a
//! finding if ANY of them makes it one. Then, at every use:
//!
//! - `io_decl`: a chain that resolves to `std.Io.<banned>`, through any alias.
//!   `<anything>.Io.<banned>` is one too, so an `Io` re-exported by another
//!   of our files is caught where it is used.
//! - `io_alias`: an alias of `std.Io` named anything but `Io`. Under another
//!   name, a use from another file would look like one of our own decls.
//! - `unresolved`: `<receiver>.<BannedType>` where the receiver does not
//!   resolve to `zio`, to a std namespace other than `std.Io`, or to one of
//!   our own files (`own_modules`, a `.zig` path): a parameter, a field, a
//!   call result (`@TypeOf(io).Mutex`), a dependency's module. This is the
//!   rule that fails toward noise. `T.Event` with `T` a `comptime T: type` is
//!   caught here; so is anything this scan cannot follow.
//! - `spawn_call`: `.async(`, `.concurrent(`, the group and batch vtable calls,
//!   `cancelDiscard`, the futex and cancel-protection calls, on ANY receiver,
//!   because zio has none of these names (`rt.io().concurrent(`,
//!   `select.concurrent(`, `io.vtable.groupAsync(` all fail).
//! - `non_zio_call`: `.select(`, `.checkCancel(`, `.recancel(` on a receiver
//!   that does not resolve to `zio`. `zio.select(` passes.
//! - `field_access`: `@field(<std.Io>, "<banned>")`, and `@field(<std or
//!   std.Io>, <not a string literal>)`.
//!
//! # What a token scan cannot see
//!
//! - Reflection over decls: `@typeInfo(std.Io).@"struct".decls` and then
//!   `@field` with a computed name on a receiver that is not std or std.Io.
//! - A decl of one of our own files reached through it (`wire_ring.Event`) is
//!   trusted, because the decl itself was scanned where it is defined. So a
//!   std.Io alias built some way other than a chain binding, and re-exported,
//!   is invisible at the use.
//! - A name bound to std.Io by something other than a plain chain binding,
//!   for example a function that returns `type`, a tuple destructure, or a
//!   `comptime` parameter whose name is also bound to `zio` elsewhere in the
//!   same file (the zio binding exempts it).
//! - Concurrency reached inside a dependency that we call: zig-pkg is not
//!   scanned, so a library function that uses std.Io.Group internally is
//!   invisible.
//! - `.await(` is not banned, because starh2's own test clients have an
//!   `await` method. A `Future` can only be made by a banned call, which fails
//!   first.
//!
//! `std.Thread` (OS threads) is outside this ban.
const std = @import("std");

/// Declarations of `std.Io` that are concurrency (Zig 0.17). Reaching one
/// through a chain that resolves to `std.Io` is an `io_decl` finding.
pub const io_decls = [_][]const u8{
    "Future",                "AnyFuture", "Group",                "Select",
    "Batch",                 "Queue",     "TypeErasedQueue",      "Mutex",
    "Condition",             "Event",     "RwLock",               "Semaphore",
    "CancelProtection",      "Threaded",  "Evented",              "Dispatch",
    "Kqueue",                "Uring",     "fiber",                "async",
    "concurrent",            "select",    "futexWait",            "futexWaitTimeout",
    "futexWaitUncancelable", "futexWake", "swapCancelProtection", "checkCancel",
    "recancel",
};

/// Calls banned on any receiver: zio has no method or function of these
/// names, so any call is a std.Io one.
pub const spawn_calls = [_][]const u8{
    "async",                 "concurrent",    "groupAsync",           "groupConcurrent",
    "groupAwait",            "groupCancel",   "batchAwaitAsync",      "batchAwaitConcurrent",
    "batchCancel",           "cancelDiscard", "futexWait",            "futexWaitTimeout",
    "futexWaitUncancelable", "futexWake",     "swapCancelProtection",
};

/// Calls zio has under the same name: banned unless the receiver resolves
/// to zio.
pub const non_zio_calls = [_][]const u8{ "select", "checkCancel", "recancel" };

/// Directory names never entered, at any depth: fetched dependencies and
/// build output, not code this repository owns.
pub const skipped_dirs = [_][]const u8{ ".git", "zig-pkg", ".zig-cache", "zig-out" };

/// Top-level directories that must each contribute at least one scanned
/// file. A walk that silently lost one of them would otherwise still pass.
pub const required_dirs = [_][]const u8{ "src", "tests", "examples", "tools" };

/// Imports whose source this scan reads (a `.zig` path, or a module built
/// from this repository's files). A decl reached through one of them was
/// already checked where it is defined, so `wire_ring.Event` is starh2's own
/// type, not an unresolved receiver.
pub const own_modules = [_][]const u8{ "starh2", "starh2_h2_client", "starh2_datastar", "h2_client", "h2_oracle" };

pub const Rule = enum { io_decl, io_alias, unresolved, spawn_call, non_zio_call, field_access };

pub const Finding = struct {
    /// Byte offset of the offending name in the source.
    offset: usize,
    rule: Rule,
    name: []const u8,
};

const Root = enum { std, zio, own, other };

/// What a chain prefix resolves to: a root and the components after it.
/// `std.Io` is `.{ .root = .std, .comps = &.{"Io"} }`.
const Path = struct {
    root: Root,
    comps: []const []const u8,
};

const Tok = struct {
    tag: std.zig.Token.Tag,
    start: usize,
    end: usize,
};

fn contains(list: []const []const u8, name: []const u8) bool {
    for (list) |s| if (std.mem.eql(u8, s, name)) return true;
    return false;
}

fn isUpper(name: []const u8) bool {
    return name.len > 0 and name[0] >= 'A' and name[0] <= 'Z';
}

const Scanner = struct {
    arena: std.mem.Allocator,
    src: []const u8,
    toks: []const Tok,
    aliases: std.StringHashMapUnmanaged(std.ArrayList(Path)) = .empty,
    out: *std.ArrayList(Finding),
    out_gpa: std.mem.Allocator,

    fn tag(s: *const Scanner, i: usize) std.zig.Token.Tag {
        return if (i < s.toks.len) s.toks[i].tag else .eof;
    }

    /// An identifier's name; `@"async"` is `async`.
    fn ident(s: *const Scanner, i: usize) []const u8 {
        const t = s.toks[i];
        const text = s.src[t.start..t.end];
        if (text.len >= 3 and text[0] == '@' and text[1] == '"') return text[2 .. text.len - 1];
        return text;
    }

    fn stringBody(s: *const Scanner, i: usize) []const u8 {
        const t = s.toks[i];
        return s.src[t.start + 1 .. t.end - 1];
    }

    fn isBuiltin(s: *const Scanner, i: usize, name: []const u8) bool {
        if (s.tag(i) != .builtin) return false;
        return std.mem.eql(u8, s.src[s.toks[i].start..s.toks[i].end], name);
    }

    /// `@import ( "x" )` at `i`: the root it names, or null.
    fn importAt(s: *const Scanner, i: usize) ?Root {
        if (!s.isBuiltin(i, "@import")) return null;
        if (s.tag(i + 1) != .l_paren or s.tag(i + 2) != .string_literal or s.tag(i + 3) != .r_paren) return null;
        const name = s.stringBody(i + 2);
        if (std.mem.eql(u8, name, "std")) return .std;
        if (std.mem.eql(u8, name, "zio")) return .zio;
        if (contains(&own_modules, name)) return .own;
        if (std.mem.endsWith(u8, name, ".zig") and std.mem.indexOf(u8, name, "zig-pkg") == null) return .own;
        return .other;
    }

    fn report(s: *Scanner, i: usize, rule: Rule, name: []const u8) !void {
        try s.out.append(s.out_gpa, .{ .offset = s.toks[i].start, .rule = rule, .name = name });
    }

    fn addAlias(s: *Scanner, name: []const u8, p: Path) !bool {
        const gop = try s.aliases.getOrPut(s.arena, name);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        for (gop.value_ptr.items) |q| {
            if (q.root != p.root or q.comps.len != p.comps.len) continue;
            var same = true;
            for (q.comps, p.comps) |a, b| {
                if (!std.mem.eql(u8, a, b)) same = false;
            }
            if (same) return false;
        }
        try gop.value_ptr.append(s.arena, p);
        return true;
    }

    /// The candidate paths of a chain head identifier. An unbound `std` is
    /// std; anything else unbound is `.other`.
    fn headPaths(s: *Scanner, name: []const u8) ![]const Path {
        if (s.aliases.get(name)) |list| return list.items;
        const root: Root = if (std.mem.eql(u8, name, "std")) .std else .other;
        const one = try s.arena.alloc(Path, 1);
        one[0] = .{ .root = root, .comps = &.{} };
        return one;
    }

    fn extend(s: *Scanner, paths: []const Path, comp: []const u8) ![]const Path {
        const next = try s.arena.alloc(Path, paths.len);
        for (paths, next) |p, *n| {
            const comps = try s.arena.alloc([]const u8, p.comps.len + 1);
            @memcpy(comps[0..p.comps.len], p.comps);
            comps[p.comps.len] = comp;
            n.* = .{ .root = p.root, .comps = comps };
        }
        return next;
    }

    const Binding = struct {
        name: []const u8,
        /// Index of the bound name's token.
        name_tok: usize,
        /// Index of the chain's first token.
        first: usize,
        /// Index one past the chain's last token (the `;`).
        end: usize,
    };

    /// Parses `const|var N [: T] = <chain> ;` at `i`. Returns the binding if
    /// the whole initializer is a chain.
    fn bindingAt(s: *const Scanner, i: usize) ?Binding {
        if (s.tag(i) != .keyword_const and s.tag(i) != .keyword_var) return null;
        if (s.tag(i + 1) != .identifier) return null;
        var j = i + 2;
        if (s.tag(j) == .colon) {
            var depth: usize = 0;
            while (j < s.toks.len) : (j += 1) {
                switch (s.tag(j)) {
                    .l_paren, .l_bracket, .l_brace => depth += 1,
                    .r_paren, .r_bracket, .r_brace => {
                        if (depth == 0) return null;
                        depth -= 1;
                    },
                    .equal => if (depth == 0) break,
                    .semicolon, .eof => return null,
                    else => {},
                }
            }
        }
        if (s.tag(j) != .equal) return null;
        const first = j + 1;
        var k = first;
        if (s.importAt(k) != null) {
            k += 4;
        } else if (s.tag(k) == .identifier) {
            k += 1;
        } else return null;
        while (s.tag(k) == .period and s.tag(k + 1) == .identifier) k += 2;
        if (s.tag(k) != .semicolon) return null;
        return .{ .name = s.ident(i + 1), .name_tok = i + 1, .first = first, .end = k };
    }

    /// Resolves a binding's chain with the aliases known so far. Null if the
    /// head is not known yet (another round may resolve it).
    fn resolveBinding(s: *Scanner, b: Binding) !?[]const Path {
        var k = b.first;
        var paths: []const Path = undefined;
        if (s.importAt(k)) |root| {
            const one = try s.arena.alloc(Path, 1);
            one[0] = .{ .root = root, .comps = &.{} };
            paths = one;
            k += 4;
        } else {
            const head = s.ident(k);
            if (s.aliases.get(head) == null and !std.mem.eql(u8, head, "std")) return null;
            paths = try s.headPaths(head);
            k += 1;
        }
        while (k < b.end) : (k += 2) paths = try s.extend(paths, s.ident(k + 1));
        return paths;
    }

    fn collectAliases(s: *Scanner) !void {
        var bindings: std.ArrayList(Binding) = .empty;
        for (0..s.toks.len) |i| {
            if (s.bindingAt(i)) |b| try bindings.append(s.arena, b);
        }
        // Fixpoint: each round can only add a path, and there are finitely
        // many bindings, so this stops. The bound is a guard, not a limit
        // the scan relies on.
        var round: usize = 0;
        while (round <= bindings.items.len) : (round += 1) {
            var changed = false;
            for (bindings.items) |b| {
                const paths = (try s.resolveBinding(b)) orelse continue;
                for (paths) |p| {
                    if (try s.addAlias(b.name, p)) changed = true;
                }
            }
            if (!changed) break;
        }
        // An alias of std.Io must be called `Io`. Another file reaches it as
        // `mod.Io.<banned>`, which the `.Io.` rule in `checkComp` catches;
        // under any other name that use would look like starh2's own decl.
        for (bindings.items) |b| {
            if (std.mem.eql(u8, b.name, "Io")) continue;
            const paths = (try s.resolveBinding(b)) orelse continue;
            for (paths) |p| {
                if (p.root == .std and p.comps.len == 1 and std.mem.eql(u8, p.comps[0], "Io")) {
                    try s.report(b.name_tok, .io_alias, b.name);
                    break;
                }
            }
        }
    }

    /// Checks one component `name` reached from `recv` (the receiver's
    /// candidate paths). `call` is whether `(` follows it.
    fn checkComp(s: *Scanner, i: usize, recv: []const Path, name: []const u8, call: bool) !void {
        if (call and contains(&spawn_calls, name)) return s.report(i, .spawn_call, name);
        var all_zio = true;
        var io_hit = false;
        var unresolved_hit = false;
        for (recv) |p| {
            switch (p.root) {
                .zio => {},
                .std => {
                    all_zio = false;
                    if (p.comps.len == 1 and std.mem.eql(u8, p.comps[0], "Io") and contains(&io_decls, name)) io_hit = true;
                },
                .own, .other => {
                    all_zio = false;
                    // `mod.Io.Event`: an `Io` re-exported from another file.
                    const via_io = p.comps.len > 0 and std.mem.eql(u8, p.comps[p.comps.len - 1], "Io");
                    if (via_io and contains(&io_decls, name)) io_hit = true;
                    if (p.root == .other and isUpper(name) and contains(&io_decls, name)) unresolved_hit = true;
                },
            }
        }
        if (io_hit) return s.report(i, .io_decl, name);
        if (call and !all_zio and contains(&non_zio_calls, name)) return s.report(i, .non_zio_call, name);
        if (unresolved_hit) return s.report(i, .unresolved, name);
    }

    fn unknownPath(s: *Scanner) ![]const Path {
        const one = try s.arena.alloc(Path, 1);
        one[0] = .{ .root = .other, .comps = &.{} };
        return one;
    }

    /// Walks the chain whose head is at `i`, checking each component.
    /// Returns the paths of the whole chain and the index after it.
    fn walkChain(s: *Scanner, i: usize, head: []const Path) !struct { []const Path, usize } {
        var paths = head;
        var k = i;
        while (s.tag(k) == .period and s.tag(k + 1) == .identifier) {
            const name = s.ident(k + 1);
            try s.checkComp(k + 1, paths, name, s.tag(k + 2) == .l_paren);
            paths = try s.extend(paths, name);
            k += 2;
        }
        return .{ paths, k };
    }

    /// Resolves a pure chain starting at `i` without reporting, for the
    /// first argument of `@field`. Null if it is not a chain.
    fn quietChain(s: *Scanner, i: usize) !?struct { []const Path, usize } {
        var k = i;
        var paths: []const Path = undefined;
        if (s.importAt(k)) |root| {
            const one = try s.arena.alloc(Path, 1);
            one[0] = .{ .root = root, .comps = &.{} };
            paths = one;
            k += 4;
        } else if (s.tag(k) == .identifier) {
            paths = try s.headPaths(s.ident(k));
            k += 1;
        } else return null;
        while (s.tag(k) == .period and s.tag(k + 1) == .identifier) : (k += 2) {
            paths = try s.extend(paths, s.ident(k + 1));
        }
        return .{ paths, k };
    }

    fn checkField(s: *Scanner, i: usize) !void {
        if (s.tag(i + 1) != .l_paren) return;
        const chain = (try s.quietChain(i + 2)) orelse return;
        const paths, const after = chain;
        if (s.tag(after) != .comma) return;
        const arg = after + 1;
        if (s.tag(arg) == .string_literal and (s.tag(arg + 1) == .r_paren or s.tag(arg + 1) == .comma)) {
            const name = s.stringBody(arg);
            for (paths) |p| {
                if (p.root == .std and p.comps.len == 1 and std.mem.eql(u8, p.comps[0], "Io") and contains(&io_decls, name)) {
                    return s.report(arg, .field_access, name);
                }
            }
            return;
        }
        // A computed name: banned when the receiver is std or std.Io, since
        // either can reach a banned decl that no token here spells.
        for (paths) |p| {
            if (p.root != .std) continue;
            if (p.comps.len == 0 or (p.comps.len == 1 and std.mem.eql(u8, p.comps[0], "Io"))) {
                return s.report(arg, .field_access, "<computed>");
            }
        }
    }

    fn scanUses(s: *Scanner) !void {
        var i: usize = 0;
        while (i < s.toks.len) {
            const t = s.tag(i);
            if (t == .identifier and (i == 0 or s.tag(i - 1) != .period)) {
                _, i = try s.walkChain(i + 1, try s.headPaths(s.ident(i)));
                continue;
            }
            if (t == .builtin) {
                if (s.importAt(i)) |root| {
                    const one = try s.arena.alloc(Path, 1);
                    one[0] = .{ .root = root, .comps = &.{} };
                    _, i = try s.walkChain(i + 4, one);
                    continue;
                }
                if (s.isBuiltin(i, "@field")) try s.checkField(i);
                i += 1;
                continue;
            }
            // A call result, an index, an unwrap or a deref: the receiver is
            // not a name, so it cannot resolve to zio.
            const opaque_recv = t == .r_paren or t == .r_bracket or t == .period_asterisk or
                (t == .question_mark and i > 0 and s.tag(i - 1) == .period);
            if (opaque_recv and s.tag(i + 1) == .period and s.tag(i + 2) == .identifier) {
                _, i = try s.walkChain(i + 1, try s.unknownPath());
                continue;
            }
            // A period with no receiver (`.{ .x = }`, a decl literal
            // `.init()`): only the any-receiver calls apply.
            if (t == .period and s.tag(i + 1) == .identifier and s.tag(i + 2) == .l_paren) {
                const name = s.ident(i + 1);
                if (contains(&spawn_calls, name)) try s.report(i + 1, .spawn_call, name);
                if (contains(&non_zio_calls, name)) try s.report(i + 1, .non_zio_call, name);
                i += 2;
                continue;
            }
            i += 1;
        }
    }
};

/// Scans one source file. Findings are appended to `out`; each `name` is
/// a slice of `source` or a string literal, never arena memory.
pub fn scan(gpa: std.mem.Allocator, source: [:0]const u8, out: *std.ArrayList(Finding)) !void {
    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var toks: std.ArrayList(Tok) = .empty;
    var tokenizer: std.zig.Tokenizer = .init(source);
    while (true) {
        const t = tokenizer.next();
        switch (t.tag) {
            .eof => break,
            .doc_comment, .container_doc_comment => continue,
            else => try toks.append(arena, .{ .tag = t.tag, .start = t.loc.start, .end = t.loc.end }),
        }
    }

    var s: Scanner = .{ .arena = arena, .src = source, .toks = toks.items, .out = out, .out_gpa = gpa };
    try s.collectAliases();
    try s.scanUses();
}

fn skipped(name: []const u8) bool {
    return contains(&skipped_dirs, name);
}

pub fn main(init: std.process.Init) !u8 {
    const gpa = init.gpa;
    const io = init.io;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args.deinit();
    _ = args.next();
    const root_path = args.next() orelse {
        std.debug.print("usage: std-io-gate <repo-root>\n", .{});
        return 3;
    };

    var root = std.Io.Dir.cwd().openDir(io, root_path, .{ .iterate = true }) catch |err| {
        std.debug.print("std-io-gate FATAL: cannot open {s}: {t}\n", .{ root_path, err });
        return 3;
    };
    defer root.close(io);

    var arena_state: std.heap.ArenaAllocator = .init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var files: std.ArrayList([]const u8) = .empty;
    var walker = try root.walkSelectively(gpa);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        switch (entry.kind) {
            .directory => if (!skipped(entry.basename)) try walker.enter(io, entry),
            // A symlinked file is read through the link; a symlinked
            // directory is not entered (no cycles), so say so.
            .file, .sym_link => if (std.mem.endsWith(u8, entry.basename, ".zig")) {
                try files.append(arena, try arena.dupe(u8, entry.path));
            } else if (entry.kind == .sym_link) {
                std.debug.print("std-io-gate: not following symlink {s}\n", .{entry.path});
            },
            else => {},
        }
    }
    std.mem.sort([]const u8, files.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);

    var per_dir: [required_dirs.len]usize = @splat(0);
    var lines: usize = 0;
    var total: usize = 0;
    for (files.items) |path| {
        const source = root.readFileAllocOptions(io, path, arena, .limited(64 << 20), .of(u8), 0) catch |err| {
            std.debug.print("std-io-gate FATAL: cannot read {s}: {t}\n", .{ path, err });
            return 3;
        };
        lines += std.mem.count(u8, source, "\n");
        for (required_dirs, &per_dir) |d, *n| {
            if (std.mem.startsWith(u8, path, d) and path.len > d.len and path[d.len] == std.fs.path.sep) n.* += 1;
        }
        var found: std.ArrayList(Finding) = .empty;
        defer found.deinit(gpa);
        try scan(gpa, source, &found);
        for (found.items) |f| {
            var line: usize = 1;
            var line_start: usize = 0;
            for (source[0..f.offset], 0..) |c, at| {
                if (c == '\n') {
                    line += 1;
                    line_start = at + 1;
                }
            }
            const line_end = std.mem.indexOfScalarPos(u8, source, f.offset, '\n') orelse source.len;
            std.debug.print("{s}:{d}:{d}: {t}: {s}\n    {s}\n", .{
                path,                                                    line, f.offset - line_start + 1, f.rule, f.name,
                std.mem.trim(u8, source[line_start..line_end], " \t\r"),
            });
        }
        total += found.items.len;
    }

    std.debug.print("std-io-gate: scanned {d} files, {d} lines under {s} (", .{ files.items.len, lines, root_path });
    for (required_dirs, per_dir, 0..) |d, n, idx| {
        std.debug.print("{s}{s}/ {d}", .{ if (idx == 0) "" else ", ", d, n });
    }
    std.debug.print("; skipped dirs named", .{});
    for (skipped_dirs) |d| std.debug.print(" {s}", .{d});
    std.debug.print(")\n", .{});

    if (files.items.len == 0) {
        std.debug.print("std-io-gate FATAL: scanned zero files\n", .{});
        return 3;
    }
    for (required_dirs, per_dir) |d, n| {
        if (n == 0) {
            std.debug.print("std-io-gate FATAL: scanned zero files under {s}/\n", .{d});
            return 3;
        }
    }
    if (total != 0) {
        std.debug.print("std-io-gate: {d} banned std.Io concurrency uses. Use zio, or ask Ryan.\n", .{total});
        return 1;
    }
    std.debug.print("std-io-gate: 0 banned uses\n", .{});
    return 0;
}

fn expectFindings(source: [:0]const u8, want: []const Rule) !void {
    var out: std.ArrayList(Finding) = .empty;
    defer out.deinit(std.testing.allocator);
    try scan(std.testing.allocator, source, &out);
    errdefer for (out.items) |f| std.debug.print("  got {t} {s} at {d}\n", .{ f.rule, f.name, f.offset });
    try std.testing.expectEqual(want.len, out.items.len);
    for (want, out.items) |w, f| try std.testing.expectEqual(w, f.rule);
}

test "direct std.Io types and calls" {
    try expectFindings(
        \\const std = @import("std");
        \\var e: std.Io.Event = .unset;
        \\var q = std.Io.Queue(u8).init(&buf);
        \\var g: std.Io.Group = .init;
        \\const f = std.Io.concurrent;
    , &.{ .io_decl, .io_decl, .io_decl, .io_decl });
}

test "an alias of std.Io, in any order and through other aliases" {
    try expectFindings(
        \\var x: Io.Event = .unset;
        \\const Io = std.Io;
        \\const std = @import("std");
    , &.{.io_decl});
    try expectFindings(
        \\const S = @import("std");
        \\const I = S.Io;
        \\const J = I;
        \\var q: J.Queue(u8) = undefined;
        \\var m: @import("std").Io.Mutex = .init;
    , &.{ .io_alias, .io_alias, .io_decl, .io_decl });
}

test "async, concurrent and select whatever the receiver is called" {
    try expectFindings(
        \\var a = try io.concurrent(f, .{});
        \\var b = rt.io().async(f, .{});
        \\var c = try ctx.io.concurrent(f, .{});
        \\try select.concurrent(.byte, f, .{});
        \\_ = Io.async(io, f, .{});
        \\_ = io.@"concurrent"(f, .{});
        \\_ = io.select(.{});
        \\io.vtable.groupAsync(io.userdata, g, c, s);
        \\select.cancelDiscard();
    , &.{ .spawn_call, .spawn_call, .spawn_call, .spawn_call, .spawn_call, .spawn_call, .non_zio_call, .spawn_call, .spawn_call });
}

test "zio is allowed, under its own name or an alias" {
    try expectFindings(
        \\const std = @import("std");
        \\const zio = @import("zio");
        \\const z = zio;
        \\var e: zio.Event = .init;
        \\var m: zio.Mutex = .init;
        \\var g: zio.Group = .init;
        \\const w = try zio.select(.{ .a = &h });
        \\const v = try z.select(.{ .a = &h });
        \\var h = try zio.spawn(f, .{});
        \\try zio.checkCancel();
        \\var c: @import("zio").Condition = .init;
    , &.{});
}

test "the vtable itself is allowed" {
    try expectFindings(
        \\const std = @import("std");
        \\fn f(self: *Conn, io: std.Io) std.Io.Cancelable!void {
        \\    try self.io.sleep(.fromSeconds(1), .awake);
        \\    const now = std.Io.Clock.awake.now(io);
        \\    const s: std.Io.net.Stream = try addr.connect(io, .{ .mode = .stream });
        \\    var r = s.reader(io, &buf);
        \\    const t: std.Io.Timeout = .none;
        \\    var m: std.Thread.Mutex = .{};
        \\    self.mu.lock();
        \\}
    , &.{});
}

test "comments and strings are not code, and do not hide code" {
    try expectFindings(
        \\// var e: std.Io.Event = .unset;
        \\/// std.Io.Select discards bytes
        \\const s = "std.Io.Event io.concurrent(";
        \\const m =
        \\    \\std.Io.Mutex
        \\;
    , &.{});
    // The old sed gate cut every line at `//`, so this line hid its use.
    try expectFindings(
        \\const u = "http://x"; var e: std.Io.Event = .unset;
    , &.{.io_decl});
}

test "a reference split across lines is one reference" {
    try expectFindings(
        \\var m: std.Io
        \\    .Mutex = .init;
    , &.{.io_decl});
}

test "receivers the scan cannot resolve fail toward noise" {
    try expectFindings(
        \\fn f(comptime T: type) void { var e: T.Event = .unset; }
        \\var m: @TypeOf(io).Mutex = .init;
        \\var q: mod.thing.Queue(u8) = undefined;
        \\var x = arr[0].Select(u8);
    , &.{ .unresolved, .unresolved, .unresolved, .unresolved });
}

test "our own modules resolve; a dependency's module does not" {
    try expectFindings(
        \\const wire_ring = @import("wire_ring.zig");
        \\const starh2 = @import("starh2");
        \\fn ringPeerClosed(self: *Conn, ev: ?wire_ring.Event) void {}
        \\var q: starh2.edge.Queue = undefined;
        \\var e: @import("io_queue.zig").Queue = undefined;
    , &.{});
    try expectFindings(
        \\const tls = @import("tls");
        \\var q: tls.Queue = undefined;
        \\const vendored = @import("../zig-pkg/x/src/x.zig");
        \\var m: vendored.Mutex = .init;
    , &.{ .unresolved, .unresolved });
}

test "an Io re-exported by another file is caught at the use" {
    try expectFindings(
        \\const a = @import("a.zig");
        \\var e: a.Io.Event = .unset;
        \\const MyIo = a.Io;
        \\var q: MyIo.Queue(u8) = undefined;
    , &.{ .io_decl, .io_decl });
    try expectFindings(
        \\const std = @import("std");
        \\pub const Io = std.Io;
        \\pub const StdIo = std.Io;
    , &.{.io_alias});
}

test "@field on std.Io" {
    try expectFindings(
        \\const std = @import("std");
        \\var e: @field(std.Io, "Event") = .unset;
        \\const T = @field(std.Io, name);
        \\const U = @field(std, name);
        \\const ok = @field(self, "event");
    , &.{ .field_access, .field_access, .field_access });
}
