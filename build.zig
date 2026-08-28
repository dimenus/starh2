const std = @import("std");

comptime {
    const min = "0.16.0";
    const ver = @import("builtin").zig_version;
    if (ver.major != 0 or ver.minor != 16) {
        @compileError("starh2 requires Zig " ++ min ++ ", found a different minor");
    }
}

const brotli_common_sources = [_][]const u8{
    "common/constants.c",
    "common/context.c",
    "common/dictionary.c",
    "common/platform.c",
    "common/shared_dictionary.c",
    "common/transform.c",
};

const brotli_enc_sources = [_][]const u8{
    "enc/backward_references.c",
    "enc/backward_references_hq.c",
    "enc/bit_cost.c",
    "enc/block_splitter.c",
    "enc/brotli_bit_stream.c",
    "enc/cluster.c",
    "enc/command.c",
    "enc/compound_dictionary.c",
    "enc/compress_fragment.c",
    "enc/compress_fragment_two_pass.c",
    "enc/dictionary_hash.c",
    "enc/encode.c",
    "enc/encoder_dict.c",
    "enc/entropy_encode.c",
    "enc/fast_log.c",
    "enc/histogram.c",
    "enc/literal_cost.c",
    "enc/memory.c",
    "enc/metablock.c",
    "enc/static_dict.c",
    "enc/utf8_util.c",
};

const brotli_dec_sources = [_][]const u8{
    "dec/bit_reader.c",
    "dec/decode.c",
    "dec/huffman.c",
    "dec/state.c",
};

fn brotliFlags(target: std.Build.ResolvedTarget) []const []const u8 {
    // BROTLI_ENCODER_CLEANUP_ON_OOM: without it, enc/memory.h defaults to
    // BROTLI_ENCODER_EXIT_ON_OOM and the first null custom-alloc calls exit(1),
    // taking the whole server with it. With CLEANUP, CompressStream returns
    // BROTLI_FALSE and our Zig error paths (identity pre-commit / RST post-commit)
    // engage.
    return switch (target.result.os.tag) {
        .macos => &[_][]const u8{ "-std=c99", "-O2", "-DOS_MACOSX", "-DBROTLI_ENCODER_CLEANUP_ON_OOM" },
        .linux => &[_][]const u8{ "-std=c99", "-O2", "-DOS_LINUX", "-DBROTLI_ENCODER_CLEANUP_ON_OOM" },
        else => &[_][]const u8{ "-std=c99", "-O2", "-DBROTLI_ENCODER_CLEANUP_ON_OOM" },
    };
}

/// Brotli is linked as a static library rather than `addCSourceFiles` on the
/// starh2 module. Fuzz rebuilds (`-ffuzz`) instrument every C source on the
/// module and then fail to resolve `___sanitizer_cov_*` from libbrotli's
/// object files; a prebuilt static lib is not re-instrumented.
fn brotliEncLib(
    b: *std.Build,
    brotli: *std.Build.Dependency,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    name: []const u8,
) *std.Build.Step.Compile {
    const mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    mod.addIncludePath(brotli.path("c/include"));
    mod.addCSourceFiles(.{
        .root = brotli.path("c"),
        .files = &(brotli_common_sources ++ brotli_enc_sources),
        .flags = brotliFlags(target),
    });
    return b.addLibrary(.{
        .name = name,
        .linkage = .static,
        .root_module = mod,
    });
}

fn brotliDecLib(
    b: *std.Build,
    brotli: *std.Build.Dependency,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    name: []const u8,
) *std.Build.Step.Compile {
    const mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    mod.addIncludePath(brotli.path("c/include"));
    mod.addCSourceFiles(.{
        .root = brotli.path("c"),
        .files = &brotli_dec_sources,
        .flags = brotliFlags(target),
    });
    return b.addLibrary(.{
        .name = name,
        .linkage = .static,
        .root_module = mod,
    });
}

fn linkBrotliEnc(mod: *std.Build.Module, brotli: *std.Build.Dependency, lib: *std.Build.Step.Compile) void {
    mod.addIncludePath(brotli.path("c/include"));
    mod.link_libc = true;
    mod.linkLibrary(lib);
}

fn linkBrotliDec(mod: *std.Build.Module, brotli: *std.Build.Dependency, lib: *std.Build.Step.Compile) void {
    mod.addIncludePath(brotli.path("c/include"));
    mod.link_libc = true;
    mod.linkLibrary(lib);
}

fn attachStarh2Options(b: *std.Build, mod: *std.Build.Module, observe: bool) void {
    const opts = b.addOptions();
    opts.addOption(bool, "observe", observe);
    mod.addOptions("build_options", opts);
}

/// Copy `tools/build-boringssl.sh` over the fetched boring package *before*
/// `b.dependency("boring")` hashes that script. Zig's glibc headers make
/// memchr const-generic; without the wrapper flag, aarch64-linux-gnu -Werror
/// fails BoringSSL.
fn overlayBoringBuildScript(b: *std.Build) void {
    const io = b.graph.io;
    const root = b.build_root.handle;
    root.access(io, "tools/build-boringssl.sh", .{}) catch return;
    var pkg = root.openDir(io, "zig-pkg", .{ .iterate = true }) catch return;
    defer pkg.close(io);
    var it = pkg.iterate();
    while (it.next(io) catch return) |entry| {
        if (entry.kind != .directory) continue;
        if (!std.mem.startsWith(u8, entry.name, "boring-")) continue;
        var dest = pkg.openDir(io, entry.name, .{}) catch continue;
        defer dest.close(io);
        _ = root.updateFile(io, "tools/build-boringssl.sh", dest, "tools/build-boringssl.sh", .{}) catch continue;
    }
}

fn resolveBoringsslSource(b: *std.Build) []const u8 {
    if (b.option([]const u8, "boringssl-source-path", "Path to a BoringSSL source checkout")) |path| {
        return path;
    }
    const candidates = [_][]const u8{
        "vendor/boringssl",
        "../../oss/http2-zig-hendrik/boringssl",
    };
    for (candidates) |candidate| {
        const cmake = b.pathJoin(&.{ candidate, "CMakeLists.txt" });
        b.build_root.handle.access(b.graph.io, cmake, .{}) catch continue;
        return candidate;
    }
    std.process.fatal(
        "BoringSSL source not found. Pass -Dboringssl-source-path=... or place a checkout at vendor/boringssl",
        .{},
    );
}

fn boringModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    source_path: []const u8,
) *std.Build.Module {
    overlayBoringBuildScript(b);
    const dep = b.dependency("boring", .{
        .target = target,
        .optimize = optimize,
        .@"boringssl-source-path" = source_path,
    });
    return dep.module("boring");
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const observe = b.option(
        bool,
        "observe",
        "Keep test-only hot counters in non-Debug artifacts (A/B the counter tax)",
    ) orelse false;
    // Gate builds are Debug; ReleaseFast benches compile the counters out unless
    // `-Dobserve=true`. Do not read `builtin.mode` inside connection.zig — an
    // imported module's mode is not the test artifact's mode.
    const observe_hot = observe or (optimize == .Debug);
    const boringssl_source_path = resolveBoringsslSource(b);

    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });
    // Lazy: the core `starh2` module does not use the Datastar SDK, so a
    // consumer of an HTTP/2 server must not be made to fetch and compile a
    // hypermedia SDK. Only `starh2_datastar` and the examples ask for it.
    const datastar_dep = b.lazyDependency("datastar", .{
        .target = target,
        .optimize = optimize,
    });
    const boring_mod = boringModule(b, target, optimize, boringssl_source_path);
    const brotli_dep = b.dependency("brotli", .{
        .target = target,
        .optimize = optimize,
    });

    const starh2_mod = b.addModule("starh2", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "boring", .module = boring_mod },
            // src/edge/tls.zig is a zio.CompletionQueue driver; the core
            // module carries the zio dependency openly (t-878).
            .{ .name = "zio", .module = zio_dep.module("zio") },
        },
    });
    attachStarh2Options(b, starh2_mod, observe_hot);
    // Encoder only on the production module (static lib — see brotliEncLib).
    // Decoder is linked only into test artifacts that round-trip.
    const brotli_enc = brotliEncLib(b, brotli_dep, target, optimize, "brotli_enc");
    const brotli_dec = brotliDecLib(b, brotli_dep, target, optimize, "brotli_dec");
    linkBrotliEnc(starh2_mod, brotli_dep, brotli_enc);
    // The Datastar signal convention: which query parameter carries signals and
    // how they are bounded. It needs NO Datastar SDK — reading signals is
    // form-decoding plus std.json — so nothing starh2 ships depends on the SDK.
    // A consumer that also wants the SDK's emitters declares the SDK itself,
    // at whatever version it wants, rather than inheriting starh2's pin.
    const datastar_mod = b.addModule("starh2_datastar", .{
        .root_source_file = b.path("src/datastar.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "starh2", .module = starh2_mod },
        },
    });

    // Exported for dependents that need h2 client wire bytes (std.http.Client is h1-only).
    const h2_client_mod = b.addModule("starh2_h2_client", .{
        .root_source_file = b.path("src/h2_client.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "starh2", .module = starh2_mod },
        },
    });

    // Separate root module so decoder C sources stay off the production starh2_mod.
    // `addTest(.{ .root_module = starh2_mod })` plus addBrotliDec would mutate
    // the shared module and double-link decoder into every test that also calls
    // addBrotliDec (compression tests).
    const lib_test_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "boring", .module = boring_mod },
            .{ .name = "zio", .module = zio_dep.module("zio") },
        },
    });
    attachStarh2Options(b, lib_test_mod, true);
    linkBrotliEnc(lib_test_mod, brotli_dep, brotli_enc);
    linkBrotliDec(lib_test_mod, brotli_dep, brotli_dec);
    const lib_tests = b.addTest(.{
        .root_module = lib_test_mod,
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    const protocol_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/protocol.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
            },
        }),
    });
    const run_protocol_tests = b.addRunArtifact(protocol_tests);

    const http1_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/http1.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_http1_tests = b.addRunArtifact(http1_tests);
    const http1_step = b.step("test-http1", "Run HTTP/1.1 oneshot gates");
    http1_step.dependOn(&run_http1_tests.step);

    const h1_battery_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/h1_battery.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
            },
        }),
    });
    const run_h1_battery = b.addRunArtifact(h1_battery_tests);
    run_h1_battery.setCwd(b.path("."));
    run_h1_battery.has_side_effects = true;
    const h1_battery_step = b.step("test-h1-battery", "Run HTTP/1.1 edge-channel battery");
    h1_battery_step.dependOn(&run_h1_battery.step);

    const limits_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/limits.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_limits_tests = b.addRunArtifact(limits_tests);

    const transport_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/transport.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_transport_tests = b.addRunArtifact(transport_tests);

    const multiplex_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/multiplex.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
            },
        }),
    });
    const run_multiplex_tests = b.addRunArtifact(multiplex_tests);

    const interop_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/interop.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
            },
        }),
    });
    const run_interop_tests = b.addRunArtifact(interop_tests);

    const regression_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/regressions.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
            },
        }),
    });
    const run_regression_tests = b.addRunArtifact(regression_tests);

    const compression_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/compression.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    linkBrotliDec(compression_tests.root_module, brotli_dep, brotli_dec);
    const run_compression_tests = b.addRunArtifact(compression_tests);
    const compression_step = b.step("test-compression", "Run compression gates only");
    compression_step.dependOn(&run_compression_tests.step);

    const lifecycle_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/lifecycle.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_lifecycle_tests = b.addRunArtifact(lifecycle_tests);
    const lifecycle_step = b.step("test-lifecycle", "Run lifecycle gates only");
    lifecycle_step.dependOn(&run_lifecycle_tests.step);

    const backend_parity_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/backend_parity.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_backend_parity_tests = b.addRunArtifact(backend_parity_tests);
    const backend_parity_step = b.step("test-backend-parity", "Run std.Io backend parity and wakeup gates");
    backend_parity_step.dependOn(&run_backend_parity_tests.step);

    const live_exact_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/live_exact.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_live_exact_tests = b.addRunArtifact(live_exact_tests);

    const writer_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/writer.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_writer_tests = b.addRunArtifact(writer_tests);

    const scheduler_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/scheduler.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
            },
        }),
    });
    const run_scheduler_tests = b.addRunArtifact(scheduler_tests);

    const deadline_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/deadlines.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_deadline_tests = b.addRunArtifact(deadline_tests);
    const deadline_step = b.step("test-deadlines", "Run actor-deadline heap gates");
    deadline_step.dependOn(&run_deadline_tests.step);

    const test_step = b.step("test", "Run unit and integration tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_protocol_tests.step);
    test_step.dependOn(&run_http1_tests.step);
    test_step.dependOn(&run_h1_battery.step);
    test_step.dependOn(&run_limits_tests.step);
    test_step.dependOn(&run_transport_tests.step);
    test_step.dependOn(&run_multiplex_tests.step);
    test_step.dependOn(&run_interop_tests.step);
    test_step.dependOn(&run_regression_tests.step);
    test_step.dependOn(&run_lifecycle_tests.step);
    test_step.dependOn(&run_backend_parity_tests.step);
    test_step.dependOn(&run_live_exact_tests.step);
    test_step.dependOn(&run_writer_tests.step);
    test_step.dependOn(&run_scheduler_tests.step);
    test_step.dependOn(&run_compression_tests.step);
    test_step.dependOn(&run_deadline_tests.step);

    const test_exact_step = b.step("test-exact", "Run live_exact gates only");
    test_exact_step.dependOn(&run_live_exact_tests.step);

    // Fuzz targets (selectable)
    inline for (.{ "frame", "hpack", "session" }) |name| {
        const fuzz_mod = b.createModule(.{
            .root_source_file = b.path("fuzz/" ++ name ++ ".zig"),
            .target = target,
            .optimize = optimize,
            // Zig 0.16.0's bundled fuzz runner passes builtin.StackTrace to
            // std.debug.StackTrace when error tracing is enabled.
            .error_tracing = false,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
            },
        });
        const fuzz_tests = b.addTest(.{
            .root_module = fuzz_mod,
            .name = "fuzz-" ++ name,
        });
        const run_fuzz = b.addRunArtifact(fuzz_tests);
        const fuzz_step = b.step("fuzz-" ++ name, "Run " ++ name ++ " fuzz target");
        fuzz_step.dependOn(&run_fuzz.step);
        test_step.dependOn(&run_fuzz.step);
    }

    const examples = [_]struct { name: []const u8, path: []const u8 }{
        .{ .name = "example-hello", .path = "examples/hello.zig" },
        .{ .name = "example-datastar-sse", .path = "examples/datastar_sse.zig" },
        .{ .name = "starh2-conformance-server", .path = "examples/conformance_server.zig" },
        .{ .name = "starh2-bench-server", .path = "examples/bench_server.zig" },
    };
    // The TLS gate drives this binary, so keep a handle to it.
    var conformance_exe: ?*std.Build.Step.Compile = null;
    // The bench harness drives this one, and only at ReleaseFast.
    var bench_server_exe: ?*std.Build.Step.Compile = null;
    inline for (examples) |ex| {
        const exe_mod = b.createModule(.{
            .root_source_file = b.path(ex.path),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        });
        exe_mod.addImport("starh2_datastar", datastar_mod);
        // Only the examples use the SDK itself, which is why it stays lazy.
        if (datastar_dep) |dep| exe_mod.addImport("datastar", dep.module("datastar"));
        const exe = b.addExecutable(.{
            .name = ex.name,
            .root_module = exe_mod,
        });
        b.installArtifact(exe);
        const build_step = b.step(ex.name, "Build " ++ ex.name);
        build_step.dependOn(&b.addInstallArtifact(exe, .{}).step);
        if (comptime std.mem.eql(u8, ex.name, "starh2-conformance-server")) {
            conformance_exe = exe;
        }
        if (comptime std.mem.eql(u8, ex.name, "starh2-bench-server")) {
            bench_server_exe = exe;
        }
    }

    // Cross release targets — compile-only gates (see tools/README.md for Linux RUN).
    const release_queries = [_]struct { name: []const u8, query: std.Target.Query }{
        .{ .name = "x86_64-linux-musl", .query = .{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .musl } },
        .{ .name = "aarch64-linux-musl", .query = .{ .cpu_arch = .aarch64, .os_tag = .linux, .abi = .musl } },
        .{ .name = "aarch64-linux-gnu", .query = .{ .cpu_arch = .aarch64, .os_tag = .linux, .abi = .gnu } },
    };
    const release_step = b.step("release", "ReleaseSafe build of shipped binaries for every deploy target");
    inline for (release_queries) |rq| {
        const rt = b.resolveTargetQuery(rq.query);
        const zio_rt = b.dependency("zio", .{ .target = rt, .optimize = .ReleaseSafe });
        const datastar_rt = b.lazyDependency("datastar", .{ .target = rt, .optimize = .ReleaseSafe });
        const boring_rt = boringModule(b, rt, .ReleaseSafe, boringssl_source_path);
        const brotli_rt = b.dependency("brotli", .{ .target = rt, .optimize = .ReleaseSafe });
        const starh2_rt = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = rt,
            .optimize = .ReleaseSafe,
            .link_libc = true,
            .imports = &.{
                .{ .name = "boring", .module = boring_rt },
                .{ .name = "zio", .module = zio_rt.module("zio") },
            },
        });
        attachStarh2Options(b, starh2_rt, false);
        const brotli_enc_rt = brotliEncLib(b, brotli_rt, rt, .ReleaseSafe, b.fmt("brotli_enc_{s}", .{rq.name}));
        linkBrotliEnc(starh2_rt, brotli_rt, brotli_enc_rt);
        const datastar_mod_rt = b.createModule(.{
            .root_source_file = b.path("src/datastar.zig"),
            .target = rt,
            .optimize = .ReleaseSafe,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_rt },
            },
        });
        inline for (examples) |ex| {
            const exe_mod = b.createModule(.{
                .root_source_file = b.path(ex.path),
                .target = rt,
                .optimize = .ReleaseSafe,
                .imports = &.{
                    .{ .name = "starh2", .module = starh2_rt },
                    .{ .name = "zio", .module = zio_rt.module("zio") },
                },
            });
            exe_mod.addImport("starh2_datastar", datastar_mod_rt);
            if (datastar_rt) |dep| exe_mod.addImport("datastar", dep.module("datastar"));
            const exe = b.addExecutable(.{
                .name = ex.name ++ "-" ++ rq.name,
                .root_module = exe_mod,
            });
            const install = b.addInstallArtifact(exe, .{});
            release_step.dependOn(&install.step);
        }
    }

    // Nested fuzz smokes so `./zb build ci` always drives a bounded continuous
    // fuzz without requiring the caller to remember --fuzz=.
    const fuzz_smoke_step = b.step("fuzz-smoke", "Bounded continuous fuzz for frame/hpack/session");
    inline for (.{ "frame", "hpack", "session" }) |name| {
        // Zig 0.16 Debug --fuzz writes a coverage header with pcs_len=0 on
        // this runner; ReleaseSafe actually instruments. The 1K cap is the
        // bound; the optimize flag is the one that makes the gate runnable.
        const cmd = b.addSystemCommand(&.{ b.graph.zig_exe, "build", "fuzz-" ++ name, "--fuzz=1K", "-Doptimize=ReleaseSafe" });
        cmd.setCwd(b.path("."));
        cmd.has_side_effects = true;
        fuzz_smoke_step.dependOn(&cmd.step);
    }

    // The TLS-edge gate. `zig build test` cannot reach the TLS paths at all —
    // no test binds a tls endpoint — so without this step a TLS regression
    // ships green. It drives the conformance server with real curl connections
    // because the oracle must share no code with the stack under test; see the
    // header of tools/tls_smoke.zig for the full reasoning.
    const tls_smoke_exe = b.addExecutable(.{
        .name = "tls-smoke",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/tls_smoke.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const tls_smoke_run = b.addRunArtifact(tls_smoke_exe);
    tls_smoke_run.addArg("--bin");
    tls_smoke_run.addFileArg(conformance_exe.?.getEmittedBin());
    // Relative to the repo root, so testdata/cert.pem resolves.
    tls_smoke_run.setCwd(b.path("."));
    tls_smoke_run.has_side_effects = true;
    tls_smoke_run.stdio = .inherit;
    const tls_smoke_step = b.step("tls-smoke", "TLS-edge gate: fresh curl connections against the conformance server");
    tls_smoke_step.dependOn(&tls_smoke_run.step);

    const h1_smoke_exe = b.addExecutable(.{
        .name = "h1-smoke",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/h1_smoke.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const h1_smoke_run = b.addRunArtifact(h1_smoke_exe);
    h1_smoke_run.addArg("--bin");
    h1_smoke_run.addFileArg(conformance_exe.?.getEmittedBin());
    h1_smoke_run.setCwd(b.path("."));
    h1_smoke_run.has_side_effects = true;
    h1_smoke_run.stdio = .inherit;
    const h1_smoke_step = b.step("h1-smoke", "HTTP/1.1 edge gate: curl against tls ALPN fallback and h1c");
    h1_smoke_step.dependOn(&h1_smoke_run.step);

    // The README gate. `zig build test` compiles the examples, never the
    // README, so a snippet can name an API no consumer can reach and stay
    // green for months — which is exactly what happened to the Datastar
    // section between 23dd119 and 4be7eb9. This writes a package that declares
    // starh2 as a path dependency, wires it with the addImport lines copied
    // out of README.md, and compiles every marked block against it.
    const readme_doctest_exe = b.addExecutable(.{
        .name = "readme-doctest",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/readme_doctest.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const readme_doctest_run = b.addRunArtifact(readme_doctest_exe);
    readme_doctest_run.addArgs(&.{ "--readme", "README.md" });
    readme_doctest_run.addArgs(&.{ "--out", ".zig-cache/readme-doctest" });
    // Relative path from the generated package back to this repo, for its path
    // dependency. It follows --out, so both live on this one line.
    readme_doctest_run.addArgs(&.{ "--repo-rel", "../.." });
    readme_doctest_run.addArgs(&.{ "--zig", b.graph.zig_exe });
    readme_doctest_run.setCwd(b.path("."));
    readme_doctest_run.has_side_effects = true;
    readme_doctest_run.stdio = .inherit;
    const readme_doctest_step = b.step("readme-doctest", "Compile every marked zig block in README.md as an external consumer");
    readme_doctest_step.dependOn(&readme_doctest_run.step);

    // One-shot throughput, against another implementation when one is given.
    // Deliberately NOT in `ci`: a throughput number on a shared machine is
    // noise, and a gate that fails on noise gets ignored. Run it on purpose.
    const bench_exe = b.addExecutable(.{
        .name = "bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/bench.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const bench_run = b.addRunArtifact(bench_exe);
    bench_run.addArg("--server");
    bench_run.addFileArg(bench_server_exe.?.getEmittedBin());
    if (b.args) |extra| bench_run.addArgs(extra);
    bench_run.setCwd(b.path("."));
    bench_run.has_side_effects = true;
    bench_run.stdio = .inherit;
    const bench_step = b.step("bench", "One-shot h2 throughput: starh2 tls vs h2c, plus --opponent <binary>");
    bench_step.dependOn(&bench_run.step);

    // Local pipeline costs without sockets, contention, or a client.
    // Deliberately ReleaseFast-only and not a CI gate.
    const pipeline_bench_exe = b.addExecutable(.{
        .name = "pipeline-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/pipeline_bench.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    b.installArtifact(pipeline_bench_exe);
    const pipeline_bench_run = b.addRunArtifact(pipeline_bench_exe);
    if (b.args) |extra| pipeline_bench_run.addArgs(extra);
    pipeline_bench_run.setCwd(b.path("."));
    pipeline_bench_run.has_side_effects = true;
    pipeline_bench_run.stdio = .inherit;
    const pipeline_bench_step = b.step("bench-pipeline", "Isolated HPACK, frame parsing, and task lifecycle costs");
    pipeline_bench_step.dependOn(&pipeline_bench_run.step);

    const ci_step = b.step("ci", "Full suite + test-exact + fuzz smoke + TLS gate + README gate + every release target");
    ci_step.dependOn(test_step);
    ci_step.dependOn(test_exact_step);
    ci_step.dependOn(fuzz_smoke_step);
    ci_step.dependOn(&tls_smoke_run.step);
    ci_step.dependOn(&h1_smoke_run.step);
    ci_step.dependOn(&readme_doctest_run.step);
    ci_step.dependOn(release_step);
}
