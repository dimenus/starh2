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
    return switch (target.result.os.tag) {
        .macos => &[_][]const u8{ "-std=c99", "-O2", "-DOS_MACOSX" },
        .linux => &[_][]const u8{ "-std=c99", "-O2", "-DOS_LINUX" },
        else => &[_][]const u8{ "-std=c99", "-O2" },
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

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

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
    const tls_dep = b.dependency("tls", .{
        .target = target,
        .optimize = optimize,
    });
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
            .{ .name = "tls", .module = tls_dep.module("tls") },
        },
    });
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
            .{ .name = "tls", .module = tls_dep.module("tls") },
        },
    });
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

    const test_step = b.step("test", "Run unit and integration tests");
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_protocol_tests.step);
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
    };
    // The TLS gate drives this binary, so keep a handle to it.
    var conformance_exe: ?*std.Build.Step.Compile = null;
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
        const tls_rt = b.dependency("tls", .{ .target = rt, .optimize = .ReleaseSafe });
        const brotli_rt = b.dependency("brotli", .{ .target = rt, .optimize = .ReleaseSafe });
        const starh2_rt = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = rt,
            .optimize = .ReleaseSafe,
            .link_libc = true,
            .imports = &.{
                .{ .name = "tls", .module = tls_rt.module("tls") },
            },
        });
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
        const cmd = b.addSystemCommand(&.{ b.graph.zig_exe, "build", "fuzz-" ++ name, "--fuzz=1K" });
        cmd.setCwd(b.path("."));
        cmd.has_side_effects = true;
        fuzz_smoke_step.dependOn(&cmd.step);
    }

    // The TLS-edge gate. `zig build test` cannot reach the TLS paths at all —
    // no test binds a tls_h2 endpoint — so without this step a TLS regression
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

    const ci_step = b.step("ci", "Full suite + test-exact + fuzz smoke + TLS gate + every release target");
    ci_step.dependOn(test_step);
    ci_step.dependOn(test_exact_step);
    ci_step.dependOn(fuzz_smoke_step);
    ci_step.dependOn(&tls_smoke_run.step);
    ci_step.dependOn(release_step);
}
