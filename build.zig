const std = @import("std");
const builtin = @import("builtin");
const macos_sdk = @import("tools/macos_sdk.zig");

comptime {
    const min = "0.17.0";
    const ver = builtin.zig_version;
    if (ver.major != 0 or ver.minor != 17) {
        @compileError("starh2 requires Zig " ++ min ++ ", found a different minor");
    }
}

/// zio's scheduling discipline. The tags equal zio's `Scheduling`
/// (`src/options.zig`); zio does not let build.zig import that type.
///
/// zio fixes this at compile time, so `-Dzio-scheduling` is the A/B lever: an
/// A/B builds two binaries. The value goes explicitly to EVERY
/// `b.dependency("zio", ...)`, because the build option, not a root
/// `zio_options` declaration, also reaches test binaries, whose root module
/// is the test runner. starh2 code reads the same value as
/// `build_options.zio_scheduling`, so it can choose a spawn placement at
/// comptime. A consumer's own zio dependency must pass the same value, or the
/// build holds two zio modules and fails to compile.
const ZioScheduling = enum { single_executor, pinned, work_stealing };

/// Validity mutants of the DST gates: the connection harness (`dst-conn`)
/// and the HTTP/1.1 TLS transport witness (`dst-transport`). Every module but
/// those gates' mutant arms is built with `.none`, and each mutant site in
/// `src/` is `if (comptime ... == .<mutant>)`, so no other build contains a
/// mutant. Each tag is read in the file that carries it.
const DstMutant = enum {
    none,
    /// `h1.zig` `H1Tls.init`: the TLS pump gets no `ack_apply` sink, so its
    /// failure completions go to a channel nothing reads (the first step-7
    /// routing). Only `dst-transport`'s mutant arm.
    no_h1_ack_sink,
    /// `ticket_table.zig` `wait`: the terminal cause is checked before the
    /// completed-ok result (the pre-t-537 order).
    ticket_cause_first,
    /// `wire_pump.zig` `SocketWriter.drain`: a short write is reported as if
    /// the socket took every byte, so the unsent tail is lost.
    short_write_drop,
};

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

/// `src/http/brotli.zig` imports the brotli headers as `brotli_c`. Zig 0.17
/// removed `@cImport`, so the headers go through a translate-c step here, for
/// the module's own target and mode. Both headers are translated even where
/// only the encoder links: an unreferenced decoder extern costs nothing.
fn linkBrotliEnc(mod: *std.Build.Module, brotli: *std.Build.Dependency, lib: *std.Build.Step.Compile) void {
    const b = mod.owner;
    const header = b.addWriteFiles().add("brotli_c.h",
        \\#include <brotli/encode.h>
        \\#include <brotli/decode.h>
        \\
    );
    const tc = b.addTranslateC(.{
        .root_source_file = header,
        .target = mod.resolved_target.?,
        .optimize = mod.optimize.?,
    });
    tc.addIncludePath(brotli.path("c/include"));
    mod.addImport("brotli_c", tc.createModule());
    mod.addIncludePath(brotli.path("c/include"));
    mod.link_libc = true;
    mod.linkLibrary(lib);
}

fn linkBrotliDec(mod: *std.Build.Module, brotli: *std.Build.Dependency, lib: *std.Build.Step.Compile) void {
    mod.addIncludePath(brotli.path("c/include"));
    mod.link_libc = true;
    mod.linkLibrary(lib);
}

/// The commit this build is from, for the transport witness's seeds and
/// build identity, or "" when there is none to read: not a git checkout
/// (a `git archive` export carries it in `tests/dst_commit.txt` instead),
/// or a tree nested inside some other repository, whose HEAD is not ours.
/// "-dirty" is appended when tracked files differ from HEAD.
fn gitCommit(b: *std.Build) []const u8 {
    const root = b.root.toString(b.graph.arena) catch @panic("OOM");
    var code: u8 = 0;
    const cdup = b.runAllowFail(&.{ "git", "-C", root, "rev-parse", "--show-cdup" }, &code, .ignore) catch return "";
    if (std.mem.trim(u8, cdup, " \r\n\t").len != 0) return "";
    const head = b.runAllowFail(&.{ "git", "-C", root, "rev-parse", "HEAD" }, &code, .ignore) catch return "";
    const sha = std.mem.trim(u8, head, " \r\n\t");
    _ = b.runAllowFail(&.{ "git", "-C", root, "diff", "--quiet", "HEAD", "--" }, &code, .ignore) catch return b.fmt("{s}-dirty", .{sha});
    return sha;
}

fn attachStarh2Options(b: *std.Build, mod: *std.Build.Module, observe: bool, wire_ring: bool, zio_scheduling: ZioScheduling, dst_mutant: DstMutant) void {
    const opts = b.addOptions();
    opts.addOption(bool, "observe", observe);
    opts.addOption(bool, "wire_ring", wire_ring);
    opts.addOption(ZioScheduling, "zio_scheduling", zio_scheduling);
    opts.addOption(DstMutant, "dst_mutant", dst_mutant);
    mod.addOptions("build_options", opts);
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const observe = b.option(
        bool,
        "observe",
        "Keep test-only hot counters in non-Debug artifacts (A/B the counter tax)",
    ) orelse false;
    const wire_ring = b.option(
        bool,
        "wire-ring",
        "Per-connection TLS frame ring, printed when a peer drops a connection (t-3514)",
    ) orelse false;
    // Default work_stealing: measured faster and without the bimodal oneshot
    // bands of pinned placement on nachos (27ff454, t-853).
    const zio_scheduling = b.option(
        ZioScheduling,
        "zio-scheduling",
        "zio scheduling discipline: work_stealing (default), pinned or single_executor",
    ) orelse .work_stealing;
    const macos_sdk_override = b.option(
        []const u8,
        "macos-sdk",
        "Force a macOS SDK root and skip the SDK scan",
    );
    const macos_sdk_extra_root = b.option(
        []const u8,
        "macos-sdk-extra-root",
        "Additional directory of MacOSX*.sdk entries to scan",
    );
    // Gate builds are Debug; ReleaseFast benches compile the counters out unless
    // `-Dobserve=true`. Do not read `builtin.mode` inside connection.zig — an
    // imported module's mode is not the test artifact's mode.
    const observe_hot = observe or (optimize == .debug);

    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
        .scheduling = zio_scheduling,
    });
    // Lazy: the core `starh2` module does not use the Datastar SDK, so a
    // consumer of an HTTP/2 server must not be made to fetch and compile a
    // hypermedia SDK. Only `starh2_datastar` and the examples ask for it.
    const datastar_dep = b.lazyDependency("datastar", .{
        .target = target,
        .optimize = optimize,
    });
    const tls_mod = b.dependency("tls", .{ .target = target, .optimize = optimize }).module("tls");
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
            .{ .name = "tls", .module = tls_mod },
            // src/edge/tls.zig is a zio.CompletionQueue driver; the core
            // module carries the zio dependency openly (t-878).
            .{ .name = "zio", .module = zio_dep.module("zio") },
        },
    });
    attachStarh2Options(b, starh2_mod, observe_hot, wire_ring, zio_scheduling, .none);
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
            .{ .name = "tls", .module = tls_mod },
            .{ .name = "zio", .module = zio_dep.module("zio") },
        },
    });
    attachStarh2Options(b, lib_test_mod, true, false, zio_scheduling, .none);
    linkBrotliEnc(lib_test_mod, brotli_dep, brotli_enc);
    linkBrotliDec(lib_test_mod, brotli_dep, brotli_dec);
    const lib_tests = b.addTest(.{
        .root_module = lib_test_mod,
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);
    const unit_step = b.step("test-unit", "Run the library's own unit tests (the tests in src/, through src/root.zig)");
    unit_step.dependOn(&run_lib_tests.step);

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

    const teardown_traps = b.addExecutable(.{
        .name = "teardown-traps",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/teardown_traps.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_double_finalize_trap = b.addRunArtifact(teardown_traps);
    run_double_finalize_trap.addArg("double-finalize");
    run_double_finalize_trap.addCheck(.{ .expect_stderr_match = "handler slot finalized 2 times" });
    run_double_finalize_trap.has_side_effects = true;
    const run_sweep_lock_trap = b.addRunArtifact(teardown_traps);
    run_sweep_lock_trap.addArg("sweep-lock");
    run_sweep_lock_trap.addCheck(.{ .expect_stderr_match = "session lock acquired during shutdownHandlers" });
    run_sweep_lock_trap.has_side_effects = true;
    const run_sweep_lock_session_trap = b.addRunArtifact(teardown_traps);
    run_sweep_lock_session_trap.addArg("sweep-lock-session");
    run_sweep_lock_session_trap.addCheck(.{ .expect_stderr_match = "session lock acquired during shutdownHandlers" });
    run_sweep_lock_session_trap.has_side_effects = true;
    const run_unlocked_sweep_trap = b.addRunArtifact(teardown_traps);
    run_unlocked_sweep_trap.addArg("unlocked-sweep");
    run_unlocked_sweep_trap.addCheck(.{ .expect_stderr_match = "session lock not held at wakeHandlerWaiters" });
    run_unlocked_sweep_trap.has_side_effects = true;
    const run_conservation_trap = b.addRunArtifact(teardown_traps);
    run_conservation_trap.addArg("conservation");
    run_conservation_trap.addCheck(.{ .expect_stderr_match = "slot conservation: claimed=1 finalized=0 rolled_back=0" });
    run_conservation_trap.has_side_effects = true;
    const run_deinit_live_trap = b.addRunArtifact(teardown_traps);
    run_deinit_live_trap.addArg("deinit-live");
    run_deinit_live_trap.addCheck(.{ .expect_stderr_match = "deinit with live_handlers=1" });
    run_deinit_live_trap.has_side_effects = true;
    const run_error_path_freeze_trap = b.addRunArtifact(teardown_traps);
    run_error_path_freeze_trap.addArg("error-path-freeze");
    run_error_path_freeze_trap.addCheck(.{ .expect_stderr_match = "error-path freeze ok" });
    run_error_path_freeze_trap.expectExitCode(0);
    run_error_path_freeze_trap.has_side_effects = true;
    const run_r158_parked_trap = b.addRunArtifact(teardown_traps);
    run_r158_parked_trap.addArg("r158-parked");
    run_r158_parked_trap.addCheck(.{ .expect_stderr_match = "r158 parked enroll ok" });
    run_r158_parked_trap.addCheck(.{ .expect_stderr_match = "reaper-stuck absent" });
    run_r158_parked_trap.expectExitCode(0);
    run_r158_parked_trap.has_side_effects = true;
    lifecycle_step.dependOn(&run_double_finalize_trap.step);
    lifecycle_step.dependOn(&run_sweep_lock_trap.step);
    lifecycle_step.dependOn(&run_sweep_lock_session_trap.step);
    lifecycle_step.dependOn(&run_unlocked_sweep_trap.step);
    lifecycle_step.dependOn(&run_conservation_trap.step);
    lifecycle_step.dependOn(&run_deinit_live_trap.step);
    lifecycle_step.dependOn(&run_error_path_freeze_trap.step);
    lifecycle_step.dependOn(&run_r158_parked_trap.step);

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

    const placement_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/placement.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_placement_tests = b.addRunArtifact(placement_tests);
    const balance_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/balance.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_balance_tests = b.addRunArtifact(balance_tests);
    const drain_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/drain.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_drain_tests = b.addRunArtifact(drain_tests);
    const tls_backpressure_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/tls_backpressure.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_tls_backpressure_tests = b.addRunArtifact(tls_backpressure_tests);
    const placement_step = b.step("test-placement", "Run spawn placement and connection balancer gates (meaningful under -Dzio-scheduling=pinned)");
    placement_step.dependOn(&run_placement_tests.step);
    placement_step.dependOn(&run_balance_tests.step);
    placement_step.dependOn(&run_drain_tests.step);
    placement_step.dependOn(&run_tls_backpressure_tests.step);

    const handshake_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/handshake.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_handshake_tests = b.addRunArtifact(handshake_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_handshake_tests.setCwd(b.path("."));
    const handshake_step = b.step("test-handshake", "Run the TLS handshake timeout gate");
    handshake_step.dependOn(&run_handshake_tests.step);

    const tls_preface_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/tls_preface.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_tls_preface_tests = b.addRunArtifact(tls_preface_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_tls_preface_tests.setCwd(b.path("."));
    const tls_preface_step = b.step("test-tls-preface", "Run the TLS server-preface-first gates");
    tls_preface_step.dependOn(&run_tls_preface_tests.step);

    const goaway_delivery_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/goaway_delivery.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_goaway_delivery_tests = b.addRunArtifact(goaway_delivery_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_goaway_delivery_tests.setCwd(b.path("."));
    const goaway_delivery_step = b.step("test-goaway-delivery", "Run the GOAWAY delivery gates (connection error and graceful shutdown)");
    goaway_delivery_step.dependOn(&run_goaway_delivery_tests.step);

    const ack_channel_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/ack_channel.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_ack_channel_tests = b.addRunArtifact(ack_channel_tests);
    const ack_channel_step = b.step("test-ack-channel", "Run the h2c write-ack channel bound gate (t-3561)");
    ack_channel_step.dependOn(&run_ack_channel_tests.step);

    const slot_reuse_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/slot_reuse.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_slot_reuse_tests = b.addRunArtifact(slot_reuse_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_slot_reuse_tests.setCwd(b.path("."));
    const slot_reuse_step = b.step("test-slot-reuse", "Run the connection slot gates (reuse, ordering, boot allocator calls, dirty slots, shutdown)");
    slot_reuse_step.dependOn(&run_slot_reuse_tests.step);

    const outbound_memory_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/outbound_memory.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_outbound_memory_tests = b.addRunArtifact(outbound_memory_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_outbound_memory_tests.setCwd(b.path("."));
    const outbound_memory_step = b.step("test-outbound-memory", "Run the outbound memory gates (write-pool backpressure, header blocks cut into CONTINUATION frames)");
    outbound_memory_step.dependOn(&run_outbound_memory_tests.step);

    const outbound_budget_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/outbound_budget.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_outbound_budget_tests = b.addRunArtifact(outbound_budget_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_outbound_budget_tests.setCwd(b.path("."));
    const outbound_budget_step = b.step("test-outbound-budget", "Run the connection outbound budget gates (a full budget parks a writer in arrival order; the server cap refuses; an inline job waits)");
    outbound_budget_step.dependOn(&run_outbound_budget_tests.step);

    // t-3567: TLS bare flush barriers against a held writer. Its own step so
    // it can be run alone (also in a release-fast observe build); `test` runs it.
    const t3567_tests = b.addTest(.{
        .name = "t3567",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/t3567_tls_barriers.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_t3567_tests = b.addRunArtifact(t3567_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_t3567_tests.setCwd(b.path("."));
    const t3567_step = b.step("test-t3567", "Run the t-3567 gate: bare flush barriers against a held writer wait for room on h2c and TLS");
    t3567_step.dependOn(&run_t3567_tests.step);

    // The HTTP/1.1 TLS transport witness (DST stage 1,
    // `tests/dst_tls_lifecycle.zig`). Two builds of the same source: the
    // control, and the mutant whose shared constructor leaves the ack sink
    // out (`h1.mutant_no_ack_sink`). Each has its own starh2 module, observe
    // on in every optimize mode, because the witness drives the pump through
    // `Pump.test_io`. `tools/dst/transport.sh` requires the control to pass
    // its floors and the mutant to fail with `UnconsumedTlsAck`, the same
    // case and trace twice more from its printed replay command. Debug
    // always, dependencies included, whatever `-Doptimize` says: the slab
    // pool's double-release assert and the pump's own asserts are part of
    // the witness's oracle, and a ReleaseFast build drops them (the witness
    // refuses to compile without runtime safety).
    const dst_optimize: std.builtin.OptimizeMode = .debug;
    const dst_zio = b.dependency("zio", .{
        .target = target,
        .optimize = dst_optimize,
        .scheduling = zio_scheduling,
    }).module("zio");
    const dst_tls = b.dependency("tls", .{ .target = target, .optimize = dst_optimize }).module("tls");
    const dst_brotli_enc = brotliEncLib(b, brotli_dep, target, dst_optimize, "brotli_enc_dst");
    const dst_opts = b.addOptions();
    dst_opts.addOption([]const u8, "git_commit", gitCommit(b));
    var dst_bins: [2]*std.Build.Step.Compile = undefined;
    for ([_]DstMutant{ .none, .no_h1_ack_sink }, 0..) |mutant, i| {
        const dst_starh2 = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = dst_optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "tls", .module = dst_tls },
                .{ .name = "zio", .module = dst_zio },
            },
        });
        attachStarh2Options(b, dst_starh2, true, false, zio_scheduling, mutant);
        linkBrotliEnc(dst_starh2, brotli_dep, dst_brotli_enc);
        dst_bins[i] = b.addExecutable(.{
            .name = if (mutant == .no_h1_ack_sink) "dst-transport-mutant" else "dst-transport-control",
            .root_module = b.createModule(.{
                .root_source_file = b.path("tests/dst_tls_lifecycle.zig"),
                .target = target,
                .optimize = dst_optimize,
                .imports = &.{
                    .{ .name = "starh2", .module = dst_starh2 },
                    .{ .name = "zio", .module = dst_zio },
                    .{ .name = "dst_options", .module = dst_opts.createModule() },
                },
            }),
        });
    }
    const dst_transport_run = b.addSystemCommand(&.{ "sh", "tools/dst/transport.sh" });
    dst_transport_run.addArtifactArg(dst_bins[0]);
    dst_transport_run.addArtifactArg(dst_bins[1]);
    dst_transport_run.setCwd(b.path("."));
    dst_transport_run.has_side_effects = true;
    dst_transport_run.stdio = .inherit;
    const dst_transport_step = b.step("dst-transport", "HTTP/1.1 TLS transport witness: the control passes every enumerated and seeded case; the mutant without h1AckSink fails UnconsumedTlsAck and replays identically twice");
    dst_transport_step.dependOn(&dst_transport_run.step);

    // t-3794: the h2c WritePump settles a failed batch by the bytes the
    // socket accepted. Its own step so it can be run alone; `test` runs it.
    const h2c_settle_tests = b.addTest(.{
        .name = "h2c_settle",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/h2c_settle.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_h2c_settle_tests = b.addRunArtifact(h2c_settle_tests);
    const h2c_settle_step = b.step("test-h2c-settle", "Run the t-3794 gate: a failed h2c batch acks ok the chunks the socket accepted in full");
    h2c_settle_step.dependOn(&run_h2c_settle_tests.step);

    // t-3807: a record that fails to decrypt while the TLS pump holds a
    // chunk, on h2 and HTTP/1.1. Its own step so it can be run alone; `test`
    // runs it.
    const tls_decrypt_error_tests = b.addTest(.{
        .name = "tls_decrypt_error",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/tls_decrypt_error.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_tls_decrypt_error_tests = b.addRunArtifact(tls_decrypt_error_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_tls_decrypt_error_tests.setCwd(b.path("."));
    const tls_decrypt_error_step = b.step("test-tls-decrypt-error", "Run the t-3807 gate: a record that fails to decrypt while the TLS pump holds a chunk, on h2 and HTTP/1.1");
    tls_decrypt_error_step.dependOn(&run_tls_decrypt_error_tests.step);

    const t3562_tests = b.addTest(.{
        .name = "t3562",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/t3562_terminal_bound.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_t3562_tests = b.addRunArtifact(t3562_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_t3562_tests.setCwd(b.path("."));
    const t3562_step = b.step("test-t3562", "Run the t-3562 gate: terminal frames have one derived bound, and a flood past it ends with GOAWAY(ENHANCE_YOUR_CALM)");
    t3562_step.dependOn(&run_t3562_tests.step);

    // Not in `test` yet: it measures whether a Unix stream socket holds a
    // server stall (the transport seam's step 1), on each system.
    const unix_stall_tests = b.addTest(.{
        .name = "unix_stall",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/unix_stall_probe.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_unix_stall_tests = b.addRunArtifact(unix_stall_tests);
    run_unix_stall_tests.setCwd(b.path("."));
    const unix_stall_step = b.step("test-unix-stall", "Measure whether a Unix stream socket holds a server stall (h2c and TLS, quiet and with client writes)");
    unix_stall_step.dependOn(&run_unix_stall_tests.step);

    const request_memory_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/request_memory.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_request_memory_tests = b.addRunArtifact(request_memory_tests);
    const request_memory_step = b.step("test-request-memory", "Run the request memory gates (caps and their statuses, a header block decoded without memory, blocks coming home)");
    request_memory_step.dependOn(&run_request_memory_tests.step);

    const admission_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/admission.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_admission_tests = b.addRunArtifact(admission_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_admission_tests.setCwd(b.path("."));
    const admission_step = b.step("test-admission", "Run the accept-loop admission gates (wait, do not refuse)");
    admission_step.dependOn(&run_admission_tests.step);

    const hardening_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/hardening.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_hardening_tests = b.addRunArtifact(hardening_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_hardening_tests.setCwd(b.path("."));
    const hardening_step = b.step("test-hardening", "Run the t-3759 attack gates (missing deadlines, accept errors, the descriptor boot check)");
    hardening_step.dependOn(&run_hardening_tests.step);

    const accounts_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/accounts.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_accounts_tests = b.addRunArtifact(accounts_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_accounts_tests.setCwd(b.path("."));
    const accounts_step = b.step("test-accounts", "Run the t-3759 step 3 gates (client accounts, client identity, PROXY v2, conservation soak)");
    accounts_step.dependOn(&run_accounts_tests.step);

    const shares_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/shares.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_shares_tests = b.addRunArtifact(shares_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_shares_tests.setCwd(b.path("."));
    const shares_step = b.step("test-shares", "Run the t-3759 step 4 gates (one attack per share-table row, the aggregate, the NAT case)");
    shares_step.dependOn(&run_shares_tests.step);

    const protocol_work_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/protocol_work.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_protocol_work_tests = b.addRunArtifact(protocol_work_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_protocol_work_tests.setCwd(b.path("."));
    const protocol_work_step = b.step("test-protocol-work", "Run the t-3759 step 6 gates (bodyless DATA floods, the 1-byte flood, a browser-like load, padding, parked leases released on RST, the parked wait limit)");
    protocol_work_step.dependOn(&run_protocol_work_tests.step);

    const routes_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/routes.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_routes_tests = b.addRunArtifact(routes_tests);
    // Reads testdata/cert.pem and key.pem relative to the repo root.
    run_routes_tests.setCwd(b.path("."));
    const routes_step = b.step("test-routes", "Run the t-3759 step 5 gates (route shares under pressure: the broken route, the hot route at idle, bytes, compression, boot)");
    routes_step.dependOn(&run_routes_tests.step);

    const shutdown_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/shutdown.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_shutdown_tests = b.addRunArtifact(shutdown_tests);
    const shutdown_step = b.step("test-shutdown", "Run the shutdown contract gates (serve waits for handlers and reports them; deinit and accounting checks)");
    shutdown_step.dependOn(&run_shutdown_tests.step);
    // The cases a test process cannot survive run in a child process of
    // their own, checked on what they print.
    const shutdown_child = b.addExecutable(.{
        .name = "shutdown-child",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/shutdown_child.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "starh2_h2_client", .module = h2_client_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
                .{ .name = "tls", .module = tls_mod },
            },
        }),
    });
    const run_shutdown_stuck = b.addRunArtifact(shutdown_child);
    run_shutdown_stuck.addArg("stuck");
    run_shutdown_stuck.addCheck(.{ .expect_stderr_match = "shutdown report 1: after" });
    run_shutdown_stuck.addCheck(.{ .expect_stderr_match = "every connection has asked its handlers to cancel, 1 task handlers in all; still running: 1 (" });
    run_shutdown_stuck.addCheck(.{ .expect_stderr_match = "per task route: GET /stuck 1" });
    run_shutdown_stuck.addCheck(.{ .expect_stderr_match = "shutdown report 2: 1 task handlers" });
    run_shutdown_stuck.addCheck(.{ .expect_stderr_match = "shutdown report 4: still waiting" });
    run_shutdown_stuck.addCheck(.{ .expect_stderr_match = "child: serve still waiting for the stuck handler" });
    run_shutdown_stuck.expectExitCode(0);
    run_shutdown_stuck.has_side_effects = true;
    const run_shutdown_deinit = b.addRunArtifact(shutdown_child);
    run_shutdown_deinit.addArg("deinit-live");
    run_shutdown_deinit.addCheck(.{ .expect_stderr_match = "Server.deinit: task handlers (task_handlers) is 1, expected 0" });
    run_shutdown_deinit.addCheck(.{ .expect_stderr_match = "freeing nothing, leaking the server" });
    run_shutdown_deinit.addCheck(.{ .expect_stderr_match = "child: deinit returned with 1 task handlers live" });
    run_shutdown_deinit.addCheck(.{ .expect_stderr_match = "child: request memory still live after deinit" });
    run_shutdown_deinit.expectExitCode(0);
    run_shutdown_deinit.has_side_effects = true;
    const run_shutdown_deinit_idle = b.addRunArtifact(shutdown_child);
    run_shutdown_deinit_idle.addArg("deinit-idle");
    run_shutdown_deinit_idle.addCheck(.{ .expect_stderr_match = "Server.deinit: serve has not returned" });
    run_shutdown_deinit_idle.addCheck(.{ .expect_stderr_match = "freeing nothing, leaking the server" });
    run_shutdown_deinit_idle.addCheck(.{ .expect_stderr_match = "child: served a request after deinit" });
    run_shutdown_deinit_idle.expectExitCode(0);
    run_shutdown_deinit_idle.has_side_effects = true;
    const run_shutdown_underflow = b.addRunArtifact(shutdown_child);
    run_shutdown_underflow.addArg("underflow");
    run_shutdown_underflow.addCheck(.{ .expect_stderr_match = "accounting underflow: a release of 1 from active_streams found 0" });
    run_shutdown_underflow.addCheck(.{ .expect_stderr_match = "accounting underflow: a release of 7 from outbound_bytes found 0" });
    run_shutdown_underflow.addCheck(.{ .expect_stderr_match = "child: active_streams=0 outbound_bytes=0 underflows=2" });
    run_shutdown_underflow.expectExitCode(0);
    run_shutdown_underflow.has_side_effects = true;
    shutdown_step.dependOn(&run_shutdown_stuck.step);
    shutdown_step.dependOn(&run_shutdown_deinit.step);
    shutdown_step.dependOn(&run_shutdown_deinit_idle.step);
    shutdown_step.dependOn(&run_shutdown_underflow.step);
    const run_accounts_underflow = b.addRunArtifact(shutdown_child);
    run_accounts_underflow.addArg("accounts-underflow");
    run_accounts_underflow.addCheck(.{ .expect_stderr_match = "client account underflow: a release of 1 from streams found 0; refused, count kept (1 refused so far)" });
    run_accounts_underflow.addCheck(.{ .expect_stderr_match = "client account: a reference to record 0 generation 0 found generation 1; skipped (1 so far)" });
    run_accounts_underflow.addCheck(.{ .expect_stderr_match = "client account: a reference to record 0 generation 0 found generation 1; skipped (3 so far)" });
    run_accounts_underflow.addCheck(.{ .expect_stderr_match = "child: accounts underflows=1 stale=3 removals=1 full_after_two=true b.streams=1 b.holders=1" });
    run_accounts_underflow.expectExitCode(0);
    run_accounts_underflow.has_side_effects = true;
    shutdown_step.dependOn(&run_accounts_underflow.step);
    accounts_step.dependOn(&run_accounts_underflow.step);
    const run_accounts_race = b.addRunArtifact(shutdown_child);
    run_accounts_race.addArg("accounts-release-race");
    run_accounts_race.addCheck(.{ .expect_stderr_match = "child: release race same_record=true after_locked=1 b.live=true b.holders=2 stale=3 underflows=0 removals=1" });
    run_accounts_race.expectExitCode(0);
    run_accounts_race.has_side_effects = true;
    shutdown_step.dependOn(&run_accounts_race.step);
    accounts_step.dependOn(&run_accounts_race.step);
    const run_accounts_quarantine = b.addRunArtifact(shutdown_child);
    run_accounts_quarantine.addArg("accounts-quarantine");
    run_accounts_quarantine.addCheck(.{ .expect_stderr_match = "with connections = 1, streams = 1; record quarantined" });
    run_accounts_quarantine.addCheck(.{ .expect_stderr_match = "child: quarantine live=0 quarantined=1 removals=1" });
    run_accounts_quarantine.expectExitCode(0);
    run_accounts_quarantine.has_side_effects = true;
    shutdown_step.dependOn(&run_accounts_quarantine.step);
    accounts_step.dependOn(&run_accounts_quarantine.step);
    const run_proxy_trusted_range = b.addRunArtifact(shutdown_child);
    run_proxy_trusted_range.addArg("proxy-trusted-range");
    run_proxy_trusted_range.addCheck(.{ .expect_stderr_match = "endpoint 0: the trusted PROXY source range ::/0 holds every IPv4-mapped address (::ffff:0:0/96) without being one of them" });
    run_proxy_trusted_range.addCheck(.{ .expect_stderr_match = "list the IPv4 range (for example 192.0.2.0/24) or the mapped range (::ffff:192.0.2.0/120) explicitly" });
    run_proxy_trusted_range.addCheck(.{ .expect_stderr_match = "child: trusted range ::/0 ProxyTrustedRangeHoldsIpv4" });
    run_proxy_trusted_range.expectExitCode(0);
    run_proxy_trusted_range.has_side_effects = true;
    shutdown_step.dependOn(&run_proxy_trusted_range.step);
    accounts_step.dependOn(&run_proxy_trusted_range.step);
    const run_accounts_charge_race = b.addRunArtifact(shutdown_child);
    run_accounts_charge_race.addArg("accounts-charge-race");
    run_accounts_charge_race.addCheck(.{ .expect_stderr_match = "child: charge race same_record=true b.streams=0 b.request_bytes=10 stale=2 underflows=0" });
    run_accounts_charge_race.expectExitCode(0);
    run_accounts_charge_race.has_side_effects = true;
    shutdown_step.dependOn(&run_accounts_charge_race.step);
    accounts_step.dependOn(&run_accounts_charge_race.step);
    const run_accounts_generation_wrap = b.addRunArtifact(shutdown_child);
    run_accounts_generation_wrap.addArg("accounts-generation-wrap");
    run_accounts_generation_wrap.addCheck(.{ .expect_stderr_match = "client table: record 0 retired: its generation would wrap" });
    run_accounts_generation_wrap.addCheck(.{ .expect_stderr_match = "child: generation wrap cycles=2 b.acquired=false b.live=false stale=1 insert_failures=2" });
    run_accounts_generation_wrap.expectExitCode(0);
    run_accounts_generation_wrap.has_side_effects = true;
    shutdown_step.dependOn(&run_accounts_generation_wrap.step);
    accounts_step.dependOn(&run_accounts_generation_wrap.step);
    const run_route_share_boot = b.addRunArtifact(shutdown_child);
    run_route_share_boot.addArg("route-share-boot");
    run_route_share_boot.addCheck(.{ .expect_stderr_match = "route GET /1: its task_handlers share 456 is below a client's share 512: one client may hold more of it than this route" });
    run_route_share_boot.addCheck(.{ .expect_stderr_match = "route GET /9: its compression share 15 is below a client's share 16" });
    run_route_share_boot.addCheck(.{ .expect_stderr_match = "route POST /upload: its request_bytes share 268435457 is above the server's limit 268435456" });
    run_route_share_boot.addCheck(.{ .expect_stderr_match = "route GET /off: its compression share is 0: the route would be refused everything under pressure" });
    run_route_share_boot.addCheck(.{ .expect_stderr_match = "child: route share boot nine=456 over=RouteShareAboveServerLimit zero=RouteShareZero" });
    run_route_share_boot.expectExitCode(0);
    run_route_share_boot.has_side_effects = true;
    shutdown_step.dependOn(&run_route_share_boot.step);
    routes_step.dependOn(&run_route_share_boot.step);
    const run_route_underflow = b.addRunArtifact(shutdown_child);
    run_route_underflow.addArg("route-underflow");
    run_route_underflow.addCheck(.{ .expect_stderr_match = "route account underflow: a release of 1 from task_handlers of GET /r found 0; refused, count kept (1 refused so far)" });
    run_route_underflow.addCheck(.{ .expect_stderr_match = "route account underflow: a release of 2 from compression of GET /r found 0; refused, count kept (2 refused so far)" });
    run_route_underflow.addCheck(.{ .expect_stderr_match = "route accounts: 2 refused releases over the server's life" });
    run_route_underflow.addCheck(.{ .expect_stderr_match = "child: route underflow task_handlers=0 compression=0 underflows=2 clean=false" });
    run_route_underflow.expectExitCode(0);
    run_route_underflow.has_side_effects = true;
    shutdown_step.dependOn(&run_route_underflow.step);
    routes_step.dependOn(&run_route_underflow.step);
    const run_route_table_mismatch = b.addRunArtifact(shutdown_child);
    run_route_table_mismatch.addArg("route-table-mismatch");
    run_route_table_mismatch.addCheck(.{ .expect_stderr_match = "child: route table built from its list: true, from the other: false" });
    run_route_table_mismatch.addCheck(.{ .expect_stderr_match = "route accounts: a route of a list of 2 routes, but the table was built from another list of 2" });
    run_route_table_mismatch.addCheck(.{ .expect_term = .{ .signal = .ABRT } });
    run_route_table_mismatch.has_side_effects = true;
    shutdown_step.dependOn(&run_route_table_mismatch.step);
    routes_step.dependOn(&run_route_table_mismatch.step);
    const run_route_table_not_task = b.addRunArtifact(shutdown_child);
    run_route_table_not_task.addArg("route-table-not-task");
    run_route_table_not_task.addCheck(.{ .expect_stderr_match = "child: route 0 is record 0" });
    run_route_table_not_task.addCheck(.{ .expect_stderr_match = "route accounts: route 1 of 2 is not a task route" });
    run_route_table_not_task.addCheck(.{ .expect_term = .{ .signal = .ABRT } });
    run_route_table_not_task.has_side_effects = true;
    shutdown_step.dependOn(&run_route_table_not_task.step);
    routes_step.dependOn(&run_route_table_not_task.step);

    const macos_sdk_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/macos_sdk.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_macos_sdk_tests = b.addRunArtifact(macos_sdk_tests);
    const macos_sdk_step = b.step("test-macos-sdk", "Run macOS SDK selection tests");
    macos_sdk_step.dependOn(&run_macos_sdk_tests.step);

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
    test_step.dependOn(lifecycle_step);
    test_step.dependOn(&run_backend_parity_tests.step);
    test_step.dependOn(&run_live_exact_tests.step);
    test_step.dependOn(&run_writer_tests.step);
    test_step.dependOn(&run_scheduler_tests.step);
    test_step.dependOn(&run_compression_tests.step);
    test_step.dependOn(&run_deadline_tests.step);
    test_step.dependOn(&run_placement_tests.step);
    test_step.dependOn(&run_handshake_tests.step);
    test_step.dependOn(&run_tls_preface_tests.step);
    test_step.dependOn(&run_goaway_delivery_tests.step);
    test_step.dependOn(&run_ack_channel_tests.step);
    test_step.dependOn(&run_admission_tests.step);
    test_step.dependOn(&run_hardening_tests.step);
    test_step.dependOn(&run_accounts_tests.step);
    test_step.dependOn(&run_shares_tests.step);
    test_step.dependOn(&run_routes_tests.step);
    test_step.dependOn(&run_protocol_work_tests.step);
    test_step.dependOn(shutdown_step);
    test_step.dependOn(&run_slot_reuse_tests.step);
    test_step.dependOn(&run_request_memory_tests.step);
    test_step.dependOn(&run_outbound_memory_tests.step);
    test_step.dependOn(&run_outbound_budget_tests.step);
    test_step.dependOn(&run_t3567_tests.step);
    test_step.dependOn(&run_h2c_settle_tests.step);
    test_step.dependOn(&run_tls_decrypt_error_tests.step);
    test_step.dependOn(&run_t3562_tests.step);
    test_step.dependOn(&run_macos_sdk_tests.step);

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
        const zio_rt = b.dependency("zio", .{ .target = rt, .optimize = .safe, .scheduling = zio_scheduling });
        const datastar_rt = b.lazyDependency("datastar", .{ .target = rt, .optimize = .safe });
        const tls_rt = b.dependency("tls", .{ .target = rt, .optimize = .safe }).module("tls");
        const brotli_rt = b.dependency("brotli", .{ .target = rt, .optimize = .safe });
        const starh2_rt = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = rt,
            .optimize = .safe,
            .link_libc = true,
            .imports = &.{
                .{ .name = "tls", .module = tls_rt },
                .{ .name = "zio", .module = zio_rt.module("zio") },
            },
        });
        attachStarh2Options(b, starh2_rt, false, false, zio_scheduling, .none);
        const brotli_enc_rt = brotliEncLib(b, brotli_rt, rt, .safe, b.fmt("brotli_enc_{s}", .{rq.name}));
        linkBrotliEnc(starh2_rt, brotli_rt, brotli_enc_rt);
        const datastar_mod_rt = b.createModule(.{
            .root_source_file = b.path("src/datastar.zig"),
            .target = rt,
            .optimize = .safe,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_rt },
            },
        });
        inline for (examples) |ex| {
            const exe_mod = b.createModule(.{
                .root_source_file = b.path(ex.path),
                .target = rt,
                .optimize = .safe,
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
        const cmd = b.addSystemCommand(&.{ b.graph.zig_exe, "build", "fuzz-" ++ name, "--fuzz=1K", "-Doptimize=safe" });
        // Forwarded so a `ci` run under one scheduling does not fuzz a
        // binary built with another.
        cmd.addArg(b.fmt("-Dzio-scheduling={s}", .{@tagName(zio_scheduling)}));
        cmd.setCwd(b.path("."));
        cmd.has_side_effects = true;
        if (macos_sdk_override) |path| {
            cmd.addArg(b.fmt("-Dmacos-sdk={s}", .{path}));
        }
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
            .imports = &.{.{ .name = "zio", .module = zio_dep.module("zio") }},
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
            .imports = &.{.{ .name = "zio", .module = zio_dep.module("zio") }},
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

    const h1_go_smoke_exe = b.addExecutable(.{
        .name = "h1-go-smoke",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/h1_go_smoke.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zio", .module = zio_dep.module("zio") }},
        }),
    });
    const h1_go_smoke_run = b.addRunArtifact(h1_go_smoke_exe);
    h1_go_smoke_run.addArg("--bin");
    h1_go_smoke_run.addFileArg(conformance_exe.?.getEmittedBin());
    h1_go_smoke_run.addArg("--go");
    h1_go_smoke_run.addFileArg(b.path("tools/h1_go_client.go"));
    h1_go_smoke_run.setCwd(b.path("."));
    h1_go_smoke_run.has_side_effects = true;
    h1_go_smoke_run.stdio = .inherit;
    const h1_go_smoke_step = b.step("h1-go-smoke", "HTTP/1.1 Go net/http oracle: keep-alive and 100-continue");
    h1_go_smoke_step.dependOn(&h1_go_smoke_run.step);

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
    bench_run.addPassthruArgs();
    bench_run.setCwd(b.path("."));
    bench_run.has_side_effects = true;
    bench_run.stdio = .inherit;
    const bench_step = b.step("bench", "One-shot h2 throughput: starh2 tls vs h2c, plus --opponent <binary>");
    bench_step.dependOn(&bench_run.step);

    // Local pipeline costs without sockets, contention, or a client.
    // Its numbers are ReleaseFast-only and not a CI gate.
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
    pipeline_bench_run.addPassthruArgs();
    pipeline_bench_run.setCwd(b.path("."));
    pipeline_bench_run.has_side_effects = true;
    pipeline_bench_run.stdio = .inherit;
    const pipeline_bench_step = b.step("bench-pipeline", "Isolated HPACK, frame parsing, and task lifecycle costs");
    pipeline_bench_step.dependOn(&pipeline_bench_run.step);
    // `ci` compiles the bench, but a bench that compiles can still fail at
    // setup. This run checks only that it exits 0; it reads no timing.
    const pipeline_bench_smoke = b.addRunArtifact(pipeline_bench_exe);
    pipeline_bench_smoke.addArgs(&.{ "-n", "1000", "--rounds", "3" });
    pipeline_bench_smoke.setCwd(b.path("."));
    pipeline_bench_smoke.has_side_effects = true;
    pipeline_bench_smoke.expectExitCode(0);
    // The report goes to stderr, and the build runner shows a checked run's
    // stderr under "failed command" even when the run passed.
    _ = pipeline_bench_smoke.captureStdErr(.{});
    const pipeline_bench_smoke_step = b.step("bench-pipeline-smoke", "Run pipeline-bench once at a tiny size and fail if it does not exit 0");
    pipeline_bench_smoke_step.dependOn(&pipeline_bench_smoke.step);

    // The std.Io gate. zio is the runtime; `std.Io` is the vtable it
    // implements, so `io: std.Io` is correct and is NOT what this bans. The
    // ban is on the std.Io CONCURRENCY abstractions, because each one differs
    // from the zio primitive underneath it in a way that has already cost a
    // defect: `Select.cancelDiscard` discarded bytes already off the socket
    // (t-1760); `Future.cancel` is request PLUS mandatory await, so a worker
    // blocks forever on a handler in an uncancelable wait, where zio's
    // `AnyTask.cancel` is setCanceled+wake and returns at once (t-1802); and
    // `Event.set` on an already-set event neither wakes nor re-synchronizes a
    // waiter that already returned, which makes a following plain field read
    // a data race (t-1802). Reach for zio, or ask before adding one.
    // tools/std-io-gate/gate.zig has the rules, and what a token scan cannot
    // see. It is built for the host, so a cross `-Dtarget` still runs it.
    const std_io_gate_exe = b.addExecutable(.{
        .name = "std-io-gate",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/std-io-gate/gate.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
        }),
    });
    const std_io_gate_run = b.addRunArtifact(std_io_gate_exe);
    // The build's own directories are not code, whatever `--cache-dir` and
    // `--prefix` named them, and the cache is written during the scan.
    std_io_gate_run.addArg("--skip");
    std_io_gate_run.addDirectoryArg(.cache_root);
    std_io_gate_run.addArg("--skip");
    std_io_gate_run.addDirectoryArg(.{ .relative = .{ .base = .global_cache } });
    std_io_gate_run.addArg("--skip");
    std_io_gate_run.addDirectoryArg(.{ .relative = .{ .base = .install_prefix } });
    std_io_gate_run.addArg(".");
    std_io_gate_run.setCwd(b.path("."));
    std_io_gate_run.has_side_effects = true;
    std_io_gate_run.stdio = .inherit;
    // The scanner's own tests: every hole it closes, planted, must be found.
    const std_io_gate_tests = b.addTest(.{
        .name = "std-io-gate-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/std-io-gate/gate.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
        }),
    });
    const std_io_gate_tests_run = b.addRunArtifact(std_io_gate_tests);
    const std_io_gate_step = b.step("std-io-gate", "Fail on any std.Io concurrency use anywhere in the repo; zio owns concurrency here");
    std_io_gate_step.dependOn(&std_io_gate_tests_run.step);
    std_io_gate_step.dependOn(&std_io_gate_run.step);

    // The session_mu gate. A handler's release of `session_mu` is what wakes
    // the actor (`unlockSession` rings), so the mutex and the doorbell may be
    // touched only inside their wrappers. t-3594 has the reasoning.
    const session_mu_gate_run = b.addSystemCommand(&.{ "bash", "tools/session-mu-gate/gate.sh" });
    session_mu_gate_run.addArg(".");
    session_mu_gate_run.setCwd(b.path("."));
    session_mu_gate_run.has_side_effects = true;
    session_mu_gate_run.stdio = .inherit;
    const session_mu_gate_step = b.step("session-mu-gate", "Fail on any use of the session mutex outside lockSession/unlockSession, or a send on the actor bell outside ring");
    session_mu_gate_step.dependOn(&session_mu_gate_run.step);

    // The zio DST gate. tools/zio-dst is its own package: it builds the sim
    // line of zio (`zio_options.sim` declared in its root) and runs D1-D5
    // over the fixtures, then one build per validity mutant, each of which
    // must be FOUND by some seed and replay identically (a clean sweep
    // exits 1). It builds the sim zio three times (sim on, and once per
    // mutant), separately from the suite's own build of the same zio
    // commit. In `ci`.
    const dst_zio_step = b.step("dst-zio", "zio DST harness: D1-D5 over every fixture, and every validity mutant found (tools/zio-dst)");
    {
        const checks = b.addSystemCommand(&.{ b.graph.zig_exe, "build", "run", "--", "--d1", "--d2", "--d4", "--d5", "--all-fixtures" });
        checks.setCwd(b.path("tools/zio-dst"));
        checks.has_side_effects = true;
        checks.stdio = .inherit;
        dst_zio_step.dependOn(&checks.step);
        inline for (.{ "arm_timer_stale", "omit_timeout_recheck" }) |mutant| {
            const run = b.addSystemCommand(&.{ b.graph.zig_exe, "build", "run", "-Dsim-mutant=" ++ mutant, "--", "--arm-mutant" });
            run.setCwd(b.path("tools/zio-dst"));
            run.has_side_effects = true;
            run.stdio = .inherit;
            dst_zio_step.dependOn(&run.step);
        }
    }

    // The DST connection gate (tools/dst-conn). One real h2c `Connection`
    // and an in-sim HTTP/2 peer on a sim pipe, every interleaving chosen by
    // one seed. zio is a separate instance here: sim on, pinned scheduling
    // (the simulator does not model task migration). starh2 is built again
    // against it with the test hooks on (`observe`: the coverage marks), once
    // per validity mutant. Run arguments, not build options, carry the commit
    // seed, so a new commit does not rebuild the harness.
    const dst_conn_step = b.step("dst-conn", "DST: one h2c connection in the zio simulator; seeds to a budget, determinism, and every validity mutant caught (tools/dst-conn)");
    const dst_conn_install = b.step("dst-conn-install", "Install the dst-conn harness binaries (zig-out/bin/dst-conn*) to rerun seeds directly");
    {
        const zio_sim_dep = b.dependency("zio", .{
            .target = target,
            .optimize = optimize,
            .scheduling = .pinned,
            .sim = true,
        });
        const oracle_mod = b.createModule(.{
            .root_source_file = b.path("tests/h2_oracle.zig"),
            .target = target,
            .optimize = optimize,
        });
        var exit_code: u8 = 0;
        // No git (a `git archive` tree): the commit seed is skipped, and the
        // harness says so; the regression file and the fixed range still run.
        const commit: []const u8 = if (b.runAllowFail(&.{ "git", "rev-parse", "HEAD" }, &exit_code, .ignore)) |out|
            std.mem.trim(u8, out, " \n\r")
        else |_|
            "none";
        inline for (.{ DstMutant.none, DstMutant.ticket_cause_first, DstMutant.short_write_drop }) |mutant| {
            const starh2_dst = b.createModule(.{
                .root_source_file = b.path("src/root.zig"),
                .target = target,
                .optimize = optimize,
                .link_libc = true,
                .imports = &.{
                    .{ .name = "tls", .module = tls_mod },
                    .{ .name = "zio", .module = zio_sim_dep.module("zio") },
                },
            });
            attachStarh2Options(b, starh2_dst, true, false, .pinned, mutant);
            linkBrotliEnc(starh2_dst, brotli_dep, brotli_enc);
            const h2_client_dst = b.createModule(.{
                .root_source_file = b.path("src/h2_client.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{.{ .name = "starh2", .module = starh2_dst }},
            });
            const harness_opts = b.addOptions();
            harness_opts.addOption([]const u8, "mutant", @tagName(mutant));
            const name = if (mutant == .none) "dst-conn" else "dst-conn-" ++ @tagName(mutant);
            const exe = b.addExecutable(.{
                .name = name,
                .root_module = b.createModule(.{
                    .root_source_file = b.path("tools/dst-conn/main.zig"),
                    .target = target,
                    .optimize = optimize,
                    .imports = &.{
                        .{ .name = "zio", .module = zio_sim_dep.module("zio") },
                        .{ .name = "starh2", .module = starh2_dst },
                        .{ .name = "h2_client", .module = h2_client_dst },
                        .{ .name = "h2_oracle", .module = oracle_mod },
                        .{ .name = "harness_options", .module = harness_opts.createModule() },
                    },
                }),
            });
            dst_conn_install.dependOn(&b.addInstallArtifact(exe, .{}).step);
            const run = b.addRunArtifact(exe);
            run.setCwd(b.path("."));
            run.has_side_effects = true;
            run.stdio = .inherit;
            if (mutant == .none) {
                run.addArgs(&.{ "--ci", "--seeds-file", "tools/dst-conn/regression-seeds.txt", "--commit", commit, "--budget-ms", "45000" });
            } else {
                run.addArgs(&.{"--expect-mutant"});
            }
            dst_conn_step.dependOn(&run.step);
        }
    }

    // The ThreadSanitizer lane. The simulator runs one OS thread and cannot
    // run work stealing, so it cannot find a race that needs real
    // parallelism; this runs the concurrency test binaries on real executor
    // threads with TSan instead. Each binary sets `sanitize_thread` on its
    // root module only: starh2, zio, tls and std take the value from the
    // first module that imports them (Zig 0.17 `Module.create`, inherited
    // orelse parent), so the shared modules above stay uninstrumented for
    // every other step. zio's fiber hooks (`coro/tsan.zig`) switch on with
    // `builtin.sanitize_thread`. tools/tsan/runner.zig has the run protocol.
    // Not in `ci`: it is slow, and its TSan builds are kept out of the
    // compile walk below.
    const tsan_step = b.step("tsan", "ThreadSanitizer lane: the concurrency test binaries and bug 2's churn test under TSan; fails on any report, or if the canary race is not reported (tools/tsan)");
    const tsan_runs = b.option(usize, "tsan-runs", "Runs of each binary in the TSan lane (default 1)") orelse 1;
    const tsan_runner = b.addExecutable(.{
        .name = "tsan-runner",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/tsan/runner.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zio", .module = zio_dep.module("zio") }},
        }),
    });
    {
        const run = b.addRunArtifact(tsan_runner);
        run.addArg("--suppressions");
        run.addFileArg(b.path("tools/tsan/suppressions.txt"));
        run.addArgs(&.{ "--log-dir", ".zig-cache/tsan-lane" });
        run.addArgs(&.{ "--timeout-s", "900" });
        run.addArg("--runs");
        run.addArg(b.fmt("{d}", .{tsan_runs}));
        // Several test binaries read testdata/cert.pem relative to the root.
        run.setCwd(b.path("."));
        run.has_side_effects = true;
        run.stdio = .inherit;
        const tsan_imports: []const std.Build.Module.Import = &.{
            .{ .name = "starh2", .module = starh2_mod },
            .{ .name = "starh2_h2_client", .module = h2_client_mod },
            .{ .name = "zio", .module = zio_dep.module("zio") },
            .{ .name = "tls", .module = tls_mod },
        };
        const canary = b.addTest(.{
            .name = "tsan_canary",
            .root_module = b.createModule(.{
                .root_source_file = b.path("tests/tsan_canary.zig"),
                .target = target,
                .optimize = optimize,
                .sanitize_thread = true,
                .imports = tsan_imports,
            }),
        });
        run.addArg("--canary");
        run.addArtifactArg(canary);
        const tsan_bins = [_][]const u8{
            "goaway_delivery",
            "shutdown",
            "lifecycle",
            "slot_reuse",
            "admission",
            "placement",
            "h1_battery",
            "bug2_churn",
        };
        for (tsan_bins) |name| {
            const t = b.addTest(.{
                .name = name,
                .root_module = b.createModule(.{
                    .root_source_file = b.path(b.fmt("tests/{s}.zig", .{name})),
                    .target = target,
                    .optimize = optimize,
                    .sanitize_thread = true,
                    .imports = tsan_imports,
                }),
            });
            run.addArgs(&.{ "--bin", name });
            run.addArtifactArg(t);
        }
        tsan_step.dependOn(&run.step);
    }

    const ci_step = b.step("ci", "Full suite + test-exact + fuzz smoke + TLS gate + README gate + std.Io gate + session mutex gate + DST transport witness + every release target + zio DST gate (dst-zio) + connection DST gate (dst-conn) + every artifact a named step or the install step compiles");
    ci_step.dependOn(std_io_gate_step);
    ci_step.dependOn(&session_mu_gate_run.step);
    ci_step.dependOn(&dst_transport_run.step);
    ci_step.dependOn(test_step);
    // Placement and balancer gates; each test says what it checks under the
    // scheduling this `ci` was built with.
    ci_step.dependOn(placement_step);
    ci_step.dependOn(test_exact_step);
    ci_step.dependOn(fuzz_smoke_step);
    ci_step.dependOn(&tls_smoke_run.step);
    ci_step.dependOn(&h1_smoke_run.step);
    ci_step.dependOn(&h1_go_smoke_run.step);
    ci_step.dependOn(&readme_doctest_run.step);
    ci_step.dependOn(pipeline_bench_smoke_step);
    ci_step.dependOn(release_step);
    ci_step.dependOn(dst_zio_step);
    ci_step.dependOn(dst_conn_step);

    // Every artifact that `zig build <step>` or a plain `zig build` can
    // compile is compiled by `ci` too. A tool or example is otherwise only
    // built by its own step, so an API change that breaks it (round 1 made
    // `hpack.Decoder.init` fallible and broke `pipeline-bench`) passes `ci`
    // and fails the next full build. Walking the named steps and the install
    // step means a new artifact needs no line here to be covered.
    {
        var seen: std.AutoHashMap(*std.Build.Step, void) = .init(b.allocator);
        var named: std.ArrayList(*std.Build.Step.Compile) = .empty;
        collectCompiles(b.getInstallStep(), &seen, &named, b.allocator);
        // The TSan lane's binaries are not ci's: they are slow to build and
        // need a TSan runtime for the host. Its runner is a plain tool, so
        // ci still compiles that.
        for (b.top_level_steps.values()) |tls| {
            if (&tls.step == tsan_step) continue;
            collectCompiles(&tls.step, &seen, &named, b.allocator);
        }
        for (named.items) |compile| ci_step.dependOn(&compile.step);
        ci_step.dependOn(&tsan_runner.step);
    }

    // Per-artifact libc file for native macOS only. A global `--libc` would
    // also hit linux cross targets (`crt_dir may not be empty for linux`) and
    // would miss the nested `readme-doctest` build. Walk the graph so a new
    // Compile step inherits the fix.
    const macos_apply = applyMacosSdkLibc(b, target, macos_sdk_override, macos_sdk_extra_root);
    if (macos_apply.libc_file) |lp| {
        readme_doctest_run.addArg("--libc");
        readme_doctest_run.addFileArg(lp);
    }
    if (macos_apply.rejected_higher.len != 0) {
        const probe_step = b.step(
            "macos-libcxx-probe",
            "Fail if any rejected higher macOS SDK now builds libcxx",
        );
        for (macos_apply.rejected_higher) |rejected| {
            std.debug.print("macos-sdk: probe rejected {s}\n", .{rejected});
            const probe = b.addSystemCommand(&.{"sh"});
            probe.addFileArg(b.path("tools/macos-libcxx-probe.sh"));
            probe.addArg("--expect-no");
            probe.addArg(rejected);
            probe.setEnvironmentVariable("ZIG", b.graph.zig_exe);
            probe.setCwd(b.path("."));
            probe.has_side_effects = true;
            probe.stdio = .inherit;
            probe_step.dependOn(&probe.step);
        }
        ci_step.dependOn(probe_step);
    } else if (builtin.os.tag == .macos and !macos_apply.guard_installed) {
        std.debug.print("macos-sdk: no rejected SDK to probe\n", .{});
    }
}

const MacosLibcApply = struct {
    libc_file: ?std.Build.LazyPath = null,
    rejected_higher: []const []const u8 = &.{},
    guard_installed: bool = false,
};

const MacosSdkSelection = struct {
    chosen: ?macos_sdk.Candidate = null,
    rejected_higher: []const []const u8 = &.{},
    /// Fail-closed message for native macOS compiles. Null means no guard.
    /// This is a Step, not `std.process.fatal`: an environment condition must
    /// not kill `--help` or `session-mu-gate`.
    guard_msg: ?[]const u8 = null,
};

fn applyMacosSdkLibc(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    override: ?[]const u8,
    extra_root: ?[]const u8,
) MacosLibcApply {
    if (builtin.os.tag != .macos) return .{};
    // Every Compile step here uses `target` (the release cross-targets are
    // Linux), so a non-macOS `-Dtarget` has no Compile step for this fix to
    // reach. Returning here keeps the n == 0 fatals below meaning "the walk is
    // broken", instead of firing on every cross build from a Mac.
    if (target.result.os.tag != .macos) return .{};
    const sel = selectMacosSdk(b, override, extra_root);
    if (sel.guard_msg) |msg| {
        const fail = b.addFail(msg);
        fail.step.name = "macos-sdk-guard";
        const n = attachGuardToOwnedMacosCompiles(b, &fail.step);
        // Same as the libc-file n==0 check below: a build.zig programming
        // error. The two paths are deliberately symmetric. A broken walk on
        // the guard path must not fall through to the opaque libcxx wall.
        if (n == 0) {
            std.process.fatal(
                "{s}\nmacos-sdk: guard covered no Compile steps",
                .{msg},
            );
        }
        return .{ .guard_installed = true };
    }
    const chosen = sel.chosen orelse return .{ .rejected_higher = sel.rejected_higher };
    std.debug.print("macos-sdk: using {s}\n", .{chosen.resolved_path});
    const bytes = macos_sdk.formatLibcFile(b.allocator, chosen.resolved_path) catch @panic("OOM");
    const wf = b.addWriteFiles();
    const lp = wf.add("macos-sdk.libc", bytes);
    const n = applyLibcToOwnedMacosCompiles(b, lp);
    // Configure-time fatal: a build.zig programming error. We produced a libc
    // file and then found nobody to give it to. An environment condition
    // (unreadable SDK, no usable SDK) is a Step, not a fatal, so --help and
    // session-mu-gate still run.
    if (n == 0) {
        std.process.fatal(
            "macOS SDK workaround: libc file generated but 0 Compile steps received it",
            .{},
        );
    }
    return .{ .libc_file = lp, .rejected_higher = sel.rejected_higher };
}

fn selectMacosSdk(b: *std.Build, override: ?[]const u8, extra_root: ?[]const u8) MacosSdkSelection {
    // Scans installed SDKs at configure time; a cached configuration could
    // name an SDK that has since been removed or superseded.
    b.graph.poisonCache();
    const io = b.graph.io;
    const gpa = b.allocator;

    var override_chosen: ?macos_sdk.Candidate = null;
    if (override) |path| {
        const cand = macos_sdk.inspectSdk(gpa, io, path) catch |err| {
            std.process.fatal(
                "macOS SDK workaround: cannot read -Dmacos-sdk={s}: {s}",
                .{ path, @errorName(err) },
            );
        };
        if (cand.verdict != .usable) {
            const zig_sdk = std.zig.system.darwin.getSdk(gpa, io, &b.graph.host.result);
            const roots = collectMacosSdkRoots(b, zig_sdk, extra_root);
            const scanned = macos_sdk.scan(gpa, io, roots) catch |err| {
                std.process.fatal("macOS SDK workaround: scan failed: {s}", .{@errorName(err)});
            };
            const report = macos_sdk.formatScanReport(gpa, roots, scanned, cand) catch @panic("OOM");
            return .{ .guard_msg = report };
        }
        override_chosen = cand;
    }

    const zig_sdk = std.zig.system.darwin.getSdk(gpa, io, &b.graph.host.result);
    if (override_chosen == null) {
        if (zig_sdk) |p| {
            if (macos_sdk.inspectSdk(gpa, io, p)) |cand| {
                if (cand.verdict == .usable) return .{};
            } else |_| {}
        }
    }

    const roots = collectMacosSdkRoots(b, zig_sdk, extra_root);
    const scanned = macos_sdk.scan(gpa, io, roots) catch |err| {
        std.process.fatal("macOS SDK workaround: scan failed: {s}", .{@errorName(err)});
    };
    const chosen_opt = override_chosen orelse macos_sdk.choose(scanned);
    if (macos_sdk.firstChoiceBlocker(scanned, chosen_opt)) |block| {
        const msg = macos_sdk.formatChoiceBlocker(gpa, block) catch @panic("OOM");
        const report = macos_sdk.formatScanReport(gpa, roots, scanned, override_chosen) catch @panic("OOM");
        return .{ .guard_msg = b.fmt("{s}\n{s}", .{ msg, report }) };
    }
    if (chosen_opt) |c| {
        return .{
            .chosen = c,
            .rejected_higher = rejectedHigherPaths(gpa, scanned, c),
        };
    }
    const report = macos_sdk.formatScanReport(gpa, roots, scanned, null) catch @panic("OOM");
    return .{ .guard_msg = report };
}

fn rejectedHigherPaths(
    gpa: std.mem.Allocator,
    scanned: macos_sdk.ScanResult,
    chosen: macos_sdk.Candidate,
) []const []const u8 {
    var list: std.ArrayList([]const u8) = .empty;
    for (scanned.candidates) |c| {
        if (macos_sdk.isRejectedHigher(c, chosen)) {
            list.append(gpa, c.resolved_path) catch @panic("OOM");
        }
    }
    return list.toOwnedSlice(gpa) catch @panic("OOM");
}

fn collectMacosSdkRoots(b: *std.Build, zig_sdk: ?[]const u8, extra_root: ?[]const u8) []const []const u8 {
    var list: std.ArrayList([]const u8) = .empty;
    for (macos_sdk.default_search_roots) |root| {
        list.append(b.allocator, root) catch @panic("OOM");
    }
    if (zig_sdk) |p| {
        if (std.fs.path.dirname(p)) |parent| {
            var dup = false;
            for (list.items) |root| {
                if (std.mem.eql(u8, root, parent)) dup = true;
            }
            if (!dup) list.append(b.allocator, parent) catch @panic("OOM");
        }
    }
    if (extra_root) |root| {
        var dup = false;
        for (list.items) |existing| {
            if (std.mem.eql(u8, existing, root)) dup = true;
        }
        if (!dup) list.append(b.allocator, root) catch @panic("OOM");
    }
    return list.toOwnedSlice(b.allocator) catch @panic("OOM");
}

fn ownedMacosCompiles(b: *std.Build) []*std.Build.Step.Compile {
    var seen_steps: std.AutoHashMap(*std.Build.Step, void) = .init(b.allocator);
    var roots: std.ArrayList(*std.Build.Step.Compile) = .empty;
    for (b.top_level_steps.values()) |tls| {
        collectCompiles(&tls.step, &seen_steps, &roots, b.allocator);
    }
    var applied: std.AutoHashMap(*std.Build.Step.Compile, void) = .init(b.allocator);
    var out: std.ArrayList(*std.Build.Step.Compile) = .empty;
    for (roots.items) |root_compile| {
        for (root_compile.getCompileDependencies(true)) |compile| {
            if (applied.contains(compile)) continue;
            applied.put(compile, {}) catch @panic("OOM");
            if (compile.step.owner != b) continue;
            const resolved = compile.root_module.resolved_target orelse continue;
            if (resolved.result.os.tag != .macos) continue;
            out.append(b.allocator, compile) catch @panic("OOM");
        }
    }
    return out.toOwnedSlice(b.allocator) catch @panic("OOM");
}

fn applyLibcToOwnedMacosCompiles(b: *std.Build, libc_file: std.Build.LazyPath) usize {
    const compiles = ownedMacosCompiles(b);
    for (compiles) |compile| {
        compile.setLibCFile(libc_file);
    }
    return compiles.len;
}

fn attachGuardToOwnedMacosCompiles(b: *std.Build, guard: *std.Build.Step) usize {
    const compiles = ownedMacosCompiles(b);
    for (compiles) |compile| {
        compile.step.dependOn(guard);
    }
    return compiles.len;
}

fn collectCompiles(
    step: *std.Build.Step,
    seen: *std.AutoHashMap(*std.Build.Step, void),
    compiles: *std.ArrayList(*std.Build.Step.Compile),
    gpa: std.mem.Allocator,
) void {
    const gop = seen.getOrPut(step) catch @panic("OOM");
    if (gop.found_existing) return;
    if (step.cast(std.Build.Step.Compile)) |compile| {
        compiles.append(gpa, compile) catch @panic("OOM");
    }
    for (step.dependencies.items) |dep| {
        collectCompiles(dep, seen, compiles, gpa);
    }
}
