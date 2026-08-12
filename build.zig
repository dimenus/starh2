const std = @import("std");

comptime {
    const min = "0.16.0";
    const ver = @import("builtin").zig_version;
    if (ver.major != 0 or ver.minor != 16) {
        @compileError("starh2 requires Zig " ++ min ++ ", found a different minor");
    }
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });
    const datastar_dep = b.dependency("datastar", .{
        .target = target,
        .optimize = optimize,
    });
    const tls_dep = b.dependency("tls", .{
        .target = target,
        .optimize = optimize,
    });

    const starh2_mod = b.addModule("starh2", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zio", .module = zio_dep.module("zio") },
            .{ .name = "datastar", .module = datastar_dep.module("datastar") },
            .{ .name = "tls", .module = tls_dep.module("tls") },
        },
    });

    const lib_tests = b.addTest(.{
        .root_module = starh2_mod,
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

    const lifecycle_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/lifecycle.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
                .{ .name = "zio", .module = zio_dep.module("zio") },
            },
        }),
    });
    const run_lifecycle_tests = b.addRunArtifact(lifecycle_tests);

    const live_exact_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/live_exact.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "starh2", .module = starh2_mod },
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
    test_step.dependOn(&run_live_exact_tests.step);
    test_step.dependOn(&run_writer_tests.step);
    test_step.dependOn(&run_scheduler_tests.step);

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
        const exe = b.addExecutable(.{
            .name = ex.name,
            .root_module = exe_mod,
        });
        b.installArtifact(exe);
        const build_step = b.step(ex.name, "Build " ++ ex.name);
        build_step.dependOn(&b.addInstallArtifact(exe, .{}).step);
    }
}
