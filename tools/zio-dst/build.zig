const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    // The mutant is a zio compile-time option, declared by this harness's
    // root module (`zio_options` in src/main.zig), so it is a harness build
    // option here, not a `-D` passed through to zio's own build.
    const sim_mutant = b.option([]const u8, "sim-mutant", "DST mutant compiled into zio: none | omit_timeout_recheck | arm_timer_stale") orelse "none";

    const harness_options = b.addOptions();
    harness_options.addOption([]const u8, "sim_mutant", sim_mutant);

    const zio = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "zio-dst",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zio", .module = zio.module("zio") },
                .{ .name = "harness_options", .module = harness_options.createModule() },
            },
        }),
    });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.addPassthruArgs();
    // Every check reruns: a cached pass of a DST run is not a run.
    run.has_side_effects = true;
    const run_step = b.step("run", "Run the DST harness");
    run_step.dependOn(&run.step);
}
