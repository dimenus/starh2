const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const sim_mutant = b.option([]const u8, "sim-mutant", "DST mutant forwarded to zio: none | omit_timeout_recheck | arm_timer_stale") orelse "none";

    const zio = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
        .sim = true,
        .@"sim-mutant" = sim_mutant,
        .@"task-migration" = false,
    });

    const exe = b.addExecutable(.{
        .name = "zio-dst",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zio", .module = zio.module("zio") }},
        }),
    });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    if (b.args) |args| run.addArgs(args);
    const run_step = b.step("run", "Run the DST harness");
    run_step.dependOn(&run.step);
}
