const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const zio = b.dependency("zio", .{ .target = target, .optimize = optimize });

    const exe = b.addExecutable(.{
        .name = "select-cancel-repro",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zio", .module = zio.module("zio") }},
        }),
    });

    b.installArtifact(exe);

    const clobber = b.addExecutable(.{
        .name = "select-clobber-repro",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clobber.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "zio", .module = zio.module("zio") }},
        }),
    });
    b.installArtifact(clobber);

    const run = b.addRunArtifact(exe);
    if (b.args) |args| run.addArgs(args);

    const run_step = b.step("run", "Run the select-cancel item-drop reproducer");
    run_step.dependOn(&run.step);
}
