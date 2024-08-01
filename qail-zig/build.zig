const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // pg.zig dependency
    const pg_zig = b.dependency("pg_zig", .{
        .target = target,
        .optimize = optimize,
    });

    // Main executable (encoding benchmark)
    const exe = b.addExecutable(.{
        .name = "qail-zig-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.addLibraryPath(.{ .cwd_relative = "../target/release" });
    exe.linkSystemLibrary("qail_php");
    exe.linkSystemLibrary("c");
    exe.linkSystemLibrary("resolv");
    exe.linkSystemLibrary("c++");
    b.installArtifact(exe);

    // I/O benchmark executable
    const bench_io = b.addExecutable(.{
        .name = "qail-zig-bench-io",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_io.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    bench_io.addLibraryPath(.{ .cwd_relative = "../target/release" });
    bench_io.linkSystemLibrary("qail_php");
    bench_io.linkSystemLibrary("c");
    bench_io.linkSystemLibrary("resolv");
    bench_io.linkSystemLibrary("c++");
    b.installArtifact(bench_io);

    // pg.zig comparison benchmark
    const bench_pgzig = b.addExecutable(.{
        .name = "qail-zig-vs-pgzig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_vs_pgzig.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "pg_zig", .module = pg_zig.module("pg") },
            },
        }),
    });
    bench_pgzig.addLibraryPath(.{ .cwd_relative = "../target/release" });
    bench_pgzig.linkSystemLibrary("qail_php");
    bench_pgzig.linkSystemLibrary("c");
    bench_pgzig.linkSystemLibrary("resolv");
    bench_pgzig.linkSystemLibrary("c++");
    b.installArtifact(bench_pgzig);

    // Run commands
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    const run_step = b.step("run", "Run encoding benchmark");
    run_step.dependOn(&run_cmd.step);

    const run_io = b.addRunArtifact(bench_io);
    run_io.step.dependOn(b.getInstallStep());
    const run_io_step = b.step("bench-io", "Run I/O benchmark");
    run_io_step.dependOn(&run_io.step);

    const run_pgzig = b.addRunArtifact(bench_pgzig);
    run_pgzig.step.dependOn(b.getInstallStep());
    const run_pgzig_step = b.step("bench-pgzig", "Run pg.zig comparison");
    run_pgzig_step.dependOn(&run_pgzig.step);

    // 10M benchmark
    const bench_10m = b.addExecutable(.{
        .name = "qail-zig-10m",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_10m.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "pg_zig", .module = pg_zig.module("pg") },
            },
        }),
    });
    bench_10m.addLibraryPath(.{ .cwd_relative = "../target/release" });
    bench_10m.linkSystemLibrary("qail_php");
    bench_10m.linkSystemLibrary("c");
    bench_10m.linkSystemLibrary("resolv");
    bench_10m.linkSystemLibrary("c++");
    b.installArtifact(bench_10m);

    const run_10m = b.addRunArtifact(bench_10m);
    run_10m.step.dependOn(b.getInstallStep());
    const run_10m_step = b.step("bench-10m", "Run 10M query benchmark");
    run_10m_step.dependOn(&run_10m.step);
}
