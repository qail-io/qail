const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Ensure lib directory exists
    std.fs.cwd().makeDir("lib") catch {};

    // Check if library exists, if not build it from Rust
    const lib_path = "lib/libqail_encoder.a";
    const lib_exists = blk: {
        std.fs.cwd().access(lib_path, .{}) catch {
            break :blk false;
        };
        break :blk true;
    };

    if (!lib_exists) {
        std.debug.print("\n", .{});
        std.debug.print("╔══════════════════════════════════════════════════════════════╗\n", .{});
        std.debug.print("║  📦 Building QAIL Encoder (Rust library)...                  ║\n", .{});
        std.debug.print("╚══════════════════════════════════════════════════════════════╝\n", .{});
        std.debug.print("\n", .{});

        // Build from local Rust source (monorepo)
        const build_result = std.process.Child.run(.{
            .allocator = b.allocator,
            .argv = &.{ "cargo", "build", "--release", "-p", "qail-encoder" },
            .cwd = "..", // Parent directory (qail.rs root)
        }) catch |err| {
            std.debug.print("❌ Failed to build Rust library: {any}\n", .{err});
            std.debug.print("\nMake sure you have Rust installed: https://rustup.rs\n", .{});
            @panic("Rust build failed");
        };
        _ = build_result;

        // Copy the built library
        const copy_result = std.process.Child.run(.{
            .allocator = b.allocator,
            .argv = &.{ "cp", "../target/release/libqail_encoder.a", lib_path },
        }) catch |err| {
            std.debug.print("❌ Failed to copy library: {any}\n", .{err});
            @panic("Library copy failed");
        };
        _ = copy_result;

        std.debug.print("✅ Built successfully!\n\n", .{});
    }

    // Main benchmark executable
    const exe = b.addExecutable(.{
        .name = "qail-zig-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.addLibraryPath(.{ .cwd_relative = "lib" });
    exe.linkSystemLibrary("qail_encoder");
    exe.linkSystemLibrary("c");
    if (target.result.os.tag != .windows) {
        exe.linkSystemLibrary("resolv");
    }
    exe.linkSystemLibrary("c++");
    b.installArtifact(exe);

    // I/O benchmark
    const bench_io = b.addExecutable(.{
        .name = "qail-zig-bench-io",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_io.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    bench_io.addLibraryPath(.{ .cwd_relative = "lib" });
    bench_io.linkSystemLibrary("qail_encoder");
    bench_io.linkSystemLibrary("c");
    if (target.result.os.tag != .windows) {
        bench_io.linkSystemLibrary("resolv");
    }
    bench_io.linkSystemLibrary("c++");
    b.installArtifact(bench_io);

    // Run steps
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    const run_step = b.step("run", "Run encoding benchmark");
    run_step.dependOn(&run_cmd.step);

    const run_io = b.addRunArtifact(bench_io);
    run_io.step.dependOn(b.getInstallStep());
    const run_io_step = b.step("bench-io", "Run I/O benchmark");
    run_io_step.dependOn(&run_io.step);
}
