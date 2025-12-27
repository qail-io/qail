//! QAIL-Zig Benchmark
//!
//! Measures FFI call overhead for encoding operations.

const std = @import("std");
const qail = @import("qail.zig");

const ITERATIONS: usize = 100_000;
const BATCH_SIZE: usize = 1_000;
const BATCHES: usize = 100;

pub fn main() void {
    std.debug.print("🏁 QAIL-ZIG BENCHMARK\n", .{});
    std.debug.print("=====================\n", .{});
    std.debug.print("Version: {s}\n\n", .{qail.version()});

    // Benchmark 1: Individual encoding (one FFI call = one query)
    std.debug.print("📊 Test 1: Individual Encoding\n", .{});
    std.debug.print("   {d} FFI calls, each encodes 1 query\n", .{ITERATIONS});

    const start1 = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < ITERATIONS) : (i += 1) {
        const limit: i64 = @intCast(@mod(i, 10) + 1);
        var query = qail.encodeSelect("harbors", "id,name", limit);
        query.deinit();
    }

    const end1 = std.time.nanoTimestamp();
    const elapsed_ns: u64 = @intCast(end1 - start1);
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const ops_per_sec = @as(f64, @floatFromInt(ITERATIONS)) / (elapsed_ms / 1000.0);
    const ns_per_op = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(ITERATIONS));

    std.debug.print("   Time: {d:.2}ms\n", .{elapsed_ms});
    std.debug.print("   FFI calls/sec: {d:.0}\n", .{ops_per_sec});
    std.debug.print("   Latency: {d:.2} ns/call\n\n", .{ns_per_op});

    // Benchmark 2: Batch encoding (one FFI call = N queries encoded together)
    std.debug.print("📊 Test 2: Batch Encoding\n", .{});
    std.debug.print("   {d} FFI calls, each encodes {d} queries\n", .{ BATCHES, BATCH_SIZE });

    // Build limits array
    var limits: [BATCH_SIZE]i64 = undefined;
    for (&limits, 0..) |*l, j| {
        l.* = @intCast(@mod(j, 10) + 1);
    }

    const start2 = std.time.nanoTimestamp();

    var batch: usize = 0;
    while (batch < BATCHES) : (batch += 1) {
        var query = qail.encodeBatch("harbors", "id,name", &limits);
        query.deinit();
    }

    const end2 = std.time.nanoTimestamp();
    const batch_elapsed_ns: u64 = @intCast(end2 - start2);
    const batch_elapsed_ms = @as(f64, @floatFromInt(batch_elapsed_ns)) / 1_000_000.0;
    const batch_calls_per_sec = @as(f64, @floatFromInt(BATCHES)) / (batch_elapsed_ms / 1000.0);
    const ns_per_batch = @as(f64, @floatFromInt(batch_elapsed_ns)) / @as(f64, @floatFromInt(BATCHES));

    std.debug.print("   Time: {d:.2}ms\n", .{batch_elapsed_ms});
    std.debug.print("   FFI calls/sec: {d:.0}\n", .{batch_calls_per_sec});
    std.debug.print("   Latency: {d:.2} ns/batch\n\n", .{ns_per_batch});

    // Summary
    std.debug.print("📈 RESULTS:\n", .{});
    std.debug.print("┌─────────────────────────────────────────────┐\n", .{});
    std.debug.print("│ Single encode:  {:>10.0} calls/sec       │\n", .{ops_per_sec});
    std.debug.print("│ Batch encode:   {:>10.0} calls/sec       │\n", .{batch_calls_per_sec});
    std.debug.print("│ Batch size:     {:>10} queries/batch    │\n", .{BATCH_SIZE});
    std.debug.print("├─────────────────────────────────────────────┤\n", .{});
    std.debug.print("│ FFI overhead:   {:>10.0} ns/call          │\n", .{ns_per_op});
    std.debug.print("│ Batch overhead: {:>10.0} ns/batch         │\n", .{ns_per_batch});
    std.debug.print("└─────────────────────────────────────────────┘\n", .{});
}
