//! QAIL-Zig Benchmark
//!
//! Compares Zig FFI performance against native Rust baseline.

const std = @import("std");
const qail = @import("qail.zig");

const ITERATIONS: usize = 100_000;
const BATCH_SIZE: usize = 1_000;
const BATCHES: usize = 100;

pub fn main() void {
    std.debug.print("🏁 QAIL-ZIG BENCHMARK\n", .{});
    std.debug.print("=====================\n", .{});
    std.debug.print("Version: {s}\n\n", .{qail.version()});
    
    // Benchmark 1: Individual encoding
    std.debug.print("📊 Test 1: Individual Encoding ({d} iterations)\n", .{ITERATIONS});
    
    // Use nanoTimestamp for timing
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
    
    std.debug.print("   {d} encodes in {d:.2}ms\n", .{ITERATIONS, elapsed_ms});
    std.debug.print("   {d:.0} ops/sec\n", .{ops_per_sec});
    std.debug.print("   {d:.2} ns/op\n\n", .{ns_per_op});
    
    // Benchmark 2: Batch encoding
    std.debug.print("📊 Test 2: Batch Encoding ({d} queries per batch)\n", .{BATCH_SIZE});
    
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
    const total_queries = BATCH_SIZE * BATCHES;
    const batch_ops_per_sec = @as(f64, @floatFromInt(total_queries)) / (batch_elapsed_ms / 1000.0);
    
    std.debug.print("   {d} queries in {d:.2}ms\n", .{total_queries, batch_elapsed_ms});
    std.debug.print("   {d:.0} q/s (batched)\n\n", .{batch_ops_per_sec});
    
    // Summary
    std.debug.print("📈 RESULTS:\n", .{});
    std.debug.print("┌────────────────────────────────────────┐\n", .{});
    std.debug.print("│ Individual: {:>12.0} ops/sec       │\n", .{ops_per_sec});
    std.debug.print("│ Batched:    {:>12.0} q/s           │\n", .{batch_ops_per_sec});
    std.debug.print("├────────────────────────────────────────┤\n", .{});
    std.debug.print("│ Context:                               │\n", .{});
    std.debug.print("│ - Native Rust: 354,000 q/s             │\n", .{});
    std.debug.print("│ - Go CGO:      126,000 q/s             │\n", .{});
    std.debug.print("│ - PHP Ext:     232,000 q/s             │\n", .{});
    std.debug.print("└────────────────────────────────────────┘\n", .{});
}
