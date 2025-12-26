//! QAIL-Zig vs pg.zig - 10 Million Pipeline Benchmark
//!
//! Tests pipeline/batch mode with 10M queries

const std = @import("std");
const pg = @import("pg_zig");
const qail = @import("qail.zig");
const net = std.net;

const TOTAL_QUERIES: usize = 10_000_000;
const BATCH_SIZE: usize = 1000;
const BATCHES: usize = TOTAL_QUERIES / BATCH_SIZE;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("🏁 10 MILLION QUERY PIPELINE BENCHMARK\n", .{});
    std.debug.print("=======================================\n", .{});
    std.debug.print("Total Queries: {d}\n", .{TOTAL_QUERIES});
    std.debug.print("Batch Size: {d}\n", .{BATCH_SIZE});
    std.debug.print("Batches: {d}\n\n", .{BATCHES});

    // ========== Test 1: pg.zig (native Zig) ==========
    std.debug.print("📊 [1/2] pg.zig (native Zig, individual queries)...\n", .{});
    std.debug.print("   Note: pg.zig doesn't have native pipeline/batch mode.\n", .{});
    std.debug.print("   Running 100K queries as sample...\n", .{});

    const uri = std.Uri.parse("postgres://orion@127.0.0.1:5432/postgres") catch unreachable;
    var pool = try pg.Pool.initUri(allocator, uri, .{});
    defer pool.deinit();

    const sample_queries: usize = 100_000;
    const start1 = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < sample_queries) : (i += 1) {
        const limit: i32 = @intCast(@mod(i, 10) + 1);
        var result = try pool.query("SELECT id, name FROM harbors LIMIT $1", .{limit});
        defer result.deinit();
        while (try result.next()) |_| {}
    }

    const elapsed1 = @as(f64, @floatFromInt(@as(u64, @intCast(std.time.nanoTimestamp() - start1)))) / 1_000_000.0;
    const qps1 = @as(f64, @floatFromInt(sample_queries)) / (elapsed1 / 1000.0);
    const projected1 = (10_000_000.0 / qps1);

    std.debug.print("   {d:.0} q/s (projected: {d:.0}s for 10M)\n\n", .{ qps1, projected1 });

    // ========== Test 2: QAIL-Zig Pipeline ==========
    std.debug.print("📊 [2/2] QAIL-Zig (Rust FFI + Pipeline)...\n", .{});
    std.debug.print("   Running full 10M queries...\n", .{});

    const address = try net.Address.parseIp4("127.0.0.1", 5432);
    var stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // PostgreSQL startup
    var startup_buf: [256]u8 = undefined;
    var startup_len: usize = 8;
    std.mem.writeInt(u32, startup_buf[4..8], 196608, .big);
    startup_len += writeParam(&startup_buf, startup_len, "user", "orion");
    startup_len += writeParam(&startup_buf, startup_len, "database", "postgres");
    startup_buf[startup_len] = 0;
    startup_len += 1;
    std.mem.writeInt(u32, startup_buf[0..4], @intCast(startup_len), .big);
    _ = try stream.write(startup_buf[0..startup_len]);

    var auth_buf: [1024]u8 = undefined;
    _ = try stream.read(&auth_buf);

    // Build limits array for batch
    var limits: [BATCH_SIZE]i64 = undefined;
    for (&limits, 0..) |*l, j| {
        l.* = @intCast(@mod(j, 10) + 1);
    }

    var read_buf: [65536]u8 = undefined;
    const start2 = std.time.nanoTimestamp();

    var batch: usize = 0;
    while (batch < BATCHES) : (batch += 1) {
        var query = qail.encodeBatch("harbors", "id,name", &limits);
        defer query.deinit();

        _ = try stream.write(query.data);

        // Read responses
        var total: usize = 0;
        while (total < 2000) {
            const n = try stream.read(&read_buf);
            if (n == 0) break;
            total += n;
        }

        // Progress every 1000 batches (1M queries)
        if (batch % 1000 == 0 and batch > 0) {
            std.debug.print("   Progress: {d}M queries...\n", .{batch / 1000});
        }
    }

    const elapsed2 = @as(f64, @floatFromInt(@as(u64, @intCast(std.time.nanoTimestamp() - start2)))) / 1_000_000.0;
    const qps2 = @as(f64, @floatFromInt(TOTAL_QUERIES)) / (elapsed2 / 1000.0);

    std.debug.print("   {d:.0} q/s ({d:.2}s for 10M)\n\n", .{ qps2, elapsed2 / 1000.0 });

    // Summary
    std.debug.print("📈 RESULTS (10 MILLION QUERIES):\n", .{});
    std.debug.print("┌────────────────────────────────────────────┐\n", .{});
    std.debug.print("│ pg.zig (individual):  {:>10.0} q/s       │\n", .{qps1});
    std.debug.print("│ QAIL-Zig (pipeline):  {:>10.0} q/s       │\n", .{qps2});
    std.debug.print("├────────────────────────────────────────────┤\n", .{});
    std.debug.print("│ QAIL-Zig is {d:.1}x faster                  │\n", .{qps2 / qps1});
    std.debug.print("├────────────────────────────────────────────┤\n", .{});
    std.debug.print("│ Context (pipeline baseline):               │\n", .{});
    std.debug.print("│ - Native Rust: 354,000 q/s                 │\n", .{});
    std.debug.print("└────────────────────────────────────────────┘\n", .{});
}

fn writeParam(buf: []u8, offset: usize, name: []const u8, value: []const u8) usize {
    var len: usize = 0;
    @memcpy(buf[offset..][0..name.len], name);
    len += name.len;
    buf[offset + len] = 0;
    len += 1;
    @memcpy(buf[offset + len ..][0..value.len], value);
    len += value.len;
    buf[offset + len] = 0;
    len += 1;
    return len;
}
