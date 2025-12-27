//! QAIL-Zig 50 Million Query Benchmark
//!
//! Fair comparison with Rust fifty_million_benchmark.
//!
//! Note: qail-zig uses Simple Query protocol (not prepared statements)
//! so this is not a perfect apples-to-apples comparison.

const std = @import("std");
const qail = @import("qail.zig");
const net = std.net;

const TOTAL_QUERIES: usize = 50_000_000;
const BATCH_SIZE: usize = 10_000;
const BATCHES: usize = TOTAL_QUERIES / BATCH_SIZE;

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const host = std.process.getEnvVarOwned(allocator, "PG_HOST") catch "127.0.0.1";
    defer if (!std.mem.eql(u8, host, "127.0.0.1")) allocator.free(host);

    const port_str = std.process.getEnvVarOwned(allocator, "PG_PORT") catch "5432";
    defer if (!std.mem.eql(u8, port_str, "5432")) allocator.free(port_str);
    const port = std.fmt.parseInt(u16, port_str, 10) catch 5432;

    const user = std.process.getEnvVarOwned(allocator, "PG_USER") catch "orion";
    defer if (!std.mem.eql(u8, user, "orion")) allocator.free(user);

    const database = std.process.getEnvVarOwned(allocator, "PG_DATABASE") catch "postgres";
    defer if (!std.mem.eql(u8, database, "postgres")) allocator.free(database);

    std.debug.print("🚀 QAIL-ZIG 50 MILLION QUERY BENCHMARK\n", .{});
    std.debug.print("======================================\n", .{});
    std.debug.print("Version: {s}\n", .{qail.version()});
    std.debug.print("Total queries:    {:>15}\n", .{TOTAL_QUERIES});
    std.debug.print("Batch size:       {:>15}\n", .{BATCH_SIZE});
    std.debug.print("Batches:          {:>15}\n\n", .{BATCHES});

    // Connect
    std.debug.print("🔌 Connecting to {s}:{d} as {s}...\n", .{ host, port, user });

    const address = try net.Address.parseIp4(host, port);
    var stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Startup message
    var startup_buf: [256]u8 = undefined;
    var startup_len: usize = 8;
    std.mem.writeInt(u32, startup_buf[4..8], 196608, .big);
    startup_len += writeParam(&startup_buf, startup_len, "user", user);
    startup_len += writeParam(&startup_buf, startup_len, "database", database);
    startup_buf[startup_len] = 0;
    startup_len += 1;
    std.mem.writeInt(u32, startup_buf[0..4], @intCast(startup_len), .big);
    _ = try stream.write(startup_buf[0..startup_len]);

    // Read auth response (wait for ReadyForQuery)
    var auth_buf: [4096]u8 = undefined;
    var auth_total: usize = 0;
    while (auth_total < 100) {
        const n = try stream.read(&auth_buf);
        if (n == 0) break;
        auth_total += n;
        // Check for 'Z' (ReadyForQuery)
        if (auth_buf[n - 1] == 'I' or auth_buf[n - 6] == 'Z') break;
    }
    std.debug.print("✅ Connected!\n\n", .{});

    // Pre-encode the batch (same query repeated)
    var limits: [BATCH_SIZE]i64 = undefined;
    for (&limits, 0..) |*l, j| {
        l.* = @intCast(@mod(j, 10) + 1);
    }

    var encoded_batch = qail.encodeBatch("harbors", "id,name", &limits);
    defer encoded_batch.deinit();

    std.debug.print("📊 Batch wire bytes: {d} bytes for {d} queries\n", .{ encoded_batch.data.len, BATCH_SIZE });
    std.debug.print("📊 Executing 50 million queries...\n\n", .{});

    var read_buf: [1048576]u8 = undefined; // 1MB buffer for responses
    const start = std.time.nanoTimestamp();
    var successful_queries: usize = 0;

    var batch: usize = 0;
    while (batch < BATCHES) : (batch += 1) {
        // Send batch
        _ = try stream.write(encoded_batch.data);

        // Read all responses - count 'Z' (ReadyForQuery) messages
        // Each Simple Query gets one 'Z' back
        var z_count: usize = 0;
        while (z_count < BATCH_SIZE) {
            const n = try stream.read(&read_buf);
            if (n == 0) break;

            // Count 'Z' bytes (ReadyForQuery messages)
            for (read_buf[0..n]) |byte| {
                if (byte == 'Z') z_count += 1;
            }
        }

        successful_queries += BATCH_SIZE;

        // Progress report every 1M queries
        if (successful_queries % 1_000_000 == 0) {
            const now = std.time.nanoTimestamp();
            const elapsed_ns: u64 = @intCast(now - start);
            const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
            const qps = @as(f64, @floatFromInt(successful_queries)) / elapsed_s;
            const remaining = TOTAL_QUERIES - successful_queries;
            const eta = @as(f64, @floatFromInt(remaining)) / qps;

            std.debug.print("{:>6}M queries | {:>8.0} q/s | ETA: {:.0}s | Batch {}/{}\n", .{
                successful_queries / 1_000_000,
                qps,
                eta,
                batch + 1,
                BATCHES,
            });
        }
    }

    const end = std.time.nanoTimestamp();
    const elapsed_ns: u64 = @intCast(end - start);
    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
    const qps = @as(f64, @floatFromInt(successful_queries)) / elapsed_s;
    const per_query_ns = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(successful_queries));

    std.debug.print("\n📈 FINAL RESULTS:\n", .{});
    std.debug.print("┌──────────────────────────────────────────┐\n", .{});
    std.debug.print("│ 50 MILLION QUERY (QAIL-ZIG)              │\n", .{});
    std.debug.print("├──────────────────────────────────────────┤\n", .{});
    std.debug.print("│ Total Time:           {:>15.1}s │\n", .{elapsed_s});
    std.debug.print("│ Queries/Second:       {:>15.0} │\n", .{qps});
    std.debug.print("│ Per Query:            {:>12.0}ns │\n", .{per_query_ns});
    std.debug.print("│ Successful:           {:>15} │\n", .{successful_queries});
    std.debug.print("└──────────────────────────────────────────┘\n", .{});

    std.debug.print("\n⚡ Note: Uses Simple Query protocol (not prepared)\n", .{});
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
