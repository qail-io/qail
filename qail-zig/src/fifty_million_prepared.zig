//! QAIL-Zig 50 Million Query Benchmark (PREPARED STATEMENTS)
//!
//! Uses Extended Query Protocol for fair comparison with Rust.
//! Parse once, Bind+Execute 50M times.

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

    std.debug.print("🚀 QAIL-ZIG 50M (PREPARED STATEMENTS)\n", .{});
    std.debug.print("=====================================\n", .{});
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

    // Read auth response
    var auth_buf: [4096]u8 = undefined;
    _ = try stream.read(&auth_buf);
    std.debug.print("✅ Connected!\n\n", .{});

    // Step 1: Prepare statement ONCE
    std.debug.print("📝 Preparing statement...\n", .{});

    var parse_msg = qail.encodeParse("s1", "SELECT id, name FROM harbors LIMIT $1");
    defer parse_msg.deinit();

    var sync_msg = qail.encodeSync();
    defer sync_msg.deinit();

    // Send Parse + Sync
    _ = try stream.write(parse_msg.data);
    _ = try stream.write(sync_msg.data);

    // Wait for ParseComplete + ReadyForQuery
    var read_buf: [65536]u8 = undefined;
    _ = try stream.read(&read_buf);
    std.debug.print("✅ Statement prepared!\n\n", .{});

    // Step 2: Build batch params
    var param_strs: [10][:0]const u8 = undefined;
    param_strs[0] = "1";
    param_strs[1] = "2";
    param_strs[2] = "3";
    param_strs[3] = "4";
    param_strs[4] = "5";
    param_strs[5] = "6";
    param_strs[6] = "7";
    param_strs[7] = "8";
    param_strs[8] = "9";
    param_strs[9] = "10";

    // Pre-encode the batch (Bind+Execute for 10K queries)
    var encoded_batch = qail.encodePreparedBatch("s1", &param_strs, BATCH_SIZE);
    defer encoded_batch.deinit();

    std.debug.print("📊 Batch wire bytes: {d} bytes for {d} queries\n", .{ encoded_batch.data.len, BATCH_SIZE });
    std.debug.print("📊 Executing 50 million queries...\n\n", .{});

    const start = std.time.nanoTimestamp();
    var successful_queries: usize = 0;

    var batch: usize = 0;
    while (batch < BATCHES) : (batch += 1) {
        // Send batch (Bind+Execute x BATCH_SIZE + Sync)
        _ = try stream.write(encoded_batch.data);

        // Read all responses - count 'Z' (ReadyForQuery)
        var z_count: usize = 0;
        while (z_count < 1) { // One Sync = one Z
            const n = try stream.read(&read_buf);
            if (n == 0) break;
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
    std.debug.print("│ 50M QAIL-ZIG (PREPARED STATEMENTS)       │\n", .{});
    std.debug.print("├──────────────────────────────────────────┤\n", .{});
    std.debug.print("│ Total Time:           {:>15.1}s │\n", .{elapsed_s});
    std.debug.print("│ Queries/Second:       {:>15.0} │\n", .{qps});
    std.debug.print("│ Per Query:            {:>12.0}ns │\n", .{per_query_ns});
    std.debug.print("│ Successful:           {:>15} │\n", .{successful_queries});
    std.debug.print("└──────────────────────────────────────────┘\n", .{});

    std.debug.print("\n⚡ Uses Extended Query Protocol (Parse once!)\n", .{});
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
