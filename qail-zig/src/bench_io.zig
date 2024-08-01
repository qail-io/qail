//! QAIL-Zig PostgreSQL I/O Benchmark (FIXED v2)
//!
//! Simple approach: read until socket has no more data.

const std = @import("std");
const qail = @import("qail.zig");
const net = std.net;
const posix = std.posix;

const QUERIES: usize = 10_000;
const BATCH_SIZE: usize = 100;

pub fn main() !void {
    const host = std.process.getEnvVarOwned(std.heap.page_allocator, "PG_HOST") catch "127.0.0.1";
    defer if (!std.mem.eql(u8, host, "127.0.0.1")) std.heap.page_allocator.free(host);

    const port_str = std.process.getEnvVarOwned(std.heap.page_allocator, "PG_PORT") catch "5432";
    defer if (!std.mem.eql(u8, port_str, "5432")) std.heap.page_allocator.free(port_str);
    const port = std.fmt.parseInt(u16, port_str, 10) catch 5432;

    const user = std.process.getEnvVarOwned(std.heap.page_allocator, "PG_USER") catch "orion";
    defer if (!std.mem.eql(u8, user, "orion")) std.heap.page_allocator.free(user);

    const database = std.process.getEnvVarOwned(std.heap.page_allocator, "PG_DATABASE") catch "postgres";
    defer if (!std.mem.eql(u8, database, "postgres")) std.heap.page_allocator.free(database);

    std.debug.print("🏁 QAIL-ZIG I/O BENCHMARK (v2)\n", .{});
    std.debug.print("===============================\n", .{});
    std.debug.print("Version: {s}\n", .{qail.version()});
    std.debug.print("Host: {s}:{d}\n", .{ host, port });
    std.debug.print("Queries: {d}\n\n", .{QUERIES});

    // Connect
    std.debug.print("📡 Connecting...\n", .{});

    const address = try net.Address.parseIp4(host, port);
    var stream = try net.tcpConnectToAddress(address);
    defer stream.close();

    // Startup
    var startup_buf: [256]u8 = undefined;
    var startup_len: usize = 8;
    std.mem.writeInt(u32, startup_buf[4..8], 196608, .big);
    startup_len += writeParam(&startup_buf, startup_len, "user", user);
    startup_len += writeParam(&startup_buf, startup_len, "database", database);
    startup_buf[startup_len] = 0;
    startup_len += 1;
    std.mem.writeInt(u32, startup_buf[0..4], @intCast(startup_len), .big);
    _ = try stream.write(startup_buf[0..startup_len]);

    var auth_buf: [1024]u8 = undefined;
    _ = try stream.read(&auth_buf);
    std.debug.print("✅ Connected!\n\n", .{});

    // Test 1: Individual
    std.debug.print("📊 [1/2] Individual Queries...\n", .{});

    var read_buf: [65536]u8 = undefined;
    const start1 = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < QUERIES) : (i += 1) {
        const limit: i64 = @intCast(@mod(i, 10) + 1);
        var query = qail.encodeSelect("harbors", "id,name", limit);
        defer query.deinit();

        _ = try stream.write(query.data);

        // Read response (wait for ReadyForQuery 'Z')
        var total: usize = 0;
        while (total < 50) { // Min response size
            const n = try stream.read(&read_buf);
            if (n == 0) break;
            total += n;
            // Check last bytes for 'Z' (ReadyForQuery)
            if (n >= 6 and read_buf[n - 6] == 'Z') break;
        }
    }

    const elapsed1 = @as(f64, @floatFromInt(@as(u64, @intCast(std.time.nanoTimestamp() - start1)))) / 1_000_000.0;
    const qps1 = @as(f64, @floatFromInt(QUERIES)) / (elapsed1 / 1000.0);
    std.debug.print("   {d:.0} q/s\n\n", .{qps1});

    // Test 2: Pipeline
    std.debug.print("📊 [2/2] Pipeline (batch {d})...\n", .{BATCH_SIZE});

    var limits: [BATCH_SIZE]i64 = undefined;
    for (&limits, 0..) |*l, j| {
        l.* = @intCast(@mod(j, 10) + 1);
    }

    const batches = QUERIES / BATCH_SIZE;
    const start2 = std.time.nanoTimestamp();

    var batch: usize = 0;
    while (batch < batches) : (batch += 1) {
        var query = qail.encodeBatch("harbors", "id,name", &limits);
        defer query.deinit();

        _ = try stream.write(query.data);

        // Read all responses - expect ~5KB per batch (100 small results)
        var total: usize = 0;
        while (total < 2000) { // Reasonable min for 100 queries
            const n = try stream.read(&read_buf);
            if (n == 0) break;
            total += n;
        }
    }

    const elapsed2 = @as(f64, @floatFromInt(@as(u64, @intCast(std.time.nanoTimestamp() - start2)))) / 1_000_000.0;
    const qps2 = @as(f64, @floatFromInt(QUERIES)) / (elapsed2 / 1000.0);
    std.debug.print("   {d:.0} q/s\n\n", .{qps2});

    // Summary
    std.debug.print("📈 RESULTS:\n", .{});
    std.debug.print("┌────────────────────────────────────┐\n", .{});
    std.debug.print("│ Individual: {:>10.0} q/s        │\n", .{qps1});
    std.debug.print("│ Pipeline:   {:>10.0} q/s        │\n", .{qps2});
    std.debug.print("├────────────────────────────────────┤\n", .{});
    std.debug.print("│ Native Rust:   354,000 q/s        │\n", .{});
    std.debug.print("│ Raw PDO:        29,000 q/s        │\n", .{});
    std.debug.print("└────────────────────────────────────┘\n", .{});
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
