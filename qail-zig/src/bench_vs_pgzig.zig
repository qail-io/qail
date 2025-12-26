//! QAIL-Zig vs pg.zig Benchmark
//!
//! Compares QAIL (Rust core via FFI) against native Zig pg.zig driver

const std = @import("std");
const pg = @import("pg_zig");
const qail = @import("qail.zig");
const net = std.net;

const QUERIES: usize = 10_000;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("🏁 QAIL-Zig vs pg.zig BENCHMARK\n", .{});
    std.debug.print("================================\n", .{});
    std.debug.print("Queries: {d}\n\n", .{QUERIES});

    // ========== Test 1: pg.zig (native Zig) ==========
    std.debug.print("📊 [1/2] pg.zig (native Zig)...\n", .{});

    // Use URI-based init
    const uri = std.Uri.parse("postgres://orion@127.0.0.1:5432/postgres") catch unreachable;
    var pool = try pg.Pool.initUri(allocator, uri, .{});
    defer pool.deinit();

    const start1 = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < QUERIES) : (i += 1) {
        const limit: i32 = @intCast(@mod(i, 10) + 1);

        var result = try pool.query("SELECT id, name FROM harbors LIMIT $1", .{limit});
        defer result.deinit();

        // Drain results
        while (try result.next()) |_| {}
    }

    const elapsed1 = @as(f64, @floatFromInt(@as(u64, @intCast(std.time.nanoTimestamp() - start1)))) / 1_000_000.0;
    const qps1 = @as(f64, @floatFromInt(QUERIES)) / (elapsed1 / 1000.0);

    std.debug.print("   {d:.0} q/s\n\n", .{qps1});

    // ========== Test 2: QAIL-Zig (Rust FFI) ==========
    std.debug.print("📊 [2/2] QAIL-Zig (Rust FFI + socket)...\n", .{});

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

    var read_buf: [65536]u8 = undefined;
    const start2 = std.time.nanoTimestamp();

    var j: usize = 0;
    while (j < QUERIES) : (j += 1) {
        const limit: i64 = @intCast(@mod(j, 10) + 1);
        var query = qail.encodeSelect("harbors", "id,name", limit);
        defer query.deinit();

        _ = try stream.write(query.data);

        // Read response
        var total: usize = 0;
        while (total < 50) {
            const n = try stream.read(&read_buf);
            if (n == 0) break;
            total += n;
            if (n >= 6 and read_buf[n - 6] == 'Z') break;
        }
    }

    const elapsed2 = @as(f64, @floatFromInt(@as(u64, @intCast(std.time.nanoTimestamp() - start2)))) / 1_000_000.0;
    const qps2 = @as(f64, @floatFromInt(QUERIES)) / (elapsed2 / 1000.0);

    std.debug.print("   {d:.0} q/s\n\n", .{qps2});

    // Summary
    std.debug.print("📈 RESULTS:\n", .{});
    std.debug.print("┌────────────────────────────────────┐\n", .{});
    std.debug.print("│ pg.zig:     {:>10.0} q/s        │\n", .{qps1});
    std.debug.print("│ QAIL-Zig:   {:>10.0} q/s        │\n", .{qps2});
    std.debug.print("├────────────────────────────────────┤\n", .{});
    if (qps1 > qps2) {
        std.debug.print("│ pg.zig is {d:.1}x faster           │\n", .{qps1 / qps2});
    } else {
        std.debug.print("│ QAIL-Zig is {d:.1}x faster         │\n", .{qps2 / qps1});
    }
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
