//! QAIL-Zig vs pg.zig Benchmark (TRULY FAIR with Response Parsing)
//!
//! Both execute single queries AND parse responses.
//! This is apples-to-apples comparison.

const std = @import("std");
const pg = @import("pg_zig");
const qail = @import("qail.zig");
const net = std.net;

const QUERIES: usize = 10_000;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("🏁 QAIL-Zig vs pg.zig (TRULY FAIR - With Response Parsing)\n", .{});
    std.debug.print("============================================================\n", .{});
    std.debug.print("Queries: {d}\n", .{QUERIES});
    std.debug.print("Mode: Single query with response parsing\n\n", .{});

    // ========== Test 1: pg.zig (native Zig) ==========
    std.debug.print("📊 [1/2] pg.zig (pure Zig)...\n", .{});

    const uri = std.Uri.parse("postgres://orion@127.0.0.1:5432/postgres") catch unreachable;
    var pool = try pg.Pool.initUri(allocator, uri, .{});
    defer pool.deinit();

    var pg_total_rows: usize = 0;
    const start1 = std.time.nanoTimestamp();

    var i: usize = 0;
    while (i < QUERIES) : (i += 1) {
        const limit: i32 = @intCast(@mod(i, 10) + 1);

        var result = try pool.query("SELECT id, name FROM harbors LIMIT $1", .{limit});
        defer result.deinit();

        while (try result.next()) |row| {
            // Parse each column (same work as QAIL-Zig)
            const id = row.get(i32, 0);
            const name = row.get([]u8, 1);
            _ = id;
            _ = name;
            pg_total_rows += 1;
        }
    }

    const elapsed1 = @as(f64, @floatFromInt(@as(u64, @intCast(std.time.nanoTimestamp() - start1)))) / 1_000_000.0;
    const qps1 = @as(f64, @floatFromInt(QUERIES)) / (elapsed1 / 1000.0);

    std.debug.print("   {d:.0} q/s ({d} rows parsed)\n\n", .{ qps1, pg_total_rows });

    // ========== Test 2: QAIL-Zig (with response parsing) ==========
    std.debug.print("📊 [2/2] QAIL-Zig (with response parsing)...\n", .{});

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
    var qail_total_rows: usize = 0;
    const start2 = std.time.nanoTimestamp();

    var j: usize = 0;
    while (j < QUERIES) : (j += 1) {
        const limit: i64 = @intCast(@mod(j, 10) + 1);

        // Encode and send query
        var query = qail.encodeSelect("harbors", "id,name", limit);
        defer query.deinit();
        _ = try stream.write(query.data);

        // Read full response until 'Z' (ReadyForQuery)
        var total_read: usize = 0;
        var done = false;
        while (!done) {
            const n = try stream.read(read_buf[total_read..]);
            if (n == 0) break;
            total_read += n;

            // Check for ReadyForQuery at end
            for (read_buf[0..total_read]) |byte| {
                if (byte == 'Z') {
                    done = true;
                    break;
                }
            }
        }

        // Parse response (same work as pg.zig)
        if (qail.Response.parse(read_buf[0..total_read])) |resp| {
            var response = resp; // Make mutable copy
            defer response.deinit();

            var row_idx: usize = 0;
            while (row_idx < response.rowCount()) : (row_idx += 1) {
                // Parse each column (same work as pg.zig)
                const id = response.getI32(row_idx, 0);
                const name = response.getString(row_idx, 1);
                _ = id;
                _ = name;
                qail_total_rows += 1;
            }
        }
    }

    const elapsed2 = @as(f64, @floatFromInt(@as(u64, @intCast(std.time.nanoTimestamp() - start2)))) / 1_000_000.0;
    const qps2 = @as(f64, @floatFromInt(QUERIES)) / (elapsed2 / 1000.0);

    std.debug.print("   {d:.0} q/s ({d} rows parsed)\n\n", .{ qps2, qail_total_rows });

    // Summary
    std.debug.print("📈 RESULTS (FAIR - Both Parse Responses):\n", .{});
    std.debug.print("┌────────────────────────────────────────┐\n", .{});
    std.debug.print("│ pg.zig:     {:>10.0} q/s             │\n", .{qps1});
    std.debug.print("│ QAIL-Zig:   {:>10.0} q/s             │\n", .{qps2});
    std.debug.print("├────────────────────────────────────────┤\n", .{});
    if (qps1 > qps2) {
        const ratio = qps1 / qps2;
        if (ratio > 1.1) {
            std.debug.print("│ pg.zig is {d:.1}x faster                │\n", .{ratio});
        } else {
            std.debug.print("│ Performance is similar (~equal)       │\n", .{});
        }
    } else {
        const ratio = qps2 / qps1;
        if (ratio > 1.1) {
            std.debug.print("│ QAIL-Zig is {d:.1}x faster              │\n", .{ratio});
        } else {
            std.debug.print("│ Performance is similar (~equal)       │\n", .{});
        }
    }
    std.debug.print("└────────────────────────────────────────┘\n", .{});
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
