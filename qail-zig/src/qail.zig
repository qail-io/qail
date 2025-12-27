//! QAIL-Zig: Zero-overhead bindings for QAIL Rust core
//!
//! This module provides Zig bindings to the QAIL Rust library
//! via C FFI with no runtime overhead.

const std = @import("std");

// ============================================================================
// FFI declarations for Rust functions (C ABI) - matches qail-encoder exports
// ============================================================================

// Simple Query Protocol
extern fn qail_version() [*:0]const u8;
extern fn qail_encode_get(table: [*:0]const u8, columns: [*:0]const u8, limit: i64, out_ptr: *?[*]u8, out_len: *usize) i32;
extern fn qail_encode_uniform_batch(table: [*:0]const u8, columns: [*:0]const u8, limit: i64, count: usize, out_ptr: *?[*]u8, out_len: *usize) i32;
extern fn qail_free_bytes(ptr: ?[*]u8, len: usize) void;
extern fn qail_transpile(qail_text: [*:0]const u8) ?[*:0]u8;
extern fn qail_free(ptr: ?[*:0]u8) void;

// Extended Query Protocol (Prepared Statements)
extern fn qail_encode_parse(name: ?[*:0]const u8, sql: [*:0]const u8, out_ptr: *?[*]u8, out_len: *usize) i32;
extern fn qail_encode_sync(out_ptr: *?[*]u8, out_len: *usize) i32;
extern fn qail_encode_bind_execute_batch(statement: [*:0]const u8, params: [*]const [*:0]const u8, params_count: usize, count: usize, out_ptr: *?[*]u8, out_len: *usize) i32;

/// Get QAIL version string
pub fn version() []const u8 {
    const ptr = qail_version();
    return std.mem.span(ptr);
}

/// Encoded query bytes with automatic cleanup
pub const EncodedQuery = struct {
    data: []const u8,
    raw_ptr: ?[*]u8,

    pub fn deinit(self: *EncodedQuery) void {
        if (self.raw_ptr) |ptr| {
            qail_free_bytes(ptr, self.data.len);
            self.raw_ptr = null;
        }
    }
};

// ============================================================================
// Simple Query Protocol (full SQL sent each time)
// ============================================================================

/// Encode a SELECT query to PostgreSQL wire protocol bytes
pub fn encodeSelect(table: [:0]const u8, columns: [:0]const u8, limit: i64) EncodedQuery {
    var out_ptr: ?[*]u8 = null;
    var out_len: usize = 0;
    const result = qail_encode_get(table.ptr, columns.ptr, limit, &out_ptr, &out_len);

    if (result == 0 and out_ptr != null) {
        return .{
            .data = out_ptr.?[0..out_len],
            .raw_ptr = out_ptr,
        };
    }

    return .{
        .data = &[_]u8{},
        .raw_ptr = null,
    };
}

/// Encode a uniform batch of SELECT queries (same table/columns, repeated count times)
pub fn encodeBatch(table: [:0]const u8, columns: [:0]const u8, limits: []const i64) EncodedQuery {
    var out_ptr: ?[*]u8 = null;
    var out_len: usize = 0;

    const limit: i64 = if (limits.len > 0) limits[0] else 10;
    const result = qail_encode_uniform_batch(table.ptr, columns.ptr, limit, limits.len, &out_ptr, &out_len);

    if (result == 0 and out_ptr != null) {
        return .{
            .data = out_ptr.?[0..out_len],
            .raw_ptr = out_ptr,
        };
    }

    return .{
        .data = &[_]u8{},
        .raw_ptr = null,
    };
}

// ============================================================================
// Extended Query Protocol (Prepared Statements - FAST!)
// ============================================================================

/// Encode a Parse message to prepare a statement.
/// Use empty string "" for unnamed statement.
pub fn encodeParse(name: [:0]const u8, sql: [:0]const u8) EncodedQuery {
    var out_ptr: ?[*]u8 = null;
    var out_len: usize = 0;
    const result = qail_encode_parse(name.ptr, sql.ptr, &out_ptr, &out_len);

    if (result == 0 and out_ptr != null) {
        return .{
            .data = out_ptr.?[0..out_len],
            .raw_ptr = out_ptr,
        };
    }

    return .{
        .data = &[_]u8{},
        .raw_ptr = null,
    };
}

/// Encode a Sync message.
pub fn encodeSync() EncodedQuery {
    var out_ptr: ?[*]u8 = null;
    var out_len: usize = 0;
    const result = qail_encode_sync(&out_ptr, &out_len);

    if (result == 0 and out_ptr != null) {
        return .{
            .data = out_ptr.?[0..out_len],
            .raw_ptr = out_ptr,
        };
    }

    return .{
        .data = &[_]u8{},
        .raw_ptr = null,
    };
}

/// Encode a batch of Bind+Execute pairs for pipeline mode.
/// This is the HOT PATH for prepared statement performance.
///
/// Args:
///   statement: Name of prepared statement
///   params: Array of parameter strings (one per query)
///   count: Number of Bind+Execute pairs to generate
pub fn encodePreparedBatch(statement: [:0]const u8, params: []const [:0]const u8, count: usize) EncodedQuery {
    var out_ptr: ?[*]u8 = null;
    var out_len: usize = 0;

    // Build array of raw pointers for FFI
    var raw_params: [16][*:0]const u8 = undefined;
    const n_params = @min(params.len, 16);
    for (0..n_params) |i| {
        raw_params[i] = params[i].ptr;
    }

    const result = qail_encode_bind_execute_batch(statement.ptr, &raw_params, n_params, count, &out_ptr, &out_len);

    if (result == 0 and out_ptr != null) {
        return .{
            .data = out_ptr.?[0..out_len],
            .raw_ptr = out_ptr,
        };
    }

    return .{
        .data = &[_]u8{},
        .raw_ptr = null,
    };
}

/// Transpile QAIL text to SQL
pub fn transpile(allocator: std.mem.Allocator, qail_text: [:0]const u8) !?[]const u8 {
    const ptr = qail_transpile(qail_text.ptr);

    if (ptr) |p| {
        defer qail_free(p);
        const result = try allocator.dupe(u8, std.mem.span(p));
        return result;
    }

    return null;
}

// ============================================================================
// Tests
// ============================================================================

test "version returns string" {
    const v = version();
    try std.testing.expect(v.len > 0);
}

test "encode select" {
    var query = encodeSelect("harbors", "id,name", 10);
    defer query.deinit();
    try std.testing.expect(query.data.len > 0);
}

test "encode parse" {
    var query = encodeParse("stmt1", "SELECT $1");
    defer query.deinit();
    try std.testing.expect(query.data.len > 0);
    try std.testing.expect(query.data[0] == 'P');
}
