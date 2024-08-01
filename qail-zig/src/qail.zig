//! QAIL-Zig: Zero-overhead bindings for QAIL Rust core
//!
//! This module provides Zig bindings to the QAIL Rust library
//! via C FFI with no runtime overhead.

const std = @import("std");

// FFI declarations for Rust functions (C ABI)
extern fn qail_version() [*:0]const u8;
extern fn qail_encode_select(table: [*:0]const u8, columns: [*:0]const u8, limit: i64, out_len: *usize) ?[*]u8;
extern fn qail_encode_batch(table: [*:0]const u8, columns: [*:0]const u8, limits: [*]const i64, count: usize, out_len: *usize) ?[*]u8;
extern fn qail_bytes_free(ptr: ?[*]u8, len: usize) void;
extern fn qail_transpile(qail_text: [*:0]const u8, out_len: *usize) ?[*:0]u8;
extern fn qail_string_free(ptr: ?[*:0]u8) void;

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
            qail_bytes_free(ptr, self.data.len);
            self.raw_ptr = null;
        }
    }
};

/// Encode a SELECT query to PostgreSQL wire protocol bytes
pub fn encodeSelect(table: [:0]const u8, columns: [:0]const u8, limit: i64) EncodedQuery {
    var out_len: usize = 0;
    const ptr = qail_encode_select(table.ptr, columns.ptr, limit, &out_len);
    
    if (ptr) |p| {
        return .{
            .data = p[0..out_len],
            .raw_ptr = p,
        };
    }
    
    return .{
        .data = &[_]u8{},
        .raw_ptr = null,
    };
}

/// Encode a batch of SELECT queries
pub fn encodeBatch(table: [:0]const u8, columns: [:0]const u8, limits: []const i64) EncodedQuery {
    var out_len: usize = 0;
    const ptr = qail_encode_batch(table.ptr, columns.ptr, limits.ptr, limits.len, &out_len);
    
    if (ptr) |p| {
        return .{
            .data = p[0..out_len],
            .raw_ptr = p,
        };
    }
    
    return .{
        .data = &[_]u8{},
        .raw_ptr = null,
    };
}

/// Transpile QAIL text to SQL
pub fn transpile(allocator: std.mem.Allocator, qail_text: [:0]const u8) !?[]const u8 {
    var out_len: usize = 0;
    const ptr = qail_transpile(qail_text.ptr, &out_len);
    
    if (ptr) |p| {
        defer qail_string_free(p);
        const result = try allocator.dupe(u8, p[0..out_len]);
        return result;
    }
    
    return null;
}

// Tests
test "version returns string" {
    const v = version();
    try std.testing.expect(v.len > 0);
}

test "encode select" {
    var query = encodeSelect("harbors", "id,name", 10);
    defer query.deinit();
    try std.testing.expect(query.data.len > 0);
}
