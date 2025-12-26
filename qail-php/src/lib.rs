//! QAIL PHP FFI - C-compatible bindings for PHP FFI
//!
//! Exports functions that PHP can call via FFI extension.
//! Provides high-performance query encoding without PHP string manipulation.

use std::ffi::{CStr, c_char};
use qail_core::prelude::*;
use qail_pg::protocol::AstEncoder;

/// Encode a SELECT query to PostgreSQL wire protocol bytes.
/// 
/// # Arguments
/// * `table` - Table name (C string)
/// * `columns` - Comma-separated column names (C string, or "*" for all)
/// * `limit` - LIMIT value (0 or negative for no limit)
/// * `out_len` - Output: length of returned bytes
/// 
/// # Returns
/// Pointer to encoded bytes. Caller must free with `qail_bytes_free`.
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_select(
    table: *const c_char,
    columns: *const c_char,
    limit: i64,
    out_len: *mut usize,
) -> *mut u8 {
    if table.is_null() {
        unsafe { *out_len = 0; }
        return std::ptr::null_mut();
    }
    
    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let columns_str = if columns.is_null() {
        "*"
    } else {
        unsafe { CStr::from_ptr(columns).to_str().unwrap_or("*") }
    };
    
    // Build QailCmd
    let mut cmd = QailCmd::get(table);
    
    // Parse columns
    if !columns_str.is_empty() && columns_str != "*" {
        cmd.columns = columns_str
            .split(',')
            .map(|c| Expr::Named(c.trim().to_string()))
            .collect();
    }
    
    // Add limit if positive
    if limit > 0 {
        cmd = cmd.limit(limit);
    }
    
    // Encode to wire protocol
    let (wire_bytes, _params) = AstEncoder::encode_cmd(&cmd);
    let bytes = wire_bytes.to_vec();
    
    let len = bytes.len();
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;
    unsafe { *out_len = len; }
    ptr
}

/// Encode a batch of SELECT queries with different limits.
/// More efficient than calling qail_encode_select multiple times.
/// 
/// # Arguments
/// * `table` - Table name
/// * `columns` - Comma-separated columns
/// * `limits` - Array of limit values
/// * `count` - Number of queries
/// * `out_len` - Output: total bytes length
/// 
/// # Returns
/// Pointer to encoded bytes for entire batch.
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_batch(
    table: *const c_char,
    columns: *const c_char,
    limits: *const i64,
    count: usize,
    out_len: *mut usize,
) -> *mut u8 {
    if table.is_null() || count == 0 {
        unsafe { *out_len = 0; }
        return std::ptr::null_mut();
    }
    
    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let columns_str = if columns.is_null() {
        "*"
    } else {
        unsafe { CStr::from_ptr(columns).to_str().unwrap_or("*") }
    };
    
    // Pre-parse columns once
    let col_exprs: Vec<Expr> = if !columns_str.is_empty() && columns_str != "*" {
        columns_str.split(',')
            .map(|c| Expr::Named(c.trim().to_string()))
            .collect()
    } else {
        vec![]
    };
    
    // Build all commands
    let mut cmds = Vec::with_capacity(count);
    for i in 0..count {
        let limit = unsafe { *limits.add(i) };
        let mut cmd = QailCmd::get(table);
        cmd.columns = col_exprs.clone();
        if limit > 0 {
            cmd = cmd.limit(limit);
        }
        cmds.push(cmd);
    }
    
    // Encode entire batch
    let batch_bytes = AstEncoder::encode_batch(&cmds);
    let bytes = batch_bytes.to_vec();
    
    let len = bytes.len();
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;
    unsafe { *out_len = len; }
    ptr
}

/// Free bytes allocated by qail functions.
/// Must be called to prevent memory leaks.
#[unsafe(no_mangle)]
pub extern "C" fn qail_bytes_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len));
        }
    }
}

/// Get QAIL version string.
/// Returns static string - do not free.
#[unsafe(no_mangle)]
pub extern "C" fn qail_version() -> *const c_char {
    static VERSION: &[u8] = b"0.10.1\0";
    VERSION.as_ptr() as *const c_char
}

/// Transpile QAIL text to SQL.
/// For text-based queries (like WASM).
#[unsafe(no_mangle)]
pub extern "C" fn qail_transpile(
    qail_text: *const c_char,
    out_len: *mut usize,
) -> *mut c_char {
    if qail_text.is_null() {
        unsafe { *out_len = 0; }
        return std::ptr::null_mut();
    }
    
    let input = unsafe { CStr::from_ptr(qail_text).to_str().unwrap_or("") };
    
    match qail_core::parse(input) {
        Ok(cmd) => {
            let sql = cmd.to_sql();
            let len = sql.len();
            let c_str = std::ffi::CString::new(sql).unwrap();
            unsafe { *out_len = len; }
            c_str.into_raw()
        }
        Err(_) => {
            unsafe { *out_len = 0; }
            std::ptr::null_mut()
        }
    }
}

/// Free string allocated by qail_transpile.
#[unsafe(no_mangle)]
pub extern "C" fn qail_string_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            let _ = std::ffi::CString::from_raw(ptr);
        }
    }
}
