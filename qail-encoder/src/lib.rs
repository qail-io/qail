//! QAIL Encoder - Lightweight wire protocol encoding
//!
//! This crate provides:
//! - AST to PostgreSQL wire protocol encoding
//! - QAIL text to SQL transpilation
//! - C FFI for language bindings
//!
//! NO I/O, NO TLS, NO async - just pure encoding.
//! Languages handle their own I/O (Zig, Go, etc.)

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use qail_core::transpiler::ToSql;
use std::cell::RefCell;

thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = RefCell::new(None);
}

fn set_error(msg: String) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = Some(msg);
    });
}

fn clear_error() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

// ============================================================================
// Version
// ============================================================================

/// Get QAIL version string.
#[unsafe(no_mangle)]
pub extern "C" fn qail_version() -> *const c_char {
    static VERSION: &[u8] = concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes();
    VERSION.as_ptr() as *const c_char
}

// ============================================================================
// Transpiler
// ============================================================================

/// Transpile QAIL text to SQL.
/// Returns NULL on error.
/// Caller must free with qail_free().
#[unsafe(no_mangle)]
pub extern "C" fn qail_transpile(qail: *const c_char) -> *mut c_char {
    clear_error();
    
    if qail.is_null() {
        set_error("NULL input".to_string());
        return std::ptr::null_mut();
    }

    let c_str = unsafe { CStr::from_ptr(qail) };
    let qail_str = match c_str.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(format!("Invalid UTF-8: {}", e));
            return std::ptr::null_mut();
        }
    };

    match qail_core::parse(qail_str) {
        Ok(cmd) => {
            let sql = cmd.to_sql();
            match CString::new(sql) {
                Ok(c_string) => c_string.into_raw(),
                Err(e) => {
                    set_error(format!("NUL byte in output: {}", e));
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            set_error(format!("{:?}", e));
            std::ptr::null_mut()
        }
    }
}

/// Validate QAIL syntax.
/// Returns 1 if valid, 0 if invalid.
#[unsafe(no_mangle)]
pub extern "C" fn qail_validate(qail: *const c_char) -> i32 {
    if qail.is_null() {
        return 0;
    }

    let c_str = unsafe { CStr::from_ptr(qail) };
    match c_str.to_str() {
        Ok(s) => {
            if qail_core::parse(s).is_ok() { 1 } else { 0 }
        }
        Err(_) => 0,
    }
}

// ============================================================================
// Wire Protocol Encoding
// ============================================================================

/// Encode a SELECT query to PostgreSQL wire protocol bytes.
/// 
/// Returns 0 on success, non-zero on error.
/// Caller must free with qail_free_bytes().
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_get(
    table: *const c_char,
    columns: *const c_char,  // comma-separated, or "*" for all
    limit: i64,              // -1 for no limit
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    clear_error();
    
    if table.is_null() || out_ptr.is_null() || out_len.is_null() {
        set_error("NULL pointer argument".to_string());
        return -1;
    }
    
    let table_str = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(format!("Invalid UTF-8 in table: {}", e));
            return -2;
        }
    };
    
    // Build QailCmd
    let mut cmd = qail_core::ast::QailCmd::get(table_str);
    
    // Parse columns
    if !columns.is_null() {
        let cols_str = match unsafe { CStr::from_ptr(columns) }.to_str() {
            Ok(s) => s,
            Err(e) => {
                set_error(format!("Invalid UTF-8 in columns: {}", e));
                return -3;
            }
        };
        
        if cols_str == "*" {
            cmd = cmd.select_all();
        } else {
            for col in cols_str.split(',') {
                let col = col.trim();
                if !col.is_empty() {
                    cmd = cmd.column(col);
                }
            }
        }
    } else {
        cmd = cmd.select_all();
    }
    
    // Apply limit
    if limit >= 0 {
        cmd = cmd.limit(limit);
    }
    
    // Encode to Simple Query wire bytes
    let sql = cmd.to_sql();
    let wire_bytes = encode_simple_query(&sql);
    let len = wire_bytes.len();
    
    // Transfer ownership to caller
    let mut boxed = wire_bytes.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    
    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }
    
    0 // Success
}

/// Encode a batch of uniform SELECT queries.
/// All queries have same table/columns, just repeated `count` times.
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_uniform_batch(
    table: *const c_char,
    columns: *const c_char,
    limit: i64,
    count: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    clear_error();
    
    if table.is_null() || out_ptr.is_null() || out_len.is_null() || count == 0 {
        set_error("NULL pointer or zero count".to_string());
        return -1;
    }
    
    let table_str = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(format!("Invalid UTF-8 in table: {}", e));
            return -2;
        }
    };
    
    // Build the base command
    let mut base_cmd = qail_core::ast::QailCmd::get(table_str);
    
    if !columns.is_null() {
        if let Ok(cols_str) = unsafe { CStr::from_ptr(columns) }.to_str() {
            if cols_str == "*" {
                base_cmd = base_cmd.select_all();
            } else {
                for col in cols_str.split(',') {
                    let col = col.trim();
                    if !col.is_empty() {
                        base_cmd = base_cmd.column(col);
                    }
                }
            }
        }
    } else {
        base_cmd = base_cmd.select_all();
    }
    
    if limit >= 0 {
        base_cmd = base_cmd.limit(limit);
    }
    
    // Encode SQL once, repeat count times
    let sql = base_cmd.to_sql();
    let single_query = encode_simple_query(&sql);
    
    // Batch: repeat the query `count` times
    let mut batch_bytes = Vec::with_capacity(single_query.len() * count);
    for _ in 0..count {
        batch_bytes.extend_from_slice(&single_query);
    }
    
    let len = batch_bytes.len();
    let mut boxed = batch_bytes.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    
    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }
    
    0
}

// ============================================================================
// Memory Management
// ============================================================================

/// Free a string returned by qail_transpile.
#[unsafe(no_mangle)]
pub extern "C" fn qail_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

/// Free bytes returned by qail_encode_* functions.
#[unsafe(no_mangle)]
pub extern "C" fn qail_free_bytes(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let _ = Vec::from_raw_parts(ptr, len, len);
        }
    }
}

/// Get the last error message.
#[unsafe(no_mangle)]
pub extern "C" fn qail_last_error() -> *const c_char {
    thread_local! {
        static ERROR_CSTRING: RefCell<Option<CString>> = RefCell::new(None);
    }
    
    LAST_ERROR.with(|e| {
        let error = e.borrow();
        match &*error {
            Some(msg) => {
                ERROR_CSTRING.with(|ec| {
                    let c_str = CString::new(msg.clone()).unwrap_or_default();
                    let ptr = c_str.as_ptr();
                    *ec.borrow_mut() = Some(c_str);
                    ptr
                })
            }
            None => std::ptr::null(),
        }
    })
}

// ============================================================================
// Internal: Simple Query Encoding
// ============================================================================

/// Encode a SQL string as PostgreSQL Simple Query message.
/// Format: 'Q' + int32 length + query string + '\0'
fn encode_simple_query(sql: &str) -> Vec<u8> {
    let sql_bytes = sql.as_bytes();
    let msg_len = 4 + sql_bytes.len() + 1; // 4 byte length + query + null
    
    let mut buf = Vec::with_capacity(1 + msg_len);
    buf.push(b'Q');                              // Message type
    buf.extend_from_slice(&(msg_len as i32).to_be_bytes()); // Length (big-endian)
    buf.extend_from_slice(sql_bytes);            // Query
    buf.push(0);                                 // Null terminator
    
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_version() {
        let v = qail_version();
        let s = unsafe { CStr::from_ptr(v) }.to_str().unwrap();
        assert!(!s.is_empty());
    }

    #[test]
    fn test_encode_simple_query() {
        let bytes = encode_simple_query("SELECT 1");
        assert_eq!(bytes[0], b'Q');
        assert!(bytes.len() > 5);
    }
}
