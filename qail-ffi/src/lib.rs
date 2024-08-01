//! QAIL C-API for FFI Bindings
//!
//! Provides a C-compatible interface for using QAIL from Go, PHP, Python, Java, etc.
//!
//! ## Usage
//! ```c
//! #include "qail.h"
//!
//! char* sql = qail_transpile("get::users:'_");
//! printf("%s\n", sql);  // SELECT * FROM users
//! qail_free(sql);
//! ```

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use qail_core::transpiler::{ToSql, Dialect};
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

/// Parse QAIL and return SQL string.
/// Returns NULL on error (check qail_last_error).
/// Caller must free the returned string with qail_free().
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

#[unsafe(no_mangle)]
pub extern "C" fn qail_transpile_with_dialect(qail: *const c_char, dialect: *const c_char) -> *mut c_char {
    clear_error();

    if qail.is_null() {
        set_error("NULL QAIL input".to_string());
        return std::ptr::null_mut();
    }
    if dialect.is_null() {
        set_error("NULL dialect input".to_string());
        return std::ptr::null_mut();
    }

    let qail_str = match unsafe { CStr::from_ptr(qail) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(format!("Invalid UTF-8 in qail string: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    let dialect_str = match unsafe { CStr::from_ptr(dialect) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(format!("Invalid UTF-8 in dialect string: {}", e));
            return std::ptr::null_mut();
        }
    };

    let d = match dialect_str.to_lowercase().as_str() {
        "postgres" | "postgresql" => Dialect::Postgres,
        _ => {
            set_error(format!("Unsupported dialect: {}. Only 'postgres' is supported.", dialect_str));
            return std::ptr::null_mut();
        }
    };

    match qail_core::parse(qail_str) {
        Ok(cmd) => {
            let sql = cmd.to_sql_with_dialect(d);
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

/// Parse QAIL and return AST as JSON string.
/// Returns NULL on error.
/// Caller must free the returned string with qail_free().
#[unsafe(no_mangle)]
pub extern "C" fn qail_parse_json(qail: *const c_char) -> *mut c_char {
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
            match serde_json::to_string(&cmd) {
                Ok(json) => {
                    match CString::new(json) {
                        Ok(c_string) => c_string.into_raw(),
                        Err(e) => {
                            set_error(format!("NUL byte in output: {}", e));
                            std::ptr::null_mut()
                        }
                    }
                }
                Err(e) => {
                    set_error(format!("JSON serialization error: {}", e));
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

/// Get the last error message.
/// Returns NULL if no error.
/// The returned string is valid until the next QAIL function call.
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

/// Free a string returned by QAIL functions.
/// Safe to call with NULL.
#[unsafe(no_mangle)]
pub extern "C" fn qail_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

/// Free bytes returned by qail_encode_* functions.
/// Safe to call with NULL.
#[unsafe(no_mangle)]
pub extern "C" fn qail_free_bytes(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let _ = Vec::from_raw_parts(ptr, len, len);
        }
    }
}

// ============================================================================
// Wire Protocol Encoder (Layer 2 - AST Native)
// ============================================================================

use qail_pg::protocol::AstEncoder;

/// Encode a QAIL GET query to PostgreSQL wire protocol bytes.
/// 
/// Returns pointer to bytes via out_ptr, length via out_len.
/// Returns 0 on success, non-zero on error.
/// Caller must free the bytes with qail_free_bytes(out_ptr, out_len).
///
/// # Example (C):
/// ```c
/// uint8_t* bytes;
/// size_t len;
/// int rc = qail_encode_get("users", "id,name", 10, &bytes, &len);
/// if (rc == 0) {
///     // send bytes to PostgreSQL
///     qail_free_bytes(bytes, len);
/// }
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_get(
    table: *const c_char,
    columns: *const c_char,  // comma-separated, or "*" for all
    limit: i64,              // -1 for no limit
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    clear_error();
    
    // Validate inputs
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
    
    // Encode to wire bytes
    let (wire_bytes, _) = AstEncoder::encode_cmd(&cmd);
    let bytes_vec = wire_bytes.to_vec();
    let len = bytes_vec.len();
    
    // Transfer ownership to caller
    let mut boxed = bytes_vec.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    
    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }
    
    0 // Success
}

/// Encode a batch of GET queries to wire protocol bytes.
/// 
/// Encodes multiple queries for pipeline execution (single round-trip).
/// Each query is: table, columns (comma-sep), limit.
/// 
/// # Parameters
/// - tables: null-terminated array of table names
/// - columns_arr: null-terminated array of column specs (comma-sep or "*")
/// - limits: array of limits (-1 for no limit)
/// - count: number of queries
/// - out_ptr: receives pointer to encoded bytes
/// - out_len: receives length of bytes
///
/// # Example (C):
/// ```c
/// const char* tables[] = {"users", "orders", NULL};
/// const char* cols[] = {"id,name", "*", NULL};
/// int64_t limits[] = {10, 5};
/// uint8_t* bytes;
/// size_t len;
/// qail_encode_batch_get(tables, cols, limits, 2, &bytes, &len);
/// // send bytes to PostgreSQL
/// qail_free_bytes(bytes, len);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_batch_get(
    tables: *const *const c_char,
    columns_arr: *const *const c_char,
    limits: *const i64,
    count: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    clear_error();
    
    if tables.is_null() || columns_arr.is_null() || limits.is_null() 
        || out_ptr.is_null() || out_len.is_null() || count == 0 {
        set_error("NULL pointer or zero count".to_string());
        return -1;
    }
    
    let mut cmds = Vec::with_capacity(count);
    
    for i in 0..count {
        let table_ptr = unsafe { *tables.add(i) };
        let cols_ptr = unsafe { *columns_arr.add(i) };
        let limit = unsafe { *limits.add(i) };
        
        if table_ptr.is_null() {
            set_error(format!("NULL table at index {}", i));
            return -2;
        }
        
        let table_str = match unsafe { CStr::from_ptr(table_ptr) }.to_str() {
            Ok(s) => s,
            Err(_) => {
                set_error(format!("Invalid UTF-8 in table at index {}", i));
                return -3;
            }
        };
        
        let mut cmd = qail_core::ast::QailCmd::get(table_str);
        
        // Parse columns
        if !cols_ptr.is_null() {
            if let Ok(cols_str) = unsafe { CStr::from_ptr(cols_ptr) }.to_str() {
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
            }
        } else {
            cmd = cmd.select_all();
        }
        
        if limit >= 0 {
            cmd = cmd.limit(limit);
        }
        
        cmds.push(cmd);
    }
    
    // Encode batch
    let wire_bytes = AstEncoder::encode_batch(&cmds);
    let bytes_vec = wire_bytes.to_vec();
    let len = bytes_vec.len();
    
    // Transfer ownership
    let mut boxed = bytes_vec.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    
    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }
    
    0
}

/// Encode a UNIFORM batch of identical GET queries.
/// 
/// This is the HIGH-PERFORMANCE path: encode ONCE, execute MANY times.
/// All queries in the batch are identical (same table, columns, limit).
/// 
/// # Parameters
/// - table: Table name
/// - columns: Columns (comma-separated or "*")
/// - limit: Row limit (-1 for no limit)
/// - count: Number of queries in batch
/// - out_ptr: Receives pointer to encoded bytes
/// - out_len: Receives byte length
///
/// # Usage Pattern (Python):
/// ```python
/// # Encode ONCE at startup
/// batch_bytes = qail_encode_uniform_batch("harbors", "id,name", 10, 10000)
///
/// # Execute MANY times in hot loop
/// for _ in range(5000):
///     writer.write(batch_bytes)  # Same bytes, no FFI call!
///     await read_responses()
/// ```
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
    
    // Clone for batch - all identical
    let cmds: Vec<_> = (0..count).map(|_| base_cmd.clone()).collect();
    
    // Encode batch
    let wire_bytes = AstEncoder::encode_batch(&cmds);
    let bytes_vec = wire_bytes.to_vec();
    let len = bytes_vec.len();
    
    // Transfer ownership
    let mut boxed = bytes_vec.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    
    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }
    
    0
}

/// Get QAIL version string.
/// Caller must free the returned string with qail_free().
#[unsafe(no_mangle)]
pub extern "C" fn qail_version() -> *mut c_char {
    let version = env!("CARGO_PKG_VERSION");
    CString::new(version)
        .map(|s| s.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use qail_core::ast::{QailCmd, Expr};
    use qail_core::transpiler::ToSql;

    // Test via AST construction - immune to syntax changes
    #[test]
    fn test_ast_transpile() {
        let mut cmd = QailCmd::get("users");
        cmd.columns.push(Expr::Star);
        let sql = cmd.to_sql();
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM users"));
    }

    #[test]
    fn test_ffi_transpile() {
        // Use simple v2 syntax that's stable
        let input = CString::new("get users fields *").unwrap();
        let result = qail_transpile(input.as_ptr());
        assert!(!result.is_null(), "transpile returned null, check qail_last_error");
        
        let sql = unsafe { CStr::from_ptr(result) }.to_str().unwrap();
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM users"));
        
        qail_free(result);
    }

    #[test]
    fn test_validate() {
        let valid = CString::new("get users fields *").unwrap();
        assert_eq!(qail_validate(valid.as_ptr()), 1);
        
        let invalid = CString::new("invalid syntax!!!").unwrap();
        assert_eq!(qail_validate(invalid.as_ptr()), 0);
    }

    #[test]
    fn test_null_input() {
        let result = qail_transpile(std::ptr::null());
        assert!(result.is_null());
    }
}
