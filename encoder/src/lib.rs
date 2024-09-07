//! QAIL Encoder - Lightweight wire protocol encoding
//!
//! This crate provides:
//! - AST to PostgreSQL wire protocol encoding
//! - QAIL text to SQL transpilation
//! - C FFI for language bindings
//!
//! NO I/O, NO TLS, NO async - just pure encoding.
//! Languages handle their own I/O (Zig, Go, etc.)

// FFI functions check pointers before dereferencing, clippy doesn't understand this pattern
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use qail_core::transpiler::ToSql;
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
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
            if qail_core::parse(s).is_ok() {
                1
            } else {
                0
            }
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
    columns: *const c_char, // comma-separated, or "*" for all
    limit: i64,             // -1 for no limit
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

    // Build Qail
    let mut cmd = qail_core::ast::Qail::get(table_str);

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
    let mut base_cmd = qail_core::ast::Qail::get(table_str);

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
        static ERROR_CSTRING: RefCell<Option<CString>> = const { RefCell::new(None) };
    }

    LAST_ERROR.with(|e| {
        let error = e.borrow();
        match &*error {
            Some(msg) => ERROR_CSTRING.with(|ec| {
                let c_str = CString::new(msg.clone()).unwrap_or_default();
                let ptr = c_str.as_ptr();
                *ec.borrow_mut() = Some(c_str);
                ptr
            }),
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
    buf.push(b'Q'); // Message type
    buf.extend_from_slice(&(msg_len as i32).to_be_bytes()); // Length (big-endian)
    buf.extend_from_slice(sql_bytes); // Query
    buf.push(0); // Null terminator

    buf
}

// ============================================================================
// Extended Query Protocol (Prepared Statements)
// ============================================================================

/// Encode a Parse message to prepare a statement.
///
/// # Arguments
/// * `name` - Statement name (use "" for unnamed)
/// * `sql` - SQL with $1, $2, etc placeholders
/// * `out_ptr` - Output pointer for allocated bytes
/// * `out_len` - Output length
///
/// Returns 0 on success.
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_parse(
    name: *const c_char,
    sql: *const c_char,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    clear_error();

    if sql.is_null() || out_ptr.is_null() || out_len.is_null() {
        set_error("NULL pointer argument".to_string());
        return -1;
    }

    let name_str = if name.is_null() {
        ""
    } else {
        unsafe { CStr::from_ptr(name) }.to_str().unwrap_or_default()
    };

    let sql_str = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(format!("Invalid UTF-8 in SQL: {}", e));
            return -2;
        }
    };

    let wire_bytes = encode_parse_message(name_str, sql_str);
    let len = wire_bytes.len();

    let mut boxed = wire_bytes.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }

    0
}

/// Encode a Sync message.
/// Used after Parse to wait for ParseComplete.
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_sync(out_ptr: *mut *mut u8, out_len: *mut usize) -> i32 {
    if out_ptr.is_null() || out_len.is_null() {
        return -1;
    }

    let wire_bytes = vec![b'S', 0, 0, 0, 4];
    let len = wire_bytes.len();

    let mut boxed = wire_bytes.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }

    0
}

/// Encode a batch of Bind + Execute pairs for pipeline mode.
/// This is the hot path for prepared statement performance.
///
/// # Arguments
/// * `statement` - Prepared statement name
/// * `params` - Array of parameter strings (all queries use same single param)
/// * `count` - Number of Bind+Execute pairs to generate
/// * `out_ptr` - Output pointer for allocated bytes
/// * `out_len` - Output length
///
/// Each query in batch uses params[i % params_count] as its parameter.
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_bind_execute_batch(
    statement: *const c_char,
    params: *const *const c_char, // Array of param strings
    params_count: usize,
    count: usize, // Number of Bind+Execute pairs
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    clear_error();

    if statement.is_null() || out_ptr.is_null() || out_len.is_null() || count == 0 {
        set_error("NULL pointer or zero count".to_string());
        return -1;
    }

    let stmt_str = unsafe { CStr::from_ptr(statement) }
        .to_str()
        .unwrap_or_default();

    // Collect params
    let param_strs: Vec<&str> = if params.is_null() || params_count == 0 {
        vec![]
    } else {
        (0..params_count)
            .filter_map(|i| {
                let p = unsafe { *params.add(i) };
                if p.is_null() {
                    None
                } else {
                    unsafe { CStr::from_ptr(p) }.to_str().ok()
                }
            })
            .collect()
    };

    // Pre-calculate size: each Bind+Execute is ~30 bytes + param data
    let avg_param_len = if param_strs.is_empty() {
        2
    } else {
        param_strs.iter().map(|s| s.len()).sum::<usize>() / param_strs.len()
    };
    let estimated_size = count * (30 + stmt_str.len() + avg_param_len);
    let mut buf = Vec::with_capacity(estimated_size);

    for i in 0..count {
        // Get param for this query
        let param = if param_strs.is_empty() {
            None
        } else {
            Some(param_strs[i % param_strs.len()])
        };

        // Encode Bind
        encode_bind_to_buf(&mut buf, stmt_str, param);

        // Encode Execute
        buf.extend_from_slice(&[b'E', 0, 0, 0, 9, 0, 0, 0, 0, 0]);
    }

    // Add Sync at end
    buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);

    let len = buf.len();
    let mut boxed = buf.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    unsafe {
        *out_ptr = ptr;
        *out_len = len;
    }

    0
}

// ============================================================================
// Internal: Extended Query Message Helpers
// ============================================================================

/// Encode Parse message.
/// Format: 'P' + len + name + sql + param_count
fn encode_parse_message(name: &str, sql: &str) -> Vec<u8> {
    let content_len = name.len() + 1 + sql.len() + 1 + 2; // name\0 + sql\0 + param_count
    let total_len = 1 + 4 + content_len;

    let mut buf = Vec::with_capacity(total_len);
    buf.push(b'P');
    buf.extend_from_slice(&((content_len + 4) as i32).to_be_bytes());
    buf.extend_from_slice(name.as_bytes());
    buf.push(0);
    buf.extend_from_slice(sql.as_bytes());
    buf.push(0);
    buf.extend_from_slice(&0i16.to_be_bytes()); // No param types (infer)

    buf
}

/// Encode Bind message directly into buffer.
/// Format: 'B' + len + portal\0 + statement\0 + formats + params + result_formats
fn encode_bind_to_buf(buf: &mut Vec<u8>, statement: &str, param: Option<&str>) {
    let param_bytes = param.map(|s| s.as_bytes());
    let param_len = param_bytes.map_or(0, |b| b.len());

    // Content: portal(1) + statement(len+1) + format_codes(2) + param_count(2)
    //          + param_len(4) + param_data + result_format(2)
    let content_len = 1 + statement.len() + 1 + 2 + 2 + 4 + param_len + 2;

    buf.push(b'B');
    buf.extend_from_slice(&((content_len + 4) as i32).to_be_bytes());
    buf.push(0); // Unnamed portal
    buf.extend_from_slice(statement.as_bytes());
    buf.push(0);
    buf.extend_from_slice(&0i16.to_be_bytes()); // Format codes (text)
    buf.extend_from_slice(&1i16.to_be_bytes()); // 1 parameter

    // Parameter
    if let Some(data) = param_bytes {
        buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
        buf.extend_from_slice(data);
    } else {
        buf.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
    }

    buf.extend_from_slice(&0i16.to_be_bytes()); // Result format (text)
}

// ============================================================================
// Response Parsing (for fair comparison with pg.zig)
// Enabled only with the "response" feature to keep library size small
// ============================================================================

#[cfg(feature = "response")]
use qail_pg::protocol::wire::BackendMessage;

#[cfg(feature = "response")]
/// Opaque handle to decoded response
pub struct QailResponse {
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub affected_rows: u64,
    pub error: Option<String>,
}

#[cfg(feature = "response")]

/// Decode PostgreSQL response bytes.
/// Returns a handle that must be freed with qail_response_free.
#[unsafe(no_mangle)]
pub extern "C" fn qail_decode_response(
    data: *const u8,
    len: usize,
    out_handle: *mut *mut QailResponse,
) -> i32 {
    clear_error();

    if data.is_null() || out_handle.is_null() {
        set_error("Null pointer".to_string());
        return -1;
    }

    let bytes = unsafe { std::slice::from_raw_parts(data, len) };
    let mut response = QailResponse {
        rows: Vec::new(),
        affected_rows: 0,
        error: None,
    };

    let mut offset = 0;
    while offset < bytes.len() {
        match BackendMessage::decode(&bytes[offset..]) {
            Ok((msg, consumed)) => {
                offset += consumed;

                match msg {
                    BackendMessage::DataRow(columns) => {
                        response.rows.push(columns);
                    }
                    BackendMessage::CommandComplete(tag) => {
                        // Parse affected rows from tag like "INSERT 0 5" or "UPDATE 10"
                        if let Some(num) = tag.split_whitespace().last() {
                            response.affected_rows = num.parse().unwrap_or(0);
                        }
                    }
                    BackendMessage::ErrorResponse(fields) => {
                        response.error = Some(if fields.message.is_empty() {
                            "Unknown error".to_string()
                        } else {
                            fields.message
                        });
                    }
                    BackendMessage::ReadyForQuery(_) => {
                        break; // Done
                    }
                    _ => {} // Skip other messages
                }
            }
            Err(e) => {
                // Not enough data yet, or parse error
                if e.contains("not enough") || e.contains("Need") {
                    break;
                }
                set_error(e);
                return -1;
            }
        }
    }

    let boxed = Box::new(response);
    unsafe { *out_handle = Box::into_raw(boxed) };
    0
}

#[cfg(feature = "response")]
/// Get number of rows in response.
#[unsafe(no_mangle)]
pub extern "C" fn qail_response_row_count(handle: *const QailResponse) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (&*handle).rows.len() }
}

#[cfg(feature = "response")]
/// Get number of columns in a row.
#[unsafe(no_mangle)]
pub extern "C" fn qail_response_column_count(handle: *const QailResponse, row: usize) -> usize {
    if handle.is_null() {
        return 0;
    }
    unsafe { (&*handle).rows.get(row).map(|r| r.len()).unwrap_or(0) }
}

#[cfg(feature = "response")]
/// Get affected row count.
#[unsafe(no_mangle)]
pub extern "C" fn qail_response_affected_rows(handle: *const QailResponse) -> u64 {
    if handle.is_null() {
        return 0;
    }
    unsafe { (&*handle).affected_rows }
}

#[cfg(feature = "response")]
/// Check if a column is NULL.
#[unsafe(no_mangle)]
pub extern "C" fn qail_response_is_null(
    handle: *const QailResponse,
    row: usize,
    col: usize,
) -> i32 {
    if handle.is_null() {
        return 1;
    }
    unsafe {
        let resp = &*handle;
        resp.rows
            .get(row)
            .and_then(|r| r.get(col))
            .map(|c| if c.is_none() { 1 } else { 0 })
            .unwrap_or(1)
    }
}

#[cfg(feature = "response")]
/// Get column value as string.
/// Returns pointer to null-terminated string, or NULL if value is NULL.
/// The returned string is only valid until the response is freed.
#[unsafe(no_mangle)]
pub extern "C" fn qail_response_get_string(
    handle: *const QailResponse,
    row: usize,
    col: usize,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    if handle.is_null() || out_ptr.is_null() || out_len.is_null() {
        return -1;
    }

    unsafe {
        let resp = &*handle;
        if let Some(Some(bytes)) = resp.rows.get(row).and_then(|r| r.get(col)) {
            *out_ptr = bytes.as_ptr();
            *out_len = bytes.len();
            0
        } else {
            *out_ptr = std::ptr::null();
            *out_len = 0;
            0 // NULL is not an error
        }
    }
}

#[cfg(feature = "response")]
/// Get column value as i32.
#[unsafe(no_mangle)]
pub extern "C" fn qail_response_get_i32(
    handle: *const QailResponse,
    row: usize,
    col: usize,
    out_value: *mut i32,
) -> i32 {
    if handle.is_null() || out_value.is_null() {
        return -1;
    }

    unsafe {
        let resp = &*handle;
        if let Some(Some(bytes)) = resp.rows.get(row).and_then(|r| r.get(col)) {
            if let Ok(s) = std::str::from_utf8(bytes) {
                if let Ok(v) = s.parse::<i32>() {
                    *out_value = v;
                    return 0;
                }
            }
        }
        -1
    }
}

#[cfg(feature = "response")]
/// Get column value as i64.
#[unsafe(no_mangle)]
pub extern "C" fn qail_response_get_i64(
    handle: *const QailResponse,
    row: usize,
    col: usize,
    out_value: *mut i64,
) -> i32 {
    if handle.is_null() || out_value.is_null() {
        return -1;
    }

    unsafe {
        let resp = &*handle;
        if let Some(Some(bytes)) = resp.rows.get(row).and_then(|r| r.get(col)) {
            if let Ok(s) = std::str::from_utf8(bytes) {
                if let Ok(v) = s.parse::<i64>() {
                    *out_value = v;
                    return 0;
                }
            }
        }
        -1
    }
}

#[cfg(feature = "response")]
/// Get column value as f64.
#[unsafe(no_mangle)]
pub extern "C" fn qail_response_get_f64(
    handle: *const QailResponse,
    row: usize,
    col: usize,
    out_value: *mut f64,
) -> i32 {
    if handle.is_null() || out_value.is_null() {
        return -1;
    }

    unsafe {
        let resp = &*handle;
        if let Some(Some(bytes)) = resp.rows.get(row).and_then(|r| r.get(col)) {
            if let Ok(s) = std::str::from_utf8(bytes) {
                if let Ok(v) = s.parse::<f64>() {
                    *out_value = v;
                    return 0;
                }
            }
        }
        -1
    }
}

#[cfg(feature = "response")]
/// Get column value as bool.
#[unsafe(no_mangle)]
pub extern "C" fn qail_response_get_bool(
    handle: *const QailResponse,
    row: usize,
    col: usize,
    out_value: *mut i32,
) -> i32 {
    if handle.is_null() || out_value.is_null() {
        return -1;
    }

    unsafe {
        let resp = &*handle;
        if let Some(Some(bytes)) = resp.rows.get(row).and_then(|r| r.get(col)) {
            if let Ok(s) = std::str::from_utf8(bytes) {
                *out_value = match s {
                    "t" | "true" | "1" => 1,
                    "f" | "false" | "0" => 0,
                    _ => return -1,
                };
                return 0;
            }
        }
        -1
    }
}

#[cfg(feature = "response")]
/// Free a response handle.
#[unsafe(no_mangle)]
pub extern "C" fn qail_response_free(handle: *mut QailResponse) {
    if !handle.is_null() {
        unsafe {
            let _ = Box::from_raw(handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_encode_parse_message() {
        let bytes = encode_parse_message("stmt1", "SELECT $1");
        assert_eq!(bytes[0], b'P');
        assert!(bytes.len() > 10);
    }
}
