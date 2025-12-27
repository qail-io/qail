//! QAIL PHP FFI - C-compatible bindings for PHP FFI
//!
//! Exports functions that PHP can call via FFI extension.
//! Provides high-performance query encoding and true pipelining.

use std::ffi::{CStr, c_char};
use std::sync::Mutex;
use qail_core::prelude::*;
use qail_pg::protocol::AstEncoder;
use qail_pg::driver::PreparedStatement as PgPreparedStatement;
use once_cell::sync::Lazy;

// ==================== Tokio Runtime ====================
// Global runtime for async operations from synchronous FFI
static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .build()
        .expect("Failed to create tokio runtime")
});

// ==================== Connection Handle ====================
// Opaque handle for PHP - wraps PgConnection in a Mutex for thread safety
pub struct QailConnection {
    inner: Mutex<qail_pg::PgConnection>,
}

// ==================== Prepared Statement Handle ====================
pub struct QailPreparedStatement {
    name: String,
    sql: String,
    param_count: usize,
}

// ==================== Encoding Functions (existing) ====================

/// Encode a SELECT query to PostgreSQL wire protocol bytes.
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
    
    let mut cmd = QailCmd::get(table);
    
    if !columns_str.is_empty() && columns_str != "*" {
        cmd.columns = columns_str
            .split(',')
            .map(|c| Expr::Named(c.trim().to_string()))
            .collect();
    }
    
    if limit > 0 {
        cmd = cmd.limit(limit);
    }
    
    let (wire_bytes, _params) = AstEncoder::encode_cmd(&cmd);
    let bytes = wire_bytes.to_vec();
    
    let len = bytes.len();
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;
    unsafe { *out_len = len; }
    ptr
}

/// Encode a batch of SELECT queries with different limits.
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
    
    let col_exprs: Vec<Expr> = if !columns_str.is_empty() && columns_str != "*" {
        columns_str.split(',')
            .map(|c| Expr::Named(c.trim().to_string()))
            .collect()
    } else {
        vec![]
    };
    
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
    
    let batch_bytes = AstEncoder::encode_batch(&cmds);
    let bytes = batch_bytes.to_vec();
    
    let len = bytes.len();
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;
    unsafe { *out_len = len; }
    ptr
}

/// Free bytes allocated by qail functions.
#[unsafe(no_mangle)]
pub extern "C" fn qail_bytes_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len));
        }
    }
}

/// Get QAIL version string.
#[unsafe(no_mangle)]
pub extern "C" fn qail_version() -> *const c_char {
    static VERSION: &[u8] = b"0.10.2\0";
    VERSION.as_ptr() as *const c_char
}

/// Transpile QAIL text to SQL.
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

// ==================== Connection Functions (NEW) ====================

/// Connect to PostgreSQL and return a connection handle.
/// 
/// Returns NULL on connection failure.
/// Caller must call qail_disconnect() to free the connection.
#[unsafe(no_mangle)]
pub extern "C" fn qail_connect(
    host: *const c_char,
    port: u16,
    user: *const c_char,
    database: *const c_char,
) -> *mut QailConnection {
    if host.is_null() || user.is_null() || database.is_null() {
        return std::ptr::null_mut();
    }
    
    let host = unsafe { CStr::from_ptr(host).to_str().unwrap_or("127.0.0.1") };
    let user = unsafe { CStr::from_ptr(user).to_str().unwrap_or("postgres") };
    let database = unsafe { CStr::from_ptr(database).to_str().unwrap_or("postgres") };
    
    // Connect using tokio runtime
    let result = RUNTIME.block_on(async {
        qail_pg::PgConnection::connect(host, port, user, database).await
    });
    
    match result {
        Ok(conn) => {
            let handle = Box::new(QailConnection {
                inner: Mutex::new(conn),
            });
            Box::into_raw(handle)
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Disconnect and free a connection handle.
#[unsafe(no_mangle)]
pub extern "C" fn qail_disconnect(conn: *mut QailConnection) {
    if !conn.is_null() {
        unsafe {
            let _ = Box::from_raw(conn);
        }
    }
}

/// Prepare a SQL statement for pipelined execution.
/// 
/// Returns NULL on failure.
/// Caller must call qail_prepared_free() to free the handle.
#[unsafe(no_mangle)]
pub extern "C" fn qail_prepare(
    conn: *mut QailConnection,
    sql: *const c_char,
) -> *mut QailPreparedStatement {
    if conn.is_null() || sql.is_null() {
        return std::ptr::null_mut();
    }
    
    let sql_str = unsafe { CStr::from_ptr(sql).to_str().unwrap_or("") };
    let conn_ref = unsafe { &*conn };
    
    let result = RUNTIME.block_on(async {
        let mut conn_guard = conn_ref.inner.lock().unwrap();
        conn_guard.prepare(sql_str).await
    });
    
    match result {
        Ok(stmt) => {
            // Use public accessors - param_count from SQL $ placeholders
            let param_count = sql_str.matches('$').count();
            let handle = Box::new(QailPreparedStatement {
                name: stmt.name().to_string(),
                sql: sql_str.to_string(),
                param_count,
            });
            Box::into_raw(handle)
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Free a prepared statement handle.
#[unsafe(no_mangle)]
pub extern "C" fn qail_prepared_free(stmt: *mut QailPreparedStatement) {
    if !stmt.is_null() {
        unsafe {
            let _ = Box::from_raw(stmt);
        }
    }
}

/// Execute a prepared statement N times with different parameters.
/// 
/// TRUE PIPELINING: All queries sent in ONE network packet,
/// all responses read in ONE round-trip.
/// 
/// # Arguments
/// * `conn` - Connection handle from qail_connect()
/// * `stmt` - Prepared statement from qail_prepare()
/// * `params` - Array of null-terminated C strings (one per query)
/// * `count` - Number of queries to execute
/// 
/// # Returns
/// Number of queries completed, or -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn qail_pipeline_exec(
    conn: *mut QailConnection,
    stmt: *mut QailPreparedStatement,
    params: *const *const c_char,
    count: usize,
) -> i64 {
    if conn.is_null() || stmt.is_null() || count == 0 {
        return -1;
    }
    
    let conn_ref = unsafe { &*conn };
    let stmt_ref = unsafe { &*stmt };
    
    // Build params batch
    let mut params_batch: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(count);
    for i in 0..count {
        let param_ptr = unsafe { *params.add(i) };
        if param_ptr.is_null() {
            params_batch.push(vec![None]);
        } else {
            let param_str = unsafe { CStr::from_ptr(param_ptr).to_bytes().to_vec() };
            params_batch.push(vec![Some(param_str)]);
        }
    }
    
    // Execute pipeline
    let result = RUNTIME.block_on(async {
        let mut conn_guard = conn_ref.inner.lock().unwrap();
        
        // Create PreparedStatement handle for driver using from_sql
        let driver_stmt = PgPreparedStatement::from_sql(&stmt_ref.sql);
        
        conn_guard.pipeline_prepared_fast(&driver_stmt, &params_batch).await
    });
    
    match result {
        Ok(count) => count as i64,
        Err(_) => -1,
    }
}

/// Execute pipeline and return results as JSON.
/// 
/// Returns pointer to JSON string with all rows.
/// Caller must call qail_string_free() to free.
#[unsafe(no_mangle)]
pub extern "C" fn qail_pipeline_exec_json(
    conn: *mut QailConnection,
    stmt: *mut QailPreparedStatement,
    params: *const *const c_char,
    count: usize,
    out_len: *mut usize,
) -> *mut c_char {
    if conn.is_null() || stmt.is_null() || count == 0 {
        unsafe { *out_len = 0; }
        return std::ptr::null_mut();
    }
    
    let conn_ref = unsafe { &*conn };
    let stmt_ref = unsafe { &*stmt };
    
    // Build params batch
    let mut params_batch: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(count);
    for i in 0..count {
        let param_ptr = unsafe { *params.add(i) };
        if param_ptr.is_null() {
            params_batch.push(vec![None]);
        } else {
            let param_str = unsafe { CStr::from_ptr(param_ptr).to_bytes().to_vec() };
            params_batch.push(vec![Some(param_str)]);
        }
    }
    
    // Execute pipeline with results
    let result = RUNTIME.block_on(async {
        let mut conn_guard = conn_ref.inner.lock().unwrap();
        
        let driver_stmt = PgPreparedStatement::from_sql(&stmt_ref.sql);
        
        conn_guard.pipeline_prepared_results(&driver_stmt, &params_batch).await
    });
    
    match result {
        Ok(results) => {
            // Convert to simple JSON array
            let mut json = String::from("[");
            for (qi, rows) in results.iter().enumerate() {
                if qi > 0 { json.push(','); }
                json.push('[');
                for (ri, row) in rows.iter().enumerate() {
                    if ri > 0 { json.push(','); }
                    json.push('[');
                    for (ci, col) in row.iter().enumerate() {
                        if ci > 0 { json.push(','); }
                        match col {
                            Some(data) => {
                                let s = String::from_utf8_lossy(data);
                                json.push('"');
                                json.push_str(&s.replace('"', "\\\""));
                                json.push('"');
                            }
                            None => json.push_str("null"),
                        }
                    }
                    json.push(']');
                }
                json.push(']');
            }
            json.push(']');
            
            let len = json.len();
            let c_str = std::ffi::CString::new(json).unwrap();
            unsafe { *out_len = len; }
            c_str.into_raw()
        }
        Err(_) => {
            unsafe { *out_len = 0; }
            std::ptr::null_mut()
        }
    }
}

/// Simplified pipeline execution - takes limit values as int64 array.
/// 
/// This is easier to call from PHP than passing char** arrays.
/// 
/// # Arguments
/// * `conn` - Connection handle from qail_connect()
/// * `stmt` - Prepared statement from qail_prepare()
/// * `limits` - Array of i64 limit values (one per query)
/// * `count` - Number of queries to execute
/// 
/// # Returns
/// Number of queries completed, or -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn qail_pipeline_exec_limits(
    conn: *mut QailConnection,
    stmt: *mut QailPreparedStatement,
    limits: *const i64,
    count: usize,
) -> i64 {
    if conn.is_null() || stmt.is_null() || count == 0 || limits.is_null() {
        return -1;
    }
    
    let conn_ref = unsafe { &*conn };
    let stmt_ref = unsafe { &*stmt };
    
    // Build params batch from limits
    let mut params_batch: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(count);
    for i in 0..count {
        let limit = unsafe { *limits.add(i) };
        // Convert i64 to string bytes
        let param_str = limit.to_string().into_bytes();
        params_batch.push(vec![Some(param_str)]);
    }
    
    // Execute pipeline
    let result = RUNTIME.block_on(async {
        let mut conn_guard = conn_ref.inner.lock().unwrap();
        let driver_stmt = PgPreparedStatement::from_sql(&stmt_ref.sql);
        conn_guard.pipeline_prepared_fast(&driver_stmt, &params_batch).await
    });
    
    match result {
        Ok(count) => count as i64,
        Err(_) => -1,
    }
}
