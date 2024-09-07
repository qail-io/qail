//! QAIL PHP FFI - C-compatible bindings for PHP FFI
//!
//! Exports functions that PHP can call via FFI extension.
//! Provides high-performance query encoding and true pipelining.

// FFI functions check pointers before dereferencing, clippy doesn't understand this pattern
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use once_cell::sync::Lazy;
use qail_core::prelude::*;
use qail_pg::driver::PreparedStatement as PgPreparedStatement;
use qail_pg::protocol::AstEncoder;
use std::ffi::{CStr, c_char};
use std::sync::Mutex as SyncMutex;
use tokio::sync::Mutex as AsyncMutex;

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
// Opaque handle for PHP - wraps PgConnection in an async Mutex for thread safety
pub struct QailConnection {
    inner: AsyncMutex<qail_pg::PgConnection>,
}

// ==================== Prepared Statement Handle ====================
pub struct QailPreparedStatement {
    sql: String,
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
        unsafe {
            *out_len = 0;
        }
        return std::ptr::null_mut();
    }

    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let columns_str = if columns.is_null() {
        "*"
    } else {
        unsafe { CStr::from_ptr(columns).to_str().unwrap_or("*") }
    };

    let mut cmd = Qail::get(table);

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
    unsafe {
        *out_len = len;
    }
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
        unsafe {
            *out_len = 0;
        }
        return std::ptr::null_mut();
    }

    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let columns_str = if columns.is_null() {
        "*"
    } else {
        unsafe { CStr::from_ptr(columns).to_str().unwrap_or("*") }
    };

    let col_exprs: Vec<Expr> = if !columns_str.is_empty() && columns_str != "*" {
        columns_str
            .split(',')
            .map(|c| Expr::Named(c.trim().to_string()))
            .collect()
    } else {
        vec![]
    };

    let mut cmds = Vec::with_capacity(count);
    for i in 0..count {
        let limit = unsafe { *limits.add(i) };
        let mut cmd = Qail::get(table);
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
    unsafe {
        *out_len = len;
    }
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
pub extern "C" fn qail_transpile(qail_text: *const c_char, out_len: *mut usize) -> *mut c_char {
    if qail_text.is_null() {
        unsafe {
            *out_len = 0;
        }
        return std::ptr::null_mut();
    }

    let input = unsafe { CStr::from_ptr(qail_text).to_str().unwrap_or("") };

    match qail_core::parse(input) {
        Ok(cmd) => {
            let sql = cmd.to_sql();
            let len = sql.len();
            let c_str = std::ffi::CString::new(sql).unwrap();
            unsafe {
                *out_len = len;
            }
            c_str.into_raw()
        }
        Err(_) => {
            unsafe {
                *out_len = 0;
            }
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
    let result = RUNTIME
        .block_on(async { qail_pg::PgConnection::connect(host, port, user, database).await });

    match result {
        Ok(conn) => {
            let handle = Box::new(QailConnection {
                inner: AsyncMutex::new(conn),
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
        let mut conn_guard = conn_ref.inner.lock().await;
        conn_guard.prepare(sql_str).await
    });

    match result {
        Ok(_stmt) => {
            let handle = Box::new(QailPreparedStatement {
                sql: sql_str.to_string(),
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
        let mut conn_guard = conn_ref.inner.lock().await;

        // Create PreparedStatement handle for driver using from_sql
        let driver_stmt = PgPreparedStatement::from_sql(&stmt_ref.sql);

        conn_guard
            .pipeline_prepared_fast(&driver_stmt, &params_batch)
            .await
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
        unsafe {
            *out_len = 0;
        }
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
        let mut conn_guard = conn_ref.inner.lock().await;

        let driver_stmt = PgPreparedStatement::from_sql(&stmt_ref.sql);

        conn_guard
            .pipeline_prepared_results(&driver_stmt, &params_batch)
            .await
    });

    match result {
        Ok(results) => {
            // Convert to simple JSON array
            let mut json = String::from("[");
            for (qi, rows) in results.iter().enumerate() {
                if qi > 0 {
                    json.push(',');
                }
                json.push('[');
                for (ri, row) in rows.iter().enumerate() {
                    if ri > 0 {
                        json.push(',');
                    }
                    json.push('[');
                    for (ci, col) in row.iter().enumerate() {
                        if ci > 0 {
                            json.push(',');
                        }
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
            unsafe {
                *out_len = len;
            }
            c_str.into_raw()
        }
        Err(_) => {
            unsafe {
                *out_len = 0;
            }
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
        let mut conn_guard = conn_ref.inner.lock().await;
        let driver_stmt = PgPreparedStatement::from_sql(&stmt_ref.sql);
        conn_guard
            .pipeline_prepared_fast(&driver_stmt, &params_batch)
            .await
    });

    match result {
        Ok(count) => count as i64,
        Err(_) => -1,
    }
}

// ==================== Streaming COPY FFI ====================

/// Handle for streaming COPY operation.
/// Buffers rows in Rust to minimize FFI overhead and memory allocation.
pub struct QailCopyStream {
    conn: *mut QailConnection,
    table: String,
    columns: Vec<String>,
    buffer: SyncMutex<Vec<u8>>,
    row_count: std::sync::atomic::AtomicUsize,
}

/// Start a COPY stream for bulk inserts.
///
/// Returns a handle for streaming rows, or NULL on failure.
/// Call qail_copy_row() to add rows, then qail_copy_end() to commit.
///
/// # Example (PHP)
/// ```php
/// $copy = qail_copy_start($conn, "users", "id,name,email");
/// while ($row = $mysql->fetch()) {
///     qail_copy_row_3($copy, $row['id'], $row['name'], $row['email']);
/// }
/// $count = qail_copy_end($copy);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn qail_copy_start(
    conn: *mut QailConnection,
    table: *const c_char,
    columns: *const c_char,
) -> *mut QailCopyStream {
    if conn.is_null() || table.is_null() || columns.is_null() {
        return std::ptr::null_mut();
    }

    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") }.to_string();
    let columns_str = unsafe { CStr::from_ptr(columns).to_str().unwrap_or("") };
    let columns: Vec<String> = columns_str
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    if table.is_empty() || columns.is_empty() {
        return std::ptr::null_mut();
    }

    let stream = Box::new(QailCopyStream {
        conn,
        table,
        columns,
        buffer: SyncMutex::new(Vec::with_capacity(1024 * 1024)), // 1MB initial buffer
        row_count: std::sync::atomic::AtomicUsize::new(0),
    });

    Box::into_raw(stream)
}

/// Add a row to the COPY stream (3-column version for users table).
///
/// Returns 1 on success, 0 on failure.
#[unsafe(no_mangle)]
pub extern "C" fn qail_copy_row_3(
    stream: *mut QailCopyStream,
    col0: *const c_char,
    col1: *const c_char,
    col2: *const c_char,
) -> i32 {
    if stream.is_null() {
        return 0;
    }

    let stream_ref = unsafe { &*stream };
    let mut buffer = match stream_ref.buffer.lock() {
        Ok(b) => b,
        Err(_) => return 0,
    };

    // Encode values as TSV line
    let v0 = if col0.is_null() {
        "\\N"
    } else {
        unsafe { CStr::from_ptr(col0).to_str().unwrap_or("\\N") }
    };
    let v1 = if col1.is_null() {
        "\\N"
    } else {
        unsafe { CStr::from_ptr(col1).to_str().unwrap_or("\\N") }
    };
    let v2 = if col2.is_null() {
        "\\N"
    } else {
        unsafe { CStr::from_ptr(col2).to_str().unwrap_or("\\N") }
    };

    // Write TSV line: col0\tcol1\tcol2\n
    buffer.extend_from_slice(v0.as_bytes());
    buffer.push(b'\t');
    buffer.extend_from_slice(v1.as_bytes());
    buffer.push(b'\t');
    buffer.extend_from_slice(v2.as_bytes());
    buffer.push(b'\n');

    stream_ref
        .row_count
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    1
}

/// Add a row to the COPY stream (4-column version).
#[unsafe(no_mangle)]
pub extern "C" fn qail_copy_row_4(
    stream: *mut QailCopyStream,
    col0: *const c_char,
    col1: *const c_char,
    col2: *const c_char,
    col3: *const c_char,
) -> i32 {
    if stream.is_null() {
        return 0;
    }

    let stream_ref = unsafe { &*stream };
    let mut buffer = match stream_ref.buffer.lock() {
        Ok(b) => b,
        Err(_) => return 0,
    };

    fn get_val(ptr: *const c_char) -> &'static str {
        if ptr.is_null() {
            "\\N"
        } else {
            unsafe { CStr::from_ptr(ptr).to_str().unwrap_or("\\N") }
        }
    }

    buffer.extend_from_slice(get_val(col0).as_bytes());
    buffer.push(b'\t');
    buffer.extend_from_slice(get_val(col1).as_bytes());
    buffer.push(b'\t');
    buffer.extend_from_slice(get_val(col2).as_bytes());
    buffer.push(b'\t');
    buffer.extend_from_slice(get_val(col3).as_bytes());
    buffer.push(b'\n');

    stream_ref
        .row_count
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    1
}

/// Add a row to the COPY stream (6-column version for orders table).
#[unsafe(no_mangle)]
pub extern "C" fn qail_copy_row_6(
    stream: *mut QailCopyStream,
    col0: *const c_char,
    col1: *const c_char,
    col2: *const c_char,
    col3: *const c_char,
    col4: *const c_char,
    col5: *const c_char,
) -> i32 {
    if stream.is_null() {
        return 0;
    }

    let stream_ref = unsafe { &*stream };
    let mut buffer = match stream_ref.buffer.lock() {
        Ok(b) => b,
        Err(_) => return 0,
    };

    fn get_val(ptr: *const c_char) -> &'static str {
        if ptr.is_null() {
            "\\N"
        } else {
            unsafe { CStr::from_ptr(ptr).to_str().unwrap_or("\\N") }
        }
    }

    buffer.extend_from_slice(get_val(col0).as_bytes());
    buffer.push(b'\t');
    buffer.extend_from_slice(get_val(col1).as_bytes());
    buffer.push(b'\t');
    buffer.extend_from_slice(get_val(col2).as_bytes());
    buffer.push(b'\t');
    buffer.extend_from_slice(get_val(col3).as_bytes());
    buffer.push(b'\t');
    buffer.extend_from_slice(get_val(col4).as_bytes());
    buffer.push(b'\t');
    buffer.extend_from_slice(get_val(col5).as_bytes());
    buffer.push(b'\n');

    stream_ref
        .row_count
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    1
}

/// End the COPY stream and commit to PostgreSQL.
///
/// Returns number of rows inserted, or -1 on failure.
/// Frees the stream handle.
#[unsafe(no_mangle)]
pub extern "C" fn qail_copy_end(stream: *mut QailCopyStream) -> i64 {
    if stream.is_null() {
        return -1;
    }

    // Take ownership of stream
    let stream = unsafe { Box::from_raw(stream) };

    let conn_ref = unsafe { &*stream.conn };
    let buffer = match stream.buffer.lock() {
        Ok(b) => b,
        Err(_) => return -1,
    };

    if buffer.is_empty() {
        return 0;
    }

    // Execute COPY using Rust's async runtime
    let result = RUNTIME.block_on(async {
        let mut conn_guard = conn_ref.inner.lock().await;

        // Use copy_in_raw for maximum speed
        conn_guard
            .copy_in_raw(&stream.table, &stream.columns, &buffer)
            .await
    });

    match result {
        Ok(count) => count as i64,
        Err(_) => -1,
    }
}

/// Cancel and free a COPY stream without committing.
#[unsafe(no_mangle)]
pub extern "C" fn qail_copy_cancel(stream: *mut QailCopyStream) {
    if !stream.is_null() {
        unsafe {
            let _ = Box::from_raw(stream);
        }
    }
}

// ==================== Direct MySQL→PostgreSQL Migration ====================

/// Direct Rust-to-Rust MySQL → PostgreSQL migration.
///
/// Bypasses PHP loop entirely for maximum throughput.
/// Expected: 600K+ rows/s vs 330K rows/s with PHP loop.
///
/// # Arguments
/// * `mysql_host`, `mysql_port`, `mysql_user`, `mysql_pass`, `mysql_db` - MySQL connection
/// * `pg_conn` - Existing PostgreSQL connection from qail_connect()
/// * `sql` - SELECT query to execute on MySQL
/// * `pg_table` - Target PostgreSQL table name
/// * `pg_columns` - Comma-separated column names for COPY
///
/// # Returns
/// Number of rows migrated, or -1 on error.
#[unsafe(no_mangle)]
pub extern "C" fn qail_mysql_to_pg(
    mysql_host: *const c_char,
    mysql_port: u16,
    mysql_user: *const c_char,
    mysql_pass: *const c_char,
    mysql_db: *const c_char,
    pg_conn: *mut QailConnection,
    sql: *const c_char,
    pg_table: *const c_char,
    pg_columns: *const c_char,
) -> i64 {
    if mysql_host.is_null()
        || mysql_user.is_null()
        || mysql_db.is_null()
        || pg_conn.is_null()
        || sql.is_null()
        || pg_table.is_null()
        || pg_columns.is_null()
    {
        return -1;
    }

    let mysql_host = unsafe { CStr::from_ptr(mysql_host).to_str().unwrap_or("127.0.0.1") };
    let mysql_user = unsafe { CStr::from_ptr(mysql_user).to_str().unwrap_or("root") };
    let mysql_pass = if mysql_pass.is_null() {
        ""
    } else {
        unsafe { CStr::from_ptr(mysql_pass).to_str().unwrap_or("") }
    };
    let mysql_db = unsafe { CStr::from_ptr(mysql_db).to_str().unwrap_or("") };
    let sql = unsafe { CStr::from_ptr(sql).to_str().unwrap_or("") };
    let pg_table = unsafe { CStr::from_ptr(pg_table).to_str().unwrap_or("") };
    let pg_columns_str = unsafe { CStr::from_ptr(pg_columns).to_str().unwrap_or("") };

    let pg_columns: Vec<String> = pg_columns_str
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let pg_conn_ref = unsafe { &*pg_conn };

    // Initialize TLS crypto provider
    qail_mysql::init();

    // Execute migration in tokio runtime
    let result = RUNTIME.block_on(async {
        // Connect to MySQL with TLS
        let mut mysql_conn = match qail_mysql::MySqlConnection::connect(
            mysql_host, mysql_port, mysql_user, mysql_pass, mysql_db,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => return Err(format!("MySQL connection failed: {}", e)),
        };

        // Execute query and get TSV data
        let tsv_data = match mysql_conn.query_to_tsv(sql).await {
            Ok(data) => data,
            Err(e) => return Err(format!("MySQL query failed: {}", e)),
        };

        // Write to PostgreSQL using COPY
        let mut pg_guard = pg_conn_ref.inner.lock().await;
        match pg_guard.copy_in_raw(pg_table, &pg_columns, &tsv_data).await {
            Ok(count) => Ok(count),
            Err(e) => Err(format!("PostgreSQL COPY failed: {}", e)),
        }
    });

    match result {
        Ok(count) => count as i64,
        Err(msg) => {
            eprintln!("qail_mysql_to_pg error: {}", msg);
            -1
        }
    }
}
