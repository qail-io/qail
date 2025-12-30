//! QAIL Go FFI - C-compatible bindings for Go CGO
//!
//! Exports sync encoding functions that Go can call via CGO.
//! All I/O is done in Go - Rust only handles AST encoding.

// FFI functions check pointers before dereferencing, clippy doesn't understand this pattern
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use qail_core::prelude::*;
use qail_pg::protocol::AstEncoder;
use std::ffi::{CStr, c_char, c_int};

/// Opaque handle to Qail
pub struct QailHandle {
    cmd: Qail,
}

/// Create a GET command
/// Returns opaque handle, caller must free with qail_free
#[unsafe(no_mangle)]
pub extern "C" fn qail_get(table: *const c_char) -> *mut QailHandle {
    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let cmd = Qail::get(table);
    Box::into_raw(Box::new(QailHandle { cmd }))
}

/// Create an ADD (INSERT) command
#[unsafe(no_mangle)]
pub extern "C" fn qail_add(table: *const c_char) -> *mut QailHandle {
    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let cmd = Qail::add(table);
    Box::into_raw(Box::new(QailHandle { cmd }))
}

/// Create a SET (UPDATE) command
#[unsafe(no_mangle)]
pub extern "C" fn qail_set(table: *const c_char) -> *mut QailHandle {
    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let cmd = Qail::set(table);
    Box::into_raw(Box::new(QailHandle { cmd }))
}

/// Create a DEL (DELETE) command
#[unsafe(no_mangle)]
pub extern "C" fn qail_del(table: *const c_char) -> *mut QailHandle {
    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let cmd = Qail::del(table);
    Box::into_raw(Box::new(QailHandle { cmd }))
}

/// Add column to command
#[unsafe(no_mangle)]
pub extern "C" fn qail_column(handle: *mut QailHandle, col: *const c_char) {
    if handle.is_null() {
        return;
    }
    let col = unsafe { CStr::from_ptr(col).to_str().unwrap_or("") };
    unsafe {
        (*handle).cmd.columns.push(Expr::Named(col.to_string()));
    }
}

/// Add filter condition with int value
#[unsafe(no_mangle)]
pub extern "C" fn qail_filter_int(
    handle: *mut QailHandle,
    col: *const c_char,
    op: c_int,
    value: i64,
) {
    if handle.is_null() {
        return;
    }
    let col = unsafe { CStr::from_ptr(col).to_str().unwrap_or("") };
    let operator = int_to_operator(op);
    unsafe {
        (*handle).cmd = (*handle).cmd.clone().filter(col, operator, value);
    }
}

/// Add filter with string value
#[unsafe(no_mangle)]
pub extern "C" fn qail_filter_str(
    handle: *mut QailHandle,
    col: *const c_char,
    op: c_int,
    value: *const c_char,
) {
    if handle.is_null() {
        return;
    }
    let col = unsafe { CStr::from_ptr(col).to_str().unwrap_or("") };
    let value = unsafe { CStr::from_ptr(value).to_str().unwrap_or("") };
    let operator = int_to_operator(op);
    unsafe {
        (*handle).cmd = (*handle).cmd.clone().filter(col, operator, value);
    }
}

/// Add filter with bool value
#[unsafe(no_mangle)]
pub extern "C" fn qail_filter_bool(
    handle: *mut QailHandle,
    col: *const c_char,
    op: c_int,
    value: c_int,
) {
    if handle.is_null() {
        return;
    }
    let col = unsafe { CStr::from_ptr(col).to_str().unwrap_or("") };
    let operator = int_to_operator(op);
    let bool_val = value != 0;
    unsafe {
        (*handle).cmd = (*handle).cmd.clone().filter(col, operator, bool_val);
    }
}

/// Set LIMIT
#[unsafe(no_mangle)]
pub extern "C" fn qail_limit(handle: *mut QailHandle, limit: i64) {
    if handle.is_null() {
        return;
    }
    unsafe {
        (*handle).cmd = (*handle).cmd.clone().limit(limit);
    }
}

/// Set OFFSET
#[unsafe(no_mangle)]
pub extern "C" fn qail_offset(handle: *mut QailHandle, offset: i64) {
    if handle.is_null() {
        return;
    }
    unsafe {
        (*handle).cmd = (*handle).cmd.clone().offset(offset);
    }
}

/// Encode command to PostgreSQL wire protocol bytes
/// Returns pointer to bytes, sets out_len to length
/// Caller must free with qail_bytes_free
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode(handle: *const QailHandle, out_len: *mut usize) -> *mut u8 {
    if handle.is_null() {
        unsafe {
            *out_len = 0;
        }
        return std::ptr::null_mut();
    }

    let cmd = unsafe { &(*handle).cmd };
    let (wire_bytes, _params) = AstEncoder::encode_cmd(cmd);
    let bytes = wire_bytes.to_vec();

    let len = bytes.len();
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;

    unsafe {
        *out_len = len;
    }
    ptr
}

/// Encode batch of commands to PostgreSQL wire protocol bytes
/// Returns single buffer with all commands encoded
#[unsafe(no_mangle)]
pub extern "C" fn qail_batch_encode(
    handles: *const *const QailHandle,
    count: usize,
    out_len: *mut usize,
) -> *mut u8 {
    if handles.is_null() || count == 0 {
        unsafe {
            *out_len = 0;
        }
        return std::ptr::null_mut();
    }

    // Collect all commands
    let mut cmds = Vec::with_capacity(count);
    for i in 0..count {
        let handle = unsafe { *handles.add(i) };
        if !handle.is_null() {
            let cmd = unsafe { &(*handle).cmd };
            cmds.push(cmd.clone());
        }
    }

    // Encode batch
    let wire_bytes = AstEncoder::encode_batch(&cmds);
    let bytes = wire_bytes.to_vec();

    let len = bytes.len();
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;

    unsafe {
        *out_len = len;
    }
    ptr
}

/// Free command handle
#[unsafe(no_mangle)]
pub extern "C" fn qail_free(handle: *mut QailHandle) {
    if !handle.is_null() {
        unsafe {
            let _ = Box::from_raw(handle);
        }
    }
}

/// Free bytes allocated by encode functions
#[unsafe(no_mangle)]
pub extern "C" fn qail_bytes_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let slice = std::slice::from_raw_parts_mut(ptr, len);
            let _ = Box::from_raw(slice as *mut [u8]);
        }
    }
}

// Operator constants (must match Go side)
const OP_EQ: c_int = 0;
const OP_NE: c_int = 1;
const OP_GT: c_int = 2;
const OP_GTE: c_int = 3;
const OP_LT: c_int = 4;
const OP_LTE: c_int = 5;

fn int_to_operator(op: c_int) -> Operator {
    match op {
        OP_EQ => Operator::Eq,
        OP_NE => Operator::Ne,
        OP_GT => Operator::Gt,
        OP_GTE => Operator::Gte,
        OP_LT => Operator::Lt,
        OP_LTE => Operator::Lte,
        _ => Operator::Eq,
    }
}

// =============================================================================
// OPTIMIZED: Single-call encoding functions (minimize CGO crossings)
// =============================================================================

/// Encode SELECT directly without intermediate handle.
/// ONE CGO call instead of 5+!
///
/// columns: comma-separated column names (e.g., "id,name")
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_select_fast(
    table: *const c_char,
    columns: *const c_char, // comma-separated
    limit: i64,
    out_len: *mut usize,
) -> *mut u8 {
    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let columns_str = unsafe { CStr::from_ptr(columns).to_str().unwrap_or("*") };

    // Build command directly
    let mut cmd = Qail::get(table);

    // Parse comma-separated columns
    if !columns_str.is_empty() && columns_str != "*" {
        for col in columns_str.split(',') {
            cmd.columns.push(Expr::Named(col.trim().to_string()));
        }
    }

    // Add limit
    if limit > 0 {
        cmd = cmd.limit(limit);
    }

    // Encode directly
    let (wire_bytes, _params) = AstEncoder::encode_cmd(&cmd);
    let bytes = wire_bytes.to_vec();

    let len = bytes.len();
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;

    unsafe {
        *out_len = len;
    }
    ptr
}

/// Encode batch of SELECT queries with same structure but different limits.
/// Returns all wire bytes in one buffer.
///
/// ONE CGO call for entire batch!
#[unsafe(no_mangle)]
pub extern "C" fn qail_encode_select_batch_fast(
    table: *const c_char,
    columns: *const c_char, // comma-separated
    limits: *const i64,     // array of limit values
    count: usize,
    out_len: *mut usize,
) -> *mut u8 {
    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let columns_str = unsafe { CStr::from_ptr(columns).to_str().unwrap_or("*") };

    // Pre-parse columns once
    let col_exprs: Vec<Expr> = if !columns_str.is_empty() && columns_str != "*" {
        columns_str
            .split(',')
            .map(|col| Expr::Named(col.trim().to_string()))
            .collect()
    } else {
        vec![]
    };

    // Build all commands
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

    // Encode batch
    let wire_bytes = AstEncoder::encode_batch(&cmds);
    let bytes = wire_bytes.to_vec();

    let len = bytes.len();
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;

    unsafe {
        *out_len = len;
    }
    ptr
}

// =============================================================================
// RUST I/O v2: Channel-based async - NO block_on overhead!
// =============================================================================

use once_cell::sync::Lazy;
use qail_pg::PgConnection;
use std::sync::Mutex;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};

/// Global Tokio runtime - multi-thread for CGO compatibility
static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

/// Command sent to the connection task
enum ConnCmd {
    ExecuteBatch {
        cmds: Vec<Qail>,
        reply: oneshot::Sender<Result<usize, String>>,
    },
    Close,
}

/// Opaque connection handle - now uses channel to spawned task
pub struct ConnHandleV2 {
    tx: mpsc::UnboundedSender<ConnCmd>,
}

/// Connect to PostgreSQL using spawned Tokio task.
/// Connection lives in async task - NO block_on per query!
#[unsafe(no_mangle)]
pub extern "C" fn qail_connect_v2(
    host: *const c_char,
    port: u16,
    user: *const c_char,
    database: *const c_char,
) -> *mut ConnHandleV2 {
    let host = unsafe { CStr::from_ptr(host).to_str().unwrap_or("127.0.0.1") }.to_string();
    let user = unsafe { CStr::from_ptr(user).to_str().unwrap_or("postgres") }.to_string();
    let database = unsafe { CStr::from_ptr(database).to_str().unwrap_or("postgres") }.to_string();

    let (tx, mut rx) = mpsc::unbounded_channel::<ConnCmd>();

    // Spawn connection task that lives for duration of connection
    RUNTIME.spawn(async move {
        let conn_result = PgConnection::connect(&host, port, &user, &database).await;

        let mut conn = match conn_result {
            Ok(c) => c,
            Err(_) => return, // Connection failed, task exits
        };

        // Process commands until Close or channel drops
        while let Some(cmd) = rx.recv().await {
            match cmd {
                ConnCmd::ExecuteBatch { cmds, reply } => {
                    let result = conn.pipeline_ast_fast(&cmds).await;
                    let _ = reply.send(result.map_err(|e| e.to_string()));
                }
                ConnCmd::Close => break,
            }
        }
    });

    // Small delay to let connection establish
    std::thread::sleep(std::time::Duration::from_millis(50));

    Box::into_raw(Box::new(ConnHandleV2 { tx }))
}

/// Execute batch of SELECT queries via async task.
/// Uses oneshot channel - much faster than block_on!
#[unsafe(no_mangle)]
pub extern "C" fn qail_execute_batch_v2(
    conn_handle: *mut ConnHandleV2,
    table: *const c_char,
    columns: *const c_char,
    limits: *const i64,
    count: usize,
) -> i64 {
    if conn_handle.is_null() || count == 0 {
        return -1;
    }

    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let columns_str = unsafe { CStr::from_ptr(columns).to_str().unwrap_or("*") };

    // Pre-parse columns once
    let col_exprs: Vec<Expr> = if !columns_str.is_empty() && columns_str != "*" {
        columns_str
            .split(',')
            .map(|col| Expr::Named(col.trim().to_string()))
            .collect()
    } else {
        vec![]
    };

    // Build all commands
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

    // Send via channel - async task processes it
    let handle = unsafe { &*conn_handle };
    let (reply_tx, reply_rx) = oneshot::channel();

    if handle
        .tx
        .send(ConnCmd::ExecuteBatch {
            cmds,
            reply: reply_tx,
        })
        .is_err()
    {
        return -1;
    }

    // Wait for result via oneshot (this DOES block, but with less overhead)
    match reply_rx.blocking_recv() {
        Ok(Ok(n)) => n as i64,
        _ => -1,
    }
}

/// Close connection v2.
#[unsafe(no_mangle)]
pub extern "C" fn qail_conn_close_v2(handle: *mut ConnHandleV2) {
    if !handle.is_null() {
        let handle = unsafe { Box::from_raw(handle) };
        let _ = handle.tx.send(ConnCmd::Close);
    }
}

// Keep old API for backwards compatibility
/// Opaque connection handle (old block_on version)
pub struct ConnHandle {
    conn: Mutex<Option<PgConnection>>,
}

#[unsafe(no_mangle)]
pub extern "C" fn qail_connect(
    host: *const c_char,
    port: u16,
    user: *const c_char,
    database: *const c_char,
) -> *mut ConnHandle {
    let host = unsafe { CStr::from_ptr(host).to_str().unwrap_or("127.0.0.1") };
    let user = unsafe { CStr::from_ptr(user).to_str().unwrap_or("postgres") };
    let database = unsafe { CStr::from_ptr(database).to_str().unwrap_or("postgres") };

    let result =
        RUNTIME.block_on(async { PgConnection::connect(host, port, user, database).await });

    match result {
        Ok(conn) => Box::into_raw(Box::new(ConnHandle {
            conn: Mutex::new(Some(conn)),
        })),
        Err(_) => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn qail_execute_batch(
    conn_handle: *mut ConnHandle,
    table: *const c_char,
    columns: *const c_char,
    limits: *const i64,
    count: usize,
) -> i64 {
    if conn_handle.is_null() || count == 0 {
        return -1;
    }

    let table = unsafe { CStr::from_ptr(table).to_str().unwrap_or("") };
    let columns_str = unsafe { CStr::from_ptr(columns).to_str().unwrap_or("*") };

    let col_exprs: Vec<Expr> = if !columns_str.is_empty() && columns_str != "*" {
        columns_str
            .split(',')
            .map(|col| Expr::Named(col.trim().to_string()))
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

    let handle = unsafe { &*conn_handle };
    let mut guard = handle.conn.lock().unwrap();

    if let Some(conn) = guard.as_mut() {
        let result = RUNTIME.block_on(async { conn.pipeline_ast_fast(&cmds).await });

        match result {
            Ok(n) => n as i64,
            Err(_) => -1,
        }
    } else {
        -1
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn qail_conn_close(handle: *mut ConnHandle) {
    if !handle.is_null() {
        unsafe {
            let _ = Box::from_raw(handle);
        }
    }
}
