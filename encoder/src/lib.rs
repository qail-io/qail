//! QAIL Encoder — C FFI for QAIL wire protocol encoding
//!
//! Pure encoding library: **no I/O, no TLS, no async**.
//! Languages bring their own transport layer (Go, Swift, C, etc.)
//! This crate intentionally does not expose authentication, SSO, Kerberos/GSS,
//! TLS, socket, or connection-management ABI. Enterprise authentication stays
//! in the Rust PostgreSQL driver/provider layer; this FFI surface only encodes
//! and decodes protocol/query data.
//!
//! ## Features
//!
//! - **Transpiler** — QAIL text → SQL string (`qail_transpile`, `qail_validate`)
//! - **Simple Query** — AST → PostgreSQL `'Q'` message bytes (`qail_encode_get`)
//! - **Extended Query Protocol** — `Parse`/`Bind`/`Execute`/`Sync` message encoding
//! - **Pipeline batching** — uniform batch + Bind/Execute batch for prepared statements
//! - **Response parsing** — decode `DataRow`, `CommandComplete`, `ErrorResponse` (feature-gated: `response`)
//!
//! ## Safety
//!
//! - All FFI functions are panic-safe via `ffi_catch!` (catches unwind, sets thread-local error)
//! - Null pointer checks on every public function
//! - Caller-owned memory with explicit `qail_free` / `qail_free_bytes` deallocation
//! - Thread-local error reporting via `qail_last_error()`

use qail_core::transpiler::ToSql;
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic;

const MAX_FFI_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Helper: wrap an FFI body in catch_unwind and return a default on panic.
/// Also sets the thread-local error so callers can inspect via qail_last_error().
macro_rules! ffi_catch {
    ($default:expr, $body:expr) => {
        match panic::catch_unwind(panic::AssertUnwindSafe(|| $body)) {
            Ok(result) => result,
            Err(_) => {
                set_error("Internal panic in QAIL encoder".to_string());
                $default
            }
        }
    };
}

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

fn checked_batch_capacity(unit_len: usize, count: usize, label: &str) -> Result<usize, String> {
    let total = unit_len
        .checked_mul(count)
        .ok_or_else(|| format!("{label} batch size overflow"))?;
    if total > MAX_FFI_BATCH_BYTES {
        return Err(format!(
            "{label} batch too large: {total} bytes (max {MAX_FFI_BATCH_BYTES})"
        ));
    }
    Ok(total)
}

fn checked_bind_execute_pair_len(
    statement_len: usize,
    param_len: Option<usize>,
) -> Result<usize, String> {
    let param_wire_len = match param_len {
        Some(len) => 4usize
            .checked_add(len)
            .ok_or_else(|| "Bind message size overflow".to_string())?,
        None => 0,
    };
    let content_len = 1usize
        .checked_add(statement_len)
        .and_then(|v| v.checked_add(1))
        .and_then(|v| v.checked_add(2))
        .and_then(|v| v.checked_add(2))
        .and_then(|v| v.checked_add(param_wire_len))
        .and_then(|v| v.checked_add(2))
        .ok_or_else(|| "Bind message size overflow".to_string())?;
    1usize
        .checked_add(4)
        .and_then(|v| v.checked_add(content_len))
        .and_then(|v| v.checked_add(10))
        .ok_or_else(|| "Bind/Execute pair size overflow".to_string())
}

fn checked_bind_execute_batch_capacity(
    statement_len: usize,
    param_strs: &[Option<&str>],
    count: usize,
) -> Result<usize, String> {
    let cycle_count = param_strs.len().max(1);
    let mut cycle_len = 0usize;

    if param_strs.is_empty() {
        cycle_len = checked_bind_execute_pair_len(statement_len, None)?;
    } else {
        for param in param_strs {
            let param_len = param.map_or(0, str::len);
            cycle_len = cycle_len
                .checked_add(checked_bind_execute_pair_len(
                    statement_len,
                    Some(param_len),
                )?)
                .ok_or_else(|| "Bind/Execute cycle size overflow".to_string())?;
        }
    }

    let full_cycles = count / cycle_count;
    let remainder = count % cycle_count;
    let repeated = cycle_len
        .checked_mul(full_cycles)
        .ok_or_else(|| "Bind/Execute batch size overflow".to_string())?;
    let mut total = 5usize
        .checked_add(repeated)
        .ok_or_else(|| "Bind/Execute batch size overflow".to_string())?;

    if param_strs.is_empty() {
        if remainder > 0 {
            total = total
                .checked_add(checked_bind_execute_pair_len(statement_len, None)?)
                .ok_or_else(|| "Bind/Execute batch size overflow".to_string())?;
        }
    } else {
        for param in param_strs.iter().take(remainder) {
            let param_len = param.map_or(0, str::len);
            total = total
                .checked_add(checked_bind_execute_pair_len(
                    statement_len,
                    Some(param_len),
                )?)
                .ok_or_else(|| "Bind/Execute batch size overflow".to_string())?;
        }
    }

    if total > MAX_FFI_BATCH_BYTES {
        return Err(format!(
            "Bind/Execute batch too large: {total} bytes (max {MAX_FFI_BATCH_BYTES})"
        ));
    }
    Ok(total)
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
///
/// # Safety
///
/// `qail` must be a valid, NUL-terminated C string pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_transpile(qail: *const c_char) -> *mut c_char {
    ffi_catch!(std::ptr::null_mut(), {
        clear_error();

        if qail.is_null() {
            set_error("NULL input".to_string());
            return std::ptr::null_mut();
        }

        // SAFETY: `qail` is checked non-null above and the caller contract
        // requires it to point to a valid NUL-terminated C string.
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
    })
}

/// Validate QAIL syntax.
/// Returns 1 if valid, 0 if invalid.
///
/// # Safety
///
/// If non-null, `qail` must be a valid, NUL-terminated C string pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_validate(qail: *const c_char) -> i32 {
    ffi_catch!(0, {
        clear_error();

        if qail.is_null() {
            set_error("NULL input".to_string());
            return 0;
        }

        // SAFETY: `qail` is checked non-null above and the caller contract
        // requires it to point to a valid NUL-terminated C string.
        let c_str = unsafe { CStr::from_ptr(qail) };
        match c_str.to_str() {
            Ok(s) => {
                if qail_core::parse(s).is_ok() {
                    1
                } else {
                    set_error("Invalid QAIL syntax".to_string());
                    0
                }
            }
            Err(e) => {
                set_error(format!("Invalid UTF-8: {}", e));
                0
            }
        }
    })
}

// ============================================================================
// Wire Protocol Encoding
// ============================================================================

/// Encode a SELECT query to PostgreSQL wire protocol bytes.
/// Returns 0 on success, non-zero on error.
/// Caller must free with qail_free_bytes().
///
/// # Safety
///
/// `table` and optional `columns` must be valid, NUL-terminated C strings.
/// `out_ptr` and `out_len` must be valid writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_encode_get(
    table: *const c_char,
    columns: *const c_char, // comma-separated, or "*" for all
    limit: i64,             // -1 for no limit
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    ffi_catch!(-99, {
        clear_error();

        if table.is_null() || out_ptr.is_null() || out_len.is_null() {
            set_error("NULL pointer argument".to_string());
            return -1;
        }

        // SAFETY: `table` is checked non-null above and the caller contract
        // requires it to point to a valid NUL-terminated C string.
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
            // SAFETY: `columns` is non-null in this branch and the caller
            // contract requires it to be a valid NUL-terminated C string.
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

        // SAFETY: `out_ptr` and `out_len` are checked non-null above and
        // the caller contract requires them to be writable.
        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        0 // Success
    })
}

/// Encode a batch of uniform SELECT queries.
/// All queries have same table/columns, just repeated `count` times.
///
/// # Safety
///
/// `table` and optional `columns` must be valid, NUL-terminated C strings.
/// `out_ptr` and `out_len` must be valid writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_encode_uniform_batch(
    table: *const c_char,
    columns: *const c_char,
    limit: i64,
    count: usize,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    ffi_catch!(-99, {
        clear_error();

        if table.is_null() || out_ptr.is_null() || out_len.is_null() || count == 0 {
            set_error("NULL pointer or zero count".to_string());
            return -1;
        }

        // SAFETY: `table` is checked non-null above and the caller contract
        // requires it to point to a valid NUL-terminated C string.
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
            // SAFETY: `columns` is non-null in this branch and the caller
            // contract requires it to be a valid NUL-terminated C string.
            let cols_str = match unsafe { CStr::from_ptr(columns) }.to_str() {
                Ok(s) => s,
                Err(e) => {
                    set_error(format!("Invalid UTF-8 in columns: {}", e));
                    return -3;
                }
            };
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
        let batch_len = match checked_batch_capacity(single_query.len(), count, "uniform query") {
            Ok(len) => len,
            Err(e) => {
                set_error(e);
                return -4;
            }
        };
        let mut batch_bytes = Vec::with_capacity(batch_len);
        for _ in 0..count {
            batch_bytes.extend_from_slice(&single_query);
        }

        let len = batch_bytes.len();
        let mut boxed = batch_bytes.into_boxed_slice();
        let ptr = boxed.as_mut_ptr();
        std::mem::forget(boxed);

        // SAFETY: `out_ptr` and `out_len` are checked non-null above and
        // the caller contract requires them to be writable.
        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        0
    })
}

// ============================================================================
// Memory Management
// ============================================================================

/// Free a string returned by qail_transpile.
///
/// # Safety
///
/// `ptr` must be null or a pointer returned by `qail_transpile` that has not
/// already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        // SAFETY: The caller contract requires `ptr` to come from
        // `CString::into_raw` in `qail_transpile` and not be freed already.
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

/// Free bytes returned by qail_encode_* functions.
///
/// # Safety
///
/// `ptr` and `len` must match a byte buffer returned by a `qail_encode_*`
/// function and must not have been freed already.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_free_bytes(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        // SAFETY: The caller contract requires `ptr` and `len` to match a
        // buffer allocated by this library and not be freed already.
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
                let c_str = CString::new(msg.as_str()).unwrap_or_default();
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
/// # Arguments
/// * `name` - Statement name (use "" for unnamed)
/// * `sql` - SQL with $1, $2, etc placeholders
/// * `out_ptr` - Output pointer for allocated bytes
/// * `out_len` - Output length
///
/// Returns 0 on success.
///
/// # Safety
///
/// `sql` and optional `name` must be valid, NUL-terminated C strings.
/// `out_ptr` and `out_len` must be valid writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_encode_parse(
    name: *const c_char,
    sql: *const c_char,
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    ffi_catch!(-99, {
        clear_error();

        if sql.is_null() || out_ptr.is_null() || out_len.is_null() {
            set_error("NULL pointer argument".to_string());
            return -1;
        }

        let name_str = if name.is_null() {
            ""
        } else {
            // SAFETY: `name` is non-null in this branch and the caller
            // contract requires it to be a valid NUL-terminated C string.
            match unsafe { CStr::from_ptr(name) }.to_str() {
                Ok(s) => s,
                Err(e) => {
                    set_error(format!("Invalid UTF-8 in statement name: {}", e));
                    return -3;
                }
            }
        };

        // SAFETY: `sql` is checked non-null above and the caller contract
        // requires it to point to a valid NUL-terminated C string.
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

        // SAFETY: `out_ptr` and `out_len` are checked non-null above and
        // the caller contract requires them to be writable.
        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        0
    })
}

/// Encode a Sync message.
/// Used after Parse to wait for ParseComplete.
///
/// # Safety
///
/// `out_ptr` and `out_len` must be valid writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_encode_sync(out_ptr: *mut *mut u8, out_len: *mut usize) -> i32 {
    ffi_catch!(-99, {
        clear_error();

        if out_ptr.is_null() || out_len.is_null() {
            set_error("NULL pointer argument".to_string());
            return -1;
        }

        let wire_bytes = vec![b'S', 0, 0, 0, 4];
        let len = wire_bytes.len();

        let mut boxed = wire_bytes.into_boxed_slice();
        let ptr = boxed.as_mut_ptr();
        std::mem::forget(boxed);

        // SAFETY: `out_ptr` and `out_len` are checked non-null above and the
        // caller contract requires them to be writable.
        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        0
    })
}

/// Encode a batch of Bind + Execute pairs for pipeline mode.
/// This is the hot path for prepared statement performance.
/// # Arguments
/// * `statement` - Prepared statement name
/// * `params` - Array of parameter strings (all queries use same single param)
/// * `params_count` - Number of elements in the `params` array
/// * `count` - Number of Bind+Execute pairs to generate
/// * `out_ptr` - Output pointer for allocated bytes
/// * `out_len` - Output length
///
/// Each query in batch uses params[i % params_count] as its parameter.
///
/// # Safety
///
/// The caller **MUST** ensure that `params` points to a valid array of at least
/// `params_count` elements when `params_count > 0`. Providing a smaller array
/// is **undefined behavior** (the function iterates `0..params_count` via
/// `params.add(i)`). When `params` is null or `params_count == 0`, each Bind
/// has zero parameters. Individual null elements within a non-empty array are
/// encoded as SQL NULL parameters and preserve their position in the batch
/// cycle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_encode_bind_execute_batch(
    statement: *const c_char,
    params: *const *const c_char, // Array of param strings
    params_count: usize,
    count: usize, // Number of Bind+Execute pairs
    out_ptr: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    ffi_catch!(-99, {
        clear_error();

        if statement.is_null() || out_ptr.is_null() || out_len.is_null() || count == 0 {
            set_error("NULL pointer or zero count".to_string());
            return -1;
        }

        // SAFETY: `statement` is checked non-null above and the caller
        // contract requires it to be a valid NUL-terminated C string.
        let stmt_str = match unsafe { CStr::from_ptr(statement) }.to_str() {
            Ok(s) => s,
            Err(e) => {
                set_error(format!("Invalid UTF-8 in statement: {}", e));
                return -2;
            }
        };

        // Collect params
        let param_strs: Vec<Option<&str>> = if params.is_null() || params_count == 0 {
            vec![]
        } else {
            let mut out = Vec::new();
            for i in 0..params_count {
                // SAFETY: The caller contract requires `params` to point
                // to an array containing at least `params_count` entries.
                let p = unsafe { *params.add(i) };
                if p.is_null() {
                    out.push(None);
                    continue;
                }
                // SAFETY: Non-null parameter entries are expected to
                // point to valid NUL-terminated C strings.
                match unsafe { CStr::from_ptr(p) }.to_str() {
                    Ok(s) => out.push(Some(s)),
                    Err(e) => {
                        set_error(format!("Invalid UTF-8 in param {i}: {e}"));
                        return -3;
                    }
                }
            }
            out
        };

        let batch_len =
            match checked_bind_execute_batch_capacity(stmt_str.len(), &param_strs, count) {
                Ok(len) => len,
                Err(e) => {
                    set_error(e);
                    return -4;
                }
            };
        let mut buf = Vec::with_capacity(batch_len);

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

        // SAFETY: `out_ptr` and `out_len` are checked non-null above and
        // the caller contract requires them to be writable.
        unsafe {
            *out_ptr = ptr;
            *out_len = len;
        }

        0
    })
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
fn encode_bind_to_buf(buf: &mut Vec<u8>, statement: &str, param: Option<Option<&str>>) {
    let param_bytes = param.flatten().map(|s| s.as_bytes());
    let param_len = param_bytes.map_or(0, |b| b.len());
    let param_count = if param.is_some() { 1i16 } else { 0i16 };
    let param_section_len = if param.is_some() { 4 + param_len } else { 0 };

    // Content: portal(1) + statement(len+1) + format_codes(2) + param_count(2)
    //          + optional param_len(4) + param_data + result_format(2)
    let content_len = 1 + statement.len() + 1 + 2 + 2 + param_section_len + 2;

    buf.push(b'B');
    buf.extend_from_slice(&((content_len + 4) as i32).to_be_bytes());
    buf.push(0); // Unnamed portal
    buf.extend_from_slice(statement.as_bytes());
    buf.push(0);
    buf.extend_from_slice(&0i16.to_be_bytes()); // Format codes (text)
    buf.extend_from_slice(&param_count.to_be_bytes());

    if param.is_some() {
        if let Some(data) = param_bytes {
            buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
            buf.extend_from_slice(data);
        } else {
            buf.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
        }
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
fn response_cell_bytes(response: &QailResponse, row: usize, col: usize) -> Result<&[u8], String> {
    let row_values = response
        .rows
        .get(row)
        .ok_or_else(|| format!("Row index out of range: {row}"))?;
    let value = row_values
        .get(col)
        .ok_or_else(|| format!("Column index out of range: {col}"))?;
    value
        .as_deref()
        .ok_or_else(|| format!("Response value is NULL at row {row}, column {col}"))
}

#[cfg(feature = "response")]
/// Decode PostgreSQL response bytes.
/// Returns a handle that must be freed with qail_response_free.
///
/// # Safety
///
/// `data` must point to `len` readable bytes. `out_handle` must be a valid
/// writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_decode_response(
    data: *const u8,
    len: usize,
    out_handle: *mut *mut QailResponse,
) -> i32 {
    ffi_catch!(-99, {
        clear_error();

        if data.is_null() || out_handle.is_null() {
            set_error("Null pointer".to_string());
            return -1;
        }

        // SAFETY: `data` is checked non-null above and the caller contract
        // requires it to point to `len` readable bytes.
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
        // SAFETY: `out_handle` is checked non-null above and the caller
        // contract requires it to be writable.
        unsafe { *out_handle = Box::into_raw(boxed) };
        0
    })
}

#[cfg(feature = "response")]
/// Get number of rows in response.
///
/// # Safety
///
/// If non-null, `handle` must point to a live `QailResponse` returned by
/// `qail_decode_response`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_response_row_count(handle: *const QailResponse) -> usize {
    ffi_catch!(0, {
        clear_error();
        if handle.is_null() {
            set_error("NULL response handle".to_string());
            return 0;
        }
        // SAFETY: `handle` is checked non-null above and the caller contract
        // requires it to point to a live `QailResponse`.
        unsafe { (&*handle).rows.len() }
    })
}

#[cfg(feature = "response")]
/// Get number of columns in a row.
///
/// # Safety
///
/// If non-null, `handle` must point to a live `QailResponse` returned by
/// `qail_decode_response`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_response_column_count(
    handle: *const QailResponse,
    row: usize,
) -> usize {
    ffi_catch!(0, {
        clear_error();
        if handle.is_null() {
            set_error("NULL response handle".to_string());
            return 0;
        }
        // SAFETY: `handle` is checked non-null above and the caller contract
        // requires it to point to a live `QailResponse`.
        unsafe {
            let resp = &*handle;
            match resp.rows.get(row) {
                Some(row) => row.len(),
                None => {
                    set_error(format!("Row index out of range: {row}"));
                    0
                }
            }
        }
    })
}

#[cfg(feature = "response")]
/// Get affected row count.
///
/// # Safety
///
/// If non-null, `handle` must point to a live `QailResponse` returned by
/// `qail_decode_response`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_response_affected_rows(handle: *const QailResponse) -> u64 {
    ffi_catch!(0, {
        clear_error();
        if handle.is_null() {
            set_error("NULL response handle".to_string());
            return 0;
        }
        // SAFETY: `handle` is checked non-null above and the caller contract
        // requires it to point to a live `QailResponse`.
        unsafe { (&*handle).affected_rows }
    })
}

#[cfg(feature = "response")]
/// Check if a column is NULL.
///
/// # Safety
///
/// If non-null, `handle` must point to a live `QailResponse` returned by
/// `qail_decode_response`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_response_is_null(
    handle: *const QailResponse,
    row: usize,
    col: usize,
) -> i32 {
    ffi_catch!(1, {
        clear_error();
        if handle.is_null() {
            set_error("NULL response handle".to_string());
            return 1;
        }
        // SAFETY: `handle` is checked non-null above and the caller contract
        // requires it to point to a live `QailResponse`.
        unsafe {
            let resp = &*handle;
            let Some(row_values) = resp.rows.get(row) else {
                set_error(format!("Row index out of range: {row}"));
                return 1;
            };
            let Some(value) = row_values.get(col) else {
                set_error(format!("Column index out of range: {col}"));
                return 1;
            };
            if value.is_none() { 1 } else { 0 }
        }
    })
}

#[cfg(feature = "response")]
/// Get column value as string.
/// Returns pointer to null-terminated string, or NULL if value is NULL.
///
/// # Safety
///
/// The returned `*out_ptr` borrows memory from the `QailResponse` handle.
/// It is **only valid** until `qail_response_free(handle)` is called.
/// Callers **MUST** copy the data (e.g., `memcpy`) before freeing the response
/// if the value is needed beyond the response's lifetime.
/// `out_ptr` and `out_len` must be valid writable pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_response_get_string(
    handle: *const QailResponse,
    row: usize,
    col: usize,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    ffi_catch!(-99, {
        clear_error();
        if handle.is_null() || out_ptr.is_null() || out_len.is_null() {
            set_error("NULL pointer argument".to_string());
            return -1;
        }

        // SAFETY: Pointers are checked non-null above. `handle` must point to a
        // live response and `out_ptr`/`out_len` must be writable per caller contract.
        unsafe {
            let resp = &*handle;
            let Some(row_values) = resp.rows.get(row) else {
                set_error(format!("Row index out of range: {row}"));
                return -1;
            };
            let Some(value) = row_values.get(col) else {
                set_error(format!("Column index out of range: {col}"));
                return -1;
            };

            if let Some(bytes) = value {
                *out_ptr = bytes.as_ptr();
                *out_len = bytes.len();
            } else {
                *out_ptr = std::ptr::null();
                *out_len = 0;
            }
            0
        }
    })
}

#[cfg(feature = "response")]
/// Get column value as i32.
///
/// # Safety
///
/// If non-null, `handle` must point to a live `QailResponse` returned by
/// `qail_decode_response`. `out_value` must be a valid writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_response_get_i32(
    handle: *const QailResponse,
    row: usize,
    col: usize,
    out_value: *mut i32,
) -> i32 {
    ffi_catch!(-99, {
        clear_error();
        if handle.is_null() || out_value.is_null() {
            set_error("NULL pointer argument".to_string());
            return -1;
        }

        // SAFETY: Pointers are checked non-null above. `handle` must point to a
        // live response and `out_value` must be writable per caller contract.
        unsafe {
            let bytes = match response_cell_bytes(&*handle, row, col) {
                Ok(bytes) => bytes,
                Err(e) => {
                    set_error(e);
                    return -1;
                }
            };
            match std::str::from_utf8(bytes)
                .ok()
                .and_then(|s| s.parse::<i32>().ok())
            {
                Some(v) => {
                    *out_value = v;
                    0
                }
                None => {
                    set_error("Response value is not a valid i32".to_string());
                    -1
                }
            }
        }
    })
}

#[cfg(feature = "response")]
/// Get column value as i64.
///
/// # Safety
///
/// If non-null, `handle` must point to a live `QailResponse` returned by
/// `qail_decode_response`. `out_value` must be a valid writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_response_get_i64(
    handle: *const QailResponse,
    row: usize,
    col: usize,
    out_value: *mut i64,
) -> i32 {
    ffi_catch!(-99, {
        clear_error();
        if handle.is_null() || out_value.is_null() {
            set_error("NULL pointer argument".to_string());
            return -1;
        }

        // SAFETY: Pointers are checked non-null above. `handle` must point to a
        // live response and `out_value` must be writable per caller contract.
        unsafe {
            let bytes = match response_cell_bytes(&*handle, row, col) {
                Ok(bytes) => bytes,
                Err(e) => {
                    set_error(e);
                    return -1;
                }
            };
            match std::str::from_utf8(bytes)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(v) => {
                    *out_value = v;
                    0
                }
                None => {
                    set_error("Response value is not a valid i64".to_string());
                    -1
                }
            }
        }
    })
}

#[cfg(feature = "response")]
/// Get column value as f64.
///
/// # Safety
///
/// If non-null, `handle` must point to a live `QailResponse` returned by
/// `qail_decode_response`. `out_value` must be a valid writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_response_get_f64(
    handle: *const QailResponse,
    row: usize,
    col: usize,
    out_value: *mut f64,
) -> i32 {
    ffi_catch!(-99, {
        clear_error();
        if handle.is_null() || out_value.is_null() {
            set_error("NULL pointer argument".to_string());
            return -1;
        }

        // SAFETY: Pointers are checked non-null above. `handle` must point to a
        // live response and `out_value` must be writable per caller contract.
        unsafe {
            let bytes = match response_cell_bytes(&*handle, row, col) {
                Ok(bytes) => bytes,
                Err(e) => {
                    set_error(e);
                    return -1;
                }
            };
            match std::str::from_utf8(bytes)
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
            {
                Some(v) => {
                    *out_value = v;
                    0
                }
                None => {
                    set_error("Response value is not a valid f64".to_string());
                    -1
                }
            }
        }
    })
}

#[cfg(feature = "response")]
/// Get column value as bool.
///
/// # Safety
///
/// If non-null, `handle` must point to a live `QailResponse` returned by
/// `qail_decode_response`. `out_value` must be a valid writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_response_get_bool(
    handle: *const QailResponse,
    row: usize,
    col: usize,
    out_value: *mut i32,
) -> i32 {
    ffi_catch!(-99, {
        clear_error();
        if handle.is_null() || out_value.is_null() {
            set_error("NULL pointer argument".to_string());
            return -1;
        }

        // SAFETY: Pointers are checked non-null above. `handle` must point to a
        // live response and `out_value` must be writable per caller contract.
        unsafe {
            let bytes = match response_cell_bytes(&*handle, row, col) {
                Ok(bytes) => bytes,
                Err(e) => {
                    set_error(e);
                    return -1;
                }
            };
            let Ok(s) = std::str::from_utf8(bytes) else {
                set_error("Response value is not valid UTF-8".to_string());
                return -1;
            };
            *out_value = match s {
                "t" | "true" | "1" => 1,
                "f" | "false" | "0" => 0,
                _ => {
                    set_error("Response value is not a valid bool".to_string());
                    return -1;
                }
            };
            0
        }
    })
}

#[cfg(feature = "response")]
/// Free a response handle.
///
/// # Safety
///
/// `handle` must be null or a pointer returned by `qail_decode_response` that
/// has not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qail_response_free(handle: *mut QailResponse) {
    if !handle.is_null() {
        // SAFETY: The caller contract requires `handle` to be null or a value
        // returned by `qail_decode_response` that has not been freed already.
        unsafe {
            let _ = Box::from_raw(handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exported_symbol_names_from_source() -> Vec<&'static str> {
        let mut symbols = Vec::new();
        let mut expect_export = false;

        for line in include_str!("lib.rs").lines() {
            let line = line.trim();
            if line == "#[unsafe(no_mangle)]" {
                expect_export = true;
                continue;
            }

            if !expect_export {
                continue;
            }

            if let Some(fn_pos) = line.find("fn ") {
                let after_fn = &line[fn_pos + 3..];
                let name_end = after_fn
                    .find('(')
                    .expect("exported function line must include argument list");
                symbols.push(&after_fn[..name_end]);
                expect_export = false;
            }
        }

        symbols
    }

    #[test]
    fn exported_ffi_symbols_stay_wire_only() {
        let symbols = exported_symbol_names_from_source();
        assert_eq!(
            symbols,
            vec![
                "qail_version",
                "qail_transpile",
                "qail_validate",
                "qail_encode_get",
                "qail_encode_uniform_batch",
                "qail_free",
                "qail_free_bytes",
                "qail_last_error",
                "qail_encode_parse",
                "qail_encode_sync",
                "qail_encode_bind_execute_batch",
                "qail_decode_response",
                "qail_response_row_count",
                "qail_response_column_count",
                "qail_response_affected_rows",
                "qail_response_is_null",
                "qail_response_get_string",
                "qail_response_get_i32",
                "qail_response_get_i64",
                "qail_response_get_f64",
                "qail_response_get_bool",
                "qail_response_free",
            ]
        );

        let forbidden = [
            "auth", "connect", "gss", "jwt", "kerberos", "krb", "login", "sso", "ssl", "tls",
            "token",
        ];
        for symbol in symbols {
            let lower = symbol.to_ascii_lowercase();
            for needle in forbidden {
                assert!(
                    !lower.contains(needle),
                    "qail-encoder FFI must stay wire/query-only; forbidden `{needle}` in symbol `{symbol}`"
                );
            }
        }
    }

    #[test]
    fn c_header_covers_exported_ffi_symbols() {
        let header = include_str!("../include/qail_encoder.h");
        for symbol in exported_symbol_names_from_source() {
            assert!(
                header.contains(&format!("{symbol}(")),
                "missing C header declaration for exported symbol `{symbol}`"
            );
        }
    }

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

    fn last_error_string() -> String {
        let ptr = qail_last_error();
        assert!(!ptr.is_null(), "expected last error to be set");
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }

    fn assert_last_error_clear() {
        assert!(
            qail_last_error().is_null(),
            "expected qail_last_error to be clear"
        );
    }

    fn bind_param_values(bytes: &[u8]) -> Vec<Option<Vec<u8>>> {
        let mut values = Vec::new();
        let mut offset = 0usize;

        while offset < bytes.len() {
            match bytes[offset] {
                b'B' => {
                    let msg_len = i32::from_be_bytes(
                        bytes[offset + 1..offset + 5]
                            .try_into()
                            .expect("bind message length"),
                    ) as usize;
                    let end = offset + 1 + msg_len;
                    let mut pos = offset + 5;

                    while bytes[pos] != 0 {
                        pos += 1;
                    }
                    pos += 1;

                    while bytes[pos] != 0 {
                        pos += 1;
                    }
                    pos += 1;

                    let format_count =
                        i16::from_be_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2 + (format_count * 2);

                    let param_count =
                        i16::from_be_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    assert_eq!(param_count, 1);

                    let param_len = i32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    if param_len == -1 {
                        values.push(None);
                    } else {
                        let param_len = param_len as usize;
                        values.push(Some(bytes[pos..pos + param_len].to_vec()));
                    }

                    offset = end;
                }
                b'E' => offset += 10,
                b'S' => break,
                other => panic!("unexpected message byte {other} at offset {offset}"),
            }
        }

        values
    }

    fn bind_param_counts(bytes: &[u8]) -> Vec<usize> {
        let mut counts = Vec::new();
        let mut offset = 0usize;

        while offset < bytes.len() {
            match bytes[offset] {
                b'B' => {
                    let msg_len = i32::from_be_bytes(
                        bytes[offset + 1..offset + 5]
                            .try_into()
                            .expect("bind message length"),
                    ) as usize;
                    let end = offset + 1 + msg_len;
                    let mut pos = offset + 5;

                    while bytes[pos] != 0 {
                        pos += 1;
                    }
                    pos += 1;

                    while bytes[pos] != 0 {
                        pos += 1;
                    }
                    pos += 1;

                    let format_count =
                        i16::from_be_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2 + (format_count * 2);

                    counts
                        .push(i16::from_be_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize);

                    offset = end;
                }
                b'E' => offset += 10,
                b'S' => break,
                other => panic!("unexpected message byte {other} at offset {offset}"),
            }
        }

        counts
    }

    #[test]
    fn test_uniform_batch_rejects_size_overflow_without_allocation() {
        let table = CString::new("users").unwrap();
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len = 0usize;

        let rc = unsafe {
            qail_encode_uniform_batch(
                table.as_ptr(),
                std::ptr::null(),
                1,
                usize::MAX,
                &mut out_ptr,
                &mut out_len,
            )
        };

        assert_eq!(rc, -4);
        assert!(out_ptr.is_null());
        assert_eq!(out_len, 0);
        assert!(last_error_string().contains("uniform query batch"));
    }

    #[test]
    fn test_bind_execute_batch_rejects_size_overflow_without_allocation() {
        let statement = CString::new("stmt").unwrap();
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len = 0usize;

        let rc = unsafe {
            qail_encode_bind_execute_batch(
                statement.as_ptr(),
                std::ptr::null(),
                0,
                usize::MAX,
                &mut out_ptr,
                &mut out_len,
            )
        };

        assert_eq!(rc, -4);
        assert!(out_ptr.is_null());
        assert_eq!(out_len, 0);
        assert!(last_error_string().contains("Bind/Execute batch"));
    }

    #[test]
    fn test_uniform_batch_rejects_invalid_columns_utf8() {
        let table = CString::new("users").unwrap();
        let columns = b"\xff\0";
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len = 0usize;

        let rc = unsafe {
            qail_encode_uniform_batch(
                table.as_ptr(),
                columns.as_ptr() as *const c_char,
                1,
                1,
                &mut out_ptr,
                &mut out_len,
            )
        };

        assert_eq!(rc, -3);
        assert!(out_ptr.is_null());
        assert_eq!(out_len, 0);
        assert!(last_error_string().contains("Invalid UTF-8 in columns"));
    }

    #[test]
    fn test_bind_execute_batch_rejects_invalid_statement_utf8() {
        let statement = b"\xff\0";
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len = 0usize;

        let rc = unsafe {
            qail_encode_bind_execute_batch(
                statement.as_ptr() as *const c_char,
                std::ptr::null(),
                0,
                1,
                &mut out_ptr,
                &mut out_len,
            )
        };

        assert_eq!(rc, -2);
        assert!(out_ptr.is_null());
        assert_eq!(out_len, 0);
        assert!(last_error_string().contains("Invalid UTF-8 in statement"));
    }

    #[test]
    fn test_bind_execute_batch_rejects_invalid_param_utf8() {
        let statement = CString::new("stmt").unwrap();
        let invalid = b"\xff\0";
        let params = [invalid.as_ptr() as *const c_char];
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len = 0usize;

        let rc = unsafe {
            qail_encode_bind_execute_batch(
                statement.as_ptr(),
                params.as_ptr(),
                params.len(),
                1,
                &mut out_ptr,
                &mut out_len,
            )
        };

        assert_eq!(rc, -3);
        assert!(out_ptr.is_null());
        assert_eq!(out_len, 0);
        assert!(last_error_string().contains("Invalid UTF-8 in param 0"));
    }

    #[test]
    fn test_bind_execute_batch_preserves_null_param_slots() {
        let statement = CString::new("stmt").unwrap();
        let param = CString::new("alice").unwrap();
        let params = [std::ptr::null(), param.as_ptr()];
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len = 0usize;

        let rc = unsafe {
            qail_encode_bind_execute_batch(
                statement.as_ptr(),
                params.as_ptr(),
                params.len(),
                4,
                &mut out_ptr,
                &mut out_len,
            )
        };

        assert_eq!(rc, 0);
        assert!(!out_ptr.is_null());
        assert!(out_len > 0);

        let bytes = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        let values = bind_param_values(bytes);
        assert_eq!(
            values,
            vec![None, Some(b"alice".to_vec()), None, Some(b"alice".to_vec())]
        );

        unsafe {
            qail_free_bytes(out_ptr, out_len);
        }
    }

    #[test]
    fn test_bind_execute_batch_with_no_params_encodes_zero_bind_parameters() {
        let statement = CString::new("stmt").unwrap();
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len = 0usize;

        let rc = unsafe {
            qail_encode_bind_execute_batch(
                statement.as_ptr(),
                std::ptr::null(),
                0,
                2,
                &mut out_ptr,
                &mut out_len,
            )
        };

        assert_eq!(rc, 0);
        assert!(!out_ptr.is_null());
        assert!(out_len > 0);

        let bytes = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        assert_eq!(
            bind_param_counts(bytes),
            vec![0, 0],
            "params_count=0 must encode zero Bind parameters, not one NULL"
        );

        unsafe {
            qail_free_bytes(out_ptr, out_len);
        }
    }

    #[test]
    fn test_encode_parse_rejects_invalid_statement_name_utf8() {
        let name = b"\xff\0";
        let sql = CString::new("SELECT 1").unwrap();
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len = 0usize;

        let rc = unsafe {
            qail_encode_parse(
                name.as_ptr() as *const c_char,
                sql.as_ptr(),
                &mut out_ptr,
                &mut out_len,
            )
        };

        assert_eq!(rc, -3);
        assert!(out_ptr.is_null());
        assert_eq!(out_len, 0);
        assert!(last_error_string().contains("Invalid UTF-8 in statement name"));
    }

    #[test]
    fn test_validate_clears_stale_error_on_success() {
        let invalid = CString::new("definitely not valid qail").unwrap();
        assert_eq!(unsafe { qail_validate(invalid.as_ptr()) }, 0);
        assert!(last_error_string().contains("Invalid QAIL syntax"));

        let valid = CString::new("get users fields id").unwrap();
        assert_eq!(unsafe { qail_validate(valid.as_ptr()) }, 1);
        assert_last_error_clear();
    }

    #[test]
    fn test_encode_sync_clears_stale_error_on_success() {
        assert_eq!(unsafe { qail_validate(std::ptr::null()) }, 0);
        assert!(last_error_string().contains("NULL input"));

        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len = 0usize;
        let rc = unsafe { qail_encode_sync(&mut out_ptr, &mut out_len) };

        assert_eq!(rc, 0);
        assert!(!out_ptr.is_null());
        assert_eq!(out_len, 5);
        assert_last_error_clear();

        unsafe {
            qail_free_bytes(out_ptr, out_len);
        }
    }

    #[test]
    fn test_encode_sync_sets_error_on_null_outputs() {
        let rc = unsafe { qail_encode_sync(std::ptr::null_mut(), std::ptr::null_mut()) };

        assert_eq!(rc, -1);
        assert!(last_error_string().contains("NULL pointer argument"));
    }

    #[cfg(feature = "response")]
    fn sample_response() -> QailResponse {
        QailResponse {
            rows: vec![vec![Some(b"42".to_vec()), None]],
            affected_rows: 7,
            error: None,
        }
    }

    #[cfg(feature = "response")]
    #[test]
    fn test_response_get_i32_clears_stale_error_on_success() {
        let _ = unsafe { qail_encode_sync(std::ptr::null_mut(), std::ptr::null_mut()) };
        assert!(last_error_string().contains("NULL pointer argument"));

        let response = sample_response();
        let mut value = 0i32;
        let rc = unsafe { qail_response_get_i32(&response, 0, 0, &mut value) };

        assert_eq!(rc, 0);
        assert_eq!(value, 42);
        assert_last_error_clear();
    }

    #[cfg(feature = "response")]
    #[test]
    fn test_response_get_string_keeps_sql_null_distinct_from_error() {
        let _ = unsafe { qail_encode_sync(std::ptr::null_mut(), std::ptr::null_mut()) };
        assert!(last_error_string().contains("NULL pointer argument"));

        let response = sample_response();
        let mut out_ptr: *const u8 = std::ptr::null();
        let mut out_len = usize::MAX;
        let rc = unsafe { qail_response_get_string(&response, 0, 1, &mut out_ptr, &mut out_len) };

        assert_eq!(rc, 0);
        assert!(out_ptr.is_null());
        assert_eq!(out_len, 0);
        assert_last_error_clear();
    }

    #[cfg(feature = "response")]
    #[test]
    fn test_response_get_string_rejects_out_of_range_access() {
        let response = sample_response();
        let mut out_ptr: *const u8 = std::ptr::null();
        let mut out_len = 0usize;
        let rc = unsafe { qail_response_get_string(&response, 9, 0, &mut out_ptr, &mut out_len) };

        assert_eq!(rc, -1);
        assert!(last_error_string().contains("Row index out of range"));
    }

    #[cfg(feature = "response")]
    #[test]
    fn test_response_null_handle_sets_error() {
        assert_eq!(unsafe { qail_response_row_count(std::ptr::null()) }, 0);
        assert!(last_error_string().contains("NULL response handle"));
    }
}
