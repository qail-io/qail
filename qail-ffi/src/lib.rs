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
        "mysql" => Dialect::MySQL,
        "sqlite" => Dialect::SQLite,
        "sqlserver" | "mssql" => Dialect::SqlServer,
        _ => {
            set_error(format!("Unsupported dialect: {}", dialect_str));
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
