//! QAIL WebAssembly Bindings
//!
//! Provides QAIL parsing and PostgreSQL transpilation for JavaScript/TypeScript.
//! Focused on AST-native PostgreSQL wire protocol.
//!
//! ## Usage (npm)
//! ```javascript
//! import init, { parse, parseAndTranspile, validate } from 'qail-wasm';
//!
//! await init();
//!
//! // Parse and get SQL directly
//! const sql = parseAndTranspile("get users fields * where active = true");
//! console.log(sql); // "SELECT * FROM users WHERE active = true"
//!
//! // Schema operations
//! const createTable = parseAndTranspile("make users fields id UUID, email VARCHAR");
//! console.log(createTable); // "CREATE TABLE users (...)"
//!
//! // With filters
//! const filtered = parseAndTranspile("get users fields id, name where role = 'admin'");
//! ```

use qail_core::transpiler::ToSql;
use wasm_bindgen::prelude::*;

/// Parse QAIL and return SQL string (PostgreSQL dialect).
#[wasm_bindgen]
pub fn parse_and_transpile(qail: &str) -> Result<String, JsError> {
    let cmd = qail_core::parse(qail).map_err(|e| JsError::new(&format!("{:?}", e)))?;
    Ok(cmd.to_sql())
}

/// Parse QAIL and return AST as JSON.
#[wasm_bindgen]
pub fn parse(qail: &str) -> Result<JsValue, JsError> {
    let cmd = qail_core::parse(qail).map_err(|e| JsError::new(&format!("{:?}", e)))?;
    serde_wasm_bindgen::to_value(&cmd).map_err(|e| JsError::new(&e.to_string()))
}

/// Validate QAIL syntax (returns true if valid).
#[wasm_bindgen]
pub fn validate(qail: &str) -> bool {
    qail_core::parse(qail).is_ok()
}

/// Get QAIL version.
#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[cfg(test)]
mod tests {
    use qail_core::transpiler::ToSql;

    fn transpile(qail: &str) -> String {
        qail_core::parse(qail).unwrap().to_sql()
    }

    #[test]
    fn test_simple_select() {
        let sql = transpile("get users fields *");
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM users"));
    }

    #[test]
    fn test_select_with_filter() {
        let sql = transpile("get users fields id, name where active = true");
        assert!(sql.contains("WHERE active = true"));
    }

    #[test]
    fn test_distinct() {
        let sql = transpile("get distinct users fields role");
        assert!(sql.contains("SELECT DISTINCT"));
    }
}
