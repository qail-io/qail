//! QAIL WebAssembly Bindings
//!
//! Provides QAIL parsing and SQL transpilation for JavaScript/TypeScript.
//! Wraps `qail-core` for full feature parity.
//!
//! ## Usage (npm)
//! ```javascript
//! import init, { parse, parseAndTranspile, validate } from 'qail-wasm';
//!
//! await init();
//!
//! // Parse and get SQL directly
//! const sql = parseAndTranspile("get::users:'_[active=true]");
//! console.log(sql); // "SELECT * FROM users WHERE active = true"
//!
//! // Schema operations
//! const createTable = parseAndTranspile("make::users:'id:uuid^pk = uuid()'email:varchar^uniq");
//! console.log(createTable); // "CREATE TABLE users (...)"
//!
//! // Index creation
//! const index = parseAndTranspile("index::idx_email^on(users:'email)^unique");
//! console.log(index); // "CREATE UNIQUE INDEX idx_email ON users (email)"
//! ```

use wasm_bindgen::prelude::*;
use qail_core::transpiler::{ToSql, Dialect};
use qail_core::transpiler::{ToMongo, ToDynamo, ToCassandra, ToRedis, ToElastic, ToNeo4j, ToQdrant};

/// Parse QAIL and return SQL string.
#[wasm_bindgen]
pub fn parse_and_transpile(qail: &str) -> Result<String, JsError> {
    let cmd = qail_core::parse(qail)
        .map_err(|e| JsError::new(&format!("{:?}", e)))?;
    Ok(cmd.to_sql())
}

/// Parse QAIL and return SQL string with specific dialect.
/// Dialect: "postgres", "mysql", "sqlite", "sqlserver"
#[wasm_bindgen]
pub fn parse_and_transpile_with_dialect(qail: &str, dialect: &str) -> Result<String, JsError> {
    let cmd = qail_core::parse(qail)
        .map_err(|e| JsError::new(&format!("{:?}", e)))?;
    
    let d = match dialect.to_lowercase().as_str() {
        "postgres" | "postgresql" => Dialect::Postgres,
        "mysql" => Dialect::MySQL,
        "sqlite" => Dialect::SQLite,
        "sqlserver" | "mssql" => Dialect::SqlServer,
        _ => return Err(JsError::new(&format!("Unsupported dialect: {}", dialect))),
    };

    Ok(cmd.to_sql_with_dialect(d))
}

/// Parse QAIL and return AST as JSON.
#[wasm_bindgen]
pub fn parse(qail: &str) -> Result<JsValue, JsError> {
    let cmd = qail_core::parse(qail)
        .map_err(|e| JsError::new(&format!("{:?}", e)))?;
    serde_wasm_bindgen::to_value(&cmd)
        .map_err(|e| JsError::new(&e.to_string()))
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

// ============= NoSQL Transpilation Functions =============

/// Parse QAIL and return MongoDB query/command.
#[wasm_bindgen]
pub fn to_mongo(qail: &str) -> Result<String, JsError> {
    let cmd = qail_core::parse(qail)
        .map_err(|e| JsError::new(&format!("{:?}", e)))?;
    Ok(cmd.to_mongo())
}

/// Parse QAIL and return DynamoDB JSON.
#[wasm_bindgen]
pub fn to_dynamo(qail: &str) -> Result<String, JsError> {
    let cmd = qail_core::parse(qail)
        .map_err(|e| JsError::new(&format!("{:?}", e)))?;
    Ok(cmd.to_dynamo())
}

/// Parse QAIL and return Cassandra CQL.
#[wasm_bindgen]
pub fn to_cassandra(qail: &str) -> Result<String, JsError> {
    let cmd = qail_core::parse(qail)
        .map_err(|e| JsError::new(&format!("{:?}", e)))?;
    Ok(cmd.to_cassandra())
}

/// Parse QAIL and return Redis/RediSearch command.
#[wasm_bindgen]
pub fn to_redis(qail: &str) -> Result<String, JsError> {
    let cmd = qail_core::parse(qail)
        .map_err(|e| JsError::new(&format!("{:?}", e)))?;
    Ok(cmd.to_redis_search())
}

/// Parse QAIL and return Elasticsearch DSL JSON.
#[wasm_bindgen]
pub fn to_elastic(qail: &str) -> Result<String, JsError> {
    let cmd = qail_core::parse(qail)
        .map_err(|e| JsError::new(&format!("{:?}", e)))?;
    Ok(cmd.to_elastic())
}

/// Parse QAIL and return Neo4j Cypher query.
#[wasm_bindgen]
pub fn to_neo4j(qail: &str) -> Result<String, JsError> {
    let cmd = qail_core::parse(qail)
        .map_err(|e| JsError::new(&format!("{:?}", e)))?;
    Ok(cmd.to_cypher())
}

/// Parse QAIL and return Qdrant vector search JSON.
#[wasm_bindgen]
pub fn to_qdrant(qail: &str) -> Result<String, JsError> {
    let cmd = qail_core::parse(qail)
        .map_err(|e| JsError::new(&format!("{:?}", e)))?;
    Ok(cmd.to_qdrant_search())
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

    // DDL tests disabled temporarily - use qail-core tests for DDL verification
}
