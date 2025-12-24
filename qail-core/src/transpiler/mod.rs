//! SQL Transpiler for QAIL AST.
//!
//! Converts parsed QAIL commands into executable SQL strings.

pub mod traits;
pub mod sql;
pub mod dialect;
pub mod conditions;
pub mod ddl;
pub mod dml;

// NoSQL transpilers (organized in nosql/ subdirectory)
pub mod nosql;
pub use nosql::mongo::ToMongo;
pub use nosql::dynamo::ToDynamo;
pub use nosql::cassandra::ToCassandra;
pub use nosql::redis::ToRedis;
pub use nosql::elastic::ToElastic;
pub use nosql::neo4j::ToNeo4j;
pub use nosql::qdrant::ToQdrant;

#[cfg(test)]
mod tests;

use crate::ast::*;
pub use traits::SqlGenerator;
pub use traits::escape_identifier;
pub use dialect::Dialect;
pub use conditions::ConditionToSql;

/// Result of transpilation with extracted parameters.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TranspileResult {
    /// The SQL template with placeholders (e.g., $1, $2 or ?, ?)
    pub sql: String,
    /// The extracted parameter values in order
    pub params: Vec<Value>,
    /// Names of named parameters in order they appear (for :name → $n mapping)
    pub named_params: Vec<String>,
}

impl TranspileResult {
    /// Create a new TranspileResult.
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
            named_params: vec![],
        }
    }

    /// Create a result with no parameters.
    pub fn sql_only(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
            named_params: Vec::new(),
        }
    }
}

/// Trait for converting AST nodes to parameterized SQL.
pub trait ToSqlParameterized {
    /// Convert to SQL with extracted parameters (default dialect).
    fn to_sql_parameterized(&self) -> TranspileResult {
        self.to_sql_parameterized_with_dialect(Dialect::default())
    }
    /// Convert to SQL with extracted parameters for specific dialect.
    fn to_sql_parameterized_with_dialect(&self, dialect: Dialect) -> TranspileResult;
}

/// Trait for converting AST nodes to SQL.
pub trait ToSql {
    /// Convert this node to a SQL string using default dialect.
    fn to_sql(&self) -> String {
        self.to_sql_with_dialect(Dialect::default())
    }
    /// Convert this node to a SQL string with specific dialect.
    fn to_sql_with_dialect(&self, dialect: Dialect) -> String;
}

impl ToSql for QailCmd {
    fn to_sql_with_dialect(&self, dialect: Dialect) -> String {
        match self.action {
            Action::Get => dml::select::build_select(self, dialect),
            Action::Set => dml::update::build_update(self, dialect),
            Action::Del => dml::delete::build_delete(self, dialect),
            Action::Add => dml::insert::build_insert(self, dialect),
            Action::Gen => format!("-- gen::{}  (generates Rust struct, not SQL)", self.table),
            Action::Make => ddl::build_create_table(self, dialect),
            Action::Mod => ddl::build_alter_table(self, dialect),
            Action::Over => dml::window::build_window(self, dialect),
            Action::With => dml::cte::build_cte(self, dialect),
            Action::Index => ddl::build_create_index(self, dialect),
            // Stubs
            Action::TxnStart => "BEGIN TRANSACTION;".to_string(), // Default stub
            Action::TxnCommit => "COMMIT;".to_string(),
            Action::TxnRollback => "ROLLBACK;".to_string(),
            Action::Put => dml::upsert::build_upsert(self, dialect),
            Action::Drop => format!("DROP TABLE {}", self.table),
            Action::DropCol | Action::RenameCol => ddl::build_alter_column(self, dialect),
            // JSON features
            Action::JsonTable => dml::json_table::build_json_table(self, dialect),
        }
    }
}

impl ToSqlParameterized for QailCmd {
    fn to_sql_parameterized_with_dialect(&self, dialect: Dialect) -> TranspileResult {
        // Use the full ToSql implementation which handles CTEs, JOINs, etc.
        // Then post-process to extract named parameters for binding
        let full_sql = self.to_sql_with_dialect(dialect);
        
        // Extract named parameters (those starting with :) from the SQL
        // and replace them with positional parameters ($1, $2, etc.)
        let mut named_params: Vec<String> = Vec::new();
        let mut seen_params: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut result = String::with_capacity(full_sql.len());
        let mut chars = full_sql.chars().peekable();
        let mut param_index = 1;
        
        while let Some(c) = chars.next() {
            if c == ':' {
                // Check if this is a Postgres cast (::) - pass through
                if let Some(&next) = chars.peek() {
                    if next == ':' {
                        // Double colon - emit both and continue
                        result.push(':');
                        result.push(chars.next().unwrap());
                        continue;
                    }
                    // Check if this is a named parameter (followed by identifier chars)
                    if next.is_ascii_alphabetic() || next == '_' {
                        // Parse the parameter name
                        let mut param_name = String::new();
                        while let Some(&ch) = chars.peek() {
                            if ch.is_ascii_alphanumeric() || ch == '_' {
                                param_name.push(chars.next().unwrap());
                            } else {
                                break;
                            }
                        }
                        
                        // Get or assign positional index
                        let idx = if let Some(&existing) = seen_params.get(&param_name) {
                            existing
                        } else {
                            let idx = param_index;
                            seen_params.insert(param_name.clone(), idx);
                            named_params.push(param_name);
                            param_index += 1;
                            idx
                        };
                        
                        result.push('$');
                        result.push_str(&idx.to_string());
                        continue;
                    }
                }
            }
            result.push(c);
        }
        
        TranspileResult {
            sql: result,
            params: Vec::new(), // Positional params not used, named_params provides mapping
            named_params,
        }
    }
}
