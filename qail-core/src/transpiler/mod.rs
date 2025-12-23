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
        use conditions::ParamContext;
        use conditions::ConditionToSql;
        
        let generator = dialect.generator();
        let mut param_ctx = ParamContext::new();
        
        // Build WHERE clause with parameterized conditions
        let mut where_parts = Vec::new();
        for cage in &self.cages {
            if let CageKind::Filter = cage.kind {
                for cond in &cage.conditions {
                    where_parts.push(cond.to_sql_parameterized(&generator, Some(self), &mut param_ctx));
                }
            }
        }

        // For now, build a simplified parameterized SELECT
        // Full implementation would parameterize all DML operations
        let table = generator.quote_identifier(&self.table);
        
        let cols = if self.columns.is_empty() {
            "*".to_string()
        } else {
            self.columns
                .iter()
                .map(|c| match c {
                    Expr::Star => "*".to_string(),
                    Expr::Named(n) => generator.quote_identifier(n),
                    Expr::Aliased { name, alias } => {
                        format!("{} AS {}", generator.quote_identifier(name), generator.quote_identifier(alias))
                    }
                    Expr::Aggregate { col, func } => {
                        format!("{}({})", func, generator.quote_identifier(col))
                    }
                    _ => "*".to_string(), // Fallback for other variants
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        
        let mut sql = format!("SELECT {} FROM {}", cols, table);
        
        if !where_parts.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_parts.join(" AND "));
        }
        
        // Handle LIMIT/OFFSET
        for cage in &self.cages {
            match cage.kind {
                CageKind::Limit(n) => sql.push_str(&format!(" LIMIT {}", n)),
                CageKind::Offset(n) => sql.push_str(&format!(" OFFSET {}", n)),
                _ => {}
            }
        }

        TranspileResult {
            sql,
            params: param_ctx.params,
            named_params: param_ctx.named_params,
        }
    }
}
