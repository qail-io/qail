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
