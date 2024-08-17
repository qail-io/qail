//! QAIL Migration Module
//!
//! AST-native schema diffs with intent-awareness.
//!
//! ## Key Features
//! - Native QAIL schema format (not JSON)
//! - Intent-aware: `rename`, `transform`, `drop confirm`
//! - Diff-friendly for git
//!
//! ## Example
//! ```qail
//! table users {
//!   id serial primary_key
//!   name text not_null
//! }
//!
//! rename users.username -> users.name
//! ```

pub mod diff;
pub mod named_migration;
pub mod parser;
pub mod schema;
pub mod types;

pub use diff::diff_schemas;
pub use named_migration::{MigrationMeta, parse_migration_meta, validate_dependencies};
pub use parser::parse_qail;
pub use schema::{
    Column, FkAction, ForeignKey, Index, MigrationHint, Schema, Table, to_qail_string,
};
pub use types::ColumnType;
