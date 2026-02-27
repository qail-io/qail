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

pub mod alter;
pub mod diff;
pub mod named_migration;
pub mod parser;
pub mod policy;
pub mod policy_parser;
pub mod schema;
pub mod types;

pub use alter::{AlterOp, AlterTable, TableConstraint};
pub use diff::diff_schemas;
pub use named_migration::{MigrationMeta, parse_migration_meta, validate_dependencies};
pub use parser::{parse_qail, parse_qail_file};
pub use policy::{PolicyPermissiveness, PolicyTarget, RlsPolicy, session_bool_check, tenant_check};
pub use policy_parser::parse_policy_expr;
pub use schema::{
    CheckConstraint, CheckExpr, Column, Comment, CommentTarget, Deferrable, EnumType, Extension,
    FkAction, ForeignKey, Generated, Grant, GrantAction, Index, IndexMethod, MigrationHint,
    MultiColumnForeignKey, Privilege, Schema, SchemaFunctionDef, SchemaTriggerDef, Sequence, Table,
    ViewDef, schema_to_commands, to_qail_string,
};
pub use types::ColumnType;
