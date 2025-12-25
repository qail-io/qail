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

pub mod schema;
pub mod diff;
pub mod parser;

pub use schema::{Schema, Table, Column, Index, MigrationHint, to_qail_string};
pub use diff::diff_schemas;
pub use parser::parse_qail;
