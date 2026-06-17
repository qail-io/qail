//! QAIL SchemaOps CLI library surface.
//!
//! The `qail` crate is primarily installed as a command-line tool:
//!
//! ```text
//! cargo install qail
//! qail pull --url postgres://... > schema.qail
//! qail diff _ schema.qail --live --url postgres://...
//! qail migrate apply --phase expand
//! qail migrate apply --phase backfill
//! qail migrate apply --phase contract --codebase ./src
//! ```
//!
//! For application runtime code, start with:
//!
//! - `qail-core` for the AST Kernel: typed query AST, expressions, RLS, and
//!   native access policy.
//! - `qail-pg` for the Postgres Driver: async wire-protocol execution of QAIL
//!   AST commands.
//! - `qail-gateway` for the Access Gateway: AutoREST, WebSocket, OpenAPI, and
//!   policy enforcement.
//!
//! This library re-exports selected parser and AST modules for the CLI's own
//! internals and for advanced tooling. It is not the preferred runtime entry
//! point for database access.

pub use qail_core::parse;
pub use qail_core::prelude;
pub use qail_core::{ast, error, parser, transpiler};

// CLI modules
pub mod backup;
pub mod branch;
pub mod colors;
pub mod exec;
pub mod init;
pub mod introspection;
pub mod lint;
pub mod migrations;
#[cfg(feature = "repl")]
pub mod repl;
pub mod resolve;
pub mod schema;
pub mod schema_tools;
pub mod shadow;
#[cfg(feature = "vector")]
pub mod snapshot;
pub mod sql_gen;
pub mod sync;
pub mod time;
pub mod types;
pub mod util;
#[cfg(feature = "vector")]
pub mod vector;
#[cfg(feature = "vector")]
pub mod worker;
