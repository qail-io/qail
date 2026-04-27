//! Build-time QAIL validation module.
//!
//! This module provides compile-time validation for QAIL queries
//! without requiring proc macros.
//!
//! # Usage in build.rs
//!
//! ```ignore
//! // In your build.rs:
//! fn main() {
//!     qail_core::build::validate();
//! }
//! ```
//!
//! # Environment Variables
//!
//! - `QAIL=schema` - Validate against schema.qail file
//! - `QAIL=live` - Validate against live database
//! - `QAIL=false` - Skip validation
//! - `QAIL_SCAN_DIRS=src,app` - Comma-separated Rust source roots to scan

/// Typed schema code generation.
mod codegen;
/// Semantic N+1 detector that reasons on executable query patterns.
pub(crate) mod nplus1_semantic;
/// Shared query IR used across build-time rules.
mod query_ir;
/// Shared Rust lexical masking helpers.
mod rust_lex;
/// Semantic source scanner for Rust QAIL usage.
pub mod scanner;
/// Schema types and parsing.
pub mod schema;
/// Build-time no-raw-SQL policy detector.
mod sql_guard;
/// Validation pipeline.
mod validate;

// ── Re-exports for public API ────────────────────────────────────────
pub use codegen::{generate_schema_code, generate_typed_schema};
pub use scanner::{QailUsage, scan_source_files, scan_source_text};
pub use schema::{ForeignKey, ResourceSchema, Schema, TableSchema};
pub use validate::{
    ValidationDiagnostic, ValidationDiagnosticKind, validate, validate_against_schema_diagnostics,
};

#[cfg(test)]
mod tests;
