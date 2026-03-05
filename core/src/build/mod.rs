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
/// Shared query IR used across build-time rules.
mod query_ir;
/// Text-based source scanner.
pub mod scanner;
/// Schema types and parsing.
pub mod schema;
/// Syn-based AST analyzer (requires `syn-scanner` feature).
#[cfg(feature = "syn-scanner")]
pub mod syn_analyzer;
/// Syn-based N+1 detector used when `syn-scanner` is enabled without full `analyzer`.
#[cfg(feature = "syn-scanner")]
#[allow(dead_code, unused_imports)]
use crate::analyzer::rust_ast::nplus1 as syn_nplus1;
/// Validation pipeline.
mod validate;

// ── Re-exports for public API ────────────────────────────────────────
pub use codegen::{generate_schema_code, generate_typed_schema};
pub use scanner::{QailUsage, scan_source_files};
pub use schema::{ForeignKey, ResourceSchema, Schema, TableSchema};
pub use validate::{
    ValidationDiagnostic, ValidationDiagnosticKind, validate, validate_against_schema,
    validate_against_schema_diagnostics,
};

#[cfg(test)]
mod tests;
