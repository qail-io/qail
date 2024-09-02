//! Rust AST analyzer modules.
//!
//! This module provides functionality for analyzing Rust source code,
//! detecting raw SQL patterns, and generating QAIL equivalents.

mod detector;
pub mod query_extractor;  // Extract query patterns for migration
pub mod transformer;      // SQL to QAIL transformation
pub mod utils;

// Re-export public types and functions from detector
pub use detector::{detect_raw_sql, detect_raw_sql_in_file, RawSqlMatch, RustAnalyzer};
#[allow(unused_imports)]
pub use query_extractor::{detect_query_calls, QueryCall};
