//! Codebase analyzer for migration impact detection.
//!
//! Scans source files for QAIL queries and raw SQL to detect
//! breaking changes before migrations are applied.
//!
//! Supports tiered analysis:
//! - Rust files: Full AST parsing with `syn` (100% accurate)
//! - Other files: Regex-based scanning (90% accurate)

mod impact;
mod rust_ast;
mod scanner;

pub use impact::{BreakingChange, MigrationImpact};
pub use rust_ast::{detect_raw_sql, detect_raw_sql_in_file, RawSqlMatch, RustAnalyzer};
pub use scanner::{AnalysisMode, CodeReference, CodebaseScanner, FileAnalysis, QueryType, ScanResult};
