//! Codebase analyzer for migration impact detection.
//!
//! Scans source files for QAIL queries and raw SQL to detect
//! breaking changes before migrations are applied.
//!
//! Supports tiered analysis:
//! - Rust files: semantic extraction plus semantic N+1 analysis
//! - Other files: Regex-based scanning

mod impact;
pub mod rust_ast; // Public for LSP access to query_extractor
mod scanner;

pub use impact::{BreakingChange, MigrationImpact};
pub use rust_ast::{QueryCall, detect_query_calls};
pub use rust_ast::{RawSqlMatch, RustAnalyzer, detect_raw_sql, detect_raw_sql_in_file};
pub use scanner::{
    AnalysisMode, CodeReference, CodebaseScanner, FileAnalysis, QueryType, ScanResult,
};
// N+1 detection
pub use rust_ast::{
    NPlusOneCode, NPlusOneDiagnostic, NPlusOneSeverity, detect_n_plus_one_in_dir,
    detect_n_plus_one_in_file,
};
