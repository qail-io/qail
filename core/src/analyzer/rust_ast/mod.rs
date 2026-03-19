//! Rust semantic analyzer modules.
//!
//! This module provides functionality for analyzing Rust source code,
//! detecting raw SQL patterns, and generating QAIL equivalents.

mod detector;
pub(crate) mod nplus1;
pub mod query_extractor;
pub(crate) mod sql_semantics;
pub mod transformer;
pub mod utils;

pub use detector::{RawSqlMatch, RustAnalyzer, detect_raw_sql, detect_raw_sql_in_file};
pub use nplus1::{
    NPlusOneCode, NPlusOneDiagnostic, NPlusOneSeverity, detect_n_plus_one_in_dir,
    detect_n_plus_one_in_file,
};
#[allow(unused_imports)]
pub use query_extractor::{FetchMethod, QueryCall, SqlType, detect_query_calls};
