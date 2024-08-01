//! Codebase analyzer for migration impact detection.
//!
//! Scans source files for QAIL queries and raw SQL to detect
//! breaking changes before migrations are applied.

mod scanner;
mod impact;

pub use scanner::{CodebaseScanner, CodeReference, QueryType};
pub use impact::{MigrationImpact, BreakingChange};
