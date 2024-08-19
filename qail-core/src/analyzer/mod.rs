//! Codebase analyzer for migration impact detection.
//!
//! Scans source files for QAIL queries and raw SQL to detect
//! breaking changes before migrations are applied.

mod impact;
mod scanner;

pub use impact::{BreakingChange, MigrationImpact};
pub use scanner::{CodeReference, CodebaseScanner, QueryType};
