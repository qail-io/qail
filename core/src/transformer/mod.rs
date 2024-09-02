//! Scalable SQL to QAIL Transformer
//!
//! This module provides a pattern-based transformer architecture that:
//! - Supports multiple SQL patterns (SELECT, INSERT, UPDATE, DELETE, JOIN, CTE)
//! - Extracts structured data from SQL AST
//! - Generates QAIL code for multiple target languages
//!
//! ## Architecture
//!
//! ```text
//! SQL String → Parser → Statement AST → Pattern Matcher → PatternData → Target Emitter → QAIL Code
//! ```

mod traits;
mod patterns;
mod clauses;
mod registry;

pub use traits::*;
pub use patterns::*;
pub use clauses::*;
pub use registry::*;
