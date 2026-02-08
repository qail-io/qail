//! QAIL Parser using nom.
//!
//! Parses QAIL v2 keyword-based syntax into an AST.
//!
//! # Syntax Overview
//!
//! ```text
//! get users
//! fields id, email
//! where active = true
//! order by created_at desc
//! limit 10
//! ```

pub mod grammar;
pub mod query_file;
pub mod schema;

#[cfg(test)]
mod tests;

use crate::ast::*;
use crate::error::{QailError, QailResult};

/// Parse a complete QAIL query string (v2 syntax only).
/// Uses keyword-based syntax: `get table fields * where col = value`
/// Also supports shorthand: `get table[filter]` desugars to `get table where filter`
pub fn parse(input: &str) -> QailResult<Qail> {
    let input = input.trim();

    // Use grammar::parse which handles comment stripping + [filter] desugaring
    match grammar::parse(input) {
        Ok(cmd) => Ok(cmd),
        Err(e) => Err(QailError::parse(0, e)),
    }
}
