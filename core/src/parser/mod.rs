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

/// Maximum Qail input length (64 KB).
/// Prevents stack overflow via deeply nested parenthesized expressions
/// (the recursive descent parser has no depth limit, but 64KB is insufficient
/// to encode enough nesting to blow the stack while being generous for any
/// legitimate query).
const MAX_INPUT_LENGTH: usize = 64 * 1024;

/// Parse a complete QAIL query string (v2 syntax only).
/// Uses keyword-based syntax: `get table fields * where col = value`
/// Also supports shorthand: `get table[filter]` desugars to `get table where filter`
pub fn parse(input: &str) -> QailResult<Qail> {
    let input = input.trim();

    // R8-A: Reject oversized inputs before recursive descent to prevent stack overflow
    if input.len() > MAX_INPUT_LENGTH {
        return Err(QailError::parse(
            0,
            format!(
                "Input too large: {} bytes (max {} bytes)",
                input.len(),
                MAX_INPUT_LENGTH,
            ),
        ));
    }

    // Use grammar::parse which handles comment stripping + [filter] desugaring
    match grammar::parse(input) {
        Ok(cmd) => Ok(cmd),
        Err(e) => Err(QailError::parse(0, e)),
    }
}
