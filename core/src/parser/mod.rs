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
///
/// Uses keyword-based syntax: `get table fields * where col = value`
pub fn parse(input: &str) -> QailResult<Qail> {
    let input = input.trim();

    match grammar::parse_root(input) {
        Ok(("", cmd)) => Ok(cmd),
        Ok((remaining, _)) => Err(QailError::parse(
            input.len() - remaining.len(),
            format!("Unexpected trailing content: '{}'", remaining),
        )),
        Err(e) => Err(QailError::parse(0, format!("Parse failed: {:?}", e))),
    }
}
