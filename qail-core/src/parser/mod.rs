//! QAIL Parser using nom.
//!
//! Parses QAIL syntax into an AST.
//!
//! # Syntax Overview
//!
//! ```text
//! get::users:'id'email [ 'active == true, -created_at, 0..10 ]
//! ─┬─ ─┬─ ─┬────┬──── ─────────────────┬────────────────────
//!  │   │   │    │                      │
//!  │   │   │    │                      └── Unified Block (filters, sorts, ranges)
//!  │   │   │    └── Labels (columns with ')
//!  │   │   │    └── Link (connects to table with :)
//!  │   │   └── Table name
//!  │   └── Gate (action with ::)
//! ```

pub mod tokens;
pub mod columns;
pub mod cages;
pub mod commands;

#[cfg(test)]
mod tests;

use crate::ast::*;
use crate::error::{QailError, QailResult};
use commands::parse_qail_cmd;

/// Parse a complete QAIL query string.
pub fn parse(input: &str) -> QailResult<QailCmd> {
    let input = input.trim();
    
    match parse_qail_cmd(input) {
        Ok(("", cmd)) => Ok(cmd),
        Ok((remaining, _)) => Err(QailError::parse(
            input.len() - remaining.len(),
            format!("Unexpected trailing content: '{}'", remaining),
        )),
        Err(e) => Err(QailError::parse(0, format!("Parse failed: {:?}", e))),
    }
}
