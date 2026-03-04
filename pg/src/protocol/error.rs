//! Encoding errors for PostgreSQL wire protocol.
//!
//! Shared by `PgEncoder` and `AstEncoder`.

use std::fmt;

use qail_core::ast::Action;

/// Errors that can occur during wire protocol encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    /// A string value contains a literal NULL byte (0x00).
    NullByte,
    /// Too many parameters for the protocol (limit is i16::MAX = 32767).
    TooManyParameters(usize),
    /// A single parameter or message exceeds i32::MAX bytes.
    MessageTooLarge(usize),
    /// Execute `max_rows` must be non-negative (0 means unlimited).
    InvalidMaxRows(i32),
    /// Action not supported by the AST-native encoder (e.g. Listen, Search).
    UnsupportedAction(Action),
    /// A Value::Function/expression contains SQL injection markers.
    UnsafeExpression(String),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodeError::NullByte => {
                write!(
                    f,
                    "Value contains NULL byte (0x00) which is invalid in PostgreSQL"
                )
            }
            EncodeError::TooManyParameters(count) => {
                write!(f, "Too many parameters: {} (Limit is 32767)", count)
            }
            EncodeError::MessageTooLarge(size) => {
                write!(
                    f,
                    "Message too large: {} bytes (Limit is {})",
                    size,
                    i32::MAX
                )
            }
            EncodeError::InvalidMaxRows(v) => {
                write!(f, "Invalid Execute max_rows: {} (must be >= 0)", v)
            }
            EncodeError::UnsupportedAction(action) => {
                write!(f, "Unsupported action {:?} in AST-native encoder", action)
            }
            EncodeError::UnsafeExpression(expr) => {
                write!(f, "Unsafe expression rejected: {}", expr)
            }
        }
    }
}

impl std::error::Error for EncodeError {}
