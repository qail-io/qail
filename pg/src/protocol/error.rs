//! Encoding errors for PostgreSQL wire protocol.
//!
//! Shared by `PgEncoder` and `AstEncoder`.

use std::fmt;

/// Errors that can occur during wire protocol encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    /// A string value contains a literal NULL byte (0x00).
    NullByte,
    /// Too many parameters for the protocol (limit is i16::MAX = 32767).
    TooManyParameters(usize),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodeError::NullByte => {
                write!(f, "Value contains NULL byte (0x00) which is invalid in PostgreSQL")
            }
            EncodeError::TooManyParameters(count) => {
                write!(f, "Too many parameters: {} (Limit is 32767)", count)
            }
        }
    }
}

impl std::error::Error for EncodeError {}
