//! Encoding errors for AST to wire protocol conversion.

use std::fmt;

/// Errors that can occur during AST encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    /// A string value contains a literal NULL byte (0x00).
    NullByte,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodeError::NullByte => {
                write!(f, "Value contains NULL byte (0x00) which is invalid in PostgreSQL")
            }
        }
    }
}

impl std::error::Error for EncodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_error_display() {
        let err = EncodeError::NullByte;
        assert!(err.to_string().contains("NULL byte"));
    }
}

