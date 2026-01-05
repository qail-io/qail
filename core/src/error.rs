//! Error types for QAIL.

use thiserror::Error;

/// Error types for QAIL operations.
#[derive(Debug, Error)]
pub enum QailError {
    /// Failed to parse the QAIL query string.
    #[error("Parse error at position {position}: {message}")]
    Parse {
        /// Byte offset of the error.
        position: usize,
        /// Human-readable error message.
        message: String,
    },

    /// Invalid action (must be get, set, del, or add).
    #[error("Invalid action: '{0}'. Expected: get, set, del, or add")]
    InvalidAction(String),

    /// Required syntax symbol is missing.
    #[error("Missing required symbol: {symbol} ({description})")]
    MissingSymbol {
        /// The missing symbol.
        symbol: &'static str,
        /// Description of the expected symbol.
        description: &'static str,
    },

    /// Invalid operator in expression.
    #[error("Invalid operator: '{0}'")]
    InvalidOperator(String),

    /// Invalid value in expression.
    #[error("Invalid value: {0}")]
    InvalidValue(String),

    /// Database-layer error.
    #[error("Database error: {0}")]
    Database(String),

    /// Connection-layer error.
    #[error("Connection error: {0}")]
    Connection(String),

    /// Execution-layer error.
    #[error("Execution error: {0}")]
    Execution(String),

    /// Validation error.
    #[error("Validation error: {0}")]
    Validation(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// I/O error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl QailError {
    /// Create a parse error at the given position.
    pub fn parse(position: usize, message: impl Into<String>) -> Self {
        Self::Parse {
            position,
            message: message.into(),
        }
    }

    /// Create a missing symbol error.
    pub fn missing(symbol: &'static str, description: &'static str) -> Self {
        Self::MissingSymbol {
            symbol,
            description,
        }
    }
}

/// Result type alias for QAIL operations.
pub type QailResult<T> = Result<T, QailError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = QailError::parse(5, "unexpected character");
        assert_eq!(
            err.to_string(),
            "Parse error at position 5: unexpected character"
        );
    }
}
