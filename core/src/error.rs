//! Error types for QAIL.

/// Error types for QAIL operations.
#[derive(Debug)]
pub enum QailError {
    /// Failed to parse the QAIL query string.
    Parse {
        /// Byte offset of the error.
        position: usize,
        /// Human-readable error message.
        message: String,
    },

    /// Invalid action (must be get, set, del, or add).
    InvalidAction(String),

    /// Required syntax symbol is missing.
    MissingSymbol {
        /// The missing symbol.
        symbol: &'static str,
        /// Description of the expected symbol.
        description: &'static str,
    },

    /// Invalid operator in expression.
    InvalidOperator(String),

    /// Invalid value in expression.
    InvalidValue(String),

    /// Database-layer error.
    Database(String),

    /// Connection-layer error.
    Connection(String),

    /// Execution-layer error.
    Execution(String),

    /// Validation error.
    Validation(String),

    /// Configuration error.
    Config(String),

    /// I/O error.
    Io(std::io::Error),
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

impl std::fmt::Display for QailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse { position, message } => {
                write!(f, "Parse error at position {position}: {message}")
            }
            Self::InvalidAction(action) => {
                write!(
                    f,
                    "Invalid action: '{action}'. Expected: get, set, del, or add"
                )
            }
            Self::MissingSymbol {
                symbol,
                description,
            } => {
                write!(f, "Missing required symbol: {symbol} ({description})")
            }
            Self::InvalidOperator(op) => write!(f, "Invalid operator: '{op}'"),
            Self::InvalidValue(value) => write!(f, "Invalid value: {value}"),
            Self::Database(msg) => write!(f, "Database error: {msg}"),
            Self::Connection(msg) => write!(f, "Connection error: {msg}"),
            Self::Execution(msg) => write!(f, "Execution error: {msg}"),
            Self::Validation(msg) => write!(f, "Validation error: {msg}"),
            Self::Config(msg) => write!(f, "Configuration error: {msg}"),
            Self::Io(err) => write!(f, "IO error: {err}"),
        }
    }
}

impl std::error::Error for QailError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for QailError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
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
