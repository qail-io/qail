//! Error types for Qdrant driver.

use std::fmt;

/// Result type for Qdrant operations.
pub type QdrantResult<T> = Result<T, QdrantError>;

/// Errors that can occur during Qdrant operations.
#[derive(Debug)]
pub enum QdrantError {
    /// Connection failed.
    Connection(String),
    /// gRPC error.
    Grpc(String),
    /// Collection not found.
    CollectionNotFound(String),
    /// Point not found.
    PointNotFound(String),
    /// Invalid vector dimension.
    DimensionMismatch { expected: usize, got: usize },
    /// Encoding error.
    Encode(String),
    /// Decode error.
    Decode(String),
    /// Timeout.
    Timeout,
}

impl fmt::Display for QdrantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QdrantError::Connection(msg) => write!(f, "Connection error: {}", msg),
            QdrantError::Grpc(msg) => write!(f, "gRPC error: {}", msg),
            QdrantError::CollectionNotFound(name) => write!(f, "Collection not found: {}", name),
            QdrantError::PointNotFound(id) => write!(f, "Point not found: {}", id),
            QdrantError::DimensionMismatch { expected, got } => {
                write!(f, "Vector dimension mismatch: expected {}, got {}", expected, got)
            }
            QdrantError::Encode(msg) => write!(f, "Encode error: {}", msg),
            QdrantError::Decode(msg) => write!(f, "Decode error: {}", msg),
            QdrantError::Timeout => write!(f, "Operation timed out"),
        }
    }
}

impl std::error::Error for QdrantError {}
