//! Error types for qail-redis.

use thiserror::Error;

/// Redis driver error types.
#[derive(Debug, Error)]
pub enum RedisError {
    #[error("Connection failed: {0}")]
    Connection(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Redis error: {0}")]
    Redis(String),

    #[error("Timeout")]
    Timeout,

    #[error("Pool exhausted")]
    PoolExhausted,

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("Pool error: {0}")]
    Pool(String),

    #[error("Incomplete data")]
    Incomplete,
}

/// Result type for Redis operations.
pub type RedisResult<T> = Result<T, RedisError>;
