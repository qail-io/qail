//! High-level Redis driver using unified QAIL AST.
//!
//! # Example
//! ```ignore
//! use qail_redis::RedisDriver;
//! use qail_core::prelude::*;
//!
//! let mut driver = RedisDriver::connect("localhost", 6379).await?;
//!
//! // Using Qail AST
//! driver.execute(&Qail::redis_set("key", b"value".to_vec())).await?;
//! let val = driver.execute(&Qail::redis_get("key")).await?;
//! ```

use qail_core::ast::Qail;

use crate::error::{RedisError, RedisResult};
use crate::transport::Transport;
use crate::value::Value;

/// High-level Redis driver with connection management.
pub struct RedisDriver {
    transport: Transport,
    resp3: bool,
}

impl RedisDriver {
    /// Connect to Redis server.
    pub async fn connect(host: &str, port: u16) -> RedisResult<Self> {
        let mut transport = Transport::connect(host, port).await?;

        // Upgrade to RESP3
        let resp3 = transport.upgrade_to_resp3().await.unwrap_or(false);

        Ok(Self { transport, resp3 })
    }

    /// Execute a Qail command.
    pub async fn execute(&mut self, cmd: &Qail) -> RedisResult<Value> {
        self.transport.execute(cmd).await
    }

    /// Check if connection is using RESP3.
    pub fn is_resp3(&self) -> bool {
        self.resp3
    }

    // ========== Convenience Methods ==========

    /// GET key
    pub async fn get(&mut self, key: &str) -> RedisResult<Option<Vec<u8>>> {
        let cmd = Qail::redis_get(key);
        match self.execute(&cmd).await? {
            Value::Null => Ok(None),
            Value::Bulk(data) => Ok(Some(data)),
            Value::String(s) => Ok(Some(s.into_bytes())),
            other => Err(RedisError::Protocol(format!(
                "Unexpected GET response: {:?}",
                other
            ))),
        }
    }

    /// GET key as string
    pub async fn get_str(&mut self, key: &str) -> RedisResult<Option<String>> {
        match self.get(key).await? {
            Some(bytes) => Ok(Some(
                String::from_utf8(bytes)
                    .map_err(|e| RedisError::Protocol(e.to_string()))?,
            )),
            None => Ok(None),
        }
    }

    /// SET key value
    pub async fn set(&mut self, key: &str, value: &[u8]) -> RedisResult<()> {
        let cmd = Qail::redis_set(key, value.to_vec());
        self.execute(&cmd).await?;
        Ok(())
    }

    /// SET key value EX seconds
    pub async fn set_ex(&mut self, key: &str, value: &[u8], seconds: i64) -> RedisResult<()> {
        use crate::cmd::RedisExt;
        let cmd = Qail::redis_set(key, value.to_vec()).redis_ex(seconds);
        self.execute(&cmd).await?;
        Ok(())
    }

    /// DEL key
    pub async fn del(&mut self, key: &str) -> RedisResult<i64> {
        let cmd = Qail::redis_del(key);
        match self.execute(&cmd).await? {
            Value::Integer(n) => Ok(n),
            other => Err(RedisError::Protocol(format!(
                "Unexpected DEL response: {:?}",
                other
            ))),
        }
    }

    /// INCR key
    pub async fn incr(&mut self, key: &str) -> RedisResult<i64> {
        let cmd = Qail::redis_incr(key);
        match self.execute(&cmd).await? {
            Value::Integer(n) => Ok(n),
            other => Err(RedisError::Protocol(format!(
                "Unexpected INCR response: {:?}",
                other
            ))),
        }
    }

    /// TTL key
    pub async fn ttl(&mut self, key: &str) -> RedisResult<i64> {
        let cmd = Qail::redis_ttl(key);
        match self.execute(&cmd).await? {
            Value::Integer(n) => Ok(n),
            other => Err(RedisError::Protocol(format!(
                "Unexpected TTL response: {:?}",
                other
            ))),
        }
    }

    /// EXPIRE key seconds
    pub async fn expire(&mut self, key: &str, seconds: i64) -> RedisResult<bool> {
        let cmd = Qail::redis_expire(key, seconds);
        match self.execute(&cmd).await? {
            Value::Integer(1) => Ok(true),
            Value::Integer(0) => Ok(false),
            other => Err(RedisError::Protocol(format!(
                "Unexpected EXPIRE response: {:?}",
                other
            ))),
        }
    }

    /// EXISTS key
    pub async fn exists(&mut self, key: &str) -> RedisResult<bool> {
        let cmd = Qail::redis_exists(key);
        match self.execute(&cmd).await? {
            Value::Integer(n) => Ok(n > 0),
            other => Err(RedisError::Protocol(format!(
                "Unexpected EXISTS response: {:?}",
                other
            ))),
        }
    }

    /// PING
    pub async fn ping(&mut self) -> RedisResult<bool> {
        let cmd = Qail::redis_ping();
        match self.execute(&cmd).await? {
            Value::String(s) if s == "PONG" => Ok(true),
            Value::Bulk(b) if b == b"PONG" => Ok(true),
            _ => Ok(false),
        }
    }
}
