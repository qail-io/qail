//! Async TCP transport for Redis RESP3 protocol.
//!
//! Handles raw TCP connection and RESP3 message exchange.

use bytes::BytesMut;
use qail_core::ast::Qail;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::decoder;
use crate::encoder;
use crate::error::{RedisError, RedisResult};
use crate::value::Value;

/// Low-level Redis transport over TCP.
pub struct Transport {
    stream: TcpStream,
    read_buf: BytesMut,
}

impl Transport {
    /// Connect to Redis server.
    pub async fn connect(host: &str, port: u16) -> RedisResult<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(RedisError::Io)?;

        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(4096),
        })
    }

    /// Execute a Qail Redis command and return the response.
    pub async fn execute(&mut self, cmd: &Qail) -> RedisResult<Value> {
        let bytes = encoder::encode(cmd);

        self.stream
            .write_all(&bytes)
            .await
            .map_err(RedisError::Io)?;

        // Read response
        self.read_response().await
    }

    /// Send raw bytes and read response.
    pub async fn execute_raw(&mut self, bytes: &[u8]) -> RedisResult<Value> {
        self.stream
            .write_all(bytes)
            .await
            .map_err(RedisError::Io)?;

        self.read_response().await
    }

    /// Read a RESP3 response from the connection.
    async fn read_response(&mut self) -> RedisResult<Value> {
        loop {
            // Try to decode from existing buffer
            if !self.read_buf.is_empty() {
                match decoder::decode(&self.read_buf) {
                    Ok((value, consumed)) => {
                        let _ = self.read_buf.split_to(consumed);
                        return Ok(value);
                    }
                    Err(RedisError::Incomplete) => {
                        // Need more data
                    }
                    Err(e) => return Err(e),
                }
            }

            // Read more data
            let n = self.stream
                .read_buf(&mut self.read_buf)
                .await
                .map_err(RedisError::Io)?;

            if n == 0 {
                return Err(RedisError::Connection("Connection closed".into()));
            }
        }
    }

    /// Upgrade connection to RESP3.
    pub async fn upgrade_to_resp3(&mut self) -> RedisResult<bool> {
        let mut buf = BytesMut::with_capacity(64);
        encoder::encode_hello(&mut buf, 3);

        self.execute_raw(&buf).await?;
        Ok(true)
    }
}
