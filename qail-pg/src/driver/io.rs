//! Core I/O operations for PostgreSQL connection.
//!
//! This module provides low-level send/receive methods.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::protocol::{FrontendMessage, BackendMessage};
use super::{PgConnection, PgError, PgResult};

impl PgConnection {
    /// Send a frontend message.
    pub async fn send(&mut self, msg: FrontendMessage) -> PgResult<()> {
        let bytes = msg.encode();
        self.stream.write_all(&bytes).await?;
        Ok(())
    }

    /// Receive backend messages.
    /// Loops until a complete message is available.
    pub async fn recv(&mut self) -> PgResult<BackendMessage> {
        loop {
            // Try to decode from buffer first
            if self.buffer.len() >= 5 {
                let msg_len = i32::from_be_bytes([
                    self.buffer[1], self.buffer[2], self.buffer[3], self.buffer[4]
                ]) as usize;
                
                if self.buffer.len() >= msg_len + 1 {
                    // We have a complete message - zero-copy split
                    let msg_bytes = self.buffer.split_to(msg_len + 1);
                    let (msg, _) = BackendMessage::decode(&msg_bytes)
                        .map_err(PgError::Protocol)?;
                    return Ok(msg);
                }
            }
            
            // Need more data - read directly into BytesMut (no temp buffer)
            // Reserve space if needed
            if self.buffer.capacity() - self.buffer.len() < 32768 {
                self.buffer.reserve(32768);  // 32KB buffer
            }
            
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }

    /// Send raw bytes to the stream.
    pub async fn send_bytes(&mut self, bytes: &[u8]) -> PgResult<()> {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;  // CRITICAL: Must flush for PostgreSQL to process!
        Ok(())
    }
    
    /// FAST receive - returns only message type byte, skips parsing.
    /// This is ~10x faster than recv() for pipelining benchmarks.
    /// Returns: message_type
    #[inline]
    pub(crate) async fn recv_msg_type_fast(&mut self) -> PgResult<u8> {
        loop {
            // Check if we have at least the header
            if self.buffer.len() >= 5 {
                let msg_len = i32::from_be_bytes([
                    self.buffer[1], self.buffer[2], self.buffer[3], self.buffer[4]
                ]) as usize;
                
                if self.buffer.len() >= msg_len + 1 {
                    // Get message type, then skip the whole message
                    let msg_type = self.buffer[0];
                    
                    // Check for error before discarding
                    if msg_type == b'E' {
                        // Parse error properly
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) = BackendMessage::decode(&msg_bytes)
                            .map_err(PgError::Protocol)?;
                        if let BackendMessage::ErrorResponse(err) = msg {
                            return Err(PgError::Query(err.message));
                        }
                    }
                    
                    // Skip the message (no parsing!)
                    let _ = self.buffer.split_to(msg_len + 1);
                    return Ok(msg_type);
                }
            }
            
            // Need more data - use large buffer to reduce syscalls
            if self.buffer.capacity() - self.buffer.len() < 65536 {
                self.buffer.reserve(65536);  // 64KB buffer for maximum pipeline throughput
            }
            
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }
}
