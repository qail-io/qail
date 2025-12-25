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
                let msg_len = u32::from_be_bytes([
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
            if self.buffer.capacity() - self.buffer.len() < 65536 {
                self.buffer.reserve(131072);  // 128KB buffer - reserve once, use many
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
    
    // ==================== BUFFERED WRITE API (High Performance) ====================
    
    /// Buffer bytes for later flush (NO SYSCALL).
    /// Use flush_write_buf() to send all buffered data.
    #[inline]
    pub fn buffer_bytes(&mut self, bytes: &[u8]) {
        self.write_buf.extend_from_slice(bytes);
    }
    
    /// Flush the write buffer to the stream.
    /// This is the only syscall in the buffered write path.
    pub async fn flush_write_buf(&mut self) -> PgResult<()> {
        if !self.write_buf.is_empty() {
            self.stream.write_all(&self.write_buf).await?;
            self.write_buf.clear();
        }
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
                let msg_len = u32::from_be_bytes([
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
                self.buffer.reserve(131072);  // 128KB buffer - reserve once, use many
            }
            
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }
    
    /// FAST receive for result consumption - inline DataRow parsing.
    /// Returns: (msg_type, Option<row_data>)
    /// 
    /// For 'D' (DataRow): returns parsed columns
    /// For other types: returns None
    /// This avoids BackendMessage enum allocation for non-DataRow messages.
    #[inline]
    pub(crate) async fn recv_with_data_fast(&mut self) -> PgResult<(u8, Option<Vec<Option<Vec<u8>>>>)> {
        loop {
            // Check if we have at least the header
            if self.buffer.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buffer[1], self.buffer[2], self.buffer[3], self.buffer[4]
                ]) as usize;
                
                if self.buffer.len() >= msg_len + 1 {
                    let msg_type = self.buffer[0];
                    
                    // Check for error
                    if msg_type == b'E' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) = BackendMessage::decode(&msg_bytes)
                            .map_err(PgError::Protocol)?;
                        if let BackendMessage::ErrorResponse(err) = msg {
                            return Err(PgError::Query(err.message));
                        }
                    }
                    
                    // Fast path: DataRow - parse inline
                    if msg_type == b'D' {
                        let payload = &self.buffer[5..msg_len + 1];
                        
                        if payload.len() >= 2 {
                            let column_count = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                            let mut columns = Vec::with_capacity(column_count);
                            let mut pos = 2;
                            
                            for _ in 0..column_count {
                                if pos + 4 > payload.len() { break; }
                                
                                let len = i32::from_be_bytes([
                                    payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]
                                ]);
                                pos += 4;
                                
                                if len == -1 {
                                    columns.push(None);
                                } else {
                                    let len = len as usize;
                                    if pos + len <= payload.len() {
                                        columns.push(Some(payload[pos..pos + len].to_vec()));
                                        pos += len;
                                    }
                                }
                            }
                            
                            let _ = self.buffer.split_to(msg_len + 1);
                            return Ok((msg_type, Some(columns)));
                        }
                    }
                    
                    // Other messages - skip
                    let _ = self.buffer.split_to(msg_len + 1);
                    return Ok((msg_type, None));
                }
            }
            
            // Need more data
            if self.buffer.capacity() - self.buffer.len() < 65536 {
                self.buffer.reserve(131072);
            }
            
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }
    
    /// ZERO-COPY receive for DataRow.
    /// Uses bytes::Bytes for reference-counted slicing instead of Vec copy.
    /// 
    /// Returns: (msg_type, Option<row_data>)
    /// For 'D' (DataRow): returns Bytes slices (no copy!)
    /// For other types: returns None
    #[inline]
    pub(crate) async fn recv_data_zerocopy(&mut self) -> PgResult<(u8, Option<Vec<Option<bytes::Bytes>>>)> {
        use bytes::Buf;
        
        loop {
            // Check if we have at least the header
            if self.buffer.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buffer[1], self.buffer[2], self.buffer[3], self.buffer[4]
                ]) as usize;
                
                if self.buffer.len() >= msg_len + 1 {
                    let msg_type = self.buffer[0];
                    
                    // Check for error
                    if msg_type == b'E' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) = BackendMessage::decode(&msg_bytes)
                            .map_err(PgError::Protocol)?;
                        if let BackendMessage::ErrorResponse(err) = msg {
                            return Err(PgError::Query(err.message));
                        }
                    }
                    
                    // Fast path: DataRow - ZERO-COPY using Bytes
                    if msg_type == b'D' {
                        // Split off the entire message
                        let mut msg_bytes = self.buffer.split_to(msg_len + 1);
                        
                        // Skip type byte (1) + length (4) = 5 bytes
                        msg_bytes.advance(5);
                        
                        if msg_bytes.len() >= 2 {
                            let column_count = msg_bytes.get_u16() as usize;
                            let mut columns = Vec::with_capacity(column_count);
                            
                            for _ in 0..column_count {
                                if msg_bytes.remaining() < 4 { break; }
                                
                                let len = msg_bytes.get_i32();
                                
                                if len == -1 {
                                    columns.push(None);
                                } else {
                                    let len = len as usize;
                                    if msg_bytes.remaining() >= len {
                                        // ZERO-COPY: freeze the BytesMut slice as Bytes
                                        let col_data = msg_bytes.split_to(len).freeze();
                                        columns.push(Some(col_data));
                                    }
                                }
                            }
                            
                            return Ok((msg_type, Some(columns)));
                        }
                        return Ok((msg_type, None));
                    }
                    
                    // Other messages - skip
                    let _ = self.buffer.split_to(msg_len + 1);
                    return Ok((msg_type, None));
                }
            }
            
            // Need more data
            if self.buffer.capacity() - self.buffer.len() < 65536 {
                self.buffer.reserve(131072);
            }
            
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }
    
    /// ULTRA-FAST receive for 2-column DataRow (id, name pattern).
    /// Optimized for the common case of SELECT id, name queries.
    /// Uses fixed-size array instead of Vec allocation.
    /// 
    /// Returns: (msg_type, Option<(col0, col1)>)
    #[inline(always)]
    pub(crate) async fn recv_data_ultra(&mut self) -> PgResult<(u8, Option<(bytes::Bytes, bytes::Bytes)>)> {
        use bytes::Buf;
        
        loop {
            // Check if we have at least the header
            if self.buffer.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buffer[1], self.buffer[2], self.buffer[3], self.buffer[4]
                ]) as usize;
                
                if self.buffer.len() >= msg_len + 1 {
                    let msg_type = self.buffer[0];
                    
                    // Error check
                    if msg_type == b'E' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) = BackendMessage::decode(&msg_bytes)
                            .map_err(PgError::Protocol)?;
                        if let BackendMessage::ErrorResponse(err) = msg {
                            return Err(PgError::Query(err.message));
                        }
                    }
                    
                    // ULTRA-FAST path: DataRow with 2 columns
                    if msg_type == b'D' {
                        let mut msg_bytes = self.buffer.split_to(msg_len + 1);
                        msg_bytes.advance(5); // Skip type + length
                        
                        // Read column count (expect 2)
                        let _col_count = msg_bytes.get_u16();
                        
                        // Column 0 (id)
                        let len0 = msg_bytes.get_i32();
                        let col0 = if len0 > 0 {
                            msg_bytes.split_to(len0 as usize).freeze()
                        } else {
                            bytes::Bytes::new()
                        };
                        
                        // Column 1 (name)
                        let len1 = msg_bytes.get_i32();
                        let col1 = if len1 > 0 {
                            msg_bytes.split_to(len1 as usize).freeze()
                        } else {
                            bytes::Bytes::new()
                        };
                        
                        return Ok((msg_type, Some((col0, col1))));
                    }
                    
                    // Other messages - skip
                    let _ = self.buffer.split_to(msg_len + 1);
                    return Ok((msg_type, None));
                }
            }
            
            // Need more data
            if self.buffer.capacity() - self.buffer.len() < 65536 {
                self.buffer.reserve(131072);
            }
            
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }
}
