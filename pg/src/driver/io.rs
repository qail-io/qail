//! Core I/O operations for PostgreSQL connection.
//!
//! This module provides low-level send/receive methods.

use super::{PgConnection, PgError, PgResult};
use crate::protocol::{BackendMessage, FrontendMessage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB — prevents OOM from malicious server messages

/// Default read timeout for individual socket reads.
/// Prevents Slowloris DoS where a server sends partial data then goes silent.
const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl PgConnection {
    /// Send a frontend message.
    pub async fn send(&mut self, msg: FrontendMessage) -> PgResult<()> {
        let bytes = msg.encode();
        self.stream.write_all(&bytes).await?;
        Ok(())
    }

    /// Loops until a complete message is available.
    /// Automatically buffers NotificationResponse messages for LISTEN/NOTIFY.
    pub async fn recv(&mut self) -> PgResult<BackendMessage> {
        loop {
            // Try to decode from buffer first
            if self.buffer.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buffer[1],
                    self.buffer[2],
                    self.buffer[3],
                    self.buffer[4],
                ]) as usize;

                if msg_len > MAX_MESSAGE_SIZE {
                    return Err(PgError::Protocol(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    )));
                }

                if self.buffer.len() > msg_len {
                    // We have a complete message - zero-copy split
                    let msg_bytes = self.buffer.split_to(msg_len + 1);
                    let (msg, _) = BackendMessage::decode(&msg_bytes).map_err(PgError::Protocol)?;

                    // Intercept async notifications — buffer them instead of returning
                    if let BackendMessage::NotificationResponse { process_id, channel, payload } = msg {
                        self.notifications.push_back(
                            super::notification::Notification { process_id, channel, payload }
                        );
                        continue; // Keep reading for the actual response
                    }

                    return Ok(msg);
                }
            }


            let n = self.read_with_timeout().await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }

    /// Read from the socket with a timeout guard.
    /// Returns the number of bytes read, or an error if the timeout fires.
    /// This prevents Slowloris DoS attacks where a malicious server sends
    /// partial data then goes silent, causing the driver to hang forever.
    #[inline]
    pub(crate) async fn read_with_timeout(&mut self) -> PgResult<usize> {
        if self.buffer.capacity() - self.buffer.len() < 65536 {
            self.buffer.reserve(131072);
        }
        
        match tokio::time::timeout(
            DEFAULT_READ_TIMEOUT,
            self.stream.read_buf(&mut self.buffer),
        ).await {
            Ok(Ok(n)) => Ok(n),
            Ok(Err(e)) => Err(PgError::Connection(format!("Read error: {}", e))),
            Err(_) => Err(PgError::Connection(format!(
                "Read timeout after {:?} — possible Slowloris attack or dead connection",
                DEFAULT_READ_TIMEOUT
            ))),
        }
    }

    /// Send raw bytes to the stream.
    /// Includes flush for TLS safety — TLS buffers internally and
    /// needs flush to push encrypted data to the underlying TCP socket.
    pub async fn send_bytes(&mut self, bytes: &[u8]) -> PgResult<()> {
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    // ==================== BUFFERED WRITE API (High Performance) ====================

    /// Buffer bytes for later flush (NO SYSCALL).
    /// Use flush_write_buf() to send all buffered data.
    #[inline]
    pub fn buffer_bytes(&mut self, bytes: &[u8]) {
        self.write_buf.extend_from_slice(bytes);
    }

    /// Flush the write buffer to the stream (single write_all + flush).
    /// The flush is critical for TLS connections.
    pub async fn flush_write_buf(&mut self) -> PgResult<()> {
        if !self.write_buf.is_empty() {
            self.stream.write_all(&self.write_buf).await?;
            self.write_buf.clear();
            self.stream.flush().await?;
        }
        Ok(())
    }

    /// FAST receive - returns only message type byte, skips parsing.
    /// This is ~10x faster than recv() for pipelining benchmarks.
    /// Returns: message_type
    #[inline]
    pub(crate) async fn recv_msg_type_fast(&mut self) -> PgResult<u8> {
        loop {
            if self.buffer.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buffer[1],
                    self.buffer[2],
                    self.buffer[3],
                    self.buffer[4],
                ]) as usize;

                if msg_len > MAX_MESSAGE_SIZE {
                    return Err(PgError::Protocol(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    )));
                }

                if self.buffer.len() > msg_len {
                    let msg_type = self.buffer[0];

                    if msg_type == b'E' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) =
                            BackendMessage::decode(&msg_bytes).map_err(PgError::Protocol)?;
                        if let BackendMessage::ErrorResponse(err) = msg {
                            return Err(PgError::Query(err.message));
                        }
                    }

                    let _ = self.buffer.split_to(msg_len + 1);
                    return Ok(msg_type);
                }
            }


            let n = self.read_with_timeout().await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }

    /// FAST receive for result consumption - inline DataRow parsing.
    /// Returns: (msg_type, Option<row_data>)
    /// For 'D' (DataRow): returns parsed columns
    /// For other types: returns None
    /// This avoids BackendMessage enum allocation for non-DataRow messages.
    #[inline]
    pub(crate) async fn recv_with_data_fast(
        &mut self,
    ) -> PgResult<(u8, Option<Vec<Option<Vec<u8>>>>)> {
        loop {
            if self.buffer.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buffer[1],
                    self.buffer[2],
                    self.buffer[3],
                    self.buffer[4],
                ]) as usize;

                if msg_len > MAX_MESSAGE_SIZE {
                    return Err(PgError::Protocol(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    )));
                }

                if self.buffer.len() > msg_len {
                    let msg_type = self.buffer[0];

                    if msg_type == b'E' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) =
                            BackendMessage::decode(&msg_bytes).map_err(PgError::Protocol)?;
                        if let BackendMessage::ErrorResponse(err) = msg {
                            return Err(PgError::Query(err.message));
                        }
                    }

                    // Fast path: DataRow - parse inline
                    if msg_type == b'D' {
                        let payload = &self.buffer[5..msg_len + 1];

                        if payload.len() >= 2 {
                            let column_count =
                                u16::from_be_bytes([payload[0], payload[1]]) as usize;
                            let mut columns = Vec::with_capacity(column_count);
                            let mut pos = 2;

                            for _ in 0..column_count {
                                if pos + 4 > payload.len() {
                                    let _ = self.buffer.split_to(msg_len + 1);
                                    return Err(PgError::Protocol("DataRow truncated: missing column length".into()));
                                }

                                let len = i32::from_be_bytes([
                                    payload[pos],
                                    payload[pos + 1],
                                    payload[pos + 2],
                                    payload[pos + 3],
                                ]);
                                pos += 4;

                                if len == -1 {
                                    columns.push(None);
                                } else {
                                    let len = len as usize;
                                    if pos + len > payload.len() {
                                        let _ = self.buffer.split_to(msg_len + 1);
                                        return Err(PgError::Protocol("DataRow truncated: column data exceeds payload".into()));
                                    }
                                    columns.push(Some(payload[pos..pos + len].to_vec()));
                                    pos += len;
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


            let n = self.read_with_timeout().await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }

    /// ZERO-COPY receive for DataRow.
    /// Uses bytes::Bytes for reference-counted slicing instead of Vec copy.
    /// Returns: (msg_type, Option<row_data>)
    /// For 'D' (DataRow): returns Bytes slices (no copy!)
    /// For other types: returns None
    #[inline]
    pub(crate) async fn recv_data_zerocopy(
        &mut self,
    ) -> PgResult<(u8, Option<Vec<Option<bytes::Bytes>>>)> {
        use bytes::Buf;

        loop {
            if self.buffer.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buffer[1],
                    self.buffer[2],
                    self.buffer[3],
                    self.buffer[4],
                ]) as usize;

                if msg_len > MAX_MESSAGE_SIZE {
                    return Err(PgError::Protocol(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    )));
                }

                if self.buffer.len() > msg_len {
                    let msg_type = self.buffer[0];

                    if msg_type == b'E' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) =
                            BackendMessage::decode(&msg_bytes).map_err(PgError::Protocol)?;
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
                                if msg_bytes.remaining() < 4 {
                                    return Err(PgError::Protocol("DataRow truncated: missing column length".into()));
                                }

                                let len = msg_bytes.get_i32();

                                if len == -1 {
                                    columns.push(None);
                                } else {
                                    let len = len as usize;
                                    if msg_bytes.remaining() < len {
                                        return Err(PgError::Protocol("DataRow truncated: column data exceeds payload".into()));
                                    }
                                    let col_data = msg_bytes.split_to(len).freeze();
                                    columns.push(Some(col_data));
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


            let n = self.read_with_timeout().await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }

    /// ULTRA-FAST receive for 2-column DataRow (id, name pattern).
    /// Uses fixed-size array instead of Vec allocation.
    /// Returns: (msg_type, Option<(col0, col1)>)
    #[inline(always)]
    pub(crate) async fn recv_data_ultra(
        &mut self,
    ) -> PgResult<(u8, Option<(bytes::Bytes, bytes::Bytes)>)> {
        use bytes::Buf;

        loop {
            if self.buffer.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buffer[1],
                    self.buffer[2],
                    self.buffer[3],
                    self.buffer[4],
                ]) as usize;

                if msg_len > MAX_MESSAGE_SIZE {
                    return Err(PgError::Protocol(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    )));
                }

                if self.buffer.len() > msg_len {
                    let msg_type = self.buffer[0];

                    // Error check
                    if msg_type == b'E' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) =
                            BackendMessage::decode(&msg_bytes).map_err(PgError::Protocol)?;
                        if let BackendMessage::ErrorResponse(err) = msg {
                            return Err(PgError::Query(err.message));
                        }
                    }

                    if msg_type == b'D' {
                        let mut msg_bytes = self.buffer.split_to(msg_len + 1);
                        msg_bytes.advance(5); // Skip type + length

                        // Bounds checks to prevent panic on truncated DataRow
                        if msg_bytes.remaining() < 2 {
                            return Err(PgError::Protocol("DataRow ultra: too short for column count".into()));
                        }

                        // Read column count (expect 2)
                        let _col_count = msg_bytes.get_u16();

                        if msg_bytes.remaining() < 4 {
                            return Err(PgError::Protocol("DataRow ultra: truncated before col0 length".into()));
                        }
                        let len0 = msg_bytes.get_i32();
                        let col0 = if len0 > 0 {
                            let len0 = len0 as usize;
                            if msg_bytes.remaining() < len0 {
                                return Err(PgError::Protocol("DataRow ultra: col0 data exceeds payload".into()));
                            }
                            msg_bytes.split_to(len0).freeze()
                        } else {
                            bytes::Bytes::new()
                        };

                        if msg_bytes.remaining() < 4 {
                            return Err(PgError::Protocol("DataRow ultra: truncated before col1 length".into()));
                        }
                        let len1 = msg_bytes.get_i32();
                        let col1 = if len1 > 0 {
                            let len1 = len1 as usize;
                            if msg_bytes.remaining() < len1 {
                                return Err(PgError::Protocol("DataRow ultra: col1 data exceeds payload".into()));
                            }
                            msg_bytes.split_to(len1).freeze()
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


            let n = self.read_with_timeout().await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
        }
    }
}
