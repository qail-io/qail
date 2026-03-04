//! Core I/O operations for PostgreSQL connection.
//!
//! This module provides low-level send/receive methods.

use super::{PgConnection, PgError, PgResult, is_ignorable_session_message};
use crate::protocol::{BackendMessage, FrontendMessage, PgEncoder};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MB — prevents OOM from malicious server messages

/// Default read timeout for individual socket reads.
/// Prevents Slowloris DoS where a server sends partial data then goes silent.
const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Default write timeout for individual socket writes/flushes.
/// Prevents indefinitely blocked writes from pinning pool slots.
const DEFAULT_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[inline]
fn parse_data_row_payload_owned(payload: &[u8]) -> PgResult<Vec<Option<Vec<u8>>>> {
    if payload.len() < 2 {
        return Err(PgError::Protocol("DataRow payload too short".into()));
    }

    let raw_count = i16::from_be_bytes([payload[0], payload[1]]);
    if raw_count < 0 {
        return Err(PgError::Protocol(format!(
            "DataRow invalid column count: {}",
            raw_count
        )));
    }
    let column_count = raw_count as usize;
    if column_count > (payload.len() - 2) / 4 + 1 {
        return Err(PgError::Protocol(format!(
            "DataRow claims {} columns but payload is only {} bytes",
            column_count,
            payload.len()
        )));
    }

    let mut columns = Vec::with_capacity(column_count);
    let mut pos = 2;
    for _ in 0..column_count {
        if pos + 4 > payload.len() {
            return Err(PgError::Protocol(
                "DataRow truncated: missing column length".into(),
            ));
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
            continue;
        }
        if len < -1 {
            return Err(PgError::Protocol(format!(
                "DataRow invalid column length: {}",
                len
            )));
        }

        let len = len as usize;
        if len > payload.len().saturating_sub(pos) {
            return Err(PgError::Protocol(
                "DataRow truncated: column data exceeds payload".into(),
            ));
        }
        columns.push(Some(payload[pos..pos + len].to_vec()));
        pos += len;
    }

    if pos != payload.len() {
        return Err(PgError::Protocol("DataRow has trailing bytes".into()));
    }

    Ok(columns)
}

impl PgConnection {
    #[inline]
    pub(crate) fn mark_io_desynced(&mut self) {
        self.io_desynced = true;
    }

    #[inline]
    pub(crate) fn is_io_desynced(&self) -> bool {
        self.io_desynced
    }

    #[inline]
    fn protocol_desync<T>(&mut self, msg: String) -> PgResult<T> {
        self.mark_io_desynced();
        Err(PgError::Protocol(msg))
    }

    #[inline]
    fn connection_desync<T>(&mut self, msg: String) -> PgResult<T> {
        self.mark_io_desynced();
        Err(PgError::Connection(msg))
    }

    /// Send queued statement `Close` messages and drain until `ReadyForQuery`.
    ///
    /// We ignore `26000 prepared statement ... does not exist` because this
    /// can happen after failover or server-side invalidation, and in that case
    /// local state is already being reconciled by retry paths.
    async fn flush_pending_statement_closes(&mut self) -> PgResult<()> {
        if self.draining_statement_closes || self.pending_statement_closes.is_empty() {
            return Ok(());
        }

        self.draining_statement_closes = true;
        let close_names = std::mem::take(&mut self.pending_statement_closes);

        let estimated_payload_len: usize = close_names
            .iter()
            .map(|name| 16usize.saturating_add(name.len()))
            .sum();
        let mut buf = BytesMut::with_capacity(estimated_payload_len.saturating_add(5));
        for stmt_name in &close_names {
            let close_msg = PgEncoder::try_encode_close(false, stmt_name)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            buf.extend_from_slice(&close_msg);
        }
        PgEncoder::encode_sync_to(&mut buf);

        if let Err(err) = self
            .write_all_with_timeout_inner(&buf, "pending statement close write")
            .await
        {
            self.draining_statement_closes = false;
            return Err(err);
        }
        if let Err(err) = self
            .flush_with_timeout("pending statement close flush")
            .await
        {
            self.draining_statement_closes = false;
            return Err(err);
        }

        let mut error: Option<PgError> = None;
        loop {
            let msg = match self.recv().await {
                Ok(msg) => msg,
                Err(err) => {
                    self.draining_statement_closes = false;
                    return Err(err);
                }
            };
            match msg {
                BackendMessage::CloseComplete => {}
                BackendMessage::ReadyForQuery(_) => {
                    self.draining_statement_closes = false;
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err_fields) => {
                    if error.is_none() {
                        let code_26000 = err_fields.code.eq_ignore_ascii_case("26000");
                        let msg_lower = err_fields.message.to_ascii_lowercase();
                        let missing_prepared = msg_lower.contains("prepared statement")
                            && msg_lower.contains("does not exist");
                        if !(code_26000 && missing_prepared) {
                            error = Some(PgError::QueryServer(err_fields.into()));
                        }
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    self.draining_statement_closes = false;
                    return self.protocol_desync(format!(
                        "Unexpected backend message during pending statement close drain: {:?}",
                        other
                    ));
                }
            }
        }
    }

    /// Write all bytes with a timeout guard.
    ///
    /// Prevents stuck kernel send buffers or dead sockets from hanging forever.
    pub(crate) async fn write_all_with_timeout(
        &mut self,
        bytes: &[u8],
        operation: &str,
    ) -> PgResult<()> {
        if !self.draining_statement_closes && !self.pending_statement_closes.is_empty() {
            self.flush_pending_statement_closes().await?;
        }
        self.write_all_with_timeout_inner(bytes, operation).await
    }

    async fn write_all_with_timeout_inner(
        &mut self,
        bytes: &[u8],
        operation: &str,
    ) -> PgResult<()> {
        if bytes.is_empty() {
            return Err(PgError::Encode(
                "refusing to send empty frontend payload".to_string(),
            ));
        }
        use super::stream::PgStream;
        let mut mark_desync = false;
        let result = match &mut self.stream {
            PgStream::Tcp(stream) => {
                match tokio::time::timeout(DEFAULT_WRITE_TIMEOUT, stream.write_all(bytes)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Write error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Timeout(format!(
                            "{} timeout after {:?}",
                            operation, DEFAULT_WRITE_TIMEOUT
                        )))
                    }
                }
            }
            PgStream::Tls(stream) => {
                match tokio::time::timeout(DEFAULT_WRITE_TIMEOUT, stream.write_all(bytes)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Write error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Timeout(format!(
                            "{} timeout after {:?}",
                            operation, DEFAULT_WRITE_TIMEOUT
                        )))
                    }
                }
            }
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            PgStream::Uring(stream) => {
                match tokio::time::timeout(DEFAULT_WRITE_TIMEOUT, stream.write_all(bytes)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Write error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        let _ = stream.abort_inflight();
                        Err(PgError::Timeout(format!(
                            "{} timeout after {:?}",
                            operation, DEFAULT_WRITE_TIMEOUT
                        )))
                    }
                }
            }
            #[cfg(unix)]
            PgStream::Unix(stream) => {
                match tokio::time::timeout(DEFAULT_WRITE_TIMEOUT, stream.write_all(bytes)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Write error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Timeout(format!(
                            "{} timeout after {:?}",
                            operation, DEFAULT_WRITE_TIMEOUT
                        )))
                    }
                }
            }
            #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
            PgStream::GssEnc(stream) => {
                match tokio::time::timeout(DEFAULT_WRITE_TIMEOUT, stream.write_all(bytes)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Write error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Timeout(format!(
                            "{} timeout after {:?}",
                            operation, DEFAULT_WRITE_TIMEOUT
                        )))
                    }
                }
            }
        };
        if mark_desync {
            self.mark_io_desynced();
        }
        result
    }

    /// Flush with a timeout guard.
    pub(crate) async fn flush_with_timeout(&mut self, operation: &str) -> PgResult<()> {
        use super::stream::PgStream;
        let mut mark_desync = false;
        let result = match &mut self.stream {
            PgStream::Tcp(stream) => {
                match tokio::time::timeout(DEFAULT_WRITE_TIMEOUT, stream.flush()).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Flush error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Timeout(format!(
                            "{} timeout after {:?}",
                            operation, DEFAULT_WRITE_TIMEOUT
                        )))
                    }
                }
            }
            PgStream::Tls(stream) => {
                match tokio::time::timeout(DEFAULT_WRITE_TIMEOUT, stream.flush()).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Flush error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Timeout(format!(
                            "{} timeout after {:?}",
                            operation, DEFAULT_WRITE_TIMEOUT
                        )))
                    }
                }
            }
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            PgStream::Uring(stream) => {
                match tokio::time::timeout(DEFAULT_WRITE_TIMEOUT, stream.flush()).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Flush error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        let _ = stream.abort_inflight();
                        Err(PgError::Timeout(format!(
                            "{} timeout after {:?}",
                            operation, DEFAULT_WRITE_TIMEOUT
                        )))
                    }
                }
            }
            #[cfg(unix)]
            PgStream::Unix(stream) => {
                match tokio::time::timeout(DEFAULT_WRITE_TIMEOUT, stream.flush()).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Flush error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Timeout(format!(
                            "{} timeout after {:?}",
                            operation, DEFAULT_WRITE_TIMEOUT
                        )))
                    }
                }
            }
            #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
            PgStream::GssEnc(stream) => {
                match tokio::time::timeout(DEFAULT_WRITE_TIMEOUT, stream.flush()).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Flush error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Timeout(format!(
                            "{} timeout after {:?}",
                            operation, DEFAULT_WRITE_TIMEOUT
                        )))
                    }
                }
            }
        };
        if mark_desync {
            self.mark_io_desynced();
        }
        result
    }

    /// Send a frontend message.
    pub async fn send(&mut self, msg: FrontendMessage) -> PgResult<()> {
        let bytes = msg
            .encode_checked()
            .map_err(|e| PgError::Encode(e.to_string()))?;
        self.write_all_with_timeout(&bytes, "send frontend message")
            .await?;
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

                if msg_len < 4 {
                    return self.protocol_desync(format!(
                        "Invalid message length: {} (minimum 4)",
                        msg_len
                    ));
                }

                if msg_len > MAX_MESSAGE_SIZE {
                    return self.protocol_desync(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    ));
                }

                if self.buffer.len() > msg_len {
                    // We have a complete message - zero-copy split
                    let msg_bytes = self.buffer.split_to(msg_len + 1);
                    let (msg, _) = match BackendMessage::decode(&msg_bytes) {
                        Ok(decoded) => decoded,
                        Err(e) => return self.protocol_desync(e),
                    };

                    // Intercept async notifications — buffer them instead of returning
                    if let BackendMessage::NotificationResponse {
                        process_id,
                        channel,
                        payload,
                    } = msg
                    {
                        self.notifications
                            .push_back(super::notification::Notification {
                                process_id,
                                channel,
                                payload,
                            });
                        continue; // Keep reading for the actual response
                    }

                    return Ok(msg);
                }
            }

            let n = self.read_with_timeout().await?;
            if n == 0 {
                return self.connection_desync("Connection closed".to_string());
            }
        }
    }

    /// Receive a backend message with idle-friendly timeout behavior.
    ///
    /// For long-lived idle streams (e.g. logical replication), an empty
    /// buffer uses no-timeout reads so inactivity does not fail the stream.
    /// If a backend frame is already partially buffered, switch back to the
    /// normal read timeout to fail-closed on partial-frame stalls.
    pub(crate) async fn recv_without_timeout(&mut self) -> PgResult<BackendMessage> {
        loop {
            if self.buffer.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buffer[1],
                    self.buffer[2],
                    self.buffer[3],
                    self.buffer[4],
                ]) as usize;

                if msg_len < 4 {
                    return self.protocol_desync(format!(
                        "Invalid message length: {} (minimum 4)",
                        msg_len
                    ));
                }

                if msg_len > MAX_MESSAGE_SIZE {
                    return self.protocol_desync(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    ));
                }

                if self.buffer.len() > msg_len {
                    let msg_bytes = self.buffer.split_to(msg_len + 1);
                    let (msg, _) = match BackendMessage::decode(&msg_bytes) {
                        Ok(decoded) => decoded,
                        Err(e) => return self.protocol_desync(e),
                    };

                    if let BackendMessage::NotificationResponse {
                        process_id,
                        channel,
                        payload,
                    } = msg
                    {
                        self.notifications
                            .push_back(super::notification::Notification {
                                process_id,
                                channel,
                                payload,
                            });
                        continue;
                    }

                    return Ok(msg);
                }
            }

            let n = if self.buffer.is_empty() {
                self.read_without_timeout().await?
            } else {
                self.read_with_timeout().await?
            };
            if n == 0 {
                return self.connection_desync("Connection closed".to_string());
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

        use super::stream::PgStream;
        let (stream, buffer) = (&mut self.stream, &mut self.buffer);
        let mut mark_desync = false;
        let result = match stream {
            PgStream::Tcp(stream) => {
                match tokio::time::timeout(DEFAULT_READ_TIMEOUT, stream.read_buf(buffer)).await {
                    Ok(Ok(n)) => Ok(n),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Read error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!(
                            "Read timeout after {:?} — possible Slowloris attack or dead connection",
                            DEFAULT_READ_TIMEOUT
                        )))
                    }
                }
            }
            PgStream::Tls(stream) => {
                match tokio::time::timeout(DEFAULT_READ_TIMEOUT, stream.read_buf(buffer)).await {
                    Ok(Ok(n)) => Ok(n),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Read error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!(
                            "Read timeout after {:?} — possible Slowloris attack or dead connection",
                            DEFAULT_READ_TIMEOUT
                        )))
                    }
                }
            }
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            PgStream::Uring(stream) => {
                match tokio::time::timeout(DEFAULT_READ_TIMEOUT, stream.read_into(buffer, 131072))
                    .await
                {
                    Ok(Ok(n)) => Ok(n),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Read error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        let _ = stream.abort_inflight();
                        Err(PgError::Connection(format!(
                            "Read timeout after {:?} — possible Slowloris attack or dead connection",
                            DEFAULT_READ_TIMEOUT
                        )))
                    }
                }
            }
            #[cfg(unix)]
            PgStream::Unix(stream) => {
                match tokio::time::timeout(DEFAULT_READ_TIMEOUT, stream.read_buf(buffer)).await {
                    Ok(Ok(n)) => Ok(n),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Read error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!(
                            "Read timeout after {:?} — possible Slowloris attack or dead connection",
                            DEFAULT_READ_TIMEOUT
                        )))
                    }
                }
            }
            #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
            PgStream::GssEnc(stream) => {
                match tokio::time::timeout(DEFAULT_READ_TIMEOUT, stream.read_buf(buffer)).await {
                    Ok(Ok(n)) => Ok(n),
                    Ok(Err(e)) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!("Read error: {}", e)))
                    }
                    Err(_) => {
                        mark_desync = true;
                        Err(PgError::Connection(format!(
                            "Read timeout after {:?} — possible Slowloris attack or dead connection",
                            DEFAULT_READ_TIMEOUT
                        )))
                    }
                }
            }
        };
        if mark_desync {
            self.mark_io_desynced();
        }
        result
    }

    /// Read from socket without timeout guard.
    ///
    /// Used for long-idle LISTEN/NOTIFY connections.
    pub(crate) async fn read_without_timeout(&mut self) -> PgResult<usize> {
        if self.buffer.capacity() - self.buffer.len() < 65536 {
            self.buffer.reserve(131072);
        }

        use super::stream::PgStream;
        let (stream, buffer) = (&mut self.stream, &mut self.buffer);
        let read_result = match stream {
            PgStream::Tcp(stream) => stream.read_buf(buffer).await,
            PgStream::Tls(stream) => stream.read_buf(buffer).await,
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            PgStream::Uring(stream) => stream.read_into(buffer, 131072).await,
            #[cfg(unix)]
            PgStream::Unix(stream) => stream.read_buf(buffer).await,
            #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
            PgStream::GssEnc(stream) => stream.read_buf(buffer).await,
        };

        match read_result {
            Ok(n) => Ok(n),
            Err(e) => {
                self.mark_io_desynced();
                Err(PgError::Connection(format!("Read error: {}", e)))
            }
        }
    }

    /// Send raw bytes to the stream.
    /// Includes flush for TLS safety — TLS buffers internally and
    /// needs flush to push encrypted data to the underlying TCP socket.
    pub async fn send_bytes(&mut self, bytes: &[u8]) -> PgResult<()> {
        self.write_all_with_timeout(bytes, "send raw bytes").await?;
        self.flush_with_timeout("flush raw bytes").await?;
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
            let payload = std::mem::take(&mut self.write_buf);
            self.write_all_with_timeout(&payload, "flush write buffer")
                .await?;
            self.flush_with_timeout("flush write buffer").await?;
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

                if msg_len < 4 {
                    return self.protocol_desync(format!(
                        "Invalid message length: {} (minimum 4)",
                        msg_len
                    ));
                }

                if msg_len > MAX_MESSAGE_SIZE {
                    return self.protocol_desync(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    ));
                }

                if self.buffer.len() > msg_len {
                    let msg_type = self.buffer[0];

                    if msg_type == b'E' || msg_type == b'A' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) = match BackendMessage::decode(&msg_bytes) {
                            Ok(decoded) => decoded,
                            Err(e) => return self.protocol_desync(e),
                        };
                        match msg {
                            BackendMessage::ErrorResponse(err) => {
                                return Err(PgError::QueryServer(err.into()));
                            }
                            BackendMessage::NotificationResponse {
                                process_id,
                                channel,
                                payload,
                            } => {
                                self.notifications
                                    .push_back(super::notification::Notification {
                                        process_id,
                                        channel,
                                        payload,
                                    });
                                continue;
                            }
                            _ => {
                                return Err(PgError::Protocol(
                                    "Unexpected fast-path message".into(),
                                ));
                            }
                        }
                    }

                    let _ = self.buffer.split_to(msg_len + 1);
                    return Ok(msg_type);
                }
            }

            let n = self.read_with_timeout().await?;
            if n == 0 {
                return self.connection_desync("Connection closed".to_string());
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

                if msg_len < 4 {
                    return self.protocol_desync(format!(
                        "Invalid message length: {} (minimum 4)",
                        msg_len
                    ));
                }

                if msg_len > MAX_MESSAGE_SIZE {
                    return self.protocol_desync(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    ));
                }

                if self.buffer.len() > msg_len {
                    let msg_type = self.buffer[0];

                    if msg_type == b'E' || msg_type == b'A' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) = match BackendMessage::decode(&msg_bytes) {
                            Ok(decoded) => decoded,
                            Err(e) => return self.protocol_desync(e),
                        };
                        match msg {
                            BackendMessage::ErrorResponse(err) => {
                                return Err(PgError::QueryServer(err.into()));
                            }
                            BackendMessage::NotificationResponse {
                                process_id,
                                channel,
                                payload,
                            } => {
                                self.notifications
                                    .push_back(super::notification::Notification {
                                        process_id,
                                        channel,
                                        payload,
                                    });
                                continue;
                            }
                            _ => {
                                return Err(PgError::Protocol(
                                    "Unexpected fast-path message".into(),
                                ));
                            }
                        }
                    }

                    // Fast path: DataRow - parse inline
                    if msg_type == b'D' {
                        let parse_result = {
                            let payload = &self.buffer[5..msg_len + 1];
                            parse_data_row_payload_owned(payload)
                        };

                        let _ = self.buffer.split_to(msg_len + 1);
                        match parse_result {
                            Ok(columns) => return Ok((msg_type, Some(columns))),
                            Err(err) => return Err(err),
                        }
                    }

                    // Other messages - skip
                    let _ = self.buffer.split_to(msg_len + 1);
                    return Ok((msg_type, None));
                }
            }

            let n = self.read_with_timeout().await?;
            if n == 0 {
                return self.connection_desync("Connection closed".to_string());
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

                if msg_len < 4 {
                    return self.protocol_desync(format!(
                        "Invalid message length: {} (minimum 4)",
                        msg_len
                    ));
                }

                if msg_len > MAX_MESSAGE_SIZE {
                    return self.protocol_desync(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    ));
                }

                if self.buffer.len() > msg_len {
                    let msg_type = self.buffer[0];

                    if msg_type == b'E' || msg_type == b'A' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) = match BackendMessage::decode(&msg_bytes) {
                            Ok(decoded) => decoded,
                            Err(e) => return self.protocol_desync(e),
                        };
                        match msg {
                            BackendMessage::ErrorResponse(err) => {
                                return Err(PgError::QueryServer(err.into()));
                            }
                            BackendMessage::NotificationResponse {
                                process_id,
                                channel,
                                payload,
                            } => {
                                self.notifications
                                    .push_back(super::notification::Notification {
                                        process_id,
                                        channel,
                                        payload,
                                    });
                                continue;
                            }
                            _ => {
                                return Err(PgError::Protocol(
                                    "Unexpected fast-path message".into(),
                                ));
                            }
                        }
                    }

                    // Fast path: DataRow - ZERO-COPY using Bytes
                    if msg_type == b'D' {
                        // Split off the entire message
                        let mut msg_bytes = self.buffer.split_to(msg_len + 1);

                        // Skip type byte (1) + length (4) = 5 bytes
                        msg_bytes.advance(5);

                        if msg_bytes.len() >= 2 {
                            let raw_count = msg_bytes.get_i16();
                            if raw_count < 0 {
                                return Err(PgError::Protocol(format!(
                                    "DataRow invalid column count: {}",
                                    raw_count
                                )));
                            }
                            let column_count = raw_count as usize;
                            if column_count > msg_bytes.remaining() / 4 + 1 {
                                return Err(PgError::Protocol(format!(
                                    "DataRow claims {} columns but payload is only {} bytes",
                                    column_count,
                                    msg_bytes.remaining() + 2
                                )));
                            }
                            let mut columns = Vec::with_capacity(column_count);

                            for _ in 0..column_count {
                                if msg_bytes.remaining() < 4 {
                                    return Err(PgError::Protocol(
                                        "DataRow truncated: missing column length".into(),
                                    ));
                                }

                                let len = msg_bytes.get_i32();

                                if len == -1 {
                                    columns.push(None);
                                } else {
                                    if len < -1 {
                                        return Err(PgError::Protocol(format!(
                                            "DataRow invalid column length: {}",
                                            len
                                        )));
                                    }
                                    let len = len as usize;
                                    if msg_bytes.remaining() < len {
                                        return Err(PgError::Protocol(
                                            "DataRow truncated: column data exceeds payload".into(),
                                        ));
                                    }
                                    let col_data = msg_bytes.split_to(len).freeze();
                                    columns.push(Some(col_data));
                                }
                            }

                            if msg_bytes.remaining() != 0 {
                                return Err(PgError::Protocol("DataRow has trailing bytes".into()));
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
                return self.connection_desync("Connection closed".to_string());
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

                if msg_len < 4 {
                    return self.protocol_desync(format!(
                        "Invalid message length: {} (minimum 4)",
                        msg_len
                    ));
                }

                if msg_len > MAX_MESSAGE_SIZE {
                    return self.protocol_desync(format!(
                        "Message too large: {} bytes (max {})",
                        msg_len, MAX_MESSAGE_SIZE
                    ));
                }

                if self.buffer.len() > msg_len {
                    let msg_type = self.buffer[0];

                    // Error and async-notify checks
                    if msg_type == b'E' || msg_type == b'A' {
                        let msg_bytes = self.buffer.split_to(msg_len + 1);
                        let (msg, _) = match BackendMessage::decode(&msg_bytes) {
                            Ok(decoded) => decoded,
                            Err(e) => return self.protocol_desync(e),
                        };
                        match msg {
                            BackendMessage::ErrorResponse(err) => {
                                return Err(PgError::QueryServer(err.into()));
                            }
                            BackendMessage::NotificationResponse {
                                process_id,
                                channel,
                                payload,
                            } => {
                                self.notifications
                                    .push_back(super::notification::Notification {
                                        process_id,
                                        channel,
                                        payload,
                                    });
                                continue;
                            }
                            _ => {
                                return Err(PgError::Protocol(
                                    "Unexpected fast-path message".into(),
                                ));
                            }
                        }
                    }

                    if msg_type == b'D' {
                        let mut msg_bytes = self.buffer.split_to(msg_len + 1);
                        msg_bytes.advance(5); // Skip type + length

                        // Bounds checks to prevent panic on truncated DataRow
                        if msg_bytes.remaining() < 2 {
                            return Err(PgError::Protocol(
                                "DataRow ultra: too short for column count".into(),
                            ));
                        }

                        // Read column count (expect 2)
                        let col_count = msg_bytes.get_i16();
                        if col_count != 2 {
                            return Err(PgError::Protocol(format!(
                                "DataRow ultra expects exactly 2 columns, got {}",
                                col_count
                            )));
                        }

                        if msg_bytes.remaining() < 4 {
                            return Err(PgError::Protocol(
                                "DataRow ultra: truncated before col0 length".into(),
                            ));
                        }
                        let len0 = msg_bytes.get_i32();
                        let col0 = if len0 > 0 {
                            let len0 = len0 as usize;
                            if msg_bytes.remaining() < len0 {
                                return Err(PgError::Protocol(
                                    "DataRow ultra: col0 data exceeds payload".into(),
                                ));
                            }
                            msg_bytes.split_to(len0).freeze()
                        } else if len0 == 0 {
                            bytes::Bytes::new()
                        } else if len0 == -1 {
                            return Err(PgError::Protocol(
                                "DataRow ultra does not support NULL columns".into(),
                            ));
                        } else {
                            return Err(PgError::Protocol(format!(
                                "DataRow ultra: invalid col0 length {}",
                                len0
                            )));
                        };

                        if msg_bytes.remaining() < 4 {
                            return Err(PgError::Protocol(
                                "DataRow ultra: truncated before col1 length".into(),
                            ));
                        }
                        let len1 = msg_bytes.get_i32();
                        let col1 = if len1 > 0 {
                            let len1 = len1 as usize;
                            if msg_bytes.remaining() < len1 {
                                return Err(PgError::Protocol(
                                    "DataRow ultra: col1 data exceeds payload".into(),
                                ));
                            }
                            msg_bytes.split_to(len1).freeze()
                        } else if len1 == 0 {
                            bytes::Bytes::new()
                        } else if len1 == -1 {
                            return Err(PgError::Protocol(
                                "DataRow ultra does not support NULL columns".into(),
                            ));
                        } else {
                            return Err(PgError::Protocol(format!(
                                "DataRow ultra: invalid col1 length {}",
                                len1
                            )));
                        };

                        if msg_bytes.remaining() != 0 {
                            return Err(PgError::Protocol(
                                "DataRow ultra: trailing bytes after expected columns".into(),
                            ));
                        }

                        return Ok((msg_type, Some((col0, col1))));
                    }

                    // Other messages - skip
                    let _ = self.buffer.split_to(msg_len + 1);
                    return Ok((msg_type, None));
                }
            }

            let n = self.read_with_timeout().await?;
            if n == 0 {
                return self.connection_desync("Connection closed".to_string());
            }
        }
    }
}
