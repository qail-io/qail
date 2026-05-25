//! LISTEN/NOTIFY support for PostgreSQL connections.
//!
//! PostgreSQL sends `NotificationResponse` messages asynchronously when
//! a channel the connection is LISTENing on receives a NOTIFY.
//!
//! This module provides:
//! - `Notification` struct — channel name + payload
//! - `listen()` / `unlisten()` — subscribe/unsubscribe to channels
//! - `poll_notifications()` — drain buffered notifications (non-blocking)
//! - `recv_notification()` — block-wait for the next notification

use super::{
    PgConnection, PgError, PgResult, io::MAX_MESSAGE_SIZE, is_ignorable_session_message,
    unexpected_backend_message,
};
use crate::protocol::PgEncoder;

/// A notification received from PostgreSQL LISTEN/NOTIFY.
#[derive(Debug, Clone)]
pub struct Notification {
    /// The PID of the notifying backend process
    pub process_id: i32,
    /// The channel name
    pub channel: String,
    /// The payload (may be empty)
    pub payload: String,
}

#[inline]
fn return_with_desync<T>(conn: &mut PgConnection, err: PgError) -> PgResult<T> {
    if matches!(
        err,
        PgError::Protocol(_) | PgError::Connection(_) | PgError::Timeout(_)
    ) {
        conn.mark_io_desynced();
    }
    Err(err)
}

impl PgConnection {
    /// Subscribe to a notification channel.
    ///
    /// ```ignore
    /// conn.listen("price_calendar_changed").await?;
    /// ```
    pub async fn listen(&mut self, channel: &str) -> PgResult<()> {
        // Channel names are identifiers, quote them to prevent injection
        let sql = format!("LISTEN \"{}\"", channel.replace('"', "\"\""));
        self.execute_simple(&sql).await
    }

    /// Unsubscribe from a notification channel.
    pub async fn unlisten(&mut self, channel: &str) -> PgResult<()> {
        let sql = format!("UNLISTEN \"{}\"", channel.replace('"', "\"\""));
        self.execute_simple(&sql).await
    }

    /// Unsubscribe from all notification channels.
    pub async fn unlisten_all(&mut self) -> PgResult<()> {
        self.execute_simple("UNLISTEN *").await
    }

    /// Drain all buffered notifications without blocking.
    ///
    /// Notifications arrive asynchronously from PostgreSQL and are buffered
    /// whenever `recv()` encounters a `NotificationResponse`. This method
    /// returns all currently buffered notifications.
    pub fn poll_notifications(&mut self) -> Vec<Notification> {
        self.notifications.drain(..).collect()
    }

    /// Wait for the next notification, blocking until one arrives.
    ///
    /// Unlike `recv()`, this does NOT use the 30-second Slowloris timeout
    /// guard. LISTEN connections idle for long periods — that's normal,
    /// not a DoS attack.
    ///
    /// Useful for a dedicated LISTEN connection in a background task.
    pub async fn recv_notification(&mut self) -> PgResult<Notification> {
        use crate::protocol::BackendMessage;

        // Return buffered notification immediately if available
        if let Some(n) = self.notifications.pop_front() {
            return Ok(n);
        }

        // Send empty query to flush any pending notifications from server
        let bytes = PgEncoder::try_encode_query_string("")?;
        self.write_all_with_timeout(&bytes, "stream write").await?;

        // Read messages — use recv() for the initial empty query response
        // (which completes quickly), then switch to no-timeout reads
        let mut got_ready = false;
        loop {
            // Try to decode from the existing buffer first
            if self.buffer.len() >= 5 {
                let msg_len = u32::from_be_bytes([
                    self.buffer[1],
                    self.buffer[2],
                    self.buffer[3],
                    self.buffer[4],
                ]) as usize;

                if msg_len < 4 {
                    return return_with_desync(
                        self,
                        PgError::Protocol(format!(
                            "Invalid message length: {} (minimum 4)",
                            msg_len
                        )),
                    );
                }

                if msg_len > MAX_MESSAGE_SIZE {
                    return return_with_desync(
                        self,
                        PgError::Protocol(format!(
                            "Message too large: {} bytes (max {})",
                            msg_len, MAX_MESSAGE_SIZE
                        )),
                    );
                }

                if self.buffer.len() > msg_len {
                    let msg_bytes = self.buffer.split_to(msg_len + 1);
                    let (msg, _) = match BackendMessage::decode(&msg_bytes) {
                        Ok(decoded) => decoded,
                        Err(err) => return return_with_desync(self, PgError::Protocol(err)),
                    };

                    match msg {
                        BackendMessage::NotificationResponse {
                            process_id,
                            channel,
                            payload,
                        } => {
                            return Ok(Notification {
                                process_id,
                                channel,
                                payload,
                            });
                        }
                        BackendMessage::EmptyQueryResponse => continue,
                        BackendMessage::NoticeResponse(_) => continue,
                        BackendMessage::ParameterStatus { .. } => continue,
                        BackendMessage::CommandComplete(_) => continue,
                        BackendMessage::ReadyForQuery(_) => {
                            got_ready = true;
                            // Check buffer for notifications that arrived with this batch
                            if let Some(n) = self.notifications.pop_front() {
                                return Ok(n);
                            }
                            continue;
                        }
                        BackendMessage::ErrorResponse(err) => {
                            return Err(PgError::QueryServer(err.into()));
                        }
                        msg if is_ignorable_session_message(&msg) => continue,
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_message("listen/notify wait", &other),
                            );
                        }
                    }
                }
            }

            // Read from socket — use tokio read (no timeout!) if we've
            // already gotten ReadyForQuery (now we're just waiting for NOTIFY)
            if self.buffer.capacity() - self.buffer.len() < 65536 {
                self.buffer.reserve(131072);
            }

            if got_ready {
                // LISTEN connections can stay idle for hours (empty buffer),
                // but a partially buffered backend frame should still timeout
                // to fail-closed on slowloris-style partial writes.
                let n = if self.buffer.is_empty() {
                    self.read_without_timeout().await?
                } else {
                    self.read_with_timeout().await?
                };
                if n == 0 {
                    return return_with_desync(
                        self,
                        PgError::Connection("Connection closed".to_string()),
                    );
                }
            } else {
                // Initial flush — use the normal timeout to avoid hanging
                // if the server is unresponsive during the empty query
                let n = self.read_with_timeout().await?;
                if n == 0 {
                    return return_with_desync(
                        self,
                        PgError::Connection("Connection closed".to_string()),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::return_with_desync;
    use crate::driver::{PgConnection, PgError};

    #[cfg(unix)]
    fn test_conn() -> PgConnection {
        use crate::driver::connection::StatementCache;
        use crate::driver::stream::PgStream;
        use bytes::BytesMut;
        use std::collections::{HashMap, VecDeque};
        use std::num::NonZeroUsize;
        use tokio::net::UnixStream;

        let (unix_stream, _peer) = UnixStream::pair().expect("unix stream pair");
        PgConnection {
            stream: PgStream::Unix(unix_stream),
            buffer: BytesMut::with_capacity(1024),
            write_buf: BytesMut::with_capacity(1024),
            sql_buf: BytesMut::with_capacity(256),
            params_buf: Vec::new(),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(NonZeroUsize::new(2).expect("non-zero")),
            column_info_cache: HashMap::new(),
            process_id: 0,
            cancel_key_bytes: Vec::new(),
            requested_protocol_minor: PgConnection::default_protocol_minor(),
            negotiated_protocol_minor: PgConnection::default_protocol_minor(),
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled: false,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn notification_return_with_desync_marks_protocol_error() {
        let mut conn = test_conn();

        let err =
            return_with_desync::<()>(&mut conn, PgError::Protocol("bad notify frame".to_string()))
                .expect_err("protocol error must be returned");

        assert!(err.to_string().contains("bad notify frame"));
        assert!(conn.is_io_desynced());
    }
}
