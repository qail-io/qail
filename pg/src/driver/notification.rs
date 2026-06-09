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
                            let notification = Notification {
                                process_id,
                                channel,
                                payload,
                            };
                            if got_ready {
                                return Ok(notification);
                            }
                            self.notifications.push_back(notification);
                            continue;
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
                            self.mark_io_desynced();
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
    fn test_conn_with_peer() -> (PgConnection, tokio::net::UnixStream) {
        use crate::driver::connection::StatementCache;
        use crate::driver::stream::PgStream;
        use bytes::BytesMut;
        use std::collections::{HashMap, VecDeque};
        use std::num::NonZeroUsize;
        use tokio::net::UnixStream;

        let (unix_stream, peer) = UnixStream::pair().expect("unix stream pair");
        (
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
            },
            peer,
        )
    }

    #[cfg(unix)]
    fn test_conn() -> PgConnection {
        test_conn_with_peer().0
    }

    #[cfg(unix)]
    fn push_backend_frame(conn: &mut PgConnection, msg_type: u8, payload: &[u8]) {
        conn.buffer.extend_from_slice(&[msg_type]);
        conn.buffer
            .extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        conn.buffer.extend_from_slice(payload);
    }

    #[cfg(unix)]
    fn notification_payload(process_id: i32, channel: &str, payload: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&process_id.to_be_bytes());
        bytes.extend_from_slice(channel.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(payload.as_bytes());
        bytes.push(0);
        bytes
    }

    fn error_response_payload(code: &str, message: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'C');
        payload.extend_from_slice(code.as_bytes());
        payload.push(0);
        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);
        payload.push(0);
        payload
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

    #[cfg(unix)]
    #[tokio::test]
    async fn recv_notification_drains_empty_query_before_returning_pre_ready_notify() {
        let (mut conn, _peer) = test_conn_with_peer();
        let payload = notification_payload(42, "jobs", "ready");

        push_backend_frame(&mut conn, b'A', &payload);
        push_backend_frame(&mut conn, b'I', &[]);
        push_backend_frame(&mut conn, b'Z', b"I");

        let notification = conn
            .recv_notification()
            .await
            .expect("pre-ready notification should be returned after flush drain");

        assert_eq!(notification.process_id, 42);
        assert_eq!(notification.channel, "jobs");
        assert_eq!(notification.payload, "ready");
        assert!(
            conn.buffer.is_empty(),
            "empty-query flush frames must not remain buffered"
        );
        assert!(!conn.is_io_desynced());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recv_notification_error_response_marks_connection_desynced() {
        let (mut conn, _peer) = test_conn_with_peer();
        let payload = error_response_payload("XX000", "notify wait failed");
        push_backend_frame(&mut conn, b'E', &payload);

        let err = conn
            .recv_notification()
            .await
            .expect_err("server error must fail");

        assert!(matches!(err, PgError::QueryServer(_)));
        assert!(conn.is_io_desynced());
    }
}
