//! PostgreSQL Connection
//!
//! Low-level TCP connection with wire protocol handling.
//! This is Layer 3 (async I/O).

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::protocol::{FrontendMessage, BackendMessage, TransactionStatus};
use super::{PgError, PgResult};

/// A raw PostgreSQL connection.
pub struct PgConnection {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl PgConnection {
    /// Connect to PostgreSQL server.
    pub async fn connect(host: &str, port: u16, user: &str, database: &str) -> PgResult<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr).await?;

        let mut conn = Self {
            stream,
            buffer: Vec::with_capacity(8192),
        };

        // Send startup message
        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
        }).await?;

        // Handle authentication
        conn.handle_startup().await?;

        Ok(conn)
    }

    /// Send a frontend message.
    pub async fn send(&mut self, msg: FrontendMessage) -> PgResult<()> {
        let bytes = msg.encode();
        self.stream.write_all(&bytes).await?;
        Ok(())
    }

    /// Receive backend messages.
    pub async fn recv(&mut self) -> PgResult<BackendMessage> {
        // Read into buffer
        let mut temp = [0u8; 4096];
        let n = self.stream.read(&mut temp).await?;
        if n == 0 {
            return Err(PgError::Connection("Connection closed".to_string()));
        }
        self.buffer.extend_from_slice(&temp[..n]);

        // Decode message
        let (msg, consumed) = BackendMessage::decode(&self.buffer)
            .map_err(PgError::Protocol)?;

        self.buffer.drain(..consumed);
        Ok(msg)
    }

    /// Handle startup sequence (auth + params).
    async fn handle_startup(&mut self) -> PgResult<()> {
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::AuthenticationOk => {
                    // Continue to receive params
                }
                BackendMessage::AuthenticationMD5Password(_salt) => {
                    return Err(PgError::Auth("MD5 auth not implemented yet".to_string()));
                }
                BackendMessage::ParameterStatus { .. } => {
                    // Store server parameters if needed
                }
                BackendMessage::BackendKeyData { .. } => {
                    // Store for cancel requests
                }
                BackendMessage::ReadyForQuery(TransactionStatus::Idle) |
                BackendMessage::ReadyForQuery(TransactionStatus::InBlock) |
                BackendMessage::ReadyForQuery(TransactionStatus::Failed) => {
                    // Connection ready!
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Connection(err.message));
                }
                _ => {}
            }
        }
    }

    /// Execute a simple query and return results.
    pub async fn simple_query(&mut self, sql: &str) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        self.send(FrontendMessage::Query(sql.to_string())).await?;

        let mut rows = Vec::new();

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::RowDescription(_) => {
                    // Column metadata - could store for later
                }
                BackendMessage::DataRow(data) => {
                    rows.push(data);
                }
                BackendMessage::CommandComplete(_) => {
                    // Query done
                }
                BackendMessage::ReadyForQuery(_) => {
                    return Ok(rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }
    }
}
