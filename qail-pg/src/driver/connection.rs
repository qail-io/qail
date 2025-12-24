//! PostgreSQL Connection
//!
//! Low-level TCP connection with wire protocol handling.
//! This is Layer 3 (async I/O).

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::protocol::{FrontendMessage, BackendMessage, TransactionStatus, ScramClient};
use super::{PgError, PgResult};

/// A raw PostgreSQL connection.
pub struct PgConnection {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl PgConnection {
    /// Connect to PostgreSQL server without authentication (trust mode).
    pub async fn connect(host: &str, port: u16, user: &str, database: &str) -> PgResult<Self> {
        Self::connect_with_password(host, port, user, database, None).await
    }

    /// Connect to PostgreSQL server with optional password authentication.
    pub async fn connect_with_password(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
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
        conn.handle_startup(user, password).await?;

        Ok(conn)
    }

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
                    // We have a complete message
                    let (msg, consumed) = BackendMessage::decode(&self.buffer)
                        .map_err(PgError::Protocol)?;
                    self.buffer.drain(..consumed);
                    return Ok(msg);
                }
            }
            
            // Need more data - read from stream
            let mut temp = [0u8; 4096];
            let n = self.stream.read(&mut temp).await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
            self.buffer.extend_from_slice(&temp[..n]);
        }
    }

    /// Handle startup sequence (auth + params).
    async fn handle_startup(&mut self, user: &str, password: Option<&str>) -> PgResult<()> {
        let mut scram_client: Option<ScramClient> = None;

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::AuthenticationOk => {
                    // Authentication successful, continue to receive params
                }
                BackendMessage::AuthenticationMD5Password(_salt) => {
                    return Err(PgError::Auth("MD5 auth not supported. Use SCRAM-SHA-256.".to_string()));
                }
                BackendMessage::AuthenticationSASL(mechanisms) => {
                    // SCRAM-SHA-256 authentication requested
                    let password = password.ok_or_else(|| {
                        PgError::Auth("Password required for SCRAM authentication".to_string())
                    })?;

                    if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                        return Err(PgError::Auth(format!(
                            "Server doesn't support SCRAM-SHA-256. Available: {:?}",
                            mechanisms
                        )));
                    }

                    // Initialize SCRAM client
                    let client = ScramClient::new(user, password);
                    let first_message = client.client_first_message();

                    // Send SASL initial response
                    self.send(FrontendMessage::SASLInitialResponse {
                        mechanism: "SCRAM-SHA-256".to_string(),
                        data: first_message,
                    }).await?;

                    scram_client = Some(client);
                }
                BackendMessage::AuthenticationSASLContinue(server_data) => {
                    // Process server challenge and send final response
                    let client = scram_client.as_mut().ok_or_else(|| {
                        PgError::Auth("Received SASL Continue without SASL init".to_string())
                    })?;

                    let final_message = client.process_server_first(&server_data)
                        .map_err(|e| PgError::Auth(format!("SCRAM error: {}", e)))?;

                    self.send(FrontendMessage::SASLResponse(final_message)).await?;
                }
                BackendMessage::AuthenticationSASLFinal(server_signature) => {
                    // Verify server signature
                    if let Some(client) = scram_client.as_ref() {
                        client.verify_server_final(&server_signature)
                            .map_err(|e| PgError::Auth(format!("Server verification failed: {}", e)))?;
                    }
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

    /// Execute an extended query with binary parameters.
    ///
    /// This uses the Parse/Bind/Execute protocol:
    /// 1. Parse: Send SQL template with $1, $2, etc. placeholders
    /// 2. Bind: Send parameter values as binary bytes
    /// 3. Execute: Run the query
    /// 4. Sync: End the pipeline
    ///
    /// Parameters skip the string layer - they're sent as raw bytes.
    pub async fn extended_query(
        &mut self, 
        sql: &str, 
        params: &[Option<Vec<u8>>]
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        use crate::protocol::PgEncoder;
        
        // Send Parse + Bind + Execute + Sync as one pipeline
        let bytes = PgEncoder::encode_extended_query(sql, params);
        self.stream.write_all(&bytes).await?;

        let mut rows = Vec::new();

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::ParseComplete => {
                    // Parse succeeded
                }
                BackendMessage::BindComplete => {
                    // Bind succeeded
                }
                BackendMessage::RowDescription(_) => {
                    // Column metadata
                }
                BackendMessage::DataRow(data) => {
                    rows.push(data);
                }
                BackendMessage::CommandComplete(_) => {
                    // Query done
                }
                BackendMessage::NoData => {
                    // No rows (for non-SELECT)
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

    /// Send raw bytes to the stream.
    pub async fn send_bytes(&mut self, bytes: &[u8]) -> PgResult<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }
}
