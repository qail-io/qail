//! PostgreSQL Connection
//!
//! Low-level TCP connection with wire protocol handling.
//! This is Layer 3 (async I/O).
//!
//! Methods are split across modules for easier maintenance:
//! - `io.rs` - Core I/O (send, recv)
//! - `query.rs` - Query execution
//! - `transaction.rs` - Transaction control
//! - `cursor.rs` - Streaming cursors
//! - `copy.rs` - COPY protocol
//! - `pipeline.rs` - High-performance pipelining
//! - `cancel.rs` - Query cancellation

use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use bytes::BytesMut;
use crate::protocol::{FrontendMessage, BackendMessage, TransactionStatus, ScramClient};
use super::stream::PgStream;
use super::{PgError, PgResult};

/// Initial buffer capacity (64KB for pipeline performance)
pub(crate) const BUFFER_CAPACITY: usize = 65536;

/// SSLRequest message bytes (request code: 80877103)
const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

/// CancelRequest protocol code: 80877102
pub(crate) const CANCEL_REQUEST_CODE: i32 = 80877102;

/// A raw PostgreSQL connection.
pub struct PgConnection {
    pub(crate) stream: PgStream,
    pub(crate) buffer: BytesMut,
    /// Cache of prepared statements: stmt_name -> SQL text
    pub(crate) prepared_statements: HashMap<String, String>,
    /// Backend process ID (for query cancellation)  
    pub(crate) process_id: i32,
    /// Backend secret key (for query cancellation)
    pub(crate) secret_key: i32,
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
        let tcp_stream = TcpStream::connect(&addr).await?;
        
        // Disable Nagle's algorithm for lower latency
        tcp_stream.set_nodelay(true)?;

        let mut conn = Self {
            stream: PgStream::Tcp(tcp_stream),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            prepared_statements: HashMap::new(),
            process_id: 0,
            secret_key: 0,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
        }).await?;

        conn.handle_startup(user, password).await?;

        Ok(conn)
    }

    /// Connect to PostgreSQL server with TLS encryption.
    pub async fn connect_tls(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
        use tokio::io::AsyncReadExt;
        use tokio_rustls::TlsConnector;
        use tokio_rustls::rustls::ClientConfig;
        use tokio_rustls::rustls::pki_types::ServerName;
        
        let addr = format!("{}:{}", host, port);
        let mut tcp_stream = TcpStream::connect(&addr).await?;

        // Send SSLRequest
        tcp_stream.write_all(&SSL_REQUEST).await?;

        // Read response
        let mut response = [0u8; 1];
        tcp_stream.read_exact(&mut response).await?;

        if response[0] != b'S' {
            return Err(PgError::Connection("Server does not support TLS".to_string()));
        }

        // TLS handshake
        let certs = rustls_native_certs::load_native_certs();
        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
        for cert in certs.certs {
            let _ = root_cert_store.add(cert);
        }
        
        let config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        
        let connector = TlsConnector::from(Arc::new(config));
        let server_name = ServerName::try_from(host.to_string())
            .map_err(|_| PgError::Connection("Invalid hostname for TLS".to_string()))?;
        
        let tls_stream = connector.connect(server_name, tcp_stream).await
            .map_err(|e| PgError::Connection(format!("TLS handshake failed: {}", e)))?;

        let mut conn = Self {
            stream: PgStream::Tls(tls_stream),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            prepared_statements: HashMap::new(),
            process_id: 0,
            secret_key: 0,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
        }).await?;

        conn.handle_startup(user, password).await?;

        Ok(conn)
    }

    /// Connect to PostgreSQL server via Unix domain socket.
    #[cfg(unix)]
    pub async fn connect_unix(
        socket_path: &str,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
        use tokio::net::UnixStream;
        
        let unix_stream = UnixStream::connect(socket_path).await?;

        let mut conn = Self {
            stream: PgStream::Unix(unix_stream),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            prepared_statements: HashMap::new(),
            process_id: 0,
            secret_key: 0,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
        }).await?;

        conn.handle_startup(user, password).await?;

        Ok(conn)
    }

    /// Handle startup sequence (auth + params).
    async fn handle_startup(&mut self, user: &str, password: Option<&str>) -> PgResult<()> {
        let mut scram_client: Option<ScramClient> = None;

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::AuthenticationOk => {}
                BackendMessage::AuthenticationMD5Password(_salt) => {
                    return Err(PgError::Auth("MD5 auth not supported. Use SCRAM-SHA-256.".to_string()));
                }
                BackendMessage::AuthenticationSASL(mechanisms) => {
                    let password = password.ok_or_else(|| {
                        PgError::Auth("Password required for SCRAM authentication".to_string())
                    })?;

                    if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                        return Err(PgError::Auth(format!(
                            "Server doesn't support SCRAM-SHA-256. Available: {:?}",
                            mechanisms
                        )));
                    }

                    let client = ScramClient::new(user, password);
                    let first_message = client.client_first_message();

                    self.send(FrontendMessage::SASLInitialResponse {
                        mechanism: "SCRAM-SHA-256".to_string(),
                        data: first_message,
                    }).await?;

                    scram_client = Some(client);
                }
                BackendMessage::AuthenticationSASLContinue(server_data) => {
                    let client = scram_client.as_mut().ok_or_else(|| {
                        PgError::Auth("Received SASL Continue without SASL init".to_string())
                    })?;

                    let final_message = client.process_server_first(&server_data)
                        .map_err(|e| PgError::Auth(format!("SCRAM error: {}", e)))?;

                    self.send(FrontendMessage::SASLResponse(final_message)).await?;
                }
                BackendMessage::AuthenticationSASLFinal(server_signature) => {
                    if let Some(client) = scram_client.as_ref() {
                        client.verify_server_final(&server_signature)
                            .map_err(|e| PgError::Auth(format!("Server verification failed: {}", e)))?;
                    }
                }
                BackendMessage::ParameterStatus { .. } => {}
                BackendMessage::BackendKeyData { process_id, secret_key } => {
                    self.process_id = process_id;
                    self.secret_key = secret_key;
                }
                BackendMessage::ReadyForQuery(TransactionStatus::Idle) |
                BackendMessage::ReadyForQuery(TransactionStatus::InBlock) |
                BackendMessage::ReadyForQuery(TransactionStatus::Failed) => {
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Connection(err.message));
                }
                _ => {}
            }
        }
    }
}

/// Parse affected rows from CommandComplete tag.
pub(crate) fn parse_affected_rows(tag: &str) -> u64 {
    tag.split_whitespace()
        .last()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}
