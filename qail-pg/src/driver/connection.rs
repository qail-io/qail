//! PostgreSQL Connection
//!
//! Low-level TCP connection with wire protocol handling.
//! This is Layer 3 (async I/O).

use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::BytesMut;
use crate::protocol::{FrontendMessage, BackendMessage, TransactionStatus, ScramClient, PgEncoder};
use super::stream::PgStream;
use super::{PgError, PgResult};

/// Initial buffer capacity (8KB - typical response size)
const BUFFER_CAPACITY: usize = 8192;

/// SSLRequest message bytes (request code: 80877103)
const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

/// A raw PostgreSQL connection.
pub struct PgConnection {
    stream: PgStream,
    buffer: BytesMut,
    /// Cache of prepared statements: stmt_name -> SQL text (for debugging)
    prepared_statements: HashMap<String, String>,
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

        let mut conn = Self {
            stream: PgStream::Tcp(tcp_stream),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            prepared_statements: HashMap::new(),
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

    /// Connect to PostgreSQL server with TLS encryption.
    ///
    /// This method:
    /// 1. Connects via TCP
    /// 2. Sends SSLRequest
    /// 3. If server accepts ('S'), performs TLS handshake
    /// 4. Continues with normal startup over encrypted connection
    pub async fn connect_tls(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
        use tokio_rustls::TlsConnector;
        use tokio_rustls::rustls::ClientConfig;
        use tokio_rustls::rustls::pki_types::ServerName;
        
        let addr = format!("{}:{}", host, port);
        let mut tcp_stream = TcpStream::connect(&addr).await?;

        // Step 1: Send SSLRequest
        tcp_stream.write_all(&SSL_REQUEST).await?;

        // Step 2: Read server response (single byte: 'S' or 'N')
        let mut response = [0u8; 1];
        tcp_stream.read_exact(&mut response).await?;

        if response[0] != b'S' {
            return Err(PgError::Connection(
                "Server does not support TLS".to_string()
            ));
        }

        // Step 3: Perform TLS handshake
        let certs = rustls_native_certs::load_native_certs();
        
        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
        for cert in certs.certs {
            let _ = root_cert_store.add(cert); // Ignore invalid certs
        }
        
        let config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        
        let connector = TlsConnector::from(Arc::new(config));
        let server_name = ServerName::try_from(host.to_string())
            .map_err(|_| PgError::Connection("Invalid hostname for TLS".to_string()))?;
        
        let tls_stream = connector.connect(server_name, tcp_stream).await
            .map_err(|e| PgError::Connection(format!("TLS handshake failed: {}", e)))?;

        // Step 4: Create connection with TLS stream
        let mut conn = Self {
            stream: PgStream::Tls(tls_stream),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            prepared_statements: HashMap::new(),
        };

        // Send startup message over TLS
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
                    // We have a complete message - zero-copy split
                    let msg_bytes = self.buffer.split_to(msg_len + 1);
                    let (msg, _) = BackendMessage::decode(&msg_bytes)
                        .map_err(PgError::Protocol)?;
                    return Ok(msg);
                }
            }
            
            // Need more data - read directly into BytesMut (no temp buffer)
            // Reserve space if needed
            if self.buffer.capacity() - self.buffer.len() < 4096 {
                self.buffer.reserve(4096);
            }
            
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                return Err(PgError::Connection("Connection closed".to_string()));
            }
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

    /// Execute a query with binary parameters.
    ///
    /// This uses the Extended Query Protocol (Parse/Bind/Execute/Sync):
    /// - Parameters are sent as binary bytes, skipping the string layer
    /// - No SQL injection possible - parameters are never interpolated
    /// - Better performance via prepared statement reuse
    ///
    /// # Example
    /// ```ignore
    /// let rows = conn.query(
    ///     "SELECT * FROM users WHERE name = $1",
    ///     &[Some(b"Alice".to_vec())]
    /// ).await?;
    /// ```
    pub async fn query(
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

    /// Execute a query with cached prepared statement.
    ///
    /// Like `query()`, but reuses prepared statements across calls.
    /// The statement name is derived from a hash of the SQL text.
    /// 
    /// Benefits:
    /// - Skip parsing on repeated queries (faster execution)
    /// - Better query plan caching in PostgreSQL
    ///
    /// # Example
    /// ```ignore
    /// // First call: Parse + Bind + Execute + Sync
    /// let rows1 = conn.query_cached("SELECT * FROM users WHERE id = $1", &[params]).await?;
    /// 
    /// // Second call: Bind + Execute + Sync (no Parse needed!)
    /// let rows2 = conn.query_cached("SELECT * FROM users WHERE id = $1", &[params]).await?;
    /// ```
    pub async fn query_cached(
        &mut self, 
        sql: &str, 
        params: &[Option<Vec<u8>>]
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        // Generate statement name from SQL hash
        let stmt_name = Self::sql_to_stmt_name(sql);
        let is_new = !self.prepared_statements.contains_key(&stmt_name);
        
        // Build the message: Parse (if new) + Bind + Execute + Sync
        let mut buf = BytesMut::new();
        
        if is_new {
            // Parse statement with name
            buf.extend(PgEncoder::encode_parse(&stmt_name, sql, &[]));
        }
        
        // Bind to named statement
        buf.extend(PgEncoder::encode_bind("", &stmt_name, params));
        // Execute
        buf.extend(PgEncoder::encode_execute("", 0));
        // Sync
        buf.extend(PgEncoder::encode_sync());
        
        self.stream.write_all(&buf).await?;
        
        let mut rows = Vec::new();
        let sql_owned = sql.to_string(); // For storing in cache
        
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::ParseComplete => {
                    // Statement parsed - store with SQL text for debugging
                    self.prepared_statements.insert(stmt_name.clone(), sql_owned.clone());
                }
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    rows.push(data);
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::NoData => {}
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

    /// Generate a statement name from SQL hash.
    /// Uses a simple hash to create a unique name like "stmt_12345abc".
    fn sql_to_stmt_name(sql: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        format!("s{:016x}", hasher.finish())
    }

    /// Execute a mutation and return affected rows count.
    ///
    /// Parses the affected rows from CommandComplete tag:
    /// - "INSERT 0 1" → 1
    /// - "UPDATE 5" → 5
    /// - "DELETE 10" → 10
    pub async fn execute_raw(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>]
    ) -> PgResult<u64> {
        use crate::protocol::PgEncoder;
        
        // Send Parse + Bind + Execute + Sync as one pipeline
        let bytes = PgEncoder::encode_extended_query(sql, params);
        self.stream.write_all(&bytes).await?;

        let mut affected_rows = 0u64;

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::NoData => {}
                BackendMessage::DataRow(_) => {
                    // Mutations might return rows (RETURNING clause)
                }
                BackendMessage::CommandComplete(tag) => {
                    // Parse affected rows from tag: "INSERT 0 1", "UPDATE 5", "DELETE 10"
                    affected_rows = parse_affected_rows(&tag);
                }
                BackendMessage::ReadyForQuery(_) => {
                    return Ok(affected_rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }
    }

    // ==================== Transaction Support ====================

    /// Begin a new transaction.
    ///
    /// After calling this, all queries run within the transaction
    /// until `commit()` or `rollback()` is called.
    ///
    /// # Example
    /// ```ignore
    /// conn.begin_transaction().await?;
    /// conn.execute_raw("INSERT INTO users (name) VALUES ($1)", &[Some(b"Alice".to_vec())]).await?;
    /// conn.execute_raw("INSERT INTO profiles (user_id) VALUES ($1)", &[Some(b"1".to_vec())]).await?;
    /// conn.commit().await?;  // Both inserts succeed or neither do
    /// ```
    pub async fn begin_transaction(&mut self) -> PgResult<()> {
        self.execute_simple("BEGIN").await
    }

    /// Commit the current transaction.
    ///
    /// Makes all changes since `begin_transaction()` permanent.
    pub async fn commit(&mut self) -> PgResult<()> {
        self.execute_simple("COMMIT").await
    }

    /// Rollback the current transaction.
    ///
    /// Discards all changes since `begin_transaction()`.
    pub async fn rollback(&mut self) -> PgResult<()> {
        self.execute_simple("ROLLBACK").await
    }

    /// Execute a simple SQL statement (no parameters).
    /// Used internally for transaction control.
    async fn execute_simple(&mut self, sql: &str) -> PgResult<()> {
        let bytes = PgEncoder::encode_query_string(sql);
        self.stream.write_all(&bytes).await?;

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::ReadyForQuery(_) => {
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }
    }

    /// Execute multiple queries in a single network round-trip (PIPELINING).
    ///
    /// This sends all queries at once, then reads all responses.
    /// Reduces N queries from N round-trips to 1 round-trip.
    ///
    /// # Example
    /// ```ignore
    /// let results = conn.query_pipeline(&[
    ///     ("SELECT * FROM users WHERE id = $1", &[Some(b"1".to_vec())]),
    ///     ("SELECT * FROM orders WHERE user_id = $1", &[Some(b"1".to_vec())]),
    /// ]).await?;
    /// ```
    pub async fn query_pipeline(
        &mut self,
        queries: &[(&str, &[Option<Vec<u8>>])]
    ) -> PgResult<Vec<Vec<Vec<Option<Vec<u8>>>>>> {
        use bytes::BytesMut;
        use crate::protocol::PgEncoder;
        
        // Encode all queries into a single buffer
        let mut buf = BytesMut::new();
        for (sql, params) in queries {
            buf.extend(PgEncoder::encode_extended_query(sql, params));
        }
        
        // Send all queries in ONE write
        self.stream.write_all(&buf).await?;
        
        // Collect all results
        let mut all_results: Vec<Vec<Vec<Option<Vec<u8>>>>> = Vec::with_capacity(queries.len());
        let mut current_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut queries_completed = 0;
        
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::ParseComplete | BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    current_rows.push(data);
                }
                BackendMessage::CommandComplete(_) => {
                    // One query finished - save results and reset
                    all_results.push(std::mem::take(&mut current_rows));
                    queries_completed += 1;
                }
                BackendMessage::NoData => {
                    // Non-SELECT query completed
                    all_results.push(Vec::new());
                    queries_completed += 1;
                }
                BackendMessage::ReadyForQuery(_) => {
                    // All queries done
                    if queries_completed == queries.len() {
                        return Ok(all_results);
                    }
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

/// Parse affected rows from CommandComplete tag.
/// Examples: "INSERT 0 1" → 1, "UPDATE 5" → 5, "DELETE 10" → 10
fn parse_affected_rows(tag: &str) -> u64 {
    // Format is: "COMMAND [OID] ROWS" where OID is only for INSERT
    // Examples: "INSERT 0 5", "UPDATE 5", "DELETE 10", "SELECT 100"
    tag.split_whitespace()
        .last()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}
