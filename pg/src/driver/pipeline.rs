//! High-performance pipelining methods for PostgreSQL connection.
//!
//!
//! Performance hierarchy (fastest to slowest):
//! 1. `pipeline_ast_cached` - Parse once, Bind+Execute many (275k q/s)
//! 2. `pipeline_simple_bytes_fast` - Pre-encoded simple query
//! 3. `pipeline_bytes_fast` - Pre-encoded extended query
//! 4. `pipeline_simple_fast` - Simple query protocol (~99k q/s)
//! 5. `pipeline_ast_fast` - Fast extended query, count only
//! 6. `pipeline_ast` - Full results collection
//! 7. `query_pipeline` - SQL-based pipelining

use super::{PgConnection, PgError, PgResult};
use crate::protocol::{AstEncoder, BackendMessage, PgEncoder};
use bytes::BytesMut;
use tokio::io::AsyncWriteExt;

impl PgConnection {
    /// Execute multiple SQL queries in a single network round-trip (PIPELINING).
    ///
    /// This sends all queries at once, then reads all responses.
    /// Reduces N queries from N round-trips to 1 round-trip.
    pub async fn query_pipeline(
        &mut self,
        queries: &[(&str, &[Option<Vec<u8>>])],
    ) -> PgResult<Vec<Vec<Vec<Option<Vec<u8>>>>>> {
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
                    all_results.push(std::mem::take(&mut current_rows));
                    queries_completed += 1;
                }
                BackendMessage::NoData => {
                    all_results.push(Vec::new());
                    queries_completed += 1;
                }
                BackendMessage::ReadyForQuery(_) => {
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

    /// Execute multiple Qail ASTs in a single network round-trip.
    ///
    pub async fn pipeline_ast(
        &mut self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<Vec<Vec<Vec<Option<Vec<u8>>>>>> {
        let buf = AstEncoder::encode_batch(cmds);
        self.stream.write_all(&buf).await?;

        let mut all_results: Vec<Vec<Vec<Option<Vec<u8>>>>> = Vec::with_capacity(cmds.len());
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
                    all_results.push(std::mem::take(&mut current_rows));
                    queries_completed += 1;
                }
                BackendMessage::NoData => {
                    all_results.push(Vec::new());
                    queries_completed += 1;
                }
                BackendMessage::ReadyForQuery(_) => {
                    if queries_completed == cmds.len() {
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

    /// FAST AST pipeline - returns only query count, no result parsing.
    pub async fn pipeline_ast_fast(&mut self, cmds: &[qail_core::ast::Qail]) -> PgResult<usize> {
        let buf = AstEncoder::encode_batch(cmds);

        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        let mut queries_completed = 0;

        loop {
            let msg_type = self.recv_msg_type_fast().await?;
            match msg_type {
                b'C' | b'n' => queries_completed += 1,
                b'Z' => {
                    if queries_completed == cmds.len() {
                        return Ok(queries_completed);
                    }
                }
                _ => {}
            }
        }
    }

    /// FASTEST extended query pipeline - takes pre-encoded wire bytes.
    #[inline]
    pub async fn pipeline_bytes_fast(
        &mut self,
        wire_bytes: &[u8],
        expected_queries: usize,
    ) -> PgResult<usize> {
        self.stream.write_all(wire_bytes).await?;
        self.stream.flush().await?;

        let mut queries_completed = 0;

        loop {
            let msg_type = self.recv_msg_type_fast().await?;
            match msg_type {
                b'C' | b'n' => queries_completed += 1,
                b'Z' => {
                    if queries_completed == expected_queries {
                        return Ok(queries_completed);
                    }
                }
                _ => {}
            }
        }
    }

    /// Simple query protocol pipeline - uses 'Q' message.
    #[inline]
    pub async fn pipeline_simple_fast(
        &mut self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<usize> {
        let buf = AstEncoder::encode_batch_simple(cmds);

        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        let mut queries_completed = 0;

        loop {
            let msg_type = self.recv_msg_type_fast().await?;
            match msg_type {
                b'C' => queries_completed += 1,
                b'Z' => {
                    if queries_completed == cmds.len() {
                        return Ok(queries_completed);
                    }
                }
                _ => {}
            }
        }
    }

    /// FASTEST simple query pipeline - takes pre-encoded bytes.
    #[inline]
    pub async fn pipeline_simple_bytes_fast(
        &mut self,
        wire_bytes: &[u8],
        expected_queries: usize,
    ) -> PgResult<usize> {
        self.stream.write_all(wire_bytes).await?;
        self.stream.flush().await?;

        let mut queries_completed = 0;

        loop {
            let msg_type = self.recv_msg_type_fast().await?;
            match msg_type {
                b'C' => queries_completed += 1,
                b'Z' => {
                    if queries_completed == expected_queries {
                        return Ok(queries_completed);
                    }
                }
                _ => {}
            }
        }
    }

    /// CACHED PREPARED STATEMENT pipeline - Parse once, Bind+Execute many.
    ///
    /// This achieves ~280k q/s by:
    /// 1. Generate SQL template with $1, $2, etc. placeholders
    /// 2. Parse template ONCE (cached in PostgreSQL)
    /// 3. Send Bind+Execute for each instance (params differ per query)
    #[inline]
    pub async fn pipeline_ast_cached(
        &mut self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<usize> {
        if cmds.is_empty() {
            return Ok(0);
        }

        let mut buf = BytesMut::with_capacity(cmds.len() * 64);

        for cmd in cmds {
            let (sql, params) = AstEncoder::encode_cmd_sql(cmd);
            let stmt_name = Self::sql_to_stmt_name(&sql);

            if !self.prepared_statements.contains_key(&stmt_name) {
                buf.extend(PgEncoder::encode_parse(&stmt_name, &sql, &[]));
                self.prepared_statements.insert(stmt_name.clone(), sql);
            }

            buf.extend(PgEncoder::encode_bind("", &stmt_name, &params));
            buf.extend(PgEncoder::encode_execute("", 0));
        }

        buf.extend(PgEncoder::encode_sync());

        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        let mut queries_completed = 0;

        loop {
            let msg_type = self.recv_msg_type_fast().await?;
            match msg_type {
                b'C' | b'n' => queries_completed += 1,
                b'Z' => {
                    if queries_completed == cmds.len() {
                        return Ok(queries_completed);
                    }
                }
                _ => {}
            }
        }
    }

    /// ZERO-LOOKUP prepared statement pipeline.
    ///
    /// Uses pre-computed PreparedStatement handle to eliminate:
    /// - Hash computation per query
    /// - HashMap lookup per query
    /// - String allocation for stmt_name
    ///
    /// This is the fastest possible path for repeated identical queries.
    ///
    /// # Example
    /// ```ignore
    /// // Prepare once (outside timing loop):
    /// let stmt = PreparedStatement::from_sql("SELECT id, name FROM harbors LIMIT $1");
    /// let params_batch: Vec<Vec<Option<Vec<u8>>>> = (1..=1000)
    ///     .map(|i| vec![Some(i.to_string().into_bytes())])
    ///     .collect();
    ///
    /// // Execute many (no hash, no lookup!):
    /// conn.pipeline_prepared_fast(&stmt, &params_batch).await?;
    /// ```
    #[inline]
    pub async fn pipeline_prepared_fast(
        &mut self,
        stmt: &super::PreparedStatement,
        params_batch: &[Vec<Option<Vec<u8>>>],
    ) -> PgResult<usize> {
        if params_batch.is_empty() {
            return Ok(0);
        }

        // Local buffer - faster than reusing connection buffer
        let mut buf = BytesMut::with_capacity(params_batch.len() * 64);

        let is_new = !self.prepared_statements.contains_key(&stmt.name);

        if is_new {
            return Err(PgError::Query(
                "Statement not prepared. Call prepare() first.".to_string(),
            ));
        }

        // ZERO ALLOCATION: write directly to local buffer
        for params in params_batch {
            PgEncoder::encode_bind_to(&mut buf, &stmt.name, params);
            PgEncoder::encode_execute_to(&mut buf);
        }

        PgEncoder::encode_sync_to(&mut buf);

        // Write and flush
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        let mut queries_completed = 0;

        loop {
            let msg_type = self.recv_msg_type_fast().await?;
            match msg_type {
                b'C' | b'n' => queries_completed += 1,
                b'Z' => {
                    if queries_completed == params_batch.len() {
                        return Ok(queries_completed);
                    }
                }
                _ => {}
            }
        }
    }

    /// Prepare a statement and return a handle for fast execution.
    ///
    /// This registers the statement with PostgreSQL and returns a
    /// PreparedStatement handle for use with pipeline_prepared_fast.
    pub async fn prepare(&mut self, sql: &str) -> PgResult<super::PreparedStatement> {
        use super::prepared::sql_bytes_to_stmt_name;

        let stmt_name = sql_bytes_to_stmt_name(sql.as_bytes());

        if !self.prepared_statements.contains_key(&stmt_name) {
            // Send Parse + Sync
            let mut buf = BytesMut::with_capacity(sql.len() + 32);
            buf.extend(PgEncoder::encode_parse(&stmt_name, sql, &[]));
            buf.extend(PgEncoder::encode_sync());

            self.stream.write_all(&buf).await?;
            self.stream.flush().await?;

            // Wait for ParseComplete
            loop {
                let msg_type = self.recv_msg_type_fast().await?;
                match msg_type {
                    b'1' => {
                        // ParseComplete
                        self.prepared_statements
                            .insert(stmt_name.clone(), sql.to_string());
                    }
                    b'Z' => break, // ReadyForQuery
                    _ => {}
                }
            }
        }

        Ok(super::PreparedStatement {
            name: stmt_name,
            param_count: sql.matches('$').count(),
        })
    }

    /// Execute a prepared statement pipeline and return actual results.
    ///
    /// Unlike `pipeline_prepared_fast` which only counts, this method
    /// parses and returns all row data. Use for fair benchmarking with
    ///
    /// Returns: Vec of query results, each containing Vec of rows,
    /// where each row is Vec<Option<Vec<u8>>> (column values).
    pub async fn pipeline_prepared_results(
        &mut self,
        stmt: &super::PreparedStatement,
        params_batch: &[Vec<Option<Vec<u8>>>],
    ) -> PgResult<Vec<Vec<Vec<Option<Vec<u8>>>>>> {
        if params_batch.is_empty() {
            return Ok(Vec::new());
        }

        if !self.prepared_statements.contains_key(&stmt.name) {
            return Err(PgError::Query(
                "Statement not prepared. Call prepare() first.".to_string(),
            ));
        }

        let mut buf = BytesMut::with_capacity(params_batch.len() * 64);

        for params in params_batch {
            PgEncoder::encode_bind_to(&mut buf, &stmt.name, params);
            PgEncoder::encode_execute_to(&mut buf);
        }

        PgEncoder::encode_sync_to(&mut buf);

        // Write and flush
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        // Collect results using fast inline DataRow parsing
        let mut all_results: Vec<Vec<Vec<Option<Vec<u8>>>>> =
            Vec::with_capacity(params_batch.len());
        let mut current_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();

        loop {
            let (msg_type, data) = self.recv_with_data_fast().await?;

            match msg_type {
                b'2' => {} // BindComplete
                b'T' => {} // RowDescription
                b'D' => {
                    // DataRow
                    if let Some(row) = data {
                        current_rows.push(row);
                    }
                }
                b'C' => {
                    // CommandComplete
                    all_results.push(std::mem::take(&mut current_rows));
                }
                b'n' => {
                    // NoData
                    all_results.push(Vec::new());
                }
                b'Z' => {
                    // ReadyForQuery
                    if all_results.len() == params_batch.len() {
                        return Ok(all_results);
                    }
                }
                _ => {}
            }
        }
    }

    /// ZERO-COPY pipeline execution with Bytes for column data.
    ///
    /// Uses reference-counted Bytes slices instead of Vec copies.
    /// This matches C's libpq behavior of returning pointers into internal buffers.
    pub async fn pipeline_prepared_zerocopy(
        &mut self,
        stmt: &super::PreparedStatement,
        params_batch: &[Vec<Option<Vec<u8>>>],
    ) -> PgResult<Vec<Vec<Vec<Option<bytes::Bytes>>>>> {
        if params_batch.is_empty() {
            return Ok(Vec::new());
        }

        if !self.prepared_statements.contains_key(&stmt.name) {
            return Err(PgError::Query(
                "Statement not prepared. Call prepare() first.".to_string(),
            ));
        }

        let mut buf = BytesMut::with_capacity(params_batch.len() * 64);

        for params in params_batch {
            PgEncoder::encode_bind_to(&mut buf, &stmt.name, params);
            PgEncoder::encode_execute_to(&mut buf);
        }

        PgEncoder::encode_sync_to(&mut buf);

        // Write and flush
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        // Collect results using ZERO-COPY Bytes
        let mut all_results: Vec<Vec<Vec<Option<bytes::Bytes>>>> =
            Vec::with_capacity(params_batch.len());
        let mut current_rows: Vec<Vec<Option<bytes::Bytes>>> = Vec::new();

        loop {
            let (msg_type, data) = self.recv_data_zerocopy().await?;

            match msg_type {
                b'2' => {} // BindComplete
                b'T' => {} // RowDescription
                b'D' => {
                    // DataRow
                    if let Some(row) = data {
                        current_rows.push(row);
                    }
                }
                b'C' => {
                    // CommandComplete
                    all_results.push(std::mem::take(&mut current_rows));
                }
                b'n' => {
                    // NoData
                    all_results.push(Vec::new());
                }
                b'Z' => {
                    // ReadyForQuery
                    if all_results.len() == params_batch.len() {
                        return Ok(all_results);
                    }
                }
                _ => {}
            }
        }
    }

    /// ULTRA-FAST pipeline for 2-column SELECT queries.
    /// Optimized for the common "SELECT id, name FROM table" pattern.
    pub async fn pipeline_prepared_ultra(
        &mut self,
        stmt: &super::PreparedStatement,
        params_batch: &[Vec<Option<Vec<u8>>>],
    ) -> PgResult<Vec<Vec<(bytes::Bytes, bytes::Bytes)>>> {
        if params_batch.is_empty() {
            return Ok(Vec::new());
        }

        if !self.prepared_statements.contains_key(&stmt.name) {
            return Err(PgError::Query(
                "Statement not prepared. Call prepare() first.".to_string(),
            ));
        }

        let mut buf = BytesMut::with_capacity(params_batch.len() * 64);

        for params in params_batch {
            PgEncoder::encode_bind_to(&mut buf, &stmt.name, params);
            PgEncoder::encode_execute_to(&mut buf);
        }

        PgEncoder::encode_sync_to(&mut buf);

        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        // Pre-allocate with expected capacity
        let mut all_results: Vec<Vec<(bytes::Bytes, bytes::Bytes)>> =
            Vec::with_capacity(params_batch.len());
        let mut current_rows: Vec<(bytes::Bytes, bytes::Bytes)> = Vec::with_capacity(16);

        loop {
            let (msg_type, data) = self.recv_data_ultra().await?;

            match msg_type {
                b'2' | b'T' => {} // BindComplete, RowDescription
                b'D' => {
                    if let Some(row) = data {
                        current_rows.push(row);
                    }
                }
                b'C' => {
                    all_results.push(std::mem::take(&mut current_rows));
                    current_rows = Vec::with_capacity(16);
                }
                b'n' => {
                    all_results.push(Vec::new());
                }
                b'Z' => {
                    if all_results.len() == params_batch.len() {
                        return Ok(all_results);
                    }
                }
                _ => {}
            }
        }
    }
}
