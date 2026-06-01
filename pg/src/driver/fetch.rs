//! PgDriver fetch methods: fetch_all (cached/uncached/fast), fetch_typed,
//! fetch_one, execute, and query_ast.

use super::core::PgDriver;
use super::prepared::PreparedAstQuery;
use super::types::*;
use qail_core::ast::Qail;
use std::sync::Arc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

#[inline]
fn return_with_desync<T>(driver: &mut PgDriver, err: PgError) -> PgResult<T> {
    if matches!(
        err,
        PgError::Protocol(_) | PgError::Connection(_) | PgError::Timeout(_)
    ) {
        driver.connection.mark_io_desynced();
    }
    Err(err)
}

#[inline]
fn encoded_sql_str(sql_buf: &[u8]) -> PgResult<&str> {
    std::str::from_utf8(sql_buf)
        .map_err(|e| PgError::Encode(format!("encoded SQL is not UTF-8: {}", e)))
}

async fn reprepare_prepared_ast_query(
    conn: &mut super::PgConnection,
    prepared: &PreparedAstQuery,
) -> PgResult<()> {
    conn.clear_prepared_statement_state();
    let stmt = conn.prepare(&prepared.sql).await?;
    conn.stmt_cache
        .put(prepared.sql_hash, stmt.name().to_string());
    conn.prepared_statements
        .insert(stmt.name().to_string(), prepared.sql.clone());
    Ok(())
}

impl PgDriver {
    /// Execute a QAIL command and fetch all rows (CACHED + ZERO-ALLOC).
    /// **Default method** - uses prepared statement caching for best performance.
    /// On first call: sends Parse + Bind + Execute + Sync
    /// On subsequent calls with same SQL: sends only Bind + Execute (SKIPS Parse!)
    /// Uses per-connection LRU cache with max 100 statements (auto-evicts oldest),
    /// with a hard prepared-statement cap of 128 per connection.
    pub async fn fetch_all(&mut self, cmd: &Qail) -> PgResult<Vec<PgRow>> {
        self.fetch_all_with_format(cmd, ResultFormat::Text).await
    }

    /// Execute a QAIL command and fetch all rows using a specific result format.
    ///
    /// `result_format` controls server result-column encoding:
    /// - [`ResultFormat::Text`] for standard text decoding.
    /// - [`ResultFormat::Binary`] for binary wire values.
    pub async fn fetch_all_with_format(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        // Delegate to cached-by-default behavior.
        self.fetch_all_cached_with_format(cmd, result_format).await
    }

    /// Prepare an AST query once and return a reusable frozen handle.
    ///
    /// This is the lowest-overhead path for repeating the **exact same** AST
    /// command (same SQL text and same bind values). It avoids per-call AST
    /// encoding and statement-cache hash/lookup in `fetch_all_cached`.
    pub async fn prepare_ast_query(&mut self, cmd: &Qail) -> PgResult<PreparedAstQuery> {
        use crate::protocol::AstEncoder;

        let (sql, params) =
            AstEncoder::encode_cmd_sql(cmd).map_err(|e| PgError::Encode(e.to_string()))?;
        let stmt = self.connection.prepare(&sql).await?;

        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        let sql_hash = hasher.finish();

        self.connection
            .stmt_cache
            .put(sql_hash, stmt.name().to_string());
        self.connection
            .prepared_statements
            .insert(stmt.name().to_string(), sql.clone());

        Ok(PreparedAstQuery {
            stmt,
            params,
            sql,
            sql_hash,
        })
    }

    /// Execute a precompiled AST query handle and return rows.
    ///
    /// Rows are returned without `ColumnInfo` metadata (`column_info = None`),
    /// so prefer positional access (`row.text(0)`, `row.get_i64(1)`, ...).
    pub async fn fetch_all_prepared_ast(
        &mut self,
        prepared: &PreparedAstQuery,
    ) -> PgResult<Vec<PgRow>> {
        self.fetch_all_prepared_ast_with_format(prepared, ResultFormat::Text)
            .await
    }

    /// Execute a precompiled AST query handle with explicit result format.
    pub async fn fetch_all_prepared_ast_with_format(
        &mut self,
        prepared: &PreparedAstQuery,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        let mut retried = false;

        loop {
            self.connection.stmt_cache.touch_key(prepared.sql_hash);
            self.connection.write_buf.clear();
            if let Err(e) = crate::protocol::PgEncoder::encode_bind_to_with_result_format(
                &mut self.connection.write_buf,
                prepared.stmt.name(),
                &prepared.params,
                result_format.as_wire_code(),
            ) {
                return Err(PgError::Encode(e.to_string()));
            }
            crate::protocol::PgEncoder::encode_execute_to(&mut self.connection.write_buf);
            crate::protocol::PgEncoder::encode_sync_to(&mut self.connection.write_buf);

            if let Err(err) = self.connection.flush_write_buf().await {
                if !retried && err.is_prepared_statement_retryable() {
                    retried = true;
                    reprepare_prepared_ast_query(&mut self.connection, prepared).await?;
                    continue;
                }
                return Err(err);
            }

            let mut rows: Vec<PgRow> = Vec::with_capacity(32);
            let mut error: Option<PgError> = None;
            let mut flow = super::extended_flow::ExtendedFlowTracker::new(
                super::extended_flow::ExtendedFlowConfig::parse_bind_execute(false),
            );

            loop {
                let msg = self.connection.recv().await?;
                if let Err(err) = flow.validate(
                    &msg,
                    "driver fetch_all_prepared_ast execute",
                    error.is_some(),
                ) {
                    return return_with_desync(self, err);
                }
                match msg {
                    crate::protocol::BackendMessage::BindComplete => {}
                    crate::protocol::BackendMessage::RowDescription(_) => {}
                    crate::protocol::BackendMessage::DataRow(data) => {
                        if error.is_none() {
                            rows.push(PgRow {
                                columns: data,
                                column_info: None,
                            });
                        }
                    }
                    crate::protocol::BackendMessage::CommandComplete(_) => {}
                    crate::protocol::BackendMessage::NoData => {}
                    crate::protocol::BackendMessage::ReadyForQuery(_) => {
                        if let Some(err) = error {
                            if !retried && err.is_prepared_statement_retryable() {
                                retried = true;
                                reprepare_prepared_ast_query(&mut self.connection, prepared)
                                    .await?;
                                break;
                            }
                            return Err(err);
                        }
                        return Ok(rows);
                    }
                    crate::protocol::BackendMessage::ErrorResponse(err) => {
                        if error.is_none() {
                            error = Some(PgError::QueryServer(err.into()));
                        }
                    }
                    msg if is_ignorable_session_message(&msg) => {}
                    other => {
                        return return_with_desync(
                            self,
                            unexpected_backend_message(
                                "driver fetch_all_prepared_ast execute",
                                &other,
                            ),
                        );
                    }
                }
            }
        }
    }

    /// Execute a QAIL command and fetch all rows as a typed struct (text format).
    /// Requires the target type to implement `QailRow` trait.
    ///
    /// # Example
    /// ```ignore
    /// let users: Vec<User> = driver.fetch_typed::<User>(&query).await?;
    /// ```
    pub async fn fetch_typed<T: super::row::QailRow>(&mut self, cmd: &Qail) -> PgResult<Vec<T>> {
        self.fetch_typed_with_format(cmd, ResultFormat::Text).await
    }

    /// Execute a QAIL command and fetch all rows as a typed struct with explicit result format.
    ///
    /// Use [`ResultFormat::Binary`] to get binary wire values; row decoding should use
    /// metadata-aware accessors such as `PgRow::try_get()` / `try_get_by_name()`.
    pub async fn fetch_typed_with_format<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<T>> {
        let rows = self.fetch_all_with_format(cmd, result_format).await?;
        Ok(rows.iter().map(T::from_row).collect())
    }

    /// Execute a QAIL command and fetch a single row as a typed struct (text format).
    /// Returns None if no rows are returned.
    pub async fn fetch_one_typed<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
    ) -> PgResult<Option<T>> {
        self.fetch_one_typed_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command and fetch a single row as a typed struct with explicit result format.
    pub async fn fetch_one_typed_with_format<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Option<T>> {
        let rows = self.fetch_all_with_format(cmd, result_format).await?;
        Ok(rows.first().map(T::from_row))
    }

    /// Execute a QAIL command and fetch all rows (UNCACHED).
    /// Sends Parse + Bind + Execute on every call.
    /// Use for one-off queries or when caching is not desired.
    ///
    /// Optimized: encodes wire bytes into reusable write_buf (zero-alloc).
    pub async fn fetch_all_uncached(&mut self, cmd: &Qail) -> PgResult<Vec<PgRow>> {
        self.fetch_all_uncached_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command and fetch all rows (UNCACHED) with explicit result format.
    pub async fn fetch_all_uncached_with_format(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;

        AstEncoder::encode_cmd_reuse_into_with_result_format(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
            &mut self.connection.write_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

        self.connection.flush_write_buf().await?;

        let mut rows: Vec<PgRow> = Vec::with_capacity(32);
        let mut column_info: Option<Arc<ColumnInfo>> = None;

        let mut error: Option<PgError> = None;
        let mut flow = super::extended_flow::ExtendedFlowTracker::new(
            super::extended_flow::ExtendedFlowConfig::parse_bind_describe_portal_execute(),
        );

        loop {
            let msg = self.connection.recv().await?;
            if let Err(err) = flow.validate(&msg, "driver fetch_all execute", error.is_some()) {
                return return_with_desync(self, err);
            }
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    column_info = Some(Arc::new(ColumnInfo::from_fields(&fields)));
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::NoData => {}
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("driver fetch_all execute", &other),
                    );
                }
            }
        }
    }

    /// Execute a QAIL command and fetch all rows (FAST VERSION).
    /// Uses optimized recv_with_data_fast for faster response parsing.
    /// Skips column metadata collection for maximum speed.
    pub async fn fetch_all_fast(&mut self, cmd: &Qail) -> PgResult<Vec<PgRow>> {
        self.fetch_all_fast_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command and fetch all rows (FAST VERSION) with explicit result format.
    pub async fn fetch_all_fast_with_format(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;

        AstEncoder::encode_cmd_reuse_into_with_result_format(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
            &mut self.connection.write_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

        self.connection.flush_write_buf().await?;

        // Collect results using FAST receiver
        let mut rows: Vec<PgRow> = Vec::with_capacity(32);
        let mut error: Option<PgError> = None;
        let mut flow = super::extended_flow::ExtendedFlowTracker::new(
            super::extended_flow::ExtendedFlowConfig::parse_bind_execute(true),
        );

        loop {
            let res = self.connection.recv_with_data_fast().await;
            match res {
                Ok((msg_type, data)) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "driver fetch_all_fast execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'D' => {
                            if error.is_none()
                                && let Some(columns) = data
                            {
                                rows.push(PgRow {
                                    columns,
                                    column_info: None,
                                });
                            }
                        }
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(rows);
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    // QueryServer means backend sent ErrorResponse; keep draining to ReadyForQuery.
                    if matches!(&e, PgError::QueryServer(_)) {
                        if error.is_none() {
                            error = Some(e);
                        }
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Execute a QAIL command and fetch one row.
    pub async fn fetch_one(&mut self, cmd: &Qail) -> PgResult<PgRow> {
        let rows = self.fetch_all(cmd).await?;
        rows.into_iter().next().ok_or(PgError::NoRows)
    }

    /// Execute a QAIL command with PREPARED STATEMENT CACHING.
    /// Like fetch_all(), but caches the prepared statement on the server.
    /// On first call: sends Parse + Describe + Bind + Execute + Sync
    /// On subsequent calls: sends only Bind + Execute + Sync (SKIPS Parse!)
    /// Column metadata (RowDescription) is cached alongside the statement
    /// so that by-name column access works on every call.
    ///
    /// Optimized: all wire messages are batched into a single write_all syscall.
    pub async fn fetch_all_cached(&mut self, cmd: &Qail) -> PgResult<Vec<PgRow>> {
        self.fetch_all_cached_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command with prepared statement caching and explicit result format.
    pub async fn fetch_all_cached_with_format(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        let mut retried = false;
        loop {
            match self
                .fetch_all_cached_with_format_once(cmd, result_format)
                .await
            {
                Ok(rows) => return Ok(rows),
                Err(err)
                    if !retried
                        && (err.is_prepared_statement_retryable()
                            || err.is_prepared_statement_already_exists()) =>
                {
                    retried = true;
                    if err.is_prepared_statement_retryable() {
                        self.connection.clear_prepared_statement_state();
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn fetch_all_cached_with_format_once(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<PgRow>> {
        use crate::protocol::AstEncoder;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        if !AstEncoder::encode_cacheable_cmd_sql_to(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
        )? {
            // Fallback for unsupported actions
            let (sql, params) =
                AstEncoder::encode_cmd_sql(cmd).map_err(|e| PgError::Encode(e.to_string()))?;
            let raw_rows = self
                .connection
                .query_cached_with_result_format(&sql, &params, result_format.as_wire_code())
                .await?;
            return Ok(raw_rows
                .into_iter()
                .map(|data| PgRow {
                    columns: data,
                    column_info: None,
                })
                .collect());
        }

        let mut hasher = DefaultHasher::new();
        self.connection.sql_buf.hash(&mut hasher);
        let sql_hash = hasher.finish();

        let is_cache_miss = !self.connection.stmt_cache.contains(&sql_hash);

        // Build ALL wire messages into write_buf (single syscall)
        self.connection.write_buf.clear();

        let stmt_name = if let Some(name) = self.connection.stmt_cache.get(&sql_hash) {
            name
        } else {
            let name = format!("qail_{:x}", sql_hash);

            // Evict LRU before borrowing sql_buf to avoid borrow conflict
            self.connection.evict_prepared_if_full();

            let sql_str = encoded_sql_str(&self.connection.sql_buf)?;

            // Buffer Parse + Describe(Statement) for first call
            use crate::protocol::PgEncoder;
            let parse_msg = PgEncoder::try_encode_parse(&name, sql_str, &[])?;
            let describe_msg = PgEncoder::try_encode_describe(false, &name)?;
            self.connection.write_buf.extend_from_slice(&parse_msg);
            self.connection.write_buf.extend_from_slice(&describe_msg);

            self.connection.stmt_cache.put(sql_hash, name.clone());
            self.connection
                .prepared_statements
                .insert(name.clone(), sql_str.to_string());

            name
        };

        // Append Bind + Execute + Sync to same buffer
        use crate::protocol::PgEncoder;
        if let Err(e) = PgEncoder::encode_bind_to_with_result_format(
            &mut self.connection.write_buf,
            &stmt_name,
            &self.connection.params_buf,
            result_format.as_wire_code(),
        ) {
            if is_cache_miss {
                self.connection.stmt_cache.remove(&sql_hash);
                self.connection.prepared_statements.remove(&stmt_name);
                self.connection.column_info_cache.remove(&sql_hash);
            }
            return Err(PgError::Encode(e.to_string()));
        }
        PgEncoder::encode_execute_to(&mut self.connection.write_buf);
        PgEncoder::encode_sync_to(&mut self.connection.write_buf);

        // Single write_all syscall for all messages
        if let Err(err) = self.connection.flush_write_buf().await {
            if is_cache_miss {
                self.connection.stmt_cache.remove(&sql_hash);
                self.connection.prepared_statements.remove(&stmt_name);
                self.connection.column_info_cache.remove(&sql_hash);
            }
            return Err(err);
        }

        // On cache hit, use the previously cached ColumnInfo
        let cached_column_info = self.connection.column_info_cache.get(&sql_hash).cloned();

        let mut rows: Vec<PgRow> = Vec::with_capacity(32);
        let mut column_info: Option<Arc<ColumnInfo>> = cached_column_info;
        let mut error: Option<PgError> = None;
        let mut flow = super::extended_flow::ExtendedFlowTracker::new(
            super::extended_flow::ExtendedFlowConfig::parse_describe_statement_bind_execute(
                is_cache_miss,
            ),
        );

        loop {
            let msg = match self.connection.recv().await {
                Ok(msg) => msg,
                Err(err) => {
                    if is_cache_miss && !flow.saw_parse_complete() {
                        self.connection.stmt_cache.remove(&sql_hash);
                        self.connection.prepared_statements.remove(&stmt_name);
                        self.connection.column_info_cache.remove(&sql_hash);
                    }
                    return Err(err);
                }
            };
            if let Err(err) =
                flow.validate(&msg, "driver fetch_all_cached execute", error.is_some())
            {
                if is_cache_miss && !flow.saw_parse_complete() {
                    self.connection.stmt_cache.remove(&sql_hash);
                    self.connection.prepared_statements.remove(&stmt_name);
                    self.connection.column_info_cache.remove(&sql_hash);
                }
                return return_with_desync(self, err);
            }
            match msg {
                crate::protocol::BackendMessage::ParseComplete => {}
                crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::ParameterDescription(_) => {
                    // Sent after Describe(Statement) — ignore
                }
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    // Received after Describe(Statement) on cache miss
                    let info = Arc::new(ColumnInfo::from_fields(&fields));
                    if is_cache_miss {
                        self.connection
                            .column_info_cache
                            .insert(sql_hash, Arc::clone(&info));
                    }
                    column_info = Some(info);
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::NoData => {
                    // Sent by Describe for statements that return no data (e.g. pure UPDATE without RETURNING)
                }
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        if is_cache_miss
                            && !flow.saw_parse_complete()
                            && !err.is_prepared_statement_already_exists()
                        {
                            self.connection.stmt_cache.remove(&sql_hash);
                            self.connection.prepared_statements.remove(&stmt_name);
                            self.connection.column_info_cache.remove(&sql_hash);
                        }
                        return Err(err);
                    }
                    if is_cache_miss && !flow.saw_parse_complete() {
                        self.connection.stmt_cache.remove(&sql_hash);
                        self.connection.prepared_statements.remove(&stmt_name);
                        self.connection.column_info_cache.remove(&sql_hash);
                        return return_with_desync(
                            self,
                            PgError::Protocol(
                                "Cache miss query reached ReadyForQuery without ParseComplete"
                                    .to_string(),
                            ),
                        );
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        let query_err = PgError::QueryServer(err.into());
                        if query_err.is_prepared_statement_retryable() {
                            self.connection.clear_prepared_statement_state();
                        }
                        error = Some(query_err);
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    if is_cache_miss && !flow.saw_parse_complete() {
                        self.connection.stmt_cache.remove(&sql_hash);
                        self.connection.prepared_statements.remove(&stmt_name);
                        self.connection.column_info_cache.remove(&sql_hash);
                    }
                    return return_with_desync(
                        self,
                        unexpected_backend_message("driver fetch_all_cached execute", &other),
                    );
                }
            }
        }
    }

    /// Execute a QAIL command (for mutations) - ZERO-ALLOC.
    pub async fn execute(&mut self, cmd: &Qail) -> PgResult<u64> {
        use crate::protocol::AstEncoder;

        let wire_bytes = AstEncoder::encode_cmd_reuse(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

        self.connection.send_bytes(&wire_bytes).await?;

        let mut affected = 0u64;
        let mut error: Option<PgError> = None;
        let mut flow = super::extended_flow::ExtendedFlowTracker::new(
            super::extended_flow::ExtendedFlowConfig::parse_bind_describe_portal_execute(),
        );

        loop {
            let msg = self.connection.recv().await?;
            if let Err(err) = flow.validate(&msg, "driver execute mutation", error.is_some()) {
                return return_with_desync(self, err);
            }
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(_) => {}
                crate::protocol::BackendMessage::DataRow(_) => {}
                crate::protocol::BackendMessage::NoData => {}
                crate::protocol::BackendMessage::CommandComplete(tag) => {
                    if error.is_none() {
                        match super::parse_affected_rows(&tag) {
                            Ok(parsed) => affected = parsed,
                            Err(err) => return return_with_desync(self, err),
                        }
                    }
                }
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(affected);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("driver execute mutation", &other),
                    );
                }
            }
        }
    }

    /// Query a QAIL command and return rows (for SELECT/GET queries).
    /// Like `execute()` but collects RowDescription + DataRow messages
    /// instead of discarding them.
    pub async fn query_ast(&mut self, cmd: &Qail) -> PgResult<QueryResult> {
        self.query_ast_with_format(cmd, ResultFormat::Text).await
    }

    /// Query a QAIL command and return rows using an explicit result format.
    pub async fn query_ast_with_format(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
    ) -> PgResult<QueryResult> {
        use crate::protocol::AstEncoder;

        let wire_bytes = AstEncoder::encode_cmd_reuse_with_result_format(
            cmd,
            &mut self.connection.sql_buf,
            &mut self.connection.params_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

        self.connection.send_bytes(&wire_bytes).await?;

        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut error: Option<PgError> = None;
        let mut flow = super::extended_flow::ExtendedFlowTracker::new(
            super::extended_flow::ExtendedFlowConfig::parse_bind_describe_portal_execute(),
        );

        loop {
            let msg = self.connection.recv().await?;
            if let Err(err) = flow.validate(&msg, "driver query_ast", error.is_some()) {
                return return_with_desync(self, err);
            }
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    columns = fields.into_iter().map(|f| f.name).collect();
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        let row: Vec<Option<String>> = data
                            .into_iter()
                            .map(|col| col.map(|bytes| String::from_utf8_lossy(&bytes).to_string()))
                            .collect();
                        rows.push(row);
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::NoData => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(QueryResult { columns, rows });
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("driver query_ast", &other),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_encoded_sql_str_rejects_invalid_utf8() {
        let err = encoded_sql_str(&[0xff]).expect_err("invalid SQL UTF-8 must fail");
        assert!(err.to_string().contains("encoded SQL is not UTF-8"));
    }

    #[cfg(unix)]
    fn test_driver_with_peer() -> (PgDriver, tokio::net::UnixStream) {
        use crate::driver::connection::StatementCache;
        use crate::driver::stream::PgStream;
        use bytes::BytesMut;
        use std::collections::{HashMap, VecDeque};
        use std::num::NonZeroUsize;
        use tokio::net::UnixStream;

        let (unix_stream, peer) = UnixStream::pair().expect("unix stream pair");
        let conn = super::super::PgConnection {
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
            requested_protocol_minor: super::super::PgConnection::default_protocol_minor(),
            negotiated_protocol_minor: super::super::PgConnection::default_protocol_minor(),
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled: false,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        };
        (PgDriver::new(conn), peer)
    }

    #[cfg(unix)]
    fn push_backend_frame(driver: &mut PgDriver, msg_type: u8, payload: &[u8]) {
        driver.connection.buffer.extend_from_slice(&[msg_type]);
        driver
            .connection
            .buffer
            .extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        driver.connection.buffer.extend_from_slice(payload);
    }

    #[cfg(unix)]
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
    fn push_command_complete(driver: &mut PgDriver, tag: &str) {
        let mut payload = Vec::with_capacity(tag.len() + 1);
        payload.extend_from_slice(tag.as_bytes());
        payload.push(0);
        push_backend_frame(driver, b'C', &payload);
    }

    #[cfg(unix)]
    fn prepared_ast_for_sql(sql: &str) -> PreparedAstQuery {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);

        PreparedAstQuery {
            stmt: crate::driver::PreparedStatement::from_sql(sql),
            params: Vec::new(),
            sql: sql.to_string(),
            sql_hash: hasher.finish(),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fetch_fast_protocol_error_marks_driver_connection_desynced() {
        let (mut driver, _peer) = test_driver_with_peer();
        push_backend_frame(&mut driver, b'D', &0i16.to_be_bytes());

        let err = match driver.fetch_all_fast(&Qail::get("users")).await {
            Ok(_) => panic!("out-of-order DataRow must fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("DataRow before BindComplete"));
        assert!(driver.connection.is_io_desynced());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn execute_bad_command_tag_marks_driver_connection_desynced() {
        let (mut driver, _peer) = test_driver_with_peer();
        push_backend_frame(&mut driver, b'1', &[]);
        push_backend_frame(&mut driver, b'2', &[]);
        push_backend_frame(&mut driver, b'n', &[]);
        push_command_complete(&mut driver, "UPDATE");
        push_backend_frame(&mut driver, b'Z', b"I");

        let err = driver
            .execute(&Qail::get("users"))
            .await
            .expect_err("malformed CommandComplete tag must fail");

        assert!(
            err.to_string().contains("missing affected row count")
                || err.to_string().contains("invalid affected row count")
        );
        assert!(driver.connection.is_io_desynced());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepared_ast_retry_reparses_after_missing_server_statement() {
        let (mut driver, _peer) = test_driver_with_peer();
        let prepared = prepared_ast_for_sql("SELECT 1");
        let stmt_name = prepared.stmt.name().to_string();

        driver
            .connection
            .stmt_cache
            .put(prepared.sql_hash, stmt_name.clone());
        driver
            .connection
            .prepared_statements
            .insert(stmt_name.clone(), prepared.sql.clone());

        let missing_payload = error_response_payload(
            "26000",
            &format!("prepared statement \"{}\" does not exist", stmt_name),
        );

        // First execution: backend says local prepared state is stale.
        push_backend_frame(&mut driver, b'E', &missing_payload);
        push_backend_frame(&mut driver, b'Z', b"I");
        // Re-prepare: this must consume ParseComplete + ReadyForQuery.
        push_backend_frame(&mut driver, b'1', &[]);
        push_backend_frame(&mut driver, b'Z', b"I");
        // Retried execution succeeds.
        push_backend_frame(&mut driver, b'2', &[]);
        push_command_complete(&mut driver, "SELECT 0");
        push_backend_frame(&mut driver, b'Z', b"I");

        let rows = driver
            .fetch_all_prepared_ast(&prepared)
            .await
            .expect("stale prepared AST handle should reparse and retry once");

        assert!(rows.is_empty());
        assert!(
            driver
                .connection
                .prepared_statements
                .contains_key(&stmt_name)
        );
        assert!(driver.connection.stmt_cache.contains(&prepared.sql_hash));
        assert!(!driver.connection.is_io_desynced());
    }
}
