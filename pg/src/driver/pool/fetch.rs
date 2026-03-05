//! Fetch methods for PooledConnection: uncached, fast, cached, typed, and pipelined-RLS variants.

use super::connection::PooledConnection;
use super::lifecycle::MAX_HOT_STATEMENTS;
use crate::driver::{
    PgError, PgResult, ResultFormat,
    extended_flow::{ExtendedFlowConfig, ExtendedFlowTracker},
    is_ignorable_session_message, unexpected_backend_message,
};
use std::sync::Arc;

impl PooledConnection {
    /// Execute a QAIL command and fetch all rows (UNCACHED).
    /// Returns rows with column metadata for JSON serialization.
    pub async fn fetch_all_uncached(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Vec<crate::driver::PgRow>> {
        self.fetch_all_uncached_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute raw SQL with bind parameters and return raw row data.
    ///
    /// Uses the Extended Query Protocol so parameters are never interpolated
    /// into the SQL string. Intended for EXPLAIN or other SQL that can't be
    /// represented as a `Qail` AST but still needs parameterized execution.
    ///
    /// Returns raw column bytes; callers must decode as needed.
    pub async fn query_raw_with_params(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        let conn = self.conn_mut()?;
        conn.query(sql, params).await
    }

    /// Export data using AST-native COPY TO STDOUT and collect parsed rows.
    pub async fn copy_export(&mut self, cmd: &qail_core::ast::Qail) -> PgResult<Vec<Vec<String>>> {
        self.conn_mut()?.copy_export(cmd).await
    }

    /// Stream AST-native COPY TO STDOUT chunks with bounded memory usage.
    pub async fn copy_export_stream_raw<F, Fut>(
        &mut self,
        cmd: &qail_core::ast::Qail,
        on_chunk: F,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = PgResult<()>>,
    {
        self.conn_mut()?.copy_export_stream_raw(cmd, on_chunk).await
    }

    /// Stream AST-native COPY TO STDOUT rows with bounded memory usage.
    pub async fn copy_export_stream_rows<F>(
        &mut self,
        cmd: &qail_core::ast::Qail,
        on_row: F,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<String>) -> PgResult<()>,
    {
        self.conn_mut()?.copy_export_stream_rows(cmd, on_row).await
    }

    /// Export a table using COPY TO STDOUT and collect raw bytes.
    pub async fn copy_export_table(
        &mut self,
        table: &str,
        columns: &[String],
    ) -> PgResult<Vec<u8>> {
        let quote_ident = |ident: &str| -> String {
            format!("\"{}\"", ident.replace('\0', "").replace('"', "\"\""))
        };
        let cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let sql = format!(
            "COPY {} ({}) TO STDOUT",
            quote_ident(table),
            cols.join(", ")
        );
        self.conn_mut()?.copy_out_raw(&sql).await
    }

    /// Stream a table export using COPY TO STDOUT with bounded memory usage.
    pub async fn copy_export_table_stream<F, Fut>(
        &mut self,
        table: &str,
        columns: &[String],
        on_chunk: F,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = PgResult<()>>,
    {
        let quote_ident = |ident: &str| -> String {
            format!("\"{}\"", ident.replace('\0', "").replace('"', "\"\""))
        };
        let cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let sql = format!(
            "COPY {} ({}) TO STDOUT",
            quote_ident(table),
            cols.join(", ")
        );
        self.conn_mut()?.copy_out_raw_stream(&sql, on_chunk).await
    }

    /// Execute a QAIL command and fetch all rows (UNCACHED) with explicit result format.
    pub async fn fetch_all_uncached_with_format(
        &mut self,
        cmd: &qail_core::ast::Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<crate::driver::PgRow>> {
        use crate::driver::ColumnInfo;
        use crate::protocol::AstEncoder;

        let conn = self.conn_mut()?;

        AstEncoder::encode_cmd_reuse_into_with_result_format(
            cmd,
            &mut conn.sql_buf,
            &mut conn.params_buf,
            &mut conn.write_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

        conn.flush_write_buf().await?;

        let mut rows: Vec<crate::driver::PgRow> = Vec::new();
        let mut column_info: Option<Arc<ColumnInfo>> = None;
        let mut error: Option<PgError> = None;
        let mut flow =
            ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_describe_portal_execute());

        loop {
            let msg = conn.recv().await?;
            flow.validate(&msg, "pool fetch_all execute", error.is_some())?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    column_info = Some(Arc::new(ColumnInfo::from_fields(&fields)));
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(crate::driver::PgRow {
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
                    return Err(unexpected_backend_message("pool fetch_all execute", &other));
                }
            }
        }
    }

    /// Execute a QAIL command and fetch all rows (FAST VERSION).
    /// Uses native AST-to-wire encoding and optimized recv_with_data_fast.
    /// Skips column metadata for maximum speed.
    pub async fn fetch_all_fast(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Vec<crate::driver::PgRow>> {
        self.fetch_all_fast_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command and fetch all rows (FAST VERSION) with explicit result format.
    pub async fn fetch_all_fast_with_format(
        &mut self,
        cmd: &qail_core::ast::Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<crate::driver::PgRow>> {
        use crate::protocol::AstEncoder;

        let conn = self.conn_mut()?;

        AstEncoder::encode_cmd_reuse_into_with_result_format(
            cmd,
            &mut conn.sql_buf,
            &mut conn.params_buf,
            &mut conn.write_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

        conn.flush_write_buf().await?;

        let mut rows: Vec<crate::driver::PgRow> = Vec::with_capacity(32);
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));

        loop {
            let res = conn.recv_with_data_fast().await;
            match res {
                Ok((msg_type, data)) => {
                    flow.validate_msg_type(
                        msg_type,
                        "pool fetch_all_fast execute",
                        error.is_some(),
                    )?;
                    match msg_type {
                        b'D' => {
                            if error.is_none()
                                && let Some(columns) = data
                            {
                                rows.push(crate::driver::PgRow {
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

    /// Execute a QAIL command and fetch all rows (CACHED).
    /// Uses prepared statement caching: Parse+Describe on first call,
    /// then Bind+Execute only on subsequent calls with the same SQL shape.
    /// This matches PostgREST's behavior for fair benchmarks.
    pub async fn fetch_all_cached(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Vec<crate::driver::PgRow>> {
        self.fetch_all_cached_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command and fetch all rows (CACHED) with explicit result format.
    pub async fn fetch_all_cached_with_format(
        &mut self,
        cmd: &qail_core::ast::Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<crate::driver::PgRow>> {
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
                    if err.is_prepared_statement_retryable()
                        && let Some(conn) = self.conn.as_mut()
                    {
                        conn.clear_prepared_statement_state();
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Execute a QAIL command and decode rows into typed structs (CACHED, text format).
    pub async fn fetch_typed<T: crate::driver::row::QailRow>(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Vec<T>> {
        self.fetch_typed_with_format(cmd, ResultFormat::Text).await
    }

    /// Execute a QAIL command and decode rows into typed structs with explicit result format.
    ///
    /// Use [`ResultFormat::Binary`] for binary wire values; row decoders should use
    /// metadata-aware helpers like `PgRow::try_get()` / `try_get_by_name()`.
    pub async fn fetch_typed_with_format<T: crate::driver::row::QailRow>(
        &mut self,
        cmd: &qail_core::ast::Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<T>> {
        let rows = self
            .fetch_all_cached_with_format(cmd, result_format)
            .await?;
        Ok(rows.iter().map(T::from_row).collect())
    }

    /// Execute a QAIL command and decode one typed row (CACHED, text format).
    pub async fn fetch_one_typed<T: crate::driver::row::QailRow>(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Option<T>> {
        self.fetch_one_typed_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command and decode one typed row with explicit result format.
    pub async fn fetch_one_typed_with_format<T: crate::driver::row::QailRow>(
        &mut self,
        cmd: &qail_core::ast::Qail,
        result_format: ResultFormat,
    ) -> PgResult<Option<T>> {
        let rows = self
            .fetch_all_cached_with_format(cmd, result_format)
            .await?;
        Ok(rows.first().map(T::from_row))
    }

    async fn fetch_all_cached_with_format_once(
        &mut self,
        cmd: &qail_core::ast::Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<crate::driver::PgRow>> {
        use crate::driver::ColumnInfo;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let conn = self.conn.as_mut().ok_or_else(|| {
            PgError::Connection("Connection already released back to pool".into())
        })?;

        conn.sql_buf.clear();
        conn.params_buf.clear();

        // Encode SQL + params to reusable buffers
        match cmd.action {
            qail_core::ast::Action::Get | qail_core::ast::Action::With => {
                crate::protocol::ast_encoder::dml::encode_select(
                    cmd,
                    &mut conn.sql_buf,
                    &mut conn.params_buf,
                )?;
            }
            qail_core::ast::Action::Add => {
                crate::protocol::ast_encoder::dml::encode_insert(
                    cmd,
                    &mut conn.sql_buf,
                    &mut conn.params_buf,
                )?;
            }
            qail_core::ast::Action::Set => {
                crate::protocol::ast_encoder::dml::encode_update(
                    cmd,
                    &mut conn.sql_buf,
                    &mut conn.params_buf,
                )?;
            }
            qail_core::ast::Action::Del => {
                crate::protocol::ast_encoder::dml::encode_delete(
                    cmd,
                    &mut conn.sql_buf,
                    &mut conn.params_buf,
                )?;
            }
            _ => {
                // Fallback: unsupported actions go through uncached path
                return self
                    .fetch_all_uncached_with_format(cmd, result_format)
                    .await;
            }
        }

        let mut hasher = DefaultHasher::new();
        conn.sql_buf.hash(&mut hasher);
        let sql_hash = hasher.finish();

        let is_cache_miss = !conn.stmt_cache.contains(&sql_hash);

        conn.write_buf.clear();

        let stmt_name = if let Some(name) = conn.stmt_cache.get(&sql_hash) {
            name
        } else {
            let name = format!("qail_{:x}", sql_hash);

            conn.evict_prepared_if_full();

            let sql_str = std::str::from_utf8(&conn.sql_buf).unwrap_or("");

            use crate::protocol::PgEncoder;
            let parse_msg = PgEncoder::try_encode_parse(&name, sql_str, &[])?;
            let describe_msg = PgEncoder::try_encode_describe(false, &name)?;
            conn.write_buf.extend_from_slice(&parse_msg);
            conn.write_buf.extend_from_slice(&describe_msg);

            conn.stmt_cache.put(sql_hash, name.clone());
            conn.prepared_statements
                .insert(name.clone(), sql_str.to_string());

            // Register in global hot-statement registry for cross-connection sharing
            if let Ok(mut hot) = self.pool.hot_statements.write()
                && hot.len() < MAX_HOT_STATEMENTS
            {
                hot.insert(sql_hash, (name.clone(), sql_str.to_string()));
            }

            name
        };

        use crate::protocol::PgEncoder;
        if let Err(e) = PgEncoder::encode_bind_to_with_result_format(
            &mut conn.write_buf,
            &stmt_name,
            &conn.params_buf,
            result_format.as_wire_code(),
        ) {
            if is_cache_miss {
                conn.stmt_cache.remove(&sql_hash);
                conn.prepared_statements.remove(&stmt_name);
                conn.column_info_cache.remove(&sql_hash);
            }
            return Err(PgError::Encode(e.to_string()));
        }
        PgEncoder::encode_execute_to(&mut conn.write_buf);
        PgEncoder::encode_sync_to(&mut conn.write_buf);

        if let Err(err) = conn.flush_write_buf().await {
            if is_cache_miss {
                conn.stmt_cache.remove(&sql_hash);
                conn.prepared_statements.remove(&stmt_name);
                conn.column_info_cache.remove(&sql_hash);
            }
            return Err(err);
        }

        let cached_column_info = conn.column_info_cache.get(&sql_hash).cloned();

        let mut rows: Vec<crate::driver::PgRow> = Vec::with_capacity(32);
        let mut column_info: Option<Arc<ColumnInfo>> = cached_column_info;
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(
            ExtendedFlowConfig::parse_describe_statement_bind_execute(is_cache_miss),
        );

        loop {
            let msg = match conn.recv().await {
                Ok(msg) => msg,
                Err(err) => {
                    if is_cache_miss && !flow.saw_parse_complete() {
                        conn.stmt_cache.remove(&sql_hash);
                        conn.prepared_statements.remove(&stmt_name);
                        conn.column_info_cache.remove(&sql_hash);
                    }
                    return Err(err);
                }
            };
            if let Err(err) = flow.validate(&msg, "pool fetch_all_cached execute", error.is_some())
            {
                if is_cache_miss && !flow.saw_parse_complete() {
                    conn.stmt_cache.remove(&sql_hash);
                    conn.prepared_statements.remove(&stmt_name);
                    conn.column_info_cache.remove(&sql_hash);
                }
                return Err(err);
            }
            match msg {
                crate::protocol::BackendMessage::ParseComplete => {}
                crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::ParameterDescription(_) => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    let info = Arc::new(ColumnInfo::from_fields(&fields));
                    if is_cache_miss {
                        conn.column_info_cache.insert(sql_hash, info.clone());
                    }
                    column_info = Some(info);
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(crate::driver::PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        if is_cache_miss
                            && !flow.saw_parse_complete()
                            && !err.is_prepared_statement_already_exists()
                        {
                            conn.stmt_cache.remove(&sql_hash);
                            conn.prepared_statements.remove(&stmt_name);
                            conn.column_info_cache.remove(&sql_hash);
                        }
                        return Err(err);
                    }
                    if is_cache_miss && !flow.saw_parse_complete() {
                        conn.stmt_cache.remove(&sql_hash);
                        conn.prepared_statements.remove(&stmt_name);
                        conn.column_info_cache.remove(&sql_hash);
                        return Err(PgError::Protocol(
                            "Cache miss query reached ReadyForQuery without ParseComplete"
                                .to_string(),
                        ));
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
                    if is_cache_miss && !flow.saw_parse_complete() {
                        conn.stmt_cache.remove(&sql_hash);
                        conn.prepared_statements.remove(&stmt_name);
                        conn.column_info_cache.remove(&sql_hash);
                    }
                    return Err(unexpected_backend_message(
                        "pool fetch_all_cached execute",
                        &other,
                    ));
                }
            }
        }
    }

    /// Execute a QAIL command with RLS context in a SINGLE roundtrip.
    ///
    /// Pipelines the RLS setup (BEGIN + set_config) and the query
    /// (Parse/Bind/Execute/Sync) into one `write_all` syscall.
    /// PG processes messages in order, so the BEGIN + set_config
    /// completes before the query executes — security is preserved.
    ///
    /// Wire layout:
    /// ```text
    /// [SimpleQuery: "BEGIN; SET LOCAL...; SELECT set_config(...)"]
    /// [Parse (if cache miss)]
    /// [Describe (if cache miss)]
    /// [Bind]
    /// [Execute]
    /// [Sync]
    /// ```
    ///
    /// Response processing: consume 2× ReadyForQuery (SimpleQuery + Sync).
    pub async fn fetch_all_with_rls(
        &mut self,
        cmd: &qail_core::ast::Qail,
        rls_sql: &str,
    ) -> PgResult<Vec<crate::driver::PgRow>> {
        self.fetch_all_with_rls_with_format(cmd, rls_sql, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command with RLS context in a SINGLE roundtrip with explicit result format.
    pub async fn fetch_all_with_rls_with_format(
        &mut self,
        cmd: &qail_core::ast::Qail,
        rls_sql: &str,
        result_format: ResultFormat,
    ) -> PgResult<Vec<crate::driver::PgRow>> {
        let mut retried = false;
        loop {
            match self
                .fetch_all_with_rls_with_format_once(cmd, rls_sql, result_format)
                .await
            {
                Ok(rows) => return Ok(rows),
                Err(err)
                    if !retried
                        && (err.is_prepared_statement_retryable()
                            || err.is_prepared_statement_already_exists()) =>
                {
                    retried = true;
                    if err.is_prepared_statement_retryable()
                        && let Some(conn) = self.conn.as_mut()
                    {
                        conn.clear_prepared_statement_state();
                        let _ = conn.execute_simple("ROLLBACK").await;
                    }
                    self.rls_dirty = false;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn fetch_all_with_rls_with_format_once(
        &mut self,
        cmd: &qail_core::ast::Qail,
        rls_sql: &str,
        result_format: ResultFormat,
    ) -> PgResult<Vec<crate::driver::PgRow>> {
        use crate::driver::ColumnInfo;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let conn = self.conn.as_mut().ok_or_else(|| {
            PgError::Connection("Connection already released back to pool".into())
        })?;

        conn.sql_buf.clear();
        conn.params_buf.clear();

        // Encode SQL + params to reusable buffers
        if cmd.is_raw_sql() {
            // Raw SQL pass-through: write verbatim, RLS context already set above
            conn.sql_buf.clear();
            conn.params_buf.clear();
            conn.sql_buf.extend_from_slice(cmd.table.as_bytes());
        } else {
            match cmd.action {
                qail_core::ast::Action::Get | qail_core::ast::Action::With => {
                    crate::protocol::ast_encoder::dml::encode_select(
                        cmd,
                        &mut conn.sql_buf,
                        &mut conn.params_buf,
                    )?;
                }
                qail_core::ast::Action::Add => {
                    crate::protocol::ast_encoder::dml::encode_insert(
                        cmd,
                        &mut conn.sql_buf,
                        &mut conn.params_buf,
                    )?;
                }
                qail_core::ast::Action::Set => {
                    crate::protocol::ast_encoder::dml::encode_update(
                        cmd,
                        &mut conn.sql_buf,
                        &mut conn.params_buf,
                    )?;
                }
                qail_core::ast::Action::Del => {
                    crate::protocol::ast_encoder::dml::encode_delete(
                        cmd,
                        &mut conn.sql_buf,
                        &mut conn.params_buf,
                    )?;
                }
                _ => {
                    // Fallback: RLS setup must happen synchronously for unsupported actions
                    conn.execute_simple(rls_sql).await?;
                    self.rls_dirty = true;
                    return self
                        .fetch_all_uncached_with_format(cmd, result_format)
                        .await;
                }
            }
        }

        let mut hasher = DefaultHasher::new();
        conn.sql_buf.hash(&mut hasher);
        let sql_hash = hasher.finish();

        let is_cache_miss = !conn.stmt_cache.contains(&sql_hash);

        conn.write_buf.clear();

        // ── Prepend RLS Simple Query message ─────────────────────────
        // This is the key optimization: RLS setup bytes go first in the
        // same buffer as the query messages.
        let rls_msg = crate::protocol::PgEncoder::try_encode_query_string(rls_sql)?;
        conn.write_buf.extend_from_slice(&rls_msg);

        // ── Then append the query messages (same as fetch_all_cached) ──
        let stmt_name = if let Some(name) = conn.stmt_cache.get(&sql_hash) {
            name
        } else {
            let name = format!("qail_{:x}", sql_hash);

            conn.evict_prepared_if_full();

            let sql_str = std::str::from_utf8(&conn.sql_buf).unwrap_or("");

            use crate::protocol::PgEncoder;
            let parse_msg = PgEncoder::try_encode_parse(&name, sql_str, &[])?;
            let describe_msg = PgEncoder::try_encode_describe(false, &name)?;
            conn.write_buf.extend_from_slice(&parse_msg);
            conn.write_buf.extend_from_slice(&describe_msg);

            conn.stmt_cache.put(sql_hash, name.clone());
            conn.prepared_statements
                .insert(name.clone(), sql_str.to_string());

            if let Ok(mut hot) = self.pool.hot_statements.write()
                && hot.len() < MAX_HOT_STATEMENTS
            {
                hot.insert(sql_hash, (name.clone(), sql_str.to_string()));
            }

            name
        };

        use crate::protocol::PgEncoder;
        if let Err(e) = PgEncoder::encode_bind_to_with_result_format(
            &mut conn.write_buf,
            &stmt_name,
            &conn.params_buf,
            result_format.as_wire_code(),
        ) {
            if is_cache_miss {
                conn.stmt_cache.remove(&sql_hash);
                conn.prepared_statements.remove(&stmt_name);
                conn.column_info_cache.remove(&sql_hash);
            }
            return Err(PgError::Encode(e.to_string()));
        }
        PgEncoder::encode_execute_to(&mut conn.write_buf);
        PgEncoder::encode_sync_to(&mut conn.write_buf);

        // ── Single write_all for RLS + Query ────────────────────────
        if let Err(err) = conn.flush_write_buf().await {
            if is_cache_miss {
                conn.stmt_cache.remove(&sql_hash);
                conn.prepared_statements.remove(&stmt_name);
                conn.column_info_cache.remove(&sql_hash);
            }
            return Err(err);
        }

        // Mark connection as RLS-dirty (needs COMMIT on release)
        self.rls_dirty = true;

        // ── Phase 1: Consume Simple Query responses (RLS setup) ─────
        // Simple Query produces: CommandComplete × N, then ReadyForQuery.
        // set_config results and BEGIN/SET LOCAL responses are all here.
        let mut rls_error: Option<PgError> = None;
        loop {
            let msg = match conn.recv().await {
                Ok(msg) => msg,
                Err(err) => {
                    if is_cache_miss {
                        conn.stmt_cache.remove(&sql_hash);
                        conn.prepared_statements.remove(&stmt_name);
                        conn.column_info_cache.remove(&sql_hash);
                    }
                    return Err(err);
                }
            };
            match msg {
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    // RLS setup done — break to Extended Query phase
                    if let Some(err) = rls_error {
                        return Err(err);
                    }
                    break;
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if rls_error.is_none() {
                        rls_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                // CommandComplete, DataRow (from set_config), RowDescription — ignore
                crate::protocol::BackendMessage::CommandComplete(_)
                | crate::protocol::BackendMessage::DataRow(_)
                | crate::protocol::BackendMessage::RowDescription(_)
                | crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                msg if is_ignorable_session_message(&msg) => {}
                other => return Err(unexpected_backend_message("pool rls setup", &other)),
            }
        }

        // ── Phase 2: Consume Extended Query responses (actual data) ──
        let cached_column_info = conn.column_info_cache.get(&sql_hash).cloned();

        let mut rows: Vec<crate::driver::PgRow> = Vec::with_capacity(32);
        let mut column_info: Option<std::sync::Arc<ColumnInfo>> = cached_column_info;
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(
            ExtendedFlowConfig::parse_describe_statement_bind_execute(is_cache_miss),
        );

        loop {
            let msg = match conn.recv().await {
                Ok(msg) => msg,
                Err(err) => {
                    if is_cache_miss && !flow.saw_parse_complete() {
                        conn.stmt_cache.remove(&sql_hash);
                        conn.prepared_statements.remove(&stmt_name);
                        conn.column_info_cache.remove(&sql_hash);
                    }
                    return Err(err);
                }
            };
            if let Err(err) =
                flow.validate(&msg, "pool fetch_all_with_rls execute", error.is_some())
            {
                if is_cache_miss && !flow.saw_parse_complete() {
                    conn.stmt_cache.remove(&sql_hash);
                    conn.prepared_statements.remove(&stmt_name);
                    conn.column_info_cache.remove(&sql_hash);
                }
                return Err(err);
            }
            match msg {
                crate::protocol::BackendMessage::ParseComplete => {}
                crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::ParameterDescription(_) => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    let info = std::sync::Arc::new(ColumnInfo::from_fields(&fields));
                    if is_cache_miss {
                        conn.column_info_cache.insert(sql_hash, info.clone());
                    }
                    column_info = Some(info);
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(crate::driver::PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        if is_cache_miss
                            && !flow.saw_parse_complete()
                            && !err.is_prepared_statement_already_exists()
                        {
                            conn.stmt_cache.remove(&sql_hash);
                            conn.prepared_statements.remove(&stmt_name);
                            conn.column_info_cache.remove(&sql_hash);
                        }
                        return Err(err);
                    }
                    if is_cache_miss && !flow.saw_parse_complete() {
                        conn.stmt_cache.remove(&sql_hash);
                        conn.prepared_statements.remove(&stmt_name);
                        conn.column_info_cache.remove(&sql_hash);
                        return Err(PgError::Protocol(
                            "Cache miss query reached ReadyForQuery without ParseComplete"
                                .to_string(),
                        ));
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
                    if is_cache_miss && !flow.saw_parse_complete() {
                        conn.stmt_cache.remove(&sql_hash);
                        conn.prepared_statements.remove(&stmt_name);
                        conn.column_info_cache.remove(&sql_hash);
                    }
                    return Err(unexpected_backend_message(
                        "pool fetch_all_with_rls execute",
                        &other,
                    ));
                }
            }
        }
    }
}
