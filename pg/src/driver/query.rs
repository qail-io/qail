//! Query execution methods for PostgreSQL connection.
//!
//! This module provides query, query_cached, and execute_simple.

use super::{
    PgConnection, PgError, PgResult,
    extended_flow::{ExtendedFlowConfig, ExtendedFlowTracker},
    is_ignorable_session_message, is_ignorable_session_msg_type, unexpected_backend_message,
    unexpected_backend_msg_type,
};
use crate::protocol::{BackendMessage, PgEncoder};
use bytes::BytesMut;

#[inline]
fn capture_query_server_error(conn: &mut PgConnection, slot: &mut Option<PgError>, err: PgError) {
    if slot.is_some() {
        return;
    }
    if err.is_prepared_statement_retryable() {
        conn.clear_prepared_statement_state();
    }
    *slot = Some(err);
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimpleStatementState {
    AwaitingResult,
    InRowStream,
}

#[derive(Debug, Clone, Copy)]
struct SimpleFlowTracker {
    state: SimpleStatementState,
    saw_completion: bool,
}

impl SimpleFlowTracker {
    fn new() -> Self {
        Self {
            state: SimpleStatementState::AwaitingResult,
            saw_completion: false,
        }
    }

    fn on_row_description(&mut self, context: &'static str) -> PgResult<()> {
        if self.state == SimpleStatementState::InRowStream {
            return Err(PgError::Protocol(format!(
                "{}: duplicate RowDescription before statement completion",
                context
            )));
        }
        self.state = SimpleStatementState::InRowStream;
        self.saw_completion = false;
        Ok(())
    }

    fn on_data_row(&self, context: &'static str) -> PgResult<()> {
        if self.state != SimpleStatementState::InRowStream {
            return Err(PgError::Protocol(format!(
                "{}: DataRow before RowDescription",
                context
            )));
        }
        Ok(())
    }

    fn on_command_complete(&mut self) {
        self.state = SimpleStatementState::AwaitingResult;
        self.saw_completion = true;
    }

    fn on_empty_query_response(&mut self, context: &'static str) -> PgResult<()> {
        if self.state == SimpleStatementState::InRowStream {
            return Err(PgError::Protocol(format!(
                "{}: EmptyQueryResponse during active row stream",
                context
            )));
        }
        self.saw_completion = true;
        Ok(())
    }

    fn on_ready_for_query(&self, context: &'static str, error_pending: bool) -> PgResult<()> {
        if error_pending {
            return Ok(());
        }
        if self.state == SimpleStatementState::InRowStream {
            return Err(PgError::Protocol(format!(
                "{}: ReadyForQuery before CommandComplete",
                context
            )));
        }
        if !self.saw_completion {
            return Err(PgError::Protocol(format!(
                "{}: ReadyForQuery before completion",
                context
            )));
        }
        Ok(())
    }
}

impl PgConnection {
    fn validate_param_type_arity(params: &[Option<Vec<u8>>], param_types: &[u32]) -> PgResult<()> {
        if !param_types.is_empty() && param_types.len() != params.len() {
            return Err(PgError::Encode(format!(
                "parameter type count {} does not match parameter count {}",
                param_types.len(),
                params.len()
            )));
        }
        Ok(())
    }

    /// Execute a query with binary parameters (crate-internal).
    /// This uses the Extended Query Protocol (Parse/Bind/Execute/Sync):
    /// - Parameters are sent as binary bytes, skipping the string layer
    /// - No SQL injection possible - parameters are never interpolated
    /// - Better performance via prepared statement reuse
    pub(crate) async fn query(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        self.query_with_result_format(sql, params, PgEncoder::FORMAT_TEXT)
            .await
    }

    /// Execute a query with binary parameters and explicit result-column format.
    pub(crate) async fn query_with_result_format(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        let bytes = PgEncoder::encode_extended_query_with_result_format(sql, params, result_format)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        self.write_all_with_timeout(&bytes, "stream write").await?;

        let mut rows = Vec::new();

        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));

        loop {
            let msg = self.recv().await?;
            flow.validate(&msg, "extended-query execute", error.is_some())?;
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    // Only collect rows if no error occurred
                    if error.is_none() {
                        rows.push(data);
                    }
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::NoData => {}
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return Err(unexpected_backend_message("extended-query execute", &other));
                }
            }
        }
    }

    /// Execute a query with bind parameters and return rows with column metadata.
    ///
    /// Uses the Extended Query Protocol without prepared statement caching.
    /// This is intended for raw SQL compatibility paths that still need
    /// `PgRow` + `ColumnInfo` for name-aware JSON conversion.
    pub async fn query_rows(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<Vec<super::PgRow>> {
        self.query_rows_with_result_format(sql, params, PgEncoder::FORMAT_TEXT)
            .await
    }

    /// Execute a query with bind parameters and explicit result-column format,
    /// returning rows with column metadata.
    pub async fn query_rows_with_result_format(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> PgResult<Vec<super::PgRow>> {
        self.query_rows_with_param_types_and_result_format(sql, &[], params, result_format)
            .await
    }

    /// Execute an uncached query and stream zero-copy rows to `on_row`.
    ///
    /// This is the lowest-allocation raw-SQL path for large result sets when
    /// the caller does not need `PgRow` materialization or column-name metadata.
    pub async fn query_visit_bytes_rows_with_result_format<F>(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
        result_format: i16,
        on_row: F,
    ) -> PgResult<usize>
    where
        F: FnMut(&super::PgBytesRow) -> PgResult<()>,
    {
        self.query_visit_bytes_rows_with_param_types_and_result_format(
            sql,
            &[],
            params,
            result_format,
            on_row,
        )
        .await
    }

    /// Execute a query with explicit PostgreSQL parameter type OIDs and return
    /// rows with column metadata.
    pub async fn query_rows_with_param_types_and_result_format(
        &mut self,
        sql: &str,
        param_types: &[u32],
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> PgResult<Vec<super::PgRow>> {
        use std::sync::Arc;

        Self::validate_param_type_arity(params, param_types)?;

        let parse = PgEncoder::try_encode_parse("", sql, param_types)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        let bind = PgEncoder::encode_bind_with_result_format("", "", params, result_format)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        let mut bytes = BytesMut::with_capacity(parse.len() + bind.len() + 10 + 5);
        bytes.extend_from_slice(&parse);
        bytes.extend_from_slice(&bind);
        PgEncoder::encode_execute_to(&mut bytes);
        PgEncoder::encode_sync_to(&mut bytes);
        self.write_all_with_timeout(&bytes, "stream write").await?;

        let mut rows: Vec<super::PgRow> = Vec::new();
        let mut column_info: Option<Arc<super::ColumnInfo>> = None;
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));

        loop {
            let msg = self.recv().await?;
            flow.validate(&msg, "extended-query rows execute", error.is_some())?;
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(fields) => {
                    column_info = Some(Arc::new(super::ColumnInfo::from_fields(&fields)));
                }
                BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(super::PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::NoData => {}
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return Err(unexpected_backend_message(
                        "extended-query rows execute",
                        &other,
                    ));
                }
            }
        }
    }

    /// Execute an uncached query with explicit PostgreSQL parameter types and
    /// stream zero-copy rows to `on_row`.
    ///
    /// Rows are backed by one shared payload buffer plus column offsets, so the
    /// callback must not hold references past the current invocation.
    pub async fn query_visit_bytes_rows_with_param_types_and_result_format<F>(
        &mut self,
        sql: &str,
        param_types: &[u32],
        params: &[Option<Vec<u8>>],
        result_format: i16,
        mut on_row: F,
    ) -> PgResult<usize>
    where
        F: FnMut(&super::PgBytesRow) -> PgResult<()>,
    {
        Self::validate_param_type_arity(params, param_types)?;

        self.write_buf.clear();
        self.write_buf.extend_from_slice(
            &PgEncoder::try_encode_parse("", sql, param_types)
                .map_err(|e| PgError::Encode(e.to_string()))?,
        );
        self.write_buf.extend_from_slice(
            &PgEncoder::encode_bind_with_result_format("", "", params, result_format)
                .map_err(|e| PgError::Encode(e.to_string()))?,
        );
        PgEncoder::encode_execute_to(&mut self.write_buf);
        PgEncoder::encode_sync_to(&mut self.write_buf);

        self.flush_write_buf().await?;

        let mut row_count = 0usize;
        let mut row = super::PgBytesRow::default();
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));

        loop {
            match self.recv_fill_zerocopy_row_fast(&mut row).await {
                Ok(msg_type) => {
                    flow.validate_msg_type(
                        msg_type,
                        "extended-query visit bytes execute",
                        error.is_some(),
                    )?;
                    match msg_type {
                        b'1' | b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                on_row(&row)?;
                                row_count += 1;
                            }
                        }
                        b'C' => {}
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(row_count);
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return Err(unexpected_backend_msg_type(
                                "extended-query visit bytes execute",
                                other,
                            ));
                        }
                    }
                }
                Err(e) => {
                    if matches!(&e, PgError::QueryServer(_)) {
                        capture_query_server_error(self, &mut error, e);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Validate a query with explicit PostgreSQL parameter type OIDs without
    /// executing it. Uses Parse + Bind + Describe(Portal) + Sync.
    pub async fn probe_query_with_param_types(
        &mut self,
        sql: &str,
        param_types: &[u32],
        params: &[Option<Vec<u8>>],
    ) -> PgResult<()> {
        Self::validate_param_type_arity(params, param_types)?;

        let parse = PgEncoder::try_encode_parse("", sql, param_types)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        let bind =
            PgEncoder::encode_bind("", "", params).map_err(|e| PgError::Encode(e.to_string()))?;
        let describe =
            PgEncoder::try_encode_describe(true, "").map_err(|e| PgError::Encode(e.to_string()))?;
        let sync = PgEncoder::encode_sync();
        let mut bytes =
            BytesMut::with_capacity(parse.len() + bind.len() + describe.len() + sync.len());
        bytes.extend_from_slice(&parse);
        bytes.extend_from_slice(&bind);
        bytes.extend_from_slice(&describe);
        bytes.extend_from_slice(&sync);
        self.write_all_with_timeout(&bytes, "stream write").await?;

        let mut saw_describe_response = false;
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_describe_portal());

        loop {
            let msg = self.recv().await?;
            flow.validate(&msg, "extended-query probe", error.is_some())?;
            match msg {
                BackendMessage::ParseComplete => {}
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) | BackendMessage::NoData => {
                    saw_describe_response = true;
                }
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    if !saw_describe_response {
                        return Err(PgError::Protocol(
                            "extended-query probe finished without RowDescription/NoData"
                                .to_string(),
                        ));
                    }
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return Err(unexpected_backend_message("extended-query probe", &other));
                }
            }
        }
    }

    /// Execute a query with cached prepared statement.
    /// Like `query()`, but reuses prepared statements across calls.
    /// The statement name is derived from a hash of the SQL text.
    /// OPTIMIZED: Pre-allocated buffer + ultra-fast encoders.
    pub async fn query_cached(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        self.query_cached_with_result_format(sql, params, PgEncoder::FORMAT_TEXT)
            .await
    }

    /// Execute a query with cached prepared statement and explicit result-column format.
    pub async fn query_cached_with_result_format(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        let mut retried = false;
        loop {
            match self
                .query_cached_with_result_format_once(sql, params, result_format)
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
                        self.clear_prepared_statement_state();
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn query_cached_with_result_format_once(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        let stmt_name = Self::sql_to_stmt_name(sql);
        let is_new = !self.prepared_statements.contains_key(&stmt_name);

        // Pre-calculate buffer size for single allocation
        let params_size: usize = params
            .iter()
            .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
            .sum();

        let estimated_size = if is_new {
            50 + sql.len() + stmt_name.len() * 2 + params_size
        } else {
            30 + stmt_name.len() + params_size
        };

        let mut buf = BytesMut::with_capacity(estimated_size);

        if is_new {
            // Evict LRU prepared statement if at capacity. This prevents
            // unbounded memory growth from dynamic batch filters while
            // preserving hot statements (unlike the old nuclear `.clear()`).
            self.evict_prepared_if_full();
            buf.extend(PgEncoder::try_encode_parse(&stmt_name, sql, &[])?);
            // Cache the SQL for debugging
            self.prepared_statements
                .insert(stmt_name.clone(), sql.to_string());
        }

        // Use ULTRA-OPTIMIZED encoders - write directly to buffer
        if let Err(e) = PgEncoder::encode_bind_to_with_result_format(
            &mut buf,
            &stmt_name,
            params,
            result_format,
        ) {
            if is_new {
                self.prepared_statements.remove(&stmt_name);
            }
            return Err(PgError::Encode(e.to_string()));
        }
        PgEncoder::encode_execute_to(&mut buf);
        PgEncoder::encode_sync_to(&mut buf);

        if let Err(err) = self.write_all_with_timeout(&buf, "stream write").await {
            if is_new {
                self.prepared_statements.remove(&stmt_name);
            }
            return Err(err);
        }

        let mut rows = Vec::new();

        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(is_new));

        loop {
            let msg = match self.recv().await {
                Ok(msg) => msg,
                Err(err) => {
                    if is_new && !flow.saw_parse_complete() {
                        self.prepared_statements.remove(&stmt_name);
                    }
                    return Err(err);
                }
            };
            if let Err(err) = flow.validate(&msg, "extended-query cached execute", error.is_some())
            {
                if is_new && !flow.saw_parse_complete() {
                    self.prepared_statements.remove(&stmt_name);
                }
                return Err(err);
            }
            match msg {
                BackendMessage::ParseComplete => {
                    // Already cached in is_new block above.
                }
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(data);
                    }
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::NoData => {}
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        if is_new
                            && !flow.saw_parse_complete()
                            && !err.is_prepared_statement_already_exists()
                        {
                            self.prepared_statements.remove(&stmt_name);
                        }
                        return Err(err);
                    }
                    if is_new && !flow.saw_parse_complete() {
                        self.prepared_statements.remove(&stmt_name);
                        return Err(PgError::Protocol(
                            "Cache miss query reached ReadyForQuery without ParseComplete"
                                .to_string(),
                        ));
                    }
                    return Ok(rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        let query_err = PgError::QueryServer(err.into());
                        if !query_err.is_prepared_statement_already_exists() {
                            // Invalidate cache to prevent stale local mapping after parse failure.
                            self.prepared_statements.remove(&stmt_name);
                        }
                        error = Some(query_err);
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    if is_new && !flow.saw_parse_complete() {
                        self.prepared_statements.remove(&stmt_name);
                    }
                    return Err(unexpected_backend_message(
                        "extended-query cached execute",
                        &other,
                    ));
                }
            }
        }
    }

    /// Generate a statement name from SQL hash.
    /// Uses a simple hash to create a unique name like "stmt_12345abc".
    pub(crate) fn sql_to_stmt_name(sql: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        format!("s{:016x}", hasher.finish())
    }

    /// Execute a simple SQL statement (no parameters).
    pub async fn execute_simple(&mut self, sql: &str) -> PgResult<()> {
        let bytes = PgEncoder::try_encode_query_string(sql)?;
        self.write_all_with_timeout(&bytes, "stream write").await?;

        let mut error: Option<PgError> = None;
        let mut flow = SimpleFlowTracker::new();

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::RowDescription(_) => {
                    // Some callers use execute_simple() with session-shaping SQL that
                    // can legally return rows (e.g., SELECT set_config(...)).
                    // Drain and ignore row data while preserving protocol ordering checks.
                    flow.on_row_description("simple-query execute")?;
                }
                BackendMessage::DataRow(_) => {
                    flow.on_data_row("simple-query execute")?;
                }
                BackendMessage::CommandComplete(_) => {
                    flow.on_command_complete();
                }
                BackendMessage::EmptyQueryResponse => {
                    flow.on_empty_query_response("simple-query execute")?;
                }
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    flow.on_ready_for_query("simple-query execute", error.is_some())?;
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return Err(unexpected_backend_message("simple-query execute", &other));
                }
            }
        }
    }

    /// Execute a simple SQL query and return rows (Simple Query Protocol).
    ///
    /// Unlike `execute_simple`, this collects and returns data rows.
    /// Used for branch management and other administrative queries.
    ///
    /// SECURITY: Capped at 10,000 rows to prevent OOM from unbounded results.
    pub async fn simple_query(&mut self, sql: &str) -> PgResult<Vec<super::PgRow>> {
        use std::sync::Arc;

        /// Safety cap to prevent OOM from unbounded result accumulation.
        /// Simple Query Protocol has no streaming; all rows are buffered in memory.
        const MAX_SIMPLE_QUERY_ROWS: usize = 10_000;

        let bytes = PgEncoder::try_encode_query_string(sql)?;
        self.write_all_with_timeout(&bytes, "stream write").await?;

        let mut rows: Vec<super::PgRow> = Vec::new();
        let mut column_info: Option<Arc<super::ColumnInfo>> = None;
        let mut error: Option<PgError> = None;
        let mut flow = SimpleFlowTracker::new();

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::RowDescription(fields) => {
                    flow.on_row_description("simple-query read")?;
                    column_info = Some(Arc::new(super::ColumnInfo::from_fields(&fields)));
                }
                BackendMessage::DataRow(data) => {
                    flow.on_data_row("simple-query read")?;
                    if error.is_none() {
                        if rows.len() >= MAX_SIMPLE_QUERY_ROWS {
                            if error.is_none() {
                                error = Some(PgError::Query(format!(
                                    "simple_query exceeded {} row safety cap",
                                    MAX_SIMPLE_QUERY_ROWS,
                                )));
                            }
                            // Continue draining to reach ReadyForQuery
                        } else {
                            rows.push(super::PgRow {
                                columns: data,
                                column_info: column_info.clone(),
                            });
                        }
                    }
                }
                BackendMessage::CommandComplete(_) => {
                    flow.on_command_complete();
                    column_info = None;
                }
                BackendMessage::EmptyQueryResponse => {
                    flow.on_empty_query_response("simple-query read")?;
                    column_info = None;
                }
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    flow.on_ready_for_query("simple-query read", error.is_some())?;
                    return Ok(rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return Err(unexpected_backend_message("simple-query read", &other));
                }
            }
        }
    }

    /// ZERO-HASH sequential query using pre-computed PreparedStatement.
    /// This is the FASTEST sequential path because it skips:
    /// - SQL generation from AST (done once outside loop)
    /// - Hash computation for statement name (pre-computed in PreparedStatement)
    /// - HashMap lookup for is_new check (statement already prepared)
    /// # Example
    /// ```ignore
    /// let stmt = conn.prepare("SELECT * FROM users WHERE id = $1").await?;
    /// for id in 1..10000 {
    ///     let rows = conn.query_prepared_single(&stmt, &[Some(id.to_string().into_bytes())]).await?;
    /// }
    /// ```
    #[inline]
    pub async fn query_prepared_single(
        &mut self,
        stmt: &super::PreparedStatement,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        self.query_prepared_single_with_result_format(stmt, params, PgEncoder::FORMAT_TEXT)
            .await
    }

    /// ZERO-HASH sequential prepared execution that drains rows without
    /// materializing them.
    ///
    /// Useful for throughput-focused paths where only protocol completion
    /// matters and result payload is intentionally ignored.
    #[inline]
    pub async fn query_prepared_single_count(
        &mut self,
        stmt: &super::PreparedStatement,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<()> {
        let params_size: usize = params
            .iter()
            .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
            .sum();

        let mut buf = BytesMut::with_capacity(30 + stmt.name.len() + params_size);

        PgEncoder::encode_bind_to(&mut buf, &stmt.name, params)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut buf);
        PgEncoder::encode_sync_to(&mut buf);

        self.write_all_with_timeout(&buf, "stream write").await?;

        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            let msg = self.recv().await?;
            flow.validate(&msg, "prepared single count execute", error.is_some())?;
            match msg {
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(_) => {}
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::NoData => {}
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return Err(unexpected_backend_message(
                        "prepared single count execute",
                        &other,
                    ));
                }
            }
        }
    }

    /// ZERO-HASH sequential query with explicit result-column format.
    #[inline]
    pub async fn query_prepared_single_with_result_format(
        &mut self,
        stmt: &super::PreparedStatement,
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        // Pre-calculate buffer size for single allocation
        let params_size: usize = params
            .iter()
            .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
            .sum();

        let mut buf = BytesMut::with_capacity(30 + stmt.name.len() + params_size);

        // ZERO HASH, ZERO LOOKUP - just encode and send!
        PgEncoder::encode_bind_to_with_result_format(&mut buf, &stmt.name, params, result_format)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut buf);
        PgEncoder::encode_sync_to(&mut buf);

        self.write_all_with_timeout(&buf, "stream write").await?;

        let mut rows = Vec::new();

        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            let msg = self.recv().await?;
            flow.validate(&msg, "prepared single execute", error.is_some())?;
            match msg {
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(data);
                    }
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::NoData => {}
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return Err(unexpected_backend_message(
                        "prepared single execute",
                        &other,
                    ));
                }
            }
        }
    }

    /// ZERO-HASH sequential query with explicit result-column format using
    /// reusable connection buffers (avoids per-call `BytesMut` allocation).
    #[inline]
    pub async fn query_prepared_single_reuse_with_result_format(
        &mut self,
        stmt: &super::PreparedStatement,
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        self.write_buf.clear();

        PgEncoder::encode_bind_to_with_result_format(
            &mut self.write_buf,
            &stmt.name,
            params,
            result_format,
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut self.write_buf);
        PgEncoder::encode_sync_to(&mut self.write_buf);

        self.flush_write_buf().await?;

        let mut rows = Vec::with_capacity(32);
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            let msg = self.recv().await?;
            flow.validate(&msg, "prepared single reuse execute", error.is_some())?;
            match msg {
                BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(data);
                    }
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::NoData => {}
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return Err(unexpected_backend_message(
                        "prepared single reuse execute",
                        &other,
                    ));
                }
            }
        }
    }

    /// Sequential prepared query using reusable connection buffers and row visitor.
    ///
    /// Rows are streamed to `on_row` as owned column buffers, avoiding
    /// materializing the full result set.
    #[inline]
    pub async fn query_prepared_single_reuse_visit_rows_with_result_format<F>(
        &mut self,
        stmt: &super::PreparedStatement,
        params: &[Option<Vec<u8>>],
        result_format: i16,
        mut on_row: F,
    ) -> PgResult<usize>
    where
        F: FnMut(&[Option<Vec<u8>>]) -> PgResult<()>,
    {
        self.write_buf.clear();

        PgEncoder::encode_bind_to_with_result_format(
            &mut self.write_buf,
            &stmt.name,
            params,
            result_format,
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut self.write_buf);
        PgEncoder::encode_sync_to(&mut self.write_buf);

        self.flush_write_buf().await?;

        let mut row_count = 0usize;
        let mut row_buf: Vec<Option<Vec<u8>>> = Vec::new();
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            match self.recv_fill_data_row_fast(&mut row_buf).await {
                Ok(msg_type) => {
                    flow.validate_msg_type(
                        msg_type,
                        "prepared single reuse visit execute",
                        error.is_some(),
                    )?;
                    match msg_type {
                        b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                on_row(row_buf.as_slice())?;
                                row_count += 1;
                            }
                        }
                        b'C' => {}
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(row_count);
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return Err(unexpected_backend_msg_type(
                                "prepared single reuse visit execute",
                                other,
                            ));
                        }
                    }
                }
                Err(e) => {
                    if matches!(&e, PgError::QueryServer(_)) {
                        capture_query_server_error(self, &mut error, e);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Sequential prepared query using reusable connection buffers and zero-copy row visitor.
    ///
    /// Rows are backed by a shared payload buffer plus column offsets, avoiding
    /// per-cell byte copies during receive.
    #[inline]
    pub async fn query_prepared_single_reuse_visit_bytes_rows_with_result_format<F>(
        &mut self,
        stmt: &super::PreparedStatement,
        params: &[Option<Vec<u8>>],
        result_format: i16,
        mut on_row: F,
    ) -> PgResult<usize>
    where
        F: FnMut(&super::PgBytesRow) -> PgResult<()>,
    {
        self.write_buf.clear();

        PgEncoder::encode_bind_to_with_result_format(
            &mut self.write_buf,
            &stmt.name,
            params,
            result_format,
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut self.write_buf);
        PgEncoder::encode_sync_to(&mut self.write_buf);

        self.flush_write_buf().await?;

        let mut row_count = 0usize;
        let mut row = super::PgBytesRow::default();
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            match self.recv_fill_zerocopy_row_fast(&mut row).await {
                Ok(msg_type) => {
                    flow.validate_msg_type(
                        msg_type,
                        "prepared single reuse visit bytes execute",
                        error.is_some(),
                    )?;
                    match msg_type {
                        b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                on_row(&row)?;
                                row_count += 1;
                            }
                        }
                        b'C' => {}
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(row_count);
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return Err(unexpected_backend_msg_type(
                                "prepared single reuse visit bytes execute",
                                other,
                            ));
                        }
                    }
                }
                Err(e) => {
                    if matches!(&e, PgError::QueryServer(_)) {
                        capture_query_server_error(self, &mut error, e);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }
}
