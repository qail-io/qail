//! Query execution methods for PostgreSQL connection.
//!
//! This module provides query, query_cached, and execute_simple.

use super::{
    PgConnection, PgError, PgResult,
    extended_flow::{ExtendedFlowConfig, ExtendedFlowTracker},
    is_ignorable_session_message, unexpected_backend_message,
};
use crate::protocol::{BackendMessage, PgEncoder};
use bytes::BytesMut;

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
        let mut flow =
            ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(is_new));

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
            if let Err(err) = flow.validate(&msg, "extended-query cached execute", error.is_some()) {
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

        // Bind: ~15 + stmt.name.len() + params_size, Execute: 10, Sync: 5
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
}
