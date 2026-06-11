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
use bytes::{Bytes, BytesMut};
use std::time::{Duration, Instant};

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

#[inline]
fn return_callback_error_with_desync<T>(conn: &mut PgConnection, err: PgError) -> PgResult<T> {
    conn.mark_io_desynced();
    Err(err)
}

#[inline]
fn prepared_bind_execute_sync_wire_len(
    statement: &str,
    params: &[Option<Vec<u8>>],
    result_format: i16,
) -> PgResult<usize> {
    let needed = PgEncoder::bind_execute_sync_wire_len_with_formats(
        statement,
        params,
        PgEncoder::FORMAT_TEXT,
        result_format,
    )
    .map_err(|e| PgError::Encode(e.to_string()))?;
    Ok(needed)
}

#[inline]
fn reserve_prepared_single_write_buf(
    conn: &mut PgConnection,
    stmt: &super::PreparedStatement,
    params: &[Option<Vec<u8>>],
    result_format: i16,
) -> PgResult<()> {
    conn.write_buf.clear();
    let needed = prepared_bind_execute_sync_wire_len(&stmt.name, params, result_format)?;
    conn.write_buf.reserve(needed);
    Ok(())
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
        self.send_bytes(&bytes).await?;

        let mut rows = Vec::new();

        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));

        loop {
            let msg = self.recv().await?;
            if let Err(err) = flow.validate(&msg, "extended-query execute", error.is_some()) {
                return return_with_desync(self, err);
            }
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
                    return return_with_desync(
                        self,
                        unexpected_backend_message("extended-query execute", &other),
                    );
                }
            }
        }
    }

    /// Execute an uncached query and drain completion without materializing rows.
    pub async fn query_count(&mut self, sql: &str, params: &[Option<Vec<u8>>]) -> PgResult<()> {
        self.query_count_with_param_types(sql, &[], params).await
    }

    /// Execute an uncached query with explicit PostgreSQL parameter type OIDs
    /// and drain completion without materializing rows.
    pub async fn query_count_with_param_types(
        &mut self,
        sql: &str,
        param_types: &[u32],
        params: &[Option<Vec<u8>>],
    ) -> PgResult<()> {
        Self::validate_param_type_arity(params, param_types)?;

        self.write_buf.clear();
        PgEncoder::try_encode_parse_to(&mut self.write_buf, "", sql, param_types)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_bind_to(&mut self.write_buf, "", params)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut self.write_buf);
        PgEncoder::encode_sync_to(&mut self.write_buf);

        self.flush_write_buf().await?;

        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));

        loop {
            match self.recv_msg_type_fast().await {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "extended-query count execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'1' | b'2' | b'T' | b'D' | b'C' | b'n' => {}
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(());
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type("extended-query count execute", other),
                            );
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

    /// Execute an uncached query and stream only the first column of each row.
    pub async fn query_visit_first_column_bytes_with_result_format<F>(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
        result_format: i16,
        on_value: F,
    ) -> PgResult<usize>
    where
        F: FnMut(Option<&[u8]>) -> PgResult<()>,
    {
        self.query_visit_first_column_bytes_with_param_types_and_result_format(
            sql,
            &[],
            params,
            result_format,
            on_value,
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

        self.write_buf.clear();
        PgEncoder::try_encode_parse_to(&mut self.write_buf, "", sql, param_types)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_bind_to_with_result_format(
            &mut self.write_buf,
            "",
            params,
            result_format,
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;
        let describe_msg =
            PgEncoder::try_encode_describe(true, "").map_err(|e| PgError::Encode(e.to_string()))?;
        self.write_buf.extend_from_slice(&describe_msg);
        PgEncoder::encode_execute_to(&mut self.write_buf);
        PgEncoder::encode_sync_to(&mut self.write_buf);
        self.flush_write_buf().await?;

        let mut rows: Vec<super::PgRow> = Vec::new();
        let mut column_info: Option<Arc<super::ColumnInfo>> = None;
        let mut error: Option<PgError> = None;
        let mut flow =
            ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_describe_portal_execute());

        loop {
            let msg = self.recv().await?;
            if let Err(err) = flow.validate(&msg, "extended-query rows execute", error.is_some()) {
                return return_with_desync(self, err);
            }
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
                    return return_with_desync(
                        self,
                        unexpected_backend_message("extended-query rows execute", &other),
                    );
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
        PgEncoder::try_encode_parse_to(&mut self.write_buf, "", sql, param_types)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_bind_to_with_result_format(
            &mut self.write_buf,
            "",
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
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));

        loop {
            match self.recv_fill_zerocopy_row_fast(&mut row).await {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "extended-query visit bytes execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'1' | b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                if let Err(err) = on_row(&row) {
                                    return return_callback_error_with_desync(self, err);
                                }
                                row_count += 1;
                                row.release_payload();
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
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "extended-query visit bytes execute",
                                    other,
                                ),
                            );
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

    /// Execute an uncached query with explicit PostgreSQL parameter types and
    /// stream only the first column of each row.
    pub async fn query_visit_first_column_bytes_with_param_types_and_result_format<F>(
        &mut self,
        sql: &str,
        param_types: &[u32],
        params: &[Option<Vec<u8>>],
        result_format: i16,
        mut on_value: F,
    ) -> PgResult<usize>
    where
        F: FnMut(Option<&[u8]>) -> PgResult<()>,
    {
        Self::validate_param_type_arity(params, param_types)?;

        self.write_buf.clear();
        PgEncoder::try_encode_parse_to(&mut self.write_buf, "", sql, param_types)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_bind_to_with_result_format(
            &mut self.write_buf,
            "",
            params,
            result_format,
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut self.write_buf);
        PgEncoder::encode_sync_to(&mut self.write_buf);

        self.flush_write_buf().await?;

        let mut row_count = 0usize;
        let mut first_column: Option<Bytes> = None;
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(true));

        loop {
            match self
                .recv_fill_first_column_zerocopy_fast(&mut first_column)
                .await
            {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "extended-query visit first-column execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'1' | b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                if let Err(err) = on_value(first_column.as_deref()) {
                                    return return_callback_error_with_desync(self, err);
                                }
                                row_count += 1;
                                first_column = None;
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
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "extended-query visit first-column execute",
                                    other,
                                ),
                            );
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
        self.send_bytes(&bytes).await?;

        let mut saw_describe_response = false;
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_describe_portal());

        loop {
            let msg = self.recv().await?;
            if let Err(err) = flow.validate(&msg, "extended-query probe", error.is_some()) {
                return return_with_desync(self, err);
            }
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
                        return return_with_desync(
                            self,
                            PgError::Protocol(
                                "extended-query probe finished without RowDescription/NoData"
                                    .to_string(),
                            ),
                        );
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
                    return return_with_desync(
                        self,
                        unexpected_backend_message("extended-query probe", &other),
                    );
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

        let needed = prepared_bind_execute_sync_wire_len(&stmt_name, params, result_format)?;
        let mut buf = BytesMut::with_capacity(needed);

        if is_new {
            // Evict LRU prepared statement if at capacity. This prevents
            // unbounded memory growth from dynamic batch filters while
            // preserving hot statements (unlike the old nuclear `.clear()`).
            self.evict_prepared_if_full();
            if let Err(e) = PgEncoder::try_encode_parse_to(&mut buf, &stmt_name, sql, &[]) {
                return Err(PgError::Encode(e.to_string()));
            }
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

        if let Err(err) = self.send_bytes(&buf).await {
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
                return return_with_desync(self, err);
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
                BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        let query_err = PgError::QueryServer(err.into());
                        if query_err.is_prepared_statement_retryable()
                            || (is_new
                                && !flow.saw_parse_complete()
                                && !query_err.is_prepared_statement_already_exists())
                        {
                            // Invalidate cache only when the server-side prepared
                            // statement is known absent or the Parse step failed.
                            // Execution-stage errors after ParseComplete leave the
                            // statement usable on the backend.
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
                    return return_with_desync(
                        self,
                        unexpected_backend_message("extended-query cached execute", &other),
                    );
                }
            }
        }
    }

    /// Generate a statement name from SQL hash.
    /// Uses a simple hash to create a unique name like "stmt_12345abc".
    pub(crate) fn sql_to_stmt_name(sql: &str) -> String {
        super::prepared::sql_bytes_to_stmt_name(sql.as_bytes())
    }

    /// Execute a simple SQL statement (no parameters).
    pub async fn execute_simple(&mut self, sql: &str) -> PgResult<()> {
        let bytes = PgEncoder::try_encode_query_string(sql)?;
        self.send_bytes(&bytes).await?;

        let mut error: Option<PgError> = None;
        let mut flow = SimpleFlowTracker::new();

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::RowDescription(_) => {
                    // Some callers use execute_simple() with session-shaping SQL that
                    // can legally return rows (e.g., SELECT set_config(...)).
                    // Drain and ignore row data while preserving protocol ordering checks.
                    if let Err(err) = flow.on_row_description("simple-query execute") {
                        return return_with_desync(self, err);
                    }
                }
                BackendMessage::DataRow(_) => {
                    if let Err(err) = flow.on_data_row("simple-query execute") {
                        return return_with_desync(self, err);
                    }
                }
                BackendMessage::CommandComplete(_) => {
                    flow.on_command_complete();
                }
                BackendMessage::EmptyQueryResponse => {
                    if let Err(err) = flow.on_empty_query_response("simple-query execute") {
                        return return_with_desync(self, err);
                    }
                }
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    if let Err(err) =
                        flow.on_ready_for_query("simple-query execute", error.is_some())
                    {
                        return return_with_desync(self, err);
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
                    return return_with_desync(
                        self,
                        unexpected_backend_message("simple-query execute", &other),
                    );
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
        self.send_bytes(&bytes).await?;

        let mut rows: Vec<super::PgRow> = Vec::new();
        let mut column_info: Option<Arc<super::ColumnInfo>> = None;
        let mut error: Option<PgError> = None;
        let mut flow = SimpleFlowTracker::new();

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::RowDescription(fields) => {
                    if let Err(err) = flow.on_row_description("simple-query read") {
                        return return_with_desync(self, err);
                    }
                    column_info = Some(Arc::new(super::ColumnInfo::from_fields(&fields)));
                }
                BackendMessage::DataRow(data) => {
                    if let Err(err) = flow.on_data_row("simple-query read") {
                        return return_with_desync(self, err);
                    }
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
                    if let Err(err) = flow.on_empty_query_response("simple-query read") {
                        return return_with_desync(self, err);
                    }
                    column_info = None;
                }
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    if let Err(err) = flow.on_ready_for_query("simple-query read", error.is_some())
                    {
                        return return_with_desync(self, err);
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
                    return return_with_desync(
                        self,
                        unexpected_backend_message("simple-query read", &other),
                    );
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
        self.write_buf.clear();
        PgEncoder::encode_bind_to(&mut self.write_buf, &stmt.name, params)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut self.write_buf);
        PgEncoder::encode_sync_to(&mut self.write_buf);

        self.flush_write_buf().await?;

        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            match self.recv_msg_type_fast().await {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "prepared single count execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'D' | b'C' | b'n' => {}
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(());
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type("prepared single count execute", other),
                            );
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

    /// ZERO-HASH sequential query with explicit result-column format.
    #[inline]
    pub async fn query_prepared_single_with_result_format(
        &mut self,
        stmt: &super::PreparedStatement,
        params: &[Option<Vec<u8>>],
        result_format: i16,
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        let needed = prepared_bind_execute_sync_wire_len(&stmt.name, params, result_format)?;
        let mut buf = BytesMut::with_capacity(needed);

        // ZERO HASH, ZERO LOOKUP - just encode and send!
        PgEncoder::encode_bind_to_with_result_format(&mut buf, &stmt.name, params, result_format)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut buf);
        PgEncoder::encode_sync_to(&mut buf);

        self.send_bytes(&buf).await?;

        let mut rows = Vec::new();

        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            let msg = self.recv().await?;
            if let Err(err) = flow.validate(&msg, "prepared single execute", error.is_some()) {
                return return_with_desync(self, err);
            }
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
                    capture_query_server_error(self, &mut error, PgError::QueryServer(err.into()));
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("prepared single execute", &other),
                    );
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
        reserve_prepared_single_write_buf(self, stmt, params, result_format)?;

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
            if let Err(err) = flow.validate(&msg, "prepared single reuse execute", error.is_some())
            {
                return return_with_desync(self, err);
            }
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
                    capture_query_server_error(self, &mut error, PgError::QueryServer(err.into()));
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("prepared single reuse execute", &other),
                    );
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
        reserve_prepared_single_write_buf(self, stmt, params, result_format)?;

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
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "prepared single reuse visit execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                if let Err(err) = on_row(row_buf.as_slice()) {
                                    return return_callback_error_with_desync(self, err);
                                }
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
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "prepared single reuse visit execute",
                                    other,
                                ),
                            );
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
        reserve_prepared_single_write_buf(self, stmt, params, result_format)?;

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
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "prepared single reuse visit bytes execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                if let Err(err) = on_row(&row) {
                                    return return_callback_error_with_desync(self, err);
                                }
                                row_count += 1;
                                row.release_payload();
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
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "prepared single reuse visit bytes execute",
                                    other,
                                ),
                            );
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

    /// Sequential prepared query using reusable buffers and first-column visitor.
    #[inline]
    pub async fn query_prepared_single_reuse_visit_first_column_bytes_with_result_format<F>(
        &mut self,
        stmt: &super::PreparedStatement,
        params: &[Option<Vec<u8>>],
        result_format: i16,
        mut on_value: F,
    ) -> PgResult<usize>
    where
        F: FnMut(Option<&[u8]>) -> PgResult<()>,
    {
        reserve_prepared_single_write_buf(self, stmt, params, result_format)?;

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
        let mut first_column: Option<Bytes> = None;
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            match self
                .recv_fill_first_column_zerocopy_fast(&mut first_column)
                .await
            {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "prepared single reuse visit first-column execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                if let Err(err) = on_value(first_column.as_deref()) {
                                    return return_callback_error_with_desync(self, err);
                                }
                                row_count += 1;
                                first_column = None;
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
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "prepared single reuse visit first-column execute",
                                    other,
                                ),
                            );
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

    /// Sequential prepared query using reusable buffers and fixed 4-column visitor.
    #[inline]
    pub async fn query_prepared_single_reuse_visit_first_four_columns_bytes_with_result_format<F>(
        &mut self,
        stmt: &super::PreparedStatement,
        params: &[Option<Vec<u8>>],
        result_format: i16,
        mut on_row: F,
    ) -> PgResult<usize>
    where
        F: FnMut([Option<&[u8]>; 4]) -> PgResult<()>,
    {
        reserve_prepared_single_write_buf(self, stmt, params, result_format)?;

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
        let mut columns = [None, None, None, None];
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            match self
                .recv_fill_first_four_columns_zerocopy_fast(&mut columns)
                .await
            {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "prepared single reuse visit first-four execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                if let Err(err) = on_row([
                                    columns[0].as_deref(),
                                    columns[1].as_deref(),
                                    columns[2].as_deref(),
                                    columns[3].as_deref(),
                                ]) {
                                    return return_callback_error_with_desync(self, err);
                                }
                                columns.fill(None);
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
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "prepared single reuse visit first-four execute",
                                    other,
                                ),
                            );
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

    /// Sequential prepared query from pre-encoded Bind/Execute/Sync wire bytes.
    ///
    /// `wire` must contain exactly one prepared-statement execution.
    #[inline]
    pub async fn query_prepared_single_encoded_visit_bytes_rows<F>(
        &mut self,
        wire: &[u8],
        on_row: F,
    ) -> PgResult<usize>
    where
        F: FnMut(&super::PgBytesRow) -> PgResult<()>,
    {
        let (row_count, _, _) = self
            .query_prepared_single_encoded_visit_bytes_rows_profiled(wire, on_row)
            .await?;
        Ok(row_count)
    }

    /// Sequential prepared query from pre-encoded Bind/Execute/Sync wire bytes.
    ///
    /// Returns `(rows, send_elapsed, consume_elapsed)`.
    #[inline]
    pub async fn query_prepared_single_encoded_visit_bytes_rows_profiled<F>(
        &mut self,
        wire: &[u8],
        mut on_row: F,
    ) -> PgResult<(usize, Duration, Duration)>
    where
        F: FnMut(&super::PgBytesRow) -> PgResult<()>,
    {
        let send_start = Instant::now();
        self.send_bytes(wire).await?;
        let send_elapsed = send_start.elapsed();
        let consume_start = Instant::now();

        let mut row_count = 0usize;
        let mut row = super::PgBytesRow::default();
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            match self.recv_fill_zerocopy_row_fast(&mut row).await {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "prepared single encoded visit bytes execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                if let Err(err) = on_row(&row) {
                                    return return_callback_error_with_desync(self, err);
                                }
                                row_count += 1;
                                row.release_payload();
                            }
                        }
                        b'C' => {}
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok((row_count, send_elapsed, consume_start.elapsed()));
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "prepared single encoded visit bytes execute",
                                    other,
                                ),
                            );
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

    /// Sequential prepared query from pre-encoded Bind/Execute/Sync wire bytes.
    #[inline]
    pub async fn query_prepared_single_encoded_visit_first_column_bytes<F>(
        &mut self,
        wire: &[u8],
        on_value: F,
    ) -> PgResult<usize>
    where
        F: FnMut(Option<&[u8]>) -> PgResult<()>,
    {
        let (row_count, _, _) = self
            .query_prepared_single_encoded_visit_first_column_bytes_profiled(wire, on_value)
            .await?;
        Ok(row_count)
    }

    /// Sequential prepared query from pre-encoded Bind/Execute/Sync wire bytes.
    ///
    /// Returns `(rows, send_elapsed, consume_elapsed)`.
    #[inline]
    pub async fn query_prepared_single_encoded_visit_first_column_bytes_profiled<F>(
        &mut self,
        wire: &[u8],
        mut on_value: F,
    ) -> PgResult<(usize, Duration, Duration)>
    where
        F: FnMut(Option<&[u8]>) -> PgResult<()>,
    {
        let send_start = Instant::now();
        self.send_bytes(wire).await?;
        let send_elapsed = send_start.elapsed();
        let consume_start = Instant::now();

        let mut row_count = 0usize;
        let mut first_column: Option<Bytes> = None;
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            match self
                .recv_fill_first_column_zerocopy_fast(&mut first_column)
                .await
            {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "prepared single encoded visit first-column execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                if let Err(err) = on_value(first_column.as_deref()) {
                                    return return_callback_error_with_desync(self, err);
                                }
                                row_count += 1;
                                first_column = None;
                            }
                        }
                        b'C' => {}
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok((row_count, send_elapsed, consume_start.elapsed()));
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "prepared single encoded visit first-column execute",
                                    other,
                                ),
                            );
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

    /// Sequential prepared query from pre-encoded Bind/Execute/Sync wire bytes.
    #[inline]
    pub async fn query_prepared_single_encoded_visit_first_four_columns_bytes<F>(
        &mut self,
        wire: &[u8],
        on_row: F,
    ) -> PgResult<usize>
    where
        F: FnMut([Option<&[u8]>; 4]) -> PgResult<()>,
    {
        let (row_count, _, _) = self
            .query_prepared_single_encoded_visit_first_four_columns_bytes_profiled(wire, on_row)
            .await?;
        Ok(row_count)
    }

    /// Sequential prepared query from pre-encoded Bind/Execute/Sync wire bytes.
    ///
    /// Returns `(rows, send_elapsed, consume_elapsed)`.
    #[inline]
    pub async fn query_prepared_single_encoded_visit_first_four_columns_bytes_profiled<F>(
        &mut self,
        wire: &[u8],
        mut on_row: F,
    ) -> PgResult<(usize, Duration, Duration)>
    where
        F: FnMut([Option<&[u8]>; 4]) -> PgResult<()>,
    {
        let send_start = Instant::now();
        self.send_bytes(wire).await?;
        let send_elapsed = send_start.elapsed();
        let consume_start = Instant::now();

        let mut row_count = 0usize;
        let mut columns = [None, None, None, None];
        let mut error: Option<PgError> = None;
        let mut flow = ExtendedFlowTracker::new(ExtendedFlowConfig::parse_bind_execute(false));

        loop {
            match self
                .recv_fill_first_four_columns_zerocopy_fast(&mut columns)
                .await
            {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "prepared single encoded visit first-four execute",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                if let Err(err) = on_row([
                                    columns[0].as_deref(),
                                    columns[1].as_deref(),
                                    columns[2].as_deref(),
                                    columns[3].as_deref(),
                                ]) {
                                    return return_callback_error_with_desync(self, err);
                                }
                                columns.fill(None);
                                row_count += 1;
                            }
                        }
                        b'C' => {}
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok((row_count, send_elapsed, consume_start.elapsed()));
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "prepared single encoded visit first-four execute",
                                    other,
                                ),
                            );
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn prepared_buffer_sizing_rejects_too_many_params_before_allocation() {
        let params = vec![None; i16::MAX as usize + 1];
        let err = prepared_bind_execute_sync_wire_len("stmt", &params, PgEncoder::FORMAT_TEXT)
            .expect_err("parameter overflow must be rejected");

        assert!(matches!(err, PgError::Encode(msg) if msg.contains("Too many parameters")));
    }

    #[test]
    fn sql_to_stmt_name_matches_prepared_statement_identity() {
        let sql = "SELECT id, name FROM users WHERE id = $1";
        let stmt = super::super::PreparedStatement::from_sql(sql);
        assert_eq!(PgConnection::sql_to_stmt_name(sql), stmt.name());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn streaming_callback_error_marks_query_connection_desynced() {
        let mut conn = test_conn();

        let err = return_callback_error_with_desync::<()>(
            &mut conn,
            PgError::Query("consumer stopped".to_string()),
        )
        .expect_err("callback error should be returned");

        assert!(matches!(err, PgError::Query(msg) if msg == "consumer stopped"));
        assert!(conn.is_io_desynced());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn protocol_order_error_marks_query_connection_desynced() {
        let (mut conn, _peer) = test_conn_with_peer();
        push_backend_frame(&mut conn, b'D', &0i16.to_be_bytes());
        let stmt = super::super::PreparedStatement::from_sql("SELECT 1");

        let err = conn
            .query_prepared_single_count(&stmt, &[])
            .await
            .expect_err("out-of-order DataRow must fail");

        assert!(err.to_string().contains("DataRow before BindComplete"));
        assert!(conn.is_io_desynced());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn simple_flow_error_marks_query_connection_desynced() {
        let (mut conn, _peer) = test_conn_with_peer();
        push_backend_frame(&mut conn, b'Z', b"I");

        let err = conn
            .execute_simple("SELECT 1")
            .await
            .expect_err("ReadyForQuery before completion must fail");

        assert!(err.to_string().contains("ReadyForQuery before completion"));
        assert!(conn.is_io_desynced());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn query_cached_keeps_statement_after_post_parse_error() {
        let (mut conn, _peer) = test_conn_with_peer();
        let sql = "SELECT $1";
        let stmt_name = PgConnection::sql_to_stmt_name(sql);
        let err_payload = error_response_payload("23514", "check constraint violation");

        push_backend_frame(&mut conn, b'1', &[]);
        push_backend_frame(&mut conn, b'E', &err_payload);
        push_backend_frame(&mut conn, b'Z', b"I");

        let err = conn
            .query_cached(sql, &[Some(b"bad".to_vec())])
            .await
            .expect_err("execution-stage error should be returned");

        assert!(matches!(err, PgError::QueryServer(_)));
        assert!(conn.prepared_statements.contains_key(&stmt_name));
        assert!(!conn.is_io_desynced());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn query_cached_removes_new_statement_when_parse_fails() {
        let (mut conn, _peer) = test_conn_with_peer();
        let sql = "SELECT broken";
        let stmt_name = PgConnection::sql_to_stmt_name(sql);
        let err_payload = error_response_payload("42601", "syntax error");

        push_backend_frame(&mut conn, b'E', &err_payload);
        push_backend_frame(&mut conn, b'Z', b"I");

        let err = conn
            .query_cached(sql, &[])
            .await
            .expect_err("parse-stage error should be returned");

        assert!(matches!(err, PgError::QueryServer(_)));
        assert!(!conn.prepared_statements.contains_key(&stmt_name));
        assert!(!conn.is_io_desynced());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn query_prepared_single_clears_state_on_missing_statement() {
        let (mut conn, _peer) = test_conn_with_peer();
        let stmt = super::super::PreparedStatement::from_sql("SELECT 1");
        let stmt_name = stmt.name().to_string();
        conn.prepared_statements
            .insert(stmt_name.clone(), "SELECT 1".to_string());
        let err_payload = error_response_payload(
            "26000",
            &format!("prepared statement \"{}\" does not exist", stmt_name),
        );

        push_backend_frame(&mut conn, b'E', &err_payload);
        push_backend_frame(&mut conn, b'Z', b"I");

        let err = conn
            .query_prepared_single(&stmt, &[])
            .await
            .expect_err("missing server statement should be returned");

        assert!(matches!(err, PgError::QueryServer(_)));
        assert!(conn.prepared_statements.is_empty());
        assert!(!conn.is_io_desynced());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn query_prepared_single_reuse_clears_state_on_missing_statement() {
        let (mut conn, _peer) = test_conn_with_peer();
        let stmt = super::super::PreparedStatement::from_sql("SELECT 1");
        let stmt_name = stmt.name().to_string();
        conn.prepared_statements
            .insert(stmt_name.clone(), "SELECT 1".to_string());
        let err_payload = error_response_payload(
            "26000",
            &format!("prepared statement \"{}\" does not exist", stmt_name),
        );

        push_backend_frame(&mut conn, b'E', &err_payload);
        push_backend_frame(&mut conn, b'Z', b"I");

        let err = conn
            .query_prepared_single_reuse_with_result_format(&stmt, &[], PgEncoder::FORMAT_TEXT)
            .await
            .expect_err("missing server statement should be returned");

        assert!(matches!(err, PgError::QueryServer(_)));
        assert!(conn.prepared_statements.is_empty());
        assert!(!conn.is_io_desynced());
    }
}
