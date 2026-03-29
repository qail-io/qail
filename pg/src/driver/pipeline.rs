//! High-performance pipelining methods for PostgreSQL connection.
//!
//!
//! Performance hierarchy (fastest to slowest):
//! 1. `pipeline_execute_count_ast_cached` - Parse once, Bind+Execute many (275k q/s)
//! 2. `pipeline_execute_count_simple_wire` - Pre-encoded simple query
//! 3. `pipeline_execute_count_wire` - Pre-encoded extended query
//! 4. `pipeline_execute_count_simple_ast` - Simple query protocol (~99k q/s)
//! 5. `pipeline_execute_count_ast_oneshot` - Fast extended query, count only
//! 6. `pipeline_execute_rows_ast` - Full results collection
//! 7. `query_pipeline` - SQL-based pipelining

use super::{
    PgConnection, PgError, PgResult, is_ignorable_session_message, is_ignorable_session_msg_type,
    unexpected_backend_message, unexpected_backend_msg_type,
};
use crate::protocol::{AstEncoder, BackendMessage, PgEncoder};
use bytes::{Bytes, BytesMut};

/// Strategy for AST pipeline execution.
///
/// `Auto` favors the cached prepared-statement path for large batches and
/// one-shot execution for tiny batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AstPipelineMode {
    /// Heuristic strategy:
    /// - small batch => `OneShot`
    /// - larger batch => `Cached`
    #[default]
    Auto,
    /// Parse+Bind+Execute for each command in the batch.
    OneShot,
    /// Cache prepared SQL templates and execute Bind+Execute in hot path.
    Cached,
}

impl AstPipelineMode {
    const AUTO_CACHE_MIN_BATCH: usize = 8;

    #[inline]
    fn resolve_for_batch_len(self, batch_len: usize) -> Self {
        match self {
            Self::Auto => {
                if batch_len >= Self::AUTO_CACHE_MIN_BATCH {
                    Self::Cached
                } else {
                    Self::OneShot
                }
            }
            mode => mode,
        }
    }
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
fn rollback_new_cached_statements(conn: &mut PgConnection, new_stmt_hashes: &[u64]) {
    for sql_hash in new_stmt_hashes {
        if let Some(stmt_name) = conn.stmt_cache.remove(sql_hash) {
            conn.prepared_statements.remove(&stmt_name);
        }
    }
}

#[inline]
fn reserve_prepared_pipeline_write_buf(
    conn: &mut PgConnection,
    stmt: &super::PreparedStatement,
    params_batch: &[Vec<Option<Vec<u8>>>],
    result_format: i16,
) -> PgResult<()> {
    conn.write_buf.clear();
    let mut needed = 5usize;
    for params in params_batch {
        let bind_execute = PgEncoder::bind_execute_wire_len_with_formats(
            &stmt.name,
            params,
            PgEncoder::FORMAT_TEXT,
            result_format,
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;
        needed = needed
            .checked_add(bind_execute)
            .ok_or_else(|| PgError::Encode("prepared pipeline batch too large".to_string()))?;
    }
    conn.write_buf.reserve(needed);
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct FastExtendedFlowConfig {
    expected_queries: usize,
    allow_parse_complete: bool,
    require_parse_before_bind: bool,
    no_data_counts_as_completion: bool,
    allow_no_data_nonterminal: bool,
    expected_parse_completes: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct FastExtendedFlowTracker {
    cfg: FastExtendedFlowConfig,
    completed_queries: usize,
    parse_completes: usize,
    current_parse_seen: bool,
    current_bind_seen: bool,
}

impl FastExtendedFlowTracker {
    fn new(cfg: FastExtendedFlowConfig) -> Self {
        Self {
            cfg,
            completed_queries: 0,
            parse_completes: 0,
            current_parse_seen: false,
            current_bind_seen: false,
        }
    }

    fn completed_queries(&self) -> usize {
        self.completed_queries
    }

    fn validate_msg_type(
        &mut self,
        msg_type: u8,
        context: &'static str,
        error_pending: bool,
    ) -> PgResult<FastPipelineEvent> {
        if is_ignorable_session_msg_type(msg_type) {
            return Ok(FastPipelineEvent::Continue);
        }

        if error_pending {
            if msg_type == b'Z' {
                return Ok(FastPipelineEvent::ReadyForQuery);
            }
            return Ok(FastPipelineEvent::Continue);
        }

        if msg_type == b'Z' {
            if self.completed_queries != self.cfg.expected_queries {
                return Err(PgError::Protocol(format!(
                    "{}: Pipeline completion mismatch: expected {}, got {}",
                    context, self.cfg.expected_queries, self.completed_queries
                )));
            }
            if self.current_parse_seen || self.current_bind_seen {
                return Err(PgError::Protocol(format!(
                    "{}: ReadyForQuery with incomplete query state",
                    context
                )));
            }
            if let Some(expected) = self.cfg.expected_parse_completes
                && self.parse_completes != expected
            {
                return Err(PgError::Protocol(format!(
                    "{}: ParseComplete mismatch: expected {}, got {}",
                    context, expected, self.parse_completes
                )));
            }
            return Ok(FastPipelineEvent::ReadyForQuery);
        }

        if self.completed_queries >= self.cfg.expected_queries {
            return Err(PgError::Protocol(format!(
                "{}: unexpected message '{}' after all queries completed",
                context, msg_type as char
            )));
        }

        match msg_type {
            b'1' => {
                if !self.cfg.allow_parse_complete {
                    return Err(PgError::Protocol(format!(
                        "{}: unexpected ParseComplete",
                        context
                    )));
                }
                if self.current_bind_seen {
                    return Err(PgError::Protocol(format!(
                        "{}: ParseComplete after BindComplete",
                        context
                    )));
                }
                if self.current_parse_seen {
                    return Err(PgError::Protocol(format!(
                        "{}: duplicate ParseComplete",
                        context
                    )));
                }
                self.current_parse_seen = true;
                self.parse_completes += 1;
                if let Some(expected) = self.cfg.expected_parse_completes
                    && self.parse_completes > expected
                {
                    return Err(PgError::Protocol(format!(
                        "{}: ParseComplete mismatch: expected {}, got at least {}",
                        context, expected, self.parse_completes
                    )));
                }
            }
            b'2' => {
                if self.current_bind_seen {
                    return Err(PgError::Protocol(format!(
                        "{}: duplicate BindComplete",
                        context
                    )));
                }
                if self.cfg.require_parse_before_bind && !self.current_parse_seen {
                    return Err(PgError::Protocol(format!(
                        "{}: BindComplete before ParseComplete",
                        context
                    )));
                }
                self.current_bind_seen = true;
            }
            b'T' | b't' | b's' => {
                if !self.current_bind_seen {
                    return Err(PgError::Protocol(format!(
                        "{}: '{}' before BindComplete",
                        context, msg_type as char
                    )));
                }
            }
            b'D' => {
                if !self.current_bind_seen {
                    return Err(PgError::Protocol(format!(
                        "{}: DataRow before BindComplete",
                        context
                    )));
                }
            }
            b'n' => {
                if !self.current_bind_seen {
                    return Err(PgError::Protocol(format!(
                        "{}: NoData before BindComplete",
                        context
                    )));
                }
                if self.cfg.no_data_counts_as_completion {
                    self.complete_current();
                } else if !self.cfg.allow_no_data_nonterminal {
                    return Err(PgError::Protocol(format!("{}: unexpected NoData", context)));
                }
            }
            b'C' => {
                if !self.current_bind_seen {
                    return Err(PgError::Protocol(format!(
                        "{}: CommandComplete before BindComplete",
                        context
                    )));
                }
                self.complete_current();
            }
            b'I' => {
                return Err(PgError::Protocol(format!(
                    "{}: unexpected EmptyQueryResponse in extended pipeline",
                    context
                )));
            }
            other => return Err(unexpected_backend_msg_type(context, other)),
        }

        Ok(FastPipelineEvent::Continue)
    }

    fn complete_current(&mut self) {
        self.completed_queries += 1;
        self.current_parse_seen = false;
        self.current_bind_seen = false;
    }
}

#[derive(Debug, Clone, Copy)]
struct FastSimpleFlowTracker {
    expected_queries: usize,
    completed_queries: usize,
    current_row_description_seen: bool,
}

impl FastSimpleFlowTracker {
    fn new(expected_queries: usize) -> Self {
        Self {
            expected_queries,
            completed_queries: 0,
            current_row_description_seen: false,
        }
    }

    fn completed_queries(&self) -> usize {
        self.completed_queries
    }

    fn validate_msg_type(
        &mut self,
        msg_type: u8,
        context: &'static str,
        error_pending: bool,
    ) -> PgResult<FastPipelineEvent> {
        if is_ignorable_session_msg_type(msg_type) {
            return Ok(FastPipelineEvent::Continue);
        }

        if error_pending {
            if msg_type == b'Z' {
                return Ok(FastPipelineEvent::ReadyForQuery);
            }
            return Ok(FastPipelineEvent::Continue);
        }

        if msg_type == b'Z' {
            if self.completed_queries != self.expected_queries {
                return Err(PgError::Protocol(format!(
                    "{}: Pipeline completion mismatch: expected {}, got {}",
                    context, self.expected_queries, self.completed_queries
                )));
            }
            if self.current_row_description_seen {
                return Err(PgError::Protocol(format!(
                    "{}: ReadyForQuery with incomplete row stream",
                    context
                )));
            }
            return Ok(FastPipelineEvent::ReadyForQuery);
        }

        if self.completed_queries >= self.expected_queries {
            return Err(PgError::Protocol(format!(
                "{}: unexpected message '{}' after all queries completed",
                context, msg_type as char
            )));
        }

        match msg_type {
            b'T' => {
                if self.current_row_description_seen {
                    return Err(PgError::Protocol(format!(
                        "{}: duplicate RowDescription",
                        context
                    )));
                }
                self.current_row_description_seen = true;
            }
            b'D' => {
                if !self.current_row_description_seen {
                    return Err(PgError::Protocol(format!(
                        "{}: DataRow before RowDescription",
                        context
                    )));
                }
            }
            b'C' | b'I' => {
                self.completed_queries += 1;
                self.current_row_description_seen = false;
            }
            b'1' | b'2' | b'n' | b't' | b's' => {
                return Err(PgError::Protocol(format!(
                    "{}: unexpected '{}' in simple pipeline",
                    context, msg_type as char
                )));
            }
            other => return Err(unexpected_backend_msg_type(context, other)),
        }

        Ok(FastPipelineEvent::Continue)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FastPipelineEvent {
    Continue,
    ReadyForQuery,
}

#[inline]
fn backend_msg_type_for_flow(msg: &BackendMessage) -> Option<u8> {
    match msg {
        BackendMessage::ParseComplete => Some(b'1'),
        BackendMessage::BindComplete => Some(b'2'),
        BackendMessage::ParameterDescription(_) => Some(b't'),
        BackendMessage::RowDescription(_) => Some(b'T'),
        BackendMessage::NoData => Some(b'n'),
        BackendMessage::PortalSuspended => Some(b's'),
        BackendMessage::DataRow(_) => Some(b'D'),
        BackendMessage::CommandComplete(_) => Some(b'C'),
        BackendMessage::EmptyQueryResponse => Some(b'I'),
        BackendMessage::ReadyForQuery(_) => Some(b'Z'),
        _ => None,
    }
}

impl PgConnection {
    /// Execute multiple SQL queries in a single network round-trip (PIPELINING).
    pub async fn query_pipeline(
        &mut self,
        queries: &[(&str, &[Option<Vec<u8>>])],
    ) -> PgResult<Vec<Vec<Vec<Option<Vec<u8>>>>>> {
        // Encode all queries into a single buffer
        let mut buf = BytesMut::new();
        for (sql, params) in queries {
            buf.extend_from_slice(
                &PgEncoder::try_encode_parse("", sql, &[])
                    .map_err(|e| PgError::Encode(e.to_string()))?,
            );
            buf.extend_from_slice(
                &PgEncoder::encode_bind("", "", params)
                    .map_err(|e| PgError::Encode(e.to_string()))?,
            );
            buf.extend_from_slice(
                &PgEncoder::try_encode_execute("", 0)
                    .map_err(|e| PgError::Encode(e.to_string()))?,
            );
        }
        buf.extend_from_slice(&PgEncoder::encode_sync());

        // Send all queries in ONE write
        self.write_all_with_timeout(&buf, "stream write").await?;

        // Collect all results
        let mut all_results: Vec<Vec<Vec<Option<Vec<u8>>>>> = Vec::with_capacity(queries.len());
        let mut current_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: queries.len(),
            allow_parse_complete: true,
            require_parse_before_bind: true,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(queries.len()),
        });

        loop {
            let msg = self.recv().await?;
            if is_ignorable_session_message(&msg) {
                continue;
            }
            if let BackendMessage::ErrorResponse(err) = msg {
                if error.is_none() {
                    error = Some(PgError::QueryServer(err.into()));
                }
                continue;
            }
            let msg_type = backend_msg_type_for_flow(&msg)
                .ok_or_else(|| unexpected_backend_message("pipeline query", &msg));
            let msg_type = match msg_type {
                Ok(msg_type) => msg_type,
                Err(err) => return return_with_desync(self, err),
            };
            if let Err(err) = flow.validate_msg_type(msg_type, "pipeline query", error.is_some()) {
                return return_with_desync(self, err);
            }
            match msg {
                BackendMessage::ParseComplete | BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        current_rows.push(data);
                    }
                }
                BackendMessage::CommandComplete(_) => {
                    all_results.push(std::mem::take(&mut current_rows));
                }
                BackendMessage::NoData => {
                    all_results.push(Vec::new());
                }
                BackendMessage::ReadyForQuery(_) => {
                    if all_results.len() != queries.len() {
                        return Err(error.unwrap_or_else(|| {
                            PgError::Protocol(format!(
                                "Pipeline completion mismatch: expected {}, got {}",
                                queries.len(),
                                all_results.len()
                            ))
                        }));
                    }
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(all_results);
                }
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("pipeline query", &other),
                    );
                }
            }
        }
    }

    /// Execute multiple uncached SQL queries in one round-trip and drain
    /// completion without materializing rows.
    pub async fn query_pipeline_count(
        &mut self,
        queries: &[(&str, &[Option<Vec<u8>>])],
    ) -> PgResult<usize> {
        if queries.is_empty() {
            return Ok(0);
        }

        self.write_buf.clear();
        for (sql, params) in queries {
            PgEncoder::try_encode_parse_to(&mut self.write_buf, "", sql, &[])
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_bind_to(&mut self.write_buf, "", params)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_execute_to(&mut self.write_buf);
        }
        PgEncoder::encode_sync_to(&mut self.write_buf);

        self.flush_write_buf().await?;

        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: queries.len(),
            allow_parse_complete: true,
            require_parse_before_bind: true,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(queries.len()),
        });

        loop {
            match self.recv_msg_type_fast().await {
                Ok(msg_type) => {
                    let event =
                        match flow.validate_msg_type(msg_type, "query_pipeline_count", error.is_some())
                        {
                            Ok(event) => event,
                            Err(err) => return return_with_desync(self, err),
                        };
                    match event {
                        FastPipelineEvent::Continue => {}
                        FastPipelineEvent::ReadyForQuery => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
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

    /// Execute multiple uncached SQL queries in one round-trip and stream
    /// result rows through a zero-copy visitor.
    pub async fn query_pipeline_visit_bytes_rows<F>(
        &mut self,
        queries: &[(&str, &[Option<Vec<u8>>])],
        mut on_row: F,
    ) -> PgResult<usize>
    where
        F: FnMut(&super::PgBytesRow) -> PgResult<()>,
    {
        if queries.is_empty() {
            return Ok(0);
        }

        self.write_buf.clear();
        for (sql, params) in queries {
            PgEncoder::try_encode_parse_to(&mut self.write_buf, "", sql, &[])
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_bind_to(&mut self.write_buf, "", params)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_execute_to(&mut self.write_buf);
        }
        PgEncoder::encode_sync_to(&mut self.write_buf);

        self.flush_write_buf().await?;

        let mut row = super::PgBytesRow::default();
        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: queries.len(),
            allow_parse_complete: true,
            require_parse_before_bind: true,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(queries.len()),
        });

        loop {
            match self.recv_fill_zerocopy_row_fast(&mut row).await {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "query_pipeline_visit_bytes_rows",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'1' | b'2' | b'T' | b'C' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                on_row(&row)?;
                                row.release_payload();
                            }
                        }
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "query_pipeline_visit_bytes_rows",
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

    /// Execute multiple uncached SQL queries in one round-trip and stream only
    /// the first column of each row.
    pub async fn query_pipeline_visit_first_column_bytes<F>(
        &mut self,
        queries: &[(&str, &[Option<Vec<u8>>])],
        mut on_value: F,
    ) -> PgResult<usize>
    where
        F: FnMut(Option<&[u8]>) -> PgResult<()>,
    {
        if queries.is_empty() {
            return Ok(0);
        }

        self.write_buf.clear();
        for (sql, params) in queries {
            PgEncoder::try_encode_parse_to(&mut self.write_buf, "", sql, &[])
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_bind_to(&mut self.write_buf, "", params)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_execute_to(&mut self.write_buf);
        }
        PgEncoder::encode_sync_to(&mut self.write_buf);

        self.flush_write_buf().await?;

        let mut first_column: Option<Bytes> = None;
        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: queries.len(),
            allow_parse_complete: true,
            require_parse_before_bind: true,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(queries.len()),
        });

        loop {
            match self
                .recv_fill_first_column_zerocopy_fast(&mut first_column)
                .await
            {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "query_pipeline_visit_first_column_bytes",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'1' | b'2' | b'T' | b'C' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                on_value(first_column.as_deref())?;
                                first_column = None;
                            }
                        }
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "query_pipeline_visit_first_column_bytes",
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

    /// Execute multiple Qail ASTs in a single network round-trip.
    pub async fn pipeline_execute_rows_ast(
        &mut self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<Vec<Vec<Vec<Option<Vec<u8>>>>>> {
        let buf = AstEncoder::encode_batch(cmds).map_err(|e| PgError::Encode(e.to_string()))?;
        self.write_all_with_timeout(&buf, "stream write").await?;

        let mut all_results: Vec<Vec<Vec<Option<Vec<u8>>>>> = Vec::with_capacity(cmds.len());
        let mut current_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: cmds.len(),
            allow_parse_complete: true,
            require_parse_before_bind: true,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(cmds.len()),
        });

        loop {
            let msg = self.recv().await?;
            if is_ignorable_session_message(&msg) {
                continue;
            }
            if let BackendMessage::ErrorResponse(err) = msg {
                if error.is_none() {
                    error = Some(PgError::QueryServer(err.into()));
                }
                continue;
            }
            let msg_type = backend_msg_type_for_flow(&msg)
                .ok_or_else(|| unexpected_backend_message("pipeline ast", &msg));
            let msg_type = match msg_type {
                Ok(msg_type) => msg_type,
                Err(err) => return return_with_desync(self, err),
            };
            if let Err(err) = flow.validate_msg_type(msg_type, "pipeline ast", error.is_some()) {
                return return_with_desync(self, err);
            }
            match msg {
                BackendMessage::ParseComplete | BackendMessage::BindComplete => {}
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        current_rows.push(data);
                    }
                }
                BackendMessage::CommandComplete(_) => {
                    all_results.push(std::mem::take(&mut current_rows));
                }
                BackendMessage::NoData => {
                    all_results.push(Vec::new());
                }
                BackendMessage::ReadyForQuery(_) => {
                    if all_results.len() != cmds.len() {
                        return Err(error.unwrap_or_else(|| {
                            PgError::Protocol(format!(
                                "Pipeline completion mismatch: expected {}, got {}",
                                cmds.len(),
                                all_results.len()
                            ))
                        }));
                    }
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(all_results);
                }
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("pipeline ast", &other),
                    );
                }
            }
        }
    }

    /// FAST AST pipeline - returns only query count, no result parsing.
    pub async fn pipeline_execute_count_ast_oneshot(
        &mut self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<usize> {
        let buf = AstEncoder::encode_batch(cmds).map_err(|e| PgError::Encode(e.to_string()))?;

        self.write_all_with_timeout(&buf, "stream write").await?;
        self.flush_with_timeout("stream flush").await?;

        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: cmds.len(),
            allow_parse_complete: true,
            require_parse_before_bind: true,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(cmds.len()),
        });

        loop {
            match self.recv_msg_type_fast().await {
                Ok(msg_type) => {
                    let event = match flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_count_ast_oneshot",
                        error.is_some(),
                    ) {
                        Ok(event) => event,
                        Err(err) => return return_with_desync(self, err),
                    };
                    match event {
                        FastPipelineEvent::Continue => {}
                        FastPipelineEvent::ReadyForQuery => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
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

    /// Execute AST pipeline with explicit strategy mode.
    ///
    /// `Auto` uses a lightweight batch-size heuristic:
    /// - `< 8` queries: one-shot path (`pipeline_execute_count_ast_oneshot`)
    /// - `>= 8` queries: cached path (`pipeline_execute_count_ast_cached`)
    #[inline]
    pub async fn pipeline_execute_count_ast_with_mode(
        &mut self,
        cmds: &[qail_core::ast::Qail],
        mode: AstPipelineMode,
    ) -> PgResult<usize> {
        if cmds.is_empty() {
            return Ok(0);
        }

        match mode.resolve_for_batch_len(cmds.len()) {
            AstPipelineMode::OneShot => self.pipeline_execute_count_ast_oneshot(cmds).await,
            AstPipelineMode::Cached => self.pipeline_execute_count_ast_cached(cmds).await,
            AstPipelineMode::Auto => unreachable!("Auto mode must resolve to concrete strategy"),
        }
    }

    /// FASTEST extended query pipeline - takes pre-encoded wire bytes.
    #[inline]
    pub async fn pipeline_execute_count_wire(
        &mut self,
        wire_bytes: &[u8],
        expected_queries: usize,
    ) -> PgResult<usize> {
        self.write_all_with_timeout(wire_bytes, "stream write")
            .await?;
        self.flush_with_timeout("stream flush").await?;

        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries,
            allow_parse_complete: true,
            require_parse_before_bind: false,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: None,
        });

        loop {
            match self.recv_msg_type_fast().await {
                Ok(msg_type) => {
                    let event = match flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_count_wire",
                        error.is_some(),
                    ) {
                        Ok(event) => event,
                        Err(err) => return return_with_desync(self, err),
                    };
                    match event {
                        FastPipelineEvent::Continue => {}
                        FastPipelineEvent::ReadyForQuery => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
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

    /// Simple query protocol pipeline - uses 'Q' message.
    #[inline]
    pub async fn pipeline_execute_count_simple_ast(
        &mut self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<usize> {
        if cmds.is_empty() {
            return Ok(0);
        }

        let buf =
            AstEncoder::encode_batch_simple(cmds).map_err(|e| PgError::Encode(e.to_string()))?;
        self.write_all_with_timeout(&buf, "stream write").await?;
        self.flush_with_timeout("stream flush").await?;

        let mut error: Option<PgError> = None;
        let mut flow = FastSimpleFlowTracker::new(cmds.len());

        loop {
            match self.recv_msg_type_fast().await {
                Ok(msg_type) => {
                    let event = match flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_count_simple_ast",
                        error.is_some(),
                    ) {
                        Ok(event) => event,
                        Err(err) => return return_with_desync(self, err),
                    };
                    match event {
                        FastPipelineEvent::Continue => {}
                        FastPipelineEvent::ReadyForQuery => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
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

    /// FASTEST simple query pipeline - takes pre-encoded bytes.
    #[inline]
    pub async fn pipeline_execute_count_simple_wire(
        &mut self,
        wire_bytes: &[u8],
        expected_queries: usize,
    ) -> PgResult<usize> {
        self.write_all_with_timeout(wire_bytes, "stream write")
            .await?;
        self.flush_with_timeout("stream flush").await?;

        let mut error: Option<PgError> = None;
        let mut flow = FastSimpleFlowTracker::new(expected_queries);

        loop {
            match self.recv_msg_type_fast().await {
                Ok(msg_type) => {
                    let event = match flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_count_simple_wire",
                        error.is_some(),
                    ) {
                        Ok(event) => event,
                        Err(err) => return return_with_desync(self, err),
                    };
                    match event {
                        FastPipelineEvent::Continue => {}
                        FastPipelineEvent::ReadyForQuery => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
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

    /// CACHED PREPARED STATEMENT pipeline - Parse once, Bind+Execute many.
    /// 1. Generate SQL template with $1, $2, etc. placeholders
    /// 2. Parse template ONCE (cached in PostgreSQL)
    /// 3. Send Bind+Execute for each instance (params differ per query)
    #[inline]
    pub async fn pipeline_execute_count_ast_cached(
        &mut self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<usize> {
        if cmds.is_empty() {
            return Ok(0);
        }

        use super::prepared::{sql_bytes_hash, stmt_name_from_hash};

        let mut buf = BytesMut::with_capacity(cmds.len() * 64);
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();
        let mut new_stmt_hashes: Vec<u64> = Vec::new();

        for cmd in cmds {
            if let Err(e) = AstEncoder::encode_cmd_sql_reuse(cmd, &mut sql_buf, &mut params) {
                rollback_new_cached_statements(self, &new_stmt_hashes);
                return Err(PgError::Encode(e.to_string()));
            }

            let sql_hash = sql_bytes_hash(sql_buf.as_ref());

            if self.stmt_cache.contains(&sql_hash) {
                self.stmt_cache.touch_key(sql_hash);
            } else {
                let stmt_name = stmt_name_from_hash(sql_hash);
                if self.prepared_statements.contains_key(&stmt_name) {
                    // Recover from old cache states where prepared_statements had
                    // entries that were not mirrored in stmt_cache.
                    self.stmt_cache.put(sql_hash, stmt_name.clone());
                } else {
                    self.evict_prepared_if_full();

                    let sql = String::from_utf8_lossy(sql_buf.as_ref()).to_string();
                    let parse_msg = match PgEncoder::try_encode_parse(&stmt_name, &sql, &[]) {
                        Ok(msg) => msg,
                        Err(e) => {
                            rollback_new_cached_statements(self, &new_stmt_hashes);
                            return Err(PgError::Encode(e.to_string()));
                        }
                    };
                    buf.extend(parse_msg);
                    self.stmt_cache.put(sql_hash, stmt_name.clone());
                    self.prepared_statements.insert(stmt_name.clone(), sql);
                    new_stmt_hashes.push(sql_hash);
                }
            }

            let Some(stmt_name) = self.stmt_cache.peek(&sql_hash) else {
                rollback_new_cached_statements(self, &new_stmt_hashes);
                return Err(PgError::Protocol(
                    "stmt_cache lookup failed after statement registration".to_string(),
                ));
            };

            if let Err(e) = PgEncoder::encode_bind_to(&mut buf, stmt_name, &params) {
                rollback_new_cached_statements(self, &new_stmt_hashes);
                return Err(PgError::Encode(e.to_string()));
            }
            PgEncoder::encode_execute_to(&mut buf);
        }

        PgEncoder::encode_sync_to(&mut buf);

        if let Err(err) = self.write_all_with_timeout(&buf, "stream write").await {
            rollback_new_cached_statements(self, &new_stmt_hashes);
            return Err(err);
        }
        if let Err(err) = self.flush_with_timeout("stream flush").await {
            rollback_new_cached_statements(self, &new_stmt_hashes);
            return Err(err);
        }

        let mut error: Option<PgError> = None;
        let expected_parse_completes = new_stmt_hashes.len();
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: cmds.len(),
            allow_parse_complete: true,
            require_parse_before_bind: false,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(expected_parse_completes),
        });

        loop {
            match self.recv_msg_type_fast().await {
                Ok(msg_type) => {
                    match flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_count_ast_cached",
                        error.is_some(),
                    ) {
                        Ok(FastPipelineEvent::Continue) => {}
                        Ok(FastPipelineEvent::ReadyForQuery) => {
                            if let Some(err) = error {
                                rollback_new_cached_statements(self, &new_stmt_hashes);
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
                        }
                        Err(err) => {
                            rollback_new_cached_statements(self, &new_stmt_hashes);
                            return return_with_desync(self, err);
                        }
                    }
                }
                Err(e) => {
                    if matches!(&e, PgError::QueryServer(_)) {
                        capture_query_server_error(self, &mut error, e);
                        continue;
                    }
                    rollback_new_cached_statements(self, &new_stmt_hashes);
                    return Err(e);
                }
            }
        }
    }
    /// ZERO-LOOKUP prepared statement pipeline.
    /// - Hash computation per query
    /// - HashMap lookup per query
    /// - String allocation for stmt_name
    /// # Example
    /// ```ignore
    /// // Prepare once (outside timing loop):
    /// let stmt = PreparedStatement::from_sql("SELECT id, name FROM harbors LIMIT $1");
    /// let params_batch: Vec<Vec<Option<Vec<u8>>>> = (1..=1000)
    ///     .map(|i| vec![Some(i.to_string().into_bytes())])
    ///     .collect();
    /// // Execute many (no hash, no lookup!):
    /// conn.pipeline_execute_prepared_count(&stmt, &params_batch).await?;
    /// ```
    #[inline]
    pub async fn pipeline_execute_prepared_count(
        &mut self,
        stmt: &super::PreparedStatement,
        params_batch: &[Vec<Option<Vec<u8>>>],
    ) -> PgResult<usize> {
        if params_batch.is_empty() {
            return Ok(0);
        }

        let is_new = !self.prepared_statements.contains_key(&stmt.name);

        if is_new {
            return Err(PgError::Query(
                "Statement not prepared. Call prepare() first.".to_string(),
            ));
        }

        self.write_buf.clear();
        for params in params_batch {
            PgEncoder::encode_bind_to(&mut self.write_buf, &stmt.name, params)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_execute_to(&mut self.write_buf);
        }

        PgEncoder::encode_sync_to(&mut self.write_buf);
        self.flush_write_buf().await?;

        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: params_batch.len(),
            allow_parse_complete: false,
            require_parse_before_bind: false,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(0),
        });

        loop {
            match self.recv_msg_type_fast().await {
                Ok(msg_type) => {
                    let event = match flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_prepared_count",
                        error.is_some(),
                    ) {
                        Ok(event) => event,
                        Err(err) => return return_with_desync(self, err),
                    };
                    match event {
                        FastPipelineEvent::Continue => {}
                        FastPipelineEvent::ReadyForQuery => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
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

    /// Prepare a statement and return a handle for fast execution.
    /// PreparedStatement handle for use with pipeline_execute_prepared_count.
    pub async fn prepare(&mut self, sql: &str) -> PgResult<super::PreparedStatement> {
        use super::prepared::sql_bytes_to_stmt_name;

        let stmt_name = sql_bytes_to_stmt_name(sql.as_bytes());

        if !self.prepared_statements.contains_key(&stmt_name) {
            self.evict_prepared_if_full();
            let mut buf = BytesMut::with_capacity(sql.len() + 32);
            buf.extend(PgEncoder::try_encode_parse(&stmt_name, sql, &[])?);
            buf.extend(PgEncoder::encode_sync());

            self.write_all_with_timeout(&buf, "stream write").await?;
            self.flush_with_timeout("stream flush").await?;

            // Wait for ParseComplete
            let mut error: Option<PgError> = None;
            let mut saw_parse_complete = false;
            loop {
                match self.recv_msg_type_fast().await {
                    Ok(msg_type) => match msg_type {
                        b'1' => {
                            if saw_parse_complete {
                                return Err(PgError::Protocol(
                                    "prepare received duplicate ParseComplete".to_string(),
                                ));
                            }
                            saw_parse_complete = true;
                            self.prepared_statements
                                .insert(stmt_name.clone(), sql.to_string());
                        }
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            if !saw_parse_complete {
                                return Err(PgError::Protocol(
                                    "prepare reached ReadyForQuery without ParseComplete"
                                        .to_string(),
                                ));
                            }
                            break;
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type("prepare", other),
                            );
                        }
                    },
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

        Ok(super::PreparedStatement {
            name: stmt_name,
            param_count: sql.matches('$').count(),
        })
    }

    /// Execute a prepared statement pipeline and return all row data.
    pub async fn pipeline_execute_prepared_rows(
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

        self.write_buf.clear();
        for params in params_batch {
            PgEncoder::encode_bind_to(&mut self.write_buf, &stmt.name, params)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_execute_to(&mut self.write_buf);
        }

        PgEncoder::encode_sync_to(&mut self.write_buf);
        self.flush_write_buf().await?;

        // Collect results using fast inline DataRow parsing
        let mut all_results: Vec<Vec<Vec<Option<Vec<u8>>>>> =
            Vec::with_capacity(params_batch.len());
        let mut current_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: params_batch.len(),
            allow_parse_complete: false,
            require_parse_before_bind: false,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(0),
        });

        loop {
            match self.recv_with_data_fast().await {
                Ok((msg_type, data)) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_prepared_rows",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' => {} // BindComplete
                        b'T' => {} // RowDescription
                        b'D' => {
                            // DataRow
                            if error.is_none()
                                && let Some(row) = data
                            {
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
                            if all_results.len() != params_batch.len() {
                                return Err(error.unwrap_or_else(|| {
                                    PgError::Protocol(format!(
                                        "Pipeline completion mismatch: expected {}, got {}",
                                        params_batch.len(),
                                        all_results.len()
                                    ))
                                }));
                            }
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(all_results);
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "pipeline_execute_prepared_rows",
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

    /// ZERO-COPY pipeline execution with Bytes for column data.
    pub async fn pipeline_execute_prepared_rows_bytes(
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

        reserve_prepared_pipeline_write_buf(self, stmt, params_batch, PgEncoder::FORMAT_TEXT)?;

        for params in params_batch {
            PgEncoder::encode_bind_to(&mut self.write_buf, &stmt.name, params)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_execute_to(&mut self.write_buf);
        }

        PgEncoder::encode_sync_to(&mut self.write_buf);
        self.flush_write_buf().await?;

        // Collect results using ZERO-COPY Bytes
        let mut all_results: Vec<Vec<Vec<Option<bytes::Bytes>>>> =
            Vec::with_capacity(params_batch.len());
        let mut current_rows: Vec<Vec<Option<bytes::Bytes>>> = Vec::new();
        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: params_batch.len(),
            allow_parse_complete: false,
            require_parse_before_bind: false,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(0),
        });

        loop {
            match self.recv_data_zerocopy().await {
                Ok((msg_type, data)) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_prepared_rows_bytes",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' => {} // BindComplete
                        b'T' => {} // RowDescription
                        b'D' => {
                            // DataRow
                            if error.is_none()
                                && let Some(row) = data
                            {
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
                            if all_results.len() != params_batch.len() {
                                return Err(error.unwrap_or_else(|| {
                                    PgError::Protocol(format!(
                                        "Pipeline completion mismatch: expected {}, got {}",
                                        params_batch.len(),
                                        all_results.len()
                                    ))
                                }));
                            }
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(all_results);
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "pipeline_execute_prepared_rows_bytes",
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

    /// Pipeline execution with row visitor.
    ///
    /// Rows are streamed to `on_row` as owned column buffers, avoiding
    /// materializing the full result set.
    pub async fn pipeline_execute_prepared_visit_rows<F>(
        &mut self,
        stmt: &super::PreparedStatement,
        params_batch: &[Vec<Option<Vec<u8>>>],
        mut on_row: F,
    ) -> PgResult<usize>
    where
        F: FnMut(&[Option<Vec<u8>>]) -> PgResult<()>,
    {
        if params_batch.is_empty() {
            return Ok(0);
        }

        if !self.prepared_statements.contains_key(&stmt.name) {
            return Err(PgError::Query(
                "Statement not prepared. Call prepare() first.".to_string(),
            ));
        }

        reserve_prepared_pipeline_write_buf(self, stmt, params_batch, PgEncoder::FORMAT_TEXT)?;

        for params in params_batch {
            PgEncoder::encode_bind_to(&mut self.write_buf, &stmt.name, params)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_execute_to(&mut self.write_buf);
        }

        PgEncoder::encode_sync_to(&mut self.write_buf);
        self.flush_write_buf().await?;

        let mut row_buf: Vec<Option<Vec<u8>>> = Vec::new();
        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: params_batch.len(),
            allow_parse_complete: false,
            require_parse_before_bind: false,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(0),
        });

        loop {
            match self.recv_fill_data_row_fast(&mut row_buf).await {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_prepared_visit_rows",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'C' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                on_row(row_buf.as_slice())?;
                            }
                        }
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "pipeline_execute_prepared_visit_rows",
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

    /// Pipeline execution with zero-copy row visitor.
    ///
    /// Rows are backed by one shared payload buffer plus column offsets,
    /// avoiding per-cell byte copies during receive.
    pub async fn pipeline_execute_prepared_visit_bytes_rows<F>(
        &mut self,
        stmt: &super::PreparedStatement,
        params_batch: &[Vec<Option<Vec<u8>>>],
        mut on_row: F,
    ) -> PgResult<usize>
    where
        F: FnMut(&super::PgBytesRow) -> PgResult<()>,
    {
        if params_batch.is_empty() {
            return Ok(0);
        }

        if !self.prepared_statements.contains_key(&stmt.name) {
            return Err(PgError::Query(
                "Statement not prepared. Call prepare() first.".to_string(),
            ));
        }

        reserve_prepared_pipeline_write_buf(self, stmt, params_batch, PgEncoder::FORMAT_TEXT)?;

        for params in params_batch {
            PgEncoder::encode_bind_to(&mut self.write_buf, &stmt.name, params)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_execute_to(&mut self.write_buf);
        }

        PgEncoder::encode_sync_to(&mut self.write_buf);
        self.flush_write_buf().await?;

        let mut row = super::PgBytesRow::default();
        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: params_batch.len(),
            allow_parse_complete: false,
            require_parse_before_bind: false,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(0),
        });

        loop {
            match self.recv_fill_zerocopy_row_fast(&mut row).await {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_prepared_visit_bytes_rows",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'C' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                on_row(&row)?;
                                row.release_payload();
                            }
                        }
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "pipeline_execute_prepared_visit_bytes_rows",
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

    /// Pipeline execution with first-column visitor for scalar result sets.
    pub async fn pipeline_execute_prepared_visit_first_column_bytes<F>(
        &mut self,
        stmt: &super::PreparedStatement,
        params_batch: &[Vec<Option<Vec<u8>>>],
        mut on_value: F,
    ) -> PgResult<usize>
    where
        F: FnMut(Option<&[u8]>) -> PgResult<()>,
    {
        if params_batch.is_empty() {
            return Ok(0);
        }

        if !self.prepared_statements.contains_key(&stmt.name) {
            return Err(PgError::Query(
                "Statement not prepared. Call prepare() first.".to_string(),
            ));
        }

        reserve_prepared_pipeline_write_buf(self, stmt, params_batch, PgEncoder::FORMAT_TEXT)?;
        for params in params_batch {
            PgEncoder::encode_bind_to(&mut self.write_buf, &stmt.name, params)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_execute_to(&mut self.write_buf);
        }

        PgEncoder::encode_sync_to(&mut self.write_buf);
        self.flush_write_buf().await?;

        let mut first_column: Option<Bytes> = None;
        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: params_batch.len(),
            allow_parse_complete: false,
            require_parse_before_bind: false,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(0),
        });

        loop {
            match self
                .recv_fill_first_column_zerocopy_fast(&mut first_column)
                .await
            {
                Ok(msg_type) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_prepared_visit_first_column_bytes",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' | b'C' | b'n' => {}
                        b'D' => {
                            if error.is_none() {
                                on_value(first_column.as_deref())?;
                                first_column = None;
                            }
                        }
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(flow.completed_queries());
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "pipeline_execute_prepared_visit_first_column_bytes",
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

    /// ULTRA-FAST pipeline for 2-column SELECT queries.
    pub async fn pipeline_execute_prepared_rows_2cols_bytes(
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

        reserve_prepared_pipeline_write_buf(self, stmt, params_batch, PgEncoder::FORMAT_TEXT)?;

        for params in params_batch {
            PgEncoder::encode_bind_to(&mut self.write_buf, &stmt.name, params)
                .map_err(|e| PgError::Encode(e.to_string()))?;
            PgEncoder::encode_execute_to(&mut self.write_buf);
        }

        PgEncoder::encode_sync_to(&mut self.write_buf);
        self.flush_write_buf().await?;

        // Pre-allocate with expected capacity
        let mut all_results: Vec<Vec<(bytes::Bytes, bytes::Bytes)>> =
            Vec::with_capacity(params_batch.len());
        let mut current_rows: Vec<(bytes::Bytes, bytes::Bytes)> = Vec::with_capacity(16);
        let mut error: Option<PgError> = None;
        let mut flow = FastExtendedFlowTracker::new(FastExtendedFlowConfig {
            expected_queries: params_batch.len(),
            allow_parse_complete: false,
            require_parse_before_bind: false,
            no_data_counts_as_completion: true,
            allow_no_data_nonterminal: false,
            expected_parse_completes: Some(0),
        });

        loop {
            match self.recv_data_ultra().await {
                Ok((msg_type, data)) => {
                    if let Err(err) = flow.validate_msg_type(
                        msg_type,
                        "pipeline_execute_prepared_rows_2cols_bytes",
                        error.is_some(),
                    ) {
                        return return_with_desync(self, err);
                    }
                    match msg_type {
                        b'2' | b'T' => {} // BindComplete, RowDescription
                        b'D' => {
                            if error.is_none()
                                && let Some(row) = data
                            {
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
                            if all_results.len() != params_batch.len() {
                                return Err(error.unwrap_or_else(|| {
                                    PgError::Protocol(format!(
                                        "Pipeline completion mismatch: expected {}, got {}",
                                        params_batch.len(),
                                        all_results.len()
                                    ))
                                }));
                            }
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(all_results);
                        }
                        msg_type if is_ignorable_session_msg_type(msg_type) => {}
                        other => {
                            return return_with_desync(
                                self,
                                unexpected_backend_msg_type(
                                    "pipeline_execute_prepared_rows_2cols_bytes",
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
    use qail_core::ast::Qail;

    #[test]
    fn ast_pipeline_mode_auto_resolves_by_batch_size() {
        assert_eq!(
            AstPipelineMode::Auto.resolve_for_batch_len(0),
            AstPipelineMode::OneShot
        );
        assert_eq!(
            AstPipelineMode::Auto.resolve_for_batch_len(7),
            AstPipelineMode::OneShot
        );
        assert_eq!(
            AstPipelineMode::Auto.resolve_for_batch_len(8),
            AstPipelineMode::Cached
        );
        assert_eq!(
            AstPipelineMode::Cached.resolve_for_batch_len(1),
            AstPipelineMode::Cached
        );
        assert_eq!(
            AstPipelineMode::OneShot.resolve_for_batch_len(1000),
            AstPipelineMode::OneShot
        );
    }

    #[cfg(unix)]
    fn make_test_conn_with_prepared() -> PgConnection {
        use crate::driver::connection::StatementCache;
        use crate::driver::stream::PgStream;
        use bytes::BytesMut;
        use std::collections::{HashMap, VecDeque};
        use std::num::NonZeroUsize;
        use tokio::net::UnixStream;

        let (unix_stream, _peer) = UnixStream::pair().expect("unix stream pair");
        let mut conn = PgConnection {
            stream: PgStream::Unix(unix_stream),
            buffer: BytesMut::with_capacity(1024),
            write_buf: BytesMut::with_capacity(1024),
            sql_buf: BytesMut::with_capacity(256),
            params_buf: Vec::new(),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(NonZeroUsize::new(16).expect("non-zero")),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
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
        };
        conn.prepared_statements
            .insert("s1".to_string(), "SELECT 1".to_string());
        conn.stmt_cache.put(1, "s1".to_string());
        conn
    }

    fn server_error(code: &str, message: &str) -> PgError {
        PgError::QueryServer(super::super::PgServerError {
            severity: "ERROR".to_string(),
            code: code.to_string(),
            message: message.to_string(),
            detail: None,
            hint: None,
        })
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn capture_query_server_error_clears_prepared_state_on_retryable_error() {
        let mut conn = make_test_conn_with_prepared();
        let mut slot = None;
        let err = server_error("26000", "prepared statement \"s1\" does not exist");
        capture_query_server_error(&mut conn, &mut slot, err);

        assert!(slot.is_some());
        assert!(conn.prepared_statements.is_empty());
        assert_eq!(conn.stmt_cache.len(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn capture_query_server_error_preserves_prepared_state_on_non_retryable_error() {
        let mut conn = make_test_conn_with_prepared();
        let mut slot = None;
        let err = server_error("23505", "duplicate key value violates unique constraint");
        capture_query_server_error(&mut conn, &mut slot, err);

        assert!(slot.is_some());
        assert_eq!(conn.prepared_statements.len(), 1);
        assert_eq!(conn.stmt_cache.len(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn capture_query_server_error_does_not_override_existing_error() {
        let mut conn = make_test_conn_with_prepared();
        let mut slot = Some(server_error("23505", "duplicate key"));
        let retryable = server_error("26000", "prepared statement \"s1\" does not exist");
        capture_query_server_error(&mut conn, &mut slot, retryable);

        assert_eq!(conn.prepared_statements.len(), 1);
        assert_eq!(conn.stmt_cache.len(), 1);
        assert_eq!(
            slot.and_then(|e| e.sqlstate().map(str::to_string))
                .as_deref(),
            Some("23505")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pipeline_ast_cached_rolls_back_new_state_on_encode_error() {
        let mut conn = make_test_conn_with_prepared();
        let baseline = conn.prepared_statements.len();
        let baseline_stmt_cache = conn.stmt_cache.len();

        let cmds = vec![
            Qail::get("harbors").columns(["id", "name"]).limit(1),
            Qail::get("bad\0table").columns(["id"]).limit(1),
        ];

        let err = conn
            .pipeline_execute_count_ast_cached(&cmds)
            .await
            .expect_err("expected encode error for NUL byte in table name");

        assert!(matches!(err, PgError::Encode(_)));
        assert_eq!(conn.prepared_statements.len(), baseline);
        assert_eq!(conn.stmt_cache.len(), baseline_stmt_cache);
        assert!(conn.prepared_statements.contains_key("s1"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pipeline_simple_ast_empty_batch_returns_zero_without_io() {
        let mut conn = make_test_conn_with_prepared();
        let res = conn
            .pipeline_execute_count_simple_ast(&[])
            .await
            .expect("empty batch should be a fast no-op");
        assert_eq!(res, 0);
        assert!(!conn.is_io_desynced());
    }
}
