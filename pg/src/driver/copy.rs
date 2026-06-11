//! COPY protocol methods for PostgreSQL bulk operations.
//!

use super::{
    PgConnection, PgError, PgResult, is_ignorable_session_message, parse_affected_rows,
    unexpected_backend_message,
};
use crate::protocol::{AstEncoder, BackendMessage, PgEncoder};
use bytes::BytesMut;
use qail_core::ast::{Action, Qail};
use std::future::Future;

/// Quote a single SQL identifier atom for COPY statements.
pub(crate) fn quote_copy_column_ident(ident: &str) -> PgResult<String> {
    if ident.is_empty() {
        return Err(PgError::Query(
            "COPY column identifier is empty".to_string(),
        ));
    }
    if ident.contains('\0') {
        return Err(PgError::Query(
            "COPY column identifier contains NUL byte".to_string(),
        ));
    }
    Ok(format!("\"{}\"", ident.replace('"', "\"\"")))
}

/// Quote a COPY table reference, preserving schema-qualified names.
pub(crate) fn quote_copy_table_ref(table: &str) -> PgResult<String> {
    if table.is_empty() {
        return Err(PgError::Query("COPY table identifier is empty".to_string()));
    }
    if table.contains('\0') {
        return Err(PgError::Query(
            "COPY table identifier contains NUL byte".to_string(),
        ));
    }

    table
        .split('.')
        .map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return Err(PgError::Query(
                    "COPY table identifier contains an empty path segment".to_string(),
                ));
            }
            quote_copy_column_ident(part)
        })
        .collect::<PgResult<Vec<_>>>()
        .map(|parts| parts.join("."))
}

fn parse_copy_text_row(line: &[u8]) -> PgResult<Vec<String>> {
    let line = if line.ends_with(b"\r") {
        &line[..line.len().saturating_sub(1)]
    } else {
        line
    };

    let mut fields = Vec::new();
    let mut start = 0;
    for (idx, byte) in line.iter().enumerate() {
        if *byte == b'\t' {
            fields.push(decode_copy_text_field(&line[start..idx])?);
            start = idx + 1;
        }
    }
    fields.push(decode_copy_text_field(&line[start..])?);
    Ok(fields)
}

fn decode_copy_text_field(field: &[u8]) -> PgResult<String> {
    if field == b"\\N" {
        return Err(PgError::Protocol(
            "COPY text NULL cannot be represented by Vec<String>; use copy_export_stream_raw for nullable exports"
                .to_string(),
        ));
    }

    let mut out = Vec::with_capacity(field.len());
    let mut idx = 0;
    while idx < field.len() {
        if field[idx] != b'\\' {
            out.push(field[idx]);
            idx += 1;
            continue;
        }

        let Some(&escaped) = field.get(idx + 1) else {
            return Err(PgError::Protocol(
                "COPY text field ends with incomplete backslash escape".to_string(),
            ));
        };

        match escaped {
            b'b' => {
                out.push(0x08);
                idx += 2;
            }
            b'f' => {
                out.push(0x0c);
                idx += 2;
            }
            b'n' => {
                out.push(b'\n');
                idx += 2;
            }
            b'r' => {
                out.push(b'\r');
                idx += 2;
            }
            b't' => {
                out.push(b'\t');
                idx += 2;
            }
            b'v' => {
                out.push(0x0b);
                idx += 2;
            }
            b'\\' => {
                out.push(b'\\');
                idx += 2;
            }
            b'0'..=b'7' => {
                let mut value = 0u16;
                let mut next = idx + 1;
                for _ in 0..3 {
                    let Some(&digit) = field.get(next) else {
                        break;
                    };
                    if !(b'0'..=b'7').contains(&digit) {
                        break;
                    }
                    value = (value * 8) + u16::from(digit - b'0');
                    next += 1;
                }
                if value > u16::from(u8::MAX) {
                    return Err(PgError::Protocol(format!(
                        "COPY text octal escape is out of byte range: \\{:o}",
                        value
                    )));
                }
                out.push(value as u8);
                idx = next;
            }
            b'x' => {
                let mut value = 0u8;
                let mut next = idx + 2;
                let mut digits = 0;
                while digits < 2 {
                    let Some(&digit) = field.get(next) else {
                        break;
                    };
                    let Some(nibble) = hex_nibble(digit) else {
                        break;
                    };
                    value = (value << 4) | nibble;
                    next += 1;
                    digits += 1;
                }
                if digits == 0 {
                    return Err(PgError::Protocol(
                        "COPY text hex escape requires at least one hex digit".to_string(),
                    ));
                } else {
                    out.push(value);
                    idx = next;
                }
            }
            other => {
                out.push(other);
                idx += 2;
            }
        }
    }

    String::from_utf8(out)
        .map_err(|e| PgError::Protocol(format!("COPY text field is not valid UTF-8: {}", e)))
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
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

fn encode_copy_export_sql(cmd: &Qail) -> PgResult<String> {
    if cmd.action != Action::Export {
        return Err(PgError::Query(
            "copy_export requires Qail::Export action".to_string(),
        ));
    }

    let (sql, params) =
        AstEncoder::encode_cmd_sql(cmd).map_err(|e| PgError::Encode(e.to_string()))?;
    if !params.is_empty() {
        return Err(PgError::Encode(format!(
            "copy_export cannot encode parameterized export with {} bind parameter(s); use an unfiltered export, a prefiltered database view, or a raw COPY statement with trusted SQL",
            params.len()
        )));
    }

    Ok(sql)
}

fn drain_copy_text_rows<F>(pending: &mut Vec<u8>, chunk: &[u8], on_row: &mut F) -> PgResult<()>
where
    F: FnMut(Vec<String>) -> PgResult<()>,
{
    pending.extend_from_slice(chunk);
    while let Some(pos) = pending.iter().position(|&b| b == b'\n') {
        let line = pending[..pos].to_vec();
        pending.drain(..=pos);
        let row = parse_copy_text_row(&line)?;
        on_row(row)?;
    }
    Ok(())
}

fn flush_pending_copy_text_row(pending: &[u8]) -> PgResult<()> {
    if pending.is_empty() {
        return Ok(());
    }
    Err(PgError::Protocol(
        "COPY text stream ended with a truncated row without final newline".to_string(),
    ))
}

impl PgConnection {
    /// **Fast** bulk insert using COPY protocol with zero-allocation encoding.
    /// Encodes all rows into a single buffer and writes with one syscall.
    /// ~2x faster than `copy_in_internal` due to batched I/O.
    pub(crate) async fn copy_in_fast(
        &mut self,
        table: &str,
        columns: &[String],
        rows: &[Vec<qail_core::ast::Value>],
    ) -> PgResult<u64> {
        use crate::protocol::try_encode_copy_batch;

        let cols: Vec<String> = columns
            .iter()
            .map(|c| quote_copy_column_ident(c))
            .collect::<PgResult<_>>()?;
        let sql = format!(
            "COPY {} ({}) FROM STDIN",
            quote_copy_table_ref(table)?,
            cols.join(", ")
        );

        // Encode before opening COPY mode so invalid AST data cannot leave the
        // connection waiting for CopyFail/CopyDone cleanup.
        let batch_data = try_encode_copy_batch(rows)?;

        // Send COPY command
        let bytes = PgEncoder::try_encode_query_string(&sql)?;
        self.send_bytes(&bytes).await?;

        // Wait for CopyInResponse
        let mut startup_error: Option<PgError> = None;
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyInResponse { .. } => {
                    if let Some(err) = startup_error {
                        return return_with_desync(self, err);
                    }
                    break;
                }
                BackendMessage::ReadyForQuery(_) => {
                    return return_with_desync(
                        self,
                        startup_error.unwrap_or_else(|| {
                            PgError::Protocol(
                                "COPY IN failed before CopyInResponse (unexpected ReadyForQuery)"
                                    .to_string(),
                            )
                        }),
                    );
                }
                BackendMessage::ErrorResponse(err) => {
                    if startup_error.is_none() {
                        startup_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("copy-in startup", &other),
                    );
                }
            }
        }

        // Single write for entire batch!
        self.send_copy_data(&batch_data).await?;

        // Send CopyDone
        self.send_copy_done().await?;

        // Wait for CommandComplete
        let mut affected = 0u64;
        let mut final_error: Option<PgError> = None;
        let mut saw_command_complete = false;
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CommandComplete(tag) => {
                    if saw_command_complete {
                        return return_with_desync(
                            self,
                            PgError::Protocol(
                                "COPY IN received duplicate CommandComplete".to_string(),
                            ),
                        );
                    }
                    saw_command_complete = true;
                    if final_error.is_none() {
                        match parse_affected_rows(&tag) {
                            Ok(parsed) => affected = parsed,
                            Err(err) => return return_with_desync(self, err),
                        }
                    }
                }
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = final_error {
                        return Err(err);
                    }
                    if !saw_command_complete {
                        return return_with_desync(
                            self,
                            PgError::Protocol(
                                "COPY IN completion missing CommandComplete before ReadyForQuery"
                                    .to_string(),
                            ),
                        );
                    }
                    return Ok(affected);
                }
                BackendMessage::ErrorResponse(err) => {
                    if final_error.is_none() {
                        final_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("copy-in completion", &other),
                    );
                }
            }
        }
    }

    /// **Fastest** bulk insert using COPY protocol with pre-encoded data.
    /// Accepts raw COPY text format bytes, no encoding needed.
    /// Use when caller has already encoded rows to COPY format.
    /// # Format
    /// Data should be tab-separated rows with newlines:
    /// `1\thello\t3.14\n2\tworld\t2.71\n`
    pub async fn copy_in_raw(
        &mut self,
        table: &str,
        columns: &[String],
        data: &[u8],
    ) -> PgResult<u64> {
        let cols: Vec<String> = columns
            .iter()
            .map(|c| quote_copy_column_ident(c))
            .collect::<PgResult<_>>()?;
        let sql = format!(
            "COPY {} ({}) FROM STDIN",
            quote_copy_table_ref(table)?,
            cols.join(", ")
        );

        // Send COPY command
        let bytes = PgEncoder::try_encode_query_string(&sql)?;
        self.send_bytes(&bytes).await?;

        // Wait for CopyInResponse
        let mut startup_error: Option<PgError> = None;
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyInResponse { .. } => {
                    if let Some(err) = startup_error {
                        return return_with_desync(self, err);
                    }
                    break;
                }
                BackendMessage::ReadyForQuery(_) => {
                    return return_with_desync(
                        self,
                        startup_error.unwrap_or_else(|| {
                            PgError::Protocol(
                                "COPY IN failed before CopyInResponse (unexpected ReadyForQuery)"
                                    .to_string(),
                            )
                        }),
                    );
                }
                BackendMessage::ErrorResponse(err) => {
                    if startup_error.is_none() {
                        startup_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("copy-in raw startup", &other),
                    );
                }
            }
        }

        // Single write - data is already encoded!
        self.send_copy_data(data).await?;

        // Send CopyDone
        self.send_copy_done().await?;

        // Wait for CommandComplete
        let mut affected = 0u64;
        let mut final_error: Option<PgError> = None;
        let mut saw_command_complete = false;
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CommandComplete(tag) => {
                    if saw_command_complete {
                        return return_with_desync(
                            self,
                            PgError::Protocol(
                                "COPY IN raw received duplicate CommandComplete".to_string(),
                            ),
                        );
                    }
                    saw_command_complete = true;
                    if final_error.is_none() {
                        match parse_affected_rows(&tag) {
                            Ok(parsed) => affected = parsed,
                            Err(err) => return return_with_desync(self, err),
                        }
                    }
                }
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = final_error {
                        return Err(err);
                    }
                    if !saw_command_complete {
                        return return_with_desync(
                            self,
                            PgError::Protocol(
                                "COPY IN raw completion missing CommandComplete before ReadyForQuery"
                                    .to_string(),
                            ),
                        );
                    }
                    return Ok(affected);
                }
                BackendMessage::ErrorResponse(err) => {
                    if final_error.is_none() {
                        final_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(
                        self,
                        unexpected_backend_message("copy-in raw completion", &other),
                    );
                }
            }
        }
    }

    /// Send CopyData message (raw bytes).
    pub(crate) async fn send_copy_data(&mut self, data: &[u8]) -> PgResult<()> {
        let total_len = data
            .len()
            .checked_add(4)
            .ok_or_else(|| PgError::Protocol("CopyData frame length overflow".to_string()))?;
        let len = i32::try_from(total_len)
            .map_err(|_| PgError::Protocol("CopyData frame exceeds i32::MAX".to_string()))?;

        // CopyData: 'd' + length + data
        let mut buf = BytesMut::with_capacity(1 + 4 + data.len());
        buf.extend_from_slice(b"d");
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(data);
        self.send_bytes(&buf).await?;
        Ok(())
    }

    async fn send_copy_done(&mut self) -> PgResult<()> {
        // CopyDone: 'c' + length (4)
        self.send_bytes(&[b'c', 0, 0, 0, 4]).await?;
        Ok(())
    }

    async fn start_copy_out(&mut self, sql: &str, context: &str) -> PgResult<()> {
        let bytes = PgEncoder::try_encode_query_string(sql)?;
        self.send_bytes(&bytes).await?;

        let mut startup_error: Option<PgError> = None;
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyOutResponse { .. } => {
                    if let Some(err) = startup_error {
                        return return_with_desync(self, err);
                    }
                    return Ok(());
                }
                BackendMessage::ReadyForQuery(_) => {
                    return return_with_desync(
                        self,
                        startup_error.unwrap_or_else(|| {
                            PgError::Protocol(format!(
                                "{} failed before CopyOutResponse (unexpected ReadyForQuery)",
                                context
                            ))
                        }),
                    );
                }
                BackendMessage::ErrorResponse(err) => {
                    if startup_error.is_none() {
                        startup_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(self, unexpected_backend_message(context, &other));
                }
            }
        }
    }

    async fn stream_copy_out_chunks<F, Fut>(
        &mut self,
        context: &str,
        mut on_chunk: F,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: Future<Output = PgResult<()>>,
    {
        let mut stream_error: Option<PgError> = None;
        let mut callback_error: Option<PgError> = None;
        let mut saw_copy_done = false;
        let mut saw_command_complete = false;

        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyData(chunk) => {
                    if saw_copy_done {
                        return return_with_desync(
                            self,
                            PgError::Protocol(format!(
                                "{} received CopyData after CopyDone",
                                context
                            )),
                        );
                    }
                    if stream_error.is_none()
                        && callback_error.is_none()
                        && let Err(e) = on_chunk(chunk).await
                    {
                        callback_error = Some(e);
                    }
                }
                BackendMessage::CopyDone => {
                    if saw_copy_done {
                        return return_with_desync(
                            self,
                            PgError::Protocol(format!("{} received duplicate CopyDone", context)),
                        );
                    }
                    saw_copy_done = true;
                }
                BackendMessage::CommandComplete(_) => {
                    if !saw_copy_done {
                        return return_with_desync(
                            self,
                            PgError::Protocol(format!(
                                "{} received CommandComplete before CopyDone",
                                context
                            )),
                        );
                    }
                    if saw_command_complete {
                        return return_with_desync(
                            self,
                            PgError::Protocol(format!(
                                "{} received duplicate CommandComplete",
                                context
                            )),
                        );
                    }
                    saw_command_complete = true;
                }
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = stream_error {
                        return Err(err);
                    }
                    if let Some(err) = callback_error {
                        return Err(err);
                    }
                    if !saw_copy_done {
                        return return_with_desync(
                            self,
                            PgError::Protocol(format!(
                                "{} missing CopyDone before ReadyForQuery",
                                context
                            )),
                        );
                    }
                    if !saw_command_complete {
                        return return_with_desync(
                            self,
                            PgError::Protocol(format!(
                                "{} missing CommandComplete before ReadyForQuery",
                                context
                            )),
                        );
                    }
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    if stream_error.is_none() {
                        stream_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return return_with_desync(self, unexpected_backend_message(context, &other));
                }
            }
        }
    }

    /// Export data using COPY TO STDOUT (AST-native).
    /// Takes a `Qail::Export` and returns rows as `Vec<Vec<String>>`.
    /// # Example
    /// ```ignore
    /// let cmd = Qail::export("users")
    ///     .columns(["id", "name"]);
    /// let rows = conn.copy_export(&cmd).await?;
    /// ```
    pub async fn copy_export(&mut self, cmd: &Qail) -> PgResult<Vec<Vec<String>>> {
        let mut rows = Vec::new();
        self.copy_export_stream_rows(cmd, |row| {
            rows.push(row);
            Ok(())
        })
        .await?;
        Ok(rows)
    }

    /// Stream COPY TO STDOUT chunks using an AST-native `Qail::Export` command.
    ///
    /// Chunks are forwarded as they arrive from PostgreSQL, so memory usage
    /// stays bounded by network frame size and callback processing.
    pub async fn copy_export_stream_raw<F, Fut>(&mut self, cmd: &Qail, on_chunk: F) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: Future<Output = PgResult<()>>,
    {
        let sql = encode_copy_export_sql(cmd)?;

        self.copy_out_raw_stream(&sql, on_chunk).await
    }

    /// Stream COPY TO STDOUT rows using an AST-native `Qail::Export` command.
    ///
    /// Parses PostgreSQL COPY text lines into `Vec<String>` rows and invokes
    /// `on_row` for each row without buffering the full result.
    pub async fn copy_export_stream_rows<F>(&mut self, cmd: &Qail, mut on_row: F) -> PgResult<()>
    where
        F: FnMut(Vec<String>) -> PgResult<()>,
    {
        let mut pending = Vec::new();
        self.copy_export_stream_raw(cmd, |chunk| {
            let res = drain_copy_text_rows(&mut pending, &chunk, &mut on_row);
            std::future::ready(res)
        })
        .await?;
        flush_pending_copy_text_row(&pending)
    }

    /// Export data using raw COPY TO STDOUT, returning raw bytes.
    /// Format: tab-separated values, newline-terminated rows.
    /// Suitable for direct re-import via copy_in_raw.
    ///
    /// # Safety
    /// `pub(crate)` — not exposed externally because callers pass raw SQL.
    /// External code should use `copy_export()` with the AST encoder instead.
    pub(crate) async fn copy_out_raw(&mut self, sql: &str) -> PgResult<Vec<u8>> {
        let mut data = Vec::new();
        self.copy_out_raw_stream(sql, |chunk| {
            data.extend_from_slice(&chunk);
            std::future::ready(Ok(()))
        })
        .await?;
        Ok(data)
    }

    /// Stream raw COPY TO STDOUT bytes with bounded memory usage.
    ///
    /// # Safety
    /// `pub(crate)` — callers pass raw SQL.
    pub(crate) async fn copy_out_raw_stream<F, Fut>(
        &mut self,
        sql: &str,
        on_chunk: F,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: Future<Output = PgResult<()>>,
    {
        self.start_copy_out(sql, "copy-out raw startup").await?;
        self.stream_copy_out_chunks("copy-out raw stream", on_chunk)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        drain_copy_text_rows, encode_copy_export_sql, flush_pending_copy_text_row,
        parse_copy_text_row, quote_copy_column_ident, quote_copy_table_ref, return_with_desync,
    };
    use crate::driver::{PgConnection, PgError, PgResult};
    use qail_core::ast::{Operator, Qail};

    #[cfg(unix)]
    fn test_conn() -> PgConnection {
        use crate::driver::connection::StatementCache;
        use crate::driver::stream::PgStream;
        use bytes::BytesMut;
        use std::collections::{HashMap, VecDeque};
        use std::num::NonZeroUsize;
        use tokio::net::UnixStream;

        let (unix_stream, _peer) = UnixStream::pair().expect("unix stream pair");
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
        }
    }

    #[test]
    fn parse_copy_text_row_splits_tabs() {
        let row = parse_copy_text_row(b"a\tb\tc").unwrap();
        assert_eq!(row, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_copy_text_row_trims_cr() {
        let row = parse_copy_text_row(b"a\tb\r").unwrap();
        assert_eq!(row, vec!["a", "b"]);
    }

    #[test]
    fn parse_copy_text_row_unescapes_copy_text_values() {
        let row = parse_copy_text_row(b"a\\tb\tline\\nnext\tc\\\\d").unwrap();
        assert_eq!(row, vec!["a\tb", "line\nnext", "c\\d"]);
    }

    #[test]
    fn parse_copy_text_row_rejects_copy_null_marker() {
        let err = parse_copy_text_row(b"a\t\\N\tb").expect_err("COPY NULL must not be lossy");
        assert!(
            err.to_string()
                .contains("COPY text NULL cannot be represented"),
            "{err}"
        );
    }

    #[test]
    fn parse_copy_text_row_rejects_invalid_utf8() {
        let err = parse_copy_text_row(&[0xff]).expect_err("invalid UTF-8 must fail");
        assert!(
            err.to_string()
                .contains("COPY text field is not valid UTF-8")
        );
    }

    #[test]
    fn parse_copy_text_row_rejects_incomplete_escape() {
        let err = parse_copy_text_row(b"bad\\").expect_err("trailing backslash must fail");
        assert!(err.to_string().contains("incomplete backslash escape"));
    }

    #[test]
    fn parse_copy_text_row_rejects_out_of_range_octal_escape() {
        let err = parse_copy_text_row(br"\400").expect_err("octal escape > 377 must fail");
        assert!(err.to_string().contains("out of byte range"));
    }

    #[test]
    fn parse_copy_text_row_rejects_hex_escape_without_digits() {
        let err = parse_copy_text_row(br"\xG").expect_err("hex escape without digits must fail");
        assert!(err.to_string().contains("hex escape requires"));
    }

    #[test]
    fn copy_table_quoting_preserves_schema_qualification() {
        assert_eq!(
            quote_copy_table_ref("tenant_a.users").unwrap(),
            "\"tenant_a\".\"users\""
        );
    }

    #[test]
    fn copy_identifier_quoting_rejects_nul_bytes() {
        assert!(quote_copy_table_ref("tenant\0.users").is_err());
        assert!(quote_copy_column_ident("name\0").is_err());
    }

    #[test]
    fn copy_export_rejects_parameterized_ast_before_streaming() {
        let cmd = Qail::export("users").filter("active", Operator::Eq, true);
        let err = encode_copy_export_sql(&cmd).expect_err("bind params cannot be ignored");

        assert!(matches!(err, PgError::Encode(msg) if msg.contains("parameterized export")));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn copy_return_with_desync_marks_protocol_error() {
        let mut conn = test_conn();

        let err = return_with_desync::<()>(
            &mut conn,
            PgError::Protocol("copy protocol ordering broke".to_string()),
        )
        .expect_err("protocol error must be returned");

        assert!(err.to_string().contains("copy protocol ordering broke"));
        assert!(conn.is_io_desynced());
    }

    #[test]
    fn drain_copy_text_rows_handles_chunk_boundaries() {
        let mut pending = Vec::new();
        let mut rows: Vec<Vec<String>> = Vec::new();

        drain_copy_text_rows(&mut pending, b"a\tb\nc", &mut |row: Vec<String>| {
            rows.push(row);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, vec![vec!["a".to_string(), "b".to_string()]]);
        assert_eq!(pending, b"c");

        drain_copy_text_rows(&mut pending, b"\td\n", &mut |row: Vec<String>| {
            rows.push(row);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            rows,
            vec![
                vec!["a".to_string(), "b".to_string()],
                vec!["c".to_string(), "d".to_string()]
            ]
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn flush_pending_copy_text_row_rejects_final_partial_line() {
        let pending = b"x\ty".to_vec();
        let err = flush_pending_copy_text_row(&pending)
            .expect_err("partial final COPY row must fail closed");
        assert!(matches!(err, PgError::Protocol(msg) if msg.contains("truncated row")));
        assert_eq!(pending, b"x\ty");
    }

    #[test]
    fn callback_error_bubbles_from_row_drainer() {
        let mut pending = Vec::new();
        let mut on_row =
            |_row: Vec<String>| -> PgResult<()> { Err(PgError::Query("fail".to_string())) };

        let err = drain_copy_text_rows(&mut pending, b"a\tb\n", &mut on_row).unwrap_err();
        assert!(matches!(err, PgError::Query(msg) if msg == "fail"));
    }
}
