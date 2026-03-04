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

/// Quote a SQL identifier to prevent injection.
/// Wraps in double-quotes, escapes embedded double-quotes, and strips NUL bytes.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('\0', "").replace('"', "\"\""))
}

fn parse_copy_text_row(line: &[u8]) -> Vec<String> {
    let line = if line.ends_with(b"\r") {
        &line[..line.len().saturating_sub(1)]
    } else {
        line
    };
    let text = String::from_utf8_lossy(line);
    text.split('\t').map(|s| s.to_string()).collect()
}

fn drain_copy_text_rows<F>(pending: &mut Vec<u8>, chunk: &[u8], on_row: &mut F) -> PgResult<()>
where
    F: FnMut(Vec<String>) -> PgResult<()>,
{
    pending.extend_from_slice(chunk);
    while let Some(pos) = pending.iter().position(|&b| b == b'\n') {
        let line = pending[..pos].to_vec();
        pending.drain(..=pos);
        on_row(parse_copy_text_row(&line))?;
    }
    Ok(())
}

fn flush_pending_copy_text_row<F>(pending: &mut Vec<u8>, on_row: &mut F) -> PgResult<()>
where
    F: FnMut(Vec<String>) -> PgResult<()>,
{
    if pending.is_empty() {
        return Ok(());
    }
    let line = std::mem::take(pending);
    on_row(parse_copy_text_row(&line))
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
        use crate::protocol::encode_copy_batch;

        let cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let sql = format!(
            "COPY {} ({}) FROM STDIN",
            quote_ident(table),
            cols.join(", ")
        );

        // Send COPY command
        let bytes = PgEncoder::try_encode_query_string(&sql)?;
        self.write_all_with_timeout(&bytes, "stream write").await?;

        // Wait for CopyInResponse
        let mut startup_error: Option<PgError> = None;
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyInResponse { .. } => {
                    if let Some(err) = startup_error {
                        return Err(err);
                    }
                    break;
                }
                BackendMessage::ReadyForQuery(_) => {
                    return Err(startup_error.unwrap_or_else(|| {
                        PgError::Protocol(
                            "COPY IN failed before CopyInResponse (unexpected ReadyForQuery)"
                                .to_string(),
                        )
                    }));
                }
                BackendMessage::ErrorResponse(err) => {
                    if startup_error.is_none() {
                        startup_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return Err(unexpected_backend_message("copy-in startup", &other));
                }
            }
        }

        // Encode ALL rows into a single buffer (zero-allocation per value)
        let batch_data = encode_copy_batch(rows);

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
                        return Err(PgError::Protocol(
                            "COPY IN received duplicate CommandComplete".to_string(),
                        ));
                    }
                    saw_command_complete = true;
                    if final_error.is_none() {
                        affected = parse_affected_rows(&tag);
                    }
                }
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = final_error {
                        return Err(err);
                    }
                    if !saw_command_complete {
                        return Err(PgError::Protocol(
                            "COPY IN completion missing CommandComplete before ReadyForQuery"
                                .to_string(),
                        ));
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
                    return Err(unexpected_backend_message("copy-in completion", &other));
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
        let cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let sql = format!(
            "COPY {} ({}) FROM STDIN",
            quote_ident(table),
            cols.join(", ")
        );

        // Send COPY command
        let bytes = PgEncoder::try_encode_query_string(&sql)?;
        self.write_all_with_timeout(&bytes, "stream write").await?;

        // Wait for CopyInResponse
        let mut startup_error: Option<PgError> = None;
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyInResponse { .. } => {
                    if let Some(err) = startup_error {
                        return Err(err);
                    }
                    break;
                }
                BackendMessage::ReadyForQuery(_) => {
                    return Err(startup_error.unwrap_or_else(|| {
                        PgError::Protocol(
                            "COPY IN failed before CopyInResponse (unexpected ReadyForQuery)"
                                .to_string(),
                        )
                    }));
                }
                BackendMessage::ErrorResponse(err) => {
                    if startup_error.is_none() {
                        startup_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => {
                    return Err(unexpected_backend_message("copy-in raw startup", &other));
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
                        return Err(PgError::Protocol(
                            "COPY IN raw received duplicate CommandComplete".to_string(),
                        ));
                    }
                    saw_command_complete = true;
                    if final_error.is_none() {
                        affected = parse_affected_rows(&tag);
                    }
                }
                BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = final_error {
                        return Err(err);
                    }
                    if !saw_command_complete {
                        return Err(PgError::Protocol(
                            "COPY IN raw completion missing CommandComplete before ReadyForQuery"
                                .to_string(),
                        ));
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
                    return Err(unexpected_backend_message("copy-in raw completion", &other));
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
        self.write_all_with_timeout(&buf, "stream write").await?;
        Ok(())
    }

    async fn send_copy_done(&mut self) -> PgResult<()> {
        // CopyDone: 'c' + length (4)
        self.write_all_with_timeout(&[b'c', 0, 0, 0, 4], "stream write")
            .await?;
        Ok(())
    }

    async fn start_copy_out(&mut self, sql: &str, context: &str) -> PgResult<()> {
        let bytes = PgEncoder::try_encode_query_string(sql)?;
        self.write_all_with_timeout(&bytes, "stream write").await?;

        let mut startup_error: Option<PgError> = None;
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyOutResponse { .. } => {
                    if let Some(err) = startup_error {
                        return Err(err);
                    }
                    return Ok(());
                }
                BackendMessage::ReadyForQuery(_) => {
                    return Err(startup_error.unwrap_or_else(|| {
                        PgError::Protocol(format!(
                            "{} failed before CopyOutResponse (unexpected ReadyForQuery)",
                            context
                        ))
                    }));
                }
                BackendMessage::ErrorResponse(err) => {
                    if startup_error.is_none() {
                        startup_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => return Err(unexpected_backend_message(context, &other)),
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
                        return Err(PgError::Protocol(format!(
                            "{} received CopyData after CopyDone",
                            context
                        )));
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
                        return Err(PgError::Protocol(format!(
                            "{} received duplicate CopyDone",
                            context
                        )));
                    }
                    saw_copy_done = true;
                }
                BackendMessage::CommandComplete(_) => {
                    if saw_command_complete {
                        return Err(PgError::Protocol(format!(
                            "{} received duplicate CommandComplete",
                            context
                        )));
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
                        return Err(PgError::Protocol(format!(
                            "{} missing CopyDone before ReadyForQuery",
                            context
                        )));
                    }
                    if !saw_command_complete {
                        return Err(PgError::Protocol(format!(
                            "{} missing CommandComplete before ReadyForQuery",
                            context
                        )));
                    }
                    return Ok(());
                }
                BackendMessage::ErrorResponse(err) => {
                    if stream_error.is_none() {
                        stream_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                msg if is_ignorable_session_message(&msg) => {}
                other => return Err(unexpected_backend_message(context, &other)),
            }
        }
    }

    /// Export data using COPY TO STDOUT (AST-native).
    /// Takes a `Qail::Export` and returns rows as `Vec<Vec<String>>`.
    /// # Example
    /// ```ignore
    /// let cmd = Qail::export("users")
    ///     .columns(["id", "name"])
    ///     .filter("active", true);
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
    pub async fn copy_export_stream_raw<F, Fut>(
        &mut self,
        cmd: &Qail,
        on_chunk: F,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: Future<Output = PgResult<()>>,
    {
        if cmd.action != Action::Export {
            return Err(PgError::Query(
                "copy_export requires Qail::Export action".to_string(),
            ));
        }

        // Encode command to SQL using AST encoder
        let (sql, _params) =
            AstEncoder::encode_cmd_sql(cmd).map_err(|e| PgError::Encode(e.to_string()))?;

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
        flush_pending_copy_text_row(&mut pending, &mut on_row)
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
    use super::{drain_copy_text_rows, flush_pending_copy_text_row, parse_copy_text_row};
    use crate::driver::{PgError, PgResult};

    #[test]
    fn parse_copy_text_row_splits_tabs() {
        let row = parse_copy_text_row(b"a\tb\tc");
        assert_eq!(row, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_copy_text_row_trims_cr() {
        let row = parse_copy_text_row(b"a\tb\r");
        assert_eq!(row, vec!["a", "b"]);
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
    fn flush_pending_copy_text_row_emits_final_partial_line() {
        let mut pending = b"x\ty".to_vec();
        let mut rows = Vec::new();
        let mut on_row = |row: Vec<String>| -> PgResult<()> {
            rows.push(row);
            Ok(())
        };

        flush_pending_copy_text_row(&mut pending, &mut on_row).unwrap();
        assert_eq!(rows, vec![vec!["x".to_string(), "y".to_string()]]);
        assert!(pending.is_empty());
    }

    #[test]
    fn callback_error_bubbles_from_row_drainer() {
        let mut pending = Vec::new();
        let mut on_row = |_row: Vec<String>| -> PgResult<()> {
            Err(PgError::Query("fail".to_string()))
        };

        let err = drain_copy_text_rows(&mut pending, b"a\tb\n", &mut on_row).unwrap_err();
        assert!(matches!(err, PgError::Query(msg) if msg == "fail"));
    }
}
