//! COPY protocol methods for PostgreSQL bulk operations.
//!
//! All methods use AST-native approach - no raw SQL strings.

use bytes::BytesMut;
use tokio::io::AsyncWriteExt;
use qail_core::ast::{QailCmd, Action};
use crate::protocol::{BackendMessage, PgEncoder, AstEncoder};
use super::{PgConnection, PgError, PgResult, parse_affected_rows};

impl PgConnection {
    /// Internal bulk insert using COPY protocol (crate-private).
    ///
    /// Use `PgDriver::copy_bulk(&QailCmd)` for AST-native access.
    pub(crate) async fn copy_in_internal(
        &mut self,
        table: &str,
        columns: &[String],
        rows: &[Vec<String>],
    ) -> PgResult<u64> {
        // Build COPY command
        let cols = columns.join(", ");
        let sql = format!("COPY {} ({}) FROM STDIN", table, cols);
        
        // Send COPY command
        let bytes = PgEncoder::encode_query_string(&sql);
        self.stream.write_all(&bytes).await?;

        // Wait for CopyInResponse
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyInResponse { .. } => break,
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }

        // Send data rows as CopyData messages
        for row in rows {
            let line = row.join("\t") + "\n";
            self.send_copy_data(line.as_bytes()).await?;
        }

        // Send CopyDone
        self.send_copy_done().await?;

        // Wait for CommandComplete
        let mut affected = 0u64;
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CommandComplete(tag) => {
                    affected = parse_affected_rows(&tag);
                }
                BackendMessage::ReadyForQuery(_) => {
                    return Ok(affected);
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }
    }

    /// Send CopyData message (raw bytes).
    async fn send_copy_data(&mut self, data: &[u8]) -> PgResult<()> {
        // CopyData: 'd' + length + data
        let len = (data.len() + 4) as i32;
        let mut buf = BytesMut::with_capacity(1 + 4 + data.len());
        buf.extend_from_slice(&[b'd']);
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(data);
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    /// Send CopyDone message.
    async fn send_copy_done(&mut self) -> PgResult<()> {
        // CopyDone: 'c' + length (4)
        self.stream.write_all(&[b'c', 0, 0, 0, 4]).await?;
        Ok(())
    }

    /// Export data using COPY TO STDOUT (AST-native).
    /// 
    /// Takes a QailCmd::Export and returns rows as Vec<Vec<String>>.
    /// 
    /// # Example
    /// ```ignore
    /// let cmd = QailCmd::export("users")
    ///     .columns(["id", "name"])
    ///     .filter("active", true);
    /// 
    /// let rows = conn.copy_export(&cmd).await?;
    /// ```
    pub async fn copy_export(
        &mut self,
        cmd: &QailCmd,
    ) -> PgResult<Vec<Vec<String>>> {
        // Validate action
        if cmd.action != Action::Export {
            return Err(PgError::Query(
                "copy_export requires QailCmd::Export action".to_string()
            ));
        }

        // Encode command to SQL using AST encoder
        let (sql, _params) = AstEncoder::encode_cmd_sql(cmd);
        
        // Send COPY command
        let bytes = PgEncoder::encode_query_string(&sql);
        self.stream.write_all(&bytes).await?;

        // Wait for CopyOutResponse
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyOutResponse { .. } => break,
                BackendMessage::ErrorResponse(err) => {
                    return Err(PgError::Query(err.message));
                }
                _ => {}
            }
        }

        // Receive CopyData messages until CopyDone
        let mut rows = Vec::new();
        loop {
            let msg = self.recv().await?;
            match msg {
                BackendMessage::CopyData(data) => {
                    // Parse tab-separated line
                    let line = String::from_utf8_lossy(&data);
                    let line = line.trim_end_matches('\n');
                    let cols: Vec<String> = line.split('\t')
                        .map(|s| s.to_string())
                        .collect();
                    rows.push(cols);
                }
                BackendMessage::CopyDone => {}
                BackendMessage::CommandComplete(_) => {}
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
}
