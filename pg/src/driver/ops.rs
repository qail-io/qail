//! PgDriver operations: transaction control, batch execution, statement timeout,
//! RLS context, pipeline, COPY bulk/export, and cursor streaming.

use super::core::PgDriver;
use super::pipeline::AstPipelineMode;
use super::prepared::PreparedStatement;
use super::rls;
use super::types::*;
use super::{AutoCountPath, AutoCountPlan};
use qail_core::ast::Qail;
use qail_core::transpiler::ToSql;

impl PgDriver {
    // ==================== TRANSACTION CONTROL ====================

    /// Begin a transaction (AST-native).
    pub async fn begin(&mut self) -> PgResult<()> {
        self.connection.begin_transaction().await
    }

    /// Commit the current transaction (AST-native).
    pub async fn commit(&mut self) -> PgResult<()> {
        self.connection.commit().await
    }

    /// Rollback the current transaction (AST-native).
    pub async fn rollback(&mut self) -> PgResult<()> {
        self.connection.rollback().await
    }

    /// Create a named savepoint within the current transaction.
    /// Savepoints allow partial rollback within a transaction.
    /// Use `rollback_to()` to return to this savepoint.
    /// # Example
    /// ```ignore
    /// driver.begin().await?;
    /// driver.execute(&insert1).await?;
    /// driver.savepoint("sp1").await?;
    /// driver.execute(&insert2).await?;
    /// driver.rollback_to("sp1").await?; // Undo insert2, keep insert1
    /// driver.commit().await?;
    /// ```
    pub async fn savepoint(&mut self, name: &str) -> PgResult<()> {
        self.connection.savepoint(name).await
    }

    /// Rollback to a previously created savepoint.
    /// Discards all changes since the named savepoint was created,
    /// but keeps the transaction open.
    pub async fn rollback_to(&mut self, name: &str) -> PgResult<()> {
        self.connection.rollback_to(name).await
    }

    /// Release a savepoint (free resources, if no longer needed).
    /// After release, the savepoint cannot be rolled back to.
    pub async fn release_savepoint(&mut self, name: &str) -> PgResult<()> {
        self.connection.release_savepoint(name).await
    }

    // ==================== BATCH TRANSACTIONS ====================

    /// Execute multiple commands in a single atomic transaction.
    /// All commands succeed or all are rolled back.
    /// # Example
    /// ```ignore
    /// let cmds = vec![
    ///     Qail::add("users").columns(["name"]).values(["Alice"]),
    ///     Qail::add("users").columns(["name"]).values(["Bob"]),
    /// ];
    /// let results = driver.execute_batch(&cmds).await?;
    /// // results = [1, 1] (rows affected)
    /// ```
    pub async fn execute_batch(&mut self, cmds: &[Qail]) -> PgResult<Vec<u64>> {
        self.begin().await?;
        let mut results = Vec::with_capacity(cmds.len());
        for cmd in cmds {
            match self.execute(cmd).await {
                Ok(n) => results.push(n),
                Err(e) => {
                    self.rollback().await?;
                    return Err(e);
                }
            }
        }
        self.commit().await?;
        Ok(results)
    }

    // ==================== STATEMENT TIMEOUT ====================

    /// Set statement timeout for this connection (in milliseconds).
    /// # Example
    /// ```ignore
    /// driver.set_statement_timeout(30_000).await?; // 30 seconds
    /// ```
    pub async fn set_statement_timeout(&mut self, ms: u32) -> PgResult<()> {
        let cmd = Qail::session_set("statement_timeout", ms.to_string());
        self.execute(&cmd).await.map(|_| ())
    }

    /// Reset statement timeout to default (no limit).
    pub async fn reset_statement_timeout(&mut self) -> PgResult<()> {
        let cmd = Qail::session_reset("statement_timeout");
        self.execute(&cmd).await.map(|_| ())
    }

    // ==================== RLS (MULTI-TENANT) ====================

    /// Set the RLS context for multi-tenant data isolation.
    ///
    /// Configures PostgreSQL session variables (`app.current_tenant_id`, etc.)
    /// so that RLS policies automatically filter data by tenant.
    ///
    /// Since `PgDriver` takes `&mut self`, the borrow checker guarantees
    /// that `set_config` and all subsequent queries execute on the **same
    /// connection** — no pool race conditions possible.
    ///
    /// # Example
    /// ```ignore
    /// driver.set_rls_context(RlsContext::tenant("tenant-123")).await?;
    /// let orders = driver.fetch_all(&Qail::get("orders")).await?;
    /// // orders only contains rows for tenant-123
    /// ```
    pub async fn set_rls_context(&mut self, ctx: rls::RlsContext) -> PgResult<()> {
        let sql = rls::context_to_sql(&ctx);
        if sql.as_bytes().contains(&0) {
            return Err(crate::PgError::Protocol(
                "SQL contains NULL byte (0x00) which is invalid in PostgreSQL".to_string(),
            ));
        }
        self.connection.execute_simple(&sql).await?;
        self.rls_context = Some(ctx);
        Ok(())
    }

    /// Clear the RLS context, resetting session variables to safe defaults.
    ///
    /// After clearing, all RLS-protected queries will return zero rows
    /// (empty tenant scope matches nothing).
    pub async fn clear_rls_context(&mut self) -> PgResult<()> {
        let sql = rls::reset_sql();
        if sql.as_bytes().contains(&0) {
            return Err(crate::PgError::Protocol(
                "SQL contains NULL byte (0x00) which is invalid in PostgreSQL".to_string(),
            ));
        }
        self.connection.execute_simple(sql).await?;
        self.rls_context = None;
        Ok(())
    }

    /// Get the current RLS context, if any.
    pub fn rls_context(&self) -> Option<&rls::RlsContext> {
        self.rls_context.as_ref()
    }

    // ==================== PIPELINE (BATCH) ====================

    /// Execute multiple Qail ASTs in a single network round-trip (PIPELINING).
    /// # Example
    /// ```ignore
    /// let cmds: Vec<Qail> = (1..=1000)
    ///     .map(|i| Qail::get("harbors").columns(["id", "name"]).limit(i))
    ///     .collect();
    /// let count = driver.pipeline_execute_count(&cmds).await?;
    /// assert_eq!(count, 1000);
    /// ```
    pub async fn pipeline_execute_count(&mut self, cmds: &[Qail]) -> PgResult<usize> {
        self.pipeline_execute_count_with_mode(cmds, AstPipelineMode::Auto)
            .await
    }

    /// Execute commands with runtime auto strategy and return both count and plan.
    ///
    /// Strategy:
    /// - `len <= 1`: single cached query path
    /// - `2..8`: one-shot pipeline
    /// - `>= 8`: cached pipeline
    pub async fn execute_count_auto_with_plan(
        &mut self,
        cmds: &[Qail],
    ) -> PgResult<(usize, AutoCountPlan)> {
        let plan = AutoCountPlan::for_driver(cmds.len());

        let completed = match plan.path {
            AutoCountPath::SingleCached => {
                if cmds.is_empty() {
                    0
                } else {
                    let _ = self.fetch_all_cached(&cmds[0]).await?;
                    1
                }
            }
            AutoCountPath::PipelineOneShot => {
                self.connection
                    .pipeline_execute_count_ast_with_mode(cmds, AstPipelineMode::OneShot)
                    .await?
            }
            AutoCountPath::PipelineCached => {
                self.connection
                    .pipeline_execute_count_ast_with_mode(cmds, AstPipelineMode::Cached)
                    .await?
            }
            AutoCountPath::PoolParallel => {
                return Err(PgError::Protocol(
                    "driver auto planner returned pool-parallel path".to_string(),
                ));
            }
        };

        Ok((completed, plan))
    }

    /// Execute commands with runtime auto strategy.
    #[inline]
    pub async fn execute_count_auto(&mut self, cmds: &[Qail]) -> PgResult<usize> {
        let (completed, _plan) = self.execute_count_auto_with_plan(cmds).await?;
        Ok(completed)
    }

    /// Execute multiple Qail ASTs with an explicit pipeline strategy.
    ///
    /// Use [`AstPipelineMode::Cached`] for repeated templates in large batches,
    /// or [`AstPipelineMode::OneShot`] for tiny one-off batches.
    pub async fn pipeline_execute_count_with_mode(
        &mut self,
        cmds: &[Qail],
        mode: AstPipelineMode,
    ) -> PgResult<usize> {
        self.connection
            .pipeline_execute_count_ast_with_mode(cmds, mode)
            .await
    }

    /// Execute multiple Qail ASTs and return full row data.
    pub async fn pipeline_execute_rows(&mut self, cmds: &[Qail]) -> PgResult<Vec<Vec<PgRow>>> {
        let raw_results = self.connection.pipeline_execute_rows_ast(cmds).await?;

        let results: Vec<Vec<PgRow>> = raw_results
            .into_iter()
            .map(|rows| {
                rows.into_iter()
                    .map(|columns| PgRow {
                        columns,
                        column_info: None,
                    })
                    .collect()
            })
            .collect();

        Ok(results)
    }

    /// Run `EXPLAIN (FORMAT JSON)` on a Qail AST command and return parsed estimates.
    ///
    /// Returns `Ok(None)` when PostgreSQL returns an unexpected JSON shape.
    pub async fn explain_estimate(
        &mut self,
        cmd: &Qail,
    ) -> PgResult<Option<crate::driver::explain::ExplainEstimate>> {
        let explain_sql = format!("EXPLAIN (FORMAT JSON) {}", cmd.to_sql());
        let rows = self.connection.simple_query(&explain_sql).await?;

        let mut json_output = String::new();
        for row in &rows {
            if let Some(Some(val)) = row.columns.first()
                && let Ok(text) = std::str::from_utf8(val)
            {
                json_output.push_str(text);
            }
        }

        Ok(crate::driver::explain::parse_explain_json(&json_output))
    }

    /// Prepare a SQL statement for repeated execution.
    pub async fn prepare(&mut self, sql: &str) -> PgResult<PreparedStatement> {
        self.connection.prepare(sql).await
    }

    /// Execute a prepared statement pipeline in FAST mode (count only).
    pub async fn pipeline_execute_prepared_count(
        &mut self,
        stmt: &PreparedStatement,
        params_batch: &[Vec<Option<Vec<u8>>>],
    ) -> PgResult<usize> {
        self.connection
            .pipeline_execute_prepared_count(stmt, params_batch)
            .await
    }

    /// Bulk insert data using PostgreSQL COPY protocol (AST-native).
    /// Uses a Qail::Add to get validated table and column names from the AST,
    /// not user-provided strings. This is the sound, AST-native approach.
    /// # Example
    /// ```ignore
    /// // Create a Qail::Add to define table and columns
    /// let cmd = Qail::add("users")
    ///     .columns(["id", "name", "email"]);
    /// // Bulk insert rows
    /// let rows: Vec<Vec<Value>> = vec![
    ///     vec![Value::Int(1), Value::String("Alice"), Value::String("alice@ex.com")],
    ///     vec![Value::Int(2), Value::String("Bob"), Value::String("bob@ex.com")],
    /// ];
    /// driver.copy_bulk(&cmd, &rows).await?;
    /// ```
    pub async fn copy_bulk(
        &mut self,
        cmd: &Qail,
        rows: &[Vec<qail_core::ast::Value>],
    ) -> PgResult<u64> {
        use qail_core::ast::Action;

        if cmd.action != Action::Add {
            return Err(PgError::Query(
                "copy_bulk requires Qail::Add action".to_string(),
            ));
        }

        let table = &cmd.table;

        let columns: Vec<String> = cmd
            .columns
            .iter()
            .filter_map(|expr| {
                use qail_core::ast::Expr;
                match expr {
                    Expr::Named(name) => Some(name.clone()),
                    Expr::Aliased { name, .. } => Some(name.clone()),
                    Expr::Star => None, // Can't COPY with *
                    _ => None,
                }
            })
            .collect();

        if columns.is_empty() {
            return Err(PgError::Query(
                "copy_bulk requires columns in Qail".to_string(),
            ));
        }

        // Use optimized COPY path: direct Value → bytes encoding, single syscall
        self.connection.copy_in_fast(table, &columns, rows).await
    }

    /// **Fastest** bulk insert using pre-encoded COPY data.
    /// Accepts raw COPY text format bytes. Use when caller has already
    /// encoded rows to avoid any encoding overhead.
    /// # Format
    /// Data should be tab-separated rows with newlines (COPY text format):
    /// `1\thello\t3.14\n2\tworld\t2.71\n`
    /// # Example
    /// ```ignore
    /// let cmd = Qail::add("users").columns(["id", "name"]);
    /// let data = b"1\tAlice\n2\tBob\n";
    /// driver.copy_bulk_bytes(&cmd, data).await?;
    /// ```
    pub async fn copy_bulk_bytes(&mut self, cmd: &Qail, data: &[u8]) -> PgResult<u64> {
        use qail_core::ast::Action;

        if cmd.action != Action::Add {
            return Err(PgError::Query(
                "copy_bulk_bytes requires Qail::Add action".to_string(),
            ));
        }

        let table = &cmd.table;
        let columns: Vec<String> = cmd
            .columns
            .iter()
            .filter_map(|expr| {
                use qail_core::ast::Expr;
                match expr {
                    Expr::Named(name) => Some(name.clone()),
                    Expr::Aliased { name, .. } => Some(name.clone()),
                    _ => None,
                }
            })
            .collect();

        if columns.is_empty() {
            return Err(PgError::Query(
                "copy_bulk_bytes requires columns in Qail".to_string(),
            ));
        }

        // Direct to raw COPY - zero encoding!
        self.connection.copy_in_raw(table, &columns, data).await
    }

    /// Export table data using PostgreSQL COPY TO STDOUT (zero-copy streaming).
    /// Returns rows as tab-separated bytes for direct re-import via copy_bulk_bytes.
    /// # Example
    /// ```ignore
    /// let data = driver.copy_export_table("users", &["id", "name"]).await?;
    /// shadow_driver.copy_bulk_bytes(&cmd, &data).await?;
    /// ```
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

        self.connection.copy_out_raw(&sql).await
    }

    /// Stream table export using COPY TO STDOUT with bounded memory usage.
    ///
    /// Chunks are forwarded directly from PostgreSQL to `on_chunk`.
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
        self.connection.copy_out_raw_stream(&sql, on_chunk).await
    }

    /// Stream an AST-native `Qail::Export` command as raw COPY chunks.
    pub async fn copy_export_cmd_stream<F, Fut>(&mut self, cmd: &Qail, on_chunk: F) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = PgResult<()>>,
    {
        self.connection.copy_export_stream_raw(cmd, on_chunk).await
    }

    /// Stream an AST-native `Qail::Export` command as parsed text rows.
    pub async fn copy_export_cmd_stream_rows<F>(&mut self, cmd: &Qail, on_row: F) -> PgResult<()>
    where
        F: FnMut(Vec<String>) -> PgResult<()>,
    {
        self.connection.copy_export_stream_rows(cmd, on_row).await
    }

    /// Stream large result sets using PostgreSQL cursors.
    /// This method uses DECLARE CURSOR internally to stream rows in batches,
    /// avoiding loading the entire result set into memory.
    /// # Example
    /// ```ignore
    /// let cmd = Qail::get("large_table");
    /// let batches = driver.stream_cmd(&cmd, 100).await?;
    /// for batch in batches {
    ///     for row in batch {
    ///         // process row
    ///     }
    /// }
    /// ```
    pub async fn stream_cmd(&mut self, cmd: &Qail, batch_size: usize) -> PgResult<Vec<Vec<PgRow>>> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CURSOR_ID: AtomicU64 = AtomicU64::new(0);

        let cursor_name = format!("qail_cursor_{}", CURSOR_ID.fetch_add(1, Ordering::SeqCst));

        // AST-NATIVE: Generate SQL directly from AST (no to_sql_parameterized!)
        use crate::protocol::AstEncoder;
        let mut sql_buf = bytes::BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();
        AstEncoder::encode_select_sql(cmd, &mut sql_buf, &mut params)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        let sql = String::from_utf8_lossy(&sql_buf).to_string();

        // Must be in a transaction for cursors
        self.connection.begin_transaction().await?;

        // Declare cursor
        // Declare cursor with bind params — Extended Query Protocol handles $1, $2 etc.
        self.connection
            .declare_cursor(&cursor_name, &sql, &params)
            .await?;

        // Fetch all batches
        let mut all_batches = Vec::new();
        while let Some(rows) = self
            .connection
            .fetch_cursor(&cursor_name, batch_size)
            .await?
        {
            let pg_rows: Vec<PgRow> = rows
                .into_iter()
                .map(|cols| PgRow {
                    columns: cols,
                    column_info: None,
                })
                .collect();
            all_batches.push(pg_rows);
        }

        self.connection.close_cursor(&cursor_name).await?;
        self.connection.commit().await?;

        Ok(all_batches)
    }
}
