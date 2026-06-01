//! Native access-policy wrappers for qail-pg execution APIs.

use qail_core::access::{AccessContext, AccessError, AccessPolicy};
use qail_core::ast::Qail;

use super::{
    AstPipelineMode, AutoCountPlan, PgDriver, PgError, PgPool, PgResult, PgRow, PooledConnection,
    PreparedAstQuery, QueryResult, ResultFormat,
};

fn access_denied_error(err: AccessError) -> PgError {
    PgError::Query(format!("Access denied by policy: {}", err))
}

fn check_access(policy: &AccessPolicy, ctx: &AccessContext, cmd: &Qail) -> PgResult<()> {
    policy.check_command(ctx, cmd).map_err(access_denied_error)
}

fn check_all_access(policy: &AccessPolicy, ctx: &AccessContext, cmds: &[Qail]) -> PgResult<()> {
    for cmd in cmds {
        check_access(policy, ctx, cmd)?;
    }
    Ok(())
}

fn copy_export_table_command(table: &str, columns: &[String]) -> Qail {
    Qail::export(table).columns(columns.iter().map(String::as_str))
}

impl PgDriver {
    /// Check a command against an access policy without executing it.
    pub fn check_access(
        &self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<()> {
        check_access(access_policy, access_ctx, cmd)
    }

    /// Execute a checked QAIL command and fetch all rows using the default text format.
    pub async fn fetch_all_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all(cmd).await
    }

    /// Execute a checked QAIL command and fetch all rows using an explicit result format.
    pub async fn fetch_all_with_format_checked(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_with_format(cmd, result_format).await
    }

    /// Execute a checked QAIL command using the uncached path.
    pub async fn fetch_all_uncached_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_uncached(cmd).await
    }

    /// Execute a checked QAIL command using the uncached path and explicit result format.
    pub async fn fetch_all_uncached_with_format_checked(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_uncached_with_format(cmd, result_format)
            .await
    }

    /// Execute a checked QAIL command using the fast path.
    pub async fn fetch_all_fast_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_fast(cmd).await
    }

    /// Execute a checked QAIL command using the fast path and explicit result format.
    pub async fn fetch_all_fast_with_format_checked(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_fast_with_format(cmd, result_format).await
    }

    /// Execute a checked QAIL command and fetch one row.
    pub async fn fetch_one_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<PgRow> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_one(cmd).await
    }

    /// Prepare a checked AST query once and return a reusable frozen handle.
    ///
    /// Policy is checked at prepare time. Callers that need per-request policy
    /// changes should prepare per request or execute through the non-prepared
    /// checked wrappers.
    pub async fn prepare_ast_query_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<PreparedAstQuery> {
        check_access(access_policy, access_ctx, cmd)?;
        self.prepare_ast_query(cmd).await
    }

    /// Execute a checked QAIL command and decode rows into typed structs.
    pub async fn fetch_typed_checked<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<T>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_typed(cmd).await
    }

    /// Execute a checked QAIL command and decode typed rows using an explicit result format.
    pub async fn fetch_typed_with_format_checked<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<T>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_typed_with_format(cmd, result_format).await
    }

    /// Execute a checked QAIL command and decode one typed row.
    pub async fn fetch_one_typed_checked<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Option<T>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_one_typed(cmd).await
    }

    /// Execute a checked QAIL command and decode one typed row using an explicit format.
    pub async fn fetch_one_typed_with_format_checked<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Option<T>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_one_typed_with_format(cmd, result_format).await
    }

    /// Execute a checked mutation command.
    pub async fn execute_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<u64> {
        check_access(access_policy, access_ctx, cmd)?;
        self.execute(cmd).await
    }

    /// Bulk insert checked AST rows using PostgreSQL COPY.
    pub async fn copy_bulk_checked(
        &mut self,
        cmd: &Qail,
        rows: &[Vec<qail_core::ast::Value>],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<u64> {
        check_access(access_policy, access_ctx, cmd)?;
        self.copy_bulk(cmd, rows).await
    }

    /// Bulk insert checked pre-encoded COPY bytes.
    pub async fn copy_bulk_bytes_checked(
        &mut self,
        cmd: &Qail,
        data: &[u8],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<u64> {
        check_access(access_policy, access_ctx, cmd)?;
        self.copy_bulk_bytes(cmd, data).await
    }

    /// Export a checked table/column selection using COPY TO STDOUT.
    pub async fn copy_export_table_checked(
        &mut self,
        table: &str,
        columns: &[String],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<u8>> {
        check_access(
            access_policy,
            access_ctx,
            &copy_export_table_command(table, columns),
        )?;
        self.copy_export_table(table, columns).await
    }

    /// Stream a checked table/column selection using COPY TO STDOUT.
    pub async fn copy_export_table_stream_checked<F, Fut>(
        &mut self,
        table: &str,
        columns: &[String],
        on_chunk: F,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = PgResult<()>>,
    {
        check_access(
            access_policy,
            access_ctx,
            &copy_export_table_command(table, columns),
        )?;
        self.copy_export_table_stream(table, columns, on_chunk)
            .await
    }

    /// Stream a checked AST-native export command as raw COPY chunks.
    pub async fn copy_export_cmd_stream_checked<F, Fut>(
        &mut self,
        cmd: &Qail,
        on_chunk: F,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = PgResult<()>>,
    {
        check_access(access_policy, access_ctx, cmd)?;
        self.copy_export_cmd_stream(cmd, on_chunk).await
    }

    /// Stream a checked AST-native export command as parsed rows.
    pub async fn copy_export_cmd_stream_rows_checked<F>(
        &mut self,
        cmd: &Qail,
        on_row: F,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<String>) -> PgResult<()>,
    {
        check_access(access_policy, access_ctx, cmd)?;
        self.copy_export_cmd_stream_rows(cmd, on_row).await
    }

    /// Stream checked cursor batches for a QAIL command.
    pub async fn stream_cmd_checked(
        &mut self,
        cmd: &Qail,
        batch_size: usize,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<Vec<PgRow>>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.stream_cmd(cmd, batch_size).await
    }

    /// Execute a checked query and return a structured query result.
    pub async fn query_ast_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<QueryResult> {
        check_access(access_policy, access_ctx, cmd)?;
        self.query_ast(cmd).await
    }

    /// Execute a checked query and return a structured query result using an explicit format.
    pub async fn query_ast_with_format_checked(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<QueryResult> {
        check_access(access_policy, access_ctx, cmd)?;
        self.query_ast_with_format(cmd, result_format).await
    }

    /// Execute checked commands in one transaction.
    ///
    /// All commands are checked before `BEGIN`, so a denied later command cannot
    /// partially execute earlier commands.
    pub async fn execute_batch_checked(
        &mut self,
        cmds: &[Qail],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<u64>> {
        check_all_access(access_policy, access_ctx, cmds)?;
        self.execute_batch(cmds).await
    }

    /// Execute checked commands with runtime auto strategy and return both count and plan.
    pub async fn execute_count_auto_with_plan_checked(
        &mut self,
        cmds: &[Qail],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<(usize, AutoCountPlan)> {
        check_all_access(access_policy, access_ctx, cmds)?;
        self.execute_count_auto_with_plan(cmds).await
    }

    /// Execute checked commands with runtime auto strategy.
    pub async fn execute_count_auto_checked(
        &mut self,
        cmds: &[Qail],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<usize> {
        check_all_access(access_policy, access_ctx, cmds)?;
        self.execute_count_auto(cmds).await
    }

    /// Execute checked commands with an explicit pipeline strategy.
    pub async fn pipeline_execute_count_with_mode_checked(
        &mut self,
        cmds: &[Qail],
        mode: AstPipelineMode,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<usize> {
        check_all_access(access_policy, access_ctx, cmds)?;
        self.pipeline_execute_count_with_mode(cmds, mode).await
    }

    /// Execute checked commands with the default pipeline strategy.
    pub async fn pipeline_execute_count_checked(
        &mut self,
        cmds: &[Qail],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<usize> {
        check_all_access(access_policy, access_ctx, cmds)?;
        self.pipeline_execute_count(cmds).await
    }

    /// Execute checked commands and return full row data.
    pub async fn pipeline_execute_rows_checked(
        &mut self,
        cmds: &[Qail],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<Vec<PgRow>>> {
        check_all_access(access_policy, access_ctx, cmds)?;
        self.pipeline_execute_rows(cmds).await
    }
}

impl PooledConnection {
    /// Check a command against an access policy without executing it.
    pub fn check_access(
        &self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<()> {
        check_access(access_policy, access_ctx, cmd)
    }

    /// Execute a checked QAIL command using the default cached pooled path.
    pub async fn fetch_all_cached_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_cached(cmd).await
    }

    /// Execute a checked QAIL command using the cached pooled path with explicit format.
    pub async fn fetch_all_cached_with_format_checked(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_cached_with_format(cmd, result_format).await
    }

    /// Execute a checked QAIL command using the uncached pooled path.
    pub async fn fetch_all_uncached_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_uncached(cmd).await
    }

    /// Execute a checked QAIL command using the uncached pooled path with explicit format.
    pub async fn fetch_all_uncached_with_format_checked(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_uncached_with_format(cmd, result_format)
            .await
    }

    /// Execute a checked QAIL command using the fast pooled path.
    pub async fn fetch_all_fast_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_fast(cmd).await
    }

    /// Execute a checked QAIL command using the fast pooled path with explicit format.
    pub async fn fetch_all_fast_with_format_checked(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_fast_with_format(cmd, result_format).await
    }

    /// Execute a checked QAIL command under an already prepared RLS setup string.
    pub async fn fetch_all_with_rls_checked(
        &mut self,
        cmd: &Qail,
        rls_sql: &str,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_with_rls(cmd, rls_sql).await
    }

    /// Execute a checked QAIL command under RLS with an explicit result format.
    pub async fn fetch_all_with_rls_with_format_checked(
        &mut self,
        cmd: &Qail,
        rls_sql: &str,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<PgRow>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_all_with_rls_with_format(cmd, rls_sql, result_format)
            .await
    }

    /// Export checked data using AST-native COPY TO STDOUT.
    pub async fn copy_export_checked(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<Vec<String>>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.copy_export(cmd).await
    }

    /// Stream a checked AST-native COPY export as raw chunks.
    pub async fn copy_export_stream_raw_checked<F, Fut>(
        &mut self,
        cmd: &Qail,
        on_chunk: F,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = PgResult<()>>,
    {
        check_access(access_policy, access_ctx, cmd)?;
        self.copy_export_stream_raw(cmd, on_chunk).await
    }

    /// Stream a checked AST-native COPY export as parsed rows.
    pub async fn copy_export_stream_rows_checked<F>(
        &mut self,
        cmd: &Qail,
        on_row: F,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<String>) -> PgResult<()>,
    {
        check_access(access_policy, access_ctx, cmd)?;
        self.copy_export_stream_rows(cmd, on_row).await
    }

    /// Export a checked table/column selection using COPY TO STDOUT.
    pub async fn copy_export_table_checked(
        &mut self,
        table: &str,
        columns: &[String],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<u8>> {
        check_access(
            access_policy,
            access_ctx,
            &copy_export_table_command(table, columns),
        )?;
        self.copy_export_table(table, columns).await
    }

    /// Stream a checked table/column selection using COPY TO STDOUT.
    pub async fn copy_export_table_stream_checked<F, Fut>(
        &mut self,
        table: &str,
        columns: &[String],
        on_chunk: F,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = PgResult<()>>,
    {
        check_access(
            access_policy,
            access_ctx,
            &copy_export_table_command(table, columns),
        )?;
        self.copy_export_table_stream(table, columns, on_chunk)
            .await
    }

    /// Execute a checked QAIL command and decode rows into typed structs.
    pub async fn fetch_typed_checked<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<T>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_typed(cmd).await
    }

    /// Execute a checked QAIL command and decode typed rows using an explicit result format.
    pub async fn fetch_typed_with_format_checked<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<T>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_typed_with_format(cmd, result_format).await
    }

    /// Execute a checked QAIL command and decode one typed row.
    pub async fn fetch_one_typed_checked<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Option<T>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_one_typed(cmd).await
    }

    /// Execute a checked QAIL command and decode one typed row using an explicit format.
    pub async fn fetch_one_typed_with_format_checked<T: super::row::QailRow>(
        &mut self,
        cmd: &Qail,
        result_format: ResultFormat,
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Option<T>> {
        check_access(access_policy, access_ctx, cmd)?;
        self.fetch_one_typed_with_format(cmd, result_format).await
    }

    /// Execute checked AST commands in one pooled pipeline call.
    pub async fn pipeline_execute_rows_ast_checked(
        &mut self,
        cmds: &[Qail],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<Vec<Vec<Vec<Option<Vec<u8>>>>>> {
        check_all_access(access_policy, access_ctx, cmds)?;
        self.pipeline_execute_rows_ast(cmds).await
    }
}

impl PgPool {
    /// Execute checked commands with the pool auto strategy and return both count and plan.
    pub async fn execute_count_auto_with_plan_checked(
        &self,
        cmds: &[Qail],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<(usize, AutoCountPlan)> {
        check_all_access(access_policy, access_ctx, cmds)?;
        self.execute_count_auto_with_plan(cmds).await
    }

    /// Execute checked commands with the pool auto strategy.
    pub async fn execute_count_auto_checked(
        &self,
        cmds: &[Qail],
        access_ctx: &AccessContext,
        access_policy: &AccessPolicy,
    ) -> PgResult<usize> {
        check_all_access(access_policy, access_ctx, cmds)?;
        self.execute_count_auto(cmds).await
    }
}

#[cfg(test)]
mod tests {
    use qail_core::access::{
        AccessContext, AccessOperation, AccessPolicy, ColumnRule, TableAccessPolicy,
    };
    use qail_core::ast::{Expr, Qail};

    use super::{check_access, check_all_access, copy_export_table_command};
    use crate::driver::PgError;

    #[test]
    fn checked_pg_error_uses_existing_query_variant() {
        let err = check_access(
            &AccessPolicy::new(),
            &AccessContext::anonymous(),
            &Qail::get("orders"),
        )
        .expect_err("missing policy should fail closed");

        match err {
            PgError::Query(message) => {
                assert!(message.contains("Access denied by policy"));
                assert!(message.contains("orders"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn checked_batch_rejects_denied_later_command_before_execution() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id"])),
        );
        let cmds = vec![
            Qail::get("orders").columns(["id"]),
            Qail::get("orders").columns(["id", "private_note"]),
        ];

        let err = check_all_access(&policy, &AccessContext::anonymous(), &cmds)
            .expect_err("second command should deny before any wrapper executes");

        assert!(matches!(err, PgError::Query(_)));
    }

    #[test]
    fn checked_policy_recurses_into_subqueries() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new().allow_operations([AccessOperation::Read]),
        );
        let cmd = Qail::get("orders").columns_expr([Expr::Subquery {
            query: Box::new(Qail::get("users").columns(["id"])),
            alias: None,
        }]);

        let err = check_access(&policy, &AccessContext::anonymous(), &cmd)
            .expect_err("subquery table should require its own policy");

        match err {
            PgError::Query(message) => assert!(message.contains("users")),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn checked_copy_export_table_command_uses_read_column_policy() {
        let policy = AccessPolicy::new().with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id"])),
        );
        let columns = vec!["id".to_string(), "private_note".to_string()];
        let cmd = copy_export_table_command("orders", &columns);

        let err = check_access(&policy, &AccessContext::anonymous(), &cmd)
            .expect_err("denied COPY export column should fail before execution");

        match err {
            PgError::Query(message) => assert!(message.contains("private_note")),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
