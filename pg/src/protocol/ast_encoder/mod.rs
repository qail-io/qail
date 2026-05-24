//! AST-Native Encoder
//!
//! Direct AST → Wire Protocol Bytes conversion.
//! NO INTERMEDIATE SQL STRING!
//!
//! This is the TRUE AST-native path:
//! Qail → BytesMut (no to_sql() call)
//!
//! ## Module Structure
//!
//! - `helpers` - Zero-allocation lookup tables and write functions
//! - `ddl` - CREATE, DROP, ALTER statements
//! - `dml` - SELECT, INSERT, UPDATE, DELETE, EXPORT
//! - `values` - Expression, operator, and value encoding
//! - `batch` - Batch and wire protocol encoding

mod batch;
mod ddl;
pub(crate) mod dml; // pub(crate) for internal use in driver
pub use crate::protocol::EncodeError;
mod helpers;
mod values;

use bytes::BytesMut;
use qail_core::ast::{Action, Qail};

/// Shorthand for the common return type of encode methods.
type EncodeResult = Result<(BytesMut, Vec<Option<Vec<u8>>>), EncodeError>;

/// Shorthand for encode methods that return SQL as a `String`.
type EncodeSqlResult = Result<(String, Vec<Option<Vec<u8>>>), EncodeError>;

/// AST-native encoder that skips SQL string generation.
pub struct AstEncoder;

impl AstEncoder {
    /// Encode a Qail directly to Extended Query protocol bytes.
    /// Returns (wire_bytes, extracted_params_as_bytes)
    pub fn encode_cmd(cmd: &Qail) -> EncodeResult {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        Self::encode_cmd_sql_to(cmd, &mut sql_buf, &mut params)?;

        let sql_bytes = sql_buf.freeze();
        let wire = batch::build_extended_query(&sql_bytes, &params)?;

        Ok((wire, params))
    }

    /// Encode a Qail directly to Extended Query protocol bytes.
    /// `result_format`: 0 = text, 1 = binary.
    pub fn encode_cmd_with_result_format(cmd: &Qail, result_format: i16) -> EncodeResult {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        Self::encode_cmd_sql_to(cmd, &mut sql_buf, &mut params)?;

        let sql_bytes = sql_buf.freeze();
        let wire =
            batch::build_extended_query_with_result_format(&sql_bytes, &params, result_format)?;

        Ok((wire, params))
    }

    /// Encode a Qail using CALLER'S BUFFERS (ZERO-ALLOC).
    /// Clears and reuses the provided buffers to avoid allocations.
    /// Returns wire protocol bytes ready to send.
    #[inline]
    pub fn encode_cmd_reuse(
        cmd: &Qail,
        sql_buf: &mut BytesMut,
        params: &mut Vec<Option<Vec<u8>>>,
    ) -> Result<BytesMut, EncodeError> {
        Self::encode_cmd_sql_to(cmd, sql_buf, params)?;

        // Build wire protocol (allocates a new BytesMut)
        batch::build_extended_query(sql_buf, params)
    }

    /// Encode a Qail using caller buffers with explicit result-column format.
    /// `result_format`: 0 = text, 1 = binary.
    #[inline]
    pub fn encode_cmd_reuse_with_result_format(
        cmd: &Qail,
        sql_buf: &mut BytesMut,
        params: &mut Vec<Option<Vec<u8>>>,
        result_format: i16,
    ) -> Result<BytesMut, EncodeError> {
        Self::encode_cmd_sql_to(cmd, sql_buf, params)?;

        // Build wire protocol (allocates a new BytesMut)
        batch::build_extended_query_with_result_format(sql_buf, params, result_format)
    }

    /// Encode a Qail using CALLER'S BUFFERS — writes wire bytes into `wire_buf` (ZERO-ALLOC).
    /// This is the fastest path: clears all 3 buffers but keeps capacity.
    /// Use with `connection.write_buf` for single-syscall send.
    #[inline]
    pub fn encode_cmd_reuse_into(
        cmd: &Qail,
        sql_buf: &mut BytesMut,
        params: &mut Vec<Option<Vec<u8>>>,
        wire_buf: &mut BytesMut,
    ) -> Result<(), EncodeError> {
        Self::encode_cmd_sql_to(cmd, sql_buf, params)?;

        // Build wire protocol into caller's buffer (zero-alloc)
        batch::build_extended_query_into(wire_buf, sql_buf, params)?;
        Ok(())
    }

    /// Encode a Qail into caller-provided wire buffer with explicit result format.
    /// `result_format`: 0 = text, 1 = binary.
    #[inline]
    pub fn encode_cmd_reuse_into_with_result_format(
        cmd: &Qail,
        sql_buf: &mut BytesMut,
        params: &mut Vec<Option<Vec<u8>>>,
        wire_buf: &mut BytesMut,
        result_format: i16,
    ) -> Result<(), EncodeError> {
        Self::encode_cmd_sql_to(cmd, sql_buf, params)?;

        // Build wire protocol into caller's buffer (zero-alloc)
        batch::build_extended_query_into_with_result_format(
            wire_buf,
            sql_buf,
            params,
            result_format,
        )?;
        Ok(())
    }

    /// Internal helper: encode AST to SQL bytes + params (shared by both reuse variants).
    #[inline]
    fn encode_cmd_sql_to(
        cmd: &Qail,
        sql_buf: &mut BytesMut,
        params: &mut Vec<Option<Vec<u8>>>,
    ) -> Result<(), EncodeError> {
        // Clear buffers (but keep capacity!)
        sql_buf.clear();
        params.clear();

        match cmd.action {
            Action::Get | Action::With => {
                dml::encode_select(cmd, sql_buf, params)?;
            }
            Action::Cnt => {
                dml::encode_count(cmd, sql_buf, params)?;
            }
            Action::Add => {
                dml::encode_insert(cmd, sql_buf, params)?;
            }
            Action::Set => {
                dml::encode_update(cmd, sql_buf, params)?;
            }
            Action::Del => {
                dml::encode_delete(cmd, sql_buf, params)?;
            }
            Action::Merge => {
                dml::encode_merge(cmd, sql_buf, params)?;
            }
            Action::Export => {
                dml::encode_export(cmd, sql_buf, params)?;
            }
            Action::Make => ddl::encode_make(cmd, sql_buf)?,
            Action::Index => ddl::encode_index(cmd, sql_buf)?,
            Action::Drop => ddl::encode_drop_table(cmd, sql_buf),
            Action::DropIndex => ddl::encode_drop_index(cmd, sql_buf),
            Action::Alter => ddl::encode_alter_add_column(cmd, sql_buf)?,
            Action::AlterDrop => ddl::encode_alter_drop_column(cmd, sql_buf)?,
            Action::AlterType => ddl::encode_alter_column_type(cmd, sql_buf)?,
            Action::Mod => ddl::encode_rename_column(cmd, sql_buf)?,
            Action::CreateView => ddl::encode_create_view(cmd, sql_buf, params)?,
            Action::DropView => ddl::encode_drop_view(cmd, sql_buf),
            Action::CreateMaterializedView => {
                ddl::encode_create_materialized_view(cmd, sql_buf, params)?
            }
            Action::RefreshMaterializedView => ddl::encode_refresh_materialized_view(cmd, sql_buf),
            Action::DropMaterializedView => ddl::encode_drop_materialized_view(cmd, sql_buf),
            Action::CreateFunction => ddl::encode_create_function(cmd, sql_buf)?,
            Action::DropFunction => ddl::encode_drop_function(cmd, sql_buf),
            Action::CreateTrigger => ddl::encode_create_trigger(cmd, sql_buf)?,
            Action::DropTrigger => ddl::encode_drop_trigger(cmd, sql_buf)?,
            Action::CreateExtension => ddl::encode_create_extension(cmd, sql_buf)?,
            Action::DropExtension => ddl::encode_drop_extension(cmd, sql_buf),
            Action::CommentOn => ddl::encode_comment_on(cmd, sql_buf),
            Action::CreateSequence => ddl::encode_create_sequence(cmd, sql_buf)?,
            Action::DropSequence => ddl::encode_drop_sequence(cmd, sql_buf),
            Action::CreateEnum => ddl::encode_create_enum(cmd, sql_buf),
            Action::DropEnum => ddl::encode_drop_enum(cmd, sql_buf),
            Action::AlterEnumAddValue => ddl::encode_alter_enum_add_value(cmd, sql_buf),
            Action::AlterSetNotNull => ddl::encode_alter_set_not_null(cmd, sql_buf)?,
            Action::AlterDropNotNull => ddl::encode_alter_drop_not_null(cmd, sql_buf)?,
            Action::AlterSetDefault => ddl::encode_alter_set_default(cmd, sql_buf)?,
            Action::AlterDropDefault => ddl::encode_alter_drop_default(cmd, sql_buf)?,
            Action::AlterEnableRls => ddl::encode_alter_enable_rls(cmd, sql_buf),
            Action::AlterDisableRls => ddl::encode_alter_disable_rls(cmd, sql_buf),
            Action::AlterForceRls => ddl::encode_alter_force_rls(cmd, sql_buf),
            Action::AlterNoForceRls => ddl::encode_alter_no_force_rls(cmd, sql_buf),
            Action::Call => ddl::encode_call(cmd, sql_buf),
            Action::Do => ddl::encode_do(cmd, sql_buf),
            Action::SessionSet => ddl::encode_session_set(cmd, sql_buf),
            Action::SessionShow => ddl::encode_session_show(cmd, sql_buf),
            Action::SessionReset => ddl::encode_session_reset(cmd, sql_buf),
            Action::CreateDatabase => ddl::encode_create_database(cmd, sql_buf),
            Action::DropDatabase => ddl::encode_drop_database(cmd, sql_buf),
            Action::Grant => ddl::encode_grant(cmd, sql_buf)?,
            Action::Revoke => ddl::encode_revoke(cmd, sql_buf)?,
            Action::CreatePolicy => ddl::encode_create_policy(cmd, sql_buf)?,
            Action::DropPolicy => ddl::encode_drop_policy(cmd, sql_buf)?,
            Action::Listen => ddl::encode_listen(cmd, sql_buf),
            Action::Unlisten => ddl::encode_unlisten(cmd, sql_buf),
            Action::Notify => ddl::encode_notify(cmd, sql_buf),
            Action::Savepoint => ddl::encode_savepoint(cmd, sql_buf),
            Action::ReleaseSavepoint => ddl::encode_release_savepoint(cmd, sql_buf),
            Action::RollbackToSavepoint => ddl::encode_rollback_to_savepoint(cmd, sql_buf),
            _ => return Err(EncodeError::UnsupportedAction(cmd.action)),
        }
        Ok(())
    }

    /// Encode a Qail to SQL string + params (for prepared statement caching).
    pub fn encode_cmd_sql(cmd: &Qail) -> EncodeSqlResult {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        match cmd.action {
            Action::Get | Action::With => {
                dml::encode_select(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Cnt => {
                dml::encode_count(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Add => {
                dml::encode_insert(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Set => {
                dml::encode_update(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Del => {
                dml::encode_delete(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Merge => {
                dml::encode_merge(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Export => {
                dml::encode_export(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Make => ddl::encode_make(cmd, &mut sql_buf)?,
            Action::Index => ddl::encode_index(cmd, &mut sql_buf)?,
            Action::Drop => ddl::encode_drop_table(cmd, &mut sql_buf),
            Action::DropIndex => ddl::encode_drop_index(cmd, &mut sql_buf),
            Action::Alter => ddl::encode_alter_add_column(cmd, &mut sql_buf)?,
            Action::AlterDrop => ddl::encode_alter_drop_column(cmd, &mut sql_buf)?,
            Action::AlterType => ddl::encode_alter_column_type(cmd, &mut sql_buf)?,
            Action::Mod => ddl::encode_rename_column(cmd, &mut sql_buf)?,
            Action::CreateView => ddl::encode_create_view(cmd, &mut sql_buf, &mut params)?,
            Action::DropView => ddl::encode_drop_view(cmd, &mut sql_buf),
            Action::CreateMaterializedView => {
                ddl::encode_create_materialized_view(cmd, &mut sql_buf, &mut params)?
            }
            Action::RefreshMaterializedView => {
                ddl::encode_refresh_materialized_view(cmd, &mut sql_buf)
            }
            Action::DropMaterializedView => ddl::encode_drop_materialized_view(cmd, &mut sql_buf),
            Action::CreateFunction => ddl::encode_create_function(cmd, &mut sql_buf)?,
            Action::DropFunction => ddl::encode_drop_function(cmd, &mut sql_buf),
            Action::CreateTrigger => ddl::encode_create_trigger(cmd, &mut sql_buf)?,
            Action::DropTrigger => ddl::encode_drop_trigger(cmd, &mut sql_buf)?,
            Action::CreateExtension => ddl::encode_create_extension(cmd, &mut sql_buf)?,
            Action::DropExtension => ddl::encode_drop_extension(cmd, &mut sql_buf),
            Action::CommentOn => ddl::encode_comment_on(cmd, &mut sql_buf),
            Action::CreateSequence => ddl::encode_create_sequence(cmd, &mut sql_buf)?,
            Action::DropSequence => ddl::encode_drop_sequence(cmd, &mut sql_buf),
            Action::CreateEnum => ddl::encode_create_enum(cmd, &mut sql_buf),
            Action::DropEnum => ddl::encode_drop_enum(cmd, &mut sql_buf),
            Action::AlterEnumAddValue => ddl::encode_alter_enum_add_value(cmd, &mut sql_buf),
            Action::AlterSetNotNull => ddl::encode_alter_set_not_null(cmd, &mut sql_buf)?,
            Action::AlterDropNotNull => ddl::encode_alter_drop_not_null(cmd, &mut sql_buf)?,
            Action::AlterSetDefault => ddl::encode_alter_set_default(cmd, &mut sql_buf)?,
            Action::AlterDropDefault => ddl::encode_alter_drop_default(cmd, &mut sql_buf)?,
            Action::AlterEnableRls => ddl::encode_alter_enable_rls(cmd, &mut sql_buf),
            Action::AlterDisableRls => ddl::encode_alter_disable_rls(cmd, &mut sql_buf),
            Action::AlterForceRls => ddl::encode_alter_force_rls(cmd, &mut sql_buf),
            Action::AlterNoForceRls => ddl::encode_alter_no_force_rls(cmd, &mut sql_buf),
            Action::Call => ddl::encode_call(cmd, &mut sql_buf),
            Action::Do => ddl::encode_do(cmd, &mut sql_buf),
            Action::SessionSet => ddl::encode_session_set(cmd, &mut sql_buf),
            Action::SessionShow => ddl::encode_session_show(cmd, &mut sql_buf),
            Action::SessionReset => ddl::encode_session_reset(cmd, &mut sql_buf),
            Action::CreateDatabase => ddl::encode_create_database(cmd, &mut sql_buf),
            Action::DropDatabase => ddl::encode_drop_database(cmd, &mut sql_buf),
            Action::Grant => ddl::encode_grant(cmd, &mut sql_buf)?,
            Action::Revoke => ddl::encode_revoke(cmd, &mut sql_buf)?,
            Action::CreatePolicy => ddl::encode_create_policy(cmd, &mut sql_buf)?,
            Action::DropPolicy => ddl::encode_drop_policy(cmd, &mut sql_buf)?,
            Action::Listen => ddl::encode_listen(cmd, &mut sql_buf),
            Action::Unlisten => ddl::encode_unlisten(cmd, &mut sql_buf),
            Action::Notify => ddl::encode_notify(cmd, &mut sql_buf),
            Action::Savepoint => ddl::encode_savepoint(cmd, &mut sql_buf),
            Action::ReleaseSavepoint => ddl::encode_release_savepoint(cmd, &mut sql_buf),
            Action::RollbackToSavepoint => ddl::encode_rollback_to_savepoint(cmd, &mut sql_buf),
            _ => return Err(EncodeError::UnsupportedAction(cmd.action)),
        }

        let sql = String::from_utf8_lossy(&sql_buf).to_string();
        Ok((sql, params))
    }

    /// Encode DML shapes supported by the prepared-statement cache fast path.
    ///
    /// Returns `Ok(false)` when callers should use their existing fallback for
    /// non-cacheable actions such as COPY/DDL/session commands.
    #[inline]
    pub(crate) fn encode_cacheable_cmd_sql_to(
        cmd: &Qail,
        sql_buf: &mut BytesMut,
        params: &mut Vec<Option<Vec<u8>>>,
    ) -> Result<bool, EncodeError> {
        sql_buf.clear();
        params.clear();

        match cmd.action {
            Action::Get | Action::With => {
                dml::encode_select(cmd, sql_buf, params)?;
            }
            Action::Cnt => {
                dml::encode_count(cmd, sql_buf, params)?;
            }
            Action::Add => {
                dml::encode_insert(cmd, sql_buf, params)?;
            }
            Action::Set => {
                dml::encode_update(cmd, sql_buf, params)?;
            }
            Action::Del => {
                dml::encode_delete(cmd, sql_buf, params)?;
            }
            Action::Merge => {
                dml::encode_merge(cmd, sql_buf, params)?;
            }
            _ => return Ok(false),
        }

        Ok(true)
    }

    /// Encode AST into caller-provided SQL/params buffers (no SQL `String` allocation).
    ///
    /// This is useful for hot paths that need SQL bytes + params, but can defer
    /// `String` creation to cache-miss branches only.
    #[inline]
    pub fn encode_cmd_sql_reuse(
        cmd: &Qail,
        sql_buf: &mut BytesMut,
        params: &mut Vec<Option<Vec<u8>>>,
    ) -> Result<(), EncodeError> {
        Self::encode_cmd_sql_to(cmd, sql_buf, params)
    }

    /// Extract ONLY params from a Qail (for reusing cached SQL template).
    #[inline]
    pub fn encode_cmd_params_only(cmd: &Qail) -> Result<Vec<Option<Vec<u8>>>, EncodeError> {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        match cmd.action {
            Action::Get => {
                dml::encode_select(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Add => {
                dml::encode_insert(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Set => {
                dml::encode_update(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Del => {
                dml::encode_delete(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Merge => {
                dml::encode_merge(cmd, &mut sql_buf, &mut params)?;
            }
            _ => {}
        }

        Ok(params)
    }

    /// Generate just SQL bytes for a SELECT statement.
    pub fn encode_select_sql(
        cmd: &Qail,
        buf: &mut BytesMut,
        params: &mut Vec<Option<Vec<u8>>>,
    ) -> Result<(), EncodeError> {
        dml::encode_select(cmd, buf, params)
    }

    /// Encode multiple Qails as a pipeline batch.
    pub fn encode_batch(cmds: &[Qail]) -> Result<BytesMut, EncodeError> {
        batch::encode_batch(cmds)
    }

    /// Encode multiple Qails as a pipeline batch with explicit result format.
    /// `result_format`: 0 = text, 1 = binary.
    pub fn encode_batch_with_result_format(
        cmds: &[Qail],
        result_format: i16,
    ) -> Result<BytesMut, EncodeError> {
        batch::encode_batch_with_result_format(cmds, result_format)
    }

    /// Encode multiple Qails using Simple Query Protocol.
    #[inline]
    pub fn encode_batch_simple(cmds: &[Qail]) -> Result<BytesMut, EncodeError> {
        batch::encode_batch_simple(cmds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_cacheable_cmd_sql_to_supports_count_and_merge() {
        use qail_core::ast::{Expr, Operator};

        let mut sql_buf = BytesMut::new();
        let mut params = Vec::new();
        let count_cmd = Qail {
            action: Action::Cnt,
            table: "orders".to_string(),
            ..Default::default()
        }
        .filter("status", Operator::Eq, "paid");

        assert!(
            AstEncoder::encode_cacheable_cmd_sql_to(&count_cmd, &mut sql_buf, &mut params).unwrap()
        );
        let sql = String::from_utf8_lossy(&sql_buf);
        assert!(sql.contains("SELECT COUNT(*) FROM orders"), "{sql}");
        assert!(sql.contains("WHERE status = $1"), "{sql}");
        assert_eq!(params, vec![Some(b"paid".to_vec())]);

        let merge_cmd = Qail::merge_into("users")
            .target_alias("u")
            .using_table_as("staging_users", "s")
            .merge_on_column("u.id", Operator::Eq, "s.id")
            .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);

        assert!(
            AstEncoder::encode_cacheable_cmd_sql_to(&merge_cmd, &mut sql_buf, &mut params).unwrap()
        );
        let sql = String::from_utf8_lossy(&sql_buf);
        assert!(sql.contains("MERGE INTO users AS u"), "{sql}");
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET name = s.name"),
            "{sql}"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_cacheable_cmd_sql_to_rejects_non_dml_fast_path() {
        let mut sql_buf = BytesMut::from("stale");
        let mut params = vec![Some(b"stale".to_vec())];
        let cmd = Qail::make("users");

        assert!(!AstEncoder::encode_cacheable_cmd_sql_to(&cmd, &mut sql_buf, &mut params).unwrap());
        assert!(sql_buf.is_empty());
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_select() {
        let cmd = Qail::get("users").columns(["id", "name"]);

        let (wire, params) = AstEncoder::encode_cmd(&cmd).unwrap();

        let wire_str = String::from_utf8_lossy(&wire);
        assert!(wire_str.contains("SELECT"));
        assert!(wire_str.contains("users"));
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_recursive_cte_parenthesizes_set_op_base_term() {
        use qail_core::ast::{CTEDef, Expr, SetOp};

        let mut base = Qail::get("employees");
        base.columns.push(Expr::Named("id".to_string()));

        let mut second_base = Qail::get("contractors");
        second_base.columns.push(Expr::Named("id".to_string()));

        base.set_ops.push((SetOp::UnionAll, Box::new(second_base)));

        let mut recursive = Qail::get("tree");
        recursive.columns.push(Expr::Named("id".to_string()));

        let mut cmd = Qail::get("tree");
        cmd.action = Action::With;
        cmd.ctes = vec![CTEDef {
            name: "tree".to_string(),
            recursive: true,
            columns: vec!["id".to_string()],
            base_query: Box::new(base),
            recursive_query: Some(Box::new(recursive)),
            source_table: None,
        }];

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(params.is_empty());
        assert_eq!(
            sql,
            "WITH RECURSIVE tree(id) AS ((SELECT id FROM employees UNION ALL SELECT id FROM contractors) UNION ALL SELECT id FROM tree) SELECT * FROM tree"
        );
    }

    #[test]
    fn test_encode_recursive_cte_parenthesizes_limited_base_term() {
        use qail_core::ast::{CTEDef, Expr};

        let base = Qail::get("roots").columns(["id"]).limit(1);

        let mut recursive = Qail::get("tree");
        recursive.columns.push(Expr::Named("id".to_string()));

        let mut cmd = Qail::get("tree");
        cmd.action = Action::With;
        cmd.ctes = vec![CTEDef {
            name: "tree".to_string(),
            recursive: true,
            columns: vec!["id".to_string()],
            base_query: Box::new(base),
            recursive_query: Some(Box::new(recursive)),
            source_table: None,
        }];

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(params.is_empty());
        assert_eq!(
            sql,
            "WITH RECURSIVE tree(id) AS ((SELECT id FROM roots LIMIT 1) UNION ALL SELECT id FROM tree) SELECT * FROM tree"
        );
    }

    #[test]
    fn test_encode_set_op_parenthesizes_limited_left_operand() {
        use qail_core::ast::SetOp;

        let mut q1 = Qail::get("employees").columns(["id"]).limit(5);
        let q2 = Qail::get("contractors").columns(["id"]);

        q1.set_ops.push((SetOp::Union, Box::new(q2)));

        let (sql, params) = AstEncoder::encode_cmd_sql(&q1).unwrap();

        assert!(params.is_empty());
        assert_eq!(
            sql,
            "(SELECT id FROM employees LIMIT 5) UNION SELECT id FROM contractors"
        );
    }

    #[test]
    fn test_encode_set_op_parenthesizes_sorted_right_operand() {
        use qail_core::ast::SetOp;

        let mut q1 = Qail::get("employees").columns(["id"]);
        let q2 = Qail::get("contractors")
            .columns(["id"])
            .order_desc("id")
            .limit(5);

        q1.set_ops.push((SetOp::Union, Box::new(q2)));

        let (sql, params) = AstEncoder::encode_cmd_sql(&q1).unwrap();

        assert!(params.is_empty());
        assert_eq!(
            sql,
            "SELECT id FROM employees UNION (SELECT id FROM contractors ORDER BY id DESC LIMIT 5)"
        );
    }

    #[test]
    fn test_encode_fetch_first() {
        let cmd = Qail::get("employees").columns(["id"]).fetch_first(5);

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(params.is_empty());
        assert_eq!(sql, "SELECT id FROM employees FETCH FIRST 5 ROWS ONLY");
    }

    #[test]
    fn test_encode_set_op_parenthesizes_fetch_left_operand() {
        use qail_core::ast::SetOp;

        let mut q1 = Qail::get("employees").columns(["id"]).fetch_first(5);
        let q2 = Qail::get("contractors").columns(["id"]);

        q1.set_ops.push((SetOp::Union, Box::new(q2)));

        let (sql, params) = AstEncoder::encode_cmd_sql(&q1).unwrap();

        assert!(params.is_empty());
        assert_eq!(
            sql,
            "(SELECT id FROM employees FETCH FIRST 5 ROWS ONLY) UNION SELECT id FROM contractors"
        );
    }

    #[test]
    fn test_encode_cmd_with_binary_result_format() {
        let cmd = Qail::get("users").columns(["id"]);
        let (wire, params) = AstEncoder::encode_cmd_with_result_format(&cmd, 1).unwrap();

        assert!(params.is_empty());
        let parse_len = i32::from_be_bytes([wire[1], wire[2], wire[3], wire[4]]) as usize;
        let bind_start = 1 + parse_len;
        assert_eq!(wire[bind_start], b'B');
        let bind_len = i32::from_be_bytes([
            wire[bind_start + 1],
            wire[bind_start + 2],
            wire[bind_start + 3],
            wire[bind_start + 4],
        ]);
        let bind_content = &wire[bind_start + 5..bind_start + 1 + bind_len as usize];
        assert_eq!(&bind_content[6..10], &[0, 1, 0, 1]);
    }

    #[test]
    fn test_encode_cmd_rejects_nul_in_identifier() {
        let cmd = Qail::get("users\0");
        let err = AstEncoder::encode_cmd(&cmd).expect_err("NUL in SQL must be rejected");
        assert_eq!(err, EncodeError::NullByte);
    }

    #[test]
    fn test_encode_batch_simple_rejects_nul_in_identifier() {
        let cmd = Qail::get("users\0");
        let err = AstEncoder::encode_batch_simple(&[cmd]).expect_err("NUL in SQL must be rejected");
        assert_eq!(err, EncodeError::NullByte);
    }

    #[test]
    fn test_encode_session_set_escapes_value_and_name() {
        let cmd = Qail::session_set(
            "app.current_tenant_id",
            "t1'; SET app.is_super_admin = 'true'; SELECT 'ok",
        );

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(params.is_empty());
        assert_eq!(
            sql,
            "SET app.current_tenant_id = 't1''; SET app.is_super_admin = ''true''; SELECT ''ok'"
        );
    }

    #[test]
    fn test_encode_session_commands_escape_malformed_setting_names() {
        let set_cmd = Qail::session_set("statement_timeout; RESET ALL", "5000");
        let show_cmd = Qail::session_show("statement_timeout; RESET ALL");
        let reset_cmd = Qail::session_reset("statement_timeout; RESET ALL");

        assert_eq!(
            AstEncoder::encode_cmd_sql(&set_cmd).unwrap().0,
            "SET \"statement_timeout; RESET ALL\" = '5000'"
        );
        assert_eq!(
            AstEncoder::encode_cmd_sql(&show_cmd).unwrap().0,
            "SHOW \"statement_timeout; RESET ALL\""
        );
        assert_eq!(
            AstEncoder::encode_cmd_sql(&reset_cmd).unwrap().0,
            "RESET \"statement_timeout; RESET ALL\""
        );
    }

    #[test]
    fn test_encode_rejects_unsafe_dml_table_ref() {
        use qail_core::ast::Operator;

        let cmd = Qail::get("orders WHERE tenant_id <> 'tenant-a' --").filter(
            "tenant_id",
            Operator::Eq,
            "tenant-a",
        );

        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("unsafe table ref must fail");

        assert!(matches!(
            err,
            EncodeError::InvalidAst(message)
                if message.contains("unsafe identifier")
                    && message.contains("table")
        ));
    }

    #[test]
    fn test_encode_rejects_unsafe_dml_column_ref() {
        let cmd = Qail::get("orders").columns(["id, tenant_id FROM secrets --"]);

        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("unsafe column ref must fail");

        assert!(matches!(
            err,
            EncodeError::InvalidAst(message)
                if message.contains("unsafe identifier")
                    && message.contains("columns")
        ));
    }

    #[test]
    fn test_encode_rejects_unsafe_join_column_ref() {
        let cmd =
            Qail::get("orders").left_join("payments p", "orders.payment_id", "p.id OR TRUE --");

        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("unsafe join ref must fail");

        assert!(matches!(
            err,
            EncodeError::InvalidAst(message)
                if message.contains("unsafe identifier")
                    && message.contains("join.on")
        ));
    }

    #[test]
    fn test_encode_allows_safe_table_and_join_aliases() {
        let cmd = Qail::get("pg_catalog.pg_proc p")
            .columns(["p.oid", "p.proname"])
            .left_join("pg_catalog.pg_namespace n", "p.pronamespace", "n.oid");

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(params.is_empty());
        assert!(
            sql.contains(
                "FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid"
            ),
            "{sql}"
        );
    }

    #[test]
    fn test_encode_json_access_escapes_path_segment_quotes() {
        use qail_core::ast::Expr;

        let mut cmd = Qail::get("events");
        cmd.columns.push(Expr::JsonAccess {
            column: "payload".to_string(),
            path_segments: vec![("x') IS NOT NULL OR TRUE --".to_string(), true)],
            alias: None,
        });

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(params.is_empty());
        assert!(
            sql.contains("(payload->>'x'') IS NOT NULL OR TRUE --')"),
            "{sql}"
        );
        assert!(
            !sql.contains("(payload->>'x') IS NOT NULL OR TRUE --')"),
            "{sql}"
        );
    }

    #[test]
    fn test_encode_json_access_rejects_nul_path_segment() {
        use qail_core::ast::Expr;

        let mut cmd = Qail::get("events");
        cmd.columns.push(Expr::JsonAccess {
            column: "payload".to_string(),
            path_segments: vec![("bad\0path".to_string(), true)],
            alias: None,
        });

        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("NUL path segment must fail");
        assert_eq!(err, EncodeError::NullByte);
    }

    #[test]
    fn test_encode_select_with_filter() {
        use qail_core::ast::Operator;

        let cmd = Qail::get("users")
            .columns(["id", "name"])
            .filter("active", Operator::Eq, true);

        let (wire, params) = AstEncoder::encode_cmd(&cmd).unwrap();

        let wire_str = String::from_utf8_lossy(&wire);
        assert!(wire_str.contains("WHERE"));
        assert!(wire_str.contains("$1"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_encode_rejects_unresolved_positional_parameter() {
        use qail_core::ast::{Operator, Value};

        let cmd = Qail::get("users").filter("id", Operator::Eq, Value::Param(1));

        let err = AstEncoder::encode_cmd_sql(&cmd).unwrap_err();

        assert!(matches!(
            err,
            EncodeError::InvalidAst(message)
                if message.contains("unresolved positional parameter $1")
        ));
    }

    #[test]
    fn test_encode_rejects_unresolved_named_parameter() {
        use qail_core::ast::{Operator, Value};

        let cmd = Qail::get("users").filter(
            "email",
            Operator::Eq,
            Value::NamedParam("email".to_string()),
        );

        let err = AstEncoder::encode_cmd_sql(&cmd).unwrap_err();

        assert!(matches!(
            err,
            EncodeError::InvalidAst(message)
                if message.contains("unresolved named parameter :email")
        ));
    }

    #[test]
    fn test_encode_aggregate_filter_parameterizes_string_values() {
        use qail_core::ast::{AggregateFunc, Condition, Expr, Operator, Value};

        let mut cmd = Qail::get("events");
        cmd.columns.push(Expr::Aggregate {
            col: "*".to_string(),
            func: AggregateFunc::Count,
            distinct: false,
            filter: Some(vec![Condition {
                left: Expr::Named("direction".to_string()),
                op: Operator::Eq,
                value: Value::String("outbound' OR true --".to_string()),
                is_array_unnest: false,
            }]),
            alias: Some("outbound_count".to_string()),
        });

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("COUNT(*) FILTER (WHERE direction = $1) AS outbound_count"),
            "aggregate FILTER must use condition encoder: {sql}"
        );
        assert_eq!(
            params,
            vec![Some(b"outbound' OR true --".to_vec())],
            "string value must be carried as a bind parameter"
        );
    }

    #[test]
    fn test_encode_aggregate_filter_handles_is_null_without_rhs() {
        use qail_core::ast::{AggregateFunc, Condition, Expr, Operator, Value};

        let mut cmd = Qail::get("events");
        cmd.columns.push(Expr::Aggregate {
            col: "*".to_string(),
            func: AggregateFunc::Count,
            distinct: false,
            filter: Some(vec![Condition {
                left: Expr::Named("deleted_at".to_string()),
                op: Operator::IsNull,
                value: Value::Null,
                is_array_unnest: false,
            }]),
            alias: Some("deleted_count".to_string()),
        });

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("COUNT(*) FILTER (WHERE deleted_at IS NULL) AS deleted_count"),
            "aggregate FILTER must not render an extra NULL operand: {sql}"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_value_expr_subquery_shares_outer_params() {
        use qail_core::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Value};

        let subquery = Qail::get("plans")
            .columns(["id"])
            .filter("tier", Operator::Eq, "gold");
        let mut cmd = Qail::get("users");
        cmd.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("plan_id".to_string()),
                op: Operator::Eq,
                value: Value::Expr(Box::new(Expr::Subquery {
                    query: Box::new(subquery),
                    alias: None,
                })),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("plan_id = (SELECT id FROM plans WHERE tier = $1)"),
            "subquery expression value must share the outer parameter context: {sql}"
        );
        assert_eq!(params, vec![Some(b"gold".to_vec())]);
    }

    #[test]
    fn test_encode_literal_timestamp_escapes_quotes() {
        use qail_core::ast::{Expr, Value};

        let mut cmd = Qail::get("events");
        cmd.columns.push(Expr::Literal(Value::Timestamp(
            "2026-05-24 00:00:00'::timestamp); SELECT 1 --".to_string(),
        )));

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("'2026-05-24 00:00:00''::timestamp); SELECT 1 --'"),
            "timestamp literal must escape embedded quotes: {sql}"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_select_with_multiple_and_cages() {
        use qail_core::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Value};

        let mut cmd = Qail::get("orders").filter("id", Operator::Eq, "ord_1");
        cmd.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("tenant_id".to_string()),
                op: Operator::Eq,
                value: Value::String("tenant_a".to_string()),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("id = $1"),
            "first AND cage condition must be encoded: {}",
            sql
        );
        assert!(
            sql.contains("tenant_id = $2"),
            "second AND cage condition must be encoded: {}",
            sql
        );
        assert_eq!(
            params.len(),
            2,
            "both AND-cage filters must produce parameters"
        );
    }

    #[test]
    fn test_encode_select_with_multiple_and_cages_and_or_group() {
        use qail_core::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Value};

        let mut cmd = Qail::get("orders")
            .filter("status", Operator::Eq, "pending")
            .or_filter("customer_name", Operator::ILike, "%john%");
        cmd.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("tenant_id".to_string()),
                op: Operator::Eq,
                value: Value::String("tenant_a".to_string()),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("status = $1"),
            "primary AND condition must be encoded: {}",
            sql
        );
        assert!(
            sql.contains("tenant_id = $2"),
            "second AND condition must be encoded before OR group: {}",
            sql
        );
        assert!(
            sql.contains("(customer_name ILIKE $3)"),
            "OR group must still be encoded: {}",
            sql
        );
        assert_eq!(params.len(), 3, "expected 3 params for AND+AND+OR");
    }

    #[test]
    fn test_encode_select_with_or_filter() {
        use qail_core::ast::Operator;

        // or_filter should produce: WHERE is_active = $1 AND (topic ILIKE $2 OR question ILIKE $3)
        let cmd = Qail::get("kb")
            .columns(["topic", "question"])
            .eq("is_active", "true")
            .or_filter("topic", Operator::ILike, "%test%")
            .or_filter("question", Operator::ILike, "%test%");

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(sql.contains("WHERE"), "Should have WHERE: {}", sql);
        assert!(sql.contains("is_active"), "Should have AND filter: {}", sql);
        assert!(sql.contains("$1"), "Should have param $1: {}", sql);
        assert!(sql.contains("$2"), "Should have param $2: {}", sql);
        assert!(sql.contains("$3"), "Should have param $3: {}", sql);
        assert!(
            sql.contains(" OR "),
            "Should have OR between conditions: {}",
            sql
        );
        assert!(
            sql.contains("(topic"),
            "OR group should be wrapped in parens: {}",
            sql
        );
        assert_eq!(params.len(), 3, "3 params: is_active + 2 ILIKE patterns");
    }

    #[test]
    fn test_encode_select_or_filter_only() {
        use qail_core::ast::Operator;

        // Only OR conditions, no AND
        let cmd = Qail::get("products")
            .or_filter("name", Operator::ILike, "%coffee%")
            .or_filter("description", Operator::ILike, "%coffee%");

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(sql.contains("WHERE"), "Should have WHERE: {}", sql);
        assert!(
            sql.contains("(name ILIKE $1 OR description ILIKE $2)"),
            "Should have grouped OR: {}",
            sql
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_encode_update_with_or_filter() {
        use qail_core::ast::Operator;

        let cmd = Qail::set("kb")
            .set_value("archived", true)
            .or_filter("topic", Operator::ILike, "%test%")
            .or_filter("question", Operator::ILike, "%test%");

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(sql.contains("UPDATE kb SET archived = $1"), "{}", sql);
        assert!(
            sql.contains("WHERE (topic ILIKE $2 OR question ILIKE $3)"),
            "{}",
            sql
        );
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_encode_insert_conflict_update_with_filter_guard() {
        use qail_core::ast::{Expr, Operator};

        let cmd = Qail::add("orders")
            .set_value("id", "order-1")
            .set_value("status", "paid")
            .on_conflict_update(
                &["id"],
                &[("status", Expr::Named("EXCLUDED.status".into()))],
            )
            .filter("operator_id", Operator::Eq, "operator-1");

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains(
                "ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status WHERE operator_id = $3"
            ),
            "{}",
            sql
        );
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn test_encode_insert_select_source_query() {
        let mut cmd = Qail::add("archived_orders").columns(["id", "total"]);
        cmd.source_query = Some(Box::new(Qail::get("orders").columns(["id", "total"])));

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert_eq!(
            sql,
            "INSERT INTO archived_orders (id, total) SELECT id, total FROM orders"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_insert_rejects_mutating_source_query() {
        let mut cmd = Qail::add("archived_orders").columns(["id", "total"]);
        cmd.source_query = Some(Box::new(Qail::del("orders")));

        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("mutating source must fail");

        assert!(
            err.to_string()
                .contains("read-only SELECT query slot requires get/with action"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_update_from_tables() {
        let cmd = Qail::set("orders")
            .set_value("status", "paid")
            .update_from(["payments"])
            .eq("orders.payment_id", 42);

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains(
                "UPDATE orders SET status = $1 FROM payments WHERE orders.payment_id = $2"
            ),
            "{}",
            sql
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_encode_delete_using_tables() {
        let cmd = Qail::del("orders")
            .delete_using(["payments"])
            .eq("orders.payment_id", 42);

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("DELETE FROM orders USING payments WHERE orders.payment_id = $1"),
            "{}",
            sql
        );
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_encode_delete_with_or_filter() {
        use qail_core::ast::Operator;

        let cmd = Qail::del("kb")
            .or_filter("topic", Operator::ILike, "%test%")
            .or_filter("question", Operator::ILike, "%test%");

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("DELETE FROM kb WHERE (topic ILIKE $1 OR question ILIKE $2)"),
            "{}",
            sql
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_encode_export() {
        let cmd = Qail::export("users").columns(["id", "name"]);

        let (sql, _params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(sql.starts_with("COPY (SELECT"));
        assert!(sql.contains("FROM users"));
        assert!(sql.ends_with(") TO STDOUT"));
    }

    #[test]
    fn test_encode_export_with_filter() {
        use qail_core::ast::Operator;

        let cmd =
            Qail::export("users")
                .columns(["id", "name"])
                .filter("active", Operator::Eq, true);

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(sql.contains("COPY (SELECT"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("$1"));
        assert!(sql.ends_with(") TO STDOUT"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_encode_cte_single() {
        use qail_core::ast::Operator;

        let users_query =
            Qail::get("users")
                .columns(["id", "name"])
                .filter("active", Operator::Eq, true);

        let cmd = Qail::get("active_users").with("active_users", users_query);

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.starts_with("WITH active_users"),
            "SQL should start with WITH: {}",
            sql
        );
        assert!(
            sql.contains("AS (SELECT id, name FROM users"),
            "CTE should have subquery: {}",
            sql
        );
        assert!(
            sql.contains("FROM active_users"),
            "SQL should select from CTE: {}",
            sql
        );
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_encode_cte_rejects_mutating_base_query() {
        let cmd = Qail::get("recent_orders").with("recent_orders", Qail::del("orders"));

        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("mutating CTE must fail");

        assert!(
            err.to_string()
                .contains("read-only SELECT query slot requires get/with action"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_cte_multiple() {
        let users = Qail::get("users").columns(["id", "name"]);
        let orders = Qail::get("orders").columns(["id", "user_id", "total"]);

        let cmd = Qail::get("summary")
            .with("active_users", users)
            .with("recent_orders", orders);

        let (sql, _) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("active_users"),
            "SQL should have first CTE: {}",
            sql
        );
        assert!(
            sql.contains("recent_orders"),
            "SQL should have second CTE: {}",
            sql
        );
        assert!(
            sql.starts_with("WITH"),
            "SQL should start with WITH: {}",
            sql
        );
    }

    // ================================================================
    // Edge case tests — wire protocol safety
    // ================================================================

    #[test]
    fn test_encode_null_parameter() {
        use qail_core::ast::Operator;

        let cmd = Qail::get("users").filter("deleted_at", Operator::IsNull, true);

        let (wire, params) = AstEncoder::encode_cmd(&cmd).unwrap();
        let wire_str = String::from_utf8_lossy(&wire);
        // IS NULL should not create a parameter — it's a keyword
        assert!(wire_str.contains("IS NULL") || wire_str.contains("$1"));
        // At most 1 param (depends on filter encoding)
        assert!(params.len() <= 1);
    }

    #[test]
    fn test_encode_sql_injection_in_string_value() {
        use qail_core::ast::Operator;

        // Attempt SQL injection via a filter value
        let malicious = "'; DROP TABLE users; --";
        let cmd = Qail::get("users").filter("name", Operator::Eq, malicious);

        // Use SQL output (not wire bytes which include Bind params)
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        // The injection string must NOT appear in the SQL —
        // it must be in a parameter slot ($1)
        assert!(
            !sql.contains("DROP TABLE"),
            "SQL injection detected in SQL: {}",
            sql
        );
        assert!(sql.contains("$1"), "Should use parameterized query");
        assert_eq!(params.len(), 1, "Injection should be captured as a param");
    }

    #[test]
    fn test_encode_unicode_and_emoji() {
        use qail_core::ast::Operator;

        let cmd = Qail::get("products").filter("name", Operator::Eq, "日本語テスト 🚀");

        let (wire, params) = AstEncoder::encode_cmd(&cmd).unwrap();
        let wire_str = String::from_utf8_lossy(&wire);

        assert!(wire_str.contains("$1"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_encode_empty_string_filter() {
        use qail_core::ast::Operator;

        let cmd = Qail::get("users").filter("email", Operator::Eq, "");

        let (_wire, params) = AstEncoder::encode_cmd(&cmd).unwrap();
        assert_eq!(params.len(), 1, "Empty string should still produce a param");
    }

    #[test]
    fn test_encode_large_offset_limit() {
        let cmd = Qail::get("orders").limit(100_000).offset(999_999);

        let (sql, _) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert!(
            sql.contains("LIMIT 100000"),
            "Large limit should appear: {}",
            sql
        );
        assert!(
            sql.contains("OFFSET 999999"),
            "Large offset should appear: {}",
            sql
        );
    }

    #[test]
    fn test_encode_multi_filter_and_chain() {
        use qail_core::ast::Operator;

        let cmd = Qail::get("orders")
            .filter("status", Operator::Eq, "active")
            .filter("total", Operator::Gte, 100)
            .filter("created_at", Operator::Lte, "2026-01-01");

        let (wire, params) = AstEncoder::encode_cmd(&cmd).unwrap();
        let wire_str = String::from_utf8_lossy(&wire);

        // Should have 3 parameters: $1, $2, $3
        assert!(wire_str.contains("$1"));
        assert!(wire_str.contains("$2"));
        assert!(wire_str.contains("$3"));
        assert_eq!(params.len(), 3, "Should have 3 params for 3 filters");
    }

    #[test]
    fn test_encode_update_with_mixed_types() {
        let cmd = Qail::set("users")
            .set_value("name", "Alice")
            .set_value("age", 30)
            .set_value("active", true)
            .set_value("bio", "");

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(sql.contains("UPDATE"), "Should be UPDATE: {}", sql);
        assert_eq!(params.len(), 4, "Should have 4 params for 4 values");
    }

    // ================================================================
    // Gap analysis fix tests
    // ================================================================

    #[test]
    fn test_encode_function_expr() {
        let cmd = Qail::get("users").columns_expr(vec![qail_core::ast::Expr::FunctionCall {
            name: "NOW".to_string(),
            args: vec![],
            alias: None,
        }]);

        let (sql, _) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("NOW()"),
            "Function expr should emit NOW(), got: {}",
            sql
        );
        assert!(
            !sql.contains("SELECT *"),
            "Function expr should NOT emit *, got: {}",
            sql
        );
    }

    #[test]
    fn test_encode_def_expr() {
        use qail_core::ast::{Constraint, Expr};

        let def = Expr::Def {
            name: "email".to_string(),
            data_type: "text".to_string(),
            constraints: vec![Constraint::Unique],
        };

        let mut buf = bytes::BytesMut::with_capacity(128);
        super::values::encode_column_expr(&def, &mut buf).unwrap();
        let result = String::from_utf8_lossy(&buf).to_string();

        assert!(
            result.contains("email"),
            "Should have column name: {}",
            result
        );
        assert!(result.contains("TEXT"), "Should have type: {}", result);
        assert!(
            result.contains("UNIQUE"),
            "Should have constraint: {}",
            result
        );
    }

    #[test]
    fn test_encode_mod_expr() {
        use qail_core::ast::{Expr, ModKind};

        let mod_add = Expr::Mod {
            kind: ModKind::Add,
            col: Box::new(Expr::Named("email".to_string())),
        };

        let mut buf = bytes::BytesMut::with_capacity(128);
        super::values::encode_column_expr(&mod_add, &mut buf).unwrap();
        let result = String::from_utf8_lossy(&buf).to_string();

        assert!(
            result.contains("ADD COLUMN"),
            "Should have ADD COLUMN: {}",
            result
        );
        assert!(
            result.contains("email"),
            "Should have column name: {}",
            result
        );
    }

    #[test]
    fn test_encode_batch_cnt() {
        let cmd = Qail {
            action: qail_core::ast::Action::Cnt,
            table: "users".to_string(),
            ..Default::default()
        };

        let result = AstEncoder::encode_batch(&[cmd]);
        assert!(
            result.is_ok(),
            "Cnt should be supported in batch: {:?}",
            result.err()
        );

        let buf = result.unwrap();
        let wire_str = String::from_utf8_lossy(&buf);
        assert!(
            wire_str.contains("COUNT"),
            "Batch Cnt should produce COUNT: {}",
            wire_str
        );
    }

    #[test]
    fn test_encode_batch_export() {
        let cmd = Qail::export("users").columns(["id", "name"]);

        let result = AstEncoder::encode_batch(&[cmd]);
        assert!(
            result.is_ok(),
            "Export should be supported in batch: {:?}",
            result.err()
        );

        let buf = result.unwrap();
        let wire_str = String::from_utf8_lossy(&buf);
        assert!(
            wire_str.contains("COPY"),
            "Batch Export should produce COPY: {}",
            wire_str
        );
    }

    #[test]
    fn test_encode_merge_sql() {
        use qail_core::ast::{Expr, Operator};

        let cmd = Qail::merge_into("users")
            .target_alias("u")
            .using_table_as("staging_users", "s")
            .merge_on_column("u.id", Operator::Eq, "s.id")
            .when_matched_update(&[("name", Expr::Named("s.name".to_string()))])
            .when_not_matched_insert(
                &["id", "name"],
                &[
                    Expr::Named("s.id".to_string()),
                    Expr::Named("s.name".to_string()),
                ],
            );

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert!(params.is_empty());
        assert_eq!(
            sql,
            "MERGE INTO users AS u USING staging_users AS s ON u.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) VALUES (s.id, s.name)"
        );
    }

    #[test]
    fn test_encode_merge_with_cte_sql() {
        use qail_core::ast::{Expr, Operator};

        let source = Qail::get("staging_users").columns(["id", "name"]);
        let cmd = Qail::merge_into("users")
            .with("incoming", source)
            .using_table_as("incoming", "s")
            .merge_on_column("users.id", Operator::Eq, "s.id")
            .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert!(params.is_empty());
        assert_eq!(
            sql,
            "WITH incoming(id, name) AS (SELECT id, name FROM staging_users) \
             MERGE INTO users USING incoming AS s ON users.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name"
        );
    }

    #[test]
    fn test_encode_merge_rejects_invalid_action_shape() {
        use qail_core::ast::{Expr, MergeAction, MergeClause, MergeMatchKind, Operator};

        let mut cmd = Qail::merge_into("users")
            .using_table("staging_users")
            .merge_on_column("users.id", Operator::Eq, "staging_users.id");
        cmd.merge
            .as_mut()
            .expect("merge spec")
            .clauses
            .push(MergeClause {
                match_kind: MergeMatchKind::Matched,
                condition: vec![],
                action: MergeAction::Insert {
                    columns: vec!["id".to_string()],
                    values: vec![Expr::Named("staging_users.id".to_string())],
                },
            });

        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("invalid merge should fail");
        assert!(
            err.to_string().contains("WHEN MATCHED cannot INSERT"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_merge_rejects_mutating_source_query() {
        use qail_core::ast::{Expr, Operator};

        let source = Qail::del("staging_users");
        let cmd = Qail::merge_into("users")
            .using_query_as(source, "s")
            .merge_on_column("users.id", Operator::Eq, "s.id")
            .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);

        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("mutating source must fail");
        assert!(
            err.to_string()
                .contains("MERGE source query must be read-only SELECT"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_merge_rejects_mutating_source_cte() {
        use qail_core::ast::{Expr, Operator};

        let source = Qail::get("incoming").with("incoming", Qail::add("staging_users"));
        let cmd = Qail::merge_into("users")
            .using_query_as(source, "s")
            .merge_on_column("users.id", Operator::Eq, "s.id")
            .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);

        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("mutating CTE source must fail");
        assert!(
            err.to_string()
                .contains("MERGE source query must be read-only SELECT"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_merge_complex_expressions_and_params() {
        use qail_core::ast::{BinaryOp, Condition, Expr, Operator, Value};

        let cmd = Qail::merge_into("users")
            .target_alias("u")
            .using_table_as("staging_users", "s")
            .merge_on_condition(Condition {
                left: Expr::Cast {
                    expr: Box::new(Expr::JsonAccess {
                        column: "u.profile".to_string(),
                        path_segments: vec![("external_id".to_string(), true)],
                        alias: None,
                    }),
                    target_type: "integer".to_string(),
                    alias: None,
                },
                op: Operator::Eq,
                value: Value::Column("s.external_id".to_string()),
                is_array_unnest: false,
            })
            .when_matched_update_if(
                vec![
                    Condition {
                        left: Expr::JsonAccess {
                            column: "s.profile".to_string(),
                            path_segments: vec![("tier".to_string(), true)],
                            alias: None,
                        },
                        op: Operator::Eq,
                        value: Value::String("gold".to_string()),
                        is_array_unnest: false,
                    },
                    Condition {
                        left: Expr::Named("s.score".to_string()),
                        op: Operator::Gt,
                        value: Value::Expr(Box::new(Expr::Binary {
                            left: Box::new(Expr::Named("u.score".to_string())),
                            op: BinaryOp::Add,
                            right: Box::new(Expr::Literal(Value::Int(5))),
                            alias: None,
                        })),
                        is_array_unnest: false,
                    },
                ],
                &[
                    (
                        "name",
                        Expr::FunctionCall {
                            name: "coalesce".to_string(),
                            args: vec![
                                Expr::Named("s.name".to_string()),
                                Expr::Named("u.name".to_string()),
                            ],
                            alias: None,
                        },
                    ),
                    (
                        "score",
                        Expr::Binary {
                            left: Box::new(Expr::Named("s.score".to_string())),
                            op: BinaryOp::Add,
                            right: Box::new(Expr::Literal(Value::Int(1))),
                            alias: None,
                        },
                    ),
                    (
                        "tier",
                        Expr::JsonAccess {
                            column: "s.profile".to_string(),
                            path_segments: vec![("tier".to_string(), true)],
                            alias: None,
                        },
                    ),
                    (
                        "status",
                        Expr::Case {
                            when_clauses: vec![(
                                Condition {
                                    left: Expr::Cast {
                                        expr: Box::new(Expr::JsonAccess {
                                            column: "s.profile".to_string(),
                                            path_segments: vec![("active".to_string(), true)],
                                            alias: None,
                                        }),
                                        target_type: "integer".to_string(),
                                        alias: None,
                                    },
                                    op: Operator::Gt,
                                    value: Value::Int(0),
                                    is_array_unnest: false,
                                },
                                Box::new(Expr::Literal(Value::String("active".to_string()))),
                            )],
                            else_value: Some(Box::new(Expr::Literal(Value::String(
                                "archived".to_string(),
                            )))),
                            alias: None,
                        },
                    ),
                ],
            )
            .when_not_matched_insert_if(
                vec![Condition {
                    left: Expr::Cast {
                        expr: Box::new(Expr::Named("s.external_id".to_string())),
                        target_type: "integer".to_string(),
                        alias: None,
                    },
                    op: Operator::Gt,
                    value: Value::Int(0),
                    is_array_unnest: false,
                }],
                &["id", "name", "score", "tier", "status"],
                &[
                    Expr::Cast {
                        expr: Box::new(Expr::Named("s.external_id".to_string())),
                        target_type: "integer".to_string(),
                        alias: None,
                    },
                    Expr::FunctionCall {
                        name: "coalesce".to_string(),
                        args: vec![
                            Expr::Named("s.name".to_string()),
                            Expr::Literal(Value::String("unknown".to_string())),
                        ],
                        alias: None,
                    },
                    Expr::Binary {
                        left: Box::new(Expr::Named("s.score".to_string())),
                        op: BinaryOp::Add,
                        right: Box::new(Expr::Literal(Value::Int(1))),
                        alias: None,
                    },
                    Expr::JsonAccess {
                        column: "s.profile".to_string(),
                        path_segments: vec![("tier".to_string(), true)],
                        alias: None,
                    },
                    Expr::Literal(Value::String("new".to_string())),
                ],
            );

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(
            sql,
            "MERGE INTO users AS u USING staging_users AS s ON (u.profile->>'external_id')::integer = s.external_id \
             WHEN MATCHED AND (s.profile->>'tier') = $1 AND s.score > (u.score + 5) \
             THEN UPDATE SET name = COALESCE(s.name, u.name), score = (s.score + 1), tier = (s.profile->>'tier'), status = CASE WHEN (s.profile->>'active')::integer > 0 THEN 'active' ELSE 'archived' END \
             WHEN NOT MATCHED BY TARGET AND s.external_id::integer > $2 \
             THEN INSERT (id, name, score, tier, status) VALUES (s.external_id::integer, COALESCE(s.name, 'unknown'), (s.score + 1), (s.profile->>'tier'), 'new')"
        );
        assert_eq!(params, vec![Some(b"gold".to_vec()), Some(b"0".to_vec())]);
    }

    #[test]
    fn test_encode_batch_ddl_make() {
        use qail_core::ast::{Constraint, Expr};

        let cmd = Qail {
            action: qail_core::ast::Action::Make,
            table: "test_table".to_string(),
            columns: vec![
                Expr::Def {
                    name: "id".to_string(),
                    data_type: "serial".to_string(),
                    constraints: vec![Constraint::PrimaryKey],
                },
                Expr::Def {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
            ],
            ..Default::default()
        };

        let result = AstEncoder::encode_batch(&[cmd]);
        assert!(
            result.is_ok(),
            "Make (DDL) should be supported in batch: {:?}",
            result.err()
        );

        let buf = result.unwrap();
        let wire_str = String::from_utf8_lossy(&buf);
        assert!(
            wire_str.contains("CREATE TABLE"),
            "Batch Make should produce CREATE TABLE: {}",
            wire_str
        );
    }

    #[test]
    fn test_encode_batch_ddl_drop() {
        let cmd = Qail {
            action: qail_core::ast::Action::Drop,
            table: "test_table".to_string(),
            ..Default::default()
        };

        let result = AstEncoder::encode_batch(&[cmd]);
        assert!(
            result.is_ok(),
            "Drop (DDL) should be supported in batch: {:?}",
            result.err()
        );

        let buf = result.unwrap();
        let wire_str = String::from_utf8_lossy(&buf);
        assert!(
            wire_str.contains("DROP TABLE"),
            "Batch Drop should produce DROP TABLE: {}",
            wire_str
        );
    }

    #[test]
    fn test_encode_create_database() {
        let cmd = Qail::create_database("shadow_db");
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(sql, "CREATE DATABASE shadow_db");
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_drop_database() {
        let cmd = Qail::drop_database("shadow_db");
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(sql, "DROP DATABASE IF EXISTS shadow_db");
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_create_database_quotes_hyphenated_name() {
        let cmd = Qail::create_database("qail-engine-db_shadow");
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(sql, "CREATE DATABASE \"qail-engine-db_shadow\"");
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_drop_database_quotes_hyphenated_name() {
        let cmd = Qail::drop_database("qail-engine-db_shadow");
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(sql, "DROP DATABASE IF EXISTS \"qail-engine-db_shadow\"");
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_ddl_quotes_structured_identifiers() {
        use qail_core::ast::{Constraint, Expr, IndexDef};

        let make = Qail {
            action: Action::Make,
            table: "orders; DROP TABLE users; --".to_string(),
            columns: vec![Expr::Def {
                name: "status; DROP".to_string(),
                data_type: "str".to_string(),
                constraints: vec![Constraint::Check(vec![
                    "draft".to_string(),
                    "O'Brien".to_string(),
                ])],
            }],
            ..Default::default()
        };
        let (make_sql, params) = AstEncoder::encode_cmd_sql(&make).unwrap();
        assert!(params.is_empty());
        assert_eq!(
            make_sql,
            "CREATE TABLE \"orders; DROP TABLE users; --\" (\"status; DROP\" TEXT NOT NULL CHECK (\"status; DROP\" IN ('draft', 'O''Brien')))"
        );

        let drop = Qail {
            action: Action::Drop,
            table: "orders; DROP TABLE users; --".to_string(),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&drop).unwrap().0,
            "DROP TABLE IF EXISTS \"orders; DROP TABLE users; --\""
        );

        let index = Qail {
            action: Action::Index,
            index_def: Some(IndexDef {
                name: "idx; DROP INDEX x; --".to_string(),
                table: "orders; DROP TABLE users; --".to_string(),
                columns: vec!["tenant_id; DROP".to_string()],
                unique: false,
                index_type: None,
                where_clause: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&index).unwrap().0,
            "CREATE INDEX \"idx; DROP INDEX x; --\" ON \"orders; DROP TABLE users; --\" (\"tenant_id; DROP\")"
        );

        let enable_rls = Qail {
            action: Action::AlterEnableRls,
            table: "orders; DROP TABLE users; --".to_string(),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&enable_rls).unwrap().0,
            "ALTER TABLE \"orders; DROP TABLE users; --\" ENABLE ROW LEVEL SECURITY"
        );
    }

    #[test]
    fn test_encode_create_extension() {
        use qail_core::ast::Expr;

        let cmd = Qail {
            action: Action::CreateExtension,
            table: "uuid-ossp".to_string(),
            columns: vec![
                Expr::Named("SCHEMA public".to_string()),
                Expr::Named("VERSION '1.1'".to_string()),
            ],
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(
            sql,
            "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA public VERSION '1.1'"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_ddl_options_reject_invalid_fragments() {
        use qail_core::ast::Expr;

        let extension = Qail {
            action: Action::CreateExtension,
            table: "uuid-ossp\0".to_string(),
            columns: vec![
                Expr::Named("SCHEMA public; DROP TABLE users; --".to_string()),
                Expr::Named("VERSION '1.1; DROP SCHEMA public; --'".to_string()),
            ],
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&extension).unwrap().0,
            "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA \"public; DROP TABLE users; --\" VERSION '1.1; DROP SCHEMA public; --'"
        );

        let invalid_extension = Qail {
            action: Action::CreateExtension,
            table: "uuid-ossp".to_string(),
            columns: vec![
                Expr::Named("SCHEMA public".to_string()),
                Expr::Named("CASCADE; DROP TABLE users".to_string()),
            ],
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&invalid_extension)
            .expect_err("invalid extension option must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid extension option")),
            "unexpected error: {err}"
        );

        let sequence = Qail {
            action: Action::CreateSequence,
            table: "order_seq".to_string(),
            columns: vec![
                Expr::Named("start 1000".to_string()),
                Expr::Named("increment by -1".to_string()),
                Expr::Named("owned_by public.orders.id".to_string()),
            ],
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&sequence).unwrap().0,
            "CREATE SEQUENCE order_seq START WITH 1000 INCREMENT BY -1 OWNED BY public.orders.id"
        );

        let invalid_sequence = Qail {
            action: Action::CreateSequence,
            table: "order_seq".to_string(),
            columns: vec![
                Expr::Named("start 1000".to_string()),
                Expr::Named("cache 10; DROP TABLE users".to_string()),
            ],
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&invalid_sequence)
            .expect_err("invalid sequence option must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid sequence option")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_index_fragments_validate_method_and_predicate() {
        use qail_core::ast::IndexDef;

        let valid = Qail {
            action: Action::Index,
            index_def: Some(IndexDef {
                name: "idx_lower_email".to_string(),
                table: "users".to_string(),
                columns: vec!["lower(email)".to_string()],
                unique: false,
                index_type: Some("btree".to_string()),
                where_clause: Some("active = true".to_string()),
            }),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&valid).unwrap().0,
            "CREATE INDEX idx_lower_email ON users USING btree (lower(email)) WHERE active = true"
        );

        let hnsw = Qail {
            action: Action::Index,
            index_def: Some(IndexDef {
                name: "idx_docs_embedding".to_string(),
                table: "documents".to_string(),
                columns: vec!["embedding vector_l2_ops".to_string()],
                unique: false,
                index_type: Some("hnsw".to_string()),
                where_clause: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&hnsw).unwrap().0,
            "CREATE INDEX idx_docs_embedding ON documents USING hnsw (embedding vector_l2_ops)"
        );

        let ivfflat = Qail {
            action: Action::Index,
            index_def: Some(IndexDef {
                name: "idx_docs_embedding_cosine".to_string(),
                table: "documents".to_string(),
                columns: vec!["embedding vector_cosine_ops".to_string()],
                unique: false,
                index_type: Some("ivf-flat".to_string()),
                where_clause: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&ivfflat).unwrap().0,
            "CREATE INDEX idx_docs_embedding_cosine ON documents USING ivfflat (embedding vector_cosine_ops)"
        );

        let quoted_column = Qail {
            action: Action::Index,
            index_def: Some(IndexDef {
                name: "idx_bad".to_string(),
                table: "users".to_string(),
                columns: vec!["lower(email); DROP TABLE users; --".to_string()],
                unique: false,
                index_type: None,
                where_clause: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&quoted_column).unwrap().0,
            "CREATE INDEX idx_bad ON users (\"lower(email); DROP TABLE users; --\")"
        );

        let invalid_column = Qail {
            action: Action::Index,
            index_def: Some(IndexDef {
                name: "idx_bad".to_string(),
                table: "users".to_string(),
                columns: vec!["lower(email)\0".to_string()],
                unique: false,
                index_type: None,
                where_clause: None,
            }),
            ..Default::default()
        };
        let err =
            AstEncoder::encode_cmd_sql(&invalid_column).expect_err("nul index column must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid index column")),
            "unexpected error: {err}"
        );

        let invalid_method = Qail {
            action: Action::Index,
            index_def: Some(IndexDef {
                name: "idx_bad".to_string(),
                table: "users".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: Some("btree; DROP TABLE users".to_string()),
                where_clause: None,
            }),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&invalid_method)
            .expect_err("invalid index method must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid index method")),
            "unexpected error: {err}"
        );

        let invalid_predicate = Qail {
            action: Action::Index,
            index_def: Some(IndexDef {
                name: "idx_bad".to_string(),
                table: "users".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: Some("btree".to_string()),
                where_clause: Some("active = true; DROP TABLE users; --".to_string()),
            }),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&invalid_predicate)
            .expect_err("invalid index predicate must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid index predicate")),
            "unexpected error: {err}"
        );

        let invalid_nul_predicate = Qail {
            action: Action::Index,
            index_def: Some(IndexDef {
                name: "idx_bad".to_string(),
                table: "users".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: Some("btree".to_string()),
                where_clause: Some("active = true\0".to_string()),
            }),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&invalid_nul_predicate)
            .expect_err("nul index predicate must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid index predicate")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_foreign_key_reference_targets_are_sanitized() {
        use qail_core::ast::{Constraint, Expr};

        let cmd = Qail {
            action: Action::Make,
            table: "posts".to_string(),
            columns: vec![
                Expr::Def {
                    name: "user_id".to_string(),
                    data_type: "uuid".to_string(),
                    constraints: vec![Constraint::References(
                        "public.users(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED"
                            .to_string(),
                    )],
                },
                Expr::Def {
                    name: "unsafe_ref".to_string(),
                    data_type: "uuid".to_string(),
                    constraints: vec![Constraint::References(
                        "users(id); DROP TABLE users; --".to_string(),
                    )],
                },
            ],
            ..Default::default()
        };
        let sql = AstEncoder::encode_cmd_sql(&cmd).unwrap().0;
        assert!(sql.contains(
            "REFERENCES public.users(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED"
        ));
        assert!(sql.contains("REFERENCES \"users(id); DROP TABLE users; --\""));
        assert!(!sql.contains("REFERENCES REFERENCES"));
    }

    #[test]
    fn test_encode_column_expression_fragments_reject_invalid_fragments() {
        use qail_core::ast::{ColumnGeneration, Constraint, Expr};

        let safe = Qail {
            action: Action::Make,
            table: "events".to_string(),
            columns: vec![Expr::Def {
                name: "safe_note".to_string(),
                data_type: "str".to_string(),
                constraints: vec![Constraint::Default("'semi;inside'".to_string())],
            }],
            ..Default::default()
        };
        let sql = AstEncoder::encode_cmd_sql(&safe).unwrap().0;
        assert!(sql.contains("DEFAULT 'semi;inside'"));

        let unsafe_default = Qail {
            action: Action::Make,
            table: "events".to_string(),
            columns: vec![Expr::Def {
                name: "unsafe_default".to_string(),
                data_type: "int".to_string(),
                constraints: vec![Constraint::Default("0; DROP TABLE users; --".to_string())],
            }],
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&unsafe_default)
            .expect_err("unsafe default expression must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("column default expression")),
            "unexpected error: {err}"
        );

        let unsafe_check = Qail {
            action: Action::Make,
            table: "events".to_string(),
            columns: vec![Expr::Def {
                name: "unsafe_check".to_string(),
                data_type: "int".to_string(),
                constraints: vec![Constraint::Check(vec![
                    "unsafe_check > 0; DROP TABLE users; --".to_string(),
                ])],
            }],
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&unsafe_check)
            .expect_err("unsafe check expression must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("column check expression")),
            "unexpected error: {err}"
        );

        let unsafe_constraint_check = Qail {
            action: Action::Make,
            table: "events".to_string(),
            columns: vec![Expr::Def {
                name: "unsafe_check_constraint".to_string(),
                data_type: "int".to_string(),
                constraints: vec![Constraint::Check(vec![
                    "CONSTRAINT score_positive CHECK (unsafe_check_constraint > 0)\0".to_string(),
                ])],
            }],
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&unsafe_constraint_check)
            .expect_err("nul column check constraint must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("column check constraint")),
            "unexpected error: {err}"
        );

        let unsafe_generated = Qail {
            action: Action::Make,
            table: "events".to_string(),
            columns: vec![Expr::Def {
                name: "unsafe_generated".to_string(),
                data_type: "str".to_string(),
                constraints: vec![Constraint::Generated(ColumnGeneration::Stored(
                    "lower(safe_note); DROP TABLE users; --".to_string(),
                ))],
            }],
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&unsafe_generated)
            .expect_err("unsafe generated expression must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("generated column expression")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_column_data_type_fragments_are_sanitized() {
        use qail_core::ast::Expr;

        let unsafe_type = "text); DROP TABLE users; --";
        let make = Qail {
            action: Action::Make,
            table: "events".to_string(),
            columns: vec![
                Expr::Def {
                    name: "safe_custom".to_string(),
                    data_type: "public.citext".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "unsafe_type".to_string(),
                    data_type: unsafe_type.to_string(),
                    constraints: vec![],
                },
            ],
            ..Default::default()
        };
        let sql = AstEncoder::encode_cmd_sql(&make).unwrap().0;
        assert!(sql.contains("safe_custom public.citext NOT NULL"));
        assert!(sql.contains("unsafe_type TEXT NOT NULL"));
        assert!(!sql.contains("DROP TABLE"));

        let alter_add = Qail {
            action: Action::Alter,
            table: "events".to_string(),
            columns: vec![Expr::Def {
                name: "unsafe_type".to_string(),
                data_type: unsafe_type.to_string(),
                constraints: vec![],
            }],
            ..Default::default()
        };
        let sql = AstEncoder::encode_cmd_sql(&alter_add).unwrap().0;
        assert_eq!(
            sql,
            "ALTER TABLE events ADD COLUMN unsafe_type TEXT NOT NULL"
        );

        let alter_type = Qail {
            action: Action::AlterType,
            table: "events".to_string(),
            columns: vec![Expr::Def {
                name: "unsafe_type".to_string(),
                data_type: unsafe_type.to_string(),
                constraints: vec![],
            }],
            ..Default::default()
        };
        let sql = AstEncoder::encode_cmd_sql(&alter_type).unwrap().0;
        assert_eq!(sql, "ALTER TABLE events ALTER COLUMN unsafe_type TYPE TEXT");
    }

    #[test]
    fn test_encode_alter_columns_validate_shapes_and_multiple_actions() {
        use qail_core::ast::{Expr, Value};

        let multi_add = Qail {
            action: Action::Alter,
            table: "events".to_string(),
            columns: vec![
                Expr::Def {
                    name: "title".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "attempts".to_string(),
                    data_type: "int".to_string(),
                    constraints: vec![],
                },
            ],
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&multi_add).unwrap().0,
            "ALTER TABLE events ADD COLUMN title TEXT NOT NULL, ADD COLUMN attempts INT NOT NULL"
        );

        let invalid_add = Qail {
            action: Action::Alter,
            table: "events".to_string(),
            columns: vec![Expr::Named("not_a_definition".to_string())],
            ..Default::default()
        };
        let err =
            AstEncoder::encode_cmd_sql(&invalid_add).expect_err("invalid alter add must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("ALTER ADD columns")),
            "unexpected error: {err}"
        );

        let invalid_drop = Qail {
            action: Action::AlterDrop,
            table: "events".to_string(),
            columns: vec![Expr::Literal(Value::Int(1))],
            ..Default::default()
        };
        let err =
            AstEncoder::encode_cmd_sql(&invalid_drop).expect_err("invalid alter drop must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("ALTER DROP columns")),
            "unexpected error: {err}"
        );

        let invalid_type = Qail {
            action: Action::AlterType,
            table: "events".to_string(),
            columns: vec![Expr::Named("not_a_definition".to_string())],
            ..Default::default()
        };
        let err =
            AstEncoder::encode_cmd_sql(&invalid_type).expect_err("invalid alter type must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("ALTER TYPE columns")),
            "unexpected error: {err}"
        );

        let invalid_rename = Qail {
            action: Action::Mod,
            table: "events".to_string(),
            columns: vec![Expr::Named("old_name new_name".to_string())],
            ..Default::default()
        };
        let err =
            AstEncoder::encode_cmd_sql(&invalid_rename).expect_err("invalid rename must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("old -> new")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_alter_set_default_rejects_invalid_fragments() {
        use qail_core::ast::Expr;

        let safe = Qail {
            action: Action::AlterSetDefault,
            table: "events".to_string(),
            columns: vec![Expr::Named("note".to_string())],
            payload: Some("'semi;inside'".to_string()),
            ..Default::default()
        };
        let sql = AstEncoder::encode_cmd_sql(&safe).unwrap().0;
        assert_eq!(
            sql,
            "ALTER TABLE events ALTER COLUMN note SET DEFAULT 'semi;inside'"
        );

        let unsafe_default = Qail {
            action: Action::AlterSetDefault,
            table: "events".to_string(),
            columns: vec![Expr::Named("score".to_string())],
            payload: Some("0; DROP TABLE events; --".to_string()),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&unsafe_default)
            .expect_err("unsafe default expression must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid default expression")),
            "unexpected error: {err}"
        );

        let unsafe_nul_default = Qail {
            action: Action::AlterSetDefault,
            table: "events".to_string(),
            columns: vec![Expr::Named("score".to_string())],
            payload: Some("0\0".to_string()),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&unsafe_nul_default)
            .expect_err("nul default expression must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid default expression")),
            "unexpected error: {err}"
        );

        let missing_column = Qail {
            action: Action::AlterDropDefault,
            table: "events".to_string(),
            columns: vec![],
            ..Default::default()
        };
        let err =
            AstEncoder::encode_cmd_sql(&missing_column).expect_err("missing column must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("ALTER DROP DEFAULT")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_savepoint_commands() {
        let savepoint = Qail {
            action: Action::Savepoint,
            savepoint_name: Some("sp1".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&savepoint).unwrap();
        assert_eq!(sql, "SAVEPOINT \"sp1\"");
        assert!(params.is_empty());

        let rollback = Qail {
            action: Action::RollbackToSavepoint,
            savepoint_name: Some("sp1".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&rollback).unwrap();
        assert_eq!(sql, "ROLLBACK TO SAVEPOINT \"sp1\"");
        assert!(params.is_empty());

        let release = Qail {
            action: Action::ReleaseSavepoint,
            savepoint_name: Some("sp1".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&release).unwrap();
        assert_eq!(sql, "RELEASE SAVEPOINT \"sp1\"");
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_savepoint_quotes_untrusted_name() {
        let savepoint = Qail {
            action: Action::Savepoint,
            savepoint_name: Some("sp\"; DROP TABLE users; --\0tail".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&savepoint).unwrap();
        assert_eq!(sql, "SAVEPOINT \"sp\"\"; DROP TABLE users; --tail\"");
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_create_view_with_payload() {
        let cmd = Qail {
            action: Action::CreateView,
            table: "active_users".to_string(),
            payload: Some("SELECT id FROM users WHERE active = true".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(
            sql,
            "CREATE VIEW active_users AS SELECT id FROM users WHERE active = true"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_view_payload_fragments_reject_invalid_fragments() {
        let safe = Qail {
            action: Action::CreateView,
            table: "notes_view".to_string(),
            payload: Some("SELECT 'semi;inside' AS note".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&safe).unwrap();
        assert_eq!(
            sql,
            "CREATE VIEW notes_view AS SELECT 'semi;inside' AS note"
        );
        assert!(params.is_empty());

        let unsafe_view = Qail {
            action: Action::CreateView,
            table: "active_users".to_string(),
            payload: Some("SELECT id FROM users; DROP TABLE users; --".to_string()),
            ..Default::default()
        };
        let err =
            AstEncoder::encode_cmd_sql(&unsafe_view).expect_err("unsafe view query must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("view query")),
            "unexpected error: {err}"
        );

        let unsafe_materialized = Qail {
            action: Action::CreateMaterializedView,
            table: "booking_stats".to_string(),
            payload: Some("SELECT COUNT(*) FROM bookings; DROP TABLE bookings; --".to_string()),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&unsafe_materialized)
            .expect_err("unsafe materialized view query must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("materialized view query")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_comment_on_targets_are_sanitized() {
        use qail_core::ast::Expr;

        let safe = Qail {
            action: Action::CommentOn,
            table: "FUNCTION public.cleanup(numeric(10,2), text)".to_string(),
            columns: vec![Expr::Named("cleanup helper".to_string())],
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&safe).unwrap();
        assert_eq!(
            sql,
            "COMMENT ON FUNCTION public.cleanup(numeric(10,2), text) IS 'cleanup helper'"
        );
        assert!(params.is_empty());

        let unsafe_target = Qail {
            action: Action::CommentOn,
            table: "TABLE users; DROP TABLE users; --".to_string(),
            columns: vec![Expr::Named("owner's note\0".to_string())],
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&unsafe_target).unwrap();
        assert_eq!(
            sql,
            "COMMENT ON TABLE \"TABLE users; DROP TABLE users; --\" IS 'owner''s note'"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_create_view_rejects_mutating_source_query() {
        let cmd = Qail {
            action: Action::CreateView,
            table: "active_users".to_string(),
            source_query: Some(Box::new(Qail::del("users"))),
            ..Default::default()
        };

        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("mutating view source must fail");

        assert!(
            err.to_string()
                .contains("read-only SELECT query slot requires get/with action"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_create_materialized_view_with_payload() {
        let cmd = Qail {
            action: Action::CreateMaterializedView,
            table: "booking_stats".to_string(),
            payload: Some("SELECT COUNT(*) AS total FROM bookings".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(
            sql,
            "CREATE MATERIALIZED VIEW booking_stats AS SELECT COUNT(*) AS total FROM bookings"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_drop_trigger_table_target() {
        let cmd = Qail {
            action: Action::DropTrigger,
            table: "users.trg_users_updated".to_string(),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(sql, "DROP TRIGGER IF EXISTS trg_users_updated ON users");
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_grant() {
        use qail_core::ast::Expr;

        let cmd = Qail {
            action: Action::Grant,
            table: "users".to_string(),
            columns: vec![
                Expr::Named("SELECT".to_string()),
                Expr::Named("INSERT".to_string()),
            ],
            payload: Some("app_role".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(sql, "GRANT SELECT, INSERT ON users TO app_role");
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_grant_rejects_invalid_privileges() {
        use qail_core::ast::Expr;

        let grant = Qail {
            action: Action::Grant,
            table: "users".to_string(),
            columns: vec![
                Expr::Named("all privileges".to_string()),
                Expr::Named("temp".to_string()),
            ],
            payload: Some("app_role".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&grant).unwrap();
        assert_eq!(sql, "GRANT ALL PRIVILEGES, TEMPORARY ON users TO app_role");
        assert!(params.is_empty());

        let mixed_invalid = Qail {
            action: Action::Grant,
            table: "users".to_string(),
            columns: vec![
                Expr::Named("SELECT".to_string()),
                Expr::Named("INSERT; DROP TABLE users; --".to_string()),
            ],
            payload: Some("app_role".to_string()),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&mixed_invalid)
            .expect_err("mixed invalid privileges must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid privilege")),
            "unexpected error: {err}"
        );

        let revoke = Qail {
            action: Action::Revoke,
            table: "users".to_string(),
            columns: vec![Expr::Named("UPDATE; DROP TABLE users; --".to_string())],
            payload: Some("app_role".to_string()),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&revoke).expect_err("invalid privileges must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid privilege")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_encode_revoke() {
        use qail_core::ast::Expr;

        let cmd = Qail {
            action: Action::Revoke,
            table: "users".to_string(),
            columns: vec![Expr::Named("UPDATE".to_string())],
            payload: Some("app_role".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(sql, "REVOKE UPDATE ON users FROM app_role");
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_create_policy() {
        use qail_core::ast::{BinaryOp, Expr, Value};
        use qail_core::migrate::policy::RlsPolicy;

        let policy = RlsPolicy::create("users_isolation", "users")
            .for_all()
            .using(Expr::Binary {
                left: Box::new(Expr::Named("tenant_id".to_string())),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Cast {
                    expr: Box::new(Expr::FunctionCall {
                        name: "current_setting".to_string(),
                        args: vec![Expr::Literal(Value::String(
                            "app.current_tenant_id".to_string(),
                        ))],
                        alias: None,
                    }),
                    target_type: "uuid".to_string(),
                    alias: None,
                }),
                alias: None,
            });

        let cmd = Qail {
            action: Action::CreatePolicy,
            policy_def: Some(policy),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(
            sql,
            "CREATE POLICY users_isolation ON users FOR ALL USING ((tenant_id = CURRENT_SETTING('app.current_tenant_id')::uuid))"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_policy_expression_fragments_reject_invalid_fragments() {
        use qail_core::ast::Expr;
        use qail_core::migrate::policy::RlsPolicy;

        let policy = RlsPolicy::create("unsafe_policy", "users")
            .for_all()
            .using(Expr::Named(
                "tenant_id = current_setting('app.tenant')::uuid; DROP TABLE users; --".to_string(),
            ))
            .with_check(Expr::Named("note = 'semi;inside'".to_string()));

        let cmd = Qail {
            action: Action::CreatePolicy,
            policy_def: Some(policy),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("unsafe policy expression must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("policy expression")),
            "unexpected error: {err}"
        );

        let nul_policy = RlsPolicy::create("nul_policy", "users")
            .for_all()
            .using(Expr::Named(
                "tenant_id = current_setting('app.tenant')::uuid\0".to_string(),
            ));
        let cmd = Qail {
            action: Action::CreatePolicy,
            policy_def: Some(nul_policy),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("nul policy expression must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("policy expression")),
            "unexpected error: {err}"
        );

        let safe_policy = RlsPolicy::create("safe_policy", "users")
            .for_all()
            .with_check(Expr::Named("note = 'semi;inside'".to_string()));
        let cmd = Qail {
            action: Action::CreatePolicy,
            policy_def: Some(safe_policy),
            ..Default::default()
        };
        let sql = AstEncoder::encode_cmd_sql(&cmd).unwrap().0;
        assert!(sql.contains("WITH CHECK (note = 'semi;inside')"));
    }

    #[test]
    fn test_encode_drop_policy() {
        let cmd = Qail {
            action: Action::DropPolicy,
            table: "users".to_string(),
            payload: Some("users_isolation".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(sql, "DROP POLICY IF EXISTS users_isolation ON users");
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_create_function_with_args() {
        let cmd = Qail {
            action: Action::CreateFunction,
            function_def: Some(qail_core::ast::FunctionDef {
                name: "sum_one".to_string(),
                args: vec!["v int".to_string()],
                returns: "int".to_string(),
                body: "BEGIN RETURN v + 1; END;".to_string(),
                language: Some("plpgsql".to_string()),
                volatility: None,
            }),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(
            sql,
            "CREATE OR REPLACE FUNCTION sum_one(v int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN v + 1; END; $$"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_function_definition_rejects_invalid_fragments() {
        let invalid_arg = Qail {
            action: Action::CreateFunction,
            function_def: Some(qail_core::ast::FunctionDef {
                name: "notice_boom".to_string(),
                args: vec!["v int); DROP TABLE users; --".to_string()],
                returns: "int".to_string(),
                body: "BEGIN RETURN; END;".to_string(),
                language: Some("plpgsql".to_string()),
                volatility: None,
            }),
            ..Default::default()
        };
        let err =
            AstEncoder::encode_cmd_sql(&invalid_arg).expect_err("invalid function arg must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid function argument")),
            "unexpected error: {err}"
        );

        let invalid_return = Qail {
            action: Action::CreateFunction,
            function_def: Some(qail_core::ast::FunctionDef {
                name: "notice_boom".to_string(),
                args: vec![
                    "amount numeric(10,2)".to_string(),
                    "OUT result text".to_string(),
                ],
                returns: "int; DROP TABLE users".to_string(),
                body: "BEGIN RETURN; END;".to_string(),
                language: Some("plpgsql".to_string()),
                volatility: None,
            }),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&invalid_return)
            .expect_err("invalid function return type must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid function return type")),
            "unexpected error: {err}"
        );

        let invalid_volatility = Qail {
            action: Action::CreateFunction,
            function_def: Some(qail_core::ast::FunctionDef {
                name: "notice_boom".to_string(),
                args: vec![
                    "amount numeric(10,2)".to_string(),
                    "OUT result text".to_string(),
                ],
                returns: "int".to_string(),
                body: "BEGIN RETURN; END;".to_string(),
                language: Some("plpgsql".to_string()),
                volatility: Some("stable; DROP TABLE users".to_string()),
            }),
            ..Default::default()
        };
        let err = AstEncoder::encode_cmd_sql(&invalid_volatility)
            .expect_err("invalid function volatility must fail");
        assert!(
            matches!(&err, EncodeError::InvalidAst(message) if message.contains("invalid function volatility")),
            "unexpected error: {err}"
        );

        let valid_drop = Qail {
            action: Action::DropFunction,
            payload: Some("public.cleanup(numeric(10,2), text)".to_string()),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&valid_drop).unwrap().0,
            "DROP FUNCTION IF EXISTS public.cleanup(numeric(10,2), text)"
        );

        let malicious_drop = Qail {
            action: Action::DropFunction,
            payload: Some("public.cleanup(int); DROP TABLE users; --".to_string()),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&malicious_drop).unwrap().0,
            "DROP FUNCTION IF EXISTS public.\"cleanup(int); DROP TABLE users; --\""
        );
    }

    #[test]
    fn test_encode_procedural_bodies_use_non_colliding_dollar_quotes() {
        let do_cmd = Qail {
            action: Action::Do,
            table: "plpgsql".to_string(),
            payload: Some("BEGIN RAISE NOTICE $$boom$$; END;".to_string()),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&do_cmd).unwrap().0,
            "DO $qail_body_1$ BEGIN RAISE NOTICE $$boom$$; END; $qail_body_1$ LANGUAGE plpgsql"
        );

        let function_cmd = Qail {
            action: Action::CreateFunction,
            function_def: Some(qail_core::ast::FunctionDef {
                name: "notice_boom".to_string(),
                args: vec![],
                returns: "void".to_string(),
                body: "BEGIN RAISE NOTICE $$boom$$; END;".to_string(),
                language: Some("plpgsql".to_string()),
                volatility: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&function_cmd).unwrap().0,
            "CREATE OR REPLACE FUNCTION notice_boom() RETURNS void LANGUAGE plpgsql AS $qail_body_1$ BEGIN RAISE NOTICE $$boom$$; END; $qail_body_1$"
        );
    }

    #[test]
    fn test_encode_call_targets_are_sanitized() {
        let valid = Qail {
            action: Action::Call,
            table: "maintenance.refresh()".to_string(),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&valid).unwrap().0,
            "CALL maintenance.refresh()"
        );

        let malicious = Qail {
            action: Action::Call,
            table: "refresh(); DROP TABLE users; --".to_string(),
            ..Default::default()
        };
        assert_eq!(
            AstEncoder::encode_cmd_sql(&malicious).unwrap().0,
            "CALL \"refresh(); DROP TABLE users; --\""
        );
    }

    #[test]
    fn test_encode_batch_mixed_dml_ddl() {
        use qail_core::ast::Expr;

        let ddl = Qail {
            action: qail_core::ast::Action::Make,
            table: "new_table".to_string(),
            columns: vec![Expr::Def {
                name: "id".to_string(),
                data_type: "serial".to_string(),
                constraints: vec![],
            }],
            ..Default::default()
        };
        let dml = Qail::get("new_table").columns(["id"]);

        let result = AstEncoder::encode_batch(&[ddl, dml]);
        assert!(
            result.is_ok(),
            "Mixed DDL+DML batch should work: {:?}",
            result.err()
        );

        let buf = result.unwrap();
        let wire_str = String::from_utf8_lossy(&buf);
        assert!(
            wire_str.contains("CREATE TABLE"),
            "Should contain DDL: {}",
            wire_str
        );
        assert!(
            wire_str.contains("SELECT"),
            "Should contain DML: {}",
            wire_str
        );
    }

    #[test]
    fn test_encode_batch_with_binary_result_format() {
        let cmd = Qail::get("users").columns(["id"]);
        let wire = AstEncoder::encode_batch_with_result_format(&[cmd], 1).unwrap();

        let parse_len = i32::from_be_bytes([wire[1], wire[2], wire[3], wire[4]]) as usize;
        let bind_start = 1 + parse_len;
        assert_eq!(wire[bind_start], b'B');
        let bind_len = i32::from_be_bytes([
            wire[bind_start + 1],
            wire[bind_start + 2],
            wire[bind_start + 3],
            wire[bind_start + 4],
        ]);
        let bind_content = &wire[bind_start + 5..bind_start + 1 + bind_len as usize];
        assert_eq!(&bind_content[6..10], &[0, 1, 0, 1]);
    }

    #[test]
    fn test_regular_get_encoding() {
        let cmd = Qail::get("users").columns(["id", "name"]);
        let (sql, _) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert!(sql.contains("SELECT"), "Normal GET should produce SELECT");
        assert!(
            sql.contains("FROM users"),
            "Normal GET should have FROM clause"
        );
    }
}
