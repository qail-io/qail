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
                // COUNT: clone AST, replace columns with COUNT(*), delegate to SELECT
                let mut count_cmd = cmd.clone();
                count_cmd.action = Action::Get;
                count_cmd.columns = vec![qail_core::ast::Expr::Aggregate {
                    col: "*".to_string(),
                    func: qail_core::ast::AggregateFunc::Count,
                    distinct: false,
                    filter: None,
                    alias: None,
                }];
                dml::encode_select(&count_cmd, sql_buf, params)?;
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
            Action::Export => {
                dml::encode_export(cmd, sql_buf, params)?;
            }
            Action::Make => ddl::encode_make(cmd, sql_buf),
            Action::Index => ddl::encode_index(cmd, sql_buf),
            Action::Drop => ddl::encode_drop_table(cmd, sql_buf),
            Action::DropIndex => ddl::encode_drop_index(cmd, sql_buf),
            Action::Alter => ddl::encode_alter_add_column(cmd, sql_buf),
            Action::AlterDrop => ddl::encode_alter_drop_column(cmd, sql_buf),
            Action::AlterType => ddl::encode_alter_column_type(cmd, sql_buf),
            Action::Mod => ddl::encode_rename_column(cmd, sql_buf),
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
            Action::CreateExtension => ddl::encode_create_extension(cmd, sql_buf),
            Action::DropExtension => ddl::encode_drop_extension(cmd, sql_buf),
            Action::CommentOn => ddl::encode_comment_on(cmd, sql_buf),
            Action::CreateSequence => ddl::encode_create_sequence(cmd, sql_buf),
            Action::DropSequence => ddl::encode_drop_sequence(cmd, sql_buf),
            Action::CreateEnum => ddl::encode_create_enum(cmd, sql_buf),
            Action::DropEnum => ddl::encode_drop_enum(cmd, sql_buf),
            Action::AlterEnumAddValue => ddl::encode_alter_enum_add_value(cmd, sql_buf),
            Action::AlterSetNotNull => ddl::encode_alter_set_not_null(cmd, sql_buf),
            Action::AlterDropNotNull => ddl::encode_alter_drop_not_null(cmd, sql_buf),
            Action::AlterSetDefault => ddl::encode_alter_set_default(cmd, sql_buf),
            Action::AlterDropDefault => ddl::encode_alter_drop_default(cmd, sql_buf),
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
                let mut count_cmd = cmd.clone();
                count_cmd.action = Action::Get;
                count_cmd.columns = vec![qail_core::ast::Expr::Aggregate {
                    col: "*".to_string(),
                    func: qail_core::ast::AggregateFunc::Count,
                    distinct: false,
                    filter: None,
                    alias: None,
                }];
                dml::encode_select(&count_cmd, &mut sql_buf, &mut params)?;
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
            Action::Export => {
                dml::encode_export(cmd, &mut sql_buf, &mut params)?;
            }
            Action::Make => ddl::encode_make(cmd, &mut sql_buf),
            Action::Index => ddl::encode_index(cmd, &mut sql_buf),
            Action::Drop => ddl::encode_drop_table(cmd, &mut sql_buf),
            Action::DropIndex => ddl::encode_drop_index(cmd, &mut sql_buf),
            Action::Alter => ddl::encode_alter_add_column(cmd, &mut sql_buf),
            Action::AlterDrop => ddl::encode_alter_drop_column(cmd, &mut sql_buf),
            Action::AlterType => ddl::encode_alter_column_type(cmd, &mut sql_buf),
            Action::Mod => ddl::encode_rename_column(cmd, &mut sql_buf),
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
            Action::CreateExtension => ddl::encode_create_extension(cmd, &mut sql_buf),
            Action::DropExtension => ddl::encode_drop_extension(cmd, &mut sql_buf),
            Action::CommentOn => ddl::encode_comment_on(cmd, &mut sql_buf),
            Action::CreateSequence => ddl::encode_create_sequence(cmd, &mut sql_buf),
            Action::DropSequence => ddl::encode_drop_sequence(cmd, &mut sql_buf),
            Action::CreateEnum => ddl::encode_create_enum(cmd, &mut sql_buf),
            Action::DropEnum => ddl::encode_drop_enum(cmd, &mut sql_buf),
            Action::AlterEnumAddValue => ddl::encode_alter_enum_add_value(cmd, &mut sql_buf),
            Action::AlterSetNotNull => ddl::encode_alter_set_not_null(cmd, &mut sql_buf),
            Action::AlterDropNotNull => ddl::encode_alter_drop_not_null(cmd, &mut sql_buf),
            Action::AlterSetDefault => ddl::encode_alter_set_default(cmd, &mut sql_buf),
            Action::AlterDropDefault => ddl::encode_alter_drop_default(cmd, &mut sql_buf),
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
    fn test_encode_select() {
        let cmd = Qail::get("users").columns(["id", "name"]);

        let (wire, params) = AstEncoder::encode_cmd(&cmd).unwrap();

        let wire_str = String::from_utf8_lossy(&wire);
        assert!(wire_str.contains("SELECT"));
        assert!(wire_str.contains("users"));
        assert!(params.is_empty());
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
        super::values::encode_column_expr(&def, &mut buf);
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
        super::values::encode_column_expr(&mod_add, &mut buf);
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
    fn test_encode_savepoint_commands() {
        let savepoint = Qail {
            action: Action::Savepoint,
            savepoint_name: Some("sp1".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&savepoint).unwrap();
        assert_eq!(sql, "SAVEPOINT sp1");
        assert!(params.is_empty());

        let rollback = Qail {
            action: Action::RollbackToSavepoint,
            savepoint_name: Some("sp1".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&rollback).unwrap();
        assert_eq!(sql, "ROLLBACK TO SAVEPOINT sp1");
        assert!(params.is_empty());

        let release = Qail {
            action: Action::ReleaseSavepoint,
            savepoint_name: Some("sp1".to_string()),
            ..Default::default()
        };
        let (sql, params) = AstEncoder::encode_cmd_sql(&release).unwrap();
        assert_eq!(sql, "RELEASE SAVEPOINT sp1");
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
