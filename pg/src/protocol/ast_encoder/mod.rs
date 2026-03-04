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

        // Raw SQL pass-through: write the SQL verbatim (no AST encoding)
        if cmd.is_raw_sql() {
            sql_buf.extend_from_slice(cmd.table.as_bytes());
            return Ok(());
        }

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
            Action::Listen => ddl::encode_listen(cmd, sql_buf),
            Action::Unlisten => ddl::encode_unlisten(cmd, sql_buf),
            Action::Notify => ddl::encode_notify(cmd, sql_buf),
            _ => return Err(EncodeError::UnsupportedAction(cmd.action)),
        }
        Ok(())
    }

    /// Encode a Qail to SQL string + params (for prepared statement caching).
    pub fn encode_cmd_sql(cmd: &Qail) -> EncodeSqlResult {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        // Raw SQL pass-through: write the SQL verbatim (no AST encoding)
        if cmd.is_raw_sql() {
            sql_buf.extend_from_slice(cmd.table.as_bytes());
            let sql = String::from_utf8_lossy(&sql_buf).to_string();
            return Ok((sql, params));
        }

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
            Action::Listen => ddl::encode_listen(cmd, &mut sql_buf),
            Action::Unlisten => ddl::encode_unlisten(cmd, &mut sql_buf),
            Action::Notify => ddl::encode_notify(cmd, &mut sql_buf),
            _ => return Err(EncodeError::UnsupportedAction(cmd.action)),
        }

        let sql = String::from_utf8_lossy(&sql_buf).to_string();
        Ok((sql, params))
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
    fn test_encode_cmd_rejects_raw_sql_with_nul() {
        let cmd = Qail::raw_sql("SELECT 1\0; SELECT 2");
        let err = AstEncoder::encode_cmd(&cmd).expect_err("NUL in SQL must be rejected");
        assert_eq!(err, EncodeError::NullByte);
    }

    #[test]
    fn test_encode_batch_simple_rejects_raw_sql_with_nul() {
        let cmd = Qail::raw_sql("SELECT 1\0; SELECT 2");
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
    fn test_encode_raw_expr() {
        let cmd =
            Qail::get("users").columns_expr(vec![qail_core::ast::Expr::Raw("NOW()".to_string())]);

        let (sql, _) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert!(
            sql.contains("NOW()"),
            "Raw expr should emit NOW(), got: {}",
            sql
        );
        assert!(
            !sql.contains("SELECT *"),
            "Raw expr should NOT emit *, got: {}",
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

    // ================================================================
    // Raw SQL pass-through tests
    // ================================================================

    #[test]
    fn test_raw_sql_passes_through_verbatim() {
        let sql = "SELECT id, name FROM users WHERE name ILIKE '%test%'";
        let cmd = Qail::raw_sql(sql);

        let (encoded_sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

        assert_eq!(encoded_sql, sql, "Raw SQL must pass through verbatim");
        assert!(params.is_empty(), "Raw SQL should have no params");
    }

    #[test]
    fn test_raw_sql_complex_query() {
        let sql = "SELECT k.id, k.title FROM ai_knowledge_base k WHERE EXISTS (SELECT 1 FROM unnest(k.keywords) kw WHERE kw ILIKE $1)";
        let cmd = Qail::raw_sql(sql);

        let (encoded_sql, _) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(encoded_sql, sql);
    }

    #[test]
    fn test_raw_sql_with_cte() {
        let sql = "WITH ranked AS (SELECT *, ROW_NUMBER() OVER () AS rn FROM orders) SELECT * FROM ranked WHERE rn <= 10";
        let cmd = Qail::raw_sql(sql);

        let (encoded_sql, _) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(encoded_sql, sql);
    }

    #[test]
    fn test_regular_get_not_detected_as_raw_sql() {
        let cmd = Qail::get("users").columns(["id", "name"]);
        assert!(
            !cmd.is_raw_sql(),
            "Normal GET should not be detected as raw SQL"
        );

        let (sql, _) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert!(sql.contains("SELECT"), "Normal GET should produce SELECT");
        assert!(
            sql.contains("FROM users"),
            "Normal GET should have FROM clause"
        );
    }

    #[test]
    fn test_raw_sql_wire_encoding() {
        let sql = "SELECT 1";
        let cmd = Qail::raw_sql(sql);

        let (wire, params) = AstEncoder::encode_cmd(&cmd).unwrap();
        let wire_str = String::from_utf8_lossy(&wire);
        assert!(
            wire_str.contains("SELECT 1"),
            "Wire encoding should contain raw SQL: {}",
            wire_str
        );
        assert!(params.is_empty());
    }
}
