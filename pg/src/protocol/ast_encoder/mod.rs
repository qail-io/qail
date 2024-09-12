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
pub(crate) mod dml;  // pub(crate) for internal use in driver
mod helpers;
mod values;

use bytes::BytesMut;
use qail_core::ast::{Action, Qail};

/// AST-native encoder that skips SQL string generation.
pub struct AstEncoder;

impl AstEncoder {
    /// Encode a Qail directly to Extended Query protocol bytes.
    ///
    /// Returns (wire_bytes, extracted_params_as_bytes)
    pub fn encode_cmd(cmd: &Qail) -> (BytesMut, Vec<Option<Vec<u8>>>) {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        match cmd.action {
            Action::Get | Action::With => dml::encode_select(cmd, &mut sql_buf, &mut params),
            Action::Add => dml::encode_insert(cmd, &mut sql_buf, &mut params),
            Action::Set => dml::encode_update(cmd, &mut sql_buf, &mut params),
            Action::Del => dml::encode_delete(cmd, &mut sql_buf, &mut params),
            Action::Export => dml::encode_export(cmd, &mut sql_buf, &mut params),
            Action::Make => ddl::encode_make(cmd, &mut sql_buf),
            Action::Index => ddl::encode_index(cmd, &mut sql_buf),
            Action::Drop => ddl::encode_drop_table(cmd, &mut sql_buf),
            Action::DropIndex => ddl::encode_drop_index(cmd, &mut sql_buf),
            Action::Alter => ddl::encode_alter_add_column(cmd, &mut sql_buf),
            Action::AlterDrop => ddl::encode_alter_drop_column(cmd, &mut sql_buf),
            Action::AlterType => ddl::encode_alter_column_type(cmd, &mut sql_buf),
            Action::CreateView => ddl::encode_create_view(cmd, &mut sql_buf, &mut params),
            Action::DropView => ddl::encode_drop_view(cmd, &mut sql_buf),
            _ => panic!(
                "Unsupported action {:?} in AST-native encoder. Use legacy encoder for DDL.",
                cmd.action
            ),
        }

        let sql_bytes = sql_buf.freeze();
        let wire = batch::build_extended_query(&sql_bytes, &params);

        (wire, params)
    }

    /// Encode a Qail using CALLER'S BUFFERS (ZERO-ALLOC).
    ///
    /// Clears and reuses the provided buffers to avoid allocations.
    /// Returns wire protocol bytes ready to send.
    #[inline]
    pub fn encode_cmd_reuse(
        cmd: &Qail,
        sql_buf: &mut BytesMut,
        params: &mut Vec<Option<Vec<u8>>>,
    ) -> BytesMut {
        // Clear buffers (but keep capacity!)
        sql_buf.clear();
        params.clear();

        match cmd.action {
            Action::Get | Action::With => dml::encode_select(cmd, sql_buf, params),
            Action::Add => dml::encode_insert(cmd, sql_buf, params),
            Action::Set => dml::encode_update(cmd, sql_buf, params),
            Action::Del => dml::encode_delete(cmd, sql_buf, params),
            Action::Export => dml::encode_export(cmd, sql_buf, params),
            Action::Make => ddl::encode_make(cmd, sql_buf),
            Action::Index => ddl::encode_index(cmd, sql_buf),
            Action::Drop => ddl::encode_drop_table(cmd, sql_buf),
            Action::DropIndex => ddl::encode_drop_index(cmd, sql_buf),
            Action::Alter => ddl::encode_alter_add_column(cmd, sql_buf),
            Action::AlterDrop => ddl::encode_alter_drop_column(cmd, sql_buf),
            Action::AlterType => ddl::encode_alter_column_type(cmd, sql_buf),
            Action::CreateView => ddl::encode_create_view(cmd, sql_buf, params),
            Action::DropView => ddl::encode_drop_view(cmd, sql_buf),
            _ => panic!(
                "Unsupported action {:?} in AST-native encoder.",
                cmd.action
            ),
        }

        // Build wire protocol (reuses internal allocation in batch module)
        batch::build_extended_query(sql_buf, params)
    }

    /// Encode a Qail to SQL string + params (for prepared statement caching).
    pub fn encode_cmd_sql(cmd: &Qail) -> (String, Vec<Option<Vec<u8>>>) {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        match cmd.action {
            Action::Get | Action::With => dml::encode_select(cmd, &mut sql_buf, &mut params),
            Action::Add => dml::encode_insert(cmd, &mut sql_buf, &mut params),
            Action::Set => dml::encode_update(cmd, &mut sql_buf, &mut params),
            Action::Del => dml::encode_delete(cmd, &mut sql_buf, &mut params),
            Action::Export => dml::encode_export(cmd, &mut sql_buf, &mut params),
            Action::Make => ddl::encode_make(cmd, &mut sql_buf),
            Action::Index => ddl::encode_index(cmd, &mut sql_buf),
            _ => panic!("Unsupported action {:?} in AST-native encoder.", cmd.action),
        }

        let sql = String::from_utf8_lossy(&sql_buf).to_string();
        (sql, params)
    }

    /// Extract ONLY params from a Qail (for reusing cached SQL template).
    #[inline]
    pub fn encode_cmd_params_only(cmd: &Qail) -> Vec<Option<Vec<u8>>> {
        let mut sql_buf = BytesMut::with_capacity(256);
        let mut params: Vec<Option<Vec<u8>>> = Vec::new();

        match cmd.action {
            Action::Get => dml::encode_select(cmd, &mut sql_buf, &mut params),
            Action::Add => dml::encode_insert(cmd, &mut sql_buf, &mut params),
            Action::Set => dml::encode_update(cmd, &mut sql_buf, &mut params),
            Action::Del => dml::encode_delete(cmd, &mut sql_buf, &mut params),
            _ => {}
        }

        params
    }

    /// Generate just SQL bytes for a SELECT statement.
    pub fn encode_select_sql(
        cmd: &Qail,
        buf: &mut BytesMut,
        params: &mut Vec<Option<Vec<u8>>>,
    ) {
        dml::encode_select(cmd, buf, params);
    }

    /// Encode multiple Qails as a pipeline batch.
    pub fn encode_batch(cmds: &[Qail]) -> BytesMut {
        batch::encode_batch(cmds)
    }

    /// Encode multiple Qails using Simple Query Protocol.
    #[inline]
    pub fn encode_batch_simple(cmds: &[Qail]) -> BytesMut {
        batch::encode_batch_simple(cmds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_select() {
        let cmd = Qail::get("users").columns(["id", "name"]);

        let (wire, params) = AstEncoder::encode_cmd(&cmd);

        let wire_str = String::from_utf8_lossy(&wire);
        assert!(wire_str.contains("SELECT"));
        assert!(wire_str.contains("users"));
        assert!(params.is_empty());
    }

    #[test]
    fn test_encode_select_with_filter() {
        use qail_core::ast::Operator;

        let cmd = Qail::get("users")
            .columns(["id", "name"])
            .filter("active", Operator::Eq, true);

        let (wire, params) = AstEncoder::encode_cmd(&cmd);

        let wire_str = String::from_utf8_lossy(&wire);
        assert!(wire_str.contains("WHERE"));
        assert!(wire_str.contains("$1"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_encode_export() {
        let cmd = Qail::export("users").columns(["id", "name"]);

        let (sql, _params) = AstEncoder::encode_cmd_sql(&cmd);

        assert!(sql.starts_with("COPY (SELECT"));
        assert!(sql.contains("FROM users"));
        assert!(sql.ends_with(") TO STDOUT"));
    }

    #[test]
    fn test_encode_export_with_filter() {
        use qail_core::ast::Operator;

        let cmd = Qail::export("users")
            .columns(["id", "name"])
            .filter("active", Operator::Eq, true);

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd);

        assert!(sql.contains("COPY (SELECT"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("$1"));
        assert!(sql.ends_with(") TO STDOUT"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_encode_cte_single() {
        use qail_core::ast::Operator;

        let users_query = Qail::get("users")
            .columns(["id", "name"])
            .filter("active", Operator::Eq, true);

        let cmd = Qail::get("active_users").with("active_users", users_query);

        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd);

        assert!(sql.starts_with("WITH active_users"), "SQL should start with WITH: {}", sql);
        assert!(sql.contains("AS (SELECT id, name FROM users"), "CTE should have subquery: {}", sql);
        assert!(sql.contains("FROM active_users"), "SQL should select from CTE: {}", sql);
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_encode_cte_multiple() {
        let users = Qail::get("users").columns(["id", "name"]);
        let orders = Qail::get("orders").columns(["id", "user_id", "total"]);

        let cmd = Qail::get("summary")
            .with("active_users", users)
            .with("recent_orders", orders);

        let (sql, _) = AstEncoder::encode_cmd_sql(&cmd);

        assert!(sql.contains("active_users"), "SQL should have first CTE: {}", sql);
        assert!(sql.contains("recent_orders"), "SQL should have second CTE: {}", sql);
        assert!(sql.starts_with("WITH"), "SQL should start with WITH: {}", sql);
    }
}
