//! Shared SQL statement kind classification for analyzer modules.
//!
//! This classifier is quote/parenthesis aware and checks top-level SQL tokens,
//! so it avoids the broad `contains(...)` heuristics used previously.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlStmtKind {
    Select,
    Insert,
    Update,
    Delete,
    Merge,
    Truncate,
    Copy,
    Lock,
    Create,
    Alter,
    Comment,
    Grant,
    Revoke,
    Analyze,
    Vacuum,
    Reindex,
    Cluster,
    Refresh,
    Drop,
}

impl SqlStmtKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Merge => "MERGE",
            Self::Truncate => "TRUNCATE",
            Self::Copy => "COPY",
            Self::Lock => "LOCK",
            Self::Create => "CREATE",
            Self::Alter => "ALTER",
            Self::Comment => "COMMENT",
            Self::Grant => "GRANT",
            Self::Revoke => "REVOKE",
            Self::Analyze => "ANALYZE",
            Self::Vacuum => "VACUUM",
            Self::Reindex => "REINDEX",
            Self::Cluster => "CLUSTER",
            Self::Refresh => "REFRESH",
            Self::Drop => "DROP",
        }
    }
}

/// Classify SQL statement kind from raw SQL text.
pub(crate) fn classify_sql_kind(sql: &str) -> Option<SqlStmtKind> {
    let normalized = normalize_whitespace(sql);
    if normalized.is_empty() {
        return None;
    }

    let starts_with_dml = statement_starts_with_keyword(&normalized, "SELECT")
        || statement_starts_with_keyword(&normalized, "INSERT")
        || statement_starts_with_keyword(&normalized, "UPDATE")
        || statement_starts_with_keyword(&normalized, "DELETE")
        || statement_starts_with_keyword(&normalized, "MERGE")
        || statement_starts_with_keyword(&normalized, "TRUNCATE")
        || statement_starts_with_keyword(&normalized, "COPY")
        || statement_starts_with_keyword(&normalized, "LOCK")
        || statement_starts_with_keyword(&normalized, "CREATE")
        || statement_starts_with_keyword(&normalized, "ALTER")
        || statement_starts_with_keyword(&normalized, "COMMENT")
        || statement_starts_with_keyword(&normalized, "GRANT")
        || statement_starts_with_keyword(&normalized, "REVOKE")
        || statement_starts_with_keyword(&normalized, "ANALYZE")
        || statement_starts_with_keyword(&normalized, "VACUUM")
        || statement_starts_with_keyword(&normalized, "REINDEX")
        || statement_starts_with_keyword(&normalized, "CLUSTER")
        || statement_starts_with_keyword(&normalized, "REFRESH")
        || statement_starts_with_keyword(&normalized, "DROP");
    let starts_with_wrapper = statement_starts_with_keyword(&normalized, "WITH")
        || statement_starts_with_keyword(&normalized, "EXPLAIN");
    if !starts_with_dml && !starts_with_wrapper {
        return None;
    }

    let mut candidates = Vec::new();
    if let Some(pos) = find_keyword_top_level_from(&normalized, "SELECT", 0) {
        candidates.push((pos, SqlStmtKind::Select));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "INSERT", 0) {
        candidates.push((pos, SqlStmtKind::Insert));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "UPDATE", 0) {
        candidates.push((pos, SqlStmtKind::Update));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "DELETE", 0) {
        candidates.push((pos, SqlStmtKind::Delete));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "MERGE", 0) {
        candidates.push((pos, SqlStmtKind::Merge));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "TRUNCATE", 0) {
        candidates.push((pos, SqlStmtKind::Truncate));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "COPY", 0) {
        candidates.push((pos, SqlStmtKind::Copy));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "LOCK", 0) {
        candidates.push((pos, SqlStmtKind::Lock));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "CREATE", 0) {
        candidates.push((pos, SqlStmtKind::Create));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "ALTER", 0) {
        candidates.push((pos, SqlStmtKind::Alter));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "COMMENT", 0) {
        candidates.push((pos, SqlStmtKind::Comment));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "GRANT", 0) {
        candidates.push((pos, SqlStmtKind::Grant));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "REVOKE", 0) {
        candidates.push((pos, SqlStmtKind::Revoke));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "ANALYZE", 0) {
        candidates.push((pos, SqlStmtKind::Analyze));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "VACUUM", 0) {
        candidates.push((pos, SqlStmtKind::Vacuum));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "REINDEX", 0) {
        candidates.push((pos, SqlStmtKind::Reindex));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "CLUSTER", 0) {
        candidates.push((pos, SqlStmtKind::Cluster));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "REFRESH", 0) {
        candidates.push((pos, SqlStmtKind::Refresh));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "DROP", 0) {
        candidates.push((pos, SqlStmtKind::Drop));
    }

    let (_, kind) = candidates.into_iter().min_by_key(|(pos, _)| *pos)?;
    match kind {
        SqlStmtKind::Select => find_keyword_top_level_from(&normalized, "FROM", 0)
            .is_some()
            .then_some(SqlStmtKind::Select),
        SqlStmtKind::Insert => {
            let insert_pos = find_keyword_top_level_from(&normalized, "INSERT", 0)?;
            find_keyword_top_level_from(&normalized, "INTO", insert_pos + "INSERT".len())
                .is_some()
                .then_some(SqlStmtKind::Insert)
        }
        SqlStmtKind::Update => {
            let update_pos = find_keyword_top_level_from(&normalized, "UPDATE", 0)?;
            find_keyword_top_level_from(&normalized, "SET", update_pos + "UPDATE".len())
                .is_some()
                .then_some(SqlStmtKind::Update)
        }
        SqlStmtKind::Delete => {
            let delete_pos = find_keyword_top_level_from(&normalized, "DELETE", 0)?;
            find_keyword_top_level_from(&normalized, "FROM", delete_pos + "DELETE".len())
                .is_some()
                .then_some(SqlStmtKind::Delete)
        }
        SqlStmtKind::Merge => {
            let merge_pos = find_keyword_top_level_from(&normalized, "MERGE", 0)?;
            let into_pos =
                find_keyword_top_level_from(&normalized, "INTO", merge_pos + "MERGE".len())?;
            find_keyword_top_level_from(&normalized, "USING", into_pos + "INTO".len())
                .is_some()
                .then_some(SqlStmtKind::Merge)
        }
        SqlStmtKind::Truncate => Some(SqlStmtKind::Truncate),
        SqlStmtKind::Copy => Some(SqlStmtKind::Copy),
        SqlStmtKind::Lock => {
            let lock_pos = find_keyword_top_level_from(&normalized, "LOCK", 0)?;
            find_keyword_top_level_from(&normalized, "TABLE", lock_pos + "LOCK".len())
                .is_some()
                .then_some(SqlStmtKind::Lock)
        }
        SqlStmtKind::Create => Some(SqlStmtKind::Create),
        SqlStmtKind::Alter => [
            "TABLE",
            "VIEW",
            "MATERIALIZED VIEW",
            "POLICY",
            "PUBLICATION",
            "TRIGGER",
        ]
        .into_iter()
        .any(|keyword| find_keyword_top_level_from(&normalized, keyword, 0).is_some())
        .then_some(SqlStmtKind::Alter),
        SqlStmtKind::Comment => find_keyword_top_level_from(&normalized, "ON", 0)
            .is_some()
            .then_some(SqlStmtKind::Comment),
        SqlStmtKind::Grant => find_keyword_top_level_from(&normalized, "ON", 0)
            .is_some()
            .then_some(SqlStmtKind::Grant),
        SqlStmtKind::Revoke => find_keyword_top_level_from(&normalized, "ON", 0)
            .is_some()
            .then_some(SqlStmtKind::Revoke),
        SqlStmtKind::Analyze
        | SqlStmtKind::Vacuum
        | SqlStmtKind::Reindex
        | SqlStmtKind::Cluster
        | SqlStmtKind::Refresh
        | SqlStmtKind::Drop => Some(kind),
    }
}

fn normalize_whitespace(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn statement_starts_with_keyword(sql: &str, keyword: &str) -> bool {
    find_keyword_top_level_from(sql, keyword, 0) == Some(0)
}

fn find_keyword_top_level_from(text: &str, keyword: &str, min_idx: usize) -> Option<usize> {
    if keyword.is_empty() {
        return None;
    }

    let bytes = text.as_bytes();
    let upper = bytes
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let kw = keyword
        .as_bytes()
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();

    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;
    let mut i = 0usize;

    while i < bytes.len() {
        let b = bytes[i];

        if let Some(q) = in_quote {
            if b == q {
                if matches!(q, b'\'' | b'"') && bytes.get(i + 1).copied() == Some(q) {
                    i += 2;
                    continue;
                }
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => {
                in_quote = Some(b);
                i += 1;
                continue;
            }
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }

        if depth == 0
            && i >= min_idx
            && upper
                .get(i..i.saturating_add(kw.len()))
                .is_some_and(|slice| slice == kw)
        {
            let before_ok = if i == 0 {
                true
            } else {
                !is_ident_byte(upper[i - 1])
            };
            let after = i + kw.len();
            let after_ok = if after >= upper.len() {
                true
            } else {
                !is_ident_byte(upper[after])
            };

            if before_ok && after_ok {
                return Some(i);
            }
        }

        i += 1;
    }

    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_basic_dml() {
        assert_eq!(
            classify_sql_kind("SELECT id FROM users"),
            Some(SqlStmtKind::Select)
        );
        assert_eq!(
            classify_sql_kind("INSERT INTO users (id) VALUES ($1)"),
            Some(SqlStmtKind::Insert)
        );
        assert_eq!(
            classify_sql_kind("UPDATE users SET active = true"),
            Some(SqlStmtKind::Update)
        );
        assert_eq!(
            classify_sql_kind("DELETE FROM users WHERE id = $1"),
            Some(SqlStmtKind::Delete)
        );
    }

    #[test]
    fn classifies_cte_by_outer_statement() {
        let cte_select = "WITH x AS (SELECT id FROM users) SELECT * FROM x";
        let cte_delete =
            "WITH x AS (SELECT id FROM users) DELETE FROM users WHERE id IN (SELECT id FROM x)";

        assert_eq!(classify_sql_kind(cte_select), Some(SqlStmtKind::Select));
        assert_eq!(classify_sql_kind(cte_delete), Some(SqlStmtKind::Delete));
    }

    #[test]
    fn ignores_keywords_inside_strings() {
        let sql = "UPDATE users SET note = 'DELETE FROM x', active = true";
        assert_eq!(classify_sql_kind(sql), Some(SqlStmtKind::Update));
    }

    #[test]
    fn rejects_sql_keywords_that_do_not_start_a_statement() {
        assert_eq!(classify_sql_kind("debug SELECT id FROM users"), None);
        assert_eq!(classify_sql_kind("message: DELETE FROM sessions"), None);
    }

    #[test]
    fn classifies_postgres_merge() {
        let sql = "MERGE INTO orders USING staging_orders ON orders.id = staging_orders.id WHEN MATCHED THEN UPDATE SET status = staging_orders.status";

        assert_eq!(
            classify_sql_kind(sql).map(|kind| kind.as_str()),
            Some("MERGE")
        );
    }

    #[test]
    fn classifies_table_touching_utility_statements() {
        assert_eq!(
            classify_sql_kind("TRUNCATE TABLE users"),
            Some(SqlStmtKind::Truncate)
        );
        assert_eq!(
            classify_sql_kind("COPY users (email) FROM STDIN"),
            Some(SqlStmtKind::Copy)
        );
        assert_eq!(
            classify_sql_kind("LOCK TABLE users IN ACCESS EXCLUSIVE MODE"),
            Some(SqlStmtKind::Lock)
        );
    }

    #[test]
    fn classifies_postgres_schema_utility_statements() {
        assert_eq!(
            classify_sql_kind("CREATE INDEX users_email_idx ON users (email)"),
            Some(SqlStmtKind::Create)
        );
        assert_eq!(
            classify_sql_kind("ALTER TABLE users DROP COLUMN email"),
            Some(SqlStmtKind::Alter)
        );
        assert_eq!(
            classify_sql_kind("ALTER VIEW active_users RENAME TO users_active"),
            Some(SqlStmtKind::Alter)
        );
        assert_eq!(
            classify_sql_kind(
                "ALTER POLICY tenant_users ON users USING (tenant_id = current_setting('app.tenant_id')::uuid)"
            ),
            Some(SqlStmtKind::Alter)
        );
        assert_eq!(
            classify_sql_kind("COMMENT ON COLUMN users.email IS 'legacy'"),
            Some(SqlStmtKind::Comment)
        );
        assert_eq!(
            classify_sql_kind("GRANT SELECT (email) ON TABLE users TO app"),
            Some(SqlStmtKind::Grant)
        );
        assert_eq!(
            classify_sql_kind("REVOKE UPDATE (email) ON users FROM app"),
            Some(SqlStmtKind::Revoke)
        );
        assert_eq!(
            classify_sql_kind("ANALYZE users (email)"),
            Some(SqlStmtKind::Analyze)
        );
        assert_eq!(
            classify_sql_kind("VACUUM (VERBOSE, ANALYZE) users"),
            Some(SqlStmtKind::Vacuum)
        );
        assert_eq!(
            classify_sql_kind("REINDEX TABLE users"),
            Some(SqlStmtKind::Reindex)
        );
        assert_eq!(
            classify_sql_kind("CLUSTER users USING users_email_idx"),
            Some(SqlStmtKind::Cluster)
        );
        assert_eq!(
            classify_sql_kind("REFRESH MATERIALIZED VIEW active_users"),
            Some(SqlStmtKind::Refresh)
        );
        assert_eq!(
            classify_sql_kind("DROP TABLE IF EXISTS users"),
            Some(SqlStmtKind::Drop)
        );
    }
}
