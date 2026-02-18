//! Branch SQL helpers for Data Virtualization
//!
//! Generates SQL for branch metadata management:
//! - Session variable injection (`app.branch_id`)
//! - DDL for internal `_qail_branches` + `_qail_branch_rows` tables
//! - Branch CRUD operations
//! - Merge logic (apply overlay → main tables)

/// Safely escape a SQL string literal value.
///
/// Strips NUL bytes, escapes single quotes and backslashes, then wraps in
/// single quotes. This is resistant to backslash-escaping attacks even
/// when `standard_conforming_strings = off`.
pub fn escape_literal(val: &str) -> String {
    let clean = val
        .replace('\0', "")     // Strip NUL bytes (C-level truncation)
        .replace('\\', "\\\\") // Escape backslashes FIRST
        .replace('\'', "''");  // Then escape single quotes
    format!("'{}'", clean)
}

/// SQL to set the branch context on a connection session.
pub fn branch_context_sql(branch_name: &str) -> String {
    format!(
        "SET LOCAL app.branch_id = {};",
        escape_literal(branch_name)
    )
}

/// SQL to reset (clear) the branch context.
pub fn branch_reset_sql() -> &'static str {
    "RESET app.branch_id;"
}

/// DDL to create the internal branch metadata tables.
/// Idempotent — uses IF NOT EXISTS.
pub fn create_branch_tables_sql() -> &'static str {
    r#"
CREATE TABLE IF NOT EXISTS _qail_branches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    parent_branch_id UUID REFERENCES _qail_branches(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    merged_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS _qail_branch_rows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    branch_id UUID NOT NULL REFERENCES _qail_branches(id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    row_pk TEXT NOT NULL,
    operation TEXT NOT NULL,
    row_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_branch_rows_lookup
    ON _qail_branch_rows (branch_id, table_name, row_pk);

CREATE INDEX IF NOT EXISTS idx_branch_rows_branch
    ON _qail_branch_rows (branch_id);
"#
}

/// SQL to create a new branch.
///
/// Branch names are validated to prevent SQL injection.
pub fn create_branch_sql(name: &str, parent: Option<&str>) -> String {
    let safe_name = escape_literal(name);
    match parent {
        Some(parent_name) => {
            let safe_parent = escape_literal(parent_name);
            format!(
                "INSERT INTO _qail_branches (name, parent_branch_id) \
                 VALUES ({}, (SELECT id FROM _qail_branches WHERE name = {})) \
                 RETURNING id, name, created_at;",
                safe_name, safe_parent
            )
        }
        None => format!(
            "INSERT INTO _qail_branches (name) VALUES ({}) RETURNING id, name, created_at;",
            safe_name
        ),
    }
}

/// SQL to list all branches.
pub fn list_branches_sql() -> &'static str {
    "SELECT id, name, parent_branch_id, created_at, merged_at, status \
     FROM _qail_branches ORDER BY created_at DESC;"
}

/// SQL to soft-delete a branch.
pub fn delete_branch_sql(name: &str) -> String {
    let safe_name = escape_literal(name);
    format!(
        "UPDATE _qail_branches SET status = 'deleted' WHERE name = {} AND status = 'active';",
        safe_name
    )
}

/// SQL to read overlay rows for a branch on a specific table.
///
/// Returns the latest overlay row per PK (last write wins).
/// Use this to merge with main table results in CoW reads.
pub fn read_overlay_sql(branch_name: &str, table_name: &str) -> String {
    let safe_branch = escape_literal(branch_name);
    let safe_table = escape_literal(table_name);
    format!(
        "SELECT DISTINCT ON (row_pk) row_pk, operation, row_data \
         FROM _qail_branch_rows \
         WHERE branch_id = (SELECT id FROM _qail_branches WHERE name = {}) \
           AND table_name = {} \
         ORDER BY row_pk, created_at DESC;",
        safe_branch, safe_table
    )
}

/// SQL to insert a CoW write into the overlay.
///
/// # Arguments
///
/// * `branch_name` — Name of the branch to write to.
/// * `table_name` — Target table for the overlay row.
/// * `row_pk` — Primary key of the affected row.
/// * `operation` — Operation type (`insert`, `update`, `delete`).
pub fn write_overlay_sql(branch_name: &str, table_name: &str, row_pk: &str, operation: &str) -> String {
    let safe_branch = escape_literal(branch_name);
    let safe_table = escape_literal(table_name);
    let safe_pk = escape_literal(row_pk);
    let safe_op = escape_literal(operation);
    format!(
        "INSERT INTO _qail_branch_rows (branch_id, table_name, row_pk, operation, row_data) \
         VALUES (\
           (SELECT id FROM _qail_branches WHERE name = {}), \
           {}, {}, {}, $1\
         ) RETURNING id;",
        safe_branch, safe_table, safe_pk, safe_op
    )
}

/// SQL to merge a branch — applies all overlay rows to the main tables.
///
/// This is a multi-step operation:
/// 1. For each 'insert' overlay: INSERT INTO main table
/// 2. For each 'update' overlay: UPDATE main table SET ... WHERE pk = ...
/// 3. For each 'delete' overlay: DELETE FROM main table WHERE pk = ...
/// 4. Mark branch as merged
///
/// Returns SQL to mark the branch as merged (the actual merge logic
/// is done in Rust by iterating overlay rows).
pub fn mark_merged_sql(name: &str) -> String {
    let safe_name = escape_literal(name);
    format!(
        "UPDATE _qail_branches SET status = 'merged', merged_at = now() \
         WHERE name = {} AND status = 'active';",
        safe_name
    )
}

/// SQL to get overlay stats for a branch.
pub fn branch_stats_sql(name: &str) -> String {
    let safe_name = escape_literal(name);
    format!(
        "SELECT table_name, operation, COUNT(*) as count \
         FROM _qail_branch_rows \
         WHERE branch_id = (SELECT id FROM _qail_branches WHERE name = {}) \
         GROUP BY table_name, operation \
         ORDER BY table_name, operation;",
        safe_name
    )
}

/// SQL to get all overlay rows for a branch, ordered for merge application.
///
/// Returns (table_name, row_pk, operation, row_data) tuples.
/// Order: by table_name then created_at so operations are applied chronologically.
pub fn merge_overlay_rows_sql(name: &str) -> String {
    let safe_name = escape_literal(name);
    format!(
        "SELECT DISTINCT ON (table_name, row_pk) table_name, row_pk, operation, row_data::text \
         FROM _qail_branch_rows \
         WHERE branch_id = (SELECT id FROM _qail_branches WHERE name = {}) \
         ORDER BY table_name, row_pk, created_at DESC;",
        safe_name
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_branch_context_sql() {
        let sql = branch_context_sql("feature-auth");
        assert_eq!(sql, "SET LOCAL app.branch_id = 'feature-auth';");
    }

    #[test]
    fn test_branch_context_sql_escapes_quotes() {
        let sql = branch_context_sql("it's a branch");
        assert_eq!(sql, "SET LOCAL app.branch_id = 'it''s a branch';");
    }

    #[test]
    fn test_create_branch_sql_no_parent() {
        let sql = create_branch_sql("dev", None);
        assert!(sql.contains("INSERT INTO _qail_branches"));
        assert!(sql.contains("'dev'"));
        assert!(!sql.contains("parent_branch_id"));
    }

    #[test]
    fn test_create_branch_sql_with_parent() {
        let sql = create_branch_sql("feature-1", Some("dev"));
        assert!(sql.contains("parent_branch_id"));
        assert!(sql.contains("'feature-1'"));
        assert!(sql.contains("'dev'"));
    }

    #[test]
    fn test_read_overlay_sql() {
        let sql = read_overlay_sql("feature-1", "users");
        assert!(sql.contains("DISTINCT ON (row_pk)"));
        assert!(sql.contains("'feature-1'"));
        assert!(sql.contains("'users'"));
    }

    #[test]
    fn test_write_overlay_sql() {
        let sql = write_overlay_sql("feat", "orders", "123", "insert");
        assert!(sql.contains("INSERT INTO _qail_branch_rows"));
        assert!(sql.contains("'orders'"));
        assert!(sql.contains("'123'"));
        assert!(sql.contains("'insert'"));
    }

    #[test]
    fn test_mark_merged_sql() {
        let sql = mark_merged_sql("dev");
        assert!(sql.contains("status = 'merged'"));
        assert!(sql.contains("merged_at = now()"));
    }
}
