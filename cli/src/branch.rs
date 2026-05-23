//! Branch management CLI commands — Data Virtualization
//!
//! `qail branch create <name>` — Create a new branch
//! `qail branch list`          — List all branches
//! `qail branch delete <name>` — Soft-delete a branch
//! `qail branch merge <name>`  — Mark a branch as merged

use anyhow::{Context, Result, bail};
use qail_pg::driver::branch_sql;
use serde_json::Value;
use std::collections::HashMap;

/// Create internal branch tables + a new named branch.
pub async fn branch_create(name: &str, parent: Option<&str>, db_url: &str) -> Result<()> {
    let (host, port, user, database, password) = parse_url(db_url)?;
    let mut conn = connect(&host, port, &user, &database, password.as_deref()).await?;

    // Auto-bootstrap tables
    let ddl = branch_sql::create_branch_tables_sql();
    conn.execute_simple(ddl)
        .await
        .context("Failed to create branch tables (may already exist)")?;

    let sql = branch_sql::create_branch_sql(name, parent);
    let rows = conn
        .simple_query(&sql)
        .await
        .context(format!("Failed to create branch '{}'", name))?;
    require_branch_returning_row(
        rows.len(),
        match parent {
            Some(parent) => format!(
                "Branch '{}' was not created because parent branch '{}' was not found or is not active",
                name, parent
            ),
            None => format!("Branch '{}' was not created", name),
        },
    )?;

    println!("✅ Branch '{}' created", name);
    if let Some(p) = parent {
        println!("   Parent: {}", p);
    }
    Ok(())
}

/// List all branches.
pub async fn branch_list(db_url: &str) -> Result<()> {
    let (host, port, user, database, password) = parse_url(db_url)?;
    let mut conn = connect(&host, port, &user, &database, password.as_deref()).await?;

    let sql = branch_sql::list_branches_sql();
    let rows = conn
        .simple_query(sql)
        .await
        .context("Failed to list branches")?;

    if rows.is_empty() {
        println!("No branches found. Create one with: qail branch create <name>");
        return Ok(());
    }

    println!("{:<36}  {:<20}  {:<10}  CREATED", "ID", "NAME", "STATUS");
    println!("{}", "-".repeat(80));

    for row in &rows {
        let id = row.get_string(0).unwrap_or_default();
        let name = row.get_string(1).unwrap_or_default();
        let status = row.get_string(5).unwrap_or_default();
        let created = row.get_string(3).unwrap_or_default();
        println!("{:<36}  {:<20}  {:<10}  {}", id, name, status, created);
    }

    Ok(())
}

/// Soft-delete a branch.
pub async fn branch_delete(name: &str, db_url: &str) -> Result<()> {
    let (host, port, user, database, password) = parse_url(db_url)?;
    let mut conn = connect(&host, port, &user, &database, password.as_deref()).await?;

    let sql = branch_sql::delete_branch_sql(name);
    let rows = conn
        .simple_query(&sql)
        .await
        .context(format!("Failed to delete branch '{}'", name))?;
    require_branch_returning_row(
        rows.len(),
        format!("Branch '{}' was not found or is not active", name),
    )?;

    println!("🗑  Branch '{}' deleted", name);
    Ok(())
}

/// Mark a branch as merged.
pub async fn branch_merge(name: &str, db_url: &str) -> Result<()> {
    let (host, port, user, database, password) = parse_url(db_url)?;
    let mut conn = connect(&host, port, &user, &database, password.as_deref()).await?;

    // Show stats first
    let stats_sql = branch_sql::branch_stats_sql(name);
    if let Ok(rows) = conn.simple_query(&stats_sql).await
        && !rows.is_empty()
    {
        println!("📊 Overlay stats for '{}':", name);
        for row in &rows {
            let table = row.get_string(0).unwrap_or_default();
            let op = row.get_string(1).unwrap_or_default();
            let count = row.get_string(2).unwrap_or_default();
            println!("   {} {} → {} rows", table, op, count);
        }
    }

    let applied = merge_branch_transactional(&mut conn, name)
        .await
        .context(format!("Failed to merge branch '{}'", name))?;

    println!(
        "✅ Branch '{}' merged ({} overlay rows applied)",
        name, applied
    );
    Ok(())
}

// Helpers

#[derive(Debug, Clone, PartialEq, Eq)]
struct OverlayRow {
    table: String,
    row_pk: String,
    operation: String,
    row_data: Option<String>,
}

async fn merge_branch_transactional(
    conn: &mut qail_pg::driver::PgConnection,
    name: &str,
) -> Result<u32> {
    conn.begin_transaction()
        .await
        .context("Failed to begin branch merge transaction")?;

    let result = apply_branch_merge(conn, name).await;
    match result {
        Ok(applied) => {
            if let Err(err) = conn.commit().await {
                let _ = conn.rollback().await;
                return Err(err).context("Failed to commit branch merge transaction");
            }
            Ok(applied)
        }
        Err(err) => {
            let _ = conn.rollback().await;
            Err(err)
        }
    }
}

async fn apply_branch_merge(conn: &mut qail_pg::driver::PgConnection, name: &str) -> Result<u32> {
    let lock_sql = branch_sql::lock_active_branch_for_merge_sql(name);
    let lock_rows = conn
        .simple_query(&lock_sql)
        .await
        .context("Failed to lock branch for merge")?;
    if lock_rows.is_empty() {
        bail!("Branch '{}' was not found or is not active", name);
    }

    let overlay_sql = branch_sql::merge_overlay_rows_sql(name);
    let overlay_rows = conn
        .simple_query(&overlay_sql)
        .await
        .context("Failed to read branch overlay rows")?;

    let mut pk_cache = HashMap::<String, String>::new();
    let mut applied = 0u32;
    for row in overlay_rows {
        let overlay = overlay_row_from_pg(&row)?;
        let pk = primary_key_for_table(conn, &mut pk_cache, &overlay.table).await?;
        if let Some(sql) = overlay_apply_sql(&overlay, &pk)? {
            conn.execute_simple(&sql).await.with_context(|| {
                format!(
                    "Failed to apply branch overlay row {}.{} ({})",
                    overlay.table, overlay.row_pk, overlay.operation
                )
            })?;
            applied += 1;
        }
    }

    let merge_sql = branch_sql::mark_merged_sql(name);
    let merged_rows = conn
        .simple_query(&merge_sql)
        .await
        .context("Failed to mark branch merged")?;
    if merged_rows.is_empty() {
        bail!("Branch '{}' was not found or is not active", name);
    }

    Ok(applied)
}

fn overlay_row_from_pg(row: &qail_pg::driver::PgRow) -> Result<OverlayRow> {
    Ok(OverlayRow {
        table: row
            .get_string(0)
            .ok_or_else(|| anyhow::anyhow!("Branch overlay row is missing table_name"))?,
        row_pk: row
            .get_string(1)
            .ok_or_else(|| anyhow::anyhow!("Branch overlay row is missing row_pk"))?,
        operation: row
            .get_string(2)
            .ok_or_else(|| anyhow::anyhow!("Branch overlay row is missing operation"))?,
        row_data: row.get_string(3),
    })
}

fn require_branch_returning_row(row_count: usize, missing_message: String) -> Result<()> {
    if row_count == 0 {
        bail!(missing_message);
    }
    Ok(())
}

async fn primary_key_for_table(
    conn: &mut qail_pg::driver::PgConnection,
    cache: &mut HashMap<String, String>,
    table: &str,
) -> Result<String> {
    if let Some(pk) = cache.get(table) {
        return Ok(pk.clone());
    }

    let pk = load_single_primary_key(conn, table).await?;
    cache.insert(table.to_string(), pk.clone());
    Ok(pk)
}

async fn load_single_primary_key(
    conn: &mut qail_pg::driver::PgConnection,
    table: &str,
) -> Result<String> {
    let sql = format!(
        "SELECT a.attname \
         FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         WHERE i.indrelid = to_regclass({}) AND i.indisprimary \
         ORDER BY a.attnum;",
        branch_sql::escape_literal(&quote_ident(table))
    );
    let rows = conn
        .simple_query(&sql)
        .await
        .with_context(|| format!("Failed to inspect primary key for table '{}'", table))?;

    match rows.len() {
        1 => rows[0]
            .get_string(0)
            .ok_or_else(|| anyhow::anyhow!("Primary key metadata for '{}' was empty", table)),
        0 => bail!(
            "Cannot merge branch overlay for table '{}': table has no primary key",
            table
        ),
        _ => bail!(
            "Cannot merge branch overlay for table '{}': composite primary keys are not supported by CLI merge",
            table
        ),
    }
}

fn overlay_apply_sql(row: &OverlayRow, pk_col: &str) -> Result<Option<String>> {
    match row.operation.as_str() {
        "insert" => overlay_insert_sql(row, pk_col).map(Some),
        "update" => overlay_update_sql(row, pk_col),
        "delete" => Ok(Some(overlay_delete_sql(row, pk_col))),
        other => bail!("Unsupported branch overlay operation '{}'", other),
    }
}

fn overlay_insert_sql(row: &OverlayRow, pk_col: &str) -> Result<String> {
    let columns = overlay_columns(row)?;
    let table = quote_ident(&row.table);
    let quoted_columns = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let select_columns = columns
        .iter()
        .map(|c| format!("src.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let data = branch_sql::escape_literal(row.row_data.as_deref().unwrap_or("{}"));
    let pk = quote_ident(pk_col);
    let update_columns = columns
        .iter()
        .filter(|c| c.as_str() != pk_col)
        .map(|c| {
            let col = quote_ident(c);
            format!("{} = EXCLUDED.{}", col, col)
        })
        .collect::<Vec<_>>();
    let conflict = if update_columns.is_empty() {
        format!("ON CONFLICT ({}) DO NOTHING", pk)
    } else {
        format!(
            "ON CONFLICT ({}) DO UPDATE SET {}",
            pk,
            update_columns.join(", ")
        )
    };

    Ok(format!(
        "INSERT INTO {} ({}) \
         SELECT {} FROM jsonb_populate_record(NULL::{}, {}::jsonb) AS src \
         {};",
        table, quoted_columns, select_columns, table, data, conflict
    ))
}

fn overlay_update_sql(row: &OverlayRow, pk_col: &str) -> Result<Option<String>> {
    let columns = overlay_columns(row)?;
    let assignments = columns
        .iter()
        .filter(|c| c.as_str() != pk_col)
        .map(|c| {
            let col = quote_ident(c);
            format!("{} = src.{}", col, col)
        })
        .collect::<Vec<_>>();
    if assignments.is_empty() {
        return Ok(None);
    }

    let table = quote_ident(&row.table);
    let pk = quote_ident(pk_col);
    let data = branch_sql::escape_literal(row.row_data.as_deref().unwrap_or("{}"));
    let row_pk = branch_sql::escape_literal(&row.row_pk);
    Ok(Some(format!(
        "UPDATE {} AS target \
         SET {} \
         FROM jsonb_populate_record(NULL::{}, {}::jsonb) AS src \
         WHERE target.{}::text = {};",
        table,
        assignments.join(", "),
        table,
        data,
        pk,
        row_pk
    )))
}

fn overlay_delete_sql(row: &OverlayRow, pk_col: &str) -> String {
    format!(
        "DELETE FROM {} WHERE {}::text = {};",
        quote_ident(&row.table),
        quote_ident(pk_col),
        branch_sql::escape_literal(&row.row_pk)
    )
}

fn overlay_columns(row: &OverlayRow) -> Result<Vec<String>> {
    let data = row.row_data.as_deref().ok_or_else(|| {
        anyhow::anyhow!("Branch overlay {} row is missing row_data", row.operation)
    })?;
    let value: Value = serde_json::from_str(data).with_context(|| {
        format!(
            "Branch overlay row {}.{} contains invalid JSON",
            row.table, row.row_pk
        )
    })?;
    let obj = value.as_object().ok_or_else(|| {
        anyhow::anyhow!(
            "Branch overlay row {}.{} row_data must be a JSON object",
            row.table,
            row.row_pk
        )
    })?;
    let columns = obj.keys().cloned().collect::<Vec<_>>();
    if columns.is_empty() {
        bail!("Branch overlay row_data must contain at least one column");
    }
    Ok(columns)
}

fn quote_ident(name: &str) -> String {
    name.split('.')
        .map(|part| format!("\"{}\"", part.replace('\0', "").replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

fn parse_url(url: &str) -> Result<(String, u16, String, String, Option<String>)> {
    let (host, port, user, password, database) = crate::util::parse_pg_url(url)?;
    Ok((host, port, user, database, password))
}

async fn connect(
    host: &str,
    port: u16,
    user: &str,
    database: &str,
    password: Option<&str>,
) -> Result<qail_pg::driver::PgConnection> {
    let conn =
        qail_pg::driver::PgConnection::connect_with_password(host, port, user, database, password)
            .await
            .context("Failed to connect to database")?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::{
        OverlayRow, overlay_apply_sql, parse_url, quote_ident, require_branch_returning_row,
    };
    use anyhow::Result;
    use qail_pg::driver::branch_sql;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn branch_parse_url_reuses_strict_cli_parser() {
        let (host, port, user, database, password) =
            parse_url("postgres://us%40er:p%40ss@db.example.com:15432/app").unwrap();
        assert_eq!(host, "db.example.com");
        assert_eq!(port, 15432);
        assert_eq!(user, "us@er");
        assert_eq!(database, "app");
        assert_eq!(password, Some("p@ss".to_string()));
    }

    #[test]
    fn branch_mutations_reject_empty_returning_rows() {
        let err = require_branch_returning_row(0, "Branch 'missing' was not found".to_string())
            .expect_err("zero RETURNING rows must fail");

        assert!(err.to_string().contains("missing"));
        require_branch_returning_row(1, "unused".to_string()).expect("returned row should pass");
    }

    #[test]
    fn branch_merge_insert_overlay_sql_upserts_into_base_table() {
        let row = OverlayRow {
            table: "users".to_string(),
            row_pk: "u1".to_string(),
            operation: "insert".to_string(),
            row_data: Some(r#"{"id":"u1","name":"Ada"}"#.to_string()),
        };

        let sql = overlay_apply_sql(&row, "id")
            .expect("insert sql")
            .expect("non-empty sql");

        assert!(sql.starts_with(r#"INSERT INTO "users""#), "{sql}");
        assert!(
            sql.contains(r#"jsonb_populate_record(NULL::"users""#),
            "{sql}"
        );
        assert!(sql.contains(r#"ON CONFLICT ("id") DO UPDATE SET"#), "{sql}");
        assert!(sql.contains(r#""name" = EXCLUDED."name""#), "{sql}");
    }

    #[test]
    fn branch_merge_update_overlay_sql_uses_row_pk_filter() {
        let row = OverlayRow {
            table: "users".to_string(),
            row_pk: "u'1".to_string(),
            operation: "update".to_string(),
            row_data: Some(r#"{"name":"Grace"}"#.to_string()),
        };

        let sql = overlay_apply_sql(&row, "id")
            .expect("update sql")
            .expect("non-empty sql");

        assert!(sql.starts_with(r#"UPDATE "users" AS target"#), "{sql}");
        assert!(sql.contains(r#"SET "name" = src."name""#), "{sql}");
        assert!(sql.contains(r#"WHERE target."id"::text = 'u''1'"#), "{sql}");
    }

    #[test]
    fn branch_merge_delete_overlay_sql_deletes_from_base_table() {
        let row = OverlayRow {
            table: "users".to_string(),
            row_pk: "u1".to_string(),
            operation: "delete".to_string(),
            row_data: None,
        };

        let sql = overlay_apply_sql(&row, "id")
            .expect("delete sql")
            .expect("non-empty sql");

        assert_eq!(sql, r#"DELETE FROM "users" WHERE "id"::text = 'u1';"#);
    }

    #[test]
    fn branch_merge_rejects_malformed_overlay_json() {
        let row = OverlayRow {
            table: "users".to_string(),
            row_pk: "u1".to_string(),
            operation: "insert".to_string(),
            row_data: Some("[]".to_string()),
        };

        let err = overlay_apply_sql(&row, "id").expect_err("array overlay must fail closed");
        assert!(err.to_string().contains("row_data must be a JSON object"));
    }

    #[tokio::test]
    async fn branch_merge_applies_overlay_rows_in_live_db_when_configured() -> Result<()> {
        let Ok(db_url) = std::env::var("QAIL_TEST_DB_URL") else {
            eprintln!("Skipping branch merge live DB test (set QAIL_TEST_DB_URL)");
            return Ok(());
        };

        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let table = format!("qail_branch_merge_cli_{}", suffix);
        let branch = format!("qail_branch_merge_cli_{}", suffix);
        let (host, port, user, database, password) = parse_url(&db_url)?;
        let mut conn = super::connect(&host, port, &user, &database, password.as_deref()).await?;

        let setup_result: Result<()> = async {
            conn.execute_simple(&format!("DROP TABLE IF EXISTS {}", quote_ident(&table)))
                .await?;
            conn.execute_simple(&format!(
                "CREATE TABLE {} (id text PRIMARY KEY, name text)",
                quote_ident(&table)
            ))
            .await?;
            conn.execute_simple(branch_sql::create_branch_tables_sql())
                .await?;
            conn.execute_simple(&branch_sql::create_branch_sql(&branch, None))
                .await?;
            conn.execute_simple(&format!(
                "INSERT INTO _qail_branch_rows (branch_id, table_name, row_pk, operation, row_data) \
                 SELECT id, {}, 'u1', 'insert', {}::jsonb \
                 FROM _qail_branches WHERE name = {};",
                branch_sql::escape_literal(&table),
                branch_sql::escape_literal(r#"{"id":"u1","name":"Ada"}"#),
                branch_sql::escape_literal(&branch)
            ))
            .await?;

            super::branch_merge(&branch, &db_url).await?;

            let rows = conn
                .simple_query(&format!(
                    "SELECT name FROM {} WHERE id = 'u1'",
                    quote_ident(&table)
                ))
                .await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get_string(0).as_deref(), Some("Ada"));

            let status_rows = conn
                .simple_query(&format!(
                    "SELECT status FROM _qail_branches WHERE name = {}",
                    branch_sql::escape_literal(&branch)
                ))
                .await?;
            assert_eq!(status_rows[0].get_string(0).as_deref(), Some("merged"));
            Ok(())
        }
        .await;

        let _ = conn
            .execute_simple(&format!("DROP TABLE IF EXISTS {}", quote_ident(&table)))
            .await;
        let _ = conn
            .execute_simple(&format!(
                "DELETE FROM _qail_branches WHERE name = {}",
                branch_sql::escape_literal(&branch)
            ))
            .await;

        setup_result
    }
}
