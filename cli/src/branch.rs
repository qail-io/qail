//! Branch management CLI commands — Data Virtualization
//!
//! `qail branch create <name>` — Create a new branch
//! `qail branch list`          — List all branches
//! `qail branch delete <name>` — Soft-delete a branch
//! `qail branch merge <name>`  — Mark a branch as merged

use anyhow::{Context, Result};
use qail_pg::driver::branch_sql;

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
    conn.execute_simple(&sql)
        .await
        .context(format!("Failed to create branch '{}'", name))?;

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
    let rows = conn.simple_query(sql).await.unwrap_or_default();

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
    conn.execute_simple(&sql)
        .await
        .context(format!("Failed to delete branch '{}'", name))?;

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

    let sql = branch_sql::mark_merged_sql(name);
    conn.execute_simple(&sql)
        .await
        .context(format!("Failed to merge branch '{}'", name))?;

    println!("✅ Branch '{}' merged", name);
    Ok(())
}

// Helpers

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
    use super::parse_url;

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
}
