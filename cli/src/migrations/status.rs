//! Migration status operations

use crate::colors::*;
use anyhow::Result;
use qail_core::prelude::*;
use qail_pg::PgDriver;

use crate::migrations::ensure_migration_table;
use crate::util::parse_pg_url;

/// Show migration status and history.
pub async fn migrate_status(url: &str) -> Result<()> {
    println!("{}", "📋 Migration Status".cyan().bold());
    println!();

    let (host, port, user, password, database) = parse_pg_url(url)?;
    let mut driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?
    };

    // Ensure migration table exists (AST-native bootstrap)
    ensure_migration_table(&mut driver)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create migration table: {}", e))?;

    println!("  Database: {}", database.yellow());
    println!("  Migration table: {}", "_qail_migrations".green());
    println!();

    // Query migration history with column data
    let status_cmd = Qail::get("_qail_migrations")
        .columns(vec!["version", "name", "applied_at", "checksum"])
        .order_by("applied_at", SortOrder::Desc);

    match driver.query_ast(&status_cmd).await {
        Ok(result) => {
            if result.rows.is_empty() {
                println!("  {} No migrations applied yet", "○".dimmed());
                println!();
                println!("  Run {} to apply migrations", "qail migrate up".cyan());
                return Ok(());
            }

            // Print migration history table
            println!(
                "  {} {} migration(s) applied\n",
                "✓".green(),
                result.rows.len().to_string().green()
            );

            // Header
            println!(
                "  {}  {}  {}  {}",
                format!("{:<14}", "VERSION").cyan().bold(),
                format!("{:<30}", "NAME").cyan().bold(),
                format!("{:<25}", "APPLIED AT").cyan().bold(),
                "CHECKSUM".cyan().bold(),
            );
            println!("  {}", "─".repeat(85).dimmed());

            // Rows
            for row in &result.rows {
                let version = row
                    .first()
                    .and_then(|v| v.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or("?");
                let name = row
                    .get(1)
                    .and_then(|v| v.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or("-");
                let applied_at = row
                    .get(2)
                    .and_then(|v| v.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or("-");
                let checksum = row
                    .get(3)
                    .and_then(|v| v.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or("-");

                // Truncate checksum for display
                let checksum_short = if checksum.len() > 12 {
                    format!("{}…", &checksum[..12])
                } else {
                    checksum.to_string()
                };

                // Truncate applied_at to remove microseconds
                let applied_short = if applied_at.len() > 19 {
                    &applied_at[..19]
                } else {
                    applied_at
                };

                println!(
                    "  {:<14}  {:<30}  {:<25}  {}",
                    version.white(),
                    name,
                    applied_short.dimmed(),
                    checksum_short.dimmed(),
                );
            }

            println!();
            println!(
                "  Run {} to rollback by version",
                "qail migrate rollback --to".cyan()
            );
        }
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Failed to query migration history from _qail_migrations: {}",
                e
            ));
        }
    }

    Ok(())
}
