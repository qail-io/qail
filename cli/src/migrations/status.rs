//! Migration status operations

use anyhow::Result;
use colored::*;
use qail_core::prelude::Qail;
use qail_pg::driver::PgDriver;

use crate::migrations::migration_table_ddl;
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
    driver
        .execute_raw(&migration_table_ddl())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create migration table: {}", e))?;

    // Query migration history (AST-native)
    let status_cmd = Qail::get("_qail_migrations");

    // Status check: attempt to fetch from migration table
    println!("  Database: {}", database.yellow());
    println!("  Migration table: {}", "_qail_migrations".green());
    println!();

    // Try to fetch (AST-native check)
    let check_result = driver.fetch_all(&status_cmd).await;

    match check_result {
        Ok(rows) => {
            println!(
                "  {} Migration history table is ready ({} records)",
                "✓".green(),
                rows.len()
            );
            println!();
            println!("  Run {} to apply migrations", "qail migrate up".cyan());
        }
        Err(_) => {
            println!("  {} No migrations applied yet", "○".dimmed());
        }
    }

    Ok(())
}
