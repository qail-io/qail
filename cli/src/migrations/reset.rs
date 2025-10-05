//! Migration reset — drop everything and re-apply from scratch.
//!
//! `qail migrate reset schema.qail <url>`
//!
//! Equivalent to: down (current → empty) + clear history + up (empty → target)

use anyhow::Result;
use crate::colors::*;
use qail_core::migrate::{diff_schemas, parse_qail};
use qail_pg::PgDriver;

use crate::migrations::migration_table_ddl;
use crate::util::parse_pg_url;

/// Reset database: drop all objects, clear migration history, re-apply target schema.
pub async fn migrate_reset(schema_file: &str, url: &str) -> Result<()> {
    println!(
        "{} {}",
        "🔄 Resetting database:".cyan().bold(),
        url
    );
    println!();

    // Parse target schema
    let target_content = std::fs::read_to_string(schema_file)
        .map_err(|e| anyhow::anyhow!("Cannot read {}: {}", schema_file, e))?;
    let target_schema = parse_qail(&target_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", schema_file, e))?;

    let empty_schema = Default::default();

    // Phase 1: Diff target → empty (generates DROP statements)
    let drop_cmds = diff_schemas(&target_schema, &empty_schema);

    // Phase 2: Diff empty → target (generates CREATE statements)
    let create_cmds = diff_schemas(&empty_schema, &target_schema);

    // Connect
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

    // Ensure migration table exists
    driver
        .execute_raw(&migration_table_ddl())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bootstrap migration table: {}", e))?;

    // === Phase 1: DROP everything ===
    if drop_cmds.is_empty() {
        println!("  {} No objects to drop", "○".dimmed());
    } else {
        println!(
            "  {} Dropping {} object(s)...",
            "↓".red(),
            drop_cmds.len()
        );

        driver
            .begin()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

        for (i, cmd) in drop_cmds.iter().enumerate() {
            print!("    [{}] {} ", i + 1, format!("{:?}", cmd.action).red());
            match driver.execute(cmd).await {
                Ok(_) => println!("{}", "✓".green()),
                Err(e) => {
                    println!("{}", "✗".red());
                    let _ = driver.rollback().await;
                    anyhow::bail!("Drop failed at step {}: {}", i + 1, e);
                }
            }
        }

        driver
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to commit drops: {}", e))?;
    }

    // === Phase 2: Clear migration history ===
    println!("  {} Clearing migration history...", "⊘".yellow());
    let clear_cmd = qail_core::prelude::Qail::del("_qail_migrations");
    driver.execute(&clear_cmd).await.ok(); // Ignore if empty

    // === Phase 3: CREATE everything ===
    if create_cmds.is_empty() {
        println!("  {} No objects to create", "○".dimmed());
    } else {
        println!(
            "\n  {} Creating {} object(s)...",
            "↑".green(),
            create_cmds.len()
        );

        driver
            .begin()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;

        for (i, cmd) in create_cmds.iter().enumerate() {
            print!("    [{}] {} ", i + 1, format!("{:?}", cmd.action).green());
            match driver.execute(cmd).await {
                Ok(_) => println!("{}", "✓".green()),
                Err(e) => {
                    println!("{}", "✗".red());
                    let _ = driver.rollback().await;
                    anyhow::bail!("Create failed at step {}: {}", i + 1, e);
                }
            }
        }

        // Record migration
        let version = crate::time::timestamp_version();
        let name = format!("reset_{}", version);
        let checksum = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            target_content.hash(&mut hasher);
            format!("{:x}", hasher.finish())
        };

        let record_sql = format!(
            "INSERT INTO _qail_migrations (version, name, checksum, sql_up) VALUES ('{}', '{}', '{}', '{}')",
            version,
            name,
            checksum,
            "-- reset migration"
        );
        driver
            .execute_raw(&record_sql)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to record migration: {}", e))?;

        driver
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to commit creates: {}", e))?;

        println!(
            "\n  {} Recorded as migration: {}",
            "✓".green(),
            version.white()
        );
    }

    println!(
        "\n{} Database reset to {} successfully",
        "✅".green(),
        schema_file.cyan()
    );

    Ok(())
}
