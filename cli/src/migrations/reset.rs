//! Migration reset — drop everything and re-apply from scratch.
//!
//! `qail migrate reset schema.qail <url>`
//!
//! Equivalent to: down (current → empty) + clear history + up (empty → target)

use crate::colors::*;
use anyhow::Result;
use qail_core::migrate::{diff_schemas, parse_qail};
use qail_pg::PgDriver;

use crate::migrations::{
    MigrationReceipt, acquire_migration_lock, ensure_migration_table, now_epoch_ms, runtime_actor,
    runtime_git_sha, write_migration_receipt,
};
use crate::util::parse_pg_url;

/// Reset database: drop all objects, clear migration history, re-apply target schema.
pub async fn migrate_reset(
    schema_file: &str,
    url: &str,
    wait_for_lock: bool,
    lock_timeout_secs: Option<u64>,
) -> Result<()> {
    println!("{} {}", "🔄 Resetting database:".cyan().bold(), url);
    println!();

    // Parse target schema
    let target_content = qail_core::schema_source::read_qail_schema_source(schema_file)
        .map_err(|e| anyhow::anyhow!("Cannot read {}: {}", schema_file, e))?;
    let target_schema = parse_qail(&target_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", schema_file, e))?;

    let empty_schema = Default::default();

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
    acquire_migration_lock(
        &mut driver,
        "migrate reset",
        wait_for_lock,
        lock_timeout_secs,
        Some(database.as_str()),
    )
    .await?;

    // Build DROP plan from live schema to avoid leaving drift objects behind.
    let live_schema = crate::shadow::introspect_schema(&mut driver)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to introspect live schema: {}", e))?;

    // Phase 1: Diff live → empty (generates DROP statements for current DB state)
    let drop_cmds = diff_schemas(&live_schema, &empty_schema);

    // === Phase 1: DROP everything ===
    if drop_cmds.is_empty() {
        println!("  {} No objects to drop", "○".dimmed());
    } else {
        println!("  {} Dropping {} object(s)...", "↓".red(), drop_cmds.len());

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
    match driver.execute(&clear_cmd).await {
        Ok(_) => println!("  {} Cleared migration history", "✓".green()),
        Err(e) => {
            let msg = e.to_string();
            // Table doesn't exist yet — that's fine for reset
            if msg.contains("does not exist") || msg.contains("42P01") {
                println!("  {} No migration history to clear", "○".dimmed());
            } else {
                anyhow::bail!(
                    "Failed to clear migration history (stale rows may cause drift): {}",
                    e
                );
            }
        }
    }

    // === Phase 3: CREATE everything ===
    if create_cmds.is_empty() {
        println!("  {} No objects to create", "○".dimmed());
    } else {
        println!(
            "\n  {} Creating {} object(s)...",
            "↑".green(),
            create_cmds.len()
        );

        // Capture start time before create phase for accurate receipts
        let started_ms = now_epoch_ms();

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
        ensure_migration_table(&mut driver)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to bootstrap migration table: {}", e))?;

        let version = crate::time::timestamp_version();
        let name = format!("reset_{}", version);
        let checksum = crate::time::md5_hex(&target_content);
        let finished_ms = now_epoch_ms();
        let receipt = MigrationReceipt {
            version: version.clone(),
            name: name.clone(),
            checksum,
            sql_up: "-- reset migration".to_string(),
            git_sha: runtime_git_sha(),
            qail_version: env!("CARGO_PKG_VERSION").to_string(),
            actor: runtime_actor(),
            started_at_ms: Some(started_ms),
            finished_at_ms: Some(finished_ms),
            duration_ms: Some(finished_ms.saturating_sub(started_ms)),
            affected_rows_est: None,
            risk_summary: Some(format!(
                "source=reset;drop_cmds={};create_cmds={}",
                drop_cmds.len(),
                create_cmds.len()
            )),
            shadow_checksum: None,
        };
        write_migration_receipt(&mut driver, &receipt)
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
