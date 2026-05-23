//! Migration reset — drop everything and re-apply from scratch.
//!
//! `qail migrate reset schema.qail <url>`
//!
//! Equivalent to: down (current → empty) + clear history + up (empty → target)

use crate::colors::*;
use anyhow::Result;
use qail_core::migrate::{diff_schemas_checked, parse_qail};
use qail_pg::PgDriver;

use crate::migrations::{
    MigrationReceipt, acquire_migration_lock, ensure_migration_table, now_epoch_ms, runtime_actor,
    runtime_git_sha, write_migration_receipt,
};
use crate::util::{parse_pg_url, redact_url};

async fn migration_history_table_exists(driver: &mut PgDriver) -> Result<bool> {
    let exists_cmd = qail_core::prelude::Qail::get("information_schema.tables")
        .column("1")
        .where_eq("table_schema", "public")
        .where_eq("table_name", "_qail_migrations")
        .limit(1);
    let rows = driver
        .fetch_all(&exists_cmd)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to check migration history table: {}", e))?;
    Ok(!rows.is_empty())
}

/// Reset database: drop all objects, clear migration history, re-apply target schema.
pub async fn migrate_reset(
    schema_file: &str,
    url: &str,
    wait_for_lock: bool,
    lock_timeout_secs: Option<u64>,
) -> Result<()> {
    println!(
        "{} {}",
        "🔄 Resetting database:".cyan().bold(),
        redact_url(url)
    );
    println!();

    // Parse target schema
    let target_content = qail_core::schema_source::read_qail_schema_source(schema_file)
        .map_err(|e| anyhow::anyhow!("Cannot read {}: {}", schema_file, e))?;
    let target_schema = parse_qail(&target_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse {}: {}", schema_file, e))?;

    let empty_schema = Default::default();

    // Phase 2: Diff empty → target (generates CREATE statements)
    let create_cmds = diff_schemas_checked(&empty_schema, &target_schema).map_err(|e| {
        anyhow::anyhow!(
            "State-based diff unsupported for target reset schema '{}': {}",
            schema_file,
            e
        )
    })?;

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
    let drop_cmds = diff_schemas_checked(&live_schema, &empty_schema).map_err(|e| {
        anyhow::anyhow!(
            "State-based diff unsupported for live reset schema '{}': {}",
            schema_file,
            e
        )
    })?;

    let history_table_exists = migration_history_table_exists(&mut driver).await?;

    driver
        .begin()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to begin reset transaction: {}", e))?;

    // === Phase 1: DROP everything ===
    if drop_cmds.is_empty() {
        println!("  {} No objects to drop", "○".dimmed());
    } else {
        println!("  {} Dropping {} object(s)...", "↓".red(), drop_cmds.len());

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
    }

    // === Phase 2: Clear migration history ===
    println!("  {} Clearing migration history...", "⊘".yellow());
    if history_table_exists {
        let clear_cmd = qail_core::prelude::Qail::del("_qail_migrations");
        match driver.execute(&clear_cmd).await {
            Ok(_) => println!("  {} Cleared migration history", "✓".green()),
            Err(e) => {
                let _ = driver.rollback().await;
                anyhow::bail!(
                    "Failed to clear migration history (stale rows may cause drift): {}",
                    e
                );
            }
        }
    } else {
        println!("  {} No migration history to clear", "○".dimmed());
    }

    let mut recorded_version = None;

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
        if let Err(e) = ensure_migration_table(&mut driver).await {
            let _ = driver.rollback().await;
            return Err(anyhow::anyhow!(
                "Failed to bootstrap migration table: {}",
                e
            ));
        }

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
        if let Err(e) = write_migration_receipt(&mut driver, &receipt).await {
            let _ = driver.rollback().await;
            return Err(anyhow::anyhow!("Failed to record migration: {}", e));
        }
        recorded_version = Some(version);
    }

    driver
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to commit reset transaction: {}", e))?;

    if let Some(version) = recorded_version {
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

#[cfg(test)]
mod tests {
    use super::migrate_reset;
    use qail_core::prelude::Qail;
    use qail_pg::PgDriver;

    async fn table_exists(pg: &mut PgDriver, table: &str) -> bool {
        let cmd = Qail::get("information_schema.tables")
            .column("1")
            .where_eq("table_schema", "public")
            .where_eq("table_name", table)
            .limit(1);
        match pg.fetch_all(&cmd).await {
            Ok(rows) => !rows.is_empty(),
            Err(_) => false,
        }
    }

    async fn version_exists(pg: &mut PgDriver, version: &str) -> bool {
        let cmd = Qail::get("_qail_migrations")
            .column("version")
            .where_eq("version", version)
            .limit(1);
        match pg.query_ast(&cmd).await {
            Ok(result) => !result.rows.is_empty(),
            Err(_) => false,
        }
    }

    #[tokio::test]
    async fn reset_create_failure_rolls_back_drops_and_history_cleanup_in_real_db() {
        let Some(url) = std::env::var("QAIL_TEST_DB_URL").ok() else {
            eprintln!("Skipping reset atomicity DB test (set QAIL_TEST_DB_URL)");
            return;
        };

        let suffix = format!(
            "{}_{}",
            std::process::id(),
            crate::time::timestamp_version()
        );
        let marker_table = format!("reset_atomic_marker_{}", suffix);
        let seed_version = format!("reset_atomic_seed_{}.up.qail", suffix);
        let schema_file = std::env::temp_dir().join(format!("reset_atomic_{}.qail", suffix));

        let mut pg = PgDriver::connect_url(&url)
            .await
            .expect("connect QAIL_TEST_DB_URL");
        crate::migrations::ensure_migration_table(&mut pg)
            .await
            .expect("bootstrap _qail_migrations");

        let _ = pg
            .execute_simple(&format!("DROP TABLE IF EXISTS {} CASCADE", marker_table))
            .await;
        let cleanup_seed = Qail::del("_qail_migrations").where_eq("version", seed_version.as_str());
        let _ = pg.execute(&cleanup_seed).await;

        pg.execute_simple(&format!("CREATE TABLE {} (id integer)", marker_table))
            .await
            .expect("create marker table");
        let seed = Qail::add("_qail_migrations")
            .set_value("version", seed_version.as_str())
            .set_value("name", "reset_atomic_seed")
            .set_value("checksum", "reset_atomic_seed_checksum")
            .set_value("sql_up", "-- reset atomic seed");
        pg.execute(&seed).await.expect("seed migration history");

        std::fs::write(
            &schema_file,
            "table _qail_migrations {\n  id serial primary_key\n}\n",
        )
        .expect("write conflicting reset schema");

        let err = migrate_reset(
            schema_file.to_str().expect("utf-8 temp path"),
            &url,
            true,
            Some(10),
        )
        .await
        .expect_err("reset should fail when target create conflicts with history table");

        assert!(
            err.to_string().contains("Create failed"),
            "unexpected reset error: {err}"
        );
        assert!(
            table_exists(&mut pg, marker_table.as_str()).await,
            "drop phase should be rolled back when create fails"
        );
        assert!(
            version_exists(&mut pg, seed_version.as_str()).await,
            "history cleanup should be rolled back when create fails"
        );

        let _ = pg
            .execute_simple(&format!("DROP TABLE IF EXISTS {} CASCADE", marker_table))
            .await;
        let _ = pg.execute(&cleanup_seed).await;
        let _ = std::fs::remove_file(&schema_file);
    }
}
