//! Schema watch mode

use crate::colors::*;
use anyhow::Result;
use qail_core::ast::Qail;
use qail_core::migrate::{diff_schemas_checked, parse_qail_file};
use qail_pg::PgDriver;

use crate::sql_gen::cmd_to_sql;
use crate::util::parse_pg_url;

/// Watch a schema file for changes and auto-generate migrations.
pub async fn watch_schema(schema_path: &str, db_url: Option<&str>, auto_apply: bool) -> Result<()> {
    use notify_debouncer_full::{DebounceEventResult, new_debouncer, notify::RecursiveMode};
    use std::sync::mpsc::channel;
    use std::time::Duration;

    if auto_apply && db_url.is_none() {
        return Err(anyhow::anyhow!(
            "Auto-apply requires a database URL. Pass --url or configure DATABASE_URL/qail.toml."
        ));
    }

    let source = qail_core::schema_source::resolve_schema_source(schema_path)
        .map_err(|e| anyhow::anyhow!("Schema source not found: {}", e))?;

    println!("{}", "👀 QAIL Schema Watch Mode".cyan().bold());
    println!("   Watching: {}", schema_path.yellow());
    if let Some(url) = db_url {
        println!("   Database: {}", url.yellow());
        if auto_apply {
            println!("   Auto-apply: {}", "enabled".green());
        }
    }
    println!("   Press {} to stop\n", "Ctrl+C".red());

    // Load initial schema
    let mut last_schema = parse_qail_file(schema_path)
        .map_err(|e| anyhow::anyhow!("Failed to parse initial schema: {}", e))?;

    let mut driver = if auto_apply {
        let url = db_url.expect("checked above");
        let (host, port, user, password, database) = parse_pg_url(url)?;
        let connected = if let Some(pwd) = password {
            PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect for auto-apply: {}", e))?
        } else {
            PgDriver::connect(&host, port, &user, &database)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect for auto-apply: {}", e))?
        };
        Some(connected)
    } else {
        None
    };

    println!(
        "[{}] Initial schema loaded: {} tables",
        crate::time::timestamp_short().dimmed(),
        last_schema.tables.len()
    );

    let (tx, rx) = channel::<DebounceEventResult>();
    let mut debouncer = new_debouncer(Duration::from_millis(500), None, tx)?;

    let mode = if source.is_directory() {
        RecursiveMode::Recursive
    } else {
        RecursiveMode::NonRecursive
    };
    debouncer.watch(&source.root, mode)?;

    loop {
        match rx.recv() {
            Ok(Ok(events)) => {
                if !events.is_empty() {
                    let now = crate::time::timestamp_short();

                    let new_schema = match parse_qail_file(schema_path) {
                        Ok(s) => s,
                        Err(e) => {
                            println!("[{}] {} Parse error: {}", now.dimmed(), "✗".red(), e);
                            continue;
                        }
                    };

                    let cmds = match diff_schemas_checked(&last_schema, &new_schema) {
                        Ok(cmds) => cmds,
                        Err(e) => {
                            println!(
                                "[{}] {} State-based diff unsupported: {}",
                                now.dimmed(),
                                "✗".red(),
                                e
                            );
                            continue;
                        }
                    };

                    if cmds.is_empty() {
                        println!("[{}] {} No changes detected", now.dimmed(), "•".dimmed());
                    } else {
                        let mut should_advance_schema = true;
                        println!(
                            "[{}] {} Detected {} change(s):",
                            now.dimmed(),
                            "✓".green(),
                            cmds.len()
                        );

                        for cmd in &cmds {
                            let sql = cmd_to_sql(cmd);
                            println!("       {}", sql.cyan());
                        }

                        if auto_apply {
                            println!("[{}] Applying to database...", now.dimmed());
                            if let Some(driver) = driver.as_mut() {
                                match apply_watch_changes(driver, &cmds).await {
                                    Ok(()) => {
                                        println!("       {} Applied successfully", "✓".green())
                                    }
                                    Err(e) => {
                                        println!("       {} Apply failed: {}", "✗".red(), e);
                                        println!(
                                            "       {} Keeping previous schema baseline for retry",
                                            "⚠".yellow()
                                        );
                                        should_advance_schema = false;
                                    }
                                }
                            }
                        }

                        if should_advance_schema {
                            last_schema = new_schema;
                        }
                        continue;
                    }

                    last_schema = new_schema;
                }
            }
            Ok(Err(errors)) => {
                for e in errors {
                    println!("{} Watch error: {}", "✗".red(), e);
                }
            }
            Err(e) => {
                println!("{} Channel error: {}", "✗".red(), e);
                break;
            }
        }
    }

    Ok(())
}

async fn apply_watch_changes(driver: &mut PgDriver, cmds: &[Qail]) -> Result<()> {
    if cmds.is_empty() {
        return Ok(());
    }

    driver
        .begin()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to begin watch apply transaction: {}", e))?;

    for (idx, cmd) in cmds.iter().enumerate() {
        if let Err(e) = driver.execute(cmd).await {
            let _ = driver.rollback().await;
            return Err(anyhow::anyhow!(
                "Watch auto-apply failed at step {}/{}: {}",
                idx + 1,
                cmds.len(),
                e
            ));
        }
    }

    driver
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to commit watch apply transaction: {}", e))?;
    Ok(())
}
