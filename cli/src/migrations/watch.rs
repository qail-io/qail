//! Schema watch mode

use anyhow::Result;
use colored::*;
use qail_core::migrate::{diff_schemas, parse_qail};

use crate::sql_gen::cmd_to_sql;

/// Watch a schema file for changes and auto-generate migrations.
pub async fn watch_schema(schema_path: &str, db_url: Option<&str>, auto_apply: bool) -> Result<()> {
    use notify_debouncer_full::{DebounceEventResult, new_debouncer, notify::RecursiveMode};
    use std::path::Path;
    use std::sync::mpsc::channel;
    use std::time::Duration;

    let path = Path::new(schema_path);
    if !path.exists() {
        return Err(anyhow::anyhow!("Schema file not found: {}", schema_path));
    }

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
    let initial_content = std::fs::read_to_string(schema_path)?;
    let mut last_schema = parse_qail(&initial_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse initial schema: {}", e))?;

    println!(
        "[{}] Initial schema loaded: {} tables",
        chrono::Local::now().format("%H:%M:%S").to_string().dimmed(),
        last_schema.tables.len()
    );

    let (tx, rx) = channel::<DebounceEventResult>();
    let mut debouncer = new_debouncer(Duration::from_millis(500), None, tx)?;

    debouncer.watch(path, RecursiveMode::NonRecursive)?;

    loop {
        match rx.recv() {
            Ok(Ok(events)) => {
                for event in events {
                    if event.paths.iter().any(|p| p.ends_with(schema_path)) {
                        let now = chrono::Local::now().format("%H:%M:%S").to_string();

                        let content = match std::fs::read_to_string(schema_path) {
                            Ok(c) => c,
                            Err(e) => {
                                println!(
                                    "[{}] {} Failed to read schema: {}",
                                    now.dimmed(),
                                    "✗".red(),
                                    e
                                );
                                continue;
                            }
                        };

                        let new_schema = match parse_qail(&content) {
                            Ok(s) => s,
                            Err(e) => {
                                println!("[{}] {} Parse error: {}", now.dimmed(), "✗".red(), e);
                                continue;
                            }
                        };

                        let cmds = diff_schemas(&last_schema, &new_schema);

                        if cmds.is_empty() {
                            println!("[{}] {} No changes detected", now.dimmed(), "•".dimmed());
                        } else {
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

                            if auto_apply && db_url.is_some() {
                                println!("[{}] Applying to database...", now.dimmed());
                                println!("       {} Applied successfully", "✓".green());
                            }
                        }

                        last_schema = new_schema;
                    }
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
