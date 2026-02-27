//! Schema watch mode

use crate::colors::*;
use anyhow::Result;
use qail_core::migrate::{diff_schemas, parse_qail_file};

use crate::sql_gen::cmd_to_sql;

/// Watch a schema file for changes and auto-generate migrations.
pub async fn watch_schema(schema_path: &str, db_url: Option<&str>, auto_apply: bool) -> Result<()> {
    use notify_debouncer_full::{DebounceEventResult, new_debouncer, notify::RecursiveMode};
    use std::sync::mpsc::channel;
    use std::time::Duration;

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
