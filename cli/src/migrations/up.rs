//! Migration UP operations

use anyhow::Result;
use colored::*;
use qail_core::migrate::{diff_schemas, parse_qail};
use qail_core::prelude::Qail;
use qail_pg::driver::PgDriver;

use crate::migrations::migration_table_ddl;
use crate::sql_gen::cmd_to_sql;
use crate::util::parse_pg_url;

/// Apply migrations forward using qail-pg native driver.
pub async fn migrate_up(
    schema_diff_path: &str,
    url: &str,
    codebase: Option<&str>,
    force: bool,
) -> Result<()> {
    println!("{} {}", "Migrating UP:".cyan().bold(), url.yellow());

    let (old_schema, new_schema, cmds) =
        if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
            let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
            let old_path = parts[0];
            let new_path = parts[1];

            let old_content = std::fs::read_to_string(old_path)
                .map_err(|e| anyhow::anyhow!("Failed to read old schema: {}", e))?;
            let new_content = std::fs::read_to_string(new_path)
                .map_err(|e| anyhow::anyhow!("Failed to read new schema: {}", e))?;

            let old_schema = parse_qail(&old_content)
                .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;
            let new_schema = parse_qail(&new_content)
                .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

            let cmds = diff_schemas(&old_schema, &new_schema);
            (old_schema, new_schema, cmds)
        } else {
            return Err(anyhow::anyhow!(
                "Please provide two .qail files: old.qail:new.qail"
            ));
        };

    if cmds.is_empty() {
        println!("{}", "No migrations to apply.".green());
        return Ok(());
    }

    println!("{} {} migration(s) to apply", "Found:".cyan(), cmds.len());

    // === PHASE 0: Codebase Impact Analysis ===
    if let Some(codebase_path) = codebase {
        use qail_core::analyzer::{CodebaseScanner, MigrationImpact};
        use std::path::Path;

        println!();
        println!("{}", "🔍 Scanning codebase for breaking changes...".cyan());

        let scanner = CodebaseScanner::new();
        let code_path = Path::new(codebase_path);

        if !code_path.exists() {
            return Err(anyhow::anyhow!("Codebase path not found: {}", codebase_path));
        }

        let code_refs = scanner.scan(code_path);
        let impact = MigrationImpact::analyze(&cmds, &code_refs, &old_schema, &new_schema);

        if !impact.safe_to_run {
            println!();
            println!("{}", "⚠️  BREAKING CHANGES DETECTED IN CODEBASE".red().bold());
            println!(
                "   {} file(s) affected, {} reference(s) found",
                impact.affected_files,
                code_refs.len()
            );
            println!();

            for change in &impact.breaking_changes {
                match change {
                    qail_core::analyzer::BreakingChange::DroppedColumn {
                        table,
                        column,
                        references,
                    } => {
                        println!(
                            "   {} {}.{} ({} refs)",
                            "DROP COLUMN".red(),
                            table.yellow(),
                            column.yellow(),
                            references.len()
                        );
                        for r in references.iter().take(3) {
                            println!(
                                "     ❌ {}:{} → uses {} in {}",
                                r.file.display(),
                                r.line,
                                column.cyan().bold(),
                                r.snippet.dimmed()
                            );
                        }
                    }
                    qail_core::analyzer::BreakingChange::DroppedTable { table, references } => {
                        println!(
                            "   {} {} ({} refs)",
                            "DROP TABLE".red(),
                            table.yellow(),
                            references.len()
                        );
                        for r in references.iter().take(3) {
                            println!(
                                "     ❌ {}:{} → {}",
                                r.file.display(),
                                r.line,
                                r.snippet.cyan()
                            );
                        }
                    }
                    _ => {}
                }
            }

            if !force {
                println!();
                println!(
                    "{}",
                    "Migration BLOCKED. Fix your code first, or use --force to proceed anyway.".red()
                );
                return Ok(());
            } else {
                println!();
                println!("{}", "⚠️  Proceeding anyway due to --force flag...".yellow());
            }
        } else {
            println!("   {} No breaking changes detected", "✓".green());
        }
    }

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

    // === PHASE 1: Impact Analysis ===
    use crate::backup::{
        analyze_impact, create_snapshots, display_impact, prompt_migration_choice, MigrationChoice,
    };

    let mut impacts = Vec::new();
    for cmd in &cmds {
        if let Ok(impact) = analyze_impact(&mut driver, cmd).await {
            impacts.push(impact);
        }
    }

    let has_destructive = impacts.iter().any(|i| i.is_destructive);
    let mut _migration_version = String::new();

    if has_destructive {
        display_impact(&impacts);

        let choice = prompt_migration_choice();

        _migration_version = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();

        match choice {
            MigrationChoice::Cancel => {
                println!("{}", "Migration cancelled.".yellow());
                return Ok(());
            }
            MigrationChoice::BackupToFile => {
                create_snapshots(&mut driver, &impacts).await?;
            }
            MigrationChoice::BackupToDatabase => {
                use crate::backup::create_db_snapshots;
                create_db_snapshots(&mut driver, &_migration_version, &impacts).await?;
            }
            MigrationChoice::Proceed => {
                println!("{}", "Proceeding without backup...".dimmed());
            }
        }
    }

    // Begin transaction for atomic migration
    println!("{}", "Starting transaction...".dimmed());
    driver
        .begin()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start transaction: {}", e))?;

    // Ensure migration table exists (AST-native bootstrap)
    driver
        .execute_raw(&migration_table_ddl())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create migration table: {}", e))?;

    let mut applied = 0;
    let mut sql_up_all = String::new();

    for (i, cmd) in cmds.iter().enumerate() {
        println!(
            "  {} {} {}",
            format!("[{}/{}]", i + 1, cmds.len()).cyan(),
            format!("{}", cmd.action).yellow(),
            &cmd.table
        );

        let sql = cmd_to_sql(cmd);
        sql_up_all.push_str(&sql);
        sql_up_all.push_str(";\n");

        if let Err(e) = driver.execute(cmd).await {
            println!("{}", "Rolling back transaction...".red());
            let _ = driver.rollback().await;
            return Err(anyhow::anyhow!(
                "Migration failed at step {}/{}: {}\nTransaction rolled back - database unchanged.",
                i + 1,
                cmds.len(),
                e
            ));
        }
        applied += 1;
    }

    let version = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
    let checksum = format!("{:x}", md5::compute(&sql_up_all));

    // Record migration in history (AST-native)
    let record_cmd = Qail::add("_qail_migrations")
        .columns(["version", "name", "checksum", "sql_up"])
        .values([
            version.clone(),
            format!("auto_{}", version),
            checksum,
            sql_up_all,
        ]);

    driver
        .execute(&record_cmd)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to record migration: {}", e))?;

    // Commit transaction
    driver
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;

    println!(
        "{}",
        format!("✓ {} migrations applied successfully (atomic)", applied)
            .green()
            .bold()
    );
    println!("  Recorded as migration: {}", version.cyan());
    Ok(())
}
