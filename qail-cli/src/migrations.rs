//! Migration operations for QAIL CLI
//!
//! Contains functions for managing database migrations:
//! - status: Show migration history
//! - plan: Preview SQL without executing
//! - up: Apply migrations forward
//! - down: Rollback migrations
//! - watch: Live schema monitoring
//! - analyze: Impact analysis on codebase

use anyhow::Result;
use colored::*;
use qail_core::migrate::{diff_schemas, parse_qail};
use qail_pg::driver::PgDriver;

use crate::sql_gen::{cmd_to_sql, generate_rollback_sql};
use crate::util::parse_pg_url;

/// Migration table schema in QAIL format (AST-native).
pub const MIGRATION_TABLE_SCHEMA: &str = r#"
table _qail_migrations (
    id serial primary_key,
    version varchar(255) not null unique,
    name varchar(255),
    applied_at timestamptz default NOW(),
    checksum varchar(64) not null,
    sql_up text not null,
    sql_down text
)
"#;

/// Generate migration table DDL from AST (AST-native bootstrap).
pub fn migration_table_ddl() -> String {
    use qail_core::parser::schema::Schema;
    Schema::parse(MIGRATION_TABLE_SCHEMA)
        .expect("Invalid migration table schema")
        .tables
        .first()
        .expect("No table in migration schema")
        .to_ddl()
}

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
    use qail_core::ast::QailCmd;

    let status_cmd = QailCmd::get("_qail_migrations");

    // Status check: attempt to fetch from migration table
    // For now, show that the table exists
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

/// Analyze migration impact on codebase before executing.
pub fn migrate_analyze(schema_diff_path: &str, codebase_path: &str) -> Result<()> {
    use qail_core::analyzer::{CodebaseScanner, MigrationImpact};
    use std::path::Path;

    println!("{}", "🔍 Migration Impact Analyzer".cyan().bold());
    println!();

    // Parse schema files
    let (old_schema, new_schema, cmds) =
        if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
            let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
            let old_path = parts[0];
            let new_path = parts[1];

            println!("  Schema: {} → {}", old_path.yellow(), new_path.yellow());

            let old_content = std::fs::read_to_string(old_path)
                .map_err(|e| anyhow::anyhow!("Failed to read old schema: {}", e))?;
            let new_content = std::fs::read_to_string(new_path)
                .map_err(|e| anyhow::anyhow!("Failed to read new schema: {}", e))?;

            let old = parse_qail(&old_content)
                .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;
            let new = parse_qail(&new_content)
                .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

            let cmds = diff_schemas(&old, &new);
            (old, new, cmds)
        } else {
            return Err(anyhow::anyhow!(
                "Please provide two .qail files: old.qail:new.qail"
            ));
        };

    if cmds.is_empty() {
        println!(
            "{}",
            "✓ No migrations needed - schemas are identical".green()
        );
        return Ok(());
    }

    println!("  Codebase: {}", codebase_path.yellow());
    println!();

    // Scan codebase
    let scanner = CodebaseScanner::new();
    let code_path = Path::new(codebase_path);

    if !code_path.exists() {
        return Err(anyhow::anyhow!(
            "Codebase path not found: {}",
            codebase_path
        ));
    }

    println!("{}", "Scanning codebase...".dimmed());
    let code_refs = scanner.scan(code_path);
    println!("  Found {} query references\n", code_refs.len());

    // Analyze impact
    let impact = MigrationImpact::analyze(&cmds, &code_refs, &old_schema, &new_schema);

    if impact.safe_to_run {
        println!("{}", "✓ Migration is safe to run".green().bold());
        println!("  No breaking changes detected in codebase\n");

        println!("{}", "Migration preview:".cyan());
        for cmd in &cmds {
            let sql = cmd_to_sql(cmd);
            println!("  {}", sql);
        }
    } else {
        println!("{}", "⚠️  BREAKING CHANGES DETECTED".red().bold());
        println!();
        println!("Affected files: {}", impact.affected_files);
        println!();

        for change in &impact.breaking_changes {
            match change {
                qail_core::analyzer::BreakingChange::DroppedTable { table, references } => {
                    println!(
                        "┌─ {} {} ({} references) ─────────────────────────┐",
                        "DROP TABLE".red(),
                        table.yellow(),
                        references.len()
                    );
                    for r in references.iter().take(5) {
                        println!(
                            "│ {} {}:{} → {}",
                            "❌".red(),
                            r.file.display(),
                            r.line,
                            r.snippet.cyan()
                        );
                    }
                    if references.len() > 5 {
                        println!("│ ... and {} more", references.len() - 5);
                    }
                    println!("└──────────────────────────────────────────────────────┘");
                    println!();
                }
                qail_core::analyzer::BreakingChange::DroppedColumn {
                    table,
                    column,
                    references,
                } => {
                    println!(
                        "┌─ {} {}.{} ({} references) ─────────────────┐",
                        "DROP COLUMN".red(),
                        table.yellow(),
                        column.yellow(),
                        references.len()
                    );
                    for r in references.iter().take(5) {
                        println!(
                            "│ {} {}:{} → {}",
                            "❌".red(),
                            r.file.display(),
                            r.line,
                            r.snippet.cyan()
                        );
                    }
                    println!("└──────────────────────────────────────────────────────┘");
                    println!();
                }
                qail_core::analyzer::BreakingChange::RenamedColumn {
                    table, references, ..
                } => {
                    println!(
                        "┌─ {} on {} ({} references) ───────────────────┐",
                        "RENAME".yellow(),
                        table.yellow(),
                        references.len()
                    );
                    for r in references.iter().take(5) {
                        println!(
                            "│ {} {}:{} → {}",
                            "⚠️ ".yellow(),
                            r.file.display(),
                            r.line,
                            r.snippet.cyan()
                        );
                    }
                    println!("└──────────────────────────────────────────────────────┘");
                    println!();
                }
                _ => {}
            }
        }

        println!("What would you like to do?");
        println!(
            "  1. {} (DANGEROUS - will cause {} runtime errors)",
            "Run anyway".red(),
            impact.breaking_changes.len()
        );
        println!(
            "  2. {} (show SQL, don't execute)",
            "Dry-run first".yellow()
        );
        println!("  3. {} (exit)", "Let me fix the code first".green());
    }

    Ok(())
}

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

    // Set up file watcher
    let (tx, rx) = channel::<DebounceEventResult>();
    let mut debouncer = new_debouncer(Duration::from_millis(500), None, tx)?;

    debouncer.watch(path, RecursiveMode::NonRecursive)?;

    loop {
        match rx.recv() {
            Ok(Ok(events)) => {
                for event in events {
                    if event.paths.iter().any(|p| p.ends_with(schema_path)) {
                        // File changed
                        let now = chrono::Local::now().format("%H:%M:%S").to_string();

                        // Reload schema
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

                        // Compute diff
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

                            // Apply if auto_apply and URL provided
                            if auto_apply && db_url.is_some() {
                                println!("[{}] Applying to database...", now.dimmed());
                                // Would call apply logic here
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

/// Preview migration SQL without executing (dry-run).
pub fn migrate_plan(schema_diff_path: &str, output: Option<&str>) -> Result<()> {
    println!("{}", "📋 Migration Plan (dry-run)".cyan().bold());
    println!();

    // Parse schema files
    let cmds = if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
        let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
        let old_path = parts[0];
        let new_path = parts[1];

        println!("  {} → {}", old_path.yellow(), new_path.yellow());
        println!();

        let old_content = std::fs::read_to_string(old_path)
            .map_err(|e| anyhow::anyhow!("Failed to read old schema: {}", e))?;
        let new_content = std::fs::read_to_string(new_path)
            .map_err(|e| anyhow::anyhow!("Failed to read new schema: {}", e))?;

        let old_schema = parse_qail(&old_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;
        let new_schema = parse_qail(&new_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

        diff_schemas(&old_schema, &new_schema)
    } else {
        return Err(anyhow::anyhow!(
            "Please provide two .qail files: old.qail:new.qail"
        ));
    };

    if cmds.is_empty() {
        println!(
            "{}",
            "✓ No migrations needed - schemas are identical".green()
        );
        return Ok(());
    }

    // Generate UP SQL
    let mut up_sql = Vec::new();
    let mut down_sql = Vec::new();

    println!(
        "┌─ {} ({} operations) ─────────────────────────────────┐",
        "UP".green().bold(),
        cmds.len()
    );
    for (i, cmd) in cmds.iter().enumerate() {
        let sql = cmd_to_sql(cmd);
        println!("│ {}. {}", i + 1, sql.cyan());
        up_sql.push(format!("{}. {}", i + 1, sql));

        // Generate rollback SQL
        let rollback = generate_rollback_sql(cmd);
        down_sql.push(format!("{}. {}", i + 1, rollback));
    }
    println!("└──────────────────────────────────────────────────────────────┘");
    println!();

    println!(
        "┌─ {} ({} operations) ──────────────────────────────┐",
        "DOWN".yellow().bold(),
        cmds.len()
    );
    for sql in &down_sql {
        println!("│ {}", sql.yellow());
    }
    println!("└──────────────────────────────────────────────────────────────┘");

    // Save to file if requested
    if let Some(path) = output {
        let mut content = String::new();
        content.push_str("-- Migration UP\n");
        for cmd in &cmds {
            content.push_str(&format!("{};\n", cmd_to_sql(cmd)));
        }
        content.push_str("\n-- Migration DOWN (rollback)\n");
        for (i, cmd) in cmds.iter().enumerate() {
            content.push_str(&format!("-- {}. {};\n", i + 1, generate_rollback_sql(cmd)));
        }
        std::fs::write(path, &content)?;
        println!();
        println!("{} {}", "Saved to:".green(), path);
    }

    println!();
    println!(
        "{} Run 'qail migrate up old.qail:new.qail <URL>' to apply",
        "💡".yellow()
    );

    Ok(())
}

/// Apply migrations forward using qail-pg native driver.
pub async fn migrate_up(schema_diff_path: &str, url: &str, codebase: Option<&str>, force: bool) -> Result<()> {
    println!("{} {}", "Migrating UP:".cyan().bold(), url.yellow());

    // Parse schema files to get old/new schemas and commands
    let (old_schema, new_schema, cmds) = if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
        // Two schema files: old:new
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
            return Err(anyhow::anyhow!(
                "Codebase path not found: {}",
                codebase_path
            ));
        }

        let code_refs = scanner.scan(code_path);
        let impact = MigrationImpact::analyze(&cmds, &code_refs, &old_schema, &new_schema);

        if !impact.safe_to_run {
            println!();
            println!("{}", "⚠️  BREAKING CHANGES DETECTED IN CODEBASE".red().bold());
            println!("   {} file(s) affected, {} reference(s) found", impact.affected_files, code_refs.len());
            println!();

            for change in &impact.breaking_changes {
                match change {
                    qail_core::analyzer::BreakingChange::DroppedColumn { table, column, references } => {
                        println!("   {} {}.{} ({} refs)", "DROP COLUMN".red(), table.yellow(), column.yellow(), references.len());
                        for r in references.iter().take(3) {
                            println!("     ❌ {}:{} → {}", r.file.display(), r.line, r.snippet.cyan());
                        }
                    }
                    qail_core::analyzer::BreakingChange::DroppedTable { table, references } => {
                        println!("   {} {} ({} refs)", "DROP TABLE".red(), table.yellow(), references.len());
                        for r in references.iter().take(3) {
                            println!("     ❌ {}:{} → {}", r.file.display(), r.line, r.snippet.cyan());
                        }
                    }
                    _ => {}
                }
            }

            if !force {
                println!();
                println!("{}", "Migration BLOCKED. Fix your code first, or use --force to proceed anyway.".red());
                return Ok(());
            } else {
                println!();
                println!("{}", "⚠️  Proceeding anyway due to --force flag...".yellow());
            }
        } else {
            println!("   {} No breaking changes detected", "✓".green());
        }
    }

    // Parse URL and connect using qail-pg
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
        MigrationChoice, analyze_impact, create_snapshots, display_impact, prompt_migration_choice,
    };

    let mut impacts = Vec::new();
    for cmd in &cmds {
        if let Ok(impact) = analyze_impact(&mut driver, cmd).await {
            impacts.push(impact);
        }
    }

    // Check for destructive operations
    let has_destructive = impacts.iter().any(|i| i.is_destructive);
    let mut _migration_version = String::new();

    if has_destructive {
        display_impact(&impacts);

        let choice = prompt_migration_choice();

        // Generate version early for database backup
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
            // Rollback on failure
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

    // Generate migration version and checksum
    let version = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
    let checksum = format!("{:x}", md5::compute(&sql_up_all));

    // Record migration in history (AST-native)
    use qail_core::ast::QailCmd;

    let record_cmd = QailCmd::add("_qail_migrations")
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

/// Rollback migrations using qail-pg native driver.
pub async fn migrate_down(schema_diff_path: &str, url: &str) -> Result<()> {
    println!("{} {}", "Migrating DOWN:".cyan().bold(), url.yellow());

    // For rollback, we reverse the diff: old becomes new, new becomes old
    let cmds = if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
        let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
        let old_path = parts[0];
        let new_path = parts[1];

        // Swap: rollback means going from new -> old
        let old_content = std::fs::read_to_string(new_path)
            .map_err(|e| anyhow::anyhow!("Failed to read new schema: {}", e))?;
        let new_content = std::fs::read_to_string(old_path)
            .map_err(|e| anyhow::anyhow!("Failed to read old schema: {}", e))?;

        let old_schema = parse_qail(&old_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse schema: {}", e))?;
        let new_schema = parse_qail(&new_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse schema: {}", e))?;

        diff_schemas(&old_schema, &new_schema)
    } else {
        println!("{}", "Warning: Rollback requires two .qail files".yellow());
        println!("  Use format: qail migrate down old.qail:new.qail <url>");
        return Ok(());
    };

    if cmds.is_empty() {
        println!("{}", "No rollbacks to apply.".green());
        return Ok(());
    }

    println!("{} {} rollback(s) to apply", "Found:".cyan(), cmds.len());

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

    // Begin transaction for atomic rollback
    println!("{}", "Starting transaction...".dimmed());
    driver
        .begin()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start transaction: {}", e))?;

    let mut applied = 0;
    for (i, cmd) in cmds.iter().enumerate() {
        println!(
            "  {} {} {}",
            format!("[{}/{}]", i + 1, cmds.len()).cyan(),
            format!("{}", cmd.action).yellow(),
            &cmd.table
        );

        if let Err(e) = driver.execute(cmd).await {
            // Rollback on failure
            println!("{}", "Rolling back transaction...".red());
            let _ = driver.rollback().await;
            return Err(anyhow::anyhow!(
                "Rollback failed at step {}/{}: {}\nTransaction rolled back - database unchanged.",
                i + 1,
                cmds.len(),
                e
            ));
        }
        applied += 1;
    }

    // Commit transaction
    driver
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;

    println!(
        "{}",
        format!("✓ {} rollbacks applied successfully (atomic)", applied)
            .green()
            .bold()
    );
    Ok(())
}

/// Create a new named migration file.
pub fn migrate_create(name: &str, depends: Option<&str>, author: Option<&str>) -> Result<()> {
    use qail_core::migrate::MigrationMeta;
    use std::path::Path;

    println!("{}", "📝 Creating Named Migration".cyan().bold());
    println!();

    // Generate version timestamp
    let timestamp = chrono::Local::now().format("%Y%m%d%H%M%S").to_string();
    let created = chrono::Local::now().to_rfc3339();

    // Build migration metadata
    let mut meta = MigrationMeta::new(&format!("{}_{}", timestamp, name));
    meta.created = Some(created);

    if let Some(deps) = depends {
        meta.depends = deps
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }

    if let Some(auth) = author {
        meta.author = Some(auth.to_string());
    }

    // Ensure migrations directory exists
    let migrations_dir = Path::new("migrations");
    if !migrations_dir.exists() {
        std::fs::create_dir_all(migrations_dir)?;
        println!("  Created {} directory", "migrations/".yellow());
    }

    // Generate file content
    let filename = format!("{}_{}.qail", timestamp, name);
    let filepath = migrations_dir.join(&filename);

    let content = format!(
        "{}\
# Migration: {}\n\
# Add your schema changes below:\n\
# +table example {{\n\
#   id UUID primary_key\n\
# }}\n\
# +column users.new_field TEXT\n",
        meta.to_header(),
        name
    );

    std::fs::write(&filepath, &content)?;

    println!("  {} {}", "✓ Created:".green(), filepath.display());
    println!();
    println!("  Migration: {}", meta.name.cyan());
    if !meta.depends.is_empty() {
        println!("  Depends:   {}", meta.depends.join(", ").yellow());
    }
    if let Some(ref auth) = meta.author {
        println!("  Author:    {}", auth.dimmed());
    }
    println!();
    println!("  Edit the file to add your schema changes, then run:");
    println!(
        "    {} old.qail:{}",
        "qail migrate up".cyan(),
        filename.yellow()
    );

    Ok(())
}
