//! Schema validation and diff operations

use anyhow::Result;
use colored::*;
use qail_core::migrate::{diff_schemas, parse_qail};
use qail_core::prelude::*;
use qail_core::transpiler::Dialect;
use crate::migrations::types::{MigrationClass, classify_migration};

/// Output format for schema operations.
#[derive(Clone)]
pub enum OutputFormat {
    Sql,
    Json,
    Pretty,
}

/// Validate a QAIL schema file with detailed error reporting.
/// When `src_dir` is provided, also scans source for query validation + RLS audit.
pub fn check_schema(schema_path: &str, src_dir: Option<&str>, migrations_dir: &str) -> Result<()> {
    if schema_path.contains(':') && !schema_path.starts_with("postgres") {
        let parts: Vec<&str> = schema_path.splitn(2, ':').collect();
        if parts.len() == 2 {
            println!(
                "{} {} → {}",
                "Checking migration:".cyan().bold(),
                parts[0].yellow(),
                parts[1].yellow()
            );
            return check_migration(parts[0], parts[1]);
        }
    }

    // Single schema file validation
    println!(
        "{} {}",
        "Checking schema:".cyan().bold(),
        schema_path.yellow()
    );

    let content = std::fs::read_to_string(schema_path)
        .map_err(|e| anyhow::anyhow!("Failed to read schema file '{}': {}", schema_path, e))?;

    match parse_qail(&content) {
        Ok(schema) => {
            println!("{}", "✓ Schema is valid".green().bold());
            println!("  Tables: {}", schema.tables.len());

            // Detailed breakdown
            let mut total_columns = 0;
            let mut primary_keys = 0;
            let mut unique_constraints = 0;

            for table in schema.tables.values() {
                total_columns += table.columns.len();
                for col in &table.columns {
                    if col.primary_key {
                        primary_keys += 1;
                    }
                    if col.unique {
                        unique_constraints += 1;
                    }
                }
            }

            println!("  Columns: {}", total_columns);
            println!("  Indexes: {}", schema.indexes.len());
            println!("  Migration Hints: {}", schema.migrations.len());

            if primary_keys > 0 {
                println!("  {} {} primary key(s)", "✓".green(), primary_keys);
            }
            if unique_constraints > 0 {
                println!(
                    "  {} {} unique constraint(s)",
                    "✓".green(),
                    unique_constraints
                );
            }

            // Source scan + RLS audit (when --src is provided)
            if let Some(src) = src_dir {
                println!();
                println!("{}", "── Source Validation & RLS Audit ──".cyan().bold());

                // Use build module's Schema (has rls_enabled detection)
                let mut build_schema = qail_core::build::Schema::parse(&content)
                    .map_err(|e| anyhow::anyhow!("Failed to parse schema for audit: {}", e))?;

                // Merge migrations if directory exists
                let mig_path = std::path::Path::new(migrations_dir);
                if mig_path.exists() {
                    let merged = build_schema.merge_migrations(migrations_dir).unwrap_or(0);
                    if merged > 0 {
                        println!("  {} Merged {} schema changes from {}", "✓".green(), merged, migrations_dir);
                    }
                }

                // Show RLS-enabled tables
                let rls_tables = build_schema.rls_tables();
                if rls_tables.is_empty() {
                    println!("  {} No RLS-enabled tables detected", "ℹ".dimmed());
                } else {
                    println!("  {} {} RLS-enabled table(s): {}",
                        "🔐".to_string().green(),
                        rls_tables.len(),
                        rls_tables.join(", ").yellow()
                    );
                }

                // Scan source files
                let usages = qail_core::build::scan_source_files(src);

                if usages.is_empty() {
                    println!("  {} No Qail queries found in {}", "ℹ".dimmed(), src);
                } else {
                    // Run validation + RLS audit
                    let errors = qail_core::build::validate_against_schema(&build_schema, &usages);

                    // Separate schema errors from RLS warnings
                    let schema_errors: Vec<_> = errors.iter().filter(|e| !e.contains("RLS AUDIT")).collect();
                    let rls_warnings: Vec<_> = errors.iter().filter(|e| e.contains("RLS AUDIT")).collect();

                    // Query stats
                    let total_queries = usages.len();
                    let rls_scoped = usages.iter().filter(|u| u.has_rls).count();
                    let on_rls_tables = usages.iter().filter(|u| build_schema.is_rls_table(&u.table)).count();

                    println!("  {} {} queries scanned in {}",
                        "✓".green(), total_queries, src
                    );

                    // Schema validation results
                    if schema_errors.is_empty() {
                        println!("  {} All queries valid against schema", "✓".green());
                    } else {
                        println!("  {} {} schema error(s):", "✗".red(), schema_errors.len());
                        for err in &schema_errors {
                            println!("    {}", err.red());
                        }
                    }

                    // RLS audit results
                    if on_rls_tables > 0 {
                        let coverage = if on_rls_tables > 0 {
                            (rls_scoped as f64 / on_rls_tables as f64 * 100.0) as u32
                        } else {
                            100
                        };

                        println!();
                        println!("  {} RLS Coverage: {}/{} queries scoped ({}%)",
                            if rls_warnings.is_empty() { "✓".green() } else { "⚠".yellow() },
                            rls_scoped,
                            on_rls_tables,
                            if coverage == 100 { format!("{}", coverage).green() } else { format!("{}", coverage).yellow() }
                        );

                        if !rls_warnings.is_empty() {
                            println!();
                            println!("  {} {} unscoped query(ies) on RLS tables:", "⚠".yellow(), rls_warnings.len());
                            for warn in &rls_warnings {
                                // Extract file:line from warning for clean display
                                println!("    {}", warn.yellow());
                            }
                        }
                    }
                }
            }

            Ok(())
        }
        Err(e) => {
            println!("{} {}", "✗ Schema validation failed:".red().bold(), e);
            Err(anyhow::anyhow!("Schema is invalid"))
        }
    }
}

/// Validate a migration between two schemas.
pub fn check_migration(old_path: &str, new_path: &str) -> Result<()> {
    // Load old schema
    let old_content = std::fs::read_to_string(old_path)
        .map_err(|e| anyhow::anyhow!("Failed to read old schema '{}': {}", old_path, e))?;
    let old_schema = parse_qail(&old_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;

    // Load new schema
    let new_content = std::fs::read_to_string(new_path)
        .map_err(|e| anyhow::anyhow!("Failed to read new schema '{}': {}", new_path, e))?;
    let new_schema = parse_qail(&new_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

    println!("{}", "✓ Both schemas are valid".green().bold());

    // Compute diff
    let cmds = diff_schemas(&old_schema, &new_schema);

    if cmds.is_empty() {
        println!(
            "{}",
            "✓ No migration needed - schemas are identical".green()
        );
        return Ok(());
    }

    println!(
        "{} {} operation(s)",
        "Migration preview:".cyan().bold(),
        cmds.len()
    );

    // Classify operations by safety
    let mut safe_ops = 0;
    let mut reversible_ops = 0;
    let mut destructive_ops = 0;

    for cmd in &cmds {
        match cmd.action {
            Action::Make | Action::Alter | Action::Index => safe_ops += 1,
            Action::Set | Action::Mod => reversible_ops += 1,
            Action::Drop | Action::AlterDrop | Action::DropIndex => destructive_ops += 1,
            _ => {}
        }
    }

    if safe_ops > 0 {
        println!(
            "  {} {} safe operation(s) (CREATE TABLE, ADD COLUMN, CREATE INDEX)",
            "✓".green(),
            safe_ops
        );
    }
    if reversible_ops > 0 {
        println!(
            "  {} {} reversible operation(s) (UPDATE, RENAME)",
            "⚠️ ".yellow(),
            reversible_ops
        );
    }
    if destructive_ops > 0 {
        println!(
            "  {} {} destructive operation(s) (DROP)",
            "⚠️ ".red(),
            destructive_ops
        );
        println!(
            "    {} Review carefully before applying!",
            "⚠ WARNING:".red().bold()
        );
    }

    Ok(())
}

/// Compare two schema .qail files and output migration commands.
pub fn diff_schemas_cmd(
    old_path: &str,
    new_path: &str,
    format: OutputFormat,
    dialect: Dialect,
) -> Result<()> {
    println!(
        "{} {} → {}",
        "Diffing:".cyan(),
        old_path.yellow(),
        new_path.yellow()
    );

    // Load old schema
    let old_content = std::fs::read_to_string(old_path)
        .map_err(|e| anyhow::anyhow!("Failed to read old schema '{}': {}", old_path, e))?;
    let old_schema = parse_qail(&old_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;

    // Load new schema
    let new_content = std::fs::read_to_string(new_path)
        .map_err(|e| anyhow::anyhow!("Failed to read new schema '{}': {}", new_path, e))?;
    let new_schema = parse_qail(&new_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

    // Compute diff
    let cmds = diff_schemas(&old_schema, &new_schema);

    if cmds.is_empty() {
        println!("{}", "No changes detected.".green());
        return Ok(());
    }

    println!("{} {} migration command(s):", "Found:".green(), cmds.len());
    println!();

    match format {
        OutputFormat::Sql => {
            for cmd in &cmds {
                println!("{};", cmd.to_sql_with_dialect(dialect));
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&cmds)?);
        }
        OutputFormat::Pretty => {
            for (i, cmd) in cmds.iter().enumerate() {
                let class = classify_migration(cmd);
                let class_str = match class {
                    MigrationClass::Reversible => "reversible".green(),
                    MigrationClass::DataLosing => "data-losing".red(),
                    MigrationClass::Irreversible => "irreversible".red().bold(),
                };
                println!(
                    "{} {} {}",
                    format!("{}.", i + 1).cyan(),
                    format!("{}", cmd.action).yellow(),
                    cmd.table.white()
                );
                println!("   {}", cmd.to_sql_with_dialect(dialect).dimmed());
                println!("   Class: {}", class_str);
            }
        }
    }

    Ok(())
}

/// Live drift detection: introspect live DB as "old", diff against .qail file as "new".
/// Usage: `qail diff _ new.qail --live --url postgresql://...`
pub async fn diff_live(
    db_url: &str,
    new_path: &str,
    format: OutputFormat,
    dialect: Dialect,
) -> Result<()> {
    use qail_pg::driver::PgDriver;

    println!(
        "{} {} → {}",
        "Drift detection:".cyan().bold(),
        "[live DB]".yellow(),
        new_path.yellow()
    );

    // Step 1: Connect and introspect live schema
    println!("  {} Introspecting live database...", "→".dimmed());
    let mut driver = PgDriver::connect_url(db_url).await
        .map_err(|e| anyhow::anyhow!("Connection failed: {}", e))?;
    let live_schema = crate::shadow::introspect_schema(&mut driver).await?;
    println!(
        "    {} tables, {} indexes introspected",
        live_schema.tables.len().to_string().green(),
        live_schema.indexes.len().to_string().green()
    );

    // Step 2: Parse target schema file
    let new_content = std::fs::read_to_string(new_path)
        .map_err(|e| anyhow::anyhow!("Failed to read schema '{}': {}", new_path, e))?;
    let new_schema = parse_qail(&new_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse schema: {}", e))?;

    // Step 3: Diff live → target
    let cmds = diff_schemas(&live_schema, &new_schema);

    if cmds.is_empty() {
        println!("\n{}", "✅ No drift detected — live DB matches schema file.".green().bold());
        return Ok(());
    }

    println!(
        "\n{} {} drift(s) detected:\n",
        "⚠️".yellow(),
        cmds.len().to_string().red().bold()
    );

    match format {
        OutputFormat::Sql => {
            for cmd in &cmds {
                println!("{};", cmd.to_sql_with_dialect(dialect));
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&cmds)?);
        }
        OutputFormat::Pretty => {
            for (i, cmd) in cmds.iter().enumerate() {
                let class = classify_migration(cmd);
                let class_str = match class {
                    MigrationClass::Reversible => "reversible".green(),
                    MigrationClass::DataLosing => "data-losing".red(),
                    MigrationClass::Irreversible => "irreversible".red().bold(),
                };
                println!(
                    "{} {} {}",
                    format!("{}.", i + 1).cyan(),
                    format!("{}", cmd.action).yellow(),
                    cmd.table.white()
                );
                println!("   {}", cmd.to_sql_with_dialect(dialect).dimmed());
                println!("   Class: {}", class_str);
            }
        }
    }

    Ok(())
}
