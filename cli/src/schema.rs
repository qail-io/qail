//! Schema validation and diff operations

use crate::colors::*;
use crate::migrations::types::{MigrationClass, classify_migration};
use anyhow::Result;
use qail_core::migrate::{Schema, diff_schemas_checked, parse_qail, parse_qail_file};
use qail_core::prelude::*;
use qail_core::transpiler::Dialect;
use std::collections::BTreeSet;
use std::path::Path;

/// Output format for schema operations.
#[derive(Clone)]
pub enum OutputFormat {
    Sql,
    Json,
    Pretty,
}

fn cmds_wire_json(cmds: &[Qail], dialect: Dialect) -> serde_json::Value {
    let rows = cmds
        .iter()
        .map(|cmd| {
            serde_json::json!({
                "wire": qail_core::wire::encode_cmd_text(cmd),
                "sql": cmd.to_sql_with_dialect(dialect),
                "action": format!("{}", cmd.action),
                "table": cmd.table.clone(),
            })
        })
        .collect();
    serde_json::Value::Array(rows)
}

pub(crate) fn schema_for_live_table_index_diff(
    mut schema: Schema,
    skipped: &mut BTreeSet<&'static str>,
) -> Schema {
    macro_rules! clear_family {
        ($field:ident, $label:literal) => {
            if !schema.$field.is_empty() {
                skipped.insert($label);
                schema.$field.clear();
            }
        };
    }

    clear_family!(comments, "comments");
    clear_family!(enums, "enums");
    clear_family!(extensions, "extensions");
    clear_family!(functions, "functions");
    clear_family!(grants, "grants");
    clear_family!(policies, "policies");
    clear_family!(resources, "resources");
    clear_family!(sequences, "sequences");
    clear_family!(triggers, "triggers");
    clear_family!(views, "views");

    schema
}

fn merge_migrations_for_source_audit(
    build_schema: &mut qail_core::build::Schema,
    migrations_dir: &str,
) -> Result<usize> {
    if !Path::new(migrations_dir).exists() {
        return Ok(0);
    }

    build_schema
        .merge_migrations(migrations_dir)
        .map_err(|e| anyhow::anyhow!("Failed to merge migrations from {}: {}", migrations_dir, e))
}

fn validate_source_audit_dir(src: &str) -> Result<()> {
    let path = Path::new(src);
    if !path.exists() {
        return Err(anyhow::anyhow!("Source directory '{}' not found", src));
    }
    if !path.is_dir() {
        return Err(anyhow::anyhow!("Source path '{}' is not a directory", src));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RlsCoverageStats {
    scoped: usize,
    total_on_rls_tables: usize,
    percent: u32,
}

fn rls_coverage_stats(
    build_schema: &qail_core::build::Schema,
    usages: &[qail_core::build::QailUsage],
) -> Option<RlsCoverageStats> {
    let mut scoped = 0usize;
    let mut total_on_rls_tables = 0usize;

    for usage in usages {
        if build_schema.is_rls_table(&usage.table) {
            total_on_rls_tables += 1;
            if usage.has_rls {
                scoped += 1;
            }
        }
    }

    (total_on_rls_tables > 0).then(|| RlsCoverageStats {
        scoped,
        total_on_rls_tables,
        percent: (scoped as f64 / total_on_rls_tables as f64 * 100.0) as u32,
    })
}

/// Validate a QAIL schema file with detailed error reporting.
/// When `src_dir` is provided, also scans source for query validation + RLS audit.
pub fn check_schema(
    schema_path: &str,
    src_dir: Option<&str>,
    migrations_dir: &str,
    nplus1_deny: bool,
) -> Result<()> {
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

    let content = qail_core::schema_source::read_qail_schema_source(schema_path)
        .map_err(|e| anyhow::anyhow!("Failed to read schema source '{}': {}", schema_path, e))?;

    match parse_qail(&content) {
        Ok(schema) => {
            if let Err(validation_errors) = schema.validate() {
                return Err(anyhow::anyhow!(
                    "Schema validation failed:\n{}",
                    validation_errors.join("\n")
                ));
            }

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
                validate_source_audit_dir(src)?;
                let mut source_schema_error_count = 0usize;

                // Use build module's Schema (has rls_enabled detection)
                let mut build_schema = qail_core::build::Schema::parse(&content)
                    .map_err(|e| anyhow::anyhow!("Failed to parse schema for audit: {}", e))?;

                // Merge migrations if directory exists
                let merged = merge_migrations_for_source_audit(&mut build_schema, migrations_dir)?;
                if merged > 0 {
                    println!(
                        "  {} Merged {} schema changes from {}",
                        "✓".green(),
                        merged,
                        migrations_dir
                    );
                }

                // Show RLS-enabled tables
                let rls_tables = build_schema.rls_tables();
                if rls_tables.is_empty() {
                    println!("  {} No RLS-enabled tables detected", "ℹ".dimmed());
                } else {
                    println!(
                        "  {} {} RLS-enabled table(s): {}",
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
                    let diagnostics = qail_core::build::validate_against_schema_diagnostics(
                        &build_schema,
                        &usages,
                    );

                    // Separate schema errors from RLS warnings
                    let schema_errors: Vec<_> = diagnostics
                        .iter()
                        .filter(|d| {
                            matches!(
                                d.kind,
                                qail_core::build::ValidationDiagnosticKind::SchemaError
                            )
                        })
                        .collect();
                    source_schema_error_count = schema_errors.len();
                    let rls_warnings: Vec<_> = diagnostics
                        .iter()
                        .filter(|d| {
                            matches!(
                                d.kind,
                                qail_core::build::ValidationDiagnosticKind::RlsWarning
                            )
                        })
                        .collect();

                    // Query stats
                    let total_queries = usages.len();
                    let rls_coverage = rls_coverage_stats(&build_schema, &usages);

                    println!(
                        "  {} {} queries scanned in {}",
                        "✓".green(),
                        total_queries,
                        src
                    );

                    // Schema validation results
                    if schema_errors.is_empty() {
                        println!("  {} All queries valid against schema", "✓".green());
                    } else {
                        println!("  {} {} schema error(s):", "✗".red(), schema_errors.len());
                        for err in &schema_errors {
                            println!("    {}", err.message.red());
                        }
                    }

                    // RLS audit results
                    if let Some(coverage) = rls_coverage {
                        println!();
                        println!(
                            "  {} RLS Coverage: {}/{} queries scoped ({}%)",
                            if rls_warnings.is_empty() {
                                "✓".green()
                            } else {
                                "⚠".yellow()
                            },
                            coverage.scoped,
                            coverage.total_on_rls_tables,
                            if coverage.percent == 100 {
                                format!("{}", coverage.percent).green()
                            } else {
                                format!("{}", coverage.percent).yellow()
                            }
                        );

                        if !rls_warnings.is_empty() {
                            println!();
                            println!(
                                "  {} {} unscoped query(ies) on RLS tables:",
                                "⚠".yellow(),
                                rls_warnings.len()
                            );
                            for warn in &rls_warnings {
                                println!("    {}", warn.message.yellow());
                            }
                        }
                    }
                }

                // ── N+1 Detection ──────────────────────────────────────
                println!();
                println!("{}", "── N+1 Query Detection ──".cyan().bold());

                let diagnostics =
                    qail_core::analyzer::detect_n_plus_one_in_dir(std::path::Path::new(src));

                if diagnostics.is_empty() {
                    println!("  {} No N+1 patterns detected", "✓".green());
                } else {
                    let errors: Vec<_> = diagnostics
                        .iter()
                        .filter(|d| d.severity == qail_core::analyzer::NPlusOneSeverity::Error)
                        .collect();
                    let warnings: Vec<_> = diagnostics
                        .iter()
                        .filter(|d| d.severity == qail_core::analyzer::NPlusOneSeverity::Warning)
                        .collect();

                    if !errors.is_empty() {
                        println!("  {} {} N+1 error(s):", "✗".red(), errors.len());
                        for diag in &errors {
                            println!("    {} {}", diag.code.as_str().red(), diag);
                        }
                    }
                    if !warnings.is_empty() {
                        println!("  {} {} N+1 warning(s):", "⚠".yellow(), warnings.len());
                        for diag in &warnings {
                            println!("    {} {}", diag.code.as_str().yellow(), diag);
                        }
                    }

                    if nplus1_deny {
                        return Err(anyhow::anyhow!(
                            "N+1 detection: {} diagnostic(s) found (--nplus1-deny is set)",
                            diagnostics.len()
                        ));
                    }
                }

                if source_schema_error_count > 0 {
                    return Err(anyhow::anyhow!(
                        "Source validation: {} schema error(s) found",
                        source_schema_error_count
                    ));
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
    let old_schema = parse_qail_file(old_path)
        .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;

    // Load new schema
    let new_schema = parse_qail_file(new_path)
        .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

    println!("{}", "✓ Both schemas are valid".green().bold());

    // Compute diff
    let cmds = diff_schemas_checked(&old_schema, &new_schema)
        .map_err(|e| anyhow::anyhow!("State-based diff unsupported for this schema pair: {}", e))?;

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
            Action::Make | Action::Alter | Action::AlterAddConstraint | Action::Index => {
                safe_ops += 1
            }
            Action::Set | Action::Mod => reversible_ops += 1,
            Action::Drop | Action::AlterDrop | Action::AlterDropConstraint | Action::DropIndex => {
                destructive_ops += 1
            }
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
    let old_schema = parse_qail_file(old_path)
        .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;

    // Load new schema
    let new_schema = parse_qail_file(new_path)
        .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

    // Compute diff
    let cmds = diff_schemas_checked(&old_schema, &new_schema)
        .map_err(|e| anyhow::anyhow!("State-based diff unsupported for this schema pair: {}", e))?;

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
            println!(
                "{}",
                serde_json::to_string_pretty(&cmds_wire_json(&cmds, dialect))?
            );
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
    let mut driver = PgDriver::connect_url(db_url)
        .await
        .map_err(|e| anyhow::anyhow!("Connection failed: {}", e))?;
    let live_schema = crate::shadow::introspect_schema(&mut driver).await?;
    println!(
        "    {} tables, {} indexes introspected",
        live_schema.tables.len().to_string().green(),
        live_schema.indexes.len().to_string().green()
    );

    // Step 2: Parse target schema file
    let new_schema =
        parse_qail_file(new_path).map_err(|e| anyhow::anyhow!("Failed to parse schema: {}", e))?;

    // Step 3: Diff live → target. Live state diff is intentionally scoped to
    // tables/indexes/hints; richer object families are handled by strict migrations.
    let mut skipped_families = BTreeSet::new();
    let live_schema = schema_for_live_table_index_diff(live_schema, &mut skipped_families);
    let new_schema = schema_for_live_table_index_diff(new_schema, &mut skipped_families);
    if !skipped_families.is_empty() {
        println!(
            "    {} live diff scoped to tables/indexes; strict migrations cover other object families",
            "↷".yellow()
        );
    }

    let cmds = diff_schemas_checked(&live_schema, &new_schema)
        .map_err(|e| anyhow::anyhow!("State-based diff unsupported for this schema pair: {}", e))?;

    if cmds.is_empty() {
        if skipped_families.is_empty() {
            println!(
                "\n{}",
                "✅ No drift detected — live DB matches schema file."
                    .green()
                    .bold()
            );
        } else {
            println!(
                "\n{}",
                "✅ No table/index drift detected — non-table families skipped."
                    .green()
                    .bold()
            );
        }
        return Ok(());
    }

    println!(
        "\n{} {} table/index drift(s) detected:\n",
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
            println!(
                "{}",
                serde_json::to_string_pretty(&cmds_wire_json(&cmds, dialect))?
            );
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn usage(table: &str, has_rls: bool) -> qail_core::build::QailUsage {
        qail_core::build::QailUsage {
            file: "src/main.rs".to_string(),
            line: 1,
            column: 1,
            table: table.to_string(),
            is_dynamic_table: false,
            columns: vec!["id".to_string()],
            action: "GET".to_string(),
            related_tables: Vec::new(),
            is_cte_ref: false,
            has_rls,
            has_explicit_tenant_scope: false,
            file_uses_super_admin: false,
        }
    }

    fn unique_temp_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("qail_schema_{name}_{nanos}"))
    }

    #[test]
    fn live_table_index_diff_scope_prunes_non_table_families() {
        use qail_core::migrate::{
            Comment, EnumType, Extension, Grant, Index, Privilege, RlsPolicy, SchemaFunctionDef,
            SchemaTriggerDef, Sequence, Table, ViewDef, schema::ResourceDef,
        };

        let mut schema = Schema::new();
        schema.add_table(Table::new("users"));
        schema.add_index(Index::new(
            "idx_users_email",
            "users",
            vec!["email".to_string()],
        ));
        schema.add_comment(Comment::on_table("users", "profile rows"));
        schema.add_enum(EnumType::new("user_status", vec!["active".to_string()]));
        schema.add_extension(Extension::new("pgcrypto"));
        schema.add_function(SchemaFunctionDef::new(
            "touch_users",
            "trigger",
            "BEGIN RETURN NEW; END",
        ));
        schema.add_grant(Grant::new(vec![Privilege::Select], "users", "app_user"));
        schema.add_policy(RlsPolicy::create("users_tenant", "users"));
        schema.add_resource(ResourceDef {
            name: "avatars".to_string(),
            kind: qail_core::migrate::schema::ResourceKind::Bucket,
            provider: Some("s3".to_string()),
            properties: std::collections::HashMap::new(),
        });
        schema.add_sequence(Sequence::new("user_number_seq"));
        schema.add_trigger(SchemaTriggerDef::new(
            "touch_users_trigger",
            "users",
            "touch_users()",
        ));
        schema.add_view(ViewDef::new("active_users", "SELECT 1"));

        let mut skipped = std::collections::BTreeSet::new();
        let scoped = schema_for_live_table_index_diff(schema, &mut skipped);

        assert!(scoped.tables.contains_key("users"));
        assert_eq!(scoped.indexes.len(), 1);
        assert!(scoped.comments.is_empty());
        assert!(scoped.enums.is_empty());
        assert!(scoped.extensions.is_empty());
        assert!(scoped.functions.is_empty());
        assert!(scoped.grants.is_empty());
        assert!(scoped.policies.is_empty());
        assert!(scoped.resources.is_empty());
        assert!(scoped.sequences.is_empty());
        assert!(scoped.triggers.is_empty());
        assert!(scoped.views.is_empty());
        assert_eq!(
            skipped.into_iter().collect::<Vec<_>>(),
            vec![
                "comments",
                "enums",
                "extensions",
                "functions",
                "grants",
                "policies",
                "resources",
                "sequences",
                "triggers",
                "views",
            ]
        );
    }

    #[test]
    fn rls_coverage_counts_scoped_queries_only_on_rls_tables() {
        let schema = qail_core::build::Schema::parse(
            r#"
table orders rls {
  id uuid primary_key
}
table audit_log {
  id uuid primary_key
}
"#,
        )
        .expect("schema should parse");
        let usages = vec![
            usage("orders", true),
            usage("orders", false),
            usage("audit_log", true),
        ];

        let stats = rls_coverage_stats(&schema, &usages).expect("orders is RLS-enabled");
        assert_eq!(
            stats,
            RlsCoverageStats {
                scoped: 1,
                total_on_rls_tables: 2,
                percent: 50,
            }
        );
    }

    #[test]
    fn rls_coverage_is_absent_when_no_queries_target_rls_tables() {
        let schema = qail_core::build::Schema::parse(
            r#"
table audit_log {
  id uuid primary_key
}
"#,
        )
        .expect("schema should parse");
        let usages = vec![usage("audit_log", true)];

        assert!(rls_coverage_stats(&schema, &usages).is_none());
    }

    #[test]
    fn rls_coverage_counts_public_qualified_source_tables() {
        let schema = qail_core::build::Schema::parse(
            r#"
table orders rls {
  id uuid primary_key
}
"#,
        )
        .expect("schema should parse");
        let usages = vec![usage("orders", true), usage("public.orders", false)];

        let stats = rls_coverage_stats(&schema, &usages).expect("orders is RLS-enabled");
        assert_eq!(
            stats,
            RlsCoverageStats {
                scoped: 1,
                total_on_rls_tables: 2,
                percent: 50,
            }
        );
    }

    #[test]
    fn check_schema_fails_when_source_query_has_schema_error() {
        let dir = unique_temp_dir("source_schema_error");
        let src_dir = dir.join("src");
        fs::create_dir_all(&src_dir).expect("create source dir");
        let schema_path = dir.join("schema.qail");
        fs::write(
            &schema_path,
            "table users {\n  id uuid primary_key\n  email text\n}\n",
        )
        .expect("write schema");
        fs::write(
            src_dir.join("main.rs"),
            r#"
fn demo() {
    let _query = Qail::get("usres").columns(["id", "email"]);
}
"#,
        )
        .expect("write source");

        let err = check_schema(
            schema_path.to_str().expect("schema path should be utf8"),
            Some(src_dir.to_str().expect("source path should be utf8")),
            dir.join("migrations")
                .to_str()
                .expect("migration path should be utf8"),
            false,
        )
        .expect_err("source schema errors should fail qail check");

        assert!(err.to_string().contains("Source validation"));
        fs::remove_dir_all(&dir).expect("remove temp dir");
    }

    #[test]
    fn check_schema_fails_when_multi_column_fk_is_invalid() {
        let dir = unique_temp_dir("invalid_composite_fk");
        fs::create_dir_all(&dir).expect("create temp dir");
        let schema_path = dir.join("schema.qail");
        fs::write(
            &schema_path,
            r#"
table trips {
  route_id text
  foreign_key (route_id, schedule_id) references schedules(route_id, schedule_id)
}
"#,
        )
        .expect("write schema");

        let err = check_schema(
            schema_path.to_str().expect("schema path should be utf8"),
            None,
            dir.join("migrations")
                .to_str()
                .expect("migration path should be utf8"),
            false,
        )
        .expect_err("invalid composite FK should fail qail check");

        assert!(err.to_string().contains("Schema validation failed"));
        assert!(err.to_string().contains("non-existent table 'schedules'"));
        assert!(
            err.to_string()
                .contains("non-existent source column 'trips.schedule_id'")
        );
        fs::remove_dir_all(&dir).expect("remove temp dir");
    }

    #[test]
    fn check_schema_fails_when_source_dir_is_missing() {
        let dir = unique_temp_dir("missing_source_dir");
        fs::create_dir_all(&dir).expect("create temp dir");
        let schema_path = dir.join("schema.qail");
        fs::write(&schema_path, "table users {\n  id uuid primary_key\n}\n").expect("write schema");
        let missing_src = dir.join("missing-src");

        let err = check_schema(
            schema_path.to_str().expect("schema path should be utf8"),
            Some(missing_src.to_str().expect("source path should be utf8")),
            dir.join("migrations")
                .to_str()
                .expect("migration path should be utf8"),
            false,
        )
        .expect_err("missing explicit --src should fail");

        assert!(err.to_string().contains("Source directory"));
        fs::remove_dir_all(&dir).expect("remove temp dir");
    }

    #[test]
    fn check_schema_fails_when_source_path_is_file() {
        let dir = unique_temp_dir("source_path_file");
        fs::create_dir_all(&dir).expect("create temp dir");
        let schema_path = dir.join("schema.qail");
        let src_path = dir.join("main.rs");
        fs::write(&schema_path, "table users {\n  id uuid primary_key\n}\n").expect("write schema");
        fs::write(&src_path, r#"let q = Qail::get("users");"#).expect("write source file");

        let err = check_schema(
            schema_path.to_str().expect("schema path should be utf8"),
            Some(src_path.to_str().expect("source path should be utf8")),
            dir.join("migrations")
                .to_str()
                .expect("migration path should be utf8"),
            false,
        )
        .expect_err("file-valued explicit --src should fail");

        assert!(err.to_string().contains("not a directory"));
        fs::remove_dir_all(&dir).expect("remove temp dir");
    }

    #[test]
    fn source_audit_fails_when_migration_merge_fails() {
        let mut build_schema =
            qail_core::build::Schema::parse("table users {\n  id uuid primary_key\n}\n")
                .expect("base schema should parse");
        let dir = unique_temp_dir("bad_migration");
        let migration_dir = dir.join("001_bad");
        fs::create_dir_all(&migration_dir).expect("create migration dir");
        fs::write(migration_dir.join("up.qail"), "table {\n").expect("write invalid migration");

        let err = merge_migrations_for_source_audit(
            &mut build_schema,
            dir.to_str().expect("temp path should be utf8"),
        )
        .expect_err("invalid migration should fail source audit");

        assert!(err.to_string().contains("Failed to merge migrations"));
        fs::remove_dir_all(&dir).expect("remove temp dir");
    }
}
