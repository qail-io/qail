//! Apply migrations from migrations/ folder
//!
//! Reads `.qail` migration files in order and executes them against the database.
//! Tracks applied migrations in `_qail_migrations` table.

use crate::colors::*;
use anyhow::{Context, Result, anyhow, bail};
use qail_core::analyzer::{CodebaseScanner, QueryType};
use qail_core::migrate::parse_qail;
use qail_core::migrate::schema::{FkAction, GrantAction};
use qail_core::parser::schema::Schema;
use qail_core::prelude::Qail;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::migrations::{
    MigrationReceipt, ensure_migration_table, now_epoch_ms, runtime_actor, runtime_git_sha,
    write_migration_receipt,
};
use crate::util::parse_pg_url;

/// Apply filter for phased migration execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyPhase {
    All,
    Expand,
    Backfill,
    Contract,
}

impl ApplyPhase {
    fn allows(self, phase: MigrationPhase) -> bool {
        match self {
            Self::All => true,
            Self::Expand => phase == MigrationPhase::Expand,
            Self::Backfill => phase == MigrationPhase::Backfill,
            Self::Contract => phase == MigrationPhase::Contract,
        }
    }
}

impl std::fmt::Display for ApplyPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => write!(f, "all"),
            Self::Expand => write!(f, "expand"),
            Self::Backfill => write!(f, "backfill"),
            Self::Contract => write!(f, "contract"),
        }
    }
}

/// Expand/Backfill/Contract phase for a discovered migration file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MigrationPhase {
    Expand = 0,
    Backfill = 1,
    Contract = 2,
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expand => write!(f, "expand"),
            Self::Backfill => write!(f, "backfill"),
            Self::Contract => write!(f, "contract"),
        }
    }
}

/// A discovered migration, from either flat or subdirectory layout.
struct MigrationFile {
    /// Group key (timestamp/name without phase suffix)
    group_key: String,
    /// Sort key (directory/file name prefix)
    sort_key: String,
    /// Display name
    display_name: String,
    /// Full path to the .qail file
    path: PathBuf,
    /// Workflow phase this file belongs to
    phase: MigrationPhase,
}

#[derive(Debug, Clone)]
struct BackfillSpec {
    table: String,
    pk_column: String,
    set_clause: String,
    where_clause: Option<String>,
    chunk_size: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct BackfillRun {
    resumed: bool,
    rows_updated: i64,
    chunks: i64,
}

const BACKFILL_CHECKPOINT_TABLE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS _qail_backfill_checkpoints (
    migration_version varchar(255) primary key,
    table_name varchar(255) not null,
    pk_column varchar(255) not null,
    last_pk bigint not null default 0,
    chunk_size integer not null,
    rows_processed bigint not null default 0,
    started_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    finished_at timestamptz
)
"#;

fn phase_rank(phase: MigrationPhase) -> u8 {
    match phase {
        MigrationPhase::Expand => 0,
        MigrationPhase::Backfill => 1,
        MigrationPhase::Contract => 2,
    }
}

fn detect_phase(name: &str) -> MigrationPhase {
    let lower = name.to_ascii_lowercase();
    if lower.contains("contract") {
        MigrationPhase::Contract
    } else if lower.contains("backfill") {
        MigrationPhase::Backfill
    } else {
        MigrationPhase::Expand
    }
}

fn normalize_group_key(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    let mut base = lower
        .trim_end_matches(".qail")
        .trim_end_matches(".up")
        .trim_end_matches(".down")
        .to_string();
    for token in [
        ".expand", ".backfill", ".contract",
        "_expand", "_backfill", "_contract",
        "-expand", "-backfill", "-contract",
    ] {
        if let Some(stripped) = base.strip_suffix(token) {
            base = stripped.to_string();
        }
    }
    base
}

fn quote_ident(ident: &str) -> String {
    ident
        .split('.')
        .map(|p| format!("\"{}\"", p.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

fn is_valid_ident(ident: &str) -> bool {
    let mut parts = ident.split('.');
    let mut seen = false;
    for part in &mut parts {
        seen = true;
        if part.is_empty()
            || !part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || !part
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        {
            return false;
        }
    }
    seen
}

fn unquote_sql_ident(token: &str) -> String {
    let cleaned = token
        .trim()
        .trim_end_matches(',')
        .trim_end_matches(';')
        .trim_matches('"')
        .trim_matches('`')
        .to_string();
    cleaned
        .split('.')
        .next_back()
        .unwrap_or(cleaned.as_str())
        .trim_matches('"')
        .trim_matches('`')
        .to_ascii_lowercase()
}

fn parse_drop_targets(sql: &str) -> (Vec<String>, Vec<(String, String)>) {
    let mut tables = Vec::new();
    let mut columns = Vec::new();

    for stmt in sql.split(';') {
        let normalized = stmt.replace(['\n', '\t'], " ");
        let tokens: Vec<String> = normalized
            .split_whitespace()
            .map(|t| t.trim().to_string())
            .collect();
        if tokens.is_empty() {
            continue;
        }
        let upper: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();

        if upper.len() >= 3 && upper[0] == "DROP" && upper[1] == "TABLE" {
            let mut idx = 2usize;
            if upper.get(idx).is_some_and(|t| t == "IF")
                && upper.get(idx + 1).is_some_and(|t| t == "EXISTS")
            {
                idx += 2;
            }
            if let Some(name) = tokens.get(idx) {
                tables.push(unquote_sql_ident(name));
            }
            continue;
        }

        if upper.len() >= 6 && upper[0] == "ALTER" && upper[1] == "TABLE" {
            let mut table_idx = 2usize;
            if upper.get(table_idx).is_some_and(|t| t == "ONLY") {
                table_idx += 1;
            }
            let Some(table_token) = tokens.get(table_idx) else {
                continue;
            };
            let table = unquote_sql_ident(table_token);

            let drop_idx = upper.iter().position(|t| t == "DROP");
            let col_idx = upper.iter().position(|t| t == "COLUMN");
            if let (Some(d_idx), Some(c_idx)) = (drop_idx, col_idx)
                && d_idx + 1 == c_idx
            {
                let mut idx = c_idx + 1;
                if upper.get(idx).is_some_and(|t| t == "IF")
                    && upper.get(idx + 1).is_some_and(|t| t == "EXISTS")
                {
                    idx += 2;
                }
                if let Some(col_token) = tokens.get(idx) {
                    columns.push((table, unquote_sql_ident(col_token)));
                }
            }
        }
    }

    (tables, columns)
}

/// Discover migration files in both flat and subdirectory layouts.
///
/// Supported layouts:
///   Flat:   `migrations/001_name.up.qail`
///   Subdir: `migrations/20251207000000_name/up.qail`
///
/// Raw `.sql` files are rejected to enforce the type-safe barrier.
fn discover_migrations(
    migrations_dir: &Path,
    direction: MigrateDirection,
) -> Result<Vec<MigrationFile>> {
    let suffix = match direction {
        MigrateDirection::Up => "up",
        MigrateDirection::Down => "down",
    };

    let mut migrations = Vec::new();

    for entry in fs::read_dir(migrations_dir)?.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy().to_string();

        if path.is_dir() {
            // Subdirectory layout:
            //   legacy:   <dir>/up.qail
            //   phased:   <dir>/expand.qail, backfill.qail, contract.qail
            if matches!(direction, MigrateDirection::Up) {
                let phased = [
                    ("expand.qail", MigrationPhase::Expand),
                    ("backfill.qail", MigrationPhase::Backfill),
                    ("contract.qail", MigrationPhase::Contract),
                ];
                let mut has_phased_files = false;

                for (filename, phase) in phased {
                    let qail_file = path.join(filename);
                    let sql_file = path.join(filename.replace(".qail", ".sql"));
                    if sql_file.exists() && !qail_file.exists() {
                        eprintln!(
                            "  {} {}/{} found but .sql is not supported — convert to .qail",
                            "⚠".yellow(),
                            name_str,
                            filename.replace(".qail", ".sql")
                        );
                    }
                    if qail_file.exists() {
                        has_phased_files = true;
                        migrations.push(MigrationFile {
                            group_key: name_str.clone(),
                            sort_key: format!("{}/{}", name_str, filename),
                            display_name: format!("{}/{}", name_str, filename),
                            path: qail_file,
                            phase,
                        });
                    }
                }

                // Legacy fallback when phased files do not exist
                if !has_phased_files {
                    let qail_file = path.join("up.qail");
                    let sql_file = path.join("up.sql");
                    if sql_file.exists() && !qail_file.exists() {
                        eprintln!(
                            "  {} {}/up.sql found but .sql is not supported — convert to .qail",
                            "⚠".yellow(),
                            name_str
                        );
                        continue;
                    }
                    if qail_file.exists() {
                        migrations.push(MigrationFile {
                            group_key: name_str.clone(),
                            sort_key: name_str.clone(),
                            display_name: format!("{}/up.qail", name_str),
                            path: qail_file,
                            phase: MigrationPhase::Expand,
                        });
                    }
                }
            } else {
                // Down direction: keep single rollback file
                let qail_file = path.join(format!("{}.qail", suffix));
                let sql_file = path.join(format!("{}.sql", suffix));
                if sql_file.exists() && !qail_file.exists() {
                    eprintln!(
                        "  {} {}/{}.sql found but .sql is not supported — convert to .qail",
                        "⚠".yellow(),
                        name_str,
                        suffix
                    );
                    continue;
                }
                if qail_file.exists() {
                    migrations.push(MigrationFile {
                        group_key: name_str.clone(),
                        sort_key: name_str.clone(),
                        display_name: format!("{}/{}.qail", name_str, suffix),
                        path: qail_file,
                        phase: detect_phase(&name_str),
                    });
                }
            }
        } else if path.is_file() {
            // Flat layout: NNN_name.up.qail / NNN_name.down.qail
            let flat_suffix = format!(".{}.qail", suffix);
            if name_str.ends_with(&flat_suffix) {
                let group_name = name_str.trim_end_matches(&flat_suffix);
                migrations.push(MigrationFile {
                    group_key: normalize_group_key(group_name),
                    sort_key: name_str.clone(),
                    display_name: name_str.clone(),
                    path: path.clone(),
                    phase: detect_phase(&name_str),
                });
            } else if name_str.ends_with(&format!(".{}.sql", suffix)) {
                eprintln!(
                    "  {} {} — .sql migrations are not supported, convert to .qail",
                    "⚠".yellow(),
                    name_str
                );
            }
        }
    }

    // Sort by group key + phase order to enforce expand -> backfill -> contract.
    migrations.sort_by(|a, b| {
        a.group_key
            .cmp(&b.group_key)
            .then_with(|| phase_rank(a.phase).cmp(&phase_rank(b.phase)))
            .then_with(|| a.sort_key.cmp(&b.sort_key))
    });

    Ok(migrations)
}

/// Apply all pending migrations from the migrations/ folder.
///
/// Tracks applied migrations in `_qail_migrations` table so re-running
/// is safe (idempotent). Skips migrations that have already been applied.
pub async fn migrate_apply(
    url: &str,
    direction: MigrateDirection,
    phase_filter: ApplyPhase,
    codebase: Option<&str>,
    allow_contract_with_references: bool,
    backfill_chunk_size: usize,
) -> Result<()> {
    let migrations_dir = super::resolve_deltas_dir(false)?;

    let discovered = discover_migrations(&migrations_dir, direction)?;
    let migrations: Vec<MigrationFile> = discovered
        .into_iter()
        .filter(|m| {
            if matches!(direction, MigrateDirection::Down) {
                return true;
            }
            phase_filter.allows(m.phase)
        })
        .collect();

    if migrations.is_empty() {
        let suffix = match direction {
            MigrateDirection::Up => "up.qail",
            MigrateDirection::Down => "down.qail",
        };
        println!(
            "{} No {} migrations found for phase '{}'",
            "!".yellow(),
            suffix,
            phase_filter
        );
        return Ok(());
    }

    println!(
        "{} Found {} migration file(s) (phase: {})\n",
        "→".cyan(),
        migrations.len(),
        phase_filter.to_string().yellow(),
    );

    // Connect to database
    let (host, port, user, password, database) = parse_pg_url(url)?;
    let mut pg = if let Some(password) = password {
        qail_pg::PgDriver::connect_with_password(&host, port, &user, &database, &password).await?
    } else {
        qail_pg::PgDriver::connect(&host, port, &user, &database).await?
    };

    println!("{} Connected to {}", "✓".green(), database.cyan());

    // Bootstrap migration tracking table
    ensure_migration_table(&mut pg)
        .await
        .context("Failed to create _qail_migrations table")?;

    // Query already-applied migration versions
    let status_cmd = Qail::get("_qail_migrations").columns(vec!["version"]);

    let applied_versions: Vec<String> = match pg.query_ast(&status_cmd).await {
        Ok(result) => result
            .rows
            .iter()
            .filter_map(|row| row.first().and_then(|v| v.clone()))
            .collect(),
        Err(_) => Vec::new(), // Table may not exist yet
    };

    // Phase prerequisite check: when running --phase backfill or --phase contract,
    // verify that earlier phases for each group have already been applied.
    if matches!(direction, MigrateDirection::Up)
        && !matches!(phase_filter, ApplyPhase::All | ApplyPhase::Expand)
    {
        let all_discovered = discover_migrations(&migrations_dir, direction)?;
        // Group migrations by group_key and check prerequisites
        let mut groups: std::collections::BTreeMap<String, Vec<&MigrationFile>> =
            std::collections::BTreeMap::new();
        for m in &all_discovered {
            groups.entry(m.group_key.clone()).or_default().push(m);
        }

        for mig in &migrations {
            if let Some(group_files) = groups.get(&mig.group_key) {
                // Already applied — no need to check prerequisites
                if applied_versions.iter().any(|v| v == &mig.display_name) {
                    continue;
                }

                let required_phases: &[MigrationPhase] = match mig.phase {
                    MigrationPhase::Backfill => &[MigrationPhase::Expand],
                    MigrationPhase::Contract => {
                        &[MigrationPhase::Expand, MigrationPhase::Backfill]
                    }
                    MigrationPhase::Expand => &[],
                };

                for &req_phase in required_phases {
                    // Check if there are files for this prerequisite phase in the group
                    let prereq_files: Vec<&&MigrationFile> = group_files
                        .iter()
                        .filter(|f| f.phase == req_phase)
                        .collect();

                    for prereq in &prereq_files {
                        if !applied_versions.iter().any(|v| v == &prereq.display_name) {
                            bail!(
                                "Phase prerequisite not met for '{}': \
                                 {} phase '{}' has not been applied yet. \
                                 Run --phase {} first.",
                                mig.display_name,
                                req_phase,
                                prereq.display_name,
                                req_phase
                            );
                        }
                    }
                }
            }
        }
    }

    // Apply each pending migration
    let mut applied = 0;
    let mut skipped = 0;
    let mut current_phase = MigrationPhase::Expand;
    let mut current_group = String::new();

    for mig in &migrations {
        if mig.group_key != current_group {
            current_group = mig.group_key.clone();
            current_phase = MigrationPhase::Expand;
        }

        if matches!(direction, MigrateDirection::Up)
            && phase_rank(mig.phase) < phase_rank(current_phase)
        {
            bail!(
                "Phase ordering violation at {}: found '{}' after '{}'. Expected expand -> backfill -> contract.",
                mig.display_name,
                mig.phase,
                current_phase
            );
        }
        current_phase = mig.phase;

        // Use display_name as the migration version key
        if applied_versions.iter().any(|v| v == &mig.display_name) {
            println!(
                "  {} {} {}",
                "‒".dimmed(),
                mig.display_name.dimmed(),
                "(already applied)".dimmed()
            );
            skipped += 1;
            continue;
        }

        print!(
            "  {} {} [{}]... ",
            "→".cyan(),
            mig.display_name,
            mig.phase.to_string().yellow()
        );

        let content = fs::read_to_string(&mig.path)
            .context(format!("Failed to read {}", mig.path.display()))?;

        let mut risk_summary = format!(
            "source=folder_apply;direction={};phase={}",
            match direction {
                MigrateDirection::Up => "up",
                MigrateDirection::Down => "down",
            },
            mig.phase
        );

        // Parse .qail content and generate SQL
        let sql = parse_qail_to_sql(&content);
        let started_ms = now_epoch_ms();

        let (executed_sql_for_receipt, checksum_input, backfill_result) =
            if matches!(direction, MigrateDirection::Up) && mig.phase == MigrationPhase::Backfill {
                if let Some(spec) = parse_backfill_spec(&content, backfill_chunk_size)? {
                    let backfill_result = run_chunked_backfill(&mut pg, &mig.display_name, &spec)
                        .await
                        .with_context(|| {
                            format!("Failed to run chunked backfill {}", mig.display_name)
                        })?;
                    risk_summary.push_str(&format!(
                        ";chunked_backfill=true;rows_updated={};chunks={};resumed={}",
                        backfill_result.rows_updated,
                        backfill_result.chunks,
                        backfill_result.resumed
                    ));
                    (content.clone(), content.clone(), backfill_result)
                } else {
                    let sql = sql.context("Failed to parse backfill migration as QAIL")?;
                    pg.execute_raw(&sql)
                        .await
                        .context(format!("Failed to execute migration {}", mig.display_name))?;
                    risk_summary.push_str(";chunked_backfill=false");
                    (sql.clone(), sql, BackfillRun::default())
                }
            } else {
                let sql = sql.context("Failed to parse migration as QAIL")?;

                if matches!(direction, MigrateDirection::Up)
                    && mig.phase == MigrationPhase::Contract
                {
                    enforce_contract_safety(
                        &mig.display_name,
                        &sql,
                        codebase,
                        allow_contract_with_references,
                    )?;
                }

                pg.execute_raw(&sql)
                    .await
                    .context(format!("Failed to execute migration {}", mig.display_name))?;
                (sql.clone(), sql, BackfillRun::default())
            };
        let finished_ms = now_epoch_ms();

        // Record in _qail_migrations
        let checksum = crate::time::md5_hex(&checksum_input);
        let receipt = MigrationReceipt {
            version: mig.display_name.clone(),
            name: mig.display_name.clone(),
            checksum,
            sql_up: executed_sql_for_receipt,
            git_sha: runtime_git_sha(),
            qail_version: env!("CARGO_PKG_VERSION").to_string(),
            actor: runtime_actor(),
            started_at_ms: Some(started_ms),
            finished_at_ms: Some(finished_ms),
            duration_ms: Some(finished_ms.saturating_sub(started_ms)),
            affected_rows_est: if backfill_result.rows_updated > 0 {
                Some(backfill_result.rows_updated)
            } else {
                None
            },
            risk_summary: Some(risk_summary),
            shadow_checksum: None,
        };
        write_migration_receipt(&mut pg, &receipt)
            .await
            .context(format!("Failed to record migration {}", mig.display_name))?;

        println!("{}", "✓".green());
        applied += 1;
    }

    // Summary
    if applied > 0 {
        println!(
            "\n{}",
            format!("✓ {} migration(s) applied successfully!", applied)
                .green()
                .bold()
        );
    }
    if skipped > 0 {
        println!(
            "  {} {} migration(s) already applied (skipped)",
            "‒".dimmed(),
            skipped
        );
    }
    if applied == 0 && skipped > 0 {
        println!("\n{}", "✓ Database is up to date.".green().bold());
    }
    Ok(())
}

/// Direction for migration
#[derive(Clone, Copy)]
pub enum MigrateDirection {
    Up,
    Down,
}

fn parse_backfill_spec(content: &str, default_chunk_size: usize) -> Result<Option<BackfillSpec>> {
    let mut entries = BTreeMap::<String, String>::new();

    for line in content.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("-- @backfill.") else {
            continue;
        };
        let Some((raw_key, raw_val)) = rest.split_once(':') else {
            bail!(
                "Invalid backfill directive '{}'. Expected '-- @backfill.<key>: <value>'",
                trimmed
            );
        };
        let key = raw_key.trim().to_ascii_lowercase();
        let val = raw_val.trim().to_string();
        if !val.is_empty() {
            entries.insert(key, val);
        }
    }

    if entries.is_empty() {
        return Ok(None);
    }

    // Enforce directive-only: reject files that mix directives with SQL/QAIL body.
    // Non-directive, non-comment, non-blank lines indicate a body that would be silently skipped.
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") || trimmed.starts_with('#') {
            continue;
        }
        bail!(
            "Backfill directive file must only contain `-- @backfill.*` directives and comments, \
             but found non-directive body: '{}'. Move schema/data SQL to a separate expand or \
             contract migration.",
            if trimmed.len() > 80 { &trimmed[..80] } else { trimmed }
        );
    }

    let table = entries
        .remove("table")
        .ok_or_else(|| anyhow!("Missing backfill directive: -- @backfill.table: <table>"))?;
    let pk_column = entries
        .remove("pk")
        .ok_or_else(|| anyhow!("Missing backfill directive: -- @backfill.pk: <pk_column>"))?;
    let set_clause = entries
        .remove("set")
        .ok_or_else(|| anyhow!("Missing backfill directive: -- @backfill.set: <col = expr>"))?;
    let where_clause = entries.remove("where");

    let chunk_size = if let Some(raw_chunk) = entries.remove("chunk_size") {
        raw_chunk
            .parse::<usize>()
            .map_err(|_| anyhow!("Invalid -- @backfill.chunk_size: '{}'", raw_chunk))?
    } else if let Some(raw_chunk) = entries.remove("chunk") {
        raw_chunk
            .parse::<usize>()
            .map_err(|_| anyhow!("Invalid -- @backfill.chunk: '{}'", raw_chunk))?
    } else {
        default_chunk_size.max(1)
    };

    if !entries.is_empty() {
        let unknown = entries.keys().cloned().collect::<Vec<_>>().join(", ");
        bail!("Unknown backfill directive(s): {}", unknown);
    }

    if !is_valid_ident(&table) {
        bail!("Invalid -- @backfill.table identifier '{}'", table);
    }
    if !is_valid_ident(&pk_column) {
        bail!("Invalid -- @backfill.pk identifier '{}'", pk_column);
    }
    if set_clause.trim().is_empty() {
        bail!("-- @backfill.set cannot be empty");
    }

    Ok(Some(BackfillSpec {
        table,
        pk_column,
        set_clause,
        where_clause,
        chunk_size: chunk_size.max(1),
    }))
}

async fn ensure_backfill_checkpoint_table(pg: &mut qail_pg::PgDriver) -> Result<()> {
    pg.execute_raw(BACKFILL_CHECKPOINT_TABLE_SCHEMA)
        .await
        .context("Failed to ensure _qail_backfill_checkpoints table")?;
    Ok(())
}

/// Split a potentially schema-qualified table name into (schema, table).
/// Defaults to `"public"` when no schema prefix is present.
fn split_schema_table(table: &str) -> (&str, &str) {
    match table.split_once('.') {
        Some((schema, name)) => (schema, name),
        None => ("public", table),
    }
}

async fn ensure_integer_backfill_pk(
    pg: &mut qail_pg::PgDriver,
    table: &str,
    pk_column: &str,
) -> Result<()> {
    let (schema, table_name) = split_schema_table(table);
    let schema_escaped = schema.replace('\'', "''");
    let table_escaped = table_name.replace('\'', "''");
    let pk_escaped = pk_column.replace('\'', "''");
    let sql = format!(
        r#"
        SELECT format_type(a.atttypid, a.atttypmod) AS typ
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}'
          AND c.relname = '{table}'
          AND a.attname = '{pk}'
          AND a.attnum > 0
          AND NOT a.attisdropped
        LIMIT 1
        "#,
        schema = schema_escaped,
        table = table_escaped,
        pk = pk_escaped
    );

    let rows = pg.fetch_raw(&sql).await.map_err(|e| {
        anyhow!(
            "Failed to inspect backfill PK column '{}.{}': {}",
            table,
            pk_column,
            e
        )
    })?;

    let Some(row) = rows.first() else {
        bail!(
            "Backfill PK column '{}.{}' not found in '{}' schema",
            table,
            pk_column,
            schema
        );
    };

    let typ = row.get_string(0).unwrap_or_default().to_ascii_lowercase();
    let supported = ["smallint", "integer", "bigint"];
    if !supported.iter().any(|t| typ.contains(t)) {
        bail!(
            "Backfill checkpoint runner requires integer PK (smallint/int/bigint). Found '{}.{}' type '{}'",
            table,
            pk_column,
            typ
        );
    }

    Ok(())
}

async fn run_chunked_backfill(
    pg: &mut qail_pg::PgDriver,
    migration_version: &str,
    spec: &BackfillSpec,
) -> Result<BackfillRun> {
    ensure_backfill_checkpoint_table(pg).await?;
    ensure_integer_backfill_pk(pg, &spec.table, &spec.pk_column).await?;

    let migration_escaped = migration_version.replace('\'', "''");
    let table_escaped = spec.table.replace('\'', "''");
    let pk_escaped = spec.pk_column.replace('\'', "''");

    let init_sql = format!(
        "INSERT INTO _qail_backfill_checkpoints \
         (migration_version, table_name, pk_column, chunk_size) \
         VALUES ('{mig}', '{table}', '{pk}', {chunk}) \
         ON CONFLICT (migration_version) DO NOTHING",
        mig = migration_escaped,
        table = table_escaped,
        pk = pk_escaped,
        chunk = spec.chunk_size
    );
    pg.execute_raw(&init_sql)
        .await
        .context("Failed to initialize backfill checkpoint")?;

    let status_sql = format!(
        "SELECT last_pk, rows_processed, finished_at IS NOT NULL \
         FROM _qail_backfill_checkpoints WHERE migration_version = '{}'",
        migration_escaped
    );
    let status_rows = pg
        .fetch_raw(&status_sql)
        .await
        .context("Failed to read backfill checkpoint")?;
    let Some(status_row) = status_rows.first() else {
        bail!(
            "Backfill checkpoint row missing after init for '{}'",
            migration_version
        );
    };

    let mut last_pk = status_row.get_i64(0).unwrap_or(0);
    let mut rows_updated = status_row.get_i64(1).unwrap_or(0);
    let already_finished = status_row.get_bool(2).unwrap_or(false);
    if already_finished {
        println!(
            "{}",
            format!(
                "↳ backfill checkpoint already complete (rows={})",
                rows_updated
            )
            .dimmed()
        );
        return Ok(BackfillRun {
            resumed: false,
            rows_updated,
            chunks: 0,
        });
    }

    let resumed = last_pk > 0 || rows_updated > 0;
    if resumed {
        println!(
            "{}",
            format!(
                "↳ resuming checkpoint from last_pk={} rows_done={}",
                last_pk, rows_updated
            )
            .dimmed()
        );
    }

    let table_ident = quote_ident(&spec.table);
    let pk_ident = quote_ident(&spec.pk_column);
    let where_sql = spec.where_clause.as_deref().unwrap_or("TRUE");

    let mut chunks = 0i64;
    loop {
        let chunk_sql = format!(
            r#"
            WITH batch AS (
                SELECT {pk} AS pk
                FROM {table}
                WHERE {pk} > {last_pk}
                  AND ({where_clause})
                ORDER BY {pk}
                LIMIT {chunk}
            ),
            updated AS (
                UPDATE {table} AS t
                SET {set_clause}
                FROM batch
                WHERE t.{pk} = batch.pk
                RETURNING batch.pk
            )
            SELECT COALESCE(MAX(pk), {last_pk})::bigint AS max_pk,
                   COUNT(*)::bigint AS updated_rows
            FROM updated
            "#,
            pk = pk_ident,
            table = table_ident,
            last_pk = last_pk,
            where_clause = where_sql,
            chunk = spec.chunk_size,
            set_clause = spec.set_clause,
        );

        let rows = pg
            .fetch_raw(&chunk_sql)
            .await
            .map_err(|e| anyhow!("Chunked backfill execution failed: {}", e))?;
        let Some(row) = rows.first() else {
            bail!("Chunked backfill returned no status row");
        };

        let next_pk = row.get_i64(0).unwrap_or(last_pk);
        let updated = row.get_i64(1).unwrap_or(0);
        if updated <= 0 {
            break;
        }

        last_pk = next_pk;
        rows_updated = rows_updated.saturating_add(updated);
        chunks += 1;

        let checkpoint_sql = format!(
            "UPDATE _qail_backfill_checkpoints \
             SET last_pk = {last_pk}, rows_processed = {rows}, updated_at = now() \
             WHERE migration_version = '{mig}'",
            last_pk = last_pk,
            rows = rows_updated,
            mig = migration_escaped
        );
        pg.execute_raw(&checkpoint_sql)
            .await
            .context("Failed to update backfill checkpoint")?;
    }

    let finish_sql = format!(
        "UPDATE _qail_backfill_checkpoints \
         SET finished_at = now(), updated_at = now(), rows_processed = {rows}, last_pk = {last_pk} \
         WHERE migration_version = '{mig}'",
        rows = rows_updated,
        last_pk = last_pk,
        mig = migration_escaped
    );
    pg.execute_raw(&finish_sql)
        .await
        .context("Failed to finalize backfill checkpoint")?;

    Ok(BackfillRun {
        resumed,
        rows_updated,
        chunks,
    })
}

fn enforce_contract_safety(
    migration_name: &str,
    sql: &str,
    codebase: Option<&str>,
    allow_contract_with_references: bool,
) -> Result<()> {
    let (drop_tables, drop_columns) = parse_drop_targets(sql);
    if drop_tables.is_empty() && drop_columns.is_empty() {
        return Ok(());
    }

    let Some(codebase_path) = codebase else {
        if allow_contract_with_references {
            println!(
                "{}",
                "⚠️  Skipping contract reference guard (no --codebase provided) due to --allow-contract-with-references".yellow()
            );
            return Ok(());
        }
        bail!(
            "Contract migration '{}' requires code reference checks.\n\
             Re-run with --codebase <path> or explicitly override with --allow-contract-with-references.",
            migration_name
        );
    };

    let code_path = Path::new(codebase_path);
    if !code_path.exists() {
        bail!(
            "Contract migration '{}' blocked: codebase path not found: {}",
            migration_name,
            codebase_path
        );
    }

    let scanner = CodebaseScanner::new();
    let refs = scanner.scan(code_path);

    let drop_table_set = drop_tables
        .into_iter()
        .map(|t| t.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let drop_col_set = drop_columns
        .into_iter()
        .map(|(t, c)| (t.to_ascii_lowercase(), c.to_ascii_lowercase()))
        .collect::<std::collections::HashSet<_>>();

    let mut hits = Vec::<String>::new();
    for r in refs {
        let table = r.table.to_ascii_lowercase();
        if drop_table_set.contains(&table) {
            let kind = if matches!(r.query_type, QueryType::RawSql) {
                "RAW SQL"
            } else {
                "QAIL"
            };
            hits.push(format!(
                "{}:{} [{}] references dropped table '{}': {}",
                r.file.display(),
                r.line,
                kind,
                table,
                r.snippet
            ));
            continue;
        }
        for col in &r.columns {
            let normalized_col = col.trim_matches('"').to_ascii_lowercase();
            if drop_col_set.contains(&(table.clone(), normalized_col.clone()))
                || (col == "*" && drop_col_set.iter().any(|(t, _)| t == &table))
            {
                let kind = if matches!(r.query_type, QueryType::RawSql) {
                    "RAW SQL"
                } else {
                    "QAIL"
                };
                hits.push(format!(
                    "{}:{} [{}] references dropped column '{}.{}': {}",
                    r.file.display(),
                    r.line,
                    kind,
                    table,
                    normalized_col,
                    r.snippet
                ));
            }
        }
    }

    if hits.is_empty() {
        return Ok(());
    }

    if allow_contract_with_references {
        println!(
            "{}",
            format!(
                "⚠️  Contract reference guard bypassed for '{}' with {} hit(s) due to --allow-contract-with-references",
                migration_name,
                hits.len()
            )
            .yellow()
        );
        return Ok(());
    }

    let sample = hits.into_iter().take(8).collect::<Vec<_>>().join("\n  - ");
    bail!(
        "Contract migration '{}' blocked: detected live references to dropped fields/tables.\n  - {}",
        migration_name,
        sample
    );
}

/// Parse a .qail schema file and generate SQL DDL.
///
/// Detects whether the content uses brace-based (`table foo { ... }`) or
/// paren-based (`table foo ( ... )`) format and routes to the appropriate parser.
///
/// - Brace-based: handled by `parse_qail()` + `migrate_schema_to_sql()` —
///   supports tables, indexes, functions, triggers, grants, `$$` blocks.
/// - Paren-based: handled by `Schema::parse()` + `schema.to_sql()` —
///   the established "schema.qail" format with `enable_rls` annotations.
/// - Fallback: `parse_functions_and_triggers()` for raw function/trigger blocks.
fn parse_qail_to_sql(content: &str) -> Result<String> {
    // Detect format: look for `table <name> {` vs `table <name> (`
    let uses_braces = content.lines().any(|line| {
        let trimmed = line.trim();
        trimmed.starts_with("table ") && trimmed.ends_with('{')
    });

    if uses_braces {
        // 1. Brace-based format: use the full migrate parser
        if let Ok(schema) = parse_qail(content) {
            let sql = migrate_schema_to_sql(&schema);
            if !sql.is_empty() {
                return Ok(sql);
            }
        }
    }

    // 2. Paren-based format (or brace parser failed): use Schema::parse
    match Schema::parse(content) {
        Ok(schema) => {
            if schema.tables.is_empty() && schema.policies.is_empty() && schema.indexes.is_empty() {
                return parse_functions_and_triggers(content);
            }
            Ok(schema.to_sql())
        }
        Err(_) => {
            // 3. Last resort: try brace parser even without brace detection
            //    (for files with only functions/triggers/grants)
            if !uses_braces && let Ok(schema) = parse_qail(content) {
                let sql = migrate_schema_to_sql(&schema);
                if !sql.is_empty() {
                    return Ok(sql);
                }
            }
            parse_functions_and_triggers(content)
        }
    }
}

/// Generate SQL DDL from a fully-parsed migrate Schema.
fn migrate_schema_to_sql(schema: &qail_core::migrate::schema::Schema) -> String {
    let mut parts: Vec<String> = Vec::new();

    // Extensions first
    for ext in &schema.extensions {
        parts.push(format!("CREATE EXTENSION IF NOT EXISTS \"{}\";", ext.name));
    }

    // Enum types
    for en in &schema.enums {
        let values: Vec<String> = en.values.iter().map(|v| format!("'{}'", v)).collect();
        parts.push(format!(
            "DO $$ BEGIN CREATE TYPE {} AS ENUM ({}); EXCEPTION WHEN duplicate_object THEN null; END $$;",
            en.name, values.join(", ")
        ));
    }

    // Sequences
    for seq in &schema.sequences {
        parts.push(format!("CREATE SEQUENCE IF NOT EXISTS {};", seq.name));
    }

    // Tables: CREATE without FK references (avoids dependency ordering issues)
    // FK constraints are added separately via ALTER TABLE afterward.
    let mut fk_alters: Vec<String> = Vec::new();
    let mut table_names: Vec<&String> = schema.tables.keys().collect();
    table_names.sort();
    for name in &table_names {
        let table = &schema.tables[*name];
        let mut col_defs = Vec::new();
        for col in &table.columns {
            let mut line = format!("    {} {}", col.name, col.data_type);
            if col.primary_key {
                line.push_str(" PRIMARY KEY");
            }
            if !col.nullable && !col.primary_key {
                line.push_str(" NOT NULL");
            }
            if col.unique && !col.primary_key {
                line.push_str(" UNIQUE");
            }
            if let Some(ref default) = col.default {
                line.push_str(&format!(" DEFAULT {}", default));
            }
            // Collect FK constraints for deferred ALTER TABLE
            if let Some(ref fk) = col.foreign_key {
                let mut alter = format!(
                    "ALTER TABLE {} ADD CONSTRAINT fk_{}_{} FOREIGN KEY ({}) REFERENCES {}({})",
                    name, name, col.name, col.name, fk.table, fk.column
                );
                if fk.on_delete != FkAction::NoAction {
                    alter.push_str(&format!(" ON DELETE {}", fk_action_sql(&fk.on_delete)));
                }
                alter.push(';');
                fk_alters.push(alter);
            }
            col_defs.push(line);
        }
        parts.push(format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n);",
            name,
            col_defs.join(",\n")
        ));

        // RLS: ENABLE and FORCE row-level security
        if table.enable_rls {
            parts.push(format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY;", name));
        }
        if table.force_rls {
            parts.push(format!("ALTER TABLE {} FORCE ROW LEVEL SECURITY;", name));
        }
    }

    // Deferred FK constraints (after all tables exist)
    parts.extend(fk_alters);

    // Indexes
    for idx in &schema.indexes {
        let unique = if idx.unique { " UNIQUE" } else { "" };
        parts.push(format!(
            "CREATE{} INDEX IF NOT EXISTS {} ON {} ({});",
            unique,
            idx.name,
            idx.table,
            idx.columns.join(", ")
        ));
    }

    // Functions
    for func in &schema.functions {
        let args = func.args.join(", ");
        parts.push(format!(
            "CREATE OR REPLACE FUNCTION {}({}) RETURNS {} AS $$\n{}\n$$ LANGUAGE {};",
            func.name, args, func.returns, func.body, func.language
        ));
    }

    // Triggers
    for trigger in &schema.triggers {
        let events = trigger.events.join(" OR ");
        let for_each = if trigger.for_each_row {
            "FOR EACH ROW "
        } else {
            ""
        };
        // Drop + recreate for idempotency
        parts.push(format!(
            "DROP TRIGGER IF EXISTS {} ON {};\nCREATE TRIGGER {} {} {} ON {} {}EXECUTE FUNCTION {};",
            trigger.name, trigger.table,
            trigger.name, trigger.timing, events, trigger.table, for_each, trigger.execute_function
        ));
    }

    // Grants
    for grant in &schema.grants {
        let privs: Vec<String> = grant.privileges.iter().map(|p| p.to_string()).collect();
        let action = match grant.action {
            GrantAction::Grant => "GRANT",
            GrantAction::Revoke => "REVOKE",
        };
        let prep = match grant.action {
            GrantAction::Grant => "TO",
            GrantAction::Revoke => "FROM",
        };
        parts.push(format!(
            "{} {} ON {} {} {};",
            action,
            privs.join(", "),
            grant.on_object,
            prep,
            grant.to_role
        ));
    }

    // Comments
    for comment in &schema.comments {
        use qail_core::migrate::schema::CommentTarget;
        let target_sql = match &comment.target {
            CommentTarget::Table(name) => format!("TABLE {}", name),
            CommentTarget::Column { table, column } => format!("COLUMN {}.{}", table, column),
        };
        parts.push(format!(
            "COMMENT ON {} IS '{}';",
            target_sql,
            comment.text.replace('\'', "''")
        ));
    }

    parts.join("\n\n")
}

/// Convert FkAction to SQL string
fn fk_action_sql(action: &FkAction) -> &'static str {
    match action {
        FkAction::NoAction => "NO ACTION",
        FkAction::Cascade => "CASCADE",
        FkAction::SetNull => "SET NULL",
        FkAction::SetDefault => "SET DEFAULT",
        FkAction::Restrict => "RESTRICT",
    }
}

/// Parse function and trigger definitions from .qail format
fn parse_functions_and_triggers(content: &str) -> Result<String> {
    let mut sql_parts = Vec::new();
    let mut current_block = String::new();
    let mut in_function = false;
    let mut in_trigger = false;
    let mut brace_depth = 0;

    for line in content.lines() {
        let trimmed = line.trim();

        // Skip comments
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }

        // Detect function start
        if trimmed.starts_with("function ") {
            in_function = true;
            current_block = line.to_string();
            if trimmed.contains('{') {
                brace_depth = 1;
            }
            continue;
        }

        // Detect trigger start
        if trimmed.starts_with("trigger ") {
            in_trigger = true;
            current_block = line.to_string();
            continue;
        }

        // Detect table start (for index definitions)
        if trimmed.starts_with("index ") {
            sql_parts.push(parse_index_line(trimmed)?);
            continue;
        }

        // Detect table block
        if trimmed.starts_with("table ") {
            in_function = false;
            in_trigger = false;
            // Re-parse as schema
            let table_content = extract_table_block(content, trimmed)?;
            if let Ok(schema) = Schema::parse(&table_content) {
                for table in &schema.tables {
                    sql_parts.push(table.to_ddl());
                }
            }
            continue;
        }

        // Handle function body
        if in_function {
            current_block.push('\n');
            current_block.push_str(line);

            brace_depth += line.matches('{').count();
            brace_depth -= line.matches('}').count();

            if brace_depth == 0 && trimmed.ends_with('}') {
                sql_parts.push(translate_function(&current_block)?);
                in_function = false;
                current_block.clear();
            }
            continue;
        }

        // Handle trigger line
        if in_trigger {
            current_block.push('\n');
            current_block.push_str(line);

            if trimmed.contains("execute ") {
                sql_parts.push(translate_trigger(&current_block)?);
                in_trigger = false;
                current_block.clear();
            }
            continue;
        }
    }

    if sql_parts.is_empty() {
        anyhow::bail!("Could not parse any valid QAIL statements");
    }

    Ok(sql_parts.join("\n\n"))
}

/// Parse an index line: index idx_name on table (col1, col2)
fn parse_index_line(line: &str) -> Result<String> {
    // index idx_qail_queue_poll on _qail_queue (status, id)
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 5 {
        anyhow::bail!("Invalid index syntax: {}", line);
    }

    let idx_name = parts[1];
    let table_name = parts[3];

    // Extract columns between ( and )
    if let (Some(start), Some(end)) = (line.find('('), line.find(')')) {
        let columns = &line[start..=end];
        return Ok(format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}{};",
            idx_name, table_name, columns
        ));
    }

    anyhow::bail!("Invalid index syntax: {}", line)
}

/// Extract a complete table block from content
fn extract_table_block(content: &str, start_line: &str) -> Result<String> {
    let mut result = String::new();
    let mut found = false;
    let mut brace_depth = 0;

    for line in content.lines() {
        if line.trim() == start_line || (found && brace_depth > 0) {
            found = true;
            result.push_str(line);
            result.push('\n');

            brace_depth += line.matches('{').count();
            brace_depth -= line.matches('}').count();

            if brace_depth == 0 && found {
                break;
            }
        }
    }

    Ok(result)
}

/// Translate a QAIL function block to PL/pgSQL
fn translate_function(block: &str) -> Result<String> {
    // function _qail_products_notify() returns trigger { ... }
    let mut sql = String::new();

    // Extract function name and return type
    let first_line = block.lines().next().unwrap_or("");
    let func_match = first_line
        .trim()
        .strip_prefix("function ")
        .ok_or_else(|| anyhow::anyhow!("Invalid function definition"))?;

    // Parse: name() returns type
    if let Some(returns_idx) = func_match.find(" returns ") {
        let name_part = &func_match[..returns_idx];
        let returns_part = func_match[returns_idx + 9..].trim();
        let return_type = returns_part.split_whitespace().next().unwrap_or("void");

        sql.push_str(&format!(
            "CREATE OR REPLACE FUNCTION {} RETURNS {} AS $$\n",
            name_part.trim(),
            return_type
        ));
        sql.push_str("BEGIN\n");

        // Extract body (between { and })
        if let (Some(body_start), Some(body_end)) = (block.find('{'), block.rfind('}')) {
            let body = &block[body_start + 1..body_end];
            sql.push_str(&translate_function_body(body));
        }

        sql.push_str("END;\n");
        sql.push_str("$$ LANGUAGE plpgsql;");

        return Ok(sql);
    }

    anyhow::bail!("Invalid function syntax: {}", first_line)
}

/// Translate QAIL function body to PL/pgSQL
fn translate_function_body(body: &str) -> String {
    let mut sql = String::new();

    for line in body.lines() {
        let trimmed = line.trim();

        // Skip comments
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }

        // Translate if statements
        if trimmed.starts_with("if ") {
            let condition = trimmed.strip_prefix("if ").unwrap_or("");
            let condition = condition.trim_end_matches('{').trim();
            // Replace 'and' with 'AND' for SQL
            let condition = condition.replace(" and ", " AND ");
            sql.push_str(&format!("  IF {} THEN\n", condition));
            continue;
        }

        // Handle closing brace
        if trimmed == "}" {
            sql.push_str("  END IF;\n");
            continue;
        }

        // Regular statements - indent and add
        if !trimmed.is_empty() {
            sql.push_str(&format!("    {};\n", trimmed.trim_end_matches(';')));
        }
    }

    // Add RETURN statement for trigger functions
    sql.push_str("  RETURN COALESCE(NEW, OLD);\n");

    sql
}

/// Translate a QAIL trigger definition to SQL
fn translate_trigger(block: &str) -> Result<String> {
    // trigger qail_sync_products
    //   after insert or update or delete on products
    //   for each row execute _qail_products_notify()

    let lines: Vec<&str> = block.lines().collect();
    if lines.is_empty() {
        anyhow::bail!("Empty trigger definition");
    }

    let first_line = lines[0].trim();
    let trigger_name = first_line
        .strip_prefix("trigger ")
        .ok_or_else(|| anyhow::anyhow!("Invalid trigger definition"))?
        .trim();

    // Find timing and events line
    let mut timing = "";
    let mut table = "";
    let mut function = "";

    for line in &lines[1..] {
        let trimmed = line.trim();

        if trimmed.starts_with("after ") || trimmed.starts_with("before ") {
            let parts: Vec<&str> = trimmed.split(" on ").collect();
            if parts.len() >= 2 {
                timing = parts[0];
                table = parts[1].trim();
            }
        }

        if trimmed.contains("execute ")
            && let Some(func_start) = trimmed.find("execute ")
        {
            function = &trimmed[func_start + 8..];
        }
    }

    // Build SQL with DROP IF EXISTS for idempotency
    let mut sql = format!("DROP TRIGGER IF EXISTS {} ON {};\n", trigger_name, table);
    sql.push_str(&format!(
        "CREATE TRIGGER {}\n  {} ON {}\n  FOR EACH ROW EXECUTE FUNCTION {};",
        trigger_name,
        timing.to_uppercase(),
        table,
        function.trim()
    ));

    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_booking_to_sql() {
        let input = r#"
table booking_orders {
  id                    uuid primary_key default gen_random_uuid()
  hold_id               uuid nullable
  connection_id         uuid nullable
  voyage_id             uuid nullable
  operator_id           uuid not_null
  status                text not_null default 'Draft'
  total_fare            bigint not_null
  currency              text not_null default 'IDR'
  nationality           text not_null default 'indo'
  pax_breakdown         jsonb not_null default '{}'
  contact_info          jsonb not_null default '{}'
  pricing_breakdown     jsonb nullable
  passenger_details     jsonb nullable default '[]'
  connection_snapshot   jsonb nullable
  invoice_number        text nullable unique
  booking_number        text nullable
  metadata              jsonb nullable
  user_id               uuid nullable
  agent_id              uuid nullable
  created_at            timestamptz not_null default now()
  updated_at            timestamptz not_null default now()

  enable_rls
  force_rls
}

index idx_booking_orders_operator on booking_orders (operator_id)
index idx_booking_orders_status on booking_orders (status)
index idx_booking_orders_user on booking_orders (user_id)
"#;
        let sql = parse_qail_to_sql(input).expect("parse_qail_to_sql should succeed");
        assert!(
            sql.contains("CREATE TABLE IF NOT EXISTS booking_orders"),
            "SQL should contain CREATE TABLE"
        );
        assert!(
            sql.contains("ALTER TABLE booking_orders ENABLE ROW LEVEL SECURITY"),
            "SQL should enable RLS"
        );
        assert!(
            sql.contains("ALTER TABLE booking_orders FORCE ROW LEVEL SECURITY"),
            "SQL should force RLS"
        );
        assert!(
            sql.contains("CREATE INDEX IF NOT EXISTS idx_booking_orders_operator"),
            "SQL should create indexes"
        );
        assert!(
            sql.contains("CREATE INDEX IF NOT EXISTS idx_booking_orders_status"),
            "SQL should create status index"
        );
        assert!(
            sql.contains("CREATE INDEX IF NOT EXISTS idx_booking_orders_user"),
            "SQL should create user index"
        );
    }

    #[test]
    fn test_parse_paren_based_booking() {
        let input = r#"
table orders (
    id                    uuid primary_key default gen_random_uuid(),
    operator_id           uuid,
    status                varchar not_null default 'Draft',
    total_fare            bigint not_null,
    currency              varchar not_null default 'IDR',
    pax_breakdown         jsonb not_null default '{}',
    contact_info          jsonb not_null default '{}',
    created_at            timestamptz not_null default now(),
    updated_at            timestamptz not_null default now()
) enable_rls

index idx_orders_operator on orders (operator_id)
index idx_orders_status on orders (status)
"#;
        let sql = parse_qail_to_sql(input).expect("paren-based parse should succeed");
        assert!(!sql.contains("( ("), "SQL should not have double parens");
        assert!(
            sql.contains("CREATE TABLE IF NOT EXISTS orders"),
            "SQL should contain CREATE TABLE"
        );
        assert!(
            sql.contains("ALTER TABLE orders ENABLE ROW LEVEL SECURITY"),
            "SQL should enable RLS"
        );
        assert!(
            sql.contains("CREATE INDEX IF NOT EXISTS idx_orders_operator"),
            "SQL should create indexes"
        );
    }

    #[test]
    fn test_detect_phase_from_name() {
        assert_eq!(
            detect_phase("20260101010101_add_users.expand.up.qail"),
            MigrationPhase::Expand
        );
        assert_eq!(
            detect_phase("20260101010101_users_backfill.up.qail"),
            MigrationPhase::Backfill
        );
        assert_eq!(
            detect_phase("20260101010101_contract_cleanup.up.qail"),
            MigrationPhase::Contract
        );
    }

    #[test]
    fn test_parse_drop_targets_from_sql() {
        let sql = r#"
            ALTER TABLE users DROP COLUMN old_email;
            DROP TABLE IF EXISTS audit_logs;
        "#;
        let (tables, columns) = parse_drop_targets(sql);
        assert_eq!(tables, vec!["audit_logs".to_string()]);
        assert_eq!(
            columns,
            vec![("users".to_string(), "old_email".to_string())]
        );
    }

    #[test]
    fn test_parse_backfill_spec_directives() {
        let content = r#"
-- @backfill.table: users
-- @backfill.pk: id
-- @backfill.set: name_ci = lower(name)
-- @backfill.where: name_ci IS NULL
-- @backfill.chunk_size: 2048
"#;
        let spec = parse_backfill_spec(content, 5000)
            .expect("spec parse should work")
            .expect("spec should exist");
        assert_eq!(spec.table, "users");
        assert_eq!(spec.pk_column, "id");
        assert_eq!(spec.chunk_size, 2048);
        assert!(spec.set_clause.contains("lower(name)"));
        assert_eq!(spec.where_clause.as_deref(), Some("name_ci IS NULL"));
    }

    #[test]
    fn test_parse_backfill_spec_none_when_absent() {
        let content = "table users (id serial primary_key)";
        let spec = parse_backfill_spec(content, 5000).expect("parse should succeed");
        assert!(spec.is_none());
    }

    #[test]
    fn test_backfill_directive_rejects_sql_body() {
        let content = r#"
-- @backfill.table: users
-- @backfill.pk: id
-- @backfill.set: name_ci = lower(name)

ALTER TABLE users ADD COLUMN name_ci text;
"#;
        let result = parse_backfill_spec(content, 5000);
        assert!(result.is_err(), "Should reject files mixing directives and SQL body");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("non-directive body"),
            "Error should mention non-directive body, got: {}",
            msg
        );
    }

    #[test]
    fn test_backfill_directive_allows_comments_only() {
        let content = r#"
-- Backfill name_ci for existing users
-- @backfill.table: users
-- @backfill.pk: id
-- @backfill.set: name_ci = lower(name)
-- @backfill.chunk_size: 1000
"#;
        let spec = parse_backfill_spec(content, 5000)
            .expect("should parse ok")
            .expect("should have a spec");
        assert_eq!(spec.table, "users");
        assert_eq!(spec.chunk_size, 1000);
    }

    #[test]
    fn test_normalize_group_key_underscore_variants() {
        assert_eq!(normalize_group_key("001_users_expand"), "001_users");
        assert_eq!(normalize_group_key("001_users_backfill"), "001_users");
        assert_eq!(normalize_group_key("001_users_contract"), "001_users");
    }

    #[test]
    fn test_normalize_group_key_hyphen_variants() {
        assert_eq!(normalize_group_key("001_users-expand"), "001_users");
        assert_eq!(normalize_group_key("001_users-backfill"), "001_users");
        assert_eq!(normalize_group_key("001_users-contract"), "001_users");
    }

    #[test]
    fn test_normalize_group_key_dot_variants() {
        assert_eq!(normalize_group_key("001_users.expand"), "001_users");
        assert_eq!(normalize_group_key("001_users.backfill"), "001_users");
        assert_eq!(normalize_group_key("001_users.contract"), "001_users");
    }

    #[test]
    fn test_normalize_group_key_no_phase_suffix() {
        assert_eq!(normalize_group_key("001_add_users"), "001_add_users");
        assert_eq!(normalize_group_key("002_orders"), "002_orders");
    }

    #[test]
    fn test_split_schema_table_qualified() {
        let (schema, table) = split_schema_table("analytics.events");
        assert_eq!(schema, "analytics");
        assert_eq!(table, "events");
    }

    #[test]
    fn test_split_schema_table_unqualified() {
        let (schema, table) = split_schema_table("users");
        assert_eq!(schema, "public");
        assert_eq!(table, "users");
    }

    #[test]
    fn test_backfill_directive_allows_hash_comments() {
        let content = r#"
# This is a hash-style comment
-- @backfill.table: users
-- @backfill.pk: id
-- @backfill.set: email_lower = lower(email)
# Another hash comment
"#;
        let spec = parse_backfill_spec(content, 5000)
            .expect("should parse ok with # comments")
            .expect("should have a spec");
        assert_eq!(spec.table, "users");
        assert_eq!(spec.set_clause, "email_lower = lower(email)");
    }
}
