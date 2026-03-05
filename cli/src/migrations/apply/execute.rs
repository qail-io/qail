//! Main apply entry point — migrate_apply.

use super::backfill::{enforce_contract_safety, parse_backfill_spec, run_chunked_backfill};
use super::codegen::parse_qail_to_sql;
use super::discovery::{discover_migrations, phase_rank};
use super::types::{ApplyPhase, BackfillRun, MigrateDirection, MigrationFile, MigrationPhase};
use crate::colors::*;
use crate::migrations::{
    MigrationReceipt, ensure_migration_table, now_epoch_ms, runtime_actor, runtime_git_sha,
    write_migration_receipt,
};
use crate::util::parse_pg_url;
use anyhow::{Context, Result, bail};
use qail_core::prelude::Qail;
use std::fs;

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
    let migrations_dir = crate::migrations::resolve_deltas_dir(false)?;

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
                    MigrationPhase::Contract => &[MigrationPhase::Expand, MigrationPhase::Backfill],
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
