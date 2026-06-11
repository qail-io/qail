//! Main apply entry point — migrate_apply.

use super::backfill::{enforce_contract_safety, parse_backfill_spec, run_chunked_backfill};
use super::codegen::{commands_to_sql, parse_qail_to_commands_strict};
use super::discovery::{discover_migrations, phase_rank};
use super::types::{ApplyPhase, BackfillRun, MigrateDirection, MigrationFile, MigrationPhase};
use crate::backup::analyze_impact;
use crate::colors::*;
use crate::migrations::risk::preflight_lock_risk;
use crate::migrations::{
    EnforcementMode, MigrationPolicy, MigrationReceipt, ReceiptSignatureStatus,
    ReceiptValidationMode, StoredMigrationReceipt, acquire_migration_lock, ensure_migration_table,
    load_migration_policy, maybe_failpoint, now_epoch_ms, runtime_actor, runtime_git_sha,
    stable_cmds_checksum, verify_stored_receipt_signature, write_migration_receipt,
};
use crate::shadow::has_verified_shadow_receipt_with_driver;
use crate::util::parse_pg_url;
use anyhow::{Context, Result, anyhow, bail};
use qail_core::ast::Value;
use qail_core::ast::{Action, Condition, Constraint, Expr, JoinKind, Operator, TableConstraint};
use qail_core::prelude::Qail;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;

#[derive(Clone, Copy)]
pub struct MigrateApplyOptions<'a> {
    pub direction: MigrateDirection,
    pub phase_filter: ApplyPhase,
    pub codebase: Option<&'a str>,
    pub allow_contract_with_references: bool,
    pub allow_destructive: bool,
    pub allow_no_shadow_receipt: bool,
    pub allow_lock_risk: bool,
    pub adopt_existing: bool,
    pub backfill_chunk_size: usize,
    pub wait_for_lock: bool,
    pub lock_timeout_secs: Option<u64>,
}

/// Apply all pending migrations from the deltas/ folder.
///
/// Tracks applied migrations in `_qail_migrations` table so re-running
/// is safe (idempotent). Skips migrations that have already been applied.
pub async fn migrate_apply(url: &str, options: MigrateApplyOptions<'_>) -> Result<()> {
    let MigrateApplyOptions {
        direction,
        phase_filter,
        codebase,
        allow_contract_with_references,
        allow_destructive,
        allow_no_shadow_receipt,
        allow_lock_risk,
        adopt_existing,
        backfill_chunk_size,
        wait_for_lock,
        lock_timeout_secs,
    } = options;

    let migrations_dir = crate::migrations::resolve_deltas_dir(false)?;
    let policy = load_migration_policy()?;

    if matches!(direction, MigrateDirection::Down) && !matches!(phase_filter, ApplyPhase::All) {
        bail!("--phase is only supported for --direction up");
    }

    let discovered = discover_migrations(&migrations_dir, direction)?;
    if matches!(direction, MigrateDirection::Up) {
        let discovered_down = discover_migrations(&migrations_dir, MigrateDirection::Down)?;
        ensure_up_down_pairing(&discovered, &discovered_down)?;
    }
    let all_discovered = discovered.clone();
    let mut migrations: Vec<MigrationFile> = discovered
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
    acquire_migration_lock(
        &mut pg,
        "migrate apply",
        wait_for_lock,
        lock_timeout_secs,
        Some(database.as_str()),
    )
    .await?;
    let enforce_shadow_receipt = if matches!(direction, MigrateDirection::Up) {
        resolve_apply_shadow_receipt_policy(&policy, allow_no_shadow_receipt)?
    } else {
        false
    };

    // Query already-applied migration versions + receipt metadata.
    let status_cmd = Qail::get("_qail_migrations").columns(vec![
        "version",
        "checksum",
        "name",
        "sql_up",
        "git_sha",
        "qail_version",
        "actor",
        "started_at_ms",
        "finished_at_ms",
        "duration_ms",
        "affected_rows_est",
        "risk_summary",
        "shadow_checksum",
        "receipt_sig",
    ]);

    let (mut applied_migrations, applied_receipts): (
        HashMap<String, String>,
        HashMap<String, StoredMigrationReceipt>,
    ) = match pg.query_ast(&status_cmd).await {
        Ok(result) => {
            let mut checksums = HashMap::new();
            let mut receipts = HashMap::new();
            for row in &result.rows {
                let Some(version) = row.first().and_then(|v| v.as_ref()).cloned() else {
                    continue;
                };

                let checksum_opt = row.get(1).and_then(|v| v.as_ref()).cloned();
                checksums.insert(version.clone(), checksum_opt.clone().unwrap_or_default());
                receipts.insert(
                    version.clone(),
                    StoredMigrationReceipt {
                        version,
                        checksum: checksum_opt,
                        name: row.get(2).and_then(|v| v.as_ref()).cloned(),
                        sql_up: row.get(3).and_then(|v| v.as_ref()).cloned(),
                        git_sha: row.get(4).and_then(|v| v.as_ref()).cloned(),
                        qail_version: row.get(5).and_then(|v| v.as_ref()).cloned(),
                        actor: row.get(6).and_then(|v| v.as_ref()).cloned(),
                        started_at_ms: parse_i64_field(row.get(7).and_then(|v| v.as_ref())),
                        finished_at_ms: parse_i64_field(row.get(8).and_then(|v| v.as_ref())),
                        duration_ms: parse_i64_field(row.get(9).and_then(|v| v.as_ref())),
                        affected_rows_est: parse_i64_field(row.get(10).and_then(|v| v.as_ref())),
                        risk_summary: row.get(11).and_then(|v| v.as_ref()).cloned(),
                        shadow_checksum: row.get(12).and_then(|v| v.as_ref()).cloned(),
                        receipt_sig: row.get(13).and_then(|v| v.as_ref()).cloned(),
                    },
                );
            }
            (checksums, receipts)
        }
        Err(e) => {
            return Err(anyhow!(
                "Failed to query applied migrations from _qail_migrations: {}",
                e
            ));
        }
    };

    if matches!(direction, MigrateDirection::Up) {
        validate_receipts_against_local(
            &all_discovered,
            &applied_migrations,
            &applied_receipts,
            policy.receipt_validation,
            backfill_chunk_size,
        )?;

        if let Some(baseline_group) = active_contract_baseline_group(&applied_migrations) {
            let before = migrations.len();
            migrations.retain(|m| m.group_key.as_str() > baseline_group.as_str());
            let skipped = before.saturating_sub(migrations.len());
            if skipped > 0 {
                println!(
                    "  {} Baseline '{}' marks {} pre-baseline migration file(s) historical",
                    "✓".green(),
                    baseline_group.cyan(),
                    skipped
                );
            }
        }

        if migrations.is_empty() {
            println!(
                "{} No pending migrations found for phase '{}'",
                "✓".green(),
                phase_filter
            );
            return Ok(());
        }
    }

    // For down-direction apply, reconcile history by deleting matching applied up versions.
    // This keeps `_qail_migrations` consistent so a later `migrate apply` (up) can re-run.
    let mut applied_up_versions_by_group: HashMap<String, Vec<String>> = HashMap::new();
    if matches!(direction, MigrateDirection::Down) {
        let discovered_up = discover_migrations(&migrations_dir, MigrateDirection::Up)?;
        ensure_up_down_pairing(&discovered_up, &migrations)?;
        for up_mig in &discovered_up {
            if applied_migrations.contains_key(&up_mig.display_name) {
                applied_up_versions_by_group
                    .entry(up_mig.group_key.clone())
                    .or_default()
                    .push(up_mig.display_name.clone());
            }
        }
    }

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
                if applied_migrations.contains_key(&mig.display_name) {
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
                        if !applied_migrations.contains_key(&prereq.display_name) {
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

        print!(
            "  {} {} [{}]... ",
            "→".cyan(),
            mig.display_name,
            mig.phase.to_string().yellow()
        );

        let versions_to_delete = if matches!(direction, MigrateDirection::Down) {
            applied_up_versions_by_group
                .get(&mig.group_key)
                .cloned()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        if matches!(direction, MigrateDirection::Down) && versions_to_delete.is_empty() {
            println!(
                "  {} {} {}",
                "‒".dimmed(),
                mig.display_name.dimmed(),
                "(group not applied; skipped)".dimmed()
            );
            skipped += 1;
            continue;
        }

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

        let started_ms = now_epoch_ms();
        let mut chunked_backfill_spec = None;
        let (cmds, executed_sql_for_receipt, receipt_checksum, legacy_receipt_checksum) =
            if matches!(direction, MigrateDirection::Up) && mig.phase == MigrationPhase::Backfill {
                if let Some(spec) = parse_backfill_spec(&content, backfill_chunk_size)? {
                    chunked_backfill_spec = Some(spec);
                    (
                        Vec::new(),
                        content.clone(),
                        crate::time::md5_hex(&content),
                        None,
                    )
                } else {
                    let cmds = parse_qail_to_commands_strict(&content)
                        .context("Failed to compile backfill migration to AST commands")?;
                    let sql = commands_to_sql(&cmds);
                    let checksums = expected_checksums_for_commands(&cmds, &sql);
                    risk_summary.push_str(";chunked_backfill=false");
                    (cmds, sql, checksums.current, checksums.legacy)
                }
            } else {
                let cmds = parse_qail_to_commands_strict(&content)
                    .context("Failed to compile migration to AST commands")?;
                let sql = commands_to_sql(&cmds);
                let checksums = expected_checksums_for_commands(&cmds, &sql);
                (cmds, sql, checksums.current, checksums.legacy)
            };

        if matches!(direction, MigrateDirection::Up)
            && let Some(stored_checksum) = applied_migrations.get(&mig.display_name)
        {
            ensure_applied_checksum_matches(
                &mig.display_name,
                stored_checksum,
                &receipt_checksum,
                legacy_receipt_checksum.as_deref(),
            )?;
            println!(
                "  {} {} {}",
                "‒".dimmed(),
                mig.display_name.dimmed(),
                "(already applied)".dimmed()
            );
            skipped += 1;
            continue;
        }

        if matches!(direction, MigrateDirection::Up) && !cmds.is_empty() && enforce_shadow_receipt {
            let planned_checksum = stable_cmds_checksum(&cmds);
            let has_receipt =
                has_verified_shadow_receipt_with_driver(&mut pg, &planned_checksum).await?;
            if !has_receipt {
                bail!(
                    "Migration blocked: no verified shadow receipt for '{}'.\n\
                     Expected checksum: {}.\n\
                     Run 'qail migrate shadow <old.qail:new.qail> --url <db>' first, \
                     or re-run apply with --allow-no-shadow-receipt.",
                    mig.display_name,
                    planned_checksum
                );
            }
            println!(
                "  {} Verified shadow receipt checksum: {}",
                "✓".green(),
                planned_checksum.cyan()
            );
        }

        if should_run_apply_lock_risk_preflight(direction, &cmds) {
            preflight_lock_risk(
                &mut pg,
                &cmds,
                allow_lock_risk,
                policy.lock_risk,
                policy.lock_risk_max_score,
            )
            .await?;
        }

        if matches!(direction, MigrateDirection::Up) && !cmds.is_empty() {
            let mut destructive_ops = Vec::<String>::new();
            for cmd in &cmds {
                let impact = analyze_impact(&mut pg, cmd).await?;
                if impact.is_destructive {
                    destructive_ops.push(format!("{} {}", impact.operation, impact.table));
                }
            }

            enforce_apply_destructive_policy(
                &mig.display_name,
                &destructive_ops,
                policy.destructive,
                allow_destructive,
            )?;
        }

        if matches!(direction, MigrateDirection::Down) && !cmds.is_empty() {
            enforce_apply_down_destructive_policy(
                &mig.display_name,
                &cmds,
                policy.destructive,
                allow_destructive,
            )?;
        }

        if matches!(direction, MigrateDirection::Up) && mig.phase == MigrationPhase::Contract {
            enforce_contract_safety(
                &mig.display_name,
                &executed_sql_for_receipt,
                codebase,
                allow_contract_with_references,
            )?;
        }

        let backfill_result = if let Some(spec) = chunked_backfill_spec {
            let backfill_result = run_chunked_backfill(&mut pg, &mig.display_name, &spec)
                .await
                .with_context(|| format!("Failed to run chunked backfill {}", mig.display_name))?;
            risk_summary.push_str(&format!(
                ";chunked_backfill=true;rows_updated={};chunks={};resumed={}",
                backfill_result.rows_updated, backfill_result.chunks, backfill_result.resumed
            ));
            backfill_result
        } else {
            BackfillRun::default()
        };

        let affected_rows_est = if backfill_result.rows_updated > 0 {
            Some(backfill_result.rows_updated)
        } else {
            None
        };
        risk_summary.push_str(&format!(
            ";allow_destructive_flag={};allow_lock_risk_flag={};adopt_existing_flag={};shadow_receipt_required={};policy_destructive={:?};policy_lock_risk={:?};policy_lock_risk_max_score={}",
            allow_destructive,
            allow_lock_risk,
            adopt_existing,
            matches!(direction, MigrateDirection::Up) && enforce_shadow_receipt,
            policy.destructive,
            policy.lock_risk,
            policy.lock_risk_max_score
        ));
        if matches!(direction, MigrateDirection::Down) {
            apply_down_commands_and_reconcile_history_atomic(
                &mut pg,
                &cmds,
                ApplyDownContext {
                    migration_name: &mig.display_name,
                    started_ms,
                    executed_sql_for_receipt,
                    checksum: receipt_checksum,
                    risk_summary,
                    versions_to_delete: versions_to_delete.as_slice(),
                    failpoint_override: None,
                },
            )
            .await
            .context(format!(
                "Failed to apply down migration {}",
                mig.display_name
            ))?;

            for version in &versions_to_delete {
                applied_migrations.remove(version);
            }
            applied_up_versions_by_group.remove(&mig.group_key);
        } else {
            apply_commands_and_record_receipt_atomic(
                &mut pg,
                &cmds,
                adopt_existing,
                ApplyReceiptContext {
                    migration_name: &mig.display_name,
                    started_ms,
                    executed_sql_for_receipt,
                    checksum: receipt_checksum.clone(),
                    risk_summary,
                    affected_rows_est,
                    failpoint_override: None,
                },
            )
            .await
            .context(format!("Failed to apply migration {}", mig.display_name))?;

            applied_migrations.insert(mig.display_name.clone(), receipt_checksum);
        }

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

fn resolve_apply_shadow_receipt_policy(
    policy: &MigrationPolicy,
    allow_no_shadow_receipt: bool,
) -> Result<bool> {
    if !policy.require_shadow_receipt {
        println!(
            "{}",
            "⚠️  Shadow receipt verification disabled by migrations.policy.require_shadow_receipt=false"
                .yellow()
        );
        return Ok(false);
    }
    if allow_no_shadow_receipt {
        if !policy.allow_no_shadow_receipt {
            bail!(
                "Migration blocked: --allow-no-shadow-receipt is disabled by migrations.policy.allow_no_shadow_receipt=false"
            );
        }
        println!(
            "{}",
            "⚠️  Skipping shadow receipt verification due to --allow-no-shadow-receipt".yellow()
        );
        return Ok(false);
    }
    Ok(true)
}

fn should_run_apply_lock_risk_preflight(direction: MigrateDirection, cmds: &[Qail]) -> bool {
    !cmds.is_empty() && matches!(direction, MigrateDirection::Up | MigrateDirection::Down)
}

fn enforce_apply_destructive_policy(
    migration_name: &str,
    destructive_ops: &[String],
    policy_mode: EnforcementMode,
    allow_destructive: bool,
) -> Result<()> {
    if destructive_ops.is_empty() {
        return Ok(());
    }

    let detail = destructive_ops
        .iter()
        .take(4)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    match policy_mode {
        EnforcementMode::Deny => bail!(
            "Migration blocked: destructive operations are disabled by migrations.policy.destructive=deny (migration '{}'; examples: {}).",
            migration_name,
            detail
        ),
        EnforcementMode::RequireFlag if !allow_destructive => bail!(
            "Migration blocked: destructive operations detected in '{}'. \
             Re-run with --allow-destructive to continue (examples: {}).",
            migration_name,
            detail
        ),
        EnforcementMode::RequireFlag => {
            println!(
                "{}",
                "⚠️  Destructive changes acknowledged via --allow-destructive".yellow()
            );
        }
        EnforcementMode::Allow => {
            println!(
                "{}",
                "⚠️  Destructive changes allowed by migrations.policy.destructive=allow".yellow()
            );
        }
    }
    Ok(())
}

fn enforce_apply_down_destructive_policy(
    migration_name: &str,
    cmds: &[Qail],
    policy_mode: EnforcementMode,
    allow_destructive: bool,
) -> Result<()> {
    let destructive_ops = obvious_destructive_ops(cmds);
    enforce_apply_destructive_policy(
        migration_name,
        &destructive_ops,
        policy_mode,
        allow_destructive,
    )
}

fn obvious_destructive_ops(cmds: &[Qail]) -> Vec<String> {
    let mut ops = Vec::new();

    for cmd in cmds {
        match cmd.action {
            Action::Drop => ops.push(format!("DROP TABLE {}", cmd.table)),
            Action::AlterDrop => {
                for col in &cmd.columns {
                    match col {
                        Expr::Named(name) => {
                            ops.push(format!("DROP COLUMN {}.{}", cmd.table, name));
                        }
                        Expr::Def { name, .. } => {
                            ops.push(format!("DROP COLUMN {}.{}", cmd.table, name));
                        }
                        _ => ops.push(format!("DROP COLUMN {}", cmd.table)),
                    }
                }
                if cmd.columns.is_empty() {
                    ops.push(format!("DROP COLUMN {}", cmd.table));
                }
            }
            Action::AlterDropConstraint => {
                let name = cmd.channel.as_deref().unwrap_or("<unknown>");
                ops.push(format!("DROP CONSTRAINT {}.{}", cmd.table, name));
            }
            Action::DropIndex => {
                let name = cmd
                    .index_def
                    .as_ref()
                    .map(|idx| idx.name.as_str())
                    .filter(|name| !name.trim().is_empty())
                    .unwrap_or(cmd.table.as_str());
                ops.push(format!("DROP INDEX {}", name));
            }
            _ => {}
        }
    }

    ops
}

async fn execute_migration_commands(
    pg: &mut qail_pg::PgDriver,
    cmds: &[Qail],
    migration_name: &str,
    adopt_existing: bool,
) -> Result<()> {
    if cmds.is_empty() {
        return Ok(());
    }

    for (idx, cmd) in cmds.iter().enumerate() {
        let savepoint = if adopt_existing {
            Some(format!("qail_apply_cmd_{}", idx + 1))
        } else {
            None
        };

        if let Some(ref sp) = savepoint {
            pg.execute(&savepoint_cmd(sp)).await.map_err(|e| {
                anyhow!(
                    "Failed to create savepoint '{}' before migration command {} in '{}': {}",
                    sp,
                    idx + 1,
                    migration_name,
                    e
                )
            })?;
        }

        if let Err(err) = pg.execute(cmd).await {
            let err_text = err.to_string();
            if adopt_existing && should_adopt_existing_error(cmd.action, &err_text) {
                if let Some(ref sp) = savepoint {
                    pg.execute(&rollback_to_savepoint_cmd(sp))
                        .await
                        .map_err(|e| {
                            anyhow!(
                                "Failed to rollback to savepoint '{}' after adopting existing object in '{}': {}",
                                sp,
                                migration_name,
                                e
                            )
                        })?;
                    pg.execute(&release_savepoint_cmd(sp)).await.map_err(|e| {
                        anyhow!(
                            "Failed to release savepoint '{}' after adopting existing object in '{}': {}",
                            sp,
                            migration_name,
                            e
                        )
                    })?;
                }
                println!(
                    "  {} Adopted existing object: action={:?} target='{}' (migration='{}')",
                    "⚠".yellow(),
                    cmd.action,
                    cmd.table.cyan(),
                    migration_name
                );
                continue;
            }
            if let Some(ref sp) = savepoint {
                let _ = pg.execute(&rollback_to_savepoint_cmd(sp)).await;
            }
            return Err(anyhow!(
                "Migration command {} failed in '{}': action={:?} table='{}' error={}",
                idx + 1,
                migration_name,
                cmd.action,
                cmd.table,
                err_text
            ));
        }

        if let Some(ref sp) = savepoint {
            pg.execute(&release_savepoint_cmd(sp)).await.map_err(|e| {
                anyhow!(
                    "Failed to release savepoint '{}' after migration command {} in '{}': {}",
                    sp,
                    idx + 1,
                    migration_name,
                    e
                )
            })?;
        }
    }

    Ok(())
}

fn savepoint_cmd(name: &str) -> Qail {
    Qail {
        action: Action::Savepoint,
        savepoint_name: Some(name.to_string()),
        ..Default::default()
    }
}

fn rollback_to_savepoint_cmd(name: &str) -> Qail {
    Qail {
        action: Action::RollbackToSavepoint,
        savepoint_name: Some(name.to_string()),
        ..Default::default()
    }
}

fn release_savepoint_cmd(name: &str) -> Qail {
    Qail {
        action: Action::ReleaseSavepoint,
        savepoint_name: Some(name.to_string()),
        ..Default::default()
    }
}

fn should_adopt_existing_error(action: Action, error_text: &str) -> bool {
    if !is_adoptable_create_action(action) {
        return false;
    }

    if let Some(code) = extract_sqlstate(error_text)
        && matches!(code, "42P07" | "42710" | "42701" | "42P06" | "42723")
    {
        return true;
    }

    let lower = error_text.to_ascii_lowercase();
    lower.contains("already exists")
        || lower.contains("already has row security enabled")
        || lower.contains("already has row security forced")
}

fn is_adoptable_create_action(action: Action) -> bool {
    matches!(
        action,
        Action::Make
            | Action::Index
            | Action::CreateMaterializedView
            | Action::CreateView
            | Action::CreateFunction
            | Action::CreateTrigger
            | Action::CreateExtension
            | Action::CreateSequence
            | Action::CreateEnum
            | Action::CreatePolicy
            | Action::AlterAddConstraint
            | Action::AlterEnableRls
            | Action::AlterForceRls
    )
}

fn extract_sqlstate(error_text: &str) -> Option<&str> {
    let start = error_text.find('[')?;
    let rest = &error_text[start + 1..];
    let end = rest.find(']')?;
    let code = &rest[..end];
    if code.len() == 5 && code.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        Some(code)
    } else {
        None
    }
}

async fn verify_applied_commands_effects(
    pg: &mut qail_pg::PgDriver,
    migration_name: &str,
    cmds: &[Qail],
) -> Result<()> {
    if cmds.is_empty() {
        return Ok(());
    }

    let mut failures = Vec::<String>::new();
    let policy_expectations = collect_policy_final_expectations(cmds);
    let constraint_expectations = collect_constraint_final_expectations(cmds);

    for cmd in cmds {
        match cmd.action {
            Action::Make => {
                if !table_exists(pg, &cmd.table).await? {
                    failures.push(format!("expected table '{}' to exist", cmd.table));
                    continue;
                }
                verify_created_table_shape(pg, cmd, &mut failures).await?;
                verify_table_constraints(pg, cmd, &mut failures).await?;
            }
            Action::Alter => {
                for column in extract_column_names(&cmd.columns) {
                    if !column_exists(pg, &cmd.table, &column).await? {
                        failures.push(format!(
                            "expected column '{}.{}' to exist",
                            cmd.table, column
                        ));
                    }
                }
                verify_table_constraints(pg, cmd, &mut failures).await?;
            }
            Action::AlterAddConstraint => {
                // Verified in final-state pass below so drop/add replacements in the
                // same migration are checked by the last command's intent.
            }
            Action::Drop if table_exists(pg, &cmd.table).await? => {
                failures.push(format!("expected table '{}' to be dropped", cmd.table));
            }
            Action::Drop => {}
            Action::Index => {
                let index_name = cmd
                    .index_def
                    .as_ref()
                    .map(|idx| idx.name.as_str())
                    .unwrap_or(cmd.table.as_str());
                let index_name = strip_optional_if_exists_prefix(index_name);
                if !index_name.is_empty() && !index_exists(pg, &index_name).await? {
                    failures.push(format!("expected index '{}' to exist", index_name));
                }
            }
            Action::DropIndex => {
                let index_name = strip_optional_if_exists_prefix(&cmd.table);
                if !index_name.is_empty() && index_exists(pg, &index_name).await? {
                    failures.push(format!("expected index '{}' to be dropped", index_name));
                }
            }
            Action::AlterDrop => {
                for column in extract_column_names(&cmd.columns) {
                    if column_exists(pg, &cmd.table, &column).await? {
                        failures.push(format!(
                            "expected column '{}.{}' to be dropped",
                            cmd.table, column
                        ));
                    }
                }
            }
            Action::AlterDropConstraint => {
                // Verified in final-state pass below so drop/add replacements in the
                // same migration are checked by the last command's intent.
            }
            Action::Mod => {
                for rename_expr in cmd.columns.iter().filter_map(|col| match col {
                    Expr::Named(raw) => Some(raw.as_str()),
                    _ => None,
                }) {
                    let Some((old_col, new_col)) = parse_rename_expr(rename_expr) else {
                        continue;
                    };

                    let old_exists = column_exists(pg, &cmd.table, old_col).await?;
                    let new_exists = column_exists(pg, &cmd.table, new_col).await?;
                    if old_exists || !new_exists {
                        failures.push(format!(
                            "expected rename '{}.{} -> {}' to be applied (old_exists={}, new_exists={})",
                            cmd.table, old_col, new_col, old_exists, new_exists
                        ));
                    }
                }
            }
            Action::AlterEnableRls => match table_rls_flags(pg, &cmd.table).await? {
                Some((enabled, _)) if enabled => {}
                Some((enabled, _)) => failures.push(format!(
                    "expected table '{}' RLS enabled (relrowsecurity={})",
                    cmd.table, enabled
                )),
                None => failures.push(format!(
                    "expected table '{}' to exist for RLS enable verification",
                    cmd.table
                )),
            },
            Action::AlterForceRls => match table_rls_flags(pg, &cmd.table).await? {
                Some((_, forced)) if forced => {}
                Some((_, forced)) => failures.push(format!(
                    "expected table '{}' FORCE RLS enabled (relforcerowsecurity={})",
                    cmd.table, forced
                )),
                None => failures.push(format!(
                    "expected table '{}' to exist for FORCE RLS verification",
                    cmd.table
                )),
            },
            Action::CreatePolicy => {
                // Verified in final-state pass below so mixed drop/create sequences in the
                // same migration are checked by the last command's intent.
            }
            Action::DropPolicy => {
                // Verified in final-state pass below so mixed drop/create sequences in the
                // same migration are checked by the last command's intent.
            }
            _ => {}
        }
    }

    for ((table, constraint_name), should_exist) in constraint_expectations {
        if constraint_name.is_empty() {
            if should_exist {
                failures.push(format!(
                    "expected named constraint on table '{}' to exist",
                    table
                ));
            }
            continue;
        }
        let exists = table_constraint_exists(pg, &table, &constraint_name).await?;
        if should_exist && !exists {
            failures.push(format!(
                "expected constraint '{}.{}' to exist",
                table, constraint_name
            ));
        }
        if !should_exist && exists {
            failures.push(format!(
                "expected constraint '{}.{}' to be dropped",
                table, constraint_name
            ));
        }
    }

    for ((table, policy_name), should_exist) in policy_expectations {
        let exists = policy_exists(pg, &table, &policy_name).await?;
        if should_exist && !exists {
            failures.push(format!(
                "expected policy '{}' on table '{}' to exist",
                policy_name, table
            ));
        }
        if !should_exist && exists {
            failures.push(format!(
                "expected policy '{}' on table '{}' to be dropped",
                policy_name, table
            ));
        }
    }

    if failures.is_empty() {
        return Ok(());
    }

    let detail = failures
        .into_iter()
        .take(8)
        .collect::<Vec<_>>()
        .join("\n  - ");
    bail!(
        "Post-apply verification failed for '{}':\n  - {}",
        migration_name,
        detail
    );
}

fn split_schema_ident(name: &str) -> (&str, &str) {
    if let Some((schema, object)) = name.rsplit_once('.') {
        let schema = schema.trim();
        let object = object.trim();
        if !schema.is_empty() && !object.is_empty() {
            return (schema, object);
        }
    }
    ("public", name.trim())
}

fn strip_optional_if_exists_prefix(name: &str) -> String {
    let tokens: Vec<&str> = name.split_whitespace().collect();
    if tokens.len() >= 3
        && tokens[0].eq_ignore_ascii_case("if")
        && tokens[1].eq_ignore_ascii_case("exists")
    {
        tokens[2..].join(" ")
    } else {
        name.trim().to_string()
    }
}

fn parse_rename_expr(raw: &str) -> Option<(&str, &str)> {
    let (left, right) = raw.split_once("->")?;
    let left = left.trim();
    let right = right.trim();
    if left.is_empty() || right.is_empty() {
        return None;
    }
    Some((left, right))
}

fn extract_column_names(columns: &[Expr]) -> Vec<String> {
    columns
        .iter()
        .filter_map(|expr| match expr {
            Expr::Named(name) => Some(name.trim().to_string()),
            Expr::Def { name, .. } => Some(name.trim().to_string()),
            _ => None,
        })
        .filter(|name| !name.is_empty())
        .collect()
}

async fn table_exists(pg: &mut qail_pg::PgDriver, table: &str) -> Result<bool> {
    let (schema, table_name) = split_schema_ident(table);
    let cmd = Qail::get("information_schema.tables")
        .column_expr(crate::util::qail_exists_projection())
        .where_eq("table_schema", schema)
        .where_eq("table_name", table_name)
        .limit(1);
    let rows = pg
        .fetch_all(&cmd)
        .await
        .with_context(|| format!("Failed table existence check for '{}'", table))?;
    Ok(!rows.is_empty())
}

fn collect_policy_final_expectations(cmds: &[Qail]) -> HashMap<(String, String), bool> {
    let mut expected = HashMap::<(String, String), bool>::new();
    for cmd in cmds {
        match cmd.action {
            Action::CreatePolicy => {
                if let Some(policy) = &cmd.policy_def {
                    expected.insert((policy.table.clone(), policy.name.clone()), true);
                }
            }
            Action::DropPolicy => {
                if let Some(policy_name) = cmd.payload.as_ref() {
                    expected.insert((cmd.table.clone(), policy_name.clone()), false);
                }
            }
            _ => {}
        }
    }
    expected
}

fn collect_constraint_final_expectations(cmds: &[Qail]) -> HashMap<(String, String), bool> {
    let mut expected = HashMap::<(String, String), bool>::new();
    for cmd in cmds {
        match cmd.action {
            Action::AlterAddConstraint => {
                let name = cmd.channel.as_deref().unwrap_or("").trim().to_string();
                expected.insert((cmd.table.clone(), name), true);
            }
            Action::AlterDropConstraint => {
                let name = cmd.channel.as_deref().unwrap_or("").trim().to_string();
                expected.insert((cmd.table.clone(), name), false);
            }
            _ => {}
        }
    }
    expected
}

async fn column_exists(pg: &mut qail_pg::PgDriver, table: &str, column: &str) -> Result<bool> {
    let (schema, table_name) = split_schema_ident(table);
    let cmd = Qail::get("information_schema.columns")
        .column_expr(crate::util::qail_exists_projection())
        .where_eq("table_schema", schema)
        .where_eq("table_name", table_name)
        .where_eq("column_name", column)
        .limit(1);
    let rows = pg.fetch_all(&cmd).await.with_context(|| {
        format!(
            "Failed column existence check for '{}.{}'",
            table_name, column
        )
    })?;
    Ok(!rows.is_empty())
}

struct LiveColumnDefinition {
    data_type: String,
    udt_name: Option<String>,
    character_maximum_length: Option<i32>,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
    datetime_precision: Option<i32>,
    nullable: bool,
}

async fn live_column_definition(
    pg: &mut qail_pg::PgDriver,
    table: &str,
    column: &str,
) -> Result<Option<LiveColumnDefinition>> {
    let (schema, table_name) = split_schema_ident(table);
    let cmd = Qail::get("information_schema.columns")
        .columns([
            "data_type",
            "udt_name",
            "is_nullable",
            "character_maximum_length",
            "numeric_precision",
            "numeric_scale",
            "datetime_precision",
        ])
        .where_eq("table_schema", schema)
        .where_eq("table_name", table_name)
        .where_eq("column_name", column)
        .limit(1);
    let rows = pg.fetch_all(&cmd).await.with_context(|| {
        format!(
            "Failed column definition check for '{}.{}'",
            table_name, column
        )
    })?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let data_type = row
        .get_string(0)
        .ok_or_else(|| anyhow!("Missing data_type for '{}.{}'", table, column))?;
    let is_nullable = row
        .get_string(2)
        .ok_or_else(|| anyhow!("Missing is_nullable for '{}.{}'", table, column))?;
    Ok(Some(LiveColumnDefinition {
        data_type,
        udt_name: row.get_string(1),
        character_maximum_length: row.get_i32(3),
        numeric_precision: row.get_i32(4),
        numeric_scale: row.get_i32(5),
        datetime_precision: row.get_i32(6),
        nullable: is_nullable.eq_ignore_ascii_case("YES"),
    }))
}

async fn verify_created_table_shape(
    pg: &mut qail_pg::PgDriver,
    cmd: &Qail,
    failures: &mut Vec<String>,
) -> Result<()> {
    for column in &cmd.columns {
        let Expr::Def {
            name,
            data_type,
            constraints,
        } = column
        else {
            continue;
        };

        let Some(live) = live_column_definition(pg, &cmd.table, name).await? else {
            failures.push(format!(
                "expected column '{}.{}' to exist for adopted table",
                cmd.table, name
            ));
            continue;
        };

        if !column_type_matches(data_type, &live) {
            failures.push(format!(
                "expected column '{}.{}' type '{}' but found '{}'",
                cmd.table, name, data_type, live.data_type
            ));
        }

        if !constraints
            .iter()
            .any(|constraint| matches!(constraint, Constraint::Nullable))
            && live.nullable
        {
            failures.push(format!(
                "expected column '{}.{}' to be NOT NULL",
                cmd.table, name
            ));
        }

        if constraints
            .iter()
            .any(|constraint| matches!(constraint, Constraint::PrimaryKey))
            && !column_has_constraint_type(pg, &cmd.table, name, "PRIMARY KEY").await?
        {
            failures.push(format!(
                "expected column '{}.{}' to be PRIMARY KEY",
                cmd.table, name
            ));
        }

        if constraints
            .iter()
            .any(|constraint| matches!(constraint, Constraint::Unique))
            && !column_has_constraint_type(pg, &cmd.table, name, "UNIQUE").await?
        {
            failures.push(format!(
                "expected column '{}.{}' to be UNIQUE",
                cmd.table, name
            ));
        }
    }

    Ok(())
}

async fn verify_table_constraints(
    pg: &mut qail_pg::PgDriver,
    cmd: &Qail,
    failures: &mut Vec<String>,
) -> Result<()> {
    for constraint in &cmd.table_constraints {
        match constraint {
            TableConstraint::PrimaryKey(columns) => {
                if !table_has_key_constraint(pg, &cmd.table, "PRIMARY KEY", columns).await? {
                    failures.push(format!(
                        "expected table '{}' to have PRIMARY KEY ({})",
                        cmd.table,
                        columns.join(", ")
                    ));
                }
            }
            TableConstraint::Unique(columns) => {
                if !table_has_key_constraint(pg, &cmd.table, "UNIQUE", columns).await? {
                    failures.push(format!(
                        "expected table '{}' to have UNIQUE ({})",
                        cmd.table,
                        columns.join(", ")
                    ));
                }
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
                deferrable,
            } => {
                let expected = ExpectedForeignKeyConstraint {
                    name: name.as_deref(),
                    columns,
                    ref_table,
                    ref_columns,
                    on_delete: on_delete.as_deref(),
                    on_update: on_update.as_deref(),
                    deferrable: deferrable.as_deref(),
                };
                if !table_has_foreign_key_constraint(pg, &cmd.table, &expected).await? {
                    failures.push(format!(
                        "expected table '{}' to have FOREIGN KEY ({}) REFERENCES {}({})",
                        cmd.table,
                        columns.join(", "),
                        ref_table,
                        ref_columns.join(", ")
                    ));
                }
            }
        }
    }

    Ok(())
}

fn column_type_matches(expected: &str, live: &LiveColumnDefinition) -> bool {
    let live_type = if live.data_type.eq_ignore_ascii_case("USER-DEFINED") {
        live.udt_name.as_deref().unwrap_or(live.data_type.as_str())
    } else {
        live.data_type.as_str()
    };
    let expected_type = normalize_column_type(expected);
    if expected_type != normalize_column_type(live_type) {
        return false;
    }

    type_modifiers_match(&expected_type, expected, live)
}

fn type_modifiers_match(
    normalized_expected_type: &str,
    expected: &str,
    live: &LiveColumnDefinition,
) -> bool {
    let Some(modifiers) = parse_type_modifiers(expected) else {
        return false;
    };

    match normalized_expected_type {
        "character varying" | "character" => match modifiers.first() {
            Some(expected_len) => live.character_maximum_length == Some(*expected_len),
            None => live.character_maximum_length.is_none(),
        },
        "numeric" => {
            if modifiers.is_empty() {
                live.numeric_precision.is_none() && live.numeric_scale.is_none()
            } else {
                let expected_precision = modifiers[0];
                let expected_scale = modifiers.get(1).copied().unwrap_or(0);
                live.numeric_precision == Some(expected_precision)
                    && live.numeric_scale == Some(expected_scale)
            }
        }
        "timestamp without time zone"
        | "timestamp with time zone"
        | "time without time zone"
        | "time with time zone" => {
            let expected_precision = modifiers.first().copied().unwrap_or(6);
            live.datetime_precision == Some(expected_precision)
        }
        _ => true,
    }
}

fn parse_type_modifiers(raw: &str) -> Option<Vec<i32>> {
    let Some(start) = raw.find('(') else {
        return Some(Vec::new());
    };
    let end = raw[start + 1..].find(')')?;
    raw[start + 1..start + 1 + end]
        .split(',')
        .map(|part| part.trim().parse::<i32>().ok())
        .collect()
}

fn normalize_constraint_part(value: &str) -> String {
    value.trim().trim_matches('"').to_ascii_lowercase()
}

fn normalized_constraint_columns(columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .map(|column| normalize_constraint_part(column))
        .collect()
}

fn constraint_columns_match(live: &[String], expected: &[String]) -> bool {
    normalized_constraint_columns(live) == normalized_constraint_columns(expected)
}

fn join_column_eq(left: &str, right: &str) -> Condition {
    Condition {
        left: Expr::Named(left.to_string()),
        op: Operator::Eq,
        value: Value::Column(right.to_string()),
        is_array_unnest: false,
    }
}

fn normalize_column_type(raw: &str) -> String {
    let mut normalized = raw.trim().to_ascii_lowercase();
    if let Some((prefix, _)) = normalized.split_once('(') {
        normalized = prefix.trim().to_string();
    }
    normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");

    match normalized.as_str() {
        "serial" | "serial4" => "integer",
        "bigserial" | "serial8" => "bigint",
        "smallserial" | "serial2" => "smallint",
        "int" | "int4" => "integer",
        "int8" => "bigint",
        "int2" => "smallint",
        "bool" => "boolean",
        "varchar" => "character varying",
        "char" | "bpchar" => "character",
        "decimal" => "numeric",
        "float8" => "double precision",
        "float4" => "real",
        "timestamptz" => "timestamp with time zone",
        "timestamp" => "timestamp without time zone",
        "timetz" => "time with time zone",
        "time" => "time without time zone",
        other => other,
    }
    .to_string()
}

async fn column_has_constraint_type(
    pg: &mut qail_pg::PgDriver,
    table: &str,
    column: &str,
    constraint_type: &str,
) -> Result<bool> {
    let (schema, table_name) = split_schema_ident(table);
    let cmd = Qail::get("information_schema.table_constraints tc")
        .column_expr(crate::util::qail_exists_projection())
        .join(
            JoinKind::Inner,
            "information_schema.key_column_usage kcu",
            "kcu.constraint_name",
            "tc.constraint_name",
        )
        .where_eq("tc.table_schema", schema)
        .where_eq("tc.table_name", table_name)
        .where_eq("tc.constraint_type", constraint_type)
        .where_eq("kcu.table_schema", schema)
        .where_eq("kcu.table_name", table_name)
        .where_eq("kcu.column_name", column)
        .limit(1);
    let rows = pg.fetch_all(&cmd).await.with_context(|| {
        format!(
            "Failed {} constraint check for '{}.{}'",
            constraint_type, table_name, column
        )
    })?;
    Ok(!rows.is_empty())
}

async fn table_constraint_exists(
    pg: &mut qail_pg::PgDriver,
    table: &str,
    constraint_name: &str,
) -> Result<bool> {
    let (schema, table_name) = split_schema_ident(table);
    let cmd = Qail::get("information_schema.table_constraints")
        .column_expr(crate::util::qail_exists_projection())
        .where_eq("table_schema", schema)
        .where_eq("table_name", table_name)
        .where_eq("constraint_name", constraint_name)
        .limit(1);
    let rows = pg.fetch_all(&cmd).await.with_context(|| {
        format!(
            "Failed constraint existence check for '{}.{}'",
            table, constraint_name
        )
    })?;
    Ok(!rows.is_empty())
}

async fn table_has_key_constraint(
    pg: &mut qail_pg::PgDriver,
    table: &str,
    constraint_type: &str,
    expected_columns: &[String],
) -> Result<bool> {
    let (schema, table_name) = split_schema_ident(table);
    let cmd = Qail::get("information_schema.table_constraints tc")
        .columns([
            "tc.constraint_name",
            "kcu.column_name",
            "kcu.ordinal_position",
        ])
        .join_conds(
            JoinKind::Inner,
            "information_schema.key_column_usage kcu",
            vec![
                join_column_eq("kcu.constraint_schema", "tc.constraint_schema"),
                join_column_eq("kcu.constraint_name", "tc.constraint_name"),
            ],
        )
        .where_eq("tc.table_schema", schema)
        .where_eq("tc.table_name", table_name)
        .where_eq("tc.constraint_type", constraint_type)
        .where_eq("kcu.table_schema", schema)
        .where_eq("kcu.table_name", table_name);

    let rows = pg.fetch_all(&cmd).await.with_context(|| {
        format!(
            "Failed {} table constraint check for '{}'",
            constraint_type, table
        )
    })?;

    let mut by_constraint = HashMap::<String, Vec<(i32, String)>>::new();
    for row in rows {
        let Some(name) = row.get_string(0) else {
            continue;
        };
        let Some(column) = row.get_string(1) else {
            continue;
        };
        let ordinal = row
            .get_string(2)
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or_default();
        by_constraint
            .entry(name)
            .or_default()
            .push((ordinal, column));
    }

    Ok(by_constraint.into_values().any(|mut columns| {
        columns.sort_by_key(|(ordinal, _)| *ordinal);
        let live_columns = columns.into_iter().map(|(_, col)| col).collect::<Vec<_>>();
        constraint_columns_match(&live_columns, expected_columns)
    }))
}

#[derive(Debug)]
struct ExpectedForeignKeyConstraint<'a> {
    name: Option<&'a str>,
    columns: &'a [String],
    ref_table: &'a str,
    ref_columns: &'a [String],
    on_delete: Option<&'a str>,
    on_update: Option<&'a str>,
    deferrable: Option<&'a str>,
}

#[derive(Debug, Default)]
struct LiveForeignKeyConstraint {
    columns: Vec<(i32, String, String)>,
    ref_schema: String,
    ref_table: String,
    delete_rule: String,
    update_rule: String,
    is_deferrable: bool,
    initially_deferred: bool,
}

async fn table_has_foreign_key_constraint(
    pg: &mut qail_pg::PgDriver,
    table: &str,
    expected: &ExpectedForeignKeyConstraint<'_>,
) -> Result<bool> {
    let (schema, table_name) = split_schema_ident(table);
    let cmd = Qail::get("information_schema.table_constraints tc")
        .columns([
            "tc.constraint_name",
            "kcu.column_name",
            "kcu.ordinal_position",
            "rkcu.table_schema",
            "rkcu.table_name",
            "rkcu.column_name",
            "rc.delete_rule",
            "rc.update_rule",
            "tc.is_deferrable",
            "tc.initially_deferred",
        ])
        .join_conds(
            JoinKind::Inner,
            "information_schema.key_column_usage kcu",
            vec![
                join_column_eq("kcu.constraint_schema", "tc.constraint_schema"),
                join_column_eq("kcu.constraint_name", "tc.constraint_name"),
            ],
        )
        .join_conds(
            JoinKind::Inner,
            "information_schema.referential_constraints rc",
            vec![
                join_column_eq("rc.constraint_schema", "tc.constraint_schema"),
                join_column_eq("rc.constraint_name", "tc.constraint_name"),
            ],
        )
        .join_conds(
            JoinKind::Inner,
            "information_schema.key_column_usage rkcu",
            vec![
                join_column_eq("rkcu.constraint_schema", "rc.unique_constraint_schema"),
                join_column_eq("rkcu.constraint_name", "rc.unique_constraint_name"),
                join_column_eq("rkcu.ordinal_position", "kcu.position_in_unique_constraint"),
            ],
        )
        .where_eq("tc.table_schema", schema)
        .where_eq("tc.table_name", table_name)
        .where_eq("tc.constraint_type", "FOREIGN KEY")
        .where_eq("kcu.table_schema", schema)
        .where_eq("kcu.table_name", table_name);
    let cmd = if let Some(name) = expected.name {
        cmd.where_eq("tc.constraint_name", name)
    } else {
        cmd
    };

    let rows = pg
        .fetch_all(&cmd)
        .await
        .with_context(|| format!("Failed foreign key table constraint check for '{}'", table))?;

    let mut by_constraint = HashMap::<String, LiveForeignKeyConstraint>::new();
    for row in rows {
        let Some(name) = row.get_string(0) else {
            continue;
        };
        let Some(column) = row.get_string(1) else {
            continue;
        };
        let ordinal = row
            .get_string(2)
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or_default();
        let ref_schema = row.get_string(3).unwrap_or_default();
        let ref_table = row.get_string(4).unwrap_or_default();
        let ref_column = row.get_string(5).unwrap_or_default();
        let delete_rule = row.get_string(6).unwrap_or_default();
        let update_rule = row.get_string(7).unwrap_or_default();
        let is_deferrable = row
            .get_string(8)
            .is_some_and(|v| v.eq_ignore_ascii_case("YES"));
        let initially_deferred = row
            .get_string(9)
            .is_some_and(|v| v.eq_ignore_ascii_case("YES"));

        let live = by_constraint.entry(name).or_default();
        live.columns.push((ordinal, column, ref_column));
        live.ref_schema = ref_schema;
        live.ref_table = ref_table;
        live.delete_rule = delete_rule;
        live.update_rule = update_rule;
        live.is_deferrable = is_deferrable;
        live.initially_deferred = initially_deferred;
    }

    Ok(by_constraint
        .into_values()
        .any(|live| foreign_key_constraint_matches(live, expected)))
}

fn foreign_key_constraint_matches(
    mut live: LiveForeignKeyConstraint,
    expected: &ExpectedForeignKeyConstraint<'_>,
) -> bool {
    live.columns.sort_by_key(|(ordinal, _, _)| *ordinal);
    let live_columns = live
        .columns
        .iter()
        .map(|(_, column, _)| column.clone())
        .collect::<Vec<_>>();
    let live_ref_columns = live
        .columns
        .iter()
        .map(|(_, _, column)| column.clone())
        .collect::<Vec<_>>();
    let (expected_ref_schema, expected_ref_table) = split_schema_ident(expected.ref_table);

    constraint_columns_match(&live_columns, expected.columns)
        && normalize_constraint_part(&live.ref_schema)
            == normalize_constraint_part(expected_ref_schema)
        && normalize_constraint_part(&live.ref_table)
            == normalize_constraint_part(expected_ref_table)
        && constraint_columns_match(&live_ref_columns, expected.ref_columns)
        && fk_rule_matches(&live.delete_rule, expected.on_delete)
        && fk_rule_matches(&live.update_rule, expected.on_update)
        && deferrable_matches(
            live.is_deferrable,
            live.initially_deferred,
            expected.deferrable,
        )
}

fn fk_rule_matches(live_rule: &str, expected_rule: Option<&str>) -> bool {
    normalize_fk_rule(live_rule) == normalize_fk_rule(expected_rule.unwrap_or("NO ACTION"))
}

fn normalize_fk_rule(rule: &str) -> String {
    rule.trim()
        .replace('_', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase()
}

fn deferrable_matches(
    live_is_deferrable: bool,
    live_initially_deferred: bool,
    expected_deferrable: Option<&str>,
) -> bool {
    let Some(expected) = expected_deferrable else {
        return !live_is_deferrable && !live_initially_deferred;
    };
    let normalized = normalize_fk_rule(expected);
    match normalized.as_str() {
        "NOT DEFERRABLE" => !live_is_deferrable && !live_initially_deferred,
        "DEFERRABLE" | "DEFERRABLE INITIALLY IMMEDIATE" => {
            live_is_deferrable && !live_initially_deferred
        }
        "DEFERRABLE INITIALLY DEFERRED" => live_is_deferrable && live_initially_deferred,
        _ => false,
    }
}

async fn index_exists(pg: &mut qail_pg::PgDriver, index_name: &str) -> Result<bool> {
    let (schema, name) = split_schema_ident(index_name);
    let cmd = Qail::get("pg_class c")
        .column_expr(crate::util::qail_exists_projection())
        .join(JoinKind::Inner, "pg_namespace n", "n.oid", "c.relnamespace")
        .where_eq("n.nspname", schema)
        .where_eq("c.relname", name)
        .limit(1);
    let rows = pg
        .fetch_all(&cmd)
        .await
        .with_context(|| format!("Failed index existence check for '{}'", index_name))?;
    Ok(!rows.is_empty())
}

async fn policy_exists(pg: &mut qail_pg::PgDriver, table: &str, policy_name: &str) -> Result<bool> {
    let (schema, table_name) = split_schema_ident(table);
    let cmd = Qail::get("pg_policies")
        .column_expr(crate::util::qail_exists_projection())
        .where_eq("schemaname", schema)
        .where_eq("tablename", table_name)
        .where_eq("policyname", policy_name)
        .limit(1);
    let rows = pg.fetch_all(&cmd).await.with_context(|| {
        format!(
            "Failed policy existence check for '{}.{}'",
            table_name, policy_name
        )
    })?;
    Ok(!rows.is_empty())
}

async fn table_rls_flags(pg: &mut qail_pg::PgDriver, table: &str) -> Result<Option<(bool, bool)>> {
    let (schema, table_name) = split_schema_ident(table);
    let cmd = Qail::get("pg_class c")
        .columns(["c.relrowsecurity", "c.relforcerowsecurity"])
        .join(JoinKind::Inner, "pg_namespace n", "n.oid", "c.relnamespace")
        .where_eq("n.nspname", schema)
        .where_eq("c.relname", table_name)
        .limit(1);
    let rows = pg
        .fetch_all(&cmd)
        .await
        .with_context(|| format!("Failed RLS flag check for '{}'", table))?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    Ok(Some((
        row.get_bool(0).unwrap_or(false),
        row.get_bool(1).unwrap_or(false),
    )))
}

struct ApplyReceiptContext<'a> {
    migration_name: &'a str,
    started_ms: i64,
    executed_sql_for_receipt: String,
    checksum: String,
    risk_summary: String,
    affected_rows_est: Option<i64>,
    failpoint_override: Option<&'a str>,
}

async fn apply_commands_and_record_receipt_atomic(
    pg: &mut qail_pg::PgDriver,
    cmds: &[Qail],
    adopt_existing: bool,
    context: ApplyReceiptContext<'_>,
) -> Result<()> {
    let ApplyReceiptContext {
        migration_name,
        started_ms,
        executed_sql_for_receipt,
        checksum,
        risk_summary,
        affected_rows_est,
        failpoint_override,
    } = context;

    pg.begin()
        .await
        .map_err(|e| anyhow!("Failed to begin migration transaction: {}", e))?;

    if let Err(err) = execute_migration_commands(pg, cmds, migration_name, adopt_existing).await {
        let _ = pg.rollback().await;
        return Err(err);
    }

    if let Err(err) = verify_applied_commands_effects(pg, migration_name, cmds).await {
        let _ = pg.rollback().await;
        return Err(err);
    }

    let finished_ms = now_epoch_ms();
    let receipt = MigrationReceipt {
        version: migration_name.to_string(),
        name: migration_name.to_string(),
        checksum,
        sql_up: executed_sql_for_receipt,
        git_sha: runtime_git_sha(),
        qail_version: env!("CARGO_PKG_VERSION").to_string(),
        actor: runtime_actor(),
        started_at_ms: Some(started_ms),
        finished_at_ms: Some(finished_ms),
        duration_ms: Some(finished_ms.saturating_sub(started_ms)),
        affected_rows_est,
        risk_summary: Some(risk_summary),
        shadow_checksum: None,
    };

    if let Err(err) = maybe_failpoint_override("apply.before_receipt", failpoint_override) {
        let _ = pg.rollback().await;
        return Err(err);
    }

    if let Err(err) = write_migration_receipt(pg, &receipt).await {
        let _ = pg.rollback().await;
        return Err(anyhow!(
            "Failed to record migration '{}': {}",
            migration_name,
            err
        ));
    }

    if let Err(err) = maybe_failpoint_override("apply.before_commit", failpoint_override) {
        let _ = pg.rollback().await;
        return Err(err);
    }

    pg.commit()
        .await
        .map_err(|e| anyhow!("Failed to commit migration transaction: {}", e))?;

    Ok(())
}

struct ApplyDownContext<'a> {
    migration_name: &'a str,
    started_ms: i64,
    executed_sql_for_receipt: String,
    checksum: String,
    risk_summary: String,
    versions_to_delete: &'a [String],
    failpoint_override: Option<&'a str>,
}

async fn apply_down_commands_and_reconcile_history_atomic(
    pg: &mut qail_pg::PgDriver,
    cmds: &[Qail],
    context: ApplyDownContext<'_>,
) -> Result<()> {
    let ApplyDownContext {
        migration_name,
        started_ms,
        executed_sql_for_receipt,
        checksum,
        risk_summary,
        versions_to_delete,
        failpoint_override,
    } = context;

    pg.begin()
        .await
        .map_err(|e| anyhow!("Failed to begin migration transaction: {}", e))?;

    if let Err(err) = execute_migration_commands(pg, cmds, migration_name, false).await {
        let _ = pg.rollback().await;
        return Err(err);
    }

    if let Err(err) = verify_applied_commands_effects(pg, migration_name, cmds).await {
        let _ = pg.rollback().await;
        return Err(err);
    }

    for version in versions_to_delete {
        let delete_cmd = Qail::del("_qail_migrations").where_eq("version", version.as_str());
        if let Err(err) = pg.execute(&delete_cmd).await {
            let _ = pg.rollback().await;
            return Err(anyhow!(
                "Failed to reconcile migration history (delete '{}'): {}",
                version,
                err
            ));
        }
    }

    if let Err(err) = maybe_failpoint_override("apply.before_receipt", failpoint_override) {
        let _ = pg.rollback().await;
        return Err(err);
    }

    let finished_ms = now_epoch_ms();
    let deleted = if versions_to_delete.is_empty() {
        String::from("none")
    } else {
        versions_to_delete.join(",")
    };
    let receipt_tag: String = migration_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    let receipt = MigrationReceipt {
        version: format!(
            "apply_down_{}_{}",
            receipt_tag,
            crate::time::timestamp_version()
        ),
        name: format!("apply_down {}", migration_name),
        checksum,
        sql_up: executed_sql_for_receipt,
        git_sha: runtime_git_sha(),
        qail_version: env!("CARGO_PKG_VERSION").to_string(),
        actor: runtime_actor(),
        started_at_ms: Some(started_ms),
        finished_at_ms: Some(finished_ms),
        duration_ms: Some(finished_ms.saturating_sub(started_ms)),
        affected_rows_est: Some(i64::try_from(versions_to_delete.len()).unwrap_or(i64::MAX)),
        risk_summary: Some(format!("{risk_summary};rolled_back_versions={deleted}")),
        shadow_checksum: None,
    };

    if let Err(err) = write_migration_receipt(pg, &receipt).await {
        let _ = pg.rollback().await;
        return Err(anyhow!(
            "Failed to record down migration '{}': {}",
            migration_name,
            err
        ));
    }

    if let Err(err) = maybe_failpoint_override("apply.before_commit", failpoint_override) {
        let _ = pg.rollback().await;
        return Err(err);
    }

    pg.commit()
        .await
        .map_err(|e| anyhow!("Failed to commit migration transaction: {}", e))?;

    Ok(())
}

fn maybe_failpoint_override(name: &str, failpoint_override: Option<&str>) -> Result<()> {
    let Some(spec) = failpoint_override else {
        return maybe_failpoint(name);
    };
    if spec
        .split(',')
        .map(str::trim)
        .any(|token| token == "*" || token.eq_ignore_ascii_case(name))
    {
        bail!("Injected failpoint triggered: {}", name);
    }
    Ok(())
}

fn ensure_applied_checksum_matches(
    version: &str,
    stored_checksum: &str,
    expected_checksum: &str,
    legacy_expected_checksum: Option<&str>,
) -> Result<()> {
    if stored_checksum == expected_checksum || legacy_expected_checksum == Some(stored_checksum) {
        return Ok(());
    }
    bail!(
        "Migration checksum drift detected for '{}': stored={}, current={}. \
         Refusing to skip. Rename the migration or reconcile _qail_migrations before re-running.",
        version,
        stored_checksum,
        expected_checksum
    );
}

#[derive(Debug, Clone)]
pub(crate) struct ExpectedMigrationChecksums {
    pub current: String,
    pub legacy: Option<String>,
}

fn expected_checksums_for_commands(cmds: &[Qail], sql: &str) -> ExpectedMigrationChecksums {
    let current = stable_cmds_checksum(cmds);
    let legacy = crate::time::md5_hex(sql);
    ExpectedMigrationChecksums {
        current: current.clone(),
        legacy: (legacy != current).then_some(legacy),
    }
}

pub(crate) fn compute_expected_migration_checksums(
    content: &str,
    phase: MigrationPhase,
    backfill_chunk_size: usize,
) -> Result<ExpectedMigrationChecksums> {
    if phase == MigrationPhase::Backfill
        && parse_backfill_spec(content, backfill_chunk_size)?.is_some()
    {
        return Ok(ExpectedMigrationChecksums {
            current: crate::time::md5_hex(content),
            legacy: None,
        });
    }

    let cmds = parse_qail_to_commands_strict(content)
        .context("Failed to compile migration to AST commands for checksum")?;
    let sql = commands_to_sql(&cmds);
    Ok(expected_checksums_for_commands(&cmds, &sql))
}

fn active_contract_baseline_group(applied_migrations: &HashMap<String, String>) -> Option<String> {
    applied_migrations
        .keys()
        .filter_map(|version| version.strip_suffix("/expand.qail"))
        .filter(|group| group.ends_with("_contract_baseline"))
        .max()
        .map(str::to_string)
}

fn validate_receipts_against_local(
    discovered_up: &[MigrationFile],
    applied_migrations: &HashMap<String, String>,
    applied_receipts: &HashMap<String, StoredMigrationReceipt>,
    mode: ReceiptValidationMode,
    backfill_chunk_size: usize,
) -> Result<()> {
    if discovered_up.is_empty() || applied_migrations.is_empty() {
        return Ok(());
    }

    let local_versions = discovered_up
        .iter()
        .map(|m| m.display_name.clone())
        .collect::<HashSet<_>>();

    let mut missing_local = Vec::<String>::new();
    for version in applied_migrations.keys() {
        if !version.ends_with(".qail") {
            continue;
        }
        if !local_versions.contains(version) {
            missing_local.push(version.clone());
        }
    }
    missing_local.sort();

    if !missing_local.is_empty() {
        let detail = missing_local
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let msg = format!(
            "Migration receipt drift detected: {} applied migration version(s) exist in _qail_migrations but no matching local file in deltas/migrations (examples: {}).",
            missing_local.len(),
            detail
        );
        match mode {
            ReceiptValidationMode::Warn => {
                eprintln!("  {} {}", "⚠".yellow(), msg.yellow());
            }
            ReceiptValidationMode::Error => bail!("{}", msg),
        }
    }

    for mig in discovered_up {
        let Some(stored_checksum) = applied_migrations.get(&mig.display_name) else {
            continue;
        };
        let content = std::fs::read_to_string(&mig.path)
            .with_context(|| format!("Failed to read {}", mig.path.display()))?;
        let expected_checksum =
            compute_expected_migration_checksums(&content, mig.phase, backfill_chunk_size)?;
        if stored_checksum == &expected_checksum.current
            || expected_checksum.legacy.as_ref() == Some(stored_checksum)
        {
            continue;
        }
        let msg = format!(
            "Migration checksum drift detected for '{}': stored={}, local={}",
            mig.display_name, stored_checksum, expected_checksum.current
        );
        match mode {
            ReceiptValidationMode::Warn => {
                eprintln!("  {} {}", "⚠".yellow(), msg.yellow());
            }
            ReceiptValidationMode::Error => bail!("{}", msg),
        }
    }

    let mut missing_signature = Vec::<String>::new();
    let mut invalid_signature = Vec::<String>::new();
    for (version, stored) in applied_receipts {
        if !version.ends_with(".qail") {
            continue;
        }
        match verify_stored_receipt_signature(stored) {
            ReceiptSignatureStatus::DisabledNoKey | ReceiptSignatureStatus::Valid => {}
            ReceiptSignatureStatus::Missing => missing_signature.push(version.clone()),
            ReceiptSignatureStatus::Invalid => invalid_signature.push(version.clone()),
        }
    }
    missing_signature.sort();
    invalid_signature.sort();

    if !missing_signature.is_empty() {
        let detail = missing_signature
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let msg = format!(
            "Migration receipt signature missing for {} applied version(s) (examples: {}). \
             Set migrations.policy.receipt_validation=warn to bypass temporarily while backfilling signatures.",
            missing_signature.len(),
            detail
        );
        match mode {
            ReceiptValidationMode::Warn => eprintln!("  {} {}", "⚠".yellow(), msg.yellow()),
            ReceiptValidationMode::Error => bail!("{}", msg),
        }
    }

    if !invalid_signature.is_empty() {
        let detail = invalid_signature
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let msg = format!(
            "Migration receipt signature verification failed for {} applied version(s) (examples: {}). \
             Refusing to proceed with untrusted migration history.",
            invalid_signature.len(),
            detail
        );
        match mode {
            ReceiptValidationMode::Warn => eprintln!("  {} {}", "⚠".yellow(), msg.yellow()),
            ReceiptValidationMode::Error => bail!("{}", msg),
        }
    }

    Ok(())
}

fn parse_i64_field(value: Option<&String>) -> Option<i64> {
    value.and_then(|v| v.parse::<i64>().ok())
}

fn ensure_up_down_pairing(up: &[MigrationFile], down: &[MigrationFile]) -> Result<()> {
    if up.is_empty() {
        return Ok(());
    }

    let mut down_groups: HashMap<&str, usize> = HashMap::new();
    let mut down_display = HashSet::<String>::new();
    for mig in down {
        *down_groups.entry(mig.group_key.as_str()).or_insert(0) += 1;
        down_display.insert(mig.display_name.clone());
    }

    let mut missing_groups = BTreeSet::<String>::new();
    let mut ambiguous_groups = BTreeSet::<String>::new();
    let mut missing_flat_pairs = BTreeSet::<String>::new();

    for mig in up {
        match down_groups.get(mig.group_key.as_str()) {
            None => {
                missing_groups.insert(mig.group_key.clone());
            }
            Some(count) if *count > 1 => {
                ambiguous_groups.insert(mig.group_key.clone());
            }
            Some(_) => {}
        }

        if mig.display_name.ends_with(".up.qail") {
            let expected_down = mig.display_name.replacen(".up.qail", ".down.qail", 1);
            if !down_display.contains(&expected_down) {
                missing_flat_pairs.insert(format!("{} -> {}", mig.display_name, expected_down));
            }
        }
    }

    if !missing_groups.is_empty() {
        let groups = missing_groups
            .into_iter()
            .take(8)
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "Missing rollback migrations (*.down.qail or <dir>/down.qail) for group(s): {}",
            groups
        );
    }
    if !ambiguous_groups.is_empty() {
        let groups = ambiguous_groups
            .into_iter()
            .take(8)
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "Ambiguous rollback mapping: multiple down migrations found for group(s): {}",
            groups
        );
    }
    if !missing_flat_pairs.is_empty() {
        let pairs = missing_flat_pairs
            .into_iter()
            .take(8)
            .collect::<Vec<_>>()
            .join(", ");
        bail!("Missing flat rollback pair(s): {}", pairs);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ApplyDownContext, ApplyReceiptContext, LiveColumnDefinition,
        active_contract_baseline_group, apply_commands_and_record_receipt_atomic,
        apply_down_commands_and_reconcile_history_atomic, collect_constraint_final_expectations,
        collect_policy_final_expectations, column_type_matches, constraint_columns_match,
        deferrable_matches, enforce_apply_destructive_policy,
        enforce_apply_down_destructive_policy, ensure_applied_checksum_matches,
        ensure_up_down_pairing, fk_rule_matches, foreign_key_constraint_matches,
        normalize_column_type, parse_qail_to_commands_strict, parse_rename_expr,
        should_adopt_existing_error, should_run_apply_lock_risk_preflight, split_schema_ident,
        strip_optional_if_exists_prefix, validate_receipts_against_local,
        verify_applied_commands_effects,
    };
    use super::{ExpectedForeignKeyConstraint, LiveForeignKeyConstraint};
    use crate::migrations::apply::MigrationFile;
    use crate::migrations::apply::types::{MigrateDirection, MigrationPhase};
    use crate::migrations::{EnforcementMode, ReceiptValidationMode};
    use qail_core::ast::{Action, Constraint, Expr, TableConstraint};
    use qail_core::prelude::Qail;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;

    fn mig(group_key: &str, display_name: &str) -> MigrationFile {
        MigrationFile {
            group_key: group_key.to_string(),
            sort_key: display_name.to_string(),
            display_name: display_name.to_string(),
            path: PathBuf::from(display_name),
            phase: MigrationPhase::Expand,
        }
    }

    #[test]
    fn applied_checksum_match_passes() {
        assert!(ensure_applied_checksum_matches("001_init.up.qail", "abc", "abc", None).is_ok());
    }

    #[test]
    fn applied_checksum_accepts_legacy_sql_checksum() {
        assert!(
            ensure_applied_checksum_matches(
                "001_init.up.qail",
                "legacy",
                "current",
                Some("legacy"),
            )
            .is_ok()
        );
    }

    #[test]
    fn applied_checksum_mismatch_fails() {
        let err = ensure_applied_checksum_matches("001_init.up.qail", "abc", "def", None)
            .expect_err("mismatch must fail");
        assert!(
            err.to_string().contains("checksum drift"),
            "error should mention checksum drift"
        );
    }

    #[test]
    fn active_contract_baseline_group_uses_latest_applied_baseline() {
        let mut applied = HashMap::<String, String>::new();
        applied.insert(
            "202606010000_contract_baseline/expand.qail".to_string(),
            "old".to_string(),
        );
        applied.insert(
            "202606090000_contract_baseline/expand.qail".to_string(),
            "new".to_string(),
        );
        applied.insert(
            "202606090001_future_contract/contract.qail".to_string(),
            "future".to_string(),
        );

        assert_eq!(
            active_contract_baseline_group(&applied).as_deref(),
            Some("202606090000_contract_baseline")
        );
    }

    #[test]
    fn split_schema_ident_defaults_to_public() {
        let (schema, name) = split_schema_ident("users");
        assert_eq!(schema, "public");
        assert_eq!(name, "users");
    }

    #[test]
    fn split_schema_ident_handles_qualified_name() {
        let (schema, name) = split_schema_ident("analytics.users");
        assert_eq!(schema, "analytics");
        assert_eq!(name, "users");
    }

    #[test]
    fn strip_optional_if_exists_prefix_normalizes_name() {
        assert_eq!(
            strip_optional_if_exists_prefix("if exists idx_users_email"),
            "idx_users_email"
        );
        assert_eq!(
            strip_optional_if_exists_prefix("IDX_USERS_EMAIL"),
            "IDX_USERS_EMAIL"
        );
    }

    #[test]
    fn parse_rename_expr_extracts_column_pair() {
        assert_eq!(
            parse_rename_expr("old_name -> new_name"),
            Some(("old_name", "new_name"))
        );
        assert_eq!(parse_rename_expr("  a->b "), Some(("a", "b")));
        assert_eq!(parse_rename_expr("old_name"), None);
    }

    #[test]
    fn adopt_existing_accepts_duplicate_relation_errors_for_create_actions() {
        assert!(should_adopt_existing_error(
            Action::CreateSequence,
            "Query error [42P07]: relation \"booking_number_seq\" already exists"
        ));
        assert!(should_adopt_existing_error(
            Action::Make,
            "relation \"users\" already exists"
        ));
    }

    #[test]
    fn adopt_existing_rejects_non_create_actions_and_other_errors() {
        assert!(!should_adopt_existing_error(
            Action::Add,
            "Query error [42P07]: relation \"booking_number_seq\" already exists"
        ));
        assert!(!should_adopt_existing_error(
            Action::CreateSequence,
            "Query error [42501]: permission denied for schema public"
        ));
    }

    #[test]
    fn adopt_existing_column_type_normalization_handles_postgres_aliases() {
        assert_eq!(normalize_column_type("serial"), "integer");
        assert_eq!(normalize_column_type("int4"), "integer");
        assert_eq!(normalize_column_type("varchar(255)"), "character varying");
        assert_eq!(normalize_column_type("char(2)"), "character");
        assert_eq!(
            normalize_column_type("timestamptz"),
            "timestamp with time zone"
        );
    }

    fn live_column(
        data_type: &str,
        character_maximum_length: Option<i32>,
        numeric_precision: Option<i32>,
        numeric_scale: Option<i32>,
        datetime_precision: Option<i32>,
    ) -> LiveColumnDefinition {
        LiveColumnDefinition {
            data_type: data_type.to_string(),
            udt_name: None,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            nullable: false,
        }
    }

    #[test]
    fn column_type_matching_checks_type_modifiers() {
        let varchar_32 = live_column("character varying", Some(32), None, None, None);
        assert!(column_type_matches("varchar(32)", &varchar_32));
        assert!(!column_type_matches("varchar(255)", &varchar_32));
        assert!(!column_type_matches("varchar", &varchar_32));
        let varchar_unbounded = live_column("character varying", None, None, None, None);
        assert!(column_type_matches("varchar", &varchar_unbounded));

        let numeric_10_2 = live_column("numeric", None, Some(10), Some(2), None);
        assert!(column_type_matches("numeric(10,2)", &numeric_10_2));
        assert!(!column_type_matches("numeric(12,2)", &numeric_10_2));
        assert!(!column_type_matches("numeric(10,4)", &numeric_10_2));
        assert!(!column_type_matches("numeric", &numeric_10_2));
        let numeric_unbounded = live_column("numeric", None, None, None, None);
        assert!(column_type_matches("numeric", &numeric_unbounded));
        assert!(!column_type_matches("numeric(10,nope)", &numeric_10_2));

        let timestamp_3 = live_column("timestamp without time zone", None, None, None, Some(3));
        assert!(column_type_matches("timestamp(3)", &timestamp_3));
        assert!(!column_type_matches("timestamp(6)", &timestamp_3));
        assert!(!column_type_matches("timestamp", &timestamp_3));
        let timestamp_default =
            live_column("timestamp without time zone", None, None, None, Some(6));
        assert!(column_type_matches("timestamp", &timestamp_default));
    }

    #[test]
    fn table_constraint_column_matching_is_ordered_and_normalized() {
        assert!(constraint_columns_match(
            &["\"Tenant_ID\"".to_string(), "Order_ID".to_string()],
            &["tenant_id".to_string(), "order_id".to_string()]
        ));
        assert!(!constraint_columns_match(
            &["order_id".to_string(), "tenant_id".to_string()],
            &["tenant_id".to_string(), "order_id".to_string()]
        ));
    }

    #[test]
    fn foreign_key_rule_and_deferrable_matching_normalize_pg_variants() {
        assert!(fk_rule_matches("SET NULL", Some("set_null")));
        assert!(fk_rule_matches("NO ACTION", None));
        assert!(deferrable_matches(
            true,
            true,
            Some("DEFERRABLE INITIALLY DEFERRED")
        ));
        assert!(deferrable_matches(
            true,
            false,
            Some("DEFERRABLE INITIALLY IMMEDIATE")
        ));
        assert!(deferrable_matches(false, false, None));
        assert!(!deferrable_matches(false, false, Some("DEFERRABLE")));
    }

    #[test]
    fn foreign_key_constraint_matching_checks_columns_reference_and_options() {
        let live = LiveForeignKeyConstraint {
            columns: vec![
                (2, "schedule_id".to_string(), "schedule_id".to_string()),
                (1, "route_id".to_string(), "route_id".to_string()),
            ],
            ref_schema: "public".to_string(),
            ref_table: "schedules".to_string(),
            delete_rule: "CASCADE".to_string(),
            update_rule: "RESTRICT".to_string(),
            is_deferrable: true,
            initially_deferred: true,
        };
        let expected = ExpectedForeignKeyConstraint {
            name: Some("fk_trips_schedule"),
            columns: &["route_id".to_string(), "schedule_id".to_string()],
            ref_table: "schedules",
            ref_columns: &["route_id".to_string(), "schedule_id".to_string()],
            on_delete: Some("cascade"),
            on_update: Some("restrict"),
            deferrable: Some("deferrable initially deferred"),
        };

        assert!(foreign_key_constraint_matches(live, &expected));
    }

    #[test]
    fn destructive_policy_passes_when_no_destructive_ops() {
        let result =
            enforce_apply_destructive_policy("001_init.up.qail", &[], EnforcementMode::Deny, false);
        assert!(result.is_ok(), "no-op should pass regardless of policy");
    }

    #[test]
    fn destructive_policy_require_flag_blocks_without_flag() {
        let err = enforce_apply_destructive_policy(
            "002_drop_users.up.qail",
            &[String::from("DROP TABLE users")],
            EnforcementMode::RequireFlag,
            false,
        )
        .expect_err("require-flag should block without --allow-destructive");
        assert!(
            err.to_string().contains("--allow-destructive"),
            "error should mention allow-destructive override"
        );
    }

    #[test]
    fn destructive_policy_require_flag_passes_with_flag() {
        let result = enforce_apply_destructive_policy(
            "002_drop_users.up.qail",
            &[String::from("DROP TABLE users")],
            EnforcementMode::RequireFlag,
            true,
        );
        assert!(
            result.is_ok(),
            "require-flag policy should pass when allow flag is set"
        );
    }

    #[test]
    fn destructive_policy_deny_blocks_even_with_flag() {
        let err = enforce_apply_destructive_policy(
            "002_drop_users.up.qail",
            &[String::from("DROP TABLE users")],
            EnforcementMode::Deny,
            true,
        )
        .expect_err("deny mode must always block destructive migrations");
        assert!(
            err.to_string().contains("destructive=deny"),
            "error should mention deny policy"
        );
    }

    #[test]
    fn down_destructive_policy_deny_blocks_compiled_drop() {
        let cmds = parse_qail_to_commands_strict("drop table demo")
            .expect("drop table should compile to AST command");
        assert!(
            cmds.iter()
                .any(|cmd| matches!(cmd.action, Action::Drop) && cmd.table == "demo"),
            "test fixture must compile to Action::Drop"
        );

        let err = enforce_apply_down_destructive_policy(
            "001_demo.down.qail",
            &cmds,
            EnforcementMode::Deny,
            false,
        )
        .expect_err("deny policy should block destructive down migration before execution");

        let msg = err.to_string();
        assert!(msg.contains("destructive=deny"), "got: {msg}");
        assert!(msg.contains("DROP TABLE demo"), "got: {msg}");
    }

    #[test]
    fn down_destructive_policy_require_flag_blocks_without_allow_flag() {
        let cmds = parse_qail_to_commands_strict("drop table demo")
            .expect("drop table should compile to AST command");
        assert!(
            cmds.iter()
                .any(|cmd| matches!(cmd.action, Action::Drop) && cmd.table == "demo"),
            "test fixture must compile to Action::Drop"
        );

        let err = enforce_apply_down_destructive_policy(
            "001_demo.down.qail",
            &cmds,
            EnforcementMode::RequireFlag,
            false,
        )
        .expect_err("require-flag policy should block destructive down migration without flag");

        let msg = err.to_string();
        assert!(msg.contains("--allow-destructive"), "got: {msg}");
        assert!(msg.contains("DROP TABLE demo"), "got: {msg}");
    }

    #[test]
    fn lock_risk_preflight_runs_for_down_commands() {
        let cmds = parse_qail_to_commands_strict("drop table demo")
            .expect("drop table should compile to AST command");
        assert!(
            cmds.iter()
                .any(|cmd| matches!(cmd.action, Action::Drop) && cmd.table == "demo"),
            "test fixture must compile to Action::Drop"
        );

        assert!(should_run_apply_lock_risk_preflight(
            MigrateDirection::Down,
            &cmds
        ));
        assert!(should_run_apply_lock_risk_preflight(
            MigrateDirection::Up,
            &cmds
        ));
        assert!(!should_run_apply_lock_risk_preflight(
            MigrateDirection::Down,
            &[]
        ));
    }

    #[test]
    fn up_down_pairing_passes_for_flat_pair() {
        let up = vec![mig("001_add_users", "001_add_users.up.qail")];
        let down = vec![mig("001_add_users", "001_add_users.down.qail")];
        assert!(ensure_up_down_pairing(&up, &down).is_ok());
    }

    #[test]
    fn up_down_pairing_fails_when_missing_group_down() {
        let up = vec![mig("001_add_users", "001_add_users.up.qail")];
        let err = ensure_up_down_pairing(&up, &[]).expect_err("missing down must fail");
        assert!(
            err.to_string().contains("Missing rollback migrations"),
            "error should mention missing rollback migration"
        );
    }

    #[test]
    fn up_down_pairing_fails_when_ambiguous_group() {
        let up = vec![mig("001_add_users", "001_add_users.up.qail")];
        let down = vec![
            mig("001_add_users", "001_add_users.down.qail"),
            mig("001_add_users", "001_add_users_v2.down.qail"),
        ];
        let err = ensure_up_down_pairing(&up, &down).expect_err("ambiguous down must fail");
        assert!(
            err.to_string().contains("Ambiguous rollback mapping"),
            "error should mention ambiguous rollback mapping"
        );
    }

    #[test]
    fn receipt_validation_warns_on_missing_local_file() {
        let migrations = vec![mig("001_add_users", "001_add_users.up.qail")];
        let mut applied = HashMap::new();
        applied.insert("999_missing.up.qail".to_string(), "abc".to_string());
        let applied_receipts = HashMap::new();
        assert!(
            validate_receipts_against_local(
                &migrations,
                &applied,
                &applied_receipts,
                ReceiptValidationMode::Warn,
                5000
            )
            .is_ok()
        );
    }

    #[test]
    fn receipt_validation_errors_on_missing_local_file() {
        let migrations = vec![mig("001_add_users", "001_add_users.up.qail")];
        let mut applied = HashMap::new();
        applied.insert("999_missing.up.qail".to_string(), "abc".to_string());
        let applied_receipts = HashMap::new();
        let err = validate_receipts_against_local(
            &migrations,
            &applied,
            &applied_receipts,
            ReceiptValidationMode::Error,
            5000,
        )
        .expect_err("missing local receipt must fail in error mode");
        assert!(
            err.to_string().contains("receipt drift"),
            "error should mention receipt drift"
        );
    }

    #[test]
    fn receipt_validation_errors_on_checksum_mismatch() {
        let root =
            std::env::temp_dir().join(format!("qail_receipt_validation_{}", std::process::id()));
        let _ = fs::create_dir_all(&root);
        let path = root.join("001_add_users.up.qail");
        fs::write(&path, "table users (id int)\n").expect("write migration");
        let migrations = vec![MigrationFile {
            group_key: "001_add_users".to_string(),
            sort_key: "001_add_users.up.qail".to_string(),
            display_name: "001_add_users.up.qail".to_string(),
            path,
            phase: MigrationPhase::Expand,
        }];
        let mut applied = HashMap::new();
        applied.insert("001_add_users.up.qail".to_string(), "deadbeef".to_string());
        let applied_receipts = HashMap::new();
        let err = validate_receipts_against_local(
            &migrations,
            &applied,
            &applied_receipts,
            ReceiptValidationMode::Error,
            5000,
        )
        .expect_err("checksum mismatch must fail");
        assert!(
            err.to_string().contains("checksum drift"),
            "error should mention checksum drift"
        );
    }

    #[test]
    fn policy_expectations_follow_last_command_intent() {
        let cmds = vec![
            Qail {
                action: Action::DropPolicy,
                table: "tenant_contracts".to_string(),
                payload: Some("tenant_contracts_policy".to_string()),
                ..Default::default()
            },
            Qail {
                action: Action::CreatePolicy,
                policy_def: Some(
                    qail_core::migrate::policy::RlsPolicy::create(
                        "tenant_contracts_policy",
                        "tenant_contracts",
                    )
                    .for_all(),
                ),
                ..Default::default()
            },
        ];

        let expected = collect_policy_final_expectations(&cmds);
        assert_eq!(
            expected.get(&(
                "tenant_contracts".to_string(),
                "tenant_contracts_policy".to_string()
            )),
            Some(&true)
        );
    }

    #[test]
    fn policy_expectations_handle_create_then_drop() {
        let cmds = vec![
            Qail {
                action: Action::CreatePolicy,
                policy_def: Some(
                    qail_core::migrate::policy::RlsPolicy::create(
                        "tenant_isolation",
                        "reseller_pricing_overrides",
                    )
                    .for_all(),
                ),
                ..Default::default()
            },
            Qail {
                action: Action::DropPolicy,
                table: "reseller_pricing_overrides".to_string(),
                payload: Some("tenant_isolation".to_string()),
                ..Default::default()
            },
        ];

        let expected = collect_policy_final_expectations(&cmds);
        assert_eq!(
            expected.get(&(
                "reseller_pricing_overrides".to_string(),
                "tenant_isolation".to_string()
            )),
            Some(&false)
        );
    }

    #[test]
    fn constraint_expectations_follow_last_command_intent() {
        let cmds = vec![
            Qail {
                action: Action::AlterDropConstraint,
                table: "odyssey_legs".to_string(),
                channel: Some("odyssey_legs_arrival_day_offset_check".to_string()),
                ..Default::default()
            },
            Qail {
                action: Action::AlterAddConstraint,
                table: "odyssey_legs".to_string(),
                channel: Some("odyssey_legs_arrival_day_offset_check".to_string()),
                payload: Some(
                    "arrival_day_offset >= 0 AND arrival_day_offset <= 7 AND arrival_day_offset >= departure_day_offset"
                        .to_string(),
                ),
                ..Default::default()
            },
        ];

        let expected = collect_constraint_final_expectations(&cmds);
        assert_eq!(
            expected.get(&(
                "odyssey_legs".to_string(),
                "odyssey_legs_arrival_day_offset_check".to_string()
            )),
            Some(&true)
        );
    }

    #[test]
    fn constraint_expectations_handle_create_then_drop() {
        let cmds = vec![
            Qail {
                action: Action::AlterAddConstraint,
                table: "odyssey_legs".to_string(),
                channel: Some("odyssey_legs_check".to_string()),
                payload: Some("arrival_day_offset >= departure_day_offset".to_string()),
                ..Default::default()
            },
            Qail {
                action: Action::AlterDropConstraint,
                table: "odyssey_legs".to_string(),
                channel: Some("odyssey_legs_check".to_string()),
                ..Default::default()
            },
        ];

        let expected = collect_constraint_final_expectations(&cmds);
        assert_eq!(
            expected.get(&("odyssey_legs".to_string(), "odyssey_legs_check".to_string())),
            Some(&false)
        );
    }

    async fn version_exists(pg: &mut qail_pg::PgDriver, version: &str) -> bool {
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
    async fn apply_failpoint_before_receipt_rolls_back_commands_in_real_db() {
        let Some(url) = std::env::var("QAIL_TEST_DB_URL").ok() else {
            eprintln!("Skipping apply failpoint DB test (set QAIL_TEST_DB_URL)");
            return;
        };

        let mut pg = qail_pg::PgDriver::connect_url(&url)
            .await
            .expect("connect QAIL_TEST_DB_URL");
        crate::migrations::ensure_migration_table(&mut pg)
            .await
            .expect("bootstrap _qail_migrations");

        let suffix = format!(
            "{}_{}",
            std::process::id(),
            crate::time::timestamp_version()
        );
        let marker_version = format!("fp_marker_{}", suffix);
        let migration_name = format!("fp_receipt_{}.up.qail", suffix);

        let cleanup_marker =
            Qail::del("_qail_migrations").where_eq("version", marker_version.as_str());
        let cleanup_receipt =
            Qail::del("_qail_migrations").where_eq("version", migration_name.as_str());
        let _ = pg.execute(&cleanup_marker).await;
        let _ = pg.execute(&cleanup_receipt).await;

        let marker_cmd = Qail::add("_qail_migrations")
            .set_value("version", marker_version.as_str())
            .set_value("name", "fp_marker")
            .set_value("checksum", "fp_marker_checksum")
            .set_value("sql_up", "-- fp marker");

        let err = apply_commands_and_record_receipt_atomic(
            &mut pg,
            &[marker_cmd],
            false,
            ApplyReceiptContext {
                migration_name: &migration_name,
                started_ms: crate::migrations::now_epoch_ms(),
                executed_sql_for_receipt: "-- fp marker".to_string(),
                checksum: crate::time::md5_hex("-- fp marker"),
                risk_summary: "source=apply.failpoint.test".to_string(),
                affected_rows_est: None,
                failpoint_override: Some("apply.before_receipt"),
            },
        )
        .await
        .expect_err("failpoint should abort apply transaction");

        assert!(
            err.to_string()
                .contains("Injected failpoint triggered: apply.before_receipt"),
            "unexpected failpoint error: {err}"
        );
        assert!(
            !version_exists(&mut pg, marker_version.as_str()).await,
            "marker command row should have been rolled back"
        );
        assert!(
            !version_exists(&mut pg, migration_name.as_str()).await,
            "migration receipt should not be written when failpoint triggers"
        );
    }

    #[tokio::test]
    async fn adopt_existing_rejects_partial_existing_table_in_real_db() {
        let Some(url) = std::env::var("QAIL_TEST_DB_URL").ok() else {
            eprintln!("Skipping adopt-existing shape DB test (set QAIL_TEST_DB_URL)");
            return;
        };

        let mut pg = qail_pg::PgDriver::connect_url(&url)
            .await
            .expect("connect QAIL_TEST_DB_URL");
        crate::migrations::ensure_migration_table(&mut pg)
            .await
            .expect("bootstrap _qail_migrations");

        let suffix = format!(
            "{}_{}",
            std::process::id(),
            crate::time::timestamp_version()
        );
        let table = format!("adopt_existing_shape_{}", suffix);
        let migration_name = format!("adopt_existing_shape_{}.up.qail", suffix);

        let existing_cmd = Qail {
            action: Action::Make,
            table: table.clone(),
            columns: vec![Expr::Def {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            }],
            ..Default::default()
        };
        pg.execute(&existing_cmd)
            .await
            .expect("create partial existing table");

        let planned_cmd = Qail {
            action: Action::Make,
            table: table.clone(),
            columns: vec![
                Expr::Def {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    constraints: vec![Constraint::PrimaryKey],
                },
                Expr::Def {
                    name: "email".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
            ],
            ..Default::default()
        };

        let err = apply_commands_and_record_receipt_atomic(
            &mut pg,
            &[planned_cmd],
            true,
            ApplyReceiptContext {
                migration_name: &migration_name,
                started_ms: crate::migrations::now_epoch_ms(),
                executed_sql_for_receipt: "-- adopt existing shape".to_string(),
                checksum: crate::time::md5_hex("-- adopt existing shape"),
                risk_summary: "source=apply.adopt_existing.shape.test".to_string(),
                affected_rows_est: None,
                failpoint_override: None,
            },
        )
        .await
        .expect_err("partial existing table must not be adopted");

        assert!(
            err.to_string().contains("expected column")
                && err.to_string().contains("email")
                && err.to_string().contains("adopted table"),
            "unexpected adopt-existing error: {err}"
        );
        assert!(
            !version_exists(&mut pg, migration_name.as_str()).await,
            "failed adoption must not write a migration receipt"
        );

        let _ = pg
            .execute(&Qail {
                action: Action::Drop,
                table,
                ..Default::default()
            })
            .await;
    }

    #[tokio::test]
    async fn adopt_existing_rejects_column_type_modifier_drift_in_real_db() {
        let Some(url) = std::env::var("QAIL_TEST_DB_URL").ok() else {
            eprintln!("Skipping adopt-existing type modifier DB test (set QAIL_TEST_DB_URL)");
            return;
        };

        let mut pg = qail_pg::PgDriver::connect_url(&url)
            .await
            .expect("connect QAIL_TEST_DB_URL");
        crate::migrations::ensure_migration_table(&mut pg)
            .await
            .expect("bootstrap _qail_migrations");

        let suffix = format!(
            "{}_{}",
            std::process::id(),
            crate::time::timestamp_version()
        );
        let table = format!("adopt_existing_type_{}", suffix);
        let migration_name = format!("adopt_existing_type_{}.up.qail", suffix);

        let existing_cmd = Qail {
            action: Action::Make,
            table: table.clone(),
            columns: vec![
                Expr::Def {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    constraints: vec![Constraint::PrimaryKey],
                },
                Expr::Def {
                    name: "email".to_string(),
                    data_type: "varchar(32)".to_string(),
                    constraints: vec![],
                },
            ],
            ..Default::default()
        };
        pg.execute(&existing_cmd)
            .await
            .expect("create type-drift existing table");

        let planned_cmd = Qail {
            action: Action::Make,
            table: table.clone(),
            columns: vec![
                Expr::Def {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    constraints: vec![Constraint::PrimaryKey],
                },
                Expr::Def {
                    name: "email".to_string(),
                    data_type: "varchar(255)".to_string(),
                    constraints: vec![],
                },
            ],
            ..Default::default()
        };

        let err = apply_commands_and_record_receipt_atomic(
            &mut pg,
            &[planned_cmd],
            true,
            ApplyReceiptContext {
                migration_name: &migration_name,
                started_ms: crate::migrations::now_epoch_ms(),
                executed_sql_for_receipt: "-- adopt existing type modifier".to_string(),
                checksum: crate::time::md5_hex("-- adopt existing type modifier"),
                risk_summary: "source=apply.adopt_existing.type_modifier.test".to_string(),
                affected_rows_est: None,
                failpoint_override: None,
            },
        )
        .await
        .expect_err("narrower existing column type modifier must not be adopted");

        assert!(
            err.to_string().contains("expected column")
                && err.to_string().contains("email")
                && err.to_string().contains("varchar(255)")
                && err.to_string().contains("character varying"),
            "unexpected adopt-existing type modifier error: {err}"
        );
        assert!(
            !version_exists(&mut pg, migration_name.as_str()).await,
            "failed type-modifier adoption must not write a migration receipt"
        );

        let _ = pg
            .execute(&Qail {
                action: Action::Drop,
                table,
                ..Default::default()
            })
            .await;
    }

    #[tokio::test]
    async fn verify_applied_effects_detects_missing_composite_fk_in_real_db() {
        let Some(url) = std::env::var("QAIL_TEST_DB_URL").ok() else {
            eprintln!("Skipping composite FK verification DB test (set QAIL_TEST_DB_URL)");
            return;
        };

        let mut pg = qail_pg::PgDriver::connect_url(&url)
            .await
            .expect("connect QAIL_TEST_DB_URL");
        let suffix = format!(
            "{}_{}",
            std::process::id(),
            crate::time::timestamp_version()
        );
        let parent = format!("verify_fk_parent_{}", suffix);
        let child = format!("verify_fk_child_{}", suffix);

        let create_parent = Qail {
            action: Action::Make,
            table: parent.clone(),
            columns: vec![
                Expr::Def {
                    name: "route_id".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "schedule_id".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
            ],
            table_constraints: vec![TableConstraint::Unique(vec![
                "route_id".to_string(),
                "schedule_id".to_string(),
            ])],
            ..Default::default()
        };
        let create_child = Qail {
            action: Action::Make,
            table: child.clone(),
            columns: vec![
                Expr::Def {
                    name: "route_id".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "schedule_id".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
            ],
            ..Default::default()
        };
        pg.execute(&create_parent)
            .await
            .expect("create parent table");
        pg.execute(&create_child).await.expect("create child table");

        let add_fk = Qail {
            action: Action::Alter,
            table: child.clone(),
            table_constraints: vec![TableConstraint::ForeignKey {
                name: Some(format!("fk_{}_schedule", child)),
                columns: vec!["route_id".to_string(), "schedule_id".to_string()],
                ref_table: parent.clone(),
                ref_columns: vec!["route_id".to_string(), "schedule_id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: Some("RESTRICT".to_string()),
                deferrable: Some("DEFERRABLE INITIALLY DEFERRED".to_string()),
            }],
            ..Default::default()
        };

        let err = verify_applied_commands_effects(
            &mut pg,
            "missing_composite_fk.up.qail",
            std::slice::from_ref(&add_fk),
        )
        .await
        .expect_err("missing composite FK must fail verification before receipt write");
        assert!(
            err.to_string().contains("FOREIGN KEY")
                && err.to_string().contains(&child)
                && err.to_string().contains(&parent),
            "unexpected verification error: {err}"
        );

        pg.execute(&add_fk)
            .await
            .expect("add composite foreign key");
        verify_applied_commands_effects(&mut pg, "existing_composite_fk.up.qail", &[add_fk])
            .await
            .expect("existing composite FK should verify");

        let _ = pg
            .execute(&Qail {
                action: Action::Drop,
                table: child,
                ..Default::default()
            })
            .await;
        let _ = pg
            .execute(&Qail {
                action: Action::Drop,
                table: parent,
                ..Default::default()
            })
            .await;
    }

    #[tokio::test]
    async fn apply_down_reconciles_up_history_in_real_db() {
        let Some(url) = std::env::var("QAIL_TEST_DB_URL").ok() else {
            eprintln!("Skipping apply-down history reconciliation DB test (set QAIL_TEST_DB_URL)");
            return;
        };

        let mut pg = qail_pg::PgDriver::connect_url(&url)
            .await
            .expect("connect QAIL_TEST_DB_URL");
        crate::migrations::ensure_migration_table(&mut pg)
            .await
            .expect("bootstrap _qail_migrations");

        let suffix = format!(
            "{}_{}",
            std::process::id(),
            crate::time::timestamp_version()
        );
        let table = format!("apply_down_hist_{}", suffix);
        let up_v1 = format!("{}_001.up.qail", suffix);
        let up_v2 = format!("{}_002.up.qail", suffix);
        let down_name = format!("{}_group/down.qail", suffix);

        // Create a table that down command will drop.
        let create_cmd = Qail {
            action: Action::Make,
            table: table.clone(),
            columns: vec![Expr::Def {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            }],
            ..Default::default()
        };
        pg.execute(&create_cmd).await.expect("create test table");

        // Seed synthetic "applied up" history rows.
        for version in [&up_v1, &up_v2] {
            let seed = Qail::add("_qail_migrations")
                .set_value("version", version.as_str())
                .set_value("name", version.as_str())
                .set_value("checksum", "seed_checksum")
                .set_value("sql_up", "-- seed up");
            pg.execute(&seed).await.expect("seed migration history");
        }

        let down_cmds = vec![Qail {
            action: Action::Drop,
            table: table.clone(),
            ..Default::default()
        }];

        apply_down_commands_and_reconcile_history_atomic(
            &mut pg,
            &down_cmds,
            ApplyDownContext {
                migration_name: &down_name,
                started_ms: crate::migrations::now_epoch_ms(),
                executed_sql_for_receipt: format!("drop {};", table),
                checksum: crate::time::md5_hex(&format!("drop {};", table)),
                risk_summary: "source=apply.down.reconcile.test".to_string(),
                versions_to_delete: &[up_v1.clone(), up_v2.clone()],
                failpoint_override: None,
            },
        )
        .await
        .expect("apply down with history reconciliation");

        assert!(
            !version_exists(&mut pg, up_v1.as_str()).await,
            "first up version should be deleted from _qail_migrations"
        );
        assert!(
            !version_exists(&mut pg, up_v2.as_str()).await,
            "second up version should be deleted from _qail_migrations"
        );
        assert!(
            !super::table_exists(&mut pg, table.as_str())
                .await
                .expect("table existence check"),
            "down command should drop table"
        );

        let versions = pg
            .query_ast(&Qail::get("_qail_migrations").column("version"))
            .await
            .expect("query migration versions")
            .rows
            .iter()
            .filter_map(|row| row.first().and_then(|v| v.as_ref()).cloned())
            .collect::<Vec<_>>();
        assert!(
            versions.iter().all(|v| !v.ends_with(".down.qail")),
            "down-direction apply must not persist .down.qail versions"
        );
        assert!(
            versions.iter().any(|v| v.starts_with("apply_down_")),
            "down-direction apply should record a non-.qail audit receipt"
        );
    }
}
