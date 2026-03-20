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
use qail_core::ast::{Action, Expr, JoinKind};
use qail_core::prelude::Qail;
use std::collections::{BTreeSet, HashMap, HashSet};
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
    allow_destructive: bool,
    allow_no_shadow_receipt: bool,
    allow_lock_risk: bool,
    backfill_chunk_size: usize,
    wait_for_lock: bool,
    lock_timeout_secs: Option<u64>,
) -> Result<()> {
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
        let (cmds, executed_sql_for_receipt, checksum_input) =
            if matches!(direction, MigrateDirection::Up) && mig.phase == MigrationPhase::Backfill {
                if let Some(spec) = parse_backfill_spec(&content, backfill_chunk_size)? {
                    chunked_backfill_spec = Some(spec);
                    (Vec::new(), content.clone(), content.clone())
                } else {
                    let cmds = parse_qail_to_commands_strict(&content)
                        .context("Failed to compile backfill migration to AST commands")?;
                    let sql = commands_to_sql(&cmds);
                    risk_summary.push_str(";chunked_backfill=false");
                    (cmds, sql.clone(), sql)
                }
            } else {
                let cmds = parse_qail_to_commands_strict(&content)
                    .context("Failed to compile migration to AST commands")?;
                let sql = commands_to_sql(&cmds);
                (cmds, sql.clone(), sql)
            };

        let expected_checksum = crate::time::md5_hex(&checksum_input);
        if let Some(stored_checksum) = applied_migrations.get(&mig.display_name) {
            ensure_applied_checksum_matches(
                &mig.display_name,
                stored_checksum,
                &expected_checksum,
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

        if matches!(direction, MigrateDirection::Up) && !cmds.is_empty() {
            if enforce_shadow_receipt {
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

            preflight_lock_risk(
                &mut pg,
                &cmds,
                allow_lock_risk,
                policy.lock_risk,
                policy.lock_risk_max_score,
            )
            .await?;

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
            ";allow_destructive_flag={};allow_lock_risk_flag={};shadow_receipt_required={};policy_destructive={:?};policy_lock_risk={:?};policy_lock_risk_max_score={}",
            allow_destructive,
            allow_lock_risk,
            matches!(direction, MigrateDirection::Up) && enforce_shadow_receipt,
            policy.destructive,
            policy.lock_risk,
            policy.lock_risk_max_score
        ));
        apply_commands_and_record_receipt_atomic(
            &mut pg,
            &cmds,
            &mig.display_name,
            started_ms,
            executed_sql_for_receipt,
            checksum_input,
            risk_summary,
            affected_rows_est,
            None,
        )
        .await
        .context(format!("Failed to apply migration {}", mig.display_name))?;

        applied_migrations.insert(mig.display_name.clone(), expected_checksum);

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

async fn execute_migration_commands(
    pg: &mut qail_pg::PgDriver,
    cmds: &[Qail],
    migration_name: &str,
) -> Result<()> {
    if cmds.is_empty() {
        return Ok(());
    }

    for (idx, cmd) in cmds.iter().enumerate() {
        if let Err(err) = pg.execute(cmd).await {
            return Err(anyhow!(
                "Migration command {} failed in '{}': action={:?} table='{}' error={}",
                idx + 1,
                migration_name,
                cmd.action,
                cmd.table,
                err
            ));
        }
    }

    Ok(())
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

    for cmd in cmds {
        match cmd.action {
            Action::Make => {
                if !table_exists(pg, &cmd.table).await? {
                    failures.push(format!("expected table '{}' to exist", cmd.table));
                }
            }
            Action::Drop => {
                if table_exists(pg, &cmd.table).await? {
                    failures.push(format!("expected table '{}' to be dropped", cmd.table));
                }
            }
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
                if let Some(policy) = &cmd.policy_def
                    && !policy_exists(pg, &policy.table, &policy.name).await?
                {
                    failures.push(format!(
                        "expected policy '{}' on table '{}' to exist",
                        policy.name, policy.table
                    ));
                }
            }
            Action::DropPolicy => {
                let Some(policy_name) = cmd.payload.as_deref() else {
                    continue;
                };
                if policy_exists(pg, &cmd.table, policy_name).await? {
                    failures.push(format!(
                        "expected policy '{}' on table '{}' to be dropped",
                        policy_name, cmd.table
                    ));
                }
            }
            _ => {}
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
        .column("1")
        .where_eq("table_schema", schema)
        .where_eq("table_name", table_name)
        .limit(1);
    let rows = pg
        .fetch_all(&cmd)
        .await
        .with_context(|| format!("Failed table existence check for '{}'", table))?;
    Ok(!rows.is_empty())
}

async fn column_exists(pg: &mut qail_pg::PgDriver, table: &str, column: &str) -> Result<bool> {
    let (schema, table_name) = split_schema_ident(table);
    let cmd = Qail::get("information_schema.columns")
        .column("1")
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

async fn index_exists(pg: &mut qail_pg::PgDriver, index_name: &str) -> Result<bool> {
    let (schema, name) = split_schema_ident(index_name);
    let cmd = Qail::get("pg_class c")
        .column("1")
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
        .column("1")
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

async fn apply_commands_and_record_receipt_atomic(
    pg: &mut qail_pg::PgDriver,
    cmds: &[Qail],
    migration_name: &str,
    started_ms: i64,
    executed_sql_for_receipt: String,
    checksum_input: String,
    risk_summary: String,
    affected_rows_est: Option<i64>,
    failpoint_override: Option<&str>,
) -> Result<()> {
    pg.begin()
        .await
        .map_err(|e| anyhow!("Failed to begin migration transaction: {}", e))?;

    if let Err(err) = execute_migration_commands(pg, cmds, migration_name).await {
        let _ = pg.rollback().await;
        return Err(err);
    }

    if let Err(err) = verify_applied_commands_effects(pg, migration_name, cmds).await {
        let _ = pg.rollback().await;
        return Err(err);
    }

    let finished_ms = now_epoch_ms();
    let checksum = crate::time::md5_hex(&checksum_input);
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
) -> Result<()> {
    if stored_checksum == expected_checksum {
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

pub(crate) fn compute_expected_migration_checksum(
    content: &str,
    phase: MigrationPhase,
    backfill_chunk_size: usize,
) -> Result<String> {
    if phase == MigrationPhase::Backfill
        && parse_backfill_spec(content, backfill_chunk_size)?.is_some()
    {
        return Ok(crate::time::md5_hex(content));
    }

    let cmds = parse_qail_to_commands_strict(content)
        .context("Failed to compile migration to AST commands for checksum")?;
    let sql = commands_to_sql(&cmds);
    Ok(crate::time::md5_hex(&sql))
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
            compute_expected_migration_checksum(&content, mig.phase, backfill_chunk_size)?;
        if stored_checksum == &expected_checksum {
            continue;
        }
        let msg = format!(
            "Migration checksum drift detected for '{}': stored={}, local={}",
            mig.display_name, stored_checksum, expected_checksum
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
        apply_commands_and_record_receipt_atomic, enforce_apply_destructive_policy,
        ensure_applied_checksum_matches, ensure_up_down_pairing, parse_rename_expr,
        split_schema_ident, strip_optional_if_exists_prefix, validate_receipts_against_local,
    };
    use crate::migrations::apply::MigrationFile;
    use crate::migrations::apply::types::MigrationPhase;
    use crate::migrations::{EnforcementMode, ReceiptValidationMode};
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
        assert!(ensure_applied_checksum_matches("001_init.up.qail", "abc", "abc").is_ok());
    }

    #[test]
    fn applied_checksum_mismatch_fails() {
        let err = ensure_applied_checksum_matches("001_init.up.qail", "abc", "def")
            .expect_err("mismatch must fail");
        assert!(
            err.to_string().contains("checksum drift"),
            "error should mention checksum drift"
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
            &migration_name,
            crate::migrations::now_epoch_ms(),
            "-- fp marker".to_string(),
            "-- fp marker".to_string(),
            "source=apply.failpoint.test".to_string(),
            None,
            Some("apply.before_receipt"),
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
}
