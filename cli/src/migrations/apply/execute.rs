//! Main apply entry point — migrate_apply.

use super::backfill::{enforce_contract_safety, parse_backfill_spec, run_chunked_backfill};
use super::codegen::{commands_to_sql, parse_qail_to_commands_strict};
use super::discovery::{discover_migrations, phase_rank};
use super::types::{ApplyPhase, BackfillRun, MigrateDirection, MigrationFile, MigrationPhase};
use crate::colors::*;
use crate::migrations::{
    MigrationReceipt, ReceiptValidationMode, ensure_migration_table, load_migration_policy,
    now_epoch_ms, runtime_actor, runtime_git_sha, write_migration_receipt,
};
use crate::util::parse_pg_url;
use anyhow::{Context, Result, anyhow, bail};
use qail_core::prelude::Qail;
use std::fs;
use std::collections::{BTreeSet, HashMap, HashSet};

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

    // Query already-applied migration versions + checksums
    let status_cmd = Qail::get("_qail_migrations").columns(vec!["version", "checksum"]);

    let mut applied_migrations: HashMap<String, String> = match pg.query_ast(&status_cmd).await {
        Ok(result) => result
            .rows
            .iter()
            .filter_map(|row| {
                let version = row.first().and_then(|v| v.as_ref())?;
                let checksum = row.get(1).and_then(|v| v.as_ref())?;
                Some((version.clone(), checksum.clone()))
            })
            .collect(),
        Err(e) => {
            return Err(anyhow!(
                "Failed to query applied migrations from _qail_migrations: {}",
                e
            ))
        }
    };

    if matches!(direction, MigrateDirection::Up) {
        validate_receipts_against_local(
            &all_discovered,
            &applied_migrations,
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
        apply_commands_and_record_receipt_atomic(
            &mut pg,
            &cmds,
            &mig.display_name,
            started_ms,
            executed_sql_for_receipt,
            checksum_input,
            risk_summary,
            affected_rows_est,
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

async fn apply_commands_and_record_receipt_atomic(
    pg: &mut qail_pg::PgDriver,
    cmds: &[Qail],
    migration_name: &str,
    started_ms: i64,
    executed_sql_for_receipt: String,
    checksum_input: String,
    risk_summary: String,
    affected_rows_est: Option<i64>,
) -> Result<()> {
    pg.begin()
        .await
        .map_err(|e| anyhow!("Failed to begin migration transaction: {}", e))?;

    if let Err(err) = execute_migration_commands(pg, cmds, migration_name).await {
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

    if let Err(err) = write_migration_receipt(pg, &receipt).await {
        let _ = pg.rollback().await;
        return Err(anyhow!(
            "Failed to record migration '{}': {}",
            migration_name,
            err
        ));
    }

    pg.commit()
        .await
        .map_err(|e| anyhow!("Failed to commit migration transaction: {}", e))?;

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

    Ok(())
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
        let groups = missing_groups.into_iter().take(8).collect::<Vec<_>>().join(", ");
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
        ensure_applied_checksum_matches, ensure_up_down_pairing, validate_receipts_against_local,
    };
    use crate::migrations::apply::MigrationFile;
    use crate::migrations::apply::types::MigrationPhase;
    use crate::migrations::ReceiptValidationMode;
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
        assert!(
            validate_receipts_against_local(
                &migrations,
                &applied,
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
        let err = validate_receipts_against_local(
            &migrations,
            &applied,
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
        let root = std::env::temp_dir().join(format!(
            "qail_receipt_validation_{}",
            std::process::id()
        ));
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
        let err = validate_receipts_against_local(
            &migrations,
            &applied,
            ReceiptValidationMode::Error,
            5000,
        )
        .expect_err("checksum mismatch must fail");
        assert!(
            err.to_string().contains("checksum drift"),
            "error should mention checksum drift"
        );
    }
}
