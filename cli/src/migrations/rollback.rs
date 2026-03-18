//! Version-driven rollback operations from migration history.

use crate::colors::*;
use crate::migrations::apply::{
    MigrateDirection, MigrationFile, commands_to_sql, compute_expected_migration_checksum,
    discover_migrations, parse_qail_to_commands_strict,
};
use crate::migrations::{
    MigrationReceipt, ReceiptSignatureStatus, ReceiptValidationMode, StoredMigrationReceipt,
    acquire_migration_lock, ensure_migration_table, load_migration_policy, maybe_failpoint,
    now_epoch_ms, runtime_actor, runtime_git_sha, verify_stored_receipt_signature,
    write_migration_receipt,
};
use crate::util::parse_pg_url;
use anyhow::{Context, Result, anyhow, bail};
use qail_core::prelude::{Qail, SortOrder};
use qail_pg::driver::PgDriver;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;

/// Roll back applied folder migrations to a target version.
///
/// `to_version` semantics:
/// - exact applied up version: roll back everything applied *after* this version
/// - `base` / `0` / `root`: roll back all applied folder migrations
pub async fn migrate_rollback(
    to_version: &str,
    url: &str,
    wait_for_lock: bool,
    lock_timeout_secs: Option<u64>,
) -> Result<()> {
    println!(
        "{} {}",
        "Rolling back to:".cyan().bold(),
        to_version.yellow()
    );
    let policy = load_migration_policy()?;

    let migrations_dir = crate::migrations::resolve_deltas_dir(false)?;
    let up = discover_migrations(&migrations_dir, MigrateDirection::Up)?;
    let down = discover_migrations(&migrations_dir, MigrateDirection::Down)?;
    ensure_up_has_down_pairing(&up, &down)?;

    let down_by_group = index_down_by_group(&down)?;
    let mut up_by_version = HashMap::<String, MigrationFile>::new();
    for mig in up {
        up_by_version.insert(mig.display_name.clone(), mig);
    }

    let (host, port, user, password, database) = parse_pg_url(url)?;
    let mut driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect: {}", e))?
    };

    ensure_migration_table(&mut driver)
        .await
        .map_err(|e| anyhow!("Failed to bootstrap migration table: {}", e))?;
    acquire_migration_lock(
        &mut driver,
        "migrate rollback",
        wait_for_lock,
        lock_timeout_secs,
        Some(database.as_str()),
    )
    .await?;

    let history_cmd = Qail::get("_qail_migrations")
        .columns(vec![
            "version",
            "id",
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
        ])
        .order_by("id", SortOrder::Asc);
    let history = driver
        .query_ast(&history_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query migration history: {}", e))?;

    let mut applied_versions = Vec::<String>::new();
    let mut applied_receipts = HashMap::<String, StoredMigrationReceipt>::new();
    for row in &history.rows {
        let Some(version) = row.first().and_then(|v| v.as_ref()).cloned() else {
            continue;
        };
        if up_by_version.contains_key(&version) {
            applied_versions.push(version.clone());
            applied_receipts.insert(
                version.clone(),
                StoredMigrationReceipt {
                    version,
                    checksum: row.get(2).and_then(|v| v.as_ref()).cloned(),
                    name: row.get(3).and_then(|v| v.as_ref()).cloned(),
                    sql_up: row.get(4).and_then(|v| v.as_ref()).cloned(),
                    git_sha: row.get(5).and_then(|v| v.as_ref()).cloned(),
                    qail_version: row.get(6).and_then(|v| v.as_ref()).cloned(),
                    actor: row.get(7).and_then(|v| v.as_ref()).cloned(),
                    started_at_ms: parse_i64_field(row.get(8).and_then(|v| v.as_ref())),
                    finished_at_ms: parse_i64_field(row.get(9).and_then(|v| v.as_ref())),
                    duration_ms: parse_i64_field(row.get(10).and_then(|v| v.as_ref())),
                    affected_rows_est: parse_i64_field(row.get(11).and_then(|v| v.as_ref())),
                    risk_summary: row.get(12).and_then(|v| v.as_ref()).cloned(),
                    shadow_checksum: row.get(13).and_then(|v| v.as_ref()).cloned(),
                    receipt_sig: row.get(14).and_then(|v| v.as_ref()).cloned(),
                },
            );
        }
    }

    if applied_versions.is_empty() {
        println!(
            "{}",
            "No applied folder migrations found to roll back.".green()
        );
        return Ok(());
    }

    let target = normalize_target_version(to_version);
    let mut up_group_by_version = HashMap::<String, String>::new();
    for version in &applied_versions {
        let group = up_by_version
            .get(version)
            .map(|m| m.group_key.clone())
            .ok_or_else(|| {
                anyhow!(
                    "Missing migration metadata for applied version '{}'",
                    version
                )
            })?;
        up_group_by_version.insert(version.clone(), group);
    }

    let plan = plan_rollbacks(&applied_versions, &up_group_by_version, target)?;
    if plan.versions_to_delete.is_empty() {
        let label = target.unwrap_or("base");
        println!(
            "{} Already at requested target {}",
            "✓".green(),
            label.cyan()
        );
        return Ok(());
    }

    println!(
        "{} {} group rollback(s), {} applied version(s) to remove",
        "→".cyan(),
        plan.groups_to_rollback.len(),
        plan.versions_to_delete.len()
    );

    validate_rollback_receipts(
        &plan.versions_to_delete,
        &up_by_version,
        &applied_receipts,
        policy.receipt_validation,
    )?;

    let target_label = target.unwrap_or("base");
    let rollback_version =
        execute_rollback_plan_atomic(&mut driver, &plan, &down_by_group, target_label, None)
            .await?;

    println!(
        "{}",
        format!(
            "✓ Rolled back {} group(s) / {} version(s) to {}",
            plan.groups_to_rollback.len(),
            plan.versions_to_delete.len(),
            target_label
        )
        .green()
        .bold()
    );
    println!("  Recorded rollback receipt: {}", rollback_version.cyan());
    Ok(())
}

async fn execute_rollback_plan_atomic(
    driver: &mut PgDriver,
    plan: &RollbackPlan,
    down_by_group: &HashMap<String, MigrationFile>,
    target_label: &str,
    failpoint_override: Option<&str>,
) -> Result<String> {
    driver
        .begin()
        .await
        .map_err(|e| anyhow!("Failed to begin rollback transaction: {}", e))?;
    let started_ms = now_epoch_ms();

    let mut executed_sql = String::new();
    for (idx, group) in plan.groups_to_rollback.iter().enumerate() {
        let Some(down_migration) = down_by_group.get(group) else {
            let _ = driver.rollback().await;
            return Err(anyhow!(
                "Missing down migration for group '{}'. Reconcile migration files before rollback.",
                group
            ));
        };

        let content = match fs::read_to_string(&down_migration.path) {
            Ok(content) => content,
            Err(err) => {
                let _ = driver.rollback().await;
                return Err(anyhow!(
                    "Failed to read rollback file '{}': {}",
                    down_migration.path.display(),
                    err
                ));
            }
        };

        let cmds = match parse_qail_to_commands_strict(&content).with_context(|| {
            format!(
                "Failed to compile rollback migration '{}'",
                down_migration.display_name
            )
        }) {
            Ok(cmds) => cmds,
            Err(err) => {
                let _ = driver.rollback().await;
                return Err(err);
            }
        };
        let sql = commands_to_sql(&cmds);
        if !sql.trim().is_empty() {
            executed_sql.push_str(&sql);
            executed_sql.push_str(";\n");
        }

        println!(
            "  {} [{} / {}] {}",
            "→".cyan(),
            idx + 1,
            plan.groups_to_rollback.len(),
            down_migration.display_name.yellow()
        );
        for (step, cmd) in cmds.iter().enumerate() {
            if let Err(err) = driver.execute(cmd).await {
                let _ = driver.rollback().await;
                return Err(anyhow!(
                    "Rollback failed at group '{}' step {}/{}: {}\nTransaction rolled back - database unchanged.",
                    group,
                    step + 1,
                    cmds.len(),
                    err
                ));
            }
        }
    }

    if let Err(err) = maybe_failpoint_override(
        "rollback.after_down_before_history_delete",
        failpoint_override,
    ) {
        let _ = driver.rollback().await;
        return Err(err);
    }

    for version in &plan.versions_to_delete {
        let delete_cmd = Qail::del("_qail_migrations").where_eq("version", version.as_str());
        if let Err(err) = driver.execute(&delete_cmd).await {
            let _ = driver.rollback().await;
            return Err(anyhow!(
                "Failed to update migration history (delete '{}'): {}",
                version,
                err
            ));
        }
    }

    let finished_ms = now_epoch_ms();
    let rollback_version = format!("rollback_{}", crate::time::timestamp_version());
    let receipt = MigrationReceipt {
        version: rollback_version.clone(),
        name: format!("rollback_to_{}", target_label),
        checksum: crate::time::md5_hex(&executed_sql),
        sql_up: executed_sql,
        git_sha: runtime_git_sha(),
        qail_version: env!("CARGO_PKG_VERSION").to_string(),
        actor: runtime_actor(),
        started_at_ms: Some(started_ms),
        finished_at_ms: Some(finished_ms),
        duration_ms: Some(finished_ms.saturating_sub(started_ms)),
        affected_rows_est: Some(i64::try_from(plan.versions_to_delete.len()).unwrap_or(i64::MAX)),
        risk_summary: Some(format!(
            "source=rollback;to={};groups={};versions={}",
            target_label,
            plan.groups_to_rollback.len(),
            plan.versions_to_delete.join(",")
        )),
        shadow_checksum: None,
    };
    if let Err(err) = write_migration_receipt(driver, &receipt).await {
        let _ = driver.rollback().await;
        return Err(anyhow!("Failed to record rollback receipt: {}", err));
    }

    driver
        .commit()
        .await
        .map_err(|e| anyhow!("Failed to commit rollback transaction: {}", e))?;

    Ok(rollback_version)
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

fn normalize_target_version(to_version: &str) -> Option<&str> {
    let trimmed = to_version.trim();
    if trimmed.eq_ignore_ascii_case("base")
        || trimmed.eq_ignore_ascii_case("root")
        || trimmed == "0"
    {
        None
    } else {
        Some(trimmed)
    }
}

#[derive(Debug)]
struct RollbackPlan {
    groups_to_rollback: Vec<String>,
    versions_to_delete: Vec<String>,
}

fn plan_rollbacks(
    applied_versions: &[String],
    up_group_by_version: &HashMap<String, String>,
    target: Option<&str>,
) -> Result<RollbackPlan> {
    let target_idx = match target {
        Some(target_version) => Some(
            applied_versions
                .iter()
                .position(|v| v == target_version)
                .ok_or_else(|| {
                    anyhow!(
                        "Target version '{}' is not currently applied",
                        target_version
                    )
                })?,
        ),
        None => None,
    };

    if let Some(idx) = target_idx {
        let target_group = up_group_by_version
            .get(&applied_versions[idx])
            .ok_or_else(|| {
                anyhow!(
                    "Missing group metadata for target '{}'",
                    applied_versions[idx]
                )
            })?;
        let has_newer_same_group = applied_versions[idx + 1..].iter().any(|v| {
            up_group_by_version
                .get(v)
                .is_some_and(|group| group == target_group)
        });
        if has_newer_same_group {
            bail!(
                "Target version '{}' is not at a rollback boundary for group '{}'. \
                 Choose the latest applied phase for that group, or roll back to an older group.",
                applied_versions[idx],
                target_group
            );
        }
    }

    let first_idx_to_remove = target_idx.map_or(0, |idx| idx + 1);
    let versions_to_delete = applied_versions[first_idx_to_remove..].to_vec();

    let mut groups_to_rollback = Vec::<String>::new();
    let mut seen_groups = HashSet::<String>::new();
    for version in versions_to_delete.iter().rev() {
        let Some(group) = up_group_by_version.get(version) else {
            bail!(
                "Missing group metadata for applied version '{}'. Reconcile migration files.",
                version
            );
        };
        if seen_groups.insert(group.clone()) {
            groups_to_rollback.push(group.clone());
        }
    }

    Ok(RollbackPlan {
        groups_to_rollback,
        versions_to_delete,
    })
}

fn index_down_by_group(down: &[MigrationFile]) -> Result<HashMap<String, MigrationFile>> {
    let mut index = HashMap::<String, MigrationFile>::new();
    for mig in down {
        if index.insert(mig.group_key.clone(), mig.clone()).is_some() {
            bail!(
                "Ambiguous rollback mapping: multiple down migrations for group '{}'",
                mig.group_key
            );
        }
    }
    Ok(index)
}

fn ensure_up_has_down_pairing(up: &[MigrationFile], down: &[MigrationFile]) -> Result<()> {
    if up.is_empty() {
        return Ok(());
    }
    let down_index = index_down_by_group(down)?;
    let mut missing = BTreeSet::<String>::new();
    for up_mig in up {
        if !down_index.contains_key(up_mig.group_key.as_str()) {
            missing.insert(up_mig.group_key.clone());
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    bail!(
        "Missing rollback migrations (*.down.qail or <dir>/down.qail) for group(s): {}",
        missing.into_iter().collect::<Vec<_>>().join(", ")
    );
}

fn validate_rollback_receipts(
    versions_to_delete: &[String],
    up_by_version: &HashMap<String, MigrationFile>,
    applied_receipts: &HashMap<String, StoredMigrationReceipt>,
    mode: ReceiptValidationMode,
) -> Result<()> {
    if versions_to_delete.is_empty() {
        return Ok(());
    }

    for version in versions_to_delete {
        let Some(up_migration) = up_by_version.get(version) else {
            let msg = format!(
                "Missing local migration metadata for version '{}'. Reconcile migrations before rollback.",
                version
            );
            match mode {
                ReceiptValidationMode::Warn => {
                    eprintln!("  {} {}", "⚠".yellow(), msg.yellow());
                    continue;
                }
                ReceiptValidationMode::Error => bail!("{}", msg),
            }
        };
        let Some(stored_receipt) = applied_receipts.get(version) else {
            let msg = format!(
                "Missing receipt metadata in _qail_migrations for applied version '{}'.",
                version
            );
            match mode {
                ReceiptValidationMode::Warn => {
                    eprintln!("  {} {}", "⚠".yellow(), msg.yellow());
                    continue;
                }
                ReceiptValidationMode::Error => bail!("{}", msg),
            }
        };

        let Some(stored_checksum) = stored_receipt.checksum.as_deref() else {
            let msg = format!(
                "Missing checksum in _qail_migrations for applied version '{}'.",
                version
            );
            match mode {
                ReceiptValidationMode::Warn => {
                    eprintln!("  {} {}", "⚠".yellow(), msg.yellow());
                    continue;
                }
                ReceiptValidationMode::Error => bail!("{}", msg),
            }
        };

        match verify_stored_receipt_signature(stored_receipt) {
            ReceiptSignatureStatus::DisabledNoKey | ReceiptSignatureStatus::Valid => {}
            ReceiptSignatureStatus::Missing => {
                let msg = format!(
                    "Missing receipt signature for applied version '{}'. \
                     Set migrations.policy.receipt_validation=warn to bypass temporarily while backfilling signatures.",
                    version
                );
                match mode {
                    ReceiptValidationMode::Warn => {
                        eprintln!("  {} {}", "⚠".yellow(), msg.yellow());
                    }
                    ReceiptValidationMode::Error => bail!("{}", msg),
                }
            }
            ReceiptSignatureStatus::Invalid => {
                let msg = format!(
                    "Receipt signature verification failed for applied version '{}'.",
                    version
                );
                match mode {
                    ReceiptValidationMode::Warn => {
                        eprintln!("  {} {}", "⚠".yellow(), msg.yellow());
                    }
                    ReceiptValidationMode::Error => bail!("{}", msg),
                }
            }
        }

        let content = fs::read_to_string(&up_migration.path)
            .with_context(|| format!("Failed to read {}", up_migration.path.display()))?;
        let expected_checksum =
            compute_expected_migration_checksum(&content, up_migration.phase, 5000)?;
        if &expected_checksum == stored_checksum {
            continue;
        }
        let msg = format!(
            "Receipt checksum drift detected for '{}': stored={}, local={}. \
             Refusing rollback until migration history and local files are reconciled.",
            version, stored_checksum, expected_checksum
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

fn parse_i64_field(value: Option<&String>) -> Option<i64> {
    value.and_then(|v| v.parse::<i64>().ok())
}

#[cfg(test)]
mod tests {
    use super::{
        RollbackPlan, execute_rollback_plan_atomic, plan_rollbacks, validate_rollback_receipts,
    };
    use crate::migrations::ReceiptValidationMode;
    use crate::migrations::apply::MigrationFile;
    use crate::migrations::apply::types::MigrationPhase;
    use qail_core::prelude::Qail;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn rollback_plan_dedupes_group_in_reverse_order() {
        let applied = vec![
            "001.expand".to_string(),
            "001.backfill".to_string(),
            "002.expand".to_string(),
            "003.expand".to_string(),
        ];
        let mut groups = HashMap::new();
        groups.insert("001.expand".to_string(), "001".to_string());
        groups.insert("001.backfill".to_string(), "001".to_string());
        groups.insert("002.expand".to_string(), "002".to_string());
        groups.insert("003.expand".to_string(), "003".to_string());

        let plan = plan_rollbacks(&applied, &groups, Some("001.backfill")).expect("valid plan");
        assert_eq!(
            plan.versions_to_delete,
            vec!["002.expand".to_string(), "003.expand".to_string()]
        );
        assert_eq!(
            plan.groups_to_rollback,
            vec!["003".to_string(), "002".to_string()]
        );
    }

    #[test]
    fn rollback_plan_rejects_partial_group_target() {
        let applied = vec![
            "001.expand".to_string(),
            "001.backfill".to_string(),
            "001.contract".to_string(),
        ];
        let mut groups = HashMap::new();
        groups.insert("001.expand".to_string(), "001".to_string());
        groups.insert("001.backfill".to_string(), "001".to_string());
        groups.insert("001.contract".to_string(), "001".to_string());

        let err = plan_rollbacks(&applied, &groups, Some("001.expand"))
            .expect_err("must reject partial group");
        assert!(
            err.to_string().contains("not at a rollback boundary"),
            "error should mention boundary violation"
        );
    }

    #[test]
    fn rollback_receipt_validation_detects_checksum_drift() {
        let root = std::env::temp_dir().join(format!(
            "qail_rollback_receipt_validation_{}",
            std::process::id()
        ));
        let _ = fs::create_dir_all(&root);
        let path = root.join("001_add_users.up.qail");
        fs::write(&path, "table users (id int)\n").expect("write migration");

        let mut up_by_version = HashMap::new();
        up_by_version.insert(
            "001_add_users.up.qail".to_string(),
            MigrationFile {
                group_key: "001_add_users".to_string(),
                sort_key: "001_add_users.up.qail".to_string(),
                display_name: "001_add_users.up.qail".to_string(),
                path: PathBuf::from(path),
                phase: MigrationPhase::Expand,
            },
        );

        let mut receipts = HashMap::new();
        receipts.insert(
            "001_add_users.up.qail".to_string(),
            crate::migrations::StoredMigrationReceipt {
                version: "001_add_users.up.qail".to_string(),
                name: Some("001_add_users.up.qail".to_string()),
                checksum: Some("deadbeef".to_string()),
                sql_up: Some("table users (id int)\n".to_string()),
                git_sha: None,
                qail_version: Some(env!("CARGO_PKG_VERSION").to_string()),
                actor: None,
                started_at_ms: None,
                finished_at_ms: None,
                duration_ms: None,
                affected_rows_est: None,
                risk_summary: None,
                shadow_checksum: None,
                receipt_sig: None,
            },
        );

        let err = validate_rollback_receipts(
            &["001_add_users.up.qail".to_string()],
            &up_by_version,
            &receipts,
            ReceiptValidationMode::Error,
        )
        .expect_err("drift must fail");
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

    async fn receipt_name_exists(pg: &mut qail_pg::PgDriver, name: &str) -> bool {
        let cmd = Qail::get("_qail_migrations")
            .column("name")
            .where_eq("name", name)
            .limit(1);
        match pg.query_ast(&cmd).await {
            Ok(result) => !result.rows.is_empty(),
            Err(_) => false,
        }
    }

    #[tokio::test]
    async fn rollback_failpoint_before_history_delete_preserves_history_in_real_db() {
        let Some(url) = std::env::var("QAIL_TEST_DB_URL").ok() else {
            eprintln!("Skipping rollback failpoint DB test (set QAIL_TEST_DB_URL)");
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
        let applied_version = format!("fp_rollback_applied_{}.up.qail", suffix);
        let target_label = format!("fp_target_{}", suffix);
        let rollback_name = format!("rollback_to_{}", target_label);

        let cleanup_applied =
            Qail::del("_qail_migrations").where_eq("version", applied_version.as_str());
        let cleanup_receipt =
            Qail::del("_qail_migrations").where_eq("name", rollback_name.as_str());
        let _ = pg.execute(&cleanup_applied).await;
        let _ = pg.execute(&cleanup_receipt).await;

        let seed_cmd = Qail::add("_qail_migrations")
            .set_value("version", applied_version.as_str())
            .set_value("name", "fp_rollback_seed")
            .set_value("checksum", "fp_rollback_seed_checksum")
            .set_value("sql_up", "-- rollback seed");
        pg.execute(&seed_cmd)
            .await
            .expect("seed applied version row");

        let plan = RollbackPlan {
            groups_to_rollback: Vec::new(),
            versions_to_delete: vec![applied_version.clone()],
        };
        let down_by_group = HashMap::new();
        let err = execute_rollback_plan_atomic(
            &mut pg,
            &plan,
            &down_by_group,
            target_label.as_str(),
            Some("rollback.after_down_before_history_delete"),
        )
        .await
        .expect_err("failpoint should abort rollback transaction");

        assert!(
            err.to_string().contains("Injected failpoint triggered"),
            "unexpected failpoint error: {err}"
        );
        assert!(
            version_exists(&mut pg, applied_version.as_str()).await,
            "applied migration history should remain after failpoint rollback"
        );
        assert!(
            !receipt_name_exists(&mut pg, rollback_name.as_str()).await,
            "rollback receipt should not be recorded when failpoint triggers"
        );
    }
}
