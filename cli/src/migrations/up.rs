//! Migration UP operations

use crate::colors::*;
use anyhow::Result;
use qail_core::migrate::{diff_schemas, parse_qail_file};
use qail_core::transpiler::ToSql;
use qail_pg::driver::PgDriver;

use crate::migrations::risk::preflight_lock_risk;
use crate::migrations::verify::post_apply_verify;
use crate::migrations::{
    EnforcementMode, MigrationReceipt, acquire_migration_lock, ensure_migration_table,
    load_migration_policy, now_epoch_ms, runtime_actor, runtime_git_sha, stable_cmds_checksum,
    write_migration_receipt,
};
use crate::util::parse_pg_url;

/// Apply migrations forward using qail-pg native driver.
pub async fn migrate_up(
    schema_diff_path: &str,
    url: &str,
    codebase: Option<&str>,
    force: bool,
    allow_destructive: bool,
    allow_no_shadow_receipt: bool,
    allow_lock_risk: bool,
    wait_for_lock: bool,
) -> Result<()> {
    println!("{} {}", "Migrating UP:".cyan().bold(), url.yellow());

    let (old_schema, new_schema, cmds) =
        if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
            let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
            let old_path = parts[0];
            let new_path = parts[1];

            let old_schema = parse_qail_file(old_path)
                .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;
            let new_schema = parse_qail_file(new_path)
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
    let planned_checksum = stable_cmds_checksum(&cmds);
    let policy = load_migration_policy()?;
    println!(
        "  {} policy destructive={} lock_risk={} threshold={} shadow_receipt={} receipt_validation={}",
        "→".cyan(),
        format!("{:?}", policy.destructive).to_ascii_lowercase(),
        format!("{:?}", policy.lock_risk).to_ascii_lowercase(),
        policy.lock_risk_max_score,
        policy.require_shadow_receipt,
        format!("{:?}", policy.receipt_validation).to_ascii_lowercase()
    );

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
            println!(
                "{}",
                "⚠️  BREAKING CHANGES DETECTED IN CODEBASE".red().bold()
            );
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
                    "Migration BLOCKED. Fix your code first, or use --force to proceed anyway."
                        .red()
                );
                return Err(anyhow::anyhow!(
                    "Migration blocked: breaking code references detected. \
                     Update code or re-run with --force."
                ));
            } else {
                println!();
                println!(
                    "{}",
                    "⚠️  Proceeding anyway due to --force flag...".yellow()
                );
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
    acquire_migration_lock(&mut driver, "migrate up", wait_for_lock).await?;

    // === PHASE 0.5: Shadow Receipt Verification ===
    if !policy.require_shadow_receipt {
        println!(
            "{}",
            "⚠️  Shadow receipt verification disabled by migrations.policy.require_shadow_receipt=false"
                .yellow()
        );
    } else if allow_no_shadow_receipt {
        if !policy.allow_no_shadow_receipt {
            return Err(anyhow::anyhow!(
                "Migration blocked: --allow-no-shadow-receipt is disabled by migrations.policy.allow_no_shadow_receipt=false"
            ));
        }
        println!(
            "{}",
            "⚠️  Skipping shadow receipt verification due to --allow-no-shadow-receipt".yellow()
        );
    } else {
        let has_receipt =
            crate::shadow::has_verified_shadow_receipt_with_driver(&mut driver, &planned_checksum)
                .await?;
        if !has_receipt {
            return Err(anyhow::anyhow!(
                "Migration blocked: no verified shadow receipt for checksum {}.\n\
                 Run 'qail migrate shadow <old.qail:new.qail> --url <db>' first, or override with --allow-no-shadow-receipt.",
                planned_checksum
            ));
        }
        println!(
            "  {} Verified shadow receipt checksum: {}",
            "✓".green(),
            planned_checksum.cyan()
        );
    }

    // === PHASE 0.75: Lock Risk Preflight ===
    preflight_lock_risk(
        &mut driver,
        &cmds,
        allow_lock_risk,
        policy.lock_risk,
        policy.lock_risk_max_score,
    )
    .await?;

    // === PHASE 1: Impact Analysis ===
    use crate::backup::{
        MigrationChoice, analyze_impact, create_snapshots, display_impact, prompt_migration_choice,
    };

    let mut impacts = Vec::new();
    for cmd in &cmds {
        let impact = analyze_impact(&mut driver, cmd).await?;
        impacts.push(impact);
    }

    let has_destructive = impacts.iter().any(|i| i.is_destructive);

    if has_destructive {
        display_impact(&impacts);

        match policy.destructive {
            EnforcementMode::Deny => {
                return Err(anyhow::anyhow!(
                    "Migration blocked: destructive operations are disabled by migrations.policy.destructive=deny"
                ));
            }
            EnforcementMode::RequireFlag if !allow_destructive => {
                return Err(anyhow::anyhow!(
                    "Migration blocked: destructive operations detected.\n\
                     Re-run with --allow-destructive to continue."
                ));
            }
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

        let choice = prompt_migration_choice();

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
                let migration_version = crate::time::timestamp_version();
                create_db_snapshots(&mut driver, &migration_version, &impacts).await?;
            }
            MigrationChoice::Proceed => {
                println!("{}", "Proceeding without backup...".dimmed());
            }
        }
    }

    // Begin transaction for atomic migration
    println!("{}", "Starting transaction...".dimmed());
    let apply_started_ms = now_epoch_ms();
    driver
        .begin()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start transaction: {}", e))?;

    // Ensure migration table exists (AST-native bootstrap)
    ensure_migration_table(&mut driver)
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

        let sql = cmd.to_sql();
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

    // === PHASE 2: Post-apply Verification Gates ===
    post_apply_verify(&mut driver, &new_schema, &cmds).await?;

    let apply_finished_ms = now_epoch_ms();
    let version = crate::time::timestamp_version();
    let checksum = crate::time::md5_hex(&sql_up_all);
    let affected_rows_est: i64 = impacts
        .iter()
        .map(|i| i64::try_from(i.rows_affected).unwrap_or(i64::MAX))
        .sum();
    let destructive_ops = impacts.iter().filter(|i| i.is_destructive).count();
    let risk_summary = format!(
        "destructive_ops={};estimated_rows={};allow_destructive_flag={};allow_lock_risk_flag={};shadow_receipt_required={};policy_destructive={:?};policy_lock_risk={:?};policy_lock_risk_max_score={}",
        destructive_ops,
        affected_rows_est,
        allow_destructive,
        allow_lock_risk,
        policy.require_shadow_receipt && !allow_no_shadow_receipt,
        policy.destructive,
        policy.lock_risk,
        policy.lock_risk_max_score
    );

    let receipt = MigrationReceipt {
        version: version.clone(),
        name: format!("auto_{}", version),
        checksum,
        sql_up: sql_up_all,
        git_sha: runtime_git_sha(),
        qail_version: env!("CARGO_PKG_VERSION").to_string(),
        actor: runtime_actor(),
        started_at_ms: Some(apply_started_ms),
        finished_at_ms: Some(apply_finished_ms),
        duration_ms: Some(apply_finished_ms.saturating_sub(apply_started_ms)),
        affected_rows_est: Some(affected_rows_est),
        risk_summary: Some(risk_summary),
        shadow_checksum: Some(planned_checksum),
    };

    write_migration_receipt(&mut driver, &receipt)
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

#[cfg(test)]
mod tests {
    use super::migrate_up;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("{}_{}_{}", prefix, std::process::id(), nanos))
    }

    #[tokio::test]
    async fn blocked_breaking_changes_returns_error() {
        let root = unique_temp_dir("qail_migrate_up_blocked");
        fs::create_dir_all(&root).expect("create temp root");

        let old_schema = root.join("old.qail");
        let new_schema = root.join("new.qail");
        let codebase = root.join("src");
        fs::create_dir_all(&codebase).expect("create codebase");

        fs::write(
            &old_schema,
            r#"
table users {
  id uuid primary_key
  email text nullable
}
"#,
        )
        .expect("write old schema");
        fs::write(
            &new_schema,
            r#"
table users {
  id uuid primary_key
}
"#,
        )
        .expect("write new schema");
        fs::write(
            codebase.join("queries.ts"),
            r#"const q = "get users fields id, email where id = $1";"#,
        )
        .expect("write code reference");

        let schema_diff = format!("{}:{}", old_schema.display(), new_schema.display());
        let result = migrate_up(
            &schema_diff,
            "postgres://localhost/testdb",
            Some(codebase.to_str().expect("utf-8 codebase path")),
            false,
            false,
            true,
            true,
            false,
        )
        .await;

        let _ = fs::remove_dir_all(&root);

        assert!(
            result.is_err(),
            "blocked migration should return error (non-zero exit path)"
        );
    }
}
