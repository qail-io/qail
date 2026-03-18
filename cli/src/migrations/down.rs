//! Migration DOWN (rollback) operations

use crate::colors::*;
use anyhow::Result;
use qail_core::migrate::{diff_schemas, parse_qail_file};
use qail_core::prelude::{Action, Expr};
use qail_core::transpiler::ToSql;
use qail_pg::driver::PgDriver;

use super::types::is_narrowing_type;
use crate::migrations::{
    MigrationReceipt, acquire_migration_lock, ensure_migration_table, now_epoch_ms, runtime_actor,
    runtime_git_sha, write_migration_receipt,
};
use crate::util::parse_pg_url;

/// Rollback migrations using qail-pg native driver.
pub async fn migrate_down(schema_diff_path: &str, url: &str) -> Result<()> {
    println!("{} {}", "Migrating DOWN:".cyan().bold(), url.yellow());

    // For rollback, user provides: current_schema:target_schema
    let cmds = if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
        let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
        let current_path = parts[0];
        let target_path = parts[1];

        let current_schema = parse_qail_file(current_path)
            .map_err(|e| anyhow::anyhow!("Failed to parse current schema: {}", e))?;
        let target_schema = parse_qail_file(target_path)
            .map_err(|e| anyhow::anyhow!("Failed to parse target schema: {}", e))?;

        diff_schemas(&current_schema, &target_schema)
    } else {
        return Err(anyhow::anyhow!(
            "Rollback requires two .qail files.\n\
             Use format: qail migrate down current.qail:target.qail <url>"
        ));
    };

    if cmds.is_empty() {
        println!("{}", "No rollbacks to apply.".green());
        return Ok(());
    }

    // Check for unsafe type casts (e.g., TEXT -> INT)
    let unsafe_type_changes: Vec<_> = cmds
        .iter()
        .filter(|cmd| cmd.action == Action::AlterType)
        .filter_map(|cmd| {
            if let Some(Expr::Def {
                name, data_type, ..
            }) = cmd.columns.first()
            {
                let target = data_type.as_str();
                if is_narrowing_type(target) {
                    Some(format!("{}.{} → {}", cmd.table, name, target))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    if !unsafe_type_changes.is_empty() {
        println!();
        println!(
            "{} {}",
            "⚠️ Unsafe type changes detected!".yellow().bold(),
            "Rollback may fail.".dimmed()
        );
        println!("{}", "━".repeat(50).dimmed());
        for change in &unsafe_type_changes {
            println!("  {} {}", "•".red(), change.yellow());
        }
        println!("{}", "━".repeat(50).dimmed());
        println!(
            "{}",
            "These type narrowing operations require explicit USING clause.".dimmed()
        );
        println!(
            "{}",
            "PostgreSQL cannot automatically cast TEXT → INT.".dimmed()
        );
        println!();
        print!("Continue anyway? [y/N] ");
        std::io::Write::flush(&mut std::io::stdout())?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("{}", "Rollback cancelled.".yellow());
            return Ok(());
        }
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

    ensure_migration_table(&mut driver)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bootstrap migration table: {}", e))?;
    acquire_migration_lock(&mut driver, "migrate down").await?;

    // Begin transaction for atomic rollback
    println!("{}", "Starting transaction...".dimmed());
    let started_ms = now_epoch_ms();
    driver
        .begin()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start transaction: {}", e))?;

    let mut applied = 0;
    let mut sql_down_all = String::new();
    for (i, cmd) in cmds.iter().enumerate() {
        println!(
            "  {} {} {}",
            format!("[{}/{}]", i + 1, cmds.len()).cyan(),
            format!("{}", cmd.action).yellow(),
            &cmd.table
        );

        sql_down_all.push_str(&cmd.to_sql());
        sql_down_all.push_str(";\n");

        if let Err(e) = driver.execute(cmd).await {
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

    let finished_ms = now_epoch_ms();
    let version = format!("down_{}", crate::time::timestamp_version());
    let checksum = crate::time::md5_hex(&sql_down_all);
    let receipt = MigrationReceipt {
        version: version.clone(),
        name: format!("rollback_{}", version),
        checksum,
        sql_up: sql_down_all,
        git_sha: runtime_git_sha(),
        qail_version: env!("CARGO_PKG_VERSION").to_string(),
        actor: runtime_actor(),
        started_at_ms: Some(started_ms),
        finished_at_ms: Some(finished_ms),
        duration_ms: Some(finished_ms.saturating_sub(started_ms)),
        affected_rows_est: None,
        risk_summary: Some(format!("source=down;schema_diff={}", schema_diff_path)),
        shadow_checksum: None,
    };
    write_migration_receipt(&mut driver, &receipt)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to record rollback receipt: {}", e))?;

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
    println!("  Recorded rollback receipt: {}", version.cyan());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::migrate_down;

    #[tokio::test]
    async fn invalid_schema_diff_returns_error() {
        let result = migrate_down("invalid-schema-diff", "postgres://localhost/testdb").await;
        assert!(result.is_err(), "invalid rollback input must fail");
    }
}
