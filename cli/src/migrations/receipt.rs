//! Migration receipt metadata + persistence.

use anyhow::{Result, anyhow};
use qail_core::ast::{Action, Constraint, Expr, Qail};
use qail_pg::PgDriver;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct MigrationReceipt {
    pub version: String,
    pub name: String,
    pub checksum: String,
    pub sql_up: String,
    pub git_sha: Option<String>,
    pub qail_version: String,
    pub actor: Option<String>,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub duration_ms: Option<i64>,
    pub affected_rows_est: Option<i64>,
    pub risk_summary: Option<String>,
    pub shadow_checksum: Option<String>,
}

pub async fn ensure_migration_receipt_columns(driver: &mut PgDriver) -> Result<()> {
    let columns: &[(&str, &str)] = &[
        ("git_sha", "text"),
        ("qail_version", "text"),
        ("actor", "text"),
        ("started_at_ms", "bigint"),
        ("finished_at_ms", "bigint"),
        ("duration_ms", "bigint"),
        ("affected_rows_est", "bigint"),
        ("risk_summary", "text"),
        ("shadow_checksum", "text"),
    ];

    for (name, ty) in columns {
        let exists_cmd = Qail::get("information_schema.columns")
            .column("1")
            .where_eq("table_schema", "public")
            .where_eq("table_name", "_qail_migrations")
            .where_eq("column_name", *name)
            .limit(1);
        let rows = driver
            .fetch_all(&exists_cmd)
            .await
            .map_err(|e| anyhow!("Failed to check migration receipt column '{}': {}", name, e))?;
        if !rows.is_empty() {
            continue;
        }

        let alter_cmd = Qail {
            action: Action::Mod,
            table: "_qail_migrations".to_string(),
            columns: vec![Expr::Def {
                name: (*name).to_string(),
                data_type: (*ty).to_string(),
                constraints: vec![Constraint::Nullable],
            }],
            ..Default::default()
        };
        driver.execute(&alter_cmd).await.map_err(|e| {
            anyhow!(
                "Failed to ensure migration receipt column '{}': {}",
                name,
                e
            )
        })?;
    }
    Ok(())
}

pub async fn write_migration_receipt(
    driver: &mut PgDriver,
    receipt: &MigrationReceipt,
) -> Result<()> {
    let insert_cmd = Qail::add("_qail_migrations")
        .set_value("version", receipt.version.as_str())
        .set_value("name", receipt.name.as_str())
        .set_value("checksum", receipt.checksum.as_str())
        .set_value("sql_up", receipt.sql_up.as_str())
        .set_opt("git_sha", receipt.git_sha.as_deref())
        .set_value("qail_version", receipt.qail_version.as_str())
        .set_opt("actor", receipt.actor.as_deref())
        .set_opt("started_at_ms", receipt.started_at_ms)
        .set_opt("finished_at_ms", receipt.finished_at_ms)
        .set_opt("duration_ms", receipt.duration_ms)
        .set_opt("affected_rows_est", receipt.affected_rows_est)
        .set_opt("risk_summary", receipt.risk_summary.as_deref())
        .set_opt("shadow_checksum", receipt.shadow_checksum.as_deref());

    driver
        .execute(&insert_cmd)
        .await
        .map_err(|e| anyhow!("Failed to write migration receipt: {}", e))?;
    Ok(())
}

pub fn now_epoch_ms() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

pub fn runtime_actor() -> Option<String> {
    let candidates = [
        "QAIL_ACTOR",
        "GIT_AUTHOR_NAME",
        "USER",
        "USERNAME",
        "SUDO_USER",
    ];
    for key in candidates {
        if let Ok(v) = std::env::var(key) {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

pub fn runtime_git_sha() -> Option<String> {
    let candidates = [
        "QAIL_GIT_SHA",
        "GIT_SHA",
        "GITHUB_SHA",
        "CI_COMMIT_SHA",
        "BUILDKITE_COMMIT",
    ];
    for key in candidates {
        if let Ok(v) = std::env::var(key) {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.chars().take(12).collect());
            }
        }
    }

    if let Ok(out) = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        && out.status.success()
    {
        let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !sha.is_empty() {
            return Some(sha);
        }
    }

    None
}
