//! Migration receipt metadata + persistence.

use anyhow::{Result, anyhow};
use qail_pg::PgDriver;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const RECEIPT_ALTER_SQL: &[&str] = &[
    "ALTER TABLE _qail_migrations ADD COLUMN IF NOT EXISTS git_sha VARCHAR(64)",
    "ALTER TABLE _qail_migrations ADD COLUMN IF NOT EXISTS qail_version VARCHAR(32)",
    "ALTER TABLE _qail_migrations ADD COLUMN IF NOT EXISTS actor VARCHAR(255)",
    "ALTER TABLE _qail_migrations ADD COLUMN IF NOT EXISTS started_at_ms BIGINT",
    "ALTER TABLE _qail_migrations ADD COLUMN IF NOT EXISTS finished_at_ms BIGINT",
    "ALTER TABLE _qail_migrations ADD COLUMN IF NOT EXISTS duration_ms BIGINT",
    "ALTER TABLE _qail_migrations ADD COLUMN IF NOT EXISTS affected_rows_est BIGINT",
    "ALTER TABLE _qail_migrations ADD COLUMN IF NOT EXISTS risk_summary TEXT",
    "ALTER TABLE _qail_migrations ADD COLUMN IF NOT EXISTS shadow_checksum VARCHAR(64)",
];

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
    for stmt in RECEIPT_ALTER_SQL {
        driver.execute_raw(stmt).await.map_err(|e| {
            anyhow!(
                "Failed to ensure migration receipt column via '{}': {}",
                stmt,
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
    let insert_sql = format!(
        "INSERT INTO _qail_migrations \
         (version, name, checksum, sql_up, git_sha, qail_version, actor, started_at_ms, finished_at_ms, duration_ms, affected_rows_est, risk_summary, shadow_checksum) \
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
        sql_opt_str(Some(&receipt.version)),
        sql_opt_str(Some(&receipt.name)),
        sql_opt_str(Some(&receipt.checksum)),
        sql_opt_str(Some(&receipt.sql_up)),
        sql_opt_str(receipt.git_sha.as_deref()),
        sql_opt_str(Some(&receipt.qail_version)),
        sql_opt_str(receipt.actor.as_deref()),
        sql_opt_i64(receipt.started_at_ms),
        sql_opt_i64(receipt.finished_at_ms),
        sql_opt_i64(receipt.duration_ms),
        sql_opt_i64(receipt.affected_rows_est),
        sql_opt_str(receipt.risk_summary.as_deref()),
        sql_opt_str(receipt.shadow_checksum.as_deref()),
    );

    driver
        .execute_raw(&insert_sql)
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

fn sql_opt_str(v: Option<&str>) -> String {
    match v {
        Some(s) => format!("'{}'", s.replace('\'', "''")),
        None => "NULL".to_string(),
    }
}

fn sql_opt_i64(v: Option<i64>) -> String {
    v.map(|n| n.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}
