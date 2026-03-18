//! Migration receipt metadata + persistence.

use anyhow::{Result, anyhow};
use hmac::{Hmac, Mac};
use qail_core::ast::{Action, Constraint, Expr, Qail};
use qail_pg::PgDriver;
use sha2::Sha256;
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
        ("receipt_sig", "text"),
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
    let receipt_sig = runtime_receipt_hmac_key()
        .as_deref()
        .and_then(|key| compute_receipt_hmac(receipt, key));
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
        .set_opt("shadow_checksum", receipt.shadow_checksum.as_deref())
        .set_opt("receipt_sig", receipt_sig.as_deref());

    driver
        .execute(&insert_cmd)
        .await
        .map_err(|e| anyhow!("Failed to write migration receipt: {}", e))?;
    Ok(())
}

fn runtime_receipt_hmac_key() -> Option<String> {
    [
        "QAIL_MIGRATION_RECEIPT_HMAC_KEY",
        "QAIL_RECEIPT_HMAC_KEY",
        "QAIL_MIGRATION_SIGNING_KEY",
    ]
    .iter()
    .find_map(|name| {
        std::env::var(name)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    })
}

fn compute_receipt_hmac(receipt: &MigrationReceipt, key: &str) -> Option<String> {
    if key.trim().is_empty() {
        return None;
    }
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key.as_bytes()).ok()?;
    mac.update(canonical_receipt_material(receipt).as_bytes());
    let digest = mac.finalize().into_bytes();
    Some(digest.iter().map(|b| format!("{:02x}", b)).collect())
}

fn canonical_receipt_material(receipt: &MigrationReceipt) -> String {
    let mut material = String::new();
    material.push_str("version=");
    material.push_str(&receipt.version);
    material.push('\n');
    material.push_str("name=");
    material.push_str(&receipt.name);
    material.push('\n');
    material.push_str("checksum=");
    material.push_str(&receipt.checksum);
    material.push('\n');
    material.push_str("sql_up=");
    material.push_str(&receipt.sql_up);
    material.push('\n');
    material.push_str("git_sha=");
    material.push_str(receipt.git_sha.as_deref().unwrap_or(""));
    material.push('\n');
    material.push_str("qail_version=");
    material.push_str(&receipt.qail_version);
    material.push('\n');
    material.push_str("actor=");
    material.push_str(receipt.actor.as_deref().unwrap_or(""));
    material.push('\n');
    material.push_str("started_at_ms=");
    material.push_str(&receipt.started_at_ms.unwrap_or_default().to_string());
    material.push('\n');
    material.push_str("finished_at_ms=");
    material.push_str(&receipt.finished_at_ms.unwrap_or_default().to_string());
    material.push('\n');
    material.push_str("duration_ms=");
    material.push_str(&receipt.duration_ms.unwrap_or_default().to_string());
    material.push('\n');
    material.push_str("affected_rows_est=");
    material.push_str(&receipt.affected_rows_est.unwrap_or_default().to_string());
    material.push('\n');
    material.push_str("risk_summary=");
    material.push_str(receipt.risk_summary.as_deref().unwrap_or(""));
    material.push('\n');
    material.push_str("shadow_checksum=");
    material.push_str(receipt.shadow_checksum.as_deref().unwrap_or(""));
    material
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

#[cfg(test)]
mod tests {
    use super::{MigrationReceipt, canonical_receipt_material, compute_receipt_hmac};

    fn sample_receipt() -> MigrationReceipt {
        MigrationReceipt {
            version: "001_add_users.up.qail".to_string(),
            name: "001_add_users.up.qail".to_string(),
            checksum: "abc123".to_string(),
            sql_up: "CREATE TABLE users (id int);".to_string(),
            git_sha: Some("deadbeef".to_string()),
            qail_version: "0.25.0".to_string(),
            actor: Some("tester".to_string()),
            started_at_ms: Some(1000),
            finished_at_ms: Some(1100),
            duration_ms: Some(100),
            affected_rows_est: Some(0),
            risk_summary: Some("source=test".to_string()),
            shadow_checksum: None,
        }
    }

    #[test]
    fn receipt_hmac_is_deterministic_for_same_payload() {
        let receipt = sample_receipt();
        let a = compute_receipt_hmac(&receipt, "top-secret").expect("hmac");
        let b = compute_receipt_hmac(&receipt, "top-secret").expect("hmac");
        assert_eq!(a, b, "same receipt+key must produce same signature");
    }

    #[test]
    fn receipt_hmac_changes_when_payload_changes() {
        let mut receipt = sample_receipt();
        let before = compute_receipt_hmac(&receipt, "top-secret").expect("hmac");
        receipt.sql_up.push_str("\nALTER TABLE users ADD COLUMN email text;");
        let after = compute_receipt_hmac(&receipt, "top-secret").expect("hmac");
        assert_ne!(before, after, "signature must change when receipt payload changes");
    }

    #[test]
    fn canonical_material_contains_core_fields() {
        let material = canonical_receipt_material(&sample_receipt());
        assert!(material.contains("version=001_add_users.up.qail"));
        assert!(material.contains("checksum=abc123"));
        assert!(material.contains("sql_up=CREATE TABLE users (id int);"));
    }
}
