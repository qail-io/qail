//! Lock/impact risk preflight for migration apply.

use crate::colors::*;
use crate::migrations::EnforcementMode;
use anyhow::{Result, anyhow};
use qail_core::ast::{Action, JoinKind, Qail};
use qail_pg::driver::PgDriver;
use std::collections::HashMap;

const MIB: i64 = 1024 * 1024;
const GIB: i64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockLevel {
    None,
    ShareUpdateExclusive,
    AccessExclusive,
}

impl std::fmt::Display for LockLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "NONE"),
            Self::ShareUpdateExclusive => write!(f, "SHARE UPDATE EXCLUSIVE"),
            Self::AccessExclusive => write!(f, "ACCESS EXCLUSIVE"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct TableStats {
    est_rows: i64,
    total_bytes: i64,
}

#[derive(Debug, Clone)]
struct RiskEntry {
    step: usize,
    action: Action,
    table: String,
    lock_level: LockLevel,
    est_rows: i64,
    total_bytes: i64,
    score: u8,
    reason: String,
}

/// Fail risky migration plans unless explicitly allowed.
pub async fn preflight_lock_risk(
    driver: &mut PgDriver,
    cmds: &[Qail],
    allow_lock_risk: bool,
    policy_mode: EnforcementMode,
    policy_max_score: u8,
) -> Result<()> {
    let mut stats_cache: HashMap<String, TableStats> = HashMap::new();
    let mut risky = Vec::<RiskEntry>::new();

    for (idx, cmd) in cmds.iter().enumerate() {
        let lock_level = lock_level_for_action(cmd.action);
        if matches!(lock_level, LockLevel::None) {
            continue;
        }

        let stats = if let Some(existing) = stats_cache.get(&cmd.table) {
            *existing
        } else {
            let fetched = fetch_table_stats(driver, &cmd.table).await?;
            stats_cache.insert(cmd.table.clone(), fetched);
            fetched
        };

        let score = risk_score(lock_level, stats);
        let reason = risk_reason(lock_level, stats, score, policy_max_score);
        if let Some(reason) = reason {
            risky.push(RiskEntry {
                step: idx + 1,
                action: cmd.action,
                table: cmd.table.clone(),
                lock_level,
                est_rows: stats.est_rows,
                total_bytes: stats.total_bytes,
                score,
                reason,
            });
        }
    }

    if risky.is_empty() {
        println!("  {} Lock-risk preflight passed", "✓".green());
        return Ok(());
    }

    println!();
    println!("{}", "🚦 Lock Risk Preflight".yellow().bold());
    for entry in &risky {
        println!(
            "  {} [{}] {} {}.{} rows≈{} size≈{} score={} ({})",
            "⚠".yellow(),
            entry.step.to_string().cyan(),
            format!("{}", entry.action).yellow(),
            entry.table.cyan(),
            entry.lock_level.to_string().yellow(),
            format_compact_count(entry.est_rows).red().bold(),
            format_bytes(entry.total_bytes).red().bold(),
            entry.score.to_string().red().bold(),
            entry.reason.red()
        );
    }
    println!();

    match policy_mode {
        EnforcementMode::Allow => {
            println!(
                "{}",
                "⚠️  Proceeding despite lock-risk findings due to migrations.policy.lock_risk=allow"
                    .yellow()
            );
            Ok(())
        }
        EnforcementMode::Deny => Err(anyhow!(
            "Migration blocked by lock-risk policy (migrations.policy.lock_risk=deny, {} risky operation(s)).",
            risky.len()
        )),
        EnforcementMode::RequireFlag => {
            if allow_lock_risk {
                println!(
                    "{}",
                    "⚠️  Proceeding despite lock-risk findings due to --allow-lock-risk".yellow()
                );
                Ok(())
            } else {
                Err(anyhow!(
                    "Migration blocked by lock-risk guardrails ({} risky operation(s)). Re-run with --allow-lock-risk to override.",
                    risky.len()
                ))
            }
        }
    }
}

fn lock_level_for_action(action: Action) -> LockLevel {
    match action {
        Action::Drop
        | Action::AlterDrop
        | Action::AlterType
        | Action::AlterSetNotNull
        | Action::AlterDropNotNull
        | Action::AlterSetDefault
        | Action::AlterDropDefault
        | Action::Alter
        | Action::Mod
        | Action::DropCol
        | Action::RenameCol
        | Action::Truncate => LockLevel::AccessExclusive,
        Action::Index | Action::DropIndex => LockLevel::ShareUpdateExclusive,
        _ => LockLevel::None,
    }
}

fn lock_weight(level: LockLevel) -> u8 {
    match level {
        LockLevel::None => 0,
        LockLevel::ShareUpdateExclusive => 55,
        LockLevel::AccessExclusive => 85,
    }
}

fn rows_weight(rows: i64) -> u8 {
    match rows {
        r if r >= 50_000_000 => 25,
        r if r >= 10_000_000 => 20,
        r if r >= 1_000_000 => 14,
        r if r >= 100_000 => 8,
        r if r >= 10_000 => 3,
        _ => 0,
    }
}

fn bytes_weight(bytes: i64) -> u8 {
    match bytes {
        b if b >= 100 * GIB => 25,
        b if b >= 10 * GIB => 18,
        b if b >= GIB => 10,
        b if b >= 256 * MIB => 5,
        _ => 0,
    }
}

fn risk_score(level: LockLevel, stats: TableStats) -> u8 {
    lock_weight(level)
        .saturating_add(rows_weight(stats.est_rows))
        .saturating_add(bytes_weight(stats.total_bytes))
}

fn risk_reason(level: LockLevel, stats: TableStats, score: u8, max_score: u8) -> Option<String> {
    if score >= max_score {
        return Some(format!(
            "combined lock + size risk score {} exceeds policy threshold {}",
            score, max_score
        ));
    }

    match level {
        LockLevel::AccessExclusive if stats.est_rows >= 100_000 => {
            Some("ACCESS EXCLUSIVE lock on non-trivial table may block reads/writes".to_string())
        }
        LockLevel::AccessExclusive if stats.total_bytes >= 512 * MIB => {
            Some("ACCESS EXCLUSIVE lock on large relation may cause lock waits".to_string())
        }
        LockLevel::ShareUpdateExclusive if stats.est_rows >= 20_000_000 => {
            Some("table is very large; maintenance lock window is risky".to_string())
        }
        _ if score >= 90 => Some("combined lock + size risk score is high".to_string()),
        _ => None,
    }
}

async fn fetch_table_stats(driver: &mut PgDriver, table: &str) -> Result<TableStats> {
    let cmd = Qail::get("pg_class c")
        .columns(["c.reltuples", "pg_total_relation_size(c.oid)"])
        .join(JoinKind::Inner, "pg_namespace n", "n.oid", "c.relnamespace")
        .where_eq("n.nspname", "public")
        .where_eq("c.relname", table)
        .in_vals("c.relkind", ["r", "p", "m"])
        .limit(1);
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed lock-risk stats query for table '{}': {}", table, e))?;

    if let Some(row) = rows.first() {
        let est_rows = row.get_f64(0).unwrap_or(0.0).max(0.0).round() as i64;
        let total_bytes = row.get_i64(1).unwrap_or(0);
        return Ok(TableStats {
            est_rows,
            total_bytes,
        });
    }

    Ok(TableStats::default())
}

fn format_compact_count(n: i64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", (n as f64) / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", (n as f64) / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", (n as f64) / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_bytes(n: i64) -> String {
    if n >= GIB {
        format!("{:.1}GiB", (n as f64) / (GIB as f64))
    } else if n >= MIB {
        format!("{:.1}MiB", (n as f64) / (MIB as f64))
    } else if n >= 1024 {
        format!("{:.1}KiB", (n as f64) / 1024.0)
    } else {
        format!("{}B", n)
    }
}

#[cfg(test)]
mod tests {
    use super::{LockLevel, TableStats, lock_level_for_action, risk_reason, risk_score};
    use qail_core::ast::Action;

    #[test]
    fn lock_level_mapping_for_destructive_alter() {
        assert_eq!(
            lock_level_for_action(Action::AlterType),
            LockLevel::AccessExclusive
        );
        assert_eq!(
            lock_level_for_action(Action::Index),
            LockLevel::ShareUpdateExclusive
        );
        assert_eq!(lock_level_for_action(Action::Get), LockLevel::None);
    }

    #[test]
    fn high_row_access_exclusive_is_risky() {
        let stats = TableStats {
            est_rows: 500_000,
            total_bytes: 100 * 1024 * 1024,
        };
        let score = risk_score(LockLevel::AccessExclusive, stats);
        assert!(risk_reason(LockLevel::AccessExclusive, stats, score, 90).is_some());
    }

    #[test]
    fn tiny_table_access_exclusive_not_flagged() {
        let stats = TableStats {
            est_rows: 200,
            total_bytes: 16 * 1024,
        };
        let score = risk_score(LockLevel::AccessExclusive, stats);
        assert!(risk_reason(LockLevel::AccessExclusive, stats, score, 90).is_none());
    }
}
