//! Global advisory lock for migration operations.

use crate::colors::*;
use anyhow::{Result, anyhow, bail};
use qail_core::prelude::Qail;
use qail_pg::PgDriver;
use tokio::time::{Duration, sleep};

const LOCK_CLASS_ID: i32 = 20_801; // "QA"
const LOCK_OBJECT_ID: i32 = 19_783; // "MG"
const LOCK_WAIT_POLL_MS: u64 = 500;

pub async fn acquire_migration_lock(
    driver: &mut PgDriver,
    operation: &str,
    wait_for_lock: bool,
) -> Result<()> {
    if wait_for_lock {
        println!(
            "  {} Waiting for global migration lock...",
            "⏳".yellow().dimmed()
        );
        loop {
            if try_acquire_lock(driver).await? {
                println!("  {} Acquired global migration lock", "✓".green());
                return Ok(());
            }
            sleep(Duration::from_millis(LOCK_WAIT_POLL_MS)).await;
        }
    }

    if try_acquire_lock(driver).await? {
        println!("  {} Acquired global migration lock", "✓".green());
        return Ok(());
    }

    bail!(
        "Another migration operation is already running. \
         Could not acquire global migration lock for '{}'. Re-run with --wait-for-lock or retry after it completes.",
        operation
    );
}

async fn try_acquire_lock(driver: &mut PgDriver) -> Result<bool> {
    let lock_source = format!(
        "pg_try_advisory_lock({}, {})",
        LOCK_CLASS_ID, LOCK_OBJECT_ID
    );
    let lock_cmd = Qail::get(lock_source.as_str())
        .column("pg_try_advisory_lock")
        .limit(1);
    let rows = driver
        .fetch_all(&lock_cmd)
        .await
        .map_err(|e| anyhow!("Failed to acquire migration advisory lock: {}", e))?;
    Ok(rows.first().and_then(|r| r.get_bool(0)).unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::{LOCK_CLASS_ID, LOCK_OBJECT_ID};

    #[test]
    fn advisory_lock_ids_are_stable() {
        assert_eq!(LOCK_CLASS_ID, 20_801);
        assert_eq!(LOCK_OBJECT_ID, 19_783);
    }
}
