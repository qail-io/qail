//! Global advisory lock for migration operations.

use crate::colors::*;
use anyhow::{Result, anyhow, bail};
use qail_core::prelude::Qail;
use qail_pg::PgDriver;

const LOCK_CLASS_ID: i32 = 20_801; // "QA"
const LOCK_OBJECT_ID: i32 = 19_783; // "MG"

pub async fn acquire_migration_lock(driver: &mut PgDriver, operation: &str) -> Result<()> {
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
    let acquired = rows.first().and_then(|r| r.get_bool(0)).unwrap_or(false);
    if !acquired {
        bail!(
            "Another migration operation is already running. \
             Could not acquire global migration lock for '{}'. Retry after it completes.",
            operation
        );
    }
    println!("  {} Acquired global migration lock", "✓".green());
    Ok(())
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
