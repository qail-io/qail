//! Global advisory lock for migration operations.

use crate::colors::*;
use anyhow::{Result, anyhow, bail};
use qail_core::prelude::Qail;
use qail_pg::PgDriver;
use std::time::Instant;
use tokio::time::{Duration, sleep};

const LOCK_CLASS_ID: i32 = 20_801; // "QA"
const LOCK_OBJECT_SEED: i32 = 19_783; // "MG"
const LOCK_WAIT_POLL_MS: u64 = 500;

pub async fn acquire_migration_lock(
    driver: &mut PgDriver,
    operation: &str,
    wait_for_lock: bool,
    lock_timeout_secs: Option<u64>,
    lock_scope: Option<&str>,
) -> Result<()> {
    let should_wait = wait_for_lock || lock_timeout_secs.is_some();
    let deadline = lock_timeout_secs
        .map(Duration::from_secs)
        .and_then(|timeout| Instant::now().checked_add(timeout));
    let lock_object_id = scoped_lock_object_id(lock_scope);
    let scope_label = normalize_scope(lock_scope).unwrap_or_else(|| "global".to_string());

    if should_wait {
        println!(
            "  {} Waiting for migration lock (scope: {})...",
            "⏳".yellow().dimmed(),
            scope_label.cyan()
        );
        loop {
            if try_acquire_lock(driver, lock_object_id).await? {
                println!(
                    "  {} Acquired migration lock (scope: {})",
                    "✓".green(),
                    scope_label.cyan()
                );
                return Ok(());
            }
            if let Some(deadline) = deadline
                && Instant::now() >= deadline
            {
                bail!(
                    "Timed out waiting for migration lock for '{}' (scope: {}) after {} second(s).",
                    operation,
                    scope_label,
                    lock_timeout_secs.unwrap_or_default()
                );
            }
            sleep(Duration::from_millis(LOCK_WAIT_POLL_MS)).await;
        }
    }

    if try_acquire_lock(driver, lock_object_id).await? {
        println!(
            "  {} Acquired migration lock (scope: {})",
            "✓".green(),
            scope_label.cyan()
        );
        return Ok(());
    }

    bail!(
        "Another migration operation is already running. \
         Could not acquire migration lock for '{}' (scope: {}). \
         Re-run with --wait-for-lock or retry after it completes.",
        operation,
        scope_label
    );
}

fn normalize_scope(lock_scope: Option<&str>) -> Option<String> {
    lock_scope
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase())
}

fn scoped_lock_object_id(lock_scope: Option<&str>) -> i32 {
    let Some(scope) = normalize_scope(lock_scope) else {
        return LOCK_OBJECT_SEED;
    };

    // Stable FNV-1a hash mixed with a fixed seed so lock IDs are deterministic.
    let mut hash: u32 = 0x811c9dc5;
    for byte in scope.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    let mixed = hash ^ u32::try_from(LOCK_OBJECT_SEED).unwrap_or_default();
    i32::try_from(mixed & 0x7fff_ffff).unwrap_or(LOCK_OBJECT_SEED)
}

async fn try_acquire_lock(driver: &mut PgDriver, lock_object_id: i32) -> Result<bool> {
    let lock_source = format!(
        "pg_try_advisory_lock({}, {})",
        LOCK_CLASS_ID, lock_object_id
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
    use super::{LOCK_CLASS_ID, LOCK_OBJECT_SEED, acquire_migration_lock, scoped_lock_object_id};
    use qail_pg::PgDriver;
    use std::time::Instant;
    use tokio::time::Duration;

    #[test]
    fn advisory_lock_class_and_seed_are_stable() {
        assert_eq!(LOCK_CLASS_ID, 20_801);
        assert_eq!(LOCK_OBJECT_SEED, 19_783);
    }

    #[test]
    fn scoped_lock_id_is_stable_and_distinct() {
        let users_db = scoped_lock_object_id(Some("users_db"));
        let inventory_db = scoped_lock_object_id(Some("inventory_db"));
        assert_eq!(users_db, scoped_lock_object_id(Some("users_db")));
        assert_ne!(users_db, inventory_db);
        assert_eq!(LOCK_OBJECT_SEED, scoped_lock_object_id(Some("")));
        assert_eq!(LOCK_OBJECT_SEED, scoped_lock_object_id(None));
    }

    fn lock_test_db_url() -> Option<String> {
        std::env::var("QAIL_TEST_DB_URL").ok()
    }

    async fn connect_test_driver(url: &str) -> PgDriver {
        PgDriver::connect_url(url)
            .await
            .expect("Failed to connect test driver using QAIL_TEST_DB_URL")
    }

    #[tokio::test]
    async fn advisory_lock_real_db_contention_and_timeout() {
        let Some(url) = lock_test_db_url() else {
            eprintln!("Skipping advisory lock DB test (set QAIL_TEST_DB_URL)");
            return;
        };

        // Scenario 1: fail fast when lock is already held.
        let mut holder = connect_test_driver(&url).await;
        let mut contender = connect_test_driver(&url).await;
        acquire_migration_lock(&mut holder, "test holder", true, Some(5), Some(&url))
            .await
            .expect("holder should acquire lock");
        let err = acquire_migration_lock(&mut contender, "test contender", false, None, Some(&url))
            .await
            .expect_err("contender should fail fast while lock is held");
        assert!(
            err.to_string()
                .contains("Another migration operation is already running"),
            "unexpected fast-fail error: {err}"
        );
        drop(contender);
        drop(holder);

        // Scenario 2: waiting contender times out.
        let mut holder = connect_test_driver(&url).await;
        let mut waiter = connect_test_driver(&url).await;
        acquire_migration_lock(
            &mut holder,
            "test timeout holder",
            true,
            Some(5),
            Some(&url),
        )
        .await
        .expect("timeout holder should acquire lock");
        let started = Instant::now();
        let timeout_err = acquire_migration_lock(
            &mut waiter,
            "test timeout waiter",
            true,
            Some(1),
            Some(&url),
        )
        .await
        .expect_err("waiter should time out while holder retains lock");
        assert!(
            started.elapsed() >= Duration::from_millis(900),
            "wait timeout returned too quickly: {:?}",
            started.elapsed()
        );
        assert!(
            timeout_err
                .to_string()
                .contains("Timed out waiting for migration lock"),
            "unexpected timeout error: {timeout_err}"
        );
        drop(waiter);
        drop(holder);

        // Scenario 3: waiter succeeds once holder releases lock.
        let mut holder = connect_test_driver(&url).await;
        let mut waiter = connect_test_driver(&url).await;
        acquire_migration_lock(
            &mut holder,
            "test eventual holder",
            true,
            Some(5),
            Some(&url),
        )
        .await
        .expect("eventual holder should acquire lock");
        let waiter_fut = acquire_migration_lock(
            &mut waiter,
            "test eventual waiter",
            true,
            Some(5),
            Some(&url),
        );
        let release_fut = async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            drop(holder);
        };
        let ((), wait_result) = tokio::join!(release_fut, waiter_fut);
        wait_result.expect("waiter should acquire lock after holder is released");
    }
}
