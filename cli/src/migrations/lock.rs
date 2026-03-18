//! Global advisory lock for migration operations.

use crate::colors::*;
use anyhow::{Result, anyhow, bail};
use qail_core::prelude::Qail;
use qail_pg::PgDriver;
use tokio::time::{Duration, sleep};
use std::time::Instant;

const LOCK_CLASS_ID: i32 = 20_801; // "QA"
const LOCK_OBJECT_ID: i32 = 19_783; // "MG"
const LOCK_WAIT_POLL_MS: u64 = 500;

pub async fn acquire_migration_lock(
    driver: &mut PgDriver,
    operation: &str,
    wait_for_lock: bool,
    lock_timeout_secs: Option<u64>,
) -> Result<()> {
    let should_wait = wait_for_lock || lock_timeout_secs.is_some();
    let deadline = lock_timeout_secs
        .map(Duration::from_secs)
        .and_then(|timeout| Instant::now().checked_add(timeout));

    if should_wait {
        println!(
            "  {} Waiting for global migration lock...",
            "⏳".yellow().dimmed()
        );
        loop {
            if try_acquire_lock(driver).await? {
                println!("  {} Acquired global migration lock", "✓".green());
                return Ok(());
            }
            if let Some(deadline) = deadline
                && Instant::now() >= deadline
            {
                bail!(
                    "Timed out waiting for global migration lock for '{}' after {} second(s).",
                    operation,
                    lock_timeout_secs.unwrap_or_default()
                );
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
    use super::{LOCK_CLASS_ID, LOCK_OBJECT_ID, acquire_migration_lock};
    use qail_pg::PgDriver;
    use tokio::time::Duration;
    use std::time::Instant;

    #[test]
    fn advisory_lock_ids_are_stable() {
        assert_eq!(LOCK_CLASS_ID, 20_801);
        assert_eq!(LOCK_OBJECT_ID, 19_783);
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
        acquire_migration_lock(&mut holder, "test holder", true, Some(5))
            .await
            .expect("holder should acquire lock");
        let err = acquire_migration_lock(&mut contender, "test contender", false, None)
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
        acquire_migration_lock(&mut holder, "test timeout holder", true, Some(5))
            .await
            .expect("timeout holder should acquire lock");
        let started = Instant::now();
        let timeout_err = acquire_migration_lock(&mut waiter, "test timeout waiter", true, Some(1))
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
                .contains("Timed out waiting for global migration lock"),
            "unexpected timeout error: {timeout_err}"
        );
        drop(waiter);
        drop(holder);

        // Scenario 3: waiter succeeds once holder releases lock.
        let mut holder = connect_test_driver(&url).await;
        let mut waiter = connect_test_driver(&url).await;
        acquire_migration_lock(&mut holder, "test eventual holder", true, Some(5))
            .await
            .expect("eventual holder should acquire lock");
        let waiter_fut = acquire_migration_lock(&mut waiter, "test eventual waiter", true, Some(5));
        let release_fut = async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            drop(holder);
        };
        let ((), wait_result) = tokio::join!(release_fut, waiter_fut);
        wait_result.expect("waiter should acquire lock after holder is released");
    }
}
