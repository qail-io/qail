//! Live PostgreSQL checks that a COMMIT the server answered with ROLLBACK
//! reads as an error.
//!
//! PostgreSQL ends a failed transaction on COMMIT with the command tag
//! `ROLLBACK` and no ErrorResponse, so a driver that only watches for
//! errors reports the lost writes as committed.
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test commit_outcome_live -- --ignored --nocapture

use qail_core::rls::RlsContext;
use qail_pg::{PgDriver, PgPool, PgResult, PoolConfig};
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

async fn scratch_table(driver: &mut PgDriver) -> PgResult<String> {
    let table = format!("qail_commit_probe_{}", Uuid::new_v4().simple());
    driver
        .execute_simple(&format!("CREATE TABLE {table} (id integer PRIMARY KEY)"))
        .await?;
    Ok(table)
}

async fn kept_ids(driver: &mut PgDriver, table: &str) -> PgResult<usize> {
    Ok(driver
        .simple_query(&format!("SELECT id FROM {table}"))
        .await?
        .len())
}

async fn pool() -> PgResult<PgPool> {
    PgPool::connect(
        PoolConfig::from_url(&database_url())?
            .min_connections(0)
            .max_connections(1),
    )
    .await
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn release_checked_reports_a_commit_the_server_rolled_back() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = scratch_table(&mut driver).await?;
    let pool = pool().await?;

    let mut conn = pool.acquire_with_rls(RlsContext::global()).await?;
    conn.get_mut()?
        .execute_simple(&format!("INSERT INTO {table} VALUES (1)"))
        .await?;
    let duplicate = conn
        .get_mut()?
        .execute_simple(&format!("INSERT INTO {table} VALUES (1)"))
        .await;
    assert!(
        duplicate.is_err(),
        "the duplicate key aborts the transaction"
    );
    let outcome = conn.release_checked().await;
    let kept = kept_ids(&mut driver, &table).await?;
    println!("release_checked after an aborted transaction: {outcome:?}; rows kept: {kept}");
    let err = outcome.expect_err("a COMMIT answered ROLLBACK must not read as success");
    assert!(err.to_string().contains("ROLLBACK"), "{err}");
    assert_eq!(kept, 0, "the server kept nothing");

    // The rolled-back connection went back to the pool clean: the next
    // checkout on it writes and commits.
    let mut conn = pool.acquire_with_rls(RlsContext::global()).await?;
    conn.get_mut()?
        .execute_simple(&format!("INSERT INTO {table} VALUES (2)"))
        .await?;
    conn.release_checked().await?;
    assert_eq!(kept_ids(&mut driver, &table).await?, 1);

    driver
        .execute_simple(&format!("DROP TABLE {table}"))
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn release_checked_keeps_a_commit_that_landed() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = scratch_table(&mut driver).await?;
    let pool = pool().await?;

    let mut conn = pool.acquire_with_rls(RlsContext::global()).await?;
    conn.get_mut()?
        .execute_simple(&format!("INSERT INTO {table} VALUES (1)"))
        .await?;
    conn.release_checked().await?;
    assert_eq!(kept_ids(&mut driver, &table).await?, 1);

    driver
        .execute_simple(&format!("DROP TABLE {table}"))
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn commit_reports_a_transaction_the_server_rolled_back() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = scratch_table(&mut driver).await?;

    driver.begin().await?;
    driver
        .execute_simple(&format!("INSERT INTO {table} VALUES (1)"))
        .await?;
    let duplicate = driver
        .execute_simple(&format!("INSERT INTO {table} VALUES (1)"))
        .await;
    assert!(
        duplicate.is_err(),
        "the duplicate key aborts the transaction"
    );
    let outcome = driver.commit().await;
    // The transaction is over either way: the connection takes the next
    // statement outside it.
    let kept = kept_ids(&mut driver, &table).await?;
    println!("commit after an aborted transaction: {outcome:?}; rows kept: {kept}");
    let err = outcome.expect_err("a COMMIT answered ROLLBACK must not read as success");
    assert!(err.to_string().contains("ROLLBACK"), "{err}");
    assert_eq!(kept, 0, "the server kept nothing");

    driver
        .execute_simple(&format!("DROP TABLE {table}"))
        .await?;
    Ok(())
}
