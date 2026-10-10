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
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Records every WARN and ERROR event as `LEVEL field=value ...`.
#[derive(Clone, Default)]
struct WarnCapture(Arc<Mutex<Vec<String>>>);

impl WarnCapture {
    fn lines(&self) -> Vec<String> {
        self.0.lock().map(|lines| lines.clone()).unwrap_or_default()
    }
}

impl tracing::Subscriber for WarnCapture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::WARN
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut line = event.metadata().level().to_string();
        event.record(
            &mut |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                line.push_str(&format!(" {}={value:?}", field.name()));
            },
        );
        if let Ok(mut lines) = self.0.lock() {
            lines.push(line);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

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
async fn release_logs_a_commit_the_server_rolled_back_with_its_caller() -> PgResult<()> {
    let capture = WarnCapture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
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
    let release_line = line!() + 1;
    conn.release().await;
    let kept = kept_ids(&mut driver, &table).await?;
    let lines = capture.lines();
    println!("release after an aborted transaction logged: {lines:#?}; rows kept: {kept}");
    assert_eq!(kept, 0, "the server kept nothing");
    let rolled_back: Vec<&String> = lines
        .iter()
        .filter(|line| line.contains("pool_release_rolled_back"))
        .collect();
    assert_eq!(rolled_back.len(), 1, "{lines:#?}");
    let line = rolled_back[0];
    assert!(line.starts_with("WARN "), "{line}");
    assert!(
        line.contains(&format!("caller={}:{release_line}:", file!())),
        "{line}"
    );
    assert!(line.contains("ROLLBACK"), "{line}");

    // A release whose COMMIT lands logs nothing.
    let mut conn = pool.acquire_with_rls(RlsContext::global()).await?;
    conn.get_mut()?
        .execute_simple(&format!("INSERT INTO {table} VALUES (2)"))
        .await?;
    conn.release().await;
    assert_eq!(kept_ids(&mut driver, &table).await?, 1);
    assert_eq!(capture.lines().len(), lines.len(), "{:#?}", capture.lines());

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
