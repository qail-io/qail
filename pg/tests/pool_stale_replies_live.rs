//! Live PostgreSQL checks that a pooled connection whose previous user left
//! replies unread goes back to the pool in sync.
//!
//! A future dropped after its request reached the server (client disconnect,
//! `tokio::time::timeout`) leaves that request's reply on the socket. The
//! pool reset must not read that reply as its own: if it does, the reset's
//! real reply stays unread and the next checkout's Bind reads a stale
//! CommandComplete ("completion before BindComplete").
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test pool_stale_replies_live -- --ignored --nocapture

use qail_core::ast::Qail;
use qail_core::rls::RlsContext;
use qail_pg::{PgDriver, PgEncoder, PgPool, PgResult, PoolConfig, PooledConnection};
use std::time::Duration;
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

async fn pool() -> PgResult<PgPool> {
    PgPool::connect(
        PoolConfig::from_url(&database_url())?
            .min_connections(0)
            .max_connections(1),
    )
    .await
}

async fn probe_table(driver: &mut PgDriver) -> PgResult<String> {
    let table = format!("qail_stale_probe_{}", Uuid::new_v4().simple());
    driver
        .execute_simple(&format!(
            "CREATE TABLE {table} (id integer PRIMARY KEY); INSERT INTO {table} VALUES (42)"
        ))
        .await?;
    Ok(table)
}

/// The leaked-drop cleanup runs on a spawned task; wait for it to put the
/// connection back (or destroy it).
async fn wait_for_cleanup(pool: &PgPool) -> usize {
    for _ in 0..100 {
        if pool.stats().await.active == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    pool.idle_count().await
}

async fn backend_pid(conn: &mut PooledConnection) -> PgResult<String> {
    let rows = conn
        .get_mut()?
        .simple_query("SELECT pg_backend_pid()")
        .await?;
    Ok(rows[0].text(0))
}

/// The production read: a pooled AST fetch through the extended protocol.
async fn read_probe(pool: &PgPool, table: &str) -> PgResult<Vec<String>> {
    let mut conn = pool.acquire_raw().await?;
    let rows = conn
        .fetch_all_uncached(&Qail::get(table).select_all())
        .await;
    conn.release().await;
    Ok(rows?.iter().map(|row| row.text(0)).collect())
}

#[tokio::test]
#[ignore = "Requires a live PostgreSQL at QAIL_TEST_DB_URL"]
async fn query_cancelled_mid_flight_then_dropped_leaves_the_next_checkout_in_sync() -> PgResult<()>
{
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = probe_table(&mut driver).await?;
    let pool = pool().await?;

    let mut conn = pool.acquire_raw().await?;
    let pid_before = backend_pid(&mut conn).await?;
    // The request reaches the server; the future is dropped while it waits
    // for the reply, exactly as a disconnected HTTP handler is.
    let cancelled = tokio::time::timeout(
        Duration::from_millis(50),
        conn.get_mut()?.execute_simple("SELECT pg_sleep(0.3)"),
    )
    .await;
    assert!(cancelled.is_err(), "the sleep outlives the timeout");
    drop(conn);

    let idle = wait_for_cleanup(&pool).await;
    println!("after drop: idle={idle}");

    let read = read_probe(&pool, &table).await;
    println!("next checkout read: {read:?}");
    assert_eq!(read?, vec!["42".to_string()]);

    // The drained connection was kept, not replaced.
    let mut conn = pool.acquire_raw().await?;
    let pid_after = backend_pid(&mut conn).await?;
    conn.release().await;
    println!("backend pid before={pid_before} after={pid_after}");
    assert_eq!(
        pid_before, pid_after,
        "the connection was reused after draining"
    );

    driver
        .execute_simple(&format!("DROP TABLE {table}"))
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a live PostgreSQL at QAIL_TEST_DB_URL"]
async fn rls_setup_cancelled_then_released_leaves_the_next_checkout_in_sync() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = probe_table(&mut driver).await?;
    let pool = pool().await?;

    // Two reply cycles left unread, as when a pipelined RLS setup + query is
    // abandoned: a simple-query cycle with rows, then a second one.
    let mut conn = pool.acquire_with_rls(RlsContext::global()).await?;
    let mut wire = PgEncoder::try_encode_query_string("SELECT 'stale-one'")
        .map_err(|e| qail_pg::PgError::Encode(e.to_string()))?
        .to_vec();
    wire.extend_from_slice(
        &PgEncoder::try_encode_query_string("SELECT 'stale-two'")
            .map_err(|e| qail_pg::PgError::Encode(e.to_string()))?,
    );
    conn.get_mut()?.send_bytes(&wire).await?;
    let released = conn.release_checked().await;
    println!("release_checked over two unread cycles: {released:?}");
    released?;

    let read = read_probe(&pool, &table).await;
    println!("next checkout read: {read:?}");
    assert_eq!(read?, vec!["42".to_string()]);

    // The next RLS checkout's setup and fetch see their own replies.
    let mut conn = pool.acquire_with_rls(RlsContext::global()).await?;
    let rows = conn
        .fetch_all_uncached(&Qail::get(table.as_str()).select_all())
        .await;
    conn.release_checked().await?;
    let rows: Vec<String> = rows?.iter().map(|row| row.text(0)).collect();
    assert_eq!(rows, vec!["42".to_string()]);

    driver
        .execute_simple(&format!("DROP TABLE {table}"))
        .await?;
    Ok(())
}
