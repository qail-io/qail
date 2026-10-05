//! Live PostgreSQL check that a release cancelled while the server is still
//! answering its COMMIT gives the pool slot back.
//!
//! A deferred constraint trigger holds the COMMIT in `pg_sleep`, so the
//! release is reliably mid-flight when its caller goes away.
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test release_cancellation_live -- --ignored --nocapture

use std::time::{Duration, Instant};

use qail_core::rls::RlsContext;
use qail_pg::{PgPool, PgResult, PoolConfig};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn a_release_cancelled_during_its_commit_gives_the_slot_back() -> PgResult<()> {
    let pool = PgPool::connect(
        PoolConfig::from_url(&database_url())?
            .min_connections(0)
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(2)),
    )
    .await?;

    let mut conn = pool.acquire_with_rls(RlsContext::global()).await?;
    conn.get_mut()?
        .execute_simple(
            "CREATE TEMP TABLE qail_slow_commit (id integer); \
             CREATE FUNCTION pg_temp.qail_slow_commit() RETURNS trigger LANGUAGE plpgsql \
               AS $$ BEGIN PERFORM pg_sleep(3); RETURN NULL; END $$; \
             CREATE CONSTRAINT TRIGGER qail_slow_commit AFTER INSERT ON qail_slow_commit \
               DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
               EXECUTE FUNCTION pg_temp.qail_slow_commit(); \
             INSERT INTO qail_slow_commit VALUES (1)",
        )
        .await?;

    // The caller gives up while the server sleeps inside the COMMIT.
    let cancelled = tokio::time::timeout(Duration::from_millis(300), conn.release_checked()).await;
    assert!(
        cancelled.is_err(),
        "the COMMIT sleeps 3 s, so the release is still in flight"
    );

    let started = Instant::now();
    let next = pool.acquire_raw().await;
    println!(
        "checkout after a cancelled release: {:?} in {:?}",
        next.as_ref().map(|_| "ok"),
        started.elapsed()
    );
    next?.release_checked().await?;

    let stats = pool.stats().await;
    println!(
        "pool after: active={} idle={} max={}",
        stats.active, stats.idle, stats.max_size
    );
    assert_eq!(stats.active, 0, "every checkout ended");
    Ok(())
}
