//! Nested acquires and `PoolConfig::nested_reserve`.
//!
//! Every pool here starts with one idle connection per slot, each on a socket
//! pair whose peer answers every query, so acquire and release run for real
//! without a server. Levels are tracked per tokio task and `block_on` (the
//! test body) has no task id, so each scenario runs in a spawned task.

use super::config::PoolConfig;
use super::connection::PooledConn;
use super::lifecycle::PgPool;
use super::tests::socket_pair_connection;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Answer every simple query with `CommandComplete` + `ReadyForQuery`; the
/// pool reset's marker query (`SELECT '<token>'`) also gets its token row.
fn answer_queries(mut peer: tokio::net::UnixStream) {
    tokio::spawn(async move {
        let mut head = [0u8; 5];
        while peer.read_exact(&mut head).await.is_ok() {
            let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let mut payload = vec![0u8; len.saturating_sub(4)];
            if peer.read_exact(&mut payload).await.is_err() {
                return;
            }
            if head[0] != b'Q' {
                continue;
            }
            let sql =
                std::str::from_utf8(&payload[..payload.len().saturating_sub(1)]).unwrap_or("");
            let marker = sql
                .strip_prefix("SELECT '")
                .and_then(|rest| rest.strip_suffix('\''));
            let tag: &[u8] = if marker.is_some() {
                b"SELECT 1\0"
            } else {
                b"ROLLBACK\0"
            };
            let mut reply = Vec::new();
            if let Some(token) = marker {
                reply.push(b'D');
                reply.extend_from_slice(&((4 + 2 + 4 + token.len()) as u32).to_be_bytes());
                reply.extend_from_slice(&1i16.to_be_bytes());
                reply.extend_from_slice(&(token.len() as i32).to_be_bytes());
                reply.extend_from_slice(token.as_bytes());
            }
            reply.push(b'C');
            reply.extend_from_slice(&(4 + tag.len() as u32).to_be_bytes());
            reply.extend_from_slice(tag);
            reply.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
            if peer.write_all(&reply).await.is_err() {
                return;
            }
        }
    });
}

/// A pool of `max` slots, `reserve` of them nested, with `max` idle
/// connections ready so no acquire dials out.
async fn pool(max: usize, reserve: &[usize], acquire_timeout: Duration) -> PgPool {
    let pool = PgPool::connect(
        PoolConfig::new_dev("localhost", 5432, "user", "db")
            .min_connections(0)
            .max_connections(max)
            .nested_reserve(reserve)
            .acquire_timeout(acquire_timeout),
    )
    .await
    .expect("pool init");
    let mut idle = pool.inner.connections.lock().await;
    for _ in 0..max {
        let (conn, peer) = socket_pair_connection();
        answer_queries(peer);
        idle.push(PooledConn {
            conn,
            created_at: Instant::now(),
            last_used: Instant::now(),
        });
    }
    drop(idle);
    pool
}

/// Free (shared, per-reserve) slots.
fn free(pool: &PgPool) -> (usize, Vec<usize>) {
    (
        pool.inner.semaphore.available_permits(),
        pool.inner
            .nested_semaphores
            .iter()
            .map(|reserve| reserve.available_permits())
            .collect(),
    )
}

fn registered_tasks(pool: &PgPool) -> usize {
    pool.inner.level_holders.lock().expect("holders").len()
}

/// Run `scenario` in a spawned task, where acquires have a task id.
async fn in_task<F>(scenario: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(scenario).await.expect("scenario task")
}

/// Two tasks each hold one connection, then both ask for a second.
async fn hold_one_then_ask_for_a_second(pool: &PgPool) -> Vec<Result<(), String>> {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let pool = pool.clone();
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            let first = pool.acquire_raw().await.map_err(|e| e.to_string())?;
            barrier.wait().await;
            let outcome = match pool.acquire_raw().await {
                Ok(second) => {
                    second.release().await;
                    Ok(())
                }
                Err(e) => Err(e.to_string()),
            };
            first.release().await;
            outcome
        }));
    }
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(task.await.expect("task"));
    }
    outcomes
}

#[tokio::test]
async fn test_holders_waiting_for_a_second_connection_deadlock_without_a_reserve() {
    let pool = pool(2, &[], Duration::from_millis(300)).await;
    let outcomes = hold_one_then_ask_for_a_second(&pool).await;
    for outcome in &outcomes {
        let err = outcome
            .as_ref()
            .expect_err("each second acquire waits on a slot the other task holds");
        assert!(err.contains("pool acquire after"), "{err}");
    }
}

#[tokio::test]
async fn test_a_reserve_lets_both_holders_get_their_second_connection() {
    let pool = pool(3, &[1], Duration::from_secs(2)).await;
    let outcomes = hold_one_then_ask_for_a_second(&pool).await;
    assert_eq!(outcomes, vec![Ok(()), Ok(())]);
    assert_eq!(
        free(&pool),
        (2, vec![1]),
        "every slot came back to its level"
    );
    assert_eq!(registered_tasks(&pool), 0, "no task is left registered");
}

#[tokio::test]
async fn test_a_nested_acquire_takes_the_reserve_and_leaves_the_shared_slots() {
    let pool = pool(3, &[1], Duration::from_secs(2)).await;
    let scenario = pool.clone();
    in_task(async move {
        let pool = scenario;
        let first = pool.acquire_raw().await.expect("first");
        assert_eq!(free(&pool), (1, vec![1]));
        let second = pool.acquire_raw().await.expect("second, nested");
        assert_eq!(
            free(&pool),
            (1, vec![0]),
            "the nested acquire took the reserve"
        );

        // Another task holds nothing, so it still gets a shared slot.
        let other = pool.clone();
        let shared = in_task(async move {
            let conn = other.acquire_raw().await.map_err(|e| e.to_string())?;
            conn.release().await;
            Ok::<(), String>(())
        })
        .await;
        assert_eq!(shared, Ok(()));

        second.release().await;
        first.release().await;
    })
    .await;
    assert_eq!(free(&pool), (2, vec![1]));
    assert_eq!(registered_tasks(&pool), 0);
}

#[tokio::test]
async fn test_join_in_one_task_claims_distinct_levels() {
    // One shared slot and one reserve: both acquires fit only if the second
    // claims level 1 before either waits.
    let pool = pool(2, &[1], Duration::from_millis(500)).await;
    let scenario = pool.clone();
    in_task(async move {
        let pool = scenario;
        let (a, b) = tokio::join!(pool.acquire_raw(), pool.acquire_raw());
        let (a, b) = (a.expect("first"), b.expect("second"));
        assert_eq!(free(&pool), (0, vec![0]));
        a.release().await;
        b.release().await;
    })
    .await;
    assert_eq!(free(&pool), (1, vec![1]));
    assert_eq!(registered_tasks(&pool), 0);
}

#[tokio::test]
async fn test_a_cancelled_nested_acquire_leaves_no_claim() {
    let pool = pool(3, &[1], Duration::from_secs(5)).await;
    let scenario = pool.clone();
    in_task(async move {
        let pool = scenario;
        let first = pool.acquire_raw().await.expect("first");
        let second = pool.acquire_raw().await.expect("second, nested");
        // Holding levels 0 and 1, the next acquire is past the reserves and
        // waits on the last one, which this task holds; the caller gives up.
        let third = tokio::time::timeout(Duration::from_millis(100), pool.acquire_raw()).await;
        assert!(third.is_err(), "the last reserve is held");
        {
            let registered = pool.inner.level_holders.lock().expect("holders");
            let counts = registered.values().next().expect("this task");
            assert_eq!(counts, &vec![1, 1], "the cancelled claim is gone");
        }
        second.release().await;
        first.release().await;
    })
    .await;
    assert_eq!(registered_tasks(&pool), 0);
    assert_eq!(free(&pool), (2, vec![1]));
}

#[tokio::test]
async fn test_acquires_past_the_reserves_share_the_last_one() {
    let pool = pool(3, &[1], Duration::from_millis(200)).await;
    let err = in_task(async move {
        let first = pool.acquire_raw().await.expect("first");
        let second = pool.acquire_raw().await.expect("second, nested");
        let err = pool
            .acquire_raw()
            .await
            .err()
            .expect("the last reserve is held by this task")
            .to_string();
        second.release().await;
        first.release().await;
        err
    })
    .await;
    assert!(
        err.contains("pool acquire after") && err.contains("nested level 1"),
        "{err}"
    );
}

#[tokio::test]
async fn test_nested_reserve_is_validated() {
    let zero = PgPool::connect(
        PoolConfig::new_dev("localhost", 5432, "user", "db")
            .min_connections(0)
            .max_connections(4)
            .nested_reserve(&[1, 0]),
    )
    .await;
    assert!(
        zero.is_err(),
        "a level with no slot never serves an acquire"
    );

    let all = PgPool::connect(
        PoolConfig::new_dev("localhost", 5432, "user", "db")
            .min_connections(0)
            .max_connections(3)
            .nested_reserve(&[2, 1]),
    )
    .await;
    assert!(all.is_err(), "a reserve must leave shared slots");
}

#[tokio::test]
async fn test_without_a_reserve_nothing_is_tracked() {
    let pool = pool(2, &[], Duration::from_millis(500)).await;
    let scenario = pool.clone();
    in_task(async move {
        let pool = scenario;
        let first = pool.acquire_raw().await.expect("first");
        let second = pool.acquire_raw().await.expect("second");
        assert_eq!(free(&pool), (0, vec![]));
        assert_eq!(registered_tasks(&pool), 0);
        second.release().await;
        first.release().await;
    })
    .await;
}
