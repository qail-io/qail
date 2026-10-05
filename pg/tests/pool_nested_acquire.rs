//! Tasks that hold one pool connection while waiting for another.
//!
//! Without a reserve, enough such tasks hold every slot between them and
//! all wait until the acquire timeout. `PoolConfig::nested_reserve` serves
//! the second acquire from slots no first acquire can take.

use std::sync::Arc;
use std::time::Duration;

use qail_pg::protocol::PROTOCOL_VERSION_3_2;
use qail_pg::{PgPool, PoolConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn backend_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + payload.len());
    out.push(msg_type);
    out.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

async fn serve(mut sock: TcpStream) {
    let mut len_buf = [0u8; 4];
    if sock.read_exact(&mut len_buf).await.is_err() {
        return;
    }
    let mut rest = vec![0u8; u32::from_be_bytes(len_buf) as usize - 4];
    if sock.read_exact(&mut rest).await.is_err() {
        return;
    }
    let version = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
    assert_eq!(version, PROTOCOL_VERSION_3_2, "a StartupMessage first");
    let mut key = Vec::new();
    key.extend_from_slice(&1i32.to_be_bytes());
    key.extend_from_slice(&2i32.to_be_bytes());
    let mut reply = backend_frame(b'R', &0i32.to_be_bytes());
    reply.extend(backend_frame(b'K', &key));
    reply.extend(backend_frame(b'Z', b"I"));
    if sock.write_all(&reply).await.is_err() {
        return;
    }
    // Answer every simple query (the release reset) as a clean ROLLBACK, and
    // the reset's marker query (`SELECT '<token>'`) with its token row.
    let mut head = [0u8; 5];
    while sock.read_exact(&mut head).await.is_ok() {
        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let mut payload = vec![0u8; len.saturating_sub(4)];
        if sock.read_exact(&mut payload).await.is_err() {
            return;
        }
        if head[0] == b'Q' {
            let sql =
                std::str::from_utf8(&payload[..payload.len().saturating_sub(1)]).unwrap_or("");
            let marker = sql
                .strip_prefix("SELECT '")
                .and_then(|rest| rest.strip_suffix('\''));
            let mut answer = Vec::new();
            if let Some(token) = marker {
                let mut row = Vec::new();
                row.extend_from_slice(&1i16.to_be_bytes());
                row.extend_from_slice(&(token.len() as i32).to_be_bytes());
                row.extend_from_slice(token.as_bytes());
                answer.extend(backend_frame(b'D', &row));
                answer.extend(backend_frame(b'C', b"SELECT 1\0"));
            } else {
                answer.extend(backend_frame(b'C', b"ROLLBACK\0"));
            }
            answer.extend(backend_frame(b'Z', b"I"));
            if sock.write_all(&answer).await.is_err() {
                return;
            }
        }
    }
}

async fn fake_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            tokio::spawn(serve(sock));
        }
    });
    port
}

/// `holders` tasks each take one connection, wait for all of them to hold
/// one, then each asks for a second.
async fn hold_one_then_ask_for_a_second(pool: &PgPool, holders: usize) -> Vec<Result<(), String>> {
    let barrier = Arc::new(tokio::sync::Barrier::new(holders));
    let mut tasks = Vec::new();
    for _ in 0..holders {
        let pool = pool.clone();
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            let first = pool.acquire_raw().await.map_err(|e| e.to_string())?;
            barrier.wait().await;
            let outcome = match pool.acquire_raw().await {
                Ok(second) => second.release_checked().await.map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            first.release_checked().await.map_err(|e| e.to_string())?;
            outcome
        }));
    }
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(task.await.expect("task"));
    }
    outcomes
}

fn config(port: u16, max: usize) -> PoolConfig {
    PoolConfig::new_dev("127.0.0.1", port, "test_user", "test_db")
        .min_connections(0)
        .max_connections(max)
        .acquire_timeout(Duration::from_millis(500))
}

#[tokio::test]
async fn without_a_reserve_holders_waiting_for_a_second_connection_time_out() {
    let port = fake_server().await;
    let pool = PgPool::connect(config(port, 4)).await.expect("pool");
    let outcomes = hold_one_then_ask_for_a_second(&pool, 4).await;
    for outcome in &outcomes {
        let err = outcome
            .as_ref()
            .expect_err("every slot is held by a task waiting for another");
        assert!(err.contains("pool acquire after"), "{err}");
    }
}

#[tokio::test]
async fn a_nested_reserve_serves_the_second_connection() {
    let port = fake_server().await;
    // The same four shared slots, plus one reserved for nested acquires.
    let pool = PgPool::connect(config(port, 5).nested_reserve(&[1]))
        .await
        .expect("pool");
    let outcomes = hold_one_then_ask_for_a_second(&pool, 4).await;
    assert_eq!(outcomes, vec![Ok(()); 4]);
    let stats = pool.stats().await;
    assert_eq!(stats.active, 0, "every checkout ended");
}
