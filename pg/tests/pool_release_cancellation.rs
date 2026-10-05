//! A release cancelled mid-flight gives its pool slot back.
//!
//! Callers drop pool futures all the time: an HTTP client disconnects, an
//! outer `tokio::time::timeout` fires. A slot lost there stays lost for the
//! life of the pool, and once every slot is gone each acquire fails with
//! "pool acquire after Ns (N max connections)".

use std::time::Duration;

use qail_pg::protocol::PROTOCOL_VERSION_3_2;
use qail_pg::{PgPool, PoolConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

fn backend_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + payload.len());
    out.push(msg_type);
    out.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

async fn complete_startup(sock: &mut TcpStream) {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let mut rest = vec![0u8; u32::from_be_bytes(len_buf) as usize - 4];
    sock.read_exact(&mut rest).await.unwrap();
    let version = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
    assert_eq!(version, PROTOCOL_VERSION_3_2, "a StartupMessage first");

    let mut key = Vec::new();
    key.extend_from_slice(&1i32.to_be_bytes());
    key.extend_from_slice(&2i32.to_be_bytes());
    let mut reply = backend_frame(b'R', &0i32.to_be_bytes());
    reply.extend(backend_frame(b'K', &key));
    reply.extend(backend_frame(b'Z', b"I"));
    sock.write_all(&reply).await.unwrap();
}

/// One frontend message type, or `None` once the client hung up.
async fn read_frontend_message(sock: &mut TcpStream) -> Option<u8> {
    let mut head = [0u8; 5];
    sock.read_exact(&mut head).await.ok()?;
    let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    let mut payload = vec![0u8; len.saturating_sub(4)];
    sock.read_exact(&mut payload).await.ok()?;
    Some(head[0])
}

/// A server that completes every startup, then reads each query and never
/// answers it. Each simple query it reads is reported on the channel; the
/// Terminate a destroyed connection sends is not.
async fn unanswering_server() -> (u16, mpsc::UnboundedReceiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (seen_tx, seen_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let seen = seen_tx.clone();
            tokio::spawn(async move {
                complete_startup(&mut sock).await;
                while let Some(msg_type) = read_frontend_message(&mut sock).await {
                    if msg_type == b'Q' {
                        let _ = seen.send(());
                    }
                }
            });
        }
    });
    (port, seen_rx)
}

#[tokio::test]
async fn cancelled_releases_give_their_slots_back() {
    let (port, mut seen) = unanswering_server().await;
    let pool = PgPool::connect(
        PoolConfig::new_dev("127.0.0.1", port, "test_user", "test_db")
            .min_connections(0)
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1)),
    )
    .await
    .expect("pool");

    // More cancelled releases than the pool has slots.
    for round in 1..=3 {
        let conn = pool.acquire_raw().await.unwrap_or_else(|e| {
            panic!(
                "checkout {round} after {} cancelled release(s): {e}",
                round - 1
            )
        });
        let mut release = Box::pin(conn.release_checked());
        tokio::select! {
            _ = &mut release => panic!("the server never answers the reset"),
            reset = seen.recv() => assert!(reset.is_some(), "the server read the reset"),
        }
        // The caller goes away while the reset is in flight.
        drop(release);
    }

    let stats = pool.stats().await;
    assert_eq!(stats.active, 0, "every checkout ended");
    assert_eq!(
        stats.idle, 0,
        "a connection whose reset was interrupted is never pooled"
    );
}
