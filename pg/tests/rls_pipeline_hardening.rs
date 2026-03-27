//! RLS pipeline hardening tests.
//!
//! Validates failure-path protocol resync and retry hygiene for
//! `PooledConnection::fetch_all_with_rls*`.

use qail_core::ast::Qail;
use qail_pg::protocol::PROTOCOL_VERSION_3_2;
use qail_pg::{PgPool, PoolConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout};

async fn mock_listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

async fn read_startup_message(sock: &mut TcpStream) {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    assert!(len >= 8, "StartupMessage must be at least 8 bytes");

    let mut rest = vec![0u8; len - 4];
    sock.read_exact(&mut rest).await.unwrap();
    let version = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
    assert_eq!(
        version, PROTOCOL_VERSION_3_2,
        "Expected default protocol 3.2 StartupMessage"
    );
}

async fn read_frontend_msg_types_until_sync(sock: &mut TcpStream) -> Vec<u8> {
    let mut types = Vec::new();
    loop {
        let mut head = [0u8; 5];
        sock.read_exact(&mut head).await.unwrap();
        let msg_type = head[0];
        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        assert!(len >= 4, "frontend frame length must be >= 4");
        let payload_len = len - 4;
        let mut payload = vec![0u8; payload_len];
        sock.read_exact(&mut payload).await.unwrap();
        types.push(msg_type);
        if msg_type == b'S' {
            break;
        }
    }
    types
}

async fn read_frontend_frame(sock: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut head = [0u8; 5];
    sock.read_exact(&mut head).await.unwrap();
    let msg_type = head[0];
    let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    assert!(len >= 4, "frontend frame length must be >= 4");
    let payload_len = len - 4;
    let mut payload = vec![0u8; payload_len];
    sock.read_exact(&mut payload).await.unwrap();
    (msg_type, payload)
}

fn payload_cstr(payload: &[u8]) -> String {
    let nul = payload
        .iter()
        .position(|b| *b == 0)
        .unwrap_or(payload.len());
    String::from_utf8_lossy(&payload[..nul]).into_owned()
}

fn backend_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + payload.len());
    out.push(msg_type);
    out.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn auth_ok() -> Vec<u8> {
    backend_frame(b'R', &0i32.to_be_bytes())
}

fn ready_status(status: u8) -> Vec<u8> {
    backend_frame(b'Z', &[status])
}

fn ready_idle() -> Vec<u8> {
    ready_status(b'I')
}

fn ready_in_block() -> Vec<u8> {
    ready_status(b'T')
}

fn ready_failed() -> Vec<u8> {
    ready_status(b'E')
}

fn bind_complete() -> Vec<u8> {
    backend_frame(b'2', &[])
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(tag.len() + 1);
    payload.extend_from_slice(tag.as_bytes());
    payload.push(0);
    backend_frame(b'C', &payload)
}

fn error_response(code: &str, message: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"ERROR");
    payload.push(0);
    payload.push(b'C');
    payload.extend_from_slice(code.as_bytes());
    payload.push(0);
    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);
    payload.push(0);
    backend_frame(b'E', &payload)
}

fn pool_config(port: u16) -> PoolConfig {
    PoolConfig::new_dev("127.0.0.1", port, "test_user", "test_db")
        .min_connections(0)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(2))
}

#[tokio::test]
async fn fetch_all_with_rls_drains_extended_responses_after_setup_error() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let first = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(first.first().copied(), Some(b'Q'));
        assert!(
            first.contains(&b'S'),
            "first RLS pipeline write must include extended Sync"
        );

        // Phase 1 (RLS setup) fails, but phase 2 responses are already in flight.
        sock.write_all(&error_response("22012", "division by zero"))
            .await
            .unwrap();
        sock.write_all(&ready_failed()).await.unwrap();
        // Extended phase responses that must be drained before next command.
        sock.write_all(&error_response(
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        ))
        .await
        .unwrap();
        sock.write_all(&ready_failed()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = timeout(Duration::from_secs(2), read_frontend_frame(&mut sock))
            .await
            .expect("timed out waiting for follow-up simple query");
        assert_eq!(msg_type, b'Q');
        assert_eq!(payload_cstr(&payload), "SELECT 1");

        sock.write_all(&command_complete("SELECT 1")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = timeout(Duration::from_secs(2), read_frontend_frame(&mut sock))
            .await
            .expect("timed out waiting for release COMMIT");
        assert_eq!(msg_type, b'Q');
        assert_eq!(payload_cstr(&payload), "COMMIT");

        sock.write_all(&command_complete("COMMIT")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let pool = PgPool::connect(pool_config(port)).await.unwrap();

    let mut conn = pool.acquire_raw().await.unwrap();
    let err = match conn
        .fetch_all_with_rls(&Qail::get("users").limit(1), "BEGIN; SELECT 1/0")
        .await
    {
        Ok(_) => panic!("expected setup error"),
        Err(err) => err,
    };
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("division by zero"),
        "unexpected error after setup failure: {err}"
    );

    conn.get_mut()
        .unwrap()
        .execute_simple("SELECT 1")
        .await
        .unwrap();
    conn.release().await;

    pool.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn fetch_all_with_rls_retry_already_exists_rolls_back_before_retry() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // First attempt (cache miss): expect Parse in this sequence.
        let first = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(first.first().copied(), Some(b'Q'));
        assert!(first.contains(&b'P'));

        // RLS setup phase succeeds, extended phase fails with 42P05.
        sock.write_all(&ready_in_block()).await.unwrap();
        sock.write_all(&error_response(
            "42P05",
            "prepared statement \"qail_deadbeef\" already exists",
        ))
        .await
        .unwrap();
        sock.write_all(&ready_failed()).await.unwrap();
        sock.flush().await.unwrap();

        // Retry path must rollback before retrying the RLS pipeline.
        let (msg_type, payload) = timeout(Duration::from_secs(2), read_frontend_frame(&mut sock))
            .await
            .expect("timed out waiting for retry rollback");
        assert_eq!(msg_type, b'Q');
        assert_eq!(payload_cstr(&payload), "ROLLBACK");

        sock.write_all(&command_complete("ROLLBACK")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // Second attempt should be a cache hit (no Parse) and succeed.
        let second = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(second.first().copied(), Some(b'Q'));
        assert!(
            !second.contains(&b'P'),
            "retry after 42P05 should avoid re-Parse and reuse statement mapping"
        );

        sock.write_all(&ready_in_block()).await.unwrap();
        sock.write_all(&bind_complete()).await.unwrap();
        sock.write_all(&command_complete("SELECT 0")).await.unwrap();
        sock.write_all(&ready_in_block()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = timeout(Duration::from_secs(2), read_frontend_frame(&mut sock))
            .await
            .expect("timed out waiting for release COMMIT");
        assert_eq!(msg_type, b'Q');
        assert_eq!(payload_cstr(&payload), "COMMIT");

        sock.write_all(&command_complete("COMMIT")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let pool = PgPool::connect(pool_config(port)).await.unwrap();

    let mut conn = pool.acquire_raw().await.unwrap();
    let rows = conn
        .fetch_all_with_rls(
            &Qail::get("users").limit(1),
            "BEGIN; SELECT set_config('app.current_tenant_id','tenant-a',true)",
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
    conn.release().await;

    pool.close().await;
    server.await.unwrap();
}
