//! Pool hot-statement registry hardening tests.
//!
//! A statement should only become globally "hot" after PostgreSQL confirms
//! that it parsed successfully. Otherwise one bad parse can poison future
//! checkouts and force needless connection churn.

use qail_core::ast::Qail;
use qail_pg::protocol::PROTOCOL_VERSION_3_2;
use qail_pg::{PgPool, PoolConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
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

async fn read_frontend_frame(sock: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut head = [0u8; 5];
    sock.read_exact(&mut head).await.unwrap();
    let msg_type = head[0];
    let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    assert!(len >= 4, "frontend frame length must be >= 4");
    let mut payload = vec![0u8; len - 4];
    sock.read_exact(&mut payload).await.unwrap();
    (msg_type, payload)
}

async fn read_frontend_msg_types_until_sync(sock: &mut TcpStream) -> Vec<u8> {
    let mut types = Vec::new();
    loop {
        let (msg_type, _) = read_frontend_frame(sock).await;
        types.push(msg_type);
        if msg_type == b'S' {
            break;
        }
    }
    types
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

fn ready_idle() -> Vec<u8> {
    backend_frame(b'Z', b"I")
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
async fn parse_failed_cache_miss_does_not_poison_pool_hot_registry() {
    let (listener, port) = mock_listener().await;
    let (checked_tx, checked_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let first = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(first.first().copied(), Some(b'P'));

        sock.write_all(&error_response("42P01", "relation does not exist"))
            .await
            .unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        assert_eq!(payload_cstr(&payload), "COMMIT");
        sock.write_all(&command_complete("COMMIT")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        match timeout(Duration::from_millis(200), read_frontend_frame(&mut sock)).await {
            Ok((unexpected, _)) => {
                panic!(
                    "pool checkout attempted hot pre-prepare after parse failure; first frame was {}",
                    unexpected as char
                );
            }
            Err(_) => {
                checked_tx.send(()).unwrap();
            }
        }

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        assert_eq!(payload_cstr(&payload), "COMMIT");
        sock.write_all(&command_complete("COMMIT")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let pool = PgPool::connect(pool_config(port)).await.unwrap();
    let mut first_conn = pool.acquire_raw().await.unwrap();
    let err = match first_conn
        .fetch_all_cached(&Qail::get("missing_table"))
        .await
    {
        Ok(_) => panic!("parse failure should be surfaced"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("relation does not exist"),
        "unexpected parse error: {err}"
    );
    first_conn.release().await;

    let second_conn = pool.acquire_raw().await.unwrap();
    checked_rx.await.unwrap();
    second_conn.release().await;

    pool.close().await;
    server.await.unwrap();
}
