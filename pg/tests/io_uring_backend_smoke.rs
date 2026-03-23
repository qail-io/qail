#![cfg(all(target_os = "linux", feature = "io_uring"))]

//! Linux io_uring transport smoke test.
//!
//! Ensures plain TCP connections select the native io_uring path when
//! explicitly requested by environment policy.

use qail_pg::protocol::PROTOCOL_VERSION_3_2;
use qail_pg::{PgConnection, driver::io_backend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

fn backend_key_data(process_id: i32, secret_key: i32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&process_id.to_be_bytes());
    payload.extend_from_slice(&secret_key.to_be_bytes());
    backend_frame(b'K', &payload)
}

fn ready_idle() -> Vec<u8> {
    backend_frame(b'Z', b"I")
}

#[tokio::test]
async fn forced_io_uring_uses_native_transport_backend() {
    let requested_backend = std::env::var("QAIL_PG_IO_BACKEND")
        .unwrap_or_default()
        .to_ascii_lowercase();
    if requested_backend != "io_uring" {
        eprintln!("skipping: QAIL_PG_IO_BACKEND is not set to io_uring");
        return;
    }

    assert!(
        io_backend::is_uring_available(),
        "QAIL_PG_IO_BACKEND=io_uring but kernel io_uring support is unavailable"
    );

    let (listener, port) = mock_listener().await;
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&backend_key_data(1234, 5678)).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let conn = PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
        .await
        .expect("connect should succeed against mock startup server");

    assert_eq!(
        conn.transport_backend(),
        "io_uring",
        "connection did not select io_uring backend under forced policy"
    );

    server.await.unwrap();
}
