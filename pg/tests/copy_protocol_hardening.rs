//! COPY protocol hardening tests.
//!
//! Adversarial mock-server scenarios to ensure COPY paths fail closed on
//! unexpected backend messages.

use qail_core::ast::Qail;
use qail_pg::PgConnection;
use qail_pg::protocol::PROTOCOL_VERSION_3_2;
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

fn backend_key_data() -> Vec<u8> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&1234i32.to_be_bytes());
    payload.extend_from_slice(&5678i32.to_be_bytes());
    backend_frame(b'K', &payload)
}

fn copy_in_response_text_zero_cols() -> Vec<u8> {
    // format=0 (text), num_columns=0
    backend_frame(b'G', &[0, 0, 0])
}

fn copy_out_response_text_zero_cols() -> Vec<u8> {
    // format=0 (text), num_columns=0
    backend_frame(b'H', &[0, 0, 0])
}

fn copy_done() -> Vec<u8> {
    backend_frame(b'c', &[])
}

#[tokio::test]
async fn copy_in_raw_rejects_unexpected_startup_message() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(
            msg_type, b'Q',
            "COPY command should use simple query protocol"
        );
        assert!(
            payload.starts_with(b"COPY"),
            "expected COPY SQL payload, got {:?}",
            String::from_utf8_lossy(&payload)
        );

        // Unexpected in copy-in startup loop.
        sock.write_all(&backend_key_data()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = conn
        .copy_in_raw("users", &[String::from("id")], b"1\n")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("copy-in raw startup"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn copy_export_rejects_unexpected_stream_message() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(
            msg_type, b'Q',
            "COPY command should use simple query protocol"
        );
        assert!(
            payload.starts_with(b"COPY"),
            "expected COPY SQL payload, got {:?}",
            String::from_utf8_lossy(&payload)
        );

        // Valid startup response for COPY OUT.
        sock.write_all(&copy_out_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Unexpected in copy-out stream loop.
        sock.write_all(&backend_key_data()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let cmd = Qail::export("users").columns(["id"]);
    let err = conn.copy_export(&cmd).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("copy-out raw stream"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn copy_in_raw_rejects_ready_without_commandcomplete() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');

        sock.write_all(&copy_in_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        let (copy_data_type, _copy_data_payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(copy_data_type, b'd', "expected CopyData");
        let (copy_done_type, _copy_done_payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(copy_done_type, b'c', "expected CopyDone");

        // Missing CommandComplete: should fail closed.
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = conn
        .copy_in_raw("users", &[String::from("id")], b"1\n")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("missing CommandComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn copy_export_rejects_ready_without_copydone() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');

        sock.write_all(&copy_out_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Missing CopyDone + CommandComplete.
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let cmd = Qail::export("users").columns(["id"]);
    let err = conn.copy_export(&cmd).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("missing CopyDone"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn copy_export_rejects_ready_without_commandcomplete() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');

        sock.write_all(&copy_out_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        sock.write_all(&copy_done()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let cmd = Qail::export("users").columns(["id"]);
    let err = conn.copy_export(&cmd).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("missing CommandComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}
