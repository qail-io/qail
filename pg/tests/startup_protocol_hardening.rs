//! Startup protocol hardening tests.
//!
//! Validates fail-closed behavior for malformed or out-of-order backend
//! messages during connection startup/authentication.

use std::time::Duration;

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

fn backend_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + payload.len());
    out.push(msg_type);
    let len = (payload.len() + 4) as u32;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn auth_message(auth_code: i32, extra: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + extra.len());
    payload.extend_from_slice(&auth_code.to_be_bytes());
    payload.extend_from_slice(extra);
    backend_frame(b'R', &payload)
}

fn auth_ok() -> Vec<u8> {
    auth_message(0, &[])
}

fn auth_cleartext() -> Vec<u8> {
    auth_message(3, &[])
}

fn auth_md5() -> Vec<u8> {
    auth_message(5, &[1, 2, 3, 4])
}

fn auth_sasl_scram_sha256() -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&10i32.to_be_bytes());
    payload.extend_from_slice(b"SCRAM-SHA-256\0");
    payload.push(0); // final mechanism list terminator
    backend_frame(b'R', &payload)
}

fn parameter_status(name: &str, value: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(name.as_bytes());
    payload.push(0);
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
    backend_frame(b'S', &payload)
}

fn backend_key_data(process_id: i32, secret_key: i32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&process_id.to_be_bytes());
    payload.extend_from_slice(&secret_key.to_be_bytes());
    backend_frame(b'K', &payload)
}

fn ready_for_query(status: u8) -> Vec<u8> {
    backend_frame(b'Z', &[status])
}

async fn run_startup_script(script: Vec<Vec<u8>>, password: Option<&str>) -> String {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        for msg in script {
            if sock.write_all(&msg).await.is_err() {
                return;
            }
        }
        let _ = sock.flush().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    });

    let result =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", password)
            .await;

    server.await.unwrap();
    match result {
        Ok(_) => panic!("connection must fail"),
        Err(err) => format!("{err}"),
    }
}

#[tokio::test]
async fn startup_rejects_parameter_status_before_auth_ok() {
    let msg = run_startup_script(vec![parameter_status("server_version", "16.0")], None).await;
    assert!(
        msg.contains("Received ParameterStatus before AuthenticationOk"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn startup_rejects_backend_key_before_auth_ok() {
    let msg = run_startup_script(vec![backend_key_data(1234, 5678)], None).await;
    assert!(
        msg.contains("Received BackendKeyData before AuthenticationOk"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn startup_rejects_ready_before_auth_ok() {
    let msg = run_startup_script(vec![ready_for_query(b'I')], None).await;
    assert!(
        msg.contains("Startup completed without AuthenticationOk"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn startup_rejects_auth_method_switch_mid_handshake() {
    let msg = run_startup_script(vec![auth_cleartext(), auth_md5()], Some("secret")).await;
    assert!(
        msg.contains("Received AuthenticationMD5Password while cleartext-password authentication is in progress"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn startup_rejects_auth_ok_before_sasl_final() {
    let msg = run_startup_script(vec![auth_sasl_scram_sha256(), auth_ok()], Some("secret")).await;
    assert!(
        msg.contains("Received AuthenticationOk before AuthenticationSASLFinal"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn startup_rejects_auth_challenge_after_auth_ok() {
    let msg = run_startup_script(vec![auth_ok(), auth_cleartext()], None).await;
    assert!(
        msg.contains("Received authentication challenge after AuthenticationOk"),
        "unexpected error: {msg}"
    );
}
