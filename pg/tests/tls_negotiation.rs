//! TLS SSLRequest Negotiation Tests
//!
//! Mock-server tests for the SSLRequest → S/N preface: `Require` fails
//! closed when the server rejects TLS, `Prefer` falls back to plaintext
//! ONLY on the SSLRequest-rejected sentinel, and a failed TLS handshake
//! after `'S'` propagates without a plaintext retry.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use qail_pg::protocol::PROTOCOL_VERSION_3_2;
use qail_pg::{ConnectOptions, PgConnection, TlsMode};

/// The 8-byte SSLRequest the client must send (code 80877103).
const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

async fn mock_listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

fn options(tls_mode: TlsMode) -> ConnectOptions {
    ConnectOptions {
        tls_mode,
        ..Default::default()
    }
}

/// Assert the listener receives no further connection: a fallback that must
/// not happen would show up here as a second accept.
async fn assert_no_second_connection(listener: &TcpListener) {
    let second = tokio::time::timeout(Duration::from_millis(300), listener.accept()).await;
    assert!(
        second.is_err(),
        "no plaintext fallback connection may be opened"
    );
}

// ══════════════════════════════════════════════════════════════════════
// TlsMode::Require: server rejects TLS → fail closed, no plaintext retry
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn tls_require_fails_closed_on_server_n() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, SSL_REQUEST, "Client must send SSLRequest");
        sock.write_all(b"N").await.unwrap();
        sock.flush().await.unwrap();

        // The client must not answer 'N' with a plaintext StartupMessage
        // on this socket either.
        let mut trailing = [0u8; 1];
        let read =
            tokio::time::timeout(Duration::from_millis(300), sock.read(&mut trailing)).await;
        match read {
            // Clean close (FIN), reset (client dropped the socket), or
            // silence — all mean no plaintext retry on this socket.
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => {}
            Ok(Ok(n)) => panic!("client wrote {n} byte(s) after 'N' under Require"),
        }

        assert_no_second_connection(&listener).await;
    });

    let result = PgConnection::connect_with_options(
        "127.0.0.1",
        port,
        "test",
        "testdb",
        None,
        options(TlsMode::Require),
    )
    .await;

    assert!(result.is_err(), "Require must fail when the server rejects TLS");
    let err = result.err().unwrap();
    assert!(
        err.to_string().contains("Server does not support TLS"),
        "unexpected error: {err}"
    );

    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// TlsMode::Prefer: server rejects TLS → plaintext retry on a fresh socket
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn tls_prefer_falls_back_to_plaintext_on_server_n() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        // First connection: SSLRequest → 'N'
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, SSL_REQUEST);
        sock.write_all(b"N").await.unwrap();
        sock.flush().await.unwrap();
        drop(sock);

        // Second connection: plaintext StartupMessage
        let (mut sock2, _) = listener.accept().await.unwrap();
        let mut hdr = [0u8; 4];
        sock2.read_exact(&mut hdr).await.unwrap();
        let len = u32::from_be_bytes(hdr) as usize;
        assert!(len > 8, "Expected StartupMessage, got length {len}");
        let mut ver = [0u8; 4];
        sock2.read_exact(&mut ver).await.unwrap();
        let version = i32::from_be_bytes(ver);
        assert_eq!(
            version, PROTOCOL_VERSION_3_2,
            "Expected default protocol 3.2 StartupMessage"
        );
        // Close without completing the handshake — client errors, but the
        // fallback path has been proven.
        drop(sock2);
    });

    let result = PgConnection::connect_with_options(
        "127.0.0.1",
        port,
        "test",
        "testdb",
        None,
        options(TlsMode::Prefer),
    )
    .await;

    // The mock never completes the PG handshake; the assertion that matters
    // is server-side (a plaintext StartupMessage arrived on socket two).
    assert!(result.is_err());
    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// TlsMode::Prefer: TLS handshake failure after 'S' → NO plaintext retry
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn tls_prefer_does_not_fall_back_when_handshake_fails() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, SSL_REQUEST);
        // Accept TLS, then feed garbage instead of a ServerHello.
        sock.write_all(b"S").await.unwrap();
        sock.write_all(b"garbage bytes, not a tls handshake....")
            .await
            .unwrap();
        sock.flush().await.unwrap();
        drop(sock);

        assert_no_second_connection(&listener).await;
    });

    let result = PgConnection::connect_with_options(
        "127.0.0.1",
        port,
        "test",
        "testdb",
        None,
        options(TlsMode::Prefer),
    )
    .await;

    assert!(
        result.is_err(),
        "failed TLS handshake must propagate under Prefer"
    );
    let err = result.err().unwrap();
    let msg = err.to_string();
    assert!(
        msg.contains("TLS handshake failed"),
        "handshake failure must not be rewritten into the fallback sentinel: {msg}"
    );

    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// TlsMode::Require: junk preface byte → same fail-closed path as 'N'
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn tls_require_fails_closed_on_junk_preface_byte() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, SSL_REQUEST);
        sock.write_all(b"X").await.unwrap();
        sock.flush().await.unwrap();

        assert_no_second_connection(&listener).await;
    });

    let result = PgConnection::connect_with_options(
        "127.0.0.1",
        port,
        "test",
        "testdb",
        None,
        options(TlsMode::Require),
    )
    .await;

    assert!(result.is_err(), "Require must fail on a junk preface byte");
    let err = result.err().unwrap();
    assert!(
        err.to_string().contains("Server does not support TLS"),
        "unexpected error: {err}"
    );

    server.await.unwrap();
}
