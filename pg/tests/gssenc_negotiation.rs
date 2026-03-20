//! GSSENC Negotiation Tests
//!
//! Mock-server tests for the GSSENCRequest → G/N/E negotiation preface.
//! Each test spins up a minimal TCP listener that mimics a PostgreSQL
//! server's response to a GSSENCRequest (80877104).

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use qail_pg::protocol::PROTOCOL_VERSION_3_2;
use qail_pg::{ConnectOptions, GssEncMode, PgConnection, TlsMode};

/// Helper: bind a random-port listener and return (listener, port).
async fn mock_listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

/// The 8-byte GSSENCRequest the client must send.
const GSSENC_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 48];

// ══════════════════════════════════════════════════════════════════════
// GSSENCRequest: Server responds 'G'
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gssenc_negotiate_server_accepts_g() {
    let (listener, port) = mock_listener().await;

    // Mock server: read 8-byte GSSENCRequest, respond 'G', then close.
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, GSSENC_REQUEST, "Client must send GSSENCRequest");
        sock.write_all(b"G").await.unwrap();
        sock.flush().await.unwrap();
        // Keep connection open briefly so client can detect 'G'
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    });

    let opts = ConnectOptions {
        gss_enc_mode: GssEncMode::Prefer,
        tls_mode: TlsMode::Disable,
        ..Default::default()
    };

    // With prefer, server accepts G → we get the Phase 4 placeholder error
    let result =
        PgConnection::connect_with_options("127.0.0.1", port, "test", "testdb", None, opts).await;

    assert!(result.is_err());
    let err = result.err().unwrap();
    let msg = format!("{}", err);
    // On macOS (no enterprise-gssapi feature), the driver reports
    // that GSSAPI encryption requires the feature on Linux.
    // On Linux with the feature, the handshake would proceed.
    assert!(
        msg.contains("enterprise-gssapi") || msg.contains("GSSENC"),
        "Expected GSSENC feature-gate or handshake error, got: {msg}"
    );

    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// GSSENCRequest: Server responds 'N' — prefer falls through to plain
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gssenc_negotiate_server_rejects_n_prefer_fallback() {
    let (listener, port) = mock_listener().await;

    // Mock server: GSSENCRequest → 'N', then expect plain StartupMessage.
    // We won't handle the full PG handshake, just verify the client
    // falls through to the plain path by checking it sends a StartupMessage.
    let server = tokio::spawn(async move {
        // First connection: GSSENCRequest → 'N'
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, GSSENC_REQUEST);
        sock.write_all(b"N").await.unwrap();
        sock.flush().await.unwrap();
        drop(sock);

        // Second connection: client retries with plain StartupMessage
        let (mut sock2, _) = listener.accept().await.unwrap();
        let mut hdr = [0u8; 4];
        sock2.read_exact(&mut hdr).await.unwrap();
        let len = u32::from_be_bytes(hdr) as usize;
        // StartupMessage length should be > 8 (includes version + params)
        assert!(len > 8, "Expected StartupMessage, got length {len}");
        // Read protocol version (next 4 bytes)
        let mut ver = [0u8; 4];
        sock2.read_exact(&mut ver).await.unwrap();
        let version = i32::from_be_bytes(ver);
        assert_eq!(
            version, PROTOCOL_VERSION_3_2,
            "Expected default protocol 3.2 StartupMessage"
        );
        // Close without completing handshake — client will get connection error
        drop(sock2);
    });

    let opts = ConnectOptions {
        gss_enc_mode: GssEncMode::Prefer,
        tls_mode: TlsMode::Disable,
        ..Default::default()
    };

    // The connection will fail because the mock doesn't complete the
    // PG handshake, but we verify the fallback path was taken.
    let result =
        PgConnection::connect_with_options("127.0.0.1", port, "test", "testdb", None, opts).await;

    // Connection fails because mock closes, but that's expected —
    // the important thing is the server saw a StartupMessage.
    assert!(result.is_err());
    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// GSSENCRequest: Server responds 'N' — require must fail
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gssenc_negotiate_server_rejects_n_require_fails() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        sock.write_all(b"N").await.unwrap();
        sock.flush().await.unwrap();
    });

    let opts = ConnectOptions {
        gss_enc_mode: GssEncMode::Require,
        tls_mode: TlsMode::Disable,
        ..Default::default()
    };

    let result =
        PgConnection::connect_with_options("127.0.0.1", port, "test", "testdb", None, opts).await;

    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("gssencmode=require but server rejected"),
        "Expected require rejection error, got: {msg}"
    );

    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// GSSENCRequest: Server responds 'E' (ErrorMessage)
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gssenc_negotiate_server_error_response() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        // Respond with 'E' (ErrorMessage indicator)
        sock.write_all(b"E").await.unwrap();
        sock.flush().await.unwrap();
    });

    let opts = ConnectOptions {
        gss_enc_mode: GssEncMode::Prefer,
        tls_mode: TlsMode::Disable,
        ..Default::default()
    };

    // With prefer + ServerError, should fall through to plain.
    // The plain path will fail because the mock is gone, but the
    // GSSENC negotiation should not error out.
    let result =
        PgConnection::connect_with_options("127.0.0.1", port, "test", "testdb", None, opts).await;

    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    // Should NOT contain gssenc error; should be a connection error from the plain path
    assert!(
        !msg.contains("gssencmode=require"),
        "Prefer+ServerError should fall through, got: {msg}"
    );

    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// GSSENCRequest: Server responds 'E' — require must fail
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gssenc_negotiate_server_error_require_fails() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        sock.write_all(b"E").await.unwrap();
        sock.flush().await.unwrap();
    });

    let opts = ConnectOptions {
        gss_enc_mode: GssEncMode::Require,
        tls_mode: TlsMode::Disable,
        ..Default::default()
    };

    let result =
        PgConnection::connect_with_options("127.0.0.1", port, "test", "testdb", None, opts).await;

    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("gssencmode=require but server rejected"),
        "Expected require rejection, got: {msg}"
    );

    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// GSSENCRequest: Malformed response (unexpected byte)
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gssenc_negotiate_malformed_response() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        // Send a byte that is neither G, N, nor E
        sock.write_all(b"X").await.unwrap();
        sock.flush().await.unwrap();
    });

    let opts = ConnectOptions {
        gss_enc_mode: GssEncMode::Require,
        tls_mode: TlsMode::Disable,
        ..Default::default()
    };

    let result =
        PgConnection::connect_with_options("127.0.0.1", port, "test", "testdb", None, opts).await;

    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("Unexpected response to GSSENCRequest: 0x58"),
        "Expected malformed response error, got: {msg}"
    );

    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// CVE-2021-23222: Buffer-stuffing attack — extra bytes after 'G'
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gssenc_negotiate_buffer_stuffing_attack() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        sock.read_exact(&mut buf).await.unwrap();
        // Send 'G' immediately followed by extra bytes (attack payload)
        sock.write_all(b"Gextra_attack_payload").await.unwrap();
        sock.flush().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    });

    let opts = ConnectOptions {
        gss_enc_mode: GssEncMode::Require,
        tls_mode: TlsMode::Disable,
        ..Default::default()
    };

    let result =
        PgConnection::connect_with_options("127.0.0.1", port, "test", "testdb", None, opts).await;

    assert!(result.is_err());
    let msg = format!("{}", result.err().unwrap());
    assert!(
        msg.contains("buffer-stuffing") || msg.contains("CVE-2021-23222"),
        "Expected buffer-stuffing detection, got: {msg}"
    );

    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// gssencmode=disable should skip GSSENCRequest entirely
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gssenc_disable_skips_negotiation() {
    let (listener, port) = mock_listener().await;

    // Mock server: expect a plain StartupMessage (no GSSENCRequest).
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut hdr = [0u8; 4];
        sock.read_exact(&mut hdr).await.unwrap();
        let len = u32::from_be_bytes(hdr) as usize;
        let mut ver = [0u8; 4];
        sock.read_exact(&mut ver).await.unwrap();
        let version = i32::from_be_bytes(ver);
        // Should be StartupMessage (not GSSENCRequest), using the default requested protocol version.
        assert_eq!(
            version, PROTOCOL_VERSION_3_2,
            "Expected default protocol 3.2 StartupMessage, not GSSENCRequest"
        );
        assert!(len > 8, "StartupMessage should be longer than 8 bytes");
        drop(sock);
    });

    let opts = ConnectOptions {
        gss_enc_mode: GssEncMode::Disable,
        tls_mode: TlsMode::Disable,
        ..Default::default()
    };

    let result =
        PgConnection::connect_with_options("127.0.0.1", port, "test", "testdb", None, opts).await;

    // Will fail because mock doesn't complete handshake, but server
    // assertion confirms no GSSENCRequest was sent.
    assert!(result.is_err());
    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// Server closes connection immediately after GSSENCRequest
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gssenc_negotiate_server_closes_immediately() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        // Read GSSENCRequest then immediately close
        let _ = sock.read_exact(&mut buf).await;
        drop(sock);
    });

    let opts = ConnectOptions {
        gss_enc_mode: GssEncMode::Prefer,
        tls_mode: TlsMode::Disable,
        ..Default::default()
    };

    // With prefer, connection error should fall through.
    // Since no second server is listening, the plain path also fails.
    let result =
        PgConnection::connect_with_options("127.0.0.1", port, "test", "testdb", None, opts).await;

    assert!(result.is_err());
    server.await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════
// Server closes immediately — require must fail
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gssenc_negotiate_server_closes_require_fails() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8];
        let _ = sock.read_exact(&mut buf).await;
        drop(sock);
    });

    let opts = ConnectOptions {
        gss_enc_mode: GssEncMode::Require,
        tls_mode: TlsMode::Disable,
        ..Default::default()
    };

    let result =
        PgConnection::connect_with_options("127.0.0.1", port, "test", "testdb", None, opts).await;

    assert!(result.is_err());
    // With require, any network error is fatal
    let msg = format!("{}", result.err().unwrap());
    // Could be EOF or connection reset — either way it's an IO error
    assert!(
        !msg.contains("not yet implemented"),
        "Should not reach Phase 4 placeholder on connection close"
    );

    server.await.unwrap();
}
