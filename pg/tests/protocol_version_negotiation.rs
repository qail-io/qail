//! Protocol 3.2 startup negotiation and downgrade behavior tests.

use qail_pg::PgConnection;
use qail_pg::protocol::{PROTOCOL_VERSION_3_0, PROTOCOL_VERSION_3_2};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout};

async fn mock_listener() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

async fn read_startup_version(sock: &mut TcpStream) -> i32 {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    assert!(len >= 8, "StartupMessage must be at least 8 bytes");

    let mut rest = vec![0u8; len - 4];
    sock.read_exact(&mut rest).await.unwrap();
    i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]])
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

fn negotiate_protocol_version(newest_minor_supported: u16, unrecognized: &[&str]) -> Vec<u8> {
    negotiate_protocol_version_raw(i32::from(newest_minor_supported), unrecognized)
}

fn negotiate_protocol_version_raw(newest_minor_supported: i32, unrecognized: &[&str]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&newest_minor_supported.to_be_bytes());
    payload.extend_from_slice(&(unrecognized.len() as i32).to_be_bytes());
    for name in unrecognized {
        payload.extend_from_slice(name.as_bytes());
        payload.push(0);
    }
    backend_frame(b'v', &payload)
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

#[tokio::test]
async fn startup_accepts_negotiate_protocol_version_and_tracks_minor() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let version = read_startup_version(&mut sock).await;
        assert_eq!(version, PROTOCOL_VERSION_3_2);

        sock.write_all(&negotiate_protocol_version(
            1,
            &["extra_float_digits", "application_name"],
        ))
        .await
        .unwrap();
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let conn = PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
        .await
        .unwrap();

    assert_eq!(conn.requested_protocol_minor(), 2);
    assert_eq!(conn.negotiated_protocol_minor(), 1);

    server.await.unwrap();
}

#[tokio::test]
async fn startup_accepts_negotiate_protocol_version_full_protocol_value() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let version = read_startup_version(&mut sock).await;
        assert_eq!(version, PROTOCOL_VERSION_3_2);

        sock.write_all(&negotiate_protocol_version_raw(PROTOCOL_VERSION_3_0, &[]))
            .await
            .unwrap();
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let conn = PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
        .await
        .unwrap();

    assert_eq!(conn.requested_protocol_minor(), 2);
    assert_eq!(conn.negotiated_protocol_minor(), 0);

    server.await.unwrap();
}

#[tokio::test]
async fn startup_explicit_protocol_rejection_retries_once_with_3_0() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let first_version = read_startup_version(&mut first).await;
        assert_eq!(first_version, PROTOCOL_VERSION_3_2);
        first
            .write_all(&error_response(
                "0A000",
                "unsupported frontend protocol 3.2: server supports 3.0 to 3.0",
            ))
            .await
            .unwrap();
        first.flush().await.unwrap();
        drop(first);

        let (mut second, _) = listener.accept().await.unwrap();
        let second_version = read_startup_version(&mut second).await;
        assert_eq!(second_version, PROTOCOL_VERSION_3_0);
        second.write_all(&auth_ok()).await.unwrap();
        second.write_all(&ready_idle()).await.unwrap();
        second.flush().await.unwrap();

        let third = timeout(Duration::from_millis(250), listener.accept()).await;
        assert!(
            third.is_err(),
            "downgrade logic must retry at most once after explicit protocol rejection"
        );
    });

    let conn = PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
        .await
        .unwrap();
    assert_eq!(conn.requested_protocol_minor(), 0);
    assert_eq!(conn.negotiated_protocol_minor(), 0);

    server.await.unwrap();
}

#[tokio::test]
async fn startup_non_protocol_failure_does_not_trigger_downgrade_retry() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let first_version = read_startup_version(&mut first).await;
        assert_eq!(first_version, PROTOCOL_VERSION_3_2);
        first
            .write_all(&error_response(
                "28P01",
                "password authentication failed for user \"test_user\"",
            ))
            .await
            .unwrap();
        first.flush().await.unwrap();
        drop(first);

        let second = timeout(Duration::from_millis(250), listener.accept()).await;
        assert!(
            second.is_err(),
            "client must not retry downgrade on non-protocol startup failures"
        );
    });

    let err =
        match PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
        {
            Ok(_) => panic!("startup should fail"),
            Err(err) => err,
        };
    assert!(err.to_string().contains("password authentication failed"));

    server.await.unwrap();
}

#[tokio::test]
async fn startup_protocol_rejection_stops_after_one_downgrade_retry() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let first_version = read_startup_version(&mut first).await;
        assert_eq!(first_version, PROTOCOL_VERSION_3_2);
        first
            .write_all(&error_response(
                "0A000",
                "unsupported frontend protocol 3.2: server supports 3.0 to 3.0",
            ))
            .await
            .unwrap();
        first.flush().await.unwrap();
        drop(first);

        let (mut second, _) = listener.accept().await.unwrap();
        let second_version = read_startup_version(&mut second).await;
        assert_eq!(second_version, PROTOCOL_VERSION_3_0);
        second
            .write_all(&error_response(
                "0A000",
                "unsupported frontend protocol 3.0: server supports 2.0 to 2.0",
            ))
            .await
            .unwrap();
        second.flush().await.unwrap();
        drop(second);

        let third = timeout(Duration::from_millis(250), listener.accept()).await;
        assert!(
            third.is_err(),
            "client must not keep retrying after one downgrade attempt"
        );
    });

    let err =
        match PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
        {
            Ok(_) => panic!("connection should fail after downgrade retry"),
            Err(err) => err,
        };
    assert!(err.to_string().to_ascii_lowercase().contains("protocol"));

    server.await.unwrap();
}
