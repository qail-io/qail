//! Replication protocol hardening tests.
//!
//! Verifies replication stream state transitions are fail-closed.

use qail_pg::protocol::PROTOCOL_VERSION_3_2;
use qail_pg::{ConnectOptions, PgConnection};
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

fn copy_both_response_text_zero_cols() -> Vec<u8> {
    // format=0 (text), num_columns=0
    backend_frame(b'W', &[0, 0, 0])
}

fn copy_both_response_binary_zero_cols() -> Vec<u8> {
    // format=1 (binary), num_columns=0
    backend_frame(b'W', &[1, 0, 0])
}

fn copy_both_response_text_one_col() -> Vec<u8> {
    // format=0 (text), num_columns=1, col[0]=0 (text)
    backend_frame(b'W', &[0, 0, 1, 0, 0])
}

fn copy_data(payload: &[u8]) -> Vec<u8> {
    backend_frame(b'd', payload)
}

fn copy_data_xlog(wal_start: u64, wal_end: u64, server_time_micros: i64, data: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 8 + 8 + 8 + data.len());
    payload.push(b'w');
    payload.extend_from_slice(&wal_start.to_be_bytes());
    payload.extend_from_slice(&wal_end.to_be_bytes());
    payload.extend_from_slice(&server_time_micros.to_be_bytes());
    payload.extend_from_slice(data);
    copy_data(&payload)
}

fn copy_data_keepalive(wal_end: u64, server_time_micros: i64, reply_requested: bool) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 8 + 8 + 1);
    payload.push(b'k');
    payload.extend_from_slice(&wal_end.to_be_bytes());
    payload.extend_from_slice(&server_time_micros.to_be_bytes());
    payload.push(if reply_requested { 1 } else { 0 });
    copy_data(&payload)
}

async fn connect_plain(port: u16) -> PgConnection {
    PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
        .await
        .unwrap()
}

async fn connect_replication_mode(port: u16) -> PgConnection {
    PgConnection::connect_with_options(
        "127.0.0.1",
        port,
        "test_user",
        "test_db",
        None,
        ConnectOptions::default().with_logical_replication(),
    )
    .await
    .unwrap()
}

async fn assert_no_frontend_frame(sock: &mut TcpStream) {
    let mut first_byte = [0u8; 1];
    if let Ok(Ok(_)) = timeout(Duration::from_millis(250), sock.read_exact(&mut first_byte)).await {
        panic!(
            "unexpected frontend frame type '{}' while operation should be blocked locally",
            first_byte[0] as char
        );
    }
}

#[tokio::test]
async fn recv_replication_message_before_start_is_rejected() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_replication_mode(port).await;

    let err = conn.recv_replication_message().await.unwrap_err();
    assert!(
        err.to_string().contains("stream is not active"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn send_standby_status_update_before_start_is_rejected() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_replication_mode(port).await;

    let err = conn
        .send_standby_status_update(0, 0, 0, false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("stream is not active"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn start_logical_replication_rejects_second_start() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q', "START_REPLICATION must use simple query");
        assert!(
            payload.starts_with(b"START_REPLICATION "),
            "expected START_REPLICATION query, got {:?}",
            String::from_utf8_lossy(&payload)
        );

        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // If client mistakenly sends a second START_REPLICATION, avoid hanging.
        let mut first_byte = [0u8; 1];
        if let Ok(Ok(_)) =
            timeout(Duration::from_millis(250), sock.read_exact(&mut first_byte)).await
        {
            let msg_type = first_byte[0];
            let mut len_buf = [0u8; 4];
            sock.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            assert!(len >= 4, "frontend frame length must be >= 4");
            let payload_len = len - 4;
            let mut payload = vec![0u8; payload_len];
            sock.read_exact(&mut payload).await.unwrap();
            assert_eq!(msg_type, b'Q');
            sock.write_all(&ready_idle()).await.unwrap();
            sock.flush().await.unwrap();
        }
    });

    let mut conn = connect_replication_mode(port).await;

    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let err = conn
        .start_logical_replication("slot_b", "0/16B6C50", &[])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("already active"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn recv_terminal_message_clears_active_state() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Immediately terminate stream from server side.
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_replication_mode(port).await;

    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let err = conn.recv_replication_message().await.unwrap_err();
    assert!(
        err.to_string().contains("ended with ReadyForQuery"),
        "unexpected recv error: {}",
        err
    );

    let err2 = conn
        .send_standby_status_update(0, 0, 0, false)
        .await
        .unwrap_err();
    assert!(
        err2.to_string().contains("stream is not active"),
        "unexpected post-terminal error: {}",
        err2
    );

    server.await.unwrap();
}

#[tokio::test]
async fn send_standby_status_update_sends_copydata_when_active() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        let (copy_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(copy_type, b'd', "standby update must be sent as CopyData");
        assert_eq!(payload.first().copied(), Some(b'r'));
    });

    let mut conn = connect_replication_mode(port).await;

    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();
    conn.send_standby_status_update(10, 10, 10, false)
        .await
        .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn standby_status_update_rejects_invalid_lsn_order_without_sending_frame() {
    let (listener, port) = mock_listener().await;
    let (checked_tx, checked_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Invalid local update must be blocked before write.
        assert_no_frontend_frame(&mut sock).await;
        let _ = checked_tx.send(());

        // A subsequent valid update should still be sent.
        let (copy_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(copy_type, b'd', "standby update must be sent as CopyData");
        assert_eq!(payload.first().copied(), Some(b'r'));
    });

    let mut conn = connect_replication_mode(port).await;

    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let err = conn
        .send_standby_status_update(10, 11, 10, false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("flush_lsn"),
        "unexpected error: {}",
        err
    );

    checked_rx.await.unwrap();
    conn.send_standby_status_update(11, 11, 10, false)
        .await
        .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn standby_status_update_rejects_write_ahead_of_server_wal_without_sending_frame() {
    let (listener, port) = mock_listener().await;
    let (checked_tx, checked_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        sock.write_all(&copy_data_xlog(100, 200, 1, b"a"))
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Invalid local update must be blocked before write.
        assert_no_frontend_frame(&mut sock).await;
        let _ = checked_tx.send(());

        // A subsequent valid update should still be sent.
        let (copy_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(copy_type, b'd', "standby update must be sent as CopyData");
        assert_eq!(payload.first().copied(), Some(b'r'));
    });

    let mut conn = connect_replication_mode(port).await;

    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let first = conn.recv_replication_message().await.unwrap();
    match first {
        qail_pg::ReplicationStreamMessage::XLogData(x) => {
            assert_eq!(x.wal_end, 200);
        }
        _ => panic!("expected first XLogData"),
    }

    let err = conn
        .send_standby_status_update(201, 200, 200, false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("exceeds last seen server wal_end"),
        "unexpected error: {}",
        err
    );

    checked_rx.await.unwrap();
    conn.send_standby_status_update(200, 200, 200, false)
        .await
        .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn unknown_copydata_tag_is_rejected_and_clears_active_state() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Unknown replication CopyData subtag.
        sock.write_all(&copy_data(b"xgarbage")).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_replication_mode(port).await;

    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let err = conn.recv_replication_message().await.unwrap_err();
    assert!(
        err.to_string()
            .contains("Unsupported replication CopyData tag"),
        "unexpected error: {}",
        err
    );

    // State must be cleared after protocol violation.
    let err2 = conn
        .send_standby_status_update(0, 0, 0, false)
        .await
        .unwrap_err();
    assert!(
        err2.to_string().contains("stream is not active"),
        "unexpected post-violation error: {}",
        err2
    );

    server.await.unwrap();
}

#[tokio::test]
async fn start_logical_replication_rejects_binary_copyboth_format() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_binary_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_replication_mode(port).await;

    let err = conn
        .start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("unsupported CopyBothResponse format"),
        "unexpected error: {}",
        err
    );

    let err2 = conn.recv_replication_message().await.unwrap_err();
    assert!(
        err2.to_string().contains("stream is not active"),
        "unexpected post-reject error: {}",
        err2
    );

    server.await.unwrap();
}

#[tokio::test]
async fn start_logical_replication_rejects_nonempty_copyboth_columns() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_one_col())
            .await
            .unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_replication_mode(port).await;

    let err = conn
        .start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("unexpected CopyBothResponse column formats"),
        "unexpected error: {}",
        err
    );

    let err2 = conn.recv_replication_message().await.unwrap_err();
    assert!(
        err2.to_string().contains("stream is not active"),
        "unexpected post-reject error: {}",
        err2
    );

    server.await.unwrap();
}

#[tokio::test]
async fn start_logical_replication_requires_replication_mode() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_plain(port).await;

    let err = conn
        .start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("replication=database"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn recv_replication_message_requires_replication_mode() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_plain(port).await;
    let err = conn.recv_replication_message().await.unwrap_err();
    assert!(
        err.to_string().contains("replication=database"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn send_standby_status_update_requires_replication_mode() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_plain(port).await;
    let err = conn
        .send_standby_status_update(0, 0, 0, false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("replication=database"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn identify_system_requires_replication_mode() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_plain(port).await;
    let err = conn.identify_system().await.unwrap_err();
    assert!(
        err.to_string().contains("replication=database"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn create_slot_requires_replication_mode() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_plain(port).await;
    let err = conn
        .create_logical_replication_slot("slot_a", "pgoutput", true, false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("replication=database"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn drop_slot_requires_replication_mode() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_plain(port).await;
    let err = conn
        .drop_replication_slot("slot_a", false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("replication=database"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn identify_system_rejected_while_stream_active_without_sending_query() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        assert_no_frontend_frame(&mut sock).await;
    });

    let mut conn = connect_replication_mode(port).await;
    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let err = conn.identify_system().await.unwrap_err();
    assert!(
        err.to_string()
            .contains("cannot run while replication stream is active"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn create_slot_rejected_while_stream_active_without_sending_query() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        assert_no_frontend_frame(&mut sock).await;
    });

    let mut conn = connect_replication_mode(port).await;
    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let err = conn
        .create_logical_replication_slot("slot_b", "pgoutput", true, false)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("cannot run while replication stream is active"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn drop_slot_rejected_while_stream_active_without_sending_query() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        assert_no_frontend_frame(&mut sock).await;
    });

    let mut conn = connect_replication_mode(port).await;
    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let err = conn
        .drop_replication_slot("slot_b", false)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("cannot run while replication stream is active"),
        "unexpected error: {}",
        err
    );

    server.await.unwrap();
}

#[tokio::test]
async fn xlogdata_wal_end_regression_is_rejected_and_clears_stream_state() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // First frame establishes wal_end=200.
        sock.write_all(&copy_data_xlog(100, 200, 1, b"a"))
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Second frame regresses wal_end to 150.
        sock.write_all(&copy_data_xlog(120, 150, 2, b"b"))
            .await
            .unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_replication_mode(port).await;
    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let first = conn.recv_replication_message().await.unwrap();
    match first {
        qail_pg::ReplicationStreamMessage::XLogData(x) => {
            assert_eq!(x.wal_end, 200);
        }
        _ => panic!("expected first XLogData"),
    }

    let err = conn.recv_replication_message().await.unwrap_err();
    assert!(
        err.to_string().contains("wal_end regressed"),
        "unexpected error: {}",
        err
    );

    // State must be cleared after regression.
    let err2 = conn
        .send_standby_status_update(0, 0, 0, false)
        .await
        .unwrap_err();
    assert!(
        err2.to_string().contains("stream is not active"),
        "unexpected post-regression error: {}",
        err2
    );

    server.await.unwrap();
}

#[tokio::test]
async fn keepalive_wal_end_regression_below_last_xlog_is_rejected() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Establish last seen XLogData wal_end = 200.
        sock.write_all(&copy_data_xlog(100, 200, 1, b"a"))
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Keepalive regresses below that point.
        sock.write_all(&copy_data_keepalive(150, 2, false))
            .await
            .unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_replication_mode(port).await;
    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let first = conn.recv_replication_message().await.unwrap();
    match first {
        qail_pg::ReplicationStreamMessage::XLogData(x) => {
            assert_eq!(x.wal_end, 200);
        }
        _ => panic!("expected first XLogData"),
    }

    let err = conn.recv_replication_message().await.unwrap_err();
    assert!(
        err.to_string().contains("keepalive wal_end regressed"),
        "unexpected error: {}",
        err
    );

    let err2 = conn
        .send_standby_status_update(0, 0, 0, false)
        .await
        .unwrap_err();
    assert!(
        err2.to_string().contains("stream is not active"),
        "unexpected post-regression error: {}",
        err2
    );

    server.await.unwrap();
}

#[tokio::test]
async fn keepalive_wal_end_regression_after_keepalive_progress_is_rejected() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Establish watermark at wal_end = 200.
        sock.write_all(&copy_data_xlog(100, 200, 1, b"a"))
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Progress watermark via keepalive to 250.
        sock.write_all(&copy_data_keepalive(250, 2, false))
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Regress keepalive wal_end below prior keepalive.
        sock.write_all(&copy_data_keepalive(240, 3, false))
            .await
            .unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_replication_mode(port).await;
    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let first = conn.recv_replication_message().await.unwrap();
    match first {
        qail_pg::ReplicationStreamMessage::XLogData(x) => {
            assert_eq!(x.wal_end, 200);
        }
        _ => panic!("expected first XLogData"),
    }

    let second = conn.recv_replication_message().await.unwrap();
    match second {
        qail_pg::ReplicationStreamMessage::Keepalive(k) => {
            assert_eq!(k.wal_end, 250);
        }
        _ => panic!("expected keepalive"),
    }

    let err = conn.recv_replication_message().await.unwrap_err();
    assert!(
        err.to_string().contains("keepalive wal_end regressed"),
        "unexpected error: {}",
        err
    );

    let err2 = conn
        .send_standby_status_update(0, 0, 0, false)
        .await
        .unwrap_err();
    assert!(
        err2.to_string().contains("stream is not active"),
        "unexpected post-regression error: {}",
        err2
    );

    server.await.unwrap();
}

#[tokio::test]
async fn xlog_wal_end_regression_below_keepalive_is_rejected() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, _payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        sock.write_all(&copy_both_response_text_zero_cols())
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Establish watermark at wal_end = 200.
        sock.write_all(&copy_data_xlog(100, 200, 1, b"a"))
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Progress watermark via keepalive to 250.
        sock.write_all(&copy_data_keepalive(250, 2, false))
            .await
            .unwrap();
        sock.flush().await.unwrap();

        // Regress XLogData wal_end below keepalive watermark.
        sock.write_all(&copy_data_xlog(110, 240, 3, b"b"))
            .await
            .unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn = connect_replication_mode(port).await;
    conn.start_logical_replication("slot_a", "0/16B6C50", &[])
        .await
        .unwrap();

    let first = conn.recv_replication_message().await.unwrap();
    match first {
        qail_pg::ReplicationStreamMessage::XLogData(x) => {
            assert_eq!(x.wal_end, 200);
        }
        _ => panic!("expected first XLogData"),
    }

    let second = conn.recv_replication_message().await.unwrap();
    match second {
        qail_pg::ReplicationStreamMessage::Keepalive(k) => {
            assert_eq!(k.wal_end, 250);
        }
        _ => panic!("expected keepalive"),
    }

    let err = conn.recv_replication_message().await.unwrap_err();
    assert!(
        err.to_string().contains("XLogData wal_end regressed"),
        "unexpected error: {}",
        err
    );

    let err2 = conn
        .send_standby_status_update(0, 0, 0, false)
        .await
        .unwrap_err();
    assert!(
        err2.to_string().contains("stream is not active"),
        "unexpected post-regression error: {}",
        err2
    );

    server.await.unwrap();
}
