//! Cached prepared-statement desync hardening tests.
//!
//! These tests verify fail-closed behavior and cache invalidation when the
//! server returns unexpected or malformed protocol sequences.

use qail_core::ast::Qail;
use qail_pg::protocol::PROTOCOL_VERSION_3_2;
use qail_pg::{PgConnection, PgDriver};
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

fn parse_complete() -> Vec<u8> {
    backend_frame(b'1', &[])
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

fn data_row_zero_cols() -> Vec<u8> {
    backend_frame(b'D', &0i16.to_be_bytes())
}

fn row_description_zero_cols() -> Vec<u8> {
    backend_frame(b'T', &0i16.to_be_bytes())
}

fn empty_query_response() -> Vec<u8> {
    backend_frame(b'I', &[])
}

#[tokio::test]
async fn query_cached_unexpected_backend_invalidates_cache() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // First query_cached call (new statement) must include Parse.
        let first = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(first.first().copied(), Some(b'P'));

        // Trigger unexpected message path on the client.
        sock.write_all(&backend_key_data()).await.unwrap();
        sock.flush().await.unwrap();

        // Second query_cached call should re-send Parse if cache was invalidated.
        let second = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(
            second.first().copied(),
            Some(b'P'),
            "cached query must re-parse after unexpected backend message"
        );

        sock.write_all(&parse_complete()).await.unwrap();
        sock.write_all(&bind_complete()).await.unwrap();
        sock.write_all(&command_complete("SELECT 1")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let sql = "SELECT 1";
    let err = conn.query_cached(sql, &[]).await.unwrap_err();
    let err_msg = err.to_string();
    assert!(err_msg.contains("extended-query cached execute"));

    let rows = conn.query_cached(sql, &[]).await.unwrap();
    assert!(rows.is_empty());

    server.await.unwrap();
}

#[tokio::test]
async fn query_cached_encode_failure_does_not_poison_cache() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // First query_cached call fails client-side during Bind encoding, so
        // this is the first frontend sequence the server sees.
        let second = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(
            second.first().copied(),
            Some(b'P'),
            "query_cached must still Parse after local encode failure"
        );

        sock.write_all(&parse_complete()).await.unwrap();
        sock.write_all(&bind_complete()).await.unwrap();
        sock.write_all(&command_complete("SELECT 0")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let too_many_params: Vec<Option<Vec<u8>>> = (0..32768).map(|_| Some(vec![b'x'])).collect();
    let err = conn
        .query_cached("SELECT 1", &too_many_params)
        .await
        .unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.to_ascii_lowercase().contains("too many"),
        "unexpected encode error: {err_msg}"
    );

    let rows = conn.query_cached("SELECT 1", &[]).await.unwrap();
    assert!(rows.is_empty());

    server.await.unwrap();
}

#[tokio::test]
async fn pipeline_ast_cached_unexpected_backend_invalidates_cache() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // First pipeline call (cache miss) must include Parse.
        let first = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(first.first().copied(), Some(b'P'));

        // Trigger unexpected fast-path message type.
        sock.write_all(&backend_key_data()).await.unwrap();
        sock.flush().await.unwrap();

        // Second pipeline call must parse again if cache cleanup worked.
        let second = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(
            second.first().copied(),
            Some(b'P'),
            "pipeline_execute_count_ast_cached must re-parse after unexpected backend message"
        );

        sock.write_all(&parse_complete()).await.unwrap();
        sock.write_all(&bind_complete()).await.unwrap();
        sock.write_all(&command_complete("SELECT 1")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let first_cmd = Qail::get("(SELECT 1) qail_test_subq");
    let err = conn
        .pipeline_execute_count_ast_cached(&[first_cmd])
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("pipeline_execute_count_ast_cached")
    );

    let second_cmd = Qail::get("(SELECT 1) qail_test_subq");
    let completed = conn
        .pipeline_execute_count_ast_cached(&[second_cmd])
        .await
        .unwrap();
    assert_eq!(completed, 1);

    server.await.unwrap();
}

#[tokio::test]
async fn pipeline_ast_cached_parsecomplete_mismatch_invalidates_cache() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // First call must include Parse.
        let first = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(first.first().copied(), Some(b'P'));

        // Malformed response: no ParseComplete for a cache-miss parse.
        sock.write_all(&bind_complete()).await.unwrap();
        sock.write_all(&command_complete("SELECT 1")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // Second call should parse again if mismatch invalidated cache.
        let second = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(
            second.first().copied(),
            Some(b'P'),
            "pipeline_execute_count_ast_cached must re-parse after ParseComplete mismatch"
        );

        sock.write_all(&parse_complete()).await.unwrap();
        sock.write_all(&bind_complete()).await.unwrap();
        sock.write_all(&command_complete("SELECT 1")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let first_cmd = Qail::get("(SELECT 1) qail_test_subq");
    let err = conn
        .pipeline_execute_count_ast_cached(&[first_cmd])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("ParseComplete mismatch"));

    let second_cmd = Qail::get("(SELECT 1) qail_test_subq");
    let completed = conn
        .pipeline_execute_count_ast_cached(&[second_cmd])
        .await
        .unwrap();
    assert_eq!(completed, 1);

    server.await.unwrap();
}

#[tokio::test]
async fn query_cached_rejects_bind_before_parse() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let first = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(first.first().copied(), Some(b'P'));

        // Adversarial ordering: BindComplete before ParseComplete.
        sock.write_all(&bind_complete()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = conn.query_cached("SELECT 1", &[]).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("BindComplete before ParseComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn query_prepared_single_rejects_data_before_bind() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // Prepare phase: Parse + Sync
        let prepare_seq = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(prepare_seq.first().copied(), Some(b'P'));
        sock.write_all(&parse_complete()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // Execute phase: Bind + Execute + Sync
        let exec_seq = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(exec_seq.first().copied(), Some(b'B'));

        // Adversarial ordering: DataRow before BindComplete.
        sock.write_all(&data_row_zero_cols()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();
    let stmt = conn.prepare("SELECT 1").await.unwrap();
    let err = conn.query_prepared_single(&stmt, &[]).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("DataRow before BindComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn simple_query_rejects_data_before_row_description() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        assert!(payload.ends_with(&[0]));

        sock.write_all(&data_row_zero_cols()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = match conn.simple_query("SELECT 1").await {
        Ok(_) => panic!("expected protocol error"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("DataRow before RowDescription"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn simple_query_rejects_ready_before_completion() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        assert!(payload.ends_with(&[0]));

        sock.write_all(&row_description_zero_cols()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = match conn.simple_query("SELECT 1").await {
        Ok(_) => panic!("expected protocol error"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("ReadyForQuery before CommandComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn execute_simple_rejects_ready_before_completion() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        assert!(payload.ends_with(&[0]));

        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = conn.execute_simple("SELECT 1").await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ReadyForQuery before completion"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn execute_simple_accepts_row_stream_then_completion() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        assert!(payload.ends_with(&[0]));

        // Execute path must tolerate row-producing simple statements and keep draining.
        sock.write_all(&row_description_zero_cols()).await.unwrap();
        sock.write_all(&data_row_zero_cols()).await.unwrap();
        sock.write_all(&command_complete("SELECT 1")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    conn.execute_simple("SELECT set_config('a','b',true)")
        .await
        .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn simple_query_rejects_duplicate_row_description_before_completion() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        assert!(payload.ends_with(&[0]));

        sock.write_all(&row_description_zero_cols()).await.unwrap();
        sock.write_all(&row_description_zero_cols()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = match conn.simple_query("SELECT 1").await {
        Ok(_) => panic!("expected protocol error"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("duplicate RowDescription"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn simple_query_rejects_empty_query_response_inside_row_stream() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        assert!(payload.ends_with(&[0]));

        sock.write_all(&row_description_zero_cols()).await.unwrap();
        sock.write_all(&empty_query_response()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = match conn.simple_query("SELECT 1").await {
        Ok(_) => panic!("expected protocol error"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("EmptyQueryResponse during active row stream"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn simple_query_allows_command_then_result_statement_sequence() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        assert!(payload.ends_with(&[0]));

        // Statement 1: command-only completion.
        sock.write_all(&command_complete("UPDATE 1")).await.unwrap();
        // Statement 2: row-producing statement.
        sock.write_all(&row_description_zero_cols()).await.unwrap();
        sock.write_all(&data_row_zero_cols()).await.unwrap();
        sock.write_all(&command_complete("SELECT 1")).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let rows = conn.simple_query("UPDATE x; SELECT 1").await.unwrap();
    assert_eq!(rows.len(), 1, "expected only row-producing statement rows");
    assert_eq!(rows[0].columns.len(), 0);

    server.await.unwrap();
}

#[tokio::test]
async fn driver_fetch_all_uncached_rejects_data_before_bind() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let seq = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(seq.first().copied(), Some(b'P'));
        assert!(seq.contains(&b'B'));
        assert!(seq.contains(&b'D'));
        assert!(seq.contains(&b'E'));

        sock.write_all(&parse_complete()).await.unwrap();
        sock.write_all(&data_row_zero_cols()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut driver = PgDriver::connect_with_password("127.0.0.1", port, "test_user", "test_db", "")
        .await
        .unwrap();

    let err = match driver.fetch_all_uncached(&Qail::get("users")).await {
        Ok(_) => panic!("expected protocol error"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("DataRow before BindComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn driver_fetch_all_fast_rejects_data_before_bind() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let seq = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(seq.first().copied(), Some(b'P'));
        assert!(seq.contains(&b'B'));
        assert!(seq.contains(&b'D'));
        assert!(seq.contains(&b'E'));

        sock.write_all(&parse_complete()).await.unwrap();
        sock.write_all(&data_row_zero_cols()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut driver = PgDriver::connect_with_password("127.0.0.1", port, "test_user", "test_db", "")
        .await
        .unwrap();

    let err = match driver.fetch_all_fast(&Qail::get("users")).await {
        Ok(_) => panic!("expected protocol error"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("DataRow before BindComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn driver_fetch_all_fast_rejects_ready_before_completion() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let seq = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(seq.first().copied(), Some(b'P'));
        assert!(seq.contains(&b'B'));
        assert!(seq.contains(&b'D'));
        assert!(seq.contains(&b'E'));

        sock.write_all(&parse_complete()).await.unwrap();
        sock.write_all(&bind_complete()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut driver = PgDriver::connect_with_password("127.0.0.1", port, "test_user", "test_db", "")
        .await
        .unwrap();

    let err = match driver.fetch_all_fast(&Qail::get("users")).await {
        Ok(_) => panic!("expected protocol error"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("ReadyForQuery before completion message"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn pipeline_ast_fast_rejects_data_before_bind() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let seq = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(seq.first().copied(), Some(b'P'));
        assert!(seq.contains(&b'B'));
        assert!(seq.contains(&b'E'));

        sock.write_all(&parse_complete()).await.unwrap();
        // Out-of-order: DataRow before BindComplete.
        sock.write_all(&data_row_zero_cols()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = conn
        .pipeline_execute_count_ast_oneshot(&[Qail::get("(SELECT 1) qail_test_subq")])
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("DataRow before BindComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn pipeline_ast_rejects_data_before_bind() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let seq = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(seq.first().copied(), Some(b'P'));
        assert!(seq.contains(&b'B'));
        assert!(seq.contains(&b'E'));

        sock.write_all(&parse_complete()).await.unwrap();
        sock.write_all(&data_row_zero_cols()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = conn
        .pipeline_execute_rows_ast(&[Qail::get("(SELECT 1) qail_test_subq")])
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("DataRow before BindComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn query_pipeline_rejects_bind_before_parse() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let seq = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(seq.first().copied(), Some(b'P'));
        assert!(seq.contains(&b'B'));
        assert!(seq.contains(&b'E'));

        sock.write_all(&bind_complete()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();
    let query_params: &[Option<Vec<u8>>] = &[];
    let err = conn
        .query_pipeline(&[("SELECT 1", query_params)])
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("BindComplete before ParseComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn pipeline_simple_fast_rejects_data_before_row_description() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        let (msg_type, payload) = read_frontend_frame(&mut sock).await;
        assert_eq!(msg_type, b'Q');
        assert!(
            payload.ends_with(&[0]),
            "simple query payload must be NUL-terminated"
        );

        // Out-of-order: DataRow before RowDescription.
        sock.write_all(&data_row_zero_cols()).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();

    let err = conn
        .pipeline_execute_count_simple_ast(&[Qail::get("(SELECT 1) qail_test_subq")])
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("DataRow before RowDescription"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}

#[tokio::test]
async fn pipeline_prepared_fast_rejects_command_before_bind() {
    let (listener, port) = mock_listener().await;

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup_message(&mut sock).await;
        sock.write_all(&auth_ok()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // prepare(): Parse + Sync
        let prep_seq = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(prep_seq.first().copied(), Some(b'P'));
        sock.write_all(&parse_complete()).await.unwrap();
        sock.write_all(&ready_idle()).await.unwrap();
        sock.flush().await.unwrap();

        // pipeline_execute_prepared_count(): Bind + Execute + Sync
        let seq = read_frontend_msg_types_until_sync(&mut sock).await;
        assert_eq!(seq.first().copied(), Some(b'B'));
        // Out-of-order: completion before BindComplete.
        sock.write_all(&command_complete("SELECT 1")).await.unwrap();
        sock.flush().await.unwrap();
    });

    let mut conn =
        PgConnection::connect_with_password("127.0.0.1", port, "test_user", "test_db", None)
            .await
            .unwrap();
    let stmt = conn.prepare("SELECT 1").await.unwrap();
    let err = conn
        .pipeline_execute_prepared_count(&stmt, &[Vec::new()])
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("CommandComplete before BindComplete"),
        "unexpected error message: {msg}"
    );

    server.await.unwrap();
}
