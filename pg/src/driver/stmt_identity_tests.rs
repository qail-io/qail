//! Prepared-statement identity: a statement name is reused only for the SQL it
//! was parsed from.
//!
//! Cache keys are a 64-bit SipHash of the SQL bytes with fixed, public keys, so
//! a collision is constructible offline. These tests forge one by pointing a
//! second SQL at the first SQL's statement, or by occupying the hash-derived
//! server-side name with other SQL, and check that every cached path executes
//! the SQL it was given.
//!
//! Live checks use TEMP tables only:
//!   QAIL_TEST_DB_URL=postgres://... cargo test -p qail-pg --lib stmt_identity -- --ignored

use super::prepared::sql_bytes_hash;
use super::{PgConnection, PgDriver, PgPool, PgResult, PoolConfig, PreparedStatement};
use crate::protocol::{AstEncoder, BackendMessage, PgEncoder};
use bytes::BytesMut;
use qail_core::ast::{Qail, Value};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

fn encoded_sql(cmd: &Qail) -> String {
    let mut sql = BytesMut::new();
    let mut params = Vec::new();
    assert!(
        AstEncoder::encode_cacheable_cmd_sql_to(cmd, &mut sql, &mut params).expect("encode"),
        "command must take the cached path"
    );
    String::from_utf8(sql.to_vec()).expect("utf-8 SQL")
}

fn cmd_hash(cmd: &Qail) -> u64 {
    sql_bytes_hash(encoded_sql(cmd).as_bytes())
}

/// Statement name the AST cached paths derive from a cache key.
fn ast_stmt_name(hash: u64) -> String {
    format!("qail_{:x}", hash)
}

/// Statement name `query_cached` / `prepare` derive from SQL text.
fn raw_stmt_name(sql: &str) -> String {
    format!("s{:016x}", sql_bytes_hash(sql.as_bytes()))
}

/// `SELECT label ... WHERE id = $1`: one text column.
fn owner_cmd(table: &str) -> Qail {
    Qail::get(table).columns(["label"]).eq("id", 2)
}

/// `SELECT id, label ... WHERE id = $1`: same parameter shape, two columns.
fn victim_cmd(table: &str) -> Qail {
    Qail::get(table).columns(["id", "label"]).eq("id", 2)
}

/// Point `victim`'s cache key at the statement cached for `owner`, as a real
/// 64-bit collision would.
fn forge_cache_collision(conn: &mut PgConnection, owner: &Qail, victim: &Qail) -> String {
    let name = conn
        .stmt_cache
        .peek(&cmd_hash(owner))
        .expect("owner statement cached")
        .to_string();
    conn.stmt_cache.put(cmd_hash(victim), name.clone());
    name
}

/// Parse `sql` under `name` on the server without telling the local cache,
/// as after a cleared local state or a collision on the hash-derived name.
async fn parse_on_server(conn: &mut PgConnection, name: &str, sql: &str) {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&PgEncoder::try_encode_parse(name, sql, &[]).expect("parse"));
    PgEncoder::encode_sync_to(&mut buf);
    conn.send_bytes(&buf).await.expect("send parse");
    loop {
        match conn.recv().await.expect("recv") {
            BackendMessage::ReadyForQuery(_) => break,
            BackendMessage::ErrorResponse(err) => panic!("server parse failed: {:?}", err),
            _ => {}
        }
    }
}

async fn temp_table(conn: &mut PgConnection) -> PgResult<String> {
    let table = "qail_stmt_identity".to_string();
    conn.execute_simple(&format!(
        "DROP TABLE IF EXISTS pg_temp.{table}; \
         CREATE TEMP TABLE {table} (id integer, label text); \
         INSERT INTO {table} VALUES (1, 'first'), (2, 'second')"
    ))
    .await?;
    Ok(table)
}

fn text_cells(rows: &[super::PgRow]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            (0..row.columns.len())
                .map(|i| row.text(i))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn raw_cells(rows: &[Vec<Option<Vec<u8>>>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|c| String::from_utf8(c.clone().unwrap_or_default()).expect("utf-8"))
                .collect()
        })
        .collect()
}

fn owner_rows() -> Vec<Vec<String>> {
    vec![vec!["second".to_string()]]
}

fn victim_rows() -> Vec<Vec<String>> {
    vec![vec!["2".to_string(), "second".to_string()]]
}

#[derive(Clone, Copy, Debug)]
enum HandlePath {
    Rows,
    BinaryRows,
    Count,
    Reuse,
    VisitRows,
    VisitBytes,
    VisitFirst,
    VisitFour,
}

const HANDLE_PATHS: [HandlePath; 8] = [
    HandlePath::Rows,
    HandlePath::BinaryRows,
    HandlePath::Count,
    HandlePath::Reuse,
    HandlePath::VisitRows,
    HandlePath::VisitBytes,
    HandlePath::VisitFirst,
    HandlePath::VisitFour,
];

async fn execute_handle_path(
    conn: &mut PgConnection,
    stmt: &PreparedStatement,
    params: &[Option<Vec<u8>>],
    path: HandlePath,
    callbacks: &mut usize,
) -> PgResult<()> {
    match path {
        HandlePath::Rows => conn.query_prepared_single(stmt, params).await.map(|_| ()),
        HandlePath::BinaryRows => conn
            .query_prepared_single_with_result_format(stmt, params, PgEncoder::FORMAT_BINARY)
            .await
            .map(|_| ()),
        HandlePath::Count => conn.query_prepared_single_count(stmt, params).await,
        HandlePath::Reuse => conn
            .query_prepared_single_reuse_with_result_format(stmt, params, PgEncoder::FORMAT_TEXT)
            .await
            .map(|_| ()),
        HandlePath::VisitRows => conn
            .query_prepared_single_reuse_visit_rows_with_result_format(
                stmt,
                params,
                PgEncoder::FORMAT_TEXT,
                |_| {
                    *callbacks += 1;
                    Ok(())
                },
            )
            .await
            .map(|_| ()),
        HandlePath::VisitBytes => conn
            .query_prepared_single_reuse_visit_bytes_rows_with_result_format(
                stmt,
                params,
                PgEncoder::FORMAT_TEXT,
                |_| {
                    *callbacks += 1;
                    Ok(())
                },
            )
            .await
            .map(|_| ()),
        HandlePath::VisitFirst => conn
            .query_prepared_single_reuse_visit_first_column_bytes_with_result_format(
                stmt,
                params,
                PgEncoder::FORMAT_TEXT,
                |_| {
                    *callbacks += 1;
                    Ok(())
                },
            )
            .await
            .map(|_| ()),
        HandlePath::VisitFour => conn
            .query_prepared_single_reuse_visit_first_four_columns_bytes_with_result_format(
                stmt,
                params,
                PgEncoder::FORMAT_TEXT,
                |_| {
                    *callbacks += 1;
                    Ok(())
                },
            )
            .await
            .map(|_| ()),
    }
}

// ── Offline: wire trace against an in-process peer ─────────────────────

#[cfg(unix)]
mod offline {
    use super::*;
    use crate::driver::connection::StatementCache;
    use crate::driver::stream::PgStream;
    use std::collections::{HashMap, VecDeque};
    use std::io::Read;
    use std::num::NonZeroUsize;
    use std::os::unix::net::UnixStream;

    /// The peer end is a plain non-blocking std socket: tokio's `try_read`
    /// reports WouldBlock until the reactor has seen readiness.
    fn peer_driver() -> (PgDriver, UnixStream) {
        let (unix_stream, peer) = UnixStream::pair().expect("unix stream pair");
        unix_stream.set_nonblocking(true).expect("nonblocking");
        peer.set_nonblocking(true).expect("nonblocking");
        let unix_stream = tokio::net::UnixStream::from_std(unix_stream).expect("tokio stream");
        let conn = PgConnection {
            stream: PgStream::Unix(unix_stream),
            buffer: BytesMut::with_capacity(1024),
            write_buf: BytesMut::with_capacity(1024),
            sql_buf: BytesMut::with_capacity(256),
            params_buf: Vec::new(),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(NonZeroUsize::new(16).expect("non-zero")),
            column_info_cache: HashMap::new(),
            process_id: 0,
            cancel_key_bytes: Vec::new(),
            requested_protocol_minor: PgConnection::default_protocol_minor(),
            negotiated_protocol_minor: PgConnection::default_protocol_minor(),
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled: false,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        };
        (PgDriver::new(conn), peer)
    }

    fn push(driver: &mut PgDriver, msg_type: u8, payload: &[u8]) {
        let buf = &mut driver.connection.buffer;
        buf.extend_from_slice(&[msg_type]);
        buf.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        buf.extend_from_slice(payload);
    }

    /// Backend replies to Parse + Describe(S) + Bind + Execute + Sync.
    fn push_miss_reply(driver: &mut PgDriver) {
        push(driver, b'1', &[]);
        let mut params = Vec::new();
        params.extend_from_slice(&1i16.to_be_bytes());
        params.extend_from_slice(&23u32.to_be_bytes());
        push(driver, b't', &params);
        push_hit_reply_with_description(driver);
    }

    fn push_hit_reply_with_description(driver: &mut PgDriver) {
        let mut desc = Vec::new();
        desc.extend_from_slice(&1i16.to_be_bytes());
        desc.extend_from_slice(b"label\0");
        desc.extend_from_slice(&0u32.to_be_bytes());
        desc.extend_from_slice(&0i16.to_be_bytes());
        desc.extend_from_slice(&crate::protocol::types::oid::TEXT.to_be_bytes());
        desc.extend_from_slice(&(-1i16).to_be_bytes());
        desc.extend_from_slice(&(-1i32).to_be_bytes());
        desc.extend_from_slice(&0i16.to_be_bytes());
        push(driver, b'T', &desc);
        push_hit_reply(driver);
    }

    /// Backend replies to Bind + Execute + Sync.
    fn push_hit_reply(driver: &mut PgDriver) {
        push(driver, b'2', &[]);
        let mut row = Vec::new();
        row.extend_from_slice(&1i16.to_be_bytes());
        row.extend_from_slice(&6i32.to_be_bytes());
        row.extend_from_slice(b"second");
        push(driver, b'D', &row);
        push(driver, b'C', b"SELECT 1\0");
        push(driver, b'Z', b"I");
    }

    /// Frontend frames the driver wrote: (type, first C-string of payload).
    fn written_frames(peer: &UnixStream) -> Vec<(u8, Vec<u8>)> {
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            match (&mut &*peer).read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => bytes.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("peer read: {e}"),
            }
        }
        let mut frames = Vec::new();
        let mut at = 0;
        while at < bytes.len() {
            let msg_type = bytes[at];
            let len = u32::from_be_bytes(bytes[at + 1..at + 5].try_into().unwrap()) as usize;
            frames.push((msg_type, bytes[at + 5..at + 1 + len].to_vec()));
            at += 1 + len;
        }
        frames
    }

    fn cstr_at(payload: &[u8], index: usize) -> String {
        let s = payload.split(|b| *b == 0).nth(index).unwrap_or_default();
        String::from_utf8(s.to_vec()).expect("utf-8")
    }

    /// Statement name a Parse ('P') or Bind ('B') frame refers to.
    fn frame_stmt_name(frame: &(u8, Vec<u8>)) -> String {
        match frame.0 {
            b'P' => cstr_at(&frame.1, 0),
            b'B' => cstr_at(&frame.1, 1),
            other => panic!("frame {} has no statement name", other as char),
        }
    }

    #[tokio::test]
    async fn prepared_handle_identity_rejects_mismatches_before_writing() {
        let mut accepted = Vec::new();
        for record in [None, Some("SELECT 2")] {
            for path in HANDLE_PATHS {
                let (mut driver, peer) = peer_driver();
                let stmt = PreparedStatement::from_sql("SELECT 1");
                if let Some(record) = record {
                    driver
                        .connection
                        .prepared_statements
                        .insert(stmt.name.clone(), record.into());
                }
                push(&mut driver, b'2', &[]);
                push(&mut driver, b'C', b"SELECT 0\0");
                push(&mut driver, b'Z', b"I");
                let buffered = driver.connection.buffer.clone();
                driver.connection.write_buf.extend_from_slice(b"pending");
                let pending = driver.connection.write_buf.clone();
                let mut callbacks = 0;
                let result =
                    execute_handle_path(&mut driver.connection, &stmt, &[], path, &mut callbacks)
                        .await;
                let frames = written_frames(&peer);
                if result.is_ok() {
                    accepted.push(format!("{path:?}, record={record:?}, frames={frames:?}"));
                    continue;
                }
                assert!(
                    matches!(result, Err(super::super::PgError::Query(ref message)) if message.contains("Statement not prepared")),
                    "{path:?}: {result:?}"
                );
                assert!(frames.is_empty(), "{path:?} wrote frames: {frames:?}");
                assert_eq!(driver.connection.buffer, buffered);
                assert_eq!(driver.connection.write_buf, pending);
                assert_eq!(callbacks, 0);
                assert!(!driver.connection.is_io_desynced());
            }
        }
        assert!(
            accepted.is_empty(),
            "unverified handles executed: {accepted:#?}"
        );
    }

    #[tokio::test]
    async fn prepared_handle_identity_valid_paths_keep_one_execution_exchange() {
        for path in HANDLE_PATHS {
            let (mut driver, peer) = peer_driver();
            let stmt = PreparedStatement::from_sql("SELECT 1");
            driver
                .connection
                .prepared_statements
                .insert(stmt.name.clone(), "SELECT 1".into());
            push(&mut driver, b'2', &[]);
            push(&mut driver, b'C', b"SELECT 0\0");
            push(&mut driver, b'Z', b"I");
            execute_handle_path(&mut driver.connection, &stmt, &[], path, &mut 0)
                .await
                .unwrap();
            let frames = written_frames(&peer);
            assert_eq!(
                frames.iter().map(|f| f.0).collect::<Vec<_>>(),
                b"BES",
                "{path:?}"
            );
            assert_eq!(frame_stmt_name(&frames[0]), stmt.name());
        }
    }

    #[tokio::test]
    async fn prepared_handle_identity_ast_recovery_binds_the_reprepared_name() {
        let (mut driver, peer) = peer_driver();
        let sql = "SELECT 1";
        let root = raw_stmt_name(sql);
        let mut stmt = PreparedStatement::from_sql(sql);
        stmt.name = format!("{root}_1");
        driver
            .connection
            .prepared_statements
            .insert(root.clone(), "SELECT 2".into());
        driver
            .connection
            .prepared_statements
            .insert(stmt.name.clone(), "SELECT 3".into());
        let prepared = super::super::prepared::PreparedAstQuery {
            stmt,
            params: Vec::new(),
            sql: sql.into(),
            sql_hash: sql_bytes_hash(sql.as_bytes()),
        };
        driver.connection.column_info_cache.insert(
            prepared.sql_hash,
            std::sync::Arc::new(super::super::ColumnInfo::from_fields(&[])),
        );
        push(&mut driver, b'1', &[]);
        push(&mut driver, b'Z', b"I");
        push_hit_reply(&mut driver);
        let result = driver.fetch_all_prepared_ast(&prepared).await;
        let frames = written_frames(&peer);
        assert_eq!(
            frames.iter().map(|f| f.0).collect::<Vec<_>>(),
            b"PSBES",
            "{frames:?}"
        );
        assert_eq!(frame_stmt_name(&frames[0]), format!("{root}_2"));
        assert_eq!(frame_stmt_name(&frames[2]), frame_stmt_name(&frames[0]));
        assert_eq!(text_cells(&result.unwrap()), owner_rows());
        assert!(
            !driver
                .connection
                .column_info_cache
                .contains_key(&prepared.sql_hash)
        );

        push_hit_reply(&mut driver);
        driver.fetch_all_prepared_ast(&prepared).await.unwrap();
        let frames = written_frames(&peer);
        assert_eq!(frames.iter().map(|f| f.0).collect::<Vec<_>>(), b"BES");
        assert_eq!(frame_stmt_name(&frames[0]), format!("{root}_2"));
    }

    #[tokio::test]
    async fn stmt_identity_cached_hit_with_forged_key_reparses_own_sql() {
        let (mut driver, peer) = peer_driver();
        let victim = victim_cmd("t");
        let other_sql = "SELECT label FROM t WHERE id = $1";

        driver
            .connection
            .prepared_statements
            .insert("qail_owner".to_string(), other_sql.to_string());
        driver
            .connection
            .stmt_cache
            .put(cmd_hash(&victim), "qail_owner".to_string());

        push_miss_reply(&mut driver);
        let result = driver.fetch_all_cached(&victim).await;
        let frames = written_frames(&peer);
        let types: Vec<u8> = frames.iter().map(|f| f.0).collect();

        assert!(
            frames
                .iter()
                .filter(|f| f.0 == b'B')
                .all(|f| frame_stmt_name(f) != "qail_owner"),
            "victim SQL was bound to the statement parsed from other SQL; wire {:?}",
            String::from_utf8_lossy(&types)
        );
        let parse = frames
            .iter()
            .find(|f| f.0 == b'P')
            .expect("victim SQL must be parsed");
        assert_eq!(cstr_at(&parse.1, 1), encoded_sql(&victim));
        result.expect("collision path must succeed");
        assert_eq!(
            driver
                .connection
                .prepared_statements
                .get("qail_owner")
                .map(String::as_str),
            Some(other_sql),
            "owner statement record must be untouched"
        );
    }

    /// Non-colliding SQL: Parse + Describe once, then Bind + Execute only.
    #[tokio::test]
    async fn stmt_identity_cached_miss_then_hit_keeps_parse_count() {
        let (mut driver, peer) = peer_driver();
        let cmd = owner_cmd("t");

        push_miss_reply(&mut driver);
        driver.fetch_all_cached(&cmd).await.expect("miss");
        let first: Vec<u8> = written_frames(&peer).iter().map(|f| f.0).collect();

        push_hit_reply(&mut driver);
        let rows = driver.fetch_all_cached(&cmd).await.expect("hit");
        let second: Vec<u8> = written_frames(&peer).iter().map(|f| f.0).collect();

        assert_eq!(first, b"PDBES".to_vec());
        assert_eq!(second, b"BES".to_vec());
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0].column_info.is_some(),
            "hit must reuse the cached column metadata"
        );
        assert_eq!(driver.connection.stmt_cache.len(), 1);
        assert_eq!(driver.connection.prepared_statements.len(), 1);
    }

    fn ast_name(hash: u64) -> String {
        crate::driver::prepared::ast_stmt_name_from_hash(hash)
    }

    #[tokio::test]
    async fn stmt_identity_resolve_branches() {
        use crate::driver::connection::{RecordedName, StatementSlot};

        let (mut driver, _peer) = peer_driver();
        let conn = &mut driver.connection;
        let (sql, other) = (b"SELECT 1".as_slice(), "SELECT 2");

        // Key cached for other SQL: unnamed, nothing touched.
        conn.stmt_cache.put(7, "qail_owner".to_string());
        conn.prepared_statements
            .insert("qail_owner".to_string(), other.to_string());
        assert_eq!(
            conn.resolve_cached_statement(7, sql, ast_name, RecordedName::Reparse),
            StatementSlot::Unnamed
        );
        assert_eq!(conn.stmt_cache.peek(&7), Some("qail_owner"));
        assert!(conn.pending_statement_closes.is_empty());

        // Derived name recorded for other SQL (key not cached): unnamed.
        conn.prepared_statements
            .insert(ast_name(8), other.to_string());
        assert_eq!(
            conn.resolve_cached_statement(8, sql, ast_name, RecordedName::Reuse),
            StatementSlot::Unnamed
        );
        assert!(!conn.stmt_cache.contains(&8));

        // Cached entry without a record: dropped and closed, then parsed.
        conn.stmt_cache.put(9, "qail_stale".to_string());
        assert_eq!(
            conn.resolve_cached_statement(9, sql, ast_name, RecordedName::Reparse),
            StatementSlot::Parse(ast_name(9))
        );
        assert!(!conn.stmt_cache.contains(&9));
        assert_eq!(
            conn.pending_statement_closes,
            vec!["qail_stale".to_string()]
        );
        conn.pending_statement_closes.clear();

        // Recorded for the same SQL but dropped from the cache.
        conn.prepared_statements
            .insert(ast_name(10), "SELECT 1".to_string());
        assert_eq!(
            conn.resolve_cached_statement(10, sql, ast_name, RecordedName::Reuse),
            StatementSlot::Reuse(ast_name(10))
        );
        assert_eq!(conn.stmt_cache.peek(&10), Some(ast_name(10).as_str()));
        conn.stmt_cache.remove(&10);
        assert_eq!(
            conn.resolve_cached_statement(10, sql, ast_name, RecordedName::Reparse),
            StatementSlot::Parse(ast_name(10))
        );
        assert!(!conn.prepared_statements.contains_key(&ast_name(10)));
        assert_eq!(conn.pending_statement_closes, vec![ast_name(10)]);
    }

    fn bench_cmd() -> Qail {
        Qail::get("harbors")
            .columns(["id", "name", "region", "country_code", "created_at"])
            .eq("region", "bali")
            .eq("active", true)
            .limit(50)
    }

    /// Cache-hit encode + lookup, unverified (the code before this check)
    /// against verified, interleaved in one binary.
    #[tokio::test]
    #[ignore = "timing; run in release"]
    async fn stmt_identity_bench_cache_hit_lookup() {
        use crate::driver::connection::{RecordedName, StatementSlot};
        const ROUNDS: usize = 9;
        const ITERS: usize = 1_000_000;

        let (mut driver, _peer) = peer_driver();
        let conn = &mut driver.connection;
        let cmd = bench_cmd();
        let sql = encoded_sql(&cmd);
        let hash = sql_bytes_hash(sql.as_bytes());
        // Fill the cache to its 100-entry capacity; LRU touch cost scales with it.
        for i in 0..99u64 {
            let name = format!("qail_fill_{i}");
            conn.stmt_cache.put(i, name.clone());
            conn.prepared_statements.insert(name, format!("SELECT {i}"));
        }
        conn.stmt_cache.put(hash, ast_name(hash));
        conn.prepared_statements.insert(ast_name(hash), sql.clone());
        println!("SQL {} bytes: {sql}", sql.len());

        let mut runs: [Vec<f64>; 4] = Default::default();
        for _ in 0..ROUNDS {
            // 0: encode + hash + unverified lookup
            let start = std::time::Instant::now();
            for _ in 0..ITERS {
                AstEncoder::encode_cacheable_cmd_sql_to(
                    &cmd,
                    &mut conn.sql_buf,
                    &mut conn.params_buf,
                )
                .unwrap();
                let h = sql_bytes_hash(&conn.sql_buf);
                let miss = !conn.stmt_cache.contains(&h);
                std::hint::black_box((miss, conn.stmt_cache.get_unverified(&h)));
            }
            runs[0].push(start.elapsed().as_nanos() as f64 / ITERS as f64);

            // 1: encode + hash + verified lookup
            let start = std::time::Instant::now();
            for _ in 0..ITERS {
                AstEncoder::encode_cacheable_cmd_sql_to(
                    &cmd,
                    &mut conn.sql_buf,
                    &mut conn.params_buf,
                )
                .unwrap();
                let h = sql_bytes_hash(&conn.sql_buf);
                let slot =
                    conn.resolve_cached_statement_for_sql_buf(h, ast_name, RecordedName::Reparse);
                assert!(matches!(slot, StatementSlot::Reuse(_)));
                std::hint::black_box(slot);
            }
            runs[1].push(start.elapsed().as_nanos() as f64 / ITERS as f64);

            // 2/3: lookup only, SQL already encoded
            let start = std::time::Instant::now();
            for _ in 0..ITERS {
                let h = sql_bytes_hash(&conn.sql_buf);
                let miss = !conn.stmt_cache.contains(&h);
                std::hint::black_box((miss, conn.stmt_cache.get_unverified(&h)));
            }
            runs[2].push(start.elapsed().as_nanos() as f64 / ITERS as f64);

            let start = std::time::Instant::now();
            for _ in 0..ITERS {
                let h = sql_bytes_hash(&conn.sql_buf);
                std::hint::black_box(conn.resolve_cached_statement_for_sql_buf(
                    h,
                    ast_name,
                    RecordedName::Reparse,
                ));
            }
            runs[3].push(start.elapsed().as_nanos() as f64 / ITERS as f64);
        }
        let median = |v: &mut Vec<f64>| {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            v[v.len() / 2]
        };
        let [a, b, c, d] = &mut runs;
        let (a, b, c, d) = (median(a), median(b), median(c), median(d));
        println!(
            "encode+hash+lookup: unverified {a:.1} ns, verified {b:.1} ns ({:+.1}%)",
            (b - a) / a * 100.0
        );
        println!(
            "hash+lookup only:   unverified {c:.1} ns, verified {d:.1} ns ({:+.1}%)",
            (d - c) / c * 100.0
        );
    }

    /// Client-side cost of a `fetch_all_cached` hit: AST encode, cache lookup,
    /// Bind/Execute/Sync write, reply decode. No network.
    #[tokio::test]
    #[ignore = "timing; run in release"]
    async fn stmt_identity_bench_fetch_all_cached_hit() {
        const ROUNDS: usize = 7;
        const ITERS: usize = 100_000;
        let (mut driver, peer) = peer_driver();
        let cmd = bench_cmd();

        push_miss_reply(&mut driver);
        driver.fetch_all_cached(&cmd).await.expect("miss");
        written_frames(&peer);

        let mut per_op = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            let start = std::time::Instant::now();
            for i in 0..ITERS {
                push_hit_reply(&mut driver);
                let rows = driver.fetch_all_cached(&cmd).await.expect("hit");
                std::hint::black_box(rows);
                if i % 64 == 0 {
                    written_frames(&peer);
                }
            }
            per_op.push(start.elapsed().as_nanos() as f64 / ITERS as f64);
            written_frames(&peer);
        }
        per_op.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "fetch_all_cached hit (mock peer): median {:.1} ns/op, min {:.1}, max {:.1} over {ROUNDS}x{ITERS}",
            per_op[ROUNDS / 2],
            per_op[0],
            per_op[ROUNDS - 1]
        );
    }
}

// ── Live: PostgreSQL via QAIL_TEST_DB_URL ──────────────────────────────

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn stmt_identity_live_driver_fetch_all_cached_forged_collision() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = temp_table(&mut driver.connection).await?;
    let (owner, victim) = (owner_cmd(&table), victim_cmd(&table));

    assert_eq!(
        text_cells(&driver.fetch_all_cached(&owner).await?),
        owner_rows()
    );
    let name = forge_cache_collision(&mut driver.connection, &owner, &victim);
    println!("forged key {:016x} -> {name}", cmd_hash(&victim));

    let rows = text_cells(&driver.fetch_all_cached(&victim).await?);
    println!("victim via fetch_all_cached: {rows:?}");
    assert_eq!(rows, victim_rows());
    assert_eq!(
        text_cells(&driver.fetch_all_cached(&owner).await?),
        owner_rows()
    );
    assert_eq!(
        text_cells(&driver.fetch_all_cached(&victim).await?),
        victim_rows()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn stmt_identity_live_driver_fetch_all_cached_server_name_taken() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = temp_table(&mut driver.connection).await?;
    let (owner, victim) = (owner_cmd(&table), victim_cmd(&table));

    // The server holds the victim's hash-derived name with owner SQL; the
    // local cache does not know (cleared after a replan error, or a collision).
    let name = ast_stmt_name(cmd_hash(&victim));
    parse_on_server(&mut driver.connection, &name, &encoded_sql(&owner)).await;

    let rows = text_cells(&driver.fetch_all_cached(&victim).await?);
    println!("victim with {name} taken on server: {rows:?}");
    assert_eq!(rows, victim_rows());
    assert_eq!(
        text_cells(&driver.fetch_all_cached(&victim).await?),
        victim_rows()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn stmt_identity_live_query_cached_forged_collision() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = temp_table(&mut driver.connection).await?;
    let owner_sql = encoded_sql(&owner_cmd(&table));
    let victim_sql = encoded_sql(&victim_cmd(&table));
    let params = [Some(b"2".to_vec())];

    // Recorded collision: the victim's name is known locally, for owner SQL.
    let name = raw_stmt_name(&victim_sql);
    parse_on_server(&mut driver.connection, &name, &owner_sql).await;
    driver
        .connection
        .prepared_statements
        .insert(name.clone(), owner_sql.clone());
    let rows = raw_cells(&driver.connection.query_cached(&victim_sql, &params).await?);
    println!("query_cached victim, recorded collision: {rows:?}");
    assert_eq!(rows, victim_rows());
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn stmt_identity_live_query_cached_server_name_taken() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = temp_table(&mut driver.connection).await?;
    let owner_sql = encoded_sql(&owner_cmd(&table));
    let victim_sql = encoded_sql(&victim_cmd(&table));
    let params = [Some(b"2".to_vec())];

    // Unrecorded: only the server holds the name.
    let name = raw_stmt_name(&victim_sql);
    parse_on_server(&mut driver.connection, &name, &owner_sql).await;
    let rows = raw_cells(&driver.connection.query_cached(&victim_sql, &params).await?);
    println!("query_cached victim, server-only name: {rows:?}");
    assert_eq!(rows, victim_rows());
    assert_eq!(
        raw_cells(&driver.connection.query_cached(&victim_sql, &params).await?),
        victim_rows()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn stmt_identity_live_prepare_and_handles_forged_collision() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = temp_table(&mut driver.connection).await?;
    let owner_sql = encoded_sql(&owner_cmd(&table));
    let victim_sql = encoded_sql(&victim_cmd(&table));
    let params = vec![Some(b"2".to_vec())];

    let name = raw_stmt_name(&victim_sql);
    parse_on_server(&mut driver.connection, &name, &owner_sql).await;
    driver
        .connection
        .prepared_statements
        .insert(name.clone(), owner_sql.clone());

    let stmt = driver.connection.prepare(&victim_sql).await?;
    println!("prepare(victim) with {name} taken -> {}", stmt.name());
    let rows = raw_cells(
        &driver
            .connection
            .query_prepared_single(&stmt, &params)
            .await?,
    );
    assert_eq!(rows, victim_rows());

    // A handle built from SQL text must not run the statement recorded for
    // other SQL under the same name.
    let by_text = PreparedStatement::from_sql(&victim_sql);
    let outcome = driver
        .connection
        .pipeline_execute_prepared_rows(&by_text, std::slice::from_ref(&params))
        .await;
    println!("pipeline_execute_prepared_rows(from_sql(victim)): {outcome:?}");
    match outcome {
        Ok(batches) => {
            for batch in &batches {
                assert_eq!(raw_cells(batch), victim_rows());
            }
        }
        Err(err) => assert!(err.to_string().contains("Statement not prepared")),
    }
    // The handle prepare() returned passes the same check.
    let batches = driver
        .connection
        .pipeline_execute_prepared_rows(&stmt, std::slice::from_ref(&params))
        .await?;
    assert_eq!(raw_cells(&batches[0]), victim_rows());
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn prepared_handle_identity_live_rejects_reused_name_without_writes() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = temp_table(&mut driver.connection).await?;
    let sql = format!("SELECT id, label, id, label FROM {table} WHERE id = $1");
    let stmt = driver.connection.prepare(&sql).await?;
    let replacement =
        format!("UPDATE {table} SET label = 'wrong' WHERE id = $1 RETURNING id, label, id, label");
    driver
        .connection
        .execute_simple(&format!("DEALLOCATE {}", stmt.name()))
        .await?;
    driver.connection.prepared_statements.remove(stmt.name());
    parse_on_server(&mut driver.connection, stmt.name(), &replacement).await;

    let mut accepted = Vec::new();
    for recorded in [false, true] {
        for path in HANDLE_PATHS {
            if recorded {
                driver
                    .connection
                    .prepared_statements
                    .insert(stmt.name.clone(), replacement.clone());
            } else {
                driver.connection.prepared_statements.remove(stmt.name());
            }
            let mut callbacks = 0;
            let result = execute_handle_path(
                &mut driver.connection,
                &stmt,
                &[Some(b"2".to_vec())],
                path,
                &mut callbacks,
            )
            .await;
            println!(
                "{path:?}, recorded={recorded}: rejected={}",
                result.is_err()
            );
            if result.is_ok() {
                accepted.push(format!("{path:?}, recorded={recorded}"));
            } else {
                assert!(
                    matches!(result, Err(super::PgError::Query(ref message)) if message.contains("Statement not prepared")),
                    "{result:?}"
                );
                assert_eq!(callbacks, 0);
            }
        }
    }
    let rows = driver
        .simple_query(&format!("SELECT label FROM {table} WHERE id = 2"))
        .await?;
    println!(
        "fixture after reused-name attempts: {:?}",
        text_cells(&rows)
    );
    assert!(
        accepted.is_empty(),
        "unverified handles executed: {accepted:?}"
    );
    assert_eq!(text_cells(&rows), owner_rows());
    let active = driver.connection.prepare(&sql).await?;
    assert_ne!(active.name(), stmt.name());
    for path in HANDLE_PATHS {
        let mut callbacks = 0;
        execute_handle_path(
            &mut driver.connection,
            &active,
            &[Some(b"2".to_vec())],
            path,
            &mut callbacks,
        )
        .await?;
        if matches!(
            path,
            HandlePath::VisitRows
                | HandlePath::VisitBytes
                | HandlePath::VisitFirst
                | HandlePath::VisitFour
        ) {
            assert_eq!(callbacks, 1);
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn prepared_handle_identity_live_ast_recovers_disambiguated_name() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = temp_table(&mut driver.connection).await?;
    let cmd = Qail::get(&table).columns(["label", "label"]).eq("id", 2);
    let sql = encoded_sql(&cmd);
    let root = raw_stmt_name(&sql);
    let occupied = encoded_sql(&owner_cmd(&table));
    parse_on_server(&mut driver.connection, &root, &occupied).await;
    driver
        .connection
        .prepared_statements
        .insert(root.clone(), occupied.clone());
    let prepared = driver.prepare_ast_query(&cmd).await?;
    assert_eq!(prepared.statement_name(), format!("{root}_1"));
    let expected = vec![vec!["second".to_string(), "second".to_string()]];
    assert_eq!(
        text_cells(&driver.fetch_all_prepared_ast(&prepared).await?),
        expected
    );

    driver
        .connection
        .execute_simple(&format!("DEALLOCATE {}", prepared.statement_name()))
        .await?;
    driver
        .connection
        .prepared_statements
        .remove(prepared.statement_name());
    driver.connection.stmt_cache.remove(&prepared.sql_hash);
    parse_on_server(&mut driver.connection, prepared.statement_name(), &occupied).await;
    driver
        .connection
        .prepared_statements
        .insert(prepared.statement_name().to_string(), occupied);
    for format in [super::ResultFormat::Text, super::ResultFormat::Binary] {
        let rows = driver
            .fetch_all_prepared_ast_with_format(&prepared, format)
            .await?;
        println!("AST after name reuse: {:?}", text_cells(&rows));
        assert_eq!(text_cells(&rows), expected);
    }

    driver.connection.execute_simple("DEALLOCATE ALL").await?;
    let rows = driver.fetch_all_prepared_ast(&prepared).await?;
    println!("AST after server deallocation: {:?}", text_cells(&rows));
    assert_eq!(text_cells(&rows), expected);
    driver.connection.clear_prepared_statement_state();
    assert_eq!(
        text_cells(&driver.fetch_all_prepared_ast(&prepared).await?),
        expected
    );
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn prepared_handle_identity_live_eviction_and_transaction_boundaries() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = temp_table(&mut driver.connection).await?;
    let sql = format!("SELECT id, label, id, label FROM {table} WHERE id = $1");
    let stmt = driver.connection.prepare(&sql).await?;
    driver
        .connection
        .stmt_cache
        .put(sql_bytes_hash(sql.as_bytes()), stmt.name().to_string());
    for value in 0..PgConnection::MAX_PREPARED_PER_CONN {
        driver
            .connection
            .prepare(&format!("SELECT {value}"))
            .await?;
    }
    assert!(!driver.connection.records_prepared_statement(&stmt));
    driver.connection.execute_simple("BEGIN").await?;
    for path in HANDLE_PATHS {
        let mut callbacks = 0;
        let err = execute_handle_path(
            &mut driver.connection,
            &stmt,
            &[Some(b"2".to_vec())],
            path,
            &mut callbacks,
        )
        .await
        .expect_err("evicted handle must fail locally");
        assert!(matches!(err, super::PgError::Query(_)), "{err}");
        assert_eq!(callbacks, 0);
    }
    assert_eq!(
        text_cells(
            &driver
                .simple_query(&format!("SELECT label FROM {table} WHERE id = 2"))
                .await?
        ),
        owner_rows()
    );
    driver.connection.execute_simple("COMMIT").await?;
    let stmt = driver.connection.prepare(&sql).await?;
    driver
        .connection
        .query_prepared_single(&stmt, &[Some(b"2".to_vec())])
        .await?;

    let cmd = Qail::get(&table).columns(["label", "label"]).eq("id", 2);
    let prepared = driver.prepare_ast_query(&cmd).await?;
    for value in 1000..1000 + PgConnection::MAX_PREPARED_PER_CONN {
        driver
            .connection
            .prepare(&format!("SELECT {value}"))
            .await?;
    }
    assert!(!driver.connection.records_prepared_statement(&prepared.stmt));
    driver.connection.execute_simple("BEGIN").await?;
    let expected = vec![vec!["second".to_string(), "second".to_string()]];
    assert_eq!(
        text_cells(&driver.fetch_all_prepared_ast(&prepared).await?),
        expected
    );
    driver.connection.execute_simple("COMMIT").await?;
    println!("capacity eviction recovered; local rejection preserved transaction");

    driver.connection.execute_simple("BEGIN").await?;
    driver
        .connection
        .execute_simple(&format!(
            "UPDATE {table} SET label = 'pending' WHERE id = 2"
        ))
        .await?;
    driver.connection.execute_simple("DEALLOCATE ALL").await?;
    let err = driver
        .fetch_all_prepared_ast(&prepared)
        .await
        .map(|_| ())
        .expect_err("server error in a transaction requires caller rollback");
    assert_eq!(err.sqlstate(), Some("25P02"), "{err}");
    driver.connection.execute_simple("ROLLBACK").await?;
    assert_eq!(
        text_cells(&driver.fetch_all_prepared_ast(&prepared).await?),
        expected
    );
    println!("server error stayed aborted until rollback; TEMP update rolled back");
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn stmt_identity_live_pipeline_cached_forged_collision() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = temp_table(&mut driver.connection).await?;
    let owner = owner_cmd(&table);
    driver.fetch_all_cached(&owner).await?;

    // INSERT with one parameter, the same parameter shape as the owner SELECT.
    let inserts: Vec<Qail> = (10..18)
        .map(|id| Qail::add(&table).columns(["id"]).values([Value::Int(id)]))
        .collect();
    let owner_name = driver
        .connection
        .stmt_cache
        .peek(&cmd_hash(&owner))
        .expect("owner cached")
        .to_string();
    driver
        .connection
        .stmt_cache
        .put(cmd_hash(&inserts[0]), owner_name);

    let completed = driver
        .connection
        .pipeline_execute_count_ast_cached(&inserts)
        .await?;
    let inserted = driver
        .connection
        .simple_query(&format!("SELECT id FROM {table} WHERE id >= 10"))
        .await?
        .len();
    println!("pipeline cached: completed={completed} inserted={inserted}");
    assert_eq!(completed, inserts.len());
    assert_eq!(inserted, inserts.len());
    Ok(())
}

async fn live_pool() -> PgResult<PgPool> {
    PgPool::connect(
        PoolConfig::from_url(&database_url())?
            .min_connections(0)
            .max_connections(1),
    )
    .await
}

const RLS_SQL: &str = "BEGIN; SELECT set_config('app.qail_stmt_identity', 'on', true)";

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn stmt_identity_live_pool_forged_collision() -> PgResult<()> {
    let pool = live_pool().await?;
    let mut conn = pool.acquire_raw().await?;
    let table = temp_table(conn.get_mut()?).await?;
    let (owner, victim) = (owner_cmd(&table), victim_cmd(&table));

    assert_eq!(
        text_cells(&conn.fetch_all_cached(&owner).await?),
        owner_rows()
    );
    forge_cache_collision(conn.get_mut()?, &owner, &victim);

    let rows = text_cells(&conn.fetch_all_cached(&victim).await?);
    println!("pool fetch_all_cached victim: {rows:?}");
    assert_eq!(rows, victim_rows());
    assert_eq!(
        text_cells(&conn.fetch_all_cached(&owner).await?),
        owner_rows()
    );
    conn.release_checked().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn stmt_identity_live_pool_rls_forged_collision() -> PgResult<()> {
    let pool = live_pool().await?;
    let mut conn = pool.acquire_raw().await?;
    let table = temp_table(conn.get_mut()?).await?;
    let (owner, victim) = (owner_cmd(&table), victim_cmd(&table));

    assert_eq!(
        text_cells(&conn.fetch_all_cached(&owner).await?),
        owner_rows()
    );
    forge_cache_collision(conn.get_mut()?, &owner, &victim);

    let rows = text_cells(&conn.fetch_all_with_rls(&victim, RLS_SQL).await?);
    println!("pool fetch_all_with_rls victim: {rows:?}");
    assert_eq!(rows, victim_rows());
    conn.release_checked().await?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL"]
async fn stmt_identity_live_pool_rls_server_name_taken() -> PgResult<()> {
    let pool = live_pool().await?;
    let mut conn = pool.acquire_raw().await?;
    let table = temp_table(conn.get_mut()?).await?;
    let (owner, victim) = (owner_cmd(&table), victim_cmd(&table));

    // 42P05 lands inside the BEGIN the RLS pipeline opened.
    let name = ast_stmt_name(cmd_hash(&victim));
    parse_on_server(conn.get_mut()?, &name, &encoded_sql(&owner)).await;
    let rows = text_cells(&conn.fetch_all_with_rls(&victim, RLS_SQL).await?);
    println!("pool fetch_all_with_rls with {name} taken on server: {rows:?}");
    assert_eq!(rows, victim_rows());
    assert_eq!(
        text_cells(&conn.fetch_all_with_rls(&victim, RLS_SQL).await?),
        victim_rows()
    );
    conn.release_checked().await?;
    pool.close().await;
    Ok(())
}
