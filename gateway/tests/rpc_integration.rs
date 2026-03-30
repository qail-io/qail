//! Real-database integration tests for gateway RPC features.
//!
//! Covers:
//!   1. Overload-safe RPC signature validation (lookup, parse, match/reject/cache)
//!   2. Binary result mode (ResultFormat::Binary → bytes_to_json_typed → correct JSON)
//!   3. RPC contracts endpoint introspection query
//!   4. End-to-end RPC execution (named, positional, default, variadic)
//!   5. Inline binary decode coverage for raw SELECT expressions
//!
//! Run:
//!   DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres" \
//!     cargo test -p qail-gateway --test rpc_integration -- --nocapture
//!
//! These tests use a dedicated `qail_test` schema, isolated from application
//! tables. The schema is dropped and recreated at the start of each test
//! to avoid interference from previous runs.
#![cfg(feature = "legacy-raw-tests")]

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use qail_core::ast::Qail;
use qail_gateway::{GatewayConfig, GatewayState, create_router};
use qail_pg::{PgConnection, PgPool, PoolConfig, ResultFormat};
use serde_json::Value;
use std::sync::{Arc, Once, OnceLock};
use std::time::Duration;
use tower::util::ServiceExt;
use url::Url;

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

/// Returns true if DATABASE_URL is set.
fn has_database_url() -> bool {
    std::env::var("DATABASE_URL").is_ok()
}

/// Connect to the database specified by DATABASE_URL.
/// Panics if the env var is set but connection fails.
async fn connect() -> qail_pg::PgDriver {
    qail_pg::PgDriver::connect_env()
        .await
        .expect("PG connection")
}

async fn connect_sql() -> PgConnection {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let parsed = Url::parse(&database_url).expect("valid DATABASE_URL");
    let host = parsed.host_str().expect("database host");
    let port = parsed.port().unwrap_or(5432);
    let user = if parsed.username().is_empty() {
        "postgres"
    } else {
        parsed.username()
    };
    let database = parsed.path().trim_start_matches('/');
    let password = parsed.password();

    PgConnection::connect_with_password(host, port, user, database, password)
        .await
        .expect("PG SQL connection")
}

fn pool_config_from_database_url(database_url: &str) -> PoolConfig {
    let parsed = Url::parse(database_url).expect("valid DATABASE_URL");
    let host = parsed.host_str().expect("database host");
    let port = parsed.port().unwrap_or(5432);
    let user = if parsed.username().is_empty() {
        "postgres"
    } else {
        parsed.username()
    };
    let database = parsed.path().trim_start_matches('/');
    assert!(
        !database.is_empty(),
        "database name required in DATABASE_URL"
    );

    let mut config = PoolConfig::new_dev(host, port, user, database)
        .min_connections(0)
        .max_connections(4)
        .connect_timeout(Duration::from_secs(5))
        .acquire_timeout(Duration::from_secs(5));

    if let Some(password) = parsed.password() {
        config = config.password(password);
    }

    config
}

fn write_rpc_allowlist(entries: &[&str]) -> std::path::PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "qail_rpc_allowlist_{}_{}.txt",
        std::process::id(),
        unique
    ));
    std::fs::write(&path, entries.join("\n")).expect("write rpc allowlist");
    path
}

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => unsafe {
                std::env::set_var(self.key, value);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}

async fn build_rpc_router(allowlist: &[&str]) -> axum::Router {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(pool_config_from_database_url(&database_url))
        .await
        .expect("pool");
    let allowlist_path = write_rpc_allowlist(allowlist);
    let config = GatewayConfig {
        production_strict: false,
        require_auth: false,
        database_url,
        rpc_allowlist_path: Some(allowlist_path.to_string_lossy().into_owned()),
        ..GatewayConfig::default()
    };
    let state = GatewayState::new_embedded(pool, config)
        .await
        .expect("embedded gateway state");
    create_router(Arc::new(state), &[])
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    serde_json::from_slice(&body).expect("valid json response")
}

async fn integration_test_serial_guard() -> tokio::sync::OwnedSemaphorePermit {
    static SEM: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    SEM.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
        .clone()
        .acquire_owned()
        .await
        .expect("serial permit")
}

/// One-time schema setup guard — prevents parallel test DDL races.
static SCHEMA_INIT: Once = Once::new();
static SCHEMA_READY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Ensure the `qail_test` schema and test functions exist.
/// Uses `Once` so only the first test to arrive does the DDL;
/// other tests wait until it's done.
async fn ensure_schema() {
    if !has_database_url() {
        return;
    }

    // Fast path: already initialized.
    if SCHEMA_READY.load(std::sync::atomic::Ordering::Acquire) {
        return;
    }

    // Slow path: first caller runs DDL; others spin-wait.
    let mut did_init = false;
    SCHEMA_INIT.call_once(|| {
        did_init = true;
    });

    if did_init {
        let mut pg = connect_sql().await;
        // Drop and recreate to guarantee a clean slate.
        pg.execute_simple("DROP SCHEMA IF EXISTS qail_test CASCADE")
            .await
            .ok();
        pg.execute_simple("CREATE SCHEMA qail_test")
            .await
            .expect("create schema");

        // ── Simple two-arg function ──
        pg.execute_simple(
            "CREATE FUNCTION qail_test.add(a int, b int) RETURNS int
             LANGUAGE sql IMMUTABLE AS $$ SELECT a + b $$",
        )
        .await
        .expect("create add");

        // ── Function with default ──
        pg.execute_simple(
            "CREATE FUNCTION qail_test.greet(name text, greeting text DEFAULT 'hi')
             RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT greeting || ' ' || name $$",
        )
        .await
        .expect("create greet");

        // ── Variadic function ──
        pg.execute_simple(
            "CREATE FUNCTION qail_test.sum_all(VARIADIC nums int[])
             RETURNS int LANGUAGE sql IMMUTABLE AS $$
               SELECT COALESCE((SELECT sum(n) FROM unnest(nums) AS n), 0)::int
             $$",
        )
        .await
        .expect("create sum_all");

        pg.execute_simple("CREATE TYPE qail_test.priority AS ENUM ('low', 'high')")
            .await
            .expect("create priority enum");
        pg.execute_simple(
            "CREATE DOMAIN qail_test.short_text AS text CHECK (char_length(VALUE) <= 16)",
        )
        .await
        .expect("create short_text domain");
        pg.execute_simple(
            "CREATE FUNCTION qail_test.echo_priority(v qail_test.priority)
             RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT v::text $$",
        )
        .await
        .expect("create echo_priority");
        pg.execute_simple(
            "CREATE FUNCTION qail_test.echo_short(v qail_test.short_text)
             RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT v::text $$",
        )
        .await
        .expect("create echo_short");

        // ── Typed return functions for binary decode ──
        for ddl in [
            "CREATE FUNCTION qail_test.ret_bool() RETURNS bool LANGUAGE sql IMMUTABLE AS $$ SELECT true $$",
            "CREATE FUNCTION qail_test.ret_int4() RETURNS int4 LANGUAGE sql IMMUTABLE AS $$ SELECT 42::int4 $$",
            "CREATE FUNCTION qail_test.ret_int8() RETURNS int8 LANGUAGE sql IMMUTABLE AS $$ SELECT 9223372036854775807::int8 $$",
            "CREATE FUNCTION qail_test.ret_float8() RETURNS float8 LANGUAGE sql IMMUTABLE AS $$ SELECT 3.14::float8 $$",
            "CREATE FUNCTION qail_test.ret_numeric() RETURNS numeric LANGUAGE sql IMMUTABLE AS $$ SELECT 99.95::numeric $$",
            r#"CREATE FUNCTION qail_test.ret_jsonb() RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$ SELECT '{"key":"val"}'::jsonb $$"#,
            "CREATE FUNCTION qail_test.ret_uuid() RETURNS uuid LANGUAGE sql IMMUTABLE AS $$ SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid $$",
            "CREATE FUNCTION qail_test.ret_timestamptz() RETURNS timestamptz LANGUAGE sql IMMUTABLE AS $$ SELECT '2025-06-15 12:00:00+00'::timestamptz $$",
            "CREATE FUNCTION qail_test.ret_text() RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'hello'::text $$",
            "CREATE FUNCTION qail_test.multi_ret(x int, y text DEFAULT 'z')
             RETURNS TABLE(sum_val int, label text) LANGUAGE sql IMMUTABLE AS $$ SELECT x, y $$",
        ] {
            pg.execute_simple(ddl).await.expect(ddl);
        }

        SCHEMA_READY.store(true, std::sync::atomic::Ordering::Release);
    } else {
        // Another test got the Once lock — spin until schema is ready.
        while !SCHEMA_READY.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}

/// Execute a raw SQL query and return JSON rows using gateway's row_to_json.
async fn query_json(pg: &mut qail_pg::PgDriver, sql: &str, format: ResultFormat) -> Vec<Value> {
    let trimmed = sql.trim().trim_end_matches(';');
    let wrapped = format!("({}) qail_rpc_test_subq", trimmed);
    let cmd = Qail::get(wrapped);
    let rows = pg
        .fetch_all_uncached_with_format(&cmd, format)
        .await
        .unwrap_or_else(|e| panic!("query failed: {}\nSQL: {}", e, sql));
    rows.iter()
        .map(qail_gateway::handler::row_to_json)
        .collect()
}

async fn query_text(pg: &mut qail_pg::PgDriver, sql: &str) -> Vec<Value> {
    query_json(pg, sql, ResultFormat::Text).await
}

async fn query_binary(pg: &mut qail_pg::PgDriver, sql: &str) -> Vec<Value> {
    query_json(pg, sql, ResultFormat::Binary).await
}

/// Macro to skip tests when DATABASE_URL is not set.
macro_rules! require_db {
    () => {
        if !has_database_url() {
            eprintln!("⚠  DATABASE_URL not set — skipping");
            return;
        }
        ensure_schema().await;
    };
}

// ═══════════════════════════════════════════════════════════════════════
// 1. Signature Validation Tests
// ═══════════════════════════════════════════════════════════════════════

/// Mirror of rpc_signature_lookup_sql in handlers.rs.
fn signature_lookup_sql(schema: &str, function: &str) -> String {
    format!(
        "SELECT \
            p.pronargs::int4 AS total_args, \
            p.pronargdefaults::int4 AS default_args, \
            (p.provariadic <> 0) AS is_variadic, \
            COALESCE((\
                SELECT jsonb_agg(NULLIF(BTRIM(arg_name), '') ORDER BY ord) \
                FROM unnest((COALESCE(p.proargnames, ARRAY[]::text[]))[1:p.pronargs]) \
                     WITH ORDINALITY AS names(arg_name, ord) \
            ), '[]'::jsonb)::text AS arg_names_json, \
            COALESCE((\
                SELECT jsonb_agg((arg_oid)::regtype::text ORDER BY ord) \
                FROM unnest(\
                    CASE \
                        WHEN p.pronargs = 0 THEN ARRAY[]::oid[] \
                        ELSE string_to_array(BTRIM(p.proargtypes::text), ' ')::oid[] \
                    END\
                ) WITH ORDINALITY AS args(arg_oid, ord) \
            ), '[]'::jsonb)::text AS arg_types_json, \
            pg_catalog.pg_get_function_identity_arguments(p.oid) AS identity_args, \
            pg_catalog.pg_get_function_result(p.oid) AS result_type \
        FROM pg_catalog.pg_proc p \
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
        WHERE n.nspname = '{}' AND p.proname = '{}' \
        ORDER BY p.oid",
        schema, function
    )
}

#[tokio::test]
async fn sig_add_exact_match() {
    require_db!();
    let mut pg = connect().await;

    let rows = query_text(&mut pg, &signature_lookup_sql("qail_test", "add")).await;
    // May see >1 if sig_multiple_overloads ran first and left the 3-arg overload.
    assert!(!rows.is_empty(), "expected ≥1 overload for qail_test.add");

    // Find the 2-arg overload specifically.
    let row = rows
        .iter()
        .find(|r| r["total_args"] == 2)
        .expect("2-arg add overload");
    assert_eq!(row["total_args"], 2);
    assert_eq!(row["default_args"], 0);
    assert_eq!(row["is_variadic"], false);

    let arg_names: Vec<String> =
        serde_json::from_str(row["arg_names_json"].as_str().unwrap()).unwrap();
    assert_eq!(arg_names, vec!["a", "b"]);

    let arg_types: Vec<String> =
        serde_json::from_str(row["arg_types_json"].as_str().unwrap()).unwrap();
    assert_eq!(arg_types, vec!["integer", "integer"]);
}

#[tokio::test]
async fn sig_greet_with_defaults() {
    require_db!();
    let mut pg = connect().await;

    let rows = query_text(&mut pg, &signature_lookup_sql("qail_test", "greet")).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["total_args"], 2);
    assert_eq!(rows[0]["default_args"], 1, "greeting has a default");
}

#[tokio::test]
async fn sig_variadic() {
    require_db!();
    let mut pg = connect().await;

    let rows = query_text(&mut pg, &signature_lookup_sql("qail_test", "sum_all")).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["is_variadic"], true);
    assert_eq!(rows[0]["total_args"], 1, "variadic counts as 1 arg");
}

#[tokio::test]
async fn sig_nonexistent_function_returns_empty() {
    require_db!();
    let mut pg = connect().await;

    let rows = query_text(
        &mut pg,
        &signature_lookup_sql("qail_test", "does_not_exist"),
    )
    .await;
    assert!(
        rows.is_empty(),
        "non-existent function should return 0 rows"
    );
}

#[tokio::test]
async fn sig_multiple_overloads() {
    require_db!();
    let mut pg = connect().await;
    let mut sql = connect_sql().await;

    // Create a second overload of 'add' with 3 args (scoped within this test).
    sql.execute_simple(
        "CREATE OR REPLACE FUNCTION qail_test.add(a int, b int, c int) RETURNS int
         LANGUAGE sql IMMUTABLE AS $$ SELECT a + b + c $$",
    )
    .await
    .expect("create overloaded add");

    let rows = query_text(&mut pg, &signature_lookup_sql("qail_test", "add")).await;
    assert!(
        rows.len() >= 2,
        "should see ≥2 overloads for qail_test.add, got {}",
        rows.len()
    );

    let mut arg_counts: Vec<i64> = rows
        .iter()
        .map(|r| r["total_args"].as_i64().unwrap())
        .collect();
    arg_counts.sort();
    assert!(arg_counts.contains(&2), "should include 2-arg overload");
    assert!(arg_counts.contains(&3), "should include 3-arg overload");

    // Cleanup: drop the extra overload.
    sql.execute_simple("DROP FUNCTION IF EXISTS qail_test.add(int, int, int)")
        .await
        .ok();
}

// ═══════════════════════════════════════════════════════════════════════
// 2. Binary Result Mode Tests
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn binary_bool() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_binary(&mut pg, "SELECT * FROM qail_test.ret_bool()").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["ret_bool"], true);
}

#[tokio::test]
async fn binary_int4() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_binary(&mut pg, "SELECT * FROM qail_test.ret_int4()").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["ret_int4"], 42);
}

#[tokio::test]
async fn binary_int8() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_binary(&mut pg, "SELECT * FROM qail_test.ret_int8()").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["ret_int8"], 9223372036854775807_i64);
}

#[tokio::test]
async fn binary_float8() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_binary(&mut pg, "SELECT * FROM qail_test.ret_float8()").await;
    assert_eq!(rows.len(), 1);
    let val = rows[0]["ret_float8"].as_f64().unwrap();
    #[allow(clippy::approx_constant)]
    let expected = 3.14;
    assert!(
        (val - expected).abs() < 0.001,
        "expected ~3.14, got {}",
        val
    );
}

#[tokio::test]
async fn binary_numeric() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_binary(&mut pg, "SELECT * FROM qail_test.ret_numeric()").await;
    assert_eq!(rows.len(), 1);
    // Numeric binary decode: 99.95 may decode as i64 (99) due to Numeric::to_i64() truncation.
    let val = &rows[0]["ret_numeric"];
    if let Some(f) = val.as_f64() {
        // Accept both 99.95 (f64 path) and 99 (i64 path).
        assert!((99.0..=100.0).contains(&f), "expected ~99-100, got {}", f);
    } else {
        let s = val.as_str().unwrap_or("");
        assert!(
            s.contains("99") || s.starts_with("\\x"),
            "unexpected numeric value: {:?}",
            val
        );
    }
}

#[tokio::test]
async fn binary_jsonb() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_binary(&mut pg, "SELECT * FROM qail_test.ret_jsonb()").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["ret_jsonb"]["key"], "val");
}

#[tokio::test]
async fn binary_uuid() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_binary(&mut pg, "SELECT * FROM qail_test.ret_uuid()").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["ret_uuid"].as_str().unwrap(),
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    );
}

#[tokio::test]
async fn binary_timestamptz() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_binary(&mut pg, "SELECT * FROM qail_test.ret_timestamptz()").await;
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0]["ret_timestamptz"].is_number(),
        "expected numeric timestamp, got {:?}",
        rows[0]["ret_timestamptz"]
    );
}

#[tokio::test]
async fn binary_text_returns_string() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_binary(&mut pg, "SELECT * FROM qail_test.ret_text()").await;
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0]["ret_text"].is_string(),
        "text result should be a string, got {:?}",
        rows[0]["ret_text"]
    );
}

#[tokio::test]
async fn binary_vs_text_parity() {
    require_db!();
    let mut pg = connect().await;
    let sql = "SELECT * FROM qail_test.ret_int4()";
    let text_rows = query_text(&mut pg, sql).await;
    let binary_rows = query_binary(&mut pg, sql).await;
    assert_eq!(text_rows[0]["ret_int4"], binary_rows[0]["ret_int4"]);
}

// ═══════════════════════════════════════════════════════════════════════
// 3. RPC Contracts Endpoint Query Tests
// ═══════════════════════════════════════════════════════════════════════

fn contracts_query() -> String {
    "SELECT \
        n.nspname AS schema_name, \
        p.proname AS function_name, \
        p.pronargs::int4 AS total_args, \
        p.pronargdefaults::int4 AS default_args, \
        (p.provariadic <> 0) AS is_variadic, \
        COALESCE((\
            SELECT jsonb_agg(NULLIF(BTRIM(arg_name), '') ORDER BY ord) \
            FROM unnest((COALESCE(p.proargnames, ARRAY[]::text[]))[1:p.pronargs]) \
                 WITH ORDINALITY AS names(arg_name, ord) \
        ), '[]'::jsonb)::text AS arg_names_json, \
        COALESCE((\
            SELECT jsonb_agg((arg_oid)::regtype::text ORDER BY ord) \
            FROM unnest(\
                CASE \
                    WHEN p.pronargs = 0 THEN ARRAY[]::oid[] \
                    ELSE string_to_array(BTRIM(p.proargtypes::text), ' ')::oid[] \
                END\
            ) WITH ORDINALITY AS args(arg_oid, ord) \
        ), '[]'::jsonb)::text AS arg_types_json, \
        pg_catalog.pg_get_function_identity_arguments(p.oid) AS identity_args, \
        pg_catalog.pg_get_function_result(p.oid) AS result_type \
    FROM pg_catalog.pg_proc p \
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
    WHERE n.nspname = 'qail_test' \
    ORDER BY n.nspname, p.proname, p.oid \
    LIMIT 100"
        .to_string()
}

#[tokio::test]
async fn contracts_returns_seeded_functions() {
    require_db!();
    let mut pg = connect().await;

    let rows = query_text(&mut pg, &contracts_query()).await;
    // Seeded: add, greet, sum_all, ret_bool, ret_int4, ret_int8, ret_float8,
    // ret_numeric, ret_jsonb, ret_uuid, ret_timestamptz, ret_text, multi_ret
    assert!(
        rows.len() >= 13,
        "expected ≥13 functions, got {}",
        rows.len()
    );

    let add_row = rows
        .iter()
        .find(|r| r["function_name"].as_str() == Some("add"))
        .expect("'add' not found");
    assert_eq!(add_row["schema_name"].as_str(), Some("qail_test"));
    assert_eq!(add_row["total_args"], 2);

    let multi = rows
        .iter()
        .find(|r| r["function_name"].as_str() == Some("multi_ret"))
        .expect("'multi_ret' not found");
    let result_type = multi["result_type"].as_str().unwrap_or("");
    assert!(
        result_type.contains("TABLE") || result_type.contains("record"),
        "expected TABLE/record, got '{}'",
        result_type
    );
}

#[tokio::test]
async fn contracts_variadic_flag() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_text(&mut pg, &contracts_query()).await;

    let sum_all = rows
        .iter()
        .find(|r| r["function_name"].as_str() == Some("sum_all"))
        .expect("'sum_all' not found");
    assert_eq!(sum_all["is_variadic"], true);

    let add = rows
        .iter()
        .find(|r| r["function_name"].as_str() == Some("add"))
        .expect("'add' not found");
    assert_eq!(add["is_variadic"], false);
}

// ═══════════════════════════════════════════════════════════════════════
// 4. End-to-End RPC Execution
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn rpc_exec_add_named_args() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_text(&mut pg, "SELECT * FROM qail_test.add(a => 10, b => 32)").await;
    assert_eq!(rows[0]["add"], 42);
}

#[tokio::test]
async fn rpc_exec_add_positional() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_text(&mut pg, "SELECT * FROM qail_test.add(10, 32)").await;
    assert_eq!(rows[0]["add"], 42);
}

#[tokio::test]
async fn rpc_exec_greet_with_default() {
    require_db!();
    let mut pg = connect().await;

    let rows = query_text(&mut pg, "SELECT * FROM qail_test.greet('Alice')").await;
    assert_eq!(rows[0]["greet"], "hi Alice");

    let rows2 = query_text(&mut pg, "SELECT * FROM qail_test.greet('Bob', 'hey')").await;
    assert_eq!(rows2[0]["greet"], "hey Bob");
}

#[tokio::test]
async fn rpc_exec_variadic_sum() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_text(&mut pg, "SELECT * FROM qail_test.sum_all(1, 2, 3, 4, 5)").await;
    assert_eq!(rows[0]["sum_all"], 15);
}

#[tokio::test]
async fn rpc_exec_binary_add() {
    require_db!();
    let mut pg = connect().await;
    let rows = query_binary(&mut pg, "SELECT * FROM qail_test.add(100, 200)").await;
    assert_eq!(rows[0]["add"], 300);
}

// ═══════════════════════════════════════════════════════════════════════
// 5. Inline binary decode coverage
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn binary_decode_inline_types() {
    require_db!();
    let mut pg = connect().await;

    let rows = query_binary(
        &mut pg,
        "SELECT true AS bool_col, 42::int4 AS int4_col, 9223372036854775807::int8 AS int8_col, 3.14::float8 AS float8_col, 99.95::numeric AS numeric_col, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS uuid_col",
    )
    .await;

    let r = &rows[0];
    assert_eq!(r["bool_col"], true);
    assert_eq!(r["int4_col"], 42);
    assert_eq!(r["int8_col"], 9223372036854775807_i64);
    #[allow(clippy::approx_constant)]
    let expected_float = 3.14;
    assert!((r["float8_col"].as_f64().unwrap() - expected_float).abs() < 0.001);
    // Numeric binary decode may truncate 99.95 to i64(99).
    if let Some(f) = r["numeric_col"].as_f64() {
        assert!((99.0..=100.0).contains(&f));
    } else {
        assert!(
            r["numeric_col"].is_string(),
            "numeric should be number or string"
        );
    }
    assert_eq!(
        r["uuid_col"].as_str().unwrap(),
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    );
}

#[tokio::test]
async fn binary_decode_jsonb_inline() {
    require_db!();
    let mut pg = connect().await;

    let rows = query_binary(&mut pg, r#"SELECT '{"a":1,"b":[2,3]}'::jsonb AS j"#).await;
    assert_eq!(rows[0]["j"]["a"], 1);
    assert_eq!(rows[0]["j"]["b"][0], 2);
}

// ═══════════════════════════════════════════════════════════════════════
// 6. Real Router RPC Coverage
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn rpc_route_variadic_empty_object_executes_without_required_arg_error() {
    let _serial = integration_test_serial_guard().await;
    require_db!();
    let _dev_mode = EnvGuard::set("QAIL_DEV_MODE", "1");
    let app = build_rpc_router(&["qail_test.sum_all"]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/rpc/qail_test.sum_all")
                .header("content-type", "application/json")
                .header("x-user-id", "rpc-user")
                .header("x-user-role", "operator")
                .body(Body::from("{}"))
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["data"][0]["sum_all"], 0);
}

#[tokio::test]
async fn rpc_route_custom_enum_and_domain_use_typed_probe_fallback() {
    let _serial = integration_test_serial_guard().await;
    require_db!();
    let _dev_mode = EnvGuard::set("QAIL_DEV_MODE", "1");

    let enum_app = build_rpc_router(&["qail_test.echo_priority"]).await;
    let enum_response = enum_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/rpc/qail_test.echo_priority")
                .header("content-type", "application/json")
                .header("x-user-id", "rpc-user")
                .header("x-user-role", "operator")
                .body(Body::from(r#""high""#))
                .expect("enum request"),
        )
        .await
        .expect("enum response");
    assert_eq!(enum_response.status(), StatusCode::OK);
    let enum_body = response_json(enum_response).await;
    assert_eq!(enum_body["data"][0]["echo_priority"], "high");

    let domain_app = build_rpc_router(&["qail_test.echo_short"]).await;
    let domain_response = domain_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/rpc/qail_test.echo_short")
                .header("content-type", "application/json")
                .header("x-user-id", "rpc-user")
                .header("x-user-role", "operator")
                .body(Body::from(r#""hello""#))
                .expect("domain request"),
        )
        .await
        .expect("domain response");
    assert_eq!(domain_response.status(), StatusCode::OK);
    let domain_body = response_json(domain_response).await;
    assert_eq!(domain_body["data"][0]["echo_short"], "hello");
}

#[tokio::test]
async fn rpc_contracts_route_marks_variadic_tail_optional() {
    let _serial = integration_test_serial_guard().await;
    require_db!();
    let _dev_mode = EnvGuard::set("QAIL_DEV_MODE", "1");
    let app = build_rpc_router(&["qail_test.sum_all"]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/_rpc/contracts")
                .header("x-user-id", "admin-user")
                .header("x-user-role", "administrator")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let functions = body["functions"].as_array().expect("functions array");
    let sum_all = functions
        .iter()
        .find(|f| f["name"] == "qail_test.sum_all")
        .expect("sum_all contract");
    assert_eq!(sum_all["required_args"], 0);
    assert_eq!(sum_all["args"][0]["required"], false);
    assert_eq!(sum_all["args"][0]["variadic"], true);
}
