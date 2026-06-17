//! Complex live PostgreSQL gateway coverage for branch overlay and RPC fallback.
//!
//! Run:
//!   QAIL_GATEWAY_COMPLEX_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:55433/postgres" \
//!     cargo test -p qail-gateway --test complex_gateway_live -- --ignored --nocapture

use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, request::Builder};
use qail_core::ast::{Operator, Qail, Value as QailValue};
use qail_gateway::{GatewayConfig, GatewayState, create_router};
use qail_pg::{PgDriver, PgPool, PoolConfig};
use serde_json::{Value, json};
use tower::util::ServiceExt;
use url::Url;
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("QAIL_GATEWAY_COMPLEX_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:55433/postgres".to_string())
}

fn pool_config_from_database_url(database_url: &str) -> PoolConfig {
    let parsed = Url::parse(database_url).expect("valid database URL");
    let host = parsed.host_str().expect("database host");
    let port = parsed.port().unwrap_or(5432);
    let user = if parsed.username().is_empty() {
        "postgres"
    } else {
        parsed.username()
    };
    let database = parsed.path().trim_start_matches('/');
    assert!(!database.is_empty(), "database name required");

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

fn unique_name(prefix: &str) -> String {
    format!("{}_{}", prefix, Uuid::new_v4().simple())
}

fn admin_request(builder: Builder) -> Builder {
    builder
        .header("x-user-id", "complex-live-admin")
        .header("x-user-role", "administrator")
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    if body.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(&body).expect("valid JSON response")
}

async fn request_json(
    app: axum::Router,
    builder: Builder,
    body: impl Into<Body>,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(builder.body(body.into()).expect("request body"))
        .await
        .expect("router response");
    let status = response.status();
    let body = response_json(response).await;
    (status, body)
}

fn write_schema(table: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "qail_complex_gateway_schema_{}_{}.qail",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::write(
        &path,
        format!(
            r#"
table {table} {{
  id text primary_key
  name text not_null
  tenant_id text
}}
"#
        ),
    )
    .expect("write schema");
    path
}

fn write_rpc_allowlist(function: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "qail_complex_gateway_rpc_{}_{}.allow",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::write(&path, function).expect("write RPC allowlist");
    path
}

async fn build_router(
    database_url: &str,
    schema_path: &std::path::Path,
    rpc_allowlist_path: &std::path::Path,
) -> axum::Router {
    let pool = PgPool::connect(pool_config_from_database_url(database_url))
        .await
        .expect("gateway pool");
    let config = GatewayConfig {
        database_url: database_url.to_string(),
        production_strict: false,
        require_auth: false,
        bind_address: "127.0.0.1:0".to_string(),
        schema_path: Some(schema_path.to_string_lossy().into_owned()),
        rpc_allowlist_path: Some(rpc_allowlist_path.to_string_lossy().into_owned()),
        rpc_signature_check: false,
        ..GatewayConfig::default()
    };
    let state = GatewayState::new_embedded(pool, config)
        .await
        .expect("embedded gateway state");
    create_router(Arc::new(state), &[])
}

async fn row_names(driver: &mut PgDriver, table: &str) -> Vec<(String, String)> {
    driver
        .fetch_all_uncached(&Qail::get(table).columns(["id", "name"]).order_asc("id"))
        .await
        .expect("read rows")
        .into_iter()
        .map(|row| {
            (
                row.get_string(0).expect("id"),
                row.get_string(1).expect("name"),
            )
        })
        .collect()
}

#[tokio::test]
#[ignore = "Requires live PostgreSQL; set QAIL_GATEWAY_COMPLEX_DATABASE_URL or DATABASE_URL"]
async fn branch_overlay_and_rpc_void_fallback_survive_real_postgres() {
    let database_url = database_url();
    let table = unique_name("qail_complex_branch");
    let side_table = unique_name("qail_complex_rpc_side");
    let function = unique_name("qail_complex_void");
    let full_function = format!("public.{function}");
    let branch = unique_name("qa_live");
    let schema_path = write_schema(&table);
    let allowlist_path = write_rpc_allowlist(&full_function);
    let _dev_auth = EnvGuard::set("QAIL_DEV_MODE", "true");
    let _jwt = EnvGuard::set("JWT_SECRET", "complex-live-secret");

    let mut driver = PgDriver::connect_url(&database_url)
        .await
        .expect("setup connection");
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .expect("drop table");
    driver
        .execute_simple(&format!(
            "CREATE TABLE {table} (
                id text PRIMARY KEY,
                name text NOT NULL,
                tenant_id text
            )"
        ))
        .await
        .expect("create table");
    driver
        .execute_simple(&format!(
            "INSERT INTO {table} (id, name, tenant_id) VALUES ('one', 'main-one', 'tenant-a')"
        ))
        .await
        .expect("seed table");
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {side_table}"))
        .await
        .expect("drop RPC side table");
    driver
        .execute_simple(&format!(
            "CREATE TABLE {side_table} (id bigserial PRIMARY KEY, note text NOT NULL)"
        ))
        .await
        .expect("create RPC side table");
    driver
        .execute_simple(&format!(
            "CREATE OR REPLACE FUNCTION public.{function}(note text) RETURNS void
             LANGUAGE plpgsql AS $$
             BEGIN
                 INSERT INTO {side_table} (note) VALUES (note);
             END
             $$"
        ))
        .await
        .expect("create void RPC");

    let app = build_router(&database_url, &schema_path, &allowlist_path).await;

    let (status, body) = request_json(
        app.clone(),
        admin_request(
            Request::builder()
                .method("POST")
                .uri("/api/_branch")
                .header("content-type", "application/json"),
        ),
        Body::from(json!({"name": branch}).to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "branch create body: {body}");

    let (status, body) = request_json(
        app.clone(),
        admin_request(Request::builder().method("GET").uri("/api/_branch")),
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "branch list body: {body}");
    assert!(
        body["branches"]
            .as_array()
            .is_some_and(|branches| branches.iter().any(|row| row["name"] == branch)),
        "branch list should include {branch}: {body}"
    );

    let (status, body) = request_json(
        app.clone(),
        admin_request(
            Request::builder()
                .method("POST")
                .uri(format!("/api/{table}?returning=*"))
                .header("content-type", "application/json")
                .header("x-branch-id", &branch),
        ),
        Body::from(json!({"id": "two", "name": "branch-two"}).to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "branch insert body: {body}");

    let (status, body) = request_json(
        app.clone(),
        admin_request(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/{table}/one?returning=*"))
                .header("content-type", "application/json")
                .header("x-branch-id", &branch),
        ),
        Body::from(json!({"name": "branch-one"}).to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "branch update body: {body}");
    assert_eq!(
        row_names(&mut driver, &table).await,
        vec![("one".into(), "main-one".into())]
    );

    let (status, body) = request_json(
        app.clone(),
        admin_request(
            Request::builder()
                .method("GET")
                .uri(format!("/api/{table}?sort=id:asc&select=id,name"))
                .header("x-branch-id", &branch),
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "branch read body: {body}");
    assert_eq!(body["data"][0]["name"], "branch-one");
    assert_eq!(body["data"][1]["name"], "branch-two");

    let (status, body) = request_json(
        app.clone(),
        admin_request(
            Request::builder()
                .method("POST")
                .uri(format!("/api/rpc/{full_function}"))
                .header("content-type", "application/json"),
        ),
        Body::from(r#""from-void-fallback""#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "void RPC body: {body}");
    let side_rows = driver
        .fetch_all_uncached(&Qail::get(&side_table).columns(["note"]).filter(
            "note",
            Operator::Eq,
            QailValue::String("from-void-fallback".into()),
        ))
        .await
        .expect("read RPC side effect");
    assert_eq!(side_rows.len(), 1, "void RPC should insert exactly once");

    let (status, body) = request_json(
        app.clone(),
        admin_request(
            Request::builder()
                .method("POST")
                .uri(format!("/api/_branch/{branch}/merge")),
        ),
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "branch merge body: {body}");
    assert_eq!(
        row_names(&mut driver, &table).await,
        vec![
            ("one".into(), "branch-one".into()),
            ("two".into(), "branch-two".into())
        ]
    );

    driver
        .execute_simple(&format!("DROP FUNCTION IF EXISTS public.{function}(text)"))
        .await
        .expect("drop function");
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {side_table}"))
        .await
        .expect("drop side table");
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .expect("drop table");
    let _ = std::fs::remove_file(schema_path);
    let _ = std::fs::remove_file(allowlist_path);
}
