//! Live PostgreSQL validation for gateway native access-policy enforcement.
//!
//! Run:
//!   QAIL_GATEWAY_ACCESS_DATABASE_URL="postgres://postgres@127.0.0.1:55433/postgres" \
//!     cargo test -p qail-gateway --test access_policy_live -- --ignored --nocapture

use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use qail_core::ast::{Expr, Operator, Qail, SortOrder, Value as QailValue};
use qail_gateway::{GatewayConfig, GatewayState, create_router};
use qail_pg::{PgDriver, PgPool, PoolConfig};
use serde_json::Value;
use tower::util::ServiceExt;
use url::Url;
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("QAIL_GATEWAY_ACCESS_DATABASE_URL")
        .or_else(|_| std::env::var("QAIL_ACCESS_DATABASE_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://postgres@127.0.0.1:55433/postgres".to_string())
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
    assert!(
        !database.is_empty(),
        "database name required in live access-policy URL"
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

fn test_table() -> String {
    format!("qail_access_policy_live_{}", Uuid::new_v4().simple())
}

fn write_access_policy(table: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "qail_gateway_access_policy_{}_{}.toml",
        std::process::id(),
        Uuid::new_v4().simple()
    ));
    std::fs::write(
        &path,
        format!(
            r#"
default_decision = "deny"

[tables.{table}]
operations = ["read", "update"]
read_columns = {{ only = ["id", "name"] }}
write_columns = {{ only = ["name"] }}
require_any_role = ["operator"]
"#
        ),
    )
    .expect("write access policy");
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

async fn create_table(driver: &mut PgDriver, table: &str) {
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .expect("drop stale live table");
    driver
        .execute_simple(&format!(
            "CREATE TABLE {table} (
                id integer PRIMARY KEY,
                name text NOT NULL,
                private_note text NOT NULL,
                tenant_id text
            )"
        ))
        .await
        .expect("create live table");
    driver
        .execute_simple(&format!(
            "INSERT INTO {table} (id, name, private_note, tenant_id)
             VALUES (1, 'visible', 'secret', 'tenant-a')"
        ))
        .await
        .expect("seed live table");
}

async fn private_note(driver: &mut PgDriver, table: &str) -> String {
    let rows = driver
        .fetch_all_uncached(&Qail::get(table).columns(["private_note"]).filter(
            "id",
            Operator::Eq,
            QailValue::Int(1),
        ))
        .await
        .expect("read private_note");
    rows.first()
        .and_then(|row| row.try_get_by_name::<String>("private_note").ok())
        .expect("private_note row")
}

async fn visible_name(driver: &mut PgDriver, table: &str) -> String {
    let rows = driver
        .fetch_all_uncached(&Qail::get(table).columns(["name"]).filter(
            "id",
            Operator::Eq,
            QailValue::Int(1),
        ))
        .await
        .expect("read name");
    rows.first()
        .and_then(|row| row.try_get_by_name::<String>("name").ok())
        .expect("name row")
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    serde_json::from_slice(&body).expect("valid JSON response")
}

fn operator_request(builder: axum::http::request::Builder) -> axum::http::request::Builder {
    builder
        .header("x-user-id", "operator-1")
        .header("x-user-role", "operator")
        .header("x-tenant-id", "tenant-a")
}

fn admin_request(builder: axum::http::request::Builder) -> axum::http::request::Builder {
    builder
        .header("x-user-id", "admin-1")
        .header("x-user-role", "administrator")
}

async fn post_binary(app: axum::Router, cmd: &Qail, admin: bool) -> axum::response::Response {
    let payload = qail_core::wire::encode_cmd_binary(cmd).expect("encode QWB2 payload");
    let builder = Request::builder()
        .method("POST")
        .uri("/qail/binary")
        .header("content-type", "application/octet-stream");
    let builder = if admin {
        admin_request(builder)
    } else {
        operator_request(builder)
    };
    app.oneshot(builder.body(Body::from(payload)).expect("request"))
        .await
        .expect("execute request")
}

#[tokio::test]
#[ignore = "Requires live PostgreSQL; set QAIL_GATEWAY_ACCESS_DATABASE_URL or DATABASE_URL"]
async fn gateway_native_access_policy_enforces_live_queries() {
    let database_url = database_url();
    let table = test_table();
    let policy_path = write_access_policy(&table);

    let mut driver = PgDriver::connect_url(&database_url)
        .await
        .expect("connect setup driver");
    create_table(&mut driver, &table).await;

    let pool = PgPool::connect(pool_config_from_database_url(&database_url))
        .await
        .expect("gateway pool");
    let config = GatewayConfig {
        database_url: database_url.clone(),
        production_strict: false,
        require_auth: false,
        bind_address: "127.0.0.1:0".to_string(),
        binary_requires_allow_list: false,
        access_policy_path: Some(policy_path.to_string_lossy().into_owned()),
        ..GatewayConfig::default()
    };
    let state = Arc::new(
        GatewayState::new_embedded(pool, config)
            .await
            .expect("embedded gateway state"),
    );
    let _dev_auth = EnvGuard::set("QAIL_DEV_MODE", "true");
    let app = create_router(Arc::clone(&state), &[]);

    let allowed_read = post_binary(
        app.clone(),
        &Qail::get(&table).columns(["id", "name"]).order_asc("id"),
        false,
    )
    .await;
    assert_eq!(allowed_read.status(), StatusCode::OK);
    let body = response_json(allowed_read).await;
    assert_eq!(body["rows"][0]["name"], "visible");
    assert!(body["rows"][0].get("private_note").is_none());

    let denied_read = post_binary(
        app.clone(),
        &Qail::get(&table).columns(["id", "private_note"]),
        false,
    )
    .await;
    assert_eq!(denied_read.status(), StatusCode::FORBIDDEN);

    let denied_filter = post_binary(
        app.clone(),
        &Qail::get(&table)
            .columns(["id"])
            .filter("private_note", Operator::Eq, "secret"),
        false,
    )
    .await;
    assert_eq!(denied_filter.status(), StatusCode::FORBIDDEN);

    let denied_payload_rhs = post_binary(
        app.clone(),
        &Qail::set(&table)
            .set_value("name", QailValue::Column("private_note".to_string()))
            .filter("id", Operator::Eq, QailValue::Int(1)),
        false,
    )
    .await;
    assert_eq!(denied_payload_rhs.status(), StatusCode::FORBIDDEN);
    assert_eq!(visible_name(&mut driver, &table).await, "visible");

    let denied_window_partition = post_binary(
        app.clone(),
        &Qail::get(&table).columns(["id"]).order_by_expr(
            Expr::Window {
                name: "ranked_rows".to_string(),
                func: "row_number".to_string(),
                params: vec![],
                partition: vec!["private_note".to_string()],
                order: vec![],
                frame: None,
            },
            SortOrder::Asc,
        ),
        false,
    )
    .await;
    assert_eq!(denied_window_partition.status(), StatusCode::FORBIDDEN);

    let denied_write = post_binary(
        app.clone(),
        &Qail::set(&table)
            .set_value("private_note", "binary-leak")
            .filter("id", Operator::Eq, QailValue::Int(1)),
        false,
    )
    .await;
    assert_eq!(denied_write.status(), StatusCode::FORBIDDEN);
    assert_eq!(private_note(&mut driver, &table).await, "secret");

    let tx_begin = app
        .clone()
        .oneshot(
            operator_request(Request::builder().method("POST").uri("/txn/begin"))
                .body(Body::empty())
                .expect("txn begin request"),
        )
        .await
        .expect("txn begin response");
    assert_eq!(tx_begin.status(), StatusCode::OK);
    let txn_id = response_json(tx_begin).await["txn_id"]
        .as_str()
        .expect("txn id")
        .to_string();

    let tx_denied = app
        .clone()
        .oneshot(
            operator_request(Request::builder().method("POST").uri("/txn/query"))
                .header("x-transaction-id", &txn_id)
                .header("content-type", "text/plain")
                .body(Body::from(format!(
                    "set {table} values private_note = \"txn-leak\" where id = 1"
                )))
                .expect("txn query request"),
        )
        .await
        .expect("txn denied response");
    assert_eq!(tx_denied.status(), StatusCode::FORBIDDEN);
    assert_eq!(private_note(&mut driver, &table).await, "secret");

    let _rollback = app
        .clone()
        .oneshot(
            operator_request(Request::builder().method("POST").uri("/txn/rollback"))
                .header("x-transaction-id", &txn_id)
                .body(Body::empty())
                .expect("txn rollback request"),
        )
        .await
        .expect("txn rollback response");

    let admin_read = post_binary(
        app,
        &Qail::get(&table).columns(["id", "private_note"]),
        true,
    )
    .await;
    assert_eq!(admin_read.status(), StatusCode::OK);
    let body = response_json(admin_read).await;
    assert_eq!(body["rows"][0]["private_note"], "secret");

    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .expect("drop live table");
    let _ = std::fs::remove_file(policy_path);
}
