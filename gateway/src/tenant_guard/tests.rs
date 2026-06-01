use super::*;
use crate::GatewayState;
use crate::cache::QueryCache;
use crate::concurrency::TenantSemaphore;
use crate::config::GatewayConfig;
use crate::event::EventTriggerEngine;
use crate::middleware::{QueryAllowList, QueryComplexityGuard, RateLimiter};
use crate::policy::PolicyEngine;
use crate::schema::SchemaRegistry;
use jsonwebtoken::Algorithm;
use metrics_exporter_prometheus::PrometheusBuilder;
use qail_pg::{PgPool, PoolConfig};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

async fn build_tenant_guard_state() -> GatewayState {
    let config = GatewayConfig {
        tenant_column: "tenant_id".to_string(),
        ..GatewayConfig::default()
    };

    let pool = PgPool::connect(
        PoolConfig::new_dev("127.0.0.1", 5432, "qail", "qail")
            .min_connections(0)
            .max_connections(1)
            .connect_timeout(Duration::from_millis(25))
            .acquire_timeout(Duration::from_millis(25)),
    )
    .await
    .expect("pool should initialize lazily with min_connections=0");

    let mut schema = SchemaRegistry::new();
    schema
        .load_from_qail_str(
            r#"
table orders {
id uuid primary_key
tenant_id text not_null
total integer
}

table source_orders {
id uuid primary_key
tenant_id text not_null
total integer
}
        "#,
        )
        .expect("test schema loads");

    let explain_config = config.explain_config();
    let explain_cache = qail_pg::explain::ExplainCache::new(explain_config.cache_ttl);
    let tenant_semaphore = Arc::new(TenantSemaphore::with_limits(
        config.max_concurrent_queries,
        config.max_tenants,
        Duration::from_secs(config.tenant_idle_timeout_secs),
    ));
    let db_backpressure = Arc::new(crate::db_backpressure::DbBackpressure::new(
        config.db_max_waiters_global,
        config.db_max_waiters_per_tenant,
        config.max_tenants,
    ));
    let prometheus_handle = {
        let recorder = PrometheusBuilder::new().build_recorder();
        Arc::new(recorder.handle())
    };
    let txn_max = if config.txn_max_sessions > 0 {
        config.txn_max_sessions
    } else {
        std::cmp::max(pool.max_connections() / 2, 2)
    };

    GatewayState {
        pool,
        policy_engine: PolicyEngine::new(),
        access_policy: None,
        event_engine: EventTriggerEngine::new(),
        schema,
        cache: QueryCache::new(config.cache_config()),
        config: config.clone(),
        rate_limiter: RateLimiter::new(config.rate_limit_rate, config.rate_limit_burst),
        tenant_rate_limiter: RateLimiter::new(
            config.tenant_rate_limit_rate,
            config.tenant_rate_limit_burst,
        ),
        explain_cache,
        explain_config,
        tenant_semaphore,
        db_backpressure,
        user_tenant_map: Arc::new(RwLock::new(HashMap::new())),
        #[cfg(feature = "qdrant")]
        qdrant_pool: None,
        prometheus_handle,
        complexity_guard: QueryComplexityGuard::new(
            config.max_query_depth,
            config.max_query_filters,
            config.max_query_joins,
        ),
        allow_list: QueryAllowList::new(),
        rpc_allow_list: None,
        rpc_signature_cache: moka::sync::Cache::builder()
            .max_capacity(64)
            .time_to_live(Duration::from_secs(60))
            .build(),
        parse_cache: moka::sync::Cache::builder()
            .max_capacity(64)
            .time_to_live(Duration::from_secs(60))
            .build(),
        idempotency_store: crate::idempotency::IdempotencyStore::production(),
        jwks_store: None,
        jwt_allowed_algorithms: Vec::<Algorithm>::new(),
        blocked_tables: HashSet::new(),
        allowed_tables: HashSet::new(),
        transaction_manager: Arc::new(crate::transaction::TransactionSessionManager::new(
            txn_max,
            config.txn_session_timeout_secs,
            config.txn_max_lifetime_secs,
            config.txn_max_statements_per_session,
        )),
    }
}

fn tenant_auth() -> crate::auth::AuthContext {
    crate::auth::AuthContext {
        user_id: "user-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: HashMap::new(),
    }
}

#[test]
fn clean_response_no_violations() {
    let rows = vec![
        json!({"id": 1, "operator_id": "op-123", "name": "Order A"}),
        json!({"id": 2, "operator_id": "op-123", "name": "Order B"}),
    ];
    assert!(verify_tenant_boundary(&rows, "op-123", "operator_id", "orders", "GET").is_ok());
}

#[test]
fn cross_tenant_violation_detected() {
    let rows = vec![
        json!({"id": 1, "operator_id": "op-123", "name": "Order A"}),
        json!({"id": 2, "operator_id": "op-EVIL", "name": "Leaked!"}),
        json!({"id": 3, "operator_id": "op-123", "name": "Order C"}),
    ];
    let err = verify_tenant_boundary(&rows, "op-123", "operator_id", "orders", "GET").unwrap_err();
    assert_eq!(err.violation_count, 1);
}

#[test]
fn all_rows_wrong_tenant() {
    let rows = vec![
        json!({"id": 1, "operator_id": "op-EVIL"}),
        json!({"id": 2, "operator_id": "op-EVIL"}),
    ];
    let err = verify_tenant_boundary(&rows, "op-123", "operator_id", "orders", "GET").unwrap_err();
    assert_eq!(err.violation_count, 2);
}

#[test]
fn rows_without_operator_id_are_violations() {
    let rows = vec![
        json!({"id": 1, "name": "No operator_id here"}),
        json!({"id": 2, "count": 42}),
    ];
    let err =
        verify_tenant_boundary(&rows, "op-123", "operator_id", "aggregate", "GET").unwrap_err();
    assert_eq!(err.violation_count, 2);
}

#[test]
fn null_operator_id_is_violation_for_tenant_scope() {
    let rows = vec![json!({"id": 1, "operator_id": null, "name": "System row"})];
    let err = verify_tenant_boundary(&rows, "op-123", "operator_id", "settings", "GET")
        .expect_err("tenant-scoped verifier must reject NULL tenant rows");
    assert_eq!(err.violation_count, 1);
}

#[test]
fn non_object_rows_are_violations() {
    let rows = vec![json!("not a row")];
    let err = verify_tenant_boundary(&rows, "op-123", "operator_id", "orders", "GET").unwrap_err();
    assert_eq!(err.violation_count, 1);
}

#[test]
fn ensure_tenant_column_projected_appends_missing_projection() {
    let mut cmd = qail_core::ast::Qail::get("orders").columns(["id", "total"]);
    let injected = ensure_tenant_column_projected(&mut cmd, "operator_id").unwrap();

    assert!(injected);
    assert!(cmd.columns.iter().any(|expr| {
        matches!(expr, qail_core::ast::Expr::Named(name) if name == "operator_id")
    }));
}

#[test]
fn ensure_tenant_column_projected_leaves_star_projection() {
    let mut cmd = qail_core::ast::Qail::get("orders").select_all();
    let injected = ensure_tenant_column_projected(&mut cmd, "operator_id").unwrap();

    assert!(!injected);
    assert_eq!(cmd.columns.len(), 1);
}

#[test]
fn ensure_tenant_column_projected_qualifies_joined_queries() {
    let mut cmd = qail_core::ast::Qail::get("orders")
        .columns(["id"])
        .left_join("customers", "customer_id", "id");
    let injected = ensure_tenant_column_projected(&mut cmd, "operator_id").unwrap();

    assert!(injected);
    assert!(cmd.columns.iter().any(|expr| {
        matches!(expr, qail_core::ast::Expr::Named(name) if name == "orders.operator_id")
    }));
}

#[test]
fn ensure_tenant_column_projected_rejects_spoofed_alias() {
    let mut cmd = qail_core::ast::Qail::get("orders");
    cmd.columns.push(qail_core::ast::Expr::FunctionCall {
        name: "current_setting".to_string(),
        args: vec![qail_core::ast::Expr::Literal(
            qail_core::ast::Value::String("app.current_tenant_id".to_string()),
        )],
        alias: Some("operator_id".to_string()),
    });

    assert!(ensure_tenant_column_projected(&mut cmd, "operator_id").is_err());
}

#[test]
fn strip_tenant_column_from_json_rows_removes_hidden_guard_column() {
    let mut rows = vec![json!({"id": 1, "total": 42, "operator_id": "op-123"})];

    strip_tenant_column_from_json_rows(&mut rows, "operator_id");

    assert_eq!(rows, vec![json!({"id": 1, "total": 42})]);
}

#[test]
fn inject_tenant_payload_overrides_named_insert_scope() {
    let mut cmd = qail_core::ast::Qail::add("orders")
        .set_value("id", qail_core::ast::Value::Int(1))
        .set_value(
            "tenant_id",
            qail_core::ast::Value::String("attacker".into()),
        );

    inject_tenant_payload(&mut cmd, "tenant_id", "tenant-1").unwrap();

    let payload = cmd
        .cages
        .iter()
        .find(|cage| matches!(cage.kind, qail_core::ast::CageKind::Payload))
        .expect("payload cage");
    assert!(payload.conditions.iter().any(|condition| {
        matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "tenant_id")
            && matches!(&condition.value, qail_core::ast::Value::String(value) if value == "tenant-1")
    }));
    assert!(!payload.conditions.iter().any(|condition| {
        matches!(&condition.value, qail_core::ast::Value::String(value) if value == "attacker")
    }));
}

#[test]
fn inject_tenant_payload_extends_positional_insert_columns() {
    let mut cmd = qail_core::ast::Qail::add("orders")
        .columns(["id", "total"])
        .values([
            qail_core::ast::Value::Int(1),
            qail_core::ast::Value::Int(42),
        ]);

    inject_tenant_payload(&mut cmd, "tenant_id", "tenant-1").unwrap();

    assert!(
        cmd.columns.iter().any(|expr| {
            matches!(expr, qail_core::ast::Expr::Named(name) if name == "tenant_id")
        })
    );
    let payload = cmd
        .cages
        .iter()
        .find(|cage| matches!(cage.kind, qail_core::ast::CageKind::Payload))
        .expect("payload cage");
    assert!(payload.conditions.iter().any(|condition| {
        matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "$3")
            && matches!(&condition.value, qail_core::ast::Value::String(value) if value == "tenant-1")
    }));
}

#[test]
fn inject_tenant_payload_from_source_query_overrides_target_tenant_column() {
    let mut cmd = qail_core::ast::Qail::add("orders").columns(["id", "tenant_id", "total"]);
    cmd.source_query = Some(Box::new(
        qail_core::ast::Qail::get("source_orders").columns(["id", "attacker_tenant_id", "total"]),
    ));

    inject_tenant_payload_from_source_query(&mut cmd, "tenant_id", "tenant-1").unwrap();

    let source_query = cmd.source_query.as_ref().expect("source query");
    assert!(matches!(
        &source_query.columns[1],
        qail_core::ast::Expr::Literal(qail_core::ast::Value::String(value)) if value == "tenant-1"
    ));
    assert!(
        !source_query.columns.iter().any(|expr| {
            matches!(expr, qail_core::ast::Expr::Named(name) if name == "attacker_tenant_id")
        }),
        "source tenant projection should be replaced by authenticated tenant"
    );
}

#[tokio::test]
async fn prepare_tenant_guarded_query_filters_insert_select_source_and_injects_target_tenant() {
    let state = build_tenant_guard_state().await;
    let auth = tenant_auth();
    let mut cmd = qail_core::ast::Qail::add("orders").columns(["id", "total"]);
    cmd.source_query = Some(Box::new(
        qail_core::ast::Qail::get("source_orders").columns(["id", "total"]),
    ));

    let plan = prepare_tenant_guarded_query(&state, &auth, &mut cmd).unwrap();

    assert!(plan.is_some());
    assert!(
        cmd.columns.iter().any(|expr| {
            matches!(expr, qail_core::ast::Expr::Named(name) if name == "tenant_id")
        })
    );
    let source_query = cmd.source_query.as_ref().expect("source query");
    assert!(source_query.cages.iter().any(|cage| {
        matches!(cage.kind, qail_core::ast::CageKind::Filter)
            && cage.conditions.iter().any(|condition| {
                matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "tenant_id")
                    && matches!(&condition.value, qail_core::ast::Value::String(value) if value == "tenant-1")
            })
    }));
    assert!(matches!(
        source_query.columns.last(),
        Some(qail_core::ast::Expr::Literal(qail_core::ast::Value::String(value))) if value == "tenant-1"
    ));
}

#[tokio::test]
async fn prepare_tenant_guarded_query_scopes_merge_target_source_and_insert_values() {
    let state = build_tenant_guard_state().await;
    let auth = tenant_auth();
    let mut cmd = qail_core::ast::Qail::merge_into("orders")
        .using_table_as("source_orders", "s")
        .merge_on_column("orders.id", qail_core::ast::Operator::Eq, "s.id")
        .when_matched_update(&[("total", qail_core::ast::Expr::Named("s.total".to_string()))])
        .when_not_matched_insert(
            &["id", "total"],
            &[
                qail_core::ast::Expr::Named("s.id".to_string()),
                qail_core::ast::Expr::Named("s.total".to_string()),
            ],
        );

    let plan = prepare_tenant_guarded_query(&state, &auth, &mut cmd).unwrap();

    assert!(plan.is_some());
    let merge = cmd.merge.as_ref().expect("merge spec");
    assert!(merge.on.iter().any(|condition| {
        matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "orders.tenant_id")
            && matches!(&condition.value, qail_core::ast::Value::Column(name) if name == "s.tenant_id")
    }));
    assert!(merge.clauses.iter().any(|clause| {
        matches!(clause.match_kind, qail_core::ast::MergeMatchKind::Matched)
            && clause.condition.iter().any(|condition| {
                matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "orders.tenant_id")
                    && matches!(&condition.value, qail_core::ast::Value::String(value) if value == "tenant-1")
            })
    }));
    assert!(merge.clauses.iter().any(|clause| {
        matches!(clause.match_kind, qail_core::ast::MergeMatchKind::NotMatchedByTarget)
            && clause.condition.iter().any(|condition| {
                matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "s.tenant_id")
                    && matches!(&condition.value, qail_core::ast::Value::String(value) if value == "tenant-1")
            })
            && matches!(&clause.action, qail_core::ast::MergeAction::Insert { columns, values }
                if columns.iter().any(|column| column == "tenant_id")
                    && values.iter().any(|expr| {
                        matches!(expr, qail_core::ast::Expr::Literal(qail_core::ast::Value::String(value)) if value == "tenant-1")
                    }))
    }));
}

#[tokio::test]
async fn prepare_tenant_guarded_query_filters_merge_query_source() {
    let state = build_tenant_guard_state().await;
    let auth = tenant_auth();
    let source = qail_core::ast::Qail::get("source_orders").columns(["id", "total"]);
    let mut cmd = qail_core::ast::Qail::merge_into("orders")
        .using_query_as(source, "s")
        .merge_on_column("orders.id", qail_core::ast::Operator::Eq, "s.id")
        .when_not_matched_insert(
            &["id", "total"],
            &[
                qail_core::ast::Expr::Named("s.id".to_string()),
                qail_core::ast::Expr::Named("s.total".to_string()),
            ],
        );

    prepare_tenant_guarded_query(&state, &auth, &mut cmd).unwrap();

    let merge = cmd.merge.as_ref().expect("merge spec");
    let qail_core::ast::MergeSource::Query { query, .. } = &merge.source else {
        panic!("expected query source");
    };
    assert!(merge.on.iter().any(|condition| {
        matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "orders.tenant_id")
            && matches!(&condition.value, qail_core::ast::Value::String(value) if value == "tenant-1")
    }));
    assert!(query.cages.iter().any(|cage| {
        matches!(cage.kind, qail_core::ast::CageKind::Filter)
            && cage.conditions.iter().any(|condition| {
                matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "tenant_id")
                    && matches!(&condition.value, qail_core::ast::Value::String(value) if value == "tenant-1")
            })
    }));
    assert!(merge.clauses.iter().any(|clause| {
        matches!(&clause.action, qail_core::ast::MergeAction::Insert { columns, values }
            if columns.iter().any(|column| column == "tenant_id")
                && values.iter().any(|expr| {
                    matches!(expr, qail_core::ast::Expr::Literal(qail_core::ast::Value::String(value)) if value == "tenant-1")
                }))
    }));
}

#[tokio::test]
async fn prepare_tenant_guarded_query_filters_expression_subquery() {
    let state = build_tenant_guard_state().await;
    let auth = tenant_auth();
    let mut cmd = qail_core::ast::Qail::get("orders").columns(["id"]);
    cmd.columns.push(qail_core::ast::Expr::Subquery {
        query: Box::new(qail_core::ast::Qail::get("source_orders").columns(["total"])),
        alias: Some("source_total".to_string()),
    });

    prepare_tenant_guarded_query(&state, &auth, &mut cmd).unwrap();

    let subquery = cmd
        .columns
        .iter()
        .find_map(|expr| {
            if let qail_core::ast::Expr::Subquery { query, .. } = expr {
                Some(query)
            } else {
                None
            }
        })
        .expect("expression subquery");
    assert!(subquery.cages.iter().any(|cage| {
        matches!(cage.kind, qail_core::ast::CageKind::Filter)
            && cage.conditions.iter().any(|condition| {
                matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "tenant_id")
                    && matches!(&condition.value, qail_core::ast::Value::String(value) if value == "tenant-1")
            })
    }));
}

#[tokio::test]
async fn prepare_tenant_guarded_query_filters_condition_value_subquery() {
    let state = build_tenant_guard_state().await;
    let auth = tenant_auth();
    let mut cmd = qail_core::ast::Qail::get("orders").filter(
        "id",
        qail_core::ast::Operator::In,
        qail_core::ast::Value::Subquery(Box::new(
            qail_core::ast::Qail::get("source_orders").columns(["id"]),
        )),
    );

    prepare_tenant_guarded_query(&state, &auth, &mut cmd).unwrap();

    let subquery = cmd
        .cages
        .iter()
        .flat_map(|cage| &cage.conditions)
        .find_map(|condition| {
            if let qail_core::ast::Value::Subquery(query) = &condition.value {
                Some(query)
            } else {
                None
            }
        })
        .expect("condition value subquery");
    assert!(subquery.cages.iter().any(|cage| {
        matches!(cage.kind, qail_core::ast::CageKind::Filter)
            && cage.conditions.iter().any(|condition| {
                matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "tenant_id")
                    && matches!(&condition.value, qail_core::ast::Value::String(value) if value == "tenant-1")
            })
    }));
}

#[tokio::test]
async fn prepare_tenant_guarded_query_rejects_merge_tenant_column_update() {
    let state = build_tenant_guard_state().await;
    let auth = tenant_auth();
    let mut cmd = qail_core::ast::Qail::merge_into("orders")
        .using_table_as("source_orders", "s")
        .merge_on_column("orders.id", qail_core::ast::Operator::Eq, "s.id")
        .when_matched_update(&[(
            "tenant_id",
            qail_core::ast::Expr::Named("s.tenant_id".to_string()),
        )]);

    let err = prepare_tenant_guarded_query(&state, &auth, &mut cmd).unwrap_err();

    assert!(
        err.to_string()
            .contains("cannot update tenant guard column"),
        "MERGE must not be able to overwrite tenant guard column"
    );
}

#[test]
fn inject_join_tenant_filter_scopes_left_join_on_clause() {
    let mut cmd =
        qail_core::ast::Qail::get("orders").left_join("users u", "orders.user_id", "u.id");
    let mut join = cmd.joins.pop().expect("join");

    inject_join_tenant_filter(&mut cmd, &mut join, "u", "tenant_id", "tenant-1").unwrap();

    let on = join.on.expect("join conditions");
    assert!(on.iter().any(|condition| {
        matches!(&condition.left, qail_core::ast::Expr::Named(name) if name == "u.tenant_id")
            && matches!(&condition.value, qail_core::ast::Value::String(value) if value == "tenant-1")
    }));
}

#[tokio::test]
async fn prepare_tenant_guarded_query_appends_base_alias_projection_for_join() {
    let state = build_tenant_guard_state().await;
    let auth = tenant_auth();
    let mut cmd = qail_core::ast::Qail::get("orders o")
        .columns(["o.id", "o.total"])
        .left_join("source_orders s", "o.id", "s.id");

    let plan = prepare_tenant_guarded_query(&state, &auth, &mut cmd).unwrap();

    assert!(plan.as_ref().is_some_and(|plan| plan.strip_output_column));
    assert!(cmd.columns.iter().any(|expr| {
        matches!(expr, qail_core::ast::Expr::Named(name) if name == "o.tenant_id")
    }));
    assert!(
        !cmd.columns.iter().any(|expr| {
            matches!(expr, qail_core::ast::Expr::Named(name) if name == "orders o.tenant_id")
        }),
        "guard projection must use the table alias, not the raw table ref"
    );
}

#[tokio::test]
async fn prepare_tenant_guarded_query_rejects_join_tenant_projection_spoof() {
    let state = build_tenant_guard_state().await;
    let auth = tenant_auth();
    let mut cmd = qail_core::ast::Qail::get("orders o")
        .columns(["o.id", "s.tenant_id"])
        .left_join("source_orders s", "o.id", "s.id");

    let err = prepare_tenant_guarded_query(&state, &auth, &mut cmd).unwrap_err();

    assert_eq!(err.column, "tenant_id");
    assert!(
        err.reason.is_none(),
        "spoofed tenant output should be rejected as a reserved projection"
    );
}

#[tokio::test]
async fn prepare_tenant_guarded_query_rejects_joined_wildcard_projection() {
    let state = build_tenant_guard_state().await;
    let auth = tenant_auth();
    let mut cmd =
        qail_core::ast::Qail::get("orders o").left_join("source_orders s", "o.id", "s.id");

    let err = prepare_tenant_guarded_query(&state, &auth, &mut cmd).unwrap_err();

    assert_eq!(err.column, "tenant_id");
    assert!(
        err.to_string()
            .contains("requires explicit base-table projections"),
        "joined wildcard projections cannot prove which tenant_id reached the verifier"
    );
}

#[test]
fn empty_expected_operator_id_skips_check() {
    let rows = vec![json!({"id": 1, "operator_id": "op-123"})];
    assert!(verify_tenant_boundary(&rows, "", "operator_id", "orders", "GET").is_ok());
}

#[test]
fn empty_rows_is_clean() {
    assert!(verify_tenant_boundary(&[], "op-123", "operator_id", "orders", "GET").is_ok());
}

#[test]
fn integer_operator_id_compared_as_string() {
    let rows = vec![json!({"id": 1, "operator_id": 123})];
    assert!(verify_tenant_boundary(&rows, "op-123", "operator_id", "orders", "GET").is_err());
    assert!(verify_tenant_boundary(&rows, "123", "operator_id", "orders", "GET").is_ok());
}

#[test]
fn custom_tenant_column() {
    let rows = vec![
        json!({"id": 1, "tenant_id": "t-abc", "name": "Order A"}),
        json!({"id": 2, "tenant_id": "t-abc", "name": "Order B"}),
    ];
    assert!(verify_tenant_boundary(&rows, "t-abc", "tenant_id", "orders", "GET").is_ok());
    assert!(verify_tenant_boundary(&rows, "t-xyz", "tenant_id", "orders", "GET").is_err());
}
