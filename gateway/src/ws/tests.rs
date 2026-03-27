use super::*;
use axum::http::{HeaderMap, HeaderValue, Uri, header::AUTHORIZATION};

#[test]
fn live_query_listen_success_registers_channel() {
    let mut subscribed = vec!["tenant_qail_table_orders".to_string()];
    let result = finalize_live_query_listen(true, &mut subscribed, "tenant_qail_table_invoices");
    assert!(result.is_none());
    assert!(subscribed.contains(&"tenant_qail_table_orders".to_string()));
    assert!(subscribed.contains(&"tenant_qail_table_invoices".to_string()));
}

#[test]
fn live_query_listen_failure_returns_error_without_registration() {
    let mut subscribed = vec!["tenant_qail_table_orders".to_string()];
    let result = finalize_live_query_listen(false, &mut subscribed, "tenant_qail_table_invoices");
    match result {
        Some(WsServerMessage::Error { message }) => {
            assert_eq!(message, WS_ERR_LIVE_QUERY_SUB_FAILED);
        }
        _ => panic!("expected error response"),
    }
    assert_eq!(subscribed, vec!["tenant_qail_table_orders".to_string()]);
}

#[test]
fn live_query_listen_success_is_idempotent() {
    let mut subscribed = vec!["tenant_qail_table_orders".to_string()];
    let result = finalize_live_query_listen(true, &mut subscribed, "tenant_qail_table_orders");
    assert!(result.is_none());
    assert_eq!(subscribed, vec!["tenant_qail_table_orders".to_string()]);
}

#[test]
fn stop_live_query_unlisten_success_clears_tracking_and_acknowledges() {
    let mut subscribed = vec![
        "tenant_qail_table_orders".to_string(),
        "tenant_qail_table_invoices".to_string(),
    ];
    let msg = finalize_stop_live_query_unlisten(true, &mut subscribed, "tenant_qail_table_orders");
    match msg {
        WsServerMessage::Unsubscribed { channel } => {
            assert_eq!(channel, "tenant_qail_table_orders");
        }
        _ => panic!("expected unsubscribed response"),
    }
    assert_eq!(subscribed, vec!["tenant_qail_table_invoices".to_string()]);
}

#[test]
fn stop_live_query_unlisten_failure_clears_tracking_and_errors() {
    let mut subscribed = vec![
        "tenant_qail_table_orders".to_string(),
        "tenant_qail_table_invoices".to_string(),
    ];
    let msg = finalize_stop_live_query_unlisten(false, &mut subscribed, "tenant_qail_table_orders");
    match msg {
        WsServerMessage::Error { message } => {
            assert_eq!(message, WS_ERR_LIVE_QUERY_UNSUB_FAILED);
        }
        _ => panic!("expected error response"),
    }
    assert_eq!(subscribed, vec!["tenant_qail_table_invoices".to_string()]);
}

#[test]
fn dispatch_live_query_update_drops_when_channel_full() {
    let (tx, mut rx) = mpsc::channel(1);
    tx.try_send(WsServerMessage::Pong)
        .expect("seed channel should succeed");

    let keep_running =
        dispatch_live_query_update(&tx, "orders", vec![serde_json::json!({"id": 1})], 1, 1);
    assert!(keep_running);
    assert!(matches!(rx.try_recv(), Ok(WsServerMessage::Pong)));
    assert!(rx.try_recv().is_err());
}

#[test]
fn dispatch_live_query_update_stops_when_channel_closed() {
    let (tx, rx) = mpsc::channel(1);
    drop(rx);

    let keep_running =
        dispatch_live_query_update(&tx, "orders", vec![serde_json::json!({"id": 1})], 1, 1);
    assert!(!keep_running);
}

#[test]
fn channel_refcount_tracks_unique_channels() {
    let mut state = WsConnectionState {
        subscribed_channels: Vec::new(),
        manual_subscriptions: std::collections::HashSet::new(),
        channel_refcounts: std::collections::HashMap::new(),
        live_query_tasks: std::collections::HashMap::new(),
        live_query_triggers: std::collections::HashMap::new(),
        live_query_channels: std::collections::HashMap::new(),
    };

    increment_channel_refcount(&mut state, "tenant_orders");
    increment_channel_refcount(&mut state, "tenant_orders");
    assert_eq!(tracked_channel_count(&state), 1);
    assert_eq!(state.channel_refcounts.get("tenant_orders"), Some(&2));
    assert_eq!(state.subscribed_channels, vec!["tenant_orders".to_string()]);

    assert!(!decrement_channel_refcount(&mut state, "tenant_orders"));
    assert_eq!(state.channel_refcounts.get("tenant_orders"), Some(&1));
    assert_eq!(tracked_channel_count(&state), 1);

    assert!(decrement_channel_refcount(&mut state, "tenant_orders"));
    assert_eq!(tracked_channel_count(&state), 0);
    assert!(
        !state
            .subscribed_channels
            .contains(&"tenant_orders".to_string())
    );
}

#[test]
fn manual_notify_channel_rejects_over_pg_identifier_limit() {
    let fragment = "a".repeat(WS_PG_CHANNEL_MAX_BYTES);
    let err = build_manual_notify_channel("tenant", &fragment).expect_err("must reject overflow");
    assert!(err.contains("63 bytes"));
}

#[test]
fn manual_notify_channel_prevents_tenant_channel_collisions() {
    let a = build_manual_notify_channel("acme", "eu_orders").expect("first channel");
    let b = build_manual_notify_channel("acme_eu", "orders").expect("second channel");
    assert_ne!(a, b, "tenant/channel tuples must map to distinct channels");
}

#[test]
fn manual_notify_channel_rejects_unsafe_tenant_identifier() {
    let err = build_manual_notify_channel("tenant.with.dot", "orders")
        .expect_err("unsafe tenant identifiers must be rejected");
    assert!(err.contains("unsupported characters"));
}

#[test]
fn live_query_notify_channel_rejects_over_pg_identifier_limit() {
    let tenant = "t".repeat(32);
    let table = "orders_snapshot_partitioned_2026_region_alpha";
    let err =
        build_live_query_notify_channel(Some(&tenant), table).expect_err("must reject overflow");
    assert!(err.contains("63 bytes"));
}

#[test]
fn live_query_notify_channel_prevents_tenant_table_collisions() {
    let a = build_live_query_notify_channel(Some("acme"), "eu_orders").expect("first channel");
    let b = build_live_query_notify_channel(Some("acme_eu"), "orders").expect("second channel");
    assert_ne!(a, b, "tenant/table tuples must map to distinct channels");
}

#[test]
fn live_query_notify_channel_allows_exact_pg_identifier_limit() {
    let table = "a".repeat(WS_PG_CHANNEL_MAX_BYTES - "qail_table_".len());
    let channel =
        build_live_query_notify_channel(None, &table).expect("exact 63-byte channel should pass");
    assert_eq!(channel.len(), WS_PG_CHANNEL_MAX_BYTES);
}

#[tokio::test]
async fn prune_finished_live_query_tasks_removes_finished_handles() {
    let mut tasks: std::collections::HashMap<String, tokio::task::JoinHandle<()>> =
        std::collections::HashMap::new();
    tasks.insert("finished".to_string(), tokio::spawn(async {}));
    tasks.insert(
        "running".to_string(),
        tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }),
    );

    tokio::task::yield_now().await;
    prune_finished_live_query_tasks(&mut tasks);
    assert_eq!(tasks.len(), 1);
    assert!(tasks.contains_key("running"));

    for (_, handle) in tasks.drain() {
        handle.abort();
    }
}

#[test]
fn ws_auth_headers_extracts_query_token_when_authorization_missing() {
    let headers = HeaderMap::new();
    let uri: Uri = "/ws?access_token=query-token".parse().expect("uri parse");

    let merged = auth_headers_for_ws(&headers, &uri, true);
    assert_eq!(
        merged.get(AUTHORIZATION).and_then(|v| v.to_str().ok()),
        Some("Bearer query-token")
    );
}

#[test]
fn ws_auth_headers_ignores_query_token_when_disabled() {
    let headers = HeaderMap::new();
    let uri: Uri = "/ws?access_token=query-token".parse().expect("uri parse");

    let merged = auth_headers_for_ws(&headers, &uri, false);
    assert!(merged.get(AUTHORIZATION).is_none());
}

#[test]
fn ws_auth_headers_keeps_existing_authorization_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer header-token"),
    );
    let uri: Uri = "/ws?access_token=query-token".parse().expect("uri parse");

    let merged = auth_headers_for_ws(&headers, &uri, false);
    assert_eq!(
        merged.get(AUTHORIZATION).and_then(|v| v.to_str().ok()),
        Some("Bearer header-token")
    );
}
