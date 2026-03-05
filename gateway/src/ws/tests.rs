use super::*;

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
