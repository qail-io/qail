//! WebSocket subscription handler
//!
//! Provides real-time data subscriptions via PostgreSQL LISTEN/NOTIFY.

use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::HeaderMap,
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use crate::GatewayState;
use crate::auth::{ensure_request_auth, ensure_tenant_rate_limit, extract_auth_for_state};

/// Messages sent from the WebSocket client to the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WsClientMessage {
    /// Subscribe to a PostgreSQL NOTIFY channel.
    #[serde(rename = "subscribe")]
    Subscribe {
        /// Channel name to subscribe to (scoped per tenant).
        channel: String,
    },

    /// Unsubscribe from a previously subscribed channel.
    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        /// Channel name to unsubscribe from.
        channel: String,
    },

    /// Execute a one-shot Qail query over the WebSocket.
    #[serde(rename = "query")]
    Query {
        /// Qail query string (e.g., `"get orders"`).
        qail: String,
    },

    /// Live query: execute query now, then re-execute on table changes
    #[serde(rename = "live_query")]
    LiveQuery {
        /// Qail query string (e.g., "get orders")
        qail: String,
        /// Table to watch for changes (auto-subscribes to PG NOTIFY)
        table: String,
        /// Fallback polling interval in ms (0 = NOTIFY only)
        #[serde(default)]
        interval_ms: u64,
    },

    /// Stop a live query
    #[serde(rename = "stop_live_query")]
    StopLiveQuery {
        /// Table whose live query should be stopped.
        table: String,
    },

    /// Keep-alive ping.
    #[serde(rename = "ping")]
    Ping,
}

/// Messages sent from the server to the WebSocket client.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum WsServerMessage {
    /// Acknowledgement that a channel subscription was created.
    #[serde(rename = "subscribed")]
    Subscribed {
        /// The subscribed channel name.
        channel: String,
    },

    /// Acknowledgement that a channel subscription was removed.
    #[serde(rename = "unsubscribed")]
    Unsubscribed {
        /// The unsubscribed channel name.
        channel: String,
    },

    /// A PostgreSQL NOTIFY event on a subscribed channel.
    #[serde(rename = "notification")]
    Notification {
        /// Channel that triggered the notification.
        channel: String,
        /// Notification payload string.
        payload: String,
    },

    /// Query result returned in response to a `Query` message.
    #[serde(rename = "result")]
    Result {
        /// Rows returned by the query.
        rows: Vec<serde_json::Value>,
        /// Number of rows returned.
        count: usize,
    },

    /// Error message sent to the client.
    #[serde(rename = "error")]
    Error {
        /// Human-readable error description.
        message: String,
    },

    /// Live query update — pushed when subscribed query data changes
    #[serde(rename = "live_query_update")]
    LiveQueryUpdate {
        /// Table being watched.
        table: String,
        /// Current result set.
        rows: Vec<serde_json::Value>,
        /// Number of rows in this snapshot.
        count: usize,
        /// Monotonically increasing sequence number
        seq: u64,
    },

    /// Keep-alive pong response.
    #[serde(rename = "pong")]
    Pong,
}

const WS_ERR_DB_UNAVAILABLE: &str = "Database connection unavailable";
const WS_ERR_DB_UNAVAILABLE_UNSUBSCRIBE: &str = "Database connection unavailable for unsubscribe";
const WS_ERR_LIVE_QUERY_SUB_FAILED: &str = "Live query subscription failed";
const WS_ERR_LIVE_QUERY_STOPPED_DB_UNAVAILABLE: &str = "Live query stopped: database unavailable";
const WS_ERR_LIVE_QUERY_UNSUB_FAILED: &str = "Live query unsubscribe failed";
const WS_OUTBOX_CAPACITY: usize = 32;
const WS_MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 50;
const WS_MIN_LIVE_QUERY_INTERVAL_MS: u64 = 1000;
const WS_MAX_MESSAGE_BYTES: usize = 64 * 1024;
const WS_LISTENER_RETRY_MS: u64 = 500;
const WS_LISTENER_CMD_TIMEOUT_MS: u64 = 3000;
const WS_LISTENER_UNAVAILABLE_NOTICE_MS: u64 = 5000;
const WS_NOTIFY_QUEUE_CAPACITY: usize = 256;
const WS_NOTIFY_DROP_NOTICE_MS: u64 = 5000;

enum ListenControl {
    Listen {
        channel: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Unlisten {
        channel: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Shutdown,
}

#[derive(Debug, Clone)]
struct ListenerNotification {
    channel: String,
    payload: String,
}

struct WsConnectionState {
    subscribed_channels: Vec<String>,
    manual_subscriptions: std::collections::HashSet<String>,
    channel_refcounts: std::collections::HashMap<String, usize>,
    live_query_tasks: std::collections::HashMap<String, tokio::task::JoinHandle<()>>,
    live_query_triggers: std::collections::HashMap<String, mpsc::Sender<()>>,
    live_query_channels: std::collections::HashMap<String, String>,
}

fn tracked_channel_count(conn_state: &WsConnectionState) -> usize {
    conn_state.channel_refcounts.len()
}

fn increment_channel_refcount(conn_state: &mut WsConnectionState, channel: &str) {
    let entry = conn_state
        .channel_refcounts
        .entry(channel.to_string())
        .or_insert(0);
    *entry = entry.saturating_add(1);
    if *entry == 1 {
        conn_state.subscribed_channels.push(channel.to_string());
    }
}

fn decrement_channel_refcount(conn_state: &mut WsConnectionState, channel: &str) -> bool {
    match conn_state.channel_refcounts.get_mut(channel) {
        Some(count) if *count > 1 => {
            *count -= 1;
            false
        }
        Some(_) => {
            conn_state.channel_refcounts.remove(channel);
            conn_state.subscribed_channels.retain(|c| c != channel);
            true
        }
        None => false,
    }
}

fn dispatch_live_query_update(
    tx: &mpsc::Sender<WsServerMessage>,
    table: &str,
    rows: Vec<serde_json::Value>,
    count: usize,
    seq: u64,
) -> bool {
    match tx.try_send(WsServerMessage::LiveQueryUpdate {
        table: table.to_string(),
        rows,
        count,
        seq,
    }) {
        Ok(_) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!(
                table = %table,
                seq,
                "Dropping live query update: outbound WebSocket queue is full"
            );
            true
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
    }
}

#[cfg(test)]
fn finalize_live_query_listen(
    listen_ok: bool,
    subscribed_channels: &mut Vec<String>,
    notify_channel: &str,
) -> Option<WsServerMessage> {
    if listen_ok {
        if !subscribed_channels.iter().any(|c| c == notify_channel) {
            subscribed_channels.push(notify_channel.to_string());
        }
        None
    } else {
        Some(WsServerMessage::Error {
            message: WS_ERR_LIVE_QUERY_SUB_FAILED.to_string(),
        })
    }
}

async fn release_listener_conn(conn: &mut Option<qail_pg::PooledConnection>) {
    if let Some(mut c) = conn.take() {
        if let Err(e) = c.unlisten_all().await {
            tracing::debug!("WS listener cleanup UNLISTEN failed: {}", e);
        }
        c.release().await;
    }
}

async fn ensure_listener_conn(
    state: &Arc<GatewayState>,
    conn: &mut Option<qail_pg::PooledConnection>,
    channels: &std::collections::HashSet<String>,
) -> Result<(), String> {
    if conn.is_some() || channels.is_empty() {
        return Ok(());
    }

    let mut c = state
        .acquire_system_guarded("ws_listener", None)
        .await
        .map_err(|_| "Database connection unavailable".to_string())?;

    for channel in channels {
        if let Err(e) = c.listen(channel).await {
            c.release().await;
            return Err(format!("LISTEN failed: {}", e));
        }
    }
    *conn = Some(c);
    Ok(())
}

async fn listener_rpc(
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    make_cmd: impl FnOnce(oneshot::Sender<Result<(), String>>) -> ListenControl,
) -> Result<(), String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    listener_tx
        .send(make_cmd(reply_tx))
        .map_err(|_| "Notification worker unavailable".to_string())?;

    match tokio::time::timeout(
        std::time::Duration::from_millis(WS_LISTENER_CMD_TIMEOUT_MS),
        reply_rx,
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err("Notification worker unavailable".to_string()),
        Err(_) => Err("Notification worker timeout".to_string()),
    }
}

async fn run_listener_session(
    state: Arc<GatewayState>,
    tx: mpsc::Sender<WsServerMessage>,
    notify_tx: mpsc::Sender<ListenerNotification>,
    mut control_rx: mpsc::UnboundedReceiver<ListenControl>,
) {
    let mut conn: Option<qail_pg::PooledConnection> = None;
    let mut channels: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut last_unavailable_notice: Option<std::time::Instant> = None;
    let mut last_notify_drop_notice: Option<std::time::Instant> = None;

    loop {
        if conn.is_none()
            && !channels.is_empty()
            && let Err(e) = ensure_listener_conn(&state, &mut conn, &channels).await
        {
            tracing::warn!("WS listener reconnect failed: {}", e);
            let should_notify = last_unavailable_notice.is_none_or(|t| {
                t.elapsed() >= std::time::Duration::from_millis(WS_LISTENER_UNAVAILABLE_NOTICE_MS)
            });
            if should_notify {
                let _ = tx.try_send(WsServerMessage::Error {
                    message: "Notification channel unavailable".to_string(),
                });
                last_unavailable_notice = Some(std::time::Instant::now());
            }
            tokio::select! {
                cmd = control_rx.recv() => {
                    match cmd {
                        Some(ListenControl::Listen { channel, reply }) => {
                            if channels.contains(&channel) {
                                let _ = reply.send(Ok(()));
                            } else {
                                let _ = reply.send(Err("Notification channel unavailable".to_string()));
                            }
                        }
                        Some(ListenControl::Unlisten { channel, reply }) => {
                            channels.remove(&channel);
                            let _ = reply.send(Ok(()));
                        }
                        Some(ListenControl::Shutdown) | None => break,
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(WS_LISTENER_RETRY_MS)) => {}
            }
            continue;
        }
        if conn.is_some() {
            last_unavailable_notice = None;
        }

        if let Some(c) = conn.as_mut() {
            tokio::select! {
                cmd = control_rx.recv() => {
                    match cmd {
                        Some(ListenControl::Listen { channel, reply }) => {
                            if channels.contains(&channel) {
                                let _ = reply.send(Ok(()));
                                continue;
                            }
                            if channels.len() >= WS_MAX_SUBSCRIPTIONS_PER_CONNECTION {
                                let _ = reply.send(Err(format!(
                                    "Too many subscriptions (max {})",
                                    WS_MAX_SUBSCRIPTIONS_PER_CONNECTION
                                )));
                                continue;
                            }
                            channels.insert(channel.clone());
                            if let Err(e) = c.listen(&channel).await {
                                channels.remove(&channel);
                                tracing::warn!("WS listener LISTEN failed: {}", e);
                                let _ = reply.send(Err("Subscribe failed".to_string()));
                            } else {
                                let _ = reply.send(Ok(()));
                            }
                        }
                        Some(ListenControl::Unlisten { channel, reply }) => {
                            channels.remove(&channel);
                            if let Err(e) = c.unlisten(&channel).await {
                                tracing::warn!("WS listener UNLISTEN failed: {}", e);
                                let _ = reply.send(Err("Unsubscribe failed".to_string()));
                            } else {
                                let _ = reply.send(Ok(()));
                            }
                            if channels.is_empty() {
                                release_listener_conn(&mut conn).await;
                            }
                        }
                        Some(ListenControl::Shutdown) | None => break,
                    }
                }
                result = c.recv_notification() => {
                    match result {
                        Ok(notification) => {
                            let channel = notification.channel;
                            if channels.contains(&channel) {
                                match notify_tx.try_send(ListenerNotification {
                                    channel: channel.clone(),
                                    payload: notification.payload,
                                }) {
                                    Ok(()) => {}
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        let should_log = last_notify_drop_notice.is_none_or(|t| {
                                            t.elapsed() >= std::time::Duration::from_millis(WS_NOTIFY_DROP_NOTICE_MS)
                                        });
                                        if should_log {
                                            tracing::warn!(
                                                channel = %channel,
                                                "Dropping WS notification: internal queue is full"
                                            );
                                            last_notify_drop_notice = Some(std::time::Instant::now());
                                        }
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("WS listener recv_notification failed: {}", e);
                            release_listener_conn(&mut conn).await;
                            if channels.is_empty() {
                                tokio::time::sleep(std::time::Duration::from_millis(WS_LISTENER_RETRY_MS)).await;
                            }
                        }
                    }
                }
            }
        } else {
            match control_rx.recv().await {
                Some(ListenControl::Listen { channel, reply }) => {
                    if channels.contains(&channel) {
                        let _ = reply.send(Ok(()));
                        continue;
                    }
                    if channels.len() >= WS_MAX_SUBSCRIPTIONS_PER_CONNECTION {
                        let _ = reply.send(Err(format!(
                            "Too many subscriptions (max {})",
                            WS_MAX_SUBSCRIPTIONS_PER_CONNECTION
                        )));
                        continue;
                    }
                    channels.insert(channel.clone());
                    match ensure_listener_conn(&state, &mut conn, &channels).await {
                        Ok(()) => {
                            let _ = reply.send(Ok(()));
                        }
                        Err(_) => {
                            channels.remove(&channel);
                            let _ = reply.send(Err("Subscribe failed".to_string()));
                        }
                    }
                }
                Some(ListenControl::Unlisten { channel, reply }) => {
                    channels.remove(&channel);
                    let _ = reply.send(Ok(()));
                }
                Some(ListenControl::Shutdown) | None => break,
            }
        }
    }

    release_listener_conn(&mut conn).await;
}

fn prune_finished_live_query_tasks(
    live_query_tasks: &mut std::collections::HashMap<String, tokio::task::JoinHandle<()>>,
) {
    live_query_tasks.retain(|table, handle| {
        let keep = !handle.is_finished();
        if !keep {
            tracing::debug!(
                table = %table,
                "Pruning finished LiveQuery poller from tracking map"
            );
        }
        keep
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_query_listen_success_registers_channel() {
        let mut subscribed = vec!["tenant_qail_table_orders".to_string()];
        let result =
            finalize_live_query_listen(true, &mut subscribed, "tenant_qail_table_invoices");
        assert!(result.is_none());
        assert!(subscribed.contains(&"tenant_qail_table_orders".to_string()));
        assert!(subscribed.contains(&"tenant_qail_table_invoices".to_string()));
    }

    #[test]
    fn live_query_listen_failure_returns_error_without_registration() {
        let mut subscribed = vec!["tenant_qail_table_orders".to_string()];
        let result =
            finalize_live_query_listen(false, &mut subscribed, "tenant_qail_table_invoices");
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
        let msg =
            finalize_stop_live_query_unlisten(true, &mut subscribed, "tenant_qail_table_orders");
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
        let msg =
            finalize_stop_live_query_unlisten(false, &mut subscribed, "tenant_qail_table_orders");
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
}

#[cfg(test)]
fn finalize_stop_live_query_unlisten(
    unlisten_ok: bool,
    subscribed_channels: &mut Vec<String>,
    notify_channel: &str,
) -> WsServerMessage {
    // Keep local tracking consistent even if backend UNLISTEN fails.
    subscribed_channels.retain(|c| c != notify_channel);
    if unlisten_ok {
        WsServerMessage::Unsubscribed {
            channel: notify_channel.to_string(),
        }
    } else {
        WsServerMessage::Error {
            message: WS_ERR_LIVE_QUERY_UNSUB_FAILED.to_string(),
        }
    }
}

/// Axum handler that upgrades an HTTP request to a WebSocket connection.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = extract_auth_for_state(&headers, state.as_ref()).await;

    // SECURITY (P0-3): Enforce authentication policy on WS upgrade.
    if let Err(e) = ensure_request_auth(&auth, state.config.production_strict) {
        return e.into_response();
    }

    // SECURITY: Validate Origin header against CORS allowed origins.
    // Prevents cross-site WebSocket hijacking from malicious pages.
    if !state.config.cors_allowed_origins.is_empty() {
        let origin_ok = headers
            .get("origin")
            .and_then(|v| v.to_str().ok())
            .map(|origin| {
                state
                    .config
                    .cors_allowed_origins
                    .iter()
                    .any(|allowed| allowed == origin)
            })
            .unwrap_or(false);
        if !origin_ok {
            return crate::middleware::ApiError::forbidden(
                "WebSocket Origin not in allowed origins",
            )
            .into_response();
        }
    }

    tracing::info!("WebSocket connection from user: {}", auth.user_id);

    ws.on_upgrade(move |socket| handle_socket(socket, state, auth))
        .into_response()
}

async fn handle_socket(
    socket: WebSocket,
    state: Arc<GatewayState>,
    auth: crate::auth::AuthContext,
) {
    let user_id = auth.user_id.clone();
    let (mut sender, mut receiver) = socket.split();

    let (tx, mut rx) = mpsc::channel::<WsServerMessage>(WS_OUTBOX_CAPACITY);
    let (notify_tx, mut notify_rx) =
        mpsc::channel::<ListenerNotification>(WS_NOTIFY_QUEUE_CAPACITY);
    let (listener_tx, listener_rx) = mpsc::unbounded_channel::<ListenControl>();
    let mut listener_task = tokio::spawn(run_listener_session(
        Arc::clone(&state),
        tx.clone(),
        notify_tx,
        listener_rx,
    ));

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let text = match serde_json::to_string(&msg) {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!("Failed to serialize WS message: {}", e);
                    continue;
                }
            };

            if sender.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    let mut conn_state = WsConnectionState {
        subscribed_channels: Vec::new(),
        manual_subscriptions: std::collections::HashSet::new(),
        channel_refcounts: std::collections::HashMap::new(),
        live_query_tasks: std::collections::HashMap::new(),
        live_query_triggers: std::collections::HashMap::new(),
        live_query_channels: std::collections::HashMap::new(),
    };

    loop {
        // Keep task tracking bounded when pollers exit due transient failures.
        prune_finished_live_query_tasks(&mut conn_state.live_query_tasks);
        let stale_tables: Vec<String> = conn_state
            .live_query_channels
            .keys()
            .filter(|table| !conn_state.live_query_tasks.contains_key(*table))
            .cloned()
            .collect();
        for table in stale_tables {
            conn_state.live_query_triggers.remove(&table);
            if let Some(channel) = conn_state.live_query_channels.remove(&table)
                && decrement_channel_refcount(&mut conn_state, &channel)
                && let Err(e) = listener_rpc(&listener_tx, |reply| ListenControl::Unlisten {
                    channel: channel.clone(),
                    reply,
                })
                .await
            {
                tracing::warn!(
                    table = %table,
                    channel = %channel,
                    "WS stale LiveQuery UNLISTEN failed: {}",
                    e
                );
            }
        }

        tokio::select! {
            maybe_msg = receiver.next() => {
                let Some(msg_result) = maybe_msg else {
                    break;
                };
                let Ok(msg) = msg_result else {
                    break;
                };
                match msg {
                    Message::Text(text) => {
                        if text.len() > WS_MAX_MESSAGE_BYTES {
                            let _ = tx
                                .send(WsServerMessage::Error {
                                    message: format!(
                                        "Message too large (max {} bytes)",
                                        WS_MAX_MESSAGE_BYTES
                                    ),
                                })
                                .await;
                            continue;
                        }
                        let text_str = text.to_string();
                        match serde_json::from_str::<WsClientMessage>(&text_str) {
                            Ok(client_msg) => {
                                handle_client_message(
                                    client_msg,
                                    &state,
                                    &tx,
                                    &listener_tx,
                                    &auth,
                                    &mut conn_state,
                                )
                                .await;
                            }
                            Err(e) => {
                                let _ = tx
                                    .send(WsServerMessage::Error {
                                        message: format!("Invalid message: {}", e),
                                    })
                                    .await;
                            }
                        }
                    }
                    Message::Close(_) => {
                        tracing::debug!("WebSocket closed by client: {}", user_id);
                        break;
                    }
                    _ => {}
                }
            }
            maybe_notification = notify_rx.recv() => {
                let Some(notification) = maybe_notification else {
                    break;
                };
                match tx.try_send(WsServerMessage::Notification {
                    channel: notification.channel.clone(),
                    payload: notification.payload,
                }) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        tracing::warn!(
                            channel = %notification.channel,
                            "Dropping WS notification: outbound queue is full"
                        );
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                }

                let mut stale_tables = Vec::new();
                for (table, channel) in &conn_state.live_query_channels {
                    if channel == &notification.channel
                        && let Some(trigger_tx) = conn_state.live_query_triggers.get(table)
                    {
                        match trigger_tx.try_send(()) {
                            Ok(()) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                // Coalesce bursts: one pending trigger is enough.
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                stale_tables.push(table.clone());
                            }
                        }
                    }
                }
                for table in stale_tables {
                    conn_state.live_query_triggers.remove(&table);
                    if let Some(channel) = conn_state.live_query_channels.remove(&table)
                        && decrement_channel_refcount(&mut conn_state, &channel)
                        && let Err(e) = listener_rpc(&listener_tx, |reply| ListenControl::Unlisten {
                            channel: channel.clone(),
                            reply,
                        })
                        .await
                    {
                        tracing::warn!(
                            table = %table,
                            channel = %channel,
                            "WS stale-trigger UNLISTEN failed: {}",
                            e
                        );
                    }
                    if let Some(handle) = conn_state.live_query_tasks.remove(&table) {
                        handle.abort();
                    }
                }
            }
        }
    }

    // SECURITY (P0-R3): Abort all spawned LiveQuery polling tasks on disconnect.
    for (table, handle) in conn_state.live_query_tasks.drain() {
        tracing::debug!(
            "Aborting LiveQuery poller for table '{}' on disconnect",
            table
        );
        handle.abort();
    }
    conn_state.live_query_triggers.clear();
    conn_state.live_query_channels.clear();

    let _ = listener_tx.send(ListenControl::Shutdown);
    match tokio::time::timeout(std::time::Duration::from_secs(2), &mut listener_task).await {
        Ok(_) => {}
        Err(_) => {
            tracing::warn!("WS listener task did not shutdown in time");
            listener_task.abort();
        }
    }

    send_task.abort();
    tracing::info!("WebSocket disconnected: {}", user_id);
}

/// Handle a client message
async fn handle_client_message(
    msg: WsClientMessage,
    state: &Arc<GatewayState>,
    tx: &mpsc::Sender<WsServerMessage>,
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    auth: &crate::auth::AuthContext,
    conn_state: &mut WsConnectionState,
) {
    let user_id = &auth.user_id;

    // SECURITY (P0-5): Post-auth tenant rate limiting on ALL WS messages
    // to prevent LISTEN/UNLISTEN flood attacks, not just data-bearing messages.
    if let Err(e) = ensure_tenant_rate_limit(state.as_ref(), auth).await {
        let _ = tx
            .send(WsServerMessage::Error {
                message: e.message.clone(),
            })
            .await;
        return;
    }

    match msg {
        WsClientMessage::Subscribe { channel } => {
            tracing::debug!("User {} subscribing to channel: {}", user_id, channel);

            // SECURITY: Scope channels to tenant to prevent cross-tenant eavesdropping.
            // Without this, any user could LISTEN to "qail_table_orders" and receive
            // ALL tenants' notifications.
            let tenant_id = match &auth.tenant_id {
                Some(tid) if !tid.is_empty() => tid,
                _ => {
                    let _ = tx
                        .send(WsServerMessage::Error {
                            message: "Subscribe requires authenticated tenant context".to_string(),
                        })
                        .await;
                    return;
                }
            };

            // Validate channel name: alphanumeric + underscores only
            if !channel.chars().all(|c| c.is_alphanumeric() || c == '_') {
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: "Invalid channel name — alphanumeric and underscores only"
                            .to_string(),
                    })
                    .await;
                return;
            }

            // Prefix with tenant_id to isolate notifications per tenant
            let scoped_channel = format!("{}_{}", tenant_id, channel);

            // Idempotent for manual subscriptions: don't increment ownership twice.
            if conn_state.manual_subscriptions.contains(&scoped_channel) {
                let _ = tx.send(WsServerMessage::Subscribed { channel }).await;
                return;
            }

            let need_listen = !conn_state.channel_refcounts.contains_key(&scoped_channel);
            if need_listen {
                // Cap unique LISTEN channels per socket to prevent channel exhaustion.
                if tracked_channel_count(conn_state) >= WS_MAX_SUBSCRIPTIONS_PER_CONNECTION {
                    let _ = tx
                        .send(WsServerMessage::Error {
                            message: format!(
                                "Too many subscriptions (max {})",
                                WS_MAX_SUBSCRIPTIONS_PER_CONNECTION
                            ),
                        })
                        .await;
                    return;
                }

                if let Err(e) = listener_rpc(listener_tx, |reply| ListenControl::Listen {
                    channel: scoped_channel.clone(),
                    reply,
                })
                .await
                {
                    tracing::warn!("WS Subscribe failed: {}", e);
                    let _ = tx
                        .send(WsServerMessage::Error {
                            message: WS_ERR_DB_UNAVAILABLE.to_string(),
                        })
                        .await;
                    return;
                }
            }

            conn_state
                .manual_subscriptions
                .insert(scoped_channel.clone());
            increment_channel_refcount(conn_state, &scoped_channel);
            let _ = tx.send(WsServerMessage::Subscribed { channel }).await;
        }

        WsClientMessage::Unsubscribe { channel } => {
            tracing::debug!("User {} unsubscribing from channel: {}", user_id, channel);

            // Reconstruct the scoped channel name
            let scoped_channel = match &auth.tenant_id {
                Some(tid) if !tid.is_empty() => format!("{}_{}", tid, channel),
                _ => {
                    let _ = tx
                        .send(WsServerMessage::Error {
                            message: "Unsubscribe requires authenticated tenant context"
                                .to_string(),
                        })
                        .await;
                    return;
                }
            };

            // Idempotent unsubscribe for non-manual channels.
            if !conn_state.manual_subscriptions.remove(&scoped_channel) {
                let _ = tx.send(WsServerMessage::Unsubscribed { channel }).await;
                return;
            }

            if decrement_channel_refcount(conn_state, &scoped_channel) {
                match listener_rpc(listener_tx, |reply| ListenControl::Unlisten {
                    channel: scoped_channel.clone(),
                    reply,
                })
                .await
                {
                    Ok(()) => {
                        let _ = tx.send(WsServerMessage::Unsubscribed { channel }).await;
                    }
                    Err(e) => {
                        tracing::warn!("WS Unsubscribe failed: {}", e);
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: WS_ERR_DB_UNAVAILABLE_UNSUBSCRIBE.to_string(),
                            })
                            .await;
                    }
                }
            } else {
                let _ = tx.send(WsServerMessage::Unsubscribed { channel }).await;
            }
        }

        WsClientMessage::Query { qail } => {
            tracing::debug!("User {} executing query: {}", user_id, qail);

            match qail_core::parser::parse(&qail) {
                Ok(mut cmd) => {
                    // SECURITY (P0-2): Reject dangerous actions on WS.
                    if matches!(
                        cmd.action,
                        qail_core::ast::Action::Call
                            | qail_core::ast::Action::Do
                            | qail_core::ast::Action::SessionSet
                            | qail_core::ast::Action::SessionShow
                            | qail_core::ast::Action::SessionReset
                    ) {
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: format!(
                                    "Action {:?} is not allowed on WebSocket",
                                    cmd.action
                                ),
                            })
                            .await;
                        return;
                    }

                    // SECURITY: Enforce allow-list — same as HTTP handler.
                    // Without this, WS is a bypass channel for allow-list restrictions.
                    if !crate::handler::is_query_allowed(&state.allow_list, Some(&qail), &cmd) {
                        tracing::warn!("WS query rejected by allow-list: {}", qail);
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: "Query not in allow-list".to_string(),
                            })
                            .await;
                        return;
                    }

                    // SECURITY (G3): Apply row-level security policies — same as HTTP handler.
                    // Without this, WS queries bypass all PolicyEngine filters.
                    if let Err(e) = state.policy_engine.apply_policies(auth, &mut cmd) {
                        tracing::warn!("WS policy error: {}", e);
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: "Access denied by policy".to_string(),
                            })
                            .await;
                        return;
                    }
                    // SECURITY: Clamp LIMIT at AST level so PostgreSQL stops scanning early.
                    crate::handler::clamp_query_limit(&mut cmd, state.config.max_result_rows);

                    // SECURITY: Query complexity guard — same as HTTP /qail handler.
                    let (depth, filters, joins) = crate::handler::query::query_complexity(&cmd);
                    if let Err(_api_err) = state.complexity_guard.check(depth, filters, joins) {
                        tracing::warn!(
                            table = %cmd.table,
                            depth, filters, joins,
                            "WS query rejected by complexity guard"
                        );
                        crate::metrics::record_complexity_rejected();
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: "Query too complex".to_string(),
                            })
                            .await;
                        return;
                    }

                    // WS queries use RLS-scoped connections with statement/lock timeouts
                    if let Ok(mut conn) = state
                        .acquire_with_auth_rls_guarded(auth, Some(&cmd.table))
                        .await
                    {
                        match conn.fetch_all_uncached(&cmd).await {
                            Ok(rows) => {
                                let json_rows: Vec<serde_json::Value> =
                                    rows.iter().map(crate::handler::row_to_json).collect();

                                // SECURITY (G4): Verify tenant boundary — fail-closed.
                                if let Some(ref tenant_id) = auth.tenant_id
                                    && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                                        &json_rows,
                                        tenant_id,
                                        &state.config.tenant_column,
                                        &cmd.table,
                                        "ws_query",
                                    )
                                {
                                    tracing::error!("{}", v);
                                    conn.release().await;
                                    let _ = tx
                                        .send(WsServerMessage::Error {
                                            message: "Data integrity error".to_string(),
                                        })
                                        .await;
                                    return;
                                }

                                let count = json_rows.len();
                                conn.release().await;
                                let _ = tx
                                    .send(WsServerMessage::Result {
                                        rows: json_rows,
                                        count,
                                    })
                                    .await;
                            }
                            Err(e) => {
                                // SECURITY: Do not leak raw PG error to WS client
                                tracing::error!("WS query error: {}", e);
                                conn.release().await;
                                let _ = tx
                                    .send(WsServerMessage::Error {
                                        message: "Query execution failed".to_string(),
                                    })
                                    .await;
                            }
                        }
                    } else {
                        // SECURITY: Inform client instead of silently dropping the query.
                        tracing::warn!("WS query: pool acquire failed");
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: "Database connection unavailable".to_string(),
                            })
                            .await;
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(WsServerMessage::Error {
                            message: format!("Parse error: {}", e),
                        })
                        .await;
                }
            }
        }

        WsClientMessage::Ping => {
            let _ = tx.send(WsServerMessage::Pong).await;
        }

        WsClientMessage::LiveQuery {
            qail,
            table,
            interval_ms,
        } => {
            tracing::info!("User {} starting live query on table: {}", user_id, table);

            // Parse the query
            let mut cmd = match qail_core::parser::parse(&qail) {
                Ok(cmd) => cmd,
                Err(e) => {
                    let _ = tx
                        .send(WsServerMessage::Error {
                            message: format!("Parse error: {}", e),
                        })
                        .await;
                    return;
                }
            };

            // SECURITY (P0-2): Reject dangerous actions on WS LiveQuery.
            if matches!(
                cmd.action,
                qail_core::ast::Action::Call
                    | qail_core::ast::Action::Do
                    | qail_core::ast::Action::SessionSet
                    | qail_core::ast::Action::SessionShow
                    | qail_core::ast::Action::SessionReset
            ) {
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: format!("Action {:?} is not allowed on WebSocket", cmd.action),
                    })
                    .await;
                return;
            }

            // SECURITY (E5): Validate table name against schema registry.
            if state.schema.table(&table).is_none() {
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: format!(
                            "Unknown table: '{}'. Live queries require a valid table name.",
                            table
                        ),
                    })
                    .await;
                return;
            }

            // SECURITY: Enforce allow-list — same as HTTP and WS Query handlers.
            if !crate::handler::is_query_allowed(&state.allow_list, Some(&qail), &cmd) {
                tracing::warn!("WS LiveQuery rejected by allow-list: {}", qail);
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: "Query not in allow-list".to_string(),
                    })
                    .await;
                return;
            }

            // SECURITY (R1): Apply row-level security policies — same as HTTP handler.
            // Without this, LiveQuery bypasses all PolicyEngine filters.
            if let Err(e) = state.policy_engine.apply_policies(auth, &mut cmd) {
                tracing::warn!("WS LiveQuery policy error: {}", e);
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: "Access denied by policy".to_string(),
                    })
                    .await;
                return;
            }

            // SECURITY: Clamp LIMIT at AST level so PostgreSQL stops scanning early.
            crate::handler::clamp_query_limit(&mut cmd, state.config.max_result_rows);

            // SECURITY: Query complexity guard — same as HTTP /qail handler.
            let (depth, filters, joins) = crate::handler::query::query_complexity(&cmd);
            if let Err(_api_err) = state.complexity_guard.check(depth, filters, joins) {
                tracing::warn!(
                    table = %cmd.table,
                    depth, filters, joins,
                    "WS LiveQuery rejected by complexity guard"
                );
                crate::metrics::record_complexity_rejected();
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: "Query too complex".to_string(),
                    })
                    .await;
                return;
            }

            // Execute immediately and send initial snapshot
            if let Ok(mut conn) = state
                .acquire_with_auth_rls_guarded(auth, Some(&cmd.table))
                .await
            {
                match conn.fetch_all_uncached(&cmd).await {
                    Ok(rows) => {
                        let json_rows: Vec<serde_json::Value> =
                            rows.iter().map(crate::handler::row_to_json).collect();

                        // SECURITY (R2): Verify tenant boundary — fail-closed.
                        if let Some(ref tenant_id) = auth.tenant_id
                            && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                                &json_rows,
                                tenant_id,
                                &state.config.tenant_column,
                                &cmd.table,
                                "ws_live_query",
                            )
                        {
                            tracing::error!("{}", v);
                            conn.release().await;
                            let _ = tx
                                .send(WsServerMessage::Error {
                                    message: "Data integrity error".to_string(),
                                })
                                .await;
                            return;
                        }

                        let count = json_rows.len();
                        conn.release().await;
                        let _ = tx
                            .send(WsServerMessage::LiveQueryUpdate {
                                table: table.clone(),
                                rows: json_rows,
                                count,
                                seq: 1,
                            })
                            .await;
                    }
                    Err(e) => {
                        // SECURITY (R3): Do not leak raw PG error to WS client
                        tracing::error!("Live query initial exec failed: {}", e);
                        conn.release().await;
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: "Live query execution failed".to_string(),
                            })
                            .await;
                        return;
                    }
                }
            } else {
                // SECURITY: Inform client instead of silently dropping the LiveQuery.
                tracing::warn!("WS LiveQuery: pool acquire failed");
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: "Database connection unavailable".to_string(),
                    })
                    .await;
                return;
            }

            // Subscribe to table's NOTIFY channel for change detection
            // SECURITY: prefix with tenant_id to isolate per-tenant
            let notify_channel = match &auth.tenant_id {
                Some(tid) if !tid.is_empty() => format!("{}_qail_table_{}", tid, table),
                _ => format!("qail_table_{}", table),
            };
            let previous_channel = conn_state.live_query_channels.get(&table).cloned();
            if let Some(prev_channel) = previous_channel.as_ref()
                && prev_channel != &notify_channel
                && decrement_channel_refcount(conn_state, prev_channel)
                && let Err(e) = listener_rpc(listener_tx, |reply| ListenControl::Unlisten {
                    channel: prev_channel.clone(),
                    reply,
                })
                .await
            {
                tracing::warn!(
                    table = %table,
                    channel = %prev_channel,
                    "WS LiveQuery replacement UNLISTEN failed: {}",
                    e
                );
            }
            let same_channel_replacement = previous_channel.as_ref() == Some(&notify_channel);

            if !same_channel_replacement {
                let need_listen = !conn_state.channel_refcounts.contains_key(&notify_channel);
                if need_listen {
                    if tracked_channel_count(conn_state) >= WS_MAX_SUBSCRIPTIONS_PER_CONNECTION {
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: format!(
                                    "LiveQuery subscription limit reached (max {})",
                                    WS_MAX_SUBSCRIPTIONS_PER_CONNECTION
                                ),
                            })
                            .await;
                        return;
                    }

                    if let Err(e) = listener_rpc(listener_tx, |reply| ListenControl::Listen {
                        channel: notify_channel.clone(),
                        reply,
                    })
                    .await
                    {
                        tracing::warn!("WS LiveQuery LISTEN failed: {}", e);
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: WS_ERR_LIVE_QUERY_SUB_FAILED.to_string(),
                            })
                            .await;
                        return;
                    }
                }
                increment_channel_refcount(conn_state, &notify_channel);
            }

            // SECURITY: Cap live-query worker count per connection.
            if conn_state.live_query_tasks.len() >= WS_MAX_SUBSCRIPTIONS_PER_CONNECTION
                && !conn_state.live_query_tasks.contains_key(&table)
            {
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: format!(
                            "LiveQuery limit reached (max {} per connection)",
                            WS_MAX_SUBSCRIPTIONS_PER_CONNECTION
                        ),
                    })
                    .await;
                return;
            }

            let poll_interval = if interval_ms > 0 {
                Some(std::time::Duration::from_millis(
                    interval_ms.max(WS_MIN_LIVE_QUERY_INTERVAL_MS),
                ))
            } else {
                None
            };
            let (trigger_tx, mut trigger_rx) = mpsc::channel::<()>(1);
            let tx_clone = tx.clone();
            let state_clone = Arc::clone(state);
            let table_clone = table.clone();
            let rls_ctx = auth.to_rls_context();
            let waiter_key = format!(
                "{}:{}",
                auth.tenant_id.as_deref().unwrap_or("_"),
                auth.user_id
            );
            let stmt_timeout = state.config.statement_timeout_ms;
            let lock_timeout = state.config.lock_timeout_ms;
            let tenant_id_clone = auth.tenant_id.clone();
            let tenant_col = state.config.tenant_column.clone();

            let handle = tokio::spawn(async move {
                let mut seq = 2u64;
                loop {
                    match poll_interval {
                        Some(interval) => {
                            tokio::select! {
                                _ = tokio::time::sleep(interval) => {}
                                trigger = trigger_rx.recv() => {
                                    if trigger.is_none() {
                                        break;
                                    }
                                }
                            }
                        }
                        None => {
                            if trigger_rx.recv().await.is_none() {
                                break;
                            }
                        }
                    }

                    // Burst coalescing: one refresh is enough for many notifications.
                    while trigger_rx.try_recv().is_ok() {}

                    if let Ok(mut conn) = state_clone
                        .acquire_with_rls_timeouts_guarded(
                            &waiter_key,
                            rls_ctx.clone(),
                            stmt_timeout,
                            lock_timeout,
                            Some(table_clone.as_str()),
                        )
                        .await
                    {
                        match conn.fetch_all_uncached(&cmd).await {
                            Ok(rows) => {
                                let json_rows: Vec<serde_json::Value> =
                                    rows.iter().map(crate::handler::row_to_json).collect();

                                // SECURITY: Verify tenant boundary on every update tick.
                                if let Some(ref tenant_id) = tenant_id_clone
                                    && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                                        &json_rows,
                                        tenant_id,
                                        &tenant_col,
                                        &table_clone,
                                        "ws_live_query_poll",
                                    )
                                {
                                    tracing::error!("{}", v);
                                    conn.release().await;
                                    let _ = tx_clone
                                        .send(WsServerMessage::Error {
                                            message: "Data integrity error".to_string(),
                                        })
                                        .await;
                                    break;
                                }

                                let count = json_rows.len();
                                conn.release().await;
                                if !dispatch_live_query_update(
                                    &tx_clone,
                                    &table_clone,
                                    json_rows,
                                    count,
                                    seq,
                                ) {
                                    break;
                                }
                                seq += 1;
                            }
                            Err(e) => {
                                tracing::warn!("Live query update failed: {}", e);
                                conn.release().await;
                            }
                        }
                    } else {
                        tracing::warn!("Live query update acquire failed");
                        let _ = tx_clone
                            .send(WsServerMessage::Error {
                                message: WS_ERR_LIVE_QUERY_STOPPED_DB_UNAVAILABLE.to_string(),
                            })
                            .await;
                        break;
                    }
                }
            });
            // SECURITY: Abort old worker before replacing (prevents task leak on re-subscribe).
            if let Some(old_handle) = conn_state.live_query_tasks.insert(table.clone(), handle) {
                old_handle.abort();
            }
            conn_state
                .live_query_triggers
                .insert(table.clone(), trigger_tx);
            conn_state
                .live_query_channels
                .insert(table.clone(), notify_channel.clone());
        }

        WsClientMessage::StopLiveQuery { table } => {
            // SECURITY (P0-R3): Abort the spawned polling task for this table.
            if let Some(handle) = conn_state.live_query_tasks.remove(&table) {
                tracing::debug!("Aborting LiveQuery poller for table '{}'", table);
                handle.abort();
            }
            conn_state.live_query_triggers.remove(&table);

            if let Some(notify_channel) = conn_state.live_query_channels.remove(&table) {
                if decrement_channel_refcount(conn_state, &notify_channel) {
                    match listener_rpc(listener_tx, |reply| ListenControl::Unlisten {
                        channel: notify_channel.clone(),
                        reply,
                    })
                    .await
                    {
                        Ok(()) => {
                            let _ = tx
                                .send(WsServerMessage::Unsubscribed {
                                    channel: notify_channel,
                                })
                                .await;
                        }
                        Err(e) => {
                            tracing::warn!("WS StopLiveQuery UNLISTEN failed: {}", e);
                            let _ = tx
                                .send(WsServerMessage::Error {
                                    message: WS_ERR_LIVE_QUERY_UNSUB_FAILED.to_string(),
                                })
                                .await;
                        }
                    }
                } else {
                    let _ = tx
                        .send(WsServerMessage::Unsubscribed {
                            channel: notify_channel,
                        })
                        .await;
                }
            } else {
                let fallback_channel = match &auth.tenant_id {
                    Some(tid) if !tid.is_empty() => format!("{}_qail_table_{}", tid, table),
                    _ => format!("qail_table_{}", table),
                };
                let _ = tx
                    .send(WsServerMessage::Unsubscribed {
                        channel: fallback_channel,
                    })
                    .await;
            }
        }
    }
}
