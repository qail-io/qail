//! WebSocket subscription handler
//!
//! Provides real-time data subscriptions via PostgreSQL LISTEN/NOTIFY.

use axum::{
    extract::{State, ws::WebSocketUpgrade},
    http::HeaderMap,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use crate::GatewayState;
use crate::auth::{ensure_request_auth, extract_auth_for_state};

mod listener;
mod message;
mod session;

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
mod tests;

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

    ws.on_upgrade(move |socket| session::handle_socket(socket, state, auth))
        .into_response()
}
