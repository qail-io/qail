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
use tokio::sync::mpsc;

use crate::GatewayState;
use crate::auth::extract_auth_from_headers;

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

/// Axum handler that upgrades an HTTP request to a WebSocket connection.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let mut auth = extract_auth_from_headers(&headers);
    auth.enrich_with_operator_map(&state.user_operator_map)
        .await;
    tracing::info!("WebSocket connection from user: {}", auth.user_id);

    ws.on_upgrade(move |socket| handle_socket(socket, state, auth))
}

async fn handle_socket(
    socket: WebSocket,
    state: Arc<GatewayState>,
    auth: crate::auth::AuthContext,
) {
    let user_id = auth.user_id.clone();
    let (mut sender, mut receiver) = socket.split();

    let (tx, mut rx) = mpsc::channel::<WsServerMessage>(32);

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

    let mut subscribed_channels: Vec<String> = Vec::new();

    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(text) => {
                let text_str = text.to_string();
                match serde_json::from_str::<WsClientMessage>(&text_str) {
                    Ok(client_msg) => {
                        handle_client_message(
                            client_msg,
                            &state,
                            &tx,
                            &auth,
                            &mut subscribed_channels,
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

    // Cleanup: UNLISTEN all channels
    if !subscribed_channels.is_empty()
        && let Ok(mut conn) = state.pool.acquire_system().await
    {
        for channel in &subscribed_channels {
            let cmd = qail_core::ast::Qail::unlisten(channel);
            let _ = conn.fetch_all_uncached(&cmd).await;
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
    auth: &crate::auth::AuthContext,
    subscribed_channels: &mut Vec<String>,
) {
    let user_id = &auth.user_id;
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

            // R7-C: Cap subscriptions per WebSocket client to prevent LISTEN exhaustion
            if subscribed_channels.len() >= 50 {
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: "Too many subscriptions (max 50)".to_string(),
                    })
                    .await;
                return;
            }

            if let Ok(mut conn) = state.pool.acquire_system().await {
                let cmd = qail_core::ast::Qail::listen(&scoped_channel);
                match conn.fetch_all_uncached(&cmd).await {
                    Ok(_) => {
                        subscribed_channels.push(scoped_channel);
                        let _ = tx.send(WsServerMessage::Subscribed { channel }).await;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: format!("Subscribe failed: {}", e),
                            })
                            .await;
                    }
                }
            }
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

            if let Ok(mut conn) = state.pool.acquire_system().await {
                let cmd = qail_core::ast::Qail::unlisten(&scoped_channel);
                match conn.fetch_all_uncached(&cmd).await {
                    Ok(_) => {
                        subscribed_channels.retain(|c| c != &scoped_channel);
                        let _ = tx.send(WsServerMessage::Unsubscribed { channel }).await;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: format!("Unsubscribe failed: {}", e),
                            })
                            .await;
                    }
                }
            }
        }

        WsClientMessage::Query { qail } => {
            tracing::debug!("User {} executing query: {}", user_id, qail);

            match qail_core::parser::parse(&qail) {
                Ok(mut cmd) => {
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

                    // WS queries use RLS-scoped connections
                    if let Ok(mut conn) = state.pool.acquire_with_rls(auth.to_rls_context()).await {
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
                                    let _ = tx
                                        .send(WsServerMessage::Error {
                                            message: "Data integrity error".to_string(),
                                        })
                                        .await;
                                    return;
                                }

                                let count = json_rows.len();
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
                                let _ = tx
                                    .send(WsServerMessage::Error {
                                        message: "Query execution failed".to_string(),
                                    })
                                    .await;
                            }
                        }
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

            // Execute immediately and send initial snapshot
            if let Ok(mut conn) = state.pool.acquire_with_rls(auth.to_rls_context()).await {
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
                            let _ = tx
                                .send(WsServerMessage::Error {
                                    message: "Data integrity error".to_string(),
                                })
                                .await;
                            return;
                        }

                        let count = json_rows.len();
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
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: "Live query execution failed".to_string(),
                            })
                            .await;
                        return;
                    }
                }
            }

            // Subscribe to table's NOTIFY channel for change detection
            // SECURITY: prefix with tenant_id to isolate per-tenant
            let notify_channel = match &auth.tenant_id {
                Some(tid) if !tid.is_empty() => format!("{}_qail_table_{}", tid, table),
                _ => format!("qail_table_{}", table),
            };
            if let Ok(mut conn) = state.pool.acquire_system().await {
                let listen_cmd = qail_core::ast::Qail::listen(&notify_channel);
                if conn.fetch_all_uncached(&listen_cmd).await.is_ok() {
                    subscribed_channels.push(notify_channel.clone());
                }
            }

            // Spawn polling task if interval > 0
            if interval_ms > 0 {
                // R7-D: Floor the poll interval to 1000ms to prevent tight-loop DoS
                let safe_interval_ms = interval_ms.max(1000);
                let tx_clone = tx.clone();
                let state_clone = Arc::clone(state);
                let table_clone = table.clone();
                let rls_ctx = auth.to_rls_context();

                tokio::spawn(async move {
                    let mut seq = 2u64;
                    let interval = std::time::Duration::from_millis(safe_interval_ms);
                    loop {
                        tokio::time::sleep(interval).await;

                        if let Ok(mut conn) =
                            state_clone.pool.acquire_with_rls(rls_ctx.clone()).await
                        {
                            match conn.fetch_all_uncached(&cmd).await {
                                Ok(rows) => {
                                    let json_rows: Vec<serde_json::Value> =
                                        rows.iter().map(crate::handler::row_to_json).collect();
                                    let count = json_rows.len();
                                    if tx_clone
                                        .send(WsServerMessage::LiveQueryUpdate {
                                            table: table_clone.clone(),
                                            rows: json_rows,
                                            count,
                                            seq,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break; // Client disconnected
                                    }
                                    seq += 1;
                                }
                                Err(e) => {
                                    tracing::warn!("Live query poll failed: {}", e);
                                }
                            }
                        }
                    }
                });
            }
        }

        WsClientMessage::StopLiveQuery { table } => {
            // SECURITY: reconstruct tenant-scoped channel name
            let notify_channel = match &auth.tenant_id {
                Some(tid) if !tid.is_empty() => format!("{}_qail_table_{}", tid, table),
                _ => format!("qail_table_{}", table),
            };
            if let Ok(mut conn) = state.pool.acquire_system().await {
                let cmd = qail_core::ast::Qail::unlisten(&notify_channel);
                let _ = conn.fetch_all_uncached(&cmd).await;
                subscribed_channels.retain(|c| c != &notify_channel);
            }
            let _ = tx
                .send(WsServerMessage::Unsubscribed {
                    channel: notify_channel,
                })
                .await;
        }
    }
}
