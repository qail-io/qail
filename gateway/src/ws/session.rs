use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;

use crate::GatewayState;

use super::listener::{listener_rpc, run_listener_session};
use super::message::handle_client_message;
use super::{
    ListenControl, ListenerNotification, WS_MAX_MESSAGE_BYTES, WS_NOTIFY_QUEUE_CAPACITY,
    WS_OUTBOX_CAPACITY, WsClientMessage, WsConnectionState, WsServerMessage,
    decrement_channel_refcount, prune_finished_live_query_tasks,
};

pub(super) async fn handle_socket(
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
        if auth.is_token_expired_now() {
            let _ = tx
                .send(WsServerMessage::Error {
                    message: "Authentication token expired; reconnect with a fresh token"
                        .to_string(),
                })
                .await;
            break;
        }

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
