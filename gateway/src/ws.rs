//! WebSocket subscription handler
//!
//! Provides real-time data subscriptions via PostgreSQL LISTEN/NOTIFY.

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::HeaderMap,
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::auth::extract_auth_from_headers;
use crate::GatewayState;

/// WebSocket subscription message from client
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WsClientMessage {
    /// Subscribe to a channel: { "type": "subscribe", "channel": "orders" }
    #[serde(rename = "subscribe")]
    Subscribe { channel: String },
    
    /// Unsubscribe from a channel: { "type": "unsubscribe", "channel": "orders" }
    #[serde(rename = "unsubscribe")]
    Unsubscribe { channel: String },
    
    /// Execute a query: { "type": "query", "qail": "get users limit 10" }
    #[serde(rename = "query")]
    Query { qail: String },
    
    /// Ping to keep connection alive
    #[serde(rename = "ping")]
    Ping,
}

/// WebSocket message to client
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum WsServerMessage {
    /// Subscription confirmed
    #[serde(rename = "subscribed")]
    Subscribed { channel: String },
    
    /// Unsubscription confirmed
    #[serde(rename = "unsubscribed")]
    Unsubscribed { channel: String },
    
    /// Notification from NOTIFY
    #[serde(rename = "notification")]
    Notification {
        channel: String,
        payload: String,
    },
    
    /// Query result
    #[serde(rename = "result")]
    Result {
        rows: Vec<serde_json::Value>,
        count: usize,
    },
    
    /// Error message
    #[serde(rename = "error")]
    Error { message: String },
    
    /// Pong response
    #[serde(rename = "pong")]
    Pong,
}

/// WebSocket upgrade handler
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = extract_auth_from_headers(&headers);
    tracing::info!("WebSocket connection from user: {}", auth.user_id);
    
    ws.on_upgrade(move |socket| handle_socket(socket, state, auth.user_id))
}

/// Handle WebSocket connection
async fn handle_socket(socket: WebSocket, state: Arc<GatewayState>, user_id: String) {
    let (mut sender, mut receiver) = socket.split();
    
    // Channel for sending messages to client
    let (tx, mut rx) = mpsc::channel::<WsServerMessage>(32);
    
    // Spawn task to send messages to WebSocket
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
    
    // Track subscribed channels
    let mut subscribed_channels: Vec<String> = Vec::new();
    
    // Process incoming messages
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
                            &user_id,
                            &mut subscribed_channels,
                        ).await;
                    }
                    Err(e) => {
                        let _ = tx.send(WsServerMessage::Error {
                            message: format!("Invalid message: {}", e),
                        }).await;
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
    if !subscribed_channels.is_empty() {
        if let Ok(mut conn) = state.pool.acquire().await {
            for channel in &subscribed_channels {
                let cmd = qail_core::ast::Qail::unlisten(channel);
                let _ = conn.fetch_all_uncached(&cmd).await;
            }
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
    user_id: &str,
    subscribed_channels: &mut Vec<String>,
) {
    match msg {
        WsClientMessage::Subscribe { channel } => {
            tracing::debug!("User {} subscribing to channel: {}", user_id, channel);
            
            // Execute LISTEN command
            if let Ok(mut conn) = state.pool.acquire().await {
                let cmd = qail_core::ast::Qail::listen(&channel);
                match conn.fetch_all_uncached(&cmd).await {
                    Ok(_) => {
                        subscribed_channels.push(channel.clone());
                        let _ = tx.send(WsServerMessage::Subscribed { channel }).await;
                    }
                    Err(e) => {
                        let _ = tx.send(WsServerMessage::Error {
                            message: format!("Subscribe failed: {}", e),
                        }).await;
                    }
                }
            }
        }
        
        WsClientMessage::Unsubscribe { channel } => {
            tracing::debug!("User {} unsubscribing from channel: {}", user_id, channel);
            
            if let Ok(mut conn) = state.pool.acquire().await {
                let cmd = qail_core::ast::Qail::unlisten(&channel);
                match conn.fetch_all_uncached(&cmd).await {
                    Ok(_) => {
                        subscribed_channels.retain(|c| c != &channel);
                        let _ = tx.send(WsServerMessage::Unsubscribed { channel }).await;
                    }
                    Err(e) => {
                        let _ = tx.send(WsServerMessage::Error {
                            message: format!("Unsubscribe failed: {}", e),
                        }).await;
                    }
                }
            }
        }
        
        WsClientMessage::Query { qail } => {
            tracing::debug!("User {} executing query: {}", user_id, qail);
            
            match qail_core::parser::parse(&qail) {
                Ok(cmd) => {
                    if let Ok(mut conn) = state.pool.acquire().await {
                        match conn.fetch_all_uncached(&cmd).await {
                            Ok(rows) => {
                                let json_rows: Vec<serde_json::Value> = rows
                                    .iter()
                                    .map(crate::handler::row_to_json)
                                    .collect();
                                let count = json_rows.len();
                                let _ = tx.send(WsServerMessage::Result { rows: json_rows, count }).await;
                            }
                            Err(e) => {
                                let _ = tx.send(WsServerMessage::Error {
                                    message: format!("Query failed: {}", e),
                                }).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(WsServerMessage::Error {
                        message: format!("Parse error: {}", e),
                    }).await;
                }
            }
        }
        
        WsClientMessage::Ping => {
            let _ = tx.send(WsServerMessage::Pong).await;
        }
    }
}
