use tokio::sync::mpsc;

use crate::auth::AuthContext;

use super::super::listener::listener_rpc;
use super::super::{
    ListenControl, WS_ERR_DB_UNAVAILABLE, WS_ERR_DB_UNAVAILABLE_UNSUBSCRIBE,
    WS_MAX_SUBSCRIPTIONS_PER_CONNECTION, WsConnectionState, WsServerMessage,
    build_manual_notify_channel, decrement_channel_refcount, increment_channel_refcount,
    tracked_channel_count,
};

pub(super) async fn handle_subscribe(
    channel: String,
    tx: &mpsc::Sender<WsServerMessage>,
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    auth: &AuthContext,
    conn_state: &mut WsConnectionState,
) {
    tracing::debug!("User {} subscribing to channel: {}", auth.user_id, channel);

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

    if !channel.chars().all(|c| c.is_alphanumeric() || c == '_') {
        let _ = tx
            .send(WsServerMessage::Error {
                message: "Invalid channel name — alphanumeric and underscores only".to_string(),
            })
            .await;
        return;
    }

    let scoped_channel = match build_manual_notify_channel(tenant_id, &channel) {
        Ok(scoped) => scoped,
        Err(message) => {
            let _ = tx.send(WsServerMessage::Error { message }).await;
            return;
        }
    };

    if conn_state.manual_subscriptions.contains(&scoped_channel) {
        let _ = tx.send(WsServerMessage::Subscribed { channel }).await;
        return;
    }

    let need_listen = !conn_state.channel_refcounts.contains_key(&scoped_channel);
    if need_listen {
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

pub(super) async fn handle_unsubscribe(
    channel: String,
    tx: &mpsc::Sender<WsServerMessage>,
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    auth: &AuthContext,
    conn_state: &mut WsConnectionState,
) {
    tracing::debug!(
        "User {} unsubscribing from channel: {}",
        auth.user_id,
        channel
    );

    let scoped_channel = match &auth.tenant_id {
        Some(tid) if !tid.is_empty() => match build_manual_notify_channel(tid, &channel) {
            Ok(scoped) => scoped,
            Err(_) => {
                let _ = tx.send(WsServerMessage::Unsubscribed { channel }).await;
                return;
            }
        },
        _ => {
            let _ = tx
                .send(WsServerMessage::Error {
                    message: "Unsubscribe requires authenticated tenant context".to_string(),
                })
                .await;
            return;
        }
    };

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
