use tokio::sync::mpsc;

use crate::auth::AuthContext;
use qail_core::rls::channel::ChannelScope;

use super::super::listener::listener_rpc;
use super::super::{
    ListenControl, WS_ERR_DB_UNAVAILABLE, WS_ERR_DB_UNAVAILABLE_UNSUBSCRIBE,
    WS_MAX_SUBSCRIPTIONS_PER_CONNECTION, WsConnectionState, WsServerMessage,
    build_manual_notify_channel, decrement_channel_refcount, increment_channel_refcount,
    tracked_channel_count,
};

/// The identity a manual subscription is scoped under.
///
/// Tenant-shaped deployments scope on the tenant. A tenant-less authenticated
/// user (consumer marketplace shape) scopes on their own user id, so the
/// derived channel is per-recipient: a producer that wants both parties of a
/// chat to hear an event NOTIFYs each party's channel, and no client can
/// listen outside its own scope. Mirrors `qail_core::rls::channel::scoped_channel_for`.
fn subscription_scope(auth: &AuthContext) -> Option<(ChannelScope, &str)> {
    match auth.tenant_id.as_deref() {
        Some(tid) if !tid.is_empty() => Some((ChannelScope::Tenant, tid)),
        _ if auth.is_authenticated() && !auth.user_id.is_empty() => {
            Some((ChannelScope::User, auth.user_id.as_str()))
        }
        _ => None,
    }
}

pub(super) async fn handle_subscribe(
    channel: String,
    state: &std::sync::Arc<crate::GatewayState>,
    tx: &mpsc::Sender<WsServerMessage>,
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    auth: &AuthContext,
    conn_state: &mut WsConnectionState,
) {
    tracing::debug!("User {} subscribing to channel: {}", auth.user_id, channel);

    let Some((scope, id)) = subscription_scope(auth) else {
        let _ = tx
            .send(WsServerMessage::Error {
                message: "Subscribe requires an authenticated tenant or user context".to_string(),
            })
            .await;
        return;
    };

    let scoped_channel = match build_manual_notify_channel(scope, id, &channel) {
        Ok(scoped) => scoped,
        Err(message) => {
            let _ = tx.send(WsServerMessage::Error { message }).await;
            return;
        }
    };

    // Authorization happens BEFORE LISTEN: once channel policies exist, a
    // fragment that matches none is denied and never reaches PostgreSQL.
    if let Err(e) = state.policy_engine.authorize_channel(auth, &channel) {
        tracing::warn!("WS Subscribe denied by channel policy: {}", e);
        let _ = tx
            .send(WsServerMessage::Error {
                message: "Channel not allowed by policy".to_string(),
            })
            .await;
        return;
    }

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

    let scoped_channel = match subscription_scope(auth) {
        Some((scope, id)) => match build_manual_notify_channel(scope, id, &channel) {
            Ok(scoped) => scoped,
            Err(_) => {
                let _ = tx.send(WsServerMessage::Unsubscribed { channel }).await;
                return;
            }
        },
        None => {
            let _ = tx
                .send(WsServerMessage::Error {
                    message: "Unsubscribe requires an authenticated tenant or user context"
                        .to_string(),
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
