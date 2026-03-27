use tokio::sync::mpsc;

use crate::auth::AuthContext;

use super::super::super::listener::listener_rpc;
use super::super::super::{
    ListenControl, WS_ERR_LIVE_QUERY_UNSUB_FAILED, WsConnectionState, WsServerMessage,
    build_live_query_notify_channel, decrement_channel_refcount,
};

pub(super) async fn handle_stop_live_query(
    table: String,
    tx: &mpsc::Sender<WsServerMessage>,
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    auth: &AuthContext,
    conn_state: &mut WsConnectionState,
) {
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
        let fallback_channel = build_live_query_notify_channel(auth.tenant_id.as_deref(), &table)
            .unwrap_or_else(|_| "qail_table_unknown".to_string());
        let _ = tx
            .send(WsServerMessage::Unsubscribed {
                channel: fallback_channel,
            })
            .await;
    }
}
