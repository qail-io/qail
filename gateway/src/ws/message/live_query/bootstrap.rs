use std::sync::Arc;

use tokio::sync::mpsc;

use crate::GatewayState;
use crate::auth::AuthContext;

use super::super::super::listener::listener_rpc;
use super::super::super::{
    ListenControl, WS_ERR_LIVE_QUERY_SUB_FAILED, WS_MAX_SUBSCRIPTIONS_PER_CONNECTION,
    WS_MIN_LIVE_QUERY_INTERVAL_MS, WsConnectionState, WsServerMessage, decrement_channel_refcount,
    increment_channel_refcount, tracked_channel_count,
};
use super::poller::{LiveQueryPollerConfig, spawn_live_query_poller};

fn exceeds_live_query_task_limit(task_count: usize, replacing_existing: bool) -> bool {
    !replacing_existing && task_count >= WS_MAX_SUBSCRIPTIONS_PER_CONNECTION
}

pub(super) async fn subscribe_and_spawn_live_query(
    table: &str,
    interval_ms: u64,
    cmd: qail_core::ast::Qail,
    state: &Arc<GatewayState>,
    tx: &mpsc::Sender<WsServerMessage>,
    listener_tx: &mpsc::UnboundedSender<ListenControl>,
    auth: &AuthContext,
    conn_state: &mut WsConnectionState,
) {
    let notify_channel = match &auth.tenant_id {
        Some(tid) if !tid.is_empty() => format!("{}_qail_table_{}", tid, table),
        _ => format!("qail_table_{}", table),
    };

    // Enforce per-connection poller cap before mutating LISTEN/refcount state.
    if exceeds_live_query_task_limit(
        conn_state.live_query_tasks.len(),
        conn_state.live_query_tasks.contains_key(table),
    ) {
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

    let previous_channel = conn_state.live_query_channels.get(table).cloned();
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

    let poll_interval = if interval_ms > 0 {
        Some(std::time::Duration::from_millis(
            interval_ms.max(WS_MIN_LIVE_QUERY_INTERVAL_MS),
        ))
    } else {
        None
    };

    let (handle, trigger_tx) = spawn_live_query_poller(LiveQueryPollerConfig {
        poll_interval,
        tx: tx.clone(),
        state: Arc::clone(state),
        table: table.to_string(),
        cmd,
        rls_ctx: auth.to_rls_context(),
        waiter_key: format!(
            "{}:{}",
            auth.tenant_id.as_deref().unwrap_or("_"),
            auth.user_id
        ),
        stmt_timeout: state.config.statement_timeout_ms,
        lock_timeout: state.config.lock_timeout_ms,
        tenant_id: auth.tenant_id.clone(),
        tenant_col: state.config.tenant_column.clone(),
    });

    if let Some(old_handle) = conn_state
        .live_query_tasks
        .insert(table.to_string(), handle)
    {
        old_handle.abort();
    }
    conn_state
        .live_query_triggers
        .insert(table.to_string(), trigger_tx);
    conn_state
        .live_query_channels
        .insert(table.to_string(), notify_channel);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_check_rejects_new_task_at_cap() {
        assert!(exceeds_live_query_task_limit(
            WS_MAX_SUBSCRIPTIONS_PER_CONNECTION,
            false
        ));
    }

    #[test]
    fn limit_check_allows_replacing_existing_task_at_cap() {
        assert!(!exceeds_live_query_task_limit(
            WS_MAX_SUBSCRIPTIONS_PER_CONNECTION,
            true
        ));
    }

    #[test]
    fn limit_check_allows_new_task_below_cap() {
        assert!(!exceeds_live_query_task_limit(
            WS_MAX_SUBSCRIPTIONS_PER_CONNECTION.saturating_sub(1),
            false
        ));
    }
}
