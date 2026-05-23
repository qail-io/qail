use std::sync::Arc;

use super::super::super::listener::listener_rpc;
use super::super::super::{
    ListenControl, WS_ERR_LIVE_QUERY_SUB_FAILED, WS_MAX_SUBSCRIPTIONS_PER_CONNECTION,
    WS_MIN_LIVE_QUERY_INTERVAL_MS, WsServerMessage, build_live_query_notify_channel,
    decrement_channel_refcount, increment_channel_refcount, tracked_channel_count,
};
use super::poller::{LiveQueryPollerConfig, spawn_live_query_poller};
use super::{LiveQueryRuntime, PreparedLiveQuery};

fn exceeds_live_query_task_limit(task_count: usize, replacing_existing: bool) -> bool {
    !replacing_existing && task_count >= WS_MAX_SUBSCRIPTIONS_PER_CONNECTION
}

pub(super) async fn subscribe_and_spawn_live_query(
    table: &str,
    interval_ms: u64,
    prepared: PreparedLiveQuery,
    runtime: &mut LiveQueryRuntime<'_>,
) {
    let state = runtime.state;
    let tx = runtime.tx;
    let listener_tx = runtime.listener_tx;
    let auth = runtime.auth;
    let conn_state = &mut *runtime.conn_state;

    let notify_channel = match build_live_query_notify_channel(auth.tenant_id.as_deref(), table) {
        Ok(channel) => channel,
        Err(message) => {
            let _ = tx.send(WsServerMessage::Error { message }).await;
            return;
        }
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

    let PreparedLiveQuery {
        cmd,
        tenant_guard_plan,
    } = prepared;
    let tenant_guard_column = tenant_guard_plan
        .as_ref()
        .filter(|plan| plan.verify_rows)
        .map(|plan| plan.column.clone())
        .or_else(|| {
            auth.tenant_id.as_ref().and_then(|_| {
                crate::tenant_guard::tenant_guard_column_for_table(state.as_ref(), table)
            })
        });
    let strip_tenant_col = tenant_guard_plan
        .as_ref()
        .is_some_and(|plan| plan.strip_output_column);

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
        tenant_id: tenant_guard_column
            .as_ref()
            .and_then(|_| auth.tenant_id.clone()),
        tenant_col: tenant_guard_column.unwrap_or_else(|| state.config.tenant_column.clone()),
        strip_tenant_col,
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
