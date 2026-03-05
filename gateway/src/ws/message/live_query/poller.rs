use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::GatewayState;

use super::super::super::{
    WS_ERR_LIVE_QUERY_STOPPED_DB_UNAVAILABLE, WsServerMessage, dispatch_live_query_update,
};

pub(super) struct LiveQueryPollerConfig {
    pub(super) poll_interval: Option<std::time::Duration>,
    pub(super) tx: mpsc::Sender<WsServerMessage>,
    pub(super) state: Arc<GatewayState>,
    pub(super) table: String,
    pub(super) cmd: qail_core::ast::Qail,
    pub(super) rls_ctx: qail_core::rls::RlsContext,
    pub(super) waiter_key: String,
    pub(super) stmt_timeout: u32,
    pub(super) lock_timeout: u32,
    pub(super) tenant_id: Option<String>,
    pub(super) tenant_col: String,
}

pub(super) fn spawn_live_query_poller(
    cfg: LiveQueryPollerConfig,
) -> (JoinHandle<()>, mpsc::Sender<()>) {
    let LiveQueryPollerConfig {
        poll_interval,
        tx,
        state,
        table,
        cmd,
        rls_ctx,
        waiter_key,
        stmt_timeout,
        lock_timeout,
        tenant_id,
        tenant_col,
    } = cfg;

    let (trigger_tx, mut trigger_rx) = mpsc::channel::<()>(1);

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

            if let Ok(mut conn) = state
                .acquire_with_rls_timeouts_guarded(
                    &waiter_key,
                    rls_ctx.clone(),
                    stmt_timeout,
                    lock_timeout,
                    Some(table.as_str()),
                )
                .await
            {
                match conn.fetch_all_uncached(&cmd).await {
                    Ok(rows) => {
                        let json_rows: Vec<serde_json::Value> =
                            rows.iter().map(crate::handler::row_to_json).collect();

                        if let Some(ref tenant_id) = tenant_id
                            && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                                &json_rows,
                                tenant_id,
                                &tenant_col,
                                &table,
                                "ws_live_query_poll",
                            )
                        {
                            tracing::error!("{}", v);
                            conn.release().await;
                            let _ = tx
                                .send(WsServerMessage::Error {
                                    message: "Data integrity error".to_string(),
                                })
                                .await;
                            break;
                        }

                        let count = json_rows.len();
                        conn.release().await;
                        if !dispatch_live_query_update(&tx, &table, json_rows, count, seq) {
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
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: WS_ERR_LIVE_QUERY_STOPPED_DB_UNAVAILABLE.to_string(),
                    })
                    .await;
                break;
            }
        }
    });

    (handle, trigger_tx)
}
