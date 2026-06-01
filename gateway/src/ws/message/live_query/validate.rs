use std::sync::Arc;

use qail_core::ast::Action;
use tokio::sync::mpsc;

use crate::GatewayState;
use crate::auth::AuthContext;

use super::super::super::{WS_ERR_DB_UNAVAILABLE, WsServerMessage};
use super::PreparedLiveQuery;

pub(super) async fn prepare_live_query(
    qail: &str,
    table: &str,
    state: &Arc<GatewayState>,
    tx: &mpsc::Sender<WsServerMessage>,
    auth: &AuthContext,
) -> Option<PreparedLiveQuery> {
    let mut cmd = match qail_core::parser::parse(qail) {
        Ok(cmd) => cmd,
        Err(e) => {
            let _ = tx
                .send(WsServerMessage::Error {
                    message: format!("Parse error: {}", e),
                })
                .await;
            return None;
        }
    };

    if !matches!(cmd.action, Action::Get) {
        let _ = tx
            .send(WsServerMessage::Error {
                message: format!(
                    "Action {:?} is not allowed on WebSocket live_query",
                    cmd.action
                ),
            })
            .await;
        return None;
    }
    if let Err(e) = crate::handler::query::reject_non_read_action(&cmd, "WebSocket live_query") {
        let _ = tx
            .send(WsServerMessage::Error {
                message: e.message.clone(),
            })
            .await;
        return None;
    }

    if state.schema.table(table).is_none() {
        let _ = tx
            .send(WsServerMessage::Error {
                message: format!(
                    "Unknown table: '{}'. Live queries require a valid table name.",
                    table
                ),
            })
            .await;
        return None;
    }

    if cmd.table != table {
        let _ = tx
            .send(WsServerMessage::Error {
                message: format!(
                    "Live query table mismatch: message table '{}' does not match query table '{}'",
                    table, cmd.table
                ),
            })
            .await;
        return None;
    }

    let tenant_guard_plan =
        match crate::tenant_guard::prepare_tenant_guarded_query(state.as_ref(), auth, &mut cmd) {
            Ok(column) => column,
            Err(e) => {
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: e.to_string(),
                    })
                    .await;
                return None;
            }
        };

    let allow_list_raw_query = if tenant_guard_plan.is_some() {
        None
    } else {
        Some(qail)
    };
    if !crate::handler::is_query_allowed(&state.allow_list, allow_list_raw_query, &cmd) {
        tracing::warn!("WS LiveQuery rejected by allow-list: {}", qail);
        let _ = tx
            .send(WsServerMessage::Error {
                message: "Query not in allow-list".to_string(),
            })
            .await;
        return None;
    }

    if let Err(e) = state.policy_engine.apply_policies(auth, &mut cmd) {
        tracing::warn!("WS LiveQuery policy error: {}", e);
        let _ = tx
            .send(WsServerMessage::Error {
                message: "Access denied by policy".to_string(),
            })
            .await;
        return None;
    }

    if let Some(ref plan) = tenant_guard_plan
        && plan.verify_rows
        && let Err(e) = crate::tenant_guard::ensure_verifiable_tenant_projection(&cmd, &plan.column)
    {
        let _ = tx
            .send(WsServerMessage::Error {
                message: e.to_string(),
            })
            .await;
        return None;
    }

    crate::handler::clamp_query_limit(&mut cmd, state.config.max_result_rows);
    state.optimize_qail_for_execution(&mut cmd);
    if let Err(e) = crate::access::check_access_policy(state.as_ref(), auth, &cmd) {
        let _ = tx
            .send(WsServerMessage::Error {
                message: e.message.clone(),
            })
            .await;
        return None;
    }

    let (depth, filters, joins) = crate::handler::query::query_complexity(&cmd);
    if let Err(_api_err) = state.complexity_guard.check(depth, filters, joins) {
        tracing::warn!(
            table = %cmd.table,
            depth, filters, joins,
            "WS LiveQuery rejected by complexity guard"
        );
        crate::metrics::record_complexity_rejected();
        let _ = tx
            .send(WsServerMessage::Error {
                message: "Query too complex".to_string(),
            })
            .await;
        return None;
    }

    Some(PreparedLiveQuery {
        cmd,
        tenant_guard_plan,
    })
}

pub(super) async fn send_initial_snapshot(
    table: &str,
    state: &Arc<GatewayState>,
    tx: &mpsc::Sender<WsServerMessage>,
    auth: &AuthContext,
    prepared: &PreparedLiveQuery,
) -> bool {
    if let Ok(mut conn) = state
        .acquire_with_auth_rls_guarded(auth, Some(&prepared.cmd.table))
        .await
    {
        match conn.fetch_all_uncached(&prepared.cmd).await {
            Ok(rows) => {
                let mut json_rows: Vec<serde_json::Value> =
                    rows.iter().map(crate::handler::row_to_json).collect();

                if let (Some(tenant_id), Some(plan)) = (
                    auth.tenant_id.as_deref(),
                    prepared.tenant_guard_plan.as_ref(),
                ) && plan.verify_rows
                    && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                        &json_rows,
                        tenant_id,
                        &plan.column,
                        &prepared.cmd.table,
                        "ws_live_query",
                    )
                {
                    tracing::error!("{}", v);
                    conn.release().await;
                    let _ = tx
                        .send(WsServerMessage::Error {
                            message: "Data integrity error".to_string(),
                        })
                        .await;
                    return false;
                }

                if let Some(plan) = prepared.tenant_guard_plan.as_ref()
                    && plan.strip_output_column
                {
                    crate::tenant_guard::strip_tenant_column_from_json_rows(
                        &mut json_rows,
                        &plan.column,
                    );
                }

                let count = json_rows.len();
                conn.release().await;
                let _ = tx
                    .send(WsServerMessage::LiveQueryUpdate {
                        table: table.to_string(),
                        rows: json_rows,
                        count,
                        seq: 1,
                    })
                    .await;
            }
            Err(e) => {
                tracing::error!("Live query initial exec failed: {}", e);
                conn.release().await;
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: "Live query execution failed".to_string(),
                    })
                    .await;
                return false;
            }
        }
    } else {
        tracing::warn!("WS LiveQuery: pool acquire failed");
        let _ = tx
            .send(WsServerMessage::Error {
                message: WS_ERR_DB_UNAVAILABLE.to_string(),
            })
            .await;
        return false;
    }

    true
}
