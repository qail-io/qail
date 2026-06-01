use std::sync::Arc;

use tokio::sync::mpsc;

use crate::GatewayState;
use crate::auth::AuthContext;

use super::super::{WS_ERR_DB_UNAVAILABLE, WsServerMessage};

pub(super) async fn handle_query(
    qail: String,
    state: &Arc<GatewayState>,
    tx: &mpsc::Sender<WsServerMessage>,
    auth: &AuthContext,
) {
    tracing::debug!("User {} executing query: {}", auth.user_id, qail);

    match qail_core::parser::parse(&qail) {
        Ok(mut cmd) => {
            if let Err(e) = crate::handler::query::reject_non_read_action(&cmd, "WebSocket query") {
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: e.message.clone(),
                    })
                    .await;
                return;
            }

            let tenant_guard_plan = match crate::tenant_guard::prepare_tenant_guarded_query(
                state.as_ref(),
                auth,
                &mut cmd,
            ) {
                Ok(column) => column,
                Err(e) => {
                    let _ = tx
                        .send(WsServerMessage::Error {
                            message: e.to_string(),
                        })
                        .await;
                    return;
                }
            };

            let allow_list_raw_query = if tenant_guard_plan.is_some() {
                None
            } else {
                Some(qail.as_str())
            };
            if !crate::handler::is_query_allowed(&state.allow_list, allow_list_raw_query, &cmd) {
                tracing::warn!("WS query rejected by allow-list: {}", qail);
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: "Query not in allow-list".to_string(),
                    })
                    .await;
                return;
            }

            if let Err(e) = state.policy_engine.apply_policies(auth, &mut cmd) {
                tracing::warn!("WS policy error: {}", e);
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: "Access denied by policy".to_string(),
                    })
                    .await;
                return;
            }

            if let Some(ref plan) = tenant_guard_plan
                && plan.verify_rows
                && let Err(e) =
                    crate::tenant_guard::ensure_verifiable_tenant_projection(&cmd, &plan.column)
            {
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: e.to_string(),
                    })
                    .await;
                return;
            }

            crate::handler::clamp_query_limit(&mut cmd, state.config.max_result_rows);
            state.optimize_qail_for_execution(&mut cmd);
            if let Err(e) = crate::access::check_access_policy(state.as_ref(), auth, &cmd) {
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: e.message.clone(),
                    })
                    .await;
                return;
            }

            let (depth, filters, joins) = crate::handler::query::query_complexity(&cmd);
            if let Err(_api_err) = state.complexity_guard.check(depth, filters, joins) {
                tracing::warn!(
                    table = %cmd.table,
                    depth, filters, joins,
                    "WS query rejected by complexity guard"
                );
                crate::metrics::record_complexity_rejected();
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: "Query too complex".to_string(),
                    })
                    .await;
                return;
            }

            if let Ok(mut conn) = state
                .acquire_with_auth_rls_guarded(auth, Some(&cmd.table))
                .await
            {
                match conn.fetch_all_uncached(&cmd).await {
                    Ok(rows) => {
                        let mut json_rows: Vec<serde_json::Value> =
                            rows.iter().map(crate::handler::row_to_json).collect();

                        if let (Some(tenant_id), Some(plan)) =
                            (auth.tenant_id.as_deref(), tenant_guard_plan.as_ref())
                            && plan.verify_rows
                            && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                                &json_rows,
                                tenant_id,
                                &plan.column,
                                &cmd.table,
                                "ws_query",
                            )
                        {
                            tracing::error!("{}", v);
                            conn.release().await;
                            let _ = tx
                                .send(WsServerMessage::Error {
                                    message: "Data integrity error".to_string(),
                                })
                                .await;
                            return;
                        }

                        if let Some(plan) = tenant_guard_plan.as_ref()
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
                            .send(WsServerMessage::Result {
                                rows: json_rows,
                                count,
                            })
                            .await;
                    }
                    Err(e) => {
                        tracing::error!("WS query error: {}", e);
                        conn.release().await;
                        let _ = tx
                            .send(WsServerMessage::Error {
                                message: "Query execution failed".to_string(),
                            })
                            .await;
                    }
                }
            } else {
                tracing::warn!("WS query: pool acquire failed");
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: WS_ERR_DB_UNAVAILABLE.to_string(),
                    })
                    .await;
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
