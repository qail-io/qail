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
            if matches!(
                cmd.action,
                qail_core::ast::Action::Call
                    | qail_core::ast::Action::Do
                    | qail_core::ast::Action::SessionSet
                    | qail_core::ast::Action::SessionShow
                    | qail_core::ast::Action::SessionReset
            ) {
                let _ = tx
                    .send(WsServerMessage::Error {
                        message: format!("Action {:?} is not allowed on WebSocket", cmd.action),
                    })
                    .await;
                return;
            }

            if !crate::handler::is_query_allowed(&state.allow_list, Some(&qail), &cmd) {
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

            crate::handler::clamp_query_limit(&mut cmd, state.config.max_result_rows);

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
                        let json_rows: Vec<serde_json::Value> =
                            rows.iter().map(crate::handler::row_to_json).collect();

                        if let Some(ref tenant_id) = auth.tenant_id
                            && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                                &json_rows,
                                tenant_id,
                                &state.config.tenant_column,
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
