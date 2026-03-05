use std::sync::Arc;

use axum::{extract::State, http::HeaderMap, response::Json};

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::middleware::ApiError;

use super::{
    SavepointRequest, SavepointResponse, TxnBeginResponse, TxnEndResponse, extract_txn_id,
    reject_ddl_in_transaction, txn_err_to_api,
};

/// `POST /txn/begin` — Start a new transaction session.
///
/// Acquires a connection from the pool, sets RLS context, and issues BEGIN.
/// Returns a session ID to use in subsequent `/txn/*` requests.
pub async fn txn_begin(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> Result<Json<TxnBeginResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;

    let tenant_id = auth.tenant_id.clone().unwrap_or_default();
    let user_id = Some(auth.user_id.clone());

    let limits = state.config.effective_limits(&auth.role);

    let txn_id = state
        .create_txn_session_guarded(
            &auth,
            tenant_id,
            user_id,
            limits.statement_timeout_ms,
            limits.lock_timeout_ms,
        )
        .await
        .map_err(txn_err_to_api)?;

    Ok(Json(TxnBeginResponse { txn_id }))
}

/// `POST /txn/query` — Execute a query within an existing transaction session.
///
/// Requires `X-Transaction-Id` header. The query runs on the pinned connection
/// bound to that session with full RLS context.
pub async fn txn_query(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<crate::handler::QueryResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;
    let txn_id = extract_txn_id(&headers)?;
    let tenant_id = auth.tenant_id.clone().unwrap_or_default();

    // Parse the query
    let mut cmd = qail_core::parser::parse(&body)
        .map_err(|e| ApiError::bad_request("PARSE_ERROR", format!("Parse error: {}", e)))?;

    // Security: reject dangerous actions
    crate::handler::query::reject_dangerous_action(&cmd)?;

    // Security: reject DDL inside transactions
    reject_ddl_in_transaction(&cmd)?;

    // Enforce query allow-list parity with non-transaction endpoints.
    if !crate::handler::is_query_allowed(&state.allow_list, Some(&body), &cmd) {
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    // Apply policy filters/rewrites before execution.
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::with_code("POLICY_DENIED", e.to_string()))?;

    // Clamp LIMIT to prevent oversized result sets in long-lived txn sessions.
    crate::handler::clamp_query_limit(&mut cmd, state.config.max_result_rows);

    // Complexity guard parity with /qail.
    let (depth, filters, joins) = crate::handler::query::query_complexity(&cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        crate::metrics::record_complexity_rejected();
        return Err(api_err);
    }

    let cmd_table = cmd.table.clone();

    // Execute within the pinned session
    let rows = state
        .transaction_manager
        .with_session(&txn_id, &tenant_id, |session| {
            Box::pin(async move {
                use crate::handler::convert::row_to_json;
                let conn = session
                    .conn
                    .as_mut()
                    .ok_or(crate::transaction::TransactionError::SessionNotFound)?;
                let result =
                    conn.fetch_all_uncached(&cmd)
                        .await
                        .map_err(|e: qail_pg::PgError| {
                            crate::transaction::TransactionError::Database(e.to_string())
                        })?;

                let json_rows: Vec<serde_json::Value> = result.iter().map(row_to_json).collect();

                Ok(json_rows)
            })
        })
        .await
        .map_err(txn_err_to_api)?;

    if let Some(ref tenant_id) = auth.tenant_id {
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &rows,
            tenant_id,
            &state.config.tenant_column,
            &cmd_table,
            "txn_query",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }

    let count = rows.len();
    Ok(Json(crate::handler::QueryResponse { rows, count }))
}

/// `POST /txn/commit` — Commit and close a transaction session.
///
/// Requires `X-Transaction-Id` header. The pinned connection is released
/// back to the pool after COMMIT.
pub async fn txn_commit(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> Result<Json<TxnEndResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;
    let txn_id = extract_txn_id(&headers)?;
    let tenant_id = auth.tenant_id.clone().unwrap_or_default();

    state
        .transaction_manager
        .close_session(&txn_id, &tenant_id, true)
        .await
        .map_err(txn_err_to_api)?;

    Ok(Json(TxnEndResponse {
        status: "committed".to_string(),
    }))
}

/// `POST /txn/rollback` — Rollback and close a transaction session.
///
/// Requires `X-Transaction-Id` header. The pinned connection is released
/// back to the pool after ROLLBACK.
pub async fn txn_rollback(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> Result<Json<TxnEndResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;
    let txn_id = extract_txn_id(&headers)?;
    let tenant_id = auth.tenant_id.clone().unwrap_or_default();

    state
        .transaction_manager
        .close_session(&txn_id, &tenant_id, false)
        .await
        .map_err(txn_err_to_api)?;

    Ok(Json(TxnEndResponse {
        status: "rolled_back".to_string(),
    }))
}

/// `POST /txn/savepoint` — Savepoint operations within a transaction.
///
/// Requires `X-Transaction-Id` header and JSON body.
/// Actions: "create", "rollback", "release".
pub async fn txn_savepoint(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(request): Json<SavepointRequest>,
) -> Result<Json<SavepointResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;
    let txn_id = extract_txn_id(&headers)?;
    let tenant_id = auth.tenant_id.clone().unwrap_or_default();

    // Validate savepoint name (alphanumeric + underscore only)
    if !request
        .name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_')
    {
        return Err(ApiError::bad_request(
            "INVALID_SAVEPOINT_NAME",
            "Savepoint name must be alphanumeric (with underscores)",
        ));
    }

    if request.name.is_empty() || request.name.len() > 63 {
        return Err(ApiError::bad_request(
            "INVALID_SAVEPOINT_NAME",
            "Savepoint name must be 1-63 characters",
        ));
    }

    let action = request.action.clone();
    let name = request.name.clone();

    state
        .transaction_manager
        .with_session(&txn_id, &tenant_id, |session| {
            let action = action.clone();
            let name = name.clone();
            Box::pin(async move {
                let conn = session
                    .conn
                    .as_mut()
                    .ok_or(crate::transaction::TransactionError::SessionNotFound)?;
                match action.as_str() {
                    "create" => conn.savepoint(&name).await.map_err(|e: qail_pg::PgError| {
                        crate::transaction::TransactionError::Database(e.to_string())
                    }),
                    "rollback" => conn
                        .rollback_to(&name)
                        .await
                        .map_err(|e: qail_pg::PgError| {
                            crate::transaction::TransactionError::Database(e.to_string())
                        }),
                    "release" => {
                        conn.release_savepoint(&name)
                            .await
                            .map_err(|e: qail_pg::PgError| {
                                crate::transaction::TransactionError::Database(e.to_string())
                            })
                    }
                    _ => Err(crate::transaction::TransactionError::Rejected(format!(
                        "Invalid savepoint action '{}'. Use 'create', 'rollback', or 'release'",
                        action
                    ))),
                }
            })
        })
        .await
        .map_err(txn_err_to_api)?;

    Ok(Json(SavepointResponse {
        action: request.action,
        name: request.name,
    }))
}
