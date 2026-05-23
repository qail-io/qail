use std::sync::Arc;

use axum::{extract::State, http::HeaderMap, response::Json};

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::middleware::ApiError;

use super::{
    SavepointRequest, SavepointResponse, TxnBeginResponse, TxnEndResponse, extract_txn_id,
    reject_ddl_in_transaction, txn_err_to_api,
};

fn run_savepoint_action<'a>(
    session: &'a mut crate::transaction::TransactionSession,
    action: String,
    name: String,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<(), crate::transaction::TransactionError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let conn = session
            .conn
            .as_mut()
            .ok_or(crate::transaction::TransactionError::SessionNotFound)?;
        match action.as_str() {
            "create" => conn.savepoint(&name).await.map_err(|e: qail_pg::PgError| {
                crate::transaction::TransactionError::Database(e.to_string())
            }),
            "rollback" => {
                let result = conn
                    .rollback_to(&name)
                    .await
                    .map_err(|e: qail_pg::PgError| {
                        crate::transaction::TransactionError::Database(e.to_string())
                    });
                if result.is_ok() {
                    session.pg_aborted = false;
                }
                result
            }
            "release" => conn
                .release_savepoint(&name)
                .await
                .map_err(|e: qail_pg::PgError| {
                    crate::transaction::TransactionError::Database(e.to_string())
                }),
            _ => Err(crate::transaction::TransactionError::Rejected(format!(
                "Invalid savepoint action '{}'. Use 'create', 'rollback', or 'release'",
                action
            ))),
        }
    })
}

fn validate_savepoint_action(action: &str) -> Result<(), ApiError> {
    match action {
        "create" | "rollback" | "release" => Ok(()),
        _ => Err(ApiError::bad_request(
            "TXN_REJECTED",
            format!(
                "Invalid savepoint action '{}'. Use 'create', 'rollback', or 'release'",
                action
            ),
        )),
    }
}

fn txn_table_name(table_ref: &str) -> String {
    table_ref
        .split_whitespace()
        .next()
        .unwrap_or(table_ref)
        .trim_matches('"')
        .to_string()
}

fn txn_action_mutates(action: qail_core::ast::Action) -> bool {
    matches!(
        action,
        qail_core::ast::Action::Add
            | qail_core::ast::Action::Set
            | qail_core::ast::Action::Del
            | qail_core::ast::Action::Over
            | qail_core::ast::Action::Put
            | qail_core::ast::Action::Upsert
    )
}

fn push_unique_table(tables: &mut Vec<String>, table_ref: &str) {
    let table = txn_table_name(table_ref);
    if !table.is_empty() && !tables.iter().any(|existing| existing == &table) {
        tables.push(table);
    }
}

fn collect_txn_mutated_tables(cmd: &qail_core::ast::Qail, tables: &mut Vec<String>) {
    if txn_action_mutates(cmd.action) {
        push_unique_table(tables, &cmd.table);
    }

    for cte in &cmd.ctes {
        collect_txn_mutated_tables(&cte.base_query, tables);
        if let Some(ref recursive_query) = cte.recursive_query {
            collect_txn_mutated_tables(recursive_query, tables);
        }
    }

    if let Some(ref source_query) = cmd.source_query {
        collect_txn_mutated_tables(source_query, tables);
    }

    for (_, set_query) in &cmd.set_ops {
        collect_txn_mutated_tables(set_query, tables);
    }
}

fn txn_mutated_tables(cmd: &qail_core::ast::Qail) -> Vec<String> {
    let mut tables = Vec::new();
    collect_txn_mutated_tables(cmd, &mut tables);
    tables
}

/// `POST /txn/begin` — Start a new transaction session.
///
/// Acquires a connection from the pool. The RLS checkout opens the transaction
/// that pins tenant-local settings for subsequent statements.
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
    extensions: axum::http::Extensions,
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

    let tenant_guard_plan =
        crate::tenant_guard::prepare_tenant_guarded_query(state.as_ref(), &auth, &mut cmd)
            .map_err(|e| ApiError::bad_request("TENANT_GUARD_PROJECTION", e.to_string()))?;

    // Enforce query allow-list parity with non-transaction endpoints.
    let allow_list_raw_query = if tenant_guard_plan.is_some() {
        None
    } else {
        Some(body.as_str())
    };
    if !crate::handler::is_query_allowed(&state.allow_list, allow_list_raw_query, &cmd) {
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

    if let Some(ref plan) = tenant_guard_plan
        && plan.verify_rows
    {
        crate::tenant_guard::ensure_verifiable_tenant_projection(&cmd, &plan.column)
            .map_err(|e| ApiError::forbidden(e.to_string()))?;
    }

    // Clamp LIMIT to prevent oversized result sets in long-lived txn sessions.
    crate::handler::clamp_query_limit(&mut cmd, state.config.max_result_rows);
    state.optimize_qail_for_execution(&mut cmd);

    // Complexity guard parity with /qail.
    let (depth, filters, joins) = crate::handler::query::query_complexity(&cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        crate::metrics::record_complexity_rejected();
        return Err(api_err);
    }

    let cmd_table = cmd.table.clone();
    let mutated_tables = txn_mutated_tables(&cmd);

    // Execute within the pinned session
    let mut rows = state
        .transaction_manager
        .with_session(
            &txn_id,
            &tenant_id,
            Some(auth.user_id.as_str()),
            |session| {
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

                    let json_rows: Vec<serde_json::Value> =
                        result.iter().map(row_to_json).collect();
                    session.mutated_tables.extend(mutated_tables);

                    Ok(json_rows)
                })
            },
        )
        .await
        .map_err(txn_err_to_api)?;

    if let (Some(tenant_id_str), Some(plan)) =
        (auth.tenant_id.as_deref(), tenant_guard_plan.as_ref())
        && plan.verify_rows
    {
        if let Err(v) = crate::tenant_guard::verify_tenant_boundary(
            &rows,
            tenant_id_str,
            &plan.column,
            &cmd_table,
            "txn_query",
        ) {
            tracing::error!("{}", v);
            if let Err(e) = state
                .transaction_manager
                .close_session(&txn_id, &tenant_id, Some(auth.user_id.as_str()), false)
                .await
            {
                tracing::error!(
                    error = %e,
                    txn_id = %txn_id,
                    "Failed to rollback transaction after tenant boundary violation"
                );
            }
            return Err(ApiError::internal("Data integrity error"));
        }
    }

    if let Some(plan) = tenant_guard_plan.as_ref()
        && plan.strip_output_column
    {
        crate::tenant_guard::strip_tenant_column_from_json_rows(&mut rows, &plan.column);
    }

    let count = rows.len();
    let request_id = match extensions.get::<crate::middleware::RequestId>() {
        Some(id) => id.0.clone(),
        None => String::new(),
    };

    Ok(Json(crate::handler::QueryResponse {
        rows,
        count,
        metadata: Some(crate::handler::ResponseMetadata {
            request_id,
            duration_ms: None, // Txn queries don't use the simple QueryTimer yet
        }),
    }))
}

/// `POST /txn/commit` — Commit and close a transaction session.
///
/// Requires `X-Transaction-Id` header. The pinned RLS transaction is committed
/// and the connection is released back to the pool.
pub async fn txn_commit(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> Result<Json<TxnEndResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;
    let txn_id = extract_txn_id(&headers)?;
    let tenant_id = auth.tenant_id.clone().unwrap_or_default();

    let mutated_tables = state
        .transaction_manager
        .close_session(&txn_id, &tenant_id, Some(auth.user_id.as_str()), true)
        .await
        .map_err(txn_err_to_api)?;
    for table in mutated_tables {
        state.cache.invalidate_table(&table);
    }

    Ok(Json(TxnEndResponse {
        status: "committed".to_string(),
    }))
}

/// `POST /txn/rollback` — Rollback and close a transaction session.
///
/// Requires `X-Transaction-Id` header. The pinned RLS transaction is rolled
/// back and the connection is released back to the pool.
pub async fn txn_rollback(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> Result<Json<TxnEndResponse>, ApiError> {
    let auth = authenticate_request(&state, &headers).await?;
    let txn_id = extract_txn_id(&headers)?;
    let tenant_id = auth.tenant_id.clone().unwrap_or_default();

    let _ = state
        .transaction_manager
        .close_session(&txn_id, &tenant_id, Some(auth.user_id.as_str()), false)
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
    validate_savepoint_action(&request.action)?;

    let action = request.action.clone();
    let name = request.name.clone();

    let run_result = if request.action == "rollback" {
        state
            .transaction_manager
            .with_session_allow_aborted(
                &txn_id,
                &tenant_id,
                Some(auth.user_id.as_str()),
                |session| run_savepoint_action(session, action.clone(), name.clone()),
            )
            .await
    } else {
        state
            .transaction_manager
            .with_session(
                &txn_id,
                &tenant_id,
                Some(auth.user_id.as_str()),
                |session| run_savepoint_action(session, action.clone(), name.clone()),
            )
            .await
    };

    run_result.map_err(txn_err_to_api)?;

    Ok(Json(SavepointResponse {
        action: request.action,
        name: request.name,
    }))
}

#[cfg(test)]
mod tests {
    use super::validate_savepoint_action;

    #[test]
    fn savepoint_action_validation_rejects_before_session_access() {
        assert!(validate_savepoint_action("create").is_ok());
        assert!(validate_savepoint_action("rollback").is_ok());
        assert!(validate_savepoint_action("release").is_ok());

        let err = validate_savepoint_action("drop").unwrap_err();
        assert_eq!(err.code, "TXN_REJECTED");
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }
}
