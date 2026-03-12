use super::super::RpcFunctionName;
use super::matcher::{format_signature_brief, select_matching_rpc_signature};
use super::parse::{parse_rpc_signatures, rpc_signature_lookup_cmd};
use crate::GatewayState;
use crate::middleware::ApiError;
use crate::server::RpcCallableSignature;
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

fn next_rpc_probe_stmt_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!("qail_rpc_probe_{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

async fn probe_rpc_overload_resolution(
    conn: &mut qail_pg::PooledConnection,
    sql: &str,
) -> Result<(), qail_pg::PgError> {
    let stmt = next_rpc_probe_stmt_name();
    let probe = format!("PREPARE {} AS {}; DEALLOCATE {}", stmt, sql, stmt);
    conn.get_mut()?.execute_simple(&probe).await
}

fn map_probe_resolution_error(
    err: &qail_pg::PgError,
    function_name: &str,
    signatures: &[RpcCallableSignature],
) -> ApiError {
    let available = signatures
        .iter()
        .map(format_signature_brief)
        .collect::<Vec<_>>()
        .join(", ");

    if let Some(server) = err.server_error() {
        match server.code.as_str() {
            "42725" => {
                crate::metrics::record_rpc_signature_rejection("ambiguous");
                return ApiError::parse_error(format!(
                    "RPC call is ambiguous for '{}'. Available overloads: {}",
                    function_name, available
                ));
            }
            "42883" | "42703" => {
                crate::metrics::record_rpc_signature_rejection("no_match");
                return ApiError::parse_error(format!(
                    "RPC arguments do not match any overload for '{}'. Available overloads: {}",
                    function_name, available
                ));
            }
            _ => {}
        }
    }

    ApiError::from_pg_driver_error(err, None)
}

pub(in super::super) async fn enforce_rpc_signature_contract(
    state: &Arc<GatewayState>,
    conn: &mut qail_pg::PooledConnection,
    function_name: &RpcFunctionName,
    args: Option<&Value>,
    sql: &str,
) -> Result<(), ApiError> {
    if !state.config.rpc_signature_check {
        return Ok(());
    }

    let key = function_name.canonical();
    let signatures = if let Some(cached) = state.rpc_signature_cache.get(&key) {
        crate::metrics::record_rpc_signature_cache_hit();
        cached
    } else {
        crate::metrics::record_rpc_signature_cache_miss();
        let cmd = rpc_signature_lookup_cmd(function_name)?;
        let rows = conn
            .fetch_all_uncached(&cmd)
            .await
            .map_err(|e| ApiError::from_pg_driver_error(&e, None))?;

        if rows.is_empty() {
            crate::metrics::record_rpc_signature_rejection("not_found");
            return Err(ApiError::not_found(&key));
        }

        let parsed = parse_rpc_signatures(&rows)?;
        let cached = Arc::new(parsed);
        state
            .rpc_signature_cache
            .insert(key.clone(), Arc::clone(&cached));
        cached
    };

    if select_matching_rpc_signature(&key, signatures.as_ref(), args).is_ok() {
        return Ok(());
    }

    match probe_rpc_overload_resolution(conn, sql).await {
        Ok(()) => {
            crate::metrics::record_rpc_signature_local_mismatch();
            Ok(())
        }
        Err(err) => Err(map_probe_resolution_error(&err, &key, signatures.as_ref())),
    }
}
