use super::super::{RpcFunctionName, build_rpc_bound_sql};
use super::matcher::{
    format_signature_brief, select_matching_rpc_signature, signature_matches_call_shape,
};
use super::parse::{parse_rpc_signatures, rpc_signature_lookup_cmd};
use crate::GatewayState;
use crate::middleware::ApiError;
use crate::server::RpcCallableSignature;
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) enum RpcExecutionMode {
    Rows,
    Void,
    Unknown,
}

impl RpcExecutionMode {
    fn from_signature(signature: &RpcCallableSignature) -> Self {
        if signature.result_type.trim().eq_ignore_ascii_case("void") {
            Self::Void
        } else {
            Self::Rows
        }
    }
}

#[derive(Debug, Clone)]
pub(in super::super) struct RpcSignatureContract {
    pub execution_mode: RpcExecutionMode,
    pub signature: Option<RpcCallableSignature>,
}

fn available_overloads(signatures: &[RpcCallableSignature]) -> String {
    signatures
        .iter()
        .map(format_signature_brief)
        .collect::<Vec<_>>()
        .join(", ")
}

fn is_rpc_probe_candidate_rejection(err: &qail_pg::PgError) -> bool {
    let Some(server) = err.server_error() else {
        return false;
    };

    let code = server.code.trim();
    code.starts_with("22")
        || matches!(
            code,
            // Overload/type-resolution failures.
            "42725"
                | "42804"
                | "42846"
                | "42P08"
                | "42P18"
                // Preserve legacy behavior for defensive void-context mismatches.
                | "42809"
        )
}

async fn probe_rpc_signature_candidate(
    conn: &mut qail_pg::PooledConnection,
    function_name: &RpcFunctionName,
    args: Option<&Value>,
    signature: &RpcCallableSignature,
) -> Result<bool, qail_pg::PgError> {
    let scalar_context = signature.result_type.trim().eq_ignore_ascii_case("void");
    let Ok(query) = build_rpc_bound_sql(function_name, args, Some(signature), scalar_context)
    else {
        return Ok(false);
    };

    if query.param_type_oids.len() != query.params.len() || query.param_type_oids.contains(&0) {
        return Ok(false);
    }

    match conn
        .probe_query_with_param_types(&query.sql, &query.param_type_oids, &query.params)
        .await
    {
        Ok(()) => Ok(true),
        Err(err) if is_rpc_probe_candidate_rejection(&err) => Ok(false),
        Err(err) => Err(err),
    }
}

async fn probe_rpc_overload_resolution(
    conn: &mut qail_pg::PooledConnection,
    function_name: &RpcFunctionName,
    args: Option<&Value>,
    signatures: &[RpcCallableSignature],
) -> Result<Option<RpcCallableSignature>, ApiError> {
    let candidates: Vec<&RpcCallableSignature> = signatures
        .iter()
        .filter(|sig| signature_matches_call_shape(sig, args))
        .collect();

    if candidates.is_empty() {
        crate::metrics::record_rpc_signature_rejection("no_match");
        return Ok(None);
    }

    let mut matched: Vec<RpcCallableSignature> = Vec::new();
    for signature in candidates {
        match probe_rpc_signature_candidate(conn, function_name, args, signature).await {
            Ok(true) => matched.push(signature.clone()),
            Ok(false) => {}
            Err(err) => return Err(ApiError::from_pg_driver_error(&err, None)),
        }
    }

    if matched.is_empty() {
        crate::metrics::record_rpc_signature_rejection("no_match");
        return Ok(None);
    }

    if matched.len() > 1 {
        crate::metrics::record_rpc_signature_rejection("ambiguous");
        let matched_overloads = matched
            .iter()
            .map(format_signature_brief)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ApiError::parse_error(format!(
            "RPC call is ambiguous for '{}'. Matching overloads: {}",
            function_name.canonical(),
            matched_overloads
        )));
    }

    crate::metrics::record_rpc_signature_local_mismatch();
    Ok(matched.into_iter().next())
}

pub(in super::super) async fn enforce_rpc_signature_contract(
    state: &Arc<GatewayState>,
    conn: &mut qail_pg::PooledConnection,
    function_name: &RpcFunctionName,
    args: Option<&Value>,
) -> Result<RpcSignatureContract, ApiError> {
    if !state.config.rpc_signature_check {
        return Ok(RpcSignatureContract {
            execution_mode: RpcExecutionMode::Unknown,
            signature: None,
        });
    }

    let key = function_name.canonical();
    let signatures = if let Some(cached) = state.rpc_signature_cache.get(&key) {
        crate::metrics::record_rpc_signature_cache_hit();
        cached
    } else {
        crate::metrics::record_rpc_signature_cache_miss();
        let mut cmd = rpc_signature_lookup_cmd(function_name)?;
        state.optimize_qail_for_execution(&mut cmd);
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

    if let Ok(signature) = select_matching_rpc_signature(&key, signatures.as_ref(), args) {
        return Ok(RpcSignatureContract {
            execution_mode: RpcExecutionMode::from_signature(signature),
            signature: Some(signature.clone()),
        });
    }

    match probe_rpc_overload_resolution(conn, function_name, args, signatures.as_ref()).await? {
        Some(signature) => Ok(RpcSignatureContract {
            execution_mode: RpcExecutionMode::from_signature(&signature),
            signature: Some(signature),
        }),
        None => Err(ApiError::parse_error(format!(
            "RPC arguments do not match any overload for '{}'. Available overloads: {}",
            key,
            available_overloads(signatures.as_ref())
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::is_rpc_probe_candidate_rejection;

    fn server_error(code: &str, message: &str) -> qail_pg::PgError {
        qail_pg::PgError::QueryServer(qail_pg::PgServerError {
            severity: "ERROR".to_string(),
            code: code.to_string(),
            message: message.to_string(),
            detail: None,
            hint: None,
        })
    }

    #[test]
    fn treats_data_exception_as_candidate_rejection() {
        let err = server_error("22P02", "invalid input syntax for type uuid");
        assert!(is_rpc_probe_candidate_rejection(&err));
    }

    #[test]
    fn treats_type_resolution_errors_as_candidate_rejection() {
        let err = server_error("42804", "datatype mismatch");
        assert!(is_rpc_probe_candidate_rejection(&err));
    }

    #[test]
    fn does_not_hide_privilege_errors_as_candidate_rejection() {
        let err = server_error("42501", "permission denied for function secure_fn");
        assert!(!is_rpc_probe_candidate_rejection(&err));
    }

    #[test]
    fn does_not_hide_non_server_errors_as_candidate_rejection() {
        let err = qail_pg::PgError::Connection("socket closed".to_string());
        assert!(!is_rpc_probe_candidate_rejection(&err));
    }
}
