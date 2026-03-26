use super::common::parse_cached_query;
use super::*;

/// Execute a QAIL query (BINARY format)
///
/// Accepts strict QWB2 AST-binary payloads and returns JSON results.
/// This path is parser-free and rejects legacy QWB1 text payloads.
pub async fn execute_query_binary(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<QueryResponse>, ApiError> {
    if body.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty binary query"));
    }

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    tracing::debug!(
        "Executing binary query ({} bytes, user: {})",
        body.len(),
        auth.user_id
    );

    if body.len() > 64 * 1024 {
        return Err(ApiError::bad_request(
            "PAYLOAD_TOO_LARGE",
            "Binary query exceeds 64 KiB limit",
        ));
    }

    let mut cmd = match qail_core::wire::decode_cmd_binary(&body) {
        Ok(cmd) => cmd,
        Err(e) => {
            tracing::warn!("Wire decode error: {}", e);
            return Err(ApiError::bad_request(
                "DECODE_ERROR",
                format!("Invalid binary format: {}", e),
            ));
        }
    };

    if let Err(e) = qail_core::sanitize::validate_ast(&cmd) {
        tracing::warn!("Binary AST rejected by structural validation: {}", e);
        return Err(ApiError::bad_request(
            "AST_VALIDATION_FAILED",
            format!("Invalid AST: {}", e),
        ));
    }

    if state.config.binary_requires_allow_list && !state.allow_list.is_enabled() {
        tracing::warn!(
            "Binary query rejected: binary_requires_allow_list=true but no allow-list is loaded. \
             Set QAIL_BINARY_REQUIRES_ALLOW_LIST=false or configure an allow-list file."
        );
        return Err(ApiError::with_code(
            "BINARY_REQUIRES_ALLOW_LIST",
            "Binary endpoint requires a query allow-list to be configured",
        ));
    }

    reject_dangerous_action(&cmd)?;

    if !is_query_allowed(&state.allow_list, None, &cmd) {
        tracing::warn!("Binary query rejected by allow-list");
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }

    clamp_query_limit(&mut cmd, state.config.max_result_rows);

    execute_qail_cmd(&state, &auth, &cmd).await
}

/// Execute a QAIL query (FAST — array-of-arrays response)
///
/// Returns rows as positional arrays instead of keyed objects.
/// Skips column metadata for maximum throughput.
/// Use for data pipelines and internal services that know the schema.
pub async fn execute_query_fast(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<FastQueryResponse>, ApiError> {
    let query_text = body.trim();

    if query_text.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty query"));
    }

    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let mut cmd = parse_cached_query(&state, query_text)?;

    reject_dangerous_action(&cmd)?;

    if !is_query_allowed(&state.allow_list, Some(query_text), &cmd) {
        tracing::warn!("Fast query rejected by allow-list: {}", query_text);
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }

    clamp_query_limit(&mut cmd, state.config.max_result_rows);

    execute_qail_cmd_fast(&state, &auth, &cmd).await
}
