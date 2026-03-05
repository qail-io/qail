use super::common::parse_cached_query;
use super::*;

pub async fn execute_query(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<QueryResponse>, ApiError> {
    let query_text = body.trim();

    if query_text.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty query"));
    }

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    tracing::debug!(
        "Executing text query: {} (user: {})",
        query_text,
        auth.user_id
    );

    let mut cmd = parse_cached_query(&state, query_text)?;

    reject_dangerous_action(&cmd)?;

    if !is_query_allowed(&state.allow_list, Some(query_text), &cmd) {
        tracing::warn!("Query rejected by allow-list: {}", query_text);
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

/// Execute a streaming export query (POST /qail/export).
///
/// Accepts QAIL text that must compile to `Action::Export` and streams raw
/// COPY TO STDOUT chunks to the HTTP response body.
pub async fn execute_query_export(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: String,
) -> Result<Response, ApiError> {
    let query_text = body.trim();
    if query_text.is_empty() {
        return Err(ApiError::bad_request("EMPTY_QUERY", "Empty query"));
    }

    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let mut cmd = parse_cached_query(&state, query_text)?;

    if cmd.action != Action::Export {
        return Err(ApiError::bad_request(
            "EXPORT_ONLY",
            "Endpoint /qail/export only accepts QAIL export commands",
        ));
    }

    reject_dangerous_action(&cmd)?;

    if !is_query_allowed(&state.allow_list, Some(query_text), &cmd) {
        tracing::warn!("Export query rejected by allow-list: {}", query_text);
        return Err(ApiError::with_code(
            "QUERY_NOT_ALLOWED",
            "Query not in allow-list",
        ));
    }

    if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
        tracing::warn!("Policy error: {}", e);
        return Err(ApiError::with_code("POLICY_DENIED", e.to_string()));
    }

    let (depth, filters, joins) = query_complexity(&cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        crate::metrics::record_complexity_rejected();
        return Err(api_err);
    }

    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&cmd.table))
        .await?;
    let cmd_for_stream = cmd.clone();
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(8);

    tokio::spawn(async move {
        let tx_for_chunks = tx.clone();
        let result = conn
            .copy_export_stream_raw(&cmd_for_stream, move |chunk| {
                let tx = tx_for_chunks.clone();
                async move {
                    tx.send(Ok(Bytes::from(chunk))).await.map_err(|_| {
                        qail_pg::PgError::Query(
                            "export stream receiver dropped before completion".to_string(),
                        )
                    })
                }
            })
            .await;

        if let Err(e) = result {
            let _ = tx
                .send(Err(std::io::Error::other(format!(
                    "export stream failed: {}",
                    e
                ))))
                .await;
        }
        conn.release().await;
    });

    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}
