use super::*;

pub(crate) async fn delete_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    request: axum::extract::Request,
) -> Result<axum::http::StatusCode, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    // F5: Accept any PK type
    if id.is_empty() {
        return Err(ApiError::parse_error(
            "ID parameter cannot be empty".to_string(),
        ));
    }

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let pk = table
        .primary_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?
        .clone();

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // Build: del table[pk = $id]
    let mut cmd = qail_core::ast::Qail::del(&table_name).filter(
        &pk,
        Operator::Eq,
        QailValue::String(id.clone()),
    );

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;
    state.optimize_qail_for_execution(&mut cmd);

    // SECURITY: Check branch admin gate BEFORE acquiring connection
    let branch_ctx = extract_branch_from_headers(&headers);
    if branch_ctx.branch_name().is_some() && auth.role != "admin" && auth.role != "super_admin" {
        return Err(ApiError::forbidden(
            "Admin role required for branch overlay writes",
        ));
    }

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // Branch CoW Write: redirect deletes to overlay (tombstone)
    if let Some(branch_name) = branch_ctx.branch_name() {
        let overlay_result = redirect_to_overlay(
            &mut conn,
            branch_name,
            &table_name,
            &id,
            "delete",
            &Value::Null,
        )
        .await;
        if let Err(e) = overlay_result {
            conn.release().await;
            return Err(e);
        }
        conn.release().await;
        return Ok(axum::http::StatusCode::NO_CONTENT);
    }

    match conn.fetch_all_uncached(&cmd).await {
        Ok(_) => {}
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    // Release connection before event processing
    conn.release().await;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    // Fire event triggers
    state.event_engine.fire(
        &table_name,
        OperationType::Delete,
        None,
        Some(json!({"id": id})),
    );

    // F6: Return 204 No Content to match OpenAPI spec
    Ok(axum::http::StatusCode::NO_CONTENT)
}
