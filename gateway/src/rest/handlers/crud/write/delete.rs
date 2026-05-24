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
    let tenant_scope =
        crate::rest::tenant_scope_filter_for_table(state.as_ref(), &auth, &table_name);

    // Build: del table[pk = $id]
    let mut cmd = qail_core::ast::Qail::del(&table_name).filter(
        &pk,
        Operator::Eq,
        QailValue::String(id.clone()),
    );
    if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
        cmd = cmd.filter(
            scope_column,
            Operator::Eq,
            QailValue::String(tenant_id.clone()),
        );
    }
    cmd = cmd.returning_all();

    let has_delete_triggers = !state
        .event_engine
        .triggers_for(&table_name, &OperationType::Delete)
        .is_empty();

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;
    state.optimize_qail_for_execution(&mut cmd);

    // SECURITY: Check branch admin gate BEFORE acquiring connection
    let branch_ctx = extract_branch_from_headers(&headers)?;
    if branch_ctx.branch_name().is_some() && !auth.can_use_branching() {
        return Err(ApiError::forbidden(
            "Platform administrator role required for branch overlay writes",
        ));
    }

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // Branch CoW Write: redirect deletes to overlay (tombstone)
    if let Some(branch_name) = branch_ctx.branch_name() {
        let overlay_rows = match read_branch_overlay_rows(&mut conn, branch_name, &table_name).await
        {
            Ok(rows) => rows,
            Err(e) => {
                let _ = conn.rollback_and_release().await;
                return Err(e);
            }
        };
        let overlay_state = match branch_overlay_row_state(&overlay_rows, &id) {
            Ok(state) => state,
            Err(e) => {
                let _ = conn.rollback_and_release().await;
                return Err(e);
            }
        };
        match branch_overlay_write_needs_base_lookup(overlay_state, &id) {
            Ok(true) => {
                let mut exists_cmd = qail_core::ast::Qail::get(&table_name)
                    .filter(&pk, Operator::Eq, QailValue::String(id.clone()))
                    .limit(1);
                if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
                    exists_cmd = exists_cmd.filter(
                        scope_column,
                        Operator::Eq,
                        QailValue::String(tenant_id.clone()),
                    );
                }
                if let Err(e) = state.policy_engine.apply_policies(&auth, &mut exists_cmd) {
                    let _ = conn.rollback_and_release().await;
                    return Err(ApiError::forbidden(e.to_string()));
                }
                state.optimize_qail_for_execution(&mut exists_cmd);
                let rows = match conn.fetch_all_uncached(&exists_cmd).await {
                    Ok(rows) => rows,
                    Err(e) => {
                        let _ = conn.rollback_and_release().await;
                        return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
                    }
                };
                if rows.is_empty() {
                    let _ = conn.rollback_and_release().await;
                    return Err(ApiError::not_found(format!("row '{}'", id)));
                }
            }
            Ok(false) => {}
            Err(e) => {
                let _ = conn.rollback_and_release().await;
                return Err(e);
            }
        }

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
            let _ = conn.rollback_and_release().await;
            return Err(e);
        }
        conn.release_checked()
            .await
            .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&table_name)))?;
        return Ok(axum::http::StatusCode::NO_CONTENT);
    }

    let rows = match conn.fetch_all_uncached(&cmd).await {
        Ok(rows) => rows,
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    if let Err(e) = ensure_path_mutation_affected(rows.len(), &id) {
        conn.release().await;
        return Err(e);
    }

    let deleted_data = if has_delete_triggers {
        rows.first().map(row_to_json)
    } else {
        None
    };

    if let Some(old_data) = deleted_data
        && let Err(e) = state
            .event_engine
            .enqueue_durable(
                &mut conn,
                &table_name,
                OperationType::Delete,
                None,
                Some(old_data),
            )
            .await
    {
        let _ = conn.rollback_and_release().await;
        return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
    }

    conn.release_checked()
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&table_name)))?;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    // F6: Return 204 No Content to match OpenAPI spec
    Ok(axum::http::StatusCode::NO_CONTENT)
}
