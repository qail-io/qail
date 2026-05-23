use super::*;

fn reject_tenant_column_update(
    obj: &serde_json::Map<String, Value>,
    tenant_column: &str,
    tenant_id: Option<&str>,
) -> Result<(), ApiError> {
    if tenant_id.is_some() && obj.contains_key(tenant_column) {
        return Err(ApiError::forbidden(format!(
            "Field '{}' is server-managed and cannot be updated",
            tenant_column
        )));
    }
    Ok(())
}

fn build_branch_update_overlay_row(
    base_row: Option<Value>,
    obj: &serde_json::Map<String, Value>,
    pk_column: &str,
    row_id: &str,
    tenant_column: &str,
    tenant_id: Option<&str>,
) -> Value {
    let mut overlay_obj = base_row
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    for (key, value) in obj {
        overlay_obj.insert(key.clone(), value.clone());
    }
    overlay_obj.insert(pk_column.to_string(), Value::String(row_id.to_string()));
    if let Some(tid) = tenant_id {
        overlay_obj.insert(tenant_column.to_string(), Value::String(tid.to_string()));
    }
    Value::Object(overlay_obj)
}

pub(crate) async fn update_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(mutation_params): Query<MutationParams>,
    request: axum::extract::Request,
) -> Result<Json<SingleResponse>, ApiError> {
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
    let tenant_scope_column =
        crate::rest::tenant_scope_column_for_table(state.as_ref(), &table_name);
    let tenant_scope =
        crate::rest::tenant_scope_filter_for_table(state.as_ref(), &auth, &table_name);

    // Parse JSON body
    let body = axum::body::to_bytes(request.into_body(), state.config.max_request_body_bytes)
        .await
        .map_err(|e| ApiError::parse_error(e.to_string()))?;
    let body: Value =
        serde_json::from_slice(&body).map_err(|e| ApiError::parse_error(e.to_string()))?;
    let obj = body
        .as_object()
        .ok_or_else(|| ApiError::parse_error("Expected JSON object"))?;

    if obj.is_empty() {
        return Err(ApiError::parse_error("No fields to update"));
    }
    // SECURITY: Fail closed on invalid JSON keys instead of silently skipping.
    for key in obj.keys() {
        if !crate::rest::filters::is_safe_identifier(key) {
            return Err(ApiError::parse_error(format!(
                "Invalid field name '{}' in update payload",
                key
            )));
        }
    }
    if let Some(scope_column) = tenant_scope_column.as_deref() {
        reject_tenant_column_update(obj, scope_column, auth.tenant_id.as_deref())?;
    }
    if let Some(returning) = mutation_params.returning.as_deref() {
        parse_select_columns(returning).map_err(ApiError::parse_error)?;
    }

    // Build: set table { col1 = val1 } [pk = $id]
    let mut cmd = qail_core::ast::Qail::set(&table_name).filter(
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

    for (key, value) in obj {
        let qail_val = json_to_qail_value(value);
        cmd = cmd.set_value(key, qail_val);
    }

    let has_update_triggers = !state
        .event_engine
        .triggers_for(&table_name, &OperationType::Update)
        .is_empty();
    let response_requested_returning = mutation_params.returning.is_some();

    // Returning clause. Event triggers need the post-update row even when the
    // HTTP caller did not ask for representation.
    if mutation_needs_full_returning(
        response_requested_returning,
        mutation_params.returning.as_deref(),
        has_update_triggers,
    ) {
        cmd = cmd.returning_all();
    } else {
        cmd = apply_returning(cmd, mutation_params.returning.as_deref())
            .map_err(ApiError::parse_error)?;
    }

    let mut old_cmd = if has_update_triggers {
        let mut old_cmd = qail_core::ast::Qail::get(&table_name)
            .filter(&pk, Operator::Eq, QailValue::String(id.clone()))
            .limit(1);
        if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
            old_cmd = old_cmd.filter(
                scope_column,
                Operator::Eq,
                QailValue::String(tenant_id.clone()),
            );
        }
        Some(old_cmd)
    } else {
        None
    };

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;
    if let Some(ref mut old_cmd) = old_cmd {
        state
            .policy_engine
            .apply_policies(&auth, old_cmd)
            .map_err(|e| ApiError::forbidden(e.to_string()))?;
        state.optimize_qail_for_execution(old_cmd);
    }
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

    // Branch CoW Write: redirect updates to overlay
    if let Some(branch_name) = branch_ctx.branch_name() {
        let mut base_row = None;
        let overlay_rows = match read_branch_overlay_rows(&mut conn, branch_name, &table_name).await
        {
            Ok(rows) => rows,
            Err(e) => {
                conn.release().await;
                return Err(e);
            }
        };
        match branch_overlay_row_state(&overlay_rows, &id) {
            BranchOverlayRowState::Visible => {}
            BranchOverlayRowState::Deleted => {
                conn.release().await;
                return Err(ApiError::not_found(format!("row '{}'", id)));
            }
            BranchOverlayRowState::Absent => {
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
                    conn.release().await;
                    return Err(ApiError::forbidden(e.to_string()));
                }
                state.optimize_qail_for_execution(&mut exists_cmd);
                let rows = match conn.fetch_all_uncached(&exists_cmd).await {
                    Ok(rows) => rows,
                    Err(e) => {
                        conn.release().await;
                        return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
                    }
                };
                if rows.is_empty() {
                    conn.release().await;
                    return Err(ApiError::not_found(format!("row '{}'", id)));
                }
                base_row = rows.first().map(row_to_json);
            }
        }

        let row_data = build_branch_update_overlay_row(
            base_row,
            obj,
            &pk,
            &id,
            tenant_scope_column
                .as_deref()
                .unwrap_or(&state.config.tenant_column),
            auth.tenant_id.as_deref(),
        );
        let overlay_result = redirect_to_overlay(
            &mut conn,
            branch_name,
            &table_name,
            &id,
            "update",
            &row_data,
        )
        .await;
        if let Err(e) = overlay_result {
            conn.release().await;
            return Err(e);
        }
        conn.release_checked()
            .await
            .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&table_name)))?;
        return Ok(Json(SingleResponse {
            data: json!({"updated": true, "branch": branch_name}),
        }));
    }

    let old_data = if let Some(ref old_cmd) = old_cmd {
        let rows = match conn.fetch_all_uncached(old_cmd).await {
            Ok(rows) => rows,
            Err(e) => {
                conn.release().await;
                return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
            }
        };
        rows.first().map(row_to_json)
    } else {
        None
    };

    let rows = match conn.fetch_all_uncached(&cmd).await {
        Ok(rows) => rows,
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    let returned_rows: Vec<Value> = rows.iter().map(row_to_json).collect();
    let event_new = returned_rows.first().cloned();
    let data = if response_requested_returning {
        project_mutation_returning_rows(returned_rows, mutation_params.returning.as_deref())?
            .into_iter()
            .next()
            .unwrap_or_else(|| json!({"updated": true}))
    } else {
        json!({"updated": true})
    };

    if has_update_triggers
        && event_new.is_some()
        && let Err(e) = state
            .event_engine
            .enqueue_durable(
                &mut conn,
                &table_name,
                OperationType::Update,
                event_new.clone(),
                old_data.clone(),
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

    Ok(Json(SingleResponse { data }))
}

#[cfg(test)]
mod tests {
    use super::{build_branch_update_overlay_row, reject_tenant_column_update};
    use serde_json::{Map, json};

    #[test]
    fn reject_tenant_column_update_blocks_scoped_mutation() {
        let mut obj = Map::new();
        obj.insert("tenant_id".to_string(), json!("tenant_b"));

        let err = reject_tenant_column_update(&obj, "tenant_id", Some("tenant_a")).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn reject_tenant_column_update_allows_unscoped_payloads() {
        let mut obj = Map::new();
        obj.insert("tenant_id".to_string(), json!("tenant_b"));

        assert!(reject_tenant_column_update(&obj, "tenant_id", None).is_ok());
    }

    #[test]
    fn build_branch_update_overlay_row_injects_pk_and_tenant() {
        let mut obj = Map::new();
        obj.insert("name".to_string(), json!("new-name"));

        let row =
            build_branch_update_overlay_row(None, &obj, "id", "42", "tenant_id", Some("tenant-a"));
        assert_eq!(row.get("id"), Some(&json!("42")));
        assert_eq!(row.get("tenant_id"), Some(&json!("tenant-a")));
        assert_eq!(row.get("name"), Some(&json!("new-name")));
    }

    #[test]
    fn build_branch_update_overlay_row_merges_base_row_for_first_update() {
        let mut obj = Map::new();
        obj.insert("status".to_string(), json!("paid"));

        let row = build_branch_update_overlay_row(
            Some(json!({
                "id": "order-1",
                "tenant_id": "tenant-a",
                "status": "draft",
                "total": 42
            })),
            &obj,
            "id",
            "order-1",
            "tenant_id",
            Some("tenant-a"),
        );

        assert_eq!(
            row,
            json!({
                "id": "order-1",
                "tenant_id": "tenant-a",
                "status": "paid",
                "total": 42
            })
        );
    }

    #[test]
    fn build_branch_update_overlay_row_keeps_overlay_pk_aligned_with_path() {
        let mut obj = Map::new();
        obj.insert("id".to_string(), json!("payload-id"));
        obj.insert("tenant_id".to_string(), json!("payload-tenant"));

        let row = build_branch_update_overlay_row(
            Some(json!({"id": "base-id", "tenant_id": "base-tenant"})),
            &obj,
            "id",
            "path-id",
            "tenant_id",
            Some("auth-tenant"),
        );

        assert_eq!(row.get("id"), Some(&json!("path-id")));
        assert_eq!(row.get("tenant_id"), Some(&json!("auth-tenant")));
    }
}
