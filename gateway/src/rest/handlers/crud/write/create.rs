use super::*;

fn normalize_create_object_for_tenant(
    obj: &serde_json::Map<String, Value>,
    tenant_column: &str,
    tenant_id: Option<&str>,
) -> Result<serde_json::Map<String, Value>, ApiError> {
    let Some(tid) = tenant_id else {
        return Ok(obj.clone());
    };

    if let Some(existing) = obj.get(tenant_column)
        && existing != &Value::String(tid.to_string())
    {
        return Err(ApiError::forbidden(format!(
            "Field '{}' must match authenticated tenant context",
            tenant_column
        )));
    }

    let mut normalized = obj.clone();
    normalized.insert(tenant_column.to_string(), Value::String(tid.to_string()));
    Ok(normalized)
}

fn pk_to_overlay_key(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

pub(crate) async fn create_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(mutation_params): Query<MutationParams>,
    request: axum::extract::Request,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;
    let table_has_tenant_column = table
        .columns
        .iter()
        .any(|c| c.name == state.config.tenant_column);

    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let prefer = parse_prefer_header(&headers);

    // Validate required columns upfront (skip for upserts — conflict rows may exist)
    let required: Vec<String> = if prefer.wants_upsert() || prefer.wants_ignore_duplicates() {
        Vec::new() // Upsert: required columns may already exist in the row
    } else {
        table
            .required_columns()
            .iter()
            .map(|c| c.name.clone())
            // Skip tenant_column from required validation — it will be auto-injected
            // from the auth context if not provided by the client.
            .filter(|name| {
                if auth.tenant_id.is_some()
                    && table_has_tenant_column
                    && name == &state.config.tenant_column
                {
                    return false;
                }
                true
            })
            .collect()
    };

    // Parse JSON body
    let body = axum::body::to_bytes(request.into_body(), state.config.max_request_body_bytes)
        .await
        .map_err(|e| ApiError::parse_error(e.to_string()))?;
    let body: Value =
        serde_json::from_slice(&body).map_err(|e| ApiError::parse_error(e.to_string()))?;

    // Detect batch vs single
    let is_batch = body.is_array();
    let objects: Vec<&serde_json::Map<String, Value>> = if is_batch {
        let arr = body
            .as_array()
            .ok_or_else(|| ApiError::parse_error("Expected JSON array body"))?;
        arr.iter()
            .map(|v| {
                v.as_object()
                    .ok_or_else(|| ApiError::parse_error("Batch items must be JSON objects"))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![
            body.as_object()
                .ok_or_else(|| ApiError::parse_error("Expected JSON object or array"))?,
        ]
    };

    if objects.is_empty() {
        return Err(ApiError::parse_error("Empty request body"));
    }

    // Validate required columns for each object
    for (i, obj) in objects.iter().enumerate() {
        for col_name in &required {
            if !obj.contains_key(col_name) {
                return Err(ApiError::parse_error(format!(
                    "Missing required field '{}' in item {}",
                    col_name, i
                )));
            }
        }
    }
    // SECURITY: Fail closed on invalid JSON keys instead of silently skipping.
    // Skipping can produce unintended default-row inserts.
    for obj in &objects {
        for key in obj.keys() {
            if !crate::rest::filters::is_safe_identifier(key) {
                return Err(ApiError::parse_error(format!(
                    "Invalid field name '{}' in create payload",
                    key
                )));
            }
        }
    }

    let normalized_objects: Vec<serde_json::Map<String, Value>> = objects
        .iter()
        .map(|obj| {
            normalize_create_object_for_tenant(
                obj,
                &state.config.tenant_column,
                if table_has_tenant_column {
                    auth.tenant_id.as_deref()
                } else {
                    None
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    // SECURITY: Check branch admin gate BEFORE acquiring connection
    let branch_ctx = extract_branch_from_headers(&headers);
    if branch_ctx.branch_name().is_some() && !auth.can_use_branching() {
        return Err(ApiError::forbidden(
            "Platform administrator role required for branch overlay writes",
        ));
    }

    // Acquire connection
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // Branch CoW Write: redirect inserts to overlay table
    if let Some(branch_name) = branch_ctx.branch_name() {
        let mut all_results: Vec<Value> = Vec::with_capacity(normalized_objects.len());
        for obj in &normalized_objects {
            let row_data: Value = Value::Object(obj.clone());
            let pk_col = table.primary_key.as_deref().unwrap_or("id");
            let row_pk = obj
                .get(pk_col)
                .and_then(pk_to_overlay_key)
                .unwrap_or_else(|| Uuid::new_v4().to_string());

            let overlay_result = redirect_to_overlay(
                &mut conn,
                branch_name,
                &table_name,
                &row_pk,
                "insert",
                &row_data,
            )
            .await;
            if let Err(e) = overlay_result {
                conn.release().await;
                return Err(e);
            }
            all_results.push(row_data);
        }

        conn.release().await;

        if is_batch {
            return Ok((
                StatusCode::CREATED,
                Json(
                    json!({ "data": all_results, "count": all_results.len(), "branch": branch_name }),
                ),
            ));
        } else {
            let data = all_results
                .into_iter()
                .next()
                .unwrap_or_else(|| json!({"created": true}));
            return Ok((
                StatusCode::CREATED,
                Json(json!({ "data": data, "branch": branch_name })),
            ));
        }
    }

    // Resolve PK column for Prefer: resolution=merge-duplicates
    let prefer_conflict_col: Option<String> =
        if prefer.wants_upsert() && mutation_params.on_conflict.is_none() {
            // Auto-resolve PK column from schema
            table.primary_key.clone()
        } else if prefer.wants_ignore_duplicates() && mutation_params.on_conflict.is_none() {
            table.primary_key.clone()
        } else {
            None
        };

    let mut all_results: Vec<Value> = Vec::with_capacity(normalized_objects.len());
    let enforce_tenant_column = auth.tenant_id.is_some() && table_has_tenant_column;
    let tenant_column = state.config.tenant_column.as_str();

    for obj in &normalized_objects {
        let mut cmd = qail_core::ast::Qail::add(&table_name);

        for (key, value) in obj {
            let qail_val = json_to_qail_value(value);
            cmd = cmd.set_value(key, qail_val);
        }

        // Upsert support: explicit on_conflict param takes precedence
        if let Some(ref conflict_col) = mutation_params.on_conflict {
            // SECURITY: Validate on_conflict column identifiers.
            let conflict_cols: Vec<&str> = conflict_col
                .split(',')
                .map(|s| s.trim())
                .filter(|s| crate::rest::filters::is_safe_identifier(s))
                .collect();
            let action = mutation_params
                .on_conflict_action
                .as_deref()
                .unwrap_or("update");

            if action == "nothing" {
                cmd = cmd.on_conflict_nothing(&conflict_cols);
            } else {
                // Default: update all provided columns on conflict
                // SECURITY: Filter update keys through identifier guard.
                let updates: Vec<(&str, Expr)> = obj
                    .keys()
                    .filter(|k| !conflict_cols.contains(&k.as_str()))
                    .filter(|k| !enforce_tenant_column || k.as_str() != tenant_column)
                    .filter(|k| crate::rest::filters::is_safe_identifier(k))
                    .map(|k| (k.as_str(), Expr::Named(format!("EXCLUDED.{}", k))))
                    .collect();
                cmd = cmd.on_conflict_update(&conflict_cols, &updates);
            }
        } else if prefer.wants_ignore_duplicates() {
            // Prefer: resolution=ignore-duplicates → DO NOTHING on PK
            if let Some(ref pk_col) = prefer_conflict_col {
                let cols: Vec<&str> = vec![pk_col.as_str()];
                cmd = cmd.on_conflict_nothing(&cols);
            }
        } else if let Some(ref pk_col) = prefer_conflict_col {
            // Prefer: resolution=merge-duplicates → DO UPDATE on all cols
            let conflict_cols: Vec<&str> = vec![pk_col.as_str()];
            // SECURITY: Filter update keys through identifier guard.
            let updates: Vec<(&str, Expr)> = obj
                .keys()
                .filter(|k| k.as_str() != pk_col.as_str())
                .filter(|k| !enforce_tenant_column || k.as_str() != tenant_column)
                .filter(|k| crate::rest::filters::is_safe_identifier(k))
                .map(|k| (k.as_str(), Expr::Named(format!("EXCLUDED.{}", k))))
                .collect();
            cmd = cmd.on_conflict_update(&conflict_cols, &updates);
        }

        // Returning clause: Prefer return=representation forces RETURNING *
        if prefer.return_mode.as_deref() == Some("representation")
            && mutation_params.returning.is_none()
        {
            cmd = apply_returning(cmd, Some("*"));
        } else {
            cmd = apply_returning(cmd, mutation_params.returning.as_deref());
        }

        // Apply RLS
        if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
            conn.release().await;
            return Err(ApiError::forbidden(e.to_string()));
        }
        state.optimize_qail_for_execution(&mut cmd);

        let rows = match conn.fetch_all_uncached(&cmd).await {
            Ok(rows) => rows,
            Err(e) => {
                conn.release().await;
                return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
            }
        };

        if !rows.is_empty() {
            for row in &rows {
                all_results.push(row_to_json(row));
            }
        }
    }

    // Release connection before JSON processing
    conn.release().await;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    // Prefer: return=minimal → 201 with no body
    if prefer.wants_minimal() {
        state.event_engine.fire(
            &table_name,
            OperationType::Create,
            Some(json!(all_results)),
            None,
        );
        return Ok((StatusCode::CREATED, Json(json!({}))));
    }

    if is_batch {
        let count = all_results.len();
        // Fire event triggers
        state.event_engine.fire(
            &table_name,
            OperationType::Create,
            Some(json!(all_results)),
            None,
        );
        Ok((
            StatusCode::CREATED,
            Json(json!({
                "data": all_results,
                "count": count,
            })),
        ))
    } else {
        let data = all_results
            .into_iter()
            .next()
            .unwrap_or_else(|| json!({"created": true}));
        // Fire event triggers
        state
            .event_engine
            .fire(&table_name, OperationType::Create, Some(data.clone()), None);
        Ok((StatusCode::CREATED, Json(json!({ "data": data }))))
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_create_object_for_tenant, pk_to_overlay_key};
    use serde_json::{Map, Value, json};

    #[test]
    fn normalize_create_object_injects_tenant_column() {
        let mut obj = Map::new();
        obj.insert("name".to_string(), json!("alice"));

        let normalized =
            normalize_create_object_for_tenant(&obj, "tenant_id", Some("tenant_a")).unwrap();
        assert_eq!(
            normalized.get("tenant_id"),
            Some(&Value::String("tenant_a".to_string()))
        );
    }

    #[test]
    fn normalize_create_object_rejects_mismatched_tenant_column() {
        let mut obj = Map::new();
        obj.insert("tenant_id".to_string(), json!("tenant_b"));

        let err =
            normalize_create_object_for_tenant(&obj, "tenant_id", Some("tenant_a")).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn normalize_create_object_preserves_matching_tenant_column() {
        let mut obj = Map::new();
        obj.insert("tenant_id".to_string(), json!("tenant_a"));
        obj.insert("name".to_string(), json!("alice"));

        let normalized =
            normalize_create_object_for_tenant(&obj, "tenant_id", Some("tenant_a")).unwrap();
        assert_eq!(normalized.get("tenant_id"), Some(&json!("tenant_a")));
        assert_eq!(normalized.get("name"), Some(&json!("alice")));
    }

    #[test]
    fn pk_to_overlay_key_accepts_scalar_json() {
        assert_eq!(
            pk_to_overlay_key(&json!("user-1")),
            Some("user-1".to_string())
        );
        assert_eq!(pk_to_overlay_key(&json!(42)), Some("42".to_string()));
        assert_eq!(pk_to_overlay_key(&json!(true)), Some("true".to_string()));
    }

    #[test]
    fn pk_to_overlay_key_rejects_non_scalar_json() {
        assert_eq!(pk_to_overlay_key(&json!(null)), None);
        assert_eq!(pk_to_overlay_key(&json!([1, 2, 3])), None);
        assert_eq!(pk_to_overlay_key(&json!({"id": 1})), None);
    }
}
