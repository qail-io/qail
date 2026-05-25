use super::*;

fn normalize_create_object_for_tenant(
    obj: &serde_json::Map<String, Value>,
    tenant_column: &str,
    tenant_id: Option<&str>,
) -> Result<serde_json::Map<String, Value>, ApiError> {
    let Some(tid) = tenant_id else {
        return Ok(obj.clone());
    };

    let mut normalized = obj.clone();
    for (key, existing) in obj {
        if !identifier_matches_column(key, tenant_column) {
            continue;
        }
        if existing != &Value::String(tid.to_string()) {
            return Err(ApiError::forbidden(format!(
                "Field '{}' must match authenticated tenant context",
                tenant_column
            )));
        }
        normalized.remove(key);
    }
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

fn create_request_can_skip_required_validation(
    prefer_wants_upsert: bool,
    prefer_wants_ignore_duplicates: bool,
    has_explicit_on_conflict: bool,
) -> bool {
    prefer_wants_upsert || prefer_wants_ignore_duplicates || has_explicit_on_conflict
}

fn resolve_prefer_conflict_column(
    prefer_wants_upsert: bool,
    prefer_wants_ignore_duplicates: bool,
    has_explicit_on_conflict: bool,
    primary_key: Option<&str>,
) -> Result<Option<String>, ApiError> {
    if has_explicit_on_conflict || (!prefer_wants_upsert && !prefer_wants_ignore_duplicates) {
        return Ok(None);
    }

    primary_key
        .filter(|pk| !pk.trim().is_empty())
        .map(|pk| Some(pk.to_string()))
        .ok_or_else(|| {
            ApiError::bad_request(
                "VALIDATION_ERROR",
                "Prefer resolution requires a primary key or explicit on_conflict parameter",
            )
        })
}

fn branch_insert_overlay_key(
    obj: &serde_json::Map<String, Value>,
    pk_col: &str,
) -> Result<String, ApiError> {
    let value = obj.get(pk_col).ok_or_else(|| {
        ApiError::bad_request(
            "VALIDATION_ERROR",
            format!(
                "Branch insert requires primary key column '{}' in payload",
                pk_col
            ),
        )
    })?;

    pk_to_overlay_key(value).ok_or_else(|| {
        ApiError::bad_request(
            "VALIDATION_ERROR",
            format!(
                "Branch insert primary key column '{}' must be a non-null scalar value",
                pk_col
            ),
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpsertConflictMode {
    ExplicitUpdate,
    ImplicitMerge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnConflictActionParam {
    Update,
    Nothing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZeroRowConflictDisposition {
    RejectAsConflict,
    AcceptAsNoop,
}

fn upsert_update_assignments<'a>(
    obj: &'a serde_json::Map<String, Value>,
    conflict_cols: &[&str],
    enforce_tenant_column: bool,
    tenant_column: &str,
) -> Vec<(&'a str, Expr)> {
    obj.keys()
        .filter(|k| !conflict_cols.contains(&k.as_str()))
        .filter(|k| !enforce_tenant_column || !identifier_matches_column(k, tenant_column))
        .filter(|k| crate::rest::filters::is_safe_identifier(k))
        .map(|k| (k.as_str(), Expr::Named(format!("EXCLUDED.{}", k))))
        .collect()
}

fn parse_explicit_on_conflict_columns(input: &str) -> Result<Vec<String>, ApiError> {
    crate::rest::filters::parse_identifier_csv(input).map_err(|msg| {
        ApiError::bad_request(
            "VALIDATION_ERROR",
            format!("Invalid on_conflict parameter: {}", msg),
        )
    })
}

fn parse_explicit_on_conflict_param(input: Option<&str>) -> Result<Option<Vec<String>>, ApiError> {
    input.map(parse_explicit_on_conflict_columns).transpose()
}

fn parse_on_conflict_action(input: Option<&str>) -> Result<OnConflictActionParam, ApiError> {
    let Some(input) = input else {
        return Ok(OnConflictActionParam::Update);
    };

    match input.trim().to_ascii_lowercase().as_str() {
        "update" => Ok(OnConflictActionParam::Update),
        "nothing" => Ok(OnConflictActionParam::Nothing),
        _ => Err(ApiError::bad_request(
            "VALIDATION_ERROR",
            "Invalid on_conflict_action parameter: expected 'update' or 'nothing'",
        )),
    }
}

fn apply_on_conflict_update_or_noop(
    cmd: qail_core::ast::Qail,
    conflict_cols: &[&str],
    updates: &[(&str, Expr)],
    mode: UpsertConflictMode,
) -> Result<qail_core::ast::Qail, ApiError> {
    if updates.is_empty() {
        return match mode {
            UpsertConflictMode::ImplicitMerge => Ok(cmd.on_conflict_nothing(conflict_cols)),
            UpsertConflictMode::ExplicitUpdate => Err(ApiError::bad_request(
                "VALIDATION_ERROR",
                "on_conflict update requires at least one non-conflict updatable column",
            )),
        };
    }

    Ok(cmd.on_conflict_update(conflict_cols, updates))
}

fn guard_upsert_conflict_update_tenant(
    cmd: qail_core::ast::Qail,
    tenant_scope: Option<(&str, &str)>,
) -> qail_core::ast::Qail {
    let Some((scope_column, tenant_id)) = tenant_scope else {
        return cmd;
    };
    let has_conflict_update = cmd.on_conflict.as_ref().is_some_and(|on_conflict| {
        matches!(
            on_conflict.action,
            qail_core::ast::ConflictAction::DoUpdate { .. }
        )
    });
    if !has_conflict_update {
        return cmd;
    }

    cmd.filter(
        scope_column,
        Operator::Eq,
        QailValue::String(tenant_id.to_string()),
    )
}

fn zero_row_conflict_disposition(cmd: &qail_core::ast::Qail) -> Option<ZeroRowConflictDisposition> {
    match cmd
        .on_conflict
        .as_ref()
        .map(|on_conflict| &on_conflict.action)
    {
        Some(qail_core::ast::ConflictAction::DoUpdate { .. }) => {
            Some(ZeroRowConflictDisposition::RejectAsConflict)
        }
        Some(qail_core::ast::ConflictAction::DoNothing) => {
            Some(ZeroRowConflictDisposition::AcceptAsNoop)
        }
        None => None,
    }
}

fn apply_create_probe_returning(
    cmd: qail_core::ast::Qail,
    table: &crate::schema::GatewayTable,
) -> qail_core::ast::Qail {
    if let Some(column) = table
        .primary_key
        .as_deref()
        .or_else(|| table.columns.first().map(|column| column.name.as_str()))
        .filter(|column| crate::rest::filters::is_safe_identifier(column))
    {
        cmd.returning([column])
    } else {
        cmd.returning_all()
    }
}

fn build_upsert_old_row_lookup(
    table_name: &str,
    conflict_cols: &[String],
    obj: &serde_json::Map<String, Value>,
    tenant_scope: Option<(&str, &str)>,
) -> Result<qail_core::ast::Qail, ApiError> {
    if conflict_cols.is_empty() {
        return Err(ApiError::bad_request(
            "VALIDATION_ERROR",
            "on_conflict update requires at least one conflict column for event classification",
        ));
    }

    let mut cmd = qail_core::ast::Qail::get(table_name).limit(1);
    for column in conflict_cols {
        let value = obj.get(column).ok_or_else(|| {
            ApiError::bad_request(
                "VALIDATION_ERROR",
                format!(
                    "on_conflict update with triggers requires conflict column '{}' in payload",
                    column
                ),
            )
        })?;
        cmd = cmd.filter(column, Operator::Eq, json_to_qail_value(value));
    }

    if let Some((scope_column, tenant_id)) = tenant_scope {
        cmd = cmd.filter(
            scope_column,
            Operator::Eq,
            QailValue::String(tenant_id.to_string()),
        );
    }

    Ok(cmd)
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

    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let tenant_scope_column =
        crate::rest::tenant_scope_column_for_table(state.as_ref(), &table_name);
    let tenant_scope =
        crate::rest::tenant_scope_filter_for_table(state.as_ref(), &auth, &table_name);
    let prefer = parse_prefer_header(&headers);

    // Validate required columns upfront (skip for upserts — conflict rows may exist)
    let required: Vec<String> = if create_request_can_skip_required_validation(
        prefer.wants_upsert(),
        prefer.wants_ignore_duplicates(),
        mutation_params.on_conflict.is_some(),
    ) {
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
                    && tenant_scope_column
                        .as_ref()
                        .is_some_and(|scope_column| name == scope_column)
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
        .map(|obj| match tenant_scope_column.as_deref() {
            Some(scope_column) => {
                normalize_create_object_for_tenant(obj, scope_column, auth.tenant_id.as_deref())
            }
            None => Ok((*obj).clone()),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let on_conflict_action =
        parse_on_conflict_action(mutation_params.on_conflict_action.as_deref())?;
    let explicit_conflict_cols =
        parse_explicit_on_conflict_param(mutation_params.on_conflict.as_deref())?;
    if let Some(returning) = mutation_params.returning.as_deref() {
        parse_select_columns(returning).map_err(ApiError::parse_error)?;
    }

    // SECURITY: Check branch admin gate BEFORE acquiring connection
    let branch_ctx = extract_branch_from_headers(&headers)?;
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
            let row_pk = match branch_insert_overlay_key(obj, pk_col) {
                Ok(row_pk) => row_pk,
                Err(e) => {
                    let _ = conn.rollback_and_release().await;
                    return Err(e);
                }
            };

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
                let _ = conn.rollback_and_release().await;
                return Err(e);
            }
            all_results.push(row_data);
        }

        conn.release_checked()
            .await
            .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&table_name)))?;

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
    let prefer_conflict_col = resolve_prefer_conflict_column(
        prefer.wants_upsert(),
        prefer.wants_ignore_duplicates(),
        explicit_conflict_cols.is_some(),
        table.primary_key.as_deref(),
    )?;

    let mut all_results: Vec<Value> = Vec::with_capacity(normalized_objects.len());
    let enforce_tenant_column = auth.tenant_id.is_some() && tenant_scope_column.is_some();
    let tenant_column = tenant_scope_column.as_deref().unwrap_or("");
    let has_create_triggers = !state
        .event_engine
        .triggers_for(&table_name, &OperationType::Create)
        .is_empty();
    let has_update_triggers = !state
        .event_engine
        .triggers_for(&table_name, &OperationType::Update)
        .is_empty();
    let response_requested_returning = prefer.return_mode.as_deref() == Some("representation")
        || mutation_params.returning.is_some();
    let mut classified_upsert_events: Vec<(OperationType, Option<Value>, Option<Value>)> =
        Vec::new();
    let mut ignored_conflict_noops = 0usize;

    for obj in &normalized_objects {
        let mut cmd = qail_core::ast::Qail::add(&table_name);
        let mut conflict_update_cols: Option<Vec<String>> = None;

        for (key, value) in obj {
            let qail_val = json_to_qail_value(value);
            cmd = cmd.set_value(key, qail_val);
        }

        // Upsert support: explicit on_conflict param takes precedence
        if let Some(conflict_cols) = explicit_conflict_cols.as_ref() {
            let conflict_col_refs: Vec<&str> = conflict_cols.iter().map(String::as_str).collect();

            match on_conflict_action {
                OnConflictActionParam::Nothing => {
                    cmd = cmd.on_conflict_nothing(&conflict_col_refs);
                }
                OnConflictActionParam::Update => {
                    // Default: update all provided columns on conflict
                    // SECURITY: Filter update keys through identifier guard.
                    conflict_update_cols = Some(conflict_cols.clone());
                    let updates = upsert_update_assignments(
                        obj,
                        &conflict_col_refs,
                        enforce_tenant_column,
                        tenant_column,
                    );
                    cmd = apply_on_conflict_update_or_noop(
                        cmd,
                        &conflict_col_refs,
                        &updates,
                        UpsertConflictMode::ExplicitUpdate,
                    )?;
                }
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
            conflict_update_cols = Some(vec![pk_col.clone()]);
            // SECURITY: Filter update keys through identifier guard.
            let updates = upsert_update_assignments(
                obj,
                &conflict_cols,
                enforce_tenant_column,
                tenant_column,
            );
            cmd = apply_on_conflict_update_or_noop(
                cmd,
                &conflict_cols,
                &updates,
                UpsertConflictMode::ImplicitMerge,
            )?;
        }
        cmd = guard_upsert_conflict_update_tenant(
            cmd,
            tenant_scope
                .as_ref()
                .map(|(column, tenant_id)| (column.as_str(), tenant_id.as_str())),
        );

        // Returning clause: Prefer return=representation and webhook triggers
        // need the created row even when the HTTP caller did not ask for it.
        let classify_upsert_event =
            conflict_update_cols.is_some() && (has_create_triggers || has_update_triggers);
        if mutation_needs_full_returning(
            response_requested_returning,
            mutation_params.returning.as_deref(),
            has_create_triggers || classify_upsert_event,
        ) {
            cmd = apply_returning(cmd, Some("*")).map_err(ApiError::parse_error)?;
        } else if mutation_params.returning.is_none() {
            cmd = apply_create_probe_returning(cmd, table);
        } else {
            cmd = apply_returning(cmd, mutation_params.returning.as_deref())
                .map_err(ApiError::parse_error)?;
        }

        let mut old_cmd = if classify_upsert_event {
            let Some(conflict_cols) = conflict_update_cols.as_ref() else {
                let _ = conn.rollback_and_release().await;
                return Err(ApiError::internal(
                    "Upsert conflict columns missing for event classification",
                ));
            };
            Some(build_upsert_old_row_lookup(
                &table_name,
                conflict_cols,
                obj,
                tenant_scope
                    .as_ref()
                    .map(|(column, tenant_id)| (column.as_str(), tenant_id.as_str())),
            )?)
        } else {
            None
        };

        // Apply RLS
        if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
            let _ = conn.rollback_and_release().await;
            return Err(ApiError::forbidden(e.to_string()));
        }
        if let Some(ref mut old_cmd) = old_cmd {
            if let Err(e) = state.policy_engine.apply_policies(&auth, old_cmd) {
                let _ = conn.rollback_and_release().await;
                return Err(ApiError::forbidden(e.to_string()));
            }
            state.optimize_qail_for_execution(old_cmd);
        }
        state.optimize_qail_for_execution(&mut cmd);
        let zero_row_disposition = zero_row_conflict_disposition(&cmd);

        let old_data = if let Some(ref old_cmd) = old_cmd {
            let rows = match conn.fetch_all_uncached(old_cmd).await {
                Ok(rows) => rows,
                Err(e) => {
                    let _ = conn.rollback_and_release().await;
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
                let _ = conn.rollback_and_release().await;
                return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
            }
        };

        if rows.is_empty() {
            match zero_row_disposition {
                Some(ZeroRowConflictDisposition::AcceptAsNoop) => {
                    ignored_conflict_noops += 1;
                    continue;
                }
                Some(ZeroRowConflictDisposition::RejectAsConflict) => {
                    let _ = conn.rollback_and_release().await;
                    return Err(ApiError::with_code(
                        "CONFLICT",
                        "Create/upsert conflict did not affect a visible row",
                    ));
                }
                None => {
                    let _ = conn.rollback_and_release().await;
                    return Err(ApiError::with_code("CONFLICT", "Create affected no rows"));
                }
            }
        }

        if !rows.is_empty() {
            for row in &rows {
                all_results.push(row_to_json(row));
            }
        }

        if classify_upsert_event {
            let new_data = rows.first().map(row_to_json);
            match (old_data, new_data) {
                (Some(old_data), Some(new_data)) if has_update_triggers => {
                    classified_upsert_events.push((
                        OperationType::Update,
                        Some(new_data),
                        Some(old_data),
                    ));
                }
                (None, Some(new_data)) if has_create_triggers => {
                    classified_upsert_events.push((OperationType::Create, Some(new_data), None));
                }
                _ => {}
            }
        }
    }

    if !classified_upsert_events.is_empty() {
        for (operation, new_data, old_data) in classified_upsert_events {
            if let Err(e) = state
                .event_engine
                .enqueue_durable(&mut conn, &table_name, operation, new_data, old_data)
                .await
            {
                let _ = conn.rollback_and_release().await;
                return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
            }
        }
    } else {
        let event_new = if all_results.is_empty() {
            None
        } else if is_batch {
            Some(json!(all_results.clone()))
        } else {
            all_results.first().cloned()
        };

        if let Some(new_data) = event_new.clone()
            && let Err(e) = state
                .event_engine
                .enqueue_durable(
                    &mut conn,
                    &table_name,
                    OperationType::Create,
                    Some(new_data),
                    None,
                )
                .await
        {
            let _ = conn.rollback_and_release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    }

    conn.release_checked()
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&table_name)))?;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    let response_status = if all_results.is_empty() && ignored_conflict_noops > 0 {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };

    // Prefer: return=minimal → no response body. Ignore-duplicate no-ops use
    // 200 so clients do not mistake them for new rows.
    if prefer.wants_minimal() {
        return Ok((response_status, Json(json!({}))));
    }

    if is_batch {
        let response_results = if response_requested_returning {
            project_mutation_returning_rows(
                all_results.clone(),
                mutation_params.returning.as_deref(),
            )?
        } else {
            Vec::new()
        };
        let count = response_results.len();
        Ok((
            response_status,
            Json(json!({
                "data": response_results,
                "count": count,
            })),
        ))
    } else {
        let data = if response_requested_returning {
            project_mutation_returning_rows(
                all_results.clone(),
                mutation_params.returning.as_deref(),
            )?
            .into_iter()
            .next()
            .unwrap_or_else(|| json!({"created": false}))
        } else {
            json!({"created": !all_results.is_empty()})
        };
        Ok((response_status, Json(json!({ "data": data }))))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OnConflictActionParam, UpsertConflictMode, ZeroRowConflictDisposition,
        apply_create_probe_returning, apply_on_conflict_update_or_noop, branch_insert_overlay_key,
        build_upsert_old_row_lookup, create_request_can_skip_required_validation,
        guard_upsert_conflict_update_tenant, normalize_create_object_for_tenant,
        parse_explicit_on_conflict_columns, parse_explicit_on_conflict_param,
        parse_on_conflict_action, pk_to_overlay_key, resolve_prefer_conflict_column,
        upsert_update_assignments, zero_row_conflict_disposition,
    };
    use crate::schema::{GatewayColumn, GatewayTable};
    use qail_core::ast::{CageKind, ConflictAction, Expr, Operator, Value as QailValue};
    use serde_json::{Map, Value, json};

    fn test_column(name: &str, primary_key: bool) -> GatewayColumn {
        GatewayColumn {
            name: name.to_string(),
            col_type: "text".to_string(),
            pg_type: "text".to_string(),
            nullable: false,
            primary_key,
            unique: false,
            has_default: false,
            foreign_key: None,
        }
    }

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
    fn normalize_create_object_rejects_case_variant_tenant_column() {
        let mut obj = Map::new();
        obj.insert("Tenant_ID".to_string(), json!("tenant_b"));

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
    fn normalize_create_object_canonicalizes_case_variant_tenant_column() {
        let mut obj = Map::new();
        obj.insert("Tenant_ID".to_string(), json!("tenant_a"));
        obj.insert("name".to_string(), json!("alice"));

        let normalized =
            normalize_create_object_for_tenant(&obj, "tenant_id", Some("tenant_a")).unwrap();
        assert_eq!(normalized.get("tenant_id"), Some(&json!("tenant_a")));
        assert!(!normalized.contains_key("Tenant_ID"));
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

    #[test]
    fn explicit_on_conflict_skips_create_required_validation() {
        assert!(create_request_can_skip_required_validation(
            false, false, true
        ));
        assert!(create_request_can_skip_required_validation(
            true, false, false
        ));
        assert!(create_request_can_skip_required_validation(
            false, true, false
        ));
        assert!(!create_request_can_skip_required_validation(
            false, false, false
        ));
    }

    #[test]
    fn prefer_conflict_resolution_uses_primary_key_when_available() {
        assert_eq!(
            resolve_prefer_conflict_column(true, false, false, Some("id")).unwrap(),
            Some("id".to_string())
        );
        assert_eq!(
            resolve_prefer_conflict_column(false, true, false, Some("uuid")).unwrap(),
            Some("uuid".to_string())
        );
    }

    #[test]
    fn prefer_conflict_resolution_requires_pk_without_explicit_conflict_target() {
        let err = resolve_prefer_conflict_column(true, false, false, None).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert!(
            err.message
                .contains("requires a primary key or explicit on_conflict")
        );
    }

    #[test]
    fn prefer_conflict_resolution_allows_explicit_conflict_without_primary_key() {
        assert_eq!(
            resolve_prefer_conflict_column(true, false, true, None).unwrap(),
            None
        );
    }

    #[test]
    fn branch_insert_overlay_key_requires_explicit_scalar_pk() {
        let mut obj = Map::new();

        let missing = branch_insert_overlay_key(&obj, "id").unwrap_err();
        assert_eq!(missing.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert!(missing.message.contains("requires primary key column 'id'"));

        obj.insert("id".to_string(), json!({"nested": true}));
        let nonscalar = branch_insert_overlay_key(&obj, "id").unwrap_err();
        assert_eq!(nonscalar.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert!(
            nonscalar
                .message
                .contains("must be a non-null scalar value")
        );

        obj.insert("id".to_string(), json!("order-1"));
        assert_eq!(
            branch_insert_overlay_key(&obj, "id").unwrap(),
            "order-1".to_string()
        );
    }

    #[test]
    fn implicit_merge_with_no_updatable_columns_uses_do_nothing() {
        let mut obj = Map::new();
        obj.insert("id".to_string(), json!("order-1"));
        obj.insert("tenant_id".to_string(), json!("tenant-a"));
        let conflict_cols = vec!["id"];
        let updates = upsert_update_assignments(&obj, &conflict_cols, true, "tenant_id");
        assert!(updates.is_empty());

        let cmd = apply_on_conflict_update_or_noop(
            qail_core::ast::Qail::add("orders"),
            &conflict_cols,
            &updates,
            UpsertConflictMode::ImplicitMerge,
        )
        .unwrap();

        assert!(matches!(
            cmd.on_conflict.expect("on conflict").action,
            ConflictAction::DoNothing
        ));
    }

    #[test]
    fn upsert_update_assignments_exclude_case_variant_tenant_column() {
        let mut obj = Map::new();
        obj.insert("id".to_string(), json!("order-1"));
        obj.insert("Tenant_ID".to_string(), json!("tenant-b"));
        obj.insert("status".to_string(), json!("paid"));
        let conflict_cols = vec!["id"];

        let updates = upsert_update_assignments(&obj, &conflict_cols, true, "tenant_id");

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0, "status");
    }

    #[test]
    fn explicit_update_with_no_updatable_columns_is_400() {
        let mut obj = Map::new();
        obj.insert("id".to_string(), json!("order-1"));
        obj.insert("tenant_id".to_string(), json!("tenant-a"));
        let conflict_cols = vec!["id"];
        let updates = upsert_update_assignments(&obj, &conflict_cols, true, "tenant_id");

        let err = apply_on_conflict_update_or_noop(
            qail_core::ast::Qail::add("orders"),
            &conflict_cols,
            &updates,
            UpsertConflictMode::ExplicitUpdate,
        )
        .unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert!(err.message.contains("at least one"));
    }

    #[test]
    fn conflict_update_gets_tenant_where_guard() {
        let cmd = qail_core::ast::Qail::add("orders")
            .set_value("id", "order-1")
            .set_value("status", "paid")
            .on_conflict_update(
                &["id"],
                &[("status", Expr::Named("EXCLUDED.status".to_string()))],
            );

        let guarded = guard_upsert_conflict_update_tenant(cmd, Some(("tenant_id", "tenant-a")));

        assert!(guarded.cages.iter().any(|cage| {
            matches!(cage.kind, CageKind::Filter)
                && cage.conditions.iter().any(|condition| {
                    condition.left == Expr::Named("tenant_id".to_string())
                        && condition.op == Operator::Eq
                        && condition.value == QailValue::String("tenant-a".to_string())
                })
        }));
    }

    #[test]
    fn conflict_do_nothing_does_not_add_tenant_where_guard() {
        let cmd = qail_core::ast::Qail::add("orders")
            .set_value("id", "order-1")
            .on_conflict_nothing(&["id"]);

        let guarded = guard_upsert_conflict_update_tenant(cmd, Some(("tenant_id", "tenant-a")));

        assert!(!guarded.cages.iter().any(|cage| {
            matches!(cage.kind, CageKind::Filter)
                && cage
                    .conditions
                    .iter()
                    .any(|condition| condition.left == Expr::Named("tenant_id".to_string()))
        }));
    }

    #[test]
    fn zero_row_conflict_disposition_rejects_guarded_updates() {
        let update = qail_core::ast::Qail::add("orders").on_conflict_update(
            &["id"],
            &[("status", Expr::Named("EXCLUDED.status".into()))],
        );
        let nothing = qail_core::ast::Qail::add("orders").on_conflict_nothing(&["id"]);
        let plain = qail_core::ast::Qail::add("orders");

        assert_eq!(
            zero_row_conflict_disposition(&update),
            Some(ZeroRowConflictDisposition::RejectAsConflict)
        );
        assert_eq!(
            zero_row_conflict_disposition(&nothing),
            Some(ZeroRowConflictDisposition::AcceptAsNoop)
        );
        assert_eq!(zero_row_conflict_disposition(&plain), None);
    }

    #[test]
    fn create_probe_returning_uses_primary_key_sentinel() {
        let table = GatewayTable {
            name: "orders".to_string(),
            columns: vec![test_column("id", true), test_column("status", false)],
            primary_key: Some("id".to_string()),
        };

        let cmd = apply_create_probe_returning(qail_core::ast::Qail::add("orders"), &table);

        assert!(matches!(
            cmd.returning.as_deref(),
            Some([Expr::Named(name)]) if name == "id"
        ));
    }

    #[test]
    fn explicit_on_conflict_columns_accept_valid_list() {
        let cols = parse_explicit_on_conflict_columns("id, tenant_id").unwrap();
        assert_eq!(cols, vec!["id".to_string(), "tenant_id".to_string()]);
    }

    #[test]
    fn explicit_on_conflict_columns_reject_empty_segment() {
        let err = parse_explicit_on_conflict_columns("id, ,tenant_id").unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert!(err.message.contains("Invalid on_conflict parameter"));
    }

    #[test]
    fn explicit_on_conflict_columns_reject_empty_list() {
        let err = parse_explicit_on_conflict_columns("").unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert!(err.message.contains("Invalid on_conflict parameter"));
    }

    #[test]
    fn explicit_on_conflict_columns_reject_invalid_column() {
        let err = parse_explicit_on_conflict_columns("id,tenant-id").unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert!(err.message.contains("Invalid on_conflict parameter"));
    }

    #[test]
    fn explicit_on_conflict_param_parses_before_connection_scope() {
        assert!(parse_explicit_on_conflict_param(None).unwrap().is_none());

        let cols = parse_explicit_on_conflict_param(Some("id, tenant_id"))
            .unwrap()
            .expect("columns");
        assert_eq!(cols, vec!["id".to_string(), "tenant_id".to_string()]);

        let err = parse_explicit_on_conflict_param(Some("id,tenant-id")).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert!(err.message.contains("Invalid on_conflict parameter"));
    }

    #[test]
    fn on_conflict_action_defaults_to_update() {
        assert_eq!(
            parse_on_conflict_action(None).unwrap(),
            OnConflictActionParam::Update
        );
    }

    #[test]
    fn on_conflict_action_accepts_update_and_nothing_case_insensitive() {
        assert_eq!(
            parse_on_conflict_action(Some("update")).unwrap(),
            OnConflictActionParam::Update
        );
        assert_eq!(
            parse_on_conflict_action(Some("NoThInG")).unwrap(),
            OnConflictActionParam::Nothing
        );
    }

    #[test]
    fn on_conflict_action_rejects_unknown_values() {
        let err = parse_on_conflict_action(Some("merge")).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert!(err.message.contains("Invalid on_conflict_action"));
    }

    #[test]
    fn upsert_old_row_lookup_uses_conflict_and_tenant_filters() {
        let mut obj = Map::new();
        obj.insert("id".to_string(), json!("order-1"));
        obj.insert("status".to_string(), json!("paid"));
        let conflict_cols = vec!["id".to_string()];

        let cmd = build_upsert_old_row_lookup(
            "orders",
            &conflict_cols,
            &obj,
            Some(("tenant_id", "tenant-a")),
        )
        .unwrap();

        assert_eq!(cmd.table, "orders");
        assert!(cmd.cages.len() >= 2);
        assert!(cmd.cages.iter().any(|cage| {
            cage.conditions
                .iter()
                .any(|condition| condition.left == qail_core::ast::Expr::Named("id".to_string()))
        }));
        assert!(cmd.cages.iter().any(|cage| cage.conditions.iter().any(
            |condition| condition.left == qail_core::ast::Expr::Named("tenant_id".to_string())
        )));
    }

    #[test]
    fn upsert_old_row_lookup_requires_conflict_column_payload() {
        let obj = Map::new();
        let conflict_cols = vec!["id".to_string()];

        let err = build_upsert_old_row_lookup("orders", &conflict_cols, &obj, None).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert!(err.message.contains("requires conflict column 'id'"));
    }
}
