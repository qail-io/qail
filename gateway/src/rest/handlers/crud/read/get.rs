use super::*;

pub(crate) async fn get_by_id_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    request: axum::extract::Request,
) -> Result<Json<SingleResponse>, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    // F5: Accept any PK type (UUID, text, integer, serial, etc.)
    // Let Postgres validate the value against the actual column type.
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
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // Build: get table[pk = $id] — use String value; PG handles type coercion
    let mut cmd = qail_core::ast::Qail::get(&table_name)
        .filter(pk, Operator::Eq, QailValue::String(id.clone()))
        .limit(1);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;
    state.optimize_qail_for_execution(&mut cmd);

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    let rows = match conn.fetch_all_uncached(&cmd).await {
        Ok(rows) => rows,
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    let mut data = rows.first().map(row_to_json);

    // Branch overlay: check if this row is overridden on the branch — admin-gated
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        if !auth.can_use_branching() {
            conn.release().await;
            return Err(ApiError::forbidden(
                "Platform administrator role required for branch overlay reads",
            ));
        }
        let sql = qail_pg::driver::branch_sql::read_overlay_sql(branch_name, &table_name);
        if let Ok(pg_conn) = conn.get_mut()
            && let Ok(overlay_rows) = pg_conn.simple_query(&sql).await
        {
            for orow in &overlay_rows {
                let row_pk = orow
                    .try_get_by_name::<String>("row_pk")
                    .ok()
                    .or_else(|| orow.get_string(0))
                    .unwrap_or_default();
                if row_pk == id {
                    let operation = orow
                        .try_get_by_name::<String>("operation")
                        .ok()
                        .or_else(|| orow.get_string(1))
                        .unwrap_or_default();
                    match operation.as_str() {
                        "delete" => {
                            data = None;
                        }
                        "update" | "insert" => {
                            let row_data_str = orow
                                .try_get_by_name::<String>("row_data")
                                .ok()
                                .or_else(|| orow.get_string(2))
                                .unwrap_or_default();
                            if let Ok(mut val) = serde_json::from_str::<Value>(&row_data_str) {
                                if let Some(obj) = val.as_object_mut() {
                                    obj.entry(pk.to_string())
                                        .or_insert_with(|| Value::String(id.clone()));
                                }
                                data = Some(val);
                            }
                        }
                        _ => {}
                    }
                    break;
                }
            }
        }
    }

    conn.release().await;

    let data = data.ok_or_else(|| ApiError::not_found(format!("{}/{}", table_name, id)))?;

    // ── Tenant Boundary Invariant ────────────────────────────────────
    let is_exempt = state
        .config
        .tenant_guard_exempt_tables
        .iter()
        .any(|t| t == &table_name);
    if !is_exempt && let Some(ref tenant_id) = auth.tenant_id {
        let single = vec![data.clone()];
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &single,
            tenant_id,
            &state.config.tenant_column,
            &table_name,
            "rest_get_by_id",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }

    Ok(Json(SingleResponse { data }))
}
