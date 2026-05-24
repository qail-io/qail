use super::*;

fn apply_branch_single_row_constraints(
    row: &mut Value,
    policy_filter_cages: &[qail_core::ast::Cage],
    selected_columns: Option<&[String]>,
) -> Result<bool, ApiError> {
    if !super::list::row_matches_policy_filter_cages(row, policy_filter_cages)? {
        return Ok(false);
    }

    if let Some(selected_columns) = selected_columns {
        project_rows_to_selected_columns(std::slice::from_mut(row), selected_columns);
    }

    Ok(true)
}

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
    let tenant_scope =
        crate::rest::tenant_scope_filter_for_table(state.as_ref(), &auth, &table_name);
    let branch_ctx = extract_branch_from_headers(&headers)?;
    if branch_ctx.branch_name().is_some() && !auth.can_use_branching() {
        return Err(ApiError::forbidden(
            "Platform administrator role required for branch overlay reads",
        ));
    }

    // Build: get table[pk = $id] — use String value; PG handles type coercion
    let mut cmd = qail_core::ast::Qail::get(&table_name)
        .filter(pk, Operator::Eq, QailValue::String(id.clone()))
        .limit(1);
    if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
        cmd = cmd.filter(
            scope_column,
            Operator::Eq,
            QailValue::String(tenant_id.clone()),
        );
    }

    let branch_policy_filter_start = cmd.cages.len();

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    let branch_policy_filter_cages = if branch_ctx.branch_name().is_some() {
        super::list::branch_policy_filter_cages_from(&cmd, branch_policy_filter_start)
    } else {
        Vec::new()
    };
    let mut strip_tenant_scope_column = false;
    if let Some((scope_column, _)) = tenant_scope.as_ref() {
        strip_tenant_scope_column =
            crate::tenant_guard::ensure_tenant_column_projected(&mut cmd, scope_column)
                .map_err(|e| ApiError::bad_request("TENANT_GUARD_PROJECTION", e.to_string()))?;
    }
    let branch_projection = if branch_ctx.branch_name().is_some() {
        super::list::branch_projection_columns_from_cmd(&cmd)?
    } else {
        None
    };
    if branch_ctx.branch_name().is_some() {
        super::list::ensure_branch_policy_filter_columns_projected(
            &mut cmd,
            &branch_policy_filter_cages,
        )?;
    }

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

    // Branch overlay: check if this row is overridden on the branch.
    if let Some(branch_name) = branch_ctx.branch_name() {
        let overlay_rows = match read_branch_overlay_rows(&mut conn, branch_name, &table_name).await
        {
            Ok(rows) => rows,
            Err(err) => {
                conn.release().await;
                return Err(err);
            }
        };
        data = apply_branch_overlay_to_single_row(&overlay_rows, data, pk, &id)?;
    }

    conn.release().await;

    if let Some(row) = data.as_mut()
        && !apply_branch_single_row_constraints(
            row,
            &branch_policy_filter_cages,
            branch_projection.as_deref(),
        )?
    {
        data = None;
    }

    let mut data = data.ok_or_else(|| ApiError::not_found(format!("{}/{}", table_name, id)))?;

    // ── Tenant Boundary Invariant ────────────────────────────────────
    if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
        let single = vec![data.clone()];
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &single,
            tenant_id,
            scope_column,
            &table_name,
            "rest_get_by_id",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }
    if strip_tenant_scope_column && let Some((scope_column, _)) = tenant_scope.as_ref() {
        let mut single = vec![data];
        crate::tenant_guard::strip_tenant_column_from_json_rows(&mut single, scope_column);
        data = single
            .into_iter()
            .next()
            .ok_or_else(|| ApiError::internal("Missing REST GET row after tenant strip"))?;
    }

    Ok(Json(SingleResponse { data }))
}

#[cfg(test)]
mod tests {
    use super::apply_branch_single_row_constraints;
    use qail_core::ast::{
        Cage, CageKind, Condition, Expr, LogicalOp, Operator, Value as QailValue,
    };
    use serde_json::json;

    fn policy_cage(column: &str, op: Operator, value: QailValue) -> Cage {
        Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named(column.to_string()),
                op,
                value,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::Or,
        }
    }

    #[test]
    fn branch_single_row_overlay_applies_policy_filter_and_projection() {
        let cages = vec![policy_cage(
            "region",
            Operator::Eq,
            QailValue::String("west".to_string()),
        )];
        let selected = vec![
            "id".to_string(),
            "name".to_string(),
            "tenant_id".to_string(),
        ];
        let mut row = json!({
            "id": "order-1",
            "name": "visible",
            "region": "west",
            "secret": "hidden",
            "tenant_id": "tenant-a"
        });

        let visible =
            apply_branch_single_row_constraints(&mut row, &cages, Some(&selected)).unwrap();

        assert!(visible);
        assert_eq!(
            row,
            json!({"id": "order-1", "name": "visible", "tenant_id": "tenant-a"})
        );
    }

    #[test]
    fn branch_single_row_overlay_policy_mismatch_is_hidden() {
        let cages = vec![policy_cage(
            "region",
            Operator::Eq,
            QailValue::String("west".to_string()),
        )];
        let mut row = json!({"id": "order-1", "region": "east", "tenant_id": "tenant-a"});

        let visible = apply_branch_single_row_constraints(&mut row, &cages, None).unwrap();

        assert!(!visible);
    }
}
