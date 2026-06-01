use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Json,
};
use qail_core::ast::{Operator, Value as QailValue};
use serde_json::Value;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;
use crate::middleware::ApiError;
use crate::rest::filters::{
    apply_filters, apply_sorting, parse_filters_checked, parse_identifier_csv,
};
use crate::rest::types::{ListParams, ListResponse};

fn parse_nested_search_columns(input: Option<&str>) -> Result<String, String> {
    let cols = parse_identifier_csv(input.unwrap_or("name"))?;
    Ok(cols.join(","))
}

fn nested_parent_probe_cmd(
    parent_table: &str,
    parent_pk_col: &str,
    parent_id: &str,
    tenant_scope: Option<(&str, &str)>,
) -> qail_core::ast::Qail {
    let mut cmd = qail_core::ast::Qail::get(parent_table)
        .filter(
            parent_pk_col,
            Operator::Eq,
            QailValue::String(parent_id.to_string()),
        )
        .limit(1);
    if let Some((scope_column, tenant_id)) = tenant_scope {
        cmd = cmd.filter(
            scope_column,
            Operator::Eq,
            QailValue::String(tenant_id.to_string()),
        );
    }
    cmd
}

/// GET /api/{parent}/:id/{child} — list child rows filtered by parent FK
///
/// Example: `GET /api/users/123/orders` → `get orders[user_id = 123]`
///
/// Supports the same query parameters as the main list handler.
pub(crate) async fn nested_list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(parent_id): Path<String>,
    Query(params): Query<ListParams>,
    request: axum::extract::Request,
) -> Result<Json<ListResponse>, ApiError> {
    let path = request.uri().path().to_string();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    // /api/{parent}/{id}/{child}
    if parts.len() < 4 || parts[0] != "api" {
        return Err(ApiError::not_found("nested route"));
    }
    let parent_table = parts[1].to_string();
    let child_table = parts[3].to_string();

    // SECURITY: Block nested access to/from inaccessible tables
    let parent_blocked = if !state.allowed_tables.is_empty() {
        !state.allowed_tables.contains(&parent_table)
    } else {
        state.blocked_tables.contains(&parent_table)
    };
    if parent_blocked {
        return Err(ApiError::forbidden(format!(
            "Table '{}' is not accessible via REST",
            parent_table
        )));
    }
    let child_blocked = if !state.allowed_tables.is_empty() {
        !state.allowed_tables.contains(&child_table)
    } else {
        state.blocked_tables.contains(&child_table)
    };
    if child_blocked {
        return Err(ApiError::forbidden(format!(
            "Table '{}' is not accessible via REST",
            child_table
        )));
    }

    // Look up FK relation: child → parent
    let (fk_col, pk_col) = state
        .schema
        .relation_for(&child_table, &parent_table)
        .ok_or_else(|| {
            ApiError::not_found(format!("No relation: {} → {}", child_table, parent_table))
        })?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let parent_tenant_scope =
        crate::rest::tenant_scope_filter_for_table(state.as_ref(), &auth, &parent_table);
    let tenant_scope =
        crate::rest::tenant_scope_filter_for_table(state.as_ref(), &auth, &child_table);
    let tenant_scope_column = tenant_scope.as_ref().map(|(col, _)| col.as_str());

    let (limit, offset) = params
        .bounded_limit_offset(state.config.max_result_rows)
        .map_err(ApiError::parse_error)?;

    let mut parent_cmd = nested_parent_probe_cmd(
        &parent_table,
        pk_col,
        &parent_id,
        parent_tenant_scope
            .as_ref()
            .map(|(scope_column, tenant_id)| (scope_column.as_str(), tenant_id.as_str())),
    );

    // Build: get child[fk_col = parent_id]
    let mut cmd = qail_core::ast::Qail::get(&child_table).filter(
        fk_col,
        Operator::Eq,
        QailValue::String(parent_id.clone()),
    );
    let mut strip_tenant_scope_column = false;

    // Column selection
    if let Some(ref select) = params.select {
        let mut cols =
            crate::rest::filters::parse_select_columns(select).map_err(ApiError::parse_error)?;
        if let Some(scope_column) = tenant_scope_column
            && !cols.iter().any(|col| col == "*")
            && !cols.iter().any(|col| col == scope_column)
        {
            cols.push(scope_column.to_string());
            strip_tenant_scope_column = true;
        }
        if !cols.is_empty() {
            cmd = cmd.columns(cols);
        }
    }

    // Sorting (multi-column)
    if let Some(ref sort) = params.sort {
        cmd = apply_sorting(cmd, sort).map_err(ApiError::parse_error)?;
    }

    // Distinct
    if let Some(ref distinct) = params.distinct {
        let cols = parse_identifier_csv(distinct).map_err(ApiError::parse_error)?;
        cmd = cmd.distinct_on(cols);
    }

    // Parse and apply filters from query string
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters_checked(query_string).map_err(ApiError::parse_error)?;
    cmd = apply_filters(cmd, &filters);
    if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
        cmd = cmd.filter(
            scope_column,
            Operator::Eq,
            QailValue::String(tenant_id.clone()),
        );
    }

    // Full-text search
    if let Some(ref term) = params.search {
        let cols = parse_nested_search_columns(params.search_columns.as_deref())
            .map_err(ApiError::parse_error)?;
        cmd = cmd.filter(&cols, Operator::TextSearch, QailValue::String(term.clone()));
    }

    cmd = cmd.limit(limit);
    cmd = cmd.offset(offset);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut parent_cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;
    state.optimize_qail_for_execution(&mut parent_cmd);
    state.optimize_qail_for_execution(&mut cmd);
    crate::access::check_access_policy(state.as_ref(), &auth, &parent_cmd)?;
    crate::access::check_access_policy(state.as_ref(), &auth, &cmd)?;

    // Execute — parent proof first, then the child query.
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&child_table))
        .await?;

    let parent_rows = match conn.fetch_all_uncached(&parent_cmd).await {
        Ok(rows) => rows,
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&parent_table)));
        }
    };
    if parent_rows.is_empty() {
        conn.release().await;
        return Err(ApiError::not_found(format!(
            "{}/{}",
            parent_table, parent_id
        )));
    }

    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&child_table)));

    // Release connection back to pool before processing results
    conn.release().await;

    let rows = rows?;
    let mut data: Vec<Value> = rows.iter().map(row_to_json).collect();

    // ── Tenant Boundary Invariant ────────────────────────────────────
    if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &data,
            tenant_id,
            scope_column,
            &child_table,
            "rest_nested_list",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            crate::middleware::ApiError::internal("Data integrity error")
        })?;
    }
    if strip_tenant_scope_column && let Some(scope_column) = tenant_scope_column {
        crate::tenant_guard::strip_tenant_column_from_json_rows(&mut data, scope_column);
    }

    let count = data.len();

    Ok(Json(ListResponse {
        data,
        count,
        total: None,
        limit,
        offset,
    }))
}

#[cfg(test)]
mod tests {
    use super::{nested_parent_probe_cmd, parse_nested_search_columns};
    use qail_core::ast::{Expr, Operator, Value as QailValue};

    #[test]
    fn nested_search_columns_accept_csv_and_default() {
        assert_eq!(parse_nested_search_columns(None).unwrap(), "name");
        assert_eq!(
            parse_nested_search_columns(Some("name, description,name")).unwrap(),
            "name,description"
        );
    }

    #[test]
    fn nested_search_columns_reject_fail_open_inputs() {
        assert!(parse_nested_search_columns(Some("")).is_err());
        assert!(parse_nested_search_columns(Some("name,")).is_err());
        assert!(parse_nested_search_columns(Some("name,bad-col")).is_err());
    }

    #[test]
    fn nested_parent_probe_filters_parent_id_and_tenant_scope() {
        let cmd = nested_parent_probe_cmd("users", "id", "user-1", Some(("tenant_id", "tenant-a")));

        assert_eq!(cmd.table, "users");
        assert!(cmd.cages.iter().any(|cage| {
            cage.conditions.iter().any(|condition| {
                condition.left == Expr::Named("id".to_string())
                    && condition.op == Operator::Eq
                    && condition.value == QailValue::String("user-1".to_string())
            })
        }));
        assert!(cmd.cages.iter().any(|cage| {
            cage.conditions.iter().any(|condition| {
                condition.left == Expr::Named("tenant_id".to_string())
                    && condition.op == Operator::Eq
                    && condition.value == QailValue::String("tenant-a".to_string())
            })
        }));
    }
}
