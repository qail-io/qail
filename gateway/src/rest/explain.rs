//! EXPLAIN endpoint for query plan analysis.

use axum::{
    extract::{Query, State},
    http::HeaderMap,
    response::Json,
};
use qail_core::ast::{JoinKind, Operator, Value as QailValue};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::middleware::ApiError;

use super::extract_table_name;
use super::filters::{
    apply_sorting, parse_cursor_value, parse_expand_relations, parse_filters_checked,
};
use super::handlers::{check_table_not_blocked, primary_sort_for_cursor};
use super::types::ListParams;

fn apply_cursor_filter(
    mut cmd: qail_core::ast::Qail,
    cursor: Option<&str>,
    sort: Option<&str>,
    default_sort_column: &str,
) -> Result<qail_core::ast::Qail, ApiError> {
    let Some(cursor) = cursor else {
        return Ok(cmd);
    };

    let (sort_col, sort_desc) = primary_sort_for_cursor(sort, default_sort_column);
    let cursor_val = parse_cursor_value(cursor).map_err(ApiError::parse_error)?;
    if sort_desc {
        cmd = cmd.lt(&sort_col, cursor_val);
    } else {
        cmd = cmd.gt(&sort_col, cursor_val);
    }
    Ok(cmd)
}

fn parse_explain_plan_rows(raw_rows: &[Vec<Option<Vec<u8>>>]) -> Result<Vec<Value>, ApiError> {
    let mut plan = Vec::with_capacity(raw_rows.len());
    for (idx, cols) in raw_rows.iter().enumerate() {
        let bytes = cols
            .first()
            .and_then(|c| c.as_ref())
            .ok_or_else(|| ApiError::internal(format!("EXPLAIN row {idx} is missing plan JSON")))?;
        let raw = std::str::from_utf8(bytes).map_err(|e| {
            ApiError::internal(format!("EXPLAIN row {idx} contains invalid UTF-8: {e}"))
        })?;
        let value = serde_json::from_str(raw).map_err(|e| {
            ApiError::internal(format!("EXPLAIN row {idx} contains invalid JSON: {e}"))
        })?;
        plan.push(value);
    }
    Ok(plan)
}

/// GET /api/{table}/_explain — return EXPLAIN ANALYZE for the query
///
/// Accepts the same query params as the list handler (filters, sort, expand, etc.)
/// and returns the PostgreSQL execution plan as JSON.
pub(crate) async fn explain_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
    request: axum::extract::Request,
) -> Result<Json<Value>, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // SECURITY: Restrict EXPLAIN ANALYZE to admin roles only.
    // This endpoint executes the query and reveals plan internals (costs, indexes, row counts).
    if !auth.can_run_explain_analyze() {
        return Err(ApiError::forbidden(
            "EXPLAIN ANALYZE requires platform administrator access",
        ));
    }

    // Build query (same as list_handler)
    let (limit, offset) = params
        .bounded_limit_offset(state.config.max_result_rows)
        .map_err(ApiError::parse_error)?;
    let mut cmd = qail_core::ast::Qail::get(&table_name);

    // Apply select
    if let Some(ref select) = params.select {
        let cols =
            crate::rest::filters::parse_select_columns(select).map_err(ApiError::parse_error)?;
        if !cols.is_empty() {
            cmd = cmd.columns(cols);
        }
    }

    let default_sort_column = table.primary_key.as_deref().unwrap_or("id");

    // Apply sorting — default to the schema primary key for deterministic pagination
    if let Some(ref sort) = params.sort {
        cmd = apply_sorting(cmd, sort).map_err(ApiError::parse_error)?;
    } else if crate::rest::filters::is_safe_identifier(default_sort_column) {
        cmd = cmd.order_asc(default_sort_column);
    }

    // Apply expand (flat JOIN only) — enforce depth limit
    let mut has_joins = false;
    if let Some(ref expand) = params.expand {
        let (relations, nested_relations) =
            parse_expand_relations(expand, state.config.max_expand_depth)
                .map_err(ApiError::parse_error)?;
        if !nested_relations.is_empty() {
            return Err(ApiError::parse_error(format!(
                "Nested expand is not supported by _explain: {}",
                nested_relations.join(",")
            )));
        }
        for rel in relations {
            check_table_not_blocked(&state, rel)?;
            if let Some((fk_col, ref_col)) = state.schema.relation_for(&table_name, rel) {
                let left = format!("{}.{}", table_name, fk_col);
                let right = format!("{}.{}", rel, ref_col);
                cmd = cmd.join(JoinKind::Left, rel, &left, &right);
                has_joins = true;
            } else if state.schema.relation_for(rel, &table_name).is_some() {
                return Err(ApiError::parse_error(format!(
                    "Reverse relation '{}' expands one-to-many and can duplicate parent rows. Use 'nested:{}' instead.",
                    rel, rel
                )));
            }
        }
    }

    // When JOINs are present, table-qualify base table columns
    if has_joins {
        use qail_core::ast::Expr;
        if cmd.columns.is_empty() || cmd.columns == vec![Expr::Named("*".into())] {
            cmd.columns = vec![Expr::Named(format!("{}.*", table_name))];
        } else {
            cmd.columns = cmd
                .columns
                .into_iter()
                .map(|expr| match expr {
                    Expr::Named(ref name) if !name.contains('.') => {
                        Expr::Named(format!("{}.{}", table_name, name))
                    }
                    other => other,
                })
                .collect();
        }
    }

    // Apply filters
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters_checked(query_string).map_err(ApiError::parse_error)?;
    cmd = crate::rest::filters::apply_filters_owned(cmd, filters);

    // Full-text search
    if let Some(ref term) = params.search {
        let cols_input = params.search_columns.as_deref().unwrap_or("name");
        let cols = crate::rest::filters::parse_identifier_csv(cols_input)
            .map_err(ApiError::parse_error)?;
        let search_cols = cols.join(",");
        cmd = cmd.filter(
            &search_cols,
            Operator::TextSearch,
            QailValue::String(term.clone()),
        );
    }

    cmd = apply_cursor_filter(
        cmd,
        params.cursor.as_deref(),
        params.sort.as_deref(),
        default_sort_column,
    )?;

    cmd = cmd.limit(limit);
    cmd = cmd.offset(offset);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // When JOINs are present, table-qualify unqualified filter columns
    if has_joins {
        crate::rest::filters::qualify_base_filter_columns_for_join(&mut cmd, &table_name);
    }
    crate::access::check_access_policy(state.as_ref(), &auth, &cmd)?;

    // ── Query Complexity Guard ───────────────────────────────────────
    let (depth, filters, joins) = crate::handler::query::query_complexity(&cmd);
    if let Err(api_err) = state.complexity_guard.check(depth, filters, joins) {
        tracing::warn!(
            table = %table_name,
            depth, filters, joins,
            "EXPLAIN query rejected by complexity guard"
        );
        crate::metrics::record_complexity_rejected();
        return Err(api_err);
    }

    // Generate SQL from AST
    use qail_pg::protocol::AstEncoder;
    let mut sql_buf = bytes::BytesMut::with_capacity(256);
    let mut params_buf: Vec<Option<Vec<u8>>> = Vec::new();
    AstEncoder::encode_select_sql(&cmd, &mut sql_buf, &mut params_buf)
        .map_err(|e| ApiError::bad_request("ENCODE_ERROR", e.to_string()))?;
    let sql = std::str::from_utf8(&sql_buf)
        .map_err(|e| {
            ApiError::bad_request("ENCODE_ERROR", format!("encoded SQL is not UTF-8: {}", e))
        })?
        .to_string();

    // Run EXPLAIN ANALYZE with bind parameters
    // SECURITY: `sql` contains $1, $2 placeholders — we MUST pass `params_buf`
    // alongside so PostgreSQL resolves them. Previously params were discarded,
    // causing "bind message has 0 parameters but 2 were expected" errors.
    let explain_sql = format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", sql);

    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    let raw_rows = match conn.query_raw_with_params(&explain_sql, &params_buf).await {
        Ok(rows) => {
            conn.release().await;
            rows
        }
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    let plan = parse_explain_plan_rows(&raw_rows)?;

    Ok(Json(json!({
        "query": sql,
        "plan": plan,
    })))
}

#[cfg(test)]
mod tests {
    use qail_core::ast::Qail;
    use qail_core::transpiler::ToSql;

    use super::{apply_cursor_filter, parse_explain_plan_rows};

    #[test]
    fn explain_cursor_filter_uses_default_ascending_sort() {
        let cmd = apply_cursor_filter(Qail::get("orders"), Some("42"), None, "id").unwrap();

        assert_eq!(cmd.to_sql(), "SELECT * FROM orders WHERE id > 42");
    }

    #[test]
    fn explain_cursor_filter_uses_descending_primary_sort() {
        let cmd = apply_cursor_filter(
            Qail::get("orders"),
            Some("2026-01-01"),
            Some("-created_at,total:asc"),
            "id",
        )
        .unwrap();

        assert_eq!(
            cmd.to_sql(),
            "SELECT * FROM orders WHERE created_at < '2026-01-01'"
        );
    }

    #[test]
    fn explain_cursor_filter_rejects_non_finite_numbers() {
        let err = apply_cursor_filter(Qail::get("orders"), Some("NaN"), None, "id").unwrap_err();
        assert_eq!(err.code, "PARSE_ERROR");
        assert!(
            err.details
                .as_deref()
                .is_some_and(|details| details.contains("non-finite numeric value"))
        );
    }

    #[test]
    fn explain_plan_parser_rejects_malformed_rows() {
        let missing = vec![vec![None]];
        let err = parse_explain_plan_rows(&missing).unwrap_err();
        assert_eq!(err.code, "INTERNAL_ERROR");

        let invalid_utf8 = vec![vec![Some(vec![0xff])]];
        let err = parse_explain_plan_rows(&invalid_utf8).unwrap_err();
        assert_eq!(err.code, "INTERNAL_ERROR");

        let invalid_json = vec![vec![Some(b"not-json".to_vec())]];
        let err = parse_explain_plan_rows(&invalid_json).unwrap_err();
        assert_eq!(err.code, "INTERNAL_ERROR");
    }

    #[test]
    fn explain_plan_parser_returns_all_plan_rows() {
        let rows = vec![
            vec![Some(br#"[{"Plan":{"Node Type":"Seq Scan"}}]"#.to_vec())],
            vec![Some(br#"[{"Planning Time":0.1}]"#.to_vec())],
        ];

        let plan = parse_explain_plan_rows(&rows).expect("valid explain rows");

        assert_eq!(plan.len(), 2);
    }
}
