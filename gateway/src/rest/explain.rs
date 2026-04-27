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
use super::filters::{apply_filters, parse_filters};
use super::types::ListParams;

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

    let _table = state
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
    let max_rows = state.config.max_result_rows.min(1000) as i64;
    let limit = params.limit.unwrap_or(50).clamp(1, max_rows);
    let offset = params.offset.unwrap_or(0).clamp(0, 100_000);
    let mut cmd = qail_core::ast::Qail::get(&table_name);

    // Apply select
    if let Some(ref select) = params.select {
        let cols: Vec<&str> = select
            .split(',')
            .map(|s| s.trim())
            .filter(|s| *s == "*" || crate::rest::filters::is_safe_identifier(s))
            .collect();
        if !cols.is_empty() {
            cmd = cmd.columns(cols);
        }
    }

    // Apply sorting — default to `id ASC` for deterministic pagination
    if let Some(ref sort) = params.sort {
        for part in sort.split(',') {
            let mut iter = part.splitn(2, ':');
            let col = iter.next().unwrap_or("id");
            let dir = iter.next().unwrap_or("asc");
            // SECURITY: Validate sort column identifier.
            if !crate::rest::filters::is_safe_identifier(col) {
                continue;
            }
            cmd = if dir == "desc" {
                cmd.order_desc(col)
            } else {
                cmd.order_asc(col)
            };
        }
    } else {
        cmd = cmd.order_asc("id");
    }

    // Apply expand (flat JOIN only) — enforce depth limit
    let mut has_joins = false;
    if let Some(ref expand) = params.expand {
        let relations: Vec<&str> = {
            let mut seen = std::collections::HashSet::new();
            expand
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty() && !s.starts_with("nested:") && seen.insert(*s))
                .collect()
        };
        if relations.len() > state.config.max_expand_depth {
            return Err(ApiError::parse_error(format!(
                "Too many expand relations ({}). Maximum is {}",
                relations.len(),
                state.config.max_expand_depth
            )));
        }
        for rel in relations {
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
    let filters = parse_filters(query_string);
    cmd = apply_filters(cmd, &filters);

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

    cmd = cmd.limit(limit);
    cmd = cmd.offset(offset);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // When JOINs are present, table-qualify unqualified filter columns
    if has_joins {
        use qail_core::ast::Expr;
        for cage in &mut cmd.cages {
            for cond in &mut cage.conditions {
                if let Expr::Named(ref name) = cond.left
                    && !name.contains('.')
                {
                    cond.left = Expr::Named(format!("{}.{}", table_name, name));
                }
            }
        }
    }

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
    let sql = String::from_utf8_lossy(&sql_buf).to_string();

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

    // Convert raw row data to JSON
    let plan: Vec<Value> = raw_rows
        .iter()
        .filter_map(|cols| {
            cols.first()
                .and_then(|c| c.as_ref())
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .and_then(|s| serde_json::from_str(s).ok())
        })
        .collect();

    Ok(Json(json!({
        "query": sql,
        "plan": plan,
    })))
}
