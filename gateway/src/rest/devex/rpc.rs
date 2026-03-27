use axum::{extract::State, response::Json};
use qail_core::ast::{Expr, Operator, Qail, Value as AstValue};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::middleware::ApiError;
use crate::rest::handlers::parse_rpc_input_arg_names;

fn is_callable_rpc_ident(segment: &str) -> bool {
    let mut chars = segment.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn callable_rpc_allow_list_key(schema_name: &str, function_name: &str) -> Option<String> {
    if !is_callable_rpc_ident(schema_name) || !is_callable_rpc_ident(function_name) {
        return None;
    }
    Some(format!(
        "{}.{}",
        schema_name.to_ascii_lowercase(),
        function_name.to_ascii_lowercase()
    ))
}

/// GET /api/_rpc/contracts — Introspect callable PostgreSQL function contracts.
///
/// Returns schema-qualified function signatures, argument defaults, and result types.
/// Useful for generating typed internal clients without GraphQL.
pub(crate) async fn rpc_contracts_handler(
    headers: axum::http::HeaderMap,
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<Value>, ApiError> {
    let auth = authenticate_request(state.as_ref(), &headers).await?;
    if !auth.is_authenticated() {
        return Err(ApiError::auth_error(
            "Authentication required for RPC contract introspection",
        ));
    }
    if !auth.can_use_branching() {
        return Err(ApiError::forbidden(
            "Platform administrator role required for RPC contract introspection",
        ));
    }
    let Some(rpc_allow_list) = state.rpc_allow_list.as_ref() else {
        return Err(ApiError::forbidden(
            "RPC contract endpoint is disabled until rpc_allowlist_path is configured",
        ));
    };

    let mut conn = state.acquire_with_auth_rls_guarded(&auth, None).await?;

    let mut cmd = Qail::get("pg_catalog.pg_proc p")
        .columns_expr(vec![
            Expr::Named("n.nspname AS schema_name".to_string()),
            Expr::Named("p.proname AS function_name".to_string()),
            Expr::Named("p.pronargs::int4 AS total_args".to_string()),
            Expr::Named("p.pronargdefaults::int4 AS default_args".to_string()),
            Expr::Named("(p.provariadic <> 0) AS is_variadic".to_string()),
            Expr::Named(
                "COALESCE(to_jsonb(COALESCE(p.proargnames, ARRAY[]::text[])), '[]'::jsonb)::text AS arg_names_json".to_string(),
            ),
            Expr::Named(
                "COALESCE(to_jsonb(COALESCE(p.proargmodes, ARRAY[]::\"char\"[])), '[]'::jsonb)::text AS arg_modes_json".to_string(),
            ),
            Expr::Named(
                "COALESCE((SELECT jsonb_agg((arg_oid)::regtype::text ORDER BY ord) FROM unnest(CASE WHEN p.pronargs = 0 THEN ARRAY[]::oid[] ELSE string_to_array(BTRIM(p.proargtypes::text), ' ')::oid[] END) WITH ORDINALITY AS args(arg_oid, ord)), '[]'::jsonb)::text AS arg_types_json".to_string(),
            ),
            Expr::Named(
                "pg_catalog.pg_get_function_identity_arguments(p.oid) AS identity_args"
                    .to_string(),
            ),
            Expr::Named("pg_catalog.pg_get_function_result(p.oid) AS result_type".to_string()),
        ])
        .inner_join("pg_catalog.pg_namespace n", "n.oid", "p.pronamespace")
        .eq("p.prokind", "f")
        .filter(
            "n.nspname",
            Operator::NotIn,
            AstValue::Array(vec![
                AstValue::String("pg_catalog".to_string()),
                AstValue::String("information_schema".to_string()),
            ]),
        )
        .order_asc("n.nspname")
        .order_asc("p.proname")
        .order_asc("p.oid")
        .limit(5000);
    state.optimize_qail_for_execution(&mut cmd);
    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, None));
    conn.release().await;
    let rows = rows?;

    let mut functions: Vec<Value> = Vec::with_capacity(rows.len());
    for row in &rows {
        let schema_name = row
            .try_get_by_name::<String>("schema_name")
            .ok()
            .or_else(|| row.get_string(0))
            .unwrap_or_default();
        let function_name = row
            .try_get_by_name::<String>("function_name")
            .ok()
            .or_else(|| row.get_string(1))
            .unwrap_or_default();
        let Some(canonical_name) = callable_rpc_allow_list_key(&schema_name, &function_name) else {
            continue;
        };
        if !rpc_allow_list.contains(&canonical_name) {
            continue;
        }
        let total_args = row
            .try_get_by_name::<i32>("total_args")
            .ok()
            .or_else(|| row.get_i32(2))
            .unwrap_or(0)
            .max(0) as usize;
        let default_args = row
            .try_get_by_name::<i32>("default_args")
            .ok()
            .or_else(|| row.get_i32(3))
            .unwrap_or(0)
            .max(0) as usize;
        let variadic = row
            .try_get_by_name::<bool>("is_variadic")
            .ok()
            .or_else(|| row.get_bool(4))
            .unwrap_or(false);
        let identity_args = row
            .try_get_by_name::<String>("identity_args")
            .ok()
            .or_else(|| row.get_string(8))
            .unwrap_or_default();
        let result_type = row
            .try_get_by_name::<String>("result_type")
            .ok()
            .or_else(|| row.get_string(9))
            .unwrap_or_default();

        let arg_names: Vec<Option<String>> = parse_rpc_input_arg_names(
            &row.try_get_by_name::<String>("arg_names_json")
                .ok()
                .or_else(|| row.get_string(5))
                .unwrap_or_else(|| "[]".to_string()),
            &row.try_get_by_name::<String>("arg_modes_json")
                .ok()
                .or_else(|| row.get_string(6))
                .unwrap_or_else(|| "[]".to_string()),
            total_args,
        )
        .unwrap_or_default();
        let arg_types: Vec<String> = serde_json::from_str(
            &row.try_get_by_name::<String>("arg_types_json")
                .ok()
                .or_else(|| row.get_string(7))
                .unwrap_or_else(|| "[]".to_string()),
        )
        .unwrap_or_default();

        let mut args_json: Vec<Value> = Vec::with_capacity(total_args);
        for idx in 0..total_args {
            let name = arg_names
                .get(idx)
                .and_then(|v| v.as_ref())
                .map(|v| v.to_ascii_lowercase());
            let arg_type = arg_types
                .get(idx)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            args_json.push(json!({
                "position": idx + 1,
                "name": name,
                "type": arg_type,
                "required": idx < total_args.saturating_sub(default_args),
                "variadic": variadic && idx + 1 == total_args,
            }));
        }

        functions.push(json!({
            "schema": schema_name,
            "function": function_name,
            "name": canonical_name,
            "identity_args": identity_args,
            "result_type": result_type,
            "total_args": total_args,
            "required_args": total_args.saturating_sub(default_args),
            "default_args": default_args,
            "variadic": variadic,
            "args": args_json,
        }));
    }

    Ok(Json(json!({
        "functions": functions,
        "count": functions.len(),
    })))
}

#[cfg(test)]
mod tests {
    use super::callable_rpc_allow_list_key;

    #[test]
    fn callable_rpc_allow_list_key_normalizes_safe_identifiers() {
        assert_eq!(
            callable_rpc_allow_list_key("API", "Search_Orders"),
            Some("api.search_orders".to_string())
        );
    }

    #[test]
    fn callable_rpc_allow_list_key_rejects_non_callable_identifiers() {
        assert_eq!(callable_rpc_allow_list_key("api", "search-orders"), None);
        assert_eq!(callable_rpc_allow_list_key("quoted.schema", "fn"), None);
    }
}
