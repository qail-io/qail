use axum::{extract::State, response::Json};
use qail_core::ast::{Expr, Operator, Qail, Value as AstValue};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::middleware::ApiError;
use crate::rest::handlers::{
    minimum_required_rpc_args, normalize_pg_type_name, parse_rpc_input_arg_names,
};

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

fn normalize_contract_arg_types(
    raw_arg_types_json: &str,
    total_args: usize,
) -> Result<Vec<String>, ApiError> {
    let arg_types: Vec<String> = serde_json::from_str(raw_arg_types_json)
        .map_err(|e| ApiError::internal(format!("Invalid RPC arg type metadata: {}", e)))?;
    let arg_types: Vec<String> = arg_types
        .into_iter()
        .map(|t| normalize_pg_type_name(&t))
        .collect();

    if arg_types.len() != total_args {
        return Err(ApiError::internal(format!(
            "Invalid RPC arg type metadata length: expected {}, got {}",
            total_args,
            arg_types.len()
        )));
    }

    Ok(arg_types)
}

fn rpc_contract_allow_list_names(rpc_allow_list: &HashSet<String>) -> Vec<String> {
    let mut names: Vec<String> = rpc_allow_list
        .iter()
        .filter_map(|name| {
            let (schema_name, function_name) = name.split_once('.')?;
            callable_rpc_allow_list_key(schema_name, function_name)
        })
        .collect();
    names.sort();
    names.dedup();
    names
}

fn rpc_contract_catalog_row_limit(allowed_function_count: usize) -> i64 {
    const MIN_CONTRACT_ROWS: usize = 5_000;
    const MAX_CONTRACT_ROWS: usize = 50_000;
    const OVERLOADS_PER_FUNCTION_BUDGET: usize = 16;

    allowed_function_count
        .saturating_mul(OVERLOADS_PER_FUNCTION_BUDGET)
        .clamp(MIN_CONTRACT_ROWS, MAX_CONTRACT_ROWS) as i64
}

fn rpc_contract_catalog_cmd(allowed_rpc_names: &[String]) -> Qail {
    let allowed_values = allowed_rpc_names
        .iter()
        .cloned()
        .map(AstValue::String)
        .collect();

    Qail::get("pg_catalog.pg_proc p")
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
        .filter(
            "LOWER(n.nspname || '.' || p.proname)",
            Operator::In,
            AstValue::Array(allowed_values),
        )
        .order_asc("n.nspname")
        .order_asc("p.proname")
        .order_asc("p.oid")
        .limit(rpc_contract_catalog_row_limit(allowed_rpc_names.len()))
}

fn required_non_negative_usize(
    row: &qail_pg::PgRow,
    name: &str,
    idx: usize,
    label: &str,
) -> Result<usize, ApiError> {
    let value = row
        .try_get_by_name::<i32>(name)
        .ok()
        .or_else(|| row.get_i32(idx))
        .ok_or_else(|| ApiError::internal(format!("Invalid RPC {} metadata", label)))?;
    usize::try_from(value)
        .map_err(|_| ApiError::internal(format!("Invalid negative RPC {} metadata", label)))
}

fn required_bool(
    row: &qail_pg::PgRow,
    name: &str,
    idx: usize,
    label: &str,
) -> Result<bool, ApiError> {
    row.try_get_by_name::<bool>(name)
        .ok()
        .or_else(|| row.get_bool(idx))
        .ok_or_else(|| ApiError::internal(format!("Invalid RPC {} metadata", label)))
}

fn required_string(
    row: &qail_pg::PgRow,
    name: &str,
    idx: usize,
    label: &str,
) -> Result<String, ApiError> {
    row.try_get_by_name::<String>(name)
        .ok()
        .or_else(|| row.get_string(idx))
        .ok_or_else(|| ApiError::internal(format!("Invalid RPC {} metadata", label)))
}

fn rpc_contract_from_row(
    row: &qail_pg::PgRow,
    rpc_allow_list: &HashSet<String>,
) -> Result<Option<Value>, ApiError> {
    let schema_name = required_string(row, "schema_name", 0, "schema_name")?;
    let function_name = required_string(row, "function_name", 1, "function_name")?;
    let Some(canonical_name) = callable_rpc_allow_list_key(&schema_name, &function_name) else {
        return Ok(None);
    };
    if !rpc_allow_list.contains(&canonical_name) {
        return Ok(None);
    }

    let total_args = required_non_negative_usize(row, "total_args", 2, "total_args")?;
    let default_args = required_non_negative_usize(row, "default_args", 3, "default_args")?;
    if default_args > total_args {
        return Err(ApiError::internal(
            "Invalid RPC contract metadata: default_args exceeds total_args",
        ));
    }
    let variadic = required_bool(row, "is_variadic", 4, "is_variadic")?;
    let identity_args = required_string(row, "identity_args", 8, "identity_args")?;
    let result_type = required_string(row, "result_type", 9, "result_type")?;
    let required_args = minimum_required_rpc_args(total_args, default_args, variadic);

    let raw_arg_names_json = required_string(row, "arg_names_json", 5, "arg_names_json")?;
    let raw_arg_modes_json = required_string(row, "arg_modes_json", 6, "arg_modes_json")?;
    let arg_names =
        parse_rpc_input_arg_names(&raw_arg_names_json, &raw_arg_modes_json, total_args)?;
    let raw_arg_types_json = required_string(row, "arg_types_json", 7, "arg_types_json")?;
    let arg_types = normalize_contract_arg_types(&raw_arg_types_json, total_args)?;

    let mut args_json: Vec<Value> = Vec::with_capacity(total_args);
    for idx in 0..total_args {
        let name = arg_names
            .get(idx)
            .and_then(|v| v.as_ref())
            .map(|v| v.to_ascii_lowercase());
        let arg_type = arg_types
            .get(idx)
            .cloned()
            .ok_or_else(|| ApiError::internal("Missing RPC arg type after metadata validation"))?;
        args_json.push(json!({
            "position": idx + 1,
            "name": name,
            "type": arg_type,
            "required": idx < required_args,
            "variadic": variadic && idx + 1 == total_args,
        }));
    }

    Ok(Some(json!({
        "schema": schema_name,
        "function": function_name,
        "name": canonical_name,
        "identity_args": identity_args,
        "result_type": result_type,
        "total_args": total_args,
        "required_args": required_args,
        "default_args": default_args,
        "variadic": variadic,
        "args": args_json,
    })))
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
    let allowed_rpc_names = rpc_contract_allow_list_names(rpc_allow_list);
    if allowed_rpc_names.is_empty() {
        return Ok(Json(json!({
            "functions": [],
            "count": 0,
        })));
    }

    let mut conn = state.acquire_with_auth_rls_guarded(&auth, None).await?;

    let mut cmd = rpc_contract_catalog_cmd(&allowed_rpc_names);
    state.optimize_qail_for_execution(&mut cmd);
    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, None));
    conn.release().await;
    let rows = rows?;

    let mut functions: Vec<Value> = Vec::with_capacity(rows.len());
    for row in &rows {
        if let Some(function) = rpc_contract_from_row(row, rpc_allow_list)? {
            functions.push(function);
        }
    }

    Ok(Json(json!({
        "functions": functions,
        "count": functions.len(),
    })))
}

#[cfg(test)]
mod tests {
    use super::{
        callable_rpc_allow_list_key, normalize_contract_arg_types, rpc_contract_allow_list_names,
        rpc_contract_catalog_cmd, rpc_contract_catalog_row_limit, rpc_contract_from_row,
    };
    use crate::middleware::ApiError;
    use crate::rest::handlers::minimum_required_rpc_args;
    use qail_core::ast::{CageKind, Operator, Value as AstValue};
    use qail_pg::PgRow;
    use std::collections::HashSet;

    fn rpc_contract_row(values: &[Option<&str>]) -> PgRow {
        PgRow {
            columns: values
                .iter()
                .map(|value| value.map(|value| value.as_bytes().to_vec()))
                .collect(),
            column_info: None,
        }
    }

    fn valid_rpc_contract_row() -> PgRow {
        rpc_contract_row(&[
            Some("api"),
            Some("search_orders"),
            Some("2"),
            Some("1"),
            Some("f"),
            Some(r#"["tenant_id","limit"]"#),
            Some("[]"),
            Some(r#"["text","integer"]"#),
            Some("tenant_id text, limit integer DEFAULT 10"),
            Some("jsonb"),
        ])
    }

    fn rpc_allow_list() -> HashSet<String> {
        HashSet::from(["api.search_orders".to_string()])
    }

    fn assert_internal_error(err: ApiError) {
        assert_eq!(err.code, "INTERNAL_ERROR");
        assert_eq!(err.message, "An internal error occurred.");
    }

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

    #[test]
    fn normalize_contract_arg_types_matches_runtime_type_canonicalization() {
        let arg_types =
            normalize_contract_arg_types(r#"["UUID","\"Api\".\"My_Enum\""]"#, 2).unwrap();
        assert_eq!(arg_types, vec!["uuid", "api.my_enum"]);
    }

    #[test]
    fn normalize_contract_arg_types_rejects_malformed_or_mismatched_metadata() {
        let err = normalize_contract_arg_types("not-json", 1).unwrap_err();
        assert_internal_error(err);

        let err = normalize_contract_arg_types(r#"["text"]"#, 2).unwrap_err();
        assert_internal_error(err);
    }

    #[test]
    fn rpc_contract_from_row_accepts_complete_metadata() {
        let contract = rpc_contract_from_row(&valid_rpc_contract_row(), &rpc_allow_list())
            .unwrap()
            .unwrap();
        assert_eq!(contract["name"], "api.search_orders");
        assert_eq!(contract["total_args"], 2);
        assert_eq!(contract["required_args"], 1);
        assert_eq!(contract["args"][0]["type"], "text");
        assert_eq!(contract["args"][1]["type"], "integer");
    }

    #[test]
    fn rpc_contract_from_row_rejects_bad_required_metadata() {
        let mut row = valid_rpc_contract_row();
        row.columns[2] = Some(b"nope".to_vec());
        let err = rpc_contract_from_row(&row, &rpc_allow_list()).unwrap_err();
        assert_internal_error(err);

        let mut row = valid_rpc_contract_row();
        row.columns[3] = Some(b"3".to_vec());
        let err = rpc_contract_from_row(&row, &rpc_allow_list()).unwrap_err();
        assert_internal_error(err);
    }

    #[test]
    fn rpc_contract_from_row_rejects_arg_metadata_drift() {
        let mut row = valid_rpc_contract_row();
        row.columns[5] = Some(b"not-json".to_vec());
        let err = rpc_contract_from_row(&row, &rpc_allow_list()).unwrap_err();
        assert_eq!(err.code, "INTERNAL_ERROR");

        let mut row = valid_rpc_contract_row();
        row.columns[7] = Some(br#"["text"]"#.to_vec());
        let err = rpc_contract_from_row(&row, &rpc_allow_list()).unwrap_err();
        assert_internal_error(err);
    }

    #[test]
    fn rpc_contract_allow_list_names_filters_and_normalizes_config_values() {
        let allow_list = HashSet::from([
            "API.Search_Orders".to_string(),
            "bad-name.fn".to_string(),
            "api.search_orders".to_string(),
        ]);

        assert_eq!(
            rpc_contract_allow_list_names(&allow_list),
            vec!["api.search_orders".to_string()]
        );
    }

    #[test]
    fn rpc_contract_catalog_limit_scales_with_allow_list_size() {
        assert_eq!(rpc_contract_catalog_row_limit(1), 5_000);
        assert_eq!(rpc_contract_catalog_row_limit(400), 6_400);
        assert_eq!(rpc_contract_catalog_row_limit(10_000), 50_000);
    }

    #[test]
    fn rpc_contract_catalog_cmd_filters_to_allow_list_before_row_cap() {
        let cmd = rpc_contract_catalog_cmd(&[
            "api.search_orders".to_string(),
            "public.reprice".to_string(),
        ]);

        let allow_list_filter = cmd
            .cages
            .iter()
            .filter(|cage| matches!(cage.kind, CageKind::Filter))
            .flat_map(|cage| cage.conditions.iter())
            .find(|condition| condition.left.to_string() == "LOWER(n.nspname || '.' || p.proname)")
            .expect("catalog query should constrain functions to the allow-list");
        assert_eq!(allow_list_filter.op, Operator::In);
        assert_eq!(
            allow_list_filter.value,
            AstValue::Array(vec![
                AstValue::String("api.search_orders".to_string()),
                AstValue::String("public.reprice".to_string())
            ])
        );
    }

    #[test]
    fn minimum_required_rpc_args_treats_variadic_tail_as_optional() {
        assert_eq!(minimum_required_rpc_args(1, 0, true), 0);
        assert_eq!(minimum_required_rpc_args(2, 0, true), 1);
        assert_eq!(minimum_required_rpc_args(2, 1, true), 1);
    }
}
