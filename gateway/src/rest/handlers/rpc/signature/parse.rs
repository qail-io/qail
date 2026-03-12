use super::super::RpcFunctionName;
use super::types::normalize_pg_type_name;
use crate::middleware::ApiError;
use crate::server::RpcCallableSignature;
use qail_core::ast::{Expr, Qail};

pub(super) fn rpc_signature_lookup_cmd(function_name: &RpcFunctionName) -> Result<Qail, ApiError> {
    let Some((schema, function)) = function_name.schema_and_name() else {
        return Err(ApiError::parse_error(
            "rpc_signature_check requires schema-qualified function names",
        ));
    };

    Ok(Qail::get("pg_catalog.pg_proc p")
        .columns_expr(vec![
            Expr::Named("p.pronargs::int4 AS total_args".to_string()),
            Expr::Named("p.pronargdefaults::int4 AS default_args".to_string()),
            Expr::Named("(p.provariadic <> 0) AS is_variadic".to_string()),
            Expr::Named(
                "COALESCE((SELECT jsonb_agg(NULLIF(BTRIM(arg_name), '') ORDER BY ord) FROM unnest((COALESCE(p.proargnames, ARRAY[]::text[]))[1:p.pronargs]) WITH ORDINALITY AS names(arg_name, ord)), '[]'::jsonb)::text AS arg_names_json".to_string(),
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
        .eq("n.nspname", schema)
        .eq("p.proname", function)
        .order_asc("p.oid"))
}

pub(super) fn parse_rpc_signatures(
    rows: &[qail_pg::PgRow],
) -> Result<Vec<RpcCallableSignature>, ApiError> {
    let mut signatures = Vec::with_capacity(rows.len());
    for row in rows {
        let total_args = row
            .try_get_by_name::<i32>("total_args")
            .ok()
            .or_else(|| row.get_i32(0))
            .unwrap_or(0)
            .max(0) as usize;
        let default_args = row
            .try_get_by_name::<i32>("default_args")
            .ok()
            .or_else(|| row.get_i32(1))
            .unwrap_or(0)
            .max(0) as usize;
        let variadic = row
            .try_get_by_name::<bool>("is_variadic")
            .ok()
            .or_else(|| row.get_bool(2))
            .unwrap_or(false);

        let raw_arg_names = row
            .try_get_by_name::<String>("arg_names_json")
            .ok()
            .or_else(|| row.get_string(3))
            .unwrap_or_else(|| "[]".to_string());
        let mut arg_names: Vec<Option<String>> = serde_json::from_str(&raw_arg_names)
            .map_err(|e| ApiError::internal(format!("Invalid RPC arg name metadata: {}", e)))?;
        for name in &mut arg_names {
            if let Some(v) = name {
                let normalized = v.trim().to_ascii_lowercase();
                if normalized.is_empty() {
                    *name = None;
                } else {
                    *name = Some(normalized);
                }
            }
        }

        let raw_arg_types = row
            .try_get_by_name::<String>("arg_types_json")
            .ok()
            .or_else(|| row.get_string(4))
            .unwrap_or_else(|| "[]".to_string());
        let mut arg_types: Vec<String> = serde_json::from_str(&raw_arg_types)
            .map_err(|e| ApiError::internal(format!("Invalid RPC arg type metadata: {}", e)))?;
        arg_types = arg_types
            .into_iter()
            .map(|t| normalize_pg_type_name(&t))
            .collect();

        if arg_names.len() < total_args {
            arg_names.resize(total_args, None);
        } else if arg_names.len() > total_args {
            arg_names.truncate(total_args);
        }
        if arg_types.len() < total_args {
            arg_types.resize(total_args, "anyelement".to_string());
        } else if arg_types.len() > total_args {
            arg_types.truncate(total_args);
        }

        signatures.push(RpcCallableSignature {
            total_args,
            default_args,
            variadic,
            arg_names,
            arg_types,
            identity_args: row
                .try_get_by_name::<String>("identity_args")
                .ok()
                .or_else(|| row.get_string(5))
                .unwrap_or_default(),
            result_type: row
                .try_get_by_name::<String>("result_type")
                .ok()
                .or_else(|| row.get_string(6))
                .unwrap_or_default(),
        });
    }

    Ok(signatures)
}
