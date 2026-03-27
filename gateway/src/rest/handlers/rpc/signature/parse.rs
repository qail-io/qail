use super::super::RpcFunctionName;
use super::types::normalize_pg_type_name;
use crate::middleware::ApiError;
use crate::server::RpcCallableSignature;
use qail_core::ast::{Expr, Qail};

fn normalize_rpc_arg_name(name: Option<String>) -> Option<String> {
    name.and_then(|raw| {
        let normalized = raw.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            None
        } else {
            Some(normalized)
        }
    })
}

pub(crate) fn parse_rpc_input_arg_names(
    raw_arg_names_json: &str,
    raw_arg_modes_json: &str,
    total_args: usize,
) -> Result<Vec<Option<String>>, ApiError> {
    let arg_names: Vec<Option<String>> = serde_json::from_str(raw_arg_names_json)
        .map_err(|e| ApiError::internal(format!("Invalid RPC arg name metadata: {}", e)))?;
    let arg_modes: Vec<String> = serde_json::from_str(raw_arg_modes_json)
        .map_err(|e| ApiError::internal(format!("Invalid RPC arg mode metadata: {}", e)))?;

    let mut input_arg_names = if arg_modes.is_empty() {
        arg_names
            .into_iter()
            .take(total_args)
            .map(normalize_rpc_arg_name)
            .collect::<Vec<_>>()
    } else {
        arg_names
            .into_iter()
            .zip(arg_modes)
            .filter_map(
                |(name, mode)| match mode.trim().to_ascii_lowercase().as_str() {
                    // PostgreSQL marks callable inputs as IN, INOUT, or VARIADIC.
                    "i" | "b" | "v" => Some(normalize_rpc_arg_name(name)),
                    _ => None,
                },
            )
            .collect::<Vec<_>>()
    };

    if input_arg_names.len() < total_args {
        input_arg_names.resize(total_args, None);
    } else if input_arg_names.len() > total_args {
        input_arg_names.truncate(total_args);
    }

    Ok(input_arg_names)
}

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
                "CASE WHEN p.provariadic = 0 THEN NULL ELSE p.provariadic::int8 END AS variadic_element_oid"
                    .to_string(),
            ),
            Expr::Named(
                "COALESCE(to_jsonb(COALESCE(p.proargnames, ARRAY[]::text[])), '[]'::jsonb)::text AS arg_names_json".to_string(),
            ),
            Expr::Named(
                "COALESCE(to_jsonb(COALESCE(p.proargmodes, ARRAY[]::\"char\"[])), '[]'::jsonb)::text AS arg_modes_json".to_string(),
            ),
            Expr::Named(
                "COALESCE((SELECT jsonb_agg(arg_oid ORDER BY ord) FROM unnest(CASE WHEN p.pronargs = 0 THEN ARRAY[]::oid[] ELSE string_to_array(BTRIM(p.proargtypes::text), ' ')::oid[] END) WITH ORDINALITY AS args(arg_oid, ord)), '[]'::jsonb)::text AS arg_type_oids_json".to_string(),
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

        let raw_arg_names_json = row
            .try_get_by_name::<String>("arg_names_json")
            .ok()
            .or_else(|| row.get_string(4))
            .unwrap_or_else(|| "[]".to_string());
        let raw_arg_modes_json = row
            .try_get_by_name::<String>("arg_modes_json")
            .ok()
            .or_else(|| row.get_string(5))
            .unwrap_or_else(|| "[]".to_string());
        let mut arg_names =
            parse_rpc_input_arg_names(&raw_arg_names_json, &raw_arg_modes_json, total_args)?;

        let raw_arg_type_oids = row
            .try_get_by_name::<String>("arg_type_oids_json")
            .ok()
            .or_else(|| row.get_string(6))
            .unwrap_or_else(|| "[]".to_string());
        let mut arg_type_oids: Vec<u32> = serde_json::from_str(&raw_arg_type_oids)
            .map_err(|e| ApiError::internal(format!("Invalid RPC arg type OID metadata: {}", e)))?;

        let raw_arg_types = row
            .try_get_by_name::<String>("arg_types_json")
            .ok()
            .or_else(|| row.get_string(7))
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
        if arg_type_oids.len() < total_args {
            arg_type_oids.resize(total_args, 0);
        } else if arg_type_oids.len() > total_args {
            arg_type_oids.truncate(total_args);
        }

        signatures.push(RpcCallableSignature {
            total_args,
            default_args,
            variadic,
            arg_names,
            arg_types,
            arg_type_oids,
            variadic_element_oid: row
                .try_get_by_name::<i64>("variadic_element_oid")
                .ok()
                .or_else(|| row.get_i64(3))
                .and_then(|oid| u32::try_from(oid).ok()),
            identity_args: row
                .try_get_by_name::<String>("identity_args")
                .ok()
                .or_else(|| row.get_string(8))
                .unwrap_or_default(),
            result_type: row
                .try_get_by_name::<String>("result_type")
                .ok()
                .or_else(|| row.get_string(9))
                .unwrap_or_default(),
        });
    }

    Ok(signatures)
}

#[cfg(test)]
mod tests {
    use super::parse_rpc_input_arg_names;

    #[test]
    fn input_arg_names_fall_back_to_pronargs_slice_for_all_in_functions() {
        let names =
            parse_rpc_input_arg_names(r#"["tenant_id","limit","unused_output"]"#, r#"[]"#, 2)
                .expect("parse input arg names");
        assert_eq!(
            names,
            vec![Some("tenant_id".to_string()), Some("limit".to_string())]
        );
    }

    #[test]
    fn input_arg_names_skip_out_args_when_proargmodes_present() {
        let names = parse_rpc_input_arg_names(
            r#"["tenant_id","result_count","limit"]"#,
            r#"["i","o","i"]"#,
            2,
        )
        .expect("parse input arg names");
        assert_eq!(
            names,
            vec![Some("tenant_id".to_string()), Some("limit".to_string())]
        );
    }

    #[test]
    fn input_arg_names_include_inout_and_variadic_args() {
        let names = parse_rpc_input_arg_names(
            r#"["tenant_id","cursor_token","ids"]"#,
            r#"["i","b","v"]"#,
            3,
        )
        .expect("parse input arg names");
        assert_eq!(
            names,
            vec![
                Some("tenant_id".to_string()),
                Some("cursor_token".to_string()),
                Some("ids".to_string()),
            ]
        );
    }
}
