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
        let total_args = required_non_negative_usize(row, "total_args", 0, "total_args")?;
        let default_args = required_non_negative_usize(row, "default_args", 1, "default_args")?;
        if default_args > total_args {
            return Err(ApiError::internal(
                "Invalid RPC signature metadata: default_args exceeds total_args",
            ));
        }
        let variadic = required_bool(row, "is_variadic", 2, "is_variadic")?;

        let raw_arg_names_json = required_string(row, "arg_names_json", 4, "arg_names_json")?;
        let raw_arg_modes_json = required_string(row, "arg_modes_json", 5, "arg_modes_json")?;
        let mut arg_names =
            parse_rpc_input_arg_names(&raw_arg_names_json, &raw_arg_modes_json, total_args)?;

        let raw_arg_type_oids =
            required_string(row, "arg_type_oids_json", 6, "arg_type_oids_json")?;
        let arg_type_oids: Vec<u32> = serde_json::from_str(&raw_arg_type_oids)
            .map_err(|e| ApiError::internal(format!("Invalid RPC arg type OID metadata: {}", e)))?;

        let raw_arg_types = required_string(row, "arg_types_json", 7, "arg_types_json")?;
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
        if arg_types.len() != total_args {
            return Err(ApiError::internal(format!(
                "Invalid RPC arg type metadata length: expected {}, got {}",
                total_args,
                arg_types.len()
            )));
        }
        if arg_type_oids.len() != total_args {
            return Err(ApiError::internal(format!(
                "Invalid RPC arg type OID metadata length: expected {}, got {}",
                total_args,
                arg_type_oids.len()
            )));
        }
        if arg_type_oids.iter().any(|oid| *oid == 0) {
            return Err(ApiError::internal(
                "Invalid RPC arg type OID metadata: OID 0 is not allowed",
            ));
        }

        let variadic_element_oid =
            optional_u32(row, "variadic_element_oid", 3, "variadic_element_oid")?;
        if variadic_element_oid == Some(0) {
            return Err(ApiError::internal(
                "Invalid RPC variadic element OID metadata: OID 0 is not allowed",
            ));
        }
        if variadic && variadic_element_oid.is_none() {
            return Err(ApiError::internal(
                "Invalid RPC signature metadata: variadic function is missing element OID",
            ));
        }

        signatures.push(RpcCallableSignature {
            total_args,
            default_args,
            variadic,
            arg_names,
            arg_types,
            arg_type_oids,
            variadic_element_oid,
            identity_args: required_string(row, "identity_args", 8, "identity_args")?,
            result_type: required_string(row, "result_type", 9, "result_type")?,
        });
    }

    Ok(signatures)
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

fn optional_u32(
    row: &qail_pg::PgRow,
    name: &str,
    idx: usize,
    label: &str,
) -> Result<Option<u32>, ApiError> {
    if row.is_null(idx) {
        return Ok(None);
    }
    let value = row
        .try_get_by_name::<i64>(name)
        .ok()
        .or_else(|| row.get_i64(idx))
        .ok_or_else(|| ApiError::internal(format!("Invalid RPC {} metadata", label)))?;
    u32::try_from(value)
        .map(Some)
        .map_err(|_| ApiError::internal(format!("Invalid RPC {} metadata", label)))
}

#[cfg(test)]
mod tests {
    use super::{parse_rpc_input_arg_names, parse_rpc_signatures};
    use qail_pg::PgRow;

    fn rpc_signature_row(values: &[Option<&str>]) -> PgRow {
        PgRow {
            columns: values
                .iter()
                .map(|value| value.map(|value| value.as_bytes().to_vec()))
                .collect(),
            column_info: None,
        }
    }

    fn valid_rpc_signature_row() -> PgRow {
        rpc_signature_row(&[
            Some("2"),
            Some("1"),
            Some("f"),
            None,
            Some(r#"["tenant_id","limit"]"#),
            Some("[]"),
            Some("[25,23]"),
            Some(r#"["text","integer"]"#),
            Some("tenant_id text, limit integer DEFAULT 10"),
            Some("jsonb"),
        ])
    }

    fn assert_internal_error(err: crate::middleware::ApiError) {
        assert_eq!(err.code, "INTERNAL_ERROR");
        assert_eq!(err.message, "An internal error occurred.");
    }

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

    #[test]
    fn parse_rpc_signatures_accepts_complete_metadata() {
        let signatures = parse_rpc_signatures(&[valid_rpc_signature_row()]).unwrap();
        assert_eq!(signatures.len(), 1);
        assert_eq!(signatures[0].total_args, 2);
        assert_eq!(signatures[0].default_args, 1);
        assert_eq!(signatures[0].arg_types, vec!["text", "integer"]);
        assert_eq!(signatures[0].arg_type_oids, vec![25, 23]);
    }

    #[test]
    fn parse_rpc_signatures_rejects_bad_required_counts() {
        let mut row = valid_rpc_signature_row();
        row.columns[0] = Some(b"not-an-int".to_vec());
        let err = parse_rpc_signatures(&[row]).unwrap_err();
        assert_internal_error(err);

        let mut row = valid_rpc_signature_row();
        row.columns[1] = Some(b"3".to_vec());
        let err = parse_rpc_signatures(&[row]).unwrap_err();
        assert_internal_error(err);
    }

    #[test]
    fn parse_rpc_signatures_rejects_type_metadata_length_mismatch() {
        let mut row = valid_rpc_signature_row();
        row.columns[6] = Some(b"[25]".to_vec());
        let err = parse_rpc_signatures(&[row]).unwrap_err();
        assert_internal_error(err);

        let mut row = valid_rpc_signature_row();
        row.columns[7] = Some(br#"["text"]"#.to_vec());
        let err = parse_rpc_signatures(&[row]).unwrap_err();
        assert_internal_error(err);
    }

    #[test]
    fn parse_rpc_signatures_rejects_zero_type_oids() {
        let mut row = valid_rpc_signature_row();
        row.columns[6] = Some(b"[25,0]".to_vec());
        let err = parse_rpc_signatures(&[row]).unwrap_err();
        assert_internal_error(err);

        let mut row = valid_rpc_signature_row();
        row.columns[2] = Some(b"t".to_vec());
        row.columns[3] = Some(b"0".to_vec());
        let err = parse_rpc_signatures(&[row]).unwrap_err();
        assert_internal_error(err);
    }

    #[test]
    fn parse_rpc_signatures_rejects_missing_variadic_element_oid() {
        let mut row = valid_rpc_signature_row();
        row.columns[2] = Some(b"t".to_vec());
        row.columns[3] = None;
        let err = parse_rpc_signatures(&[row]).unwrap_err();
        assert_internal_error(err);
    }
}
