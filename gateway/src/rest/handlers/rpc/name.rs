use super::*;
use crate::server::RpcCallableSignature;

type RpcBindParams = Vec<Option<Vec<u8>>>;
type RpcParamTypeOids = Vec<u32>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super) struct RpcFunctionName {
    schema: Option<String>,
    function: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super) struct RpcBoundQuery {
    pub sql: String,
    pub params: RpcBindParams,
    pub param_type_oids: RpcParamTypeOids,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcSelectContext {
    Rows,
    Scalar,
}

impl RpcFunctionName {
    pub(in super::super) fn parse(input: &str) -> Result<Self, ApiError> {
        if input.trim().is_empty() {
            return Err(ApiError::parse_error("Function name is required"));
        }

        let segments: Vec<&str> = input.split('.').collect();
        if segments.is_empty()
            || segments.len() > 2
            || segments
                .iter()
                .any(|segment| !is_safe_ident_segment(segment))
        {
            return Err(ApiError::parse_error(
                "Invalid function name. Use [schema.]function with letters, numbers, and underscores.",
            ));
        }

        match segments.as_slice() {
            [function] => Ok(Self {
                schema: None,
                function: function.to_ascii_lowercase(),
            }),
            [schema, function] => Ok(Self {
                schema: Some(schema.to_ascii_lowercase()),
                function: function.to_ascii_lowercase(),
            }),
            _ => Err(ApiError::parse_error(
                "Invalid function name. Use [schema.]function with letters, numbers, and underscores.",
            )),
        }
    }

    pub(in super::super) fn is_schema_qualified(&self) -> bool {
        self.schema.is_some()
    }

    pub(in super::super) fn canonical(&self) -> String {
        if let Some(schema) = &self.schema {
            format!("{}.{}", schema, self.function)
        } else {
            self.function.clone()
        }
    }

    pub(in super::super) fn quoted(&self) -> String {
        if let Some(schema) = &self.schema {
            format!("{}.{}", quote_ident(schema), quote_ident(&self.function))
        } else {
            quote_ident(&self.function)
        }
    }

    pub(in super::super) fn schema_and_name(&self) -> Option<(&str, &str)> {
        self.schema
            .as_deref()
            .map(|schema| (schema, self.function.as_str()))
    }
}

#[cfg(test)]
pub(in super::super) fn json_literal_sql(value: &Value) -> Result<String, ApiError> {
    fn escape_sql_literal(val: &str) -> String {
        let clean = val
            .replace('\0', "")
            .replace('\\', "\\\\")
            .replace('\'', "''");
        format!("'{}'", clean)
    }

    match value {
        Value::Null => Ok("NULL".to_string()),
        Value::Bool(b) => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::String(s) => Ok(escape_sql_literal(s)),
        Value::Array(_) | Value::Object(_) => {
            let json = serde_json::to_string(value)
                .map_err(|e| ApiError::parse_error(format!("Invalid JSON value: {}", e)))?;
            Ok(format!("{}::jsonb", escape_sql_literal(&json)))
        }
    }
}

#[cfg(test)]
pub(in super::super) fn build_rpc_sql(
    function_name: &RpcFunctionName,
    args: Option<&Value>,
) -> Result<String, ApiError> {
    let call_target = build_rpc_call_target(function_name, args)?;
    Ok(format!("SELECT * FROM {}", call_target))
}

#[cfg(test)]
pub(in super::super) fn build_rpc_probe_sql(
    function_name: &RpcFunctionName,
    args: Option<&Value>,
) -> Result<String, ApiError> {
    let call_target = build_rpc_call_target(function_name, args)?;
    Ok(format!("SELECT {}", call_target))
}

pub(in super::super) fn build_rpc_bound_sql(
    function_name: &RpcFunctionName,
    args: Option<&Value>,
    signature: Option<&RpcCallableSignature>,
    scalar_context: bool,
) -> Result<RpcBoundQuery, ApiError> {
    let (call_target, params, param_type_oids) =
        build_rpc_bound_call_target(function_name, args, signature)?;
    if signature.is_some()
        && (param_type_oids.len() != params.len() || param_type_oids.contains(&0))
    {
        return Err(ApiError::internal(
            "Invalid RPC signature metadata: missing parameter type OID",
        ));
    }
    let sql = match if scalar_context {
        RpcSelectContext::Scalar
    } else {
        RpcSelectContext::Rows
    } {
        RpcSelectContext::Rows => format!("SELECT * FROM {}", call_target),
        RpcSelectContext::Scalar => format!("SELECT {}", call_target),
    };
    Ok(RpcBoundQuery {
        sql,
        params,
        param_type_oids,
    })
}

#[cfg(test)]
pub(in super::super) fn build_rpc_call_target(
    function_name: &RpcFunctionName,
    args: Option<&Value>,
) -> Result<String, ApiError> {
    let function_sql = function_name.quoted();

    let args_sql = match args {
        None => String::new(),
        Some(Value::Object(map)) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut parts: Vec<String> = Vec::with_capacity(keys.len());
            for key in keys {
                let normalized_key = key.to_ascii_lowercase();
                if !is_safe_ident_segment(&normalized_key) {
                    return Err(ApiError::parse_error(format!(
                        "Invalid RPC argument name '{}'",
                        key
                    )));
                }
                let value_sql = json_literal_sql(
                    map.get(key)
                        .ok_or_else(|| ApiError::parse_error("Missing RPC argument value"))?,
                )?;
                parts.push(format!("{} => {}", quote_ident(&normalized_key), value_sql));
            }
            parts.join(", ")
        }
        Some(Value::Array(items)) => {
            let mut parts: Vec<String> = Vec::with_capacity(items.len());
            for item in items {
                parts.push(json_literal_sql(item)?);
            }
            parts.join(", ")
        }
        Some(other) => json_literal_sql(other)?,
    };

    if args_sql.is_empty() {
        Ok(format!("{}()", function_sql))
    } else {
        Ok(format!("{}({})", function_sql, args_sql))
    }
}

fn build_rpc_bound_call_target(
    function_name: &RpcFunctionName,
    args: Option<&Value>,
    signature: Option<&RpcCallableSignature>,
) -> Result<(String, RpcBindParams, RpcParamTypeOids), ApiError> {
    let function_sql = function_name.quoted();
    let mut params: RpcBindParams = Vec::new();
    let mut param_type_oids: RpcParamTypeOids = Vec::new();

    let args_sql = match args {
        None => empty_variadic_arg_sql(&mut params, &mut param_type_oids, signature)?
            .unwrap_or_default(),
        Some(Value::Object(map)) if map.is_empty() => {
            empty_variadic_arg_sql(&mut params, &mut param_type_oids, signature)?
                .unwrap_or_default()
        }
        Some(Value::Object(map)) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut parts: Vec<String> = Vec::with_capacity(keys.len());
            for key in keys {
                let normalized_key = key.to_ascii_lowercase();
                if !is_safe_ident_segment(&normalized_key) {
                    return Err(ApiError::parse_error(format!(
                        "Invalid RPC argument name '{}'",
                        key
                    )));
                }

                let expected =
                    signature.and_then(|sig| signature_named_arg_info(sig, &normalized_key));
                let expr = build_rpc_param_expr(
                    &mut params,
                    &mut param_type_oids,
                    map.get(key)
                        .ok_or_else(|| ApiError::parse_error("Missing RPC argument value"))?,
                    expected,
                )?;
                parts.push(format!("{} => {}", quote_ident(&normalized_key), expr));
            }
            parts.join(", ")
        }
        Some(Value::Array(items)) if items.is_empty() => {
            empty_variadic_arg_sql(&mut params, &mut param_type_oids, signature)?
                .unwrap_or_default()
        }
        Some(Value::Array(items)) => {
            let mut parts: Vec<String> = Vec::with_capacity(items.len());
            for (idx, item) in items.iter().enumerate() {
                let expected = signature.and_then(|sig| signature_positional_arg_info(sig, idx));
                parts.push(build_rpc_param_expr(
                    &mut params,
                    &mut param_type_oids,
                    item,
                    expected,
                )?);
            }
            parts.join(", ")
        }
        Some(other) => {
            let expected = signature.and_then(|sig| signature_positional_arg_info(sig, 0));
            build_rpc_param_expr(&mut params, &mut param_type_oids, other, expected)?
        }
    };

    let call_target = if args_sql.is_empty() {
        format!("{}()", function_sql)
    } else {
        format!("{}({})", function_sql, args_sql)
    };

    Ok((call_target, params, param_type_oids))
}

fn empty_variadic_arg_sql(
    params: &mut Vec<Option<Vec<u8>>>,
    param_type_oids: &mut RpcParamTypeOids,
    signature: Option<&RpcCallableSignature>,
) -> Result<Option<String>, ApiError> {
    let Some(signature) = signature else {
        return Ok(None);
    };
    if !signature.variadic || signature.total_args != 1 {
        return Ok(None);
    }

    let oid = signature
        .arg_type_oids
        .first()
        .copied()
        .filter(|oid| *oid != 0)
        .ok_or_else(|| {
            ApiError::internal("Invalid RPC signature metadata: missing variadic array type OID")
        })?;
    params.push(Some(b"{}".to_vec()));
    param_type_oids.push(oid);
    Ok(Some(format!("VARIADIC ${}", params.len())))
}

fn build_rpc_param_expr(
    params: &mut Vec<Option<Vec<u8>>>,
    param_type_oids: &mut RpcParamTypeOids,
    value: &Value,
    expected: Option<(&str, u32)>,
) -> Result<String, ApiError> {
    params.push(encode_rpc_param_value(value, expected.map(|(ty, _)| ty))?);
    if let Some((_, oid)) = expected {
        param_type_oids.push(oid);
    }
    Ok(format!("${}", params.len()))
}

fn encode_rpc_param_value(
    value: &Value,
    expected_type: Option<&str>,
) -> Result<Option<Vec<u8>>, ApiError> {
    if matches!(value, Value::Null) {
        return Ok(None);
    }

    let expected_type = expected_type.map(str::trim).filter(|ty| !ty.is_empty());
    let expects_json = expected_type.is_some_and(is_json_rpc_type);
    let expects_array = expected_type.and_then(|ty| ty.strip_suffix("[]"));

    if expects_json {
        return serde_json::to_vec(value)
            .map(Some)
            .map_err(|e| ApiError::parse_error(format!("Invalid JSON value: {}", e)));
    }

    if let Some(element_type) = expects_array
        && let Value::Array(items) = value
    {
        return encode_pg_array_param(items, element_type).map(Some);
    }

    match value {
        Value::Null => Ok(None),
        Value::Bool(b) => Ok(Some(if *b { b"t".to_vec() } else { b"f".to_vec() })),
        Value::Number(n) => Ok(Some(n.to_string().into_bytes())),
        Value::String(s) => {
            if s.as_bytes().contains(&0) {
                return Err(ApiError::parse_error(
                    "RPC string arguments cannot contain NULL bytes",
                ));
            }
            Ok(Some(s.as_bytes().to_vec()))
        }
        Value::Array(_) | Value::Object(_) => serde_json::to_vec(value)
            .map(Some)
            .map_err(|e| ApiError::parse_error(format!("Invalid JSON value: {}", e))),
    }
}

fn encode_pg_array_param(items: &[Value], element_type: &str) -> Result<Vec<u8>, ApiError> {
    let mut out = Vec::with_capacity(items.len() * 8 + 2);
    write_pg_array_items(&mut out, items, element_type)?;
    Ok(out)
}

fn write_pg_array_items(
    out: &mut Vec<u8>,
    items: &[Value],
    element_type: &str,
) -> Result<(), ApiError> {
    out.push(b'{');
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push(b',');
        }
        write_pg_array_element(out, item, element_type)?;
    }
    out.push(b'}');
    Ok(())
}

fn write_pg_array_element(
    out: &mut Vec<u8>,
    value: &Value,
    element_type: &str,
) -> Result<(), ApiError> {
    if is_json_rpc_type(element_type) {
        let json = serde_json::to_string(value)
            .map_err(|e| ApiError::parse_error(format!("Invalid JSON value: {}", e)))?;
        write_pg_quoted_array_string(out, &json)?;
        return Ok(());
    }

    match value {
        Value::Null => out.extend_from_slice(b"NULL"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"t" } else { b"f" }),
        Value::Number(n) => out.extend_from_slice(n.to_string().as_bytes()),
        Value::String(s) => write_pg_quoted_array_string(out, s)?,
        Value::Array(items) => {
            let nested_element_type = element_type.strip_suffix("[]").unwrap_or(element_type);
            write_pg_array_items(out, items, nested_element_type)?;
        }
        Value::Object(_) => {
            return Err(ApiError::parse_error(format!(
                "RPC argument is not compatible with PostgreSQL array type '{}'",
                element_type
            )));
        }
    }
    Ok(())
}

fn write_pg_quoted_array_string(out: &mut Vec<u8>, value: &str) -> Result<(), ApiError> {
    if value.as_bytes().contains(&0) {
        return Err(ApiError::parse_error(
            "RPC string arguments cannot contain NULL bytes",
        ));
    }

    out.push(b'"');
    for byte in value.bytes() {
        if byte == b'"' || byte == b'\\' {
            out.push(b'\\');
        }
        out.push(byte);
    }
    out.push(b'"');
    Ok(())
}

fn signature_named_arg_info<'a>(
    signature: &'a RpcCallableSignature,
    arg_name: &str,
) -> Option<(&'a str, u32)> {
    signature
        .arg_names
        .iter()
        .zip(signature.arg_types.iter().zip(&signature.arg_type_oids))
        .find_map(|(name, (ty, oid))| {
            name.as_deref()
                .filter(|candidate| candidate.eq_ignore_ascii_case(arg_name))
                .map(|_| (ty.as_str(), *oid))
        })
}

fn signature_positional_arg_info(
    signature: &RpcCallableSignature,
    idx: usize,
) -> Option<(&str, u32)> {
    if signature.variadic && idx >= signature.total_args.saturating_sub(1) {
        let oid = signature
            .variadic_element_oid
            .or_else(|| signature.arg_type_oids.last().copied())
            .filter(|oid| *oid != 0)?;
        return signature
            .arg_types
            .last()
            .map(|type_name| (variadic_element_type(type_name), oid));
    }

    signature
        .arg_types
        .get(idx)
        .zip(signature.arg_type_oids.get(idx))
        .map(|(type_name, oid)| (type_name.as_str(), *oid))
}

fn variadic_element_type(type_name: &str) -> &str {
    type_name.strip_suffix("[]").unwrap_or(type_name)
}

fn is_json_rpc_type(type_name: &str) -> bool {
    matches!(
        type_name.trim().to_ascii_lowercase().as_str(),
        "json" | "jsonb"
    )
}

pub(in super::super) fn enforce_rpc_name_contract(
    require_schema_qualified: bool,
    allow_list: Option<&HashSet<String>>,
    function_name: &RpcFunctionName,
) -> Result<(), ApiError> {
    if require_schema_qualified && !function_name.is_schema_qualified() {
        return Err(ApiError::parse_error(
            "RPC function must be schema-qualified (schema.function)",
        ));
    }

    if let Some(allow_list) = allow_list {
        let key = function_name.canonical();
        if !allow_list.contains(&key) {
            crate::metrics::record_rpc_allowlist_rejection();
            return Err(ApiError::forbidden(format!(
                "RPC function '{}' is not in the allow-list",
                key
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{RpcFunctionName, build_rpc_bound_sql};
    use crate::server::RpcCallableSignature;
    use serde_json::json;

    fn variadic_int_signature() -> RpcCallableSignature {
        RpcCallableSignature {
            total_args: 1,
            default_args: 0,
            variadic: true,
            arg_names: vec![Some("nums".to_string())],
            arg_types: vec!["integer[]".to_string()],
            arg_type_oids: vec![1007],
            variadic_element_oid: Some(23),
            identity_args: "VARIADIC nums integer[]".to_string(),
            result_type: "integer".to_string(),
        }
    }

    #[test]
    fn empty_object_call_binds_empty_variadic_array() {
        let function = RpcFunctionName::parse("qail_test.sum_all").unwrap();
        let args = json!({});
        let query = build_rpc_bound_sql(
            &function,
            Some(&args),
            Some(&variadic_int_signature()),
            false,
        )
        .unwrap();

        assert_eq!(
            query.sql,
            "SELECT * FROM \"qail_test\".\"sum_all\"(VARIADIC $1)"
        );
        assert_eq!(query.params[0].as_deref(), Some(b"{}".as_slice()));
        assert_eq!(query.param_type_oids, vec![1007]);
    }

    #[test]
    fn empty_array_call_binds_empty_variadic_array() {
        let function = RpcFunctionName::parse("qail_test.sum_all").unwrap();
        let args = json!([]);
        let query = build_rpc_bound_sql(
            &function,
            Some(&args),
            Some(&variadic_int_signature()),
            false,
        )
        .unwrap();

        assert_eq!(
            query.sql,
            "SELECT * FROM \"qail_test\".\"sum_all\"(VARIADIC $1)"
        );
        assert_eq!(query.params[0].as_deref(), Some(b"{}".as_slice()));
        assert_eq!(query.param_type_oids, vec![1007]);
    }
}
