use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super) struct RpcFunctionName {
    schema: Option<String>,
    function: String,
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

pub(in super::super) fn json_literal_sql(value: &Value) -> Result<String, ApiError> {
    match value {
        Value::Null => Ok("NULL".to_string()),
        Value::Bool(b) => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
        Value::Array(_) | Value::Object(_) => {
            let json = serde_json::to_string(value)
                .map_err(|e| ApiError::parse_error(format!("Invalid JSON value: {}", e)))?;
            Ok(format!("'{}'::jsonb", json.replace('\'', "''")))
        }
    }
}

pub(in super::super) fn build_rpc_sql(
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
        Ok(format!("SELECT * FROM {}()", function_sql))
    } else {
        Ok(format!("SELECT * FROM {}({})", function_sql, args_sql))
    }
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
