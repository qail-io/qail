//! RPC (Remote Procedure Call) handler — invoke PostgreSQL functions via REST.
//!
//! POST /api/rpc/{function} — call PG functions with JSON arguments.
//! Includes overload resolution, signature checking, and name contracts.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::Json,
};
use serde_json::{Value, json};

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;
use crate::middleware::ApiError;
use crate::server::RpcCallableSignature;

use super::{is_safe_ident_segment, quote_ident};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RpcFunctionName {
    schema: Option<String>,
    function: String,
}

impl RpcFunctionName {
    pub(super) fn parse(input: &str) -> Result<Self, ApiError> {
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

    pub(super) fn is_schema_qualified(&self) -> bool {
        self.schema.is_some()
    }

    pub(super) fn canonical(&self) -> String {
        if let Some(schema) = &self.schema {
            format!("{}.{}", schema, self.function)
        } else {
            self.function.clone()
        }
    }

    pub(super) fn quoted(&self) -> String {
        if let Some(schema) = &self.schema {
            format!("{}.{}", quote_ident(schema), quote_ident(&self.function))
        } else {
            quote_ident(&self.function)
        }
    }

    pub(super) fn schema_and_name(&self) -> Option<(&str, &str)> {
        self.schema
            .as_deref()
            .map(|schema| (schema, self.function.as_str()))
    }
}

pub(super) fn json_literal_sql(value: &Value) -> Result<String, ApiError> {
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

pub(super) fn build_rpc_sql(
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

pub(super) fn enforce_rpc_name_contract(
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

fn normalize_pg_type_name(input: &str) -> String {
    input.trim().trim_matches('"').to_ascii_lowercase()
}

fn rpc_signature_lookup_sql(function_name: &RpcFunctionName) -> Result<String, ApiError> {
    let Some((schema, function)) = function_name.schema_and_name() else {
        return Err(ApiError::parse_error(
            "rpc_signature_check requires schema-qualified function names",
        ));
    };

    let schema_literal = schema.replace('\'', "''");
    let function_literal = function.replace('\'', "''");

    Ok(format!(
        "SELECT \
            p.pronargs::int4 AS total_args, \
            p.pronargdefaults::int4 AS default_args, \
            (p.provariadic <> 0) AS is_variadic, \
            COALESCE((\
                SELECT jsonb_agg(NULLIF(BTRIM(arg_name), '') ORDER BY ord) \
                FROM unnest((COALESCE(p.proargnames, ARRAY[]::text[]))[1:p.pronargs]) \
                     WITH ORDINALITY AS names(arg_name, ord) \
            ), '[]'::jsonb)::text AS arg_names_json, \
            COALESCE((\
                SELECT jsonb_agg((arg_oid)::regtype::text ORDER BY ord) \
                FROM unnest(\
                    CASE \
                        WHEN p.pronargs = 0 THEN ARRAY[]::oid[] \
                        ELSE string_to_array(BTRIM(p.proargtypes::text), ' ')::oid[] \
                    END\
                ) WITH ORDINALITY AS args(arg_oid, ord) \
            ), '[]'::jsonb)::text AS arg_types_json, \
            pg_catalog.pg_get_function_identity_arguments(p.oid) AS identity_args, \
            pg_catalog.pg_get_function_result(p.oid) AS result_type \
        FROM pg_catalog.pg_proc p \
        JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
        WHERE n.nspname = '{}' AND p.proname = '{}' \
        ORDER BY p.oid",
        schema_literal, function_literal
    ))
}

fn parse_rpc_signatures(rows: &[qail_pg::PgRow]) -> Result<Vec<RpcCallableSignature>, ApiError> {
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

fn is_pg_any_type(type_name: &str) -> bool {
    matches!(
        type_name,
        "any"
            | "anyelement"
            | "anyarray"
            | "anynonarray"
            | "anyenum"
            | "anyrange"
            | "anycompatible"
            | "anycompatiblearray"
            | "anycompatiblenonarray"
            | "anycompatiblerange"
            | "record"
            | "unknown"
    )
}

fn is_pg_bool_type(type_name: &str) -> bool {
    matches!(type_name, "bool" | "boolean")
}

fn is_pg_integer_type(type_name: &str) -> bool {
    matches!(
        type_name,
        "int2"
            | "smallint"
            | "int4"
            | "integer"
            | "int8"
            | "bigint"
            | "serial"
            | "bigserial"
            | "oid"
    )
}

fn is_pg_numeric_type(type_name: &str) -> bool {
    is_pg_integer_type(type_name)
        || matches!(
            type_name,
            "numeric" | "decimal" | "real" | "float4" | "double precision" | "float8" | "money"
        )
}

fn is_pg_json_type(type_name: &str) -> bool {
    matches!(type_name, "json" | "jsonb")
}

fn is_pg_string_like_type(type_name: &str) -> bool {
    matches!(
        type_name,
        "text"
            | "varchar"
            | "character varying"
            | "char"
            | "character"
            | "bpchar"
            | "uuid"
            | "name"
            | "citext"
            | "date"
            | "time"
            | "timetz"
            | "time with time zone"
            | "time without time zone"
            | "timestamp"
            | "timestamptz"
            | "timestamp with time zone"
            | "timestamp without time zone"
            | "interval"
            | "inet"
            | "cidr"
            | "macaddr"
            | "bytea"
    )
}

fn is_json_value_compatible_with_pg_type(value: &Value, type_name: &str) -> bool {
    let normalized = normalize_pg_type_name(type_name);

    if value.is_null() || is_pg_any_type(&normalized) {
        return true;
    }

    if let Some(element_type) = normalized.strip_suffix("[]") {
        return match value {
            Value::Array(items) => items
                .iter()
                .all(|item| is_json_value_compatible_with_pg_type(item, element_type)),
            _ => false,
        };
    }

    match value {
        Value::Bool(_) => is_pg_bool_type(&normalized) || is_pg_json_type(&normalized),
        Value::Number(num) => {
            if num.is_i64() || num.is_u64() {
                is_pg_numeric_type(&normalized) || is_pg_json_type(&normalized)
            } else {
                matches!(
                    normalized.as_str(),
                    "numeric" | "decimal" | "real" | "float4" | "double precision" | "float8"
                ) || is_pg_json_type(&normalized)
            }
        }
        Value::String(_) => is_pg_string_like_type(&normalized) || is_pg_json_type(&normalized),
        Value::Array(_) | Value::Object(_) => is_pg_json_type(&normalized),
        Value::Null => true,
    }
}

fn variadic_element_type(type_name: &str) -> &str {
    type_name.strip_suffix("[]").unwrap_or(type_name)
}

pub(super) fn matches_positional_signature(signature: &RpcCallableSignature, values: &[Value]) -> bool {
    let provided = values.len();
    let min_required = if signature.variadic && signature.total_args > 0 {
        signature
            .required_args()
            .min(signature.total_args.saturating_sub(1))
    } else {
        signature.required_args()
    };
    let max_allowed = if signature.variadic {
        usize::MAX
    } else {
        signature.total_args
    };

    if provided < min_required || provided > max_allowed {
        return false;
    }

    if signature.total_args == 0 {
        return provided == 0;
    }

    for (idx, value) in values.iter().enumerate() {
        let expected_type = if signature.variadic && idx >= signature.total_args.saturating_sub(1) {
            signature
                .arg_types
                .last()
                .map(|t| variadic_element_type(t))
                .unwrap_or("anyelement")
        } else {
            signature
                .arg_types
                .get(idx)
                .map(String::as_str)
                .unwrap_or("anyelement")
        };

        if !is_json_value_compatible_with_pg_type(value, expected_type) {
            return false;
        }
    }

    true
}

fn matches_named_signature(
    signature: &RpcCallableSignature,
    named_args: &serde_json::Map<String, Value>,
) -> bool {
    if named_args.len() > signature.total_args {
        return false;
    }

    let mut normalized_args: std::collections::HashMap<String, &Value> =
        std::collections::HashMap::with_capacity(named_args.len());
    for (raw_key, value) in named_args {
        if !is_safe_ident_segment(raw_key) {
            return false;
        }
        let normalized_key = raw_key.to_ascii_lowercase();
        // Reject duplicate keys after case-folding.
        if normalized_args.insert(normalized_key, value).is_some() {
            return false;
        }
    }

    let mut name_to_index = std::collections::HashMap::with_capacity(signature.arg_names.len());
    for (idx, maybe_name) in signature.arg_names.iter().enumerate() {
        if let Some(name) = maybe_name {
            name_to_index.insert(name.as_str(), idx);
        }
    }

    // All required arguments must be present and name-addressable.
    for idx in 0..signature.required_args().min(signature.total_args) {
        let Some(required_name) = signature.arg_names.get(idx).and_then(|v| v.as_ref()) else {
            return false;
        };
        if !normalized_args.contains_key(required_name) {
            return false;
        }
    }

    // All provided keys must exist and be type-compatible.
    for (normalized_key, value) in &normalized_args {
        let Some(idx) = name_to_index.get(normalized_key.as_str()) else {
            return false;
        };
        let expected_type = signature
            .arg_types
            .get(*idx)
            .map(String::as_str)
            .unwrap_or("anyelement");
        if !is_json_value_compatible_with_pg_type(value, expected_type) {
            return false;
        }
    }

    true
}

pub(super) fn signature_matches_call(signature: &RpcCallableSignature, args: Option<&Value>) -> bool {
    match args {
        None => matches_positional_signature(signature, &[]),
        Some(Value::Object(map)) => matches_named_signature(signature, map),
        Some(Value::Array(values)) => matches_positional_signature(signature, values),
        Some(single) => matches_positional_signature(signature, std::slice::from_ref(single)),
    }
}

fn format_signature_brief(signature: &RpcCallableSignature) -> String {
    let identity = if signature.identity_args.is_empty() {
        "".to_string()
    } else {
        signature.identity_args.clone()
    };
    if signature.result_type.is_empty() {
        format!("({})", identity)
    } else {
        format!("({}) -> {}", identity, signature.result_type)
    }
}

pub(super) fn select_matching_rpc_signature<'a>(
    function_name: &str,
    signatures: &'a [RpcCallableSignature],
    args: Option<&Value>,
) -> Result<&'a RpcCallableSignature, ApiError> {
    let matches: Vec<&RpcCallableSignature> = signatures
        .iter()
        .filter(|sig| signature_matches_call(sig, args))
        .collect();

    if matches.is_empty() {
        let available = signatures
            .iter()
            .map(format_signature_brief)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ApiError::parse_error(format!(
            "RPC arguments do not match any overload for '{}'. Available overloads: {}",
            function_name, available
        )));
    }

    if matches.len() > 1 {
        let matched = matches
            .iter()
            .map(|sig| format_signature_brief(sig))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ApiError::parse_error(format!(
            "RPC call is ambiguous for '{}'. Matching overloads: {}",
            function_name, matched
        )));
    }

    Ok(matches[0])
}

fn next_rpc_probe_stmt_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!("qail_rpc_probe_{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

async fn probe_rpc_overload_resolution(
    conn: &mut qail_pg::PooledConnection,
    sql: &str,
) -> Result<(), qail_pg::PgError> {
    let stmt = next_rpc_probe_stmt_name();
    let probe = format!("PREPARE {} AS {}; DEALLOCATE {}", stmt, sql, stmt);
    conn.get_mut()?.execute_simple(&probe).await
}

fn map_probe_resolution_error(
    err: &qail_pg::PgError,
    function_name: &str,
    signatures: &[RpcCallableSignature],
) -> ApiError {
    let available = signatures
        .iter()
        .map(format_signature_brief)
        .collect::<Vec<_>>()
        .join(", ");

    if let Some(server) = err.server_error() {
        match server.code.as_str() {
            // ambiguous_function
            "42725" => {
                crate::metrics::record_rpc_signature_rejection("ambiguous");
                return ApiError::parse_error(format!(
                    "RPC call is ambiguous for '{}'. Available overloads: {}",
                    function_name, available
                ));
            }
            // undefined_function or undefined_column (named arg mismatch path)
            "42883" | "42703" => {
                crate::metrics::record_rpc_signature_rejection("no_match");
                return ApiError::parse_error(format!(
                    "RPC arguments do not match any overload for '{}'. Available overloads: {}",
                    function_name, available
                ));
            }
            _ => {}
        }
    }

    ApiError::from_pg_driver_error(err, None)
}

async fn enforce_rpc_signature_contract(
    state: &Arc<GatewayState>,
    conn: &mut qail_pg::PooledConnection,
    function_name: &RpcFunctionName,
    args: Option<&Value>,
    sql: &str,
) -> Result<(), ApiError> {
    if !state.config.rpc_signature_check {
        return Ok(());
    }

    let key = function_name.canonical();
    let signatures = if let Some(cached) = state.rpc_signature_cache.get(&key) {
        crate::metrics::record_rpc_signature_cache_hit();
        cached
    } else {
        crate::metrics::record_rpc_signature_cache_miss();
        let sql = rpc_signature_lookup_sql(function_name)?;
        let cmd = qail_core::ast::Qail::raw_sql(sql);
        let rows = conn
            .fetch_all_uncached(&cmd)
            .await
            .map_err(|e| ApiError::from_pg_driver_error(&e, None))?;

        if rows.is_empty() {
            crate::metrics::record_rpc_signature_rejection("not_found");
            return Err(ApiError::not_found(&key));
        }

        let parsed = parse_rpc_signatures(&rows)?;
        let cached = Arc::new(parsed);
        state
            .rpc_signature_cache
            .insert(key.clone(), Arc::clone(&cached));
        cached
    };

    // Fast path: local resolver found one unambiguous overload.
    if select_matching_rpc_signature(&key, signatures.as_ref(), args).is_ok() {
        return Ok(());
    }

    // Authoritative parity path: ask PostgreSQL parser/analyzer to resolve.
    match probe_rpc_overload_resolution(conn, sql).await {
        Ok(()) => {
            crate::metrics::record_rpc_signature_local_mismatch();
            Ok(())
        }
        Err(err) => Err(map_probe_resolution_error(&err, &key, signatures.as_ref())),
    }
}

/// POST /api/rpc/{function} — invoke PostgreSQL functions with JSON args.
///
/// Body forms:
/// - object: named args (`{ "tenant_id": "...", "limit": 10 }`)
/// - array: positional args (`["...", 10]`)
/// - scalar/null: single positional argument
/// - empty body: no arguments
pub(crate) async fn rpc_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(function_name): Path<String>,
    request: axum::extract::Request,
) -> Result<Json<Value>, ApiError> {
    let started_at = Instant::now();
    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let function = RpcFunctionName::parse(&function_name)?;
    enforce_rpc_name_contract(
        state.config.rpc_require_schema_qualified,
        state.rpc_allow_list.as_ref(),
        &function,
    )?;

    let mut policy_probe = qail_core::ast::Qail::get(function.canonical());
    state
        .policy_engine
        .apply_policies(&auth, &mut policy_probe)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|e| ApiError::parse_error(e.to_string()))?;
    let args: Option<Value> = if body.is_empty() {
        None
    } else {
        Some(
            serde_json::from_slice(&body)
                .map_err(|e| ApiError::parse_error(format!("Invalid RPC JSON body: {}", e)))?,
        )
    };

    let result_format = match headers
        .get("x-qail-result-format")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
    {
        None | Some("") => qail_pg::ResultFormat::Text,
        Some(v) if v.eq_ignore_ascii_case("text") => qail_pg::ResultFormat::Text,
        Some(v) if v.eq_ignore_ascii_case("binary") => qail_pg::ResultFormat::Binary,
        Some(other) => {
            return Err(ApiError::parse_error(format!(
                "Invalid x-qail-result-format '{}'. Use 'text' or 'binary'.",
                other
            )));
        }
    };
    let result_format_label = if matches!(result_format, qail_pg::ResultFormat::Binary) {
        "binary"
    } else {
        "text"
    };

    let sql = match build_rpc_sql(&function, args.as_ref()) {
        Ok(sql) => sql,
        Err(err) => {
            crate::metrics::record_rpc_call(
                started_at.elapsed().as_secs_f64() * 1000.0,
                false,
                result_format_label,
            );
            return Err(err);
        }
    };

    let mut conn = state.acquire_with_auth_rls_guarded(&auth, None).await?;

    if let Err(err) =
        enforce_rpc_signature_contract(&state, &mut conn, &function, args.as_ref(), &sql).await
    {
        conn.release().await;
        crate::metrics::record_rpc_call(
            started_at.elapsed().as_secs_f64() * 1000.0,
            false,
            result_format_label,
        );
        return Err(err);
    }

    let cmd = qail_core::ast::Qail::raw_sql(sql);

    let rows = match conn
        .fetch_all_uncached_with_format(&cmd, result_format)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            conn.release().await;
            crate::metrics::record_rpc_call(
                started_at.elapsed().as_secs_f64() * 1000.0,
                false,
                result_format_label,
            );
            return Err(ApiError::from_pg_driver_error(&e, None));
        }
    };

    conn.release().await;

    let data: Vec<Value> = rows.iter().map(row_to_json).collect();
    let count = data.len();
    crate::metrics::record_rpc_call(
        started_at.elapsed().as_secs_f64() * 1000.0,
        true,
        result_format_label,
    );

    Ok(Json(json!({
        "data": data,
        "count": count,
        "function": function.canonical(),
        "result_format": result_format_label,
    })))
}
