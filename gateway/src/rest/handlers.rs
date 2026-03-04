//! CRUD handlers for REST endpoints.
//!
//! - `list_handler` — GET /api/{table} with pagination, filtering, sorting, expand, streaming
//! - `aggregate_handler` — GET /api/{table}/aggregate
//! - `get_by_id_handler` — GET /api/{table}/:id
//! - `create_handler` — POST /api/{table}
//! - `update_handler` — PATCH /api/{table}/:id
//! - `delete_handler` — DELETE /api/{table}/:id

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{
        HeaderMap, StatusCode,
        header::{CONTENT_TYPE, HeaderValue},
    },
    response::{IntoResponse, Json, Response},
};
use qail_core::ast::{AggregateFunc, Expr, JoinKind, Operator, Value as QailValue};
use qail_core::transpiler::ToSql;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use uuid::Uuid;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;
use crate::middleware::ApiError;
use crate::policy::OperationType;
use crate::server::RpcCallableSignature;

use super::branch::{apply_branch_overlay, redirect_to_overlay};
use super::filters::{
    apply_filters, apply_returning, apply_sorting, json_to_qail_value, parse_filters,
    parse_scalar_value,
};
use super::nested::expand_nested;
use super::types::*;
use super::{debug_sql, extract_branch_from_headers, extract_table_name, is_debug_request};

/// Parse the primary sort column and direction for cursor pagination.
///
/// Supports:
/// - prefix style: `-col`, `+col`
/// - explicit style: `col:desc`, `col:asc`
/// - default style: `col`
///
/// Falls back to `id ASC` when sort is missing or malformed.
fn primary_sort_for_cursor(sort: Option<&str>) -> (String, bool) {
    let first = sort
        .and_then(|s| s.split(',').map(str::trim).find(|p| !p.is_empty()))
        .unwrap_or("id");

    if let Some(col) = first.strip_prefix('-') {
        let col = col.trim();
        return if col.is_empty() || !crate::rest::filters::is_safe_identifier(col) {
            ("id".to_string(), true)
        } else {
            (col.to_string(), true)
        };
    }

    if let Some(col) = first.strip_prefix('+') {
        let col = col.trim();
        return if col.is_empty() || !crate::rest::filters::is_safe_identifier(col) {
            ("id".to_string(), false)
        } else {
            (col.to_string(), false)
        };
    }

    if let Some((col, dir)) = first.split_once(':') {
        let col = col.trim();
        let is_desc = dir.trim().eq_ignore_ascii_case("desc");
        return if col.is_empty() || !crate::rest::filters::is_safe_identifier(col) {
            ("id".to_string(), is_desc)
        } else {
            (col.to_string(), is_desc)
        };
    }

    let col = first.trim();
    if col.is_empty() || !crate::rest::filters::is_safe_identifier(col) {
        ("id".to_string(), false)
    } else {
        (col.to_string(), false)
    }
}

/// PostgREST-compatible `Prefer` header directives (Phase 4).
///
/// Supported directives:
/// - `resolution=merge-duplicates` → auto-upsert on PK conflict
/// - `resolution=ignore-duplicates` → INSERT ... ON CONFLICT DO NOTHING
/// - `return=representation` → return the created/upserted row(s)
/// - `return=minimal` → return 201 with no body
#[derive(Debug, Default)]
struct PreferDirectives {
    resolution: Option<String>,
    return_mode: Option<String>,
}

impl PreferDirectives {
    fn wants_upsert(&self) -> bool {
        self.resolution.as_deref() == Some("merge-duplicates")
    }

    fn wants_ignore_duplicates(&self) -> bool {
        self.resolution.as_deref() == Some("ignore-duplicates")
    }

    fn wants_minimal(&self) -> bool {
        matches!(
            self.return_mode.as_deref(),
            Some("minimal") | Some("headers-only")
        )
    }
}

/// Parse the `Prefer` header into structured directives.
fn parse_prefer_header(headers: &HeaderMap) -> PreferDirectives {
    let mut directives = PreferDirectives::default();

    let Some(value) = headers.get("prefer").and_then(|v| v.to_str().ok()) else {
        return directives;
    };

    for part in value.split(',').flat_map(|s| s.split(';')) {
        let part = part.trim();
        if let Some((key, val)) = part.split_once('=') {
            match key.trim().to_ascii_lowercase().as_str() {
                "resolution" => directives.resolution = Some(val.trim().to_ascii_lowercase()),
                "return" => directives.return_mode = Some(val.trim().to_ascii_lowercase()),
                _ => {}
            }
        }
    }

    directives
}

fn is_safe_ident_segment(segment: &str) -> bool {
    let mut chars = segment.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn quote_ident(segment: &str) -> String {
    format!("\"{}\"", segment.replace('"', "\"\""))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RpcFunctionName {
    schema: Option<String>,
    function: String,
}

impl RpcFunctionName {
    fn parse(input: &str) -> Result<Self, ApiError> {
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

    fn is_schema_qualified(&self) -> bool {
        self.schema.is_some()
    }

    fn canonical(&self) -> String {
        if let Some(schema) = &self.schema {
            format!("{}.{}", schema, self.function)
        } else {
            self.function.clone()
        }
    }

    fn quoted(&self) -> String {
        if let Some(schema) = &self.schema {
            format!("{}.{}", quote_ident(schema), quote_ident(&self.function))
        } else {
            quote_ident(&self.function)
        }
    }

    fn schema_and_name(&self) -> Option<(&str, &str)> {
        self.schema
            .as_deref()
            .map(|schema| (schema, self.function.as_str()))
    }
}

fn json_literal_sql(value: &Value) -> Result<String, ApiError> {
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

fn build_rpc_sql(
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

fn enforce_rpc_name_contract(
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

fn matches_positional_signature(signature: &RpcCallableSignature, values: &[Value]) -> bool {
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

fn signature_matches_call(signature: &RpcCallableSignature, args: Option<&Value>) -> bool {
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

fn select_matching_rpc_signature<'a>(
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

/// SECURITY: Runtime guard — reject requests targeting inaccessible tables.
/// Allowlist takes precedence: if set, only listed tables are allowed.
/// Otherwise falls back to blocklist check.
/// Belt-and-suspenders: routes for blocked tables are not registered,
/// but this catches edge cases (e.g., expand references, nested routes).
fn check_table_not_blocked(state: &GatewayState, table_name: &str) -> Result<(), ApiError> {
    if !state.allowed_tables.is_empty() {
        // Allowlist mode: only allow listed tables
        if !state.allowed_tables.contains(table_name) {
            return Err(ApiError::forbidden(format!(
                "Table '{}' is not accessible via REST",
                table_name
            )));
        }
    } else if state.blocked_tables.contains(table_name) {
        return Err(ApiError::forbidden(format!(
            "Table '{}' is not accessible via REST",
            table_name
        )));
    }
    Ok(())
}

/// GET /api/{table} — list with pagination, sorting, filtering, column selection
pub(crate) async fn list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
    request: axum::extract::Request,
) -> Result<Response, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    let _table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // Build Qail AST
    let max_rows = state.config.max_result_rows.min(1000) as i64;
    let limit = params.limit.unwrap_or(50).clamp(1, max_rows);
    let offset = params.offset.unwrap_or(0).clamp(0, 100_000);

    let mut cmd = qail_core::ast::Qail::get(&table_name);

    // Column selection
    if let Some(ref select) = params.select {
        let mut cols: Vec<&str> = select
            .split(',')
            .map(|s| s.trim())
            .filter(|s| *s == "*" || crate::rest::filters::is_safe_identifier(s))
            .collect();

        // SECURITY: Ensure tenant column is always projected so verify_tenant_boundary()
        // can check row ownership. Without this, a malicious client could bypass the
        // tenant guard by omitting the tenant column from `select`.
        if !cols.contains(&"*")
            && auth.tenant_id.is_some()
            && !cols
                .iter()
                .any(|c| *c == state.config.tenant_column.as_str())
        {
            cols.push(&state.config.tenant_column);
        }

        if !cols.is_empty() {
            cmd = cmd.columns(cols);
        }
    }

    // Sorting (multi-column) — default to `id ASC` for deterministic pagination
    if let Some(ref sort) = params.sort {
        cmd = apply_sorting(cmd, sort);
    } else {
        cmd = cmd.order_asc("id");
    }

    // Distinct
    if let Some(ref distinct) = params.distinct {
        let cols: Vec<&str> = distinct
            .split(',')
            .map(|s| s.trim())
            .filter(|s| crate::rest::filters::is_safe_identifier(s))
            .collect();
        if !cols.is_empty() {
            cmd = cmd.distinct_on(cols);
        }
    }

    // Expand FK relations via LEFT JOIN
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
            // SECURITY: Block expand into blocked tables
            check_table_not_blocked(&state, rel)?;

            // Try: this table references `rel` (forward: orders?expand=users)
            if let Some((fk_col, ref_col)) = state.schema.relation_for(&table_name, rel) {
                let left = format!("{}.{}", table_name, fk_col);
                let right = format!("{}.{}", rel, ref_col);
                cmd = cmd.join(JoinKind::Left, rel, &left, &right);
                has_joins = true;
                continue;
            }
            // Reverse relation (one-to-many) multiplies parent rows on flat JOIN.
            // Force nested expansion to preserve parent-row semantics.
            if state.schema.relation_for(rel, &table_name).is_some() {
                return Err(ApiError::parse_error(format!(
                    "Reverse relation '{}' expands one-to-many and can duplicate parent rows. Use 'nested:{}' instead.",
                    rel, rel
                )));
            }
            return Err(ApiError::parse_error(format!(
                "No relation between '{}' and '{}'",
                table_name, rel
            )));
        }
    }

    // When JOINs are present, table-qualify base table columns in SELECT
    // to avoid ambiguous column errors (e.g., both tables have `tenant_id`)
    if has_joins {
        if cmd.columns.is_empty() || cmd.columns == vec![Expr::Named("*".into())] {
            // SELECT * → qualify with table name: SELECT base_table.*
            cmd.columns = vec![Expr::Named(format!("{}.*", table_name))];
        } else {
            // Qualify each unqualified column: col → base_table.col
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

    // Parse and apply filters from query string
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters(query_string);
    cmd = apply_filters(cmd, &filters);

    // Cursor-based pagination: filter rows after the cursor value
    if let Some(ref cursor) = params.cursor {
        let (sort_col, sort_desc) = primary_sort_for_cursor(params.sort.as_deref());
        let cursor_val = parse_scalar_value(cursor);
        if sort_desc {
            cmd = cmd.lt(&sort_col, cursor_val);
        } else {
            cmd = cmd.gt(&sort_col, cursor_val);
        }
    }

    // Full-text search
    if let Some(ref term) = params.search {
        let cols = params.search_columns.as_deref().unwrap_or("name");
        // SECURITY: Validate search column identifier.
        if crate::rest::filters::is_safe_identifier(cols) {
            cmd = cmd.filter(cols, Operator::TextSearch, QailValue::String(term.clone()));
        } else {
            tracing::warn!(cols = %cols, "search_columns rejected by identifier guard");
        }
    }

    // Pagination
    cmd = cmd.limit(limit);
    cmd = cmd.offset(offset);

    // Apply RLS policies
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // When JOINs are present, table-qualify unqualified filter columns
    // to avoid ambiguous column errors (e.g., RLS `tenant_id` → `base_table.tenant_id`)
    if has_joins {
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

    // Build cache key from full URI + user identity
    let is_streaming = params.stream.unwrap_or(false);
    let has_branch = headers.get("x-branch-id").is_some();
    let has_nested = params
        .expand
        .as_deref()
        .is_some_and(|e| e.contains("nested:"));
    let can_cache = !is_streaming && !has_branch && !has_nested;
    // SECURITY (E1): Include tenant_id to prevent cross-tenant cache poisoning.
    let tenant = auth.tenant_id.as_deref().unwrap_or("_anon");
    let cache_key = format!(
        "rest:{}:{}:{}:{}",
        tenant,
        table_name,
        auth.user_id,
        request.uri()
    );

    // Check cache for simple read queries
    if can_cache && let Some(cached) = state.cache.get(&cache_key) {
        let mut response = Response::new(Body::from(cached));
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        response
            .headers_mut()
            .insert("x-cache", HeaderValue::from_static("HIT"));
        return Ok(response);
    }

    // ── Per-tenant concurrency guard ────────────────────────────────────
    let tenant_id = auth.to_rls_context().operator_id.clone();
    let _concurrency_permit = state
        .tenant_semaphore
        .try_acquire(&tenant_id)
        .await
        .ok_or_else(|| {
            tracing::warn!(
                tenant = %tenant_id,
                table = %table_name,
                "Tenant concurrency limit reached"
            );
            ApiError::rate_limited()
        })?;

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // ── EXPLAIN Pre-check ──────────────────────────────────────────────
    // Run EXPLAIN (FORMAT JSON) for queries with expand depth ≥ threshold
    // to reject outrageously expensive queries before they consume resources.
    {
        use qail_pg::explain::{ExplainMode, check_estimate};
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let expand_depth = params
            .expand
            .as_deref()
            .map(|e| e.split(',').filter(|s| !s.trim().is_empty()).count())
            .unwrap_or(0);

        let should_explain = match state.explain_config.mode {
            ExplainMode::Off => false,
            ExplainMode::Enforce => true,
            ExplainMode::Precheck => expand_depth >= state.explain_config.depth_threshold,
        };

        if should_explain {
            // Hash the SQL shape for cache lookup
            let sql_shape = cmd.to_sql();
            let mut hasher = DefaultHasher::new();
            sql_shape.hash(&mut hasher);
            let shape_hash = hasher.finish();

            let estimate = if let Some(cached) = state.explain_cache.get(shape_hash, None) {
                cached
            } else {
                // Run EXPLAIN on the live connection
                match conn.explain_estimate(&cmd).await {
                    Ok(Some(est)) => {
                        state.explain_cache.insert(shape_hash, est.clone());
                        est
                    }
                    Ok(None) => {
                        // SECURITY (E8): In Enforce mode, fail closed.
                        if matches!(state.explain_config.mode, ExplainMode::Enforce) {
                            tracing::warn!(
                                table = %table_name,
                                sql = %sql_shape,
                                "EXPLAIN pre-check: parse failure in Enforce mode — rejecting query"
                            );
                            conn.release().await;
                            return Err(ApiError::internal(
                                "EXPLAIN pre-check failed (enforce mode)",
                            ));
                        }
                        tracing::warn!(
                            table = %table_name,
                            sql = %sql_shape,
                            "EXPLAIN pre-check: failed to parse EXPLAIN output, allowing query"
                        );
                        qail_pg::explain::ExplainEstimate {
                            total_cost: 0.0,
                            plan_rows: 0,
                        }
                    }
                    Err(e) => {
                        // SECURITY (E8): In Enforce mode, fail closed.
                        if matches!(state.explain_config.mode, ExplainMode::Enforce) {
                            tracing::warn!(
                                table = %table_name,
                                error = %e,
                                "EXPLAIN pre-check: EXPLAIN failed in Enforce mode — rejecting query"
                            );
                            conn.release().await;
                            return Err(ApiError::internal(
                                "EXPLAIN pre-check failed (enforce mode)",
                            ));
                        }
                        tracing::warn!(
                            table = %table_name,
                            error = %e,
                            "EXPLAIN pre-check: EXPLAIN query failed, allowing query"
                        );
                        qail_pg::explain::ExplainEstimate {
                            total_cost: 0.0,
                            plan_rows: 0,
                        }
                    }
                }
            };
            // P1-E: Log cost estimates for observability
            tracing::info!(
                table = %table_name,
                explain_cost = estimate.total_cost,
                explain_rows = estimate.plan_rows,
                expand_depth,
                "EXPLAIN estimate"
            );

            let decision = check_estimate(&estimate, &state.explain_config);
            if decision.is_rejected() {
                let msg = decision.rejection_message().unwrap_or_default();
                let Some(detail) = decision.rejection_detail() else {
                    tracing::error!(
                        table = %table_name,
                        "EXPLAIN pre-check rejected query without rejection detail"
                    );
                    conn.release().await;
                    return Err(ApiError::internal("EXPLAIN pre-check rejected query"));
                };
                tracing::warn!(
                    table = %table_name,
                    cost = estimate.total_cost,
                    rows = estimate.plan_rows,
                    expand_depth,
                    "EXPLAIN pre-check REJECTED query"
                );
                conn.release().await;
                return Err(ApiError::too_expensive(msg, detail));
            }
        }
    }

    let timer = crate::metrics::QueryTimer::new(&table_name, "select");
    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&table_name)));
    timer.finish(rows.is_ok());

    // Release connection early — after this point only JSON processing remains.
    // Branch overlay still needs conn, so we do it before release.
    let mut data: Vec<Value> = match &rows {
        Ok(rows) => rows.iter().map(row_to_json).collect(),
        Err(_) => Vec::new(),
    };

    // Branch overlay merge (CoW Read) — admin-gated
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        if auth.role != "admin" && auth.role != "super_admin" {
            conn.release().await;
            return Err(ApiError::forbidden(
                "Admin role required for branch overlay reads",
            ));
        }
        let pk_col = _table.primary_key.as_deref().unwrap_or("id");
        apply_branch_overlay(&mut conn, branch_name, &table_name, &mut data, pk_col).await;
    }

    // Deterministic cleanup — connection is no longer needed
    conn.release().await;

    // Now propagate the error if query failed
    let _rows = rows?;

    // ── Tenant Boundary Invariant ────────────────────────────────────
    if let Some(ref tenant_id) = auth.tenant_id {
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &data,
            tenant_id,
            &state.config.tenant_column,
            &table_name,
            "rest_list",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }

    let count = data.len();

    // Nested FK expansion: `?expand=nested:users,nested:items`
    // Runs sub-queries for each relation and stitches into nested JSON
    if let Some(ref expand) = params.expand {
        let nested_rels: Vec<&str> = expand
            .split(',')
            .map(|s| s.trim())
            .filter(|s| s.starts_with("nested:"))
            .map(|s| &s[7..])
            .collect();

        if !nested_rels.is_empty() && !data.is_empty() {
            expand_nested(&state, &table_name, &mut data, &nested_rels, &auth).await?;
        }
    }

    // NDJSON streaming: one JSON object per line
    if is_streaming {
        let mut body = String::new();
        for row in &data {
            body.push_str(&serde_json::to_string(row).unwrap_or_default());
            body.push('\n');
        }
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-ndjson"),
        );
        return Ok(response);
    }

    let response_body = ListResponse {
        data,
        count,
        total: None,
        limit,
        offset,
    };

    let debug = is_debug_request(&headers);
    let debug_sql_str = if debug { Some(debug_sql(&cmd)) } else { None };

    // Store in cache for simple queries
    if can_cache && let Ok(json) = serde_json::to_string(&response_body) {
        state.cache.set(&cache_key, &table_name, json);
    }

    let mut response = Json(response_body).into_response();

    // Attach debug headers if X-Qail-Debug was requested
    if let Some(sql) = debug_sql_str {
        let hdrs = response.headers_mut();
        if let Ok(val) = axum::http::HeaderValue::from_str(&sql) {
            hdrs.insert("x-qail-sql", val);
        }
        if let Ok(val) = axum::http::HeaderValue::from_str(&table_name) {
            hdrs.insert("x-qail-table", val);
        }
    }

    Ok(response)
}

/// GET /api/{table}/aggregate — aggregation queries
///
/// `?func=count`                      → SELECT COUNT(*) FROM table
/// `?func=sum&column=price`           → SELECT SUM(price) FROM table
/// `?func=avg&column=price&group_by=status`  → SELECT status, AVG(price) FROM table GROUP BY status
/// `?name.eq=John`                    → with filters
pub(crate) async fn aggregate_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(params): Query<AggregateParams>,
    request: axum::extract::Request,
) -> Result<Json<AggregateResponse>, ApiError> {
    // Extract table from path: /api/{table}/aggregate → table
    let path = request.uri().path().to_string();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if parts.len() < 3 || parts[0] != "api" {
        return Err(ApiError::not_found("aggregate route"));
    }
    let table_name = parts[1].to_string();
    check_table_not_blocked(&state, &table_name)?;

    let _table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    let func_name = params.func.as_deref().unwrap_or("count");
    let agg_func = match func_name.to_lowercase().as_str() {
        "count" => AggregateFunc::Count,
        "sum" => AggregateFunc::Sum,
        "avg" => AggregateFunc::Avg,
        "min" => AggregateFunc::Min,
        "max" => AggregateFunc::Max,
        _ => {
            return Err(ApiError::parse_error(format!(
                "Unknown aggregate function: '{}'. Use: count, sum, avg, min, max",
                func_name
            )));
        }
    };

    let col_name = params.column.as_deref().unwrap_or("*");

    // SECURITY: Validate aggregate column identifier.
    if col_name != "*" && !crate::rest::filters::is_safe_identifier(col_name) {
        return Err(ApiError::parse_error(format!(
            "Invalid aggregate column: '{}'",
            col_name
        )));
    }

    let is_distinct = params
        .distinct
        .as_deref()
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    // Build aggregate expression
    let agg_expr = Expr::Aggregate {
        col: if col_name == "*" {
            "*".to_string()
        } else {
            col_name.to_string()
        },
        func: agg_func,
        distinct: is_distinct,
        filter: None,
        alias: None,
    };

    let mut cmd = qail_core::ast::Qail::get(&table_name).column_expr(agg_expr);

    // Group by
    if let Some(ref group_by) = params.group_by {
        let group_exprs: Vec<Expr> = group_by
            .split(',')
            .map(|s| s.trim())
            .filter(|s| crate::rest::filters::is_safe_identifier(s))
            .map(|s| Expr::Named(s.to_string()))
            .collect();
        // Add group-by columns to SELECT so they appear in the result
        for expr in &group_exprs {
            cmd = cmd.column_expr(expr.clone());
        }
        cmd = cmd.group_by_expr(group_exprs);
    }

    // Parse and apply filters from query string
    let query_string = request.uri().query().unwrap_or("");
    let filters = parse_filters(query_string);
    cmd = apply_filters(cmd, &filters);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    let rows = conn
        .fetch_all_uncached(&cmd)
        .await
        .map_err(|e| ApiError::from_pg_driver_error(&e, Some(&table_name)));

    conn.release().await;
    let rows = rows?;

    let data: Vec<Value> = rows.iter().map(row_to_json).collect();

    // ── Tenant Boundary Invariant ────────────────────────────────────
    if let Some(ref tenant_id) = auth.tenant_id {
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &data,
            tenant_id,
            &state.config.tenant_column,
            &table_name,
            "rest_aggregate",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }

    let count = data.len();

    Ok(Json(AggregateResponse { data, count }))
}

/// GET /api/{table}/:id — get single row by PK
pub(crate) async fn get_by_id_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    request: axum::extract::Request,
) -> Result<Json<SingleResponse>, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    // F5: Accept any PK type (UUID, text, integer, serial, etc.)
    // Let Postgres validate the value against the actual column type.
    if id.is_empty() {
        return Err(ApiError::parse_error(
            "ID parameter cannot be empty".to_string(),
        ));
    }

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let pk = table
        .primary_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // Build: get table[pk = $id] — use String value; PG handles type coercion
    let mut cmd = qail_core::ast::Qail::get(&table_name)
        .filter(pk, Operator::Eq, QailValue::String(id.clone()))
        .limit(1);

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    let rows = match conn.fetch_all_uncached(&cmd).await {
        Ok(rows) => rows,
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    let row = match rows.first() {
        Some(row) => row,
        None => {
            conn.release().await;
            return Err(ApiError::not_found(format!("{}/{}", table_name, id)));
        }
    };

    let mut data = row_to_json(row);

    // Branch overlay: check if this row is overridden on the branch — admin-gated
    let branch_ctx = extract_branch_from_headers(&headers);
    if let Some(branch_name) = branch_ctx.branch_name() {
        if auth.role != "admin" && auth.role != "super_admin" {
            conn.release().await;
            return Err(ApiError::forbidden(
                "Admin role required for branch overlay reads",
            ));
        }
        let sql = qail_pg::driver::branch_sql::read_overlay_sql(branch_name, &table_name);
        if let Ok(pg_conn) = conn.get_mut()
            && let Ok(overlay_rows) = pg_conn.simple_query(&sql).await
        {
            for orow in &overlay_rows {
                let row_pk = orow
                    .try_get_by_name::<String>("row_pk")
                    .ok()
                    .or_else(|| orow.get_string(0))
                    .unwrap_or_default();
                if row_pk == id {
                    let operation = orow
                        .try_get_by_name::<String>("operation")
                        .ok()
                        .or_else(|| orow.get_string(1))
                        .unwrap_or_default();
                    match operation.as_str() {
                        "delete" => {
                            conn.release().await;
                            return Err(ApiError::not_found(format!(
                                "{}/{} (deleted on branch)",
                                table_name, id
                            )));
                        }
                        "update" | "insert" => {
                            let row_data_str = orow
                                .try_get_by_name::<String>("row_data")
                                .ok()
                                .or_else(|| orow.get_string(2))
                                .unwrap_or_default();
                            if let Ok(val) = serde_json::from_str::<Value>(&row_data_str) {
                                data = val;
                            }
                        }
                        _ => {}
                    }
                    break;
                }
            }
        }
    }

    conn.release().await;

    // ── Tenant Boundary Invariant ────────────────────────────────────
    if let Some(ref tenant_id) = auth.tenant_id {
        let single = vec![data.clone()];
        let _proof = crate::tenant_guard::verify_tenant_boundary(
            &single,
            tenant_id,
            &state.config.tenant_column,
            &table_name,
            "rest_get_by_id",
        )
        .map_err(|v| {
            tracing::error!("{}", v);
            ApiError::internal("Data integrity error")
        })?;
    }

    Ok(Json(SingleResponse { data }))
}

/// POST /api/{table} — create from JSON body (single object or batch array)
///
/// Supports:
/// - Single: `{ "name": "Alice" }` → creates 1 row
/// - Batch:  `[{ "name": "Alice" }, { "name": "Bob" }]` → creates N rows
/// - Upsert: `?on_conflict=id` → INSERT ... ON CONFLICT (id) DO UPDATE
/// - Returning: `?returning=*` → RETURNING *
pub(crate) async fn create_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Query(mutation_params): Query<MutationParams>,
    request: axum::extract::Request,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let auth = authenticate_request(state.as_ref(), &headers).await?;
    let prefer = parse_prefer_header(&headers);

    // Validate required columns upfront (skip for upserts — conflict rows may exist)
    let required: Vec<String> = if prefer.wants_upsert() || prefer.wants_ignore_duplicates() {
        Vec::new() // Upsert: required columns may already exist in the row
    } else {
        table
            .required_columns()
            .iter()
            .map(|c| c.name.clone())
            // Skip tenant_column from required validation — it will be auto-injected
            // from the auth context if not provided by the client.
            .filter(|name| {
                if auth.tenant_id.is_some() && name == &state.config.tenant_column {
                    return false;
                }
                true
            })
            .collect()
    };

    // Parse JSON body
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|e| ApiError::parse_error(e.to_string()))?;
    let body: Value =
        serde_json::from_slice(&body).map_err(|e| ApiError::parse_error(e.to_string()))?;

    // Detect batch vs single
    let is_batch = body.is_array();
    let objects: Vec<&serde_json::Map<String, Value>> = if is_batch {
        let arr = body
            .as_array()
            .ok_or_else(|| ApiError::parse_error("Expected JSON array body"))?;
        arr.iter()
            .map(|v| {
                v.as_object()
                    .ok_or_else(|| ApiError::parse_error("Batch items must be JSON objects"))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![
            body.as_object()
                .ok_or_else(|| ApiError::parse_error("Expected JSON object or array"))?,
        ]
    };

    if objects.is_empty() {
        return Err(ApiError::parse_error("Empty request body"));
    }

    // Validate required columns for each object
    for (i, obj) in objects.iter().enumerate() {
        for col_name in &required {
            if !obj.contains_key(col_name) {
                return Err(ApiError::parse_error(format!(
                    "Missing required field '{}' in item {}",
                    col_name, i
                )));
            }
        }
    }
    // SECURITY: Fail closed on invalid JSON keys instead of silently skipping.
    // Skipping can produce unintended default-row inserts.
    for obj in &objects {
        for key in obj.keys() {
            if !crate::rest::filters::is_safe_identifier(key) {
                return Err(ApiError::parse_error(format!(
                    "Invalid field name '{}' in create payload",
                    key
                )));
            }
        }
    }

    // SECURITY: Check branch admin gate BEFORE acquiring connection
    let branch_ctx = extract_branch_from_headers(&headers);
    if branch_ctx.branch_name().is_some() && auth.role != "admin" && auth.role != "super_admin" {
        return Err(ApiError::forbidden(
            "Admin role required for branch overlay writes",
        ));
    }

    // Acquire connection
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // Branch CoW Write: redirect inserts to overlay table
    if let Some(branch_name) = branch_ctx.branch_name() {
        let mut all_results: Vec<Value> = Vec::with_capacity(objects.len());
        for obj in &objects {
            let row_data: Value = Value::Object((*obj).clone());
            let pk_col = table.primary_key.as_deref().unwrap_or("id");
            let row_pk = obj
                .get(pk_col)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| Uuid::new_v4().to_string());

            let overlay_result = redirect_to_overlay(
                &mut conn,
                branch_name,
                &table_name,
                &row_pk,
                "insert",
                &row_data,
            )
            .await;
            if let Err(e) = overlay_result {
                conn.release().await;
                return Err(e);
            }
            all_results.push(row_data);
        }

        conn.release().await;

        if is_batch {
            return Ok((
                StatusCode::CREATED,
                Json(
                    json!({ "data": all_results, "count": all_results.len(), "branch": branch_name }),
                ),
            ));
        } else {
            let data = all_results
                .into_iter()
                .next()
                .unwrap_or_else(|| json!({"created": true}));
            return Ok((
                StatusCode::CREATED,
                Json(json!({ "data": data, "branch": branch_name })),
            ));
        }
    }

    // Resolve PK column for Prefer: resolution=merge-duplicates
    let prefer_conflict_col: Option<String> =
        if prefer.wants_upsert() && mutation_params.on_conflict.is_none() {
            // Auto-resolve PK column from schema
            table.primary_key.clone()
        } else if prefer.wants_ignore_duplicates() && mutation_params.on_conflict.is_none() {
            table.primary_key.clone()
        } else {
            None
        };

    let mut all_results: Vec<Value> = Vec::with_capacity(objects.len());

    for obj in &objects {
        let mut cmd = qail_core::ast::Qail::add(&table_name);

        for (key, value) in *obj {
            let qail_val = json_to_qail_value(value);
            cmd = cmd.set_value(key, qail_val);
        }

        // Auto-inject tenant_id from auth context if not provided by client.
        // This ensures multi-tenant tables get the correct tenant_id without
        // requiring every frontend form to explicitly include it.
        if let Some(ref tid) = auth.tenant_id {
            let tc = &state.config.tenant_column;
            if !obj.contains_key(tc) {
                cmd = cmd.set_value(tc, QailValue::String(tid.clone()));
            }
        }

        // Upsert support: explicit on_conflict param takes precedence
        if let Some(ref conflict_col) = mutation_params.on_conflict {
            // SECURITY: Validate on_conflict column identifiers.
            let conflict_cols: Vec<&str> = conflict_col
                .split(',')
                .map(|s| s.trim())
                .filter(|s| crate::rest::filters::is_safe_identifier(s))
                .collect();
            let action = mutation_params
                .on_conflict_action
                .as_deref()
                .unwrap_or("update");

            if action == "nothing" {
                cmd = cmd.on_conflict_nothing(&conflict_cols);
            } else {
                // Default: update all provided columns on conflict
                // SECURITY: Filter update keys through identifier guard.
                let updates: Vec<(&str, Expr)> = obj
                    .keys()
                    .filter(|k| !conflict_cols.contains(&k.as_str()))
                    .filter(|k| crate::rest::filters::is_safe_identifier(k))
                    .map(|k| (k.as_str(), Expr::Named(format!("EXCLUDED.{}", k))))
                    .collect();
                cmd = cmd.on_conflict_update(&conflict_cols, &updates);
            }
        } else if prefer.wants_ignore_duplicates() {
            // Prefer: resolution=ignore-duplicates → DO NOTHING on PK
            if let Some(ref pk_col) = prefer_conflict_col {
                let cols: Vec<&str> = vec![pk_col.as_str()];
                cmd = cmd.on_conflict_nothing(&cols);
            }
        } else if let Some(ref pk_col) = prefer_conflict_col {
            // Prefer: resolution=merge-duplicates → DO UPDATE on all cols
            let conflict_cols: Vec<&str> = vec![pk_col.as_str()];
            // SECURITY: Filter update keys through identifier guard.
            let updates: Vec<(&str, Expr)> = obj
                .keys()
                .filter(|k| k.as_str() != pk_col.as_str())
                .filter(|k| crate::rest::filters::is_safe_identifier(k))
                .map(|k| (k.as_str(), Expr::Named(format!("EXCLUDED.{}", k))))
                .collect();
            cmd = cmd.on_conflict_update(&conflict_cols, &updates);
        }

        // Returning clause: Prefer return=representation forces RETURNING *
        if prefer.return_mode.as_deref() == Some("representation")
            && mutation_params.returning.is_none()
        {
            cmd = apply_returning(cmd, Some("*"));
        } else {
            cmd = apply_returning(cmd, mutation_params.returning.as_deref());
        }

        // Apply RLS
        if let Err(e) = state.policy_engine.apply_policies(&auth, &mut cmd) {
            conn.release().await;
            return Err(ApiError::forbidden(e.to_string()));
        }

        let rows = match conn.fetch_all_uncached(&cmd).await {
            Ok(rows) => rows,
            Err(e) => {
                conn.release().await;
                return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
            }
        };

        if !rows.is_empty() {
            for row in &rows {
                all_results.push(row_to_json(row));
            }
        }
    }

    // Release connection before JSON processing
    conn.release().await;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    // Prefer: return=minimal → 201 with no body
    if prefer.wants_minimal() {
        state.event_engine.fire(
            &table_name,
            OperationType::Create,
            Some(json!(all_results)),
            None,
        );
        return Ok((StatusCode::CREATED, Json(json!({}))));
    }

    if is_batch {
        let count = all_results.len();
        // Fire event triggers
        state.event_engine.fire(
            &table_name,
            OperationType::Create,
            Some(json!(all_results)),
            None,
        );
        Ok((
            StatusCode::CREATED,
            Json(json!({
                "data": all_results,
                "count": count,
            })),
        ))
    } else {
        let data = all_results
            .into_iter()
            .next()
            .unwrap_or_else(|| json!({"created": true}));
        // Fire event triggers
        state
            .event_engine
            .fire(&table_name, OperationType::Create, Some(data.clone()), None);
        Ok((StatusCode::CREATED, Json(json!({ "data": data }))))
    }
}

/// PATCH /api/{table}/:id — partial update
pub(crate) async fn update_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(mutation_params): Query<MutationParams>,
    request: axum::extract::Request,
) -> Result<Json<SingleResponse>, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    // F5: Accept any PK type
    if id.is_empty() {
        return Err(ApiError::parse_error(
            "ID parameter cannot be empty".to_string(),
        ));
    }

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let pk = table
        .primary_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?
        .clone();

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // Parse JSON body
    let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|e| ApiError::parse_error(e.to_string()))?;
    let body: Value =
        serde_json::from_slice(&body).map_err(|e| ApiError::parse_error(e.to_string()))?;
    let obj = body
        .as_object()
        .ok_or_else(|| ApiError::parse_error("Expected JSON object"))?;

    if obj.is_empty() {
        return Err(ApiError::parse_error("No fields to update"));
    }
    // SECURITY: Fail closed on invalid JSON keys instead of silently skipping.
    for key in obj.keys() {
        if !crate::rest::filters::is_safe_identifier(key) {
            return Err(ApiError::parse_error(format!(
                "Invalid field name '{}' in update payload",
                key
            )));
        }
    }

    // Build: set table { col1 = val1 } [pk = $id]
    let mut cmd = qail_core::ast::Qail::set(&table_name).filter(
        &pk,
        Operator::Eq,
        QailValue::String(id.clone()),
    );

    for (key, value) in obj {
        let qail_val = json_to_qail_value(value);
        cmd = cmd.set_value(key, qail_val);
    }

    // Returning clause
    cmd = apply_returning(cmd, mutation_params.returning.as_deref());

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // SECURITY: Check branch admin gate BEFORE acquiring connection
    let branch_ctx = extract_branch_from_headers(&headers);
    if branch_ctx.branch_name().is_some() && auth.role != "admin" && auth.role != "super_admin" {
        return Err(ApiError::forbidden(
            "Admin role required for branch overlay writes",
        ));
    }

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // Branch CoW Write: redirect updates to overlay
    if let Some(branch_name) = branch_ctx.branch_name() {
        let row_data: Value = Value::Object(obj.clone());
        let overlay_result = redirect_to_overlay(
            &mut conn,
            branch_name,
            &table_name,
            &id,
            "update",
            &row_data,
        )
        .await;
        if let Err(e) = overlay_result {
            conn.release().await;
            return Err(e);
        }
        conn.release().await;
        return Ok(Json(SingleResponse {
            data: json!({"updated": true, "branch": branch_name}),
        }));
    }

    let rows = match conn.fetch_all_uncached(&cmd).await {
        Ok(rows) => rows,
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    let data = rows
        .first()
        .map(row_to_json)
        .unwrap_or_else(|| json!({"updated": true}));

    // Release connection before event processing
    conn.release().await;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    // Fire event triggers
    state
        .event_engine
        .fire(&table_name, OperationType::Update, Some(data.clone()), None);

    Ok(Json(SingleResponse { data }))
}

/// DELETE /api/{table}/:id — delete by PK
pub(crate) async fn delete_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    request: axum::extract::Request,
) -> Result<axum::http::StatusCode, ApiError> {
    let table_name =
        extract_table_name(request.uri()).ok_or_else(|| ApiError::not_found("table"))?;
    check_table_not_blocked(&state, &table_name)?;

    // F5: Accept any PK type
    if id.is_empty() {
        return Err(ApiError::parse_error(
            "ID parameter cannot be empty".to_string(),
        ));
    }

    let table = state
        .schema
        .table(&table_name)
        .ok_or_else(|| ApiError::not_found(&table_name))?;

    let pk = table
        .primary_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("Table has no primary key"))?
        .clone();

    let auth = authenticate_request(state.as_ref(), &headers).await?;

    // Build: del table[pk = $id]
    let mut cmd = qail_core::ast::Qail::del(&table_name).filter(
        &pk,
        Operator::Eq,
        QailValue::String(id.clone()),
    );

    // Apply RLS
    state
        .policy_engine
        .apply_policies(&auth, &mut cmd)
        .map_err(|e| ApiError::forbidden(e.to_string()))?;

    // SECURITY: Check branch admin gate BEFORE acquiring connection
    let branch_ctx = extract_branch_from_headers(&headers);
    if branch_ctx.branch_name().is_some() && auth.role != "admin" && auth.role != "super_admin" {
        return Err(ApiError::forbidden(
            "Admin role required for branch overlay writes",
        ));
    }

    // Execute
    let mut conn = state
        .acquire_with_auth_rls_guarded(&auth, Some(&table_name))
        .await?;

    // Branch CoW Write: redirect deletes to overlay (tombstone)
    if let Some(branch_name) = branch_ctx.branch_name() {
        let overlay_result = redirect_to_overlay(
            &mut conn,
            branch_name,
            &table_name,
            &id,
            "delete",
            &Value::Null,
        )
        .await;
        if let Err(e) = overlay_result {
            conn.release().await;
            return Err(e);
        }
        conn.release().await;
        return Ok(axum::http::StatusCode::NO_CONTENT);
    }

    match conn.fetch_all_uncached(&cmd).await {
        Ok(_) => {}
        Err(e) => {
            conn.release().await;
            return Err(ApiError::from_pg_driver_error(&e, Some(&table_name)));
        }
    };

    // Release connection before event processing
    conn.release().await;

    // Invalidate cache
    state.cache.invalidate_table(&table_name);

    // Fire event triggers
    state.event_engine.fire(
        &table_name,
        OperationType::Delete,
        None,
        Some(json!({"id": id})),
    );

    // F6: Return 204 No Content to match OpenAPI spec
    Ok(axum::http::StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::{
        RpcFunctionName, build_rpc_sql, enforce_rpc_name_contract, matches_positional_signature,
        primary_sort_for_cursor, select_matching_rpc_signature,
        signature_matches_call as signature_matches,
    };
    use crate::server::RpcCallableSignature;
    use serde_json::json;
    use std::collections::HashSet;

    fn sig(
        total_args: usize,
        default_args: usize,
        variadic: bool,
        arg_names: &[Option<&str>],
        arg_types: &[&str],
        identity: &str,
    ) -> RpcCallableSignature {
        RpcCallableSignature {
            total_args,
            default_args,
            variadic,
            arg_names: arg_names
                .iter()
                .map(|n| n.map(|v| v.to_ascii_lowercase()))
                .collect(),
            arg_types: arg_types.iter().map(|t| t.to_ascii_lowercase()).collect(),
            identity_args: identity.to_string(),
            result_type: "jsonb".to_string(),
        }
    }

    #[test]
    fn primary_sort_defaults_to_id_asc() {
        let (col, desc) = primary_sort_for_cursor(None);
        assert_eq!(col, "id");
        assert!(!desc);
    }

    #[test]
    fn primary_sort_parses_prefix_desc() {
        let (col, desc) = primary_sort_for_cursor(Some("-created_at,total:asc"));
        assert_eq!(col, "created_at");
        assert!(desc);
    }

    #[test]
    fn primary_sort_parses_prefix_asc() {
        let (col, desc) = primary_sort_for_cursor(Some("+created_at"));
        assert_eq!(col, "created_at");
        assert!(!desc);
    }

    #[test]
    fn primary_sort_parses_explicit_desc() {
        let (col, desc) = primary_sort_for_cursor(Some("created_at:desc,id:asc"));
        assert_eq!(col, "created_at");
        assert!(desc);
    }

    #[test]
    fn primary_sort_parses_plain_col() {
        let (col, desc) = primary_sort_for_cursor(Some("created_at"));
        assert_eq!(col, "created_at");
        assert!(!desc);
    }

    #[test]
    fn build_rpc_sql_named_args() {
        let args = serde_json::json!({
            "tenant_id": "abc",
            "limit": 10
        });
        let function = RpcFunctionName::parse("api.search_orders").unwrap();
        let sql = build_rpc_sql(&function, Some(&args)).unwrap();
        assert!(sql.starts_with("SELECT * FROM \"api\".\"search_orders\"("));
        assert!(sql.contains("\"limit\" => 10"));
        assert!(sql.contains("\"tenant_id\" => 'abc'"));
    }

    #[test]
    fn build_rpc_sql_rejects_unsafe_function_name() {
        let err = RpcFunctionName::parse("search_orders;DROP TABLE users").unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rpc_name_contract_requires_schema_when_enabled() {
        let function = RpcFunctionName::parse("search_orders").unwrap();
        let err = enforce_rpc_name_contract(true, None, &function).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rpc_name_contract_enforces_allow_list() {
        let function = RpcFunctionName::parse("api.search_orders").unwrap();
        let mut allow = HashSet::new();
        allow.insert("api.other_fn".to_string());

        let err = enforce_rpc_name_contract(false, Some(&allow), &function).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn rpc_signature_named_requires_required_args() {
        let signature = sig(
            2,
            1,
            false,
            &[Some("tenant_id"), Some("limit")],
            &["uuid", "integer"],
            "tenant_id uuid, limit integer",
        );
        let args = json!({ "limit": 10 });
        assert!(!signature_matches(&signature, Some(&args)));
    }

    #[test]
    fn rpc_signature_positional_allows_defaults() {
        let signature = sig(
            2,
            1,
            false,
            &[Some("tenant_id"), Some("limit")],
            &["uuid", "integer"],
            "tenant_id uuid, limit integer",
        );
        let args = json!(["550e8400-e29b-41d4-a716-446655440000"]);
        assert!(signature_matches(&signature, Some(&args)));
    }

    #[test]
    fn rpc_signature_variadic_accepts_many_positional() {
        let signature = sig(
            2,
            0,
            true,
            &[Some("prefix"), Some("ids")],
            &["text", "integer[]"],
            "prefix text, variadic ids integer[]",
        );
        let args = vec![json!("pre"), json!(1), json!(2), json!(3)];
        assert!(matches_positional_signature(&signature, &args));
    }

    #[test]
    fn rpc_signature_select_rejects_ambiguous_overloads() {
        let signatures = vec![
            sig(1, 0, false, &[Some("id")], &["integer"], "id integer"),
            sig(1, 0, false, &[Some("id")], &["bigint"], "id bigint"),
        ];
        let args = json!([1]);
        let err =
            select_matching_rpc_signature("api.lookup", &signatures, Some(&args)).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rpc_signature_select_rejects_type_mismatch() {
        let signatures = vec![sig(
            1,
            0,
            false,
            &[Some("enabled")],
            &["boolean"],
            "enabled boolean",
        )];
        let args = json!(["not_bool"]);
        let err =
            select_matching_rpc_signature("api.toggle", &signatures, Some(&args)).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 4: Prefer header parsing
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn prefer_merge_duplicates() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("prefer", "resolution=merge-duplicates".parse().unwrap());
        let prefer = super::parse_prefer_header(&headers);
        assert!(prefer.wants_upsert());
        assert!(!prefer.wants_ignore_duplicates());
        assert!(!prefer.wants_minimal());
    }

    #[test]
    fn prefer_ignore_duplicates() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("prefer", "resolution=ignore-duplicates".parse().unwrap());
        let prefer = super::parse_prefer_header(&headers);
        assert!(!prefer.wants_upsert());
        assert!(prefer.wants_ignore_duplicates());
    }

    #[test]
    fn prefer_return_minimal() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("prefer", "return=minimal".parse().unwrap());
        let prefer = super::parse_prefer_header(&headers);
        assert!(prefer.wants_minimal());
    }

    #[test]
    fn prefer_return_headers_only() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("prefer", "return=headers-only".parse().unwrap());
        let prefer = super::parse_prefer_header(&headers);
        assert!(prefer.wants_minimal());
    }

    #[test]
    fn prefer_combined_directives() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "prefer",
            "resolution=merge-duplicates,return=representation"
                .parse()
                .unwrap(),
        );
        let prefer = super::parse_prefer_header(&headers);
        assert!(prefer.wants_upsert());
        assert!(!prefer.wants_minimal());
        assert_eq!(prefer.return_mode.as_deref(), Some("representation"));
    }

    #[test]
    fn prefer_empty_header_is_noop() {
        let headers = axum::http::HeaderMap::new();
        let prefer = super::parse_prefer_header(&headers);
        assert!(!prefer.wants_upsert());
        assert!(!prefer.wants_ignore_duplicates());
        assert!(!prefer.wants_minimal());
    }
}
