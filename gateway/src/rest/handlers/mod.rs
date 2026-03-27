//! REST API handlers — CRUD operations and RPC function invocation.
//!
//! Split into sub-modules:
//! - `crud` — list, aggregate, get, create, update, delete handlers
//! - `rpc`  — PostgreSQL function invocation with overload resolution

mod crud;
mod rpc;
#[cfg(test)]
mod tests;

// ── Public API (consumed by rest/mod.rs) ────────────────────────────
pub(crate) use crud::{
    aggregate_handler, create_handler, delete_handler, get_by_id_handler, list_handler,
    update_handler,
};
pub(crate) use rpc::rpc_handler;
pub(crate) use rpc::{
    minimum_required_rpc_args, normalize_pg_type_name, parse_rpc_input_arg_names,
};

// ── Shared helpers (used by crud + rpc sub-modules) ─────────────────

use axum::http::HeaderMap;

use crate::GatewayState;
use crate::middleware::ApiError;

/// Parse the primary sort column and direction for cursor pagination.
///
/// Supports:
/// - prefix style: `-col`, `+col`
/// - explicit style: `col:desc`, `col:asc`
/// - default style: `col`
///
/// Falls back to `id ASC` when sort is missing or malformed.
pub(super) fn primary_sort_for_cursor(sort: Option<&str>) -> (String, bool) {
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
pub(super) struct PreferDirectives {
    pub resolution: Option<String>,
    pub return_mode: Option<String>,
}

impl PreferDirectives {
    pub fn wants_upsert(&self) -> bool {
        self.resolution.as_deref() == Some("merge-duplicates")
    }

    pub fn wants_ignore_duplicates(&self) -> bool {
        self.resolution.as_deref() == Some("ignore-duplicates")
    }

    pub fn wants_minimal(&self) -> bool {
        matches!(
            self.return_mode.as_deref(),
            Some("minimal") | Some("headers-only")
        )
    }
}

/// Parse the `Prefer` header into structured directives.
pub(super) fn parse_prefer_header(headers: &HeaderMap) -> PreferDirectives {
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

pub(super) fn is_safe_ident_segment(segment: &str) -> bool {
    let mut chars = segment.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub(super) fn quote_ident(segment: &str) -> String {
    format!("\"{}\"", segment.replace('"', "\"\""))
}

/// SECURITY: Runtime guard — reject requests targeting inaccessible tables.
/// Allowlist takes precedence: if set, only listed tables are allowed.
/// Otherwise falls back to blocklist check.
/// Belt-and-suspenders: routes for blocked tables are not registered,
/// but this catches edge cases (e.g., expand references, nested routes).
pub(super) fn check_table_not_blocked(
    state: &GatewayState,
    table_name: &str,
) -> Result<(), ApiError> {
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
