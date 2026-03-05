use axum::{extract::State, response::Json};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::middleware::ApiError;

/// GET /api/_schema — Schema introspection
///
/// Returns the full schema registry as JSON, including tables, columns,
/// primary keys, foreign keys, and column types.
///
/// **Security (H4):** Requires authentication in production.
/// In dev mode (`QAIL_DEV_MODE=true`), unauthenticated access is allowed.
pub(crate) async fn schema_introspection_handler(
    headers: axum::http::HeaderMap,
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<Value>, ApiError> {
    let auth = authenticate_request(state.as_ref(), &headers).await?;
    if !auth.is_authenticated() {
        return Err(ApiError::auth_error(
            "Authentication required for schema introspection",
        ));
    }
    let tables = state.schema.tables();
    let mut result = serde_json::Map::new();

    for (name, table) in tables {
        let columns: Vec<Value> = table
            .columns
            .iter()
            .map(|col| {
                json!({
                    "name": col.name,
                    "type": col.col_type,
                    "pg_type": col.pg_type,
                    "nullable": col.nullable,
                    "primary_key": col.primary_key,
                    "unique": col.unique,
                    "has_default": col.has_default,
                    "foreign_key": col.foreign_key.as_ref().map(|fk| json!({
                        "ref_table": fk.ref_table,
                        "ref_column": fk.ref_column,
                    })),
                })
            })
            .collect();

        result.insert(
            name.clone(),
            json!({
                "columns": columns,
                "primary_key": table.primary_key,
            }),
        );
    }

    Ok(Json(json!({
        "tables": result,
        "table_count": tables.len(),
    })))
}
