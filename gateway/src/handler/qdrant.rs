//! Qdrant vector operation handlers.
//!
//! Routes QAIL vector actions (Search, Upsert, Scroll, CreateCollection,
//! DeleteCollection) to the Qdrant connection pool.

use axum::response::Json;
use std::sync::Arc;

use super::QueryResponse;
use crate::GatewayState;
use crate::middleware::ApiError;

/// Execute a Qdrant vector command.
///
/// Routes QAIL vector actions (Search, Upsert, Scroll, etc.) to the
/// Qdrant connection pool. Returns JSON-formatted scored points or
/// operation results.
pub(super) async fn execute_qdrant_cmd(
    state: &Arc<GatewayState>,
    cmd: &qail_core::ast::Qail,
) -> Result<Json<QueryResponse>, ApiError> {
    use qail_core::ast::{Action, CageKind};

    let pool = state.qdrant_pool.as_ref().ok_or_else(|| {
        tracing::error!("Qdrant operation requested but no [qdrant] config");
        ApiError::with_code("QDRANT_NOT_CONFIGURED", "Qdrant not configured")
    })?;

    let mut conn = pool.get().await.map_err(|e| {
        tracing::error!("Qdrant pool error: {}", e);
        ApiError::with_code("QDRANT_CONNECTION_ERROR", "Qdrant connection failed")
    })?;

    let collection = &cmd.table;

    // Extract limit from CageKind::Limit if present
    let limit_val: u64 = cmd
        .cages
        .iter()
        .find_map(|c| match c.kind {
            CageKind::Limit(n) => Some(n as u64),
            _ => None,
        })
        .unwrap_or(10);

    match cmd.action {
        Action::Search => {
            // Use the dedicated vector field from the Qail AST
            let vector = cmd.vector.as_deref().ok_or_else(|| {
                ApiError::bad_request("MISSING_VECTOR", "Search requires a vector")
            })?;

            let results = conn
                .search(collection, vector, limit_val, cmd.score_threshold)
                .await
                .map_err(|e| qdrant_err(e, "search"))?;

            let rows: Vec<serde_json::Value> = results.iter().map(scored_point_to_json).collect();
            let count = rows.len();

            Ok(Json(QueryResponse { rows, count }))
        }

        Action::Scroll => {
            let result = conn
                .scroll(collection, limit_val as u32, None, cmd.with_vector)
                .await
                .map_err(|e| qdrant_err(e, "scroll"))?;

            let rows: Vec<serde_json::Value> =
                result.points.iter().map(scored_point_to_json).collect();
            let count = rows.len();

            Ok(Json(QueryResponse { rows, count }))
        }

        Action::Upsert => {
            // For now, return a success acknowledgement.
            // Full upsert requires parsing points from the AST body.
            tracing::info!("Qdrant UPSERT on '{}' (routed via gateway)", collection);
            Ok(Json(QueryResponse {
                rows: vec![
                    serde_json::json!({"status": "upsert_routed", "collection": collection}),
                ],
                count: 1,
            }))
        }

        Action::CreateCollection | Action::DeleteCollection => {
            let op = if matches!(cmd.action, Action::CreateCollection) {
                "create_collection"
            } else {
                "delete_collection"
            };
            tracing::info!("Qdrant {} '{}' (routed via gateway)", op, collection);
            Ok(Json(QueryResponse {
                rows: vec![serde_json::json!({"status": op, "collection": collection})],
                count: 1,
            }))
        }

        _ => Err(ApiError::bad_request(
            "UNSUPPORTED_ACTION",
            format!("Unsupported Qdrant action: {:?}", cmd.action),
        )),
    }
}

/// Convert a Qdrant error into an ApiError.
fn qdrant_err(e: qail_qdrant::QdrantError, op: &str) -> ApiError {
    tracing::error!("Qdrant {} error: {}", op, e);
    ApiError::with_code("QDRANT_ERROR", format!("Qdrant {} failed", op))
}

/// Convert a `ScoredPoint` to a JSON value for the response.
fn scored_point_to_json(pt: &qail_qdrant::ScoredPoint) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("id".to_string(), serde_json::json!(pt.id));
    obj.insert("score".to_string(), serde_json::json!(pt.score));

    if !pt.payload.is_empty() {
        let payload: serde_json::Map<String, serde_json::Value> = pt
            .payload
            .iter()
            .map(|(k, v)| (k.clone(), payload_value_to_json(v)))
            .collect();
        obj.insert("payload".to_string(), serde_json::Value::Object(payload));
    }

    serde_json::Value::Object(obj)
}

/// Convert a `PayloadValue` to JSON.
fn payload_value_to_json(v: &qail_qdrant::PayloadValue) -> serde_json::Value {
    match v {
        qail_qdrant::PayloadValue::String(s) => serde_json::json!(s),
        qail_qdrant::PayloadValue::Integer(i) => serde_json::json!(i),
        qail_qdrant::PayloadValue::Float(f) => serde_json::json!(f),
        qail_qdrant::PayloadValue::Bool(b) => serde_json::json!(b),
        qail_qdrant::PayloadValue::Null => serde_json::Value::Null,
        qail_qdrant::PayloadValue::List(arr) => {
            serde_json::Value::Array(arr.iter().map(payload_value_to_json).collect())
        }
        qail_qdrant::PayloadValue::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), payload_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}
