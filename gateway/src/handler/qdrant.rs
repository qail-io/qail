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
    use qail_core::ast::{Action, CageKind, Distance as CoreDistance};

    let pool = state.qdrant_pool.as_ref().ok_or_else(|| {
        tracing::error!("Qdrant operation requested but no [qdrant] config");
        ApiError::with_code("QDRANT_NOT_CONFIGURED", "Qdrant not configured")
    })?;

    let mut conn = pool.get().await.map_err(|e| {
        tracing::error!("Qdrant pool error: {}", e);
        ApiError::with_code("QDRANT_CONNECTION_ERROR", "Qdrant connection failed")
    })?;

    let collection = &cmd.table;
    let (must_conditions, should_groups) = split_filter_conditions(cmd);

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
            let results = if must_conditions.is_empty() && should_groups.is_empty() {
                if let Some(name) = cmd.vector_name.as_deref() {
                    conn.search_named(collection, name, vector, limit_val, cmd.score_threshold)
                        .await
                        .map_err(|e| qdrant_err(e, "search"))?
                } else {
                    conn.search(collection, vector, limit_val, cmd.score_threshold)
                        .await
                        .map_err(|e| qdrant_err(e, "search"))?
                }
            } else {
                conn.search_filtered_grouped_cages(
                    qail_qdrant::encoder::SearchRequest {
                        collection,
                        vector,
                        limit: limit_val,
                        score_threshold: cmd.score_threshold,
                        vector_name: cmd.vector_name.as_deref(),
                    },
                    &must_conditions,
                    &should_groups,
                )
                .await
                .map_err(|e| qdrant_err(e, "search"))?
            };

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
            let point = extract_upsert_point(cmd)?;
            conn.upsert(collection, &[point], false)
                .await
                .map_err(|e| qdrant_err(e, "upsert"))?;

            Ok(Json(QueryResponse {
                rows: vec![serde_json::json!({"status": "upsert_ok", "collection": collection})],
                count: 1,
            }))
        }

        Action::CreateCollection => {
            let vector_size = cmd.vector_size.ok_or_else(|| {
                ApiError::bad_request(
                    "MISSING_VECTOR_SIZE",
                    "CreateCollection requires vector_size",
                )
            })?;
            let distance = match cmd.distance.unwrap_or(CoreDistance::Cosine) {
                CoreDistance::Cosine => qail_qdrant::Distance::Cosine,
                CoreDistance::Euclid => qail_qdrant::Distance::Euclidean,
                CoreDistance::Dot => qail_qdrant::Distance::Dot,
            };
            let on_disk = cmd.on_disk.unwrap_or(false);

            conn.create_collection(collection, vector_size, distance, on_disk)
                .await
                .map_err(|e| qdrant_err(e, "create_collection"))?;

            Ok(Json(QueryResponse {
                rows: vec![
                    serde_json::json!({"status": "create_collection_ok", "collection": collection}),
                ],
                count: 1,
            }))
        }

        Action::DeleteCollection => {
            conn.delete_collection(collection)
                .await
                .map_err(|e| qdrant_err(e, "delete_collection"))?;

            Ok(Json(QueryResponse {
                rows: vec![
                    serde_json::json!({"status": "delete_collection_ok", "collection": collection}),
                ],
                count: 1,
            }))
        }

        _ => Err(ApiError::bad_request(
            "UNSUPPORTED_ACTION",
            format!("Unsupported Qdrant action: {:?}", cmd.action),
        )),
    }
}

fn split_filter_conditions(
    cmd: &qail_core::ast::Qail,
) -> (
    Vec<qail_core::ast::Condition>,
    Vec<Vec<qail_core::ast::Condition>>,
) {
    use qail_core::ast::{CageKind, LogicalOp};

    let mut must_conditions = Vec::new();
    let mut should_groups = Vec::new();
    for cage in cmd
        .cages
        .iter()
        .filter(|c| matches!(c.kind, CageKind::Filter))
    {
        match cage.logical_op {
            LogicalOp::And => must_conditions.extend(cage.conditions.iter().cloned()),
            LogicalOp::Or => {
                if !cage.conditions.is_empty() {
                    should_groups.push(cage.conditions.to_vec());
                }
            }
        }
    }
    (must_conditions, should_groups)
}

fn extract_upsert_point(cmd: &qail_core::ast::Qail) -> Result<qail_qdrant::Point, ApiError> {
    use qail_core::ast::{CageKind, Expr};

    let mut id = None;
    let mut vector = cmd.vector.clone();
    let mut payload = qail_qdrant::Payload::new();

    for cage in cmd
        .cages
        .iter()
        .filter(|c| matches!(c.kind, CageKind::Payload | CageKind::Filter))
    {
        for cond in &cage.conditions {
            let field = match &cond.left {
                Expr::Named(name) => name.as_str(),
                Expr::Aliased { name, .. } => name.as_str(),
                _ => continue,
            };

            match field {
                "id" => {
                    id = Some(point_id_from_value(&cond.value).ok_or_else(|| {
                        ApiError::bad_request(
                            "INVALID_POINT_ID",
                            "Upsert point id must be integer or string UUID",
                        )
                    })?);
                }
                "vector" => {
                    vector = Some(vector_from_value(&cond.value).ok_or_else(|| {
                        ApiError::bad_request(
                            "INVALID_VECTOR",
                            "Upsert vector must be an array of numeric values",
                        )
                    })?);
                }
                _ => {
                    if let Some(v) = payload_value_from_ast(&cond.value) {
                        payload.insert(field.to_string(), v);
                    }
                }
            }
        }
    }

    let id = id.ok_or_else(|| {
        ApiError::bad_request(
            "MISSING_POINT_ID",
            "Upsert requires payload/filter field 'id'",
        )
    })?;
    let vector = vector.ok_or_else(|| {
        ApiError::bad_request(
            "MISSING_VECTOR",
            "Upsert requires vector payload field 'vector' or cmd.vector",
        )
    })?;
    if vector.is_empty() {
        return Err(ApiError::bad_request(
            "INVALID_VECTOR",
            "Upsert vector must not be empty",
        ));
    }

    Ok(qail_qdrant::Point {
        id,
        vector,
        payload,
    })
}

fn point_id_from_value(value: &qail_core::ast::Value) -> Option<qail_qdrant::PointId> {
    use qail_core::ast::Value;
    match value {
        Value::Int(n) if *n >= 0 => Some(qail_qdrant::PointId::Num(*n as u64)),
        Value::String(s) => Some(qail_qdrant::PointId::Uuid(s.clone())),
        Value::Uuid(u) => Some(qail_qdrant::PointId::Uuid(u.to_string())),
        _ => None,
    }
}

fn vector_from_value(value: &qail_core::ast::Value) -> Option<Vec<f32>> {
    use qail_core::ast::Value;
    match value {
        Value::Vector(v) => Some(v.clone()),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Int(n) => out.push(*n as f32),
                    Value::Float(f) => out.push(*f as f32),
                    _ => return None,
                }
            }
            Some(out)
        }
        _ => None,
    }
}

fn payload_value_from_ast(value: &qail_core::ast::Value) -> Option<qail_qdrant::PayloadValue> {
    use qail_core::ast::Value;

    match value {
        Value::Null => Some(qail_qdrant::PayloadValue::Null),
        Value::Bool(b) => Some(qail_qdrant::PayloadValue::Bool(*b)),
        Value::Int(n) => Some(qail_qdrant::PayloadValue::Integer(*n)),
        Value::Float(f) => Some(qail_qdrant::PayloadValue::Float(*f)),
        Value::String(s) => Some(qail_qdrant::PayloadValue::String(s.clone())),
        Value::Uuid(u) => Some(qail_qdrant::PayloadValue::String(u.to_string())),
        Value::Json(s) => Some(qail_qdrant::PayloadValue::String(s.clone())),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(payload_value_from_ast(item)?);
            }
            Some(qail_qdrant::PayloadValue::List(out))
        }
        _ => None,
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

#[cfg(test)]
mod tests {
    use super::split_filter_conditions;
    use qail_core::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    fn cond(name: &str, value: &str) -> Condition {
        Condition {
            left: Expr::Named(name.to_string()),
            op: Operator::Eq,
            value: Value::String(value.to_string()),
            is_array_unnest: false,
        }
    }

    #[test]
    fn split_filter_conditions_keeps_or_groups_separate() {
        let cmd = Qail {
            cages: vec![
                Cage {
                    kind: CageKind::Filter,
                    conditions: vec![cond("tenant_id", "t1")],
                    logical_op: LogicalOp::And,
                },
                Cage {
                    kind: CageKind::Filter,
                    conditions: vec![cond("city", "London"), cond("city", "Paris")],
                    logical_op: LogicalOp::Or,
                },
                Cage {
                    kind: CageKind::Filter,
                    conditions: vec![cond("country", "UK"), cond("country", "FR")],
                    logical_op: LogicalOp::Or,
                },
            ],
            ..Default::default()
        };

        let (must, should_groups) = split_filter_conditions(&cmd);
        assert_eq!(must.len(), 1);
        assert_eq!(should_groups.len(), 2);
        assert_eq!(should_groups[0].len(), 2);
        assert_eq!(should_groups[1].len(), 2);
    }
}
