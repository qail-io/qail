//! Qdrant REST/JSON protocol encoding.
//!
//! This module handles encoding QAIL AST to Qdrant's REST API JSON format.
//! Using JSON instead of gRPC for simplicity and portability.

use crate::error::QdrantResult;
use crate::point::{PayloadValue, Point, PointId, ScoredPoint};
use serde_json::{Value as JsonValue, json};

fn serialize_json_request(request: &JsonValue) -> Vec<u8> {
    match serde_json::to_vec(request) {
        Ok(bytes) => bytes,
        Err(err) => {
            encode_error_request(&format!("failed to serialize Qdrant JSON request: {err}"))
        }
    }
}

fn encode_error_request(message: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({ "error": message }))
        .unwrap_or_else(|_| b"{\"error\":\"failed to encode Qdrant JSON error\"}".to_vec())
}

fn vector_to_json(values: &[f32], label: &str) -> Result<JsonValue, String> {
    if values.is_empty() {
        return Err(format!("Qdrant {label} vector must not be empty"));
    }
    let mut json_values = Vec::with_capacity(values.len());
    for (idx, value) in values.iter().enumerate() {
        if !value.is_finite() {
            return Err(format!(
                "Qdrant {label} contains non-finite vector value at index {idx}: {value}"
            ));
        }
        json_values.push(json!(value));
    }
    Ok(JsonValue::Array(json_values))
}

fn ensure_positive_limit(limit: u64, label: &str) -> Result<(), String> {
    if limit == 0 {
        return Err(format!("Qdrant {label} limit must be greater than zero"));
    }
    Ok(())
}

fn ensure_named_vector_name(name: &str, label: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err(format!("Qdrant {label} vector name must not be empty"));
    }
    Ok(())
}

fn ensure_payload_key(key: &str) -> Result<(), String> {
    if key.trim().is_empty() {
        return Err("Qdrant payload field name must not be empty".to_string());
    }
    Ok(())
}

fn ensure_point_id(id: &PointId, label: &str) -> Result<(), String> {
    match id {
        PointId::Num(_) => Ok(()),
        PointId::Uuid(s) if s.trim().is_empty() => {
            Err(format!("Qdrant {label} point id must not be empty"))
        }
        PointId::Uuid(_) => Ok(()),
    }
}

fn canonical_distance(distance: &str) -> Result<&'static str, String> {
    match distance.trim().to_ascii_lowercase().as_str() {
        "cosine" => Ok("Cosine"),
        "euclidean" => Ok("Euclidean"),
        "dot" => Ok("Dot"),
        _ => Err(format!("Unsupported Qdrant distance metric: {distance}")),
    }
}

fn optional_threshold_to_json(value: Option<f32>) -> Result<Option<JsonValue>, String> {
    match value {
        Some(value) if !value.is_finite() => Err(format!(
            "Qdrant score threshold must be finite, got {value}"
        )),
        Some(value) => Ok(Some(json!(value))),
        None => Ok(None),
    }
}

/// Encode a vector search request to JSON format.
///
/// Generates JSON for POST /collections/{collection}/points/search
///
/// Example output:
/// ```json
/// {
///   "vector": [0.1, 0.2, 0.3],
///   "limit": 10,
///   "offset": 0,
///   "with_payload": true,
///   "filter": { ... }
/// }
/// ```
pub fn encode_search_request(
    vector: &[f32],
    limit: u64,
    offset: Option<u64>,
    score_threshold: Option<f32>,
    with_vector: bool,
) -> Vec<u8> {
    if let Err(err) = ensure_positive_limit(limit, "search") {
        return encode_error_request(&err);
    }
    let vector = match vector_to_json(vector, "search request") {
        Ok(vector) => vector,
        Err(err) => return encode_error_request(&err),
    };
    let mut request = json!({
        "vector": vector,
        "limit": limit,
        "with_payload": true,
        "with_vector": with_vector,
    });

    if let Some(off) = offset {
        request["offset"] = json!(off);
    }

    let score_threshold = match optional_threshold_to_json(score_threshold) {
        Ok(score_threshold) => score_threshold,
        Err(err) => return encode_error_request(&err),
    };
    if let Some(threshold) = score_threshold {
        request["score_threshold"] = threshold;
    }

    serialize_json_request(&request)
}

/// Encode search request with filter conditions.
pub fn encode_search_request_with_filter(
    vector: &[f32],
    limit: u64,
    offset: Option<u64>,
    score_threshold: Option<f32>,
    with_vector: bool,
    filter: JsonValue,
) -> Vec<u8> {
    if let Err(err) = ensure_positive_limit(limit, "search") {
        return encode_error_request(&err);
    }
    let vector = match vector_to_json(vector, "search request") {
        Ok(vector) => vector,
        Err(err) => return encode_error_request(&err),
    };
    let mut request = json!({
        "vector": vector,
        "limit": limit,
        "with_payload": true,
        "with_vector": with_vector,
        "filter": filter,
    });

    if let Some(off) = offset {
        request["offset"] = json!(off);
    }

    let score_threshold = match optional_threshold_to_json(score_threshold) {
        Ok(score_threshold) => score_threshold,
        Err(err) => return encode_error_request(&err),
    };
    if let Some(threshold) = score_threshold {
        request["score_threshold"] = threshold;
    }

    serialize_json_request(&request)
}

/// Encode an upsert (insert/update) request to JSON.
///
/// Generates JSON for PUT /collections/{collection}/points
///
/// Example output:
/// ```json
/// {
///   "points": [
///     { "id": "abc123", "vector": [0.1, 0.2], "payload": {"name": "test"} }
///   ]
/// }
/// ```
pub fn encode_upsert_request(points: &[Point]) -> Vec<u8> {
    if points.is_empty() {
        return encode_error_request("Qdrant upsert point list must not be empty");
    }
    let points_json: Result<Vec<JsonValue>, String> = points
        .iter()
        .enumerate()
        .map(|(idx, p)| {
            ensure_point_id(&p.id, &format!("upsert point {idx}"))?;
            let id = match &p.id {
                PointId::Uuid(s) => json!(s),
                PointId::Num(n) => json!(n),
            };

            let payload = payload_to_json_map(&p.payload)?;

            let vector = vector_to_json(&p.vector, &format!("upsert point {idx}"))?;

            Ok(json!({
                "id": id,
                "vector": vector,
                "payload": JsonValue::Object(payload),
            }))
        })
        .collect();

    let points_json = match points_json {
        Ok(points_json) => points_json,
        Err(err) => return encode_error_request(&err),
    };
    let request = json!({ "points": points_json });
    serialize_json_request(&request)
}

/// Encode an upsert request for multi-vector points (named vectors).
///
/// For collections with multiple vector fields (e.g., "title", "content").
pub fn encode_upsert_multi_vector_request(points: &[crate::point::MultiVectorPoint]) -> Vec<u8> {
    use crate::point::MultiVectorPoint;

    if points.is_empty() {
        return encode_error_request("Qdrant upsert point list must not be empty");
    }
    let points_json: Result<Vec<JsonValue>, String> = points
        .iter()
        .enumerate()
        .map(|(idx, p): (usize, &MultiVectorPoint)| {
            if p.vectors.is_empty() {
                return Err(format!(
                    "Qdrant multi-vector point {idx} must contain at least one named vector"
                ));
            }
            ensure_point_id(&p.id, &format!("multi-vector point {idx}"))?;
            let id = match &p.id {
                PointId::Uuid(s) => json!(s),
                PointId::Num(n) => json!(n),
            };

            let payload = payload_to_json_map(&p.payload)?;

            // Named vectors as object
            let vectors: Result<serde_json::Map<String, JsonValue>, String> = p
                .vectors
                .iter()
                .map(|(k, v)| {
                    ensure_named_vector_name(k, &format!("multi-vector point {idx}"))?;
                    Ok((
                        k.clone(),
                        vector_to_json(v, &format!("multi-vector point {idx}.{k}"))?,
                    ))
                })
                .collect();

            Ok(json!({
                "id": id,
                "vector": JsonValue::Object(vectors?),
                "payload": JsonValue::Object(payload),
            }))
        })
        .collect();

    let points_json = match points_json {
        Ok(points_json) => points_json,
        Err(err) => return encode_error_request(&err),
    };
    let request = json!({ "points": points_json });
    serialize_json_request(&request)
}

/// Encode a delete request to JSON.
///
/// Generates JSON for POST /collections/{collection}/points/delete
///
/// Example output:
/// ```json
/// { "points": ["id1", "id2"] }
/// ```
pub fn encode_delete_request(ids: &[PointId]) -> Vec<u8> {
    if ids.is_empty() {
        return encode_error_request("Qdrant delete point id list must not be empty");
    }
    let ids_json: Result<Vec<JsonValue>, String> = ids
        .iter()
        .map(|id| match id {
            PointId::Uuid(s) => {
                ensure_point_id(id, "delete")?;
                Ok(json!(s))
            }
            PointId::Num(n) => Ok(json!(n)),
        })
        .collect();
    let ids_json = match ids_json {
        Ok(ids_json) => ids_json,
        Err(err) => return encode_error_request(&err),
    };

    let request = json!({ "points": ids_json });
    serialize_json_request(&request)
}

/// Encode create collection request.
///
/// Generates JSON for PUT /collections/{collection}
pub fn encode_create_collection_request(
    vector_size: u64,
    distance: &str, // "Cosine", "Euclidean", "Dot"
) -> Vec<u8> {
    if vector_size == 0 {
        return encode_error_request("Qdrant collection vector_size must be greater than zero");
    }
    let distance = match canonical_distance(distance) {
        Ok(distance) => distance,
        Err(err) => return encode_error_request(&err),
    };
    let request = json!({
        "vectors": {
            "size": vector_size,
            "distance": distance,
        }
    });
    serialize_json_request(&request)
}

/// Convert QAIL conditions to Qdrant filter format.
///
/// Qdrant uses `must`, `should`, `must_not` arrays for filtering.
/// Each condition becomes a clause in `must` (AND logic).
///
/// # Example
/// ```ignore
/// use qail_core::ast::{Condition, Operator, Expr, Value};
///
/// let conditions = vec![
///     Condition { left: Expr::Named("category".into()), op: Operator::Eq, value: Value::String("electronics".into()), is_array_unnest: false },
///     Condition { left: Expr::Named("price".into()), op: Operator::Lt, value: Value::Int(1000), is_array_unnest: false },
/// ];
///
/// let filter = encode_conditions_to_filter(&conditions, false);
/// // Returns: {"must": [{"key": "category", "match": {"value": "electronics"}}, {"key": "price", "range": {"lt": 1000}}]}
/// ```
pub fn encode_conditions_to_filter(
    conditions: &[qail_core::ast::Condition],
    is_or: bool,
) -> JsonValue {
    use qail_core::ast::{Expr, Operator, Value};

    let mut clauses = Vec::with_capacity(conditions.len());
    for cond in conditions {
        // Extract field name from left expression
        let key = match &cond.left {
            Expr::Named(name) => name.clone(),
            Expr::Aliased { name, .. } => name.clone(),
            _ => return impossible_filter(),
        };
        let key = normalize_filter_key(&key);
        if key.is_empty() {
            return impossible_filter();
        }

        if key == "id" {
            let Some(id) = point_id_value_to_json(&cond.value) else {
                return impossible_filter();
            };
            if cond.op != Operator::Eq {
                return impossible_filter();
            }
            clauses.push(json!({ "has_id": [id] }));
            continue;
        }

        // Convert operator and value to Qdrant filter clause
        let clause = match (&cond.op, &cond.value) {
            // Match (equality)
            (Operator::Eq, Value::String(s)) => json!({
                "key": key,
                "match": { "value": s }
            }),
            (Operator::Eq, Value::Int(n)) => json!({
                "key": key,
                "match": { "value": n }
            }),
            (Operator::Eq, Value::Float(f)) if f.is_finite() => json!({
                "key": key,
                "match": { "value": f }
            }),
            (Operator::Eq, Value::Bool(b)) => json!({
                "key": key,
                "match": { "value": b }
            }),

            // Range operators
            (Operator::Gt, Value::Int(n)) => json!({
                "key": key,
                "range": { "gt": n }
            }),
            (Operator::Gt, Value::Float(f)) if f.is_finite() => json!({
                "key": key,
                "range": { "gt": f }
            }),
            (Operator::Gte, Value::Int(n)) => json!({
                "key": key,
                "range": { "gte": n }
            }),
            (Operator::Gte, Value::Float(f)) if f.is_finite() => json!({
                "key": key,
                "range": { "gte": f }
            }),
            (Operator::Lt, Value::Int(n)) => json!({
                "key": key,
                "range": { "lt": n }
            }),
            (Operator::Lt, Value::Float(f)) if f.is_finite() => json!({
                "key": key,
                "range": { "lt": f }
            }),
            (Operator::Lte, Value::Int(n)) => json!({
                "key": key,
                "range": { "lte": n }
            }),
            (Operator::Lte, Value::Float(f)) if f.is_finite() => json!({
                "key": key,
                "range": { "lte": f }
            }),

            // In / NotIn (array membership)
            (Operator::In, Value::Array(arr)) => {
                let Some(values) = values_to_json(arr) else {
                    return impossible_filter();
                };
                json!({
                    "key": key,
                    "match": { "any": values }
                })
            }

            // IsNull / IsNotNull
            (Operator::IsNull, Value::Null | Value::NullUuid) => json!({
                "is_null": { "key": key }
            }),

            // Text/keyword match with contains
            (Operator::Contains | Operator::Like, Value::String(s)) if !s.trim().is_empty() => {
                json!({
                    "key": key,
                    "match": { "text": s }
                })
            }

            _ => return impossible_filter(),
        };

        clauses.push(clause);
    }

    // Use "should" for OR, "must" for AND
    if is_or {
        json!({ "should": clauses })
    } else {
        json!({ "must": clauses })
    }
}

fn normalize_filter_key(raw: &str) -> String {
    raw.trim().trim_matches('"').trim().to_string()
}

fn point_id_value_to_json(value: &qail_core::ast::Value) -> Option<JsonValue> {
    use qail_core::ast::Value;

    match value {
        Value::Int(id) if *id >= 0 => Some(json!(*id as u64)),
        Value::String(id) if !id.trim().is_empty() => Some(json!(id)),
        Value::Uuid(id) => Some(json!(id.to_string())),
        _ => None,
    }
}

fn impossible_filter() -> JsonValue {
    json!({
        "must": [{
            "key": "__qail_unrepresentable_filter__",
            "range": { "gt": 1, "lt": 0 }
        }]
    })
}

fn values_to_json(values: &[qail_core::ast::Value]) -> Option<Vec<JsonValue>> {
    if values.is_empty() {
        return None;
    }
    values.iter().map(value_to_json).collect()
}

/// Convert Value to JsonValue for filter encoding.
fn value_to_json(value: &qail_core::ast::Value) -> Option<JsonValue> {
    use qail_core::ast::Value;
    match value {
        Value::String(s) => Some(json!(s)),
        Value::Int(n) => Some(json!(n)),
        Value::Float(f) if f.is_finite() => Some(json!(f)),
        Value::Bool(b) => Some(json!(b)),
        _ => None,
    }
}

/// Decode search response from JSON.
pub fn decode_search_response(data: &[u8]) -> QdrantResult<Vec<ScoredPoint>> {
    let response: JsonValue = serde_json::from_slice(data)
        .map_err(|e| crate::error::QdrantError::Decode(e.to_string()))?;

    let results = response["result"]
        .as_array()
        .ok_or_else(|| crate::error::QdrantError::Decode("Missing 'result' array".to_string()))?;

    results
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let id = item.get("id").and_then(parse_point_id).ok_or_else(|| {
                crate::error::QdrantError::Decode(format!("Missing point id at result index {idx}"))
            })?;
            let score = item
                .get("score")
                .and_then(JsonValue::as_f64)
                .filter(|score| score.is_finite())
                .ok_or_else(|| {
                    crate::error::QdrantError::Decode(format!(
                        "Invalid score at result index {idx}"
                    ))
                })?;
            let score = score as f32;
            if !score.is_finite() {
                return Err(crate::error::QdrantError::Decode(format!(
                    "Invalid score at result index {idx}"
                )));
            }
            let payload = match item.get("payload") {
                Some(payload) => parse_payload_checked(payload, idx)?,
                None => crate::point::Payload::new(),
            };
            let vector = decode_result_vector(item.get("vector"), idx)?;

            Ok(ScoredPoint {
                id,
                score,
                payload,
                vector,
            })
        })
        .collect()
}

fn decode_result_vector(
    value: Option<&JsonValue>,
    result_idx: usize,
) -> QdrantResult<Option<Vec<f32>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }

    let arr = value.as_array().ok_or_else(|| {
        crate::error::QdrantError::Decode(format!("Invalid vector at result index {result_idx}"))
    })?;
    if arr.is_empty() {
        return Err(crate::error::QdrantError::Decode(format!(
            "Empty vector at result index {result_idx}"
        )));
    }

    let mut vector = Vec::with_capacity(arr.len());
    for (idx, item) in arr.iter().enumerate() {
        let value = item
            .as_f64()
            .filter(|value| value.is_finite())
            .ok_or_else(|| {
                crate::error::QdrantError::Decode(format!(
                    "Invalid vector value at result index {result_idx}, vector index {idx}"
                ))
            })?;
        let value = value as f32;
        if !value.is_finite() {
            return Err(crate::error::QdrantError::Decode(format!(
                "Invalid vector value at result index {result_idx}, vector index {idx}"
            )));
        }
        vector.push(value);
    }

    Ok(Some(vector))
}

/// Parse a point ID from JSON.
pub fn parse_point_id(value: &JsonValue) -> Option<PointId> {
    if let Some(s) = value.as_str() {
        if s.trim().is_empty() {
            None
        } else {
            Some(PointId::Uuid(s.to_string()))
        }
    } else {
        value.as_u64().map(PointId::Num)
    }
}

/// Parse payload from JSON object.
pub fn parse_payload(value: &JsonValue) -> crate::point::Payload {
    parse_payload_checked(value, 0).unwrap_or_default()
}

fn parse_payload_checked(
    value: &JsonValue,
    result_idx: usize,
) -> QdrantResult<crate::point::Payload> {
    let mut payload = crate::point::Payload::new();

    match value {
        JsonValue::Null => Ok(payload),
        JsonValue::Object(obj) => {
            for (key, value) in obj {
                ensure_payload_key(key).map_err(crate::error::QdrantError::Decode)?;
                let payload_value = json_to_payload_value_checked(
                    value,
                    &format!("result[{result_idx}].payload.{key}"),
                )?;
                payload.insert(key.clone(), payload_value);
            }
            Ok(payload)
        }
        _ => Err(crate::error::QdrantError::Decode(format!(
            "Invalid payload object at result index {result_idx}"
        ))),
    }
}

/// Convert PayloadValue to JSON.
fn payload_value_to_json(value: &PayloadValue) -> Result<JsonValue, String> {
    match value {
        PayloadValue::String(s) => Ok(json!(s)),
        PayloadValue::Integer(n) => Ok(json!(n)),
        PayloadValue::Float(f) if f.is_finite() => Ok(json!(f)),
        PayloadValue::Float(f) => Err(format!(
            "Qdrant payload contains non-finite float value: {f}"
        )),
        PayloadValue::Bool(b) => Ok(json!(b)),
        PayloadValue::List(arr) => {
            let values: Result<Vec<JsonValue>, String> =
                arr.iter().map(payload_value_to_json).collect();
            Ok(JsonValue::Array(values?))
        }
        PayloadValue::Object(obj) => Ok(JsonValue::Object(payload_to_json_map(obj)?)),
        PayloadValue::Null => Ok(JsonValue::Null),
    }
}

fn payload_to_json_map(
    payload: &crate::point::Payload,
) -> Result<serde_json::Map<String, JsonValue>, String> {
    payload
        .iter()
        .map(|(k, v)| {
            ensure_payload_key(k)?;
            Ok((k.clone(), payload_value_to_json(v)?))
        })
        .collect()
}

fn json_to_payload_value_checked(value: &JsonValue, path: &str) -> QdrantResult<PayloadValue> {
    match value {
        JsonValue::Null => Ok(PayloadValue::Null),
        JsonValue::Bool(b) => Ok(PayloadValue::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(PayloadValue::Integer(i))
            } else if let Some(u) = n.as_u64() {
                let i = i64::try_from(u).map_err(|_| {
                    crate::error::QdrantError::Decode(format!(
                        "Payload integer out of range at {path}"
                    ))
                })?;
                Ok(PayloadValue::Integer(i))
            } else {
                let f = n
                    .as_f64()
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| {
                        crate::error::QdrantError::Decode(format!(
                            "Invalid payload number at {path}"
                        ))
                    })?;
                Ok(PayloadValue::Float(f))
            }
        }
        JsonValue::String(s) => Ok(PayloadValue::String(s.clone())),
        JsonValue::Array(arr) => {
            let mut items = Vec::with_capacity(arr.len());
            for (idx, value) in arr.iter().enumerate() {
                items.push(json_to_payload_value_checked(
                    value,
                    &format!("{path}[{idx}]"),
                )?);
            }
            Ok(PayloadValue::List(items))
        }
        JsonValue::Object(obj) => {
            let mut map = std::collections::HashMap::with_capacity(obj.len());
            for (key, value) in obj {
                ensure_payload_key(key).map_err(crate::error::QdrantError::Decode)?;
                map.insert(
                    key.clone(),
                    json_to_payload_value_checked(value, &format!("{path}.{key}"))?,
                );
            }
            Ok(PayloadValue::Object(map))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_search_request() {
        let vector = vec![0.1, 0.2, 0.3];
        let json_bytes = encode_search_request(&vector, 10, None, None, false);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        // Check structure exists
        assert!(json["vector"].is_array());
        assert_eq!(json["limit"], 10);
        assert_eq!(json["with_payload"], true);

        // Check vector length
        assert_eq!(json["vector"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_encode_upsert_request() {
        let point = Point::new("test-id", vec![0.5, 0.5]);
        let json_bytes = encode_upsert_request(&[point]);
        let json_str = String::from_utf8(json_bytes).unwrap();

        assert!(json_str.contains("\"points\""));
        assert!(json_str.contains("\"test-id\""));
        assert!(json_str.contains("[0.5,0.5]"));
    }

    #[test]
    fn encode_search_request_rejects_non_finite_vector_json() {
        let json_bytes = encode_search_request(&[0.1, f32::NAN], 10, None, None, false);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("non-finite vector value")
        );
    }

    #[test]
    fn encode_search_request_rejects_empty_vector_and_zero_limit_json() {
        let json_bytes = encode_search_request(&[], 10, None, None, false);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();
        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("must not be empty")
        );

        let json_bytes = encode_search_request(&[0.1], 0, None, None, false);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();
        assert!(json["error"].as_str().unwrap().contains("limit"));
    }

    #[test]
    fn encode_search_request_rejects_non_finite_threshold_json() {
        let json_bytes = encode_search_request(&[0.1], 10, None, Some(f32::INFINITY), false);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(json["error"].as_str().unwrap().contains("score threshold"));
    }

    #[test]
    fn encode_upsert_request_rejects_non_finite_payload_json() {
        let point = Point::new("test-id", vec![0.5, 0.5])
            .with_payload("score", PayloadValue::Float(f64::INFINITY));
        let json_bytes = encode_upsert_request(&[point]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(json["error"].as_str().unwrap().contains("non-finite float"));
    }

    #[test]
    fn encode_upsert_request_rejects_empty_point_list_json() {
        let json_bytes = encode_upsert_request(&[]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(json["error"].as_str().unwrap().contains("point list"));
    }

    #[test]
    fn encode_upsert_request_rejects_empty_payload_keys_json() {
        let mut nested = crate::point::Payload::new();
        nested.insert("".to_string(), PayloadValue::String("bad".to_string()));
        let point = Point::new("test-id", vec![0.5, 0.5])
            .with_payload("  ", PayloadValue::String("bad".to_string()));
        let json_bytes = encode_upsert_request(&[point]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();
        assert!(json["error"].as_str().unwrap().contains("field name"));

        let point = Point::new("test-id", vec![0.5, 0.5])
            .with_payload("metadata", PayloadValue::Object(nested));
        let json_bytes = encode_upsert_request(&[point]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();
        assert!(json["error"].as_str().unwrap().contains("field name"));
    }

    #[test]
    fn encode_upsert_request_rejects_empty_point_ids_json() {
        let point = Point::new(PointId::Uuid(" ".to_string()), vec![0.5, 0.5]);
        let json_bytes = encode_upsert_request(&[point]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(json["error"].as_str().unwrap().contains("point id"));
    }

    #[test]
    fn encode_multi_vector_request_rejects_non_finite_vector_json() {
        let point = crate::point::MultiVectorPoint::new("test-id")
            .with_vector("image", vec![0.5, f32::NEG_INFINITY]);
        let json_bytes = encode_upsert_multi_vector_request(&[point]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("non-finite vector value")
        );
    }

    #[test]
    fn encode_multi_vector_request_rejects_empty_named_vector_set_json() {
        let point = crate::point::MultiVectorPoint::new("test-id");
        let json_bytes = encode_upsert_multi_vector_request(&[point]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("at least one named vector")
        );
    }

    #[test]
    fn encode_multi_vector_request_rejects_empty_point_list_json() {
        let json_bytes = encode_upsert_multi_vector_request(&[]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(json["error"].as_str().unwrap().contains("point list"));
    }

    #[test]
    fn encode_multi_vector_request_rejects_empty_payload_keys_json() {
        let point = crate::point::MultiVectorPoint::new("test-id")
            .with_vector("image", vec![0.1, 0.2])
            .with_payload("", PayloadValue::String("bad".to_string()));
        let json_bytes = encode_upsert_multi_vector_request(&[point]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(json["error"].as_str().unwrap().contains("field name"));
    }

    #[test]
    fn encode_multi_vector_request_rejects_empty_named_vector_name_json() {
        let point =
            crate::point::MultiVectorPoint::new("test-id").with_vector("  ", vec![0.1, 0.2]);
        let json_bytes = encode_upsert_multi_vector_request(&[point]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("vector name must not be empty")
        );
    }

    #[test]
    fn encode_multi_vector_request_rejects_empty_point_ids_json() {
        let point = crate::point::MultiVectorPoint::new(PointId::Uuid("".to_string()))
            .with_vector("image", vec![0.1, 0.2]);
        let json_bytes = encode_upsert_multi_vector_request(&[point]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(json["error"].as_str().unwrap().contains("point id"));
    }

    #[test]
    fn encode_create_collection_request_rejects_zero_vector_size_json() {
        let json_bytes = encode_create_collection_request(0, "Cosine");
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(json["error"].as_str().unwrap().contains("vector_size"));
    }

    #[test]
    fn encode_create_collection_request_validates_distance_json() {
        let json_bytes = encode_create_collection_request(32, "cosine");
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();
        assert_eq!(json["vectors"]["distance"], "Cosine");

        let json_bytes = encode_create_collection_request(32, "bad-distance");
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();
        assert!(json["error"].as_str().unwrap().contains("distance"));
    }

    #[test]
    fn test_encode_delete_request() {
        let ids = vec![PointId::Uuid("id1".to_string()), PointId::Num(42)];
        let json_bytes = encode_delete_request(&ids);
        let json_str = String::from_utf8(json_bytes).unwrap();

        assert!(json_str.contains("\"id1\""));
        assert!(json_str.contains("42"));
    }

    #[test]
    fn encode_delete_request_rejects_empty_id_list_json() {
        let json_bytes = encode_delete_request(&[]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();

        assert!(json["error"].as_str().unwrap().contains("point id list"));

        let json_bytes = encode_delete_request(&[PointId::Uuid("  ".to_string())]);
        let json: JsonValue = serde_json::from_slice(&json_bytes).unwrap();
        assert!(json["error"].as_str().unwrap().contains("point id"));
    }

    #[test]
    fn test_decode_search_response() {
        let response = r#"{
            "result": [
                {"id": "abc", "score": 0.95, "payload": {"name": "test"}},
                {"id": 123, "score": 0.80, "payload": {}, "vector": [0.1, 0.2]}
            ]
        }"#;

        let results = decode_search_response(response.as_bytes()).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].score, 0.95);
        assert_eq!(results[1].score, 0.80);
        assert_eq!(results[1].vector.as_deref(), Some(&[0.1, 0.2][..]));
    }

    #[test]
    fn decode_search_response_rejects_malformed_results() {
        let missing_id = r#"{
            "result": [
                {"score": 0.95, "payload": {"name": "test"}}
            ]
        }"#;
        let err = decode_search_response(missing_id.as_bytes())
            .expect_err("missing id should fail closed");
        assert!(err.to_string().contains("Missing point id"));

        let bad_vector = r#"{
            "result": [
                {"id": "abc", "score": 0.95, "payload": {}, "vector": [0.1, "oops"]}
            ]
        }"#;
        let err = decode_search_response(bad_vector.as_bytes())
            .expect_err("bad vector should fail closed");
        assert!(err.to_string().contains("Invalid vector value"));

        let empty_vector = r#"{
            "result": [
                {"id": "abc", "score": 0.95, "payload": {}, "vector": []}
            ]
        }"#;
        let err = decode_search_response(empty_vector.as_bytes())
            .expect_err("empty vector should fail closed");
        assert!(err.to_string().contains("Empty vector"));

        let empty_id = r#"{
            "result": [
                {"id": "", "score": 0.95, "payload": {}}
            ]
        }"#;
        let err =
            decode_search_response(empty_id.as_bytes()).expect_err("empty id should fail closed");
        assert!(err.to_string().contains("Missing point id"));

        let huge_score = r#"{
            "result": [
                {"id": "abc", "score": 1e100, "payload": {}}
            ]
        }"#;
        let err = decode_search_response(huge_score.as_bytes())
            .expect_err("score that overflows f32 should fail closed");
        assert!(err.to_string().contains("Invalid score"));
    }

    #[test]
    fn decode_search_response_rejects_malformed_payload() {
        let payload_array = r#"{
            "result": [
                {"id": "abc", "score": 0.95, "payload": ["not", "an", "object"]}
            ]
        }"#;
        let err = decode_search_response(payload_array.as_bytes())
            .expect_err("payload array should fail closed");
        assert!(err.to_string().contains("Invalid payload object"));

        let oversized_integer = r#"{
            "result": [
                {"id": "abc", "score": 0.95, "payload": {"too_big": 18446744073709551615}}
            ]
        }"#;
        let err = decode_search_response(oversized_integer.as_bytes())
            .expect_err("payload integer overflow should fail closed");
        assert!(err.to_string().contains("Payload integer out of range"));

        let empty_payload_key = r#"{
            "result": [
                {"id": "abc", "score": 0.95, "payload": {"": "bad"}}
            ]
        }"#;
        let err = decode_search_response(empty_payload_key.as_bytes())
            .expect_err("empty payload key should fail closed");
        assert!(err.to_string().contains("field name"));

        let nested_empty_payload_key = r#"{
            "result": [
                {"id": "abc", "score": 0.95, "payload": {"meta": {" ": "bad"}}}
            ]
        }"#;
        let err = decode_search_response(nested_empty_payload_key.as_bytes())
            .expect_err("nested empty payload key should fail closed");
        assert!(err.to_string().contains("field name"));
    }

    #[test]
    fn test_encode_conditions_to_filter() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let conditions = vec![
            Condition {
                left: Expr::Named("category".to_string()),
                op: Operator::Eq,
                value: Value::String("electronics".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("price".to_string()),
                op: Operator::Lt,
                value: Value::Int(1000),
                is_array_unnest: false,
            },
        ];

        let filter = encode_conditions_to_filter(&conditions, false);

        // Should have "must" with 2 clauses
        assert!(filter["must"].is_array());
        let must = filter["must"].as_array().unwrap();
        assert_eq!(must.len(), 2);

        // First clause: category match
        assert_eq!(must[0]["key"], "category");
        assert_eq!(must[0]["match"]["value"], "electronics");

        // Second clause: price range
        assert_eq!(must[1]["key"], "price");
        assert_eq!(must[1]["range"]["lt"], 1000);
    }

    #[test]
    fn test_encode_conditions_to_filter_or() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let conditions = vec![Condition {
            left: Expr::Named("status".to_string()),
            op: Operator::Eq,
            value: Value::String("active".to_string()),
            is_array_unnest: false,
        }];

        let filter = encode_conditions_to_filter(&conditions, true);

        // Should have "should" instead of "must"
        assert!(filter["should"].is_array());
        assert!(filter["must"].is_null());
    }

    #[test]
    fn encode_conditions_to_filter_uses_has_id_for_point_id_json() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let conditions = vec![Condition {
            left: Expr::Named("id".to_string()),
            op: Operator::Eq,
            value: Value::Int(42),
            is_array_unnest: false,
        }];

        let filter = encode_conditions_to_filter(&conditions, false);

        assert_eq!(filter["must"][0]["has_id"][0], 42);
        assert!(filter["must"][0]["key"].is_null());
    }

    #[test]
    fn encode_conditions_to_filter_supports_null_uuid_as_is_null_json() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let conditions = vec![Condition {
            left: Expr::Named("deleted_at".to_string()),
            op: Operator::IsNull,
            value: Value::NullUuid,
            is_array_unnest: false,
        }];

        let filter = encode_conditions_to_filter(&conditions, false);

        assert_eq!(filter["must"][0]["is_null"]["key"], "deleted_at");
    }

    #[test]
    fn encode_conditions_to_filter_fails_closed_on_unsupported_operator_json() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let conditions = vec![Condition {
            left: Expr::Named("status".to_string()),
            op: Operator::Ne,
            value: Value::String("deleted".to_string()),
            is_array_unnest: false,
        }];

        let filter = encode_conditions_to_filter(&conditions, false);

        assert_eq!(filter["must"][0]["key"], "__qail_unrepresentable_filter__");
    }

    #[test]
    fn encode_conditions_to_filter_fails_closed_on_is_not_null_json() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let conditions = vec![Condition {
            left: Expr::Named("deleted_at".to_string()),
            op: Operator::IsNotNull,
            value: Value::Null,
            is_array_unnest: false,
        }];

        let filter = encode_conditions_to_filter(&conditions, false);

        assert_eq!(filter["must"][0]["key"], "__qail_unrepresentable_filter__");
    }

    #[test]
    fn encode_conditions_to_filter_fails_closed_on_unrepresentable_condition() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let conditions = vec![
            Condition {
                left: Expr::Named("tenant_id".to_string()),
                op: Operator::Eq,
                value: Value::String("tenant-a".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Literal(Value::String("not-a-field".to_string())),
                op: Operator::Eq,
                value: Value::String("tenant-b".to_string()),
                is_array_unnest: false,
            },
        ];

        let filter = encode_conditions_to_filter(&conditions, false);

        assert_eq!(filter["must"][0]["key"], "__qail_unrepresentable_filter__");
        assert_eq!(filter["must"][0]["range"]["gt"], 1);
        assert_eq!(filter["must"][0]["range"]["lt"], 0);
    }

    #[test]
    fn encode_conditions_to_filter_fails_closed_on_bad_in_value() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let conditions = vec![Condition {
            left: Expr::Named("category".to_string()),
            op: Operator::In,
            value: Value::Array(vec![
                Value::String("a".to_string()),
                Value::Vector(vec![1.0, 2.0]),
            ]),
            is_array_unnest: false,
        }];

        let filter = encode_conditions_to_filter(&conditions, false);

        assert_eq!(filter["must"][0]["key"], "__qail_unrepresentable_filter__");
        assert!(filter.to_string().contains("\"gt\":1"));

        let conditions = vec![Condition {
            left: Expr::Named("category".to_string()),
            op: Operator::In,
            value: Value::Array(vec![]),
            is_array_unnest: false,
        }];
        let filter = encode_conditions_to_filter(&conditions, false);
        assert_eq!(filter["must"][0]["key"], "__qail_unrepresentable_filter__");

        let conditions = vec![Condition {
            left: Expr::Named("category".to_string()),
            op: Operator::In,
            value: Value::Array(vec![Value::Null]),
            is_array_unnest: false,
        }];
        let filter = encode_conditions_to_filter(&conditions, false);
        assert_eq!(filter["must"][0]["key"], "__qail_unrepresentable_filter__");
    }

    #[test]
    fn encode_conditions_to_filter_fails_closed_on_non_finite_float() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let conditions = vec![Condition {
            left: Expr::Named("score".to_string()),
            op: Operator::Gt,
            value: Value::Float(f64::NAN),
            is_array_unnest: false,
        }];

        let filter = encode_conditions_to_filter(&conditions, false);

        assert_eq!(filter["must"][0]["key"], "__qail_unrepresentable_filter__");
        assert!(!filter.to_string().contains("null"));
    }

    #[test]
    fn encode_conditions_to_filter_rejects_empty_id_text_and_quoted_fields() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let conditions = vec![Condition {
            left: Expr::Named("id".to_string()),
            op: Operator::Eq,
            value: Value::String(" ".to_string()),
            is_array_unnest: false,
        }];
        let filter = encode_conditions_to_filter(&conditions, false);
        assert_eq!(filter["must"][0]["key"], "__qail_unrepresentable_filter__");

        let conditions = vec![Condition {
            left: Expr::Named("\"   \"".to_string()),
            op: Operator::Eq,
            value: Value::String("active".to_string()),
            is_array_unnest: false,
        }];
        let filter = encode_conditions_to_filter(&conditions, false);
        assert_eq!(filter["must"][0]["key"], "__qail_unrepresentable_filter__");

        let conditions = vec![Condition {
            left: Expr::Named("description".to_string()),
            op: Operator::Contains,
            value: Value::String("  ".to_string()),
            is_array_unnest: false,
        }];
        let filter = encode_conditions_to_filter(&conditions, false);
        assert_eq!(filter["must"][0]["key"], "__qail_unrepresentable_filter__");
    }
}
