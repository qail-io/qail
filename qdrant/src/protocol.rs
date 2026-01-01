//! Qdrant REST/JSON protocol encoding.
//!
//! This module handles encoding QAIL AST to Qdrant's REST API JSON format.
//! Using JSON instead of gRPC for simplicity and portability.

use crate::error::QdrantResult;
use crate::point::{PayloadValue, Point, PointId, ScoredPoint};
use serde_json::{json, Value as JsonValue};

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
    let mut request = json!({
        "vector": vector,
        "limit": limit,
        "with_payload": true,
        "with_vector": with_vector,
    });
    
    if let Some(off) = offset {
        request["offset"] = json!(off);
    }
    
    if let Some(threshold) = score_threshold {
        request["score_threshold"] = json!(threshold);
    }
    
    serde_json::to_vec(&request).unwrap_or_default()
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
    
    if let Some(threshold) = score_threshold {
        request["score_threshold"] = json!(threshold);
    }
    
    serde_json::to_vec(&request).unwrap_or_default()
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
    let points_json: Vec<JsonValue> = points
        .iter()
        .map(|p| {
            let id = match &p.id {
                PointId::Uuid(s) => json!(s),
                PointId::Num(n) => json!(n),
            };
            
            let payload: JsonValue = p.payload
                .iter()
                .map(|(k, v)| (k.clone(), payload_value_to_json(v)))
                .collect();
            
            json!({
                "id": id,
                "vector": p.vector,
                "payload": payload,
            })
        })
        .collect();
    
    let request = json!({ "points": points_json });
    serde_json::to_vec(&request).unwrap_or_default()
}

/// Encode an upsert request for multi-vector points (named vectors).
///
/// For collections with multiple vector fields (e.g., "title", "content").
pub fn encode_upsert_multi_vector_request(points: &[crate::point::MultiVectorPoint]) -> Vec<u8> {
    use crate::point::MultiVectorPoint;
    
    let points_json: Vec<JsonValue> = points
        .iter()
        .map(|p: &MultiVectorPoint| {
            let id = match &p.id {
                PointId::Uuid(s) => json!(s),
                PointId::Num(n) => json!(n),
            };
            
            let payload: JsonValue = p.payload
                .iter()
                .map(|(k, v)| (k.clone(), payload_value_to_json(v)))
                .collect();
            
            // Named vectors as object
            let vectors: JsonValue = p.vectors
                .iter()
                .map(|(k, v)| (k.clone(), json!(v)))
                .collect();
            
            json!({
                "id": id,
                "vector": vectors,
                "payload": payload,
            })
        })
        .collect();
    
    let request = json!({ "points": points_json });
    serde_json::to_vec(&request).unwrap_or_default()
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
    let ids_json: Vec<JsonValue> = ids
        .iter()
        .map(|id| match id {
            PointId::Uuid(s) => json!(s),
            PointId::Num(n) => json!(n),
        })
        .collect();
    
    let request = json!({ "points": ids_json });
    serde_json::to_vec(&request).unwrap_or_default()
}

/// Encode create collection request.
///
/// Generates JSON for PUT /collections/{collection}
pub fn encode_create_collection_request(
    vector_size: u64,
    distance: &str, // "Cosine", "Euclidean", "Dot"
) -> Vec<u8> {
    let request = json!({
        "vectors": {
            "size": vector_size,
            "distance": distance,
        }
    });
    serde_json::to_vec(&request).unwrap_or_default()
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
pub fn encode_conditions_to_filter(conditions: &[qail_core::ast::Condition], is_or: bool) -> JsonValue {
    use qail_core::ast::{Expr, Operator, Value};
    
    let clauses: Vec<JsonValue> = conditions
        .iter()
        .filter_map(|cond| {
            // Extract field name from left expression
            let key = match &cond.left {
                Expr::Named(name) => name.clone(),
                Expr::Aliased { name, .. } => name.clone(),
                _ => return None,
            };
            
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
                (Operator::Eq, Value::Bool(b)) => json!({
                    "key": key,
                    "match": { "value": b }
                }),
                
                // Range operators
                (Operator::Gt, Value::Int(n)) => json!({
                    "key": key,
                    "range": { "gt": n }
                }),
                (Operator::Gt, Value::Float(f)) => json!({
                    "key": key,
                    "range": { "gt": f }
                }),
                (Operator::Gte, Value::Int(n)) => json!({
                    "key": key,
                    "range": { "gte": n }
                }),
                (Operator::Gte, Value::Float(f)) => json!({
                    "key": key,
                    "range": { "gte": f }
                }),
                (Operator::Lt, Value::Int(n)) => json!({
                    "key": key,
                    "range": { "lt": n }
                }),
                (Operator::Lt, Value::Float(f)) => json!({
                    "key": key,
                    "range": { "lt": f }
                }),
                (Operator::Lte, Value::Int(n)) => json!({
                    "key": key,
                    "range": { "lte": n }
                }),
                (Operator::Lte, Value::Float(f)) => json!({
                    "key": key,
                    "range": { "lte": f }
                }),
                
                // In / NotIn (array membership)
                (Operator::In, Value::Array(arr)) => {
                    let values: Vec<JsonValue> = arr.iter().filter_map(value_to_json).collect();
                    json!({
                        "key": key,
                        "match": { "any": values }
                    })
                },
                
                // IsNull / IsNotNull
                (Operator::IsNull, _) => json!({
                    "is_null": { "key": key }
                }),
                (Operator::IsNotNull, _) => json!({
                    "is_empty": { "key": key, "is_empty": false }
                }),
                
                // Text/keyword match with contains
                (Operator::Contains | Operator::Like, Value::String(s)) => json!({
                    "key": key,
                    "match": { "text": s }
                }),
                
                // Default: try match for other types
                (_, Value::String(s)) => json!({
                    "key": key,
                    "match": { "value": s }
                }),
                (_, Value::Int(n)) => json!({
                    "key": key,
                    "match": { "value": n }
                }),
                
                _ => return None,
            };
            
            Some(clause)
        })
        .collect();
    
    // Use "should" for OR, "must" for AND
    if is_or {
        json!({ "should": clauses })
    } else {
        json!({ "must": clauses })
    }
}

/// Convert Value to JsonValue for filter encoding.
fn value_to_json(value: &qail_core::ast::Value) -> Option<JsonValue> {
    use qail_core::ast::Value;
    match value {
        Value::String(s) => Some(json!(s)),
        Value::Int(n) => Some(json!(n)),
        Value::Float(f) => Some(json!(f)),
        Value::Bool(b) => Some(json!(b)),
        Value::Null => Some(JsonValue::Null),
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
    
    let scored_points: Vec<ScoredPoint> = results
        .iter()
        .filter_map(|item| {
            let id = parse_point_id(&item["id"])?;
            let score = item["score"].as_f64()? as f32;
            let payload = parse_payload(&item["payload"]);
            let vector = item["vector"]
                .as_array()
                .map(|arr| arr.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect());
            
            Some(ScoredPoint { id, score, payload, vector })
        })
        .collect();
    
    Ok(scored_points)
}

/// Parse a point ID from JSON.
pub fn parse_point_id(value: &JsonValue) -> Option<PointId> {
    if let Some(s) = value.as_str() {
        Some(PointId::Uuid(s.to_string()))
    } else if let Some(n) = value.as_u64() {
        Some(PointId::Num(n))
    } else {
        None
    }
}

/// Parse payload from JSON object.
pub fn parse_payload(value: &JsonValue) -> crate::point::Payload {
    let mut payload = crate::point::Payload::new();
    
    if let Some(obj) = value.as_object() {
        for (k, v) in obj {
            if let Some(pv) = json_to_payload_value(v) {
                payload.insert(k.clone(), pv);
            }
        }
    }
    
    payload
}

/// Convert PayloadValue to JSON.
fn payload_value_to_json(value: &PayloadValue) -> JsonValue {
    match value {
        PayloadValue::String(s) => json!(s),
        PayloadValue::Integer(n) => json!(n),
        PayloadValue::Float(f) => json!(f),
        PayloadValue::Bool(b) => json!(b),
        PayloadValue::List(arr) => {
            JsonValue::Array(arr.iter().map(payload_value_to_json).collect())
        }
        PayloadValue::Object(obj) => {
            JsonValue::Object(obj.iter().map(|(k, v)| (k.clone(), payload_value_to_json(v))).collect())
        }
        PayloadValue::Null => JsonValue::Null,
    }
}

/// Convert JSON to PayloadValue.
fn json_to_payload_value(value: &JsonValue) -> Option<PayloadValue> {
    match value {
        JsonValue::Null => Some(PayloadValue::Null),
        JsonValue::Bool(b) => Some(PayloadValue::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(PayloadValue::Integer(i))
            } else {
                n.as_f64().map(PayloadValue::Float)
            }
        }
        JsonValue::String(s) => Some(PayloadValue::String(s.clone())),
        JsonValue::Array(arr) => {
            let items: Vec<PayloadValue> = arr.iter().filter_map(json_to_payload_value).collect();
            Some(PayloadValue::List(items))
        }
        JsonValue::Object(obj) => {
            let map: std::collections::HashMap<String, PayloadValue> = obj
                .iter()
                .filter_map(|(k, v)| json_to_payload_value(v).map(|pv| (k.clone(), pv)))
                .collect();
            Some(PayloadValue::Object(map))
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
    fn test_encode_delete_request() {
        let ids = vec![PointId::Uuid("id1".to_string()), PointId::Num(42)];
        let json_bytes = encode_delete_request(&ids);
        let json_str = String::from_utf8(json_bytes).unwrap();
        
        assert!(json_str.contains("\"id1\""));
        assert!(json_str.contains("42"));
    }
    
    #[test]
    fn test_decode_search_response() {
        let response = r#"{
            "result": [
                {"id": "abc", "score": 0.95, "payload": {"name": "test"}},
                {"id": 123, "score": 0.80, "payload": {}}
            ]
        }"#;
        
        let results = decode_search_response(response.as_bytes()).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].score, 0.95);
        assert_eq!(results[1].score, 0.80);
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
        
        let conditions = vec![
            Condition {
                left: Expr::Named("status".to_string()),
                op: Operator::Eq,
                value: Value::String("active".to_string()),
                is_array_unnest: false,
            },
        ];
        
        let filter = encode_conditions_to_filter(&conditions, true);
        
        // Should have "should" instead of "must"
        assert!(filter["should"].is_array());
        assert!(filter["must"].is_null());
    }
}
