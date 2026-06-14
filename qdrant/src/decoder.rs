//! Zero-copy Protobuf decoder for Qdrant gRPC responses.
//!
//! Decodes protobuf wire format directly without intermediate allocations.
//! Matches the zero-copy pattern of proto_encoder.rs.
//!
//! ## Supported Responses
//! - SearchResponse → `Vec<ScoredPoint>`
//! - GetResponse → `Vec<RetrievedPoint>` (same as ScoredPoint with score=0)
//! - ScrollResponse → `ScrollResult { points, next_offset }`

use crate::error::{QdrantError, QdrantResult};
use crate::point::{Payload, PayloadValue, PointId, ScoredPoint};

// ============================================================================
// Wire Type Constants
// ============================================================================

const WIRE_VARINT: u8 = 0;
const WIRE_FIXED64: u8 = 1;
const WIRE_LEN: u8 = 2;
const WIRE_FIXED32: u8 = 5;

/// Maximum recursion depth for nested protobuf Value decoding.
/// Prevents stack overflow from crafted deeply nested Struct/List payloads.
const MAX_DECODE_DEPTH: usize = 32;
/// Maximum protobuf field number allowed by the wire format.
const MAX_PROTO_FIELD_NUMBER: u64 = 536_870_911;

fn ensure_non_empty_decoded_name(value: &str, label: &str) -> QdrantResult<()> {
    if value.trim().is_empty() {
        return Err(QdrantError::Decode(format!("{label} must not be empty")));
    }
    Ok(())
}

// ============================================================================
// SearchResponse Field Numbers
// ============================================================================

const SEARCH_RESULT: u32 = 1;

// ============================================================================
// ScoredPoint Field Numbers
// ============================================================================

const SCORED_POINT_ID: u32 = 1;
const SCORED_POINT_PAYLOAD: u32 = 2;
const SCORED_POINT_SCORE: u32 = 3;
const SCORED_POINT_VECTORS: u32 = 6;

// ============================================================================
// PointId Field Numbers
// ============================================================================

const POINT_ID_NUM: u32 = 1;
const POINT_ID_UUID: u32 = 2;

// ============================================================================
// GetResponse / ScrollResponse Field Numbers
// ============================================================================

/// GetResponse.result (field 1, repeated RetrievedPoint)
const GET_RESULT: u32 = 1;

/// ScrollResponse.next_page_offset (field 1, PointId)
const SCROLL_NEXT_OFFSET: u32 = 1;
/// ScrollResponse.result (field 2, repeated RetrievedPoint)
const SCROLL_RESULT: u32 = 2;

/// RetrievedPoint field numbers (same structure as ScoredPoint but no score)
const RETRIEVED_POINT_ID: u32 = 1;
const RETRIEVED_POINT_PAYLOAD: u32 = 2;
const RETRIEVED_POINT_VECTORS: u32 = 4;

// ============================================================================
// Value message field numbers (for payload decoding)
// ============================================================================

const VALUE_NULL: u32 = 1;
const VALUE_DOUBLE: u32 = 2;
const VALUE_INTEGER: u32 = 3;
const VALUE_STRING: u32 = 4;
const VALUE_BOOL: u32 = 5;
const VALUE_STRUCT: u32 = 6;
const VALUE_LIST: u32 = 7;

// ============================================================================
// Vector output field numbers
// ============================================================================

const VECTORS_OUTPUT_VECTOR: u32 = 1;
const VECTORS_OUTPUT_NAMED: u32 = 2;
const NAMED_VECTORS_OUTPUT_ENTRY: u32 = 1;
const NAMED_VECTOR_ENTRY_KEY: u32 = 1;
const NAMED_VECTOR_ENTRY_VALUE: u32 = 2;
const VECTOR_OUTPUT_DEPRECATED_DATA: u32 = 1;
const VECTOR_OUTPUT_DENSE: u32 = 101;
const DENSE_VECTOR_DATA: u32 = 1;

// ============================================================================
// Varint Decoding
// ============================================================================

/// Decode a varint from the buffer, advancing the cursor.
#[inline]
fn decode_varint(buf: &mut &[u8]) -> QdrantResult<u64> {
    let mut result: u64 = 0;

    for byte_index in 0..10 {
        if buf.is_empty() {
            return Err(QdrantError::Decode(
                "Unexpected end of data in varint".to_string(),
            ));
        }

        let byte = buf[0];
        *buf = &buf[1..];
        let payload = (byte & 0x7F) as u64;

        if byte_index == 9 && payload > 1 {
            return Err(QdrantError::Decode("Varint overflows u64".to_string()));
        }

        result |= payload << (byte_index * 7);

        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }

    Err(QdrantError::Decode("Varint too long".to_string()))
}

/// Decode a field tag (field_number << 3 | wire_type).
#[inline]
fn decode_tag(buf: &mut &[u8]) -> QdrantResult<(u32, u8)> {
    let tag = decode_varint(buf)?;
    let field_number = tag >> 3;
    if field_number == 0 || field_number > MAX_PROTO_FIELD_NUMBER {
        return Err(QdrantError::Decode(format!(
            "Invalid protobuf field number: {field_number}"
        )));
    }
    let wire_type = (tag & 0x07) as u8;
    Ok((field_number as u32, wire_type))
}

/// Skip a field value based on wire type.
#[inline]
fn skip_field(buf: &mut &[u8], wire_type: u8) -> QdrantResult<()> {
    match wire_type {
        WIRE_VARINT => {
            decode_varint(buf)?;
        }
        WIRE_FIXED64 => {
            if buf.len() < 8 {
                return Err(QdrantError::Decode("Unexpected end of data".to_string()));
            }
            *buf = &buf[8..];
        }
        WIRE_LEN => {
            let len = decode_varint(buf)? as usize;
            if buf.len() < len {
                return Err(QdrantError::Decode("Unexpected end of data".to_string()));
            }
            *buf = &buf[len..];
        }
        WIRE_FIXED32 => {
            if buf.len() < 4 {
                return Err(QdrantError::Decode("Unexpected end of data".to_string()));
            }
            *buf = &buf[4..];
        }
        _ => {
            return Err(QdrantError::Decode(format!(
                "Unknown wire type: {}",
                wire_type
            )));
        }
    }
    Ok(())
}

/// Read a length-delimited submessage, returning its data slice.
#[inline]
fn read_submessage<'a>(buf: &mut &'a [u8]) -> QdrantResult<&'a [u8]> {
    let len = decode_varint(buf)? as usize;
    if buf.len() < len {
        return Err(QdrantError::Decode("Truncated submessage".to_string()));
    }
    let data = &buf[..len];
    *buf = &buf[len..];
    Ok(data)
}

// ============================================================================
// SearchResponse Decoder
// ============================================================================

/// Decode a SearchResponse protobuf message.
///
/// # Zero-Copy Pattern
/// - Parses in a single pass through the buffer
/// - Minimal allocations (only for result Vec and PointId strings)
/// - No intermediate struct copies
pub fn decode_search_response(data: &[u8]) -> QdrantResult<Vec<ScoredPoint>> {
    let mut results = Vec::new();
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;

        match field_number {
            SEARCH_RESULT => {
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode(
                        "Expected length-delimited for ScoredPoint".to_string(),
                    ));
                }
                let point_data = read_submessage(&mut buf)?;
                let point = decode_scored_point(point_data)?;
                results.push(point);
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Ok(results)
}

/// Decode a single ScoredPoint message (with payload support).
fn decode_scored_point(data: &[u8]) -> QdrantResult<ScoredPoint> {
    let mut id = None;
    let mut score = 0.0f32;
    let mut payload = Payload::new();
    let mut vector = None;
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;

        match field_number {
            SCORED_POINT_ID => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let id_data = read_submessage(&mut buf)?;
                id = Some(decode_point_id(id_data)?);
            }
            SCORED_POINT_PAYLOAD => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let entry_data = read_submessage(&mut buf)?;
                // Payload is map<string, Value> — each entry is a MapEntry message
                let (key, value) = decode_map_entry(entry_data, 0)?;
                payload.insert(key, value);
            }
            SCORED_POINT_SCORE => {
                if wire_type != WIRE_FIXED32 {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                if buf.len() < 4 {
                    return Err(QdrantError::Decode("Truncated score".to_string()));
                }
                let bytes = [buf[0], buf[1], buf[2], buf[3]];
                score = f32::from_le_bytes(bytes);
                if !score.is_finite() {
                    return Err(QdrantError::Decode(
                        "Invalid non-finite score value".to_string(),
                    ));
                }
                buf = &buf[4..];
            }
            SCORED_POINT_VECTORS => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let vec_data = read_submessage(&mut buf)?;
                vector = decode_vectors(vec_data)?;
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    let id = id.ok_or_else(|| QdrantError::Decode("Missing point id".to_string()))?;

    Ok(ScoredPoint {
        id,
        score,
        payload,
        vector,
    })
}

// ============================================================================
// GetResponse Decoder
// ============================================================================

/// Decode a GetResponse protobuf message.
///
/// Returns `Vec<ScoredPoint>` with score = 0.0 for convenience (Get doesn't
/// have scores, but we reuse the same struct to keep the API simple).
pub fn decode_get_response(data: &[u8]) -> QdrantResult<Vec<ScoredPoint>> {
    let mut results = Vec::new();
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;

        match field_number {
            GET_RESULT => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let point_data = read_submessage(&mut buf)?;
                let point = decode_retrieved_point(point_data)?;
                results.push(point);
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Ok(results)
}

// ============================================================================
// ScrollResponse Decoder
// ============================================================================

/// Result of a Scroll operation.
pub struct ScrollResult {
    /// Points in this page.
    pub points: Vec<ScoredPoint>,
    /// Offset for the next page (None when no more pages).
    pub next_offset: Option<PointId>,
}

/// Decode a ScrollResponse protobuf message.
pub fn decode_scroll_response(data: &[u8]) -> QdrantResult<ScrollResult> {
    let mut points = Vec::new();
    let mut next_offset = None;
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;

        match field_number {
            SCROLL_RESULT => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let point_data = read_submessage(&mut buf)?;
                let point = decode_retrieved_point(point_data)?;
                points.push(point);
            }
            SCROLL_NEXT_OFFSET => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let id_data = read_submessage(&mut buf)?;
                next_offset = Some(decode_point_id(id_data)?);
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Ok(ScrollResult {
        points,
        next_offset,
    })
}

/// Decode a RetrievedPoint message (same shape as ScoredPoint, score = 0).
fn decode_retrieved_point(data: &[u8]) -> QdrantResult<ScoredPoint> {
    let mut id = None;
    let mut payload = Payload::new();
    let mut vector = None;
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;

        match field_number {
            RETRIEVED_POINT_ID => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let id_data = read_submessage(&mut buf)?;
                id = Some(decode_point_id(id_data)?);
            }
            RETRIEVED_POINT_PAYLOAD => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let entry_data = read_submessage(&mut buf)?;
                let (key, value) = decode_map_entry(entry_data, 0)?;
                payload.insert(key, value);
            }
            RETRIEVED_POINT_VECTORS => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let vec_data = read_submessage(&mut buf)?;
                vector = decode_vectors(vec_data)?;
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    let id = id.ok_or_else(|| QdrantError::Decode("Missing point id".to_string()))?;

    Ok(ScoredPoint {
        id,
        score: 0.0,
        payload,
        vector,
    })
}

// ============================================================================
// Payload Decoder (map<string, Value>)
// ============================================================================

/// Decode a map entry (MapFieldEntry with string key, Value value).
///
/// Protobuf maps are encoded as repeated fields where each entry is:
/// ```text
/// message MapEntry {
///   string key = 1;
///   Value value = 2;
/// }
/// ```
fn decode_map_entry(data: &[u8], depth: usize) -> QdrantResult<(String, PayloadValue)> {
    let mut key = None;
    let mut value = None;
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;

        match field_number {
            1 => {
                // key (string)
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload map key".to_string(),
                    ));
                }
                let s_data = read_submessage(&mut buf)?;
                let decoded_key = std::str::from_utf8(s_data).map_err(|e| {
                    QdrantError::Decode(format!("Invalid UTF-8 payload map key: {}", e))
                })?;
                key = Some(decoded_key.to_string());
            }
            2 => {
                // value (Value message)
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload map value".to_string(),
                    ));
                }
                let v_data = read_submessage(&mut buf)?;
                value = Some(decode_value_with_depth(v_data, depth)?);
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    let key = key.ok_or_else(|| QdrantError::Decode("Missing payload map key".to_string()))?;
    ensure_non_empty_decoded_name(&key, "Payload map key")?;
    let value =
        value.ok_or_else(|| QdrantError::Decode("Missing payload map value".to_string()))?;
    Ok((key, value))
}

/// Decode a protobuf Value message into PayloadValue.
///
/// ```text
/// message Value {
///   oneof kind {
///     NullValue null_value = 1;
///     double double_value = 2;
///     int64 integer_value = 3;
///     string string_value = 4;
///     bool bool_value = 5;
///     Struct struct_value = 6;
///     ListValue list_value = 7;
///   }
/// }
/// ```
#[cfg(test)]
fn decode_value(data: &[u8]) -> QdrantResult<PayloadValue> {
    decode_value_with_depth(data, 0)
}

/// Decode a protobuf Value message into PayloadValue with depth tracking.
fn decode_value_with_depth(data: &[u8], depth: usize) -> QdrantResult<PayloadValue> {
    if depth > MAX_DECODE_DEPTH {
        return Err(QdrantError::Decode(
            "Payload value nesting exceeds maximum depth".to_string(),
        ));
    }

    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;

        match field_number {
            VALUE_NULL => {
                // NullValue enum (varint)
                if wire_type == WIRE_VARINT {
                    let v = decode_varint(&mut buf)?;
                    if v != 0 {
                        return Err(QdrantError::Decode(format!(
                            "Invalid payload null enum value: {v}"
                        )));
                    }
                } else {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload null value".to_string(),
                    ));
                }
                return Ok(PayloadValue::Null);
            }
            VALUE_DOUBLE => {
                // double (fixed64)
                if wire_type != WIRE_FIXED64 {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload double value".to_string(),
                    ));
                }
                if buf.len() < 8 {
                    return Err(QdrantError::Decode(
                        "Truncated payload double value".to_string(),
                    ));
                }
                let bytes: [u8; 8] = buf[..8].try_into().map_err(|_| {
                    QdrantError::Decode("Truncated payload double value".to_string())
                })?;
                let value = f64::from_le_bytes(bytes);
                if !value.is_finite() {
                    return Err(QdrantError::Decode(
                        "Invalid non-finite payload float value".to_string(),
                    ));
                }
                return Ok(PayloadValue::Float(value));
            }
            VALUE_INTEGER => {
                // int64 (varint)
                if wire_type != WIRE_VARINT {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload integer value".to_string(),
                    ));
                }
                let n = decode_varint(&mut buf)? as i64;
                return Ok(PayloadValue::Integer(n));
            }
            VALUE_STRING => {
                // string (len-delimited)
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload string value".to_string(),
                    ));
                }
                let s_data = read_submessage(&mut buf)?;
                let s = std::str::from_utf8(s_data)
                    .map_err(|e| {
                        QdrantError::Decode(format!("Invalid UTF-8 payload string: {}", e))
                    })?
                    .to_string();
                return Ok(PayloadValue::String(s));
            }
            VALUE_BOOL => {
                // bool (varint)
                if wire_type != WIRE_VARINT {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload bool value".to_string(),
                    ));
                }
                let v = decode_varint(&mut buf)?;
                if v > 1 {
                    return Err(QdrantError::Decode(format!(
                        "Invalid payload bool value: {v}"
                    )));
                }
                return Ok(PayloadValue::Bool(v != 0));
            }
            VALUE_STRUCT => {
                // Struct (len-delimited) — map<string, Value>
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload object value".to_string(),
                    ));
                }
                let struct_data = read_submessage(&mut buf)?;
                let map = decode_struct_fields_with_depth(struct_data, depth + 1)?;
                return Ok(PayloadValue::Object(map));
            }
            VALUE_LIST => {
                // ListValue (len-delimited) — repeated Value
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload list value".to_string(),
                    ));
                }
                let list_data = read_submessage(&mut buf)?;
                let items = decode_list_values_with_depth(list_data, depth + 1)?;
                return Ok(PayloadValue::List(items));
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Err(QdrantError::Decode(
        "Missing payload value kind".to_string(),
    ))
}

/// Decode Struct.fields with depth tracking.
fn decode_struct_fields_with_depth(
    data: &[u8],
    depth: usize,
) -> QdrantResult<std::collections::HashMap<String, PayloadValue>> {
    let mut map = std::collections::HashMap::new();
    if depth > MAX_DECODE_DEPTH {
        return Err(QdrantError::Decode(
            "Payload object nesting exceeds maximum depth".to_string(),
        ));
    }
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;
        match field_number {
            1 => {
                // Struct.fields (field 1, repeated map entry)
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload object field".to_string(),
                    ));
                }
                let entry_data = read_submessage(&mut buf)?;
                let (key, value) = decode_map_entry(entry_data, depth)?;
                map.insert(key, value);
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Ok(map)
}

/// Decode ListValue.values with depth tracking.
fn decode_list_values_with_depth(data: &[u8], depth: usize) -> QdrantResult<Vec<PayloadValue>> {
    let mut items = Vec::new();
    if depth > MAX_DECODE_DEPTH {
        return Err(QdrantError::Decode(
            "Payload list nesting exceeds maximum depth".to_string(),
        ));
    }
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;
        match field_number {
            1 => {
                // ListValue.values (field 1, repeated Value)
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for payload list item".to_string(),
                    ));
                }
                let v_data = read_submessage(&mut buf)?;
                let value = decode_value_with_depth(v_data, depth)?;
                items.push(value);
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Ok(items)
}

// ============================================================================
// Vectors Decoder
// ============================================================================

/// Decode VectorsOutput message → Vec<f32>.
///
/// ```text
/// message VectorsOutput {
///   oneof vectors_options {
///     VectorOutput vector = 1;
///     NamedVectorsOutput vectors = 2;
///   }
/// }
/// message VectorOutput {
///   repeated float data = 1 [deprecated = true];
///   DenseVector dense = 101;
/// }
/// ```
fn decode_vectors(data: &[u8]) -> QdrantResult<Option<Vec<f32>>> {
    let mut vector = None;
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;
        match field_number {
            VECTORS_OUTPUT_VECTOR => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let vec_data = read_submessage(&mut buf)?;
                if let Some(next) = decode_vector_output(vec_data)? {
                    set_decoded_vector(&mut vector, next)?;
                }
            }
            VECTORS_OUTPUT_NAMED => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let vectors_data = read_submessage(&mut buf)?;
                if let Some(next) = decode_named_vectors_output(vectors_data)? {
                    set_decoded_vector(&mut vector, next)?;
                }
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Ok(vector)
}

fn set_decoded_vector(slot: &mut Option<Vec<f32>>, next: Vec<f32>) -> QdrantResult<()> {
    if let Some(existing) = slot
        && existing != &next
    {
        return Err(QdrantError::Decode(
            "Conflicting vector outputs in Qdrant response".to_string(),
        ));
    }
    *slot = Some(next);
    Ok(())
}

/// Decode VectorOutput.
fn decode_vector_output(data: &[u8]) -> QdrantResult<Option<Vec<f32>>> {
    let mut vector = None;
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;
        match field_number {
            VECTOR_OUTPUT_DEPRECATED_DATA => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let float_data = read_submessage(&mut buf)?;
                set_decoded_vector(&mut vector, decode_packed_f32_vector(float_data)?)?;
            }
            VECTOR_OUTPUT_DENSE => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let dense_data = read_submessage(&mut buf)?;
                if let Some(next) = decode_dense_vector(dense_data)? {
                    set_decoded_vector(&mut vector, next)?;
                }
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Ok(vector)
}

/// Decode DenseVector.data (packed repeated float).
fn decode_dense_vector(data: &[u8]) -> QdrantResult<Option<Vec<f32>>> {
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;
        match field_number {
            DENSE_VECTOR_DATA => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let float_data = read_submessage(&mut buf)?;
                return Ok(Some(decode_packed_f32_vector(float_data)?));
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Ok(None)
}

fn decode_named_vectors_output(data: &[u8]) -> QdrantResult<Option<Vec<f32>>> {
    let mut vector = None;
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;
        match field_number {
            NAMED_VECTORS_OUTPUT_ENTRY => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let entry_data = read_submessage(&mut buf)?;
                if let Some(next) = decode_named_vector_output_entry(entry_data)? {
                    if vector.is_some() {
                        return Err(QdrantError::Decode(
                            "Multiple named vectors cannot be represented as a single dense vector"
                                .to_string(),
                        ));
                    }
                    vector = Some(next);
                }
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Ok(vector)
}

fn decode_named_vector_output_entry(data: &[u8]) -> QdrantResult<Option<Vec<f32>>> {
    let mut key = None;
    let mut vector = None;
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;
        match field_number {
            NAMED_VECTOR_ENTRY_KEY => {
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for named vector key".to_string(),
                    ));
                }
                let key_data = read_submessage(&mut buf)?;
                let decoded_key = std::str::from_utf8(key_data).map_err(|e| {
                    QdrantError::Decode(format!("Invalid UTF-8 named vector key: {e}"))
                })?;
                ensure_non_empty_decoded_name(decoded_key, "Named vector key")?;
                key = Some(decoded_key.to_string());
            }
            NAMED_VECTOR_ENTRY_VALUE => {
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode(
                        "Invalid wire type for named vector value".to_string(),
                    ));
                }
                let value_data = read_submessage(&mut buf)?;
                vector = decode_vector_output(value_data)?;
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    if vector.is_some() && key.is_none() {
        return Err(QdrantError::Decode("Missing named vector key".to_string()));
    }

    Ok(vector)
}

fn decode_packed_f32_vector(float_data: &[u8]) -> QdrantResult<Vec<f32>> {
    if float_data.is_empty() {
        return Err(QdrantError::Decode("Empty vector data".to_string()));
    }
    if !float_data.len().is_multiple_of(4) {
        return Err(QdrantError::Decode(
            "Invalid vector data length".to_string(),
        ));
    }
    let count = float_data.len() / 4;
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let offset = i * 4;
        let bytes: [u8; 4] = float_data[offset..offset + 4]
            .try_into()
            .map_err(|_| QdrantError::Decode("Invalid vector data length".to_string()))?;
        let value = f32::from_le_bytes(bytes);
        if !value.is_finite() {
            return Err(QdrantError::Decode(
                "Invalid non-finite vector value".to_string(),
            ));
        }
        result.push(value);
    }
    Ok(result)
}

// ============================================================================
// PointId Decoder
// ============================================================================

/// Decode a PointId message.
fn decode_point_id(data: &[u8]) -> QdrantResult<PointId> {
    let mut buf = data;

    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;

        match field_number {
            POINT_ID_NUM => {
                if wire_type != WIRE_VARINT {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let num = decode_varint(&mut buf)?;
                return Ok(PointId::Num(num));
            }
            POINT_ID_UUID => {
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let len = decode_varint(&mut buf)? as usize;
                if buf.len() < len {
                    return Err(QdrantError::Decode("Truncated UUID".to_string()));
                }

                let uuid_str = std::str::from_utf8(&buf[..len])
                    .map_err(|e| QdrantError::Decode(format!("Invalid UTF-8: {}", e)))?;
                ensure_non_empty_decoded_name(uuid_str, "Point UUID")?;
                return Ok(PointId::Uuid(uuid_str.to_string()));
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }

    Err(QdrantError::Decode("Missing point id".to_string()))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_varint() {
        let mut buf: &[u8] = &[0x01];
        assert_eq!(decode_varint(&mut buf).unwrap(), 1);
        assert!(buf.is_empty());

        let mut buf: &[u8] = &[0xAC, 0x02];
        assert_eq!(decode_varint(&mut buf).unwrap(), 300);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_varint_rejects_u64_overflow() {
        let mut data = [0xFF; 10];
        data[9] = 0x7F;
        let mut buf: &[u8] = &data;

        let err = decode_varint(&mut buf).unwrap_err();
        assert!(err.to_string().contains("overflows u64"));
    }

    #[test]
    fn test_decode_varint_rejects_unterminated_tenth_byte() {
        let data = [0x80; 10];
        let mut buf: &[u8] = &data;

        let err = decode_varint(&mut buf).unwrap_err();
        assert!(err.to_string().contains("Varint too long"));
    }

    #[test]
    fn test_decode_tag() {
        let mut buf: &[u8] = &[0x0A];
        let (field, wire) = decode_tag(&mut buf).unwrap();
        assert_eq!(field, 1);
        assert_eq!(wire, WIRE_LEN);

        let mut buf: &[u8] = &[0x1D];
        let (field, wire) = decode_tag(&mut buf).unwrap();
        assert_eq!(field, 3);
        assert_eq!(wire, WIRE_FIXED32);
    }

    #[test]
    fn test_decode_tag_rejects_zero_field_number() {
        let mut buf: &[u8] = &[0x00];

        let err = decode_tag(&mut buf).unwrap_err();
        assert!(err.to_string().contains("Invalid protobuf field number"));
    }

    #[test]
    fn test_decode_tag_rejects_oversized_field_number() {
        let mut value = ((MAX_PROTO_FIELD_NUMBER + 1) << 3) | u64::from(WIRE_LEN);
        let mut encoded = Vec::new();
        while value >= 0x80 {
            encoded.push(((value as u8) & 0x7F) | 0x80);
            value >>= 7;
        }
        encoded.push(value as u8);

        let mut buf: &[u8] = &encoded;
        let err = decode_tag(&mut buf).unwrap_err();
        assert!(err.to_string().contains("Invalid protobuf field number"));
    }

    #[test]
    fn test_decode_point_id_num() {
        let data = &[0x08, 0x2A];
        let id = decode_point_id(data).unwrap();
        assert_eq!(id, PointId::Num(42));
    }

    #[test]
    fn test_decode_point_id_explicit_zero() {
        let data = &[0x08, 0x00];
        let id = decode_point_id(data).unwrap();
        assert_eq!(id, PointId::Num(0));
    }

    #[test]
    fn test_decode_point_id_rejects_empty_message() {
        let err = decode_point_id(&[]).unwrap_err();
        assert!(err.to_string().contains("Missing point id"));
    }

    #[test]
    fn test_decode_point_id_uuid() {
        let data = &[0x12, 0x03, b'a', b'b', b'c'];
        let id = decode_point_id(data).unwrap();
        assert_eq!(id, PointId::Uuid("abc".to_string()));
    }

    #[test]
    fn test_decode_point_id_rejects_empty_uuid() {
        let empty = &[0x12, 0x00];
        let err = decode_point_id(empty).unwrap_err();
        assert!(err.to_string().contains("Point UUID"));

        let blank = &[0x12, 0x01, b' '];
        let err = decode_point_id(blank).unwrap_err();
        assert!(err.to_string().contains("Point UUID"));
    }

    #[test]
    fn test_decode_scored_point() {
        let score_bytes = 0.5f32.to_le_bytes();
        let data = &[
            0x0A,
            0x02,
            0x08,
            0x01, // id = PointId { num = 1 }
            0x1D,
            score_bytes[0],
            score_bytes[1],
            score_bytes[2],
            score_bytes[3],
        ];

        let point = decode_scored_point(data).unwrap();
        assert_eq!(point.id, PointId::Num(1));
        assert!((point.score - 0.5).abs() < 0.0001);
    }

    fn push_len_field(out: &mut Vec<u8>, tag: u8, body: &[u8]) {
        assert!(body.len() < 128, "test helper only handles short bodies");
        out.push(tag);
        out.push(body.len() as u8);
        out.extend_from_slice(body);
    }

    fn packed_f32(values: &[f32]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(values.len() * 4);
        for value in values {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    fn current_dense_vectors_output(values: &[f32]) -> Vec<u8> {
        let floats = packed_f32(values);

        let mut dense = Vec::new();
        push_len_field(&mut dense, 0x0A, &floats);

        let mut vector_output = Vec::new();
        vector_output.extend_from_slice(&[0xAA, 0x06]);
        vector_output.push(dense.len() as u8);
        vector_output.extend_from_slice(&dense);

        let mut vectors_output = Vec::new();
        push_len_field(&mut vectors_output, 0x0A, &vector_output);
        vectors_output
    }

    fn deprecated_dense_vectors_output(values: &[f32]) -> Vec<u8> {
        let floats = packed_f32(values);

        let mut vector_output = Vec::new();
        push_len_field(&mut vector_output, 0x0A, &floats);

        let mut vectors_output = Vec::new();
        push_len_field(&mut vectors_output, 0x0A, &vector_output);
        vectors_output
    }

    fn named_dense_vectors_output(name: &str, values: &[f32]) -> Vec<u8> {
        let vectors_output = current_dense_vectors_output(values);
        let vector_output = &vectors_output[2..];

        let mut entry = Vec::new();
        push_len_field(&mut entry, 0x0A, name.as_bytes());
        push_len_field(&mut entry, 0x12, vector_output);

        let mut named = Vec::new();
        push_len_field(&mut named, 0x0A, &entry);

        let mut output = Vec::new();
        push_len_field(&mut output, 0x12, &named);
        output
    }

    fn scored_point_with_vectors(vectors_output: &[u8]) -> Vec<u8> {
        let score_bytes = 0.5f32.to_le_bytes();
        let mut data = vec![
            0x0A,
            0x02,
            0x08,
            0x01, // id = PointId { num = 1 }
            0x1D,
            score_bytes[0],
            score_bytes[1],
            score_bytes[2],
            score_bytes[3],
        ];
        push_len_field(&mut data, 0x32, vectors_output);
        data
    }

    #[test]
    fn test_decode_search_response_accepts_current_dense_vector_output() {
        let scored_point = scored_point_with_vectors(&current_dense_vectors_output(&[0.25, 0.75]));
        let mut data = Vec::new();
        push_len_field(&mut data, 0x0A, &scored_point);

        let points = decode_search_response(&data).unwrap();

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].vector, Some(vec![0.25, 0.75]));
    }

    #[test]
    fn test_decode_scored_point_accepts_deprecated_dense_vector_output() {
        let data = scored_point_with_vectors(&deprecated_dense_vectors_output(&[0.25, 0.75]));

        let point = decode_scored_point(&data).unwrap();

        assert_eq!(point.vector, Some(vec![0.25, 0.75]));
    }

    #[test]
    fn test_decode_scored_point_accepts_single_named_dense_vector_output() {
        let data = scored_point_with_vectors(&named_dense_vectors_output("image", &[0.25, 0.75]));

        let point = decode_scored_point(&data).unwrap();

        assert_eq!(point.vector, Some(vec![0.25, 0.75]));
    }

    #[test]
    fn test_decode_scored_point_rejects_multiple_named_vectors() {
        let mut first = named_dense_vectors_output("image", &[0.25, 0.75]);
        let second = named_dense_vectors_output("text", &[0.5, 0.5]);
        let second_named_body = &second[2..];
        let mut named_body = first.split_off(2);
        named_body.extend_from_slice(second_named_body);
        let mut output = Vec::new();
        push_len_field(&mut output, 0x12, &named_body);
        let data = scored_point_with_vectors(&output);

        let err = decode_scored_point(&data).unwrap_err();

        assert!(err.to_string().contains("Multiple named vectors"));
    }

    #[test]
    fn test_decode_scored_point_rejects_malformed_vector() {
        let data = &[
            0x0A, 0x02, 0x08, 0x01, // id = PointId { num = 1 }
            0x32, 0x07, // vectors message length
            0x0A, 0x05, // vector message length
            0x0A, 0x03, // packed float data length is not divisible by 4
            0x00, 0x00, 0x00,
        ];

        let err = decode_scored_point(data).unwrap_err();
        assert!(err.to_string().contains("Invalid vector data length"));
    }

    #[test]
    fn test_decode_scored_point_rejects_non_finite_score() {
        let nan = f32::NAN.to_le_bytes();
        let data = &[
            0x0A, 0x02, 0x08, 0x01, // id = PointId { num = 1 }
            0x1D, nan[0], nan[1], nan[2], nan[3],
        ];

        let err = decode_scored_point(data).unwrap_err();
        assert!(err.to_string().contains("non-finite score"));
    }

    #[test]
    fn test_decode_scored_point_rejects_non_finite_vector() {
        let nan = f32::NAN.to_le_bytes();
        let data = &[
            0x0A, 0x02, 0x08, 0x01, // id = PointId { num = 1 }
            0x32, 0x08, // vectors message length
            0x0A, 0x06, // vector message length
            0x0A, 0x04, // packed float data length
            nan[0], nan[1], nan[2], nan[3],
        ];

        let err = decode_scored_point(data).unwrap_err();
        assert!(err.to_string().contains("non-finite vector value"));
    }

    #[test]
    fn test_decode_scored_point_rejects_empty_vector_data() {
        let data = &[
            0x0A, 0x02, 0x08, 0x01, // id = PointId { num = 1 }
            0x32, 0x04, // vectors message length
            0x0A, 0x02, // vector message length
            0x0A, 0x00, // packed float data length = 0
        ];

        let err = decode_scored_point(data).unwrap_err();
        assert!(err.to_string().contains("Empty vector data"));
    }

    #[test]
    fn test_decode_search_response_rejects_point_without_id() {
        let score_bytes = 1.0f32.to_le_bytes();
        let data = &[
            0x0A,
            0x05,
            0x1D,
            score_bytes[0],
            score_bytes[1],
            score_bytes[2],
            score_bytes[3],
        ];

        let err = decode_search_response(data).unwrap_err();
        assert!(err.to_string().contains("Missing point id"));
    }

    #[test]
    fn test_decode_get_response_rejects_point_without_id() {
        let data = &[0x0A, 0x00];

        let err = decode_get_response(data).unwrap_err();
        assert!(err.to_string().contains("Missing point id"));
    }

    #[test]
    fn test_decode_scroll_response_rejects_point_without_id() {
        let data = &[0x12, 0x00];

        let err = match decode_scroll_response(data) {
            Ok(_) => panic!("scroll response without point id must fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("Missing point id"));
    }

    #[test]
    fn test_decode_scroll_response_rejects_empty_next_offset() {
        let data = &[0x0A, 0x00];

        let err = match decode_scroll_response(data) {
            Ok(_) => panic!("scroll response with empty next offset must fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("Missing point id"));
    }

    #[test]
    fn test_decode_search_response_empty() {
        let data: &[u8] = &[];
        let results = decode_search_response(data).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_decode_value_string() {
        // Value { string_value = "hello" }
        // field 4 (string_value), wire LEN: tag 0x22, len 5, "hello"
        let data = &[0x22, 0x05, b'h', b'e', b'l', b'l', b'o'];
        let val = decode_value(data).unwrap();
        assert_eq!(val, PayloadValue::String("hello".to_string()));
    }

    #[test]
    fn test_decode_value_integer() {
        // Value { integer_value = 42 }
        // field 3 (integer_value), wire VARINT: tag 0x18, value 42
        let data = &[0x18, 0x2A];
        let val = decode_value(data).unwrap();
        assert_eq!(val, PayloadValue::Integer(42));
    }

    #[test]
    fn test_decode_value_bool() {
        // Value { bool_value = true }
        // field 5, wire VARINT: tag 0x28, value 1
        let data = &[0x28, 0x01];
        let val = decode_value(data).unwrap();
        assert_eq!(val, PayloadValue::Bool(true));
    }

    #[test]
    fn test_decode_value_rejects_malformed_bool_varint() {
        let data = &[0x28, 0x02];

        let err = decode_value(data).unwrap_err();
        assert!(err.to_string().contains("Invalid payload bool value"));
    }

    #[test]
    fn test_decode_value_double() {
        // Value { double_value = 3.14 }
        // field 2, wire FIXED64: tag 0x11
        let f_bytes = std::f64::consts::PI.to_le_bytes();
        let mut data = vec![0x11];
        data.extend_from_slice(&f_bytes);
        let val = decode_value(&data).unwrap();
        match val {
            PayloadValue::Float(f) => assert!((f - std::f64::consts::PI).abs() < 0.001),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_decode_value_null() {
        // Value { null_value = 0 }
        // field 1, wire VARINT: tag 0x08, value 0
        let data = &[0x08, 0x00];
        let val = decode_value(data).unwrap();
        assert_eq!(val, PayloadValue::Null);
    }

    #[test]
    fn test_decode_value_rejects_malformed_null_enum() {
        let data = &[0x08, 0x01];

        let err = decode_value(data).unwrap_err();
        assert!(err.to_string().contains("Invalid payload null enum value"));
    }

    #[test]
    fn test_decode_scored_point_rejects_invalid_payload_key_utf8() {
        let data = &[
            0x0A, 0x02, 0x08, 0x01, // id = PointId { num = 1 }
            0x12, 0x09, // payload map entry length
            0x0A, 0x01, 0xFF, // key = invalid UTF-8
            0x12, 0x04, // value message length
            0x22, 0x02, b'o', b'k', // string_value = "ok"
        ];

        let err = decode_scored_point(data).unwrap_err();
        assert!(err.to_string().contains("Invalid UTF-8 payload map key"));
    }

    #[test]
    fn test_decode_scored_point_rejects_empty_payload_key() {
        let data = &[
            0x0A, 0x02, 0x08, 0x01, // id = PointId { num = 1 }
            0x12, 0x08, // payload map entry length
            0x0A, 0x00, // key = ""
            0x12, 0x04, // value message length
            0x22, 0x02, b'o', b'k', // string_value = "ok"
        ];

        let err = decode_scored_point(data).unwrap_err();
        assert!(err.to_string().contains("Payload map key"));
    }

    #[test]
    fn test_decode_scored_point_rejects_payload_entry_without_value() {
        let data = &[
            0x0A, 0x02, 0x08, 0x01, // id = PointId { num = 1 }
            0x12, 0x05, // payload map entry length
            0x0A, 0x03, b'b', b'a', b'd', // key = "bad", missing value
        ];

        let err = decode_scored_point(data).unwrap_err();
        assert!(err.to_string().contains("Missing payload map value"));
    }

    #[test]
    fn test_decode_value_rejects_non_finite_payload_float() {
        let nan = f64::NAN.to_le_bytes();
        let mut data = vec![0x11];
        data.extend_from_slice(&nan);

        let err = decode_value(&data).unwrap_err();
        assert!(err.to_string().contains("non-finite payload float"));
    }

    #[test]
    fn test_decode_value_rejects_malformed_nested_object_entry() {
        let data = &[
            0x32, 0x07, // struct_value length
            0x0A, 0x05, // Struct.fields map entry length
            0x0A, 0x03, b'b', b'a', b'd', // key = "bad", missing value
        ];

        let err = decode_value(data).unwrap_err();
        assert!(err.to_string().contains("Missing payload map value"));
    }

    #[test]
    fn test_decode_value_rejects_empty_nested_object_key() {
        let data = &[
            0x32, 0x0A, // struct_value length
            0x0A, 0x08, // Struct.fields map entry length
            0x0A, 0x00, // key = ""
            0x12, 0x04, // value message length
            0x22, 0x02, b'o', b'k', // string_value = "ok"
        ];

        let err = decode_value(data).unwrap_err();
        assert!(err.to_string().contains("Payload map key"));
    }

    #[test]
    fn test_decode_value_rejects_malformed_nested_list_item() {
        let data = &[
            0x3A, 0x02, // list_value length
            0x0A, 0x00, // ListValue.values has an empty Value message
        ];

        let err = decode_value(data).unwrap_err();
        assert!(err.to_string().contains("Missing payload value kind"));
    }

    #[test]
    fn test_decode_scroll_result_empty() {
        let data: &[u8] = &[];
        let result = decode_scroll_response(data).unwrap();
        assert!(result.points.is_empty());
        assert!(result.next_offset.is_none());
    }
}
