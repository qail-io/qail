//! Zero-copy Protobuf encoder for Qdrant gRPC protocol.
//!
//! This module implements direct wire format encoding without intermediate
//! struct allocations. Key optimizations:
//! - Pre-computed field tag bytes
//! - Buffer reuse via BytesMut
//! - Direct memcpy for vectors (no per-element loop)
//!
//! ## Supported Operations
//! - Search (with filters)
//! - Upsert (with payload)
//! - Delete points (numeric + UUID)
//! - Get points by ID
//! - Scroll (paginated iteration)
//! - Create / Delete collection
//! - Update payload
//! - Create field index

use crate::error::{QdrantError, QdrantResult};
use bytes::{BufMut, BytesMut};

// ============================================================================
// Protobuf Wire Type Constants
// ============================================================================

/// Wire type for varints (int32, int64, uint32, uint64, bool, enum)
#[allow(dead_code)]
const WIRE_VARINT: u8 = 0;
/// Wire type for length-delimited (string, bytes, embedded messages, packed repeated)
#[allow(dead_code)]
const WIRE_LEN: u8 = 2;
/// Wire type for 32-bit fixed (float, fixed32)
#[allow(dead_code)]
const WIRE_FIXED32: u8 = 5;

// ============================================================================
// SearchPoints Field Tags (pre-computed)
// ============================================================================
// Tag = (field_number << 3) | wire_type

/// Field 1: collection_name (string) -> (1 << 3) | 2 = 0x0A
const SEARCH_COLLECTION: u8 = 0x0A;
/// Field 2: vector (repeated float, packed) -> (2 << 3) | 2 = 0x12
const SEARCH_VECTOR: u8 = 0x12;
/// Field 3: filter (message) -> (3 << 3) | 2 = 0x1A
const SEARCH_FILTER: u8 = 0x1A;
/// Field 4: limit (uint64) -> (4 << 3) | 0 = 0x20
const SEARCH_LIMIT: u8 = 0x20;
/// Field 6: with_payload (message) -> (6 << 3) | 2 = 0x32
const SEARCH_WITH_PAYLOAD: u8 = 0x32;
/// Field 8: score_threshold (float) -> (8 << 3) | 5 = 0x45
const SEARCH_SCORE_THRESHOLD: u8 = 0x45;
/// Field 10: vector_name (string) -> (10 << 3) | 2 = 0x52
const SEARCH_VECTOR_NAME: u8 = 0x52;

// ============================================================================
// UpsertPoints Field Tags
// ============================================================================

/// Field 1: collection_name (string) -> 0x0A
const UPSERT_COLLECTION: u8 = 0x0A;
/// Field 2: wait (bool) -> (2 << 3) | 0 = 0x10
const UPSERT_WAIT: u8 = 0x10;
/// Field 3: points (repeated PointStruct) -> (3 << 3) | 2 = 0x1A
const UPSERT_POINTS: u8 = 0x1A;

// ============================================================================
// PointStruct Field Tags
// ============================================================================

/// Field 1: id (PointId) -> 0x0A
const POINT_ID: u8 = 0x0A;
/// Field 4: vectors (Vectors) -> (4 << 3) | 2 = 0x22 (field 2 is deprecated)
const POINT_VECTORS: u8 = 0x22;
/// Field 3: payload (map) -> (3 << 3) | 2 = 0x1A
const POINT_PAYLOAD: u8 = 0x1A;

// ============================================================================
// PointId Field Tags
// ============================================================================

/// Field 1: num (uint64) -> (1 << 3) | 0 = 0x08
const POINT_ID_NUM: u8 = 0x08;
/// Field 2: uuid (string) -> (2 << 3) | 2 = 0x12
const POINT_ID_UUID: u8 = 0x12;

// ============================================================================
// Filter Field Tags (qdrant.Filter message)
// ============================================================================

/// Filter.must (field 1, repeated Condition) -> 0x0A
const FILTER_MUST: u8 = 0x0A;
/// Filter.should (field 2, repeated Condition) -> 0x12
const FILTER_SHOULD: u8 = 0x12;
/// Filter.must_not (field 3, repeated Condition) -> 0x1A
#[allow(dead_code)]
const FILTER_MUST_NOT: u8 = 0x1A;
/// Condition.filter (field 4, nested Filter message) -> 0x22
const CONDITION_FILTER: u8 = 0x22;

// ============================================================================
// Varint Encoding
// ============================================================================

/// Encode a varint (variable-length integer) into the buffer.
/// Uses 7 bits per byte, MSB indicates continuation.
#[inline]
pub fn encode_varint(buf: &mut BytesMut, mut value: usize) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.put_u8(byte);
            break;
        } else {
            buf.put_u8(byte | 0x80);
        }
    }
}

/// Encode a u64 varint.
#[inline]
pub fn encode_varint_u64(buf: &mut BytesMut, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.put_u8(byte);
            break;
        } else {
            buf.put_u8(byte | 0x80);
        }
    }
}

// ============================================================================
// SearchPoints Encoder
// ============================================================================

/// Common search request fields shared by all search encoders.
#[derive(Clone, Copy)]
pub struct SearchRequest<'a> {
    pub collection: &'a str,
    pub vector: &'a [f32],
    pub limit: u64,
    pub score_threshold: Option<f32>,
    pub vector_name: Option<&'a str>,
}

/// Encode a SearchPoints request directly to protobuf wire format.
///
/// # Arguments
/// * `buf` - Reusable buffer (cleared before writing)
/// * `collection` - Collection name
/// * `vector` - Query vector (directly memcpy'd)
/// * `limit` - Max results
/// * `score_threshold` - Optional minimum score
/// * `vector_name` - Optional named vector field
///
/// # Zero-Copy Optimization
/// The vector is written via direct memory copy, avoiding per-element encoding.
pub fn encode_search_proto(
    buf: &mut BytesMut,
    collection: &str,
    vector: &[f32],
    limit: u64,
    score_threshold: Option<f32>,
    vector_name: Option<&str>,
) {
    buf.clear();

    // Field 1: collection_name (string)
    buf.put_u8(SEARCH_COLLECTION);
    encode_varint(buf, collection.len());
    buf.extend_from_slice(collection.as_bytes());

    // Field 2: vector (packed repeated float)
    // This is the key optimization - direct memcpy of float bytes!
    buf.put_u8(SEARCH_VECTOR);
    let vector_bytes_len = vector.len() * 4; // f32 = 4 bytes
    encode_varint(buf, vector_bytes_len);

    // ZERO-COPY: Write floats directly as bytes via bytemuck (safe, zero-cost)
    let float_bytes: &[u8] = bytemuck::cast_slice(vector);
    buf.extend_from_slice(float_bytes);

    // Field 4: limit (varint)
    buf.put_u8(SEARCH_LIMIT);
    encode_varint_u64(buf, limit);

    // Field 6: with_payload = true
    encode_with_payload_true(buf);

    // Field 8: score_threshold (float, optional)
    if let Some(threshold) = score_threshold {
        buf.put_u8(SEARCH_SCORE_THRESHOLD);
        buf.put_f32_le(threshold);
    }

    // Field 10: vector_name (string, optional)
    if let Some(name) = vector_name {
        buf.put_u8(SEARCH_VECTOR_NAME);
        encode_varint(buf, name.len());
        buf.extend_from_slice(name.as_bytes());
    }
}

/// Encode a SearchPoints request with QAIL AST filter conditions.
///
/// This is the filtered search path — translates QAIL conditions into
/// Qdrant's protobuf Filter message (must/should arrays of Condition).
pub fn encode_search_with_filter_proto(
    buf: &mut BytesMut,
    request: SearchRequest<'_>,
    conditions: &[qail_core::ast::Condition],
    is_or: bool,
) -> QdrantResult<()> {
    let (must_conditions, should_conditions): (
        &[qail_core::ast::Condition],
        &[qail_core::ast::Condition],
    ) = if is_or {
        (&[], conditions)
    } else {
        (conditions, &[])
    };

    encode_search_with_filter_groups_proto(buf, request, must_conditions, should_conditions)
}

/// Encode a SearchPoints request with grouped filter conditions.
///
/// `must_conditions` are combined as AND, `should_conditions` as OR.
pub fn encode_search_with_filter_groups_proto(
    buf: &mut BytesMut,
    request: SearchRequest<'_>,
    must_conditions: &[qail_core::ast::Condition],
    should_conditions: &[qail_core::ast::Condition],
) -> QdrantResult<()> {
    buf.clear();

    // Field 1: collection_name
    buf.put_u8(SEARCH_COLLECTION);
    encode_varint(buf, request.collection.len());
    buf.extend_from_slice(request.collection.as_bytes());

    // Field 2: vector (packed floats, zero-copy)
    buf.put_u8(SEARCH_VECTOR);
    let vector_bytes_len = request.vector.len() * 4;
    encode_varint(buf, vector_bytes_len);
    let float_bytes: &[u8] = bytemuck::cast_slice(request.vector);
    buf.extend_from_slice(float_bytes);

    // Field 3: filter (Filter message)
    if !must_conditions.is_empty() || !should_conditions.is_empty() {
        let filter_buf = encode_filter_message_grouped(must_conditions, should_conditions)?;
        buf.put_u8(SEARCH_FILTER);
        encode_varint(buf, filter_buf.len());
        buf.extend_from_slice(&filter_buf);
    }

    // Field 4: limit
    buf.put_u8(SEARCH_LIMIT);
    encode_varint_u64(buf, request.limit);

    // Field 6: with_payload = true
    encode_with_payload_true(buf);

    // Field 8: score_threshold
    if let Some(threshold) = request.score_threshold {
        buf.put_u8(SEARCH_SCORE_THRESHOLD);
        buf.put_f32_le(threshold);
    }

    // Field 10: vector_name
    if let Some(name) = request.vector_name {
        buf.put_u8(SEARCH_VECTOR_NAME);
        encode_varint(buf, name.len());
        buf.extend_from_slice(name.as_bytes());
    }

    Ok(())
}

/// Encode a SearchPoints request where OR conditions are preserved per-cage.
///
/// Every OR cage is encoded as a nested `Filter { should: [...] }` wrapped
/// in the outer filter's `must`, preserving:
/// `(A OR B) AND (C OR D)` instead of flattening to `A OR B OR C OR D`.
pub fn encode_search_with_filter_grouped_cages_proto(
    buf: &mut BytesMut,
    request: SearchRequest<'_>,
    must_conditions: &[qail_core::ast::Condition],
    should_groups: &[Vec<qail_core::ast::Condition>],
) -> QdrantResult<()> {
    buf.clear();

    // Field 1: collection_name
    buf.put_u8(SEARCH_COLLECTION);
    encode_varint(buf, request.collection.len());
    buf.extend_from_slice(request.collection.as_bytes());

    // Field 2: vector (packed floats, zero-copy)
    buf.put_u8(SEARCH_VECTOR);
    let vector_bytes_len = request.vector.len() * 4;
    encode_varint(buf, vector_bytes_len);
    let float_bytes: &[u8] = bytemuck::cast_slice(request.vector);
    buf.extend_from_slice(float_bytes);

    // Field 3: filter (Filter message)
    if !must_conditions.is_empty() || !should_groups.is_empty() {
        let filter_buf = encode_filter_message_grouped_cages(must_conditions, should_groups)?;
        buf.put_u8(SEARCH_FILTER);
        encode_varint(buf, filter_buf.len());
        buf.extend_from_slice(&filter_buf);
    }

    // Field 4: limit
    buf.put_u8(SEARCH_LIMIT);
    encode_varint_u64(buf, request.limit);

    // Field 6: with_payload = true
    encode_with_payload_true(buf);

    // Field 8: score_threshold
    if let Some(threshold) = request.score_threshold {
        buf.put_u8(SEARCH_SCORE_THRESHOLD);
        buf.put_f32_le(threshold);
    }

    // Field 10: vector_name
    if let Some(name) = request.vector_name {
        buf.put_u8(SEARCH_VECTOR_NAME);
        encode_varint(buf, name.len());
        buf.extend_from_slice(name.as_bytes());
    }

    Ok(())
}

/// Encode with_payload = true as a sub-message.
pub fn encode_with_payload_true(buf: &mut BytesMut) {
    // WithPayloadSelector { enable = true }
    // Field 1: enable (bool) = 0x08, value = 1
    buf.put_u8(SEARCH_WITH_PAYLOAD);
    encode_varint(buf, 2); // submessage length
    buf.put_u8(0x08); // field 1, varint
    buf.put_u8(0x01); // true
}

// ============================================================================
// Filter Encoder (QAIL Conditions → Qdrant Protobuf Filter)
// ============================================================================

/// Encode a Filter message from QAIL AST conditions.
///
/// Qdrant's Filter proto:
/// ```text
/// message Filter {
///   repeated Condition must = 1;
///   repeated Condition should = 2;
///   repeated Condition must_not = 3;
/// }
/// ```
///
/// Each Condition wraps a FieldCondition:
/// ```text
/// message Condition {
///   oneof condition_one_of {
///     FieldCondition field = 1;
///     IsEmptyCondition is_empty = 2;
///     HasIdCondition has_id = 3;
///     IsNullCondition is_null = 5;
///     Filter filter = 4;     // nested filters
///   }
/// }
/// ```
fn encode_filter_message_grouped(
    must_conditions: &[qail_core::ast::Condition],
    should_conditions: &[qail_core::ast::Condition],
) -> QdrantResult<BytesMut> {
    let mut filter_buf =
        BytesMut::with_capacity((must_conditions.len() + should_conditions.len()) * 32);

    let mut encode_clause =
        |conditions: &[qail_core::ast::Condition], clause_tag: u8| -> QdrantResult<()> {
            for cond in conditions {
                let cond_buf = encode_condition_message(cond)?;

                // Write as repeated Condition in the filter's must/should field
                filter_buf.put_u8(clause_tag);
                encode_varint(&mut filter_buf, cond_buf.len());
                filter_buf.extend_from_slice(&cond_buf);
            }
            Ok(())
        };

    encode_clause(must_conditions, FILTER_MUST)?;
    encode_clause(should_conditions, FILTER_SHOULD)?;

    Ok(filter_buf)
}

/// Encode a grouped filter preserving each OR cage as its own nested should-group.
fn encode_filter_message_grouped_cages(
    must_conditions: &[qail_core::ast::Condition],
    should_groups: &[Vec<qail_core::ast::Condition>],
) -> QdrantResult<BytesMut> {
    let grouped_condition_count: usize = should_groups.iter().map(Vec::len).sum();
    let mut filter_buf =
        BytesMut::with_capacity((must_conditions.len() + grouped_condition_count) * 32);

    for cond in must_conditions {
        let cond_buf = encode_condition_message(cond)?;
        filter_buf.put_u8(FILTER_MUST);
        encode_varint(&mut filter_buf, cond_buf.len());
        filter_buf.extend_from_slice(&cond_buf);
    }

    for group in should_groups {
        if group.is_empty() {
            continue;
        }

        if group.len() == 1 {
            let cond_buf = encode_condition_message(&group[0])?;
            filter_buf.put_u8(FILTER_MUST);
            encode_varint(&mut filter_buf, cond_buf.len());
            filter_buf.extend_from_slice(&cond_buf);
            continue;
        }

        let nested_filter = encode_filter_message_grouped(&[], group)?;
        let mut nested_condition = BytesMut::with_capacity(nested_filter.len() + 4);
        nested_condition.put_u8(CONDITION_FILTER);
        encode_varint(&mut nested_condition, nested_filter.len());
        nested_condition.extend_from_slice(&nested_filter);

        filter_buf.put_u8(FILTER_MUST);
        encode_varint(&mut filter_buf, nested_condition.len());
        filter_buf.extend_from_slice(&nested_condition);
    }

    Ok(filter_buf)
}

/// Encode a single `Condition` message for Qdrant filters.
fn encode_condition_message(cond: &qail_core::ast::Condition) -> QdrantResult<BytesMut> {
    use qail_core::ast::{Expr, Operator, Value};

    let key = match &cond.left {
        Expr::Named(name) => name.as_str(),
        Expr::Aliased { name, .. } => name.as_str(),
        other => {
            return Err(QdrantError::Encode(format!(
                "Unsupported filter left expression for Qdrant: {:?}",
                other
            )));
        }
    };

    match (&cond.op, &cond.value) {
        // Match (equality) conditions
        (Operator::Eq, Value::String(s)) => Ok(encode_field_condition_match_keyword(key, s)),
        (Operator::Eq, Value::Int(n)) => Ok(encode_field_condition_match_integer(key, *n)),
        (Operator::Eq, Value::Float(f)) => Ok(encode_field_condition_match_float(key, *f)),
        (Operator::Eq, Value::Bool(b)) => Ok(encode_field_condition_match_bool(key, *b)),

        // Range conditions
        (Operator::Gt, Value::Int(n)) => Ok(encode_field_condition_range(
            key,
            None,
            None,
            Some(*n as f64),
            None,
        )),
        (Operator::Gt, Value::Float(f)) => Ok(encode_field_condition_range(
            key,
            None,
            None,
            Some(*f),
            None,
        )),
        (Operator::Gte, Value::Int(n)) => Ok(encode_field_condition_range(
            key,
            None,
            None,
            None,
            Some(*n as f64),
        )),
        (Operator::Gte, Value::Float(f)) => Ok(encode_field_condition_range(
            key,
            None,
            None,
            None,
            Some(*f),
        )),
        (Operator::Lt, Value::Int(n)) => Ok(encode_field_condition_range(
            key,
            Some(*n as f64),
            None,
            None,
            None,
        )),
        (Operator::Lt, Value::Float(f)) => Ok(encode_field_condition_range(
            key,
            Some(*f),
            None,
            None,
            None,
        )),
        (Operator::Lte, Value::Int(n)) => Ok(encode_field_condition_range(
            key,
            None,
            Some(*n as f64),
            None,
            None,
        )),
        (Operator::Lte, Value::Float(f)) => Ok(encode_field_condition_range(
            key,
            None,
            Some(*f),
            None,
            None,
        )),

        // Text match (contains / like)
        (Operator::Contains | Operator::Like, Value::String(s)) => {
            Ok(encode_field_condition_match_text(key, s))
        }

        _ => Err(QdrantError::Encode(format!(
            "Unsupported Qdrant filter condition: op={:?}, value={:?}",
            cond.op, cond.value
        ))),
    }
}

/// Encode a FieldCondition with Match { keyword } for string equality.
///
/// ```text
/// Condition {
///   field = FieldCondition {
///     key = "field_name",
///     match = Match { keyword = "value" }
///   }
/// }
/// ```
fn encode_field_condition_match_keyword(key: &str, value: &str) -> BytesMut {
    // Match message: field 1 = keyword (string)
    let mut match_buf = BytesMut::with_capacity(value.len() + 8);
    match_buf.put_u8(0x0A); // field 1 (keyword), wire LEN
    encode_varint(&mut match_buf, value.len());
    match_buf.extend_from_slice(value.as_bytes());

    // FieldCondition message
    let mut fc_buf = BytesMut::with_capacity(key.len() + match_buf.len() + 16);
    // field 1: key (string)
    fc_buf.put_u8(0x0A);
    encode_varint(&mut fc_buf, key.len());
    fc_buf.extend_from_slice(key.as_bytes());
    // field 2: match (Match message)
    fc_buf.put_u8(0x12);
    encode_varint(&mut fc_buf, match_buf.len());
    fc_buf.extend_from_slice(&match_buf);

    // Condition: field 1 = FieldCondition
    let mut cond_buf = BytesMut::with_capacity(fc_buf.len() + 4);
    cond_buf.put_u8(0x0A); // field 1
    encode_varint(&mut cond_buf, fc_buf.len());
    cond_buf.extend_from_slice(&fc_buf);

    cond_buf
}

/// Encode a FieldCondition with Match { integer } for int equality.
fn encode_field_condition_match_integer(key: &str, value: i64) -> BytesMut {
    // Match message: field 2 = integer (int64)
    let mut match_buf = BytesMut::with_capacity(16);
    match_buf.put_u8(0x10); // field 2 (integer), wire VARINT
    encode_varint_u64(&mut match_buf, value as u64);

    let mut fc_buf = BytesMut::with_capacity(key.len() + match_buf.len() + 16);
    fc_buf.put_u8(0x0A);
    encode_varint(&mut fc_buf, key.len());
    fc_buf.extend_from_slice(key.as_bytes());
    fc_buf.put_u8(0x12);
    encode_varint(&mut fc_buf, match_buf.len());
    fc_buf.extend_from_slice(&match_buf);

    let mut cond_buf = BytesMut::with_capacity(fc_buf.len() + 4);
    cond_buf.put_u8(0x0A);
    encode_varint(&mut cond_buf, fc_buf.len());
    cond_buf.extend_from_slice(&fc_buf);

    cond_buf
}

/// Encode a FieldCondition with Match { integer } using float payload.
fn encode_field_condition_match_float(key: &str, value: f64) -> BytesMut {
    // Match message: field 5 = double (double)
    let mut match_buf = BytesMut::with_capacity(10);
    match_buf.put_u8(0x29); // field 5 (double), wire FIXED64
    match_buf.put_f64_le(value);

    let mut fc_buf = BytesMut::with_capacity(key.len() + match_buf.len() + 16);
    fc_buf.put_u8(0x0A);
    encode_varint(&mut fc_buf, key.len());
    fc_buf.extend_from_slice(key.as_bytes());
    fc_buf.put_u8(0x12);
    encode_varint(&mut fc_buf, match_buf.len());
    fc_buf.extend_from_slice(&match_buf);

    let mut cond_buf = BytesMut::with_capacity(fc_buf.len() + 4);
    cond_buf.put_u8(0x0A);
    encode_varint(&mut cond_buf, fc_buf.len());
    cond_buf.extend_from_slice(&fc_buf);

    cond_buf
}

/// Encode a FieldCondition with Match { boolean }.
fn encode_field_condition_match_bool(key: &str, value: bool) -> BytesMut {
    // Match message: field 4 = boolean (bool)
    let mut match_buf = BytesMut::with_capacity(4);
    match_buf.put_u8(0x20); // field 4 (boolean), wire VARINT
    match_buf.put_u8(if value { 1 } else { 0 });

    let mut fc_buf = BytesMut::with_capacity(key.len() + match_buf.len() + 16);
    fc_buf.put_u8(0x0A);
    encode_varint(&mut fc_buf, key.len());
    fc_buf.extend_from_slice(key.as_bytes());
    fc_buf.put_u8(0x12);
    encode_varint(&mut fc_buf, match_buf.len());
    fc_buf.extend_from_slice(&match_buf);

    let mut cond_buf = BytesMut::with_capacity(fc_buf.len() + 4);
    cond_buf.put_u8(0x0A);
    encode_varint(&mut cond_buf, fc_buf.len());
    cond_buf.extend_from_slice(&fc_buf);

    cond_buf
}

/// Encode a FieldCondition with Match { text } for full-text search.
fn encode_field_condition_match_text(key: &str, value: &str) -> BytesMut {
    // Match message: field 3 = text (string)
    let mut match_buf = BytesMut::with_capacity(value.len() + 8);
    match_buf.put_u8(0x1A); // field 3 (text), wire LEN
    encode_varint(&mut match_buf, value.len());
    match_buf.extend_from_slice(value.as_bytes());

    let mut fc_buf = BytesMut::with_capacity(key.len() + match_buf.len() + 16);
    fc_buf.put_u8(0x0A);
    encode_varint(&mut fc_buf, key.len());
    fc_buf.extend_from_slice(key.as_bytes());
    fc_buf.put_u8(0x12);
    encode_varint(&mut fc_buf, match_buf.len());
    fc_buf.extend_from_slice(&match_buf);

    let mut cond_buf = BytesMut::with_capacity(fc_buf.len() + 4);
    cond_buf.put_u8(0x0A);
    encode_varint(&mut cond_buf, fc_buf.len());
    cond_buf.extend_from_slice(&fc_buf);

    cond_buf
}

/// Encode a FieldCondition with Range.
///
/// Range proto: { lt, gt, gte, lte } — all optional f64.
fn encode_field_condition_range(
    key: &str,
    lt: Option<f64>,
    lte: Option<f64>,
    gt: Option<f64>,
    gte: Option<f64>,
) -> BytesMut {
    // Range message fields (all double / f64)
    // field 1: lt  -> 0x09 (field 1, wire FIXED64)
    // field 2: gt  -> 0x11 (field 2, wire FIXED64)
    // field 3: gte -> 0x19 (field 3, wire FIXED64)
    // field 4: lte -> 0x21 (field 4, wire FIXED64)
    let mut range_buf = BytesMut::with_capacity(40);
    if let Some(v) = lt {
        range_buf.put_u8(0x09); // field 1, wire 1 (64-bit)
        range_buf.put_f64_le(v);
    }
    if let Some(v) = gt {
        range_buf.put_u8(0x11); // field 2, wire 1
        range_buf.put_f64_le(v);
    }
    if let Some(v) = gte {
        range_buf.put_u8(0x19); // field 3, wire 1
        range_buf.put_f64_le(v);
    }
    if let Some(v) = lte {
        range_buf.put_u8(0x21); // field 4, wire 1
        range_buf.put_f64_le(v);
    }

    // FieldCondition: key + range
    let mut fc_buf = BytesMut::with_capacity(key.len() + range_buf.len() + 16);
    fc_buf.put_u8(0x0A); // field 1: key
    encode_varint(&mut fc_buf, key.len());
    fc_buf.extend_from_slice(key.as_bytes());
    fc_buf.put_u8(0x1A); // field 3: range (Range message)
    encode_varint(&mut fc_buf, range_buf.len());
    fc_buf.extend_from_slice(&range_buf);

    // Condition: field 1 = FieldCondition
    let mut cond_buf = BytesMut::with_capacity(fc_buf.len() + 4);
    cond_buf.put_u8(0x0A);
    encode_varint(&mut cond_buf, fc_buf.len());
    cond_buf.extend_from_slice(&fc_buf);

    cond_buf
}

// ============================================================================
// UpsertPoints Encoder
// ============================================================================

/// Encode an UpsertPoints request to protobuf wire format.
pub fn encode_upsert_proto(
    buf: &mut BytesMut,
    collection: &str,
    points: &[crate::Point],
    wait: bool,
) {
    buf.clear();

    // Field 1: collection_name
    buf.put_u8(UPSERT_COLLECTION);
    encode_varint(buf, collection.len());
    buf.extend_from_slice(collection.as_bytes());

    // Field 2: wait (bool)
    if wait {
        buf.put_u8(UPSERT_WAIT);
        buf.put_u8(0x01);
    }

    // Field 3: points (repeated PointStruct)
    for point in points {
        encode_point_struct(buf, point);
    }
}

/// Encode a single PointStruct (with payload support).
fn encode_point_struct(buf: &mut BytesMut, point: &crate::Point) {
    // We need to encode into a temp buffer first to get length,
    // since PointStruct is length-delimited
    let mut point_buf = BytesMut::with_capacity(point.vector.len() * 4 + 64);

    // Field 1: id (PointId oneof)
    encode_point_id_field(&mut point_buf, &point.id);

    // Field 3: payload (map<string, Value>)
    if !point.payload.is_empty() {
        encode_payload_map(&mut point_buf, &point.payload);
    }

    // Field 4: vectors (Vectors -> Vector)
    let vector_bytes_len = point.vector.len() * 4;
    let vector_inner_len = 1 + varint_len(vector_bytes_len as u64) + vector_bytes_len;
    let vectors_len = 1 + varint_len(vector_inner_len as u64) + vector_inner_len;

    point_buf.put_u8(POINT_VECTORS);
    encode_varint(&mut point_buf, vectors_len);
    point_buf.put_u8(0x0A); // Vectors.vector (field 1)
    encode_varint(&mut point_buf, vector_inner_len);
    point_buf.put_u8(0x0A); // Vector.data (field 1, packed floats)
    encode_varint(&mut point_buf, vector_bytes_len);
    let float_bytes: &[u8] = bytemuck::cast_slice(&point.vector);
    point_buf.extend_from_slice(float_bytes);

    // Write to main buffer with length prefix
    buf.put_u8(UPSERT_POINTS);
    encode_varint(buf, point_buf.len());
    buf.extend_from_slice(&point_buf);
}

/// Encode a PointId into a buffer as field 1 of PointStruct.
fn encode_point_id_field(buf: &mut BytesMut, id: &crate::PointId) {
    match id {
        crate::PointId::Num(n) => {
            buf.put_u8(POINT_ID);
            let id_len = 1 + varint_len(*n);
            encode_varint(buf, id_len);
            buf.put_u8(POINT_ID_NUM);
            encode_varint_u64(buf, *n);
        }
        crate::PointId::Uuid(s) => {
            buf.put_u8(POINT_ID);
            let id_len = 1 + varint_len(s.len() as u64) + s.len();
            encode_varint(buf, id_len);
            buf.put_u8(POINT_ID_UUID);
            encode_varint(buf, s.len());
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

/// Encode a payload map as protobuf map<string, Value>.
///
/// Protobuf maps are encoded as repeated field with key-value pair messages.
fn encode_payload_map(buf: &mut BytesMut, payload: &crate::point::Payload) {
    for (key, value) in payload {
        let mut entry_buf = BytesMut::with_capacity(key.len() + 32);

        // Map entry field 1: key (string)
        entry_buf.put_u8(0x0A);
        encode_varint(&mut entry_buf, key.len());
        entry_buf.extend_from_slice(key.as_bytes());

        // Map entry field 2: value (Value message)
        let value_buf = encode_payload_value(value);
        entry_buf.put_u8(0x12);
        encode_varint(&mut entry_buf, value_buf.len());
        entry_buf.extend_from_slice(&value_buf);

        // Write map entry as field 3 of PointStruct (payload)
        buf.put_u8(POINT_PAYLOAD);
        encode_varint(buf, entry_buf.len());
        buf.extend_from_slice(&entry_buf);
    }
}

/// Encode a single PayloadValue to protobuf Value message.
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
fn encode_payload_value(value: &crate::point::PayloadValue) -> BytesMut {
    use crate::point::PayloadValue;
    let mut buf = BytesMut::with_capacity(32);

    match value {
        PayloadValue::Null => {
            // field 1: null_value (enum, always 0)
            buf.put_u8(0x08);
            buf.put_u8(0x00);
        }
        PayloadValue::Float(f) => {
            // field 2: double_value (double, wire type 1 = fixed64)
            buf.put_u8(0x11); // (2 << 3) | 1 = 0x11
            buf.put_f64_le(*f);
        }
        PayloadValue::Integer(n) => {
            // field 3: integer_value (int64, wire type 0 = varint)
            buf.put_u8(0x18); // (3 << 3) | 0 = 0x18
            encode_varint_u64(&mut buf, *n as u64);
        }
        PayloadValue::String(s) => {
            // field 4: string_value (string, wire type 2 = len-delimited)
            buf.put_u8(0x22); // (4 << 3) | 2 = 0x22
            encode_varint(&mut buf, s.len());
            buf.extend_from_slice(s.as_bytes());
        }
        PayloadValue::Bool(b) => {
            // field 5: bool_value (bool, wire type 0 = varint)
            buf.put_u8(0x28); // (5 << 3) | 0 = 0x28
            buf.put_u8(if *b { 1 } else { 0 });
        }
        PayloadValue::List(items) => {
            // field 7: list_value (ListValue message)
            let mut list_buf = BytesMut::with_capacity(items.len() * 16);
            for item in items {
                let val_buf = encode_payload_value(item);
                // ListValue.values (field 1, repeated Value)
                list_buf.put_u8(0x0A);
                encode_varint(&mut list_buf, val_buf.len());
                list_buf.extend_from_slice(&val_buf);
            }
            buf.put_u8(0x3A); // (7 << 3) | 2 = 0x3A
            encode_varint(&mut buf, list_buf.len());
            buf.extend_from_slice(&list_buf);
        }
        PayloadValue::Object(map) => {
            // field 6: struct_value (Struct message)
            // Struct.fields is map<string, Value> → repeated MapEntry
            let mut struct_buf = BytesMut::with_capacity(map.len() * 32);
            for (k, v) in map {
                let val_buf = encode_payload_value(v);
                let mut entry_buf = BytesMut::with_capacity(k.len() + val_buf.len() + 8);
                // key (field 1)
                entry_buf.put_u8(0x0A);
                encode_varint(&mut entry_buf, k.len());
                entry_buf.extend_from_slice(k.as_bytes());
                // value (field 2)
                entry_buf.put_u8(0x12);
                encode_varint(&mut entry_buf, val_buf.len());
                entry_buf.extend_from_slice(&val_buf);
                // Struct.fields (field 1, repeated)
                struct_buf.put_u8(0x0A);
                encode_varint(&mut struct_buf, entry_buf.len());
                struct_buf.extend_from_slice(&entry_buf);
            }
            buf.put_u8(0x32); // (6 << 3) | 2 = 0x32
            encode_varint(&mut buf, struct_buf.len());
            buf.extend_from_slice(&struct_buf);
        }
    }

    buf
}

// ============================================================================
// GetPoints Encoder
// ============================================================================

/// Encode a GetPoints request to protobuf wire format.
///
/// ```text
/// message GetPoints {
///   string collection_name = 1;
///   repeated PointId ids = 2;
///   WithPayloadSelector with_payload = 4;
///   WithVectorsSelector with_vectors = 5;
/// }
/// ```
pub fn encode_get_points_proto(
    buf: &mut BytesMut,
    collection: &str,
    ids: &[crate::PointId],
    with_vectors: bool,
) {
    buf.clear();

    // Field 1: collection_name
    buf.put_u8(0x0A);
    encode_varint(buf, collection.len());
    buf.extend_from_slice(collection.as_bytes());

    // Field 2: ids (repeated PointId)
    for id in ids {
        let mut id_buf = BytesMut::with_capacity(40);
        match id {
            crate::PointId::Num(n) => {
                id_buf.put_u8(POINT_ID_NUM);
                encode_varint_u64(&mut id_buf, *n);
            }
            crate::PointId::Uuid(s) => {
                id_buf.put_u8(POINT_ID_UUID);
                encode_varint(&mut id_buf, s.len());
                id_buf.extend_from_slice(s.as_bytes());
            }
        }
        buf.put_u8(0x12); // field 2, wire LEN
        encode_varint(buf, id_buf.len());
        buf.extend_from_slice(&id_buf);
    }

    // Field 4: with_payload = true
    buf.put_u8(0x22); // (4 << 3) | 2 = 0x22
    encode_varint(buf, 2);
    buf.put_u8(0x08); // enable = true
    buf.put_u8(0x01);

    // Field 5: with_vectors
    if with_vectors {
        buf.put_u8(0x2A); // (5 << 3) | 2 = 0x2A
        encode_varint(buf, 2);
        buf.put_u8(0x08); // enable = true
        buf.put_u8(0x01);
    }
}

// ============================================================================
// ScrollPoints Encoder
// ============================================================================

/// Encode a ScrollPoints request to protobuf wire format.
///
/// ```text
/// message ScrollPoints {
///   string collection_name = 1;
///   Filter filter = 2;
///   optional PointId offset = 3;
///   uint32 limit = 4;
///   WithPayloadSelector with_payload = 6;
///   WithVectorsSelector with_vectors = 7;
/// }
/// ```
pub fn encode_scroll_points_proto(
    buf: &mut BytesMut,
    collection: &str,
    limit: u32,
    offset: Option<&crate::PointId>,
    with_vectors: bool,
) {
    buf.clear();

    // Field 1: collection_name
    buf.put_u8(0x0A);
    encode_varint(buf, collection.len());
    buf.extend_from_slice(collection.as_bytes());

    // Field 3: offset (optional PointId)
    if let Some(id) = offset {
        let mut id_buf = BytesMut::with_capacity(40);
        match id {
            crate::PointId::Num(n) => {
                id_buf.put_u8(POINT_ID_NUM);
                encode_varint_u64(&mut id_buf, *n);
            }
            crate::PointId::Uuid(s) => {
                id_buf.put_u8(POINT_ID_UUID);
                encode_varint(&mut id_buf, s.len());
                id_buf.extend_from_slice(s.as_bytes());
            }
        }
        buf.put_u8(0x1A); // field 3, wire LEN
        encode_varint(buf, id_buf.len());
        buf.extend_from_slice(&id_buf);
    }

    // Field 4: limit (uint32)
    buf.put_u8(0x20); // (4 << 3) | 0 = 0x20
    encode_varint(buf, limit as usize);

    // Field 6: with_payload = true
    buf.put_u8(0x32); // (6 << 3) | 2 = 0x32
    encode_varint(buf, 2);
    buf.put_u8(0x08);
    buf.put_u8(0x01);

    // Field 7: with_vectors
    if with_vectors {
        buf.put_u8(0x3A); // (7 << 3) | 2 = 0x3A
        encode_varint(buf, 2);
        buf.put_u8(0x08);
        buf.put_u8(0x01);
    }
}

// ============================================================================
// UpdatePayload Encoder
// ============================================================================

/// Encode a SetPayload request to protobuf wire format.
///
/// ```text
/// message SetPayloadPoints {
///   string collection_name = 1;
///   bool wait = 2;
///   map<string, Value> payload = 3;
///   PointsSelector points_selector = 5;
/// }
/// ```
pub fn encode_set_payload_proto(
    buf: &mut BytesMut,
    collection: &str,
    point_ids: &[crate::PointId],
    payload: &crate::point::Payload,
    wait: bool,
) {
    buf.clear();

    // Field 1: collection_name
    buf.put_u8(0x0A);
    encode_varint(buf, collection.len());
    buf.extend_from_slice(collection.as_bytes());

    // Field 2: wait
    if wait {
        buf.put_u8(0x10);
        buf.put_u8(0x01);
    }

    // Field 3: payload (map<string, Value>)
    for (key, value) in payload {
        let mut entry_buf = BytesMut::with_capacity(key.len() + 32);
        entry_buf.put_u8(0x0A); // key (field 1)
        encode_varint(&mut entry_buf, key.len());
        entry_buf.extend_from_slice(key.as_bytes());

        let val_buf = encode_payload_value(value);
        entry_buf.put_u8(0x12); // value (field 2)
        encode_varint(&mut entry_buf, val_buf.len());
        entry_buf.extend_from_slice(&val_buf);

        buf.put_u8(0x1A); // field 3 (payload map entry)
        encode_varint(buf, entry_buf.len());
        buf.extend_from_slice(&entry_buf);
    }

    // Field 5: points_selector -> PointsIdsList
    let selector_buf = encode_points_selector(point_ids);
    buf.put_u8(0x2A); // (5 << 3) | 2 = 0x2A
    encode_varint(buf, selector_buf.len());
    buf.extend_from_slice(&selector_buf);
}

// ============================================================================
// CreateFieldIndex Encoder
// ============================================================================

/// Encode a CreateFieldIndexCollection request.
///
/// ```text
/// message CreateFieldIndexCollection {
///   string collection_name = 1;
///   bool wait = 2;
///   string field_name = 3;
///   optional FieldType field_type = 4;
/// }
/// ```
///
/// FieldType enum: 0=Keyword, 1=Integer, 2=Float, 3=Geo, 4=Text, 5=Bool, 6=Datetime
pub fn encode_create_field_index_proto(
    buf: &mut BytesMut,
    collection: &str,
    field_name: &str,
    field_type: FieldType,
    wait: bool,
) {
    buf.clear();

    // Field 1: collection_name
    buf.put_u8(0x0A);
    encode_varint(buf, collection.len());
    buf.extend_from_slice(collection.as_bytes());

    // Field 2: wait
    if wait {
        buf.put_u8(0x10);
        buf.put_u8(0x01);
    }

    // Field 3: field_name
    buf.put_u8(0x1A);
    encode_varint(buf, field_name.len());
    buf.extend_from_slice(field_name.as_bytes());

    // Field 4: field_type (optional enum)
    buf.put_u8(0x20); // (4 << 3) | 0 = 0x20
    encode_varint(buf, field_type as usize);
}

/// Qdrant payload index field types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FieldType {
    /// Keyword (exact-match string) index.
    Keyword = 0,
    /// Integer index.
    Integer = 1,
    /// Float index.
    Float = 2,
    /// Geolocation index.
    Geo = 3,
    /// Full-text search index.
    Text = 4,
    /// Boolean index.
    Bool = 5,
    /// Datetime index.
    Datetime = 6,
}

// ============================================================================
// CreateCollection Field Tags
// ============================================================================

/// Field 1: collection_name (string) -> 0x0A
const CREATE_COLLECTION_NAME: u8 = 0x0A;
/// Field 10: vectors_config (VectorsConfig) -> (10 << 3) | 2 = 0x52
const CREATE_VECTORS_CONFIG: u8 = 0x52;
/// Field 8: on_disk_payload (bool) -> (8 << 3) | 0 = 0x40
const CREATE_ON_DISK: u8 = 0x40;

// ============================================================================
// DeleteCollection Field Tags
// ============================================================================

/// Field 1: collection_name (string) -> 0x0A
const DELETE_COLLECTION_NAME: u8 = 0x0A;

/// Encode CreateCollection request to protobuf wire format.
pub fn encode_create_collection_proto(
    buf: &mut BytesMut,
    collection_name: &str,
    vector_size: u64,
    distance: crate::Distance,
    on_disk: bool,
) {
    buf.clear();

    // Field 1: collection_name
    buf.put_u8(CREATE_COLLECTION_NAME);
    encode_varint(buf, collection_name.len());
    buf.extend_from_slice(collection_name.as_bytes());

    // Field 2: vectors_config (VectorsConfig -> VectorParams)
    let mut params_buf = BytesMut::with_capacity(32);

    // VectorParams.size (field 1, uint64)
    params_buf.put_u8(0x08);
    encode_varint_u64(&mut params_buf, vector_size);

    // VectorParams.distance (field 2, enum)
    params_buf.put_u8(0x10);
    let distance_val = match distance {
        crate::Distance::Cosine => 1,
        crate::Distance::Euclidean => 2,
        crate::Distance::Dot => 3,
    };
    encode_varint(&mut params_buf, distance_val);

    // VectorParams.on_disk (field 5, bool)
    if on_disk {
        params_buf.put_u8(0x28);
        params_buf.put_u8(0x01);
    }

    // VectorsConfig.params (field 1) wraps VectorParams
    let mut config_buf = BytesMut::with_capacity(params_buf.len() + 4);
    config_buf.put_u8(0x0A);
    encode_varint(&mut config_buf, params_buf.len());
    config_buf.extend_from_slice(&params_buf);

    // Write to main buffer
    buf.put_u8(CREATE_VECTORS_CONFIG);
    encode_varint(buf, config_buf.len());
    buf.extend_from_slice(&config_buf);

    if on_disk {
        buf.put_u8(CREATE_ON_DISK);
        buf.put_u8(0x01);
    }
}

/// Encode DeleteCollection request.
pub fn encode_delete_collection_proto(buf: &mut BytesMut, collection_name: &str) {
    buf.clear();
    buf.put_u8(DELETE_COLLECTION_NAME);
    encode_varint(buf, collection_name.len());
    buf.extend_from_slice(collection_name.as_bytes());
}

// ============================================================================
// DeletePoints Encoder (supports both numeric and UUID IDs)
// ============================================================================

/// Encode a DeletePoints request with support for both numeric and UUID point IDs.
///
/// ```text
/// message DeletePoints {
///   string collection_name = 1;
///   bool wait = 2;
///   PointsSelector points = 4;
/// }
/// ```
pub fn encode_delete_points_mixed_proto(
    buf: &mut BytesMut,
    collection_name: &str,
    point_ids: &[crate::PointId],
) {
    buf.clear();

    // Field 1: collection_name
    buf.put_u8(0x0A);
    encode_varint(buf, collection_name.len());
    buf.put_slice(collection_name.as_bytes());

    // Field 2: wait = true
    buf.put_u8(0x10);
    buf.put_u8(1);

    // Field 4: points (PointsSelector -> PointsIdsList)
    let selector_buf = encode_points_selector(point_ids);
    buf.put_u8(0x22);
    encode_varint(buf, selector_buf.len());
    buf.extend_from_slice(&selector_buf);
}

/// Legacy: Encode a DeletePoints request (numeric IDs only).
pub fn encode_delete_points_proto(buf: &mut BytesMut, collection_name: &str, point_ids: &[u64]) {
    let ids: Vec<crate::PointId> = point_ids
        .iter()
        .map(|&id| crate::PointId::Num(id))
        .collect();
    encode_delete_points_mixed_proto(buf, collection_name, &ids);
}

/// Encode PointsSelector containing a PointsIdsList.
fn encode_points_selector(ids: &[crate::PointId]) -> BytesMut {
    // Build PointsIdsList (field 1: repeated PointId)
    let mut ids_list = BytesMut::with_capacity(ids.len() * 40);
    for id in ids {
        let mut id_buf = BytesMut::with_capacity(40);
        match id {
            crate::PointId::Num(n) => {
                id_buf.put_u8(POINT_ID_NUM);
                encode_varint_u64(&mut id_buf, *n);
            }
            crate::PointId::Uuid(s) => {
                id_buf.put_u8(POINT_ID_UUID);
                encode_varint(&mut id_buf, s.len());
                id_buf.extend_from_slice(s.as_bytes());
            }
        }
        ids_list.put_u8(0x0A); // PointId message (field 1, LEN)
        encode_varint(&mut ids_list, id_buf.len());
        ids_list.extend_from_slice(&id_buf);
    }

    // PointsSelector.points (field 1 = PointsIdsList)
    let mut selector = BytesMut::with_capacity(ids_list.len() + 8);
    selector.put_u8(0x0A); // field 1, LEN
    encode_varint(&mut selector, ids_list.len());
    selector.extend_from_slice(&ids_list);

    selector
}

// ============================================================================
// ListCollections Encoder
// ============================================================================

/// Encode a ListCollections request (empty message).
pub fn encode_list_collections_proto(buf: &mut BytesMut) {
    buf.clear();
    // ListCollectionsRequest is an empty message
}

/// Encode a GetCollectionInfo request.
///
/// ```text
/// message GetCollectionInfoRequest {
///   string collection_name = 1;
/// }
/// ```
pub fn encode_collection_info_proto(buf: &mut BytesMut, collection_name: &str) {
    buf.clear();
    buf.put_u8(0x0A);
    encode_varint(buf, collection_name.len());
    buf.extend_from_slice(collection_name.as_bytes());
}

// ============================================================================
// Utility
// ============================================================================

/// Calculate the byte length of a varint.
#[inline]
pub fn varint_len(value: u64) -> usize {
    if value == 0 {
        1
    } else {
        let bits = 64 - value.leading_zeros() as usize;
        bits.div_ceil(7)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encoding() {
        let mut buf = BytesMut::new();

        // Single byte
        encode_varint(&mut buf, 1);
        assert_eq!(&buf[..], &[0x01]);

        buf.clear();
        encode_varint(&mut buf, 127);
        assert_eq!(&buf[..], &[0x7F]);

        // Two bytes
        buf.clear();
        encode_varint(&mut buf, 128);
        assert_eq!(&buf[..], &[0x80, 0x01]);

        buf.clear();
        encode_varint(&mut buf, 300);
        assert_eq!(&buf[..], &[0xAC, 0x02]);
    }

    #[test]
    fn test_encode_search_basic() {
        let mut buf = BytesMut::with_capacity(1024);
        let vector = vec![0.1f32, 0.2, 0.3, 0.4];

        encode_search_proto(&mut buf, "test_collection", &vector, 10, None, None);

        // Verify starts with collection name field
        assert_eq!(buf[0], SEARCH_COLLECTION);

        // Verify buffer is not empty
        assert!(buf.len() > 20);
    }

    #[test]
    fn test_zero_copy_vector() {
        let mut buf = BytesMut::with_capacity(1024);
        let vector = vec![1.0f32, 2.0, 3.0, 4.0];

        encode_search_proto(&mut buf, "test", &vector, 5, None, None);

        // Find where vector data starts (after collection name + vector tag + length)
        // collection: 0x0A, len(4), "test" = 6 bytes
        // vector tag: 0x12 = 1 byte
        // vector len: 16 (4 floats * 4 bytes) = 1 byte varint
        // Total header: 8 bytes
        let vector_start = 8;
        let vector_bytes = &buf[vector_start..vector_start + 16];

        // Verify floats are correctly encoded as little-endian bytes
        let float_bytes: [u8; 4] = 1.0f32.to_le_bytes();
        assert_eq!(&vector_bytes[0..4], &float_bytes);
    }

    #[test]
    fn test_varint_len() {
        assert_eq!(varint_len(0), 1);
        assert_eq!(varint_len(1), 1);
        assert_eq!(varint_len(127), 1);
        assert_eq!(varint_len(128), 2);
        assert_eq!(varint_len(16383), 2);
        assert_eq!(varint_len(16384), 3);
    }

    #[test]
    fn test_encode_search_with_filter() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let mut buf = BytesMut::with_capacity(1024);
        let vector = vec![0.1f32, 0.2, 0.3];
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

        encode_search_with_filter_proto(
            &mut buf,
            SearchRequest {
                collection: "products",
                vector: &vector,
                limit: 10,
                score_threshold: None,
                vector_name: None,
            },
            &conditions,
            false,
        )
        .expect("filter encoding should succeed");

        // Should contain collection, vector, filter, limit, with_payload
        assert!(buf.len() > 50);
        // First byte should be collection tag
        assert_eq!(buf[0], SEARCH_COLLECTION);
        // Filter tag (0x1A) should appear somewhere after vector
        assert!(buf.contains(&SEARCH_FILTER));
    }

    #[test]
    fn test_encode_get_points() {
        let mut buf = BytesMut::with_capacity(1024);
        let ids = vec![
            crate::PointId::Num(42),
            crate::PointId::Uuid("abc-123".to_string()),
        ];

        encode_get_points_proto(&mut buf, "my_collection", &ids, true);

        assert_eq!(buf[0], 0x0A); // collection name tag
        assert!(buf.len() > 20);
    }

    #[test]
    fn test_encode_scroll_points() {
        let mut buf = BytesMut::with_capacity(1024);

        encode_scroll_points_proto(&mut buf, "my_collection", 100, None, false);

        assert_eq!(buf[0], 0x0A);
        assert!(buf.len() > 10);
    }

    #[test]
    fn test_encode_delete_points_uuid() {
        let mut buf = BytesMut::with_capacity(1024);
        let ids = vec![
            crate::PointId::Uuid("test-uuid-1".to_string()),
            crate::PointId::Num(99),
        ];

        encode_delete_points_mixed_proto(&mut buf, "products", &ids);

        assert_eq!(buf[0], 0x0A); // collection name
        assert!(buf.len() > 20);
    }

    #[test]
    fn test_encode_set_payload() {
        let mut buf = BytesMut::with_capacity(1024);
        let ids = vec![crate::PointId::Num(1)];
        let mut payload = crate::point::Payload::new();
        payload.insert(
            "name".to_string(),
            crate::point::PayloadValue::String("updated".to_string()),
        );

        encode_set_payload_proto(&mut buf, "my_col", &ids, &payload, true);

        assert_eq!(buf[0], 0x0A);
        assert!(buf.len() > 15);
    }

    #[test]
    fn test_encode_search_with_filter_rejects_unsupported_operator() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let mut buf = BytesMut::with_capacity(512);
        let vector = vec![0.1f32, 0.2];
        let conditions = vec![Condition {
            left: Expr::Named("status".to_string()),
            op: Operator::NotLike,
            value: Value::String("%inactive%".to_string()),
            is_array_unnest: false,
        }];

        let err = encode_search_with_filter_proto(
            &mut buf,
            SearchRequest {
                collection: "products",
                vector: &vector,
                limit: 5,
                score_threshold: None,
                vector_name: None,
            },
            &conditions,
            false,
        )
        .expect_err("unsupported operator must return an explicit error");

        match err {
            QdrantError::Encode(message) => {
                assert!(message.contains("Unsupported Qdrant filter condition"));
            }
            other => panic!("expected encode error, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_search_with_filter_grouped_cages_includes_nested_filter_conditions() {
        use qail_core::ast::{Condition, Expr, Operator, Value};

        let mut buf = BytesMut::with_capacity(1024);
        let vector = vec![0.1f32, 0.2, 0.3];
        let must_conditions = vec![Condition {
            left: Expr::Named("tenant_id".to_string()),
            op: Operator::Eq,
            value: Value::String("t1".to_string()),
            is_array_unnest: false,
        }];
        let should_groups = vec![
            vec![
                Condition {
                    left: Expr::Named("city".to_string()),
                    op: Operator::Eq,
                    value: Value::String("London".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("city".to_string()),
                    op: Operator::Eq,
                    value: Value::String("Paris".to_string()),
                    is_array_unnest: false,
                },
            ],
            vec![
                Condition {
                    left: Expr::Named("country".to_string()),
                    op: Operator::Eq,
                    value: Value::String("UK".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("country".to_string()),
                    op: Operator::Eq,
                    value: Value::String("FR".to_string()),
                    is_array_unnest: false,
                },
            ],
        ];

        encode_search_with_filter_grouped_cages_proto(
            &mut buf,
            SearchRequest {
                collection: "products",
                vector: &vector,
                limit: 10,
                score_threshold: None,
                vector_name: None,
            },
            &must_conditions,
            &should_groups,
        )
        .expect("grouped-cage filter encoding should succeed");

        assert!(buf.contains(&SEARCH_FILTER));
        assert!(
            buf.contains(&CONDITION_FILTER),
            "expected nested filter condition tag for OR groups"
        );
    }

    #[test]
    fn test_encode_create_field_index() {
        let mut buf = BytesMut::with_capacity(256);

        encode_create_field_index_proto(&mut buf, "products", "category", FieldType::Keyword, true);

        assert_eq!(buf[0], 0x0A);
        assert!(buf.len() > 10);
    }

    #[test]
    fn test_encode_payload_value_string() {
        let val = crate::point::PayloadValue::String("hello".to_string());
        let buf = encode_payload_value(&val);

        // field 4 tag (0x22) + length + "hello"
        assert_eq!(buf[0], 0x22);
        assert!(buf.len() > 5);
    }

    #[test]
    fn test_encode_payload_value_integer() {
        let val = crate::point::PayloadValue::Integer(42);
        let buf = encode_payload_value(&val);

        // field 3 tag (0x18) + varint(42)
        assert_eq!(buf[0], 0x18);
    }
}
