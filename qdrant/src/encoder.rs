//! Zero-copy Protobuf encoder for Qdrant gRPC protocol.
//!
//! This module implements direct wire format encoding without intermediate
//! struct allocations. Key optimizations:
//! - Pre-computed field tag bytes
//! - Buffer reuse via BytesMut
//! - Direct memcpy for vectors (no per-element loop)

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
#[allow(dead_code)]
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
#[allow(dead_code)]
const POINT_PAYLOAD: u8 = 0x1A;

// ============================================================================
// PointId Field Tags
// ============================================================================

/// Field 1: num (uint64) -> (1 << 3) | 0 = 0x08
const POINT_ID_NUM: u8 = 0x08;
/// Field 2: uuid (string) -> (2 << 3) | 2 = 0x12
const POINT_ID_UUID: u8 = 0x12;

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
    
    // ZERO-COPY: Write floats directly as bytes
    // Safe because f32 is 4 bytes on all platforms
    let float_bytes = unsafe {
        std::slice::from_raw_parts(vector.as_ptr() as *const u8, vector_bytes_len)
    };
    buf.extend_from_slice(float_bytes);
    
    // Field 4: limit (varint)
    buf.put_u8(SEARCH_LIMIT);
    encode_varint_u64(buf, limit);
    
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

/// Encode a single PointStruct.
fn encode_point_struct(buf: &mut BytesMut, point: &crate::Point) {
    // We need to encode into a temp buffer first to get length,
    // since PointStruct is length-delimited
    let mut point_buf = BytesMut::with_capacity(point.vector.len() * 4 + 64);
    
    // Field 1: id (PointId oneof)
    match &point.id {
        crate::PointId::Num(n) => {
            // Nested message: PointId { num: n }
            point_buf.put_u8(POINT_ID);
            let id_len = 1 + varint_len(*n); // tag + varint
            encode_varint(&mut point_buf, id_len);
            point_buf.put_u8(POINT_ID_NUM);
            encode_varint_u64(&mut point_buf, *n);
        }
        crate::PointId::Uuid(s) => {
            point_buf.put_u8(POINT_ID);
            let id_len = 1 + varint_len(s.len() as u64) + s.len();
            encode_varint(&mut point_buf, id_len);
            point_buf.put_u8(POINT_ID_UUID);
            encode_varint(&mut point_buf, s.len());
            point_buf.extend_from_slice(s.as_bytes());
        }
    }
    
    // Field 4: vectors (Vectors -> Vector)
    // Using the simpler deprecated approach: Vector.data = repeated float (field 1)
    // This encodes as packed floats
    let vector_bytes_len = point.vector.len() * 4;
    
    // Vector message: field 1 = data (packed floats)
    // Tag for field 1, wire type 2 (length-delimited) = 0x0A
    let vector_inner_len = 1 + varint_len(vector_bytes_len as u64) + vector_bytes_len;
    
    // Vectors message: field 1 = vector (Vector message)  
    // Tag for field 1, wire type 2 (length-delimited) = 0x0A
    let vectors_len = 1 + varint_len(vector_inner_len as u64) + vector_inner_len;
    
    // Write Vectors field (field 4 of PointStruct)
    point_buf.put_u8(POINT_VECTORS);  // 0x22 = field 4, wire type 2
    encode_varint(&mut point_buf, vectors_len);
    
    // Vectors.vector (field 1)
    point_buf.put_u8(0x0A);  // field 1, wire type 2
    encode_varint(&mut point_buf, vector_inner_len);
    
    // Vector.data (field 1, packed floats) - deprecated but still works
    point_buf.put_u8(0x0A);  // field 1, wire type 2
    encode_varint(&mut point_buf, vector_bytes_len);
    let float_bytes = unsafe {
        std::slice::from_raw_parts(point.vector.as_ptr() as *const u8, vector_bytes_len)
    };
    point_buf.extend_from_slice(float_bytes);
    
    // TODO: Field 3: payload (map) - skipped for now
    
    // Write to main buffer with length prefix
    buf.put_u8(UPSERT_POINTS);
    encode_varint(buf, point_buf.len());
    buf.extend_from_slice(&point_buf);
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
    distance: qail_core::ast::Distance,
    on_disk: bool,
) {
    buf.clear();

    // Field 1: collection_name
    buf.put_u8(CREATE_COLLECTION_NAME);
    encode_varint(buf, collection_name.len());
    buf.extend_from_slice(collection_name.as_bytes());

    // Field 2: vectors_config (VectorsConfig -> VectorParams)
    // Need to construct nested messages for VectorParams
    let mut params_buf = BytesMut::with_capacity(32);
    
    // VectorParams.size (field 1, uint64)
    params_buf.put_u8(0x08);
    encode_varint_u64(&mut params_buf, vector_size);

    // VectorParams.distance (field 2, enum)
    // Per Qdrant proto: UnknownDistance=0, Cosine=1, Euclid=2, Dot=3
    params_buf.put_u8(0x10);
    let distance_val = match distance {
        qail_core::ast::Distance::Cosine => 1,
        qail_core::ast::Distance::Euclid => 2,
        qail_core::ast::Distance::Dot => 3,
    };
    encode_varint(&mut params_buf, distance_val);

    // VectorParams.on_disk (field 5, bool) - optional but useful
    if on_disk {
        params_buf.put_u8(0x28); // field 5
        params_buf.put_u8(0x01);
    }

    // VectorsConfig.params (field 1) wraps VectorParams
    let mut config_buf = BytesMut::with_capacity(params_buf.len() + 4);
    config_buf.put_u8(0x0A); // field 1
    encode_varint(&mut config_buf, params_buf.len());
    config_buf.extend_from_slice(&params_buf);

    // Write to main buffer
    buf.put_u8(CREATE_VECTORS_CONFIG);
    encode_varint(buf, config_buf.len());
    buf.extend_from_slice(&config_buf);

    // Field 5: on_disk_payload (bool) - optional
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

/// Calculate the byte length of a varint.
#[inline]
fn varint_len(value: u64) -> usize {
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
}

// ============================================================================
// DeletePoints Encoder
// ============================================================================

/// Encode a DeletePoints request.
/// 
/// DeletePoints message:
/// - Field 1: collection_name (string)
/// - Field 2: wait (bool)
/// - Field 4: points (PointsSelector message)
///
/// PointsSelector (oneof):
/// - Field 1: points (PointsIdsList message)
///
/// PointsIdsList:
/// - Field 1: ids (repeated PointId)
pub fn encode_delete_points_proto(
    buf: &mut BytesMut,
    collection_name: &str,
    point_ids: &[u64],
) {
    // Pre-calculate sizes for efficient allocation
    
    // Calculate PointsIdsList size (field 1 repeated PointId)
    let mut ids_list_size = 0usize;
    for &id in point_ids {
        // Each PointId is a message with field 2 (num) as varint
        // Tag for field 2 (varint): 0x10, then varint value
        let point_id_inner_size = 1 + varint_len(id);
        // PointId message: tag 0x0A (field 1, len-delimited) + len + content
        ids_list_size += 1 + varint_len(point_id_inner_size as u64) + point_id_inner_size;
    }
    
    // PointsSelector size: field 1 (points) = tag + len + ids_list_size
    let selector_inner_size = 1 + varint_len(ids_list_size as u64) + ids_list_size;
    
    // DeletePoints fields:
    // Field 1: collection_name
    let collection_size = 1 + varint_len(collection_name.len() as u64) + collection_name.len();
    // Field 2: wait = true (tag 0x10, value 1)
    let wait_size = 2;
    // Field 4: points (PointsSelector) tag 0x22 + len + content
    let points_field_size = 1 + varint_len(selector_inner_size as u64) + selector_inner_size;
    
    let total_size = collection_size + wait_size + points_field_size;
    buf.reserve(total_size);
    
    // Field 1: collection_name
    buf.put_u8(0x0A);
    encode_varint(buf, collection_name.len());
    buf.put_slice(collection_name.as_bytes());
    
    // Field 2: wait = true
    buf.put_u8(0x10);
    buf.put_u8(1);
    
    // Field 4: points (PointsSelector message)
    buf.put_u8(0x22);
    encode_varint(buf, selector_inner_size);
    
    // PointsSelector.points (field 1): PointsIdsList
    buf.put_u8(0x0A);
    encode_varint(buf, ids_list_size);
    
    // PointsIdsList.ids (field 1): repeated PointId
    for &id in point_ids {
        let point_id_inner_size = 1 + varint_len(id);
        buf.put_u8(0x0A);  // PointId message tag
        encode_varint(buf, point_id_inner_size);
        buf.put_u8(0x10);  // PointId.num field tag (field 2, varint)
        encode_varint(buf, id as usize);
    }
}

