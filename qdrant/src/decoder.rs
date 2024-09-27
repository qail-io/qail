//! Zero-copy Protobuf decoder for Qdrant gRPC responses.
//!
//! Decodes protobuf wire format directly without intermediate allocations.
//! Matches the zero-copy pattern of proto_encoder.rs.

use crate::error::{QdrantError, QdrantResult};
use crate::point::{PointId, ScoredPoint, Payload};

// ============================================================================
// Wire Type Constants
// ============================================================================

const WIRE_VARINT: u8 = 0;
const WIRE_FIXED64: u8 = 1;
const WIRE_LEN: u8 = 2;
const WIRE_FIXED32: u8 = 5;

// ============================================================================
// SearchResponse Field Numbers
// ============================================================================
// message SearchResponse {
//   repeated ScoredPoint result = 1;
//   double time = 2;
// }

const SEARCH_RESULT: u32 = 1;
#[allow(dead_code)]
const SEARCH_TIME: u32 = 2;

// ============================================================================
// ScoredPoint Field Numbers
// ============================================================================
// message ScoredPoint {
//   PointId id = 1;
//   map<string, Value> payload = 2;
//   float score = 3;
//   uint64 version = 5;
// }

const SCORED_POINT_ID: u32 = 1;
#[allow(dead_code)]
const SCORED_POINT_PAYLOAD: u32 = 2;
const SCORED_POINT_SCORE: u32 = 3;
#[allow(dead_code)]
const SCORED_POINT_VERSION: u32 = 5;

// ============================================================================
// PointId Field Numbers
// ============================================================================
// message PointId {
//   oneof point_id_options {
//     uint64 num = 1;
//     string uuid = 2;
//   }
// }

const POINT_ID_NUM: u32 = 1;
const POINT_ID_UUID: u32 = 2;

// ============================================================================
// Varint Decoding
// ============================================================================

/// Decode a varint from the buffer, advancing the cursor.
#[inline]
fn decode_varint(buf: &mut &[u8]) -> QdrantResult<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    
    loop {
        if buf.is_empty() {
            return Err(QdrantError::Decode("Unexpected end of data in varint".to_string()));
        }
        
        let byte = buf[0];
        *buf = &buf[1..];
        
        result |= ((byte & 0x7F) as u64) << shift;
        
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        
        shift += 7;
        if shift >= 64 {
            return Err(QdrantError::Decode("Varint too long".to_string()));
        }
    }
}

/// Decode a field tag (field_number << 3 | wire_type).
#[inline]
fn decode_tag(buf: &mut &[u8]) -> QdrantResult<(u32, u8)> {
    let tag = decode_varint(buf)?;
    let field_number = (tag >> 3) as u32;
    let wire_type = (tag & 0x07) as u8;
    Ok((field_number, wire_type))
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
            return Err(QdrantError::Decode(format!("Unknown wire type: {}", wire_type)));
        }
    }
    Ok(())
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
                // repeated ScoredPoint - length-delimited
                if wire_type != WIRE_LEN {
                    return Err(QdrantError::Decode("Expected length-delimited for ScoredPoint".to_string()));
                }
                
                let len = decode_varint(&mut buf)? as usize;
                if buf.len() < len {
                    return Err(QdrantError::Decode("Truncated ScoredPoint".to_string()));
                }
                
                let point_data = &buf[..len];
                buf = &buf[len..];
                
                let point = decode_scored_point(point_data)?;
                results.push(point);
            }
            _ => {
                // Skip unknown fields (including time, usage)
                skip_field(&mut buf, wire_type)?;
            }
        }
    }
    
    Ok(results)
}

/// Decode a single ScoredPoint message.
fn decode_scored_point(data: &[u8]) -> QdrantResult<ScoredPoint> {
    let mut id = PointId::Num(0);
    let mut score = 0.0f32;
    let mut buf = data;
    
    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;
        
        match field_number {
            SCORED_POINT_ID => {
                // PointId - length-delimited submessage
                if wire_type != WIRE_LEN {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                
                let len = decode_varint(&mut buf)? as usize;
                if buf.len() < len {
                    return Err(QdrantError::Decode("Truncated PointId".to_string()));
                }
                
                let id_data = &buf[..len];
                buf = &buf[len..];
                
                id = decode_point_id(id_data)?;
            }
            SCORED_POINT_SCORE => {
                // float - fixed32
                if wire_type != WIRE_FIXED32 {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                
                if buf.len() < 4 {
                    return Err(QdrantError::Decode("Truncated score".to_string()));
                }
                
                let bytes = [buf[0], buf[1], buf[2], buf[3]];
                score = f32::from_le_bytes(bytes);
                buf = &buf[4..];
            }
            _ => {
                // Skip payload, version, vectors, etc.
                skip_field(&mut buf, wire_type)?;
            }
        }
    }
    
    Ok(ScoredPoint {
        id,
        score,
        payload: Payload::new(),
        vector: None,
    })
}

/// Decode a PointId message.
fn decode_point_id(data: &[u8]) -> QdrantResult<PointId> {
    let mut buf = data;
    
    while !buf.is_empty() {
        let (field_number, wire_type) = decode_tag(&mut buf)?;
        
        match field_number {
            POINT_ID_NUM => {
                // uint64
                if wire_type != WIRE_VARINT {
                    skip_field(&mut buf, wire_type)?;
                    continue;
                }
                let num = decode_varint(&mut buf)?;
                return Ok(PointId::Num(num));
            }
            POINT_ID_UUID => {
                // string
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
                return Ok(PointId::Uuid(uuid_str.to_string()));
            }
            _ => {
                skip_field(&mut buf, wire_type)?;
            }
        }
    }
    
    // Default to Num(0) if empty
    Ok(PointId::Num(0))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_varint() {
        // Single byte
        let mut buf: &[u8] = &[0x01];
        assert_eq!(decode_varint(&mut buf).unwrap(), 1);
        assert!(buf.is_empty());
        
        // Two bytes (300 = 0xAC 0x02)
        let mut buf: &[u8] = &[0xAC, 0x02];
        assert_eq!(decode_varint(&mut buf).unwrap(), 300);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_tag() {
        // Field 1, wire type 2 = 0x0A
        let mut buf: &[u8] = &[0x0A];
        let (field, wire) = decode_tag(&mut buf).unwrap();
        assert_eq!(field, 1);
        assert_eq!(wire, WIRE_LEN);
        
        // Field 3, wire type 5 = (3 << 3) | 5 = 0x1D
        let mut buf: &[u8] = &[0x1D];
        let (field, wire) = decode_tag(&mut buf).unwrap();
        assert_eq!(field, 3);
        assert_eq!(wire, WIRE_FIXED32);
    }

    #[test]
    fn test_decode_point_id_num() {
        // PointId { num = 42 }
        // Field 1 (num), varint = 0x08, value 42 = 0x2A
        let data = &[0x08, 0x2A];
        let id = decode_point_id(data).unwrap();
        assert_eq!(id, PointId::Num(42));
    }

    #[test]
    fn test_decode_point_id_uuid() {
        // PointId { uuid = "abc" }
        // Field 2 (uuid), len-delimited = 0x12, len 3, "abc"
        let data = &[0x12, 0x03, b'a', b'b', b'c'];
        let id = decode_point_id(data).unwrap();
        assert_eq!(id, PointId::Uuid("abc".to_string()));
    }

    #[test]
    fn test_decode_scored_point() {
        // ScoredPoint { id: PointId { num: 1 }, score: 0.5 }
        // Field 1 (id): 0x0A, len 2, [0x08, 0x01] (num = 1)
        // Field 3 (score): 0x1D, f32 bytes for 0.5
        let score_bytes = 0.5f32.to_le_bytes();
        let data = &[
            0x0A, 0x02, 0x08, 0x01,  // id = PointId { num = 1 }
            0x1D, score_bytes[0], score_bytes[1], score_bytes[2], score_bytes[3],  // score = 0.5
        ];
        
        let point = decode_scored_point(data).unwrap();
        assert_eq!(point.id, PointId::Num(1));
        assert!((point.score - 0.5).abs() < 0.0001);
    }

    #[test]
    fn test_decode_search_response_empty() {
        let data: &[u8] = &[];
        let results = decode_search_response(data).unwrap();
        assert!(results.is_empty());
    }
}
