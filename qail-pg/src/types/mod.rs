//! Type conversion traits and implementations for PostgreSQL types.
//!
//! This module provides traits for converting Rust types to/from PostgreSQL wire format.

pub mod temporal;
pub mod numeric;

pub use temporal::{Timestamp, Date, Time};
pub use numeric::Numeric;

use crate::protocol::types::{oid, decode_uuid, decode_jsonb, decode_json, decode_text_array};

/// Error type for type conversion failures.
#[derive(Debug, Clone)]
pub enum TypeError {
    /// Wrong OID for expected type
    UnexpectedOid { expected: &'static str, got: u32 },
    /// Invalid binary data
    InvalidData(String),
    /// Null value where non-null expected
    UnexpectedNull,
}

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeError::UnexpectedOid { expected, got } => {
                write!(f, "Expected {} type, got OID {}", expected, got)
            }
            TypeError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            TypeError::UnexpectedNull => write!(f, "Unexpected NULL value"),
        }
    }
}

impl std::error::Error for TypeError {}

/// Trait for converting PostgreSQL binary/text data to Rust types.
pub trait FromPg: Sized {
    /// Convert from PostgreSQL wire format.
    /// 
    /// # Arguments
    /// * `bytes` - Raw bytes from PostgreSQL (may be text or binary format)
    /// * `oid` - PostgreSQL type OID
    /// * `format` - 0 = text, 1 = binary
    fn from_pg(bytes: &[u8], oid: u32, format: i16) -> Result<Self, TypeError>;
}

/// Trait for converting Rust types to PostgreSQL wire format.
pub trait ToPg {
    /// Convert to PostgreSQL wire format.
    /// Returns (bytes, oid, format_code)
    fn to_pg(&self) -> (Vec<u8>, u32, i16);
}

// ==================== String Types ====================

impl FromPg for String {
    fn from_pg(bytes: &[u8], _oid: u32, _format: i16) -> Result<Self, TypeError> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| TypeError::InvalidData(format!("Invalid UTF-8: {}", e)))
    }
}

impl ToPg for String {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.as_bytes().to_vec(), oid::TEXT, 0)
    }
}

impl ToPg for &str {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.as_bytes().to_vec(), oid::TEXT, 0)
    }
}

// ==================== Integer Types ====================

impl FromPg for i32 {
    fn from_pg(bytes: &[u8], _oid: u32, format: i16) -> Result<Self, TypeError> {
        if format == 1 {
            // Binary format: 4 bytes big-endian
            if bytes.len() != 4 {
                return Err(TypeError::InvalidData("Expected 4 bytes for i32".to_string()));
            }
            Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        } else {
            // Text format
            std::str::from_utf8(bytes)
                .map_err(|e| TypeError::InvalidData(e.to_string()))?
                .parse()
                .map_err(|e| TypeError::InvalidData(format!("Invalid i32: {}", e)))
        }
    }
}

impl ToPg for i32 {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.to_be_bytes().to_vec(), oid::INT4, 1)
    }
}

impl FromPg for i64 {
    fn from_pg(bytes: &[u8], _oid: u32, format: i16) -> Result<Self, TypeError> {
        if format == 1 {
            // Binary format: 8 bytes big-endian
            if bytes.len() != 8 {
                return Err(TypeError::InvalidData("Expected 8 bytes for i64".to_string()));
            }
            Ok(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]))
        } else {
            // Text format
            std::str::from_utf8(bytes)
                .map_err(|e| TypeError::InvalidData(e.to_string()))?
                .parse()
                .map_err(|e| TypeError::InvalidData(format!("Invalid i64: {}", e)))
        }
    }
}

impl ToPg for i64 {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.to_be_bytes().to_vec(), oid::INT8, 1)
    }
}

// ==================== Float Types ====================

impl FromPg for f64 {
    fn from_pg(bytes: &[u8], _oid: u32, format: i16) -> Result<Self, TypeError> {
        if format == 1 {
            // Binary format: 8 bytes IEEE 754
            if bytes.len() != 8 {
                return Err(TypeError::InvalidData("Expected 8 bytes for f64".to_string()));
            }
            Ok(f64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]))
        } else {
            // Text format
            std::str::from_utf8(bytes)
                .map_err(|e| TypeError::InvalidData(e.to_string()))?
                .parse()
                .map_err(|e| TypeError::InvalidData(format!("Invalid f64: {}", e)))
        }
    }
}

impl ToPg for f64 {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.to_be_bytes().to_vec(), oid::FLOAT8, 1)
    }
}

// ==================== Boolean ====================

impl FromPg for bool {
    fn from_pg(bytes: &[u8], _oid: u32, format: i16) -> Result<Self, TypeError> {
        if format == 1 {
            // Binary: 1 byte, 0 or 1
            Ok(bytes.first().map(|b| *b != 0).unwrap_or(false))
        } else {
            // Text: 't' or 'f'
            match bytes.first() {
                Some(b't') | Some(b'T') | Some(b'1') => Ok(true),
                Some(b'f') | Some(b'F') | Some(b'0') => Ok(false),
                _ => Err(TypeError::InvalidData("Invalid boolean".to_string())),
            }
        }
    }
}

impl ToPg for bool {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (vec![if *self { 1 } else { 0 }], oid::BOOL, 1)
    }
}

// ==================== UUID ====================

/// UUID type (uses String internally for simplicity)
#[derive(Debug, Clone, PartialEq)]
pub struct Uuid(pub String);

impl FromPg for Uuid {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::UUID {
            return Err(TypeError::UnexpectedOid { expected: "uuid", got: oid_val });
        }
        
        if format == 1 && bytes.len() == 16 {
            // Binary format: 16 bytes
            decode_uuid(bytes)
                .map(Uuid)
                .map_err(TypeError::InvalidData)
        } else {
            // Text format
            String::from_utf8(bytes.to_vec())
                .map(Uuid)
                .map_err(|e| TypeError::InvalidData(e.to_string()))
        }
    }
}

impl ToPg for Uuid {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        // Send as text for simplicity
        (self.0.as_bytes().to_vec(), oid::UUID, 0)
    }
}

// ==================== JSON/JSONB ====================

/// JSON value (wraps the raw JSON string)
#[derive(Debug, Clone, PartialEq)]
pub struct Json(pub String);

impl FromPg for Json {
    fn from_pg(bytes: &[u8], oid_val: u32, _format: i16) -> Result<Self, TypeError> {
        let json_str = if oid_val == oid::JSONB {
            decode_jsonb(bytes).map_err(TypeError::InvalidData)?
        } else {
            decode_json(bytes).map_err(TypeError::InvalidData)?
        };
        Ok(Json(json_str))
    }
}

impl ToPg for Json {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        // Send as JSONB with version byte
        let mut buf = Vec::with_capacity(1 + self.0.len());
        buf.push(1); // JSONB version
        buf.extend_from_slice(self.0.as_bytes());
        (buf, oid::JSONB, 1)
    }
}

// ==================== Arrays ====================

impl FromPg for Vec<String> {
    fn from_pg(bytes: &[u8], _oid: u32, _format: i16) -> Result<Self, TypeError> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| TypeError::InvalidData(e.to_string()))?;
        Ok(decode_text_array(s))
    }
}

impl FromPg for Vec<i64> {
    fn from_pg(bytes: &[u8], _oid: u32, _format: i16) -> Result<Self, TypeError> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| TypeError::InvalidData(e.to_string()))?;
        crate::protocol::types::decode_int_array(s)
            .map_err(TypeError::InvalidData)
    }
}

// ==================== Option<T> ====================

impl<T: FromPg> FromPg for Option<T> {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        // This is for non-null; actual NULL handling is done at row level
        Ok(Some(T::from_pg(bytes, oid_val, format)?))
    }
}

// ==================== Bytes ====================

impl FromPg for Vec<u8> {
    fn from_pg(bytes: &[u8], _oid: u32, _format: i16) -> Result<Self, TypeError> {
        Ok(bytes.to_vec())
    }
}

impl ToPg for Vec<u8> {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.clone(), oid::BYTEA, 1)
    }
}

impl ToPg for &[u8] {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.to_vec(), oid::BYTEA, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_from_pg() {
        let result = String::from_pg(b"hello", oid::TEXT, 0).unwrap();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_i32_from_pg_text() {
        let result = i32::from_pg(b"42", oid::INT4, 0).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_i32_from_pg_binary() {
        let bytes = 42i32.to_be_bytes();
        let result = i32::from_pg(&bytes, oid::INT4, 1).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_bool_from_pg() {
        assert!(bool::from_pg(b"t", oid::BOOL, 0).unwrap());
        assert!(!bool::from_pg(b"f", oid::BOOL, 0).unwrap());
        assert!(bool::from_pg(&[1], oid::BOOL, 1).unwrap());
        assert!(!bool::from_pg(&[0], oid::BOOL, 1).unwrap());
    }

    #[test]
    fn test_uuid_from_pg_binary() {
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
            0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
        ];
        let result = Uuid::from_pg(&uuid_bytes, oid::UUID, 1).unwrap();
        assert_eq!(result.0, "550e8400-e29b-41d4-a716-446655440000");
    }
}
