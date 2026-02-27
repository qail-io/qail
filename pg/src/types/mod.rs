//! Type conversion traits and implementations for PostgreSQL types.
//!
//! This module provides traits for converting Rust types to/from PostgreSQL wire format.

pub mod numeric;
pub mod temporal;

pub use numeric::Numeric;
pub use temporal::{Date, Time, Timestamp};

use crate::protocol::types::{decode_json, decode_jsonb, decode_text_array, decode_uuid, oid};

/// Error type for type conversion failures.
#[derive(Debug, Clone)]
pub enum TypeError {
    /// Wrong OID for expected type
    UnexpectedOid {
        /// Human-readable name of the expected type (e.g. `"uuid"`).
        expected: &'static str,
        /// Actual OID received from the server.
        got: u32,
    },
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
                return Err(TypeError::InvalidData(
                    "Expected 4 bytes for i32".to_string(),
                ));
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
                return Err(TypeError::InvalidData(
                    "Expected 8 bytes for i64".to_string(),
                ));
            }
            Ok(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
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
                return Err(TypeError::InvalidData(
                    "Expected 8 bytes for f64".to_string(),
                ));
            }
            Ok(f64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
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
            return Err(TypeError::UnexpectedOid {
                expected: "uuid",
                got: oid_val,
            });
        }

        if format == 1 && bytes.len() == 16 {
            // Binary format: 16 bytes
            decode_uuid(bytes).map(Uuid).map_err(TypeError::InvalidData)
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

// ==================== Network Types ====================

fn from_utf8_string(bytes: &[u8]) -> Result<String, TypeError> {
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|e| TypeError::InvalidData(e.to_string()))
}

fn decode_inet_like_binary(bytes: &[u8], force_prefix: bool) -> Result<String, TypeError> {
    // inet/cidr binary format:
    // 1 byte family (2 = IPv4, 3 = IPv6)
    // 1 byte bits (netmask length)
    // 1 byte is_cidr (0 = inet, 1 = cidr)
    // 1 byte addr_len
    // N bytes address
    if bytes.len() < 4 {
        return Err(TypeError::InvalidData(
            "inet/cidr binary payload too short".to_string(),
        ));
    }

    let family = bytes[0];
    let bits = bytes[1];
    let is_cidr = bytes[2];
    let addr_len = bytes[3] as usize;

    if bytes.len() != 4 + addr_len {
        return Err(TypeError::InvalidData(
            "inet/cidr binary payload length mismatch".to_string(),
        ));
    }

    let addr = &bytes[4..];
    match family {
        2 => {
            if addr_len > 4 {
                return Err(TypeError::InvalidData(
                    "invalid IPv4 inet/cidr address length".to_string(),
                ));
            }
            let mut full = [0u8; 4];
            full[..addr_len].copy_from_slice(addr);
            let ip = std::net::Ipv4Addr::from(full);
            let include_prefix = force_prefix || is_cidr != 0 || bits != 32;
            if include_prefix {
                Ok(format!("{}/{}", ip, bits))
            } else {
                Ok(ip.to_string())
            }
        }
        3 => {
            if addr_len > 16 {
                return Err(TypeError::InvalidData(
                    "invalid IPv6 inet/cidr address length".to_string(),
                ));
            }
            let mut full = [0u8; 16];
            full[..addr_len].copy_from_slice(addr);
            let ip = std::net::Ipv6Addr::from(full);
            let include_prefix = force_prefix || is_cidr != 0 || bits != 128;
            if include_prefix {
                Ok(format!("{}/{}", ip, bits))
            } else {
                Ok(ip.to_string())
            }
        }
        _ => Err(TypeError::InvalidData(format!(
            "unsupported inet/cidr address family: {}",
            family
        ))),
    }
}

/// IPv4/IPv6 host/network address (`inet`)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inet(pub String);

impl Inet {
    /// Create a new `Inet` value from text representation.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the underlying textual representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromPg for Inet {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::INET {
            return Err(TypeError::UnexpectedOid {
                expected: "inet",
                got: oid_val,
            });
        }

        let s = if format == 1 {
            decode_inet_like_binary(bytes, false)?
        } else {
            from_utf8_string(bytes)?
        };
        Ok(Inet(s))
    }
}

impl ToPg for Inet {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.0.as_bytes().to_vec(), oid::INET, 0)
    }
}

/// IPv4/IPv6 network block (`cidr`)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cidr(pub String);

impl Cidr {
    /// Create a new `Cidr` value from text representation.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the underlying textual representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromPg for Cidr {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::CIDR {
            return Err(TypeError::UnexpectedOid {
                expected: "cidr",
                got: oid_val,
            });
        }

        let s = if format == 1 {
            decode_inet_like_binary(bytes, true)?
        } else {
            from_utf8_string(bytes)?
        };
        Ok(Cidr(s))
    }
}

impl ToPg for Cidr {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.0.as_bytes().to_vec(), oid::CIDR, 0)
    }
}

/// MAC address (`macaddr`)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacAddr(pub String);

impl MacAddr {
    /// Create a new `MacAddr` value from text representation.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the underlying textual representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromPg for MacAddr {
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        if oid_val != oid::MACADDR {
            return Err(TypeError::UnexpectedOid {
                expected: "macaddr",
                got: oid_val,
            });
        }

        let s = if format == 1 {
            if bytes.len() != 6 {
                return Err(TypeError::InvalidData(
                    "Expected 6 bytes for macaddr".to_string(),
                ));
            }
            format!(
                "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
            )
        } else {
            from_utf8_string(bytes)?
        };

        Ok(MacAddr(s))
    }
}

impl ToPg for MacAddr {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        (self.0.as_bytes().to_vec(), oid::MACADDR, 0)
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
        let s = std::str::from_utf8(bytes).map_err(|e| TypeError::InvalidData(e.to_string()))?;
        Ok(decode_text_array(s))
    }
}

impl FromPg for Vec<i64> {
    fn from_pg(bytes: &[u8], _oid: u32, _format: i16) -> Result<Self, TypeError> {
        let s = std::str::from_utf8(bytes).map_err(|e| TypeError::InvalidData(e.to_string()))?;
        crate::protocol::types::decode_int_array(s).map_err(TypeError::InvalidData)
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
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let result = Uuid::from_pg(&uuid_bytes, oid::UUID, 1).unwrap();
        assert_eq!(result.0, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_inet_from_pg_text() {
        let inet = Inet::from_pg(b"10.0.0.1", oid::INET, 0).unwrap();
        assert_eq!(inet.0, "10.0.0.1");
    }

    #[test]
    fn test_inet_from_pg_binary_ipv4() {
        // family=2 (IPv4), bits=32, is_cidr=0, addr_len=4, addr=10.1.2.3
        let bytes = [2u8, 32, 0, 4, 10, 1, 2, 3];
        let inet = Inet::from_pg(&bytes, oid::INET, 1).unwrap();
        assert_eq!(inet.0, "10.1.2.3");
    }

    #[test]
    fn test_cidr_from_pg_binary_ipv4() {
        // family=2 (IPv4), bits=24, is_cidr=1, addr_len=4, addr=192.168.1.0
        let bytes = [2u8, 24, 1, 4, 192, 168, 1, 0];
        let cidr = Cidr::from_pg(&bytes, oid::CIDR, 1).unwrap();
        assert_eq!(cidr.0, "192.168.1.0/24");
    }

    #[test]
    fn test_macaddr_from_pg_binary() {
        let bytes = [0x08u8, 0x00, 0x2b, 0x01, 0x02, 0x03];
        let mac = MacAddr::from_pg(&bytes, oid::MACADDR, 1).unwrap();
        assert_eq!(mac.0, "08:00:2b:01:02:03");
    }

    #[test]
    fn test_network_types_to_pg_oids() {
        let inet = Inet::new("10.0.0.0/8");
        let (_, inet_oid, inet_format) = inet.to_pg();
        assert_eq!(inet_oid, oid::INET);
        assert_eq!(inet_format, 0);

        let cidr = Cidr::new("10.0.0.0/8");
        let (_, cidr_oid, cidr_format) = cidr.to_pg();
        assert_eq!(cidr_oid, oid::CIDR);
        assert_eq!(cidr_format, 0);

        let mac = MacAddr::new("08:00:2b:01:02:03");
        let (_, mac_oid, mac_format) = mac.to_pg();
        assert_eq!(mac_oid, oid::MACADDR);
        assert_eq!(mac_format, 0);
    }
}
