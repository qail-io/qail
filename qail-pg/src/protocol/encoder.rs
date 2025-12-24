//! PostgreSQL Encoder (Visitor Pattern)
//!
//! Compiles QailCmd AST into PostgreSQL wire protocol bytes.
//! This is pure, synchronous computation - no I/O, no async.
//!
//! # Architecture
//!
//! Layer 2 of the QAIL architecture:
//! - Input: QailCmd (AST)
//! - Output: BytesMut (ready to send over the wire)
//!
//! The async I/O layer (Layer 3) consumes these bytes.

use bytes::BytesMut;
use qail_core::ast::QailCmd;
use qail_core::transpiler::ToSql;

/// PostgreSQL protocol encoder.
///
/// Takes a QailCmd and produces wire protocol bytes.
/// This is the "Visitor" in the visitor pattern.
pub struct PgEncoder;

impl PgEncoder {
    /// Encode a QailCmd as a Simple Query message.
    ///
    /// Simple Query protocol sends SQL as text.
    /// The database parses, plans, and executes in one round-trip.
    ///
    /// # Example
    /// ```ignore
    /// let cmd = QailCmd::get("users");
    /// let bytes = PgEncoder::encode_simple_query(&cmd);
    /// // bytes can now be written to TcpStream by Layer 3
    /// ```
    pub fn encode_simple_query(cmd: &QailCmd) -> BytesMut {
        let sql = cmd.to_sql();
        Self::encode_query_string(&sql)
    }

    /// Encode a raw SQL string as a Simple Query message.
    ///
    /// Wire format:
    /// - 'Q' (1 byte) - message type
    /// - length (4 bytes, big-endian, includes self)
    /// - query string (null-terminated)
    pub fn encode_query_string(sql: &str) -> BytesMut {
        let mut buf = BytesMut::new();
        
        // Message type 'Q' for Query
        buf.extend_from_slice(&[b'Q']);
        
        // Content: query string + null terminator
        let content_len = sql.len() + 1; // +1 for null terminator
        let total_len = (content_len + 4) as i32; // +4 for length field itself
        
        // Length (4 bytes, big-endian)
        buf.extend_from_slice(&total_len.to_be_bytes());
        
        // Query string
        buf.extend_from_slice(sql.as_bytes());
        
        // Null terminator
        buf.extend_from_slice(&[0]);
        
        buf
    }

    /// Encode a Terminate message to close the connection.
    pub fn encode_terminate() -> BytesMut {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[b'X', 0, 0, 0, 4]);
        buf
    }

    /// Encode a Sync message (end of pipeline in extended query protocol).
    pub fn encode_sync() -> BytesMut {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[b'S', 0, 0, 0, 4]);
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qail_core::ast::QailCmd;

    #[test]
    fn test_encode_simple_query() {
        let cmd = QailCmd::get("users");
        let bytes = PgEncoder::encode_simple_query(&cmd);
        
        // Should start with 'Q'
        assert_eq!(bytes[0], b'Q');
        
        // Should contain "SELECT * FROM users"
        let content = String::from_utf8_lossy(&bytes[5..]);
        assert!(content.contains("SELECT"));
        assert!(content.contains("users"));
    }

    #[test]
    fn test_encode_query_string() {
        let sql = "SELECT 1";
        let bytes = PgEncoder::encode_query_string(sql);
        
        // Message type
        assert_eq!(bytes[0], b'Q');
        
        // Length: 4 (length field) + 8 (query) + 1 (null) = 13
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert_eq!(len, 13);
        
        // Query content
        assert_eq!(&bytes[5..13], b"SELECT 1");
        
        // Null terminator
        assert_eq!(bytes[13], 0);
    }

    #[test]
    fn test_encode_terminate() {
        let bytes = PgEncoder::encode_terminate();
        assert_eq!(bytes.as_ref(), &[b'X', 0, 0, 0, 4]);
    }
}
