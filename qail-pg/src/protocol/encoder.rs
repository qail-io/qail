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

    // ==================== Extended Query Protocol ====================

    /// Encode a Parse message (prepare a statement).
    ///
    /// Wire format:
    /// - 'P' (1 byte) - message type
    /// - length (4 bytes)
    /// - statement name (null-terminated, "" for unnamed)
    /// - query string (null-terminated)
    /// - parameter count (2 bytes)
    /// - parameter OIDs (4 bytes each, 0 = infer type)
    pub fn encode_parse(name: &str, sql: &str, param_types: &[u32]) -> BytesMut {
        let mut buf = BytesMut::new();
        
        // Message type 'P'
        buf.extend_from_slice(&[b'P']);
        
        // Build content first to calculate length
        let mut content = Vec::new();
        
        // Statement name (null-terminated)
        content.extend_from_slice(name.as_bytes());
        content.push(0);
        
        // Query string (null-terminated)
        content.extend_from_slice(sql.as_bytes());
        content.push(0);
        
        // Parameter count
        content.extend_from_slice(&(param_types.len() as i16).to_be_bytes());
        
        // Parameter OIDs
        for &oid in param_types {
            content.extend_from_slice(&oid.to_be_bytes());
        }
        
        // Length (includes length field itself)
        let len = (content.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&content);
        
        buf
    }

    /// Encode a Bind message (bind parameters to a prepared statement).
    ///
    /// Wire format:
    /// - 'B' (1 byte) - message type
    /// - length (4 bytes)
    /// - portal name (null-terminated)
    /// - statement name (null-terminated)
    /// - format code count (2 bytes) - we use 0 (all text)
    /// - parameter count (2 bytes)
    /// - for each parameter: length (4 bytes, -1 for NULL), data
    /// - result format count (2 bytes) - we use 0 (all text)
    pub fn encode_bind(portal: &str, statement: &str, params: &[Option<Vec<u8>>]) -> BytesMut {
        let mut buf = BytesMut::new();
        
        // Message type 'B'
        buf.extend_from_slice(&[b'B']);
        
        // Build content
        let mut content = Vec::new();
        
        // Portal name (null-terminated)
        content.extend_from_slice(portal.as_bytes());
        content.push(0);
        
        // Statement name (null-terminated)
        content.extend_from_slice(statement.as_bytes());
        content.push(0);
        
        // Format codes count (0 = use default text format)
        content.extend_from_slice(&0i16.to_be_bytes());
        
        // Parameter count
        content.extend_from_slice(&(params.len() as i16).to_be_bytes());
        
        // Parameters
        for param in params {
            match param {
                None => {
                    // NULL: length = -1
                    content.extend_from_slice(&(-1i32).to_be_bytes());
                }
                Some(data) => {
                    content.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    content.extend_from_slice(data);
                }
            }
        }
        
        // Result format codes count (0 = use default text format)
        content.extend_from_slice(&0i16.to_be_bytes());
        
        // Length
        let len = (content.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&content);
        
        buf
    }

    /// Encode an Execute message (execute a bound portal).
    ///
    /// Wire format:
    /// - 'E' (1 byte) - message type
    /// - length (4 bytes)
    /// - portal name (null-terminated)
    /// - max rows (4 bytes, 0 = unlimited)
    pub fn encode_execute(portal: &str, max_rows: i32) -> BytesMut {
        let mut buf = BytesMut::new();
        
        // Message type 'E'
        buf.extend_from_slice(&[b'E']);
        
        // Build content
        let mut content = Vec::new();
        
        // Portal name (null-terminated)
        content.extend_from_slice(portal.as_bytes());
        content.push(0);
        
        // Max rows
        content.extend_from_slice(&max_rows.to_be_bytes());
        
        // Length
        let len = (content.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&content);
        
        buf
    }

    /// Encode a Describe message (get statement/portal metadata).
    ///
    /// Wire format:
    /// - 'D' (1 byte) - message type
    /// - length (4 bytes)
    /// - 'S' for statement or 'P' for portal
    /// - name (null-terminated)
    pub fn encode_describe(is_portal: bool, name: &str) -> BytesMut {
        let mut buf = BytesMut::new();
        
        // Message type 'D'
        buf.extend_from_slice(&[b'D']);
        
        // Build content
        let mut content = Vec::new();
        
        // Type: 'S' for statement, 'P' for portal
        content.push(if is_portal { b'P' } else { b'S' });
        
        // Name (null-terminated)
        content.extend_from_slice(name.as_bytes());
        content.push(0);
        
        // Length
        let len = (content.len() + 4) as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&content);
        
        buf
    }

    /// Encode a complete extended query pipeline (OPTIMIZED).
    ///
    /// This combines Parse + Bind + Execute + Sync in a single buffer.
    /// Zero intermediate allocations - writes directly to pre-sized BytesMut.
    pub fn encode_extended_query(sql: &str, params: &[Option<Vec<u8>>]) -> BytesMut {
        // Calculate total size upfront to avoid reallocations
        // Parse: 1 + 4 + 1 + sql.len() + 1 + 2 = 9 + sql.len()
        // Bind: 1 + 4 + 1 + 1 + 2 + 2 + params_data + 2 = 13 + params_data
        // Execute: 1 + 4 + 1 + 4 = 10
        // Sync: 5
        let params_size: usize = params.iter()
            .map(|p| 4 + p.as_ref().map_or(0, |v| v.len()))
            .sum();
        let total_size = 9 + sql.len() + 13 + params_size + 10 + 5;
        
        let mut buf = BytesMut::with_capacity(total_size);
        
        // ===== PARSE =====
        buf.extend_from_slice(&[b'P']);
        let parse_len = (1 + sql.len() + 1 + 2 + 4) as i32; // name + sql + param_count
        buf.extend_from_slice(&parse_len.to_be_bytes());
        buf.extend_from_slice(&[0]); // Unnamed statement
        buf.extend_from_slice(sql.as_bytes());
        buf.extend_from_slice(&[0]); // Null terminator
        buf.extend_from_slice(&0i16.to_be_bytes()); // No param types (infer)
        
        // ===== BIND =====
        buf.extend_from_slice(&[b'B']);
        let bind_len = (1 + 1 + 2 + 2 + params_size + 2 + 4) as i32;
        buf.extend_from_slice(&bind_len.to_be_bytes());
        buf.extend_from_slice(&[0]); // Unnamed portal
        buf.extend_from_slice(&[0]); // Unnamed statement
        buf.extend_from_slice(&0i16.to_be_bytes()); // Format codes (default text)
        buf.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for param in params {
            match param {
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(data) => {
                    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    buf.extend_from_slice(data);
                }
            }
        }
        buf.extend_from_slice(&0i16.to_be_bytes()); // Result format (default text)
        
        // ===== EXECUTE =====
        buf.extend_from_slice(&[b'E']);
        buf.extend_from_slice(&9i32.to_be_bytes()); // len = 4 + 1 + 4
        buf.extend_from_slice(&[0]); // Unnamed portal
        buf.extend_from_slice(&0i32.to_be_bytes()); // Unlimited rows
        
        // ===== SYNC =====
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

    #[test]
    fn test_encode_sync() {
        let bytes = PgEncoder::encode_sync();
        assert_eq!(bytes.as_ref(), &[b'S', 0, 0, 0, 4]);
    }

    #[test]
    fn test_encode_parse() {
        let bytes = PgEncoder::encode_parse("", "SELECT $1", &[]);
        
        // Message type 'P'
        assert_eq!(bytes[0], b'P');
        
        // Content should include query
        let content = String::from_utf8_lossy(&bytes[5..]);
        assert!(content.contains("SELECT $1"));
    }

    #[test]
    fn test_encode_bind() {
        let params = vec![
            Some(b"42".to_vec()),
            None, // NULL
        ];
        let bytes = PgEncoder::encode_bind("", "", &params);
        
        // Message type 'B'
        assert_eq!(bytes[0], b'B');
        
        // Should have proper length
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert!(len > 4); // At least header
    }

    #[test]
    fn test_encode_execute() {
        let bytes = PgEncoder::encode_execute("", 0);
        
        // Message type 'E'
        assert_eq!(bytes[0], b'E');
        
        // Length: 4 + 1 (null) + 4 (max_rows) = 9
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert_eq!(len, 9);
    }

    #[test]
    fn test_encode_extended_query() {
        let params = vec![Some(b"hello".to_vec())];
        let bytes = PgEncoder::encode_extended_query("SELECT $1", &params);
        
        // Should contain all 4 message types: P, B, E, S
        assert!(bytes.windows(1).any(|w| w == [b'P']));
        assert!(bytes.windows(1).any(|w| w == [b'B']));
        assert!(bytes.windows(1).any(|w| w == [b'E']));
        assert!(bytes.windows(1).any(|w| w == [b'S']));
    }
}
