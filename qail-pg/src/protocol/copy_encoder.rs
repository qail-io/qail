//! Zero-allocation COPY protocol encoder.
//!
//! Encodes `Value` rows directly to PostgreSQL COPY text format bytes
//! without intermediate String allocations.

use bytes::BytesMut;
use qail_core::ast::Value;

/// Encode a Value directly into COPY text format (no SQL quoting).
///
/// COPY text format rules:
/// - NULL: `\N`
/// - Boolean: `t` or `f`
/// - Numeric: raw digits (no quotes)
/// - String: escape special chars (\\, \t, \n, \r)
/// - UUID: hyphenated lowercase
#[inline]
pub fn encode_copy_value(buf: &mut BytesMut, value: &Value) {
    match value {
        Value::Null | Value::NullUuid => buf.extend_from_slice(b"\\N"),
        
        Value::Bool(b) => buf.extend_from_slice(if *b { b"t" } else { b"f" }),
        
        Value::Int(n) => {
            // Zero-alloc integer formatting
            let mut tmp = itoa::Buffer::new();
            buf.extend_from_slice(tmp.format(*n).as_bytes());
        }
        
        Value::Float(n) => {
            // Zero-alloc float formatting
            let mut tmp = ryu::Buffer::new();
            buf.extend_from_slice(tmp.format(*n).as_bytes());
        }
        
        Value::String(s) => {
            // COPY text format: escape tabs, newlines, backslashes
            for c in s.bytes() {
                match c {
                    b'\\' => buf.extend_from_slice(b"\\\\"),
                    b'\t' => buf.extend_from_slice(b"\\t"),
                    b'\n' => buf.extend_from_slice(b"\\n"),
                    b'\r' => buf.extend_from_slice(b"\\r"),
                    _ => buf.extend_from_slice(&[c]),
                }
            }
        }
        
        Value::Uuid(u) => {
            // UUID: 36-char hyphenated lowercase
            let mut uuid_buf = [0u8; 36];
            u.hyphenated().encode_lower(&mut uuid_buf);
            buf.extend_from_slice(&uuid_buf);
        }
        
        Value::Timestamp(ts) => buf.extend_from_slice(ts.as_bytes()),
        
        Value::Column(s) => buf.extend_from_slice(s.as_bytes()),
        
        Value::Function(s) => buf.extend_from_slice(s.as_bytes()),
        
        Value::Param(n) => {
            // $N - unlikely in COPY but handle gracefully
            buf.extend_from_slice(b"$");
            let mut tmp = itoa::Buffer::new();
            buf.extend_from_slice(tmp.format(*n).as_bytes());
        }
        
        Value::NamedParam(name) => {
            buf.extend_from_slice(b":");
            buf.extend_from_slice(name.as_bytes());
        }
        
        Value::Array(arr) => {
            // PostgreSQL array literal: {val1,val2,...}
            buf.extend_from_slice(b"{");
            for (i, v) in arr.iter().enumerate() {
                if i > 0 { buf.extend_from_slice(b","); }
                encode_copy_value(buf, v);
            }
            buf.extend_from_slice(b"}");
        }
        
        Value::Interval { amount, unit } => {
            // interval '7 days' format
            let mut tmp = itoa::Buffer::new();
            buf.extend_from_slice(tmp.format(*amount).as_bytes());
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(unit.to_string().as_bytes());
        }
        
        Value::Subquery(_) => {
            // Can't COPY a subquery - output NULL
            buf.extend_from_slice(b"\\N");
        }
    }
}

/// Encode a batch of rows into a single COPY data buffer.
///
/// Returns a BytesMut containing all rows in tab-separated format,
/// ready to be sent as a single CopyData message.
#[inline]
pub fn encode_copy_batch(rows: &[Vec<Value>]) -> BytesMut {
    // Pre-allocate: estimate ~50 bytes per column, 7 columns avg
    let estimated_size = rows.len() * 7 * 50;
    let mut buf = BytesMut::with_capacity(estimated_size);
    
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i > 0 { buf.extend_from_slice(b"\t"); }
            encode_copy_value(&mut buf, val);
        }
        buf.extend_from_slice(b"\n");
    }
    
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_encode_int() {
        let mut buf = BytesMut::new();
        encode_copy_value(&mut buf, &Value::Int(12345));
        assert_eq!(&buf[..], b"12345");
    }

    #[test]
    fn test_encode_float() {
        let mut buf = BytesMut::new();
        encode_copy_value(&mut buf, &Value::Float(3.14159));
        assert!(buf.starts_with(b"3.14"));
    }

    #[test]
    fn test_encode_string_escaping() {
        let mut buf = BytesMut::new();
        encode_copy_value(&mut buf, &Value::String("hello\tworld\n".to_string()));
        assert_eq!(&buf[..], b"hello\\tworld\\n");
    }

    #[test]
    fn test_encode_null() {
        let mut buf = BytesMut::new();
        encode_copy_value(&mut buf, &Value::Null);
        assert_eq!(&buf[..], b"\\N");
    }

    #[test]
    fn test_encode_batch() {
        let rows = vec![
            vec![Value::Int(1), Value::String("foo".to_string())],
            vec![Value::Int(2), Value::String("bar".to_string())],
        ];
        let buf = encode_copy_batch(&rows);
        assert_eq!(&buf[..], b"1\tfoo\n2\tbar\n");
    }

    #[test]
    fn test_encode_uuid() {
        let mut buf = BytesMut::new();
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        encode_copy_value(&mut buf, &Value::Uuid(uuid));
        assert_eq!(&buf[..], b"550e8400-e29b-41d4-a716-446655440000");
    }
}
