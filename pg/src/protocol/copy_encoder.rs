//! Zero-allocation COPY protocol encoder.
//!
//! Encodes `Value` rows directly to PostgreSQL COPY text format bytes
//! without intermediate String allocations.

use bytes::BytesMut;
use qail_core::ast::Value;

use crate::protocol::EncodeError;

/// Encode a Value directly into COPY text format (no SQL quoting).
/// COPY text format rules:
/// - NULL: `\N`
/// - Boolean: `t` or `f`
/// - Numeric: raw digits (no quotes)
/// - String: escape special chars (\\, \t, \n, \r)
/// - UUID: hyphenated lowercase
#[track_caller]
#[inline]
pub fn encode_copy_value(buf: &mut BytesMut, value: &Value) {
    try_encode_copy_value(buf, value)
        .expect("invalid COPY value; use try_encode_copy_value to handle errors");
}

/// Fallible COPY text encoder for a single data value.
#[inline]
pub fn try_encode_copy_value(buf: &mut BytesMut, value: &Value) -> Result<(), EncodeError> {
    match value {
        Value::Null | Value::NullUuid => buf.extend_from_slice(b"\\N"),

        Value::Bool(b) => buf.extend_from_slice(if *b { b"t" } else { b"f" }),

        Value::Int(n) => {
            // Zero-alloc integer formatting
            let mut tmp = itoa::Buffer::new();
            buf.extend_from_slice(tmp.format(*n).as_bytes());
        }

        Value::Float(n) => {
            if !n.is_finite() {
                return Err(EncodeError::InvalidAst(format!(
                    "COPY float value must be finite, got {n}"
                )));
            }
            // Zero-alloc float formatting
            let mut tmp = ryu::Buffer::new();
            buf.extend_from_slice(tmp.format(*n).as_bytes());
        }

        Value::String(s) => write_copy_escaped_str(buf, s)?,

        Value::Uuid(u) => {
            // UUID: 36-char hyphenated lowercase
            let mut uuid_buf = [0u8; 36];
            u.hyphenated().encode_lower(&mut uuid_buf);
            buf.extend_from_slice(&uuid_buf);
        }

        Value::Timestamp(ts) => write_copy_escaped_str(buf, ts)?,

        Value::Column(_)
        | Value::Function(_)
        | Value::Param(_)
        | Value::NamedParam(_)
        | Value::Subquery(_)
        | Value::Expr(_) => {
            return Err(EncodeError::InvalidAst(
                "COPY data value cannot be an expression or unresolved parameter".to_string(),
            ));
        }

        Value::Array(arr) => {
            let mut arr_buf = Vec::with_capacity(arr.len() * 8 + 2);
            arr_buf.extend_from_slice(b"{");
            for (i, v) in arr.iter().enumerate() {
                if i > 0 {
                    arr_buf.push(b',');
                }
                write_copy_array_value(&mut arr_buf, v)?;
            }
            arr_buf.extend_from_slice(b"}");
            write_copy_escaped_bytes(buf, &arr_buf);
        }

        Value::Interval { amount, unit } => {
            // interval '7 days' format
            let mut tmp = itoa::Buffer::new();
            buf.extend_from_slice(tmp.format(*amount).as_bytes());
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(unit.to_string().as_bytes());
        }

        Value::Bytes(bytes) => {
            // PostgreSQL bytea hex format: \x followed by hex digits
            buf.extend_from_slice(b"\\\\x");
            for byte in bytes {
                // Format each byte as 2 hex digits
                let hi = byte >> 4;
                let lo = byte & 0x0f;
                buf.extend_from_slice(&[
                    if hi < 10 { b'0' + hi } else { b'a' + hi - 10 },
                    if lo < 10 { b'0' + lo } else { b'a' + lo - 10 },
                ]);
            }
        }
        Value::Vector(vec) => {
            // PostgreSQL array format for vectors: {1.0,2.0,3.0}
            buf.extend_from_slice(b"{");
            for (i, v) in vec.iter().enumerate() {
                if !v.is_finite() {
                    return Err(EncodeError::InvalidAst(format!(
                        "COPY vector value must be finite, got {v}"
                    )));
                }
                if i > 0 {
                    buf.extend_from_slice(b",");
                }
                let mut tmp = ryu::Buffer::new();
                buf.extend_from_slice(tmp.format(*v).as_bytes());
            }
            buf.extend_from_slice(b"}");
        }
        Value::Json(json) => write_copy_escaped_str(buf, json)?,
    }
    Ok(())
}

fn write_copy_escaped_str(buf: &mut BytesMut, value: &str) -> Result<(), EncodeError> {
    if value.as_bytes().contains(&0) {
        return Err(EncodeError::NullByte);
    }
    write_copy_escaped_bytes(buf, value.as_bytes());
    Ok(())
}

fn write_copy_escaped_bytes(buf: &mut BytesMut, value: &[u8]) {
    for byte in value {
        match *byte {
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            _ => buf.extend_from_slice(&[*byte]),
        }
    }
}

fn write_copy_array_value(buf: &mut Vec<u8>, value: &Value) -> Result<(), EncodeError> {
    match value {
        Value::Null | Value::NullUuid => buf.extend_from_slice(b"NULL"),
        Value::Bool(value) => buf.extend_from_slice(if *value { b"t" } else { b"f" }),
        Value::Int(value) => buf.extend_from_slice(value.to_string().as_bytes()),
        Value::Float(value) => {
            if !value.is_finite() {
                return Err(EncodeError::InvalidAst(format!(
                    "COPY array float value must be finite, got {value}"
                )));
            }
            buf.extend_from_slice(value.to_string().as_bytes());
        }
        Value::Uuid(value) => buf.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) | Value::Timestamp(value) | Value::Json(value) => {
            write_quoted_array_element(buf, value)?
        }
        Value::Interval { amount, unit } => {
            write_quoted_array_element(buf, &format!("{amount} {unit}"))?;
        }
        Value::Column(_)
        | Value::Function(_)
        | Value::Param(_)
        | Value::NamedParam(_)
        | Value::Array(_)
        | Value::Subquery(_)
        | Value::Bytes(_)
        | Value::Expr(_)
        | Value::Vector(_) => {
            return Err(EncodeError::InvalidAst(
                "COPY array value cannot contain expressions or nested binary/vector values"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn write_quoted_array_element(buf: &mut Vec<u8>, value: &str) -> Result<(), EncodeError> {
    if value.as_bytes().contains(&0) {
        return Err(EncodeError::NullByte);
    }

    buf.push(b'"');
    for byte in value.bytes() {
        if byte == b'"' || byte == b'\\' {
            buf.push(b'\\');
        }
        buf.push(byte);
    }
    buf.push(b'"');
    Ok(())
}

/// Encode a batch of rows into a single COPY data buffer.
/// Returns a BytesMut containing all rows in tab-separated format,
/// ready to be sent as a single CopyData message.
#[track_caller]
#[inline]
pub fn encode_copy_batch(rows: &[Vec<Value>]) -> BytesMut {
    // Pre-allocate: estimate ~50 bytes per column, 7 columns avg
    let estimated_size = rows.len() * 7 * 50;
    let mut buf = BytesMut::with_capacity(estimated_size);

    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b"\t");
            }
            encode_copy_value(&mut buf, val);
        }
        buf.extend_from_slice(b"\n");
    }

    buf
}

/// Fallible batch COPY text encoder.
#[inline]
pub fn try_encode_copy_batch(rows: &[Vec<Value>]) -> Result<BytesMut, EncodeError> {
    let estimated_size = rows.len() * 7 * 50;
    let mut buf = BytesMut::with_capacity(estimated_size);

    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b"\t");
            }
            try_encode_copy_value(&mut buf, val)?;
        }
        buf.extend_from_slice(b"\n");
    }

    Ok(buf)
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
        encode_copy_value(&mut buf, &Value::Float(9.87654));
        assert!(buf.starts_with(b"9.87"));
    }

    #[test]
    fn test_encode_string_escaping() {
        let mut buf = BytesMut::new();
        encode_copy_value(&mut buf, &Value::String("hello\tworld\n".to_string()));
        assert_eq!(&buf[..], b"hello\\tworld\\n");
    }

    #[test]
    fn test_try_encode_rejects_expression_values() {
        let mut buf = BytesMut::new();
        let err = try_encode_copy_value(&mut buf, &Value::Function("now()\n1\t2".to_string()))
            .unwrap_err();

        assert!(
            matches!(err, EncodeError::InvalidAst(ref message) if message.contains("COPY data value cannot be an expression")),
            "{err}"
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_try_encode_rejects_null_bytes() {
        let mut buf = BytesMut::new();
        let err =
            try_encode_copy_value(&mut buf, &Value::String("bad\0value".to_string())).unwrap_err();

        assert_eq!(err, EncodeError::NullByte);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_try_encode_rejects_non_finite_float() {
        let mut buf = BytesMut::new();
        let err = try_encode_copy_value(&mut buf, &Value::Float(f64::INFINITY)).unwrap_err();

        assert!(
            matches!(err, EncodeError::InvalidAst(ref message) if message.contains("must be finite")),
            "{err}"
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_try_encode_rejects_non_finite_vector() {
        let mut buf = BytesMut::new();
        let err = try_encode_copy_value(&mut buf, &Value::Vector(vec![1.0, f32::NAN])).unwrap_err();

        assert!(
            matches!(err, EncodeError::InvalidAst(ref message) if message.contains("must be finite")),
            "{err}"
        );
    }

    #[test]
    #[should_panic(expected = "invalid COPY value")]
    fn test_encode_copy_value_panics_on_invalid_value() {
        let mut buf = BytesMut::new();
        encode_copy_value(&mut buf, &Value::Function("now()".to_string()));
    }

    #[test]
    fn test_try_encode_array_quotes_delimiter_values() {
        let mut buf = BytesMut::new();

        try_encode_copy_value(
            &mut buf,
            &Value::Array(vec![
                Value::String("a,b".to_string()),
                Value::String("line\nnext".to_string()),
            ]),
        )
        .unwrap();

        assert_eq!(&buf[..], br#"{"a,b","line\nnext"}"#);
    }

    #[test]
    fn test_try_encode_batch_rejects_expression_values() {
        let rows = vec![vec![Value::Int(1), Value::Column("users.id".to_string())]];

        let err = try_encode_copy_batch(&rows).unwrap_err();

        assert!(
            matches!(err, EncodeError::InvalidAst(ref message) if message.contains("COPY data value cannot be an expression")),
            "{err}"
        );
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
