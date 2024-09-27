//! RESP3 protocol decoder.
//!
//! Parses Redis wire protocol responses into Value types.

use bytes::Buf;
use crate::error::{RedisError, RedisResult};
use crate::value::Value;

/// Decode a RESP3 response from the buffer.
///
/// Returns the parsed value and the number of bytes consumed.
pub fn decode(buf: &[u8]) -> RedisResult<(Value, usize)> {
    if buf.is_empty() {
        return Err(RedisError::Protocol("Empty buffer".into()));
    }

    let mut cursor = std::io::Cursor::new(buf);
    let value = decode_value(&mut cursor)?;
    Ok((value, cursor.position() as usize))
}

/// Decode a single RESP value.
fn decode_value(cursor: &mut std::io::Cursor<&[u8]>) -> RedisResult<Value> {
    if !cursor.has_remaining() {
        return Err(RedisError::Protocol("Unexpected end of buffer".into()));
    }

    let type_byte = cursor.get_u8();

    match type_byte {
        // Simple string: +OK\r\n
        b'+' => {
            let line = read_line(cursor)?;
            Ok(Value::String(line))
        }

        // Error: -ERR message\r\n
        b'-' => {
            let line = read_line(cursor)?;
            Ok(Value::Error(line))
        }

        // Integer: :1000\r\n
        b':' => {
            let line = read_line(cursor)?;
            let num: i64 = line.parse().map_err(|_| {
                RedisError::Protocol(format!("Invalid integer: {}", line))
            })?;
            Ok(Value::Integer(num))
        }

        // Bulk string: $5\r\nhello\r\n
        b'$' => {
            let line = read_line(cursor)?;
            let len: i64 = line.parse().map_err(|_| {
                RedisError::Protocol(format!("Invalid bulk length: {}", line))
            })?;

            if len < 0 {
                return Ok(Value::Null);
            }

            let len = len as usize;
            if cursor.remaining() < len + 2 {
                return Err(RedisError::Protocol("Incomplete bulk string".into()));
            }

            let mut data = vec![0u8; len];
            cursor.copy_to_slice(&mut data);

            // Skip \r\n
            cursor.advance(2);

            Ok(Value::Bulk(data))
        }

        // Array: *2\r\n...
        b'*' => {
            let line = read_line(cursor)?;
            let count: i64 = line.parse().map_err(|_| {
                RedisError::Protocol(format!("Invalid array length: {}", line))
            })?;

            if count < 0 {
                return Ok(Value::Null);
            }

            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                items.push(decode_value(cursor)?);
            }
            Ok(Value::Array(items))
        }

        // Null: _\r\n (RESP3)
        b'_' => {
            read_line(cursor)?; // consume \r\n
            Ok(Value::Null)
        }

        // Boolean: #t\r\n or #f\r\n (RESP3)
        b'#' => {
            let line = read_line(cursor)?;
            match line.as_str() {
                "t" => Ok(Value::Boolean(true)),
                "f" => Ok(Value::Boolean(false)),
                _ => Err(RedisError::Protocol(format!("Invalid boolean: {}", line))),
            }
        }

        // Double: ,1.23\r\n (RESP3)
        b',' => {
            let line = read_line(cursor)?;
            let num: f64 = line.parse().map_err(|_| {
                RedisError::Protocol(format!("Invalid double: {}", line))
            })?;
            Ok(Value::Double(num))
        }

        // Map: %2\r\n... (RESP3)
        b'%' => {
            let line = read_line(cursor)?;
            let count: i64 = line.parse().map_err(|_| {
                RedisError::Protocol(format!("Invalid map length: {}", line))
            })?;

            let mut pairs = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let key = decode_value(cursor)?;
                let value = decode_value(cursor)?;
                pairs.push((key, value));
            }
            Ok(Value::Map(pairs))
        }

        _ => Err(RedisError::Protocol(format!(
            "Unknown type byte: {}",
            type_byte as char
        ))),
    }
}

/// Read a line until \r\n.
fn read_line(cursor: &mut std::io::Cursor<&[u8]>) -> RedisResult<String> {
    let start = cursor.position() as usize;
    let buf = cursor.get_ref();

    // Find \r\n
    let mut end = start;
    while end < buf.len() - 1 {
        if buf[end] == b'\r' && buf[end + 1] == b'\n' {
            let line = std::str::from_utf8(&buf[start..end])
                .map_err(|_| RedisError::Protocol("Invalid UTF-8".into()))?;
            cursor.set_position((end + 2) as u64);
            return Ok(line.to_string());
        }
        end += 1;
    }

    Err(RedisError::Protocol("Incomplete line".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_simple_string() {
        let (value, len) = decode(b"+OK\r\n").unwrap();
        assert_eq!(value, Value::String("OK".into()));
        assert_eq!(len, 5);
    }

    #[test]
    fn test_decode_error() {
        let (value, _) = decode(b"-ERR unknown command\r\n").unwrap();
        assert_eq!(value, Value::Error("ERR unknown command".into()));
    }

    #[test]
    fn test_decode_integer() {
        let (value, _) = decode(b":1000\r\n").unwrap();
        assert_eq!(value, Value::Integer(1000));
    }

    #[test]
    fn test_decode_bulk_string() {
        let (value, _) = decode(b"$5\r\nhello\r\n").unwrap();
        assert_eq!(value, Value::Bulk(b"hello".to_vec()));
    }

    #[test]
    fn test_decode_null_bulk() {
        let (value, _) = decode(b"$-1\r\n").unwrap();
        assert_eq!(value, Value::Null);
    }

    #[test]
    fn test_decode_array() {
        let (value, _) = decode(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Bulk(b"foo".to_vec()),
                Value::Bulk(b"bar".to_vec()),
            ])
        );
    }

    #[test]
    fn test_decode_resp3_boolean() {
        let (value, _) = decode(b"#t\r\n").unwrap();
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn test_decode_resp3_null() {
        let (value, _) = decode(b"_\r\n").unwrap();
        assert_eq!(value, Value::Null);
    }
}
