//! RESP3 Protocol Encoder for QAIL Redis commands.
//!
//! Encodes Qail AST with Redis actions into RESP3 wire protocol bytes.

use bytes::BytesMut;
use qail_core::ast::{Action, Qail};

/// Encode a Qail command to RESP3 bytes.
pub fn encode(cmd: &Qail) -> BytesMut {
    let mut buf = BytesMut::with_capacity(256);
    encode_qail(cmd, &mut buf);
    buf
}

/// Encode a Qail command into the provided buffer.
pub fn encode_qail(cmd: &Qail, buf: &mut BytesMut) {
    match cmd.action {
        Action::RedisGet => encode_get(buf, &cmd.table),
        Action::RedisSet => encode_set(cmd, buf),
        Action::RedisDel => encode_del(buf, &cmd.table),
        Action::RedisIncr => encode_simple(buf, "INCR", &cmd.table),
        Action::RedisDecr => encode_simple(buf, "DECR", &cmd.table),
        Action::RedisTtl => encode_simple(buf, "TTL", &cmd.table),
        Action::RedisExpire => {
            if let Some(ttl) = cmd.redis_ttl {
                encode_expire(buf, &cmd.table, ttl);
            }
        }
        Action::RedisExists => encode_simple(buf, "EXISTS", &cmd.table),
        Action::RedisPing => encode_ping(buf, None),
        _ => {
            // Unsupported action - encode as comment/no-op
        }
    }
}

// ========== Individual Command Encoders ==========

fn encode_get(buf: &mut BytesMut, key: &str) {
    encode_array_header(buf, 2);
    encode_bulk_string(buf, b"GET");
    encode_bulk_string(buf, key.as_bytes());
}

fn encode_set(cmd: &Qail, buf: &mut BytesMut) {
    let key = &cmd.table;
    let value = cmd.raw_value.as_deref().unwrap_or(b"");
    
    // Count arguments: SET key value [EX seconds] [NX|XX]
    let mut argc = 3;
    if cmd.redis_ttl.is_some() {
        argc += 2; // EX seconds
    }
    if cmd.redis_set_condition.is_some() {
        argc += 1; // NX or XX
    }
    
    encode_array_header(buf, argc);
    encode_bulk_string(buf, b"SET");
    encode_bulk_string(buf, key.as_bytes());
    encode_bulk_string(buf, value);
    
    // EX seconds
    if let Some(ttl) = cmd.redis_ttl {
        encode_bulk_string(buf, b"EX");
        encode_bulk_string(buf, ttl.to_string().as_bytes());
    }
    
    // NX or XX
    if let Some(ref cond) = cmd.redis_set_condition {
        encode_bulk_string(buf, cond.as_bytes());
    }
}

fn encode_del(buf: &mut BytesMut, key: &str) {
    encode_array_header(buf, 2);
    encode_bulk_string(buf, b"DEL");
    encode_bulk_string(buf, key.as_bytes());
}

fn encode_simple(buf: &mut BytesMut, cmd_name: &str, key: &str) {
    encode_array_header(buf, 2);
    encode_bulk_string(buf, cmd_name.as_bytes());
    encode_bulk_string(buf, key.as_bytes());
}

fn encode_expire(buf: &mut BytesMut, key: &str, seconds: i64) {
    encode_array_header(buf, 3);
    encode_bulk_string(buf, b"EXPIRE");
    encode_bulk_string(buf, key.as_bytes());
    encode_bulk_string(buf, seconds.to_string().as_bytes());
}

fn encode_ping(buf: &mut BytesMut, message: Option<&str>) {
    if let Some(msg) = message {
        encode_array_header(buf, 2);
        encode_bulk_string(buf, b"PING");
        encode_bulk_string(buf, msg.as_bytes());
    } else {
        encode_array_header(buf, 1);
        encode_bulk_string(buf, b"PING");
    }
}

// ========== RESP3 Primitives ==========

/// Encode RESP3 array header: *<count>\r\n
fn encode_array_header(buf: &mut BytesMut, count: usize) {
    buf.extend_from_slice(b"*");
    buf.extend_from_slice(count.to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
}

/// Encode RESP3 bulk string: $<len>\r\n<data>\r\n
fn encode_bulk_string(buf: &mut BytesMut, data: &[u8]) {
    buf.extend_from_slice(b"$");
    buf.extend_from_slice(data.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

/// Encode HELLO 3 for RESP3 upgrade
pub fn encode_hello(buf: &mut BytesMut, version: u8) {
    encode_array_header(buf, 2);
    encode_bulk_string(buf, b"HELLO");
    encode_bulk_string(buf, version.to_string().as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_get() {
        let cmd = Qail::redis_get("mykey");
        let bytes = encode(&cmd);
        let expected = b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";
        assert_eq!(&bytes[..], &expected[..]);
    }

    #[test]
    fn test_encode_set() {
        let cmd = Qail::redis_set("mykey", b"myvalue".to_vec());
        let bytes = encode(&cmd);
        let expected = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
        assert_eq!(&bytes[..], &expected[..]);
    }

    #[test]
    fn test_encode_set_with_ex() {
        use crate::cmd::RedisExt;
        let cmd = Qail::redis_set("session", b"data".to_vec()).redis_ex(3600);
        let bytes = encode(&cmd);
        // *5: SET key value EX seconds
        assert!(bytes.starts_with(b"*5\r\n"));
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.contains("SET"));
        assert!(s.contains("EX"));
        assert!(s.contains("3600"));
    }

    #[test]
    fn test_encode_ping() {
        let cmd = Qail::redis_ping();
        let bytes = encode(&cmd);
        let expected = b"*1\r\n$4\r\nPING\r\n";
        assert_eq!(&bytes[..], &expected[..]);
    }
}
