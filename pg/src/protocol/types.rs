//! PostgreSQL Type OID Constants
//!
//! Reference: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat

/// PostgreSQL Type OIDs
#[allow(dead_code)]
pub mod oid {
    // Boolean
    pub const BOOL: u32 = 16;

    // Bytes
    pub const BYTEA: u32 = 17;

    // Characters
    pub const CHAR: u32 = 18;
    pub const NAME: u32 = 19;

    // Integers
    pub const INT8: u32 = 20; // bigint
    pub const INT2: u32 = 21; // smallint
    pub const INT4: u32 = 23; // integer

    // Text
    pub const TEXT: u32 = 25;
    pub const VARCHAR: u32 = 1043;
    pub const BPCHAR: u32 = 1042; // blank-padded char

    // OID
    pub const OID: u32 = 26;

    // JSON
    pub const JSON: u32 = 114;
    pub const JSONB: u32 = 3802;

    // Float
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;

    // Numeric
    pub const NUMERIC: u32 = 1700;

    // Date/Time
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const INTERVAL: u32 = 1186;

    // UUID
    pub const UUID: u32 = 2950;

    // Arrays (OID of element type + 1 in most cases, but actually defined separately)
    pub const BOOL_ARRAY: u32 = 1000;
    pub const INT2_ARRAY: u32 = 1005;
    pub const INT4_ARRAY: u32 = 1007;
    pub const INT8_ARRAY: u32 = 1016;
    pub const TEXT_ARRAY: u32 = 1009;
    pub const VARCHAR_ARRAY: u32 = 1015;
    pub const FLOAT4_ARRAY: u32 = 1021;
    pub const FLOAT8_ARRAY: u32 = 1022;
    pub const UUID_ARRAY: u32 = 2951;
    pub const JSONB_ARRAY: u32 = 3807;
}

/// Map OID to a human-readable type name
pub fn oid_to_name(oid: u32) -> &'static str {
    match oid {
        oid::BOOL => "bool",
        oid::BYTEA => "bytea",
        oid::CHAR => "char",
        oid::NAME => "name",
        oid::INT8 => "int8",
        oid::INT2 => "int2",
        oid::INT4 => "int4",
        oid::TEXT => "text",
        oid::VARCHAR => "varchar",
        oid::BPCHAR => "bpchar",
        oid::OID => "oid",
        oid::JSON => "json",
        oid::JSONB => "jsonb",
        oid::FLOAT4 => "float4",
        oid::FLOAT8 => "float8",
        oid::NUMERIC => "numeric",
        oid::DATE => "date",
        oid::TIME => "time",
        oid::TIMESTAMP => "timestamp",
        oid::TIMESTAMPTZ => "timestamptz",
        oid::INTERVAL => "interval",
        oid::UUID => "uuid",
        oid::BOOL_ARRAY => "bool[]",
        oid::INT2_ARRAY => "int2[]",
        oid::INT4_ARRAY => "int4[]",
        oid::INT8_ARRAY => "int8[]",
        oid::TEXT_ARRAY => "text[]",
        oid::VARCHAR_ARRAY => "varchar[]",
        oid::FLOAT4_ARRAY => "float4[]",
        oid::FLOAT8_ARRAY => "float8[]",
        oid::UUID_ARRAY => "uuid[]",
        oid::JSONB_ARRAY => "jsonb[]",
        _ => "unknown",
    }
}

/// Check if an OID represents an array type
pub fn is_array_oid(oid: u32) -> bool {
    matches!(
        oid,
        oid::BOOL_ARRAY
            | oid::INT2_ARRAY
            | oid::INT4_ARRAY
            | oid::INT8_ARRAY
            | oid::TEXT_ARRAY
            | oid::VARCHAR_ARRAY
            | oid::FLOAT4_ARRAY
            | oid::FLOAT8_ARRAY
            | oid::UUID_ARRAY
            | oid::JSONB_ARRAY
    )
}

// ==================== UUID Encoding/Decoding ====================

/// Encode a UUID string to 16-byte binary format for PostgreSQL wire protocol.
/// # Example
/// ```
/// use qail_pg::protocol::types::encode_uuid;
/// let bytes = encode_uuid("550e8400-e29b-41d4-a716-446655440000").unwrap();
/// assert_eq!(bytes.len(), 16);
/// ```
pub fn encode_uuid(uuid_str: &str) -> Result<[u8; 16], String> {
    // Remove hyphens and parse as hex
    let hex: String = uuid_str.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return Err(format!(
            "Invalid UUID length: expected 32 hex chars, got {}",
            hex.len()
        ));
    }

    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|e| format!("Invalid hex in UUID: {}", e))?;
    }
    Ok(bytes)
}

/// Decode 16-byte binary UUID from PostgreSQL to string format.
pub fn decode_uuid(bytes: &[u8]) -> Result<String, String> {
    if bytes.len() != 16 {
        return Err(format!(
            "Invalid UUID bytes length: expected 16, got {}",
            bytes.len()
        ));
    }

    Ok(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    ))
}

// ==================== JSON Encoding/Decoding ====================

/// Encode JSON value for PostgreSQL JSONB wire format (version byte + JSON text).
/// # Example
/// ```
/// use qail_pg::protocol::types::encode_jsonb;
/// let bytes = encode_jsonb(r#"{"key": "value"}"#);
/// assert_eq!(bytes[0], 1); // JSONB version byte
/// ```
pub fn encode_jsonb(json_str: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + json_str.len());
    buf.push(1); // JSONB version byte
    buf.extend_from_slice(json_str.as_bytes());
    buf
}

/// Decode PostgreSQL JSONB wire format to JSON string.
pub fn decode_jsonb(bytes: &[u8]) -> Result<String, String> {
    if bytes.is_empty() {
        return Ok(String::new());
    }
    // Skip version byte (first byte is JSONB version, usually 1)
    if bytes[0] != 1 {
        return Err(format!("Unsupported JSONB version: {}", bytes[0]));
    }
    String::from_utf8(bytes[1..].to_vec()).map_err(|e| format!("Invalid UTF-8 in JSONB: {}", e))
}

/// Encode plain JSON (not JSONB) - just the text.
pub fn encode_json(json_str: &str) -> Vec<u8> {
    json_str.as_bytes().to_vec()
}

/// Decode plain JSON from PostgreSQL.
pub fn decode_json(bytes: &[u8]) -> Result<String, String> {
    String::from_utf8(bytes.to_vec()).map_err(|e| format!("Invalid UTF-8 in JSON: {}", e))
}

// ==================== Array Encoding/Decoding ====================

/// Decode a PostgreSQL text-format array like {a,b,c} to Vec<String>.
/// This handles the common text-format arrays returned by PostgreSQL.
pub fn decode_text_array(s: &str) -> Vec<String> {
    if s.is_empty() || s == "{}" {
        return vec![];
    }

    // Remove outer braces
    let inner = s.trim_start_matches('{').trim_end_matches('}');
    if inner.is_empty() {
        return vec![];
    }

    // Split by comma, handling quoted elements
    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;

    for c in inner.chars() {
        if escape_next {
            current.push(c);
            escape_next = false;
            continue;
        }

        match c {
            '\\' => escape_next = true,
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                result.push(current.clone());
                current.clear();
            }
            _ => current.push(c),
        }
    }

    if !current.is_empty() {
        result.push(current);
    }

    result
}

/// Encode a Vec<String> to PostgreSQL text-format array {a,b,c}.
pub fn encode_text_array(items: &[String]) -> String {
    if items.is_empty() {
        return "{}".to_string();
    }

    let escaped: Vec<String> = items
        .iter()
        .map(|s| {
            if s.contains(',')
                || s.contains('"')
                || s.contains('\\')
                || s.contains('{')
                || s.contains('}')
            {
                format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
            } else {
                s.clone()
            }
        })
        .collect();

    format!("{{{}}}", escaped.join(","))
}

/// Decode a PostgreSQL text-format integer array to Vec<i64>.
pub fn decode_int_array(s: &str) -> Result<Vec<i64>, String> {
    decode_text_array(s)
        .into_iter()
        .map(|s| {
            s.parse::<i64>()
                .map_err(|e| format!("Invalid integer: {}", e))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oid_to_name() {
        assert_eq!(oid_to_name(oid::INT4), "int4");
        assert_eq!(oid_to_name(oid::UUID), "uuid");
        assert_eq!(oid_to_name(oid::JSONB), "jsonb");
        assert_eq!(oid_to_name(12345), "unknown");
    }

    #[test]
    fn test_is_array_oid() {
        assert!(is_array_oid(oid::INT4_ARRAY));
        assert!(is_array_oid(oid::UUID_ARRAY));
        assert!(!is_array_oid(oid::INT4));
        assert!(!is_array_oid(oid::UUID));
    }

    #[test]
    fn test_uuid_encode_decode() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let bytes = encode_uuid(uuid_str).unwrap();
        assert_eq!(bytes.len(), 16);

        let decoded = decode_uuid(&bytes).unwrap();
        assert_eq!(decoded, uuid_str);
    }

    #[test]
    fn test_jsonb_encode_decode() {
        let json = r#"{"key": "value"}"#;
        let bytes = encode_jsonb(json);
        assert_eq!(bytes[0], 1); // Version byte

        let decoded = decode_jsonb(&bytes).unwrap();
        assert_eq!(decoded, json);
    }

    #[test]
    fn test_text_array_decode() {
        assert_eq!(decode_text_array("{}"), Vec::<String>::new());
        assert_eq!(decode_text_array("{a,b,c}"), vec!["a", "b", "c"]);
        assert_eq!(
            decode_text_array("{\"hello, world\",foo}"),
            vec!["hello, world", "foo"]
        );
    }

    #[test]
    fn test_text_array_encode() {
        assert_eq!(encode_text_array(&[]), "{}");
        assert_eq!(
            encode_text_array(&["a".to_string(), "b".to_string()]),
            "{a,b}"
        );
    }

    #[test]
    fn test_int_array_decode() {
        assert_eq!(decode_int_array("{1,2,3}").unwrap(), vec![1, 2, 3]);
        assert_eq!(decode_int_array("{}").unwrap(), Vec::<i64>::new());
    }
}
