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
    pub const INT8: u32 = 20;   // bigint
    pub const INT2: u32 = 21;   // smallint
    pub const INT4: u32 = 23;   // integer
    
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
    matches!(oid, 
        oid::BOOL_ARRAY | oid::INT2_ARRAY | oid::INT4_ARRAY | oid::INT8_ARRAY |
        oid::TEXT_ARRAY | oid::VARCHAR_ARRAY | oid::FLOAT4_ARRAY | oid::FLOAT8_ARRAY |
        oid::UUID_ARRAY | oid::JSONB_ARRAY
    )
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
}
