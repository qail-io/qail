//! Compile-time Column Types
//!
//! Native AST types for schema definitions - NO runtime string parsing!

use std::fmt;

/// This replaces runtime strings with a compile-time enum, enabling:
/// - Type safety (no typos like "uuud" instead of "uuid")
/// - Compile-time validation (e.g., can this be a primary key?)
/// - Zero runtime parsing overhead
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    /// UUID.
    Uuid,
    /// TEXT.
    Text,
    /// VARCHAR with optional length.
    Varchar(Option<u16>),
    /// INTEGER (32-bit).
    Int,
    /// BIGINT (64-bit)
    BigInt,
    /// SERIAL (auto-incrementing 32-bit).
    Serial,
    /// BIGSERIAL (auto-incrementing 64-bit)
    BigSerial,
    /// BOOLEAN.
    Bool,
    /// DOUBLE PRECISION.
    Float,
    /// DECIMAL with optional (precision, scale).
    Decimal(Option<(u8, u8)>),
    /// JSONB.
    Jsonb,
    /// TIMESTAMP without timezone.
    Timestamp,
    /// TIMESTAMP with timezone
    Timestamptz,
    /// DATE.
    Date,
    /// TIME.
    Time,
    /// BYTEA.
    Bytea,
    // ==================== Phase 6: ARRAY/ENUM ====================
    /// ARRAY of inner type.
    Array(Box<ColumnType>),
    /// Custom ENUM type
    Enum {
        /// Enum type name.
        name: String,
        /// Allowed values.
        values: Vec<String>,
    },
    /// Range type.
    Range(String),
    /// INTERVAL.
    Interval,
    /// CIDR.
    Cidr,
    /// INET.
    Inet,
    /// MACADDR
    MacAddr,
}

impl ColumnType {
    /// Convert to PostgreSQL type string.
    /// This is the ONLY place where we convert to SQL strings.
    /// All builder logic works with the enum.
    pub fn to_pg_type(&self) -> String {
        match self {
            Self::Uuid => "UUID".to_string(),
            Self::Text => "TEXT".to_string(),
            Self::Varchar(None) => "VARCHAR".to_string(),
            Self::Varchar(Some(len)) => format!("VARCHAR({})", len),
            Self::Int => "INT".to_string(),
            Self::BigInt => "BIGINT".to_string(),
            Self::Serial => "SERIAL".to_string(),
            Self::BigSerial => "BIGSERIAL".to_string(),
            Self::Bool => "BOOLEAN".to_string(),
            Self::Float => "DOUBLE PRECISION".to_string(),
            Self::Decimal(None) => "DECIMAL".to_string(),
            Self::Decimal(Some((p, s))) => format!("DECIMAL({},{})", p, s),
            Self::Jsonb => "JSONB".to_string(),
            Self::Timestamp => "TIMESTAMP".to_string(),
            Self::Timestamptz => "TIMESTAMPTZ".to_string(),
            Self::Date => "DATE".to_string(),
            Self::Time => "TIME".to_string(),
            Self::Bytea => "BYTEA".to_string(),
            // Phase 6: ARRAY/ENUM
            Self::Array(inner) => format!("{}[]", inner.to_pg_type()),
            Self::Enum { name, .. } => name.clone(),
            Self::Range(name) => name.clone(),
            Self::Interval => "INTERVAL".to_string(),
            Self::Cidr => "CIDR".to_string(),
            Self::Inet => "INET".to_string(),
            Self::MacAddr => "MACADDR".to_string(),
        }
    }

    /// Check if this type can be a primary key.
    /// Compile-time validation: PKs must be scalar/indexable types.
    /// Container/blob-like types (JSONB, BYTEA, ARRAY, RANGE, INTERVAL) are rejected.
    pub const fn can_be_primary_key(&self) -> bool {
        matches!(
            self,
            Self::Uuid
                | Self::Text
                | Self::Varchar(_)
                | Self::Int
                | Self::BigInt
                | Self::Serial
                | Self::BigSerial
                | Self::Bool
                | Self::Float
                | Self::Decimal(_)
                | Self::Timestamp
                | Self::Timestamptz
                | Self::Date
                | Self::Time
                | Self::Enum { .. }
                | Self::Cidr
                | Self::Inet
                | Self::MacAddr
        )
    }

    /// Check if this type supports indexing.
    /// Most types support indexing except large binary/JSON types.
    pub const fn supports_indexing(&self) -> bool {
        !matches!(self, Self::Jsonb | Self::Bytea)
    }

    /// Check if this type requires a default value when NOT NULL.
    pub const fn requires_default_when_not_null(&self) -> bool {
        matches!(self, Self::Serial | Self::BigSerial)
    }

    /// Get a human-readable name for error messages.
    pub fn name(&self) -> &str {
        match self {
            Self::Uuid => "UUID",
            Self::Text => "TEXT",
            Self::Varchar(_) => "VARCHAR",
            Self::Int => "INT",
            Self::BigInt => "BIGINT",
            Self::Serial => "SERIAL",
            Self::BigSerial => "BIGSERIAL",
            Self::Bool => "BOOLEAN",
            Self::Float => "FLOAT",
            Self::Decimal(_) => "DECIMAL",
            Self::Jsonb => "JSONB",
            Self::Timestamp => "TIMESTAMP",
            Self::Timestamptz => "TIMESTAMPTZ",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::Bytea => "BYTEA",
            Self::Array(_) => "ARRAY",
            Self::Enum { .. } => "ENUM",
            Self::Range(_) => "RANGE",
            Self::Interval => "INTERVAL",
            Self::Cidr => "CIDR",
            Self::Inet => "INET",
            Self::MacAddr => "MACADDR",
        }
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_pg_type())
    }
}

/// Parse a string into ColumnType (for backward compatibility with .qail files).
/// This is ONLY used when parsing .qail text files, not in the builder API.
impl std::str::FromStr for ColumnType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let raw = s.trim();
        let lower = raw.to_lowercase();

        if let Some(inner) = lower.strip_suffix("[]") {
            let inner_ty = inner
                .parse::<ColumnType>()
                .unwrap_or_else(|_| Self::Range(inner.to_uppercase()));
            return Ok(Self::Array(Box::new(inner_ty)));
        }

        if let Some(inner) = lower
            .strip_prefix("varchar(")
            .and_then(|v| v.strip_suffix(')'))
        {
            if let Ok(len) = inner.trim().parse::<u16>() {
                return Ok(Self::Varchar(Some(len)));
            }
            return Err(());
        }

        if let Some(inner) = lower
            .strip_prefix("character varying(")
            .and_then(|v| v.strip_suffix(')'))
        {
            if let Ok(len) = inner.trim().parse::<u16>() {
                return Ok(Self::Varchar(Some(len)));
            }
            return Err(());
        }

        if let Some(inner) = lower
            .strip_prefix("decimal(")
            .and_then(|v| v.strip_suffix(')'))
        {
            let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
            if parts.len() == 2
                && let (Ok(p), Ok(s)) = (parts[0].parse::<u8>(), parts[1].parse::<u8>())
            {
                return Ok(Self::Decimal(Some((p, s))));
            }
            return Err(());
        }

        if let Some(inner) = lower
            .strip_prefix("numeric(")
            .and_then(|v| v.strip_suffix(')'))
        {
            let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
            if parts.len() == 2
                && let (Ok(p), Ok(s)) = (parts[0].parse::<u8>(), parts[1].parse::<u8>())
            {
                return Ok(Self::Decimal(Some((p, s))));
            }
            return Err(());
        }

        match lower.as_str() {
            "uuid" => Ok(Self::Uuid),
            "text" | "string" | "str" => Ok(Self::Text),
            "varchar" | "character varying" | "bpchar" | "char" | "character" => {
                Ok(Self::Varchar(None))
            }
            "smallint" | "int2" => Ok(Self::Range("SMALLINT".to_string())),
            "int" | "integer" | "i32" | "int4" => Ok(Self::Int),
            "bigint" | "i64" | "int8" => Ok(Self::BigInt),
            "serial" => Ok(Self::Serial),
            "bigserial" => Ok(Self::BigSerial),
            "bool" | "boolean" => Ok(Self::Bool),
            "float" | "f64" | "double" | "double precision" | "float8" | "real" | "float4" => {
                Ok(Self::Float)
            }
            "decimal" | "numeric" | "dec" => Ok(Self::Decimal(None)),
            "jsonb" | "json" => Ok(Self::Jsonb),
            "timestamp" => Ok(Self::Timestamp),
            "timestamptz" | "timestamp with time zone" => Ok(Self::Timestamptz),
            "time" | "time without time zone" => Ok(Self::Time),
            "date" => Ok(Self::Date),
            "bytea" | "bytes" => Ok(Self::Bytea),
            "interval" => Ok(Self::Interval),
            "cidr" => Ok(Self::Cidr),
            "inet" => Ok(Self::Inet),
            "macaddr" => Ok(Self::MacAddr),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pg_type() {
        assert_eq!(ColumnType::Uuid.to_pg_type(), "UUID");
        assert_eq!(ColumnType::Text.to_pg_type(), "TEXT");
        assert_eq!(ColumnType::Varchar(Some(255)).to_pg_type(), "VARCHAR(255)");
        assert_eq!(ColumnType::Serial.to_pg_type(), "SERIAL");
    }

    #[test]
    fn test_can_be_primary_key() {
        assert!(ColumnType::Uuid.can_be_primary_key());
        assert!(ColumnType::Text.can_be_primary_key());
        assert!(ColumnType::Varchar(Some(32)).can_be_primary_key());
        assert!(ColumnType::Serial.can_be_primary_key());
        assert!(ColumnType::Int.can_be_primary_key());
        assert!(ColumnType::Date.can_be_primary_key());
        assert!(!ColumnType::Jsonb.can_be_primary_key());
        assert!(!ColumnType::Bytea.can_be_primary_key());
        assert!(!ColumnType::Array(Box::new(ColumnType::Int)).can_be_primary_key());
    }

    #[test]
    fn test_supports_indexing() {
        assert!(ColumnType::Text.supports_indexing());
        assert!(ColumnType::Uuid.supports_indexing());
        assert!(!ColumnType::Jsonb.supports_indexing());
        assert!(!ColumnType::Bytea.supports_indexing());
    }

    #[test]
    fn test_from_str() {
        assert_eq!("uuid".parse::<ColumnType>(), Ok(ColumnType::Uuid));
        assert_eq!("TEXT".parse::<ColumnType>(), Ok(ColumnType::Text));
        assert_eq!("serial".parse::<ColumnType>(), Ok(ColumnType::Serial));
        assert_eq!(
            "int[]".parse::<ColumnType>(),
            Ok(ColumnType::Array(Box::new(ColumnType::Int)))
        );
        assert_eq!(
            "smallint".parse::<ColumnType>(),
            Ok(ColumnType::Range("SMALLINT".to_string()))
        );
        assert!("unknown".parse::<ColumnType>().is_err());
    }
}
