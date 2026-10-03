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

    /// Native variant whose value mapping (Rust type, decoder) also fits this
    /// exact raw type, e.g. `JSON` → `Jsonb`, `TIMESTAMP(3)` → `Timestamp`.
    /// Returns `self` unchanged when there is no closer native variant.
    pub fn native_family(&self) -> Self {
        let Self::Range(raw) = self else {
            return self.clone();
        };
        let upper = raw.to_ascii_uppercase();
        let base = upper.split('(').next().unwrap_or(&upper).trim();
        match base {
            "JSON" => Self::Jsonb,
            "REAL" => Self::Float,
            "TIMESTAMP" => Self::Timestamp,
            "TIMESTAMPTZ" => Self::Timestamptz,
            "TIME" => Self::Time,
            "CHARACTER" | "BPCHAR" => Self::Varchar(None),
            _ if base.starts_with("INTERVAL") => Self::Interval,
            _ => self.clone(),
        }
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
            let inner = inner.trim();
            if inner.is_empty() {
                return Err(());
            }
            let inner_ty = match inner.parse::<ColumnType>() {
                Ok(ty) => ty,
                Err(_) if is_custom_array_type_name(inner) => Self::Range(inner.to_uppercase()),
                Err(_) => return Err(()),
            };
            return Ok(Self::Array(Box::new(inner_ty)));
        }

        if let Some(inner) = lower
            .strip_prefix("varchar(")
            .and_then(|v| v.strip_suffix(')'))
        {
            let inner = inner.trim();
            if let Ok(len) = inner.parse::<u16>() {
                return Ok(Self::Varchar(Some(len)));
            }
            if inner.parse::<u32>().is_ok() {
                return Ok(Self::Range(format!("VARCHAR({inner})")));
            }
            return Err(());
        }

        if let Some(inner) = lower
            .strip_prefix("character varying(")
            .and_then(|v| v.strip_suffix(')'))
        {
            let inner = inner.trim();
            if let Ok(len) = inner.parse::<u16>() {
                return Ok(Self::Varchar(Some(len)));
            }
            if inner.parse::<u32>().is_ok() {
                return Ok(Self::Range(format!("VARCHAR({inner})")));
            }
            return Err(());
        }

        if let Some(inner) = lower
            .strip_prefix("char(")
            .and_then(|v| v.strip_suffix(')'))
            .or_else(|| {
                lower
                    .strip_prefix("character(")
                    .and_then(|v| v.strip_suffix(')'))
            })
        {
            let inner = inner.trim();
            if inner.parse::<u32>().is_ok() {
                return Ok(Self::Range(format!("CHARACTER({inner})")));
            }
            return Err(());
        }

        if let Some(inner) = lower
            .strip_prefix("decimal(")
            .and_then(|v| v.strip_suffix(')'))
        {
            let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
            if parts.len() == 2 {
                if let (Ok(p), Ok(s)) = (parts[0].parse::<u8>(), parts[1].parse::<u8>()) {
                    return Ok(Self::Decimal(Some((p, s))));
                }
                if parts[0].parse::<u16>().is_ok() && parts[1].parse::<u16>().is_ok() {
                    return Ok(Self::Range(format!("DECIMAL({},{})", parts[0], parts[1])));
                }
            }
            return Err(());
        }

        if let Some(inner) = lower
            .strip_prefix("numeric(")
            .and_then(|v| v.strip_suffix(')'))
        {
            let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
            if parts.len() == 2 {
                if let (Ok(p), Ok(s)) = (parts[0].parse::<u8>(), parts[1].parse::<u8>()) {
                    return Ok(Self::Decimal(Some((p, s))));
                }
                if parts[0].parse::<u16>().is_ok() && parts[1].parse::<u16>().is_ok() {
                    return Ok(Self::Range(format!("DECIMAL({},{})", parts[0], parts[1])));
                }
            }
            return Err(());
        }

        if let Some(exact) = parse_exact_raw_type(&lower) {
            return Ok(Self::Range(exact));
        }

        match lower.as_str() {
            "uuid" => Ok(Self::Uuid),
            "text" | "string" | "str" => Ok(Self::Text),
            "varchar" | "character varying" => Ok(Self::Varchar(None)),
            // Fixed-length character types are distinct from VARCHAR; bare
            // `char` / `character` is char(1) in PostgreSQL.
            "char" | "character" => Ok(Self::Range("CHARACTER(1)".to_string())),
            "bpchar" => Ok(Self::Range("BPCHAR".to_string())),
            "smallint" | "int2" => Ok(Self::Range("SMALLINT".to_string())),
            "int" | "integer" | "i32" | "int4" => Ok(Self::Int),
            "bigint" | "i64" | "int8" => Ok(Self::BigInt),
            "serial" => Ok(Self::Serial),
            "bigserial" => Ok(Self::BigSerial),
            "bool" | "boolean" => Ok(Self::Bool),
            "float" | "f64" | "double" | "double precision" | "float8" => Ok(Self::Float),
            // REAL (float4) and JSON keep their identity: they are not
            // DOUBLE PRECISION / JSONB.
            "real" | "float4" => Ok(Self::Range("REAL".to_string())),
            "decimal" | "numeric" | "dec" => Ok(Self::Decimal(None)),
            "jsonb" => Ok(Self::Jsonb),
            "json" => Ok(Self::Range("JSON".to_string())),
            "timestamp" | "timestamp without time zone" => Ok(Self::Timestamp),
            "timestamptz" | "timestamp with time zone" => Ok(Self::Timestamptz),
            "time" | "time without time zone" => Ok(Self::Time),
            "timetz" | "time with time zone" => Ok(Self::Range("TIMETZ".to_string())),
            "bit" => Ok(Self::Range("BIT".to_string())),
            "varbit" | "bit varying" => Ok(Self::Range("VARBIT".to_string())),
            "date" => Ok(Self::Date),
            "bytea" | "bytes" => Ok(Self::Bytea),
            "interval" => Ok(Self::Interval),
            "cidr" => Ok(Self::Cidr),
            "inet" => Ok(Self::Inet),
            "macaddr" => Ok(Self::MacAddr),
            // Built-in range and multirange types (period columns of temporal keys).
            "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange"
            | "int4multirange" | "int8multirange" | "nummultirange" | "tsmultirange"
            | "tstzmultirange" | "datemultirange" => Ok(Self::Range(lower.to_uppercase())),
            _ => Err(()),
        }
    }
}

/// Typmod'd types kept as exact raw contracts (lowercase input → canonical
/// uppercase spelling). Returns `None` for anything else.
fn parse_exact_raw_type(lower: &str) -> Option<String> {
    fn paren_number<'a>(s: &'a str, prefix: &str) -> Option<(&'a str, &'a str)> {
        let rest = s.strip_prefix(prefix)?.strip_prefix('(')?;
        let (num, tail) = rest.split_once(')')?;
        let num = num.trim();
        (!num.is_empty() && num.chars().all(|c| c.is_ascii_digit())).then_some((num, tail))
    }
    fn precision(num: &str) -> Option<&str> {
        num.parse::<u8>().ok().filter(|p| *p <= 6).map(|_| num)
    }

    if let Some((p, tail)) = paren_number(lower, "timestamp") {
        let p = precision(p)?;
        return match tail.trim() {
            "" | "without time zone" => Some(format!("TIMESTAMP({p})")),
            "with time zone" => Some(format!("TIMESTAMPTZ({p})")),
            _ => None,
        };
    }
    if let Some((p, "")) = paren_number(lower, "timestamptz") {
        return Some(format!("TIMESTAMPTZ({})", precision(p)?));
    }
    if let Some((p, tail)) = paren_number(lower, "time") {
        let p = precision(p)?;
        return match tail.trim() {
            "" | "without time zone" => Some(format!("TIME({p})")),
            "with time zone" => Some(format!("TIMETZ({p})")),
            _ => None,
        };
    }
    if let Some((p, "")) = paren_number(lower, "timetz") {
        return Some(format!("TIMETZ({})", precision(p)?));
    }
    if let Some((n, "")) = paren_number(lower, "bit") {
        return n
            .parse::<u32>()
            .ok()
            .filter(|n| *n > 0)
            .map(|_| format!("BIT({n})"));
    }
    if let Some((n, "")) =
        paren_number(lower, "varbit").or_else(|| paren_number(lower, "bit varying"))
    {
        return n
            .parse::<u32>()
            .ok()
            .filter(|n| *n > 0)
            .map(|_| format!("VARBIT({n})"));
    }
    if let Some((p, "")) = paren_number(lower, "interval") {
        return Some(format!("INTERVAL({})", precision(p)?));
    }
    if let Some(fields) = lower.strip_prefix("interval ") {
        const FIELDS: &[&str] = &[
            "year to month",
            "day to hour",
            "day to minute",
            "day to second",
            "hour to minute",
            "hour to second",
            "minute to second",
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
        ];
        let fields = fields.trim();
        let (fields, p) = match fields.strip_suffix(')').and_then(|f| f.rsplit_once('(')) {
            Some((f, p)) => (f.trim(), Some(precision(p.trim())?)),
            None => (fields, None),
        };
        if !FIELDS.contains(&fields) {
            return None;
        }
        // Only SECOND-ending fields accept a precision.
        if p.is_some() && !fields.ends_with("second") {
            return None;
        }
        let mut out = format!("INTERVAL {}", fields.to_ascii_uppercase());
        if let Some(p) = p {
            out.push_str(&format!("({p})"));
        }
        return Some(out);
    }
    None
}

fn is_custom_array_type_name(input: &str) -> bool {
    let mut parts = input.split('.');
    let Some(first) = parts.next() else {
        return false;
    };
    is_custom_type_ident(first) && parts.all(is_custom_type_ident)
}

fn is_custom_type_ident(input: &str) -> bool {
    let mut chars = input.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
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

    #[test]
    fn test_from_str_preserves_postgres_typmods_outside_native_width() {
        assert_eq!(
            "varchar(70000)".parse::<ColumnType>(),
            Ok(ColumnType::Range("VARCHAR(70000)".to_string()))
        );
        assert_eq!(
            "numeric(1000,2)".parse::<ColumnType>(),
            Ok(ColumnType::Range("DECIMAL(1000,2)".to_string()))
        );
        assert_eq!(
            "char(2)".parse::<ColumnType>(),
            Ok(ColumnType::Range("CHARACTER(2)".to_string()))
        );
    }

    #[test]
    fn array_type_parser_rejects_column_options_after_type() {
        assert!(
            "text[] not_null default '{}'::text[]"
                .parse::<ColumnType>()
                .is_err()
        );
    }

    #[test]
    fn array_type_parser_allows_custom_array_type_names() {
        assert_eq!(
            "ltree[]".parse::<ColumnType>(),
            Ok(ColumnType::Array(Box::new(ColumnType::Range(
                "LTREE".to_string()
            ))))
        );
    }
}
