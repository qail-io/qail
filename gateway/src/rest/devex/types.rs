use serde_json::{Value, json};

/// Convert a PG type string to its TypeScript equivalent.
pub(super) fn pg_type_to_ts(pg_type: &str, nullable: bool) -> String {
    let base = match pg_type.to_uppercase().as_str() {
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "CHARACTER" | "CITEXT" | "NAME" => {
            "string"
        }
        "UUID" => "string",
        "INT2" | "INT4" | "INT8" | "SMALLINT" | "INTEGER" | "BIGINT" | "SERIAL" | "BIGSERIAL"
        | "SMALLSERIAL" => "number",
        "FLOAT4" | "FLOAT8" | "REAL" | "DOUBLE PRECISION" | "DECIMAL" | "NUMERIC" | "MONEY" => {
            "number"
        }
        "BOOL" | "BOOLEAN" => "boolean",
        "JSON" | "JSONB" => "Record<string, unknown>",
        "TIMESTAMPTZ"
        | "TIMESTAMP"
        | "DATE"
        | "TIME"
        | "TIMETZ"
        | "INTERVAL"
        | "TIMESTAMP WITHOUT TIME ZONE"
        | "TIMESTAMP WITH TIME ZONE" => "string",
        "BYTEA" => "string",
        "INET" | "CIDR" | "MACADDR" => "string",
        t if t.ends_with("[]") => {
            let inner = &t[..t.len() - 2];
            let inner_ts = pg_type_to_ts_base(inner);
            return if nullable {
                format!("{}[] | null", inner_ts)
            } else {
                format!("{}[]", inner_ts)
            };
        }
        _ => "unknown",
    };

    if nullable {
        format!("{} | null", base)
    } else {
        base.to_string()
    }
}

/// Map PG type to TS type (non-nullable, for array inner types).
fn pg_type_to_ts_base(pg_type: &str) -> &'static str {
    match pg_type {
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "UUID" | "CITEXT" | "NAME"
        | "TIMESTAMPTZ" | "TIMESTAMP" | "DATE" | "TIME" | "TIMETZ" | "INTERVAL" | "BYTEA"
        | "INET" | "CIDR" | "MACADDR" => "string",
        "INT2" | "INT4" | "INT8" | "SMALLINT" | "INTEGER" | "BIGINT" | "SERIAL" | "BIGSERIAL"
        | "FLOAT4" | "FLOAT8" | "REAL" | "DECIMAL" | "NUMERIC" | "MONEY" => "number",
        "BOOL" | "BOOLEAN" => "boolean",
        "JSON" | "JSONB" => "Record<string, unknown>",
        _ => "unknown",
    }
}

/// Convert snake_case to PascalCase (e.g., "order_items" → "OrderItems").
pub(super) fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// Map PostgreSQL types to OpenAPI 3.0 types.
pub(super) fn pg_type_to_openapi(pg_type: &str) -> Value {
    match pg_type.to_uppercase().as_str() {
        "INT2" | "INT4" | "SMALLINT" | "INTEGER" | "SERIAL" => {
            json!({"type": "integer", "format": "int32"})
        }
        "INT8" | "BIGINT" | "BIGSERIAL" => json!({"type": "integer", "format": "int64"}),
        "FLOAT4" | "REAL" => json!({"type": "number", "format": "float"}),
        "FLOAT8" | "DOUBLE PRECISION" | "NUMERIC" | "DECIMAL" => {
            json!({"type": "number", "format": "double"})
        }
        "BOOL" | "BOOLEAN" => json!({"type": "boolean"}),
        "UUID" => json!({"type": "string", "format": "uuid"}),
        "TIMESTAMPTZ" | "TIMESTAMP" => json!({"type": "string", "format": "date-time"}),
        "DATE" => json!({"type": "string", "format": "date"}),
        "JSON" | "JSONB" => json!({"type": "object"}),
        "TEXT[]" | "VARCHAR[]" => json!({"type": "array", "items": {"type": "string"}}),
        "INT4[]" | "INT8[]" => json!({"type": "array", "items": {"type": "integer"}}),
        _ => json!({"type": "string"}),
    }
}
