use serde::Serialize;

/// Column metadata exposed to the gateway router
#[derive(Debug, Clone, Serialize)]
pub struct GatewayColumn {
    /// Column name.
    pub name: String,
    /// Qail-level type (e.g. `"string"`, `"int"`).
    pub col_type: String,
    /// PostgreSQL native type (e.g. `"text"`, `"int4"`).
    pub pg_type: String,
    /// Whether the column accepts NULL values.
    pub nullable: bool,
    /// Whether this column is (part of) the primary key.
    pub primary_key: bool,
    /// Whether the column has a UNIQUE constraint.
    pub unique: bool,
    /// Whether the column has a server-side DEFAULT.
    pub has_default: bool,
    /// Foreign key reference, if any.
    pub foreign_key: Option<GatewayForeignKey>,
}

/// Foreign key reference
#[derive(Debug, Clone, Serialize)]
pub struct GatewayForeignKey {
    /// Referenced table name.
    pub ref_table: String,
    /// Referenced column name.
    pub ref_column: String,
}

/// Table metadata for auto-REST generation
#[derive(Debug, Clone, Serialize)]
pub struct GatewayTable {
    /// Table name.
    pub name: String,
    /// Ordered list of column definitions.
    pub columns: Vec<GatewayColumn>,
    /// Name of the primary key column (if single-column PK)
    pub primary_key: Option<String>,
}

impl GatewayTable {
    /// Get column names suitable for SELECT
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get columns that can be set by the user (no auto-generated PKs, no defaults-only)
    pub fn insertable_columns(&self) -> Vec<&GatewayColumn> {
        self.columns
            .iter()
            .filter(|c| {
                // Skip serial/bigserial PKs — auto-generated
                if c.primary_key {
                    let t = c.pg_type.to_uppercase();
                    if t == "SERIAL" || t == "BIGSERIAL" {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    /// Get columns that are required (NOT NULL, no default)
    pub fn required_columns(&self) -> Vec<&GatewayColumn> {
        self.columns
            .iter()
            .filter(|c| !c.nullable && !c.has_default && !c.primary_key)
            .collect()
    }

    /// Get all foreign keys from this table → other tables
    pub fn foreign_keys(&self) -> Vec<(&str, &GatewayForeignKey)> {
        self.columns
            .iter()
            .filter_map(|c| c.foreign_key.as_ref().map(|fk| (c.name.as_str(), fk)))
            .collect()
    }
}
