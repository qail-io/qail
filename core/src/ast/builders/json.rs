//! JSON access builders for PostgreSQL JSONB operations.

use crate::ast::Expr;

/// JSON text access (column->>'key')
/// # Example
/// ```ignore
/// json("contact_info", "phone")  // contact_info->>'phone'
/// ```
pub fn json(column: &str, key: &str) -> JsonBuilder {
    JsonBuilder {
        column: column.to_string(),
        path_segments: vec![(key.to_string(), true)], // true = as text (->>)
        alias: None,
    }
}

/// JSON path access with multiple keys (column->'a'->'b'->>'c')
/// The last key extracts as text.
/// # Example
/// ```ignore
/// json_path("metadata", ["vessel_bookings", "0", "departure"])
/// // metadata->'vessel_bookings'->0->>'departure'
/// ```
pub fn json_path<S: AsRef<str>>(column: &str, keys: impl IntoIterator<Item = S>) -> JsonBuilder {
    let keys_vec: Vec<_> = keys.into_iter().collect();
    let len = keys_vec.len();
    let path_segments: Vec<(String, bool)> = keys_vec
        .into_iter()
        .enumerate()
        .map(|(i, k)| (k.as_ref().to_string(), i == len - 1)) // Last key as text
        .collect();

    JsonBuilder {
        column: column.to_string(),
        path_segments,
        alias: None,
    }
}

/// JSON object access (column->'key') - keeps as JSON, not text
pub fn json_obj(column: &str, key: &str) -> JsonBuilder {
    JsonBuilder {
        column: column.to_string(),
        path_segments: vec![(key.to_string(), false)], // false = as JSON (->)
        alias: None,
    }
}

/// Builder for JSON access expressions
#[derive(Debug, Clone)]
pub struct JsonBuilder {
    pub(crate) column: String,
    pub(crate) path_segments: Vec<(String, bool)>,
    pub(crate) alias: Option<String>,
}

impl JsonBuilder {
    /// Access another nested key as JSON (->)
    pub fn get(mut self, key: &str) -> Self {
        self.path_segments.push((key.to_string(), false));
        self
    }

    /// Access another nested key as text (->>)
    pub fn get_text(mut self, key: &str) -> Self {
        self.path_segments.push((key.to_string(), true));
        self
    }

    /// Add alias (AS name)
    pub fn alias(mut self, name: &str) -> Expr {
        self.alias = Some(name.to_string());
        self.build()
    }

    /// Build the final Expr
    pub fn build(self) -> Expr {
        Expr::JsonAccess {
            column: self.column,
            path_segments: self.path_segments,
            alias: self.alias,
        }
    }
}

impl From<JsonBuilder> for Expr {
    fn from(builder: JsonBuilder) -> Self {
        builder.build()
    }
}
