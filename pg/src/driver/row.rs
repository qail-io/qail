//! PostgreSQL Row Helpers
//!
//! Provides convenient methods to extract typed values from row data.
//! Supports both text and binary result formats when column metadata is available.

use super::PgRow;
use crate::types::{FromPg, TypeError};

/// Trait for types that can be constructed from a database row.
///
/// Implement this trait on your structs to enable typed fetching:
/// ```ignore
/// impl QailRow for User {
///     fn columns() -> &'static [&'static str] {
///         &["id", "name", "email"]
///     }
///     
///     fn from_row(row: &PgRow) -> Self {
///         User {
///             // Metadata-aware decoders (OID + text/binary format).
///             id: row
///                 .try_get_by_name::<qail_pg::Uuid>("id")
///                 .map(|v| v.0)
///                 .unwrap_or_default(),
///             name: row.try_get_by_name::<String>("name").unwrap_or_default(),
///             email: row.try_get_by_name::<String>("email").unwrap_or_default(),
///         }
///     }
/// }
///
/// // Then use:
/// let users: Vec<User> = driver.fetch_typed::<User>(&query).await?;
/// ```
pub trait QailRow: Sized {
    /// Return the column names this struct expects.
    /// These are used to automatically build SELECT queries.
    fn columns() -> &'static [&'static str];

    /// Construct an instance from a PgRow.
    /// Column indices match the order returned by `columns()`.
    fn from_row(row: &PgRow) -> Self;
}

impl PgRow {
    /// Decode a non-null column into any `FromPg` type using backend OID/format metadata.
    ///
    /// Returns:
    /// - `TypeError::UnexpectedNull` if the column value is NULL
    /// - `TypeError::InvalidData` if the column index is out of bounds or metadata is missing
    /// - other `TypeError` variants from the target decoder
    pub fn try_get<T: FromPg>(&self, idx: usize) -> Result<T, TypeError> {
        let cell = self
            .columns
            .get(idx)
            .ok_or_else(|| TypeError::InvalidData(format!("Column index {} out of bounds", idx)))?;

        let bytes = cell.as_deref().ok_or(TypeError::UnexpectedNull)?;
        let (oid, format) = self.column_type_meta(idx)?;
        T::from_pg(bytes, oid, format)
    }

    /// Decode a possibly-null column into `Option<T>` using backend OID/format metadata.
    ///
    /// Returns `Ok(None)` when the column is SQL NULL.
    pub fn try_get_opt<T: FromPg>(&self, idx: usize) -> Result<Option<T>, TypeError> {
        let cell = self
            .columns
            .get(idx)
            .ok_or_else(|| TypeError::InvalidData(format!("Column index {} out of bounds", idx)))?;

        match cell {
            None => Ok(None),
            Some(bytes) => {
                let (oid, format) = self.column_type_meta(idx)?;
                Ok(Some(T::from_pg(bytes, oid, format)?))
            }
        }
    }

    /// Decode a non-null column by name into any `FromPg` type.
    pub fn try_get_by_name<T: FromPg>(&self, name: &str) -> Result<T, TypeError> {
        let idx = self
            .column_index(name)
            .ok_or_else(|| TypeError::InvalidData(format!("Unknown column name '{}'", name)))?;
        self.try_get(idx)
    }

    /// Decode a possibly-null column by name into `Option<T>`.
    pub fn try_get_opt_by_name<T: FromPg>(&self, name: &str) -> Result<Option<T>, TypeError> {
        let idx = self
            .column_index(name)
            .ok_or_else(|| TypeError::InvalidData(format!("Unknown column name '{}'", name)))?;
        self.try_get_opt(idx)
    }

    fn column_type_meta(&self, idx: usize) -> Result<(u32, i16), TypeError> {
        let info = self.column_info.as_ref().ok_or_else(|| {
            TypeError::InvalidData(
                "Column metadata unavailable; use query APIs that preserve RowDescription"
                    .to_string(),
            )
        })?;

        let oid = info
            .oids
            .get(idx)
            .copied()
            .ok_or_else(|| TypeError::InvalidData(format!("Missing OID for column {}", idx)))?;
        let format = info.formats.get(idx).copied().ok_or_else(|| {
            TypeError::InvalidData(format!("Missing format code for column {}", idx))
        })?;
        Ok((oid, format))
    }

    /// Get a column value as String.
    /// Returns None if the value is NULL or invalid UTF-8.
    pub fn get_string(&self, idx: usize) -> Option<String> {
        self.columns
            .get(idx)?
            .as_ref()
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
    }

    /// Get a column value as i32.
    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        if self.column_info.is_some()
            && let Ok(v) = self.try_get::<i32>(idx)
        {
            return Some(v);
        }
        let bytes = self.columns.get(idx)?.as_ref()?;
        std::str::from_utf8(bytes).ok()?.parse().ok()
    }

    /// Get a column value as i64.
    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        if self.column_info.is_some()
            && let Ok(v) = self.try_get::<i64>(idx)
        {
            return Some(v);
        }
        let bytes = self.columns.get(idx)?.as_ref()?;
        std::str::from_utf8(bytes).ok()?.parse().ok()
    }

    /// Get a column value as f64.
    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        if self.column_info.is_some()
            && let Ok(v) = self.try_get::<f64>(idx)
        {
            return Some(v);
        }
        let bytes = self.columns.get(idx)?.as_ref()?;
        std::str::from_utf8(bytes).ok()?.parse().ok()
    }

    /// Get a column value as bool.
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        if self.column_info.is_some()
            && let Ok(v) = self.try_get::<bool>(idx)
        {
            return Some(v);
        }
        let bytes = self.columns.get(idx)?.as_ref()?;
        let s = std::str::from_utf8(bytes).ok()?;
        match s {
            "t" | "true" | "1" => Some(true),
            "f" | "false" | "0" => Some(false),
            _ => None,
        }
    }

    /// Check if a column is NULL.
    pub fn is_null(&self, idx: usize) -> bool {
        self.columns.get(idx).map(|v| v.is_none()).unwrap_or(true)
    }

    /// Get raw bytes of a column.
    pub fn get_bytes(&self, idx: usize) -> Option<&[u8]> {
        self.columns.get(idx)?.as_ref().map(|v| v.as_slice())
    }

    /// Get number of columns in the row.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Check if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get a column value as UUID string.
    /// Handles both text format (36-char string) and binary format (16 bytes).
    pub fn get_uuid(&self, idx: usize) -> Option<String> {
        let bytes = self.columns.get(idx)?.as_ref()?;

        if bytes.len() == 16 {
            // Binary format - decode 16 bytes
            use crate::protocol::types::decode_uuid;
            decode_uuid(bytes).ok()
        } else {
            // Text format - return as-is
            String::from_utf8(bytes.clone()).ok()
        }
    }

    /// Get a column value as JSON string.
    /// Handles both JSON (text) and JSONB (version byte prefix) formats.
    pub fn get_json(&self, idx: usize) -> Option<String> {
        let bytes = self.columns.get(idx)?.as_ref()?;

        if bytes.is_empty() {
            return Some(String::new());
        }

        // JSONB has version byte (1) as first byte
        if bytes[0] == 1 && bytes.len() > 1 {
            String::from_utf8(bytes[1..].to_vec()).ok()
        } else {
            String::from_utf8(bytes.clone()).ok()
        }
    }

    /// Get a column value as timestamp string (ISO 8601 format).
    pub fn get_timestamp(&self, idx: usize) -> Option<String> {
        let bytes = self.columns.get(idx)?.as_ref()?;
        String::from_utf8(bytes.clone()).ok()
    }

    /// Get a column value as text array.
    pub fn get_text_array(&self, idx: usize) -> Option<Vec<String>> {
        let bytes = self.columns.get(idx)?.as_ref()?;
        let s = std::str::from_utf8(bytes).ok()?;
        Some(crate::protocol::types::decode_text_array(s))
    }

    /// Get a column value as integer array.
    pub fn get_int_array(&self, idx: usize) -> Option<Vec<i64>> {
        let bytes = self.columns.get(idx)?.as_ref()?;
        let s = std::str::from_utf8(bytes).ok()?;
        crate::protocol::types::decode_int_array(s).ok()
    }

    // ==================== ERGONOMIC SHORTCUTS ====================
    // These methods reduce boilerplate by providing sensible defaults

    /// Get string, defaulting to empty string if NULL.
    /// Ergonomic shortcut: `row.text(0)` instead of `row.get_string(0).unwrap_or_default()`
    pub fn text(&self, idx: usize) -> String {
        self.get_string(idx).unwrap_or_default()
    }

    /// Get string with custom default if NULL.
    /// Example: `row.text_or(1, "Unknown")`
    pub fn text_or(&self, idx: usize, default: &str) -> String {
        self.get_string(idx).unwrap_or_else(|| default.to_string())
    }

    /// Get i64, defaulting to 0 if NULL.
    /// Ergonomic shortcut: `row.int(4)` instead of `row.get_i64(4).unwrap_or(0)`
    pub fn int(&self, idx: usize) -> i64 {
        self.get_i64(idx).unwrap_or(0)
    }

    /// Get f64, defaulting to 0.0 if NULL.
    pub fn float(&self, idx: usize) -> f64 {
        self.get_f64(idx).unwrap_or(0.0)
    }

    /// Get bool, defaulting to false if NULL.
    pub fn boolean(&self, idx: usize) -> bool {
        self.get_bool(idx).unwrap_or(false)
    }

    /// Parse timestamp as DateTime<Utc>.
    /// Handles PostgreSQL timestamp formats automatically.
    #[cfg(feature = "chrono")]
    pub fn datetime(&self, idx: usize) -> Option<chrono::DateTime<chrono::Utc>> {
        if let Ok(dt) = self.try_get::<chrono::DateTime<chrono::Utc>>(idx) {
            return Some(dt);
        }

        let s = self.get_timestamp(idx)?;
        // Try parsing various PostgreSQL timestamp formats
        chrono::DateTime::parse_from_rfc3339(&s.replace(' ', "T"))
            .ok()
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .or_else(|| {
                // Try PostgreSQL format: "2024-01-01 12:00:00.123456+00"
                chrono::DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f%#z")
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
            })
    }

    /// Parse UUID column as uuid::Uuid type.
    #[cfg(feature = "uuid")]
    pub fn uuid_typed(&self, idx: usize) -> Option<uuid::Uuid> {
        self.try_get::<uuid::Uuid>(idx).ok().or_else(|| {
            self.get_uuid(idx)
                .and_then(|s| uuid::Uuid::parse_str(&s).ok())
        })
    }

    // ==================== GET BY COLUMN NAME ====================

    /// Get column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_info.as_ref()?.name_to_index.get(name).copied()
    }

    /// Get a String column by name.
    pub fn get_string_by_name(&self, name: &str) -> Option<String> {
        self.get_string(self.column_index(name)?)
    }

    /// Get an i32 column by name.
    pub fn get_i32_by_name(&self, name: &str) -> Option<i32> {
        self.get_i32(self.column_index(name)?)
    }

    /// Get an i64 column by name.
    pub fn get_i64_by_name(&self, name: &str) -> Option<i64> {
        self.get_i64(self.column_index(name)?)
    }

    /// Get a f64 column by name.
    pub fn get_f64_by_name(&self, name: &str) -> Option<f64> {
        self.get_f64(self.column_index(name)?)
    }

    /// Get a bool column by name.
    pub fn get_bool_by_name(&self, name: &str) -> Option<bool> {
        self.get_bool(self.column_index(name)?)
    }

    /// Get a UUID column by name.
    pub fn get_uuid_by_name(&self, name: &str) -> Option<String> {
        self.get_uuid(self.column_index(name)?)
    }

    /// Get a JSON column by name.
    pub fn get_json_by_name(&self, name: &str) -> Option<String> {
        self.get_json(self.column_index(name)?)
    }

    /// Check if a column is NULL by name.
    pub fn is_null_by_name(&self, name: &str) -> bool {
        self.column_index(name)
            .map(|idx| self.is_null(idx))
            .unwrap_or(true)
    }

    /// Get a timestamp column by name.
    pub fn get_timestamp_by_name(&self, name: &str) -> Option<String> {
        self.get_timestamp(self.column_index(name)?)
    }

    /// Get a text array column by name.
    pub fn get_text_array_by_name(&self, name: &str) -> Option<Vec<String>> {
        self.get_text_array(self.column_index(name)?)
    }

    /// Get an integer array column by name.
    pub fn get_int_array_by_name(&self, name: &str) -> Option<Vec<i64>> {
        self.get_int_array(self.column_index(name)?)
    }

    // ==================== ERGONOMIC BY-NAME SHORTCUTS ====================
    // These mirror the positional shortcuts (text, boolean, int, etc.)
    // but use column names — safe with RETURNING * regardless of column order.

    /// Get string by column name, defaulting to empty string.
    /// Example: `row.text_by_name("name")` instead of `row.get_string_by_name("name").unwrap_or_default()`
    pub fn text_by_name(&self, name: &str) -> String {
        self.get_string_by_name(name).unwrap_or_default()
    }

    /// Get bool by column name, defaulting to false.
    pub fn boolean_by_name(&self, name: &str) -> bool {
        self.get_bool_by_name(name).unwrap_or(false)
    }

    /// Get i64 by column name, defaulting to 0.
    pub fn int_by_name(&self, name: &str) -> i64 {
        self.get_i64_by_name(name).unwrap_or(0)
    }

    /// Get f64 by column name, defaulting to 0.0.
    pub fn float_by_name(&self, name: &str) -> f64 {
        self.get_f64_by_name(name).unwrap_or(0.0)
    }

    /// Parse timestamp by column name as DateTime<Utc>.
    #[cfg(feature = "chrono")]
    pub fn datetime_by_name(&self, name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        self.datetime(self.column_index(name)?)
    }

    /// Parse UUID by column name as uuid::Uuid type.
    #[cfg(feature = "uuid")]
    pub fn uuid_typed_by_name(&self, name: &str) -> Option<uuid::Uuid> {
        self.uuid_typed(self.column_index(name)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::oid;
    use crate::types::{Json, Uuid};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn single_col_info(name: &str, oid: u32, format: i16) -> Arc<super::super::ColumnInfo> {
        let mut name_to_index = HashMap::new();
        name_to_index.insert(name.to_string(), 0);
        Arc::new(super::super::ColumnInfo {
            name_to_index,
            oids: vec![oid],
            formats: vec![format],
        })
    }

    #[test]
    fn test_get_string() {
        let row = PgRow {
            columns: vec![Some(b"hello".to_vec()), None, Some(b"world".to_vec())],
            column_info: None,
        };

        assert_eq!(row.get_string(0), Some("hello".to_string()));
        assert_eq!(row.get_string(1), None);
        assert_eq!(row.get_string(2), Some("world".to_string()));
    }

    #[test]
    fn test_get_i32() {
        let row = PgRow {
            columns: vec![
                Some(b"42".to_vec()),
                Some(b"-123".to_vec()),
                Some(b"not_a_number".to_vec()),
            ],
            column_info: None,
        };

        assert_eq!(row.get_i32(0), Some(42));
        assert_eq!(row.get_i32(1), Some(-123));
        assert_eq!(row.get_i32(2), None);
    }

    #[test]
    fn test_get_bool() {
        let row = PgRow {
            columns: vec![
                Some(b"t".to_vec()),
                Some(b"f".to_vec()),
                Some(b"true".to_vec()),
                Some(b"false".to_vec()),
            ],
            column_info: None,
        };

        assert_eq!(row.get_bool(0), Some(true));
        assert_eq!(row.get_bool(1), Some(false));
        assert_eq!(row.get_bool(2), Some(true));
        assert_eq!(row.get_bool(3), Some(false));
    }

    #[test]
    fn test_is_null() {
        let row = PgRow {
            columns: vec![Some(b"value".to_vec()), None],
            column_info: None,
        };

        assert!(!row.is_null(0));
        assert!(row.is_null(1));
        assert!(row.is_null(99)); // Out of bounds
    }

    #[test]
    fn test_try_get_i64_binary() {
        let row = PgRow {
            columns: vec![Some(42i64.to_be_bytes().to_vec())],
            column_info: Some(single_col_info("count", oid::INT8, 1)),
        };

        let value: i64 = row.try_get(0).unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn test_try_get_i64_text_by_name() {
        let row = PgRow {
            columns: vec![Some(b"123".to_vec())],
            column_info: Some(single_col_info("total", oid::INT8, 0)),
        };

        let value: i64 = row.try_get_by_name("total").unwrap();
        assert_eq!(value, 123);
    }

    #[test]
    fn test_try_get_opt_null() {
        let row = PgRow {
            columns: vec![None],
            column_info: Some(single_col_info("maybe_count", oid::INT8, 1)),
        };

        let value: Option<i64> = row.try_get_opt(0).unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_try_get_unexpected_null() {
        let row = PgRow {
            columns: vec![None],
            column_info: Some(single_col_info("required_count", oid::INT8, 1)),
        };

        assert!(matches!(
            row.try_get::<i64>(0),
            Err(TypeError::UnexpectedNull)
        ));
    }

    #[test]
    fn test_try_get_uuid_binary() {
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        let row = PgRow {
            columns: vec![Some(uuid_bytes.to_vec())],
            column_info: Some(single_col_info("id", oid::UUID, 1)),
        };

        let value: Uuid = row.try_get(0).unwrap();
        assert_eq!(value.0, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_try_get_jsonb_binary() {
        let mut bytes = vec![1u8];
        bytes.extend_from_slice(br#"{"ok":true}"#);
        let row = PgRow {
            columns: vec![Some(bytes)],
            column_info: Some(single_col_info("meta", oid::JSONB, 1)),
        };

        let value: Json = row.try_get(0).unwrap();
        assert_eq!(value.0, r#"{"ok":true}"#);
    }

    #[test]
    fn test_try_get_requires_column_metadata() {
        let row = PgRow {
            columns: vec![Some(b"42".to_vec())],
            column_info: None,
        };

        assert!(matches!(
            row.try_get::<i64>(0),
            Err(TypeError::InvalidData(msg)) if msg.contains("metadata")
        ));
    }

    #[test]
    fn test_get_i64_uses_metadata_binary() {
        let row = PgRow {
            columns: vec![Some(777i64.to_be_bytes().to_vec())],
            column_info: Some(single_col_info("v", oid::INT8, 1)),
        };
        assert_eq!(row.get_i64(0), Some(777));
    }

    #[test]
    fn test_get_bool_uses_metadata_binary() {
        let row = PgRow {
            columns: vec![Some(vec![1u8])],
            column_info: Some(single_col_info("flag", oid::BOOL, 1)),
        };
        assert_eq!(row.get_bool(0), Some(true));
    }
}
