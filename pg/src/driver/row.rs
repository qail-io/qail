//! PostgreSQL Row Helpers
//!
//! Provides convenient methods to extract typed values from row data.
//! PostgreSQL Simple Query protocol returns all values as text format.

use super::PgRow;

impl PgRow {
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
        let bytes = self.columns.get(idx)?.as_ref()?;
        std::str::from_utf8(bytes).ok()?.parse().ok()
    }

    /// Get a column value as i64.
    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        let bytes = self.columns.get(idx)?.as_ref()?;
        std::str::from_utf8(bytes).ok()?.parse().ok()
    }

    /// Get a column value as f64.
    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        let bytes = self.columns.get(idx)?.as_ref()?;
        std::str::from_utf8(bytes).ok()?.parse().ok()
    }

    /// Get a column value as bool.
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
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
        self.get_uuid(idx).and_then(|s| uuid::Uuid::parse_str(&s).ok())
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
