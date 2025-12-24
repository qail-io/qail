//! PostgreSQL Row Helpers
//!
//! Provides convenient methods to extract typed values from row data.
//! PostgreSQL Simple Query protocol returns all values as text format.

use super::PgRow;

impl PgRow {
    /// Get a column value as String.
    pub fn get_string(&self, idx: usize) -> Option<String> {
        self.columns.get(idx)?
            .as_ref()
            .map(|bytes| String::from_utf8_lossy(bytes).to_string())
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_string() {
        let row = PgRow {
            columns: vec![
                Some(b"hello".to_vec()),
                None,
                Some(b"world".to_vec()),
            ],
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
        };

        assert_eq!(row.get_bool(0), Some(true));
        assert_eq!(row.get_bool(1), Some(false));
        assert_eq!(row.get_bool(2), Some(true));
        assert_eq!(row.get_bool(3), Some(false));
    }

    #[test]
    fn test_is_null() {
        let row = PgRow {
            columns: vec![
                Some(b"value".to_vec()),
                None,
            ],
        };

        assert!(!row.is_null(0));
        assert!(row.is_null(1));
        assert!(row.is_null(99)); // Out of bounds
    }
}
