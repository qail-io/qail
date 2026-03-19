//! Tenant Table Registry — tracks which tables require tenant-scope injection.
//!
//! Follows the same pattern as `RelationRegistry` for `join_on()`:
//! a global `RwLock<TenantRegistry>` loaded from `schema.qail` at startup.
//!
//! # Example
//! ```
//! use qail_core::rls::tenant::{register_tenant_table, lookup_tenant_column};
//!
//! register_tenant_table("orders", "tenant_id");
//! assert_eq!(lookup_tenant_column("orders"), Some("tenant_id".to_string()));
//! assert_eq!(lookup_tenant_column("migrations"), None);
//! ```

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::RwLock;

/// Registry of tables that participate in tenant-scope isolation.
///
/// Each entry maps a table name to its tenant column
/// (prefer `tenant_id`, fallback `operator_id` for legacy schemas).
#[derive(Debug, Default)]
pub struct TenantRegistry {
    tables: HashMap<String, String>,
}

impl TenantRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a table as tenant-scoped.
    ///
    /// # Arguments
    /// * `table` — table name (e.g., `"orders"`)
    /// * `column` — tenant column (e.g., `"tenant_id"`)
    pub fn register(&mut self, table: impl Into<String>, column: impl Into<String>) {
        self.tables.insert(table.into(), column.into());
    }

    /// Lookup the tenant column for a table.
    /// Returns `None` if the table is not tenant-scoped.
    pub fn get(&self, table: &str) -> Option<&str> {
        self.tables.get(table).map(|s| s.as_str())
    }

    /// Check if a table is tenant-scoped.
    pub fn is_tenant_table(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// Number of registered tenant tables.
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Returns true if no tables are registered.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Get all registered tenant tables.
    pub fn tables(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tables.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Load tenant tables from a parsed build::Schema.
    ///
    /// Scans all tables for columns named `tenant_id` first,
    /// then falls back to `operator_id` for legacy schemas.
    pub fn from_build_schema(schema: &crate::build::Schema) -> Self {
        let mut registry = Self::new();

        for table in schema.tables.values() {
            if table.columns.contains_key("tenant_id") {
                registry.register(&table.name, "tenant_id");
            } else if table.columns.contains_key("operator_id") {
                registry.register(&table.name, "operator_id");
            }
        }

        registry
    }
}

/// Global tenant registry, loaded at startup.
pub static TENANT_TABLES: LazyLock<RwLock<TenantRegistry>> =
    LazyLock::new(|| RwLock::new(TenantRegistry::new()));

/// Register a single table as tenant-scoped at runtime.
///
/// # Example
/// ```
/// use qail_core::rls::tenant::register_tenant_table;
/// register_tenant_table("orders", "tenant_id");
/// ```
pub fn register_tenant_table(table: &str, column: &str) {
    if let Ok(mut reg) = TENANT_TABLES.write() {
        reg.register(table, column);
    }
}

/// Lookup the tenant column for a table.
/// Returns `None` if not a tenant-scoped table.
///
/// # Example
/// ```
/// use qail_core::rls::tenant::{register_tenant_table, lookup_tenant_column};
/// register_tenant_table("orders", "tenant_id");
/// assert_eq!(lookup_tenant_column("orders"), Some("tenant_id".to_string()));
/// ```
pub fn lookup_tenant_column(table: &str) -> Option<String> {
    let registry = TENANT_TABLES.read().ok()?;
    registry.get(table).map(|s| s.to_string())
}

/// Load tenant tables from a schema.qail file.
/// Auto-detects tables with `tenant_id` columns first, then `operator_id`.
/// Returns the number of tenant tables found.
pub fn load_tenant_tables(path: &str) -> Result<usize, String> {
    let schema = crate::build::Schema::parse_file(path)?;
    let mut registry = TENANT_TABLES
        .write()
        .map_err(|e| format!("Lock error: {}", e))?;

    let mut count = 0;
    for table in schema.tables.values() {
        if table.columns.contains_key("tenant_id") {
            registry.register(&table.name, "tenant_id");
            count += 1;
        } else if table.columns.contains_key("operator_id") {
            registry.register(&table.name, "operator_id");
            count += 1;
        }
    }

    Ok(count)
}

/// Bulk-register multiple tenant tables at once.
///
/// Useful for application startup when you know the tenant tables.
///
/// # Example
/// ```
/// use qail_core::rls::tenant::register_tenant_tables;
/// register_tenant_tables(&[
///     ("orders", "tenant_id"),
///     ("bookings", "tenant_id"),
///     ("users", "tenant_id"),
/// ]);
/// ```
pub fn register_tenant_tables(tables: &[(&str, &str)]) {
    if let Ok(mut reg) = TENANT_TABLES.write() {
        for (table, column) in tables {
            reg.register(*table, *column);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_register_and_lookup() {
        let mut reg = TenantRegistry::new();
        reg.register("orders", "tenant_id");
        reg.register("bookings", "tenant_id");

        assert_eq!(reg.get("orders"), Some("tenant_id"));
        assert_eq!(reg.get("bookings"), Some("tenant_id"));
        assert_eq!(reg.get("migrations"), None);
    }

    #[test]
    fn test_registry_is_tenant_table() {
        let mut reg = TenantRegistry::new();
        reg.register("orders", "tenant_id");

        assert!(reg.is_tenant_table("orders"));
        assert!(!reg.is_tenant_table("users"));
    }

    #[test]
    fn test_registry_len() {
        let mut reg = TenantRegistry::new();
        assert!(reg.is_empty());

        reg.register("orders", "tenant_id");
        assert_eq!(reg.len(), 1);
        assert!(!reg.is_empty());
    }

    #[test]
    fn test_global_register_and_lookup() {
        // Use unique table names to avoid test interference
        register_tenant_table("_test_t1", "tenant_id");
        assert_eq!(
            lookup_tenant_column("_test_t1"),
            Some("tenant_id".to_string())
        );
        assert_eq!(lookup_tenant_column("_test_nonexistent"), None);

        // Clean up
        if let Ok(mut reg) = TENANT_TABLES.write() {
            *reg = TenantRegistry::new();
        }
    }

    #[test]
    fn test_bulk_register() {
        register_tenant_tables(&[("_test_bulk_a", "tenant_id"), ("_test_bulk_b", "tenant_id")]);

        assert_eq!(
            lookup_tenant_column("_test_bulk_a"),
            Some("tenant_id".to_string())
        );
        assert_eq!(
            lookup_tenant_column("_test_bulk_b"),
            Some("tenant_id".to_string())
        );

        // Clean up
        if let Ok(mut reg) = TENANT_TABLES.write() {
            *reg = TenantRegistry::new();
        }
    }

    #[test]
    fn test_from_build_schema_prefers_tenant_id() {
        let schema = crate::build::Schema::parse(
            r#"
table orders {
  id UUID
  tenant_id UUID
}

table legacy_bookings {
  id UUID
  operator_id UUID
}
"#,
        )
        .expect("schema should parse");

        let reg = TenantRegistry::from_build_schema(&schema);
        assert_eq!(reg.get("orders"), Some("tenant_id"));
        assert_eq!(reg.get("legacy_bookings"), Some("operator_id"));
    }
}
