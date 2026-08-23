//! Owner Table Registry — tracks which tables require user-scope injection.
//!
//! The tenant registry ([`super::tenant`]) answers "which column holds the
//! tenant?". This registry answers "which column holds the owning user?" for
//! consumer-shaped tables (marketplace listings, chat messages, profiles)
//! where isolation is `owner_col = app.current_user_id` rather than
//! `tenant_id = app.current_tenant_id`.
//!
//! Owner columns are **declared**, never inferred: a column named `user_id`
//! or `seller_id` does nothing until the table carries `owner <column>` in
//! `schema.qail`. Inferring by name would turn a foreign key into an
//! isolation boundary by accident.
//!
//! # Example
//! ```
//! use qail_core::rls::owner::{register_owner_table, lookup_owner_column};
//!
//! register_owner_table("listings", "seller_id");
//! assert_eq!(lookup_owner_column("listings"), Some("seller_id".to_string()));
//! assert_eq!(lookup_owner_column("migrations"), None);
//! ```

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::RwLock;

/// Registry of tables that participate in owner-scope isolation.
#[derive(Debug, Default)]
pub struct OwnerRegistry {
    tables: HashMap<String, String>,
}

impl OwnerRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a table as owner-scoped.
    pub fn register(&mut self, table: impl Into<String>, column: impl Into<String>) {
        self.tables.insert(table.into(), column.into());
    }

    /// Lookup the owner column for a table.
    pub fn get(&self, table: &str) -> Option<&str> {
        self.tables.get(table).map(|s| s.as_str())
    }

    /// Check if a table is owner-scoped.
    pub fn is_owner_table(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// Number of registered owner tables.
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Returns true if no tables are registered.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Get all registered owner tables.
    pub fn tables(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tables.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Load owner tables from a parsed build::Schema.
    ///
    /// Only tables with an explicit `owner <column>` declaration register.
    pub fn from_build_schema(schema: &crate::build::Schema) -> Self {
        let mut registry = Self::new();
        for table in schema.tables.values() {
            if let Some(column) = table.owner_column.as_deref() {
                registry.register(&table.name, column);
            }
        }
        registry
    }
}

/// Global owner registry. Private for the same reason as the tenant
/// registry: no external path may leave the process `Initialized` with no
/// scoping metadata.
static OWNER_TABLES: LazyLock<RwLock<OwnerRegistry>> =
    LazyLock::new(|| RwLock::new(OwnerRegistry::new()));

/// Register a single table as owner-scoped at runtime. Mode-neutral.
pub fn register_owner_table(table: &str, column: &str) {
    if let Ok(mut reg) = OWNER_TABLES.write() {
        reg.register(table, column);
    }
}

/// Fallible bulk registration against an explicit lock. A poisoned lock is
/// an error — never silently discarded. Parameterised on the lock so a test
/// can poison a local one.
pub(crate) fn register_into(
    lock: &RwLock<OwnerRegistry>,
    tables: &[(&str, &str)],
) -> Result<usize, String> {
    let mut reg = lock
        .write()
        .map_err(|e| format!("owner registry lock poisoned: {}", e))?;
    for (table, column) in tables {
        reg.register(*table, *column);
    }
    Ok(tables.len())
}

/// Fallible count against an explicit lock. Never reports a poisoned
/// registry as empty.
pub(crate) fn count_in(lock: &RwLock<OwnerRegistry>) -> Result<usize, String> {
    lock.read()
        .map(|r| r.len())
        .map_err(|e| format!("owner registry lock poisoned: {}", e))
}

/// Bulk-register into the process registry, propagating a poisoned lock.
pub(crate) fn try_register_owner_tables(tables: &[(&str, &str)]) -> Result<usize, String> {
    register_into(&OWNER_TABLES, tables)
}

/// Number of owner-registered tables, or an error if the lock is poisoned.
pub(crate) fn try_owner_table_count() -> Result<usize, String> {
    count_in(&OWNER_TABLES)
}

/// Lookup the owner column for a table.
/// Returns `None` if not an owner-scoped table.
pub fn lookup_owner_column(table: &str) -> Option<String> {
    try_lookup_owner_column(table).ok().flatten()
}

/// Fallible lookup: `Ok(None)` = not registered, `Err` = registry cannot be
/// read. `Qail::with_rls` MUST use this form (see the tenant counterpart).
pub fn try_lookup_owner_column(table: &str) -> Result<Option<String>, String> {
    lookup_in(&OWNER_TABLES, table)
}

pub(crate) fn lookup_in(
    lock: &RwLock<OwnerRegistry>,
    table: &str,
) -> Result<Option<String>, String> {
    let registry = lock
        .read()
        .map_err(|e| format!("owner registry lock poisoned: {}", e))?;
    Ok(registry.get(table).map(|s| s.to_string()))
}

/// Load owner tables from a `schema.qail` file into the global registry.
///
/// Returns the number of tables registered.
pub fn load_owner_tables(path: &str) -> Result<usize, String> {
    let schema = crate::build::Schema::parse_file(path)?;
    let loaded = OwnerRegistry::from_build_schema(&schema);
    let count = loaded.len();
    let mut registry = OWNER_TABLES
        .write()
        .map_err(|e| format!("Lock error: {}", e))?;
    for (table, column) in loaded.tables() {
        registry.register(table, column);
    }
    Ok(count)
}

/// Register owner tables from the canonical migrate-parser schema
/// (`qail_core::migrate::parse_qail`). See [`crate::rls::init_scope_registries`].
///
/// Populates only; a poisoned lock is an error, never a count of zero.
pub fn register_from_migrate_schema(schema: &crate::migrate::Schema) -> Result<usize, String> {
    let mut reg = OWNER_TABLES
        .write()
        .map_err(|e| format!("owner registry lock poisoned: {}", e))?;
    let mut count = 0;
    for (name, table) in &schema.tables {
        if let Some(column) = table.owner_column.as_deref() {
            reg.register(name.as_str(), column);
            count += 1;
        }
    }
    Ok(count)
}

/// Bulk-register multiple owner tables at once. Mode-neutral.
pub fn register_owner_tables(tables: &[(&str, &str)]) {
    if let Ok(mut reg) = OWNER_TABLES.write() {
        for (table, column) in tables {
            reg.register(*table, *column);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_round_trips_owner_column() {
        let mut reg = OwnerRegistry::new();
        assert!(reg.is_empty());
        reg.register("listings", "seller_id");
        assert_eq!(reg.get("listings"), Some("seller_id"));
        assert!(reg.is_owner_table("listings"));
        assert!(!reg.is_owner_table("orders"));
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn build_schema_only_registers_declared_owner_columns() {
        let schema = crate::build::Schema::parse(
            "table listings {\n  id UUID\n  seller_id UUID\n  owner seller_id\n}\n\
             table messages {\n  id UUID\n  user_id UUID\n}\n",
        )
        .expect("schema parses");
        let reg = OwnerRegistry::from_build_schema(&schema);
        assert_eq!(reg.get("listings"), Some("seller_id"));
        assert_eq!(
            reg.get("messages"),
            None,
            "a column named user_id must never be inferred as an owner scope"
        );
    }

    #[test]
    fn global_registry_lookup() {
        register_owner_table("_owner_test_t1", "author_id");
        assert_eq!(
            lookup_owner_column("_owner_test_t1"),
            Some("author_id".to_string())
        );
        assert_eq!(lookup_owner_column("_owner_test_missing"), None);
    }
}
