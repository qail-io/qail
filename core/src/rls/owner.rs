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
//! Like the tenant registry it is private and populated only through the
//! boundary APIs in [`crate::rls`]; the only public read is the fallible
//! [`try_lookup_owner_column`].
//!
//! # Example
//! ```
//! use qail_core::rls::owner::try_lookup_owner_column;
//!
//! qail_core::rls::init_scope_registries_from_tables(&[], &[("listings", "seller_id")])
//!     .expect("scope registries seal");
//! assert_eq!(try_lookup_owner_column("listings"), Ok(Some("seller_id".to_string())));
//! assert_eq!(try_lookup_owner_column("migrations"), Ok(None));
//! ```

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::RwLock;

/// Registry of tables that participate in owner-scope isolation.
#[derive(Debug, Default)]
pub(crate) struct OwnerRegistry {
    tables: HashMap<String, String>,
}

impl OwnerRegistry {
    /// Create an empty registry.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Register a table as owner-scoped.
    pub(crate) fn register(&mut self, table: impl Into<String>, column: impl Into<String>) {
        self.tables.insert(table.into(), column.into());
    }

    /// Lookup the owner column for a table.
    pub(crate) fn get(&self, table: &str) -> Option<&str> {
        self.tables.get(table).map(|s| s.as_str())
    }

    /// Number of registered owner tables.
    pub(crate) fn len(&self) -> usize {
        self.tables.len()
    }
}

/// Global owner registry. Private for the same reason as the tenant
/// registry: no external path may leave the process `Initialized` with no
/// scoping metadata.
static OWNER_TABLES: LazyLock<RwLock<OwnerRegistry>> =
    LazyLock::new(|| RwLock::new(OwnerRegistry::new()));

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

/// Fallible lookup against an explicit lock.
pub(crate) fn lookup_in(
    lock: &RwLock<OwnerRegistry>,
    table: &str,
) -> Result<Option<String>, String> {
    let registry = lock
        .read()
        .map_err(|e| format!("owner registry lock poisoned: {}", e))?;
    Ok(registry.get(table).map(|s| s.to_string()))
}

/// Bulk-register into the process registry, propagating a poisoned lock.
pub(crate) fn try_register_owner_tables(tables: &[(&str, &str)]) -> Result<usize, String> {
    register_into(&OWNER_TABLES, tables)
}

/// Number of owner-registered tables, or an error if the lock is poisoned.
pub(crate) fn try_owner_table_count() -> Result<usize, String> {
    count_in(&OWNER_TABLES)
}

/// Register owner tables from the canonical migrate-parser schema
/// (`qail_core::migrate::parse_qail`). See [`crate::rls::init_scope_registries`].
///
/// Populates only; a poisoned lock is an error, never a count of zero.
pub(crate) fn register_from_migrate_schema(
    schema: &crate::migrate::Schema,
) -> Result<usize, String> {
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

/// Fallible lookup of the owner column for `table`.
///
/// `Ok(None)` = not registered; `Err` = the registry cannot be read.
/// `Qail::with_rls` uses this form (see the tenant counterpart). There is
/// no infallible form.
pub fn try_lookup_owner_column(table: &str) -> Result<Option<String>, String> {
    lookup_in(&OWNER_TABLES, table)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_round_trips_owner_column() {
        let mut reg = OwnerRegistry::new();
        assert_eq!(reg.len(), 0);
        reg.register("listings", "seller_id");
        assert_eq!(reg.get("listings"), Some("seller_id"));
        assert_eq!(reg.get("orders"), None);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn migrate_schema_only_registers_declared_owner_columns() {
        let schema = crate::migrate::parse_qail(
            "table _o_listings {\n  id UUID primary_key\n  seller_id UUID\n  owner seller_id\n}\n\
             table _o_messages {\n  id UUID primary_key\n  user_id UUID\n}\n",
        )
        .expect("schema parses");
        let lock = RwLock::new(OwnerRegistry::new());
        // Mirror register_from_migrate_schema against a local lock.
        let declared: Vec<(&str, &str)> = schema
            .tables
            .iter()
            .filter_map(|(n, t)| t.owner_column.as_deref().map(|c| (n.as_str(), c)))
            .collect();
        assert_eq!(register_into(&lock, &declared), Ok(1));
        assert_eq!(
            lookup_in(&lock, "_o_listings"),
            Ok(Some("seller_id".to_string()))
        );
        assert_eq!(
            lookup_in(&lock, "_o_messages"),
            Ok(None),
            "a column named user_id must never be inferred as an owner scope"
        );
        assert_eq!(register_from_migrate_schema(&schema), Ok(1));
    }

    #[test]
    fn global_lookup_is_fallible_and_distinguishes_unregistered() {
        crate::rls::init_scope_registries_from_tables(&[], &[("_owner_global_probe", "author_id")])
            .expect("boundary registration");
        assert_eq!(
            try_lookup_owner_column("_owner_global_probe"),
            Ok(Some("author_id".to_string()))
        );
        assert_eq!(try_lookup_owner_column("_owner_global_missing"), Ok(None));
    }
}
