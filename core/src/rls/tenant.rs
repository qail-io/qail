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
/// Each entry maps a table name to its tenant column (`tenant_id`).
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
    /// Scans all tables for columns named `tenant_id`.
    pub fn from_build_schema(schema: &crate::build::Schema) -> Self {
        let mut registry = Self::new();

        for table in schema.tables.values() {
            if table.columns.contains_key("tenant_id") {
                registry.register(&table.name, "tenant_id");
            }
        }

        registry
    }
}

/// Global tenant registry. Private: the only mutation paths are the
/// registration helpers below, and none of them change the process
/// isolation mode — only [`crate::rls::init_scope_registries`] /
/// [`crate::rls::init_scope_registries_from_tables`] seal it, after
/// verifying something was actually registered.
static TENANT_TABLES: LazyLock<RwLock<TenantRegistry>> =
    LazyLock::new(|| RwLock::new(TenantRegistry::new()));

/// Register a single table as tenant-scoped at runtime.
///
/// Mode-neutral: this adds metadata but does NOT publish `Initialized`.
/// Use [`crate::rls::init_scope_registries_from_tables`] at the application
/// boundary to register and seal in one fallible step.
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

/// Fallible bulk registration against an explicit lock. A poisoned lock is
/// an error — the boundary must never seal over a registry it could not
/// write. Parameterised on the lock so a test can poison a local one.
pub(crate) fn register_into(
    lock: &RwLock<TenantRegistry>,
    tables: &[(&str, &str)],
) -> Result<usize, String> {
    let mut reg = lock
        .write()
        .map_err(|e| format!("tenant registry lock poisoned: {}", e))?;
    for (table, column) in tables {
        reg.register(*table, *column);
    }
    Ok(tables.len())
}

/// Fallible count against an explicit lock. Never reports a poisoned
/// registry as empty.
pub(crate) fn count_in(lock: &RwLock<TenantRegistry>) -> Result<usize, String> {
    lock.read()
        .map(|r| r.len())
        .map_err(|e| format!("tenant registry lock poisoned: {}", e))
}

/// Bulk-register into the process registry, propagating a poisoned lock.
pub(crate) fn try_register_tenant_tables(tables: &[(&str, &str)]) -> Result<usize, String> {
    register_into(&TENANT_TABLES, tables)
}

/// Number of tenant-registered tables, or an error if the lock is poisoned.
pub(crate) fn try_tenant_table_count() -> Result<usize, String> {
    count_in(&TENANT_TABLES)
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
    try_lookup_tenant_column(table).ok().flatten()
}

/// Fallible lookup: distinguishes "not registered" (`Ok(None)`) from "the
/// registry cannot be read" (`Err`). Security-sensitive traversal
/// (`Qail::with_rls`) MUST use this form — collapsing a poisoned lock into
/// `None` would read as "unregistered" and disable every tenant predicate
/// at once.
pub fn try_lookup_tenant_column(table: &str) -> Result<Option<String>, String> {
    lookup_in(&TENANT_TABLES, table)
}

pub(crate) fn lookup_in(
    lock: &RwLock<TenantRegistry>,
    table: &str,
) -> Result<Option<String>, String> {
    let registry = lock
        .read()
        .map_err(|e| format!("tenant registry lock poisoned: {}", e))?;
    Ok(registry.get(table).map(|s| s.to_string()))
}

/// Whether [`crate::ast::Qail::with_rls`] will actually scope a query on `relation`.
///
/// `with_rls` is a NO-OP for any relation the registry does not know: it
/// returns the query untouched, so the call site reads as scoped while the SQL
/// is not. That is the intended behaviour for genuinely global reference data,
/// but it FAILS OPEN — a typo, a renamed table, a differently-named tenant
/// column, or a VIEW (never registered, since [`load_tenant_tables`] only scans
/// `table` blocks for a literal `tenant_id`) all silently produce an unscoped
/// query.
///
/// Use this to assert scoping where it is load-bearing, and in build-time
/// audits to enumerate `with_rls` call sites that scope nothing:
///
/// ```ignore
/// debug_assert!(
///     qail_core::rls::tenant::scoping_applies("orders"),
///     "orders must be tenant-registered or this read leaks across tenants",
/// );
/// ```
///
/// Views deserve particular care: they cannot carry RLS themselves, and unless
/// they are declared `security_invoker` Postgres evaluates their base tables as
/// the view OWNER, bypassing those tables' policies too. A view read through
/// `with_rls` therefore has NEITHER layer of protection.
pub fn scoping_applies(relation: &str) -> bool {
    lookup_tenant_column(relation).is_some()
}

/// Load tenant tables from a schema.qail file (build-parser format).
/// Auto-detects tables with `tenant_id` columns.
/// Returns the number of tenant tables found. Mode-neutral — see
/// [`crate::rls::init_scope_registries`] for the sealing boundary.
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
        }
    }

    Ok(count)
}

/// Register tenant tables from the canonical migrate-parser schema: every
/// table with a `tenant_id` column. See [`crate::rls::init_scope_registries`].
///
/// Populates only — the boundary publishes the mode AFTER both registries
/// are filled. A poisoned lock is an error, never a count of zero: the
/// boundary must not seal `Initialized` over a registry it failed to fill.
pub fn register_from_migrate_schema(schema: &crate::migrate::Schema) -> Result<usize, String> {
    let mut reg = TENANT_TABLES
        .write()
        .map_err(|e| format!("tenant registry lock poisoned: {}", e))?;
    let mut count = 0;
    for (name, table) in &schema.tables {
        if table.columns.iter().any(|c| c.name == "tenant_id") {
            reg.register(name.as_str(), "tenant_id");
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
    use std::sync::{Mutex, OnceLock};

    fn registry_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("tenant registry test mutex poisoned")
    }

    fn clear_global_registry() {
        if let Ok(mut reg) = TENANT_TABLES.write() {
            *reg = TenantRegistry::new();
        }
    }

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
        let _lock = registry_test_lock();
        clear_global_registry();

        // Use unique table names to avoid test interference
        register_tenant_table("_test_t1", "tenant_id");
        assert_eq!(
            lookup_tenant_column("_test_t1"),
            Some("tenant_id".to_string())
        );
        assert_eq!(lookup_tenant_column("_test_nonexistent"), None);

        // Clean up
        clear_global_registry();
    }

    #[test]
    fn test_bulk_register() {
        let _lock = registry_test_lock();
        clear_global_registry();

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
        clear_global_registry();
    }

    #[test]
    fn test_from_build_schema_prefers_tenant_id() {
        let schema = crate::build::Schema::parse(
            r#"
table orders {
  id UUID
  tenant_id UUID
}

"#,
        )
        .expect("schema should parse");

        let reg = TenantRegistry::from_build_schema(&schema);
        assert_eq!(reg.get("orders"), Some("tenant_id"));
        assert_eq!(reg.get("legacy_bookings"), None);
    }
}
