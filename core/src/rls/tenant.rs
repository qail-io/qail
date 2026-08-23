//! Tenant Table Registry — tracks which tables require tenant-scope injection.
//!
//! The registry is process-global and **private**: it is populated only
//! through the application-boundary APIs in [`crate::rls`]
//! ([`crate::rls::init_scope_registries`] /
//! [`crate::rls::init_scope_registries_from_tables`]), which seal the
//! isolation mode after verifying something was registered. The only public
//! read is the fallible [`try_lookup_tenant_column`] — a poisoned registry is
//! an error, never "unregistered".
//!
//! # Example
//! ```
//! use qail_core::rls::tenant::try_lookup_tenant_column;
//!
//! qail_core::rls::init_scope_registries_from_tables(&[("orders", "tenant_id")], &[])
//!     .expect("scope registries seal");
//! assert_eq!(try_lookup_tenant_column("orders"), Ok(Some("tenant_id".to_string())));
//! assert_eq!(try_lookup_tenant_column("migrations"), Ok(None));
//! ```

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::RwLock;

/// Registry of tables that participate in tenant-scope isolation.
///
/// Each entry maps a table name to its tenant column (`tenant_id`).
#[derive(Debug, Default)]
pub(crate) struct TenantRegistry {
    tables: HashMap<String, String>,
}

impl TenantRegistry {
    /// Create an empty registry.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Register a table as tenant-scoped.
    pub(crate) fn register(&mut self, table: impl Into<String>, column: impl Into<String>) {
        self.tables.insert(table.into(), column.into());
    }

    /// Lookup the tenant column for a table.
    pub(crate) fn get(&self, table: &str) -> Option<&str> {
        self.tables.get(table).map(|s| s.as_str())
    }

    /// Number of registered tenant tables.
    pub(crate) fn len(&self) -> usize {
        self.tables.len()
    }
}

/// Global tenant registry. Private: the only mutation paths are the
/// crate-internal registration helpers below, and none of them change the
/// process isolation mode — only the boundary APIs in [`crate::rls`] seal
/// it, after verifying something was actually registered.
static TENANT_TABLES: LazyLock<RwLock<TenantRegistry>> =
    LazyLock::new(|| RwLock::new(TenantRegistry::new()));

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

/// Fallible lookup against an explicit lock.
pub(crate) fn lookup_in(
    lock: &RwLock<TenantRegistry>,
    table: &str,
) -> Result<Option<String>, String> {
    let registry = lock
        .read()
        .map_err(|e| format!("tenant registry lock poisoned: {}", e))?;
    Ok(registry.get(table).map(|s| s.to_string()))
}

/// Bulk-register into the process registry, propagating a poisoned lock.
pub(crate) fn try_register_tenant_tables(tables: &[(&str, &str)]) -> Result<usize, String> {
    register_into(&TENANT_TABLES, tables)
}

/// Number of tenant-registered tables, or an error if the lock is poisoned.
pub(crate) fn try_tenant_table_count() -> Result<usize, String> {
    count_in(&TENANT_TABLES)
}

/// Register tenant tables from the canonical migrate-parser schema: every
/// table with a `tenant_id` column. See [`crate::rls::init_scope_registries`].
///
/// Populates only — the boundary publishes the mode AFTER both registries
/// are filled. A poisoned lock is an error, never a count of zero: the
/// boundary must not seal `Initialized` over a registry it failed to fill.
pub(crate) fn register_from_migrate_schema(
    schema: &crate::migrate::Schema,
) -> Result<usize, String> {
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

/// Fallible lookup of the tenant column for `table`.
///
/// `Ok(None)` = not registered; `Err` = the registry cannot be read.
/// Security-sensitive traversal (`Qail::with_rls`) uses this form —
/// collapsing a poisoned lock into `None` would read as "unregistered" and
/// disable every tenant predicate at once. There is no infallible form.
pub fn try_lookup_tenant_column(table: &str) -> Result<Option<String>, String> {
    lookup_in(&TENANT_TABLES, table)
}

/// Whether [`crate::ast::Qail::with_rls`] will inject a tenant predicate on
/// `relation`.
///
/// `with_rls` is a NO-OP for any relation the registry does not know, which
/// is the intended behaviour for genuinely global reference data but FAILS
/// OPEN for a typo, a renamed table, a differently-named tenant column, or a
/// VIEW (never registered — views cannot carry RLS and, unless declared
/// `security_invoker`, evaluate their base tables as the view OWNER). Use
/// this to assert scoping where it is load-bearing:
///
/// ```ignore
/// debug_assert!(
///     qail_core::rls::tenant::scoping_applies("orders")?,
///     "orders must be tenant-registered or this read leaks across tenants",
/// );
/// ```
///
/// Fallible for the same reason as [`try_lookup_tenant_column`].
pub fn scoping_applies(relation: &str) -> Result<bool, String> {
    try_lookup_tenant_column(relation).map(|col| col.is_some())
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
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn lock_level_helpers_round_trip() {
        let lock = RwLock::new(TenantRegistry::new());
        assert_eq!(count_in(&lock), Ok(0));
        assert_eq!(
            register_into(&lock, &[("_t_a", "tenant_id"), ("_t_b", "tenant_id")]),
            Ok(2)
        );
        assert_eq!(count_in(&lock), Ok(2));
        assert_eq!(lookup_in(&lock, "_t_a"), Ok(Some("tenant_id".to_string())));
        assert_eq!(lookup_in(&lock, "_t_missing"), Ok(None));
    }

    #[test]
    fn global_lookup_is_fallible_and_distinguishes_unregistered() {
        crate::rls::init_scope_registries_from_tables(
            &[("_tenant_global_probe", "tenant_id")],
            &[],
        )
        .expect("boundary registration");
        assert_eq!(
            try_lookup_tenant_column("_tenant_global_probe"),
            Ok(Some("tenant_id".to_string()))
        );
        assert_eq!(try_lookup_tenant_column("_tenant_global_missing"), Ok(None));
        assert_eq!(scoping_applies("_tenant_global_probe"), Ok(true));
        assert_eq!(scoping_applies("_tenant_global_missing"), Ok(false));
    }

    #[test]
    fn migrate_schema_registration_counts_tenant_tables() {
        let schema = crate::migrate::parse_qail(
            "table _ms_orders {\n  id UUID primary_key\n  tenant_id UUID\n}\n\
             table _ms_ref {\n  id UUID primary_key\n}\n",
        )
        .unwrap();
        assert_eq!(register_from_migrate_schema(&schema), Ok(1));
    }
}
