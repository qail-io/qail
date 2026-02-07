//! PostgreSQL-specific RLS implementation.
//!
//! Uses `set_config()` to set session variables that PostgreSQL RLS
//! policies reference for tenant data isolation.
//!
//! The `RlsContext` struct lives in `qail_core::rls` (shared across all drivers).
//! This module provides the PostgreSQL-specific methods to apply it.

pub use qail_core::rls::RlsContext;

/// PostgreSQL-specific SQL generation for RLS context.
///
/// These functions generate the `set_config()` calls that configure
/// PostgreSQL session variables for RLS policy evaluation.
pub(crate) fn context_to_sql(ctx: &RlsContext) -> String {
    format!(
        "SELECT set_config('app.current_operator_id', '{}', false), \
                set_config('app.current_agent_id', '{}', false), \
                set_config('app.is_super_admin', '{}', false)",
        ctx.operator_id,
        ctx.agent_id,
        ctx.is_super_admin,
    )
}

/// SQL to reset all RLS session variables to safe defaults.
/// Used when returning connections to the pool.
pub(crate) fn reset_sql() -> &'static str {
    "SELECT set_config('app.current_operator_id', '', false), \
            set_config('app.current_agent_id', '', false), \
            set_config('app.is_super_admin', 'false', false)"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_to_sql_operator() {
        let ctx = RlsContext::operator("abc-123");
        let sql = context_to_sql(&ctx);
        assert!(sql.contains("'abc-123'"));
        assert!(sql.contains("app.current_operator_id"));
        assert!(sql.contains("'false'")); // is_super_admin
    }

    #[test]
    fn test_context_to_sql_super_admin() {
        let ctx = RlsContext::super_admin();
        let sql = context_to_sql(&ctx);
        assert!(sql.contains("'true'")); // is_super_admin
    }

    #[test]
    fn test_reset_sql() {
        let sql = reset_sql();
        assert!(sql.contains("app.current_operator_id"));
        assert!(sql.contains("app.current_agent_id"));
        assert!(sql.contains("'false'")); // resets is_super_admin
    }
}
