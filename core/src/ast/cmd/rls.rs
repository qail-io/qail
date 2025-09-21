//! RLS tenant-scope injection for Qail queries.
//!
//! Provides `with_rls()` — the "one call to rule them all" method that
//! auto-injects tenant isolation at the AST level based on query action.
//!
//! # Architecture
//!
//! ```text
//!  Qail::get("orders")
//!    .with_rls(&ctx)          ← Phase 4: AST injection (primary)
//!    → WHERE operator_id = 'uuid'
//!
//!  acquire_with_rls(ctx)      ← Phase 2: DB session vars (backup)
//!    → SET app.current_operator_id = 'uuid'
//!
//!  CREATE POLICY ...          ← Phase 3: DB policies (safety net)
//!    → ENABLE ROW LEVEL SECURITY
//! ```
//!
//! # Example
//! ```
//! use qail_core::Qail;
//! use qail_core::rls::RlsContext;
//! use qail_core::rls::tenant::register_tenant_table;
//!
//! register_tenant_table("orders", "operator_id");
//!
//! let ctx = RlsContext::operator("550e8400-e29b-41d4-a716-446655440000");
//! let query = Qail::get("orders").with_rls(&ctx);
//! // Transpiles to: SELECT * FROM orders WHERE operator_id = '550e8400-...'
//! ```

use crate::ast::{
    Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value,
};
use crate::rls::RlsContext;
use crate::rls::tenant::lookup_tenant_column;

impl Qail {
    /// Apply tenant-scope isolation based on the query action.
    ///
    /// - **GET/SET/DEL** → injects `WHERE operator_id = $value`
    /// - **ADD/Upsert** → auto-sets `operator_id` in payload
    /// - **Super admins** → no-op (bypasses isolation)
    /// - **Unregistered tables** → no-op (not a tenant table)
    /// - **DDL/Redis/etc** → no-op
    ///
    /// # Example
    /// ```ignore
    /// let ctx = RlsContext::operator("op-uuid");
    /// let query = Qail::get("orders").with_rls(&ctx);
    /// ```
    pub fn with_rls(self, ctx: &RlsContext) -> Self {
        if ctx.bypasses_rls() {
            return self;
        }

        if !ctx.has_operator() {
            return self;
        }

        let Some(tenant_col) = lookup_tenant_column(&self.table) else {
            return self;
        };

        match self.action {
            // Read / Update / Delete → inject WHERE filter
            Action::Get | Action::Set | Action::Del | Action::Over | Action::Gen => {
                self.scope_to_tenant(&tenant_col, ctx)
            }
            // Insert / Upsert → auto-set tenant column in payload
            Action::Add | Action::Upsert | Action::Put => {
                self.scope_insert_tenant(&tenant_col, ctx)
            }
            // DDL, transactions, Redis, etc. → no injection
            _ => self,
        }
    }

    /// Inject a `WHERE tenant_col = operator_id` filter for reads.
    ///
    /// Adds the condition to the existing Filter cage (AND), or creates
    /// a new one. Uses the same pattern as `.filter()`.
    fn scope_to_tenant(mut self, tenant_col: &str, ctx: &RlsContext) -> Self {
        let condition = Condition {
            left: Expr::Named(tenant_col.to_string()),
            op: Operator::Eq,
            value: Value::String(ctx.operator_id.clone()),
            is_array_unnest: false,
        };

        // Try to append to existing filter cage
        let existing = self
            .cages
            .iter_mut()
            .find(|c| matches!(c.kind, CageKind::Filter));

        if let Some(cage) = existing {
            cage.conditions.push(condition);
        } else {
            self.cages.push(Cage {
                kind: CageKind::Filter,
                conditions: vec![condition],
                logical_op: LogicalOp::And,
            });
        }

        self
    }

    /// Auto-set `operator_id` in INSERT/UPSERT payload.
    ///
    /// Adds the tenant column to the Payload cage so the operator_id
    /// is always included in INSERT statements.
    fn scope_insert_tenant(mut self, tenant_col: &str, ctx: &RlsContext) -> Self {
        let condition = Condition {
            left: Expr::Named(tenant_col.to_string()),
            op: Operator::Eq,
            value: Value::String(ctx.operator_id.clone()),
            is_array_unnest: false,
        };

        // Try to append to existing payload cage
        let existing = self
            .cages
            .iter_mut()
            .find(|c| matches!(c.kind, CageKind::Payload));

        if let Some(cage) = existing {
            cage.conditions.push(condition);
        } else {
            self.cages.push(Cage {
                kind: CageKind::Payload,
                conditions: vec![condition],
                logical_op: LogicalOp::And,
            });
        }

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rls::tenant::register_tenant_table;

    // Each test uses a UNIQUE table name to avoid parallel-test interference
    // on the global TENANT_TABLES registry.

    #[test]
    fn test_with_rls_injects_filter_on_get() {
        register_tenant_table("_rls_get_orders", "operator_id");

        let ctx = RlsContext::operator("op-123");
        let query = Qail::get("_rls_get_orders").with_rls(&ctx);

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(filter.is_some(), "Expected filter cage");

        let conditions = &filter.unwrap().conditions;
        assert!(
            conditions.iter().any(|c| {
                matches!(&c.left, Expr::Named(n) if n == "operator_id")
                    && matches!(&c.value, Value::String(v) if v == "op-123")
            }),
            "Expected operator_id = 'op-123' condition"
        );
    }

    #[test]
    fn test_with_rls_injects_payload_on_add() {
        register_tenant_table("_rls_add_orders", "operator_id");

        let ctx = RlsContext::operator("op-456");
        let query = Qail::add("_rls_add_orders")
            .set_value("total", 100)
            .with_rls(&ctx);

        let payload = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Payload));
        assert!(payload.is_some(), "Expected payload cage");

        let conditions = &payload.unwrap().conditions;
        assert!(
            conditions.iter().any(|c| {
                matches!(&c.left, Expr::Named(n) if n == "operator_id")
                    && matches!(&c.value, Value::String(v) if v == "op-456")
            }),
            "Expected operator_id = 'op-456' in payload"
        );
    }

    #[test]
    fn test_with_rls_noop_for_super_admin() {
        register_tenant_table("_rls_admin_orders", "operator_id");

        let ctx = RlsContext::super_admin();
        let query = Qail::get("_rls_admin_orders").with_rls(&ctx);

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(filter.is_none(), "Super admin should not have filter");
    }

    #[test]
    fn test_with_rls_noop_for_unregistered_table() {
        let ctx = RlsContext::operator("op-789");
        let query = Qail::get("_rls_unreg_migrations").with_rls(&ctx);

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(filter.is_none(), "Unregistered table should not have filter");
    }

    #[test]
    fn test_with_rls_noop_for_ddl() {
        register_tenant_table("_rls_ddl_orders", "operator_id");

        let ctx = RlsContext::operator("op-000");
        let query = Qail {
            action: Action::Make,
            table: "_rls_ddl_orders".to_string(),
            ..Default::default()
        };
        let query = query.with_rls(&ctx);

        assert!(query.cages.is_empty(), "DDL should not inject cages");
    }

    #[test]
    fn test_with_rls_appends_to_existing_filter() {
        register_tenant_table("_rls_merge_orders", "operator_id");

        let ctx = RlsContext::operator("op-merge");
        let query = Qail::get("_rls_merge_orders")
            .filter("status", Operator::Eq, "active")
            .with_rls(&ctx);

        let filters: Vec<_> = query
            .cages
            .iter()
            .filter(|c| matches!(c.kind, CageKind::Filter))
            .collect();
        assert_eq!(filters.len(), 1, "Should merge into one filter cage");
        assert_eq!(
            filters[0].conditions.len(),
            2,
            "Should have 2 conditions: status + operator_id"
        );
    }

    #[test]
    fn test_with_rls_on_set_injects_filter() {
        register_tenant_table("_rls_set_orders", "operator_id");

        let ctx = RlsContext::operator("op-set");
        let query = Qail::set("_rls_set_orders")
            .set_value("status", "shipped")
            .with_rls(&ctx);

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(filter.is_some(), "SET should inject filter");

        let conditions = &filter.unwrap().conditions;
        assert!(
            conditions.iter().any(|c| {
                matches!(&c.left, Expr::Named(n) if n == "operator_id")
            }),
            "Expected operator_id filter on SET"
        );
    }

    #[test]
    fn test_with_rls_noop_no_operator() {
        register_tenant_table("_rls_noops_orders", "operator_id");

        // Agent-only context without operator_id
        let ctx = RlsContext::agent("ag-only");
        let query = Qail::get("_rls_noops_orders").with_rls(&ctx);

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(filter.is_none(), "Agent-only should not inject operator filter");
    }
}
