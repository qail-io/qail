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
//!    → WHERE tenant_id = 'uuid'
//!
//!  acquire_with_rls(ctx)      ← Phase 2: DB session vars (backup)
//!    → SET app.current_tenant_id = 'uuid'
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
//! register_tenant_table("orders", "tenant_id");
//!
//! let ctx = RlsContext::tenant("550e8400-e29b-41d4-a716-446655440000");
//! let query = Qail::get("orders").with_rls(&ctx);
//! // Transpiles to: SELECT * FROM orders WHERE tenant_id = '550e8400-...'
//! ```

use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};
use crate::rls::RlsContext;
use crate::rls::tenant::lookup_tenant_column;

impl Qail {
    /// Apply tenant-scope isolation based on the query action.
    ///
    /// - **GET/SET/DEL** → injects `WHERE tenant_col = $value`
    /// - **ADD/Upsert** → auto-sets `tenant_col` in payload
    /// - **Global context** → injects `tenant_col IS NULL` (or payload `tenant_col = NULL`)
    /// - **Super admins** → no-op (bypasses isolation)
    /// - **Unregistered tables** → no-op (not a tenant table)
    /// - **DDL/etc** → no-op
    ///
    /// # Example
    /// ```ignore
    /// let ctx = RlsContext::tenant("tenant-uuid");
    /// let query = Qail::get("orders").with_rls(&ctx);
    /// ```
    pub fn with_rls(self, ctx: &RlsContext) -> Self {
        if ctx.bypasses_rls() {
            return self;
        }

        let Some(tenant_col) = lookup_tenant_column(&self.table) else {
            return self;
        };

        if ctx.is_global() {
            return match self.action {
                Action::Get | Action::Set | Action::Del | Action::Over | Action::Gen => {
                    self.scope_to_global(&tenant_col)
                }
                Action::Add | Action::Upsert | Action::Put => self.scope_insert_global(&tenant_col),
                _ => self,
            };
        }

        if !ctx.has_tenant() {
            return self;
        }

        match self.action {
            // Read / Update / Delete → inject WHERE filter
            Action::Get | Action::Set | Action::Del | Action::Over | Action::Gen => {
                self.scope_to_tenant(&tenant_col, ctx)
            }
            // Insert / Upsert → auto-set tenant column in payload
            Action::Add | Action::Upsert | Action::Put => {
                self.scope_insert_tenant(&tenant_col, ctx)
            }
            // DDL, transactions, etc. → no injection
            _ => self,
        }
    }

    /// Inject a `WHERE tenant_col = scope_id` filter for reads.
    ///
    /// Adds the condition to the existing Filter cage (AND), or creates
    /// a new one. Uses the same pattern as `.filter()`.
    fn scope_to_tenant(mut self, tenant_col: &str, ctx: &RlsContext) -> Self {
        let condition = Condition {
            left: Expr::Named(tenant_col.to_string()),
            op: Operator::Eq,
            value: Value::String(ctx.tenant_id.clone()),
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

    /// Inject a `WHERE tenant_col IS NULL` filter for global/platform reads.
    fn scope_to_global(mut self, tenant_col: &str) -> Self {
        let condition = Condition {
            left: Expr::Named(tenant_col.to_string()),
            op: Operator::IsNull,
            value: Value::Null,
            is_array_unnest: false,
        };

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

    /// Auto-set tenant scope in INSERT/UPSERT payload.
    ///
    /// Adds the tenant column to the Payload cage so the scope id
    /// is always included in INSERT statements.
    fn scope_insert_tenant(mut self, tenant_col: &str, ctx: &RlsContext) -> Self {
        let condition = Condition {
            left: Expr::Named(tenant_col.to_string()),
            op: Operator::Eq,
            value: Value::String(ctx.tenant_id.clone()),
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

    /// Auto-set `tenant_col = NULL` in INSERT/UPSERT payload for global rows.
    fn scope_insert_global(mut self, tenant_col: &str) -> Self {
        let condition = Condition {
            left: Expr::Named(tenant_col.to_string()),
            op: Operator::Eq,
            value: Value::Null,
            is_array_unnest: false,
        };

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
        register_tenant_table("_rls_get_orders", "tenant_id");

        let ctx = RlsContext::tenant("t-123");
        let query = Qail::get("_rls_get_orders").with_rls(&ctx);

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(filter.is_some(), "Expected filter cage");

        let conditions = &filter.unwrap().conditions;
        assert!(
            conditions.iter().any(|c| {
                matches!(&c.left, Expr::Named(n) if n == "tenant_id")
                    && matches!(&c.value, Value::String(v) if v == "t-123")
            }),
            "Expected tenant_id = 't-123' condition"
        );
    }

    #[test]
    fn test_with_rls_injects_payload_on_add() {
        register_tenant_table("_rls_add_orders", "tenant_id");

        let ctx = RlsContext::tenant("t-456");
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
                matches!(&c.left, Expr::Named(n) if n == "tenant_id")
                    && matches!(&c.value, Value::String(v) if v == "t-456")
            }),
            "Expected tenant_id = 't-456' in payload"
        );
    }

    #[test]
    fn test_with_rls_noop_for_super_admin() {
        register_tenant_table("_rls_admin_orders", "tenant_id");

        let token = crate::rls::SuperAdminToken::for_system_process("test_super_admin_noop");
        let ctx = RlsContext::super_admin(token);
        let query = Qail::get("_rls_admin_orders").with_rls(&ctx);

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(filter.is_none(), "Super admin should not have filter");
    }

    #[test]
    fn test_with_rls_noop_for_unregistered_table() {
        let ctx = RlsContext::tenant("t-789");
        let query = Qail::get("_rls_unreg_migrations").with_rls(&ctx);

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(
            filter.is_none(),
            "Unregistered table should not have filter"
        );
    }

    #[test]
    fn test_with_rls_noop_for_ddl() {
        register_tenant_table("_rls_ddl_orders", "tenant_id");

        let ctx = RlsContext::tenant("t-000");
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
        register_tenant_table("_rls_merge_orders", "tenant_id");

        let ctx = RlsContext::tenant("t-merge");
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
            "Should have 2 conditions: status + tenant_id"
        );
    }

    #[test]
    fn test_with_rls_on_set_injects_filter() {
        register_tenant_table("_rls_set_orders", "tenant_id");

        let ctx = RlsContext::tenant("t-set");
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
            conditions
                .iter()
                .any(|c| { matches!(&c.left, Expr::Named(n) if n == "tenant_id") }),
            "Expected tenant_id filter on SET"
        );
    }

    #[test]
    fn test_with_rls_noop_no_tenant() {
        register_tenant_table("_rls_noops_orders", "tenant_id");

        // Agent-only context without tenant_id
        let ctx = RlsContext::agent("ag-only");
        let query = Qail::get("_rls_noops_orders").with_rls(&ctx);

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(
            filter.is_none(),
            "Agent-only should not inject tenant filter"
        );
    }

    #[test]
    fn test_with_rls_global_injects_is_null_filter() {
        register_tenant_table("_rls_global_get_orders", "tenant_id");

        let ctx = RlsContext::global();
        let query = Qail::get("_rls_global_get_orders").with_rls(&ctx);

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(filter.is_some(), "Expected filter cage for global scope");

        let conditions = &filter.expect("filter cage").conditions;
        assert!(
            conditions.iter().any(|c| {
                matches!(&c.left, Expr::Named(n) if n == "tenant_id")
                    && c.op == Operator::IsNull
                    && matches!(&c.value, Value::Null)
            }),
            "Expected tenant_id IS NULL condition"
        );
    }

    #[test]
    fn test_with_rls_global_injects_null_payload_on_add() {
        register_tenant_table("_rls_global_add_catalog", "tenant_id");

        let ctx = RlsContext::global();
        let query = Qail::add("_rls_global_add_catalog")
            .set_value("name", "item")
            .with_rls(&ctx);

        let payload = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Payload));
        assert!(payload.is_some(), "Expected payload cage");

        let conditions = &payload.expect("payload cage").conditions;
        assert!(
            conditions.iter().any(|c| {
                matches!(&c.left, Expr::Named(n) if n == "tenant_id")
                    && matches!(&c.value, Value::Null)
            }),
            "Expected tenant_id = NULL in payload"
        );
    }
}
