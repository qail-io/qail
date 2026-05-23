//! RLS tenant-scope injection for Qail queries.
//!
//! Provides `with_rls()` — the "one call to rule them all" method that
//! auto-injects tenant isolation at the AST level based on query action.
//!
//! # Architecture
//!
//! ```text
//!  Qail::get("orders")
//!    .with_rls(&ctx)              ← Phase 4: AST injection (primary)
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
//! let query = Qail::get("orders").with_rls(&ctx).expect("rls should apply");
//! // Transpiles to: SELECT * FROM orders WHERE tenant_id = '550e8400-...'
//! ```

use crate::ast::{
    Action, Cage, CageKind, Condition, Expr, LogicalOp, MergeAction, MergeMatchKind, MergeSource,
    Operator, Qail, Value,
};
use crate::error::{QailBuildError, QailBuildResult};
use crate::rls::RlsContext;
use crate::rls::tenant::lookup_tenant_column;

fn normalize_ident(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with('$') {
        return trimmed.to_string();
    }

    let segment = trimmed.rsplit('.').next().unwrap_or(trimmed).trim();
    let unquoted = if segment.len() >= 2 {
        let bytes = segment.as_bytes();
        let first = bytes[0] as char;
        let last = bytes[bytes.len() - 1] as char;
        if (first == '"' && last == '"')
            || (first == '`' && last == '`')
            || (first == '[' && last == ']')
        {
            &segment[1..segment.len() - 1]
        } else {
            segment
        }
    } else {
        segment
    };
    unquoted.to_ascii_lowercase()
}

fn expr_named_eq(expr: &Expr, name: &str) -> bool {
    matches!(expr, Expr::Named(existing) if normalize_ident(existing) == normalize_ident(name))
}

fn is_tenant_column_condition(cond: &Condition, tenant_col: &str) -> bool {
    expr_named_eq(&cond.left, tenant_col)
}

fn condition_references_tenant_column(cond: &Condition, tenant_col: &str) -> bool {
    is_tenant_column_condition(cond, tenant_col)
        || matches!(&cond.value, Value::Column(col) if normalize_ident(col) == normalize_ident(tenant_col))
}

fn payload_is_positional(cage: &Cage) -> bool {
    cage.conditions.iter().all(|cond| {
        matches!(
            &cond.left,
            Expr::Named(name) if name.starts_with('$') && name[1..].chars().all(|c| c.is_ascii_digit())
        )
    })
}

fn make_named_condition(column: &str, value: Value) -> Condition {
    Condition {
        left: Expr::Named(column.to_string()),
        op: Operator::Eq,
        value,
        is_array_unnest: false,
    }
}

fn make_positional_condition(index: usize, value: Value) -> Condition {
    Condition {
        left: Expr::Named(format!("${}", index + 1)),
        op: Operator::Eq,
        value,
        is_array_unnest: false,
    }
}

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
    /// let query = Qail::get("orders").with_rls(&ctx)?;
    /// ```
    pub fn with_rls(self, ctx: &RlsContext) -> QailBuildResult<Self> {
        if ctx.bypasses_rls() {
            return Ok(self);
        }

        if !ctx.is_global() && !ctx.has_tenant() {
            return Ok(self);
        }

        let scoped = self.scope_nested_rls(ctx)?;

        let Some(tenant_col) = lookup_tenant_column(&scoped.table) else {
            return Ok(scoped);
        };

        if ctx.is_global() {
            return match scoped.action {
                Action::Get
                | Action::Cnt
                | Action::Del
                | Action::Over
                | Action::Gen
                | Action::Export
                | Action::Search
                | Action::Scroll => Ok(scoped.scope_to_global(&tenant_col)),
                Action::Set => scoped.scope_update_global(&tenant_col),
                Action::Add | Action::Upsert | Action::Put => {
                    scoped.scope_insert_global(&tenant_col)
                }
                Action::Merge => scoped.scope_merge_global(&tenant_col),
                _ => Ok(scoped),
            };
        }

        match scoped.action {
            // Read / Update / Delete → inject WHERE filter
            Action::Get
            | Action::Cnt
            | Action::Del
            | Action::Over
            | Action::Gen
            | Action::Export
            | Action::Search
            | Action::Scroll => Ok(scoped.scope_to_tenant(&tenant_col, ctx)),
            Action::Set => scoped.scope_update_tenant(&tenant_col, ctx),
            // Insert / Upsert → auto-set tenant column in payload
            Action::Add | Action::Upsert | Action::Put => {
                scoped.scope_insert_tenant(&tenant_col, ctx)
            }
            Action::Merge => scoped.scope_merge_tenant(&tenant_col, ctx),
            // DDL, transactions, etc. → no injection
            _ => Ok(scoped),
        }
    }

    fn scope_nested_rls(mut self, ctx: &RlsContext) -> QailBuildResult<Self> {
        for cte in &mut self.ctes {
            cte.base_query = Box::new(cte.base_query.as_ref().clone().with_rls(ctx)?);
            if let Some(ref mut recursive_query) = cte.recursive_query {
                *recursive_query = Box::new(recursive_query.as_ref().clone().with_rls(ctx)?);
            }
        }

        if let Some(ref mut source_query) = self.source_query {
            *source_query = Box::new(source_query.as_ref().clone().with_rls(ctx)?);
        }

        for (_, set_query) in &mut self.set_ops {
            *set_query = Box::new(set_query.as_ref().clone().with_rls(ctx)?);
        }

        Ok(self)
    }

    fn scope_update_tenant(self, tenant_col: &str, ctx: &RlsContext) -> QailBuildResult<Self> {
        self.reject_tenant_payload_mutation(tenant_col)?;
        Ok(self.scope_to_tenant(tenant_col, ctx))
    }

    fn scope_update_global(self, tenant_col: &str) -> QailBuildResult<Self> {
        self.reject_tenant_payload_mutation(tenant_col)?;
        Ok(self.scope_to_global(tenant_col))
    }

    fn reject_tenant_payload_mutation(&self, tenant_col: &str) -> QailBuildResult<()> {
        let assigns_tenant = self
            .cages
            .iter()
            .filter(|cage| matches!(cage.kind, CageKind::Payload))
            .flat_map(|cage| cage.conditions.iter())
            .any(|cond| expr_named_eq(&cond.left, tenant_col));

        if assigns_tenant {
            return Err(QailBuildError::RlsTenantColumnMutationDenied {
                table: self.table.clone(),
                tenant_column: tenant_col.to_string(),
            });
        }

        Ok(())
    }

    /// Inject a `WHERE tenant_col = scope_id` filter for reads.
    ///
    /// Adds the condition to the existing Filter cage (AND), or creates
    /// a new one. Uses the same pattern as `.filter()`.
    fn scope_to_tenant(mut self, tenant_col: &str, ctx: &RlsContext) -> Self {
        let condition = make_named_condition(tenant_col, Value::String(ctx.tenant_id.clone()));

        // Try to append to existing filter cage
        let existing = self
            .cages
            .iter_mut()
            .find(|c| matches!(c.kind, CageKind::Filter) && c.logical_op == LogicalOp::And);

        if let Some(cage) = existing {
            cage.conditions
                .retain(|cond| !is_tenant_column_condition(cond, tenant_col));
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
            .find(|c| matches!(c.kind, CageKind::Filter) && c.logical_op == LogicalOp::And);

        if let Some(cage) = existing {
            cage.conditions
                .retain(|cond| !is_tenant_column_condition(cond, tenant_col));
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
    fn scope_insert_tenant(self, tenant_col: &str, ctx: &RlsContext) -> QailBuildResult<Self> {
        self.scope_insert_value(tenant_col, Value::String(ctx.tenant_id.clone()))
    }

    /// Auto-set `tenant_col = NULL` in INSERT/UPSERT payload for global rows.
    fn scope_insert_global(self, tenant_col: &str) -> QailBuildResult<Self> {
        self.scope_insert_value(tenant_col, Value::Null)
    }

    fn scope_insert_value(
        mut self,
        tenant_col: &str,
        tenant_value: Value,
    ) -> QailBuildResult<Self> {
        let payload_idx = self
            .cages
            .iter()
            .position(|c| matches!(c.kind, CageKind::Payload));

        let Some(idx) = payload_idx else {
            self.cages.push(Cage {
                kind: CageKind::Payload,
                conditions: vec![make_named_condition(tenant_col, tenant_value)],
                logical_op: LogicalOp::And,
            });
            return Ok(self);
        };

        let positional = payload_is_positional(&self.cages[idx]);
        if positional {
            if self.columns.is_empty() {
                return Err(QailBuildError::RlsInsertRequiresExplicitColumns {
                    table: self.table,
                    tenant_column: tenant_col.to_string(),
                });
            }

            if let Some(col_idx) = self
                .columns
                .iter()
                .position(|expr| expr_named_eq(expr, tenant_col))
            {
                let placeholder = format!("${}", col_idx + 1);
                let cage = &mut self.cages[idx];
                if let Some(cond) = cage
                    .conditions
                    .iter_mut()
                    .find(|cond| expr_named_eq(&cond.left, &placeholder))
                {
                    cond.value = tenant_value;
                    cond.op = Operator::Eq;
                    cond.is_array_unnest = false;
                } else {
                    cage.conditions
                        .push(make_positional_condition(col_idx, tenant_value));
                }
                return Ok(self);
            }

            if !self.columns.is_empty() {
                self.columns.push(Expr::Named(tenant_col.to_string()));
                let idx_col = self.columns.len() - 1;
                let cage = &mut self.cages[idx];
                cage.conditions
                    .push(make_positional_condition(idx_col, tenant_value));
                return Ok(self);
            }
        }

        let cage = &mut self.cages[idx];
        cage.conditions
            .retain(|cond| !is_tenant_column_condition(cond, tenant_col));
        cage.conditions
            .push(make_named_condition(tenant_col, tenant_value));
        Ok(self)
    }

    fn scope_merge_tenant(mut self, tenant_col: &str, ctx: &RlsContext) -> QailBuildResult<Self> {
        self.scope_merge_query_source(ctx)?;
        self.reject_merge_tenant_update_mutation(tenant_col)?;
        let target_col = self.merge_target_tenant_col(tenant_col);
        let source_col = self.merge_source_tenant_col();
        self.scope_merge_on_tenant_equality(tenant_col, target_col.clone(), source_col.clone());

        let condition = Condition {
            left: Expr::Named(target_col),
            op: Operator::Eq,
            value: Value::String(ctx.tenant_id.clone()),
            is_array_unnest: false,
        };
        let source_condition = source_col.map(|source_col| Condition {
            left: Expr::Named(source_col),
            op: Operator::Eq,
            value: Value::String(ctx.tenant_id.clone()),
            is_array_unnest: false,
        });
        self.scope_merge_clause_conditions(tenant_col, condition, source_condition);
        self.scope_merge_insert_value(
            tenant_col,
            Expr::Literal(Value::String(ctx.tenant_id.clone())),
        )?;
        Ok(self)
    }

    fn scope_merge_global(mut self, tenant_col: &str) -> QailBuildResult<Self> {
        self.scope_merge_query_source(&RlsContext::global())?;
        self.reject_merge_tenant_update_mutation(tenant_col)?;
        let target_col = self.merge_target_tenant_col(tenant_col);
        let source_col = self.merge_source_tenant_col();
        self.scope_merge_on_tenant_equality(tenant_col, target_col.clone(), source_col.clone());

        let condition = Condition {
            left: Expr::Named(target_col),
            op: Operator::IsNull,
            value: Value::Null,
            is_array_unnest: false,
        };
        let source_condition = source_col.map(|source_col| Condition {
            left: Expr::Named(source_col),
            op: Operator::IsNull,
            value: Value::Null,
            is_array_unnest: false,
        });
        self.scope_merge_clause_conditions(tenant_col, condition, source_condition);
        self.scope_merge_insert_value(tenant_col, Expr::Literal(Value::Null))?;
        Ok(self)
    }

    fn scope_merge_query_source(&mut self, ctx: &RlsContext) -> QailBuildResult<()> {
        let Some(merge) = &mut self.merge else {
            return Ok(());
        };
        let MergeSource::Query { query, .. } = &mut merge.source else {
            return Ok(());
        };

        let scoped_query = query.as_ref().clone().with_rls(ctx)?;
        *query = Box::new(scoped_query);
        Ok(())
    }

    fn merge_target_tenant_col(&self, tenant_col: &str) -> String {
        let qualifier = self
            .merge
            .as_ref()
            .and_then(|merge| merge.target_alias.as_ref())
            .map(String::as_str)
            .unwrap_or(&self.table);
        format!("{qualifier}.{tenant_col}")
    }

    fn merge_source_tenant_col(&self) -> Option<String> {
        let merge = self.merge.as_ref()?;
        let MergeSource::Table { name, alias } = &merge.source else {
            return None;
        };
        let source_tenant_col = lookup_tenant_column(name)?;
        let qualifier = alias.as_deref().unwrap_or(name);
        Some(format!("{qualifier}.{source_tenant_col}"))
    }

    fn scope_merge_on_tenant_equality(
        &mut self,
        tenant_col: &str,
        target_col: String,
        source_col: Option<String>,
    ) {
        let Some(merge) = &mut self.merge else {
            return;
        };
        merge
            .on
            .retain(|cond| !condition_references_tenant_column(cond, tenant_col));

        if let Some(source_col) = source_col {
            merge.on.push(Condition {
                left: Expr::Named(target_col),
                op: Operator::Eq,
                value: Value::Column(source_col),
                is_array_unnest: false,
            });
        }
    }

    fn scope_merge_clause_conditions(
        &mut self,
        tenant_col: &str,
        target_condition: Condition,
        source_condition: Option<Condition>,
    ) {
        let Some(merge) = &mut self.merge else {
            return;
        };

        for clause in &mut merge.clauses {
            clause
                .condition
                .retain(|cond| !condition_references_tenant_column(cond, tenant_col));

            match clause.match_kind {
                MergeMatchKind::Matched | MergeMatchKind::NotMatchedBySource => {
                    clause.condition.push(target_condition.clone());
                }
                MergeMatchKind::NotMatchedByTarget => {
                    if let Some(condition) = &source_condition {
                        clause.condition.push(condition.clone());
                    }
                }
            }
        }
    }

    fn scope_merge_insert_value(
        &mut self,
        tenant_col: &str,
        tenant_expr: Expr,
    ) -> QailBuildResult<()> {
        let Some(merge) = &mut self.merge else {
            return Ok(());
        };

        for clause in &mut merge.clauses {
            let MergeAction::Insert { columns, values } = &mut clause.action else {
                continue;
            };

            if columns.is_empty() {
                return Err(QailBuildError::RlsInsertRequiresExplicitColumns {
                    table: self.table.clone(),
                    tenant_column: tenant_col.to_string(),
                });
            }

            if let Some(pos) = columns
                .iter()
                .position(|col| normalize_ident(col) == normalize_ident(tenant_col))
            {
                if let Some(value) = values.get_mut(pos) {
                    *value = tenant_expr.clone();
                } else {
                    values.push(tenant_expr.clone());
                }
            } else {
                columns.push(tenant_col.to_string());
                values.push(tenant_expr.clone());
            }
        }

        Ok(())
    }

    fn reject_merge_tenant_update_mutation(&self, tenant_col: &str) -> QailBuildResult<()> {
        let assigns_tenant = self
            .merge
            .as_ref()
            .is_some_and(|merge| {
                merge.clauses.iter().any(|clause| {
                    matches!(&clause.action, MergeAction::Update { assignments }
                        if assignments
                            .iter()
                            .any(|(column, _)| normalize_ident(column) == normalize_ident(tenant_col)))
                })
            });

        if assigns_tenant {
            return Err(QailBuildError::RlsTenantColumnMutationDenied {
                table: self.table.clone(),
                tenant_column: tenant_col.to_string(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rls::tenant::register_tenant_table;
    use crate::transpiler::ToSql;

    // Each test uses a UNIQUE table name to avoid parallel-test interference
    // on the global TENANT_TABLES registry.

    #[test]
    fn test_with_rls_injects_filter_on_get() {
        register_tenant_table("_rls_get_orders", "tenant_id");

        let ctx = RlsContext::tenant("t-123");
        let query = Qail::get("_rls_get_orders")
            .with_rls(&ctx)
            .expect("rls should apply");

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
            .with_rls(&ctx)
            .expect("rls should apply");

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
        let query = Qail::get("_rls_admin_orders")
            .with_rls(&ctx)
            .expect("super admin rls should no-op");

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter));
        assert!(filter.is_none(), "Super admin should not have filter");
    }

    #[test]
    fn test_with_rls_noop_for_unregistered_table() {
        let ctx = RlsContext::tenant("t-789");
        let query = Qail::get("_rls_unreg_migrations")
            .with_rls(&ctx)
            .expect("unregistered table rls should no-op");

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
        let query = query.with_rls(&ctx).expect("ddl rls should no-op");

        assert!(query.cages.is_empty(), "DDL should not inject cages");
    }

    #[test]
    fn test_with_rls_appends_to_existing_filter() {
        register_tenant_table("_rls_merge_orders", "tenant_id");

        let ctx = RlsContext::tenant("t-merge");
        let query = Qail::get("_rls_merge_orders")
            .filter("status", Operator::Eq, "active")
            .with_rls(&ctx)
            .expect("rls should apply");

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
    fn test_with_rls_does_not_merge_tenant_scope_into_or_filter_cage() {
        register_tenant_table("_rls_or_orders", "tenant_id");

        let ctx = RlsContext::tenant("t-or");
        let query = Qail::get("_rls_or_orders")
            .or_filter("status", Operator::Eq, "active")
            .or_filter("status", Operator::Eq, "pending")
            .with_rls(&ctx)
            .expect("rls should apply");

        let or_filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter) && c.logical_op == LogicalOp::Or)
            .expect("Expected OR filter cage");
        assert_eq!(
            or_filter.conditions.len(),
            2,
            "OR cage should keep only OR terms"
        );
        assert!(
            !or_filter
                .conditions
                .iter()
                .any(|c| is_tenant_column_condition(c, "tenant_id")),
            "tenant scope must not be injected into OR cage"
        );

        let and_filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter) && c.logical_op == LogicalOp::And)
            .expect("Expected AND filter cage for tenant scope");
        assert!(
            and_filter
                .conditions
                .iter()
                .any(|c| is_tenant_column_condition(c, "tenant_id")),
            "tenant scope must be enforced via AND cage"
        );

        let sql = query.to_sql();
        assert!(
            sql.contains("tenant_id = 't-or'"),
            "Expected tenant scope in SQL: {sql}"
        );
        assert!(
            !sql.contains("OR tenant_id = 't-or'"),
            "tenant scope must not be OR-ed with user conditions: {sql}"
        );
    }

    #[test]
    fn test_with_rls_on_set_injects_filter() {
        register_tenant_table("_rls_set_orders", "tenant_id");

        let ctx = RlsContext::tenant("t-set");
        let query = Qail::set("_rls_set_orders")
            .set_value("status", "shipped")
            .with_rls(&ctx)
            .expect("rls should apply");

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
    fn test_with_rls_on_set_rejects_tenant_column_update() {
        register_tenant_table("_rls_set_tenant_rewrite_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-a");
        let err = Qail::set("_rls_set_tenant_rewrite_orders")
            .set_value("tenant_id", "tenant-b")
            .with_rls(&ctx)
            .expect_err("tenant column updates must fail closed");

        assert!(err.to_string().contains("tenant column mutation"));
    }

    #[test]
    fn test_with_rls_injects_filter_on_read_like_actions() {
        let actions = [
            (Action::Cnt, "_rls_cnt_orders"),
            (Action::Export, "_rls_export_orders"),
            (Action::Search, "_rls_search_vectors"),
            (Action::Scroll, "_rls_scroll_vectors"),
        ];

        for (action, table) in actions {
            register_tenant_table(table, "tenant_id");

            let ctx = RlsContext::tenant("tenant-read-like");
            let query = Qail {
                action,
                table: table.to_string(),
                ..Default::default()
            }
            .with_rls(&ctx)
            .expect("read-like action should apply RLS");

            let filter = query
                .cages
                .iter()
                .find(|c| matches!(c.kind, CageKind::Filter))
                .expect("Expected filter cage");

            assert!(
                filter.conditions.iter().any(|c| {
                    matches!(&c.left, Expr::Named(n) if n == "tenant_id")
                        && matches!(&c.value, Value::String(v) if v == "tenant-read-like")
                }),
                "Expected tenant filter on {action:?}"
            );
        }
    }

    #[test]
    fn test_with_rls_noop_no_tenant() {
        register_tenant_table("_rls_noops_orders", "tenant_id");

        // Agent-only context without tenant_id
        let ctx = RlsContext::agent("ag-only");
        let query = Qail::get("_rls_noops_orders")
            .with_rls(&ctx)
            .expect("missing tenant rls should no-op");

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
        let query = Qail::get("_rls_global_get_orders")
            .with_rls(&ctx)
            .expect("global rls should apply");

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
            .with_rls(&ctx)
            .expect("global rls should apply");

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

    #[test]
    fn test_with_rls_scopes_merge_on_and_insert_action() {
        register_tenant_table("_rls_merge_upsert_orders", "tenant_id");
        register_tenant_table("_rls_merge_source_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-merge");
        let query = Qail::merge_into("_rls_merge_upsert_orders")
            .target_alias("t")
            .using_table_as("_rls_merge_source_orders", "s")
            .merge_on_column("t.id", Operator::Eq, "s.id")
            .when_matched_update(&[("status", Expr::Named("s.status".to_string()))])
            .when_not_matched_insert(
                &["id", "status"],
                &[
                    Expr::Named("s.id".to_string()),
                    Expr::Named("s.status".to_string()),
                ],
            )
            .with_rls(&ctx)
            .expect("merge rls should apply");

        let sql = query.to_sql();
        assert!(
            sql.contains("ON t.id = s.id AND t.tenant_id = s.tenant_id"),
            "MERGE ON must preserve target/source tenant equality: {sql}"
        );
        assert!(
            sql.contains("WHEN MATCHED AND t.tenant_id = 'tenant-merge' THEN UPDATE"),
            "MERGE matched branch must be target-tenant scoped: {sql}"
        );
        assert!(
            sql.contains("WHEN NOT MATCHED BY TARGET AND s.tenant_id = 'tenant-merge' THEN INSERT"),
            "MERGE insert branch must be source-tenant scoped: {sql}"
        );
        assert!(
            sql.contains("INSERT (id, status, tenant_id) VALUES (s.id, s.status, 'tenant-merge')"),
            "MERGE insert branch must include tenant value: {sql}"
        );
    }

    #[test]
    fn test_with_rls_scopes_merge_query_source() {
        register_tenant_table("_rls_merge_query_target_orders", "tenant_id");
        register_tenant_table("_rls_merge_query_source_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-query");
        let source = Qail::get("_rls_merge_query_source_orders").columns(["id", "status"]);
        let query = Qail::merge_into("_rls_merge_query_target_orders")
            .target_alias("t")
            .using_query_as(source, "s")
            .merge_on_column("t.id", Operator::Eq, "s.id")
            .when_not_matched_insert(
                &["id", "status"],
                &[
                    Expr::Named("s.id".to_string()),
                    Expr::Named("s.status".to_string()),
                ],
            )
            .with_rls(&ctx)
            .expect("merge rls should apply");

        let merge = query.merge.as_ref().expect("merge spec");
        let MergeSource::Query {
            query: source_query,
            ..
        } = &merge.source
        else {
            panic!("expected query source");
        };
        assert!(
            source_query.cages.iter().any(|cage| {
                matches!(cage.kind, CageKind::Filter)
                    && cage.conditions.iter().any(|condition| {
                        matches!(&condition.left, Expr::Named(name) if name == "tenant_id")
                            && condition.op == Operator::Eq
                            && matches!(&condition.value, Value::String(value) if value == "tenant-query")
                    })
            }),
            "MERGE query source must be tenant-scoped"
        );
    }

    #[test]
    fn test_with_rls_scopes_cte_backed_merge_source() {
        register_tenant_table("_rls_merge_cte_target_orders", "tenant_id");
        register_tenant_table("_rls_merge_cte_source_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-cte");
        let incoming =
            Qail::get("_rls_merge_cte_source_orders").columns(["id", "status", "tenant_id"]);
        let source_query = Qail::get("incoming").columns(["id", "status", "tenant_id"]);
        let query = Qail::merge_into("_rls_merge_cte_target_orders")
            .target_alias("t")
            .with("incoming", incoming)
            .using_query_as(source_query, "s")
            .merge_on_column("t.id", Operator::Eq, "s.id")
            .when_matched_update(&[("status", Expr::Named("s.status".to_string()))])
            .when_not_matched_insert(
                &["id", "status"],
                &[
                    Expr::Named("s.id".to_string()),
                    Expr::Named("s.status".to_string()),
                ],
            )
            .with_rls(&ctx)
            .expect("merge rls should apply");

        let cte = query.ctes.first().expect("incoming CTE");
        assert!(
            cte.base_query.cages.iter().any(|cage| {
                matches!(cage.kind, CageKind::Filter) && cage.conditions.iter().any(|condition| {
                    matches!(&condition.left, Expr::Named(name) if name == "tenant_id")
                        && condition.op == Operator::Eq
                        && matches!(&condition.value, Value::String(value) if value == "tenant-cte")
                })
            }),
            "outer MERGE CTE source must be tenant-scoped"
        );
    }

    #[test]
    fn test_with_rls_scopes_cte_alias_queries_before_table_lookup() {
        register_tenant_table("_rls_cte_alias_source_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-alias");
        let query = Qail::get("incoming")
            .with(
                "incoming",
                Qail::get("_rls_cte_alias_source_orders").columns(["id", "tenant_id"]),
            )
            .with_rls(&ctx)
            .expect("cte alias query should still scope registered CTE body");

        let cte = query.ctes.first().expect("incoming CTE");
        assert!(
            cte.base_query.cages.iter().any(|cage| {
                matches!(cage.kind, CageKind::Filter)
                    && cage.conditions.iter().any(|condition| {
                        matches!(&condition.left, Expr::Named(name) if name == "tenant_id")
                            && matches!(&condition.value, Value::String(value) if value == "tenant-alias")
                    })
            }),
            "registered CTE bodies must be scoped even when outer table is a CTE alias"
        );
    }

    #[test]
    fn test_with_rls_rejects_merge_tenant_column_update() {
        register_tenant_table("_rls_merge_tenant_rewrite_orders", "tenant_id");
        register_tenant_table("_rls_merge_tenant_rewrite_source", "tenant_id");

        let ctx = RlsContext::tenant("tenant-a");
        let err = Qail::merge_into("_rls_merge_tenant_rewrite_orders")
            .using_table_as("_rls_merge_tenant_rewrite_source", "s")
            .merge_on_column("_rls_merge_tenant_rewrite_orders.id", Operator::Eq, "s.id")
            .when_matched_update(&[("tenant_id", Expr::Named("s.tenant_id".to_string()))])
            .with_rls(&ctx)
            .expect_err("MERGE tenant column updates must fail closed");

        assert!(err.to_string().contains("tenant column mutation"));
    }

    #[test]
    fn test_with_rls_global_scopes_merge_query_source() {
        register_tenant_table("_rls_global_merge_query_target", "tenant_id");
        register_tenant_table("_rls_global_merge_query_source", "tenant_id");

        let source = Qail::get("_rls_global_merge_query_source").columns(["id", "name"]);
        let query = Qail::merge_into("_rls_global_merge_query_target")
            .using_query_as(source, "s")
            .merge_on_column("_rls_global_merge_query_target.id", Operator::Eq, "s.id")
            .when_not_matched_insert(
                &["id", "name"],
                &[
                    Expr::Named("s.id".to_string()),
                    Expr::Named("s.name".to_string()),
                ],
            )
            .with_rls(&RlsContext::global())
            .expect("global merge rls should apply");

        let merge = query.merge.as_ref().expect("merge spec");
        let MergeSource::Query {
            query: source_query,
            ..
        } = &merge.source
        else {
            panic!("expected query source");
        };
        assert!(
            source_query.cages.iter().any(|cage| {
                matches!(cage.kind, CageKind::Filter)
                    && cage.conditions.iter().any(|condition| {
                        matches!(&condition.left, Expr::Named(name) if name == "tenant_id")
                            && condition.op == Operator::IsNull
                            && matches!(condition.value, Value::Null)
                    })
            }),
            "global MERGE query source must be scoped to NULL tenant rows"
        );
    }

    #[test]
    fn test_with_rls_scopes_merge_by_source_delete_without_target_only_on_predicate() {
        register_tenant_table("_rls_merge_prune_orders", "tenant_id");
        register_tenant_table("_rls_merge_prune_source_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-prune");
        let query = Qail::merge_into("_rls_merge_prune_orders")
            .target_alias("t")
            .using_table_as("_rls_merge_prune_source_orders", "s")
            .merge_on_column("t.id", Operator::Eq, "s.id")
            .when_not_matched_by_source_delete()
            .with_rls(&ctx)
            .expect("merge rls should apply");

        let sql = query.to_sql();
        assert!(
            sql.contains("ON t.id = s.id AND t.tenant_id = s.tenant_id"),
            "MERGE ON should use target/source tenant equality, not a target-only literal: {sql}"
        );
        assert!(
            sql.contains("WHEN NOT MATCHED BY SOURCE AND t.tenant_id = 'tenant-prune' THEN DELETE"),
            "BY SOURCE delete must be target-tenant scoped in the WHEN branch: {sql}"
        );
        assert!(
            !sql.contains("ON t.id = s.id AND t.tenant_id = 'tenant-prune'"),
            "target-only tenant predicates in ON can misclassify BY SOURCE rows: {sql}"
        );
    }

    #[test]
    fn test_with_rls_global_scopes_merge_to_null_tenant() {
        register_tenant_table("_rls_global_merge_catalog", "tenant_id");
        register_tenant_table("_rls_global_merge_source", "tenant_id");

        let query = Qail::merge_into("_rls_global_merge_catalog")
            .using_table_as("_rls_global_merge_source", "s")
            .merge_on_column("_rls_global_merge_catalog.id", Operator::Eq, "s.id")
            .when_not_matched_insert(
                &["id", "name"],
                &[
                    Expr::Named("s.id".to_string()),
                    Expr::Named("s.name".to_string()),
                ],
            )
            .with_rls(&RlsContext::global())
            .expect("global merge rls should apply");

        let sql = query.to_sql();
        assert!(
            sql.contains(
                "ON _rls_global_merge_catalog.id = s.id AND _rls_global_merge_catalog.tenant_id = s.tenant_id"
            ),
            "global MERGE ON must preserve target/source tenant equality: {sql}"
        );
        assert!(
            sql.contains("WHEN NOT MATCHED BY TARGET AND s.tenant_id IS NULL THEN INSERT"),
            "global MERGE insert branch must be source-null scoped: {sql}"
        );
        assert!(
            sql.contains("INSERT (id, name, tenant_id) VALUES (s.id, s.name, NULL)"),
            "global MERGE insert branch must include NULL tenant: {sql}"
        );
    }

    #[test]
    fn test_with_rls_is_idempotent_on_filter_scope() {
        register_tenant_table("_rls_idempotent_get_orders", "tenant_id");

        let ctx = RlsContext::tenant("t-idempotent");
        let query = Qail::get("_rls_idempotent_get_orders")
            .with_rls(&ctx)
            .expect("rls should apply")
            .with_rls(&ctx);
        let query = query.expect("rls should remain idempotent");

        let filter = query
            .cages
            .iter()
            .find(|c| matches!(c.kind, CageKind::Filter))
            .expect("filter cage");

        let tenant_matches = filter
            .conditions
            .iter()
            .filter(|c| matches!(&c.left, Expr::Named(n) if n == "tenant_id"))
            .count();
        assert_eq!(tenant_matches, 1, "tenant scope should not duplicate");
    }

    #[test]
    fn test_with_rls_add_positional_payload_aligns_insert_columns() {
        register_tenant_table("_rls_positional_add_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-positional");
        let query = Qail::add("_rls_positional_add_orders")
            .columns(["id", "total"])
            .values([Value::Int(1), Value::Int(100)])
            .with_rls(&ctx)
            .expect("rls should apply");

        let sql = query.to_sql();
        assert!(
            sql.contains("tenant_id"),
            "tenant column should be injected"
        );
        assert!(
            sql.contains("VALUES (1, 100, 'tenant-positional')"),
            "insert payload should include injected tenant value in positional order: {sql}"
        );
    }

    #[test]
    fn test_with_rls_add_positional_payload_overrides_existing_tenant_column_value() {
        register_tenant_table("_rls_positional_add_override_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-final");
        let query = Qail::add("_rls_positional_add_override_orders")
            .columns(["id", "tenant_id", "total"])
            .values([
                Value::Int(1),
                Value::String("tenant-wrong".to_string()),
                Value::Int(50),
            ])
            .with_rls(&ctx)
            .expect("rls should apply");

        let sql = query.to_sql();
        assert!(sql.contains("'tenant-final'"));
        assert!(!sql.contains("'tenant-wrong'"));
    }

    #[test]
    fn test_with_rls_add_positional_payload_without_columns_errors() {
        register_tenant_table("_rls_positional_add_without_columns_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-without-columns");
        let err = Qail::add("_rls_positional_add_without_columns_orders")
            .values([Value::Int(1), Value::Int(100)])
            .with_rls(&ctx)
            .expect_err("positional payload without columns should fail");

        assert!(err.to_string().contains("requires explicit columns"));
    }

    #[test]
    fn test_with_rls_replaces_qualified_tenant_filter() {
        register_tenant_table("_rls_qualified_tenant_filter_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-final");
        let query = Qail::get("_rls_qualified_tenant_filter_orders")
            .filter("orders.tenant_id", Operator::Eq, "tenant-wrong")
            .with_rls(&ctx)
            .expect("rls should apply");

        let sql = query.to_sql();
        assert!(sql.contains("'tenant-final'"));
        assert!(!sql.contains("'tenant-wrong'"));
    }
}
