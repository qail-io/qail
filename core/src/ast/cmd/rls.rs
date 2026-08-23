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
//!
//! // Application boundary: register AND seal the process `Initialized`.
//! // (`with_rls` refuses to run until the process declares its mode.)
//! qail_core::rls::init_scope_registries_from_tables(&[("orders", "tenant_id")], &[])
//!     .expect("scope registries seal");
//!
//! let ctx = RlsContext::tenant("550e8400-e29b-41d4-a716-446655440000");
//! let query = Qail::get("orders").with_rls(&ctx).expect("rls should apply");
//! // Transpiles to: SELECT * FROM orders WHERE tenant_id = '550e8400-...'
//! ```

use crate::ast::{
    Action, Cage, CageKind, Condition, ConflictAction, Expr, JoinKind, LogicalOp, MergeAction,
    MergeMatchKind, MergeSource, Operator, Qail, Value,
};
use crate::error::{QailBuildError, QailBuildResult};
use crate::rls::RlsContext;
use crate::rls::owner::try_lookup_owner_column;
use crate::rls::tenant::try_lookup_tenant_column;

/// Registry lookups used by scoping. These are the ONLY lookup forms this
/// module may use: a poisoned registry is `RlsRegistryUnavailable`, never
/// `None` — `None` means "unregistered", which is the fail-open answer.
fn tenant_column_for(table: &str) -> QailBuildResult<Option<String>> {
    map_registry_lookup(table, try_lookup_tenant_column(table))
}

fn owner_column_for(table: &str) -> QailBuildResult<Option<String>> {
    map_registry_lookup(table, try_lookup_owner_column(table))
}

/// `Err` from a registry becomes `RlsRegistryUnavailable`; `Ok(None)` stays
/// "unregistered". Separated so the mapping itself is testable with a real
/// `Err` without poisoning the process registries.
fn map_registry_lookup(
    table: &str,
    lookup: Result<Option<String>, String>,
) -> QailBuildResult<Option<String>> {
    lookup.map_err(|reason| QailBuildError::RlsRegistryUnavailable {
        table: table.to_string(),
        reason,
    })
}

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

fn split_table_reference(table_ref: &str) -> (&str, Option<&str>) {
    let parts = table_ref.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        [table, alias] => (table, Some(alias)),
        [table, as_keyword, alias] if as_keyword.eq_ignore_ascii_case("as") => (table, Some(alias)),
        _ => (table_ref.trim(), None),
    }
}

fn expr_named_eq(expr: &Expr, name: &str) -> bool {
    matches!(expr, Expr::Named(existing) if normalize_ident(existing) == normalize_ident(name))
}

fn is_tenant_column_condition(cond: &Condition, tenant_col: &str) -> bool {
    expr_named_eq(&cond.left, tenant_col)
}

/// Split `qualifier.column` into normalized segments (quotes stripped,
/// case-folded). `column` alone has one segment.
fn column_ref_segments(raw: &str) -> Vec<String> {
    raw.trim().split('.').map(normalize_ident).collect()
}

/// Whether two column references name the same column of the same relation.
///
/// A qualifier names a DIFFERENT relation only when it is one of the query's
/// join qualifiers (`joined`); every other reference — unqualified, or
/// qualified with the primary alias or a stray name — resolves to the
/// primary relation. So `a.tenant_id` and `b.tenant_id` never match when
/// `a`/`b` are joins, while a user-supplied `orders.tenant_id` spoof still
/// gets replaced by the injected primary predicate.
fn same_scoped_column(a: &str, b: &str, primary: &str, joined: &[String]) -> bool {
    let primary = normalize_ident(primary);
    let resolve = |raw: &str| -> Vec<String> {
        let mut segs = column_ref_segments(raw);
        match segs.len() {
            1 => segs.insert(0, primary.clone()),
            2 if !joined.contains(&segs[0]) => segs[0] = primary.clone(),
            _ => {}
        }
        segs
    };
    resolve(a) == resolve(b)
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

fn expr_projects_all_columns(expr: &Expr) -> bool {
    matches!(expr, Expr::Star)
        || matches!(expr, Expr::Named(name) if name == "*" || name.trim().ends_with(".*"))
}

fn expr_projects_tenant_col(expr: &Expr, tenant_col: &str) -> bool {
    match expr {
        Expr::Named(name) => normalize_ident(name) == normalize_ident(tenant_col),
        Expr::Aliased { alias, .. } => normalize_ident(alias) == normalize_ident(tenant_col),
        Expr::JsonAccess {
            alias: Some(alias), ..
        }
        | Expr::FunctionCall {
            alias: Some(alias), ..
        }
        | Expr::Cast {
            alias: Some(alias), ..
        }
        | Expr::Binary {
            alias: Some(alias), ..
        }
        | Expr::Case {
            alias: Some(alias), ..
        }
        | Expr::SpecialFunction {
            alias: Some(alias), ..
        }
        | Expr::ArrayConstructor {
            alias: Some(alias), ..
        }
        | Expr::RowConstructor {
            alias: Some(alias), ..
        }
        | Expr::Subscript {
            alias: Some(alias), ..
        }
        | Expr::Collate {
            alias: Some(alias), ..
        }
        | Expr::FieldAccess {
            alias: Some(alias), ..
        }
        | Expr::Subquery {
            alias: Some(alias), ..
        }
        | Expr::Exists {
            alias: Some(alias), ..
        } => normalize_ident(alias) == normalize_ident(tenant_col),
        _ => false,
    }
}

fn query_projects_tenant_col(query: &Qail, tenant_col: &str) -> bool {
    query.columns.is_empty()
        || query.columns.iter().any(|expr| {
            expr_projects_all_columns(expr) || expr_projects_tenant_col(expr, tenant_col)
        })
}

fn query_can_append_tenant_projection(query: &Qail) -> bool {
    query.set_ops.is_empty()
        && query.having.is_empty()
        && !query
            .columns
            .iter()
            .any(|expr| matches!(expr, Expr::Aggregate { .. } | Expr::Window { .. }))
}

fn ensure_merge_query_source_projects_tenant(
    mut query: Qail,
    target_table: &str,
    tenant_col: &str,
) -> QailBuildResult<Qail> {
    if query_projects_tenant_col(&query, tenant_col) {
        return Ok(query);
    }

    if !query_can_append_tenant_projection(&query) {
        return Err(QailBuildError::RlsMergeSourceTenantProjectionRequired {
            table: target_table.to_string(),
            tenant_column: tenant_col.to_string(),
        });
    }

    query.columns.push(Expr::Named(tenant_col.to_string()));
    Ok(query)
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
    /// # ⚠️ Unregistered relations FAIL OPEN
    ///
    /// The unregistered case returns the query **unscoped**, which makes this
    /// call site *look* protected while emitting SQL that is not. The registry
    /// is populated by scanning `schema.qail` for `table` blocks carrying a
    /// literal `tenant_id`, so a renamed table, a differently-named tenant
    /// column, or **any view** falls through silently.
    ///
    /// Views are the sharp edge: they cannot carry RLS of their own, and unless
    /// declared `security_invoker` Postgres evaluates their base tables with the
    /// view OWNER's rights — bypassing those tables' policies as well. A view
    /// read under `with_rls` has neither layer.
    ///
    /// Assert it where scoping is load-bearing, with
    /// [`crate::rls::tenant::scoping_applies`].
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

        match crate::rls::scope_registry_state() {
            crate::rls::ScopeRegistryState::Initialized => {}
            // Declared: DB policies carry isolation, AST injects nothing.
            crate::rls::ScopeRegistryState::PolicyOnly => return Ok(self),
            // Undeclared: refusing beats the silent no-op this used to be.
            crate::rls::ScopeRegistryState::Uninitialized => {
                return Err(QailBuildError::RlsRegistryUninitialized { table: self.table });
            }
        }

        let (base_table, _) = split_table_reference(&self.table);
        let tenant_col = tenant_column_for(base_table)?;
        let owner_col = owner_column_for(base_table)?;

        // Fail closed: a registered table demands the scope it registered for.
        // Returning the query untouched here would be the exact false-green
        // the build audit exists to prevent — `.with_rls()` present, nothing
        // injected.
        Self::ensure_scope_present(
            &self.table,
            tenant_col.as_deref(),
            owner_col.as_deref(),
            ctx,
        )?;

        // Nested relations FIRST, unconditionally. An unregistered outer
        // relation (a CTE alias, a wrapper view) can embed a registered
        // table; deciding "nothing to do" before visiting it would let that
        // inner table run unscoped.
        let mut scoped = self.scope_nested_rls(ctx)?;
        scoped = scoped.scope_joined_relations(ctx)?;

        if let Some(tenant_col) = tenant_col {
            scoped = scoped.scope_tenant_dimension(&tenant_col, ctx)?;
            scoped = scoped.scope_conflict_update(&tenant_col, Self::tenant_scope_value(ctx))?;
        }
        if let Some(owner_col) = owner_col {
            scoped = scoped.scope_owner_dimension(&owner_col, ctx)?;
            scoped = scoped.scope_conflict_update(
                &owner_col,
                Some(Value::String(ctx.user_id().to_string())),
            )?;
        }
        Ok(scoped)
    }

    fn ensure_scope_present(
        table: &str,
        tenant_col: Option<&str>,
        owner_col: Option<&str>,
        ctx: &RlsContext,
    ) -> QailBuildResult<()> {
        if let Some(col) = tenant_col
            && !ctx.is_global()
            && !ctx.has_tenant()
        {
            return Err(QailBuildError::RlsScopeMissing {
                table: table.to_string(),
                scope: "tenant",
                column: col.to_string(),
            });
        }
        if let Some(col) = owner_col
            && !ctx.has_user()
        {
            return Err(QailBuildError::RlsScopeMissing {
                table: table.to_string(),
                scope: "user",
                column: col.to_string(),
            });
        }
        Ok(())
    }

    /// `Some(tenant)` for tenant contexts, `None` for global (rendered as
    /// `IS NULL`).
    fn tenant_scope_value(ctx: &RlsContext) -> Option<Value> {
        if ctx.is_global() {
            None
        } else {
            Some(Value::String(ctx.tenant_id.clone()))
        }
    }

    fn scope_condition(col: &str, value: Option<Value>) -> Condition {
        match value {
            Some(value) => make_named_condition(col, value),
            None => Condition {
                left: Expr::Named(col.to_string()),
                op: Operator::IsNull,
                value: Value::Null,
                is_array_unnest: false,
            },
        }
    }

    /// Scope every JOINed relation that is registered for tenant or owner
    /// isolation. The predicate goes into the join's ON clause (INNER/LEFT/
    /// LATERAL) or the WHERE cage (CROSS); RIGHT/FULL cannot isolate and are
    /// refused. Fails closed like the primary relation.
    fn scope_joined_relations(mut self, ctx: &RlsContext) -> QailBuildResult<Self> {
        let mut extra_filters: Vec<Condition> = Vec::new();
        for join in &mut self.joins {
            let (base, alias) = split_table_reference(&join.table);
            let tenant_col = tenant_column_for(base)?;
            let owner_col = owner_column_for(base)?;
            if tenant_col.is_none() && owner_col.is_none() {
                continue;
            }
            Self::ensure_scope_present(
                &join.table,
                tenant_col.as_deref(),
                owner_col.as_deref(),
                ctx,
            )?;
            let qualifier = alias.unwrap_or(base);
            let mut conditions = Vec::new();
            if let Some(col) = tenant_col {
                conditions.push(Self::scope_condition(
                    &format!("{qualifier}.{col}"),
                    Self::tenant_scope_value(ctx),
                ));
            }
            if let Some(col) = owner_col {
                conditions.push(Self::scope_condition(
                    &format!("{qualifier}.{col}"),
                    Some(Value::String(ctx.user_id().to_string())),
                ));
            }
            match join.kind {
                JoinKind::Inner | JoinKind::Left | JoinKind::Lateral => {
                    join.on_true = false;
                    join.on.get_or_insert_with(Vec::new).extend(conditions);
                }
                JoinKind::Cross => extra_filters.extend(conditions),
                JoinKind::Right | JoinKind::Full => {
                    return Err(QailBuildError::RlsJoinKindUnsupported {
                        table: join.table.clone(),
                        join_kind: format!("{:?}", join.kind),
                    });
                }
            }
        }
        for condition in extra_filters {
            self = self.scope_to_condition(condition);
        }
        Ok(self)
    }

    /// `ON CONFLICT DO UPDATE` is an UPDATE of an existing row: it must be
    /// gated by the same scope as the insert payload, and must not be able
    /// to move the row to another scope.
    fn scope_conflict_update(mut self, col: &str, value: Option<Value>) -> QailBuildResult<Self> {
        let condition_col = self.primary_tenant_condition_col(col);
        let primary = self.primary_relation_qualifier();
        let joined = self.join_qualifiers();
        let table = self.table.clone();
        let Some(on_conflict) = self.on_conflict.as_mut() else {
            return Ok(self);
        };
        let ConflictAction::DoUpdate { assignments } = &on_conflict.action else {
            return Ok(self);
        };
        if assignments
            .iter()
            .any(|(assigned, _)| normalize_ident(assigned) == normalize_ident(col))
        {
            return Err(QailBuildError::RlsTenantColumnMutationDenied {
                table,
                tenant_column: col.to_string(),
            });
        }
        on_conflict.where_conditions.retain(|c| {
            !matches!(&c.left, Expr::Named(existing)
                if same_scoped_column(existing, &condition_col, &primary, &joined))
        });
        on_conflict
            .where_conditions
            .push(Self::scope_condition(&condition_col, value));
        Ok(self)
    }

    fn scope_tenant_dimension(self, tenant_col: &str, ctx: &RlsContext) -> QailBuildResult<Self> {
        let scoped = self;
        if ctx.is_global() {
            return match scoped.action {
                Action::Get
                | Action::Cnt
                | Action::Del
                | Action::Over
                | Action::Gen
                | Action::Export
                | Action::Search
                | Action::Scroll => {
                    let condition_col = scoped.primary_tenant_condition_col(tenant_col);
                    Ok(scoped.scope_to_global(&condition_col))
                }
                Action::Set => scoped.scope_update_global(tenant_col),
                Action::Add | Action::Upsert | Action::Put => {
                    scoped.scope_insert_global(tenant_col)
                }
                Action::Merge => scoped.scope_merge_global(tenant_col),
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
            | Action::Scroll => {
                let condition_col = scoped.primary_tenant_condition_col(tenant_col);
                Ok(scoped.scope_to_tenant(&condition_col, ctx))
            }
            Action::Set => scoped.scope_update_tenant(tenant_col, ctx),
            // Insert / Upsert → auto-set tenant column in payload
            Action::Add | Action::Upsert | Action::Put => {
                scoped.scope_insert_tenant(tenant_col, ctx)
            }
            Action::Merge => scoped.scope_merge_tenant(tenant_col, ctx),
            // DDL, transactions, etc. → no injection
            _ => Ok(scoped),
        }
    }

    /// Owner-scope injection: `owner_col = ctx.user_id`.
    ///
    /// Orthogonal to tenant scoping — a table registered for both gets both
    /// predicates ANDed. Global contexts carry no user, so they are rejected
    /// earlier by the fail-closed check in [`Qail::with_rls`].
    fn scope_owner_dimension(self, owner_col: &str, ctx: &RlsContext) -> QailBuildResult<Self> {
        let user_id = Value::String(ctx.user_id().to_string());
        match self.action {
            Action::Get
            | Action::Cnt
            | Action::Del
            | Action::Over
            | Action::Gen
            | Action::Export
            | Action::Search
            | Action::Scroll => {
                let condition_col = self.primary_tenant_condition_col(owner_col);
                Ok(self.scope_to_value(&condition_col, user_id))
            }
            Action::Set => {
                self.reject_tenant_payload_mutation(owner_col)?;
                let condition_col = self.primary_tenant_condition_col(owner_col);
                Ok(self.scope_to_value(&condition_col, user_id))
            }
            Action::Add | Action::Upsert | Action::Put => {
                self.scope_insert_value(owner_col, user_id)
            }
            Action::Merge => Err(QailBuildError::RlsOwnerMergeUnsupported {
                table: self.table,
                owner_column: owner_col.to_string(),
            }),
            _ => Ok(self),
        }
    }

    /// Declare that this query's tenant isolation is DELIBERATELY delegated to
    /// the database's row-level-security policies (Phase 2 session variable +
    /// Phase 3 `CREATE POLICY`) instead of AST injection.
    ///
    /// Use this for queries that are *intentionally cross-tenant by policy* —
    /// e.g. a reseller storefront reading an operator's rows through a
    /// contract-scope policy (`... OR tenant_id IN (SELECT principal_tenant_id
    /// FROM tenant_contracts ...)`), or an insert whose payload `tenant_id`
    /// must NOT be overwritten with the session tenant (settlement/payout
    /// attribution). Calling [`Qail::with_rls`] on such a query would inject
    /// `WHERE tenant_col = ctx.tenant` (hiding the policy-granted rows) or
    /// overwrite the payload tenant; leaving the query bare trips the
    /// build-time RLS audit. This marker is the explicit, auditable middle
    /// ground: the AST is left untouched and the audit treats the query as
    /// consciously scoped.
    ///
    /// The `RlsContext` argument is not applied to the AST — it documents (and
    /// type-checks) which session context the caller runs the query under.
    /// Policy delegation only isolates when the connection was acquired with
    /// an RLS context (`acquire_with_rls`) so `app.current_tenant_id` is set
    /// for the policies to read; it is NOT a bypass.
    ///
    /// # Example
    /// ```ignore
    /// // Reseller storefront reads the operator's package via the
    /// // charter contract-scope DB policy — do NOT inject
    /// // WHERE tenant_id = <reseller>.
    /// let ctx = tenant.to_rls_context();
    /// let query = Qail::get("charter_packages")
    ///     .eq("slug", slug)
    ///     .with_rls_policy(&ctx);
    /// ```
    #[must_use]
    pub fn with_rls_policy(self, _ctx: &RlsContext) -> Self {
        self
    }

    fn scope_boxed_query_rls(query: &mut Box<Qail>, ctx: &RlsContext) -> QailBuildResult<()> {
        let nested = std::mem::take(query.as_mut());
        **query = nested.with_rls(ctx)?;
        Ok(())
    }

    fn scope_nested_rls(mut self, ctx: &RlsContext) -> QailBuildResult<Self> {
        for cte in &mut self.ctes {
            Self::scope_boxed_query_rls(&mut cte.base_query, ctx)?;
            if let Some(ref mut recursive_query) = cte.recursive_query {
                Self::scope_boxed_query_rls(recursive_query, ctx)?;
            }
        }

        if let Some(ref mut source_query) = self.source_query {
            Self::scope_boxed_query_rls(source_query, ctx)?;
        }

        for (_, set_query) in &mut self.set_ops {
            Self::scope_boxed_query_rls(set_query, ctx)?;
        }

        self.scope_embedded_expr_rls(ctx)?;

        Ok(self)
    }

    fn scope_value_nested_rls(value: &mut Value, ctx: &RlsContext) -> QailBuildResult<()> {
        match value {
            Value::Array(values) => {
                for value in values {
                    Self::scope_value_nested_rls(value, ctx)?;
                }
            }
            Value::Subquery(query) => {
                Self::scope_boxed_query_rls(query, ctx)?;
            }
            Value::Expr(expr) => Self::scope_expr_nested_rls(expr, ctx)?,
            _ => {}
        }

        Ok(())
    }

    fn scope_condition_nested_rls(
        condition: &mut Condition,
        ctx: &RlsContext,
    ) -> QailBuildResult<()> {
        Self::scope_expr_nested_rls(&mut condition.left, ctx)?;
        Self::scope_value_nested_rls(&mut condition.value, ctx)
    }

    fn scope_expr_nested_rls(expr: &mut Expr, ctx: &RlsContext) -> QailBuildResult<()> {
        match expr {
            Expr::Aggregate {
                filter: Some(filter),
                ..
            } => {
                for condition in filter {
                    Self::scope_condition_nested_rls(condition, ctx)?;
                }
            }
            Expr::Cast { expr, .. } | Expr::Mod { col: expr, .. } | Expr::Collate { expr, .. } => {
                Self::scope_expr_nested_rls(expr, ctx)?;
            }
            Expr::Window { params, order, .. } => {
                for expr in params {
                    Self::scope_expr_nested_rls(expr, ctx)?;
                }
                for cage in order {
                    for condition in &mut cage.conditions {
                        Self::scope_condition_nested_rls(condition, ctx)?;
                    }
                }
            }
            Expr::Case {
                when_clauses,
                else_value,
                ..
            } => {
                for (condition, then_expr) in when_clauses {
                    Self::scope_condition_nested_rls(condition, ctx)?;
                    Self::scope_expr_nested_rls(then_expr, ctx)?;
                }
                if let Some(expr) = else_value {
                    Self::scope_expr_nested_rls(expr, ctx)?;
                }
            }
            Expr::FunctionCall { args, .. } => {
                for expr in args {
                    Self::scope_expr_nested_rls(expr, ctx)?;
                }
            }
            Expr::SpecialFunction { args, .. } => {
                for (_, expr) in args {
                    Self::scope_expr_nested_rls(expr, ctx)?;
                }
            }
            Expr::Binary { left, right, .. } => {
                Self::scope_expr_nested_rls(left, ctx)?;
                Self::scope_expr_nested_rls(right, ctx)?;
            }
            Expr::Literal(value) => Self::scope_value_nested_rls(value, ctx)?,
            Expr::ArrayConstructor { elements, .. } | Expr::RowConstructor { elements, .. } => {
                for expr in elements {
                    Self::scope_expr_nested_rls(expr, ctx)?;
                }
            }
            Expr::Subscript { expr, index, .. } => {
                Self::scope_expr_nested_rls(expr, ctx)?;
                Self::scope_expr_nested_rls(index, ctx)?;
            }
            Expr::FieldAccess { expr, .. } => Self::scope_expr_nested_rls(expr, ctx)?,
            Expr::Subquery { query, .. } | Expr::Exists { query, .. } => {
                Self::scope_boxed_query_rls(query, ctx)?;
            }
            Expr::Star
            | Expr::Named(_)
            | Expr::Aliased { .. }
            | Expr::Aggregate { filter: None, .. }
            | Expr::Def { .. }
            | Expr::JsonAccess { .. } => {}
        }

        Ok(())
    }

    fn scope_embedded_expr_rls(&mut self, ctx: &RlsContext) -> QailBuildResult<()> {
        for expr in &mut self.columns {
            Self::scope_expr_nested_rls(expr, ctx)?;
        }
        for expr in &mut self.distinct_on {
            Self::scope_expr_nested_rls(expr, ctx)?;
        }
        if let Some(returning) = &mut self.returning {
            for expr in returning {
                Self::scope_expr_nested_rls(expr, ctx)?;
            }
        }
        for cage in &mut self.cages {
            for condition in &mut cage.conditions {
                Self::scope_condition_nested_rls(condition, ctx)?;
            }
        }
        for condition in &mut self.having {
            Self::scope_condition_nested_rls(condition, ctx)?;
        }
        for join in &mut self.joins {
            if let Some(conditions) = &mut join.on {
                for condition in conditions {
                    Self::scope_condition_nested_rls(condition, ctx)?;
                }
            }
        }
        if let Some(on_conflict) = &mut self.on_conflict {
            for condition in &mut on_conflict.where_conditions {
                Self::scope_condition_nested_rls(condition, ctx)?;
            }
        }
        if let Some(on_conflict) = &mut self.on_conflict
            && let crate::ast::ConflictAction::DoUpdate { assignments } = &mut on_conflict.action
        {
            for (_, expr) in assignments {
                Self::scope_expr_nested_rls(expr, ctx)?;
            }
        }
        if let Some(merge) = &mut self.merge {
            for condition in &mut merge.on {
                Self::scope_condition_nested_rls(condition, ctx)?;
            }
            for clause in &mut merge.clauses {
                for condition in &mut clause.condition {
                    Self::scope_condition_nested_rls(condition, ctx)?;
                }
                match &mut clause.action {
                    MergeAction::Update { assignments } => {
                        for (_, expr) in assignments {
                            Self::scope_expr_nested_rls(expr, ctx)?;
                        }
                    }
                    MergeAction::Insert { values, .. } => {
                        for expr in values {
                            Self::scope_expr_nested_rls(expr, ctx)?;
                        }
                    }
                    MergeAction::Delete | MergeAction::DoNothing => {}
                }
            }
        }

        Ok(())
    }

    fn scope_update_tenant(self, tenant_col: &str, ctx: &RlsContext) -> QailBuildResult<Self> {
        self.reject_tenant_payload_mutation(tenant_col)?;
        let condition_col = self.primary_tenant_condition_col(tenant_col);
        Ok(self.scope_to_tenant(&condition_col, ctx))
    }

    fn scope_update_global(self, tenant_col: &str) -> QailBuildResult<Self> {
        self.reject_tenant_payload_mutation(tenant_col)?;
        let condition_col = self.primary_tenant_condition_col(tenant_col);
        Ok(self.scope_to_global(&condition_col))
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
    fn scope_to_tenant(self, tenant_col: &str, ctx: &RlsContext) -> Self {
        self.scope_to_value(tenant_col, Value::String(ctx.tenant_id.clone()))
    }

    /// Inject a `WHERE col = value` filter, ANDed into the existing Filter cage.
    fn scope_to_value(self, col: &str, value: Value) -> Self {
        self.scope_to_condition(make_named_condition(col, value))
    }

    fn scope_to_condition(mut self, condition: Condition) -> Self {
        let col = match &condition.left {
            Expr::Named(name) => name.clone(),
            _ => String::new(),
        };
        let primary = self.primary_relation_qualifier();
        let joined = self.join_qualifiers();

        // Try to append to existing filter cage. Replace only a predicate on
        // the SAME relation's scope column — a `b.tenant_id` predicate from a
        // CROSS-joined relation must survive an `a.tenant_id` injection.
        let existing = self
            .cages
            .iter_mut()
            .find(|c| matches!(c.kind, CageKind::Filter) && c.logical_op == LogicalOp::And);

        if let Some(cage) = existing {
            cage.conditions.retain(|cond| {
                !matches!(&cond.left, Expr::Named(existing)
                    if same_scoped_column(existing, &col, &primary, &joined))
            });
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

    /// Alias if the primary relation has one, else its base name.
    fn primary_relation_qualifier(&self) -> String {
        let (base, alias) = split_table_reference(&self.table);
        alias.unwrap_or(base).to_string()
    }

    /// Normalized qualifier (alias or base name) of every JOINed relation.
    fn join_qualifiers(&self) -> Vec<String> {
        self.joins
            .iter()
            .map(|join| {
                let (base, alias) = split_table_reference(&join.table);
                normalize_ident(alias.unwrap_or(base))
            })
            .collect()
    }

    fn primary_tenant_condition_col(&self, tenant_col: &str) -> String {
        let (_, alias) = split_table_reference(&self.table);
        alias
            .map(|alias| format!("{alias}.{tenant_col}"))
            .unwrap_or_else(|| tenant_col.to_string())
    }

    /// Inject a `WHERE tenant_col IS NULL` filter for global/platform reads.
    /// Same qualifier-aware de-duplication as the tenant path.
    fn scope_to_global(self, tenant_col: &str) -> Self {
        self.scope_to_condition(Self::scope_condition(tenant_col, None))
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
        self.scope_merge_query_source(ctx, tenant_col)?;
        self.reject_merge_tenant_update_mutation(tenant_col)?;
        let target_col = self.merge_target_tenant_col(tenant_col);
        let source_col = self.merge_source_tenant_col(tenant_col)?;
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
        self.scope_merge_query_source(&RlsContext::global(), tenant_col)?;
        self.reject_merge_tenant_update_mutation(tenant_col)?;
        let target_col = self.merge_target_tenant_col(tenant_col);
        let source_col = self.merge_source_tenant_col(tenant_col)?;
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

    fn scope_merge_query_source(
        &mut self,
        ctx: &RlsContext,
        tenant_col: &str,
    ) -> QailBuildResult<()> {
        let has_query_source = matches!(
            self.merge.as_ref().map(|merge| &merge.source),
            Some(MergeSource::Query { .. })
        );
        let Some(source_tenant_col) = self.merge_query_source_tenant_col(tenant_col)? else {
            if has_query_source {
                return Err(QailBuildError::RlsMergeSourceTenantProjectionRequired {
                    table: self.table.clone(),
                    tenant_column: tenant_col.to_string(),
                });
            }
            return Ok(());
        };
        let target_table = self.table.clone();

        let Some(merge) = &mut self.merge else {
            return Ok(());
        };
        let MergeSource::Query { query, .. } = &mut merge.source else {
            return Ok(());
        };

        let scoped_query = std::mem::take(query.as_mut()).with_rls(ctx)?;
        let scoped_query = ensure_merge_query_source_projects_tenant(
            scoped_query,
            &target_table,
            &source_tenant_col,
        )?;
        **query = scoped_query;
        Ok(())
    }

    fn merge_target_tenant_col(&self, tenant_col: &str) -> String {
        let (target_table, inline_alias) = split_table_reference(&self.table);
        let qualifier = self
            .merge
            .as_ref()
            .and_then(|merge| merge.target_alias.as_ref())
            .map(String::as_str)
            .or(inline_alias)
            .unwrap_or(target_table);
        format!("{qualifier}.{tenant_col}")
    }

    fn merge_source_tenant_col(&self, tenant_col: &str) -> QailBuildResult<Option<String>> {
        let Some(merge) = self.merge.as_ref() else {
            return Ok(None);
        };
        match &merge.source {
            MergeSource::Table { name, alias } => {
                let (source_table, inline_alias) = split_table_reference(name);
                let Some(source_tenant_col) = tenant_column_for(source_table)? else {
                    return Ok(None);
                };
                let qualifier = alias.as_deref().or(inline_alias).unwrap_or(source_table);
                Ok(Some(format!("{qualifier}.{source_tenant_col}")))
            }
            MergeSource::Query { query, alias } => {
                let Some(source_tenant_col) = self.merge_query_source_tenant_col(tenant_col)?
                else {
                    return Ok(None);
                };
                let Some(qualifier) = alias.as_deref() else {
                    return Ok(None);
                };
                if query_projects_tenant_col(query, &source_tenant_col) {
                    Ok(Some(format!("{qualifier}.{source_tenant_col}")))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn merge_query_source_tenant_col(&self, tenant_col: &str) -> QailBuildResult<Option<String>> {
        let Some(merge) = self.merge.as_ref() else {
            return Ok(None);
        };
        let MergeSource::Query { query, .. } = &merge.source else {
            return Ok(None);
        };

        let (source_table, _) = split_table_reference(&query.table);
        if let Some(source_tenant_col) = tenant_column_for(source_table)? {
            return Ok(Some(source_tenant_col));
        }

        if query_projects_tenant_col(query, tenant_col)
            || self.cte_exposes_tenant_col(source_table, tenant_col)?
        {
            return Ok(Some(tenant_col.to_string()));
        }

        Ok(None)
    }

    fn cte_exposes_tenant_col(&self, cte_name: &str, tenant_col: &str) -> QailBuildResult<bool> {
        let Some(cte) = self
            .ctes
            .iter()
            .find(|cte| normalize_ident(&cte.name) == normalize_ident(cte_name))
        else {
            return Ok(false);
        };
        if !cte.columns.is_empty() {
            return Ok(cte
                .columns
                .iter()
                .any(|col| normalize_ident(col) == normalize_ident(tenant_col)));
        }
        let (base_table, _) = split_table_reference(&cte.base_query.table);
        if query_projects_tenant_col(&cte.base_query, tenant_col) {
            return Ok(true);
        }
        Ok(tenant_column_for(base_table)?
            .is_some_and(|col| normalize_ident(&col) == normalize_ident(tenant_col)))
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
    use crate::ast::JoinKind;
    use crate::transpiler::ToSql;

    // Each test uses a UNIQUE table name to avoid parallel-test interference
    // on the global registries.
    //
    // Registration goes through the application-boundary API so the process
    // is sealed `Initialized` the only way production can be: with at least
    // one table registered. The low-level `register_*` helpers are
    // mode-neutral and would leave `with_rls` at `RlsRegistryUninitialized`.

    fn register_tenant_table(table: &str, column: &str) {
        crate::rls::init_scope_registries_from_tables(&[(table, column)], &[])
            .expect("boundary registration");
    }

    fn register_owner_table(table: &str, column: &str) {
        crate::rls::init_scope_registries_from_tables(&[], &[(table, column)])
            .expect("boundary registration");
    }

    /// `with_rls` on an unregistered table must be a no-op ONLY once the
    /// process is sealed `Initialized` by a real registration.
    fn ensure_initialized() {
        register_tenant_table("_rls_tests_sentinel", "tenant_id");
    }

    // ── Owner scope + fail-closed ────────────────────────────────────

    #[test]
    fn owner_scope_injects_user_filter_on_get() {
        register_owner_table("_rls_owner_listings", "seller_id");
        let ctx = RlsContext::user("u-1");
        let sql = Qail::get("_rls_owner_listings")
            .with_rls(&ctx)
            .expect("owner scope applies")
            .to_sql();
        assert!(sql.contains("seller_id = 'u-1'"), "{sql}");
    }

    #[test]
    fn owner_scope_sets_payload_on_add_and_filters_set() {
        register_owner_table("_rls_owner_posts", "author_id");
        let ctx = RlsContext::user("u-9");
        let add = Qail::add("_rls_owner_posts")
            .set_value("title", "hi")
            .with_rls(&ctx)
            .expect("add scoped")
            .to_sql();
        assert!(add.contains("'u-9'"), "{add}");

        let set = Qail::set("_rls_owner_posts")
            .set_value("title", "edited")
            .with_rls(&ctx)
            .expect("set scoped")
            .to_sql();
        assert!(set.contains("author_id = 'u-9'"), "{set}");

        let err = Qail::set("_rls_owner_posts")
            .set_value("author_id", "someone-else")
            .with_rls(&ctx)
            .expect_err("owner column mutation must be refused");
        assert!(matches!(
            err,
            QailBuildError::RlsTenantColumnMutationDenied { .. }
        ));
    }

    #[test]
    fn tenant_and_owner_scopes_are_anded() {
        register_tenant_table("_rls_both_notes", "tenant_id");
        register_owner_table("_rls_both_notes", "user_id");
        let ctx = RlsContext::tenant("t-1").with_user("u-1");
        let sql = Qail::get("_rls_both_notes")
            .with_rls(&ctx)
            .expect("both scopes apply")
            .to_sql();
        assert!(sql.contains("tenant_id = 't-1'"), "{sql}");
        assert!(sql.contains("user_id = 'u-1'"), "{sql}");
        assert!(sql.contains(" AND "), "{sql}");
    }

    #[test]
    fn registered_tenant_table_fails_closed_without_tenant() {
        register_tenant_table("_rls_fc_orders", "tenant_id");
        let err = Qail::get("_rls_fc_orders")
            .with_rls(&RlsContext::user("u-1"))
            .expect_err("user-only context on a tenant table must not silently run unscoped");
        assert!(
            matches!(
                &err,
                QailBuildError::RlsScopeMissing {
                    scope: "tenant",
                    ..
                }
            ),
            "{err:?}"
        );
        let err = Qail::get("_rls_fc_orders")
            .with_rls(&RlsContext::empty())
            .expect_err("empty context must fail closed");
        assert!(matches!(err, QailBuildError::RlsScopeMissing { .. }));
    }

    #[test]
    fn registry_lookup_failure_maps_to_registry_unavailable_not_unscoped() {
        // The process registries cannot be poisoned here without breaking
        // sibling tests, so feed the mapping layer a real registry Err (the
        // lock-level poison tests in rls::tests prove lookups produce one).
        assert!(
            matches!(super::tenant_column_for("_rls_probe_unreachable"), Ok(None)),
            "healthy registry → unregistered"
        );

        let mapped =
            super::map_registry_lookup("orders", Err("tenant registry lock poisoned".to_string()))
                .expect_err("registry Err must never become Ok(None)");
        match mapped {
            QailBuildError::RlsRegistryUnavailable { table, reason } => {
                assert_eq!(table, "orders");
                assert!(reason.contains("poisoned"), "{reason}");
            }
            other => panic!("wrong variant: {other:?}"),
        }
        assert_eq!(
            super::map_registry_lookup("orders", Ok(Some("tenant_id".into()))).unwrap(),
            Some("tenant_id".to_string())
        );
    }

    #[test]
    fn registered_owner_table_fails_closed_without_user() {
        register_owner_table("_rls_fc_listings", "seller_id");
        let err = Qail::get("_rls_fc_listings")
            .with_rls(&RlsContext::tenant("t-1"))
            .expect_err("tenant-only context on an owner table must fail closed");
        assert!(
            matches!(&err, QailBuildError::RlsScopeMissing { scope: "user", .. }),
            "{err:?}"
        );
        assert!(
            Qail::get("_rls_fc_listings")
                .with_rls(&RlsContext::global())
                .is_err(),
            "global context carries no user"
        );
    }

    #[test]
    fn owner_scope_rejects_merge_explicitly() {
        register_owner_table("_rls_owner_merge", "owner_id");
        let ctx = RlsContext::user("u-1");
        let err = Qail::merge_into("_rls_owner_merge")
            .with_rls(&ctx)
            .expect_err("owner-scoped merge is unsupported, not silently unscoped");
        assert!(matches!(
            err,
            QailBuildError::RlsOwnerMergeUnsupported { .. }
        ));
    }

    #[test]
    fn unregistered_table_with_user_only_context_stays_no_op() {
        ensure_initialized();
        let ctx = RlsContext::user("u-1");
        let sql = Qail::get("_rls_unregistered_reference")
            .with_rls(&ctx)
            .expect("no registry entry → untouched")
            .to_sql();
        assert!(!sql.contains("WHERE"), "{sql}");
    }

    #[test]
    fn super_admin_bypasses_owner_scope() {
        register_owner_table("_rls_owner_admin", "seller_id");
        let token = crate::rls::SuperAdminToken::for_system_process("owner_test");
        let sql = Qail::get("_rls_owner_admin")
            .with_rls(&RlsContext::super_admin(token))
            .expect("bypass")
            .to_sql();
        assert!(!sql.contains("seller_id"), "{sql}");
    }

    // ── Nested / joined / upsert coverage ───────────────────────────

    #[test]
    fn owner_table_inside_cte_is_scoped_even_when_outer_is_unregistered() {
        register_owner_table("_rls_cte_inner_listings", "seller_id");
        let ctx = RlsContext::user("u-1");
        let inner = Qail::get("_rls_cte_inner_listings");
        let sql = Qail::get("mine")
            .with("mine", inner)
            .with_rls(&ctx)
            .expect("outer unregistered, inner owner table must still be scoped")
            .to_sql();
        assert!(sql.contains("seller_id = 'u-1'"), "{sql}");
    }

    #[test]
    fn tenant_table_inside_cte_fails_closed_under_user_only_context() {
        register_tenant_table("_rls_cte_inner_orders", "tenant_id");
        let inner = Qail::get("_rls_cte_inner_orders");
        let err = Qail::get("mine")
            .with("mine", inner)
            .with_rls(&RlsContext::user("u-1"))
            .expect_err("inner tenant table must not run unscoped");
        assert!(matches!(
            err,
            QailBuildError::RlsScopeMissing {
                scope: "tenant",
                ..
            }
        ));
    }

    #[test]
    fn joined_owner_relation_gets_on_predicate() {
        register_owner_table("_rls_join_threads", "owner_id");
        let ctx = RlsContext::user("u-7");
        let sql = Qail::get("_rls_join_msgs")
            .join(
                JoinKind::Inner,
                "_rls_join_threads t",
                "_rls_join_msgs.thread_id",
                "t.id",
            )
            .with_rls(&ctx)
            .expect("join scoped")
            .to_sql();
        assert!(sql.contains("t.owner_id = 'u-7'"), "{sql}");
    }

    #[test]
    fn joined_tenant_relation_under_global_ctx_gets_is_null() {
        register_tenant_table("_rls_join_refs", "tenant_id");
        let sql = Qail::get("_rls_join_main")
            .join(
                JoinKind::Left,
                "_rls_join_refs",
                "_rls_join_main.ref_id",
                "_rls_join_refs.id",
            )
            .with_rls(&RlsContext::global())
            .expect("join scoped")
            .to_sql();
        assert!(sql.contains("_rls_join_refs.tenant_id IS NULL"), "{sql}");
    }

    #[test]
    fn joined_registered_relation_via_full_join_is_refused() {
        register_tenant_table("_rls_join_full", "tenant_id");
        let err = Qail::get("_rls_join_main2")
            .join(
                JoinKind::Full,
                "_rls_join_full",
                "a.id",
                "_rls_join_full.id",
            )
            .with_rls(&RlsContext::tenant("t-1"))
            .expect_err("FULL join cannot isolate");
        assert!(matches!(err, QailBuildError::RlsJoinKindUnsupported { .. }));
    }

    #[test]
    fn joined_registered_relation_fails_closed_without_scope() {
        register_owner_table("_rls_join_owned", "owner_id");
        let err = Qail::get("_rls_join_main3")
            .join(
                JoinKind::Inner,
                "_rls_join_owned",
                "a.id",
                "_rls_join_owned.id",
            )
            .with_rls(&RlsContext::tenant("t-1"))
            .expect_err("joined owner table needs a user");
        assert!(matches!(
            err,
            QailBuildError::RlsScopeMissing { scope: "user", .. }
        ));
    }

    #[test]
    fn multiple_cross_joined_registered_relations_keep_every_predicate() {
        register_tenant_table("_rls_cross_a", "tenant_id");
        register_tenant_table("_rls_cross_b", "tenant_id");
        register_tenant_table("_rls_cross_main", "tenant_id");
        let ctx = RlsContext::tenant("t1");
        let mut q = Qail::get("_rls_cross_main");
        for t in ["_rls_cross_a a", "_rls_cross_b b"] {
            q.joins.push(crate::ast::Join {
                table: t.to_string(),
                kind: JoinKind::Cross,
                on: None,
                on_true: true,
            });
        }
        let sql = q.with_rls(&ctx).expect("scoped").to_sql();
        assert!(sql.contains("a.tenant_id = 't1'"), "{sql}");
        assert!(sql.contains("b.tenant_id = 't1'"), "{sql}");
        assert!(sql.contains("tenant_id = 't1'"), "{sql}");
        assert_eq!(sql.matches("tenant_id = 't1'").count(), 3, "{sql}");
    }

    #[test]
    fn multiple_cross_joined_registered_relations_keep_every_predicate_under_global() {
        register_tenant_table("_rls_gcross_a", "tenant_id");
        register_tenant_table("_rls_gcross_b", "tenant_id");
        register_tenant_table("_rls_gcross_main", "tenant_id");
        let mut q = Qail::get("_rls_gcross_main");
        for t in ["_rls_gcross_a a", "_rls_gcross_b b"] {
            q.joins.push(crate::ast::Join {
                table: t.to_string(),
                kind: JoinKind::Cross,
                on: None,
                on_true: true,
            });
        }
        let sql = q.with_rls(&RlsContext::global()).expect("scoped").to_sql();
        assert!(sql.contains("a.tenant_id IS NULL"), "{sql}");
        assert!(sql.contains("b.tenant_id IS NULL"), "{sql}");
        assert_eq!(sql.matches("tenant_id IS NULL").count(), 3, "{sql}");
    }

    #[test]
    fn user_supplied_unqualified_scope_filter_is_replaced_not_duplicated() {
        register_tenant_table("_rls_dedup_orders", "tenant_id");
        let sql = Qail::get("_rls_dedup_orders")
            .eq("tenant_id", "spoofed")
            .with_rls(&RlsContext::tenant("t1"))
            .expect("scoped")
            .to_sql();
        assert!(!sql.contains("spoofed"), "{sql}");
        assert_eq!(sql.matches("tenant_id = 't1'").count(), 1, "{sql}");
    }

    #[test]
    fn on_conflict_where_subquery_on_registered_table_is_scoped() {
        register_tenant_table("_rls_ocw_target", "tenant_id");
        register_tenant_table("_rls_ocw_inner", "tenant_id");
        let ctx = RlsContext::tenant("t1");
        let mut q = Qail::add("_rls_ocw_target")
            .columns(["id"])
            .values(vec![Value::String("x".into())])
            .on_conflict_update(
                &["id"],
                &[("touched", Expr::Named("EXCLUDED.touched".into()))],
            );
        q.on_conflict
            .as_mut()
            .unwrap()
            .where_conditions
            .push(Condition {
                left: Expr::Named("id".into()),
                op: Operator::In,
                value: Value::Subquery(Box::new(Qail::get("_rls_ocw_inner").columns(["id"]))),
                is_array_unnest: false,
            });
        let sql = q.with_rls(&ctx).expect("scoped").to_sql();
        assert!(
            sql.contains("_rls_ocw_inner WHERE tenant_id = 't1'")
                || sql.contains("_rls_ocw_inner\" WHERE tenant_id = 't1'"),
            "nested relation inside ON CONFLICT WHERE must be scoped: {sql}"
        );
    }

    #[test]
    fn on_conflict_do_update_is_gated_by_owner_scope() {
        register_owner_table("_rls_upsert_devices", "user_id");
        let ctx = RlsContext::user("u-3");
        let sql = Qail::add("_rls_upsert_devices")
            .columns(["token", "platform"])
            .values(vec![
                Value::String("tok".into()),
                Value::String("ios".into()),
            ])
            .on_conflict_update(
                &["token"],
                &[("platform", Expr::Named("EXCLUDED.platform".into()))],
            )
            .with_rls(&ctx)
            .expect("upsert scoped")
            .to_sql();
        assert!(sql.contains("DO UPDATE SET"), "{sql}");
        assert!(sql.contains("WHERE user_id = 'u-3'"), "{sql}");
        assert!(sql.contains("'u-3'"), "{sql}");
    }

    #[test]
    fn on_conflict_do_update_cannot_reassign_scope_column() {
        register_tenant_table("_rls_upsert_tenanted", "tenant_id");
        let ctx = RlsContext::tenant("t-1");
        let err = Qail::add("_rls_upsert_tenanted")
            .columns(["id"])
            .values(vec![Value::String("x".into())])
            .on_conflict_update(
                &["id"],
                &[("tenant_id", Expr::Named("EXCLUDED.tenant_id".into()))],
            )
            .with_rls(&ctx)
            .expect_err("conflict update must not move the row to another tenant");
        assert!(matches!(
            err,
            QailBuildError::RlsTenantColumnMutationDenied { .. }
        ));
    }

    #[test]
    fn on_conflict_do_nothing_is_untouched() {
        register_tenant_table("_rls_upsert_nothing", "tenant_id");
        let sql = Qail::add("_rls_upsert_nothing")
            .columns(["id"])
            .values(vec![Value::String("x".into())])
            .on_conflict_nothing(&["id"])
            .with_rls(&RlsContext::tenant("t-1"))
            .expect("ok")
            .to_sql();
        assert!(sql.contains("DO NOTHING"), "{sql}");
        assert!(!sql.contains("DO NOTHING WHERE"), "{sql}");
    }

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
    fn test_with_rls_resolves_primary_table_alias_on_get() {
        register_tenant_table("_rls_alias_get_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-alias");
        let query = Qail::get("_rls_alias_get_orders")
            .table_alias("o")
            .with_rls(&ctx)
            .expect("rls should apply through primary table alias");

        let sql = query.to_sql();
        assert!(
            sql.contains("FROM _rls_alias_get_orders o"),
            "expected aliased FROM table: {sql}"
        );
        assert!(
            sql.contains("WHERE o.tenant_id = 'tenant-alias'"),
            "RLS tenant filter should use the primary alias: {sql}"
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
        ensure_initialized();
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
    fn test_with_rls_resolves_primary_table_alias_on_set() {
        register_tenant_table("_rls_alias_set_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-set-alias");
        let query = Qail::set("_rls_alias_set_orders")
            .table_alias("o")
            .set_value("status", "paid")
            .with_rls(&ctx)
            .expect("rls should apply through UPDATE alias");

        let sql = query.to_sql();
        assert!(
            sql.contains("UPDATE _rls_alias_set_orders o SET status = 'paid'"),
            "expected aliased UPDATE target: {sql}"
        );
        assert!(
            sql.contains("WHERE o.tenant_id = 'tenant-set-alias'"),
            "RLS tenant filter should use the UPDATE alias: {sql}"
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
    fn test_with_rls_agent_only_fails_closed_on_tenant_table() {
        register_tenant_table("_rls_noops_orders", "tenant_id");

        // Agent-only context without tenant_id: the table is registered for
        // tenant scope, so running it unscoped would be the silent false-green.
        let ctx = RlsContext::agent("ag-only");
        let err = Qail::get("_rls_noops_orders")
            .with_rls(&ctx)
            .expect_err("missing tenant on a registered table must fail closed");
        assert!(
            matches!(
                &err,
                QailBuildError::RlsScopeMissing {
                    scope: "tenant",
                    ..
                }
            ),
            "{err:?}"
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
    fn test_with_rls_scopes_expression_subquery() {
        register_tenant_table("_rls_expr_orders", "tenant_id");
        register_tenant_table("_rls_expr_invoices", "tenant_id");

        let ctx = RlsContext::tenant("tenant-expr");
        let mut query = Qail::get("_rls_expr_orders").columns(["id"]);
        query.columns.push(Expr::Subquery {
            query: Box::new(Qail::get("_rls_expr_invoices").columns(["total"])),
            alias: Some("invoice_total".to_string()),
        });

        let query = query.with_rls(&ctx).expect("rls should apply");
        let subquery = query
            .columns
            .iter()
            .find_map(|expr| {
                if let Expr::Subquery { query, .. } = expr {
                    Some(query)
                } else {
                    None
                }
            })
            .expect("expression subquery");

        assert!(subquery.cages.iter().any(|cage| {
            matches!(cage.kind, CageKind::Filter) && cage.conditions.iter().any(|condition| {
                matches!(&condition.left, Expr::Named(name) if name == "tenant_id")
                    && matches!(&condition.value, Value::String(value) if value == "tenant-expr")
            })
        }));
    }

    #[test]
    fn test_with_rls_scopes_condition_value_subquery() {
        register_tenant_table("_rls_condition_orders", "tenant_id");
        register_tenant_table("_rls_condition_invoices", "tenant_id");

        let ctx = RlsContext::tenant("tenant-condition");
        let query = Qail::get("_rls_condition_orders")
            .filter(
                "id",
                Operator::In,
                Value::Subquery(Box::new(
                    Qail::get("_rls_condition_invoices").columns(["order_id"]),
                )),
            )
            .with_rls(&ctx)
            .expect("rls should apply");

        let subquery = query
            .cages
            .iter()
            .flat_map(|cage| &cage.conditions)
            .find_map(|condition| {
                if let Value::Subquery(query) = &condition.value {
                    Some(query)
                } else {
                    None
                }
            })
            .expect("condition subquery");

        assert!(subquery.cages.iter().any(|cage| {
            matches!(cage.kind, CageKind::Filter)
                && cage.conditions.iter().any(|condition| {
                    matches!(&condition.left, Expr::Named(name) if name == "tenant_id")
                        && matches!(&condition.value, Value::String(value) if value == "tenant-condition")
                })
        }));
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
    fn test_with_rls_scopes_merge_inline_source_alias() {
        register_tenant_table("_rls_merge_inline_target_orders", "tenant_id");
        register_tenant_table("_rls_merge_inline_source_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-inline");
        let query = Qail::merge_into("_rls_merge_inline_target_orders")
            .target_alias("t")
            .using_table("_rls_merge_inline_source_orders s")
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
            .expect("merge rls should apply through inline source alias");

        let sql = query.to_sql();
        assert!(
            sql.contains("USING _rls_merge_inline_source_orders s"),
            "MERGE source should keep inline alias: {sql}"
        );
        assert!(
            sql.contains("ON t.id = s.id AND t.tenant_id = s.tenant_id"),
            "MERGE ON must scope inline source alias tenant equality: {sql}"
        );
        assert!(
            sql.contains(
                "WHEN NOT MATCHED BY TARGET AND s.tenant_id = 'tenant-inline' THEN INSERT"
            ),
            "MERGE insert branch must scope inline source alias: {sql}"
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
        assert!(
            source_query
                .columns
                .iter()
                .any(|expr| matches!(expr, Expr::Named(name) if name == "tenant_id")),
            "MERGE query source must project tenant_id for ON classification"
        );

        let sql = query.to_sql();
        assert!(
            sql.contains("ON t.id = s.id AND t.tenant_id = s.tenant_id"),
            "MERGE query source ON must include target/source tenant equality: {sql}"
        );
        assert!(
            sql.contains("WHEN NOT MATCHED BY TARGET AND s.tenant_id = 'tenant-query' THEN INSERT"),
            "MERGE query source insert branch must be source-tenant scoped: {sql}"
        );
    }

    #[test]
    fn test_with_rls_scopes_aliased_merge_query_source_table() {
        register_tenant_table("_rls_merge_query_alias_target_orders", "tenant_id");
        register_tenant_table("_rls_merge_query_alias_source_orders", "tenant_id");

        let ctx = RlsContext::tenant("tenant-query-alias");
        let source = Qail::get("_rls_merge_query_alias_source_orders")
            .table_alias("base")
            .columns(["id", "status"]);
        let query = Qail::merge_into("_rls_merge_query_alias_target_orders")
            .target_alias("t")
            .using_query_as(source, "s")
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
            .expect("merge rls should apply through aliased source query table");

        let sql = query.to_sql();
        assert!(
            sql.contains("FROM _rls_merge_query_alias_source_orders base WHERE base.tenant_id = 'tenant-query-alias'"),
            "MERGE source query should be scoped through its base-table alias: {sql}"
        );
        assert!(
            sql.contains("ON t.id = s.id AND t.tenant_id = s.tenant_id"),
            "MERGE query source ON must include outer source tenant equality: {sql}"
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

        let sql = query.to_sql();
        assert!(
            sql.contains("ON t.id = s.id AND t.tenant_id = s.tenant_id"),
            "CTE-backed MERGE query source ON must include tenant equality: {sql}"
        );
        assert!(
            sql.contains("WHEN NOT MATCHED BY TARGET AND s.tenant_id = 'tenant-cte' THEN INSERT"),
            "CTE-backed MERGE insert branch must be source-tenant scoped: {sql}"
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

        let sql = query.to_sql();
        assert!(
            sql.contains("ON _rls_global_merge_query_target.id = s.id AND _rls_global_merge_query_target.tenant_id = s.tenant_id"),
            "global MERGE query source ON must include target/source tenant equality: {sql}"
        );
        assert!(
            sql.contains("WHEN NOT MATCHED BY TARGET AND s.tenant_id IS NULL THEN INSERT"),
            "global MERGE query source insert branch must be source-tenant scoped: {sql}"
        );
    }

    #[test]
    fn test_with_rls_rejects_merge_query_source_without_tenant_projection() {
        register_tenant_table("_rls_merge_aggregate_target", "tenant_id");
        register_tenant_table("_rls_merge_aggregate_source", "tenant_id");

        let mut source = Qail::get("_rls_merge_aggregate_source");
        source.columns.push(Expr::Aggregate {
            col: "*".to_string(),
            func: crate::ast::AggregateFunc::Count,
            distinct: false,
            filter: None,
            alias: Some("total".to_string()),
        });

        let err = Qail::merge_into("_rls_merge_aggregate_target")
            .target_alias("t")
            .using_query_as(source, "s")
            .merge_on_column("t.id", Operator::Eq, "s.id")
            .when_not_matched_insert(&["id"], &[Expr::Named("s.id".to_string())])
            .with_rls(&RlsContext::tenant("tenant-aggregate"))
            .expect_err("aggregate query source without tenant projection must fail closed");

        assert!(err.to_string().contains("MERGE query sources"));
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
