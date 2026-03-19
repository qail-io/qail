//! Shared query IR for build-time validation rules.
//!
//! This normalizes scanner extraction into a single structure so
//! downstream rules (schema, RLS audits, future security checks) consume
//! one canonical representation.

use super::scanner::{QailUsage, append_scanned_columns, usage_action_to_ast};

/// Canonical build-time query representation.
#[derive(Debug, Clone)]
pub(crate) struct QueryIr {
    pub(crate) file: String,
    pub(crate) line: usize,
    pub(crate) action: String,
    pub(crate) table: String,
    pub(crate) is_dynamic_table: bool,
    pub(crate) cmd: crate::ast::Qail,
    pub(crate) has_rls: bool,
    pub(crate) has_explicit_tenant_scope: bool,
    pub(crate) is_cte_ref: bool,
    pub(crate) file_uses_super_admin: bool,
}

/// Build canonical query IR from scanned usages.
pub(crate) fn build_query_ir(usages: &[QailUsage]) -> Vec<QueryIr> {
    let mut out = Vec::with_capacity(usages.len());
    for usage in usages {
        let action = usage_action_to_ast(&usage.action);
        let mut cmd = crate::ast::Qail {
            action,
            table: usage.table.clone(),
            ..Default::default()
        };
        let has_rls = usage.has_rls;
        append_scanned_columns(&mut cmd, &usage.columns);

        out.push(QueryIr {
            file: usage.file.clone(),
            line: usage.line,
            action: usage.action.clone(),
            table: usage.table.clone(),
            is_dynamic_table: usage.is_dynamic_table,
            cmd,
            has_rls,
            has_explicit_tenant_scope: usage.has_explicit_tenant_scope,
            is_cte_ref: usage.is_cte_ref,
            file_uses_super_admin: usage.file_uses_super_admin,
        });
    }
    out
}
