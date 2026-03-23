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
        let action = match usage_action_to_ast(&usage.action) {
            Ok(action) => action,
            Err(err) => {
                println!(
                    "cargo:warning=QAIL: {} at {}:{} (table: {})",
                    err, usage.file, usage.line, usage.table
                );
                continue;
            }
        };
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

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(action: &str) -> QailUsage {
        QailUsage {
            file: "src/main.rs".to_string(),
            line: 42,
            column: 9,
            table: "users".to_string(),
            is_dynamic_table: false,
            columns: vec!["id".to_string()],
            action: action.to_string(),
            is_cte_ref: false,
            has_rls: false,
            has_explicit_tenant_scope: false,
            file_uses_super_admin: false,
        }
    }

    #[test]
    fn build_query_ir_skips_unknown_actions() {
        let usages = vec![usage("GET"), usage("UNKNOWN_ACTION")];
        let ir = build_query_ir(&usages);
        assert_eq!(ir.len(), 1);
        assert_eq!(ir[0].action, "GET");
    }
}
