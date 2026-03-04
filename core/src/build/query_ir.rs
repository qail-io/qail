//! Shared query IR for build-time validation rules.
//!
//! This normalizes scanner/syn extraction into a single structure so
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
    pub(crate) is_cte_ref: bool,
    pub(crate) file_uses_super_admin: bool,
}

/// Build canonical query IR from scanned usages.
pub(crate) fn build_query_ir(usages: &[QailUsage]) -> Vec<QueryIr> {
    #[cfg(feature = "syn-scanner")]
    let syn_usage_index = super::syn_analyzer::build_syn_usage_index(usages);

    let mut out = Vec::with_capacity(usages.len());
    for usage in usages {
        let action = usage_action_to_ast(&usage.action);
        let mut cmd = crate::ast::Qail {
            action,
            table: usage.table.clone(),
            ..Default::default()
        };
        let (has_rls, used_syn_cmd) = {
            #[cfg(feature = "syn-scanner")]
            {
                let mut v = usage.has_rls;
                let mut used_syn = false;
                if let Some(parsed) = syn_usage_index.get(&super::syn_analyzer::syn_usage_key(
                    &usage.file,
                    usage.line,
                    usage.column,
                    &usage.action,
                    &usage.table,
                )) {
                    cmd = parsed.cmd.clone();
                    v |= parsed.has_rls;
                    used_syn = true;
                }
                (v, used_syn)
            }

            #[cfg(not(feature = "syn-scanner"))]
            {
                (usage.has_rls, false)
            }
        };

        // Only apply scanner-derived column fallback when no syn-derived command
        // was found for this usage (avoids scanner/syn drift in strict syn mode).
        if !used_syn_cmd {
            append_scanned_columns(&mut cmd, &usage.columns);
        }

        out.push(QueryIr {
            file: usage.file.clone(),
            line: usage.line,
            action: usage.action.clone(),
            table: usage.table.clone(),
            is_dynamic_table: usage.is_dynamic_table,
            cmd,
            has_rls,
            is_cte_ref: usage.is_cte_ref,
            file_uses_super_admin: usage.file_uses_super_admin,
        });
    }
    out
}
