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
///
/// **Security**: GUC values are sanitized to prevent SQL injection via
/// crafted JWT claims (e.g., `operator_id: "'; DROP TABLE users; --"`).
pub(crate) fn context_to_sql(ctx: &RlsContext) -> String {
    let op_id = sanitize_guc_value(&ctx.operator_id);
    let ag_id = sanitize_guc_value(&ctx.agent_id);
    format!(
        "BEGIN; \
         SELECT set_config('app.current_operator_id', '{}', true), \
                set_config('app.current_agent_id', '{}', true), \
                set_config('app.is_super_admin', '{}', true)",
        op_id,
        ag_id,
        ctx.bypasses_rls(),
    )
}

/// Like `context_to_sql` but also sets `statement_timeout`.
///
/// Batches the RLS context and timeout into a single SQL to minimize
/// round-trips. The timeout (in milliseconds) prevents runaway queries.
pub(crate) fn context_to_sql_with_timeout(ctx: &RlsContext, timeout_ms: u32) -> String {
    let op_id = sanitize_guc_value(&ctx.operator_id);
    let ag_id = sanitize_guc_value(&ctx.agent_id);
    format!(
        "BEGIN; \
         SET LOCAL statement_timeout = {}; \
         SELECT set_config('app.current_operator_id', '{}', true), \
                set_config('app.current_agent_id', '{}', true), \
                set_config('app.is_super_admin', '{}', true)",
        timeout_ms,
        op_id,
        ag_id,
        ctx.bypasses_rls(),
    )
}

/// Strip characters that could break out of a SQL string literal or
/// cause C-level string truncation inside PostgreSQL.
///
/// Uses an **allowlist** approach: only printable ASCII (0x20–0x7E) is
/// allowed, with additional exclusions for `'`, `\`, `;`, and `$`.
///
/// This blocks:
/// - NUL bytes (`\x00`) — C string truncation inside `set_config()`
/// - Newlines/CR — log injection, multi-line confusion
/// - All control characters — unpredictable behavior
/// - Dollar signs — prevents `$$`-style quoting attempts
fn sanitize_guc_value(val: &str) -> String {
    val.chars()
        .filter(|c| {
            // Allowlist: printable ASCII only (space through tilde)
            let is_printable_ascii = *c >= ' ' && *c <= '~';
            // Denylist within printable range
            let is_dangerous = *c == '\'' || *c == '\\' || *c == ';' || *c == '$';
            is_printable_ascii && !is_dangerous
        })
        .collect()
}

/// SQL to commit the transaction and reset RLS context.
/// Transaction-local set_config values auto-reset on COMMIT,
/// so no explicit reset is needed — just end the transaction.
pub(crate) fn reset_sql() -> &'static str {
    "COMMIT; RESET statement_timeout"
}

#[cfg(test)]
mod tests {
    use super::*;
    use qail_core::rls::SuperAdminToken;

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
        let token = SuperAdminToken::issue();
        let ctx = RlsContext::super_admin(token);
        let sql = context_to_sql(&ctx);
        assert!(sql.contains("'true'")); // is_super_admin
    }

    #[test]
    fn test_reset_sql() {
        let sql = reset_sql();
        assert!(sql.contains("COMMIT"), "Should COMMIT the transaction");
        assert!(sql.contains("RESET statement_timeout"), "Should reset statement_timeout");
    }

    // ══════════════════════════════════════════════════════════════════
    // RED-TEAM: GUC Injection Tests (#6 from adversarial checklist)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn redteam_guc_injection_single_quote_stripped() {
        let ctx = RlsContext::operator("'; DROP TABLE users; --");
        let sql = context_to_sql(&ctx);
        // Quotes and semicolons stripped → value is harmless inside set_config()
        // The value becomes " DROP TABLE users --" (inert string, not executable SQL)
        let sanitized = sanitize_guc_value("'; DROP TABLE users; --");
        assert!(!sanitized.contains('\''), "Single quotes must be stripped");
        assert!(!sanitized.contains(';'), "Semicolons must be stripped");
        assert!(sql.contains("app.current_operator_id"));
    }

    #[test]
    fn redteam_guc_injection_backslash_stripped() {
        let ctx = RlsContext::operator("abc\\'; SELECT 1; --");
        let sql = context_to_sql(&ctx);
        let sanitized = sanitize_guc_value("abc\\'; SELECT 1; --");
        assert!(!sanitized.contains('\\'), "Backslashes must be stripped");
        assert!(!sanitized.contains('\''), "Quotes must be stripped");
        assert!(!sanitized.contains(';'), "Semicolons must be stripped");
        // The resulting SQL has the value safely inside set_config quotes
        assert!(sql.contains("app.current_operator_id"));
    }

    #[test]
    fn redteam_guc_injection_semicolon_stripped() {
        let input = "abc; SET app.is_super_admin = 'true'";
        let sanitized = sanitize_guc_value(input);
        // Semicolons and quotes stripped — cannot break out of set_config
        assert!(!sanitized.contains(';'), "Semicolons must be stripped");
        assert!(!sanitized.contains('\''), "Quotes must be stripped");
        assert_eq!(sanitized, "abc SET app.is_super_admin = true");
    }

    #[test]
    fn redteam_guc_injection_with_timeout() {
        let ctx = RlsContext::operator("'; DROP TABLE users; --");
        let sql = context_to_sql_with_timeout(&ctx, 5000);
        // The injected value is sanitized — no quote/semicolon escape
        assert!(!sql.contains("''; DROP"), "Injection must not escape set_config quotes");
        assert!(sql.contains("statement_timeout = 5000"));
    }

    #[test]
    fn redteam_guc_normal_uuid_passes_through() {
        let uuid = "4fcc89a7-0753-4b8d-8457-71619533dbd8";
        let ctx = RlsContext::operator(uuid);
        let sql = context_to_sql(&ctx);
        assert!(sql.contains(uuid), "Normal UUID must pass through unchanged");
    }

    #[test]
    fn redteam_sanitize_strips_dangerous_chars() {
        assert_eq!(sanitize_guc_value("normal-uuid"), "normal-uuid");
        assert_eq!(sanitize_guc_value("ab'cd"), "abcd");
        assert_eq!(sanitize_guc_value("ab\\cd"), "abcd");
        assert_eq!(sanitize_guc_value("ab;cd"), "abcd");
        assert_eq!(sanitize_guc_value("'; DROP TABLE x; --"), " DROP TABLE x --");
        assert_eq!(sanitize_guc_value(""), "");
    }
}
