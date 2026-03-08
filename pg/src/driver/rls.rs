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
    let nil_uuid = "00000000-0000-0000-0000-000000000000";
    let t_id_raw = if ctx.is_global() && ctx.tenant_id.is_empty() {
        nil_uuid
    } else {
        &ctx.tenant_id
    };
    let op_id_raw = if ctx.is_global() && ctx.operator_id.is_empty() {
        nil_uuid
    } else {
        &ctx.operator_id
    };
    let ag_id_raw = if ctx.is_global() && ctx.agent_id.is_empty() {
        nil_uuid
    } else {
        &ctx.agent_id
    };
    let t_id = sanitize_guc_value(t_id_raw);
    let op_id = sanitize_guc_value(op_id_raw);
    let ag_id = sanitize_guc_value(ag_id_raw);
    let u_id_raw = if ctx.user_id().is_empty() {
        nil_uuid
    } else {
        ctx.user_id()
    };
    let u_id = sanitize_guc_value(u_id_raw);
    let is_global = if ctx.is_global() { "true" } else { "false" };
    format!(
        "BEGIN; SET LOCAL app.is_global = '{}'; \
         SELECT set_config('app.current_user_id', '{}', true), \
                set_config('app.current_tenant_id', '{}', true), \
                set_config('app.current_operator_id', '{}', true), \
                set_config('app.current_agent_id', '{}', true), \
                set_config('app.is_super_admin', '{}', true)",
        is_global,
        u_id,
        t_id,
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
    context_to_sql_with_timeouts(ctx, timeout_ms, 0)
}

/// Like `context_to_sql_with_timeout` but also sets `lock_timeout`.
///
/// When `lock_timeout_ms` is 0, the `SET LOCAL lock_timeout` clause is omitted
/// (PostgreSQL default: no timeout).
pub(crate) fn context_to_sql_with_timeouts(
    ctx: &RlsContext,
    statement_timeout_ms: u32,
    lock_timeout_ms: u32,
) -> String {
    let nil_uuid = "00000000-0000-0000-0000-000000000000";
    let t_id_raw = if ctx.is_global() && ctx.tenant_id.is_empty() {
        nil_uuid
    } else {
        &ctx.tenant_id
    };
    let op_id_raw = if ctx.is_global() && ctx.operator_id.is_empty() {
        nil_uuid
    } else {
        &ctx.operator_id
    };
    let ag_id_raw = if ctx.is_global() && ctx.agent_id.is_empty() {
        nil_uuid
    } else {
        &ctx.agent_id
    };
    let t_id = sanitize_guc_value(t_id_raw);
    let op_id = sanitize_guc_value(op_id_raw);
    let ag_id = sanitize_guc_value(ag_id_raw);
    let u_id_raw = if ctx.user_id().is_empty() {
        nil_uuid
    } else {
        ctx.user_id()
    };
    let u_id = sanitize_guc_value(u_id_raw);
    let is_global = if ctx.is_global() { "true" } else { "false" };

    let lock_clause = if lock_timeout_ms > 0 {
        format!(" SET LOCAL lock_timeout = {};", lock_timeout_ms)
    } else {
        String::new()
    };

    format!(
        "BEGIN; SET LOCAL statement_timeout = {};{} \
         SET LOCAL app.is_global = '{}'; \
         SELECT set_config('app.current_user_id', '{}', true), \
                set_config('app.current_tenant_id', '{}', true), \
                set_config('app.current_operator_id', '{}', true), \
                set_config('app.current_agent_id', '{}', true), \
                set_config('app.is_super_admin', '{}', true)",
        statement_timeout_ms,
        lock_clause,
        is_global,
        u_id,
        t_id,
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
pub fn sanitize_guc_value(val: &str) -> String {
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
/// `SET LOCAL statement_timeout` is also transaction-scoped and
/// auto-resets on COMMIT — no separate RESET needed.
pub(crate) fn reset_sql() -> &'static str {
    "COMMIT"
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
        assert!(sql.contains("app.current_tenant_id"));
        assert!(sql.contains("app.current_operator_id"));
        assert!(sql.contains("SET LOCAL app.is_global = 'false'"));
        assert!(sql.contains("'false'")); // is_super_admin
    }

    #[test]
    fn test_context_to_sql_super_admin() {
        let token = SuperAdminToken::for_system_process("test_super_admin_sql");
        let ctx = RlsContext::super_admin(token);
        let sql = context_to_sql(&ctx);
        assert!(sql.contains("SET LOCAL app.is_global = 'false'"));
        assert!(sql.contains("'true'")); // is_super_admin
    }

    #[test]
    fn test_context_to_sql_global_context() {
        let ctx = RlsContext::global();
        let sql = context_to_sql(&ctx);
        assert!(sql.contains("SET LOCAL app.is_global = 'true'"));
        assert!(sql.contains("00000000-0000-0000-0000-000000000000"));
        assert!(sql.contains("'false'")); // is_super_admin remains false
    }

    #[test]
    fn test_context_to_sql_user_context() {
        let ctx = RlsContext::user("550e8400-e29b-41d4-a716-446655440000");
        let sql = context_to_sql(&ctx);
        assert!(
            sql.contains("set_config('app.current_user_id', '550e8400-e29b-41d4-a716-446655440000'"),
            "user_id must be set in session SQL"
        );
        assert!(sql.contains("'false'")); // is_super_admin remains false
        assert!(sql.contains("SET LOCAL app.is_global = 'false'"));
    }

    #[test]
    fn test_context_to_sql_user_empty() {
        // Empty user_id → nil UUID in session var (safe for ::uuid policy casts)
        let ctx = RlsContext::empty();
        let sql = context_to_sql(&ctx);
        assert!(
            sql.contains("set_config('app.current_user_id', '00000000-0000-0000-0000-000000000000'"),
            "empty user_id emits nil UUID to avoid ::uuid cast failures"
        );
    }

    #[test]
    fn redteam_user_id_sanitized() {
        let ctx = RlsContext::user("'; DROP TABLE users; --");
        let sql = context_to_sql(&ctx);
        assert!(
            !sql.contains("'; DROP"),
            "user_id injection must be sanitized"
        );
        assert!(sql.contains("app.current_user_id"));
    }

    #[test]
    fn test_reset_sql() {
        let sql = reset_sql();
        assert_eq!(sql, "COMMIT", "Should just COMMIT (SET LOCAL auto-resets)");
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
        assert!(
            !sql.contains("''; DROP"),
            "Injection must not escape set_config quotes"
        );
        assert!(sql.contains("statement_timeout = 5000"));
    }

    #[test]
    fn redteam_guc_normal_uuid_passes_through() {
        let uuid = "4fcc89a7-0753-4b8d-8457-71619533dbd8";
        let ctx = RlsContext::operator(uuid);
        let sql = context_to_sql(&ctx);
        assert!(
            sql.contains(uuid),
            "Normal UUID must pass through unchanged"
        );
    }

    #[test]
    fn redteam_sanitize_strips_dangerous_chars() {
        assert_eq!(sanitize_guc_value("normal-uuid"), "normal-uuid");
        assert_eq!(sanitize_guc_value("ab'cd"), "abcd");
        assert_eq!(sanitize_guc_value("ab\\cd"), "abcd");
        assert_eq!(sanitize_guc_value("ab;cd"), "abcd");
        assert_eq!(
            sanitize_guc_value("'; DROP TABLE x; --"),
            " DROP TABLE x --"
        );
        assert_eq!(sanitize_guc_value(""), "");
    }

    // ══════════════════════════════════════════════════════════════════
    // lock_timeout injection
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn lock_timeout_injected_when_nonzero() {
        let ctx = RlsContext::operator("tenant-1");
        let sql = context_to_sql_with_timeouts(&ctx, 30_000, 5_000);
        assert!(
            sql.contains("statement_timeout = 30000"),
            "statement_timeout must be set"
        );
        assert!(
            sql.contains("lock_timeout = 5000"),
            "lock_timeout must be set when > 0"
        );
        assert!(sql.contains("SET LOCAL app.is_global = 'false'"));
    }

    #[test]
    fn lock_timeout_omitted_when_zero() {
        let ctx = RlsContext::operator("tenant-1");
        let sql = context_to_sql_with_timeouts(&ctx, 30_000, 0);
        assert!(
            sql.contains("statement_timeout = 30000"),
            "statement_timeout must be set"
        );
        assert!(
            !sql.contains("lock_timeout"),
            "lock_timeout must be omitted when 0"
        );
    }
}
