//! PostgreSQL-specific RLS implementation.
//!
//! Uses `set_config()` to set session variables that PostgreSQL RLS
//! policies reference for tenant data isolation.
//!
//! The `RlsContext` struct lives in `qail_core::rls` (shared across all drivers).
//! This module provides the PostgreSQL-specific methods to apply it.

pub use qail_core::rls::RlsContext;

fn quote_guc_literal(value: &str) -> String {
    let sanitized = sanitize_guc_value(value);
    for idx in 0usize.. {
        let tag = if idx == 0 {
            "qail_guc".to_string()
        } else {
            format!("qail_guc_{}", idx)
        };
        let delim = format!("${}$", tag);
        if !sanitized.contains(&delim) {
            return format!("{}{}{}", delim, sanitized, delim);
        }
    }
    unreachable!("finite strings always admit an unused dollar-quote tag")
}

/// PostgreSQL-specific SQL generation for RLS context.
///
/// These functions generate the `set_config()` calls that configure
/// PostgreSQL session variables for RLS policy evaluation.
///
/// **Security**: GUC values are sanitized to prevent SQL injection via
/// crafted JWT claims (e.g., `tenant_id: "'; DROP TABLE users; --"`).
pub(crate) fn context_to_sql(ctx: &RlsContext) -> String {
    let nil_uuid = "00000000-0000-0000-0000-000000000000";
    let t_id_raw = if ctx.is_global() && ctx.tenant_id.is_empty() {
        nil_uuid
    } else {
        &ctx.tenant_id
    };
    let ag_id_raw = if ctx.is_global() && ctx.agent_id.is_empty() {
        nil_uuid
    } else {
        &ctx.agent_id
    };
    let t_id = quote_guc_literal(t_id_raw);
    let ag_id = quote_guc_literal(ag_id_raw);
    let u_id_raw = if ctx.user_id().is_empty() {
        nil_uuid
    } else {
        ctx.user_id()
    };
    let u_id = quote_guc_literal(u_id_raw);
    let is_global = quote_guc_literal(if ctx.is_global() { "true" } else { "false" });
    let is_super_admin = quote_guc_literal(if ctx.bypasses_rls() { "true" } else { "false" });
    format!(
        "BEGIN; SET LOCAL app.is_global = {}; \
         SELECT set_config('app.current_user_id', {}, true), \
                set_config('app.current_tenant_id', {}, true), \
                set_config('app.current_agent_id', {}, true), \
                set_config('app.is_super_admin', {}, true)",
        is_global, u_id, t_id, ag_id, is_super_admin,
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
    let ag_id_raw = if ctx.is_global() && ctx.agent_id.is_empty() {
        nil_uuid
    } else {
        &ctx.agent_id
    };
    let t_id = quote_guc_literal(t_id_raw);
    let ag_id = quote_guc_literal(ag_id_raw);
    let u_id_raw = if ctx.user_id().is_empty() {
        nil_uuid
    } else {
        ctx.user_id()
    };
    let u_id = quote_guc_literal(u_id_raw);
    let is_global = quote_guc_literal(if ctx.is_global() { "true" } else { "false" });
    let is_super_admin = quote_guc_literal(if ctx.bypasses_rls() { "true" } else { "false" });

    let lock_clause = if lock_timeout_ms > 0 {
        format!(" SET LOCAL lock_timeout = {};", lock_timeout_ms)
    } else {
        String::new()
    };

    format!(
        "BEGIN; SET LOCAL statement_timeout = {};{} \
         SET LOCAL app.is_global = {}; \
         SELECT set_config('app.current_user_id', {}, true), \
                set_config('app.current_tenant_id', {}, true), \
                set_config('app.current_agent_id', {}, true), \
                set_config('app.is_super_admin', {}, true)",
        statement_timeout_ms, lock_clause, is_global, u_id, t_id, ag_id, is_super_admin,
    )
}

/// Sanitize raw GUC values before embedding them into SQL.
///
/// PostgreSQL rejects interior NUL bytes in text payloads and in simple-query
/// frames. We preserve all other characters (including unicode/emoji) so
/// identity values are not silently collapsed.
pub fn sanitize_guc_value(val: &str) -> String {
    val.chars()
        .map(|c| if c == '\0' { '\u{FFFD}' } else { c })
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
    fn test_context_to_sql_tenant() {
        let ctx = RlsContext::tenant("abc-123");
        let sql = context_to_sql(&ctx);
        assert!(sql.contains("$qail_guc$abc-123$qail_guc$"));
        assert!(sql.contains("app.current_tenant_id"));
        assert!(sql.contains("SET LOCAL app.is_global = $qail_guc$false$qail_guc$"));
        assert!(sql.contains("set_config('app.is_super_admin', $qail_guc$false$qail_guc$, true)"));
    }

    #[test]
    fn test_context_to_sql_super_admin() {
        let token = SuperAdminToken::for_system_process("test_super_admin_sql");
        let ctx = RlsContext::super_admin(token);
        let sql = context_to_sql(&ctx);
        assert!(sql.contains("SET LOCAL app.is_global = $qail_guc$false$qail_guc$"));
        assert!(sql.contains("set_config('app.is_super_admin', $qail_guc$true$qail_guc$, true)"));
    }

    #[test]
    fn test_context_to_sql_global_context() {
        let ctx = RlsContext::global();
        let sql = context_to_sql(&ctx);
        assert!(sql.contains("SET LOCAL app.is_global = $qail_guc$true$qail_guc$"));
        assert!(sql.contains("00000000-0000-0000-0000-000000000000"));
        assert!(sql.contains("set_config('app.is_super_admin', $qail_guc$false$qail_guc$, true)"));
    }

    #[test]
    fn test_context_to_sql_user_context() {
        let ctx = RlsContext::user("550e8400-e29b-41d4-a716-446655440000");
        let sql = context_to_sql(&ctx);
        assert!(sql.contains("set_config('app.current_user_id'"));
        assert!(sql.contains("550e8400-e29b-41d4-a716-446655440000"));
        assert!(sql.contains("set_config('app.is_super_admin', $qail_guc$false$qail_guc$, true)"));
        assert!(sql.contains("SET LOCAL app.is_global = $qail_guc$false$qail_guc$"));
    }

    #[test]
    fn test_context_to_sql_user_empty() {
        // Empty user_id → nil UUID in session var (safe for ::uuid policy casts)
        let ctx = RlsContext::empty();
        let sql = context_to_sql(&ctx);
        assert!(
            sql.contains(
                "set_config('app.current_user_id', $qail_guc$00000000-0000-0000-0000-000000000000$qail_guc$"
            ),
            "empty user_id emits nil UUID to avoid ::uuid cast failures"
        );
    }

    #[test]
    fn redteam_user_id_sanitized() {
        let ctx = RlsContext::user("'; DROP TABLE users; --");
        let sql = context_to_sql(&ctx);
        assert!(
            sql.contains("$qail_guc$'; DROP TABLE users; --$qail_guc$"),
            "dangerous characters must remain isolated inside a quoted literal"
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
    fn redteam_guc_injection_single_quote_is_dollar_quoted() {
        let ctx = RlsContext::tenant("'; DROP TABLE users; --");
        let sql = context_to_sql(&ctx);
        let sanitized = sanitize_guc_value("'; DROP TABLE users; --");
        assert_eq!(sanitized, "'; DROP TABLE users; --");
        assert!(sql.contains("$qail_guc$'; DROP TABLE users; --$qail_guc$"));
        assert!(sql.contains("app.current_tenant_id"));
    }

    #[test]
    fn redteam_guc_injection_backslash_is_dollar_quoted() {
        let ctx = RlsContext::tenant("abc\\'; SELECT 1; --");
        let sql = context_to_sql(&ctx);
        let sanitized = sanitize_guc_value("abc\\'; SELECT 1; --");
        assert_eq!(sanitized, "abc\\'; SELECT 1; --");
        assert!(sql.contains("$qail_guc$abc\\'; SELECT 1; --$qail_guc$"));
        assert!(sql.contains("app.current_tenant_id"));
    }

    #[test]
    fn redteam_guc_injection_semicolon_preserved() {
        let input = "abc; SET app.is_super_admin = 'true'";
        let sanitized = sanitize_guc_value(input);
        assert_eq!(sanitized, input);
    }

    #[test]
    fn redteam_guc_injection_with_timeout() {
        let ctx = RlsContext::tenant("'; DROP TABLE users; --");
        let sql = context_to_sql_with_timeout(&ctx, 5000);
        assert!(sql.contains("$qail_guc$'; DROP TABLE users; --$qail_guc$"));
        assert!(sql.contains("statement_timeout = 5000"));
    }

    #[test]
    fn redteam_guc_normal_uuid_passes_through() {
        let uuid = "4fcc89a7-0753-4b8d-8457-71619533dbd8";
        let ctx = RlsContext::tenant(uuid);
        let sql = context_to_sql(&ctx);
        assert!(
            sql.contains(uuid),
            "Normal UUID must pass through unchanged"
        );
    }

    #[test]
    fn redteam_sanitize_preserves_unicode_and_symbols_except_nul() {
        assert_eq!(sanitize_guc_value("normal-uuid"), "normal-uuid");
        assert_eq!(sanitize_guc_value("ab'cd"), "ab'cd");
        assert_eq!(sanitize_guc_value("ab\\cd"), "ab\\cd");
        assert_eq!(sanitize_guc_value("ab;cd"), "ab;cd");
        assert_eq!(sanitize_guc_value("ten\0ant"), "ten\u{FFFD}ant");
        assert_eq!(sanitize_guc_value("tenant🚀"), "tenant🚀");
        assert_eq!(sanitize_guc_value(""), "");
    }

    #[test]
    fn quote_guc_literal_uses_non_colliding_tag() {
        let quoted = quote_guc_literal("$qail_guc$inside$qail_guc$");
        assert_eq!(quoted, "$qail_guc_1$$qail_guc$inside$qail_guc$$qail_guc_1$");
    }

    // ══════════════════════════════════════════════════════════════════
    // lock_timeout injection
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn lock_timeout_injected_when_nonzero() {
        let ctx = RlsContext::tenant("tenant-1");
        let sql = context_to_sql_with_timeouts(&ctx, 30_000, 5_000);
        assert!(
            sql.contains("statement_timeout = 30000"),
            "statement_timeout must be set"
        );
        assert!(
            sql.contains("lock_timeout = 5000"),
            "lock_timeout must be set when > 0"
        );
        assert!(sql.contains("SET LOCAL app.is_global = $qail_guc$false$qail_guc$"));
    }

    #[test]
    fn lock_timeout_omitted_when_zero() {
        let ctx = RlsContext::tenant("tenant-1");
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
