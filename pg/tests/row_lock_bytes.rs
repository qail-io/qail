//! Byte-for-byte output of the existing row-lock builders.
//!
//! Shapes mirror the claim and order-lock queries built with `for_update()`
//! and `for_update_skip_locked()`. Expected strings and params were captured
//! from the encoder before lock options (NOWAIT, OF) were added; any change
//! here changes the SQL those callers send.

use qail_core::ast::{Expr, Qail};
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;

// `with_rls` only adds scope filters ahead of the lock clause; these shapes
// leave it out so they need no RLS registry.
fn shapes() -> Vec<(&'static str, Qail)> {
    vec![
        (
            "order_lock",
            Qail::get("orders")
                .columns(["total_fare", "currency", "status", "reseller_tenant_id"])
                .eq("id", "o-1")
                .eq("tenant_id", "t-1")
                .for_update(),
        ),
        (
            "outbox_claim",
            Qail::get("whatsapp_outbox")
                .columns(["id", "tenant_id"])
                .column_expr(Expr::Cast {
                    expr: Box::new(Expr::Named("payload".to_string())),
                    target_type: "text".to_string(),
                    alias: None,
                })
                .in_vals("status", ["pending", "failed"])
                .lte("next_attempt_at", "2026-10-03T00:00:00Z")
                .order_asc("next_attempt_at")
                .limit(50)
                .for_update_skip_locked(),
        ),
        (
            "hold_sweep",
            Qail::get("holds")
                .columns(["id", "idempotency_key"])
                .eq("status", "Active")
                .eq("is_confirmed", false)
                .lt("expires_at", "2026-10-03T00:00:00Z")
                .order_asc("id")
                .limit(100)
                .for_update_skip_locked()
                .gt("id", "h-9"),
        ),
        (
            "bare_for_update",
            Qail::get("orders").column("id").eq("id", 7).for_update(),
        ),
    ]
}

fn render(name: &str, cmd: &Qail) -> String {
    let (sql, params) = AstEncoder::encode_cmd_sql(cmd).expect("native encode");
    let params: Vec<String> = params
        .iter()
        .map(|p| match p {
            Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            None => "NULL".to_string(),
        })
        .collect();
    format!(
        "{name}\n  native: {sql}\n  params: {params:?}\n  preview: {}\n",
        cmd.to_sql()
    )
}

#[test]
fn existing_lock_builders_emit_unchanged_sql() {
    let rendered: String = shapes()
        .iter()
        .map(|(name, cmd)| render(name, cmd))
        .collect();
    print!("{rendered}");
    assert_eq!(rendered, EXPECTED);
}

const EXPECTED: &str = "\
order_lock
  native: SELECT total_fare, currency, status, reseller_tenant_id FROM orders WHERE id = $1 AND tenant_id = $2 FOR UPDATE
  params: [\"o-1\", \"t-1\"]
  preview: SELECT total_fare, currency, status, reseller_tenant_id FROM orders WHERE id = 'o-1' AND tenant_id = 't-1' FOR UPDATE
outbox_claim
  native: SELECT id, tenant_id, payload::text FROM whatsapp_outbox WHERE status IN ($1, $2) AND next_attempt_at <= $3 ORDER BY next_attempt_at LIMIT 50 FOR UPDATE SKIP LOCKED
  params: [\"pending\", \"failed\", \"2026-10-03T00:00:00Z\"]
  preview: SELECT id, tenant_id, payload::text FROM whatsapp_outbox WHERE status IN ('pending', 'failed') AND next_attempt_at <= '2026-10-03T00:00:00Z' ORDER BY next_attempt_at ASC LIMIT 50 FOR UPDATE SKIP LOCKED
hold_sweep
  native: SELECT id, idempotency_key FROM holds WHERE status = $1 AND is_confirmed = $2 AND expires_at < $3 AND id > $4 ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED
  params: [\"Active\", \"f\", \"2026-10-03T00:00:00Z\", \"h-9\"]
  preview: SELECT id, idempotency_key FROM holds WHERE status = 'Active' AND is_confirmed = false AND expires_at < '2026-10-03T00:00:00Z' AND id > 'h-9' ORDER BY id ASC LIMIT 100 FOR UPDATE SKIP LOCKED
bare_for_update
  native: SELECT id FROM orders WHERE id = $1 FOR UPDATE
  params: [\"7\"]
  preview: SELECT id FROM orders WHERE id = 7 FOR UPDATE
";
