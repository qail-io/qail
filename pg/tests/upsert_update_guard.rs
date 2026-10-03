//! Native encoder regressions for the existing-row ON CONFLICT guard.

use qail_core::ast::{Condition, Expr, Operator, Qail, Value};
use qail_core::prelude::eq;
use qail_pg::protocol::AstEncoder;

fn upsert() -> Qail {
    Qail::add("guard_rows")
        .set_value("id", 1)
        .set_value("status", "proposed")
        .on_conflict_update(
            &["id"],
            &[("status", Expr::Named("EXCLUDED.status".into()))],
        )
}

fn guarded(conditions: Vec<Condition>) -> Qail {
    let mut cmd = upsert();
    cmd.on_conflict.as_mut().unwrap().where_conditions = conditions;
    cmd
}

#[test]
fn conflict_guard_binds_after_payload_before_returning() {
    let cmd =
        guarded(vec![eq("guard_rows.tenant_id", "tenant.a'quoted")]).returning(["id", "status"]);
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert_eq!(
        sql,
        "INSERT INTO guard_rows (id, status) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status WHERE guard_rows.tenant_id = $3 RETURNING id, status"
    );
    assert_eq!(params.len(), 3);
    assert_eq!(params[2].as_deref(), Some(b"tenant.a'quoted".as_slice()));
    let (_, wire_params) = AstEncoder::encode_cmd(&cmd).unwrap();
    assert_eq!(wire_params, params);
}

#[test]
fn conflict_guard_preserves_filter_groups_and_null_predicate() {
    let cmd = guarded(vec![
        eq("guard_rows.tenant_id", "tenant-a"),
        Condition {
            left: Expr::Named("guard_rows.deleted_at".into()),
            op: Operator::IsNull,
            value: Value::Null,
            is_array_unnest: false,
        },
    ])
    .eq("guard_rows.enabled", true)
    .or_filter("guard_rows.status", Operator::Eq, "draft")
    .or_filter("guard_rows.status", Operator::Eq, "pending");
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert!(sql.ends_with("WHERE guard_rows.enabled = $3 AND (guard_rows.status = $4 OR guard_rows.status = $5) AND guard_rows.tenant_id = $6 AND guard_rows.deleted_at IS NULL"), "{sql}");
    assert_eq!(params.len(), 6);
    assert_eq!(params[5].as_deref(), Some(b"tenant-a".as_slice()));
}

#[test]
fn conflict_guard_rejects_invalid_identifiers_and_nested_queries() {
    let invalid = [
        eq("guard_rows.tenant_id; SELECT 1", "tenant-a"),
        eq("guard_rows.tenant_id", Value::Column("bad;column".into())),
        eq(
            "guard_rows.tenant_id",
            Value::Subquery(Box::new(Qail::get("bad;table"))),
        ),
    ];
    for condition in invalid {
        let cmd = guarded(vec![condition]);
        assert!(AstEncoder::encode_cmd_sql(&cmd).is_err());
        assert!(AstEncoder::encode_cmd(&cmd).is_err());
    }
}

#[test]
fn empty_conflict_guard_and_do_nothing_still_encode() {
    let (sql, params) = AstEncoder::encode_cmd_sql(&upsert()).unwrap();
    assert!(!sql.contains(" WHERE "));
    assert_eq!(params.len(), 2);
    let cmd = upsert().on_conflict_nothing(&["id"]);
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert!(sql.ends_with("ON CONFLICT (id) DO NOTHING"));
    assert_eq!(params.len(), 2);
}

#[test]
fn conflict_guard_binds_null_without_shifting_later_parameters() {
    let cmd = guarded(vec![
        eq("guard_rows.marker", Value::Null),
        eq("guard_rows.tenant_id", "tenant-a"),
    ]);
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert!(sql.ends_with("WHERE guard_rows.marker = $3 AND guard_rows.tenant_id = $4"));
    assert_eq!(params.len(), 4);
    assert_eq!(params[2], None);
    assert_eq!(params[3].as_deref(), Some(b"tenant-a".as_slice()));
}

#[test]
fn conflict_guard_from_rls_builder_reaches_native_encoder() {
    use qail_core::rls::{RlsContext, init_scope_registries_from_tables};

    init_scope_registries_from_tables(&[("guard_rows", "tenant_id")], &[]).unwrap();
    let cmd = upsert().with_rls(&RlsContext::tenant("tenant-a")).unwrap();
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert!(sql.ends_with("WHERE guard_rows.tenant_id = $4"), "{sql}");
    assert_eq!(params.len(), 4);
    assert_eq!(params[2], params[3]);
    assert_eq!(params[3].as_deref(), Some(b"tenant-a".as_slice()));
}
