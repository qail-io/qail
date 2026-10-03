//! Offline regressions for write-command gaps: WITH on writes and
//! data-modifying CTE bodies (E3), targetless DO NOTHING preview (E9),
//! native action parity (A7), Unicode identifier atoms (D9), and UPDATE
//! subscript/field assignment targets (D15).

use qail_core::ast::{Condition, Expr, Operator, Qail, Value};
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;

fn native(cmd: &Qail) -> (String, Vec<Option<Vec<u8>>>) {
    AstEncoder::encode_cmd_sql(cmd).expect("native encode")
}

fn native_err(cmd: &Qail) -> String {
    AstEncoder::encode_cmd_sql(cmd)
        .expect_err("native encode must reject")
        .to_string()
}

fn text_params(params: &[Option<Vec<u8>>]) -> Vec<String> {
    params
        .iter()
        .map(|p| String::from_utf8(p.clone().expect("non-null param")).expect("utf8 param"))
        .collect()
}

fn pending_items() -> Qail {
    Qail::get("items").columns(["id"]).eq("status", "pending")
}

// ── E3: WITH on INSERT / UPDATE / DELETE ────────────────────────────

#[test]
fn insert_select_keeps_with_prefix() {
    let mut cmd = Qail::add("orders")
        .columns(["id"])
        .with("chosen", pending_items());
    cmd.source_query = Some(Box::new(Qail::get("chosen").columns(["id"])));

    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "WITH chosen(id) AS (SELECT id FROM items WHERE status = $1) \
         INSERT INTO orders (id) SELECT id FROM chosen"
    );
    assert_eq!(text_params(&params), ["pending"]);

    let preview = cmd.to_sql();
    assert!(
        preview.starts_with("WITH \"chosen\"(\"id\") AS (SELECT")
            || preview.starts_with("WITH chosen(id) AS (SELECT"),
        "{preview}"
    );
    assert!(preview.contains("INSERT INTO"), "{preview}");
}

#[test]
fn update_from_keeps_with_prefix_and_param_order() {
    let cmd = Qail::set("orders")
        .set_value("status", "picked")
        .update_from(["chosen"])
        .filter_cond(Condition {
            left: Expr::Named("orders.id".into()),
            op: Operator::Eq,
            value: Value::Column("chosen.id".into()),
            is_array_unnest: false,
        })
        .with("chosen", pending_items());

    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "WITH chosen(id) AS (SELECT id FROM items WHERE status = $1) \
         UPDATE orders SET status = $2 FROM chosen WHERE orders.id = chosen.id"
    );
    assert_eq!(text_params(&params), ["pending", "picked"]);

    let preview = cmd.to_sql();
    assert!(preview.starts_with("WITH "), "{preview}");
    assert!(preview.contains(" UPDATE "), "{preview}");
}

#[test]
fn delete_using_keeps_with_prefix() {
    let cmd = Qail::del("orders")
        .delete_using(["chosen"])
        .filter_cond(Condition {
            left: Expr::Named("orders.id".into()),
            op: Operator::Eq,
            value: Value::Column("chosen.id".into()),
            is_array_unnest: false,
        })
        .with("chosen", pending_items());

    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "WITH chosen(id) AS (SELECT id FROM items WHERE status = $1) \
         DELETE FROM orders USING chosen WHERE orders.id = chosen.id"
    );
    assert_eq!(text_params(&params), ["pending"]);

    let preview = cmd.to_sql();
    assert!(preview.starts_with("WITH "), "{preview}");
    assert!(preview.contains(" DELETE FROM "), "{preview}");
}

#[test]
fn data_modifying_cte_bodies_encode_as_their_own_statements() {
    let deleted = Qail::get("deleted").with(
        "deleted",
        Qail::del("orders").eq("status", "void").returning(["id"]),
    );
    let (sql, params) = native(&deleted);
    assert_eq!(
        sql,
        "WITH deleted AS (DELETE FROM orders WHERE status = $1 RETURNING id) \
         SELECT * FROM deleted"
    );
    assert_eq!(text_params(&params), ["void"]);
    let preview = deleted.to_sql();
    assert!(
        preview.contains("AS (DELETE FROM orders WHERE status = 'void'"),
        "{preview}"
    );

    let updated = Qail::get("moved").with(
        "moved",
        Qail::set("orders")
            .set_value("status", "moved")
            .eq("status", "pending")
            .returning(["id"]),
    );
    let (sql, params) = native(&updated);
    assert_eq!(
        sql,
        "WITH moved AS (UPDATE orders SET status = $1 WHERE status = $2 RETURNING id) \
         SELECT * FROM moved"
    );
    assert_eq!(text_params(&params), ["moved", "pending"]);
    let preview = updated.to_sql();
    assert!(preview.contains("AS (UPDATE orders SET"), "{preview}");

    let added = Qail::get("made").with(
        "made",
        Qail::add("orders")
            .set_value("status", "new_row")
            .returning(["id"]),
    );
    let (sql, _) = native(&added);
    assert_eq!(
        sql,
        "WITH made AS (INSERT INTO orders (status) VALUES ($1) RETURNING id) \
         SELECT * FROM made"
    );
    let preview = added.to_sql();
    assert!(preview.contains("AS (INSERT INTO"), "{preview}");
}

#[test]
fn data_modifying_cte_stays_rejected_in_subquery_slots() {
    let inner = Qail::get("d").with("d", Qail::del("orders").returning(["id"]));
    let cmd = Qail::get("items").filter("id", Operator::In, Value::Subquery(Box::new(inner)));
    let err = native_err(&cmd);
    assert!(err.contains("read-only"), "{err}");
}

// ── E9: targetless DO NOTHING preview ───────────────────────────────

#[test]
fn targetless_do_nothing_preview_omits_parentheses() {
    let cmd = Qail::add("orders")
        .set_value("id", 1)
        .on_conflict_nothing::<&str>(&[]);
    let preview = cmd.to_sql();
    assert!(preview.contains(" ON CONFLICT DO NOTHING"), "{preview}");
    assert!(!preview.contains("()"), "{preview}");
    let (sql, _) = native(&cmd);
    assert!(sql.ends_with(" ON CONFLICT DO NOTHING"), "{sql}");
}

// ── A7: action parity ───────────────────────────────────────────────

#[test]
fn truncate_lock_explain_encode_natively() {
    assert_eq!(native(&Qail::truncate("orders")).0, "TRUNCATE TABLE orders");
    assert_eq!(
        native(&Qail::lock("orders")).0,
        "LOCK TABLE orders IN ACCESS EXCLUSIVE MODE"
    );
    let (sql, params) = native(&Qail::explain("orders").eq("status", "paid"));
    assert_eq!(sql, "EXPLAIN SELECT * FROM orders WHERE status = $1");
    assert_eq!(text_params(&params), ["paid"]);
    let (sql, _) = native(&Qail::explain_analyze("orders").limit(1));
    assert_eq!(sql, "EXPLAIN ANALYZE SELECT * FROM orders LIMIT 1");
}

#[test]
fn put_is_rejected_with_a_pointer_to_the_guarded_upsert() {
    let cmd = Qail::put("orders").columns(["id"]).set_value("id", 1);
    let err = native_err(&cmd);
    assert!(err.contains("on_conflict_update"), "{err}");
}

// ── D9: Unicode identifier atoms ────────────────────────────────────

#[test]
fn unicode_identifier_atoms_encode_natively() {
    let cmd = Qail::get("café").columns(["naïve"]).eq("größe", 3);
    let (sql, _) = native(&cmd);
    assert_eq!(sql, "SELECT naïve FROM café WHERE größe = $1");
    assert_eq!(cmd.to_sql(), "SELECT naïve FROM café WHERE größe = 3");
}

// ── D15: UPDATE subscript / field assignment targets ────────────────

fn subscript(column: &str, index: i64) -> Expr {
    Expr::Subscript {
        expr: Box::new(Expr::Named(column.into())),
        index: Box::new(Expr::Literal(Value::Int(index))),
        alias: None,
    }
}

fn assign(target: Expr, value: impl Into<Value>) -> Condition {
    Condition {
        left: target,
        op: Operator::Eq,
        value: value.into(),
        is_array_unnest: false,
    }
}

#[test]
fn update_array_element_target() {
    let mut cmd = Qail::set("people").eq("id", 1);
    cmd.cages.push(qail_core::ast::Cage {
        kind: qail_core::ast::CageKind::Payload,
        conditions: vec![assign(subscript("names", 1), "updated")],
        logical_op: qail_core::ast::LogicalOp::And,
    });

    let (sql, params) = native(&cmd);
    assert_eq!(sql, "UPDATE people SET names[1] = $1 WHERE id = $2");
    assert_eq!(text_params(&params), ["updated", "1"]);
    assert_eq!(
        cmd.to_sql(),
        "UPDATE people SET names[1] = 'updated' WHERE id = 1"
    );
}

#[test]
fn update_field_and_chained_targets_via_builder() {
    let field = Expr::FieldAccess {
        expr: Box::new(Expr::Named("address".into())),
        field: "city".into(),
        alias: None,
    };
    let chained = Expr::FieldAccess {
        expr: Box::new(Expr::Subscript {
            expr: Box::new(Expr::Named("stops".into())),
            index: Box::new(Expr::Named("slot".into())),
            alias: None,
        }),
        field: "name".into(),
        alias: None,
    };
    let cmd = Qail::set("people")
        .set_target(field, "Ubud")
        .set_target(chained, "Gili")
        .set_value("status", "moved")
        .eq("id", 1);

    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "UPDATE people SET address.city = $1, stops[slot].name = $2, status = $3 WHERE id = $4"
    );
    assert_eq!(text_params(&params), ["Ubud", "Gili", "moved", "1"]);
    assert_eq!(
        cmd.to_sql(),
        "UPDATE people SET address.city = 'Ubud', stops[slot].name = 'Gili', \
         status = 'moved' WHERE id = 1"
    );
}

#[test]
fn update_targets_outside_the_model_fail_loudly() {
    let function_target = Expr::FunctionCall {
        name: "lower".into(),
        args: vec![Expr::Named("names".into())],
        alias: None,
    };
    let cmd = Qail::set("people").set_target(function_target, "x");
    assert!(native_err(&cmd).contains("must be a column"));
    // The preview's shape check refuses the whole statement before rendering.
    let preview = cmd.to_sql();
    assert!(
        preview.starts_with("/* ERROR: update.payload.column must be a column"),
        "{preview}"
    );

    // A table-qualified base would read as a composite field selection.
    let qualified = Qail::set("people").set_target(subscript("people.names", 1), "x");
    assert!(native_err(&qualified).contains("unsafe identifier"));
    let preview = qualified.to_sql();
    assert!(
        preview.starts_with("/* ERROR: update.payload.column must be a column"),
        "{preview}"
    );

    let param_index = Expr::Subscript {
        expr: Box::new(Expr::Named("names".into())),
        index: Box::new(Expr::Literal(Value::String("1".into()))),
        alias: None,
    };
    let cmd = Qail::set("people").set_target(param_index, "x");
    assert!(native_err(&cmd).contains("must be a column"));
}

#[test]
fn scoped_update_cannot_write_an_element_of_the_tenant_column() {
    qail_core::rls::init_scope_registries_from_tables(&[("_gap_scoped_people", "tenant_id")], &[])
        .expect("register tenant table");
    let err = Qail::set("_gap_scoped_people")
        .set_target(subscript("tenant_id", 1), "other")
        .with_rls(&qail_core::rls::RlsContext::tenant("t-1"))
        .expect_err("element write to the tenant column must be refused");
    assert!(
        matches!(
            err,
            qail_core::error::QailBuildError::RlsTenantColumnMutationDenied { .. }
        ),
        "{err:?}"
    );
}

// ── E3 continued: scope, nesting, and RLS ───────────────────────────

#[test]
fn nested_write_cte_inside_a_write_body_is_rejected() {
    let body = Qail::del("orders")
        .with("inner_d", Qail::del("items").returning(["id"]))
        .returning(["id"]);
    let cmd = Qail::get("d").with("d", body);
    let err = native_err(&cmd);
    assert!(err.contains("top-level statement"), "{err}");
}

#[test]
fn write_cte_is_rejected_where_postgres_or_execution_forbids_it() {
    let del = || Qail::del("orders").returning(["id"]);

    // COUNT, MERGE, EXPLAIN ANALYZE, and CREATE VIEW keep read-only WITH bodies.
    let mut cnt = Qail::get("d").with("d", del());
    cnt.action = qail_core::ast::Action::Cnt;
    assert!(native_err(&cnt).contains("top-level statement"));

    let explain = Qail::explain_analyze("d").with("d", del());
    assert!(native_err(&explain).contains("read-only"));

    let recursive = Qail::get("d").with("d", del()).recursive(Qail::get("d"));
    assert!(native_err(&recursive).contains("recursive arm"));
    assert!(recursive.to_sql().contains("/* ERROR: data-modifying CTE"));
}

#[test]
fn read_cte_on_insert_binds_before_values() {
    let cmd = Qail::add("orders")
        .set_value("note", "first")
        .with("chosen", pending_items());
    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "WITH chosen(id) AS (SELECT id FROM items WHERE status = $1) \
         INSERT INTO orders (note) VALUES ($2)"
    );
    assert_eq!(text_params(&params), ["pending", "first"]);
}

#[test]
fn write_cte_body_on_a_registered_table_is_scoped() {
    qail_core::rls::init_scope_registries_from_tables(&[("_gap_scoped_orders", "tenant_id")], &[])
        .expect("register tenant table");
    let ctx = qail_core::rls::RlsContext::tenant("t-1");

    // The outer relation is the CTE alias (unregistered); only the body
    // touches the registered table.
    let cmd = Qail::get("gone")
        .with(
            "gone",
            Qail::del("_gap_scoped_orders")
                .eq("status", "void")
                .returning(["id"]),
        )
        .with_rls(&ctx)
        .expect("scope nested write body");
    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "WITH gone AS (DELETE FROM _gap_scoped_orders \
         WHERE status = $1 AND _gap_scoped_orders.tenant_id = $2 RETURNING id) \
         SELECT * FROM gone"
    );
    assert_eq!(text_params(&params), ["void", "t-1"]);
    let preview = cmd.to_sql();
    assert!(preview.contains("tenant_id = 't-1'"), "{preview}");

    let made = || {
        Qail::add("_gap_scoped_orders")
            .set_value("status", "new_row")
            .returning(["id"])
    };

    // Copying the scoped body's rows into an unregistered table would drop
    // the scope: INSERT ... SELECT into an unregistered target is refused.
    let mut copy = Qail::add("_gap_unscoped_log")
        .columns(["id"])
        .with("made", made());
    copy.source_query = Some(Box::new(Qail::get("made").columns(["id"])));
    let err = copy
        .with_rls(&ctx)
        .expect_err("unregistered INSERT ... SELECT target");
    assert!(
        matches!(
            err,
            qail_core::error::QailBuildError::RlsInsertSelectUnsupported { .. }
        ),
        "{err:?}"
    );

    // A write CTE on a write statement that copies nothing: the INSERT body
    // is stamped too.
    let outer = Qail::del("_gap_unscoped_log")
        .eq("id", 0)
        .with("made", made())
        .with_rls(&ctx)
        .expect("scope write CTE on delete");
    let (sql, params) = native(&outer);
    assert_eq!(
        sql,
        "WITH made AS (INSERT INTO _gap_scoped_orders (status, tenant_id) VALUES ($1, $2) \
         RETURNING id) DELETE FROM _gap_unscoped_log WHERE id = $3"
    );
    assert_eq!(text_params(&params), ["new_row", "t-1", "0"]);

    // Missing scope still fails closed inside the CTE body.
    let err = Qail::get("gone")
        .with("gone", Qail::del("_gap_scoped_orders").returning(["id"]))
        .with_rls(&qail_core::rls::RlsContext::user("u-1"))
        .expect_err("tenant body under user-only context");
    assert!(
        matches!(
            err,
            qail_core::error::QailBuildError::RlsScopeMissing { .. }
        ),
        "{err:?}"
    );
}

#[test]
fn write_cte_columns_follow_returning_not_target_columns() {
    let cmd = Qail::get("made").with(
        "made",
        Qail::add("orders")
            .columns(["status", "note"])
            .values(["a", "b"])
            .returning(["id"]),
    );
    assert!(cmd.ctes[0].columns.is_empty());
    let (sql, _) = native(&cmd);
    assert!(sql.starts_with("WITH made AS (INSERT"), "{sql}");
}

// ── B6: ON CONFLICT ON CONSTRAINT ───────────────────────────────────

#[test]
fn on_conflict_on_constraint_targets() {
    let nothing = Qail::add("orders")
        .set_value("id", 1)
        .on_conflict_constraint_nothing("orders_pkey");
    assert_eq!(
        native(&nothing).0,
        "INSERT INTO orders (id) VALUES ($1) ON CONFLICT ON CONSTRAINT orders_pkey DO NOTHING"
    );
    assert!(
        nothing
            .to_sql()
            .contains(" ON CONFLICT ON CONSTRAINT orders_pkey DO NOTHING"),
        "{}",
        nothing.to_sql()
    );

    let update = Qail::add("orders")
        .set_value("id", 1)
        .set_value("status", "paid")
        .on_conflict_constraint_update(
            "orders_pkey",
            &[("status", Expr::Named("excluded.status".into()))],
        );
    assert_eq!(
        native(&update).0,
        "INSERT INTO orders (id, status) VALUES ($1, $2) \
         ON CONFLICT ON CONSTRAINT orders_pkey DO UPDATE SET status = excluded.status"
    );
    assert!(
        update.to_sql().contains(
            " ON CONFLICT ON CONSTRAINT orders_pkey DO UPDATE SET status = excluded.status"
        )
    );
}

#[test]
fn on_conflict_target_shape_errors_are_loud() {
    let mut both = Qail::add("orders")
        .set_value("id", 1)
        .on_conflict_nothing(&["id"]);
    both.on_conflict.as_mut().unwrap().constraint = Some("orders_pkey".into());
    assert!(native_err(&both).contains("cannot combine"));
    assert!(both.to_sql().contains("/* ERROR: conflict target has both"));

    let dotted = Qail::add("orders")
        .set_value("id", 1)
        .on_conflict_constraint_nothing("public.orders_pkey");
    assert!(native_err(&dotted).contains("unsafe identifier"));
    assert!(
        dotted
            .to_sql()
            .contains("/* ERROR: Invalid conflict constraint")
    );

    let targetless_update = Qail::add("orders")
        .set_value("id", 1)
        .on_conflict_update::<&str>(&[], &[("id", Expr::Named("excluded.id".into()))]);
    assert!(native_err(&targetless_update).contains("requires at least one conflict target"));
}

// ── A7 continued ────────────────────────────────────────────────────

#[test]
fn whole_table_actions_reject_clauses_they_would_drop() {
    let filtered = Qail::truncate("orders").eq("tenant_id", "t-1");
    assert!(native_err(&filtered).contains("takes only a table"));
    assert_eq!(
        filtered.to_sql(),
        "/* ERROR: TRUNCATE takes only a table */"
    );

    let filtered = Qail::lock("orders").eq("id", 1);
    assert!(native_err(&filtered).contains("takes only a table"));
    assert_eq!(
        filtered.to_sql(),
        "/* ERROR: LOCK TABLE takes only a table */"
    );
}

#[test]
fn whole_table_actions_refuse_rls_scoping_and_explain_is_scoped() {
    qail_core::rls::init_scope_registries_from_tables(&[("_gap_scoped_ledger", "tenant_id")], &[])
        .expect("register tenant table");
    let ctx = qail_core::rls::RlsContext::tenant("t-1");
    for cmd in [
        Qail::truncate("_gap_scoped_ledger"),
        Qail::lock("_gap_scoped_ledger"),
    ] {
        let err = cmd
            .with_rls(&ctx)
            .expect_err("whole-table action on scoped table");
        assert!(
            matches!(
                err,
                qail_core::error::QailBuildError::RlsWholeTableActionDenied { .. }
            ),
            "{err:?}"
        );
    }

    let explain = Qail::explain_analyze("_gap_scoped_ledger")
        .with_rls(&ctx)
        .expect("explain is read-scoped");
    let (sql, params) = native(&explain);
    assert_eq!(
        sql,
        "EXPLAIN ANALYZE SELECT * FROM _gap_scoped_ledger \
         WHERE _gap_scoped_ledger.tenant_id = $1"
    );
    assert_eq!(text_params(&params), ["t-1"]);
}

// ── D9 continued ────────────────────────────────────────────────────

#[test]
fn identifier_atoms_still_reject_quotes_spaces_and_punctuation() {
    for table in ["\"orders\"", "or-ders", "orders;", "ord\u{301}ers"] {
        let err = native_err(&Qail::get(table));
        assert!(err.contains("unsafe identifier"), "{table}: {err}");
    }
}
