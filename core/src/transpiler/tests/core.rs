//! Core SQL transpiler tests (SELECT, UPDATE, DELETE, INSERT).

use crate::parser::parse;
use crate::transpiler::ToSql;

#[test]
fn test_simple_select() {
    let cmd = parse("get users").unwrap();
    assert_eq!(cmd.to_sql(), "SELECT * FROM users");
}

#[test]
fn test_select_columns() {
    let cmd = parse("get users fields id, email, role").unwrap();
    assert_eq!(cmd.to_sql(), "SELECT id, email, role FROM users");
}

#[test]
fn test_select_with_where() {
    let cmd = parse("get users fields * where active = true").unwrap();
    assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE active = true");
}

#[test]
fn test_in_literal_list_uses_sql_in() {
    let cmd = parse("get users fields * where name in (\"O'Reilly\", \"Ada\")").unwrap();
    assert_eq!(
        cmd.to_sql(),
        "SELECT * FROM users WHERE name IN ('O''Reilly', 'Ada')"
    );
}

#[test]
fn test_not_in_literal_list_uses_sql_not_in() {
    let cmd = parse("get users fields * where role not in (\"guest\", \"banned\")").unwrap();
    assert_eq!(
        cmd.to_sql(),
        "SELECT * FROM users WHERE role NOT IN ('guest', 'banned')"
    );
}

#[test]
fn test_in_param_keeps_any_array_binding() {
    use crate::ast::*;

    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("id".to_string()),
            op: Operator::In,
            value: Value::Param(1),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE id = ANY($1)");
}

#[test]
fn test_select_with_limit() {
    let cmd = parse("get users fields * limit 10").unwrap();
    assert_eq!(cmd.to_sql(), "SELECT * FROM users LIMIT 10");
}

#[test]
fn test_builder_negative_limit_offset_do_not_wrap() {
    use crate::ast::Qail;

    let cmd = Qail::get("users").limit(-1).offset(-5);
    assert_eq!(cmd.to_sql(), "SELECT * FROM users LIMIT 0 OFFSET 0");
}

#[test]
fn test_select_with_order() {
    let cmd = parse("get users fields * order by created_at desc").unwrap();
    assert_eq!(cmd.to_sql(), "SELECT * FROM users ORDER BY created_at DESC");
}

#[test]
fn test_session_set_escapes_values() {
    let cmd = parse(
        "session set app.current_tenant_id = t1'; SET app.is_super_admin = 'true'; SELECT 'ok",
    )
    .unwrap();

    assert_eq!(
        cmd.to_sql(),
        "SET app.current_tenant_id = 't1''; SET app.is_super_admin = ''true''; SELECT ''ok'"
    );
}

#[test]
fn test_session_builder_escapes_malformed_setting_names() {
    use crate::ast::Qail;

    let cmd = Qail::session_set("statement_timeout; RESET ALL", "5000");
    assert_eq!(
        cmd.to_sql(),
        "SET \"statement_timeout; RESET ALL\" = '5000'"
    );
}

#[test]
fn test_select_complex() {
    let cmd =
        parse("get users fields id, email where active = true order by created_at desc limit 10")
            .unwrap();
    assert_eq!(
        cmd.to_sql(),
        "SELECT id, email FROM users WHERE active = true ORDER BY created_at DESC LIMIT 10"
    );
}

#[test]
fn test_update() {
    let cmd = parse("set users values verified = true where id = $1").unwrap();
    assert_eq!(
        cmd.to_sql(),
        "UPDATE users SET verified = true WHERE id = $1"
    );
}

#[test]
fn test_update_returning_all() {
    use crate::ast::Qail;

    let cmd = Qail::set("users")
        .set_value("verified", true)
        .eq("id", 1)
        .returning_all();
    assert_eq!(
        cmd.to_sql(),
        "UPDATE users SET verified = true WHERE id = 1 RETURNING *"
    );
}

#[test]
fn test_update_with_where_or() {
    let cmd = parse("set users values verified = true where id = $1 or email = :email").unwrap();
    assert_eq!(
        cmd.to_sql(),
        "UPDATE users SET verified = true WHERE (id = $1 OR email = :email)"
    );
}

#[test]
fn test_delete() {
    let cmd = parse("del users where id = $1").unwrap();
    assert_eq!(cmd.to_sql(), "DELETE FROM users WHERE id = $1");
}

#[test]
fn test_delete_with_where_or() {
    let cmd = parse("del users where id = $1 or email = :email").unwrap();
    assert_eq!(
        cmd.to_sql(),
        "DELETE FROM users WHERE (id = $1 OR email = :email)"
    );
}

#[test]
fn test_fuzzy_match() {
    let cmd = parse("get users fields * where name ~ $1").unwrap();
    assert_eq!(
        cmd.to_sql(),
        "SELECT * FROM users WHERE name ILIKE '%' || $1 || '%'"
    );
}

#[test]
fn test_parameterized_fuzzy_match_wraps_placeholder() {
    use crate::transpiler::ToSqlParameterized;

    let cmd = parse("get users fields * where name ~ :term").unwrap();
    let result = cmd.to_sql_parameterized();

    assert_eq!(
        result.sql,
        "SELECT * FROM users WHERE name ILIKE '%' || $1 || '%'"
    );
    assert_eq!(result.named_params, vec!["term"]);
}

#[test]
fn test_text_search_multiple_columns_to_sql() {
    use crate::ast::{Operator, Qail};

    let cmd = Qail::get("products").filter("name,description", Operator::TextSearch, "fast ferry");

    assert_eq!(
        cmd.to_sql(),
        "SELECT * FROM products WHERE to_tsvector('english', coalesce(name, '') || ' ' || coalesce(description, '')) @@ websearch_to_tsquery('english', 'fast ferry')"
    );
}

#[test]
fn test_timestamp_literal_escapes_quotes() {
    use crate::ast::{Operator, Qail, Value};

    let cmd = Qail::get("events").filter(
        "created_at",
        Operator::Eq,
        Value::Timestamp("2026-01-01'; DROP TABLE events; --".to_string()),
    );

    assert_eq!(
        cmd.to_sql(),
        "SELECT * FROM events WHERE created_at = '2026-01-01''; DROP TABLE events; --'"
    );
}

#[test]
fn test_string_literal_preserves_nul_for_downstream_rejection() {
    use crate::ast::{Operator, Qail};

    let cmd = Qail::get("users").filter("name", Operator::Eq, "Ana\0 O'Reilly");
    let sql = cmd.to_sql();

    assert!(sql.contains('\0'));
    assert_eq!(sql, "SELECT * FROM users WHERE name = 'Ana\0 O''Reilly'");
}

#[test]
fn test_identifier_preserves_nul_for_downstream_rejection() {
    use crate::ast::Qail;

    let sql = Qail::get("users\0_archive").to_sql();

    assert!(sql.contains('\0'));
    assert_eq!(sql, "SELECT * FROM \"users\0_archive\"");
}

#[test]
fn test_fuzzy_fallback_escapes_rendered_value() {
    use crate::ast::{Operator, Qail, Value};

    let cmd = Qail::get("users").filter(
        "name",
        Operator::Fuzzy,
        Value::Function("x'; DROP TABLE users; --".to_string()),
    );

    assert_eq!(
        cmd.to_sql(),
        "SELECT * FROM users WHERE name ILIKE '%x''; DROP TABLE users; --%'"
    );
}

#[test]
fn test_where_accepts_safe_raw_function_condition_value() {
    use crate::ast::{Operator, Qail, Value};

    let cmd = Qail::get("users").filter(
        "updated_at",
        Operator::Lt,
        Value::Function("NOW()".to_string()),
    );

    assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE updated_at < NOW()");
}

#[test]
fn test_where_rejects_unsafe_raw_function_condition_value() {
    use crate::ast::{Operator, Qail, Value};

    let cmd = Qail::get("users").filter(
        "updated_at",
        Operator::Lt,
        Value::Function("NOW(); DROP TABLE users; --".to_string()),
    );
    let sql = cmd.to_sql();

    assert!(
        sql.contains("updated_at < /* ERROR: Invalid function expression */"),
        "unsafe raw function value should fail closed: {sql}"
    );
    assert!(
        !sql.contains("DROP TABLE"),
        "unsafe raw function value leaked into WHERE SQL: {sql}"
    );
}

// OR conditions - using manual Qail construction
#[test]
fn test_or_conditions() {
    use crate::ast::*;
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![
            Condition {
                left: Expr::Named("status".to_string()),
                op: Operator::Eq,
                value: Value::String("active".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("status".to_string()),
                op: Operator::Eq,
                value: Value::String("pending".to_string()),
                is_array_unnest: false,
            },
        ],
        logical_op: LogicalOp::Or,
    });
    let sql = cmd.to_sql();
    assert!(sql.contains("status = 'active' OR status = 'pending'"));
}

#[test]
fn test_chained_or_filter_builder_conditions_remain_or() {
    use crate::ast::{Operator, Qail};

    let sql = Qail::get("kb")
        .or_filter("topic", Operator::ILike, "%test%")
        .or_filter("question", Operator::ILike, "%test%")
        .to_sql();

    assert!(
        sql.contains("(topic ILIKE '%test%' OR question ILIKE '%test%')"),
        "Expected grouped OR conditions from chained or_filter(), got: {sql}"
    );
    assert!(
        !sql.contains("topic ILIKE '%test%' AND question ILIKE '%test%'"),
        "chained or_filter() must not degrade into AND chain: {sql}"
    );
}

#[test]
fn test_or_filter_and_filter_do_not_mix_cages() {
    use crate::ast::{Operator, Qail};

    let sql = Qail::get("kb")
        .or_filter("topic", Operator::ILike, "%test%")
        .filter("is_active", Operator::Eq, true)
        .or_filter("question", Operator::ILike, "%test%")
        .to_sql();

    assert!(
        sql.contains("topic ILIKE '%test%' OR question ILIKE '%test%'"),
        "OR filters should stay grouped together: {sql}"
    );
    assert!(
        sql.contains("is_active = true"),
        "AND filter should remain a separate clause: {sql}"
    );
    assert!(
        sql.contains(" AND "),
        "Expected OR group to combine with AND filter via AND: {sql}"
    );
}

#[test]
fn test_update_with_or_filter_grouping() {
    use crate::ast::{Operator, Qail};

    let sql = Qail::set("kb")
        .set_value("archived", true)
        .or_filter("topic", Operator::ILike, "%test%")
        .or_filter("question", Operator::ILike, "%test%")
        .to_sql();

    assert!(
        sql.contains("WHERE (topic ILIKE '%test%' OR question ILIKE '%test%')"),
        "Expected grouped OR in UPDATE WHERE: {sql}"
    );
    assert!(
        !sql.contains("topic ILIKE '%test%' AND question ILIKE '%test%'"),
        "UPDATE OR filters must not degrade into AND chain: {sql}"
    );
}

#[test]
fn test_delete_with_or_filter_grouping() {
    use crate::ast::{Operator, Qail};

    let sql = Qail::del("kb")
        .or_filter("topic", Operator::ILike, "%test%")
        .or_filter("question", Operator::ILike, "%test%")
        .to_sql();

    assert!(
        sql.contains("WHERE (topic ILIKE '%test%' OR question ILIKE '%test%')"),
        "Expected grouped OR in DELETE WHERE: {sql}"
    );
    assert!(
        !sql.contains("topic ILIKE '%test%' AND question ILIKE '%test%'"),
        "DELETE OR filters must not degrade into AND chain: {sql}"
    );
}

#[test]
fn test_window_with_or_filter_grouping() {
    use crate::ast::{Action, Expr, Operator, Qail};

    let sql = Qail {
        action: Action::Over,
        table: "kb".to_string(),
        columns: vec![Expr::Named("id".to_string())],
        ..Default::default()
    }
    .or_filter("topic", Operator::ILike, "%test%")
    .or_filter("question", Operator::ILike, "%test%")
    .to_sql();

    assert!(
        sql.contains("WHERE (topic ILIKE '%test%' OR question ILIKE '%test%')"),
        "Expected grouped OR in WINDOW WHERE: {sql}"
    );
}

#[test]
fn test_window_target_alias_renders_as_table_reference() {
    use crate::ast::{Action, Expr, Operator, Qail};

    let sql = Qail {
        action: Action::Over,
        table: "events e".to_string(),
        columns: vec![Expr::Named("e.id".to_string())],
        ..Default::default()
    }
    .filter("e.kind", Operator::Eq, "click")
    .to_sql();

    assert_eq!(sql, "SELECT e.id FROM events e WHERE e.kind = 'click'");
}

#[test]
fn test_array_unnest() {
    use crate::ast::*;
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("tags".to_string()),
            op: Operator::Eq,
            value: Value::Param(1),
            is_array_unnest: true,
        }],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql();
    assert!(sql.contains("EXISTS (SELECT 1 FROM unnest(tags)"));
}

#[test]
fn test_array_elem_contained_in_text() {
    use crate::ast::*;
    let cmd = Qail::get("ai_knowledge_base").array_elem_contained_in_text("keywords", "Komodo");
    let sql = cmd.to_sql();
    assert!(sql.contains("EXISTS (SELECT 1 FROM unnest(keywords) _el"));
    assert!(sql.contains("LOWER('Komodo') LIKE '%' || LOWER(_el) || '%'"));
}

#[test]
fn test_array_elem_contained_in_text_parameterized() {
    use crate::ast::{Qail, Value};
    use crate::transpiler::ToSqlParameterized;

    let cmd = Qail::get("ai_knowledge_base")
        .array_elem_contained_in_text("keywords", Value::NamedParam("query".to_string()));
    let result = cmd.to_sql_parameterized();
    assert!(
        result
            .sql
            .contains("LOWER($1) LIKE '%' || LOWER(_el) || '%'"),
        "sql={}",
        result.sql
    );
    assert_eq!(result.named_params, vec!["query"]);
}

#[test]
fn test_json_exists_parameterized_path_is_not_quoted() {
    use crate::ast::*;
    use crate::transpiler::ToSqlParameterized;

    let mut cmd = Qail::get("events");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("payload".to_string()),
            op: Operator::JsonExists,
            value: Value::NamedParam("json_path".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    let result = cmd.to_sql_parameterized();
    assert_eq!(
        result.sql,
        "SELECT * FROM events WHERE JSON_EXISTS(payload, $1)"
    );
    assert_eq!(result.named_params, vec!["json_path"]);
}

#[test]
fn test_left_join() {
    use crate::ast::*;
    let mut cmd = Qail::get("users");
    cmd.joins.push(Join {
        table: "posts".to_string(),
        kind: JoinKind::Left,
        on: None,
        on_true: false,
    });
    let sql = cmd.to_sql();
    assert!(sql.contains("LEFT JOIN"));
    assert!(sql.contains("posts"));
}

#[test]
fn test_right_join() {
    use crate::ast::*;
    let mut cmd = Qail::get("users");
    cmd.joins.push(Join {
        table: "posts".to_string(),
        kind: JoinKind::Right,
        on: None,
        on_true: false,
    });
    let sql = cmd.to_sql();
    assert!(sql.contains("RIGHT JOIN"));
}

#[test]
fn test_distinct() {
    use crate::ast::*;
    let mut cmd = Qail::get("users");
    cmd.distinct = true;
    cmd.columns.push(Expr::Named("role".to_string()));
    let sql = cmd.to_sql();
    assert!(sql.contains("SELECT DISTINCT"));
    assert!(sql.contains("role"));
}

#[test]
fn test_transactions() {
    use crate::ast::{Action, Qail};
    let mut cmd = Qail::get("users");
    cmd.action = Action::TxnStart;
    assert!(cmd.to_sql().contains("BEGIN"));

    cmd.action = Action::TxnCommit;
    assert!(cmd.to_sql().contains("COMMIT"));

    cmd.action = Action::TxnRollback;
    assert!(cmd.to_sql().contains("ROLLBACK"));
}

#[test]
fn test_parameterized_sql() {
    use crate::transpiler::ToSqlParameterized;

    // Test with named parameters (current implementation supports this)
    let cmd = parse("get users fields * where name = :name and age = :age").unwrap();
    let result = cmd.to_sql_parameterized();

    // SQL should have positional placeholders, not named params
    assert!(
        result.sql.contains("$1"),
        "SQL should have $1 placeholder: {}",
        result.sql
    );
    assert!(
        result.sql.contains("$2"),
        "SQL should have $2 placeholder: {}",
        result.sql
    );
    assert!(
        !result.sql.contains(":name"),
        "SQL should NOT contain ':name': {}",
        result.sql
    );
    assert!(
        !result.sql.contains(":age"),
        "SQL should NOT contain ':age': {}",
        result.sql
    );

    // Named params should be extracted in order
    assert_eq!(result.named_params.len(), 2);
    assert_eq!(result.named_params[0], "name");
    assert_eq!(result.named_params[1], "age");
}

#[test]
fn test_parameterized_sql_ignores_param_markers_inside_literals() {
    use crate::transpiler::ToSqlParameterized;

    let cmd =
        parse("get messages fields * where body = \":not_a_param\" and owner = :owner").unwrap();

    let result = cmd.to_sql_parameterized();

    assert!(
        result.sql.contains("':not_a_param'"),
        "SQL literal should remain intact: {}",
        result.sql
    );
    assert!(
        result.sql.contains("$1"),
        "named param outside the literal should still be replaced: {}",
        result.sql
    );
    assert_eq!(result.named_params, vec!["owner"]);
}
