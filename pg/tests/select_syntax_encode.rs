//! SELECT syntax emission: preview transpiler and native encoder side by side.
//!
//! Covers qualified wildcards, subscript grouping, DSL expression parity,
//! row-lock options, set-operation ALL forms, CTE materialization and
//! SEARCH/CYCLE, window frame modes/exclusion/offsets, and array slices.

use qail_core::ast::{Expr, Qail, Value};
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;

fn native(cmd: &Qail) -> String {
    AstEncoder::encode_cmd_sql(cmd)
        .map(|(sql, _)| sql)
        .unwrap_or_else(|err| panic!("native encode failed: {err}"))
}

fn native_with_params(cmd: &Qail) -> (String, Vec<Option<Vec<u8>>>) {
    AstEncoder::encode_cmd_sql(cmd).unwrap_or_else(|err| panic!("native encode failed: {err}"))
}

fn parsed(input: &str) -> Qail {
    qail_core::parse(input).unwrap_or_else(|err| panic!("parse failed for `{input}`: {err}"))
}

fn both(cmd: &Qail) -> (String, String) {
    (cmd.to_sql(), native(cmd))
}

fn assert_both(cmd: &Qail, expected: &str) {
    let (preview, wire) = both(cmd);
    assert_eq!(preview, expected, "transpiler");
    assert_eq!(wire, expected, "native encoder");
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall {
        name: name.to_string(),
        args,
        alias: None,
    }
}

fn int(n: i64) -> Expr {
    Expr::Literal(Value::Int(n))
}

// ---------------------------------------------------------------- F3

#[test]
fn qualified_wildcard_projects_all_columns_of_the_relation() {
    let cmd = Qail::get("orders").columns(["orders.*"]);
    assert_both(&cmd, "SELECT orders.* FROM orders");
}

#[test]
fn qualified_wildcard_through_a_table_alias() {
    let cmd = Qail::get("orders o").columns(["o.*", "o.id"]);
    assert_both(&cmd, "SELECT o.*, o.id FROM orders o");
}

#[test]
fn schema_qualified_wildcard() {
    let cmd = Qail::get("public.orders").columns(["public.orders.*"]);
    assert_both(&cmd, "SELECT public.orders.* FROM public.orders");
}

#[test]
fn qualified_wildcard_in_returning() {
    let cmd = Qail::set("orders")
        .set_value("status", "paid")
        .eq("id", 7)
        .returning(["orders.*"]);
    let (preview, wire) = both(&cmd);
    assert!(preview.ends_with(" RETURNING orders.*"), "{preview}");
    assert!(wire.ends_with(" RETURNING orders.*"), "{wire}");
}

#[test]
fn qualified_wildcard_as_row_argument() {
    let cmd = Qail::get("orders").column_expr(Expr::FunctionCall {
        name: "row_to_json".to_string(),
        args: vec![Expr::Named("orders.*".to_string())],
        alias: Some("doc".to_string()),
    });
    assert_both(&cmd, "SELECT ROW_TO_JSON(orders.*) AS doc FROM orders");
}

#[test]
fn bare_star_alias_stays_quoted() {
    // `*` is only a wildcard after a qualifier; as a lone name it stays an identifier.
    assert_eq!(qail_core::transpiler::escape_identifier("*"), "\"*\"");
    assert_eq!(qail_core::transpiler::escape_identifier("t.*"), "t.*");
}

// ---------------------------------------------------------------- D5

fn appended_first() -> Expr {
    Expr::Subscript {
        expr: Box::new(call(
            "array_append",
            vec![
                Expr::ArrayConstructor {
                    elements: vec![int(1)],
                    alias: None,
                },
                int(2),
            ],
        )),
        index: Box::new(int(1)),
        alias: Some("first".to_string()),
    }
}

#[test]
fn function_result_subscript_is_parenthesized() {
    let cmd = Qail::get("orders").column_expr(appended_first());
    assert_both(
        &cmd,
        "SELECT (ARRAY_APPEND(ARRAY[1], 2))[1] AS first FROM orders",
    );
}

#[test]
fn column_subscript_stays_bare() {
    let cmd = Qail::get("orders").column_expr(Expr::Subscript {
        expr: Box::new(Expr::Named("names".to_string())),
        index: Box::new(int(1)),
        alias: None,
    });
    assert_both(&cmd, "SELECT names[1] FROM orders");
}

#[test]
fn chained_subscript_of_function_result() {
    let inner = Expr::Subscript {
        expr: Box::new(call("matrix_of", vec![Expr::Named("id".to_string())])),
        index: Box::new(int(1)),
        alias: None,
    };
    let cmd = Qail::get("orders").column_expr(Expr::Subscript {
        expr: Box::new(inner),
        index: Box::new(int(2)),
        alias: None,
    });
    assert_both(&cmd, "SELECT (MATRIX_OF(id))[1][2] FROM orders");
}

#[test]
fn function_result_subscript_in_where() {
    let left = Expr::Subscript {
        expr: Box::new(call(
            "string_to_array",
            vec![
                Expr::Named("tags".to_string()),
                Expr::Literal(Value::String(",".to_string())),
            ],
        )),
        index: Box::new(int(1)),
        alias: None,
    };
    let cmd = Qail::get("orders").filter_cond(qail_core::ast::Condition {
        left,
        op: qail_core::ast::Operator::Eq,
        value: Value::String("vip".to_string()),
        is_array_unnest: false,
    });
    let (preview, wire) = both(&cmd);
    assert!(
        preview.contains("WHERE (STRING_TO_ARRAY(tags, ','))[1] = "),
        "{preview}"
    );
    assert_eq!(
        wire,
        "SELECT * FROM orders WHERE (STRING_TO_ARRAY(tags, ','))[1] = $1"
    );
}

// ---------------------------------------------------------------- D8

#[test]
fn dsl_keeps_alias_on_arithmetic_projection() {
    assert_both(
        &parsed("get orders fields (a + b) as total"),
        "SELECT (a + b) AS total FROM orders",
    );
    assert_both(
        &parsed("get orders fields amount + 1 as n"),
        "SELECT (amount + 1) AS n FROM orders",
    );
}

#[test]
fn dsl_rejects_alias_it_cannot_keep() {
    assert!(qail_core::parse("get orders fields 'lit' as e").is_err());
}

#[test]
fn dsl_rejects_json_path_on_non_column() {
    assert!(qail_core::parse("get orders fields coalesce(a, b)->'k'").is_err());
}

#[test]
fn dsl_casts_with_typmods_and_multiword_types() {
    assert_both(
        &parsed("get orders fields amount::numeric(12,2) as amt"),
        "SELECT amount::numeric(12,2) AS amt FROM orders",
    );
    assert_both(
        &parsed("get orders fields amount::double precision"),
        "SELECT amount::double precision FROM orders",
    );
    assert_both(
        &parsed("get orders fields created_at::timestamp with time zone as ts"),
        "SELECT created_at::timestamp with time zone AS ts FROM orders",
    );
    assert_both(
        &parsed("get orders fields tags::text[]"),
        "SELECT tags::text[] FROM orders",
    );
}

#[test]
fn dsl_projected_boolean_comparison_and_not() {
    assert_both(
        &parsed("get orders fields (amount > 10) as big"),
        "SELECT (amount > 10) AS big FROM orders",
    );
    // Boolean literals: the preview prints `true`, the native encoder `TRUE`.
    let cmd = parsed("get orders fields (amount > 10 and paid = true) as ok");
    assert_eq!(
        cmd.to_sql(),
        "SELECT ((amount > 10) AND (paid = true)) AS ok FROM orders"
    );
    assert_eq!(
        native(&cmd),
        "SELECT ((amount > 10) AND (paid = TRUE)) AS ok FROM orders"
    );
    let cmd = parsed("get orders fields not active as inactive");
    assert_eq!(
        cmd.to_sql(),
        "SELECT (active = false) AS inactive FROM orders"
    );
    assert_eq!(
        native(&cmd),
        "SELECT (active = FALSE) AS inactive FROM orders"
    );
    assert!(qail_core::parse("get orders fields (a > 1) as x as y").is_err());
}

#[test]
fn dsl_simple_case() {
    assert_both(
        &parsed(
            "get orders fields case status when 'paid' then 1 when 'void' then 2 else 0 end as code",
        ),
        "SELECT CASE WHEN status = 'paid' THEN 1 WHEN status = 'void' THEN 2 ELSE 0 END AS code FROM orders",
    );
}

#[test]
fn dsl_merge_returning() {
    let cmd = parsed(
        "merge users as u using staging_users as s on u.id = s.id \
         when matched then delete returning u.id",
    );
    assert_eq!(cmd.returning, Some(vec![Expr::Named("u.id".to_string())]));
    assert!(
        cmd.to_sql().ends_with(" RETURNING u.id"),
        "{}",
        cmd.to_sql()
    );
    assert!(
        native(&cmd).ends_with(" RETURNING u.id"),
        "{}",
        native(&cmd)
    );
}

#[test]
fn dsl_qualified_wildcard() {
    assert_both(
        &parsed("get orders fields orders.*, orders.id"),
        "SELECT orders.*, orders.id FROM orders",
    );
}

#[test]
fn dsl_subscripts() {
    assert_both(
        &parsed("get orders fields arr[1] as first"),
        "SELECT arr[1] AS first FROM orders",
    );
    assert_both(
        &parsed("get orders fields array_append(arr, 2)[1]"),
        "SELECT (ARRAY_APPEND(arr, 2))[1] FROM orders",
    );
}

// ---------------------------------------------------------------- B7

#[test]
fn dsl_row_locks() {
    assert_both(
        &parsed("get jobs for update"),
        "SELECT * FROM jobs FOR UPDATE",
    );
    let cmd = parsed("get jobs where state = 'queued' limit 5 for update skip locked");
    assert_eq!(
        cmd.to_sql(),
        "SELECT * FROM jobs WHERE state = 'queued' LIMIT 5 FOR UPDATE SKIP LOCKED"
    );
    assert_eq!(
        native(&cmd),
        "SELECT * FROM jobs WHERE state = $1 LIMIT 5 FOR UPDATE SKIP LOCKED"
    );
    assert_both(
        &parsed("get jobs for no key update nowait"),
        "SELECT * FROM jobs FOR NO KEY UPDATE NOWAIT",
    );
    assert_both(
        &parsed("get jobs for key share of jobs"),
        "SELECT * FROM jobs FOR KEY SHARE OF jobs",
    );
    assert!(qail_core::parse("get jobs for update nowait skip locked").is_err());
}

// ---------------------------------------------------------------- B8

#[test]
fn dsl_cte_materialization() {
    let cmd = parsed(
        "with recent as materialized (get orders fields id where paid = true) get recent fields id",
    );
    let (preview, wire) = both(&cmd);
    assert!(
        preview.starts_with("WITH recent AS MATERIALIZED (SELECT id FROM orders"),
        "{preview}"
    );
    assert!(
        wire.starts_with("WITH recent AS MATERIALIZED (SELECT id FROM orders"),
        "{wire}"
    );

    let cmd = parsed("with recent as not materialized (get orders fields id) get recent fields id");
    assert!(
        cmd.to_sql().contains("AS NOT MATERIALIZED ("),
        "{}",
        cmd.to_sql()
    );
    assert!(
        native(&cmd).contains("AS NOT MATERIALIZED ("),
        "{}",
        native(&cmd)
    );
}

// ---------------------------------------------------------------- B5

#[test]
fn dsl_groups_frame_with_exclusion() {
    assert_both(
        &parsed(
            "get orders fields sum(amount) over (order by day groups between 1 preceding and current row exclude ties) as s",
        ),
        "SELECT SUM(amount) OVER (ORDER BY day ASC GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE TIES) AS s FROM orders",
    );
}

// ---------------------------------------------------------------- B11

#[test]
fn dsl_array_slices() {
    assert_both(
        &parsed("get orders fields arr[1:3] as head"),
        "SELECT arr[1:3] AS head FROM orders",
    );
    assert_both(
        &parsed("get orders fields arr[2:], arr[:2]"),
        "SELECT arr[2:], arr[:2] FROM orders",
    );
}

#[test]
fn params_number_left_to_right_around_new_syntax() {
    let cmd = Qail::get("orders")
        .column_expr(appended_first())
        .eq("status", "paid")
        .eq("region", "eu");
    let (sql, params) = native_with_params(&cmd);
    assert_eq!(
        sql,
        "SELECT (ARRAY_APPEND(ARRAY[1], 2))[1] AS first FROM orders WHERE status = $1 AND region = $2"
    );
    assert_eq!(params[0].as_deref(), Some(b"paid".as_slice()));
    assert_eq!(params[1].as_deref(), Some(b"eu".as_slice()));
}
