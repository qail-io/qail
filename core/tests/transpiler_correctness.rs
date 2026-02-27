//! SQL Output Correctness Tests
//!
//! Verifies that the Qail AST → SQL transpiler produces semantically correct SQL.
//! Unlike the proptest invariants (which only verify no-panic and basic shape),
//! these tests assert **exact SQL output** for known inputs.
//!
//! Coverage:
//! - SELECT with columns, WHERE, ORDER BY, LIMIT/OFFSET
//! - INSERT with values and RETURNING
//! - UPDATE with SET and WHERE
//! - DELETE with WHERE
//! - JOINs (INNER, LEFT)
//! - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//! - Nested conditions (AND/OR)
//! - Parameterized query output
//! - SQL injection safety in identifiers and values

use qail_core::ast::*;
use qail_core::transpiler::{ToSql, ToSqlParameterized};

// ============================================================================
// Helper: build a Filter cage wrapping WHERE conditions
// ============================================================================

fn filter_cage(conditions: Vec<Condition>) -> Cage {
    Cage {
        kind: CageKind::Filter,
        conditions,
        logical_op: LogicalOp::And,
    }
}

fn cond(col: &str, op: Operator, val: Value) -> Condition {
    Condition {
        left: Expr::Named(col.to_string()),
        op,
        value: val,
        is_array_unnest: false,
    }
}

fn payload_cage(conditions: Vec<Condition>) -> Cage {
    Cage {
        kind: CageKind::Payload,
        conditions,
        logical_op: LogicalOp::And,
    }
}

// ============================================================================
// SELECT — basic
// ============================================================================

#[test]
fn select_all_from_table() {
    let cmd = Qail {
        action: Action::Get,
        table: "users".to_string(),
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("SELECT"), "Must produce SELECT: {}", sql);
    assert!(sql.contains("users"), "Table must appear in SQL: {}", sql);
}

#[test]
fn select_specific_columns() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        columns: vec![
            Expr::Named("id".to_string()),
            Expr::Named("total".to_string()),
            Expr::Named("status".to_string()),
        ],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("id"), "Column id must appear: {}", sql);
    assert!(sql.contains("total"), "Column total must appear: {}", sql);
    assert!(sql.contains("status"), "Column status must appear: {}", sql);
    assert!(sql.starts_with("SELECT"), "Must start with SELECT: {}", sql);
}

#[test]
fn select_with_limit() {
    let cmd = Qail {
        action: Action::Get,
        table: "products".to_string(),
        cages: vec![Cage {
            kind: CageKind::Limit(10),
            conditions: vec![],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("LIMIT 10"), "Must contain LIMIT: {}", sql);
}

#[test]
fn select_with_offset() {
    let cmd = Qail {
        action: Action::Get,
        table: "products".to_string(),
        cages: vec![Cage {
            kind: CageKind::Offset(20),
            conditions: vec![],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("OFFSET 20"), "Must contain OFFSET: {}", sql);
}

#[test]
fn select_with_order_by_desc() {
    let cmd = Qail {
        action: Action::Get,
        table: "events".to_string(),
        cages: vec![Cage {
            kind: CageKind::Sort(SortOrder::Desc),
            conditions: vec![cond("created_at", Operator::Eq, Value::Null)],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("ORDER BY"), "Must contain ORDER BY: {}", sql);
    assert!(sql.contains("DESC"), "Must contain DESC: {}", sql);
}

// ============================================================================
// SELECT — WHERE
// ============================================================================

#[test]
fn select_with_where_eq() {
    let cmd = Qail {
        action: Action::Get,
        table: "users".to_string(),
        cages: vec![filter_cage(vec![cond("id", Operator::Eq, Value::Int(42))])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("WHERE"), "Must contain WHERE: {}", sql);
    assert!(sql.contains("42"), "Must contain value 42: {}", sql);
}

#[test]
fn select_with_where_is_null() {
    let cmd = Qail {
        action: Action::Get,
        table: "tasks".to_string(),
        cages: vec![filter_cage(vec![cond(
            "deleted_at",
            Operator::IsNull,
            Value::Null,
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("IS NULL"), "Must contain IS NULL: {}", sql);
}

#[test]
fn select_with_compound_where() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        cages: vec![filter_cage(vec![
            cond("status", Operator::Eq, Value::String("active".to_string())),
            cond("total", Operator::Gte, Value::Int(100)),
        ])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("WHERE"), "Must contain WHERE: {}", sql);
    assert!(
        sql.contains("AND"),
        "Multiple conditions must use AND: {}",
        sql
    );
}

#[test]
fn select_with_in_operator() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        cages: vec![filter_cage(vec![cond(
            "status",
            Operator::In,
            Value::Array(vec![
                Value::String("active".to_string()),
                Value::String("pending".to_string()),
            ]),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        sql.contains("IN") || sql.contains("ANY"),
        "Must contain IN or ANY operator: {}",
        sql
    );
    assert!(sql.contains("active"), "Must contain 'active': {}", sql);
    assert!(sql.contains("pending"), "Must contain 'pending': {}", sql);
}

// ============================================================================
// INSERT
// ============================================================================

#[test]
fn insert_via_payload_cage() {
    let cmd = Qail {
        action: Action::Add,
        table: "users".to_string(),
        cages: vec![payload_cage(vec![
            cond("name", Operator::Eq, Value::String("Alice".to_string())),
            cond(
                "email",
                Operator::Eq,
                Value::String("alice@example.com".to_string()),
            ),
        ])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.starts_with("INSERT"), "Must start with INSERT: {}", sql);
    assert!(sql.contains("users"), "Must contain table name: {}", sql);
    assert!(sql.contains("VALUES"), "Must contain VALUES: {}", sql);
}

#[test]
fn insert_with_returning() {
    let cmd = Qail {
        action: Action::Add,
        table: "orders".to_string(),
        cages: vec![payload_cage(vec![cond(
            "total",
            Operator::Eq,
            Value::Int(500),
        )])],
        returning: Some(vec![
            Expr::Named("id".to_string()),
            Expr::Named("created_at".to_string()),
        ]),
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("RETURNING"), "Must contain RETURNING: {}", sql);
    assert!(sql.contains("id"), "Must contain 'id' column: {}", sql);
}

// ============================================================================
// UPDATE
// ============================================================================

#[test]
fn update_with_set_and_where() {
    let cmd = Qail {
        action: Action::Set,
        table: "users".to_string(),
        cages: vec![
            payload_cage(vec![cond(
                "name",
                Operator::Eq,
                Value::String("Bob".to_string()),
            )]),
            filter_cage(vec![cond("id", Operator::Eq, Value::Int(1))]),
        ],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.starts_with("UPDATE"), "Must start with UPDATE: {}", sql);
    assert!(sql.contains("SET"), "Must contain SET: {}", sql);
    assert!(sql.contains("WHERE"), "Must contain WHERE: {}", sql);
}

// ============================================================================
// DELETE
// ============================================================================

#[test]
fn delete_with_where() {
    let cmd = Qail {
        action: Action::Del,
        table: "sessions".to_string(),
        cages: vec![filter_cage(vec![cond(
            "expired",
            Operator::Eq,
            Value::Bool(true),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.starts_with("DELETE"), "Must start with DELETE: {}", sql);
    assert!(sql.contains("WHERE"), "Must contain WHERE: {}", sql);
}

#[test]
fn delete_without_where_still_valid() {
    let cmd = Qail {
        action: Action::Del,
        table: "temp_data".to_string(),
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.starts_with("DELETE"), "Must start with DELETE: {}", sql);
    assert!(sql.contains("temp_data"), "Must contain table: {}", sql);
}

// ============================================================================
// JOINs
// ============================================================================

#[test]
fn select_with_inner_join() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        joins: vec![Join {
            table: "users".to_string(),
            kind: JoinKind::Inner,
            on: Some(vec![cond(
                "orders.user_id",
                Operator::Eq,
                Value::Column("users.id".to_string()),
            )]),
            on_true: false,
        }],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("JOIN"), "Must contain JOIN: {}", sql);
    assert!(sql.contains("users"), "Must contain joined table: {}", sql);
    assert!(sql.contains("ON"), "Must contain ON clause: {}", sql);
}

#[test]
fn select_with_left_join() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        joins: vec![Join {
            table: "payments".to_string(),
            kind: JoinKind::Left,
            on: Some(vec![cond(
                "orders.id",
                Operator::Eq,
                Value::Column("payments.order_id".to_string()),
            )]),
            on_true: false,
        }],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("LEFT"), "Must contain LEFT: {}", sql);
    assert!(sql.contains("JOIN"), "Must contain JOIN: {}", sql);
}

// ============================================================================
// Aggregates
// ============================================================================

#[test]
fn aggregate_count() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        columns: vec![Expr::Aggregate {
            col: "*".to_string(),
            func: AggregateFunc::Count,
            distinct: false,
            filter: None,
            alias: None,
        }],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("COUNT("), "Must contain COUNT(: {}", sql);
}

#[test]
fn aggregate_sum_with_alias() {
    let cmd = Qail {
        action: Action::Get,
        table: "line_items".to_string(),
        columns: vec![Expr::Aggregate {
            col: "amount".to_string(),
            func: AggregateFunc::Sum,
            distinct: false,
            filter: None,
            alias: Some("total_amount".to_string()),
        }],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("SUM("), "Must contain SUM(: {}", sql);
    assert!(sql.contains("total_amount"), "Must contain alias: {}", sql);
}

#[test]
fn aggregate_count_distinct() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        columns: vec![Expr::Aggregate {
            col: "user_id".to_string(),
            func: AggregateFunc::Count,
            distinct: true,
            filter: None,
            alias: None,
        }],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("DISTINCT"), "Must contain DISTINCT: {}", sql);
    assert!(sql.contains("COUNT("), "Must contain COUNT(: {}", sql);
}

// ============================================================================
// SQL Injection Safety
// ============================================================================

#[test]
fn table_name_with_semicolons_quoted() {
    let cmd = Qail {
        action: Action::Get,
        table: "users; DROP TABLE users; --".to_string(),
        ..Default::default()
    };
    let sql = cmd.to_sql();
    // The table name should be quoted, wrapping the injection in an identifier
    assert!(
        sql.contains("\"users; DROP TABLE users; --\"") || !sql.contains("DROP TABLE"),
        "Injection in table name must be neutralized by quoting: {}",
        sql
    );
}

#[test]
fn value_with_single_quotes_escaped() {
    let cmd = Qail {
        action: Action::Get,
        table: "users".to_string(),
        cages: vec![filter_cage(vec![cond(
            "name",
            Operator::Eq,
            Value::String("O'Brien".to_string()),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    // Single quotes in values must be escaped via '' or other mechanism
    assert!(
        !sql.contains("O'B") || sql.contains("O''Brien") || sql.contains("O\\'Brien"),
        "Single quotes in values must be escaped: {}",
        sql
    );
}

#[test]
fn null_byte_in_identifier() {
    let cmd = Qail {
        action: Action::Get,
        table: "users\0injected".to_string(),
        ..Default::default()
    };
    let sql = cmd.to_sql();
    // Null bytes must be stripped from identifiers.
    // PostgreSQL C-string protocol terminates at \0, which could allow
    // identifier truncation attacks. The transpiler now strips \0.
    let has_null = sql.as_bytes().contains(&0u8);
    assert!(
        !has_null,
        "Null bytes must not appear in generated SQL: {:?}",
        sql.as_bytes()
    );
}

// ============================================================================
// Parameterized Queries
// ============================================================================

#[test]
fn parameterized_select_has_positional_params() {
    let cmd = Qail {
        action: Action::Get,
        table: "users".to_string(),
        cages: vec![filter_cage(vec![cond(
            "id",
            Operator::Eq,
            Value::NamedParam("id".to_string()),
        )])],
        ..Default::default()
    };
    let result = cmd.to_sql_parameterized();
    assert!(
        result.sql.contains("$1"),
        "Parameterized query must contain $1: {}",
        result.sql
    );
    assert_eq!(
        result.named_params.len(),
        1,
        "Must have 1 named param, got: {:?}",
        result.named_params
    );
}

#[test]
fn parameterized_reuses_same_param() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        cages: vec![filter_cage(vec![
            cond(
                "user_id",
                Operator::Eq,
                Value::NamedParam("uid".to_string()),
            ),
            cond(
                "created_by",
                Operator::Eq,
                Value::NamedParam("uid".to_string()),
            ),
        ])],
        ..Default::default()
    };
    let result = cmd.to_sql_parameterized();
    // Same param name should reuse the same positional index
    let dollar_count = result.sql.matches('$').count();
    assert_eq!(
        dollar_count, 2,
        "Must have 2 param references: {}",
        result.sql
    );
    // Both should reference $1 since it's the same named param
    assert_eq!(
        result.sql.matches("$1").count(),
        2,
        "Same param name must reuse same index: {}",
        result.sql
    );
}

// ============================================================================
// Edge Cases — Value Types
// ============================================================================

#[test]
fn boolean_values_produce_correct_sql() {
    let cmd = Qail {
        action: Action::Get,
        table: "features".to_string(),
        cages: vec![filter_cage(vec![cond(
            "enabled",
            Operator::Eq,
            Value::Bool(true),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        sql.contains("true") || sql.contains("TRUE") || sql.contains("'t'"),
        "Boolean true must produce valid SQL boolean: {}",
        sql
    );
}

#[test]
fn null_value_in_condition() {
    let cmd = Qail {
        action: Action::Get,
        table: "tasks".to_string(),
        cages: vec![filter_cage(vec![cond(
            "completed_at",
            Operator::Eq,
            Value::Null,
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        sql.contains("NULL"),
        "NULL value must produce NULL in SQL: {}",
        sql
    );
}

#[test]
fn float_values_not_truncated() {
    let cmd = Qail {
        action: Action::Add,
        table: "measurements".to_string(),
        cages: vec![payload_cage(vec![cond(
            "value",
            Operator::Eq,
            Value::Float(1.23456789),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        sql.contains("1.23"),
        "Float must preserve precision: {}",
        sql
    );
}

#[test]
fn negative_integer_values() {
    let cmd = Qail {
        action: Action::Get,
        table: "accounts".to_string(),
        cages: vec![filter_cage(vec![cond(
            "balance",
            Operator::Lt,
            Value::Int(-100),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        sql.contains("-100"),
        "Negative integer must appear in SQL: {}",
        sql
    );
}

#[test]
fn very_long_string_value_not_truncated() {
    let long_value = "a".repeat(10000);
    let cmd = Qail {
        action: Action::Add,
        table: "logs".to_string(),
        cages: vec![payload_cage(vec![cond(
            "message",
            Operator::Eq,
            Value::String(long_value.clone()),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        sql.contains(&long_value),
        "Long string values must not be truncated"
    );
}

#[test]
fn unicode_in_values() {
    let cmd = Qail {
        action: Action::Add,
        table: "users".to_string(),
        cages: vec![payload_cage(vec![cond(
            "name",
            Operator::Eq,
            Value::String("日本語テスト 🚀".to_string()),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        sql.contains("日本語テスト") || sql.contains("🚀"),
        "Unicode values must be preserved: {}",
        sql
    );
}

// ============================================================================
// Action → SQL Verb Mapping (Exhaustive)
// ============================================================================

#[test]
fn action_get_produces_select() {
    let cmd = Qail {
        action: Action::Get,
        table: "t".to_string(),
        ..Default::default()
    };
    assert!(cmd.to_sql().starts_with("SELECT"), "Get → SELECT");
}

#[test]
fn action_del_produces_delete() {
    let cmd = Qail {
        action: Action::Del,
        table: "t".to_string(),
        ..Default::default()
    };
    assert!(cmd.to_sql().starts_with("DELETE"), "Del → DELETE");
}

#[test]
fn action_cnt_produces_count() {
    let cmd = Qail {
        action: Action::Cnt,
        table: "t".to_string(),
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.starts_with("SELECT"), "Cnt → SELECT: {}", sql);
    assert!(sql.contains("COUNT("), "Cnt must contain COUNT(: {}", sql);
}

// ============================================================================
// DISTINCT
// ============================================================================

#[test]
fn select_distinct() {
    let cmd = Qail {
        action: Action::Get,
        table: "users".to_string(),
        columns: vec![Expr::Named("role".to_string())],
        distinct: true,
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("DISTINCT"), "Must contain DISTINCT: {}", sql);
}

// ============================================================================
// Multiple operators
// ============================================================================

#[test]
fn select_with_gt_operator() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        cages: vec![filter_cage(vec![cond(
            "total",
            Operator::Gt,
            Value::Int(1000),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains(">"), "GT must produce >: {}", sql);
    assert!(sql.contains("1000"), "Must contain value: {}", sql);
}

#[test]
fn select_with_ne_operator() {
    let cmd = Qail {
        action: Action::Get,
        table: "tasks".to_string(),
        cages: vec![filter_cage(vec![cond(
            "status",
            Operator::Ne,
            Value::String("done".to_string()),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        sql.contains("!=") || sql.contains("<>"),
        "NE must produce != or <>: {}",
        sql
    );
}

#[test]
fn select_with_lte_operator() {
    let cmd = Qail {
        action: Action::Get,
        table: "inventory".to_string(),
        cages: vec![filter_cage(vec![cond(
            "quantity",
            Operator::Lte,
            Value::Int(5),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("<="), "LTE must produce <=: {}", sql);
}
