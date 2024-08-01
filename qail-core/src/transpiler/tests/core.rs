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
fn test_select_with_limit() {
    let cmd = parse("get users fields * limit 10").unwrap();
    assert_eq!(cmd.to_sql(), "SELECT * FROM users LIMIT 10");
}

#[test]
fn test_select_with_order() {
    let cmd = parse("get users fields * order by created_at desc").unwrap();
    assert_eq!(cmd.to_sql(), "SELECT * FROM users ORDER BY created_at DESC");
}

#[test]
fn test_select_complex() {
    let cmd = parse("get users fields id, email where active = true order by created_at desc limit 10").unwrap();
    assert_eq!(
        cmd.to_sql(),
        "SELECT id, email FROM users WHERE active = true ORDER BY created_at DESC LIMIT 10"
    );
}

#[test]
fn test_update() {
    let cmd = parse("set users values verified = true where id = $1").unwrap();
    assert_eq!(cmd.to_sql(), "UPDATE users SET verified = true WHERE id = $1");
}

#[test]
fn test_delete() {
    let cmd = parse("del users where id = $1").unwrap();
    assert_eq!(cmd.to_sql(), "DELETE FROM users WHERE id = $1");
}

#[test]
fn test_fuzzy_match() {
    let cmd = parse("get users fields * where name ~ $1").unwrap();
    assert_eq!(cmd.to_sql(), "SELECT * FROM users WHERE name ILIKE '%' || $1 || '%'");
}

// OR conditions - using manual QailCmd construction
#[test]
fn test_or_conditions() {
    use crate::ast::*;
    let mut cmd = QailCmd::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![
            Condition { left: Expr::Named("status".to_string()), op: Operator::Eq, value: Value::String("active".to_string()), is_array_unnest: false },
            Condition { left: Expr::Named("status".to_string()), op: Operator::Eq, value: Value::String("pending".to_string()), is_array_unnest: false },
        ],
        logical_op: LogicalOp::Or,
    });
    let sql = cmd.to_sql();
    assert!(sql.contains("status = 'active' OR status = 'pending'"));
}

#[test]
fn test_array_unnest() {
    use crate::ast::*;
    let mut cmd = QailCmd::get("users");
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
fn test_left_join() {
    use crate::ast::*;
    let mut cmd = QailCmd::get("users");
    cmd.joins.push(Join { table: "posts".to_string(), kind: JoinKind::Left, on: None, on_true: false });
    let sql = cmd.to_sql();
    assert!(sql.contains("LEFT JOIN"));
    assert!(sql.contains("posts"));
}

#[test]
fn test_right_join() {
    use crate::ast::*;
    let mut cmd = QailCmd::get("users");
    cmd.joins.push(Join { table: "posts".to_string(), kind: JoinKind::Right, on: None, on_true: false });
    let sql = cmd.to_sql();
    assert!(sql.contains("RIGHT JOIN"));
}

#[test]
fn test_distinct() {
    use crate::ast::*;
    let mut cmd = QailCmd::get("users");
    cmd.distinct = true;
    cmd.columns.push(Expr::Named("role".to_string()));
    let sql = cmd.to_sql();
    assert!(sql.contains("SELECT DISTINCT"));
    assert!(sql.contains("role"));
}

#[test]
fn test_transactions() {
    use crate::ast::{QailCmd, Action};
    let mut cmd = QailCmd::get("users");
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
    assert!(result.sql.contains("$1"), "SQL should have $1 placeholder: {}", result.sql);
    assert!(result.sql.contains("$2"), "SQL should have $2 placeholder: {}", result.sql);
    assert!(!result.sql.contains(":name"), "SQL should NOT contain ':name': {}", result.sql);
    assert!(!result.sql.contains(":age"), "SQL should NOT contain ':age': {}", result.sql);
    
    // Named params should be extracted in order
    assert_eq!(result.named_params.len(), 2);
    assert_eq!(result.named_params[0], "name");
    assert_eq!(result.named_params[1], "age");
}
