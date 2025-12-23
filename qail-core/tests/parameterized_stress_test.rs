//! Stress tests for parameterized SQL generation.
//! 
//! These tests verify that QAIL correctly extracts values into 
//! prepared statement placeholders ($1, $2, etc.) for SQLx integration.

use qail_core::prelude::*;
use qail_core::transpiler::ToSqlParameterized;
use qail_core::parser::parse;

/// Test basic value extraction
#[test]
fn test_basic_parameterization() {
    let cmd = parse("get users fields * where name = \"Alice\" and age = 25").unwrap();
    let result = cmd.to_sql_parameterized();
    
    println!("SQL: {}", result.sql);
    println!("Params: {:?}", result.params);
    
    assert!(result.sql.contains("$1"), "Expected $1 placeholder");
    assert!(result.sql.contains("$2"), "Expected $2 placeholder");
    assert!(!result.sql.contains("Alice"), "Literal should not be in SQL");
    assert_eq!(result.params.len(), 2);
}

/// Test mixed params and literals
#[test]
fn test_explicit_param_passthrough() {
    // When user explicitly uses $1, it should pass through
    let cmd = parse("get users fields * where id = $1").unwrap();
    let result = cmd.to_sql_parameterized();
    
    println!("SQL: {}", result.sql);
    println!("Params: {:?}", result.params);
    
    // Explicit $1 should be kept as-is (user provides this value)
    assert!(result.sql.contains("$1"));
    // No extracted params since user is managing $1
    assert_eq!(result.params.len(), 0);
}

/// Test boolean parameterization
#[test]
fn test_boolean_params() {
    let cmd = parse("get users fields * where active = true and verified = false").unwrap();
    let result = cmd.to_sql_parameterized();
    
    println!("SQL: {}", result.sql);
    println!("Params: {:?}", result.params);
    
    assert_eq!(result.params.len(), 2);
    assert_eq!(result.params[0], Value::Bool(true));
    assert_eq!(result.params[1], Value::Bool(false));
}

/// Test numeric types
#[test]
fn test_numeric_params() {
    let cmd = parse("get products fields * where price = 99.99 and quantity = 100").unwrap();
    let result = cmd.to_sql_parameterized();
    
    println!("SQL: {}", result.sql);
    println!("Params: {:?}", result.params);
    
    assert_eq!(result.params.len(), 2);
    // Check we have float and int
    match &result.params[0] {
        Value::Float(f) => assert!((f - 99.99).abs() < 0.001),
        _ => panic!("Expected Float for price"),
    }
    match &result.params[1] {
        Value::Int(i) => assert_eq!(*i, 100),
        _ => panic!("Expected Int for quantity"),
    }
}

/// Test complex query with multiple conditions
#[test]
fn test_complex_multi_condition() {
    let cmd = parse("get orders fields id, total, status where status = \"pending\" and amount > 100").unwrap();
    let result = cmd.to_sql_parameterized();
    
    println!("SQL: {}", result.sql);
    println!("Params: {:?}", result.params);
    
    // Should have proper column selection
    assert!(result.sql.contains("SELECT"));
    assert!(result.sql.contains("id"));
    assert!(result.sql.contains("total"));
    assert!(result.sql.contains("WHERE"));
    
    // Values should be parameterized
    assert!(!result.sql.contains("pending"));
    assert!(!result.sql.contains("100"));
    assert!(result.params.len() >= 2);
}

/// Test NULL handling
#[test]
fn test_null_handling() {
    // NULL conditions should not create params
    let mut cmd = QailCmd::get("users");
    cmd.columns.push(Expr::Star);
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("deleted_at".to_string()),
            op: Operator::IsNull,
            value: Value::Null,
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    
    let result = cmd.to_sql_parameterized();
    println!("SQL: {}", result.sql);
    
    assert!(result.sql.contains("IS NULL"));
    assert_eq!(result.params.len(), 0, "NULL checks shouldn't add params");
}

/// Test LIMIT/OFFSET
#[test]
fn test_limit_offset() {
    let cmd = parse("get users fields * limit 20 offset 40").unwrap();
    let result = cmd.to_sql_parameterized();
    
    println!("SQL: {}", result.sql);
    
    // LIMIT/OFFSET are inline in current impl (not parameterized)
    // This is common behavior - many drivers don't parameterize LIMIT
    assert!(result.sql.contains("LIMIT"));
    assert!(result.sql.contains("OFFSET"));
}

/// Placeholder numbering should be sequential
#[test]
fn test_sequential_placeholder_numbering() {
    let cmd = parse("get users fields * where a=1 and b=2 and c=3 and d=4 and e=5").unwrap();
    let result = cmd.to_sql_parameterized();
    
    println!("SQL: {}", result.sql);
    
    // Should have $1 through $5
    assert!(result.sql.contains("$1"));
    assert!(result.sql.contains("$2"));
    assert!(result.sql.contains("$3"));
    assert!(result.sql.contains("$4"));
    assert!(result.sql.contains("$5"));
    assert_eq!(result.params.len(), 5);
}

/// Test edge case: empty conditions
#[test]
fn test_no_conditions() {
    let cmd = parse("get users fields *").unwrap();
    let result = cmd.to_sql_parameterized();
    
    println!("SQL: {}", result.sql);
    
    assert_eq!(result.sql, "SELECT * FROM users");
    assert_eq!(result.params.len(), 0);
}

/// Test string escaping doesn't affect parameterized version
#[test]
fn test_string_with_quotes() {
    // Parser doesn't handle embedded quotes, so test via AST
    let mut cmd = QailCmd::get("users");
    cmd.columns.push(Expr::Star);
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("name".to_string()),
            op: Operator::Eq,
            value: Value::String("O'Brien".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    
    let result = cmd.to_sql_parameterized();
    
    println!("SQL: {}", result.sql);
    println!("Params: {:?}", result.params);
    
    // The literal with quote should be in params, not SQL
    assert!(!result.sql.contains("O'Brien"));
    assert!(!result.sql.contains("O''Brien")); // No escaped quotes in SQL
    assert_eq!(result.params.len(), 1);
    assert_eq!(result.params[0], Value::String("O'Brien".to_string()));
}
