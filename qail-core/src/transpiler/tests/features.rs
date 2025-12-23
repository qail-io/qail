//! Feature tests (DDL, Upsert, JSON operations, advanced features).

use crate::ast::*;
use crate::parser::parse;
use crate::transpiler::{ToSql, Dialect};

// ============= DDL Tests =============

#[test]
fn test_index_sql_basic() {
    let cmd = parse("index idx_email on users email").unwrap();
    let sql = cmd.to_sql();
    assert!(sql.contains("CREATE INDEX idx_email ON users"));
    assert!(sql.contains("email"));
}

#[test]
fn test_index_sql_unique() {
    let cmd = parse("index idx_unique_email on users email unique").unwrap();
    let sql = cmd.to_sql();
    assert!(sql.contains("CREATE UNIQUE INDEX"));
}

#[test]
fn test_composite_pk_sql() {
    // make order_items order_id:uuid, item_id:uuid primary key(order_id, item_id)
    let cmd = parse("make order_items order_id:uuid, item_id:uuid primary key(order_id, item_id)").unwrap();
    let sql = cmd.to_sql();
    assert!(sql.contains("PRIMARY KEY (order_id, item_id)"));
}

#[test]
fn test_drop_column() {
    // Manual construction for DROP COLUMN
    let mut cmd = QailCmd::get("users");
    cmd.action = Action::DropCol;
    cmd.columns.push(Expr::Named("password".to_string()));
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("ALTER TABLE users DROP COLUMN password"));
}

#[test]
fn test_rename_column() {
    // Manual construction for RENAME COLUMN
    let mut cmd = QailCmd::get("users");
    cmd.action = Action::RenameCol;
    cmd.columns.push(Expr::Named("old_name".to_string()));
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("to".to_string()),
            op: Operator::Eq,
            value: Value::String("new_name".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("ALTER TABLE users RENAME COLUMN old_name TO new_name"));
}

// ============= Upsert Tests =============

#[test]
fn test_upsert_postgres() {
    // Manual construction for UPSERT
    let mut cmd = QailCmd::put("users");
    cmd.columns.push(Expr::Named("id".to_string())); // Conflict key
    cmd.cages.push(Cage {
        kind: CageKind::Payload,
        conditions: vec![
            Condition { left: Expr::Named("id".to_string()), op: Operator::Eq, value: Value::Int(1), is_array_unnest: false },
            Condition { left: Expr::Named("name".to_string()), op: Operator::Eq, value: Value::String("John".to_string()), is_array_unnest: false },
            Condition { left: Expr::Named("role".to_string()), op: Operator::Eq, value: Value::String("admin".to_string()), is_array_unnest: false },
        ],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("INSERT INTO users"));
    assert!(sql.contains("ON CONFLICT (id) DO UPDATE SET"));
    assert!(sql.contains("name = EXCLUDED.name"));
    assert!(sql.contains("RETURNING *"));
}

#[test]
fn test_upsert_mysql() {
    let mut cmd = QailCmd::put("users");
    cmd.columns.push(Expr::Named("id".to_string()));
    cmd.cages.push(Cage {
        kind: CageKind::Payload,
        conditions: vec![
            Condition { left: Expr::Named("id".to_string()), op: Operator::Eq, value: Value::Int(1), is_array_unnest: false },
            Condition { left: Expr::Named("name".to_string()), op: Operator::Eq, value: Value::String("John".to_string()), is_array_unnest: false },
        ],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::MySQL);
    assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
    assert!(sql.contains("`name` = VALUES(`name`)"));
}

// ============= JSON Tests =============

#[test]
fn test_json_access() {
    // Manual construction for JSON field access
    let mut cmd = QailCmd::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("meta.theme".to_string()),
            op: Operator::Eq,
            value: Value::String("dark".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains(r#"meta->>'theme' = 'dark'"#));

    let sql = cmd.to_sql_with_dialect(Dialect::MySQL);
    assert!(sql.contains(r#"`meta`->"$.theme" = 'dark'"#));
}

#[test]
fn test_json_contains() {
    let mut cmd = QailCmd::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("metadata".to_string()),
            op: Operator::Contains,
            value: Value::String(r#"{"theme": "dark"}"#.to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains(r#"@> '{"theme": "dark"}'"#));

    let sql = cmd.to_sql_with_dialect(Dialect::MySQL);
    assert!(sql.contains("JSON_CONTAINS"));
}

#[test]
fn test_json_key_exists() {
    let mut cmd = QailCmd::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("metadata".to_string()),
            op: Operator::KeyExists,
            value: Value::String("theme".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("metadata ? 'theme'"));

    let sql = cmd.to_sql_with_dialect(Dialect::MySQL);
    assert!(sql.contains("JSON_CONTAINS_PATH"));
}

// ============= Advanced Features =============

#[test]
fn test_json_table() {
    let mut cmd = QailCmd::get("orders.items");
    cmd.action = Action::JsonTable;
    cmd.columns = vec![
        Expr::Named("name=$.product".to_string()),
        Expr::Named("qty=$.quantity".to_string()),
    ];

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("JSON_TABLE("));
    assert!(sql.contains("COLUMNS"));
}

#[test]
fn test_tablesample() {
    let mut cmd = QailCmd::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Sample(10),
        conditions: vec![],
        logical_op: LogicalOp::And,
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("TABLESAMPLE BERNOULLI(10)"));
}

#[test]
fn test_qualify() {
    let mut cmd = QailCmd::get("users");
    cmd.columns.push(Expr::Named("id".to_string()));
    cmd.cages.push(Cage {
        kind: CageKind::Qualify,
        conditions: vec![Condition {
            left: Expr::Named("rn".to_string()),
            op: Operator::Eq,
            value: Value::Int(1),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Snowflake);
    assert!(sql.contains(r#"QUALIFY "rn" = 1"#));
}

#[test]
fn test_lateral_join() {
    let mut cmd = QailCmd::get("users");
    cmd.columns.push(Expr::Named("*".to_string()));
    cmd.joins.push(Join {
        table: "orders".to_string(),
        kind: JoinKind::Lateral,
        on: None,
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("LATERAL JOIN"));
}

// ============= SQL/JSON Standard Functions (Postgres 17+) =============

#[test]
fn test_json_exists() {
    let mut cmd = QailCmd::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("metadata".to_string()),
            op: Operator::JsonExists,
            value: Value::String("$.theme".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    println!("JSON_EXISTS: {}", sql);
    assert!(sql.contains("JSON_EXISTS("));
    assert!(sql.contains("$.theme"));
}

#[test]
fn test_json_query() {
    let mut cmd = QailCmd::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("settings".to_string()),
            op: Operator::JsonQuery,
            value: Value::String("$.notifications".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    println!("JSON_QUERY: {}", sql);
    assert!(sql.contains("JSON_QUERY("));
}

#[test]
fn test_json_value() {
    let mut cmd = QailCmd::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("profile".to_string()),
            op: Operator::JsonValue,
            value: Value::String("$.name".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    println!("JSON_VALUE: {}", sql);
    assert!(sql.contains("JSON_VALUE("));
    
    // MySQL should also use JSON_VALUE
    let sql = cmd.to_sql_with_dialect(Dialect::MySQL);
    println!("MySQL JSON_VALUE: {}", sql);
    assert!(sql.contains("JSON_VALUE("));
}

// ============= Set Operations (UNION, INTERSECT, EXCEPT) =============

#[test]
fn test_union() {
    let mut users_cmd = QailCmd::get("users");
    users_cmd.columns.push(Expr::Named("name".to_string()));
    
    let mut admins_cmd = QailCmd::get("admins");
    admins_cmd.columns.push(Expr::Named("name".to_string()));
    
    users_cmd.set_ops.push((SetOp::Union, Box::new(admins_cmd)));
    
    let sql = users_cmd.to_sql_with_dialect(Dialect::Postgres);
    println!("UNION: {}", sql);
    assert!(sql.contains("UNION"));
    assert!(sql.contains("users"));
    assert!(sql.contains("admins"));
}

#[test]
fn test_union_all() {
    let mut q1 = QailCmd::get("active_users");
    let q2 = QailCmd::get("inactive_users");
    
    q1.set_ops.push((SetOp::UnionAll, Box::new(q2)));
    
    let sql = q1.to_sql();
    println!("UNION ALL: {}", sql);
    assert!(sql.contains("UNION ALL"));
}

#[test]
fn test_intersect() {
    let mut q1 = QailCmd::get("premium_users");
    q1.columns.push(Expr::Named("id".to_string()));
    
    let mut q2 = QailCmd::get("verified_users");
    q2.columns.push(Expr::Named("id".to_string()));
    
    q1.set_ops.push((SetOp::Intersect, Box::new(q2)));
    
    let sql = q1.to_sql();
    println!("INTERSECT: {}", sql);
    assert!(sql.contains("INTERSECT"));
}

// ============= CASE Expressions =============

#[test]
fn test_case_expression() {
    let mut cmd = QailCmd::get("users");
    cmd.columns.push(Expr::Named("name".to_string()));
    cmd.columns.push(Expr::Case {
        when_clauses: vec![
            (Condition {
                left: Expr::Named("status".to_string()),
                op: Operator::Eq,
                value: Value::String("active".to_string()),
                is_array_unnest: false,
            }, Box::new(Expr::Named("1".to_string()))),
            (Condition {
                left: Expr::Named("status".to_string()),
                op: Operator::Eq,
                value: Value::String("pending".to_string()),
                is_array_unnest: false,
            }, Box::new(Expr::Named("2".to_string()))),
        ],
        else_value: Some(Box::new(Expr::Named("0".to_string()))),
        alias: Some("priority".to_string()),
    });
    
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    println!("CASE: {}", sql);
    assert!(sql.contains("CASE"));
    assert!(sql.contains("WHEN"));
    assert!(sql.contains("THEN"));
    assert!(sql.contains("ELSE"));
    assert!(sql.contains("END"));
    assert!(sql.contains("AS"));
}

// ============= HAVING Clause =============

#[test]
fn test_having_clause() {
    let mut cmd = QailCmd::get("orders");
    cmd.columns.push(Expr::Named("customer_id".to_string()));
    cmd.columns.push(Expr::Aggregate { 
        col: "total".to_string(), 
        func: AggregateFunc::Sum 
    });
    cmd.having.push(Condition {
        left: Expr::Named("SUM(total)".to_string()),
        op: Operator::Gt,
        value: Value::Int(100),
        is_array_unnest: false,
    });
    
    let sql = cmd.to_sql();
    println!("HAVING: {}", sql);
    assert!(sql.contains("HAVING"));
    assert!(sql.contains("SUM(total)"));
}

// ============= ROLLUP / CUBE =============

#[test]
fn test_group_by_rollup() {
    let mut cmd = QailCmd::get("sales");
    cmd.columns.push(Expr::Named("region".to_string()));
    cmd.columns.push(Expr::Named("year".to_string()));
    cmd.columns.push(Expr::Aggregate { 
        col: "amount".to_string(), 
        func: AggregateFunc::Sum 
    });
    cmd.group_by_mode = GroupByMode::Rollup;
    
    let sql = cmd.to_sql();
    println!("ROLLUP: {}", sql);
    assert!(sql.contains("GROUP BY ROLLUP("));
}

#[test]
fn test_group_by_cube() {
    let mut cmd = QailCmd::get("sales");
    cmd.columns.push(Expr::Named("region".to_string()));
    cmd.columns.push(Expr::Named("product".to_string()));
    cmd.columns.push(Expr::Aggregate { 
        col: "amount".to_string(), 
        func: AggregateFunc::Sum 
    });
    cmd.group_by_mode = GroupByMode::Cube;
    
    let sql = cmd.to_sql();
    println!("CUBE: {}", sql);
    assert!(sql.contains("GROUP BY CUBE("));
}

// ============= RECURSIVE CTEs =============

#[test]
fn test_recursive_cte() {
    // Build base query: SELECT id, name, manager_id FROM employees WHERE manager_id IS NULL
    let mut base = QailCmd::get("employees");
    base.columns.push(Expr::Named("id".to_string()));
    base.columns.push(Expr::Named("name".to_string()));
    base.columns.push(Expr::Named("manager_id".to_string()));
    base.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("manager_id".to_string()),
            op: Operator::IsNull,
            value: Value::Null,
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    
    // Build recursive query: SELECT e.id, e.name, e.manager_id FROM employees e JOIN emp_tree ...
    let mut recursive = QailCmd::get("employees");
    recursive.columns.push(Expr::Named("id".to_string()));
    recursive.columns.push(Expr::Named("name".to_string()));
    recursive.columns.push(Expr::Named("manager_id".to_string()));
    
    // Outer query with CTE
    let mut cmd = QailCmd::get("emp_tree");
    cmd.ctes = vec![CTEDef {
        name: "emp_tree".to_string(),
        recursive: true,
        columns: vec!["id".to_string(), "name".to_string(), "manager_id".to_string()],
        base_query: Box::new(base),
        recursive_query: Some(Box::new(recursive)),
        source_table: Some("employees".to_string()),
    }];
    cmd.action = Action::With;
    
    use crate::transpiler::dml::cte::build_cte;
    let sql = build_cte(&cmd, Dialect::Postgres);
    println!("RECURSIVE CTE: {}", sql);
    assert!(sql.contains("WITH RECURSIVE"));
    assert!(sql.contains("emp_tree"));
    assert!(sql.contains("UNION ALL"));
}


// ============= v0.8.6: Custom JOINs & DISTINCT ON =============

#[test]
fn test_custom_join_on() {
    // Manual construction for JOIN with ON clause
    let mut cmd = QailCmd::get("users");
    cmd.joins.push(Join {
        table: "orders".to_string(),
        kind: JoinKind::Inner,
        on: Some(vec![Condition {
            left: Expr::Named("users.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("orders.user_id".to_string()),
            is_array_unnest: false,
        }]),
    });
    let sql = cmd.to_sql();
    // Identifiers are unquoted if safe in Postgres dialect implementation used
    assert!(sql.contains("INNER JOIN orders ON users.id = orders.user_id"), "SQL was: {}", sql);
}

#[test]
fn test_custom_join_multiple_conditions() {
    let mut cmd = QailCmd::get("A");
    cmd.joins.push(Join {
        table: "B".to_string(),
        kind: JoinKind::Inner,
        on: Some(vec![
            Condition {
                left: Expr::Named("A.x".to_string()),
                op: Operator::Eq,
                value: Value::Column("B.x".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("A.y".to_string()),
                op: Operator::Eq,
                value: Value::Column("B.y".to_string()),
                is_array_unnest: false,
            },
        ]),
    });
    let sql = cmd.to_sql();
    assert!(sql.contains("INNER JOIN B ON A.x = B.x AND A.y = B.y"), "SQL was: {}", sql);
    // Verify AST structure
    assert!(cmd.joins[0].on.is_some());
    assert_eq!(cmd.joins[0].on.as_ref().unwrap().len(), 2);
}

#[test]
fn test_distinct_on() {
    // Manual construction for DISTINCT ON
    let mut cmd = QailCmd::get("employees");
    cmd.distinct_on = vec!["department".to_string(), "role".to_string()];
    
    // Transpiler check (Postgres default)
    let sql = cmd.to_sql();
    assert!(sql.starts_with("SELECT DISTINCT ON (department, role)"), "SQL was: {}", sql);
}
