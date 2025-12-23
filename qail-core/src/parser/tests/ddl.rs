#![allow(unused_imports)]
use crate::parser::parse;
use crate::ast::*;

// ========================================================================
// Schema Tests - V2 Syntax
// ========================================================================

#[test]
fn test_make_with_default_uuid() {
    let q = "make users id:uuid:pk:default=uuid()";
    let cmd = parse(q).unwrap();
    
    assert_eq!(cmd.action, Action::Make);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.columns.len(), 1);
    
    if let Expr::Def { name, data_type, constraints } = &cmd.columns[0] {
        assert_eq!(name, "id");
        assert_eq!(data_type, "uuid");
        assert!(constraints.contains(&Constraint::PrimaryKey));
        assert!(constraints.iter().any(|c| matches!(c, Constraint::Default(v) if v == "uuid()")));
    } else {
        panic!("Expected Expr::Def");
    }
}

#[test]
fn test_make_with_default_numeric() {
    let q = "make stats count:bigint:default=0";
    let cmd = parse(q).unwrap();
    
    assert_eq!(cmd.action, Action::Make);
    if let Expr::Def { constraints, .. } = &cmd.columns[0] {
        assert!(constraints.iter().any(|c| matches!(c, Constraint::Default(v) if v == "0")));
    } else {
        panic!("Expected Expr::Def");
    }
}

#[test]
fn test_make_with_check_constraint() {
    let q = "make orders status:varchar:check=pending";
    // note: parser simplified check constraint to single token in taking_while1 or similar? 
    // In parse_constraint: recognize(take_while1(|c| c != ',' && c != ':' && c != ' '))
    // So "check=pending" works. 
    // What if we want "check=age>18"? "check=age>18" works if no spaces.
    let cmd = parse(q).unwrap();
    
    assert_eq!(cmd.action, Action::Make);
    if let Expr::Def { name, constraints, .. } = &cmd.columns[0] {
        assert_eq!(name, "status");
        let check = constraints.iter().find(|c| matches!(c, Constraint::Check(_)));
        assert!(check.is_some());
        if let Some(Constraint::Check(vals)) = check {
            assert_eq!(vals[0], "pending");
        }
    } else {
        panic!("Expected Expr::Def");
    }
}

#[test]
fn test_make_composite_unique() {
    // make bookings user_id:uuid, schedule_id:uuid unique(user_id, schedule_id)
    let q = "make bookings user_id:uuid, schedule_id:uuid unique(user_id, schedule_id)";
    let cmd = parse(q).unwrap();
    
    assert_eq!(cmd.action, Action::Make);
    assert_eq!(cmd.table_constraints.len(), 1);
    if let TableConstraint::Unique(cols) = &cmd.table_constraints[0] {
        assert_eq!(cols, &vec!["user_id".to_string(), "schedule_id".to_string()]);
    } else {
        panic!("Expected Unique constraint");
    }
}

#[test]
fn test_make_composite_pk() {
    // make order_items order_id:uuid, product_id:uuid primary key(order_id, product_id)
    let q = "make order_items order_id:uuid, product_id:uuid primary key(order_id, product_id)";
    let cmd = parse(q).unwrap();
    
    assert_eq!(cmd.action, Action::Make);
    assert_eq!(cmd.table_constraints.len(), 1);
    if let TableConstraint::PrimaryKey(cols) = &cmd.table_constraints[0] {
        assert_eq!(cols, &vec!["order_id".to_string(), "product_id".to_string()]);
    } else {
        panic!("Expected PrimaryKey constraint");
    }
}

// Keep manual construction for unimplemented/complex commands
#[test]
fn test_ddl_commands_manual() {
    // Put command (not fully in v2 parser yet)
    let mut cmd = QailCmd::put("users");
    cmd.columns.push(Expr::Named("id".to_string())); // Conflict col
    cmd.cages.push(Cage {
        kind: CageKind::Payload,
        conditions: vec![
            Condition { left: Expr::Named("id".to_string()), op: Operator::Eq, value: Value::Int(1), is_array_unnest: false },
        ],
        logical_op: LogicalOp::And,
    });
    assert_eq!(cmd.action, Action::Put);
    
    // Drop column (Action::DropCol) - no parser syntax yet
    let mut cmd_drop = QailCmd::get("users");
    cmd_drop.action = Action::DropCol;
    cmd_drop.columns.push(Expr::Named("password".to_string()));
    assert_eq!(cmd_drop.action, Action::DropCol);
    
    // Rename column (Action::RenameCol) - no parser syntax yet
    let mut cmd_ren = QailCmd::get("users");
    cmd_ren.action = Action::RenameCol;
    cmd_ren.columns.push(Expr::Named("oldname".to_string()));
    assert_eq!(cmd_ren.action, Action::RenameCol);
}
