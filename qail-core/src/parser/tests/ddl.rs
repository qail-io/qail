use crate::parser::parse;
use crate::ast::*;

// ========================================================================
// Schema v0.7.0 Tests (DEFAULT, CHECK)
// ========================================================================

#[test]
fn test_make_with_default_uuid() {
    let cmd = parse("make::users:'id:uuid^pk = uuid()").unwrap();
    assert_eq!(cmd.action, Action::Make);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.columns.len(), 1);
    if let Column::Def { name, data_type, constraints } = &cmd.columns[0] {
        assert_eq!(name, "id");
        assert_eq!(data_type, "uuid");
        assert!(constraints.contains(&Constraint::PrimaryKey));
        assert!(constraints.iter().any(|c| matches!(c, Constraint::Default(v) if v == "uuid()")));
    } else {
        panic!("Expected Column::Def");
    }
}

#[test]
fn test_make_with_default_numeric() {
    let cmd = parse("make::stats:'count:bigint = 0").unwrap();
    assert_eq!(cmd.action, Action::Make);
    if let Column::Def { constraints, .. } = &cmd.columns[0] {
        assert!(constraints.iter().any(|c| matches!(c, Constraint::Default(v) if v == "0")));
    } else {
        panic!("Expected Column::Def");
    }
}

#[test]
fn test_make_with_check_constraint() {
    let cmd = parse(r#"make::orders:'status:varchar^check("pending","paid","cancelled")"#).unwrap();
    assert_eq!(cmd.action, Action::Make);
    if let Column::Def { name, constraints, .. } = &cmd.columns[0] {
        assert_eq!(name, "status");
        let check = constraints.iter().find(|c| matches!(c, Constraint::Check(_)));
        assert!(check.is_some());
        if let Some(Constraint::Check(vals)) = check {
            assert_eq!(vals, &vec!["pending".to_string(), "paid".to_string(), "cancelled".to_string()]);
        }
    } else {
        panic!("Expected Column::Def");
    }
}

// ========================================================================
// Composite Table Constraints Tests (v0.7.0)
// ========================================================================

#[test]
fn test_make_composite_unique() {
    let cmd = parse("make::bookings:'user_id:uuid'schedule_id:uuid^unique(user_id, schedule_id)").unwrap();
    assert_eq!(cmd.action, Action::Make);
    assert_eq!(cmd.table_constraints.len(), 1);
    if let TableConstraint::Unique(cols) = &cmd.table_constraints[0] {
        assert_eq!(cols, &vec!["user_id".to_string(), "schedule_id".to_string()]);
    } else {
        panic!("Expected TableConstraint::Unique");
    }
}

#[test]
fn test_make_composite_pk() {
    let cmd = parse("make::order_items:'order_id:uuid'product_id:uuid^pk(order_id, product_id)").unwrap();
    assert_eq!(cmd.action, Action::Make);
    assert_eq!(cmd.table_constraints.len(), 1);
    if let TableConstraint::PrimaryKey(cols) = &cmd.table_constraints[0] {
        assert_eq!(cols, &vec!["order_id".to_string(), "product_id".to_string()]);
    } else {
        panic!("Expected TableConstraint::PrimaryKey");
    }
}

#[test]
fn test_ddl_commands() {
    let cmd = parse("put::users:[id=1][name=John]").unwrap();
    assert_eq!(cmd.action, Action::Put);
    assert_eq!(cmd.table, "users");
    
    let cmd = parse("drop::users:password").unwrap();
    assert_eq!(cmd.action, Action::DropCol);
    assert_eq!(cmd.table, "users");
    if let Column::Named(n) = &cmd.columns[0] {
        assert_eq!(n, "password");
    }
    
    let cmd = parse("rename::users:oldname").unwrap();
    assert_eq!(cmd.action, Action::RenameCol);
}
