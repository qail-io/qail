use crate::parser::parse;
use crate::ast::*;

#[test]
fn test_set_command() {
    // set users values verified = true where id = $1
    let cmd = parse("set users values verified = true where id = $1").unwrap();
    assert_eq!(cmd.action, Action::Set);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.cages.len(), 2); // Payload + Filter
}

#[test]
fn test_del_command() {
    // del sessions where expired_at < $1
    let cmd = parse("del sessions where expired_at < $1").unwrap();
    assert_eq!(cmd.action, Action::Del);
    assert_eq!(cmd.table, "sessions");
}

#[test]
fn test_param_in_update() {
    let cmd = parse("set users values verified = true where id = $1").unwrap();
    assert_eq!(cmd.action, Action::Set);
    assert_eq!(cmd.cages.len(), 2);
    // Filter cage is second
    assert_eq!(cmd.cages[1].conditions[0].value, Value::Param(1));
}

#[test]
fn test_update_multiple_values() {
    let cmd = parse("set users values name = \"John\", active = true where id = $1").unwrap();
    assert_eq!(cmd.action, Action::Set);
    // Payload cage should have 2 conditions
    let payload = &cmd.cages[0];
    assert_eq!(payload.kind, CageKind::Payload);
    assert_eq!(payload.conditions.len(), 2);
    assert_eq!(payload.conditions[0].left, Expr::Named("name".to_string()));
    assert_eq!(payload.conditions[1].left, Expr::Named("active".to_string()));
}

#[test]
fn test_delete_with_filter() {
    let cmd = parse("del sessions where user_id = $1 and expired = true").unwrap();
    assert_eq!(cmd.action, Action::Del);
    assert_eq!(cmd.cages[0].conditions.len(), 2);
}
