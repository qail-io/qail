use crate::ast::*;
use crate::parser::parse;

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
    assert_eq!(
        payload.conditions[1].left,
        Expr::Named("active".to_string())
    );
}

#[test]
fn test_delete_with_filter() {
    let cmd = parse("del sessions where user_id = $1 and expired = true").unwrap();
    assert_eq!(cmd.action, Action::Del);
    assert_eq!(cmd.cages[0].conditions.len(), 2);
}

#[test]
fn test_delete_with_or_filter() {
    let cmd = parse("del sessions where user_id = $1 or expired = true").unwrap();
    assert_eq!(cmd.action, Action::Del);
    assert_eq!(cmd.cages[0].logical_op, LogicalOp::Or);
    assert_eq!(cmd.cages[0].conditions.len(), 2);
}

#[test]
fn test_set_mixed_and_or_rejected() {
    let result = parse(
        "set users values verified = true where id = $1 and active = true or role = \"admin\"",
    );
    assert!(result.is_err());
}

#[test]
fn test_conflict_update_escapes_triple_quoted_string_assignment() {
    let cmd =
        parse("add users values 1, \"Ana\" conflict (id) update name = '''O'Reilly'''").unwrap();

    let on_conflict = cmd.on_conflict.unwrap();
    match on_conflict.action {
        ConflictAction::DoUpdate { assignments } => {
            assert_eq!(assignments[0].0, "name");
            assert_eq!(assignments[0].1, Expr::Named("'O''Reilly'".to_string()));
        }
        other => panic!("expected conflict update, got {other:?}"),
    }
}
