use crate::parser::parse;
use crate::ast::*;

#[test]
fn test_set_command() {
    let cmd = parse("set::users:[verified=true][id=$1]").unwrap();
    assert_eq!(cmd.action, Action::Set);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.cages.len(), 2);
}

#[test]
fn test_del_command() {
    let cmd = parse("del::sessions:[expired_at<now]").unwrap();
    assert_eq!(cmd.action, Action::Del);
    assert_eq!(cmd.table, "sessions");
}

#[test]
fn test_param_in_update() {
    let cmd = parse("set::users:[verified=true][id=$1]").unwrap();
    assert_eq!(cmd.action, Action::Set);
    assert_eq!(cmd.cages.len(), 2);
    assert_eq!(cmd.cages[1].conditions[0].value, Value::Param(1));
}
