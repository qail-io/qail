use crate::parser::parse;
use crate::ast::*;

#[test]
fn test_index_basic() {
    let cmd = parse("index::idx_users_email^on(users:'email)").unwrap();
    assert_eq!(cmd.action, Action::Index);
    let idx = cmd.index_def.expect("index_def should be Some");
    assert_eq!(idx.name, "idx_users_email");
    assert_eq!(idx.table, "users");
    assert_eq!(idx.columns, vec!["email".to_string()]);
    assert!(!idx.unique);
}

#[test]
fn test_index_composite() {
    let cmd = parse("index::idx_lookup^on(orders:'user_id-created_at)").unwrap();
    assert_eq!(cmd.action, Action::Index);
    let idx = cmd.index_def.expect("index_def should be Some");
    assert_eq!(idx.name, "idx_lookup");
    assert_eq!(idx.table, "orders");
    assert_eq!(idx.columns, vec!["user_id".to_string(), "created_at".to_string()]);
}

#[test]
fn test_index_unique() {
    let cmd = parse("index::idx_phone^on(users:'phone)^unique").unwrap();
    assert_eq!(cmd.action, Action::Index);
    let idx = cmd.index_def.expect("index_def should be Some");
    assert_eq!(idx.name, "idx_phone");
    assert!(idx.unique);
}
