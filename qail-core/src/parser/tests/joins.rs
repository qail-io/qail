use crate::parser::parse;
use crate::ast::*;

#[test]
fn test_v2_left_join() {
    // Joins come directly after table name, not after columns
    let cmd = parse("get::users<-posts:'id'title").unwrap();
    assert_eq!(cmd.joins.len(), 1);
    assert_eq!(cmd.joins[0].table, "posts");
    assert_eq!(cmd.joins[0].kind, JoinKind::Left);
}

#[test]
fn test_v2_inner_join() {
    let cmd = parse("get::users->posts:'id'title").unwrap();
    assert_eq!(cmd.joins.len(), 1);
    assert_eq!(cmd.joins[0].table, "posts");
    assert_eq!(cmd.joins[0].kind, JoinKind::Inner);
}

#[test]
fn test_v2_right_join() {
    let cmd = parse("get::orders->>customers:'_").unwrap();
    assert_eq!(cmd.joins.len(), 1);
    assert_eq!(cmd.joins[0].table, "customers");
    assert_eq!(cmd.joins[0].kind, JoinKind::Right);
}
