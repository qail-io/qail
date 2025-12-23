use crate::parser::parse;
use crate::ast::*;

// Join tests - parser v2 join syntax needs to be implemented
// For now, test with AST construction

#[test]
fn test_v2_left_join() {
    let cmd = parse("get users join posts on users.id = posts.user_id fields id, title").unwrap();
    assert_eq!(cmd.joins.len(), 1);
    assert_eq!(cmd.joins[0].table, "posts");
    assert_eq!(cmd.joins[0].kind, JoinKind::Left);
}

#[test]
fn test_v2_inner_join() {
    let cmd = parse("get users inner join posts on users.id = posts.user_id fields id, title").unwrap();
    assert_eq!(cmd.joins.len(), 1);
    assert_eq!(cmd.joins[0].table, "posts");
    assert_eq!(cmd.joins[0].kind, JoinKind::Inner);
}

#[test]
fn test_v2_right_join() {
    let cmd = parse("get orders right join customers on orders.customer_id = customers.id").unwrap();
    assert_eq!(cmd.joins.len(), 1);
    assert_eq!(cmd.joins[0].table, "customers");
    assert_eq!(cmd.joins[0].kind, JoinKind::Right);
}
