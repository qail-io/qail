use crate::parser::parse;
use crate::ast::*;

#[test]
fn test_v2_simple_get() {
    let cmd = parse("get users").unwrap();
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    // Default is Star when no fields specified
    assert_eq!(cmd.columns, vec![Expr::Star]);
}

#[test]
fn test_v2_get_with_star() {
    let cmd = parse("get users fields *").unwrap();
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.columns, vec![Expr::Star]);
}

#[test]
fn test_v2_get_with_columns() {
    let cmd = parse("get users fields id, email").unwrap();
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    assert_eq!(
        cmd.columns,
        vec![
            Expr::Named("id".to_string()),
            Expr::Named("email".to_string()),
        ]
    );
}

#[test]
fn test_v2_get_with_filter() {
    let cmd = parse("get users fields * where active = true").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].kind, CageKind::Filter);
    assert_eq!(cmd.cages[0].conditions.len(), 1);
    assert_eq!(cmd.cages[0].conditions[0].left, Expr::Named("active".to_string()));
    assert_eq!(cmd.cages[0].conditions[0].op, Operator::Eq);
    assert_eq!(cmd.cages[0].conditions[0].value, Value::Bool(true));
}

#[test]
fn test_v2_get_with_limit() {
    let cmd = parse("get users fields * limit 10").unwrap();
    let limit_cage = cmd.cages.iter().find(|c| matches!(c.kind, CageKind::Limit(_)));
    assert!(limit_cage.is_some());
    assert_eq!(limit_cage.unwrap().kind, CageKind::Limit(10));
}

#[test]
fn test_v2_get_with_offset() {
    let cmd = parse("get users fields * offset 20").unwrap();
    let offset_cage = cmd.cages.iter().find(|c| matches!(c.kind, CageKind::Offset(_)));
    assert!(offset_cage.is_some());
    assert_eq!(offset_cage.unwrap().kind, CageKind::Offset(20));
}

#[test]
fn test_v2_get_with_limit_offset() {
    let cmd = parse("get users fields * limit 10 offset 20").unwrap();
    let limit_cage = cmd.cages.iter().find(|c| matches!(c.kind, CageKind::Limit(_)));
    let offset_cage = cmd.cages.iter().find(|c| matches!(c.kind, CageKind::Offset(_)));
    assert_eq!(limit_cage.unwrap().kind, CageKind::Limit(10));
    assert_eq!(offset_cage.unwrap().kind, CageKind::Offset(20));
}

#[test]
fn test_v2_get_with_sort_desc() {
    let cmd = parse("get users fields * order by created_at desc").unwrap();
    let sort_cage = cmd.cages.iter().find(|c| matches!(c.kind, CageKind::Sort(_)));
    assert!(sort_cage.is_some());
    assert_eq!(sort_cage.unwrap().kind, CageKind::Sort(SortOrder::Desc));
    assert_eq!(sort_cage.unwrap().conditions[0].left, Expr::Named("created_at".to_string()));
}

#[test]
fn test_v2_get_with_sort_asc() {
    let cmd = parse("get users fields * order by id asc").unwrap();
    let sort_cage = cmd.cages.iter().find(|c| matches!(c.kind, CageKind::Sort(_)));
    assert!(sort_cage.is_some());
    assert_eq!(sort_cage.unwrap().kind, CageKind::Sort(SortOrder::Asc));
    assert_eq!(sort_cage.unwrap().conditions[0].left, Expr::Named("id".to_string()));
}

#[test]
fn test_v2_get_with_sort_default_asc() {
    let cmd = parse("get users fields * order by name").unwrap();
    let sort_cage = cmd.cages.iter().find(|c| matches!(c.kind, CageKind::Sort(_)));
    assert!(sort_cage.is_some());
    // Default is ASC
    assert_eq!(sort_cage.unwrap().kind, CageKind::Sort(SortOrder::Asc));
}

#[test]
fn test_v2_fuzzy_match() {
    let cmd = parse("get users fields id where name ~ \"john\"").unwrap();
    assert_eq!(cmd.cages[0].conditions[0].op, Operator::Fuzzy);
    assert_eq!(cmd.cages[0].conditions[0].value, Value::String("john".to_string()));
}

#[test]
fn test_v2_param_in_filter() {
    let cmd = parse("get users fields id where email = $1").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].conditions[0].value, Value::Param(1));
}

#[test]
fn test_v2_multiple_conditions() {
    let cmd = parse("get users fields * where active = true and role = \"admin\"").unwrap();
    assert_eq!(cmd.cages[0].conditions.len(), 2);
    assert_eq!(cmd.cages[0].conditions[0].left, Expr::Named("active".to_string()));
    assert_eq!(cmd.cages[0].conditions[1].left, Expr::Named("role".to_string()));
}

#[test]
fn test_v2_full_query() {
    let cmd = parse("get users fields id, name, email where active = true order by created_at desc limit 10").unwrap();
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.columns.len(), 3);
    assert!(!cmd.cages.is_empty());
}
