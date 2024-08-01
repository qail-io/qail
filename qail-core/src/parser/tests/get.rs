use crate::parser::parse;
use crate::ast::*;

#[test]
fn test_v2_simple_get() {
    let cmd = parse("get::users:'_").unwrap();
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.columns, vec![Column::Star]);
}

#[test]
fn test_v2_get_with_columns() {
    let cmd = parse("get::users:'id'email").unwrap();
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    assert_eq!(
        cmd.columns,
        vec![
            Column::Named("id".to_string()),
            Column::Named("email".to_string()),
        ]
    );
}

#[test]
fn test_v2_get_with_filter() {
    let cmd = parse("get::users:'_ [ 'active == true ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].kind, CageKind::Filter);
    assert_eq!(cmd.cages[0].conditions.len(), 1);
    assert_eq!(cmd.cages[0].conditions[0].column, "active");
    assert_eq!(cmd.cages[0].conditions[0].op, Operator::Eq);
    assert_eq!(cmd.cages[0].conditions[0].value, Value::Bool(true));
}

#[test]
fn test_v2_get_with_range_limit() {
    let cmd = parse("get::users:'_ [ 0..10 ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].kind, CageKind::Limit(10));
}

#[test]
fn test_v2_get_with_range_offset() {
    let cmd = parse("get::users:'_ [ 20..30 ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    // Range 20..30 = LIMIT 10 with offset 20
    assert_eq!(cmd.cages[0].kind, CageKind::Limit(10));
    assert_eq!(cmd.cages[0].conditions[0].column, "__offset__");
    assert_eq!(cmd.cages[0].conditions[0].value, Value::Int(20));
}

#[test]
fn test_v2_get_with_sort_desc() {
    let cmd = parse("get::users:'_ [ -created_at ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].kind, CageKind::Sort(SortOrder::Desc));
    assert_eq!(cmd.cages[0].conditions[0].column, "created_at");
}

#[test]
fn test_v2_get_with_sort_asc() {
    let cmd = parse("get::users:'_ [ +id ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].kind, CageKind::Sort(SortOrder::Asc));
    assert_eq!(cmd.cages[0].conditions[0].column, "id");
}

#[test]
fn test_v2_fuzzy_match() {
    let cmd = parse("get::users:'id [ 'name ~ \"john\" ]").unwrap();
    assert_eq!(cmd.cages[0].conditions[0].op, Operator::Fuzzy);
    assert_eq!(cmd.cages[0].conditions[0].value, Value::String("john".to_string()));
}

#[test]
fn test_v2_param_in_filter() {
    let cmd = parse("get::users:'id [ 'email == $1 ]").unwrap();
    assert_eq!(cmd.cages.len(), 1);
    assert_eq!(cmd.cages[0].conditions[0].value, Value::Param(1));
}
