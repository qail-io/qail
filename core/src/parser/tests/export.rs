use crate::ast::*;
use crate::parser::parse;

#[test]
fn test_v2_simple_export() {
    let cmd = parse("export users").unwrap();
    assert_eq!(cmd.action, Action::Export);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.columns, vec![Expr::Star]);
}

#[test]
fn test_v2_export_with_columns_filter_and_limit() {
    let cmd = parse("export users fields id, email where active = true limit 100").unwrap();

    assert_eq!(cmd.action, Action::Export);
    assert_eq!(cmd.table, "users");
    assert_eq!(
        cmd.columns,
        vec![
            Expr::Named("id".to_string()),
            Expr::Named("email".to_string()),
        ]
    );

    let filter_cage = cmd
        .cages
        .iter()
        .find(|c| matches!(c.kind, CageKind::Filter))
        .expect("filter cage must exist");
    assert_eq!(filter_cage.conditions.len(), 1);
    assert_eq!(filter_cage.conditions[0].left, Expr::Named("active".into()));
    assert_eq!(filter_cage.conditions[0].op, Operator::Eq);
    assert_eq!(filter_cage.conditions[0].value, Value::Bool(true));

    let limit_cage = cmd
        .cages
        .iter()
        .find(|c| matches!(c.kind, CageKind::Limit(_)))
        .expect("limit cage must exist");
    assert_eq!(limit_cage.kind, CageKind::Limit(100));
}
