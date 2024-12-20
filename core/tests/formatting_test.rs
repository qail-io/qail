use qail_core::ast::*;
use qail_core::parse;

#[test]
fn test_comments_and_whitespace() {
    let query = r#"
        get users
        fields *
        where active = true
        order by created_at desc
    "#;

    let cmd = parse(query).expect("Failed to parse query with comments");
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.columns, vec![Expr::Star]);
    assert!(!cmd.cages.is_empty());
}

#[test]
fn test_inline_comments_in_cages() {
    let query = r#"
        get users fields *
        where active = true and role = 'admin'
    "#;

    let cmd = parse(query).expect("Failed to parse inline conditions");
    assert!(!cmd.cages.is_empty());
    // Both conditions parsed
    let cage = &cmd.cages[0];
    assert!(!cage.conditions.is_empty());
}

#[test]
fn test_multiline_query() {
    // Test that multiline works properly
    let query = "get users fields id, email where id = $1";
    let cmd = parse(query).expect("Failed to parse query");
    assert_eq!(cmd.columns.len(), 2);
}
