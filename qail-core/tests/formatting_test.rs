use qail_core::parse;
use qail_core::ast::*;

#[test]
#[ignore] // Requires V2 parser
fn test_comments_and_whitespace() {
    let query = r#"
        get::users -- The users table
        :'_       -- Get everything (all columns)
        // Just active ones
        [active=true]
        
        -- Sort by creation
        [^!created_at]
    "#;
    
    let cmd = parse(query).expect("Failed to parse query with comments");
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.columns, vec![Expr::Star]);
    assert_eq!(cmd.cages.len(), 2);
}

#[test]
#[ignore] // Requires V2 parser
fn test_inline_comments_in_cages() {
    let query = r#"
        get::users:'_
        [
            active=true -- status check
            &
            role='admin' // role check
        ]
    "#;

    let cmd = parse(query).expect("Failed to parse inline comments in cages");
    assert_eq!(cmd.cages.len(), 1);
    let cage = &cmd.cages[0];
    assert_eq!(cage.conditions.len(), 2);
    assert_eq!(cage.conditions[0].left, Expr::Named("active".to_string()));
    assert_eq!(cage.conditions[1].left, Expr::Named("role".to_string()));
}

#[test]
#[ignore] // Requires V2 parser
fn test_tabbed_formatting() {
    let query = "get::users\t\t-- tabbed action\n\t:\n\t'id\n\t'email\n\t[id=$1]";
    let cmd = parse(query).expect("Failed to parse tabbed query");
    assert_eq!(cmd.columns.len(), 2);
}
