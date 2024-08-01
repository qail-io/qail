use crate::parser::parse;
use crate::ast::*;

#[test]
fn test_nested_identifiers() {
    // In Context - using v2 syntax
    let cmd = parse("get users fields * where metadata.theme = \"dark\"").unwrap();
    if let CageKind::Filter = cmd.cages[0].kind {
        assert_eq!(cmd.cages[0].conditions[0].left, Expr::Named("metadata.theme".to_string()));
        match &cmd.cages[0].conditions[0].value {
            Value::String(s) => assert_eq!(s, "dark"),
            _ => panic!("Expected string value"),
        }
    } else {
        panic!("Expected filter cage");
    }
}
