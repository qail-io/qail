use crate::parser::parse;
use crate::ast::*;

#[test]
fn test_nested_identifiers() {
    use crate::parser::tokens::parse_identifier;
    
    // 1. Basic Nested
    let (_, id) = parse_identifier("metadata.theme").unwrap();
    assert_eq!(id, "metadata.theme");

    // 2. In Context (Cage)
    let cmd = parse("get::users [metadata.theme='dark']").unwrap();
    if let CageKind::Filter = cmd.cages[0].kind {
        assert_eq!(cmd.cages[0].conditions[0].column, "metadata.theme");
        match &cmd.cages[0].conditions[0].value {
            Value::String(s) => assert_eq!(s, "dark"),
            _ => panic!("Expected string value"),
        }
    } else {
        panic!("Expected filter cage");
    }
}

#[test]
fn test_lsp_snippet_identifier() {
    use crate::parser::tokens::parse_identifier;
    
    // 1. Check identifier parsing directly
    let (_, id) = parse_identifier("${1:table}").unwrap();
    assert_eq!(id, "${1:table}");

    // 2. Check within a command
    let cmd = parse("get::${1:table}:'${2:column}").unwrap();
    assert_eq!(cmd.table, "${1:table}");
    if let Column::Named(n) = &cmd.columns[0] {
        assert_eq!(n, "${2:column}");
    } else {
        panic!("Expected Column::Named");
    }
}

#[test]
fn test_debug_parse_arg() {
    use crate::parser::columns::parse_arg_value;
    // Input is "a,b"
    // parse_arg_value should return "a" and leave ",b"
    let input = "a,b";
    let (rest, val) = parse_arg_value(input).expect("Should parse");
   
    assert_eq!(val, "a");
    assert_eq!(rest, ",b");
}

#[test]
fn test_debug_parse_func() {
    use crate::parser::columns::parse_function_column;
    let input = "coalesce(a,b)";
    let (rest, _col) = parse_function_column(input).expect("Should parse func");
    assert_eq!(rest, "");
}
