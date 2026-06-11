//! Query file parser for `.qail` format.
//!
//! Parses named query templates like:
//! ```text
//! query find_user_by_email(email: String) -> User:
//!   get users where email = :email
//!
//! query list_orders(user_id: Uuid) -> Vec<Order>:
//!   get orders where user_id = :user_id order by created_at desc
//!
//! execute create_user(email: String, name: String):
//!   add users fields email, name values :email, :name
//! ```

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, multispace0, multispace1, not_line_ending},
    combinator::map,
    multi::{many0, separated_list0},
};

/// Collection of named queries from a queries.qail file
#[derive(Debug, Clone, Default)]
pub struct QueryFile {
    /// Named query definitions.
    pub queries: Vec<QueryDef>,
}

/// A named query definition
#[derive(Debug, Clone)]
pub struct QueryDef {
    /// Query name (function name)
    pub name: String,
    /// Parameters with types
    pub params: Vec<QueryParam>,
    /// Return type (None for execute-only queries).
    pub return_type: Option<ReturnType>,
    /// The QAIL query body.
    pub body: String,
    /// Whether this is an `execute` (write) rather than `query` (read).
    pub is_execute: bool,
}

/// Query parameter
#[derive(Debug, Clone)]
pub struct QueryParam {
    /// Parameter name.
    pub name: String,
    /// Parameter type (e.g., "Uuid", "String").
    pub typ: String,
}

/// Return type for queries
#[derive(Debug, Clone)]
pub enum ReturnType {
    /// Single result: -> User
    Single(String),
    /// Multiple results: -> `Vec<User>`
    Vec(String),
    /// Optional result: -> `Option<User>`
    Option(String),
}

impl QueryFile {
    /// Parse a query file from `.qail` format string
    pub fn parse(input: &str) -> Result<Self, String> {
        match parse_query_file(input) {
            Ok(("", qf)) => Ok(qf),
            Ok((remaining, _)) => Err(format!("Unexpected content: '{}'", remaining.trim())),
            Err(e) => Err(format!("Parse error: {:?}", e)),
        }
    }

    /// Find a query by name
    pub fn find_query(&self, name: &str) -> Option<&QueryDef> {
        self.queries
            .iter()
            .find(|q| q.name.eq_ignore_ascii_case(name))
    }
}

// =============================================================================
// Parsing Combinators
// =============================================================================

/// Parse identifier
fn identifier(input: &str) -> IResult<&str, &str> {
    let (remaining, ident) =
        take_while1(|c: char| c.is_ascii_alphanumeric() || c == '_').parse(input)?;
    if ident
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
    {
        Ok((remaining, ident))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TakeWhile1,
        )))
    }
}

fn rust_type_expr(input: &str) -> IResult<&str, &str> {
    let mut angle_depth = 0usize;
    let mut end = None;

    for (idx, ch) in input.char_indices() {
        match ch {
            '<' => {
                angle_depth += 1;
            }
            '>' => {
                let Some(next) = angle_depth.checked_sub(1) else {
                    return Err(nom::Err::Error(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::TakeWhile1,
                    )));
                };
                angle_depth = next;
            }
            ',' | ')' if angle_depth == 0 => {
                end = Some(idx);
                break;
            }
            ':' if angle_depth == 0
                && !input[..idx].ends_with(':')
                && !input[idx + ch.len_utf8()..].starts_with(':') =>
            {
                end = Some(idx);
                break;
            }
            c if c.is_whitespace() && angle_depth == 0 => {
                end = Some(idx);
                break;
            }
            c if c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '[' | ']' | '.') => {}
            _ => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::TakeWhile1,
                )));
            }
        }
    }

    let end = end.unwrap_or(input.len());
    if end == 0 || angle_depth != 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TakeWhile1,
        )));
    }
    let typ = &input[..end];
    if !validate_rust_type_generics(typ) {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TakeWhile1,
        )));
    }
    Ok((&input[end..], typ))
}

fn validate_rust_type_generics(typ: &str) -> bool {
    let mut stack: Vec<Option<char>> = Vec::new();

    for ch in typ.chars() {
        match ch {
            '<' => stack.push(None),
            '>' => {
                let Some(last_arg) = stack.pop() else {
                    return false;
                };
                if !matches!(last_arg, Some('t')) {
                    return false;
                }
                if let Some(parent) = stack.last_mut() {
                    *parent = Some('t');
                }
            }
            ',' if !stack.is_empty() => {
                let Some(current) = stack.last_mut() else {
                    return false;
                };
                if !matches!(current, Some('t')) {
                    return false;
                }
                *current = Some(',');
            }
            c if c.is_whitespace() => {}
            _ if !stack.is_empty() => {
                if let Some(current) = stack.last_mut() {
                    *current = Some('t');
                }
            }
            _ => {}
        }
    }

    stack.is_empty()
}

/// Skip whitespace and comments
fn ws_and_comments(input: &str) -> IResult<&str, ()> {
    let (input, _) = many0(alt((
        map(multispace1, |_| ()),
        map((tag("--"), not_line_ending), |_| ()),
    )))
    .parse(input)?;
    Ok((input, ()))
}

/// Parse a single parameter: name: Type
fn parse_param(input: &str) -> IResult<&str, QueryParam> {
    let (input, _) = multispace0(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(':').parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, typ) = rust_type_expr(input)?;

    Ok((
        input,
        QueryParam {
            name: name.to_string(),
            typ: typ.to_string(),
        },
    ))
}

/// Parse parameter list: (param1: Type, param2: Type)
fn parse_params(input: &str) -> IResult<&str, Vec<QueryParam>> {
    let (input, _) = char('(').parse(input)?;
    let (input, params) = separated_list0(char(','), parse_param).parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')').parse(input)?;

    Ok((input, params))
}

/// Parse return type: -> Type, -> Vec<Type>, -> Option<Type>
fn parse_return_type(input: &str) -> IResult<&str, ReturnType> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("->").parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, typ) = rust_type_expr(input)?;
    if let Some(inner) = strip_outer_generic(typ, "Vec") {
        return Ok((input, ReturnType::Vec(inner.to_string())));
    }
    if let Some(inner) = strip_outer_generic(typ, "Option") {
        return Ok((input, ReturnType::Option(inner.to_string())));
    }
    Ok((input, ReturnType::Single(typ.to_string())))
}

fn strip_outer_generic<'a>(typ: &'a str, outer: &str) -> Option<&'a str> {
    let inner = typ
        .strip_prefix(outer)?
        .strip_prefix('<')?
        .strip_suffix('>')?;
    (!inner.is_empty()).then_some(inner)
}

/// Parse query body (everything after : until next query/execute or EOF)
fn parse_body(input: &str) -> IResult<&str, &str> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char(':').parse(input)?;
    let (input, _) = multispace0(input)?;

    // Find end: next "query" or "execute" keyword at line start (after whitespace), or EOF
    let mut end = input.len();

    for (i, _) in input.char_indices() {
        if i == 0 || input.as_bytes().get(i.saturating_sub(1)) == Some(&b'\n') {
            // At start of line, skip whitespace and check for keyword
            let line_rest = &input[i..];
            let trimmed = line_rest.trim_start();
            if trimmed.starts_with("query ") || trimmed.starts_with("execute ") {
                // Find where the trimmed content starts
                let ws_len = line_rest.len() - trimmed.len();
                end = i + ws_len;
                break;
            }
        }
    }

    let body = input[..end].trim();
    Ok((&input[end..], body))
}

/// Parse a single query definition
fn parse_query_def(input: &str) -> IResult<&str, QueryDef> {
    let (input, _) = ws_and_comments(input)?;

    let (input, is_execute) = alt((
        map(tag_no_case("query"), |_| false),
        map(tag_no_case("execute"), |_| true),
    ))
    .parse(input)?;

    let (input, _) = multispace1(input)?;
    let (input, name) = identifier(input)?;
    let (input, params) = parse_params(input)?;

    // Return type (optional for execute)
    let (input, return_type) = if is_execute {
        (input, None)
    } else {
        let (input, rt) = parse_return_type(input)?;
        (input, Some(rt))
    };

    let (input, body) = parse_body(input)?;

    Ok((
        input,
        QueryDef {
            name: name.to_string(),
            params,
            return_type,
            body: body.to_string(),
            is_execute,
        },
    ))
}

/// Parse complete query file
fn parse_query_file(input: &str) -> IResult<&str, QueryFile> {
    let (input, _) = ws_and_comments(input)?;
    let (input, queries) = many0(parse_query_def).parse(input)?;
    let (input, _) = ws_and_comments(input)?;

    Ok((input, QueryFile { queries }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_query() {
        let input = r#"
            query find_user(id: Uuid) -> User:
              get users where id = :id
        "#;

        let qf = QueryFile::parse(input).expect("parse failed");
        assert_eq!(qf.queries.len(), 1);

        let q = &qf.queries[0];
        assert_eq!(q.name, "find_user");
        assert!(!q.is_execute);
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[0].typ, "Uuid");
        assert!(matches!(q.return_type, Some(ReturnType::Single(ref t)) if t == "User"));
        assert!(q.body.contains("get users"));
    }

    #[test]
    fn test_parse_vec_return() {
        let input = r#"
            query list_orders(user_id: Uuid) -> Vec<Order>:
              get orders where user_id = :user_id order by created_at desc
        "#;

        let qf = QueryFile::parse(input).expect("parse failed");
        let q = &qf.queries[0];
        assert!(matches!(q.return_type, Some(ReturnType::Vec(ref t)) if t == "Order"));
    }

    #[test]
    fn test_parse_option_return() {
        let input = r#"
            query find_optional(email: String) -> Option<User>:
              get users where email = :email limit 1
        "#;

        let qf = QueryFile::parse(input).expect("parse failed");
        let q = &qf.queries[0];
        assert!(matches!(q.return_type, Some(ReturnType::Option(ref t)) if t == "User"));
    }

    #[test]
    fn test_parse_generic_param_and_nested_return_types() {
        let input = r#"
            query find_many(ids: std::vec::Vec<Uuid>, tags: Option<Vec<String>>) -> Option<Vec<User>>:
              get users where id in :ids
        "#;

        let qf = QueryFile::parse(input).expect("parse failed");
        let q = &qf.queries[0];
        assert_eq!(q.params[0].typ, "std::vec::Vec<Uuid>");
        assert_eq!(q.params[1].typ, "Option<Vec<String>>");
        assert!(matches!(q.return_type, Some(ReturnType::Option(ref t)) if t == "Vec<User>"));
    }

    #[test]
    fn test_parse_rejects_unbalanced_generic_param_type() {
        let input = r#"
            query broken(ids: Vec<Uuid) -> Vec<User>:
              get users where id in :ids
        "#;

        let err = QueryFile::parse(input).expect_err("unbalanced generic must fail");
        assert!(err.contains("Parse error") || err.contains("Unexpected content"));
    }

    #[test]
    fn test_parse_rejects_invalid_query_and_param_identifiers() {
        let invalid_query_name = r#"
            query 1find_user(id: Uuid) -> User:
              get users where id = :id
        "#;
        QueryFile::parse(invalid_query_name)
            .expect_err("query names must be valid Rust identifiers");

        let invalid_param_name = r#"
            query find_user(1id: Uuid) -> User:
              get users where id = :id
        "#;
        QueryFile::parse(invalid_param_name)
            .expect_err("parameter names must be valid Rust identifiers");
    }

    #[test]
    fn test_parse_rejects_empty_generic_type_arguments() {
        for input in [
            r#"
            query broken(ids: Vec<>) -> Vec<User>:
              get users where id in :ids
            "#,
            r#"
            query broken(id: Uuid) -> Option<>:
              get users where id = :id
            "#,
            r#"
            query broken(ids: Vec<,Uuid>) -> Vec<User>:
              get users where id in :ids
            "#,
            r#"
            query broken(ids: Vec<Uuid,>) -> Vec<User>:
              get users where id in :ids
            "#,
        ] {
            QueryFile::parse(input).expect_err("empty generic type arguments must fail");
        }
    }

    #[test]
    fn test_parse_execute() {
        let input = r#"
            execute create_user(email: String, name: String):
              add users fields email, name values :email, :name
        "#;

        let qf = QueryFile::parse(input).expect("parse failed");
        let q = &qf.queries[0];
        assert!(q.is_execute);
        assert!(q.return_type.is_none());
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn test_parse_multiple_queries() {
        let input = r#"
            -- User queries
            query find_user(id: Uuid) -> User:
              get users where id = :id
            
            query list_users() -> Vec<User>:
              get users order by created_at desc
            
            execute delete_user(id: Uuid):
              del users where id = :id
        "#;

        let qf = QueryFile::parse(input).expect("parse failed");
        assert_eq!(qf.queries.len(), 3);

        assert_eq!(qf.queries[0].name, "find_user");
        assert_eq!(qf.queries[1].name, "list_users");
        assert_eq!(qf.queries[2].name, "delete_user");
    }
}
