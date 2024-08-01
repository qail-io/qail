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
//!   add::users : email, name [ :email, :name ]
//! ```

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{multispace0, multispace1, char, not_line_ending},
    combinator::map,
    multi::{many0, separated_list0},
    Parser,
    IResult,
};
use serde::{Deserialize, Serialize};

/// Collection of named queries from a queries.qail file
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryFile {
    pub queries: Vec<QueryDef>,
}

/// A named query definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDef {
    /// Query name (function name)
    pub name: String,
    /// Parameters with types
    pub params: Vec<QueryParam>,
    /// Return type (None for execute statements)
    pub return_type: Option<ReturnType>,
    /// The QAIL query body
    pub body: String,
    /// Is this an execute (mutation) vs query (read)
    pub is_execute: bool,
}

/// Query parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryParam {
    pub name: String,
    pub typ: String,
}

/// Return type for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReturnType {
    /// Single result: -> User
    Single(String),
    /// Multiple results: -> Vec<User>
    Vec(String),
    /// Optional result: -> Option<User>
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
        self.queries.iter().find(|q| q.name.eq_ignore_ascii_case(name))
    }
    
    /// Export to JSON
    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| format!("JSON serialization failed: {}", e))
    }
    
    /// Import from JSON
    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json)
            .map_err(|e| format!("JSON deserialization failed: {}", e))
    }
}

// =============================================================================
// Parsing Combinators
// =============================================================================

/// Parse identifier
fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_').parse(input)
}

/// Skip whitespace and comments
fn ws_and_comments(input: &str) -> IResult<&str, ()> {
    let (input, _) = many0(alt((
        map(multispace1, |_| ()),
        map((tag("--"), not_line_ending), |_| ()),
    ))).parse(input)?;
    Ok((input, ()))
}

/// Parse a single parameter: name: Type
fn parse_param(input: &str) -> IResult<&str, QueryParam> {
    let (input, _) = multispace0(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(':').parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, typ) = identifier(input)?;
    
    Ok((input, QueryParam {
        name: name.to_string(),
        typ: typ.to_string(),
    }))
}

/// Parse parameter list: (param1: Type, param2: Type)
fn parse_params(input: &str) -> IResult<&str, Vec<QueryParam>> {
    let (input, _) = char('(').parse(input)?;
    let (input, params) = separated_list0(
        char(','),
        parse_param,
    ).parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')').parse(input)?;
    
    Ok((input, params))
}

/// Parse return type: -> Type, -> Vec<Type>, -> Option<Type>
fn parse_return_type(input: &str) -> IResult<&str, ReturnType> {
    let (input, _) = multispace0(input)?;
    let (input, _) = tag("->").parse(input)?;
    let (input, _) = multispace0(input)?;
    
    // Check for Vec<T> or Option<T>
    if let Ok((input, _)) = tag::<_, _, nom::error::Error<&str>>("Vec<")(input) {
        let (input, inner) = take_while1(|c: char| c != '>').parse(input)?;
        let (input, _) = char('>').parse(input)?;
        return Ok((input, ReturnType::Vec(inner.to_string())));
    }
    
    if let Ok((input, _)) = tag::<_, _, nom::error::Error<&str>>("Option<")(input) {
        let (input, inner) = take_while1(|c: char| c != '>').parse(input)?;
        let (input, _) = char('>').parse(input)?;
        return Ok((input, ReturnType::Option(inner.to_string())));
    }
    
    // Single type
    let (input, typ) = identifier(input)?;
    Ok((input, ReturnType::Single(typ.to_string())))
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
    
    // Check for query or execute keyword
    let (input, is_execute) = alt((
        map(tag_no_case("query"), |_| false),
        map(tag_no_case("execute"), |_| true),
    )).parse(input)?;
    
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
    
    Ok((input, QueryDef {
        name: name.to_string(),
        params,
        return_type,
        body: body.to_string(),
        is_execute,
    }))
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
    fn test_parse_execute() {
        let input = r#"
            execute create_user(email: String, name: String):
              add::users : email, name [ :email, :name ]
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
              del::users where id = :id
        "#;
        
        let qf = QueryFile::parse(input).expect("parse failed");
        assert_eq!(qf.queries.len(), 3);
        
        assert_eq!(qf.queries[0].name, "find_user");
        assert_eq!(qf.queries[1].name, "list_users");
        assert_eq!(qf.queries[2].name, "delete_user");
    }
}
