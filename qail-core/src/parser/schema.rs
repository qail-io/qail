//! Schema file parser for `.qail` format.
//!
//! Parses schema definitions like:
//! ```text
//! table users (
//!   id uuid primary_key,
//!   email text not null,
//!   name text,
//!   created_at timestamp
//! )
//! ```

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{multispace1, char, not_line_ending},
    combinator::{opt, map},
    multi::{separated_list0, many0},
    sequence::{preceded, tuple},
    IResult,
};
use serde::{Deserialize, Serialize};

/// Schema containing all table definitions
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Schema {
    pub tables: Vec<TableDef>,
}

/// Table definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

/// Column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type", alias = "typ")]
    pub typ: String,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub references: Option<String>,
    #[serde(default)]
    pub default_value: Option<String>,
}

impl Default for ColumnDef {
    fn default() -> Self {
        Self {
            name: String::new(),
            typ: String::new(),
            nullable: true,
            primary_key: false,
            unique: false,
            references: None,
            default_value: None,
        }
    }
}

impl Schema {
    /// Parse a schema from `.qail` format string
    pub fn parse(input: &str) -> Result<Self, String> {
        match parse_schema(input) {
            Ok(("", schema)) => Ok(schema),
            Ok((remaining, _)) => Err(format!("Unexpected content: '{}'", remaining.trim())),
            Err(e) => Err(format!("Parse error: {:?}", e)),
        }
    }

    /// Find a table by name
    pub fn find_table(&self, name: &str) -> Option<&TableDef> {
        self.tables.iter().find(|t| t.name.eq_ignore_ascii_case(name))
    }
}

impl TableDef {
    /// Find a column by name
    pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
}

// =============================================================================
// Parsing Combinators
// =============================================================================

/// Parse identifier (table/column name)
fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

/// Skip whitespace and comments
fn ws_and_comments(input: &str) -> IResult<&str, ()> {
    let (input, _) = many0(alt((
        map(multispace1, |_| ()),
        map(tuple((tag("--"), not_line_ending)), |_| ()),
    )))(input)?;
    Ok((input, ()))
}

/// Parse column type
fn column_type(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric())(input)
}

/// Parse constraint text until comma or closing paren (handling nested parens)
fn constraint_text(input: &str) -> IResult<&str, &str> {
    let mut paren_depth = 0;
    let mut end = 0;
    
    for (i, c) in input.char_indices() {
        match c {
            '(' => paren_depth += 1,
            ')' => {
                if paren_depth == 0 {
                    break; // End at column-level closing paren
                }
                paren_depth -= 1;
            }
            ',' if paren_depth == 0 => break,
            '\n' | '\r' if paren_depth == 0 => break,
            _ => {}
        }
        end = i + c.len_utf8();
    }
    
    if end == 0 {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::TakeWhile1)))
    } else {
        Ok((&input[end..], &input[..end]))
    }
}

/// Parse a single column definition
fn parse_column(input: &str) -> IResult<&str, ColumnDef> {
    let (input, _) = ws_and_comments(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, typ) = column_type(input)?;
    
    // Get remaining text until comma or paren for constraints
    let (input, constraint_str) = opt(preceded(multispace1, constraint_text))(input)?;
    
    // Parse constraints from the string
    let mut col = ColumnDef {
        name: name.to_string(),
        typ: typ.to_lowercase(),
        nullable: true,
        ..Default::default()
    };
    
    if let Some(constraints) = constraint_str {
        let lower = constraints.to_lowercase();
        
        if lower.contains("primary_key") || lower.contains("primary key") {
            col.primary_key = true;
            col.nullable = false;
        }
        if lower.contains("not_null") || lower.contains("not null") {
            col.nullable = false;
        }
        if lower.contains("unique") {
            col.unique = true;
        }
        
        // Parse references
        if let Some(idx) = lower.find("references ") {
            let rest = &constraints[idx + 11..];
            let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
            col.references = Some(rest[..end].to_string());
        }
        
        // Parse default
        if let Some(idx) = lower.find("default ") {
            let rest = &constraints[idx + 8..];
            let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
            col.default_value = Some(rest[..end].to_string());
        }
    }
    
    Ok((input, col))
}

/// Parse column list: (col1 type, col2 type, ...)
fn parse_column_list(input: &str) -> IResult<&str, Vec<ColumnDef>> {
    let (input, _) = ws_and_comments(input)?;
    let (input, _) = char('(')(input)?;
    let (input, columns) = separated_list0(
        char(','),
        parse_column,
    )(input)?;
    let (input, _) = ws_and_comments(input)?;
    let (input, _) = char(')')(input)?;
    
    Ok((input, columns))
}

/// Parse a table definition
fn parse_table(input: &str) -> IResult<&str, TableDef> {
    let (input, _) = ws_and_comments(input)?;
    let (input, _) = tag_no_case("table")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, name) = identifier(input)?;
    let (input, columns) = parse_column_list(input)?;
    
    Ok((input, TableDef {
        name: name.to_string(),
        columns,
    }))
}

/// Parse complete schema file
fn parse_schema(input: &str) -> IResult<&str, Schema> {
    let (input, _) = ws_and_comments(input)?;
    let (input, tables) = many0(parse_table)(input)?;
    let (input, _) = ws_and_comments(input)?;
    
    Ok((input, Schema { tables }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_table() {
        let input = r#"
            table users (
                id uuid primary_key,
                email text not null,
                name text
            )
        "#;
        
        let schema = Schema::parse(input).expect("parse failed");
        assert_eq!(schema.tables.len(), 1);
        
        let users = &schema.tables[0];
        assert_eq!(users.name, "users");
        assert_eq!(users.columns.len(), 3);
        
        let id = &users.columns[0];
        assert_eq!(id.name, "id");
        assert_eq!(id.typ, "uuid");
        assert!(id.primary_key);
        assert!(!id.nullable);
        
        let email = &users.columns[1];
        assert_eq!(email.name, "email");
        assert!(!email.nullable);
        
        let name = &users.columns[2];
        assert!(name.nullable);
    }

    #[test]
    fn test_parse_multiple_tables() {
        let input = r#"
            -- Users table
            table users (
                id uuid primary_key,
                email text not null unique
            )
            
            -- Orders table
            table orders (
                id uuid primary_key,
                user_id uuid references users(id),
                total i64 not null default 0
            )
        "#;
        
        let schema = Schema::parse(input).expect("parse failed");
        assert_eq!(schema.tables.len(), 2);
        
        let orders = schema.find_table("orders").expect("orders not found");
        let user_id = orders.find_column("user_id").expect("user_id not found");
        assert_eq!(user_id.references, Some("users(id)".to_string()));
        
        let total = orders.find_column("total").expect("total not found");
        assert_eq!(total.default_value, Some("0".to_string()));
    }

    #[test]
    fn test_parse_comments() {
        let input = r#"
            -- This is a comment
            table foo (
                bar text
            )
        "#;
        
        let schema = Schema::parse(input).expect("parse failed");
        assert_eq!(schema.tables.len(), 1);
    }
}
