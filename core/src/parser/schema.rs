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
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, multispace1, not_line_ending},
    combinator::{map, opt},
    multi::{many0, separated_list0},
    sequence::preceded,
};
use serde::{Deserialize, Serialize};

/// Schema containing all table definitions
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Schema {
    /// Schema format version (extracted from `-- qail: version=N` directive)
    #[serde(default)]
    pub version: Option<u32>,
    pub tables: Vec<TableDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type", alias = "typ")]
    pub typ: String,
    /// Type is an array (e.g., text[], uuid[])
    #[serde(default)]
    pub is_array: bool,
    /// Type parameters (e.g., varchar(255) -> Some(vec!["255"]), decimal(10,2) -> Some(vec!["10", "2"]))
    #[serde(default)]
    pub type_params: Option<Vec<String>>,
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
    /// Check constraint expression
    #[serde(default)]
    pub check: Option<String>,
    /// Is this a serial/auto-increment type
    #[serde(default)]
    pub is_serial: bool,
}

impl Default for ColumnDef {
    fn default() -> Self {
        Self {
            name: String::new(),
            typ: String::new(),
            is_array: false,
            type_params: None,
            nullable: true,
            primary_key: false,
            unique: false,
            references: None,
            default_value: None,
            check: None,
            is_serial: false,
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
        self.tables
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
    }

    /// Export schema to JSON string (for qail-macros compatibility)
    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string_pretty(self).map_err(|e| format!("JSON serialization failed: {}", e))
    }

    /// Import schema from JSON string
    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|e| format!("JSON deserialization failed: {}", e))
    }

    /// Load schema from a .qail file
    pub fn from_file(path: &std::path::Path) -> Result<Self, String> {
        let content =
            std::fs::read_to_string(path).map_err(|e| format!("Failed to read file: {}", e))?;

        if content.trim().starts_with('{') {
            Self::from_json(&content)
        } else {
            Self::parse(&content)
        }
    }
}

impl TableDef {
    /// Find a column by name
    pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Generate CREATE TABLE IF NOT EXISTS SQL (AST-native DDL).
    pub fn to_ddl(&self) -> String {
        let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (\n", self.name);

        let mut col_defs = Vec::new();
        for col in &self.columns {
            let mut line = format!("    {}", col.name);

            // Type with params
            let mut typ = col.typ.to_uppercase();
            if let Some(params) = &col.type_params {
                typ = format!("{}({})", typ, params.join(", "));
            }
            if col.is_array {
                typ.push_str("[]");
            }
            line.push_str(&format!(" {}", typ));

            // Constraints
            if col.primary_key {
                line.push_str(" PRIMARY KEY");
            }
            if !col.nullable && !col.primary_key && !col.is_serial {
                line.push_str(" NOT NULL");
            }
            if col.unique && !col.primary_key {
                line.push_str(" UNIQUE");
            }
            if let Some(ref default) = col.default_value {
                line.push_str(&format!(" DEFAULT {}", default));
            }
            if let Some(ref refs) = col.references {
                line.push_str(&format!(" REFERENCES {}", refs));
            }
            if let Some(ref check) = col.check {
                line.push_str(&format!(" CHECK({})", check));
            }

            col_defs.push(line);
        }

        sql.push_str(&col_defs.join(",\n"));
        sql.push_str("\n)");
        sql
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
        map((tag("--"), not_line_ending), |_| ()),
    )))
    .parse(input)?;
    Ok((input, ()))
}

struct TypeInfo {
    name: String,
    params: Option<Vec<String>>,
    is_array: bool,
    is_serial: bool,
}

/// Parse column type with optional params and array suffix
/// Handles: varchar(255), decimal(10,2), text[], serial, bigserial
fn parse_type_info(input: &str) -> IResult<&str, TypeInfo> {
    let (input, type_name) = take_while1(|c: char| c.is_alphanumeric()).parse(input)?;

    let (input, params) = if input.starts_with('(') {
        let paren_start = 1;
        let mut paren_end = paren_start;
        for (i, c) in input[paren_start..].char_indices() {
            if c == ')' {
                paren_end = paren_start + i;
                break;
            }
        }
        let param_str = &input[paren_start..paren_end];
        let params: Vec<String> = param_str.split(',').map(|s| s.trim().to_string()).collect();
        (&input[paren_end + 1..], Some(params))
    } else {
        (input, None)
    };

    let (input, is_array) = if let Some(stripped) = input.strip_prefix("[]") {
        (stripped, true)
    } else {
        (input, false)
    };

    let lower = type_name.to_lowercase();
    let is_serial = lower == "serial" || lower == "bigserial" || lower == "smallserial";

    Ok((
        input,
        TypeInfo {
            name: lower,
            params,
            is_array,
            is_serial,
        },
    ))
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
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TakeWhile1,
        )))
    } else {
        Ok((&input[end..], &input[..end]))
    }
}

/// Parse a single column definition
fn parse_column(input: &str) -> IResult<&str, ColumnDef> {
    let (input, _) = ws_and_comments(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, type_info) = parse_type_info(input)?;

    let (input, constraint_str) = opt(preceded(multispace1, constraint_text)).parse(input)?;

    let mut col = ColumnDef {
        name: name.to_string(),
        typ: type_info.name,
        is_array: type_info.is_array,
        type_params: type_info.params,
        is_serial: type_info.is_serial,
        nullable: !type_info.is_serial, // Serial types are implicitly not null
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

        if let Some(idx) = lower.find("references ") {
            let rest = &constraints[idx + 11..];
            // Find end (space or end of string), but handle nested parens
            let mut paren_depth = 0;
            let mut end = rest.len();
            for (i, c) in rest.char_indices() {
                match c {
                    '(' => paren_depth += 1,
                    ')' => {
                        if paren_depth == 0 {
                            end = i;
                            break;
                        }
                        paren_depth -= 1;
                    }
                    c if c.is_whitespace() && paren_depth == 0 => {
                        end = i;
                        break;
                    }
                    _ => {}
                }
            }
            col.references = Some(rest[..end].to_string());
        }

        if let Some(idx) = lower.find("default ") {
            let rest = &constraints[idx + 8..];
            let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
            col.default_value = Some(rest[..end].to_string());
        }

        if let Some(idx) = lower.find("check(") {
            let rest = &constraints[idx + 6..];
            // Find matching closing paren
            let mut depth = 1;
            let mut end = rest.len();
            for (i, c) in rest.char_indices() {
                match c {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = i;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            col.check = Some(rest[..end].to_string());
        }
    }

    Ok((input, col))
}

/// Parse column list: (col1 type, col2 type, ...)
fn parse_column_list(input: &str) -> IResult<&str, Vec<ColumnDef>> {
    let (input, _) = ws_and_comments(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, columns) = separated_list0(char(','), parse_column).parse(input)?;
    let (input, _) = ws_and_comments(input)?;
    let (input, _) = char(')').parse(input)?;

    Ok((input, columns))
}

/// Parse a table definition
fn parse_table(input: &str) -> IResult<&str, TableDef> {
    let (input, _) = ws_and_comments(input)?;
    let (input, _) = tag_no_case("table").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, name) = identifier(input)?;
    let (input, columns) = parse_column_list(input)?;

    Ok((
        input,
        TableDef {
            name: name.to_string(),
            columns,
        },
    ))
}

/// Parse complete schema file
fn parse_schema(input: &str) -> IResult<&str, Schema> {
    // Extract version directive before parsing
    let version = extract_version_directive(input);
    
    let (input, _) = ws_and_comments(input)?;
    let (input, tables) = many0(parse_table).parse(input)?;
    let (input, _) = ws_and_comments(input)?;

    Ok((input, Schema { version, tables }))
}

/// Extract version from `-- qail: version=N` directive
fn extract_version_directive(input: &str) -> Option<u32> {
    for line in input.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("-- qail:") {
            let rest = rest.trim();
            if let Some(version_str) = rest.strip_prefix("version=") {
                return version_str.trim().parse().ok();
            }
        }
    }
    None
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

    #[test]
    fn test_array_types() {
        let input = r#"
            table products (
                id uuid primary_key,
                tags text[],
                prices decimal[]
            )
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        let products = &schema.tables[0];

        let tags = products.find_column("tags").expect("tags not found");
        assert_eq!(tags.typ, "text");
        assert!(tags.is_array);

        let prices = products.find_column("prices").expect("prices not found");
        assert!(prices.is_array);
    }

    #[test]
    fn test_type_params() {
        let input = r#"
            table items (
                id serial primary_key,
                name varchar(255) not null,
                price decimal(10,2),
                code varchar(50) unique
            )
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        let items = &schema.tables[0];

        let id = items.find_column("id").expect("id not found");
        assert!(id.is_serial);
        assert!(!id.nullable); // Serial is implicitly not null

        let name = items.find_column("name").expect("name not found");
        assert_eq!(name.typ, "varchar");
        assert_eq!(name.type_params, Some(vec!["255".to_string()]));

        let price = items.find_column("price").expect("price not found");
        assert_eq!(
            price.type_params,
            Some(vec!["10".to_string(), "2".to_string()])
        );

        let code = items.find_column("code").expect("code not found");
        assert!(code.unique);
    }

    #[test]
    fn test_check_constraint() {
        let input = r#"
            table employees (
                id uuid primary_key,
                age i32 check(age >= 18),
                salary decimal check(salary > 0)
            )
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        let employees = &schema.tables[0];

        let age = employees.find_column("age").expect("age not found");
        assert_eq!(age.check, Some("age >= 18".to_string()));

        let salary = employees.find_column("salary").expect("salary not found");
        assert_eq!(salary.check, Some("salary > 0".to_string()));
    }

    #[test]
    fn test_version_directive() {
        let input = r#"
            -- qail: version=1
            table users (
                id uuid primary_key
            )
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        assert_eq!(schema.version, Some(1));
        assert_eq!(schema.tables.len(), 1);

        // Without version directive
        let input_no_version = r#"
            table items (
                id uuid primary_key
            )
        "#;
        let schema2 = Schema::parse(input_no_version).expect("parse failed");
        assert_eq!(schema2.version, None);
    }
}
