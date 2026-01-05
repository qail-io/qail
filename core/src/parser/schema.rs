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
//!
//! policy users_isolation on users
//!     for all
//!     using (operator_id = current_setting('app.operator_id')::uuid)
//! ```

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, multispace0 as nom_ws0, multispace1, not_line_ending},
    combinator::{map, opt},
    multi::{many0, separated_list0},
    sequence::preceded,
};
use serde::{Deserialize, Serialize};

use crate::ast::{Expr, BinaryOp, Value as AstValue};
use crate::migrate::policy::{RlsPolicy, PolicyTarget, PolicyPermissiveness};
use crate::transpiler::policy::{create_policy_sql, alter_table_sql};
use crate::migrate::alter::AlterTable;

/// Schema containing all table definitions
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Schema {
    /// Schema format version (extracted from `-- qail: version=N` directive)
    #[serde(default)]
    pub version: Option<u32>,
    /// Table definitions.
    pub tables: Vec<TableDef>,
    /// RLS policies declared in the schema
    #[serde(default)]
    pub policies: Vec<RlsPolicy>,
    /// Indexes declared in the schema
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
}

/// Index definition parsed from `index <name> on <table> (<columns>) [unique]`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    /// Index name.
    pub name: String,
    /// Table this index belongs to.
    pub table: String,
    /// Columns included in the index.
    pub columns: Vec<String>,
    /// Whether this is a UNIQUE index.
    #[serde(default)]
    pub unique: bool,
}

impl IndexDef {
    /// Generate `CREATE INDEX IF NOT EXISTS` SQL.
    pub fn to_sql(&self) -> String {
        let unique = if self.unique { " UNIQUE" } else { "" };
        format!(
            "CREATE{} INDEX IF NOT EXISTS {} ON {} ({})",
            unique,
            self.name,
            self.table,
            self.columns.join(", ")
        )
    }
}

/// Table definition parsed from a `.qail` schema file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    /// Table name.
    pub name: String,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
    /// Whether this table has RLS enabled.
    #[serde(default)]
    pub enable_rls: bool,
}

/// Column definition parsed from a `.qail` schema file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// SQL data type (lowercased).
    #[serde(rename = "type", alias = "typ")]
    pub typ: String,
    /// Type is an array (e.g., text[], uuid[]).
    #[serde(default)]
    pub is_array: bool,
    /// Type parameters (e.g., varchar(255) → Some(vec!["255"]), decimal(10,2) → Some(vec!["10", "2"])).
    #[serde(default)]
    pub type_params: Option<Vec<String>>,
    /// Whether the column accepts NULL.
    #[serde(default)]
    pub nullable: bool,
    /// Whether the column is a primary key.
    #[serde(default)]
    pub primary_key: bool,
    /// Whether the column has a UNIQUE constraint.
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    /// Foreign key reference (e.g. "users(id)").
    pub references: Option<String>,
    /// Default value expression.
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

    /// Generate complete SQL for this schema: tables + RLS + policies + indexes.
    pub fn to_sql(&self) -> String {
        let mut parts = Vec::new();

        for table in &self.tables {
            parts.push(table.to_ddl());

            if table.enable_rls {
                let alter = AlterTable::new(&table.name).enable_rls().force_rls();
                for stmt in alter_table_sql(&alter) {
                    parts.push(stmt);
                }
            }
        }

        for idx in &self.indexes {
            parts.push(idx.to_sql());
        }

        for policy in &self.policies {
            parts.push(create_policy_sql(policy));
        }

        parts.join(";\n\n") + ";"
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

/// Skip whitespace and comments (both `--` and `#` styles)
fn ws_and_comments(input: &str) -> IResult<&str, ()> {
    let (input, _) = many0(alt((
        map(multispace1, |_| ()),
        map((tag("--"), not_line_ending), |_| ()),
        map((tag("#"), not_line_ending), |_| ()),
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

    // Optional enable_rls annotation after closing paren
    let (input, _) = ws_and_comments(input)?;
    let enable_rls = if let Ok((rest, _)) =
        tag_no_case::<_, _, nom::error::Error<&str>>("enable_rls").parse(input)
    {
        return Ok((
            rest,
            TableDef {
                name: name.to_string(),
                columns,
                enable_rls: true,
            },
        ));
    } else {
        false
    };

    Ok((
        input,
        TableDef {
            name: name.to_string(),
            columns,
            enable_rls,
        },
    ))
}

// =============================================================================
// Policy Parsing
// =============================================================================

/// A schema item is either a table, policy, or index.
enum SchemaItem {
    Table(TableDef),
    Policy(Box<RlsPolicy>),
    Index(IndexDef),
}

/// Parse a policy definition.
///
/// Syntax:
/// ```text
/// policy <name> on <table>
///     [for all|select|insert|update|delete]
///     [restrictive]
///     [to <role>]
///     [using (<expr>)]
///     [with check (<expr>)]
/// ```
fn parse_policy(input: &str) -> IResult<&str, RlsPolicy> {
    let (input, _) = ws_and_comments(input)?;
    let (input, _) = tag_no_case("policy").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("on").parse(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = identifier(input)?;

    let mut policy = RlsPolicy::create(name, table);

    // Parse optional clauses in any order
    let mut remaining = input;
    loop {
        let (input, _) = ws_and_comments(remaining)?;

        // for all|select|insert|update|delete
        if let Ok((rest, _)) =
            tag_no_case::<_, _, nom::error::Error<&str>>("for").parse(input)
        {
            let (rest, _) = multispace1(rest)?;
            let (rest, target) = alt((
                map(tag_no_case("all"), |_| PolicyTarget::All),
                map(tag_no_case("select"), |_| PolicyTarget::Select),
                map(tag_no_case("insert"), |_| PolicyTarget::Insert),
                map(tag_no_case("update"), |_| PolicyTarget::Update),
                map(tag_no_case("delete"), |_| PolicyTarget::Delete),
            ))
            .parse(rest)?;
            policy.target = target;
            remaining = rest;
            continue;
        }

        // restrictive
        if let Ok((rest, _)) =
            tag_no_case::<_, _, nom::error::Error<&str>>("restrictive").parse(input)
        {
            policy.permissiveness = PolicyPermissiveness::Restrictive;
            remaining = rest;
            continue;
        }

        // to <role>
        if let Ok((rest, _)) =
            tag_no_case::<_, _, nom::error::Error<&str>>("to").parse(input)
        {
            // Make sure it's not "to_sql" or similar — needs whitespace after
            if let Ok((rest, _)) = multispace1::<_, nom::error::Error<&str>>(rest) {
                let (rest, role) = identifier(rest)?;
                policy.role = Some(role.to_string());
                remaining = rest;
                continue;
            }
        }

        // with check (<expr>)
        if let Ok((rest, _)) =
            tag_no_case::<_, _, nom::error::Error<&str>>("with").parse(input)
        {
            let (rest, _) = multispace1(rest)?;
            if let Ok((rest, _)) =
                tag_no_case::<_, _, nom::error::Error<&str>>("check").parse(rest)
            {
                let (rest, _) = nom_ws0(rest)?;
                let (rest, _) = char('(').parse(rest)?;
                let (rest, _) = nom_ws0(rest)?;
                let (rest, expr) = parse_policy_expr(rest)?;
                let (rest, _) = nom_ws0(rest)?;
                let (rest, _) = char(')').parse(rest)?;
                policy.with_check = Some(expr);
                remaining = rest;
                continue;
            }
        }

        // using (<expr>)
        if let Ok((rest, _)) =
            tag_no_case::<_, _, nom::error::Error<&str>>("using").parse(input)
        {
            let (rest, _) = nom_ws0(rest)?;
            let (rest, _) = char('(').parse(rest)?;
            let (rest, _) = nom_ws0(rest)?;
            let (rest, expr) = parse_policy_expr(rest)?;
            let (rest, _) = nom_ws0(rest)?;
            let (rest, _) = char(')').parse(rest)?;
            policy.using = Some(expr);
            remaining = rest;
            continue;
        }

        // No more clauses matched
        remaining = input;
        break;
    }

    Ok((remaining, policy))
}

/// Parse a policy expression: `left op right [AND/OR left op right ...]`
///
/// Produces typed `Expr::Binary` AST nodes — no raw SQL.
///
/// Handles:
/// - `column = value`
/// - `column = function('arg')::type`   (function call + cast)
/// - `expr AND expr`, `expr OR expr`
fn parse_policy_expr(input: &str) -> IResult<&str, Expr> {
    let (input, first) = parse_policy_comparison(input)?;

    // Check for AND/OR chaining
    let mut result = first;
    let mut remaining = input;
    loop {
        let (input, _) = nom_ws0(remaining)?;

        if let Ok((rest, _)) =
            tag_no_case::<_, _, nom::error::Error<&str>>("or").parse(input)
            && let Ok((rest, _)) = multispace1::<_, nom::error::Error<&str>>(rest) {
                let (rest, right) = parse_policy_comparison(rest)?;
                result = Expr::Binary {
                    left: Box::new(result),
                    op: BinaryOp::Or,
                    right: Box::new(right),
                    alias: None,
                };
                remaining = rest;
                continue;
        }

        if let Ok((rest, _)) =
            tag_no_case::<_, _, nom::error::Error<&str>>("and").parse(input)
            && let Ok((rest, _)) = multispace1::<_, nom::error::Error<&str>>(rest) {
                let (rest, right) = parse_policy_comparison(rest)?;
                result = Expr::Binary {
                    left: Box::new(result),
                    op: BinaryOp::And,
                    right: Box::new(right),
                    alias: None,
                };
                remaining = rest;
                continue;
        }

        remaining = input;
        break;
    }

    Ok((remaining, result))
}

/// Parse a single comparison: `atom op atom`
fn parse_policy_comparison(input: &str) -> IResult<&str, Expr> {
    let (input, left) = parse_policy_atom(input)?;
    let (input, _) = nom_ws0(input)?;

    // Try to parse comparison operator
    if let Ok((rest, op)) = parse_cmp_op(input) {
        let (rest, _) = nom_ws0(rest)?;
        let (rest, right) = parse_policy_atom(rest)?;
        return Ok((
            rest,
            Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
                alias: None,
            },
        ));
    }

    // No operator — just an atom
    Ok((input, left))
}

/// Parse comparison operators: =, !=, <>, >=, <=, >, <
fn parse_cmp_op(input: &str) -> IResult<&str, BinaryOp> {
    alt((
        map(tag(">="), |_| BinaryOp::Gte),
        map(tag("<="), |_| BinaryOp::Lte),
        map(tag("<>"), |_| BinaryOp::Ne),
        map(tag("!="), |_| BinaryOp::Ne),
        map(tag("="), |_| BinaryOp::Eq),
        map(tag(">"), |_| BinaryOp::Gt),
        map(tag("<"), |_| BinaryOp::Lt),
    ))
    .parse(input)
}

/// Parse a policy expression atom:
/// - identifier  (column name)
/// - function_call(args)  with optional ::cast
/// - 'string literal'
/// - numeric literal
/// - true/false
/// - (sub_expr)  grouped
fn parse_policy_atom(input: &str) -> IResult<&str, Expr> {
    alt((
        parse_policy_grouped,
        parse_policy_bool,
        parse_policy_string,
        parse_policy_number,
        parse_policy_func_or_ident, // function call or plain identifier, with optional ::cast
    ))
    .parse(input)
}

/// Parse grouped expression in parens
fn parse_policy_grouped(input: &str) -> IResult<&str, Expr> {
    let (input, _) = char('(').parse(input)?;
    let (input, _) = nom_ws0(input)?;
    let (input, expr) = parse_policy_expr(input)?;
    let (input, _) = nom_ws0(input)?;
    let (input, _) = char(')').parse(input)?;
    Ok((input, expr))
}

/// Parse true / false
fn parse_policy_bool(input: &str) -> IResult<&str, Expr> {
    alt((
        map(tag_no_case("true"), |_| {
            Expr::Literal(AstValue::Bool(true))
        }),
        map(tag_no_case("false"), |_| {
            Expr::Literal(AstValue::Bool(false))
        }),
    ))
    .parse(input)
}

/// Parse a 'string literal'
fn parse_policy_string(input: &str) -> IResult<&str, Expr> {
    let (input, _) = char('\'').parse(input)?;
    let mut end = 0;
    for (i, c) in input.char_indices() {
        if c == '\'' {
            end = i;
            break;
        }
    }
    let content = &input[..end];
    let rest = &input[end + 1..];
    Ok((rest, Expr::Literal(AstValue::String(content.to_string()))))
}

/// Parse numeric literal
fn parse_policy_number(input: &str) -> IResult<&str, Expr> {
    let (input, digits) = take_while1(|c: char| c.is_ascii_digit() || c == '.')(input)?;
    // Make sure it starts with digit (not just '.')
    if digits.starts_with('.') || digits.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Digit,
        )));
    }
    if let Ok(n) = digits.parse::<i64>() {
        Ok((input, Expr::Literal(AstValue::Int(n))))
    } else if let Ok(f) = digits.parse::<f64>() {
        Ok((input, Expr::Literal(AstValue::Float(f))))
    } else {
        Ok((input, Expr::Named(digits.to_string())))
    }
}

/// Parse function call or identifier, with optional ::cast
fn parse_policy_func_or_ident(input: &str) -> IResult<&str, Expr> {
    let (input, name) = identifier(input)?;

    // Check for function call: name(
    let mut expr = if let Ok((rest, _)) = char::<_, nom::error::Error<&str>>('(').parse(input) {
        // Parse args
        let (rest, _) = nom_ws0(rest)?;
        let (rest, args) = separated_list0(
            (nom_ws0, char(','), nom_ws0),
            parse_policy_atom,
        )
        .parse(rest)?;
        let (rest, _) = nom_ws0(rest)?;
        let (rest, _) = char(')').parse(rest)?;
        let input = rest;
        (input, Expr::FunctionCall {
            name: name.to_string(),
            args,
            alias: None,
        })
    } else {
        (input, Expr::Named(name.to_string()))
    };

    // Check for ::cast
    if let Ok((rest, _)) = tag::<_, _, nom::error::Error<&str>>("::").parse(expr.0) {
        let (rest, cast_type) = identifier(rest)?;
        expr = (
            rest,
            Expr::Cast {
                expr: Box::new(expr.1),
                target_type: cast_type.to_string(),
                alias: None,
            },
        );
    }

    Ok(expr)
}

/// Parse a single schema item: table, policy, or index
fn parse_schema_item(input: &str) -> IResult<&str, SchemaItem> {
    let (input, _) = ws_and_comments(input)?;

    // Try policy first (since "policy" is a distinct keyword)
    if let Ok((rest, policy)) = parse_policy(input) {
        return Ok((rest, SchemaItem::Policy(Box::new(policy))));
    }

    // Try index
    if let Ok((rest, idx)) = parse_index(input) {
        return Ok((rest, SchemaItem::Index(idx)));
    }

    // Otherwise parse table
    let (rest, table) = parse_table(input)?;
    Ok((rest, SchemaItem::Table(table)))
}

/// Parse an index line: `index <name> on <table> (<col1>, <col2>) [unique]`
fn parse_index(input: &str) -> IResult<&str, IndexDef> {
    let (input, _) = tag_no_case("index")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, name) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("on")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    let (input, _) = nom_ws0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, cols_str) = take_while1(|c: char| c != ')')(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = nom_ws0(input)?;
    let (input, unique_tag) = opt(tag_no_case("unique")).parse(input)?;

    let columns: Vec<String> = cols_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let is_unique = unique_tag.is_some();

    Ok((input, IndexDef {
        name: name.to_string(),
        table: table.to_string(),
        columns,
        unique: is_unique,
    }))
}

/// Parse complete schema file
fn parse_schema(input: &str) -> IResult<&str, Schema> {
    // Extract version directive before parsing
    let version = extract_version_directive(input);

    let (input, items) = many0(parse_schema_item).parse(input)?;
    let (input, _) = ws_and_comments(input)?;

    let mut tables = Vec::new();
    let mut policies = Vec::new();
    let mut indexes = Vec::new();
    for item in items {
        match item {
            SchemaItem::Table(t) => tables.push(t),
            SchemaItem::Policy(p) => policies.push(*p),
            SchemaItem::Index(i) => indexes.push(i),
        }
    }

    Ok((input, Schema { version, tables, policies, indexes }))
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

    // =========================================================================
    // Policy + enable_rls tests
    // =========================================================================

    #[test]
    fn test_enable_rls_table() {
        let input = r#"
            table orders (
                id uuid primary_key,
                operator_id uuid not null
            ) enable_rls
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        assert_eq!(schema.tables.len(), 1);
        assert!(schema.tables[0].enable_rls);
    }

    #[test]
    fn test_parse_policy_basic() {
        let input = r#"
            table orders (
                id uuid primary_key,
                operator_id uuid not null
            ) enable_rls

            policy orders_isolation on orders
                for all
                using (operator_id = current_setting('app.current_operator_id')::uuid)
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.policies.len(), 1);

        let policy = &schema.policies[0];
        assert_eq!(policy.name, "orders_isolation");
        assert_eq!(policy.table, "orders");
        assert_eq!(policy.target, PolicyTarget::All);
        assert!(policy.using.is_some());

        // Verify the expression is a typed Binary, not raw SQL
        match policy.using.as_ref().unwrap() {
            Expr::Binary { left, op, right, .. } => {
                assert_eq!(*op, BinaryOp::Eq);
                match left.as_ref() {
                    Expr::Named(n) => assert_eq!(n, "operator_id"),
                    _ => panic!("Expected Named, got {:?}", left),
                }
                match right.as_ref() {
                    Expr::Cast { target_type, expr, .. } => {
                        assert_eq!(target_type, "uuid");
                        match expr.as_ref() {
                            Expr::FunctionCall { name, args, .. } => {
                                assert_eq!(name, "current_setting");
                                assert_eq!(args.len(), 1);
                            }
                            _ => panic!("Expected FunctionCall"),
                        }
                    }
                    _ => panic!("Expected Cast, got {:?}", right),
                }
            }
            _ => panic!("Expected Binary"),
        }
    }

    #[test]
    fn test_parse_policy_with_check() {
        let input = r#"
            table orders (
                id uuid primary_key
            )

            policy orders_write on orders
                for insert
                with check (operator_id = current_setting('app.current_operator_id')::uuid)
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        let policy = &schema.policies[0];
        assert_eq!(policy.target, PolicyTarget::Insert);
        assert!(policy.with_check.is_some());
        assert!(policy.using.is_none());
    }

    #[test]
    fn test_parse_policy_restrictive_with_role() {
        let input = r#"
            table secrets (
                id uuid primary_key
            )

            policy admin_only on secrets
                for select
                restrictive
                to app_user
                using (current_setting('app.is_super_admin')::boolean = true)
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        let policy = &schema.policies[0];
        assert_eq!(policy.target, PolicyTarget::Select);
        assert_eq!(policy.permissiveness, PolicyPermissiveness::Restrictive);
        assert_eq!(policy.role.as_deref(), Some("app_user"));
        assert!(policy.using.is_some());
    }

    #[test]
    fn test_parse_policy_or_expr() {
        let input = r#"
            table orders (
                id uuid primary_key
            )

            policy tenant_or_admin on orders
                for all
                using (operator_id = current_setting('app.current_operator_id')::uuid or current_setting('app.is_super_admin')::boolean = true)
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        let policy = &schema.policies[0];

        match policy.using.as_ref().unwrap() {
            Expr::Binary { op: BinaryOp::Or, .. } => {}
            e => panic!("Expected Binary OR, got {:?}", e),
        }
    }

    #[test]
    fn test_schema_to_sql() {
        let input = r#"
            table orders (
                id uuid primary_key,
                operator_id uuid not null
            ) enable_rls

            policy orders_isolation on orders
                for all
                using (operator_id = current_setting('app.current_operator_id')::uuid)
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        let sql = schema.to_sql();
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(sql.contains("ENABLE ROW LEVEL SECURITY"));
        assert!(sql.contains("FORCE ROW LEVEL SECURITY"));
        assert!(sql.contains("CREATE POLICY"));
        assert!(sql.contains("orders_isolation"));
        assert!(sql.contains("FOR ALL"));
    }

    #[test]
    fn test_multiple_policies() {
        let input = r#"
            table orders (
                id uuid primary_key,
                operator_id uuid not null
            ) enable_rls

            policy orders_read on orders
                for select
                using (operator_id = current_setting('app.current_operator_id')::uuid)

            policy orders_write on orders
                for insert
                with check (operator_id = current_setting('app.current_operator_id')::uuid)
        "#;

        let schema = Schema::parse(input).expect("parse failed");
        assert_eq!(schema.policies.len(), 2);
        assert_eq!(schema.policies[0].name, "orders_read");
        assert_eq!(schema.policies[0].target, PolicyTarget::Select);
        assert_eq!(schema.policies[1].name, "orders_write");
        assert_eq!(schema.policies[1].target, PolicyTarget::Insert);
    }
}
