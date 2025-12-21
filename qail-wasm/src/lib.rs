//! QAIL WebAssembly Bindings
//!
//! Provides QAIL parsing and SQL transpilation for JavaScript/TypeScript.
//!
//! Usage (npm):
//! ```javascript
//! import init, { parse, toSql, parseAndTranspile } from 'qail-wasm';
//!
//! await init();
//!
//! // Parse to AST
//! const ast = parse("get::users•@*[active=true]");
//! console.log(ast);
//!
//! // Parse and get SQL directly
//! const sql = parseAndTranspile("get::users•@*[active=true]");
//! console.log(sql); // "SELECT * FROM users WHERE active = true"
//! ```

use wasm_bindgen::prelude::*;
use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0},
    combinator::{map, opt, value},
    multi::{many0, separated_list0},
    sequence::{delimited, preceded, tuple},
};

#[wasm_bindgen]
pub fn parse_and_transpile(qail: &str) -> Result<String, JsError> {
    let cmd = parse_qail(qail).map_err(|e| JsError::new(&e))?;
    Ok(cmd.to_sql())
}

#[wasm_bindgen]
pub fn parse(qail: &str) -> Result<JsValue, JsError> {
    let cmd = parse_qail(qail).map_err(|e| JsError::new(&e))?;
    serde_wasm_bindgen::to_value(&cmd).map_err(|e| JsError::new(&e.to_string()))
}

#[wasm_bindgen]
pub fn validate(qail: &str) -> bool {
    parse_qail(qail).is_ok()
}

// ========== AST Types ==========

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QailCmd {
    pub action: String,
    pub table: String,
    pub columns: Vec<Column>,
    pub conditions: Vec<Condition>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub order_by: Option<OrderBy>,
    pub distinct: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Column {
    pub name: String,
    pub alias: Option<String>,
    pub aggregate: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Condition {
    pub column: String,
    pub operator: String,
    pub value: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OrderBy {
    pub column: String,
    pub descending: bool,
}

impl QailCmd {
    fn to_sql(&self) -> String {
        match self.action.as_str() {
            "get" => self.to_select_sql(),
            "set" => self.to_update_sql(),
            "del" => self.to_delete_sql(),
            "add" => self.to_insert_sql(),
            _ => format!("-- Unknown action: {}", self.action),
        }
    }

    fn to_select_sql(&self) -> String {
        let mut sql = if self.distinct {
            String::from("SELECT DISTINCT ")
        } else {
            String::from("SELECT ")
        };

        // Columns
        if self.columns.is_empty() || self.columns.iter().any(|c| c.name == "*") {
            sql.push('*');
        } else {
            let cols: Vec<String> = self.columns.iter().map(|c| {
                if let Some(agg) = &c.aggregate {
                    format!("{}({})", agg.to_uppercase(), c.name)
                } else if let Some(alias) = &c.alias {
                    format!("{} AS {}", c.name, alias)
                } else {
                    c.name.clone()
                }
            }).collect();
            sql.push_str(&cols.join(", "));
        }

        sql.push_str(" FROM ");
        sql.push_str(&self.table);

        // WHERE
        if !self.conditions.is_empty() {
            sql.push_str(" WHERE ");
            let conds: Vec<String> = self.conditions.iter().map(|c| {
                format!("{} {} {}", c.column, c.operator, c.value)
            }).collect();
            sql.push_str(&conds.join(" AND "));
        }

        // ORDER BY
        if let Some(order) = &self.order_by {
            sql.push_str(" ORDER BY ");
            sql.push_str(&order.column);
            if order.descending {
                sql.push_str(" DESC");
            }
        }

        // LIMIT
        if let Some(lim) = self.limit {
            sql.push_str(&format!(" LIMIT {}", lim));
        }

        // OFFSET
        if let Some(off) = self.offset {
            sql.push_str(&format!(" OFFSET {}", off));
        }

        sql
    }

    fn to_update_sql(&self) -> String {
        let mut sql = format!("UPDATE {} SET ", self.table);
        
        // First condition is the SET clause for updates
        if let Some(set_cond) = self.conditions.first() {
            sql.push_str(&format!("{} = {}", set_cond.column, set_cond.value));
        }

        // Remaining conditions are WHERE clauses
        if self.conditions.len() > 1 {
            sql.push_str(" WHERE ");
            let where_conds: Vec<String> = self.conditions[1..].iter().map(|c| {
                format!("{} {} {}", c.column, c.operator, c.value)
            }).collect();
            sql.push_str(&where_conds.join(" AND "));
        }

        sql
    }

    fn to_delete_sql(&self) -> String {
        let mut sql = format!("DELETE FROM {}", self.table);

        if !self.conditions.is_empty() {
            sql.push_str(" WHERE ");
            let conds: Vec<String> = self.conditions.iter().map(|c| {
                format!("{} {} {}", c.column, c.operator, c.value)
            }).collect();
            sql.push_str(&conds.join(" AND "));
        }

        sql
    }

    fn to_insert_sql(&self) -> String {
        let cols: Vec<String> = self.columns.iter().map(|c| c.name.clone()).collect();
        let vals: Vec<String> = self.conditions.iter().map(|c| c.value.clone()).collect();

        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table,
            cols.join(", "),
            vals.join(", ")
        )
    }
}

// ========== Parser ==========

fn parse_qail(input: &str) -> Result<QailCmd, String> {
    let input = input.trim();
    
    // Parse action (get, set, del, add)
    let (input, (action, distinct)) = parse_action(input)
        .map_err(|_| "Failed to parse action (expected get/set/del/add)")?;
    
    // Parse ::
    let (input, _) = tag::<_, _, nom::error::Error<&str>>("::")(input)
        .map_err(|_| "Expected '::' after action")?;
    
    // Parse table name
    let (input, table) = parse_identifier(input)
        .map_err(|_| "Expected table name")?;
    
    // Parse optional pivot and columns
    let (input, columns) = parse_columns(input).unwrap_or((input, vec![]));
    
    // Parse conditions/cages
    let (_, (conditions, limit, offset, order_by)) = parse_cages(input)
        .unwrap_or((input, (vec![], None, None, None)));

    Ok(QailCmd {
        action: action.to_string(),
        table: table.to_string(),
        columns,
        conditions,
        limit,
        offset,
        order_by,
        distinct,
    })
}

fn parse_action(input: &str) -> IResult<&str, (&str, bool)> {
    let (input, action) = alt((
        tag("get"),
        tag("set"),
        tag("del"),
        tag("add"),
    ))(input)?;
    
    let (input, distinct_marker) = opt(char('!'))(input)?;
    
    Ok((input, (action, distinct_marker.is_some())))
}

fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

fn parse_columns(input: &str) -> IResult<&str, Vec<Column>> {
    // Skip pivot character
    let (input, _) = opt(alt((char::<&str, nom::error::Error<&str>>('•'), char('.'))))(input)?;
    
    // Parse @column hooks
    let (input, columns) = many0(preceded(
        char::<&str, nom::error::Error<&str>>('@'),
        map(
            tuple((
                parse_column_name,
                opt(preceded(char::<&str, nom::error::Error<&str>>(':'), parse_identifier)),
            )),
            |(name, alias)| Column {
                name: name.to_string(),
                alias: alias.map(|s| s.to_string()),
                aggregate: None,
            }
        )
    ))(input)?;

    Ok((input, columns))
}

fn parse_column_name(input: &str) -> IResult<&str, &str> {
    alt((
        tag("*"),
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
    ))(input)
}

fn parse_cages(input: &str) -> IResult<&str, (Vec<Condition>, Option<u32>, Option<u32>, Option<OrderBy>)> {
    let mut conditions = vec![];
    let mut limit = None;
    let mut offset = None;
    let mut order_by = None;
    let mut remaining = input;

    while let Ok((rest, cage_content)) = delimited::<&str, _, _, _, nom::error::Error<&str>, _, _, _>(
        char('['),
        take_while1(|c: char| c != ']'),
        char(']'),
    )(remaining) {
        remaining = rest;
        
        // Parse cage content
        if let Some(lim) = cage_content.strip_prefix("lim=") {
            limit = lim.parse().ok();
        } else if let Some(off) = cage_content.strip_prefix("off=") {
            offset = off.parse().ok();
        } else if cage_content.starts_with("^!") {
            order_by = Some(OrderBy {
                column: cage_content[2..].to_string(),
                descending: true,
            });
        } else if cage_content.starts_with("^") {
            order_by = Some(OrderBy {
                column: cage_content[1..].to_string(),
                descending: false,
            });
        } else {
            // Parse condition
            if let Some(cond) = parse_condition(cage_content) {
                conditions.push(cond);
            }
        }
    }

    Ok((remaining, (conditions, limit, offset, order_by)))
}

fn parse_condition(input: &str) -> Option<Condition> {
    // Try different operators
    let operators = [">=", "<=", "!=", "=", ">", "<", "~"];
    
    for op in operators {
        if let Some(pos) = input.find(op) {
            let column = input[..pos].trim().to_string();
            let value = input[pos + op.len()..].trim().to_string();
            
            let sql_op = match op {
                "~" => "ILIKE",
                "!=" => "<>",
                _ => op,
            };
            
            return Some(Condition {
                column,
                operator: sql_op.to_string(),
                value,
            });
        }
    }
    
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let sql = parse_and_transpile("get::users•@*").unwrap();
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("FROM users"));
    }

    #[test]
    fn test_select_with_filter() {
        let sql = parse_and_transpile("get::users•@id@name[active=true]").unwrap();
        assert!(sql.contains("WHERE active = true"));
    }

    #[test]
    fn test_distinct() {
        let sql = parse_and_transpile("get!::users•@role").unwrap();
        assert!(sql.contains("SELECT DISTINCT"));
    }
}
