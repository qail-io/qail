//! QAIL Parser using nom.
//!
//! Parses QAIL syntax into an AST.
//!
//! # Syntax Overview (v2.0 - Rust Native)
//!
//! ```text
//! get::users:'id'email [ 'active == true, -created_at, 0..10 ]
//! ─┬─ ─┬─ ─┬────┬──── ─────────────────┬────────────────────
//!  │   │   │    │                      │
//!  │   │   │    │                      └── Unified Block (filters, sorts, ranges)
//!  │   │   │    └── Labels (columns with ')
//!  │   │   └── Link (connects to table with :)
//!  │   └── Table name
//!  └── Gate (action with ::)
//! ```
//!
//! ## Syntax v2.0 Reference
//!
//! | Symbol | Name   | Function          | Example |
//! |--------|--------|-------------------|---------|
//! | `::`   | Gate   | Action            | `get::users` |
//! | `:`    | Link   | Table to columns  | `get::users:'id` |
//! | `'`    | Label  | Column marker     | `'email'name` |
//! | `'_`   | Infer  | All columns       | `'_` → `*` |
//! | `==`   | Equal  | Filter            | `'active == true` |
//! | `~`    | Match  | Fuzzy/ILIKE       | `'name ~ "john"` |
//! | `-`    | Desc   | Sort descending   | `-created_at` |
//! | `+`    | Asc    | Sort ascending    | `+id` |
//! | `..`   | Range  | Limit/Offset      | `0..10` = LIMIT 10 OFFSET 0 |
//! | `->`   | Inner  | Inner Join        | `users -> posts` |
//! | `<-`   | Left   | Left Join         | `users <- posts` |
//! | `->>`  | Right  | Right Join        | `orders ->> customers` |
//! | `$`    | Param  | Bind variable     | `$1` |

use nom::{
    branch::alt,
    bytes::complete::{tag, take_while, take_while1},
    character::complete::{char, digit1, multispace1, not_line_ending},
    combinator::{map, opt, recognize, value},
    multi::many0,
    sequence::{pair, preceded, tuple},
    IResult,
};

use crate::ast::*;
use crate::error::{QailError, QailResult};

/// Parse whitespace or comments.
fn ws_or_comment(input: &str) -> IResult<&str, ()> {
    value((), many0(alt((
        value((), multispace1),
        parse_comment,
    ))))(input)
}

/// Parse a single comment line (// ... or -- ...).
fn parse_comment(input: &str) -> IResult<&str, ()> {
    value((), pair(alt((tag("//"), tag("--"))), not_line_ending))(input)
}

/// Parse a complete QAIL query string.
pub fn parse(input: &str) -> QailResult<QailCmd> {
    let input = input.trim();
    
    match parse_qail_cmd(input) {
        Ok(("", cmd)) => Ok(cmd),
        Ok((remaining, _)) => Err(QailError::parse(
            input.len() - remaining.len(),
            format!("Unexpected trailing content: '{}'", remaining),
        )),
        Err(e) => Err(QailError::parse(0, format!("Parse failed: {:?}", e))),
    }
}

/// Parse the complete QAIL command.
fn parse_qail_cmd(input: &str) -> IResult<&str, QailCmd> {
    let (input, action) = parse_action(input)?;
    // Check for ! after action for DISTINCT (e.g., get!::users)
    let (input, distinct_marker) = opt(char('!'))(input)?;
    let distinct = distinct_marker.is_some();
    let (input, _) = tag("::")(input)?;
    let (input, table) = parse_identifier(input)?;
    let (input, joins) = parse_joins(input)?;
    let (input, _) = ws_or_comment(input)?;
    // Link character ':' is optional - connects table to columns
    let (input, _) = opt(char(':'))(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, columns) = parse_columns(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, cages) = parse_unified_blocks(input)?;

    Ok((
        input,
        QailCmd {
            action,
            table: table.to_string(),
            joins,
            columns,
            cages,
            distinct,
        },
    ))
}

/// Parse the action (get, set, del, add, gen, make, mod, over, with).
fn parse_action(input: &str) -> IResult<&str, Action> {
    alt((
        value(Action::Get, tag("get")),
        value(Action::Set, tag("set")),
        value(Action::Del, tag("del")),
        value(Action::Add, tag("add")),
        value(Action::Gen, tag("gen")),
        value(Action::Make, tag("make")),
        value(Action::Mod, tag("mod")),
        value(Action::Over, tag("over")),
        value(Action::With, tag("with")),
    ))(input)
}

/// Parse an identifier (table name, column name).
fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

fn parse_joins(input: &str) -> IResult<&str, Vec<Join>> {
    many0(parse_single_join)(input)
}

/// Parse a single join: `->` (INNER), `<-` (LEFT), `->>` (RIGHT)
fn parse_single_join(input: &str) -> IResult<&str, Join> {
    let (input, _) = ws_or_comment(input)?;
    
    // Try RIGHT JOIN first (->>)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("->>") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return Ok((remaining, Join {
            table: table.to_string(),
            kind: JoinKind::Right,
        }));
    }
    
    // Try LEFT JOIN (<-)
    if let Ok((remaining, _)) = tag::<_, _, nom::error::Error<&str>>("<-") (input) {
        let (remaining, _) = ws_or_comment(remaining)?;
        let (remaining, table) = parse_identifier(remaining)?;
        return Ok((remaining, Join {
            table: table.to_string(),
            kind: JoinKind::Left,
        }));
    }
    
    // Default: INNER JOIN (->)
    let (input, _) = tag("->")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, table) = parse_identifier(input)?;
    Ok((input, Join {
        table: table.to_string(),
        kind: JoinKind::Inner,
    }))
}

/// Parse columns using the v2.0 label syntax ('col).
fn parse_columns(input: &str) -> IResult<&str, Vec<Column>> {
    many0(preceded(ws_or_comment, parse_any_column))(input)
}

fn parse_any_column(input: &str) -> IResult<&str, Column> {
    alt((
        // v2.0 Label: 'col...
        preceded(char('\''), parse_label_column),
        // Legacy Hook: @col... (for backwards compat during migration)
        preceded(char('@'), parse_at_column),
    ))(input)
}

/// Parse a column with the v2.0 label syntax ('col).
fn parse_label_column(input: &str) -> IResult<&str, Column> {
    alt((
        // Wildcard: '_ for all columns
        value(Column::Star, char('_')),
        // Named or complex column
        parse_column_full_def_or_named,
    ))(input)
}

/// Legacy @ column parsing for backwards compatibility.
fn parse_at_column(input: &str) -> IResult<&str, Column> {
    alt((
        value(Column::Star, char('*')),
        // Check for drop via @-name convention
        map(preceded(char('-'), parse_identifier), |name| Column::Mod { 
            kind: ModKind::Drop, 
            col: Box::new(Column::Named(name.to_string())) 
        }),
        parse_column_full_def_or_named, 
    ))(input)
}

fn parse_column_full_def_or_named(input: &str) -> IResult<&str, Column> {
    // 1. Parse Name
    let (input, name) = parse_identifier(input)?;
    
    // 2. Opt: Aggregates (#func)
    if let Ok((input, Some(func))) = opt(preceded(char('#'), parse_agg_func))(input) {
        return Ok((input, Column::Aggregate {
             col: name.to_string(),
             func
        }));
    }
    
    // 3. Opt: check for colon (type definition)
    if let Ok((input, _)) = char::<_, nom::error::Error<&str>>(':')(input) {
        // We have a type OR a window function.
        let (input, type_or_func) = parse_identifier(input)?;
        
        let (input, _) = ws_or_comment(input)?;
        
        // Peek/Check for open paren `(` for window function
        if let Ok((input, _)) = char::<_, nom::error::Error<&str>>('(')(input) {
            // It IS a function call -> Window Column
            let (input, _) = ws_or_comment(input)?;
            let (input, args) = opt(tuple((
                parse_value,
                many0(preceded(
                    tuple((ws_or_comment, char(','), ws_or_comment)),
                    parse_value
                ))
            )))(input)?;
            let (input, _) = ws_or_comment(input)?;
            let (input, _) = char(')')(input)?;
            
            let params = match args {
                Some((first, mut rest)) => {
                    let mut v = vec![first];
                    v.append(&mut rest);
                    v
                },
                None => vec![],
            };

            // Parse Order Cages (e.g. ^!amount) - legacy syntax
            let (input, sorts) = many0(parse_legacy_sort_cage)(input)?;
            
            // Parse Partition: {Part=...}
            let (input, partitions) = opt(parse_partition_block)(input)?;
            let partition = partitions.unwrap_or_default();

            return Ok((input, Column::Window {
                name: name.to_string(),
                func: type_or_func.to_string(),
                params,
                partition,
                order: sorts,
            }));
        } else {
            // It is just a Type Definition
            let (input, constraints) = parse_constraints(input)?;
            
            return Ok((input, Column::Def { 
                name: name.to_string(), 
                data_type: type_or_func.to_string(), 
                constraints 
            }));
        }
    }
    
    // No colon, check for constraints (inferred type Def)
    let (input, constraints) = parse_constraints(input)?;
    if !constraints.is_empty() {
         Ok((input, Column::Def { 
            name: name.to_string(), 
            data_type: "str".to_string(), 
            constraints 
        }))
    } else {
        // Just a named column
        Ok((input, Column::Named(name.to_string())))
    }
}

fn parse_constraints(input: &str) -> IResult<&str, Vec<Constraint>> {
    many0(alt((
        value(Constraint::PrimaryKey, tag("^pk")),
        value(Constraint::Unique, tag("^uniq")),
        value(Constraint::Nullable, char('?')),
    )))(input)
}

fn parse_agg_func(input: &str) -> IResult<&str, AggregateFunc> {
    alt((
        value(AggregateFunc::Count, tag("count")),
        value(AggregateFunc::Sum, tag("sum")),
        value(AggregateFunc::Avg, tag("avg")),
        value(AggregateFunc::Min, tag("min")),
        value(AggregateFunc::Max, tag("max")),
    ))(input)
}

/// Parse unified constraint blocks [...].
/// v2.0 syntax: [ 'active == true, -created_at, 0..10 ]
fn parse_unified_blocks(input: &str) -> IResult<&str, Vec<Cage>> {
    many0(preceded(ws_or_comment, parse_unified_block))(input)
}

/// Parse a unified constraint block [...].
/// Contains comma-separated items: filters, sorts, ranges.
fn parse_unified_block(input: &str) -> IResult<&str, Cage> {
    let (input, _) = char('[')(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    // Parse all items in the block (comma-separated)
    let (input, items) = parse_block_items(input)?;
    
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char(']')(input)?;
    
    // Convert items into appropriate cages
    // For now, combine into a single cage or return the first meaningful one
    items_to_cage(items, input)
}

/// Represents a parsed item within a unified block.
#[derive(Debug)]
enum BlockItem {
    Filter(Condition, LogicalOp),
    Sort(String, SortOrder),
    Range(usize, Option<usize>), // start, end
    LegacyLimit(usize),
    LegacyOffset(usize),
}

/// Parse comma-separated items within a block.
/// Also handles | (OR) and & (AND) operators for filter conditions.
fn parse_block_items(input: &str) -> IResult<&str, Vec<BlockItem>> {
    let (input, first) = opt(parse_block_item)(input)?;
    
    match first {
        None => Ok((input, vec![])),
        Some(mut item) => {
            let mut items = vec![];
            let mut remaining = input;
            
            loop {
                let (input, _) = ws_or_comment(remaining)?;
                
                // Check for various separators: comma, pipe (OR), ampersand (AND)
                if let Ok((input, _)) = char::<_, nom::error::Error<&str>>(',')(input) {
                    // Comma separator - add current item and parse next
                    items.push(item);
                    let (input, _) = ws_or_comment(input)?;
                    let (input, next_item) = parse_block_item(input)?;
                    item = next_item;
                    remaining = input;
                } else if let Ok((new_input, _)) = char::<_, nom::error::Error<&str>>('|')(input) {
                    // OR separator - update item's logical op and parse next filter
                    if let BlockItem::Filter(cond, _) = item {
                        items.push(BlockItem::Filter(cond, LogicalOp::Or));
                    } else {
                        items.push(item);
                    }
                    let (new_input, _) = ws_or_comment(new_input)?;
                    let (new_input, next_item) = parse_filter_item(new_input)?;
                    // Mark the next item as part of an OR chain
                    if let BlockItem::Filter(cond, _) = next_item {
                        item = BlockItem::Filter(cond, LogicalOp::Or);
                    } else {
                        item = next_item;
                    }
                    remaining = new_input;
                } else if let Ok((new_input, _)) = char::<_, nom::error::Error<&str>>('&')(input) {
                    // AND separator
                    items.push(item);
                    let (new_input, _) = ws_or_comment(new_input)?;
                    let (new_input, next_item) = parse_filter_item(new_input)?;
                    item = next_item;
                    remaining = new_input;
                } else {
                    items.push(item);
                    remaining = input;
                    break;
                }
            }
            
            Ok((remaining, items))
        }
    }
}

/// Parse a single item in a unified block.
fn parse_block_item(input: &str) -> IResult<&str, BlockItem> {
    alt((
        // Range: N..M or N.. (must try before other number parsing)
        parse_range_item,
        // Sort: +col (asc) or -col (desc)
        parse_sort_item,
        // Legacy limit: lim=N
        parse_legacy_limit_item,
        // Legacy offset: off=N
        parse_legacy_offset_item,
        // Legacy sort: ^col or ^!col
        parse_legacy_sort_item,
        // Filter: 'col == value
        parse_filter_item,
    ))(input)
}

/// Parse a range item: N..M or N..
fn parse_range_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, start) = digit1(input)?;
    let (input, _) = tag("..")(input)?;
    let (input, end) = opt(digit1)(input)?;
    
    let start_num: usize = start.parse().unwrap_or(0);
    let end_num = end.map(|e| e.parse().unwrap_or(0));
    
    Ok((input, BlockItem::Range(start_num, end_num)))
}

/// Parse a sort item: +col (asc) or -col (desc).
fn parse_sort_item(input: &str) -> IResult<&str, BlockItem> {
    alt((
        map(preceded(char('+'), parse_identifier), |col| {
            BlockItem::Sort(col.to_string(), SortOrder::Asc)
        }),
        map(preceded(char('-'), parse_identifier), |col| {
            BlockItem::Sort(col.to_string(), SortOrder::Desc)
        }),
    ))(input)
}

/// Parse legacy limit: lim=N
fn parse_legacy_limit_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, _) = tag("lim")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, n) = digit1(input)?;
    Ok((input, BlockItem::LegacyLimit(n.parse().unwrap_or(10))))
}

/// Parse legacy offset: off=N
fn parse_legacy_offset_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, _) = tag("off")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, n) = digit1(input)?;
    Ok((input, BlockItem::LegacyOffset(n.parse().unwrap_or(0))))
}

/// Parse legacy sort: ^col or ^!col
fn parse_legacy_sort_item(input: &str) -> IResult<&str, BlockItem> {
    let (input, _) = char('^')(input)?;
    let (input, desc) = opt(char('!'))(input)?;
    let (input, col) = parse_identifier(input)?;
    
    let order = if desc.is_some() {
        SortOrder::Desc
    } else {
        SortOrder::Asc
    };
    
    Ok((input, BlockItem::Sort(col.to_string(), order)))
}

/// Parse a filter item: 'col == value or col == value
fn parse_filter_item(input: &str) -> IResult<&str, BlockItem> {
    // Optional leading ' for column (v2.0 style)
    let (input, _) = opt(char('\''))(input)?;
    let (input, column) = parse_identifier(input)?;
    
    // Check for array unnest syntax: column[*]
    let (input, is_array_unnest) = if input.starts_with("[*]") {
        (&input[3..], true)
    } else {
        (input, false)
    };
    
    let (input, _) = ws_or_comment(input)?;
    let (input, (op, val)) = parse_operator_and_value(input)?;
    
    Ok((input, BlockItem::Filter(
        Condition {
            column: column.to_string(),
            op,
            value: val,
            is_array_unnest,
        },
        LogicalOp::And, // Default, could be enhanced for | and &
    )))
}

/// Convert parsed block items into a Cage.
fn items_to_cage(items: Vec<BlockItem>, input: &str) -> IResult<&str, Cage> {
    // Default: return a filter cage if we have filters
    let mut conditions = Vec::new();
    let mut logical_op = LogicalOp::And;
    
    // Check for special single-item cases
    for item in &items {
        match item {
            BlockItem::Range(start, end) => {
                // Range: start..end means OFFSET start, LIMIT (end - start)
                // If end is None, it's just OFFSET start
                // v2.0 semantics: 0..10 = LIMIT 10 OFFSET 0
                //                 20..30 = LIMIT 10 OFFSET 20
                if let Some(e) = end {
                    let limit = e - start;
                    let offset = *start;
                    // We need to return multiple cages, but our current structure
                    // returns one. For now, prioritize LIMIT if offset is 0,
                    // otherwise use OFFSET.
                    if offset == 0 {
                        return Ok((input, Cage {
                            kind: CageKind::Limit(limit),
                            conditions: vec![],
                            logical_op: LogicalOp::And,
                        }));
                    } else {
                        // Store limit in conditions as a workaround? No, just return offset.
                        // Actually, let's return a compound cage. But the AST doesn't support that.
                        // For proper v2.0, we'd need to extend the AST or return Vec<Cage>.
                        // For now: return LIMIT with offset stored somehow.
                        // Workaround: return the cage kind that combines both.
                        return Ok((input, Cage {
                            kind: CageKind::Limit(limit),
                            conditions: vec![Condition {
                                column: "__offset__".to_string(),
                                op: Operator::Eq,
                                value: Value::Int(offset as i64),
                                is_array_unnest: false,
                            }],
                            logical_op: LogicalOp::And,
                        }));
                    }
                } else {
                    // Just offset
                    return Ok((input, Cage {
                        kind: CageKind::Offset(*start),
                        conditions: vec![],
                        logical_op: LogicalOp::And,
                    }));
                }
            }
            BlockItem::Sort(col, order) => {
                return Ok((input, Cage {
                    kind: CageKind::Sort(*order),
                    conditions: vec![Condition {
                        column: col.clone(),
                        op: Operator::Eq,
                        value: Value::Null,
                        is_array_unnest: false,
                    }],
                    logical_op: LogicalOp::And,
                }));
            }
            BlockItem::LegacyLimit(n) => {
                return Ok((input, Cage {
                    kind: CageKind::Limit(*n),
                    conditions: vec![],
                    logical_op: LogicalOp::And,
                }));
            }
            BlockItem::LegacyOffset(n) => {
                return Ok((input, Cage {
                    kind: CageKind::Offset(*n),
                    conditions: vec![],
                    logical_op: LogicalOp::And,
                }));
            }
            BlockItem::Filter(cond, op) => {
                conditions.push(cond.clone());
                logical_op = *op;
            }
        }
    }
    
    // If we have conditions, return a filter cage
    if !conditions.is_empty() {
        Ok((input, Cage {
            kind: CageKind::Filter,
            conditions,
            logical_op,
        }))
    } else {
        // Empty block - return empty filter
        Ok((input, Cage {
            kind: CageKind::Filter,
            conditions: vec![],
            logical_op: LogicalOp::And,
        }))
    }
}

/// Parse operator and value together.
fn parse_operator_and_value(input: &str) -> IResult<&str, (Operator, Value)> {
    alt((
        // Fuzzy match: ~value
        map(preceded(char('~'), preceded(ws_or_comment, parse_value)), |v| (Operator::Fuzzy, v)),
        // v2.0 Equal: ==value (try before >=)
        map(preceded(tag("=="), preceded(ws_or_comment, parse_value)), |v| (Operator::Eq, v)),
        // Greater than or equal: >=value
        map(preceded(tag(">="), preceded(ws_or_comment, parse_value)), |v| (Operator::Gte, v)),
        // Less than or equal: <=value
        map(preceded(tag("<="), preceded(ws_or_comment, parse_value)), |v| (Operator::Lte, v)),
        // Not equal: !=value
        map(preceded(tag("!="), preceded(ws_or_comment, parse_value)), |v| (Operator::Ne, v)),
        // Greater than: >value
        map(preceded(char('>'), preceded(ws_or_comment, parse_value)), |v| (Operator::Gt, v)),
        // Less than: <value
        map(preceded(char('<'), preceded(ws_or_comment, parse_value)), |v| (Operator::Lt, v)),
        // Legacy Equal: =value (backwards compat)
        map(preceded(char('='), preceded(ws_or_comment, parse_value)), |v| (Operator::Eq, v)),
    ))(input)
}

/// Parse a value.
fn parse_value(input: &str) -> IResult<&str, Value> {
    let (input, _) = ws_or_comment(input)?;
    
    alt((
        // Parameter: $1, $2, etc.
        map(preceded(char('$'), digit1), |n: &str| {
            Value::Param(n.parse().unwrap_or(1))
        }),
        // Boolean: true/false
        value(Value::Bool(true), tag("true")),
        value(Value::Bool(false), tag("false")),
        // Function call: name(args)
        parse_function_call,
        // Function without parens: now, etc. (keyword-like)
        map(tag("now"), |_| Value::Function("now".to_string())),
        // Number (float or int)
        parse_number,
        // Double-quoted string (v2.0 style)
        parse_double_quoted_string,
        // Single-quoted string (legacy)
        parse_quoted_string,
        // Bare identifier (treated as string)
        map(parse_identifier, |s| Value::String(s.to_string())),
    ))(input)
}

/// Parse function call: name(arg1, arg2)
fn parse_function_call(input: &str) -> IResult<&str, Value> {
    let (input, name) = parse_identifier(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, args) = opt(tuple((
        parse_value,
        many0(preceded(
            tuple((ws_or_comment, char(','), ws_or_comment)),
            parse_value
        ))
    )))(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char(')')(input)?;

    let params = match args {
        Some((first, mut rest)) => {
            let mut v = vec![first];
            v.append(&mut rest);
            v
        },
        None => vec![],
    };

    Ok((input, Value::Function(format!("{}({})", name, params.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ")))))
}

/// Parse a number (integer or float).
fn parse_number(input: &str) -> IResult<&str, Value> {
    let (input, num_str) = recognize(tuple((
        opt(char('-')),
        digit1,
        opt(pair(char('.'), digit1)),
    )))(input)?;
    
    if num_str.contains('.') {
        Ok((input, Value::Float(num_str.parse().unwrap_or(0.0))))
    } else {
        Ok((input, Value::Int(num_str.parse().unwrap_or(0))))
    }
}

/// Parse a single-quoted string (legacy).
fn parse_quoted_string(input: &str) -> IResult<&str, Value> {
    let (input, _) = char('\'')(input)?;
    let (input, content) = take_while(|c| c != '\'')(input)?;
    let (input, _) = char('\'')(input)?;
    
    Ok((input, Value::String(content.to_string())))
}

/// Parse a double-quoted string (v2.0 style).
fn parse_double_quoted_string(input: &str) -> IResult<&str, Value> {
    let (input, _) = char('"')(input)?;
    let (input, content) = take_while(|c| c != '"')(input)?;
    let (input, _) = char('"')(input)?;
    
    Ok((input, Value::String(content.to_string())))
}

/// Parse legacy sort cage [^col] or [^!col] for window functions.
fn parse_legacy_sort_cage(input: &str) -> IResult<&str, Cage> {
    let (input, _) = char('^')(input)?;
    let (input, desc) = opt(char('!'))(input)?;
    let (input, col) = parse_identifier(input)?;
    
    let order = if desc.is_some() {
        SortOrder::Desc
    } else {
        SortOrder::Asc
    };
    
    Ok((
        input,
        Cage {
            kind: CageKind::Sort(order),
            conditions: vec![Condition {
                column: col.to_string(),
                op: Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        },
    ))
}

fn parse_partition_block(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = char('{')(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = tag("Part")(input)?;
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = ws_or_comment(input)?;
    
    let (input, first) = parse_identifier(input)?;
    let (input, rest) = many0(preceded(
        tuple((ws_or_comment, char(','), ws_or_comment)),
        parse_identifier
    ))(input)?;
    
    let (input, _) = ws_or_comment(input)?;
    let (input, _) = char('}')(input)?;
    
    let mut cols = vec![first.to_string()];
    cols.append(&mut rest.iter().map(|s| s.to_string()).collect());
    Ok((input, cols))
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // v2.0 Syntax Tests
    // ========================================================================

    #[test]
    fn test_v2_simple_get() {
        let cmd = parse("get::users:'_").unwrap();
        assert_eq!(cmd.action, Action::Get);
        assert_eq!(cmd.table, "users");
        assert_eq!(cmd.columns, vec![Column::Star]);
    }

    #[test]
    fn test_v2_get_with_columns() {
        let cmd = parse("get::users:'id'email").unwrap();
        assert_eq!(cmd.action, Action::Get);
        assert_eq!(cmd.table, "users");
        assert_eq!(
            cmd.columns,
            vec![
                Column::Named("id".to_string()),
                Column::Named("email".to_string()),
            ]
        );
    }

    #[test]
    fn test_v2_get_with_filter() {
        let cmd = parse("get::users:'_ [ 'active == true ]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].kind, CageKind::Filter);
        assert_eq!(cmd.cages[0].conditions.len(), 1);
        assert_eq!(cmd.cages[0].conditions[0].column, "active");
        assert_eq!(cmd.cages[0].conditions[0].op, Operator::Eq);
        assert_eq!(cmd.cages[0].conditions[0].value, Value::Bool(true));
    }

    #[test]
    fn test_v2_get_with_range_limit() {
        let cmd = parse("get::users:'_ [ 0..10 ]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].kind, CageKind::Limit(10));
    }

    #[test]
    fn test_v2_get_with_range_offset() {
        let cmd = parse("get::users:'_ [ 20..30 ]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        // Range 20..30 = LIMIT 10 with offset 20
        assert_eq!(cmd.cages[0].kind, CageKind::Limit(10));
        // Offset stored in conditions as workaround
        assert_eq!(cmd.cages[0].conditions[0].column, "__offset__");
        assert_eq!(cmd.cages[0].conditions[0].value, Value::Int(20));
    }

    #[test]
    fn test_v2_get_with_sort_desc() {
        let cmd = parse("get::users:'_ [ -created_at ]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].kind, CageKind::Sort(SortOrder::Desc));
        assert_eq!(cmd.cages[0].conditions[0].column, "created_at");
    }

    #[test]
    fn test_v2_get_with_sort_asc() {
        let cmd = parse("get::users:'_ [ +id ]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].kind, CageKind::Sort(SortOrder::Asc));
        assert_eq!(cmd.cages[0].conditions[0].column, "id");
    }

    #[test]
    fn test_v2_fuzzy_match() {
        let cmd = parse("get::users:'id [ 'name ~ \"john\" ]").unwrap();
        assert_eq!(cmd.cages[0].conditions[0].op, Operator::Fuzzy);
        assert_eq!(cmd.cages[0].conditions[0].value, Value::String("john".to_string()));
    }

    #[test]
    fn test_v2_param_in_filter() {
        let cmd = parse("get::users:'id [ 'email == $1 ]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].conditions[0].value, Value::Param(1));
    }

    #[test]
    fn test_v2_left_join() {
        // Joins come directly after table name, not after columns
        let cmd = parse("get::users<-posts:'id'title").unwrap();
        assert_eq!(cmd.joins.len(), 1);
        assert_eq!(cmd.joins[0].table, "posts");
        assert_eq!(cmd.joins[0].kind, JoinKind::Left);
    }

    #[test]
    fn test_v2_inner_join() {
        let cmd = parse("get::users->posts:'id'title").unwrap();
        assert_eq!(cmd.joins.len(), 1);
        assert_eq!(cmd.joins[0].table, "posts");
        assert_eq!(cmd.joins[0].kind, JoinKind::Inner);
    }

    #[test]
    fn test_v2_right_join() {
        let cmd = parse("get::orders->>customers:'_").unwrap();
        assert_eq!(cmd.joins.len(), 1);
        assert_eq!(cmd.joins[0].table, "customers");
        assert_eq!(cmd.joins[0].kind, JoinKind::Right);
    }

    // ========================================================================
    // Legacy Syntax Tests (backwards compatibility)
    // ========================================================================

    #[test]
    fn test_legacy_simple_get() {
        let cmd = parse("get::users:@*").unwrap();
        assert_eq!(cmd.action, Action::Get);
        assert_eq!(cmd.table, "users");
        assert_eq!(cmd.columns, vec![Column::Star]);
    }

    #[test]
    fn test_legacy_get_with_columns() {
        let cmd = parse("get::users:@id@email@role").unwrap();
        assert_eq!(cmd.action, Action::Get);
        assert_eq!(cmd.table, "users");
        assert_eq!(
            cmd.columns,
            vec![
                Column::Named("id".to_string()),
                Column::Named("email".to_string()),
                Column::Named("role".to_string()),
            ]
        );
    }

    #[test]
    fn test_legacy_get_with_filter() {
        let cmd = parse("get::users:@*[active=true]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].kind, CageKind::Filter);
        assert_eq!(cmd.cages[0].conditions.len(), 1);
        assert_eq!(cmd.cages[0].conditions[0].column, "active");
        assert_eq!(cmd.cages[0].conditions[0].op, Operator::Eq);
        assert_eq!(cmd.cages[0].conditions[0].value, Value::Bool(true));
    }

    #[test]
    fn test_legacy_get_with_limit() {
        let cmd = parse("get::users:@*[lim=10]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].kind, CageKind::Limit(10));
    }

    #[test]
    fn test_legacy_get_with_sort_desc() {
        let cmd = parse("get::users:@*[^!created_at]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].kind, CageKind::Sort(SortOrder::Desc));
    }

    #[test]
    fn test_set_command() {
        let cmd = parse("set::users:[verified=true][id=$1]").unwrap();
        assert_eq!(cmd.action, Action::Set);
        assert_eq!(cmd.table, "users");
        assert_eq!(cmd.cages.len(), 2);
    }

    #[test]
    fn test_del_command() {
        let cmd = parse("del::sessions:[expired_at<now]").unwrap();
        assert_eq!(cmd.action, Action::Del);
        assert_eq!(cmd.table, "sessions");
    }

    #[test]
    fn test_legacy_fuzzy_match() {
        let cmd = parse("get::users:@*[name~$1]").unwrap();
        assert_eq!(cmd.cages[0].conditions[0].op, Operator::Fuzzy);
    }

    #[test]
    fn test_legacy_complex_query() {
        let cmd = parse("get::users:@id@email@role[active=true][lim=10]").unwrap();
        assert_eq!(cmd.action, Action::Get);
        assert_eq!(cmd.table, "users");
        assert_eq!(cmd.columns.len(), 3);
        assert_eq!(cmd.cages.len(), 2);
    }

    #[test]
    fn test_legacy_param_in_filter() {
        let cmd = parse("get::users:@*[id=$1]").unwrap();
        assert_eq!(cmd.cages.len(), 1);
        assert_eq!(cmd.cages[0].conditions[0].value, Value::Param(1));
    }

    #[test]
    fn test_legacy_param_in_update() {
        let cmd = parse("set::users:[verified=true][id=$1]").unwrap();
        assert_eq!(cmd.action, Action::Set);
        assert_eq!(cmd.cages.len(), 2);
        assert_eq!(cmd.cages[1].conditions[0].value, Value::Param(1));
    }
}
