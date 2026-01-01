pub mod base;
pub mod binary_ops;
pub mod case_when;
pub mod clauses;
pub mod cte;
pub mod ddl;
pub mod dml;
pub mod expressions;
pub mod functions;
pub mod joins;
pub mod special_funcs;

use crate::ast::*;
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1},
    combinator::{opt, value},
    multi::many0,
};
use self::base::*;
use self::clauses::*;
use self::ddl::*;
use self::dml::*;
use self::joins::*;
// use self::expressions::*; // Used in clauses module

/// Parse a QAIL query with comment preprocessing.
/// This is the recommended entry point - handles SQL comment stripping.
pub fn parse(input: &str) -> Result<Qail, String> {
    let cleaned = strip_sql_comments(input);
    match parse_root(&cleaned) {
        Ok((_, cmd)) => Ok(cmd),
        Err(e) => Err(format!("Parse error: {:?}", e)),
    }
}

/// Parse a QAIL query (root entry point).
/// Note: Does NOT strip comments. Use `parse()` for automatic comment handling.
pub fn parse_root(input: &str) -> IResult<&str, Qail> {
    let input = input.trim();

    // Try transaction commands first (single keywords)
    if let Ok((remaining, cmd)) = parse_txn_command(input) {
        return Ok((remaining, cmd));
    }

    // Try CREATE INDEX first (special case: "index name on table ...")
    if let Ok((remaining, cmd)) = parse_create_index(input) {
        return Ok((remaining, cmd));
    }

    // Try WITH clause (CTE) parsing
    let lower_input = input.to_lowercase();
    let (input, ctes) = if lower_input.starts_with("with")
        && lower_input
            .chars()
            .nth(4)
            .map(|c| c.is_whitespace())
            .unwrap_or(false)
    {
        let (remaining, (cte_defs, _is_recursive)) = cte::parse_with_clause(input)?;
        let (remaining, _) = multispace0(remaining)?;
        (remaining, cte_defs)
    } else {
        (input, vec![])
    };

    let (input, (action, distinct)) = parse_action(input)?;
    // Accept either "::" or whitespace as separator (supports add::table and add table)
    let (input, _) = alt((
        value((), tag("::")),
        value((), multispace1),
    )).parse(input)?;

    // Supports expressions like: CASE WHEN ... END, functions, columns
    let (input, distinct_on) = if distinct {
        // If already parsed "get distinct", check for "on (...)"
        if let Ok((remaining, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("on").parse(input)
        {
            let (remaining, _) = multispace0(remaining)?;
            let (remaining, exprs) = nom::sequence::delimited(
                nom::character::complete::char('('),
                nom::multi::separated_list1(
                    (
                        multispace0,
                        nom::character::complete::char(','),
                        multispace0,
                    ),
                    expressions::parse_expression,
                ),
                nom::character::complete::char(')'),
            )
            .parse(remaining)?;
            let (remaining, _) = multispace1(remaining)?;
            (remaining, exprs)
        } else {
            (input, vec![])
        }
    } else {
        (input, vec![])
    };

    //  Parse table name
    let (input, table) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;

    // For MAKE (CREATE TABLE): parse column definitions
    if matches!(action, Action::Make) {
        return parse_create_table(input, table);
    }

    let (input, joins) = many0(parse_join_clause).parse(input)?;
    let (input, _) = multispace0(input)?;

    // For SET/UPDATE: parse "values col = val, col2 = val2" before fields
    let (input, set_cages) = if matches!(action, Action::Set) {
        opt(parse_values_clause).parse(input)?
    } else {
        (input, None)
    };
    let (input, _) = multispace0(input)?;

    let (input, columns) = opt(parse_fields_clause).parse(input)?;
    let (input, _) = multispace0(input)?;

    // For ADD/INSERT: try "from (get ...)" first, then fall back to "values val1, val2"
    let (input, source_query) = if matches!(action, Action::Add) {
        opt(dml::parse_source_query).parse(input)?
    } else {
        (input, None)
    };
    let (input, _) = multispace0(input)?;

    // Only parse values if no source_query (INSERT...SELECT takes precedence)
    let (input, add_cages) = if source_query.is_none() && matches!(action, Action::Add) {
        opt(dml::parse_insert_values).parse(input)?
    } else {
        (input, None)
    };
    let (input, _) = multispace0(input)?;

    let (input, where_cages) = opt(parse_where_clause).parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, having) = opt(parse_having_clause).parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, on_conflict) = if matches!(action, Action::Add) {
        opt(dml::parse_on_conflict).parse(input)?
    } else {
        (input, None)
    };
    let (input, _) = multispace0(input)?;

    let (input, order_cages) = opt(parse_order_by_clause).parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, limit_cage) = opt(parse_limit_clause).parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, offset_cage) = opt(parse_offset_clause).parse(input)?;

    let mut cages = Vec::new();

    // For SET, values come first (as Payload cage)
    if let Some(sc) = set_cages {
        cages.push(sc);
    }

    // For ADD, values come as Payload cage too
    if let Some(ac) = add_cages {
        cages.push(ac);
    }

    if let Some(wc) = where_cages {
        cages.extend(wc);
    }
    if let Some(oc) = order_cages {
        cages.extend(oc);
    }
    if let Some(lc) = limit_cage {
        cages.push(lc);
    }
    if let Some(oc) = offset_cage {
        cages.push(oc);
    }

    Ok((
        input,
        Qail {
            action,
            table: table.to_string(),
            columns: columns.unwrap_or_else(|| vec![Expr::Star]),
            joins,
            cages,
            distinct,
            distinct_on,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: having.unwrap_or_default(),
            group_by_mode: GroupByMode::default(),
            returning: None,
            ctes,
            on_conflict,
            source_query,
            channel: None,
            payload: None,
            savepoint_name: None,
            from_tables: vec![],
            using_tables: vec![],
            lock_mode: None,
            fetch: None,
            default_values: false,
            overriding: None,
            sample: None,
            only_table: false,
            vector: None,
            score_threshold: None,
            vector_name: None,
            with_vector: false,
            vector_size: None,
            distance: None,
            on_disk: None,
            function_def: None,
            trigger_def: None,
        },
    ))
}

/// Strip SQL comments from input (both -- line comments and /* */ block comments)
fn strip_sql_comments(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '-' && chars.peek() == Some(&'-') {
            // Line comment: skip until end of line
            chars.next(); // consume second -
            while let Some(&nc) = chars.peek() {
                if nc == '\n' {
                    result.push('\n'); // preserve newline
                    chars.next();
                    break;
                }
                chars.next();
            }
        } else if c == '/' && chars.peek() == Some(&'*') {
            // Block comment: skip until */
            chars.next(); // consume *
            while let Some(nc) = chars.next() {
                if nc == '*' && chars.peek() == Some(&'/') {
                    chars.next(); // consume /
                    result.push(' '); // replace with space to preserve separation
                    break;
                }
            }
        } else {
            result.push(c);
        }
    }

    result
}
