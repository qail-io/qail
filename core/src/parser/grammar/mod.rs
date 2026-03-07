/// Base parsing utilities (identifiers, literals, whitespace).
pub mod base;
/// Binary operator parsing (AND, OR, arithmetic).
pub mod binary_ops;
/// CASE WHEN expression parsing.
pub mod case_when;
/// Clause parsing (WHERE, ORDER BY, LIMIT, etc.).
pub mod clauses;
/// Common Table Expression (WITH) parsing.
pub mod cte;
/// Data Definition Language parsing (CREATE TABLE, INDEX).
pub mod ddl;
/// Data Manipulation Language parsing (INSERT values, ON CONFLICT).
pub mod dml;
/// Expression parsing (columns, functions, sub-expressions).
pub mod expressions;
/// Function call parsing.
pub mod functions;
/// JOIN clause parsing.
pub mod joins;
/// Special function parsing (COALESCE, NULLIF, GREATEST, etc.).
pub mod special_funcs;

use self::base::*;
use self::clauses::*;
use self::ddl::*;
use self::dml::*;
use self::joins::*;
use crate::ast::*;
use nom::{
    IResult, Parser,
    bytes::complete::tag_no_case,
    character::complete::{multispace0, multispace1},
    combinator::opt,
    multi::many0,
};
// use self::expressions::*; // Used in clauses module

/// Parse a QAIL query with comment preprocessing.
/// This is the recommended entry point - handles SQL comment stripping
/// and `table[filter]` shorthand desugaring.
pub fn parse(input: &str) -> Result<Qail, String> {
    let cleaned = strip_sql_comments(input);
    // Desugar table[filter] shorthand: "set users[active = true] fields ..."
    // → "set users fields ... where active = true"
    let desugared = desugar_bracket_filter(&cleaned);
    match parse_root(&desugared) {
        Ok(("", cmd)) => Ok(cmd),
        Ok((remaining, _)) => Err(format!("Unexpected trailing content: '{}'", remaining)),
        Err(e) => Err(format!("Parse error: {:?}", e)),
    }
}

/// Desugar `table[filter]` shorthand into `table ... where filter`.
/// Transforms: `action table[cond] rest` → `action table rest where cond`
fn desugar_bracket_filter(input: &str) -> String {
    let trimmed = input.trim();
    // Find the opening bracket after the table name
    // Must be: action<ws>table[...] — the [ must immediately follow the table name
    if let Some(bracket_start) = trimmed.find('[') {
        // Ensure the bracket is in the table position (after action + space + identifier)
        let before_bracket = &trimmed[..bracket_start];
        // There should be at least "action table" before the bracket
        if !before_bracket.contains(' ') {
            return trimmed.to_string();
        }

        // Guard: don't treat brackets in clauses/values as table shorthand.
        // Example to avoid: `... where tags && '["a","b"]'`
        let before_lower = before_bracket.to_ascii_lowercase();
        if before_lower.contains(" where ")
            || before_lower.contains(" fields ")
            || before_lower.contains(" having ")
            || before_lower.contains(" order ")
            || before_lower.contains(" limit ")
            || before_lower.contains(" offset ")
            || before_lower.contains(" join ")
        {
            return trimmed.to_string();
        }

        // Find matching closing bracket, respecting nesting and quotes
        let after_bracket = &trimmed[bracket_start + 1..];
        let mut depth = 1;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut bracket_end = None;

        for (i, c) in after_bracket.char_indices() {
            match c {
                '\'' if !in_double_quote => in_single_quote = !in_single_quote,
                '"' if !in_single_quote => in_double_quote = !in_double_quote,
                '[' if !in_single_quote && !in_double_quote => depth += 1,
                ']' if !in_single_quote && !in_double_quote => {
                    depth -= 1;
                    if depth == 0 {
                        bracket_end = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }

        if let Some(end_pos) = bracket_end {
            let filter = &after_bracket[..end_pos];
            let rest = &after_bracket[end_pos + 1..].trim();

            // Check if there's already a "where" in the rest
            let rest_lower = rest.to_lowercase();
            if rest_lower.contains("where ") || rest_lower.contains("where\n") {
                // Already has WHERE — append with AND
                return format!("{} {} AND {}", before_bracket, rest, filter);
            } else if rest.is_empty() {
                return format!("{} where {}", before_bracket, filter);
            } else {
                return format!("{} {} where {}", before_bracket, rest, filter);
            }
        }
    }
    trimmed.to_string()
}

/// Parse a QAIL query (root entry point).
/// Note: Does NOT strip comments. Use `parse()` for automatic comment handling.
pub fn parse_root(input: &str) -> IResult<&str, Qail> {
    let input = input.trim();

    // Try transaction commands first (single keywords)
    if let Ok((remaining, cmd)) = parse_txn_command(input) {
        return Ok((remaining, cmd));
    }

    // Parse procedural/session commands that don't follow `action table ...`
    if let Ok((remaining, cmd)) = parse_procedural_command(input) {
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
    // v2 syntax only: whitespace separator between action and table
    let (input, _) = multispace1(input)?;

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
            skip_locked: false,
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
            let mut closed = false;
            while let Some(nc) = chars.next() {
                if nc == '*' && chars.peek() == Some(&'/') {
                    chars.next(); // consume /
                    result.push(' '); // replace with space to preserve separation
                    closed = true;
                    break;
                }
            }
            if !closed {
                // Unclosed block comment — preserve raw text so parser reports error
                result.push_str("/*");
            }
        } else {
            result.push(c);
        }
    }

    result
}
