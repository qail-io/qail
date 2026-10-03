//! Expression parsing coordinator.
//!
//! This module provides the main entry point for parsing SQL expressions
//! and coordinates parsing by importing from specialized submodules:
//! - `binary_ops`: Binary operator chains (+, -, *, /, ||)
//! - `functions`: Function calls and aggregates (COUNT, SUM, FILTER)
//! - `case_when`: CASE WHEN expressions
//! - `special_funcs`: SUBSTRING, EXTRACT, TRIM with keyword syntax

use super::base::{parse_identifier, parse_value};
use crate::ast::*;
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::multispace0,
    combinator::{map, opt},
    sequence::{delimited, preceded},
};

// Re-export from submodules for internal use
pub use super::binary_ops::{parse_additive_expr, parse_concat_expr, parse_multiplicative_expr};
pub use super::case_when::parse_case;
pub use super::functions::{parse_function_arg, parse_function_or_aggregate};
pub use super::special_funcs::parse_special_function;

pub(super) fn value_to_expr(value: Value) -> Expr {
    match value {
        Value::Expr(expr) => *expr,
        Value::Column(column) => Expr::Named(column),
        Value::Subquery(query) => Expr::Subquery { query, alias: None },
        value => Expr::Literal(value),
    }
}

/// Parse a general expression.
/// Handles binary operators with precedence:
/// - Low: || (concat)  
/// - Medium: + -
/// - High: * / %
pub fn parse_expression(input: &str) -> IResult<&str, Expr> {
    parse_concat_expr(input)
}

/// Parse an expression with optional AS alias
/// e.g., `column`, `CASE...END AS name`, `func(...) AS alias`
pub fn parse_expression_with_alias(input: &str) -> IResult<&str, Expr> {
    let (input, expr) = parse_expression(input)?;
    let (input, _) = multispace0(input)?;

    if let Ok((remaining, _)) = tag_no_case::<_, _, nom::error::Error<&str>>("as").parse(input) {
        let (remaining, _) = nom::character::complete::multispace1(remaining)?;
        let (remaining, alias) = parse_identifier(remaining)?;
        return Ok((remaining, alias_or_fail(input, expr, alias)?));
    }

    Ok((input, expr))
}

/// Attach `alias`, or fail hard when the expression has no alias slot:
/// dropping the alias would silently rename the output column.
pub(super) fn alias_or_fail<'a>(
    at: &'a str,
    mut expr: Expr,
    alias: &str,
) -> Result<Expr, nom::Err<nom::error::Error<&'a str>>> {
    // `expr as a as b`: the second alias would replace the first.
    if expr.alias_name().is_some() || !expr.set_alias(alias) {
        return Err(hard_failure(at));
    }
    Ok(expr)
}

fn hard_failure(at: &str) -> nom::Err<nom::error::Error<&str>> {
    nom::Err::Failure(nom::error::Error::new(at, nom::error::ErrorKind::Verify))
}

/// Parse identifier or JSON access or type cast.
/// JSON access: col->'key' or col->>'key' or chained col->'a'->0->>'b'
/// Subscript / slice: col[1], f(x)[1], col[1:3], col[:2]
/// Type cast: expr::type, expr::numeric(12,2), expr::double precision, expr::text[]
pub fn parse_json_or_ident(input: &str) -> IResult<&str, Expr> {
    let (input, atom) = parse_atom(input)?;
    let (mut input, atom) = parse_subscripts(input, atom)?;

    // For JSON access, we need the base column name
    let col_name = match &atom {
        Expr::Named(name) => Some(name.clone()),
        _ => None,
    };

    // Collect path segments for chained JSON access
    let mut path_segments: Vec<(JsonPathSegment, bool)> = Vec::new();

    loop {
        let (remaining, json_op) = opt(alt((tag("->>"), tag("->")))).parse(input)?;

        if let Some(op) = json_op {
            let (remaining, _) = multispace0(remaining)?;
            let (remaining, key_val) = parse_value(remaining)?;

            // A quoted operand is an object key even when it looks numeric.
            let path = match key_val {
                Value::String(s) => JsonPathSegment::Key(s),
                Value::Int(n) => JsonPathSegment::Index(n),
                other => JsonPathSegment::from_path_text(&other.to_string()),
            };

            path_segments.push((path, op == "->>"));
            input = remaining;
        } else {
            break;
        }
    }

    let mut expr = if !path_segments.is_empty() {
        if let Some(column) = col_name {
            Expr::JsonAccess {
                column,
                path_segments,
                alias: None,
            }
        } else {
            // JsonAccess only roots at a column; keeping `atom` would drop the path.
            return Err(hard_failure(input));
        }
    } else {
        atom
    };

    // Chained casts: x::numeric(12,2)::text
    while let (rest, Some(target_type)) = opt(preceded(tag("::"), parse_cast_type)).parse(input)? {
        expr = Expr::Cast {
            expr: Box::new(expr),
            target_type,
            alias: None,
        };
        input = rest;
    }

    Ok((input, expr))
}

/// Postfix `[index]` / `[lower:upper]` chains written directly after an atom.
fn parse_subscripts(mut input: &str, mut expr: Expr) -> IResult<&str, Expr> {
    use nom::character::complete::char;

    while let Ok((after_open, _)) = char::<_, nom::error::Error<&str>>('[').parse(input) {
        let (rest, _) = multispace0(after_open)?;
        // `[:name` reads both as a slice to column `name` and as an index by
        // the named parameter `:name`; refuse rather than pick one.
        if rest
            .strip_prefix(':')
            .and_then(|after| after.chars().next())
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        {
            return Err(hard_failure(rest));
        }
        let (rest, lower) = if rest.starts_with(':') {
            (rest, None)
        } else {
            opt(parse_expression).parse(rest)?
        };
        let (rest, _) = multispace0(rest)?;
        let (rest, colon) = opt(char(':')).parse(rest)?;
        let (rest, _) = multispace0(rest)?;
        let (rest, upper) = if colon.is_some() {
            opt(parse_expression).parse(rest)?
        } else {
            (rest, None)
        };
        let (rest, _) = multispace0(rest)?;
        let (rest, _) = char(']').parse(rest)?;

        expr = match (colon, lower) {
            (None, Some(index)) => Expr::Subscript {
                expr: Box::new(expr),
                index: Box::new(index),
                alias: None,
            },
            (None, None) => return Err(hard_failure(input)),
            (Some(_), lower) => Expr::ArraySlice {
                expr: Box::new(expr),
                lower: lower.map(Box::new),
                upper: upper.map(Box::new),
                alias: None,
            },
        };
        input = rest;
    }
    Ok((input, expr))
}

/// Cast target: `name[.name]`, PostgreSQL multiword names (`double precision`,
/// `character varying`, `timestamp with time zone`, ...), an optional typmod
/// `(n[, m])`, and `[]` array suffixes. Returns normalized text.
fn parse_cast_type(input: &str) -> IResult<&str, String> {
    use nom::character::complete::{char, digit1, multispace1};

    let (mut input, base) = parse_identifier(input)?;
    let mut text = base.to_string();
    let lower = base.to_ascii_lowercase();

    let word = |input, w: &'static str| -> IResult<&str, &str> {
        preceded(multispace1, tag_no_case(w)).parse(input)
    };

    match lower.as_str() {
        "double" => {
            if let Ok((rest, _)) = word(input, "precision") {
                text.push_str(" precision");
                input = rest;
            }
        }
        "character" | "char" | "bit" => {
            if let Ok((rest, _)) = word(input, "varying") {
                text.push_str(" varying");
                input = rest;
            }
        }
        _ => {}
    }

    let typmod = |input| -> IResult<&str, String> {
        let (input, _) = (multispace0, char('('), multispace0).parse(input)?;
        let (input, first) = digit1(input)?;
        let (input, second) =
            opt(preceded((multispace0, char(','), multispace0), digit1)).parse(input)?;
        let (input, _) = (multispace0, char(')')).parse(input)?;
        Ok((
            input,
            match second {
                Some(second) => format!("({first},{second})"),
                None => format!("({first})"),
            },
        ))
    };
    if let Ok((rest, modifier)) = typmod(input) {
        text.push_str(&modifier);
        input = rest;
    }

    if matches!(lower.as_str(), "timestamp" | "time") {
        for (lead, rendered) in [
            ("with", " with time zone"),
            ("without", " without time zone"),
        ] {
            let zone: IResult<&str, _> = (
                multispace1,
                tag_no_case(lead),
                multispace1,
                tag_no_case("time"),
                multispace1,
                tag_no_case("zone"),
            )
                .parse(input);
            if let Ok((rest, _)) = zone {
                text.push_str(rendered);
                input = rest;
                break;
            }
        }
    }

    while let Ok((rest, _)) = (char::<_, nom::error::Error<&str>>('['), char(']')).parse(input) {
        text.push_str("[]");
        input = rest;
    }

    Ok((input, text))
}

/// Parse a parenthesized expression: `(expr)`, or a boolean expression
/// `(a > 1 and not b = 2)` built from comparisons, AND, OR and NOT.
fn parse_grouped_expr(input: &str) -> IResult<&str, Expr> {
    use nom::character::complete::multispace0;

    delimited(
        (nom::character::complete::char('('), multispace0),
        parse_bool_or,
        (multispace0, nom::character::complete::char(')')),
    )
    .parse(input)
}

fn boolean_chain<'a>(
    input: &'a str,
    keyword: &'static str,
    op: BinaryOp,
    operand: fn(&'a str) -> IResult<&'a str, Expr>,
) -> IResult<&'a str, Expr> {
    use nom::character::complete::multispace1;

    let (mut input, mut left) = operand(input)?;
    loop {
        let separator: IResult<&str, _> =
            (multispace1, tag_no_case(keyword), multispace1).parse(input);
        let Ok((rest, _)) = separator else {
            break;
        };
        let (rest, right) = operand(rest)?;
        left = Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
            alias: None,
        };
        input = rest;
    }
    Ok((input, left))
}

fn parse_bool_or(input: &str) -> IResult<&str, Expr> {
    boolean_chain(input, "or", BinaryOp::Or, parse_bool_and)
}

fn parse_bool_and(input: &str) -> IResult<&str, Expr> {
    boolean_chain(input, "and", BinaryOp::And, parse_bool_not)
}

fn parse_bool_not(input: &str) -> IResult<&str, Expr> {
    if let Ok((rest, _)) = (
        tag_no_case::<_, _, nom::error::Error<&str>>("not"),
        nom::character::complete::multispace1,
    )
        .parse(input)
    {
        let (rest, inner) = parse_bool_not(rest)?;
        return Ok((rest, negate(inner)));
    }
    parse_comparison(input)
}

/// `NOT x` as `(x = FALSE)`: identical for booleans, including NULL -> NULL.
pub(super) fn negate(expr: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(expr),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(Value::Bool(false))),
        alias: None,
    }
}

fn parse_comparison(input: &str) -> IResult<&str, Expr> {
    let (input, left) = parse_expression(input)?;
    let (after_ws, _) = multispace0(input)?;
    let comparison: IResult<&str, BinaryOp> = alt((
        nom::combinator::value(BinaryOp::Ne, tag("<>")),
        nom::combinator::value(BinaryOp::Ne, tag("!=")),
        nom::combinator::value(BinaryOp::Lte, tag("<=")),
        nom::combinator::value(BinaryOp::Gte, tag(">=")),
        nom::combinator::value(BinaryOp::Eq, tag("=")),
        nom::combinator::value(BinaryOp::Lt, tag("<")),
        nom::combinator::value(BinaryOp::Gt, tag(">")),
    ))
    .parse(after_ws);
    let Ok((rest, op)) = comparison else {
        return Ok((input, left));
    };
    let (rest, _) = multispace0(rest)?;
    let (rest, right) = parse_expression(rest)?;
    Ok((
        rest,
        Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
            alias: None,
        },
    ))
}

/// Parse atomic expressions (functions, case, literals, identifiers, wildcards, grouped)
fn parse_atom(input: &str) -> IResult<&str, Expr> {
    alt((
        parse_grouped_expr, // Try (expr) first
        parse_case,
        parse_special_function,
        parse_function_or_aggregate,
        parse_star,
        parse_literal,
        parse_qualified_star,
        parse_simple_ident,
    ))
    .parse(input)
}

fn parse_star(input: &str) -> IResult<&str, Expr> {
    map(tag("*"), |_| Expr::Star).parse(input)
}

/// `table.*` / `schema.table.*`: kept as `Expr::Named("table.*")`, the
/// qualified-wildcard form every renderer and policy check recognizes.
fn parse_qualified_star(input: &str) -> IResult<&str, Expr> {
    let (rest, dotted) = nom::bytes::complete::take_while1(|c: char| {
        c.is_ascii_alphanumeric() || c == '_' || c == '.'
    })
    .parse(input)?;
    let (rest, _) = tag("*").parse(rest)?;
    let qualifier = dotted.strip_suffix('.').ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    match parse_identifier(qualifier) {
        Ok(("", _)) => Ok((rest, Expr::Named(format!("{qualifier}.*")))),
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        ))),
    }
}

/// Parse literal values (strings, numbers, named params) as expressions
fn parse_literal(input: &str) -> IResult<&str, Expr> {
    map(parse_value, value_to_expr).parse(input)
}

fn parse_simple_ident(input: &str) -> IResult<&str, Expr> {
    map(parse_identifier, |s| Expr::Named(s.to_string())).parse(input)
}
