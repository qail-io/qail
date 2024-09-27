use crate::ast::values::IntervalUnit;
use crate::ast::*;
use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, digit1, multispace1},
    combinator::{map, opt, recognize, value},
    sequence::{delimited, preceded},
};

/// Parse checking identifier (table name, column name, or qualified name like table.column)
pub fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.').parse(input)
}

/// Parse interval shorthand: 24h, 7d, 1w, 30m, 6mo, 1y
pub fn parse_interval(input: &str) -> IResult<&str, Value> {
    let (input, num_str) = digit1(input)?;
    let amount: i64 = num_str.parse().unwrap_or(0);

    let (input, unit) = alt((
        value(IntervalUnit::Second, tag_no_case("s")),
        value(IntervalUnit::Minute, tag_no_case("m")),
        value(IntervalUnit::Hour, tag_no_case("h")),
        value(IntervalUnit::Day, tag_no_case("d")),
        value(IntervalUnit::Week, tag_no_case("w")),
        value(IntervalUnit::Month, tag_no_case("mo")),
        value(IntervalUnit::Year, tag_no_case("y")),
    ))
    .parse(input)?;

    Ok((input, Value::Interval { amount, unit }))
}

/// Parse value: string, number, bool, null, $param, :named_param, interval
pub fn parse_value(input: &str) -> IResult<&str, Value> {
    alt((
        // Parameter: $1, $2
        map(preceded(char('$'), digit1), |d: &str| {
            Value::Param(d.parse().unwrap_or(0))
        }),
        // Named parameter: :name, :id, :user_id
        map(
            preceded(
                char(':'),
                take_while1(|c: char| c.is_alphanumeric() || c == '_'),
            ),
            |name: &str| Value::NamedParam(name.to_string()),
        ),
        // Boolean
        value(Value::Bool(true), tag_no_case("true")),
        value(Value::Bool(false), tag_no_case("false")),
        // Null
        value(Value::Null, tag_no_case("null")),
        // String (double quoted) - allow empty strings
        map(
            delimited(
                char('"'),
                nom::bytes::complete::take_while(|c| c != '"'),
                char('"'),
            ),
            |s: &str| Value::String(s.to_string()),
        ),
        // String (single quoted) - allow empty strings
        map(
            delimited(
                char('\''),
                nom::bytes::complete::take_while(|c| c != '\''),
                char('\''),
            ),
            |s: &str| Value::String(s.to_string()),
        ),
        // Float (must check before int)
        map(
            recognize((opt(char('-')), digit1, char('.'), digit1)),
            |s: &str| Value::Float(s.parse().unwrap_or(0.0)),
        ),
        // Interval shorthand before plain integers: 24h, 7d, 1w
        parse_interval,
        // Integer (last, after interval)
        map(recognize((opt(char('-')), digit1)), |s: &str| {
            Value::Int(s.parse().unwrap_or(0))
        }),
    ))
    .parse(input)
}

/// Parse comparison operator
pub fn parse_operator(input: &str) -> IResult<&str, Operator> {
    alt((
        // Multi-char operators first
        value(Operator::NotBetween, tag_no_case("not between")),
        value(Operator::Between, tag_no_case("between")),
        value(Operator::IsNotNull, tag_no_case("is not null")),
        value(Operator::IsNull, tag_no_case("is null")),
        value(Operator::NotIn, tag_no_case("not in")),
        value(Operator::NotILike, tag_no_case("not ilike")),
        value(Operator::NotLike, tag_no_case("not like")),
        value(Operator::ILike, tag_no_case("ilike")),
        value(Operator::Like, tag_no_case("like")),
        value(Operator::In, tag_no_case("in")),
        value(Operator::Gte, tag(">=")),
        value(Operator::Lte, tag("<=")),
        value(Operator::Ne, tag("!=")),
        value(Operator::Ne, tag("<>")),
        // Single char operators
        value(Operator::Eq, tag("=")),
        value(Operator::Gt, tag(">")),
        value(Operator::Lt, tag("<")),
        value(Operator::Fuzzy, tag("~")),
    ))
    .parse(input)
}

/// Parse action keyword: get, set, del, add, make
pub fn parse_action(input: &str) -> IResult<&str, (Action, bool)> {
    alt((
        // get distinct
        map(
            (tag_no_case("get"), multispace1, tag_no_case("distinct")),
            |_| (Action::Get, true),
        ),
        // get
        value((Action::Get, false), tag_no_case("get")),
        // set
        value((Action::Set, false), tag_no_case("set")),
        // del / delete
        alt((
            value((Action::Del, false), tag_no_case("delete")),
            value((Action::Del, false), tag_no_case("del")),
        )),
        // add / insert
        alt((
            value((Action::Add, false), tag_no_case("insert")),
            value((Action::Add, false), tag_no_case("add")),
        )),
        // make / create
        alt((
            value((Action::Make, false), tag_no_case("create")),
            value((Action::Make, false), tag_no_case("make")),
        )),
    ))
    .parse(input)
}

/// Parse transaction commands: begin, commit, rollback
pub fn parse_txn_command(input: &str) -> IResult<&str, Qail> {
    let (input, action) = alt((
        value(Action::TxnStart, tag_no_case("begin")),
        value(Action::TxnCommit, tag_no_case("commit")),
        value(Action::TxnRollback, tag_no_case("rollback")),
    ))
    .parse(input)?;

    Ok((
        input,
        Qail {
            action,
            table: String::new(),
            columns: vec![],
            joins: vec![],
            cages: vec![],
            distinct: false,
            distinct_on: vec![],
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::default(),
            ctes: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
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
