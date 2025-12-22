use nom::{
    branch::alt,
    bytes::complete::tag,
    combinator::value,
    IResult,
};
use crate::ast::*;

pub fn parse_agg_func(input: &str) -> IResult<&str, AggregateFunc> {
    alt((
        value(AggregateFunc::Count, tag("count")),
        value(AggregateFunc::Sum, tag("sum")),
        value(AggregateFunc::Avg, tag("avg")),
        value(AggregateFunc::Min, tag("min")),
        value(AggregateFunc::Max, tag("max")),
    ))(input)
}
