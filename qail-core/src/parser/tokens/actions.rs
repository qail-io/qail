use nom::{
    branch::alt,
    bytes::complete::tag,
    combinator::value,
    IResult,
};
use crate::ast::*;

/// Parse the action (get, set, del, add, gen, make, mod, over, with).
pub fn parse_action(input: &str) -> IResult<&str, Action> {
    alt((
        value(Action::Get, tag("get")),
        value(Action::Set, tag("set")),
        value(Action::Del, tag("del")),
        value(Action::Add, tag("add")),
        value(Action::Gen, tag("gen")),
        value(Action::Make, tag("make")),
        value(Action::Mod, tag("mod")),
        value(Action::Over, tag("over")),
        value(Action::With, tag("with")), // Listed twice in original, one is enough
        value(Action::Index, tag("index")),
        // Transactions
        value(Action::TxnStart, tag("txn")), 
        value(Action::Put, tag("put")),
        value(Action::DropCol, tag("drop")),
        value(Action::RenameCol, tag("rename")),
        // Additional clauses
        value(Action::JsonTable, tag("jtable")),
    ))(input)
}
