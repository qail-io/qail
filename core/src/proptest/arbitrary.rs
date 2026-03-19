//! Arbitrary implementations for AST types.
//!
//! Provides proptest strategies for generating random AST nodes.

use crate::ast::values::IntervalUnit;
use crate::ast::*;
use proptest::prelude::*;

/// Generate valid SQL identifiers (alphanumeric + underscore, starting with letter)
pub fn arb_identifier() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,15}".prop_map(|s| s.to_string())
}

/// Strategy for IntervalUnit
pub fn arb_interval_unit() -> impl Strategy<Value = IntervalUnit> {
    prop_oneof![
        Just(IntervalUnit::Second),
        Just(IntervalUnit::Minute),
        Just(IntervalUnit::Hour),
        Just(IntervalUnit::Day),
        Just(IntervalUnit::Week),
        Just(IntervalUnit::Month),
        Just(IntervalUnit::Year),
    ]
}

/// Strategy for Operator (comparison operators)
pub fn arb_operator() -> impl Strategy<Value = Operator> {
    prop_oneof![
        10 => Just(Operator::Eq),
        10 => Just(Operator::Ne),
        5 => Just(Operator::Gt),
        5 => Just(Operator::Gte),
        5 => Just(Operator::Lt),
        5 => Just(Operator::Lte),
        3 => Just(Operator::In),
        3 => Just(Operator::NotIn),
        3 => Just(Operator::IsNull),
        3 => Just(Operator::IsNotNull),
        2 => Just(Operator::Like),
        2 => Just(Operator::ILike),
        2 => Just(Operator::NotLike),
        2 => Just(Operator::NotILike),
        1 => Just(Operator::Between),
        1 => Just(Operator::NotBetween),
        1 => Just(Operator::Fuzzy),
        1 => Just(Operator::Contains),
        1 => Just(Operator::Regex),
        1 => Just(Operator::RegexI),
    ]
}

/// Strategy for SortOrder
pub fn arb_sort_order() -> impl Strategy<Value = SortOrder> {
    prop_oneof![
        Just(SortOrder::Asc),
        Just(SortOrder::Desc),
        Just(SortOrder::AscNullsFirst),
        Just(SortOrder::AscNullsLast),
        Just(SortOrder::DescNullsFirst),
        Just(SortOrder::DescNullsLast),
    ]
}

/// Strategy for AggregateFunc
pub fn arb_aggregate_func() -> impl Strategy<Value = AggregateFunc> {
    prop_oneof![
        Just(AggregateFunc::Count),
        Just(AggregateFunc::Sum),
        Just(AggregateFunc::Avg),
        Just(AggregateFunc::Min),
        Just(AggregateFunc::Max),
        Just(AggregateFunc::ArrayAgg),
        Just(AggregateFunc::StringAgg),
        Just(AggregateFunc::JsonAgg),
        Just(AggregateFunc::JsonbAgg),
        Just(AggregateFunc::BoolAnd),
        Just(AggregateFunc::BoolOr),
    ]
}

/// Strategy for LogicalOp
pub fn arb_logical_op() -> impl Strategy<Value = LogicalOp> {
    prop_oneof![Just(LogicalOp::And), Just(LogicalOp::Or),]
}

/// Strategy for Value (non-recursive variants only)
/// Filter floats to use normal range to avoid precision loss in JSON roundtrip
fn is_safe_float(f: &f64) -> bool {
    f.is_finite() && f.abs() > 1e-100 && f.abs() < 1e100
}

pub fn arb_value_leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        5 => Just(Value::Null),
        10 => any::<bool>().prop_map(Value::Bool),
        10 => any::<i64>().prop_map(Value::Int),
        5 => any::<f64>().prop_filter("safe floats", is_safe_float).prop_map(Value::Float),
        10 => "[a-zA-Z0-9 _-]{0,50}".prop_map(Value::String),
        5 => (1usize..100).prop_map(Value::Param),
        3 => arb_identifier().prop_map(Value::NamedParam),
        3 => arb_identifier().prop_map(Value::Column),
        2 => (1i64..1000, arb_interval_unit()).prop_map(|(amount, unit)| Value::Interval { amount, unit }),
    ]
}

/// Strategy for simple Expr (non-recursive)
pub fn arb_expr_leaf() -> impl Strategy<Value = Expr> {
    prop_oneof![
        5 => Just(Expr::Star),
        20 => arb_identifier().prop_map(Expr::Named),
        10 => (arb_identifier(), arb_identifier()).prop_map(|(name, alias)| Expr::Aliased { name, alias }),
        5 => arb_value_leaf().prop_map(Expr::Literal),
    ]
}

/// Strategy for Condition
pub fn arb_condition() -> impl Strategy<Value = Condition> {
    (arb_expr_leaf(), arb_operator(), arb_value_leaf()).prop_map(|(left, op, value)| Condition {
        left,
        op,
        value,
        is_array_unnest: false,
    })
}

/// Strategy for CageKind
pub fn arb_cage_kind() -> impl Strategy<Value = CageKind> {
    prop_oneof![
        Just(CageKind::Filter),
        (1usize..1000).prop_map(CageKind::Limit),
        (0usize..1000).prop_map(CageKind::Offset),
        arb_sort_order().prop_map(CageKind::Sort),
    ]
}

/// Strategy for Cage
pub fn arb_cage() -> impl Strategy<Value = Cage> {
    (
        arb_cage_kind(),
        proptest::collection::vec(arb_condition(), 0..3),
        arb_logical_op(),
    )
        .prop_map(|(kind, conditions, logical_op)| Cage {
            kind,
            conditions,
            logical_op,
        })
}

/// Strategy for simple Qail queries (GET only for SQL output tests)
pub fn arb_qail_get() -> impl Strategy<Value = Qail> {
    (
        arb_identifier(),
        proptest::collection::vec(arb_expr_leaf(), 1..5),
        proptest::collection::vec(arb_cage(), 0..3),
    )
        .prop_map(|(table, columns, cages)| Qail {
            action: Action::Get,
            table,
            columns,
            cages,
            ..Default::default()
        })
}
