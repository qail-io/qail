//! Aggregate call shape shared by the transpiler and the native encoder:
//! expression arguments, aggregate-local ORDER BY and WITHIN GROUP.

use crate::ast::{
    AggregateFunc, Cage, CageKind, Condition, Expr, LogicalOp, Operator, SortOrder, Value,
};

impl AggregateFunc {
    /// The variant for a SQL function name (any case), if `Expr::Aggregate`
    /// models it.
    pub fn from_sql_name(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "count" => Self::Count,
            "sum" => Self::Sum,
            "avg" => Self::Avg,
            "min" => Self::Min,
            "max" => Self::Max,
            "array_agg" => Self::ArrayAgg,
            "string_agg" => Self::StringAgg,
            "json_agg" => Self::JsonAgg,
            "jsonb_agg" => Self::JsonbAgg,
            "bool_and" => Self::BoolAnd,
            "bool_or" => Self::BoolOr,
            "percentile_cont" => Self::PercentileCont,
            "percentile_disc" => Self::PercentileDisc,
            "mode" => Self::Mode,
            _ => return None,
        })
    }

    /// Lower-case spelling used by the text DSL.
    pub fn dsl_name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::ArrayAgg => "array_agg",
            Self::StringAgg => "string_agg",
            Self::JsonAgg => "json_agg",
            Self::JsonbAgg => "jsonb_agg",
            Self::BoolAnd => "bool_and",
            Self::BoolOr => "bool_or",
            Self::PercentileCont => "percentile_cont",
            Self::PercentileDisc => "percentile_disc",
            Self::Mode => "mode",
        }
    }

    /// Ordered-set aggregates read their sorted input from WITHIN GROUP.
    pub fn is_ordered_set(self) -> bool {
        matches!(
            self,
            Self::PercentileCont | Self::PercentileDisc | Self::Mode
        )
    }
}

/// One aggregate ORDER BY / WITHIN GROUP key as the cage the AST stores.
pub fn aggregate_sort_cage(key: Expr, order: SortOrder) -> Cage {
    Cage {
        kind: CageKind::Sort(order),
        conditions: vec![Condition {
            left: key,
            op: Operator::Eq,
            value: Value::Null,
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    }
}

/// Key and direction of one aggregate ORDER BY / WITHIN GROUP cage.
///
/// Only the cage shape [`aggregate_sort_cage`] builds is accepted: a cage
/// with another kind or a second condition would otherwise be partly ignored.
pub fn aggregate_sort_key(cage: &Cage) -> Result<(&Expr, SortOrder), String> {
    let CageKind::Sort(order) = cage.kind else {
        return Err("aggregate ORDER BY entries must be sort cages".to_string());
    };
    match cage.conditions.as_slice() {
        [condition] => Ok((&condition.left, order)),
        _ => Err("aggregate ORDER BY entries need exactly one sort key".to_string()),
    }
}

/// SQL direction suffix, including the leading space.
pub fn sort_order_sql(order: SortOrder) -> &'static str {
    match order {
        SortOrder::Asc => " ASC",
        SortOrder::Desc => " DESC",
        SortOrder::AscNullsFirst => " ASC NULLS FIRST",
        SortOrder::AscNullsLast => " ASC NULLS LAST",
        SortOrder::DescNullsFirst => " DESC NULLS FIRST",
        SortOrder::DescNullsLast => " DESC NULLS LAST",
    }
}

/// Reject `Expr::Aggregate` shapes that would render something other than
/// what the fields say.
pub fn check_aggregate_shape(
    func: AggregateFunc,
    col: &str,
    distinct: bool,
    args: &[Expr],
    order_by: &[Cage],
    within_group: &[Cage],
) -> Result<(), String> {
    if !args.is_empty() && !col.is_empty() {
        return Err(format!(
            "{func} sets both `col` and `args`; put every argument in `args`"
        ));
    }
    // Every modelled function has one built-in arity; a wrong count would
    // only fail at the server (or, for STRING_AGG, had no slot at all).
    let given = match (args.len(), col.is_empty()) {
        (0, true) => 0,
        (0, false) => 1,
        (n, _) => n,
    };
    let (expected, shape) = match func {
        AggregateFunc::StringAgg => (2, "`args: [value, delimiter]`"),
        AggregateFunc::Mode => (0, "no direct argument"),
        _ => (1, "one argument (`col` or one `args` entry)"),
    };
    if given != expected {
        return Err(format!("{func} takes {shape}; got {given}"));
    }
    if func.is_ordered_set() {
        if within_group.is_empty() {
            return Err(format!("{func} needs WITHIN GROUP (ORDER BY ...)"));
        }
        if distinct || !order_by.is_empty() {
            return Err(format!(
                "{func} takes neither DISTINCT nor ORDER BY inside the call"
            ));
        }
    } else if !within_group.is_empty() {
        return Err(format!(
            "{func} is not an ordered-set aggregate: no WITHIN GROUP"
        ));
    }
    for cage in order_by.iter().chain(within_group) {
        aggregate_sort_key(cage)?;
    }
    Ok(())
}

/// `key [DIR], ...` for `Display`; malformed cages are shown, not hidden.
pub(crate) fn sort_keys_text(cages: &[Cage]) -> String {
    cages
        .iter()
        .map(|cage| match aggregate_sort_key(cage) {
            Ok((key, order)) => format!("{key}{}", sort_order_sql(order)),
            Err(message) => format!("/* {message} */"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}
