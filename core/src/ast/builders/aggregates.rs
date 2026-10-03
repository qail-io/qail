//! Aggregate function builders (COUNT, SUM, AVG, etc.)

use crate::ast::{AggregateFunc, Cage, Condition, Expr, SortOrder, Value, aggregate_sort_cage};

fn column_aggregate(column: &str, func: AggregateFunc) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func,
        distinct: false,
        filter: None,
        alias: None,
        args: Vec::new(),
        order_by: Vec::new(),
        within_group: Vec::new(),
    }
}

/// COUNT(*) aggregate
pub fn count() -> AggregateBuilder {
    column_aggregate("*", AggregateFunc::Count)
}

/// COUNT(DISTINCT column) aggregate
pub fn count_distinct(column: &str) -> AggregateBuilder {
    column_aggregate(column, AggregateFunc::Count).distinct()
}

/// COUNT(*) FILTER (WHERE conditions) aggregate
pub fn count_filter(conditions: Vec<Condition>) -> AggregateBuilder {
    column_aggregate("*", AggregateFunc::Count).filter(conditions)
}

/// SUM(column) aggregate
pub fn sum(column: &str) -> AggregateBuilder {
    column_aggregate(column, AggregateFunc::Sum)
}

/// AVG(column) aggregate
pub fn avg(column: &str) -> AggregateBuilder {
    column_aggregate(column, AggregateFunc::Avg)
}

/// MIN(column) aggregate
pub fn min(column: &str) -> AggregateBuilder {
    column_aggregate(column, AggregateFunc::Min)
}

/// MAX(column) aggregate
pub fn max(column: &str) -> AggregateBuilder {
    column_aggregate(column, AggregateFunc::Max)
}

/// ARRAY_AGG(column) - collect all values into an array
pub fn array_agg(column: &str) -> AggregateBuilder {
    column_aggregate(column, AggregateFunc::ArrayAgg)
}

/// JSON_AGG(column) - aggregate values as JSON array
pub fn json_agg(column: &str) -> AggregateBuilder {
    column_aggregate(column, AggregateFunc::JsonAgg)
}

/// JSONB_AGG(column) - aggregate values as JSONB array
pub fn jsonb_agg(column: &str) -> AggregateBuilder {
    column_aggregate(column, AggregateFunc::JsonbAgg)
}

/// BOOL_AND(column) - returns TRUE if all values are true
pub fn bool_and(column: &str) -> AggregateBuilder {
    column_aggregate(column, AggregateFunc::BoolAnd)
}

/// BOOL_OR(column) - returns TRUE if any value is true
pub fn bool_or(column: &str) -> AggregateBuilder {
    column_aggregate(column, AggregateFunc::BoolOr)
}

/// Aggregate over expression arguments: `aggregate(AggregateFunc::Sum,
/// [binary(col("price"), BinaryOp::Mul, col("quantity"))])` is
/// `SUM((price * quantity))`; STRING_AGG takes `[value, delimiter]`.
pub fn aggregate(func: AggregateFunc, args: impl IntoIterator<Item = Expr>) -> AggregateBuilder {
    AggregateBuilder {
        args: args.into_iter().collect(),
        ..column_aggregate("", func)
    }
}

/// PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY ...); add the sort key
/// with [`AggregateBuilder::within_group`].
pub fn percentile_cont(fraction: f64) -> AggregateBuilder {
    aggregate(
        AggregateFunc::PercentileCont,
        [Expr::Literal(Value::Float(fraction))],
    )
}

/// PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY ...); add the sort key
/// with [`AggregateBuilder::within_group`].
pub fn percentile_disc(fraction: f64) -> AggregateBuilder {
    aggregate(
        AggregateFunc::PercentileDisc,
        [Expr::Literal(Value::Float(fraction))],
    )
}

/// MODE() WITHIN GROUP (ORDER BY ...); add the sort key with
/// [`AggregateBuilder::within_group`].
pub fn mode() -> AggregateBuilder {
    aggregate(AggregateFunc::Mode, [])
}

/// Builder for aggregate expressions
#[derive(Debug, Clone)]
pub struct AggregateBuilder {
    pub(crate) col: String,
    pub(crate) func: AggregateFunc,
    pub(crate) distinct: bool,
    pub(crate) filter: Option<Vec<Condition>>,
    pub(crate) alias: Option<String>,
    pub(crate) args: Vec<Expr>,
    pub(crate) order_by: Vec<Cage>,
    pub(crate) within_group: Vec<Cage>,
}

impl AggregateBuilder {
    /// Add DISTINCT modifier
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add a `FILTER (WHERE ...)` clause to restrict which rows feed the aggregate.
    pub fn filter(mut self, conditions: Vec<Condition>) -> Self {
        self.filter = Some(conditions);
        self
    }

    /// Append an aggregate-local `ORDER BY` key: `ARRAY_AGG(x ORDER BY key)`.
    pub fn order_by(mut self, key: impl Into<Expr>, order: SortOrder) -> Self {
        self.order_by.push(aggregate_sort_cage(key.into(), order));
        self
    }

    /// Append a `WITHIN GROUP (ORDER BY key)` key of an ordered-set aggregate.
    pub fn within_group(mut self, key: impl Into<Expr>, order: SortOrder) -> Self {
        self.within_group
            .push(aggregate_sort_cage(key.into(), order));
        self
    }

    /// Add alias (AS name)
    pub fn alias(mut self, name: &str) -> Expr {
        self.alias = Some(name.to_string());
        self.build()
    }

    /// Build the final Expr
    pub fn build(self) -> Expr {
        Expr::Aggregate {
            col: self.col,
            func: self.func,
            distinct: self.distinct,
            filter: self.filter,
            alias: self.alias,
            args: self.args,
            order_by: self.order_by,
            within_group: self.within_group,
        }
    }
}

impl From<AggregateBuilder> for Expr {
    fn from(builder: AggregateBuilder) -> Self {
        builder.build()
    }
}
