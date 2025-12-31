//! Ergonomic shortcuts for common query patterns.
//!
//! These helpers make QAIL simpler than raw SQL for analytics queries.
//! All shortcuts are AST-native - no raw SQL strings!

use crate::ast::{Condition, Expr, Operator, Value};
use super::{count_filter, case_when, col, binary, cast, now_minus};
use crate::ast::BinaryOp;

/// Combine multiple conditions with AND logic
/// # Example
/// ```ignore
/// all([eq("direction", "outbound"), recent("24 hours")])
/// ```
pub fn all<I>(conditions: I) -> Vec<Condition>
where
    I: IntoIterator<Item = Condition>,
{
    conditions.into_iter().collect()
}

/// Combine two conditions with AND logic
/// # Example
/// ```ignore
/// and(eq("direction", "outbound"), recent("24 hours"))
/// ```
pub fn and(a: Condition, b: Condition) -> Vec<Condition> {
    vec![a, b]
}

/// Combine three conditions with AND logic
pub fn and3(a: Condition, b: Condition, c: Condition) -> Vec<Condition> {
    vec![a, b, c]
}

/// COUNT(*) with single WHERE condition - shorthand for count_filter
/// # Example
/// ```ignore
/// count_where(eq("direction", "outbound"))
///     .alias("messages_sent_24h")
/// ```
pub fn count_where(condition: Condition) -> super::AggregateBuilder {
    count_filter(vec![condition])
}

/// COUNT(*) with multiple WHERE conditions (AND) - shorthand for count_filter
/// # Example
/// ```ignore
/// count_where_all([eq("direction", "outbound"), recent("24 hours")])
///     .alias("messages_sent_24h")
/// ```
pub fn count_where_all<I>(conditions: I) -> super::AggregateBuilder
where
    I: IntoIterator<Item = Condition>,
{
    count_filter(conditions.into_iter().collect())
}

/// Filter for recent records (created_at > NOW() - INTERVAL)
/// AST-native: uses proper Expr nodes, not raw SQL strings!
/// # Example
/// ```ignore
/// // created_at > NOW() - INTERVAL '24 hours'
/// recent("24 hours")
/// ```
pub fn recent(duration: &str) -> Condition {
    recent_col("created_at", duration)
}

/// Filter for recent records on a custom column
/// AST-native: uses proper Expr nodes!
/// # Example
/// ```ignore
/// // updated_at > NOW() - INTERVAL '7 days'
/// recent_col("updated_at", "7 days")
/// ```
pub fn recent_col(column: &str, duration: &str) -> Condition {
    Condition {
        left: Expr::Named(column.to_string()),
        op: Operator::Gt,
        // AST-native: use now_minus() which produces Expr::Binary AST node
        value: Value::Expr(Box::new(now_minus(duration))),
        is_array_unnest: false,
    }
}

/// IN list condition - shorthand for is_in
/// # Example
/// ```ignore
/// in_list("status", ["delivered", "read"])
/// ```
pub fn in_list<I, S>(column: &str, values: I) -> Condition
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let list: Vec<Value> = values
        .into_iter()
        .map(|v| Value::String(v.as_ref().to_string()))
        .collect();
    
    Condition {
        left: Expr::Named(column.to_string()),
        op: Operator::In,
        value: Value::Array(list),
        is_array_unnest: false,
    }
}

/// Calculate percentage with safe division (returns 0 if denominator is 0)
/// AST-native: produces CASE WHEN, Binary, and Cast AST nodes!
/// # Example
/// ```ignore
/// percentage("delivered", "sent").alias("delivery_rate")
/// // Expands to: CASE WHEN sent > 0 THEN (delivered::float8 / sent::float8) * 100 ELSE 0 END
/// ```
pub fn percentage(numerator: &str, denominator: &str) -> super::CaseBuilder {
    let division = binary(
        cast(col(numerator), "float8").build(),
        BinaryOp::Div,
        cast(col(denominator), "float8").build(),
    ).build();
    
    let multiplied = binary(division, BinaryOp::Mul, Expr::Literal(Value::Float(100.0))).build();
    
    case_when(
        super::gt(denominator, 0),
        multiplied,
    ).otherwise(Expr::Literal(Value::Float(0.0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::builders::eq;

    #[test]
    fn test_count_where() {
        let agg = count_where(eq("status", "active"));
        let expr = agg.alias("active_count");
        assert!(matches!(expr, Expr::Aggregate { alias: Some(a), .. } if a == "active_count"));
    }

    #[test]
    fn test_recent_is_ast_native() {
        let cond = recent("24 hours");
        assert!(matches!(cond.op, Operator::Gt));
        // Verify it uses Value::Expr, not Value::Function (raw SQL)
        assert!(matches!(cond.value, Value::Expr(_)));
    }

    #[test]
    fn test_in_list() {
        let cond = in_list("status", ["a", "b", "c"]);
        assert!(matches!(cond.op, Operator::In));
    }

    #[test]
    fn test_percentage() {
        let builder = percentage("delivered", "sent");
        let expr = builder.alias("rate");
        assert!(matches!(expr, Expr::Case { alias: Some(a), .. } if a == "rate"));
    }
}
