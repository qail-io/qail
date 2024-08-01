//! Aggregate function builders (COUNT, SUM, AVG, etc.)

use crate::ast::{AggregateFunc, Condition, Expr};

/// COUNT(*) aggregate
pub fn count() -> AggregateBuilder {
    AggregateBuilder {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: None,
    }
}

/// COUNT(DISTINCT column) aggregate
pub fn count_distinct(column: &str) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func: AggregateFunc::Count,
        distinct: true,
        filter: None,
        alias: None,
    }
}

/// COUNT(*) FILTER (WHERE conditions) aggregate
pub fn count_filter(conditions: Vec<Condition>) -> AggregateBuilder {
    AggregateBuilder {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: Some(conditions),
        alias: None,
    }
}

/// SUM(column) aggregate
pub fn sum(column: &str) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func: AggregateFunc::Sum,
        distinct: false,
        filter: None,
        alias: None,
    }
}

/// AVG(column) aggregate
pub fn avg(column: &str) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func: AggregateFunc::Avg,
        distinct: false,
        filter: None,
        alias: None,
    }
}

/// MIN(column) aggregate
pub fn min(column: &str) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func: AggregateFunc::Min,
        distinct: false,
        filter: None,
        alias: None,
    }
}

/// MAX(column) aggregate
pub fn max(column: &str) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func: AggregateFunc::Max,
        distinct: false,
        filter: None,
        alias: None,
    }
}

/// Builder for aggregate expressions
#[derive(Debug, Clone)]
pub struct AggregateBuilder {
    pub(crate) col: String,
    pub(crate) func: AggregateFunc,
    pub(crate) distinct: bool,
    pub(crate) filter: Option<Vec<Condition>>,
    pub(crate) alias: Option<String>,
}

impl AggregateBuilder {
    /// Add DISTINCT modifier
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add FILTER (WHERE ...) clause
    pub fn filter(mut self, conditions: Vec<Condition>) -> Self {
        self.filter = Some(conditions);
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
        }
    }
}

impl From<AggregateBuilder> for Expr {
    fn from(builder: AggregateBuilder) -> Self {
        builder.build()
    }
}
