//! Query builder methods for Qail.
//!
//! Common fluent methods: columns, filter, join, order_by, limit, etc.

use crate::ast::{
    Cage, CageKind, Condition, Expr, Join, JoinKind, LogicalOp, Operator, Qail, SortOrder,
    Value,
};

impl Qail {
    pub fn limit(mut self, n: i64) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Limit(n as usize),
            conditions: vec![],
            logical_op: LogicalOp::And,
        });
        self
    }

    #[deprecated(since = "0.11.0", note = "Use .order_asc(column) instead")]
    pub fn sort_asc(mut self, column: &str) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Sort(SortOrder::Asc),
            conditions: vec![Condition {
                left: Expr::Named(column.to_string()),
                op: Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });
        self
    }

    pub fn select_all(mut self) -> Self {
        self.columns.push(Expr::Star);
        self
    }

    pub fn columns<I, S>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.columns.extend(
            cols.into_iter()
                .map(|c| Expr::Named(c.as_ref().to_string())),
        );
        self
    }

    pub fn column(mut self, col: impl AsRef<str>) -> Self {
        self.columns.push(Expr::Named(col.as_ref().to_string()));
        self
    }

    pub fn filter(
        mut self,
        column: impl AsRef<str>,
        op: Operator,
        value: impl Into<Value>,
    ) -> Self {
        let filter_cage = self
            .cages
            .iter_mut()
            .find(|c| matches!(c.kind, CageKind::Filter));

        let condition = Condition {
            left: Expr::Named(column.as_ref().to_string()),
            op,
            value: value.into(),
            is_array_unnest: false,
        };

        if let Some(cage) = filter_cage {
            cage.conditions.push(condition);
        } else {
            self.cages.push(Cage {
                kind: CageKind::Filter,
                conditions: vec![condition],
                logical_op: LogicalOp::And,
            });
        }
        self
    }

    pub fn or_filter(
        mut self,
        column: impl AsRef<str>,
        op: Operator,
        value: impl Into<Value>,
    ) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named(column.as_ref().to_string()),
                op,
                value: value.into(),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::Or,
        });
        self
    }

    pub fn where_eq(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Eq, value)
    }

    // ============================================================
    // FLUENT FILTER SHORTCUTS (ergonomic API)
    // ============================================================

    /// Filter: column = value (shorter alias for where_eq)
    pub fn eq(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Eq, value)
    }

    /// Filter: column != value
    pub fn ne(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Ne, value)
    }

    /// Filter: column > value
    pub fn gt(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Gt, value)
    }

    /// Filter: column >= value
    pub fn gte(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Gte, value)
    }

    /// Filter: column < value
    pub fn lt(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Lt, value)
    }

    /// Filter: column <= value
    pub fn lte(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Lte, value)
    }

    /// Filter: column IS NULL
    pub fn is_null(self, column: impl AsRef<str>) -> Self {
        self.filter(column, Operator::IsNull, Value::Null)
    }

    /// Filter: column IS NOT NULL
    pub fn is_not_null(self, column: impl AsRef<str>) -> Self {
        self.filter(column, Operator::IsNotNull, Value::Null)
    }

    /// Filter: column LIKE pattern
    pub fn like(self, column: impl AsRef<str>, pattern: impl Into<Value>) -> Self {
        self.filter(column, Operator::Like, pattern)
    }

    /// Filter: column ILIKE pattern (case-insensitive)
    pub fn ilike(self, column: impl AsRef<str>, pattern: impl Into<Value>) -> Self {
        self.filter(column, Operator::ILike, pattern)
    }

    /// Filter: column IN (values)
    pub fn in_vals<I, V>(self, column: impl AsRef<str>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        let arr: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
        self.filter(column, Operator::In, Value::Array(arr))
    }

    pub fn order_by(mut self, column: impl AsRef<str>, order: SortOrder) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Sort(order),
            conditions: vec![Condition {
                left: Expr::Named(column.as_ref().to_string()),
                op: Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });
        self
    }

    pub fn order_desc(self, column: impl AsRef<str>) -> Self {
        self.order_by(column, SortOrder::Desc)
    }

    pub fn order_asc(self, column: impl AsRef<str>) -> Self {
        self.order_by(column, SortOrder::Asc)
    }

    pub fn offset(mut self, n: i64) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Offset(n as usize),
            conditions: vec![],
            logical_op: LogicalOp::And,
        });
        self
    }

    pub fn group_by<I, S>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let conditions: Vec<Condition> = cols
            .into_iter()
            .map(|c| Condition {
                left: Expr::Named(c.as_ref().to_string()),
                op: Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            })
            .collect();

        self.cages.push(Cage {
            kind: CageKind::Partition,
            conditions,
            logical_op: LogicalOp::And,
        });
        self
    }

    pub fn distinct_on_all(mut self) -> Self {
        self.distinct = true;
        self
    }

    pub fn join(
        mut self,
        kind: JoinKind,
        table: impl AsRef<str>,
        left_col: impl AsRef<str>,
        right_col: impl AsRef<str>,
    ) -> Self {
        self.joins.push(Join {
            kind,
            table: table.as_ref().to_string(),
            on: Some(vec![Condition {
                left: Expr::Named(left_col.as_ref().to_string()),
                op: Operator::Eq,
                value: Value::Column(right_col.as_ref().to_string()),
                is_array_unnest: false,
            }]),
            on_true: false,
        });
        self
    }

    pub fn left_join(
        self,
        table: impl AsRef<str>,
        left_col: impl AsRef<str>,
        right_col: impl AsRef<str>,
    ) -> Self {
        self.join(JoinKind::Left, table, left_col, right_col)
    }

    pub fn inner_join(
        self,
        table: impl AsRef<str>,
        left_col: impl AsRef<str>,
        right_col: impl AsRef<str>,
    ) -> Self {
        self.join(JoinKind::Inner, table, left_col, right_col)
    }

    pub fn returning<I, S>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.returning = Some(
            cols.into_iter()
                .map(|c| Expr::Named(c.as_ref().to_string()))
                .collect(),
        );
        self
    }

    pub fn returning_all(mut self) -> Self {
        self.returning = Some(vec![Expr::Star]);
        self
    }

    pub fn values<I, V>(mut self, vals: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        self.cages.push(Cage {
            kind: CageKind::Payload,
            conditions: vals
                .into_iter()
                .enumerate()
                .map(|(i, v)| Condition {
                    left: Expr::Named(format!("${}", i + 1)),
                    op: Operator::Eq,
                    value: v.into(),
                    is_array_unnest: false,
                })
                .collect(),
            logical_op: LogicalOp::And,
        });
        self
    }

    pub fn set_value(mut self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        let payload_cage = self
            .cages
            .iter_mut()
            .find(|c| matches!(c.kind, CageKind::Payload));

        let condition = Condition {
            left: Expr::Named(column.as_ref().to_string()),
            op: Operator::Eq,
            value: value.into(),
            is_array_unnest: false,
        };

        if let Some(cage) = payload_cage {
            cage.conditions.push(condition);
        } else {
            self.cages.push(Cage {
                kind: CageKind::Payload,
                conditions: vec![condition],
                logical_op: LogicalOp::And,
            });
        }
        self
    }

    /// Set value only if Some, skip entirely if None
    /// This is ergonomic for optional fields - the column is not included in the INSERT at all if None
    pub fn set_opt<T>(self, column: impl AsRef<str>, value: Option<T>) -> Self
    where
        T: Into<Value>,
    {
        match value {
            Some(v) => self.set_value(column, v),
            None => self, // Skip entirely, don't add column
        }
    }
}
