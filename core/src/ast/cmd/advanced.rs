//! Advanced query builder methods.
//!
//! DISTINCT ON, HAVING, row locks, table sampling, JOIN aliases, etc.

use crate::ast::{
    Cage, CageKind, Condition, Expr, Join, JoinKind, LockMode, LogicalOp, Operator,
    OverridingKind, Qail, SampleMethod, SortOrder, Value, CTEDef,
};

impl Qail {
    pub fn column_expr(mut self, expr: Expr) -> Self {
        self.columns.push(expr);
        self
    }

    pub fn columns_expr<I>(mut self, exprs: I) -> Self
    where
        I: IntoIterator<Item = Expr>,
    {
        self.columns.extend(exprs);
        self
    }

    pub fn distinct_on<I, S>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.distinct_on = cols
            .into_iter()
            .map(|c| Expr::Named(c.as_ref().to_string()))
            .collect();
        self
    }

    pub fn distinct_on_expr<I>(mut self, exprs: I) -> Self
    where
        I: IntoIterator<Item = Expr>,
    {
        self.distinct_on = exprs.into_iter().collect();
        self
    }

    pub fn filter_cond(mut self, condition: Condition) -> Self {
        let filter_cage = self
            .cages
            .iter_mut()
            .find(|c| matches!(c.kind, CageKind::Filter));

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

    pub fn having_cond(mut self, condition: Condition) -> Self {
        self.having.push(condition);
        self
    }

    pub fn having_conds(mut self, conditions: impl IntoIterator<Item = Condition>) -> Self {
        self.having.extend(conditions);
        self
    }

    pub fn with_ctes(mut self, ctes: Vec<CTEDef>) -> Self {
        self.ctes = ctes;
        self
    }

    pub fn update_from<I, S>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.from_tables.extend(tables.into_iter().map(|s| s.as_ref().to_string()));
        self
    }

    pub fn delete_using<I, S>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.using_tables.extend(tables.into_iter().map(|s| s.as_ref().to_string()));
        self
    }

    pub fn for_update(mut self) -> Self {
        self.lock_mode = Some(LockMode::Update);
        self
    }

    pub fn for_no_key_update(mut self) -> Self {
        self.lock_mode = Some(LockMode::NoKeyUpdate);
        self
    }

    pub fn for_share(mut self) -> Self {
        self.lock_mode = Some(LockMode::Share);
        self
    }

    pub fn for_key_share(mut self) -> Self {
        self.lock_mode = Some(LockMode::KeyShare);
        self
    }

    pub fn fetch_first(mut self, count: u64) -> Self {
        self.fetch = Some((count, false));
        self
    }

    pub fn fetch_with_ties(mut self, count: u64) -> Self {
        self.fetch = Some((count, true));
        self
    }

    pub fn default_values(mut self) -> Self {
        self.default_values = true;
        self
    }

    pub fn overriding_system_value(mut self) -> Self {
        self.overriding = Some(OverridingKind::SystemValue);
        self
    }

    pub fn overriding_user_value(mut self) -> Self {
        self.overriding = Some(OverridingKind::UserValue);
        self
    }

    pub fn tablesample_bernoulli(mut self, percent: f64) -> Self {
        self.sample = Some((SampleMethod::Bernoulli, percent, None));
        self
    }

    pub fn tablesample_system(mut self, percent: f64) -> Self {
        self.sample = Some((SampleMethod::System, percent, None));
        self
    }

    pub fn repeatable(mut self, seed: u64) -> Self {
        if let Some((method, percent, _)) = self.sample {
            self.sample = Some((method, percent, Some(seed)));
        }
        self
    }

    pub fn only(mut self) -> Self {
        self.only_table = true;
        self
    }

    pub fn left_join_as(
        mut self,
        table: impl AsRef<str>,
        alias: impl AsRef<str>,
        left_col: impl AsRef<str>,
        right_col: impl AsRef<str>,
    ) -> Self {
        self.joins.push(Join {
            kind: JoinKind::Left,
            table: format!("{} {}", table.as_ref(), alias.as_ref()),
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

    pub fn inner_join_as(
        mut self,
        table: impl AsRef<str>,
        alias: impl AsRef<str>,
        left_col: impl AsRef<str>,
        right_col: impl AsRef<str>,
    ) -> Self {
        self.joins.push(Join {
            kind: JoinKind::Inner,
            table: format!("{} {}", table.as_ref(), alias.as_ref()),
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

    pub fn table_alias(mut self, alias: impl AsRef<str>) -> Self {
        self.table = format!("{} {}", self.table, alias.as_ref());
        self
    }

    pub fn order_by_expr(mut self, expr: Expr, order: SortOrder) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Sort(order),
            conditions: vec![Condition {
                left: expr,
                op: Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });
        self
    }

    pub fn group_by_expr<I>(mut self, exprs: I) -> Self
    where
        I: IntoIterator<Item = Expr>,
    {
        let conditions: Vec<Condition> = exprs
            .into_iter()
            .map(|e| Condition {
                left: e,
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
}
