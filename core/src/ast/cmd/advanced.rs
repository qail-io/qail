//! Advanced query builder methods.
//!
//! DISTINCT ON, HAVING, row locks, table sampling, JOIN aliases, etc.

use crate::ast::{
    CTEDef, Cage, CageKind, Condition, Expr, Join, JoinKind, LockMode, LogicalOp, Operator,
    OverridingKind, Qail, SampleMethod, SortOrder, Value,
};

impl Qail {
    /// Add a column expression.
    pub fn column_expr(mut self, expr: Expr) -> Self {
        self.columns.push(expr);
        self
    }

    /// Add multiple column expressions.
    pub fn columns_expr<I>(mut self, exprs: I) -> Self
    where
        I: IntoIterator<Item = Expr>,
    {
        self.columns.extend(exprs);
        self
    }

    /// DISTINCT ON named columns.
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

    /// DISTINCT ON expressions.
    pub fn distinct_on_expr<I>(mut self, exprs: I) -> Self
    where
        I: IntoIterator<Item = Expr>,
    {
        self.distinct_on = exprs.into_iter().collect();
        self
    }

    /// Add a raw Condition to the WHERE clause.
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

    /// Add a HAVING condition.
    pub fn having_cond(mut self, condition: Condition) -> Self {
        self.having.push(condition);
        self
    }

    /// Add multiple HAVING conditions.
    pub fn having_conds(mut self, conditions: impl IntoIterator<Item = Condition>) -> Self {
        self.having.extend(conditions);
        self
    }

    /// Set CTEs (WITH clause).
    pub fn with_ctes(mut self, ctes: Vec<CTEDef>) -> Self {
        self.ctes = ctes;
        self
    }

    /// UPDATE … FROM additional tables.
    pub fn update_from<I, S>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.from_tables
            .extend(tables.into_iter().map(|s| s.as_ref().to_string()));
        self
    }

    /// DELETE … USING additional tables.
    pub fn delete_using<I, S>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.using_tables
            .extend(tables.into_iter().map(|s| s.as_ref().to_string()));
        self
    }

    /// FOR UPDATE row lock.
    pub fn for_update(mut self) -> Self {
        self.lock_mode = Some(LockMode::Update);
        self
    }

    /// FOR UPDATE SKIP LOCKED row lock.
    /// Used for concurrent job claiming (e.g. outbox dispatch).
    pub fn for_update_skip_locked(mut self) -> Self {
        self.lock_mode = Some(LockMode::Update);
        self.skip_locked = true;
        self
    }

    /// FOR NO KEY UPDATE row lock.
    pub fn for_no_key_update(mut self) -> Self {
        self.lock_mode = Some(LockMode::NoKeyUpdate);
        self
    }

    /// FOR SHARE row lock.
    pub fn for_share(mut self) -> Self {
        self.lock_mode = Some(LockMode::Share);
        self
    }

    /// FOR KEY SHARE row lock.
    pub fn for_key_share(mut self) -> Self {
        self.lock_mode = Some(LockMode::KeyShare);
        self
    }

    /// FETCH FIRST n ROWS ONLY.
    pub fn fetch_first(mut self, count: u64) -> Self {
        self.fetch = Some((count, false));
        self
    }

    /// FETCH FIRST n ROWS WITH TIES.
    pub fn fetch_with_ties(mut self, count: u64) -> Self {
        self.fetch = Some((count, true));
        self
    }

    /// INSERT with DEFAULT VALUES.
    pub fn default_values(mut self) -> Self {
        self.default_values = true;
        self
    }

    /// OVERRIDING SYSTEM VALUE.
    pub fn overriding_system_value(mut self) -> Self {
        self.overriding = Some(OverridingKind::SystemValue);
        self
    }

    /// OVERRIDING USER VALUE.
    pub fn overriding_user_value(mut self) -> Self {
        self.overriding = Some(OverridingKind::UserValue);
        self
    }

    /// TABLESAMPLE BERNOULLI(percent).
    pub fn tablesample_bernoulli(mut self, percent: f64) -> Self {
        self.sample = Some((SampleMethod::Bernoulli, percent, None));
        self
    }

    /// TABLESAMPLE SYSTEM(percent).
    pub fn tablesample_system(mut self, percent: f64) -> Self {
        self.sample = Some((SampleMethod::System, percent, None));
        self
    }

    /// REPEATABLE(seed) for TABLESAMPLE.
    pub fn repeatable(mut self, seed: u64) -> Self {
        if let Some((method, percent, _)) = self.sample {
            self.sample = Some((method, percent, Some(seed)));
        }
        self
    }

    /// SELECT FROM ONLY (exclude child tables).
    pub fn only(mut self) -> Self {
        self.only_table = true;
        self
    }

    /// LEFT JOIN with alias.
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

    /// INNER JOIN with alias.
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

    /// JOIN with multiple ON conditions.
    ///
    /// The table string may include an alias (e.g. `"inventory inv"`).
    ///
    /// # Example
    /// ```ignore
    /// use qail_core::ast::builders::{eq, col};
    /// use qail_core::ast::{Condition, Operator, Expr, Value};
    ///
    /// // LEFT JOIN odyssey_leg_inventory inv
    /// //   ON inv.leg_id = ol.id AND inv.service_date = '2024-01-15'
    /// .left_join_conds("odyssey_leg_inventory inv", vec![
    ///     Condition { left: Expr::Named("inv.leg_id".into()), op: Operator::Eq,
    ///                 value: Value::Column("ol.id".into()), is_array_unnest: false },
    ///     Condition { left: Expr::Named("inv.service_date".into()), op: Operator::Eq,
    ///                 value: Value::String("2024-01-15".into()), is_array_unnest: false },
    /// ])
    /// ```
    pub fn join_conds(
        mut self,
        kind: JoinKind,
        table: impl AsRef<str>,
        conditions: Vec<Condition>,
    ) -> Self {
        self.joins.push(Join {
            kind,
            table: table.as_ref().to_string(),
            on: Some(conditions),
            on_true: false,
        });
        self
    }

    /// LEFT JOIN with multiple ON conditions.
    pub fn left_join_conds(self, table: impl AsRef<str>, conditions: Vec<Condition>) -> Self {
        self.join_conds(JoinKind::Left, table, conditions)
    }

    /// INNER JOIN with multiple ON conditions.
    pub fn inner_join_conds(self, table: impl AsRef<str>, conditions: Vec<Condition>) -> Self {
        self.join_conds(JoinKind::Inner, table, conditions)
    }

    /// Set an alias for the FROM table.
    pub fn table_alias(mut self, alias: impl AsRef<str>) -> Self {
        self.table = format!("{} {}", self.table, alias.as_ref());
        self
    }

    /// ORDER BY an expression.
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

    /// GROUP BY expressions.
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
