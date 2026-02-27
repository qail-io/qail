//! Query builder methods for Qail.
//!
//! Common fluent methods: columns, filter, join, order_by, limit, etc.

use crate::ast::{
    Cage, CageKind, Condition, Expr, Join, JoinKind, LogicalOp, Operator, Qail, SortOrder, Value,
};

impl Qail {
    /// Set LIMIT.
    pub fn limit(mut self, n: i64) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Limit(n as usize),
            conditions: vec![],
            logical_op: LogicalOp::And,
        });
        self
    }

    /// Sort by column ascending (deprecated, use `.order_asc()`).
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

    /// SELECT * (all columns).
    pub fn select_all(mut self) -> Self {
        self.columns.push(Expr::Star);
        self
    }

    /// Add columns by name.
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

    /// Add a single column by name.
    pub fn column(mut self, col: impl AsRef<str>) -> Self {
        self.columns.push(Expr::Named(col.as_ref().to_string()));
        self
    }

    /// Add a computed expression as a SELECT column.
    ///
    /// Use this for subqueries, aggregates, CASE WHEN, COALESCE, etc.
    ///
    /// # Example
    /// ```ignore
    /// use qail_core::ast::builders::{subquery, coalesce, col, text};
    /// use qail_core::ast::builders::ExprExt;
    ///
    /// Qail::get("orders")
    ///     .columns(&["id", "status"])
    ///     .select_expr(
    ///         subquery(Qail::get("order_items")
    ///             .column("sum(amount)")
    ///             .eq("order_id", col("orders.id")))
    ///         .with_alias("total_amount")
    ///     )
    ///     .select_expr(
    ///         coalesce([col("nickname"), col("first_name"), text("Guest")])
    ///             .alias("display_name")
    ///     )
    /// ```
    pub fn select_expr(mut self, expr: impl Into<Expr>) -> Self {
        self.columns.push(expr.into());
        self
    }

    /// Add multiple computed expressions as SELECT columns.
    ///
    /// # Example
    /// ```ignore
    /// .select_exprs([
    ///     count().alias("total"),
    ///     sum("amount").alias("grand_total"),
    /// ])
    /// ```
    pub fn select_exprs<I, E>(mut self, exprs: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: Into<Expr>,
    {
        self.columns.extend(exprs.into_iter().map(|e| e.into()));
        self
    }

    /// Add a WHERE filter with an operator and value.
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

    /// Add an OR filter condition.
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

    /// Filter: column = value.
    pub fn where_eq(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Eq, value)
    }

    /// Filter: column = value (alias for `where_eq`).
    pub fn eq(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Eq, value)
    }

    /// Filter: column != value.
    pub fn ne(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Ne, value)
    }

    /// Filter: column > value.
    pub fn gt(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Gt, value)
    }
    /// Filter: column >= value.
    pub fn gte(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Gte, value)
    }

    /// Filter: column < value
    pub fn lt(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Lt, value)
    }

    /// Filter: column <= value.
    pub fn lte(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Lte, value)
    }

    /// Filter: column IS NULL.
    pub fn is_null(self, column: impl AsRef<str>) -> Self {
        self.filter(column, Operator::IsNull, Value::Null)
    }

    /// Filter: column IS NOT NULL.
    pub fn is_not_null(self, column: impl AsRef<str>) -> Self {
        self.filter(column, Operator::IsNotNull, Value::Null)
    }

    /// Filter: column LIKE pattern.
    pub fn like(self, column: impl AsRef<str>, pattern: impl Into<Value>) -> Self {
        self.filter(column, Operator::Like, pattern)
    }

    /// Filter: column ILIKE pattern.
    pub fn ilike(self, column: impl AsRef<str>, pattern: impl Into<Value>) -> Self {
        self.filter(column, Operator::ILike, pattern)
    }

    /// Add a raw SQL boolean expression to the WHERE clause.
    ///
    /// Use this for complex predicates that can't be expressed through the
    /// standard filter methods (e.g. date arithmetic with `MAKE_INTERVAL`,
    /// `NOW()`, multi-column comparisons, etc.).
    ///
    /// The expression must evaluate to a boolean in PostgreSQL.
    ///
    /// # Example
    /// ```ignore
    /// Qail::get("orders")
    ///     .raw_where("created_at > NOW() - INTERVAL '24 hours'")
    ///     .raw_where("(status = 'active' OR priority > 5)")
    /// ```
    pub fn raw_where(self, sql: impl Into<String>) -> Self {
        self.filter_cond(Condition {
            left: Expr::Raw(sql.into()),
            op: Operator::IsNotNull,
            value: Value::Null,
            is_array_unnest: false,
        })
    }

    /// Filter: does `text` contain any element from `array_column`?
    ///
    /// Generates an `EXISTS (SELECT 1 FROM unnest(array_column) _el WHERE ...)`
    /// predicate with case-insensitive matching.
    pub fn array_elem_contained_in_text(
        self,
        array_column: impl AsRef<str>,
        text: impl Into<Value>,
    ) -> Self {
        self.filter_cond(Condition {
            left: Expr::Named(array_column.as_ref().to_string()),
            op: Operator::ArrayElemContainedInText,
            value: text.into(),
            is_array_unnest: true,
        })
    }

    /// Filter: column IN (values).
    ///
    /// # Arguments
    ///
    /// * `column` — Column name to filter on.
    /// * `values` — Iterable of values for the IN list.
    pub fn in_vals<I, V>(self, column: impl AsRef<str>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        let arr: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
        self.filter(column, Operator::In, Value::Array(arr))
    }

    /// Add ORDER BY clause.
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

    /// ORDER BY column DESC.
    pub fn order_desc(self, column: impl AsRef<str>) -> Self {
        self.order_by(column, SortOrder::Desc)
    }

    /// ORDER BY column ASC.
    pub fn order_asc(self, column: impl AsRef<str>) -> Self {
        self.order_by(column, SortOrder::Asc)
    }

    /// Set OFFSET.
    pub fn offset(mut self, n: i64) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Offset(n as usize),
            conditions: vec![],
            logical_op: LogicalOp::And,
        });
        self
    }

    /// GROUP BY columns.
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

    /// SELECT DISTINCT (all columns).
    pub fn distinct_on_all(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add a JOIN clause.
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

    /// LEFT JOIN.
    pub fn left_join(
        self,
        table: impl AsRef<str>,
        left_col: impl AsRef<str>,
        right_col: impl AsRef<str>,
    ) -> Self {
        self.join(JoinKind::Left, table, left_col, right_col)
    }

    /// INNER JOIN.
    pub fn inner_join(
        self,
        table: impl AsRef<str>,
        left_col: impl AsRef<str>,
        right_col: impl AsRef<str>,
    ) -> Self {
        self.join(JoinKind::Inner, table, left_col, right_col)
    }

    /// Join a related table using schema-defined foreign key relationship.
    ///
    /// This is the "First-Class Relations" API - it automatically infers
    /// the join condition from the schema's `ref:` definitions.
    ///
    /// # Example
    /// ```ignore
    /// // Schema: posts.user_id UUID ref:users.id
    ///
    /// // Instead of:
    /// Qail::get("users").left_join("posts", "users.id", "posts.user_id")
    ///
    /// // Simply:
    /// Qail::get("users").join_on("posts")
    /// ```
    ///
    /// # Panics
    /// Panics if no relation is found between the current table and the target.
    /// Load relations first using `schema::load_schema_relations()`.
    pub fn join_on(self, related_table: impl AsRef<str>) -> Self {
        let related = related_table.as_ref();

        // Try: current table -> related (forward relation)
        if let Some((from_col, to_col)) = crate::schema::lookup_relation(&self.table, related) {
            return self.left_join(related, &from_col, &to_col);
        }

        // Try: related -> current table (reverse relation)
        if let Some((from_col, to_col)) = crate::schema::lookup_relation(related, &self.table) {
            // Reverse: related.from_col references self.to_col
            return self.left_join(related, &to_col, &from_col);
        }

        panic!(
            "No relation found between '{}' and '{}'. \
             Define a ref: in schema.qail or use load_schema_relations() first.",
            self.table, related
        );
    }

    /// Join a related table if relation exists, otherwise no-op.
    ///
    /// This is the safe version of `join_on()` that doesn't panic.
    pub fn join_on_optional(self, related_table: impl AsRef<str>) -> Self {
        let related = related_table.as_ref();

        // Try forward relation
        if let Some((from_col, to_col)) = crate::schema::lookup_relation(&self.table, related) {
            return self.left_join(related, &from_col, &to_col);
        }

        // Try reverse relation
        if let Some((from_col, to_col)) = crate::schema::lookup_relation(related, &self.table) {
            return self.left_join(related, &to_col, &from_col);
        }

        // No relation found, return self unchanged
        self
    }

    /// Add RETURNING clause with column names.
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

    /// RETURNING * (all columns).
    pub fn returning_all(mut self) -> Self {
        self.returning = Some(vec![Expr::Star]);
        self
    }

    /// Add payload values (INSERT positional).
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

    /// Set a column = value pair for UPDATE or INSERT.
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

    /// Set column to COALESCE(new_value, existing_column) for partial updates.
    ///
    /// This is useful for UPDATE operations where you want to keep the existing
    /// value if the new value is NULL.
    ///
    /// # Example
    /// ```ignore
    /// Qail::set("users")
    ///     .set_coalesce("name", "Alice")  // name = COALESCE('Alice', name)
    ///     .eq("id", 1)
    /// ```
    pub fn set_coalesce(mut self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        use crate::ast::builders::coalesce;

        let col_name = column.as_ref().to_string();
        let coalesce_expr =
            coalesce([Expr::Literal(value.into()), Expr::Named(col_name.clone())]).build();

        let payload_cage = self
            .cages
            .iter_mut()
            .find(|c| matches!(c.kind, CageKind::Payload));

        let condition = Condition {
            left: Expr::Named(col_name),
            op: Operator::Eq,
            value: Value::Expr(Box::new(coalesce_expr)),
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

    /// Set column to COALESCE(new_value, existing_column) only if value is Some.
    ///
    /// Combines set_coalesce() with optional handling - if None, still adds
    /// the COALESCE with NULL as the first argument (so existing value is kept).
    pub fn set_coalesce_opt<T>(self, column: impl AsRef<str>, value: Option<T>) -> Self
    where
        T: Into<Value>,
    {
        match value {
            Some(v) => self.set_coalesce(column, v),
            None => self, // Skip - existing value will be kept
        }
    }

    /// Add ON CONFLICT DO UPDATE clause for UPSERT operations.
    ///
    /// # Example
    /// ```ignore
    /// Qail::add("users")
    ///     .set_value("id", 1)
    ///     .set_value("name", "Alice")
    ///     .on_conflict_update(&["id"], &[("name", Expr::Named("EXCLUDED.name".into()))])
    /// ```
    pub fn on_conflict_update<S>(mut self, conflict_cols: &[S], updates: &[(S, Expr)]) -> Self
    where
        S: AsRef<str>,
    {
        use super::{ConflictAction, OnConflict};

        self.on_conflict = Some(OnConflict {
            columns: conflict_cols
                .iter()
                .map(|c| c.as_ref().to_string())
                .collect(),
            action: ConflictAction::DoUpdate {
                assignments: updates
                    .iter()
                    .map(|(col, expr)| (col.as_ref().to_string(), expr.clone()))
                    .collect(),
            },
        });
        self
    }

    /// Add ON CONFLICT DO NOTHING clause (ignore duplicates).
    ///
    /// # Example
    /// ```ignore
    /// Qail::add("users")
    ///     .set_value("id", 1)
    ///     .on_conflict_nothing(&["id"])
    /// ```
    pub fn on_conflict_nothing<S>(mut self, conflict_cols: &[S]) -> Self
    where
        S: AsRef<str>,
    {
        use super::{ConflictAction, OnConflict};

        self.on_conflict = Some(OnConflict {
            columns: conflict_cols
                .iter()
                .map(|c| c.as_ref().to_string())
                .collect(),
            action: ConflictAction::DoNothing,
        });
        self
    }
}
