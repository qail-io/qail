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


    pub fn eq(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Eq, value)
    }

    pub fn ne(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Ne, value)
    }

    pub fn gt(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Gt, value)
    }
    pub fn gte(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Gte, value)
    }

    /// Filter: column < value
    pub fn lt(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Lt, value)
    }

    pub fn lte(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Lte, value)
    }

    pub fn is_null(self, column: impl AsRef<str>) -> Self {
        self.filter(column, Operator::IsNull, Value::Null)
    }

    pub fn is_not_null(self, column: impl AsRef<str>) -> Self {
        self.filter(column, Operator::IsNotNull, Value::Null)
    }

    pub fn like(self, column: impl AsRef<str>, pattern: impl Into<Value>) -> Self {
        self.filter(column, Operator::Like, pattern)
    }

    pub fn ilike(self, column: impl AsRef<str>, pattern: impl Into<Value>) -> Self {
        self.filter(column, Operator::ILike, pattern)
    }

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
        let coalesce_expr = coalesce([
            Expr::Literal(value.into()),
            Expr::Named(col_name.clone()),
        ]).build();
        
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
        use super::{OnConflict, ConflictAction};
        
        self.on_conflict = Some(OnConflict {
            columns: conflict_cols.iter().map(|c| c.as_ref().to_string()).collect(),
            action: ConflictAction::DoUpdate {
                assignments: updates.iter()
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
        use super::{OnConflict, ConflictAction};
        
        self.on_conflict = Some(OnConflict {
            columns: conflict_cols.iter().map(|c| c.as_ref().to_string()).collect(),
            action: ConflictAction::DoNothing,
        });
        self
    }
}
