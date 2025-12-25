use serde::{Deserialize, Serialize};
use crate::ast::{Action, Cage, CageKind, Expr, Condition, GroupByMode, IndexDef, Join, JoinKind, LogicalOp, Operator, SetOp, SortOrder, TableConstraint, Value};

/// The primary command structure representing a parsed QAIL query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QailCmd {
    /// The action to perform (GET, SET, DEL, ADD)
    pub action: Action,
    /// Target table name
    pub table: String,
    /// Columns to select/return (now Expressions)
    pub columns: Vec<Expr>,
    /// Joins to other tables
    #[serde(default)]
    pub joins: Vec<Join>,
    /// Cages (filters, sorts, limits, payloads)
    pub cages: Vec<Cage>,
    /// Whether to use DISTINCT in SELECT
    #[serde(default)]
    pub distinct: bool,
    /// Index definition (for Action::Index)
    #[serde(default)]
    pub index_def: Option<IndexDef>,
    /// Table-level constraints (for Action::Make)
    #[serde(default)]
    pub table_constraints: Vec<TableConstraint>,
    /// Set operations (UNION, INTERSECT, EXCEPT) chained queries
    #[serde(default)]
    pub set_ops: Vec<(SetOp, Box<QailCmd>)>,
    /// HAVING clause conditions (filter on aggregates)
    #[serde(default)]
    pub having: Vec<Condition>,
    /// GROUP BY mode (Simple, Rollup, Cube)
    #[serde(default)]
    pub group_by_mode: GroupByMode,
    /// CTE definitions (for WITH/WITH RECURSIVE queries)
    #[serde(default)]
    pub ctes: Vec<CTEDef>,
    /// DISTINCT ON expressions (Postgres-specific) - supports columns and expressions
    #[serde(default)]
    pub distinct_on: Vec<Expr>,
    /// RETURNING clause columns (for INSERT/UPDATE/DELETE)
    /// Empty = RETURNING *, Some([]) = no RETURNING, Some([cols]) = RETURNING cols
    #[serde(default)]
    pub returning: Option<Vec<Expr>>,
    /// ON CONFLICT clause for upsert operations (INSERT only)
    #[serde(default)]
    pub on_conflict: Option<OnConflict>,
    /// Source query for INSERT...SELECT (INSERT only)
    /// When present, values come from this subquery instead of VALUES clause
    #[serde(default)]
    pub source_query: Option<Box<QailCmd>>,
}

/// CTE (Common Table Expression) definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CTEDef {
    /// CTE name (the alias used in the query)
    pub name: String,
    /// Whether this is a RECURSIVE CTE
    pub recursive: bool,
    /// Column list for the CTE (optional)
    pub columns: Vec<String>,
    /// Base query (non-recursive part)
    pub base_query: Box<QailCmd>,
    /// Recursive part (UNION ALL with self-reference)
    pub recursive_query: Option<Box<QailCmd>>,
    /// Source table for recursive join (references CTE name)
    pub source_table: Option<String>,
}

/// ON CONFLICT clause for upsert operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnConflict {
    /// Columns that define the conflict (unique constraint)
    pub columns: Vec<String>,
    /// What to do on conflict
    pub action: ConflictAction,
}

/// Action to take when a conflict occurs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConflictAction {
    /// DO NOTHING - ignore the insert
    DoNothing,
    /// DO UPDATE SET - update the existing row
    DoUpdate {
        /// Column assignments: (column_name, new_value)
        assignments: Vec<(String, Expr)>,
    },
}

impl Default for OnConflict {
    fn default() -> Self {
        Self {
            columns: vec![],
            action: ConflictAction::DoNothing,
        }
    }
}

impl Default for QailCmd {
    fn default() -> Self {
        Self {
            action: Action::Get,
            table: String::new(),
            columns: vec![],
            joins: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
        }
    }
}

impl QailCmd {
    /// Create a new GET command for the given table.
    pub fn get(table: impl Into<String>) -> Self {
        Self {
            action: Action::Get,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
        }
    }

    /// Create a placeholder command for raw SQL (used in CTE subqueries).
    pub fn raw_sql(sql: impl Into<String>) -> Self {
        Self {
            action: Action::Get,
            table: sql.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
        }
    }

    /// Create a new SET (update) command for the given table.
    pub fn set(table: impl Into<String>) -> Self {
        Self {
            action: Action::Set,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
        }
    }

    /// Create a new DEL (delete) command for the given table.
    pub fn del(table: impl Into<String>) -> Self {
        Self {
            action: Action::Del,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
        }
    }

    /// Create a new ADD (insert) command for the given table.
    pub fn add(table: impl Into<String>) -> Self {
        Self {
            action: Action::Add,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
        }
    }

    /// Create a new PUT (upsert) command for the given table.
    pub fn put(table: impl Into<String>) -> Self {
        Self {
            action: Action::Put,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
        }
    }

    /// Create a new EXPORT (COPY TO STDOUT) command for the given table.
    /// 
    /// Used for bulk data export via PostgreSQL COPY protocol.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// 
    /// // Export all users
    /// let cmd = QailCmd::export("users")
    ///     .columns(["id", "name", "email"]);
    /// 
    /// // Export with filter
    /// let cmd = QailCmd::export("users")
    ///     .columns(["id", "name"])
    ///     .filter("active", true);
    /// ```
    pub fn export(table: impl Into<String>) -> Self {
        Self {
            action: Action::Export,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
        }
    }

    /// Create a new MAKE (create table) command for the given table.
    pub fn make(table: impl Into<String>) -> Self {
        Self {
            action: Action::Make,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
        }
    }

    /// Add columns to hook (select).
    pub fn hook(mut self, cols: &[&str]) -> Self {
        self.columns = cols.iter().map(|c| Expr::Named(c.to_string())).collect();
        self
    }

    /// Add a filter cage.
    pub fn cage(mut self, column: &str, value: impl Into<Value>) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named(column.to_string()),
                op: Operator::Eq,
                value: value.into(),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });
        self
    }

    /// Add a limit cage.
    pub fn limit(mut self, n: i64) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Limit(n as usize),
            conditions: vec![],
            logical_op: LogicalOp::And,
        });
        self
    }

    /// Add a sort cage (ascending).
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

    // =========================================================================
    // Fluent Builder API (New)
    // =========================================================================

    /// Select all columns (*).
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::get("users").select_all();
    /// ```
    pub fn select_all(mut self) -> Self {
        self.columns.push(Expr::Star);
        self
    }

    /// Select specific columns.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::get("users").columns(["id", "email", "name"]);
    /// ```
    pub fn columns<I, S>(mut self, cols: I) -> Self 
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.columns.extend(cols.into_iter().map(|c| Expr::Named(c.as_ref().to_string())));
        self
    }

    /// Add a single column.
    pub fn column(mut self, col: impl AsRef<str>) -> Self {
        self.columns.push(Expr::Named(col.as_ref().to_string()));
        self
    }

    /// Add a filter condition with a specific operator.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::{QailCmd, Operator};
    /// let cmd = QailCmd::get("users")
    ///     .filter("age", Operator::Gte, 18)
    ///     .filter("status", Operator::Eq, "active");
    /// ```
    pub fn filter(mut self, column: impl AsRef<str>, op: Operator, value: impl Into<Value>) -> Self {
        // Check if there's already a Filter cage to add to
        let filter_cage = self.cages.iter_mut().find(|c| matches!(c.kind, CageKind::Filter));
        
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
    pub fn or_filter(mut self, column: impl AsRef<str>, op: Operator, value: impl Into<Value>) -> Self {
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

    /// Add a WHERE equals condition (shorthand for filter with Eq).
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::get("users").where_eq("id", 42);
    /// ```
    pub fn where_eq(self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        self.filter(column, Operator::Eq, value)
    }

    /// Add ORDER BY clause.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::{QailCmd, SortOrder};
    /// let cmd = QailCmd::get("users")
    ///     .order_by("created_at", SortOrder::Desc)
    ///     .order_by("name", SortOrder::Asc);
    /// ```
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

    /// Add OFFSET clause.
    pub fn offset(mut self, n: i64) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Offset(n as usize),
            conditions: vec![],
            logical_op: LogicalOp::And,
        });
        self
    }

    /// Add GROUP BY columns.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::get("orders")
    ///     .columns(["status", "count(*) as cnt"])
    ///     .group_by(["status"]);
    /// ```
    pub fn group_by<I, S>(mut self, cols: I) -> Self 
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        // Use Partition cage kind for GROUP BY (closest match)
        let conditions: Vec<Condition> = cols.into_iter().map(|c| Condition {
            left: Expr::Named(c.as_ref().to_string()),
            op: Operator::Eq,
            value: Value::Null,
            is_array_unnest: false,
        }).collect();
        
        self.cages.push(Cage {
            kind: CageKind::Partition,
            conditions,
            logical_op: LogicalOp::And,
        });
        self
    }

    /// Enable DISTINCT.
    pub fn distinct_on_all(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add a JOIN.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::{QailCmd, JoinKind};
    /// let cmd = QailCmd::get("users")
    ///     .join(JoinKind::Left, "profiles", "users.id", "profiles.user_id");
    /// ```
    pub fn join(
        mut self, 
        kind: JoinKind, 
        table: impl AsRef<str>, 
        left_col: impl AsRef<str>, 
        right_col: impl AsRef<str>
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

    /// Left join shorthand.
    pub fn left_join(self, table: impl AsRef<str>, left_col: impl AsRef<str>, right_col: impl AsRef<str>) -> Self {
        self.join(JoinKind::Left, table, left_col, right_col)
    }

    /// Inner join shorthand.
    pub fn inner_join(self, table: impl AsRef<str>, left_col: impl AsRef<str>, right_col: impl AsRef<str>) -> Self {
        self.join(JoinKind::Inner, table, left_col, right_col)
    }

    /// Set RETURNING clause for INSERT/UPDATE/DELETE.
    pub fn returning<I, S>(mut self, cols: I) -> Self 
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.returning = Some(cols.into_iter().map(|c| Expr::Named(c.as_ref().to_string())).collect());
        self
    }

    /// Set RETURNING * for INSERT/UPDATE/DELETE.
    pub fn returning_all(mut self) -> Self {
        self.returning = Some(vec![Expr::Star]);
        self
    }

    /// Set values for INSERT.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::add("users")
    ///     .columns(["email", "name"])
    ///     .values(["alice@example.com", "Alice"]);
    /// ```
    pub fn values<I, V>(mut self, vals: I) -> Self 
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        // Use Payload cage kind for INSERT values
        self.cages.push(Cage {
            kind: CageKind::Payload,
            conditions: vals.into_iter().enumerate().map(|(i, v)| Condition {
                left: Expr::Named(format!("${}", i + 1)),
                op: Operator::Eq,
                value: v.into(),
                is_array_unnest: false,
            }).collect(),
            logical_op: LogicalOp::And,
        });
        self
    }

    /// Set update assignments for SET command.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::set("users")
    ///     .set_value("status", "active")
    ///     .set_value("updated_at", "now()")
    ///     .where_eq("id", 42);
    /// ```
    pub fn set_value(mut self, column: impl AsRef<str>, value: impl Into<Value>) -> Self {
        // Find or create Payload cage for SET assignments
        let payload_cage = self.cages.iter_mut().find(|c| matches!(c.kind, CageKind::Payload));
        
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

    // =========================================================================
    // CTE (Common Table Expression) Builder Methods
    // =========================================================================

    /// Wrap this query as a CTE with the given name.
    /// 
    /// # Example
    /// ```ignore
    /// let cte = QailCmd::get("employees")
    ///     .hook(&["id", "name"])
    ///     .cage("manager_id", Value::Null)
    ///     .as_cte("emp_tree");
    /// ```
    pub fn as_cte(self, name: impl Into<String>) -> Self {
        let cte_name = name.into();
        let columns: Vec<String> = self.columns.iter().filter_map(|c| {
            match c {
                Expr::Named(n) => Some(n.clone()),
                Expr::Aliased { alias, .. } => Some(alias.clone()),
                _ => None,
            }
        }).collect();
        
        Self {
            action: Action::With,
            table: cte_name.clone(),
            columns: vec![],
            joins: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
            ctes: vec![CTEDef {
                name: cte_name,
                recursive: false,
                columns,
                base_query: Box::new(self),
                recursive_query: None,
                source_table: None,
            }],
        }
    }

    /// Make this CTE recursive and add the recursive part.
    /// 
    /// # Example
    /// ```ignore
    /// let recursive_cte = base_query
    ///     .as_cte("emp_tree")
    ///     .recursive(recursive_query);
    /// ```
    pub fn recursive(mut self, recursive_part: QailCmd) -> Self {
        if let Some(cte) = self.ctes.last_mut() {
            cte.recursive = true;
            cte.recursive_query = Some(Box::new(recursive_part));
        }
        self
    }

    /// Set the source table for recursive join (self-reference).
    pub fn from_cte(mut self, cte_name: impl Into<String>) -> Self {
        if let Some(cte) = self.ctes.last_mut() {
            cte.source_table = Some(cte_name.into());
        }
        self
    }

    /// Chain a final SELECT from the CTE.
    /// 
    /// # Example
    /// ```ignore
    /// let final_query = cte.select_from_cte(&["id", "name", "level"]);
    /// ```
    pub fn select_from_cte(mut self, columns: &[&str]) -> Self {
        self.columns = columns.iter().map(|c| Expr::Named(c.to_string())).collect();
        self
    }
}
