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
    /// Channel name for LISTEN/NOTIFY operations
    #[serde(default)]
    pub channel: Option<String>,
    /// Payload for NOTIFY operations
    #[serde(default)]
    pub payload: Option<String>,
    /// Savepoint name for SAVEPOINT/RELEASE/ROLLBACK TO operations
    #[serde(default)]
    pub savepoint_name: Option<String>,
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
            channel: None,
            payload: None,
            savepoint_name: None,
        }
    }
}

impl QailCmd {
    /// Create a new GET command for the given table.
    pub fn get(table: impl Into<String>) -> Self {
        Self {
            action: Action::Get,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create a placeholder command for raw SQL (used in CTE subqueries).
    pub fn raw_sql(sql: impl Into<String>) -> Self {
        Self {
            action: Action::Get,
            table: sql.into(),
            ..Default::default()
        }
    }

    /// Create a new SET (update) command for the given table.
    pub fn set(table: impl Into<String>) -> Self {
        Self {
            action: Action::Set,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create a new DEL (delete) command for the given table.
    pub fn del(table: impl Into<String>) -> Self {
        Self {
            action: Action::Del,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create a new ADD (insert) command for the given table.
    pub fn add(table: impl Into<String>) -> Self {
        Self {
            action: Action::Add,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create a new PUT (upsert) command for the given table.
    pub fn put(table: impl Into<String>) -> Self {
        Self {
            action: Action::Put,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create a new EXPORT (COPY TO STDOUT) command for the given table.
    /// 
    /// Used for bulk data export via PostgreSQL COPY protocol.
    pub fn export(table: impl Into<String>) -> Self {
        Self {
            action: Action::Export,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create a new MAKE (create table) command for the given table.
    pub fn make(table: impl Into<String>) -> Self {
        Self {
            action: Action::Make,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create a new TRUNCATE command for the given table.
    /// 
    /// TRUNCATE is faster than DELETE for removing all rows.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::truncate("temp_data");
    /// ```
    pub fn truncate(table: impl Into<String>) -> Self {
        Self {
            action: Action::Truncate,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create an EXPLAIN command to analyze a query's execution plan.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::explain("users").columns(["id", "name"]);
    /// // Generates: EXPLAIN SELECT id, name FROM users
    /// ```
    pub fn explain(table: impl Into<String>) -> Self {
        Self {
            action: Action::Explain,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create an EXPLAIN ANALYZE command to execute and analyze a query.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::explain_analyze("users").columns(["id", "name"]);
    /// // Generates: EXPLAIN ANALYZE SELECT id, name FROM users
    /// ```
    pub fn explain_analyze(table: impl Into<String>) -> Self {
        Self {
            action: Action::ExplainAnalyze,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create a LOCK TABLE command for explicit table locking.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::lock("users");
    /// // Generates: LOCK TABLE users IN ACCESS EXCLUSIVE MODE
    /// ```
    pub fn lock(table: impl Into<String>) -> Self {
        Self {
            action: Action::Lock,
            table: table.into(),
            ..Default::default()
        }
    }

    /// Create a materialized view from a query.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::{QailCmd, Operator};
    /// 
    /// // Create view definition query
    /// let view_query = QailCmd::get("users")
    ///     .columns(["id", "name"])
    ///     .filter("active", Operator::Eq, true);
    /// 
    /// // Create the materialized view
    /// let cmd = QailCmd::create_materialized_view("active_users", view_query);
    /// // Generates: CREATE MATERIALIZED VIEW active_users AS SELECT id, name FROM users WHERE active = true
    /// ```
    pub fn create_materialized_view(name: impl Into<String>, query: QailCmd) -> Self {
        Self {
            action: Action::CreateMaterializedView,
            table: name.into(),
            source_query: Some(Box::new(query)),
            ..Default::default()
        }
    }

    /// Refresh a materialized view to update its data.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::refresh_materialized_view("active_users");
    /// // Generates: REFRESH MATERIALIZED VIEW active_users
    /// ```
    pub fn refresh_materialized_view(name: impl Into<String>) -> Self {
        Self {
            action: Action::RefreshMaterializedView,
            table: name.into(),
            ..Default::default()
        }
    }

    /// Drop a materialized view.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::drop_materialized_view("active_users");
    /// // Generates: DROP MATERIALIZED VIEW active_users
    /// ```
    pub fn drop_materialized_view(name: impl Into<String>) -> Self {
        Self {
            action: Action::DropMaterializedView,
            table: name.into(),
            ..Default::default()
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

    /// ORDER BY column DESC shorthand.
    /// 
    /// # Example
    /// ```
    /// use qail_core::ast::QailCmd;
    /// let cmd = QailCmd::get("users").order_desc("created_at");
    /// ```
    pub fn order_desc(self, column: impl AsRef<str>) -> Self {
        self.order_by(column, SortOrder::Desc)
    }

    /// ORDER BY column ASC shorthand.
    pub fn order_asc(self, column: impl AsRef<str>) -> Self {
        self.order_by(column, SortOrder::Asc)
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
            channel: None,
            payload: None,
            savepoint_name: None,
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

    // =========================================================================
    // Advanced Expression Builder Methods
    // =========================================================================

    /// Add an expression column (for aggregates, functions, CASE WHEN, etc.)
    /// 
    /// # Example
    /// ```ignore
    /// use qail_core::ast::{QailCmd, Expr, AggregateFunc};
    /// let cmd = QailCmd::get("orders")
    ///     .column_expr(Expr::Aggregate {
    ///         col: "*".to_string(),
    ///         func: AggregateFunc::Count,
    ///         distinct: false,
    ///         filter: None,
    ///         alias: Some("total".to_string()),
    ///     });
    /// ```
    pub fn column_expr(mut self, expr: Expr) -> Self {
        self.columns.push(expr);
        self
    }

    /// Add multiple expression columns.
    pub fn columns_expr<I>(mut self, exprs: I) -> Self 
    where 
        I: IntoIterator<Item = Expr>,
    {
        self.columns.extend(exprs);
        self
    }

    /// Add DISTINCT ON expressions (PostgreSQL-specific).
    /// 
    /// # Example
    /// ```ignore
    /// let cmd = QailCmd::get("messages")
    ///     .distinct_on(["phone_number"])
    ///     .columns(["phone_number", "content"])
    ///     .order_by("phone_number", SortOrder::Asc)
    ///     .order_by("created_at", SortOrder::Desc);
    /// ```
    pub fn distinct_on<I, S>(mut self, cols: I) -> Self 
    where 
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.distinct_on = cols.into_iter()
            .map(|c| Expr::Named(c.as_ref().to_string()))
            .collect();
        self
    }

    /// Add DISTINCT ON with expression columns.
    pub fn distinct_on_expr<I>(mut self, exprs: I) -> Self 
    where 
        I: IntoIterator<Item = Expr>,
    {
        self.distinct_on = exprs.into_iter().collect();
        self
    }

    /// Add a filter condition using an expression (for JSON, function results, etc.)
    /// 
    /// # Example
    /// ```ignore
    /// let cmd = QailCmd::get("orders")
    ///     .filter_cond(Condition {
    ///         left: Expr::JsonAccess { 
    ///             column: "data".to_string(), 
    ///             path_segments: vec![("status".to_string(), true)],
    ///             alias: None,
    ///         },
    ///         op: Operator::Eq,
    ///         value: Value::String("active".to_string()),
    ///         is_array_unnest: false,
    ///     });
    /// ```
    pub fn filter_cond(mut self, condition: Condition) -> Self {
        let filter_cage = self.cages.iter_mut().find(|c| matches!(c.kind, CageKind::Filter));
        
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

    /// Add CTEs to this query.
    /// 
    /// # Example
    /// ```ignore
    /// let cmd = QailCmd::get("cte_results")
    ///     .with_ctes(vec![cte1, cte2, cte3])
    ///     .columns(["*"]);
    /// ```
    pub fn with_ctes(mut self, ctes: Vec<CTEDef>) -> Self {
        self.ctes = ctes;
        self
    }

    /// Add a CTE to this query.
    pub fn with_cte(mut self, cte: CTEDef) -> Self {
        self.ctes.push(cte);
        self
    }

    /// LEFT JOIN with table alias support.
    /// 
    /// # Example
    /// ```ignore
    /// let cmd = QailCmd::get("users")
    ///     .left_join_as("profiles", "p", "users.id", "p.user_id");
    /// ```
    pub fn left_join_as(
        mut self, 
        table: impl AsRef<str>, 
        alias: impl AsRef<str>,
        left_col: impl AsRef<str>, 
        right_col: impl AsRef<str>
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

    /// INNER JOIN with table alias support.
    pub fn inner_join_as(
        mut self, 
        table: impl AsRef<str>, 
        alias: impl AsRef<str>,
        left_col: impl AsRef<str>, 
        right_col: impl AsRef<str>
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

    /// Set table alias for the main table.
    /// 
    /// # Example
    /// ```ignore
    /// let cmd = QailCmd::get("users").table_alias("u");
    /// ```
    pub fn table_alias(mut self, alias: impl AsRef<str>) -> Self {
        self.table = format!("{} {}", self.table, alias.as_ref());
        self
    }

    /// Order by expression (for complex ORDER BY like CASE WHEN).
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

    /// Group by expressions (for complex GROUP BY like JSON accessors).
    pub fn group_by_expr<I>(mut self, exprs: I) -> Self 
    where 
        I: IntoIterator<Item = Expr>,
    {
        let conditions: Vec<Condition> = exprs.into_iter().map(|e| Condition {
            left: e,
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
}
