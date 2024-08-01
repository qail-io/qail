use serde::{Deserialize, Serialize};
use crate::ast::{Action, Cage, CageKind, Expr, Condition, GroupByMode, IndexDef, Join, LogicalOp, Operator, SetOp, SortOrder, TableConstraint, Value};

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
    /// DISTINCT ON columns (Postgres-specific)
    #[serde(default)]
    pub distinct_on: Vec<String>,
    /// RETURNING clause columns (for INSERT/UPDATE/DELETE)
    /// Empty = RETURNING *, Some([]) = no RETURNING, Some([cols]) = RETURNING cols
    #[serde(default)]
    pub returning: Option<Vec<Expr>>,
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

    /// Add a sort cage (descending).
    pub fn sort_desc(mut self, column: &str) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Sort(SortOrder::Desc),
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
