//! CTE (Common Table Expression) builder methods.

use crate::ast::{Action, CTEDef, Expr, GroupByMode, Qail};

impl Qail {
    pub fn to_cte(self, name: impl Into<String>) -> CTEDef {
        let cte_name = name.into();
        let columns: Vec<String> = self
            .columns
            .iter()
            .filter_map(|c| match c {
                Expr::Named(n) => Some(n.clone()),
                Expr::Aliased { alias, .. } => Some(alias.clone()),
                _ => None,
            })
            .collect();

        CTEDef {
            name: cte_name,
            recursive: false,
            columns,
            base_query: Box::new(self),
            recursive_query: None,
            source_table: None,
        }
    }

    pub fn with(self, name: impl Into<String>, query: Qail) -> Self {
        self.with_cte(query.to_cte(name))
    }

    #[deprecated(
        since = "0.13.0",
        note = "Use .to_cte() for reusable CTEDef or .with() for inline CTE"
    )]
    pub fn as_cte(self, name: impl Into<String>) -> Self {
        let cte_name = name.into();
        let columns: Vec<String> = self
            .columns
            .iter()
            .filter_map(|c| match c {
                Expr::Named(n) => Some(n.clone()),
                Expr::Aliased { alias, .. } => Some(alias.clone()),
                _ => None,
            })
            .collect();

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
            from_tables: vec![],
            using_tables: vec![],
            lock_mode: None,
            fetch: None,
            default_values: false,
            overriding: None,
            sample: None,
            only_table: false,
            vector: None,
            score_threshold: None,
            vector_name: None,
            with_vector: false,
            vector_size: None,
            distance: None,
            on_disk: None,
            function_def: None,
            trigger_def: None,
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

    pub fn recursive(mut self, recursive_part: Qail) -> Self {
        if let Some(cte) = self.ctes.last_mut() {
            cte.recursive = true;
            cte.recursive_query = Some(Box::new(recursive_part));
        }
        self
    }

    pub fn from_cte(mut self, cte_name: impl Into<String>) -> Self {
        if let Some(cte) = self.ctes.last_mut() {
            cte.source_table = Some(cte_name.into());
        }
        self
    }

    pub fn select_from_cte(mut self, columns: &[&str]) -> Self {
        self.columns = columns.iter().map(|c| Expr::Named(c.to_string())).collect();
        self
    }

    pub fn with_cte(mut self, cte: CTEDef) -> Self {
        self.ctes.push(cte);
        self
    }
}
