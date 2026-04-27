//! CTE (Common Table Expression) builder methods.

use crate::ast::{CTEDef, Expr, Qail};

impl Qail {
    /// Convert this query into a reusable CTE definition.
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

    /// Add an inline CTE from another query.
    pub fn with(self, name: impl Into<String>, query: Qail) -> Self {
        self.with_cte(query.to_cte(name))
    }

    /// Mark the last CTE as recursive and attach the recursive query.
    pub fn recursive(mut self, recursive_part: Qail) -> Self {
        if let Some(cte) = self.ctes.last_mut() {
            cte.recursive = true;
            cte.recursive_query = Some(Box::new(recursive_part));
        }
        self
    }

    /// Set the source table of the last CTE.
    pub fn from_cte(mut self, cte_name: impl Into<String>) -> Self {
        if let Some(cte) = self.ctes.last_mut() {
            cte.source_table = Some(cte_name.into());
        }
        self
    }

    /// Replace the column list with named columns (for selecting from a CTE).
    pub fn select_from_cte(mut self, columns: &[&str]) -> Self {
        self.columns = columns.iter().map(|c| Expr::Named(c.to_string())).collect();
        self
    }

    /// Append a pre-built CTE definition.
    pub fn with_cte(mut self, cte: CTEDef) -> Self {
        self.ctes.push(cte);
        self
    }
}
