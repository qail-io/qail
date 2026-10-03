//! GROUP BY resolution shared by the SQL transpiler and the native encoder.

use std::borrow::Cow;

use crate::ast::{CageKind, Expr, GroupByMode, Qail};

/// GROUP BY clause resolved from explicit keys, the grouping mode, and the
/// projection.
#[derive(Debug, Clone, PartialEq)]
pub enum GroupByClause<'a> {
    /// `GROUP BY k1, k2`
    Keys(Vec<Cow<'a, Expr>>),
    /// `GROUP BY ROLLUP(k1, k2)`
    Rollup(Vec<Cow<'a, Expr>>),
    /// `GROUP BY CUBE(k1, k2)`
    Cube(Vec<Cow<'a, Expr>>),
    /// `GROUP BY GROUPING SETS ((a, b), (a), ())`
    GroupingSets(&'a [Vec<String>]),
}

impl Qail {
    /// Resolve the GROUP BY clause for a SELECT that projects `columns`.
    ///
    /// Keys from every `.group_by()` / `.group_by_expr()` cage are used as
    /// given. Only when there are none are keys inferred from the projection
    /// (plain columns and JSON access, aliases stripped): for plain grouping
    /// that requires a projected aggregate, for ROLLUP/CUBE it does not.
    ///
    /// Errors when the mode cannot be rendered as PostgreSQL accepts it:
    /// ROLLUP/CUBE with no key, GROUPING SETS with no set, or GROUPING SETS
    /// combined with explicit keys.
    pub fn group_by_clause<'a>(
        &'a self,
        columns: &'a [Expr],
    ) -> Result<Option<GroupByClause<'a>>, &'static str> {
        let explicit: Vec<Cow<'a, Expr>> = self
            .cages
            .iter()
            .filter(|cage| cage.kind == CageKind::Partition)
            .flat_map(|cage| cage.conditions.iter())
            .map(|condition| Cow::Borrowed(&condition.left))
            .collect();

        match &self.group_by_mode {
            GroupByMode::GroupingSets(_) if !explicit.is_empty() => {
                Err("GROUPING SETS cannot be combined with explicit GROUP BY keys")
            }
            GroupByMode::GroupingSets(sets) if sets.is_empty() => {
                Err("GROUPING SETS requires at least one set")
            }
            GroupByMode::GroupingSets(sets) => Ok(Some(GroupByClause::GroupingSets(sets))),
            GroupByMode::Simple => {
                let keys = if explicit.is_empty()
                    && columns.iter().any(|e| matches!(e, Expr::Aggregate { .. }))
                {
                    projection_group_keys(columns)
                } else {
                    explicit
                };
                Ok((!keys.is_empty()).then_some(GroupByClause::Keys(keys)))
            }
            GroupByMode::Rollup => {
                let keys = keys_or_projection(explicit, columns)
                    .ok_or("ROLLUP requires at least one grouping key")?;
                Ok(Some(GroupByClause::Rollup(keys)))
            }
            GroupByMode::Cube => {
                let keys = keys_or_projection(explicit, columns)
                    .ok_or("CUBE requires at least one grouping key")?;
                Ok(Some(GroupByClause::Cube(keys)))
            }
        }
    }
}

fn keys_or_projection<'a>(
    explicit: Vec<Cow<'a, Expr>>,
    columns: &'a [Expr],
) -> Option<Vec<Cow<'a, Expr>>> {
    let keys = if explicit.is_empty() {
        projection_group_keys(columns)
    } else {
        explicit
    };
    (!keys.is_empty()).then_some(keys)
}

fn projection_group_keys(columns: &[Expr]) -> Vec<Cow<'_, Expr>> {
    columns
        .iter()
        .filter_map(|expr| match expr {
            Expr::Named(_) => Some(Cow::Borrowed(expr)),
            Expr::Aliased { name, .. } => Some(Cow::Owned(Expr::Named(name.clone()))),
            Expr::JsonAccess { alias: None, .. } => Some(Cow::Borrowed(expr)),
            Expr::JsonAccess {
                column,
                path_segments,
                alias: Some(_),
            } => Some(Cow::Owned(Expr::JsonAccess {
                column: column.clone(),
                path_segments: path_segments.clone(),
                alias: None,
            })),
            _ => None,
        })
        .collect()
}
