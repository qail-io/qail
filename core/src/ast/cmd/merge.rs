//! Builder methods for PostgreSQL MERGE.

use crate::ast::{
    Condition, Expr, Merge, MergeAction, MergeClause, MergeMatchKind, MergeSource, Operator, Qail,
    Value,
};

impl Qail {
    /// Set a target alias for `MERGE INTO`.
    pub fn target_alias(mut self, alias: impl Into<String>) -> Self {
        self.ensure_merge().target_alias = Some(alias.into());
        self
    }

    /// Set a table source for `MERGE USING`.
    pub fn using_table(mut self, table: impl Into<String>) -> Self {
        self.ensure_merge().source = MergeSource::Table {
            name: table.into(),
            alias: None,
        };
        self
    }

    /// Set an aliased table source for `MERGE USING`.
    pub fn using_table_as(mut self, table: impl Into<String>, alias: impl Into<String>) -> Self {
        self.ensure_merge().source = MergeSource::Table {
            name: table.into(),
            alias: Some(alias.into()),
        };
        self
    }

    /// Set an aliased query source for `MERGE USING`.
    pub fn using_query_as(mut self, query: Qail, alias: impl Into<String>) -> Self {
        self.ensure_merge().source = MergeSource::Query {
            query: Box::new(query),
            alias: Some(alias.into()),
        };
        self
    }

    /// Add an `ON` condition comparing the target side to a source column.
    pub fn merge_on_column(
        mut self,
        left: impl Into<String>,
        op: Operator,
        right: impl Into<String>,
    ) -> Self {
        self.ensure_merge().on.push(Condition {
            left: Expr::Named(left.into()),
            op,
            value: Value::Column(right.into()),
            is_array_unnest: false,
        });
        self
    }

    /// Add an arbitrary `ON` condition.
    pub fn merge_on_condition(mut self, condition: Condition) -> Self {
        self.ensure_merge().on.push(condition);
        self
    }

    /// Add `WHEN MATCHED THEN UPDATE SET ...`.
    pub fn when_matched_update<S>(mut self, assignments: &[(S, Expr)]) -> Self
    where
        S: AsRef<str>,
    {
        self.push_merge_clause(
            MergeMatchKind::Matched,
            Vec::new(),
            MergeAction::Update {
                assignments: assignments
                    .iter()
                    .map(|(col, expr)| (col.as_ref().to_string(), expr.clone()))
                    .collect(),
            },
        );
        self
    }

    /// Add `WHEN MATCHED AND ... THEN UPDATE SET ...`.
    pub fn when_matched_update_if<S>(
        mut self,
        condition: Vec<Condition>,
        assignments: &[(S, Expr)],
    ) -> Self
    where
        S: AsRef<str>,
    {
        self.push_merge_clause(
            MergeMatchKind::Matched,
            condition,
            MergeAction::Update {
                assignments: assignments
                    .iter()
                    .map(|(col, expr)| (col.as_ref().to_string(), expr.clone()))
                    .collect(),
            },
        );
        self
    }

    /// Add `WHEN MATCHED THEN DELETE`.
    pub fn when_matched_delete(mut self) -> Self {
        self.push_merge_clause(MergeMatchKind::Matched, Vec::new(), MergeAction::Delete);
        self
    }

    /// Add `WHEN MATCHED THEN DO NOTHING`.
    pub fn when_matched_do_nothing(mut self) -> Self {
        self.push_merge_clause(MergeMatchKind::Matched, Vec::new(), MergeAction::DoNothing);
        self
    }

    /// Add `WHEN NOT MATCHED [BY TARGET] THEN INSERT (...) VALUES (...)`.
    pub fn when_not_matched_insert<S>(mut self, columns: &[S], values: &[Expr]) -> Self
    where
        S: AsRef<str>,
    {
        self.push_merge_clause(
            MergeMatchKind::NotMatchedByTarget,
            Vec::new(),
            MergeAction::Insert {
                columns: columns.iter().map(|col| col.as_ref().to_string()).collect(),
                values: values.to_vec(),
            },
        );
        self
    }

    /// Add `WHEN NOT MATCHED [BY TARGET] AND ... THEN INSERT (...) VALUES (...)`.
    pub fn when_not_matched_insert_if<S>(
        mut self,
        condition: Vec<Condition>,
        columns: &[S],
        values: &[Expr],
    ) -> Self
    where
        S: AsRef<str>,
    {
        self.push_merge_clause(
            MergeMatchKind::NotMatchedByTarget,
            condition,
            MergeAction::Insert {
                columns: columns.iter().map(|col| col.as_ref().to_string()).collect(),
                values: values.to_vec(),
            },
        );
        self
    }

    /// Add `WHEN NOT MATCHED [BY TARGET] THEN DO NOTHING`.
    pub fn when_not_matched_do_nothing(mut self) -> Self {
        self.push_merge_clause(
            MergeMatchKind::NotMatchedByTarget,
            Vec::new(),
            MergeAction::DoNothing,
        );
        self
    }

    /// Add `WHEN NOT MATCHED BY SOURCE THEN DELETE`.
    pub fn when_not_matched_by_source_delete(mut self) -> Self {
        self.push_merge_clause(
            MergeMatchKind::NotMatchedBySource,
            Vec::new(),
            MergeAction::Delete,
        );
        self
    }

    /// Add `WHEN NOT MATCHED BY SOURCE THEN UPDATE SET ...`.
    pub fn when_not_matched_by_source_update<S>(mut self, assignments: &[(S, Expr)]) -> Self
    where
        S: AsRef<str>,
    {
        self.push_merge_clause(
            MergeMatchKind::NotMatchedBySource,
            Vec::new(),
            MergeAction::Update {
                assignments: assignments
                    .iter()
                    .map(|(col, expr)| (col.as_ref().to_string(), expr.clone()))
                    .collect(),
            },
        );
        self
    }

    /// Add `WHEN NOT MATCHED BY SOURCE THEN DO NOTHING`.
    pub fn when_not_matched_by_source_do_nothing(mut self) -> Self {
        self.push_merge_clause(
            MergeMatchKind::NotMatchedBySource,
            Vec::new(),
            MergeAction::DoNothing,
        );
        self
    }

    fn push_merge_clause(
        &mut self,
        match_kind: MergeMatchKind,
        condition: Vec<Condition>,
        action: MergeAction,
    ) {
        self.ensure_merge().clauses.push(MergeClause {
            match_kind,
            condition,
            action,
        });
    }

    fn ensure_merge(&mut self) -> &mut Merge {
        self.merge.get_or_insert_with(|| Merge {
            target_alias: None,
            source: MergeSource::Table {
                name: String::new(),
                alias: None,
            },
            on: Vec::new(),
            clauses: Vec::new(),
        })
    }
}
