//! PostgreSQL MERGE SQL generation.

use crate::ast::{Condition, Expr, Merge, MergeAction, MergeMatchKind, MergeSource, Qail};
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::{SqlGenerator, ToSql};

/// Generate PostgreSQL `MERGE` SQL.
pub fn build_merge(cmd: &Qail, dialect: Dialect) -> String {
    if dialect != Dialect::Postgres {
        return "-- MERGE is only supported by the PostgreSQL dialect".to_string();
    }

    let Some(merge) = &cmd.merge else {
        return "/* ERROR: MERGE requires source, ON conditions, and WHEN clauses */".to_string();
    };
    if merge.on.is_empty() {
        return "/* ERROR: MERGE requires at least one ON condition */".to_string();
    }
    if merge.clauses.is_empty() {
        return "/* ERROR: MERGE requires at least one WHEN clause */".to_string();
    }
    if let Some(error) = validate_merge_shape(merge) {
        return format!("/* ERROR: {} */", error);
    }

    let generator = dialect.generator();
    let mut sql = String::new();
    push_cte_prefix(&mut sql, cmd, dialect);
    sql.push_str("MERGE INTO ");
    sql.push_str(&generator.quote_identifier(&cmd.table));
    if let Some(alias) = &merge.target_alias {
        sql.push_str(" AS ");
        sql.push_str(&generator.quote_identifier(alias));
    }

    sql.push_str(" USING ");
    sql.push_str(&merge_source_sql(
        &merge.source,
        dialect,
        generator.as_ref(),
    ));

    sql.push_str(" ON ");
    sql.push_str(&conditions_sql(&merge.on, generator.as_ref()));

    for clause in &merge.clauses {
        sql.push_str(" WHEN ");
        sql.push_str(match clause.match_kind {
            MergeMatchKind::Matched => "MATCHED",
            MergeMatchKind::NotMatchedByTarget => "NOT MATCHED BY TARGET",
            MergeMatchKind::NotMatchedBySource => "NOT MATCHED BY SOURCE",
        });
        if !clause.condition.is_empty() {
            sql.push_str(" AND ");
            sql.push_str(&conditions_sql(&clause.condition, generator.as_ref()));
        }
        sql.push_str(" THEN ");
        sql.push_str(&merge_action_sql(&clause.action, generator.as_ref()));
    }

    if let Some(returning) = &cmd.returning
        && !returning.is_empty()
    {
        sql.push_str(" RETURNING ");
        let returning_sql: Vec<String> = returning.iter().map(expr_sql).collect();
        sql.push_str(&returning_sql.join(", "));
    }

    sql
}

fn push_cte_prefix(sql: &mut String, cmd: &Qail, dialect: Dialect) {
    if cmd.ctes.is_empty() {
        return;
    }

    if cmd.ctes.iter().any(|cte| cte.recursive) {
        sql.push_str("WITH RECURSIVE ");
    } else {
        sql.push_str("WITH ");
    }

    let cte_parts = cmd
        .ctes
        .iter()
        .map(|cte| super::cte::build_single_cte(cte, dialect))
        .collect::<Vec<_>>();
    sql.push_str(&cte_parts.join(", "));
    sql.push(' ');
}

fn merge_source_sql(
    source: &MergeSource,
    dialect: Dialect,
    generator: &dyn SqlGenerator,
) -> String {
    match source {
        MergeSource::Table { name, alias } => {
            let mut sql = generator.quote_identifier(name);
            if let Some(alias) = alias {
                sql.push_str(" AS ");
                sql.push_str(&generator.quote_identifier(alias));
            }
            sql
        }
        MergeSource::Query { query, alias } => {
            let mut sql = format!("({})", query.to_sql_with_dialect(dialect));
            if let Some(alias) = alias {
                sql.push_str(" AS ");
                sql.push_str(&generator.quote_identifier(alias));
            }
            sql
        }
    }
}

fn conditions_sql(conditions: &[Condition], generator: &dyn SqlGenerator) -> String {
    conditions
        .iter()
        .map(|condition| condition.to_sql(generator, None))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn merge_action_sql(action: &MergeAction, generator: &dyn SqlGenerator) -> String {
    match action {
        MergeAction::Update { assignments } => {
            let assignments = assignments
                .iter()
                .map(|(col, expr)| {
                    format!("{} = {}", generator.quote_identifier(col), expr_sql(expr))
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("UPDATE SET {}", assignments)
        }
        MergeAction::Insert { columns, values } => {
            let mut sql = String::from("INSERT");
            if !columns.is_empty() {
                let cols = columns
                    .iter()
                    .map(|col| generator.quote_identifier(col))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(" (");
                sql.push_str(&cols);
                sql.push(')');
            }
            let values = values.iter().map(expr_sql).collect::<Vec<_>>().join(", ");
            sql.push_str(" VALUES (");
            sql.push_str(&values);
            sql.push(')');
            sql
        }
        MergeAction::Delete => "DELETE".to_string(),
        MergeAction::DoNothing => "DO NOTHING".to_string(),
    }
}

fn expr_sql(expr: &Expr) -> String {
    expr.to_string()
}

fn validate_merge_shape(merge: &Merge) -> Option<String> {
    if let MergeSource::Table { name, .. } = &merge.source
        && name.trim().is_empty()
    {
        return Some("MERGE requires a USING source table or query".to_string());
    }

    for clause in &merge.clauses {
        match (&clause.match_kind, &clause.action) {
            (MergeMatchKind::Matched, MergeAction::Insert { .. }) => {
                return Some("WHEN MATCHED cannot INSERT".to_string());
            }
            (MergeMatchKind::NotMatchedByTarget, MergeAction::Update { .. })
            | (MergeMatchKind::NotMatchedByTarget, MergeAction::Delete) => {
                return Some(
                    "WHEN NOT MATCHED BY TARGET can only INSERT or DO NOTHING".to_string(),
                );
            }
            (MergeMatchKind::NotMatchedBySource, MergeAction::Insert { .. }) => {
                return Some("WHEN NOT MATCHED BY SOURCE cannot INSERT".to_string());
            }
            (_, MergeAction::Update { assignments }) if assignments.is_empty() => {
                return Some("MERGE UPDATE requires at least one assignment".to_string());
            }
            (_, MergeAction::Insert { columns, values }) => {
                if values.is_empty() {
                    return Some("MERGE INSERT requires at least one value".to_string());
                }
                if !columns.is_empty() && columns.len() != values.len() {
                    return Some("MERGE INSERT column count must match value count".to_string());
                }
            }
            _ => {}
        }
    }

    None
}
