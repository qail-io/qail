//! INSERT SQL generation.

use crate::ast::*;
use crate::transpiler::SqlGenerator;
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;

pub fn build_insert(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::from("INSERT INTO ");
    sql.push_str(&generator.quote_identifier(&cmd.table));

    // For ADD queries, we use columns and first cage contains values
    let cols: Vec<String> = cmd.columns.iter().map(|c| c.to_string()).collect();

    if !cols.is_empty() {
        sql.push_str(" (");
        sql.push_str(&cols.join(", "));
        sql.push(')');
    }

    // OVERRIDING clause for GENERATED columns
    if let Some(ref overriding) = cmd.overriding {
        match overriding {
            OverridingKind::SystemValue => sql.push_str(" OVERRIDING SYSTEM VALUE"),
            OverridingKind::UserValue => sql.push_str(" OVERRIDING USER VALUE"),
        }
    }

    // DEFAULT VALUES - insert a row with all defaults
    if cmd.default_values {
        sql.push_str(" DEFAULT VALUES");
    } else if let Some(ref source_query) = cmd.source_query {
        // INSERT...SELECT: use source_query if present
        use crate::transpiler::ToSql;
        sql.push(' ');
        sql.push_str(&source_query.to_sql_with_dialect(dialect));
    } else if let Some(cage) = cmd.cages.first() {
        // Traditional INSERT with VALUES
        let values: Vec<String> = cage
            .conditions
            .iter()
            .map(|c| c.to_value_sql(&generator))
            .collect();

        if !values.is_empty() {
            sql.push_str(" VALUES (");
            sql.push_str(&values.join(", "));
            sql.push(')');
        }
    }

    // ON CONFLICT clause
    if let Some(on_conflict) = &cmd.on_conflict {
        sql.push_str(&build_on_conflict(
            on_conflict,
            &dialect,
            generator.as_ref(),
        ));
    }

    match &cmd.returning {
        None => sql.push_str(" RETURNING *"), // Default: return all
        Some(cols) if cols.is_empty() => {}   // Explicitly no RETURNING
        Some(cols) => {
            let col_strs: Vec<String> = cols.iter().map(|e| e.to_string()).collect();
            sql.push_str(" RETURNING ");
            sql.push_str(&col_strs.join(", "));
        }
    }

    sql
}

/// Build ON CONFLICT clause (Standard SQL / Postgres / SQLite style)
fn build_on_conflict(
    on_conflict: &OnConflict,
    _dialect: &Dialect,
    generator: &dyn SqlGenerator,
) -> String {
    // Both Postgres and SQLite support ON CONFLICT
    build_on_conflict_postgres(on_conflict, generator)
}

/// PostgreSQL/SQLite style: ON CONFLICT (cols) DO UPDATE SET ... or DO NOTHING
fn build_on_conflict_postgres(on_conflict: &OnConflict, generator: &dyn SqlGenerator) -> String {
    let mut sql = String::from(" ON CONFLICT (");
    let cols: Vec<String> = on_conflict
        .columns
        .iter()
        .map(|c| generator.quote_identifier(c))
        .collect();
    sql.push_str(&cols.join(", "));
    sql.push(')');

    match &on_conflict.action {
        ConflictAction::DoNothing => {
            sql.push_str(" DO NOTHING");
        }
        ConflictAction::DoUpdate { assignments } => {
            sql.push_str(" DO UPDATE SET ");
            let sets: Vec<String> = assignments
                .iter()
                .map(|(col, expr)| format!("{} = {}", generator.quote_identifier(col), expr))
                .collect();
            sql.push_str(&sets.join(", "));
        }
    }

    sql
}
