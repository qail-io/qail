//! PostgreSQL MERGE SQL generation.

use crate::ast::{
    Action, Condition, Expr, Merge, MergeAction, MergeMatchKind, MergeSource, Operator, Qail, Value,
};
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::traits::escape_sql_string_literal;
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
        let returning_sql: Vec<String> = returning
            .iter()
            .map(|expr| expr_sql(expr, generator.as_ref()))
            .collect();
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
        .map(|condition| condition_sql(condition, generator))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn condition_sql(condition: &Condition, generator: &dyn SqlGenerator) -> String {
    if condition.is_array_unnest {
        return condition.to_sql(generator, None);
    }

    let left = expr_sql(&condition.left, generator);
    match condition.op {
        Operator::Fuzzy => {
            let value = match &condition.value {
                Value::String(value) => format!("'%{}%'", value.replace('\'', "''")),
                Value::Param(index) => {
                    let placeholder = generator.placeholder(*index);
                    generator.string_concat(&["'%'", &placeholder, "'%'"])
                }
                value => format!("'%{}%'", value_sql(value, generator)),
            };
            format!("{left} {} {value}", generator.fuzzy_operator())
        }
        Operator::IsNull => format!("{left} IS NULL"),
        Operator::IsNotNull => format!("{left} IS NOT NULL"),
        Operator::In | Operator::NotIn => {
            if let Value::Array(values) = &condition.value {
                let values = values
                    .iter()
                    .map(|value| value_sql(value, generator))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{left} {} ({values})", condition.op.sql_symbol())
            } else if condition.op == Operator::In {
                generator.in_array(&left, &value_sql(&condition.value, generator))
            } else {
                generator.not_in_array(&left, &value_sql(&condition.value, generator))
            }
        }
        Operator::Contains => {
            generator.json_contains(&left, &value_sql(&condition.value, generator))
        }
        Operator::KeyExists => {
            generator.json_key_exists(&left, &value_sql(&condition.value, generator))
        }
        Operator::JsonExists => {
            let path = value_sql(&condition.value, generator);
            generator.json_exists(&left, path.trim_matches('\''))
        }
        Operator::JsonQuery => {
            let path = value_sql(&condition.value, generator);
            generator.json_query(&left, path.trim_matches('\''))
        }
        Operator::JsonValue => {
            let path = value_sql(&condition.value, generator);
            format!(
                "{} = {}",
                generator.json_value(&left, path.trim_matches('\'')),
                value_sql(&condition.value, generator)
            )
        }
        Operator::Between | Operator::NotBetween => {
            if let Value::Array(values) = &condition.value
                && values.len() >= 2
            {
                return format!(
                    "{left} {} {} AND {}",
                    condition.op.sql_symbol(),
                    value_sql(&values[0], generator),
                    value_sql(&values[1], generator)
                );
            }
            format!(
                "{left} {} {}",
                condition.op.sql_symbol(),
                value_sql(&condition.value, generator)
            )
        }
        Operator::Exists | Operator::NotExists => match &condition.value {
            Value::Subquery(query) => {
                let keyword = condition.op.sql_symbol();
                format!("{keyword} ({})", query.to_sql())
            }
            _ => format!(
                "{} ({})",
                condition.op.sql_symbol(),
                value_sql(&condition.value, generator)
            ),
        },
        _ => format!(
            "{left} {} {}",
            condition.op.sql_symbol(),
            value_sql(&condition.value, generator)
        ),
    }
}

fn value_sql(value: &Value, generator: &dyn SqlGenerator) -> String {
    match value {
        Value::Column(column) => render_named_expr(column, generator),
        Value::Expr(expr) => expr_sql(expr, generator),
        Value::Subquery(query) => format!("({})", query.to_sql()),
        _ => value.to_string(),
    }
}

fn merge_action_sql(action: &MergeAction, generator: &dyn SqlGenerator) -> String {
    match action {
        MergeAction::Update { assignments } => {
            let assignments = assignments
                .iter()
                .map(|(col, expr)| {
                    format!(
                        "{} = {}",
                        generator.quote_identifier(col),
                        expr_sql(expr, generator)
                    )
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
            let values = values
                .iter()
                .map(|expr| expr_sql(expr, generator))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(" VALUES (");
            sql.push_str(&values);
            sql.push(')');
            sql
        }
        MergeAction::Delete => "DELETE".to_string(),
        MergeAction::DoNothing => "DO NOTHING".to_string(),
    }
}

fn expr_sql(expr: &Expr, generator: &dyn SqlGenerator) -> String {
    match expr {
        Expr::Named(name) => render_named_expr(name, generator),
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            let mut sql = String::from("CASE");
            for (condition, value) in when_clauses {
                sql.push_str(" WHEN ");
                sql.push_str(&condition_sql(condition, generator));
                sql.push_str(" THEN ");
                sql.push_str(&expr_sql(value, generator));
            }
            if let Some(value) = else_value {
                sql.push_str(" ELSE ");
                sql.push_str(&expr_sql(value, generator));
            }
            sql.push_str(" END");
            sql
        }
        Expr::Binary {
            left, op, right, ..
        } => match op {
            crate::ast::BinaryOp::IsNull => {
                format!("({} IS NULL)", expr_sql(left, generator))
            }
            crate::ast::BinaryOp::IsNotNull => {
                format!("({} IS NOT NULL)", expr_sql(left, generator))
            }
            _ => format!(
                "({} {} {})",
                expr_sql(left, generator),
                op,
                expr_sql(right, generator)
            ),
        },
        Expr::FunctionCall { name, args, .. } => {
            let args = args
                .iter()
                .map(|arg| expr_sql(arg, generator))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({})", name.to_uppercase(), args)
        }
        Expr::Cast {
            expr, target_type, ..
        } => {
            let inner = expr_sql(expr, generator);
            if matches!(expr.as_ref(), Expr::JsonAccess { .. } | Expr::Case { .. }) {
                format!("({inner})::{target_type}")
            } else {
                format!("{inner}::{target_type}")
            }
        }
        Expr::JsonAccess {
            column,
            path_segments,
            ..
        } => {
            let mut sql = generator.quote_identifier(column);
            for (path, as_text) in path_segments {
                let op = if *as_text { "->>" } else { "->" };
                if path.parse::<i64>().is_ok() {
                    sql.push_str(&format!("{}{}", op, path));
                } else {
                    sql.push_str(&format!("{}'{}'", op, escape_sql_string_literal(path)));
                }
            }
            sql
        }
        _ => expr.to_string(),
    }
}

fn render_named_expr(name: &str, generator: &dyn SqlGenerator) -> String {
    if name == "*"
        || name.contains('(')
        || name.starts_with('\'')
        || name.starts_with('"')
        || name.starts_with(':')
        || name.starts_with('$')
        || name.parse::<f64>().is_ok()
        || name.eq_ignore_ascii_case("NULL")
        || name.eq_ignore_ascii_case("TRUE")
        || name.eq_ignore_ascii_case("FALSE")
    {
        name.to_string()
    } else {
        generator.quote_identifier(name)
    }
}

fn validate_merge_shape(merge: &Merge) -> Option<String> {
    match &merge.source {
        MergeSource::Table { name, .. } if name.trim().is_empty() => {
            return Some("MERGE requires a USING source table or query".to_string());
        }
        MergeSource::Query { query, .. } => {
            if let Some(error) = validate_merge_source_query(query) {
                return Some(error);
            }
        }
        _ => {}
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

fn validate_merge_source_query(query: &Qail) -> Option<String> {
    if !matches!(query.action, Action::Get | Action::With) {
        return Some(format!(
            "MERGE source query must be read-only SELECT, got {}",
            query.action
        ));
    }

    for cte in &query.ctes {
        if let Some(error) = validate_merge_source_query(&cte.base_query) {
            return Some(error);
        }
        if let Some(ref recursive_query) = cte.recursive_query
            && let Some(error) = validate_merge_source_query(recursive_query)
        {
            return Some(error);
        }
    }
    for (_, set_query) in &query.set_ops {
        if let Some(error) = validate_merge_source_query(set_query) {
            return Some(error);
        }
    }

    None
}
