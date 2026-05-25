use super::ToSql;
use super::traits::{SqlGenerator, escape_sql_string_literal};
use crate::ast::*;
use crate::transpiler::identifier::qualifier_for_column_reference;

/// Context for parameterized query building.
#[derive(Debug, Default)]
pub struct ParamContext {
    /// Current parameter index (1-based).
    pub index: usize,
    /// Collected parameter values in order
    pub params: Vec<Value>,
    /// Names of named parameters in order (for :name → $n mapping)
    pub named_params: Vec<String>,
}

impl ParamContext {
    /// Create a new empty parameter context.
    pub fn new() -> Self {
        Self {
            index: 0,
            params: Vec::new(),
            named_params: Vec::new(),
        }
    }

    /// Add a value and return the placeholder for it.
    pub fn add_param(&mut self, value: Value, generator: &dyn SqlGenerator) -> String {
        self.index += 1;
        self.params.push(value);
        generator.placeholder(self.index)
    }

    /// Add a named parameter and return the placeholder for it.
    pub fn add_named_param(&mut self, name: String, generator: &dyn SqlGenerator) -> String {
        self.index += 1;
        self.named_params.push(name);
        generator.placeholder(self.index)
    }
}

/// Heuristic:
/// 1. Split by '.'
/// 2. If single part -> quote_identifier
/// 3. If multiple parts:
///    - If first part matches table name or any join alias -> Treat as "Table"."Col".
///    - Else -> Treat as "Col"->"Field" (JSON).
fn resolve_col_syntax(col: &str, cmd: &Qail, generator: &dyn SqlGenerator) -> String {
    if col.starts_with('{') && col.ends_with('}') {
        return col[1..col.len() - 1].to_string();
    }

    let parts: Vec<&str> = col.split('.').collect();
    if parts.len() <= 1 {
        return generator.quote_identifier(col);
    }

    let first = parts[0];

    if let Some(sql_qualifier) = qualifier_for_column_reference(&cmd.table, first) {
        return format!(
            "{}.{}",
            generator.quote_identifier(sql_qualifier),
            generator.quote_identifier(&parts[1..].join("."))
        );
    }

    for join in &cmd.joins {
        if let Some(sql_qualifier) = qualifier_for_column_reference(&join.table, first) {
            return format!(
                "{}.{}",
                generator.quote_identifier(sql_qualifier),
                generator.quote_identifier(&parts[1..].join("."))
            );
        }
    }

    // Default: treated as JSON access on the first part
    let col_name = parts[0];
    let path = &parts[1..];
    generator.json_access(col_name, path)
}

fn resolve_text_search_vector(
    expr: &Expr,
    generator: &dyn SqlGenerator,
    context: Option<&Qail>,
) -> Option<String> {
    let Expr::Named(columns) = expr else {
        return None;
    };

    let parts: Vec<String> = columns
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(|column| {
            let rendered = if let Some(cmd) = context {
                resolve_col_syntax(column, cmd, generator)
            } else {
                generator.quote_identifier(column)
            };
            format!("coalesce({}, '')", rendered)
        })
        .collect();

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" || ' ' || "))
    }
}

fn condition_left_sql(expr: &Expr, generator: &dyn SqlGenerator, context: Option<&Qail>) -> String {
    match expr {
        Expr::Named(name) => {
            if name.starts_with('{') && name.ends_with('}') {
                name[1..name.len() - 1].to_string()
            } else if let Some(cmd) = context {
                resolve_col_syntax(name, cmd, generator)
            } else {
                generator.quote_identifier(name)
            }
        }
        Expr::JsonAccess {
            column,
            path_segments,
            ..
        } => render_json_access(column, path_segments, generator),
        Expr::Literal(value) => condition_value_sql(value, generator),
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            let mut sql = String::from("CASE");
            for (condition, value) in when_clauses {
                sql.push_str(&format!(
                    " WHEN {} THEN {}",
                    condition.to_sql(generator, context),
                    condition_left_sql(value, generator, context)
                ));
            }
            if let Some(value) = else_value {
                sql.push_str(&format!(
                    " ELSE {}",
                    condition_left_sql(value, generator, context)
                ));
            }
            sql.push_str(" END");
            sql
        }
        Expr::Binary {
            left, op, right, ..
        } => {
            let left = condition_left_sql(left, generator, context);
            let right = condition_left_sql(right, generator, context);
            match op {
                BinaryOp::IsNull => format!("({left} IS NULL)"),
                BinaryOp::IsNotNull => format!("({left} IS NOT NULL)"),
                _ => format!("({left} {op} {right})"),
            }
        }
        Expr::FunctionCall { name, args, .. } => {
            let Some(function) = render_function_name(name) else {
                return "/* ERROR: Invalid function name */".to_string();
            };
            let args = args
                .iter()
                .map(|arg| condition_left_sql(arg, generator, context))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{function}({args})")
        }
        Expr::SpecialFunction { name, args, .. } => {
            let Some(function) = render_function_name(name) else {
                return "/* ERROR: Invalid function name */".to_string();
            };
            let mut parts = Vec::new();
            for (keyword, expr) in args {
                let expr = condition_left_sql(expr, generator, context);
                if let Some(keyword) = keyword {
                    let Some(keyword) = render_sql_keyword(keyword) else {
                        return "/* ERROR: Invalid function keyword */".to_string();
                    };
                    parts.push(format!("{keyword} {expr}"));
                } else {
                    parts.push(expr);
                }
            }
            format!("{function}({})", parts.join(" "))
        }
        Expr::Cast {
            expr, target_type, ..
        } => {
            let Some(target_type) = checked_sql_type_fragment(target_type) else {
                return "/* ERROR: Invalid cast target type */".to_string();
            };
            format!(
                "{}::{}",
                condition_left_sql(expr, generator, context),
                target_type
            )
        }
        Expr::Collate {
            expr, collation, ..
        } => format!(
            "{} COLLATE {}",
            condition_left_sql(expr, generator, context),
            render_qualified_identifier(collation, generator)
        ),
        Expr::FieldAccess { expr, field, .. } => format!(
            "({}).{}",
            condition_left_sql(expr, generator, context),
            render_qualified_identifier(field, generator)
        ),
        Expr::ArrayConstructor { elements, .. } => {
            let elements = elements
                .iter()
                .map(|element| condition_left_sql(element, generator, context))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ARRAY[{elements}]")
        }
        Expr::RowConstructor { elements, .. } => {
            let elements = elements
                .iter()
                .map(|element| condition_left_sql(element, generator, context))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ROW({elements})")
        }
        Expr::Subscript { expr, index, .. } => format!(
            "{}[{}]",
            condition_left_sql(expr, generator, context),
            condition_left_sql(index, generator, context)
        ),
        Expr::Subquery { query, .. } => format!("({})", read_only_subquery_sql(query)),
        Expr::Exists { query, negated, .. } => {
            if *negated {
                format!("NOT EXISTS ({})", read_only_subquery_sql(query))
            } else {
                format!("EXISTS ({})", read_only_subquery_sql(query))
            }
        }
        _ => "/* ERROR: Invalid condition expression */".to_string(),
    }
}

pub(crate) fn read_only_subquery_sql(query: &Qail) -> String {
    if let Some(error) = validate_read_only_subquery(query) {
        format!("/* ERROR: {error} */")
    } else {
        query.to_sql()
    }
}

fn validate_read_only_subquery(query: &Qail) -> Option<String> {
    if !matches!(query.action, Action::Get | Action::Cnt | Action::With) {
        return Some(format!(
            "subquery must be read-only SELECT, got {}",
            query.action
        ));
    }

    for column in &query.columns {
        if let Some(error) = validate_read_only_expr(column) {
            return Some(error);
        }
    }
    for expr in &query.distinct_on {
        if let Some(error) = validate_read_only_expr(expr) {
            return Some(error);
        }
    }
    for cage in &query.cages {
        for condition in &cage.conditions {
            if let Some(error) = validate_read_only_condition(condition) {
                return Some(error);
            }
        }
    }
    for condition in &query.having {
        if let Some(error) = validate_read_only_condition(condition) {
            return Some(error);
        }
    }
    for join in &query.joins {
        if let Some(conditions) = &join.on {
            for condition in conditions {
                if let Some(error) = validate_read_only_condition(condition) {
                    return Some(error);
                }
            }
        }
    }
    for cte in &query.ctes {
        if let Some(error) = validate_read_only_subquery(&cte.base_query) {
            return Some(error);
        }
        if let Some(recursive_query) = &cte.recursive_query
            && let Some(error) = validate_read_only_subquery(recursive_query)
        {
            return Some(error);
        }
    }
    for (_, set_query) in &query.set_ops {
        if let Some(error) = validate_read_only_subquery(set_query) {
            return Some(error);
        }
    }
    if let Some(source_query) = &query.source_query
        && let Some(error) = validate_read_only_subquery(source_query)
    {
        return Some(error);
    }
    if let Some(returning) = &query.returning {
        for expr in returning {
            if let Some(error) = validate_read_only_expr(expr) {
                return Some(error);
            }
        }
    }

    None
}

fn validate_read_only_condition(condition: &Condition) -> Option<String> {
    validate_read_only_expr(&condition.left).or_else(|| validate_read_only_value(&condition.value))
}

fn validate_read_only_value(value: &Value) -> Option<String> {
    match value {
        Value::Subquery(query) => validate_read_only_subquery(query),
        Value::Expr(expr) => validate_read_only_expr(expr),
        Value::Array(values) => values.iter().find_map(validate_read_only_value),
        _ => None,
    }
}

fn validate_read_only_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            for (condition, value) in when_clauses {
                if let Some(error) = validate_read_only_condition(condition)
                    .or_else(|| validate_read_only_expr(value))
                {
                    return Some(error);
                }
            }
            else_value
                .as_ref()
                .and_then(|expr| validate_read_only_expr(expr))
        }
        Expr::Binary { left, right, .. } => {
            validate_read_only_expr(left).or_else(|| validate_read_only_expr(right))
        }
        Expr::FunctionCall { args, .. } => args.iter().find_map(validate_read_only_expr),
        Expr::SpecialFunction { args, .. } => args
            .iter()
            .find_map(|(_, expr)| validate_read_only_expr(expr)),
        Expr::Cast { expr, .. } | Expr::FieldAccess { expr, .. } | Expr::Collate { expr, .. } => {
            validate_read_only_expr(expr)
        }
        Expr::ArrayConstructor { elements, .. } | Expr::RowConstructor { elements, .. } => {
            elements.iter().find_map(validate_read_only_expr)
        }
        Expr::Subscript { expr, index, .. } => {
            validate_read_only_expr(expr).or_else(|| validate_read_only_expr(index))
        }
        Expr::Subquery { query, .. } | Expr::Exists { query, .. } => {
            validate_read_only_subquery(query)
        }
        _ => None,
    }
}

fn render_function_name(name: &str) -> Option<String> {
    if name.is_empty()
        || name.contains('\0')
        || name.split('.').any(str::is_empty)
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
    {
        None
    } else {
        Some(name.to_uppercase())
    }
}

fn render_sql_keyword(keyword: &str) -> Option<String> {
    if keyword.is_empty()
        || keyword.contains('\0')
        || !keyword
            .bytes()
            .all(|b| b.is_ascii_alphabetic() || b == b'_')
    {
        None
    } else {
        Some(keyword.to_uppercase())
    }
}

fn checked_sql_type_fragment(fragment: &str) -> Option<String> {
    let fragment = fragment.trim();
    if fragment.is_empty()
        || fragment.contains('\0')
        || fragment.contains(';')
        || fragment.contains('\'')
        || fragment.contains('"')
        || fragment.contains("--")
        || fragment.contains("/*")
        || fragment.contains("*/")
        || !fragment.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'_' | b'.' | b' ' | b'(' | b')' | b',' | b'[' | b']' | b'%' | b'+' | b'-'
                )
        })
    {
        None
    } else {
        Some(fragment.to_string())
    }
}

fn render_qualified_identifier(value: &str, generator: &dyn SqlGenerator) -> String {
    if value.is_empty() || value.as_bytes().contains(&0) || value.split('.').any(str::is_empty) {
        "/* ERROR: Invalid identifier */".to_string()
    } else {
        generator.quote_identifier(value)
    }
}

fn render_json_access(
    column: &str,
    path_segments: &[(String, bool)],
    generator: &dyn SqlGenerator,
) -> String {
    let mut result = generator.quote_identifier(column);
    for (path, as_text) in path_segments {
        let op = if *as_text { "->>" } else { "->" };
        if path.parse::<i64>().is_ok() {
            result.push_str(&format!("{}{}", op, path));
        } else {
            result.push_str(&format!("{}'{}'", op, escape_sql_string_literal(path)));
        }
    }
    result
}

fn fuzzy_pattern_sql(value: &Value, generator: &dyn SqlGenerator) -> String {
    match value {
        Value::String(s) => format!("'%{}%'", escape_sql_string_literal(s)),
        Value::Param(n) => {
            let p = generator.placeholder(*n);
            generator.string_concat(&["'%'", &p, "'%'"])
        }
        Value::NamedParam(name) => {
            let p = format!(":{}", name);
            generator.string_concat(&["'%'", &p, "'%'"])
        }
        v => format!("'%{}%'", escape_sql_string_literal(&v.to_string())),
    }
}

fn json_path_arg(condition: &Condition, generator: &dyn SqlGenerator) -> String {
    match &condition.value {
        Value::String(path) => path.clone(),
        Value::Param(n) => generator.placeholder(*n),
        Value::NamedParam(name) => format!(":{}", name),
        _ => condition.to_value_sql(generator),
    }
}

fn condition_value_sql(value: &Value, generator: &dyn SqlGenerator) -> String {
    match value {
        Value::Param(n) => generator.placeholder(*n),
        Value::String(s) => format!("'{}'", escape_sql_string_literal(s)),
        Value::Bool(b) => generator.bool_literal(*b),
        Value::Subquery(cmd) => format!("({})", read_only_subquery_sql(cmd)),
        Value::Column(col) => {
            if col.contains('.') {
                col.split('.')
                    .map(|part| generator.quote_identifier(part))
                    .collect::<Vec<_>>()
                    .join(".")
            } else {
                generator.quote_identifier(col)
            }
        }
        Value::Array(values) => {
            let values = values
                .iter()
                .map(|value| condition_value_sql(value, generator))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({values})")
        }
        Value::Expr(expr) => condition_left_sql(expr, generator, None),
        v => v.to_string(),
    }
}

fn in_condition_sql(
    col: &str,
    op: Operator,
    value: &Value,
    generator: &dyn SqlGenerator,
) -> String {
    match value {
        Value::Array(values) if !values.is_empty() => {
            let values = values
                .iter()
                .map(|value| condition_value_sql(value, generator))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{col} {} ({values})", op.sql_symbol())
        }
        Value::Subquery(_) => {
            format!(
                "{col} {} {}",
                op.sql_symbol(),
                condition_value_sql(value, generator)
            )
        }
        Value::Param(_) | Value::NamedParam(_) if op == Operator::In => {
            generator.in_array(col, &condition_value_sql(value, generator))
        }
        Value::Param(_) | Value::NamedParam(_) => {
            generator.not_in_array(col, &condition_value_sql(value, generator))
        }
        _ => invalid_in_condition_sql(),
    }
}

fn invalid_exists_condition_sql() -> String {
    "FALSE /* ERROR: EXISTS condition requires subquery value */".to_string()
}

fn invalid_in_condition_sql() -> String {
    "FALSE /* ERROR: IN condition requires a non-empty array, subquery, or array parameter */"
        .to_string()
}

fn invalid_between_condition_sql() -> String {
    "FALSE /* ERROR: BETWEEN condition requires exactly two array values */".to_string()
}

/// Trait for converting AST conditions to SQL strings.
pub trait ConditionToSql {
    /// Render this condition as a SQL string.
    fn to_sql(&self, generator: &dyn SqlGenerator, context: Option<&Qail>) -> String;
    /// Render the right-hand value of this condition as a SQL string.
    fn to_value_sql(&self, generator: &dyn SqlGenerator) -> String;

    /// Convert condition to SQL with parameterized values.
    fn to_sql_parameterized(
        &self,
        generator: &dyn SqlGenerator,
        context: Option<&Qail>,
        params: &mut ParamContext,
    ) -> String;
}

impl ConditionToSql for Condition {
    /// Convert condition to SQL string.
    fn to_sql(&self, generator: &dyn SqlGenerator, context: Option<&Qail>) -> String {
        let col = condition_left_sql(&self.left, generator, context);

        if self.is_array_unnest {
            let inner_condition = match self.op {
                Operator::Eq => format!("_el = {}", self.to_value_sql(generator)),
                Operator::Ne => format!("_el != {}", self.to_value_sql(generator)),
                Operator::Gt => format!("_el > {}", self.to_value_sql(generator)),
                Operator::Gte => format!("_el >= {}", self.to_value_sql(generator)),
                Operator::Lt => format!("_el < {}", self.to_value_sql(generator)),
                Operator::Lte => format!("_el <= {}", self.to_value_sql(generator)),
                Operator::Fuzzy => {
                    let val = fuzzy_pattern_sql(&self.value, generator);
                    format!("_el {} {}", generator.fuzzy_operator(), val)
                }
                Operator::ArrayElemContainedInText => format!(
                    "LOWER({}) LIKE '%' || LOWER(_el) || '%'",
                    self.to_value_sql(generator)
                ),
                _ => format!("_el = {}", self.to_value_sql(generator)),
            };
            return format!(
                "EXISTS (SELECT 1 FROM unnest({}) _el WHERE {})",
                col, inner_condition
            );
        }

        // Normal conditions
        // Simple binary operators use sql_symbol() for unified handling
        if self.op.is_simple_binary() {
            return format!(
                "{} {} {}",
                col,
                self.op.sql_symbol(),
                self.to_value_sql(generator)
            );
        }

        // Special operators that need custom handling
        match self.op {
            Operator::Fuzzy => {
                let val = fuzzy_pattern_sql(&self.value, generator);
                format!("{} {} {}", col, generator.fuzzy_operator(), val)
            }
            Operator::TextSearch => {
                let vector = resolve_text_search_vector(&self.left, generator, context)
                    .unwrap_or_else(|| col.clone());
                format!(
                    "to_tsvector('english', {}) @@ websearch_to_tsquery('english', {})",
                    vector,
                    self.to_value_sql(generator)
                )
            }
            Operator::In | Operator::NotIn => {
                in_condition_sql(&col, self.op, &self.value, generator)
            }
            Operator::IsNull => format!("{} IS NULL", col),
            Operator::IsNotNull => format!("{} IS NOT NULL", col),
            Operator::Contains => generator.json_contains(&col, &self.to_value_sql(generator)),
            Operator::KeyExists => generator.json_key_exists(&col, &self.to_value_sql(generator)),
            // Postgres 17+ SQL/JSON standard functions
            Operator::JsonExists => {
                let path = json_path_arg(self, generator);
                generator.json_exists(&col, &path)
            }
            Operator::JsonQuery => {
                let path = json_path_arg(self, generator);
                format!("{} IS NOT NULL", generator.json_query(&col, &path))
            }
            Operator::JsonValue => {
                let path = json_path_arg(self, generator);
                format!("{} IS NOT NULL", generator.json_value(&col, &path))
            }
            Operator::Between => {
                // Value is Array with 2 elements [min, max]
                if let Value::Array(vals) = &self.value
                    && vals.len() == 2
                {
                    return format!(
                        "{} BETWEEN {} AND {}",
                        col,
                        condition_value_sql(&vals[0], generator),
                        condition_value_sql(&vals[1], generator)
                    );
                }
                invalid_between_condition_sql()
            }
            Operator::NotBetween => {
                if let Value::Array(vals) = &self.value
                    && vals.len() == 2
                {
                    return format!(
                        "{} NOT BETWEEN {} AND {}",
                        col,
                        condition_value_sql(&vals[0], generator),
                        condition_value_sql(&vals[1], generator)
                    );
                }
                invalid_between_condition_sql()
            }
            Operator::Exists => {
                // EXISTS takes subquery, col is ignored
                if let Value::Subquery(cmd) = &self.value {
                    let subquery_sql = read_only_subquery_sql(cmd);
                    format!("EXISTS ({})", subquery_sql)
                } else {
                    invalid_exists_condition_sql()
                }
            }
            Operator::NotExists => {
                if let Value::Subquery(cmd) = &self.value {
                    let subquery_sql = read_only_subquery_sql(cmd);
                    format!("NOT EXISTS ({})", subquery_sql)
                } else {
                    invalid_exists_condition_sql()
                }
            }
            // Simple binary operators are handled above by is_simple_binary()
            _ => format!(
                "{} {} {}",
                col,
                self.op.sql_symbol(),
                self.to_value_sql(generator)
            ),
        }
    }

    fn to_value_sql(&self, generator: &dyn SqlGenerator) -> String {
        condition_value_sql(&self.value, generator)
    }

    fn to_sql_parameterized(
        &self,
        generator: &dyn SqlGenerator,
        context: Option<&Qail>,
        params: &mut ParamContext,
    ) -> String {
        let col = condition_left_sql(&self.left, generator, context);

        // Helper to convert value to placeholder
        let value_placeholder = |v: &Value, p: &mut ParamContext| -> String {
            match v {
                Value::Param(n) => generator.placeholder(*n), // Already a placeholder
                Value::NamedParam(name) => p.add_named_param(name.clone(), generator),
                Value::Null => "NULL".to_string(),
                other => p.add_param(other.clone(), generator),
            }
        };

        if self.is_array_unnest {
            let inner_condition = match self.op {
                Operator::Eq => format!("_el = {}", value_placeholder(&self.value, params)),
                Operator::Ne => format!("_el != {}", value_placeholder(&self.value, params)),
                Operator::Gt => format!("_el > {}", value_placeholder(&self.value, params)),
                Operator::Gte => format!("_el >= {}", value_placeholder(&self.value, params)),
                Operator::Lt => format!("_el < {}", value_placeholder(&self.value, params)),
                Operator::Lte => format!("_el <= {}", value_placeholder(&self.value, params)),
                Operator::Fuzzy => {
                    let val = generator.string_concat(&[
                        "'%'",
                        &value_placeholder(&self.value, params),
                        "'%'",
                    ]);
                    format!("_el {} {}", generator.fuzzy_operator(), val)
                }
                Operator::ArrayElemContainedInText => format!(
                    "LOWER({}) LIKE '%' || LOWER(_el) || '%'",
                    value_placeholder(&self.value, params)
                ),
                _ => format!("_el = {}", value_placeholder(&self.value, params)),
            };

            return format!(
                "EXISTS (SELECT 1 FROM unnest({}) _el WHERE {})",
                col, inner_condition
            );
        }

        match self.op {
            Operator::Eq => {
                // Raw conditions ({...}, op=Eq, value=Null) are now handled at col resolution
                if matches!(self.value, Value::Null)
                    && let Expr::Named(name) = &self.left
                    && name.starts_with('{')
                    && name.ends_with('}')
                {
                    return col; // col already contains raw SQL content
                }
                format!("{} = {}", col, value_placeholder(&self.value, params))
            }
            Operator::Fuzzy => {
                // For LIKE, we need to wrap in wildcards
                let placeholder = value_placeholder(&self.value, params);
                let pattern = generator.string_concat(&["'%'", &placeholder, "'%'"]);
                format!("{} {} {}", col, generator.fuzzy_operator(), pattern)
            }
            Operator::TextSearch => {
                let vector = resolve_text_search_vector(&self.left, generator, context)
                    .unwrap_or_else(|| col.clone());
                format!(
                    "to_tsvector('english', {}) @@ websearch_to_tsquery('english', {})",
                    vector,
                    value_placeholder(&self.value, params)
                )
            }
            Operator::IsNull => format!("{} IS NULL", col),
            Operator::IsNotNull => format!("{} IS NOT NULL", col),
            Operator::In | Operator::NotIn => match &self.value {
                Value::Array(values) if !values.is_empty() => {
                    let value = value_placeholder(&self.value, params);
                    if self.op == Operator::In {
                        generator.in_array(&col, &value)
                    } else {
                        generator.not_in_array(&col, &value)
                    }
                }
                Value::Subquery(cmd) => format!(
                    "{} {} ({})",
                    col,
                    self.op.sql_symbol(),
                    read_only_subquery_sql(cmd)
                ),
                Value::Param(_) | Value::NamedParam(_) => {
                    let value = value_placeholder(&self.value, params);
                    if self.op == Operator::In {
                        generator.in_array(&col, &value)
                    } else {
                        generator.not_in_array(&col, &value)
                    }
                }
                _ => invalid_in_condition_sql(),
            },
            Operator::Contains => {
                generator.json_contains(&col, &value_placeholder(&self.value, params))
            }
            Operator::KeyExists => {
                generator.json_key_exists(&col, &value_placeholder(&self.value, params))
            }
            Operator::JsonExists => {
                let path = value_placeholder(&self.value, params);
                generator.json_exists(&col, &path)
            }
            Operator::JsonQuery => {
                let path = value_placeholder(&self.value, params);
                format!("{} IS NOT NULL", generator.json_query(&col, &path))
            }
            Operator::JsonValue => {
                let path = value_placeholder(&self.value, params);
                format!("{} IS NOT NULL", generator.json_value(&col, &path))
            }
            Operator::Between => {
                if let Value::Array(vals) = &self.value
                    && vals.len() == 2
                {
                    let low = value_placeholder(&vals[0], params);
                    let high = value_placeholder(&vals[1], params);
                    return format!("{} BETWEEN {} AND {}", col, low, high);
                }
                invalid_between_condition_sql()
            }
            Operator::NotBetween => {
                if let Value::Array(vals) = &self.value
                    && vals.len() == 2
                {
                    let low = value_placeholder(&vals[0], params);
                    let high = value_placeholder(&vals[1], params);
                    return format!("{} NOT BETWEEN {} AND {}", col, low, high);
                }
                invalid_between_condition_sql()
            }
            Operator::Exists => {
                if let Value::Subquery(cmd) = &self.value {
                    let subquery_sql = read_only_subquery_sql(cmd);
                    format!("EXISTS ({})", subquery_sql)
                } else {
                    invalid_exists_condition_sql()
                }
            }
            Operator::NotExists => {
                if let Value::Subquery(cmd) = &self.value {
                    let subquery_sql = read_only_subquery_sql(cmd);
                    format!("NOT EXISTS ({})", subquery_sql)
                } else {
                    invalid_exists_condition_sql()
                }
            }
            // Simple operators (Ne, Gt, Gte, Lt, Lte, Like, NotLike, ILike, NotILike) use sql_symbol()
            _ => format!(
                "{} {} {}",
                col,
                self.op.sql_symbol(),
                value_placeholder(&self.value, params)
            ),
        }
    }
}
