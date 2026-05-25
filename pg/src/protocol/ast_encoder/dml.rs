//! DML (Data Manipulation Language) encoders.
//!
//! SELECT, INSERT, UPDATE, DELETE, EXPORT, and CTE statements.

use bytes::BytesMut;
use qail_core::ast::{
    Action, CTEDef, CageKind, ColumnGeneration, Condition, ConflictAction, Constraint, Expr,
    GroupByMode, JoinKind, LogicalOp, Merge, MergeAction, MergeMatchKind, MergeSource, Operator,
    Qail, SetOp, SortOrder, Value,
};
use qail_core::transpiler::escape_identifier;

use super::helpers::write_usize;
use super::values::{
    encode_columns, encode_columns_with_params, encode_conditions, encode_expr, encode_join_value,
    encode_operator, encode_value,
};

const MAX_IDENT_LEN: usize = 63;

fn invalid_identifier(field: &str, value: &str, reason: &str) -> crate::protocol::EncodeError {
    let preview: String = value.chars().take(64).collect();
    crate::protocol::EncodeError::InvalidAst(format!(
        "unsafe identifier in {field}: `{preview}` ({reason})"
    ))
}

fn validate_ident_atom(field: &str, value: &str) -> Result<(), crate::protocol::EncodeError> {
    if value.is_empty() {
        return Err(invalid_identifier(field, value, "empty identifier"));
    }
    if value.as_bytes().contains(&0) {
        return Err(crate::protocol::EncodeError::NullByte);
    }
    if value.len() > MAX_IDENT_LEN {
        return Err(invalid_identifier(field, value, "identifier is too long"));
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(invalid_identifier(
            field,
            value,
            "expected only ASCII letters, digits, and underscores",
        ));
    }
    Ok(())
}

fn validate_qualified_ident(
    field: &str,
    value: &str,
    allow_star: bool,
) -> Result<(), crate::protocol::EncodeError> {
    if allow_star && value == "*" {
        return Ok(());
    }

    let mut parts = value.split('.').peekable();
    if parts.peek().is_none() {
        return Err(invalid_identifier(field, value, "empty identifier"));
    }

    while let Some(part) = parts.next() {
        if allow_star && part == "*" && parts.peek().is_none() {
            continue;
        }
        validate_ident_atom(field, part)?;
    }

    Ok(())
}

fn validate_table_ref(field: &str, value: &str) -> Result<(), crate::protocol::EncodeError> {
    let parts: Vec<&str> = value.split_whitespace().collect();
    match parts.as_slice() {
        [table] => validate_qualified_ident(field, table, false),
        [table, alias] => {
            validate_qualified_ident(field, table, false)?;
            validate_ident_atom(&format!("{field}.alias"), alias)
        }
        [table, as_kw, alias] if as_kw.eq_ignore_ascii_case("AS") => {
            validate_qualified_ident(field, table, false)?;
            validate_ident_atom(&format!("{field}.alias"), alias)
        }
        _ => Err(invalid_identifier(
            field,
            value,
            "expected `table`, `schema.table`, or `table alias`",
        )),
    }
}

fn push_identifier_ref(buf: &mut BytesMut, ident: &str, allow_star: bool) {
    if allow_star && ident == "*" {
        buf.extend_from_slice(b"*");
    } else {
        buf.extend_from_slice(escape_identifier(ident).as_bytes());
    }
}

fn push_table_ref(buf: &mut BytesMut, value: &str) {
    let parts: Vec<&str> = value.split_whitespace().collect();
    match parts.as_slice() {
        [table] => push_identifier_ref(buf, table, false),
        [table, alias] => {
            push_identifier_ref(buf, table, false);
            buf.extend_from_slice(b" ");
            push_identifier_ref(buf, alias, false);
        }
        [table, as_kw, alias] if as_kw.eq_ignore_ascii_case("AS") => {
            push_identifier_ref(buf, table, false);
            buf.extend_from_slice(b" AS ");
            push_identifier_ref(buf, alias, false);
        }
        _ => push_identifier_ref(buf, value, false),
    }
}

fn validate_sql_type_fragment(
    field: &str,
    value: &str,
) -> Result<(), crate::protocol::EncodeError> {
    if value.is_empty() || value.as_bytes().contains(&0) {
        return Err(invalid_identifier(field, value, "invalid SQL type"));
    }
    if value.contains(';')
        || value.contains('\'')
        || value.contains('"')
        || value.contains("--")
        || value.contains("/*")
        || value.contains("*/")
    {
        return Err(invalid_identifier(
            field,
            value,
            "SQL type contains statement or comment delimiters",
        ));
    }
    if !value.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'_' | b'.' | b' ' | b'(' | b')' | b',' | b'[' | b']' | b'%' | b'+' | b'-'
            )
    }) {
        return Err(invalid_identifier(
            field,
            value,
            "SQL type contains unsafe characters",
        ));
    }
    Ok(())
}

fn contains_unquoted_statement_delimiter(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;

    while i < bytes.len() {
        let b = bytes[i];
        if b == 0 {
            return true;
        }

        if in_single {
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single = false;
            }
            i += 1;
            continue;
        }

        if in_double {
            if b == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double = false;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' => in_single = true,
            b'"' => in_double = true,
            b';' => return true,
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => return true,
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => return true,
            _ => {}
        }
        i += 1;
    }

    false
}

fn validate_sql_expr_fragment(
    field: &str,
    value: &str,
) -> Result<(), crate::protocol::EncodeError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || contains_unquoted_statement_delimiter(trimmed) {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "invalid SQL expression fragment in {field}: {trimmed:?}"
        )));
    }
    Ok(())
}

fn validate_comment_fragment(field: &str, value: &str) -> Result<(), crate::protocol::EncodeError> {
    if value.as_bytes().contains(&0)
        || value.contains('"')
        || value.contains(';')
        || value.contains("--")
        || value.contains("/*")
        || value.contains("*/")
    {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "invalid column comment fragment in {field}: {value:?}"
        )));
    }
    Ok(())
}

fn validate_def_constraint(
    field: &str,
    constraint: &Constraint,
) -> Result<(), crate::protocol::EncodeError> {
    match constraint {
        Constraint::PrimaryKey | Constraint::Unique | Constraint::Nullable => Ok(()),
        Constraint::Default(value) => {
            validate_sql_expr_fragment(&format!("{field}.default"), value)
        }
        Constraint::Check(values) => {
            validate_sql_expr_fragment(&format!("{field}.check"), &values.join(", "))
        }
        Constraint::References(target) => {
            validate_sql_expr_fragment(&format!("{field}.references"), target)
        }
        Constraint::Generated(ColumnGeneration::Stored(expr))
            if expr == "identity" || expr == "identity_by_default" =>
        {
            Ok(())
        }
        Constraint::Generated(ColumnGeneration::Stored(expr))
        | Constraint::Generated(ColumnGeneration::Virtual(expr)) => {
            validate_sql_expr_fragment(&format!("{field}.generated"), expr)
        }
        Constraint::Comment(value) => validate_comment_fragment(&format!("{field}.comment"), value),
    }
}

fn is_positional_placeholder(expr: &Expr) -> bool {
    let Expr::Named(name) = expr else {
        return false;
    };
    name.strip_prefix('$')
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

pub(crate) fn validate_expr_ref(
    field: &str,
    expr: &Expr,
) -> Result<(), crate::protocol::EncodeError> {
    match expr {
        Expr::Star => Ok(()),
        Expr::Named(name) => validate_qualified_ident(field, name, true),
        Expr::Aliased { name, alias } => {
            validate_qualified_ident(field, name, true)?;
            validate_ident_atom(&format!("{field}.alias"), alias)
        }
        Expr::Aggregate {
            col, alias, filter, ..
        } => {
            validate_qualified_ident(field, col, true)?;
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            if let Some(filter) = filter {
                validate_conditions(&format!("{field}.filter"), filter)?;
            }
            Ok(())
        }
        Expr::Cast {
            expr,
            target_type,
            alias,
        } => {
            validate_expr_ref(field, expr)?;
            validate_sql_type_fragment(&format!("{field}.cast_type"), target_type)?;
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::Def {
            name,
            data_type,
            constraints,
        } => {
            validate_ident_atom(field, name)?;
            validate_sql_type_fragment(&format!("{field}.type"), data_type)?;
            for constraint in constraints {
                validate_def_constraint(field, constraint)?;
            }
            Ok(())
        }
        Expr::Mod { col, .. } => validate_expr_ref(field, col),
        Expr::Window {
            name,
            func,
            params,
            partition,
            order,
            ..
        } => {
            if !name.is_empty() {
                validate_ident_atom(&format!("{field}.alias"), name)?;
            }
            validate_qualified_ident(&format!("{field}.function"), func, false)?;
            for param in params {
                validate_expr_ref(&format!("{field}.param"), param)?;
            }
            for part in partition {
                validate_qualified_ident(&format!("{field}.partition"), part, false)?;
            }
            for cage in order {
                validate_cage_conditions(field, cage.kind == CageKind::Payload, &cage.conditions)?;
            }
            Ok(())
        }
        Expr::Case {
            when_clauses,
            else_value,
            alias,
        } => {
            for (condition, then_expr) in when_clauses {
                validate_condition(&format!("{field}.when"), condition)?;
                validate_expr_ref(&format!("{field}.then"), then_expr)?;
            }
            if let Some(else_value) = else_value {
                validate_expr_ref(&format!("{field}.else"), else_value)?;
            }
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::JsonAccess {
            column,
            path_segments,
            alias,
        } => {
            validate_qualified_ident(field, column, false)?;
            for (segment, _) in path_segments {
                if segment.as_bytes().contains(&0) {
                    return Err(crate::protocol::EncodeError::NullByte);
                }
            }
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::FunctionCall { name, args, alias } => {
            validate_qualified_ident(&format!("{field}.function"), name, false)?;
            for arg in args {
                validate_expr_ref(&format!("{field}.arg"), arg)?;
            }
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::SpecialFunction { name, args, alias } => {
            validate_qualified_ident(&format!("{field}.special_function"), name, false)?;
            for (keyword, arg) in args {
                if let Some(keyword) = keyword {
                    validate_ident_atom(&format!("{field}.keyword"), keyword)?;
                }
                validate_expr_ref(&format!("{field}.arg"), arg)?;
            }
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::Binary {
            left, right, alias, ..
        } => {
            validate_expr_ref(&format!("{field}.left"), left)?;
            validate_expr_ref(&format!("{field}.right"), right)?;
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::Literal(value) => validate_value_ref(field, value),
        Expr::ArrayConstructor { elements, alias } | Expr::RowConstructor { elements, alias } => {
            for element in elements {
                validate_expr_ref(&format!("{field}.element"), element)?;
            }
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::Subscript { expr, index, alias } => {
            validate_expr_ref(&format!("{field}.subscript"), expr)?;
            validate_expr_ref(&format!("{field}.index"), index)?;
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::Collate {
            expr,
            collation,
            alias,
        } => {
            validate_expr_ref(&format!("{field}.collate"), expr)?;
            validate_qualified_ident(&format!("{field}.collation"), collation, false)?;
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::FieldAccess {
            expr,
            field: field_name,
            alias,
        } => {
            validate_expr_ref(&format!("{field}.field_access"), expr)?;
            validate_ident_atom(&format!("{field}.field"), field_name)?;
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::Subquery { query, alias } | Expr::Exists { query, alias, .. } => {
            validate_dml_command(query, &query.columns)?;
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
    }
}

fn validate_value_ref(field: &str, value: &Value) -> Result<(), crate::protocol::EncodeError> {
    match value {
        Value::Column(column) => validate_qualified_ident(field, column, false),
        Value::Expr(expr) => validate_expr_ref(field, expr),
        Value::Subquery(query) => validate_dml_command(query, &query.columns),
        Value::Array(values) => {
            for value in values {
                validate_value_ref(field, value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_condition(
    field: &str,
    condition: &Condition,
) -> Result<(), crate::protocol::EncodeError> {
    if condition.op == Operator::TextSearch {
        validate_text_search_columns(&format!("{field}.left"), &condition.left)?;
    } else {
        validate_expr_ref(&format!("{field}.left"), &condition.left)?;
    }
    validate_value_ref(&format!("{field}.value"), &condition.value)
}

fn validate_text_search_columns(
    field: &str,
    expr: &Expr,
) -> Result<(), crate::protocol::EncodeError> {
    let Expr::Named(columns) = expr else {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "text search left side must be a comma-separated identifier list in {field}"
        )));
    };

    let mut saw_column = false;
    for raw_column in columns.split(',') {
        let column = raw_column.trim();
        if column.is_empty() {
            return Err(invalid_identifier(
                field,
                columns,
                "text search column list contains an empty entry",
            ));
        }
        validate_qualified_ident(field, column, false)?;
        saw_column = true;
    }

    if !saw_column {
        return Err(invalid_identifier(
            field,
            columns,
            "text search column list cannot be empty",
        ));
    }

    Ok(())
}

fn validate_join_condition(
    field: &str,
    condition: &Condition,
) -> Result<(), crate::protocol::EncodeError> {
    validate_expr_ref(&format!("{field}.left"), &condition.left)?;
    match &condition.value {
        Value::String(value) if value.contains('.') => {
            validate_qualified_ident(&format!("{field}.value"), value, false)
        }
        value => validate_value_ref(&format!("{field}.value"), value),
    }
}

fn validate_conditions(
    field: &str,
    conditions: &[Condition],
) -> Result<(), crate::protocol::EncodeError> {
    for condition in conditions {
        validate_condition(field, condition)?;
    }
    Ok(())
}

fn validate_cage_conditions(
    field: &str,
    skip_placeholders: bool,
    conditions: &[Condition],
) -> Result<(), crate::protocol::EncodeError> {
    for condition in conditions {
        if !(skip_placeholders && is_positional_placeholder(&condition.left)) {
            if condition.op == Operator::TextSearch {
                validate_text_search_columns(&format!("{field}.left"), &condition.left)?;
            } else {
                validate_expr_ref(&format!("{field}.left"), &condition.left)?;
            }
        }
        validate_value_ref(&format!("{field}.value"), &condition.value)?;
    }
    Ok(())
}

fn validate_dml_command(
    cmd: &Qail,
    projection_columns: &[Expr],
) -> Result<(), crate::protocol::EncodeError> {
    if !cmd.table.is_empty() {
        validate_table_ref("table", &cmd.table)?;
    }

    for column in projection_columns {
        validate_expr_ref("columns", column)?;
    }

    for expr in &cmd.distinct_on {
        validate_expr_ref("distinct_on", expr)?;
    }

    for join in &cmd.joins {
        validate_table_ref("join.table", &join.table)?;
        if let Some(conditions) = &join.on {
            for condition in conditions {
                validate_join_condition("join.on", condition)?;
            }
        }
    }

    for cage in &cmd.cages {
        validate_cage_conditions(
            "cage.condition",
            cage.kind == CageKind::Payload,
            &cage.conditions,
        )?;
    }

    for condition in &cmd.having {
        validate_condition("having", condition)?;
    }

    if let GroupByMode::GroupingSets(sets) = &cmd.group_by_mode {
        for set in sets {
            for column in set {
                validate_qualified_ident("group_by.grouping_set", column, true)?;
            }
        }
    }

    for cte in &cmd.ctes {
        validate_ident_atom("cte.name", &cte.name)?;
        for column in &cte.columns {
            validate_ident_atom("cte.column", column)?;
        }
        validate_dml_command(&cte.base_query, &cte.base_query.columns)?;
        if let Some(recursive_query) = &cte.recursive_query {
            validate_dml_command(recursive_query, &recursive_query.columns)?;
        }
    }

    for (_, set_query) in &cmd.set_ops {
        validate_dml_command(set_query, &set_query.columns)?;
    }

    if let Some(source_query) = &cmd.source_query {
        validate_dml_command(source_query, &source_query.columns)?;
    }

    for table in &cmd.from_tables {
        validate_table_ref("from_tables", table)?;
    }
    for table in &cmd.using_tables {
        validate_table_ref("using_tables", table)?;
    }

    if let Some(returning) = &cmd.returning {
        for expr in returning {
            validate_expr_ref("returning", expr)?;
        }
    }

    if let Some(on_conflict) = &cmd.on_conflict {
        for column in &on_conflict.columns {
            validate_qualified_ident("on_conflict.column", column, false)?;
        }
        if let ConflictAction::DoUpdate { assignments } = &on_conflict.action {
            for (column, expr) in assignments {
                validate_qualified_ident("on_conflict.assignment.column", column, false)?;
                validate_expr_ref("on_conflict.assignment.expr", expr)?;
            }
        }
    }

    if let Some(merge) = &cmd.merge {
        if let Some(alias) = &merge.target_alias {
            validate_ident_atom("merge.target_alias", alias)?;
        }
        match &merge.source {
            MergeSource::Table { name, alias } => {
                validate_table_ref("merge.source.table", name)?;
                if let Some(alias) = alias {
                    validate_ident_atom("merge.source.alias", alias)?;
                }
            }
            MergeSource::Query { query, alias } => {
                validate_dml_command(query, &query.columns)?;
                if let Some(alias) = alias {
                    validate_ident_atom("merge.source.alias", alias)?;
                }
            }
        }
        validate_conditions("merge.on", &merge.on)?;
        for clause in &merge.clauses {
            validate_conditions("merge.clause.condition", &clause.condition)?;
            match &clause.action {
                MergeAction::Update { assignments } => {
                    for (column, expr) in assignments {
                        validate_qualified_ident("merge.update.column", column, false)?;
                        validate_expr_ref("merge.update.expr", expr)?;
                    }
                }
                MergeAction::Insert { columns, values } => {
                    for column in columns {
                        validate_qualified_ident("merge.insert.column", column, false)?;
                    }
                    for value in values {
                        validate_expr_ref("merge.insert.value", value)?;
                    }
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }

    Ok(())
}

/// Encode a SELECT statement directly to bytes.
///
/// # Arguments
///
/// * `cmd` — Qail AST command with `Action::Get`.
/// * `buf` — Output buffer to append the SQL bytes to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_select(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    validate_read_only_select_query(cmd)?;
    encode_select_with_columns(cmd, &cmd.columns, buf, params)
}

fn validate_read_only_select_query(query: &Qail) -> Result<(), crate::protocol::EncodeError> {
    validate_read_only_select_query_with_message(
        query,
        "read-only SELECT query slot requires get/with action",
    )
}

fn validate_read_only_select_query_with_message(
    query: &Qail,
    message: &str,
) -> Result<(), crate::protocol::EncodeError> {
    if !matches!(query.action, Action::Get | Action::With) {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "{message}, got {}",
            query.action
        )));
    }

    for cte in &query.ctes {
        validate_read_only_select_query_with_message(&cte.base_query, message)?;
        if let Some(ref recursive_query) = cte.recursive_query {
            validate_read_only_select_query_with_message(recursive_query, message)?;
        }
    }
    for (_, set_query) in &query.set_ops {
        validate_read_only_select_query_with_message(set_query, message)?;
    }
    if let Some(ref source_query) = query.source_query {
        validate_read_only_select_query_with_message(source_query, message)?;
    }

    Ok(())
}

/// Encode a COUNT statement using the original query shape with a COUNT(*)
/// projection, without cloning the full AST.
pub fn encode_count(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    let count_columns = [Expr::Aggregate {
        col: "*".to_string(),
        func: qail_core::ast::AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: None,
    }];
    encode_select_with_columns(cmd, &count_columns, buf, params)
}

fn encode_select_with_columns(
    cmd: &Qail,
    columns: &[Expr],
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    validate_dml_command(cmd, columns)?;

    if try_encode_simple_select_fast(cmd, columns, buf, params)? {
        return Ok(());
    }

    let select_start = buf.len();

    // CTE prefix
    encode_cte_prefix(cmd, buf, params)?;

    buf.extend_from_slice(b"SELECT ");

    // DISTINCT ON (col1, col2, ...)
    if !cmd.distinct_on.is_empty() {
        buf.extend_from_slice(b"DISTINCT ON (");
        for (i, expr) in cmd.distinct_on.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_expr(expr, buf)?;
        }
        buf.extend_from_slice(b") ");
    } else if cmd.distinct {
        // Regular DISTINCT (mutually exclusive with DISTINCT ON)
        buf.extend_from_slice(b"DISTINCT ");
    }

    encode_columns_with_params(columns, buf, Some(params))?;

    // FROM
    buf.extend_from_slice(b" FROM ");
    push_table_ref(buf, &cmd.table);

    // JOINs
    for join in &cmd.joins {
        match join.kind {
            JoinKind::Inner => buf.extend_from_slice(b" INNER JOIN "),
            JoinKind::Left => buf.extend_from_slice(b" LEFT JOIN "),
            JoinKind::Right => buf.extend_from_slice(b" RIGHT JOIN "),
            JoinKind::Full => buf.extend_from_slice(b" FULL OUTER JOIN "),
            JoinKind::Cross => buf.extend_from_slice(b" CROSS JOIN "),
            JoinKind::Lateral => buf.extend_from_slice(b" LEFT JOIN LATERAL "),
        }
        push_table_ref(buf, &join.table);

        if join.on_true {
            buf.extend_from_slice(b" ON TRUE");
        } else if let Some(conditions) = &join.on
            && !conditions.is_empty()
        {
            buf.extend_from_slice(b" ON ");
            for (i, cond) in conditions.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b" AND ");
                }
                encode_expr(&cond.left, buf)?;
                buf.extend_from_slice(b" ");
                encode_operator(&cond.op, buf);
                buf.extend_from_slice(b" ");
                encode_join_value(&cond.value, buf, params)?;
            }
        }
    }

    // WHERE (supports AND + OR filter cages)
    encode_where(cmd, buf, params)?;

    // GROUP BY - prefer explicit Partition cage, fall back to auto-extraction from columns
    let partition_cage = cmd.cages.iter().find(|c| c.kind == CageKind::Partition);

    if let Some(cage) = partition_cage {
        // Explicit GROUP BY from .group_by() or .group_by_expr()
        if !cage.conditions.is_empty() {
            buf.extend_from_slice(b" GROUP BY ");
            for (i, cond) in cage.conditions.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_expr(&cond.left, buf)?;
            }
        }
    } else {
        // Auto-generate GROUP BY from columns when aggregates are present
        let group_cols: Vec<&str> = columns
            .iter()
            .filter_map(|e| match e {
                Expr::Named(name) => Some(name.as_str()),
                Expr::Aliased { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect();

        let has_aggregates = columns.iter().any(|e| matches!(e, Expr::Aggregate { .. }));
        if has_aggregates && !group_cols.is_empty() {
            buf.extend_from_slice(b" GROUP BY ");
            match &cmd.group_by_mode {
                GroupByMode::Simple => {
                    for (i, col) in group_cols.iter().enumerate() {
                        if i > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        push_identifier_ref(buf, col, true);
                    }
                }
                GroupByMode::Rollup => {
                    buf.extend_from_slice(b"ROLLUP(");
                    for (i, col) in group_cols.iter().enumerate() {
                        if i > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        push_identifier_ref(buf, col, true);
                    }
                    buf.extend_from_slice(b")");
                }
                GroupByMode::Cube => {
                    buf.extend_from_slice(b"CUBE(");
                    for (i, col) in group_cols.iter().enumerate() {
                        if i > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        push_identifier_ref(buf, col, true);
                    }
                    buf.extend_from_slice(b")");
                }
                GroupByMode::GroupingSets(sets) => {
                    buf.extend_from_slice(b"GROUPING SETS (");
                    for (i, set) in sets.iter().enumerate() {
                        if i > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        buf.extend_from_slice(b"(");
                        for (j, col) in set.iter().enumerate() {
                            if j > 0 {
                                buf.extend_from_slice(b", ");
                            }
                            push_identifier_ref(buf, col, true);
                        }
                        buf.extend_from_slice(b")");
                    }
                    buf.extend_from_slice(b")");
                }
            }
        }
    }

    // ORDER BY - collect ALL sort cages and output them together
    let sort_cages: Vec<_> = cmd
        .cages
        .iter()
        .filter_map(|cage| {
            if let CageKind::Sort(order) = &cage.kind {
                Some((cage, *order))
            } else {
                None
            }
        })
        .collect();

    if !sort_cages.is_empty() {
        buf.extend_from_slice(b" ORDER BY ");
        let mut first = true;
        for (cage, order) in &sort_cages {
            for cond in &cage.conditions {
                if !first {
                    buf.extend_from_slice(b", ");
                }
                first = false;
                encode_expr(&cond.left, buf)?;
                match order {
                    SortOrder::Desc | SortOrder::DescNullsFirst | SortOrder::DescNullsLast => {
                        buf.extend_from_slice(b" DESC");
                    }
                    SortOrder::Asc | SortOrder::AscNullsFirst | SortOrder::AscNullsLast => {}
                }
            }
        }
    }

    // LIMIT
    for cage in &cmd.cages {
        if let CageKind::Limit(n) = cage.kind {
            buf.extend_from_slice(b" LIMIT ");
            write_usize(buf, n);
            break;
        }
    }

    // OFFSET
    for cage in &cmd.cages {
        if let CageKind::Offset(n) = cage.kind {
            buf.extend_from_slice(b" OFFSET ");
            write_usize(buf, n);
            break;
        }
    }

    append_fetch_clause(cmd, buf);

    if !cmd.set_ops.is_empty() && set_operand_has_branch_clauses(cmd) {
        wrap_sql_range_in_parens(buf, select_start);
    }

    // SET OPERATIONS (UNION, INTERSECT, EXCEPT)
    for (set_op, other_cmd) in &cmd.set_ops {
        match set_op {
            SetOp::Union => buf.extend_from_slice(b" UNION "),
            SetOp::UnionAll => buf.extend_from_slice(b" UNION ALL "),
            SetOp::Intersect => buf.extend_from_slice(b" INTERSECT "),
            SetOp::Except => buf.extend_from_slice(b" EXCEPT "),
        }
        encode_set_operand(other_cmd, buf, params)?;
    }

    Ok(())
}

fn encode_set_operand(
    query: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    let wrap = set_operand_needs_wrapper(query);
    if wrap {
        buf.extend_from_slice(b"(");
    }

    encode_select(query, buf, params)?;

    if wrap {
        buf.extend_from_slice(b")");
    }

    Ok(())
}

fn set_operand_needs_wrapper(query: &Qail) -> bool {
    !query.set_ops.is_empty() || set_operand_has_branch_clauses(query)
}

fn set_operand_has_branch_clauses(query: &Qail) -> bool {
    query.fetch.is_some()
        || query.cages.iter().any(|cage| {
            matches!(
                cage.kind,
                CageKind::Sort(_) | CageKind::Limit(_) | CageKind::Offset(_)
            )
        })
}

fn wrap_sql_range_in_parens(buf: &mut BytesMut, start: usize) {
    let suffix = buf.split_off(start);
    buf.extend_from_slice(b"(");
    buf.extend_from_slice(&suffix);
    buf.extend_from_slice(b")");
}

fn append_fetch_clause(cmd: &Qail, buf: &mut BytesMut) {
    if let Some((count, with_ties)) = cmd.fetch {
        buf.extend_from_slice(b" FETCH FIRST ");
        buf.extend_from_slice(count.to_string().as_bytes());
        if with_ties {
            buf.extend_from_slice(b" ROWS WITH TIES");
        } else {
            buf.extend_from_slice(b" ROWS ONLY");
        }
    }
}

/// Fast path for the dominant read shape:
/// `SELECT <columns> FROM <table> [LIMIT n] [OFFSET n]`
///
/// This bypasses the generic cage scans and grouping/order machinery when the
/// AST shape is simple enough, reducing branch and allocation overhead.
#[inline]
fn try_encode_simple_select_fast(
    cmd: &Qail,
    columns: &[Expr],
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<bool, crate::protocol::EncodeError> {
    if !cmd.ctes.is_empty()
        || cmd.distinct
        || !cmd.distinct_on.is_empty()
        || !cmd.joins.is_empty()
        || !cmd.set_ops.is_empty()
        || !cmd.having.is_empty()
        || cmd.fetch.is_some()
        || !matches!(cmd.group_by_mode, GroupByMode::Simple)
    {
        return Ok(false);
    }

    if columns
        .iter()
        .any(|expr| matches!(expr, Expr::Aggregate { .. }))
    {
        return Ok(false);
    }

    let mut limit: Option<usize> = None;
    let mut offset: Option<usize> = None;

    for cage in &cmd.cages {
        if !cage.conditions.is_empty() {
            return Ok(false);
        }

        match cage.kind {
            CageKind::Limit(n) => {
                if limit.is_none() {
                    limit = Some(n);
                }
            }
            CageKind::Offset(n) => {
                if offset.is_none() {
                    offset = Some(n);
                }
            }
            _ => return Ok(false),
        }
    }

    buf.extend_from_slice(b"SELECT ");
    encode_columns_with_params(columns, buf, Some(params))?;
    buf.extend_from_slice(b" FROM ");
    push_table_ref(buf, &cmd.table);

    if let Some(n) = limit {
        buf.extend_from_slice(b" LIMIT ");
        write_usize(buf, n);
    }

    if let Some(n) = offset {
        buf.extend_from_slice(b" OFFSET ");
        write_usize(buf, n);
    }

    Ok(true)
}

/// Encode the CTE prefix (`WITH [RECURSIVE] cte1 AS (...), cte2 AS (...)`).
///
/// # Arguments
///
/// * `cmd` — Qail AST command containing CTE definitions.
/// * `buf` — Output buffer to append the WITH clause to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_cte_prefix(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), super::super::EncodeError> {
    if cmd.ctes.is_empty() {
        return Ok(());
    }

    buf.extend_from_slice(b"WITH ");

    let has_recursive = cmd.ctes.iter().any(|c| c.recursive);
    if has_recursive {
        buf.extend_from_slice(b"RECURSIVE ");
    }

    for (i, cte) in cmd.ctes.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        encode_single_cte(cte, buf, params)?;
    }

    buf.extend_from_slice(b" ");
    Ok(())
}

/// Encode a single CTE definition.
fn encode_single_cte(
    cte: &CTEDef,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), super::super::EncodeError> {
    push_identifier_ref(buf, &cte.name, false);

    // Optional column list
    if !cte.columns.is_empty() {
        buf.extend_from_slice(b"(");
        for (i, col) in cte.columns.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            push_identifier_ref(buf, col, false);
        }
        buf.extend_from_slice(b")");
    }

    buf.extend_from_slice(b" AS (");

    encode_recursive_cte_arm(&cte.base_query, buf, params)?;

    // Recursive part (UNION ALL)
    if cte.recursive
        && let Some(ref recursive_query) = cte.recursive_query
    {
        buf.extend_from_slice(b" UNION ALL ");
        encode_recursive_cte_arm(recursive_query, buf, params)?;
    }

    buf.extend_from_slice(b")");
    Ok(())
}

fn encode_recursive_cte_arm(
    query: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), super::super::EncodeError> {
    let wrap_set_ops = set_operand_needs_wrapper(query);
    if wrap_set_ops {
        buf.extend_from_slice(b"(");
    }

    encode_select(query, buf, params)?;

    if wrap_set_ops {
        buf.extend_from_slice(b")");
    }

    Ok(())
}

/// Encode an INSERT statement.
///
/// # Arguments
///
/// * `cmd` — Qail AST command with `Action::Add`.
/// * `buf` — Output buffer to append the SQL bytes to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_insert(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    validate_dml_command(cmd, &cmd.columns)?;

    buf.extend_from_slice(b"INSERT INTO ");
    push_table_ref(buf, &cmd.table);

    // Find payload cage
    let payload_cage = cmd.cages.iter().find(|c| c.kind == CageKind::Payload);

    // Column list - prefer cmd.columns, but extract from conditions if empty (set_value pattern)
    if !cmd.columns.is_empty() {
        buf.extend_from_slice(b" (");
        encode_columns(&cmd.columns, buf)?;
        buf.extend_from_slice(b")");
    } else if let Some(cage) = payload_cage {
        // Extract column names from condition.left (set_value pattern)
        buf.extend_from_slice(b" (");
        for (i, cond) in cage.conditions.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_expr(&cond.left, buf)?;
        }
        buf.extend_from_slice(b")");
    }

    // INSERT ... SELECT source query takes the place of VALUES.
    if let Some(source_query) = &cmd.source_query {
        buf.extend_from_slice(b" ");
        encode_select(source_query, buf, params)?;
    } else if let Some(cage) = payload_cage {
        buf.extend_from_slice(b" VALUES (");
        for (i, cond) in cage.conditions.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_value(&cond.value, buf, params)?;
        }
        buf.extend_from_slice(b")");
    }

    // ON CONFLICT clause (UPSERT support)
    if let Some(ref on_conflict) = cmd.on_conflict {
        use qail_core::ast::ConflictAction;

        buf.extend_from_slice(b" ON CONFLICT ");

        // Conflict target columns
        if !on_conflict.columns.is_empty() {
            buf.extend_from_slice(b"(");
            for (i, col) in on_conflict.columns.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                push_identifier_ref(buf, col, false);
            }
            buf.extend_from_slice(b") ");
        }

        // Conflict action
        match &on_conflict.action {
            ConflictAction::DoNothing => {
                buf.extend_from_slice(b"DO NOTHING");
            }
            ConflictAction::DoUpdate { assignments } => {
                buf.extend_from_slice(b"DO UPDATE SET ");
                for (i, (col, expr)) in assignments.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    push_identifier_ref(buf, col, false);
                    buf.extend_from_slice(b" = ");
                    encode_expr(expr, buf)?;
                }
                encode_where(cmd, buf, params)?;
            }
        }
    }

    // RETURNING clause
    if let Some(ref ret_cols) = cmd.returning {
        buf.extend_from_slice(b" RETURNING ");
        encode_columns(ret_cols, buf)?;
    }

    Ok(())
}

/// Encode an UPDATE statement.
///
/// # Arguments
///
/// * `cmd` — Qail AST command with `Action::Set`.
/// * `buf` — Output buffer to append the SQL bytes to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_update(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    validate_dml_command(cmd, &cmd.columns)?;

    buf.extend_from_slice(b"UPDATE ");
    push_table_ref(buf, &cmd.table);
    buf.extend_from_slice(b" SET ");

    // SET clause - pair columns with payload values
    if let Some(cage) = cmd.cages.iter().find(|c| c.kind == CageKind::Payload) {
        // Use cmd.columns if available (from .columns([...]).values([...]) pattern)
        // Otherwise use cage.conditions.left (from .set("col", value) pattern)
        if !cmd.columns.is_empty() {
            // Zip columns with values
            for (i, (col, cond)) in cmd.columns.iter().zip(cage.conditions.iter()).enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                // Column name from cmd.columns
                encode_expr(col, buf)?;
                buf.extend_from_slice(b" = ");
                // Value from payload condition
                encode_value(&cond.value, buf, params)?;
            }
        } else {
            // Fallback to old behavior (direct set)
            for (i, cond) in cage.conditions.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_expr(&cond.left, buf)?;
                buf.extend_from_slice(b" = ");
                encode_value(&cond.value, buf, params)?;
            }
        }
    }

    if !cmd.from_tables.is_empty() {
        buf.extend_from_slice(b" FROM ");
        for (i, table) in cmd.from_tables.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            push_table_ref(buf, table);
        }
    }

    // WHERE (supports AND + OR filter cages)
    encode_where(cmd, buf, params)?;

    // RETURNING clause
    if let Some(ref ret_cols) = cmd.returning {
        buf.extend_from_slice(b" RETURNING ");
        encode_columns(ret_cols, buf)?;
    }

    Ok(())
}

/// Encode a DELETE statement.
///
/// # Arguments
///
/// * `cmd` — Qail AST command with `Action::Del`.
/// * `buf` — Output buffer to append the SQL bytes to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_delete(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    validate_dml_command(cmd, &cmd.columns)?;

    buf.extend_from_slice(b"DELETE FROM ");
    push_table_ref(buf, &cmd.table);

    if !cmd.using_tables.is_empty() {
        buf.extend_from_slice(b" USING ");
        for (i, table) in cmd.using_tables.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            push_table_ref(buf, table);
        }
    }

    // WHERE (supports AND + OR filter cages)
    encode_where(cmd, buf, params)?;

    // RETURNING clause
    if let Some(ref ret_cols) = cmd.returning {
        buf.extend_from_slice(b" RETURNING ");
        encode_columns(ret_cols, buf)?;
    }

    Ok(())
}

/// Encode a PostgreSQL MERGE statement.
pub fn encode_merge(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    validate_dml_command(cmd, &cmd.columns)?;

    let merge = cmd
        .merge
        .as_ref()
        .ok_or(crate::protocol::EncodeError::InvalidAst(
            "MERGE requires merge specification".to_string(),
        ))?;
    validate_merge_shape(merge)?;

    encode_cte_prefix(cmd, buf, params)?;
    buf.extend_from_slice(b"MERGE INTO ");
    push_table_ref(buf, &cmd.table);
    if let Some(alias) = &merge.target_alias {
        buf.extend_from_slice(b" AS ");
        push_identifier_ref(buf, alias, false);
    }

    buf.extend_from_slice(b" USING ");
    encode_merge_source(&merge.source, buf, params)?;

    buf.extend_from_slice(b" ON ");
    encode_conditions(&merge.on, buf, params)?;

    for clause in &merge.clauses {
        buf.extend_from_slice(b" WHEN ");
        match clause.match_kind {
            MergeMatchKind::Matched => buf.extend_from_slice(b"MATCHED"),
            MergeMatchKind::NotMatchedByTarget => buf.extend_from_slice(b"NOT MATCHED BY TARGET"),
            MergeMatchKind::NotMatchedBySource => buf.extend_from_slice(b"NOT MATCHED BY SOURCE"),
        }
        if !clause.condition.is_empty() {
            buf.extend_from_slice(b" AND ");
            encode_conditions(&clause.condition, buf, params)?;
        }
        buf.extend_from_slice(b" THEN ");
        encode_merge_action(&clause.action, buf)?;
    }

    if let Some(ref ret_cols) = cmd.returning
        && !ret_cols.is_empty()
    {
        buf.extend_from_slice(b" RETURNING ");
        encode_columns(ret_cols, buf)?;
    }

    Ok(())
}

fn validate_merge_shape(merge: &Merge) -> Result<(), crate::protocol::EncodeError> {
    match &merge.source {
        MergeSource::Table { name, .. } if name.trim().is_empty() => {
            return Err(crate::protocol::EncodeError::InvalidAst(
                "MERGE requires a USING source table or query".to_string(),
            ));
        }
        MergeSource::Query { query, .. } => {
            validate_merge_source_query(query)?;
        }
        _ => {}
    }
    if merge.on.is_empty() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "MERGE requires at least one ON condition".to_string(),
        ));
    }
    if merge.clauses.is_empty() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "MERGE requires at least one WHEN clause".to_string(),
        ));
    }

    for clause in &merge.clauses {
        match (&clause.match_kind, &clause.action) {
            (MergeMatchKind::Matched, MergeAction::Insert { .. }) => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "WHEN MATCHED cannot INSERT".to_string(),
                ));
            }
            (MergeMatchKind::NotMatchedByTarget, MergeAction::Update { .. })
            | (MergeMatchKind::NotMatchedByTarget, MergeAction::Delete) => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "WHEN NOT MATCHED BY TARGET can only INSERT or DO NOTHING".to_string(),
                ));
            }
            (MergeMatchKind::NotMatchedBySource, MergeAction::Insert { .. }) => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "WHEN NOT MATCHED BY SOURCE cannot INSERT".to_string(),
                ));
            }
            (_, MergeAction::Update { assignments }) if assignments.is_empty() => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "MERGE UPDATE requires at least one assignment".to_string(),
                ));
            }
            (_, MergeAction::Insert { columns, values }) => {
                if values.is_empty() {
                    return Err(crate::protocol::EncodeError::InvalidAst(
                        "MERGE INSERT requires at least one value".to_string(),
                    ));
                }
                if !columns.is_empty() && columns.len() != values.len() {
                    return Err(crate::protocol::EncodeError::InvalidAst(
                        "MERGE INSERT column count must match value count".to_string(),
                    ));
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn validate_merge_source_query(query: &Qail) -> Result<(), crate::protocol::EncodeError> {
    validate_read_only_select_query_with_message(
        query,
        "MERGE source query must be read-only SELECT",
    )
}

fn encode_merge_source(
    source: &MergeSource,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    match source {
        MergeSource::Table { name, alias } => {
            push_table_ref(buf, name);
            if let Some(alias) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, alias, false);
            }
        }
        MergeSource::Query { query, alias } => {
            buf.extend_from_slice(b"(");
            encode_select(query, buf, params)?;
            buf.extend_from_slice(b")");
            if let Some(alias) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, alias, false);
            }
        }
    }
    Ok(())
}

fn encode_merge_action(
    action: &MergeAction,
    buf: &mut BytesMut,
) -> Result<(), crate::protocol::EncodeError> {
    match action {
        MergeAction::Update { assignments } => {
            buf.extend_from_slice(b"UPDATE SET ");
            for (i, (col, expr)) in assignments.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                push_identifier_ref(buf, col, false);
                buf.extend_from_slice(b" = ");
                encode_expr(expr, buf)?;
            }
        }
        MergeAction::Insert { columns, values } => {
            buf.extend_from_slice(b"INSERT");
            if !columns.is_empty() {
                buf.extend_from_slice(b" (");
                for (i, col) in columns.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    push_identifier_ref(buf, col, false);
                }
                buf.extend_from_slice(b")");
            }
            buf.extend_from_slice(b" VALUES (");
            for (i, value) in values.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_expr(value, buf)?;
            }
            buf.extend_from_slice(b")");
        }
        MergeAction::Delete => buf.extend_from_slice(b"DELETE"),
        MergeAction::DoNothing => buf.extend_from_slice(b"DO NOTHING"),
    }
    Ok(())
}

/// Encode an EXPORT command as `COPY (SELECT ...) TO STDOUT`.
///
/// # Arguments
///
/// * `cmd` — Qail AST command with `Action::Export`.
/// * `buf` — Output buffer to append the SQL bytes to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_export(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    buf.extend_from_slice(b"COPY (");
    encode_select_with_columns(cmd, &cmd.columns, buf, params)?;
    buf.extend_from_slice(b") TO STDOUT");
    Ok(())
}

fn encode_condition_group(
    conditions: &[Condition],
    joiner: &[u8],
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    for (idx, condition) in conditions.iter().enumerate() {
        if idx > 0 {
            buf.extend_from_slice(joiner);
        }
        encode_conditions(std::slice::from_ref(condition), buf, params)?;
    }
    Ok(())
}

/// Encode a WHERE clause that preserves each filter cage as its own group.
///
/// - AND cages are emitted first and joined internally with `AND`.
/// - OR cages are emitted after AND cages, joined internally with `OR`, and
///   parenthesized.
/// - Distinct OR cages stay separate and are joined together with `AND`, so
///   policy/user OR groups do not widen each other.
///
/// Example output: `WHERE is_active = $1 AND (topic ILIKE $2 OR question ILIKE $3)`
fn encode_where(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    if !cmd
        .cages
        .iter()
        .any(|cage| cage.kind == CageKind::Filter && !cage.conditions.is_empty())
    {
        return Ok(());
    }

    buf.extend_from_slice(b" WHERE ");

    let mut wrote_clause = false;
    for target_op in [LogicalOp::And, LogicalOp::Or] {
        for cage in &cmd.cages {
            if cage.kind != CageKind::Filter
                || cage.logical_op != target_op
                || cage.conditions.is_empty()
            {
                continue;
            }
            if wrote_clause {
                buf.extend_from_slice(b" AND ");
            }

            match target_op {
                LogicalOp::And => {
                    encode_condition_group(&cage.conditions, b" AND ", buf, params)?;
                }
                LogicalOp::Or => {
                    buf.extend_from_slice(b"(");
                    encode_condition_group(&cage.conditions, b" OR ", buf, params)?;
                    buf.extend_from_slice(b")");
                }
            }
            wrote_clause = true;
        }
    }

    Ok(())
}
