//! DML (Data Manipulation Language) encoders.
//!
//! SELECT, INSERT, UPDATE, DELETE, EXPORT, and CTE statements.

use bytes::BytesMut;
use qail_core::ast::write_payload::{
    check_insert_shape, check_update_shape, insert_columns, insert_values,
    is_positional_placeholder, update_assignments,
};
use qail_core::ast::{
    Action, CTEDef, CageKind, ColumnGeneration, Condition, ConflictAction, Constraint,
    CteMaterialization, CteSearchOrder, Expr, FromSource, GroupByClause, GroupByMode, JoinKind,
    LockMode, LogicalOp, Merge, MergeAction, MergeMatchKind, MergeSource, Operator, OverridingKind,
    Qail, SampleMethod, SortOrder, Value,
};
use qail_core::transpiler::escape_identifier;
use std::collections::HashSet;

use super::helpers::write_usize;
use super::values::{
    OperandMode, encode_call_args_with_params, encode_columns_with_params, encode_condition,
    encode_conditions, encode_expr, encode_expr_with_params, encode_ref_expr, encode_value,
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
    // Unicode letters/digits are valid PostgreSQL identifier characters and
    // escape_identifier leaves them unquoted. Quotes, spaces, dots, and other
    // punctuation stay rejected: atoms carry no quoting of their own yet.
    if !value.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(invalid_identifier(
            field,
            value,
            "expected only letters, digits, and underscores",
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

fn validate_write_column_expr(
    field: &str,
    expr: &Expr,
) -> Result<String, crate::protocol::EncodeError> {
    let Expr::Named(name) = expr else {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "{field} must be a simple column identifier"
        )));
    };
    validate_ident_atom(field, name)?;
    Ok(name.to_ascii_lowercase())
}

/// UPDATE SET target: a column, optionally followed by `[index]` subscripts
/// and `.field` selections. PostgreSQL forbids a table qualifier and
/// parentheses here, so the read-side expression encoding does not apply.
/// Returns the uniqueness key (the lowercased target text).
fn validate_update_target(
    field: &str,
    expr: &Expr,
) -> Result<String, crate::protocol::EncodeError> {
    let target_error = || {
        crate::protocol::EncodeError::InvalidAst(format!(
            "{field} must be a column with optional [integer or column] subscripts \
             and .field selections"
        ))
    };
    match expr {
        Expr::Named(name) => {
            validate_ident_atom(field, name)?;
            Ok(name.to_ascii_lowercase())
        }
        Expr::Subscript {
            expr,
            index,
            alias: None,
        } => {
            let base = validate_update_target(field, expr)?;
            let index = match index.as_ref() {
                Expr::Literal(Value::Int(n)) => n.to_string(),
                Expr::Named(column) => {
                    validate_qualified_ident(&format!("{field}.index"), column, false)?;
                    column.to_ascii_lowercase()
                }
                _ => return Err(target_error()),
            };
            Ok(format!("{base}[{index}]"))
        }
        Expr::FieldAccess {
            expr,
            field: name,
            alias: None,
        } => {
            let base = validate_update_target(field, expr)?;
            validate_ident_atom(&format!("{field}.field"), name)?;
            Ok(format!("{base}.{}", name.to_ascii_lowercase()))
        }
        _ => Err(target_error()),
    }
}

fn encode_update_target(expr: &Expr, buf: &mut BytesMut) {
    match expr {
        Expr::Subscript { expr, index, .. } => {
            encode_update_target(expr, buf);
            buf.extend_from_slice(b"[");
            match index.as_ref() {
                Expr::Literal(Value::Int(n)) => buf.extend_from_slice(n.to_string().as_bytes()),
                Expr::Named(column) => push_identifier_ref(buf, column, false),
                _ => {}
            }
            buf.extend_from_slice(b"]");
        }
        Expr::FieldAccess { expr, field, .. } => {
            encode_update_target(expr, buf);
            buf.extend_from_slice(b".");
            push_identifier_ref(buf, field, false);
        }
        Expr::Named(name) => push_identifier_ref(buf, name, false),
        _ => {}
    }
}

fn validate_insert_shape(cmd: &Qail) -> Result<(), crate::protocol::EncodeError> {
    check_insert_shape(
        cmd,
        validate_write_column_expr,
        crate::protocol::EncodeError::InvalidAst,
    )?;
    validate_on_conflict_shape(cmd)?;
    Ok(())
}

fn validate_update_shape(cmd: &Qail) -> Result<(), crate::protocol::EncodeError> {
    // Targets may carry `[index]` / `.field` selections (`validate_update_target`);
    // a plain column folds exactly as `validate_write_column_expr` does.
    check_update_shape(
        cmd,
        validate_update_target,
        crate::protocol::EncodeError::InvalidAst,
    )
}

fn validate_select_shape(cmd: &Qail) -> Result<(), crate::protocol::EncodeError> {
    for cage in &cmd.cages {
        match cage.kind {
            CageKind::Payload if !cage.conditions.is_empty() => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "SELECT cannot contain payload assignments".to_string(),
                ));
            }
            CageKind::Qualify if !cage.conditions.is_empty() => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "QUALIFY is not supported by PostgreSQL".to_string(),
                ));
            }
            _ => {}
        }
    }

    Ok(())
}

fn validate_on_conflict_shape(cmd: &Qail) -> Result<(), crate::protocol::EncodeError> {
    let Some(on_conflict) = &cmd.on_conflict else {
        return Ok(());
    };

    let mut conflict_targets = HashSet::new();
    for column in &on_conflict.columns {
        validate_ident_atom("on_conflict.column", column)?;
        if !conflict_targets.insert(column.to_ascii_lowercase()) {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "ON CONFLICT target column appears more than once: {column}"
            )));
        }
    }
    if let Some(constraint) = &on_conflict.constraint {
        validate_ident_atom("on_conflict.constraint", constraint)?;
        if !on_conflict.columns.is_empty() {
            return Err(crate::protocol::EncodeError::InvalidAst(
                "ON CONFLICT target cannot combine columns with ON CONSTRAINT".to_string(),
            ));
        }
    }

    if let ConflictAction::DoUpdate { assignments } = &on_conflict.action {
        cmd.validate_conflict_update_scope()
            .map_err(|error| crate::protocol::EncodeError::InvalidAst(error.to_string()))?;
        if cmd
            .conflict_update_scope
            .iter()
            .any(|condition| !on_conflict.where_conditions.contains(condition))
        {
            return Err(crate::protocol::EncodeError::InvalidAst(
                "ON CONFLICT DO UPDATE is missing an applied scope guard".to_string(),
            ));
        }
        if on_conflict.columns.is_empty() && on_conflict.constraint.is_none() {
            return Err(crate::protocol::EncodeError::InvalidAst(
                "ON CONFLICT DO UPDATE requires at least one conflict target".to_string(),
            ));
        }
        if assignments.is_empty() {
            return Err(crate::protocol::EncodeError::InvalidAst(
                "ON CONFLICT DO UPDATE requires at least one assignment".to_string(),
            ));
        }
        validate_merge_write_targets(
            assignments.iter().map(|(column, _)| column.as_str()),
            "ON CONFLICT UPDATE",
        )?;
    }

    Ok(())
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
            col,
            func,
            distinct,
            alias,
            filter,
            args,
            order_by,
            within_group,
        } => {
            qail_core::ast::check_aggregate_shape(
                *func,
                col,
                *distinct,
                args,
                order_by,
                within_group,
            )
            .map_err(crate::protocol::EncodeError::InvalidAst)?;
            // `col` is empty when the arguments live in `args` (or MODE() has none).
            if !col.is_empty() {
                validate_qualified_ident(field, col, true)?;
            }
            for arg in args {
                validate_expr_ref(&format!("{field}.arg"), arg)?;
            }
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            if let Some(filter) = filter {
                validate_conditions(&format!("{field}.filter"), filter)?;
            }
            for cage in order_by.iter().chain(within_group) {
                validate_cage_conditions(&format!("{field}.order_by"), false, &cage.conditions)?;
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
            filter,
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
            if let Some(filter) = filter {
                validate_conditions(&format!("{field}.filter"), filter)?;
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
                if segment
                    .as_key()
                    .is_some_and(|key| key.as_bytes().contains(&0))
                {
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
            validate_function_call_args(field, args)?;
            if let Some(alias) = alias {
                validate_ident_atom(&format!("{field}.alias"), alias)?;
            }
            Ok(())
        }
        Expr::FunctionArg { .. } => Err(crate::protocol::EncodeError::InvalidAst(format!(
            "{field}: named/VARIADIC argument outside a function call"
        ))),
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
        Expr::ArraySlice {
            expr,
            lower,
            upper,
            alias,
        } => {
            validate_expr_ref(&format!("{field}.slice"), expr)?;
            for bound in [lower, upper].into_iter().flatten() {
                validate_expr_ref(&format!("{field}.slice_bound"), bound)?;
            }
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
        Expr::Default => Err(crate::protocol::EncodeError::InvalidAst(format!(
            "{field}: {DEFAULT_POSITION_ERROR}"
        ))),
    }
}

pub(crate) const DEFAULT_POSITION_ERROR: &str =
    "DEFAULT is only valid as a whole MERGE assignment or INSERT value";

/// Validate a whole MERGE UPDATE assignment value or INSERT value, the only places `DEFAULT` is valid.
fn validate_write_value_ref(field: &str, expr: &Expr) -> Result<(), crate::protocol::EncodeError> {
    match expr {
        Expr::Default => Ok(()),
        expr => validate_expr_ref(field, expr),
    }
}

fn encode_write_value(
    expr: &Expr,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    match expr {
        Expr::Default => {
            buf.extend_from_slice(b"DEFAULT");
            Ok(())
        }
        expr => encode_expr_with_params(expr, buf, params),
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
    validate_condition_left(field, condition)?;
    validate_value_ref(&format!("{field}.value"), &condition.value)
}

/// The left side as `encode_condition` reads it: a column list for text
/// search, ignored for EXISTS (the parser leaves it empty), else an expression.
fn validate_condition_left(
    field: &str,
    condition: &Condition,
) -> Result<(), crate::protocol::EncodeError> {
    match condition.op {
        Operator::TextSearch => {
            validate_text_search_columns(&format!("{field}.left"), &condition.left)
        }
        Operator::Exists | Operator::NotExists => Ok(()),
        _ => validate_expr_ref(&format!("{field}.left"), &condition.left),
    }
}

pub(crate) fn validate_text_search_columns(
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
            validate_condition_left(field, condition)?;
        }
        validate_value_ref(&format!("{field}.value"), &condition.value)?;
    }
    Ok(())
}

fn validate_dml_command(
    cmd: &Qail,
    projection_columns: &[Expr],
) -> Result<(), crate::protocol::EncodeError> {
    cmd.validate_applied_insert_scope()
        .map_err(|error| crate::protocol::EncodeError::InvalidAst(error.to_string()))?;
    if !cmd.table.is_empty() {
        validate_table_ref("table", &cmd.table)?;
    }
    validate_from_source(cmd)?;

    if let Some((_, percent, _)) = cmd.sample
        && (!percent.is_finite() || !(0.0..=100.0).contains(&percent))
    {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "TABLESAMPLE percent must be finite and between 0 and 100".to_string(),
        ));
    }
    for cage in &cmd.cages {
        if let CageKind::Sample(percent) = cage.kind
            && percent > 100
        {
            return Err(crate::protocol::EncodeError::InvalidAst(
                "TABLESAMPLE percent must be between 0 and 100".to_string(),
            ));
        }
    }

    if cmd.skip_locked && cmd.lock_mode.is_none() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "SKIP LOCKED requires a row lock mode".to_string(),
        ));
    }
    if cmd.lock_nowait && cmd.lock_mode.is_none() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "NOWAIT requires a row lock mode".to_string(),
        ));
    }
    if !cmd.lock_of.is_empty() && cmd.lock_mode.is_none() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "row lock OF requires a row lock mode".to_string(),
        ));
    }
    if cmd.lock_nowait && cmd.skip_locked {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "NOWAIT and SKIP LOCKED are mutually exclusive".to_string(),
        ));
    }
    for name in &cmd.lock_of {
        // PostgreSQL rejects qualified names here: OF takes FROM names or aliases.
        validate_ident_atom("lock_of", name)?;
    }

    if let Some((_, true)) = cmd.fetch {
        let has_order_by = cmd
            .cages
            .iter()
            .any(|cage| matches!(cage.kind, CageKind::Sort(_)) && !cage.conditions.is_empty());
        if !has_order_by {
            return Err(crate::protocol::EncodeError::InvalidAst(
                "FETCH WITH TIES requires ORDER BY".to_string(),
            ));
        }
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
            // Only Value::Column names a column; a Value::String is always a literal.
            validate_conditions("join.on", conditions)?;
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
        validate_cte_search_cycle(cte)?;
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
    validate_returning_aliases(cmd)?;

    if let Some(on_conflict) = &cmd.on_conflict {
        for column in &on_conflict.columns {
            validate_qualified_ident("on_conflict.column", column, false)?;
        }
        if let ConflictAction::DoUpdate { assignments } = &on_conflict.action {
            for (column, expr) in assignments {
                validate_qualified_ident("on_conflict.assignment.column", column, false)?;
                validate_expr_ref("on_conflict.assignment.expr", expr)?;
            }
            validate_conditions(
                "on_conflict.where_conditions",
                &on_conflict.where_conditions,
            )?;
        }
    }

    if let Some(merge) = &cmd.merge {
        if let Some(alias) = &merge.target_alias {
            validate_ident_atom("merge.target_alias", alias)?;
        }
        match &merge.source {
            MergeSource::Table { name, alias, .. } => {
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
                        validate_write_value_ref("merge.update.expr", expr)?;
                    }
                }
                MergeAction::Insert {
                    columns, values, ..
                } => {
                    for column in columns {
                        validate_qualified_ident("merge.insert.column", column, false)?;
                    }
                    for value in values {
                        validate_write_value_ref("merge.insert.value", value)?;
                    }
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }

    Ok(())
}

/// Call arguments: the only place `Expr::FunctionArg` may appear.
fn validate_function_call_args(
    field: &str,
    args: &[Expr],
) -> Result<(), crate::protocol::EncodeError> {
    qail_core::ast::validate_function_args(args)
        .map_err(|error| crate::protocol::EncodeError::InvalidAst(format!("{field}: {error}")))?;
    for arg in args {
        match arg {
            Expr::FunctionArg { name, value, .. } => {
                if let Some(name) = name {
                    validate_ident_atom(&format!("{field}.arg_name"), name)?;
                }
                validate_expr_ref(&format!("{field}.arg"), value)?;
            }
            other => validate_expr_ref(&format!("{field}.arg"), other)?,
        }
    }
    Ok(())
}

fn validate_from_source(cmd: &Qail) -> Result<(), crate::protocol::EncodeError> {
    let Some(source) = &cmd.from_source else {
        return Ok(());
    };
    if let Some(error) = qail_core::transpiler::dml::select::from_source_error(cmd, source) {
        return Err(crate::protocol::EncodeError::InvalidAst(error));
    }
    validate_ident_atom("from_source.alias", source.alias())?;
    for column in source.column_alias_list() {
        validate_ident_atom("from_source.column_alias", column)?;
    }
    match source {
        FromSource::Subquery { query, .. } => {
            validate_read_only_select_query_with_message(
                query,
                "FROM subquery requires a read-only get/with query",
            )?;
            validate_dml_command(query, &query.columns)?;
        }
        FromSource::Function { name, args, .. } => {
            validate_qualified_ident("from_source.function", name, false)?;
            validate_function_call_args("from_source", args)?;
        }
    }
    Ok(())
}

/// `(subquery) AS alias (cols)` or `func(args) [WITH ORDINALITY] AS alias (cols)`,
/// parameters numbered in text order with the rest of the statement.
/// Callers run `validate_dml_command` (and so `validate_from_source`) first.
fn encode_from_source(
    source: &FromSource,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    match source {
        FromSource::Subquery { query, .. } => {
            buf.extend_from_slice(b"(");
            encode_select(query, buf, params)?;
            buf.extend_from_slice(b")");
        }
        FromSource::Function {
            name,
            args,
            with_ordinality,
            ..
        } => {
            buf.extend_from_slice(name.to_uppercase().as_bytes());
            buf.extend_from_slice(b"(");
            encode_call_args_with_params(args, buf, params)?;
            buf.extend_from_slice(b")");
            if *with_ordinality {
                buf.extend_from_slice(b" WITH ORDINALITY");
            }
        }
    }
    buf.extend_from_slice(b" AS ");
    push_identifier_ref(buf, source.alias(), false);
    let columns = source.column_alias_list();
    if !columns.is_empty() {
        buf.extend_from_slice(b" (");
        for (i, column) in columns.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            push_identifier_ref(buf, column, false);
        }
        buf.extend_from_slice(b")");
    }
    Ok(())
}

/// `RETURNING WITH (...)` needs a write action, a non-empty RETURNING list,
/// and two distinct plain identifiers.
fn validate_returning_aliases(cmd: &Qail) -> Result<(), crate::protocol::EncodeError> {
    let Some(aliases) = &cmd.returning_aliases else {
        return Ok(());
    };
    let Some(parts) = aliases.sql_parts() else {
        return Ok(());
    };
    if !matches!(
        cmd.action,
        Action::Add | Action::Set | Action::Del | Action::Merge | Action::Upsert | Action::Put
    ) {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "RETURNING WITH aliases require a write action, got {}",
            cmd.action
        )));
    }
    if cmd.returning.as_ref().is_none_or(|cols| cols.is_empty()) {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "RETURNING WITH aliases require a non-empty RETURNING list".to_string(),
        ));
    }
    for (_, alias) in &parts {
        validate_ident_atom("returning_aliases", alias)?;
    }
    if let [(_, before), (_, after)] = parts.as_slice()
        && before.eq_ignore_ascii_case(after)
    {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "RETURNING WITH aliases must differ".to_string(),
        ));
    }
    Ok(())
}

/// `WITH (OLD AS x, NEW AS y) ` after `RETURNING `; nothing when unset.
fn encode_returning_aliases(cmd: &Qail, buf: &mut BytesMut) {
    let Some(parts) = cmd.returning_aliases.as_ref().and_then(|a| a.sql_parts()) else {
        return;
    };
    buf.extend_from_slice(b"WITH (");
    for (i, (keyword, alias)) in parts.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        buf.extend_from_slice(keyword.as_bytes());
        buf.extend_from_slice(b" AS ");
        push_identifier_ref(buf, alias, false);
    }
    buf.extend_from_slice(b") ");
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
    encode_select_with_columns(cmd, &cmd.columns, buf, params, WithScope::Nested)
}

/// Encode a top-level SELECT statement.
///
/// Unlike [`encode_select`], which serves subquery slots, the statement's own
/// WITH list may hold INSERT/UPDATE/DELETE bodies. Set operands and nested
/// queries stay read-only.
pub fn encode_select_statement(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    let message = "read-only SELECT query slot requires get/with action";
    if !matches!(cmd.action, Action::Get | Action::With) {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "{message}, got {}",
            cmd.action
        )));
    }
    for (_, set_query) in &cmd.set_ops {
        validate_read_only_select_query_with_message(set_query, message)?;
    }
    if let Some(ref source_query) = cmd.source_query {
        validate_read_only_select_query_with_message(source_query, message)?;
    }
    encode_select_with_columns(cmd, &cmd.columns, buf, params, WithScope::TopLevel)
}

/// Where a WITH list sits. PostgreSQL accepts data-modifying CTE bodies only
/// in the WITH attached to the top-level statement (SQLSTATE 0A000 otherwise).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WithScope {
    TopLevel,
    Nested,
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
        args: Vec::new(),
        order_by: Vec::new(),
        within_group: Vec::new(),
    }];
    encode_select_with_columns(cmd, &count_columns, buf, params, WithScope::Nested)
}

fn encode_select_with_columns(
    cmd: &Qail,
    columns: &[Expr],
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
    with_scope: WithScope,
) -> Result<(), crate::protocol::EncodeError> {
    validate_dml_command(cmd, columns)?;
    validate_select_shape(cmd)?;

    if try_encode_simple_select_fast(cmd, columns, buf, params)? {
        return Ok(());
    }

    let select_start = buf.len();

    // CTE prefix
    encode_cte_prefix_scoped(cmd, buf, params, with_scope)?;

    buf.extend_from_slice(b"SELECT ");

    // DISTINCT ON (col1, col2, ...)
    if !cmd.distinct_on.is_empty() {
        buf.extend_from_slice(b"DISTINCT ON (");
        for (i, expr) in cmd.distinct_on.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_ref_expr(expr, buf, Some(params))?;
        }
        buf.extend_from_slice(b") ");
    } else if cmd.distinct {
        // Regular DISTINCT (mutually exclusive with DISTINCT ON)
        buf.extend_from_slice(b"DISTINCT ");
    }

    encode_columns_with_params(columns, buf, Some(params))?;

    // FROM
    buf.extend_from_slice(b" FROM ");
    if let Some(source) = &cmd.from_source {
        encode_from_source(source, buf, params)?;
    } else {
        if cmd.only_table {
            buf.extend_from_slice(b"ONLY ");
        }
        push_table_ref(buf, &cmd.table);
    }
    append_table_sample_clause(cmd, buf);

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
                encode_condition(cond, buf, OperandMode::Join(params))?;
            }
        }
    }

    // WHERE (supports AND + OR filter cages)
    encode_where(cmd, buf, params)?;

    encode_group_by(cmd, columns, buf, params)?;

    // HAVING binds follow WHERE binds: the clause follows WHERE in the text.
    if !cmd.having.is_empty() {
        buf.extend_from_slice(b" HAVING ");
        encode_condition_group(&cmd.having, b" AND ", buf, params)?;
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
                encode_ref_expr(&cond.left, buf, Some(params))?;
                append_sort_order(*order, buf);
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
    append_lock_clause(cmd, buf);

    if !cmd.set_ops.is_empty() && set_operand_has_branch_clauses(cmd) {
        wrap_sql_range_in_parens(buf, select_start);
    }

    // SET OPERATIONS (UNION, INTERSECT, EXCEPT)
    for (set_op, other_cmd) in &cmd.set_ops {
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(set_op.sql_keyword().as_bytes());
        buf.extend_from_slice(b" ");
        encode_set_operand(other_cmd, buf, params)?;
    }

    Ok(())
}

/// Encode ` GROUP BY ...` from [`Qail::group_by_clause`], shared with the
/// transpiler so both paths pick the same keys and mode.
fn encode_group_by(
    cmd: &Qail,
    columns: &[Expr],
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    let clause = cmd
        .group_by_clause(columns)
        .map_err(|message| crate::protocol::EncodeError::InvalidAst(message.to_string()))?;
    let Some(clause) = clause else {
        return Ok(());
    };

    buf.extend_from_slice(b" GROUP BY ");
    let (prefix, keys): (&[u8], _) = match clause {
        GroupByClause::Keys(keys) => (b"", keys),
        GroupByClause::Rollup(keys) => (b"ROLLUP(", keys),
        GroupByClause::Cube(keys) => (b"CUBE(", keys),
        GroupByClause::GroupingSets(sets) => {
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
            return Ok(());
        }
    };

    buf.extend_from_slice(prefix);
    for (i, key) in keys.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        // GROUP BY binds sit between WHERE and HAVING binds, as in the text.
        encode_ref_expr(key, buf, Some(params))?;
    }
    if !prefix.is_empty() {
        buf.extend_from_slice(b")");
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
        || query.lock_mode.is_some()
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

fn append_sort_order(order: SortOrder, buf: &mut BytesMut) {
    match order {
        SortOrder::Asc => {}
        SortOrder::Desc => buf.extend_from_slice(b" DESC"),
        SortOrder::AscNullsFirst => buf.extend_from_slice(b" ASC NULLS FIRST"),
        SortOrder::AscNullsLast => buf.extend_from_slice(b" ASC NULLS LAST"),
        SortOrder::DescNullsFirst => buf.extend_from_slice(b" DESC NULLS FIRST"),
        SortOrder::DescNullsLast => buf.extend_from_slice(b" DESC NULLS LAST"),
    }
}

fn append_table_sample_clause(cmd: &Qail, buf: &mut BytesMut) {
    let sample = cmd.sample.or_else(|| {
        cmd.cages.iter().find_map(|cage| match cage.kind {
            CageKind::Sample(percent) => Some((SampleMethod::Bernoulli, percent as f64, None)),
            _ => None,
        })
    });
    let Some((method, percent, seed)) = sample else {
        return;
    };

    match method {
        SampleMethod::Bernoulli => buf.extend_from_slice(b" TABLESAMPLE BERNOULLI ("),
        SampleMethod::System => buf.extend_from_slice(b" TABLESAMPLE SYSTEM ("),
    }
    buf.extend_from_slice(percent.to_string().as_bytes());
    buf.extend_from_slice(b")");
    if let Some(seed) = seed {
        buf.extend_from_slice(b" REPEATABLE (");
        buf.extend_from_slice(seed.to_string().as_bytes());
        buf.extend_from_slice(b")");
    }
}

fn append_lock_clause(cmd: &Qail, buf: &mut BytesMut) {
    let Some(lock_mode) = cmd.lock_mode else {
        return;
    };

    match lock_mode {
        LockMode::Update => buf.extend_from_slice(b" FOR UPDATE"),
        LockMode::NoKeyUpdate => buf.extend_from_slice(b" FOR NO KEY UPDATE"),
        LockMode::Share => buf.extend_from_slice(b" FOR SHARE"),
        LockMode::KeyShare => buf.extend_from_slice(b" FOR KEY SHARE"),
    }

    for (i, name) in cmd.lock_of.iter().enumerate() {
        buf.extend_from_slice(if i == 0 { b" OF " } else { b", " });
        push_identifier_ref(buf, name, false);
    }

    // validate_dml_command rejects NOWAIT together with SKIP LOCKED.
    if cmd.lock_nowait {
        buf.extend_from_slice(b" NOWAIT");
    } else if cmd.skip_locked {
        buf.extend_from_slice(b" SKIP LOCKED");
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
        || cmd.lock_mode.is_some()
        || cmd.sample.is_some()
        || cmd
            .cages
            .iter()
            .any(|cage| matches!(cage.kind, CageKind::Sample(_)))
        || cmd.only_table
        || cmd.from_source.is_some()
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
    encode_cte_prefix_scoped(cmd, buf, params, WithScope::Nested)
}

fn encode_cte_prefix_scoped(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
    with_scope: WithScope,
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
        encode_single_cte(cte, buf, params, with_scope)?;
    }

    buf.extend_from_slice(b" ");
    Ok(())
}

/// Encode a single CTE definition.
fn encode_single_cte(
    cte: &CTEDef,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
    with_scope: WithScope,
) -> Result<(), super::super::EncodeError> {
    let data_modifying = matches!(
        cte.base_query.action,
        Action::Add | Action::Set | Action::Del
    );
    if data_modifying {
        if with_scope == WithScope::Nested {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "data-modifying CTE `{}` requires the WITH of the top-level statement",
                cte.name
            )));
        }
        if cte.recursive_query.is_some() {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "data-modifying CTE `{}` cannot have a recursive arm",
                cte.name
            )));
        }
    }

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

    buf.extend_from_slice(match cte.materialization {
        None => b" AS (",
        Some(CteMaterialization::Materialized) => b" AS MATERIALIZED (",
        Some(CteMaterialization::NotMaterialized) => b" AS NOT MATERIALIZED (",
    });

    if data_modifying {
        // The body's own WITH, source query, and subqueries are nested slots.
        match cte.base_query.action {
            Action::Add => encode_insert_scoped(&cte.base_query, buf, params, WithScope::Nested)?,
            Action::Set => encode_update_scoped(&cte.base_query, buf, params, WithScope::Nested)?,
            _ => encode_delete_scoped(&cte.base_query, buf, params, WithScope::Nested)?,
        }
        buf.extend_from_slice(b")");
        return Ok(());
    }

    encode_recursive_cte_arm(&cte.base_query, buf, params)?;

    // Recursive part (UNION ALL)
    if cte.recursive
        && let Some(ref recursive_query) = cte.recursive_query
    {
        buf.extend_from_slice(b" UNION ALL ");
        encode_recursive_cte_arm(recursive_query, buf, params)?;
    }

    buf.extend_from_slice(b")");

    // validate_dml_command checked recursion and names.
    if let Some(search) = &cte.search {
        buf.extend_from_slice(match search.order {
            CteSearchOrder::DepthFirst => b" SEARCH DEPTH FIRST BY ",
            CteSearchOrder::BreadthFirst => b" SEARCH BREADTH FIRST BY ",
        });
        push_ident_list(buf, &search.by);
        buf.extend_from_slice(b" SET ");
        push_identifier_ref(buf, &search.set_column, false);
    }
    if let Some(cycle) = &cte.cycle {
        buf.extend_from_slice(b" CYCLE ");
        push_ident_list(buf, &cycle.columns);
        buf.extend_from_slice(b" SET ");
        push_identifier_ref(buf, &cycle.set_column, false);
        buf.extend_from_slice(b" USING ");
        push_identifier_ref(buf, &cycle.using_column, false);
    }
    Ok(())
}

fn push_ident_list(buf: &mut BytesMut, names: &[String]) {
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        push_identifier_ref(buf, name, false);
    }
}

fn validate_cte_search_cycle(cte: &CTEDef) -> Result<(), crate::protocol::EncodeError> {
    if cte.search.is_none() && cte.cycle.is_none() {
        return Ok(());
    }
    if !cte.recursive || cte.recursive_query.is_none() {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "SEARCH/CYCLE requires a recursive CTE: {}",
            cte.name
        )));
    }
    let non_empty = |field: &str, names: &[String]| {
        if names.is_empty() {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "{field} requires at least one column"
            )));
        }
        names
            .iter()
            .try_for_each(|name| validate_ident_atom(field, name))
    };
    if let Some(search) = &cte.search {
        non_empty("cte.search.by", &search.by)?;
        validate_ident_atom("cte.search.set", &search.set_column)?;
    }
    if let Some(cycle) = &cte.cycle {
        non_empty("cte.cycle.columns", &cycle.columns)?;
        validate_ident_atom("cte.cycle.set", &cycle.set_column)?;
        validate_ident_atom("cte.cycle.using", &cycle.using_column)?;
    }
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
    encode_insert_scoped(cmd, buf, params, WithScope::TopLevel)
}

fn encode_insert_scoped(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
    with_scope: WithScope,
) -> Result<(), crate::protocol::EncodeError> {
    validate_dml_command(cmd, &cmd.columns)?;
    validate_insert_shape(cmd)?;

    // WITH binds first: its placeholders precede every later clause.
    encode_cte_prefix_scoped(cmd, buf, params, with_scope)?;
    buf.extend_from_slice(b"INSERT INTO ");
    push_table_ref(buf, &cmd.table);

    // Column list: explicit columns, else the names of a named payload. The
    // transpiler reads the same list (qail_core::ast::write_payload).
    let columns = insert_columns(cmd);
    if !columns.is_empty() {
        buf.extend_from_slice(b" (");
        for (i, column) in columns.into_iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_expr(column, buf)?;
        }
        buf.extend_from_slice(b")");
    }

    if let Some(overriding) = &cmd.overriding {
        match overriding {
            OverridingKind::SystemValue => buf.extend_from_slice(b" OVERRIDING SYSTEM VALUE"),
            OverridingKind::UserValue => buf.extend_from_slice(b" OVERRIDING USER VALUE"),
        }
    }

    // INSERT ... SELECT source query takes the place of VALUES.
    if cmd.default_values {
        buf.extend_from_slice(b" DEFAULT VALUES");
    } else if let Some(source_query) = &cmd.source_query {
        buf.extend_from_slice(b" ");
        encode_select(source_query, buf, params)?;
    } else {
        buf.extend_from_slice(b" VALUES (");
        for (i, cond) in insert_values(cmd).iter().enumerate() {
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

        // Conflict target: named constraint, inferred columns, or none
        if let Some(constraint) = &on_conflict.constraint {
            buf.extend_from_slice(b"ON CONSTRAINT ");
            push_identifier_ref(buf, constraint, false);
            buf.extend_from_slice(b" ");
        } else if !on_conflict.columns.is_empty() {
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
                    encode_ref_expr(expr, buf, Some(params))?;
                }
                encode_where(cmd, buf, params)?;
                if !on_conflict.where_conditions.is_empty() {
                    // Conflict guards (including injected tenant scope) must also
                    // restrict the existing row when command filters are present.
                    let has_filters = cmd
                        .cages
                        .iter()
                        .any(|cage| cage.kind == CageKind::Filter && !cage.conditions.is_empty());
                    buf.extend_from_slice(if has_filters { b" AND " } else { b" WHERE " });
                    encode_conditions(&on_conflict.where_conditions, buf, params)?;
                }
            }
        }
    }

    encode_returning(cmd, buf, params)
}

/// RETURNING contract shared by INSERT, UPDATE, DELETE and MERGE (and the
/// transpiler): `None` and `Some(empty)` emit no clause; `[Expr::Star]` is
/// `RETURNING *`. Output expressions share the statement's `$N` numbering.
fn encode_returning(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    if let Some(ret_cols) = &cmd.returning
        && !ret_cols.is_empty()
    {
        buf.extend_from_slice(b" RETURNING ");
        // PG 18 `RETURNING WITH (OLD AS o, NEW AS n)` precedes the items.
        encode_returning_aliases(cmd, buf);
        encode_columns_with_params(ret_cols, buf, Some(params))?;
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
    encode_update_scoped(cmd, buf, params, WithScope::TopLevel)
}

fn encode_update_scoped(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
    with_scope: WithScope,
) -> Result<(), crate::protocol::EncodeError> {
    validate_dml_command(cmd, &cmd.columns)?;
    validate_update_shape(cmd)?;

    encode_cte_prefix_scoped(cmd, buf, params, with_scope)?;
    buf.extend_from_slice(b"UPDATE ");
    if cmd.only_table {
        buf.extend_from_slice(b"ONLY ");
    }
    push_table_ref(buf, &cmd.table);
    buf.extend_from_slice(b" SET ");

    // SET clause: explicit columns pair with positional values, else each
    // named payload condition assigns its own column (shared with the
    // transpiler via qail_core::ast::write_payload). A target may be a
    // `col[1]` / `col.field` chain, validated by validate_update_shape.
    for (i, (col, cond)) in update_assignments(cmd).into_iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        encode_update_target(col, buf);
        buf.extend_from_slice(b" = ");
        encode_value(&cond.value, buf, params)?;
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

    encode_returning(cmd, buf, params)
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
    encode_delete_scoped(cmd, buf, params, WithScope::TopLevel)
}

fn encode_delete_scoped(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
    with_scope: WithScope,
) -> Result<(), crate::protocol::EncodeError> {
    validate_dml_command(cmd, &cmd.columns)?;

    encode_cte_prefix_scoped(cmd, buf, params, with_scope)?;
    buf.extend_from_slice(b"DELETE FROM ");
    if cmd.only_table {
        buf.extend_from_slice(b"ONLY ");
    }
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

    encode_returning(cmd, buf, params)
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
    // Command-level INSERT flags have no single arm to apply to.
    if cmd.default_values {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "MERGE ignores command DEFAULT VALUES; use when_not_matched_insert_default_values"
                .to_string(),
        ));
    }
    if cmd.overriding.is_some() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "MERGE ignores command OVERRIDING; use when_not_matched_insert_overriding".to_string(),
        ));
    }
    validate_merge_shape(merge)?;

    encode_cte_prefix(cmd, buf, params)?;
    buf.extend_from_slice(b"MERGE INTO ");
    if cmd.only_table {
        buf.extend_from_slice(b"ONLY ");
    }
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
        encode_merge_action(&clause.action, buf, params)?;
    }

    encode_returning(cmd, buf, params)
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
            (_, MergeAction::Update { assignments }) => {
                validate_merge_write_targets(
                    assignments.iter().map(|(column, _)| column.as_str()),
                    "UPDATE",
                )?;
            }
            (
                _,
                MergeAction::Insert {
                    columns,
                    values,
                    overriding,
                    default_values: true,
                },
            ) if !columns.is_empty() || !values.is_empty() || overriding.is_some() => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "MERGE INSERT DEFAULT VALUES cannot have columns, values, or OVERRIDING"
                        .to_string(),
                ));
            }
            (
                _,
                MergeAction::Insert {
                    default_values: true,
                    ..
                },
            ) => {}
            (
                _,
                MergeAction::Insert {
                    columns, values, ..
                },
            ) => {
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
                validate_merge_write_targets(columns.iter().map(String::as_str), "INSERT")?;
            }
            _ => {}
        }
    }

    Ok(())
}

fn validate_merge_write_targets<'a>(
    columns: impl Iterator<Item = &'a str>,
    action: &str,
) -> Result<(), crate::protocol::EncodeError> {
    let mut seen = HashSet::new();
    for column in columns {
        validate_ident_atom(&format!("merge.{action}.column"), column)?;
        if !seen.insert(column.to_ascii_lowercase()) {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "MERGE {action} target column is assigned more than once: {column}"
            )));
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
        MergeSource::Table { name, alias, only } => {
            if *only {
                buf.extend_from_slice(b"ONLY ");
            }
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
    params: &mut Vec<Option<Vec<u8>>>,
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
                encode_write_value(expr, buf, params)?;
            }
        }
        MergeAction::Insert {
            default_values: true,
            ..
        } => buf.extend_from_slice(b"INSERT DEFAULT VALUES"),
        MergeAction::Insert {
            columns,
            values,
            overriding,
            ..
        } => {
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
            match overriding {
                Some(OverridingKind::SystemValue) => {
                    buf.extend_from_slice(b" OVERRIDING SYSTEM VALUE")
                }
                Some(OverridingKind::UserValue) => buf.extend_from_slice(b" OVERRIDING USER VALUE"),
                None => {}
            }
            buf.extend_from_slice(b" VALUES (");
            for (i, value) in values.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_write_value(value, buf, params)?;
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
    encode_select_with_columns(cmd, &cmd.columns, buf, params, WithScope::Nested)?;
    buf.extend_from_slice(b") TO STDOUT");
    Ok(())
}

/// Reject any clause beyond action + table. TRUNCATE and LOCK act on the whole
/// relation; a filter left on the AST would otherwise be dropped silently.
fn validate_table_only_command(cmd: &Qail, verb: &str) -> Result<(), crate::protocol::EncodeError> {
    let bare = Qail {
        action: cmd.action,
        table: cmd.table.clone(),
        ..Default::default()
    };
    if *cmd != bare {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "{verb} takes only a table; filters and other clauses would not apply"
        )));
    }
    validate_qualified_ident("table", &cmd.table, false)
}

/// Encode `TRUNCATE TABLE <table>`.
pub fn encode_truncate(cmd: &Qail, buf: &mut BytesMut) -> Result<(), crate::protocol::EncodeError> {
    validate_table_only_command(cmd, "TRUNCATE")?;
    buf.extend_from_slice(b"TRUNCATE TABLE ");
    push_identifier_ref(buf, &cmd.table, false);
    Ok(())
}

/// Encode `LOCK TABLE <table> IN ACCESS EXCLUSIVE MODE`, the only mode the
/// AST models. PostgreSQL accepts LOCK only inside a transaction block.
pub fn encode_lock_table(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), crate::protocol::EncodeError> {
    validate_table_only_command(cmd, "LOCK TABLE")?;
    buf.extend_from_slice(b"LOCK TABLE ");
    push_identifier_ref(buf, &cmd.table, false);
    buf.extend_from_slice(b" IN ACCESS EXCLUSIVE MODE");
    Ok(())
}

/// Encode `EXPLAIN [ANALYZE] SELECT ...` for `Action::Explain` and
/// `Action::ExplainAnalyze`. The explained query stays read-only: EXPLAIN
/// ANALYZE executes it, so data-modifying CTE bodies are rejected.
pub fn encode_explain(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    let prefix: &[u8] = match cmd.action {
        Action::Explain => b"EXPLAIN ",
        Action::ExplainAnalyze => b"EXPLAIN ANALYZE ",
        other => return Err(crate::protocol::EncodeError::UnsupportedAction(other)),
    };
    let mut query = cmd.clone();
    query.action = Action::Get;
    buf.extend_from_slice(prefix);
    encode_select(&query, buf, params)
}

/// `Action::Put` has no native encoding. Its preview upsert updates every
/// payload column on conflict with no existing-row guard, so a scoped write
/// could overwrite another scope's row.
pub(crate) fn reject_put() -> crate::protocol::EncodeError {
    crate::protocol::EncodeError::InvalidAst(
        "Put has no native encoding (its upsert has no existing-row guard); \
         use Qail::add(..).on_conflict_update(..)"
            .to_string(),
    )
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
