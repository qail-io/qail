//! Value and expression encoding.
//!
//! Functions for encoding Expr, Value, Operator, and conditions to wire format.

use bytes::BytesMut;
use qail_core::ast::{
    CageKind, Condition, Constraint, Expr, FrameBound, ModKind, Operator, SortOrder, Value,
    WindowFrame,
};
use qail_core::transpiler::escape_identifier;

use super::super::helpers::{NUMERIC_VALUES, i64_to_bytes, write_param_placeholder};

fn reject_non_finite_f64(label: &str, value: f64) -> Result<(), crate::protocol::EncodeError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(crate::protocol::EncodeError::InvalidAst(format!(
            "{label} must be finite, got {value}"
        )))
    }
}

fn reject_non_finite_f32(label: &str, value: f32) -> Result<(), crate::protocol::EncodeError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(crate::protocol::EncodeError::InvalidAst(format!(
            "{label} must be finite, got {value}"
        )))
    }
}

fn push_identifier_ref(buf: &mut BytesMut, ident: &str, allow_star: bool) {
    if allow_star && ident == "*" {
        buf.extend_from_slice(b"*");
    } else {
        buf.extend_from_slice(escape_identifier(ident).as_bytes());
    }
}

fn encode_text_search_vector(
    expr: &Expr,
    buf: &mut BytesMut,
) -> Result<(), crate::protocol::EncodeError> {
    super::super::dml::validate_text_search_columns("text search column", expr)?;

    let Expr::Named(columns) = expr else {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "text search left side must be a comma-separated identifier list".to_string(),
        ));
    };

    for (i, raw_column) in columns.split(',').enumerate() {
        if i > 0 {
            buf.extend_from_slice(b" || ' ' || ");
        }
        buf.extend_from_slice(b"coalesce(");
        push_identifier_ref(buf, raw_column.trim(), false);
        buf.extend_from_slice(b",'')");
    }

    Ok(())
}

fn encode_json_path_segment(
    key: &str,
    buf: &mut BytesMut,
) -> Result<(), crate::protocol::EncodeError> {
    if key.as_bytes().contains(&0) {
        return Err(crate::protocol::EncodeError::NullByte);
    }

    if key.contains('\'') {
        buf.extend_from_slice(key.replace('\'', "''").as_bytes());
    } else {
        buf.extend_from_slice(key.as_bytes());
    }

    Ok(())
}

/// Encode column list to buffer.
pub fn encode_columns(
    columns: &[Expr],
    buf: &mut BytesMut,
) -> Result<(), crate::protocol::EncodeError> {
    for column in columns {
        super::super::dml::validate_expr_ref("columns", column)?;
    }
    encode_columns_with_params(columns, buf, None)
}

/// Encode column list with shared params (for subquery param sharing).
pub fn encode_columns_with_params(
    columns: &[Expr],
    buf: &mut BytesMut,
    params: Option<&mut Vec<Option<Vec<u8>>>>,
) -> Result<(), crate::protocol::EncodeError> {
    for column in columns {
        super::super::dml::validate_expr_ref("columns", column)?;
    }

    if columns.is_empty() {
        buf.extend_from_slice(b"*");
        return Ok(());
    }

    // We need to reborrow params for each iteration
    let mut params_opt = params;
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        encode_column_expr_inner(col, buf, params_opt.as_deref_mut())?;
    }
    Ok(())
}

/// Encode a single column expression (supports complex expressions).
pub fn encode_column_expr(
    col: &Expr,
    buf: &mut BytesMut,
) -> Result<(), crate::protocol::EncodeError> {
    super::super::dml::validate_expr_ref("column", col)?;
    encode_column_expr_inner(col, buf, None)
}

/// Encode a single column expression with optional shared params.
///
/// When `params` is `Some`, subqueries share the outer query's parameter
/// buffer so that `$1, $2, ...` numbering is continuous.
fn encode_column_expr_inner(
    col: &Expr,
    buf: &mut BytesMut,
    mut params: Option<&mut Vec<Option<Vec<u8>>>>,
) -> Result<(), crate::protocol::EncodeError> {
    match col {
        Expr::Star => buf.extend_from_slice(b"*"),
        Expr::Named(name) => push_identifier_ref(buf, name, true),
        Expr::Aliased { name, alias } => {
            push_identifier_ref(buf, name, true);
            buf.extend_from_slice(b" AS ");
            push_identifier_ref(buf, alias, false);
        }
        Expr::Aggregate {
            col,
            func,
            distinct,
            filter,
            alias,
        } => {
            buf.extend_from_slice(func.to_string().as_bytes());
            buf.extend_from_slice(b"(");
            if *distinct {
                buf.extend_from_slice(b"DISTINCT ");
            }
            push_identifier_ref(buf, col, true);
            buf.extend_from_slice(b")");

            // FILTER (WHERE ...) clause for aggregates
            if let Some(conditions) = filter
                && !conditions.is_empty()
            {
                buf.extend_from_slice(b" FILTER (WHERE ");
                for (i, cond) in conditions.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b" AND ");
                    }
                    let mode = match params.as_deref_mut() {
                        Some(params) => OperandMode::Bind(params),
                        None => OperandMode::Literal(None),
                    };
                    encode_condition(cond, buf, mode)?;
                }
                buf.extend_from_slice(b")");
            }

            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::FunctionCall { name, args, alias } => {
            buf.extend_from_slice(name.to_uppercase().as_bytes());
            buf.extend_from_slice(b"(");
            for (i, arg) in args.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_column_expr_inner(arg, buf, params.as_deref_mut())?;
            }
            buf.extend_from_slice(b")");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::Cast {
            expr,
            target_type,
            alias,
        } => {
            encode_column_expr_inner(expr, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b"::");
            buf.extend_from_slice(target_type.as_bytes());
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::Binary {
            left,
            op,
            right,
            alias,
        } => {
            buf.extend_from_slice(b"(");
            encode_column_expr_inner(left, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(op.to_string().as_bytes());
            // IS NULL / IS TRUE / ... carry a placeholder right operand.
            if !op.is_postfix() {
                buf.extend_from_slice(b" ");
                encode_column_expr_inner(right, buf, params.as_deref_mut())?;
            }
            buf.extend_from_slice(b")");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::Literal(val) => {
            encode_literal_operand(val, buf, params.as_deref_mut())?;
        }
        Expr::Case {
            when_clauses,
            else_value,
            alias,
        } => {
            buf.extend_from_slice(b"CASE");
            for (cond, then_expr) in when_clauses {
                buf.extend_from_slice(b" WHEN ");
                encode_condition(cond, buf, OperandMode::Literal(params.as_deref_mut()))?;
                buf.extend_from_slice(b" THEN ");
                encode_column_expr_inner(then_expr, buf, params.as_deref_mut())?;
            }
            if let Some(else_val) = else_value {
                buf.extend_from_slice(b" ELSE ");
                encode_column_expr_inner(else_val, buf, params.as_deref_mut())?;
            }
            buf.extend_from_slice(b" END");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::SpecialFunction { name, args, alias } => {
            if name.eq_ignore_ascii_case("INTERVAL") {
                buf.extend_from_slice(b"INTERVAL ");
                for (_kw, expr) in args {
                    encode_column_expr_inner(expr, buf, params.as_deref_mut())?;
                }
            } else {
                buf.extend_from_slice(name.to_uppercase().as_bytes());
                buf.extend_from_slice(b"(");
                for (i, (keyword, expr)) in args.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b" ");
                    }
                    if let Some(kw) = keyword {
                        buf.extend_from_slice(kw.as_bytes());
                        buf.extend_from_slice(b" ");
                    }
                    encode_column_expr_inner(expr, buf, params.as_deref_mut())?;
                }
                buf.extend_from_slice(b")");
            }
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::JsonAccess {
            column,
            path_segments,
            alias,
        } => {
            // Wrap in parentheses to avoid operator precedence issues with || (concat)
            buf.extend_from_slice(b"(");
            push_identifier_ref(buf, column, false);
            for (key, as_text) in path_segments {
                // Check if key is an integer (array index)
                let is_integer = key.parse::<i64>().is_ok();

                if *as_text {
                    if is_integer {
                        buf.extend_from_slice(b"->>");
                        buf.extend_from_slice(key.as_bytes());
                    } else {
                        buf.extend_from_slice(b"->>'");
                        encode_json_path_segment(key, buf)?;
                        buf.extend_from_slice(b"'");
                    }
                } else if is_integer {
                    buf.extend_from_slice(b"->");
                    buf.extend_from_slice(key.as_bytes());
                } else {
                    buf.extend_from_slice(b"->'");
                    encode_json_path_segment(key, buf)?;
                    buf.extend_from_slice(b"'");
                }
            }
            buf.extend_from_slice(b")");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::Window {
            name,
            func,
            params: window_params,
            partition,
            order,
            frame,
        } => {
            buf.extend_from_slice(func.to_uppercase().as_bytes());
            buf.extend_from_slice(b"(");
            for (i, p) in window_params.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_column_expr_inner(p, buf, params.as_deref_mut())?;
            }
            buf.extend_from_slice(b") OVER (");
            if !partition.is_empty() {
                buf.extend_from_slice(b"PARTITION BY ");
                for (i, col) in partition.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    push_identifier_ref(buf, col, false);
                }
            }
            if !order.is_empty() {
                if !partition.is_empty() {
                    buf.extend_from_slice(b" ");
                }
                buf.extend_from_slice(b"ORDER BY ");
                for (i, cage) in order.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    if let Some(cond) = cage.conditions.first() {
                        encode_column_expr_inner(&cond.left, buf, params.as_deref_mut())?;
                    }
                    if let CageKind::Sort(sort) = &cage.kind {
                        match sort {
                            SortOrder::Asc => buf.extend_from_slice(b" ASC"),
                            SortOrder::Desc => buf.extend_from_slice(b" DESC"),
                            SortOrder::AscNullsFirst => buf.extend_from_slice(b" ASC NULLS FIRST"),
                            SortOrder::AscNullsLast => buf.extend_from_slice(b" ASC NULLS LAST"),
                            SortOrder::DescNullsFirst => {
                                buf.extend_from_slice(b" DESC NULLS FIRST")
                            }
                            SortOrder::DescNullsLast => buf.extend_from_slice(b" DESC NULLS LAST"),
                        }
                    }
                }
            }
            // FRAME clause (ROWS/RANGE BETWEEN ... AND ...)
            if let Some(f) = frame {
                buf.extend_from_slice(b" ");
                encode_window_frame(f, buf);
            }
            buf.extend_from_slice(b")");
            if !name.is_empty() {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, name, false);
            }
        }
        Expr::ArrayConstructor { elements, alias } => {
            buf.extend_from_slice(b"ARRAY[");
            for (i, elem) in elements.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_column_expr_inner(elem, buf, params.as_deref_mut())?;
            }
            buf.extend_from_slice(b"]");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::RowConstructor { elements, alias } => {
            buf.extend_from_slice(b"ROW(");
            for (i, elem) in elements.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_column_expr_inner(elem, buf, params.as_deref_mut())?;
            }
            buf.extend_from_slice(b")");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::Subscript { expr, index, alias } => {
            encode_column_expr_inner(expr, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b"[");
            encode_column_expr_inner(index, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b"]");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::Collate {
            expr,
            collation,
            alias,
        } => {
            encode_column_expr_inner(expr, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b" COLLATE ");
            push_identifier_ref(buf, collation, false);
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::FieldAccess { expr, field, alias } => {
            buf.extend_from_slice(b"(");
            encode_column_expr_inner(expr, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b").");
            push_identifier_ref(buf, field, false);
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::Subquery { query, alias } => {
            // Encode scalar subquery: (SELECT ... LIMIT 1)
            // When params is available, share it so $N numbering is continuous.
            buf.extend_from_slice(b"(");
            match params {
                Some(ref mut p) => {
                    super::super::dml::encode_select(query, buf, p)?;
                }
                None => {
                    let mut sub_buf = BytesMut::with_capacity(128);
                    let mut sub_params: Vec<Option<Vec<u8>>> = Vec::new();
                    super::super::dml::encode_select(query, &mut sub_buf, &mut sub_params)?;
                    if !sub_params.is_empty() {
                        return Err(crate::protocol::EncodeError::InvalidAst(
                            "subquery expression requires a parameter context".to_string(),
                        ));
                    }
                    buf.extend_from_slice(&sub_buf);
                }
            }
            buf.extend_from_slice(b")");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::Exists {
            query,
            negated,
            alias,
        } => {
            // Encode EXISTS or NOT EXISTS subquery
            if *negated {
                buf.extend_from_slice(b"NOT ");
            }
            buf.extend_from_slice(b"EXISTS (");
            match params {
                Some(ref mut p) => {
                    super::super::dml::encode_select(query, buf, p)?;
                }
                None => {
                    let mut sub_buf = BytesMut::with_capacity(128);
                    let mut sub_params: Vec<Option<Vec<u8>>> = Vec::new();
                    super::super::dml::encode_select(query, &mut sub_buf, &mut sub_params)?;
                    if !sub_params.is_empty() {
                        return Err(crate::protocol::EncodeError::InvalidAst(
                            "exists expression requires a parameter context".to_string(),
                        ));
                    }
                    buf.extend_from_slice(&sub_buf);
                }
            }
            buf.extend_from_slice(b")");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                push_identifier_ref(buf, a, false);
            }
        }
        Expr::Def {
            name,
            data_type,
            constraints,
        } => {
            push_identifier_ref(buf, name, false);
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(data_type.to_uppercase().as_bytes());
            for c in constraints {
                match c {
                    Constraint::PrimaryKey => buf.extend_from_slice(b" PRIMARY KEY"),
                    Constraint::Unique => buf.extend_from_slice(b" UNIQUE"),
                    Constraint::Nullable => buf.extend_from_slice(b" NULL"),
                    Constraint::Default(val) => {
                        buf.extend_from_slice(b" DEFAULT ");
                        buf.extend_from_slice(val.as_bytes());
                    }
                    Constraint::Check(vals) => {
                        buf.extend_from_slice(b" CHECK (");
                        buf.extend_from_slice(vals.join(", ").as_bytes());
                        buf.extend_from_slice(b")");
                    }
                    Constraint::References(target) => {
                        buf.extend_from_slice(b" REFERENCES ");
                        buf.extend_from_slice(target.as_bytes());
                    }
                    _ => {
                        buf.extend_from_slice(b" ");
                        buf.extend_from_slice(c.to_string().as_bytes());
                    }
                }
            }
        }
        Expr::Mod { kind, col } => match kind {
            ModKind::Add => {
                buf.extend_from_slice(b"ADD COLUMN ");
                encode_column_expr_inner(col, buf, params)?;
            }
            ModKind::Drop => {
                buf.extend_from_slice(b"DROP COLUMN ");
                encode_column_expr_inner(col, buf, params)?;
            }
        },
    }
    Ok(())
}

/// Where a condition's right-hand operands go.
///
/// The predicate shape (arity, lists, ranges, function syntax) is written
/// once by [`encode_condition`]; contexts differ only in how operands are
/// written, so plain comparisons keep the SQL text each context had.
pub(crate) enum OperandMode<'a> {
    /// Every scalar is a bind parameter (WHERE, MERGE, FILTER in a projection).
    Bind(&'a mut Vec<Option<Vec<u8>>>),
    /// JOIN ON: columns stay identifiers, NULL/bool/integers are inlined,
    /// other scalars bind.
    Join(&'a mut Vec<Option<Vec<u8>>>),
    /// CASE WHEN, and FILTER without a parameter buffer: scalars are SQL
    /// literals; nested subqueries share the buffer when there is one.
    Literal(Option<&'a mut Vec<Option<Vec<u8>>>>),
}

impl OperandMode<'_> {
    fn params(&mut self) -> Option<&mut Vec<Option<Vec<u8>>>> {
        match self {
            OperandMode::Bind(params) | OperandMode::Join(params) => Some(&mut **params),
            OperandMode::Literal(params) => params.as_deref_mut(),
        }
    }

    fn operand(
        &mut self,
        value: &Value,
        buf: &mut BytesMut,
    ) -> Result<(), crate::protocol::EncodeError> {
        match self {
            OperandMode::Bind(params) => encode_value(value, buf, params),
            OperandMode::Join(params) => encode_join_value(value, buf, params),
            OperandMode::Literal(params) => {
                encode_literal_operand(value, buf, params.as_deref_mut())
            }
        }
    }
}

fn encode_literal_operand(
    value: &Value,
    buf: &mut BytesMut,
    mut params: Option<&mut Vec<Option<Vec<u8>>>>,
) -> Result<(), crate::protocol::EncodeError> {
    match value {
        Value::Expr(expr) => encode_column_expr_inner(expr, buf, params)?,
        Value::Column(column) => push_identifier_ref(buf, column, false),
        Value::Array(values) => {
            buf.extend_from_slice(b"(");
            for (i, value) in values.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_literal_operand(value, buf, params.as_deref_mut())?;
            }
            buf.extend_from_slice(b")");
        }
        Value::Subquery(query) => encode_subquery_operand(query, buf, params)?,
        _ => encode_inline_value(value, buf)?,
    }
    Ok(())
}

/// `(SELECT ...)`, sharing `params` so `$N` stays continuous. Without a
/// buffer a subquery that would bind is rejected rather than dropping binds.
fn encode_subquery_operand(
    query: &qail_core::ast::Qail,
    buf: &mut BytesMut,
    params: Option<&mut Vec<Option<Vec<u8>>>>,
) -> Result<(), crate::protocol::EncodeError> {
    buf.extend_from_slice(b"(");
    if let Some(params) = params {
        super::super::dml::encode_select(query, buf, params)?;
    } else {
        let mut sub_params = Vec::new();
        super::super::dml::encode_select(query, buf, &mut sub_params)?;
        if !sub_params.is_empty() {
            return Err(crate::protocol::EncodeError::InvalidAst(
                "subquery operand requires a parameter context".to_string(),
            ));
        }
    }
    buf.extend_from_slice(b")");
    Ok(())
}

/// Encode one condition. WHERE, CASE WHEN, JOIN ON, FILTER and MERGE all
/// come through here; see [`OperandMode`].
pub(crate) fn encode_condition(
    cond: &Condition,
    buf: &mut BytesMut,
    mut mode: OperandMode<'_>,
) -> Result<(), crate::protocol::EncodeError> {
    use crate::protocol::EncodeError;

    if cond.is_array_unnest {
        return encode_array_membership(cond, buf, mode);
    }

    match cond.op {
        Operator::Exists | Operator::NotExists => {
            // The left side is ignored: EXISTS is a standalone predicate.
            let Value::Subquery(query) = &cond.value else {
                return Err(EncodeError::InvalidAst(
                    "EXISTS condition requires a subquery value".to_string(),
                ));
            };
            if cond.op == Operator::NotExists {
                buf.extend_from_slice(b"NOT ");
            }
            buf.extend_from_slice(b"EXISTS ");
            return encode_subquery_operand(query, buf, mode.params());
        }
        Operator::TextSearch => {
            // The left side is a comma-separated column list.
            buf.extend_from_slice(b"to_tsvector('english', ");
            encode_text_search_vector(&cond.left, buf)?;
            buf.extend_from_slice(b") @@ websearch_to_tsquery('english', ");
            mode.operand(&cond.value, buf)?;
            buf.extend_from_slice(b")");
            return Ok(());
        }
        Operator::JsonExists | Operator::JsonQuery | Operator::JsonValue => {
            buf.extend_from_slice(cond.op.sql_symbol().as_bytes());
            buf.extend_from_slice(b"(");
            encode_ref_expr(&cond.left, buf, mode.params())?;
            buf.extend_from_slice(b", ");
            mode.operand(&cond.value, buf)?;
            buf.extend_from_slice(b")");
            if cond.op != Operator::JsonExists {
                buf.extend_from_slice(b" IS NOT NULL");
            }
            return Ok(());
        }
        Operator::ArrayElemContainedInText => {
            return Err(EncodeError::InvalidAst(
                "ArrayElemContainedInText requires is_array_unnest".to_string(),
            ));
        }
        _ => {}
    }

    encode_ref_expr(&cond.left, buf, mode.params())?;
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(cond.op.sql_symbol().as_bytes());

    if cond.op.is_postfix() {
        return Ok(());
    }

    match cond.op {
        Operator::In | Operator::NotIn => {
            buf.extend_from_slice(b" ");
            match &cond.value {
                Value::Array(values) if !values.is_empty() => {
                    buf.extend_from_slice(b"(");
                    for (i, value) in values.iter().enumerate() {
                        if i > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        mode.operand(value, buf)?;
                    }
                    buf.extend_from_slice(b")");
                }
                Value::Subquery(query) => encode_subquery_operand(query, buf, mode.params())?,
                _ => {
                    return Err(EncodeError::InvalidAst(
                        "IN condition requires a non-empty array or subquery value".to_string(),
                    ));
                }
            }
        }
        op if op.is_range() => {
            let Value::Array(values) = &cond.value else {
                return Err(EncodeError::InvalidAst(
                    "BETWEEN condition requires exactly two array values".to_string(),
                ));
            };
            let [low, high] = values.as_slice() else {
                return Err(EncodeError::InvalidAst(
                    "BETWEEN condition requires exactly two array values".to_string(),
                ));
            };
            buf.extend_from_slice(b" ");
            mode.operand(low, buf)?;
            buf.extend_from_slice(b" AND ");
            mode.operand(high, buf)?;
        }
        Operator::Fuzzy => {
            buf.extend_from_slice(b" '%' || ");
            mode.operand(&cond.value, buf)?;
            buf.extend_from_slice(b" || '%'");
        }
        _ => {
            buf.extend_from_slice(b" ");
            mode.operand(&cond.value, buf)?;
        }
    }
    Ok(())
}

/// `EXISTS (SELECT 1 FROM unnest(left) _el WHERE _el <op> value)`.
fn encode_array_membership(
    cond: &Condition,
    buf: &mut BytesMut,
    mut mode: OperandMode<'_>,
) -> Result<(), crate::protocol::EncodeError> {
    if !matches!(
        cond.op,
        Operator::Eq
            | Operator::Ne
            | Operator::Gt
            | Operator::Gte
            | Operator::Lt
            | Operator::Lte
            | Operator::Fuzzy
            | Operator::ArrayElemContainedInText
    ) {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "is_array_unnest supports comparisons, Fuzzy and ArrayElemContainedInText, got {:?}",
            cond.op
        )));
    }

    buf.extend_from_slice(b"EXISTS (SELECT 1 FROM unnest(");
    encode_ref_expr(&cond.left, buf, mode.params())?;
    buf.extend_from_slice(b") _el WHERE ");
    match cond.op {
        Operator::Fuzzy => {
            buf.extend_from_slice(b"_el ILIKE '%' || ");
            mode.operand(&cond.value, buf)?;
            buf.extend_from_slice(b" || '%'");
        }
        Operator::ArrayElemContainedInText => {
            buf.extend_from_slice(b"LOWER(");
            mode.operand(&cond.value, buf)?;
            buf.extend_from_slice(b") LIKE '%' || LOWER(_el) || '%'");
        }
        _ => {
            buf.extend_from_slice(b"_el ");
            buf.extend_from_slice(cond.op.sql_symbol().as_bytes());
            buf.extend_from_slice(b" ");
            mode.operand(&cond.value, buf)?;
        }
    }
    buf.extend_from_slice(b")");
    Ok(())
}

fn encode_inline_value(
    value: &Value,
    buf: &mut BytesMut,
) -> Result<(), crate::protocol::EncodeError> {
    match value {
        Value::String(value) | Value::Timestamp(value) => {
            if value.as_bytes().contains(&0) {
                return Err(crate::protocol::EncodeError::NullByte);
            }
            buf.extend_from_slice(b"'");
            buf.extend_from_slice(value.replace('\'', "''").as_bytes());
            buf.extend_from_slice(b"'");
        }
        Value::Json(value) => {
            if value.as_bytes().contains(&0) {
                return Err(crate::protocol::EncodeError::NullByte);
            }
            buf.extend_from_slice(b"'");
            buf.extend_from_slice(value.replace('\'', "''").as_bytes());
            buf.extend_from_slice(b"'::jsonb");
        }
        Value::Bool(value) => buf.extend_from_slice(if *value { b"TRUE" } else { b"FALSE" }),
        Value::Column(column) => push_identifier_ref(buf, column, false),
        Value::Expr(expr) => encode_column_expr(expr, buf)?,
        Value::Param(n) => {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "unresolved positional parameter ${n} cannot be encoded without a bind value"
            )));
        }
        Value::NamedParam(name) => {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "unresolved named parameter :{name} cannot be encoded by the PostgreSQL AST encoder"
            )));
        }
        Value::Function(function) => {
            if function.len() > 1024
                || function.contains(';')
                || function.contains("--")
                || function.contains("/*")
                || function.contains("*/")
            {
                return Err(crate::protocol::EncodeError::UnsafeExpression(format!(
                    "Value::Function rejected: suspicious content in '{}'",
                    &function[..function.len().min(80)]
                )));
            }
            buf.extend_from_slice(function.as_bytes());
        }
        Value::Subquery(query) => {
            let mut sub_params = Vec::new();
            buf.extend_from_slice(b"(");
            super::super::dml::encode_select(query, buf, &mut sub_params)?;
            if !sub_params.is_empty() {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "inline subquery value requires a parameter context".to_string(),
                ));
            }
            buf.extend_from_slice(b")");
        }
        Value::Array(values) => {
            buf.extend_from_slice(b"(");
            for (i, value) in values.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_inline_value(value, buf)?;
            }
            buf.extend_from_slice(b")");
        }
        Value::Float(value) => {
            reject_non_finite_f64("inline float value", *value)?;
            buf.extend_from_slice(value.to_string().as_bytes());
        }
        Value::Vector(values) => {
            buf.extend_from_slice(b"[");
            for (idx, value) in values.iter().enumerate() {
                reject_non_finite_f32("inline vector value", *value)?;
                if idx > 0 {
                    buf.extend_from_slice(b", ");
                }
                buf.extend_from_slice(value.to_string().as_bytes());
            }
            buf.extend_from_slice(b"]");
        }
        _ => buf.extend_from_slice(value.to_string().as_bytes()),
    }
    Ok(())
}

/// Encode simple expression (for WHERE left side).
pub fn encode_expr(expr: &Expr, buf: &mut BytesMut) -> Result<(), crate::protocol::EncodeError> {
    encode_ref_expr(expr, buf, None)
}

/// Encode an expression used as a reference (condition left side, ORDER BY,
/// GROUP BY, DISTINCT ON, assignment value): an alias is not rendered.
/// `params` lets bound subqueries share the statement's `$N` numbering.
pub fn encode_ref_expr(
    expr: &Expr,
    buf: &mut BytesMut,
    params: Option<&mut Vec<Option<Vec<u8>>>>,
) -> Result<(), crate::protocol::EncodeError> {
    match expr {
        Expr::Named(name) => push_identifier_ref(buf, name, true),
        Expr::Star => buf.extend_from_slice(b"*"),
        Expr::Aliased { name, .. } => push_identifier_ref(buf, name, true),
        _ => {
            super::super::dml::validate_expr_ref("column", expr)?;
            encode_column_expr_inner(expr, buf, params)?;
        }
    }
    Ok(())
}

/// Encode an expression while sharing the caller's parameter buffer.
pub fn encode_expr_with_params(
    expr: &Expr,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    super::super::dml::validate_expr_ref("expression", expr)?;
    encode_column_expr_inner(expr, buf, Some(params))
}

/// Encode JOIN ON value - AST-native, no allocations for column references.
///
/// Only `Value::Column` emits an identifier; a `Value::String` binds as a
/// parameter even when it contains a dot (`'red.blue'`, `'$.a'`).
pub fn encode_join_value(
    value: &Value,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    match value {
        Value::Column(col) => push_identifier_ref(buf, col, false),
        Value::Null => buf.extend_from_slice(b"NULL"),
        Value::Bool(b) => buf.extend_from_slice(if *b { b"TRUE" } else { b"FALSE" }),
        Value::Int(n) => {
            if (0..100).contains(n) {
                buf.extend_from_slice(NUMERIC_VALUES[*n as usize]);
            } else {
                buf.extend_from_slice(n.to_string().as_bytes());
            }
        }
        _ => encode_value(value, buf, params)?,
    }
    Ok(())
}

/// Encode WHERE conditions with parameter extraction.
pub fn encode_conditions(
    conditions: &[Condition],
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    for (i, cond) in conditions.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b" AND ");
        }

        encode_condition(cond, buf, OperandMode::Bind(params))?;
    }
    Ok(())
}

/// Encode a value — extract to a bind parameter or inline as a literal.
///
/// Returns `Err` if the value contains invalid data (e.g., NULL byte in string).
///
/// # Arguments
///
/// * `value` — AST value to encode.
/// * `buf` — Output buffer to append the SQL fragment to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_value(
    value: &Value,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    use crate::protocol::EncodeError;

    match value {
        Value::Null => {
            params.push(None);
            write_param_placeholder(buf, params.len());
        }
        Value::String(s) => {
            // Reject literal NULL bytes - they corrupt PostgreSQL connection state
            if s.as_bytes().contains(&0) {
                return Err(EncodeError::NullByte);
            }
            params.push(Some(s.as_bytes().to_vec()));
            write_param_placeholder(buf, params.len());
        }
        Value::Int(n) => {
            params.push(Some(i64_to_bytes(*n)));
            write_param_placeholder(buf, params.len());
        }
        Value::Float(f) => {
            reject_non_finite_f64("float parameter", *f)?;
            params.push(Some(f.to_string().into_bytes()));
            write_param_placeholder(buf, params.len());
        }
        Value::Bool(b) => {
            params.push(Some(if *b { b"t".to_vec() } else { b"f".to_vec() }));
            write_param_placeholder(buf, params.len());
        }
        Value::Param(n) => {
            return Err(EncodeError::InvalidAst(format!(
                "unresolved positional parameter ${n} cannot be encoded without a bind value"
            )));
        }
        Value::NamedParam(name) => {
            return Err(EncodeError::InvalidAst(format!(
                "unresolved named parameter :{name} cannot be encoded by the PostgreSQL AST encoder"
            )));
        }
        Value::Uuid(uuid) => {
            let bytes = uuid.as_bytes();
            let mut uuid_buf = Vec::with_capacity(36);
            for (i, byte) in bytes.iter().enumerate() {
                if i == 4 || i == 6 || i == 8 || i == 10 {
                    uuid_buf.push(b'-');
                }
                let hi = byte >> 4;
                let lo = byte & 0x0f;
                uuid_buf.push(if hi < 10 { b'0' + hi } else { b'a' + hi - 10 });
                uuid_buf.push(if lo < 10 { b'0' + lo } else { b'a' + lo - 10 });
            }
            params.push(Some(uuid_buf));
            write_param_placeholder(buf, params.len());
        }
        Value::Array(arr) => {
            let mut arr_buf = Vec::with_capacity(arr.len() * 8 + 2);
            arr_buf.push(b'{');
            for (i, v) in arr.iter().enumerate() {
                if i > 0 {
                    arr_buf.push(b',');
                }
                write_value_to_array(&mut arr_buf, v)?;
            }
            arr_buf.push(b'}');
            params.push(Some(arr_buf));
            write_param_placeholder(buf, params.len());
        }
        Value::Function(f) => {
            // R9: Reject injection markers in function expressions.
            // The parser generates safe values like "NOW() - INTERVAL '24 hours'",
            // but guard against programmatic misuse.
            if f.len() > 1024
                || f.contains(';')
                || f.contains("--")
                || f.contains("/*")
                || f.contains("*/")
            {
                return Err(super::super::EncodeError::UnsafeExpression(format!(
                    "Value::Function rejected: suspicious content in '{}'",
                    &f[..f.len().min(80)]
                )));
            }
            buf.extend_from_slice(f.as_bytes());
        }
        Value::Column(col) => {
            push_identifier_ref(buf, col, false);
        }
        Value::Subquery(q) => {
            buf.extend_from_slice(b"(");
            super::super::dml::encode_select(q, buf, params)?;
            buf.extend_from_slice(b")");
        }
        Value::Timestamp(ts) => {
            params.push(Some(ts.as_bytes().to_vec()));
            write_param_placeholder(buf, params.len());
        }
        Value::Interval { amount, unit } => {
            let mut interval_buf = Vec::with_capacity(16);
            interval_buf.extend_from_slice(amount.to_string().as_bytes());
            interval_buf.push(b' ');
            interval_buf.extend_from_slice(unit.to_string().as_bytes());
            params.push(Some(interval_buf));
            write_param_placeholder(buf, params.len());
        }
        Value::NullUuid => {
            params.push(None);
            write_param_placeholder(buf, params.len());
        }
        Value::Bytes(bytes) => {
            params.push(Some(bytes.clone()));
            write_param_placeholder(buf, params.len());
        }
        Value::Expr(expr) => {
            encode_column_expr_inner(expr, buf, Some(params))?;
        }
        Value::Vector(vec) => {
            // Encode vector as PostgreSQL array format: '{1.0,2.0,3.0}'
            let mut arr_buf = Vec::with_capacity(vec.len() * 12 + 2);
            arr_buf.push(b'{');
            for (i, v) in vec.iter().enumerate() {
                reject_non_finite_f32("vector parameter", *v)?;
                if i > 0 {
                    arr_buf.push(b',');
                }
                arr_buf.extend_from_slice(v.to_string().as_bytes());
            }
            arr_buf.push(b'}');
            params.push(Some(arr_buf));
            write_param_placeholder(buf, params.len());
        }
        Value::Json(json) => {
            // JSONB: encode as text parameter with escaping
            params.push(Some(json.as_bytes().to_vec()));
            write_param_placeholder(buf, params.len());
        }
    }
    Ok(())
}

/// Write a scalar data value into a PostgreSQL array text parameter.
pub fn write_value_to_array(
    buf: &mut Vec<u8>,
    value: &Value,
) -> Result<(), crate::protocol::EncodeError> {
    use crate::protocol::EncodeError;

    match value {
        Value::Int(n) => {
            if (0..100).contains(n) {
                buf.extend_from_slice(NUMERIC_VALUES[*n as usize]);
            } else {
                buf.extend_from_slice(n.to_string().as_bytes());
            }
        }
        Value::String(s) | Value::Timestamp(s) | Value::Json(s) => {
            write_quoted_array_element(buf, s)?
        }
        Value::Bool(b) => buf.extend_from_slice(if *b { b"t" } else { b"f" }),
        Value::Null | Value::NullUuid => buf.extend_from_slice(b"NULL"),
        Value::Float(f) => {
            reject_non_finite_f64("array float value", *f)?;
            buf.extend_from_slice(f.to_string().as_bytes());
        }
        Value::Uuid(uuid) => buf.extend_from_slice(uuid.to_string().as_bytes()),
        Value::Interval { amount, unit } => {
            write_quoted_array_element(buf, &format!("{amount} {unit}"))?;
        }
        Value::Param(n) => {
            return Err(EncodeError::InvalidAst(format!(
                "unresolved positional parameter ${n} cannot be encoded inside array data"
            )));
        }
        Value::NamedParam(name) => {
            return Err(EncodeError::InvalidAst(format!(
                "unresolved named parameter :{name} cannot be encoded inside array data"
            )));
        }
        Value::Function(_)
        | Value::Array(_)
        | Value::Subquery(_)
        | Value::Column(_)
        | Value::Bytes(_)
        | Value::Expr(_)
        | Value::Vector(_) => {
            return Err(EncodeError::InvalidAst(format!(
                "unsupported array element value: {value:?}"
            )));
        }
    }
    Ok(())
}

fn write_quoted_array_element(
    buf: &mut Vec<u8>,
    value: &str,
) -> Result<(), crate::protocol::EncodeError> {
    if value.as_bytes().contains(&0) {
        return Err(crate::protocol::EncodeError::NullByte);
    }

    buf.push(b'"');
    for byte in value.bytes() {
        if byte == b'"' || byte == b'\\' {
            buf.push(b'\\');
        }
        buf.push(byte);
    }
    buf.push(b'"');
    Ok(())
}

/// Encode window frame (ROWS/RANGE BETWEEN ... AND ...)
fn encode_window_frame(frame: &WindowFrame, buf: &mut BytesMut) {
    match frame {
        WindowFrame::Rows { start, end } => {
            buf.extend_from_slice(b"ROWS BETWEEN ");
            encode_frame_bound(start, buf);
            buf.extend_from_slice(b" AND ");
            encode_frame_bound(end, buf);
        }
        WindowFrame::Range { start, end } => {
            buf.extend_from_slice(b"RANGE BETWEEN ");
            encode_frame_bound(start, buf);
            buf.extend_from_slice(b" AND ");
            encode_frame_bound(end, buf);
        }
    }
}

/// Encode a single frame bound
fn encode_frame_bound(bound: &FrameBound, buf: &mut BytesMut) {
    match bound {
        FrameBound::UnboundedPreceding => buf.extend_from_slice(b"UNBOUNDED PRECEDING"),
        FrameBound::Preceding(n) => {
            buf.extend_from_slice(n.to_string().as_bytes());
            buf.extend_from_slice(b" PRECEDING");
        }
        FrameBound::CurrentRow => buf.extend_from_slice(b"CURRENT ROW"),
        FrameBound::Following(n) => {
            buf.extend_from_slice(n.to_string().as_bytes());
            buf.extend_from_slice(b" FOLLOWING");
        }
        FrameBound::UnboundedFollowing => buf.extend_from_slice(b"UNBOUNDED FOLLOWING"),
    }
}

#[cfg(test)]
mod tests {
    use super::{encode_conditions, encode_value};
    use bytes::BytesMut;
    use qail_core::ast::{Condition, Expr, Operator, Qail, Value};
    use uuid::Uuid;

    #[test]
    fn encode_uuid_array_parameter_uses_raw_uuid_tokens() {
        let uuid = Uuid::parse_str("b0e72b4f-c883-42ce-a5a9-96de097d6c54").unwrap();
        let value = Value::Array(vec![Value::Uuid(uuid)]);
        let mut sql = BytesMut::new();
        let mut params = Vec::new();

        encode_value(&value, &mut sql, &mut params).unwrap();

        assert_eq!(sql.as_ref(), b"$1");
        assert_eq!(params.len(), 1);
        assert_eq!(
            params[0].as_deref(),
            Some(b"{b0e72b4f-c883-42ce-a5a9-96de097d6c54}".as_slice())
        );
    }

    #[test]
    fn encode_array_string_parameter_escapes_backslashes_and_quotes() {
        let value = Value::Array(vec![Value::String("a\\b\"c".to_string())]);
        let mut sql = BytesMut::new();
        let mut params = Vec::new();

        encode_value(&value, &mut sql, &mut params).unwrap();

        assert_eq!(sql.as_ref(), b"$1");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].as_deref(), Some(br#"{"a\\b\"c"}"#.as_slice()));
    }

    #[test]
    fn encode_array_string_parameter_rejects_null_bytes() {
        let value = Value::Array(vec![Value::String("bad\0value".to_string())]);
        let mut sql = BytesMut::new();
        let mut params = Vec::new();

        let err = encode_value(&value, &mut sql, &mut params).unwrap_err();

        assert_eq!(err, crate::protocol::EncodeError::NullByte);
        assert!(params.is_empty());
    }

    #[test]
    fn encode_float_parameter_rejects_non_finite_values() {
        let value = Value::Float(f64::NAN);
        let mut sql = BytesMut::new();
        let mut params = Vec::new();

        let err = encode_value(&value, &mut sql, &mut params).unwrap_err();

        assert!(
            matches!(err, crate::protocol::EncodeError::InvalidAst(ref message) if message.contains("must be finite")),
            "{err}"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn encode_vector_parameter_rejects_non_finite_values() {
        let value = Value::Vector(vec![1.0, f32::INFINITY]);
        let mut sql = BytesMut::new();
        let mut params = Vec::new();

        let err = encode_value(&value, &mut sql, &mut params).unwrap_err();

        assert!(
            matches!(err, crate::protocol::EncodeError::InvalidAst(ref message) if message.contains("must be finite")),
            "{err}"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn encode_array_parameter_rejects_expression_only_values() {
        let value = Value::Array(vec![Value::NamedParam("tenant_id".to_string())]);
        let mut sql = BytesMut::new();
        let mut params = Vec::new();

        let err = encode_value(&value, &mut sql, &mut params).unwrap_err();

        assert!(
            matches!(err, crate::protocol::EncodeError::InvalidAst(ref message) if message.contains("unresolved named parameter :tenant_id")),
            "{err}"
        );
        assert!(params.is_empty());
    }

    #[test]
    fn encode_value_subquery_appends_to_outer_params() {
        let subquery =
            Qail::get("users")
                .column("id")
                .filter("tenant_id", Operator::Eq, "tenant-b");
        let mut sql = BytesMut::new();
        let mut params = vec![Some(b"tenant-a".to_vec())];

        encode_value(&Value::Subquery(Box::new(subquery)), &mut sql, &mut params).unwrap();

        let sql = String::from_utf8(sql.to_vec()).unwrap();
        assert!(
            sql.contains("$2"),
            "subquery should continue outer placeholder numbering: {sql}"
        );
        assert_eq!(params.len(), 2);
        assert_eq!(params[1].as_deref(), Some(b"tenant-b".as_slice()));
    }

    #[test]
    fn encode_exists_subquery_appends_to_outer_params() {
        let subquery =
            Qail::get("users")
                .column("id")
                .filter("tenant_id", Operator::Eq, "tenant-b");
        let cond = Condition {
            left: Expr::Named("ignored".to_string()),
            op: Operator::Exists,
            value: Value::Subquery(Box::new(subquery)),
            is_array_unnest: false,
        };
        let mut sql = BytesMut::new();
        let mut params = vec![Some(b"tenant-a".to_vec())];

        encode_conditions(&[cond], &mut sql, &mut params).unwrap();

        let sql = String::from_utf8(sql.to_vec()).unwrap();
        assert!(sql.contains("EXISTS ("), "expected EXISTS SQL: {sql}");
        assert!(
            sql.contains("$2"),
            "EXISTS subquery should continue outer placeholder numbering: {sql}"
        );
        assert_eq!(params.len(), 2);
        assert_eq!(params[1].as_deref(), Some(b"tenant-b".as_slice()));
    }

    #[test]
    fn encode_exists_ignores_left_expr_without_truncation_panic() {
        let subquery = Qail::get("users").column("id");
        let cond = Condition {
            left: Expr::Aliased {
                name: "ignored".to_string(),
                alias: "display_is_longer_than_encoded".to_string(),
            },
            op: Operator::Exists,
            value: Value::Subquery(Box::new(subquery)),
            is_array_unnest: false,
        };
        let mut sql = BytesMut::new();
        let mut params = Vec::new();

        encode_conditions(&[cond], &mut sql, &mut params).unwrap();

        let sql = String::from_utf8(sql.to_vec()).unwrap();
        assert!(
            sql.starts_with("EXISTS ("),
            "expected clean EXISTS SQL: {sql}"
        );
        assert!(
            !sql.contains("ignored"),
            "EXISTS left expression should not leak into SQL: {sql}"
        );
    }
}
