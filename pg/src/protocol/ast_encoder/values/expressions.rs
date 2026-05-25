//! Value and expression encoding.
//!
//! Functions for encoding Expr, Value, Operator, and conditions to wire format.

use bytes::BytesMut;
use qail_core::ast::{
    CageKind, Condition, Constraint, Expr, FrameBound, ModKind, Operator, SortOrder, Value,
    WindowFrame,
};

use super::super::helpers::{NUMERIC_VALUES, i64_to_bytes, write_param_placeholder};

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
    encode_columns_with_params(columns, buf, None)
}

/// Encode column list with shared params (for subquery param sharing).
pub fn encode_columns_with_params(
    columns: &[Expr],
    buf: &mut BytesMut,
    params: Option<&mut Vec<Option<Vec<u8>>>>,
) -> Result<(), crate::protocol::EncodeError> {
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
        Expr::Named(name) => buf.extend_from_slice(name.as_bytes()),
        Expr::Aliased { name, alias } => {
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(b" AS ");
            buf.extend_from_slice(alias.as_bytes());
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
            buf.extend_from_slice(col.as_bytes());
            buf.extend_from_slice(b")");

            // FILTER (WHERE ...) clause for aggregates
            if let Some(conditions) = filter
                && !conditions.is_empty()
            {
                buf.extend_from_slice(b" FILTER (WHERE ");
                if let Some(params) = params.as_deref_mut() {
                    encode_conditions(conditions, buf, params)?;
                } else {
                    encode_conditions_inline(conditions, buf)?;
                }
                buf.extend_from_slice(b")");
            }

            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(a.as_bytes());
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
                buf.extend_from_slice(a.as_bytes());
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
                buf.extend_from_slice(a.as_bytes());
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
            buf.extend_from_slice(b" ");
            encode_column_expr_inner(right, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b")");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::Literal(val) => {
            encode_inline_value(val, buf)?;
        }
        Expr::Case {
            when_clauses,
            else_value,
            alias,
        } => {
            buf.extend_from_slice(b"CASE");
            for (cond, then_expr) in when_clauses {
                buf.extend_from_slice(b" WHEN ");
                encode_column_expr_inner(&cond.left, buf, params.as_deref_mut())?;
                buf.extend_from_slice(b" ");
                encode_operator(&cond.op, buf);
                if !matches!(cond.op, Operator::IsNull | Operator::IsNotNull) {
                    buf.extend_from_slice(b" ");
                    encode_case_condition_value(&cond.value, buf, params.as_deref_mut())?;
                }
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
                buf.extend_from_slice(a.as_bytes());
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
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::JsonAccess {
            column,
            path_segments,
            alias,
        } => {
            // Wrap in parentheses to avoid operator precedence issues with || (concat)
            buf.extend_from_slice(b"(");
            buf.extend_from_slice(column.as_bytes());
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
                buf.extend_from_slice(a.as_bytes());
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
                    buf.extend_from_slice(col.as_bytes());
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
                        buf.extend_from_slice(cond.left.to_string().as_bytes());
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
                buf.extend_from_slice(name.as_bytes());
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
                buf.extend_from_slice(a.as_bytes());
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
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::Subscript { expr, index, alias } => {
            encode_column_expr_inner(expr, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b"[");
            encode_column_expr_inner(index, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b"]");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::Collate {
            expr,
            collation,
            alias,
        } => {
            encode_column_expr_inner(expr, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b" COLLATE \"");
            buf.extend_from_slice(collation.as_bytes());
            buf.extend_from_slice(b"\"");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::FieldAccess { expr, field, alias } => {
            buf.extend_from_slice(b"(");
            encode_column_expr_inner(expr, buf, params.as_deref_mut())?;
            buf.extend_from_slice(b").");
            buf.extend_from_slice(field.as_bytes());
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(a.as_bytes());
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
                buf.extend_from_slice(a.as_bytes());
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
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::Def {
            name,
            data_type,
            constraints,
        } => {
            buf.extend_from_slice(name.as_bytes());
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

/// Encode an operator to bytes.
pub fn encode_operator(op: &Operator, buf: &mut BytesMut) {
    let bytes: &[u8] = match op {
        Operator::Eq => b"=",
        Operator::Ne => b"!=",
        Operator::Gt => b">",
        Operator::Gte => b">=",
        Operator::Lt => b"<",
        Operator::Lte => b"<=",
        Operator::Like => b"LIKE",
        Operator::NotLike => b"NOT LIKE",
        Operator::ILike => b"ILIKE",
        Operator::NotILike => b"NOT ILIKE",
        Operator::Fuzzy => b"ILIKE",
        Operator::In => b"IN",
        Operator::NotIn => b"NOT IN",
        Operator::IsNull => b"IS NULL",
        Operator::IsNotNull => b"IS NOT NULL",
        Operator::Between => b"BETWEEN",
        Operator::NotBetween => b"NOT BETWEEN",
        Operator::Regex => b"~",
        Operator::RegexI => b"~*",
        Operator::SimilarTo => b"SIMILAR TO",
        Operator::Contains => b"@>",
        Operator::ContainedBy => b"<@",
        Operator::Overlaps => b"&&",
        Operator::KeyExists => b"?",
        Operator::JsonExists => b"JSON_EXISTS",
        Operator::JsonQuery => b"JSON_QUERY",
        Operator::JsonValue => b"JSON_VALUE",
        Operator::Exists => b"EXISTS",
        Operator::NotExists => b"NOT EXISTS",
        Operator::TextSearch => b"@@",
        Operator::KeyExistsAny => b"?|",
        Operator::KeyExistsAll => b"?&",
        Operator::JsonPath => b"#>",
        Operator::JsonPathText => b"#>>",
        Operator::ArrayElemContainedInText => b"CONTAINS_ANY_TOKEN",
    };
    buf.extend_from_slice(bytes);
}

fn encode_case_condition_value(
    value: &Value,
    buf: &mut BytesMut,
    params: Option<&mut Vec<Option<Vec<u8>>>>,
) -> Result<(), crate::protocol::EncodeError> {
    let mut params = params;
    match value {
        Value::Expr(expr) => encode_column_expr_inner(expr, buf, params.as_deref_mut())?,
        Value::Column(column) => buf.extend_from_slice(column.as_bytes()),
        Value::Array(values) => {
            buf.extend_from_slice(b"(");
            for (i, value) in values.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_case_condition_value(value, buf, params.as_deref_mut())?;
            }
            buf.extend_from_slice(b")");
        }
        Value::Subquery(query) => {
            buf.extend_from_slice(b"(");
            if let Some(params) = params.as_deref_mut() {
                super::super::dml::encode_select(query, buf, params)?;
            } else {
                let mut sub_params = Vec::new();
                super::super::dml::encode_select(query, buf, &mut sub_params)?;
                if !sub_params.is_empty() {
                    return Err(crate::protocol::EncodeError::InvalidAst(
                        "CASE condition subquery requires a parameter context".to_string(),
                    ));
                }
            }
            buf.extend_from_slice(b")");
        }
        _ => encode_inline_value(value, buf)?,
    }
    Ok(())
}

fn encode_conditions_inline(
    conditions: &[Condition],
    buf: &mut BytesMut,
) -> Result<(), crate::protocol::EncodeError> {
    for (i, cond) in conditions.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b" AND ");
        }

        if matches!(cond.op, Operator::Exists | Operator::NotExists) {
            if cond.op == Operator::NotExists {
                buf.extend_from_slice(b"NOT ");
            }
            buf.extend_from_slice(b"EXISTS (");
            if let Value::Subquery(query) = &cond.value {
                let mut sub_params = Vec::new();
                super::super::dml::encode_select(query, buf, &mut sub_params)?;
                if !sub_params.is_empty() {
                    return Err(crate::protocol::EncodeError::InvalidAst(
                        "inline EXISTS condition requires a parameter context".to_string(),
                    ));
                }
            } else {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "EXISTS condition requires a subquery value".to_string(),
                ));
            }
            buf.extend_from_slice(b")");
            continue;
        }

        encode_expr(&cond.left, buf)?;
        buf.extend_from_slice(b" ");
        encode_operator(&cond.op, buf);

        match cond.op {
            Operator::IsNull | Operator::IsNotNull => {}
            Operator::In | Operator::NotIn => {
                buf.extend_from_slice(b" ");
                if let Value::Array(values) = &cond.value {
                    if values.is_empty() {
                        return Err(crate::protocol::EncodeError::InvalidAst(
                            "IN condition requires a non-empty array or subquery value".to_string(),
                        ));
                    }
                    buf.extend_from_slice(b"(");
                    for (j, value) in values.iter().enumerate() {
                        if j > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        encode_inline_value(value, buf)?;
                    }
                    buf.extend_from_slice(b")");
                } else if matches!(&cond.value, Value::Subquery(_)) {
                    encode_inline_value(&cond.value, buf)?;
                } else {
                    return Err(crate::protocol::EncodeError::InvalidAst(
                        "IN condition requires a non-empty array or subquery value".to_string(),
                    ));
                }
            }
            Operator::Between | Operator::NotBetween => {
                if let Value::Array(values) = &cond.value
                    && values.len() == 2
                {
                    buf.extend_from_slice(b" ");
                    encode_inline_value(&values[0], buf)?;
                    buf.extend_from_slice(b" AND ");
                    encode_inline_value(&values[1], buf)?;
                } else {
                    return Err(crate::protocol::EncodeError::InvalidAst(
                        "BETWEEN condition requires exactly two array values".to_string(),
                    ));
                }
            }
            _ => {
                buf.extend_from_slice(b" ");
                encode_inline_value(&cond.value, buf)?;
            }
        }
    }
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
        Value::Column(column) => buf.extend_from_slice(column.as_bytes()),
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
        _ => buf.extend_from_slice(value.to_string().as_bytes()),
    }
    Ok(())
}

/// Encode simple expression (for WHERE left side).
pub fn encode_expr(expr: &Expr, buf: &mut BytesMut) -> Result<(), crate::protocol::EncodeError> {
    match expr {
        Expr::Named(name) => buf.extend_from_slice(name.as_bytes()),
        Expr::Star => buf.extend_from_slice(b"*"),
        Expr::Aliased { name, .. } => buf.extend_from_slice(name.as_bytes()),
        // Delegate complex expressions to the full encoder
        _ => encode_column_expr(expr, buf)?,
    }
    Ok(())
}

/// Encode JOIN ON value - AST-native, no allocations for column references.
pub fn encode_join_value(
    value: &Value,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    match value {
        Value::Column(col) => buf.extend_from_slice(col.as_bytes()),
        Value::String(s) if s.contains('.') => buf.extend_from_slice(s.as_bytes()),
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

        if cond.is_array_unnest {
            buf.extend_from_slice(b"EXISTS (SELECT 1 FROM unnest(");
            encode_expr(&cond.left, buf)?;
            buf.extend_from_slice(b") _el WHERE ");

            match cond.op {
                Operator::Eq => {
                    buf.extend_from_slice(b"_el = ");
                    encode_value(&cond.value, buf, params)?;
                }
                Operator::Ne => {
                    buf.extend_from_slice(b"_el != ");
                    encode_value(&cond.value, buf, params)?;
                }
                Operator::Gt => {
                    buf.extend_from_slice(b"_el > ");
                    encode_value(&cond.value, buf, params)?;
                }
                Operator::Gte => {
                    buf.extend_from_slice(b"_el >= ");
                    encode_value(&cond.value, buf, params)?;
                }
                Operator::Lt => {
                    buf.extend_from_slice(b"_el < ");
                    encode_value(&cond.value, buf, params)?;
                }
                Operator::Lte => {
                    buf.extend_from_slice(b"_el <= ");
                    encode_value(&cond.value, buf, params)?;
                }
                Operator::Fuzzy => {
                    buf.extend_from_slice(b"_el ILIKE '%' || ");
                    encode_value(&cond.value, buf, params)?;
                    buf.extend_from_slice(b" || '%'");
                }
                Operator::ArrayElemContainedInText => {
                    buf.extend_from_slice(b"LOWER(");
                    encode_value(&cond.value, buf, params)?;
                    buf.extend_from_slice(b") LIKE '%' || LOWER(_el) || '%'");
                }
                _ => {
                    buf.extend_from_slice(b"_el = ");
                    encode_value(&cond.value, buf, params)?;
                }
            }

            buf.extend_from_slice(b")");
            continue;
        }

        let left_start = buf.len();
        encode_expr(&cond.left, buf)?;

        match cond.op {
            Operator::Eq => buf.extend_from_slice(b" = "),
            Operator::Ne => buf.extend_from_slice(b" != "),
            Operator::Gt => buf.extend_from_slice(b" > "),
            Operator::Gte => buf.extend_from_slice(b" >= "),
            Operator::Lt => buf.extend_from_slice(b" < "),
            Operator::Lte => buf.extend_from_slice(b" <= "),
            Operator::Like => buf.extend_from_slice(b" LIKE "),
            Operator::NotLike => buf.extend_from_slice(b" NOT LIKE "),
            Operator::ILike => buf.extend_from_slice(b" ILIKE "),
            Operator::NotILike => buf.extend_from_slice(b" NOT ILIKE "),
            Operator::In => {
                if let Value::Array(vals) = &cond.value {
                    if vals.is_empty() {
                        return Err(crate::protocol::EncodeError::InvalidAst(
                            "IN condition requires a non-empty array or subquery value".to_string(),
                        ));
                    }
                    buf.extend_from_slice(b" IN (");
                    for (j, v) in vals.iter().enumerate() {
                        if j > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        encode_value(v, buf, params)?;
                    }
                    buf.extend_from_slice(b")");
                    continue;
                } else if matches!(&cond.value, Value::Subquery(_)) {
                    buf.extend_from_slice(b" IN ");
                    encode_value(&cond.value, buf, params)?;
                    continue;
                }
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "IN condition requires a non-empty array or subquery value".to_string(),
                ));
            }
            Operator::NotIn => {
                if let Value::Array(vals) = &cond.value {
                    if vals.is_empty() {
                        return Err(crate::protocol::EncodeError::InvalidAst(
                            "IN condition requires a non-empty array or subquery value".to_string(),
                        ));
                    }
                    buf.extend_from_slice(b" NOT IN (");
                    for (j, v) in vals.iter().enumerate() {
                        if j > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        encode_value(v, buf, params)?;
                    }
                    buf.extend_from_slice(b")");
                    continue;
                } else if matches!(&cond.value, Value::Subquery(_)) {
                    buf.extend_from_slice(b" NOT IN ");
                    encode_value(&cond.value, buf, params)?;
                    continue;
                }
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "IN condition requires a non-empty array or subquery value".to_string(),
                ));
            }
            Operator::IsNull => {
                buf.extend_from_slice(b" IS NULL");
                continue;
            }
            Operator::IsNotNull => {
                buf.extend_from_slice(b" IS NOT NULL");
                continue;
            }
            Operator::Between => {
                if let Value::Array(vals) = &cond.value
                    && vals.len() == 2
                {
                    buf.extend_from_slice(b" BETWEEN ");
                    encode_value(&vals[0], buf, params)?;
                    buf.extend_from_slice(b" AND ");
                    encode_value(&vals[1], buf, params)?;
                    continue;
                }
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "BETWEEN condition requires exactly two array values".to_string(),
                ));
            }
            Operator::NotBetween => {
                if let Value::Array(vals) = &cond.value
                    && vals.len() == 2
                {
                    buf.extend_from_slice(b" NOT BETWEEN ");
                    encode_value(&vals[0], buf, params)?;
                    buf.extend_from_slice(b" AND ");
                    encode_value(&vals[1], buf, params)?;
                    continue;
                }
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "BETWEEN condition requires exactly two array values".to_string(),
                ));
            }
            Operator::Regex => buf.extend_from_slice(b" ~ "),
            Operator::RegexI => buf.extend_from_slice(b" ~* "),
            Operator::SimilarTo => buf.extend_from_slice(b" SIMILAR TO "),
            Operator::Contains => buf.extend_from_slice(b" @> "),
            Operator::ContainedBy => buf.extend_from_slice(b" <@ "),
            Operator::Overlaps => buf.extend_from_slice(b" && "),
            Operator::Fuzzy => {
                buf.extend_from_slice(b" ILIKE '%' || ");
                encode_value(&cond.value, buf, params)?;
                buf.extend_from_slice(b" || '%'");
                continue;
            }
            Operator::KeyExists => buf.extend_from_slice(b" ? "),
            Operator::KeyExistsAny => buf.extend_from_slice(b" ?| "),
            Operator::KeyExistsAll => buf.extend_from_slice(b" ?& "),
            Operator::JsonPath => buf.extend_from_slice(b" #> "),
            Operator::JsonPathText => buf.extend_from_slice(b" #>> "),
            Operator::ArrayElemContainedInText => buf.extend_from_slice(b" = "),
            Operator::JsonExists | Operator::JsonQuery | Operator::JsonValue => {
                buf.extend_from_slice(b" = ");
            }
            Operator::Exists | Operator::NotExists => {
                // EXISTS/NOT EXISTS: rewrite as a standalone subquery check.
                // Truncate the left-side expression that was already written.
                buf.truncate(left_start);
                // Remove the preceding " AND " if this isn't the first condition
                if i > 0 {
                    // " AND " was already written before encode_expr
                    // but we already truncated the left expr, the " AND " is still there
                }
                if cond.op == Operator::NotExists {
                    buf.extend_from_slice(b"NOT EXISTS (");
                } else {
                    buf.extend_from_slice(b"EXISTS (");
                }
                // Encode the subquery from the value
                match &cond.value {
                    Value::Subquery(q) => {
                        super::super::dml::encode_select(q, buf, params)?;
                    }
                    _ => {
                        return Err(crate::protocol::EncodeError::InvalidAst(
                            "EXISTS condition requires a subquery value".to_string(),
                        ));
                    }
                }
                buf.extend_from_slice(b")");
                continue;
            }
            Operator::TextSearch => {
                // Full-text search: to_tsvector('english', coalesce(col1,'') || ' ' || coalesce(col2,'')) @@ websearch_to_tsquery('english', $N)
                // The left expression contains comma-separated column names
                // We need to rewrite the entire condition
                let col_str = cond.left.to_string();
                let cols: Vec<&str> = col_str.split(',').map(|s| s.trim()).collect();

                // Clear what was already written for the left side
                // We need to truncate back to before encode_expr wrote the left side
                // Instead, we'll handle this specially by computing the full expression
                let tsvector_parts: Vec<String> =
                    cols.iter().map(|c| format!("coalesce({},'')", c)).collect();
                let tsvector_expr = tsvector_parts.join(" || ' ' || ");

                // We already wrote the left expr, so let's replace it
                // Truncate to position before left was written
                // For simplicity, we append the @@ and tsquery part
                // The left side is already written as the column name(s)
                // We need to wrap it. Since encode_expr already wrote, we'll
                // rewrite by clearing and rebuilding this condition.

                buf.truncate(left_start);

                // Write the full tsvector expression
                buf.extend_from_slice(b"to_tsvector('english', ");
                buf.extend_from_slice(tsvector_expr.as_bytes());
                buf.extend_from_slice(b") @@ websearch_to_tsquery('english', ");
                encode_value(&cond.value, buf, params)?;
                buf.extend_from_slice(b")");
                continue;
            }
        }

        encode_value(&cond.value, buf, params)?;
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
            buf.extend_from_slice(col.as_bytes());
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
        Value::Float(f) => buf.extend_from_slice(f.to_string().as_bytes()),
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
