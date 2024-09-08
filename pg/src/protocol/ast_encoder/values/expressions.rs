//! Value and expression encoding.
//!
//! Functions for encoding Expr, Value, Operator, and conditions to wire format.

use bytes::BytesMut;
use qail_core::ast::{Action, CageKind, Condition, Expr, Operator, SortOrder, Value};

use super::super::helpers::{i64_to_bytes, write_param_placeholder, NUMERIC_VALUES};

/// Encode column list to buffer.
pub fn encode_columns(columns: &[Expr], buf: &mut BytesMut) {
    if columns.is_empty() {
        buf.extend_from_slice(b"*");
        return;
    }

    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        encode_column_expr(col, buf);
    }
}

/// Encode a single column expression (supports complex expressions).
pub fn encode_column_expr(col: &Expr, buf: &mut BytesMut) {
    match col {
        Expr::Star => buf.extend_from_slice(b"*"),
        Expr::Named(name) => buf.extend_from_slice(name.as_bytes()),
        Expr::Aliased { name, alias } => {
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(b" AS ");
            buf.extend_from_slice(alias.as_bytes());
        }
        Expr::Aggregate { col, func, distinct, filter: _, alias } => {
            buf.extend_from_slice(func.to_string().as_bytes());
            buf.extend_from_slice(b"(");
            if *distinct {
                buf.extend_from_slice(b"DISTINCT ");
            }
            buf.extend_from_slice(col.as_bytes());
            buf.extend_from_slice(b")");
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
                encode_column_expr(arg, buf);
            }
            buf.extend_from_slice(b")");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::Cast { expr, target_type, alias } => {
            encode_column_expr(expr, buf);
            buf.extend_from_slice(b"::");
            buf.extend_from_slice(target_type.as_bytes());
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::Binary { left, op, right, alias } => {
            buf.extend_from_slice(b"(");
            encode_column_expr(left, buf);
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(op.to_string().as_bytes());
            buf.extend_from_slice(b" ");
            encode_column_expr(right, buf);
            buf.extend_from_slice(b")");
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::Literal(val) => {
            buf.extend_from_slice(val.to_string().as_bytes());
        }
        Expr::Case { when_clauses, else_value, alias } => {
            buf.extend_from_slice(b"CASE");
            for (cond, then_expr) in when_clauses {
                buf.extend_from_slice(b" WHEN ");
                buf.extend_from_slice(cond.left.to_string().as_bytes());
                buf.extend_from_slice(b" ");
                encode_operator(&cond.op, buf);
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(cond.value.to_string().as_bytes());
                buf.extend_from_slice(b" THEN ");
                encode_column_expr(then_expr, buf);
            }
            if let Some(else_val) = else_value {
                buf.extend_from_slice(b" ELSE ");
                encode_column_expr(else_val, buf);
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
                    encode_column_expr(expr, buf);
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
                    encode_column_expr(expr, buf);
                }
                buf.extend_from_slice(b")");
            }
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::JsonAccess { column, path_segments, alias } => {
            buf.extend_from_slice(column.as_bytes());
            for (key, as_text) in path_segments {
                if *as_text {
                    buf.extend_from_slice(b"->>'");
                } else {
                    buf.extend_from_slice(b"->'");
                }
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b"'");
            }
            if let Some(a) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(a.as_bytes());
            }
        }
        Expr::Window { name, func, params, partition, order, frame: _ } => {
            buf.extend_from_slice(func.to_uppercase().as_bytes());
            buf.extend_from_slice(b"(");
            for (i, p) in params.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                buf.extend_from_slice(p.to_string().as_bytes());
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
                            SortOrder::DescNullsFirst => buf.extend_from_slice(b" DESC NULLS FIRST"),
                            SortOrder::DescNullsLast => buf.extend_from_slice(b" DESC NULLS LAST"),
                        }
                    }
                }
            }
            buf.extend_from_slice(b")");
            if !name.is_empty() {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(name.as_bytes());
            }
        }
        _ => buf.extend_from_slice(b"*"),
    }
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
    };
    buf.extend_from_slice(bytes);
}

/// Encode simple expression (for WHERE left side).
pub fn encode_expr(expr: &Expr, buf: &mut BytesMut) {
    match expr {
        Expr::Named(name) => buf.extend_from_slice(name.as_bytes()),
        Expr::Star => buf.extend_from_slice(b"*"),
        Expr::Aliased { name, .. } => buf.extend_from_slice(name.as_bytes()),
        _ => buf.extend_from_slice(b"*"),
    }
}

/// Encode JOIN ON value - AST-native, no allocations for column references.
pub fn encode_join_value(value: &Value, buf: &mut BytesMut) {
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
        _ => buf.extend_from_slice(value.to_string().as_bytes()),
    }
}

/// Encode WHERE conditions with parameter extraction.
pub fn encode_conditions(
    conditions: &[Condition],
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) {
    for (i, cond) in conditions.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b" AND ");
        }
        encode_expr(&cond.left, buf);

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
                    buf.extend_from_slice(b" IN (");
                    for (j, v) in vals.iter().enumerate() {
                        if j > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        encode_value(v, buf, params);
                    }
                    buf.extend_from_slice(b")");
                    continue;
                }
                buf.extend_from_slice(b" IN ");
            }
            Operator::NotIn => {
                if let Value::Array(vals) = &cond.value {
                    buf.extend_from_slice(b" NOT IN (");
                    for (j, v) in vals.iter().enumerate() {
                        if j > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        encode_value(v, buf, params);
                    }
                    buf.extend_from_slice(b")");
                    continue;
                }
                buf.extend_from_slice(b" NOT IN ");
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
                    && vals.len() >= 2
                {
                    buf.extend_from_slice(b" BETWEEN ");
                    encode_value(&vals[0], buf, params);
                    buf.extend_from_slice(b" AND ");
                    encode_value(&vals[1], buf, params);
                    continue;
                }
                buf.extend_from_slice(b" = ");
            }
            Operator::NotBetween => {
                if let Value::Array(vals) = &cond.value
                    && vals.len() >= 2
                {
                    buf.extend_from_slice(b" NOT BETWEEN ");
                    encode_value(&vals[0], buf, params);
                    buf.extend_from_slice(b" AND ");
                    encode_value(&vals[1], buf, params);
                    continue;
                }
                buf.extend_from_slice(b" = ");
            }
            Operator::Regex => buf.extend_from_slice(b" ~ "),
            Operator::RegexI => buf.extend_from_slice(b" ~* "),
            Operator::SimilarTo => buf.extend_from_slice(b" SIMILAR TO "),
            Operator::Contains => buf.extend_from_slice(b" @> "),
            Operator::ContainedBy => buf.extend_from_slice(b" <@ "),
            Operator::Overlaps => buf.extend_from_slice(b" && "),
            Operator::Fuzzy => buf.extend_from_slice(b" ILIKE "),
            Operator::KeyExists => buf.extend_from_slice(b" ? "),
            Operator::JsonExists | Operator::JsonQuery | Operator::JsonValue => {
                buf.extend_from_slice(b" = ");
            }
            Operator::Exists | Operator::NotExists => {
                buf.extend_from_slice(b" = ");
            }
        }

        encode_value(&cond.value, buf, params);
    }
}

/// Encode value - extract to parameter or inline.
/// Uses pre-computed lookup tables for zero allocation in common cases.
pub fn encode_value(value: &Value, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) {
    match value {
        Value::Null => {
            params.push(None);
            write_param_placeholder(buf, params.len());
        }
        Value::String(s) => {
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
            write_param_placeholder(buf, *n);
        }
        Value::NamedParam(name) => {
            params.push(None);
            write_param_placeholder(buf, params.len());
            let _ = name;
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
                write_value_to_array(&mut arr_buf, v);
            }
            arr_buf.push(b'}');
            params.push(Some(arr_buf));
            write_param_placeholder(buf, params.len());
        }
        Value::Function(f) => {
            buf.extend_from_slice(f.as_bytes());
        }
        Value::Column(col) => {
            buf.extend_from_slice(col.as_bytes());
        }
        Value::Subquery(q) => {
            let mut sub_buf = BytesMut::with_capacity(128);
            let mut sub_params: Vec<Option<Vec<u8>>> = Vec::new();
            match q.action {
                Action::Get => super::super::dml::encode_select(q, &mut sub_buf, &mut sub_params),
                _ => panic!("Unsupported subquery action {:?}", q.action),
            }
            buf.extend_from_slice(b"(");
            buf.extend_from_slice(&sub_buf);
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
            encode_column_expr(expr, buf);
        }
    }
}

/// Write a Value as a literal into an array buffer.
pub fn write_value_to_array(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Int(n) => {
            if (0..100).contains(n) {
                buf.extend_from_slice(NUMERIC_VALUES[*n as usize]);
            } else {
                buf.extend_from_slice(n.to_string().as_bytes());
            }
        }
        Value::String(s) => {
            buf.push(b'"');
            for byte in s.bytes() {
                if byte == b'"' {
                    buf.push(b'\\');
                }
                buf.push(byte);
            }
            buf.push(b'"');
        }
        Value::Bool(b) => buf.extend_from_slice(if *b { b"t" } else { b"f" }),
        Value::Null => buf.extend_from_slice(b"NULL"),
        Value::Float(f) => buf.extend_from_slice(f.to_string().as_bytes()),
        _ => buf.extend_from_slice(value.to_string().as_bytes()),
    }
}
