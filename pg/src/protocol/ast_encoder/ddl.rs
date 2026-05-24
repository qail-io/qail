//! DDL (Data Definition Language) encoders.
//!
//! CREATE TABLE, CREATE INDEX, DROP, ALTER statements.

use bytes::BytesMut;
use qail_core::ast::{
    Action, ColumnGeneration, Constraint, Expr, Qail, TableConstraint, TriggerEvent, TriggerTiming,
};
use qail_core::migrate::policy::{PolicyPermissiveness, PolicyTarget};
use qail_core::transpiler::{escape_identifier, escape_sql_string_literal};

/// Quote a SQL identifier for savepoint names.
fn quote_savepoint_name(name: &str) -> String {
    let clean = name.replace('\0', "").replace('"', "\"\"");
    format!("\"{}\"", clean)
}

/// Map QAIL types to PostgreSQL types.
#[inline]
pub fn map_type(t: &str) -> &str {
    match t {
        "str" | "text" | "string" | "TEXT" => "TEXT",
        "smallint" | "SMALLINT" | "int2" | "INT2" => "SMALLINT",
        "int" | "i32" | "INT" | "INTEGER" => "INT",
        "bigint" | "i64" | "BIGINT" => "BIGINT",
        "uuid" | "UUID" => "UUID",
        "bool" | "boolean" | "BOOLEAN" => "BOOLEAN",
        "dec" | "decimal" | "DECIMAL" => "DECIMAL",
        "float" | "f64" | "DOUBLE PRECISION" => "DOUBLE PRECISION",
        "serial" | "SERIAL" => "SERIAL",
        "bigserial" | "BIGSERIAL" => "BIGSERIAL",
        "timestamp" | "time" | "TIMESTAMP" => "TIMESTAMP",
        "timestamptz" | "TIMESTAMPTZ" => "TIMESTAMPTZ",
        "date" | "DATE" => "DATE",
        "json" | "jsonb" | "JSON" | "JSONB" => "JSONB",
        "varchar" | "VARCHAR" => "VARCHAR(255)",
        _ => t,
    }
}

fn data_type_to_sql(t: &str) -> String {
    let mapped = map_type(t);
    if mapped != t {
        mapped.to_string()
    } else {
        sql_type_fragment_to_sql(t, "TEXT")
    }
}

fn push_joined_ident_list(buf: &mut BytesMut, cols: &[String]) {
    for (i, col) in cols.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        push_identifier(buf, col);
    }
}

fn push_identifier(buf: &mut BytesMut, ident: &str) {
    buf.extend_from_slice(escape_identifier(ident).as_bytes());
}

fn quote_double_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\0', "").replace('"', "\"\""))
}

fn strip_option_quotes(value: &str) -> &str {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        if (bytes[0] == b'\'' && bytes[trimmed.len() - 1] == b'\'')
            || (bytes[0] == b'"' && bytes[trimmed.len() - 1] == b'"')
        {
            return &trimmed[1..trimmed.len() - 1];
        }
    }
    trimmed
}

fn extension_option_to_sql(opt: &str) -> Option<String> {
    let trimmed = opt.trim();
    let (keyword, rest) = trimmed.split_once(char::is_whitespace)?;
    let rest = rest.trim();
    if rest.is_empty() {
        return None;
    }

    match keyword.to_ascii_uppercase().as_str() {
        "SCHEMA" => Some(format!(
            "SCHEMA {}",
            escape_identifier(strip_option_quotes(rest))
        )),
        "VERSION" => Some(format!(
            "VERSION '{}'",
            escape_sql_string_literal(strip_option_quotes(rest))
        )),
        _ => None,
    }
}

fn parse_sequence_i64(value: &str) -> Option<i64> {
    value.trim().parse::<i64>().ok()
}

fn sequence_type_to_sql(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "smallint" | "int2" => Some("SMALLINT"),
        "integer" | "int" | "int4" => Some("INTEGER"),
        "bigint" | "int8" => Some("BIGINT"),
        _ => None,
    }
}

fn sequence_owned_by_to_sql(parts: &[&str]) -> Option<String> {
    if parts.len() == 1 && parts[0].eq_ignore_ascii_case("none") {
        return Some("OWNED BY NONE".to_string());
    }

    let dotted_parts;
    let ident_parts = if parts.len() == 1 {
        dotted_parts = parts[0].split('.').collect::<Vec<_>>();
        dotted_parts.as_slice()
    } else {
        parts
    };
    if !(2..=3).contains(&ident_parts.len()) {
        return None;
    }

    Some(format!(
        "OWNED BY {}",
        ident_parts
            .iter()
            .map(|part| escape_identifier(part))
            .collect::<Vec<_>>()
            .join(".")
    ))
}

fn sequence_option_to_sql(opt: &str) -> Option<String> {
    let parts: Vec<&str> = opt.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    match parts[0].to_ascii_lowercase().as_str() {
        "as" if parts.len() == 2 => sequence_type_to_sql(parts[1]).map(|t| format!("AS {t}")),
        "start" => {
            let value = match parts.as_slice() {
                [_, value] => *value,
                [_, with, value] if with.eq_ignore_ascii_case("with") => *value,
                _ => return None,
            };
            parse_sequence_i64(value).map(|n| format!("START WITH {n}"))
        }
        "increment" => {
            let value = match parts.as_slice() {
                [_, value] => *value,
                [_, by, value] if by.eq_ignore_ascii_case("by") => *value,
                _ => return None,
            };
            parse_sequence_i64(value).map(|n| format!("INCREMENT BY {n}"))
        }
        "minvalue" if parts.len() == 2 => {
            parse_sequence_i64(parts[1]).map(|n| format!("MINVALUE {n}"))
        }
        "maxvalue" if parts.len() == 2 => {
            parse_sequence_i64(parts[1]).map(|n| format!("MAXVALUE {n}"))
        }
        "cache" if parts.len() == 2 => parse_sequence_i64(parts[1]).map(|n| format!("CACHE {n}")),
        "cycle" if parts.len() == 1 => Some("CYCLE".to_string()),
        "owned_by" => sequence_owned_by_to_sql(&parts[1..]),
        "owned" if parts.len() >= 3 && parts[1].eq_ignore_ascii_case("by") => {
            sequence_owned_by_to_sql(&parts[2..])
        }
        "no" if parts.len() == 2 && parts[1].eq_ignore_ascii_case("minvalue") => {
            Some("NO MINVALUE".to_string())
        }
        "no" if parts.len() == 2 && parts[1].eq_ignore_ascii_case("maxvalue") => {
            Some("NO MAXVALUE".to_string())
        }
        "no" if parts.len() == 2 && parts[1].eq_ignore_ascii_case("cycle") => {
            Some("NO CYCLE".to_string())
        }
        _ => None,
    }
}

fn push_index_column(buf: &mut BytesMut, column: &str) -> Result<(), crate::protocol::EncodeError> {
    let column = column.trim();
    if column.is_empty() || column.contains('\0') {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "invalid index column: {column:?}"
        )));
    }
    if column.contains('(') {
        if contains_unquoted_statement_delimiter(column) {
            push_identifier(buf, column);
        } else {
            buf.extend_from_slice(column.trim().as_bytes());
        }
    } else {
        push_identifier(buf, column);
    }
    Ok(())
}

fn dollar_quote_block(body: &str) -> String {
    let body = body.replace('\0', "");
    for idx in 0..=body.len() {
        let tag = if idx == 0 {
            String::new()
        } else {
            format!("qail_body_{idx}")
        };
        let delimiter = format!("${tag}$");
        if !body.contains(&delimiter) {
            return format!("{delimiter} {body} {delimiter}");
        }
    }

    format!("'{}'", escape_sql_string_literal(&body))
}

fn call_target_to_sql(target: &str) -> String {
    let target = target.trim().trim_end_matches(';').trim();
    if target.is_empty()
        || target.contains('\0')
        || target.contains(';')
        || target.contains("--")
        || target.contains("/*")
        || target.contains("*/")
    {
        return escape_identifier(target);
    }

    match target.split_once('(') {
        Some((name, args)) if args.ends_with(')') && !args[..args.len() - 1].contains('(') => {
            format!("{}({}", escape_identifier(name.trim()), args)
        }
        None => escape_identifier(target),
        _ => escape_identifier(target),
    }
}

fn is_safe_sql_type_fragment(fragment: &str) -> bool {
    let fragment = fragment.trim();
    !fragment.is_empty()
        && !fragment.contains('\0')
        && !fragment.contains(';')
        && !fragment.contains('\'')
        && !fragment.contains('"')
        && !fragment.contains("--")
        && !fragment.contains("/*")
        && !fragment.contains("*/")
        && fragment.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'_' | b'.' | b' ' | b'(' | b')' | b',' | b'[' | b']' | b'%' | b'+' | b'-'
                )
        })
}

fn sql_type_fragment_to_sql(fragment: &str, fallback: &str) -> String {
    let fragment = fragment.trim();
    if is_safe_sql_type_fragment(fragment) {
        fragment.to_string()
    } else {
        fallback.to_string()
    }
}

fn volatility_to_sql(volatility: &str) -> Option<&'static str> {
    match volatility.trim().to_ascii_uppercase().as_str() {
        "VOLATILE" => Some("VOLATILE"),
        "STABLE" => Some("STABLE"),
        "IMMUTABLE" => Some("IMMUTABLE"),
        _ => None,
    }
}

fn function_arg_to_sql(arg: &str) -> Option<String> {
    let arg = arg.trim();
    if !is_safe_sql_type_fragment(arg) {
        return None;
    }

    let mut parts = arg.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    if parts.len() == 1 {
        return Some(parts[0].to_string());
    }

    let mode = match parts[0].to_ascii_uppercase().as_str() {
        "IN" | "OUT" | "INOUT" | "VARIADIC" => Some(parts.remove(0).to_ascii_uppercase()),
        _ => None,
    };
    if parts.len() < 2 {
        return None;
    }

    let name = escape_identifier(parts.remove(0));
    let type_fragment = parts.join(" ");
    if !is_safe_sql_type_fragment(&type_fragment) {
        return None;
    }

    let mut rendered = String::new();
    if let Some(mode) = mode {
        rendered.push_str(&mode);
        rendered.push(' ');
    }
    rendered.push_str(&name);
    rendered.push(' ');
    rendered.push_str(type_fragment.trim());
    Some(rendered)
}

fn function_args_to_sql(args: &[String]) -> Result<String, crate::protocol::EncodeError> {
    let mut rendered = Vec::with_capacity(args.len());
    for arg in args {
        let Some(sql) = function_arg_to_sql(arg) else {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "invalid function argument: {arg:?}"
            )));
        };
        rendered.push(sql);
    }
    Ok(rendered.join(", "))
}

fn split_top_level_args(args: &str) -> Option<Vec<&str>> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    for (idx, ch) in args.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.checked_sub(1)?,
            ',' if depth == 0 => {
                result.push(args[start..idx].trim());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    if depth != 0 {
        return None;
    }
    let tail = args[start..].trim();
    if !tail.is_empty() {
        result.push(tail);
    }
    Some(result)
}

fn function_signature_to_sql(signature: &str) -> String {
    let signature = signature.trim().trim_end_matches(';').trim();
    if signature.is_empty()
        || signature.contains('\0')
        || signature.contains(';')
        || signature.contains("--")
        || signature.contains("/*")
        || signature.contains("*/")
    {
        return escape_identifier(signature);
    }

    match signature.split_once('(') {
        Some((name, args)) if args.ends_with(')') => {
            let args = &args[..args.len() - 1];
            let Some(parts) = split_top_level_args(args) else {
                return escape_identifier(signature);
            };
            let mut rendered_args = Vec::new();
            for part in parts {
                if part.is_empty() {
                    continue;
                }
                if !is_safe_sql_type_fragment(part) {
                    return escape_identifier(signature);
                }
                rendered_args.push(part.trim().to_string());
            }
            format!(
                "{}({})",
                escape_identifier(name.trim()),
                rendered_args.join(", ")
            )
        }
        None => escape_identifier(signature),
        _ => escape_identifier(signature),
    }
}

fn contains_statement_delimiter(value: &str) -> bool {
    value.contains('\0')
        || value.contains(';')
        || value.contains("--")
        || value.contains("/*")
        || value.contains("*/")
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

fn checked_sql_expr_fragment(
    expr: &str,
    context: &str,
) -> Result<String, crate::protocol::EncodeError> {
    let expr = expr.trim();
    if expr.is_empty() || expr.contains('\0') || contains_unquoted_statement_delimiter(expr) {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "invalid {context}: {expr:?}"
        )));
    }
    Ok(expr.to_string())
}

fn index_method_to_sql(method: &str) -> Option<&'static str> {
    match method.trim().to_ascii_lowercase().as_str() {
        "btree" => Some("btree"),
        "hash" => Some("hash"),
        "gin" => Some("gin"),
        "gist" => Some("gist"),
        "brin" => Some("brin"),
        "spgist" | "sp-gist" => Some("spgist"),
        _ => None,
    }
}

fn parse_fk_action(tokens: &[&str], index: usize) -> Option<(String, usize)> {
    match tokens.get(index)?.to_ascii_uppercase().as_str() {
        "CASCADE" => Some(("CASCADE".to_string(), index + 1)),
        "RESTRICT" => Some(("RESTRICT".to_string(), index + 1)),
        "NO" if tokens.get(index + 1)?.eq_ignore_ascii_case("ACTION") => {
            Some(("NO ACTION".to_string(), index + 2))
        }
        "SET" if tokens.get(index + 1)?.eq_ignore_ascii_case("NULL") => {
            Some(("SET NULL".to_string(), index + 2))
        }
        "SET" if tokens.get(index + 1)?.eq_ignore_ascii_case("DEFAULT") => {
            Some(("SET DEFAULT".to_string(), index + 2))
        }
        _ => None,
    }
}

fn reference_tail_to_sql(tail: &str) -> Option<String> {
    let tail = tail.trim();
    if tail.is_empty() {
        return Some(String::new());
    }
    if contains_statement_delimiter(tail) {
        return None;
    }

    let tokens = tail.split_whitespace().collect::<Vec<_>>();
    let mut rendered = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("ON") {
            let event = tokens.get(i + 1)?;
            let event_sql = if event.eq_ignore_ascii_case("DELETE") {
                "DELETE"
            } else if event.eq_ignore_ascii_case("UPDATE") {
                "UPDATE"
            } else {
                return None;
            };
            let (action, next) = parse_fk_action(&tokens, i + 2)?;
            rendered.push(format!("ON {event_sql} {action}"));
            i = next;
        } else if tokens[i].eq_ignore_ascii_case("DEFERRABLE") {
            rendered.push("DEFERRABLE".to_string());
            i += 1;
        } else if tokens[i].eq_ignore_ascii_case("NOT")
            && tokens.get(i + 1)?.eq_ignore_ascii_case("DEFERRABLE")
        {
            rendered.push("NOT DEFERRABLE".to_string());
            i += 2;
        } else if tokens[i].eq_ignore_ascii_case("INITIALLY") {
            let mode = tokens.get(i + 1)?;
            if mode.eq_ignore_ascii_case("DEFERRED") {
                rendered.push("INITIALLY DEFERRED".to_string());
            } else if mode.eq_ignore_ascii_case("IMMEDIATE") {
                rendered.push("INITIALLY IMMEDIATE".to_string());
            } else {
                return None;
            }
            i += 2;
        } else {
            return None;
        }
    }

    Some(format!(" {}", rendered.join(" ")))
}

fn references_target_to_sql(target: &str) -> String {
    let target = target.trim();
    if target.is_empty() || contains_statement_delimiter(target) {
        return escape_identifier(target);
    }

    let Some((table, rest)) = target.split_once('(') else {
        return escape_identifier(target);
    };
    let Some(close_idx) = rest.find(')') else {
        return escape_identifier(target);
    };

    let table = table.trim();
    let columns = rest[..close_idx]
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>();
    if table.is_empty() || columns.is_empty() || columns.iter().any(|col| col.is_empty()) {
        return escape_identifier(target);
    }

    let Some(tail) = reference_tail_to_sql(&rest[close_idx + 1..]) else {
        return escape_identifier(target);
    };

    format!(
        "{}({}){}",
        escape_identifier(table),
        columns
            .iter()
            .map(|col| escape_identifier(col))
            .collect::<Vec<_>>()
            .join(", "),
        tail
    )
}

fn encode_table_constraint(constraint: &TableConstraint, buf: &mut BytesMut) {
    match constraint {
        TableConstraint::Unique(cols) => {
            buf.extend_from_slice(b"UNIQUE (");
            push_joined_ident_list(buf, cols);
            buf.extend_from_slice(b")");
        }
        TableConstraint::PrimaryKey(cols) => {
            buf.extend_from_slice(b"PRIMARY KEY (");
            push_joined_ident_list(buf, cols);
            buf.extend_from_slice(b")");
        }
        TableConstraint::ForeignKey {
            name,
            columns,
            ref_table,
            ref_columns,
        } => {
            if let Some(name) = name {
                buf.extend_from_slice(b"CONSTRAINT ");
                push_identifier(buf, name);
                buf.extend_from_slice(b" ");
            }
            buf.extend_from_slice(b"FOREIGN KEY (");
            push_joined_ident_list(buf, columns);
            buf.extend_from_slice(b") REFERENCES ");
            push_identifier(buf, ref_table);
            buf.extend_from_slice(b"(");
            push_joined_ident_list(buf, ref_columns);
            buf.extend_from_slice(b")");
        }
    }
}

fn encode_column_check_constraint(
    name: &str,
    vals: &[String],
    buf: &mut BytesMut,
) -> Result<(), crate::protocol::EncodeError> {
    if vals.len() == 1
        && vals[0]
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("CONSTRAINT ")
    {
        buf.extend_from_slice(b" ");
        if vals[0].contains('\0') || contains_unquoted_statement_delimiter(&vals[0]) {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "invalid column check constraint for {name:?}: {:?}",
                vals[0]
            )));
        } else {
            buf.extend_from_slice(vals[0].as_bytes());
        }
        return Ok(());
    }

    let looks_like_expr = vals.len() == 1
        || vals.iter().any(|v| {
            v.chars()
                .any(|c| c.is_whitespace() || matches!(c, '<' | '>' | '=' | '!' | '(' | ')'))
        });

    if looks_like_expr {
        let raw_check = checked_sql_expr_fragment(
            &vals.join(" "),
            &format!("column check expression for {name:?}"),
        )?;
        buf.extend_from_slice(b" CHECK (");
        buf.extend_from_slice(raw_check.as_bytes());
        buf.extend_from_slice(b")");
    } else {
        buf.extend_from_slice(b" CHECK (");
        push_identifier(buf, name);
        buf.extend_from_slice(b" IN (");
        for (i, v) in vals.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            buf.extend_from_slice(b"'");
            buf.extend_from_slice(escape_sql_string_literal(v).as_bytes());
            buf.extend_from_slice(b"'");
        }
        buf.extend_from_slice(b"))");
    }
    Ok(())
}

/// Encode CREATE TABLE statement.
pub fn encode_make(cmd: &Qail, buf: &mut BytesMut) -> Result<(), crate::protocol::EncodeError> {
    buf.extend_from_slice(b"CREATE TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" (");

    let composite_pk_columns: Vec<&str> = cmd
        .columns
        .iter()
        .filter_map(|col| match col {
            Expr::Def {
                name, constraints, ..
            } if constraints.contains(&Constraint::PrimaryKey) => Some(name.as_str()),
            _ => None,
        })
        .collect();
    let use_composite_pk = composite_pk_columns.len() > 1;

    let mut first = true;
    for col in &cmd.columns {
        if let Expr::Def {
            name,
            data_type,
            constraints,
        } = col
        {
            if !first {
                buf.extend_from_slice(b", ");
            }
            first = false;

            push_identifier(buf, name);
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(data_type_to_sql(data_type).as_bytes());

            // Default to NOT NULL unless Nullable
            if !constraints.contains(&Constraint::Nullable) {
                buf.extend_from_slice(b" NOT NULL");
            }

            // DEFAULT
            for constraint in constraints {
                if let Constraint::Default(val) = constraint {
                    buf.extend_from_slice(b" DEFAULT ");
                    let sql_default = match val.as_str() {
                        "uuid()" => "gen_random_uuid()".to_string(),
                        "now()" => "NOW()".to_string(),
                        other => checked_sql_expr_fragment(
                            other,
                            &format!("column default expression for {name:?}"),
                        )?,
                    };
                    buf.extend_from_slice(sql_default.as_bytes());
                }
                if let Constraint::Generated(generation) = constraint {
                    match generation {
                        ColumnGeneration::Stored(expr) if expr == "identity" => {
                            buf.extend_from_slice(b" GENERATED ALWAYS AS IDENTITY");
                        }
                        ColumnGeneration::Stored(expr) if expr == "identity_by_default" => {
                            buf.extend_from_slice(b" GENERATED BY DEFAULT AS IDENTITY");
                        }
                        ColumnGeneration::Stored(expr) => {
                            buf.extend_from_slice(b" GENERATED ALWAYS AS (");
                            let expr = checked_sql_expr_fragment(
                                expr,
                                &format!("generated column expression for {name:?}"),
                            )?;
                            buf.extend_from_slice(expr.as_bytes());
                            buf.extend_from_slice(b") STORED");
                        }
                        ColumnGeneration::Virtual(expr) => {
                            buf.extend_from_slice(b" GENERATED ALWAYS AS (");
                            let expr = checked_sql_expr_fragment(
                                expr,
                                &format!("generated column expression for {name:?}"),
                            )?;
                            buf.extend_from_slice(expr.as_bytes());
                            buf.extend_from_slice(b")");
                        }
                    }
                }
            }

            // PRIMARY KEY
            if constraints.contains(&Constraint::PrimaryKey) && !use_composite_pk {
                buf.extend_from_slice(b" PRIMARY KEY");
            }

            // UNIQUE
            if constraints.contains(&Constraint::Unique) {
                buf.extend_from_slice(b" UNIQUE");
            }

            // REFERENCES (foreign key)
            for constraint in constraints {
                if let Constraint::References(target) = constraint {
                    buf.extend_from_slice(b" REFERENCES ");
                    buf.extend_from_slice(references_target_to_sql(target).as_bytes());
                }
            }

            // CHECK constraint
            for constraint in constraints {
                if let Constraint::Check(vals) = constraint {
                    encode_column_check_constraint(name, vals, buf)?;
                }
            }
        }
    }

    if use_composite_pk {
        if !first {
            buf.extend_from_slice(b", ");
        }
        first = false;
        buf.extend_from_slice(b"PRIMARY KEY (");
        for (i, col) in composite_pk_columns.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            push_identifier(buf, col);
        }
        buf.extend_from_slice(b")");
    }

    // Table constraints
    for tc in &cmd.table_constraints {
        if !first {
            buf.extend_from_slice(b", ");
        }
        first = false;
        encode_table_constraint(tc, buf);
    }

    buf.extend_from_slice(b")");
    Ok(())
}

/// Encode CREATE INDEX statement.
pub fn encode_index(cmd: &Qail, buf: &mut BytesMut) -> Result<(), super::super::EncodeError> {
    let Some(idx) = &cmd.index_def else {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "CREATE INDEX requires an index definition".to_string(),
        ));
    };
    if idx.columns.is_empty() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "CREATE INDEX requires at least one column".to_string(),
        ));
    }

    if idx.unique {
        buf.extend_from_slice(b"CREATE UNIQUE INDEX ");
    } else {
        buf.extend_from_slice(b"CREATE INDEX ");
    }
    push_identifier(buf, &idx.name);
    buf.extend_from_slice(b" ON ");
    push_identifier(buf, &idx.table);
    if let Some(method) = &idx.index_type
        && !method.trim().is_empty()
    {
        let Some(method) = index_method_to_sql(method) else {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "invalid index method: {method:?}"
            )));
        };
        buf.extend_from_slice(b" USING ");
        buf.extend_from_slice(method.as_bytes());
    }
    buf.extend_from_slice(b" (");
    for (i, col) in idx.columns.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        push_index_column(buf, col)?;
    }
    buf.extend_from_slice(b")");
    if let Some(where_clause) = &idx.where_clause {
        if where_clause.trim().is_empty()
            || where_clause.contains('\0')
            || contains_unquoted_statement_delimiter(where_clause)
        {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "invalid index predicate: {where_clause:?}"
            )));
        }
        buf.extend_from_slice(b" WHERE ");
        buf.extend_from_slice(where_clause.trim().as_bytes());
    }
    Ok(())
}

/// Encode DROP TABLE statement.
pub fn encode_drop_table(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP TABLE IF EXISTS ");
    push_identifier(buf, &cmd.table);
}

/// Encode DROP INDEX statement.
pub fn encode_drop_index(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP INDEX IF EXISTS ");
    push_identifier(buf, &cmd.table);
}

/// Encode ALTER TABLE ADD COLUMN statement.
pub fn encode_alter_add_column(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    if cmd.columns.is_empty() && cmd.table_constraints.is_empty() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "ALTER ADD requires a column or table constraint".to_string(),
        ));
    }
    for col in &cmd.columns {
        if !matches!(col, Expr::Def { .. }) {
            return Err(crate::protocol::EncodeError::InvalidAst(
                "ALTER ADD columns must be column definitions".to_string(),
            ));
        }
    }

    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" ");

    let mut first = true;
    for col in &cmd.columns {
        if let Expr::Def {
            name,
            data_type,
            constraints,
        } = col
        {
            if !first {
                buf.extend_from_slice(b", ");
            }
            first = false;

            buf.extend_from_slice(b"ADD COLUMN ");
            push_identifier(buf, name);
            buf.extend_from_slice(b" ");
            buf.extend_from_slice(data_type_to_sql(data_type).as_bytes());

            if !constraints.contains(&Constraint::Nullable) {
                buf.extend_from_slice(b" NOT NULL");
            }
            if constraints.contains(&Constraint::Unique) {
                buf.extend_from_slice(b" UNIQUE");
            }

            for constraint in constraints {
                if let Constraint::Default(val) = constraint {
                    buf.extend_from_slice(b" DEFAULT ");
                    let sql_default = match val.as_str() {
                        "uuid()" => "gen_random_uuid()".to_string(),
                        "now()" => "NOW()".to_string(),
                        other => checked_sql_expr_fragment(
                            other,
                            &format!("column default expression for {name:?}"),
                        )?,
                    };
                    buf.extend_from_slice(sql_default.as_bytes());
                }
                if let Constraint::References(target) = constraint {
                    buf.extend_from_slice(b" REFERENCES ");
                    buf.extend_from_slice(references_target_to_sql(target).as_bytes());
                }
                if let Constraint::Check(vals) = constraint {
                    encode_column_check_constraint(name, vals, buf)?;
                }
            }
        }
    }

    for constraint in &cmd.table_constraints {
        if !first {
            buf.extend_from_slice(b", ");
        }
        first = false;
        buf.extend_from_slice(b"ADD ");
        encode_table_constraint(constraint, buf);
    }

    Ok(())
}

/// Encode ALTER TABLE DROP COLUMN statement.
pub fn encode_alter_drop_column(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    if cmd.columns.is_empty() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "ALTER DROP requires at least one column".to_string(),
        ));
    }

    let mut names = Vec::with_capacity(cmd.columns.len());
    for col in &cmd.columns {
        let col_name = match col {
            Expr::Named(n) => n.as_str(),
            Expr::Def { name, .. } => name.as_str(),
            _ => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "ALTER DROP columns must be named expressions or definitions".to_string(),
                ));
            }
        };
        names.push(col_name);
    }

    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" ");
    for (i, col_name) in names.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        buf.extend_from_slice(b"DROP COLUMN ");
        push_identifier(buf, col_name);
    }
    Ok(())
}

/// Encode ALTER TABLE ALTER COLUMN TYPE statement.
pub fn encode_alter_column_type(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    if cmd.columns.is_empty() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "ALTER TYPE requires at least one column definition".to_string(),
        ));
    }

    let mut defs = Vec::with_capacity(cmd.columns.len());
    for col in &cmd.columns {
        let Expr::Def {
            name, data_type, ..
        } = col
        else {
            return Err(crate::protocol::EncodeError::InvalidAst(
                "ALTER TYPE columns must be column definitions".to_string(),
            ));
        };
        defs.push((name.as_str(), data_type.as_str()));
    }

    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" ");
    for (i, (name, data_type)) in defs.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        buf.extend_from_slice(b"ALTER COLUMN ");
        push_identifier(buf, name);
        buf.extend_from_slice(b" TYPE ");
        buf.extend_from_slice(data_type_to_sql(data_type).as_bytes());
    }
    Ok(())
}

/// Encode ALTER TABLE RENAME COLUMN statement.
/// The `Mod` action stores renames as `Expr::Named("old_name -> new_name")`.
pub fn encode_rename_column(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    let mut rename = None;
    for col in &cmd.columns {
        match col {
            Expr::Named(rename_str) => {
                let Some((old, new)) = rename_str.split_once(" -> ") else {
                    return Err(crate::protocol::EncodeError::InvalidAst(
                        "rename column expressions must use `old -> new`".to_string(),
                    ));
                };
                let old = old.trim();
                let new = new.trim();
                if old.is_empty() || new.is_empty() || rename.replace((old, new)).is_some() {
                    return Err(crate::protocol::EncodeError::InvalidAst(
                        "rename column requires exactly one non-empty `old -> new` expression"
                            .to_string(),
                    ));
                }
            }
            _ => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "rename column expressions must be named expressions".to_string(),
                ));
            }
        }
    }

    let Some((old, new)) = rename else {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "rename column requires exactly one `old -> new` expression".to_string(),
        ));
    };

    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" RENAME COLUMN ");
    push_identifier(buf, old);
    buf.extend_from_slice(b" TO ");
    push_identifier(buf, new);
    Ok(())
}

/// Encode CREATE VIEW statement.
/// CREATE VIEW name AS SELECT ...
pub fn encode_create_view(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), super::super::EncodeError> {
    buf.extend_from_slice(b"CREATE VIEW ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" AS ");

    // The source_query contains the SELECT statement for the view
    if let Some(ref source) = cmd.source_query {
        super::dml::encode_select(source, buf, params)?;
    } else if let Some(query) = &cmd.payload {
        let query = checked_sql_expr_fragment(query, "view query")?;
        buf.extend_from_slice(query.as_bytes());
    } else {
        return Err(super::super::EncodeError::UnsupportedAction(
            Action::CreateView,
        ));
    }
    Ok(())
}

/// Encode DROP VIEW statement.
pub fn encode_drop_view(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP VIEW IF EXISTS ");
    push_identifier(buf, &cmd.table);
}

/// Encode CREATE MATERIALIZED VIEW statement.
pub fn encode_create_materialized_view(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), super::super::EncodeError> {
    buf.extend_from_slice(b"CREATE MATERIALIZED VIEW ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" AS ");

    if let Some(ref source) = cmd.source_query {
        super::dml::encode_select(source, buf, params)?;
    } else if let Some(query) = &cmd.payload {
        let query = checked_sql_expr_fragment(query, "materialized view query")?;
        buf.extend_from_slice(query.as_bytes());
    } else {
        return Err(super::super::EncodeError::UnsupportedAction(
            Action::CreateMaterializedView,
        ));
    }
    Ok(())
}

/// Encode REFRESH MATERIALIZED VIEW statement.
pub fn encode_refresh_materialized_view(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"REFRESH MATERIALIZED VIEW ");
    push_identifier(buf, &cmd.table);
}

/// Encode DROP MATERIALIZED VIEW statement.
pub fn encode_drop_materialized_view(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP MATERIALIZED VIEW IF EXISTS ");
    push_identifier(buf, &cmd.table);
}

fn single_named_column<'a>(
    cmd: &'a Qail,
    action: &str,
) -> Result<&'a str, super::super::EncodeError> {
    let [Expr::Named(col)] = cmd.columns.as_slice() else {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "{action} requires exactly one named column"
        )));
    };
    if col.trim().is_empty() {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "{action} column cannot be empty"
        )));
    }
    Ok(col)
}

/// Encode ALTER TABLE ALTER COLUMN SET NOT NULL.
pub fn encode_alter_set_not_null(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    let col = single_named_column(cmd, "ALTER SET NOT NULL")?;
    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" ALTER COLUMN ");
    push_identifier(buf, col);
    buf.extend_from_slice(b" SET NOT NULL");
    Ok(())
}

/// Encode ALTER TABLE ALTER COLUMN DROP NOT NULL.
pub fn encode_alter_drop_not_null(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    let col = single_named_column(cmd, "ALTER DROP NOT NULL")?;
    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" ALTER COLUMN ");
    push_identifier(buf, col);
    buf.extend_from_slice(b" DROP NOT NULL");
    Ok(())
}

/// Encode ALTER TABLE ALTER COLUMN SET DEFAULT.
pub fn encode_alter_set_default(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    let col = single_named_column(cmd, "ALTER SET DEFAULT")?;
    let Some(default_expr) = cmd.payload.as_deref() else {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "ALTER SET DEFAULT requires a default expression".to_string(),
        ));
    };
    if default_expr.trim().is_empty()
        || default_expr.contains('\0')
        || contains_unquoted_statement_delimiter(default_expr)
    {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "invalid default expression: {default_expr:?}"
        )));
    }
    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" ALTER COLUMN ");
    push_identifier(buf, col);
    buf.extend_from_slice(b" SET DEFAULT ");
    buf.extend_from_slice(default_expr.trim().as_bytes());
    Ok(())
}

/// Encode ALTER TABLE ALTER COLUMN DROP DEFAULT.
pub fn encode_alter_drop_default(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    let col = single_named_column(cmd, "ALTER DROP DEFAULT")?;
    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" ALTER COLUMN ");
    push_identifier(buf, col);
    buf.extend_from_slice(b" DROP DEFAULT");
    Ok(())
}

/// Encode ALTER TABLE ENABLE ROW LEVEL SECURITY.
pub fn encode_alter_enable_rls(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" ENABLE ROW LEVEL SECURITY");
}

/// Encode ALTER TABLE DISABLE ROW LEVEL SECURITY.
pub fn encode_alter_disable_rls(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" DISABLE ROW LEVEL SECURITY");
}

/// Encode ALTER TABLE FORCE ROW LEVEL SECURITY.
pub fn encode_alter_force_rls(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" FORCE ROW LEVEL SECURITY");
}

/// Encode ALTER TABLE NO FORCE ROW LEVEL SECURITY.
pub fn encode_alter_no_force_rls(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"ALTER TABLE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" NO FORCE ROW LEVEL SECURITY");
}

// ── Session & procedural commands ──────────────────────────────────

/// Encode SAVEPOINT name.
pub fn encode_savepoint(cmd: &Qail, buf: &mut BytesMut) {
    let name = quote_savepoint_name(cmd.savepoint_name.as_deref().unwrap_or("qail_sp"));
    buf.extend_from_slice(b"SAVEPOINT ");
    buf.extend_from_slice(name.as_bytes());
}

/// Encode RELEASE SAVEPOINT name.
pub fn encode_release_savepoint(cmd: &Qail, buf: &mut BytesMut) {
    let name = quote_savepoint_name(cmd.savepoint_name.as_deref().unwrap_or("qail_sp"));
    buf.extend_from_slice(b"RELEASE SAVEPOINT ");
    buf.extend_from_slice(name.as_bytes());
}

/// Encode ROLLBACK TO SAVEPOINT name.
pub fn encode_rollback_to_savepoint(cmd: &Qail, buf: &mut BytesMut) {
    let name = quote_savepoint_name(cmd.savepoint_name.as_deref().unwrap_or("qail_sp"));
    buf.extend_from_slice(b"ROLLBACK TO SAVEPOINT ");
    buf.extend_from_slice(name.as_bytes());
}

/// Encode CALL procedure_name.
pub fn encode_call(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"CALL ");
    buf.extend_from_slice(call_target_to_sql(&cmd.table).as_bytes());
}

/// Encode DO $$ body $$ LANGUAGE lang.
pub fn encode_do(cmd: &Qail, buf: &mut BytesMut) {
    let body = cmd.payload.as_deref().unwrap_or("");
    let lang = if cmd.table.is_empty() {
        "plpgsql"
    } else {
        &cmd.table
    };
    buf.extend_from_slice(b"DO ");
    buf.extend_from_slice(dollar_quote_block(body).as_bytes());
    buf.extend_from_slice(b" LANGUAGE ");
    buf.extend_from_slice(escape_identifier(lang).as_bytes());
}

/// Encode SET key = 'value'.
///
/// The value is escaped: `'` → `''` to prevent SQL injection.
pub fn encode_session_set(cmd: &Qail, buf: &mut BytesMut) {
    let value = cmd.payload.as_deref().unwrap_or("");
    buf.extend_from_slice(b"SET ");
    encode_session_setting_name(buf, &cmd.table);
    buf.extend_from_slice(b" = '");
    buf.extend_from_slice(escape_sql_string_literal(value).as_bytes());
    buf.extend_from_slice(b"'");
}

/// Encode SHOW key.
pub fn encode_session_show(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"SHOW ");
    encode_session_setting_name(buf, &cmd.table);
}

/// Encode RESET key.
pub fn encode_session_reset(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"RESET ");
    encode_session_setting_name(buf, &cmd.table);
}

fn encode_session_setting_name(buf: &mut BytesMut, name: &str) {
    if is_valid_session_setting_name(name) {
        buf.extend_from_slice(name.as_bytes());
    } else {
        buf.extend_from_slice(escape_identifier(name).as_bytes());
    }
}

fn is_valid_session_setting_name(name: &str) -> bool {
    !name.is_empty()
        && name.split('.').all(|part| {
            let mut chars = part.chars();
            matches!(chars.next(), Some(ch) if ch.is_ascii_alphabetic() || ch == '_')
                && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        })
}

#[inline]
fn encode_identifier_maybe_quoted(buf: &mut BytesMut, ident: &str) {
    let needs_quotes = ident.is_empty()
        || ident
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
        || ident
            .chars()
            .any(|c| !c.is_ascii_alphanumeric() && c != '_');

    if needs_quotes {
        buf.extend_from_slice(b"\"");
        buf.extend_from_slice(ident.replace('"', "\"\"").as_bytes());
        buf.extend_from_slice(b"\"");
    } else {
        buf.extend_from_slice(ident.as_bytes());
    }
}

/// Encode CREATE DATABASE name.
pub fn encode_create_database(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"CREATE DATABASE ");
    encode_identifier_maybe_quoted(buf, &cmd.table);
}

/// Encode DROP DATABASE IF EXISTS name.
pub fn encode_drop_database(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP DATABASE IF EXISTS ");
    encode_identifier_maybe_quoted(buf, &cmd.table);
}

/// Encode GRANT privileges ON object TO role.
pub fn encode_grant(cmd: &Qail, buf: &mut BytesMut) -> Result<(), super::super::EncodeError> {
    let Some(role) = cmd.payload.as_deref() else {
        return Err(super::super::EncodeError::UnsupportedAction(Action::Grant));
    };

    let privs = privileges_to_sql(&cmd.columns)?;
    if cmd.table.trim().is_empty() || role.trim().is_empty() {
        return Err(super::super::EncodeError::UnsupportedAction(Action::Grant));
    }

    buf.extend_from_slice(b"GRANT ");
    buf.extend_from_slice(privs.as_bytes());
    buf.extend_from_slice(b" ON ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" TO ");
    push_identifier(buf, role);
    Ok(())
}

/// Encode REVOKE privileges ON object FROM role.
pub fn encode_revoke(cmd: &Qail, buf: &mut BytesMut) -> Result<(), super::super::EncodeError> {
    let Some(role) = cmd.payload.as_deref() else {
        return Err(super::super::EncodeError::UnsupportedAction(Action::Revoke));
    };

    let privs = privileges_to_sql(&cmd.columns)?;
    if cmd.table.trim().is_empty() || role.trim().is_empty() {
        return Err(super::super::EncodeError::UnsupportedAction(Action::Revoke));
    }

    buf.extend_from_slice(b"REVOKE ");
    buf.extend_from_slice(privs.as_bytes());
    buf.extend_from_slice(b" ON ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" FROM ");
    push_identifier(buf, role);
    Ok(())
}

fn privilege_to_sql(privilege: &str) -> Option<&'static str> {
    match privilege.trim().to_ascii_uppercase().as_str() {
        "SELECT" => Some("SELECT"),
        "INSERT" => Some("INSERT"),
        "UPDATE" => Some("UPDATE"),
        "DELETE" => Some("DELETE"),
        "TRUNCATE" => Some("TRUNCATE"),
        "REFERENCES" => Some("REFERENCES"),
        "TRIGGER" => Some("TRIGGER"),
        "USAGE" => Some("USAGE"),
        "CREATE" => Some("CREATE"),
        "CONNECT" => Some("CONNECT"),
        "TEMP" | "TEMPORARY" => Some("TEMPORARY"),
        "EXECUTE" => Some("EXECUTE"),
        "ALL" | "ALL PRIVILEGES" => Some("ALL PRIVILEGES"),
        _ => None,
    }
}

fn privileges_to_sql(columns: &[Expr]) -> Result<String, crate::protocol::EncodeError> {
    if columns.is_empty() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "privilege list cannot be empty".to_string(),
        ));
    }

    let mut privileges = Vec::with_capacity(columns.len());
    for column in columns {
        match column {
            Expr::Named(privilege) => {
                let Some(sql) = privilege_to_sql(privilege) else {
                    return Err(crate::protocol::EncodeError::InvalidAst(format!(
                        "invalid privilege: {privilege:?}"
                    )));
                };
                privileges.push(sql);
            }
            _ => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "privileges must be named expressions".to_string(),
                ));
            }
        }
    }

    Ok(privileges.join(", "))
}

/// Encode CREATE POLICY statement.
pub fn encode_create_policy(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    let Some(policy) = &cmd.policy_def else {
        return Err(super::super::EncodeError::UnsupportedAction(
            Action::CreatePolicy,
        ));
    };

    buf.extend_from_slice(b"CREATE POLICY ");
    push_identifier(buf, &policy.name);
    buf.extend_from_slice(b" ON ");
    push_identifier(buf, &policy.table);

    if policy.permissiveness == PolicyPermissiveness::Restrictive {
        buf.extend_from_slice(b" AS RESTRICTIVE");
    }

    let target = match policy.target {
        PolicyTarget::All => "ALL",
        PolicyTarget::Select => "SELECT",
        PolicyTarget::Insert => "INSERT",
        PolicyTarget::Update => "UPDATE",
        PolicyTarget::Delete => "DELETE",
    };
    buf.extend_from_slice(b" FOR ");
    buf.extend_from_slice(target.as_bytes());

    if let Some(role) = &policy.role {
        buf.extend_from_slice(b" TO ");
        push_identifier(buf, role);
    }

    if let Some(expr) = &policy.using {
        let expr = checked_sql_expr_fragment(&expr.to_string(), "policy expression")?;
        buf.extend_from_slice(b" USING (");
        buf.extend_from_slice(expr.as_bytes());
        buf.extend_from_slice(b")");
    }
    if let Some(expr) = &policy.with_check {
        let expr = checked_sql_expr_fragment(&expr.to_string(), "policy expression")?;
        buf.extend_from_slice(b" WITH CHECK (");
        buf.extend_from_slice(expr.as_bytes());
        buf.extend_from_slice(b")");
    }

    Ok(())
}

/// Encode DROP POLICY statement.
///
/// Expects table in `cmd.table` and policy name in `cmd.payload` (or `cmd.policy_def`).
pub fn encode_drop_policy(cmd: &Qail, buf: &mut BytesMut) -> Result<(), super::super::EncodeError> {
    let (policy_name, table_name) = if let Some(policy) = &cmd.policy_def {
        (policy.name.as_str(), policy.table.as_str())
    } else if let Some(name) = cmd.payload.as_deref() {
        (name, cmd.table.as_str())
    } else {
        return Err(super::super::EncodeError::UnsupportedAction(
            Action::DropPolicy,
        ));
    };

    if policy_name.trim().is_empty() || table_name.trim().is_empty() {
        return Err(super::super::EncodeError::UnsupportedAction(
            Action::DropPolicy,
        ));
    }

    buf.extend_from_slice(b"DROP POLICY IF EXISTS ");
    push_identifier(buf, policy_name);
    buf.extend_from_slice(b" ON ");
    push_identifier(buf, table_name);
    Ok(())
}

/// Encode CREATE FUNCTION statement.
pub fn encode_create_function(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    let Some(func) = &cmd.function_def else {
        return Err(super::super::EncodeError::UnsupportedAction(
            Action::CreateFunction,
        ));
    };

    let lang = func.language.as_deref().unwrap_or("plpgsql");
    let args = function_args_to_sql(&func.args)?;
    if !is_safe_sql_type_fragment(&func.returns) {
        return Err(crate::protocol::EncodeError::InvalidAst(format!(
            "invalid function return type: {:?}",
            func.returns
        )));
    }
    buf.extend_from_slice(b"CREATE OR REPLACE FUNCTION ");
    push_identifier(buf, &func.name);
    buf.extend_from_slice(b"(");
    buf.extend_from_slice(args.as_bytes());
    buf.extend_from_slice(b") RETURNS ");
    buf.extend_from_slice(func.returns.trim().as_bytes());
    buf.extend_from_slice(b" LANGUAGE ");
    buf.extend_from_slice(escape_identifier(lang).as_bytes());
    if let Some(volatility) = &func.volatility
        && !volatility.trim().is_empty()
    {
        let Some(volatility) = volatility_to_sql(volatility) else {
            return Err(crate::protocol::EncodeError::InvalidAst(format!(
                "invalid function volatility: {volatility:?}"
            )));
        };
        buf.extend_from_slice(b" ");
        buf.extend_from_slice(volatility.as_bytes());
    }
    buf.extend_from_slice(b" AS ");
    buf.extend_from_slice(dollar_quote_block(&func.body).as_bytes());
    Ok(())
}

/// Encode DROP FUNCTION statement.
pub fn encode_drop_function(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP FUNCTION IF EXISTS ");
    if let Some(signature) = &cmd.payload {
        buf.extend_from_slice(function_signature_to_sql(signature).as_bytes());
    } else {
        push_identifier(buf, &cmd.table);
        buf.extend_from_slice(b"()");
    }
}

/// Encode CREATE TRIGGER statement.
pub fn encode_create_trigger(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    let Some(trig) = &cmd.trigger_def else {
        return Err(super::super::EncodeError::UnsupportedAction(
            Action::CreateTrigger,
        ));
    };

    let timing = match trig.timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    };

    let mut first = true;
    let mut events = String::new();
    for evt in &trig.events {
        if !first {
            events.push_str(" OR ");
        }
        first = false;
        let evt_str = match evt {
            TriggerEvent::Insert => "INSERT",
            TriggerEvent::Update => {
                if !trig.update_columns.is_empty() {
                    events.push_str("UPDATE OF ");
                    for (idx, column) in trig.update_columns.iter().enumerate() {
                        if idx > 0 {
                            events.push_str(", ");
                        }
                        events.push_str(&escape_identifier(column));
                    }
                    continue;
                }
                "UPDATE"
            }
            TriggerEvent::Delete => "DELETE",
            TriggerEvent::Truncate => "TRUNCATE",
        };
        events.push_str(evt_str);
    }

    if events.is_empty() {
        return Err(super::super::EncodeError::UnsupportedAction(
            Action::CreateTrigger,
        ));
    }

    let for_each = if trig.for_each_row {
        "FOR EACH ROW"
    } else {
        "FOR EACH STATEMENT"
    };

    buf.extend_from_slice(b"CREATE TRIGGER ");
    push_identifier(buf, &trig.name);
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(timing.as_bytes());
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(events.as_bytes());
    buf.extend_from_slice(b" ON ");
    push_identifier(buf, &trig.table);
    buf.extend_from_slice(b" ");
    buf.extend_from_slice(for_each.as_bytes());
    buf.extend_from_slice(b" EXECUTE FUNCTION ");
    push_identifier(buf, &trig.execute_function);
    buf.extend_from_slice(b"()");
    Ok(())
}

/// Encode DROP TRIGGER statement.
///
/// Expects `cmd.table` in `table.trigger` form.
pub fn encode_drop_trigger(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    let Some((table, trigger)) = cmd.table.rsplit_once('.') else {
        return Err(super::super::EncodeError::UnsupportedAction(
            Action::DropTrigger,
        ));
    };
    buf.extend_from_slice(b"DROP TRIGGER IF EXISTS ");
    push_identifier(buf, trigger);
    buf.extend_from_slice(b" ON ");
    push_identifier(buf, table);
    Ok(())
}

/// Encode CREATE EXTENSION statement.
pub fn encode_create_extension(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    buf.extend_from_slice(b"CREATE EXTENSION IF NOT EXISTS ");
    buf.extend_from_slice(quote_double_string(&cmd.table).as_bytes());

    for col in &cmd.columns {
        match col {
            Expr::Named(opt) => {
                let Some(option) = extension_option_to_sql(opt) else {
                    return Err(crate::protocol::EncodeError::InvalidAst(format!(
                        "invalid extension option: {opt:?}"
                    )));
                };
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(option.as_bytes());
            }
            _ => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "extension options must be named expressions".to_string(),
                ));
            }
        }
    }

    Ok(())
}

/// Encode DROP EXTENSION statement.
pub fn encode_drop_extension(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP EXTENSION IF EXISTS ");
    buf.extend_from_slice(quote_double_string(&cmd.table).as_bytes());
}

/// Encode COMMENT ON TABLE/COLUMN statement.
pub fn encode_comment_on(cmd: &Qail, buf: &mut BytesMut) {
    let comment_text = cmd
        .columns
        .first()
        .and_then(|c| match c {
            Expr::Named(s) => Some(s.as_str()),
            _ => None,
        })
        .unwrap_or("");
    let escaped = escape_sql_string_literal(comment_text);
    let target = comment_target_to_sql(&cmd.table);

    buf.extend_from_slice(b"COMMENT ON ");
    buf.extend_from_slice(target.as_bytes());
    buf.extend_from_slice(b" IS '");
    buf.extend_from_slice(escaped.as_bytes());
    buf.extend_from_slice(b"'");
}

fn is_explicit_comment_target(trimmed: &str) -> bool {
    let upper = trimmed.to_ascii_uppercase();
    upper.starts_with("TABLE ")
        || upper.starts_with("COLUMN ")
        || upper.starts_with("FUNCTION ")
        || upper.starts_with("TYPE ")
        || upper.starts_with("POLICY ")
        || upper.starts_with("CONSTRAINT ")
        || upper.starts_with("INDEX ")
        || upper.starts_with("SEQUENCE ")
        || upper.starts_with("VIEW ")
        || upper.starts_with("MATERIALIZED VIEW ")
        || upper.starts_with("SCHEMA ")
}

fn comment_target_to_sql(target: &str) -> String {
    let trimmed = target.trim();
    if is_explicit_comment_target(trimmed) {
        if contains_unquoted_statement_delimiter(trimmed) {
            format!("TABLE {}", escape_identifier(trimmed))
        } else {
            trimmed.to_string()
        }
    } else if trimmed.contains('.') {
        let mut parts = trimmed.splitn(2, '.');
        let table = parts.next().unwrap_or_default();
        let col = parts.next().unwrap_or_default();
        format!(
            "COLUMN {}.{}",
            escape_identifier(table),
            escape_identifier(col)
        )
    } else {
        format!("TABLE {}", escape_identifier(trimmed))
    }
}

/// Encode CREATE SEQUENCE statement.
pub fn encode_create_sequence(
    cmd: &Qail,
    buf: &mut BytesMut,
) -> Result<(), super::super::EncodeError> {
    buf.extend_from_slice(b"CREATE SEQUENCE ");
    push_identifier(buf, &cmd.table);

    for col in &cmd.columns {
        match col {
            Expr::Named(opt) => {
                let Some(option) = sequence_option_to_sql(opt) else {
                    return Err(crate::protocol::EncodeError::InvalidAst(format!(
                        "invalid sequence option: {opt:?}"
                    )));
                };
                buf.extend_from_slice(b" ");
                buf.extend_from_slice(option.as_bytes());
            }
            _ => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "sequence options must be named expressions".to_string(),
                ));
            }
        }
    }

    Ok(())
}

/// Encode DROP SEQUENCE statement.
pub fn encode_drop_sequence(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP SEQUENCE IF EXISTS ");
    push_identifier(buf, &cmd.table);
}

/// Encode CREATE TYPE ... AS ENUM statement.
pub fn encode_create_enum(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"CREATE TYPE ");
    push_identifier(buf, &cmd.table);
    buf.extend_from_slice(b" AS ENUM (");

    let mut first = true;
    for col in &cmd.columns {
        if let Expr::Named(val) = col {
            if !first {
                buf.extend_from_slice(b", ");
            }
            first = false;
            buf.extend_from_slice(b"'");
            buf.extend_from_slice(escape_sql_string_literal(val).as_bytes());
            buf.extend_from_slice(b"'");
        }
    }

    buf.extend_from_slice(b")");
}

/// Encode DROP TYPE statement.
pub fn encode_drop_enum(cmd: &Qail, buf: &mut BytesMut) {
    buf.extend_from_slice(b"DROP TYPE IF EXISTS ");
    push_identifier(buf, &cmd.table);
}

/// Encode ALTER TYPE ... ADD VALUE IF NOT EXISTS statement(s).
pub fn encode_alter_enum_add_value(cmd: &Qail, buf: &mut BytesMut) {
    let mut first = true;
    for col in &cmd.columns {
        if let Expr::Named(val) = col {
            if !first {
                buf.extend_from_slice(b"; ");
            }
            first = false;
            buf.extend_from_slice(b"ALTER TYPE ");
            push_identifier(buf, &cmd.table);
            buf.extend_from_slice(b" ADD VALUE IF NOT EXISTS '");
            buf.extend_from_slice(escape_sql_string_literal(val).as_bytes());
            buf.extend_from_slice(b"'");
        }
    }
}

// ── Pub/Sub commands ───────────────────────────────────────────────

/// Encode LISTEN "channel".
pub fn encode_listen(cmd: &Qail, buf: &mut BytesMut) {
    let channel = cmd.channel.as_deref().unwrap_or("");
    buf.extend_from_slice(b"LISTEN \"");
    // Escape double-quotes in channel name
    buf.extend_from_slice(channel.replace('"', "\"\"").as_bytes());
    buf.extend_from_slice(b"\"");
}

/// Encode UNLISTEN "channel".
pub fn encode_unlisten(cmd: &Qail, buf: &mut BytesMut) {
    let channel = cmd.channel.as_deref().unwrap_or("");
    buf.extend_from_slice(b"UNLISTEN \"");
    buf.extend_from_slice(channel.replace('"', "\"\"").as_bytes());
    buf.extend_from_slice(b"\"");
}

/// Encode NOTIFY "channel", 'payload'.
pub fn encode_notify(cmd: &Qail, buf: &mut BytesMut) {
    let channel = cmd.channel.as_deref().unwrap_or("");
    buf.extend_from_slice(b"NOTIFY \"");
    buf.extend_from_slice(channel.replace('"', "\"\"").as_bytes());
    buf.extend_from_slice(b"\"");
    if let Some(ref payload) = cmd.payload {
        buf.extend_from_slice(b", '");
        buf.extend_from_slice(escape_sql_string_literal(payload).as_bytes());
        buf.extend_from_slice(b"'");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_alter_add_column_renders_check_constraint() {
        let cmd = Qail {
            action: Action::Alter,
            table: "players".to_string(),
            columns: vec![Expr::Def {
                name: "score".to_string(),
                data_type: "int".to_string(),
                constraints: vec![Constraint::Check(vec!["score >= 0".to_string()])],
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();

        encode_alter_add_column(&cmd, &mut buf).unwrap();

        let sql = String::from_utf8(buf.to_vec()).expect("encoded SQL should be UTF-8");
        assert!(
            sql.contains("CHECK (score >= 0)"),
            "add-column SQL should preserve CHECK constraint, got: {sql}"
        );
    }

    #[test]
    fn encode_alter_add_column_rejects_invalid_default_expression() {
        let cmd = Qail {
            action: Action::Alter,
            table: "players".to_string(),
            columns: vec![Expr::Def {
                name: "score".to_string(),
                data_type: "int".to_string(),
                constraints: vec![Constraint::Default("0; DROP TABLE users; --".to_string())],
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();

        let err = encode_alter_add_column(&cmd, &mut buf)
            .expect_err("unsafe add-column default must fail");

        assert!(
            matches!(&err, crate::protocol::EncodeError::InvalidAst(message) if message.contains("column default expression")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn encode_alter_add_column_renders_unique_constraint() {
        let cmd = Qail {
            action: Action::Alter,
            table: "users".to_string(),
            columns: vec![Expr::Def {
                name: "email".to_string(),
                data_type: "text".to_string(),
                constraints: vec![Constraint::Unique],
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();

        encode_alter_add_column(&cmd, &mut buf).unwrap();

        let sql = String::from_utf8(buf.to_vec()).expect("encoded SQL should be UTF-8");
        assert!(
            sql.contains("UNIQUE"),
            "add-column SQL should preserve UNIQUE constraint, got: {sql}"
        );
    }
}
