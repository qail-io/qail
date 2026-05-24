use super::dialect::Dialect;
use super::traits::SqlGenerator;
use crate::ast::*;

fn quote_double_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\0', "").replace('"', "\"\""))
}

fn escape_single_string(value: &str) -> String {
    value.replace('\0', "").replace('\'', "''")
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

fn extension_option_to_sql(opt: &str, generator: &dyn SqlGenerator) -> Option<String> {
    let trimmed = opt.trim();
    let (keyword, rest) = trimmed.split_once(char::is_whitespace)?;
    let rest = rest.trim();
    if rest.is_empty() {
        return None;
    }

    match keyword.to_ascii_uppercase().as_str() {
        "SCHEMA" => Some(format!(
            "SCHEMA {}",
            generator.quote_identifier(strip_option_quotes(rest))
        )),
        "VERSION" => Some(format!(
            "VERSION '{}'",
            escape_single_string(strip_option_quotes(rest))
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

fn sequence_owned_by_to_sql(parts: &[&str], generator: &dyn SqlGenerator) -> Option<String> {
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
            .map(|part| generator.quote_identifier(part))
            .collect::<Vec<_>>()
            .join(".")
    ))
}

fn sequence_option_to_sql(opt: &str, generator: &dyn SqlGenerator) -> Option<String> {
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
        "owned_by" => sequence_owned_by_to_sql(&parts[1..], generator),
        "owned" if parts.len() >= 3 && parts[1].eq_ignore_ascii_case("by") => {
            sequence_owned_by_to_sql(&parts[2..], generator)
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

fn checked_sql_expr_fragment(expr: &str, context: &str) -> Result<String, String> {
    let expr = expr.trim();
    if expr.is_empty() || expr.contains('\0') || contains_unquoted_statement_delimiter(expr) {
        return Err(format!("/* ERROR: Invalid {context} */"));
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
        "hnsw" => Some("hnsw"),
        "ivfflat" | "ivf-flat" => Some("ivfflat"),
        _ => None,
    }
}

fn index_column_to_sql(column: &str, generator: &dyn SqlGenerator) -> Result<String, String> {
    let column = column.trim();
    if column.is_empty() || column.contains('\0') {
        return Err("/* ERROR: Invalid index column */".to_string());
    }
    if is_simple_identifier(column) {
        Ok(generator.quote_identifier(column))
    } else if contains_unquoted_statement_delimiter(column) {
        Ok(generator.quote_identifier(column))
    } else {
        Ok(column.to_string())
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

fn references_target_to_sql(target: &str, generator: &dyn SqlGenerator) -> String {
    let target = target.trim();
    if target.is_empty() || contains_statement_delimiter(target) {
        return generator.quote_identifier(target);
    }

    let Some((table, rest)) = target.split_once('(') else {
        return generator.quote_identifier(target);
    };
    let Some(close_idx) = rest.find(')') else {
        return generator.quote_identifier(target);
    };

    let table = table.trim();
    let columns = rest[..close_idx]
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>();
    if table.is_empty() || columns.is_empty() || columns.iter().any(|col| col.is_empty()) {
        return generator.quote_identifier(target);
    }

    let Some(tail) = reference_tail_to_sql(&rest[close_idx + 1..]) else {
        return generator.quote_identifier(target);
    };

    format!(
        "{}({}){}",
        generator.quote_identifier(table),
        columns
            .iter()
            .map(|col| generator.quote_identifier(col))
            .collect::<Vec<_>>()
            .join(", "),
        tail
    )
}

fn quoted_column_list(cols: &[String], generator: &dyn SqlGenerator) -> String {
    cols.iter()
        .map(|c| generator.quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ")
}

fn table_constraint_to_sql(constraint: &TableConstraint, generator: &dyn SqlGenerator) -> String {
    match constraint {
        TableConstraint::Unique(cols) => {
            format!("UNIQUE ({})", quoted_column_list(cols, generator))
        }
        TableConstraint::PrimaryKey(cols) => {
            format!("PRIMARY KEY ({})", quoted_column_list(cols, generator))
        }
        TableConstraint::ForeignKey {
            name,
            columns,
            ref_table,
            ref_columns,
        } => {
            let mut sql = String::new();
            if let Some(name) = name {
                sql.push_str("CONSTRAINT ");
                sql.push_str(&generator.quote_identifier(name));
                sql.push(' ');
            }
            sql.push_str("FOREIGN KEY (");
            sql.push_str(&quoted_column_list(columns, generator));
            sql.push_str(") REFERENCES ");
            sql.push_str(&generator.quote_identifier(ref_table));
            sql.push('(');
            sql.push_str(&quoted_column_list(ref_columns, generator));
            sql.push(')');
            sql
        }
    }
}

fn append_column_check_sql(
    out: &mut String,
    column_name: &str,
    vals: &[String],
    generator: &dyn SqlGenerator,
) -> Result<(), String> {
    if vals.len() == 1
        && vals[0]
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("CONSTRAINT ")
    {
        out.push(' ');
        if vals[0].contains('\0') || contains_unquoted_statement_delimiter(&vals[0]) {
            return Err(format!(
                "/* ERROR: Invalid column check constraint for {column_name} */"
            ));
        } else {
            out.push_str(&vals[0]);
        }
        return Ok(());
    }

    let looks_like_expr = vals.len() == 1
        || vals.iter().any(|v| {
            v.chars()
                .any(|c| c.is_whitespace() || matches!(c, '<' | '>' | '=' | '!' | '(' | ')'))
        });

    if looks_like_expr {
        let raw_check = checked_sql_expr_fragment(&vals.join(" "), "column check expression")?;
        out.push_str(&format!(" CHECK ({raw_check})"));
    } else {
        out.push_str(&format!(
            " CHECK ({} IN ({}))",
            generator.quote_identifier(column_name),
            vals.iter()
                .map(|v| format!("'{}'", escape_single_string(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(())
}

/// Generate CREATE TABLE SQL.
pub fn build_create_table(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::new();
    sql.push_str("CREATE TABLE ");
    sql.push_str(&generator.quote_identifier(&cmd.table));
    sql.push_str(" (\n");

    let composite_pk_columns: Vec<String> = cmd
        .columns
        .iter()
        .filter_map(|col| match col {
            Expr::Def {
                name, constraints, ..
            } if constraints.contains(&Constraint::PrimaryKey) => Some(name.clone()),
            _ => None,
        })
        .collect();
    let use_composite_pk = composite_pk_columns.len() > 1;

    let mut defs = Vec::new();
    for col in &cmd.columns {
        if let Expr::Def {
            name,
            data_type,
            constraints,
        } = col
        {
            let sql_type = data_type_to_sql(data_type);
            let mut line = format!("    {} {}", generator.quote_identifier(name), sql_type);

            // Default to NOT NULL unless Nullable (?) constraint is present
            let is_nullable = constraints.contains(&Constraint::Nullable);
            if !is_nullable {
                line.push_str(" NOT NULL");
            }

            for constraint in constraints {
                if let Constraint::Default(val) = constraint {
                    line.push_str(" DEFAULT ");
                    // Map common functions to SQL equivalents
                    let sql_default = match val.as_str() {
                        "uuid()" => "gen_random_uuid()".to_string(),
                        "now()" => "NOW()".to_string(),
                        other => {
                            match checked_sql_expr_fragment(other, "column default expression") {
                                Ok(expr) => expr,
                                Err(err) => return err,
                            }
                        }
                    };
                    line.push_str(&sql_default);
                }
                if let Constraint::Generated(generation) = constraint {
                    match generation {
                        ColumnGeneration::Stored(expr) if expr == "identity" => {
                            line.push_str(" GENERATED ALWAYS AS IDENTITY");
                        }
                        ColumnGeneration::Stored(expr) if expr == "identity_by_default" => {
                            line.push_str(" GENERATED BY DEFAULT AS IDENTITY");
                        }
                        ColumnGeneration::Stored(expr) => {
                            let expr = match checked_sql_expr_fragment(
                                expr,
                                "generated column expression",
                            ) {
                                Ok(expr) => expr,
                                Err(err) => return err,
                            };
                            line.push_str(&format!(" GENERATED ALWAYS AS ({}) STORED", expr));
                        }
                        ColumnGeneration::Virtual(expr) => {
                            let expr = match checked_sql_expr_fragment(
                                expr,
                                "generated column expression",
                            ) {
                                Ok(expr) => expr,
                                Err(err) => return err,
                            };
                            line.push_str(&format!(" GENERATED ALWAYS AS ({})", expr));
                        }
                    }
                }
            }

            if constraints.contains(&Constraint::PrimaryKey) && !use_composite_pk {
                line.push_str(" PRIMARY KEY");
            }
            if constraints.contains(&Constraint::Unique) {
                line.push_str(" UNIQUE");
            }

            for constraint in constraints {
                if let Constraint::Check(vals) = constraint {
                    if let Err(err) =
                        append_column_check_sql(&mut line, name, vals, generator.as_ref())
                    {
                        return err;
                    }
                }
                if let Constraint::References(target) = constraint {
                    line.push_str(" REFERENCES ");
                    line.push_str(&references_target_to_sql(target, generator.as_ref()));
                }
            }

            defs.push(line);
        }
    }

    if use_composite_pk {
        let cols = composite_pk_columns
            .iter()
            .map(|c| generator.quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
        defs.push(format!("    PRIMARY KEY ({cols})"));
    }

    for tc in &cmd.table_constraints {
        defs.push(format!(
            "    {}",
            table_constraint_to_sql(tc, generator.as_ref())
        ));
    }

    sql.push_str(&defs.join(",\n"));
    sql.push_str("\n)");

    let mut comments = Vec::new();
    for col in &cmd.columns {
        if let Expr::Def {
            name, constraints, ..
        } = col
        {
            for c in constraints {
                if let Constraint::Comment(text) = c {
                    comments.push(format!(
                        "COMMENT ON COLUMN {}.{} IS '{}'",
                        generator.quote_identifier(&cmd.table),
                        generator.quote_identifier(name),
                        text.replace('\'', "''")
                    ));
                }
            }
        }
    }
    if !comments.is_empty() {
        sql.push_str(";\n");
        sql.push_str(&comments.join(";\n"));
    }

    sql
}

/// Generate ALTER TABLE SQL.
pub fn build_alter_table(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut stmts = Vec::new();
    let table_name = generator.quote_identifier(&cmd.table);

    for col in &cmd.columns {
        match col {
            Expr::Mod { kind, col } => match kind {
                ModKind::Add => {
                    if let Expr::Def {
                        name,
                        data_type,
                        constraints,
                    } = col.as_ref()
                    {
                        let sql_type = data_type_to_sql(data_type);
                        let mut line = format!(
                            "ALTER TABLE {} ADD COLUMN {} {}",
                            table_name,
                            generator.quote_identifier(name),
                            sql_type
                        );

                        let is_nullable = constraints.contains(&Constraint::Nullable);
                        if !is_nullable {
                            line.push_str(" NOT NULL");
                        }

                        if constraints.contains(&Constraint::Unique) {
                            line.push_str(" UNIQUE");
                        }
                        stmts.push(line);
                    }
                }
                ModKind::Drop => {
                    if let Expr::Named(name) = col.as_ref() {
                        stmts.push(format!(
                            "ALTER TABLE {} DROP COLUMN {}",
                            table_name,
                            generator.quote_identifier(name)
                        ));
                    }
                }
            },
            Expr::Named(rename_expr) if rename_expr.contains(" -> ") => {
                let parts: Vec<&str> = rename_expr.split(" -> ").collect();
                if parts.len() == 2 {
                    let old_name = parts[0].trim();
                    let new_name = parts[1].trim();
                    stmts.push(format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        table_name,
                        generator.quote_identifier(old_name),
                        generator.quote_identifier(new_name)
                    ));
                }
            }
            _ => {}
        }
    }
    stmts.join(";\n")
}

/// Generate CREATE INDEX SQL.
pub fn build_create_index(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    match &cmd.index_def {
        Some(idx) => {
            if idx.columns.is_empty() {
                return "/* ERROR: CREATE INDEX requires at least one column */".to_string();
            }
            let unique = if idx.unique { "UNIQUE " } else { "" };
            let mut cols = Vec::with_capacity(idx.columns.len());
            for column in &idx.columns {
                let Ok(column) = index_column_to_sql(column, generator.as_ref()) else {
                    return "/* ERROR: Invalid index column */".to_string();
                };
                cols.push(column);
            }
            let cols = cols.join(", ");
            let mut sql = format!(
                "CREATE {}INDEX {} ON {}",
                unique,
                generator.quote_identifier(&idx.name),
                generator.quote_identifier(&idx.table)
            );
            if let Some(method) = &idx.index_type
                && !method.trim().is_empty()
            {
                let Some(method) = index_method_to_sql(method) else {
                    return "/* ERROR: Invalid index method */".to_string();
                };
                sql.push_str(" USING ");
                sql.push_str(method);
            }
            sql.push_str(" (");
            sql.push_str(&cols);
            sql.push(')');
            if let Some(where_clause) = &idx.where_clause {
                if where_clause.trim().is_empty()
                    || where_clause.contains('\0')
                    || contains_unquoted_statement_delimiter(where_clause)
                {
                    return "/* ERROR: Invalid index predicate */".to_string();
                }
                sql.push_str(" WHERE ");
                sql.push_str(where_clause.trim());
            }
            sql
        }
        None => "/* ERROR: CREATE INDEX requires an index definition */".to_string(),
    }
}

fn map_type(t: &str) -> &str {
    match t {
        "str" | "text" | "string" => "VARCHAR(255)",
        "int" | "i32" => "INT",
        "bigint" | "i64" => "BIGINT",
        "uuid" => "UUID",
        "bool" | "boolean" => "BOOLEAN",
        "dec" | "decimal" => "DECIMAL",
        "float" | "f64" => "DOUBLE PRECISION",
        "serial" => "SERIAL",
        "timestamp" | "time" => "TIMESTAMP",
        "json" | "jsonb" => "JSONB",
        _ => t,
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

fn data_type_to_sql(t: &str) -> String {
    let mapped = map_type(t);
    if mapped != t {
        mapped.to_string()
    } else {
        sql_type_fragment_to_sql(t, "TEXT")
    }
}

fn is_simple_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

/// Generate ALTER COLUMN SQL (drop or rename column).
pub fn build_alter_column(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let table = generator.quote_identifier(&cmd.table);

    // Identified columns (target column)
    let cols: Vec<String> = cmd
        .columns
        .iter()
        .filter_map(|c| match c {
            Expr::Named(n) => Some(n.clone()),
            _ => None,
        })
        .collect();

    if cols.is_empty() {
        return "/* ERROR: Column required */".to_string();
    }
    let col_name = &cols[0];
    let quoted_col = generator.quote_identifier(col_name);

    match cmd.action {
        Action::DropCol => {
            format!("ALTER TABLE {} DROP COLUMN {}", table, quoted_col)
        }
        Action::RenameCol => {
            // Find "to" or "new" in cages
            // Syntax: rename::users:old[to=new]
            let new_name_opt = cmd
                .cages
                .iter()
                .flat_map(|c| &c.conditions)
                .find(|c| {
                    let col = match &c.left {
                        Expr::Named(n) => n.as_str(),
                        _ => "",
                    };
                    matches!(col, "to" | "new" | "rename")
                })
                .map(|c| match &c.value {
                    Value::String(s) => s.clone(),
                    Value::Param(_) => "PARAM".to_string(), // unsupported
                    _ => c.value.to_string(),
                });

            if let Some(new_name) = new_name_opt {
                let quoted_new = generator.quote_identifier(&new_name);
                format!(
                    "ALTER TABLE {} RENAME COLUMN {} TO {}",
                    table, quoted_col, quoted_new
                )
            } else {
                "/* ERROR: New name required (e.g. [to=new_name]) */".to_string()
            }
        }
        _ => "/* ERROR: Unknown Column Action */".to_string(),
    }
}

/// Generate ALTER TABLE ADD COLUMN SQL (for migrations).
pub fn build_alter_add_column(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let table = generator.quote_identifier(&cmd.table);

    let mut parts = Vec::new();

    for col in &cmd.columns {
        let Expr::Def {
            name,
            data_type,
            constraints,
        } = col
        else {
            return "/* ERROR: Invalid ALTER ADD column */".to_string();
        };
        let sql_type = data_type_to_sql(data_type);
        let quoted_name = generator.quote_identifier(name);

        let mut col_def = format!("{} {}", quoted_name, sql_type);

        let is_nullable = constraints.contains(&Constraint::Nullable);
        if !is_nullable {
            col_def.push_str(" NOT NULL");
        }
        if constraints.contains(&Constraint::Unique) {
            col_def.push_str(" UNIQUE");
        }

        for constraint in constraints {
            if let Constraint::Default(val) = constraint {
                col_def.push_str(" DEFAULT ");
                let sql_default = match val.as_str() {
                    "uuid()" => "gen_random_uuid()".to_string(),
                    "now()" => "NOW()".to_string(),
                    other => match checked_sql_expr_fragment(other, "column default expression") {
                        Ok(expr) => expr,
                        Err(err) => return err,
                    },
                };
                col_def.push_str(&sql_default);
            }
            if let Constraint::Generated(generation) = constraint {
                match generation {
                    ColumnGeneration::Stored(expr) if expr == "identity" => {
                        col_def.push_str(" GENERATED ALWAYS AS IDENTITY");
                    }
                    ColumnGeneration::Stored(expr) if expr == "identity_by_default" => {
                        col_def.push_str(" GENERATED BY DEFAULT AS IDENTITY");
                    }
                    ColumnGeneration::Stored(expr) => {
                        let expr =
                            match checked_sql_expr_fragment(expr, "generated column expression") {
                                Ok(expr) => expr,
                                Err(err) => return err,
                            };
                        col_def.push_str(&format!(" GENERATED ALWAYS AS ({}) STORED", expr));
                    }
                    ColumnGeneration::Virtual(expr) => {
                        let expr =
                            match checked_sql_expr_fragment(expr, "generated column expression") {
                                Ok(expr) => expr,
                                Err(err) => return err,
                            };
                        col_def.push_str(&format!(" GENERATED ALWAYS AS ({})", expr));
                    }
                }
            }
            if let Constraint::References(target) = constraint {
                col_def.push_str(" REFERENCES ");
                col_def.push_str(&references_target_to_sql(target, generator.as_ref()));
            }
            if let Constraint::Check(vals) = constraint {
                if let Err(err) =
                    append_column_check_sql(&mut col_def, name, vals, generator.as_ref())
                {
                    return err;
                }
            }
        }

        parts.push(format!("ALTER TABLE {} ADD COLUMN {}", table, col_def));
    }
    for constraint in &cmd.table_constraints {
        parts.push(format!(
            "ALTER TABLE {} ADD {}",
            table,
            table_constraint_to_sql(constraint, generator.as_ref())
        ));
    }

    if parts.is_empty() {
        "/* ERROR: ALTER ADD requires a column or table constraint */".to_string()
    } else {
        parts.join(";\n")
    }
}

/// Generate ALTER TABLE DROP COLUMN SQL (for migrations).
pub fn build_alter_drop_column(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let table = generator.quote_identifier(&cmd.table);

    let mut parts = Vec::new();

    for col in &cmd.columns {
        let col_name = match col {
            Expr::Named(n) => n.clone(),
            Expr::Def { name, .. } => name.clone(),
            _ => return "/* ERROR: Invalid ALTER DROP column */".to_string(),
        };

        let quoted_col = generator.quote_identifier(&col_name);
        parts.push(format!("ALTER TABLE {} DROP COLUMN {}", table, quoted_col));
    }

    if parts.is_empty() {
        "/* ERROR: ALTER DROP requires at least one column */".to_string()
    } else {
        parts.join(";\n")
    }
}

/// Generate ALTER TABLE ALTER COLUMN TYPE SQL (for migrations).
pub fn build_alter_column_type(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let table = generator.quote_identifier(&cmd.table);

    let mut parts = Vec::new();

    for col in &cmd.columns {
        let (col_name, new_type) = match col {
            Expr::Def {
                name, data_type, ..
            } => (name.clone(), data_type.clone()),
            _ => return "/* ERROR: Invalid ALTER TYPE column */".to_string(),
        };

        let quoted_col = generator.quote_identifier(&col_name);
        parts.push(format!(
            "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
            table,
            quoted_col,
            data_type_to_sql(&new_type)
        ));
    }

    if parts.is_empty() {
        "/* ERROR: ALTER TYPE requires at least one column definition */".to_string()
    } else {
        parts.join(";\n")
    }
}

// ============================================================================
// Phase 7: Extensions, Comments, Sequences
// ============================================================================

/// Generate CREATE EXTENSION IF NOT EXISTS SQL.
pub fn build_create_extension(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();

    // table field holds extension name, columns[0] may hold schema, columns[1] may hold version
    let mut sql = format!(
        "CREATE EXTENSION IF NOT EXISTS {}",
        quote_double_string(&cmd.table)
    );

    for col in &cmd.columns {
        match col {
            Expr::Named(val) => {
                let Some(option) = extension_option_to_sql(val, generator.as_ref()) else {
                    return "/* ERROR: Invalid extension option */".to_string();
                };
                sql.push(' ');
                sql.push_str(&option);
            }
            _ => return "/* ERROR: Invalid extension option */".to_string(),
        }
    }

    sql
}

/// Generate DROP EXTENSION IF EXISTS SQL.
pub fn build_drop_extension(cmd: &Qail, _dialect: Dialect) -> String {
    format!(
        "DROP EXTENSION IF EXISTS {}",
        quote_double_string(&cmd.table)
    )
}

/// Generate COMMENT ON SQL.
pub fn build_comment_on(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();

    // table field holds the target: "TABLE tablename" or "COLUMN table.column"
    // columns[0] holds the comment text as Expr::Named
    let comment_text = cmd
        .columns
        .first()
        .map(|c| match c {
            Expr::Named(s) => s.clone(),
            _ => String::new(),
        })
        .unwrap_or_default();

    // Escape single quotes in comment text
    let escaped = escape_single_string(&comment_text);

    format!(
        "COMMENT ON {} IS '{}'",
        comment_target_to_sql(&cmd.table, generator.as_ref()),
        escaped
    )
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

fn comment_target_to_sql(target: &str, generator: &dyn SqlGenerator) -> String {
    let trimmed = target.trim();
    if is_explicit_comment_target(trimmed) {
        if contains_unquoted_statement_delimiter(trimmed) {
            format!("TABLE {}", generator.quote_identifier(trimmed))
        } else {
            trimmed.to_string()
        }
    } else if trimmed.contains('.') {
        let parts: Vec<&str> = trimmed.splitn(2, '.').collect();
        format!(
            "COLUMN {}.{}",
            generator.quote_identifier(parts[0]),
            generator.quote_identifier(parts[1])
        )
    } else {
        format!("TABLE {}", generator.quote_identifier(trimmed))
    }
}

/// Generate CREATE SEQUENCE SQL.
pub fn build_create_sequence(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = format!("CREATE SEQUENCE {}", generator.quote_identifier(&cmd.table));

    for col in &cmd.columns {
        match col {
            Expr::Named(opt) => {
                let Some(option) = sequence_option_to_sql(opt, generator.as_ref()) else {
                    return "/* ERROR: Invalid sequence option */".to_string();
                };
                sql.push(' ');
                sql.push_str(&option);
            }
            _ => return "/* ERROR: Invalid sequence option */".to_string(),
        }
    }

    sql
}

/// Generate DROP SEQUENCE SQL.
pub fn build_drop_sequence(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    format!(
        "DROP SEQUENCE IF EXISTS {}",
        generator.quote_identifier(&cmd.table)
    )
}

/// Generate `CREATE TYPE ... AS ENUM` SQL from an AST command.
///
/// Enum values are taken from `cmd.columns` (each as a `Named` expression).
///
/// # Arguments
///
/// * `cmd` — Qail AST command whose `table` is the type name and `columns` are enum values.
/// * `dialect` — Target SQL dialect for identifier quoting.
pub fn build_create_enum(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let values: Vec<String> = cmd
        .columns
        .iter()
        .filter_map(|c| match c {
            Expr::Named(v) => Some(format!("'{}'", v.replace('\'', "''"))),
            _ => None,
        })
        .collect();

    format!(
        "CREATE TYPE {} AS ENUM ({})",
        generator.quote_identifier(&cmd.table),
        values.join(", ")
    )
}

/// Generate DROP TYPE SQL.
pub fn build_drop_enum(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    format!(
        "DROP TYPE IF EXISTS {}",
        generator.quote_identifier(&cmd.table)
    )
}

/// Generate `ALTER TYPE ... ADD VALUE` SQL for one or more new enum values.
///
/// Uses `IF NOT EXISTS` to be idempotent.
///
/// # Arguments
///
/// * `cmd` — Qail AST command whose `table` is the type name and `columns` are the new values.
/// * `dialect` — Target SQL dialect for identifier quoting.
pub fn build_alter_enum_add_value(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut parts = Vec::new();

    for col in &cmd.columns {
        if let Expr::Named(val) = col {
            parts.push(format!(
                "ALTER TYPE {} ADD VALUE IF NOT EXISTS '{}'",
                generator.quote_identifier(&cmd.table),
                val.replace('\'', "''")
            ));
        }
    }

    parts.join(";\n")
}
