//! QAIL Schema Parser
//!
//! Parses .qail text format into Schema AST.
//!
//! ## Grammar
//! ```text
//! schema = { table_def | index_def | migration_hint }*
//!
//! table_def = "table" IDENT "{" column_def* "}"
//! column_def = IDENT TYPE constraint*
//! constraint = "primary_key" | "not_null" | "nullable" | "unique" | "default" VALUE
//!
//! index_def = ["unique"] "index" IDENT "on" IDENT "(" IDENT+ ")"
//!
//! migration_hint = "rename" PATH "->" PATH
//!                | "transform" EXPR "->" PATH
//!                | "drop" PATH ["confirm"]
//! ```

use super::schema::{
    CheckConstraint, CheckExpr, Column, Comment, EnumType, Extension, FkAction, Grant, Index,
    MigrationHint, MultiColumnForeignKey, Privilege, ResourceDef, ResourceKind, Schema,
    SchemaFunctionDef, SchemaTriggerDef, Sequence, Table, ViewDef,
};
use super::policy::{RlsPolicy, PolicyTarget};
use super::types::ColumnType;
use crate::ast::Expr;
use std::collections::HashMap;

/// Parse a .qail file into a Schema.
pub fn parse_qail(input: &str) -> Result<Schema, String> {
    let mut schema = Schema::new();
    let mut lines = input.lines().peekable();

    while let Some(line) = lines.next() {
        let line = line.trim();

        // Skip empty lines, # comments, -- comments, and version directives
        if line.is_empty() || line.starts_with('#') || line.starts_with("--") {
            continue;
        }

        if line.starts_with("table ") {
            let (table, consumed) = parse_table(line, &mut lines, &schema.enums)?;
            schema.add_table(table);
            // consumed lines already processed
            let _ = consumed;
        }
        else if line.starts_with("unique index ") || line.starts_with("index ") {
            let index = parse_index(line)?;
            schema.add_index(index);
        }
        else if line.starts_with("extension ") {
            let ext = parse_extension(line)?;
            schema.add_extension(ext);
        }
        else if line.starts_with("comment ") {
            let comment = parse_comment(line)?;
            schema.add_comment(comment);
        }
        else if line.starts_with("sequence ") {
            let seq = parse_sequence(line, &mut lines)?;
            schema.add_sequence(seq);
        }
        else if line.starts_with("enum ") {
            let enum_type = parse_enum(line, &mut lines)?;
            schema.add_enum(enum_type);
        }
        else if line.starts_with("view ") || line.starts_with("materialized view ") {
            let view = parse_view(line, &mut lines)?;
            schema.add_view(view);
        }
        else if line.starts_with("function ") {
            let func = parse_function(line, &mut lines)?;
            schema.add_function(func);
        }
        else if line.starts_with("trigger ") {
            let trigger = parse_trigger(line)?;
            schema.add_trigger(trigger);
        }
        else if line.starts_with("grant ") || line.starts_with("revoke ") {
            let grant = parse_grant(line)?;
            schema.add_grant(grant);
        }
        else if line.starts_with("rename ") {
            let hint = parse_rename(line)?;
            schema.add_hint(hint);
        } else if line.starts_with("transform ") {
            let hint = parse_transform(line)?;
            schema.add_hint(hint);
        } else if line.starts_with("drop ") {
            let hint = parse_drop(line)?;
            schema.add_hint(hint);
        } else if line.starts_with("bucket ") {
            let res = parse_resource(line, &mut lines, ResourceKind::Bucket)?;
            schema.add_resource(res);
        } else if line.starts_with("queue ") {
            let res = parse_resource(line, &mut lines, ResourceKind::Queue)?;
            schema.add_resource(res);
        } else if line.starts_with("topic ") {
            let res = parse_resource(line, &mut lines, ResourceKind::Topic)?;
            schema.add_resource(res);
        } else if line.starts_with("policy ") {
            let policy = parse_policy(line, &mut lines)?;
            schema.add_policy(policy);
        } else {
            return Err(format!("Unknown statement: {}", line));
        }
    }

    Ok(schema)
}

/// Parse a table definition with columns.
fn parse_table<'a, I>(
    first_line: &str,
    lines: &mut std::iter::Peekable<I>,
    enum_types: &[EnumType],
) -> Result<(Table, usize), String>
where
    I: Iterator<Item = &'a str>,
{
    let rest = first_line.strip_prefix("table ").unwrap();
    let name = rest.trim_end_matches('{').trim().to_string();

    if name.is_empty() {
        return Err("Table name required".to_string());
    }

    let mut table = Table::new(&name);
    let mut consumed = 0;

    for line in lines.by_ref() {
        consumed += 1;
        let line = line.trim();

        if line == "}" || line.starts_with('}') {
            break;
        }

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Table-level multi-column foreign key
        if line.starts_with("foreign_key") {
            let fk = parse_multi_column_fk(line)?;
            table.multi_column_fks.push(fk);
            continue;
        }

        // Table-level RLS directives
        if line == "enable_rls" {
            table.enable_rls = true;
            continue;
        }
        if line == "force_rls" {
            table.force_rls = true;
            continue;
        }

        let col = parse_column(line, enum_types)?;
        table.columns.push(col);
    }

    Ok((table, consumed))
}

/// Parse a column definition.
fn parse_column(line: &str, enum_types: &[EnumType]) -> Result<Column, String> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.len() < 2 {
        return Err(format!("Invalid column: {}", line));
    }

    let name = parts[0].to_string();
    let type_str = parts[1];

    // Try standard type first, then check enum types
    let data_type: ColumnType = type_str.parse().unwrap_or_else(|_| {
        // Check if it's a known enum type
        if let Some(et) = enum_types.iter().find(|e| e.name == type_str) {
            ColumnType::Enum {
                name: et.name.clone(),
                values: et.values.clone(),
            }
        } else {
            ColumnType::Text // fallback
        }
    });

    let mut col = Column::new(&name, data_type);

    let mut i = 2;
    while i < parts.len() {
        match parts[i] {
            "primary_key" => {
                col.primary_key = true;
                col.nullable = false;
            }
            "not_null" => {
                col.nullable = false;
            }
            "nullable" => {
                col.nullable = true;
            }
            "unique" => {
                col.unique = true;
            }
            "default" => {
                if i + 1 < parts.len() {
                    col.default = Some(parts[i + 1].to_string());
                    i += 1;
                }
            }
            s if s.starts_with("references") => {
                let fk_str = if s.contains('(') {
                    // references is attached: "references users(id)"
                    s.strip_prefix("references").unwrap_or(s)
                } else if i + 1 < parts.len() {
                    // references is separate: "references" "users(id)"
                    i += 1;
                    parts[i]
                } else {
                    ""
                };

                if let Some(paren_start) = fk_str.find('(')
                    && let Some(paren_end) = fk_str.find(')')
                {
                    let table = &fk_str[..paren_start];
                    let column = &fk_str[paren_start + 1..paren_end];
                    col = col.references(table, column);
                }

                // Check for on_delete / on_update after references
                while i + 1 < parts.len() {
                    match parts[i + 1] {
                        "on_delete" if i + 2 < parts.len() => {
                            let action = parse_fk_action_str(parts[i + 2]);
                            col = col.on_delete(action);
                            i += 2;
                        }
                        "on_update" if i + 2 < parts.len() => {
                            let action = parse_fk_action_str(parts[i + 2]);
                            col = col.on_update(action);
                            i += 2;
                        }
                        _ => break,
                    }
                }
            }
            s if s.starts_with("check(") => {
                // Parse check(expr) — may span multiple parts if expression has spaces
                // Reconstruct the full check(...) from remaining parts
                let check_str = if s.ends_with(')') {
                    s.to_string()
                } else {
                    // Consume parts until we find the closing )
                    let mut full = s.to_string();
                    while i + 1 < parts.len() {
                        i += 1;
                        full.push(' ');
                        full.push_str(parts[i]);
                        if parts[i].ends_with(')') {
                            break;
                        }
                    }
                    full
                };
                // Strip "check(" and trailing ")"
                let inner = check_str
                    .strip_prefix("check(")
                    .and_then(|s| s.strip_suffix(')'))
                    .unwrap_or("");
                if !inner.is_empty()
                    && let Some(expr) = parse_check_expr_from_qail(inner) {
                        col.check = Some(CheckConstraint { expr, name: None });
                }
            }
            _ => {
                // Unknown constraint, might be part of default value
            }
        }
        i += 1;
    }

    Ok(col)
}

/// Parse an index definition.
fn parse_index(line: &str) -> Result<Index, String> {
    let is_unique = line.starts_with("unique ");
    let rest = if is_unique {
        line.strip_prefix("unique index ").unwrap()
    } else {
        line.strip_prefix("index ").unwrap()
    };

    let parts: Vec<&str> = rest.splitn(2, " on ").collect();
    if parts.len() != 2 {
        return Err(format!("Invalid index: {}", line));
    }

    let name = parts[0].trim().to_string();
    let rest = parts[1];

    let paren_start = rest.find('(').ok_or("Missing ( in index")?;
    let paren_end = rest.rfind(')').ok_or("Missing ) in index")?;

    let table = rest[..paren_start].trim().to_string();
    let cols_str = &rest[paren_start + 1..paren_end];
    let columns: Vec<String> = cols_str.split(',').map(|s| s.trim().to_string()).collect();

    // Detect expression indexes: columns contain parentheses like "(lower(email))"
    let has_expressions = columns.iter().any(|c| c.starts_with('(') || c.contains("("));

    let mut index = if has_expressions {
        Index::expression(&name, &table, columns)
    } else {
        Index::new(&name, &table, columns)
    };
    if is_unique {
        index.unique = true;
    }

    Ok(index)
}

/// Parse a rename hint.
fn parse_rename(line: &str) -> Result<MigrationHint, String> {
    // rename users.username -> users.name
    let rest = line.strip_prefix("rename ").unwrap();
    let parts: Vec<&str> = rest.split(" -> ").collect();

    if parts.len() != 2 {
        return Err(format!("Invalid rename: {}", line));
    }

    Ok(MigrationHint::Rename {
        from: parts[0].trim().to_string(),
        to: parts[1].trim().to_string(),
    })
}

/// Parse a transform hint.
fn parse_transform(line: &str) -> Result<MigrationHint, String> {
    // transform age * 12 -> age_months
    let rest = line.strip_prefix("transform ").unwrap();
    let parts: Vec<&str> = rest.split(" -> ").collect();

    if parts.len() != 2 {
        return Err(format!("Invalid transform: {}", line));
    }

    Ok(MigrationHint::Transform {
        expression: parts[0].trim().to_string(),
        target: parts[1].trim().to_string(),
    })
}

/// Parse a drop hint.
fn parse_drop(line: &str) -> Result<MigrationHint, String> {
    // drop temp_table confirm
    let rest = line.strip_prefix("drop ").unwrap();
    let confirmed = rest.ends_with(" confirm");
    let target = if confirmed {
        rest.strip_suffix(" confirm").unwrap().trim().to_string()
    } else {
        rest.trim().to_string()
    };

    Ok(MigrationHint::Drop { target, confirmed })
}

/// Parse an extension definition.
/// Syntax: `extension "uuid-ossp"` or `extension pgcrypto`
///         `extension "uuid-ossp" schema public version "1.1"`
fn parse_extension(line: &str) -> Result<Extension, String> {
    let rest = line.strip_prefix("extension ").unwrap().trim();
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in rest.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ' ' if !in_quotes => {
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }

    if parts.is_empty() {
        return Err("extension requires a name".to_string());
    }

    let mut ext = Extension::new(&parts[0]);
    let mut i = 1;
    while i < parts.len() {
        match parts[i].as_str() {
            "schema" if i + 1 < parts.len() => {
                ext = ext.schema(&parts[i + 1]);
                i += 2;
            }
            "version" if i + 1 < parts.len() => {
                ext = ext.version(&parts[i + 1]);
                i += 2;
            }
            _ => return Err(format!("Unknown extension option: {}", parts[i])),
        }
    }

    Ok(ext)
}

/// Parse a comment definition.
/// Syntax: `comment on users "User accounts table"`
///         `comment on users.email "Primary contact email"`
fn parse_comment(line: &str) -> Result<Comment, String> {
    let rest = line
        .strip_prefix("comment on ")
        .ok_or_else(|| "comment must use 'comment on <target> \"text\"'".to_string())?
        .trim();

    let quote_start = rest
        .find('"')
        .ok_or_else(|| "comment text must be quoted".to_string())?;
    let target_str = rest[..quote_start].trim();
    let text = rest[quote_start + 1..]
        .strip_suffix('"')
        .ok_or_else(|| "unterminated comment text".to_string())?
        .to_string();

    if target_str.contains('.') {
        let (table, column) = target_str
            .split_once('.')
            .ok_or_else(|| "invalid comment target".to_string())?;
        Ok(Comment::on_column(table, column, text))
    } else {
        Ok(Comment::on_table(target_str, text))
    }
}

/// Parse a sequence definition.
/// Single-line: `sequence order_number_seq`
/// Multi-line:  `sequence order_number_seq { start 1000 increment 1 cache 10 }`
fn parse_sequence<'a, I: Iterator<Item = &'a str>>(
    first_line: &str,
    lines: &mut std::iter::Peekable<I>,
) -> Result<Sequence, String> {
    let rest = first_line.strip_prefix("sequence ").unwrap().trim();

    if rest.contains('{') {
        let name = rest.split('{').next().unwrap().trim();
        let mut seq = Sequence::new(name);

        let mut tokens_str = rest.split('{').nth(1).unwrap_or("").to_string();

        if !tokens_str.contains('}') {
            for line in lines.by_ref() {
                let line = line.trim();
                tokens_str.push(' ');
                tokens_str.push_str(line);
                if line.contains('}') {
                    break;
                }
            }
        }

        let tokens_str = tokens_str.replace('}', "");
        let tokens: Vec<&str> = tokens_str.split_whitespace().collect();

        let mut i = 0;
        while i < tokens.len() {
            match tokens[i] {
                "start" if i + 1 < tokens.len() => {
                    seq.start = Some(tokens[i + 1].parse().map_err(|_| "invalid start value")?);
                    i += 2;
                }
                "increment" if i + 1 < tokens.len() => {
                    seq.increment =
                        Some(tokens[i + 1].parse().map_err(|_| "invalid increment value")?);
                    i += 2;
                }
                "minvalue" if i + 1 < tokens.len() => {
                    seq.min_value =
                        Some(tokens[i + 1].parse().map_err(|_| "invalid minvalue")?);
                    i += 2;
                }
                "maxvalue" if i + 1 < tokens.len() => {
                    seq.max_value =
                        Some(tokens[i + 1].parse().map_err(|_| "invalid maxvalue")?);
                    i += 2;
                }
                "cache" if i + 1 < tokens.len() => {
                    seq.cache = Some(tokens[i + 1].parse().map_err(|_| "invalid cache value")?);
                    i += 2;
                }
                "cycle" => {
                    seq.cycle = true;
                    i += 1;
                }
                "owned_by" if i + 1 < tokens.len() => {
                    seq.owned_by = Some(tokens[i + 1].to_string());
                    i += 2;
                }
                "as" if i + 1 < tokens.len() => {
                    seq.data_type = Some(tokens[i + 1].to_string());
                    i += 2;
                }
                _ => return Err(format!("Unknown sequence option: {}", tokens[i])),
            }
        }

        Ok(seq)
    } else {
        Ok(Sequence::new(rest))
    }
}

/// Parse a standalone ENUM type definition.
/// Syntax: `enum status { active, inactive, pending }`
///         or multi-line block
fn parse_enum<'a, I: Iterator<Item = &'a str>>(
    first_line: &str,
    lines: &mut std::iter::Peekable<I>,
) -> Result<EnumType, String> {
    let rest = first_line.strip_prefix("enum ").unwrap().trim();

    if rest.contains('{') {
        let name = rest.split('{').next().unwrap().trim();

        let mut values_str = rest.split('{').nth(1).unwrap_or("").to_string();

        if !values_str.contains('}') {
            for line in lines.by_ref() {
                let line = line.trim();
                values_str.push(' ');
                values_str.push_str(line);
                if line.contains('}') {
                    break;
                }
            }
        }

        let values_str = values_str.replace('}', "");
        let values: Vec<String> = values_str
            .split(',')
            .map(|s| s.trim().trim_matches('"').trim_matches('\'').to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if values.is_empty() {
            return Err(format!("enum '{}' must have at least one value", name));
        }

        Ok(EnumType::new(name, values))
    } else {
        Err("enum definition requires { values }".to_string())
    }
}

/// Parse a table-level multi-column foreign key.
/// Syntax: `foreign_key (a, b) references other_table(x, y)`
fn parse_multi_column_fk(line: &str) -> Result<MultiColumnForeignKey, String> {
    let rest = line
        .strip_prefix("foreign_key")
        .unwrap_or(line)
        .trim();

    // Extract local columns from (...)
    let local_start = rest.find('(').ok_or("foreign_key missing ( for columns")?;
    let local_end = rest.find(')').ok_or("foreign_key missing ) for columns")?;
    let local_cols: Vec<String> = rest[local_start + 1..local_end]
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    // After first ) find "references"
    let after_locals = rest[local_end + 1..].trim();
    let ref_part = after_locals
        .strip_prefix("references")
        .ok_or("foreign_key missing 'references' keyword")?
        .trim();

    // Extract ref table and ref columns from table(cols)
    let ref_paren_start = ref_part.find('(').ok_or("foreign_key ref missing (")?;
    let ref_paren_end = ref_part.find(')').ok_or("foreign_key ref missing )")?;

    let ref_table = ref_part[..ref_paren_start].trim().to_string();
    let ref_cols: Vec<String> = ref_part[ref_paren_start + 1..ref_paren_end]
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    Ok(MultiColumnForeignKey::new(local_cols, ref_table, ref_cols))
}

/// Parse a view definition.
/// Syntax: `view name $$ SELECT ... $$`
///     or: `materialized view name $$ SELECT ... $$`
///     or multi-line block
fn parse_view<'a, I: Iterator<Item = &'a str>>(
    first_line: &str,
    lines: &mut std::iter::Peekable<I>,
) -> Result<ViewDef, String> {
    let materialized = first_line.starts_with("materialized ");
    let rest = if materialized {
        first_line.strip_prefix("materialized view ").unwrap().trim()
    } else {
        first_line.strip_prefix("view ").unwrap().trim()
    };

    // Split name from body at $$
    if let Some(dollar_pos) = rest.find("$$") {
        let name = rest[..dollar_pos].trim();
        let mut body = rest[dollar_pos + 2..].to_string();

        if !body.contains("$$") {
            // Multi-line: read until closing $$
            for line in lines.by_ref() {
                if line.contains("$$") {
                    let before_closing = line.split("$$").next().unwrap_or("");
                    body.push('\n');
                    body.push_str(before_closing);
                    break;
                }
                body.push('\n');
                body.push_str(line);
            }
        } else {
            // Inline: strip closing $$
            body = body.replace("$$", "");
        }

        let mut view = ViewDef::new(name, body.trim());
        if materialized {
            view = view.materialized();
        }
        Ok(view)
    } else {
        Err("view body must be wrapped in $$...$$".to_string())
    }
}

/// Parse a function definition.
/// Syntax: `function name(args) returns type language lang $$ body $$`
fn parse_function<'a, I: Iterator<Item = &'a str>>(
    first_line: &str,
    lines: &mut std::iter::Peekable<I>,
) -> Result<SchemaFunctionDef, String> {
    let rest = first_line.strip_prefix("function ").unwrap().trim();

    // Extract name and args
    let paren_start = rest.find('(').ok_or("function missing (")?;
    let paren_end = rest.find(')').ok_or("function missing )")?;

    let name = rest[..paren_start].trim();
    let args_str = &rest[paren_start + 1..paren_end];
    let args: Vec<String> = if args_str.trim().is_empty() {
        Vec::new()
    } else {
        args_str.split(',').map(|s| s.trim().to_string()).collect()
    };

    let after_args = rest[paren_end + 1..].trim();

    // Parse returns/language
    let parts: Vec<&str> = after_args.split_whitespace().collect();
    let mut returns = "void".to_string();
    let mut language = "plpgsql".to_string();

    let mut i = 0;
    let mut body_start_idx = None;
    while i < parts.len() {
        if parts[i] == "returns" && i + 1 < parts.len() {
            returns = parts[i + 1].to_string();
            i += 2;
        } else if parts[i] == "language" && i + 1 < parts.len() {
            language = parts[i + 1].to_string();
            i += 2;
        } else if parts[i] == "$$" {
            body_start_idx = Some(i);
            break;
        } else {
            i += 1;
        }
    }

    // Extract body between $$ markers
    let body = if let Some(idx) = body_start_idx {
        let after_first_dollar = parts[idx + 1..].join(" ");
        let mut body_str = after_first_dollar;

        if !body_str.contains("$$") {
            for line in lines.by_ref() {
                if line.contains("$$") {
                    let before = line.split("$$").next().unwrap_or("");
                    body_str.push('\n');
                    body_str.push_str(before);
                    break;
                }
                body_str.push('\n');
                body_str.push_str(line);
            }
        } else {
            body_str = body_str.replace("$$", "");
        }

        body_str.trim().to_string()
    } else {
        return Err("function body must be wrapped in $$...$$".to_string());
    };

    let mut func = SchemaFunctionDef::new(name, &returns, body);
    func.language = language;
    func.args = args;

    Ok(func)
}

/// Parse a trigger definition.
/// Syntax: `trigger name on table before|after insert|update|delete execute function_name`
fn parse_trigger(line: &str) -> Result<SchemaTriggerDef, String> {
    let rest = line.strip_prefix("trigger ").unwrap().trim();
    let parts: Vec<&str> = rest.split_whitespace().collect();

    if parts.len() < 6 {
        return Err("trigger requires: name on table timing event execute func".to_string());
    }

    let name = parts[0];

    // Find "on" keyword
    let on_idx = parts
        .iter()
        .position(|&p| p == "on")
        .ok_or("trigger missing 'on' keyword")?;
    let table = parts.get(on_idx + 1).ok_or("trigger missing table name")?;

    let timing = parts.get(on_idx + 2).ok_or("trigger missing timing")?.to_uppercase();

    // Collect events (INSERT, UPDATE, DELETE, etc.) until "execute"
    let mut events = Vec::new();
    let mut exec_idx = None;
    for (j, part) in parts.iter().enumerate().skip(on_idx + 3) {
        if part.eq_ignore_ascii_case("execute") {
            exec_idx = Some(j);
            break;
        }
        let evt = part.to_uppercase();
        if evt != "OR" {
            events.push(evt);
        }
    }

    let exec_idx = exec_idx.ok_or("trigger missing 'execute' keyword")?;
    let func_name = parts
        .get(exec_idx + 1)
        .ok_or("trigger missing function name")?;

    let mut trigger = SchemaTriggerDef::new(name, *table, *func_name);
    trigger.timing = timing;
    trigger.events = events;

    Ok(trigger)
}

/// Parse GRANT/REVOKE.
/// Syntax: `grant select, insert on users to app_role`
///     or: `revoke all on users from public`
fn parse_grant(line: &str) -> Result<Grant, String> {
    let is_revoke = line.starts_with("revoke ");
    let rest = if is_revoke {
        line.strip_prefix("revoke ").unwrap()
    } else {
        line.strip_prefix("grant ").unwrap()
    }
    .trim();

    // Find "on" keyword
    let on_idx = rest.find(" on ").ok_or("grant/revoke missing 'on' keyword")?;
    let privs_str = &rest[..on_idx].trim();
    let after_on = rest[on_idx + 4..].trim();

    // Find "to" or "from" keyword
    let (obj_str, role_str) = if is_revoke {
        let from_idx = after_on
            .find(" from ")
            .ok_or("revoke missing 'from' keyword")?;
        (
            after_on[..from_idx].trim(),
            after_on[from_idx + 6..].trim(),
        )
    } else {
        let to_idx = after_on
            .find(" to ")
            .ok_or("grant missing 'to' keyword")?;
        (after_on[..to_idx].trim(), after_on[to_idx + 4..].trim())
    };

    let privileges: Vec<Privilege> = privs_str
        .split(',')
        .map(|s| match s.trim().to_uppercase().as_str() {
            "ALL" => Privilege::All,
            "SELECT" => Privilege::Select,
            "INSERT" => Privilege::Insert,
            "UPDATE" => Privilege::Update,
            "DELETE" => Privilege::Delete,
            "USAGE" => Privilege::Usage,
            "EXECUTE" => Privilege::Execute,
            _ => Privilege::All,
        })
        .collect();

    if is_revoke {
        Ok(Grant::revoke(privileges, obj_str, role_str))
    } else {
        Ok(Grant::new(privileges, obj_str, role_str))
    }
}

/// Parse QAIL FK action string to FkAction enum.
/// Accepts: cascade, set_null, set_default, restrict, no_action
fn parse_fk_action_str(s: &str) -> FkAction {
    match s {
        "cascade" => FkAction::Cascade,
        "set_null" => FkAction::SetNull,
        "set_default" => FkAction::SetDefault,
        "restrict" => FkAction::Restrict,
        _ => FkAction::NoAction,
    }
}

/// Parse a QAIL check expression string into a CheckExpr.
/// Supports:
///   "col >= 0"           → GreaterOrEqual
///   "col > 0"            → GreaterThan
///   "col <= 100"         → LessOrEqual
///   "col < 100"          → LessThan
///   "col between 0 200"  → Between
///   "col >= 0 and col <= 200" → And(GreaterOrEqual, LessOrEqual)
fn parse_check_expr_from_qail(s: &str) -> Option<CheckExpr> {
    let s = s.trim();

    // Try "col between low high"
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() == 4 && parts[1] == "between" {
        let col = parts[0].to_string();
        let low = parts[2].parse::<i64>().ok()?;
        let high = parts[3].parse::<i64>().ok()?;
        return Some(CheckExpr::Between { column: col, low, high });
    }

    // Try "left and right"
    if let Some(and_pos) = s.find(" and ") {
        let left = parse_check_expr_from_qail(&s[..and_pos])?;
        let right = parse_check_expr_from_qail(&s[and_pos + 5..])?;
        return Some(CheckExpr::And(Box::new(left), Box::new(right)));
    }

    // Try "left or right"
    if let Some(or_pos) = s.find(" or ") {
        let left = parse_check_expr_from_qail(&s[..or_pos])?;
        let right = parse_check_expr_from_qail(&s[or_pos + 4..])?;
        return Some(CheckExpr::Or(Box::new(left), Box::new(right)));
    }

    // Try simple comparisons: "col >= val", "col > val", etc.
    #[allow(clippy::type_complexity)]
    let ops: &[(&str, fn(String, i64) -> CheckExpr)] = &[
        (">=", |col, val| CheckExpr::GreaterOrEqual { column: col, value: val }),
        ("<=", |col, val| CheckExpr::LessOrEqual { column: col, value: val }),
        (">", |col, val| CheckExpr::GreaterThan { column: col, value: val }),
        ("<", |col, val| CheckExpr::LessThan { column: col, value: val }),
    ];

    for (op, constructor) in ops {
        if let Some(pos) = s.find(op) {
            let col = s[..pos].trim().to_string();
            let val = s[pos + op.len()..].trim().parse::<i64>().ok()?;
            return Some(constructor(col, val));
        }
    }

    // Try "length(col) >= min" / "length(col) <= max"
    if s.starts_with("length(") {
        let inner_end = s.find(')')?;
        let col = s[7..inner_end].to_string();
        let rest = s[inner_end + 1..].trim();
        if let Some(val_str) = rest.strip_prefix(">=") {
            let min = val_str.trim().parse::<usize>().ok()?;
            return Some(CheckExpr::MinLength { column: col, min });
        }
        if let Some(val_str) = rest.strip_prefix("<=") {
            let max = val_str.trim().parse::<usize>().ok()?;
            return Some(CheckExpr::MaxLength { column: col, max });
        }
    }

    // Try "col not_null"
    if parts.len() == 2 && parts[1] == "not_null" {
        return Some(CheckExpr::NotNull { column: parts[0].to_string() });
    }

    None
}

/// Parse an infrastructure resource declaration.
/// Supports single-line: `bucket avatars`
/// and multi-line block: `bucket avatars { provider s3 region "ap-southeast-1" }`
fn parse_resource<'a, I: Iterator<Item = &'a str>>(
    first_line: &str,
    lines: &mut std::iter::Peekable<I>,
    kind: ResourceKind,
) -> Result<ResourceDef, String> {
    let keyword = kind.to_string();
    let after_keyword = first_line
        .strip_prefix(&keyword)
        .ok_or_else(|| format!("Expected '{}' keyword", keyword))?
        .trim();

    // Extract name (first word after the keyword)
    let (name, rest) = match after_keyword.split_once(|c: char| c.is_whitespace() || c == '{') {
        Some((n, r)) => (n.trim(), r.trim()),
        None => (after_keyword.trim_end_matches('{'), ""),
    };

    if name.is_empty() {
        return Err(format!("Missing name for {} declaration", keyword));
    }

    let mut provider = None;
    let mut properties = HashMap::new();

    // Check if block is on the same line: `bucket avatars { provider s3 }`
    let has_block = first_line.contains('{');

    if has_block {
        // Collect content until closing brace
        let mut block_content = rest.trim_start_matches('{').to_string();

        // If no closing brace on same line, read until we find it
        if !block_content.contains('}') {
            for next_line in lines.by_ref() {
                let next_line = next_line.trim();
                if next_line == "}" || next_line.ends_with('}') {
                    let trimmed = next_line.trim_end_matches('}').trim();
                    if !trimmed.is_empty() {
                        block_content.push(' ');
                        block_content.push_str(trimmed);
                    }
                    break;
                }
                block_content.push(' ');
                block_content.push_str(next_line);
            }
        }

        // Parse key-value pairs from block content
        let content = block_content.trim_end_matches('}').trim();
        let mut tokens = content.split_whitespace().peekable();

        while let Some(key) = tokens.next() {
            if key.is_empty() || key == "}" {
                continue;
            }
            if let Some(value) = tokens.next() {
                let value = value.trim_matches('"').to_string();
                if key == "provider" {
                    provider = Some(value);
                } else {
                    properties.insert(key.to_string(), value);
                }
            }
        }
    }

    Ok(ResourceDef {
        name: name.to_string(),
        kind,
        provider,
        properties,
    })
}

/// Parse an RLS policy definition.
///
/// Syntax:
/// ```text
/// policy NAME on TABLE for TARGET
///   using $$ EXPR $$
///   with_check $$ EXPR $$
/// ```
///
/// Both `using` and `with_check` are optional. The `$$` delimiters may span
/// multiple lines (same pattern as views / functions).
fn parse_policy<'a, I: Iterator<Item = &'a str>>(
    first_line: &str,
    lines: &mut std::iter::Peekable<I>,
) -> Result<RlsPolicy, String> {
    // Parse header: "policy NAME on TABLE for TARGET"
    let rest = first_line.strip_prefix("policy ").unwrap().trim();
    let parts: Vec<&str> = rest.split_whitespace().collect();

    // Minimum: NAME on TABLE for TARGET  (4 tokens)
    if parts.len() < 4 {
        return Err(format!("Invalid policy: {}", first_line));
    }

    let name = parts[0];

    let on_idx = parts.iter().position(|&p| p == "on")
        .ok_or_else(|| format!("policy missing 'on' keyword: {}", first_line))?;
    let table = parts.get(on_idx + 1)
        .ok_or_else(|| format!("policy missing table name: {}", first_line))?;

    let for_idx = parts.iter().position(|&p| p == "for")
        .ok_or_else(|| format!("policy missing 'for' keyword: {}", first_line))?;
    let target_str = parts.get(for_idx + 1)
        .ok_or_else(|| format!("policy missing target: {}", first_line))?;

    let target = match target_str.to_lowercase().as_str() {
        "all" => PolicyTarget::All,
        "select" => PolicyTarget::Select,
        "insert" => PolicyTarget::Insert,
        "update" => PolicyTarget::Update,
        "delete" => PolicyTarget::Delete,
        _ => return Err(format!("Unknown policy target: {}", target_str)),
    };

    let mut policy = RlsPolicy::create(name, *table);
    policy.target = target;

    // Consume indented continuation lines (using / with_check)
    while let Some(&next_line) = lines.peek() {
        let trimmed = next_line.trim();
        if trimmed.is_empty() {
            lines.next();
            continue;
        }
        // Only continue if the line is indented (part of this policy block)
        if !next_line.starts_with("  ") && !next_line.starts_with('\t') {
            break;
        }

        // Consume the peeked line before processing it
        lines.next();

        if trimmed.starts_with("using ") || trimmed.starts_with("with_check ") {
            let is_using = trimmed.starts_with("using ");
            let keyword = if is_using { "using " } else { "with_check " };
            let after_keyword = trimmed.strip_prefix(keyword).unwrap_or("").trim();

            let body = extract_dollar_body(after_keyword, lines)?;
            // Store as raw SQL — the gateway only needs table/column metadata
            // for auto-REST routing; typed expression parsing is done by the
            // migration diff engine via parse_policy_expr() when needed.
            let expr = Expr::Raw(body);

            if is_using {
                policy.using = Some(expr);
            } else {
                policy.with_check = Some(expr);
            }
        }
        // Unknown indented lines are already consumed above
    }

    Ok(policy)
}

/// Extract text between `$$` markers, consuming continuation lines if needed.
fn extract_dollar_body<'a, I: Iterator<Item = &'a str>>(
    first_part: &str,
    lines: &mut std::iter::Peekable<I>,
) -> Result<String, String> {
    // Strip leading $$
    let after_open = first_part
        .strip_prefix("$$")
        .ok_or("expected $$ to start expression")?
        .trim_start();

    if let Some(pos) = after_open.find("$$") {
        // Single-line: $$ body $$
        Ok(after_open[..pos].trim().to_string())
    } else {
        // Multi-line: collect until closing $$
        let mut body = after_open.to_string();
        for line in lines.by_ref() {
            if let Some(pos) = line.find("$$") {
                let before = &line[..pos];
                if !body.is_empty() {
                    body.push('\n');
                }
                body.push_str(before);
                break;
            }
            body.push('\n');
            body.push_str(line);
        }
        Ok(body.trim().to_string())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use super::super::schema::GrantAction;

    #[test]
    fn test_parse_simple_table() {
        let input = r#"
table users {
  id serial primary_key
  name text not_null
  email text nullable unique
}
"#;
        let schema = parse_qail(input).unwrap();
        assert!(schema.tables.contains_key("users"));
        let table = &schema.tables["users"];
        assert_eq!(table.columns.len(), 3);
        assert!(table.columns[0].primary_key);
        assert!(!table.columns[1].nullable);
        assert!(table.columns[2].unique);
    }

    #[test]
    fn test_parse_index() {
        let input = "unique index idx_users_email on users (email)";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.indexes.len(), 1);
        assert!(schema.indexes[0].unique);
        assert_eq!(schema.indexes[0].name, "idx_users_email");
    }

    #[test]
    fn test_parse_rename() {
        let input = "rename users.username -> users.name";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.migrations.len(), 1);
        assert!(matches!(
            &schema.migrations[0],
            MigrationHint::Rename { from, to } if from == "users.username" && to == "users.name"
        ));
    }

    #[test]
    fn test_parse_full_schema() {
        let input = r#"
# User table
table users {
  id serial primary_key
  name text not_null
  email text unique
  created_at timestamptz default now()
}

unique index idx_users_email on users (email)

rename users.username -> users.name
"#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.indexes.len(), 1);
        assert_eq!(schema.migrations.len(), 1);
    }

    #[test]
    fn test_parse_extension() {
        let input = r#"extension "uuid-ossp""#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.extensions.len(), 1);
        assert_eq!(schema.extensions[0].name, "uuid-ossp");
    }

    #[test]
    fn test_parse_extension_with_options() {
        let input = r#"extension "uuid-ossp" schema public version "1.1""#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.extensions[0].name, "uuid-ossp");
        assert_eq!(schema.extensions[0].schema.as_deref(), Some("public"));
        assert_eq!(schema.extensions[0].version.as_deref(), Some("1.1"));
    }

    #[test]
    fn test_parse_extension_unquoted() {
        let input = "extension pgcrypto";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.extensions[0].name, "pgcrypto");
    }

    #[test]
    fn test_parse_comment_on_table() {
        let input = r#"comment on users "User accounts table""#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.comments.len(), 1);
        assert_eq!(schema.comments[0].text, "User accounts table");
    }

    #[test]
    fn test_parse_comment_on_column() {
        let input = r#"comment on users.email "Primary contact email""#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.comments.len(), 1);
        assert_eq!(schema.comments[0].text, "Primary contact email");
    }

    #[test]
    fn test_parse_sequence_simple() {
        let input = "sequence order_number_seq";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.sequences.len(), 1);
        assert_eq!(schema.sequences[0].name, "order_number_seq");
    }

    #[test]
    fn test_parse_sequence_with_options() {
        let input = "sequence order_seq { start 1000 increment 1 cache 10 cycle }";
        let schema = parse_qail(input).unwrap();
        let seq = &schema.sequences[0];
        assert_eq!(seq.name, "order_seq");
        assert_eq!(seq.start, Some(1000));
        assert_eq!(seq.increment, Some(1));
        assert_eq!(seq.cache, Some(10));
        assert!(seq.cycle);
    }

    #[test]
    fn test_parse_full_schema_with_extensions() {
        let input = r#"
extension "uuid-ossp"
extension pgcrypto

table users {
  id uuid primary_key
  name text not_null
}

sequence order_seq { start 1000 increment 1 }

comment on users "User accounts"
comment on users.name "Full name"
"#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.extensions.len(), 2);
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.sequences.len(), 1);
        assert_eq!(schema.comments.len(), 2);
    }

    // ======================== Phase 2 Tests ========================

    #[test]
    fn test_parse_enum_inline() {
        let input = "enum status { active, inactive, pending }";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.enums.len(), 1);
        assert_eq!(schema.enums[0].name, "status");
        assert_eq!(schema.enums[0].values, vec!["active", "inactive", "pending"]);
    }

    #[test]
    fn test_parse_enum_multiline() {
        let input = r#"
enum booking_status {
  draft,
  confirmed,
  cancelled,
  completed
}
"#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.enums[0].name, "booking_status");
        assert_eq!(schema.enums[0].values.len(), 4);
        assert_eq!(schema.enums[0].values[0], "draft");
        assert_eq!(schema.enums[0].values[3], "completed");
    }

    #[test]
    fn test_parse_expression_index() {
        let input = "index idx_users_email_lower on users ((lower(email)))";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.indexes.len(), 1);
        let idx = &schema.indexes[0];
        assert_eq!(idx.name, "idx_users_email_lower");
        assert!(!idx.expressions.is_empty());
        assert_eq!(idx.expressions[0], "(lower(email))");
    }

    #[test]
    fn test_parse_multi_column_fk() {
        let input = r#"
table bookings {
  id serial primary_key
  route_id integer not_null
  schedule_id integer not_null
  foreign_key (route_id, schedule_id) references schedules(route_id, schedule_id)
}
"#;
        let schema = parse_qail(input).unwrap();
        let table = &schema.tables["bookings"];
        assert_eq!(table.multi_column_fks.len(), 1);
        let fk = &table.multi_column_fks[0];
        assert_eq!(fk.columns, vec!["route_id", "schedule_id"]);
        assert_eq!(fk.ref_table, "schedules");
        assert_eq!(fk.ref_columns, vec!["route_id", "schedule_id"]);
    }

    #[test]
    fn test_parse_full_schema_phase2() {
        let input = r#"
enum status { active, inactive }

table users {
  id serial primary_key
  name text not_null
  status text not_null
}

index idx_name_lower on users ((lower(name)))
"#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.enums.len(), 1);
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.indexes.len(), 1);
        assert!(!schema.indexes[0].expressions.is_empty());
    }

    // ======================== Phase 3 Tests ========================

    #[test]
    fn test_parse_view() {
        let input = "view active_users $$ SELECT * FROM users WHERE active = true $$";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.views.len(), 1);
        assert_eq!(schema.views[0].name, "active_users");
        assert!(schema.views[0].query.contains("SELECT * FROM users"));
        assert!(!schema.views[0].materialized);
    }

    #[test]
    fn test_parse_materialized_view() {
        let input = r#"
materialized view booking_stats $$
  SELECT route_id, count(*) as total
  FROM bookings
  GROUP BY route_id
$$
"#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.views[0].name, "booking_stats");
        assert!(schema.views[0].materialized);
        assert!(schema.views[0].query.contains("GROUP BY"));
    }

    #[test]
    fn test_parse_function() {
        let input = "function set_updated_at() returns trigger language plpgsql $$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $$";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.functions.len(), 1);
        assert_eq!(schema.functions[0].name, "set_updated_at");
        assert_eq!(schema.functions[0].returns, "trigger");
        assert_eq!(schema.functions[0].language, "plpgsql");
        assert!(schema.functions[0].body.contains("RETURN NEW"));
    }

    #[test]
    fn test_parse_trigger() {
        let input = "trigger trg_updated_at on users before update execute set_updated_at";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.triggers.len(), 1);
        assert_eq!(schema.triggers[0].name, "trg_updated_at");
        assert_eq!(schema.triggers[0].table, "users");
        assert_eq!(schema.triggers[0].timing, "BEFORE");
        assert_eq!(schema.triggers[0].events, vec!["UPDATE"]);
        assert_eq!(schema.triggers[0].execute_function, "set_updated_at");
    }

    #[test]
    fn test_parse_grant() {
        let input = "grant select, insert on users to app_role";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.grants.len(), 1);
        assert_eq!(schema.grants[0].privileges.len(), 2);
        assert_eq!(schema.grants[0].on_object, "users");
        assert_eq!(schema.grants[0].to_role, "app_role");
        assert!(matches!(schema.grants[0].action, GrantAction::Grant));
    }

    #[test]
    fn test_parse_revoke() {
        let input = "revoke all on users from public";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.grants.len(), 1);
        assert!(matches!(schema.grants[0].action, GrantAction::Revoke));
        assert_eq!(schema.grants[0].on_object, "users");
        assert_eq!(schema.grants[0].to_role, "public");
    }

    #[test]
    fn test_parse_full_phase3_schema() {
        let input = r#"
extension pgcrypto

enum status { active, inactive }

table users {
  id uuid primary_key
  name text not_null
  status text not_null
}

view active_users $$ SELECT * FROM users WHERE status = 'active' $$

function set_updated_at() returns trigger language plpgsql $$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $$

trigger trg_updated on users before insert or update execute set_updated_at

grant select on users to readonly_role
"#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.extensions.len(), 1);
        assert_eq!(schema.enums.len(), 1);
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.views.len(), 1);
        assert_eq!(schema.functions.len(), 1);
        assert_eq!(schema.triggers.len(), 1);
        assert_eq!(schema.grants.len(), 1);
    }

    // ======================== Phase 4 Tests — New Parser Features ========================

    #[test]
    fn test_parse_fk_actions() {
        let input = r#"
table orders {
  id uuid primary_key
  user_id uuid references users(id) on_delete cascade on_update restrict
}
"#;
        let schema = parse_qail(input).unwrap();
        let col = &schema.tables["orders"].columns[1];
        assert_eq!(col.name, "user_id");
        let fk = col.foreign_key.as_ref().unwrap();
        assert_eq!(fk.table, "users");
        assert_eq!(fk.column, "id");
        assert!(matches!(fk.on_delete, FkAction::Cascade));
        assert!(matches!(fk.on_update, FkAction::Restrict));
    }

    #[test]
    fn test_parse_fk_on_delete_only() {
        let input = r#"
table orders {
  id uuid primary_key
  operator_id uuid references operators(id) on_delete set_null
}
"#;
        let schema = parse_qail(input).unwrap();
        let col = &schema.tables["orders"].columns[1];
        let fk = col.foreign_key.as_ref().unwrap();
        assert!(matches!(fk.on_delete, FkAction::SetNull));
        assert!(matches!(fk.on_update, FkAction::NoAction));
    }

    #[test]
    fn test_parse_check_between() {
        let input = r#"
table products {
  id uuid primary_key
  age int check(age between 0 200)
}
"#;
        let schema = parse_qail(input).unwrap();
        let col = &schema.tables["products"].columns[1];
        assert!(col.check.is_some());
        let expr = &col.check.as_ref().unwrap().expr;
        match expr {
            CheckExpr::Between { column, low, high } => {
                assert_eq!(column, "age");
                assert_eq!(*low, 0);
                assert_eq!(*high, 200);
            }
            _ => panic!("Expected Between, got {:?}", expr),
        }
    }

    #[test]
    fn test_parse_check_comparison() {
        let input = r#"
table products {
  id uuid primary_key
  score int check(score >= 0)
}
"#;
        let schema = parse_qail(input).unwrap();
        let col = &schema.tables["products"].columns[1];
        let expr = &col.check.as_ref().unwrap().expr;
        match expr {
            CheckExpr::GreaterOrEqual { column, value } => {
                assert_eq!(column, "score");
                assert_eq!(*value, 0);
            }
            _ => panic!("Expected GreaterOrEqual, got {:?}", expr),
        }
    }

    #[test]
    fn test_parse_enum_column_type() {
        let input = r#"
enum ticket_status { draft, active, cancelled }

table tickets {
  id uuid primary_key
  status ticket_status default 'draft'
}
"#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.enums.len(), 1);
        let col = &schema.tables["tickets"].columns[1];
        assert_eq!(col.name, "status");
        match &col.data_type {
            ColumnType::Enum { name, values } => {
                assert_eq!(name, "ticket_status");
                assert_eq!(values, &["draft", "active", "cancelled"]);
            }
            _ => panic!("Expected Enum type, got {:?}", col.data_type),
        }
        assert_eq!(col.default.as_deref(), Some("'draft'"));
    }

    #[test]
    fn test_parse_roundtrip_all_features() {
        let input = r#"
extension pgcrypto

enum payment_method { card, va, qris, cash }

sequence invoice_counter { start 1000 increment 1 }

table orders {
  id uuid primary_key default gen_random_uuid()
  method payment_method not_null default 'card'
  user_id uuid references users(id) on_delete cascade
  score int check(score >= 0)
  age int check(age between 0 200)
  enable_rls
  force_rls
}
"#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.extensions.len(), 1);
        assert_eq!(schema.enums.len(), 1);
        assert_eq!(schema.sequences.len(), 1);
        assert_eq!(schema.tables.len(), 1);

        let table = &schema.tables["orders"];
        assert!(table.enable_rls);
        assert!(table.force_rls);

        // Enum column
        let method = &table.columns[1];
        assert!(matches!(&method.data_type, ColumnType::Enum { name, .. } if name == "payment_method"));
        assert_eq!(method.default.as_deref(), Some("'card'"));

        // FK with cascade
        let user_id = &table.columns[2];
        let fk = user_id.foreign_key.as_ref().unwrap();
        assert!(matches!(fk.on_delete, FkAction::Cascade));

        // CHECK >= 0
        let score = &table.columns[3];
        assert!(matches!(&score.check.as_ref().unwrap().expr, CheckExpr::GreaterOrEqual { .. }));

        // CHECK between
        let age = &table.columns[4];
        assert!(matches!(&age.check.as_ref().unwrap().expr, CheckExpr::Between { .. }));
    }

    #[test]
    fn test_parse_booking_migration() {
        let input = r#"
table booking_orders {
  id                    uuid primary_key default gen_random_uuid()
  hold_id               uuid nullable
  connection_id         uuid nullable
  voyage_id             uuid nullable
  operator_id           uuid not_null
  status                text not_null default 'Draft'
  total_fare            bigint not_null
  currency              text not_null default 'IDR'
  nationality           text not_null default 'indo'
  pax_breakdown         jsonb not_null default '{}'
  contact_info          jsonb not_null default '{}'
  pricing_breakdown     jsonb nullable
  passenger_details     jsonb nullable default '[]'
  connection_snapshot   jsonb nullable
  invoice_number        text nullable unique
  booking_number        text nullable
  metadata              jsonb nullable
  user_id               uuid nullable
  agent_id              uuid nullable
  created_at            timestamptz not_null default now()
  updated_at            timestamptz not_null default now()

  enable_rls
  force_rls
}

index idx_booking_orders_operator on booking_orders (operator_id)
index idx_booking_orders_status on booking_orders (status)
index idx_booking_orders_user on booking_orders (user_id)
"#;
        let schema = parse_qail(input).expect("parse_qail should succeed for booking migration");
        assert_eq!(schema.tables.len(), 1);
        let table = &schema.tables["booking_orders"];
        assert!(table.enable_rls);
        assert!(table.force_rls);
        assert_eq!(table.columns.len(), 21);
        assert_eq!(schema.indexes.len(), 3);
    }
}
