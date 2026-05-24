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

use super::policy::{PolicyPermissiveness, PolicyTarget, RlsPolicy};
use super::schema::{
    CheckConstraint, CheckExpr, Column, Comment, EnumType, Extension, FkAction, Generated, Grant,
    Index, IndexMethod, MigrationHint, MultiColumnForeignKey, Privilege, ResourceDef, ResourceKind,
    Schema, SchemaFunctionDef, SchemaTriggerDef, Sequence, Table, ViewDef,
};
use super::types::ColumnType;
use crate::ast::Expr;
use std::collections::{HashMap, HashSet};

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
            if schema.tables.contains_key(&table.name) {
                return Err(format!("duplicate table declaration '{}'", table.name));
            }
            schema.add_table(table);
            // consumed lines already processed
            let _ = consumed;
        } else if line.starts_with("unique index ") || line.starts_with("index ") {
            let index = parse_index(line)?;
            if schema
                .indexes
                .iter()
                .any(|existing| existing.name == index.name)
            {
                return Err(format!("duplicate index declaration '{}'", index.name));
            }
            schema.add_index(index);
        } else if line.starts_with("extension ") {
            let ext = parse_extension(line)?;
            schema.add_extension(ext);
        } else if line.starts_with("comment ") {
            let comment = parse_comment(line)?;
            schema.add_comment(comment);
        } else if line.starts_with("sequence ") {
            let seq = parse_sequence(line, &mut lines)?;
            if schema
                .sequences
                .iter()
                .any(|existing| existing.name == seq.name)
            {
                return Err(format!("duplicate sequence declaration '{}'", seq.name));
            }
            schema.add_sequence(seq);
        } else if line.starts_with("enum ") {
            let enum_type = parse_enum(line, &mut lines)?;
            if schema
                .enums
                .iter()
                .any(|existing| existing.name == enum_type.name)
            {
                return Err(format!("duplicate enum declaration '{}'", enum_type.name));
            }
            schema.add_enum(enum_type);
        } else if line.starts_with("view ") || line.starts_with("materialized view ") {
            let view = parse_view(line, &mut lines)?;
            if schema
                .views
                .iter()
                .any(|existing| existing.name == view.name)
            {
                return Err(format!("duplicate view declaration '{}'", view.name));
            }
            schema.add_view(view);
        } else if line.starts_with("function ") {
            let func = parse_function(line, &mut lines)?;
            if schema
                .functions
                .iter()
                .any(|existing| existing.name == func.name && existing.args == func.args)
            {
                return Err(format!(
                    "duplicate function declaration '{}({})'",
                    func.name,
                    func.args.join(", ")
                ));
            }
            schema.add_function(func);
        } else if line.starts_with("trigger ") {
            let trigger = parse_trigger(line)?;
            if schema
                .triggers
                .iter()
                .any(|existing| existing.name == trigger.name && existing.table == trigger.table)
            {
                return Err(format!(
                    "duplicate trigger declaration '{} on {}'",
                    trigger.name, trigger.table
                ));
            }
            schema.add_trigger(trigger);
        } else if line.starts_with("grant ") || line.starts_with("revoke ") {
            let grant = parse_grant(line)?;
            schema.add_grant(grant);
        } else if line.starts_with("rename ") {
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
            if schema
                .resources
                .iter()
                .any(|existing| existing.name == res.name)
            {
                return Err(format!("duplicate resource declaration '{}'", res.name));
            }
            schema.add_resource(res);
        } else if line.starts_with("queue ") {
            let res = parse_resource(line, &mut lines, ResourceKind::Queue)?;
            if schema
                .resources
                .iter()
                .any(|existing| existing.name == res.name)
            {
                return Err(format!("duplicate resource declaration '{}'", res.name));
            }
            schema.add_resource(res);
        } else if line.starts_with("topic ") {
            let res = parse_resource(line, &mut lines, ResourceKind::Topic)?;
            if schema
                .resources
                .iter()
                .any(|existing| existing.name == res.name)
            {
                return Err(format!("duplicate resource declaration '{}'", res.name));
            }
            schema.add_resource(res);
        } else if line.starts_with("policy ") {
            let policy = parse_policy(line, &mut lines)?;
            if schema
                .policies
                .iter()
                .any(|existing| existing.name == policy.name && existing.table == policy.table)
            {
                return Err(format!(
                    "duplicate policy declaration '{} on {}'",
                    policy.name, policy.table
                ));
            }
            schema.add_policy(policy);
        } else {
            return Err(format!("Unknown statement: {}", line));
        }
    }

    Ok(schema)
}

/// Parse schema from a file or modular schema directory.
///
/// `path` may be:
/// - a single `.qail` file
/// - a directory containing one or more `.qail` modules
pub fn parse_qail_file(path: &str) -> Result<Schema, String> {
    let content = crate::schema_source::read_qail_schema_source(path)?;
    parse_qail(&content)
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
    let rest = first_line
        .strip_prefix("table ")
        .ok_or("Expected 'table' prefix")?;
    let (name_part, after_open) = rest
        .split_once('{')
        .ok_or_else(|| "table declaration requires an opening '{'".to_string())?;
    if !after_open.trim().is_empty() {
        return Err("trailing content after table opening brace".to_string());
    }
    let name = name_part.trim().to_string();

    if name.is_empty() {
        return Err("Table name required".to_string());
    }

    let mut table = Table::new(&name);
    let mut consumed = 0;
    let mut found_closing_brace = false;
    let mut seen_columns = HashSet::new();

    for line in lines.by_ref() {
        consumed += 1;
        let line = line.trim();

        if let Some(after_close) = line.strip_prefix('}') {
            if !after_close.trim().is_empty() {
                return Err("trailing content after table closing brace".to_string());
            }
            found_closing_brace = true;
            break;
        }

        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Table-level multi-column foreign key
        if line == "foreign_key"
            || line.starts_with("foreign_key ")
            || line.starts_with("foreign_key(")
        {
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
        if !seen_columns.insert(col.name.clone()) {
            return Err(format!(
                "duplicate column '{}' in table '{}'",
                col.name, name
            ));
        }
        table.columns.push(col);
    }

    if !found_closing_brace {
        return Err(format!("Unclosed table definition '{}'", name));
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
    let data_type: ColumnType = match type_str.parse() {
        Ok(t) => t,
        Err(_) => {
            if let Some(et) = enum_types.iter().find(|e| e.name == type_str) {
                ColumnType::Enum {
                    name: et.name.clone(),
                    values: et.values.clone(),
                }
            } else {
                return Err(format!(
                    "Unknown column type '{}' for column '{}'",
                    type_str, name
                ));
            }
        }
    };

    let mut col = Column::new(&name, data_type);

    let mut i = 2;
    let mut seen_primary_key = false;
    let mut nullability_option: Option<&str> = None;
    let mut seen_default = false;
    let mut seen_unique = false;
    let mut seen_generated = false;
    let mut seen_check = false;
    while i < parts.len() {
        match parts[i] {
            "primary_key" => {
                if seen_primary_key {
                    return Err(format!(
                        "duplicate primary_key option for column '{}'",
                        name
                    ));
                }
                if nullability_option == Some("nullable") {
                    return Err(format!(
                        "primary_key conflicts with nullable for column '{}'",
                        name
                    ));
                }
                seen_primary_key = true;
                col = col
                    .try_primary_key()
                    .map_err(|e| format!("{} (column '{}')", e, name))?;
            }
            "not_null" => {
                if let Some(existing) = nullability_option {
                    return Err(format!(
                        "conflicting nullability options '{}' and 'not_null' for column '{}'",
                        existing, name
                    ));
                }
                nullability_option = Some("not_null");
                col.nullable = false;
            }
            "nullable" => {
                if seen_primary_key {
                    return Err(format!(
                        "nullable conflicts with primary_key for column '{}'",
                        name
                    ));
                }
                if let Some(existing) = nullability_option {
                    return Err(format!(
                        "conflicting nullability options '{}' and 'nullable' for column '{}'",
                        existing, name
                    ));
                }
                nullability_option = Some("nullable");
                col.nullable = true;
            }
            "unique" => {
                if seen_unique {
                    return Err(format!("duplicate unique option for column '{}'", name));
                }
                seen_unique = true;
                col = col
                    .try_unique()
                    .map_err(|e| format!("{} (column '{}')", e, name))?;
            }
            "default" if i + 1 < parts.len() => {
                if seen_default {
                    return Err(format!("duplicate default option for column '{}'", name));
                }
                seen_default = true;
                let mut default_parts = Vec::new();
                i += 1;
                default_parts.push(parts[i]);
                while i + 1 < parts.len() && !is_column_constraint_keyword(parts[i + 1]) {
                    i += 1;
                    default_parts.push(parts[i]);
                }
                col.default = Some(default_parts.join(" "));
            }
            "default" => {
                return Err(format!("default requires a value for column '{}'", name));
            }
            "generated_identity" => {
                if seen_generated {
                    return Err(format!("duplicate generated option for column '{}'", name));
                }
                seen_generated = true;
                col.generated = Some(Generated::AlwaysIdentity);
            }
            "generated_by_default_identity" => {
                if seen_generated {
                    return Err(format!("duplicate generated option for column '{}'", name));
                }
                seen_generated = true;
                col.generated = Some(Generated::ByDefaultIdentity);
            }
            s if s.starts_with("generated_stored(") => {
                if seen_generated {
                    return Err(format!("duplicate generated option for column '{}'", name));
                }
                seen_generated = true;
                let mut generated_str = s.to_string();
                let mut depth: i32 = s.chars().fold(0, |acc, ch| match ch {
                    '(' => acc + 1,
                    ')' => acc - 1,
                    _ => acc,
                });

                while depth > 0 && i + 1 < parts.len() {
                    i += 1;
                    generated_str.push(' ');
                    generated_str.push_str(parts[i]);
                    depth += parts[i].chars().fold(0, |acc, ch| match ch {
                        '(' => acc + 1,
                        ')' => acc - 1,
                        _ => acc,
                    });
                }
                if depth != 0 {
                    return Err(format!(
                        "unclosed generated_stored expression for column '{}'",
                        name
                    ));
                }

                let inner = generated_str
                    .strip_prefix("generated_stored(")
                    .and_then(|s| s.strip_suffix(')'))
                    .ok_or_else(|| {
                        format!("invalid generated_stored expression for column '{}'", name)
                    })?
                    .trim();
                if inner.is_empty() {
                    return Err(format!(
                        "generated_stored expression is empty for column '{}'",
                        name
                    ));
                }
                col.generated = Some(Generated::AlwaysStored(inner.to_string()));
            }
            "references" => {
                let fk_str = if i + 1 < parts.len() {
                    i += 1;
                    parts[i]
                } else {
                    return Err(format!(
                        "foreign key reference target is required for column '{}'",
                        name
                    ));
                };

                let (table, column) = parse_fk_reference_target(fk_str)?;
                col = col.references(table, column);
                col = apply_fk_action_options(col, &parts, &mut i)?;
            }
            s if s.starts_with("references(") => {
                let inner = s
                    .strip_prefix("references(")
                    .and_then(|s| s.strip_suffix(')'))
                    .ok_or_else(|| format!("invalid foreign key reference target: {}", s))?;
                let (table, column) = inner
                    .split_once('.')
                    .ok_or_else(|| format!("invalid foreign key reference target: {}", s))?;
                if !is_native_table_ref(table.trim()) || !is_native_identifier(column.trim()) {
                    return Err(format!("invalid foreign key reference target: {}", s));
                }
                col = col.references(table.trim(), column.trim());
                col = apply_fk_action_options(col, &parts, &mut i)?;
            }
            s if s.starts_with("check(") => {
                if seen_check {
                    return Err(format!("duplicate check expression for column '{}'", name));
                }
                seen_check = true;
                // Parse check(expr) — expression may contain nested parens and spaces.
                // Keep consuming tokens until the outer `check(` parenthesis is balanced.
                let mut check_str = s.to_string();
                let mut depth: i32 = s.chars().fold(0, |acc, ch| match ch {
                    '(' => acc + 1,
                    ')' => acc - 1,
                    _ => acc,
                });

                while depth > 0 && i + 1 < parts.len() {
                    i += 1;
                    check_str.push(' ');
                    check_str.push_str(parts[i]);
                    depth += parts[i].chars().fold(0, |acc, ch| match ch {
                        '(' => acc + 1,
                        ')' => acc - 1,
                        _ => acc,
                    });
                }
                if depth != 0 {
                    return Err(format!("unclosed check expression for column '{}'", name));
                }

                // Strip "check(" and trailing ")"
                let inner = check_str
                    .strip_prefix("check(")
                    .and_then(|s| s.strip_suffix(')'))
                    .ok_or_else(|| format!("invalid check expression for column '{}'", name))?
                    .trim();
                if inner.is_empty() {
                    return Err(format!("check expression is empty for column '{}'", name));
                }
                let expr = parse_check_expr_from_qail(inner).ok_or_else(|| {
                    format!("invalid check expression for column '{}': {}", name, inner)
                })?;
                col.check = Some(CheckConstraint { expr, name: None });
            }
            "check_name" if i + 1 < parts.len() => {
                i += 1;
                if let Some(ref mut check) = col.check {
                    if check.name.is_some() {
                        return Err(format!("duplicate check_name for column '{}'", name));
                    }
                    check.name = Some(parts[i].to_string());
                } else {
                    return Err(format!(
                        "check_name requires a preceding check expression for column '{}'",
                        name
                    ));
                }
            }
            "check_name" => {
                return Err(format!("check_name requires a name for column '{}'", name));
            }
            _ => {
                return Err(format!(
                    "unknown column option '{}' for column '{}'",
                    parts[i], name
                ));
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
        line.strip_prefix("unique index ")
            .ok_or("Expected 'unique index' prefix")?
    } else {
        line.strip_prefix("index ")
            .ok_or("Expected 'index' prefix")?
    };

    let parts: Vec<&str> = rest.splitn(2, " on ").collect();
    if parts.len() != 2 {
        return Err(format!("Invalid index: {}", line));
    }

    let name = parts[0].trim().to_string();
    if name.is_empty() {
        return Err("index name is required".to_string());
    }
    let rest = parts[1];

    let paren_start = rest.find('(').ok_or("Missing ( in index")?;
    let paren_end = find_matching_paren(rest, paren_start).ok_or("Missing ) in index")?;

    let before_cols = rest[..paren_start].trim();
    let (table, method) = if let Some((tbl, method)) = before_cols.split_once(" using ") {
        (
            tbl.trim().to_string(),
            Some(parse_index_method_str(method)?),
        )
    } else {
        (before_cols.to_string(), None)
    };
    if table.trim().is_empty() {
        return Err("index table is required".to_string());
    }
    let cols_str = &rest[paren_start + 1..paren_end];
    let columns: Vec<String> = split_top_level_csv(cols_str)?;
    if columns.is_empty() {
        return Err("index columns are required".to_string());
    }

    // Detect expression indexes: columns contain parentheses like "(lower(email))"
    let has_expressions = columns
        .iter()
        .any(|c| c.starts_with('(') || c.contains("("));

    let mut index = if has_expressions {
        Index::expression(&name, &table, columns)
    } else {
        Index::new(&name, &table, columns)
    };
    if is_unique {
        index.unique = true;
    }
    if let Some(method) = method {
        index.method = method;
    }

    let trailing = rest[paren_end + 1..].trim();
    if let Some(pred) = trailing.strip_prefix("where ") {
        let pred = pred.trim();
        if pred.is_empty() {
            return Err("index where clause is empty".to_string());
        }
        index.where_clause = Some(CheckExpr::Sql(pred.to_string()));
    } else if !trailing.is_empty() {
        return Err("trailing content after index definition".to_string());
    }

    Ok(index)
}

fn split_top_level_csv(s: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        if let Some(q) = quote {
            cur.push(ch);
            if ch == q {
                if chars.peek().is_some_and(|next| *next == q) {
                    cur.push(ch);
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' => {
                quote = Some(ch);
                cur.push(ch);
            }
            '(' => {
                depth += 1;
                cur.push(ch);
            }
            ')' => {
                if depth == 0 {
                    return Err("unbalanced parentheses in index columns".to_string());
                }
                depth -= 1;
                cur.push(ch);
            }
            ',' if depth == 0 => {
                let piece = cur.trim();
                if piece.is_empty() {
                    return Err("empty index column or expression".to_string());
                }
                out.push(piece.to_string());
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }

    if quote.is_some() {
        return Err("unterminated quote in index columns".to_string());
    }
    if depth != 0 {
        return Err("unbalanced parentheses in index columns".to_string());
    }
    let tail = cur.trim();
    if tail.is_empty() {
        if !s.trim().is_empty() {
            return Err("empty index column or expression".to_string());
        }
    } else {
        out.push(tail.to_string());
    }
    Ok(out)
}

/// Parse a rename hint.
fn parse_rename(line: &str) -> Result<MigrationHint, String> {
    // rename users.username -> users.name
    let rest = line
        .strip_prefix("rename ")
        .ok_or("Expected 'rename' prefix")?;
    let (from, to) = rest
        .split_once("->")
        .ok_or_else(|| format!("Invalid rename: {}", line))?;
    let from = from.trim();
    let to = to.trim();
    if from.is_empty() || to.is_empty() {
        return Err(format!(
            "rename requires non-empty source and target: {}",
            line
        ));
    }

    Ok(MigrationHint::Rename {
        from: from.to_string(),
        to: to.to_string(),
    })
}

/// Parse a transform hint.
fn parse_transform(line: &str) -> Result<MigrationHint, String> {
    // transform age * 12 -> age_months
    let rest = line
        .strip_prefix("transform ")
        .ok_or("Expected 'transform' prefix")?;
    let (expression, target) = rest
        .split_once("->")
        .ok_or_else(|| format!("Invalid transform: {}", line))?;
    let expression = expression.trim();
    let target = target.trim();
    if expression.is_empty() || target.is_empty() {
        return Err(format!(
            "transform requires non-empty expression and target: {}",
            line
        ));
    }

    Ok(MigrationHint::Transform {
        expression: expression.to_string(),
        target: target.to_string(),
    })
}

/// Parse a drop hint.
fn parse_drop(line: &str) -> Result<MigrationHint, String> {
    // drop temp_table confirm
    let rest = line.strip_prefix("drop ").ok_or("Expected 'drop' prefix")?;
    let confirmed = rest.ends_with(" confirm");
    let target = if confirmed {
        rest.strip_suffix(" confirm")
            .ok_or("Expected 'confirm' suffix")?
            .trim()
            .to_string()
    } else {
        rest.trim().to_string()
    };
    if target.is_empty() {
        return Err("drop requires a target".to_string());
    }

    Ok(MigrationHint::Drop { target, confirmed })
}

/// Parse an extension definition.
/// Syntax: `extension "uuid-ossp"` or `extension pgcrypto`
///         `extension "uuid-ossp" schema public version "1.1"`
fn parse_extension(line: &str) -> Result<Extension, String> {
    let rest = line
        .strip_prefix("extension ")
        .ok_or("Expected 'extension' prefix")?
        .trim();
    let parts = split_extension_tokens(rest)?;

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

fn split_extension_tokens(rest: &str) -> Result<Vec<String>, String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = rest.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes => {
                if chars.peek().is_some_and(|next| *next == '"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            }
            '"' => in_quotes = true,
            c if c.is_whitespace() && !in_quotes => {
                if !current.is_empty() {
                    parts.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if in_quotes {
        return Err("unterminated quoted extension token".to_string());
    }
    if !current.is_empty() {
        parts.push(current);
    }

    Ok(parts)
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
    if target_str.is_empty() {
        return Err("comment target is required".to_string());
    }
    let text = parse_comment_text(&rest[quote_start..])?;

    if is_comment_raw_target(target_str) {
        Ok(Comment::on_raw(target_str, text))
    } else if target_str.contains('.') {
        let (table, column) = target_str
            .split_once('.')
            .ok_or_else(|| "invalid comment target".to_string())?;
        let table = table.trim();
        let column = column.trim();
        if table.is_empty() || column.is_empty() {
            return Err("invalid comment target".to_string());
        }
        Ok(Comment::on_column(table, column, text))
    } else {
        Ok(Comment::on_table(target_str, text))
    }
}

fn parse_comment_text(raw: &str) -> Result<String, String> {
    let mut chars = raw.char_indices().peekable();
    match chars.next() {
        Some((_, '"')) => {}
        _ => return Err("comment text must be quoted".to_string()),
    }

    let mut text = String::new();
    while let Some((idx, ch)) = chars.next() {
        if ch == '"' {
            if chars.peek().is_some_and(|(_, next)| *next == '"') {
                text.push('"');
                chars.next();
                continue;
            }

            let after = idx + ch.len_utf8();
            if !raw[after..].trim().is_empty() {
                return Err("trailing content after comment text".to_string());
            }
            return Ok(text);
        }
        text.push(ch);
    }

    Err("unterminated comment text".to_string())
}

fn is_comment_raw_target(target: &str) -> bool {
    let t = target.trim().to_ascii_lowercase();
    t.starts_with("function ")
        || t.starts_with("type ")
        || t.starts_with("policy ")
        || t.starts_with("constraint ")
        || t.starts_with("index ")
        || t.starts_with("sequence ")
        || t.starts_with("view ")
        || t.starts_with("materialized view ")
        || t.starts_with("schema ")
}

/// Parse a sequence definition.
/// Single-line: `sequence order_number_seq`
/// Multi-line:  `sequence order_number_seq { start 1000 increment 1 cache 10 }`
fn parse_sequence<'a, I: Iterator<Item = &'a str>>(
    first_line: &str,
    lines: &mut std::iter::Peekable<I>,
) -> Result<Sequence, String> {
    let rest = first_line
        .strip_prefix("sequence ")
        .ok_or("Expected 'sequence' prefix")?
        .trim();

    if rest.contains('{') {
        let name = rest
            .split('{')
            .next()
            .ok_or_else(|| "sequence name is missing before '{'".to_string())?
            .trim();
        if name.is_empty() {
            return Err("sequence name is missing before '{'".to_string());
        }
        let mut seq = Sequence::new(name);

        let mut tokens_str = rest.split('{').nth(1).unwrap_or("").to_string();
        let mut found_closing_brace = tokens_str.contains('}');

        if !found_closing_brace {
            for line in lines.by_ref() {
                let line = line.trim();
                tokens_str.push(' ');
                tokens_str.push_str(line);
                if line.contains('}') {
                    found_closing_brace = true;
                    break;
                }
            }
        }
        if !found_closing_brace {
            return Err(format!("Unclosed sequence block '{}'", name));
        }

        let close_idx = tokens_str
            .find('}')
            .expect("found_closing_brace guarantees a sequence block terminator");
        if !tokens_str[close_idx + 1..].trim().is_empty() {
            return Err("trailing content after sequence block".to_string());
        }
        let tokens_str = &tokens_str[..close_idx];
        let tokens: Vec<&str> = tokens_str.split_whitespace().collect();

        let mut i = 0;
        while i < tokens.len() {
            match tokens[i] {
                "start" if i + 1 < tokens.len() => {
                    seq.start = Some(tokens[i + 1].parse().map_err(|_| "invalid start value")?);
                    i += 2;
                }
                "increment" if i + 1 < tokens.len() => {
                    seq.increment = Some(
                        tokens[i + 1]
                            .parse()
                            .map_err(|_| "invalid increment value")?,
                    );
                    i += 2;
                }
                "minvalue" if i + 1 < tokens.len() => {
                    seq.min_value = Some(tokens[i + 1].parse().map_err(|_| "invalid minvalue")?);
                    i += 2;
                }
                "maxvalue" if i + 1 < tokens.len() => {
                    seq.max_value = Some(tokens[i + 1].parse().map_err(|_| "invalid maxvalue")?);
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
    let rest = first_line
        .strip_prefix("enum ")
        .ok_or("Expected 'enum' prefix")?
        .trim();

    if rest.contains('{') {
        let name = rest
            .split('{')
            .next()
            .ok_or_else(|| "enum name is missing before '{'".to_string())?
            .trim();
        if name.is_empty() {
            return Err("enum name is missing before '{'".to_string());
        }

        let mut values_str = rest.split('{').nth(1).unwrap_or("").to_string();

        if enum_body_before_closing_brace(&values_str)?.is_none() {
            for line in lines.by_ref() {
                let line = line.trim();
                values_str.push(' ');
                values_str.push_str(line);
                if enum_body_before_closing_brace(&values_str)?.is_some() {
                    break;
                }
            }
        }

        let values_str = enum_body_before_closing_brace(&values_str)?
            .ok_or_else(|| format!("enum '{}' is missing closing '}}'", name))?;
        let values = parse_enum_values(values_str)?;

        if values.is_empty() {
            return Err(format!("enum '{}' must have at least one value", name));
        }

        Ok(EnumType::new(name, values))
    } else {
        Err("enum definition requires { values }".to_string())
    }
}

fn enum_body_before_closing_brace(raw: &str) -> Result<Option<&str>, String> {
    let mut quote: Option<char> = None;
    let mut chars = raw.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if let Some(q) = quote {
            if ch == q {
                if chars.peek().is_some_and(|(_, next)| *next == q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            '}' => {
                let rest = &raw[idx + ch.len_utf8()..];
                if !rest.trim().is_empty() {
                    return Err("trailing content after enum block".to_string());
                }
                return Ok(Some(&raw[..idx]));
            }
            _ => {}
        }
    }

    Ok(None)
}

fn parse_enum_values(raw: &str) -> Result<Vec<String>, String> {
    let mut values = Vec::new();
    let mut quote: Option<char> = None;
    let mut start = 0;
    let mut chars = raw.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if let Some(q) = quote {
            if ch == q {
                if chars.peek().is_some_and(|(_, next)| *next == q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            ',' => {
                push_enum_value(&mut values, &raw[start..idx])?;
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    push_enum_value(&mut values, &raw[start..])?;
    let mut seen = HashSet::new();
    for value in &values {
        if !seen.insert(value) {
            return Err(format!("duplicate enum value '{}'", value));
        }
    }
    Ok(values)
}

fn push_enum_value(values: &mut Vec<String>, raw: &str) -> Result<(), String> {
    if raw.trim().is_empty() {
        return Err("enum value is empty".to_string());
    }

    let value = parse_enum_value(raw)?;
    values.push(value);
    Ok(())
}

fn parse_enum_value(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    let Some(quote) = trimmed.chars().next().filter(|ch| matches!(ch, '\'' | '"')) else {
        return Ok(trimmed.to_string());
    };

    let mut value = String::new();
    let mut chars = trimmed.char_indices();
    chars.next();
    let mut chars = chars.peekable();

    while let Some((idx, ch)) = chars.next() {
        if ch == quote {
            if chars.peek().is_some_and(|(_, next)| *next == quote) {
                value.push(quote);
                chars.next();
                continue;
            }

            let after = idx + ch.len_utf8();
            if !trimmed[after..].trim().is_empty() {
                return Err(format!("invalid enum value token '{}'", trimmed));
            }
            return Ok(value);
        }

        value.push(ch);
    }

    Err(format!("unterminated quoted enum value '{}'", trimmed))
}

/// Parse a table-level multi-column foreign key.
/// Syntax: `foreign_key (a, b) references other_table(x, y)`
fn parse_multi_column_fk(line: &str) -> Result<MultiColumnForeignKey, String> {
    let rest = line.strip_prefix("foreign_key").unwrap_or(line).trim();

    // Extract local columns from (...)
    let local_start = rest.find('(').ok_or("foreign_key missing ( for columns")?;
    let local_end = rest.find(')').ok_or("foreign_key missing ) for columns")?;
    let local_cols: Vec<String> = rest[local_start + 1..local_end]
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    if local_cols.is_empty() || local_cols.iter().any(|col| col.is_empty()) {
        return Err("foreign_key local columns are required".to_string());
    }

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
    if ref_table.is_empty() {
        return Err("foreign_key referenced table is required".to_string());
    }
    let ref_cols: Vec<String> = ref_part[ref_paren_start + 1..ref_paren_end]
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let trailing = ref_part[ref_paren_end + 1..].trim();
    if !trailing.is_empty() {
        return Err("trailing content after foreign_key definition".to_string());
    }
    if ref_cols.is_empty() || ref_cols.iter().any(|col| col.is_empty()) {
        return Err("foreign_key referenced columns are required".to_string());
    }
    if local_cols.len() != ref_cols.len() {
        return Err("foreign_key local/ref column counts must match".to_string());
    }

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
        first_line
            .strip_prefix("materialized view ")
            .ok_or("Expected 'materialized view' prefix")?
            .trim()
    } else {
        first_line
            .strip_prefix("view ")
            .ok_or("Expected 'view' prefix")?
            .trim()
    };

    if let Some((dollar_pos, delimiter)) = find_dollar_delimiter(rest) {
        let name = rest[..dollar_pos].trim();
        if name.is_empty() {
            return Err("view name is required".to_string());
        }
        let body = collect_dollar_body(
            &rest[dollar_pos + delimiter.len()..],
            lines,
            &delimiter,
            "view",
        )?;

        let mut view = ViewDef::new(name, body.trim());
        if materialized {
            view = view.materialized();
        }
        Ok(view)
    } else {
        Err("view body must be wrapped in a dollar-quoted block".to_string())
    }
}

/// Parse a function definition.
/// Syntax: `function name(args) returns type language lang $$ body $$`
fn parse_function<'a, I: Iterator<Item = &'a str>>(
    first_line: &str,
    lines: &mut std::iter::Peekable<I>,
) -> Result<SchemaFunctionDef, String> {
    let rest = first_line
        .strip_prefix("function ")
        .ok_or("Expected 'function' prefix")?
        .trim();

    // Extract name and args
    let paren_start = rest.find('(').ok_or("function missing (")?;
    let paren_end = find_matching_paren(rest, paren_start).ok_or("function missing )")?;

    let name = rest[..paren_start].trim();
    if name.is_empty() {
        return Err("function name is required".to_string());
    }
    let args_str = &rest[paren_start + 1..paren_end];
    let args = split_function_args(args_str);

    let after_args = rest[paren_end + 1..].trim();

    let (body_start_idx, delimiter) = find_dollar_delimiter(after_args)
        .ok_or_else(|| "function body must be wrapped in a dollar-quoted block".to_string())?;
    let header = after_args[..body_start_idx].trim();
    let (returns, language, volatility) = parse_function_header(header)?;

    let body = collect_dollar_body(
        &after_args[body_start_idx + delimiter.len()..],
        lines,
        &delimiter,
        "function",
    )?
    .trim()
    .to_string();

    let mut func = SchemaFunctionDef::new(name, &returns, body);
    func.language = language;
    func.args = args;
    func.volatility = volatility;

    Ok(func)
}

fn find_matching_paren(raw: &str, open_idx: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut chars = raw[open_idx..].char_indices().peekable();

    while let Some((relative_idx, ch)) = chars.next() {
        let idx = open_idx + relative_idx;

        if let Some(q) = quote {
            if ch == q {
                if chars.peek().is_some_and(|(_, next)| *next == q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
    }

    None
}

fn split_function_args(args: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut chars = args.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if let Some(q) = quote {
            if ch == q {
                if chars.peek().is_some_and(|(_, next)| *next == q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                let arg = args[start..idx].trim();
                if !arg.is_empty() {
                    out.push(arg.to_string());
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    let arg = args[start..].trim();
    if !arg.is_empty() {
        out.push(arg.to_string());
    }

    out
}

#[derive(Debug)]
struct HeaderWord {
    start: usize,
    end: usize,
    depth: usize,
}

fn parse_function_header(header: &str) -> Result<(String, String, Option<String>), String> {
    let words = header_word_spans(header);
    let returns_idx = words.iter().position(|word| {
        word.depth == 0 && header[word.start..word.end].eq_ignore_ascii_case("returns")
    });
    let language_idx = words.iter().position(|word| {
        word.depth == 0 && header[word.start..word.end].eq_ignore_ascii_case("language")
    });
    let volatility_idx = words.iter().position(|word| {
        if word.depth != 0 {
            return false;
        }
        matches!(
            header[word.start..word.end].to_ascii_lowercase().as_str(),
            "volatile" | "stable" | "immutable"
        )
    });

    let returns_idx = returns_idx.ok_or_else(|| "function missing returns clause".to_string())?;
    let start = words[returns_idx].end;
    let end = [language_idx, volatility_idx]
        .into_iter()
        .flatten()
        .filter(|next_idx| *next_idx > returns_idx)
        .min()
        .map(|next_idx| words[next_idx].start)
        .unwrap_or(header.len());
    let returns = header[start..end].trim();
    if returns.is_empty() {
        return Err("function returns clause requires a type".to_string());
    }

    let language_idx =
        language_idx.ok_or_else(|| "function missing language clause".to_string())?;
    let language = words
        .get(language_idx + 1)
        .map(|word| header[word.start..word.end].to_string())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "function language clause requires a language".to_string())?;

    let volatility =
        volatility_idx.map(|idx| header[words[idx].start..words[idx].end].to_ascii_lowercase());

    Ok((returns.to_string(), language, volatility))
}

fn header_word_spans(header: &str) -> Vec<HeaderWord> {
    let mut words = Vec::new();
    let mut start: Option<usize> = None;
    let mut start_depth = 0usize;
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut chars = header.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if ch.is_whitespace() {
            if let Some(word_start) = start.take() {
                words.push(HeaderWord {
                    start: word_start,
                    end: idx,
                    depth: start_depth,
                });
            }
            continue;
        }

        if start.is_none() {
            start = Some(idx);
            start_depth = depth;
        }

        if let Some(q) = quote {
            if ch == q {
                if chars.peek().is_some_and(|(_, next)| *next == q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }

    if let Some(word_start) = start {
        words.push(HeaderWord {
            start: word_start,
            end: header.len(),
            depth: start_depth,
        });
    }

    words
}

fn find_dollar_delimiter(raw: &str) -> Option<(usize, String)> {
    let mut search_start = 0;
    while let Some(relative_open) = raw[search_start..].find('$') {
        let open = search_start + relative_open;
        let tag_start = open + 1;
        let relative_close = raw[tag_start..].find('$')?;
        let close = tag_start + relative_close;
        let tag = &raw[tag_start..close];
        if tag
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            return Some((open, raw[open..=close].to_string()));
        }
        search_start = tag_start;
    }

    None
}

fn collect_dollar_body<'a, I: Iterator<Item = &'a str>>(
    first_fragment: &str,
    lines: &mut std::iter::Peekable<I>,
    delimiter: &str,
    kind: &str,
) -> Result<String, String> {
    let mut body = String::new();
    if let Some(closing) = first_fragment.find(delimiter) {
        body.push_str(&first_fragment[..closing]);
        let trailing = &first_fragment[closing + delimiter.len()..];
        if !trailing.trim().is_empty() {
            return Err(format!(
                "{kind} body has trailing content after closing delimiter"
            ));
        }
        return Ok(body);
    }

    body.push_str(first_fragment);
    for line in lines.by_ref() {
        if let Some(closing) = line.find(delimiter) {
            body.push('\n');
            body.push_str(&line[..closing]);
            let trailing = &line[closing + delimiter.len()..];
            if !trailing.trim().is_empty() {
                return Err(format!(
                    "{kind} body has trailing content after closing delimiter"
                ));
            }
            return Ok(body);
        }
        body.push('\n');
        body.push_str(line);
    }

    Err(format!(
        "{kind} body is missing closing delimiter {delimiter}"
    ))
}

/// Parse a trigger definition.
/// Syntax: `trigger name on table before|after insert|update|delete execute function_name`
fn parse_trigger(line: &str) -> Result<SchemaTriggerDef, String> {
    let rest = line
        .strip_prefix("trigger ")
        .ok_or("Expected 'trigger' prefix")?
        .trim();
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

    let timing = parts
        .get(on_idx + 2)
        .ok_or("trigger missing timing")?
        .to_uppercase();
    if !matches!(timing.as_str(), "BEFORE" | "AFTER") {
        return Err(format!("unsupported trigger timing: {timing}"));
    }

    // Collect events (INSERT, UPDATE, DELETE, etc.) until "execute"
    let mut events = Vec::new();
    let mut update_columns = Vec::new();
    let mut exec_idx = None;
    for (j, part) in parts.iter().enumerate().skip(on_idx + 3) {
        if part.eq_ignore_ascii_case("execute") {
            exec_idx = Some(j);
            break;
        }
    }

    let exec_idx = exec_idx.ok_or("trigger missing 'execute' keyword")?;
    let event_tokens = &parts[on_idx + 3..exec_idx];
    let mut chunks: Vec<Vec<&str>> = Vec::new();
    let mut current = Vec::new();
    for tok in event_tokens {
        if tok.eq_ignore_ascii_case("or") {
            if !current.is_empty() {
                chunks.push(current);
                current = Vec::new();
            }
            continue;
        }
        current.push(*tok);
    }
    if !current.is_empty() {
        chunks.push(current);
    }

    let mut seen_events = HashSet::new();
    for chunk in chunks {
        if chunk.is_empty() {
            continue;
        }
        if chunk.len() >= 3
            && chunk[0].eq_ignore_ascii_case("update")
            && chunk[1].eq_ignore_ascii_case("of")
        {
            if !seen_events.insert("UPDATE".to_string()) {
                return Err("duplicate trigger event: UPDATE".to_string());
            }
            events.push("UPDATE".to_string());
            let before_count = update_columns.len();
            let mut seen_cols = HashSet::new();
            let cols = chunk[2..].join(" ");
            for col in cols.split(',') {
                let c = col.trim();
                if c.is_empty() {
                    return Err("trigger update of contains an empty column".to_string());
                }
                if !seen_cols.insert(c.to_string()) {
                    return Err(format!("duplicate trigger update column '{}'", c));
                }
                update_columns.push(c.to_string());
            }
            if update_columns.len() == before_count {
                return Err("trigger update of requires at least one column".to_string());
            }
            continue;
        }
        let event = chunk.join(" ").to_uppercase();
        if !matches!(event.as_str(), "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE") {
            return Err(format!("unsupported trigger event: {event}"));
        }
        if !seen_events.insert(event.clone()) {
            return Err(format!("duplicate trigger event: {event}"));
        }
        events.push(event);
    }
    if events.is_empty() {
        return Err("trigger requires at least one event".to_string());
    }

    let func_name = parts
        .get(exec_idx + 1)
        .ok_or("trigger missing function name")?;
    if parts.len() > exec_idx + 2 {
        return Err("trailing content after trigger function".to_string());
    }

    let mut trigger = SchemaTriggerDef::new(name, *table, *func_name);
    trigger.timing = timing;
    trigger.events = events;
    trigger.update_columns = update_columns;

    Ok(trigger)
}

/// Parse GRANT/REVOKE.
/// Syntax: `grant select, insert on users to app_role`
///     or: `revoke all on users from public`
fn parse_grant(line: &str) -> Result<Grant, String> {
    let is_revoke = line.starts_with("revoke ");
    let rest = if is_revoke {
        line.strip_prefix("revoke ")
            .ok_or("Expected 'revoke' prefix")?
    } else {
        line.strip_prefix("grant ")
            .ok_or("Expected 'grant' prefix")?
    }
    .trim();

    // Find "on" keyword
    let on_idx = rest
        .find(" on ")
        .ok_or("grant/revoke missing 'on' keyword")?;
    let privs_str = &rest[..on_idx].trim();
    let after_on = rest[on_idx + 4..].trim();

    // Find "to" or "from" keyword
    let (obj_str, role_str) = if is_revoke {
        split_grant_subject(after_on, "from")
            .ok_or_else(|| "revoke missing 'from' keyword".to_string())?
    } else {
        split_grant_subject(after_on, "to")
            .ok_or_else(|| "grant missing 'to' keyword".to_string())?
    };
    if obj_str.trim().is_empty() {
        return Err("grant/revoke object is required".to_string());
    }
    if role_str.trim().is_empty() {
        return Err("grant/revoke role is required".to_string());
    }

    let mut privileges = Vec::new();
    let mut seen_privileges = HashSet::new();
    for raw_privilege in privs_str.split(',') {
        let privilege_key = raw_privilege.trim().to_uppercase();
        let privilege = parse_privilege(raw_privilege)?;
        if privilege_key == "ALL" && !seen_privileges.is_empty()
            || privilege_key != "ALL" && seen_privileges.contains("ALL")
        {
            return Err("ALL privilege cannot be combined with specific privileges".to_string());
        }
        if !seen_privileges.insert(privilege_key.clone()) {
            return Err(format!("duplicate grant/revoke privilege: {privilege_key}"));
        }
        privileges.push(privilege);
    }

    if is_revoke {
        Ok(Grant::revoke(privileges, obj_str.trim(), role_str.trim()))
    } else {
        Ok(Grant::new(privileges, obj_str.trim(), role_str.trim()))
    }
}

fn split_grant_subject(after_on: &str, keyword: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = after_on.split_whitespace().collect();
    let idx = parts
        .iter()
        .position(|part| part.eq_ignore_ascii_case(keyword))?;
    Some((parts[..idx].join(" "), parts[idx + 1..].join(" ")))
}

fn parse_privilege(raw: &str) -> Result<Privilege, String> {
    match raw.trim().to_uppercase().as_str() {
        "ALL" => Ok(Privilege::All),
        "SELECT" => Ok(Privilege::Select),
        "INSERT" => Ok(Privilege::Insert),
        "UPDATE" => Ok(Privilege::Update),
        "DELETE" => Ok(Privilege::Delete),
        "USAGE" => Ok(Privilege::Usage),
        "EXECUTE" => Ok(Privilege::Execute),
        "" => Err("grant/revoke privilege is empty".to_string()),
        other => Err(format!("unknown grant/revoke privilege: {other}")),
    }
}

/// Parse QAIL FK action string to FkAction enum.
/// Accepts: cascade, set_null, set_default, restrict, no_action
fn parse_fk_action_str(s: &str) -> Result<FkAction, String> {
    match s {
        "cascade" => Ok(FkAction::Cascade),
        "set_null" => Ok(FkAction::SetNull),
        "set_default" => Ok(FkAction::SetDefault),
        "restrict" => Ok(FkAction::Restrict),
        "no_action" => Ok(FkAction::NoAction),
        other => Err(format!("unknown foreign key action: {other}")),
    }
}

fn apply_fk_action_options(
    mut col: Column,
    parts: &[&str],
    i: &mut usize,
) -> Result<Column, String> {
    let mut seen_on_delete = false;
    let mut seen_on_update = false;
    while *i + 1 < parts.len() {
        match parts[*i + 1] {
            "on_delete" if *i + 2 < parts.len() => {
                if seen_on_delete {
                    return Err("duplicate on_delete action".to_string());
                }
                seen_on_delete = true;
                let action = parse_fk_action_str(parts[*i + 2])?;
                col = col.on_delete(action);
                *i += 2;
            }
            "on_update" if *i + 2 < parts.len() => {
                if seen_on_update {
                    return Err("duplicate on_update action".to_string());
                }
                seen_on_update = true;
                let action = parse_fk_action_str(parts[*i + 2])?;
                col = col.on_update(action);
                *i += 2;
            }
            "on_delete" | "on_update" => {
                return Err(format!("{} requires a foreign key action", parts[*i + 1]));
            }
            _ => break,
        }
    }

    Ok(col)
}

fn parse_fk_reference_target(raw: &str) -> Result<(&str, &str), String> {
    let paren_start = raw
        .find('(')
        .ok_or_else(|| format!("invalid foreign key reference target: {raw}"))?;
    let paren_end = raw[paren_start + 1..]
        .find(')')
        .map(|idx| paren_start + 1 + idx)
        .ok_or_else(|| format!("invalid foreign key reference target: {raw}"))?;
    if !raw[paren_end + 1..].trim().is_empty() {
        return Err(format!(
            "trailing content in foreign key reference target: {raw}"
        ));
    }

    let table = raw[..paren_start].trim();
    let column = raw[paren_start + 1..paren_end].trim();
    if !is_native_table_ref(table) || !is_native_identifier(column) {
        return Err(format!("invalid foreign key reference target: {raw}"));
    }

    Ok((table, column))
}

fn is_native_table_ref(value: &str) -> bool {
    let mut parts = value.split('.');
    let Some(first) = parts.next() else {
        return false;
    };
    !first.is_empty() && is_native_identifier(first) && parts.all(is_native_identifier)
}

fn is_native_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn parse_index_method_str(s: &str) -> Result<IndexMethod, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "btree" => Ok(IndexMethod::BTree),
        "hash" => Ok(IndexMethod::Hash),
        "gin" => Ok(IndexMethod::Gin),
        "gist" => Ok(IndexMethod::Gist),
        "brin" => Ok(IndexMethod::Brin),
        "spgist" => Ok(IndexMethod::SpGist),
        "hnsw" => Ok(IndexMethod::Hnsw),
        "ivfflat" => Ok(IndexMethod::IvfFlat),
        "" => Err("index method is empty".to_string()),
        other => Err(format!("unknown index method: {other}")),
    }
}

fn is_column_constraint_keyword(token: &str) -> bool {
    matches!(
        token,
        "primary_key"
            | "not_null"
            | "nullable"
            | "unique"
            | "default"
            | "generated_identity"
            | "generated_by_default_identity"
            | "references"
            | "on_delete"
            | "on_update"
            | "check_name"
    ) || token.starts_with("check(")
        || token.starts_with("generated_stored(")
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
        return Some(CheckExpr::Between {
            column: col,
            low,
            high,
        });
    }

    // Try "col in [a, b, \"c,d\"]"
    if let Some(expr) = parse_check_in_expr(s) {
        return Some(expr);
    }

    // Try "left and right"
    if let Some(and_pos) = find_top_level_operator(s, " and ") {
        let left = parse_check_expr_from_qail(&s[..and_pos])?;
        let right = parse_check_expr_from_qail(&s[and_pos + 5..])?;
        return Some(CheckExpr::And(Box::new(left), Box::new(right)));
    }

    // Try "left or right"
    if let Some(or_pos) = find_top_level_operator(s, " or ") {
        let left = parse_check_expr_from_qail(&s[..or_pos])?;
        let right = parse_check_expr_from_qail(&s[or_pos + 4..])?;
        return Some(CheckExpr::Or(Box::new(left), Box::new(right)));
    }

    // Try simple comparisons: "col >= val", "col > val", etc.
    type CheckExprConstructor = fn(String, i64) -> CheckExpr;
    let ops: &[(&str, CheckExprConstructor)] = &[
        (">=", |col, val| CheckExpr::GreaterOrEqual {
            column: col,
            value: val,
        }),
        ("<=", |col, val| CheckExpr::LessOrEqual {
            column: col,
            value: val,
        }),
        (">", |col, val| CheckExpr::GreaterThan {
            column: col,
            value: val,
        }),
        ("<", |col, val| CheckExpr::LessThan {
            column: col,
            value: val,
        }),
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
        return Some(CheckExpr::NotNull {
            column: parts[0].to_string(),
        });
    }

    if s.is_empty() {
        None
    } else {
        Some(CheckExpr::Sql(s.to_string()))
    }
}

fn find_top_level_operator(s: &str, operator: &str) -> Option<usize> {
    let mut quote: Option<char> = None;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut chars = s.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if let Some(q) = quote {
            if ch == q {
                if chars.peek().is_some_and(|(_, next)| *next == q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            _ => {
                if paren_depth == 0 && bracket_depth == 0 && s[idx..].starts_with(operator) {
                    return Some(idx);
                }
            }
        }
    }

    None
}

fn parse_check_in_expr(s: &str) -> Option<CheckExpr> {
    let marker = " in [";
    let pos = s.find(marker)?;
    let column = s[..pos].trim();
    if column.is_empty() {
        return None;
    }

    let values_start = pos + marker.len();
    let values_raw = list_body_before_closing_bracket(&s[values_start..])?;
    let values = parse_enum_values(values_raw).ok()?;
    if values.is_empty() {
        return None;
    }

    Some(CheckExpr::In {
        column: column.to_string(),
        values,
    })
}

fn list_body_before_closing_bracket(raw: &str) -> Option<&str> {
    let mut quote: Option<char> = None;
    let mut chars = raw.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if let Some(q) = quote {
            if ch == q {
                if chars.peek().is_some_and(|(_, next)| *next == q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            ']' => {
                if raw[idx + ch.len_utf8()..].trim().is_empty() {
                    return Some(&raw[..idx]);
                }
                return None;
            }
            _ => {}
        }
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
    if !has_block && !rest.is_empty() {
        return Err(format!("Trailing content after {} resource name", keyword));
    }

    if has_block {
        // Collect content until closing brace
        let mut block_content = rest.trim_start_matches('{').to_string();
        let mut found_closing_brace = false;
        let mut content = String::new();

        // If no closing brace on same line, read until we find it
        if let Some(closed_content) = resource_block_content_before_closing(&block_content)? {
            found_closing_brace = true;
            content = closed_content;
        } else {
            for next_line in lines.by_ref() {
                let next_line = next_line.trim();
                block_content.push(' ');
                block_content.push_str(next_line);
                if let Some(closed_content) = resource_block_content_before_closing(&block_content)?
                {
                    found_closing_brace = true;
                    content = closed_content;
                    break;
                }
            }
        }
        if !found_closing_brace {
            return Err(format!("Unclosed {} resource block '{}'", keyword, name));
        }

        // Parse key-value pairs from block content
        let content = content.trim();
        let tokens = split_resource_tokens(content)?;
        let mut tokens = tokens.iter();
        let mut seen_keys = HashSet::new();

        while let Some(key) = tokens.next() {
            if key.is_empty() || key == "}" {
                continue;
            }
            if !seen_keys.insert(key) {
                return Err(format!(
                    "Duplicate resource property '{}' in '{}'",
                    key, name
                ));
            }
            if let Some(value) = tokens.next() {
                if key == "provider" {
                    provider = Some(value.to_string());
                } else {
                    properties.insert(key.to_string(), value.to_string());
                }
            } else {
                return Err(format!(
                    "Resource property '{}' in '{}' requires a value",
                    key, name
                ));
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

fn resource_block_content_before_closing(content: &str) -> Result<Option<String>, String> {
    let mut quote: Option<char> = None;
    let mut escaped = false;

    for (idx, ch) in content.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }

        match quote {
            Some(q) => match ch {
                '\\' => escaped = true,
                c if c == q => quote = None,
                _ => {}
            },
            None => match ch {
                '"' | '\'' => quote = Some(ch),
                '}' => {
                    let rest = &content[idx + ch.len_utf8()..];
                    if !rest.trim().is_empty() {
                        return Err("Trailing content after resource block".to_string());
                    }
                    return Ok(Some(content[..idx].trim().to_string()));
                }
                _ => {}
            },
        }
    }

    Ok(None)
}

fn split_resource_tokens(content: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;

    for ch in content.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        match quote {
            Some(q) => match ch {
                '\\' => escaped = true,
                c if c == q => quote = None,
                c => current.push(c),
            },
            None => match ch {
                '"' | '\'' => quote = Some(ch),
                c if c.is_whitespace() => {
                    if !current.is_empty() {
                        tokens.push(std::mem::take(&mut current));
                    }
                }
                c => current.push(c),
            },
        }
    }

    if escaped {
        current.push('\\');
    }
    if quote.is_some() {
        return Err("Unterminated quoted resource value".to_string());
    }
    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

/// Parse an RLS policy definition.
///
/// Syntax:
/// ```text
/// policy NAME on TABLE [for TARGET] [to ROLE] [restrictive|permissive]
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
    // Parse header: "policy NAME on TABLE [for TARGET] [to ROLE] [restrictive|permissive]"
    let rest = first_line
        .strip_prefix("policy ")
        .ok_or("Expected 'policy' prefix")?
        .trim();
    let parts: Vec<&str> = rest.split_whitespace().collect();

    // Minimum: NAME on TABLE
    if parts.len() < 3 {
        return Err(format!("Invalid policy: {}", first_line));
    }

    let name = parts[0];

    let on_idx = parts
        .iter()
        .position(|&p| p == "on")
        .ok_or_else(|| format!("policy missing 'on' keyword: {}", first_line))?;
    let table = parts
        .get(on_idx + 1)
        .ok_or_else(|| format!("policy missing table name: {}", first_line))?;

    let mut policy = RlsPolicy::create(name, *table);
    parse_policy_clause_tokens(&parts[on_idx + 2..], &mut policy, first_line)?;

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

        if is_policy_header_clause(trimmed) {
            let clause_parts: Vec<&str> = trimmed.split_whitespace().collect();
            parse_policy_clause_tokens(&clause_parts, &mut policy, trimmed)?;
        } else if trimmed.starts_with("using ") || trimmed.starts_with("with_check ") {
            let is_using = trimmed.starts_with("using ");
            let keyword = if is_using { "using " } else { "with_check " };
            let after_keyword = trimmed.strip_prefix(keyword).unwrap_or("").trim();

            let body = extract_dollar_body(after_keyword, lines)?;
            // Preserve policy predicate text as-is. Parsing/re-serialization can
            // alter semantics for complex predicates.
            let expr = Expr::Named(body.clone());

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

fn is_policy_header_clause(trimmed: &str) -> bool {
    let first = trimmed.split_whitespace().next().unwrap_or("");
    matches!(
        first.to_ascii_lowercase().as_str(),
        "for" | "to" | "restrictive" | "permissive"
    )
}

fn parse_policy_clause_tokens(
    parts: &[&str],
    policy: &mut RlsPolicy,
    source: &str,
) -> Result<(), String> {
    let mut idx = 0;
    while idx < parts.len() {
        match parts[idx].to_ascii_lowercase().as_str() {
            "for" => {
                idx += 1;
                let target_str = parts
                    .get(idx)
                    .ok_or_else(|| format!("policy missing target: {}", source))?;
                policy.target = parse_policy_target(target_str)?;
                idx += 1;
            }
            "to" => {
                idx += 1;
                let role = parts
                    .get(idx)
                    .ok_or_else(|| format!("policy missing role after 'to': {}", source))?;
                policy.role = Some((*role).to_string());
                idx += 1;
            }
            "restrictive" => {
                policy.permissiveness = PolicyPermissiveness::Restrictive;
                idx += 1;
            }
            "permissive" => {
                policy.permissiveness = PolicyPermissiveness::Permissive;
                idx += 1;
            }
            _ => {
                return Err(format!(
                    "Unknown policy clause '{}': {}",
                    parts[idx], source
                ));
            }
        }
    }

    Ok(())
}

fn parse_policy_target(target_str: &str) -> Result<PolicyTarget, String> {
    match target_str.to_lowercase().as_str() {
        "all" => Ok(PolicyTarget::All),
        "select" => Ok(PolicyTarget::Select),
        "insert" => Ok(PolicyTarget::Insert),
        "update" => Ok(PolicyTarget::Update),
        "delete" => Ok(PolicyTarget::Delete),
        _ => Err(format!("Unknown policy target: {}", target_str)),
    }
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
    use super::super::schema::GrantAction;
    use super::*;

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
    fn test_parse_table_rejects_malformed_braces() {
        let cases = [
            ("table users\n  id serial primary_key\n}", "opening"),
            (
                "table users {\n  id serial primary_key",
                "Unclosed table definition 'users'",
            ),
            (
                "table users { id serial primary_key }",
                "trailing content after table opening brace",
            ),
            (
                "table users {\n  id serial primary_key\n} trailing",
                "trailing content after table closing brace",
            ),
        ];

        for (input, expected) in cases {
            let err = parse_qail(input).expect_err("malformed table braces should fail");
            assert!(err.contains(expected), "expected '{expected}' in '{err}'");
        }
    }

    #[test]
    fn test_parse_table_rejects_duplicate_columns() {
        let input = r#"
table users {
  id serial primary_key
  id uuid
}
"#;
        let err = parse_qail(input).expect_err("duplicate columns should fail");
        assert!(err.contains("duplicate column 'id' in table 'users'"));
    }

    #[test]
    fn test_parse_qail_rejects_duplicate_tables() {
        let input = r#"
table users {
  id serial primary_key
}

table users {
  email text
}
"#;
        let err = parse_qail(input).expect_err("duplicate tables should fail");
        assert!(err.contains("duplicate table declaration 'users'"));
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
    fn test_parse_index_rejects_missing_shape_parts() {
        for (input, expected) in [
            ("index  on users (email)", "index name is required"),
            (
                "index idx_users_email on  (email)",
                "index table is required",
            ),
            (
                "index idx_users_email on users ()",
                "index columns are required",
            ),
            (
                "index idx_users_email on users (email,)",
                "empty index column or expression",
            ),
            (
                "index idx_users_email on users (,email)",
                "empty index column or expression",
            ),
            (
                "index idx_users_email on users (email,,name)",
                "empty index column or expression",
            ),
        ] {
            let err = parse_qail(input).expect_err("invalid index should fail");
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn test_parse_index_rejects_trailing_content() {
        let input = "index idx_users_email on users (email) garbage";
        let err = parse_qail(input).expect_err("trailing index content should fail");
        assert!(err.contains("trailing content after index definition"));
    }

    #[test]
    fn test_parse_vector_index_methods() {
        let input = r#"
index idx_docs_embedding_hnsw on documents using hnsw (embedding vector_l2_ops)
index idx_docs_embedding_ivfflat on documents using ivfflat (embedding vector_cosine_ops)
"#;
        let schema = parse_qail(input).unwrap();

        assert_eq!(schema.indexes.len(), 2);
        assert_eq!(schema.indexes[0].method, IndexMethod::Hnsw);
        assert_eq!(
            schema.indexes[0].columns,
            vec!["embedding vector_l2_ops".to_string()]
        );
        assert_eq!(schema.indexes[1].method, IndexMethod::IvfFlat);
        assert_eq!(
            schema.indexes[1].columns,
            vec!["embedding vector_cosine_ops".to_string()]
        );
    }

    #[test]
    fn test_parse_index_rejects_unknown_method() {
        let input = "index idx_users_email on users using btre (email)";
        let err = parse_qail(input).expect_err("unknown index method should fail");
        assert!(err.contains("unknown index method: btre"));
    }

    #[test]
    fn test_parse_index_rejects_duplicate_names() {
        let input = r#"
index idx_users_email on users (email)
unique index idx_users_email on users (tenant_id, email)
"#;
        let err = parse_qail(input).expect_err("duplicate indexes should fail");
        assert!(err.contains("duplicate index declaration 'idx_users_email'"));
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
    fn test_parse_rename_rejects_empty_paths() {
        for input in ["rename users.username -> ", "rename  -> users.name"] {
            let err = parse_qail(input).expect_err("empty rename paths should fail");
            assert!(err.contains("rename requires non-empty source and target"));
        }
    }

    #[test]
    fn test_parse_transform_rejects_empty_parts() {
        for input in ["transform age * 12 -> ", "transform  -> age_months"] {
            let err = parse_qail(input).expect_err("empty transform parts should fail");
            assert!(err.contains("transform requires non-empty expression and target"));
        }
    }

    #[test]
    fn test_parse_drop_rejects_empty_target() {
        let err = parse_qail("drop  confirm").expect_err("empty drop target should fail");
        assert!(err.contains("drop requires a target"));
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
    fn test_parse_extension_round_trips_quoted_tokens() {
        let input = r#"extension "uuid""ossp" schema "tenant schema" version "1.""1""#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.extensions[0].name, r#"uuid"ossp"#);
        assert_eq!(
            schema.extensions[0].schema.as_deref(),
            Some("tenant schema")
        );
        assert_eq!(schema.extensions[0].version.as_deref(), Some(r#"1."1"#));

        let rendered = super::super::schema::to_qail_string(&schema);
        assert!(
            rendered.contains(r#"extension "uuid""ossp" schema "tenant schema" version "1.""1""#)
        );

        let reparsed = parse_qail(&rendered).unwrap();
        assert_eq!(reparsed.extensions, schema.extensions);
    }

    #[test]
    fn test_parse_extension_unquoted() {
        let input = "extension pgcrypto";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.extensions[0].name, "pgcrypto");
    }

    #[test]
    fn test_parse_extension_rejects_unterminated_quote() {
        let input = r#"extension "uuid-ossp"#;
        let err = parse_qail(input).expect_err("unterminated extension quote should fail");
        assert!(err.contains("unterminated quoted extension token"));
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
    fn test_parse_comment_rejects_missing_target() {
        let input = r#"comment on "orphaned comment""#;
        let err = parse_qail(input).expect_err("missing comment target should fail");
        assert!(err.contains("comment target is required"));
    }

    #[test]
    fn test_parse_comment_rejects_empty_column_target_segments() {
        for input in [
            r#"comment on users. "missing column""#,
            r#"comment on .email "missing table""#,
        ] {
            let err = parse_qail(input).expect_err("empty comment target segment should fail");
            assert!(err.contains("invalid comment target"));
        }
    }

    #[test]
    fn test_parse_comment_round_trips_doubled_quotes() {
        let input = r#"comment on users "He said ""hello""""#;
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.comments[0].text, r#"He said "hello""#);

        let rendered = super::super::schema::to_qail_string(&schema);
        assert!(rendered.contains(r#"comment on users "He said ""hello""""#));

        let reparsed = parse_qail(&rendered).unwrap();
        assert_eq!(reparsed.comments[0].text, schema.comments[0].text);
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
    fn test_parse_rejects_unclosed_sequence_block() {
        let input = r#"
sequence order_seq {
  start 1000
  increment 1
"#;

        let err = parse_qail(input).expect_err("unclosed sequence block should be rejected");
        assert!(err.contains("Unclosed sequence block"));
    }

    #[test]
    fn test_parse_sequence_rejects_missing_name() {
        let input = "sequence { start 1 }";
        let err = parse_qail(input).expect_err("missing sequence name should fail");
        assert!(err.contains("sequence name is missing before '{'"));
    }

    #[test]
    fn test_parse_sequence_rejects_duplicate_names() {
        let input = r#"
sequence order_ids
sequence order_ids { start 100 }
"#;
        let err = parse_qail(input).expect_err("duplicate sequences should fail");
        assert!(err.contains("duplicate sequence declaration 'order_ids'"));
    }

    #[test]
    fn test_parse_sequence_rejects_trailing_content_after_block() {
        let input = "sequence order_seq { start 1 } }";
        let err = parse_qail(input).expect_err("extra sequence content should fail");
        assert!(err.contains("trailing content after sequence block"));
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
        assert_eq!(
            schema.enums[0].values,
            vec!["active", "inactive", "pending"]
        );
    }

    #[test]
    fn test_parse_enum_quoted_values_with_commas_and_quotes() {
        let input =
            r#"enum status { "needs review", "card,bank", "quote "" ok", 'single '' ok', "" }"#;
        let schema = parse_qail(input).unwrap();

        assert_eq!(
            schema.enums[0].values,
            vec![
                "needs review".to_string(),
                "card,bank".to_string(),
                "quote \" ok".to_string(),
                "single ' ok".to_string(),
                String::new(),
            ]
        );
    }

    #[test]
    fn test_parse_enum_rejects_trailing_content_after_block() {
        let input = "enum status { active } garbage";
        let err = parse_qail(input).expect_err("trailing enum content should fail");
        assert!(err.contains("trailing content after enum block"));
    }

    #[test]
    fn test_parse_enum_rejects_missing_name() {
        let input = "enum { active }";
        let err = parse_qail(input).expect_err("missing enum name should fail");
        assert!(err.contains("enum name is missing before '{'"));
    }

    #[test]
    fn test_parse_enum_rejects_duplicate_values() {
        let input = "enum order_status { pending, paid, pending }";
        let err = parse_qail(input).expect_err("duplicate enum values should fail");
        assert!(err.contains("duplicate enum value 'pending'"));
    }

    #[test]
    fn test_parse_enum_rejects_duplicate_names() {
        let input = r#"
enum status { pending, approved }
enum status { draft, archived }
"#;
        let err = parse_qail(input).expect_err("duplicate enum declarations should fail");
        assert!(err.contains("duplicate enum declaration 'status'"));
    }

    #[test]
    fn test_parse_enum_rejects_empty_unquoted_values() {
        for input in [
            "enum order_status { pending,, paid }",
            "enum order_status { pending, }",
        ] {
            let err = parse_qail(input).expect_err("empty enum values should fail");
            assert!(err.contains("enum value is empty"), "{err}");
        }
    }

    #[test]
    fn test_enum_to_qail_string_round_trips_quoted_values() {
        let input = r#"enum status { "needs review", "card,bank", "quote "" ok", plain }"#;
        let schema = parse_qail(input).unwrap();
        let output = super::super::schema::to_qail_string(&schema);
        let reparsed = parse_qail(&output).unwrap();

        assert_eq!(reparsed.enums[0].values, schema.enums[0].values);
        assert!(
            output.contains(r#"enum status { "needs review", "card,bank", "quote "" ok", plain }"#)
        );
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
    fn test_parse_expression_index_ignores_commas_and_parens_inside_literals() {
        let input = r#"
index idx_docs_meta on docs (metadata->>'a,b', lower(title))
index idx_docs_regex on docs (regexp_replace(title, ')', '', 'g'))
"#;
        let schema = parse_qail(input).unwrap();

        assert_eq!(schema.indexes[0].expressions[0], "metadata->>'a,b'");
        assert_eq!(schema.indexes[0].expressions[1], "lower(title)");
        assert_eq!(
            schema.indexes[1].expressions[0],
            "regexp_replace(title, ')', '', 'g')"
        );
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
    fn test_parse_column_name_starting_with_foreign_key() {
        let input = r#"
table audits {
  foreign_key_id uuid
}
"#;
        let schema = parse_qail(input).unwrap();
        let table = &schema.tables["audits"];
        assert_eq!(table.columns.len(), 1);
        assert_eq!(table.columns[0].name, "foreign_key_id");
        assert!(table.multi_column_fks.is_empty());
    }

    #[test]
    fn test_parse_multi_column_fk_rejects_invalid_shapes() {
        for (input, expected) in [
            (
                "table bookings {\n  foreign_key () references schedules(id)\n}",
                "foreign_key local columns are required",
            ),
            (
                "table bookings {\n  foreign_key (route_id) references (id)\n}",
                "foreign_key referenced table is required",
            ),
            (
                "table bookings {\n  foreign_key (route_id,) references schedules(id)\n}",
                "foreign_key local columns are required",
            ),
            (
                "table bookings {\n  foreign_key (route_id, schedule_id) references schedules(id)\n}",
                "foreign_key local/ref column counts must match",
            ),
            (
                "table bookings {\n  foreign_key (route_id) references schedules(id) on_delete cascade\n}",
                "trailing content after foreign_key definition",
            ),
        ] {
            let err = parse_qail(input).expect_err("invalid multi-column fk should fail");
            assert!(err.contains(expected), "{err}");
        }
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
    fn test_parse_view_with_tagged_dollar_delimiter() {
        let input = r#"
view debug_sql $qail$
  SELECT '$$literal$$' AS sample
$qail$
"#;
        let schema = parse_qail(input).unwrap();
        let rendered = super::super::schema::to_qail_string(&schema);
        let reparsed = parse_qail(&rendered).unwrap();

        assert_eq!(schema.views[0].name, "debug_sql");
        assert!(schema.views[0].query.contains("$$literal$$"));
        assert_eq!(reparsed.views[0].query, schema.views[0].query);
    }

    #[test]
    fn test_parse_view_rejects_missing_name() {
        let input = "view $$ SELECT 1 $$";
        let err = parse_qail(input).expect_err("missing view name should fail");
        assert!(err.contains("view name is required"));
    }

    #[test]
    fn test_parse_view_rejects_duplicate_names() {
        let input = r#"
view active_users $$ SELECT 1 $$
materialized view active_users $$ SELECT 2 $$
"#;
        let err = parse_qail(input).expect_err("duplicate views should fail");
        assert!(err.contains("duplicate view declaration 'active_users'"));
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
    fn test_parse_function_with_volatility() {
        let input = "function is_super_admin() returns boolean language plpgsql stable $$ BEGIN RETURN true; END; $$";
        let schema = parse_qail(input).unwrap();
        assert_eq!(schema.functions.len(), 1);
        assert_eq!(schema.functions[0].name, "is_super_admin");
        assert_eq!(schema.functions[0].volatility.as_deref(), Some("stable"));
    }

    #[test]
    fn test_parse_function_rejects_missing_name() {
        let input = "function () returns int language sql $$ SELECT 1 $$";
        let err = parse_qail(input).expect_err("missing function name should fail");
        assert!(err.contains("function name is required"));
    }

    #[test]
    fn test_parse_function_rejects_missing_header_fields() {
        for (input, expected) in [
            (
                "function f() language sql $$ SELECT 1 $$",
                "function missing returns clause",
            ),
            (
                "function f() returns language sql $$ SELECT 1 $$",
                "function returns clause requires a type",
            ),
            (
                "function f() returns int $$ SELECT 1 $$",
                "function missing language clause",
            ),
        ] {
            let err = parse_qail(input).expect_err("missing function header field should fail");
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn test_parse_function_args_with_nested_type_parentheses() {
        let input = r#"
function normalize_amount(amount numeric(10,2), labels text[]) returns numeric language sql $$
  SELECT amount
$$
"#;
        let schema = parse_qail(input).unwrap();
        let func = &schema.functions[0];

        assert_eq!(
            func.args,
            vec![
                "amount numeric(10,2)".to_string(),
                "labels text[]".to_string()
            ]
        );
        assert_eq!(func.returns, "numeric");
        assert_eq!(func.language, "sql");
    }

    #[test]
    fn test_parse_function_returns_table_with_nested_type_parentheses() {
        let input = r#"
function report_amounts() returns table(id uuid, amount numeric(10,2), language text) language sql stable $$
  SELECT id, amount, language FROM reports
$$
"#;
        let schema = parse_qail(input).unwrap();
        let func = &schema.functions[0];

        assert_eq!(
            func.returns,
            "table(id uuid, amount numeric(10,2), language text)"
        );
        assert_eq!(func.language, "sql");
        assert_eq!(func.volatility.as_deref(), Some("stable"));
    }

    #[test]
    fn test_function_to_qail_string_round_trips_body_with_dollar_delimiter() {
        let input = r#"
function debug_notice() returns void language plpgsql $qail$
BEGIN
  RAISE NOTICE $$hello$$;
END;
$qail$
"#;
        let schema = parse_qail(input).unwrap();
        let rendered = super::super::schema::to_qail_string(&schema);
        let reparsed = parse_qail(&rendered).unwrap();

        assert!(rendered.contains("$qail$"));
        assert_eq!(reparsed.functions[0].body, schema.functions[0].body);
    }

    #[test]
    fn test_parse_function_rejects_duplicate_signatures() {
        let input = r#"
function normalize_email(email text) returns text language sql $$ SELECT lower(email) $$
function normalize_email(email text) returns text language sql $$ SELECT trim(email) $$
"#;
        let err = parse_qail(input).expect_err("duplicate function signatures should fail");
        assert!(err.contains("duplicate function declaration 'normalize_email(email text)'"));
    }

    #[test]
    fn test_parse_function_allows_overloads() {
        let input = r#"
function normalize_email(email text) returns text language sql $$ SELECT lower(email) $$
function normalize_email(email text, fallback text) returns text language sql $$ SELECT lower(email) $$
"#;
        let schema = parse_qail(input).expect("function overloads should parse");
        assert_eq!(schema.functions.len(), 2);
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
    fn test_parse_trigger_rejects_missing_event() {
        let input = "trigger trg_updated_at on users before execute set_updated_at";
        let err = parse_qail(input).expect_err("missing trigger event should fail");
        assert!(err.contains("trigger requires at least one event"));
    }

    #[test]
    fn test_parse_trigger_rejects_invalid_timing() {
        let input = "trigger trg_updated_at on users during update execute set_updated_at";
        let err = parse_qail(input).expect_err("invalid trigger timing should fail");
        assert!(err.contains("unsupported trigger timing: DURING"));
    }

    #[test]
    fn test_parse_trigger_rejects_invalid_event() {
        let input = "trigger trg_updated_at on users before banana execute set_updated_at";
        let err = parse_qail(input).expect_err("invalid trigger event should fail");
        assert!(err.contains("unsupported trigger event: BANANA"));
    }

    #[test]
    fn test_parse_trigger_rejects_trailing_content() {
        let input = "trigger trg_updated_at on users before update execute set_updated_at garbage";
        let err = parse_qail(input).expect_err("trailing trigger content should fail");
        assert!(err.contains("trailing content after trigger function"));
    }

    #[test]
    fn test_parse_trigger_rejects_empty_update_of_columns() {
        for (input, expected) in [
            (
                "trigger trg_updated_at on users before update of , execute set_updated_at",
                "trigger update of contains an empty column",
            ),
            (
                "trigger trg_updated_at on users before update of name, execute set_updated_at",
                "trigger update of contains an empty column",
            ),
        ] {
            let err = parse_qail(input).expect_err("empty update-of columns should fail");
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn test_parse_trigger_rejects_duplicate_update_of_columns() {
        let input =
            "trigger trg_updated_at on users before update of name,name execute set_updated_at";
        let err = parse_qail(input).expect_err("duplicate update-of columns should fail");
        assert!(err.contains("duplicate trigger update column 'name'"));
    }

    #[test]
    fn test_parse_trigger_rejects_duplicate_events() {
        for input in [
            "trigger trg_updated_at on users before update or update execute set_updated_at",
            "trigger trg_updated_at on users before update of name or update execute set_updated_at",
        ] {
            let err = parse_qail(input).expect_err("duplicate trigger events should fail");
            assert!(err.contains("duplicate trigger event: UPDATE"), "{err}");
        }
    }

    #[test]
    fn test_parse_trigger_rejects_duplicate_table_scoped_names() {
        let input = r#"
trigger trg_updated_at on users before update execute touch_users
trigger trg_updated_at on users after insert execute touch_users
"#;
        let err = parse_qail(input).expect_err("duplicate trigger should fail");
        assert!(err.contains("duplicate trigger declaration 'trg_updated_at on users'"));
    }

    #[test]
    fn test_parse_trigger_allows_same_name_on_different_tables() {
        let input = r#"
trigger audit_change on users after update execute audit_user
trigger audit_change on posts after update execute audit_post
"#;
        let schema = parse_qail(input).expect("same trigger name on different tables should parse");
        assert_eq!(schema.triggers.len(), 2);
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
    fn test_parse_grant_rejects_unknown_privilege() {
        let input = "grant selcet on users to app_role";
        let err = parse_qail(input).expect_err("unknown grant privilege should fail");
        assert!(err.contains("unknown grant/revoke privilege: SELCET"));
    }

    #[test]
    fn test_parse_grant_rejects_duplicate_privileges() {
        let input = "grant select, SELECT on users to app_role";
        let err = parse_qail(input).expect_err("duplicate grant privilege should fail");
        assert!(err.contains("duplicate grant/revoke privilege: SELECT"));
    }

    #[test]
    fn test_parse_grant_rejects_all_with_specific_privileges() {
        for input in [
            "grant all, select on users to app_role",
            "revoke select, all on users from app_role",
        ] {
            let err = parse_qail(input).expect_err("mixed ALL grant privileges should fail");
            assert!(err.contains("ALL privilege cannot be combined with specific privileges"));
        }
    }

    #[test]
    fn test_parse_grant_rejects_missing_object_or_role() {
        let missing_object = "grant select on  to app_role";
        let err = parse_qail(missing_object).expect_err("missing grant object should fail");
        assert!(err.contains("grant/revoke object is required"));

        let missing_role = "revoke select on users from ";
        let err = parse_qail(missing_role).expect_err("missing revoke role should fail");
        assert!(err.contains("grant/revoke role is required"));
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
    fn test_parse_fk_references_dot_form() {
        let input = r#"
table orders {
  id uuid primary_key
  user_id uuid references(users.id) on_delete cascade on_update restrict
}
"#;
        let schema = parse_qail(input).unwrap();
        let fk = schema.tables["orders"].columns[1]
            .foreign_key
            .as_ref()
            .unwrap();
        assert_eq!(fk.table, "users");
        assert_eq!(fk.column, "id");
        assert!(matches!(fk.on_delete, FkAction::Cascade));
        assert!(matches!(fk.on_update, FkAction::Restrict));
    }

    #[test]
    fn test_parse_fk_rejects_unknown_action() {
        let input = r#"
table orders {
  id uuid primary_key
  user_id uuid references users(id) on_delete cascad
}
"#;
        let err = parse_qail(input).expect_err("unknown foreign key action should fail");
        assert!(err.contains("unknown foreign key action: cascad"));
    }

    #[test]
    fn test_parse_fk_rejects_missing_action() {
        let input = r#"
table orders {
  id uuid primary_key
  user_id uuid references users(id) on_delete
}
"#;
        let err = parse_qail(input).expect_err("missing foreign key action should fail");
        assert!(err.contains("on_delete requires a foreign key action"));
    }

    #[test]
    fn test_parse_fk_rejects_duplicate_actions() {
        for (input, expected) in [
            (
                r#"
table orders {
  id uuid primary_key
  user_id uuid references users(id) on_delete cascade on_delete restrict
}
"#,
                "duplicate on_delete action",
            ),
            (
                r#"
table orders {
  id uuid primary_key
  user_id uuid references users(id) on_update cascade on_update restrict
}
"#,
                "duplicate on_update action",
            ),
        ] {
            let err = parse_qail(input).expect_err("duplicate foreign key action should fail");
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn test_parse_fk_rejects_invalid_reference_target() {
        for (input, expected) in [
            (
                r#"
table orders {
  id uuid primary_key
  user_id uuid references
}
"#,
                "foreign key reference target is required",
            ),
            (
                r#"
table orders {
  id uuid primary_key
  user_id uuid referencesusers(id)
}
"#,
                "unknown column option 'referencesusers(id)' for column 'user_id'",
            ),
            (
                r#"
table orders {
  id uuid primary_key
  user_id uuid references users
}
"#,
                "invalid foreign key reference target: users",
            ),
            (
                r#"
table orders {
  id uuid primary_key
  user_id uuid references users()
}
"#,
                "invalid foreign key reference target: users()",
            ),
            (
                r#"
table orders {
  id uuid primary_key
  user_id uuid references users(i-d)
}
"#,
                "invalid foreign key reference target: users(i-d)",
            ),
            (
                r#"
table orders {
  id uuid primary_key
  user_id uuid references bad-table(id)
}
"#,
                "invalid foreign key reference target: bad-table(id)",
            ),
            (
                r#"
table orders {
  id uuid primary_key
  user_id uuid references(users.i-d)
}
"#,
                "invalid foreign key reference target: references(users.i-d)",
            ),
        ] {
            let err = parse_qail(input).expect_err("invalid foreign key target should fail");
            assert!(err.contains(expected), "{err}");
        }
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
        let CheckExpr::Between { column, low, high } = expr else {
            panic!("Expected Between, got {expr:?}");
        };
        assert_eq!(column, "age");
        assert_eq!(*low, 0);
        assert_eq!(*high, 200);
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
        let CheckExpr::GreaterOrEqual { column, value } = expr else {
            panic!("Expected GreaterOrEqual, got {expr:?}");
        };
        assert_eq!(column, "score");
        assert_eq!(*value, 0);
    }

    #[test]
    fn test_parse_check_in_round_trips_quoted_values() {
        let input = r#"
table tickets {
  status text check(status in [draft, "needs review", "card,bank", "quote "" ok", ""])
}
"#;
        let schema = parse_qail(input).unwrap();
        let col = &schema.tables["tickets"].columns[0];
        let CheckExpr::In { column, values } = &col.check.as_ref().unwrap().expr else {
            panic!("Expected In, got {:?}", col.check);
        };

        assert_eq!(column, "status");
        assert_eq!(
            values,
            &[
                "draft".to_string(),
                "needs review".to_string(),
                "card,bank".to_string(),
                "quote \" ok".to_string(),
                String::new(),
            ]
        );

        let rendered = super::super::schema::to_qail_string(&schema);
        let reparsed = parse_qail(&rendered).unwrap();
        let CheckExpr::In {
            column: reparsed_column,
            values: reparsed_values,
        } = &reparsed.tables["tickets"].columns[0]
            .check
            .as_ref()
            .unwrap()
            .expr
        else {
            panic!("Expected reparsed In");
        };
        assert_eq!(reparsed_column, column);
        assert_eq!(reparsed_values, values);
        assert!(
            rendered
                .contains(r#"status in [draft, "needs review", "card,bank", "quote "" ok", ""]"#)
        );
    }

    #[test]
    fn test_parse_check_logical_operators_ignore_quoted_values() {
        let input = r#"
table tickets {
  status text check(status in ["needs and review", ready] and score >= 0)
  title text check(title ~ 'rock and roll')
}
"#;
        let schema = parse_qail(input).unwrap();
        let status_check = &schema.tables["tickets"].columns[0]
            .check
            .as_ref()
            .unwrap()
            .expr;
        let CheckExpr::And(left, right) = status_check else {
            panic!("Expected And, got {status_check:?}");
        };
        assert!(matches!(
            left.as_ref(),
            CheckExpr::In { values, .. } if values == &["needs and review".to_string(), "ready".to_string()]
        ));
        assert!(matches!(
            right.as_ref(),
            CheckExpr::GreaterOrEqual { column, value } if column == "score" && *value == 0
        ));

        let title_check = &schema.tables["tickets"].columns[1]
            .check
            .as_ref()
            .unwrap()
            .expr;
        assert!(matches!(
            title_check,
            CheckExpr::Sql(sql) if sql == "title ~ 'rock and roll'"
        ));
    }

    #[test]
    fn test_parse_default_expression_with_spaces_and_cast() {
        let input = r#"
table idempotency_keys {
  expires_at timestamptz default (now() + '24:00:00'::interval)
}
"#;
        let schema = parse_qail(input).unwrap();
        let col = &schema.tables["idempotency_keys"].columns[0];
        assert_eq!(
            col.default.as_deref(),
            Some("(now() + '24:00:00'::interval)")
        );
    }

    #[test]
    fn test_parse_default_rejects_missing_value() {
        let input = r#"
table idempotency_keys {
  expires_at timestamptz default
}
"#;
        let err = parse_qail(input).expect_err("missing default value should fail");
        assert!(err.contains("default requires a value for column 'expires_at'"));
    }

    #[test]
    fn test_parse_check_expression_falls_back_to_raw() {
        let input = r#"
table vendors {
  name text check(char_length(btrim(name::text)) > 0)
}
"#;
        let schema = parse_qail(input).unwrap();
        let col = &schema.tables["vendors"].columns[0];
        let expr = &col.check.as_ref().unwrap().expr;
        match expr {
            CheckExpr::Sql(raw) => assert_eq!(raw, "char_length(btrim(name::text)) > 0"),
            CheckExpr::GreaterThan { column, value } => {
                assert_eq!(column, "char_length(btrim(name::text))");
                assert_eq!(*value, 0);
            }
            other => panic!("Expected raw-or-greater-than check expression, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_check_rejects_invalid_expression_shape() {
        for (input, expected) in [
            (
                r#"
table tickets {
  id uuid primary_key
  score int check(score >= 0
}
"#,
                "unclosed check expression",
            ),
            (
                r#"
table tickets {
  id uuid primary_key
  score int check()
}
"#,
                "check expression is empty",
            ),
        ] {
            let err = parse_qail(input).expect_err("invalid check expression should fail");
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn test_parse_check_name_rejects_invalid_shape() {
        for (input, expected) in [
            (
                r#"
table tickets {
  id uuid primary_key
  score int check_name positive_score
}
"#,
                "check_name requires a preceding check expression",
            ),
            (
                r#"
table tickets {
  id uuid primary_key
  score int check(score >= 0) check_name
}
"#,
                "check_name requires a name",
            ),
        ] {
            let err = parse_qail(input).expect_err("invalid check name shape should fail");
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn test_parse_check_rejects_duplicates() {
        for (input, expected) in [
            (
                r#"
table products {
  score int check(score >= 0) check(score <= 100)
}
"#,
                "duplicate check expression for column 'score'",
            ),
            (
                r#"
table products {
  score int check(score >= 0) check_name score_min check_name score_min_again
}
"#,
                "duplicate check_name for column 'score'",
            ),
        ] {
            let err = parse_qail(input).expect_err("duplicate check metadata should fail");
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn test_parse_column_rejects_unknown_option() {
        let input = r#"
table users {
  id uuid primary_key
  email text uniq
}
"#;
        let err = parse_qail(input).expect_err("unknown column option should fail");
        assert!(err.contains("unknown column option 'uniq' for column 'email'"));
    }

    #[test]
    fn test_parse_column_rejects_conflicting_nullability() {
        for (input, expected) in [
            (
                r#"
table users {
  email text not_null nullable
}
"#,
                "conflicting nullability options 'not_null' and 'nullable' for column 'email'",
            ),
            (
                r#"
table users {
  email text nullable not_null
}
"#,
                "conflicting nullability options 'nullable' and 'not_null' for column 'email'",
            ),
            (
                r#"
table users {
  id uuid primary_key nullable
}
"#,
                "nullable conflicts with primary_key for column 'id'",
            ),
            (
                r#"
table users {
  id uuid nullable primary_key
}
"#,
                "primary_key conflicts with nullable for column 'id'",
            ),
        ] {
            let err = parse_qail(input).expect_err("conflicting nullability should fail");
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn test_parse_column_rejects_duplicate_default() {
        let input = r#"
table users {
  status text default 'draft' default 'active'
}
"#;
        let err = parse_qail(input).expect_err("duplicate default should fail");
        assert!(err.contains("duplicate default option for column 'status'"));
    }

    #[test]
    fn test_parse_column_rejects_duplicate_key_options() {
        for (input, expected) in [
            (
                r#"
table users {
  id uuid primary_key primary_key
}
"#,
                "duplicate primary_key option for column 'id'",
            ),
            (
                r#"
table users {
  email text unique unique
}
"#,
                "duplicate unique option for column 'email'",
            ),
        ] {
            let err = parse_qail(input).expect_err("duplicate key option should fail");
            assert!(err.contains(expected), "{err}");
        }
    }

    #[test]
    fn test_parse_column_rejects_duplicate_generated_options() {
        let input = r#"
table users {
  id bigint generated_identity generated_by_default_identity
}
"#;
        let err = parse_qail(input).expect_err("duplicate generated option should fail");
        assert!(err.contains("duplicate generated option for column 'id'"));
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
        let ColumnType::Enum { name, values } = &col.data_type else {
            panic!("Expected Enum type, got {:?}", col.data_type);
        };
        assert_eq!(name, "ticket_status");
        assert_eq!(values, &["draft", "active", "cancelled"]);
        assert_eq!(col.default.as_deref(), Some("'draft'"));
    }

    #[test]
    fn test_generated_columns_roundtrip_and_emit_sql() {
        use crate::transpiler::{Dialect, ToSql};

        let input = r#"
table people {
  first_name text
  last_name text
  full_name text generated_stored(first_name || ' ' || last_name)
  row_seq bigint generated_by_default_identity
}
"#;
        let schema = parse_qail(input).expect("generated columns should parse");
        let table = &schema.tables["people"];

        assert!(matches!(
            table.columns[2].generated.as_ref(),
            Some(Generated::AlwaysStored(expr)) if expr == "first_name || ' ' || last_name"
        ));
        assert!(matches!(
            table.columns[3].generated.as_ref(),
            Some(Generated::ByDefaultIdentity)
        ));

        let rendered = super::super::schema::to_qail_string(&schema);
        assert!(rendered.contains("generated_stored(first_name || ' ' || last_name)"));
        assert!(rendered.contains("generated_by_default_identity"));

        let reparsed = parse_qail(&rendered).expect("rendered generated columns should parse");
        assert!(matches!(
            reparsed.tables["people"].columns[2].generated.as_ref(),
            Some(Generated::AlwaysStored(expr)) if expr == "first_name || ' ' || last_name"
        ));
        assert!(matches!(
            reparsed.tables["people"].columns[3].generated.as_ref(),
            Some(Generated::ByDefaultIdentity)
        ));

        let sql = super::super::schema::schema_to_commands(&reparsed)
            .into_iter()
            .map(|cmd| cmd.to_sql_with_dialect(Dialect::Postgres))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(sql.contains("GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED"));
        assert!(sql.contains("GENERATED BY DEFAULT AS IDENTITY"));
    }

    #[test]
    fn test_parse_generated_stored_rejects_invalid_expression() {
        for (input, expected) in [
            (
                r#"
table invoices {
  id uuid primary_key
  total numeric generated_stored(subtotal + tax
}
"#,
                "unclosed generated_stored expression",
            ),
            (
                r#"
table invoices {
  id uuid primary_key
  total numeric generated_stored()
}
"#,
                "generated_stored expression is empty",
            ),
        ] {
            let err = parse_qail(input).expect_err("invalid generated expression should fail");
            assert!(err.contains(expected), "{err}");
        }
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
        assert!(
            matches!(&method.data_type, ColumnType::Enum { name, .. } if name == "payment_method")
        );
        assert_eq!(method.default.as_deref(), Some("'card'"));

        // FK with cascade
        let user_id = &table.columns[2];
        let fk = user_id.foreign_key.as_ref().unwrap();
        assert!(matches!(fk.on_delete, FkAction::Cascade));

        // CHECK >= 0
        let score = &table.columns[3];
        assert!(matches!(
            &score.check.as_ref().unwrap().expr,
            CheckExpr::GreaterOrEqual { .. }
        ));

        // CHECK between
        let age = &table.columns[4];
        assert!(matches!(
            &age.check.as_ref().unwrap().expr,
            CheckExpr::Between { .. }
        ));
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

    #[test]
    fn test_parse_rejects_invalid_primary_key_type() {
        let input = r#"
table bad_pk {
  id jsonb primary_key
}
"#;
        let err = parse_qail(input).expect_err("JSONB primary key should be rejected");
        assert!(err.contains("cannot be a primary key"));
    }

    #[test]
    fn test_parse_accepts_date_primary_key_type() {
        let input = r#"
table daily_stats {
  date date primary_key
}
"#;
        let schema = parse_qail(input).expect("DATE primary key should be accepted");
        let table = &schema.tables["daily_stats"];
        assert_eq!(table.columns.len(), 1);
        assert!(table.columns[0].primary_key);
    }

    #[test]
    fn test_parse_policy_fallback_keeps_unsupported_expression() {
        let input = r#"
table seo_comparisons {
  id uuid primary_key
}

policy seo_comparisons_admin on seo_comparisons for all
  using $$ status = 'cancelled'::text $$
"#;

        let schema = parse_qail(input).expect("policy parser should fall back to raw expr");
        let policy = schema
            .policies
            .iter()
            .find(|p| p.name == "seo_comparisons_admin")
            .expect("policy missing");

        match policy.using.as_ref() {
            Some(Expr::Named(expr)) => {
                assert!(expr.contains("status = 'cancelled'::text"));
            }
            other => panic!("expected fallback Expr::Named, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_policy_preserves_role_and_restrictive_from_roundtrip_header() {
        let input = r#"
table docs {
  id uuid primary_key
}

policy docs_select on docs for select to app_user restrictive
  using $$ owner_id = current_setting('app.current_user_id')::uuid $$
"#;

        let schema = parse_qail(input).expect("policy should parse");
        let policy = schema.policies.first().expect("policy missing");

        assert_eq!(policy.target, PolicyTarget::Select);
        assert_eq!(policy.role.as_deref(), Some("app_user"));
        assert_eq!(policy.permissiveness, PolicyPermissiveness::Restrictive);

        let sql = crate::transpiler::policy::create_policy_sql(policy);
        assert!(sql.contains("AS RESTRICTIVE"));
        assert!(sql.contains("FOR SELECT"));
        assert!(sql.contains("TO app_user"));

        let rendered = super::super::schema::to_qail_string(&schema);
        assert!(rendered.contains("policy docs_select on docs for select to app_user restrictive"));

        let reparsed = parse_qail(&rendered).expect("rendered policy should parse");
        let reparsed_policy = reparsed.policies.first().expect("reparsed policy missing");
        assert_eq!(reparsed_policy.target, PolicyTarget::Select);
        assert_eq!(reparsed_policy.role.as_deref(), Some("app_user"));
        assert_eq!(
            reparsed_policy.permissiveness,
            PolicyPermissiveness::Restrictive
        );
    }

    #[test]
    fn test_parse_policy_preserves_split_line_role_and_permissiveness_clauses() {
        let input = r#"
table docs {
  id uuid primary_key
}

policy docs_select on docs
  for select
  restrictive
  to app_user
  using $$ owner_id = current_setting('app.current_user_id')::uuid $$
"#;

        let schema = parse_qail(input).expect("policy should parse");
        let policy = schema.policies.first().expect("policy missing");

        assert_eq!(policy.target, PolicyTarget::Select);
        assert_eq!(policy.role.as_deref(), Some("app_user"));
        assert_eq!(policy.permissiveness, PolicyPermissiveness::Restrictive);
    }

    #[test]
    fn test_parse_policy_rejects_duplicate_table_scoped_names() {
        let input = r#"
policy tenant_isolation on docs for select
policy tenant_isolation on docs for update
"#;
        let err = parse_qail(input).expect_err("duplicate policy should fail");
        assert!(err.contains("duplicate policy declaration 'tenant_isolation on docs'"));
    }

    #[test]
    fn test_parse_policy_allows_same_name_on_different_tables() {
        let input = r#"
policy tenant_isolation on docs for select
policy tenant_isolation on folders for select
"#;
        let schema = parse_qail(input).expect("same policy name on different tables should parse");
        assert_eq!(schema.policies.len(), 2);
    }

    #[test]
    fn test_parse_rejects_unclosed_resource_block() {
        let input = r#"
bucket avatars {
  provider s3
  region "ap-southeast-1"
"#;

        let err = parse_qail(input).expect_err("unclosed resource block should be rejected");
        assert!(err.contains("Unclosed bucket resource block"));
    }

    #[test]
    fn test_parse_resource_rejects_trailing_content_without_block() {
        let err = parse_qail("bucket avatars provider s3")
            .expect_err("resource trailing content should fail");
        assert!(err.contains("Trailing content after bucket resource name"));
    }

    #[test]
    fn test_parse_resource_rejects_property_without_value() {
        let input = r#"
bucket avatars {
  provider
}
"#;
        let err = parse_qail(input).expect_err("resource property without value should fail");
        assert!(err.contains("Resource property 'provider' in 'avatars' requires a value"));
    }

    #[test]
    fn test_parse_resource_rejects_duplicate_properties() {
        let input = r#"
bucket avatars {
  provider s3
  provider gcs
}
"#;
        let err = parse_qail(input).expect_err("duplicate resource properties should fail");
        assert!(err.contains("Duplicate resource property 'provider' in 'avatars'"));
    }

    #[test]
    fn test_parse_resource_rejects_duplicate_names() {
        let input = r#"
bucket notifications { provider s3 }
queue notifications { provider sqs }
"#;
        let err = parse_qail(input).expect_err("duplicate resources should fail");
        assert!(err.contains("duplicate resource declaration 'notifications'"));
    }

    #[test]
    fn test_parse_resource_preserves_quoted_property_values() {
        let input = r#"
bucket avatars {
  provider s3
  display_name "Profile Images"
  region 'ap southeast 1'
}
"#;

        let schema = parse_qail(input).expect("resource should parse");
        let resource = schema.resources.first().expect("resource missing");

        assert_eq!(resource.provider.as_deref(), Some("s3"));
        assert_eq!(
            resource.properties.get("display_name").map(String::as_str),
            Some("Profile Images")
        );
        assert_eq!(
            resource.properties.get("region").map(String::as_str),
            Some("ap southeast 1")
        );
    }

    #[test]
    fn test_parse_resource_ignores_braces_inside_quoted_values() {
        let input = r#"
bucket avatars {
  provider s3
  label "Profile } Images"
}
"#;

        let schema = parse_qail(input).expect("resource should parse");
        let resource = schema.resources.first().expect("resource missing");

        assert_eq!(
            resource.properties.get("label").map(String::as_str),
            Some("Profile } Images")
        );
    }

    #[test]
    fn test_parse_rejects_invalid_unique_type() {
        let input = r#"
table bad_unique {
  payload jsonb unique
}
"#;
        let err = parse_qail(input).expect_err("JSONB unique should be rejected");
        assert!(err.contains("cannot have UNIQUE"));
    }

    #[test]
    fn test_parse_rejects_unknown_column_type() {
        let input = r#"
table bad_type {
  data mysterytype
}
"#;
        let err = parse_qail(input).expect_err("unknown type should be rejected");
        assert!(err.contains("Unknown column type"));
    }
}
