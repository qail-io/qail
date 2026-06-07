use std::collections::HashSet;

use super::super::rust_ast::sql_semantics::{SqlStmtKind, classify_sql_kind};

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

pub(super) fn normalize_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(super) fn sanitize_sql_for_reference_scan(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"--") {
            while i < bytes.len() {
                let b = bytes[i];
                if b == b'\n' {
                    out.push('\n');
                    i += 1;
                    break;
                }
                out.push(' ');
                i += 1;
            }
            continue;
        }

        if starts_with_bytes(bytes, i, b"/*") {
            out.push(' ');
            out.push(' ');
            i += 2;
            while i < bytes.len() {
                if starts_with_bytes(bytes, i, b"*/") {
                    out.push(' ');
                    out.push(' ');
                    i += 2;
                    break;
                }
                push_sql_sanitized_byte(bytes[i], &mut out);
                i += 1;
            }
            continue;
        }

        if bytes[i] == b'\'' {
            out.push(' ');
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    out.push(' ');
                    i += 1;
                    if bytes.get(i).copied() == Some(b'\'') {
                        out.push(' ');
                        i += 1;
                        continue;
                    }
                    break;
                }
                push_sql_sanitized_byte(bytes[i], &mut out);
                i += 1;
            }
            continue;
        }

        if let Some((tag, end)) = sql_dollar_quote_tag(input, i) {
            for b in &bytes[i..end] {
                push_sql_sanitized_byte(*b, &mut out);
            }
            i = end;
            while i < bytes.len() {
                if input
                    .get(i..)
                    .is_some_and(|tail| tail.starts_with(tag.as_str()))
                {
                    for b in &bytes[i..i + tag.len()] {
                        push_sql_sanitized_byte(*b, &mut out);
                    }
                    i += tag.len();
                    break;
                }
                push_sql_sanitized_byte(bytes[i], &mut out);
                i += 1;
            }
            continue;
        }

        out.push(bytes[i] as char);
        i += 1;
    }

    out
}

fn push_sql_sanitized_byte(byte: u8, out: &mut String) {
    if byte == b'\n' {
        out.push('\n');
    } else {
        out.push(' ');
    }
}

fn sql_dollar_quote_tag(input: &str, start: usize) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    if bytes.get(start).copied() != Some(b'$') {
        return None;
    }
    if bytes.get(start + 1).copied() == Some(b'$') {
        return Some(("$$".to_string(), start + 2));
    }

    let mut cursor = start + 1;
    let first = bytes.get(cursor).copied()?;
    if first.is_ascii_digit() || !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    cursor += 1;

    while cursor < bytes.len() && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_') {
        cursor += 1;
    }
    if bytes.get(cursor).copied() != Some(b'$') {
        return None;
    }

    Some((input.get(start..=cursor)?.to_string(), cursor + 1))
}

fn starts_with_bytes(haystack: &[u8], idx: usize, needle: &[u8]) -> bool {
    haystack
        .get(idx..idx.saturating_add(needle.len()))
        .is_some_and(|slice| slice == needle)
}

fn advance_sql_quoted_index(bytes: &[u8], idx: usize, quote: u8) -> Option<usize> {
    let b = *bytes.get(idx)?;
    if b == quote {
        if bytes.get(idx + 1).copied() == Some(quote) {
            return Some(idx + 2);
        }
        return None;
    }
    if b == b'\\' && idx + 1 < bytes.len() {
        return Some(idx + 2);
    }
    Some(idx + 1)
}

fn push_column_ref(name: &str, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    let name = name.trim();
    if !name.is_empty() && seen.insert(name.to_string()) {
        cols.push(name.to_string());
    }
}

pub(super) fn parse_sql_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    parse_sql_references_with_cte_aliases(sql, &[])
}

fn parse_sql_references_with_cte_aliases(
    sql: &str,
    inherited_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let sanitized = sanitize_sql_for_reference_scan(sql);
    let normalized = normalize_whitespace(&sanitized);
    let cte_parts = parse_sql_cte_parts(&normalized, inherited_cte_aliases);
    let mut refs = cte_parts
        .as_ref()
        .map(|parts| parts.references.clone())
        .unwrap_or_default();
    let local_cte_aliases = cte_parts
        .as_ref()
        .map(|parts| parts.aliases.as_slice())
        .unwrap_or(&[]);

    let classified_kind = classify_sql_kind(&normalized);

    if classified_kind == Some(SqlStmtKind::Select) {
        refs.extend(parse_sql_select_references(
            &normalized,
            inherited_cte_aliases,
            local_cte_aliases,
        ));
        refs.extend(parse_sql_nested_query_references(
            &normalized,
            inherited_cte_aliases,
            local_cte_aliases,
        ));
        refs.extend(parse_sql_set_operation_references(
            &normalized,
            inherited_cte_aliases,
            local_cte_aliases,
        ));
        dedupe_sql_references(&mut refs);
        return refs;
    }

    if let Some(kind) = classified_kind
        && is_sql_utility_reference_kind(kind)
    {
        refs.extend(parse_sql_utility_references(
            &normalized,
            kind,
            inherited_cte_aliases,
            local_cte_aliases,
        ));
        refs.extend(parse_sql_nested_query_references(
            &normalized,
            inherited_cte_aliases,
            local_cte_aliases,
        ));
        dedupe_sql_references(&mut refs);
        return refs;
    }

    if let Some((kind, table, columns)) = parse_sql_reference(&normalized) {
        let is_cte_alias = cte_parts.as_ref().is_some_and(|parts| {
            parts
                .aliases
                .iter()
                .any(|alias| sql_ident_eq(alias, &table))
        });
        let is_inherited_cte_alias = inherited_cte_aliases
            .iter()
            .any(|alias| sql_ident_eq(alias, &table));
        if !is_cte_alias && !is_inherited_cte_alias {
            refs.push((kind, table, columns));
        }
        refs.extend(parse_sql_auxiliary_write_references(
            &normalized,
            kind,
            inherited_cte_aliases,
            local_cte_aliases,
        ));
        refs.extend(parse_sql_insert_select_references(
            &normalized,
            kind,
            inherited_cte_aliases,
            local_cte_aliases,
        ));
    }
    refs.extend(parse_sql_nested_query_references(
        &normalized,
        inherited_cte_aliases,
        local_cte_aliases,
    ));
    dedupe_sql_references(&mut refs);

    refs
}

fn is_sql_utility_reference_kind(kind: SqlStmtKind) -> bool {
    matches!(
        kind,
        SqlStmtKind::Create
            | SqlStmtKind::Alter
            | SqlStmtKind::Comment
            | SqlStmtKind::Grant
            | SqlStmtKind::Revoke
            | SqlStmtKind::Analyze
            | SqlStmtKind::Vacuum
            | SqlStmtKind::Reindex
            | SqlStmtKind::Cluster
            | SqlStmtKind::Refresh
            | SqlStmtKind::Drop
    )
}

fn parse_sql_nested_query_references(
    sql: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let bytes = sql.as_bytes();
    let mut refs = Vec::new();
    let mut i = 0usize;
    let mut in_quote: Option<u8> = None;
    let mut aliases = inherited_cte_aliases.to_vec();
    aliases.extend(local_cte_aliases.iter().cloned());

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if let Some(next) = advance_sql_quoted_index(bytes, i, q) {
                i = next;
                continue;
            }
            in_quote = None;
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => {
                if let Some((segment, end)) = balanced_paren_segment(sql, i) {
                    let nested = normalize_whitespace(segment.trim());
                    if classify_sql_kind(&nested).is_some() {
                        refs.extend(parse_sql_references_with_cte_aliases(&nested, &aliases));
                        i = end;
                        continue;
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }

    refs
}

fn dedupe_sql_references(refs: &mut Vec<(SqlStmtKind, String, Vec<String>)>) {
    let mut seen = HashSet::new();
    refs.retain(|(kind, table, columns)| {
        seen.insert(format!(
            "{}\x1e{}\x1e{}",
            kind.as_str(),
            table,
            columns.join("\x1f")
        ))
    });
}

#[derive(Debug, Clone)]
struct SqlTableSource {
    table: String,
    alias: String,
}

fn parse_sql_select_references(
    sql: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(select_idx) = find_keyword_top_level_from(sql, "SELECT", 0) else {
        return Vec::new();
    };
    let Some(from_idx) = find_keyword_top_level_from(sql, "FROM", select_idx + "SELECT".len())
    else {
        return Vec::new();
    };
    let Some(columns_raw) = sql
        .get(select_idx + "SELECT".len()..from_idx)
        .map(str::trim)
    else {
        return Vec::new();
    };

    let sources = parse_sql_select_table_sources(
        sql,
        from_idx + "FROM".len(),
        inherited_cte_aliases,
        local_cte_aliases,
    );
    if sources.is_empty() {
        return Vec::new();
    }

    let columns_by_source =
        collect_sql_select_columns_by_source(sql, columns_raw, from_idx, &sources);

    sources
        .into_iter()
        .zip(columns_by_source)
        .map(|(source, columns)| (SqlStmtKind::Select, source.table, columns))
        .collect()
}

fn parse_sql_select_table_sources(
    sql: &str,
    start: usize,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<SqlTableSource> {
    let mut sources = Vec::new();
    let mut cursor = start;
    let from_end = top_level_sql_clause_start(
        sql,
        start,
        &[
            "WHERE",
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "LIMIT",
            "OFFSET",
            "FETCH",
            "FOR",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "WINDOW",
            "RETURNING",
        ],
    )
    .unwrap_or(sql.len());

    loop {
        cursor = skip_sql_ws(sql.as_bytes(), cursor);
        if cursor >= from_end {
            break;
        }

        if starts_with_keyword_at(sql, cursor, "LATERAL") {
            cursor = skip_sql_ws(sql.as_bytes(), cursor + "LATERAL".len());
        }
        if starts_with_keyword_at(sql, cursor, "ONLY") {
            cursor = skip_sql_ws(sql.as_bytes(), cursor + "ONLY".len());
        }

        let source_end = if let Some(rows_from_end) = skip_sql_rows_from_source(sql, cursor) {
            rows_from_end
        } else if sql.as_bytes().get(cursor).copied() == Some(b'(') {
            balanced_paren_segment(sql, cursor)
                .map(|(_, end)| end)
                .unwrap_or(cursor)
        } else if let Some((table, table_end)) = parse_sql_object_name_with_end(sql, cursor) {
            let table_ref_end = skip_sql_table_inheritance_star(sql, table_end);
            let after_table = skip_sql_ws(sql.as_bytes(), table_ref_end);
            if sql.as_bytes().get(after_table).copied() == Some(b'(') {
                let function_end = balanced_paren_segment(sql, after_table)
                    .map(|(_, end)| end)
                    .unwrap_or(after_table);
                let (_, alias_end) = parse_sql_optional_table_alias(sql, function_end);
                alias_end
            } else {
                let tablesample_end = skip_sql_tablesample_clause(sql, table_ref_end);
                let (alias, alias_end) = parse_sql_optional_table_alias(sql, tablesample_end);
                let source_end = skip_sql_tablesample_clause(sql, alias_end.max(tablesample_end));
                if !is_sql_cte_alias(&table, inherited_cte_aliases, local_cte_aliases) {
                    sources.push(SqlTableSource {
                        alias: alias.unwrap_or_else(|| table.clone()),
                        table,
                    });
                }
                source_end.max(alias_end).max(tablesample_end)
            }
        } else {
            break;
        };

        let Some(next_start) = next_sql_table_source_start(sql, source_end, from_end) else {
            break;
        };
        cursor = next_start;
    }

    sources
}

fn parse_sql_optional_table_alias(sql: &str, start: usize) -> (Option<String>, usize) {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, start);
    if starts_with_keyword_at(sql, cursor, "AS") {
        cursor = skip_sql_ws(bytes, cursor + "AS".len());
    }

    let Some((alias, end)) = parse_sql_identifier_segment(sql, cursor) else {
        return (None, start);
    };
    if is_sql_table_source_boundary(&alias) {
        return (None, start);
    }

    (Some(alias), end)
}

fn skip_sql_rows_from_source(sql: &str, start: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, start);
    if !starts_with_keyword_at(sql, cursor, "ROWS") {
        return None;
    }
    cursor = skip_sql_ws(bytes, cursor + "ROWS".len());
    if !starts_with_keyword_at(sql, cursor, "FROM") {
        return None;
    }
    cursor = skip_sql_ws(bytes, cursor + "FROM".len());
    if bytes.get(cursor).copied() != Some(b'(') {
        return None;
    }

    let (_, rows_end) = balanced_paren_segment(sql, cursor)?;
    let (_, alias_end) = parse_sql_optional_table_alias(sql, rows_end);
    Some(alias_end.max(rows_end))
}

fn skip_sql_table_inheritance_star(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let cursor = skip_sql_ws(bytes, start);
    if bytes.get(cursor).copied() == Some(b'*') {
        skip_sql_ws(bytes, cursor + 1)
    } else {
        start
    }
}

fn skip_sql_tablesample_clause(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, start);
    if !starts_with_keyword_at(sql, cursor, "TABLESAMPLE") {
        return start;
    }

    cursor = skip_sql_ws(bytes, cursor + "TABLESAMPLE".len());
    let Some((_, method_end, _)) = parse_sql_identifier_path(sql, cursor) else {
        return cursor;
    };

    cursor = skip_sql_ws(bytes, method_end);
    if bytes.get(cursor).copied() == Some(b'(') {
        cursor = balanced_paren_segment(sql, cursor)
            .map(|(_, end)| end)
            .unwrap_or(cursor);
    }

    cursor = skip_sql_ws(bytes, cursor);
    if starts_with_keyword_at(sql, cursor, "REPEATABLE") {
        cursor = skip_sql_ws(bytes, cursor + "REPEATABLE".len());
        if bytes.get(cursor).copied() == Some(b'(') {
            cursor = balanced_paren_segment(sql, cursor)
                .map(|(_, end)| end)
                .unwrap_or(cursor);
        }
    }

    cursor
}

fn next_sql_table_source_start(sql: &str, start: usize, end: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut i = start;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;

    while i < end {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if let Some(next) = advance_sql_quoted_index(bytes, i, q) {
                i = next;
                continue;
            }
            in_quote = None;
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => return Some(i + 1),
            _ => {
                if depth == 0 && starts_with_keyword_at(sql, i, "JOIN") {
                    return Some(i + "JOIN".len());
                }
            }
        }
        i += 1;
    }

    None
}

fn is_sql_cte_alias(
    table: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> bool {
    inherited_cte_aliases
        .iter()
        .chain(local_cte_aliases.iter())
        .any(|alias| sql_ident_eq(alias, table))
}

fn is_sql_table_source_boundary(ident: &str) -> bool {
    matches!(
        ident.to_ascii_uppercase().as_str(),
        "CROSS"
            | "DEFAULT"
            | "FULL"
            | "GROUP"
            | "HAVING"
            | "INNER"
            | "INTERSECT"
            | "JOIN"
            | "LEFT"
            | "LIMIT"
            | "NATURAL"
            | "OFFSET"
            | "ON"
            | "ORDER"
            | "ORDINALITY"
            | "OUTER"
            | "OVERRIDING"
            | "RIGHT"
            | "RETURNING"
            | "SET"
            | "TABLESAMPLE"
            | "UNION"
            | "USING"
            | "VALUES"
            | "WHERE"
            | "WITH"
    )
}

#[derive(Debug, Default)]
struct SqlCteParts {
    aliases: Vec<String>,
    references: Vec<(SqlStmtKind, String, Vec<String>)>,
}

fn parse_sql_cte_parts(sql: &str, inherited_cte_aliases: &[String]) -> Option<SqlCteParts> {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, 0);
    if !starts_with_keyword_at(sql, cursor, "WITH") {
        return None;
    }
    cursor += "WITH".len();
    cursor = skip_sql_ws(bytes, cursor);
    if starts_with_keyword_at(sql, cursor, "RECURSIVE") {
        cursor += "RECURSIVE".len();
    }

    let mut parts = SqlCteParts::default();
    let mut known_aliases = inherited_cte_aliases.to_vec();

    loop {
        cursor = skip_sql_ws(bytes, cursor);
        let (alias, alias_end) = parse_sql_identifier_segment(sql, cursor)?;
        parts.aliases.push(alias.clone());
        known_aliases.push(alias);
        cursor = skip_sql_ws(bytes, alias_end);

        if bytes.get(cursor).copied() == Some(b'(') {
            let (_, end) = balanced_paren_segment(sql, cursor)?;
            cursor = skip_sql_ws(bytes, end);
        }

        if !starts_with_keyword_at(sql, cursor, "AS") {
            return None;
        }
        cursor += "AS".len();
        cursor = skip_sql_ws(bytes, cursor);
        cursor = skip_sql_cte_materialization_modifier(sql, cursor);

        if bytes.get(cursor).copied() != Some(b'(') {
            return None;
        }
        let (body, end) = balanced_paren_segment(sql, cursor)?;
        parts
            .references
            .extend(parse_sql_references_with_cte_aliases(body, &known_aliases));
        cursor = skip_sql_ws(bytes, end);

        if bytes.get(cursor).copied() == Some(b',') {
            cursor += 1;
            continue;
        }
        break;
    }

    if parts.aliases.is_empty() {
        None
    } else {
        Some(parts)
    }
}

fn skip_sql_cte_materialization_modifier(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let cursor = skip_sql_ws(bytes, start);
    if starts_with_keyword_at(sql, cursor, "MATERIALIZED") {
        return skip_sql_ws(bytes, cursor + "MATERIALIZED".len());
    }
    if starts_with_keyword_at(sql, cursor, "NOT") {
        let after_not = skip_sql_ws(bytes, cursor + "NOT".len());
        if starts_with_keyword_at(sql, after_not, "MATERIALIZED") {
            return skip_sql_ws(bytes, after_not + "MATERIALIZED".len());
        }
    }
    cursor
}

fn sql_ident_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn parse_sql_auxiliary_write_references(
    sql: &str,
    kind: SqlStmtKind,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    match kind {
        SqlStmtKind::Update => {
            parse_sql_update_from_references(sql, inherited_cte_aliases, local_cte_aliases)
        }
        SqlStmtKind::Delete => {
            parse_sql_delete_using_references(sql, inherited_cte_aliases, local_cte_aliases)
        }
        SqlStmtKind::Merge => {
            parse_sql_merge_source_references(sql, inherited_cte_aliases, local_cte_aliases)
        }
        SqlStmtKind::Truncate => parse_sql_truncate_references(sql),
        SqlStmtKind::Lock => parse_sql_lock_references(sql),
        SqlStmtKind::Select
        | SqlStmtKind::Insert
        | SqlStmtKind::Copy
        | SqlStmtKind::Create
        | SqlStmtKind::Alter
        | SqlStmtKind::Comment
        | SqlStmtKind::Grant
        | SqlStmtKind::Revoke
        | SqlStmtKind::Analyze
        | SqlStmtKind::Vacuum
        | SqlStmtKind::Reindex
        | SqlStmtKind::Cluster
        | SqlStmtKind::Refresh
        | SqlStmtKind::Drop => Vec::new(),
    }
}

fn parse_sql_truncate_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(truncate_idx) = find_keyword_top_level_from(sql, "TRUNCATE", 0) else {
        return Vec::new();
    };
    let list_start = skip_optional_sql_keyword(sql, truncate_idx + "TRUNCATE".len(), "TABLE");
    let list_end = top_level_sql_clause_start(
        sql,
        list_start,
        &[
            "RESTART IDENTITY",
            "CONTINUE IDENTITY",
            "CASCADE",
            "RESTRICT",
        ],
    )
    .unwrap_or(sql.len());
    let Some(list) = sql.get(list_start..list_end) else {
        return Vec::new();
    };

    split_sql_top_level(list, ',')
        .into_iter()
        .filter_map(|item| parse_sql_write_object_name_with_end(item, 0).map(|(table, _)| table))
        .map(|table| (SqlStmtKind::Truncate, table, Vec::new()))
        .collect()
}

fn parse_sql_lock_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(lock_idx) = find_keyword_top_level_from(sql, "LOCK", 0) else {
        return Vec::new();
    };
    let Some(table_idx) = find_keyword_top_level_from(sql, "TABLE", lock_idx + "LOCK".len()) else {
        return Vec::new();
    };
    let list_start = table_idx + "TABLE".len();
    let list_end =
        top_level_sql_clause_start(sql, list_start, &["IN", "NOWAIT"]).unwrap_or(sql.len());
    parse_sql_table_list(sql, list_start, list_end)
        .into_iter()
        .map(|table| (SqlStmtKind::Lock, table, Vec::new()))
        .collect()
}

fn parse_sql_table_list(sql: &str, start: usize, end: usize) -> Vec<String> {
    let Some(list) = sql.get(start..end) else {
        return Vec::new();
    };
    split_sql_top_level(list, ',')
        .into_iter()
        .filter_map(|item| parse_sql_write_object_name_with_end(item, 0).map(|(table, _)| table))
        .collect()
}

fn parse_sql_utility_references(
    sql: &str,
    kind: SqlStmtKind,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    match kind {
        SqlStmtKind::Create => {
            parse_sql_create_references(sql, inherited_cte_aliases, local_cte_aliases)
        }
        SqlStmtKind::Alter => parse_sql_alter_references(sql),
        SqlStmtKind::Comment => parse_sql_comment_references(sql),
        SqlStmtKind::Grant => parse_sql_privilege_references(sql, SqlStmtKind::Grant),
        SqlStmtKind::Revoke => parse_sql_privilege_references(sql, SqlStmtKind::Revoke),
        SqlStmtKind::Analyze => parse_sql_table_maintenance_reference(
            sql,
            SqlStmtKind::Analyze,
            "ANALYZE",
            &["VERBOSE"],
        ),
        SqlStmtKind::Vacuum => parse_sql_table_maintenance_reference(
            sql,
            SqlStmtKind::Vacuum,
            "VACUUM",
            &[
                "FULL",
                "FREEZE",
                "VERBOSE",
                "ANALYZE",
                "DISABLE_PAGE_SKIPPING",
                "SKIP_LOCKED",
                "PROCESS_TOAST",
                "INDEX_CLEANUP",
                "TRUNCATE",
                "PARALLEL",
                "BUFFER_USAGE_LIMIT",
            ],
        ),
        SqlStmtKind::Reindex => parse_sql_reindex_references(sql),
        SqlStmtKind::Cluster => parse_sql_table_maintenance_reference(
            sql,
            SqlStmtKind::Cluster,
            "CLUSTER",
            &["VERBOSE"],
        ),
        SqlStmtKind::Refresh => parse_sql_refresh_references(sql),
        SqlStmtKind::Drop => parse_sql_drop_references(sql),
        SqlStmtKind::Select
        | SqlStmtKind::Insert
        | SqlStmtKind::Update
        | SqlStmtKind::Delete
        | SqlStmtKind::Merge
        | SqlStmtKind::Truncate
        | SqlStmtKind::Copy
        | SqlStmtKind::Lock => Vec::new(),
    }
}

fn parse_sql_create_references(
    sql: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    if find_keyword_top_level_from(sql, "TRIGGER", 0).is_some() {
        return parse_sql_create_trigger_references(sql);
    }
    if find_keyword_top_level_from(sql, "RULE", 0).is_some() {
        return parse_sql_create_rule_references(sql, inherited_cte_aliases, local_cte_aliases);
    }
    if find_keyword_top_level_from(sql, "PUBLICATION", 0).is_some() {
        return parse_sql_create_publication_references(sql);
    }
    if find_keyword_top_level_from(sql, "STATISTICS", 0).is_some() {
        return parse_sql_create_statistics_references(sql);
    }
    if find_keyword_top_level_from(sql, "POLICY", 0).is_some() {
        return parse_sql_create_policy_references(sql);
    }
    if find_keyword_top_level_from(sql, "INDEX", 0).is_some() {
        return parse_sql_create_index_references(sql);
    }

    let view_idx = find_keyword_top_level_from(sql, "VIEW", 0);
    let table_idx = find_keyword_top_level_from(sql, "TABLE", 0);
    let Some(object_idx) = (match (view_idx, table_idx) {
        (Some(view), Some(table)) => Some(view.min(table)),
        (Some(view), None) => Some(view),
        (None, Some(table)) => Some(table),
        (None, None) => None,
    }) else {
        return Vec::new();
    };

    if Some(object_idx) == table_idx {
        let refs = parse_sql_create_table_references(sql, object_idx);
        if !refs.is_empty() {
            return refs;
        }
    }

    let Some((_, object_end)) = parse_sql_object_name_with_end(sql, object_idx) else {
        return Vec::new();
    };
    let Some(as_idx) = find_keyword_top_level_from(sql, "AS", object_end) else {
        return Vec::new();
    };
    let Some(query) = sql.get(as_idx + "AS".len()..).map(str::trim) else {
        return Vec::new();
    };
    if classify_sql_kind(query) != Some(SqlStmtKind::Select) {
        return Vec::new();
    }

    let mut aliases = inherited_cte_aliases.to_vec();
    aliases.extend(local_cte_aliases.iter().cloned());
    parse_sql_references_with_cte_aliases(query, &aliases)
}

fn parse_sql_create_trigger_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(trigger_idx) = find_keyword_top_level_from(sql, "TRIGGER", 0) else {
        return Vec::new();
    };
    let Some(on_idx) = find_keyword_top_level_from(sql, "ON", trigger_idx + "TRIGGER".len()) else {
        return Vec::new();
    };
    let Some((table, table_end)) = parse_sql_write_object_name_with_end(sql, on_idx + "ON".len())
    else {
        return Vec::new();
    };

    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    if let Some(update_of_idx) = find_keyword_top_level_from(sql, "UPDATE OF", trigger_idx)
        && update_of_idx < on_idx
        && let Some(segment) = sql.get(update_of_idx + "UPDATE OF".len()..on_idx)
    {
        collect_sql_column_list(segment, &mut cols, &mut seen);
    }
    collect_sql_parenthesized_clause_columns(sql, "WHEN", table_end, &mut cols, &mut seen);

    vec![(SqlStmtKind::Create, table, cols)]
}

fn parse_sql_create_rule_references(
    sql: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(rule_idx) = find_keyword_top_level_from(sql, "RULE", 0) else {
        return Vec::new();
    };
    let Some(on_idx) = find_keyword_top_level_from(sql, "ON", rule_idx + "RULE".len()) else {
        return Vec::new();
    };
    let Some(to_idx) = find_keyword_top_level_from(sql, "TO", on_idx + "ON".len()) else {
        return Vec::new();
    };
    let Some((table, table_end)) = parse_sql_write_object_name_with_end(sql, to_idx + "TO".len())
    else {
        return Vec::new();
    };

    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    if let Some(where_idx) = find_keyword_top_level_from(sql, "WHERE", table_end) {
        let do_idx = find_keyword_top_level_from(sql, "DO", table_end).unwrap_or(sql.len());
        if where_idx < do_idx
            && let Some(segment) = sql.get(where_idx + "WHERE".len()..do_idx)
        {
            collect_sql_identifier_columns(segment, &mut cols, &mut seen);
        }
    }

    let mut refs = vec![(SqlStmtKind::Create, table, cols)];
    if let Some(do_idx) = find_keyword_top_level_from(sql, "DO", table_end)
        && let Some(action) = sql.get(do_idx + "DO".len()..).map(str::trim)
    {
        refs.extend(parse_sql_rule_action_references(
            action,
            inherited_cte_aliases,
            local_cte_aliases,
        ));
    }
    refs
}

fn parse_sql_rule_action_references(
    action: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let mut action = action.trim();
    for keyword in ["ALSO", "INSTEAD"] {
        if starts_with_keyword_at(action, 0, keyword) {
            action = action.get(keyword.len()..).unwrap_or_default().trim_start();
            break;
        }
    }
    if starts_with_keyword_at(action, 0, "NOTHING") {
        return Vec::new();
    }

    let mut aliases = inherited_cte_aliases.to_vec();
    aliases.extend(local_cte_aliases.iter().cloned());
    if action.starts_with('(')
        && let Some((segment, _)) = balanced_paren_segment(action, 0)
    {
        let mut refs = Vec::new();
        for stmt in split_sql_top_level(segment, ';') {
            let stmt = stmt.trim();
            if classify_sql_kind(stmt).is_some() {
                refs.extend(parse_sql_references_with_cte_aliases(stmt, &aliases));
            }
        }
        return refs;
    }
    if classify_sql_kind(action).is_some() {
        return parse_sql_references_with_cte_aliases(action, &aliases);
    }
    Vec::new()
}

fn parse_sql_create_publication_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(for_idx) = find_keyword_top_level_from(sql, "FOR", 0) else {
        return Vec::new();
    };
    let Some(table_idx) = find_keyword_top_level_from(sql, "TABLE", for_idx + "FOR".len()) else {
        return Vec::new();
    };
    parse_sql_publication_table_references(sql, table_idx, SqlStmtKind::Create)
}

fn parse_sql_create_statistics_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(on_idx) = find_keyword_top_level_from(sql, "ON", 0) else {
        return Vec::new();
    };
    let Some(from_idx) = find_keyword_top_level_from(sql, "FROM", on_idx + "ON".len()) else {
        return Vec::new();
    };
    let Some((table, _)) = parse_sql_write_object_name_with_end(sql, from_idx + "FROM".len())
    else {
        return Vec::new();
    };

    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    if let Some(segment) = sql.get(on_idx + "ON".len()..from_idx) {
        collect_sql_identifier_columns(segment, &mut cols, &mut seen);
    }
    vec![(SqlStmtKind::Create, table, cols)]
}

fn parse_sql_create_table_references(
    sql: &str,
    table_idx: usize,
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let cursor = skip_sql_optional_if_not_exists(sql, table_idx + "TABLE".len());
    let Some((_, table_end)) = parse_sql_write_object_name_with_end(sql, cursor) else {
        return Vec::new();
    };

    let mut refs = Vec::new();
    let mut scan = table_end;
    while let Some(like_idx) = find_keyword_at_depth_from(sql, "LIKE", scan, 1) {
        let Some((table, table_end)) =
            parse_sql_write_object_name_with_end(sql, like_idx + "LIKE".len())
        else {
            scan = like_idx + "LIKE".len();
            continue;
        };
        refs.push((SqlStmtKind::Create, table, Vec::new()));
        scan = table_end;
    }

    scan = table_end;
    while let Some(references_idx) = find_keyword_at_depth_from(sql, "REFERENCES", scan, 1) {
        let Some((table, ref_table_end)) =
            parse_sql_object_name_with_end(sql, references_idx + "REFERENCES".len())
        else {
            scan = references_idx + "REFERENCES".len();
            continue;
        };
        let mut cols = Vec::new();
        let mut seen = HashSet::new();
        let after = skip_sql_ws(sql.as_bytes(), ref_table_end);
        if sql.as_bytes().get(after).copied() == Some(b'(')
            && let Some((segment, end)) = balanced_paren_segment(sql, after)
        {
            collect_sql_column_list(segment, &mut cols, &mut seen);
            refs.push((SqlStmtKind::Create, table, cols));
            scan = end;
            continue;
        }
        refs.push((SqlStmtKind::Create, table, cols));
        scan = ref_table_end;
    }

    refs
}

fn parse_sql_create_index_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(index_idx) = find_keyword_top_level_from(sql, "INDEX", 0) else {
        return Vec::new();
    };
    let Some(on_idx) = find_keyword_top_level_from(sql, "ON", index_idx + "INDEX".len()) else {
        return Vec::new();
    };
    let Some((table, table_end)) = parse_sql_write_object_name_with_end(sql, on_idx + "ON".len())
    else {
        return Vec::new();
    };

    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    if let Some(open_idx) = find_sql_top_level_byte(sql, table_end, b'(')
        && let Some((segment, _)) = balanced_paren_segment(sql, open_idx)
    {
        collect_sql_index_key_columns(segment, &mut cols, &mut seen);
    }
    if let Some(include_idx) = find_keyword_top_level_from(sql, "INCLUDE", table_end) {
        let after = skip_sql_ws(sql.as_bytes(), include_idx + "INCLUDE".len());
        if sql.as_bytes().get(after).copied() == Some(b'(')
            && let Some((segment, _)) = balanced_paren_segment(sql, after)
        {
            collect_sql_column_list(segment, &mut cols, &mut seen);
        }
    }
    if let Some(where_segment) = top_level_sql_clause_segment(sql, "WHERE", table_end) {
        collect_sql_identifier_columns(where_segment, &mut cols, &mut seen);
    }

    vec![(SqlStmtKind::Create, table, cols)]
}

fn collect_sql_index_key_columns(
    segment: &str,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    for item in split_sql_top_level(segment, ',') {
        let expression = sql_index_key_expression(item);
        collect_sql_identifier_columns(expression, cols, seen);
    }
}

fn sql_index_key_expression(item: &str) -> &str {
    let item = item.trim();
    if item.is_empty() {
        return item;
    }

    let bytes = item.as_bytes();
    let start = skip_sql_ws(bytes, 0);
    if bytes.get(start).copied() == Some(b'(')
        && let Some((segment, _)) = balanced_paren_segment(item, start)
    {
        return segment.trim();
    }

    let mut i = start;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;
    let mut saw_expression = false;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if let Some(next) = advance_sql_quoted_index(bytes, i, q) {
                i = next;
                continue;
            }
            in_quote = None;
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => {
                in_quote = Some(b);
                saw_expression = true;
            }
            b'(' => {
                depth += 1;
                saw_expression = true;
            }
            b')' => {
                depth -= 1;
                saw_expression = true;
            }
            _ if b.is_ascii_whitespace() && depth == 0 && saw_expression => {
                return item.get(start..i).unwrap_or(item).trim();
            }
            _ if !b.is_ascii_whitespace() => saw_expression = true,
            _ => {}
        }

        i += 1;
    }

    item.get(start..).unwrap_or(item).trim()
}

fn parse_sql_create_policy_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(policy_idx) = find_keyword_top_level_from(sql, "POLICY", 0) else {
        return Vec::new();
    };
    let Some(on_idx) = find_keyword_top_level_from(sql, "ON", policy_idx + "POLICY".len()) else {
        return Vec::new();
    };
    let Some((table, table_end)) = parse_sql_write_object_name_with_end(sql, on_idx + "ON".len())
    else {
        return Vec::new();
    };

    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    collect_sql_parenthesized_clause_columns(sql, "USING", table_end, &mut cols, &mut seen);
    collect_sql_parenthesized_clause_columns(sql, "WITH CHECK", table_end, &mut cols, &mut seen);
    vec![(SqlStmtKind::Create, table, cols)]
}

fn parse_sql_alter_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(alter_idx) = find_keyword_top_level_from(sql, "ALTER", 0) else {
        return Vec::new();
    };
    if find_keyword_top_level_from(sql, "POLICY", alter_idx + "ALTER".len()).is_some() {
        return parse_sql_alter_policy_references(sql);
    }
    if find_keyword_top_level_from(sql, "PUBLICATION", alter_idx + "ALTER".len()).is_some() {
        return parse_sql_alter_publication_references(sql);
    }
    if find_keyword_top_level_from(sql, "TRIGGER", alter_idx + "ALTER".len()).is_some() {
        return parse_sql_alter_trigger_references(sql);
    }
    if let Some(view_idx) =
        find_keyword_top_level_from(sql, "MATERIALIZED VIEW", alter_idx + "ALTER".len())
    {
        return parse_sql_alter_view_references(sql, view_idx + "MATERIALIZED VIEW".len());
    }
    if let Some(view_idx) = find_keyword_top_level_from(sql, "VIEW", alter_idx + "ALTER".len()) {
        return parse_sql_alter_view_references(sql, view_idx + "VIEW".len());
    }

    let Some(table_idx) = find_keyword_top_level_from(sql, "TABLE", alter_idx + "ALTER".len())
    else {
        return Vec::new();
    };
    let cursor = skip_sql_optional_if_exists(sql, table_idx + "TABLE".len());
    let Some((table, table_end)) = parse_sql_write_object_name_with_end(sql, cursor) else {
        return Vec::new();
    };

    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    for keyword in ["DROP COLUMN", "RENAME COLUMN", "ALTER COLUMN", "ADD COLUMN"] {
        collect_sql_columns_after_keyword(sql, keyword, table_end, &mut cols, &mut seen);
    }
    for keyword in ["FOREIGN KEY", "UNIQUE", "PRIMARY KEY"] {
        collect_sql_parenthesized_clause_columns(sql, keyword, table_end, &mut cols, &mut seen);
    }
    collect_sql_parenthesized_clause_columns(sql, "CHECK", table_end, &mut cols, &mut seen);

    let mut refs = vec![(SqlStmtKind::Alter, table, cols)];
    refs.extend(parse_sql_alter_referenced_table_references(sql, table_end));
    refs
}

fn parse_sql_alter_policy_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(policy_idx) = find_keyword_top_level_from(sql, "POLICY", 0) else {
        return Vec::new();
    };
    let Some(on_idx) = find_keyword_top_level_from(sql, "ON", policy_idx + "POLICY".len()) else {
        return Vec::new();
    };
    let Some((table, table_end)) = parse_sql_write_object_name_with_end(sql, on_idx + "ON".len())
    else {
        return Vec::new();
    };

    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    collect_sql_parenthesized_clause_columns(sql, "USING", table_end, &mut cols, &mut seen);
    collect_sql_parenthesized_clause_columns(sql, "WITH CHECK", table_end, &mut cols, &mut seen);
    vec![(SqlStmtKind::Alter, table, cols)]
}

fn parse_sql_alter_publication_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(publication_idx) = find_keyword_top_level_from(sql, "PUBLICATION", 0) else {
        return Vec::new();
    };
    let Some(table_idx) =
        find_keyword_top_level_from(sql, "TABLE", publication_idx + "PUBLICATION".len())
    else {
        return Vec::new();
    };
    parse_sql_publication_table_references(sql, table_idx, SqlStmtKind::Alter)
}

fn parse_sql_alter_trigger_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(trigger_idx) = find_keyword_top_level_from(sql, "TRIGGER", 0) else {
        return Vec::new();
    };
    let Some(on_idx) = find_keyword_top_level_from(sql, "ON", trigger_idx + "TRIGGER".len()) else {
        return Vec::new();
    };
    let Some((table, _)) = parse_sql_write_object_name_with_end(sql, on_idx + "ON".len()) else {
        return Vec::new();
    };
    vec![(SqlStmtKind::Alter, table, Vec::new())]
}

fn parse_sql_alter_view_references(
    sql: &str,
    start: usize,
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let cursor = skip_sql_optional_if_exists(sql, start);
    let Some((table, _)) = parse_sql_write_object_name_with_end(sql, cursor) else {
        return Vec::new();
    };
    vec![(SqlStmtKind::Alter, table, Vec::new())]
}

fn parse_sql_alter_referenced_table_references(
    sql: &str,
    min_idx: usize,
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let mut refs = Vec::new();
    let mut cursor = min_idx;
    while let Some(references_idx) = find_keyword_top_level_from(sql, "REFERENCES", cursor) {
        let Some((table, table_end)) =
            parse_sql_object_name_with_end(sql, references_idx + "REFERENCES".len())
        else {
            cursor = references_idx + "REFERENCES".len();
            continue;
        };
        let mut cols = Vec::new();
        let mut seen = HashSet::new();
        let after = skip_sql_ws(sql.as_bytes(), table_end);
        if sql.as_bytes().get(after).copied() == Some(b'(')
            && let Some((segment, end)) = balanced_paren_segment(sql, after)
        {
            collect_sql_column_list(segment, &mut cols, &mut seen);
            refs.push((SqlStmtKind::Alter, table, cols));
            cursor = end;
            continue;
        }
        refs.push((SqlStmtKind::Alter, table, cols));
        cursor = table_end;
    }
    refs
}

fn parse_sql_comment_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(on_idx) = find_keyword_top_level_from(sql, "ON", 0) else {
        return Vec::new();
    };
    let after_on = skip_sql_ws(sql.as_bytes(), on_idx + "ON".len());
    if starts_with_keyword_at(sql, after_on, "COLUMN") {
        let Some((parts, _)) = parse_sql_identifier_path_parts(sql, after_on + "COLUMN".len())
        else {
            return Vec::new();
        };
        if parts.len() < 2 {
            return Vec::new();
        }
        let table = parts[parts.len() - 2].clone();
        let column = parts[parts.len() - 1].clone();
        return vec![(SqlStmtKind::Comment, table, vec![column])];
    }
    if starts_with_keyword_at(sql, after_on, "TABLE")
        && let Some((table, _)) =
            parse_sql_write_object_name_with_end(sql, after_on + "TABLE".len())
    {
        return vec![(SqlStmtKind::Comment, table, Vec::new())];
    }
    Vec::new()
}

fn parse_sql_privilege_references(
    sql: &str,
    kind: SqlStmtKind,
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(on_idx) = find_keyword_top_level_from(sql, "ON", 0) else {
        return Vec::new();
    };
    let Some(cursor) = sql_privilege_table_target_start(sql, on_idx + "ON".len()) else {
        return Vec::new();
    };
    let Some((table, _)) = parse_sql_write_object_name_with_end(sql, cursor) else {
        return Vec::new();
    };

    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    if let Some(privileges) = sql.get(..on_idx) {
        let mut cursor = 0usize;
        while let Some(open_idx) = find_sql_top_level_byte(privileges, cursor, b'(') {
            if let Some((segment, end)) = balanced_paren_segment(privileges, open_idx) {
                collect_sql_column_list(segment, &mut cols, &mut seen);
                cursor = end;
            } else {
                break;
            }
        }
    }

    vec![(kind, table, cols)]
}

fn sql_privilege_table_target_start(sql: &str, start: usize) -> Option<usize> {
    let cursor = skip_sql_ws(sql.as_bytes(), start);
    if starts_with_keyword_at(sql, cursor, "ALL") {
        return None;
    }
    if starts_with_keyword_at(sql, cursor, "FOREIGN TABLE") {
        return Some(skip_sql_ws(sql.as_bytes(), cursor + "FOREIGN TABLE".len()));
    }
    if starts_with_keyword_at(sql, cursor, "TABLE") {
        return Some(skip_sql_ws(sql.as_bytes(), cursor + "TABLE".len()));
    }
    for keyword in [
        "SCHEMA",
        "SEQUENCE",
        "DATABASE",
        "FUNCTION",
        "PROCEDURE",
        "ROUTINE",
        "TYPE",
        "LANGUAGE",
        "TABLESPACE",
        "FOREIGN DATA WRAPPER",
        "SERVER",
        "LARGE OBJECT",
        "PARAMETER",
    ] {
        if starts_with_keyword_at(sql, cursor, keyword) {
            return None;
        }
    }
    Some(cursor)
}

fn parse_sql_table_maintenance_reference(
    sql: &str,
    kind: SqlStmtKind,
    keyword: &str,
    option_keywords: &[&str],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(keyword_idx) = find_keyword_top_level_from(sql, keyword, 0) else {
        return Vec::new();
    };
    let cursor = skip_sql_utility_options(sql, keyword_idx + keyword.len(), option_keywords);
    let Some((table, table_end)) = parse_sql_write_object_name_with_end(sql, cursor) else {
        return Vec::new();
    };
    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    let after = skip_sql_ws(sql.as_bytes(), table_end);
    if sql.as_bytes().get(after).copied() == Some(b'(')
        && let Some((segment, _)) = balanced_paren_segment(sql, after)
    {
        collect_sql_column_list(segment, &mut cols, &mut seen);
    }
    vec![(kind, table, cols)]
}

fn parse_sql_reindex_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(table_idx) = find_keyword_top_level_from(sql, "TABLE", 0) else {
        return Vec::new();
    };
    let cursor =
        skip_sql_utility_options(sql, table_idx + "TABLE".len(), &["CONCURRENTLY", "VERBOSE"]);
    let Some((table, _)) = parse_sql_write_object_name_with_end(sql, cursor) else {
        return Vec::new();
    };
    vec![(SqlStmtKind::Reindex, table, Vec::new())]
}

fn parse_sql_refresh_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(view_idx) = find_keyword_top_level_from(sql, "VIEW", 0) else {
        return Vec::new();
    };
    let cursor = skip_sql_utility_options(sql, view_idx + "VIEW".len(), &["CONCURRENTLY"]);
    let Some((table, _)) = parse_sql_write_object_name_with_end(sql, cursor) else {
        return Vec::new();
    };
    vec![(SqlStmtKind::Refresh, table, Vec::new())]
}

fn parse_sql_drop_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(drop_idx) = find_keyword_top_level_from(sql, "DROP", 0) else {
        return Vec::new();
    };

    for (keyword, after_len) in [
        ("MATERIALIZED VIEW", "MATERIALIZED VIEW".len()),
        ("FOREIGN TABLE", "FOREIGN TABLE".len()),
        ("TABLE", "TABLE".len()),
        ("VIEW", "VIEW".len()),
    ] {
        if let Some(keyword_idx) =
            find_keyword_top_level_from(sql, keyword, drop_idx + "DROP".len())
        {
            return parse_sql_drop_object_list(sql, keyword_idx + after_len);
        }
    }

    for keyword in ["POLICY", "TRIGGER", "RULE"] {
        if let Some(keyword_idx) =
            find_keyword_top_level_from(sql, keyword, drop_idx + "DROP".len())
        {
            return parse_sql_drop_on_table_reference(sql, keyword_idx + keyword.len());
        }
    }

    Vec::new()
}

fn parse_sql_drop_object_list(sql: &str, start: usize) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let list_start = skip_sql_optional_if_exists(sql, start);
    let list_end =
        top_level_sql_clause_start(sql, list_start, &["CASCADE", "RESTRICT"]).unwrap_or(sql.len());
    parse_sql_table_list(sql, list_start, list_end)
        .into_iter()
        .map(|table| (SqlStmtKind::Drop, table, Vec::new()))
        .collect()
}

fn parse_sql_drop_on_table_reference(
    sql: &str,
    start: usize,
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(on_idx) = find_keyword_top_level_from(sql, "ON", start) else {
        return Vec::new();
    };
    let Some((table, _)) = parse_sql_write_object_name_with_end(sql, on_idx + "ON".len()) else {
        return Vec::new();
    };
    vec![(SqlStmtKind::Drop, table, Vec::new())]
}

fn parse_sql_publication_table_references(
    sql: &str,
    table_idx: usize,
    kind: SqlStmtKind,
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let list_start = table_idx + "TABLE".len();
    let list_end = top_level_sql_clause_start(sql, list_start, &["WITH"]).unwrap_or(sql.len());
    let Some(list) = sql.get(list_start..list_end) else {
        return Vec::new();
    };

    split_sql_top_level(list, ',')
        .into_iter()
        .filter_map(|item| parse_sql_publication_table_item(item, kind))
        .collect()
}

fn parse_sql_publication_table_item(
    item: &str,
    kind: SqlStmtKind,
) -> Option<(SqlStmtKind, String, Vec<String>)> {
    let (table, table_end) = parse_sql_write_object_name_with_end(item, 0)?;
    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    let after_table = skip_sql_ws(item.as_bytes(), table_end);
    let mut cursor = table_end;
    if item.as_bytes().get(after_table).copied() == Some(b'(')
        && let Some((segment, end)) = balanced_paren_segment(item, after_table)
    {
        collect_sql_column_list(segment, &mut cols, &mut seen);
        cursor = end;
    }
    collect_sql_clause_columns_until(item, "WHERE", cursor, &[], &mut cols, &mut seen);
    Some((kind, table, cols))
}

fn collect_sql_columns_after_keyword(
    sql: &str,
    keyword: &str,
    min_idx: usize,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let mut cursor = min_idx;
    while let Some(keyword_idx) = find_keyword_top_level_from(sql, keyword, cursor) {
        let mut column_start = keyword_idx + keyword.len();
        column_start = skip_sql_optional_if_exists(sql, column_start);
        column_start = skip_sql_optional_if_not_exists(sql, column_start);
        if let Some((column, next, _)) = parse_sql_identifier_path(sql, column_start) {
            push_column_ref(&column, cols, seen);
            cursor = next;
        } else {
            cursor = keyword_idx + keyword.len();
        }
    }
}

fn collect_sql_parenthesized_clause_columns(
    sql: &str,
    keyword: &str,
    min_idx: usize,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let mut cursor = min_idx;
    while let Some(keyword_idx) = find_keyword_top_level_from(sql, keyword, cursor) {
        let after_keyword = skip_sql_ws(sql.as_bytes(), keyword_idx + keyword.len());
        if sql.as_bytes().get(after_keyword).copied() == Some(b'(')
            && let Some((segment, end)) = balanced_paren_segment(sql, after_keyword)
        {
            collect_sql_identifier_columns(segment, cols, seen);
            cursor = end;
            continue;
        }
        cursor = keyword_idx + keyword.len();
    }
}

fn collect_sql_clause_columns_until(
    sql: &str,
    keyword: &str,
    min_idx: usize,
    end_clauses: &[&str],
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let mut cursor = min_idx;
    while let Some(keyword_idx) = find_keyword_top_level_from(sql, keyword, cursor) {
        let start = keyword_idx + keyword.len();
        let end = top_level_sql_clause_start(sql, start, end_clauses).unwrap_or(sql.len());
        if let Some(segment) = sql.get(start..end) {
            collect_sql_identifier_columns(segment, cols, seen);
        }
        cursor = end;
    }
}

fn skip_sql_utility_options(sql: &str, start: usize, option_keywords: &[&str]) -> usize {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, start);
    if bytes.get(cursor).copied() == Some(b'(')
        && let Some((_, end)) = balanced_paren_segment(sql, cursor)
    {
        cursor = skip_sql_ws(bytes, end);
    }

    loop {
        let before = cursor;
        for keyword in option_keywords {
            if starts_with_keyword_at(sql, cursor, keyword) {
                cursor = skip_sql_ws(bytes, cursor + keyword.len());
                break;
            }
        }
        if cursor == before {
            break;
        }
    }
    cursor
}

fn skip_sql_optional_if_exists(sql: &str, start: usize) -> usize {
    let cursor = skip_sql_ws(sql.as_bytes(), start);
    if starts_with_keyword_at(sql, cursor, "IF") {
        let after_if = skip_sql_ws(sql.as_bytes(), cursor + "IF".len());
        if starts_with_keyword_at(sql, after_if, "EXISTS") {
            return skip_sql_ws(sql.as_bytes(), after_if + "EXISTS".len());
        }
    }
    cursor
}

fn skip_sql_optional_if_not_exists(sql: &str, start: usize) -> usize {
    let cursor = skip_sql_ws(sql.as_bytes(), start);
    if starts_with_keyword_at(sql, cursor, "IF") {
        let after_if = skip_sql_ws(sql.as_bytes(), cursor + "IF".len());
        if starts_with_keyword_at(sql, after_if, "NOT") {
            let after_not = skip_sql_ws(sql.as_bytes(), after_if + "NOT".len());
            if starts_with_keyword_at(sql, after_not, "EXISTS") {
                return skip_sql_ws(sql.as_bytes(), after_not + "EXISTS".len());
            }
        }
    }
    cursor
}

fn parse_sql_update_from_references(
    sql: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(update_idx) = find_keyword_top_level_from(sql, "UPDATE", 0) else {
        return Vec::new();
    };
    let Some((_, table_end)) =
        parse_sql_write_object_name_with_end(sql, update_idx + "UPDATE".len())
    else {
        return Vec::new();
    };
    let Some(from_idx) = find_keyword_top_level_from(sql, "FROM", table_end) else {
        return Vec::new();
    };

    let source_start = from_idx + "FROM".len();
    let source_end = sql_table_source_clause_end(sql, source_start);
    let sources =
        parse_sql_select_table_sources(sql, source_start, inherited_cte_aliases, local_cte_aliases);
    let columns_by_source = collect_sql_update_from_columns_by_source(
        sql,
        &sources,
        source_start,
        source_end,
        table_end,
    );

    sources
        .into_iter()
        .zip(columns_by_source)
        .map(|(source, columns)| (SqlStmtKind::Update, source.table, columns))
        .collect()
}

fn parse_sql_delete_using_references(
    sql: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(delete_idx) = find_keyword_top_level_from(sql, "DELETE", 0) else {
        return Vec::new();
    };
    let Some(from_idx) = find_keyword_top_level_from(sql, "FROM", delete_idx + "DELETE".len())
    else {
        return Vec::new();
    };
    let Some((_, table_end)) = parse_sql_write_object_name_with_end(sql, from_idx + "FROM".len())
    else {
        return Vec::new();
    };
    let Some(using_idx) = find_keyword_top_level_from(sql, "USING", table_end) else {
        return Vec::new();
    };

    let source_start = using_idx + "USING".len();
    let source_end = sql_table_source_clause_end(sql, source_start);
    let sources =
        parse_sql_select_table_sources(sql, source_start, inherited_cte_aliases, local_cte_aliases);
    let columns_by_source = collect_sql_auxiliary_columns_by_source(
        sql,
        &sources,
        source_start,
        source_end,
        table_end,
        &["WHERE", "RETURNING"],
    );

    sources
        .into_iter()
        .zip(columns_by_source)
        .map(|(source, columns)| (SqlStmtKind::Delete, source.table, columns))
        .collect()
}

fn parse_sql_merge_source_references(
    sql: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(merge_idx) = find_keyword_top_level_from(sql, "MERGE", 0) else {
        return Vec::new();
    };
    let Some(into_idx) = find_keyword_top_level_from(sql, "INTO", merge_idx + "MERGE".len()) else {
        return Vec::new();
    };
    let Some((_, target_end)) = parse_sql_object_name_with_end(sql, into_idx + "INTO".len()) else {
        return Vec::new();
    };
    let (_, target_alias_end) = parse_sql_optional_table_alias(sql, target_end);
    let Some(using_idx) = find_keyword_top_level_from(sql, "USING", target_alias_end) else {
        return Vec::new();
    };
    let source_start = using_idx + "USING".len();
    let source_end = top_level_sql_clause_start(sql, source_start, &["ON"]).unwrap_or(sql.len());
    if sql
        .as_bytes()
        .get(skip_sql_ws(sql.as_bytes(), source_start))
        == Some(&b'(')
    {
        return Vec::new();
    }

    let sources =
        parse_sql_select_table_sources(sql, source_start, inherited_cte_aliases, local_cte_aliases);
    let columns_by_source = collect_sql_merge_columns_by_source(sql, &sources, source_end);

    sources
        .into_iter()
        .zip(columns_by_source)
        .map(|(source, columns)| (SqlStmtKind::Merge, source.table, columns))
        .collect()
}

fn parse_sql_insert_select_references(
    sql: &str,
    kind: SqlStmtKind,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    if kind != SqlStmtKind::Insert {
        return Vec::new();
    }

    let Some(insert_idx) = find_keyword_top_level_from(sql, "INSERT", 0) else {
        return Vec::new();
    };
    let Some(into_idx) = find_keyword_top_level_from(sql, "INTO", insert_idx + "INSERT".len())
    else {
        return Vec::new();
    };
    let Some((_, table_end)) = parse_sql_object_name_with_end(sql, into_idx + "INTO".len()) else {
        return Vec::new();
    };
    let Some(select_idx) = find_keyword_top_level_from(sql, "SELECT", table_end) else {
        return Vec::new();
    };
    let select_end = top_level_sql_clause_start(sql, select_idx + "SELECT".len(), &["RETURNING"])
        .unwrap_or(sql.len());
    let Some(select_sql) = sql.get(select_idx..select_end).map(str::trim) else {
        return Vec::new();
    };
    if classify_sql_kind(select_sql) != Some(SqlStmtKind::Select) {
        return Vec::new();
    }

    let mut aliases = inherited_cte_aliases.to_vec();
    aliases.extend(local_cte_aliases.iter().cloned());
    parse_sql_references_with_cte_aliases(select_sql, &aliases)
}

fn parse_sql_set_operation_references(
    sql: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let mut refs = Vec::new();
    let mut cursor = 0usize;
    let mut aliases = inherited_cte_aliases.to_vec();
    aliases.extend(local_cte_aliases.iter().cloned());

    while let Some((set_idx, keyword)) = next_sql_set_operator(sql, cursor) {
        let mut operand_start = skip_sql_ws(sql.as_bytes(), set_idx + keyword.len());
        for modifier in ["ALL", "DISTINCT"] {
            if starts_with_keyword_at(sql, operand_start, modifier) {
                operand_start = skip_sql_ws(sql.as_bytes(), operand_start + modifier.len());
                break;
            }
        }

        let operand_end = next_sql_set_operator(sql, operand_start)
            .map(|(idx, _)| idx)
            .unwrap_or(sql.len());
        if let Some(operand) = sql.get(operand_start..operand_end).map(str::trim)
            && classify_sql_kind(operand) == Some(SqlStmtKind::Select)
        {
            refs.extend(parse_sql_references_with_cte_aliases(operand, &aliases));
        }
        cursor = operand_end;
    }

    refs
}

fn next_sql_set_operator(sql: &str, min_idx: usize) -> Option<(usize, &'static str)> {
    ["UNION", "INTERSECT", "EXCEPT"]
        .into_iter()
        .filter_map(|keyword| {
            find_keyword_top_level_from(sql, keyword, min_idx).map(|idx| (idx, keyword))
        })
        .min_by_key(|(idx, _)| *idx)
}

fn parse_sql_reference(sql: &str) -> Option<(SqlStmtKind, String, Vec<String>)> {
    let normalized = normalize_whitespace(sql);
    let kind = classify_sql_kind(&normalized)?;

    match kind {
        SqlStmtKind::Select => {
            let select_idx = find_keyword_top_level_from(&normalized, "SELECT", 0)?;
            let from_idx =
                find_keyword_top_level_from(&normalized, "FROM", select_idx + "SELECT".len())?;

            let columns_raw = normalized
                .get(select_idx + "SELECT".len()..from_idx)?
                .trim();
            let table = parse_sql_object_name(&normalized, from_idx + "FROM".len())?;

            let columns = collect_sql_select_columns(&normalized, columns_raw, from_idx);

            Some((kind, table, columns))
        }
        SqlStmtKind::Insert => {
            let insert_idx = find_keyword_top_level_from(&normalized, "INSERT", 0)?;
            let into_idx =
                find_keyword_top_level_from(&normalized, "INTO", insert_idx + "INSERT".len())?;
            let (table, table_end) =
                parse_sql_object_name_with_end(&normalized, into_idx + "INTO".len())?;
            let (alias, cursor) = parse_sql_optional_insert_alias(&normalized, table_end);
            let columns = collect_sql_insert_columns(
                &normalized,
                &table,
                alias.as_deref().unwrap_or(&table),
                cursor,
            );
            Some((kind, table, columns))
        }
        SqlStmtKind::Update => {
            let update_idx = find_keyword_top_level_from(&normalized, "UPDATE", 0)?;
            let (table, table_end) =
                parse_sql_write_object_name_with_end(&normalized, update_idx + "UPDATE".len())?;
            let (alias, alias_end) = parse_sql_optional_table_alias(&normalized, table_end);
            let columns = collect_sql_update_columns(
                &normalized,
                &table,
                alias.as_deref().unwrap_or(&table),
                alias_end,
            );
            Some((kind, table, columns))
        }
        SqlStmtKind::Delete => {
            let delete_idx = find_keyword_top_level_from(&normalized, "DELETE", 0)?;
            let from_idx =
                find_keyword_top_level_from(&normalized, "FROM", delete_idx + "DELETE".len())?;
            let (table, table_end) =
                parse_sql_write_object_name_with_end(&normalized, from_idx + "FROM".len())?;
            let (alias, alias_end) = parse_sql_optional_table_alias(&normalized, table_end);
            let columns = collect_sql_delete_columns(
                &normalized,
                &table,
                alias.as_deref().unwrap_or(&table),
                alias_end,
            );
            Some((kind, table, columns))
        }
        SqlStmtKind::Merge => {
            let merge_idx = find_keyword_top_level_from(&normalized, "MERGE", 0)?;
            let into_idx =
                find_keyword_top_level_from(&normalized, "INTO", merge_idx + "MERGE".len())?;
            let (table, table_end) =
                parse_sql_object_name_with_end(&normalized, into_idx + "INTO".len())?;
            let (alias, alias_end) = parse_sql_optional_table_alias(&normalized, table_end);
            let columns = collect_sql_merge_target_columns(
                &normalized,
                &table,
                alias.as_deref().unwrap_or(&table),
                alias_end,
            );
            Some((kind, table, columns))
        }
        SqlStmtKind::Truncate => {
            let truncate_idx = find_keyword_top_level_from(&normalized, "TRUNCATE", 0)?;
            let cursor =
                skip_optional_sql_keyword(&normalized, truncate_idx + "TRUNCATE".len(), "TABLE");
            let (table, _) = parse_sql_write_object_name_with_end(&normalized, cursor)?;
            Some((kind, table, Vec::new()))
        }
        SqlStmtKind::Copy => {
            let copy_idx = find_keyword_top_level_from(&normalized, "COPY", 0)?;
            let cursor = copy_idx + "COPY".len();
            if normalized
                .as_bytes()
                .get(skip_sql_ws(normalized.as_bytes(), cursor))
                .copied()
                == Some(b'(')
            {
                return None;
            }
            let (table, table_end) = parse_sql_object_name_with_end(&normalized, cursor)?;
            let columns = collect_sql_copy_columns(&normalized, table_end);
            Some((kind, table, columns))
        }
        SqlStmtKind::Lock => {
            let lock_idx = find_keyword_top_level_from(&normalized, "LOCK", 0)?;
            let table_idx =
                find_keyword_top_level_from(&normalized, "TABLE", lock_idx + "LOCK".len())?;
            let (table, _) =
                parse_sql_write_object_name_with_end(&normalized, table_idx + "TABLE".len())?;
            Some((kind, table, Vec::new()))
        }
        SqlStmtKind::Create
        | SqlStmtKind::Alter
        | SqlStmtKind::Comment
        | SqlStmtKind::Grant
        | SqlStmtKind::Revoke
        | SqlStmtKind::Analyze
        | SqlStmtKind::Vacuum
        | SqlStmtKind::Reindex
        | SqlStmtKind::Cluster
        | SqlStmtKind::Refresh
        | SqlStmtKind::Drop => None,
    }
}

fn parse_sql_object_name(sql: &str, start: usize) -> Option<String> {
    parse_sql_object_name_with_end(sql, start).map(|(name, _)| name)
}

fn parse_sql_write_object_name_with_end(sql: &str, start: usize) -> Option<(String, usize)> {
    let mut cursor = skip_sql_ws(sql.as_bytes(), start);
    if starts_with_keyword_at(sql, cursor, "ONLY") {
        cursor = skip_sql_ws(sql.as_bytes(), cursor + "ONLY".len());
    }
    parse_sql_object_name_with_end(sql, cursor)
}

fn skip_optional_sql_keyword(sql: &str, start: usize, keyword: &str) -> usize {
    let cursor = skip_sql_ws(sql.as_bytes(), start);
    if starts_with_keyword_at(sql, cursor, keyword) {
        skip_sql_ws(sql.as_bytes(), cursor + keyword.len())
    } else {
        cursor
    }
}

fn parse_sql_object_name_with_end(sql: &str, start: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, start);
    if cursor >= bytes.len() {
        return None;
    }

    let mut segments = Vec::new();
    loop {
        if cursor >= bytes.len() {
            break;
        }

        let (segment, next) = if matches!(bytes[cursor], b'"' | b'`') {
            parse_sql_quoted_identifier(sql, cursor)?
        } else {
            let start_seg = cursor;
            while cursor < bytes.len()
                && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
            {
                cursor += 1;
            }
            (sql.get(start_seg..cursor)?.to_string(), cursor)
        };

        if segment.is_empty() {
            break;
        }
        segments.push(segment);
        cursor = skip_sql_ws(bytes, next);
        if cursor < bytes.len() && bytes[cursor] == b'.' {
            cursor = skip_sql_ws(bytes, cursor + 1);
            continue;
        }
        break;
    }

    let tail = segments.last()?.trim();
    if tail.is_empty() {
        None
    } else {
        Some((tail.to_string(), cursor))
    }
}

fn collect_sql_select_columns(sql: &str, columns_raw: &str, from_idx: usize) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();

    if columns_raw == "*" {
        push_column_ref("*", &mut cols, &mut seen);
    } else {
        collect_sql_projection_columns(columns_raw, &mut cols, &mut seen);
    }

    let clause_min = from_idx + "FROM".len();
    for clause in ["WHERE", "GROUP BY", "HAVING", "ORDER BY"] {
        if let Some(segment) = top_level_sql_clause_segment(sql, clause, clause_min) {
            collect_sql_identifier_columns(segment, &mut cols, &mut seen);
        }
    }
    if let Some(segment) = top_level_sql_clause_segment(sql, "WINDOW", clause_min) {
        collect_sql_window_columns(segment, &mut cols, &mut seen);
    }

    cols
}

fn collect_sql_select_columns_by_source(
    sql: &str,
    columns_raw: &str,
    from_idx: usize,
    sources: &[SqlTableSource],
) -> Vec<Vec<String>> {
    let mut qualified = Vec::new();
    let mut unqualified = Vec::new();

    collect_sql_projection_column_refs(columns_raw, &mut qualified, &mut unqualified);
    collect_sql_join_condition_refs(sql, from_idx, &mut qualified, &mut unqualified);

    let clause_min = from_idx + "FROM".len();
    for clause in ["WHERE", "GROUP BY", "HAVING", "ORDER BY"] {
        if let Some(segment) = top_level_sql_clause_segment(sql, clause, clause_min) {
            collect_sql_column_refs(segment, &mut qualified, &mut unqualified);
        }
    }
    if let Some(segment) = top_level_sql_clause_segment(sql, "WINDOW", clause_min) {
        collect_sql_window_column_refs(segment, &mut qualified, &mut unqualified);
    }

    let mut columns = vec![Vec::new(); sources.len()];
    let mut seen = vec![HashSet::new(); sources.len()];

    for (qualifier, column) in qualified {
        for (idx, source) in sources.iter().enumerate() {
            if sql_ident_eq(&qualifier, &source.alias) || sql_ident_eq(&qualifier, &source.table) {
                push_column_ref(&column, &mut columns[idx], &mut seen[idx]);
            }
        }
    }

    for column in unqualified {
        if sources.len() == 1 {
            push_column_ref(&column, &mut columns[0], &mut seen[0]);
        } else {
            for idx in 0..sources.len() {
                push_column_ref(&column, &mut columns[idx], &mut seen[idx]);
            }
        }
    }

    columns
}

fn collect_sql_projection_column_refs(
    columns_raw: &str,
    qualified: &mut Vec<(String, String)>,
    unqualified: &mut Vec<String>,
) {
    for projection in split_sql_top_level(columns_raw, ',') {
        let mut base = strip_sql_distinct_prefix(projection.trim());
        if let Some((distinct_on_segment, remainder)) = split_sql_distinct_on_projection(base) {
            collect_sql_column_refs(distinct_on_segment, qualified, unqualified);
            base = strip_sql_projection_alias(remainder);
        } else {
            base = strip_sql_projection_alias(base);
        }
        if base == "*" {
            if !unqualified.iter().any(|column| column == "*") {
                unqualified.push("*".to_string());
            }
            continue;
        }

        collect_sql_column_refs(base, qualified, unqualified);
    }
}

fn collect_sql_window_columns(segment: &str, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    let mut qualified = Vec::new();
    let mut unqualified = Vec::new();
    collect_sql_window_column_refs(segment, &mut qualified, &mut unqualified);
    for (_, column) in qualified {
        push_column_ref(&column, cols, seen);
    }
    for column in unqualified {
        push_column_ref(&column, cols, seen);
    }
}

fn collect_sql_window_column_refs(
    segment: &str,
    qualified: &mut Vec<(String, String)>,
    unqualified: &mut Vec<String>,
) {
    let mut cursor = 0usize;
    while let Some(as_idx) = find_keyword_top_level_from(segment, "AS", cursor) {
        let after_as = skip_sql_ws(segment.as_bytes(), as_idx + "AS".len());
        if segment.as_bytes().get(after_as).copied() == Some(b'(')
            && let Some((body, end)) = balanced_paren_segment(segment, after_as)
        {
            collect_sql_column_refs(body, qualified, unqualified);
            cursor = end;
            continue;
        }
        cursor = as_idx + "AS".len();
    }
}

fn collect_sql_join_condition_refs(
    sql: &str,
    from_idx: usize,
    qualified: &mut Vec<(String, String)>,
    unqualified: &mut Vec<String>,
) {
    let start = from_idx + "FROM".len();
    let end = top_level_sql_clause_start(
        sql,
        start,
        &[
            "WHERE",
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "LIMIT",
            "OFFSET",
            "FETCH",
            "FOR",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "WINDOW",
            "RETURNING",
        ],
    )
    .unwrap_or(sql.len());

    collect_sql_join_condition_refs_in_range(sql, start, end, qualified, unqualified);
}

fn collect_sql_join_condition_refs_in_range(
    sql: &str,
    start: usize,
    end: usize,
    qualified: &mut Vec<(String, String)>,
    unqualified: &mut Vec<String>,
) {
    let mut cursor = start;
    while cursor < end {
        let on_idx = find_keyword_top_level_from(sql, "ON", cursor).filter(|idx| *idx < end);
        let using_idx = find_keyword_top_level_from(sql, "USING", cursor).filter(|idx| *idx < end);
        let Some((keyword, idx)) = (match (on_idx, using_idx) {
            (Some(on), Some(using)) if on < using => Some(("ON", on)),
            (Some(_), Some(using)) => Some(("USING", using)),
            (Some(on), None) => Some(("ON", on)),
            (None, Some(using)) => Some(("USING", using)),
            (None, None) => None,
        }) else {
            break;
        };

        if keyword == "USING" {
            let after = skip_sql_ws(sql.as_bytes(), idx + "USING".len());
            if sql.as_bytes().get(after).copied() == Some(b'(')
                && let Some((segment, segment_end)) = balanced_paren_segment(sql, after)
            {
                collect_sql_column_list(segment, unqualified, &mut HashSet::new());
                cursor = segment_end;
                continue;
            }
            cursor = idx + "USING".len();
            continue;
        }

        let segment_start = idx + "ON".len();
        let segment_end = next_sql_join_condition_end(sql, segment_start, end);
        if let Some(segment) = sql.get(segment_start..segment_end) {
            collect_sql_column_refs(segment, qualified, unqualified);
        }
        cursor = segment_end;
    }
}

fn collect_sql_auxiliary_columns_by_source(
    sql: &str,
    sources: &[SqlTableSource],
    source_start: usize,
    source_end: usize,
    clause_min: usize,
    clauses: &[&str],
) -> Vec<Vec<String>> {
    let mut qualified = Vec::new();
    let mut unqualified = Vec::new();

    collect_sql_join_condition_refs_in_range(
        sql,
        source_start,
        source_end,
        &mut qualified,
        &mut unqualified,
    );
    for clause in clauses {
        if let Some(segment) = top_level_sql_clause_segment(sql, clause, clause_min) {
            collect_sql_column_refs(segment, &mut qualified, &mut unqualified);
        }
    }

    let mut columns = vec![Vec::new(); sources.len()];
    let mut seen = vec![HashSet::new(); sources.len()];

    for (qualifier, column) in qualified {
        for (idx, source) in sources.iter().enumerate() {
            if sql_ident_eq(&qualifier, &source.alias) || sql_ident_eq(&qualifier, &source.table) {
                push_column_ref(&column, &mut columns[idx], &mut seen[idx]);
            }
        }
    }

    push_unqualified_columns_to_sources(sources, unqualified, &mut columns, &mut seen);

    columns
}

fn collect_sql_update_from_columns_by_source(
    sql: &str,
    sources: &[SqlTableSource],
    source_start: usize,
    source_end: usize,
    clause_min: usize,
) -> Vec<Vec<String>> {
    let mut qualified = Vec::new();
    let mut unqualified = Vec::new();

    collect_sql_join_condition_refs_in_range(
        sql,
        source_start,
        source_end,
        &mut qualified,
        &mut unqualified,
    );
    if let Some(segment) = top_level_sql_clause_segment(sql, "SET", clause_min) {
        for assignment in split_sql_top_level(segment, ',') {
            let Some((_, right)) = assignment.split_once('=') else {
                continue;
            };
            collect_sql_column_refs(right, &mut qualified, &mut unqualified);
        }
    }
    for clause in ["WHERE", "RETURNING"] {
        if let Some(segment) = top_level_sql_clause_segment(sql, clause, clause_min) {
            collect_sql_column_refs(segment, &mut qualified, &mut unqualified);
        }
    }

    let mut columns = vec![Vec::new(); sources.len()];
    let mut seen = vec![HashSet::new(); sources.len()];

    for (qualifier, column) in qualified {
        for (idx, source) in sources.iter().enumerate() {
            if sql_ident_eq(&qualifier, &source.alias) || sql_ident_eq(&qualifier, &source.table) {
                push_column_ref(&column, &mut columns[idx], &mut seen[idx]);
            }
        }
    }

    push_unqualified_columns_to_sources(sources, unqualified, &mut columns, &mut seen);

    columns
}

fn push_unqualified_columns_to_sources(
    sources: &[SqlTableSource],
    unqualified: Vec<String>,
    columns: &mut [Vec<String>],
    seen: &mut [HashSet<String>],
) {
    for column in unqualified {
        if sources.len() == 1 {
            push_column_ref(&column, &mut columns[0], &mut seen[0]);
        } else {
            for idx in 0..sources.len() {
                push_column_ref(&column, &mut columns[idx], &mut seen[idx]);
            }
        }
    }
}

fn collect_sql_merge_columns_by_source(
    sql: &str,
    sources: &[SqlTableSource],
    on_idx: usize,
) -> Vec<Vec<String>> {
    let mut qualified = Vec::new();
    let mut unqualified = Vec::new();

    if let Some(segment) = sql.get(on_idx..) {
        collect_sql_column_refs(segment, &mut qualified, &mut unqualified);
    }

    columns_for_qualified_refs(sources, qualified)
}

fn collect_sql_merge_target_columns(
    sql: &str,
    target_table: &str,
    target_alias: &str,
    min_idx: usize,
) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    let source = SqlTableSource {
        table: target_table.to_string(),
        alias: target_alias.to_string(),
    };

    if let Some(on_idx) = find_keyword_top_level_from(sql, "ON", min_idx) {
        let scan_end = top_level_sql_clause_start(sql, on_idx, &["RETURNING"]).unwrap_or(sql.len());
        let mut qualified = Vec::new();
        let mut unqualified = Vec::new();
        if let Some(segment) = sql.get(on_idx..scan_end) {
            collect_sql_column_refs(segment, &mut qualified, &mut unqualified);
        }
        for column_set in columns_for_qualified_refs(&[source], qualified) {
            for column in column_set {
                push_column_ref(&column, &mut cols, &mut seen);
            }
        }
    }

    collect_sql_merge_update_target_columns(sql, min_idx, &mut cols, &mut seen);
    collect_sql_merge_insert_target_columns(sql, min_idx, &mut cols, &mut seen);
    if let Some(returning) = top_level_sql_clause_segment(sql, "RETURNING", min_idx) {
        collect_sql_target_projection_columns_from_segment(
            returning,
            target_table,
            target_alias,
            &mut cols,
            &mut seen,
        );
    }

    cols
}

fn columns_for_qualified_refs(
    sources: &[SqlTableSource],
    qualified: Vec<(String, String)>,
) -> Vec<Vec<String>> {
    let mut columns = vec![Vec::new(); sources.len()];
    let mut seen = vec![HashSet::new(); sources.len()];

    for (qualifier, column) in qualified {
        for (idx, source) in sources.iter().enumerate() {
            if sql_ident_eq(&qualifier, &source.alias) || sql_ident_eq(&qualifier, &source.table) {
                push_column_ref(&column, &mut columns[idx], &mut seen[idx]);
            }
        }
    }

    columns
}

fn collect_sql_merge_update_target_columns(
    sql: &str,
    min_idx: usize,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let mut cursor = min_idx;
    while let Some(set_idx) = find_keyword_top_level_from(sql, "SET", cursor) {
        let start = set_idx + "SET".len();
        let end =
            top_level_sql_clause_start(sql, start, &["WHEN", "RETURNING"]).unwrap_or(sql.len());
        if let Some(segment) = sql.get(start..end) {
            for assignment in split_sql_top_level(segment, ',') {
                let left = assignment
                    .split_once('=')
                    .map_or(assignment, |(left, _)| left);
                if let Some((column, _, _)) = parse_sql_identifier_path(left.trim(), 0) {
                    push_column_ref(&column, cols, seen);
                }
            }
        }
        cursor = end;
    }
}

fn collect_sql_merge_insert_target_columns(
    sql: &str,
    min_idx: usize,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let mut cursor = min_idx;
    while let Some(insert_idx) = find_keyword_top_level_from(sql, "INSERT", cursor) {
        let after_insert = skip_sql_ws(sql.as_bytes(), insert_idx + "INSERT".len());
        if sql.as_bytes().get(after_insert).copied() == Some(b'(')
            && let Some((segment, end)) = balanced_paren_segment(sql, after_insert)
        {
            collect_sql_column_list(segment, cols, seen);
            cursor = end;
            continue;
        }
        cursor = insert_idx + "INSERT".len();
    }
}

fn next_sql_join_condition_end(sql: &str, start: usize, end: usize) -> usize {
    find_keyword_top_level_from(sql, "JOIN", start)
        .filter(|idx| *idx < end)
        .unwrap_or(end)
}

fn collect_sql_column_refs(
    segment: &str,
    qualified: &mut Vec<(String, String)>,
    unqualified: &mut Vec<String>,
) {
    let bytes = segment.as_bytes();
    let mut i = 0usize;
    let mut seen_qualified = HashSet::new();
    let mut seen_unqualified = HashSet::new();

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = skip_sql_single_quote(bytes, i + 1);
                continue;
            }
            b'(' => {
                if let Some((nested, end)) = balanced_paren_segment(segment, i)
                    && classify_sql_kind(&normalize_whitespace(nested.trim())).is_some()
                {
                    i = end;
                    continue;
                }
                i += 1;
                continue;
            }
            b'"' | b'`' | b'a'..=b'z' | b'A'..=b'Z' | b'_' => {}
            _ => {
                i += 1;
                continue;
            }
        }

        if is_sql_cast_type_start(segment, i) || is_sql_cast_as_type_start(segment, i) {
            i = parse_sql_identifier_path_parts(segment, i)
                .map(|(_, next)| next)
                .unwrap_or(i + 1);
            continue;
        }

        let Some((parts, next)) = parse_sql_identifier_path_parts(segment, i) else {
            i += 1;
            continue;
        };
        let after = skip_sql_ws(bytes, next);
        if after < bytes.len() && bytes[after] == b'(' {
            i = next;
            continue;
        }
        if parts.len() == 1 && previous_sql_keyword_is(segment, i, "OVER") {
            i = next;
            continue;
        }
        if parts.len() == 1 && previous_sql_keyword_is(segment, i, "COLLATE") {
            i = next;
            continue;
        }
        if parts.len() == 1 && should_skip_sql_syntax_identifier(segment, i, next, &parts[0]) {
            i = next;
            continue;
        }

        if let Some(column) = parts.last()
            && !is_sql_reference_keyword(column)
        {
            if parts.len() >= 2 {
                let qualifier = parts[parts.len() - 2].clone();
                if seen_qualified.insert((qualifier.clone(), column.clone())) {
                    qualified.push((qualifier, column.clone()));
                }
            } else if seen_unqualified.insert(column.clone()) {
                unqualified.push(column.clone());
            }
        }

        i = next;
    }
}

fn collect_sql_projection_columns(
    columns_raw: &str,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    for projection in split_sql_top_level(columns_raw, ',') {
        let mut base = strip_sql_distinct_prefix(projection.trim());
        if let Some((distinct_on_segment, remainder)) = split_sql_distinct_on_projection(base) {
            collect_sql_identifier_columns(distinct_on_segment, cols, seen);
            base = strip_sql_projection_alias(remainder);
        } else {
            base = strip_sql_projection_alias(base);
        }
        if base == "*" {
            push_column_ref("*", cols, seen);
            continue;
        }

        if let Some(column) = normalize_projection_column(base)
            && is_plain_sql_column_ref(&column)
            && !is_sql_numeric_literal(&column)
            && !is_sql_reference_keyword(&column)
        {
            push_column_ref(&column, cols, seen);
            continue;
        }

        collect_sql_identifier_columns(base, cols, seen);
    }
}

fn strip_sql_distinct_prefix(input: &str) -> &str {
    let trimmed = input.trim();
    for keyword in ["DISTINCT", "ALL"] {
        if trimmed.len() >= keyword.len()
            && trimmed[..keyword.len()].eq_ignore_ascii_case(keyword)
            && trimmed
                .as_bytes()
                .get(keyword.len())
                .is_some_and(|b| b.is_ascii_whitespace())
        {
            return trimmed[keyword.len()..].trim_start();
        }
    }
    trimmed
}

fn split_sql_distinct_on_projection(input: &str) -> Option<(&str, &str)> {
    let trimmed = input.trim_start();
    if !starts_with_keyword_at(trimmed, 0, "ON") {
        return None;
    }
    let after_on = skip_sql_ws(trimmed.as_bytes(), "ON".len());
    if trimmed.as_bytes().get(after_on).copied() != Some(b'(') {
        return None;
    }

    let (segment, end) = balanced_paren_segment(trimmed, after_on)?;
    Some((segment, trimmed.get(end..).unwrap_or_default().trim_start()))
}

fn is_plain_sql_column_ref(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.'))
}

fn is_sql_numeric_literal(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
}

fn top_level_sql_clause_segment<'a>(sql: &'a str, clause: &str, min_idx: usize) -> Option<&'a str> {
    let clause_idx = find_keyword_top_level_from(sql, clause, min_idx)?;
    let start = clause_idx + clause.len();
    let end = top_level_sql_clause_start(
        sql,
        start,
        &[
            "WHERE",
            "FROM",
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "SET",
            "VALUES",
            "ON CONFLICT",
            "USING",
            "RETURNING",
            "LIMIT",
            "OFFSET",
            "FETCH",
            "FOR",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "WINDOW",
        ],
    )
    .unwrap_or(sql.len());

    sql.get(start..end)
}

fn top_level_sql_clause_start(sql: &str, min_idx: usize, clauses: &[&str]) -> Option<usize> {
    clauses
        .iter()
        .filter_map(|keyword| find_keyword_top_level_from(sql, keyword, min_idx))
        .min()
}

fn sql_table_source_clause_end(sql: &str, start: usize) -> usize {
    top_level_sql_clause_start(
        sql,
        start,
        &[
            "WHERE",
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "LIMIT",
            "OFFSET",
            "FETCH",
            "FOR",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "WINDOW",
            "RETURNING",
        ],
    )
    .unwrap_or(sql.len())
}

fn collect_sql_insert_columns(
    sql: &str,
    target_table: &str,
    target_alias: &str,
    table_end: usize,
) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    let cursor = skip_sql_ws(sql.as_bytes(), table_end);
    if sql.as_bytes().get(cursor).copied() == Some(b'(')
        && let Some((segment, _)) = balanced_paren_segment(sql, cursor)
    {
        collect_sql_column_list(segment, &mut cols, &mut seen);
    }
    collect_sql_insert_conflict_columns(
        sql,
        target_table,
        target_alias,
        table_end,
        &mut cols,
        &mut seen,
    );
    if let Some(returning) = top_level_sql_clause_segment(sql, "RETURNING", table_end) {
        collect_sql_target_projection_columns_from_segment(
            returning,
            target_table,
            target_alias,
            &mut cols,
            &mut seen,
        );
    }
    cols
}

fn parse_sql_optional_insert_alias(sql: &str, table_end: usize) -> (Option<String>, usize) {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, table_end);
    if starts_with_keyword_at(sql, cursor, "AS") {
        cursor = skip_sql_ws(bytes, cursor + "AS".len());
        return parse_sql_identifier_segment(sql, cursor)
            .map(|(alias, end)| (Some(alias), end))
            .unwrap_or((None, cursor));
    }
    if let Some((alias, end)) = parse_sql_identifier_segment(sql, cursor)
        && !is_sql_table_source_boundary(&alias)
    {
        return (Some(alias), end);
    }
    (None, table_end)
}

fn collect_sql_update_columns(
    sql: &str,
    target_table: &str,
    target_alias: &str,
    table_end: usize,
) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    if let Some(set_segment) = top_level_sql_clause_segment(sql, "SET", table_end) {
        collect_sql_assignment_target_columns(set_segment, &mut cols, &mut seen);
        for assignment in split_sql_top_level(set_segment, ',') {
            let Some((_, right)) = assignment.split_once('=') else {
                continue;
            };
            collect_sql_target_columns_from_segment(
                right,
                target_table,
                target_alias,
                true,
                &mut cols,
                &mut seen,
            );
        }
    }
    if let Some(segment) = top_level_sql_clause_segment(sql, "WHERE", table_end) {
        collect_sql_target_columns_from_segment(
            segment,
            target_table,
            target_alias,
            true,
            &mut cols,
            &mut seen,
        );
    }
    if let Some(segment) = top_level_sql_clause_segment(sql, "RETURNING", table_end) {
        collect_sql_target_projection_columns_from_segment(
            segment,
            target_table,
            target_alias,
            &mut cols,
            &mut seen,
        );
    }
    cols
}

fn collect_sql_delete_columns(
    sql: &str,
    target_table: &str,
    target_alias: &str,
    table_end: usize,
) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    if let Some(segment) = top_level_sql_clause_segment(sql, "WHERE", table_end) {
        collect_sql_target_columns_from_segment(
            segment,
            target_table,
            target_alias,
            true,
            &mut cols,
            &mut seen,
        );
    }
    if let Some(segment) = top_level_sql_clause_segment(sql, "RETURNING", table_end) {
        collect_sql_target_projection_columns_from_segment(
            segment,
            target_table,
            target_alias,
            &mut cols,
            &mut seen,
        );
    }
    cols
}

fn collect_sql_insert_conflict_columns(
    sql: &str,
    target_table: &str,
    target_alias: &str,
    min_idx: usize,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let Some(conflict_idx) = find_keyword_top_level_from(sql, "ON CONFLICT", min_idx) else {
        return;
    };
    let after_conflict = skip_sql_ws(sql.as_bytes(), conflict_idx + "ON CONFLICT".len());
    if sql.as_bytes().get(after_conflict).copied() == Some(b'(')
        && let Some((segment, _)) = balanced_paren_segment(sql, after_conflict)
    {
        collect_sql_column_list(segment, cols, seen);
    }

    let do_update_idx = find_keyword_top_level_from(sql, "DO UPDATE SET", after_conflict);
    let conflict_end = do_update_idx.unwrap_or_else(|| {
        top_level_sql_clause_start(sql, after_conflict, &["RETURNING"]).unwrap_or(sql.len())
    });
    collect_sql_conflict_where_columns(
        sql,
        target_table,
        target_alias,
        after_conflict,
        conflict_end,
        cols,
        seen,
    );

    if let Some(do_update_idx) = do_update_idx {
        let set_start = do_update_idx + "DO UPDATE SET".len();
        let set_end = top_level_sql_clause_start(sql, set_start, &["WHERE", "RETURNING"])
            .unwrap_or(sql.len());
        if let Some(segment) = sql.get(set_start..set_end) {
            collect_sql_assignment_target_columns(segment, cols, seen);
            for assignment in split_sql_top_level(segment, ',') {
                let Some((_, right)) = assignment.split_once('=') else {
                    continue;
                };
                collect_sql_target_columns_from_segment(
                    right,
                    target_table,
                    target_alias,
                    true,
                    cols,
                    seen,
                );
            }
        }

        let update_end =
            top_level_sql_clause_start(sql, set_end, &["RETURNING"]).unwrap_or(sql.len());
        collect_sql_conflict_where_columns(
            sql,
            target_table,
            target_alias,
            set_end,
            update_end,
            cols,
            seen,
        );
    }
}

fn collect_sql_conflict_where_columns(
    sql: &str,
    target_table: &str,
    target_alias: &str,
    start: usize,
    end: usize,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let mut cursor = start;
    while let Some(where_idx) = find_keyword_top_level_from(sql, "WHERE", cursor) {
        if where_idx >= end {
            break;
        }
        let segment_start = where_idx + "WHERE".len();
        let segment_end =
            top_level_sql_clause_start(sql, segment_start, &["DO UPDATE SET", "RETURNING"])
                .filter(|idx| *idx <= end)
                .unwrap_or(end);
        if let Some(segment) = sql.get(segment_start..segment_end) {
            collect_sql_target_columns_from_segment(
                segment,
                target_table,
                target_alias,
                true,
                cols,
                seen,
            );
        }
        cursor = segment_end;
    }
}

fn collect_sql_assignment_target_columns(
    segment: &str,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    for assignment in split_sql_top_level(segment, ',') {
        let left = assignment
            .split_once('=')
            .map_or(assignment, |(left, _)| left);
        if let Some((column, _, _)) = parse_sql_identifier_path(left.trim(), 0) {
            push_column_ref(&column, cols, seen);
        } else if left.trim_start().starts_with('(')
            && let Some((columns, _)) = balanced_paren_segment(left.trim_start(), 0)
        {
            collect_sql_column_list(columns, cols, seen);
        }
    }
}

fn collect_sql_target_columns_from_segment(
    segment: &str,
    target_table: &str,
    target_alias: &str,
    include_unqualified: bool,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    collect_sql_target_columns_from_segment_with_aliases(
        segment,
        target_table,
        &[target_alias],
        include_unqualified,
        cols,
        seen,
    );
}

fn collect_sql_target_columns_from_segment_with_aliases(
    segment: &str,
    target_table: &str,
    target_aliases: &[&str],
    include_unqualified: bool,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let mut qualified = Vec::new();
    let mut unqualified = Vec::new();
    collect_sql_column_refs(segment, &mut qualified, &mut unqualified);

    let sources = target_aliases
        .iter()
        .map(|alias| SqlTableSource {
            table: target_table.to_string(),
            alias: (*alias).to_string(),
        })
        .collect::<Vec<_>>();
    for column_set in columns_for_qualified_refs(&sources, qualified) {
        for column in column_set {
            push_column_ref(&column, cols, seen);
        }
    }
    if include_unqualified {
        for column in unqualified {
            push_column_ref(&column, cols, seen);
        }
    }
}

fn collect_sql_target_projection_columns_from_segment(
    segment: &str,
    target_table: &str,
    target_alias: &str,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let (segment, returning_aliases) = strip_sql_returning_with_aliases(segment);
    let mut target_aliases = vec![target_alias, "old", "new"];
    target_aliases.extend(returning_aliases.iter().map(String::as_str));

    for projection in split_sql_top_level(segment, ',') {
        let base = strip_sql_projection_alias(projection);
        collect_sql_target_columns_from_segment_with_aliases(
            base,
            target_table,
            &target_aliases,
            true,
            cols,
            seen,
        );
    }
}

fn strip_sql_returning_with_aliases(segment: &str) -> (&str, Vec<String>) {
    let trimmed = segment.trim_start();
    if !starts_with_keyword_at(trimmed, 0, "WITH") {
        return (segment, Vec::new());
    }

    let after_with = skip_sql_ws(trimmed.as_bytes(), "WITH".len());
    if trimmed.as_bytes().get(after_with).copied() != Some(b'(') {
        return (segment, Vec::new());
    }

    let Some((alias_segment, end)) = balanced_paren_segment(trimmed, after_with) else {
        return (segment, Vec::new());
    };
    let aliases = parse_sql_returning_with_aliases(alias_segment);
    let rest = trimmed.get(end..).unwrap_or_default().trim_start();
    (rest, aliases)
}

fn parse_sql_returning_with_aliases(segment: &str) -> Vec<String> {
    let mut aliases = Vec::new();
    for item in split_sql_top_level(segment, ',') {
        let item = item.trim();
        let Some(as_idx) = find_keyword_top_level_from(item, "AS", 0) else {
            continue;
        };
        let kind = item.get(..as_idx).unwrap_or_default().trim();
        if !starts_with_keyword_at(kind, 0, "OLD") && !starts_with_keyword_at(kind, 0, "NEW") {
            continue;
        }
        if let Some((alias, _)) = parse_sql_identifier_segment(item, as_idx + "AS".len()) {
            aliases.push(alias);
        }
    }
    aliases
}

fn strip_sql_projection_alias(projection: &str) -> &str {
    let projection = projection.trim();
    if let Some(as_idx) = find_keyword_top_level_from(projection, "AS", 0) {
        projection.get(..as_idx).unwrap_or(projection).trim()
    } else if let Some(alias_start) = sql_trailing_projection_alias_start(projection) {
        projection.get(..alias_start).unwrap_or(projection).trim()
    } else {
        projection
    }
}

fn sql_trailing_projection_alias_start(projection: &str) -> Option<usize> {
    let trimmed_end = projection.trim_end().len();
    if trimmed_end == 0 {
        return None;
    }
    let bytes = projection.as_bytes();
    let mut alias_start = trimmed_end;
    let quote = bytes[trimmed_end - 1];

    if matches!(quote, b'"' | b'`') {
        alias_start = find_sql_opening_quote_before(projection, trimmed_end - 1, quote)?;
    } else {
        while alias_start > 0 && is_ident_char(bytes[alias_start - 1] as char) {
            alias_start -= 1;
        }
        if alias_start == trimmed_end {
            return None;
        }
        let first = bytes[alias_start];
        if !matches!(first, b'a'..=b'z' | b'A'..=b'Z' | b'_') {
            return None;
        }
    }

    if alias_start == 0 || !bytes[alias_start - 1].is_ascii_whitespace() {
        return None;
    }
    let base = projection.get(..alias_start)?.trim_end();
    if base.is_empty() || sql_projection_alias_base_blocks_strip(base) {
        return None;
    }

    Some(alias_start)
}

fn find_sql_opening_quote_before(input: &str, close_idx: usize, quote: u8) -> Option<usize> {
    input
        .as_bytes()
        .get(..close_idx)?
        .iter()
        .enumerate()
        .rev()
        .find_map(|(idx, byte)| (*byte == quote).then_some(idx))
}

fn sql_projection_alias_base_blocks_strip(base: &str) -> bool {
    let previous = base
        .rsplit(|ch: char| !is_ident_char(ch))
        .find(|part| !part.is_empty());

    previous.is_some_and(|word| {
        matches!(
            word.to_ascii_uppercase().as_str(),
            "AT" | "COLLATE" | "TIME" | "ZONE"
        )
    })
}

fn collect_sql_column_list(segment: &str, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    for item in split_sql_top_level(segment, ',') {
        if let Some((column, _, _)) = parse_sql_identifier_path(item.trim(), 0) {
            push_column_ref(&column, cols, seen);
        }
    }
}

fn collect_sql_copy_columns(sql: &str, table_end: usize) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    let cursor = skip_sql_ws(sql.as_bytes(), table_end);
    if sql.as_bytes().get(cursor).copied() == Some(b'(')
        && let Some((segment, _)) = balanced_paren_segment(sql, cursor)
    {
        collect_sql_column_list(segment, &mut cols, &mut seen);
    }
    if let Some(where_segment) = top_level_sql_clause_segment(sql, "WHERE", table_end) {
        collect_sql_identifier_columns(where_segment, &mut cols, &mut seen);
    }
    cols
}

fn normalize_projection_column(expr: &str) -> Option<String> {
    let expr = expr.trim();
    if expr.is_empty() {
        return None;
    }
    if expr == "*" {
        return Some("*".to_string());
    }

    let mut base = expr;
    if let Some(as_idx) = find_keyword_top_level_from(expr, "AS", 0) {
        base = expr.get(..as_idx).unwrap_or(expr).trim();
    }
    if let Some(cast_idx) = find_sql_top_level_cast_operator(base) {
        base = base.get(..cast_idx).unwrap_or(base).trim();
    }
    let token = base.split_whitespace().next().unwrap_or(base).trim();
    if token.is_empty() {
        return None;
    }

    let normalized = token.trim_matches('"').trim_matches('`');
    let tail = normalized.rsplit('.').next().unwrap_or(normalized).trim();
    if tail.is_empty() {
        None
    } else {
        Some(tail.to_string())
    }
}

fn balanced_paren_segment(input: &str, open_idx: usize) -> Option<(&str, usize)> {
    let bytes = input.as_bytes();
    if bytes.get(open_idx).copied() != Some(b'(') {
        return None;
    }

    let mut depth = 1i32;
    let mut i = open_idx + 1;
    let start = i;
    let mut in_quote: Option<u8> = None;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if let Some(next) = advance_sql_quoted_index(bytes, i, q) {
                i = next;
                continue;
            }
            in_quote = None;
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((input.get(start..i)?, i + 1));
                }
            }
            _ => {}
        }
        i += 1;
    }

    None
}

fn collect_sql_identifier_columns(
    segment: &str,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let bytes = segment.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = skip_sql_single_quote(bytes, i + 1);
                continue;
            }
            b'(' => {
                if let Some((nested, end)) = balanced_paren_segment(segment, i)
                    && classify_sql_kind(&normalize_whitespace(nested.trim())).is_some()
                {
                    i = end;
                    continue;
                }
                i += 1;
                continue;
            }
            b'"' | b'`' | b'a'..=b'z' | b'A'..=b'Z' | b'_' => {}
            _ => {
                i += 1;
                continue;
            }
        }

        if is_sql_cast_type_start(segment, i) || is_sql_cast_as_type_start(segment, i) {
            i = parse_sql_identifier_path_parts(segment, i)
                .map(|(_, next)| next)
                .unwrap_or(i + 1);
            continue;
        }

        let Some((column, next, segment_count)) = parse_sql_identifier_path(segment, i) else {
            i += 1;
            continue;
        };
        let after = skip_sql_ws(bytes, next);
        if after < bytes.len() && bytes[after] == b'(' {
            i = next;
            continue;
        }
        if segment_count == 1 && previous_sql_keyword_is(segment, i, "OVER") {
            i = next;
            continue;
        }
        if segment_count == 1 && previous_sql_keyword_is(segment, i, "COLLATE") {
            i = next;
            continue;
        }
        if segment_count == 1 && should_skip_sql_syntax_identifier(segment, i, next, &column) {
            i = next;
            continue;
        }
        if !is_sql_reference_keyword(&column) {
            push_column_ref(&column, cols, seen);
        }
        i = next;
    }
}

fn parse_sql_identifier_path(input: &str, start: usize) -> Option<(String, usize, usize)> {
    let bytes = input.as_bytes();
    let (mut last, mut cursor) = parse_sql_identifier_segment(input, start)?;
    let mut count = 1usize;

    loop {
        cursor = skip_sql_ws(bytes, cursor);
        if cursor < bytes.len() && bytes[cursor] == b'.' {
            let (segment, next) = parse_sql_identifier_segment(input, cursor + 1)?;
            last = segment;
            count += 1;
            cursor = next;
            continue;
        }
        break;
    }

    Some((last, cursor, count))
}

fn parse_sql_identifier_path_parts(input: &str, start: usize) -> Option<(Vec<String>, usize)> {
    let bytes = input.as_bytes();
    let (first, mut cursor) = parse_sql_identifier_segment(input, start)?;
    let mut parts = vec![first];

    loop {
        cursor = skip_sql_ws(bytes, cursor);
        if cursor < bytes.len() && bytes[cursor] == b'.' {
            let next_start = skip_sql_ws(bytes, cursor + 1);
            if bytes.get(next_start).copied() == Some(b'*') {
                parts.push("*".to_string());
                cursor = next_start + 1;
                break;
            }
            let (segment, next) = parse_sql_identifier_segment(input, next_start)?;
            parts.push(segment);
            cursor = next;
            continue;
        }
        break;
    }

    Some((parts, cursor))
}

fn parse_sql_identifier_segment(input: &str, start: usize) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    let cursor = skip_sql_ws(bytes, start);
    if cursor >= bytes.len() {
        return None;
    }

    if matches!(bytes[cursor], b'"' | b'`') {
        return parse_sql_quoted_identifier(input, cursor);
    }

    if !matches!(bytes[cursor], b'a'..=b'z' | b'A'..=b'Z' | b'_') {
        return None;
    }

    let mut i = cursor + 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }

    Some((input.get(cursor..i)?.to_string(), i))
}

fn previous_sql_keyword_is(input: &str, idx: usize, keyword: &str) -> bool {
    let before = input.get(..idx).unwrap_or_default().trim_end();
    let Some(start) = before.rfind(|ch: char| !is_ident_char(ch)) else {
        return before.eq_ignore_ascii_case(keyword);
    };
    before
        .get(start + 1..)
        .is_some_and(|word| word.eq_ignore_ascii_case(keyword))
}

fn should_skip_sql_syntax_identifier(segment: &str, start: usize, end: usize, ident: &str) -> bool {
    let upper = ident.to_ascii_uppercase();
    match upper.as_str() {
        "AT" => {
            let after = skip_sql_ws(segment.as_bytes(), end);
            starts_with_keyword_at(segment, after, "TIME")
        }
        "TIME" => previous_sql_keyword_is(segment, start, "AT"),
        "ZONE" => previous_sql_keyword_is(segment, start, "TIME"),
        "INTERVAL" => {
            matches!(
                previous_sql_non_ws_byte(segment, start),
                Some(b'+' | b'-' | b'*' | b'/' | b'(' | b'=' | b'<' | b'>')
            )
        }
        "ESCAPE" => has_unclosed_like_before(segment, start),
        "EPOCH" => {
            let after = skip_sql_ws(segment.as_bytes(), end);
            starts_with_keyword_at(segment, after, "FROM")
        }
        _ => false,
    }
}

fn is_sql_cast_type_start(segment: &str, idx: usize) -> bool {
    let bytes = segment.as_bytes();
    let mut cursor = idx;
    while cursor > 0 && bytes[cursor - 1].is_ascii_whitespace() {
        cursor -= 1;
    }

    cursor >= 2 && bytes[cursor - 1] == b':' && bytes[cursor - 2] == b':'
}

fn is_sql_cast_as_type_start(segment: &str, idx: usize) -> bool {
    if !previous_sql_keyword_is(segment, idx, "AS") {
        return false;
    }

    let bytes = segment.as_bytes();
    let mut cursor = 0usize;
    let mut depth = 0i32;
    let mut cast_depths = Vec::new();
    let mut in_quote: Option<u8> = None;
    let mut pending_cast_open = None;

    while cursor < idx && cursor < bytes.len() {
        if let Some(q) = in_quote {
            if let Some(next) = advance_sql_quoted_index(bytes, cursor, q) {
                cursor = next;
                continue;
            }
            in_quote = None;
            cursor += 1;
            continue;
        }

        match bytes[cursor] {
            b'\'' | b'"' | b'`' => {
                in_quote = Some(bytes[cursor]);
                cursor += 1;
                continue;
            }
            b'(' => {
                depth += 1;
                if pending_cast_open == Some(cursor) {
                    cast_depths.push(depth);
                    pending_cast_open = None;
                }
                cursor += 1;
                continue;
            }
            b')' => {
                if cast_depths.last().copied() == Some(depth) {
                    cast_depths.pop();
                }
                depth -= 1;
                cursor += 1;
                continue;
            }
            _ => {}
        }

        if starts_with_keyword_at(segment, cursor, "CAST") {
            let after_cast = skip_sql_ws(bytes, cursor + "CAST".len());
            if bytes.get(after_cast).copied() == Some(b'(') {
                pending_cast_open = Some(after_cast);
            }
            cursor += "CAST".len();
            continue;
        }

        cursor += 1;
    }

    !cast_depths.is_empty()
}

fn previous_sql_non_ws_byte(segment: &str, idx: usize) -> Option<u8> {
    segment
        .as_bytes()
        .get(..idx)?
        .iter()
        .rev()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
}

fn has_unclosed_like_before(segment: &str, idx: usize) -> bool {
    let mut cursor = 0usize;
    let mut like_idx = None;
    while let Some(found) = find_keyword_top_level_from(segment, "LIKE", cursor) {
        if found >= idx {
            break;
        }
        like_idx = Some(found);
        cursor = found + "LIKE".len();
    }

    let Some(like_idx) = like_idx else {
        return false;
    };
    for boundary in ["AND", "OR", "WHERE", "HAVING", "ORDER BY", "GROUP BY"] {
        if find_keyword_top_level_from(segment, boundary, like_idx + "LIKE".len())
            .is_some_and(|found| found < idx)
        {
            return false;
        }
    }
    true
}

fn parse_sql_quoted_identifier(input: &str, start: usize) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    let quote = *bytes.get(start)?;
    if !matches!(quote, b'"' | b'`') {
        return None;
    }

    let mut out = String::new();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1).copied() == Some(quote) {
                out.push(quote as char);
                i += 2;
                continue;
            }
            return Some((out, i + 1));
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    None
}

fn skip_sql_single_quote(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() {
        if bytes[idx] == b'\'' {
            if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                idx += 2;
                continue;
            }
            return idx + 1;
        }
        idx += 1;
    }
    idx
}

fn is_sql_reference_keyword(ident: &str) -> bool {
    matches!(
        ident.to_ascii_uppercase().as_str(),
        "ALL"
            | "AND"
            | "ANY"
            | "ASC"
            | "AS"
            | "BETWEEN"
            | "BY"
            | "CASE"
            | "COLLATE"
            | "CONFLICT"
            | "CONSTRAINT"
            | "CROSS"
            | "CUBE"
            | "CURRENT_DATE"
            | "CURRENT_TIME"
            | "CURRENT_TIMESTAMP"
            | "DESC"
            | "DISTINCT"
            | "DO"
            | "ELSE"
            | "END"
            | "EXCLUDED"
            | "FALSE"
            | "FIRST"
            | "FROM"
            | "FOLLOWING"
            | "GROUP"
            | "GROUPING"
            | "GROUPS"
            | "HAVING"
            | "IN"
            | "INNER"
            | "INSERT"
            | "IS"
            | "JOIN"
            | "LAST"
            | "LEFT"
            | "LIKE"
            | "LIMIT"
            | "LOCALTIME"
            | "LOCALTIMESTAMP"
            | "NATURAL"
            | "NOT"
            | "NOTHING"
            | "NULL"
            | "NULLS"
            | "OFFSET"
            | "ON"
            | "OR"
            | "ORDER"
            | "ORDINALITY"
            | "OUTER"
            | "OVER"
            | "PARTITION"
            | "PRECEDING"
            | "RANGE"
            | "RIGHT"
            | "ROW"
            | "ROWS"
            | "ROLLUP"
            | "SELECT"
            | "SET"
            | "SETS"
            | "TABLESAMPLE"
            | "THEN"
            | "TIES"
            | "TRUE"
            | "UNBOUNDED"
            | "UPDATE"
            | "USING"
            | "VALUES"
            | "WHEN"
            | "WHERE"
            | "WINDOW"
            | "WITH"
    )
}

fn split_sql_top_level(input: &str, delimiter: char) -> Vec<&str> {
    let bytes = input.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if let Some(next) = advance_sql_quoted_index(bytes, i, q) {
                i = next;
                continue;
            }
            in_quote = None;
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }

        if b == delimiter as u8 && depth == 0 {
            out.push(input.get(start..i).unwrap_or_default());
            start = i + 1;
        }
        i += 1;
    }
    out.push(input.get(start..).unwrap_or_default());
    out
}

fn find_sql_top_level_byte(sql: &str, min_idx: usize, needle: u8) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut i = min_idx;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if let Some(next) = advance_sql_quoted_index(bytes, i, q) {
                i = next;
                continue;
            }
            in_quote = None;
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => {
                if depth == 0 && b == needle {
                    return Some(i);
                }
                depth += 1;
            }
            b')' => depth -= 1,
            _ if depth == 0 && b == needle => return Some(i),
            _ => {}
        }

        i += 1;
    }

    None
}

fn find_sql_top_level_cast_operator(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if let Some(next) = advance_sql_quoted_index(bytes, i, q) {
                i = next;
                continue;
            }
            in_quote = None;
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => depth -= 1,
            b':' if depth == 0 && bytes.get(i + 1).copied() == Some(b':') => return Some(i),
            _ => {}
        }

        i += 1;
    }

    None
}

fn find_keyword_at_depth_from(
    sql: &str,
    keyword: &str,
    min_idx: usize,
    target_depth: i32,
) -> Option<usize> {
    if keyword.is_empty() {
        return None;
    }

    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if let Some(next) = advance_sql_quoted_index(bytes, i, q) {
                i = next;
                continue;
            }
            in_quote = None;
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => {
                in_quote = Some(b);
                i += 1;
                continue;
            }
            b'(' => {
                if i >= min_idx && depth == target_depth && starts_with_keyword_at(sql, i, keyword)
                {
                    return Some(i);
                }
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                depth -= 1;
                i += 1;
                continue;
            }
            _ => {}
        }

        if i >= min_idx && depth == target_depth && starts_with_keyword_at(sql, i, keyword) {
            return Some(i);
        }
        i += 1;
    }

    None
}

fn find_keyword_top_level_from(sql: &str, keyword: &str, min_idx: usize) -> Option<usize> {
    if keyword.is_empty() {
        return None;
    }

    let bytes = sql.as_bytes();
    let upper = bytes
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let kw = keyword
        .as_bytes()
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();

    let mut i = 0usize;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if let Some(next) = advance_sql_quoted_index(bytes, i, q) {
                i = next;
                continue;
            }
            in_quote = None;
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }

        if depth == 0
            && i >= min_idx
            && upper
                .get(i..i.saturating_add(kw.len()))
                .is_some_and(|slice| slice == kw)
        {
            let before_ok = if i == 0 {
                true
            } else {
                !is_ident_char(upper[i - 1] as char)
            };
            let after = i + kw.len();
            let after_ok = if after >= upper.len() {
                true
            } else {
                !is_ident_char(upper[after] as char)
            };

            if before_ok && after_ok {
                return Some(i);
            }
        }

        i += 1;
    }

    None
}

fn starts_with_keyword_at(sql: &str, idx: usize, keyword: &str) -> bool {
    let bytes = sql.as_bytes();
    let kw = keyword.as_bytes();
    if idx + kw.len() > bytes.len() {
        return false;
    }
    if !bytes[idx..idx + kw.len()]
        .iter()
        .zip(kw)
        .all(|(left, right)| left.eq_ignore_ascii_case(right))
    {
        return false;
    }

    let before_ok = if idx == 0 {
        true
    } else {
        !is_ident_char(bytes[idx - 1] as char)
    };
    let after = idx + kw.len();
    let after_ok = if after >= bytes.len() {
        true
    } else {
        !is_ident_char(bytes[after] as char)
    };

    before_ok && after_ok
}

fn skip_sql_ws(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ref_columns(refs: &[(SqlStmtKind, String, Vec<String>)], table: &str) -> Vec<String> {
        refs.iter()
            .find(|(_, ref_table, _)| ref_table == table)
            .map(|(_, _, columns)| columns.clone())
            .unwrap_or_else(|| panic!("missing {table} reference in {refs:?}"))
    }

    fn has_table_ref(refs: &[(SqlStmtKind, String, Vec<String>)], table: &str) -> bool {
        refs.iter().any(|(_, ref_table, _)| ref_table == table)
    }

    #[test]
    fn test_parse_sql_reference_select() {
        let sql = "SELECT name, email FROM users WHERE id = $1";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["name", "email", "id"]);
    }

    #[test]
    fn test_parse_sql_reference_select_numeric_projection_is_not_column() {
        let sql = "SELECT 1 FROM users WHERE active = true";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["active"]);
    }

    #[test]
    fn test_parse_sql_reference_quoted_schema_table() {
        let sql = r#"SELECT "id", "email" FROM "public"."users" WHERE "id" = $1"#;
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["id", "email"]);
    }

    #[test]
    fn test_parse_sql_reference_escaped_quoted_identifier() {
        let sql = r#"SELECT "weird""column" FROM "weird""users" WHERE "weird""column" = $1"#;
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "weird\"users");
        assert_eq!(cols, vec!["weird\"column"]);
    }

    #[test]
    fn test_parse_sql_reference_tracks_predicate_and_order_columns() {
        let sql = "SELECT id FROM users WHERE email = $1 ORDER BY created_at DESC";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["id", "email", "created_at"]);
    }

    #[test]
    fn test_parse_sql_reference_tracks_projection_expression_columns() {
        let sql =
            "SELECT COUNT(email) AS email_count, date_trunc('day', created_at) AS day FROM users";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "created_at"]);
    }

    #[test]
    fn test_parse_sql_reference_skips_projection_alias_without_as() {
        let sql = "SELECT lower(email) email_lower, id user_id FROM users";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "id"]);
    }

    #[test]
    fn test_parse_sql_reference_skips_schema_qualified_function_names() {
        let sql = "SELECT pg_catalog.lower(email), public.coalesce(name, email) FROM users WHERE pg_catalog.lower(users.status) = $1";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "name", "status"]);
    }

    #[test]
    fn test_parse_sql_reference_skips_cast_type_names() {
        let sql = "SELECT id::public.user_id, CAST(email AS public.email_text) FROM users WHERE status::public.status_name = $1";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["id", "email", "status"]);
    }

    #[test]
    fn test_parse_sql_reference_tracks_window_clause_columns() {
        let sql = "SELECT row_number() OVER w FROM users WINDOW w AS (PARTITION BY tenant_id ORDER BY created_at)";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["tenant_id", "created_at"]);
    }

    #[test]
    fn test_parse_sql_reference_skips_params_strings_and_keywords() {
        let sql = "SELECT id FROM users WHERE lower(users.email) = lower(:email) AND status = 'active' ORDER BY users.created_at DESC NULLS LAST";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["id", "email", "status", "created_at"]);
    }

    #[test]
    fn test_parse_sql_references_skip_expression_syntax_keywords() {
        let sql = r#"SELECT id FROM users WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '1 day' AND starts_at AT TIME ZONE 'UTC' > CURRENT_DATE AND lower(name COLLATE "C") LIKE $1 ESCAPE '\' ORDER BY EXTRACT(EPOCH FROM created_at)"#;
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(
            ref_columns(&refs, "users"),
            vec!["id", "created_at", "starts_at", "name"]
        );
    }

    #[test]
    fn test_parse_sql_references_tracks_columns_named_like_expression_syntax() {
        let sql = "SELECT time, interval FROM events WHERE zone = $1 AND escape = true";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(
            ref_columns(&refs, "events"),
            vec!["time", "interval", "zone", "escape"]
        );
    }

    #[test]
    fn test_parse_sql_reference_skips_comments_and_dollar_quoted_text() {
        let sql = r#"
            -- SELECT id FROM ghosts
            SELECT id
            FROM users
            WHERE note = $$SELECT secret FROM ghosts;$$
              AND status = 'active'
              /* AND deleted_at FROM block_users */
        "#;
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].1, "users");
        assert_eq!(refs[0].2, vec!["id", "note", "status"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_columns() {
        let sql = "INSERT INTO users (email, status) VALUES ($1, $2)";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "status"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_returning_columns() {
        let sql = "INSERT INTO users (email) VALUES ($1) RETURNING id, created_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "id", "created_at"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_returning_alias_is_not_column() {
        let sql = "INSERT INTO users (email) VALUES ($1) RETURNING id AS user_id";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "id"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_returning_alias_without_as_is_not_column() {
        let sql = "INSERT INTO users (email) VALUES ($1) RETURNING id user_id";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "id"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_alias_columns() {
        let sql = "INSERT INTO users AS u (email, status) VALUES ($1, $2)";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "status"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_alias_returning_columns() {
        let sql = "INSERT INTO users AS u (email) VALUES ($1) RETURNING u.id, u.created_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "id", "created_at"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_conflict_columns_without_keywords() {
        let sql = "INSERT INTO users (email) VALUES ($1) ON CONFLICT (email) DO UPDATE SET last_seen = EXCLUDED.last_seen WHERE users.active RETURNING id";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "last_seen", "active", "id"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_alias_conflict_columns() {
        let sql = "INSERT INTO users AS u (email) VALUES ($1) ON CONFLICT (email) DO UPDATE SET last_seen = EXCLUDED.last_seen WHERE u.active RETURNING u.id";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "last_seen", "active", "id"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_conflict_constraint_is_not_column() {
        let sql =
            "INSERT INTO users DEFAULT VALUES ON CONFLICT ON CONSTRAINT users_email_key DO NOTHING";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert!(cols.is_empty(), "{cols:?}");
    }

    #[test]
    fn test_parse_sql_reference_insert_values_is_not_alias() {
        let sql = "INSERT INTO users VALUES ($1, $2) RETURNING id";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["id"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_overriding_is_not_alias() {
        let sql = "INSERT INTO users OVERRIDING SYSTEM VALUE VALUES ($1, $2) RETURNING id";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["id"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_insert_select_source_table() {
        let sql = "INSERT INTO archived_orders (id, total) SELECT id, total FROM orders WHERE status = $1";
        let refs = parse_sql_references(sql);

        let orders = refs
            .iter()
            .find(|(_, table, _)| table == "orders")
            .expect("orders reference");
        assert_eq!(orders.2, vec!["id", "total", "status"]);
    }

    #[test]
    fn test_parse_sql_reference_update_columns() {
        let sql =
            "UPDATE users SET email = $1, status = 'active' WHERE id = $2 RETURNING updated_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Update);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "status", "id", "updated_at"]);
    }

    #[test]
    fn test_parse_sql_reference_update_returning_alias_is_not_column() {
        let sql = "UPDATE users SET email = $1 WHERE id = $2 RETURNING updated_at AS changed_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Update);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "id", "updated_at"]);
    }

    #[test]
    fn test_parse_sql_reference_update_returning_alias_without_as_is_not_column() {
        let sql = "UPDATE users SET email = $1 WHERE id = $2 RETURNING updated_at changed_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Update);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "id", "updated_at"]);
    }

    #[test]
    fn test_parse_sql_reference_update_returning_old_new_aliases_are_target_columns() {
        let default_alias_sql =
            "UPDATE users SET status = $1 WHERE id = $2 RETURNING old.email, new.updated_at";
        let explicit_alias_sql = "UPDATE users SET status = $1 WHERE id = $2 RETURNING WITH (OLD AS o, NEW AS n) o.email AS old_email, n.updated_at AS new_updated_at";

        let (_, _, default_cols) = parse_sql_reference(default_alias_sql).expect("sql parse");
        let (_, _, explicit_cols) = parse_sql_reference(explicit_alias_sql).expect("sql parse");

        assert_eq!(default_cols, vec!["status", "id", "email", "updated_at"]);
        assert_eq!(explicit_cols, vec!["status", "id", "email", "updated_at"]);
    }

    #[test]
    fn test_parse_sql_reference_update_only_table() {
        let sql = "UPDATE ONLY users SET email = $1 WHERE id = $2";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Update);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "id"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_update_from_source_table() {
        let sql = "UPDATE orders o SET status = p.status FROM payments p WHERE o.payment_id = p.id AND p.state = $1";
        let refs = parse_sql_references(sql);

        let orders = refs
            .iter()
            .find(|(_, table, _)| table == "orders")
            .expect("orders reference");
        assert_eq!(orders.2, vec!["status", "payment_id"]);

        let payments = refs
            .iter()
            .find(|(_, table, _)| table == "payments")
            .expect("payments reference");
        assert_eq!(payments.2, vec!["status", "id", "state"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_update_from_unqualified_source_columns() {
        let sql =
            "UPDATE orders SET status = state FROM payments WHERE orders.payment_id = payments.id";
        let refs = parse_sql_references(sql);

        let payments = refs
            .iter()
            .find(|(_, table, _)| table == "payments")
            .expect("payments reference");
        assert_eq!(payments.2, vec!["id", "state"]);
    }

    #[test]
    fn test_parse_sql_reference_delete_columns() {
        let sql = "DELETE FROM users WHERE email = $1 RETURNING deleted_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Delete);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "deleted_at"]);
    }

    #[test]
    fn test_parse_sql_reference_delete_returning_alias_is_not_column() {
        let sql = "DELETE FROM users WHERE email = $1 RETURNING deleted_at AS removed_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Delete);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "deleted_at"]);
    }

    #[test]
    fn test_parse_sql_reference_delete_returning_alias_without_as_is_not_column() {
        let sql = "DELETE FROM users WHERE email = $1 RETURNING deleted_at removed_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Delete);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "deleted_at"]);
    }

    #[test]
    fn test_parse_sql_reference_delete_only_table() {
        let sql = "DELETE FROM ONLY users WHERE email = $1";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Delete);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_delete_using_source_table() {
        let sql =
            "DELETE FROM sessions s USING users u WHERE s.user_id = u.id AND u.disabled = true";
        let refs = parse_sql_references(sql);

        let sessions = refs
            .iter()
            .find(|(_, table, _)| table == "sessions")
            .expect("sessions reference");
        assert_eq!(sessions.2, vec!["user_id"]);

        let users = refs
            .iter()
            .find(|(_, table, _)| table == "users")
            .expect("users reference");
        assert_eq!(users.2, vec!["id", "disabled"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_delete_using_unqualified_source_columns() {
        let sql =
            "DELETE FROM sessions USING users WHERE sessions.user_id = id AND disabled = true";
        let refs = parse_sql_references(sql);

        let users = refs
            .iter()
            .find(|(_, table, _)| table == "users")
            .expect("users reference");
        assert_eq!(users.2, vec!["id", "disabled"]);
    }

    #[test]
    fn test_parse_sql_reference_merge() {
        let sql = "MERGE INTO orders USING staging_orders ON orders.id = staging_orders.id WHEN MATCHED THEN UPDATE SET status = staging_orders.status";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind.as_str(), "MERGE");
        assert_eq!(table, "orders");
        assert_eq!(cols, vec!["id", "status"]);
    }

    #[test]
    fn test_parse_sql_reference_merge_returning_alias_columns() {
        let sql = "MERGE INTO orders o USING staging_orders s ON o.id = s.id WHEN MATCHED THEN UPDATE SET status = s.status RETURNING created_at AS merged_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind.as_str(), "MERGE");
        assert_eq!(table, "orders");
        assert_eq!(cols, vec!["id", "status", "created_at"]);
    }

    #[test]
    fn test_parse_sql_reference_merge_returning_alias_without_as_columns() {
        let sql = "MERGE INTO orders o USING staging_orders s ON o.id = s.id WHEN MATCHED THEN UPDATE SET status = s.status RETURNING created_at merged_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind.as_str(), "MERGE");
        assert_eq!(table, "orders");
        assert_eq!(cols, vec!["id", "status", "created_at"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_merge_source_table() {
        let sql = "MERGE INTO orders o USING staging_orders s ON o.id = s.id WHEN MATCHED THEN UPDATE SET status = s.status WHEN NOT MATCHED THEN INSERT (id, status) VALUES (s.id, s.status)";
        let refs = parse_sql_references(sql);

        let staging_orders = refs
            .iter()
            .find(|(_, table, _)| table == "staging_orders")
            .expect("staging orders reference");
        assert_eq!(staging_orders.2, vec!["id", "status"]);
    }

    #[test]
    fn test_parse_sql_reference_truncate_table() {
        let sql = "TRUNCATE TABLE ONLY users";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");

        assert_eq!(kind, SqlStmtKind::Truncate);
        assert_eq!(table, "users");
        assert!(cols.is_empty(), "{cols:?}");
    }

    #[test]
    fn test_parse_sql_references_track_multi_table_truncate() {
        let sql = "TRUNCATE TABLE users, orders CASCADE";
        let refs = parse_sql_references(sql);

        assert!(
            refs.iter()
                .any(|(kind, table, _)| *kind == SqlStmtKind::Truncate && table == "users"),
            "{refs:?}"
        );
        assert!(
            refs.iter()
                .any(|(kind, table, _)| *kind == SqlStmtKind::Truncate && table == "orders"),
            "{refs:?}"
        );
    }

    #[test]
    fn test_parse_sql_reference_copy_table_columns() {
        let sql = "COPY users (email, status) FROM STDIN";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");

        assert_eq!(kind, SqlStmtKind::Copy);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "status"]);
    }

    #[test]
    fn test_parse_sql_reference_lock_table() {
        let sql = "LOCK TABLE users IN ACCESS EXCLUSIVE MODE";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");

        assert_eq!(kind, SqlStmtKind::Lock);
        assert_eq!(table, "users");
        assert!(cols.is_empty(), "{cols:?}");
    }

    #[test]
    fn test_parse_sql_references_track_multi_table_lock() {
        let refs = parse_sql_references("LOCK TABLE users, orders IN SHARE MODE");

        assert!(has_table_ref(&refs, "users"), "{refs:?}");
        assert!(has_table_ref(&refs, "orders"), "{refs:?}");
    }

    #[test]
    fn test_parse_sql_reference_copy_where_columns() {
        let sql = "COPY users (email) FROM STDIN WHERE active = true AND tenant_id = $1";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");

        assert_eq!(kind, SqlStmtKind::Copy);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "active", "tenant_id"]);
    }

    #[test]
    fn test_parse_sql_references_track_create_index_columns() {
        let sql = "CREATE INDEX users_email_idx ON users (email, created_at DESC)";
        let refs = parse_sql_references(sql);

        assert_eq!(ref_columns(&refs, "users"), vec!["email", "created_at"]);
    }

    #[test]
    fn test_parse_sql_references_track_create_index_expression_columns() {
        let sql = "CREATE INDEX users_lower_email_idx ON users ((lower(email)))";
        let refs = parse_sql_references(sql);

        assert_eq!(ref_columns(&refs, "users"), vec!["email"]);
    }

    #[test]
    fn test_parse_sql_references_track_create_index_include_and_predicate_columns() {
        let sql = "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS users_active_idx ON ONLY users USING btree (email) INCLUDE (created_at) WHERE active = true AND deleted_at IS NULL";
        let refs = parse_sql_references(sql);

        assert_eq!(
            ref_columns(&refs, "users"),
            vec!["email", "created_at", "active", "deleted_at"]
        );
    }

    #[test]
    fn test_parse_sql_references_create_index_skips_operator_class_names() {
        let opclass_refs = parse_sql_references(
            "CREATE INDEX users_payload_idx ON users USING gin (payload jsonb_path_ops)",
        );
        let expression_refs = parse_sql_references(
            "CREATE INDEX users_email_idx ON users (lower(email) text_pattern_ops, created_at DESC NULLS LAST)",
        );

        assert_eq!(ref_columns(&opclass_refs, "users"), vec!["payload"]);
        assert_eq!(
            ref_columns(&expression_refs, "users"),
            vec!["email", "created_at"]
        );
    }

    #[test]
    fn test_parse_sql_references_track_create_view_source_query() {
        let sql = "CREATE OR REPLACE VIEW active_users AS SELECT id, email FROM users WHERE active = true";
        let refs = parse_sql_references(sql);

        assert_eq!(ref_columns(&refs, "users"), vec!["id", "email", "active"]);
    }

    #[test]
    fn test_parse_sql_references_track_create_materialized_view_source_query() {
        let sql = "CREATE MATERIALIZED VIEW order_totals AS SELECT user_id, total FROM orders WHERE status = 'paid'";
        let refs = parse_sql_references(sql);

        assert_eq!(
            ref_columns(&refs, "orders"),
            vec!["user_id", "total", "status"]
        );
    }

    #[test]
    fn test_parse_sql_references_track_create_table_as_source_query() {
        let sql = "CREATE TABLE archived_orders AS SELECT id, total FROM orders WHERE status = 'archived'";
        let refs = parse_sql_references(sql);

        assert_eq!(ref_columns(&refs, "orders"), vec!["id", "total", "status"]);
    }

    #[test]
    fn test_parse_sql_references_track_create_policy_columns() {
        let sql = "CREATE POLICY tenant_users ON users USING (tenant_id = current_setting('app.tenant_id')::uuid) WITH CHECK (tenant_id = current_setting('app.tenant_id')::uuid AND active = true)";
        let refs = parse_sql_references(sql);

        assert_eq!(ref_columns(&refs, "users"), vec!["tenant_id", "active"]);
    }

    #[test]
    fn test_parse_sql_references_track_create_trigger_columns() {
        let sql = "CREATE TRIGGER order_status_changed BEFORE UPDATE OF status, total ON orders FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status AND NEW.total > 0) EXECUTE FUNCTION audit_order()";
        let refs = parse_sql_references(sql);

        assert_eq!(ref_columns(&refs, "orders"), vec!["status", "total"]);
    }

    #[test]
    fn test_parse_sql_references_track_create_rule_target_and_action() {
        let sql = "CREATE RULE active_user_update AS ON UPDATE TO active_users WHERE old.active DO ALSO UPDATE users SET email = new.email WHERE users.id = old.id";
        let refs = parse_sql_references(sql);

        assert_eq!(ref_columns(&refs, "active_users"), vec!["active"]);
        assert_eq!(ref_columns(&refs, "users"), vec!["email", "id"]);
    }

    #[test]
    fn test_parse_sql_references_track_create_publication_tables() {
        let sql = "CREATE PUBLICATION tenant_pub FOR TABLE users (email, status) WHERE (active), orders WHERE (total > 0 AND status = 'paid')";
        let refs = parse_sql_references(sql);

        assert_eq!(
            ref_columns(&refs, "users"),
            vec!["email", "status", "active"]
        );
        assert_eq!(ref_columns(&refs, "orders"), vec!["total", "status"]);
    }

    #[test]
    fn test_parse_sql_references_track_create_statistics_columns() {
        let sql = "CREATE STATISTICS users_stats ON lower(email), status, tenant_id FROM users";
        let refs = parse_sql_references(sql);

        assert_eq!(
            ref_columns(&refs, "users"),
            vec!["email", "status", "tenant_id"]
        );
    }

    #[test]
    fn test_parse_sql_references_track_create_table_like_and_inline_references() {
        let sql = "CREATE TABLE invoices (LIKE invoice_template INCLUDING ALL, org_id uuid REFERENCES orgs(id), user_id uuid REFERENCES users(id))";
        let refs = parse_sql_references(sql);
        let check_refs =
            parse_sql_references("CREATE TABLE users (name text CHECK (name LIKE pattern))");

        assert!(has_table_ref(&refs, "invoice_template"), "{refs:?}");
        assert_eq!(ref_columns(&refs, "orgs"), vec!["id"]);
        assert_eq!(ref_columns(&refs, "users"), vec!["id"]);
        assert!(check_refs.is_empty(), "{check_refs:?}");
    }

    #[test]
    fn test_parse_sql_references_track_alter_table_column_operations() {
        let sql = "ALTER TABLE users DROP COLUMN old_email, RENAME COLUMN legacy_name TO name, ALTER COLUMN status TYPE text USING status::text";
        let refs = parse_sql_references(sql);

        assert_eq!(
            ref_columns(&refs, "users"),
            vec!["old_email", "legacy_name", "status"]
        );
    }

    #[test]
    fn test_parse_sql_references_track_alter_table_constraints() {
        let sql = "ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email), ADD CONSTRAINT users_active_check CHECK (active OR status = 'pending')";
        let refs = parse_sql_references(sql);

        assert_eq!(
            ref_columns(&refs, "users"),
            vec!["email", "active", "status"]
        );
    }

    #[test]
    fn test_parse_sql_references_track_alter_table_foreign_key_references() {
        let sql = "ALTER TABLE users ADD CONSTRAINT users_org_fk FOREIGN KEY (org_id) REFERENCES orgs(id)";
        let refs = parse_sql_references(sql);

        assert_eq!(ref_columns(&refs, "users"), vec!["org_id"]);
        assert_eq!(ref_columns(&refs, "orgs"), vec!["id"]);
    }

    #[test]
    fn test_parse_sql_references_track_alter_policy_columns() {
        let sql = "ALTER POLICY tenant_users ON users USING (tenant_id = current_setting('app.tenant_id')::uuid) WITH CHECK (tenant_id = current_setting('app.tenant_id')::uuid AND active = true)";
        let refs = parse_sql_references(sql);

        assert_eq!(ref_columns(&refs, "users"), vec!["tenant_id", "active"]);
    }

    #[test]
    fn test_parse_sql_references_track_alter_publication_tables() {
        let sql = "ALTER PUBLICATION tenant_pub SET TABLE users (email) WHERE (active), orders WHERE (status = 'paid')";
        let refs = parse_sql_references(sql);

        assert_eq!(ref_columns(&refs, "users"), vec!["email", "active"]);
        assert_eq!(ref_columns(&refs, "orders"), vec!["status"]);
    }

    #[test]
    fn test_parse_sql_references_track_alter_view_and_trigger_targets() {
        let view_refs = parse_sql_references("ALTER VIEW active_users RENAME TO users_active");
        let mat_view_refs =
            parse_sql_references("ALTER MATERIALIZED VIEW active_orders SET SCHEMA reporting");
        let trigger_refs =
            parse_sql_references("ALTER TRIGGER sync_users ON users RENAME TO sync_users_v2");

        assert!(has_table_ref(&view_refs, "active_users"), "{view_refs:?}");
        assert!(
            has_table_ref(&mat_view_refs, "active_orders"),
            "{mat_view_refs:?}"
        );
        assert!(has_table_ref(&trigger_refs, "users"), "{trigger_refs:?}");
    }

    #[test]
    fn test_parse_sql_references_track_drop_table_like_objects() {
        let table_refs = parse_sql_references("DROP TABLE IF EXISTS users, orders CASCADE");
        let view_refs = parse_sql_references("DROP VIEW IF EXISTS active_users, stale_users");
        let mat_view_refs =
            parse_sql_references("DROP MATERIALIZED VIEW IF EXISTS active_orders RESTRICT");
        let foreign_table_refs = parse_sql_references("DROP FOREIGN TABLE IF EXISTS remote_users");

        assert!(has_table_ref(&table_refs, "users"), "{table_refs:?}");
        assert!(has_table_ref(&table_refs, "orders"), "{table_refs:?}");
        assert!(has_table_ref(&view_refs, "active_users"), "{view_refs:?}");
        assert!(has_table_ref(&view_refs, "stale_users"), "{view_refs:?}");
        assert!(
            has_table_ref(&mat_view_refs, "active_orders"),
            "{mat_view_refs:?}"
        );
        assert!(
            has_table_ref(&foreign_table_refs, "remote_users"),
            "{foreign_table_refs:?}"
        );
    }

    #[test]
    fn test_parse_sql_references_track_drop_policy_trigger_rule_targets() {
        let policy_refs = parse_sql_references("DROP POLICY IF EXISTS tenant_users ON users");
        let trigger_refs = parse_sql_references("DROP TRIGGER IF EXISTS sync_users ON users");
        let rule_refs = parse_sql_references("DROP RULE IF EXISTS active_users_update ON users");

        assert!(has_table_ref(&policy_refs, "users"), "{policy_refs:?}");
        assert!(has_table_ref(&trigger_refs, "users"), "{trigger_refs:?}");
        assert!(has_table_ref(&rule_refs, "users"), "{rule_refs:?}");
    }

    #[test]
    fn test_parse_sql_references_track_comment_targets() {
        let column_refs = parse_sql_references("COMMENT ON COLUMN users.email IS 'legacy email'");
        let table_refs = parse_sql_references("COMMENT ON TABLE users IS 'tenant scoped'");

        assert_eq!(ref_columns(&column_refs, "users"), vec!["email"]);
        assert!(has_table_ref(&table_refs, "users"), "{table_refs:?}");
    }

    #[test]
    fn test_parse_sql_references_track_grant_and_revoke_columns() {
        let grant_refs = parse_sql_references(
            "GRANT SELECT (email), UPDATE (status) ON TABLE users TO app_role",
        );
        let revoke_refs = parse_sql_references("REVOKE UPDATE (status) ON users FROM app_role");

        assert_eq!(ref_columns(&grant_refs, "users"), vec!["email", "status"]);
        assert_eq!(ref_columns(&revoke_refs, "users"), vec!["status"]);
    }

    #[test]
    fn test_parse_sql_references_skip_non_table_privilege_targets() {
        let schema_refs = parse_sql_references("GRANT USAGE ON SCHEMA public TO app_role");
        let all_tables_refs =
            parse_sql_references("GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_role");
        let sequence_refs =
            parse_sql_references("REVOKE USAGE ON SEQUENCE users_id_seq FROM app_role");

        assert!(schema_refs.is_empty(), "{schema_refs:?}");
        assert!(all_tables_refs.is_empty(), "{all_tables_refs:?}");
        assert!(sequence_refs.is_empty(), "{sequence_refs:?}");
    }

    #[test]
    fn test_parse_sql_references_track_analyze_and_vacuum_columns() {
        let analyze_refs = parse_sql_references("ANALYZE VERBOSE users (email, status)");
        let vacuum_refs = parse_sql_references("VACUUM (VERBOSE, ANALYZE) users (deleted_at)");

        assert_eq!(ref_columns(&analyze_refs, "users"), vec!["email", "status"]);
        assert_eq!(ref_columns(&vacuum_refs, "users"), vec!["deleted_at"]);
    }

    #[test]
    fn test_parse_sql_references_track_reindex_cluster_and_refresh_tables() {
        let reindex_refs = parse_sql_references("REINDEX TABLE CONCURRENTLY users");
        let cluster_refs = parse_sql_references("CLUSTER VERBOSE users USING users_email_idx");
        let refresh_refs =
            parse_sql_references("REFRESH MATERIALIZED VIEW CONCURRENTLY active_users");

        assert!(has_table_ref(&reindex_refs, "users"), "{reindex_refs:?}");
        assert!(has_table_ref(&cluster_refs, "users"), "{cluster_refs:?}");
        assert!(
            has_table_ref(&refresh_refs, "active_users"),
            "{refresh_refs:?}"
        );
    }

    #[test]
    fn test_parse_sql_references_skip_set_returning_function_sources() {
        let refs = parse_sql_references("SELECT value FROM unnest($1::int[]) AS value");

        assert!(refs.is_empty(), "{refs:?}");
    }

    #[test]
    fn test_parse_sql_references_tracks_cte_base_table() {
        let sql = "WITH active_users AS (SELECT id, email FROM users WHERE status = $1) SELECT id FROM active_users";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        let (kind, table, cols) = &refs[0];
        assert_eq!(kind.as_str(), "SELECT");
        assert_eq!(table, "users");
        assert_eq!(cols, &vec!["id", "email", "status"]);
    }

    #[test]
    fn test_parse_sql_references_supports_materialized_ctes() {
        let sql = "WITH active_users AS NOT MATERIALIZED (SELECT id, email FROM users WHERE status = $1) SELECT id FROM ACTIVE_USERS";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].1, "users");
        assert_eq!(refs[0].2, vec!["id", "email", "status"]);
    }

    #[test]
    fn test_parse_sql_references_skips_intermediate_cte_aliases() {
        let sql = "WITH raw_users AS (SELECT id, email FROM users), active_users AS (SELECT id FROM raw_users WHERE email IS NOT NULL) SELECT id FROM active_users";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].1, "users");
        assert_eq!(refs[0].2, vec!["id", "email"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_joined_table_columns() {
        let sql = "SELECT u.id, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE o.status = $1 ORDER BY o.created_at DESC";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 2, "{refs:?}");

        let users = refs
            .iter()
            .find(|(_, table, _)| table == "users")
            .expect("users reference");
        assert_eq!(users.2, vec!["id"]);

        let orders = refs
            .iter()
            .find(|(_, table, _)| table == "orders")
            .expect("orders reference");
        assert_eq!(orders.2, vec!["total", "user_id", "status", "created_at"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_select_star_columns() {
        let sql = "SELECT * FROM users WHERE active = true";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(ref_columns(&refs, "users"), vec!["*", "active"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_all_star_columns() {
        let sql = "SELECT ALL * FROM users WHERE active = true";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(ref_columns(&refs, "users"), vec!["*", "active"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_distinct_on_star_columns() {
        let sql = "SELECT DISTINCT ON (tenant_id) * FROM users WHERE active = true";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(
            ref_columns(&refs, "users"),
            vec!["tenant_id", "*", "active"]
        );
    }

    #[test]
    fn test_parse_sql_references_tracks_join_select_star_columns() {
        let sql = "SELECT * FROM users u JOIN orders o ON o.user_id = u.id WHERE o.status = $1";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 2, "{refs:?}");
        assert_eq!(ref_columns(&refs, "users"), vec!["id", "*"]);
        assert_eq!(ref_columns(&refs, "orders"), vec!["user_id", "status", "*"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_distinct_on_and_filter_columns() {
        let sql = "SELECT DISTINCT ON (tenant_id) id, COUNT(*) FILTER (WHERE active) AS active_count FROM users WHERE status = $1 ORDER BY tenant_id, created_at DESC";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(
            ref_columns(&refs, "users"),
            vec!["tenant_id", "id", "active", "status", "created_at"]
        );
    }

    #[test]
    fn test_parse_sql_references_tracks_grouping_sets_without_syntax_keywords() {
        let sql = "SELECT tenant_id, status, count(*) FROM orders GROUP BY GROUPING SETS ((tenant_id, status), (tenant_id), ()) HAVING count(*) > 0";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(ref_columns(&refs, "orders"), vec!["tenant_id", "status"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_rollup_cube_group_columns() {
        let sql = "SELECT region, product, sum(total) FROM orders GROUP BY ROLLUP (region, product), CUBE (channel, status)";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(
            ref_columns(&refs, "orders"),
            vec!["region", "product", "total", "channel", "status"]
        );
    }

    #[test]
    fn test_parse_sql_references_tracks_grouping_function_columns_without_keywords() {
        let sql = "SELECT GROUPING(tenant_id), tenant_id FROM users GROUP BY GROUPING SETS ((tenant_id), ())";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(ref_columns(&refs, "users"), vec!["tenant_id"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_tablesample_columns_without_fake_source() {
        let sql =
            "SELECT id FROM users TABLESAMPLE BERNOULLI(10) REPEATABLE (42) WHERE active = true";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(ref_columns(&refs, "users"), vec!["id", "active"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_tablesample_alias_columns() {
        let sql = "SELECT u.id FROM users TABLESAMPLE SYSTEM (25) AS u WHERE u.active = true";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(ref_columns(&refs, "users"), vec!["id", "active"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_alias_before_tablesample_columns() {
        let sql = "SELECT u.id FROM users u TABLESAMPLE SYSTEM (25) WHERE u.active = true";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(ref_columns(&refs, "users"), vec!["id", "active"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_tablesample_join_columns() {
        let sql = "SELECT u.id, o.total FROM users TABLESAMPLE SYSTEM (25) u JOIN orders TABLESAMPLE BERNOULLI(10) o ON o.user_id = u.id WHERE o.status = $1";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 2, "{refs:?}");
        assert_eq!(ref_columns(&refs, "users"), vec!["id"]);
        assert_eq!(
            ref_columns(&refs, "orders"),
            vec!["total", "user_id", "status"]
        );
    }

    #[test]
    fn test_parse_sql_references_tracks_only_inheritance_star_alias_columns() {
        let sql = "SELECT u.id FROM ONLY users * AS u WHERE u.active = true";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(ref_columns(&refs, "users"), vec!["id", "active"]);
    }

    #[test]
    fn test_parse_sql_references_skips_rows_from_table_function_source() {
        let sql = "SELECT u.id FROM ROWS FROM (jsonb_to_recordset($1) AS (id int)) AS r(id) JOIN users u ON u.id = r.id WHERE u.active = true";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert!(!has_table_ref(&refs, "ROWS"), "{refs:?}");
        assert_eq!(ref_columns(&refs, "users"), vec!["id", "active"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_set_operation_rhs_table() {
        let sql = "SELECT id FROM users WHERE active = true UNION ALL SELECT user_id FROM orders WHERE status = $1";
        let refs = parse_sql_references(sql);

        let orders = refs
            .iter()
            .find(|(_, table, _)| table == "orders")
            .expect("orders reference");
        assert_eq!(orders.2, vec!["user_id", "status"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_nested_subquery_table() {
        let sql = "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > $1)";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 2, "{refs:?}");

        let users = refs
            .iter()
            .find(|(_, table, _)| table == "users")
            .expect("users reference");
        assert_eq!(users.2, vec!["id"]);

        let orders = refs
            .iter()
            .find(|(_, table, _)| table == "orders")
            .expect("orders reference");
        assert_eq!(orders.2, vec!["user_id", "total"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_derived_table_and_nested_subquery_sources() {
        let sql = "SELECT s.id FROM (SELECT id FROM users WHERE status = 'active') s WHERE s.id IN (SELECT user_id FROM orders WHERE total > $1)";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 2, "{refs:?}");

        let users = refs
            .iter()
            .find(|(_, table, _)| table == "users")
            .expect("users reference");
        assert_eq!(users.2, vec!["id", "status"]);

        let orders = refs
            .iter()
            .find(|(_, table, _)| table == "orders")
            .expect("orders reference");
        assert_eq!(orders.2, vec!["user_id", "total"]);
    }
}
