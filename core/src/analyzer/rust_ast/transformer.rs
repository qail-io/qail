//! SQL to QAIL transformation using parser-backed semantic text scanning.
//!
//! This transformer intentionally avoids regex-heavy parsing and instead uses
//! top-level SQL clause parsing (quote/parenthesis aware) to reduce false
//! positives and keep output stable for code actions.

use super::utils::to_pascal_case;

/// Transform SQL string to QAIL builder code.
pub fn sql_to_qail(sql: &str) -> Result<String, String> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err("Empty SQL".to_string());
    }

    if starts_with_keyword(sql, "EXPLAIN") {
        return Ok(transform_explain(sql));
    }
    if starts_with_keyword(sql, "WITH") {
        return Ok(transform_cte_select(sql));
    }
    if starts_with_keyword(sql, "SELECT") {
        return Ok(transform_select(sql));
    }
    if starts_with_keyword(sql, "INSERT") {
        return Ok(transform_insert(sql));
    }
    if starts_with_keyword(sql, "UPDATE") {
        return Ok(transform_update(sql));
    }
    if starts_with_keyword(sql, "DELETE") {
        return Ok(transform_delete(sql));
    }
    if starts_with_keyword(sql, "CREATE TABLE") {
        return Ok(transform_create_table(sql));
    }
    if starts_with_keyword(sql, "DROP") {
        return Ok(transform_drop(sql));
    }
    if starts_with_keyword(sql, "TRUNCATE") {
        return Ok(transform_truncate(sql));
    }

    Ok("// SQL statement type not yet mapped to QAIL".to_string())
}

fn starts_with_keyword(sql: &str, keyword: &str) -> bool {
    let sql = sql.trim_start();
    let upper = sql.to_ascii_uppercase();
    upper.starts_with(&keyword.to_ascii_uppercase())
}

fn transform_cte_select(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let (cte_name, inner_sql) = parse_first_cte(&normalized)
        .unwrap_or_else(|| ("cte".to_string(), "SELECT * FROM table".to_string()));

    let source_table =
        extract_table_name_from_select(&inner_sql).unwrap_or_else(|| "table".to_string());

    format!(
        "use qail_core::ast::{{Qail, Operator, Order}};\n\n
         // CTE '{}': define as separate query and pass it to .with(\"{}\", query)\n
         let {}_cte = Qail::get(\"{}\")
             .columns([\"*\"]);\n\n
         // Then reference CTE in main query using the alias\n\n
         let cmd = Qail::get(\"{}\")
             .columns([\"*\"]);\n\n
         // Execute with qail-pg driver:\n
         let rows: Vec<{}Row> = driver.query_as(&cmd).await?;",
        cte_name,
        cte_name,
        cte_name,
        source_table,
        cte_name,
        to_pascal_case(&cte_name)
    )
}

fn transform_select(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let parsed = parse_select(&normalized);

    let table = parsed.table;
    let columns = parsed.columns;
    let where_clause = parsed.where_clause;
    let order_by = parsed.order_by;
    let limit = parsed.limit;

    let mut result = String::new();
    result.push_str("use qail_core::ast::{Qail, Operator, Order};\n\n");
    result.push_str(&format!("let cmd = Qail::get(\"{}\")\n", table));
    result.push_str(&format!("    .columns([{}])", columns.join(", ")));

    if let Some(filter) = where_clause {
        for f in filter {
            result.push_str("\n    ");
            result.push_str(&f);
        }
    }

    if let Some((col, desc)) = order_by {
        let dir = if desc { "Desc" } else { "Asc" };
        result.push_str(&format!("\n    .order_by(\"{}\", Order::{})", col, dir));
    }

    if let Some(limit) = limit {
        result.push_str(&format!("\n    .limit({})", limit));
    }

    result.push_str(";\n\n");
    result.push_str("// Execute with qail-pg driver:\n");
    result.push_str(&format!(
        "let rows: Vec<{}Row> = driver.query_as(&cmd).await?;",
        to_pascal_case(&table)
    ));

    result
}

fn transform_insert(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let (table, columns) = parse_insert_table_columns(&normalized)
        .unwrap_or_else(|| ("table".to_string(), Vec::new()));

    let mut set_values = String::new();
    for col in &columns {
        set_values.push_str(&format!("    .set_value(\"{}\", {}_value)\n", col, col));
    }
    if set_values.is_empty() {
        set_values = "    // Add .set_value(col, val) for each column\n".to_string();
    }

    format!(
        "use qail_core::ast::Qail;\n\n
         let cmd = Qail::add(\"{}\")\n{};\n\n
         let result = driver.execute(&cmd).await?;",
        table,
        set_values.trim_end()
    )
}

fn transform_update(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let (table, assignments) = parse_update_set_assignments(&normalized)
        .unwrap_or_else(|| ("table".to_string(), Vec::new()));

    let mut set_values = String::new();
    for (col, val) in assignments {
        set_values.push_str(&format!(
            "    .set_value(\"{}\", {})\n",
            col,
            rewrite_param_literal(&val)
        ));
    }
    if set_values.is_empty() {
        set_values = "    // Add .set_value(col, val) for each column\n".to_string();
    }

    format!(
        "use qail_core::ast::{{Qail, Operator}};\n\n
         let cmd = Qail::set(\"{}\")\n{}    .filter(\"id\", Operator::Eq, id);\n\n
         let result = driver.execute(&cmd).await?;",
        table, set_values
    )
}

fn transform_delete(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let table = parse_delete_table(&normalized).unwrap_or_else(|| "table".to_string());

    format!(
        "use qail_core::ast::{{Qail, Operator}};\n\n
         let cmd = Qail::del(\"{}\")\n    .filter(\"id\", Operator::Eq, id);\n\n
         let result = driver.execute(&cmd).await?;",
        table
    )
}

fn transform_create_table(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let table = parse_create_table_name(&normalized).unwrap_or_else(|| "table".to_string());

    format!(
        "use qail_core::ast::Qail;\n\n
         let cmd = Qail::make(\"{}\")\n    // Add column definitions with .column_def(name, type, constraints)\n;\n\n
         let result = driver.execute(&cmd).await?;",
        table
    )
}

fn transform_drop(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);

    if let Some((kind, name)) = parse_drop_object(&normalized) {
        if kind == "INDEX" {
            return format!(
                "use qail_core::ast::{{Qail, Action}};\n\n
                 let cmd = Qail {{ action: Action::DropIndex, table: \"{}\".into(), ..Default::default() }};\n\n
                 let result = driver.execute(&cmd).await?;",
                name
            );
        }

        return format!(
            "use qail_core::ast::{{Qail, Action}};\n\n
             let cmd = Qail {{ action: Action::Drop, table: \"{}\".into(), ..Default::default() }};\n\n
             let result = driver.execute(&cmd).await?;",
            name
        );
    }

    "// DROP object not yet mapped to QAIL".to_string()
}

fn transform_truncate(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let table = parse_truncate_table(&normalized).unwrap_or_else(|| "table".to_string());

    format!(
        "use qail_core::ast::Qail;\n\n
         let cmd = Qail::truncate(\"{}\");\n\n
         let result = driver.execute(&cmd).await?;",
        table
    )
}

fn transform_explain(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let mut rest = normalized.trim_start();

    if starts_with_keyword(rest, "EXPLAIN") {
        rest = rest["EXPLAIN".len()..].trim_start();
    }

    let mut analyze = false;
    if starts_with_keyword(rest, "ANALYZE") {
        analyze = true;
        rest = rest["ANALYZE".len()..].trim_start();
    }

    let inner = sql_to_qail(rest).unwrap_or_else(|_| "// Could not parse SQL".to_string());

    if analyze {
        format!(
            "// EXPLAIN ANALYZE wrapper:\n
             // Use Qail::explain_analyze(table) instead of Qail::get(table)\n\n{}",
            inner
        )
    } else {
        format!(
            "// EXPLAIN wrapper:\n
             // Use Qail::explain(table) instead of Qail::get(table)\n\n{}",
            inner
        )
    }
}

#[derive(Debug)]
struct ParsedSelect {
    table: String,
    columns: Vec<String>,
    where_clause: Option<Vec<String>>,
    order_by: Option<(String, bool)>,
    limit: Option<u64>,
}

fn parse_select(sql: &str) -> ParsedSelect {
    ParsedSelect {
        table: extract_table_name_from_select(sql).unwrap_or_else(|| "table".to_string()),
        columns: extract_columns_from_select(sql),
        where_clause: extract_where_clause(sql),
        order_by: extract_order_by(sql),
        limit: extract_limit(sql),
    }
}

fn normalize_whitespace(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn parse_first_cte(sql: &str) -> Option<(String, String)> {
    if !starts_with_keyword(sql, "WITH") {
        return None;
    }

    let mut cursor = skip_ws(sql, "WITH".len());
    let (cte_name, next) = parse_sql_object_name(sql, cursor)?;
    cursor = skip_ws(sql, next);

    let as_idx = find_keyword_top_level_from(sql, "AS", cursor)?;
    let open_idx = skip_ws(sql, as_idx + "AS".len());
    let close_idx = find_matching_paren(sql, open_idx)?;

    let inner_sql = sql.get(open_idx + 1..close_idx)?.trim().to_string();
    Some((cte_name, inner_sql))
}

fn extract_table_name_from_select(sql: &str) -> Option<String> {
    let from_idx = find_keyword_top_level(sql, "FROM")?;
    let start = from_idx + "FROM".len();
    let (table, _) = parse_sql_object_name(sql, start)?;
    Some(table)
}

fn extract_columns_from_select(sql: &str) -> Vec<String> {
    let select_idx = find_keyword_top_level(sql, "SELECT").unwrap_or(0);
    let from_idx = match find_keyword_top_level_from(sql, "FROM", select_idx + "SELECT".len()) {
        Some(idx) => idx,
        None => return vec!["\"*\"".to_string()],
    };

    let cols_raw = sql
        .get(select_idx + "SELECT".len()..from_idx)
        .unwrap_or("*")
        .trim();

    if cols_raw == "*" || cols_raw.is_empty() {
        return vec!["\"*\"".to_string()];
    }

    split_top_level(cols_raw, ',')
        .into_iter()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|expr| {
            let expr = strip_alias(expr);
            let first = expr.split_whitespace().next().unwrap_or(expr);
            let normalized = normalize_ref_name(first);
            format!("\"{}\"", normalized)
        })
        .collect()
}

fn strip_alias(expr: &str) -> &str {
    if let Some(as_idx) = find_keyword_top_level(expr, "AS") {
        return expr.get(..as_idx).unwrap_or(expr).trim();
    }
    expr.trim()
}

fn extract_where_clause(sql: &str) -> Option<Vec<String>> {
    let where_idx = find_keyword_top_level(sql, "WHERE")?;
    let content_start = where_idx + "WHERE".len();

    let order_idx = find_keyword_top_level_from(sql, "ORDER BY", content_start);
    let limit_idx = find_keyword_top_level_from(sql, "LIMIT", content_start);
    let end = match (order_idx, limit_idx) {
        (Some(a), Some(b)) => a.min(b),
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => sql.len(),
    };

    let where_raw = sql.get(content_start..end)?.trim();
    if where_raw.is_empty() {
        return None;
    }

    let mut filters = Vec::new();
    for cond in split_top_level_keyword(where_raw, "AND") {
        let cond = cond.trim();
        if cond.is_empty() {
            continue;
        }
        if let Some(filter) = parse_simple_condition(cond) {
            filters.push(filter);
        }
    }

    if filters.is_empty() {
        Some(vec![format!("// Complex WHERE: {}", where_raw)])
    } else {
        Some(filters)
    }
}

fn parse_simple_condition(cond: &str) -> Option<String> {
    let (col_raw, op, rhs_raw) = find_comparison(cond)?;

    let col = normalize_ref_name(col_raw.trim());
    let op_str = match op {
        "=" => "Operator::Eq",
        "!=" | "<>" => "Operator::Ne",
        "<" => "Operator::Lt",
        "<=" => "Operator::Lte",
        ">" => "Operator::Gt",
        ">=" => "Operator::Gte",
        _ => "Operator::Eq",
    };

    Some(format!(
        ".filter(\"{}\", {}, {})",
        col,
        op_str,
        rewrite_param_literal(rhs_raw.trim())
    ))
}

fn find_comparison(cond: &str) -> Option<(&str, &'static str, &str)> {
    const OPS: [&str; 7] = [">=", "<=", "!=", "<>", "=", ">", "<"];

    for op in OPS {
        if let Some(idx) = find_token_top_level(cond, op) {
            let left = cond.get(..idx)?;
            let right = cond.get(idx + op.len()..)?;
            return Some((left, op, right));
        }
    }

    None
}

fn extract_order_by(sql: &str) -> Option<(String, bool)> {
    let order_idx = find_keyword_top_level(sql, "ORDER BY")?;
    let start = order_idx + "ORDER BY".len();
    let limit_idx = find_keyword_top_level_from(sql, "LIMIT", start).unwrap_or(sql.len());

    let clause = sql.get(start..limit_idx)?.trim();
    if clause.is_empty() {
        return None;
    }

    let first_expr = split_top_level(clause, ',')
        .into_iter()
        .next()
        .unwrap_or("")
        .trim();
    if first_expr.is_empty() {
        return None;
    }

    let mut parts = first_expr.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }

    let desc = parts.last().is_some_and(|p| p.eq_ignore_ascii_case("DESC"));
    if parts
        .last()
        .is_some_and(|p| p.eq_ignore_ascii_case("ASC") || p.eq_ignore_ascii_case("DESC"))
    {
        parts.pop();
    }

    let col = normalize_ref_name(parts.join(" ").trim());
    if col.is_empty() {
        None
    } else {
        Some((col, desc))
    }
}

fn extract_limit(sql: &str) -> Option<u64> {
    let limit_idx = find_keyword_top_level(sql, "LIMIT")?;
    let mut cursor = skip_ws(sql, limit_idx + "LIMIT".len());
    let bytes = sql.as_bytes();

    let start = cursor;
    while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
        cursor += 1;
    }

    if cursor == start {
        return None;
    }

    sql.get(start..cursor)?.parse::<u64>().ok()
}

fn parse_insert_table_columns(sql: &str) -> Option<(String, Vec<String>)> {
    if !starts_with_keyword(sql, "INSERT INTO") {
        return None;
    }

    let mut cursor = skip_ws(sql, "INSERT INTO".len());
    let (table, next) = parse_sql_object_name(sql, cursor)?;
    cursor = skip_ws(sql, next);

    let bytes = sql.as_bytes();
    if bytes.get(cursor).copied() != Some(b'(') {
        return Some((table, Vec::new()));
    }

    let close = find_matching_paren(sql, cursor)?;
    let raw_cols = sql.get(cursor + 1..close).unwrap_or_default();
    let columns = split_top_level(raw_cols, ',')
        .into_iter()
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(normalize_ref_name)
        .filter(|c| !c.is_empty())
        .collect::<Vec<_>>();

    Some((table, columns))
}

fn parse_update_set_assignments(sql: &str) -> Option<(String, Vec<(String, String)>)> {
    if !starts_with_keyword(sql, "UPDATE") {
        return None;
    }

    let mut cursor = skip_ws(sql, "UPDATE".len());
    let (table, next) = parse_sql_object_name(sql, cursor)?;
    cursor = skip_ws(sql, next);

    let set_idx = find_keyword_top_level_from(sql, "SET", cursor)?;
    let set_start = set_idx + "SET".len();
    let where_idx = find_keyword_top_level_from(sql, "WHERE", set_start).unwrap_or(sql.len());
    let set_raw = sql.get(set_start..where_idx).unwrap_or_default().trim();

    let assignments = split_top_level(set_raw, ',')
        .into_iter()
        .map(str::trim)
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| {
            let eq = find_token_top_level(pair, "=")?;
            let col = normalize_ref_name(pair.get(..eq)?.trim());
            let val = pair.get(eq + 1..)?.trim().to_string();
            Some((col, val))
        })
        .collect::<Vec<_>>();

    Some((table, assignments))
}

fn parse_delete_table(sql: &str) -> Option<String> {
    if !starts_with_keyword(sql, "DELETE FROM") {
        return None;
    }

    let cursor = skip_ws(sql, "DELETE FROM".len());
    parse_sql_object_name(sql, cursor).map(|(table, _)| table)
}

fn parse_create_table_name(sql: &str) -> Option<String> {
    if !starts_with_keyword(sql, "CREATE TABLE") {
        return None;
    }

    let mut cursor = skip_ws(sql, "CREATE TABLE".len());
    if starts_with_keyword(sql.get(cursor..).unwrap_or_default(), "IF NOT EXISTS") {
        cursor = skip_ws(sql, cursor + "IF NOT EXISTS".len());
    }

    parse_sql_object_name(sql, cursor).map(|(table, _)| table)
}

fn parse_drop_object(sql: &str) -> Option<(String, String)> {
    if !starts_with_keyword(sql, "DROP") {
        return None;
    }

    let mut cursor = skip_ws(sql, "DROP".len());
    let (kind_raw, next) = parse_keyword_word(sql, cursor)?;
    let kind = kind_raw.to_ascii_uppercase();
    if kind != "TABLE" && kind != "INDEX" {
        return None;
    }

    cursor = skip_ws(sql, next);
    if starts_with_keyword(sql.get(cursor..).unwrap_or_default(), "IF EXISTS") {
        cursor = skip_ws(sql, cursor + "IF EXISTS".len());
    }

    let (name, _) = parse_sql_object_name(sql, cursor)?;
    Some((kind, name))
}

fn parse_truncate_table(sql: &str) -> Option<String> {
    if !starts_with_keyword(sql, "TRUNCATE") {
        return None;
    }

    let mut cursor = skip_ws(sql, "TRUNCATE".len());
    if starts_with_keyword(sql.get(cursor..).unwrap_or_default(), "TABLE") {
        cursor = skip_ws(sql, cursor + "TABLE".len());
    }

    parse_sql_object_name(sql, cursor).map(|(table, _)| table)
}

fn rewrite_param_literal(raw: &str) -> String {
    let raw = raw.trim();
    if let Some(stripped) = raw.strip_prefix('$')
        && let Ok(n) = stripped.parse::<u32>()
    {
        return format!("param_{} /* replace with actual value */", n);
    }
    raw.to_string()
}

fn split_top_level(text: &str, delimiter: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;

    let bytes = text.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if b == q {
                if matches!(q, b'\'' | b'"') && bytes.get(i + 1).copied() == Some(q) {
                    i += 2;
                    continue;
                }
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => {
                in_quote = Some(b);
            }
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }

        if b == delimiter as u8 && depth == 0 {
            out.push(text.get(start..i).unwrap_or_default());
            start = i + 1;
        }

        i += 1;
    }

    out.push(text.get(start..).unwrap_or_default());
    out
}

fn split_top_level_keyword<'a>(text: &'a str, keyword: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut cursor = 0usize;

    while let Some(idx) = find_keyword_top_level_from(text, keyword, cursor) {
        out.push(text.get(start..idx).unwrap_or_default());
        start = idx + keyword.len();
        cursor = start;
    }

    out.push(text.get(start..).unwrap_or_default());
    out
}

fn find_token_top_level(text: &str, token: &str) -> Option<usize> {
    find_keyword_top_level(text, token)
}

fn find_keyword_top_level(text: &str, keyword: &str) -> Option<usize> {
    find_keyword_top_level_from(text, keyword, 0)
}

fn find_keyword_top_level_from(text: &str, keyword: &str, min_idx: usize) -> Option<usize> {
    if keyword.is_empty() {
        return None;
    }

    let bytes = text.as_bytes();
    let upper = bytes
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let kw = keyword
        .as_bytes()
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();

    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;
    let mut i = 0usize;

    while i < bytes.len() {
        let b = bytes[i];

        if let Some(q) = in_quote {
            if b == q {
                if matches!(q, b'\'' | b'"') && bytes.get(i + 1).copied() == Some(q) {
                    i += 2;
                    continue;
                }
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => {
                in_quote = Some(b);
                i += 1;
                continue;
            }
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
                !is_ident_byte(upper[i - 1])
            };
            let after = i + kw.len();
            let after_ok = if after >= upper.len() {
                true
            } else {
                !is_ident_byte(upper[after])
            };

            if before_ok && after_ok {
                return Some(i);
            }
        }

        i += 1;
    }

    None
}

fn parse_sql_object_name(text: &str, start: usize) -> Option<(String, usize)> {
    let mut cursor = skip_ws(text, start);
    let mut segments = Vec::new();

    loop {
        let (segment, next) = parse_identifier_segment(text, cursor)?;
        let segment = segment.trim();
        if segment.is_empty() {
            break;
        }
        segments.push(segment.to_string());

        cursor = skip_ws(text, next);
        if text.as_bytes().get(cursor).copied() == Some(b'.') {
            cursor += 1;
            cursor = skip_ws(text, cursor);
            continue;
        }
        break;
    }

    let raw = segments.last()?.as_str();
    Some((trim_wrapping_quotes(raw).to_string(), cursor))
}

fn parse_identifier_segment(text: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = text.as_bytes();
    let mut cursor = skip_ws(text, start);

    let quote = bytes.get(cursor).copied();
    if matches!(quote, Some(b'"') | Some(b'`')) {
        let q = quote?;
        let open = cursor;
        cursor += 1;
        while cursor < bytes.len() {
            if bytes[cursor] == q {
                if bytes.get(cursor + 1).copied() == Some(q) {
                    cursor += 2;
                    continue;
                }
                cursor += 1;
                return text.get(open..cursor).map(|s| (s, cursor));
            }
            cursor += 1;
        }
        return None;
    }

    let start_ident = cursor;
    while cursor < bytes.len() {
        let b = bytes[cursor];
        if b.is_ascii_alphanumeric() || b == b'_' {
            cursor += 1;
        } else {
            break;
        }
    }

    if cursor == start_ident {
        None
    } else {
        text.get(start_ident..cursor).map(|s| (s, cursor))
    }
}

fn parse_keyword_word(text: &str, start: usize) -> Option<(String, usize)> {
    let bytes = text.as_bytes();
    let mut cursor = skip_ws(text, start);
    let word_start = cursor;

    while cursor < bytes.len() && bytes[cursor].is_ascii_alphabetic() {
        cursor += 1;
    }

    if cursor == word_start {
        None
    } else {
        Some((text.get(word_start..cursor)?.to_string(), cursor))
    }
}

fn normalize_ref_name(raw: &str) -> String {
    raw.split('.')
        .map(|part| trim_wrapping_quotes(part.trim()))
        .collect::<Vec<_>>()
        .join(".")
}

fn trim_wrapping_quotes(raw: &str) -> &str {
    raw.trim_matches('"').trim_matches('`')
}

fn find_matching_paren(text: &str, open_idx: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    if bytes.get(open_idx).copied() != Some(b'(') {
        return None;
    }

    let mut depth = 1usize;
    let mut in_quote: Option<u8> = None;
    let mut i = open_idx + 1;

    while i < bytes.len() {
        let b = bytes[i];

        if let Some(q) = in_quote {
            if b == q {
                if matches!(q, b'\'' | b'"') && bytes.get(i + 1).copied() == Some(q) {
                    i += 2;
                    continue;
                }
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }

        i += 1;
    }

    None
}

fn skip_ws(text: &str, mut idx: usize) -> usize {
    let bytes = text.as_bytes();
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let sql = "SELECT id, name FROM users WHERE active = true ORDER BY name ASC";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("Qail::get(\"users\")"));
        assert!(result.contains(".columns"));
        assert!(result.contains(".filter"));
        assert!(result.contains(".order_by"));
    }

    #[test]
    fn test_select_with_limit() {
        let sql = "SELECT * FROM users LIMIT 10";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains(".limit(10)"));
    }

    #[test]
    fn test_insert() {
        let sql = "INSERT INTO users (name, email) VALUES ('test', 'test@example.com')";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("Qail::add"));
    }

    #[test]
    fn test_update() {
        let sql = "UPDATE users SET name = 'new' WHERE id = 1";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("Qail::set"));
    }

    #[test]
    fn test_delete() {
        let sql = "DELETE FROM users WHERE id = 1";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("Qail::del"));
    }

    #[test]
    fn test_cte() {
        let sql = "WITH stats AS (SELECT COUNT(*) FROM orders) SELECT * FROM stats";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("CTE"));
    }

    #[test]
    fn test_quoted_identifiers() {
        let sql = "SELECT \"id\" FROM \"public\".\"users\" WHERE \"users\".\"id\" = $1";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("Qail::get(\"users\")"), "{result}");
        assert!(result.contains("param_1"), "{result}");
    }

    #[test]
    fn test_drop_index_if_exists() {
        let sql = "DROP INDEX IF EXISTS idx_users_email";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("DropIndex"), "{result}");
        assert!(result.contains("idx_users_email"), "{result}");
    }
}
