//! SQL to QAIL transformation using semantic text parsing.
//!
//! This transformer intentionally avoids external SQL AST parser dependencies.
//! It handles common CRUD forms plus basic CTE wrappers for migration workflows.

use regex::Regex;

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
    sql.trim_start()
        .to_ascii_uppercase()
        .starts_with(&keyword.to_ascii_uppercase())
}

fn transform_cte_select(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let cte_name_re = Regex::new(r"(?i)^\s*WITH\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(")
        .expect("valid cte name regex");

    let cte_name = cte_name_re
        .captures(&normalized)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_else(|| "cte".to_string());

    let mut source_table = "table".to_string();
    if let Some(inner_sql) = extract_first_cte_inner_sql(&normalized)
        && let Some(table) = extract_table_name_from_select(&inner_sql)
    {
        source_table = table;
    }

    format!(
        "use qail_core::ast::{{Qail, Operator, Order}};\n\n
         // CTE '{}': define as separate query and use .as_cte(\"{}\")\n
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

    let table = extract_table_name_from_select(&normalized).unwrap_or_else(|| "table".to_string());
    let columns = extract_columns_from_select(&normalized);
    let where_clause = extract_where_clause(&normalized);
    let order_by = extract_order_by(&normalized);
    let limit = extract_limit(&normalized);

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
    let insert_re =
        Regex::new(r"(?i)^\s*INSERT\s+INTO\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*(?:\(([^)]*)\))?")
            .expect("valid insert regex");

    let (table, columns) = if let Some(caps) = insert_re.captures(&normalized) {
        let table = caps
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| "table".to_string());
        let columns = caps
            .get(2)
            .map(|m| {
                m.as_str()
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        (table, columns)
    } else {
        ("table".to_string(), Vec::new())
    };

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
    let update_re =
        Regex::new(r"(?i)^\s*UPDATE\s+([A-Za-z_][A-Za-z0-9_\.]*)\s+SET\s+(.+?)(?:\s+WHERE\s+|$)")
            .expect("valid update regex");

    let (table, assignments_raw) = if let Some(caps) = update_re.captures(&normalized) {
        (
            caps.get(1)
                .map(|m| m.as_str().to_string())
                .unwrap_or_else(|| "table".to_string()),
            caps.get(2)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
        )
    } else {
        ("table".to_string(), String::new())
    };

    let assignments = assignments_raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|pair| {
            let eq = pair.find('=')?;
            let col = pair[..eq].trim().to_string();
            let val = pair[eq + 1..].trim().to_string();
            Some((col, val))
        })
        .collect::<Vec<_>>();

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
    let delete_re = Regex::new(r"(?i)^\s*DELETE\s+FROM\s+([A-Za-z_][A-Za-z0-9_\.]*)")
        .expect("valid delete regex");
    let table = delete_re
        .captures(&normalized)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_else(|| "table".to_string());

    format!(
        "use qail_core::ast::{{Qail, Operator}};\n\n
         let cmd = Qail::del(\"{}\")\n    .filter(\"id\", Operator::Eq, id);\n\n
         let result = driver.execute(&cmd).await?;",
        table
    )
}

fn transform_create_table(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let create_re = Regex::new(r"(?i)^\s*CREATE\s+TABLE\s+([A-Za-z_][A-Za-z0-9_\.]*)")
        .expect("valid create table regex");
    let table = create_re
        .captures(&normalized)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_else(|| "table".to_string());

    format!(
        "use qail_core::ast::Qail;\n\n
         let cmd = Qail::make(\"{}\")\n    // Add column definitions with .column_def(name, type, constraints)\n;\n\n
         let result = driver.execute(&cmd).await?;",
        table
    )
}

fn transform_drop(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let drop_re = Regex::new(r"(?i)^\s*DROP\s+(TABLE|INDEX)\s+([A-Za-z_][A-Za-z0-9_\.]*)")
        .expect("valid drop regex");

    if let Some(caps) = drop_re.captures(&normalized) {
        let kind = caps
            .get(1)
            .map(|m| m.as_str().to_ascii_uppercase())
            .unwrap_or_else(|| "TABLE".to_string());
        let name = caps
            .get(2)
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| "table".to_string());

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
    let truncate_re = Regex::new(r"(?i)^\s*TRUNCATE\s+(?:TABLE\s+)?([A-Za-z_][A-Za-z0-9_\.]*)")
        .expect("valid truncate regex");
    let table = truncate_re
        .captures(&normalized)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_else(|| "table".to_string());

    format!(
        "use qail_core::ast::Qail;\n\n
         let cmd = Qail::truncate(\"{}\");\n\n
         let result = driver.execute(&cmd).await?;",
        table
    )
}

fn transform_explain(sql: &str) -> String {
    let normalized = normalize_whitespace(sql);
    let analyze = normalized
        .to_ascii_uppercase()
        .starts_with("EXPLAIN ANALYZE");

    let inner_sql = normalized
        .split_once(' ')
        .map(|(_, rest)| rest)
        .unwrap_or_default();

    let inner = sql_to_qail(inner_sql).unwrap_or_else(|_| "// Could not parse SQL".to_string());

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

fn normalize_whitespace(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn extract_first_cte_inner_sql(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let with_pos = upper.find("WITH ")?;
    let as_pos = upper[with_pos..].find(" AS (")? + with_pos;
    let open_idx = as_pos + " AS ".len();
    let bytes = sql.as_bytes();
    if bytes.get(open_idx).copied() != Some(b'(') {
        return None;
    }

    let mut depth = 1usize;
    let mut i = open_idx + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(sql[open_idx + 1..i].to_string());
                }
            }
            _ => {}
        }
        i += 1;
    }

    None
}

fn extract_table_name_from_select(sql: &str) -> Option<String> {
    let re = Regex::new(r"(?i)\bFROM\s+([A-Za-z_][A-Za-z0-9_\.]*)").expect("valid from regex");
    re.captures(sql)
        .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
}

fn extract_columns_from_select(sql: &str) -> Vec<String> {
    let re = Regex::new(r"(?is)^\s*SELECT\s+(.+?)\s+FROM\b").expect("valid select columns regex");
    let cols = re
        .captures(sql)
        .and_then(|c| c.get(1).map(|m| m.as_str().trim().to_string()))
        .unwrap_or_else(|| "*".to_string());

    if cols == "*" {
        return vec!["\"*\"".to_string()];
    }

    cols.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|c| {
            let col = c.split_whitespace().next().unwrap_or(c).trim_matches('"');
            format!("\"{}\"", col)
        })
        .collect()
}

fn extract_where_clause(sql: &str) -> Option<Vec<String>> {
    let re = Regex::new(r"(?is)\bWHERE\s+(.+?)(?:\bORDER\s+BY\b|\bLIMIT\b|$)")
        .expect("valid where regex");
    let where_raw = re
        .captures(sql)
        .and_then(|c| c.get(1).map(|m| m.as_str().trim().to_string()))?;

    let and_re = Regex::new(r"(?i)\s+AND\s+").expect("valid and split regex");
    let mut filters = Vec::new();

    for cond in and_re.split(&where_raw) {
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
    let cmp_re = Regex::new(r"^([A-Za-z_][A-Za-z0-9_\.]*)\s*(=|!=|<>|<=|>=|<|>)\s*(.+)$")
        .expect("valid condition regex");
    let caps = cmp_re.captures(cond)?;

    let col = caps.get(1)?.as_str().trim().trim_matches('"');
    let op = caps.get(2)?.as_str();
    let rhs = caps.get(3)?.as_str().trim();

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
        rewrite_param_literal(rhs)
    ))
}

fn extract_order_by(sql: &str) -> Option<(String, bool)> {
    let re = Regex::new(r"(?is)\bORDER\s+BY\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*(ASC|DESC)?")
        .expect("valid order by regex");
    let caps = re.captures(sql)?;
    let col = caps.get(1)?.as_str().trim().trim_matches('"').to_string();
    let desc = caps
        .get(2)
        .map(|m| m.as_str().eq_ignore_ascii_case("DESC"))
        .unwrap_or(false);
    Some((col, desc))
}

fn extract_limit(sql: &str) -> Option<u64> {
    let re = Regex::new(r"(?i)\bLIMIT\s+(\d+)").expect("valid limit regex");
    let caps = re.captures(sql)?;
    caps.get(1)?.as_str().parse::<u64>().ok()
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
}
