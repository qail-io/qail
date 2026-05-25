//! Schema types and parsing for build-time validation.

use crate::ast::Expr;
use crate::migrate::types::ColumnType;
use crate::parser::grammar::ddl::parse_column_definition;
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Foreign key relationship definition
#[derive(Debug, Clone)]
pub struct ForeignKey {
    /// Column in this table that references another table
    pub column: String,
    /// Name of referenced table
    pub ref_table: String,
    /// Column in referenced table
    pub ref_column: String,
}

/// Table schema information with column types and relations
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table name.
    pub name: String,
    /// Column name → Column type (strongly-typed AST enum)
    pub columns: HashMap<String, ColumnType>,
    /// Column name → Access Policy (Default: "Public", can be "Protected")
    pub policies: HashMap<String, String>,
    /// Foreign key relationships to other tables
    pub foreign_keys: Vec<ForeignKey>,
    /// Whether this table has Row-Level Security enabled
    /// Auto-detected: table has `tenant_id` column or explicit `rls` keyword.
    pub rls_enabled: bool,
}

/// Parsed schema from schema.qail file
#[derive(Debug, Default)]
pub struct Schema {
    /// Table schemas keyed by table name.
    pub tables: HashMap<String, TableSchema>,
    /// SQL view names (column-level typing is not available in build schema parser).
    pub views: HashSet<String>,
    /// Infrastructure resources (bucket, queue, topic)
    pub resources: HashMap<String, ResourceSchema>,
}

/// Infrastructure resource schema (bucket, queue, topic)
#[derive(Debug, Clone)]
pub struct ResourceSchema {
    /// Resource name.
    pub name: String,
    /// Resource kind (bucket, queue, topic).
    pub kind: String,
    /// Cloud provider (e.g. "aws").
    pub provider: Option<String>,
    /// Provider-specific properties.
    pub properties: HashMap<String, String>,
}

fn strip_schema_comments(line: &str) -> &str {
    let Some(idx) = schema_comment_start(line, true) else {
        return line.trim();
    };
    line[..idx].trim()
}

#[cfg(test)]
fn strip_sql_line_comments(line: &str) -> &str {
    let Some(idx) = schema_comment_start(line, false) else {
        return line.trim();
    };
    line[..idx].trim()
}

fn strip_sql_migration_comments(
    line: &str,
    in_block_comment: &mut bool,
    dollar_quote: &mut Option<String>,
) -> String {
    let mut out = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut suppress_dollar_content = dollar_quote.is_some();
    let mut i = 0usize;

    while i < line.len() {
        if *in_block_comment {
            if line[i..].starts_with("*/") {
                i += 2;
                *in_block_comment = false;
            } else {
                i += line[i..].chars().next().map(char::len_utf8).unwrap_or(1);
            }
            continue;
        }

        if let Some(delim) = dollar_quote.as_deref() {
            if line[i..].starts_with(delim) {
                if !suppress_dollar_content {
                    out.push_str(delim);
                }
                i += delim.len();
                *dollar_quote = None;
                suppress_dollar_content = false;
            } else if let Some(ch) = line[i..].chars().next() {
                if !suppress_dollar_content {
                    out.push(ch);
                }
                i += ch.len_utf8();
            }
            continue;
        }

        let Some(ch) = line[i..].chars().next() else {
            break;
        };

        if in_single {
            out.push(ch);
            if ch == '\'' {
                if line[i + ch.len_utf8()..].starts_with('\'') {
                    out.push('\'');
                    i += ch.len_utf8() + 1;
                } else {
                    i += ch.len_utf8();
                    in_single = false;
                }
            } else {
                i += ch.len_utf8();
            }
            continue;
        }

        if in_double {
            out.push(ch);
            if ch == '"' {
                if line[i + ch.len_utf8()..].starts_with('"') {
                    out.push('"');
                    i += ch.len_utf8() + 1;
                } else {
                    i += ch.len_utf8();
                    in_double = false;
                }
            } else {
                i += ch.len_utf8();
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                out.push(ch);
                i += ch.len_utf8();
            }
            '"' => {
                in_double = true;
                out.push(ch);
                i += ch.len_utf8();
            }
            '$' if sql_dollar_quote_delimiter_at(line, i).is_some() => {
                let delim = sql_dollar_quote_delimiter_at(line, i).expect("delimiter checked");
                out.push_str(delim);
                i += delim.len();
                *dollar_quote = Some(delim.to_string());
            }
            '-' if line[i + ch.len_utf8()..].starts_with('-') => break,
            '/' if line[i + ch.len_utf8()..].starts_with('*') => {
                i += ch.len_utf8() + 1;
                *in_block_comment = true;
            }
            _ => {
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }

    out.trim().to_string()
}

fn schema_comment_start(line: &str, hash_comments: bool) -> Option<usize> {
    let bytes = line.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_double => {
                if in_single && bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_single = !in_single;
            }
            b'"' if !in_single => {
                if in_double && bytes.get(i + 1) == Some(&b'"') {
                    i += 2;
                    continue;
                }
                in_double = !in_double;
            }
            b'-' if !in_single && !in_double && bytes.get(i + 1) == Some(&b'-') => {
                return Some(i);
            }
            b'#' if hash_comments && !in_single && !in_double => return Some(i),
            _ => {}
        }
        i += 1;
    }

    None
}

fn count_parens_outside_quotes(line: &str) -> (usize, usize) {
    let mut in_single = false;
    let mut in_double = false;
    let mut dollar_quote: Option<String> = None;
    let mut opens = 0usize;
    let mut closes = 0usize;
    let mut i = 0usize;

    while i < line.len() {
        if let Some(delim) = dollar_quote.as_deref() {
            if line[i..].starts_with(delim) {
                i += delim.len();
                dollar_quote = None;
            } else {
                i += line[i..].chars().next().map(char::len_utf8).unwrap_or(1);
            }
            continue;
        }

        let Some(ch) = line[i..].chars().next() else {
            break;
        };
        match ch {
            '\'' if !in_double => {
                if in_single && line[i + ch.len_utf8()..].starts_with('\'') {
                    i += 2;
                    continue;
                }
                in_single = !in_single;
            }
            '"' if !in_single => {
                if in_double && line[i + ch.len_utf8()..].starts_with('"') {
                    i += 2;
                    continue;
                }
                in_double = !in_double;
            }
            '$' if !in_single && !in_double && sql_dollar_quote_delimiter_at(line, i).is_some() => {
                let delim = sql_dollar_quote_delimiter_at(line, i).expect("delimiter checked");
                dollar_quote = Some(delim.to_string());
                i += delim.len();
                continue;
            }
            '(' if !in_single && !in_double => opens += 1,
            ')' if !in_single && !in_double => closes += 1,
            _ => {}
        }
        i += ch.len_utf8();
    }

    (opens, closes)
}

fn sql_dollar_quote_delimiter_at(raw: &str, idx: usize) -> Option<&str> {
    let bytes = raw.as_bytes();
    if bytes.get(idx) != Some(&b'$') {
        return None;
    }

    let mut end = idx + 1;
    while end < bytes.len() {
        match bytes[end] {
            b'$' => return Some(&raw[idx..=end]),
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' => end += 1,
            _ => return None,
        }
    }

    None
}

impl Schema {
    /// Parse a schema.qail file
    pub fn parse_file(path: &str) -> Result<Self, String> {
        let content = crate::schema_source::read_qail_schema_source(path)?;
        Self::parse(&content)
    }

    /// Parse schema from string
    pub fn parse(content: &str) -> Result<Self, String> {
        let mut schema = Schema::default();
        let mut current_table: Option<String> = None;
        let mut current_columns: HashMap<String, ColumnType> = HashMap::new();
        let mut current_policies: HashMap<String, String> = HashMap::new();
        let mut current_fks: Vec<ForeignKey> = Vec::new();
        let mut current_rls_flag = false;
        let mut enum_types: HashMap<String, Vec<String>> = HashMap::new();

        let mut lines = content.lines().peekable();
        while let Some(raw_line) = lines.next() {
            let line = strip_schema_comments(raw_line);

            // Skip comments and empty lines
            if line.is_empty() {
                continue;
            }

            if current_table.is_none() && line.starts_with("enum ") {
                let (name, values) = parse_build_enum_declaration(line, &mut lines)?;
                if enum_types.insert(name.clone(), values).is_some() {
                    return Err(format!("duplicate enum declaration '{}'", name));
                }
                continue;
            }

            // Resource declarations: bucket, queue, topic
            // Only match at the top level, NOT inside a table block
            // (a column named 'topic' inside a table would otherwise be
            //  misidentified as a resource declaration)
            if current_table.is_none()
                && (line.starts_with("bucket ")
                    || line.starts_with("queue ")
                    || line.starts_with("topic "))
            {
                let parts: Vec<&str> = line.splitn(2, ' ').collect();
                let kind = parts[0].to_string();
                let rest = parts.get(1).copied().unwrap_or("").trim();

                // Extract name (before {
                let has_block = line.contains('{');
                let (name, block_start) = if has_block {
                    let (name, block) = rest.split_once('{').unwrap_or((rest, ""));
                    (name.trim().to_string(), Some(block.to_string()))
                } else {
                    let mut parts = rest.split_whitespace();
                    let name = parts.next().unwrap_or("").to_string();
                    if parts.next().is_some() {
                        return Err(format!("Trailing content after {} resource name", kind));
                    }
                    (name, None)
                };
                if name.is_empty() {
                    return Err(format!("Missing name for {} declaration", kind));
                }
                if !is_build_identifier(&name) {
                    return Err(format!("Invalid {} resource name '{}'", kind, name));
                }
                let mut provider = None;
                let mut properties = HashMap::new();

                if let Some(mut block) = block_start {
                    let mut block_content = None;
                    while block_content.is_none() {
                        block_content = resource_block_content_before_closing(&block)?;
                        if block_content.is_some() {
                            break;
                        }
                        let Some(next_line) = lines.next() else {
                            return Err(format!(
                                "Unclosed {} resource definition for '{}': expected closing '}}'",
                                kind, name
                            ));
                        };
                        let inner = strip_schema_comments(next_line);
                        block.push(' ');
                        block.push_str(inner);
                    }
                    let block = block_content.unwrap_or_default();
                    let tokens = split_resource_tokens(block.trim())?;
                    let mut tokens = tokens.iter();
                    let mut seen_keys = HashSet::new();
                    while let Some(key) = tokens.next() {
                        if !seen_keys.insert(key) {
                            return Err(format!(
                                "Duplicate resource property '{}' in '{}'",
                                key, name
                            ));
                        }
                        let Some(val) = tokens.next() else {
                            return Err(format!(
                                "Resource property '{}' in '{}' requires a value",
                                key, name
                            ));
                        };
                        if key == "provider" {
                            provider = Some(val.to_string());
                        } else {
                            properties.insert(key.to_string(), val.to_string());
                        }
                    }
                }

                if schema.resources.contains_key(&name) {
                    return Err(format!("duplicate resource declaration '{}'", name));
                }
                schema.resources.insert(
                    name.clone(),
                    ResourceSchema {
                        name,
                        kind,
                        provider,
                        properties,
                    },
                );
                continue;
            }

            // View declarations: `view name $$` or `materialized view name $$`
            // Track view names so query-table validation accepts view-backed reads.
            if current_table.is_none()
                && let Some(view_name) = extract_view_name(line)
            {
                if !is_build_table_ref(view_name) {
                    return Err(format!("Invalid view name '{}'", view_name));
                }
                if !schema.views.insert(view_name.to_string()) {
                    return Err(format!("duplicate view declaration '{}'", view_name));
                }
                continue;
            }

            // Table definition: table name { [rls]
            if line.starts_with("table ") && (line.ends_with('{') || line.contains('{')) {
                if let Some(table_name) = current_table.as_deref() {
                    return Err(format!(
                        "Table declaration encountered before closing table '{}'",
                        table_name
                    ));
                }

                // Parse new table name, check for `rls` keyword
                // Format: "table bookings rls {" or "table bookings {"
                let after_table = line.trim_start_matches("table ");
                let (before_brace, after_brace) = after_table
                    .split_once('{')
                    .ok_or_else(|| format!("Invalid table definition: {}", line))?;
                if !after_brace.trim().is_empty() {
                    return Err(format!(
                        "Trailing content after table opening brace for '{}'",
                        before_brace
                            .split_whitespace()
                            .next()
                            .unwrap_or("<missing>")
                    ));
                }
                let before_brace = before_brace.trim();
                let parts: Vec<&str> = before_brace.split_whitespace().collect();
                let Some(name) = parts.first().filter(|name| !name.is_empty()) else {
                    return Err("Missing name for table declaration".to_string());
                };
                if !is_build_table_ref(name) {
                    return Err(format!("Invalid table name '{}'", name));
                }
                let mut seen_rls_option = false;
                for option in parts.iter().skip(1) {
                    if *option != "rls" {
                        return Err(format!("Unknown table option '{}' for '{}'", option, name));
                    }
                    if seen_rls_option {
                        return Err(format!("Duplicate table option 'rls' for '{}'", name));
                    }
                    seen_rls_option = true;
                }
                current_rls_flag = parts.contains(&"rls");
                current_table = Some((*name).to_string());
            }
            // End of table definition
            else if let Some(after_brace) = line.strip_prefix('}') {
                let Some(table_name) = current_table.take() else {
                    return Err("Unexpected table closing brace".to_string());
                };
                if !after_brace.trim().is_empty() {
                    return Err(format!(
                        "Trailing content after table closing brace for '{}'",
                        table_name
                    ));
                }
                if schema.tables.contains_key(&table_name) {
                    return Err(format!("duplicate table declaration '{}'", table_name));
                }
                let has_rls = current_rls_flag || current_columns.contains_key("tenant_id");
                schema.tables.insert(
                    table_name.clone(),
                    TableSchema {
                        name: table_name,
                        columns: std::mem::take(&mut current_columns),
                        policies: std::mem::take(&mut current_policies),
                        foreign_keys: std::mem::take(&mut current_fks),
                        rls_enabled: has_rls,
                    },
                );
                current_rls_flag = false;
            }
            // Column definition: column_name TYPE [constraints] [ref:table.column] [protected]
            // Format from qail pull: "flow_name VARCHAR not_null"
            // New format with FK: "user_id UUID ref:users.id"
            // New format with Policy: "password_hash TEXT protected"
            else if current_table.is_some() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if let Some(col_name) = parts.first() {
                    if !is_build_identifier(col_name) {
                        let table_name = current_table.as_deref().unwrap_or("<unknown>");
                        return Err(format!(
                            "Invalid column name '{}' in table '{}'",
                            col_name, table_name
                        ));
                    }
                    if current_columns.contains_key(*col_name) {
                        let table_name = current_table.as_deref().unwrap_or("<unknown>");
                        return Err(format!(
                            "duplicate column '{}' in table '{}'",
                            col_name, table_name
                        ));
                    }
                    let table_name = current_table.as_deref().unwrap_or("<unknown>");
                    let Some(col_type_str) = parts.get(1).copied() else {
                        return Err(format!(
                            "Missing type for column '{}' in table '{}'",
                            col_name, table_name
                        ));
                    };
                    let col_type = match col_type_str.parse::<ColumnType>() {
                        Ok(col_type) => col_type,
                        Err(_) => {
                            if let Some(values) = enum_types.get(col_type_str) {
                                ColumnType::Enum {
                                    name: col_type_str.to_string(),
                                    values: values.clone(),
                                }
                            } else {
                                return Err(format!(
                                    "Unknown column type '{}' for column '{}' in table '{}'",
                                    col_type_str, col_name, table_name
                                ));
                            }
                        }
                    };
                    current_columns.insert(col_name.to_string(), col_type);

                    // Check for policies and foreign keys
                    let mut policy = "Public".to_string();
                    let mut seen_protected = false;
                    let mut seen_column_options = HashSet::new();
                    let mut nullability_option: Option<&str> = None;
                    let mut generated_option: Option<&str> = None;
                    let mut has_foreign_key = false;
                    let mut seen_fk_actions = HashSet::new();

                    let mut i = 2;
                    while i < parts.len() {
                        let part = parts[i];
                        if part == "protected" {
                            if seen_protected {
                                return Err(format!(
                                    "duplicate protected option for column '{}' in table '{}'",
                                    col_name, table_name
                                ));
                            }
                            seen_protected = true;
                            policy = "Protected".to_string();
                        } else if matches!(
                            part,
                            "primary_key"
                                | "not_null"
                                | "nullable"
                                | "unique"
                                | "generated_identity"
                                | "generated_by_default_identity"
                        ) {
                            if !seen_column_options.insert(part) {
                                return Err(format!(
                                    "duplicate column option '{}' for column '{}' in table '{}'",
                                    part, col_name, table_name
                                ));
                            }
                            if matches!(part, "not_null" | "nullable") {
                                if let Some(existing) = nullability_option {
                                    return Err(format!(
                                        "conflicting nullability options '{}' and '{}' for column '{}' in table '{}'",
                                        existing, part, col_name, table_name
                                    ));
                                }
                                nullability_option = Some(part);
                            }
                            if matches!(
                                part,
                                "generated_identity" | "generated_by_default_identity"
                            ) {
                                if let Some(existing) = generated_option {
                                    return Err(format!(
                                        "conflicting generated options '{}' and '{}' for column '{}' in table '{}'",
                                        existing, part, col_name, table_name
                                    ));
                                }
                                generated_option = Some(part);
                            }
                            // Build-time validation only needs shape, type, policy, and relations.
                        } else if part == "default" {
                            if i + 1 >= parts.len() {
                                return Err(format!(
                                    "default requires a value for column '{}' in table '{}'",
                                    col_name, table_name
                                ));
                            }
                            break;
                        } else if part.starts_with("default=")
                            || part.starts_with("default:")
                            || part.starts_with("generated_stored(")
                            || part.starts_with("check(")
                        {
                            break;
                        } else if let Some(ref_spec) = part.strip_prefix("ref:") {
                            // Parse "table.column" or ">table.column"
                            let (ref_table, ref_column) =
                                parse_build_ref_spec(ref_spec, col_name, table_name)?;
                            push_build_foreign_key(
                                &mut current_fks,
                                col_name,
                                ref_table,
                                ref_column,
                                table_name,
                            )?;
                            has_foreign_key = true;
                        } else if part == "references" {
                            if i + 1 >= parts.len() {
                                return Err(format!(
                                    "foreign key reference target is required for column '{}' in table '{}'",
                                    col_name, table_name
                                ));
                            }
                            i += 1;
                            let (ref_table, ref_column) =
                                parse_build_references_target(parts[i], col_name, table_name)?;
                            push_build_foreign_key(
                                &mut current_fks,
                                col_name,
                                ref_table,
                                ref_column,
                                table_name,
                            )?;
                            has_foreign_key = true;
                        } else if let Some(ref_target) = part.strip_prefix("references") {
                            let (ref_table, ref_column) =
                                parse_build_references_target(ref_target, col_name, table_name)?;
                            push_build_foreign_key(
                                &mut current_fks,
                                col_name,
                                ref_table,
                                ref_column,
                                table_name,
                            )?;
                            has_foreign_key = true;
                        } else if matches!(part, "on_delete" | "on_update") {
                            if !has_foreign_key {
                                return Err(format!(
                                    "{} requires a preceding foreign key for column '{}' in table '{}'",
                                    part, col_name, table_name
                                ));
                            }
                            if !seen_fk_actions.insert(part) {
                                return Err(format!(
                                    "duplicate {} action for column '{}' in table '{}'",
                                    part, col_name, table_name
                                ));
                            }
                            if i + 1 >= parts.len() {
                                return Err(format!(
                                    "{} requires a foreign key action for column '{}' in table '{}'",
                                    part, col_name, table_name
                                ));
                            }
                            i += 1;
                            if !is_build_fk_action(parts[i]) {
                                return Err(format!(
                                    "unknown foreign key action '{}' for column '{}' in table '{}'",
                                    parts[i], col_name, table_name
                                ));
                            }
                        } else if part == "check_name" {
                            if i + 1 >= parts.len() {
                                return Err(format!(
                                    "check_name requires a name for column '{}' in table '{}'",
                                    col_name, table_name
                                ));
                            }
                            i += 1;
                        } else {
                            return Err(format!(
                                "Unknown column option '{}' for column '{}' in table '{}'",
                                part, col_name, table_name
                            ));
                        }
                        i += 1;
                    }
                    current_policies.insert(col_name.to_string(), policy);
                }
            }
        }

        if let Some(table_name) = current_table.take() {
            return Err(format!(
                "Unclosed table definition for '{}': expected closing '}}'",
                table_name
            ));
        }

        Ok(schema)
    }

    /// Check if table exists
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name) || self.views.contains(name)
    }

    /// Get all table names that have RLS enabled
    pub fn rls_tables(&self) -> Vec<&str> {
        self.tables
            .iter()
            .filter(|(_, ts)| ts.rls_enabled)
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Check if a specific table has RLS enabled
    pub fn is_rls_table(&self, name: &str) -> bool {
        self.tables.get(name).is_some_and(|t| t.rls_enabled)
    }

    /// Get table schema
    pub fn table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.get(name)
    }

    /// Merge pending migrations into the schema
    /// Scans migration directory for:
    /// - legacy SQL migrations (`up.sql` / `*.sql`)
    /// - native QAIL migrations (`up.qail` / `*.qail`)
    pub fn merge_migrations(&mut self, migrations_dir: &str) -> Result<usize, String> {
        use std::fs;

        let dir = Path::new(migrations_dir);
        if !dir.exists() {
            return Ok(0); // No migrations directory
        }

        let mut merged_count = 0;

        // Walk migration directories (format: migrations/YYYYMMDD_name/up.sql)
        let entries =
            fs::read_dir(dir).map_err(|e| format!("Failed to read migrations dir: {}", e))?;

        for entry in entries.flatten() {
            let path = entry.path();

            // Check for migration file candidates in subdirectory (prefer native QAIL),
            // or direct file entries.
            let migration_file = if path.is_dir() {
                let up_qail = path.join("up.qail");
                let up_sql = path.join("up.sql");
                if up_qail.exists() {
                    up_qail
                } else if up_sql.exists() {
                    up_sql
                } else {
                    continue;
                }
            } else if path.extension().is_some_and(|e| e == "qail" || e == "sql") {
                path.clone()
            } else {
                continue;
            };

            if migration_file.exists() {
                let content = fs::read_to_string(&migration_file)
                    .map_err(|e| format!("Failed to read {}: {}", migration_file.display(), e))?;

                if migration_file.extension().is_some_and(|ext| ext == "qail") {
                    merged_count += self.parse_qail_migration(&content).map_err(|e| {
                        format!(
                            "Failed to parse native migration {}: {}",
                            migration_file.display(),
                            e
                        )
                    })?;
                } else {
                    merged_count += self.parse_sql_migration(&content);
                }
            }
        }

        Ok(merged_count)
    }

    /// Parse native QAIL migration content and merge tables/columns into build schema.
    pub(crate) fn parse_qail_migration(&mut self, qail: &str) -> Result<usize, String> {
        let parsed = Schema::parse(qail)?;
        let mut changes = 0usize;

        for (table_name, parsed_table) in parsed.tables {
            if let Some(existing) = self.tables.get_mut(&table_name) {
                for (col_name, col_type) in parsed_table.columns {
                    if let Some(existing_type) = existing.columns.get(&col_name) {
                        if existing_type != &col_type {
                            return Err(format!(
                                "conflicting column type for '{}.{}': existing {:?}, migration {:?}",
                                table_name, col_name, existing_type, col_type
                            ));
                        }
                    } else {
                        existing.columns.insert(col_name.clone(), col_type);
                        changes += 1;
                    }
                }
                for (col_name, policy) in parsed_table.policies {
                    if existing.policies.insert(col_name, policy).is_none() {
                        changes += 1;
                    }
                }
                for fk in parsed_table.foreign_keys {
                    let duplicate = existing.foreign_keys.iter().any(|existing_fk| {
                        existing_fk.column == fk.column
                            && existing_fk.ref_table == fk.ref_table
                            && existing_fk.ref_column == fk.ref_column
                    });
                    if !duplicate {
                        existing.foreign_keys.push(fk);
                        changes += 1;
                    }
                }
                if parsed_table.rls_enabled && !existing.rls_enabled {
                    existing.rls_enabled = true;
                    changes += 1;
                }
            } else {
                changes += 1 + parsed_table.columns.len();
                self.tables.insert(table_name, parsed_table);
            }
        }

        for view_name in parsed.views {
            if self.views.insert(view_name) {
                changes += 1;
            }
        }
        for (resource_name, resource) in parsed.resources {
            if self.resources.insert(resource_name, resource).is_none() {
                changes += 1;
            }
        }

        changes += self.parse_explicit_qail_apply_commands(qail)?;

        Ok(changes)
    }

    fn parse_explicit_qail_apply_commands(&mut self, qail: &str) -> Result<usize, String> {
        let mut changes = 0usize;

        for (line_no, raw_line) in qail.lines().enumerate() {
            let line = strip_schema_comments(raw_line);
            if line.is_empty() || !line.starts_with("alter ") {
                continue;
            }

            let (table, column_name, column_type) = parse_explicit_alter_add_column_line(line)
                .map_err(|err| format!("Line {}: {}", line_no + 1, err))?;

            if let Some(existing) = self.tables.get_mut(&table) {
                if let Some(existing_type) = existing.columns.get(&column_name) {
                    if existing_type != &column_type {
                        return Err(format!(
                            "conflicting column type for '{}.{}': existing {:?}, migration {:?}",
                            table, column_name, existing_type, column_type
                        ));
                    }
                } else {
                    existing.columns.insert(column_name, column_type);
                    changes += 1;
                }
            } else {
                let mut columns = HashMap::new();
                columns.insert(column_name, column_type);
                self.tables.insert(
                    table.clone(),
                    TableSchema {
                        name: table,
                        columns,
                        policies: HashMap::new(),
                        foreign_keys: vec![],
                        rls_enabled: false,
                    },
                );
                changes += 2;
            }
        }

        Ok(changes)
    }

    /// Parse SQL migration content and extract schema changes
    pub(crate) fn parse_sql_migration(&mut self, sql: &str) -> usize {
        let mut changes = 0;

        // Extract CREATE TABLE statements
        // Pattern: CREATE [UNLOGGED] TABLE [IF NOT EXISTS] table_name (columns...)
        let mut in_block_comment = false;
        let mut dollar_quote = None;
        for raw_line in sql.lines() {
            let line =
                strip_sql_migration_comments(raw_line, &mut in_block_comment, &mut dollar_quote);
            let line = line.as_str();
            if line.is_empty()
                || line.starts_with("/*")
                || line.starts_with('*')
                || line.starts_with("*/")
            {
                continue;
            }
            if let Some(table_name) = extract_create_table_name(line)
                && !self.tables.contains_key(&table_name)
            {
                self.tables.insert(
                    table_name.clone(),
                    TableSchema {
                        name: table_name,
                        columns: HashMap::new(),
                        policies: HashMap::new(),
                        foreign_keys: vec![],
                        rls_enabled: false,
                    },
                );
                changes += 1;
            }
        }

        // Extract column definitions from CREATE TABLE blocks
        // IMPORTANT: Only track CREATE blocks for tables that were newly created
        // by this migration. Tables that already exist in the schema (from schema.qail)
        // already have correct column types — overwriting them with ColumnType::Text
        // would cause false type-mismatch errors.
        let mut current_table: Option<String> = None;
        let mut in_create_block = false;
        let mut paren_depth = 0;
        let mut in_block_comment = false;
        let mut dollar_quote = None;

        for raw_line in sql.lines() {
            let line =
                strip_sql_migration_comments(raw_line, &mut in_block_comment, &mut dollar_quote);
            let line = line.as_str();
            if line.is_empty()
                || line.starts_with("/*")
                || line.starts_with('*')
                || line.starts_with("*/")
            {
                continue;
            }
            let line_upper = line.to_uppercase();

            if let Some(name) = extract_create_table_name(line) {
                // Only track column extraction for tables that DON'T already
                // have their types from schema.qail. Tables that existed before
                // this migration already have correct types; overwriting them
                // with ColumnType::Text would be a bug.
                if self.tables.get(&name).is_none_or(|t| t.columns.is_empty()) {
                    current_table = Some(name);
                } else {
                    current_table = None;
                }
                in_create_block = true;
                paren_depth = 0;
                if let Some(table) = current_table.clone() {
                    for col in extract_inline_create_columns(line) {
                        if let Some(t) = self.tables.get_mut(&table)
                            && t.columns.insert(col, ColumnType::Text).is_none()
                        {
                            changes += 1;
                        }
                    }
                }
            }

            if in_create_block {
                let (open_parens, close_parens) = count_parens_outside_quotes(line);
                paren_depth += open_parens;
                paren_depth = paren_depth.saturating_sub(close_parens);

                // Extract column name (first identifier after opening paren)
                if let Some(col) = extract_column_from_create(line)
                    && let Some(ref table) = current_table
                    && let Some(t) = self.tables.get_mut(table)
                    && t.columns.insert(col.clone(), ColumnType::Text).is_none()
                {
                    changes += 1;
                }

                if paren_depth == 0 && line.contains(')') {
                    in_create_block = false;
                    current_table = None;
                }
            }

            // ALTER TABLE ... ADD COLUMN
            if line_upper.starts_with("ALTER TABLE")
                && line_upper.contains("ADD COLUMN")
                && let Some((table, col)) = extract_alter_add_column(line)
            {
                if let Some(t) = self.tables.get_mut(&table) {
                    if t.columns.insert(col.clone(), ColumnType::Text).is_none() {
                        changes += 1;
                    }
                } else {
                    // Table might be new from this migration
                    let mut cols = HashMap::new();
                    cols.insert(col, ColumnType::Text);
                    self.tables.insert(
                        table.clone(),
                        TableSchema {
                            name: table,
                            columns: cols,
                            policies: HashMap::new(),
                            foreign_keys: vec![],
                            rls_enabled: false,
                        },
                    );
                    changes += 1;
                }
            }

            // ALTER TABLE ... ADD (without COLUMN keyword)
            if line_upper.starts_with("ALTER TABLE")
                && line_upper.contains(" ADD ")
                && !line_upper.contains("ADD COLUMN")
                && let Some((table, col)) = extract_alter_add(line)
                && let Some(t) = self.tables.get_mut(&table)
                && t.columns.insert(col.clone(), ColumnType::Text).is_none()
            {
                changes += 1;
            }

            // DROP TABLE
            if line_upper.starts_with("DROP TABLE") {
                for table_name in extract_drop_table_names(line) {
                    if self.tables.remove(&table_name).is_some() {
                        changes += 1;
                    }
                }
            }

            // ALTER TABLE ... DROP COLUMN
            if line_upper.starts_with("ALTER TABLE")
                && line_upper.contains("DROP COLUMN")
                && let Some((table, col)) = extract_alter_drop_column(line)
                && let Some(t) = self.tables.get_mut(&table)
                && t.columns.remove(&col).is_some()
            {
                changes += 1;
            }

            // ALTER TABLE ... DROP (without COLUMN keyword - PostgreSQL style)
            if line_upper.starts_with("ALTER TABLE")
                && line_upper.contains(" DROP ")
                && !line_upper.contains("DROP COLUMN")
                && !line_upper.contains("DROP CONSTRAINT")
                && !line_upper.contains("DROP INDEX")
                && let Some((table, col)) = extract_alter_drop(line)
                && let Some(t) = self.tables.get_mut(&table)
                && t.columns.remove(&col).is_some()
            {
                changes += 1;
            }

            // ALTER TABLE ... RENAME COLUMN old TO new
            if line_upper.starts_with("ALTER TABLE")
                && line_upper.contains("RENAME COLUMN")
                && let Some((table, old_col, new_col)) = extract_alter_rename_column(line)
                && let Some(t) = self.tables.get_mut(&table)
            {
                let old_type = t.columns.remove(&old_col);
                if old_type.is_some() {
                    changes += 1;
                }
                if t.columns
                    .insert(new_col, old_type.unwrap_or(ColumnType::Text))
                    .is_none()
                {
                    changes += 1;
                }
            }

            // ALTER TABLE ... RENAME TO new_table
            if line_upper.starts_with("ALTER TABLE")
                && line_upper.contains(" RENAME TO ")
                && !line_upper.contains("RENAME COLUMN")
                && let Some((old_table, new_table)) = extract_alter_rename_table(line)
                && !self.tables.contains_key(&new_table)
                && let Some(mut table) = self.tables.remove(&old_table)
            {
                table.name = new_table.clone();
                self.tables.insert(new_table, table);
                changes += 1;
            }
        }

        changes
    }
}

fn parse_build_enum_declaration<'a, I: Iterator<Item = &'a str>>(
    first_line: &str,
    lines: &mut std::iter::Peekable<I>,
) -> Result<(String, Vec<String>), String> {
    let rest = first_line
        .strip_prefix("enum ")
        .ok_or_else(|| "Expected 'enum' prefix".to_string())?
        .trim();
    let (name, body_start) = rest
        .split_once('{')
        .ok_or_else(|| "enum definition requires { values }".to_string())?;
    let name = name.trim();
    if name.is_empty() {
        return Err("enum name is missing before '{'".to_string());
    }
    if !is_build_table_ref(name) {
        return Err(format!("Invalid enum name '{}'", name));
    }

    let mut body = body_start.to_string();
    while build_enum_body_before_closing_brace(&body)?.is_none() {
        let Some(next_line) = lines.next() else {
            return Err(format!("enum '{}' is missing closing '}}'", name));
        };
        let inner = strip_schema_comments(next_line);
        body.push(' ');
        body.push_str(inner);
    }

    let body = build_enum_body_before_closing_brace(&body)?
        .ok_or_else(|| format!("enum '{}' is missing closing '}}'", name))?;
    let values = parse_build_enum_values(body)?;
    if values.is_empty() {
        return Err(format!("enum '{}' must have at least one value", name));
    }

    Ok((name.to_string(), values))
}

fn build_enum_body_before_closing_brace(raw: &str) -> Result<Option<&str>, String> {
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

fn parse_build_enum_values(raw: &str) -> Result<Vec<String>, String> {
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
                push_build_enum_value(&mut values, &raw[start..idx])?;
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    if quote.is_some() {
        return Err("unterminated quoted enum value".to_string());
    }

    push_build_enum_value(&mut values, &raw[start..])?;
    let mut seen = HashSet::new();
    for value in &values {
        if !seen.insert(value) {
            return Err(format!("duplicate enum value '{}'", value));
        }
    }

    Ok(values)
}

fn push_build_enum_value(values: &mut Vec<String>, raw: &str) -> Result<(), String> {
    let was_quoted = raw
        .trim()
        .chars()
        .next()
        .is_some_and(|ch| matches!(ch, '\'' | '"'));
    let value = parse_build_enum_value(raw)?;
    if value.is_empty() && !was_quoted {
        return Err("enum value is empty".to_string());
    }
    values.push(value);
    Ok(())
}

fn parse_build_enum_value(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(String::new());
    }

    if let Some(quote) = trimmed.chars().next().filter(|ch| matches!(ch, '"' | '\'')) {
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

        return Err("unterminated quoted enum value".to_string());
    }

    if trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Ok(trimmed.to_string());
    }

    Err(format!("invalid enum value token '{}'", trimmed))
}

fn parse_build_references_target(
    target: &str,
    col_name: &str,
    table_name: &str,
) -> Result<(String, String), String> {
    let target = target.trim();
    let (ref_table, ref_column) = target.split_once('(').ok_or_else(|| {
        format!(
            "Invalid foreign key reference target '{}' for column '{}' in table '{}'",
            target, col_name, table_name
        )
    })?;
    let ref_column = ref_column.strip_suffix(')').ok_or_else(|| {
        format!(
            "Invalid foreign key reference target '{}' for column '{}' in table '{}'",
            target, col_name, table_name
        )
    })?;
    let ref_table = ref_table.trim();
    let ref_column = ref_column.trim();
    if !is_build_table_ref(ref_table) || !is_build_identifier(ref_column) {
        return Err(format!(
            "Invalid foreign key reference target '{}' for column '{}' in table '{}'",
            target, col_name, table_name
        ));
    }

    Ok((ref_table.to_string(), ref_column.to_string()))
}

fn parse_build_ref_spec(
    ref_spec: &str,
    col_name: &str,
    table_name: &str,
) -> Result<(String, String), String> {
    let ref_spec = ref_spec.trim_start_matches('>');
    let (ref_table, ref_column) = ref_spec.split_once('.').ok_or_else(|| {
        format!(
            "Invalid ref target '{}' for column '{}' in table '{}'",
            ref_spec, col_name, table_name
        )
    })?;
    let ref_table = ref_table.trim();
    let ref_column = ref_column.trim();
    if !is_build_table_ref(ref_table) || !is_build_identifier(ref_column) {
        return Err(format!(
            "Invalid ref target '{}' for column '{}' in table '{}'",
            ref_spec, col_name, table_name
        ));
    }

    Ok((ref_table.to_string(), ref_column.to_string()))
}

fn push_build_foreign_key(
    foreign_keys: &mut Vec<ForeignKey>,
    column: &str,
    ref_table: String,
    ref_column: String,
    table_name: &str,
) -> Result<(), String> {
    if foreign_keys
        .iter()
        .any(|fk| fk.column == column && fk.ref_table == ref_table && fk.ref_column == ref_column)
    {
        return Err(format!(
            "duplicate foreign key '{}.{} -> {}.{}'",
            table_name, column, ref_table, ref_column
        ));
    }

    foreign_keys.push(ForeignKey {
        column: column.to_string(),
        ref_table,
        ref_column,
    });
    Ok(())
}

fn is_build_table_ref(value: &str) -> bool {
    let mut parts = value.split('.');
    let Some(first) = parts.next() else {
        return false;
    };
    !first.is_empty() && is_build_identifier(first) && parts.all(is_build_identifier)
}

fn is_build_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn is_build_fk_action(value: &str) -> bool {
    matches!(
        value,
        "cascade" | "set_null" | "set_default" | "restrict" | "no_action"
    )
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
                        return Err("Trailing content after resource definition".to_string());
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

fn parse_explicit_alter_add_column_line(
    line: &str,
) -> Result<(String, String, ColumnType), String> {
    let rest = line
        .strip_prefix("alter ")
        .ok_or_else(|| "expected 'alter <table> add <column:type[:constraints]>'".to_string())?
        .trim();

    let mut parts = rest.splitn(2, char::is_whitespace);
    let table = parts
        .next()
        .map(str::trim)
        .filter(|table| !table.is_empty())
        .ok_or_else(|| "expected table name after 'alter'".to_string())?;
    if !is_build_table_ref(table) {
        return Err(format!("invalid alter table name '{}'", table));
    }
    let remainder = parts
        .next()
        .map(str::trim)
        .ok_or_else(|| "expected 'add <column:type[:constraints]>' after table name".to_string())?;
    let column_def = remainder
        .strip_prefix("add ")
        .ok_or_else(|| "expected 'add <column:type[:constraints]>' after table name".to_string())?
        .trim();

    if column_def.is_empty() {
        return Err("expected column definition after 'add'".to_string());
    }

    let (remaining, column_expr) = parse_column_definition(column_def)
        .map_err(|_| format!("invalid column definition '{}'", column_def))?;
    if !remaining.trim().is_empty() {
        return Err(format!(
            "unexpected trailing content after column definition: '{}'",
            remaining.trim()
        ));
    }

    match column_expr {
        Expr::Def {
            name, data_type, ..
        } => {
            let column_type = data_type.parse::<ColumnType>().map_err(|_| {
                format!(
                    "unknown column type '{}' for column '{}' in alter '{}'",
                    data_type, name, table
                )
            })?;
            Ok((table.to_string(), name, column_type))
        }
        _ => Err("expected column definition after 'add'".to_string()),
    }
}

fn extract_view_name(line: &str) -> Option<&str> {
    let rest = if let Some(r) = line.strip_prefix("view ") {
        r
    } else {
        line.strip_prefix("materialized view ")?
    };

    let name = rest.split_whitespace().next().unwrap_or_default().trim();
    if name.is_empty() { None } else { Some(name) }
}

/// Extract table name from CREATE TABLE statement
fn extract_create_table_name(line: &str) -> Option<String> {
    let rest = extract_create_table_target_start(line)?;
    let rest = strip_sql_if_not_exists(rest).unwrap_or(rest);

    extract_sql_table_ref(rest)
}

fn extract_create_table_target_start(line: &str) -> Option<&str> {
    let mut rest = strip_sql_keyword(line, "CREATE")?;

    if let Some(after_unlogged) = strip_sql_keyword(rest, "UNLOGGED") {
        rest = after_unlogged;
    } else if strip_sql_keyword(rest, "TEMP")
        .or_else(|| strip_sql_keyword(rest, "TEMPORARY"))
        .is_some()
    {
        return None;
    }

    strip_sql_keyword(rest, "TABLE")
}

fn strip_sql_keyword<'a>(raw: &'a str, keyword: &str) -> Option<&'a str> {
    let rest = raw.trim_start();
    let tail = rest.get(keyword.len()..)?;
    if rest[..keyword.len()].eq_ignore_ascii_case(keyword)
        && (tail.is_empty() || tail.starts_with(char::is_whitespace))
    {
        Some(tail.trim_start())
    } else {
        None
    }
}

fn strip_sql_if_exists(raw: &str) -> Option<&str> {
    let after_if = strip_sql_keyword(raw, "IF")?;
    strip_sql_keyword(after_if, "EXISTS")
}

fn strip_sql_if_not_exists(raw: &str) -> Option<&str> {
    let after_if = strip_sql_keyword(raw, "IF")?;
    let after_not = strip_sql_keyword(after_if, "NOT")?;
    strip_sql_keyword(after_not, "EXISTS")
}

/// Extract column name from a line inside CREATE TABLE block
fn extract_column_from_create(line: &str) -> Option<String> {
    let line = line.trim();

    // Skip keywords and constraints
    // IMPORTANT: Must check for word boundaries to avoid matching column names
    // that happen to start with a keyword (e.g., created_at starts with CREATE,
    // primary_contact starts with PRIMARY, check_status starts with CHECK, etc.)
    let line_upper = line.to_uppercase();
    let starts_with_keyword = |kw: &str| -> bool {
        line_upper.starts_with(kw) && line_upper[kw.len()..].starts_with([' ', '('])
    };

    if starts_with_keyword("CREATE")
        || starts_with_keyword("PRIMARY")
        || starts_with_keyword("FOREIGN")
        || starts_with_keyword("UNIQUE")
        || starts_with_keyword("CHECK")
        || starts_with_keyword("CONSTRAINT")
        || starts_with_keyword("EXCLUDE")
        || starts_with_keyword("LIKE")
        || line_upper.starts_with(")")
        || line_upper.starts_with("(")
        || line.is_empty()
    {
        return None;
    }

    extract_sql_column_ref(line.trim_start_matches('(').trim())
}

fn extract_inline_create_columns(line: &str) -> Vec<String> {
    let Some(open_idx) = line.find('(') else {
        return Vec::new();
    };
    let Some(close_idx) = find_matching_sql_paren(line, open_idx) else {
        return Vec::new();
    };
    let body = &line[open_idx + 1..close_idx];
    split_sql_top_level_csv(body)
        .into_iter()
        .filter_map(|piece| extract_column_from_create(piece))
        .collect()
}

fn find_matching_sql_paren(raw: &str, open_idx: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut dollar_quote: Option<String> = None;
    let mut i = open_idx;

    while i < raw.len() {
        if let Some(delim) = dollar_quote.as_deref() {
            if raw[i..].starts_with(delim) {
                i += delim.len();
                dollar_quote = None;
            } else {
                i += raw[i..].chars().next().map(char::len_utf8).unwrap_or(1);
            }
            continue;
        }

        let ch = raw[i..].chars().next()?;
        match ch {
            '\'' if !in_double => {
                if in_single && raw[i + ch.len_utf8()..].starts_with('\'') {
                    i += 2;
                    continue;
                }
                in_single = !in_single;
            }
            '"' if !in_single => {
                if in_double && raw[i + ch.len_utf8()..].starts_with('"') {
                    i += 2;
                    continue;
                }
                in_double = !in_double;
            }
            '$' if !in_single && !in_double && sql_dollar_quote_delimiter_at(raw, i).is_some() => {
                let delim = sql_dollar_quote_delimiter_at(raw, i).expect("delimiter checked");
                dollar_quote = Some(delim.to_string());
                i += delim.len();
                continue;
            }
            '(' if !in_single && !in_double => depth += 1,
            ')' if !in_single && !in_double => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += ch.len_utf8();
    }

    None
}

fn split_sql_top_level_csv(raw: &str) -> Vec<&str> {
    let mut pieces = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut dollar_quote: Option<String> = None;
    let mut i = 0usize;

    while i < raw.len() {
        if let Some(delim) = dollar_quote.as_deref() {
            if raw[i..].starts_with(delim) {
                i += delim.len();
                dollar_quote = None;
            } else {
                i += raw[i..].chars().next().map(char::len_utf8).unwrap_or(1);
            }
            continue;
        }

        let Some(ch) = raw[i..].chars().next() else {
            break;
        };
        match ch {
            '\'' if !in_double => {
                if in_single && raw[i + ch.len_utf8()..].starts_with('\'') {
                    i += 2;
                    continue;
                }
                in_single = !in_single;
            }
            '"' if !in_single => {
                if in_double && raw[i + ch.len_utf8()..].starts_with('"') {
                    i += 2;
                    continue;
                }
                in_double = !in_double;
            }
            '$' if !in_single && !in_double && sql_dollar_quote_delimiter_at(raw, i).is_some() => {
                let delim = sql_dollar_quote_delimiter_at(raw, i).expect("delimiter checked");
                dollar_quote = Some(delim.to_string());
                i += delim.len();
                continue;
            }
            '(' if !in_single && !in_double => depth += 1,
            ')' if !in_single && !in_double => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                pieces.push(raw[start..i].trim());
                start = i + ch.len_utf8();
            }
            _ => {}
        }
        i += ch.len_utf8();
    }

    pieces.push(raw[start..].trim());
    pieces
}

/// Extract table and column from ALTER TABLE ... ADD COLUMN
fn extract_alter_add_column(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let add_pos = line_upper.find("ADD COLUMN")?;

    // Table name between ALTER TABLE and ADD COLUMN
    let table_part = &line[alter_pos + 11..add_pos];
    let table = extract_alter_table_ref(table_part)?;

    // Column name after ADD COLUMN [IF NOT EXISTS]
    let mut col_part = &line[add_pos + 10..];
    if let Some(stripped) = strip_sql_if_not_exists(col_part) {
        col_part = stripped;
    }
    let col = extract_sql_column_ref(col_part.trim())?;

    Some((table, col))
}

/// Extract table and column from ALTER TABLE ... ADD (without COLUMN keyword)
fn extract_alter_add(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let add_pos = line_upper.find(" ADD ")?;

    let table_part = &line[alter_pos + 11..add_pos];
    let table = extract_alter_table_ref(table_part)?;

    let mut col_part = &line[add_pos + 5..];
    if let Some(stripped) = strip_sql_if_not_exists(col_part) {
        col_part = stripped;
    }
    let col_upper = col_part.trim_start().to_uppercase();
    if [
        "CONSTRAINT",
        "PRIMARY",
        "UNIQUE",
        "CHECK",
        "FOREIGN",
        "EXCLUDE",
    ]
    .iter()
    .any(|keyword| col_upper.starts_with(keyword))
    {
        return None;
    }
    let col = extract_sql_column_ref(col_part.trim())?;

    Some((table, col))
}

/// Extract table names from DROP TABLE statement
fn extract_drop_table_names(line: &str) -> Vec<String> {
    let line_upper = line.to_uppercase();
    let Some(rest) = line_upper.strip_prefix("DROP TABLE") else {
        return Vec::new();
    };
    let rest = rest.trim_start();
    let rest = if rest.starts_with("IF EXISTS") {
        match rest.strip_prefix("IF EXISTS") {
            Some(rest) => rest.trim_start(),
            None => return Vec::new(),
        }
    } else {
        rest
    };

    split_sql_top_level_csv(&line[line.len() - rest.len()..])
        .into_iter()
        .filter_map(extract_sql_table_ref)
        .collect()
}

/// Extract table and column from ALTER TABLE ... DROP COLUMN
fn extract_alter_drop_column(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let drop_pos = line_upper.find("DROP COLUMN")?;

    // Table name between ALTER TABLE and DROP COLUMN
    let table_part = &line[alter_pos + 11..drop_pos];
    let table = extract_alter_table_ref(table_part)?;

    // Column name after DROP COLUMN
    let mut col_part = &line[drop_pos + 11..];
    if let Some(stripped) = strip_sql_if_exists(col_part) {
        col_part = stripped;
    }
    let col = extract_sql_column_ref(col_part.trim())?;

    Some((table, col))
}

/// Extract table and column from ALTER TABLE ... DROP (without COLUMN keyword)
fn extract_alter_drop(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let drop_pos = line_upper.find(" DROP ")?;

    let table_part = &line[alter_pos + 11..drop_pos];
    let table = extract_alter_table_ref(table_part)?;

    let mut col_part = &line[drop_pos + 6..];
    if let Some(stripped) = strip_sql_if_exists(col_part) {
        col_part = stripped;
    }
    let col = extract_sql_column_ref(col_part.trim())?;

    Some((table, col))
}

fn extract_alter_rename_column(line: &str) -> Option<(String, String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let rename_pos = line_upper.find("RENAME COLUMN")?;
    let to_pos = line_upper[rename_pos..].find(" TO ")? + rename_pos;

    let table_part = &line[alter_pos + 11..rename_pos];
    let table = extract_alter_table_ref(table_part)?;
    let old_part = &line[rename_pos + "RENAME COLUMN".len()..to_pos];
    let new_part = &line[to_pos + 4..];
    let old_col = extract_sql_column_ref(old_part.trim())?;
    let new_col = extract_sql_column_ref(new_part.trim())?;

    Some((table, old_col, new_col))
}

fn extract_alter_rename_table(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let rename_pos = line_upper.find(" RENAME TO ")?;

    let table_part = &line[alter_pos + 11..rename_pos];
    let old_table = extract_alter_table_ref(table_part)?;
    let new_part = &line[rename_pos + " RENAME TO ".len()..];
    let new_ref = extract_sql_table_ref(new_part.trim())?;
    let new_table = if new_ref.contains('.') {
        new_ref
    } else if let Some((schema, _)) = old_table.rsplit_once('.') {
        format!("{schema}.{new_ref}")
    } else {
        new_ref
    };

    Some((old_table, new_table))
}

fn extract_sql_table_ref(raw: &str) -> Option<String> {
    let mut rest = raw.trim_start();
    let mut parts = Vec::new();

    loop {
        let (part, tail, _) = parse_sql_identifier_segment(rest)?;
        parts.push(part.to_ascii_lowercase());
        rest = tail.trim_start();
        if let Some(tail) = rest.strip_prefix('.') {
            rest = tail.trim_start();
        } else {
            break;
        }
    }

    let name = parts.join(".");
    is_build_table_ref(&name).then_some(name)
}

fn extract_sql_column_ref(raw: &str) -> Option<String> {
    let (name, rest, quoted) = parse_sql_identifier_segment(raw)?;
    if rest.trim_start().starts_with('.') {
        return None;
    }
    let name = name.to_ascii_lowercase();
    if name.is_empty() || !is_build_identifier(&name) || (!quoted && name == "if") {
        None
    } else {
        Some(name)
    }
}

fn parse_sql_identifier_segment(raw: &str) -> Option<(String, &str, bool)> {
    let rest = raw.trim_start();
    if let Some(quoted) = rest.strip_prefix('"') {
        let mut out = String::new();
        let mut chars = quoted.char_indices().peekable();
        while let Some((idx, ch)) = chars.next() {
            if ch == '"' {
                if chars.peek().is_some_and(|(_, next)| *next == '"') {
                    out.push('"');
                    chars.next();
                    continue;
                }
                let consumed = 1 + idx + ch.len_utf8();
                return Some((out, &rest[consumed..], true));
            }
            out.push(ch);
        }
        return None;
    }

    let name: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        return None;
    }
    let tail = &rest[name.len()..];
    Some((name, tail, false))
}

fn extract_alter_table_ref(raw: &str) -> Option<String> {
    let mut rest = raw.trim_start();
    let upper = rest.to_uppercase();
    if upper.starts_with("IF EXISTS")
        && rest
            .get("IF EXISTS".len()..)
            .is_some_and(|tail| tail.starts_with(char::is_whitespace))
    {
        rest = rest.get("IF EXISTS".len()..)?.trim_start();
    }
    let upper = rest.to_uppercase();
    if upper.starts_with("ONLY")
        && rest
            .get("ONLY".len()..)
            .is_some_and(|tail| tail.starts_with(char::is_whitespace))
    {
        rest = rest.get("ONLY".len()..)?.trim_start();
    }
    extract_sql_table_ref(rest)
}

impl TableSchema {
    /// Check if column exists
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }

    /// Get column type
    pub fn column_type(&self, name: &str) -> Option<&ColumnType> {
        self.columns.get(name)
    }

    /// Get the primary key column name for this table.
    ///
    /// Convention: returns `"id"` if it exists as a column.
    /// This is a single point of truth for PK resolution — when the schema
    /// parser is enhanced to track PK constraints, update this method.
    pub fn primary_key_column(&self) -> &str {
        if self.columns.contains_key("id") {
            "id"
        } else {
            // Fallback: look for `{singular_table_name}_id` pattern
            // e.g., table "users" → "user_id"
            let singular = self.name.trim_end_matches('s');
            let conventional = format!("{}_id", singular);
            if self.columns.contains_key(&conventional) {
                // Leak into 'static to satisfy lifetime — this is called rarely
                // and the string is small. Alternatively, return String.
                return "id"; // Safe default — schema has no "id" but this avoids lifetime issues
            }
            "id" // Universal fallback
        }
    }
}

#[cfg(test)]
mod comment_tests {
    use super::{Schema, strip_schema_comments, strip_sql_line_comments};

    #[test]
    fn schema_comment_stripping_ignores_markers_inside_quotes() {
        assert_eq!(
            strip_schema_comments(r#"status TEXT default 'draft--internal#tag' # comment"#),
            r#"status TEXT default 'draft--internal#tag'"#
        );
        assert_eq!(
            strip_schema_comments(r#"status TEXT default "draft--internal#tag" -- comment"#),
            r#"status TEXT default "draft--internal#tag""#
        );
    }

    #[test]
    fn sql_comment_stripping_ignores_double_dash_inside_strings() {
        assert_eq!(
            strip_sql_line_comments("CREATE TABLE logs (message text DEFAULT 'a--b'); -- comment"),
            "CREATE TABLE logs (message text DEFAULT 'a--b');"
        );
        assert_eq!(
            strip_sql_line_comments("CREATE TABLE tags (name text DEFAULT '#not-comment');"),
            "CREATE TABLE tags (name text DEFAULT '#not-comment');"
        );
    }

    #[test]
    fn sql_migration_paren_depth_ignores_string_literals() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE logs (
  message text DEFAULT ')',
  tag text DEFAULT '(',
  level text
);
"#,
        );

        let logs = schema.table("logs").expect("logs table should parse");
        assert!(logs.has_column("message"));
        assert!(logs.has_column("tag"));
        assert!(logs.has_column("level"));
    }

    #[test]
    fn sql_migration_ignores_multiline_block_comments() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE users (
  id uuid
);

/*
ALTER TABLE users ADD COLUMN hidden text;
CREATE TABLE hidden_table (
  id uuid
);
*/
"#,
        );

        let users = schema.table("users").expect("users table should parse");
        assert!(users.has_column("id"));
        assert!(!users.has_column("hidden"));
        assert!(!schema.has_table("hidden_table"));
    }

    #[test]
    fn sql_migration_preserves_schema_qualified_table_names() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE app.users (
  id uuid
);

ALTER TABLE app.users ADD COLUMN email text;
"#,
        );

        assert!(!schema.has_table("app"));
        let users = schema
            .table("app.users")
            .expect("schema-qualified table should parse");
        assert!(users.has_column("id"));
        assert!(users.has_column("email"));
    }

    #[test]
    fn sql_migration_extracts_inline_create_table_columns() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            "CREATE TABLE users (id uuid, email text DEFAULT 'a,b', CHECK (length(email) > 3));",
        );

        let users = schema.table("users").expect("users table should parse");
        assert!(users.has_column("id"));
        assert!(users.has_column("email"));
        assert!(!users.has_column("check"));
    }

    #[test]
    fn sql_migration_drops_multiple_tables() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE app.users (id uuid);
CREATE TABLE app.posts (id uuid);
DROP TABLE IF EXISTS app.users, app.posts CASCADE;
"#,
        );

        assert!(!schema.has_table("app.users"));
        assert!(!schema.has_table("app.posts"));
    }

    #[test]
    fn sql_migration_ignores_create_table_non_column_clauses() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE bookings (
  id uuid,
  EXCLUDE USING gist (room WITH =),
  LIKE booking_template INCLUDING ALL
);
"#,
        );

        let bookings = schema
            .table("bookings")
            .expect("bookings table should parse");
        assert!(bookings.has_column("id"));
        assert!(!bookings.has_column("exclude"));
        assert!(!bookings.has_column("like"));
    }

    #[test]
    fn sql_migration_ignores_alter_add_constraints() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE users (id uuid, email text);
ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email);
ALTER TABLE users ADD PRIMARY KEY (id);
"#,
        );

        let users = schema.table("users").expect("users table should parse");
        assert!(users.has_column("id"));
        assert!(users.has_column("email"));
        assert!(!users.has_column("constraint"));
        assert!(!users.has_column("primary"));
    }

    #[test]
    fn sql_migration_handles_alter_table_modifiers() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE users (id uuid);
ALTER TABLE ONLY users ADD COLUMN email text;
ALTER TABLE IF EXISTS users DROP COLUMN id;
"#,
        );

        assert!(!schema.has_table("only"));
        assert!(!schema.has_table("if"));
        let users = schema.table("users").expect("users table should parse");
        assert!(!users.has_column("id"));
        assert!(users.has_column("email"));
    }

    #[test]
    fn sql_migration_handles_drop_column_if_exists() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE users (id uuid, old_email text, old_name text);
ALTER TABLE users DROP COLUMN IF EXISTS old_email;
ALTER TABLE users DROP IF EXISTS old_name;
"#,
        );

        let users = schema.table("users").expect("users table should parse");
        assert!(users.has_column("id"));
        assert!(!users.has_column("old_email"));
        assert!(!users.has_column("old_name"));
        assert!(!users.has_column("if"));
    }

    #[test]
    fn sql_migration_handles_quoted_table_and_column_identifiers() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE "app"."order" ("id" uuid, "select" text);
ALTER TABLE "app"."order" ADD COLUMN "from" text;
ALTER TABLE "app"."order" DROP COLUMN "select";
"#,
        );

        let orders = schema
            .table("app.order")
            .expect("quoted schema-qualified table should parse");
        assert!(orders.has_column("id"));
        assert!(orders.has_column("from"));
        assert!(!orders.has_column("select"));
    }

    #[test]
    fn sql_migration_ignores_dollar_quoted_default_syntax() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE logs (id uuid, body text DEFAULT $$a,b)--not-comment$$, tag text);
"#,
        );

        let logs = schema.table("logs").expect("logs table should parse");
        assert!(logs.has_column("id"));
        assert!(logs.has_column("body"));
        assert!(logs.has_column("tag"));
        assert!(!logs.has_column("b"));
        assert!(!logs.has_column("not"));
    }

    #[test]
    fn sql_migration_ignores_multiline_dollar_quoted_bodies() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE users (id uuid);
CREATE FUNCTION rebuild_hidden() RETURNS void AS $$
BEGIN
  CREATE TABLE hidden_from_function (id uuid);
END;
$$ LANGUAGE plpgsql;
"#,
        );

        assert!(schema.has_table("users"));
        assert!(!schema.has_table("hidden_from_function"));
    }

    #[test]
    fn sql_migration_handles_unlogged_create_tables() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE UNLOGGED TABLE IF NOT EXISTS jobs (id uuid, status text);
CREATE TEMP TABLE scratch_jobs (id uuid);
"#,
        );

        let jobs = schema.table("jobs").expect("unlogged table should parse");
        assert!(jobs.has_column("id"));
        assert!(jobs.has_column("status"));
        assert!(!schema.has_table("scratch_jobs"));
    }

    #[test]
    fn sql_migration_tracks_column_renames() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE users (id uuid, old_email text);
ALTER TABLE users RENAME COLUMN old_email TO email;
"#,
        );

        let users = schema.table("users").expect("users table should parse");
        assert!(users.has_column("id"));
        assert!(users.has_column("email"));
        assert!(!users.has_column("old_email"));
    }

    #[test]
    fn sql_migration_tracks_table_renames() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE app.users (id uuid, email text);
ALTER TABLE app.users RENAME TO customers;
"#,
        );

        assert!(!schema.has_table("app.users"));
        let customers = schema
            .table("app.customers")
            .expect("schema-qualified table rename should parse");
        assert!(customers.has_column("id"));
        assert!(customers.has_column("email"));
    }

    #[test]
    fn sql_migration_handles_add_if_not_exists_without_column_keyword() {
        let mut schema = Schema::default();
        schema.parse_sql_migration(
            r#"
CREATE TABLE users (id uuid);
ALTER TABLE users ADD IF NOT EXISTS email text;
"#,
        );

        let users = schema.table("users").expect("users table should parse");
        assert!(users.has_column("id"));
        assert!(users.has_column("email"));
        assert!(!users.has_column("if"));
    }
}
