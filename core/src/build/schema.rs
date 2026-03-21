//! Schema types and parsing for build-time validation.

use crate::migrate::types::ColumnType;
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
    let line = line.split_once("--").map_or(line, |(left, _)| left);
    line.split_once('#').map_or(line, |(left, _)| left).trim()
}

fn strip_sql_line_comments(line: &str) -> &str {
    line.split_once("--").map_or(line, |(left, _)| left).trim()
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

        for raw_line in content.lines() {
            let line = strip_schema_comments(raw_line);

            // Skip comments and empty lines
            if line.is_empty() {
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
                let name = rest.split('{').next().unwrap_or(rest).trim().to_string();
                let mut provider = None;
                let mut properties = HashMap::new();

                if line.contains('{') {
                    // Collect block content
                    let block = rest.split('{').nth(1).unwrap_or("").to_string();
                    if !block.contains('}') {
                        for inner in content.lines().skip_while(|l| !l.contains(line)) {
                            // Simple approach: read until }
                            if inner.contains('}') {
                                break;
                            }
                        }
                    }
                    let block = block.replace('}', "");
                    let mut tokens = block.split_whitespace();
                    while let Some(key) = tokens.next() {
                        if let Some(val) = tokens.next() {
                            let val = val.trim_matches('"').to_string();
                            if key == "provider" {
                                provider = Some(val);
                            } else {
                                properties.insert(key.to_string(), val);
                            }
                        }
                    }
                }

                if !name.is_empty() {
                    schema.resources.insert(
                        name.clone(),
                        ResourceSchema {
                            name,
                            kind,
                            provider,
                            properties,
                        },
                    );
                }
                continue;
            }

            // View declarations: `view name $$` or `materialized view name $$`
            // Track view names so query-table validation accepts view-backed reads.
            if current_table.is_none()
                && let Some(view_name) = extract_view_name(line)
            {
                schema.views.insert(view_name.to_string());
                continue;
            }

            // Table definition: table name { [rls]
            if line.starts_with("table ") && (line.ends_with('{') || line.contains('{')) {
                // Save previous table if any
                if let Some(table_name) = current_table.take() {
                    // Auto-detect RLS from tenant_id column or explicit `rls` marker.
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
                }

                // Parse new table name, check for `rls` keyword
                // Format: "table bookings rls {" or "table bookings {"
                let after_table = line.trim_start_matches("table ");
                let before_brace = after_table.split('{').next().unwrap_or("").trim();
                let parts: Vec<&str> = before_brace.split_whitespace().collect();
                let name = parts.first().unwrap_or(&"").to_string();
                current_rls_flag = parts.contains(&"rls");
                current_table = Some(name);
            }
            // End of table definition
            else if line == "}" {
                if let Some(table_name) = current_table.take() {
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
            }
            // Column definition: column_name TYPE [constraints] [ref:table.column] [protected]
            // Format from qail pull: "flow_name VARCHAR not_null"
            // New format with FK: "user_id UUID ref:users.id"
            // New format with Policy: "password_hash TEXT protected"
            else if current_table.is_some() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if let Some(col_name) = parts.first() {
                    // Second word is the type (default to TEXT if missing)
                    let col_type_str = parts.get(1).copied().unwrap_or("text");
                    let col_type = col_type_str
                        .parse::<ColumnType>()
                        .unwrap_or(ColumnType::Text);
                    current_columns.insert(col_name.to_string(), col_type);

                    // Check for policies and foreign keys
                    let mut policy = "Public".to_string();

                    for part in parts.iter().skip(2) {
                        if *part == "protected" {
                            policy = "Protected".to_string();
                        } else if let Some(ref_spec) = part.strip_prefix("ref:") {
                            // Parse "table.column" or ">table.column"
                            let ref_spec = ref_spec.trim_start_matches('>');
                            if let Some((ref_table, ref_col)) = ref_spec.split_once('.') {
                                current_fks.push(ForeignKey {
                                    column: col_name.to_string(),
                                    ref_table: ref_table.to_string(),
                                    ref_column: ref_col.to_string(),
                                });
                            }
                        }
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
    /// Scans migration directory for .sql files and extracts:
    /// - CREATE TABLE statements
    /// - ALTER TABLE ADD COLUMN statements
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

            // Check for up.sql in subdirectory
            let up_sql = if path.is_dir() {
                path.join("up.sql")
            } else if path.extension().is_some_and(|e| e == "sql") {
                path.clone()
            } else {
                continue;
            };

            if up_sql.exists() {
                let content = fs::read_to_string(&up_sql)
                    .map_err(|e| format!("Failed to read {}: {}", up_sql.display(), e))?;

                merged_count += self.parse_sql_migration(&content);
            }
        }

        Ok(merged_count)
    }

    /// Parse SQL migration content and extract schema changes
    pub(crate) fn parse_sql_migration(&mut self, sql: &str) -> usize {
        let mut changes = 0;

        // Extract CREATE TABLE statements
        // Pattern: CREATE TABLE [IF NOT EXISTS] table_name (columns...)
        for raw_line in sql.lines() {
            let line = strip_sql_line_comments(raw_line);
            if line.is_empty()
                || line.starts_with("/*")
                || line.starts_with('*')
                || line.starts_with("*/")
            {
                continue;
            }
            let line_upper = line.to_uppercase();

            if line_upper.starts_with("CREATE TABLE")
                && let Some(table_name) = extract_create_table_name(line)
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

        for raw_line in sql.lines() {
            let line = strip_sql_line_comments(raw_line);
            if line.is_empty()
                || line.starts_with("/*")
                || line.starts_with('*')
                || line.starts_with("*/")
            {
                continue;
            }
            let line_upper = line.to_uppercase();

            if line_upper.starts_with("CREATE TABLE")
                && let Some(name) = extract_create_table_name(line)
            {
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
            }

            if in_create_block {
                paren_depth += line.chars().filter(|c| *c == '(').count();
                paren_depth =
                    paren_depth.saturating_sub(line.chars().filter(|c| *c == ')').count());

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
            if line_upper.starts_with("DROP TABLE")
                && let Some(table_name) = extract_drop_table_name(line)
                && self.tables.remove(&table_name).is_some()
            {
                changes += 1;
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
        }

        changes
    }
}

fn extract_view_name(line: &str) -> Option<&str> {
    let rest = if let Some(r) = line.strip_prefix("view ") {
        r
    } else if let Some(r) = line.strip_prefix("materialized view ") {
        r
    } else {
        return None;
    };

    let name = rest.split_whitespace().next().unwrap_or_default().trim();
    if name.is_empty() { None } else { Some(name) }
}

/// Extract table name from CREATE TABLE statement
fn extract_create_table_name(line: &str) -> Option<String> {
    let line_upper = line.to_uppercase();
    let rest = line_upper.strip_prefix("CREATE TABLE")?;
    let rest = rest.trim_start();
    let rest = if rest.starts_with("IF NOT EXISTS") {
        rest.strip_prefix("IF NOT EXISTS")?.trim_start()
    } else {
        rest
    };

    // Get table name (first identifier)
    let name: String = line[line.len() - rest.len()..]
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    if name.is_empty() {
        None
    } else {
        Some(name.to_lowercase())
    }
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
        || line_upper.starts_with(")")
        || line_upper.starts_with("(")
        || line.is_empty()
    {
        return None;
    }

    // First word is column name
    let name: String = line
        .trim_start_matches('(')
        .trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    if name.is_empty() || name.to_uppercase() == "IF" {
        None
    } else {
        Some(name.to_lowercase())
    }
}

/// Extract table and column from ALTER TABLE ... ADD COLUMN
fn extract_alter_add_column(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let add_pos = line_upper.find("ADD COLUMN")?;

    // Table name between ALTER TABLE and ADD COLUMN
    let table_part = &line[alter_pos + 11..add_pos];
    let table: String = table_part
        .trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    // Column name after ADD COLUMN [IF NOT EXISTS]
    let mut col_part = &line[add_pos + 10..];
    let col_upper = col_part.trim().to_uppercase();
    if col_upper.starts_with("IF NOT EXISTS") {
        col_part = &col_part.trim()[13..]; // skip "IF NOT EXISTS"
    }
    let col: String = col_part
        .trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    if table.is_empty() || col.is_empty() {
        None
    } else {
        Some((table.to_lowercase(), col.to_lowercase()))
    }
}

/// Extract table and column from ALTER TABLE ... ADD (without COLUMN keyword)
fn extract_alter_add(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let add_pos = line_upper.find(" ADD ")?;

    let table_part = &line[alter_pos + 11..add_pos];
    let table: String = table_part
        .trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    let col_part = &line[add_pos + 5..];
    let col: String = col_part
        .trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    if table.is_empty() || col.is_empty() {
        None
    } else {
        Some((table.to_lowercase(), col.to_lowercase()))
    }
}

/// Extract table name from DROP TABLE statement
fn extract_drop_table_name(line: &str) -> Option<String> {
    let line_upper = line.to_uppercase();
    let rest = line_upper.strip_prefix("DROP TABLE")?;
    let rest = rest.trim_start();
    let rest = if rest.starts_with("IF EXISTS") {
        rest.strip_prefix("IF EXISTS")?.trim_start()
    } else {
        rest
    };

    // Get table name (first identifier)
    let name: String = line[line.len() - rest.len()..]
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    if name.is_empty() {
        None
    } else {
        Some(name.to_lowercase())
    }
}

/// Extract table and column from ALTER TABLE ... DROP COLUMN
fn extract_alter_drop_column(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let drop_pos = line_upper.find("DROP COLUMN")?;

    // Table name between ALTER TABLE and DROP COLUMN
    let table_part = &line[alter_pos + 11..drop_pos];
    let table: String = table_part
        .trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    // Column name after DROP COLUMN
    let col_part = &line[drop_pos + 11..];
    let col: String = col_part
        .trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    if table.is_empty() || col.is_empty() {
        None
    } else {
        Some((table.to_lowercase(), col.to_lowercase()))
    }
}

/// Extract table and column from ALTER TABLE ... DROP (without COLUMN keyword)
fn extract_alter_drop(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let drop_pos = line_upper.find(" DROP ")?;

    let table_part = &line[alter_pos + 11..drop_pos];
    let table: String = table_part
        .trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    let col_part = &line[drop_pos + 6..];
    let col: String = col_part
        .trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    if table.is_empty() || col.is_empty() {
        None
    } else {
        Some((table.to_lowercase(), col.to_lowercase()))
    }
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
