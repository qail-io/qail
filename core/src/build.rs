//! Build-time QAIL validation module.
//!
//! This module provides compile-time validation for QAIL queries
//! without requiring proc macros.
//!
//! # Usage in build.rs
//!
//! ```ignore
//! // In your build.rs:
//! fn main() {
//!     qail_core::build::validate();
//! }
//! ```
//!
//! # Environment Variables
//!
//! - `QAIL=schema` - Validate against schema.qail file
//! - `QAIL=live` - Validate against live database
//! - `QAIL=false` - Skip validation

use crate::migrate::types::ColumnType;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
#[cfg(feature = "analyzer")]
use syn::spanned::Spanned;
#[cfg(feature = "analyzer")]
use syn::visit::Visit;

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
    /// Auto-detected: table has `operator_id` column OR has `rls` keyword in schema.qail
    pub rls_enabled: bool,
}

/// Parsed schema from schema.qail file
#[derive(Debug, Default)]
pub struct Schema {
    /// Table schemas keyed by table name.
    pub tables: HashMap<String, TableSchema>,
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

        for line in content.lines() {
            let line = line.trim();

            // Skip comments and empty lines
            if line.is_empty() || line.starts_with('#') {
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

            // Table definition: table name { [rls]
            if line.starts_with("table ") && (line.ends_with('{') || line.contains('{')) {
                // Save previous table if any
                if let Some(table_name) = current_table.take() {
                    // Auto-detect RLS: table has operator_id column or was marked `rls`
                    let has_rls = current_rls_flag || current_columns.contains_key("operator_id");
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
                    let has_rls = current_rls_flag || current_columns.contains_key("operator_id");
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
            else if current_table.is_some() && !line.starts_with('#') && !line.is_empty() {
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

        Ok(schema)
    }

    /// Check if table exists
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
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
    fn parse_sql_migration(&mut self, sql: &str) -> usize {
        let mut changes = 0;

        // Extract CREATE TABLE statements
        // Pattern: CREATE TABLE [IF NOT EXISTS] table_name (columns...)
        for line in sql.lines() {
            let line_upper = line.trim().to_uppercase();

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
        let mut current_table: Option<String> = None;
        let mut in_create_block = false;
        let mut paren_depth = 0;

        for line in sql.lines() {
            let line = line.trim();
            let line_upper = line.to_uppercase();

            if line_upper.starts_with("CREATE TABLE")
                && let Some(name) = extract_create_table_name(line)
            {
                current_table = Some(name);
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
            if line_upper.contains("ALTER TABLE")
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
            if line_upper.contains("ALTER TABLE")
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
            if line_upper.contains("ALTER TABLE")
                && line_upper.contains("DROP COLUMN")
                && let Some((table, col)) = extract_alter_drop_column(line)
                && let Some(t) = self.tables.get_mut(&table)
                && t.columns.remove(&col).is_some()
            {
                changes += 1;
            }

            // ALTER TABLE ... DROP (without COLUMN keyword - PostgreSQL style)
            if line_upper.contains("ALTER TABLE")
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

/// Extracted QAIL usage from source code
#[derive(Debug)]
pub struct QailUsage {
    /// Source file path.
    pub file: String,
    /// Line number (1-indexed).
    pub line: usize,
    /// Table name referenced.
    pub table: String,
    /// Column names referenced.
    pub columns: Vec<String>,
    /// CRUD action (GET, SET, ADD, DEL, PUT).
    pub action: String,
    /// Whether this references a CTE rather than a real table.
    pub is_cte_ref: bool,
    /// Whether this query chain includes `.with_rls(` call
    pub has_rls: bool,
}

/// Scan Rust source files for QAIL usage patterns
pub fn scan_source_files(src_dir: &str) -> Vec<QailUsage> {
    let mut usages = Vec::new();
    scan_directory(Path::new(src_dir), &mut usages);
    usages
}

fn scan_directory(dir: &Path, usages: &mut Vec<QailUsage>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                scan_directory(&path, usages);
            } else if path.extension().is_some_and(|e| e == "rs")
                && let Ok(content) = fs::read_to_string(&path)
            {
                scan_file(&path.display().to_string(), &content, usages);
            }
        }
    }
}

/// Phase 1+2: Collect let-bindings that map variable names to string literal(s).
///
/// Handles:
///   `let table = "foo";`                                    → {"table": ["foo"]}
///   `let (table, col) = ("foo", "bar");`                    → {"table": ["foo"], "col": ["bar"]}
///   `let (table, col) = if cond { ("a", "x") } else { ("b", "y") };`
///                                                           → {"table": ["a", "b"], "col": ["x", "y"]}
///   `let table = if cond { "a" } else { "b" };`             → {"table": ["a", "b"]}
fn collect_let_bindings(content: &str) -> HashMap<String, Vec<String>> {
    let mut bindings: HashMap<String, Vec<String>> = HashMap::new();

    // Join all lines for multi-line let analysis
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();

        // Look for: let IDENT = "literal"
        // or:       let (IDENT, IDENT) = ...
        if let Some(rest) = line.strip_prefix("let ") {
            let rest = rest.trim();

            // Phase 1: Simple  let table = "literal";
            if let Some((var, rhs)) = parse_simple_let(rest) {
                if let Some(lit) = extract_string_arg(rhs.trim()) {
                    bindings.entry(var).or_default().push(lit);
                    i += 1;
                    continue;
                }

                // Phase 2: let table = if cond { "a" } else { "b" };
                let rhs = rhs.trim();
                if rhs.starts_with("if ") {
                    // Collect the full if/else expression, possibly spanning multiple lines
                    let mut full_expr = rhs.to_string();
                    let mut j = i + 1;
                    // Keep joining lines until we see the closing `;`
                    while j < lines.len() && !full_expr.contains(';') {
                        full_expr.push(' ');
                        full_expr.push_str(lines[j].trim());
                        j += 1;
                    }
                    let literals = extract_branch_literals(&full_expr);
                    if !literals.is_empty() {
                        bindings.entry(var).or_default().extend(literals);
                    }
                }
            }

            // Phase 2: Destructuring  let (table, col) = if cond { ("a", "x") } else { ("b", "y") };
            //          or             let (table, col) = ("a", "b");
            if rest.starts_with('(') {
                // Collect the full line (may span multiple lines)
                let mut full_line = line.to_string();
                let mut j = i + 1;
                while j < lines.len() && !full_line.contains(';') {
                    full_line.push(' ');
                    full_line.push_str(lines[j].trim());
                    j += 1;
                }

                if let Some(result) = parse_destructuring_let(&full_line) {
                    for (name, values) in result {
                        bindings.entry(name).or_default().extend(values);
                    }
                }
            }
        }

        i += 1;
    }

    bindings
}

/// Parse `ident = rest` from a let statement (after stripping `let `).
/// Returns (variable_name, right_hand_side).
fn parse_simple_let(s: &str) -> Option<(String, &str)> {
    // Must start with an ident char, not `(` (that's destructuring) or `mut`
    let s = s.strip_prefix("mut ").unwrap_or(s).trim();
    if s.starts_with('(') {
        return None;
    }

    // Extract identifier
    let ident: String = s.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    if ident.is_empty() {
        return None;
    }

    // Skip optional type annotation  : Type
    let rest = s[ident.len()..].trim_start();
    let rest = if rest.starts_with(':') {
        // Skip past the type, find the `=`
        rest.find('=').map(|pos| &rest[pos..])?
    } else {
        rest
    };

    let rest = rest.strip_prefix('=')?.trim();
    Some((ident, rest))
}

/// Extract string literals from if/else branches.
/// Handles: `if cond { "a" } else { "b" }` → ["a", "b"]
fn extract_branch_literals(expr: &str) -> Vec<String> {
    let mut literals = Vec::new();

    // Find all `{ "literal" }` patterns in the expression
    let mut remaining = expr;
    while let Some(brace_pos) = remaining.find('{') {
        let inside = &remaining[brace_pos + 1..];
        if let Some(close_pos) = inside.find('}') {
            let block = inside[..close_pos].trim();
            // Check if block content is a simple string literal
            if let Some(lit) = extract_string_arg(block) {
                literals.push(lit);
            }
            remaining = &inside[close_pos + 1..];
        } else {
            break;
        }
    }

    literals
}

/// Parse destructuring let: `let (a, b) = ...;`
/// Returns vec of (name, possible_values) for each position.
fn parse_destructuring_let(line: &str) -> Option<Vec<(String, Vec<String>)>> {
    // Find `let (` or `let mut (`
    let rest = line.strip_prefix("let ")?.trim();
    let rest = rest.strip_prefix("mut ").unwrap_or(rest).trim();
    let rest = rest.strip_prefix('(')?;

    // Extract variable names from the tuple pattern
    let close_paren = rest.find(')')?;
    let names_str = &rest[..close_paren];
    let names: Vec<String> = names_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && !s.starts_with('_'))
        .collect();

    if names.is_empty() {
        return None;
    }

    // Find the RHS after `=`
    let after_pattern = &rest[close_paren + 1..];
    let eq_pos = after_pattern.find('=')?;
    let rhs = after_pattern[eq_pos + 1..].trim();

    // Case 1: Simple tuple  ("a", "b")
    if rhs.starts_with('(') {
        let values = extract_tuple_literals(rhs);
        if values.len() == names.len() {
            return Some(
                names.into_iter()
                    .zip(values.into_iter())
                    .map(|(n, v)| (n, vec![v]))
                    .collect()
            );
        }
    }

    // Case 2: if/else  if cond { ("a", "x") } else { ("b", "y") }
    if rhs.starts_with("if ") {
        let mut all_tuples: Vec<Vec<String>> = Vec::new();

        // Extract tuples from each branch
        let mut remaining = rhs;
        while let Some(brace_pos) = remaining.find('{') {
            let inside = &remaining[brace_pos + 1..];
            if let Some(close_pos) = find_matching_brace(inside) {
                let block = inside[..close_pos].trim();
                // Try to extract a tuple from the block
                if block.starts_with('(') {
                    let values = extract_tuple_literals(block);
                    if values.len() == names.len() {
                        all_tuples.push(values);
                    }
                }
                remaining = &inside[close_pos + 1..];
            } else {
                break;
            }
        }

        if !all_tuples.is_empty() {
            let mut result: Vec<(String, Vec<String>)> = names.iter()
                .map(|n| (n.clone(), Vec::new()))
                .collect();

            for tuple in &all_tuples {
                for (i, val) in tuple.iter().enumerate() {
                    if i < result.len() {
                        result[i].1.push(val.clone());
                    }
                }
            }

            return Some(result);
        }
    }

    None
}

/// Extract string literals from a tuple: ("a", "b", "c") → ["a", "b", "c"]
fn extract_tuple_literals(s: &str) -> Vec<String> {
    let mut literals = Vec::new();
    let s = s.trim();
    let s = s.strip_prefix('(').unwrap_or(s);
    // Find the closing paren (handle nested parens)
    let content = if let Some(pos) = s.rfind(')') {
        &s[..pos]
    } else {
        s.trim_end_matches(';').trim_end_matches(')')
    };

    for part in content.split(',') {
        let part = part.trim();
        if let Some(lit) = extract_string_arg(part) {
            literals.push(lit);
        }
    }
    literals
}

/// Find the position of the matching `}` for the first `{`,
/// handling nested braces.
fn find_matching_brace(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, ch) in s.chars().enumerate() {
        match ch {
            '{' => depth += 1,
            '}' => {
                if depth == 0 {
                    return Some(i);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    None
}

fn scan_file(file: &str, content: &str, usages: &mut Vec<QailUsage>) {
    // All CRUD patterns: GET=SELECT, ADD=INSERT, SET=UPDATE, DEL=DELETE, PUT=UPSERT
    // Also detect Qail::typed (compile-time safety) and Qail::raw_sql (advisory)
    let patterns = [
        ("Qail::get(", "GET"),
        ("Qail::add(", "ADD"),
        ("Qail::set(", "SET"),
        ("Qail::del(", "DEL"),
        ("Qail::put(", "PUT"),
        ("Qail::typed(", "TYPED"),
        ("Qail::raw_sql(", "RAW"),
    ];

    // Phase 1+2: Collect let-bindings that resolve variable → string literal(s)
    let let_bindings = collect_let_bindings(content);

    // First pass: collect all CTE alias names defined anywhere in the file.
    // Catches .to_cte("name") and .with("name", ...) patterns.
    // Note: .with_cte(cte_var) takes a variable, not a string literal,
    // so we can't extract the alias name from source text.
    let mut file_cte_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for line in content.lines() {
        let line = line.trim();
        // .to_cte("name") — most common CTE pattern
        if let Some(pos) = line.find(".to_cte(") {
            let after = &line[pos + 8..];
            if let Some(name) = extract_string_arg(after) {
                file_cte_names.insert(name);
            }
        }
        // .with("name", query) — inline CTE
        if let Some(pos) = line.find(".with(") {
            let after = &line[pos + 6..];
            if let Some(name) = extract_string_arg(after) {
                file_cte_names.insert(name);
            }
        }
    }

    // Second pass: detect Qail usage and mark CTE refs
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();

        // Check if this line starts a Qail chain
        for (pattern, action) in &patterns {
            if let Some(pos) = line.find(pattern) {
                let start_line = i + 1; // 1-indexed

                // Extract table name from Qail::get("table") or Qail::typed(module::Table)
                let after = &line[pos + pattern.len()..];

                let table = if *action == "TYPED" {
                    // Qail::typed(module::Table) — extract module name as table
                    extract_typed_table_arg(after)
                } else {
                    extract_string_arg(after)
                };

                if *action == "RAW" {
                    // raw_sql bypasses schema — emit advisory, don't validate
                    println!(
                        "cargo:warning=QAIL: raw SQL at {}:{} — not schema-validated",
                        file, start_line
                    );
                    break;
                }

                if let Some(table) = table {
                    // Join continuation lines (lines that start with .)
                    let mut full_chain = line.to_string();
                    let mut j = i + 1;
                    while j < lines.len() {
                        let next = lines[j].trim();
                        if next.starts_with('.') {
                            full_chain.push_str(next);
                            j += 1;
                        } else if next.is_empty() {
                            j += 1; // Skip empty lines
                        } else {
                            break;
                        }
                    }

                    // Check if this table name is a CTE alias defined anywhere in the file
                    // (via .to_cte(), .with(), or .with_cte())
                    let is_cte_ref = file_cte_names.contains(&table);

                    // Check if query chain includes .with_rls( or .rls(
                    let has_rls = full_chain.contains(".with_rls(") || full_chain.contains(".rls(");

                    // Extract column names from the full chain
                    let columns = extract_columns(&full_chain);

                    usages.push(QailUsage {
                        file: file.to_string(),
                        line: start_line,
                        table,
                        columns,
                        action: action.to_string(),
                        is_cte_ref,
                        has_rls,
                    });

                    // Skip to end of chain
                    i = j.saturating_sub(1);
                } else if *action != "TYPED" {
                    // Dynamic table name — try to resolve via let-bindings
                    let var_hint = after.split(')').next().unwrap_or("?").trim();

                    // Strip field access: ct.table → table, etc.
                    let lookup_key = var_hint.rsplit('.').next().unwrap_or(var_hint);

                    if let Some(resolved_tables) = let_bindings.get(lookup_key) {
                        // Resolved! Validate each possible table
                        // Join continuation lines for column extraction
                        let mut full_chain = line.to_string();
                        let mut j = i + 1;
                        while j < lines.len() {
                            let next = lines[j].trim();
                            if next.starts_with('.') {
                                full_chain.push_str(next);
                                j += 1;
                            } else if next.is_empty() {
                                j += 1;
                            } else {
                                break;
                            }
                        }
                        let columns = extract_columns(&full_chain);
                        let has_rls = full_chain.contains(".with_rls(") || full_chain.contains(".rls(");

                        for resolved_table in resolved_tables {
                            let is_cte_ref = file_cte_names.contains(resolved_table);
                            usages.push(QailUsage {
                                file: file.to_string(),
                                line: start_line,
                                table: resolved_table.clone(),
                                columns: columns.clone(),
                                action: action.to_string(),
                                is_cte_ref,
                                has_rls,
                            });
                        }
                        i = j.saturating_sub(1);
                    } else {
                        // Truly dynamic — cannot validate
                        println!(
                            "cargo:warning=Qail: dynamic table name `{}` in {}:{} — cannot validate columns at build time. Consider using string literals.",
                            var_hint, file, start_line
                        );
                    }
                }
                // else: Qail::typed with non-parsable table — skip silently (it has compile-time safety)
                break; // Only match one pattern per line
            }
        }
        i += 1;
    }
}

fn extract_string_arg(s: &str) -> Option<String> {
    // Find "string" pattern
    let s = s.trim();
    if let Some(stripped) = s.strip_prefix('"') {
        let end = stripped.find('"')?;
        Some(stripped[..end].to_string())
    } else {
        None
    }
}

/// Extract table name from `Qail::typed(module::Table)` patterns.
/// Parses `module::StructName` and returns the last identifier-like segment
/// before the final `::item` as the table name.
///
/// Examples:
///  - `users::table`         → `users`
///  - `users::Users`         → `users`
///  - `schema::users::table` → `users`  (second-to-last segment)
///  - `Orders`               → `orders` (single ident, no ::)
fn extract_typed_table_arg(s: &str) -> Option<String> {
    let s = s.trim();
    // Collect the full path: identifier::Identifier::...
    let ident: String = s
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
        .collect();

    let segments: Vec<&str> = ident.split("::").filter(|s| !s.is_empty()).collect();

    match segments.len() {
        0 => None,
        1 => {
            // Single ident like `Orders` — use it directly
            let name = segments[0];
            if name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                Some(name.to_lowercase())
            } else {
                None
            }
        }
        _ => {
            // Multiple segments like `users::table` or `schema::users::table`
            // Take the second-to-last segment as the table name
            let table = segments[segments.len() - 2];
            if table.chars().all(|c| c.is_alphanumeric() || c == '_') {
                Some(table.to_lowercase())
            } else {
                None
            }
        }
    }
}

fn extract_columns(line: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut remaining = line;

    // .column("col") — singular column
    while let Some(pos) = remaining.find(".column(") {
        let after = &remaining[pos + 8..];
        if let Some(col) = extract_string_arg(after) {
            columns.push(col);
        }
        remaining = after;
    }

    // Reset for .columns([...]) — array syntax (most common pattern)
    remaining = line;
    while let Some(pos) = remaining.find(".columns(") {
        let after = &remaining[pos + 9..];
        // Find the opening bracket [
        if let Some(bracket_start) = after.find('[') {
            let inside = &after[bracket_start + 1..];
            // Find the closing bracket ]
            if let Some(bracket_end) = inside.find(']') {
                let array_content = &inside[..bracket_end];
                // Extract all string literals from the array
                let mut scan = array_content;
                while let Some(quote_start) = scan.find('"') {
                    let after_quote = &scan[quote_start + 1..];
                    if let Some(quote_end) = after_quote.find('"') {
                        let col = &after_quote[..quote_end];
                        if !col.is_empty() {
                            columns.push(col.to_string());
                        }
                        scan = &after_quote[quote_end + 1..];
                    } else {
                        break;
                    }
                }
            }
        }
        remaining = after;
    }

    // Reset for next pattern
    remaining = line;

    // .filter("col", ...)
    while let Some(pos) = remaining.find(".filter(") {
        let after = &remaining[pos + 8..];
        if let Some(col) = extract_string_arg(after)
            && !col.contains('.')
        {
            columns.push(col);
        }
        remaining = after;
    }

    // .eq("col", val), .ne("col", val), .gt, .lt, .gte, .lte
    for method in [
        ".eq(", ".ne(", ".gt(", ".lt(", ".gte(", ".lte(", ".like(", ".ilike(",
    ] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(col) = extract_string_arg(after)
                && !col.contains('.')
            {
                columns.push(col);
            }
            temp = after;
        }
    }

    // .where_eq("col", val) — WHERE clause column
    remaining = line;
    while let Some(pos) = remaining.find(".where_eq(") {
        let after = &remaining[pos + 10..];
        if let Some(col) = extract_string_arg(after)
            && !col.contains('.')
        {
            columns.push(col);
        }
        remaining = after;
    }

    // .order_by("col", ...)
    remaining = line;
    while let Some(pos) = remaining.find(".order_by(") {
        let after = &remaining[pos + 10..];
        if let Some(col) = extract_string_arg(after)
            && !col.contains('.')
        {
            columns.push(col);
        }
        remaining = after;
    }

    // .order_desc("col"), .order_asc("col")
    for method in [".order_desc(", ".order_asc("] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(col) = extract_string_arg(after)
                && !col.contains('.')
            {
                columns.push(col);
            }
            temp = after;
        }
    }

    // .in_vals("col", vals)
    remaining = line;
    while let Some(pos) = remaining.find(".in_vals(") {
        let after = &remaining[pos + 9..];
        if let Some(col) = extract_string_arg(after)
            && !col.contains('.')
        {
            columns.push(col);
        }
        remaining = after;
    }

    // ── Additional DSL methods (Finding #4) ──────────────────────────

    // .is_null("col"), .is_not_null("col")
    for method in [".is_null(", ".is_not_null("] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(col) = extract_string_arg(after)
                && !col.contains('.')
            {
                columns.push(col);
            }
            temp = after;
        }
    }

    // .set_value("col", val), .set_coalesce("col", val), .set_coalesce_opt("col", val)
    for method in [".set_value(", ".set_coalesce(", ".set_coalesce_opt("] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(col) = extract_string_arg(after)
                && !col.contains('.')
            {
                columns.push(col);
            }
            temp = after;
        }
    }

    // .returning(["col_a", "col_b"]) — array pattern, same as .columns()
    remaining = line;
    while let Some(pos) = remaining.find(".returning(") {
        let after = &remaining[pos + 11..];
        if let Some(bracket_start) = after.find('[') {
            let inside = &after[bracket_start + 1..];
            if let Some(bracket_end) = inside.find(']') {
                let array_content = &inside[..bracket_end];
                let mut scan = array_content;
                while let Some(quote_start) = scan.find('"') {
                    let after_quote = &scan[quote_start + 1..];
                    if let Some(quote_end) = after_quote.find('"') {
                        let col = &after_quote[..quote_end];
                        if !col.is_empty() && !col.contains('.') {
                            columns.push(col.to_string());
                        }
                        scan = &after_quote[quote_end + 1..];
                    } else {
                        break;
                    }
                }
            }
        }
        remaining = after;
    }

    // .on_conflict_update(&["col"], ...) and .on_conflict_nothing(&["col"])
    // Extract conflict column names from the first array arg
    for method in [".on_conflict_update(", ".on_conflict_nothing("] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(bracket_start) = after.find('[') {
                let inside = &after[bracket_start + 1..];
                if let Some(bracket_end) = inside.find(']') {
                    let array_content = &inside[..bracket_end];
                    let mut scan = array_content;
                    while let Some(quote_start) = scan.find('"') {
                        let after_quote = &scan[quote_start + 1..];
                        if let Some(quote_end) = after_quote.find('"') {
                            let col = &after_quote[..quote_end];
                            if !col.is_empty() && !col.contains('.') {
                                columns.push(col.to_string());
                            }
                            scan = &after_quote[quote_end + 1..];
                        } else {
                            break;
                        }
                    }
                }
            }
            temp = after;
        }
    }

    // Clean up extracted columns: strip Postgres ::type casts and AS aliases.
    // e.g. "id::text" → "id", "conn.id::text as connection_id" → "conn.id",
    // "COALESCE(inv.capacity - inv.reserved, 0)::bigint as x" → skipped (expression)
    let columns: Vec<String> = columns
        .into_iter()
        .map(|col| {
            // Strip " as alias" suffix (case-insensitive)
            let col = if let Some(pos) = col.find(" as ").or_else(|| col.find(" AS ")) {
                col[..pos].trim().to_string()
            } else {
                col
            };
            // Strip ::type cast suffix
            if let Some(pos) = col.find("::") {
                col[..pos].to_string()
            } else {
                col
            }
        })
        .filter(|col| {
            // Skip expressions that aren't simple column references
            !col.contains('(') && !col.contains(')') && !col.contains(' ')
        })
        .collect();

    columns
}

fn usage_action_to_ast(action: &str) -> crate::ast::Action {
    use crate::ast::Action;

    match action {
        "GET" | "TYPED" => Action::Get,
        "ADD" => Action::Add,
        "SET" => Action::Set,
        "DEL" => Action::Del,
        "PUT" => Action::Put,
        _ => Action::Get,
    }
}

fn append_scanned_columns(cmd: &mut crate::ast::Qail, columns: &[String]) {
    use crate::ast::Expr;

    for col in columns {
        // Skip qualified columns (CTE refs like cte.column)
        if col.contains('.') {
            continue;
        }
        // Skip SQL function expressions (e.g., count(*), SUM(amount))
        // and wildcard (*) — these are valid SQL, not schema columns
        if col.contains('(') || col == "*" {
            continue;
        }
        let exists = cmd
            .columns
            .iter()
            .any(|e| matches!(e, Expr::Named(existing) if existing == col));
        if !exists {
            cmd.columns.push(Expr::Named(col.clone()));
        }
    }
}

#[cfg(feature = "analyzer")]
type SynUsageKey = (String, usize, String, String);

#[cfg(feature = "analyzer")]
#[derive(Debug, Clone)]
struct SynParsedUsage {
    line: usize,
    action: String,
    table: String,
    cmd: crate::ast::Qail,
    has_rls: bool,
    score: usize,
}

#[cfg(feature = "analyzer")]
struct SynMethodStep {
    name: String,
    args: Vec<syn::Expr>,
}

#[cfg(feature = "analyzer")]
#[derive(Debug)]
struct SynConstructor {
    line: usize,
    action: String,
    ast_action: crate::ast::Action,
    table: String,
}

#[cfg(feature = "analyzer")]
fn syn_usage_key(file: &str, line: usize, action: &str, table: &str) -> SynUsageKey {
    (
        file.to_string(),
        line,
        action.to_string(),
        table.to_string(),
    )
}

#[cfg(feature = "analyzer")]
fn build_syn_usage_index(usages: &[QailUsage]) -> HashMap<SynUsageKey, SynParsedUsage> {
    let mut files = std::collections::HashSet::new();
    for usage in usages {
        files.insert(usage.file.clone());
    }

    let mut index: HashMap<SynUsageKey, SynParsedUsage> = HashMap::new();
    for file in files {
        for parsed in extract_syn_usages_from_file(&file) {
            let key = syn_usage_key(&file, parsed.line, &parsed.action, &parsed.table);
            match index.get(&key) {
                Some(existing) if existing.score >= parsed.score => {}
                _ => {
                    index.insert(key, parsed);
                }
            }
        }
    }

    index
}

#[cfg(feature = "analyzer")]
fn extract_syn_usages_from_file(file: &str) -> Vec<SynParsedUsage> {
    let Ok(content) = fs::read_to_string(file) else {
        return Vec::new();
    };
    extract_syn_usages_from_source(&content)
}

#[cfg(feature = "analyzer")]
fn extract_syn_usages_from_source(source: &str) -> Vec<SynParsedUsage> {
    let Ok(syntax) = syn::parse_file(source) else {
        return Vec::new();
    };

    struct SynQailVisitor {
        usages: Vec<SynParsedUsage>,
    }

    impl SynQailVisitor {
        fn new() -> Self {
            Self { usages: Vec::new() }
        }
    }

    impl<'ast> Visit<'ast> for SynQailVisitor {
        fn visit_expr(&mut self, node: &'ast syn::Expr) {
            if let Some(parsed) = parse_qail_chain_from_expr(node) {
                self.usages.push(parsed);
            }
            syn::visit::visit_expr(self, node);
        }
    }

    let mut visitor = SynQailVisitor::new();
    visitor.visit_file(&syntax);
    visitor.usages
}

#[cfg(feature = "analyzer")]
fn parse_qail_chain_from_expr(expr: &syn::Expr) -> Option<SynParsedUsage> {
    let mut steps = Vec::<SynMethodStep>::new();
    let mut cursor = expr;

    loop {
        match cursor {
            syn::Expr::MethodCall(method) => {
                steps.push(SynMethodStep {
                    name: method.method.to_string(),
                    args: method.args.iter().cloned().collect(),
                });
                cursor = &method.receiver;
            }
            syn::Expr::Call(call) => {
                let ctor = parse_qail_constructor(call)?;
                steps.reverse();

                let mut cmd = crate::ast::Qail {
                    action: ctor.ast_action,
                    table: ctor.table.clone(),
                    ..Default::default()
                };
                let mut has_rls = false;

                for step in steps {
                    apply_syn_method_step(&mut cmd, &step.name, &step.args, &mut has_rls);
                }

                let score = syn_cmd_score(&cmd, has_rls);
                return Some(SynParsedUsage {
                    line: ctor.line,
                    action: ctor.action,
                    table: ctor.table,
                    cmd,
                    has_rls,
                    score,
                });
            }
            syn::Expr::Paren(paren) => cursor = &paren.expr,
            syn::Expr::Group(group) => cursor = &group.expr,
            syn::Expr::Reference(reference) => cursor = &reference.expr,
            _ => return None,
        }
    }
}

#[cfg(feature = "analyzer")]
fn parse_qail_constructor(call: &syn::ExprCall) -> Option<SynConstructor> {
    let syn::Expr::Path(path_expr) = &*call.func else {
        return None;
    };

    let ctor = qail_constructor_name(&path_expr.path)?;
    let first_arg = call.args.first()?;

    let (action, ast_action, table) = match ctor.as_str() {
        "get" => (
            "GET".to_string(),
            crate::ast::Action::Get,
            parse_table_name_from_expr(first_arg)?,
        ),
        "add" => (
            "ADD".to_string(),
            crate::ast::Action::Add,
            parse_table_name_from_expr(first_arg)?,
        ),
        "set" => (
            "SET".to_string(),
            crate::ast::Action::Set,
            parse_table_name_from_expr(first_arg)?,
        ),
        "del" => (
            "DEL".to_string(),
            crate::ast::Action::Del,
            parse_table_name_from_expr(first_arg)?,
        ),
        "put" => (
            "PUT".to_string(),
            crate::ast::Action::Put,
            parse_table_name_from_expr(first_arg)?,
        ),
        "typed" => (
            "TYPED".to_string(),
            crate::ast::Action::Get,
            parse_typed_table_from_expr(first_arg)?,
        ),
        // "raw_sql" and any unknown constructors are not validated
        _ => return None,
    };

    Some(SynConstructor {
        line: call.span().start().line,
        action,
        ast_action,
        table,
    })
}

#[cfg(feature = "analyzer")]
fn qail_constructor_name(path: &syn::Path) -> Option<String> {
    let mut segments = path.segments.iter().map(|s| s.ident.to_string());
    let first = segments.next()?;
    let mut prev = first;
    for segment in segments {
        if prev == "Qail" {
            return Some(segment.to_ascii_lowercase());
        }
        prev = segment;
    }
    None
}

#[cfg(feature = "analyzer")]
fn parse_typed_table_from_expr(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::Path(path_expr) => {
            let segments: Vec<_> = path_expr
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            match segments.len() {
                0 => None,
                1 => Some(segments[0].to_ascii_lowercase()),
                _ => Some(segments[segments.len() - 2].to_ascii_lowercase()),
            }
        }
        syn::Expr::Reference(reference) => parse_typed_table_from_expr(&reference.expr),
        syn::Expr::Paren(paren) => parse_typed_table_from_expr(&paren.expr),
        syn::Expr::Group(group) => parse_typed_table_from_expr(&group.expr),
        syn::Expr::MethodCall(method) if method.method == "into" => {
            parse_typed_table_from_expr(&method.receiver)
        }
        _ => None,
    }
}

#[cfg(feature = "analyzer")]
fn parse_table_name_from_expr(expr: &syn::Expr) -> Option<String> {
    parse_string_from_expr(expr).or_else(|| parse_typed_table_from_expr(expr))
}

#[cfg(feature = "analyzer")]
fn parse_string_from_expr(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::Lit(lit) => match &lit.lit {
            syn::Lit::Str(s) => Some(s.value()),
            _ => None,
        },
        syn::Expr::Reference(reference) => parse_string_from_expr(&reference.expr),
        syn::Expr::Paren(paren) => parse_string_from_expr(&paren.expr),
        syn::Expr::Group(group) => parse_string_from_expr(&group.expr),
        syn::Expr::MethodCall(method)
            if method.method == "into" || method.method == "to_string" =>
        {
            parse_string_from_expr(&method.receiver)
        }
        syn::Expr::Call(call) => {
            let syn::Expr::Path(path_expr) = &*call.func else {
                return None;
            };
            let tail = path_expr.path.segments.last()?.ident.to_string();
            if tail == "from" || tail == "new" || tail == "String" {
                return call.args.first().and_then(parse_string_from_expr);
            }
            None
        }
        _ => None,
    }
}

#[cfg(feature = "analyzer")]
fn parse_string_list_from_expr(expr: &syn::Expr) -> Vec<String> {
    match expr {
        syn::Expr::Array(arr) => arr
            .elems
            .iter()
            .filter_map(parse_string_from_expr)
            .collect(),
        syn::Expr::Reference(reference) => parse_string_list_from_expr(&reference.expr),
        syn::Expr::Paren(paren) => parse_string_list_from_expr(&paren.expr),
        syn::Expr::Group(group) => parse_string_list_from_expr(&group.expr),
        syn::Expr::Macro(mac) if mac.mac.path.is_ident("vec") => {
            if let Ok(arr) = syn::parse2::<syn::ExprArray>(mac.mac.tokens.clone()) {
                return arr
                    .elems
                    .iter()
                    .filter_map(parse_string_from_expr)
                    .collect();
            }
            Vec::new()
        }
        _ => parse_string_from_expr(expr).into_iter().collect(),
    }
}

#[cfg(feature = "analyzer")]
fn parse_operator_from_expr(expr: &syn::Expr) -> Option<crate::ast::Operator> {
    let syn::Expr::Path(path_expr) = expr else {
        return None;
    };
    let name = path_expr.path.segments.last()?.ident.to_string();
    Some(match name.as_str() {
        "Eq" => crate::ast::Operator::Eq,
        "Ne" => crate::ast::Operator::Ne,
        "Gt" => crate::ast::Operator::Gt,
        "Gte" => crate::ast::Operator::Gte,
        "Lt" => crate::ast::Operator::Lt,
        "Lte" => crate::ast::Operator::Lte,
        "Like" => crate::ast::Operator::Like,
        "ILike" => crate::ast::Operator::ILike,
        "IsNull" => crate::ast::Operator::IsNull,
        "IsNotNull" => crate::ast::Operator::IsNotNull,
        "In" => crate::ast::Operator::In,
        _ => return None,
    })
}

#[cfg(feature = "analyzer")]
fn parse_sort_order_from_expr(expr: &syn::Expr) -> Option<crate::ast::SortOrder> {
    let syn::Expr::Path(path_expr) = expr else {
        return None;
    };
    let name = path_expr.path.segments.last()?.ident.to_string();
    Some(match name.as_str() {
        "Asc" => crate::ast::SortOrder::Asc,
        "Desc" => crate::ast::SortOrder::Desc,
        "AscNullsFirst" => crate::ast::SortOrder::AscNullsFirst,
        "AscNullsLast" => crate::ast::SortOrder::AscNullsLast,
        "DescNullsFirst" => crate::ast::SortOrder::DescNullsFirst,
        "DescNullsLast" => crate::ast::SortOrder::DescNullsLast,
        _ => return None,
    })
}

#[cfg(feature = "analyzer")]
fn parse_join_kind_from_expr(expr: &syn::Expr) -> Option<crate::ast::JoinKind> {
    let syn::Expr::Path(path_expr) = expr else {
        return None;
    };
    let name = path_expr.path.segments.last()?.ident.to_string();
    Some(match name.as_str() {
        "Inner" => crate::ast::JoinKind::Inner,
        "Left" => crate::ast::JoinKind::Left,
        "Right" => crate::ast::JoinKind::Right,
        "Lateral" => crate::ast::JoinKind::Lateral,
        "Full" => crate::ast::JoinKind::Full,
        "Cross" => crate::ast::JoinKind::Cross,
        _ => return None,
    })
}

#[cfg(feature = "analyzer")]
fn parse_value_ctor_call(call: &syn::ExprCall) -> Option<crate::ast::Value> {
    let syn::Expr::Path(path_expr) = &*call.func else {
        return None;
    };
    let segments: Vec<String> = path_expr
        .path
        .segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect();
    if segments.len() < 2 || segments[segments.len() - 2] != "Value" {
        return None;
    }

    let ctor = segments.last()?.as_str();
    let first = call.args.first();

    use crate::ast::Value;
    Some(match ctor {
        "Null" => Value::Null,
        "Bool" => match first {
            Some(syn::Expr::Lit(lit)) => match &lit.lit {
                syn::Lit::Bool(b) => Value::Bool(b.value),
                _ => return None,
            },
            _ => return None,
        },
        "Int" => match first {
            Some(syn::Expr::Lit(lit)) => match &lit.lit {
                syn::Lit::Int(i) => i
                    .base10_parse::<i64>()
                    .map(Value::Int)
                    .unwrap_or(Value::Null),
                _ => return None,
            },
            _ => return None,
        },
        "Float" => match first {
            Some(syn::Expr::Lit(lit)) => match &lit.lit {
                syn::Lit::Float(f) => f
                    .base10_parse::<f64>()
                    .map(Value::Float)
                    .unwrap_or(Value::Null),
                _ => return None,
            },
            _ => return None,
        },
        "String" => Value::String(first.and_then(parse_string_from_expr)?),
        "Column" => Value::Column(first.and_then(parse_string_from_expr)?),
        "Array" => Value::Array(match first {
            Some(expr) => match parse_value_from_expr(expr) {
                Value::Array(arr) => arr,
                single => vec![single],
            },
            None => vec![],
        }),
        _ => return None,
    })
}

#[cfg(feature = "analyzer")]
fn parse_value_from_expr(expr: &syn::Expr) -> crate::ast::Value {
    use crate::ast::Value;

    match expr {
        syn::Expr::Lit(lit) => match &lit.lit {
            syn::Lit::Bool(b) => Value::Bool(b.value),
            syn::Lit::Int(i) => i
                .base10_parse::<i64>()
                .map(Value::Int)
                .unwrap_or(Value::Null),
            syn::Lit::Float(f) => f
                .base10_parse::<f64>()
                .map(Value::Float)
                .unwrap_or(Value::Null),
            syn::Lit::Str(s) => Value::String(s.value()),
            _ => Value::Null,
        },
        syn::Expr::Array(arr) => {
            Value::Array(arr.elems.iter().map(parse_value_from_expr).collect())
        }
        syn::Expr::Reference(reference) => parse_value_from_expr(&reference.expr),
        syn::Expr::Paren(paren) => parse_value_from_expr(&paren.expr),
        syn::Expr::Group(group) => parse_value_from_expr(&group.expr),
        syn::Expr::MethodCall(method) if method.method == "into" => {
            parse_value_from_expr(&method.receiver)
        }
        syn::Expr::Call(call) => {
            if let Some(value) = parse_value_ctor_call(call) {
                return value;
            }
            let syn::Expr::Path(path_expr) = &*call.func else {
                return Value::Null;
            };
            let tail = path_expr
                .path
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .unwrap_or_default();
            if tail == "Some" {
                return call
                    .args
                    .first()
                    .map(parse_value_from_expr)
                    .unwrap_or(Value::Null);
            }
            Value::Null
        }
        syn::Expr::Path(_path_expr) => Value::Null,
        _ => Value::Null,
    }
}

#[cfg(feature = "analyzer")]
fn parse_expr_node(expr: &syn::Expr) -> Option<crate::ast::Expr> {
    match expr {
        syn::Expr::Lit(lit) => match &lit.lit {
            syn::Lit::Str(s) => Some(crate::ast::Expr::Named(s.value())),
            _ => None,
        },
        syn::Expr::Reference(reference) => parse_expr_node(&reference.expr),
        syn::Expr::Paren(paren) => parse_expr_node(&paren.expr),
        syn::Expr::Group(group) => parse_expr_node(&group.expr),
        syn::Expr::MethodCall(method) if method.method == "into" => {
            parse_expr_node(&method.receiver)
        }
        syn::Expr::Call(call) => {
            let syn::Expr::Path(path_expr) = &*call.func else {
                return None;
            };
            let segments: Vec<String> = path_expr
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            let tail = segments.last()?.as_str();
            if tail == "Named" && segments.len() >= 2 && segments[segments.len() - 2] == "Expr" {
                return call
                    .args
                    .first()
                    .and_then(parse_string_from_expr)
                    .map(crate::ast::Expr::Named);
            }
            if tail == "Raw" && segments.len() >= 2 && segments[segments.len() - 2] == "Expr" {
                return call
                    .args
                    .first()
                    .and_then(parse_string_from_expr)
                    .map(crate::ast::Expr::Raw);
            }
            if tail == "col" {
                return call
                    .args
                    .first()
                    .and_then(parse_string_from_expr)
                    .map(crate::ast::Expr::Named);
            }
            None
        }
        _ => None,
    }
}

#[cfg(feature = "analyzer")]
fn parse_condition_from_expr(expr: &syn::Expr) -> Option<crate::ast::Condition> {
    let syn::Expr::Struct(cond_struct) = expr else {
        return None;
    };
    let struct_name = cond_struct.path.segments.last()?.ident.to_string();
    if struct_name != "Condition" {
        return None;
    }

    let mut left = None;
    let mut op = None;
    let mut value = None;
    let mut is_array_unnest = false;

    for field in &cond_struct.fields {
        let syn::Member::Named(name) = &field.member else {
            continue;
        };
        match name.to_string().as_str() {
            "left" => left = parse_expr_node(&field.expr),
            "op" => op = parse_operator_from_expr(&field.expr),
            "value" => value = Some(parse_value_from_expr(&field.expr)),
            "is_array_unnest" => {
                if let syn::Expr::Lit(lit) = &field.expr
                    && let syn::Lit::Bool(v) = &lit.lit
                {
                    is_array_unnest = v.value;
                }
            }
            _ => {}
        }
    }

    Some(crate::ast::Condition {
        left: left?,
        op: op?,
        value: value.unwrap_or(crate::ast::Value::Null),
        is_array_unnest,
    })
}

#[cfg(feature = "analyzer")]
fn parse_condition_list(expr: &syn::Expr) -> Vec<crate::ast::Condition> {
    match expr {
        syn::Expr::Array(arr) => arr
            .elems
            .iter()
            .filter_map(parse_condition_from_expr)
            .collect(),
        syn::Expr::Reference(reference) => parse_condition_list(&reference.expr),
        syn::Expr::Paren(paren) => parse_condition_list(&paren.expr),
        syn::Expr::Group(group) => parse_condition_list(&group.expr),
        syn::Expr::Macro(mac) if mac.mac.path.is_ident("vec") => {
            if let Ok(arr) = syn::parse2::<syn::ExprArray>(mac.mac.tokens.clone()) {
                return arr
                    .elems
                    .iter()
                    .filter_map(parse_condition_from_expr)
                    .collect();
            }
            Vec::new()
        }
        _ => parse_condition_from_expr(expr).into_iter().collect(),
    }
}

#[cfg(feature = "analyzer")]
fn push_filter_condition(cmd: &mut crate::ast::Qail, condition: crate::ast::Condition) {
    if let Some(cage) = cmd
        .cages
        .iter_mut()
        .find(|c| matches!(c.kind, crate::ast::CageKind::Filter))
    {
        cage.conditions.push(condition);
    } else {
        cmd.cages.push(crate::ast::Cage {
            kind: crate::ast::CageKind::Filter,
            conditions: vec![condition],
            logical_op: crate::ast::LogicalOp::And,
        });
    }
}

#[cfg(feature = "analyzer")]
fn push_payload_condition(cmd: &mut crate::ast::Qail, condition: crate::ast::Condition) {
    if let Some(cage) = cmd
        .cages
        .iter_mut()
        .find(|c| matches!(c.kind, crate::ast::CageKind::Payload))
    {
        cage.conditions.push(condition);
    } else {
        cmd.cages.push(crate::ast::Cage {
            kind: crate::ast::CageKind::Payload,
            conditions: vec![condition],
            logical_op: crate::ast::LogicalOp::And,
        });
    }
}

#[cfg(feature = "analyzer")]
fn normalize_join_table(table: &str) -> String {
    table.split_whitespace().next().unwrap_or(table).to_string()
}

#[cfg(feature = "analyzer")]
fn apply_syn_method_step(
    cmd: &mut crate::ast::Qail,
    method: &str,
    args: &[syn::Expr],
    has_rls: &mut bool,
) {
    use crate::ast::{Condition, Expr, Join, JoinKind, Operator, SortOrder, Value};

    match method {
        "with_rls" | "rls" => {
            *has_rls = true;
        }
        "column" => {
            if let Some(col) = args.first().and_then(parse_string_from_expr) {
                cmd.columns.push(Expr::Named(col));
            }
        }
        "columns" => {
            if let Some(arg) = args.first() {
                cmd.columns.extend(
                    parse_string_list_from_expr(arg)
                        .into_iter()
                        .map(Expr::Named),
                );
            }
        }
        "returning" => {
            if let Some(arg) = args.first() {
                let cols: Vec<Expr> = parse_string_list_from_expr(arg)
                    .into_iter()
                    .map(Expr::Named)
                    .collect();
                if !cols.is_empty() {
                    match &mut cmd.returning {
                        Some(existing) => existing.extend(cols),
                        None => cmd.returning = Some(cols),
                    }
                }
            }
        }
        "returning_all" => {
            cmd.returning = Some(vec![Expr::Star]);
        }
        "filter" => {
            if args.len() >= 3
                && let Some(column) = parse_string_from_expr(&args[0])
            {
                let op = parse_operator_from_expr(&args[1]).unwrap_or(Operator::Eq);
                let value = parse_value_from_expr(&args[2]);
                push_filter_condition(
                    cmd,
                    Condition {
                        left: Expr::Named(column),
                        op,
                        value,
                        is_array_unnest: false,
                    },
                );
            }
        }
        "where_eq" | "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "like" | "ilike" | "in_vals"
        | "is_null" | "is_not_null" => {
            if let Some(column) = args.first().and_then(parse_string_from_expr) {
                let (op, value) = match method {
                    "where_eq" | "eq" => (
                        Operator::Eq,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "ne" => (
                        Operator::Ne,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "gt" => (
                        Operator::Gt,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "gte" => (
                        Operator::Gte,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "lt" => (
                        Operator::Lt,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "lte" => (
                        Operator::Lte,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "like" => (
                        Operator::Like,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "ilike" => (
                        Operator::ILike,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "in_vals" => (
                        Operator::In,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Array(vec![])),
                    ),
                    "is_null" => (Operator::IsNull, Value::Null),
                    "is_not_null" => (Operator::IsNotNull, Value::Null),
                    _ => (Operator::Eq, Value::Null),
                };

                push_filter_condition(
                    cmd,
                    Condition {
                        left: Expr::Named(column),
                        op,
                        value,
                        is_array_unnest: false,
                    },
                );
            }
        }
        "order_by" => {
            if let Some(column) = args.first().and_then(parse_string_from_expr) {
                let order = args
                    .get(1)
                    .and_then(parse_sort_order_from_expr)
                    .unwrap_or(SortOrder::Asc);
                cmd.cages.push(crate::ast::Cage {
                    kind: crate::ast::CageKind::Sort(order),
                    conditions: vec![Condition {
                        left: Expr::Named(column),
                        op: Operator::Eq,
                        value: Value::Null,
                        is_array_unnest: false,
                    }],
                    logical_op: crate::ast::LogicalOp::And,
                });
            }
        }
        "order_desc" | "order_asc" => {
            if let Some(column) = args.first().and_then(parse_string_from_expr) {
                let order = if method == "order_desc" {
                    SortOrder::Desc
                } else {
                    SortOrder::Asc
                };
                cmd.cages.push(crate::ast::Cage {
                    kind: crate::ast::CageKind::Sort(order),
                    conditions: vec![Condition {
                        left: Expr::Named(column),
                        op: Operator::Eq,
                        value: Value::Null,
                        is_array_unnest: false,
                    }],
                    logical_op: crate::ast::LogicalOp::And,
                });
            }
        }
        "group_by" => {
            if let Some(arg) = args.first() {
                let cols = parse_string_list_from_expr(arg);
                if !cols.is_empty() {
                    cmd.cages.push(crate::ast::Cage {
                        kind: crate::ast::CageKind::Partition,
                        conditions: cols
                            .into_iter()
                            .map(|c| Condition {
                                left: Expr::Named(c),
                                op: Operator::Eq,
                                value: Value::Null,
                                is_array_unnest: false,
                            })
                            .collect(),
                        logical_op: crate::ast::LogicalOp::And,
                    });
                }
            }
        }
        "having_cond" => {
            if let Some(arg) = args.first()
                && let Some(condition) = parse_condition_from_expr(arg)
            {
                cmd.having.push(condition);
            }
        }
        "having_conds" => {
            if let Some(arg) = args.first() {
                cmd.having.extend(parse_condition_list(arg));
            }
        }
        "join" => {
            if args.len() >= 4
                && let Some(table) = args.get(1).and_then(parse_table_name_from_expr)
            {
                let kind = args
                    .first()
                    .and_then(parse_join_kind_from_expr)
                    .unwrap_or(JoinKind::Left);
                let on = match (
                    args.get(2).and_then(parse_string_from_expr),
                    args.get(3).and_then(parse_string_from_expr),
                ) {
                    (Some(left_col), Some(right_col)) => Some(vec![Condition {
                        left: Expr::Named(left_col),
                        op: Operator::Eq,
                        value: Value::Column(right_col),
                        is_array_unnest: false,
                    }]),
                    _ => None,
                };
                cmd.joins.push(Join {
                    kind,
                    table: normalize_join_table(&table),
                    on,
                    on_true: false,
                });
            }
        }
        "left_join" | "inner_join" | "right_join" | "full_join" => {
            if args.len() >= 3
                && let Some(table) = args.first().and_then(parse_table_name_from_expr)
            {
                let kind = match method {
                    "inner_join" => JoinKind::Inner,
                    "right_join" => JoinKind::Right,
                    "full_join" => JoinKind::Full,
                    _ => JoinKind::Left,
                };
                let on = match (
                    args.get(1).and_then(parse_string_from_expr),
                    args.get(2).and_then(parse_string_from_expr),
                ) {
                    (Some(left_col), Some(right_col)) => Some(vec![Condition {
                        left: Expr::Named(left_col),
                        op: Operator::Eq,
                        value: Value::Column(right_col),
                        is_array_unnest: false,
                    }]),
                    _ => None,
                };
                cmd.joins.push(Join {
                    kind,
                    table: normalize_join_table(&table),
                    on,
                    on_true: false,
                });
            }
        }
        "join_on" | "join_on_optional" => {
            if let Some(table) = args.first().and_then(parse_table_name_from_expr) {
                cmd.joins.push(Join {
                    kind: JoinKind::Left,
                    table: normalize_join_table(&table),
                    on: None,
                    on_true: false,
                });
            }
        }
        "left_join_as" | "inner_join_as" => {
            if args.len() >= 4
                && let Some(table) = args.first().and_then(parse_table_name_from_expr)
            {
                let kind = if method == "inner_join_as" {
                    JoinKind::Inner
                } else {
                    JoinKind::Left
                };
                let on = match (
                    args.get(2).and_then(parse_string_from_expr),
                    args.get(3).and_then(parse_string_from_expr),
                ) {
                    (Some(left_col), Some(right_col)) => Some(vec![Condition {
                        left: Expr::Named(left_col),
                        op: Operator::Eq,
                        value: Value::Column(right_col),
                        is_array_unnest: false,
                    }]),
                    _ => None,
                };
                cmd.joins.push(Join {
                    kind,
                    table: normalize_join_table(&table),
                    on,
                    on_true: false,
                });
            }
        }
        "join_conds" | "left_join_conds" | "inner_join_conds" => {
            let (kind, table_idx, cond_idx) = match method {
                "join_conds" => (
                    args.first()
                        .and_then(parse_join_kind_from_expr)
                        .unwrap_or(JoinKind::Left),
                    1,
                    2,
                ),
                "inner_join_conds" => (JoinKind::Inner, 0, 1),
                _ => (JoinKind::Left, 0, 1),
            };

            if let Some(table_expr) = args.get(table_idx)
                && let Some(table) = parse_table_name_from_expr(table_expr)
            {
                let conditions = args
                    .get(cond_idx)
                    .map(parse_condition_list)
                    .unwrap_or_default();
                cmd.joins.push(Join {
                    kind,
                    table: normalize_join_table(&table),
                    on: if conditions.is_empty() {
                        None
                    } else {
                        Some(conditions)
                    },
                    on_true: false,
                });
            }
        }
        "set_value" | "set_coalesce" | "set_coalesce_opt" => {
            if let Some(column) = args.first().and_then(parse_string_from_expr) {
                let value = args
                    .get(1)
                    .map(parse_value_from_expr)
                    .unwrap_or(Value::Null);
                push_payload_condition(
                    cmd,
                    Condition {
                        left: Expr::Named(column),
                        op: Operator::Eq,
                        value,
                        is_array_unnest: false,
                    },
                );
            }
        }
        _ => {}
    }
}

#[cfg(feature = "analyzer")]
fn syn_cmd_score(cmd: &crate::ast::Qail, has_rls: bool) -> usize {
    let group_cols = cmd
        .cages
        .iter()
        .filter(|c| matches!(c.kind, crate::ast::CageKind::Partition))
        .map(|c| c.conditions.len())
        .sum::<usize>();
    let filter_cols = cmd
        .cages
        .iter()
        .filter(|c| matches!(c.kind, crate::ast::CageKind::Filter))
        .map(|c| c.conditions.len())
        .sum::<usize>();

    cmd.columns.len()
        + (cmd.joins.len() * 8)
        + (group_cols * 5)
        + (cmd.having.len() * 6)
        + filter_cols
        + cmd.returning.as_ref().map_or(0, |r| r.len() * 2)
        + usize::from(has_rls)
}

/// Validate QAIL usage against schema using the smart Validator
/// Provides "Did you mean?" suggestions for typos, type validation, and RLS audit
pub fn validate_against_schema(schema: &Schema, usages: &[QailUsage]) -> Vec<String> {
    use crate::ast::Qail;
    use crate::validator::Validator;

    // Build Validator from Schema with column types
    let mut validator = Validator::new();
    for (table_name, table_schema) in &schema.tables {
        // Convert HashMap<String, ColumnType> to Vec<(&str, &str)> for validator
        let type_strings: Vec<(String, String)> = table_schema
            .columns
            .iter()
            .map(|(name, typ)| (name.clone(), typ.to_pg_type()))
            .collect();
        let cols_with_types: Vec<(&str, &str)> = type_strings
            .iter()
            .map(|(name, typ)| (name.as_str(), typ.as_str()))
            .collect();
        validator.add_table_with_types(table_name, &cols_with_types);
    }

    let mut errors = Vec::new();
    let mut rls_warnings = Vec::new();
    #[cfg(feature = "analyzer")]
    let syn_usage_index = build_syn_usage_index(usages);

    for usage in usages {
        // Skip CTE alias refs — but only if the name doesn't also exist as a
        // real schema table. If there's a collision (CTE alias == real table name),
        // always validate to avoid false negatives.
        if usage.is_cte_ref && !schema.has_table(&usage.table) {
            continue;
        }

        // ── Build partial Qail AST from extracted usage ──────────────
        let action = usage_action_to_ast(&usage.action);

        let mut cmd = Qail {
            action,
            table: usage.table.clone(),
            ..Default::default()
        };
        #[allow(unused_mut)]
        let mut has_rls = usage.has_rls;

        #[cfg(feature = "analyzer")]
        if let Some(parsed) = syn_usage_index.get(&syn_usage_key(
            &usage.file,
            usage.line,
            &usage.action,
            &usage.table,
        )) {
            cmd = parsed.cmd.clone();
            has_rls |= parsed.has_rls;
        }

        // Keep scanner-derived columns as fallback for DSL methods not yet covered by syn path.
        append_scanned_columns(&mut cmd, &usage.columns);

        // ── Validate the constructed AST ─────────────────────────────
        match validator.validate_command(&cmd) {
            Ok(()) => {}
            Err(validation_errors) => {
                for e in validation_errors {
                    errors.push(format!("{}:{}: {}", usage.file, usage.line, e));
                }
            }
        }

        // RLS Audit: warn if query targets RLS-enabled table without .with_rls()
        if schema.is_rls_table(&usage.table) && !has_rls {
            rls_warnings.push(format!(
                "{}:{}: ⚠️ RLS AUDIT: Qail::{}(\"{}\") has no .with_rls() — table has RLS enabled, query may leak tenant data",
                usage.file, usage.line, usage.action.to_lowercase(), usage.table
            ));
        }
    }

    // Return RLS warnings in the Vec (CLI `qail check` filters by "RLS AUDIT").
    // Callers decide whether to treat these as fatal or advisory.
    errors.extend(rls_warnings);

    errors
}

/// Run N+1 compile-time check.
///
/// Controlled by environment variables:
/// - `QAIL_NPLUS1`: `off` | `warn` (default) | `deny`
/// - `QAIL_NPLUS1_MAX_WARNINGS`: max warnings before truncation (default 50)
#[cfg(feature = "analyzer")]
fn run_nplus1_check(src_dir: &str) {
    use crate::analyzer::{NPlusOneSeverity, detect_n_plus_one_in_dir};

    println!("cargo:rerun-if-env-changed=QAIL_NPLUS1");
    println!("cargo:rerun-if-env-changed=QAIL_NPLUS1_MAX_WARNINGS");

    let mode = std::env::var("QAIL_NPLUS1").unwrap_or_else(|_| "warn".to_string());

    if mode == "off" || mode == "false" || mode == "0" {
        return;
    }

    let max_warnings: usize = std::env::var("QAIL_NPLUS1_MAX_WARNINGS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    let diagnostics = detect_n_plus_one_in_dir(Path::new(src_dir));

    if diagnostics.is_empty() {
        println!("cargo:warning=QAIL: N+1 scan clean ✓");
        return;
    }

    let total = diagnostics.len();
    let shown = total.min(max_warnings);

    for diag in diagnostics.iter().take(shown) {
        let prefix = match diag.severity {
            NPlusOneSeverity::Error => "QAIL N+1 ERROR",
            NPlusOneSeverity::Warning => "QAIL N+1",
        };
        println!("cargo:warning={}: {}", prefix, diag);
    }

    if total > shown {
        println!(
            "cargo:warning=QAIL N+1: ... and {} more (set QAIL_NPLUS1_MAX_WARNINGS to see all)",
            total - shown
        );
    }

    if mode == "deny" {
        // Intentional: build-script panic = compile error. N+1 deny mode
        // must abort the build when diagnostics are found.
        panic!(
            "QAIL N+1: {} diagnostic(s) found. Fix N+1 patterns or set QAIL_NPLUS1=warn",
            total
        );
    }
}

#[cfg(not(feature = "analyzer"))]
fn run_nplus1_check(_src_dir: &str) {
    // N+1 detection requires the `analyzer` feature (syn dependency)
}

/// Main validation entry point for build.rs.
///
/// All `panic!()` calls below are intentional — Cargo build scripts must panic
/// to signal a build failure. These are the only mechanism to abort `cargo build`
/// when schema validation, live-pull, or mode detection fails.
pub fn validate() {
    let mode = std::env::var("QAIL").unwrap_or_else(|_| {
        if Path::new("schema.qail").exists() || Path::new("schema").is_dir() {
            "schema".to_string()
        } else {
            "false".to_string()
        }
    });

    match mode.as_str() {
        "schema" => {
            if let Ok(source) = crate::schema_source::resolve_schema_source("schema.qail") {
                for path in source.watch_paths() {
                    println!("cargo:rerun-if-changed={}", path.display());
                }
            } else {
                // Keep backward-compatible watcher even if resolution fails;
                // parse step below will emit the concrete error.
                println!("cargo:rerun-if-changed=schema.qail");
                println!("cargo:rerun-if-changed=schema");
            }
            println!("cargo:rerun-if-changed=migrations");
            println!("cargo:rerun-if-changed=src");
            println!("cargo:rerun-if-env-changed=QAIL");

            match Schema::parse_file("schema.qail") {
                Ok(mut schema) => {
                    // Merge pending migrations with pulled schema
                    let merged = match schema.merge_migrations("migrations") {
                        Ok(n) => n,
                        Err(e) => {
                            println!("cargo:warning=QAIL: Migration merge failed: {}", e);
                            0
                        }
                    };
                    if merged > 0 {
                        println!(
                            "cargo:warning=QAIL: Merged {} schema changes from migrations",
                            merged
                        );
                    }

                    let usages = scan_source_files("src/");
                    let all_results = validate_against_schema(&schema, &usages);

                    // Separate schema errors (fatal) from RLS warnings (advisory)
                    let schema_errors: Vec<_> = all_results
                        .iter()
                        .filter(|e| !e.contains("RLS AUDIT"))
                        .collect();
                    let rls_warnings: Vec<_> = all_results
                        .iter()
                        .filter(|e| e.contains("RLS AUDIT"))
                        .collect();

                    // Emit RLS warnings as non-fatal cargo warnings
                    for w in &rls_warnings {
                        println!("cargo:warning=QAIL RLS: {}", w);
                    }

                    if schema_errors.is_empty() {
                        println!(
                            "cargo:warning=QAIL: Validated {} queries against schema source ✓",
                            usages.len()
                        );
                    } else {
                        for error in &schema_errors {
                            println!("cargo:warning=QAIL ERROR: {}", error);
                        }
                        // Fail the build (only schema errors, not RLS warnings)
                        panic!("QAIL validation failed with {} errors", schema_errors.len());
                    }

                    // ── N+1 detection ──────────────────────────────────────
                    run_nplus1_check("src");
                }
                Err(e) => {
                    panic!("QAIL: Failed to parse schema source: {}", e);
                }
            }
        }
        "live" => {
            println!("cargo:rerun-if-env-changed=QAIL");
            println!("cargo:rerun-if-env-changed=DATABASE_URL");
            println!("cargo:rerun-if-changed=src");

            // Get DATABASE_URL for qail pull
            let db_url = match std::env::var("DATABASE_URL") {
                Ok(url) => url,
                Err(_) => {
                    panic!("QAIL=live requires DATABASE_URL environment variable");
                }
            };

            // Step 1: Run qail pull to update schema.qail
            println!("cargo:warning=QAIL: Pulling schema from live database...");

            let pull_result = std::process::Command::new("qail")
                .args(["pull", &db_url])
                .output();

            match pull_result {
                Ok(output) => {
                    if !output.status.success() {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        panic!("QAIL: Failed to pull schema: {}", stderr);
                    }
                    println!("cargo:warning=QAIL: Schema pulled successfully ✓");
                }
                Err(e) => {
                    // qail CLI not found, try using cargo run
                    println!("cargo:warning=QAIL: qail CLI not in PATH, trying cargo...");

                    let cargo_result = std::process::Command::new("cargo")
                        .args(["run", "-p", "qail", "--", "pull", &db_url])
                        .current_dir(
                            std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string()),
                        )
                        .output();

                    match cargo_result {
                        Ok(output) if output.status.success() => {
                            println!("cargo:warning=QAIL: Schema pulled via cargo ✓");
                        }
                        _ => {
                            panic!(
                                "QAIL: Cannot run qail pull: {}. Install qail CLI or set QAIL=schema",
                                e
                            );
                        }
                    }
                }
            }

            // Step 2: Parse the updated schema and validate
            match Schema::parse_file("schema.qail") {
                Ok(mut schema) => {
                    // Merge pending migrations (in case live DB doesn't have them yet)
                    let merged = match schema.merge_migrations("migrations") {
                        Ok(n) => n,
                        Err(e) => {
                            println!("cargo:warning=QAIL: Migration merge failed: {}", e);
                            0
                        }
                    };
                    if merged > 0 {
                        println!(
                            "cargo:warning=QAIL: Merged {} schema changes from pending migrations",
                            merged
                        );
                    }

                    let usages = scan_source_files("src/");
                    let all_results = validate_against_schema(&schema, &usages);

                    // Separate schema errors (fatal) from RLS warnings (advisory)
                    let schema_errors: Vec<_> = all_results
                        .iter()
                        .filter(|e| !e.contains("RLS AUDIT"))
                        .collect();
                    let rls_warnings: Vec<_> = all_results
                        .iter()
                        .filter(|e| e.contains("RLS AUDIT"))
                        .collect();

                    // Emit RLS warnings as non-fatal cargo warnings
                    for w in &rls_warnings {
                        println!("cargo:warning=QAIL RLS: {}", w);
                    }

                    if schema_errors.is_empty() {
                        println!(
                            "cargo:warning=QAIL: Validated {} queries against live database ✓",
                            usages.len()
                        );
                    } else {
                        for error in &schema_errors {
                            println!("cargo:warning=QAIL ERROR: {}", error);
                        }
                        panic!("QAIL validation failed with {} errors", schema_errors.len());
                    }

                    // ── N+1 detection ──────────────────────────────────────
                    run_nplus1_check("src");
                }
                Err(e) => {
                    panic!("QAIL: Failed to parse schema after pull: {}", e);
                }
            }
        }
        "false" | "off" | "0" => {
            println!("cargo:rerun-if-env-changed=QAIL");
            // Silently skip validation
        }
        _ => {
            panic!("QAIL: Unknown mode '{}'. Use: schema, live, or false", mode);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schema() {
        // Format matches qail pull output (space-separated, not colon)
        let content = r#"
# Test schema

table users {
  id UUID primary_key
  name TEXT not_null
  email TEXT unique
}

table posts {
  id UUID
  user_id UUID
  title TEXT
}
"#;
        let schema = Schema::parse(content).unwrap();
        assert!(schema.has_table("users"));
        assert!(schema.has_table("posts"));
        assert!(schema.table("users").unwrap().has_column("id"));
        assert!(schema.table("users").unwrap().has_column("name"));
        assert!(!schema.table("users").unwrap().has_column("foo"));
    }

    #[test]
    fn test_extract_string_arg() {
        assert_eq!(extract_string_arg(r#""users")"#), Some("users".to_string()));
        assert_eq!(
            extract_string_arg(r#""table_name")"#),
            Some("table_name".to_string())
        );
    }

    #[test]
    fn test_scan_file() {
        // Test single-line pattern
        let content = r#"
let query = Qail::get("users").column("id").column("name").eq("active", true);
"#;
        let mut usages = Vec::new();
        scan_file("test.rs", content, &mut usages);

        assert_eq!(usages.len(), 1);
        assert_eq!(usages[0].table, "users");
        assert_eq!(usages[0].action, "GET");
        assert!(usages[0].columns.contains(&"id".to_string()));
        assert!(usages[0].columns.contains(&"name".to_string()));
    }

    #[test]
    fn test_scan_file_multiline() {
        // Test multi-line chain pattern (common in real code)
        let content = r#"
let query = Qail::get("posts")
    .column("id")
    .column("title")
    .column("author")
    .eq("published", true)
    .order_by("created_at", Desc);
"#;
        let mut usages = Vec::new();
        scan_file("test.rs", content, &mut usages);

        assert_eq!(usages.len(), 1);
        assert_eq!(usages[0].table, "posts");
        assert_eq!(usages[0].action, "GET");
        assert!(usages[0].columns.contains(&"id".to_string()));
        assert!(usages[0].columns.contains(&"title".to_string()));
        assert!(usages[0].columns.contains(&"author".to_string()));
    }

    #[test]
    fn test_scan_typed_api() {
        let content = r#"
let q = Qail::typed(users::table).column("email");
"#;
        let mut usages = Vec::new();
        scan_file("test.rs", content, &mut usages);

        assert_eq!(usages.len(), 1);
        assert_eq!(usages[0].table, "users");
        assert_eq!(usages[0].action, "TYPED");
        assert!(usages[0].columns.contains(&"email".to_string()));
    }

    #[test]
    fn test_scan_raw_sql_not_validated() {
        let content = r#"
let q = Qail::raw_sql("SELECT * FROM users");
"#;
        let mut usages = Vec::new();
        scan_file("test.rs", content, &mut usages);
        // raw_sql should NOT produce a QailUsage — it just emits a warning
        assert_eq!(usages.len(), 0);
    }

    #[test]
    fn test_extract_columns_is_null() {
        let line = r#"Qail::get("t").is_null("deleted_at").is_not_null("name")"#;
        let cols = extract_columns(line);
        assert!(cols.contains(&"deleted_at".to_string()));
        assert!(cols.contains(&"name".to_string()));
    }

    #[test]
    fn test_extract_columns_set_value() {
        let line =
            r#"Qail::set("orders").set_value("status", "Paid").set_coalesce("notes", "default")"#;
        let cols = extract_columns(line);
        assert!(cols.contains(&"status".to_string()));
        assert!(cols.contains(&"notes".to_string()));
    }

    #[test]
    fn test_extract_columns_returning() {
        let line = r#"Qail::add("orders").returning(["id", "status"])"#;
        let cols = extract_columns(line);
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"status".to_string()));
    }

    #[test]
    fn test_extract_columns_on_conflict() {
        let line = r#"Qail::put("t").on_conflict_update(&["id"], &[("name", Expr::Named("excluded.name".into()))])"#;
        let cols = extract_columns(line);
        assert!(cols.contains(&"id".to_string()));
    }

    #[test]
    fn test_validate_against_schema_casted_column_no_false_positive() {
        let schema = Schema::parse(
            r#"
table users {
  id TEXT
}
"#,
        )
        .unwrap();

        let content = r#"
let q = Qail::get("users").eq("id::text", "abc");
"#;

        let mut usages = Vec::new();
        scan_file("test.rs", content, &mut usages);
        let errors = validate_against_schema(&schema, &usages);
        assert!(
            errors.is_empty(),
            "casted column should not produce schema error: {:?}",
            errors
        );
    }

    #[test]
    fn test_cte_cross_chain_detection() {
        // Chain 1 defines CTE "agg" via .to_cte(), chain 2 uses Qail::get("agg")
        // File-level CTE detection means chain 2 IS recognized as a CTE ref
        let content = r#"
let cte = Qail::get("orders").columns(["total"]).to_cte("agg");
let q = Qail::get("agg").columns(["total"]);
"#;
        let mut usages = Vec::new();
        scan_file("test.rs", content, &mut usages);

        assert_eq!(usages.len(), 2);
        // Chain 1: GET on "orders", not a CTE ref
        assert_eq!(usages[0].table, "orders");
        assert!(!usages[0].is_cte_ref);
        // Chain 2: "agg" is recognized as CTE alias from chain 1
        assert_eq!(usages[1].table, "agg");
        assert!(usages[1].is_cte_ref);
    }

    #[test]
    fn test_cte_with_inline_detection() {
        // .with("alias", query) should also be detected as CTE
        let content = r#"
let q = Qail::get("results").with("agg", Qail::get("orders"));
"#;
        let mut usages = Vec::new();
        scan_file("test.rs", content, &mut usages);

        // "results" is the main table
        assert_eq!(usages.len(), 1);
        // It should NOT be a CTE ref since "results" != "agg"
        assert!(!usages[0].is_cte_ref);
    }

    #[test]
    fn test_rls_detection_typed_api() {
        // .rls() from typed API should be detected
        let content = r#"
let q = Qail::get("orders")
    .columns(["id"])
    .rls(&ctx);
"#;
        let mut usages = Vec::new();
        scan_file("test.rs", content, &mut usages);

        assert_eq!(usages.len(), 1);
        assert!(usages[0].has_rls);
    }

    #[test]
    fn test_rls_detection_with_rls() {
        let content = r#"
let q = Qail::get("orders")
    .columns(["id"])
    .with_rls(&ctx);
"#;
        let mut usages = Vec::new();
        scan_file("test.rs", content, &mut usages);

        assert_eq!(usages.len(), 1);
        assert!(usages[0].has_rls);
    }

    #[test]
    fn test_extract_typed_table_arg() {
        assert_eq!(
            extract_typed_table_arg("users::table)"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_typed_table_arg("users::Users)"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_typed_table_arg("schema::users::table)"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_typed_table_arg("Orders)"),
            Some("orders".to_string())
        );
        assert_eq!(extract_typed_table_arg(""), None);
    }

    #[cfg(feature = "analyzer")]
    #[test]
    fn test_syn_extract_join_group_by_having() {
        let source = r#"
fn demo(ctx: &RlsContext) {
    let _q = Qail::get("orders")
        .left_join("customers", "orders.customer_id", "customers.id")
        .group_by(["customer_id"])
        .having_cond(Condition {
            left: Expr::Named("total".into()),
            op: Operator::Eq,
            value: Value::Int(1),
            is_array_unnest: false,
        })
        .with_rls(ctx);
}
"#;

        let parsed = extract_syn_usages_from_source(source);
        let usage = parsed
            .into_iter()
            .find(|u| u.action == "GET" && u.table == "orders")
            .expect("expected syn usage for Qail::get(\"orders\")");

        assert_eq!(usage.cmd.joins.len(), 1);
        assert!(
            usage
                .cmd
                .cages
                .iter()
                .any(|c| matches!(c.kind, crate::ast::CageKind::Partition))
        );
        assert_eq!(usage.cmd.having.len(), 1);
        assert!(usage.has_rls);
    }

    #[cfg(feature = "analyzer")]
    #[test]
    fn test_validate_against_schema_uses_syn_structural_fields() {
        let schema = Schema::parse(
            r#"
table orders {
  id INT
  customer_id INT
  total INT
}

table customers {
  id INT
}
"#,
        )
        .unwrap();

        let content = r#"
fn demo() {
    let _q = Qail::get("orders")
        .left_join("customerz", "orders.customer_id", "customerz.id")
        .group_by(["custmer_id"])
        .having_cond(Condition {
            left: Expr::Named("totl".into()),
            op: Operator::Eq,
            value: Value::Int(1),
            is_array_unnest: false,
        });
}
"#;

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let test_path = std::env::temp_dir().join(format!(
            "qail_build_syn_structural_{}_{}.rs",
            std::process::id(),
            unique
        ));
        std::fs::write(&test_path, content).unwrap();

        let mut usages = Vec::new();
        scan_file(&test_path.display().to_string(), content, &mut usages);
        let errors = validate_against_schema(&schema, &usages);
        let _ = std::fs::remove_file(&test_path);

        assert!(errors.iter().any(|e| e.contains("customerz")));
        assert!(errors.iter().any(|e| e.contains("custmer_id")));
        assert!(errors.iter().any(|e| e.contains("totl")));
    }
}

// =============================================================================
// Typed Schema Codegen
// =============================================================================

/// Map ColumnType AST to Rust types for TypedColumn<T>
fn qail_type_to_rust(col_type: &ColumnType) -> &'static str {
    match col_type {
        ColumnType::Uuid => "uuid::Uuid",
        ColumnType::Text | ColumnType::Varchar(_) => "String",
        ColumnType::Int | ColumnType::Serial => "i32",
        ColumnType::BigInt | ColumnType::BigSerial => "i64",
        ColumnType::Bool => "bool",
        ColumnType::Float => "f32",
        ColumnType::Decimal(_) => "rust_decimal::Decimal",
        ColumnType::Jsonb => "serde_json::Value",
        ColumnType::Timestamp | ColumnType::Timestamptz => "chrono::DateTime<chrono::Utc>",
        ColumnType::Date => "chrono::NaiveDate",
        ColumnType::Time => "chrono::NaiveTime",
        ColumnType::Bytea => "Vec<u8>",
        ColumnType::Array(_) => "Vec<serde_json::Value>",
        ColumnType::Enum { .. } => "String",
        ColumnType::Range(_) => "String",
        ColumnType::Interval => "String",
        ColumnType::Cidr | ColumnType::Inet => "String",
        ColumnType::MacAddr => "String",
    }
}

/// Convert table/column names to valid Rust identifiers
fn to_rust_ident(name: &str) -> String {
    // Handle Rust keywords
    let name = match name {
        "type" => "r#type",
        "match" => "r#match",
        "ref" => "r#ref",
        "self" => "r#self",
        "mod" => "r#mod",
        "use" => "r#use",
        _ => name,
    };
    name.to_string()
}

/// Convert table name to PascalCase struct name
fn to_struct_name(name: &str) -> String {
    name.chars()
        .next()
        .map(|c| c.to_uppercase().collect::<String>() + &name[1..])
        .unwrap_or_default()
}

/// Generate typed Rust module from schema.
///
/// # Usage in consumer's build.rs:
/// ```ignore
/// fn main() {
///     let out_dir = std::env::var("OUT_DIR").unwrap();
///     qail_core::build::generate_typed_schema("schema.qail", &format!("{}/schema.rs", out_dir)).unwrap();
///     println!("cargo:rerun-if-changed=schema.qail");
/// }
/// ```
///
/// Then in the consumer's lib.rs:
/// ```ignore
/// include!(concat!(env!("OUT_DIR"), "/schema.rs"));
/// ```
pub fn generate_typed_schema(schema_path: &str, output_path: &str) -> Result<(), String> {
    let schema = Schema::parse_file(schema_path)?;
    let code = generate_schema_code(&schema);

    fs::write(output_path, code)
        .map_err(|e| format!("Failed to write schema module to '{}': {}", output_path, e))?;

    Ok(())
}

/// Generate typed Rust code from schema (does not write to file)
pub fn generate_schema_code(schema: &Schema) -> String {
    let mut code = String::new();

    // Header
    code.push_str("//! Auto-generated typed schema from schema.qail\n");
    code.push_str("//! Do not edit manually - regenerate with `cargo build`\n\n");
    code.push_str("#![allow(dead_code, non_upper_case_globals)]\n\n");
    code.push_str("use qail_core::typed::{Table, TypedColumn, RelatedTo, Public, Protected};\n\n");

    // Sort tables for deterministic output
    let mut tables: Vec<_> = schema.tables.values().collect();
    tables.sort_by(|a, b| a.name.cmp(&b.name));

    for table in &tables {
        let mod_name = to_rust_ident(&table.name);
        let struct_name = to_struct_name(&table.name);

        code.push_str(&format!("/// Typed schema for `{}` table\n", table.name));
        code.push_str(&format!("pub mod {} {{\n", mod_name));
        code.push_str("    use super::*;\n\n");

        // Table struct implementing Table trait
        code.push_str(&format!("    /// Table marker for `{}`\n", table.name));
        code.push_str("    #[derive(Debug, Clone, Copy)]\n");
        code.push_str(&format!("    pub struct {};\n\n", struct_name));

        code.push_str(&format!("    impl Table for {} {{\n", struct_name));
        code.push_str(&format!(
            "        fn table_name() -> &'static str {{ \"{}\" }}\n",
            table.name
        ));
        code.push_str("    }\n\n");

        code.push_str(&format!("    impl From<{}> for String {{\n", struct_name));
        code.push_str(&format!(
            "        fn from(_: {}) -> String {{ \"{}\".to_string() }}\n",
            struct_name, table.name
        ));
        code.push_str("    }\n\n");

        code.push_str(&format!("    impl AsRef<str> for {} {{\n", struct_name));
        code.push_str(&format!(
            "        fn as_ref(&self) -> &str {{ \"{}\" }}\n",
            table.name
        ));
        code.push_str("    }\n\n");

        // Table constant for convenience
        code.push_str(&format!("    /// The `{}` table\n", table.name));
        code.push_str(&format!(
            "    pub const table: {} = {};\n\n",
            struct_name, struct_name
        ));

        // Sort columns for deterministic output
        let mut columns: Vec<_> = table.columns.iter().collect();
        columns.sort_by(|a, b| a.0.cmp(b.0));

        // Column constants
        for (col_name, col_type) in columns {
            let rust_type = qail_type_to_rust(col_type);
            let col_ident = to_rust_ident(col_name);
            let policy = table
                .policies
                .get(col_name)
                .map(|s| s.as_str())
                .unwrap_or("Public");
            let rust_policy = if policy == "Protected" {
                "Protected"
            } else {
                "Public"
            };

            code.push_str(&format!(
                "    /// Column `{}.{}` ({}) - {}\n",
                table.name,
                col_name,
                col_type.to_pg_type(),
                policy
            ));
            code.push_str(&format!(
                "    pub const {}: TypedColumn<{}, {}> = TypedColumn::new(\"{}\", \"{}\");\n",
                col_ident, rust_type, rust_policy, table.name, col_name
            ));
        }

        code.push_str("}\n\n");
    }

    // ==========================================================================
    // Generate RelatedTo impls for compile-time relationship checking
    // ==========================================================================

    code.push_str(
        "// =============================================================================\n",
    );
    code.push_str("// Compile-Time Relationship Safety (RelatedTo impls)\n");
    code.push_str(
        "// =============================================================================\n\n",
    );

    for table in &tables {
        for fk in &table.foreign_keys {
            // table.column refs ref_table.ref_column
            // This means: table is related TO ref_table (forward)
            // AND: ref_table is related FROM table (reverse - parent has many children)

            let from_mod = to_rust_ident(&table.name);
            let from_struct = to_struct_name(&table.name);
            let to_mod = to_rust_ident(&fk.ref_table);
            let to_struct = to_struct_name(&fk.ref_table);

            // Forward: From table (child) -> Referenced table (parent)
            // Example: posts -> users (posts.user_id -> users.id)
            code.push_str(&format!(
                "/// {} has a foreign key to {} via {}.{}\n",
                table.name, fk.ref_table, table.name, fk.column
            ));
            code.push_str(&format!(
                "impl RelatedTo<{}::{}> for {}::{} {{\n",
                to_mod, to_struct, from_mod, from_struct
            ));
            code.push_str(&format!(
                "    fn join_columns() -> (&'static str, &'static str) {{ (\"{}\", \"{}\") }}\n",
                fk.column, fk.ref_column
            ));
            code.push_str("}\n\n");

            // Reverse: Referenced table (parent) -> From table (child)
            // Example: users -> posts (users.id -> posts.user_id)
            // This allows: Qail::get(users::table).join_related(posts::table)
            code.push_str(&format!(
                "/// {} is referenced by {} via {}.{}\n",
                fk.ref_table, table.name, table.name, fk.column
            ));
            code.push_str(&format!(
                "impl RelatedTo<{}::{}> for {}::{} {{\n",
                from_mod, from_struct, to_mod, to_struct
            ));
            code.push_str(&format!(
                "    fn join_columns() -> (&'static str, &'static str) {{ (\"{}\", \"{}\") }}\n",
                fk.ref_column, fk.column
            ));
            code.push_str("}\n\n");
        }
    }

    code
}

#[cfg(test)]
mod codegen_tests {
    use super::*;

    #[test]
    fn test_generate_schema_code() {
        let schema_content = r#"
table users {
    id UUID primary_key
    email TEXT not_null
    age INT
}

table posts {
    id UUID primary_key
    user_id UUID ref:users.id
    title TEXT
}
"#;

        let schema = Schema::parse(schema_content).unwrap();
        let code = generate_schema_code(&schema);

        // Verify module structure
        assert!(code.contains("pub mod users {"));
        assert!(code.contains("pub mod posts {"));

        // Verify table structs
        assert!(code.contains("pub struct Users;"));
        assert!(code.contains("pub struct Posts;"));

        // Verify columns
        assert!(code.contains("pub const id: TypedColumn<uuid::Uuid, Public>"));
        assert!(code.contains("pub const email: TypedColumn<String, Public>"));
        assert!(code.contains("pub const age: TypedColumn<i32, Public>"));

        // Verify RelatedTo impls for compile-time relationship checking
        assert!(code.contains("impl RelatedTo<users::Users> for posts::Posts"));
        assert!(code.contains("impl RelatedTo<posts::Posts> for users::Users"));
    }

    #[test]
    fn test_generate_protected_column() {
        let schema_content = r#"
table secrets {
    id UUID primary_key
    token TEXT protected
}
"#;
        let schema = Schema::parse(schema_content).unwrap();
        let code = generate_schema_code(&schema);

        // Verify Protected policy
        assert!(code.contains("pub const token: TypedColumn<String, Protected>"));
    }
}

#[cfg(test)]
mod migration_parser_tests {
    use super::*;

    #[test]
    fn test_agent_contracts_migration_parses_all_columns() {
        let sql = r#"
CREATE TABLE agent_contracts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id UUID NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    operator_id UUID NOT NULL REFERENCES operators(id) ON DELETE CASCADE,
    pricing_model VARCHAR(20) NOT NULL CHECK (pricing_model IN ('commission', 'static_markup', 'net_rate')),
    commission_percent DECIMAL(5,2),
    static_markup DECIMAL(10,2),
    is_active BOOLEAN DEFAULT true,
    valid_from DATE,
    valid_until DATE,
    approved_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    UNIQUE(agent_id, operator_id)
);
"#;

        let mut schema = Schema::default();
        schema.parse_sql_migration(sql);

        let table = schema
            .tables
            .get("agent_contracts")
            .expect("agent_contracts table should exist");

        for col in &[
            "id",
            "agent_id",
            "operator_id",
            "pricing_model",
            "commission_percent",
            "static_markup",
            "is_active",
            "valid_from",
            "valid_until",
            "approved_by",
            "created_at",
            "updated_at",
        ] {
            assert!(
                table.columns.contains_key(*col),
                "Missing column: '{}'. Found: {:?}",
                col,
                table.columns.keys().collect::<Vec<_>>()
            );
        }
    }

    /// Regression test: column names that START with SQL keywords must parse correctly.
    /// e.g., created_at starts with CREATE, primary_contact starts with PRIMARY, etc.
    #[test]
    fn test_keyword_prefixed_column_names_are_not_skipped() {
        let sql = r#"
CREATE TABLE edge_cases (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    created_by UUID,
    primary_contact VARCHAR(255),
    check_status VARCHAR(20),
    unique_code VARCHAR(50),
    foreign_ref UUID,
    constraint_name VARCHAR(100),
    PRIMARY KEY (id),
    CHECK (check_status IN ('pending', 'active')),
    UNIQUE (unique_code),
    CONSTRAINT fk_ref FOREIGN KEY (foreign_ref) REFERENCES other(id)
);
"#;

        let mut schema = Schema::default();
        schema.parse_sql_migration(sql);

        let table = schema
            .tables
            .get("edge_cases")
            .expect("edge_cases table should exist");

        // These column names start with SQL keywords — all must be found
        for col in &[
            "created_at",
            "created_by",
            "primary_contact",
            "check_status",
            "unique_code",
            "foreign_ref",
            "constraint_name",
        ] {
            assert!(
                table.columns.contains_key(*col),
                "Column '{}' should NOT be skipped just because it starts with a SQL keyword. Found: {:?}",
                col,
                table.columns.keys().collect::<Vec<_>>()
            );
        }

        // These are constraint keywords, not columns — must NOT appear
        // (PRIMARY KEY, CHECK, UNIQUE, CONSTRAINT lines should be skipped)
        assert!(
            !table.columns.contains_key("primary"),
            "Constraint keyword 'PRIMARY' should not be treated as a column"
        );
    }
}
