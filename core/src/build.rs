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
    pub name: String,
    /// Column name -> Column type (strongly-typed AST enum)
    pub columns: HashMap<String, ColumnType>,
    /// Column name -> Access Policy (Default: "Public", can be "Protected")
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
    pub tables: HashMap<String, TableSchema>,
    /// Infrastructure resources (bucket, queue, topic)
    pub resources: HashMap<String, ResourceSchema>,
}

/// Infrastructure resource schema (bucket, queue, topic)
#[derive(Debug, Clone)]
pub struct ResourceSchema {
    pub name: String,
    pub kind: String,
    pub provider: Option<String>,
    pub properties: HashMap<String, String>,
}

impl Schema {
    /// Parse a schema.qail file
    pub fn parse_file(path: &str) -> Result<Self, String> {
        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read schema file '{}': {}", path, e))?;
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
            if line.starts_with("bucket ") || line.starts_with("queue ") || line.starts_with("topic ") {
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
                            if inner.contains('}') { break; }
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
                    schema.resources.insert(name.clone(), ResourceSchema {
                        name,
                        kind,
                        provider,
                        properties,
                    });
                }
                continue;
            }

            // Table definition: table name { [rls]
            if line.starts_with("table ") && (line.ends_with('{') || line.contains('{')) {
                // Save previous table if any
                if let Some(table_name) = current_table.take() {
                    // Auto-detect RLS: table has operator_id column or was marked `rls`
                    let has_rls = current_rls_flag || current_columns.contains_key("operator_id");
                    schema.tables.insert(table_name.clone(), TableSchema {
                        name: table_name,
                        columns: std::mem::take(&mut current_columns),
                        policies: std::mem::take(&mut current_policies),
                        foreign_keys: std::mem::take(&mut current_fks),
                        rls_enabled: has_rls,
                    });
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
                    schema.tables.insert(table_name.clone(), TableSchema {
                        name: table_name,
                        columns: std::mem::take(&mut current_columns),
                        policies: std::mem::take(&mut current_policies),
                        foreign_keys: std::mem::take(&mut current_fks),
                        rls_enabled: has_rls,
                    });
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
                    let col_type = col_type_str.parse::<ColumnType>().unwrap_or(ColumnType::Text);
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
        self.tables.iter()
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
        let entries = fs::read_dir(dir)
            .map_err(|e| format!("Failed to read migrations dir: {}", e))?;
        
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
                self.tables.insert(table_name.clone(), TableSchema {
                    name: table_name,
                    columns: HashMap::new(),
                    policies: HashMap::new(),
                    foreign_keys: vec![],
                    rls_enabled: false,
                });
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
                paren_depth = paren_depth.saturating_sub(line.chars().filter(|c| *c == ')').count());
                
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
            if line_upper.contains("ALTER TABLE") && line_upper.contains("ADD COLUMN")
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
                    self.tables.insert(table.clone(), TableSchema {
                        name: table,
                        columns: cols,
                        policies: HashMap::new(),
                        foreign_keys: vec![],
                        rls_enabled: false,
                    });
                    changes += 1;
                }
            }
            
            // ALTER TABLE ... ADD (without COLUMN keyword)
            if line_upper.contains("ALTER TABLE") && line_upper.contains(" ADD ") && !line_upper.contains("ADD COLUMN")
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
            if line_upper.contains("ALTER TABLE") && line_upper.contains("DROP COLUMN")
                && let Some((table, col)) = extract_alter_drop_column(line)
                && let Some(t) = self.tables.get_mut(&table)
                && t.columns.remove(&col).is_some()
            {
                changes += 1;
            }
            
            // ALTER TABLE ... DROP (without COLUMN keyword - PostgreSQL style)
            if line_upper.contains("ALTER TABLE") && line_upper.contains(" DROP ") 
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
    
    if name.is_empty() { None } else { Some(name.to_lowercase()) }
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
        line_upper.starts_with(kw)
            && line_upper[kw.len()..].starts_with([' ', '('])
    };
    
    if starts_with_keyword("CREATE") || 
       starts_with_keyword("PRIMARY") ||
       starts_with_keyword("FOREIGN") ||
       starts_with_keyword("UNIQUE") ||
       starts_with_keyword("CHECK") ||
       starts_with_keyword("CONSTRAINT") ||
       line_upper.starts_with(")") ||
       line_upper.starts_with("(") ||
       line.is_empty() {
        return None;
    }
    
    // First word is column name
    let name: String = line
        .trim_start_matches('(')
        .trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    
    if name.is_empty() || name.to_uppercase() == "IF" { None } else { Some(name.to_lowercase()) }
}

/// Extract table and column from ALTER TABLE ... ADD COLUMN
fn extract_alter_add_column(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let add_pos = line_upper.find("ADD COLUMN")?;
    
    // Table name between ALTER TABLE and ADD COLUMN
    let table_part = &line[alter_pos + 11..add_pos];
    let table: String = table_part.trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    
    // Column name after ADD COLUMN
    let col_part = &line[add_pos + 10..];
    let col: String = col_part.trim()
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
    let table: String = table_part.trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    
    let col_part = &line[add_pos + 5..];
    let col: String = col_part.trim()
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
    
    if name.is_empty() { None } else { Some(name.to_lowercase()) }
}

/// Extract table and column from ALTER TABLE ... DROP COLUMN
fn extract_alter_drop_column(line: &str) -> Option<(String, String)> {
    let line_upper = line.to_uppercase();
    let alter_pos = line_upper.find("ALTER TABLE")?;
    let drop_pos = line_upper.find("DROP COLUMN")?;
    
    // Table name between ALTER TABLE and DROP COLUMN
    let table_part = &line[alter_pos + 11..drop_pos];
    let table: String = table_part.trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    
    // Column name after DROP COLUMN
    let col_part = &line[drop_pos + 11..];
    let col: String = col_part.trim()
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
    let table: String = table_part.trim()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    
    let col_part = &line[drop_pos + 6..];
    let col: String = col_part.trim()
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
}

/// Extracted QAIL usage from source code
#[derive(Debug)]
pub struct QailUsage {
    pub file: String,
    pub line: usize,
    pub table: String,
    pub columns: Vec<String>,
    pub action: String,
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

fn scan_file(file: &str, content: &str, usages: &mut Vec<QailUsage>) {
    // All CRUD patterns: GET=SELECT, ADD=INSERT, SET=UPDATE, DEL=DELETE, PUT=UPSERT
    let patterns = [
        ("Qail::get(", "GET"),
        ("Qail::add(", "ADD"),
        ("Qail::set(", "SET"),
        ("Qail::del(", "DEL"),
        ("Qail::put(", "PUT"),
    ];

    // First pass: extract all CTE names from .to_cte() patterns
    // Pattern: .to_cte("cte_name")
    let mut cte_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for line in content.lines() {
        let line = line.trim();
        if let Some(pos) = line.find(".to_cte(") {
            let after = &line[pos + 8..]; // ".to_cte(" is 8 chars
            if let Some(name) = extract_string_arg(after) {
                cte_names.insert(name);
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
                
                // Extract table name from Qail::get("table")
                let after = &line[pos + pattern.len()..];
                if let Some(table) = extract_string_arg(after) {
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
                    
                    // Check if this is a CTE reference
                    let is_cte_ref = cte_names.contains(&table);
                    
                    // Check if this query chain includes .with_rls(
                    let has_rls = full_chain.contains(".with_rls(");
                    
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
                } else {
                    // Dynamic table name — cannot validate at build time.
                    // Extract the variable name for a helpful warning.
                    let var_hint = after.split(')').next().unwrap_or("?").trim();
                    println!(
                        "cargo:warning=Qail: dynamic table name `{}` in {}:{} — cannot validate columns at build time. Consider using string literals.",
                        var_hint, file, start_line
                    );
                }
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
            && !col.contains('.') {
            columns.push(col);
        }
        remaining = after;
    }
    
    // .eq("col", val), .ne("col", val), .gt, .lt, .gte, .lte
    for method in [".eq(", ".ne(", ".gt(", ".lt(", ".gte(", ".lte(", ".like(", ".ilike("] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(col) = extract_string_arg(after)
                && !col.contains('.') {
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
            && !col.contains('.') {
            columns.push(col);
        }
        remaining = after;
    }
    
    // .order_by("col", ...)
    remaining = line;
    while let Some(pos) = remaining.find(".order_by(") {
        let after = &remaining[pos + 10..];
        if let Some(col) = extract_string_arg(after)
            && !col.contains('.') {
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
                && !col.contains('.') {
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
            && !col.contains('.') {
            columns.push(col);
        }
        remaining = after;
    }
    
    columns
}

/// Validate QAIL usage against schema using the smart Validator
/// Provides "Did you mean?" suggestions for typos, type validation, and RLS audit
pub fn validate_against_schema(schema: &Schema, usages: &[QailUsage]) -> Vec<String> {
    use crate::validator::Validator;
    
    // Build Validator from Schema with column types
    let mut validator = Validator::new();
    for (table_name, table_schema) in &schema.tables {
        // Convert HashMap<String, ColumnType> to Vec<(&str, &str)> for validator
        let type_strings: Vec<(String, String)> = table_schema.columns
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

    for usage in usages {
        // Skip CTE alias refs - these are defined in code, not in schema
        if usage.is_cte_ref {
            continue;
        }
        
        // Use Validator for smart error messages with suggestions
        match validator.validate_table(&usage.table) {
            Ok(()) => {
                // Table exists, check columns
                for col in &usage.columns {
                    // Skip qualified columns (CTE refs like cte.column)
                    if col.contains('.') {
                        continue;
                    }
                    // Skip SQL function expressions (e.g., count(*), SUM(amount))
                    // and wildcard (*) — these are valid SQL, not schema columns
                    if col.contains('(') || col == "*" {
                        continue;
                    }
                    
                    if let Err(e) = validator.validate_column(&usage.table, col) {
                        errors.push(format!("{}:{}: {}", usage.file, usage.line, e));
                    }
                }
                
                // RLS Audit: warn if query targets RLS-enabled table without .with_rls()
                if schema.is_rls_table(&usage.table) && !usage.has_rls {
                    rls_warnings.push(format!(
                        "{}:{}: ⚠️ RLS AUDIT: Qail::{}(\"{}\") has no .with_rls() — table has RLS enabled, query may leak tenant data",
                        usage.file, usage.line, usage.action.to_lowercase(), usage.table
                    ));
                }
            }
            Err(e) => {
                errors.push(format!("{}:{}: {}", usage.file, usage.line, e));
            }
        }
    }
    
    // Append RLS warnings (non-fatal, but visible)
    errors.extend(rls_warnings);

    errors
}

/// Main validation entry point for build.rs
pub fn validate() {
    let mode = std::env::var("QAIL").unwrap_or_else(|_| {
        if Path::new("schema.qail").exists() {
            "schema".to_string()
        } else {
            "false".to_string()
        }
    });

    match mode.as_str() {
        "schema" => {
            println!("cargo:rerun-if-changed=schema.qail");
            println!("cargo:rerun-if-changed=migrations");
            println!("cargo:rerun-if-env-changed=QAIL");
            
            match Schema::parse_file("schema.qail") {
                Ok(mut schema) => {
                    // Merge pending migrations with pulled schema
                    let merged = schema.merge_migrations("migrations").unwrap_or(0);
                    if merged > 0 {
                        println!("cargo:warning=QAIL: Merged {} schema changes from migrations", merged);
                    }
                    
                    let usages = scan_source_files("src/");
                    let errors = validate_against_schema(&schema, &usages);
                    
                    if errors.is_empty() {
                        println!("cargo:warning=QAIL: Validated {} queries against schema.qail ✓", usages.len());
                    } else {
                        for error in &errors {
                            println!("cargo:warning=QAIL ERROR: {}", error);
                        }
                        // Fail the build
                        panic!("QAIL validation failed with {} errors", errors.len());
                    }
                }
                Err(e) => {
                    println!("cargo:warning=QAIL: {}", e);
                }
            }
        }
        "live" => {
            println!("cargo:rerun-if-env-changed=QAIL");
            println!("cargo:rerun-if-env-changed=DATABASE_URL");
            
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
                        .current_dir(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string()))
                        .output();
                    
                    match cargo_result {
                        Ok(output) if output.status.success() => {
                            println!("cargo:warning=QAIL: Schema pulled via cargo ✓");
                        }
                        _ => {
                            panic!("QAIL: Cannot run qail pull: {}. Install qail CLI or set QAIL=schema", e);
                        }
                    }
                }
            }
            
            // Step 2: Parse the updated schema and validate
            match Schema::parse_file("schema.qail") {
                Ok(mut schema) => {
                    // Merge pending migrations (in case live DB doesn't have them yet)
                    let merged = schema.merge_migrations("migrations").unwrap_or(0);
                    if merged > 0 {
                        println!("cargo:warning=QAIL: Merged {} schema changes from pending migrations", merged);
                    }
                    
                    let usages = scan_source_files("src/");
                    let errors = validate_against_schema(&schema, &usages);
                    
                    if errors.is_empty() {
                        println!("cargo:warning=QAIL: Validated {} queries against live database ✓", usages.len());
                    } else {
                        for error in &errors {
                            println!("cargo:warning=QAIL ERROR: {}", error);
                        }
                        panic!("QAIL validation failed with {} errors", errors.len());
                    }
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
        assert_eq!(extract_string_arg(r#""table_name")"#), Some("table_name".to_string()));
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
        code.push_str(&format!("        fn table_name() -> &'static str {{ \"{}\" }}\n", table.name));
        code.push_str("    }\n\n");
        
        code.push_str(&format!("    impl From<{}> for String {{\n", struct_name));
        code.push_str(&format!("        fn from(_: {}) -> String {{ \"{}\".to_string() }}\n", struct_name, table.name));
        code.push_str("    }\n\n");

        code.push_str(&format!("    impl AsRef<str> for {} {{\n", struct_name));
        code.push_str(&format!("        fn as_ref(&self) -> &str {{ \"{}\" }}\n", table.name));
        code.push_str("    }\n\n");
        
        // Table constant for convenience
        code.push_str(&format!("    /// The `{}` table\n", table.name));
        code.push_str(&format!("    pub const table: {} = {};\n\n", struct_name, struct_name));
        
        // Sort columns for deterministic output
        let mut columns: Vec<_> = table.columns.iter().collect();
        columns.sort_by(|a, b| a.0.cmp(b.0));
        
        // Column constants
        for (col_name, col_type) in columns {
            let rust_type = qail_type_to_rust(col_type);
            let col_ident = to_rust_ident(col_name);
            let policy = table.policies.get(col_name).map(|s| s.as_str()).unwrap_or("Public");
            let rust_policy = if policy == "Protected" { "Protected" } else { "Public" };
            
            code.push_str(&format!("    /// Column `{}.{}` ({}) - {}\n", table.name, col_name, col_type.to_pg_type(), policy));
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
    
    code.push_str("// =============================================================================\n");
    code.push_str("// Compile-Time Relationship Safety (RelatedTo impls)\n");
    code.push_str("// =============================================================================\n\n");
    
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
        
        let table = schema.tables.get("agent_contracts")
            .expect("agent_contracts table should exist");
        
        for col in &["id", "agent_id", "operator_id", "pricing_model",
                      "commission_percent", "static_markup", "is_active",
                      "valid_from", "valid_until", "approved_by",
                      "created_at", "updated_at"] {
            assert!(
                table.columns.contains_key(*col),
                "Missing column: '{}'. Found: {:?}",
                col, table.columns.keys().collect::<Vec<_>>()
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
        
        let table = schema.tables.get("edge_cases")
            .expect("edge_cases table should exist");
        
        // These column names start with SQL keywords — all must be found
        for col in &["created_at", "created_by", "primary_contact",
                      "check_status", "unique_code", "foreign_ref",
                      "constraint_name"] {
            assert!(
                table.columns.contains_key(*col),
                "Column '{}' should NOT be skipped just because it starts with a SQL keyword. Found: {:?}",
                col, table.columns.keys().collect::<Vec<_>>()
            );
        }
        
        // These are constraint keywords, not columns — must NOT appear
        // (PRIMARY KEY, CHECK, UNIQUE, CONSTRAINT lines should be skipped)
        assert!(!table.columns.contains_key("primary"),
            "Constraint keyword 'PRIMARY' should not be treated as a column");
    }
}
