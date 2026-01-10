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

use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Table schema information with column types
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    /// Column name -> Column type (e.g., "id" -> "UUID", "name" -> "TEXT")
    pub columns: HashMap<String, String>,
}

/// Parsed schema from schema.qail file
#[derive(Debug, Default)]
pub struct Schema {
    pub tables: HashMap<String, TableSchema>,
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
        let mut current_columns: HashMap<String, String> = HashMap::new();

        for line in content.lines() {
            let line = line.trim();
            
            // Skip comments and empty lines
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Table definition: table name {
            if line.starts_with("table ") && line.ends_with('{') {
                // Save previous table if any
                if let Some(table_name) = current_table.take() {
                    schema.tables.insert(table_name.clone(), TableSchema {
                        name: table_name,
                        columns: std::mem::take(&mut current_columns),
                    });
                }
                
                // Parse new table name
                let name = line.trim_start_matches("table ")
                    .trim_end_matches('{')
                    .trim()
                    .to_string();
                current_table = Some(name);
            }
            // End of table definition
            else if line == "}" {
                if let Some(table_name) = current_table.take() {
                    schema.tables.insert(table_name.clone(), TableSchema {
                        name: table_name,
                        columns: std::mem::take(&mut current_columns),
                    });
                }
            }
            // Column definition: column_name TYPE [constraints]
            // Format from qail pull: "flow_name VARCHAR not_null"
            else if current_table.is_some() && !line.starts_with('#') && !line.is_empty() {
                let mut parts = line.split_whitespace();
                if let Some(col_name) = parts.next() {
                    // Second word is the type (default to TEXT if missing)
                    let col_type = parts.next().unwrap_or("TEXT").to_uppercase();
                    current_columns.insert(col_name.to_string(), col_type);
                }
            }
        }

        Ok(schema)
    }

    /// Check if table exists
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
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
            
            if line_upper.starts_with("CREATE TABLE") {
                if let Some(table_name) = extract_create_table_name(line) {
                    // Add table with empty columns (will be filled by column defs)
                    if !self.tables.contains_key(&table_name) {
                        self.tables.insert(table_name.clone(), TableSchema {
                            name: table_name,
                            columns: HashMap::new(),
                        });
                        changes += 1;
                    }
                }
            }
        }
        
        // Extract column definitions from CREATE TABLE blocks
        let mut current_table: Option<String> = None;
        let mut in_create_block = false;
        let mut paren_depth = 0;
        
        for line in sql.lines() {
            let line = line.trim();
            let line_upper = line.to_uppercase();
            
            if line_upper.starts_with("CREATE TABLE") {
                if let Some(name) = extract_create_table_name(line) {
                    current_table = Some(name);
                    in_create_block = true;
                    paren_depth = 0;
                }
            }
            
            if in_create_block {
                paren_depth += line.chars().filter(|c| *c == '(').count();
                paren_depth = paren_depth.saturating_sub(line.chars().filter(|c| *c == ')').count());
                
                // Extract column name (first identifier after opening paren)
                if let Some(col) = extract_column_from_create(line) {
                    if let Some(ref table) = current_table {
                        if let Some(t) = self.tables.get_mut(table) {
                            if t.columns.insert(col.clone(), "TEXT".to_string()).is_none() {
                                changes += 1;
                            }
                        }
                    }
                }
                
                if paren_depth == 0 && line.contains(')') {
                    in_create_block = false;
                    current_table = None;
                }
            }
            
            // ALTER TABLE ... ADD COLUMN
            if line_upper.contains("ALTER TABLE") && line_upper.contains("ADD COLUMN") {
                if let Some((table, col)) = extract_alter_add_column(line) {
                    if let Some(t) = self.tables.get_mut(&table) {
                        if t.columns.insert(col.clone(), "TEXT".to_string()).is_none() {
                            changes += 1;
                        }
                    } else {
                        // Table might be new from this migration
                        let mut cols = HashMap::new();
                        cols.insert(col, "TEXT".to_string());
                        self.tables.insert(table.clone(), TableSchema {
                            name: table,
                            columns: cols,
                        });
                        changes += 1;
                    }
                }
            }
            
            // ALTER TABLE ... ADD (without COLUMN keyword)
            if line_upper.contains("ALTER TABLE") && line_upper.contains(" ADD ") && !line_upper.contains("ADD COLUMN") {
                if let Some((table, col)) = extract_alter_add(line) {
                    if let Some(t) = self.tables.get_mut(&table) {
                        if t.columns.insert(col.clone(), "TEXT".to_string()).is_none() {
                            changes += 1;
                        }
                    }
                }
            }
            
            // DROP TABLE
            if line_upper.starts_with("DROP TABLE") {
                if let Some(table_name) = extract_drop_table_name(line) {
                    if self.tables.remove(&table_name).is_some() {
                        changes += 1;
                    }
                }
            }
            
            // ALTER TABLE ... DROP COLUMN
            if line_upper.contains("ALTER TABLE") && line_upper.contains("DROP COLUMN") {
                if let Some((table, col)) = extract_alter_drop_column(line) {
                    if let Some(t) = self.tables.get_mut(&table) {
                        if t.columns.remove(&col).is_some() {
                            changes += 1;
                        }
                    }
                }
            }
            
            // ALTER TABLE ... DROP (without COLUMN keyword - PostgreSQL style)
            if line_upper.contains("ALTER TABLE") && line_upper.contains(" DROP ") 
                && !line_upper.contains("DROP COLUMN") 
                && !line_upper.contains("DROP CONSTRAINT")
                && !line_upper.contains("DROP INDEX") 
            {
                if let Some((table, col)) = extract_alter_drop(line) {
                    if let Some(t) = self.tables.get_mut(&table) {
                        if t.columns.remove(&col).is_some() {
                            changes += 1;
                        }
                    }
                }
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
    let line_upper = line.to_uppercase();
    if line_upper.starts_with("CREATE") || 
       line_upper.starts_with("PRIMARY") ||
       line_upper.starts_with("FOREIGN") ||
       line_upper.starts_with("UNIQUE") ||
       line_upper.starts_with("CHECK") ||
       line_upper.starts_with("CONSTRAINT") ||
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
    pub fn column_type(&self, name: &str) -> Option<&str> {
        self.columns.get(name).map(|s| s.as_str())
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
            } else if path.extension().map_or(false, |e| e == "rs") {
                if let Ok(content) = fs::read_to_string(&path) {
                    scan_file(&path.display().to_string(), &content, usages);
                }
            }
        }
    }
}

fn scan_file(file: &str, content: &str, usages: &mut Vec<QailUsage>) {
    // Patterns to match:
    // Qail::get("table")
    // Qail::add("table")
    // Qail::del("table")
    // Qail::put("table")
    
    let patterns = [
        ("Qail::get(", "GET"),
        ("Qail::add(", "ADD"),
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
                    
                    // Extract column names from the full chain
                    let columns = extract_columns(&full_chain);
                    
                    usages.push(QailUsage {
                        file: file.to_string(),
                        line: start_line,
                        table,
                        columns,
                        action: action.to_string(),
                        is_cte_ref,
                    });
                    
                    // Skip to end of chain
                    i = j.saturating_sub(1);
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
    if s.starts_with('"') {
        let end = s[1..].find('"')?;
        Some(s[1..end + 1].to_string())
    } else {
        None
    }
}

fn extract_columns(line: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut remaining = line;
    
    // .column("col")
    while let Some(pos) = remaining.find(".column(") {
        let after = &remaining[pos + 8..];
        if let Some(col) = extract_string_arg(after) {
            columns.push(col);
        }
        remaining = after;
    }
    
    // Reset for next pattern
    remaining = line;
    
    // .filter("col", ...)
    while let Some(pos) = remaining.find(".filter(") {
        let after = &remaining[pos + 8..];
        if let Some(col) = extract_string_arg(after) {
            // Don't add qualified columns (CTE refs)
            if !col.contains('.') {
                columns.push(col);
            }
        }
        remaining = after;
    }
    
    // .eq("col", val), .ne("col", val), .gt, .lt, .gte, .lte
    for method in [".eq(", ".ne(", ".gt(", ".lt(", ".gte(", ".lte(", ".like(", ".ilike("] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(col) = extract_string_arg(after) {
                if !col.contains('.') {
                    columns.push(col);
                }
            }
            temp = after;
        }
    }
    
    // .order_by("col", ...)
    let mut remaining = line;
    while let Some(pos) = remaining.find(".order_by(") {
        let after = &remaining[pos + 10..];
        if let Some(col) = extract_string_arg(after) {
            if !col.contains('.') {
                columns.push(col);
            }
        }
        remaining = after;
    }
    
    columns
}

/// Validate QAIL usage against schema using the smart Validator
/// Provides "Did you mean?" suggestions for typos and type validation
pub fn validate_against_schema(schema: &Schema, usages: &[QailUsage]) -> Vec<String> {
    use crate::validator::Validator;
    
    // Build Validator from Schema with column types
    let mut validator = Validator::new();
    for (table_name, table_schema) in &schema.tables {
        // Convert HashMap<String, String> to Vec<(&str, &str)>
        let cols_with_types: Vec<(&str, &str)> = table_schema.columns
            .iter()
            .map(|(name, typ)| (name.as_str(), typ.as_str()))
            .collect();
        validator.add_table_with_types(table_name, &cols_with_types);
    }
    
    let mut errors = Vec::new();

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
                    
                    if let Err(e) = validator.validate_column(&usage.table, col) {
                        errors.push(format!("{}:{}: {}", usage.file, usage.line, e));
                    }
                }
            }
            Err(e) => {
                errors.push(format!("{}:{}: {}", usage.file, usage.line, e));
            }
        }
    }

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
