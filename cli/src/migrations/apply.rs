//! Apply migrations from migrations/ folder
//!
//! Reads `.qail` migration files in order and executes them against the database.

use anyhow::{Context, Result};
use colored::*;
use qail_core::parser::schema::Schema;
use std::fs;
use std::path::Path;

/// Apply all pending migrations from the migrations/ folder
pub async fn migrate_apply(url: &str, direction: MigrateDirection) -> Result<()> {
    let migrations_dir = Path::new("migrations");
    
    if !migrations_dir.exists() {
        anyhow::bail!("migrations/ directory not found. Run 'qail init' first.");
    }
    
    // Get migration files sorted by name
    let suffix = match direction {
        MigrateDirection::Up => ".up.qail",
        MigrateDirection::Down => ".down.qail",
    };
    
    let mut migration_files: Vec<_> = fs::read_dir(migrations_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().to_string_lossy().ends_with(suffix))
        .collect();
    
    migration_files.sort_by_key(|e| e.file_name());
    
    if migration_files.is_empty() {
        println!("{} No {} migrations found", "!".yellow(), suffix);
        return Ok(());
    }
    
    println!("{} Found {} migrations to apply\n", "→".cyan(), migration_files.len());
    
    // Connect to database
    let (host, port, user, database, password) = parse_postgres_url(url)?;
    let mut pg = if let Some(password) = password {
        qail_pg::PgDriver::connect_with_password(&host, port, &user, &database, &password).await?
    } else {
        qail_pg::PgDriver::connect(&host, port, &user, &database).await?
    };
    
    println!("{} Connected to {}", "✓".green(), database.cyan());
    
    // Apply each migration
    for entry in migration_files {
        let filename = entry.file_name();
        let filename_str = filename.to_string_lossy();
        let path = entry.path();
        
        print!("  {} {}... ", "→".cyan(), filename_str);
        
        let content = fs::read_to_string(&path)
            .context(format!("Failed to read {}", path.display()))?;
        
        // Try to parse as schema and generate DDL
        let sql = parse_qail_to_sql(&content)?;
        
        // Execute the SQL
        pg.execute_raw(&sql).await
            .context(format!("Failed to execute migration {}", filename_str))?;
        
        println!("{}", "✓".green());
    }
    
    println!("\n{} All migrations applied successfully!", "✓".green().bold());
    Ok(())
}

/// Direction for migration
#[derive(Clone, Copy)]
pub enum MigrateDirection {
    Up,
    Down,
}

/// Parse a .qail schema file and generate SQL DDL
fn parse_qail_to_sql(content: &str) -> Result<String> {
    // First, try to parse as a schema file
    match Schema::parse(content) {
        Ok(schema) => {
            let mut sql_parts = Vec::new();
            
            // Generate DDL for each table
            for table in &schema.tables {
                sql_parts.push(table.to_ddl());
            }
            
            if sql_parts.is_empty() {
                // No tables found, might be raw SQL/functions/triggers
                // For now, extract function/trigger blocks and translate
                sql_parts.push(parse_functions_and_triggers(content)?);
            }
            
            Ok(sql_parts.join("\n\n"))
        }
        Err(_) => {
            // Not a schema file, try parsing as functions/triggers
            parse_functions_and_triggers(content)
        }
    }
}

/// Parse function and trigger definitions from .qail format
fn parse_functions_and_triggers(content: &str) -> Result<String> {
    let mut sql_parts = Vec::new();
    let mut current_block = String::new();
    let mut in_function = false;
    let mut in_trigger = false;
    let mut brace_depth = 0;
    
    for line in content.lines() {
        let trimmed = line.trim();
        
        // Skip comments
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        
        // Detect function start
        if trimmed.starts_with("function ") {
            in_function = true;
            current_block = line.to_string();
            if trimmed.contains('{') {
                brace_depth = 1;
            }
            continue;
        }
        
        // Detect trigger start
        if trimmed.starts_with("trigger ") {
            in_trigger = true;
            current_block = line.to_string();
            continue;
        }
        
        // Detect table start (for index definitions)
        if trimmed.starts_with("index ") {
            sql_parts.push(parse_index_line(trimmed)?);
            continue;
        }
        
        // Detect table block
        if trimmed.starts_with("table ") {
            in_function = false;
            in_trigger = false;
            // Re-parse as schema
            let table_content = extract_table_block(content, trimmed)?;
            if let Ok(schema) = Schema::parse(&table_content) {
                for table in &schema.tables {
                    sql_parts.push(table.to_ddl());
                }
            }
            continue;
        }
        
        // Handle function body
        if in_function {
            current_block.push('\n');
            current_block.push_str(line);
            
            brace_depth += line.matches('{').count();
            brace_depth -= line.matches('}').count();
            
            if brace_depth == 0 && trimmed.ends_with('}') {
                sql_parts.push(translate_function(&current_block)?);
                in_function = false;
                current_block.clear();
            }
            continue;
        }
        
        // Handle trigger line
        if in_trigger {
            current_block.push('\n');
            current_block.push_str(line);
            
            if trimmed.contains("execute ") {
                sql_parts.push(translate_trigger(&current_block)?);
                in_trigger = false;
                current_block.clear();
            }
            continue;
        }
    }
    
    if sql_parts.is_empty() {
        anyhow::bail!("Could not parse any valid QAIL statements");
    }
    
    Ok(sql_parts.join("\n\n"))
}

/// Parse an index line: index idx_name on table (col1, col2)
fn parse_index_line(line: &str) -> Result<String> {
    // index idx_qail_queue_poll on _qail_queue (status, id)
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 5 {
        anyhow::bail!("Invalid index syntax: {}", line);
    }
    
    let idx_name = parts[1];
    let table_name = parts[3];
    
    // Extract columns between ( and )
    if let (Some(start), Some(end)) = (line.find('('), line.find(')')) {
        let columns = &line[start..=end];
        return Ok(format!("CREATE INDEX IF NOT EXISTS {} ON {}{};", 
            idx_name, table_name, columns));
    }
    
    anyhow::bail!("Invalid index syntax: {}", line)
}

/// Extract a complete table block from content
fn extract_table_block(content: &str, start_line: &str) -> Result<String> {
    let mut result = String::new();
    let mut found = false;
    let mut brace_depth = 0;
    
    for line in content.lines() {
        if line.trim() == start_line || (found && brace_depth > 0) {
            found = true;
            result.push_str(line);
            result.push('\n');
            
            brace_depth += line.matches('{').count();
            brace_depth -= line.matches('}').count();
            
            if brace_depth == 0 && found {
                break;
            }
        }
    }
    
    Ok(result)
}

/// Translate a QAIL function block to PL/pgSQL
fn translate_function(block: &str) -> Result<String> {
    // function _qail_products_notify() returns trigger { ... }
    let mut sql = String::new();
    
    // Extract function name and return type
    let first_line = block.lines().next().unwrap_or("");
    let func_match = first_line.trim()
        .strip_prefix("function ")
        .ok_or_else(|| anyhow::anyhow!("Invalid function definition"))?;
    
    // Parse: name() returns type
    if let Some(returns_idx) = func_match.find(" returns ") {
        let name_part = &func_match[..returns_idx];
        let returns_part = func_match[returns_idx + 9..].trim();
        let return_type = returns_part.split_whitespace().next().unwrap_or("void");
        
        sql.push_str(&format!("CREATE OR REPLACE FUNCTION {} RETURNS {} AS $$\n", 
            name_part.trim(), return_type));
        sql.push_str("BEGIN\n");
        
        // Extract body (between { and })
        if let (Some(body_start), Some(body_end)) = (block.find('{'), block.rfind('}')) {
            let body = &block[body_start + 1..body_end];
            sql.push_str(&translate_function_body(body));
        }
        
        sql.push_str("END;\n");
        sql.push_str("$$ LANGUAGE plpgsql;");
        
        return Ok(sql);
    }
    
    anyhow::bail!("Invalid function syntax: {}", first_line)
}

/// Translate QAIL function body to PL/pgSQL
fn translate_function_body(body: &str) -> String {
    let mut sql = String::new();
    
    for line in body.lines() {
        let trimmed = line.trim();
        
        // Skip comments
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        
        // Translate if statements
        if trimmed.starts_with("if ") {
            let condition = trimmed.strip_prefix("if ").unwrap_or("");
            let condition = condition.trim_end_matches('{').trim();
            // Replace 'and' with 'AND' for SQL
            let condition = condition.replace(" and ", " AND ");
            sql.push_str(&format!("  IF {} THEN\n", condition));
            continue;
        }
        
        // Handle closing brace
        if trimmed == "}" {
            sql.push_str("  END IF;\n");
            continue;
        }
        
        // Regular statements - indent and add
        if !trimmed.is_empty() {
            sql.push_str(&format!("    {};\n", trimmed.trim_end_matches(';')));
        }
    }
    
    // Add RETURN statement for trigger functions
    sql.push_str("  RETURN COALESCE(NEW, OLD);\n");
    
    sql
}

/// Translate a QAIL trigger definition to SQL
fn translate_trigger(block: &str) -> Result<String> {
    // trigger qail_sync_products
    //   after insert or update or delete on products
    //   for each row execute _qail_products_notify()
    
    let lines: Vec<&str> = block.lines().collect();
    if lines.is_empty() {
        anyhow::bail!("Empty trigger definition");
    }
    
    let first_line = lines[0].trim();
    let trigger_name = first_line
        .strip_prefix("trigger ")
        .ok_or_else(|| anyhow::anyhow!("Invalid trigger definition"))?
        .trim();
    
    // Find timing and events line
    let mut timing = "";
    let mut table = "";
    let mut function = "";
    
    for line in &lines[1..] {
        let trimmed = line.trim();
        
        if trimmed.starts_with("after ") || trimmed.starts_with("before ") {
            let parts: Vec<&str> = trimmed.split(" on ").collect();
            if parts.len() >= 2 {
                timing = parts[0];
                table = parts[1].trim();
            }
        }
        
        if trimmed.contains("execute ") && let Some(func_start) = trimmed.find("execute ") {
            function = &trimmed[func_start + 8..];
        }
    }
    
    // Build SQL with DROP IF EXISTS for idempotency
    let mut sql = format!("DROP TRIGGER IF EXISTS {} ON {};\n", trigger_name, table);
    sql.push_str(&format!(
        "CREATE TRIGGER {}\n  {} ON {}\n  FOR EACH ROW EXECUTE FUNCTION {};",
        trigger_name,
        timing.to_uppercase(),
        table,
        function.trim()
    ));
    
    Ok(sql)
}

/// Parse PostgreSQL URL: postgres://user:password@host:port/database
fn parse_postgres_url(url: &str) -> Result<(String, u16, String, String, Option<String>)> {
    let url = url.trim_start_matches("postgres://").trim_start_matches("postgresql://");
    
    // Split by @ to separate credentials from host
    let (credentials, host_part): (Option<&str>, &str) = if url.contains('@') {
        let parts: Vec<&str> = url.splitn(2, '@').collect();
        (Some(parts[0]), parts.get(1).copied().unwrap_or("localhost/postgres"))
    } else {
        (None, url)
    };
    
    // Parse host:port/database
    let (host_port, database) = if host_part.contains('/') {
        let parts: Vec<&str> = host_part.splitn(2, '/').collect();
        (parts[0], parts.get(1).unwrap_or(&"postgres").to_string())
    } else {
        (host_part, "postgres".to_string())
    };
    
    // Parse host:port
    let (host, port) = if host_port.contains(':') {
        let parts: Vec<&str> = host_port.split(':').collect();
        (parts[0].to_string(), parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(5432))
    } else {
        (host_port.to_string(), 5432u16)
    };
    
    // Parse user:password
    let (user, password) = if let Some(creds) = credentials {
        if creds.contains(':') {
            let parts: Vec<&str> = creds.splitn(2, ':').collect();
            (parts[0].to_string(), Some(parts.get(1).unwrap_or(&"").to_string()))
        } else {
            (creds.to_string(), None)
        }
    } else {
        ("postgres".to_string(), None)
    };
    
    Ok((host, port, user, database, password))
}
