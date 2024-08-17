//! Schema linting for best practices

use anyhow::Result;
use colored::*;
use qail_core::migrate::{ColumnType, parse_qail};

/// Lint severity level.
#[derive(Debug, Clone, PartialEq)]
pub enum LintLevel {
    Error,
    Warning,
    Info,
}

/// A lint issue found in the schema.
#[derive(Debug)]
pub struct LintIssue {
    pub level: LintLevel,
    pub table: String,
    pub column: Option<String>,
    pub message: String,
    pub suggestion: Option<String>,
}

/// Lint a schema file for best practices.
pub fn lint_schema(schema_path: &str, strict: bool) -> Result<()> {
    println!("{}", "🔍 Schema Linter".cyan().bold());
    println!();

    let content = std::fs::read_to_string(schema_path)
        .map_err(|e| anyhow::anyhow!("Failed to read schema: {}", e))?;

    let schema =
        parse_qail(&content).map_err(|e| anyhow::anyhow!("Failed to parse schema: {}", e))?;

    println!("  Linting: {}", schema_path.yellow());
    println!("  Tables: {}", schema.tables.len());
    println!();

    let mut issues: Vec<LintIssue> = Vec::new();

    for table in schema.tables.values() {
        // Check 1: Missing primary key
        let has_pk = table.columns.iter().any(|c| c.primary_key);
        if !has_pk {
            issues.push(LintIssue {
                level: LintLevel::Error,
                table: table.name.clone(),
                column: None,
                message: "Table has no primary key".to_string(),
                suggestion: Some(
                    "Add a primary key column, e.g., 'id UUID primary_key'".to_string(),
                ),
            });
        }

        // Check 2: UUID vs SERIAL preference
        for col in &table.columns {
            if col.primary_key
                && matches!(col.data_type, ColumnType::Serial | ColumnType::BigSerial)
            {
                issues.push(LintIssue {
                    level: LintLevel::Info,
                    table: table.name.clone(),
                    column: Some(col.name.clone()),
                    message: "Using SERIAL for primary key".to_string(),
                    suggestion: Some(
                        "Consider UUID for distributed systems: 'id UUID primary_key'".to_string(),
                    ),
                });
            }
        }

        // Check 3: Missing created_at/updated_at
        let has_created_at = table.columns.iter().any(|c| c.name == "created_at");
        let has_updated_at = table.columns.iter().any(|c| c.name == "updated_at");

        if !has_created_at && table.columns.len() > 2 {
            issues.push(LintIssue {
                level: LintLevel::Warning,
                table: table.name.clone(),
                column: None,
                message: "Missing created_at column".to_string(),
                suggestion: Some(
                    "Add 'created_at TIMESTAMPTZ not_null' for audit trail".to_string(),
                ),
            });
        }

        if !has_updated_at && table.columns.len() > 2 {
            issues.push(LintIssue {
                level: LintLevel::Warning,
                table: table.name.clone(),
                column: None,
                message: "Missing updated_at column".to_string(),
                suggestion: Some(
                    "Add 'updated_at TIMESTAMPTZ not_null' for audit trail".to_string(),
                ),
            });
        }

        // Check 4: Nullable columns without defaults
        for col in &table.columns {
            if col.nullable && col.default.is_none() && !col.primary_key {
                // Skip certain types
                if matches!(col.data_type, ColumnType::Text | ColumnType::Jsonb) {
                    continue;
                }
                issues.push(LintIssue {
                    level: LintLevel::Info,
                    table: table.name.clone(),
                    column: Some(col.name.clone()),
                    message: "Nullable column without default".to_string(),
                    suggestion: Some(
                        "Consider adding a default value or making it NOT NULL".to_string(),
                    ),
                });
            }
        }

        // Check 5: Foreign key columns without defined FK relation
        for col in &table.columns {
            if col.name.ends_with("_id") && !col.primary_key && col.foreign_key.is_none() {
                issues.push(LintIssue {
                    level: LintLevel::Warning,
                    table: table.name.clone(),
                    column: Some(col.name.clone()),
                    message: "Possible FK column without references()".to_string(),
                    suggestion: Some("Consider adding '.references(\"table\", \"id\")' for referential integrity".to_string()),
                });
            }
        }

        // Check 6: Table naming conventions
        if table.name.chars().any(|c| c.is_uppercase()) {
            issues.push(LintIssue {
                level: LintLevel::Warning,
                table: table.name.clone(),
                column: None,
                message: "Table name contains uppercase letters".to_string(),
                suggestion: Some("Use snake_case for table names".to_string()),
            });
        }
    }

    // Filter based on strict mode
    let filtered: Vec<_> = if strict {
        issues
            .iter()
            .filter(|i| i.level == LintLevel::Error)
            .collect()
    } else {
        issues.iter().collect()
    };

    // Print results
    if filtered.is_empty() {
        println!("{}", "✓ No issues found!".green().bold());
    } else {
        let errors = issues
            .iter()
            .filter(|i| i.level == LintLevel::Error)
            .count();
        let warnings = issues
            .iter()
            .filter(|i| i.level == LintLevel::Warning)
            .count();
        let infos = issues.iter().filter(|i| i.level == LintLevel::Info).count();

        if errors > 0 {
            println!("{} {} error(s)", "✗".red(), errors);
        }
        if warnings > 0 && !strict {
            println!("{} {} warning(s)", "⚠".yellow(), warnings);
        }
        if infos > 0 && !strict {
            println!("{} {} info(s)", "ℹ".blue(), infos);
        }
        println!();

        for issue in &filtered {
            let icon = match issue.level {
                LintLevel::Error => "✗".red(),
                LintLevel::Warning => "⚠".yellow(),
                LintLevel::Info => "ℹ".blue(),
            };

            let location = if let Some(ref col) = issue.column {
                format!("{}.{}", issue.table, col)
            } else {
                issue.table.clone()
            };

            println!("{} {} {}", icon, location.white(), issue.message);
            if let Some(ref suggestion) = issue.suggestion {
                println!("  {} {}", "→".dimmed(), suggestion.dimmed());
            }
            println!();
        }
    }

    Ok(())
}
