//! Schema linting for best practices

use crate::colors::*;
use anyhow::Result;
use qail_core::migrate::schema::ResourceKind;
use qail_core::migrate::{ColumnType, parse_qail_file};

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

    let schema = parse_qail_file(schema_path)
        .map_err(|e| anyhow::anyhow!("Failed to parse schema: {}", e))?;

    println!("  Linting: {}", schema_path.yellow());
    println!("  Tables: {}", schema.tables.len());
    println!();

    let mut issues: Vec<LintIssue> = Vec::new();
    issues.extend(lint_resource_issues(&schema, schema_path));

    for table in schema.tables.values() {
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

fn lint_resource_issues(schema: &qail_core::migrate::Schema, schema_path: &str) -> Vec<LintIssue> {
    if schema.resources.is_empty() {
        return Vec::new();
    }

    let migration_input = looks_like_migration_input(schema_path);
    let mut issues = Vec::with_capacity(schema.resources.len());

    for resource in &schema.resources {
        let level = if migration_input {
            LintLevel::Error
        } else {
            LintLevel::Warning
        };
        issues.push(LintIssue {
            level,
            table: "resource".to_string(),
            column: Some(resource.name.clone()),
            message: format!(
                "Resource declaration '{} {}' is not executable by `qail migrate apply`",
                resource_kind_name(&resource.kind),
                resource.name
            ),
            suggestion: Some(if migration_input {
                "Move infra resources to schema/build-deploy flow and keep migration files database-only".to_string()
            } else {
                "Infra resources are declarative metadata; use build/deploy tooling instead of migrate apply".to_string()
            }),
        });
    }

    issues
}

fn looks_like_migration_input(schema_path: &str) -> bool {
    let lower = schema_path.to_ascii_lowercase();
    lower.ends_with(".up.qail")
        || lower.ends_with(".down.qail")
        || lower.contains("/migrations/")
        || lower.contains("\\migrations\\")
}

fn resource_kind_name(kind: &ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Bucket => "bucket",
        ResourceKind::Queue => "queue",
        ResourceKind::Topic => "topic",
    }
}

#[cfg(test)]
mod tests {
    use super::{LintLevel, lint_resource_issues, looks_like_migration_input};
    use qail_core::migrate::Schema;
    use qail_core::migrate::schema::{ResourceDef, ResourceKind};
    use std::collections::HashMap;

    #[test]
    fn detects_migration_like_paths() {
        assert!(looks_like_migration_input(
            "deltas/migrations/001_init.up.qail"
        ));
        assert!(looks_like_migration_input("001_init.down.qail"));
        assert!(!looks_like_migration_input("schema.qail"));
    }

    #[test]
    fn resource_issue_is_error_for_migration_inputs() {
        let mut schema = Schema::new();
        schema.add_resource(ResourceDef {
            name: "avatars".to_string(),
            kind: ResourceKind::Bucket,
            provider: Some("s3".to_string()),
            properties: HashMap::new(),
        });

        let issues = lint_resource_issues(&schema, "deltas/migrations/001_assets.up.qail");
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].level, LintLevel::Error);
        assert!(issues[0].message.contains("bucket avatars"));
    }
}
