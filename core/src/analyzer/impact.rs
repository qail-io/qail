//! Migration impact analysis.

use super::scanner::CodeReference;
use crate::ast::{Action, Qail};
use crate::migrate::Schema;
use std::collections::HashMap;

/// Result of analyzing migration impact on codebase.
#[derive(Debug, Default)]
pub struct MigrationImpact {
    /// Breaking changes that will cause runtime errors
    pub breaking_changes: Vec<BreakingChange>,
    /// Warnings that may cause issues
    pub warnings: Vec<Warning>,
    pub safe_to_run: bool,
    /// Total number of affected files
    pub affected_files: usize,
}

/// A breaking change detected in the migration.
#[derive(Debug)]
pub enum BreakingChange {
    /// A column is being dropped that is still referenced in code
    DroppedColumn {
        table: String,
        column: String,
        references: Vec<CodeReference>,
    },
    /// A table is being dropped that is still referenced in code
    DroppedTable {
        table: String,
        references: Vec<CodeReference>,
    },
    /// A column is being renamed (requires code update)
    RenamedColumn {
        table: String,
        old_name: String,
        new_name: String,
        references: Vec<CodeReference>,
    },
    /// A column type is changing (may cause runtime errors)
    TypeChanged {
        table: String,
        column: String,
        old_type: String,
        new_type: String,
        references: Vec<CodeReference>,
    },
}

/// A warning about the migration.
#[derive(Debug)]
pub enum Warning {
    OrphanedReference {
        table: String,
        references: Vec<CodeReference>,
    },
}

impl MigrationImpact {
    /// Analyze migration commands against codebase references.
    pub fn analyze(
        commands: &[Qail],
        code_refs: &[CodeReference],
        _old_schema: &Schema,
        _new_schema: &Schema,
    ) -> Self {
        let mut impact = MigrationImpact::default();

        let mut table_refs: HashMap<String, Vec<&CodeReference>> = HashMap::new();
        let mut column_refs: HashMap<(String, String), Vec<&CodeReference>> = HashMap::new();

        for code_ref in code_refs {
            table_refs
                .entry(code_ref.table.clone())
                .or_default()
                .push(code_ref);

            for col in &code_ref.columns {
                column_refs
                    .entry((code_ref.table.clone(), col.clone()))
                    .or_default()
                    .push(code_ref);
            }
        }

        // Analyze each migration command
        for cmd in commands {
            match cmd.action {
                Action::Drop => {
                    // Table being dropped
                    let refs = cloned_refs_for_table(&table_refs, &cmd.table);
                    if !refs.is_empty() {
                        impact.breaking_changes.push(BreakingChange::DroppedTable {
                            table: cmd.table.clone(),
                            references: refs,
                        });
                    }
                }
                Action::AlterDrop => {
                    for col_expr in &cmd.columns {
                        if let crate::ast::Expr::Named(col_name) = col_expr {
                            let refs = cloned_refs_for_column(&column_refs, &cmd.table, col_name);
                            if !refs.is_empty() {
                                impact.breaking_changes.push(BreakingChange::DroppedColumn {
                                    table: cmd.table.clone(),
                                    column: col_name.clone(),
                                    references: refs,
                                });
                            }
                        }
                    }
                }
                Action::Mod => {
                    // Rename operation - check for references to old name
                    // Would need to parse the rename details from the command
                    // For now, flag any table with Mod action
                    let refs = cloned_refs_for_table(&table_refs, &cmd.table);
                    if !refs.is_empty() {
                        impact.breaking_changes.push(BreakingChange::RenamedColumn {
                            table: cmd.table.clone(),
                            old_name: "unknown".to_string(),
                            new_name: "unknown".to_string(),
                            references: refs,
                        });
                    }
                }
                _ => {}
            }
        }

        // Count affected files
        let mut affected: std::collections::HashSet<_> = std::collections::HashSet::new();
        for change in &impact.breaking_changes {
            match change {
                BreakingChange::DroppedColumn { references, .. }
                | BreakingChange::DroppedTable { references, .. }
                | BreakingChange::RenamedColumn { references, .. }
                | BreakingChange::TypeChanged { references, .. } => {
                    for r in references {
                        affected.insert(r.file.clone());
                    }
                }
            }
        }
        impact.affected_files = affected.len();
        impact.safe_to_run = impact.breaking_changes.is_empty();

        impact
    }

    /// Generate a human-readable report.
    pub fn report(&self) -> String {
        let mut output = String::new();

        if self.safe_to_run {
            output.push_str("✓ Migration is safe to run\n");
            return output;
        }

        output.push_str("⚠️  BREAKING CHANGES DETECTED\n\n");
        output.push_str(&format!("Affected files: {}\n\n", self.affected_files));

        for change in &self.breaking_changes {
            match change {
                BreakingChange::DroppedColumn {
                    table,
                    column,
                    references,
                } => {
                    output.push_str(&format!(
                        "DROP COLUMN {}.{} ({} references)\n",
                        table,
                        column,
                        references.len()
                    ));
                    for r in references.iter().take(5) {
                        // Show the specific column that was matched, not just the generic snippet
                        output.push_str(&format!(
                            "  ❌ {}:{} → uses \"{}\" in {}\n",
                            r.file.display(),
                            r.line,
                            column, // The actual matched column
                            r.snippet
                        ));
                    }
                    if references.len() > 5 {
                        output.push_str(&format!("  ... and {} more\n", references.len() - 5));
                    }
                    output.push('\n');
                }
                BreakingChange::DroppedTable { table, references } => {
                    output.push_str(&format!(
                        "DROP TABLE {} ({} references)\n",
                        table,
                        references.len()
                    ));
                    for r in references.iter().take(5) {
                        output.push_str(&format!(
                            "  ❌ {}:{} → {}\n",
                            r.file.display(),
                            r.line,
                            r.snippet
                        ));
                    }
                    output.push('\n');
                }
                BreakingChange::RenamedColumn {
                    table,
                    old_name,
                    new_name,
                    references,
                } => {
                    output.push_str(&format!(
                        "RENAME {}.{} → {} ({} references)\n",
                        table,
                        old_name,
                        new_name,
                        references.len()
                    ));
                    for r in references.iter().take(5) {
                        output.push_str(&format!(
                            "  ⚠️  {}:{} → {}\n",
                            r.file.display(),
                            r.line,
                            r.snippet
                        ));
                    }
                    output.push('\n');
                }
                BreakingChange::TypeChanged {
                    table,
                    column,
                    old_type,
                    new_type,
                    references,
                } => {
                    output.push_str(&format!(
                        "TYPE CHANGE {}.{}: {} → {} ({} references)\n",
                        table,
                        column,
                        old_type,
                        new_type,
                        references.len()
                    ));
                    for r in references.iter().take(5) {
                        output.push_str(&format!(
                            "  ⚠️  {}:{} → {}\n",
                            r.file.display(),
                            r.line,
                            r.snippet
                        ));
                    }
                    output.push('\n');
                }
            }
        }

        output
    }
}

fn cloned_refs_for_table(
    table_refs: &HashMap<String, Vec<&CodeReference>>,
    table: &str,
) -> Vec<CodeReference> {
    let mut out = Vec::new();
    for (ref_table, refs) in table_refs {
        if table_name_matches(table, ref_table) {
            push_unique_refs(&mut out, refs);
        }
    }
    out
}

fn cloned_refs_for_column(
    column_refs: &HashMap<(String, String), Vec<&CodeReference>>,
    table: &str,
    column: &str,
) -> Vec<CodeReference> {
    let mut out = Vec::new();
    let column = normalize_ident(column);
    for ((ref_table, ref_column), refs) in column_refs {
        let ref_column = normalize_ident(ref_column);
        if table_name_matches(table, ref_table) && (ref_column == column || ref_column == "*") {
            push_unique_refs(&mut out, refs);
        }
    }
    out
}

fn push_unique_refs(out: &mut Vec<CodeReference>, refs: &[&CodeReference]) {
    for reference in refs {
        let duplicate = out.iter().any(|existing| {
            existing.file == reference.file
                && existing.line == reference.line
                && existing.table == reference.table
                && existing.snippet == reference.snippet
        });
        if !duplicate {
            out.push((*reference).clone());
        }
    }
}

fn table_name_matches(command_table: &str, ref_table: &str) -> bool {
    let command_table = normalize_ident(command_table);
    let ref_table = normalize_ident(ref_table);
    if command_table == ref_table {
        return true;
    }

    let command_has_schema = command_table.contains('.');
    let ref_has_schema = ref_table.contains('.');
    (!command_has_schema || !ref_has_schema)
        && bare_table_name(&command_table) == bare_table_name(&ref_table)
}

fn bare_table_name(table: &str) -> &str {
    table.rsplit_once('.').map_or(table, |(_, bare)| bare)
}

fn normalize_ident(ident: &str) -> String {
    ident
        .split('.')
        .map(|part| part.trim().trim_matches('"'))
        .collect::<Vec<_>>()
        .join(".")
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_detect_dropped_table() {
        let cmd = Qail {
            action: Action::Drop,
            table: "users".to_string(),
            ..Default::default()
        };

        let code_ref = CodeReference {
            file: PathBuf::from("src/handlers.rs"),
            line: 42,
            table: "users".to_string(),
            columns: vec!["name".to_string()],
            query_type: super::super::scanner::QueryType::Qail,
            snippet: "get users fields *".to_string(),
        };

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &[code_ref], &old_schema, &new_schema);

        assert!(!impact.safe_to_run);
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_schema_qualified_drop_matches_bare_raw_sql_reference() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "app.users".to_string(),
            columns: vec![crate::ast::Expr::Named("old_email".to_string())],
            ..Default::default()
        };

        let code_ref = CodeReference {
            file: PathBuf::from("src/reporting.ts"),
            line: 17,
            table: "users".to_string(),
            columns: vec!["old_email".to_string()],
            query_type: super::super::scanner::QueryType::RawSql,
            snippet: r#"SELECT old_email FROM "app"."users""#.to_string(),
        };

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &[code_ref], &old_schema, &new_schema);

        assert!(!impact.safe_to_run);
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_schema_qualified_drop_ignores_different_schema_reference() {
        let cmd = Qail {
            action: Action::Drop,
            table: r#""app"."users""#.to_string(),
            ..Default::default()
        };

        let code_ref = CodeReference {
            file: PathBuf::from("src/admin.rs"),
            line: 24,
            table: "admin.users".to_string(),
            columns: vec!["id".to_string()],
            query_type: super::super::scanner::QueryType::RawSql,
            snippet: r#"SELECT id FROM "admin"."users""#.to_string(),
        };

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &[code_ref], &old_schema, &new_schema);

        assert!(impact.safe_to_run);
        assert_eq!(impact.breaking_changes.len(), 0);
    }

    #[test]
    fn test_dropped_filter_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_filter_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"const q = "get users fields id where email = $1";"#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run);
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_filter_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_filter_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"const sql = "SELECT id FROM users WHERE email = $1";"#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run);
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_cte_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_cte_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"
            const sql = `
                WITH active_users AS (
                    SELECT id, email FROM users WHERE status = $1
                )
                SELECT id FROM active_users
            `;
            "#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_joined_table_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("total".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_join_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"
            const sql = `
                SELECT u.id, o.total
                FROM users u
                JOIN orders o ON o.user_id = u.id
                WHERE o.status = $1
            `;
            "#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_update_from_source_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "payments".to_string(),
            columns: vec![crate::ast::Expr::Named("state".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_update_from_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"
            const sql = `
                UPDATE orders o
                SET status = p.status
                FROM payments p
                WHERE o.payment_id = p.id
                  AND p.state = $1
            `;
            "#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_insert_select_source_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("total".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_insert_select_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"
            const sql = `
                INSERT INTO archived_orders (id, total)
                SELECT id, total FROM orders WHERE status = $1
            `;
            "#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_set_operation_rhs_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("status".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_set_operation_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"
            const sql = `
                SELECT id FROM users
                UNION ALL
                SELECT user_id FROM orders WHERE status = $1
            `;
            "#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_insert_returning_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("created_at".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_insert_returning_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"
            const sql = `
                INSERT INTO users (email)
                VALUES ($1)
                RETURNING id, created_at
            `;
            "#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_merge_source_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "staging_orders".to_string(),
            columns: vec![crate::ast::Expr::Named("status".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_merge_source_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"
            const sql = `
                MERGE INTO orders o
                USING staging_orders s
                ON o.id = s.id
                WHEN MATCHED THEN UPDATE SET status = s.status
            `;
            "#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_merge_target_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("status".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_merge_target_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"
            const sql = `
                MERGE INTO orders o
                USING staging_orders s
                ON o.id = s.id
                WHEN MATCHED THEN UPDATE SET status = s.status
            `;
            "#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_native_qail_subquery_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("total".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_native_subquery_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"const q = "get users fields id where exists (get orders fields user_id where total > $1)";"#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_nested_subquery_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("total".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_nested_subquery_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"
            const sql = `
                SELECT id
                FROM users
                WHERE id IN (
                    SELECT user_id FROM orders WHERE total > $1
                )
            `;
            "#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }
}
