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

    fn scan_temp_source(prefix: &str, source: &str) -> Vec<CodeReference> {
        let tmp_name = format!(
            "{}_{}_{}.ts",
            prefix,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(&path, source).expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);
        code_refs
    }

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
    fn test_dropped_native_qail_qualified_join_target_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("id".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_native_join_target_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"const q = "get users join posts on users.id = posts.user_id fields users.id";"#,
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
    fn test_dropped_native_qail_join_source_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "posts".to_string(),
            columns: vec![crate::ast::Expr::Named("user_id".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_native_join_source_column_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);
        std::fs::write(
            &path,
            r#"const q = "get users join posts on users.id = posts.user_id fields users.id";"#,
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
    fn test_dropped_native_qail_merge_target_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_native_merge_target_column_{}_{}.ts",
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
            const q = `
                merge users as u using staging_users as s on u.id = s.id
                when matched then update set email = s.email
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
    fn test_dropped_native_qail_merge_target_alias_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("id".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_native_merge_target_alias_column_{}_{}.ts",
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
            const q = `
                merge users as u using staging_users as s on u.id = s.id
                when matched then update set email = s.email
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
    fn test_dropped_native_qail_merge_source_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "staging_users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_native_merge_source_column_{}_{}.ts",
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
            const q = `
                merge users as u using staging_users as s on u.id = s.id
                when matched then update set email = s.email
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
    fn test_dropped_column_named_like_collation_is_not_blocked_by_raw_sql_collation() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("C".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_collation_not_column",
            r#"
            const sql = `
                SELECT id
                FROM users
                WHERE lower(name COLLATE "C") LIKE $1 ESCAPE '\'
                  AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 day'
                ORDER BY EXTRACT(EPOCH FROM created_at)
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 0);
    }

    #[test]
    fn test_dropped_column_named_like_schema_function_is_not_blocked() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("lower".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_schema_function_not_column",
            r#"
            const sql = `
                SELECT pg_catalog.lower(email)
                FROM users
                WHERE pg_catalog.lower(status) = $1
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 0);
    }

    #[test]
    fn test_dropped_column_named_like_cast_type_is_not_blocked() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email_text".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_cast_type_not_column",
            r#"
            const sql = `
                SELECT CAST(email AS public.email_text)
                FROM users
                WHERE status::public.status_name = $1
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 0);
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
    fn test_dropped_raw_sql_select_star_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_select_star_column",
            r#"
            const sql = `
                SELECT *
                FROM users
                WHERE active = true
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_all_star_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_all_star_column",
            r#"
            const sql = `
                SELECT ALL *
                FROM users
                WHERE active = true
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_distinct_on_star_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_distinct_on_star_column",
            r#"
            const sql = `
                SELECT DISTINCT ON (tenant_id) *
                FROM users
                WHERE active = true
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_join_select_star_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("total".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_join_select_star_column",
            r#"
            const sql = `
                SELECT *
                FROM users u
                JOIN orders o ON o.user_id = u.id
                WHERE o.status = $1
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_distinct_on_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("tenant_id".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_distinct_on_column",
            r#"
            const sql = `
                SELECT DISTINCT ON (tenant_id) id
                FROM users
                WHERE status = $1
                ORDER BY tenant_id, created_at DESC
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_filter_projection_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("active".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_filter_projection_column",
            r#"
            const sql = `
                SELECT COUNT(*) FILTER (WHERE active) AS active_count
                FROM users
                WHERE status = $1
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_grouping_sets_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("status".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_grouping_sets_column",
            r#"
            const sql = `
                SELECT tenant_id, status, count(*)
                FROM orders
                GROUP BY GROUPING SETS ((tenant_id, status), (tenant_id), ())
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_rollup_cube_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("channel".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_rollup_cube_column",
            r#"
            const sql = `
                SELECT region, product, sum(total)
                FROM orders
                GROUP BY ROLLUP(region, product), CUBE(channel, status)
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_tablesample_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("active".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_tablesample_column",
            r#"
            const sql = `
                SELECT id
                FROM users TABLESAMPLE BERNOULLI(10) REPEATABLE (42)
                WHERE active = true
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_tablesample_join_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("user_id".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_tablesample_join_column",
            r#"
            const sql = `
                SELECT u.id, o.total
                FROM users TABLESAMPLE SYSTEM (25) u
                JOIN orders TABLESAMPLE BERNOULLI(10) o ON o.user_id = u.id
                WHERE o.status = $1
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_only_inheritance_star_alias_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("active".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_only_star_alias_column",
            r#"
            const sql = `
                SELECT u.id
                FROM ONLY users * AS u
                WHERE u.active = true
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_rows_from_join_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("active".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_rows_from_join_column",
            r#"
            const sql = `
                SELECT u.id
                FROM ROWS FROM (jsonb_to_recordset($1) AS (id int)) AS r(id)
                JOIN users u ON u.id = r.id
                WHERE u.active = true
            `;
            "#,
        );

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
    fn test_dropped_target_column_does_not_match_qualified_update_from_source_column() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("state".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_update_from_source_false_target_{}_{}.ts",
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

        assert!(impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 0);
    }

    #[test]
    fn test_dropped_raw_sql_update_from_unqualified_source_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "payments".to_string(),
            columns: vec![crate::ast::Expr::Named("state".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_update_from_unqualified_column_{}_{}.ts",
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
                UPDATE orders
                SET status = state
                FROM payments
                WHERE orders.payment_id = payments.id
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
    fn test_dropped_raw_sql_delete_using_unqualified_source_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("disabled".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_delete_using_unqualified_column_{}_{}.ts",
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
                DELETE FROM sessions
                USING users
                WHERE sessions.user_id = id
                  AND disabled = true
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
    fn test_dropped_target_column_does_not_match_qualified_delete_using_source_column() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "sessions".to_string(),
            columns: vec![crate::ast::Expr::Named("disabled".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_delete_using_source_false_target_{}_{}.ts",
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
                DELETE FROM sessions s
                USING users u
                WHERE s.user_id = u.id
                  AND u.disabled = true
            `;
            "#,
        )
        .expect("write temp source");
        let code_refs = super::super::scanner::CodebaseScanner::new().scan(&path);
        let _ = std::fs::remove_file(&path);

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 0);
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
    fn test_dropped_raw_sql_insert_alias_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_insert_alias_column_{}_{}.ts",
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
                INSERT INTO users AS u (email)
                VALUES ($1)
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
    fn test_dropped_raw_sql_insert_alias_returning_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("created_at".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_insert_alias_returning_column",
            r#"
            const sql = `
                INSERT INTO users AS u (email)
                VALUES ($1)
                RETURNING u.id, u.created_at
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_insert_alias_conflict_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("active".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_insert_alias_conflict_column",
            r#"
            const sql = `
                INSERT INTO users AS u (email)
                VALUES ($1)
                ON CONFLICT (email)
                DO UPDATE SET last_seen = EXCLUDED.last_seen
                WHERE u.active
                RETURNING u.id
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_update_only_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_update_only_column_{}_{}.ts",
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
                UPDATE ONLY users
                SET email = $1
                WHERE id = $2
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
    fn test_dropped_raw_sql_delete_only_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };

        let tmp_name = format!(
            "qail_impact_raw_sql_delete_only_column_{}_{}.ts",
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
                DELETE FROM ONLY users
                WHERE email = $1
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
    fn test_dropped_raw_sql_returning_alias_is_not_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("user_id".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_returning_alias_not_column",
            r#"
            const sql = `
                INSERT INTO users (email)
                VALUES ($1)
                RETURNING id AS user_id
            `;
            "#,
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 0);
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
    fn test_dropped_raw_sql_merge_returning_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("created_at".to_string())],
            ..Default::default()
        };

        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_merge_returning_column",
            r#"
            const sql = `
                MERGE INTO orders o
                USING staging_orders s
                ON o.id = s.id
                WHEN MATCHED THEN UPDATE SET status = s.status
                RETURNING created_at AS merged_at
            `;
            "#,
        );

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

    #[test]
    fn test_dropped_raw_sql_truncate_table_is_breaking() {
        let cmd = Qail {
            action: Action::Drop,
            table: "users".to_string(),
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_truncate_table",
            "const sql = `TRUNCATE TABLE users`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_multi_truncate_table_is_breaking() {
        let cmd = Qail {
            action: Action::Drop,
            table: "orders".to_string(),
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_multi_truncate_table",
            "const sql = `TRUNCATE TABLE users, orders CASCADE`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_copy_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_copy_column",
            "const sql = `COPY users (email, status) FROM STDIN`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_lock_table_is_breaking() {
        let cmd = Qail {
            action: Action::Drop,
            table: "users".to_string(),
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_lock_table",
            "const sql = `LOCK TABLE users IN ACCESS EXCLUSIVE MODE`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_create_index_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_create_index_column",
            "const sql = `CREATE INDEX users_email_idx ON users (email) WHERE active = true`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_create_view_source_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("active".to_string())],
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_create_view_source_column",
            "const sql = `CREATE VIEW active_users AS SELECT id FROM users WHERE active = true`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_alter_fk_referenced_table_is_breaking() {
        let cmd = Qail {
            action: Action::Drop,
            table: "orgs".to_string(),
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_alter_fk_referenced_table",
            "const sql = `ALTER TABLE users ADD CONSTRAINT users_org_fk FOREIGN KEY (org_id) REFERENCES orgs(id)`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_comment_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("email".to_string())],
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_comment_column",
            "const sql = `COMMENT ON COLUMN users.email IS 'legacy email'`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_grant_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("status".to_string())],
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_grant_column",
            "const sql = `GRANT SELECT (email), UPDATE (status) ON TABLE users TO app_role`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_copy_where_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("active".to_string())],
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_copy_where_column",
            "const sql = `COPY users (email) FROM STDIN WHERE active = true`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_maintenance_table_is_breaking() {
        let cmd = Qail {
            action: Action::Drop,
            table: "users".to_string(),
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_maintenance_table",
            "const sql = `VACUUM (VERBOSE, ANALYZE) users (deleted_at)`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_refresh_view_is_breaking() {
        let cmd = Qail {
            action: Action::Drop,
            table: "active_users".to_string(),
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_refresh_view",
            "const sql = `REFRESH MATERIALIZED VIEW CONCURRENTLY active_users`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_drop_table_reference_is_breaking() {
        let cmd = Qail {
            action: Action::Drop,
            table: "users".to_string(),
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_drop_table_reference",
            "const sql = `DROP TABLE IF EXISTS users, orders CASCADE`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_create_trigger_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "orders".to_string(),
            columns: vec![crate::ast::Expr::Named("status".to_string())],
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_create_trigger_column",
            "const sql = `CREATE TRIGGER order_status_changed BEFORE UPDATE OF status ON orders FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION audit_order()`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_publication_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("active".to_string())],
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_publication_column",
            "const sql = `CREATE PUBLICATION tenant_pub FOR TABLE users (email) WHERE (active)`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_create_table_referenced_table_is_breaking() {
        let cmd = Qail {
            action: Action::Drop,
            table: "orgs".to_string(),
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_create_table_referenced_table",
            "const sql = `CREATE TABLE invoices (org_id uuid REFERENCES orgs(id))`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }

    #[test]
    fn test_dropped_raw_sql_alter_policy_column_is_breaking() {
        let cmd = Qail {
            action: Action::AlterDrop,
            table: "users".to_string(),
            columns: vec![crate::ast::Expr::Named("tenant_id".to_string())],
            ..Default::default()
        };
        let code_refs = scan_temp_source(
            "qail_impact_raw_sql_alter_policy_column",
            "const sql = `ALTER POLICY tenant_users ON users USING (tenant_id = current_setting('app.tenant_id')::uuid)`;",
        );

        let old_schema = Schema::new();
        let new_schema = Schema::new();
        let impact = MigrationImpact::analyze(&[cmd], &code_refs, &old_schema, &new_schema);

        assert!(!impact.safe_to_run, "{code_refs:?}");
        assert_eq!(impact.breaking_changes.len(), 1);
    }
}
