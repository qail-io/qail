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
                    if let Some(refs) = table_refs.get(&cmd.table)
                        && !refs.is_empty()
                    {
                        impact.breaking_changes.push(BreakingChange::DroppedTable {
                            table: cmd.table.clone(),
                            references: refs.iter().map(|r| (*r).clone()).collect(),
                        });
                    }
                }
                Action::AlterDrop => {
                    for col_expr in &cmd.columns {
                        if let crate::ast::Expr::Named(col_name) = col_expr {
                            let key = (cmd.table.clone(), col_name.clone());
                            if let Some(refs) = column_refs.get(&key)
                                && !refs.is_empty()
                            {
                                impact.breaking_changes.push(BreakingChange::DroppedColumn {
                                    table: cmd.table.clone(),
                                    column: col_name.clone(),
                                    references: refs.iter().map(|r| (*r).clone()).collect(),
                                });
                            }
                        }
                    }
                }
                Action::Mod => {
                    // Rename operation - check for references to old name
                    // Would need to parse the rename details from the command
                    // For now, flag any table with Mod action
                    if let Some(refs) = table_refs.get(&cmd.table)
                        && !refs.is_empty()
                    {
                        impact.breaking_changes.push(BreakingChange::RenamedColumn {
                            table: cmd.table.clone(),
                            old_name: "unknown".to_string(),
                            new_name: "unknown".to_string(),
                            references: refs.iter().map(|r| (*r).clone()).collect(),
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
                            column,  // The actual matched column
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
            snippet: "get::users".to_string(),
        };

        let old_schema = Schema::new();
        let new_schema = Schema::new();

        let impact = MigrationImpact::analyze(&[cmd], &[code_ref], &old_schema, &new_schema);

        assert!(!impact.safe_to_run);
        assert_eq!(impact.breaking_changes.len(), 1);
    }
}
