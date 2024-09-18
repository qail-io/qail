//! Named Migration Format
//!
//! Provides metadata parsing for migration files with headers:
//! ```sql
//! -- migration: 003_add_user_avatar
//! -- depends: 002_add_users
//! -- author: orion
//! ```

use std::collections::HashSet;

/// Metadata for a named migration.
#[derive(Debug, Clone, Default)]
pub struct MigrationMeta {
    /// Migration name (e.g., "003_add_user_avatar")
    pub name: String,
    /// Dependencies - migrations that must run before this one
    pub depends: Vec<String>,
    /// Author of the migration
    pub author: Option<String>,
    /// Creation timestamp
    pub created: Option<String>,
}

impl MigrationMeta {
    /// Create a new migration metadata with just a name.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    pub fn with_depends(mut self, deps: Vec<String>) -> Self {
        self.depends = deps;
        self
    }

    pub fn with_author(mut self, author: &str) -> Self {
        self.author = Some(author.to_string());
        self
    }

    /// Generate metadata header for a migration file.
    pub fn to_header(&self) -> String {
        let mut lines = vec![format!("-- migration: {}", self.name)];

        if !self.depends.is_empty() {
            lines.push(format!("-- depends: {}", self.depends.join(", ")));
        }

        if let Some(ref author) = self.author {
            lines.push(format!("-- author: {}", author));
        }

        if let Some(ref created) = self.created {
            lines.push(format!("-- created: {}", created));
        }

        lines.push(String::new()); // blank line after header
        lines.join("\n")
    }
}

/// Parse migration metadata from file content.
/// Looks for lines starting with `-- migration:`, `-- depends:`, `-- author:`, `-- created:`.
pub fn parse_migration_meta(content: &str) -> Option<MigrationMeta> {
    let mut meta = MigrationMeta::default();
    let mut found_name = false;

    for line in content.lines() {
        let line = line.trim();

        if let Some(name) = line.strip_prefix("-- migration:") {
            meta.name = name.trim().to_string();
            found_name = true;
        } else if let Some(deps) = line.strip_prefix("-- depends:") {
            meta.depends = deps
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        } else if let Some(author) = line.strip_prefix("-- author:") {
            meta.author = Some(author.trim().to_string());
        } else if let Some(created) = line.strip_prefix("-- created:") {
            meta.created = Some(created.trim().to_string());
        } else if !line.starts_with("--") && !line.is_empty() {
            // Stop parsing once we hit non-comment content
            break;
        }
    }

    if found_name { Some(meta) } else { None }
}

/// Validate migration dependencies (check for cycles and missing deps).
pub fn validate_dependencies(migrations: &[MigrationMeta]) -> Result<Vec<String>, String> {
    let names: HashSet<_> = migrations.iter().map(|m| m.name.as_str()).collect();

    for mig in migrations {
        for dep in &mig.depends {
            if !names.contains(dep.as_str()) {
                return Err(format!(
                    "Migration '{}' depends on '{}' which doesn't exist",
                    mig.name, dep
                ));
            }
        }
    }

    // Topological sort to get execution order
    let mut order = Vec::new();
    let mut visited = HashSet::new();
    let mut in_progress = HashSet::new();

    fn visit<'a>(
        name: &'a str,
        migrations: &'a [MigrationMeta],
        visited: &mut HashSet<&'a str>,
        in_progress: &mut HashSet<&'a str>,
        order: &mut Vec<String>,
    ) -> Result<(), String> {
        if in_progress.contains(name) {
            return Err(format!("Circular dependency detected involving '{}'", name));
        }
        if visited.contains(name) {
            return Ok(());
        }

        in_progress.insert(name);

        if let Some(mig) = migrations.iter().find(|m| m.name == name) {
            for dep in &mig.depends {
                visit(dep, migrations, visited, in_progress, order)?;
            }
        }

        in_progress.remove(name);
        visited.insert(name);
        order.push(name.to_string());

        Ok(())
    }

    for mig in migrations {
        visit(
            &mig.name,
            migrations,
            &mut visited,
            &mut in_progress,
            &mut order,
        )?;
    }

    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_migration_meta() {
        let content = r#"-- migration: 003_add_avatars
-- depends: 001_init, 002_add_users
-- author: orion

+table avatars {
  id UUID primary_key
}
"#;
        let meta = parse_migration_meta(content).unwrap();
        assert_eq!(meta.name, "003_add_avatars");
        assert_eq!(meta.depends, vec!["001_init", "002_add_users"]);
        assert_eq!(meta.author, Some("orion".to_string()));
    }

    #[test]
    fn test_meta_to_header() {
        let meta = MigrationMeta::new("test_migration")
            .with_depends(vec!["dep1".to_string()])
            .with_author("tester");

        let header = meta.to_header();
        assert!(header.contains("-- migration: test_migration"));
        assert!(header.contains("-- depends: dep1"));
        assert!(header.contains("-- author: tester"));
    }

    #[test]
    fn test_dependency_validation() {
        let migs = vec![
            MigrationMeta::new("001_init"),
            MigrationMeta::new("002_users").with_depends(vec!["001_init".to_string()]),
            MigrationMeta::new("003_posts").with_depends(vec!["002_users".to_string()]),
        ];

        let order = validate_dependencies(&migs).unwrap();
        assert_eq!(order, vec!["001_init", "002_users", "003_posts"]);
    }
}
