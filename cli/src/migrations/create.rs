//! Migration creation

use anyhow::Result;
use colored::*;

/// Create a new named migration file.
pub fn migrate_create(name: &str, depends: Option<&str>, author: Option<&str>) -> Result<()> {
    use qail_core::migrate::MigrationMeta;
    use std::path::Path;

    println!("{}", "📝 Creating Named Migration".cyan().bold());
    println!();

    let timestamp = chrono::Local::now().format("%Y%m%d%H%M%S").to_string();
    let created = chrono::Local::now().to_rfc3339();

    let mut meta = MigrationMeta::new(&format!("{}_{}", timestamp, name));
    meta.created = Some(created);

    if let Some(deps) = depends {
        meta.depends = deps
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }

    if let Some(auth) = author {
        meta.author = Some(auth.to_string());
    }

    // Ensure migrations directory exists
    let migrations_dir = Path::new("migrations");
    if !migrations_dir.exists() {
        std::fs::create_dir_all(migrations_dir)?;
        println!("  Created {} directory", "migrations/".yellow());
    }

    let filename = format!("{}_{}.qail", timestamp, name);
    let filepath = migrations_dir.join(&filename);

    let content = format!(
        "{}# Migration: {}\n# Add your schema changes below:\n# +table example {{\n#   id UUID primary_key\n# }}\n# +column users.new_field TEXT\n",
        meta.to_header(),
        name
    );

    std::fs::write(&filepath, &content)?;

    println!("  {} {}", "✓ Created:".green(), filepath.display());
    println!();
    println!("  Migration: {}", meta.name.cyan());
    if !meta.depends.is_empty() {
        println!("  Depends:   {}", meta.depends.join(", ").yellow());
    }
    if let Some(ref auth) = meta.author {
        println!("  Author:    {}", auth.dimmed());
    }
    println!();
    println!("  Edit the file to add your schema changes, then run:");
    println!(
        "    {} old.qail:{}",
        "qail migrate up".cyan(),
        filename.yellow()
    );

    Ok(())
}
