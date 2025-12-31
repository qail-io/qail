//! Migration creation

use anyhow::Result;
use colored::*;

/// Create a new named migration file pair (up + down) with timestamp prefix.
/// 
/// ## Generated Files
/// 
/// ```text
/// migrations/
/// ├── 20251231093400_add_users.up.qail
/// └── 20251231093400_add_users.down.qail
/// ```
pub fn migrate_create(name: &str, depends: Option<&str>, author: Option<&str>) -> Result<()> {
    use std::path::Path;

    println!("{}", "📝 Creating Migration".cyan().bold());
    println!();

    let timestamp = chrono::Local::now().format("%Y%m%d%H%M%S").to_string();
    let created = chrono::Local::now().to_rfc3339();

    // Ensure migrations directory exists
    let migrations_dir = Path::new("migrations");
    if !migrations_dir.exists() {
        std::fs::create_dir_all(migrations_dir)?;
        println!("  Created {} directory", "migrations/".yellow());
    }

    // Build metadata header
    let mut meta_lines = vec![
        format!("-- @name: {}_{}", timestamp, name),
        format!("-- @created: {}", created),
    ];

    if let Some(auth) = author {
        meta_lines.push(format!("-- @author: {}", auth));
    }

    if let Some(deps) = depends {
        meta_lines.push(format!("-- @depends: {}", deps));
    }

    let meta_header = meta_lines.join("\n");

    // Create UP migration
    let up_filename = format!("{}_{}.up.qail", timestamp, name);
    let up_filepath = migrations_dir.join(&up_filename);
    let up_content = format!(
        "{}\n\n-- Add your UP migration below:\n-- Example: make users (id serial primary, email text unique)\n\n",
        meta_header
    );
    std::fs::write(&up_filepath, &up_content)?;

    // Create DOWN migration
    let down_filename = format!("{}_{}.down.qail", timestamp, name);
    let down_filepath = migrations_dir.join(&down_filename);
    let down_content = format!(
        "{}\n\n-- Add your DOWN (rollback) migration below:\n-- Example: drop users\n\n",
        meta_header
    );
    std::fs::write(&down_filepath, &down_content)?;

    println!("  {} {}", "✓ Created:".green(), up_filepath.display());
    println!("  {} {}", "✓ Created:".green(), down_filepath.display());
    println!();
    println!("  Migration: {}", format!("{}_{}", timestamp, name).cyan());
    
    if let Some(deps) = depends {
        println!("  Depends:   {}", deps.yellow());
    }
    if let Some(auth) = author {
        println!("  Author:    {}", auth.dimmed());
    }
    println!();
    println!("  Next steps:");
    println!("    1. Edit {} with your schema changes", up_filename.yellow());
    println!("    2. Edit {} with rollback logic", down_filename.yellow());
    println!("    3. Run: {} schema.qail:migrations/ postgres://...", "qail migrate up".cyan());

    Ok(())
}

