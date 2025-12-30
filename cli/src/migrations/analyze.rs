//! Migration impact analyzer

use anyhow::Result;
use colored::*;
use qail_core::migrate::{diff_schemas, parse_qail};

use crate::sql_gen::cmd_to_sql;

/// Analyze migration impact on codebase before executing.
pub fn migrate_analyze(schema_diff_path: &str, codebase_path: &str, ci_flag: bool) -> Result<()> {
    use qail_core::analyzer::{CodebaseScanner, MigrationImpact};
    use std::path::Path;

    // Detect CI mode: explicit flag OR environment variable
    let ci_mode = ci_flag 
        || std::env::var("CI").is_ok() 
        || std::env::var("GITHUB_ACTIONS").is_ok();

    if !ci_mode {
        println!("{}", "🔍 Migration Impact Analyzer".cyan().bold());
        println!();
    }

    let (old_schema, new_schema, cmds) =
        if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
            let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
            let old_path = parts[0];
            let new_path = parts[1];

            println!("  Schema: {} → {}", old_path.yellow(), new_path.yellow());

            let old_content = std::fs::read_to_string(old_path)
                .map_err(|e| anyhow::anyhow!("Failed to read old schema: {}", e))?;
            let new_content = std::fs::read_to_string(new_path)
                .map_err(|e| anyhow::anyhow!("Failed to read new schema: {}", e))?;

            let old = parse_qail(&old_content)
                .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;
            let new = parse_qail(&new_content)
                .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

            let cmds = diff_schemas(&old, &new);
            (old, new, cmds)
        } else {
            return Err(anyhow::anyhow!(
                "Please provide two .qail files: old.qail:new.qail"
            ));
        };

    if cmds.is_empty() {
        println!(
            "{}",
            "✓ No migrations needed - schemas are identical".green()
        );
        return Ok(());
    }

    // Format codebase path for human readability
    let display_path = {
        let p = codebase_path.to_string();
        if let Ok(home) = std::env::var("HOME") {
            if p.starts_with(&home) {
                p.replacen(&home, "~", 1)
            } else {
                p
            }
        } else {
            p
        }
    };
    
    println!("  Codebase: {}", display_path.yellow());
    println!();

    // Scan codebase
    let scanner = CodebaseScanner::new();
    let code_path = Path::new(codebase_path);

    if !code_path.exists() {
        return Err(anyhow::anyhow!(
            "Codebase path not found: {}",
            codebase_path
        ));
    }

    println!("{}", "Scanning codebase...".dimmed());
    let scan_result = scanner.scan_with_details(code_path);
    
    // Show per-file analysis breakdown with badges
    println!("🔍 {}", "Analyzing files...".dimmed());
    for file_analysis in &scan_result.files {
        let relative_path = file_analysis.file.strip_prefix(code_path).unwrap_or(&file_analysis.file);
        let mode_badge = match file_analysis.mode {
            qail_core::analyzer::AnalysisMode::RustAST => "🦀",
            qail_core::analyzer::AnalysisMode::Regex => {
                match file_analysis.file.extension().and_then(|e| e.to_str()) {
                    Some("ts") | Some("tsx") | Some("js") | Some("jsx") => "📘",
                    Some("py") => "🐍",
                    _ => "📄",
                }
            }
        };
        let mode_name = match file_analysis.mode {
            qail_core::analyzer::AnalysisMode::RustAST => "AST",
            qail_core::analyzer::AnalysisMode::Regex => "Regex",
        };
        println!("   ├── {} {} ({}: {} refs)", 
            mode_badge, 
            relative_path.display().to_string().cyan(),
            mode_name.dimmed(),
            file_analysis.ref_count
        );
    }
    if !scan_result.files.is_empty() {
        println!("   └── {} files analyzed", scan_result.files.len());
    }
    println!();
    
    let code_refs = scan_result.refs;
    println!("  Found {} query references\n", code_refs.len());

    // Analyze impact
    let impact = MigrationImpact::analyze(&cmds, &code_refs, &old_schema, &new_schema);

    if impact.safe_to_run {
        if ci_mode {
            println!("✅ No breaking changes detected");
        } else {
            println!("{}", "✓ Migration is safe to run".green().bold());
            println!("  No breaking changes detected in codebase\n");

            println!("{}", "Migration preview:".cyan());
            for cmd in &cmds {
                let sql = cmd_to_sql(cmd);
                println!("  {}", sql);
            }
        }
    } else {
        if ci_mode {
            print_ci_breaking_changes(&impact, code_path);
            std::process::exit(1);
        } else {
            print_human_breaking_changes(&impact);
        }
    }

    Ok(())
}

fn print_ci_breaking_changes(impact: &qail_core::analyzer::MigrationImpact, code_path: &std::path::Path) {
    // Find repo root
    let repo_root = {
        let mut current = code_path.to_path_buf();
        loop {
            if current.join(".git").exists() || current.join("Cargo.toml").exists() {
                break current;
            }
            if !current.pop() {
                break code_path.to_path_buf();
            }
        }
    };
    
    for change in &impact.breaking_changes {
        match change {
            qail_core::analyzer::BreakingChange::DroppedTable { table, references } => {
                for r in references {
                    let file_path = r.file.strip_prefix(&repo_root).unwrap_or(&r.file);
                    println!(
                        "::error file={},line={},title=Breaking Change::Table '{}' is being dropped but referenced here",
                        file_path.display(),
                        r.line,
                        table
                    );
                }
            }
            qail_core::analyzer::BreakingChange::DroppedColumn { table, column, references } => {
                for r in references {
                    let file_path = r.file.strip_prefix(&repo_root).unwrap_or(&r.file);
                    println!(
                        "::error file={},line={},title=Breaking Change::Column '{}.{}' is being dropped but referenced here in {}",
                        file_path.display(),
                        r.line,
                        table,
                        column,
                        r.snippet
                    );
                }
            }
            qail_core::analyzer::BreakingChange::RenamedColumn { table, old_name, new_name, references } => {
                for r in references {
                    let file_path = r.file.strip_prefix(&repo_root).unwrap_or(&r.file);
                    println!(
                        "::warning file={},line={},title=Column Renamed::Column '{}.{}' renamed to '{}', update reference",
                        file_path.display(),
                        r.line,
                        table,
                        old_name,
                        new_name
                    );
                }
            }
            _ => {}
        }
    }
    println!("::group::Migration Impact Summary");
    println!("{} breaking changes found in {} files", impact.breaking_changes.len(), impact.affected_files);
    println!("::endgroup::");
}

fn print_human_breaking_changes(impact: &qail_core::analyzer::MigrationImpact) {
    println!("{}", "⚠️  BREAKING CHANGES DETECTED".red().bold());
    println!();
    println!("Affected files: {}", impact.affected_files);
    println!();

    for change in &impact.breaking_changes {
        match change {
            qail_core::analyzer::BreakingChange::DroppedTable { table, references } => {
                println!(
                    "┌─ {} {} ({} references) ─────────────────────────┐",
                    "DROP TABLE".red(),
                    table.yellow(),
                    references.len()
                );
                for r in references.iter().take(5) {
                    println!(
                        "│ {} {}:{} → {}",
                        "❌".red(),
                        r.file.display(),
                        r.line,
                        r.snippet.cyan()
                    );
                }
                if references.len() > 5 {
                    println!("│ ... and {} more", references.len() - 5);
                }
                println!("└──────────────────────────────────────────────────────┘");
                println!();
            }
            qail_core::analyzer::BreakingChange::DroppedColumn { table, column, references } => {
                println!(
                    "┌─ {} {}.{} ({} references) ─────────────────┐",
                    "DROP COLUMN".red(),
                    table.yellow(),
                    column.yellow(),
                    references.len()
                );
                for r in references.iter().take(5) {
                    if matches!(r.query_type, qail_core::analyzer::QueryType::RawSql) {
                        println!(
                            "│ {} {}:{} → {} uses {}",
                            "⚠️  RAW SQL".yellow(),
                            r.file.display(),
                            r.line,
                            r.snippet.cyan(),
                            column.red().bold()
                        );
                    } else {
                        println!(
                            "│ {} {}:{} → uses {} in {}",
                            "❌".red(),
                            r.file.display(),
                            r.line,
                            column.cyan().bold(),
                            r.snippet.dimmed()
                        );
                    }
                }
                println!("└──────────────────────────────────────────────────────┘");
                println!();
            }
            qail_core::analyzer::BreakingChange::RenamedColumn { table, references, .. } => {
                println!(
                    "┌─ {} on {} ({} references) ───────────────────┐",
                    "RENAME".yellow(),
                    table.yellow(),
                    references.len()
                );
                for r in references.iter().take(5) {
                    println!(
                        "│ {} {}:{} → {}",
                        "⚠️ ".yellow(),
                        r.file.display(),
                        r.line,
                        r.snippet.cyan()
                    );
                }
                println!("└──────────────────────────────────────────────────────┘");
                println!();
            }
            _ => {}
        }
    }

    println!("What would you like to do?");
    println!(
        "  1. {} (DANGEROUS - will cause {} runtime errors)",
        "Run anyway".red(),
        impact.breaking_changes.len()
    );
    println!(
        "  2. {} (show SQL, don't execute)",
        "Dry-run first".yellow()
    );
    println!("  3. {} (exit)", "Let me fix the code first".green());
}
