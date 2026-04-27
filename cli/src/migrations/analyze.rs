//! Migration impact analyzer

use crate::colors::*;
use anyhow::Result;
use qail_core::migrate::{diff_schemas_checked, parse_qail_file};
use serde::Serialize;

use crate::sql_gen::cmd_to_sql;

#[derive(Serialize)]
struct AnalyzeJsonReport {
    schema_diff: String,
    codebase: String,
    ci_mode: bool,
    safe_to_run: bool,
    affected_files: usize,
    references_scanned: usize,
    scanned_files: Vec<AnalyzedFile>,
    breaking_changes: Vec<BreakingChangeJson>,
}

#[derive(Serialize)]
struct AnalyzedFile {
    file: String,
    mode: String,
    references: usize,
}

#[derive(Serialize)]
struct BreakingChangeJson {
    kind: String,
    table: String,
    column: Option<String>,
    old_name: Option<String>,
    new_name: Option<String>,
    old_type: Option<String>,
    new_type: Option<String>,
    references: Vec<CodeRefJson>,
}

#[derive(Serialize)]
struct CodeRefJson {
    file: String,
    line: usize,
    query_type: String,
    snippet: String,
}

/// Analyze migration impact. See [full docs](https://dev.qail.io/docs/features/analyzer.html).
pub fn migrate_analyze(
    schema_diff_path: &str,
    codebase_path: &str,
    ci_flag: bool,
    json_mode: bool,
) -> Result<()> {
    use qail_core::analyzer::{CodebaseScanner, MigrationImpact};
    use std::path::Path;

    // Detect CI mode: explicit flag OR environment variable
    let ci_mode = ci_flag || std::env::var("CI").is_ok() || std::env::var("GITHUB_ACTIONS").is_ok();

    if !ci_mode && !json_mode {
        println!("{}", "🔍 Migration Impact Analyzer".cyan().bold());
        println!();
    }

    let (old_schema, new_schema, cmds) =
        if schema_diff_path.contains(':') && !schema_diff_path.starts_with("postgres") {
            let parts: Vec<&str> = schema_diff_path.splitn(2, ':').collect();
            let old_path = parts[0];
            let new_path = parts[1];

            if !json_mode {
                println!("  Schema: {} → {}", old_path.yellow(), new_path.yellow());
            }

            let old = parse_qail_file(old_path)
                .map_err(|e| anyhow::anyhow!("Failed to parse old schema: {}", e))?;
            let new = parse_qail_file(new_path)
                .map_err(|e| anyhow::anyhow!("Failed to parse new schema: {}", e))?;

            let cmds = diff_schemas_checked(&old, &new).map_err(|e| {
                anyhow::anyhow!("State-based diff unsupported for this schema pair: {}", e)
            })?;
            (old, new, cmds)
        } else {
            return Err(anyhow::anyhow!(
                "Please provide two .qail files: old.qail:new.qail"
            ));
        };

    if cmds.is_empty() {
        if json_mode {
            let report = AnalyzeJsonReport {
                schema_diff: schema_diff_path.to_string(),
                codebase: codebase_path.to_string(),
                ci_mode,
                safe_to_run: true,
                affected_files: 0,
                references_scanned: 0,
                scanned_files: Vec::new(),
                breaking_changes: Vec::new(),
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            println!(
                "{}",
                "✓ No migrations needed - schemas are identical".green()
            );
        }
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

    if !json_mode {
        println!("  Codebase: {}", display_path.yellow());
        println!();
    }

    // Scan codebase
    let scanner = CodebaseScanner::new();
    let code_path = Path::new(codebase_path);

    if !code_path.exists() {
        return Err(anyhow::anyhow!(
            "Codebase path not found: {}",
            codebase_path
        ));
    }

    if !json_mode {
        println!("{}", "Scanning codebase...".dimmed());
    }
    let scan_result = scanner.scan_with_details(code_path);

    // Show per-file analysis breakdown with badges
    if !json_mode {
        println!("🔍 {}", "Analyzing files...".dimmed());
        for file_analysis in &scan_result.files {
            let relative_path = file_analysis
                .file
                .strip_prefix(code_path)
                .unwrap_or(&file_analysis.file);
            let mode_badge = match file_analysis.mode {
                qail_core::analyzer::AnalysisMode::RustAST => "🦀",
                qail_core::analyzer::AnalysisMode::TextSemantic => {
                    match file_analysis.file.extension().and_then(|e| e.to_str()) {
                        Some("ts") | Some("tsx") | Some("js") | Some("jsx") => "📘",
                        Some("py") => "🐍",
                        _ => "📄",
                    }
                }
            };
            let mode_name = match file_analysis.mode {
                qail_core::analyzer::AnalysisMode::RustAST => "AST",
                qail_core::analyzer::AnalysisMode::TextSemantic => "TextSemantic",
            };
            println!(
                "   ├── {} {} ({}: {} refs)",
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
    }

    let code_refs = &scan_result.refs;
    if !json_mode {
        println!("  Found {} query references\n", code_refs.len());
    }

    // Analyze impact
    let impact = MigrationImpact::analyze(&cmds, code_refs, &old_schema, &new_schema);

    if json_mode {
        let report = build_json_report(
            schema_diff_path,
            codebase_path,
            ci_mode,
            &scan_result,
            &impact,
            code_path,
        );
        println!("{}", serde_json::to_string_pretty(&report)?);
        if ci_mode && !impact.safe_to_run {
            std::process::exit(1);
        }
        return Ok(());
    }

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
    } else if ci_mode {
        print_ci_breaking_changes(&impact, code_path);
        std::process::exit(1);
    } else {
        print_human_breaking_changes(&impact);
    }

    Ok(())
}

fn build_json_report(
    schema_diff_path: &str,
    codebase_path: &str,
    ci_mode: bool,
    scan_result: &qail_core::analyzer::ScanResult,
    impact: &qail_core::analyzer::MigrationImpact,
    code_path: &std::path::Path,
) -> AnalyzeJsonReport {
    let scanned_files = scan_result
        .files
        .iter()
        .map(|f| AnalyzedFile {
            file: f
                .file
                .strip_prefix(code_path)
                .unwrap_or(&f.file)
                .display()
                .to_string(),
            mode: match f.mode {
                qail_core::analyzer::AnalysisMode::RustAST => "rust_ast".to_string(),
                qail_core::analyzer::AnalysisMode::TextSemantic => "text_semantic".to_string(),
            },
            references: f.ref_count,
        })
        .collect::<Vec<_>>();

    let breaking_changes = impact
        .breaking_changes
        .iter()
        .map(|change| match change {
            qail_core::analyzer::BreakingChange::DroppedColumn {
                table,
                column,
                references,
            } => BreakingChangeJson {
                kind: "dropped_column".to_string(),
                table: table.clone(),
                column: Some(column.clone()),
                old_name: None,
                new_name: None,
                old_type: None,
                new_type: None,
                references: refs_to_json(references, code_path),
            },
            qail_core::analyzer::BreakingChange::DroppedTable { table, references } => {
                BreakingChangeJson {
                    kind: "dropped_table".to_string(),
                    table: table.clone(),
                    column: None,
                    old_name: None,
                    new_name: None,
                    old_type: None,
                    new_type: None,
                    references: refs_to_json(references, code_path),
                }
            }
            qail_core::analyzer::BreakingChange::RenamedColumn {
                table,
                old_name,
                new_name,
                references,
            } => BreakingChangeJson {
                kind: "renamed_column".to_string(),
                table: table.clone(),
                column: None,
                old_name: Some(old_name.clone()),
                new_name: Some(new_name.clone()),
                old_type: None,
                new_type: None,
                references: refs_to_json(references, code_path),
            },
            qail_core::analyzer::BreakingChange::TypeChanged {
                table,
                column,
                old_type,
                new_type,
                references,
            } => BreakingChangeJson {
                kind: "type_changed".to_string(),
                table: table.clone(),
                column: Some(column.clone()),
                old_name: None,
                new_name: None,
                old_type: Some(old_type.clone()),
                new_type: Some(new_type.clone()),
                references: refs_to_json(references, code_path),
            },
        })
        .collect::<Vec<_>>();

    AnalyzeJsonReport {
        schema_diff: schema_diff_path.to_string(),
        codebase: codebase_path.to_string(),
        ci_mode,
        safe_to_run: impact.safe_to_run,
        affected_files: impact.affected_files,
        references_scanned: scan_result.refs.len(),
        scanned_files,
        breaking_changes,
    }
}

fn refs_to_json(
    refs: &[qail_core::analyzer::CodeReference],
    code_path: &std::path::Path,
) -> Vec<CodeRefJson> {
    refs.iter()
        .map(|r| CodeRefJson {
            file: r
                .file
                .strip_prefix(code_path)
                .unwrap_or(&r.file)
                .display()
                .to_string(),
            line: r.line,
            query_type: match r.query_type {
                qail_core::analyzer::QueryType::Qail => "qail".to_string(),
                qail_core::analyzer::QueryType::RawSql => "raw_sql".to_string(),
            },
            snippet: r.snippet.clone(),
        })
        .collect()
}

fn print_ci_breaking_changes(
    impact: &qail_core::analyzer::MigrationImpact,
    code_path: &std::path::Path,
) {
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
            qail_core::analyzer::BreakingChange::DroppedColumn {
                table,
                column,
                references,
            } => {
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
            qail_core::analyzer::BreakingChange::RenamedColumn {
                table,
                old_name,
                new_name,
                references,
            } => {
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
    println!(
        "{} breaking changes found in {} files",
        impact.breaking_changes.len(),
        impact.affected_files
    );
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
            qail_core::analyzer::BreakingChange::DroppedColumn {
                table,
                column,
                references,
            } => {
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
            qail_core::analyzer::BreakingChange::RenamedColumn {
                table, references, ..
            } => {
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
