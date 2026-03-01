//! Modular schema tooling (`qail schema ...`).

use crate::colors::*;
use anyhow::{Context, Result, anyhow, bail};
use qail_core::migrate::{CommentTarget, Schema, parse_qail, parse_qail_file, to_qail_string};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

const ORDER_FILE: &str = "_order.qail";
const STRICT_ENV_VAR: &str = "QAIL_SCHEMA_STRICT_MANIFEST";
const STRICT_DIRECTIVE: &str = "qail: strict-manifest";
const STRICT_SHORTHAND: &str = "!strict";
const QAIL_HEADER: &str = "# QAIL Schema\n\n";

#[derive(Debug, Default)]
struct OrderReport {
    strict_manifest: bool,
    duplicate_entries: Vec<String>,
    unresolved_entries: Vec<String>,
    unlisted_modules: Vec<String>,
}

/// Run schema diagnostics for modular schema sources.
pub fn doctor_schema(schema_source: &str, fail_on_warning: bool) -> Result<()> {
    let root = resolve_schema_root(Path::new(schema_source))?;
    let mut errors = Vec::<String>::new();
    let mut warnings = Vec::<String>::new();

    let module_files = if root.is_dir() {
        let mut files = Vec::new();
        collect_module_files(&root, &mut files)?;
        sort_paths_lexical(&root, &mut files);
        files
    } else {
        vec![root.clone()]
    };

    if root.is_dir() && module_files.is_empty() {
        errors.push(format!(
            "Schema directory '{}' has no .qail modules",
            root.display()
        ));
    }

    if root.is_dir() {
        let order_report = inspect_order_file(&root, &module_files)?;
        if order_report.strict_manifest {
            if order_report.unlisted_modules.is_empty() {
                println!("{} strict manifest: enabled", "✓".green());
            } else {
                errors.push(format!(
                    "Strict manifest enabled but {} unlisted module(s): {}",
                    order_report.unlisted_modules.len(),
                    order_report.unlisted_modules.join(", ")
                ));
            }
        } else if !order_report.unlisted_modules.is_empty() {
            warnings.push(format!(
                "{} module(s) are not listed in {}: {}",
                order_report.unlisted_modules.len(),
                ORDER_FILE,
                order_report.unlisted_modules.join(", ")
            ));
        }

        for d in order_report.duplicate_entries {
            warnings.push(format!("Duplicate {} entry: {}", ORDER_FILE, d));
        }
        for r in order_report.unresolved_entries {
            errors.push(format!("Unresolved {} entry: {}", ORDER_FILE, r));
        }
    }

    // Detect object collisions across modules.
    let mut table_defs: HashMap<String, Vec<String>> = HashMap::new();
    let mut index_defs: HashMap<String, Vec<String>> = HashMap::new();
    let mut function_defs: HashMap<String, Vec<String>> = HashMap::new();

    for module in &module_files {
        let content = fs::read_to_string(module)
            .with_context(|| format!("Failed to read module '{}'", module.display()))?;
        let parsed = parse_qail(&content)
            .map_err(|e| anyhow!("Failed to parse module '{}': {}", module.display(), e))?;
        let rel = rel_path(&root, module);

        for table in parsed.tables.keys() {
            table_defs
                .entry(table.clone())
                .or_default()
                .push(rel.clone());
        }
        for index in &parsed.indexes {
            index_defs
                .entry(index.name.clone())
                .or_default()
                .push(rel.clone());
        }
        for func in &parsed.functions {
            let sig = format!("{}({})", func.name, func.args.join(", "));
            function_defs.entry(sig).or_default().push(rel.clone());
        }
    }

    for (name, files) in table_defs.iter().filter(|(_, v)| v.len() > 1) {
        errors.push(format!(
            "Table '{}' is defined in multiple modules: {}",
            name,
            files.join(", ")
        ));
    }
    for (name, files) in index_defs.iter().filter(|(_, v)| v.len() > 1) {
        errors.push(format!(
            "Index '{}' is defined in multiple modules: {}",
            name,
            files.join(", ")
        ));
    }
    for (sig, files) in function_defs.iter().filter(|(_, v)| v.len() > 1) {
        warnings.push(format!(
            "Function signature '{}' appears in multiple modules: {}",
            sig,
            files.join(", ")
        ));
    }

    // Parse merged schema and validate FK references after structural checks pass.
    if errors.is_empty() {
        match parse_qail_file(schema_source) {
            Ok(schema) => {
                if let Err(fk_errors) = schema.validate() {
                    for e in fk_errors {
                        errors.push(e);
                    }
                }
            }
            Err(e) => errors.push(format!("Merged schema parse failed: {}", e)),
        }
    }

    println!("{}", "Schema Doctor".cyan().bold());
    println!("  Source: {}", schema_source.yellow());
    if root.is_dir() {
        println!("  Modules: {}", module_files.len());
    }

    if errors.is_empty() && warnings.is_empty() {
        println!("  {} No issues found", "✓".green());
        return Ok(());
    }

    if !errors.is_empty() {
        println!("  {} {} error(s):", "✗".red(), errors.len());
        for e in &errors {
            println!("    {}", e.red());
        }
    }
    if !warnings.is_empty() {
        println!("  {} {} warning(s):", "⚠".yellow(), warnings.len());
        for w in &warnings {
            println!("    {}", w.yellow());
        }
    }

    if !errors.is_empty() {
        bail!("Schema doctor found {} error(s)", errors.len());
    }
    if fail_on_warning && !warnings.is_empty() {
        bail!(
            "Schema doctor found {} warning(s) with --strict",
            warnings.len()
        );
    }
    Ok(())
}

/// Split a monolithic schema into modular `schema/` files + `_order.qail`.
pub fn split_schema(input: &str, out_dir: &str, force: bool) -> Result<()> {
    let schema = parse_qail_file(input).map_err(|e| anyhow!("Failed to parse schema: {}", e))?;
    let out = Path::new(out_dir);

    if out.exists() {
        if !out.is_dir() {
            bail!(
                "Output path '{}' exists and is not a directory",
                out.display()
            );
        }
        if !force && fs::read_dir(out)?.next().is_some() {
            bail!(
                "Output directory '{}' is not empty (use --force to overwrite files)",
                out.display()
            );
        }
    } else {
        fs::create_dir_all(out).with_context(|| format!("Failed to create '{}'", out.display()))?;
    }

    let table_names = sorted_table_names(&schema);
    let table_name_set: HashSet<&str> = schema.tables.keys().map(String::as_str).collect();

    let mut order_entries = Vec::new();
    let mut written = 0usize;

    let globals = build_globals_schema(&schema, &table_name_set);
    if schema_has_content(&globals) {
        let globals_path = out.join("globals.qail");
        write_text(&globals_path, &canonical_schema_text(&globals))?;
        order_entries.push("globals.qail".to_string());
        written += 1;
    }

    for table_name in table_names {
        let mut module = Schema::new();
        let table = schema
            .tables
            .get(&table_name)
            .cloned()
            .ok_or_else(|| anyhow!("Missing table '{}' during split", table_name))?;
        module.add_table(table);
        module.indexes = schema
            .indexes
            .iter()
            .filter(|idx| idx.table == table_name)
            .cloned()
            .collect();
        module.policies = schema
            .policies
            .iter()
            .filter(|p| p.table == table_name)
            .cloned()
            .collect();
        module.comments = schema
            .comments
            .iter()
            .filter(|c| comment_targets_table(c, &table_name))
            .cloned()
            .collect();
        module.triggers = schema
            .triggers
            .iter()
            .filter(|t| t.table == table_name)
            .cloned()
            .collect();

        let filename = format!("{table_name}.qail");
        let path = out.join(&filename);
        write_text(&path, &canonical_schema_text(&module))?;
        order_entries.push(filename);
        written += 1;
    }

    let mut order_content = String::new();
    order_content.push_str("# QAIL module order\n");
    order_content.push_str("# Modules listed first are loaded first.\n");
    for entry in &order_entries {
        order_content.push_str(entry);
        order_content.push('\n');
    }
    write_text(&out.join(ORDER_FILE), &order_content)?;

    println!(
        "{} split '{}' -> '{}' ({} module files)",
        "✓".green(),
        input,
        out_dir,
        written
    );
    Ok(())
}

/// Merge modular schema source into one canonical `.qail` file.
pub fn merge_schema(input: &str, output: &str) -> Result<()> {
    let schema = parse_qail_file(input).map_err(|e| anyhow!("Failed to parse schema: {}", e))?;
    write_text(Path::new(output), &canonical_schema_text(&schema))?;
    println!("{} merged '{}' -> '{}'", "✓".green(), input, output);
    Ok(())
}

/// Format schema file or schema directory modules in-place.
pub fn format_schema_source(path: &str) -> Result<()> {
    let resolved = qail_core::schema_source::resolve_schema_source(path)
        .map_err(|e| anyhow!("Failed to resolve schema source '{}': {}", path, e))?;

    let files = if resolved.is_directory() {
        resolved.files.clone()
    } else {
        vec![resolved.root.clone()]
    };

    let mut changed = 0usize;
    for file in &files {
        let before = fs::read_to_string(file)
            .with_context(|| format!("Failed to read '{}'", file.display()))?;
        let parsed = parse_qail(&before)
            .map_err(|e| anyhow!("Failed to parse '{}': {}", file.display(), e))?;
        let after = canonical_schema_text(&parsed);
        if before != after {
            write_text(file, &after)?;
            changed += 1;
        }
    }

    println!(
        "{} formatted {} schema module(s), {} changed",
        "✓".green(),
        files.len(),
        changed
    );
    Ok(())
}

fn resolve_schema_root(input: &Path) -> Result<PathBuf> {
    if input.exists() {
        return Ok(input.to_path_buf());
    }
    if input.file_name().is_some_and(|n| n == "schema.qail") {
        let parent = input.parent().unwrap_or_else(|| Path::new("."));
        let dir = parent.join("schema");
        if dir.is_dir() {
            return Ok(dir);
        }
    }
    bail!(
        "Schema source '{}' not found (expected file or schema directory)",
        input.display()
    )
}

fn inspect_order_file(root: &Path, modules: &[PathBuf]) -> Result<OrderReport> {
    let order_path = root.join(ORDER_FILE);
    let mut report = OrderReport {
        strict_manifest: strict_manifest_default_enabled(root),
        ..OrderReport::default()
    };
    if !order_path.exists() {
        return Ok(report);
    }

    let mut module_set = HashSet::new();
    for m in modules {
        module_set.insert(canonical(m)?);
    }
    let mut referenced = HashSet::new();

    let content = fs::read_to_string(&order_path)
        .with_context(|| format!("Failed to read '{}'", order_path.display()))?;
    for (line_no, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(comment) = line.strip_prefix("--") {
            if comment.trim().eq_ignore_ascii_case(STRICT_DIRECTIVE) {
                report.strict_manifest = true;
            }
            continue;
        }
        if line.eq_ignore_ascii_case(STRICT_SHORTHAND) {
            report.strict_manifest = true;
            continue;
        }

        let entry = root.join(line);
        if !entry.exists() {
            report.unresolved_entries.push(format!(
                "line {} '{}': path does not exist",
                line_no + 1,
                line
            ));
            continue;
        }

        if entry.is_dir() {
            let mut nested = Vec::new();
            collect_module_files(&entry, &mut nested)?;
            sort_paths_lexical(root, &mut nested);
            if nested.is_empty() {
                report.unresolved_entries.push(format!(
                    "line {} '{}': directory has no .qail files",
                    line_no + 1,
                    line
                ));
                continue;
            }
            for module in nested {
                let c = canonical(&module)?;
                if !module_set.contains(&c) {
                    report.unresolved_entries.push(format!(
                        "line {} '{}': not a loadable module",
                        line_no + 1,
                        line
                    ));
                    continue;
                }
                if !referenced.insert(c.clone()) {
                    report.duplicate_entries.push(rel_path(root, &module));
                }
            }
            continue;
        }

        if entry.file_name().is_some_and(|n| n == ORDER_FILE) {
            report.unresolved_entries.push(format!(
                "line {} '{}': cannot include {} recursively",
                line_no + 1,
                line,
                ORDER_FILE
            ));
            continue;
        }
        if entry.extension().is_none_or(|e| e != "qail") {
            report.unresolved_entries.push(format!(
                "line {} '{}': expected .qail file or directory",
                line_no + 1,
                line
            ));
            continue;
        }

        let c = canonical(&entry)?;
        if !module_set.contains(&c) {
            report.unresolved_entries.push(format!(
                "line {} '{}': not a loadable module",
                line_no + 1,
                line
            ));
            continue;
        }
        if !referenced.insert(c.clone()) {
            report.duplicate_entries.push(rel_path(root, &entry));
        }
    }

    let mut unlisted = BTreeSet::new();
    for module in modules {
        let c = canonical(module)?;
        if !referenced.contains(&c) {
            unlisted.insert(rel_path(root, module));
        }
    }
    report.unlisted_modules = unlisted.into_iter().collect();
    Ok(report)
}

fn collect_module_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let hidden = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.'));
        if hidden {
            continue;
        }
        if path.is_dir() {
            collect_module_files(&path, out)?;
            continue;
        }
        if path.extension().is_some_and(|e| e == "qail")
            && path.file_name().is_none_or(|n| n != ORDER_FILE)
        {
            out.push(path);
        }
    }
    Ok(())
}

fn sorted_table_names(schema: &Schema) -> Vec<String> {
    let mut names: Vec<String> = schema.tables.keys().cloned().collect();
    names.sort();
    names
}

fn build_globals_schema(schema: &Schema, table_name_set: &HashSet<&str>) -> Schema {
    let mut globals = Schema::new();
    globals.extensions = schema.extensions.clone();
    globals.enums = schema.enums.clone();
    globals.sequences = schema.sequences.clone();
    globals.views = schema.views.clone();
    globals.functions = schema.functions.clone();
    globals.grants = schema.grants.clone();
    globals.resources = schema.resources.clone();
    globals.migrations = schema.migrations.clone();
    globals.comments = schema
        .comments
        .iter()
        .filter(|c| match &c.target {
            CommentTarget::Table(t) => !table_name_set.contains(t.as_str()),
            CommentTarget::Column { table, column: _ } => !table_name_set.contains(table.as_str()),
        })
        .cloned()
        .collect();
    globals.indexes = schema
        .indexes
        .iter()
        .filter(|idx| !table_name_set.contains(idx.table.as_str()))
        .cloned()
        .collect();
    globals.policies = schema
        .policies
        .iter()
        .filter(|p| !table_name_set.contains(p.table.as_str()))
        .cloned()
        .collect();
    globals.triggers = schema
        .triggers
        .iter()
        .filter(|t| !table_name_set.contains(t.table.as_str()))
        .cloned()
        .collect();
    globals
}

fn schema_has_content(schema: &Schema) -> bool {
    !schema.tables.is_empty()
        || !schema.indexes.is_empty()
        || !schema.migrations.is_empty()
        || !schema.extensions.is_empty()
        || !schema.comments.is_empty()
        || !schema.sequences.is_empty()
        || !schema.enums.is_empty()
        || !schema.views.is_empty()
        || !schema.functions.is_empty()
        || !schema.triggers.is_empty()
        || !schema.grants.is_empty()
        || !schema.policies.is_empty()
        || !schema.resources.is_empty()
}

fn comment_targets_table(comment: &qail_core::migrate::Comment, table: &str) -> bool {
    match &comment.target {
        CommentTarget::Table(t) => t == table,
        CommentTarget::Column {
            table: t,
            column: _,
        } => t == table,
    }
}

fn canonical_schema_text(schema: &Schema) -> String {
    let mut out = to_qail_string(schema);
    if let Some(stripped) = out.strip_prefix(QAIL_HEADER) {
        out = stripped.to_string();
    }
    let trimmed = out.trim();
    if trimmed.is_empty() {
        String::new()
    } else {
        let mut normalized = trimmed.to_string();
        normalized.push('\n');
        normalized
    }
}

fn write_text(path: &Path, content: &str) -> Result<()> {
    fs::write(path, content).with_context(|| format!("Failed to write '{}'", path.display()))
}

fn rel_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .ok()
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

fn sort_paths_lexical(root: &Path, files: &mut [PathBuf]) {
    files.sort_by_key(|a| rel_path(root, a));
}

fn canonical(path: &Path) -> Result<PathBuf> {
    path
        .canonicalize()
        .with_context(|| format!("Failed to canonicalize '{}'", path.display()))
}

fn strict_manifest_default_enabled(schema_root: &Path) -> bool {
    if let Ok(raw) = std::env::var(STRICT_ENV_VAR) {
        let normalized = raw.trim().to_ascii_lowercase();
        return matches!(normalized.as_str(), "1" | "true" | "yes" | "on");
    }

    for dir in schema_root.ancestors() {
        let candidate = dir.join("qail.toml");
        if !candidate.is_file() {
            continue;
        }
        if let Ok(cfg) = qail_core::config::QailConfig::load_from(&candidate) {
            return cfg.project.schema_strict_manifest.unwrap_or(false);
        }
    }

    false
}
