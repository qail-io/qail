//! Source code scanner for QAIL and SQL queries.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

#[cfg(test)]
use crate::ast::CageKind;
use crate::ast::{Action, Cage, Condition, ConflictAction, Expr, MergeAction, Value};
use crate::parse;

use super::rust_ast::RustAnalyzer;
use super::rust_ast::detect_raw_sql_in_file;
use super::rust_ast::sql_semantics::{SqlStmtKind, classify_sql_kind};
#[cfg(test)]
use super::text_qail::extract_qail_candidate_from_line;
use super::text_qail::{
    TextLiteral, extract_text_literals, literal_offset_to_line_col, looks_like_qail_query,
    trim_query_bounds,
};

/// Analysis mode for the codebase scanner
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnalysisMode {
    /// Semantic Rust source analysis (shared with build scanner)
    RustAST,
    /// Text-source semantic scan for non-Rust files.
    TextSemantic,
}

/// Type of query found in source code.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    /// Native QAIL query in modern text form (e.g. `get users fields ...`).
    Qail,
    RawSql,
}

/// A reference to a query in source code.
#[derive(Debug, Clone)]
pub struct CodeReference {
    pub file: PathBuf,
    pub line: usize,
    pub table: String,
    pub columns: Vec<String>,
    pub query_type: QueryType,
    pub snippet: String,
}

/// Analysis result for a single file
#[derive(Debug, Clone)]
pub struct FileAnalysis {
    pub file: PathBuf,
    pub mode: AnalysisMode,
    pub ref_count: usize,
    pub safe: bool,
}

/// Complete scan result with per-file breakdown
#[derive(Debug, Default)]
pub struct ScanResult {
    pub refs: Vec<CodeReference>,
    pub files: Vec<FileAnalysis>,
}

/// Scanner for finding QAIL and SQL references in source code.
pub struct CodebaseScanner;

impl Default for CodebaseScanner {
    fn default() -> Self {
        Self::new()
    }
}

impl CodebaseScanner {
    /// Create a new scanner.
    pub fn new() -> Self {
        Self
    }

    /// Scan a directory for all QAIL and SQL references.
    pub fn scan(&self, path: &Path) -> Vec<CodeReference> {
        self.scan_with_details(path).refs
    }

    /// Scan a directory with detailed per-file breakdown.
    pub fn scan_with_details(&self, path: &Path) -> ScanResult {
        let mut result = ScanResult::default();

        if path.is_file() {
            if let Some(ext) = path.extension()
                && is_supported_source_extension(ext)
            {
                let mode = mode_for_extension(ext);
                let file_refs = self.scan_file(path);
                let ref_count = file_refs.len();

                result.files.push(FileAnalysis {
                    file: path.to_path_buf(),
                    mode,
                    ref_count,
                    safe: true, // Will be updated after impact analysis
                });
                result.refs.extend(file_refs);
            }
        } else if path.is_dir() {
            self.scan_dir_with_details(path, &mut result);
        }

        result
    }

    /// Recursively scan a directory with per-file tracking.
    fn scan_dir_with_details(&self, dir: &Path, result: &mut ScanResult) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        for entry in entries.flatten() {
            let path = entry.path();

            // Skip common non-source directories
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name == "target"
                    || name == "node_modules"
                    || name == ".git"
                    || name == "vendor"
                    || name == "__pycache__"
                    || name == "dist"
                {
                    continue;
                }
                self.scan_dir_with_details(&path, result);
            } else if let Some(ext) = path.extension()
                && is_supported_source_extension(ext)
            {
                let mode = mode_for_extension(ext);
                let file_refs = self.scan_file(&path);
                let ref_count = file_refs.len();

                result.files.push(FileAnalysis {
                    file: path.clone(),
                    mode,
                    ref_count,
                    safe: true,
                });
                result.refs.extend(file_refs);
            }
        }
    }

    /// Scan a single file for references.
    /// Uses semantic Rust analysis for `.rs` files and parser-based textual
    /// extraction for non-Rust sources.
    fn scan_file(&self, path: &Path) -> Vec<CodeReference> {
        if path.extension().map(|e| e == "rs").unwrap_or(false) {
            let mut refs = RustAnalyzer::scan_file(path);
            refs.extend(self.scan_rust_raw_sql(path));
            return refs;
        }

        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        self.scan_text_file(path, &content)
    }

    fn scan_text_file(&self, path: &Path, content: &str) -> Vec<CodeReference> {
        let mut refs = Vec::new();

        for literal in extract_text_literals(content) {
            refs.extend(self.scan_text_literal(path, &literal));
        }

        refs
    }

    fn scan_text_literal(&self, path: &Path, literal: &TextLiteral) -> Vec<CodeReference> {
        let mut refs = Vec::new();
        let Some((start, end)) = trim_query_bounds(&literal.text) else {
            return refs;
        };
        let Some(candidate) = literal.text.get(start..end) else {
            return refs;
        };

        // Keep scans bounded for very large embedded literals.
        if candidate.len() > 16384 {
            return refs;
        }
        let (line_number, _) = literal_offset_to_line_col(literal, start);

        if looks_like_qail_query(candidate)
            && let Ok(cmd) = parse(candidate)
            && let Some(qail_ref) = command_to_reference(path, line_number, &cmd)
        {
            refs.push(qail_ref);
        }

        let normalized = normalize_whitespace(candidate);
        for (_kind, table, columns) in parse_sql_references(&normalized) {
            refs.push(CodeReference {
                file: path.to_path_buf(),
                line: line_number,
                table,
                columns,
                query_type: QueryType::RawSql,
                snippet: normalized.chars().take(60).collect(),
            });
        }

        refs
    }

    fn scan_rust_raw_sql(&self, path: &Path) -> Vec<CodeReference> {
        let mut refs = Vec::new();

        for sql_match in detect_raw_sql_in_file(path) {
            let normalized = normalize_whitespace(&sql_match.raw_sql);
            if normalized.is_empty() {
                continue;
            }

            for (_kind, table, columns) in parse_sql_references(&normalized) {
                refs.push(CodeReference {
                    file: path.to_path_buf(),
                    line: sql_match.line,
                    table,
                    columns,
                    query_type: QueryType::RawSql,
                    snippet: normalized.chars().take(60).collect(),
                });
            }
        }

        refs
    }
}

fn mode_for_extension(ext: &std::ffi::OsStr) -> AnalysisMode {
    if ext == "rs" {
        AnalysisMode::RustAST
    } else {
        AnalysisMode::TextSemantic
    }
}

fn is_supported_source_extension(ext: &std::ffi::OsStr) -> bool {
    matches!(
        ext.to_str(),
        Some("rs" | "ts" | "tsx" | "js" | "jsx" | "py")
    )
}

fn command_to_reference(path: &Path, line: usize, cmd: &crate::Qail) -> Option<CodeReference> {
    if cmd.table.trim().is_empty() {
        return None;
    }

    let snippet = match cmd.action {
        Action::Get => format!("get {} fields ...", cmd.table),
        Action::Set => format!("set {} values ...", cmd.table),
        Action::Del => format!("del {}", cmd.table),
        Action::Add => format!("add {} fields ...", cmd.table),
        _ => return None,
    };
    let columns = collect_reference_columns(cmd);

    Some(CodeReference {
        file: path.to_path_buf(),
        line,
        table: cmd.table.clone(),
        columns,
        query_type: QueryType::Qail,
        snippet,
    })
}

fn collect_reference_columns(cmd: &crate::Qail) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();

    collect_exprs_columns(&cmd.columns, &mut cols, &mut seen);
    for cage in &cmd.cages {
        collect_cage_columns(cage, &mut cols, &mut seen);
    }
    for join in &cmd.joins {
        if let Some(conditions) = &join.on {
            collect_conditions_columns(conditions, &mut cols, &mut seen);
        }
    }
    collect_conditions_columns(&cmd.having, &mut cols, &mut seen);
    collect_exprs_columns(&cmd.distinct_on, &mut cols, &mut seen);
    if let Some(returning) = &cmd.returning {
        collect_exprs_columns(returning, &mut cols, &mut seen);
    }
    if let Some(on_conflict) = &cmd.on_conflict {
        for column in &on_conflict.columns {
            push_column_ref(column, &mut cols, &mut seen);
        }
        if let ConflictAction::DoUpdate { assignments } = &on_conflict.action {
            for (column, expr) in assignments {
                push_column_ref(column, &mut cols, &mut seen);
                collect_expr_columns(expr, &mut cols, &mut seen);
            }
        }
    }
    if let Some(merge) = &cmd.merge {
        collect_conditions_columns(&merge.on, &mut cols, &mut seen);
        for clause in &merge.clauses {
            collect_conditions_columns(&clause.condition, &mut cols, &mut seen);
            match &clause.action {
                MergeAction::Update { assignments } => {
                    for (column, expr) in assignments {
                        push_column_ref(column, &mut cols, &mut seen);
                        collect_expr_columns(expr, &mut cols, &mut seen);
                    }
                }
                MergeAction::Insert { columns, values } => {
                    for column in columns {
                        push_column_ref(column, &mut cols, &mut seen);
                    }
                    collect_exprs_columns(values, &mut cols, &mut seen);
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }

    cols
}

fn collect_exprs_columns(exprs: &[Expr], cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    for expr in exprs {
        collect_expr_columns(expr, cols, seen);
    }
}

fn collect_cage_columns(cage: &Cage, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    collect_conditions_columns(&cage.conditions, cols, seen);
}

fn collect_conditions_columns(
    conditions: &[Condition],
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    for condition in conditions {
        collect_condition_columns(condition, cols, seen);
    }
}

fn collect_condition_columns(
    condition: &Condition,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    collect_expr_columns(&condition.left, cols, seen);
    collect_value_columns(&condition.value, cols, seen);
}

fn collect_expr_columns(expr: &Expr, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    match expr {
        Expr::Star => push_column_ref("*", cols, seen),
        Expr::Named(name) | Expr::Aliased { name, .. } => push_column_ref(name, cols, seen),
        Expr::Aggregate { col, filter, .. } => {
            push_column_ref(col, cols, seen);
            if let Some(conditions) = filter {
                collect_conditions_columns(conditions, cols, seen);
            }
        }
        Expr::JsonAccess { column, .. } => push_column_ref(column, cols, seen),
        Expr::Cast { expr, .. }
        | Expr::Mod { col: expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::FieldAccess { expr, .. } => collect_expr_columns(expr, cols, seen),
        Expr::Subscript { expr, index, .. } => {
            collect_expr_columns(expr, cols, seen);
            collect_expr_columns(index, cols, seen);
        }
        Expr::FunctionCall { args, .. } | Expr::ArrayConstructor { elements: args, .. } => {
            collect_exprs_columns(args, cols, seen);
        }
        Expr::SpecialFunction { args, .. } => {
            for (_, arg) in args {
                collect_expr_columns(arg, cols, seen);
            }
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_columns(left, cols, seen);
            collect_expr_columns(right, cols, seen);
        }
        Expr::Literal(value) => collect_value_columns(value, cols, seen),
        Expr::RowConstructor { elements, .. } => collect_exprs_columns(elements, cols, seen),
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            for (condition, value) in when_clauses {
                collect_condition_columns(condition, cols, seen);
                collect_expr_columns(value, cols, seen);
            }
            if let Some(value) = else_value {
                collect_expr_columns(value, cols, seen);
            }
        }
        Expr::Window {
            params,
            partition,
            order,
            ..
        } => {
            collect_exprs_columns(params, cols, seen);
            for column in partition {
                push_column_ref(column, cols, seen);
            }
            for cage in order {
                collect_cage_columns(cage, cols, seen);
            }
        }
        Expr::Def { .. } | Expr::Subquery { .. } | Expr::Exists { .. } => {}
    }
}

fn collect_value_columns(value: &Value, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    match value {
        Value::Column(column) => push_column_ref(column, cols, seen),
        Value::Expr(expr) => collect_expr_columns(expr, cols, seen),
        Value::Array(values) => {
            for value in values {
                collect_value_columns(value, cols, seen);
            }
        }
        _ => {}
    }
}

fn push_column_ref(name: &str, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    let name = name.trim();
    if !name.is_empty() && seen.insert(name.to_string()) {
        cols.push(name.to_string());
    }
}

#[cfg(test)]
fn extract_payload_columns(cmd: &crate::Qail) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();

    for cage in &cmd.cages {
        if !matches!(cage.kind, CageKind::Payload) {
            continue;
        }

        for cond in &cage.conditions {
            if let Expr::Named(name) = &cond.left
                && !name.is_empty()
                && seen.insert(name.clone())
            {
                cols.push(name.clone());
            }
        }
    }

    cols
}

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn normalize_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn parse_sql_references(sql: &str) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    parse_sql_references_with_cte_aliases(sql, &[])
}

fn parse_sql_references_with_cte_aliases(
    sql: &str,
    inherited_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let normalized = normalize_whitespace(sql);
    let cte_parts = parse_sql_cte_parts(&normalized, inherited_cte_aliases);
    let mut refs = cte_parts
        .as_ref()
        .map(|parts| parts.references.clone())
        .unwrap_or_default();

    if classify_sql_kind(&normalized) == Some(SqlStmtKind::Select) {
        refs.extend(parse_sql_select_references(
            &normalized,
            inherited_cte_aliases,
            cte_parts
                .as_ref()
                .map(|parts| parts.aliases.as_slice())
                .unwrap_or(&[]),
        ));
        return refs;
    }

    if let Some((kind, table, columns)) = parse_sql_reference(&normalized) {
        let is_cte_alias = cte_parts.as_ref().is_some_and(|parts| {
            parts
                .aliases
                .iter()
                .any(|alias| sql_ident_eq(alias, &table))
        });
        let is_inherited_cte_alias = inherited_cte_aliases
            .iter()
            .any(|alias| sql_ident_eq(alias, &table));
        if !is_cte_alias && !is_inherited_cte_alias {
            refs.push((kind, table, columns));
        }
    }

    refs
}

#[derive(Debug, Clone)]
struct SqlTableSource {
    table: String,
    alias: String,
}

fn parse_sql_select_references(
    sql: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<(SqlStmtKind, String, Vec<String>)> {
    let Some(select_idx) = find_keyword_top_level_from(sql, "SELECT", 0) else {
        return Vec::new();
    };
    let Some(from_idx) = find_keyword_top_level_from(sql, "FROM", select_idx + "SELECT".len())
    else {
        return Vec::new();
    };
    let Some(columns_raw) = sql
        .get(select_idx + "SELECT".len()..from_idx)
        .map(str::trim)
    else {
        return Vec::new();
    };

    let sources = parse_sql_select_table_sources(
        sql,
        from_idx + "FROM".len(),
        inherited_cte_aliases,
        local_cte_aliases,
    );
    if sources.is_empty() {
        return Vec::new();
    }

    let columns_by_source =
        collect_sql_select_columns_by_source(sql, columns_raw, from_idx, &sources);

    sources
        .into_iter()
        .zip(columns_by_source)
        .map(|(source, columns)| (SqlStmtKind::Select, source.table, columns))
        .collect()
}

fn parse_sql_select_table_sources(
    sql: &str,
    start: usize,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> Vec<SqlTableSource> {
    let mut sources = Vec::new();
    let mut cursor = start;
    let from_end = top_level_sql_clause_start(
        sql,
        start,
        &[
            "WHERE",
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "LIMIT",
            "OFFSET",
            "FETCH",
            "FOR",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "WINDOW",
        ],
    )
    .unwrap_or(sql.len());

    loop {
        cursor = skip_sql_ws(sql.as_bytes(), cursor);
        if cursor >= from_end {
            break;
        }

        if starts_with_keyword_at(sql, cursor, "LATERAL") {
            cursor = skip_sql_ws(sql.as_bytes(), cursor + "LATERAL".len());
        }
        if starts_with_keyword_at(sql, cursor, "ONLY") {
            cursor = skip_sql_ws(sql.as_bytes(), cursor + "ONLY".len());
        }

        let source_end = if sql.as_bytes().get(cursor).copied() == Some(b'(') {
            balanced_paren_segment(sql, cursor)
                .map(|(_, end)| end)
                .unwrap_or(cursor)
        } else if let Some((table, table_end)) = parse_sql_object_name_with_end(sql, cursor) {
            let (alias, alias_end) = parse_sql_optional_table_alias(sql, table_end);
            if !is_sql_cte_alias(&table, inherited_cte_aliases, local_cte_aliases) {
                sources.push(SqlTableSource {
                    alias: alias.unwrap_or_else(|| table.clone()),
                    table,
                });
            }
            alias_end
        } else {
            break;
        };

        let Some(next_start) = next_sql_table_source_start(sql, source_end, from_end) else {
            break;
        };
        cursor = next_start;
    }

    sources
}

fn parse_sql_optional_table_alias(sql: &str, start: usize) -> (Option<String>, usize) {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, start);
    if starts_with_keyword_at(sql, cursor, "AS") {
        cursor = skip_sql_ws(bytes, cursor + "AS".len());
    }

    let Some((alias, end)) = parse_sql_identifier_segment(sql, cursor) else {
        return (None, start);
    };
    if is_sql_table_source_boundary(&alias) {
        return (None, start);
    }

    (Some(alias), end)
}

fn next_sql_table_source_start(sql: &str, start: usize, end: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut i = start;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;
    let mut escaped = false;

    while i < end {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' {
                escaped = true;
                i += 1;
                continue;
            }
            if b == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => return Some(i + 1),
            _ => {
                if depth == 0 && starts_with_keyword_at(sql, i, "JOIN") {
                    return Some(i + "JOIN".len());
                }
            }
        }
        i += 1;
    }

    None
}

fn is_sql_cte_alias(
    table: &str,
    inherited_cte_aliases: &[String],
    local_cte_aliases: &[String],
) -> bool {
    inherited_cte_aliases
        .iter()
        .chain(local_cte_aliases.iter())
        .any(|alias| sql_ident_eq(alias, table))
}

fn is_sql_table_source_boundary(ident: &str) -> bool {
    matches!(
        ident.to_ascii_uppercase().as_str(),
        "CROSS"
            | "FULL"
            | "GROUP"
            | "HAVING"
            | "INNER"
            | "INTERSECT"
            | "JOIN"
            | "LEFT"
            | "LIMIT"
            | "NATURAL"
            | "OFFSET"
            | "ON"
            | "ORDER"
            | "OUTER"
            | "RIGHT"
            | "UNION"
            | "USING"
            | "WHERE"
    )
}

#[derive(Debug, Default)]
struct SqlCteParts {
    aliases: Vec<String>,
    references: Vec<(SqlStmtKind, String, Vec<String>)>,
}

fn parse_sql_cte_parts(sql: &str, inherited_cte_aliases: &[String]) -> Option<SqlCteParts> {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, 0);
    if !starts_with_keyword_at(sql, cursor, "WITH") {
        return None;
    }
    cursor += "WITH".len();
    cursor = skip_sql_ws(bytes, cursor);
    if starts_with_keyword_at(sql, cursor, "RECURSIVE") {
        cursor += "RECURSIVE".len();
    }

    let mut parts = SqlCteParts::default();
    let mut known_aliases = inherited_cte_aliases.to_vec();

    loop {
        cursor = skip_sql_ws(bytes, cursor);
        let (alias, alias_end) = parse_sql_identifier_segment(sql, cursor)?;
        parts.aliases.push(alias.clone());
        known_aliases.push(alias);
        cursor = skip_sql_ws(bytes, alias_end);

        if bytes.get(cursor).copied() == Some(b'(') {
            let (_, end) = balanced_paren_segment(sql, cursor)?;
            cursor = skip_sql_ws(bytes, end);
        }

        if !starts_with_keyword_at(sql, cursor, "AS") {
            return None;
        }
        cursor += "AS".len();
        cursor = skip_sql_ws(bytes, cursor);
        cursor = skip_sql_cte_materialization_modifier(sql, cursor);

        if bytes.get(cursor).copied() != Some(b'(') {
            return None;
        }
        let (body, end) = balanced_paren_segment(sql, cursor)?;
        parts
            .references
            .extend(parse_sql_references_with_cte_aliases(body, &known_aliases));
        cursor = skip_sql_ws(bytes, end);

        if bytes.get(cursor).copied() == Some(b',') {
            cursor += 1;
            continue;
        }
        break;
    }

    if parts.aliases.is_empty() {
        None
    } else {
        Some(parts)
    }
}

fn skip_sql_cte_materialization_modifier(sql: &str, start: usize) -> usize {
    let bytes = sql.as_bytes();
    let cursor = skip_sql_ws(bytes, start);
    if starts_with_keyword_at(sql, cursor, "MATERIALIZED") {
        return skip_sql_ws(bytes, cursor + "MATERIALIZED".len());
    }
    if starts_with_keyword_at(sql, cursor, "NOT") {
        let after_not = skip_sql_ws(bytes, cursor + "NOT".len());
        if starts_with_keyword_at(sql, after_not, "MATERIALIZED") {
            return skip_sql_ws(bytes, after_not + "MATERIALIZED".len());
        }
    }
    cursor
}

fn sql_ident_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn parse_sql_reference(sql: &str) -> Option<(SqlStmtKind, String, Vec<String>)> {
    let normalized = normalize_whitespace(sql);
    let kind = classify_sql_kind(&normalized)?;

    match kind {
        SqlStmtKind::Select => {
            let select_idx = find_keyword_top_level_from(&normalized, "SELECT", 0)?;
            let from_idx =
                find_keyword_top_level_from(&normalized, "FROM", select_idx + "SELECT".len())?;

            let columns_raw = normalized
                .get(select_idx + "SELECT".len()..from_idx)?
                .trim();
            let table = parse_sql_object_name(&normalized, from_idx + "FROM".len())?;

            let columns = collect_sql_select_columns(&normalized, columns_raw, from_idx);

            Some((kind, table, columns))
        }
        SqlStmtKind::Insert => {
            let insert_idx = find_keyword_top_level_from(&normalized, "INSERT", 0)?;
            let into_idx =
                find_keyword_top_level_from(&normalized, "INTO", insert_idx + "INSERT".len())?;
            let (table, table_end) =
                parse_sql_object_name_with_end(&normalized, into_idx + "INTO".len())?;
            let columns = collect_sql_insert_columns(&normalized, table_end);
            Some((kind, table, columns))
        }
        SqlStmtKind::Update => {
            let update_idx = find_keyword_top_level_from(&normalized, "UPDATE", 0)?;
            let (table, table_end) =
                parse_sql_object_name_with_end(&normalized, update_idx + "UPDATE".len())?;
            let columns = collect_sql_update_columns(&normalized, table_end);
            Some((kind, table, columns))
        }
        SqlStmtKind::Delete => {
            let delete_idx = find_keyword_top_level_from(&normalized, "DELETE", 0)?;
            let from_idx =
                find_keyword_top_level_from(&normalized, "FROM", delete_idx + "DELETE".len())?;
            let (table, table_end) =
                parse_sql_object_name_with_end(&normalized, from_idx + "FROM".len())?;
            let columns = collect_sql_delete_columns(&normalized, table_end);
            Some((kind, table, columns))
        }
        SqlStmtKind::Merge => {
            let merge_idx = find_keyword_top_level_from(&normalized, "MERGE", 0)?;
            let into_idx =
                find_keyword_top_level_from(&normalized, "INTO", merge_idx + "MERGE".len())?;
            let table = parse_sql_object_name(&normalized, into_idx + "INTO".len())?;
            Some((kind, table, vec![]))
        }
    }
}

fn parse_sql_object_name(sql: &str, start: usize) -> Option<String> {
    parse_sql_object_name_with_end(sql, start).map(|(name, _)| name)
}

fn parse_sql_object_name_with_end(sql: &str, start: usize) -> Option<(String, usize)> {
    let bytes = sql.as_bytes();
    let mut cursor = skip_sql_ws(bytes, start);
    if cursor >= bytes.len() {
        return None;
    }

    let mut segments = Vec::new();
    loop {
        if cursor >= bytes.len() {
            break;
        }

        let (segment, next) = if matches!(bytes[cursor], b'"' | b'`') {
            let quote = bytes[cursor];
            let start_seg = cursor + 1;
            cursor += 1;
            while cursor < bytes.len() {
                if bytes[cursor] == quote {
                    break;
                }
                cursor += 1;
            }
            let seg = sql.get(start_seg..cursor)?.to_string();
            let next = if cursor < bytes.len() {
                cursor + 1
            } else {
                cursor
            };
            (seg, next)
        } else {
            let start_seg = cursor;
            while cursor < bytes.len()
                && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
            {
                cursor += 1;
            }
            (sql.get(start_seg..cursor)?.to_string(), cursor)
        };

        if segment.is_empty() {
            break;
        }
        segments.push(segment);
        cursor = skip_sql_ws(bytes, next);
        if cursor < bytes.len() && bytes[cursor] == b'.' {
            cursor = skip_sql_ws(bytes, cursor + 1);
            continue;
        }
        break;
    }

    let tail = segments.last()?.trim();
    if tail.is_empty() {
        None
    } else {
        Some((tail.to_string(), cursor))
    }
}

fn collect_sql_select_columns(sql: &str, columns_raw: &str, from_idx: usize) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();

    if columns_raw == "*" {
        push_column_ref("*", &mut cols, &mut seen);
    } else {
        collect_sql_projection_columns(columns_raw, &mut cols, &mut seen);
    }

    let clause_min = from_idx + "FROM".len();
    for clause in ["WHERE", "GROUP BY", "HAVING", "ORDER BY"] {
        if let Some(segment) = top_level_sql_clause_segment(sql, clause, clause_min) {
            collect_sql_identifier_columns(segment, &mut cols, &mut seen);
        }
    }

    cols
}

fn collect_sql_select_columns_by_source(
    sql: &str,
    columns_raw: &str,
    from_idx: usize,
    sources: &[SqlTableSource],
) -> Vec<Vec<String>> {
    let mut qualified = Vec::new();
    let mut unqualified = Vec::new();

    collect_sql_projection_column_refs(columns_raw, &mut qualified, &mut unqualified);
    collect_sql_join_condition_refs(sql, from_idx, &mut qualified, &mut unqualified);

    let clause_min = from_idx + "FROM".len();
    for clause in ["WHERE", "GROUP BY", "HAVING", "ORDER BY"] {
        if let Some(segment) = top_level_sql_clause_segment(sql, clause, clause_min) {
            collect_sql_column_refs(segment, &mut qualified, &mut unqualified);
        }
    }

    let mut columns = vec![Vec::new(); sources.len()];
    let mut seen = vec![HashSet::new(); sources.len()];

    for (qualifier, column) in qualified {
        for (idx, source) in sources.iter().enumerate() {
            if sql_ident_eq(&qualifier, &source.alias) || sql_ident_eq(&qualifier, &source.table) {
                push_column_ref(&column, &mut columns[idx], &mut seen[idx]);
            }
        }
    }

    for column in unqualified {
        if sources.len() == 1 {
            push_column_ref(&column, &mut columns[0], &mut seen[0]);
        } else {
            for idx in 0..sources.len() {
                push_column_ref(&column, &mut columns[idx], &mut seen[idx]);
            }
        }
    }

    columns
}

fn collect_sql_projection_column_refs(
    columns_raw: &str,
    qualified: &mut Vec<(String, String)>,
    unqualified: &mut Vec<String>,
) {
    for projection in split_sql_top_level(columns_raw, ',') {
        let mut base = projection.trim();
        if let Some(as_idx) = find_keyword_top_level_from(base, "AS", 0) {
            base = base.get(..as_idx).unwrap_or(base).trim();
        }
        base = strip_sql_distinct_prefix(base);

        collect_sql_column_refs(base, qualified, unqualified);
    }
}

fn collect_sql_join_condition_refs(
    sql: &str,
    from_idx: usize,
    qualified: &mut Vec<(String, String)>,
    unqualified: &mut Vec<String>,
) {
    let start = from_idx + "FROM".len();
    let end = top_level_sql_clause_start(
        sql,
        start,
        &[
            "WHERE",
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "LIMIT",
            "OFFSET",
            "FETCH",
            "FOR",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "WINDOW",
        ],
    )
    .unwrap_or(sql.len());

    let mut cursor = start;
    while cursor < end {
        let on_idx = find_keyword_top_level_from(sql, "ON", cursor).filter(|idx| *idx < end);
        let using_idx = find_keyword_top_level_from(sql, "USING", cursor).filter(|idx| *idx < end);
        let Some((keyword, idx)) = (match (on_idx, using_idx) {
            (Some(on), Some(using)) if on < using => Some(("ON", on)),
            (Some(_), Some(using)) => Some(("USING", using)),
            (Some(on), None) => Some(("ON", on)),
            (None, Some(using)) => Some(("USING", using)),
            (None, None) => None,
        }) else {
            break;
        };

        if keyword == "USING" {
            let after = skip_sql_ws(sql.as_bytes(), idx + "USING".len());
            if sql.as_bytes().get(after).copied() == Some(b'(')
                && let Some((segment, segment_end)) = balanced_paren_segment(sql, after)
            {
                collect_sql_column_list(segment, unqualified, &mut HashSet::new());
                cursor = segment_end;
                continue;
            }
            cursor = idx + "USING".len();
            continue;
        }

        let segment_start = idx + "ON".len();
        let segment_end = next_sql_join_condition_end(sql, segment_start, end);
        if let Some(segment) = sql.get(segment_start..segment_end) {
            collect_sql_column_refs(segment, qualified, unqualified);
        }
        cursor = segment_end;
    }
}

fn next_sql_join_condition_end(sql: &str, start: usize, end: usize) -> usize {
    find_keyword_top_level_from(sql, "JOIN", start)
        .filter(|idx| *idx < end)
        .unwrap_or(end)
}

fn collect_sql_column_refs(
    segment: &str,
    qualified: &mut Vec<(String, String)>,
    unqualified: &mut Vec<String>,
) {
    let bytes = segment.as_bytes();
    let mut i = 0usize;
    let mut seen_qualified = HashSet::new();
    let mut seen_unqualified = HashSet::new();

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = skip_sql_single_quote(bytes, i + 1);
                continue;
            }
            b'"' | b'`' | b'a'..=b'z' | b'A'..=b'Z' | b'_' => {}
            _ => {
                i += 1;
                continue;
            }
        }

        if i > 0 && bytes[i - 1] == b':' {
            i = parse_sql_identifier_segment(segment, i)
                .map(|(_, next)| next)
                .unwrap_or(i + 1);
            continue;
        }

        let Some((parts, next)) = parse_sql_identifier_path_parts(segment, i) else {
            i += 1;
            continue;
        };
        let after = skip_sql_ws(bytes, next);
        if parts.len() == 1 && after < bytes.len() && bytes[after] == b'(' {
            i = next;
            continue;
        }

        if let Some(column) = parts.last()
            && !is_sql_reference_keyword(column)
        {
            if parts.len() >= 2 {
                let qualifier = parts[parts.len() - 2].clone();
                if seen_qualified.insert((qualifier.clone(), column.clone())) {
                    qualified.push((qualifier, column.clone()));
                }
            } else if seen_unqualified.insert(column.clone()) {
                unqualified.push(column.clone());
            }
        }

        i = next;
    }
}

fn collect_sql_projection_columns(
    columns_raw: &str,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    for projection in split_sql_top_level(columns_raw, ',') {
        let mut base = projection.trim();
        if let Some(as_idx) = find_keyword_top_level_from(base, "AS", 0) {
            base = base.get(..as_idx).unwrap_or(base).trim();
        }
        base = strip_sql_distinct_prefix(base);

        if let Some(column) = normalize_projection_column(base)
            && is_plain_sql_column_ref(&column)
            && !is_sql_reference_keyword(&column)
        {
            push_column_ref(&column, cols, seen);
            continue;
        }

        collect_sql_identifier_columns(base, cols, seen);
    }
}

fn strip_sql_distinct_prefix(input: &str) -> &str {
    let trimmed = input.trim();
    if trimmed.len() >= "DISTINCT".len()
        && trimmed[.."DISTINCT".len()].eq_ignore_ascii_case("DISTINCT")
        && trimmed
            .as_bytes()
            .get("DISTINCT".len())
            .is_some_and(|b| b.is_ascii_whitespace())
    {
        trimmed["DISTINCT".len()..].trim_start()
    } else {
        trimmed
    }
}

fn is_plain_sql_column_ref(value: &str) -> bool {
    value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.'))
}

fn top_level_sql_clause_segment<'a>(sql: &'a str, clause: &str, min_idx: usize) -> Option<&'a str> {
    let clause_idx = find_keyword_top_level_from(sql, clause, min_idx)?;
    let start = clause_idx + clause.len();
    let end = top_level_sql_clause_start(
        sql,
        start,
        &[
            "WHERE",
            "GROUP BY",
            "HAVING",
            "ORDER BY",
            "SET",
            "VALUES",
            "ON CONFLICT",
            "USING",
            "RETURNING",
            "LIMIT",
            "OFFSET",
            "FETCH",
            "FOR",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "WINDOW",
        ],
    )
    .unwrap_or(sql.len());

    sql.get(start..end)
}

fn top_level_sql_clause_start(sql: &str, min_idx: usize, clauses: &[&str]) -> Option<usize> {
    clauses
        .iter()
        .filter_map(|keyword| find_keyword_top_level_from(sql, keyword, min_idx))
        .min()
}

fn collect_sql_insert_columns(sql: &str, table_end: usize) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    let cursor = skip_sql_ws(sql.as_bytes(), table_end);
    if sql.as_bytes().get(cursor).copied() == Some(b'(')
        && let Some((segment, _)) = balanced_paren_segment(sql, cursor)
    {
        collect_sql_column_list(segment, &mut cols, &mut seen);
    }
    if let Some(conflict) = top_level_sql_clause_segment(sql, "ON CONFLICT", table_end) {
        collect_sql_identifier_columns(conflict, &mut cols, &mut seen);
    }
    cols
}

fn collect_sql_update_columns(sql: &str, table_end: usize) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    if let Some(set_segment) = top_level_sql_clause_segment(sql, "SET", table_end) {
        collect_sql_identifier_columns(set_segment, &mut cols, &mut seen);
    }
    for clause in ["FROM", "WHERE", "RETURNING"] {
        if let Some(segment) = top_level_sql_clause_segment(sql, clause, table_end) {
            collect_sql_identifier_columns(segment, &mut cols, &mut seen);
        }
    }
    cols
}

fn collect_sql_delete_columns(sql: &str, table_end: usize) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();
    for clause in ["USING", "WHERE", "RETURNING"] {
        if let Some(segment) = top_level_sql_clause_segment(sql, clause, table_end) {
            collect_sql_identifier_columns(segment, &mut cols, &mut seen);
        }
    }
    cols
}

fn collect_sql_column_list(segment: &str, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    for item in split_sql_top_level(segment, ',') {
        if let Some((column, _, _)) = parse_sql_identifier_path(item.trim(), 0) {
            push_column_ref(&column, cols, seen);
        }
    }
}

fn normalize_projection_column(expr: &str) -> Option<String> {
    let expr = expr.trim();
    if expr.is_empty() {
        return None;
    }
    if expr == "*" {
        return Some("*".to_string());
    }

    let mut base = expr;
    if let Some(as_idx) = find_keyword_top_level_from(expr, "AS", 0) {
        base = expr.get(..as_idx).unwrap_or(expr).trim();
    }
    let token = base.split_whitespace().next().unwrap_or(base).trim();
    if token.is_empty() {
        return None;
    }

    let normalized = token.trim_matches('"').trim_matches('`');
    let tail = normalized.rsplit('.').next().unwrap_or(normalized).trim();
    if tail.is_empty() {
        None
    } else {
        Some(tail.to_string())
    }
}

fn balanced_paren_segment(input: &str, open_idx: usize) -> Option<(&str, usize)> {
    let bytes = input.as_bytes();
    if bytes.get(open_idx).copied() != Some(b'(') {
        return None;
    }

    let mut depth = 1i32;
    let mut i = open_idx + 1;
    let start = i;
    let mut in_quote: Option<u8> = None;
    let mut escaped = false;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' {
                escaped = true;
                i += 1;
                continue;
            }
            if b == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((input.get(start..i)?, i + 1));
                }
            }
            _ => {}
        }
        i += 1;
    }

    None
}

fn collect_sql_identifier_columns(
    segment: &str,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let bytes = segment.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i = skip_sql_single_quote(bytes, i + 1);
                continue;
            }
            b'"' | b'`' | b'a'..=b'z' | b'A'..=b'Z' | b'_' => {}
            _ => {
                i += 1;
                continue;
            }
        }

        if i > 0 && bytes[i - 1] == b':' {
            i = parse_sql_identifier_segment(segment, i)
                .map(|(_, next)| next)
                .unwrap_or(i + 1);
            continue;
        }

        let Some((column, next, segment_count)) = parse_sql_identifier_path(segment, i) else {
            i += 1;
            continue;
        };
        let after = skip_sql_ws(bytes, next);
        if segment_count == 1 && after < bytes.len() && bytes[after] == b'(' {
            i = next;
            continue;
        }
        if !is_sql_reference_keyword(&column) {
            push_column_ref(&column, cols, seen);
        }
        i = next;
    }
}

fn parse_sql_identifier_path(input: &str, start: usize) -> Option<(String, usize, usize)> {
    let bytes = input.as_bytes();
    let (mut last, mut cursor) = parse_sql_identifier_segment(input, start)?;
    let mut count = 1usize;

    loop {
        cursor = skip_sql_ws(bytes, cursor);
        if cursor < bytes.len() && bytes[cursor] == b'.' {
            let (segment, next) = parse_sql_identifier_segment(input, cursor + 1)?;
            last = segment;
            count += 1;
            cursor = next;
            continue;
        }
        break;
    }

    Some((last, cursor, count))
}

fn parse_sql_identifier_path_parts(input: &str, start: usize) -> Option<(Vec<String>, usize)> {
    let bytes = input.as_bytes();
    let (first, mut cursor) = parse_sql_identifier_segment(input, start)?;
    let mut parts = vec![first];

    loop {
        cursor = skip_sql_ws(bytes, cursor);
        if cursor < bytes.len() && bytes[cursor] == b'.' {
            let next_start = skip_sql_ws(bytes, cursor + 1);
            if bytes.get(next_start).copied() == Some(b'*') {
                parts.push("*".to_string());
                cursor = next_start + 1;
                break;
            }
            let (segment, next) = parse_sql_identifier_segment(input, next_start)?;
            parts.push(segment);
            cursor = next;
            continue;
        }
        break;
    }

    Some((parts, cursor))
}

fn parse_sql_identifier_segment(input: &str, start: usize) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    let cursor = skip_sql_ws(bytes, start);
    if cursor >= bytes.len() {
        return None;
    }

    if matches!(bytes[cursor], b'"' | b'`') {
        let quote = bytes[cursor];
        let mut i = cursor + 1;
        let start_seg = i;
        while i < bytes.len() {
            if bytes[i] == quote {
                return Some((input.get(start_seg..i)?.to_string(), i + 1));
            }
            i += 1;
        }
        return None;
    }

    if !matches!(bytes[cursor], b'a'..=b'z' | b'A'..=b'Z' | b'_') {
        return None;
    }

    let mut i = cursor + 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }

    Some((input.get(cursor..i)?.to_string(), i))
}

fn skip_sql_single_quote(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() {
        if bytes[idx] == b'\'' {
            if idx + 1 < bytes.len() && bytes[idx + 1] == b'\'' {
                idx += 2;
                continue;
            }
            return idx + 1;
        }
        idx += 1;
    }
    idx
}

fn is_sql_reference_keyword(ident: &str) -> bool {
    matches!(
        ident.to_ascii_uppercase().as_str(),
        "ALL"
            | "AND"
            | "ANY"
            | "ASC"
            | "AS"
            | "BETWEEN"
            | "BY"
            | "CASE"
            | "COLLATE"
            | "CROSS"
            | "DESC"
            | "DISTINCT"
            | "ELSE"
            | "END"
            | "FALSE"
            | "FIRST"
            | "FROM"
            | "GROUP"
            | "HAVING"
            | "IN"
            | "INNER"
            | "IS"
            | "JOIN"
            | "LAST"
            | "LEFT"
            | "LIKE"
            | "LIMIT"
            | "NATURAL"
            | "NOT"
            | "NULL"
            | "NULLS"
            | "OFFSET"
            | "ON"
            | "OR"
            | "ORDER"
            | "OUTER"
            | "RIGHT"
            | "SELECT"
            | "THEN"
            | "TRUE"
            | "USING"
            | "WHEN"
            | "WHERE"
    )
}

fn split_sql_top_level(input: &str, delimiter: char) -> Vec<&str> {
    let bytes = input.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;
    let mut escaped = false;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' {
                escaped = true;
                i += 1;
                continue;
            }
            if b == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }

        if b == delimiter as u8 && depth == 0 {
            out.push(input.get(start..i).unwrap_or_default());
            start = i + 1;
        }
        i += 1;
    }
    out.push(input.get(start..).unwrap_or_default());
    out
}

fn find_keyword_top_level_from(sql: &str, keyword: &str, min_idx: usize) -> Option<usize> {
    if keyword.is_empty() {
        return None;
    }

    let bytes = sql.as_bytes();
    let upper = bytes
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let kw = keyword
        .as_bytes()
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();

    let mut i = 0usize;
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;
    let mut escaped = false;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if escaped {
                escaped = false;
                i += 1;
                continue;
            }
            if b == b'\\' {
                escaped = true;
                i += 1;
                continue;
            }
            if b == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => in_quote = Some(b),
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }

        if depth == 0
            && i >= min_idx
            && upper
                .get(i..i.saturating_add(kw.len()))
                .is_some_and(|slice| slice == kw)
        {
            let before_ok = if i == 0 {
                true
            } else {
                !is_ident_char(upper[i - 1] as char)
            };
            let after = i + kw.len();
            let after_ok = if after >= upper.len() {
                true
            } else {
                !is_ident_char(upper[after] as char)
            };

            if before_ok && after_ok {
                return Some(i);
            }
        }

        i += 1;
    }

    None
}

fn starts_with_keyword_at(sql: &str, idx: usize, keyword: &str) -> bool {
    let bytes = sql.as_bytes();
    let kw = keyword.as_bytes();
    if idx + kw.len() > bytes.len() {
        return false;
    }
    if !bytes[idx..idx + kw.len()]
        .iter()
        .zip(kw)
        .all(|(left, right)| left.eq_ignore_ascii_case(right))
    {
        return false;
    }

    let before_ok = if idx == 0 {
        true
    } else {
        !is_ident_char(bytes[idx - 1] as char)
    };
    let after = idx + kw.len();
    let after_ok = if after >= bytes.len() {
        true
    } else {
        !is_ident_char(bytes[after] as char)
    };

    before_ok && after_ok
}

fn skip_sql_ws(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_qail_candidate_from_line() {
        let line = r#"const q = "get users fields name, email where id = $1";"#;
        let (_, query) = extract_qail_candidate_from_line(line).expect("qail candidate expected");
        assert_eq!(query, "get users fields name, email where id = $1");
    }

    #[test]
    fn test_parse_sql_reference_select() {
        let sql = "SELECT name, email FROM users WHERE id = $1";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["name", "email", "id"]);
    }

    #[test]
    fn test_parse_sql_reference_quoted_schema_table() {
        let sql = r#"SELECT "id", "email" FROM "public"."users" WHERE "id" = $1"#;
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["id", "email"]);
    }

    #[test]
    fn test_parse_sql_reference_tracks_predicate_and_order_columns() {
        let sql = "SELECT id FROM users WHERE email = $1 ORDER BY created_at DESC";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["id", "email", "created_at"]);
    }

    #[test]
    fn test_parse_sql_reference_tracks_projection_expression_columns() {
        let sql =
            "SELECT COUNT(email) AS email_count, date_trunc('day', created_at) AS day FROM users";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "created_at"]);
    }

    #[test]
    fn test_parse_sql_reference_skips_params_strings_and_keywords() {
        let sql = "SELECT id FROM users WHERE lower(users.email) = lower(:email) AND status = 'active' ORDER BY users.created_at DESC NULLS LAST";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Select);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["id", "email", "status", "created_at"]);
    }

    #[test]
    fn test_parse_sql_reference_insert_columns() {
        let sql = "INSERT INTO users (email, status) VALUES ($1, $2)";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Insert);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "status"]);
    }

    #[test]
    fn test_parse_sql_reference_update_columns() {
        let sql =
            "UPDATE users SET email = $1, status = 'active' WHERE id = $2 RETURNING updated_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Update);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "status", "id", "updated_at"]);
    }

    #[test]
    fn test_parse_sql_reference_delete_columns() {
        let sql = "DELETE FROM users WHERE email = $1 RETURNING deleted_at";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind, SqlStmtKind::Delete);
        assert_eq!(table, "users");
        assert_eq!(cols, vec!["email", "deleted_at"]);
    }

    #[test]
    fn test_parse_sql_reference_merge() {
        let sql = "MERGE INTO orders USING staging_orders ON orders.id = staging_orders.id WHEN MATCHED THEN UPDATE SET status = staging_orders.status";
        let (kind, table, cols) = parse_sql_reference(sql).expect("sql parse");
        assert_eq!(kind.as_str(), "MERGE");
        assert_eq!(table, "orders");
        assert!(cols.is_empty());
    }

    #[test]
    fn test_parse_sql_references_tracks_cte_base_table() {
        let sql = "WITH active_users AS (SELECT id, email FROM users WHERE status = $1) SELECT id FROM active_users";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        let (kind, table, cols) = &refs[0];
        assert_eq!(kind.as_str(), "SELECT");
        assert_eq!(table, "users");
        assert_eq!(cols, &vec!["id", "email", "status"]);
    }

    #[test]
    fn test_parse_sql_references_supports_materialized_ctes() {
        let sql = "WITH active_users AS NOT MATERIALIZED (SELECT id, email FROM users WHERE status = $1) SELECT id FROM ACTIVE_USERS";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].1, "users");
        assert_eq!(refs[0].2, vec!["id", "email", "status"]);
    }

    #[test]
    fn test_parse_sql_references_skips_intermediate_cte_aliases() {
        let sql = "WITH raw_users AS (SELECT id, email FROM users), active_users AS (SELECT id FROM raw_users WHERE email IS NOT NULL) SELECT id FROM active_users";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].1, "users");
        assert_eq!(refs[0].2, vec!["id", "email"]);
    }

    #[test]
    fn test_parse_sql_references_tracks_joined_table_columns() {
        let sql = "SELECT u.id, o.total FROM users u JOIN orders o ON o.user_id = u.id WHERE o.status = $1 ORDER BY o.created_at DESC";
        let refs = parse_sql_references(sql);

        assert_eq!(refs.len(), 2, "{refs:?}");

        let users = refs
            .iter()
            .find(|(_, table, _)| table == "users")
            .expect("users reference");
        assert_eq!(users.2, vec!["id"]);

        let orders = refs
            .iter()
            .find(|(_, table, _)| table == "orders")
            .expect("orders reference");
        assert_eq!(orders.2, vec!["total", "user_id", "status", "created_at"]);
    }

    #[test]
    fn test_set_payload_column_extraction() {
        let cmd = parse("set users values name = \"Alice\", status = \"active\" where id = $1")
            .expect("set parse");
        let columns = extract_payload_columns(&cmd);
        assert_eq!(columns, vec!["name", "status"]);
    }

    #[test]
    fn test_command_reference_tracks_filter_columns() {
        let cmd = parse("get users fields id where email = $1 order by created_at desc")
            .expect("get parse");
        let reference =
            command_to_reference(Path::new("src/users.ts"), 1, &cmd).expect("reference");

        assert_eq!(reference.table, "users");
        assert_eq!(reference.columns, vec!["id", "email", "created_at"]);
    }

    #[test]
    fn test_non_rust_scan_uses_parser_and_sql_classifier() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            const q = "get users fields id, email where active = true";
            const s = "SELECT id, email FROM users WHERE active = true";
        "#;

        std::fs::write(&path, source).expect("write temp ts file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        let qail_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::Qail)
            .collect::<Vec<_>>();
        assert_eq!(qail_refs.len(), 1);
        assert_eq!(qail_refs[0].table, "users");
        assert_eq!(qail_refs[0].columns, vec!["id", "email", "active"]);

        let raw_sql_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::RawSql)
            .collect::<Vec<_>>();
        assert_eq!(raw_sql_refs.len(), 1);
        assert_eq!(raw_sql_refs[0].table, "users");
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email", "active"]);
    }

    #[test]
    fn test_non_rust_scan_supports_multiline_literals() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_multiline_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"const q = `
get users
fields id, email
where active = true
`;
const s = "
SELECT id, email
FROM users
WHERE active = true
";"#;

        std::fs::write(&path, source).expect("write temp ts file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        let qail_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::Qail)
            .collect::<Vec<_>>();
        assert_eq!(qail_refs.len(), 1);
        assert_eq!(qail_refs[0].table, "users");
        assert_eq!(qail_refs[0].columns, vec!["id", "email", "active"]);

        let raw_sql_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::RawSql)
            .collect::<Vec<_>>();
        assert_eq!(raw_sql_refs.len(), 1);
        assert_eq!(raw_sql_refs[0].table, "users");
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email", "active"]);
    }

    #[test]
    fn test_rust_scan_uses_semantic_sql_detection() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_rust_sql_{}_{}.rs",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            // SELECT id FROM comments_should_not_match
            fn demo() {
                let sql = "SELECT id, email FROM users WHERE active = true";
                let _ = query(sql);
            }
        "#;

        std::fs::write(&path, source).expect("write temp rust file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        let raw_sql_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::RawSql)
            .collect::<Vec<_>>();

        assert_eq!(raw_sql_refs.len(), 1, "{raw_sql_refs:?}");
        assert_eq!(raw_sql_refs[0].table, "users");
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email", "active"]);
    }

    #[test]
    fn test_non_rust_scan_tracks_raw_sql_cte_base_table() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_cte_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            const sql = `
                WITH active_users AS (
                    SELECT id, email FROM users WHERE status = $1
                )
                SELECT id FROM active_users
            `;
        "#;

        std::fs::write(&path, source).expect("write temp text file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        let raw_sql_refs = refs
            .iter()
            .filter(|r| r.query_type == QueryType::RawSql)
            .collect::<Vec<_>>();

        assert_eq!(raw_sql_refs.len(), 1, "{raw_sql_refs:?}");
        assert_eq!(raw_sql_refs[0].table, "users");
        assert_eq!(raw_sql_refs[0].columns, vec!["id", "email", "status"]);
    }

    #[test]
    fn test_non_rust_scan_ignores_comment_markers() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_comments_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            // "SELECT id, email FROM users"
            -- "DELETE FROM users"
            /*
            const q = "get block_users fields id";
            const s = "DELETE FROM block_users";
            */
            const msg = "ok";
            # "DELETE FROM users"
        "#;

        std::fs::write(&path, source).expect("write temp text file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        assert!(refs.is_empty(), "{refs:?}");
    }

    #[test]
    fn test_non_rust_scan_preserves_js_private_field_queries() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_private_fields_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            class Store {
                #qail = "get users fields id";
                #sql = "SELECT id FROM users";
            }
        "#;

        std::fs::write(&path, source).expect("write temp text file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        assert_eq!(refs.len(), 2, "{refs:?}");
        assert!(refs.iter().any(|r| r.query_type == QueryType::Qail));
        assert!(refs.iter().any(|r| r.query_type == QueryType::RawSql));
    }

    #[test]
    fn test_non_rust_scan_preserves_js_decrement_operator_queries() {
        let scanner = CodebaseScanner::new();
        let tmp_name = format!(
            "qail_scanner_text_decrement_{}_{}.ts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(tmp_name);

        let source = r#"
            let counter = 1;
            counter--; const q = "get users fields id";
        "#;

        std::fs::write(&path, source).expect("write temp text file");
        let refs = scanner.scan(&path);
        let _ = std::fs::remove_file(&path);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].query_type, QueryType::Qail);
        assert_eq!(refs[0].table, "users");
        assert_eq!(refs[0].columns, vec!["id"]);
    }

    #[test]
    fn test_scan_with_details_includes_zero_ref_files_in_directories() {
        let scanner = CodebaseScanner::new();
        let root = std::env::temp_dir().join(format!(
            "qail_scanner_details_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("mkdir temp root");

        let with_ref = root.join("with_ref.ts");
        std::fs::write(&with_ref, r#"const q = "get users fields id";"#).expect("write with_ref");
        let no_ref = root.join("no_ref.ts");
        std::fs::write(&no_ref, r#"const msg = "hello";"#).expect("write no_ref");

        let result = scanner.scan_with_details(&root);

        let mut entries = result
            .files
            .iter()
            .map(|f| {
                (
                    f.file.file_name().and_then(|n| n.to_str()).unwrap_or(""),
                    f.mode,
                    f.ref_count,
                )
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(name, _, _)| *name);

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "no_ref.ts");
        assert_eq!(entries[0].1, AnalysisMode::TextSemantic);
        assert_eq!(entries[0].2, 0);
        assert_eq!(entries[1].0, "with_ref.ts");
        assert_eq!(entries[1].1, AnalysisMode::TextSemantic);
        assert_eq!(entries[1].2, 1);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_scan_with_details_includes_tsx_and_jsx_files() {
        let scanner = CodebaseScanner::new();
        let root = std::env::temp_dir().join(format!(
            "qail_scanner_jsx_tsx_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("mkdir temp root");

        let tsx = root.join("widget.tsx");
        std::fs::write(&tsx, r#"const q = "get users fields id";"#).expect("write tsx");
        let jsx = root.join("panel.jsx");
        std::fs::write(&jsx, r#"const s = "SELECT id FROM users";"#).expect("write jsx");

        let result = scanner.scan_with_details(&root);

        let mut files = result
            .files
            .iter()
            .map(|f| {
                (
                    f.file.file_name().and_then(|n| n.to_str()).unwrap_or(""),
                    f.ref_count,
                )
            })
            .collect::<Vec<_>>();
        files.sort_by_key(|(name, _)| *name);

        assert_eq!(files, vec![("panel.jsx", 1), ("widget.tsx", 1)]);
        assert_eq!(result.refs.len(), 2, "{:?}", result.refs);

        let _ = std::fs::remove_dir_all(&root);
    }
}
