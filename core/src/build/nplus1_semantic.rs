use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Diagnostic rule code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum NPlusOneCode {
    /// Query execution inside a work loop.
    N1001,
    /// Query execution inside a work loop where query shape depends on loop vars.
    N1002,
    /// Query execution inside nested work loops.
    N1004,
}

impl NPlusOneCode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::N1001 => "N1-001",
            Self::N1002 => "N1-002",
            Self::N1004 => "N1-004",
        }
    }
}

impl std::fmt::Display for NPlusOneCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Diagnostic severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NPlusOneSeverity {
    Warning,
    Error,
}

/// A single semantic N+1 diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NPlusOneDiagnostic {
    pub(crate) code: NPlusOneCode,
    pub(crate) severity: NPlusOneSeverity,
    pub(crate) file: String,
    pub(crate) line: usize,
    pub(crate) column: usize,
    pub(crate) message: String,
    pub(crate) hint: Option<String>,
}

impl std::fmt::Display for NPlusOneDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}:{}:{}: {}",
            self.code, self.file, self.line, self.column, self.message
        )?;
        if let Some(ref hint) = self.hint {
            write!(f, " (hint: {})", hint)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct QueryBinding {
    uses_loop_var: bool,
    batched: bool,
}

#[derive(Debug)]
struct LoopFrame {
    exit_depth: i32,
    loop_vars: HashSet<String>,
    query_bindings: HashMap<String, QueryBinding>,
}

impl LoopFrame {
    fn new(exit_depth: i32, loop_vars: HashSet<String>) -> Self {
        Self {
            exit_depth,
            loop_vars,
            query_bindings: HashMap::new(),
        }
    }
}

const EXEC_PATTERNS: [&str; 8] = [
    ".fetch_all_with_rls(",
    ".fetch_all_uncached(",
    ".fetch_all_fast(",
    ".fetch_all(",
    ".fetch_one(",
    ".fetch_opt(",
    ".execute(",
    ".query(",
];

/// Detect semantic N+1 patterns in a single Rust source file.
pub(crate) fn detect_n_plus_one_in_file(file: &str, source: &str) -> Vec<NPlusOneDiagnostic> {
    let lines: Vec<&str> = source.lines().collect();
    let mut out = Vec::new();
    let mut seen = HashSet::<(usize, usize, NPlusOneCode)>::new();

    let mut loop_stack: Vec<LoopFrame> = Vec::new();
    let mut pending_for_vars: Option<HashSet<String>> = None;
    let mut brace_depth: i32 = 0;

    for (idx, raw_line) in lines.iter().enumerate() {
        let line_no = idx + 1;
        let line = strip_line_comment(raw_line);
        let trimmed = line.trim();

        if let Some(vars) = pending_for_vars.take() {
            if line.contains('{') {
                loop_stack.push(LoopFrame::new(brace_depth, vars));
            } else {
                pending_for_vars = Some(vars);
            }
        }

        if let Some(for_vars) = parse_for_loop_vars(trimmed) {
            if line.contains('{') {
                loop_stack.push(LoopFrame::new(brace_depth, for_vars));
            } else {
                pending_for_vars = Some(for_vars);
            }
        }

        let work_depth = loop_stack.len();
        if work_depth > 0 {
            let loop_vars = active_loop_vars(&loop_stack);

            if let Some((var_name, qail_start_col, chain)) =
                extract_query_binding(&lines, idx, line)
            {
                if let Some(frame) = loop_stack.last_mut() {
                    frame.query_bindings.insert(
                        var_name,
                        QueryBinding {
                            uses_loop_var: any_loop_var_in_text(&loop_vars, &chain),
                            batched: is_batched_expr(&chain),
                        },
                    );
                }

                // Inline execute in builder chain inside loop.
                if let Some(exec) = find_exec_call(&chain) {
                    emit_query_loop_diag(
                        &mut out,
                        &mut seen,
                        file,
                        line_no,
                        qail_start_col + exec.column_offset.saturating_sub(1),
                        work_depth,
                        any_loop_var_in_text(&loop_vars, &chain),
                    );
                }
            }

            if let Some(exec) = find_exec_call(line) {
                let matched_binding = find_binding_for_arg(&loop_stack, &exec.first_arg);
                let batched = matched_binding
                    .as_ref()
                    .map(|b| b.batched)
                    .unwrap_or_else(|| is_batched_expr(&exec.first_arg));

                if !batched {
                    let uses_loop_var = matched_binding
                        .as_ref()
                        .map(|b| b.uses_loop_var)
                        .unwrap_or_else(|| any_loop_var_in_text(&loop_vars, &exec.first_arg));
                    emit_query_loop_diag(
                        &mut out,
                        &mut seen,
                        file,
                        line_no,
                        exec.column,
                        work_depth,
                        uses_loop_var,
                    );
                }
            }
        }

        brace_depth += brace_delta(line);
        while let Some(frame) = loop_stack.last() {
            if brace_depth <= frame.exit_depth {
                loop_stack.pop();
            } else {
                break;
            }
        }
    }

    out
}

/// Detect semantic N+1 patterns in all Rust files under a directory.
pub(crate) fn detect_n_plus_one_in_dir(dir: &Path) -> Vec<NPlusOneDiagnostic> {
    let mut files = Vec::new();
    collect_rust_files(dir, &mut files);
    let mut out = Vec::new();
    for path in files {
        let Ok(source) = std::fs::read_to_string(&path) else {
            continue;
        };
        out.extend(detect_n_plus_one_in_file(
            &path.display().to_string(),
            &source,
        ));
    }
    out
}

fn emit_query_loop_diag(
    out: &mut Vec<NPlusOneDiagnostic>,
    seen: &mut HashSet<(usize, usize, NPlusOneCode)>,
    file: &str,
    line: usize,
    column: usize,
    work_depth: usize,
    uses_loop_var: bool,
) {
    let (code, severity, message, hint) = if work_depth >= 2 {
        (
            NPlusOneCode::N1004,
            NPlusOneSeverity::Error,
            "Query execution inside nested loop can degrade to O(n^2) or worse".to_string(),
            Some("Restructure to collect keys first, then run one batched query".to_string()),
        )
    } else if uses_loop_var {
        (
            NPlusOneCode::N1002,
            NPlusOneSeverity::Warning,
            "Loop-variable-dependent query execution detected inside loop".to_string(),
            Some("Collect IDs first, then use a single batched query with IN/ANY".to_string()),
        )
    } else {
        (
            NPlusOneCode::N1001,
            NPlusOneSeverity::Warning,
            "Query execution detected inside loop".to_string(),
            Some("Move execution outside loop or batch inputs per query".to_string()),
        )
    };

    if !seen.insert((line, column, code)) {
        return;
    }

    out.push(NPlusOneDiagnostic {
        code,
        severity,
        file: file.to_string(),
        line,
        column,
        message,
        hint,
    });
}

fn parse_for_loop_vars(trimmed_line: &str) -> Option<HashSet<String>> {
    let rest = trimmed_line.strip_prefix("for ")?;
    let in_pos = rest.find(" in ")?;
    let pattern = rest[..in_pos].trim();
    let mut out = HashSet::new();
    for ident in extract_idents(pattern) {
        if ident == "_" || ident == "mut" || ident == "ref" {
            continue;
        }
        let starts_with_lower = ident
            .chars()
            .next()
            .map(|c| c.is_ascii_lowercase() || c == '_')
            .unwrap_or(false);
        if starts_with_lower {
            out.insert(ident);
        }
    }
    Some(out)
}

fn extract_idents(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if is_ident_char(ch) {
            current.push(ch);
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

fn active_loop_vars(loop_stack: &[LoopFrame]) -> HashSet<String> {
    let mut out = HashSet::new();
    for frame in loop_stack {
        for var in &frame.loop_vars {
            out.insert(var.clone());
        }
    }
    out
}

fn extract_query_binding(
    lines: &[&str],
    line_idx: usize,
    line: &str,
) -> Option<(String, usize, String)> {
    let qail_pos = line.find("Qail::")?;
    let var_name = extract_assignment_ident(line, qail_pos)?;
    let chain = collect_chain(lines, line_idx, qail_pos);
    Some((var_name, qail_pos + 1, chain))
}

fn extract_assignment_ident(line: &str, qail_pos: usize) -> Option<String> {
    let prefix = line.get(..qail_pos)?.trim_end();
    let prefix_trimmed = prefix.trim_start();

    if let Some(after_let) = prefix_trimmed.strip_prefix("let ") {
        let binding_part = after_let
            .split('=')
            .next()
            .map(str::trim)?
            .strip_prefix("mut ")
            .unwrap_or(after_let.split('=').next().map(str::trim)?);
        let binding_part = binding_part.split(':').next().map(str::trim)?;
        if binding_part.is_empty() || binding_part.starts_with('(') {
            return None;
        }
        if binding_part.chars().all(is_ident_char) {
            return Some(binding_part.to_string());
        }
        return None;
    }

    if let Some(eq_pos) = prefix_trimmed.rfind('=') {
        let lhs = prefix_trimmed[..eq_pos].trim();
        if lhs.chars().all(is_ident_char) {
            return Some(lhs.to_string());
        }
    }
    None
}

fn collect_chain(lines: &[&str], start_line_idx: usize, qail_pos: usize) -> String {
    let mut chain = lines[start_line_idx][qail_pos..].trim().to_string();
    let mut depth = super::scanner::count_net_delimiters(&chain);
    let mut j = start_line_idx + 1;

    while j < lines.len() {
        let next = strip_line_comment(lines[j]).trim();
        if next.is_empty() {
            if depth > 0 {
                j += 1;
                continue;
            }
            break;
        }
        if depth > 0 || next.starts_with('.') {
            chain.push(' ');
            chain.push_str(next);
            depth += super::scanner::count_net_delimiters(next);
            j += 1;
            continue;
        }
        break;
    }

    chain
}

fn any_loop_var_in_text(loop_vars: &HashSet<String>, text: &str) -> bool {
    loop_vars.iter().any(|v| contains_ident(text, v))
}

fn is_batched_expr(text: &str) -> bool {
    text.contains(".in_vals(")
        || text.contains("Operator::In")
        || text.contains(".chunks(")
        || text.contains("Value::Array(")
}

#[derive(Debug)]
struct ExecCall {
    column: usize,
    column_offset: usize,
    first_arg: String,
}

fn find_exec_call(line: &str) -> Option<ExecCall> {
    let mut best: Option<(usize, &str)> = None;
    for pat in EXEC_PATTERNS {
        if let Some(pos) = line.find(pat) {
            match best {
                Some((best_pos, _)) if best_pos <= pos => {}
                _ => best = Some((pos, pat)),
            }
        }
    }

    let (pos, pat) = best?;
    let arg_start = pos + pat.len();
    let first_arg = extract_first_call_arg(line, arg_start);
    Some(ExecCall {
        column: pos + 1,
        column_offset: pos + 1,
        first_arg,
    })
}

fn extract_first_call_arg(line: &str, start: usize) -> String {
    let tail = match line.get(start..) {
        Some(v) => v,
        None => return String::new(),
    };
    let mut in_string = false;
    let mut prev = '\0';
    let mut depth: i32 = 0;

    for (idx, ch) in tail.char_indices() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
            prev = ch;
            continue;
        }
        if !in_string {
            match ch {
                '(' | '[' | '{' => depth += 1,
                ')' => {
                    if depth == 0 {
                        return tail[..idx].trim().to_string();
                    }
                    depth -= 1;
                }
                ',' if depth == 0 => return tail[..idx].trim().to_string(),
                _ => {}
            }
        }
        prev = ch;
    }

    tail.trim().to_string()
}

fn find_binding_for_arg(loop_stack: &[LoopFrame], arg: &str) -> Option<QueryBinding> {
    for frame in loop_stack.iter().rev() {
        for (name, binding) in &frame.query_bindings {
            if contains_ident(arg, name) {
                return Some(binding.clone());
            }
        }
    }
    None
}

fn contains_ident(text: &str, ident: &str) -> bool {
    if ident.is_empty() {
        return false;
    }

    let mut cursor = 0usize;
    while cursor < text.len() {
        let Some(rel_pos) = text[cursor..].find(ident) else {
            return false;
        };
        let pos = cursor + rel_pos;
        let before_ok = if pos == 0 {
            true
        } else {
            let before = text[..pos].chars().next_back().unwrap_or(' ');
            !is_ident_char(before)
        };
        let after_pos = pos + ident.len();
        let after_ok = if after_pos >= text.len() {
            true
        } else {
            let after = text[after_pos..].chars().next().unwrap_or(' ');
            !is_ident_char(after)
        };
        if before_ok && after_ok {
            return true;
        }
        cursor = after_pos;
    }
    false
}

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn strip_line_comment(line: &str) -> &str {
    let mut in_string = false;
    let mut prev = '\0';
    for (idx, ch) in line.char_indices() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
        }
        if !in_string && ch == '/' && prev == '/' {
            return &line[..idx - 1];
        }
        prev = ch;
    }
    line
}

fn brace_delta(line: &str) -> i32 {
    let mut in_string = false;
    let mut prev = '\0';
    let mut depth = 0i32;
    for ch in line.chars() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
        } else if !in_string {
            match ch {
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            }
        }
        prev = ch;
    }
    depth
}

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && (name == "target" || name == ".git" || name == "node_modules")
            {
                continue;
            }
            collect_rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_loop_variable_dependent_query_execution() {
        let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn) {
    for id in ids {
        let cmd = Qail::get("users").eq("id", id);
        let _ = conn.fetch_all(&cmd).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "{diags:?}"
        );
    }

    #[test]
    fn detects_nested_loop_as_error() {
        let source = r#"
async fn demo(tenants: Vec<i64>, ids: Vec<i64>, conn: &Conn) {
    for tenant in tenants {
        for id in ids {
            let cmd = Qail::get("users").eq("tenant_id", tenant).eq("id", id);
            let _ = conn.fetch_all(&cmd).await;
        }
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1004),
            "{diags:?}"
        );
    }

    #[test]
    fn ignores_batched_in_vals_pattern() {
        let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn) {
    for chunk in ids.chunks(100) {
        let cmd = Qail::get("users").in_vals("id", chunk.to_vec());
        let _ = conn.fetch_all(&cmd).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn does_not_flag_builder_without_execution() {
        let source = r#"
fn demo(ids: Vec<i64>) {
    for id in ids {
        let _cmd = Qail::get("users").eq("id", id);
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }
}
