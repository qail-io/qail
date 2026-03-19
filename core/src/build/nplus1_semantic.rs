use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Diagnostic rule code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum NPlusOneCode {
    /// Query execution inside a work loop.
    N1001,
    /// Query execution inside a work loop where query shape depends on loop vars.
    N1002,
    /// Function/method that executes query is called inside a work loop.
    N1003,
    /// Query execution inside nested work loops.
    N1004,
}

impl NPlusOneCode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::N1001 => "N1-001",
            Self::N1002 => "N1-002",
            Self::N1003 => "N1-003",
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

#[derive(Debug, Clone)]
struct SourceUnit {
    file: String,
    source: String,
    module_path: Vec<String>,
}

#[derive(Debug, Clone)]
struct FunctionCallSite {
    column: usize,
    kind: FunctionCallKind,
}

#[derive(Debug, Clone)]
enum FunctionCallKind {
    Bare(String),
    Qualified { path: Vec<String>, name: String },
    SelfMethod(String),
}

#[derive(Debug, Clone)]
struct FunctionSymbol {
    file: String,
    module_path: Vec<String>,
    name: String,
    impl_type: Option<String>,
    start_line: usize,
    end_line: usize,
    direct_query_exec: bool,
    calls: Vec<FunctionCallSite>,
}

#[derive(Debug, Clone)]
struct PendingFunction {
    symbol: FunctionSymbol,
}

#[derive(Debug, Clone)]
struct ActiveFunction {
    exit_depth: i32,
    symbol: FunctionSymbol,
}

#[derive(Debug, Default)]
struct SemanticNPlusOneIndex {
    functions: Vec<FunctionSymbol>,
    query_executing_functions: Vec<bool>,
    line_to_function_by_file: HashMap<String, Vec<Option<usize>>>,
    free_by_module_and_name: HashMap<String, Vec<usize>>,
    free_by_short_name: HashMap<String, Vec<usize>>,
    free_by_qualified_path: HashMap<String, Vec<usize>>,
    method_by_module_impl_and_name: HashMap<String, Vec<usize>>,
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

const ITER_LOOP_PATTERNS: [&str; 4] = [
    ".for_each(",
    ".try_for_each(",
    ".for_each_concurrent(",
    ".try_for_each_concurrent(",
];

/// Detect semantic N+1 patterns in a single Rust source file.
#[cfg(any(test, feature = "analyzer"))]
pub(crate) fn detect_n_plus_one_in_file(file: &str, source: &str) -> Vec<NPlusOneDiagnostic> {
    let units = vec![SourceUnit {
        file: file.to_string(),
        source: source.to_string(),
        module_path: Vec::new(),
    }];
    let index = build_semantic_index(&units);
    detect_n_plus_one_in_source_with_index(file, source, &index)
}

fn detect_n_plus_one_in_source_with_index(
    file: &str,
    source: &str,
    index: &SemanticNPlusOneIndex,
) -> Vec<NPlusOneDiagnostic> {
    let lines: Vec<&str> = source.lines().collect();
    let mut out = Vec::new();
    let mut seen = HashSet::<(usize, usize, NPlusOneCode)>::new();

    let line_to_fn = index
        .line_to_function_by_file
        .get(file)
        .cloned()
        .unwrap_or_else(|| vec![None; lines.len()]);

    let mut loop_stack: Vec<LoopFrame> = Vec::new();
    let mut pending_loop_vars: Option<HashSet<String>> = None;
    let mut brace_depth: i32 = 0;

    for (idx, raw_line) in lines.iter().enumerate() {
        let line_no = idx + 1;
        let line = strip_line_comment(raw_line);
        let trimmed = line.trim();

        if let Some(vars) = pending_loop_vars.take() {
            if line.contains('{') {
                loop_stack.push(LoopFrame::new(brace_depth, vars));
            } else {
                pending_loop_vars = Some(vars);
            }
        }

        if let Some(work_loop_vars) = parse_work_loop_vars(trimmed) {
            if line.contains('{') {
                loop_stack.push(LoopFrame::new(brace_depth, work_loop_vars));
            } else {
                pending_loop_vars = Some(work_loop_vars);
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

            if let Some(caller_idx) = line_to_fn.get(idx).and_then(|v| *v)
                && let Some(caller) = index.functions.get(caller_idx)
            {
                for call in collect_function_calls(line) {
                    let resolved = resolve_function_call_targets(caller, &call, index);
                    if resolved
                        .iter()
                        .any(|&target_idx| index.query_executing_functions[target_idx])
                    {
                        emit_indirect_query_loop_diag(
                            &mut out,
                            &mut seen,
                            file,
                            line_no,
                            call.column,
                        );
                    }
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

    let units = files
        .iter()
        .filter_map(|path| {
            let source = std::fs::read_to_string(path).ok()?;
            Some(SourceUnit {
                file: path.display().to_string(),
                source,
                module_path: module_prefix_for_file(dir, path),
            })
        })
        .collect::<Vec<_>>();

    if units.is_empty() {
        return Vec::new();
    }

    let index = build_semantic_index(&units);
    let mut out = Vec::new();
    for unit in &units {
        out.extend(detect_n_plus_one_in_source_with_index(
            &unit.file,
            &unit.source,
            &index,
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

fn emit_indirect_query_loop_diag(
    out: &mut Vec<NPlusOneDiagnostic>,
    seen: &mut HashSet<(usize, usize, NPlusOneCode)>,
    file: &str,
    line: usize,
    column: usize,
) {
    if !seen.insert((line, column, NPlusOneCode::N1003)) {
        return;
    }

    out.push(NPlusOneDiagnostic {
        code: NPlusOneCode::N1003,
        severity: NPlusOneSeverity::Warning,
        file: file.to_string(),
        line,
        column,
        message: "Function/method that executes queries is called inside loop".to_string(),
        hint: Some("Batch outside the loop or pass pre-fetched data into the helper".to_string()),
    });
}

fn build_semantic_index(units: &[SourceUnit]) -> SemanticNPlusOneIndex {
    let mut index = SemanticNPlusOneIndex::default();

    for unit in units {
        index.functions.extend(extract_functions_from_source(unit));
        index
            .line_to_function_by_file
            .insert(unit.file.clone(), vec![None; unit.source.lines().count()]);
    }

    for (idx, func) in index.functions.iter().enumerate() {
        if let Some(impl_type) = &func.impl_type {
            let exact_key = method_module_impl_name_key(&func.module_path, impl_type, &func.name);
            index
                .method_by_module_impl_and_name
                .entry(exact_key)
                .or_default()
                .push(idx);
        } else {
            let module_name_key = module_name_key(&func.module_path, &func.name);
            index
                .free_by_module_and_name
                .entry(module_name_key)
                .or_default()
                .push(idx);

            let qualified_key = qualified_path_key(&func.module_path, &func.name);
            index
                .free_by_qualified_path
                .entry(qualified_key)
                .or_default()
                .push(idx);

            index
                .free_by_short_name
                .entry(func.name.clone())
                .or_default()
                .push(idx);
        }

        if let Some(line_map) = index.line_to_function_by_file.get_mut(&func.file) {
            let start = func.start_line.saturating_sub(1).min(line_map.len());
            let end = func.end_line.min(line_map.len());
            for slot in &mut line_map[start..end] {
                if slot.is_none() {
                    *slot = Some(idx);
                }
            }
        }
    }

    let mut query_exec = index
        .functions
        .iter()
        .map(|f| f.direct_query_exec)
        .collect::<Vec<_>>();

    let mut changed = true;
    while changed {
        changed = false;
        for idx in 0..index.functions.len() {
            if query_exec[idx] {
                continue;
            }
            let caller = &index.functions[idx];
            if caller.calls.iter().any(|call| {
                resolve_function_call_targets(caller, call, &index)
                    .iter()
                    .any(|target| query_exec[*target])
            }) {
                query_exec[idx] = true;
                changed = true;
            }
        }
    }

    index.query_executing_functions = query_exec;
    index
}

fn extract_functions_from_source(unit: &SourceUnit) -> Vec<FunctionSymbol> {
    let lines = unit.source.lines().collect::<Vec<_>>();
    let mut functions = Vec::new();

    let mut brace_depth = 0i32;
    let mut impl_stack: Vec<(String, i32)> = Vec::new();
    let mut pending_impl_type: Option<String> = None;
    let mut pending_function: Option<PendingFunction> = None;
    let mut active_function: Option<ActiveFunction> = None;

    for (idx, raw_line) in lines.iter().enumerate() {
        let line_no = idx + 1;
        let line = strip_line_comment(raw_line);
        let trimmed = line.trim();

        if let Some(impl_type) = pending_impl_type.take() {
            if line.contains('{') {
                impl_stack.push((impl_type, brace_depth));
            } else {
                pending_impl_type = Some(impl_type);
            }
        }

        if let Some(pending) = pending_function.take() {
            if line.contains('{') {
                active_function = Some(ActiveFunction {
                    exit_depth: brace_depth,
                    symbol: pending.symbol,
                });
            } else {
                pending_function = Some(pending);
            }
        }

        if active_function.is_none() {
            if let Some(impl_type) = parse_impl_type(trimmed) {
                if line.contains('{') {
                    impl_stack.push((impl_type, brace_depth));
                } else {
                    pending_impl_type = Some(impl_type);
                }
            }

            if let Some(fn_name) = parse_function_name(trimmed) {
                let symbol = FunctionSymbol {
                    file: unit.file.clone(),
                    module_path: unit.module_path.clone(),
                    name: fn_name,
                    impl_type: impl_stack.last().map(|(name, _)| name.clone()),
                    start_line: line_no,
                    end_line: line_no,
                    direct_query_exec: false,
                    calls: Vec::new(),
                };
                if line.contains('{') {
                    active_function = Some(ActiveFunction {
                        exit_depth: brace_depth,
                        symbol,
                    });
                } else {
                    pending_function = Some(PendingFunction { symbol });
                }
            }
        }

        if let Some(active) = active_function.as_mut() {
            active.symbol.end_line = line_no;
            // Ignore signature line call-like tokens to avoid false call edges.
            if line_no != active.symbol.start_line {
                if find_exec_call(line).is_some() {
                    active.symbol.direct_query_exec = true;
                }
                active.symbol.calls.extend(collect_function_calls(line));
            }
        }

        brace_depth += brace_delta(line);

        while let Some((_, exit_depth)) = impl_stack.last() {
            if brace_depth <= *exit_depth {
                impl_stack.pop();
            } else {
                break;
            }
        }

        if let Some(active) = active_function.take() {
            if brace_depth <= active.exit_depth {
                functions.push(active.symbol);
            } else {
                active_function = Some(active);
            }
        }
    }

    if let Some(active) = active_function {
        functions.push(active.symbol);
    }

    functions
}

fn parse_function_name(trimmed: &str) -> Option<String> {
    let mut rest = trimmed;
    if rest.starts_with("#[") {
        return None;
    }

    // Strip visibility and common qualifiers.
    for _ in 0..8 {
        let mut advanced = false;
        for prefix in [
            "pub(crate) ",
            "pub(super) ",
            "pub ",
            "async ",
            "const ",
            "unsafe ",
        ] {
            if let Some(next) = rest.strip_prefix(prefix) {
                rest = next.trim_start();
                advanced = true;
            }
        }
        if !advanced {
            break;
        }
    }

    let rest = rest.strip_prefix("fn ")?;
    let name = rest
        .chars()
        .take_while(|c| is_ident_char(*c))
        .collect::<String>();
    if name.is_empty() { None } else { Some(name) }
}

fn parse_impl_type(trimmed: &str) -> Option<String> {
    let rest = trimmed.strip_prefix("impl ")?;
    let header = rest.split('{').next().unwrap_or(rest).trim();
    if header.is_empty() {
        return None;
    }

    let header = trim_leading_generic_params(header).trim_start();
    if header.contains(" for ") {
        // Trait impl blocks do not resolve self-method helpers the same way for this heuristic.
        return None;
    }

    let impl_type = header
        .chars()
        .take_while(|c| is_ident_char(*c))
        .collect::<String>();
    if impl_type.is_empty() {
        None
    } else {
        Some(impl_type)
    }
}

fn trim_leading_generic_params(header: &str) -> &str {
    if !header.starts_with('<') {
        return header;
    }

    let mut depth = 0i32;
    for (idx, ch) in header.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => {
                depth -= 1;
                if depth == 0 {
                    return header[idx + 1..].trim_start();
                }
            }
            _ => {}
        }
    }
    header
}

fn resolve_function_call_targets(
    caller: &FunctionSymbol,
    call: &FunctionCallSite,
    index: &SemanticNPlusOneIndex,
) -> Vec<usize> {
    match &call.kind {
        FunctionCallKind::SelfMethod(name) => {
            let Some(impl_type) = &caller.impl_type else {
                return Vec::new();
            };
            let key = method_module_impl_name_key(&caller.module_path, impl_type, name);
            index
                .method_by_module_impl_and_name
                .get(&key)
                .cloned()
                .unwrap_or_default()
        }
        FunctionCallKind::Qualified { path, name } => {
            let resolved_path = resolve_relative_module_path(path, &caller.module_path);
            let key = qualified_path_key(&resolved_path, name);
            index
                .free_by_qualified_path
                .get(&key)
                .cloned()
                .unwrap_or_default()
        }
        FunctionCallKind::Bare(name) => {
            let module_key = module_name_key(&caller.module_path, name);
            if let Some(candidates) = index.free_by_module_and_name.get(&module_key)
                && candidates.len() == 1
            {
                return candidates.clone();
            }

            match index.free_by_short_name.get(name) {
                Some(candidates) if candidates.len() == 1 => candidates.clone(),
                _ => Vec::new(),
            }
        }
    }
}

fn collect_function_calls(line: &str) -> Vec<FunctionCallSite> {
    let trimmed = line.trim_start();
    if trimmed.starts_with("fn ")
        || trimmed.starts_with("pub fn ")
        || trimmed.starts_with("pub(crate) fn ")
        || trimmed.starts_with("pub(super) fn ")
        || trimmed.starts_with("async fn ")
        || trimmed.starts_with("pub async fn ")
        || trimmed.starts_with("pub(crate) async fn ")
        || trimmed.starts_with("pub(super) async fn ")
    {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut in_string = false;
    let mut prev = '\0';
    for (idx, ch) in line.char_indices() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
            prev = ch;
            continue;
        }
        if in_string || ch != '(' {
            prev = ch;
            continue;
        }

        if let Some((token, column)) = call_token_before_open_paren(line, idx) {
            let token = strip_generic_arguments(&token);
            if token.ends_with('!') {
                prev = ch;
                continue;
            }

            if token.contains('.') {
                let mut parts = token.split('.');
                let receiver = parts.next().unwrap_or_default().trim();
                let method = parts.next_back().unwrap_or_default().trim();
                if receiver == "self" && is_plain_ident(method) && !is_rust_keyword(method) {
                    out.push(FunctionCallSite {
                        column,
                        kind: FunctionCallKind::SelfMethod(method.to_string()),
                    });
                }
                prev = ch;
                continue;
            }

            if token.contains("::") {
                let segments = token
                    .split("::")
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>();
                if segments.len() >= 2 {
                    let name = segments[segments.len() - 1];
                    let path = segments[..segments.len() - 1]
                        .iter()
                        .map(|s| (*s).to_string())
                        .collect::<Vec<_>>();
                    if is_plain_ident(name) && !is_rust_keyword(name) {
                        out.push(FunctionCallSite {
                            column,
                            kind: FunctionCallKind::Qualified {
                                path,
                                name: name.to_string(),
                            },
                        });
                    }
                }
                prev = ch;
                continue;
            }

            if is_plain_ident(&token) && !is_rust_keyword(&token) {
                out.push(FunctionCallSite {
                    column,
                    kind: FunctionCallKind::Bare(token),
                });
            }
        }

        prev = ch;
    }
    out
}

fn call_token_before_open_paren(line: &str, open_paren_idx: usize) -> Option<(String, usize)> {
    let bytes = line.as_bytes();
    if open_paren_idx == 0 || open_paren_idx > bytes.len() {
        return None;
    }

    let mut end = open_paren_idx;
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    if end == 0 {
        return None;
    }

    let mut start = end;
    while start > 0 {
        let b = bytes[start - 1];
        let is_token = b.is_ascii_alphanumeric()
            || b == b'_'
            || b == b':'
            || b == b'.'
            || b == b'!'
            || b == b'<'
            || b == b'>';
        if !is_token {
            break;
        }
        start -= 1;
    }

    let token = line[start..end].trim().to_string();
    if token.is_empty() {
        None
    } else {
        Some((token, start + 1))
    }
}

fn strip_generic_arguments(token: &str) -> String {
    let mut out = String::new();
    let mut depth = 0i32;
    for ch in token.chars() {
        match ch {
            '<' => depth += 1,
            '>' => {
                if depth > 0 {
                    depth -= 1;
                } else {
                    out.push(ch);
                }
            }
            _ => {
                if depth == 0 {
                    out.push(ch);
                }
            }
        }
    }
    out
}

fn is_plain_ident(name: &str) -> bool {
    !name.is_empty() && name.chars().all(is_ident_char)
}

fn is_rust_keyword(name: &str) -> bool {
    matches!(
        name,
        "if" | "for"
            | "while"
            | "loop"
            | "match"
            | "return"
            | "let"
            | "fn"
            | "impl"
            | "async"
            | "await"
            | "move"
            | "in"
            | "where"
            | "else"
            | "mod"
            | "struct"
            | "enum"
            | "trait"
            | "use"
            | "pub"
            | "super"
            | "self"
            | "crate"
    )
}

fn resolve_relative_module_path(path: &[String], caller_module_path: &[String]) -> Vec<String> {
    if path.is_empty() {
        return Vec::new();
    }

    match path[0].as_str() {
        "crate" => path[1..].to_vec(),
        "self" => caller_module_path
            .iter()
            .cloned()
            .chain(path[1..].iter().cloned())
            .collect(),
        "super" => {
            let mut module = caller_module_path.to_vec();
            if !module.is_empty() {
                module.pop();
            }
            module.extend(path[1..].iter().cloned());
            module
        }
        _ => path.to_vec(),
    }
}

fn module_name_key(module_path: &[String], name: &str) -> String {
    if module_path.is_empty() {
        name.to_string()
    } else {
        format!("{}::{}", module_path.join("::"), name)
    }
}

fn qualified_path_key(path: &[String], name: &str) -> String {
    if path.is_empty() {
        name.to_string()
    } else {
        format!("{}::{}", path.join("::"), name)
    }
}

fn method_module_impl_name_key(module_path: &[String], impl_type: &str, name: &str) -> String {
    if module_path.is_empty() {
        format!("{impl_type}::{name}")
    } else {
        format!("{}::{impl_type}::{name}", module_path.join("::"))
    }
}

fn parse_work_loop_vars(trimmed_line: &str) -> Option<HashSet<String>> {
    parse_for_loop_vars(trimmed_line)
        .or_else(|| parse_while_loop_vars(trimmed_line))
        .or_else(|| parse_loop_block_vars(trimmed_line))
        .or_else(|| parse_iterator_loop_vars(trimmed_line))
}

fn parse_for_loop_vars(trimmed_line: &str) -> Option<HashSet<String>> {
    let rest = strip_loop_label(trimmed_line).strip_prefix("for ")?;
    let in_pos = rest.find(" in ")?;
    let pattern = rest[..in_pos].trim();
    let mut out = HashSet::new();
    collect_loop_idents(pattern, &mut out);
    Some(out)
}

fn parse_while_loop_vars(trimmed_line: &str) -> Option<HashSet<String>> {
    let rest = strip_loop_label(trimmed_line).strip_prefix("while ")?;
    let condition = rest.split('{').next().unwrap_or(rest).trim();
    let mut out = HashSet::new();

    if let Some(after_let) = condition.strip_prefix("let ")
        && let Some(eq_pos) = after_let.find('=')
    {
        collect_loop_idents(after_let[..eq_pos].trim(), &mut out);
    } else {
        collect_loop_idents(condition, &mut out);
    }

    Some(out)
}

fn parse_loop_block_vars(trimmed_line: &str) -> Option<HashSet<String>> {
    let line = strip_loop_label(trimmed_line);
    if !line.starts_with("loop") {
        return None;
    }
    let suffix = line["loop".len()..].trim_start();
    if suffix.is_empty() || suffix.starts_with('{') {
        Some(HashSet::new())
    } else {
        None
    }
}

fn parse_iterator_loop_vars(trimmed_line: &str) -> Option<HashSet<String>> {
    let line = strip_loop_label(trimmed_line);
    if !ITER_LOOP_PATTERNS.iter().any(|pat| line.contains(pat)) {
        return None;
    }

    let params = extract_closure_params(line)?;
    let mut out = HashSet::new();
    collect_closure_param_idents(params, &mut out);
    Some(out)
}

fn strip_loop_label(trimmed_line: &str) -> &str {
    let line = trimmed_line.trim_start();
    let Some(rest) = line.strip_prefix('\'') else {
        return line;
    };
    let Some(colon_idx) = rest.find(':') else {
        return line;
    };
    let label = rest[..colon_idx].trim();
    if label.is_empty() || !label.chars().all(is_ident_char) {
        return line;
    }
    rest[colon_idx + 1..].trim_start()
}

fn collect_loop_idents(text: &str, out: &mut HashSet<String>) {
    for ident in extract_idents(text) {
        insert_loop_ident(out, ident);
    }
}

fn insert_loop_ident(out: &mut HashSet<String>, ident: String) {
    if ident == "_" || ident == "mut" || ident == "ref" {
        return;
    }

    let starts_with_lower = ident
        .chars()
        .next()
        .map(|c| c.is_ascii_lowercase() || c == '_')
        .unwrap_or(false);
    if starts_with_lower && !is_rust_keyword(&ident) {
        out.insert(ident);
    }
}

fn extract_closure_params(line: &str) -> Option<&str> {
    let mut in_string = false;
    let mut prev = '\0';
    let mut start: Option<usize> = None;

    for (idx, ch) in line.char_indices() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
            prev = ch;
            continue;
        }

        if !in_string && ch == '|' {
            if let Some(start_idx) = start {
                return line.get(start_idx..idx).map(str::trim);
            }
            start = Some(idx + 1);
        }
        prev = ch;
    }

    None
}

fn collect_closure_param_idents(params: &str, out: &mut HashSet<String>) {
    let params = params.trim();
    if params.is_empty() {
        return;
    }

    for part in split_top_level(params, ',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let part = if let Some(colon_idx) = find_top_level_char(part, ':') {
            part[..colon_idx].trim()
        } else {
            part
        };

        let part = part
            .strip_prefix("&mut ")
            .or_else(|| part.strip_prefix('&'))
            .unwrap_or(part)
            .trim_start();
        let part = part
            .strip_prefix("mut ")
            .or_else(|| part.strip_prefix("ref "))
            .unwrap_or(part)
            .trim();

        collect_loop_idents(part, out);
    }
}

fn split_top_level(text: &str, delim: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut paren = 0i32;
    let mut bracket = 0i32;
    let mut brace = 0i32;
    let mut angle = 0i32;
    let mut in_string = false;
    let mut prev = '\0';

    for (idx, ch) in text.char_indices() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
            prev = ch;
            continue;
        }

        if in_string {
            prev = ch;
            continue;
        }

        match ch {
            '(' => paren += 1,
            ')' => paren -= 1,
            '[' => bracket += 1,
            ']' => bracket -= 1,
            '{' => brace += 1,
            '}' => brace -= 1,
            '<' => angle += 1,
            '>' => angle -= 1,
            _ => {}
        }

        if ch == delim && paren == 0 && bracket == 0 && brace == 0 && angle == 0 {
            parts.push(&text[start..idx]);
            start = idx + ch.len_utf8();
        }
        prev = ch;
    }

    parts.push(&text[start..]);
    parts
}

fn find_top_level_char(text: &str, target: char) -> Option<usize> {
    let mut paren = 0i32;
    let mut bracket = 0i32;
    let mut brace = 0i32;
    let mut angle = 0i32;
    let mut in_string = false;
    let mut prev = '\0';

    for (idx, ch) in text.char_indices() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
            prev = ch;
            continue;
        }

        if in_string {
            prev = ch;
            continue;
        }

        match ch {
            '(' => paren += 1,
            ')' => paren -= 1,
            '[' => bracket += 1,
            ']' => bracket -= 1,
            '{' => brace += 1,
            '}' => brace -= 1,
            '<' => angle += 1,
            '>' => angle -= 1,
            _ => {}
        }

        if ch == target && paren == 0 && bracket == 0 && brace == 0 && angle == 0 {
            return Some(idx);
        }
        prev = ch;
    }

    None
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

fn module_prefix_for_file(root: &Path, file: &Path) -> Vec<String> {
    let Ok(rel) = file.strip_prefix(root) else {
        return Vec::new();
    };

    let mut segs: Vec<String> = rel
        .parent()
        .into_iter()
        .flat_map(|p| p.components())
        .filter_map(|c| c.as_os_str().to_str())
        .map(ToOwned::to_owned)
        .collect();

    let file_stem = rel.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
    let file_name = rel.file_name().and_then(|s| s.to_str()).unwrap_or_default();

    if file_name != "mod.rs" && file_stem != "lib" && file_stem != "main" && !file_stem.is_empty() {
        segs.push(file_stem.to_string());
    }
    segs
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

    #[test]
    fn detects_while_loop_query_execution() {
        let source = r#"
async fn demo(mut ids: Vec<i64>, conn: &Conn) {
    while let Some(id) = ids.pop() {
        let cmd = Qail::get("users").eq("id", id);
        let _ = conn.fetch_one(&cmd).await;
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
    fn detects_loop_block_query_execution() {
        let source = r#"
async fn demo(conn: &Conn, ids: Vec<i64>) {
    let mut idx = 0usize;
    loop {
        if idx >= ids.len() {
            break;
        }
        let cmd = Qail::get("users").eq("id", ids[idx] as i64);
        let _ = conn.fetch_all(&cmd).await;
        idx += 1;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1001),
            "{diags:?}"
        );
    }

    #[test]
    fn detects_for_each_loop_query_execution() {
        let source = r#"
fn demo(conn: &Conn, ids: Vec<i64>) {
    ids.iter().for_each(|id| {
        let cmd = Qail::get("users").eq("id", *id);
        let _ = conn.fetch_all(&cmd);
    });
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "{diags:?}"
        );
    }

    #[test]
    fn detects_nested_for_each_inside_for_as_error() {
        let source = r#"
fn demo(conn: &Conn, tenants: Vec<i64>, ids: Vec<i64>) {
    for tenant in tenants {
        ids.iter().for_each(|id| {
            let cmd = Qail::get("users")
                .eq("tenant_id", tenant)
                .eq("id", *id);
            let _ = conn.fetch_all(&cmd);
        });
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
    fn detects_indirect_query_function_call_in_loop() {
        let source = r#"
async fn load_user(conn: &Conn, id: i64) {
    let cmd = Qail::get("users").eq("id", id);
    let _ = conn.fetch_one(&cmd).await;
}

async fn process(conn: &Conn, ids: Vec<i64>) {
    for id in ids {
        load_user(conn, id).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1003),
            "{diags:?}"
        );
    }

    #[test]
    fn detects_self_method_query_call_in_loop() {
        let source = r#"
struct Repo;

impl Repo {
    async fn load_user(&self, conn: &Conn, id: i64) {
        let cmd = Qail::get("users").eq("id", id);
        let _ = conn.fetch_one(&cmd).await;
    }

    async fn process(&self, conn: &Conn, ids: Vec<i64>) {
        for id in ids {
            self.load_user(conn, id).await;
        }
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1003),
            "{diags:?}"
        );
    }

    #[test]
    fn does_not_flag_ambiguous_short_name_resolution() {
        let source = r#"
mod helpers {
    pub async fn new(conn: &Conn) {
        let _ = conn.fetch_one(&Qail::get("users")).await;
    }
}

pub async fn new(_conn: &Conn) {}

async fn process(conn: &Conn, ids: Vec<i64>) {
    for _id in ids {
        new(conn).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            !diags.iter().any(|d| d.code == NPlusOneCode::N1003),
            "{diags:?}"
        );
    }

    #[test]
    fn detects_cross_file_indirect_query_call_in_loop() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "qail_semantic_nplus1_cross_file_{}_{}",
            std::process::id(),
            unique
        ));
        std::fs::create_dir_all(&root).unwrap();

        let helpers = r#"
pub async fn load_user(conn: &Conn, id: i64) {
    let cmd = Qail::get("users").eq("id", id);
    let _ = conn.fetch_one(&cmd).await;
}
"#;
        let main = r#"
mod helpers;

async fn process(conn: &Conn, ids: Vec<i64>) {
    for id in ids {
        helpers::load_user(conn, id).await;
    }
}
"#;

        std::fs::write(root.join("helpers.rs"), helpers).unwrap();
        std::fs::write(root.join("main.rs"), main).unwrap();

        let diags = detect_n_plus_one_in_dir(&root);
        let _ = std::fs::remove_dir_all(&root);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1003),
            "{diags:?}"
        );
    }
}
