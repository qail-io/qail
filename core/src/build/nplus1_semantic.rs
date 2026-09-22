use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::rust_lex::{
    consume_block_comment, consume_rust_literal, mask_non_code, starts_with_bytes,
};

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
    shape_fingerprint: Option<String>,
    prebuilt_command: bool,
}

#[derive(Debug, Clone)]
struct QueryShape {
    fingerprint: String,
    uses_loop_var: bool,
    batched: bool,
}

#[derive(Debug)]
struct LoopFrame {
    exit_depth: i32,
    loop_vars: HashSet<String>,
    query_bindings: HashMap<String, QueryBinding>,
    batched_plan_bindings: HashSet<String>,
    has_scheduler_pacing: bool,
}

impl LoopFrame {
    fn new(exit_depth: i32, loop_vars: HashSet<String>) -> Self {
        Self {
            exit_depth,
            loop_vars,
            query_bindings: HashMap::new(),
            batched_plan_bindings: HashSet::new(),
            has_scheduler_pacing: false,
        }
    }
}

/// A loop scope inside one line: an iterator closure with an expression body,
/// between its parameters and the end of its call.
#[derive(Debug)]
struct InlineLoop {
    /// Byte just past the closure's closing `|`.
    start: usize,
    /// Byte of the call's closing `)`, or the end of the line.
    end: usize,
    vars: HashSet<String>,
}

impl InlineLoop {
    fn contains(&self, pos: usize) -> bool {
        pos >= self.start && pos < self.end
    }
}

/// A closure written as an argument.
struct ClosureArg<'a> {
    params: &'a str,
    /// Byte just past the closing `|`.
    body_start: usize,
    /// The body is a block (`|id| {`, `|id| -> T {`).
    block: bool,
}

#[derive(Debug)]
struct PendingLoop {
    vars: HashSet<String>,
    /// Set when an expression-bodied iterator closure is waiting for a block
    /// on a later line: the brace depth of the block it sits in. The wait
    /// ends with its statement or that block, never at an unrelated `{`.
    closure_scope: Option<i32>,
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

const EXEC_METHODS: &[&str] = &[
    "fetch_all_cached",
    "fetch_all_cached_with_format",
    "fetch_all_with_rls",
    "fetch_all_uncached",
    "fetch_all_fast",
    "fetch_all",
    "fetch_one",
    "fetch_one_typed",
    "fetch_one_typed_with_format",
    "fetch_typed",
    "fetch_typed_with_format",
    "fetch_opt",
    "execute",
    "query",
    "query_ast",
    "query_ast_with_format",
    "scroll",
    "search",
    "search_ast",
];

const ITER_LOOP_PATTERNS: [&str; 4] = [
    ".for_each(",
    ".try_for_each(",
    ".for_each_concurrent(",
    ".try_for_each_concurrent(",
];

const ITER_MAP_LOOP_PATTERNS: [&str; 3] = [".iter().map(", ".iter_mut().map(", ".into_iter().map("];

/// Methods that start an iterator chain.
const ITER_SOURCES: [&str; 3] = ["iter", "iter_mut", "into_iter"];

/// Chain adaptors whose closure runs once per item.
const ITER_MAP_ADAPTORS: [&str; 3] = ["map", "filter_map", "flat_map"];

/// Methods whose result is no longer an iterator: a `.map(` after one maps an
/// `Option`, a collection or a value, not the items.
const ITER_TERMINALS: [&str; 33] = [
    "all",
    "any",
    "collect",
    "collect_vec",
    "count",
    "find",
    "find_map",
    "fold",
    "for_each",
    "is_empty",
    "join",
    "last",
    "len",
    "max",
    "max_by",
    "max_by_key",
    "min",
    "min_by",
    "min_by_key",
    "next",
    "next_back",
    "nth",
    "nth_back",
    "partition",
    "peek",
    "position",
    "product",
    "reduce",
    "rposition",
    "sum",
    "try_fold",
    "try_for_each",
    "unzip",
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
    let masked_source = mask_non_code(source);
    let lines: Vec<&str> = source.lines().collect();
    let code_lines: Vec<&str> = masked_source.lines().collect();
    let mut out = Vec::new();
    let mut seen = HashSet::<(usize, usize, NPlusOneCode)>::new();

    let line_to_fn = index
        .line_to_function_by_file
        .get(file)
        .cloned()
        .unwrap_or_else(|| vec![None; lines.len()]);

    let mut loop_stack: Vec<LoopFrame> = Vec::new();
    let mut pending_loop: Option<PendingLoop> = None;
    // An iterator loop call whose closure is on a later line: the brace depth
    // of the block the call sits in.
    let mut pending_iterator_scope: Option<i32> = None;
    // Per brace depth: the expression open there is an iterator chain.
    let mut iter_chains: BTreeMap<i32, bool> = BTreeMap::new();
    let mut brace_depth: i32 = 0;

    for idx in 0..lines.len() {
        let code_line = code_lines.get(idx).copied().unwrap_or_default();
        let line_no = idx + 1;
        let trimmed = code_line.trim();
        let chain_opens = scan_iterator_chains(code_line, brace_depth, &mut iter_chains);
        let mut inline_loops: Vec<InlineLoop> = Vec::new();

        if let Some(pending) = pending_loop.take() {
            if code_line.contains('{') {
                let has_scheduler_pacing =
                    loop_block_has_scheduler_pacing(&code_lines, idx, brace_depth);
                let mut frame = LoopFrame::new(brace_depth, pending.vars);
                frame.has_scheduler_pacing = has_scheduler_pacing;
                loop_stack.push(frame);
            } else if pending.closure_scope.is_some() && line_ends_statement(code_line) {
                // The expression closure's statement ended with no block: there
                // is no loop body left to find.
            } else {
                pending_loop = Some(pending);
            }
        }

        if pending_iterator_scope.is_some() {
            if let Some(params) = extract_closure_params(trimmed) {
                let mut vars = HashSet::new();
                collect_closure_param_idents(params, &mut vars);
                pending_iterator_scope = None;
                if code_line.contains('{') {
                    let has_scheduler_pacing =
                        loop_block_has_scheduler_pacing(&code_lines, idx, brace_depth);
                    let mut frame = LoopFrame::new(brace_depth, vars);
                    frame.has_scheduler_pacing = has_scheduler_pacing;
                    loop_stack.push(frame);
                } else {
                    // The expression body on this line runs per item.
                    inline_loops.extend(leading_inline_loop(code_line));
                    if !line_ends_statement(code_line) {
                        // Still open: a block on a later line of the same
                        // statement is the loop body.
                        pending_loop = Some(PendingLoop {
                            vars,
                            closure_scope: Some(brace_depth),
                        });
                    }
                }
            } else if trimmed.contains(';') {
                pending_iterator_scope = None;
            }
        }

        if let Some(work_loop_vars) = parse_work_loop_vars(trimmed) {
            let is_inline_iterator_closure =
                starts_iterator_loop(trimmed) && extract_closure_params(trimmed).is_some();
            if code_line.contains('{') {
                let has_scheduler_pacing =
                    loop_block_has_scheduler_pacing(&code_lines, idx, brace_depth);
                let mut frame = LoopFrame::new(brace_depth, work_loop_vars);
                frame.has_scheduler_pacing = has_scheduler_pacing;
                loop_stack.push(frame);
            } else if is_inline_iterator_closure {
                // No block scope: a pending frame would adopt the next
                // unrelated `{`. The closure body is a loop scope bounded by
                // its call on this line instead.
                inline_loops =
                    inline_loops_for(code_line, &iterator_call_opens(code_line, &chain_opens));
            } else {
                pending_loop = Some(PendingLoop {
                    vars: work_loop_vars,
                    closure_scope: None,
                });
            }
        } else if !chain_opens.is_empty() {
            // A map on an iterator chain the fixed patterns cannot see: split
            // over lines, or past other adaptors.
            let mut block_vars: Option<HashSet<String>> = None;
            for &open in &chain_opens {
                let unclosed = find_matching_paren_at(code_line, open).is_none();
                match closure_at(code_line, open + 1) {
                    Some(closure) if closure.block => {
                        block_vars.get_or_insert_with(|| {
                            let mut vars = HashSet::new();
                            collect_closure_param_idents(closure.params, &mut vars);
                            vars
                        });
                    }
                    Some(closure) => {
                        if unclosed {
                            let mut vars = HashSet::new();
                            collect_closure_param_idents(closure.params, &mut vars);
                            pending_loop = Some(PendingLoop {
                                vars,
                                closure_scope: Some(brace_depth),
                            });
                        }
                        inline_loops.extend(inline_loops_for(code_line, &[open]));
                    }
                    None if unclosed => pending_iterator_scope = Some(brace_depth),
                    None => {}
                }
            }
            if let Some(vars) = block_vars {
                let has_scheduler_pacing =
                    loop_block_has_scheduler_pacing(&code_lines, idx, brace_depth);
                let mut frame = LoopFrame::new(brace_depth, vars);
                frame.has_scheduler_pacing = has_scheduler_pacing;
                loop_stack.push(frame);
            }
        } else if starts_iterator_loop(trimmed)
            && extract_closure_params(trimmed).is_none()
            && !trimmed.contains(';')
        {
            pending_iterator_scope = Some(brace_depth);
        }

        let work_depth = loop_stack.len();
        if work_depth > 0 {
            let loop_vars = active_loop_vars(&loop_stack);
            let scheduler_loop_context = work_depth == 1
                && loop_stack
                    .last()
                    .is_some_and(|f| f.has_scheduler_pacing && f.loop_vars.is_empty());

            if let Some((var_name, qail_start_col, chain)) =
                extract_query_binding(&lines, &code_lines, idx)
            {
                let shape = parse_qail_chain_shape(&chain, &loop_vars);
                let uses_loop_var = shape
                    .as_ref()
                    .map(|s| s.uses_loop_var)
                    .unwrap_or_else(|| any_loop_var_in_text(&loop_vars, &chain));
                let batched = shape
                    .as_ref()
                    .map(|s| s.batched)
                    .unwrap_or_else(|| is_batched_expr(&chain));

                if let Some(frame) = loop_stack.last_mut() {
                    frame.query_bindings.insert(
                        var_name,
                        QueryBinding {
                            uses_loop_var,
                            batched,
                            shape_fingerprint: shape.as_ref().map(|s| s.fingerprint.clone()),
                            prebuilt_command: false,
                        },
                    );
                }

                // Inline execute in builder chain inside loop.
                if let Some(exec) = find_exec_call(&chain)
                    && !batched
                    && (!scheduler_loop_context || uses_loop_var)
                {
                    emit_query_loop_diag(
                        &mut out,
                        &mut seen,
                        file,
                        line_no,
                        qail_start_col + exec.column_offset.saturating_sub(1),
                        work_depth,
                        uses_loop_var,
                    );
                }
            }

            if let Some((var_name, binding)) =
                extract_prebuilt_command_binding(code_line, &loop_vars)
                && let Some(frame) = loop_stack.last_mut()
            {
                frame.query_bindings.insert(var_name, binding);
            }

            if let Some(plan_name) = extract_nested_batch_plan_binding(code_line)
                && let Some(frame) = loop_stack.last_mut()
            {
                frame.batched_plan_bindings.insert(plan_name);
            }

            if let Some((var_name, binding)) =
                extract_batched_plan_command_binding(code_line, &loop_stack)
                && let Some(frame) = loop_stack.last_mut()
            {
                frame.query_bindings.insert(var_name, binding);
            }

            // A call inside a one-line iterator closure is judged below, one
            // loop deeper.
            if let Some(exec) = find_exec_call_spanning(&lines, idx)
                && !inline_loops.iter().any(|l| l.contains(exec.column - 1))
                && let Some(uses_loop_var) = unbatched_exec(&exec, &loop_stack, &loop_vars)
                && (!scheduler_loop_context || uses_loop_var)
            {
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

            if let Some(caller_idx) = line_to_fn.get(idx).and_then(|v| *v)
                && let Some(caller) = index.functions.get(caller_idx)
            {
                for call in collect_function_calls(code_line) {
                    if inline_loops.iter().any(|l| l.contains(call.column - 1)) {
                        continue;
                    }
                    let resolved = resolve_function_call_targets(caller, &call, index);
                    if !scheduler_loop_context
                        && resolved
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

        // One-line iterator closures: a loop one deeper than the frames
        // around them, never paced.
        if !inline_loops.is_empty() {
            let depth = work_depth + 1;
            let raw = lines[idx];
            let calls = collect_function_calls(code_line);
            let caller = line_to_fn
                .get(idx)
                .and_then(|v| *v)
                .and_then(|caller_idx| index.functions.get(caller_idx));
            for inline in &inline_loops {
                let mut vars = active_loop_vars(&loop_stack);
                vars.extend(inline.vars.iter().cloned());

                let mut from = inline.start;
                while let Some(exec) = raw.get(from..inline.end).and_then(find_exec_call) {
                    let column = from + exec.column;
                    if let Some(uses_loop_var) = unbatched_exec(&exec, &loop_stack, &vars) {
                        emit_query_loop_diag(
                            &mut out,
                            &mut seen,
                            file,
                            line_no,
                            column,
                            depth,
                            uses_loop_var,
                        );
                    }
                    from = column;
                }

                if let Some(caller) = caller {
                    for call in calls.iter().filter(|c| inline.contains(c.column - 1)) {
                        if resolve_function_call_targets(caller, call, index)
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
        }

        brace_depth += brace_delta(code_line);
        while let Some(frame) = loop_stack.last() {
            if brace_depth <= frame.exit_depth {
                loop_stack.pop();
            } else {
                break;
            }
        }
        iter_chains.retain(|&depth, _| depth <= brace_depth);

        // A pending iterator call or expression closure cannot outlive its
        // statement. When the block it sits in closes, the wait carries on
        // only if the next line continues the method chain (`}).map(|k| {`
        // split over lines); otherwise the next `{` — a later fn, even —
        // belongs to other code.
        if pending_iterator_scope.is_some_and(|scope| brace_depth < scope) {
            pending_iterator_scope =
                next_code_line_continues_chain(&code_lines, idx).then_some(brace_depth);
        }
        if let Some(pending) = pending_loop.as_mut()
            && pending
                .closure_scope
                .is_some_and(|scope| brace_depth < scope)
        {
            if next_code_line_continues_chain(&code_lines, idx) {
                pending.closure_scope = Some(brace_depth);
            } else {
                pending_loop = None;
            }
        }
    }

    out
}

/// An exec call in a loop is an N+1 unless its command is batched or a
/// prebuilt command replayed: `None` then, else whether it depends on a loop
/// variable.
fn unbatched_exec(
    exec: &ExecCall,
    loop_stack: &[LoopFrame],
    loop_vars: &HashSet<String>,
) -> Option<bool> {
    let arg_shape = parse_qail_chain_shape(&exec.first_arg, loop_vars);
    let matched_binding = find_binding_for_arg(loop_stack, &exec.first_arg).filter(|binding| {
        arg_shape
            .as_ref()
            .is_none_or(|shape| binding_matches_arg_shape(binding, shape))
    });

    let batched = matched_binding
        .as_ref()
        .map(|b| b.batched)
        .or_else(|| arg_shape.as_ref().map(|s| s.batched))
        .unwrap_or_else(|| is_batched_expr(&exec.first_arg));
    let prebuilt_command = matched_binding
        .as_ref()
        .is_some_and(|binding| binding.prebuilt_command)
        || is_loop_command_replay_arg(loop_vars, &exec.first_arg);
    if batched || prebuilt_command {
        return None;
    }

    Some(
        matched_binding
            .as_ref()
            .map(|b| b.uses_loop_var)
            .or_else(|| arg_shape.as_ref().map(|s| s.uses_loop_var))
            .unwrap_or_else(|| any_loop_var_in_text(loop_vars, &exec.first_arg)),
    )
}

/// The next code line after `idx` continues a method chain (`.map(`).
fn next_code_line_continues_chain(code_lines: &[&str], idx: usize) -> bool {
    code_lines
        .iter()
        .skip(idx + 1)
        .map(|line| line.trim())
        .find(|line| !line.is_empty())
        .is_some_and(|line| line.starts_with('.') && !line.starts_with(".."))
}

/// The line ends a statement: a `;` outside every bracket the line opens.
fn line_ends_statement(code_line: &str) -> bool {
    let mut depth = 0i32;
    for ch in code_line.chars() {
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            ';' if depth <= 0 => return true,
            _ => {}
        }
    }
    false
}

fn binding_matches_arg_shape(binding: &QueryBinding, arg_shape: &QueryShape) -> bool {
    binding
        .shape_fingerprint
        .as_ref()
        .is_none_or(|fp| fp == &arg_shape.fingerprint)
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
    let masked_source = mask_non_code(&unit.source);
    let lines = unit.source.lines().collect::<Vec<_>>();
    let code_lines = masked_source.lines().collect::<Vec<_>>();
    let mut functions = Vec::new();

    let mut brace_depth = 0i32;
    let mut impl_stack: Vec<(String, i32)> = Vec::new();
    let mut pending_impl_type: Option<String> = None;
    let mut pending_function: Option<PendingFunction> = None;
    let mut active_functions: Vec<ActiveFunction> = Vec::new();

    for (idx, raw_line) in lines.iter().enumerate() {
        let code_line = code_lines.get(idx).copied().unwrap_or_default();
        let line_no = idx + 1;
        let trimmed = code_line.trim();
        let raw_trimmed = raw_line.trim();

        if let Some(impl_type) = pending_impl_type.take() {
            if code_line.contains('{') {
                impl_stack.push((impl_type, brace_depth));
            } else {
                pending_impl_type = Some(impl_type);
            }
        }

        if let Some(pending) = pending_function.take() {
            if code_line.contains('{') {
                active_functions.push(ActiveFunction {
                    exit_depth: brace_depth,
                    symbol: pending.symbol,
                });
            } else {
                pending_function = Some(pending);
            }
        }

        if active_functions.is_empty()
            && !trimmed.is_empty()
            && let Some(impl_type) = parse_impl_type(raw_trimmed)
        {
            if code_line.contains('{') {
                impl_stack.push((impl_type, brace_depth));
            } else {
                pending_impl_type = Some(impl_type);
            }
        }

        if pending_function.is_none()
            && !trimmed.is_empty()
            && let Some(fn_name) = parse_function_name(raw_trimmed)
        {
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
            if code_line.contains('{') {
                active_functions.push(ActiveFunction {
                    exit_depth: brace_depth,
                    symbol,
                });
            } else {
                pending_function = Some(PendingFunction { symbol });
            }
        }

        for active in &mut active_functions {
            active.symbol.end_line = line_no;
        }

        if let Some(active) = active_functions.last_mut() {
            // Ignore signature line call-like tokens to avoid false call edges.
            if line_no != active.symbol.start_line {
                if find_exec_call(code_line).is_some() {
                    active.symbol.direct_query_exec = true;
                }
                active
                    .symbol
                    .calls
                    .extend(collect_function_calls(code_line));
            }
        }

        brace_depth += brace_delta(code_line);

        while let Some((_, exit_depth)) = impl_stack.last() {
            if brace_depth <= *exit_depth {
                impl_stack.pop();
            } else {
                break;
            }
        }

        while active_functions
            .last()
            .is_some_and(|active| brace_depth <= active.exit_depth)
        {
            if let Some(active) = active_functions.pop() {
                functions.push(active.symbol);
            }
        }
    }

    while let Some(active) = active_functions.pop() {
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
    let rest = trimmed.strip_prefix("impl")?;
    if !rest.is_empty() && !rest.starts_with(char::is_whitespace) && !rest.starts_with('<') {
        return None;
    }
    let rest = rest.trim_start();
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
    if is_function_signature_line(trimmed) {
        return Vec::new();
    }

    let mut out = Vec::new();
    let bytes = line.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if bytes[i] != b'(' {
            i += 1;
            continue;
        }

        if let Some((token, column)) = call_token_before_open_paren(line, i) {
            let token = strip_generic_arguments(&token);
            if token.ends_with('!') {
                i += 1;
                continue;
            }

            if token.contains('.') {
                let parts = token
                    .split('.')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>();
                if parts.len() == 2 {
                    let receiver = parts[0];
                    let method = parts[1];
                    if receiver == "self" && is_plain_ident(method) && !is_rust_keyword(method) {
                        out.push(FunctionCallSite {
                            column,
                            kind: FunctionCallKind::SelfMethod(method.to_string()),
                        });
                    }
                }
                i += 1;
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
                i += 1;
                continue;
            }

            if is_plain_ident(&token) && !is_rust_keyword(&token) {
                out.push(FunctionCallSite {
                    column,
                    kind: FunctionCallKind::Bare(token),
                });
            }
        }

        i += 1;
    }
    out
}

fn is_function_signature_line(trimmed: &str) -> bool {
    parse_function_name(trimmed).is_some()
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

/// Byte positions of timer calls the line awaits directly: `.tick().await`,
/// `sleep(..).await`, `sleep_until(..).await`. An `.await` elsewhere on the
/// line (a handler in a one-line `select!`) is not the timer's.
fn awaited_timer_positions(line: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for name in [".tick(", "sleep(", "sleep_until("] {
        for (pos, found) in line.match_indices(name) {
            let joined_to_ident = !name.starts_with('.')
                && line[..pos].chars().next_back().is_some_and(is_ident_char);
            if joined_to_ident {
                continue;
            }
            let open = pos + found.len() - 1;
            if find_matching_paren_at(line, open)
                .is_some_and(|close| line[close + 1..].trim_start().starts_with(".await"))
            {
                out.push(pos);
            }
        }
    }
    out
}

/// Only a wait on the loop's own path paces it: on a line at the loop body's
/// top level, outside any block the line opens. A sleep in an `if`, a `match`
/// arm or an `else` runs on some iterations, and the rest run back to back.
fn loop_block_has_scheduler_pacing(code_lines: &[&str], start_idx: usize, exit_depth: i32) -> bool {
    let body_depth = exit_depth + 1;
    let mut depth = exit_depth;

    for (idx, raw) in code_lines.iter().enumerate().skip(start_idx) {
        if idx > start_idx && depth == body_depth && waits_on_loop_path(code_lines, idx) {
            return true;
        }

        depth += brace_delta(raw);
        if idx > start_idx && depth <= exit_depth {
            break;
        }
    }

    false
}

/// Line `idx` waits on a timer outside every block it opens: a directly
/// awaited timer call, or a `select!` that is a pure wait.
fn waits_on_loop_path(code_lines: &[&str], idx: usize) -> bool {
    let line = code_lines[idx];
    let unconditional = |pos: usize| brace_delta(&line[..pos]) == 0;
    awaited_timer_positions(line).into_iter().any(unconditional)
        || line
            .find("select!")
            .is_some_and(|pos| unconditional(pos) && select_at_line_paces(code_lines, idx))
}

/// Lines read past a `select!` looking for the end of its block.
const MAX_SELECT_LINES: usize = 200;

/// A `select!` opening on line `idx` paces its loop the way `sleep(..).await`
/// does only when it is a pure wait: every arm's pattern is `_` and one arm
/// awaits a timer. An arm that binds what its future yields (`Some(msg) =
/// rx.recv()`) feeds data into the loop, so the block paces nothing — and
/// neither does a block this cannot read arm by arm.
fn select_at_line_paces(code_lines: &[&str], idx: usize) -> bool {
    let Some(first) = code_lines.get(idx) else {
        return false;
    };
    let Some(pos) = first.find("select!") else {
        return false;
    };
    let mut text = first[pos + "select!".len()..].to_string();
    for line in code_lines.iter().skip(idx + 1).take(MAX_SELECT_LINES) {
        text.push('\n');
        text.push_str(line);
    }
    let rest = text.trim_start();
    if !rest.starts_with('{') {
        return false;
    }
    let mut depth = 0i32;
    for (i, ch) in rest.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return select_block_paces(&rest[1..i]);
                }
            }
            _ => {}
        }
    }
    false
}

fn select_block_paces(body: &str) -> bool {
    let mut has_timer = false;
    for arm in split_select_arms(body) {
        let arm = arm.trim();
        if arm.is_empty() || arm == "biased" {
            continue;
        }
        let Some(arrow) = find_top_level_arrow(arm) else {
            return false;
        };
        let head = arm[..arrow].trim();
        if head == "else" {
            continue;
        }
        let Some(eq) = find_top_level_assign(head) else {
            return false;
        };
        if head[..eq].trim() != "_" {
            return false;
        }
        has_timer |= line_has_timer_future(&head[eq + 1..]);
    }
    has_timer
}

/// The arms of a `select!` body: split at top-level `,` and `;` (`biased;`),
/// and after a handler block, but not at the `, if` of an arm's guard.
fn split_select_arms(body: &str) -> Vec<&str> {
    let bytes = body.as_bytes();
    let mut arms = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut seen_arrow = false;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' => depth -= 1,
            b'}' => {
                depth -= 1;
                if depth == 0 && seen_arrow {
                    arms.push(&body[start..=i]);
                    start = i + 1;
                    seen_arrow = false;
                }
            }
            b'=' if depth == 0 && bytes.get(i + 1) == Some(&b'>') => {
                seen_arrow = true;
                i += 1;
            }
            b',' | b';' if depth == 0 => {
                let guard_follows = bytes[i] == b','
                    && body[i + 1..]
                        .trim_start()
                        .strip_prefix("if")
                        .is_some_and(|after| {
                            after.starts_with(|c: char| c.is_whitespace() || c == '(')
                        });
                if !guard_follows {
                    arms.push(&body[start..i]);
                    start = i + 1;
                    seen_arrow = false;
                }
            }
            _ => {}
        }
        i += 1;
    }
    arms.push(&body[start..]);
    arms
}

fn find_top_level_arrow(arm: &str) -> Option<usize> {
    let bytes = arm.as_bytes();
    let mut depth = 0i32;
    for i in 0..bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b'=' if depth == 0 && bytes.get(i + 1) == Some(&b'>') => return Some(i),
            _ => {}
        }
    }
    None
}

/// The `=` between an arm's pattern and its future: top level, and not part
/// of `==`, `!=`, `<=`, `>=`, `=>` or a compound assignment.
fn find_top_level_assign(head: &str) -> Option<usize> {
    let bytes = head.as_bytes();
    let mut depth = 0i32;
    for i in 0..bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b'=' if depth == 0 => {
                let prev = i.checked_sub(1).map(|p| bytes[p]);
                let next = bytes.get(i + 1).copied();
                let joined_before = prev.is_some_and(|p| b"=!<>+-*/%&|^".contains(&p));
                let joined_after = matches!(next, Some(b'=') | Some(b'>'));
                if !joined_before && !joined_after {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// A timer future as a `select!` arm names it: `interval.tick()`,
/// `tokio::time::sleep(..)`, `sleep_until(..)`.
fn line_has_timer_future(line: &str) -> bool {
    line.contains(".tick()")
        || line.contains("tokio::time::sleep(")
        || line.contains("sleep_until(")
        || line.trim_start().starts_with("sleep(")
        || line.contains(" sleep(")
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
    if !contains_iterator_loop_pattern(line) {
        return None;
    }

    let params = extract_closure_params(line)?;
    let mut out = HashSet::new();
    collect_closure_param_idents(params, &mut out);
    Some(out)
}

fn starts_iterator_loop(trimmed_line: &str) -> bool {
    contains_iterator_loop_pattern(strip_loop_label(trimmed_line))
}

fn contains_iterator_loop_pattern(line: &str) -> bool {
    ITER_LOOP_PATTERNS.iter().any(|pat| line.contains(pat))
        || ITER_MAP_LOOP_PATTERNS.iter().any(|pat| line.contains(pat))
}

/// One line's pass over method chains. `chains` holds, per brace depth,
/// whether the expression open at that depth is an iterator chain: a source
/// (`.iter()`, `.iter_mut()`, `.into_iter()`) was called and no terminal
/// since. It carries over lines, so a chain rustfmt splits (`ids` / `.iter()`
/// / `.map(|id| {`) or runs through other adaptors (`.filter(..)`) is still
/// one chain. Returns the byte of the `(` of each map-like call (`.map(`,
/// `.filter_map(`, `.flat_map(`) made on an iterator chain.
fn scan_iterator_chains(
    code_line: &str,
    start_depth: i32,
    chains: &mut BTreeMap<i32, bool>,
) -> Vec<usize> {
    let trimmed = code_line.trim_start();
    if trimmed.is_empty() {
        return Vec::new();
    }
    if !trimmed.starts_with(['.', ')', ']', '}']) {
        // Not a continuation: a new expression starts at this depth.
        chains.insert(start_depth, false);
    }

    let bytes = code_line.as_bytes();
    let mut depth = start_depth;
    // The chain state of each `(`/`[` opened on this line; outside them the
    // brace depth's entry holds it.
    let mut groups: Vec<bool> = Vec::new();
    let mut opens = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'{' => {
                depth += 1;
                chains.insert(depth, false);
            }
            b'}' => {
                chains.remove(&depth);
                depth -= 1;
            }
            b'(' | b'[' => groups.push(false),
            b')' | b']' => {
                groups.pop();
            }
            b';' | b',' => set_chain_state(&mut groups, chains, depth, false),
            b'=' => {
                let prev = i.checked_sub(1).map(|p| bytes[p]);
                let next = bytes.get(i + 1).copied();
                let comparison = next == Some(b'=') || prev.is_some_and(|p| b"=!<>".contains(&p));
                if !comparison {
                    // An assignment or a match arm: a new expression starts.
                    set_chain_state(&mut groups, chains, depth, false);
                }
            }
            b'.' => {
                if let Some((name, open)) = method_call_at(code_line, i) {
                    let active = groups
                        .last()
                        .copied()
                        .unwrap_or_else(|| chains.get(&depth).copied().unwrap_or(false));
                    if ITER_SOURCES.contains(&name.as_str()) {
                        set_chain_state(&mut groups, chains, depth, true);
                    } else if ITER_TERMINALS.contains(&name.as_str()) {
                        set_chain_state(&mut groups, chains, depth, false);
                    } else if active && ITER_MAP_ADAPTORS.contains(&name.as_str()) {
                        opens.push(open);
                    }
                    i = open;
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
    }
    opens
}

fn set_chain_state(
    groups: &mut [bool],
    chains: &mut BTreeMap<i32, bool>,
    depth: i32,
    iterator: bool,
) {
    match groups.last_mut() {
        Some(state) => *state = iterator,
        None => {
            chains.insert(depth, iterator);
        }
    }
}

/// The method called at the `.` at byte `dot` (`.name(` or `.name::<T>(`),
/// with the byte of its `(`. A field access, `.await` or a range is not one.
fn method_call_at(line: &str, dot: usize) -> Option<(String, usize)> {
    let bytes = line.as_bytes();
    if bytes.get(dot + 1) == Some(&b'.') || (dot > 0 && bytes[dot - 1] == b'.') {
        return None;
    }
    let start = skip_ws_at(line, dot + 1);
    let first = *bytes.get(start)?;
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    let name = parse_ident_at(line, start)?;
    let mut cursor = skip_optional_turbofish(line, start + name.len());
    cursor = skip_ws_at(line, cursor);
    (bytes.get(cursor) == Some(&b'(')).then_some((name, cursor))
}

/// The closure written as the argument starting at byte `arg_start`, if the
/// argument is one (`|id| ..`, `move |id| ..`, `async move |id| ..`).
fn closure_at(line: &str, arg_start: usize) -> Option<ClosureArg<'_>> {
    let mut cursor = skip_ws_at(line, arg_start);
    for keyword in ["async ", "move "] {
        if line
            .get(cursor..)
            .is_some_and(|rest| rest.starts_with(keyword))
        {
            cursor = skip_ws_at(line, cursor + keyword.len());
        }
    }
    if line.as_bytes().get(cursor) != Some(&b'|') {
        return None;
    }
    let params_start = cursor + 1;
    let close_bar = params_start + line.get(params_start..)?.find('|')?;
    let body_start = close_bar + 1;
    let body = line.get(body_start..)?.trim_start();
    Some(ClosureArg {
        params: &line[params_start..close_bar],
        body_start,
        block: body.starts_with('{') || body.starts_with("->"),
    })
}

/// The `(` of every iterator loop call on the line: the fixed patterns
/// (`.for_each(`, `.iter().map(`, …) and the map-like calls the chain pass
/// found.
fn iterator_call_opens(code_line: &str, chain_opens: &[usize]) -> Vec<usize> {
    let mut opens: Vec<usize> = ITER_LOOP_PATTERNS
        .iter()
        .chain(ITER_MAP_LOOP_PATTERNS.iter())
        .flat_map(|pat| {
            code_line
                .match_indices(pat)
                .map(|(pos, found)| pos + found.len() - 1)
        })
        .chain(chain_opens.iter().copied())
        .collect();
    opens.sort_unstable();
    opens.dedup();
    opens
}

/// The iterator closures on the line with an expression body: each is a loop
/// scope from its parameters to the end of its call — to the end of the line
/// when the call closes on a later one.
fn inline_loops_for(code_line: &str, opens: &[usize]) -> Vec<InlineLoop> {
    opens
        .iter()
        .filter_map(|&open| {
            let closure = closure_at(code_line, open + 1)?;
            if closure.block {
                return None;
            }
            let mut vars = HashSet::new();
            collect_closure_param_idents(closure.params, &mut vars);
            Some(InlineLoop {
                start: closure.body_start,
                end: find_matching_paren_at(code_line, open).unwrap_or(code_line.len()),
                vars,
            })
        })
        .collect()
}

/// The expression-bodied closure a line starts with, as a loop scope up to
/// where its argument ends on the line (a `,` or the call's `)`).
fn leading_inline_loop(code_line: &str) -> Option<InlineLoop> {
    let first = code_line.len() - code_line.trim_start().len();
    let closure = closure_at(code_line, first)?;
    if closure.block {
        return None;
    }
    let mut vars = HashSet::new();
    collect_closure_param_idents(closure.params, &mut vars);
    let bytes = code_line.as_bytes();
    let mut depth = 0i32;
    let mut end = code_line.len();
    for (i, &b) in bytes.iter().enumerate().skip(closure.body_start) {
        match b {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                if depth == 0 {
                    end = i;
                    break;
                }
                depth -= 1;
            }
            b',' if depth == 0 => {
                end = i;
                break;
            }
            _ => {}
        }
    }
    Some(InlineLoop {
        start: closure.body_start,
        end,
        vars,
    })
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
    code_lines: &[&str],
    line_idx: usize,
) -> Option<(String, usize, String)> {
    let code_line = code_lines.get(line_idx).copied().unwrap_or_default();
    let qail_pos = code_line.find("Qail::")?;
    let var_name = extract_assignment_ident(code_line, qail_pos)?;
    let chain = collect_chain(lines, code_lines, line_idx, qail_pos);
    Some((var_name, qail_pos + 1, chain))
}

fn extract_prebuilt_command_binding(
    code_line: &str,
    loop_vars: &HashSet<String>,
) -> Option<(String, QueryBinding)> {
    let (parse_pos, first_arg) = find_qail_parse_call_arg(code_line)?;
    if !expr_uses_loop_var_semantic(&first_arg, loop_vars) {
        return None;
    }

    let var_name = extract_assignment_ident(code_line, parse_pos)?;
    Some((
        var_name,
        QueryBinding {
            uses_loop_var: false,
            batched: false,
            shape_fingerprint: None,
            prebuilt_command: true,
        },
    ))
}

fn extract_nested_batch_plan_binding(code_line: &str) -> Option<String> {
    let call_pos = code_line.find("plan_nested_batch_fetch(")?;
    extract_assignment_ident(code_line, call_pos)
}

fn extract_batched_plan_command_binding(
    code_line: &str,
    loop_stack: &[LoopFrame],
) -> Option<(String, QueryBinding)> {
    let to_qail_pos = code_line.find(".to_qail(")?;
    let receiver = receiver_before_method_call(code_line, to_qail_pos)?;
    if !loop_stack
        .iter()
        .rev()
        .any(|frame| frame.batched_plan_bindings.contains(receiver))
    {
        return None;
    }

    let var_name = extract_assignment_ident(code_line, to_qail_pos)?;
    Some((
        var_name,
        QueryBinding {
            uses_loop_var: false,
            batched: true,
            shape_fingerprint: None,
            prebuilt_command: false,
        },
    ))
}

fn receiver_before_method_call(line: &str, dot_idx: usize) -> Option<&str> {
    let prefix = line.get(..dot_idx)?.trim_end();
    let end = prefix.len();
    let start = prefix
        .char_indices()
        .rev()
        .find_map(|(idx, ch)| (!is_ident_char(ch)).then_some(idx + ch.len_utf8()))
        .unwrap_or(0);
    let receiver = prefix.get(start..end)?.trim();
    if is_plain_ident(receiver) {
        Some(receiver)
    } else {
        None
    }
}

fn find_qail_parse_call_arg(code_line: &str) -> Option<(usize, String)> {
    for callee in [
        "qail_core::parser::parse",
        "crate::parser::parse",
        "parser::parse",
    ] {
        let Some(call_pos) = code_line.find(callee) else {
            continue;
        };

        let mut cursor = call_pos + callee.len();
        cursor = skip_ws_at(code_line, cursor);
        if code_line.as_bytes().get(cursor).copied() != Some(b'(') {
            continue;
        }

        let close = find_matching_paren_at(code_line, cursor)?;
        let args = code_line.get(cursor + 1..close).unwrap_or_default();
        let first_arg = split_top_level(args, ',')
            .first()
            .copied()
            .unwrap_or_default()
            .trim()
            .to_string();
        return Some((call_pos, first_arg));
    }

    None
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

fn collect_chain(
    lines: &[&str],
    code_lines: &[&str],
    start_line_idx: usize,
    qail_pos: usize,
) -> String {
    let mut chain = lines[start_line_idx][qail_pos..].trim().to_string();
    let start_code = code_lines
        .get(start_line_idx)
        .and_then(|line| line.get(qail_pos..))
        .unwrap_or_default()
        .trim();
    let mut depth = super::scanner::count_net_delimiters(start_code);
    let mut j = start_line_idx + 1;

    while j < lines.len() {
        let next_code = code_lines.get(j).copied().unwrap_or_default().trim();
        if next_code.is_empty() {
            if depth > 0 {
                j += 1;
                continue;
            }
            break;
        }
        if depth > 0 || next_code.starts_with('.') {
            let next_raw = lines[j].trim();
            chain.push(' ');
            chain.push_str(next_raw);
            depth += super::scanner::count_net_delimiters(next_code);
            j += 1;
            continue;
        }
        break;
    }

    chain
}

fn parse_qail_chain_shape(chain: &str, loop_vars: &HashSet<String>) -> Option<QueryShape> {
    let qail_pos = chain.find("Qail::")?;
    let qail_chain = chain.get(qail_pos..)?;
    let (action, table_expr, mut cursor) = parse_qail_constructor(qail_chain)?;

    let mut pieces = vec![
        format!("a:{}", action),
        format!("t:{}", normalize_table_token(table_expr)),
    ];
    let mut uses_loop_var = expr_uses_loop_var_semantic(table_expr, loop_vars);
    let mut batched = false;

    while let Some((method, args, next_cursor)) = next_method_call(qail_chain, cursor) {
        cursor = next_cursor;
        let (fragment, method_uses_loop_var, method_batched) =
            method_shape_fragment(&method, &args, loop_vars);
        pieces.push(fragment);
        uses_loop_var |= method_uses_loop_var;
        batched |= method_batched;
    }

    Some(QueryShape {
        fingerprint: pieces.join("|"),
        uses_loop_var,
        batched,
    })
}

fn parse_qail_constructor(chain: &str) -> Option<(String, &str, usize)> {
    let mut cursor = "Qail::".len();
    let action = parse_ident_at(chain, cursor)?;
    cursor += action.len();
    cursor = skip_ws_at(chain, cursor);

    if chain.as_bytes().get(cursor).copied() != Some(b'(') {
        return None;
    }

    let close = find_matching_paren_at(chain, cursor)?;
    let args = chain.get(cursor + 1..close)?;
    let table_expr = split_top_level(args, ',')
        .first()
        .copied()
        .unwrap_or("")
        .trim();

    Some((action.to_ascii_lowercase(), table_expr, close + 1))
}

fn next_method_call(chain: &str, start: usize) -> Option<(String, String, usize)> {
    let bytes = chain.as_bytes();
    let mut i = start;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if bytes[i] != b'.' {
            i += 1;
            continue;
        }

        let mut name_start = i + 1;
        while name_start < bytes.len() && bytes[name_start].is_ascii_whitespace() {
            name_start += 1;
        }

        let name = parse_ident_at(chain, name_start)?;
        let mut cursor = name_start + name.len();
        cursor = skip_ws_at(chain, cursor);
        if chain.as_bytes().get(cursor).copied() != Some(b'(') {
            i = cursor.saturating_add(1);
            continue;
        }

        let close = find_matching_paren_at(chain, cursor)?;
        let args = chain.get(cursor + 1..close)?.to_string();
        return Some((name.to_ascii_lowercase(), args, close + 1));
    }

    None
}

fn method_shape_fragment(
    method: &str,
    args: &str,
    loop_vars: &HashSet<String>,
) -> (String, bool, bool) {
    let method = canonical_shape_method(method);
    let parts = split_top_level(args, ',')
        .into_iter()
        .map(str::trim)
        .collect::<Vec<_>>();

    match method {
        "typed_eq" | "typed_ne" | "typed_gt" | "typed_gte" | "typed_lt" | "typed_lte" => {
            let column = normalize_column_token(parts.first().copied().unwrap_or_default());
            let value_kind = classify_value_kind(parts.get(1).copied(), loop_vars);
            let uses_loop_var = value_kind == "loop";
            (
                format!("f:{method}:{column}:{value_kind}"),
                uses_loop_var,
                false,
            )
        }
        "typed_filter" => {
            let column = normalize_column_token(parts.first().copied().unwrap_or_default());
            let operator = normalize_operator_token(parts.get(1).copied().unwrap_or_default());
            let value_kind = classify_value_kind(parts.get(2).copied(), loop_vars);
            let uses_loop_var = value_kind == "loop";
            let batched = is_batched_operator(&operator);
            (
                format!("f:typed_filter:{operator}:{column}:{value_kind}"),
                uses_loop_var,
                batched,
            )
        }
        "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "like" | "ilike" | "starts_with" => {
            let column = normalize_column_token(parts.first().copied().unwrap_or_default());
            let value_kind = classify_value_kind(parts.get(1).copied(), loop_vars);
            let uses_loop_var = value_kind == "loop";
            (
                format!("f:{method}:{column}:{value_kind}"),
                uses_loop_var,
                false,
            )
        }
        "filter" => {
            let column = normalize_column_token(parts.first().copied().unwrap_or_default());
            let operator = normalize_operator_token(parts.get(1).copied().unwrap_or_default());
            let value_kind = classify_value_kind(parts.get(2).copied(), loop_vars);
            let uses_loop_var = value_kind == "loop";
            let batched = is_batched_operator(&operator);
            (
                format!("f:filter:{operator}:{column}:{value_kind}"),
                uses_loop_var,
                batched,
            )
        }
        "is_null" | "is_not_null" => {
            let column = normalize_column_token(parts.first().copied().unwrap_or_default());
            (format!("f:{method}:{column}"), false, false)
        }
        "array_elem_contained_in_text" => {
            let column = normalize_column_token(parts.first().copied().unwrap_or_default());
            let value_kind = classify_value_kind(parts.get(1).copied(), loop_vars);
            let uses_loop_var = value_kind == "loop";
            (
                format!("f:{method}:{column}:{value_kind}"),
                uses_loop_var,
                false,
            )
        }
        "set_value" => {
            let column = normalize_column_token(parts.first().copied().unwrap_or_default());
            let value_kind = classify_value_kind(parts.get(1).copied(), loop_vars);
            let uses_loop_var = value_kind == "loop";
            (
                format!("f:set_value:{column}:{value_kind}"),
                uses_loop_var,
                false,
            )
        }
        "in_vals" | "in_list" => {
            let column = normalize_column_token(parts.first().copied().unwrap_or_default());
            let value_kind = classify_value_kind(parts.get(1).copied(), loop_vars);
            (format!("f:{method}:{column}:{value_kind}"), false, true)
        }
        _ => (format!("m:{method}"), false, false),
    }
}

fn canonical_shape_method(method: &str) -> &str {
    match method {
        "where_eq" => "eq",
        "or_filter" => "filter",
        "set_opt" | "set_coalesce" | "set_coalesce_opt" => "set_value",
        _ => method,
    }
}

fn parse_ident_at(text: &str, start: usize) -> Option<String> {
    let bytes = text.as_bytes();
    let mut end = start;
    while end < bytes.len() {
        let b = bytes[end];
        if b.is_ascii_alphanumeric() || b == b'_' {
            end += 1;
        } else {
            break;
        }
    }

    if end == start {
        None
    } else {
        text.get(start..end).map(|s| s.to_string())
    }
}

fn skip_ws_at(text: &str, mut idx: usize) -> usize {
    let bytes = text.as_bytes();
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn find_matching_paren_at(text: &str, open_idx: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    if bytes.get(open_idx).copied() != Some(b'(') {
        return None;
    }

    let mut depth = 1usize;
    let mut i = open_idx + 1;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }

    None
}

fn normalize_table_token(expr: &str) -> String {
    let expr = expr.trim();
    if expr.is_empty() {
        return "dyn".to_string();
    }
    if let Some(lit) = parse_string_literal(expr) {
        return lit.to_ascii_lowercase();
    }
    "dyn".to_string()
}

fn normalize_column_token(expr: &str) -> String {
    let expr = expr.trim();
    if expr.is_empty() {
        return "dyn_col".to_string();
    }
    if let Some(lit) = parse_string_literal(expr) {
        return lit.to_ascii_lowercase();
    }
    if expr.chars().all(is_ident_char) {
        return expr.to_ascii_lowercase();
    }
    "dyn_col".to_string()
}

fn normalize_operator_token(expr: &str) -> String {
    let expr = expr.trim();
    if expr.is_empty() {
        return "op".to_string();
    }
    if let Some(lit) = parse_string_literal(expr) {
        return lit.to_ascii_lowercase();
    }
    let token = expr.rsplit("::").next().unwrap_or(expr).trim();
    if token.chars().all(is_ident_char) {
        token.to_ascii_lowercase()
    } else {
        "op".to_string()
    }
}

fn is_batched_operator(operator: &str) -> bool {
    let op = operator.trim().to_ascii_lowercase();
    op == "in" || op == "any"
}

fn classify_value_kind(value: Option<&str>, loop_vars: &HashSet<String>) -> &'static str {
    let value = value.map(str::trim).unwrap_or_default();
    if value.is_empty() {
        return "none";
    }
    if expr_uses_loop_var_semantic(value, loop_vars) {
        return "loop";
    }
    if looks_like_literal(value) {
        "lit"
    } else {
        "expr"
    }
}

fn expr_uses_loop_var_semantic(expr: &str, loop_vars: &HashSet<String>) -> bool {
    if loop_vars.is_empty() {
        return false;
    }
    let without_strings = mask_non_code(expr);
    loop_vars
        .iter()
        .any(|var| contains_ident(&without_strings, var))
}

fn looks_like_literal(expr: &str) -> bool {
    let expr = expr.trim();
    if expr.is_empty() {
        return false;
    }
    if parse_string_literal(expr).is_some() {
        return true;
    }

    if matches!(expr, "true" | "false" | "None" | "null") {
        return true;
    }

    if expr.parse::<i64>().is_ok() || expr.parse::<f64>().is_ok() {
        return true;
    }

    if expr.starts_with("Some(") && expr.ends_with(')') {
        return looks_like_literal(&expr["Some(".len()..expr.len() - 1]);
    }

    expr.starts_with('[') && expr.ends_with(']')
}

fn parse_string_literal(expr: &str) -> Option<String> {
    let expr = expr.trim();
    if expr.len() >= 2
        && ((expr.starts_with('"') && expr.ends_with('"'))
            || (expr.starts_with('\'') && expr.ends_with('\'')))
    {
        return Some(expr[1..expr.len() - 1].to_string());
    }

    if let Some(body) = expr.strip_prefix("r#\"")
        && let Some(inner) = body.strip_suffix("\"#")
    {
        return Some(inner.to_string());
    }

    if let Some(body) = expr.strip_prefix("r\"")
        && let Some(inner) = body.strip_suffix('"')
    {
        return Some(inner.to_string());
    }

    None
}

fn any_loop_var_in_text(loop_vars: &HashSet<String>, text: &str) -> bool {
    loop_vars.iter().any(|v| contains_ident(text, v))
}

fn is_batched_expr(text: &str) -> bool {
    let code = mask_non_code(text);
    code.contains(".in_vals(")
        || code.contains(".in_list(")
        || code.contains(".chunks(")
        || code.contains("Operator::In")
        || code.contains("Value::Array(")
}

fn is_loop_command_replay_arg(loop_vars: &HashSet<String>, arg: &str) -> bool {
    let Some(base) = simple_arg_base_ident(arg) else {
        return false;
    };
    loop_vars.contains(base)
}

fn simple_arg_base_ident(arg: &str) -> Option<&str> {
    let mut expr = arg.trim();

    loop {
        let before = expr;

        while let Some(next) = strip_wrapping_parens(expr) {
            if next.len() == expr.len() {
                break;
            }
            expr = next.trim();
        }

        while let Some(next) = expr.strip_prefix('&').or_else(|| expr.strip_prefix('*')) {
            expr = next.trim_start();
            if let Some(next) = expr.strip_prefix("mut ") {
                expr = next.trim_start();
            }
        }

        if let Some(base) = [".clone()", ".as_ref()", ".as_str()"]
            .iter()
            .find_map(|suffix| expr.strip_suffix(suffix))
        {
            expr = base.trim_end();
            continue;
        }

        if expr == before {
            break;
        }
    }

    if is_plain_ident(expr) {
        Some(expr)
    } else {
        None
    }
}

fn strip_wrapping_parens(expr: &str) -> Option<&str> {
    let expr = expr.trim();
    if !expr.starts_with('(') || !expr.ends_with(')') {
        return None;
    }

    let close = find_matching_paren_at(expr, 0)?;
    if close == expr.len() - 1 {
        expr.get(1..close)
    } else {
        None
    }
}

#[derive(Debug)]
struct ExecCall {
    column: usize,
    column_offset: usize,
    first_arg: String,
    /// The argument list closes in the scanned text. When the call wraps its
    /// arguments onto later lines, `first_arg` holds only what the text has.
    args_closed: bool,
}

/// Lines read past a wrapped exec call looking for its closing paren.
const MAX_WRAPPED_ARG_LINES: usize = 40;

/// `find_exec_call` on line `idx`, reading a wrapped argument list
/// (`conn.fetch_all(\n    &cmd,\n)`) across the lines after it, so the
/// first argument's shape — batched, loop-dependent — is judged whole.
fn find_exec_call_spanning(lines: &[&str], idx: usize) -> Option<ExecCall> {
    let line = lines.get(idx)?;
    let call = find_exec_call(line)?;
    if call.args_closed {
        return Some(call);
    }
    let mut text = (*line).to_string();
    for next in lines.iter().skip(idx + 1).take(MAX_WRAPPED_ARG_LINES) {
        text.push('\n');
        text.push_str(next);
        if let Some(whole) = find_exec_call(&text)
            && whole.args_closed
        {
            return Some(whole);
        }
    }
    Some(call)
}

fn find_exec_call(line: &str) -> Option<ExecCall> {
    let bytes = line.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if starts_with_bytes(bytes, i, b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if starts_with_bytes(bytes, i, b"/*") {
            i = consume_block_comment(bytes, i);
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, i) {
            i = next;
            continue;
        }

        if bytes[i] != b'.' {
            i += 1;
            continue;
        }

        let mut name_start = i + 1;
        while name_start < bytes.len() && bytes[name_start].is_ascii_whitespace() {
            name_start += 1;
        }

        let Some(name) = parse_ident_at(line, name_start) else {
            i += 1;
            continue;
        };
        let method = name.to_ascii_lowercase();
        if !is_exec_method(&method) {
            i = name_start + name.len();
            continue;
        }

        let mut cursor = name_start + name.len();
        cursor = skip_ws_at(line, cursor);
        cursor = skip_optional_turbofish(line, cursor);
        cursor = skip_ws_at(line, cursor);
        if bytes.get(cursor).copied() != Some(b'(') {
            i = cursor.saturating_add(1);
            continue;
        }

        // An argument list that wraps onto the next line is still a call:
        // giving up here left every rustfmt-wrapped `fetch_all(` unseen.
        let (args, args_closed) = match find_matching_paren_at(line, cursor) {
            Some(close) => (line.get(cursor + 1..close).unwrap_or_default(), true),
            None => (line.get(cursor + 1..).unwrap_or_default(), false),
        };
        let first_arg = split_top_level(args, ',')
            .first()
            .copied()
            .unwrap_or_default()
            .trim()
            .to_string();

        return Some(ExecCall {
            column: i + 1,
            column_offset: i + 1,
            first_arg,
            args_closed,
        });
    }

    None
}

fn is_exec_method(name: &str) -> bool {
    EXEC_METHODS.iter().any(|candidate| candidate == &name)
}

fn skip_optional_turbofish(line: &str, start: usize) -> usize {
    let bytes = line.as_bytes();
    let mut cursor = skip_ws_at(line, start);
    if !line
        .get(cursor..)
        .is_some_and(|tail| tail.starts_with("::"))
    {
        return cursor;
    }

    cursor += 2;
    cursor = skip_ws_at(line, cursor);
    if bytes.get(cursor).copied() != Some(b'<') {
        return cursor;
    }

    let mut angle_depth = 1i32;
    cursor += 1;

    while cursor < bytes.len() {
        if starts_with_bytes(bytes, cursor, b"//") {
            cursor += 2;
            while cursor < bytes.len() && bytes[cursor] != b'\n' {
                cursor += 1;
            }
            continue;
        }

        if starts_with_bytes(bytes, cursor, b"/*") {
            cursor = consume_block_comment(bytes, cursor);
            continue;
        }

        if let Some(next) = consume_rust_literal(bytes, cursor) {
            cursor = next;
            continue;
        }

        match bytes[cursor] {
            b'<' => angle_depth += 1,
            b'>' => {
                angle_depth -= 1;
                if angle_depth == 0 {
                    cursor += 1;
                    break;
                }
            }
            _ => {}
        }
        cursor += 1;
    }

    cursor
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
    fn detects_where_eq_loop_variable_dependency() {
        let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn) {
    for id in ids {
        let cmd = Qail::get("users").where_eq("id", id);
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
    fn detects_typed_loop_variable_dependent_query_execution() {
        let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn) {
    for id in ids {
        let cmd = Qail::typed(users::table).typed_eq(users::id(), id);
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
    fn does_not_mark_loop_dependent_when_only_column_matches_loop_ident() {
        let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn) {
    for id in ids {
        let cmd = Qail::get("users").eq("id", 1);
        let _ = conn.fetch_all(&cmd).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1001),
            "{diags:?}"
        );
        assert!(
            !diags.iter().any(|d| d.code == NPlusOneCode::N1002),
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
    fn ignores_batched_filter_operator_in_pattern() {
        let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn) {
    for chunk in ids.chunks(100) {
        let cmd = Qail::get("users").filter("id", Operator::In, chunk.to_vec());
        let _ = conn.fetch_all(&cmd).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn ignores_inline_batched_query_chain() {
        let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn) {
    for chunk in ids.chunks(100) {
        let _ = conn.fetch_all(&Qail::get("users").in_vals("id", chunk.to_vec())).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn detects_exec_call_with_turbofish_generics() {
        let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn) {
    for id in ids {
        let cmd = Qail::get("users").eq("id", id);
        let _ = conn.fetch_all::<UserRow>(&cmd).await;
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
    fn detects_pg_and_qdrant_exec_method_variants() {
        let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn, vectors: Vec<Vec<f32>>, driver: &mut QdrantDriver) {
    for id in ids {
        let cmd = Qail::get("users").eq("id", id);
        let _ = conn.fetch_typed::<UserRow>(&cmd).await;
        let _ = conn.fetch_one_typed::<UserRow>(&cmd).await;
        let _ = conn.fetch_all_cached(&cmd).await;
        let _ = conn.query_ast(&cmd).await;
        let _ = driver.search_ast(&cmd).await;
    }
    for vector in vectors {
        let _ = driver.search("users", vector, 10, None).await;
        let _ = driver.scroll("users", None, 100, None).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "{diags:?}"
        );
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1001),
            "{diags:?}"
        );
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
    fn detects_iter_map_query_execution() {
        let source = r#"
fn demo(conn: &Conn, ids: Vec<i64>) {
    let _rows = ids.iter().map(|id| {
        let cmd = Qail::get("users").eq("id", *id);
        conn.fetch_all(&cmd)
    }).collect::<Vec<_>>();
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "{diags:?}"
        );
    }

    #[test]
    fn ignores_expression_only_iter_map_before_later_block() {
        let source = r#"
async fn load_users(conn: &Conn, ids: &[String]) {
    let cmd = Qail::get("users").in_vals("id", ids);
    let _ = conn.fetch_all(&cmd).await;
}

async fn demo(conn: &Conn, rows: Vec<Row>, compact: bool) {
    let ids: Vec<String> = rows.iter().map(|r| r.text(0)).collect();
    let _users = if compact {
        Vec::new()
    } else {
        load_users(conn, &ids).await
    };
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            !diags.iter().any(|d| d.code == NPlusOneCode::N1003),
            "{diags:?}"
        );
    }

    #[test]
    fn ignores_method_reference_iter_map_before_later_match() {
        let source = r#"
async fn demo(conn: &Conn, ids: Vec<String>) {
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let cmd = Qail::get("users").in_vals("id", id_refs);
    let _rows = match conn.fetch_all(&cmd).await {
        Ok(rows) => rows,
        Err(_) => Vec::new(),
    };
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn detects_multiline_iterator_closure_query_execution() {
        let source = r#"
fn demo(conn: &Conn, ids: Vec<i64>) {
    ids.iter().for_each(
        |id| {
            let cmd = Qail::get("users").eq("id", *id);
            let _ = conn.fetch_all(&cmd);
        }
    );
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
    fn detects_nested_function_query_helper_called_in_loop() {
        let source = r#"
fn outer_process(conn: &Conn, ids: Vec<i64>) {
    fn run_query(conn: &Conn, id: i64) {
        let cmd = Qail::get("users").eq("id", id);
        let _ = conn.fetch_all(&cmd);
    }

    for id in ids {
        run_query(conn, id);
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
    fn detects_self_method_inside_impl_with_spaced_generics() {
        let source = r#"
struct Repo<'a, T>(&'a T);

impl   <'a, T> Repo<'a, T> {
    fn load_user(&self, conn: &Conn, id: i64) {
        let cmd = Qail::get("users").eq("id", id);
        let _ = conn.fetch_one(&cmd);
    }

    fn process(&self, conn: &Conn, ids: Vec<i64>) {
        for id in ids {
            self.load_user(conn, id);
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
    fn does_not_treat_self_field_method_call_as_self_method_helper() {
        let source = r#"
struct RepoClient;
impl RepoClient {
    async fn load_user(&self, _conn: &Conn, _id: i64) {}
}

struct Repo {
    client: RepoClient,
}

impl Repo {
    async fn load_user(&self, conn: &Conn, id: i64) {
        let cmd = Qail::get("users").eq("id", id);
        let _ = conn.fetch_one(&cmd).await;
    }

    async fn process(&self, conn: &Conn, ids: Vec<i64>) {
        for id in ids {
            self.client.load_user(conn, id).await;
        }
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
    fn ignores_indirect_call_markers_inside_comments() {
        let source = r#"
async fn load_user(conn: &Conn, id: i64) {
    let cmd = Qail::get("users").eq("id", id);
    let _ = conn.fetch_one(&cmd).await;
}

async fn process(conn: &Conn, ids: Vec<i64>) {
    for id in ids {
        // load_user(conn, id).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn unsafe_and_const_signatures_are_not_collected_as_calls() {
        assert!(collect_function_calls("pub unsafe fn fetch_user(conn: &Conn) {").is_empty());
        assert!(collect_function_calls("const unsafe fn build_query() -> Qail {").is_empty());
        assert!(
            collect_function_calls("pub(crate) const unsafe fn build_query() -> Qail {").is_empty()
        );
    }

    #[test]
    fn detects_unsafe_query_helper_called_in_loop() {
        let source = r#"
pub unsafe fn load_user(conn: &Conn, id: i64) {
    let cmd = Qail::get("users").eq("id", id);
    let _ = conn.fetch_one(&cmd);
}

fn process(conn: &Conn, ids: Vec<i64>) {
    for id in ids {
        unsafe {
            load_user(conn, id);
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

    #[test]
    fn ignores_block_comment_with_fake_loop_and_query() {
        let source = r#"
async fn demo(conn: &Conn) {
    /*
    for id in ids {
        let cmd = Qail::get("users").eq("id", id);
        let _ = conn.fetch_all(&cmd).await;
    }
    */
    let _ = conn.fetch_all(&Qail::get("users")).await;
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            !diags.iter().any(|d| d.line >= 4 && d.line <= 7),
            "{diags:?}"
        );
    }

    #[test]
    fn does_not_use_qail_marker_inside_string_as_binding_shape() {
        let source = r#"
async fn demo(ids: Vec<i64>, conn: &Conn) {
    for id in ids {
        let cmd = "Qail::get(\"users\").eq(\"id\", id)";
        let _ = conn.fetch_all(&cmd).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1001),
            "{diags:?}"
        );
        assert!(
            !diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "{diags:?}"
        );
    }

    #[test]
    fn ignores_exec_markers_inside_string_and_comments() {
        let source = r#"
fn demo(ids: Vec<i64>) {
    for id in ids {
        let _fake = ".fetch_all(&Qail::get(\"users\").eq(\"id\", id))";
        // let _ = conn.fetch_all(&Qail::get("users").eq("id", id));
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn ignores_indirect_query_call_inside_scheduler_loop() {
        let source = r#"
async fn run_once(conn: &Conn) {
    let _ = conn.fetch_all(&Qail::get("users")).await;
}

async fn worker(conn: &Conn) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        interval.tick().await;
        run_once(conn).await;
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
    fn ignores_direct_query_call_inside_sleep_paced_scheduler_loop() {
        let source = r#"
async fn worker(conn: &Conn) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let _ = conn.fetch_all(&Qail::get("users")).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            !diags.iter().any(|d| d.code == NPlusOneCode::N1001),
            "{diags:?}"
        );
    }

    #[test]
    fn keeps_loop_variable_detection_even_when_loop_has_sleep() {
        let source = r#"
async fn worker(conn: &Conn, ids: Vec<i64>) {
    for id in ids {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
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
    fn ignores_prebuilt_qail_command_replay_loop() {
        let source = r#"
async fn apply_commands(conn: &Conn, cmds: &[Qail]) {
    for cmd in cmds {
        conn.execute(cmd).await.unwrap();
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn ignores_borrowed_prebuilt_qail_command_replay_loop() {
        let source = r#"
async fn apply_commands(conn: &Conn, cmds: &[Qail]) {
    for cmd in cmds {
        conn.execute(&cmd).await.unwrap();
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn ignores_cloned_prebuilt_qail_command_replay_loop() {
        let source = r#"
async fn apply_commands(conn: &Conn, cmds: &[Qail]) {
    for cmd in cmds {
        conn.execute(cmd.clone()).await.unwrap();
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn ignores_parser_originated_loop_command_replay() {
        let source = r#"
async fn apply_commands(conn: &Conn, queries: Vec<String>) {
    for query_text in queries {
        let mut cmd = match qail_core::parser::parse(query_text.as_str()) {
            Ok(cmd) => cmd,
            Err(_) => continue,
        };
        optimize(&mut cmd);
        conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn ignores_nested_batch_plan_to_qail_loop() {
        let source = r#"
async fn expand(conn: &Conn, relations: Vec<&str>, parent_keys: Vec<Value>) {
    for rel in relations {
        let plan = match plan_nested_batch_fetch(&registry, "users", rel, parent_keys.clone()) {
            Ok(Some(plan)) => plan,
            Ok(None) => continue,
            Err(_) => continue,
        };
        let mut cmd = plan.to_qail();
        cmd = cmd.filter("tenant_id", Operator::Eq, tenant_id.clone());
        conn.fetch_all(&cmd).await.unwrap();
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn generic_to_qail_loop_still_reports_without_batch_plan_origin() {
        let source = r#"
async fn replay(conn: &Conn, items: Vec<Item>) {
    for item in items {
        let plan = build_plan(item);
        let cmd = plan.to_qail();
        conn.fetch_all(&cmd).await.unwrap();
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
    fn still_flags_fresh_loop_dependent_qail_builder_with_comment_noise() {
        let source = r#"
async fn apply_commands(conn: &Conn, versions: Vec<String>) {
    for version in versions {
        // this text is ignored; comments are not an N+1 suppression channel
        let cmd = Qail::del("_qail_migrations").where_eq("version", version.as_str());
        conn.execute(&cmd).await.unwrap();
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
    fn lifetime_in_signature_does_not_hide_later_query_loop() {
        let source = r#"
fn day_query(scope: DayScope<'_>, key: &str) -> Qail {
    Qail::get("orders").eq("key", key)
}

// The caller's orders: its own, plus the admitted ones.
async fn read_day(conn: &Conn, ids: Vec<String>, scope: DayScope<'_>) {
    for id in ids {
        let cmd = Qail::get("orders").eq("id", id.as_str());
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
    fn pure_builder_with_lifetime_is_not_query_executing() {
        let source = r#"
fn day_query(scope: DayScope<'_>, key: &str) -> Qail {
    Qail::get("orders").eq("key", key)
}

// The caller's orders.
async fn read_day(conn: &Conn, ids: Vec<String>, scope: DayScope<'_>) {
    // caller's own rows first
    let mut rows = Vec::new();
    for key in ["travel_date", "return_travel_date"] {
        let cmd = day_query(scope, key).in_vals("id", ids.clone());
        rows.extend(conn.fetch_all(&cmd).await);
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
    fn closure_free_iter_map_tail_does_not_open_a_loop() {
        let source = r#"
async fn operated(conn: &Conn) -> Result<Vec<Row>, String> {
    let rows = conn.fetch_all(&Qail::get("routes")).await?;
    Ok(rows.iter().map(route_row).collect())
}

struct Topology {
    routes: Vec<Row>,
}

impl Topology {
    fn operator_of(&self, id: &str) -> Option<&str> {
        self.routes.iter().find(|r| r.id == id).map(|r| r.tenant.as_str())
    }
}

async fn read_topology(conn: &Conn) {
    let _ = operated(conn).await;
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn closure_free_iter_map_argument_does_not_open_a_loop() {
        let source = r#"
async fn lock_rows(conn: &Conn, ids: Vec<String>) -> Result<usize, String> {
    let cmd = Qail::get("inventory")
        .in_vals("segment_id", ids.iter().map(String::as_str))
        .for_update()
        .map_err(|e| format!("lock build: {e}"))?;
    let rows = conn.fetch_all(&cmd).await.map_err(|e| format!("lock: {e}"))?;
    Ok(rows.len())
}

async fn active_orders(conn: &Conn, odyssey_id: &str) -> Result<Vec<Row>, String> {
    let cmd = Qail::get("connections").eq("odyssey_id", odyssey_id);
    let rows = conn
        .fetch_all(&cmd)
        .await
        .map_err(|e| format!("connections: {e}"))?;
    Ok(rows)
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn multiline_expression_closure_does_not_capture_a_later_block() {
        let source = r#"
async fn load(conn: &Conn) {
    let _ = conn.fetch_all(&Qail::get("users")).await;
}

async fn demo(conn: &Conn, ids: Vec<String>, compact: bool) {
    let names: Vec<String> = ids.iter().map(
        |id| id.to_uppercase()
    ).collect();
    if compact {
        load(conn).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn ignores_query_calls_in_select_tick_paced_loop() {
        let source = r#"
async fn dispatch_tick(conn: &Conn) {
    let _ = conn.fetch_all(&Qail::get("outbox")).await;
}

async fn worker(conn: &Conn, wake: &Notify) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        let from_timer = tokio::select! {
            _ = interval.tick() => true,
            _ = wake.notified() => false,
        };
        dispatch_tick(conn).await;
        if from_timer {
            dispatch_tick(conn).await;
        }
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn ignores_query_calls_in_select_sleep_paced_loop() {
        let source = r#"
async fn sweep(conn: &Conn) {
    let _ = conn.fetch_all(&Qail::get("holds")).await;
}

async fn worker(conn: &Conn, stop: &Notify) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
            _ = stop.notified() => return,
        }
        sweep(conn).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn select_loop_without_timer_arm_still_flags() {
        let source = r#"
async fn save(conn: &Conn) {
    let _ = conn.fetch_all(&Qail::get("events")).await;
}

async fn consume(conn: &Conn, rx: &mut Receiver<Event>, stop: &Notify) {
    loop {
        tokio::select! {
            _ = rx.recv() => {}
            _ = stop.notified() => return,
        }
        save(conn).await;
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
    fn select_loop_with_data_arm_and_timer_still_flags() {
        let source = r#"
async fn handle(conn: &Conn, msg: Msg) {
    let _ = conn.fetch_all(&Qail::get("events").eq("id", msg.id)).await;
}

async fn consume(conn: &Conn, rx: &mut Receiver<Msg>) {
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                handle(conn, msg).await;
            }
            _ = heartbeat.tick() => {}
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
    fn biased_select_with_guarded_timer_arm_paces_the_loop() {
        let source = r#"
async fn sweep(conn: &Conn) {
    let _ = conn.fetch_all(&Qail::get("holds")).await;
}

async fn worker(conn: &Conn, shutdown: &Notify, ready: bool) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => return,
            _ = interval.tick(), if ready => {}
        }
        sweep(conn).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn closure_chained_after_function_map_still_flags() {
        let source = r#"
async fn demo(conn: &Conn, ids: Vec<i64>) {
    let rows = ids.iter().map(to_key)
        .map(|key| {
            conn.fetch_all(&Qail::get("t").eq("k", key))
        })
        .collect::<Vec<_>>();
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "{diags:?}"
        );
    }

    #[test]
    fn closure_chained_after_a_closed_block_still_flags() {
        let source = r#"
async fn demo(conn: &Conn, data: Vec<Group>) {
    let rows = data.iter().map(|d| {
        d.items.iter().map(to_key)
    })
    .map(|k| {
        conn.fetch_all(&Qail::get("t").eq("k", k))
    })
    .collect::<Vec<_>>();
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "{diags:?}"
        );
    }

    #[test]
    fn closure_block_on_the_line_after_its_params_still_flags() {
        let source = r#"
fn demo(conn: &Conn, ids: Vec<i64>) {
    ids.iter().for_each(
        |id|
        {
            let cmd = Qail::get("users").eq("id", *id);
            let _ = conn.fetch_all(&cmd);
        }
    );
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "{diags:?}"
        );
    }

    #[test]
    fn expression_closure_opening_a_block_later_still_flags() {
        let source = r#"
fn demo(conn: &Conn, ids: Vec<i64>) {
    ids.iter().for_each(
        |id| lookup(*id)
            .map(|row| {
                let cmd = Qail::del("t").eq("id", row);
                let _ = conn.fetch_all(&cmd);
            })
    );
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1001),
            "{diags:?}"
        );
    }

    #[test]
    fn rustfmt_split_iterator_chain_is_a_loop() {
        let source = r#"
async fn demo(conn: &Conn, ids: Vec<i64>) {
    let rows = ids
        .iter()
        .map(|id| {
            conn.fetch_one(&Qail::get("t").eq("id", *id))
        })
        .collect::<Vec<_>>();
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "{diags:?}"
        );
    }

    #[test]
    fn iterator_chain_through_other_adaptors_is_a_loop() {
        let source = r#"
async fn demo(conn: &Conn, orders: Vec<Order>) {
    let rows = orders
        .iter()
        .filter(|o| o.is_paid())
        .map(|order| {
            conn.fetch_one(&Qail::get("t").eq("id", order.id))
        })
        .collect::<Vec<_>>();
    let more = orders.iter().filter(|o| o.is_paid()).map(|order| {
        conn.fetch_one(&Qail::get("t").eq("id", order.id))
    });
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        let lines: Vec<usize> = diags
            .iter()
            .filter(|d| d.code == NPlusOneCode::N1002)
            .map(|d| d.line)
            .collect();
        assert_eq!(lines, vec![7, 11], "{diags:?}");
    }

    #[test]
    fn option_map_after_an_iterator_terminal_is_not_a_loop() {
        let source = r#"
async fn demo(conn: &Conn, ids: Vec<i64>, rows: Vec<Row>) {
    let first = ids
        .iter()
        .find(|id| **id > 0)
        .map(|id| {
            conn.fetch_one(&Qail::get("t").eq("id", *id))
        });
    let next = ids.iter().next().map(|id| {
        conn.fetch_one(&Qail::get("t").eq("id", *id))
    });
    let top = rows.first().map(|row| {
        conn.fetch_one(&Qail::get("t").eq("id", row.id))
    });
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn inline_iterator_closure_calling_a_query_helper_flags() {
        let source = r#"
async fn load(conn: &Conn, id: i64) -> Row {
    conn.fetch_one(&Qail::get("t").eq("id", id)).await
}

async fn demo(conn: &Conn, ids: Vec<i64>) {
    let rows = join_all(ids.iter().map(|id| load(conn, *id))).await;
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1003),
            "{diags:?}"
        );
    }

    #[test]
    fn inline_iterator_closure_executing_a_query_flags() {
        let source = r#"
async fn demo(conn: &Conn, ids: Vec<i64>) {
    let rows = join_all(ids.iter().map(|id| conn.fetch_one(&Qail::get("t").eq("id", *id)))).await;
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(
            diags.iter().any(|d| d.code == NPlusOneCode::N1002),
            "{diags:?}"
        );
    }

    #[test]
    fn inline_closure_building_ids_for_one_batched_query_is_clean() {
        let source = r#"
async fn load_users(conn: &Conn, ids: &[String]) -> Vec<Row> {
    conn.fetch_all(&Qail::get("users").in_vals("id", ids)).await
}

async fn demo(conn: &Conn, rows: Vec<Row>) {
    let users = load_users(conn, &rows.iter().map(|r| r.text(0)).collect::<Vec<_>>()).await;
    let direct = conn.fetch_all(&Qail::get("users").in_vals("id", rows.iter().map(|r| r.text(0)).collect::<Vec<_>>())).await;
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn sleep_in_a_backoff_branch_does_not_pace_the_loop() {
        let source = r#"
async fn handle(conn: &Conn, m: Msg) {
    let _ = conn.fetch_all(&Qail::get("t").eq("id", m.id)).await;
}

async fn consume(conn: &Conn, rx: &mut Receiver<Msg>) {
    loop {
        match rx.recv().await {
            Some(m) => handle(conn, m).await,
            None => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
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
    fn sleep_inside_an_if_on_one_line_does_not_pace_the_loop() {
        let source = r#"
async fn drain(conn: &Conn) -> usize {
    conn.fetch_all(&Qail::get("jobs")).await.len()
}

async fn worker(conn: &Conn) {
    loop {
        let n = drain(conn).await;
        if n == 0 { tokio::time::sleep(std::time::Duration::from_secs(5)).await; }
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
    fn sleep_after_each_claimed_job_paces_the_loop() {
        let source = r#"
async fn claim_next_job(conn: &Conn) -> Option<Job> {
    let rows = conn.fetch_all(&Qail::get("jobs").limit(1)).await;
    rows.first().map(to_job)
}

async fn process_job(conn: &Conn, job: Job) {
    let _ = conn.fetch_all(&Qail::set("jobs").eq("id", job.id)).await;
}

async fn drain_due_jobs(conn: &Conn) {
    loop {
        let job = match claim_next_job(conn).await {
            Some(job) => job,
            None => break,
        };
        process_job(conn, job).await;
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn one_line_select_with_a_data_arm_is_not_paced_by_its_tick() {
        let source = r#"
async fn handle(conn: &Conn, m: Msg) {
    let _ = conn.fetch_all(&Qail::get("t").eq("id", m.id)).await;
}

async fn worker(conn: &Conn, rx: &mut Receiver<Msg>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! { Some(m) = rx.recv() => handle(conn, m).await, _ = interval.tick() => {} }
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
    fn detects_helper_whose_exec_call_wraps_its_arguments() {
        let source = r#"
async fn claim(conn: &Conn, lane: i64) {
    let _ = conn
        .fetch_all(
            &Qail::get("lanes")
                .eq("id", lane)
                .for_update_skip_locked(),
        )
        .await;
}

async fn run(conn: &Conn, lanes: Vec<i64>) {
    for lane in lanes {
        claim(conn, lane).await;
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
    fn detects_loop_dependent_exec_call_with_wrapped_arguments() {
        let source = r#"
async fn demo(conn: &Conn, ids: Vec<i64>) {
    for id in ids {
        let _ = conn.fetch_all(
            &Qail::get("users").eq("id", id),
        ).await;
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
    fn ignores_batched_exec_call_with_wrapped_arguments() {
        let source = r#"
async fn demo(conn: &Conn, groups: Vec<Vec<i64>>) {
    for chunk in groups {
        let _ = conn.fetch_all(
            // one batch per group
            &Qail::get("users").in_vals("id", chunk),
        ).await;
    }
}
"#;

        let diags = detect_n_plus_one_in_file("demo.rs", source);
        assert!(diags.is_empty(), "{diags:?}");
    }
}
