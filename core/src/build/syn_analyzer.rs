//! Syn-based AST analyzer for deeper Qail chain scanning.
//!
//! Gated behind `#[cfg(feature = "syn-scanner")]` — parses Rust source
//! with `syn` to follow chains through closures, async blocks, and variable assignments.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use syn::spanned::Spanned;
use syn::visit::Visit;

use super::scanner::QailUsage;

type SynUsageKey = (String, usize, usize, String, String);

#[derive(Debug, Clone)]
pub(crate) struct SynParsedUsage {
    pub(crate) line: usize,
    pub(crate) column: usize,
    pub(crate) action: String,
    pub(crate) table: String,
    pub(crate) is_dynamic_table: bool,
    pub(crate) cmd: crate::ast::Qail,
    pub(crate) has_rls: bool,
    pub(crate) scope_uses_super_admin: bool,
    pub(crate) score: usize,
}

struct SynMethodStep {
    name: String,
    args: Vec<syn::Expr>,
}

#[derive(Debug)]
struct SynConstructor {
    line: usize,
    column: usize,
    action: String,
    ast_action: crate::ast::Action,
    table: String,
    is_dynamic_table: bool,
}

pub(crate) fn syn_usage_key(
    file: &str,
    line: usize,
    column: usize,
    action: &str,
    table: &str,
) -> SynUsageKey {
    (
        file.to_string(),
        line,
        column,
        action.to_string(),
        table.to_string(),
    )
}

pub(crate) fn build_syn_usage_index(usages: &[QailUsage]) -> HashMap<SynUsageKey, SynParsedUsage> {
    let mut files = std::collections::HashSet::new();
    for usage in usages {
        files.insert(usage.file.clone());
    }

    let mut index: HashMap<SynUsageKey, SynParsedUsage> = HashMap::new();
    for file in files {
        for parsed in extract_syn_usages_from_file(&file) {
            let key = syn_usage_key(
                &file,
                parsed.line,
                parsed.column,
                &parsed.action,
                &parsed.table,
            );
            match index.get(&key) {
                Some(existing) if existing.score >= parsed.score => {}
                _ => {
                    index.insert(key, parsed);
                }
            }
        }
    }

    index
}

pub(crate) fn extract_syn_usages_from_file(file: &str) -> Vec<SynParsedUsage> {
    let Ok(content) = fs::read_to_string(file) else {
        return Vec::new();
    };
    extract_syn_usages_from_source(&content)
}

pub(crate) fn scan_source_files_syn(src_dir: &str) -> Vec<QailUsage> {
    let mut usages = Vec::new();
    scan_syn_directory(Path::new(src_dir), &mut usages);
    usages
}

fn scan_syn_directory(dir: &Path, usages: &mut Vec<QailUsage>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_syn_directory(&path, usages);
            continue;
        }
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }

        let file = path.display().to_string();
        let Ok(source) = fs::read_to_string(&path) else {
            continue;
        };
        usages.extend(emit_qail_usages_from_syn_source(&file, &source));
    }
}

pub(crate) fn emit_qail_usages_from_syn_source(file: &str, source: &str) -> Vec<QailUsage> {
    let allow_lines = collect_super_admin_allow_lines(source);
    let file_cte_names = collect_file_cte_aliases(source);
    let parsed_usages = dedupe_syn_usages(extract_syn_usages_from_source(source));
    let allow_indices = bind_allow_comments_to_next_usage(&parsed_usages, &allow_lines);

    let mut out = Vec::new();
    for (idx, parsed) in parsed_usages.into_iter().enumerate() {
        let columns = collect_columns_from_cmd(&parsed.cmd);
        let allow_super_admin = allow_indices.contains(&idx);
        out.push(QailUsage {
            file: file.to_string(),
            line: parsed.line,
            column: parsed.column,
            table: parsed.table.clone(),
            is_dynamic_table: parsed.is_dynamic_table,
            columns,
            action: parsed.action.clone(),
            is_cte_ref: file_cte_names.contains(&parsed.table),
            has_rls: parsed.has_rls,
            file_uses_super_admin: parsed.scope_uses_super_admin && !allow_super_admin,
        });
    }
    out
}

fn dedupe_syn_usages(usages: Vec<SynParsedUsage>) -> Vec<SynParsedUsage> {
    let mut best_by_key: HashMap<(usize, usize, String, String), SynParsedUsage> = HashMap::new();
    for usage in usages {
        let key = (
            usage.line,
            usage.column,
            usage.action.clone(),
            usage.table.clone(),
        );
        match best_by_key.get(&key) {
            Some(existing) if existing.score >= usage.score => {}
            _ => {
                best_by_key.insert(key, usage);
            }
        }
    }
    let mut deduped: Vec<SynParsedUsage> = best_by_key.into_values().collect();
    deduped.sort_by(|a, b| {
        (a.line, a.column, &a.action, &a.table).cmp(&(b.line, b.column, &b.action, &b.table))
    });
    deduped
}

fn collect_super_admin_allow_lines(source: &str) -> std::collections::HashSet<usize> {
    source
        .lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            if line.contains("// qail:allow(super_admin)") {
                Some(idx + 1)
            } else {
                None
            }
        })
        .collect()
}

fn bind_allow_comments_to_next_usage(
    usages: &[SynParsedUsage],
    allow_lines: &std::collections::HashSet<usize>,
) -> std::collections::HashSet<usize> {
    let mut allowed = std::collections::HashSet::new();
    if allow_lines.is_empty() {
        return allowed;
    }

    let mut sorted_allows: Vec<usize> = allow_lines.iter().copied().collect();
    sorted_allows.sort_unstable();

    let mut usage_positions: Vec<(usize, usize, usize)> = usages
        .iter()
        .enumerate()
        .map(|(idx, usage)| (usage.line, usage.column, idx))
        .collect();
    usage_positions.sort_unstable();

    let mut usage_cursor = 0usize;
    for allow_line in sorted_allows {
        while usage_cursor < usage_positions.len() && usage_positions[usage_cursor].0 < allow_line {
            usage_cursor += 1;
        }
        if usage_cursor < usage_positions.len() {
            allowed.insert(usage_positions[usage_cursor].2);
            usage_cursor += 1;
        }
    }

    allowed
}

fn collect_file_cte_aliases(source: &str) -> std::collections::HashSet<String> {
    let mut file_cte_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for line in source.lines() {
        let line = line.trim();
        // .to_cte("name")
        let mut search = 0usize;
        while let Some(pos) = line[search..].find(".to_cte(") {
            let abs = search + pos;
            let after = &line[abs + 8..];
            if let Some(name) = extract_string_arg_for_syn(after) {
                file_cte_names.insert(name);
            }
            search = abs + 8;
        }
        // .with("name", Qail::...)
        search = 0;
        while let Some(pos) = line[search..].find(".with(") {
            let abs = search + pos;
            let after = &line[abs + 6..];
            if let Some(name) = extract_inline_cte_alias_for_syn(after) {
                file_cte_names.insert(name);
            }
            search = abs + 6;
        }
    }
    file_cte_names
}

fn extract_string_arg_for_syn(s: &str) -> Option<String> {
    let s = s.trim();
    let stripped = s.strip_prefix('"')?;
    let end = stripped.find('"')?;
    Some(stripped[..end].to_string())
}

fn extract_inline_cte_alias_for_syn(after: &str) -> Option<String> {
    let alias = extract_string_arg_for_syn(after)?;
    let comma_pos = after.find(',')?;
    let rhs = after[comma_pos + 1..].trim_start();
    if rhs.starts_with("Qail::") {
        return Some(alias);
    }
    None
}

fn collect_columns_from_cmd(cmd: &crate::ast::Qail) -> Vec<String> {
    use crate::ast::Expr;
    let mut out = Vec::<String>::new();
    let mut seen = std::collections::HashSet::<String>::new();

    let mut push_col = |name: &str| {
        if name.contains('.') || name.contains('(') || name == "*" || name.is_empty() {
            return;
        }
        if seen.insert(name.to_string()) {
            out.push(name.to_string());
        }
    };

    for expr in &cmd.columns {
        match expr {
            Expr::Named(name) => push_col(name),
            Expr::Aliased { name, .. } => push_col(name),
            Expr::Aggregate { col, .. } => push_col(col),
            _ => {}
        }
    }
    for cage in &cmd.cages {
        // Skip Sort cages — ORDER BY can reference computed aliases
        // (e.g. count().alias("route_count")) that aren't physical columns.
        // The validator already skips Sort cages during validation (line 444).
        if matches!(cage.kind, crate::ast::CageKind::Sort(_)) {
            continue;
        }
        for cond in &cage.conditions {
            if let Expr::Named(name) = &cond.left {
                push_col(name);
            }
        }
    }
    for cond in &cmd.having {
        if let Expr::Named(name) = &cond.left {
            push_col(name);
        }
    }
    if let Some(returning) = &cmd.returning {
        for expr in returning {
            if let Expr::Named(name) = expr {
                push_col(name);
            }
        }
    }

    out
}

pub(crate) fn extract_syn_usages_from_source(source: &str) -> Vec<SynParsedUsage> {
    let Ok(syntax) = syn::parse_file(source) else {
        return Vec::new();
    };

    struct SynQailVisitor {
        usages: Vec<SynParsedUsage>,
        /// Track variable names bound to Qail chains: var_name → index into usages
        bindings: HashMap<String, usize>,
        /// Stack of function/method-level super-admin usage flags.
        super_admin_scopes: Vec<bool>,
    }

    impl SynQailVisitor {
        fn new() -> Self {
            Self {
                usages: Vec::new(),
                bindings: HashMap::new(),
                super_admin_scopes: Vec::new(),
            }
        }

        fn current_scope_uses_super_admin(&self) -> bool {
            self.super_admin_scopes.last().copied().unwrap_or(false)
        }

        fn with_function_scope<F>(&mut self, uses_super_admin: bool, mut visit: F)
        where
            F: FnMut(&mut Self),
        {
            let saved_bindings = std::mem::take(&mut self.bindings);
            self.super_admin_scopes.push(uses_super_admin);
            visit(self);
            self.super_admin_scopes.pop();
            self.bindings = saved_bindings;
        }

        /// Try to parse a method chain whose receiver is a known Qail variable.
        /// Returns the variable name and the method steps (excluding the variable itself).
        fn parse_chain_on_binding(&self, expr: &syn::Expr) -> Option<(String, Vec<SynMethodStep>)> {
            let mut steps = Vec::new();
            let mut cursor = expr;
            loop {
                match cursor {
                    syn::Expr::MethodCall(method) => {
                        steps.push(SynMethodStep {
                            name: method.method.to_string(),
                            args: method.args.iter().cloned().collect(),
                        });
                        cursor = &method.receiver;
                    }
                    syn::Expr::Path(path) => {
                        // Single-segment path = variable name
                        if path.path.segments.len() == 1 {
                            let var_name = path.path.segments[0].ident.to_string();
                            if self.bindings.contains_key(&var_name) && !steps.is_empty() {
                                steps.reverse();
                                return Some((var_name, steps));
                            }
                        }
                        return None;
                    }
                    syn::Expr::Paren(p) => cursor = &p.expr,
                    syn::Expr::Group(g) => cursor = &g.expr,
                    syn::Expr::Reference(r) => cursor = &r.expr,
                    syn::Expr::Await(a) => cursor = &a.base,
                    syn::Expr::Try(t) => cursor = &t.expr,
                    _ => return None,
                }
            }
        }

        /// Record a let-binding or assignment RHS as a Qail chain.
        fn try_record_binding(&mut self, var_name: &str, init_expr: &syn::Expr) {
            // Case 1: RHS is a full Qail chain (e.g. Qail::get("x").eq("y", v))
            if let Some(parsed) = parse_qail_chain_from_expr(init_expr) {
                let idx = self.usages.len();
                let mut parsed = parsed;
                parsed.scope_uses_super_admin = self.current_scope_uses_super_admin();
                self.usages.push(parsed);
                self.bindings.insert(var_name.to_string(), idx);
                return;
            }
            // Case 2: RHS is a method chain on an existing binding (e.g. cmd.eq("y", v))
            if let Some((source_var, steps)) = self.parse_chain_on_binding(init_expr)
                && let Some(&src_idx) = self.bindings.get(&source_var)
            {
                // Clone the source usage and extend it
                let mut new_usage = self.usages[src_idx].clone();
                for step in steps {
                    apply_syn_method_step(
                        &mut new_usage.cmd,
                        &step.name,
                        &step.args,
                        &mut new_usage.has_rls,
                    );
                }
                new_usage.score = syn_cmd_score(&new_usage.cmd, new_usage.has_rls);
                let idx = self.usages.len();
                self.usages.push(new_usage);
                self.bindings.insert(var_name.to_string(), idx);
            }
        }
    }

    impl<'ast> Visit<'ast> for SynQailVisitor {
        fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
            let uses_super_admin = block_uses_for_system_process(&node.block);
            self.with_function_scope(uses_super_admin, |this| {
                syn::visit::visit_block(this, &node.block);
            });
        }

        fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
            let uses_super_admin = block_uses_for_system_process(&node.block);
            self.with_function_scope(uses_super_admin, |this| {
                syn::visit::visit_block(this, &node.block);
            });
        }

        fn visit_expr(&mut self, node: &'ast syn::Expr) {
            // Handle assignment: cmd = cmd.eq("col", v) or cmd = Qail::get("x")
            if let syn::Expr::Assign(assign) = node {
                // Extract variable name from LHS
                if let syn::Expr::Path(path) = &*assign.left
                    && path.path.segments.len() == 1
                {
                    let var_name = path.path.segments[0].ident.to_string();
                    self.try_record_binding(&var_name, &assign.right);
                    // Still recurse into sub-expressions
                    syn::visit::visit_expr(self, node);
                    return;
                }
            }

            // Normal chain detection (direct chains like Qail::get("x").eq("y", v))
            if let Some(parsed) = parse_qail_chain_from_expr(node) {
                let mut parsed = parsed;
                parsed.scope_uses_super_admin = self.current_scope_uses_super_admin();
                self.usages.push(parsed);
            }
            syn::visit::visit_expr(self, node);
        }

        fn visit_local(&mut self, local: &'ast syn::Local) {
            // Handle: let cmd = Qail::get("x").eq("y", v);
            //         let cmd = existing_cmd.filter("z", v);
            if let Some(init) = &local.init
                && let syn::Pat::Ident(pat_ident) = &local.pat
            {
                let var_name = pat_ident.ident.to_string();
                self.try_record_binding(&var_name, &init.expr);
            }
            // Continue visiting child expressions
            syn::visit::visit_local(self, local);
        }
    }

    let mut visitor = SynQailVisitor::new();
    visitor.visit_file(&syntax);

    // Deduplicate: for variables that were bound+reassigned, only keep the
    // most complete (highest-scoring) usage per (line, action, table) key.
    // The bindings map points to the final state, but intermediate states
    // were also pushed. The build_syn_usage_index deduplicates by score,
    // so we can just return all usages and let the index pick the best.
    visitor.usages
}

fn block_uses_for_system_process(block: &syn::Block) -> bool {
    struct Finder {
        found: bool,
    }

    impl<'ast> Visit<'ast> for Finder {
        fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
            if self.found {
                return;
            }
            if let syn::Expr::Path(path_expr) = &*node.func
                && path_expr
                    .path
                    .segments
                    .iter()
                    .any(|segment| segment.ident == "for_system_process")
            {
                self.found = true;
                return;
            }
            syn::visit::visit_expr_call(self, node);
        }
    }

    let mut finder = Finder { found: false };
    finder.visit_block(block);
    finder.found
}

fn parse_qail_chain_from_expr(expr: &syn::Expr) -> Option<SynParsedUsage> {
    let mut steps = Vec::<SynMethodStep>::new();
    let mut cursor = expr;

    loop {
        match cursor {
            syn::Expr::MethodCall(method) => {
                steps.push(SynMethodStep {
                    name: method.method.to_string(),
                    args: method.args.iter().cloned().collect(),
                });
                cursor = &method.receiver;
            }
            syn::Expr::Call(call) => {
                let ctor = parse_qail_constructor(call)?;
                steps.reverse();

                let mut cmd = crate::ast::Qail {
                    action: ctor.ast_action,
                    table: ctor.table.clone(),
                    ..Default::default()
                };
                let mut has_rls = false;

                for step in steps {
                    apply_syn_method_step(&mut cmd, &step.name, &step.args, &mut has_rls);
                }

                let score = syn_cmd_score(&cmd, has_rls);
                return Some(SynParsedUsage {
                    line: ctor.line,
                    column: ctor.column,
                    action: ctor.action,
                    table: ctor.table,
                    is_dynamic_table: ctor.is_dynamic_table,
                    cmd,
                    has_rls,
                    scope_uses_super_admin: false,
                    score,
                });
            }
            syn::Expr::Paren(paren) => cursor = &paren.expr,
            syn::Expr::Group(group) => cursor = &group.expr,
            syn::Expr::Reference(reference) => cursor = &reference.expr,
            syn::Expr::Await(await_expr) => cursor = &await_expr.base,
            syn::Expr::Try(try_expr) => cursor = &try_expr.expr,
            _ => return None,
        }
    }
}

fn parse_qail_constructor(call: &syn::ExprCall) -> Option<SynConstructor> {
    let syn::Expr::Path(path_expr) = &*call.func else {
        return None;
    };

    let ctor = qail_constructor_name(&path_expr.path)?;
    let first_arg = call.args.first()?;

    let (action, ast_action, table, is_dynamic_table) = match ctor.as_str() {
        "get" => {
            let (table, is_dynamic) = parse_table_name_from_expr(first_arg)?;
            (
                "GET".to_string(),
                crate::ast::Action::Get,
                table,
                is_dynamic,
            )
        }
        "add" => {
            let (table, is_dynamic) = parse_table_name_from_expr(first_arg)?;
            (
                "ADD".to_string(),
                crate::ast::Action::Add,
                table,
                is_dynamic,
            )
        }
        "set" => {
            let (table, is_dynamic) = parse_table_name_from_expr(first_arg)?;
            (
                "SET".to_string(),
                crate::ast::Action::Set,
                table,
                is_dynamic,
            )
        }
        "del" => {
            let (table, is_dynamic) = parse_table_name_from_expr(first_arg)?;
            (
                "DEL".to_string(),
                crate::ast::Action::Del,
                table,
                is_dynamic,
            )
        }
        "put" => {
            let (table, is_dynamic) = parse_table_name_from_expr(first_arg)?;
            (
                "PUT".to_string(),
                crate::ast::Action::Put,
                table,
                is_dynamic,
            )
        }
        "typed" => (
            "TYPED".to_string(),
            crate::ast::Action::Get,
            parse_typed_table_from_expr(first_arg)?,
            false,
        ),
        // "raw_sql" and any unknown constructors are not validated
        _ => return None,
    };

    Some(SynConstructor {
        line: call.span().start().line,
        column: call.span().start().column + 1,
        action,
        ast_action,
        table,
        is_dynamic_table,
    })
}

fn qail_constructor_name(path: &syn::Path) -> Option<String> {
    let mut segments = path.segments.iter().map(|s| s.ident.to_string());
    let first = segments.next()?;
    let mut prev = first;
    for segment in segments {
        if prev == "Qail" {
            return Some(segment.to_ascii_lowercase());
        }
        prev = segment;
    }
    None
}

fn parse_typed_table_from_expr(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::Path(path_expr) => {
            let segments: Vec<_> = path_expr
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            match segments.len() {
                0 => None,
                1 => Some(segments[0].to_ascii_lowercase()),
                _ => Some(segments[segments.len() - 2].to_ascii_lowercase()),
            }
        }
        syn::Expr::Reference(reference) => parse_typed_table_from_expr(&reference.expr),
        syn::Expr::Paren(paren) => parse_typed_table_from_expr(&paren.expr),
        syn::Expr::Group(group) => parse_typed_table_from_expr(&group.expr),
        syn::Expr::MethodCall(method) if method.method == "into" => {
            parse_typed_table_from_expr(&method.receiver)
        }
        _ => None,
    }
}

fn parse_table_name_from_expr(expr: &syn::Expr) -> Option<(String, bool)> {
    if let Some(lit) = parse_string_from_expr(expr) {
        return Some((lit, false));
    }
    parse_dynamic_table_name_from_expr(expr).map(|name| (name, true))
}

fn parse_dynamic_table_name_from_expr(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::Path(path_expr) => Some(
            path_expr
                .path
                .segments
                .last()
                .map(|s| s.ident.to_string())?,
        ),
        syn::Expr::Field(field) => match &field.member {
            syn::Member::Named(named) => Some(named.to_string()),
            syn::Member::Unnamed(_) => None,
        },
        syn::Expr::Reference(reference) => parse_dynamic_table_name_from_expr(&reference.expr),
        syn::Expr::Paren(paren) => parse_dynamic_table_name_from_expr(&paren.expr),
        syn::Expr::Group(group) => parse_dynamic_table_name_from_expr(&group.expr),
        syn::Expr::MethodCall(method)
            if method.method == "into" || method.method == "to_string" =>
        {
            parse_dynamic_table_name_from_expr(&method.receiver)
        }
        syn::Expr::Call(call) => {
            let syn::Expr::Path(path_expr) = &*call.func else {
                return None;
            };
            let tail = path_expr.path.segments.last()?.ident.to_string();
            if tail == "String" || tail == "from" || tail == "new" {
                return call
                    .args
                    .first()
                    .and_then(parse_dynamic_table_name_from_expr);
            }
            None
        }
        _ => None,
    }
}

fn parse_string_from_expr(expr: &syn::Expr) -> Option<String> {
    match expr {
        syn::Expr::Lit(lit) => match &lit.lit {
            syn::Lit::Str(s) => Some(s.value()),
            _ => None,
        },
        syn::Expr::Reference(reference) => parse_string_from_expr(&reference.expr),
        syn::Expr::Paren(paren) => parse_string_from_expr(&paren.expr),
        syn::Expr::Group(group) => parse_string_from_expr(&group.expr),
        syn::Expr::MethodCall(method)
            if method.method == "into" || method.method == "to_string" =>
        {
            parse_string_from_expr(&method.receiver)
        }
        syn::Expr::Call(call) => {
            let syn::Expr::Path(path_expr) = &*call.func else {
                return None;
            };
            let tail = path_expr.path.segments.last()?.ident.to_string();
            if tail == "from" || tail == "new" || tail == "String" {
                return call.args.first().and_then(parse_string_from_expr);
            }
            None
        }
        _ => None,
    }
}

fn parse_string_list_from_expr(expr: &syn::Expr) -> Vec<String> {
    match expr {
        syn::Expr::Array(arr) => arr
            .elems
            .iter()
            .filter_map(parse_string_from_expr)
            .collect(),
        syn::Expr::Reference(reference) => parse_string_list_from_expr(&reference.expr),
        syn::Expr::Paren(paren) => parse_string_list_from_expr(&paren.expr),
        syn::Expr::Group(group) => parse_string_list_from_expr(&group.expr),
        syn::Expr::Macro(mac) if mac.mac.path.is_ident("vec") => {
            if let Ok(arr) = syn::parse2::<syn::ExprArray>(mac.mac.tokens.clone()) {
                return arr
                    .elems
                    .iter()
                    .filter_map(parse_string_from_expr)
                    .collect();
            }
            Vec::new()
        }
        _ => parse_string_from_expr(expr).into_iter().collect(),
    }
}

fn parse_operator_from_expr(expr: &syn::Expr) -> Option<crate::ast::Operator> {
    let syn::Expr::Path(path_expr) = expr else {
        return None;
    };
    let name = path_expr.path.segments.last()?.ident.to_string();
    Some(match name.as_str() {
        "Eq" => crate::ast::Operator::Eq,
        "Ne" => crate::ast::Operator::Ne,
        "Gt" => crate::ast::Operator::Gt,
        "Gte" => crate::ast::Operator::Gte,
        "Lt" => crate::ast::Operator::Lt,
        "Lte" => crate::ast::Operator::Lte,
        "Like" => crate::ast::Operator::Like,
        "ILike" => crate::ast::Operator::ILike,
        "IsNull" => crate::ast::Operator::IsNull,
        "IsNotNull" => crate::ast::Operator::IsNotNull,
        "In" => crate::ast::Operator::In,
        _ => return None,
    })
}

fn parse_sort_order_from_expr(expr: &syn::Expr) -> Option<crate::ast::SortOrder> {
    let syn::Expr::Path(path_expr) = expr else {
        return None;
    };
    let name = path_expr.path.segments.last()?.ident.to_string();
    Some(match name.as_str() {
        "Asc" => crate::ast::SortOrder::Asc,
        "Desc" => crate::ast::SortOrder::Desc,
        "AscNullsFirst" => crate::ast::SortOrder::AscNullsFirst,
        "AscNullsLast" => crate::ast::SortOrder::AscNullsLast,
        "DescNullsFirst" => crate::ast::SortOrder::DescNullsFirst,
        "DescNullsLast" => crate::ast::SortOrder::DescNullsLast,
        _ => return None,
    })
}

fn parse_join_kind_from_expr(expr: &syn::Expr) -> Option<crate::ast::JoinKind> {
    let syn::Expr::Path(path_expr) = expr else {
        return None;
    };
    let name = path_expr.path.segments.last()?.ident.to_string();
    Some(match name.as_str() {
        "Inner" => crate::ast::JoinKind::Inner,
        "Left" => crate::ast::JoinKind::Left,
        "Right" => crate::ast::JoinKind::Right,
        "Lateral" => crate::ast::JoinKind::Lateral,
        "Full" => crate::ast::JoinKind::Full,
        "Cross" => crate::ast::JoinKind::Cross,
        _ => return None,
    })
}

fn parse_value_ctor_call(call: &syn::ExprCall) -> Option<crate::ast::Value> {
    let syn::Expr::Path(path_expr) = &*call.func else {
        return None;
    };
    let segments: Vec<String> = path_expr
        .path
        .segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect();
    if segments.len() < 2 || segments[segments.len() - 2] != "Value" {
        return None;
    }

    let ctor = segments.last()?.as_str();
    let first = call.args.first();

    use crate::ast::Value;
    Some(match ctor {
        "Null" => Value::Null,
        "Bool" => match first {
            Some(syn::Expr::Lit(lit)) => match &lit.lit {
                syn::Lit::Bool(b) => Value::Bool(b.value),
                _ => return None,
            },
            _ => return None,
        },
        "Int" => match first {
            Some(syn::Expr::Lit(lit)) => match &lit.lit {
                syn::Lit::Int(i) => i
                    .base10_parse::<i64>()
                    .map(Value::Int)
                    .unwrap_or(Value::Null),
                _ => return None,
            },
            _ => return None,
        },
        "Float" => match first {
            Some(syn::Expr::Lit(lit)) => match &lit.lit {
                syn::Lit::Float(f) => f
                    .base10_parse::<f64>()
                    .map(Value::Float)
                    .unwrap_or(Value::Null),
                _ => return None,
            },
            _ => return None,
        },
        "String" => Value::String(first.and_then(parse_string_from_expr)?),
        "Column" => Value::Column(first.and_then(parse_string_from_expr)?),
        "Array" => Value::Array(match first {
            Some(expr) => match parse_value_from_expr(expr) {
                Value::Array(arr) => arr,
                single => vec![single],
            },
            None => vec![],
        }),
        _ => return None,
    })
}

fn parse_value_from_expr(expr: &syn::Expr) -> crate::ast::Value {
    use crate::ast::Value;

    match expr {
        syn::Expr::Lit(lit) => match &lit.lit {
            syn::Lit::Bool(b) => Value::Bool(b.value),
            syn::Lit::Int(i) => i
                .base10_parse::<i64>()
                .map(Value::Int)
                .unwrap_or(Value::Null),
            syn::Lit::Float(f) => f
                .base10_parse::<f64>()
                .map(Value::Float)
                .unwrap_or(Value::Null),
            syn::Lit::Str(s) => Value::String(s.value()),
            _ => Value::Null,
        },
        syn::Expr::Array(arr) => {
            Value::Array(arr.elems.iter().map(parse_value_from_expr).collect())
        }
        syn::Expr::Reference(reference) => parse_value_from_expr(&reference.expr),
        syn::Expr::Paren(paren) => parse_value_from_expr(&paren.expr),
        syn::Expr::Group(group) => parse_value_from_expr(&group.expr),
        syn::Expr::MethodCall(method) if method.method == "into" => {
            parse_value_from_expr(&method.receiver)
        }
        syn::Expr::Call(call) => {
            if let Some(value) = parse_value_ctor_call(call) {
                return value;
            }
            let syn::Expr::Path(path_expr) = &*call.func else {
                return Value::Null;
            };
            let tail = path_expr
                .path
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .unwrap_or_default();
            if tail == "Some" {
                return call
                    .args
                    .first()
                    .map(parse_value_from_expr)
                    .unwrap_or(Value::Null);
            }
            Value::Null
        }
        syn::Expr::Path(_path_expr) => Value::Null,
        _ => Value::Null,
    }
}

fn parse_expr_node(expr: &syn::Expr) -> Option<crate::ast::Expr> {
    match expr {
        syn::Expr::Lit(lit) => match &lit.lit {
            syn::Lit::Str(s) => Some(crate::ast::Expr::Named(s.value())),
            _ => None,
        },
        syn::Expr::Reference(reference) => parse_expr_node(&reference.expr),
        syn::Expr::Paren(paren) => parse_expr_node(&paren.expr),
        syn::Expr::Group(group) => parse_expr_node(&group.expr),
        syn::Expr::MethodCall(method) if method.method == "into" => {
            parse_expr_node(&method.receiver)
        }
        syn::Expr::Call(call) => {
            let syn::Expr::Path(path_expr) = &*call.func else {
                return None;
            };
            let segments: Vec<String> = path_expr
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            let tail = segments.last()?.as_str();
            if tail == "Named" && segments.len() >= 2 && segments[segments.len() - 2] == "Expr" {
                return call
                    .args
                    .first()
                    .and_then(parse_string_from_expr)
                    .map(crate::ast::Expr::Named);
            }
            if tail == "col" {
                return call
                    .args
                    .first()
                    .and_then(parse_string_from_expr)
                    .map(crate::ast::Expr::Named);
            }
            None
        }
        _ => None,
    }
}

fn parse_condition_from_expr(expr: &syn::Expr) -> Option<crate::ast::Condition> {
    let syn::Expr::Struct(cond_struct) = expr else {
        return None;
    };
    let struct_name = cond_struct.path.segments.last()?.ident.to_string();
    if struct_name != "Condition" {
        return None;
    }

    let mut left = None;
    let mut op = None;
    let mut value = None;
    let mut is_array_unnest = false;

    for field in &cond_struct.fields {
        let syn::Member::Named(name) = &field.member else {
            continue;
        };
        match name.to_string().as_str() {
            "left" => left = parse_expr_node(&field.expr),
            "op" => op = parse_operator_from_expr(&field.expr),
            "value" => value = Some(parse_value_from_expr(&field.expr)),
            "is_array_unnest" => {
                if let syn::Expr::Lit(lit) = &field.expr
                    && let syn::Lit::Bool(v) = &lit.lit
                {
                    is_array_unnest = v.value;
                }
            }
            _ => {}
        }
    }

    Some(crate::ast::Condition {
        left: left?,
        op: op?,
        value: value.unwrap_or(crate::ast::Value::Null),
        is_array_unnest,
    })
}

fn parse_condition_list(expr: &syn::Expr) -> Vec<crate::ast::Condition> {
    match expr {
        syn::Expr::Array(arr) => arr
            .elems
            .iter()
            .filter_map(parse_condition_from_expr)
            .collect(),
        syn::Expr::Reference(reference) => parse_condition_list(&reference.expr),
        syn::Expr::Paren(paren) => parse_condition_list(&paren.expr),
        syn::Expr::Group(group) => parse_condition_list(&group.expr),
        syn::Expr::Macro(mac) if mac.mac.path.is_ident("vec") => {
            if let Ok(arr) = syn::parse2::<syn::ExprArray>(mac.mac.tokens.clone()) {
                return arr
                    .elems
                    .iter()
                    .filter_map(parse_condition_from_expr)
                    .collect();
            }
            Vec::new()
        }
        _ => parse_condition_from_expr(expr).into_iter().collect(),
    }
}

fn push_filter_condition(cmd: &mut crate::ast::Qail, condition: crate::ast::Condition) {
    if let Some(cage) = cmd
        .cages
        .iter_mut()
        .find(|c| matches!(c.kind, crate::ast::CageKind::Filter))
    {
        cage.conditions.push(condition);
    } else {
        cmd.cages.push(crate::ast::Cage {
            kind: crate::ast::CageKind::Filter,
            conditions: vec![condition],
            logical_op: crate::ast::LogicalOp::And,
        });
    }
}

fn push_payload_condition(cmd: &mut crate::ast::Qail, condition: crate::ast::Condition) {
    if let Some(cage) = cmd
        .cages
        .iter_mut()
        .find(|c| matches!(c.kind, crate::ast::CageKind::Payload))
    {
        cage.conditions.push(condition);
    } else {
        cmd.cages.push(crate::ast::Cage {
            kind: crate::ast::CageKind::Payload,
            conditions: vec![condition],
            logical_op: crate::ast::LogicalOp::And,
        });
    }
}

fn normalize_join_table(table: &str) -> String {
    table.split_whitespace().next().unwrap_or(table).to_string()
}

pub(crate) fn apply_syn_method_step(
    cmd: &mut crate::ast::Qail,
    method: &str,
    args: &[syn::Expr],
    has_rls: &mut bool,
) {
    use crate::ast::{Condition, Expr, Join, JoinKind, Operator, SortOrder, Value};

    match method {
        "with_rls" | "rls" => {
            *has_rls = true;
        }
        "column" => {
            if let Some(col) = args.first().and_then(parse_string_from_expr) {
                cmd.columns.push(Expr::Named(col));
            }
        }
        "columns" => {
            if let Some(arg) = args.first() {
                cmd.columns.extend(
                    parse_string_list_from_expr(arg)
                        .into_iter()
                        .map(Expr::Named),
                );
            }
        }
        "returning" => {
            if let Some(arg) = args.first() {
                let cols: Vec<Expr> = parse_string_list_from_expr(arg)
                    .into_iter()
                    .map(Expr::Named)
                    .collect();
                if !cols.is_empty() {
                    match &mut cmd.returning {
                        Some(existing) => existing.extend(cols),
                        None => cmd.returning = Some(cols),
                    }
                }
            }
        }
        "returning_all" => {
            cmd.returning = Some(vec![Expr::Star]);
        }
        "filter" => {
            if args.len() >= 3
                && let Some(column) = parse_string_from_expr(&args[0])
            {
                let op = parse_operator_from_expr(&args[1]).unwrap_or(Operator::Eq);
                let value = parse_value_from_expr(&args[2]);
                push_filter_condition(
                    cmd,
                    Condition {
                        left: Expr::Named(column),
                        op,
                        value,
                        is_array_unnest: false,
                    },
                );
            }
        }
        "where_eq" | "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "like" | "ilike" | "in_vals"
        | "is_null" | "is_not_null" => {
            if let Some(column) = args.first().and_then(parse_string_from_expr) {
                let (op, value) = match method {
                    "where_eq" | "eq" => (
                        Operator::Eq,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "ne" => (
                        Operator::Ne,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "gt" => (
                        Operator::Gt,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "gte" => (
                        Operator::Gte,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "lt" => (
                        Operator::Lt,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "lte" => (
                        Operator::Lte,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "like" => (
                        Operator::Like,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "ilike" => (
                        Operator::ILike,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Null),
                    ),
                    "in_vals" => (
                        Operator::In,
                        args.get(1)
                            .map(parse_value_from_expr)
                            .unwrap_or(Value::Array(vec![])),
                    ),
                    "is_null" => (Operator::IsNull, Value::Null),
                    "is_not_null" => (Operator::IsNotNull, Value::Null),
                    _ => (Operator::Eq, Value::Null),
                };

                push_filter_condition(
                    cmd,
                    Condition {
                        left: Expr::Named(column),
                        op,
                        value,
                        is_array_unnest: false,
                    },
                );
            }
        }
        "order_by" => {
            if let Some(column) = args.first().and_then(parse_string_from_expr) {
                let order = args
                    .get(1)
                    .and_then(parse_sort_order_from_expr)
                    .unwrap_or(SortOrder::Asc);
                cmd.cages.push(crate::ast::Cage {
                    kind: crate::ast::CageKind::Sort(order),
                    conditions: vec![Condition {
                        left: Expr::Named(column),
                        op: Operator::Eq,
                        value: Value::Null,
                        is_array_unnest: false,
                    }],
                    logical_op: crate::ast::LogicalOp::And,
                });
            }
        }
        "order_desc" | "order_asc" => {
            if let Some(column) = args.first().and_then(parse_string_from_expr) {
                let order = if method == "order_desc" {
                    SortOrder::Desc
                } else {
                    SortOrder::Asc
                };
                cmd.cages.push(crate::ast::Cage {
                    kind: crate::ast::CageKind::Sort(order),
                    conditions: vec![Condition {
                        left: Expr::Named(column),
                        op: Operator::Eq,
                        value: Value::Null,
                        is_array_unnest: false,
                    }],
                    logical_op: crate::ast::LogicalOp::And,
                });
            }
        }
        "group_by" => {
            if let Some(arg) = args.first() {
                let cols = parse_string_list_from_expr(arg);
                if !cols.is_empty() {
                    cmd.cages.push(crate::ast::Cage {
                        kind: crate::ast::CageKind::Partition,
                        conditions: cols
                            .into_iter()
                            .map(|c| Condition {
                                left: Expr::Named(c),
                                op: Operator::Eq,
                                value: Value::Null,
                                is_array_unnest: false,
                            })
                            .collect(),
                        logical_op: crate::ast::LogicalOp::And,
                    });
                }
            }
        }
        "having_cond" => {
            if let Some(arg) = args.first()
                && let Some(condition) = parse_condition_from_expr(arg)
            {
                cmd.having.push(condition);
            }
        }
        "having_conds" => {
            if let Some(arg) = args.first() {
                cmd.having.extend(parse_condition_list(arg));
            }
        }
        "join" => {
            if args.len() >= 4
                && let Some((table, _)) = args.get(1).and_then(parse_table_name_from_expr)
            {
                let kind = args
                    .first()
                    .and_then(parse_join_kind_from_expr)
                    .unwrap_or(JoinKind::Left);
                let on = match (
                    args.get(2).and_then(parse_string_from_expr),
                    args.get(3).and_then(parse_string_from_expr),
                ) {
                    (Some(left_col), Some(right_col)) => Some(vec![Condition {
                        left: Expr::Named(left_col),
                        op: Operator::Eq,
                        value: Value::Column(right_col),
                        is_array_unnest: false,
                    }]),
                    _ => None,
                };
                cmd.joins.push(Join {
                    kind,
                    table: normalize_join_table(&table),
                    on,
                    on_true: false,
                });
            }
        }
        "left_join" | "inner_join" | "right_join" | "full_join" => {
            if args.len() >= 3
                && let Some((table, _)) = args.first().and_then(parse_table_name_from_expr)
            {
                let kind = match method {
                    "inner_join" => JoinKind::Inner,
                    "right_join" => JoinKind::Right,
                    "full_join" => JoinKind::Full,
                    _ => JoinKind::Left,
                };
                let on = match (
                    args.get(1).and_then(parse_string_from_expr),
                    args.get(2).and_then(parse_string_from_expr),
                ) {
                    (Some(left_col), Some(right_col)) => Some(vec![Condition {
                        left: Expr::Named(left_col),
                        op: Operator::Eq,
                        value: Value::Column(right_col),
                        is_array_unnest: false,
                    }]),
                    _ => None,
                };
                cmd.joins.push(Join {
                    kind,
                    table: normalize_join_table(&table),
                    on,
                    on_true: false,
                });
            }
        }
        "join_on" | "join_on_optional" => {
            if let Some((table, _)) = args.first().and_then(parse_table_name_from_expr) {
                cmd.joins.push(Join {
                    kind: JoinKind::Left,
                    table: normalize_join_table(&table),
                    on: None,
                    on_true: false,
                });
            }
        }
        "left_join_as" | "inner_join_as" => {
            if args.len() >= 4
                && let Some((table, _)) = args.first().and_then(parse_table_name_from_expr)
            {
                let kind = if method == "inner_join_as" {
                    JoinKind::Inner
                } else {
                    JoinKind::Left
                };
                let on = match (
                    args.get(2).and_then(parse_string_from_expr),
                    args.get(3).and_then(parse_string_from_expr),
                ) {
                    (Some(left_col), Some(right_col)) => Some(vec![Condition {
                        left: Expr::Named(left_col),
                        op: Operator::Eq,
                        value: Value::Column(right_col),
                        is_array_unnest: false,
                    }]),
                    _ => None,
                };
                cmd.joins.push(Join {
                    kind,
                    table: normalize_join_table(&table),
                    on,
                    on_true: false,
                });
            }
        }
        "join_conds" | "left_join_conds" | "inner_join_conds" => {
            let (kind, table_idx, cond_idx) = match method {
                "join_conds" => (
                    args.first()
                        .and_then(parse_join_kind_from_expr)
                        .unwrap_or(JoinKind::Left),
                    1,
                    2,
                ),
                "inner_join_conds" => (JoinKind::Inner, 0, 1),
                _ => (JoinKind::Left, 0, 1),
            };

            if let Some(table_expr) = args.get(table_idx)
                && let Some((table, _)) = parse_table_name_from_expr(table_expr)
            {
                let conditions = args
                    .get(cond_idx)
                    .map(parse_condition_list)
                    .unwrap_or_default();
                cmd.joins.push(Join {
                    kind,
                    table: normalize_join_table(&table),
                    on: if conditions.is_empty() {
                        None
                    } else {
                        Some(conditions)
                    },
                    on_true: false,
                });
            }
        }
        "set_value" | "set_coalesce" | "set_coalesce_opt" => {
            if let Some(column) = args.first().and_then(parse_string_from_expr) {
                let value = args
                    .get(1)
                    .map(parse_value_from_expr)
                    .unwrap_or(Value::Null);
                push_payload_condition(
                    cmd,
                    Condition {
                        left: Expr::Named(column),
                        op: Operator::Eq,
                        value,
                        is_array_unnest: false,
                    },
                );
            }
        }
        _ => {}
    }
}

pub(crate) fn syn_cmd_score(cmd: &crate::ast::Qail, has_rls: bool) -> usize {
    let group_cols = cmd
        .cages
        .iter()
        .filter(|c| matches!(c.kind, crate::ast::CageKind::Partition))
        .map(|c| c.conditions.len())
        .sum::<usize>();
    let filter_cols = cmd
        .cages
        .iter()
        .filter(|c| matches!(c.kind, crate::ast::CageKind::Filter))
        .map(|c| c.conditions.len())
        .sum::<usize>();

    cmd.columns.len()
        + (cmd.joins.len() * 8)
        + (group_cols * 5)
        + (cmd.having.len() * 6)
        + filter_cols
        + cmd.returning.as_ref().map_or(0, |r| r.len() * 2)
        + usize::from(has_rls)
}
