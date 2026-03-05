//! Pass 1: Collect functions that contain query execution calls.
//!
//! Builds a call-graph index mapping function paths to whether they
//! directly or transitively execute database queries.

use std::collections::{HashMap, HashSet};
use syn::visit::Visit;

use super::patterns::{is_query_exec_method, resolve_called_path, current_module_path_from_segments, impl_context_key};

#[derive(Default, Clone)]
pub(super) struct FunctionCallInfo {
    direct_query_exec: bool,
    calls_by_path: HashSet<String>,
}

#[derive(Default, Clone)]
pub(super) struct QueryFunctionIndex {
    pub(super) paths: HashSet<String>,
}

pub(super) fn compute_query_index_from_infos(infos: &HashMap<String, FunctionCallInfo>) -> QueryFunctionIndex {
    let mut query_paths: HashSet<String> = infos
        .iter()
        .filter_map(|(name, info)| info.direct_query_exec.then_some(name.clone()))
        .collect();

    // Fixed-point closure over graph edges.
    loop {
        let mut changed = false;
        for (name, info) in infos {
            if query_paths.contains(name) {
                continue;
            }
            let calls_query_path = info.calls_by_path.iter().any(|p| query_paths.contains(p));
            if calls_query_path {
                query_paths.insert(name.clone());
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    QueryFunctionIndex { paths: query_paths }
}

pub(super) struct FunctionQueryCollector {
    infos: HashMap<String, FunctionCallInfo>,
    current_function: Option<String>,
    module_prefix: Vec<String>,
    module_stack: Vec<String>,
    impl_stack: Vec<String>,
}

impl FunctionQueryCollector {
    pub(super) fn new(module_prefix: Vec<String>) -> Self {
        Self {
            infos: HashMap::new(),
            current_function: None,
            module_prefix,
            module_stack: Vec::new(),
            impl_stack: Vec::new(),
        }
    }

    fn current_module_segments(&self) -> Vec<String> {
        let mut segs = self.module_prefix.clone();
        segs.extend(self.module_stack.iter().cloned());
        segs
    }

    fn current_module_path(&self) -> String {
        current_module_path_from_segments(&self.current_module_segments())
    }

    fn function_key(&self, fn_name: &str) -> String {
        if let Some(impl_ctx) = self.impl_stack.last() {
            format!("{}::{}::{}", self.current_module_path(), impl_ctx, fn_name)
        } else {
            format!("{}::{}", self.current_module_path(), fn_name)
        }
    }

    fn ensure_current_entry(&mut self) {
        if let Some(name) = self.current_function.as_ref() {
            self.infos.entry(name.clone()).or_default();
        }
    }

    fn mark_direct_query_exec(&mut self) {
        if let Some(name) = self.current_function.as_ref() {
            self.infos
                .entry(name.clone())
                .or_default()
                .direct_query_exec = true;
        }
    }

    fn add_call_edge_path(&mut self, callee_path: String) {
        if let Some(name) = self.current_function.as_ref() {
            self.infos
                .entry(name.clone())
                .or_default()
                .calls_by_path
                .insert(callee_path);
        }
    }

    pub(super) fn compute_query_index(&self) -> QueryFunctionIndex {
        compute_query_index_from_infos(&self.infos)
    }

    pub(super) fn merge_into(&self, out: &mut HashMap<String, FunctionCallInfo>) {
        for (name, info) in &self.infos {
            let entry = out.entry(name.clone()).or_default();
            entry.direct_query_exec |= info.direct_query_exec;
            entry
                .calls_by_path
                .extend(info.calls_by_path.iter().cloned());
        }
    }
}

impl<'ast> Visit<'ast> for FunctionQueryCollector {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if let Some((_, items)) = &node.content {
            self.module_stack.push(node.ident.to_string());
            for item in items {
                self.visit_item(item);
            }
            self.module_stack.pop();
        }
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        let prev_fn = self.current_function.take();

        self.current_function = Some(self.function_key(&node.sig.ident.to_string()));
        self.ensure_current_entry();

        syn::visit::visit_item_fn(self, node);

        self.current_function = prev_fn;
    }

    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        self.impl_stack.push(impl_context_key(node));
        syn::visit::visit_item_impl(self, node);
        self.impl_stack.pop();
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        let prev_fn = self.current_function.take();

        self.current_function = Some(self.function_key(&node.sig.ident.to_string()));
        self.ensure_current_entry();

        syn::visit::visit_impl_item_fn(self, node);

        self.current_function = prev_fn;
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let method_name = node.method.to_string();
        if is_query_exec_method(&method_name) {
            self.mark_direct_query_exec();
        } else if matches!(&*node.receiver, syn::Expr::Path(p) if p.path.is_ident("self")) {
            // Method-to-method edge inside impl blocks, e.g. self.load_user(...)
            self.add_call_edge_path(self.function_key(&node.method.to_string()));
        }
        syn::visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast syn::ExprCall) {
        if let syn::Expr::Path(path_expr) = &*node.func
            && let Some(path) =
                resolve_called_path(&path_expr.path, &self.current_module_segments())
        {
            // Free/qualified function call edge, e.g. helpers::load_user(...)
            self.add_call_edge_path(path);
        }
        syn::visit::visit_expr_call(self, node);
    }
}
