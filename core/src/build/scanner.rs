//! Text-based source scanner for QAIL usage patterns.
#![cfg_attr(feature = "syn-scanner", allow(dead_code))]

use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Extracted QAIL usage from source code
#[derive(Debug)]
pub struct QailUsage {
    /// Source file path.
    pub file: String,
    /// Line number (1-indexed).
    pub line: usize,
    /// Column number (1-indexed) where `Qail::...` constructor starts.
    pub column: usize,
    /// Table name referenced.
    pub table: String,
    /// True when table name came from a dynamic expression instead of a
    /// compile-time string literal.
    pub is_dynamic_table: bool,
    /// Column names referenced.
    pub columns: Vec<String>,
    /// CRUD action (GET, SET, ADD, DEL, PUT).
    pub action: String,
    /// Whether this references a CTE rather than a real table.
    pub is_cte_ref: bool,
    /// Whether this query chain includes `.with_rls(` call
    pub has_rls: bool,
    /// Whether the containing file uses `SuperAdminToken::for_system_process()`.
    /// When true AND the queried table is tenant-scoped, the build emits a
    /// warning: the query may bypass tenant isolation.
    pub file_uses_super_admin: bool,
}

/// Scan Rust source files for QAIL usage patterns
pub fn scan_source_files(src_dir: &str) -> Vec<QailUsage> {
    #[cfg(feature = "syn-scanner")]
    {
        super::syn_analyzer::scan_source_files_syn(src_dir)
    }
    #[cfg(not(feature = "syn-scanner"))]
    {
        let mut usages = Vec::new();
        scan_directory(Path::new(src_dir), &mut usages);
        usages
    }
}

fn scan_directory(dir: &Path, usages: &mut Vec<QailUsage>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                scan_directory(&path, usages);
            } else if path.extension().is_some_and(|e| e == "rs")
                && let Ok(content) = fs::read_to_string(&path)
            {
                scan_file(&path.display().to_string(), &content, usages);
            }
        }
    }
}

/// Phase 1+2: Collect let-bindings that map variable names to string literal(s).
///
/// Handles:
///   `let table = "foo";`                                    → {"table": ["foo"]}
///   `let (table, col) = ("foo", "bar");`                    → {"table": ["foo"], "col": ["bar"]}
///   `let (table, col) = if cond { ("a", "x") } else { ("b", "y") };`
///                                                           → {"table": ["a", "b"], "col": ["x", "y"]}
///   `let table = if cond { "a" } else { "b" };`             → {"table": ["a", "b"]}
fn collect_let_bindings(content: &str) -> HashMap<String, Vec<String>> {
    let mut bindings: HashMap<String, Vec<String>> = HashMap::new();

    // Join all lines for multi-line let analysis
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();

        // Look for: let IDENT = "literal"
        // or:       let (IDENT, IDENT) = ...
        if let Some(rest) = line.strip_prefix("let ") {
            let rest = rest.trim();

            // Phase 1: Simple  let table = "literal";
            if let Some((var, rhs)) = parse_simple_let(rest) {
                if let Some(lit) = extract_string_arg(rhs.trim()) {
                    bindings.entry(var).or_default().push(lit);
                    i += 1;
                    continue;
                }

                // Phase 2: let table = if cond { "a" } else { "b" };
                let rhs = rhs.trim();
                if rhs.starts_with("if ") {
                    // Collect the full if/else expression, possibly spanning multiple lines
                    let mut full_expr = rhs.to_string();
                    let mut j = i + 1;
                    // Keep joining lines until we see the closing `;`
                    while j < lines.len() && !full_expr.contains(';') {
                        full_expr.push(' ');
                        full_expr.push_str(lines[j].trim());
                        j += 1;
                    }
                    let literals = extract_branch_literals(&full_expr);
                    if !literals.is_empty() {
                        bindings.entry(var).or_default().extend(literals);
                    }
                }
            }

            // Phase 2: Destructuring  let (table, col) = if cond { ("a", "x") } else { ("b", "y") };
            //          or             let (table, col) = ("a", "b");
            if rest.starts_with('(') {
                // Collect the full line (may span multiple lines)
                let mut full_line = line.to_string();
                let mut j = i + 1;
                while j < lines.len() && !full_line.contains(';') {
                    full_line.push(' ');
                    full_line.push_str(lines[j].trim());
                    j += 1;
                }

                if let Some(result) = parse_destructuring_let(&full_line) {
                    for (name, values) in result {
                        bindings.entry(name).or_default().extend(values);
                    }
                }
            }
        }

        i += 1;
    }

    bindings
}

/// Parse `ident = rest` from a let statement (after stripping `let `).
/// Returns (variable_name, right_hand_side).
fn parse_simple_let(s: &str) -> Option<(String, &str)> {
    // Must start with an ident char, not `(` (that's destructuring) or `mut`
    let s = s.strip_prefix("mut ").unwrap_or(s).trim();
    if s.starts_with('(') {
        return None;
    }

    // Extract identifier
    let ident: String = s
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if ident.is_empty() {
        return None;
    }

    // Skip optional type annotation  : Type
    let rest = s[ident.len()..].trim_start();
    let rest = if rest.starts_with(':') {
        // Skip past the type, find the `=`
        rest.find('=').map(|pos| &rest[pos..])?
    } else {
        rest
    };

    let rest = rest.strip_prefix('=')?.trim();
    Some((ident, rest))
}

/// Extract string literals from if/else branches.
/// Handles: `if cond { "a" } else { "b" }` → ["a", "b"]
fn extract_branch_literals(expr: &str) -> Vec<String> {
    let mut literals = Vec::new();

    // Find all `{ "literal" }` patterns in the expression
    let mut remaining = expr;
    while let Some(brace_pos) = remaining.find('{') {
        let inside = &remaining[brace_pos + 1..];
        if let Some(close_pos) = inside.find('}') {
            let block = inside[..close_pos].trim();
            // Check if block content is a simple string literal
            if let Some(lit) = extract_string_arg(block) {
                literals.push(lit);
            }
            remaining = &inside[close_pos + 1..];
        } else {
            break;
        }
    }

    literals
}

/// Parse destructuring let: `let (a, b) = ...;`
/// Returns vec of (name, possible_values) for each position.
fn parse_destructuring_let(line: &str) -> Option<Vec<(String, Vec<String>)>> {
    // Find `let (` or `let mut (`
    let rest = line.strip_prefix("let ")?.trim();
    let rest = rest.strip_prefix("mut ").unwrap_or(rest).trim();
    let rest = rest.strip_prefix('(')?;

    // Extract variable names from the tuple pattern
    let close_paren = rest.find(')')?;
    let names_str = &rest[..close_paren];
    let names: Vec<String> = names_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && !s.starts_with('_'))
        .collect();

    if names.is_empty() {
        return None;
    }

    // Find the RHS after `=`
    let after_pattern = &rest[close_paren + 1..];
    let eq_pos = after_pattern.find('=')?;
    let rhs = after_pattern[eq_pos + 1..].trim();

    // Case 1: Simple tuple  ("a", "b")
    if rhs.starts_with('(') {
        let values = extract_tuple_literals(rhs);
        if values.len() == names.len() {
            return Some(
                names
                    .into_iter()
                    .zip(values)
                    .map(|(n, v)| (n, vec![v]))
                    .collect(),
            );
        }
    }

    // Case 2: if/else  if cond { ("a", "x") } else { ("b", "y") }
    if rhs.starts_with("if ") {
        let mut all_tuples: Vec<Vec<String>> = Vec::new();

        // Extract tuples from each branch
        let mut remaining = rhs;
        while let Some(brace_pos) = remaining.find('{') {
            let inside = &remaining[brace_pos + 1..];
            if let Some(close_pos) = find_matching_brace(inside) {
                let block = inside[..close_pos].trim();
                // Try to extract a tuple from the block
                if block.starts_with('(') {
                    let values = extract_tuple_literals(block);
                    if values.len() == names.len() {
                        all_tuples.push(values);
                    }
                }
                remaining = &inside[close_pos + 1..];
            } else {
                break;
            }
        }

        if !all_tuples.is_empty() {
            let mut result: Vec<(String, Vec<String>)> =
                names.iter().map(|n| (n.clone(), Vec::new())).collect();

            for tuple in &all_tuples {
                for (i, val) in tuple.iter().enumerate() {
                    if i < result.len() {
                        result[i].1.push(val.clone());
                    }
                }
            }

            return Some(result);
        }
    }

    None
}

/// Extract string literals from a tuple: ("a", "b", "c") → ["a", "b", "c"]
fn extract_tuple_literals(s: &str) -> Vec<String> {
    let mut literals = Vec::new();
    let s = s.trim();
    let s = s.strip_prefix('(').unwrap_or(s);
    // Find the closing paren (handle nested parens)
    let content = if let Some(pos) = s.rfind(')') {
        &s[..pos]
    } else {
        s.trim_end_matches(';').trim_end_matches(')')
    };

    for part in content.split(',') {
        let part = part.trim();
        if let Some(lit) = extract_string_arg(part) {
            literals.push(lit);
        }
    }
    literals
}

/// Find the position of the matching `}` for the first `{`,
/// handling nested braces.
fn find_matching_brace(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, ch) in s.chars().enumerate() {
        match ch {
            '{' => depth += 1,
            '}' => {
                if depth == 0 {
                    return Some(i);
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    None
}

/// Count net open delimiters in a line: +1 for `(`, `[`, `{`, -1 for `)`, `]`, `}`.
/// Used by the chain scanner to continue joining lines across multi-line arguments.
pub(crate) fn count_net_delimiters(line: &str) -> i32 {
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut prev = '\0';
    for ch in line.chars() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
        } else if !in_string {
            match ch {
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                _ => {}
            }
        }
        prev = ch;
    }
    depth
}

fn find_next_pattern(
    line: &str,
    start: usize,
    patterns: &[(&str, &str)],
) -> Option<(usize, usize)> {
    let mut best: Option<(usize, usize)> = None;
    for (idx, (pattern, _)) in patterns.iter().enumerate() {
        if let Some(rel_pos) = line[start..].find(pattern) {
            let abs_pos = start + rel_pos;
            match best {
                Some((best_pos, _)) if best_pos <= abs_pos => {}
                _ => best = Some((abs_pos, idx)),
            }
        }
    }
    best
}

fn split_statement_fragment(line: &str, start: usize) -> (&str, usize) {
    let mut in_string = false;
    let mut prev = '\0';
    let mut depth: i32 = 0;

    for (idx, ch) in line[start..].char_indices() {
        if ch == '"' && prev != '\\' {
            in_string = !in_string;
        } else if !in_string {
            match ch {
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                ';' if depth == 0 => {
                    let end = start + idx;
                    return (&line[start..end], end + 1);
                }
                _ => {}
            }
        }
        prev = ch;
    }

    (&line[start..], line.len())
}

fn extract_inline_cte_alias(after: &str) -> Option<String> {
    let alias = extract_string_arg(after)?;
    let comma_pos = after.find(',')?;
    let rhs = after[comma_pos + 1..].trim_start();
    if rhs.starts_with("Qail::") {
        return Some(alias);
    }
    None
}

pub(crate) fn scan_file(file: &str, content: &str, usages: &mut Vec<QailUsage>) {
    // All CRUD patterns: GET=SELECT, ADD=INSERT, SET=UPDATE, DEL=DELETE, PUT=UPSERT
    // Also detect Qail::typed (compile-time safety) and Qail::raw_sql (advisory)
    let patterns = [
        ("Qail::get(", "GET"),
        ("Qail::add(", "ADD"),
        ("Qail::set(", "SET"),
        ("Qail::del(", "DEL"),
        ("Qail::put(", "PUT"),
        ("Qail::typed(", "TYPED"),
        ("Qail::raw_sql(", "RAW"),
    ];

    // Phase 1+2: Collect let-bindings that resolve variable → string literal(s)
    let let_bindings = collect_let_bindings(content);

    // ── File-level flags ─────────────────────────────────────────────
    // Detect SuperAdminToken::for_system_process() usage anywhere in file.
    // Files can opt out with `// qail:allow(super_admin)` comment.
    let file_has_allow_super_admin = content.contains("// qail:allow(super_admin)");
    let file_uses_super_admin =
        !file_has_allow_super_admin && content.contains("for_system_process(");

    // First pass: collect CTE alias names defined in clearly parseable patterns.
    // We intentionally avoid broad `.with("name", ...)` matching unless the
    // second arg is a Qail chain (`Qail::...`) to reduce false CTE refs.
    // Note: .with_cte(cte_var) takes a variable, not a string literal,
    // so we can't extract the alias name from source text.
    let mut file_cte_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    for line in content.lines() {
        let line = line.trim();
        // .to_cte("name")
        let mut search = 0usize;
        while let Some(pos) = line[search..].find(".to_cte(") {
            let abs = search + pos;
            let after = &line[abs + 8..];
            if let Some(name) = extract_string_arg(after) {
                file_cte_names.insert(name);
            }
            search = abs + 8;
        }
        // .with("name", Qail::...)
        search = 0;
        while let Some(pos) = line[search..].find(".with(") {
            let abs = search + pos;
            let after = &line[abs + 6..];
            if let Some(name) = extract_inline_cte_alias(after) {
                file_cte_names.insert(name);
            }
            search = abs + 6;
        }
    }

    // Second pass: detect Qail usage and mark CTE refs
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let raw_line = lines[i];
        let line = raw_line.trim();
        let leading_ws = raw_line.len().saturating_sub(raw_line.trim_start().len());
        let mut cursor = 0usize;
        while cursor < line.len() {
            let Some((pos, pattern_idx)) = find_next_pattern(line, cursor, &patterns) else {
                break;
            };
            let (pattern, action) = patterns[pattern_idx];
            let start_line = i + 1; // 1-indexed
            let start_column = leading_ws + pos + 1; // 1-indexed
            let (line_fragment, next_cursor) = split_statement_fragment(line, pos);
            let after = &line_fragment[pattern.len()..];

            let table = if action == "TYPED" {
                // Qail::typed(module::Table) — extract module name as table
                extract_typed_table_arg(after)
            } else {
                extract_string_arg(after)
            };

            if action == "RAW" {
                // raw_sql bypasses schema — emit advisory, don't validate
                println!(
                    "cargo:warning=QAIL: raw SQL at {}:{} — not schema-validated",
                    file, start_line
                );
                cursor = next_cursor;
                continue;
            }

            if let Some(table) = table {
                // Join continuation lines: lines starting with `.` OR
                // lines inside unclosed delimiters (multi-line arrays/args).
                let mut full_chain = line_fragment.to_string();
                let mut j = i + 1;
                let mut depth = count_net_delimiters(line_fragment);
                while j < lines.len() {
                    let next = lines[j].trim();
                    if next.starts_with('.') || depth > 0 {
                        full_chain.push(' ');
                        full_chain.push_str(next);
                        depth += count_net_delimiters(next);
                        j += 1;
                    } else if next.is_empty() {
                        j += 1; // Skip empty lines
                    } else {
                        break;
                    }
                }

                // Check if this table name is a CTE alias defined anywhere in the file.
                let is_cte_ref = file_cte_names.contains(&table);

                // Check if query chain includes .with_rls( or .rls(
                let has_rls = full_chain.contains(".with_rls(") || full_chain.contains(".rls(");

                // Extract column names from the full chain
                let columns = extract_columns(&full_chain);

                usages.push(QailUsage {
                    file: file.to_string(),
                    line: start_line,
                    column: start_column,
                    table,
                    is_dynamic_table: false,
                    columns,
                    action: action.to_string(),
                    is_cte_ref,
                    has_rls,
                    file_uses_super_admin,
                });

                if j > i + 1 {
                    i = j.saturating_sub(1);
                    break;
                }
            } else if action != "TYPED" {
                // Dynamic table name — try to resolve via let-bindings
                let var_hint = after.split(')').next().unwrap_or("?").trim();

                // Strip field access: ct.table → table, etc.
                let lookup_key = var_hint.rsplit('.').next().unwrap_or(var_hint);

                if let Some(resolved_tables) = let_bindings.get(lookup_key) {
                    // Resolved! Validate each possible table
                    // Join continuation lines for column extraction
                    let mut full_chain = line_fragment.to_string();
                    let mut j = i + 1;
                    let mut depth = count_net_delimiters(line_fragment);
                    while j < lines.len() {
                        let next = lines[j].trim();
                        if next.starts_with('.') || depth > 0 {
                            full_chain.push(' ');
                            full_chain.push_str(next);
                            depth += count_net_delimiters(next);
                            j += 1;
                        } else if next.is_empty() {
                            j += 1;
                        } else {
                            break;
                        }
                    }
                    let columns = extract_columns(&full_chain);
                    let has_rls = full_chain.contains(".with_rls(") || full_chain.contains(".rls(");

                    for resolved_table in resolved_tables {
                        let is_cte_ref = file_cte_names.contains(resolved_table);
                        usages.push(QailUsage {
                            file: file.to_string(),
                            line: start_line,
                            column: start_column,
                            table: resolved_table.clone(),
                            is_dynamic_table: false,
                            columns: columns.clone(),
                            action: action.to_string(),
                            is_cte_ref,
                            has_rls,
                            file_uses_super_admin,
                        });
                    }
                    if j > i + 1 {
                        i = j.saturating_sub(1);
                        break;
                    }
                } else {
                    // Truly dynamic — cannot validate
                    println!(
                        "cargo:warning=Qail: dynamic table name `{}` in {}:{} — cannot validate columns at build time. Consider using string literals.",
                        var_hint, file, start_line
                    );
                }
            }
            // else: Qail::typed with non-parsable table — skip silently (it has compile-time safety)
            cursor = next_cursor;
        }
        i += 1;
    }
}

pub(crate) fn extract_string_arg(s: &str) -> Option<String> {
    // Find "string" pattern
    let s = s.trim();
    if let Some(stripped) = s.strip_prefix('"') {
        let end = stripped.find('"')?;
        Some(stripped[..end].to_string())
    } else {
        None
    }
}

/// Extract table name from `Qail::typed(module::Table)` patterns.
/// Parses `module::StructName` and returns the last identifier-like segment
/// before the final `::item` as the table name.
///
/// Examples:
///  - `users::table`         → `users`
///  - `users::Users`         → `users`
///  - `schema::users::table` → `users`  (second-to-last segment)
///  - `Orders`               → `orders` (single ident, no ::)
pub(crate) fn extract_typed_table_arg(s: &str) -> Option<String> {
    let s = s.trim();
    // Collect the full path: identifier::Identifier::...
    let ident: String = s
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
        .collect();

    let segments: Vec<&str> = ident.split("::").filter(|s| !s.is_empty()).collect();

    match segments.len() {
        0 => None,
        1 => {
            // Single ident like `Orders` — use it directly
            let name = segments[0];
            if name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                Some(name.to_lowercase())
            } else {
                None
            }
        }
        _ => {
            // Multiple segments like `users::table` or `schema::users::table`
            // Take the second-to-last segment as the table name
            let table = segments[segments.len() - 2];
            if table.chars().all(|c| c.is_alphanumeric() || c == '_') {
                Some(table.to_lowercase())
            } else {
                None
            }
        }
    }
}

pub(crate) fn extract_columns(line: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut remaining = line;

    // .column("col") — singular column
    while let Some(pos) = remaining.find(".column(") {
        let after = &remaining[pos + 8..];
        if let Some(col) = extract_string_arg(after) {
            columns.push(col);
        }
        remaining = after;
    }

    // Reset for .columns([...]) — array syntax (most common pattern)
    remaining = line;
    while let Some(pos) = remaining.find(".columns(") {
        let after = &remaining[pos + 9..];
        // Find the opening bracket [
        if let Some(bracket_start) = after.find('[') {
            let inside = &after[bracket_start + 1..];
            // Find the closing bracket ]
            if let Some(bracket_end) = inside.find(']') {
                let array_content = &inside[..bracket_end];
                // Extract all string literals from the array
                let mut scan = array_content;
                while let Some(quote_start) = scan.find('"') {
                    let after_quote = &scan[quote_start + 1..];
                    if let Some(quote_end) = after_quote.find('"') {
                        let col = &after_quote[..quote_end];
                        if !col.is_empty() {
                            columns.push(col.to_string());
                        }
                        scan = &after_quote[quote_end + 1..];
                    } else {
                        break;
                    }
                }
            }
        }
        remaining = after;
    }

    // Reset for next pattern
    remaining = line;

    // .filter("col", ...)
    while let Some(pos) = remaining.find(".filter(") {
        let after = &remaining[pos + 8..];
        if let Some(col) = extract_string_arg(after)
            && !col.contains('.')
        {
            columns.push(col);
        }
        remaining = after;
    }

    // .eq("col", val), .ne("col", val), .gt, .lt, .gte, .lte
    for method in [
        ".eq(", ".ne(", ".gt(", ".lt(", ".gte(", ".lte(", ".like(", ".ilike(",
    ] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(col) = extract_string_arg(after)
                && !col.contains('.')
            {
                columns.push(col);
            }
            temp = after;
        }
    }

    // .where_eq("col", val) — WHERE clause column
    remaining = line;
    while let Some(pos) = remaining.find(".where_eq(") {
        let after = &remaining[pos + 10..];
        if let Some(col) = extract_string_arg(after)
            && !col.contains('.')
        {
            columns.push(col);
        }
        remaining = after;
    }

    // .order_by("col"), .order_desc("col"), .order_asc("col")
    // Keep these for scanner parity with chain coverage tests. Alias names are
    // filtered out below via the `.alias("...")` pass.
    for method in [".order_by(", ".order_desc(", ".order_asc("] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(col) = extract_string_arg(after)
                && !col.contains('.')
            {
                columns.push(col);
            }
            temp = after;
        }
    }

    // .in_vals("col", vals)
    remaining = line;
    while let Some(pos) = remaining.find(".in_vals(") {
        let after = &remaining[pos + 9..];
        if let Some(col) = extract_string_arg(after)
            && !col.contains('.')
        {
            columns.push(col);
        }
        remaining = after;
    }

    // ── Additional DSL methods (Finding #4) ──────────────────────────

    // .is_null("col"), .is_not_null("col")
    for method in [".is_null(", ".is_not_null("] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(col) = extract_string_arg(after)
                && !col.contains('.')
            {
                columns.push(col);
            }
            temp = after;
        }
    }

    // .set_value("col", val), .set_coalesce("col", val), .set_coalesce_opt("col", val)
    for method in [".set_value(", ".set_coalesce(", ".set_coalesce_opt("] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(col) = extract_string_arg(after)
                && !col.contains('.')
            {
                columns.push(col);
            }
            temp = after;
        }
    }

    // .returning(["col_a", "col_b"]) — array pattern, same as .columns()
    remaining = line;
    while let Some(pos) = remaining.find(".returning(") {
        let after = &remaining[pos + 11..];
        if let Some(bracket_start) = after.find('[') {
            let inside = &after[bracket_start + 1..];
            if let Some(bracket_end) = inside.find(']') {
                let array_content = &inside[..bracket_end];
                let mut scan = array_content;
                while let Some(quote_start) = scan.find('"') {
                    let after_quote = &scan[quote_start + 1..];
                    if let Some(quote_end) = after_quote.find('"') {
                        let col = &after_quote[..quote_end];
                        if !col.is_empty() && !col.contains('.') {
                            columns.push(col.to_string());
                        }
                        scan = &after_quote[quote_end + 1..];
                    } else {
                        break;
                    }
                }
            }
        }
        remaining = after;
    }

    // .on_conflict_update(&["col"], ...) and .on_conflict_nothing(&["col"])
    // Extract conflict column names from the first array arg
    for method in [".on_conflict_update(", ".on_conflict_nothing("] {
        let mut temp = line;
        while let Some(pos) = temp.find(method) {
            let after = &temp[pos + method.len()..];
            if let Some(bracket_start) = after.find('[') {
                let inside = &after[bracket_start + 1..];
                if let Some(bracket_end) = inside.find(']') {
                    let array_content = &inside[..bracket_end];
                    let mut scan = array_content;
                    while let Some(quote_start) = scan.find('"') {
                        let after_quote = &scan[quote_start + 1..];
                        if let Some(quote_end) = after_quote.find('"') {
                            let col = &after_quote[..quote_end];
                            if !col.is_empty() && !col.contains('.') {
                                columns.push(col.to_string());
                            }
                            scan = &after_quote[quote_end + 1..];
                        } else {
                            break;
                        }
                    }
                }
            }
            temp = after;
        }
    }

    // Extract .alias("name") patterns — these are computed expression aliases, not schema columns.
    // They're valid in ORDER BY / GROUP BY but should not be validated against the schema.
    let mut aliases = Vec::new();
    {
        let mut alias_scan = line;
        while let Some(pos) = alias_scan.find(".alias(") {
            let after = &alias_scan[pos + 7..];
            if let Some(name) = extract_string_arg(after) {
                aliases.push(name);
            }
            alias_scan = after;
        }
    }

    // Clean up extracted columns: strip Postgres ::type casts and AS aliases.
    // e.g. "id::text" → "id", "conn.id::text as connection_id" → "conn.id",
    // "COALESCE(inv.capacity - inv.reserved, 0)::bigint as x" → skipped (expression)
    let columns: Vec<String> = columns
        .into_iter()
        .map(|col| {
            // Strip " as alias" suffix (case-insensitive)
            let col = if let Some(pos) = col.find(" as ").or_else(|| col.find(" AS ")) {
                col[..pos].trim().to_string()
            } else {
                col
            };
            // Strip ::type cast suffix
            if let Some(pos) = col.find("::") {
                col[..pos].to_string()
            } else {
                col
            }
        })
        .filter(|col| {
            // Skip expressions that aren't simple column references
            !col.contains('(') && !col.contains(')') && !col.contains(' ')
        })
        .filter(|col| {
            // Skip computed alias names — these are not schema columns
            !aliases.contains(col)
        })
        .collect();

    columns
}

pub(crate) fn usage_action_to_ast(action: &str) -> crate::ast::Action {
    use crate::ast::Action;

    match action {
        "GET" | "TYPED" => Action::Get,
        "ADD" => Action::Add,
        "SET" => Action::Set,
        "DEL" => Action::Del,
        "PUT" => Action::Put,
        _ => Action::Get,
    }
}

pub(crate) fn append_scanned_columns(cmd: &mut crate::ast::Qail, columns: &[String]) {
    use crate::ast::Expr;

    for col in columns {
        // Skip qualified columns (CTE refs like cte.column)
        if col.contains('.') {
            continue;
        }
        // Skip SQL function expressions (e.g., count(*), SUM(amount))
        // and wildcard (*) — these are valid SQL, not schema columns
        if col.contains('(') || col == "*" {
            continue;
        }
        let exists = cmd
            .columns
            .iter()
            .any(|e| matches!(e, Expr::Named(existing) if existing == col));
        if !exists {
            cmd.columns.push(Expr::Named(col.clone()));
        }
    }
}
