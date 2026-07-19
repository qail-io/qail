//! Export machine-readable knowledge artifacts about the QAIL language.
//!
//! QAIL v2 has no formal grammar document — the parser *is* the specification,
//! and `docs/src/core/text-syntax.md` covers a fraction of it. This binary
//! extracts the language's real shape from source so documentation and the MCP
//! knowledge server describe what the parser actually accepts, rather than what
//! someone remembered writing.
//!
//! Two stages:
//!
//! **A. Grammar extraction** walks `core/src/parser/grammar/*.rs` and records,
//! per production: its doc comment, the `tag_no_case` keyword literals it
//! matches, and the productions it delegates to.
//!
//! **B. Example mining** collects candidate QAIL snippets from the parser test
//! suite, grammar doc comments, and ```qail fences in the docs, then *runs each
//! one through the real parser and transpiler*. Snippets that parse become
//! verified examples carrying their AST and generated SQL; snippets that fail
//! become a negative corpus carrying the real error text.
//!
//! Because docs examples are executed, a ```qail block that stops parsing fails
//! this binary — the documentation is linted by construction.
//!
//! ```sh
//! cargo run -p qail-core --example knowledge_export -- --out docs/generated
//! ```

use qail_core::parse;
use qail_core::transpiler::{Dialect, ToSql};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::Visit;

/// Maximum QAIL input the parser accepts; mirrors `parser::MAX_INPUT_LENGTH`.
const MAX_INPUT_LENGTH: usize = 64 * 1024;

fn main() {
    let mut args = std::env::args().skip(1);
    let mut out_dir = PathBuf::from("docs/generated");
    let mut strict = true;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => {
                out_dir = PathBuf::from(args.next().unwrap_or_else(|| {
                    eprintln!("error: --out requires a path");
                    std::process::exit(2);
                }));
            }
            // Escape hatch for working on the docs; CI must run without it.
            "--allow-doc-failures" => strict = false,
            other => {
                eprintln!("error: unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }

    let root = repo_root();
    fs::create_dir_all(&out_dir).expect("create output directory");

    let productions = extract_grammar(&root);
    let (valid, invalid, doc_failures) = mine_examples(&root);

    println!("grammar productions : {}", productions.len());
    println!("verified examples   : {}", valid.len());
    println!("negative examples   : {}", invalid.len());

    write_json(&out_dir.join("grammar-productions.json"), &productions);
    write_json(&out_dir.join("verified-examples.json"), &valid);
    write_json(&out_dir.join("invalid-examples.json"), &invalid);

    if !doc_failures.is_empty() {
        eprintln!("\n{} documentation example(s) do not parse:", doc_failures.len());
        for (source, snippet, err) in &doc_failures {
            eprintln!("  {source}\n    {snippet}\n    -> {err}");
        }
        if strict {
            eprintln!("\nDocumentation examples must parse. Fix them, or pass \
                       --allow-doc-failures while iterating.");
            std::process::exit(1);
        }
    }

    println!("\nwrote artifacts to {}", out_dir.display());
}

// ---------------------------------------------------------------------------
// Stage A: grammar extraction
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Production {
    name: String,
    construct: String,
    file: String,
    doc: Option<String>,
    example: Option<String>,
    keywords: Vec<String>,
    calls: Vec<String>,
}

/// Collects `tag_no_case("...")` literals and `parse_*` callees from a fn body.
#[derive(Default)]
struct BodyWalker {
    keywords: BTreeSet<String>,
    calls: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for BodyWalker {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let syn::Expr::Path(path) = &*call.func
            && let Some(last) = path.path.segments.last()
        {
            let name = last.ident.to_string();

            if name == "tag_no_case" || name == "tag" {
                if let Some(syn::Expr::Lit(lit)) = call.args.first()
                    && let syn::Lit::Str(s) = &lit.lit
                {
                    self.keywords.insert(s.value());
                }
            } else if name.starts_with("parse_") {
                self.calls.insert(name);
            }
        }

        syn::visit::visit_expr_call(self, call);
    }

    fn visit_expr_path(&mut self, path: &'ast syn::ExprPath) {
        // Combinators are often passed by name rather than called directly.
        if let Some(last) = path.path.segments.last() {
            let name = last.ident.to_string();
            if name.starts_with("parse_") {
                self.calls.insert(name);
            }
        }
        syn::visit::visit_expr_path(self, path);
    }
}

fn extract_grammar(root: &Path) -> Vec<Production> {
    let dir = root.join("core/src/parser/grammar");
    let mut productions = Vec::new();

    let mut files: Vec<PathBuf> = fs::read_dir(&dir)
        .expect("read grammar directory")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .collect();
    files.sort();

    for path in files {
        let src = fs::read_to_string(&path).expect("read grammar file");
        let file = syn::parse_file(&src).expect("parse grammar file as Rust");
        let rel = relative(root, &path);
        let stem = path.file_stem().unwrap().to_string_lossy().to_string();

        for item in &file.items {
            let syn::Item::Fn(func) = item else { continue };
            let name = func.sig.ident.to_string();
            if !name.starts_with("parse_") {
                continue;
            }

            let doc = doc_comment(&func.attrs);
            // `/// Parse: <snippet>` is the established convention for showing
            // what a production accepts.
            let example = doc.as_deref().and_then(|d| {
                d.lines()
                    .find_map(|line| line.trim().strip_prefix("Parse:"))
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty() && s.to_lowercase() != "nothing")
            });

            let mut walker = BodyWalker::default();
            walker.visit_block(&func.block);

            productions.push(Production {
                construct: construct_of(&name, &stem),
                name,
                file: rel.clone(),
                doc,
                example,
                keywords: walker.keywords.into_iter().collect(),
                calls: walker.calls.into_iter().collect(),
            });
        }
    }

    productions
}

/// Group a production under a user-facing construct name, so an agent can ask
/// for "where" rather than needing to know it maps to `parse_where_clause`.
fn construct_of(fn_name: &str, file_stem: &str) -> String {
    let base = fn_name
        .strip_prefix("parse_")
        .unwrap_or(fn_name)
        .trim_end_matches("_clause")
        .trim_end_matches("_expr")
        .to_string();

    if base.is_empty() {
        file_stem.to_string()
    } else {
        base
    }
}

fn doc_comment(attrs: &[syn::Attribute]) -> Option<String> {
    let mut lines = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }
        if let syn::Meta::NameValue(nv) = &attr.meta
            && let syn::Expr::Lit(lit) = &nv.value
            && let syn::Lit::Str(s) = &lit.lit
        {
            lines.push(s.value().trim().to_string());
        }
    }
    (!lines.is_empty()).then(|| lines.join("\n"))
}

// ---------------------------------------------------------------------------
// Stage B: example mining
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct VerifiedExample {
    input: String,
    sql: String,
    action: String,
    table: String,
    source: String,
}

#[derive(Debug)]
struct InvalidExample {
    input: String,
    error: String,
    source: String,
}

/// Every `parse("...")` string literal in a Rust file, found via AST walk so
/// multi-line calls and raw strings are captured (grep misses both).
#[derive(Default)]
struct ParseCallCollector {
    inputs: Vec<String>,
}

impl<'ast> Visit<'ast> for ParseCallCollector {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let syn::Expr::Path(path) = &*call.func
            && path
                .path
                .segments
                .last()
                .is_some_and(|s| s.ident == "parse")
            && let Some(syn::Expr::Lit(lit)) = call.args.first()
            && let syn::Lit::Str(s) = &lit.lit
        {
            self.inputs.push(s.value());
        }
        syn::visit::visit_expr_call(self, call);
    }
}

type DocFailures = Vec<(String, String, String)>;

fn mine_examples(root: &Path) -> (Vec<VerifiedExample>, Vec<InvalidExample>, DocFailures) {
    // Keyed by normalized input so the same query mined from several places
    // yields one example. BTreeMap keeps output stable across runs.
    let mut candidates: BTreeMap<String, (String, String)> = BTreeMap::new();

    // Source 1: the parser test suite. Ground truth by construction.
    let test_dir = root.join("core/src/parser/tests");
    if test_dir.is_dir() {
        let mut files: Vec<PathBuf> = fs::read_dir(&test_dir)
            .expect("read tests directory")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|e| e == "rs"))
            .collect();
        files.sort();

        for path in files {
            let src = fs::read_to_string(&path).expect("read test file");
            let Ok(file) = syn::parse_file(&src) else { continue };
            let rel = relative(root, &path);

            let mut collector = ParseCallCollector::default();
            collector.visit_file(&file);
            for input in collector.inputs {
                candidates
                    .entry(normalize(&input))
                    .or_insert((input, rel.clone()));
            }
        }
    }

    // Source 2: `/// Parse:` doc comments on the productions themselves.
    for production in extract_grammar(root) {
        if let Some(example) = production.example {
            // Doc examples are often fragments ("where col = value"), not whole
            // queries. Only whole queries can be executed.
            if starts_with_action(&example) {
                candidates
                    .entry(normalize(&example))
                    .or_insert((example, production.file.clone()));
            }
        }
    }

    // Source 3: ```qail fences in the documentation. These are what readers
    // copy, so they are the ones that most need to be correct.
    // `docs/` already contains `docs/src/`, so walking it once covers both.
    let mut doc_candidates: Vec<(String, String)> = Vec::new();
    collect_doc_fences(&root.join("docs"), root, &mut doc_candidates);

    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    let mut doc_failures = Vec::new();

    for (input, source) in candidates.into_values() {
        match evaluate(&input, &source) {
            Ok(example) => valid.push(example),
            Err(example) => invalid.push(example),
        }
    }

    // Documentation examples are held to a higher standard: a failure here is a
    // documentation bug, not an interesting negative example.
    //
    // Most ```qail fences in the docs are schema blocks rather than queries, so
    // both forms are linted — schema against the brace parser, queries against
    // the query parser. Fragments that are neither are skipped rather than
    // reported, since a doc may legitimately show a clause in isolation.
    for (input, source) in doc_candidates {
        if starts_with_action(&input) {
            match evaluate(&input, &source) {
                Ok(example) => valid.push(example),
                Err(example) => {
                    doc_failures.push((
                        example.source.clone(),
                        example.input.clone(),
                        example.error,
                    ));
                }
            }
        } else if looks_like_schema(&input) {
            // Two brace-dialect schema parsers coexist with different feature
            // sets: `migrate::parse_qail` drives migrations, while
            // `build::schema::Schema::parse` drives typed codegen and accepts
            // options the migration parser does not (`protected`, for one). A
            // documented block is valid if either accepts it; failing on the
            // migration parser alone would flag correct codegen documentation.
            let migrate_err = qail_core::migrate::parse_qail(&input).err();
            if let Some(err) = migrate_err
                && let Err(build_err) = qail_core::build::schema::Schema::parse(&input)
            {
                doc_failures.push((
                    source,
                    first_line(&input),
                    format!("migrate parser: {err}; build parser: {build_err}"),
                ));
            }
        }
    }

    valid.sort_by(|a, b| a.input.cmp(&b.input));
    valid.dedup_by(|a, b| normalize(&a.input) == normalize(&b.input));
    invalid.sort_by(|a, b| a.input.cmp(&b.input));

    (valid, invalid, doc_failures)
}

/// Run one candidate through the real parser and transpiler.
fn evaluate(input: &str, source: &str) -> Result<VerifiedExample, InvalidExample> {
    if input.len() > MAX_INPUT_LENGTH {
        return Err(InvalidExample {
            input: input.to_string(),
            error: format!("exceeds MAX_INPUT_LENGTH ({MAX_INPUT_LENGTH} bytes)"),
            source: source.to_string(),
        });
    }

    match parse(input) {
        Ok(cmd) => {
            let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
            // Some parser tests probe edge cases that parse but transpile to
            // SQL carrying an inline error marker. Those are fixtures, not
            // teaching material — publishing them would demonstrate a broken
            // pattern as if it were idiomatic.
            if sql.contains("/* ERROR") {
                return Err(InvalidExample {
                    input: input.to_string(),
                    error: format!("parses, but transpiles to invalid SQL: {sql}"),
                    source: source.to_string(),
                });
            }
            Ok(VerifiedExample {
                input: input.to_string(),
                sql,
                action: format!("{:?}", cmd.action).to_lowercase(),
                table: cmd.table.clone(),
                source: source.to_string(),
            })
        }
        Err(err) => Err(InvalidExample {
            input: input.to_string(),
            error: err.to_string(),
            source: source.to_string(),
        }),
    }
}

/// QAIL queries begin with an action keyword. Used to separate executable
/// queries from grammar fragments and schema blocks.
fn starts_with_action(input: &str) -> bool {
    let first = input.trim_start().split_whitespace().next().unwrap_or("");
    matches!(
        first.to_lowercase().as_str(),
        "get" | "add" | "set" | "del" | "merge" | "with" | "export"
    )
}

/// A `schema.qail` block declares tables, extensions, indexes or policies.
fn looks_like_schema(input: &str) -> bool {
    input.lines().map(str::trim).any(|line| {
        line.starts_with("table ")
            || line.starts_with("extension ")
            || line.starts_with("index ")
            || line.starts_with("policy ")
            || line.starts_with("enum ")
    })
}

fn first_line(input: &str) -> String {
    input.lines().next().unwrap_or("").trim().to_string()
}

fn collect_doc_fences(dir: &Path, root: &Path, out: &mut Vec<(String, String)>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    let mut paths: Vec<PathBuf> = entries.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();

    for path in paths {
        if path.is_dir() {
            // `internal` holds point-in-time audit snapshots, not documentation.
            if path.file_name().is_some_and(|n| n == "internal") {
                continue;
            }
            collect_doc_fences(&path, root, out);
            continue;
        }
        if path.extension().is_none_or(|e| e != "md") {
            continue;
        }

        let Ok(text) = fs::read_to_string(&path) else { continue };
        let rel = relative(root, &path);

        for (index, block) in text.split("```qail").skip(1).enumerate() {
            let Some(body) = block.split("```").next() else { continue };
            let body = body.trim();
            if body.is_empty() {
                continue;
            }
            out.push((body.to_string(), format!("{rel} (block {})", index + 1)));
        }
    }
}

fn normalize(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase()
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

/// Minimal JSON writer. qail-core does not depend on serde_json's `to_string`
/// for its own types here, and hand-rolling keeps the example dependency-light.
trait ToJson {
    fn to_json(&self) -> String;
}

fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn arr(items: &[String]) -> String {
    let inner: Vec<String> = items.iter().map(|s| format!("\"{}\"", esc(s))).collect();
    format!("[{}]", inner.join(","))
}

impl ToJson for Production {
    fn to_json(&self) -> String {
        let doc = self
            .doc
            .as_ref()
            .map_or("null".to_string(), |d| format!("\"{}\"", esc(d)));
        let example = self
            .example
            .as_ref()
            .map_or("null".to_string(), |e| format!("\"{}\"", esc(e)));
        format!(
            r#"{{"production":"{}","construct":"{}","file":"{}","doc":{},"example":{},"keywords":{},"calls":{}}}"#,
            esc(&self.name),
            esc(&self.construct),
            esc(&self.file),
            doc,
            example,
            arr(&self.keywords),
            arr(&self.calls)
        )
    }
}

impl ToJson for VerifiedExample {
    fn to_json(&self) -> String {
        format!(
            r#"{{"input":"{}","sql":"{}","action":"{}","table":"{}","source":"{}","verified":true}}"#,
            esc(&self.input),
            esc(&self.sql),
            esc(&self.action),
            esc(&self.table),
            esc(&self.source)
        )
    }
}

impl ToJson for InvalidExample {
    fn to_json(&self) -> String {
        format!(
            r#"{{"input":"{}","error":"{}","source":"{}","verified":false}}"#,
            esc(&self.input),
            esc(&self.error),
            esc(&self.source)
        )
    }
}

fn write_json<T: ToJson>(path: &Path, items: &[T]) {
    let body: Vec<String> = items.iter().map(|i| format!("  {}", i.to_json())).collect();
    let json = format!("[\n{}\n]\n", body.join(",\n"));
    fs::write(path, json).unwrap_or_else(|err| panic!("write {}: {err}", path.display()));
}

// ---------------------------------------------------------------------------

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is core/; the repo root is its parent.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("core/ has a parent")
        .to_path_buf()
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}
