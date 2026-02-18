//! QAIL Codegen — Rust AST → Zig type definition generator.
//!
//! Reads Rust source files from `core/src/ast/` and generates
//! equivalent Zig struct/enum definitions in `qail-zig/src/ast/generated/`.

use std::fmt::Write;
use std::path::Path;

/// Rust → Zig type mapping.
fn map_type(ty: &str) -> String {
    // Strip outer whitespace
    let ty = ty.trim();

    // Handle Option<Box<T>> → ?*const T (not ??*const T)
    if let Some(inner) = strip_angle("Option", ty) {
        if let Some(box_inner) = strip_angle("Box", inner) {
            let mapped = map_type(box_inner);
            return format!("?*const {}", mapped);
        }
        let mapped = map_type(inner);
        // Don't double-wrap optionals
        if mapped.starts_with('?') {
            return mapped;
        }
        return format!("?{}", mapped);
    }

    // Handle Vec<T>
    if let Some(inner) = strip_angle("Vec", ty) {
        let mapped = map_type(inner);
        return format!("[]const {}", mapped);
    }

    // Handle Box<T>
    if let Some(inner) = strip_angle("Box", ty) {
        let mapped = map_type(inner);
        return format!("?*const {}", mapped);
    }

    // Handle tuples (A, B)
    if ty.starts_with('(') && ty.ends_with(')') {
        let inner = &ty[1..ty.len() - 1];
        let parts: Vec<&str> = split_top_level(inner);
        let fields: Vec<String> = parts
            .iter()
            .enumerate()
            .map(|(i, p)| format!("    f{}: {},", i, map_type(p.trim())))
            .collect();
        return format!("struct {{\n{}\n}}", fields.join("\n"));
    }

    // Strip crate:: paths AFTER generic unwrapping
    // (e.g. "crate :: ast :: Expr" → "Expr")
    if ty.contains("::") {
        let cleaned: String = ty.replace(' ', "");
        if let Some(last) = cleaned.rsplit("::").next() {
            return map_type(last);
        }
    }

    // Primitives
    match ty {
        "String" => "[]const u8".to_string(),
        "bool" => "bool".to_string(),
        "i32" => "i32".to_string(),
        "i64" => "i64".to_string(),
        "u64" => "u64".to_string(),
        "usize" => "usize".to_string(),
        "f32" => "f32".to_string(),
        "f64" => "f64".to_string(),
        "u8" => "u8".to_string(),
        "Uuid" => "[16]u8".to_string(),
        // Self-referential types
        "Qail" => "QailCmd".to_string(),
        // Known AST types — keep as-is (Zig names match)
        _ => ty.to_string(),
    }
}

/// Strip `Wrapper<inner>` and return inner.
fn strip_angle<'a>(prefix: &str, ty: &'a str) -> Option<&'a str> {
    if ty.starts_with(prefix) && ty.ends_with('>') {
        let start = prefix.len() + 1; // skip "Prefix<"
        let inner = &ty[start..ty.len() - 1];
        Some(inner.trim())
    } else {
        None
    }
}

/// Split on commas, respecting nested angle brackets.
fn split_top_level(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '<' | '(' => depth += 1,
            '>' | ')' => depth -= 1,
            ',' if depth == 0 => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&s[start..]);
    result
}

/// Convert Rust PascalCase to Zig snake_case.
fn to_snake_case(name: &str) -> String {
    let mut result = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

/// Get the default value for a Zig type.
fn zig_default(zig_type: &str, field_name: &str) -> Option<String> {
    if zig_type.starts_with("?") {
        Some("null".to_string())
    } else if zig_type == "[]const u8" {
        // String fields: default to empty string
        // Exception: "table" and "name" are required identifiers, no default
        if field_name == "table" || field_name == "name" {
            None
        } else {
            Some("\"\"".to_string())
        }
    } else if zig_type.starts_with("[]const ") {
        Some("&.{}".to_string())
    } else if zig_type == "bool" {
        Some("false".to_string())
    } else {
        None
    }
}

// ============================================================================
// Parsing with syn
// ============================================================================

/// A parsed Rust enum.
struct RustEnum {
    name: String,
    doc: String,
    variants: Vec<EnumVariant>,
}

struct EnumVariant {
    name: String,
    doc: String,
    fields: Vec<StructField>,
}

/// A parsed Rust struct.
struct RustStruct {
    name: String,
    doc: String,
    fields: Vec<StructField>,
}

struct StructField {
    name: String,
    doc: String,
    ty: String,
}

/// Parse a single Rust source file and extract enums & structs.
fn parse_file(path: &Path) -> (Vec<RustEnum>, Vec<RustStruct>) {
    let source = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Cannot read {}: {}", path.display(), e));
    let syntax = syn::parse_file(&source)
        .unwrap_or_else(|e| panic!("Cannot parse {}: {}", path.display(), e));

    let mut enums = Vec::new();
    let mut structs = Vec::new();

    for item in &syntax.items {
        match item {
            syn::Item::Enum(e) => {
                // Skip non-pub or impl-only enums
                if !matches!(e.vis, syn::Visibility::Public(_)) {
                    continue;
                }
                let doc = extract_doc_attrs(&e.attrs);
                let mut variants = Vec::new();
                for v in &e.variants {
                    let vdoc = extract_doc_attrs(&v.attrs);
                    let fields = match &v.fields {
                        syn::Fields::Unit => vec![],
                        syn::Fields::Unnamed(f) => f
                            .unnamed
                            .iter()
                            .enumerate()
                            .map(|(i, field)| StructField {
                                name: format!("f{}", i),
                                doc: String::new(),
                                ty: type_to_string(&field.ty),
                            })
                            .collect(),
                        syn::Fields::Named(f) => f
                            .named
                            .iter()
                            .map(|field| StructField {
                                name: field.ident.as_ref().unwrap().to_string(),
                                doc: extract_doc_attrs(&field.attrs),
                                ty: type_to_string(&field.ty),
                            })
                            .collect(),
                    };
                    variants.push(EnumVariant {
                        name: v.ident.to_string(),
                        doc: vdoc,
                        fields,
                    });
                }
                enums.push(RustEnum {
                    name: e.ident.to_string(),
                    doc,
                    variants,
                });
            }
            syn::Item::Struct(s) => {
                if !matches!(s.vis, syn::Visibility::Public(_)) {
                    continue;
                }
                let doc = extract_doc_attrs(&s.attrs);
                let fields = match &s.fields {
                    syn::Fields::Named(f) => f
                        .named
                        .iter()
                        .map(|field| StructField {
                            name: field.ident.as_ref().unwrap().to_string(),
                            doc: extract_doc_attrs(&field.attrs),
                            ty: type_to_string(&field.ty),
                        })
                        .collect(),
                    _ => vec![],
                };
                structs.push(RustStruct {
                    name: s.ident.to_string(),
                    doc,
                    fields,
                });
            }
            _ => {}
        }
    }

    (enums, structs)
}

/// Extract doc comments from attributes.
fn extract_doc_attrs(attrs: &[syn::Attribute]) -> String {
    let mut doc = String::new();
    for attr in attrs {
        if attr.path().is_ident("doc") {
            if let syn::Meta::NameValue(nv) = &attr.meta {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) = &nv.value
                {
                    let line = s.value();
                    if !doc.is_empty() {
                        doc.push('\n');
                    }
                    doc.push_str(line.trim());
                }
            }
        }
    }
    doc
}

/// Convert a syn::Type to a string like "Vec<String>", "Option<i64>", etc.
fn type_to_string(ty: &syn::Type) -> String {
    use quote::ToTokens;
    let tokens = ty.to_token_stream();
    // Normalize by removing spaces around < >
    let s = tokens.to_string();
    s.replace(" < ", "<")
        .replace("< ", "<")
        .replace(" <", "<")
        .replace(" > ", ">")
        .replace("> ", ">")
        .replace(" >", ">")
}

// ============================================================================
// Zig code generation
// ============================================================================

/// Generate Zig source for a simple enum (all unit variants).
fn gen_zig_enum(e: &RustEnum) -> String {
    let mut out = String::new();
    if !e.doc.is_empty() {
        for line in e.doc.lines() {
            let _ = writeln!(out, "/// {}", line);
        }
    }
    let _ = writeln!(out, "pub const {} = enum {{", e.name);
    for v in &e.variants {
        if !v.doc.is_empty() {
            for line in v.doc.lines() {
                let _ = writeln!(out, "    /// {}", line);
            }
        }
        let zig_name = to_snake_case(&v.name);
        // Zig keywords need @"" quoting
        let zig_name = match zig_name.as_str() {
            "and" | "or" | "union" | "mod" | "error" => format!("@\"{}\"", zig_name),
            _ => zig_name,
        };
        let _ = writeln!(out, "    {},", zig_name);
    }
    let _ = writeln!(out, "}};");
    out
}

/// Generate Zig source for a tagged union (enum with data variants).
fn gen_zig_tagged_union(e: &RustEnum) -> String {
    let mut out = String::new();
    if !e.doc.is_empty() {
        for line in e.doc.lines() {
            let _ = writeln!(out, "/// {}", line);
        }
    }
    let _ = writeln!(out, "pub const {} = union(enum) {{", e.name);
    for v in &e.variants {
        if !v.doc.is_empty() {
            for line in v.doc.lines() {
                let _ = writeln!(out, "    /// {}", line);
            }
        }
        let zig_name = to_snake_case(&v.name);
        if v.fields.is_empty() {
            let _ = writeln!(out, "    {},", zig_name);
        } else if v.fields.len() == 1 && v.fields[0].name.starts_with('f') {
            // Unnamed tuple variant with single field
            let zig_ty = map_type(&v.fields[0].ty);
            let _ = writeln!(out, "    {}: {},", zig_name, zig_ty);
        } else {
            // Named fields → inline struct
            let _ = writeln!(out, "    {}: struct {{", zig_name);
            for field in &v.fields {
                let zig_ty = map_type(&field.ty);
                let _ = writeln!(out, "        {}: {},", field.name, zig_ty);
            }
            let _ = writeln!(out, "    }},");
        }
    }
    let _ = writeln!(out, "}};");
    out
}

/// Generate Zig source for a struct.
fn gen_zig_struct(s: &RustStruct, name_override: Option<&str>) -> String {
    let mut out = String::new();
    let name = name_override.unwrap_or(&s.name);
    if !s.doc.is_empty() {
        for line in s.doc.lines() {
            let _ = writeln!(out, "/// {}", line);
        }
    }
    let _ = writeln!(out, "pub const {} = struct {{", name);
    for field in &s.fields {
        if !field.doc.is_empty() {
            for line in field.doc.lines() {
                let _ = writeln!(out, "    /// {}", line);
            }
        }
        let zig_ty = map_type(&field.ty);
        if let Some(default) = zig_default(&zig_ty, &field.name) {
            let _ = writeln!(out, "    {}: {} = {},", field.name, zig_ty, default);
        } else {
            let _ = writeln!(out, "    {}: {},", field.name, zig_ty);
        }
    }
    let _ = writeln!(out, "}};");
    out
}

/// Check if an enum has any non-unit variants (needs tagged union).
fn is_simple_enum(e: &RustEnum) -> bool {
    e.variants.iter().all(|v| v.fields.is_empty())
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let rust_ast_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../core/src/ast");
    let zig_output_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../qail-zig/src/ast/generated");

    // Create output directory
    std::fs::create_dir_all(&zig_output_dir).expect("Failed to create output directory");

    // Files to process
    let files = [
        ("operators.rs", "operators.gen.zig"),
        ("values.rs", "values.gen.zig"),
        ("conditions.rs", "conditions.gen.zig"),
        ("cages.rs", "cages.gen.zig"),
        ("joins.rs", "joins.gen.zig"),
        ("cmd/mod.rs", "cmd.gen.zig"),
        ("expr.rs", "expr.gen.zig"),
    ];

    let mut total_enums = 0;
    let mut total_structs = 0;

    for (rust_file, zig_file) in &files {
        let rust_path = rust_ast_dir.join(rust_file);
        if !rust_path.exists() {
            eprintln!("⚠️  Skipping {} (not found)", rust_path.display());
            continue;
        }

        let (enums, structs) = parse_file(&rust_path);

        let mut output = String::new();
        let _ = writeln!(output, "// Auto-generated by qail-codegen — DO NOT EDIT");
        let _ = writeln!(output, "// Source: core/src/ast/{}", rust_file);
        let _ = writeln!(output, "// Generated: {}", chrono_like_now());
        let _ = writeln!(output);

        for e in &enums {
            if is_simple_enum(e) {
                output.push_str(&gen_zig_enum(e));
            } else {
                output.push_str(&gen_zig_tagged_union(e));
            }
            output.push('\n');
            total_enums += 1;
        }

        for s in &structs {
            // Rename Qail → QailCmd for Zig convention
            let name = if s.name == "Qail" { Some("QailCmd") } else { None };
            output.push_str(&gen_zig_struct(s, name));
            output.push('\n');
            total_structs += 1;
        }

        let zig_path = zig_output_dir.join(zig_file);
        std::fs::write(&zig_path, &output)
            .unwrap_or_else(|e| panic!("Cannot write {}: {}", zig_path.display(), e));

        println!("✅ {} → {} ({} enums, {} structs)",
            rust_file, zig_file, enums.len(), structs.len());
    }

    println!("\n🎯 Total: {} enums, {} structs generated", total_enums, total_structs);
    println!("📁 Output: {}", zig_output_dir.display());
}

/// Simple timestamp without chrono dependency.
fn chrono_like_now() -> String {
    use std::process::Command;
    Command::new("date")
        .arg("+%Y-%m-%dT%H:%M:%S")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "unknown".to_string())
        .trim()
        .to_string()
}
