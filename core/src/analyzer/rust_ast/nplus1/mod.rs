//! Semantic N+1 detection API for analyzer consumers (CLI/LSP).
//!
//! This module delegates to the shared semantic detector used by build-time
//! validation, then adapts diagnostics to the analyzer-facing types.

mod types;

pub use types::{NPlusOneCode, NPlusOneDiagnostic, NPlusOneSeverity};

use std::path::Path;

/// Detect N+1 patterns in a single Rust source file.
pub fn detect_n_plus_one_in_file(file: &str, source: &str) -> Vec<NPlusOneDiagnostic> {
    let semantic_diags = crate::build::nplus1_semantic::detect_n_plus_one_in_file(file, source);
    semantic_diags
        .into_iter()
        .map(|diag| map_diagnostic(diag, Some(source)))
        .collect()
}

/// Detect N+1 patterns in all Rust files under a directory.
pub fn detect_n_plus_one_in_dir(dir: &Path) -> Vec<NPlusOneDiagnostic> {
    let semantic_diags = crate::build::nplus1_semantic::detect_n_plus_one_in_dir(dir);
    semantic_diags
        .into_iter()
        .map(|diag| {
            let source = std::fs::read_to_string(&diag.file).ok();
            map_diagnostic(diag, source.as_deref())
        })
        .collect()
}

fn map_diagnostic(
    diag: crate::build::nplus1_semantic::NPlusOneDiagnostic,
    source: Option<&str>,
) -> NPlusOneDiagnostic {
    let code = match diag.code {
        crate::build::nplus1_semantic::NPlusOneCode::N1001 => NPlusOneCode::N1001,
        crate::build::nplus1_semantic::NPlusOneCode::N1002 => NPlusOneCode::N1002,
        crate::build::nplus1_semantic::NPlusOneCode::N1003 => NPlusOneCode::N1003,
        crate::build::nplus1_semantic::NPlusOneCode::N1004 => NPlusOneCode::N1004,
    };

    let severity = match diag.severity {
        crate::build::nplus1_semantic::NPlusOneSeverity::Warning => NPlusOneSeverity::Warning,
        crate::build::nplus1_semantic::NPlusOneSeverity::Error => NPlusOneSeverity::Error,
    };

    let end_column = source
        .and_then(|src| src.lines().nth(diag.line.saturating_sub(1)))
        .map(|line| infer_end_column(line, diag.column))
        .unwrap_or_else(|| diag.column.saturating_add(1));

    NPlusOneDiagnostic {
        code,
        severity,
        file: diag.file,
        line: diag.line,
        column: diag.column,
        end_column,
        message: diag.message,
        hint: diag.hint,
    }
}

fn infer_end_column(line: &str, column: usize) -> usize {
    if column == 0 {
        return 1;
    }
    let start = column.saturating_sub(1);
    let Some(slice) = line.get(start..) else {
        return column.saturating_add(1);
    };

    let mut chars = slice.char_indices();
    let mut ident_offset = 0usize;
    if let Some((_, first)) = chars.next()
        && first == '.'
    {
        ident_offset = 1;
    }

    let ident_slice = slice.get(ident_offset..).unwrap_or_default();
    let ident_len = ident_slice
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .count();

    if ident_len == 0 {
        column.saturating_add(1)
    } else {
        column + ident_offset + ident_len
    }
}
