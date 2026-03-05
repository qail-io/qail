//! Suppression comment parsing for N+1 lint rules.

use super::types::{NPlusOneCode, Suppressions};
use std::collections::HashSet;

pub(super) fn parse_suppressions(source: &str) -> Suppressions {
    let mut suppressions = HashSet::new();

    for (idx, line) in source.lines().enumerate() {
        let trimmed = line.trim();
        let line_number = idx + 1;

        if let Some(rest) = trimmed.strip_prefix("// qail-lint:disable-next-line") {
            for code in parse_code_list(rest) {
                suppressions.insert((line_number + 1, code));
            }
        }
        if let Some(rest) = trimmed.strip_prefix("// qail-lint:disable-line") {
            for code in parse_code_list(rest) {
                suppressions.insert((line_number, code));
            }
        }
        if let Some(pos) = trimmed.find("// qail-lint:disable-line")
            && pos > 0
        {
            let rest = &trimmed[pos + "// qail-lint:disable-line".len()..];
            for code in parse_code_list(rest) {
                suppressions.insert((line_number, code));
            }
        }
    }

    suppressions
}

pub(super) fn parse_code_list(s: &str) -> Vec<NPlusOneCode> {
    let mut codes = Vec::new();
    for token in s.split_whitespace() {
        match token.trim_matches(',') {
            "N1-001" | "N1001" => codes.push(NPlusOneCode::N1001),
            "N1-002" | "N1002" => codes.push(NPlusOneCode::N1002),
            "N1-003" | "N1003" => codes.push(NPlusOneCode::N1003),
            "N1-004" | "N1004" => codes.push(NPlusOneCode::N1004),
            _ => {}
        }
    }
    codes
}
