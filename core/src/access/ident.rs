use std::collections::BTreeSet;

use crate::ast::Qail;

pub(super) fn normalize_column_name(name: impl Into<String>) -> String {
    let name = name.into();
    name.rsplit('.')
        .next()
        .unwrap_or(&name)
        .trim_matches('"')
        .to_ascii_lowercase()
}

pub(super) fn normalize_identifier_part(part: &str) -> String {
    part.trim().trim_matches('"').to_ascii_lowercase()
}

pub(super) fn normalize_table_ref(table_ref: &str) -> String {
    normalize_qualified_identifier(first_table_ref_token(table_ref))
}

pub(super) fn target_refs_for_command(cmd: &Qail, table: &str) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();
    refs.insert(table.to_string());
    if let Some(short_name) = table.rsplit('.').next()
        && short_name != table
    {
        refs.insert(short_name.to_string());
    }
    if let Some(alias) = table_alias(&cmd.table) {
        refs.insert(alias);
    }
    if let Some(target_alias) = cmd
        .merge
        .as_ref()
        .and_then(|merge| merge.target_alias.as_deref())
    {
        refs.insert(normalize_identifier_part(target_alias));
    }
    refs
}

fn table_alias(table_ref: &str) -> Option<String> {
    let mut tokens = table_ref.split_whitespace();
    tokens.next()?;
    let token = tokens.next()?;
    let alias = if token.eq_ignore_ascii_case("as") {
        tokens.next()?
    } else {
        token
    };
    Some(normalize_identifier_part(alias)).filter(|alias| !alias.is_empty())
}

fn first_table_ref_token(table_ref: &str) -> &str {
    let trimmed = table_ref.trim_start();
    let mut in_double = false;
    let mut chars = trimmed.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        match ch {
            '"' => {
                if in_double && chars.peek().is_some_and(|(_, next)| *next == '"') {
                    chars.next();
                } else {
                    in_double = !in_double;
                }
            }
            _ if ch.is_whitespace() && !in_double => return &trimmed[..idx],
            _ => {}
        }
    }

    trimmed
}

fn normalize_qualified_identifier(value: &str) -> String {
    value
        .split('.')
        .map(normalize_identifier_part)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(".")
}
