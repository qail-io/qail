//! Shared SQL statement kind classification for analyzer modules.
//!
//! This classifier is quote/parenthesis aware and checks top-level SQL tokens,
//! so it avoids the broad `contains(...)` heuristics used previously.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlStmtKind {
    Select,
    Insert,
    Update,
    Delete,
}

impl SqlStmtKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

/// Classify SQL statement kind from raw SQL text.
pub(crate) fn classify_sql_kind(sql: &str) -> Option<SqlStmtKind> {
    let normalized = normalize_whitespace(sql);
    if normalized.is_empty() {
        return None;
    }

    let mut candidates = Vec::new();
    if let Some(pos) = find_keyword_top_level_from(&normalized, "SELECT", 0) {
        candidates.push((pos, SqlStmtKind::Select));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "INSERT", 0) {
        candidates.push((pos, SqlStmtKind::Insert));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "UPDATE", 0) {
        candidates.push((pos, SqlStmtKind::Update));
    }
    if let Some(pos) = find_keyword_top_level_from(&normalized, "DELETE", 0) {
        candidates.push((pos, SqlStmtKind::Delete));
    }

    let (_, kind) = candidates.into_iter().min_by_key(|(pos, _)| *pos)?;
    match kind {
        SqlStmtKind::Select => find_keyword_top_level_from(&normalized, "FROM", 0)
            .is_some()
            .then_some(SqlStmtKind::Select),
        SqlStmtKind::Insert => {
            let insert_pos = find_keyword_top_level_from(&normalized, "INSERT", 0)?;
            find_keyword_top_level_from(&normalized, "INTO", insert_pos + "INSERT".len())
                .is_some()
                .then_some(SqlStmtKind::Insert)
        }
        SqlStmtKind::Update => {
            let update_pos = find_keyword_top_level_from(&normalized, "UPDATE", 0)?;
            find_keyword_top_level_from(&normalized, "SET", update_pos + "UPDATE".len())
                .is_some()
                .then_some(SqlStmtKind::Update)
        }
        SqlStmtKind::Delete => {
            let delete_pos = find_keyword_top_level_from(&normalized, "DELETE", 0)?;
            find_keyword_top_level_from(&normalized, "FROM", delete_pos + "DELETE".len())
                .is_some()
                .then_some(SqlStmtKind::Delete)
        }
    }
}

fn normalize_whitespace(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn find_keyword_top_level_from(text: &str, keyword: &str, min_idx: usize) -> Option<usize> {
    if keyword.is_empty() {
        return None;
    }

    let bytes = text.as_bytes();
    let upper = bytes
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let kw = keyword
        .as_bytes()
        .iter()
        .map(|b| b.to_ascii_uppercase())
        .collect::<Vec<_>>();

    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;
    let mut i = 0usize;

    while i < bytes.len() {
        let b = bytes[i];

        if let Some(q) = in_quote {
            if b == q {
                if matches!(q, b'\'' | b'"') && bytes.get(i + 1).copied() == Some(q) {
                    i += 2;
                    continue;
                }
                in_quote = None;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' | b'"' | b'`' => {
                in_quote = Some(b);
                i += 1;
                continue;
            }
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }

        if depth == 0
            && i >= min_idx
            && upper
                .get(i..i.saturating_add(kw.len()))
                .is_some_and(|slice| slice == kw)
        {
            let before_ok = if i == 0 {
                true
            } else {
                !is_ident_byte(upper[i - 1])
            };
            let after = i + kw.len();
            let after_ok = if after >= upper.len() {
                true
            } else {
                !is_ident_byte(upper[after])
            };

            if before_ok && after_ok {
                return Some(i);
            }
        }

        i += 1;
    }

    None
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_basic_dml() {
        assert_eq!(
            classify_sql_kind("SELECT id FROM users"),
            Some(SqlStmtKind::Select)
        );
        assert_eq!(
            classify_sql_kind("INSERT INTO users (id) VALUES ($1)"),
            Some(SqlStmtKind::Insert)
        );
        assert_eq!(
            classify_sql_kind("UPDATE users SET active = true"),
            Some(SqlStmtKind::Update)
        );
        assert_eq!(
            classify_sql_kind("DELETE FROM users WHERE id = $1"),
            Some(SqlStmtKind::Delete)
        );
    }

    #[test]
    fn classifies_cte_by_outer_statement() {
        let cte_select = "WITH x AS (SELECT id FROM users) SELECT * FROM x";
        let cte_delete =
            "WITH x AS (SELECT id FROM users) DELETE FROM users WHERE id IN (SELECT id FROM x)";

        assert_eq!(classify_sql_kind(cte_select), Some(SqlStmtKind::Select));
        assert_eq!(classify_sql_kind(cte_delete), Some(SqlStmtKind::Delete));
    }

    #[test]
    fn ignores_keywords_inside_strings() {
        let sql = "UPDATE users SET note = 'DELETE FROM x', active = true";
        assert_eq!(classify_sql_kind(sql), Some(SqlStmtKind::Update));
    }
}
