//! Completion Handler - QAIL syntax suggestions

use std::collections::HashSet;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use crate::server::{EmbeddedQueryKind, QailLanguageServer};
use crate::utf16::Utf16Index;

/// QAIL keyword completions
const QAIL_KEYWORDS: &[(&str, &str)] = &[
    (
        "get",
        "SELECT query - get users fields id, email where id = :id",
    ),
    (
        "set",
        "UPDATE query - set users values name = :name where id = :id",
    ),
    (
        "add",
        "INSERT query - add users fields name, email values :name, :email",
    ),
    (
        "insert",
        "INSERT query - insert users fields name, email values :name, :email",
    ),
    ("count", "COUNT query - count users fields id"),
    ("cnt", "COUNT query - cnt users fields id"),
    ("export", "COPY TO query - export users fields id"),
    ("make", "DDL query - make users fields id:int"),
    ("create", "DDL query - create users fields id:int"),
    ("del", "DELETE query - del users where id = :id"),
    ("delete", "DELETE query - delete users where id = :id"),
    (
        "with",
        "CTE query - with recent as (get users fields id) get recent fields id",
    ),
    ("call", "CALL command - call refresh_materialized_views()"),
    (
        "do",
        "DO block - do $$ BEGIN RAISE NOTICE 'ok'; END; $$ language plpgsql",
    ),
    (
        "session set",
        "Session command - session set statement_timeout = '5000'",
    ),
    (
        "session show",
        "Session command - session show statement_timeout",
    ),
    (
        "session reset",
        "Session command - session reset statement_timeout",
    ),
    ("begin", "Transaction command - begin"),
    ("commit", "Transaction command - commit"),
    ("rollback", "Transaction command - rollback"),
];

/// QAIL operator completions
const QAIL_OPERATORS: &[(&str, &str)] = &[
    ("fields", "Select output columns"),
    ("where", "Filter rows"),
    ("order by", "Sort rows"),
    ("limit", "Limit row count"),
    ("offset", "Skip row count"),
    ("group by", "Group rows"),
    ("having", "Filter grouped rows"),
    ("on conflict", "UPSERT conflict handling"),
    ("join", "Join another table"),
    ("left join", "LEFT JOIN another table"),
    ("inner join", "INNER JOIN another table"),
];

impl QailLanguageServer {
    /// Handle completion request
    pub async fn handle_completion(
        &self,
        params: CompletionParams,
    ) -> Result<Option<CompletionResponse>> {
        let uri = params.text_document_position.text_document.uri.to_string();
        let position = params.text_document_position.position;
        let query = self.extract_query_at_position(&uri, position);
        let content = self.get_document(&uri);
        let mut items = Vec::new();

        if uri.ends_with(".rs") {
            let in_builder_context = content
                .as_deref()
                .is_some_and(|text| rust_builder_context(text, position));

            if !in_builder_context && query.is_none() {
                return Ok(None);
            }

            if in_builder_context {
                push_builder_method_items(&mut items);
            }

            if let Some(embedded) = query
                && embedded.kind == EmbeddedQueryKind::Qail
            {
                push_qail_keyword_items(&mut items);
                push_qail_operator_items(&mut items);
                if let Some(validator) = self.schema_validator_for_uri(&uri) {
                    push_schema_items(&mut items, &validator);
                }
            }
        } else {
            push_qail_keyword_items(&mut items);
            push_qail_operator_items(&mut items);

            if let Some(validator) = self.schema_validator_for_uri(&uri) {
                push_schema_items(&mut items, &validator);
            }
        }

        if items.is_empty() {
            return Ok(None);
        }

        let mut seen = HashSet::new();
        items.retain(|item| seen.insert(item.label.clone()));

        Ok(Some(CompletionResponse::Array(items)))
    }
}

fn rust_builder_context(content: &str, position: Position) -> bool {
    let index = Utf16Index::new(content);
    let Some(offset) = index.position_to_offset(position) else {
        return false;
    };

    let stmt_start = rust_statement_start_before(content, offset);

    content
        .get(stmt_start..offset)
        .is_some_and(rust_context_contains_qail_constructor)
}

fn rust_context_contains_qail_constructor(context: &str) -> bool {
    #[derive(Clone, Copy)]
    enum LexState {
        Code,
        LineComment,
        BlockComment { depth: usize },
        String { escaped: bool },
        Char { escaped: bool },
        RawString { hashes: usize },
    }

    let bytes = context.as_bytes();
    let limit = bytes.len();
    let mut i = 0usize;
    let mut state = LexState::Code;

    while i < limit {
        match state {
            LexState::Code => {
                if bytes[i] == b'/' && i + 1 < limit && bytes[i + 1] == b'/' {
                    state = LexState::LineComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'/' && i + 1 < limit && bytes[i + 1] == b'*' {
                    state = LexState::BlockComment { depth: 1 };
                    i += 2;
                    continue;
                }
                if let Some((raw_start, hashes)) = rust_raw_string_start(bytes, i, limit) {
                    state = LexState::RawString { hashes };
                    i = raw_start;
                    continue;
                }
                if bytes[i] == b'"' {
                    state = LexState::String { escaped: false };
                    i += 1;
                    continue;
                }
                if bytes[i] == b'\'' && rust_char_literal_end(bytes, i, limit).is_some() {
                    state = LexState::Char { escaped: false };
                    i += 1;
                    continue;
                }
                if starts_with_bytes(bytes, i, b"Qail::")
                    && (i == 0 || !is_ident_byte(bytes[i - 1]))
                {
                    return true;
                }
                i += 1;
            }
            LexState::LineComment => {
                if bytes[i] == b'\n' {
                    state = LexState::Code;
                }
                i += 1;
            }
            LexState::BlockComment { depth } => {
                if bytes[i] == b'/' && i + 1 < limit && bytes[i + 1] == b'*' {
                    state = LexState::BlockComment { depth: depth + 1 };
                    i += 2;
                } else if bytes[i] == b'*' && i + 1 < limit && bytes[i + 1] == b'/' {
                    if depth == 1 {
                        state = LexState::Code;
                    } else {
                        state = LexState::BlockComment { depth: depth - 1 };
                    }
                    i += 2;
                } else {
                    i += 1;
                }
            }
            LexState::String { escaped } => {
                if escaped {
                    state = LexState::String { escaped: false };
                } else if bytes[i] == b'\\' {
                    state = LexState::String { escaped: true };
                } else if bytes[i] == b'"' {
                    state = LexState::Code;
                }
                i += 1;
            }
            LexState::Char { escaped } => {
                if escaped {
                    state = LexState::Char { escaped: false };
                } else if bytes[i] == b'\\' {
                    state = LexState::Char { escaped: true };
                } else if bytes[i] == b'\'' {
                    state = LexState::Code;
                }
                i += 1;
            }
            LexState::RawString { hashes } => {
                if bytes[i] == b'"' && rust_raw_string_closes(bytes, i, hashes, limit) {
                    state = LexState::Code;
                    i += 1 + hashes;
                } else {
                    i += 1;
                }
            }
        }
    }

    false
}

fn rust_statement_start_before(content: &str, offset: usize) -> usize {
    #[derive(Clone, Copy)]
    enum LexState {
        Code,
        LineComment,
        BlockComment { depth: usize },
        String { escaped: bool },
        Char { escaped: bool },
        RawString { hashes: usize },
    }

    let bytes = content.as_bytes();
    let limit = offset.min(bytes.len());
    let mut stmt_start = 0;
    let mut i = 0;
    let mut state = LexState::Code;

    while i < limit {
        match state {
            LexState::Code => {
                if bytes[i] == b'/' && i + 1 < limit && bytes[i + 1] == b'/' {
                    state = LexState::LineComment;
                    i += 2;
                    continue;
                }
                if bytes[i] == b'/' && i + 1 < limit && bytes[i + 1] == b'*' {
                    state = LexState::BlockComment { depth: 1 };
                    i += 2;
                    continue;
                }
                if let Some((raw_start, hashes)) = rust_raw_string_start(bytes, i, limit) {
                    state = LexState::RawString { hashes };
                    i = raw_start;
                    continue;
                }
                if bytes[i] == b'"' {
                    state = LexState::String { escaped: false };
                    i += 1;
                    continue;
                }
                if bytes[i] == b'\'' && rust_char_literal_end(bytes, i, limit).is_some() {
                    state = LexState::Char { escaped: false };
                    i += 1;
                    continue;
                }
                if matches!(bytes[i], b';' | b'{' | b'}') {
                    stmt_start = i + 1;
                }
                i += 1;
            }
            LexState::LineComment => {
                if bytes[i] == b'\n' {
                    state = LexState::Code;
                }
                i += 1;
            }
            LexState::BlockComment { depth } => {
                if bytes[i] == b'/' && i + 1 < limit && bytes[i + 1] == b'*' {
                    state = LexState::BlockComment { depth: depth + 1 };
                    i += 2;
                } else if bytes[i] == b'*' && i + 1 < limit && bytes[i + 1] == b'/' {
                    if depth == 1 {
                        state = LexState::Code;
                    } else {
                        state = LexState::BlockComment { depth: depth - 1 };
                    }
                    i += 2;
                } else {
                    i += 1;
                }
            }
            LexState::String { escaped } => {
                if escaped {
                    state = LexState::String { escaped: false };
                } else if bytes[i] == b'\\' {
                    state = LexState::String { escaped: true };
                } else if bytes[i] == b'"' {
                    state = LexState::Code;
                }
                i += 1;
            }
            LexState::Char { escaped } => {
                if escaped {
                    state = LexState::Char { escaped: false };
                } else if bytes[i] == b'\\' {
                    state = LexState::Char { escaped: true };
                } else if bytes[i] == b'\'' {
                    state = LexState::Code;
                }
                i += 1;
            }
            LexState::RawString { hashes } => {
                if bytes[i] == b'"' && rust_raw_string_closes(bytes, i, hashes, limit) {
                    state = LexState::Code;
                    i += 1 + hashes;
                } else {
                    i += 1;
                }
            }
        }
    }

    stmt_start
}

fn starts_with_bytes(haystack: &[u8], idx: usize, needle: &[u8]) -> bool {
    haystack
        .get(idx..idx.saturating_add(needle.len()))
        .is_some_and(|s| s == needle)
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn rust_raw_string_start(bytes: &[u8], i: usize, limit: usize) -> Option<(usize, usize)> {
    let raw_idx = if bytes.get(i) == Some(&b'r') {
        i
    } else if matches!(bytes.get(i), Some(b'b' | b'c')) && bytes.get(i + 1) == Some(&b'r') {
        i + 1
    } else {
        return None;
    };

    let mut j = raw_idx + 1;
    let mut hashes = 0;
    while j < limit && bytes[j] == b'#' {
        hashes += 1;
        j += 1;
    }

    (j < limit && bytes[j] == b'"').then_some((j + 1, hashes))
}

fn rust_raw_string_closes(bytes: &[u8], quote_idx: usize, hashes: usize, limit: usize) -> bool {
    quote_idx + 1 + hashes <= limit
        && bytes[quote_idx + 1..quote_idx + 1 + hashes]
            .iter()
            .all(|b| *b == b'#')
}

fn rust_char_literal_end(bytes: &[u8], start: usize, limit: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'\'') {
        return None;
    }

    let mut i = start + 1;
    let mut escaped = false;
    while i < limit {
        match bytes[i] {
            b'\n' | b'\r' if !escaped => return None,
            b'\\' if !escaped => escaped = true,
            b'\'' if !escaped => return (i > start + 1).then_some(i + 1),
            _ => escaped = false,
        }
        i += 1;
    }

    None
}

fn push_qail_keyword_items(items: &mut Vec<CompletionItem>) {
    for (keyword, doc) in QAIL_KEYWORDS {
        items.push(CompletionItem {
            label: keyword.to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some(doc.to_string()),
            insert_text: Some(format!("{} ", keyword)),
            ..Default::default()
        });
    }
}

fn push_qail_operator_items(items: &mut Vec<CompletionItem>) {
    for (op, doc) in QAIL_OPERATORS {
        items.push(CompletionItem {
            label: op.to_string(),
            kind: Some(CompletionItemKind::OPERATOR),
            detail: Some(doc.to_string()),
            ..Default::default()
        });
    }
}

fn push_builder_method_items(items: &mut Vec<CompletionItem>) {
    let builder_methods = [
        ("Qail::get", "Start a SELECT query"),
        ("Qail::set", "Start an UPDATE query"),
        ("Qail::add", "Start an INSERT query"),
        ("Qail::del", "Start a DELETE query"),
        ("Qail::put", "Start an UPSERT query"),
        ("Qail::export", "Start a COPY TO query"),
        ("Qail::search", "Start a vector search query"),
        ("Qail::upsert", "Start a vector upsert query"),
        ("Qail::scroll", "Start a vector scroll query"),
        ("Qail::make", "Start a CREATE TABLE command"),
        ("Qail::truncate", "Start a TRUNCATE command"),
        ("Qail::explain", "Start an EXPLAIN command"),
        ("Qail::explain_analyze", "Start an EXPLAIN ANALYZE command"),
        ("Qail::lock", "Start a LOCK TABLE command"),
        (
            "Qail::create_materialized_view",
            "Start a CREATE MATERIALIZED VIEW command",
        ),
        (
            "Qail::refresh_materialized_view",
            "Start a REFRESH MATERIALIZED VIEW command",
        ),
        (
            "Qail::drop_materialized_view",
            "Start a DROP MATERIALIZED VIEW command",
        ),
        ("Qail::listen", "Start a LISTEN command"),
        ("Qail::unlisten", "Start an UNLISTEN command"),
        ("Qail::notify", "Start a NOTIFY command"),
        ("Qail::call", "Start a CALL command"),
        ("Qail::do_block", "Start a DO block command"),
        ("Qail::session_set", "Start a SET session command"),
        ("Qail::session_show", "Start a SHOW session command"),
        ("Qail::session_reset", "Start a RESET session command"),
        ("Qail::create_database", "Start a CREATE DATABASE command"),
        ("Qail::drop_database", "Start a DROP DATABASE command"),
        ("Qail::typed", "Start a typed query builder"),
        (".columns", "Specify columns to select"),
        (".column", "Append one selected column"),
        (".select_all", "Select all columns"),
        (".select_expr", "Append one selected expression"),
        (".select_exprs", "Append selected expressions"),
        (".column_expr", "Append one selected expression"),
        (".columns_expr", "Append selected expressions"),
        (".distinct_on", "Add DISTINCT ON columns"),
        (".distinct_on_expr", "Add DISTINCT ON expressions"),
        (".distinct_on_all", "Add DISTINCT ON all selected columns"),
        (".filter", "Add WHERE condition"),
        (".filter_cond", "Add raw WHERE condition"),
        (".or_filter", "Add OR WHERE condition group"),
        (".where_eq", "Add equality filter"),
        (".eq", "Add equality filter"),
        (".ne", "Add not-equal filter"),
        (".gt", "Add greater-than filter"),
        (".gte", "Add greater-than-or-equal filter"),
        (".lt", "Add less-than filter"),
        (".lte", "Add less-than-or-equal filter"),
        (".is_null", "Add IS NULL filter"),
        (".is_not_null", "Add IS NOT NULL filter"),
        (".like", "Add LIKE filter"),
        (".ilike", "Add ILIKE filter"),
        (".in_vals", "Add IN-list filter"),
        (
            ".array_elem_contained_in_text",
            "Filter by matching array elements in text",
        ),
        (".join_on", "Join through registered schema relation"),
        (
            ".join_on_optional",
            "Join through registered relation when available",
        ),
        (".join", "Add explicit join"),
        (".left_join", "Add LEFT JOIN"),
        (".inner_join", "Add INNER JOIN"),
        (".left_join_as", "Add aliased LEFT JOIN"),
        (".inner_join_as", "Add aliased INNER JOIN"),
        (".join_conds", "Add join with multiple ON conditions"),
        (".left_join_conds", "Add LEFT JOIN with multiple conditions"),
        (
            ".inner_join_conds",
            "Add INNER JOIN with multiple conditions",
        ),
        (".order_by", "Add ORDER BY clause"),
        (".order_by_expr", "Add ORDER BY expression"),
        (".order_desc", "Add descending ORDER BY clause"),
        (".order_asc", "Add ascending ORDER BY clause"),
        (".limit", "Add LIMIT clause"),
        (".offset", "Add OFFSET clause"),
        (".group_by", "Add GROUP BY clause"),
        (".group_by_expr", "Add GROUP BY expressions"),
        (".having_cond", "Add HAVING condition"),
        (".having_conds", "Add multiple HAVING conditions"),
        (".to_cte", "Convert query to a CTE definition"),
        (".with", "Add WITH CTE query"),
        (".with_cte", "Add CTE definition"),
        (".with_ctes", "Replace CTE definitions"),
        (".recursive", "Add recursive CTE part"),
        (".from_cte", "Read from a CTE"),
        (".select_from_cte", "Select columns from a CTE"),
        (".set_value", "Set column value for UPDATE/INSERT"),
        (".set_opt", "Set optional column value"),
        (".set_coalesce", "Set column with COALESCE"),
        (".set_coalesce_opt", "Set COALESCE only when value is Some"),
        (".values", "Set positional INSERT values"),
        (".update_from", "Add UPDATE FROM tables"),
        (".delete_using", "Add DELETE USING tables"),
        (".returning", "Add RETURNING clause"),
        (".returning_all", "Add RETURNING *"),
        (".on_conflict_update", "Add ON CONFLICT DO UPDATE"),
        (".on_conflict_nothing", "Add ON CONFLICT DO NOTHING"),
        (".with_rls", "Inject RLS context; returns QailBuildResult"),
        (".rls", "Apply RLS on a typed builder"),
        (".for_update", "Add FOR UPDATE row lock"),
        (
            ".for_update_skip_locked",
            "Add FOR UPDATE SKIP LOCKED row lock",
        ),
        (".for_no_key_update", "Add FOR NO KEY UPDATE row lock"),
        (".for_share", "Add FOR SHARE row lock"),
        (".for_key_share", "Add FOR KEY SHARE row lock"),
        (".fetch_first", "Add FETCH FIRST rows only"),
        (".fetch_with_ties", "Add FETCH FIRST rows with ties"),
        (".default_values", "Use INSERT DEFAULT VALUES"),
        (".overriding_system_value", "Add OVERRIDING SYSTEM VALUE"),
        (".overriding_user_value", "Add OVERRIDING USER VALUE"),
        (".tablesample_bernoulli", "Add TABLESAMPLE BERNOULLI"),
        (".tablesample_system", "Add TABLESAMPLE SYSTEM"),
        (".repeatable", "Add TABLESAMPLE REPEATABLE seed"),
        (".only", "Select from ONLY this table"),
        (".table_alias", "Set an alias for the source table"),
        (".vector", "Set vector search embedding"),
        (".vector_name", "Set named vector"),
        (".score_threshold", "Set vector score threshold"),
        (".with_vectors", "Include vectors in results"),
    ];

    for (method, doc) in builder_methods {
        items.push(CompletionItem {
            label: method.to_string(),
            kind: Some(CompletionItemKind::METHOD),
            detail: Some(doc.to_string()),
            ..Default::default()
        });
    }
}

fn push_schema_items(items: &mut Vec<CompletionItem>, validator: &qail_core::validator::Validator) {
    for table in validator.table_names() {
        items.push(CompletionItem {
            label: format!("get {}", table),
            kind: Some(CompletionItemKind::CLASS),
            detail: Some(format!("SELECT * FROM {}", table)),
            insert_text: Some(format!("get {} fields ", table)),
            ..Default::default()
        });
    }

    for table in validator.table_names() {
        if let Some(cols) = validator.column_names(table) {
            for col in cols {
                items.push(CompletionItem {
                    label: format!("{}.{}", table, col),
                    kind: Some(CompletionItemKind::FIELD),
                    detail: Some(format!("Column in {}", table)),
                    ..Default::default()
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position_at_marker(src: &str) -> (String, Position) {
        let marker = "/*cursor*/";
        let offset = src.find(marker).expect("test source should contain marker");
        let content = src.replace(marker, "");
        let position = Utf16Index::new(&content).offset_to_position(offset);
        (content, position)
    }

    #[test]
    fn rust_builder_context_detects_active_builder_statement() {
        let src = r#"let _q = Qail::get("users")
    .columns(["id"])
    ."#;

        assert!(rust_builder_context(
            src,
            Position {
                line: 2,
                character: 5,
            }
        ));
    }

    #[test]
    fn rust_builder_context_ignores_previous_statement() {
        let src = r#"let _q = Qail::get("users");
foo."#;

        assert!(!rust_builder_context(
            src,
            Position {
                line: 1,
                character: 4,
            }
        ));
    }

    #[test]
    fn rust_builder_context_ignores_qail_marker_inside_comment_only_context() {
        let (src, position) = position_at_marker(
            r#"// Qail::get("users")
foo./*cursor*/"#,
        );

        assert!(!rust_builder_context(&src, position));
    }

    #[test]
    fn rust_builder_context_ignores_qail_marker_inside_string_only_context() {
        let (src, position) = position_at_marker(
            r#"let _msg = "Qail::get(\"users\")"
    ./*cursor*/"#,
        );

        assert!(!rust_builder_context(&src, position));
    }

    #[test]
    fn rust_builder_context_ignores_boundaries_inside_string_literals() {
        let (src, position) = position_at_marker(
            r#"let _q = Qail::get("users")
    .filter_cond("email = 'foo;bar' AND meta ? '{tier}'")
    ./*cursor*/"#,
        );

        assert!(rust_builder_context(&src, position));
    }

    #[test]
    fn rust_builder_context_ignores_boundaries_inside_comments() {
        let (src, position) = position_at_marker(
            r#"let _q = Qail::get("users") // comment with ; { }
    /* block comment with ; { } */
    ./*cursor*/"#,
        );

        assert!(rust_builder_context(&src, position));
    }

    #[test]
    fn rust_builder_context_ignores_boundaries_inside_raw_strings() {
        let (src, position) = position_at_marker(
            r##"let _q = Qail::get("users")
    .filter_cond(r#"payload @> '{"roles":["admin;root"]}'"#)
    ./*cursor*/"##,
        );

        assert!(rust_builder_context(&src, position));
    }

    #[test]
    fn rust_builder_context_ignores_boundaries_inside_character_literals() {
        let (src, position) = position_at_marker(
            r#"let _q = Qail::get("users")
    .filter_cond(&format!("marker = {}", ';'))
    ./*cursor*/"#,
        );

        assert!(rust_builder_context(&src, position));
    }

    #[test]
    fn builder_completions_include_current_028_methods() {
        let mut items = Vec::new();
        push_builder_method_items(&mut items);
        let labels = items
            .into_iter()
            .map(|item| item.label)
            .collect::<HashSet<_>>();

        for label in [
            "Qail::search",
            "Qail::explain_analyze",
            "Qail::session_set",
            ".join_on",
            ".for_update_skip_locked",
            ".on_conflict_update",
            ".tablesample_bernoulli",
            ".with_rls",
        ] {
            assert!(labels.contains(label), "missing completion: {label}");
        }
    }

    #[test]
    fn qail_text_completions_include_current_actions() {
        let mut items = Vec::new();
        push_qail_keyword_items(&mut items);
        let labels = items
            .into_iter()
            .map(|item| item.label)
            .collect::<HashSet<_>>();

        for label in ["insert", "call", "session set", "begin"] {
            assert!(labels.contains(label), "missing keyword: {label}");
        }
    }
}
