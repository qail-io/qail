//! Handler unit tests.


#[cfg(test)]
mod tests {
    use super::super::rpc::{
        RpcFunctionName, build_rpc_sql, enforce_rpc_name_contract, matches_positional_signature,
        select_matching_rpc_signature, signature_matches_call as signature_matches,
    };
    use super::super::{primary_sort_for_cursor, parse_prefer_header};
    use crate::server::RpcCallableSignature;
    use serde_json::json;
    use std::collections::HashSet;

    fn sig(
        total_args: usize,
        default_args: usize,
        variadic: bool,
        arg_names: &[Option<&str>],
        arg_types: &[&str],
        identity: &str,
    ) -> RpcCallableSignature {
        RpcCallableSignature {
            total_args,
            default_args,
            variadic,
            arg_names: arg_names
                .iter()
                .map(|n| n.map(|v| v.to_ascii_lowercase()))
                .collect(),
            arg_types: arg_types.iter().map(|t| t.to_ascii_lowercase()).collect(),
            identity_args: identity.to_string(),
            result_type: "jsonb".to_string(),
        }
    }

    #[test]
    fn primary_sort_defaults_to_id_asc() {
        let (col, desc) = primary_sort_for_cursor(None);
        assert_eq!(col, "id");
        assert!(!desc);
    }

    #[test]
    fn primary_sort_parses_prefix_desc() {
        let (col, desc) = primary_sort_for_cursor(Some("-created_at,total:asc"));
        assert_eq!(col, "created_at");
        assert!(desc);
    }

    #[test]
    fn primary_sort_parses_prefix_asc() {
        let (col, desc) = primary_sort_for_cursor(Some("+created_at"));
        assert_eq!(col, "created_at");
        assert!(!desc);
    }

    #[test]
    fn primary_sort_parses_explicit_desc() {
        let (col, desc) = primary_sort_for_cursor(Some("created_at:desc,id:asc"));
        assert_eq!(col, "created_at");
        assert!(desc);
    }

    #[test]
    fn primary_sort_parses_plain_col() {
        let (col, desc) = primary_sort_for_cursor(Some("created_at"));
        assert_eq!(col, "created_at");
        assert!(!desc);
    }

    #[test]
    fn build_rpc_sql_named_args() {
        let args = serde_json::json!({
            "tenant_id": "abc",
            "limit": 10
        });
        let function = RpcFunctionName::parse("api.search_orders").unwrap();
        let sql = build_rpc_sql(&function, Some(&args)).unwrap();
        assert!(sql.starts_with("SELECT * FROM \"api\".\"search_orders\"("));
        assert!(sql.contains("\"limit\" => 10"));
        assert!(sql.contains("\"tenant_id\" => 'abc'"));
    }

    #[test]
    fn build_rpc_sql_rejects_unsafe_function_name() {
        let err = RpcFunctionName::parse("search_orders;DROP TABLE users").unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rpc_name_contract_requires_schema_when_enabled() {
        let function = RpcFunctionName::parse("search_orders").unwrap();
        let err = enforce_rpc_name_contract(true, None, &function).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rpc_name_contract_enforces_allow_list() {
        let function = RpcFunctionName::parse("api.search_orders").unwrap();
        let mut allow = HashSet::new();
        allow.insert("api.other_fn".to_string());

        let err = enforce_rpc_name_contract(false, Some(&allow), &function).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn rpc_signature_named_requires_required_args() {
        let signature = sig(
            2,
            1,
            false,
            &[Some("tenant_id"), Some("limit")],
            &["uuid", "integer"],
            "tenant_id uuid, limit integer",
        );
        let args = json!({ "limit": 10 });
        assert!(!signature_matches(&signature, Some(&args)));
    }

    #[test]
    fn rpc_signature_positional_allows_defaults() {
        let signature = sig(
            2,
            1,
            false,
            &[Some("tenant_id"), Some("limit")],
            &["uuid", "integer"],
            "tenant_id uuid, limit integer",
        );
        let args = json!(["550e8400-e29b-41d4-a716-446655440000"]);
        assert!(signature_matches(&signature, Some(&args)));
    }

    #[test]
    fn rpc_signature_variadic_accepts_many_positional() {
        let signature = sig(
            2,
            0,
            true,
            &[Some("prefix"), Some("ids")],
            &["text", "integer[]"],
            "prefix text, variadic ids integer[]",
        );
        let args = vec![json!("pre"), json!(1), json!(2), json!(3)];
        assert!(matches_positional_signature(&signature, &args));
    }

    #[test]
    fn rpc_signature_select_rejects_ambiguous_overloads() {
        let signatures = vec![
            sig(1, 0, false, &[Some("id")], &["integer"], "id integer"),
            sig(1, 0, false, &[Some("id")], &["bigint"], "id bigint"),
        ];
        let args = json!([1]);
        let err =
            select_matching_rpc_signature("api.lookup", &signatures, Some(&args)).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rpc_signature_select_rejects_type_mismatch() {
        let signatures = vec![sig(
            1,
            0,
            false,
            &[Some("enabled")],
            &["boolean"],
            "enabled boolean",
        )];
        let args = json!(["not_bool"]);
        let err =
            select_matching_rpc_signature("api.toggle", &signatures, Some(&args)).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 4: Prefer header parsing
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn prefer_merge_duplicates() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("prefer", "resolution=merge-duplicates".parse().unwrap());
        let prefer = parse_prefer_header(&headers);
        assert!(prefer.wants_upsert());
        assert!(!prefer.wants_ignore_duplicates());
        assert!(!prefer.wants_minimal());
    }

    #[test]
    fn prefer_ignore_duplicates() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("prefer", "resolution=ignore-duplicates".parse().unwrap());
        let prefer = parse_prefer_header(&headers);
        assert!(!prefer.wants_upsert());
        assert!(prefer.wants_ignore_duplicates());
    }

    #[test]
    fn prefer_return_minimal() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("prefer", "return=minimal".parse().unwrap());
        let prefer = parse_prefer_header(&headers);
        assert!(prefer.wants_minimal());
    }

    #[test]
    fn prefer_return_headers_only() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("prefer", "return=headers-only".parse().unwrap());
        let prefer = parse_prefer_header(&headers);
        assert!(prefer.wants_minimal());
    }

    #[test]
    fn prefer_combined_directives() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "prefer",
            "resolution=merge-duplicates,return=representation"
                .parse()
                .unwrap(),
        );
        let prefer = parse_prefer_header(&headers);
        assert!(prefer.wants_upsert());
        assert!(!prefer.wants_minimal());
        assert_eq!(prefer.return_mode.as_deref(), Some("representation"));
    }

    #[test]
    fn prefer_empty_header_is_noop() {
        let headers = axum::http::HeaderMap::new();
        let prefer = parse_prefer_header(&headers);
        assert!(!prefer.wants_upsert());
        assert!(!prefer.wants_ignore_duplicates());
        assert!(!prefer.wants_minimal());
    }
}
