//! Handler unit tests.

#[cfg(test)]
mod suite {
    use super::super::rpc::{
        RpcFunctionName, build_rpc_bound_sql, build_rpc_probe_sql, build_rpc_sql,
        enforce_rpc_name_contract, matches_positional_signature, select_matching_rpc_signature,
        signature_matches_call as signature_matches,
    };
    use super::super::{parse_prefer_header, primary_sort_for_cursor};
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
            arg_type_oids: vec![0; arg_types.len()],
            variadic_element_oid: None,
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
    fn build_rpc_probe_sql_uses_scalar_select_context() {
        let args = serde_json::json!({
            "tenant_id": "abc",
            "limit": 10
        });
        let function = RpcFunctionName::parse("api.search_orders").unwrap();
        let sql = build_rpc_probe_sql(&function, Some(&args)).unwrap();
        assert!(sql.starts_with("SELECT \"api\".\"search_orders\"("));
        assert!(!sql.contains("SELECT * FROM"));
    }

    #[test]
    fn build_rpc_bound_sql_uses_typed_placeholders_for_named_args() {
        let args = serde_json::json!({
            "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
            "limit": 10
        });
        let signature = sig(
            2,
            0,
            false,
            &[Some("tenant_id"), Some("limit")],
            &["uuid", "integer"],
            "tenant_id uuid, limit integer",
        );
        let function = RpcFunctionName::parse("api.search_orders").unwrap();
        let query = build_rpc_bound_sql(&function, Some(&args), Some(&signature), false).unwrap();

        assert_eq!(
            query.sql,
            "SELECT * FROM \"api\".\"search_orders\"(\"limit\" => $1, \"tenant_id\" => $2)"
        );
        assert_eq!(query.params[0].as_deref(), Some(b"10".as_slice()));
        assert_eq!(
            query.params[1].as_deref(),
            Some(b"550e8400-e29b-41d4-a716-446655440000".as_slice())
        );
        assert_eq!(query.param_type_oids, vec![0, 0]);
    }

    #[test]
    fn build_rpc_bound_sql_encodes_json_arguments_as_json_text() {
        let args = serde_json::json!({
            "payload": "abc"
        });
        let signature = sig(1, 0, false, &[Some("payload")], &["jsonb"], "payload jsonb");
        let function = RpcFunctionName::parse("api.echo_json").unwrap();
        let query = build_rpc_bound_sql(&function, Some(&args), Some(&signature), true).unwrap();

        assert_eq!(query.sql, "SELECT \"api\".\"echo_json\"(\"payload\" => $1)");
        assert_eq!(query.params[0].as_deref(), Some(br#""abc""#.as_slice()));
        assert_eq!(query.param_type_oids, vec![0]);
    }

    #[test]
    fn build_rpc_bound_sql_encodes_native_pg_array_arguments() {
        let args = serde_json::json!([[1, 2, 3]]);
        let signature = sig(1, 0, false, &[Some("ids")], &["integer[]"], "ids integer[]");
        let function = RpcFunctionName::parse("api.lookup_many").unwrap();
        let query = build_rpc_bound_sql(&function, Some(&args), Some(&signature), false).unwrap();

        assert_eq!(query.sql, "SELECT * FROM \"api\".\"lookup_many\"($1)");
        assert_eq!(query.params[0].as_deref(), Some(b"{1,2,3}".as_slice()));
        assert_eq!(query.param_type_oids, vec![0]);
    }

    #[test]
    fn build_rpc_sql_escapes_backslashes_in_string_args() {
        let args = serde_json::json!({
            "path": "C:\\temp\\logs\\today.txt"
        });
        let function = RpcFunctionName::parse("api.read_file").unwrap();
        let sql = build_rpc_sql(&function, Some(&args)).unwrap();
        assert!(
            sql.contains("\"path\" => 'C:\\\\temp\\\\logs\\\\today.txt'"),
            "RPC SQL must escape backslashes in string literals: {}",
            sql
        );
    }

    #[test]
    fn build_rpc_sql_strips_nul_from_string_args() {
        let args = serde_json::json!({
            "payload": "ab\u{0000}cd"
        });
        let function = RpcFunctionName::parse("api.process_payload").unwrap();
        let sql = build_rpc_sql(&function, Some(&args)).unwrap();
        assert!(!sql.contains('\0'), "RPC SQL must not contain NUL bytes");
        assert!(
            sql.contains("'abcd'"),
            "NUL bytes must be stripped: {}",
            sql
        );
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
    fn rpc_signature_named_allows_omitting_variadic_tail() {
        let signature = sig(
            2,
            0,
            true,
            &[Some("prefix"), Some("ids")],
            &["text", "integer[]"],
            "prefix text, variadic ids integer[]",
        );
        let args = json!({ "prefix": "pre" });
        assert!(signature_matches(&signature, Some(&args)));
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
