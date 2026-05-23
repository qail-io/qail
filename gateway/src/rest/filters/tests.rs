use super::*;

#[test]
fn test_parse_filters_basic() {
    let filters = parse_filters("name.eq=John&age.gte=18");
    assert_eq!(filters.len(), 2);
    assert_eq!(filters[0].0, "name");
    assert!(matches!(filters[0].1, Operator::Eq));
    assert_eq!(filters[1].0, "age");
    assert!(matches!(filters[1].1, Operator::Gte));
}

#[test]
fn test_parse_identifier_csv_accepts_documented_format() {
    let cols = parse_identifier_csv("name, description ,name").expect("valid CSV identifiers");
    assert_eq!(cols, vec!["name", "description"]);
}

#[test]
fn test_parse_identifier_csv_rejects_invalid_entries() {
    assert!(parse_identifier_csv("").is_err());
    assert!(parse_identifier_csv("name,").is_err());
    assert!(parse_identifier_csv("name,bad-col").is_err());
}

#[test]
fn test_parse_select_columns_rejects_fail_open_projection() {
    assert_eq!(
        parse_select_columns("id, name").unwrap(),
        vec!["id".to_string(), "name".to_string()]
    );
    assert_eq!(parse_select_columns("*").unwrap(), vec!["*".to_string()]);
    assert!(parse_select_columns("password-hash").is_err());
    assert!(parse_select_columns("id,").is_err());
    assert!(parse_select_columns("*,id").is_err());
}

#[test]
fn test_parse_expand_relations_rejects_fail_open_inputs() {
    let (flat, nested) =
        parse_expand_relations("users,nested:items,users,nested:items", 3).unwrap();
    assert_eq!(flat, vec!["users"]);
    assert_eq!(nested, vec!["items"]);

    assert!(parse_expand_relations("", 3).is_err());
    assert!(parse_expand_relations("users,", 3).is_err());
    assert!(parse_expand_relations("nested:", 3).is_err());
    assert!(parse_expand_relations("bad-rel", 3).is_err());
    assert!(parse_expand_relations("nested:bad-rel", 3).is_err());
    assert!(parse_expand_relations("users,nested:items,nested:payments", 2).is_err());
}

#[test]
fn test_parse_filters_checked_rejects_invalid_filter_column() {
    let err = parse_filters_checked("password-hash.eq=secret").unwrap_err();
    assert!(err.contains("Invalid filter column"));
}

#[test]
fn test_parse_filters_checked_rejects_invalid_percent_encoding() {
    let err = parse_filters_checked("name.eq=%E0%A4%A").unwrap_err();
    assert!(err.contains("Invalid percent-encoded filter value"));
}

#[test]
fn test_parse_filters_checked_rejects_malformed_pairs() {
    let err = parse_filters_checked("status.eq").unwrap_err();
    assert!(err.contains("Malformed filter parameter"));
}

#[test]
fn test_parse_filters_checked_rejects_empty_in_lists() {
    let err = parse_filters_checked("status.in=").unwrap_err();
    assert!(err.contains("requires at least one value"));

    let err = parse_filters_checked("status=in.()").unwrap_err();
    assert!(err.contains("requires at least one value"));

    let err = parse_filters_checked("status.not_in=()").unwrap_err();
    assert!(err.contains("requires at least one value"));
}

#[test]
fn test_parse_filters_in() {
    let filters = parse_filters("status.in=active,pending,closed");
    assert_eq!(filters.len(), 1);
    assert!(matches!(filters[0].1, Operator::In));
    if let QailValue::Array(vals) = &filters[0].2 {
        assert_eq!(vals.len(), 3);
    } else {
        panic!("Expected Array value for IN filter");
    }
}

#[test]
fn test_identifier_guard_strict_segments() {
    assert!(is_safe_identifier("users"));
    assert!(is_safe_identifier("public.users"));
    assert!(is_safe_identifier("_meta.v1_table"));

    assert!(!is_safe_identifier(""));
    assert!(!is_safe_identifier("1users"));
    assert!(!is_safe_identifier("users-name"));
    assert!(!is_safe_identifier("users--name"));
    assert!(!is_safe_identifier("users..name"));
    assert!(!is_safe_identifier(".users"));
    assert!(!is_safe_identifier("users."));
    assert!(!is_safe_identifier("users;drop"));
}

#[test]
fn test_parse_filters_is_null() {
    let filters = parse_filters("deleted_at.is_null=true");
    assert_eq!(filters.len(), 1);
    assert!(matches!(filters[0].1, Operator::IsNull));
}

#[test]
fn test_parse_filters_no_operator() {
    let filters = parse_filters("name=John");
    assert_eq!(filters.len(), 1);
    assert_eq!(filters[0].0, "name");
    assert!(matches!(filters[0].1, Operator::Eq));
}

#[test]
fn test_parse_filters_skips_reserved() {
    let filters = parse_filters("limit=10&offset=0&name.eq=John&sort=id:asc");
    assert_eq!(filters.len(), 1);
    assert_eq!(filters[0].0, "name");
}

#[test]
fn test_parse_scalar_value() {
    assert!(matches!(parse_scalar_value("42"), QailValue::Int(42)));
    assert!(matches!(parse_scalar_value("3.14"), QailValue::Float(_)));
    assert!(matches!(parse_scalar_value("true"), QailValue::Bool(true)));
    assert!(matches!(parse_scalar_value("null"), QailValue::Null));
    assert!(matches!(parse_scalar_value("hello"), QailValue::String(_)));
}

#[test]
fn test_sql_injection_in_filter_value() {
    let payloads = vec![
        "'; DROP TABLE users; --",
        "1 OR 1=1",
        "1; SELECT * FROM pg_shadow",
        "' UNION SELECT password FROM users --",
        "Robert'); DROP TABLE students;--",
        "1' AND '1'='1",
        "admin'--",
        "' OR ''='",
    ];
    for payload in payloads {
        let qs = format!("name.eq={}", urlencoding::encode(payload));
        let filters = parse_filters(&qs);
        assert_eq!(
            filters.len(),
            1,
            "Injection payload should produce exactly 1 filter"
        );
        match &filters[0].2 {
            QailValue::String(s) => assert_eq!(s, payload),
            QailValue::Int(_) | QailValue::Float(_) => {}
            _ => {}
        }
    }
}

#[test]
fn test_null_bytes_in_filter() {
    let filters = parse_filters("name.eq=hello%00world");
    assert_eq!(filters.len(), 1);
}

#[test]
fn test_extremely_long_value() {
    let long_val = "a".repeat(100_000);
    let qs = format!("name.eq={}", long_val);
    let filters = parse_filters(&qs);
    assert_eq!(filters.len(), 1);
}

#[test]
fn test_empty_and_malformed_query_strings() {
    assert!(parse_filters("").is_empty());
    assert!(parse_filters("&&&").is_empty());
    assert!(parse_filters("key_no_value").is_empty());
    let f = parse_filters("col.eq=");
    assert_eq!(f.len(), 1);
}

#[test]
fn test_unicode_in_filters() {
    let filters = parse_filters("name.eq=日本語テスト&city.like=%E4%B8%8A%E6%B5%B7");
    assert_eq!(filters.len(), 2);
    match &filters[0].2 {
        QailValue::String(s) => assert_eq!(s, "日本語テスト"),
        _ => panic!("Expected unicode string"),
    }
}

mod fuzz {
    use super::*;
    use proptest::prelude::*;

    fn arb_query_string() -> impl Strategy<Value = String> {
        prop::collection::vec(
            (
                "[a-z_]{1,20}",
                prop_oneof![
                    Just("eq"),
                    Just("ne"),
                    Just("gt"),
                    Just("gte"),
                    Just("lt"),
                    Just("lte"),
                    Just("like"),
                    Just("ilike"),
                    Just("in"),
                    Just("not_in"),
                    Just("is_null"),
                    Just("contains"),
                    Just("unknown_op"),
                ],
                ".*",
            ),
            0..10,
        )
        .prop_map(|pairs| {
            pairs
                .into_iter()
                .map(|(col, op, val)| format!("{}.{}={}", col, op, urlencoding::encode(&val)))
                .collect::<Vec<_>>()
                .join("&")
        })
    }

    proptest! {
        #[test]
        fn fuzz_parse_filters_never_panics(qs in ".*") {
            let _ = parse_filters(&qs);
        }

        #[test]
        fn fuzz_parse_scalar_value_never_panics(s in ".*") {
            let _ = parse_scalar_value(&s);
        }

        #[test]
        fn fuzz_structured_filters(qs in arb_query_string()) {
            let filters = parse_filters(&qs);
            for (col, _op, _val) in &filters {
                prop_assert!(!col.is_empty(), "Column name must not be empty");
            }
        }

        #[test]
        fn fuzz_reserved_params_filtered(
            col in prop_oneof![
                Just("limit"), Just("offset"), Just("sort"),
                Just("select"), Just("expand"), Just("cursor"),
                Just("distinct"), Just("returning"),
            ],
            val in "[a-z0-9]{1,10}"
        ) {
            let qs = format!("{}={}", col, val);
            let filters = parse_filters(&qs);
            prop_assert!(filters.is_empty(), "Reserved param '{}' should not become a filter", col);
        }

        #[test]
        fn fuzz_scalar_value_is_valid(s in "[^\\u{0}]{0,1000}") {
            let val = parse_scalar_value(&s);
            let _ = val;
        }
    }
}

#[test]
fn test_parse_filters_value_style() {
    let filters = parse_filters(
        "status=ne.cancelled&total=gt.100&notes=is_null&tags=contains.premium&name=like.*ferry*",
    );
    assert_eq!(filters.len(), 5);

    assert_eq!(filters[0].0, "status");
    assert!(matches!(filters[0].1, Operator::Ne));
    assert!(matches!(filters[0].2, QailValue::String(_)));

    assert_eq!(filters[1].0, "total");
    assert!(matches!(filters[1].1, Operator::Gt));
    assert!(matches!(filters[1].2, QailValue::Int(100)));

    assert_eq!(filters[2].0, "notes");
    assert!(matches!(filters[2].1, Operator::IsNull));
    assert!(matches!(filters[2].2, QailValue::Null));

    assert_eq!(filters[3].0, "tags");
    assert!(matches!(filters[3].1, Operator::Contains));
    assert!(matches!(filters[3].2, QailValue::String(_)));

    assert_eq!(filters[4].0, "name");
    assert!(matches!(filters[4].1, Operator::Like));
    match &filters[4].2 {
        QailValue::String(s) => assert_eq!(s, "%ferry%"),
        _ => panic!("Expected LIKE pattern as string"),
    }
}

#[test]
fn test_parse_filters_value_style_in_parentheses() {
    let filters = parse_filters("status=in.(active,pending,closed)");
    assert_eq!(filters.len(), 1);
    assert!(matches!(filters[0].1, Operator::In));
    match &filters[0].2 {
        QailValue::Array(vals) => assert_eq!(vals.len(), 3),
        _ => panic!("Expected Array value for IN filter"),
    }
}

#[test]
fn test_apply_sorting_supports_prefix_desc() {
    use qail_core::transpiler::ToSql;

    let cmd = qail_core::ast::Qail::get("orders");
    let cmd = apply_sorting(cmd, "-total,created_at").unwrap();
    let sql = cmd.to_sql();
    assert_eq!(
        sql,
        "SELECT * FROM orders ORDER BY total DESC, created_at ASC"
    );
}

#[test]
fn test_apply_sorting_rejects_invalid_sort_inputs() {
    let cmd = qail_core::ast::Qail::get("orders");
    assert!(apply_sorting(cmd.clone(), "total:sideways").is_err());
    assert!(apply_sorting(cmd.clone(), "total,").is_err());
    assert!(apply_sorting(cmd.clone(), "total;drop").is_err());
    assert!(apply_sorting(cmd, "-").is_err());
}

#[test]
fn test_apply_returning_rejects_invalid_projection_inputs() {
    let cmd = qail_core::ast::Qail::set("orders");
    assert!(apply_returning(cmd.clone(), Some("id,")).is_err());
    assert!(apply_returning(cmd.clone(), Some("id;drop")).is_err());
    assert!(apply_returning(cmd.clone(), Some("*,id")).is_err());
    assert!(apply_returning(cmd, Some("")).is_err());
}
