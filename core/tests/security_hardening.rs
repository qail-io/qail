//! Adversarial Security Hardening Tests
//!
//! Covers every known SQL injection, encoding attack, value type confusion,
//! and operator abuse vector against the Qail AST transpiler.
//!
//! Attack classifications:
//!   1. Classic SQL Injection (string, identifier, comment)
//!   2. LIKE/Pattern Injection (wildcard, backslash)
//!   3. Encoding Attacks (null byte, unicode, BOM, RTL)
//!   4. Value Type Confusion (NaN, infinity, overflow, empty)
//!   5. Operator Abuse (BETWEEN, EXISTS, IN, raw SQL hatch)
//!   6. Cross-Site via SQL (XSS, HTML entities)
//!   7. Combined / Multi-Vector attacks

use qail_core::ast::*;
use qail_core::transpiler::ToSql;

// ============================================================================
// Helpers — same pattern as transpiler_correctness.rs
// ============================================================================

fn filter_cage(conditions: Vec<Condition>) -> Cage {
    Cage {
        kind: CageKind::Filter,
        conditions,
        logical_op: LogicalOp::And,
    }
}

fn payload_cage(conditions: Vec<Condition>) -> Cage {
    Cage {
        kind: CageKind::Payload,
        conditions,
        logical_op: LogicalOp::And,
    }
}

fn cond(col: &str, op: Operator, val: Value) -> Condition {
    Condition {
        left: Expr::Named(col.to_string()),
        op,
        value: val,
        is_array_unnest: false,
    }
}

fn select_where(table: &str, col: &str, op: Operator, val: Value) -> Qail {
    Qail {
        action: Action::Get,
        table: table.to_string(),
        cages: vec![filter_cage(vec![cond(col, op, val)])],
        ..Default::default()
    }
}

fn select_from(table: &str) -> Qail {
    Qail {
        action: Action::Get,
        table: table.to_string(),
        ..Default::default()
    }
}

// ============================================================================
// 1. CLASSIC SQL INJECTION
// ============================================================================

#[test]
fn injection_single_quote_in_string_value() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Eq,
        Value::String("O'Brien".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("O''Brien"),
        "Single quote not escaped: {}",
        sql
    );
    assert!(
        !sql.contains("O'Brien'"),
        "Unescaped quote breaks syntax: {}",
        sql
    );
}

#[test]
fn injection_double_single_quote_in_string() {
    let cmd = select_where(
        "users",
        "bio",
        Operator::Eq,
        Value::String("it''s fine".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("it''''s fine"),
        "Double-escaped quotes: {}",
        sql
    );
}

#[test]
fn injection_semicolon_in_string_value() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Eq,
        Value::String("alice'; DROP TABLE users; --".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("alice''; DROP TABLE users; --"),
        "Semicolon injection not neutralized: {}",
        sql
    );
}

#[test]
fn injection_comment_in_string_value() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Eq,
        Value::String("admin'--".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("admin''--"),
        "Comment injection not escaped: {}",
        sql
    );
}

#[test]
fn injection_block_comment_in_string_value() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Eq,
        Value::String("admin'/**/OR 1=1".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("admin''/**/OR 1=1"),
        "Block comment injection: {}",
        sql
    );
}

#[test]
fn injection_union_select_in_string_value() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Eq,
        Value::String("' UNION SELECT password FROM users --".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("'' UNION SELECT"),
        "Union injection not escaped: {}",
        sql
    );
}

#[test]
fn injection_stacked_query_in_identifier() {
    let cmd = select_from("users; DROP TABLE orders");
    let sql = cmd.to_sql();
    assert!(
        sql.contains("\"users; DROP TABLE orders\""),
        "Semicolon in table not quoted: {}",
        sql
    );
}

#[test]
fn injection_double_quote_in_identifier() {
    let cmd = select_from("users\"inject");
    let sql = cmd.to_sql();
    assert!(
        sql.contains("\"users\"\"inject\""),
        "Double quote in identifier not escaped: {}",
        sql
    );
}

#[test]
fn injection_backslash_in_string_value() {
    let cmd = select_where(
        "users",
        "path",
        Operator::Eq,
        Value::String("C:\\Users\\admin".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("C:\\Users\\admin"),
        "Backslash modified: {}",
        sql
    );
}

#[test]
fn injection_newline_in_string_value() {
    let cmd = select_where(
        "users",
        "bio",
        Operator::Eq,
        Value::String("line1\nline2".into()),
    );
    let sql = cmd.to_sql();
    assert!(sql.contains("line1\nline2"), "Newline modified: {}", sql);
}

#[test]
fn injection_crlf_injection() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Eq,
        Value::String("alice\r\n' OR 1=1 --".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("'' OR 1=1"),
        "CRLF injection not escaped: {}",
        sql
    );
}

#[test]
fn injection_empty_string_value() {
    let cmd = select_where("users", "name", Operator::Eq, Value::String("".into()));
    let sql = cmd.to_sql();
    assert!(
        sql.contains("''"),
        "Empty string not properly represented: {}",
        sql
    );
}

#[test]
fn injection_string_of_only_quotes() {
    let cmd = select_where("users", "name", Operator::Eq, Value::String("'''".into()));
    let sql = cmd.to_sql();
    assert!(
        sql.contains("''''''"),
        "Quote-only string not escaped: {}",
        sql
    );
}

#[test]
fn injection_dollar_quoting_attempt() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Eq,
        Value::String("$$; DROP TABLE users; $$".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("'$$; DROP TABLE users; $$'"),
        "Dollar quoting not contained: {}",
        sql
    );
}

#[test]
fn injection_repeated_escaping_idempotent() {
    let cmd = select_where("users", "name", Operator::Eq, Value::String("test".into()));
    let sql = cmd.to_sql();
    assert!(sql.contains("'test'"), "Simple string broken: {}", sql);
    assert!(!sql.contains("''test''"), "Over-escaped: {}", sql);
}

// ============================================================================
// 2. LIKE / PATTERN INJECTION
// ============================================================================

#[test]
fn like_wildcard_percent_in_value() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Fuzzy,
        Value::String("100% complete".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("ILIKE") || sql.contains("LIKE"),
        "Must use LIKE: {}",
        sql
    );
    assert!(sql.contains("100% complete"), "Value present: {}", sql);
}

#[test]
fn like_underscore_in_value() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Fuzzy,
        Value::String("user_name".into()),
    );
    let sql = cmd.to_sql();
    assert!(sql.contains("user_name"), "Underscore preserved: {}", sql);
}

#[test]
fn like_quote_injection_in_fuzzy() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Fuzzy,
        Value::String("admin' OR '1'='1".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("admin'' OR ''1''=''1"),
        "ILIKE quote injection not escaped: {}",
        sql
    );
}

#[test]
fn like_backslash_escape_attempt() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Fuzzy,
        Value::String("test\\' OR 1=1 --".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("test\\'' OR 1=1 --"),
        "Backslash escape attack: {}",
        sql
    );
}

#[test]
fn like_very_long_pattern() {
    let long_pattern = "a".repeat(10_000);
    let cmd = select_where(
        "users",
        "name",
        Operator::Fuzzy,
        Value::String(long_pattern),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.len() > 10_000,
        "Long pattern truncated: len={}",
        sql.len()
    );
}

// ============================================================================
// 3. ENCODING ATTACKS
// ============================================================================

#[test]
fn encoding_null_byte_in_identifier_stripped() {
    let cmd = select_from("users\0injected");
    let sql = cmd.to_sql();
    assert!(
        !sql.as_bytes().contains(&0u8),
        "Null bytes must be stripped"
    );
    assert!(
        sql.contains("usersinjected"),
        "Name after stripping: {}",
        sql
    );
}

#[test]
fn encoding_null_byte_in_column_name() {
    let cmd = Qail {
        action: Action::Get,
        table: "users".to_string(),
        columns: vec![Expr::Named("id\0; DROP TABLE x".to_string())],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        !sql.as_bytes().contains(&0u8),
        "Null bytes stripped from columns"
    );
}

#[test]
fn encoding_utf8_bom_in_identifier() {
    let cmd = select_from("\u{FEFF}users");
    let sql = cmd.to_sql();
    assert!(
        sql.contains("users"),
        "Table name contains 'users': {}",
        sql
    );
}

#[test]
fn encoding_rtl_override_in_identifier() {
    let cmd = select_from("users\u{202E}admin");
    let sql = cmd.to_sql();
    assert!(
        sql.contains("\""),
        "RTL override must trigger quoting: {}",
        sql
    );
}

#[test]
fn encoding_zero_width_space_in_identifier() {
    let cmd = select_from("users\u{200B}");
    let sql = cmd.to_sql();
    assert!(
        sql.contains("\""),
        "Zero-width space must trigger quoting: {}",
        sql
    );
}

#[test]
fn encoding_homoglyph_in_identifier() {
    let cmd = select_from("us\u{0435}rs"); // Cyrillic 'е'
    let sql = cmd.to_sql();
    assert!(sql.contains("us\u{0435}rs"), "Homoglyph preserved: {}", sql);
}

#[test]
fn encoding_null_byte_in_string_value() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Eq,
        Value::String("hello\0world".into()),
    );
    let sql = cmd.to_sql();
    assert!(sql.contains("hello"), "Value contains hello: {}", sql);
}

#[test]
fn encoding_mixed_unicode_scripts() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Eq,
        Value::String("Hello 世界 🌍 مرحبا".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("Hello 世界 🌍 مرحبا"),
        "Unicode preserved: {}",
        sql
    );
}

// ============================================================================
// 4. VALUE TYPE CONFUSION
// ============================================================================

#[test]
fn value_float_nan() {
    let cmd = select_where("stats", "val", Operator::Eq, Value::Float(f64::NAN));
    let sql = cmd.to_sql();
    assert!(sql.contains("NaN"), "NaN representable: {}", sql);
}

#[test]
fn value_float_infinity() {
    let cmd = select_where("stats", "val", Operator::Eq, Value::Float(f64::INFINITY));
    let sql = cmd.to_sql();
    assert!(sql.contains("inf"), "Infinity representable: {}", sql);
}

#[test]
fn value_float_neg_infinity() {
    let cmd = select_where(
        "stats",
        "val",
        Operator::Eq,
        Value::Float(f64::NEG_INFINITY),
    );
    let sql = cmd.to_sql();
    assert!(sql.contains("-inf"), "Negative infinity: {}", sql);
}

#[test]
fn value_float_negative_zero() {
    let cmd = select_where("stats", "val", Operator::Eq, Value::Float(-0.0));
    let sql = cmd.to_sql();
    assert!(sql.contains("0"), "Negative zero: {}", sql);
}

#[test]
fn value_int_max() {
    let cmd = select_where("users", "id", Operator::Eq, Value::Int(i64::MAX));
    let sql = cmd.to_sql();
    assert!(sql.contains("9223372036854775807"), "i64::MAX: {}", sql);
}

#[test]
fn value_int_min() {
    let cmd = select_where("users", "id", Operator::Eq, Value::Int(i64::MIN));
    let sql = cmd.to_sql();
    assert!(sql.contains("-9223372036854775808"), "i64::MIN: {}", sql);
}

#[test]
fn value_empty_string() {
    let cmd = select_where("users", "name", Operator::Eq, Value::String("".into()));
    let sql = cmd.to_sql();
    assert!(sql.contains("''"), "Empty string: {}", sql);
}

#[test]
fn value_whitespace_only_string() {
    let cmd = select_where("users", "name", Operator::Eq, Value::String("   ".into()));
    let sql = cmd.to_sql();
    assert!(sql.contains("'   '"), "Whitespace preserved: {}", sql);
}

// ============================================================================
// 5. OPERATOR ABUSE
// ============================================================================

#[test]
fn operator_between_with_injection() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        cages: vec![filter_cage(vec![Condition {
            left: Expr::Named("price".to_string()),
            op: Operator::Between,
            value: Value::Array(vec![
                Value::String("0'; DROP TABLE orders; --".into()),
                Value::String("100".into()),
            ]),
            is_array_unnest: false,
        }])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("BETWEEN"), "Must contain BETWEEN: {}", sql);
    assert!(
        sql.contains("0''; DROP TABLE orders; --"),
        "BETWEEN bound injection not escaped: {}",
        sql
    );
}

#[test]
fn operator_exists_with_subquery() {
    let subquery = Box::new(Qail {
        action: Action::Get,
        table: "orders".to_string(),
        columns: vec![Expr::Literal(Value::Int(1))],
        cages: vec![filter_cage(vec![cond(
            "status",
            Operator::Eq,
            Value::String("active".into()),
        )])],
        ..Default::default()
    });
    let cmd = Qail {
        action: Action::Get,
        table: "users".to_string(),
        cages: vec![filter_cage(vec![Condition {
            left: Expr::Named("id".to_string()),
            op: Operator::Exists,
            value: Value::Subquery(subquery),
            is_array_unnest: false,
        }])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("EXISTS"), "Must contain EXISTS: {}", sql);
    assert!(sql.contains("SELECT"), "Subquery must expand: {}", sql);
}

#[test]
fn operator_in_with_injection_values() {
    let cmd = Qail {
        action: Action::Get,
        table: "users".to_string(),
        cages: vec![filter_cage(vec![Condition {
            left: Expr::Named("role".to_string()),
            op: Operator::In,
            value: Value::Array(vec![
                Value::String("admin".into()),
                Value::String("user'; DROP TABLE users; --".into()),
            ]),
            is_array_unnest: false,
        }])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        sql.contains("user''; DROP TABLE users; --"),
        "IN value injection not escaped: {}",
        sql
    );
}

#[test]
fn operator_is_null_no_value_leak() {
    let cmd = select_where("users", "deleted_at", Operator::IsNull, Value::Null);
    let sql = cmd.to_sql();
    assert!(sql.contains("IS NULL"), "Must contain IS NULL: {}", sql);
    assert!(
        !sql.contains("IS NULL NULL"),
        "Value leaked into IS NULL: {}",
        sql
    );
}

#[test]
fn operator_not_in_with_empty_array() {
    let cmd = Qail {
        action: Action::Get,
        table: "users".to_string(),
        cages: vec![filter_cage(vec![Condition {
            left: Expr::Named("role".to_string()),
            op: Operator::NotIn,
            value: Value::Array(vec![]),
            is_array_unnest: false,
        }])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("SELECT"), "Must produce SQL: {}", sql);
}

#[test]
fn raw_sql_escape_hatch_documented() {
    let cmd = Qail {
        action: Action::Get,
        table: "users".to_string(),
        cages: vec![filter_cage(vec![Condition {
            left: Expr::Named("{age > 18 AND verified = true}".to_string()),
            op: Operator::Eq,
            value: Value::Null,
            is_array_unnest: false,
        }])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("age > 18"), "Raw SQL included: {}", sql);
}

// ============================================================================
// 6. CROSS-SITE VIA SQL
// ============================================================================

#[test]
fn xss_script_tag_in_value() {
    let cmd = select_where(
        "users",
        "name",
        Operator::Eq,
        Value::String("<script>alert('xss')</script>".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("alert(''xss'')"),
        "XSS quotes not escaped: {}",
        sql
    );
}

#[test]
fn xss_html_entities_in_identifier() {
    let cmd = select_from("users&amp;admin");
    let sql = cmd.to_sql();
    assert!(
        sql.contains("users&amp;admin") || sql.contains("\"users&amp;admin\""),
        "HTML entities preserved: {}",
        sql
    );
}

#[test]
fn xss_css_injection_in_value() {
    let cmd = select_where(
        "users",
        "style",
        Operator::Eq,
        Value::String("background:url(javascript:alert(1))".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("background:url(javascript:alert(1))"),
        "CSS injection stored: {}",
        sql
    );
}

// ============================================================================
// 7. COMBINED / MULTI-VECTOR ATTACKS
// ============================================================================

#[test]
fn combined_identifier_and_value_injection() {
    let cmd = select_where(
        "users\"; DROP TABLE x; --",
        "name",
        Operator::Eq,
        Value::String("'; DELETE FROM users; --".into()),
    );
    let sql = cmd.to_sql();
    assert!(
        sql.contains("\"users\"\"; DROP TABLE x; --\""),
        "Table injection not escaped: {}",
        sql
    );
    assert!(
        sql.contains("''; DELETE FROM users; --"),
        "Value injection not escaped: {}",
        sql
    );
}

#[test]
fn combined_join_injection() {
    let cmd = Qail {
        action: Action::Get,
        table: "orders".to_string(),
        joins: vec![Join {
            table: "users; DROP TABLE orders".to_string(),
            kind: JoinKind::Inner,
            on: Some(vec![cond(
                "orders.user_id",
                Operator::Eq,
                Value::Column("users; DROP TABLE orders.id".to_string()),
            )]),
            on_true: false,
        }],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(
        sql.contains("\"users; DROP TABLE orders\""),
        "JOIN table injection not quoted: {}",
        sql
    );
}

#[test]
fn combined_update_payload_injection() {
    let cmd = Qail {
        action: Action::Set,
        table: "users".to_string(),
        cages: vec![
            payload_cage(vec![cond(
                "name",
                Operator::Eq,
                Value::String("admin'; UPDATE users SET role='superadmin' WHERE '1'='1".into()),
            )]),
            filter_cage(vec![cond("id", Operator::Eq, Value::Int(1))]),
        ],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.starts_with("UPDATE"), "Must be UPDATE: {}", sql);
    assert!(
        sql.contains("admin''; UPDATE users SET role=''superadmin'' WHERE ''1''=''1"),
        "UPDATE payload injection not escaped: {}",
        sql
    );
}

#[test]
fn combined_delete_where_injection() {
    let cmd = Qail {
        action: Action::Del,
        table: "orders".to_string(),
        cages: vec![filter_cage(vec![cond(
            "id",
            Operator::Eq,
            Value::String("1' OR '1'='1".into()),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.starts_with("DELETE"), "Must be DELETE: {}", sql);
    assert!(
        sql.contains("1'' OR ''1''=''1"),
        "DELETE WHERE injection not escaped: {}",
        sql
    );
}

#[test]
fn combined_insert_values_injection() {
    let cmd = Qail {
        action: Action::Add,
        table: "users".to_string(),
        cages: vec![payload_cage(vec![cond(
            "name",
            Operator::Eq,
            Value::String("alice'), ('evil_user".into()),
        )])],
        ..Default::default()
    };
    let sql = cmd.to_sql();
    assert!(sql.contains("INSERT"), "Must be INSERT: {}", sql);
    assert!(
        sql.contains("alice''), (''evil_user"),
        "INSERT values injection not escaped: {}",
        sql
    );
}
