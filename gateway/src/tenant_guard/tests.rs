use super::*;
use serde_json::json;

#[test]
fn clean_response_no_violations() {
    let rows = vec![
        json!({"id": 1, "operator_id": "op-123", "name": "Order A"}),
        json!({"id": 2, "operator_id": "op-123", "name": "Order B"}),
    ];
    assert!(verify_tenant_boundary(&rows, "op-123", "operator_id", "orders", "GET").is_ok());
}

#[test]
fn cross_tenant_violation_detected() {
    let rows = vec![
        json!({"id": 1, "operator_id": "op-123", "name": "Order A"}),
        json!({"id": 2, "operator_id": "op-EVIL", "name": "Leaked!"}),
        json!({"id": 3, "operator_id": "op-123", "name": "Order C"}),
    ];
    let err = verify_tenant_boundary(&rows, "op-123", "operator_id", "orders", "GET").unwrap_err();
    assert_eq!(err.violation_count, 1);
}

#[test]
fn all_rows_wrong_tenant() {
    let rows = vec![
        json!({"id": 1, "operator_id": "op-EVIL"}),
        json!({"id": 2, "operator_id": "op-EVIL"}),
    ];
    let err = verify_tenant_boundary(&rows, "op-123", "operator_id", "orders", "GET").unwrap_err();
    assert_eq!(err.violation_count, 2);
}

#[test]
fn rows_without_operator_id_ignored() {
    let rows = vec![
        json!({"id": 1, "name": "No operator_id here"}),
        json!({"id": 2, "count": 42}),
    ];
    assert!(verify_tenant_boundary(&rows, "op-123", "operator_id", "aggregate", "GET").is_ok());
}

#[test]
fn null_operator_id_ignored() {
    let rows = vec![json!({"id": 1, "operator_id": null, "name": "System row"})];
    assert!(verify_tenant_boundary(&rows, "op-123", "operator_id", "settings", "GET").is_ok());
}

#[test]
fn empty_expected_operator_id_skips_check() {
    let rows = vec![json!({"id": 1, "operator_id": "op-123"})];
    assert!(verify_tenant_boundary(&rows, "", "operator_id", "orders", "GET").is_ok());
}

#[test]
fn empty_rows_is_clean() {
    assert!(verify_tenant_boundary(&[], "op-123", "operator_id", "orders", "GET").is_ok());
}

#[test]
fn integer_operator_id_compared_as_string() {
    let rows = vec![json!({"id": 1, "operator_id": 123})];
    assert!(verify_tenant_boundary(&rows, "op-123", "operator_id", "orders", "GET").is_err());
    assert!(verify_tenant_boundary(&rows, "123", "operator_id", "orders", "GET").is_ok());
}

#[test]
fn custom_tenant_column() {
    let rows = vec![
        json!({"id": 1, "tenant_id": "t-abc", "name": "Order A"}),
        json!({"id": 2, "tenant_id": "t-abc", "name": "Order B"}),
    ];
    assert!(verify_tenant_boundary(&rows, "t-abc", "tenant_id", "orders", "GET").is_ok());
    assert!(verify_tenant_boundary(&rows, "t-xyz", "tenant_id", "orders", "GET").is_err());
}
