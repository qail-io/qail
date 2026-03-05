use super::*;
use axum::http::StatusCode;
use axum::response::IntoResponse;

#[tokio::test]
async fn test_rate_limiter() {
    let limiter = RateLimiter::new(10.0, 5); // 10/s, burst 5

    // First 5 requests should pass (burst)
    for i in 0..5 {
        assert!(
            limiter.check("test").await.is_ok(),
            "Request {} should pass",
            i
        );
    }

    // 6th request should fail (bucket empty)
    assert!(
        limiter.check("test").await.is_err(),
        "Request 6 should fail"
    );

    // Different key should have its own bucket
    assert!(
        limiter.check("other").await.is_ok(),
        "Other key should pass"
    );
}

#[test]
fn test_allow_list() {
    let mut allow_list = QueryAllowList::new();
    allow_list.allow("SELECT users");
    assert!(allow_list.is_allowed("SELECT users"));
    assert!(!allow_list.is_allowed("DROP TABLE users"));
}

#[test]
fn test_complexity_guard() {
    let guard = QueryComplexityGuard::new(3, 10, 5);

    // Within limits
    assert!(guard.check(2, 5, 3).is_ok());

    // Exceeds depth
    assert!(guard.check(4, 5, 3).is_err());

    // Exceeds filters
    assert!(guard.check(1, 11, 3).is_err());

    // Exceeds joins
    assert!(guard.check(1, 5, 6).is_err());
}

#[test]
fn test_from_pg_driver_error_unique_violation_sqlstate() {
    let err = qail_pg::PgError::QueryServer(qail_pg::PgServerError {
        severity: "ERROR".to_string(),
        code: "23505".to_string(),
        message: "duplicate key value violates unique constraint \"users_email_key\"".to_string(),
        detail: None,
        hint: None,
    });

    let api = ApiError::from_pg_driver_error(&err, Some("users"));
    assert_eq!(api.code, "CONFLICT");
    assert_eq!(api.status_code(), StatusCode::CONFLICT);
    assert_eq!(api.table.as_deref(), Some("users"));
}

#[test]
fn test_from_pg_driver_error_query_canceled_sqlstate() {
    let err = qail_pg::PgError::QueryServer(qail_pg::PgServerError {
        severity: "ERROR".to_string(),
        code: "57014".to_string(),
        message: "canceling statement due to statement timeout".to_string(),
        detail: None,
        hint: None,
    });

    let api = ApiError::from_pg_driver_error(&err, Some("users"));
    assert_eq!(api.code, "TIMEOUT");
    assert_eq!(api.status_code(), StatusCode::GATEWAY_TIMEOUT);
}

// ══════════════════════════════════════════════════════════════════
// Phase 4: Error code → HTTP status contract (exhaustive)
// ══════════════════════════════════════════════════════════════════

#[test]
fn error_code_rate_limited_is_429() {
    assert_eq!(
        ApiError::rate_limited().status_code(),
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[test]
fn error_code_timeout_is_504() {
    assert_eq!(
        ApiError::timeout().status_code(),
        StatusCode::GATEWAY_TIMEOUT
    );
}

#[test]
fn error_code_parse_error_is_400() {
    assert_eq!(
        ApiError::parse_error("test").status_code(),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn error_code_validation_error_is_400() {
    assert_eq!(
        ApiError::validation_error("t", "c", "msg").status_code(),
        StatusCode::BAD_REQUEST
    );
}

#[test]
fn error_code_bad_request_codes_are_400() {
    for code in [
        "EMPTY_QUERY",
        "EMPTY_BATCH",
        "DECODE_ERROR",
        "UNSUPPORTED_ACTION",
        "MISSING_VECTOR",
    ] {
        let err = ApiError::bad_request(code, "test");
        assert_eq!(
            err.status_code(),
            StatusCode::BAD_REQUEST,
            "{} should be 400",
            code
        );
    }
}

#[test]
fn error_code_conflict_is_409() {
    let err = ApiError::with_code("CONFLICT", "dup");
    assert_eq!(err.status_code(), StatusCode::CONFLICT);
    let err = ApiError::with_code("TXN_SESSION_EXPIRED", "expired");
    assert_eq!(err.status_code(), StatusCode::CONFLICT);
    let err = ApiError::with_code("TXN_STATEMENT_LIMIT", "too many");
    assert_eq!(err.status_code(), StatusCode::CONFLICT);
}

#[test]
fn error_code_query_error_is_500() {
    let err = ApiError::with_code("QUERY_ERROR", "fail");
    assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn error_code_expensive_is_422() {
    let err = ApiError::with_code("QUERY_TOO_EXPENSIVE", "cost");
    assert_eq!(err.status_code(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[test]
fn error_code_complex_is_422() {
    let err = ApiError::with_code("QUERY_TOO_COMPLEX", "depth");
    assert_eq!(err.status_code(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[test]
fn error_code_unauthorized_is_401() {
    assert_eq!(
        ApiError::auth_error("test").status_code(),
        StatusCode::UNAUTHORIZED
    );
}

#[test]
fn error_code_forbidden_variants_are_403() {
    for code in ["FORBIDDEN", "QUERY_NOT_ALLOWED", "POLICY_DENIED"] {
        let err = ApiError::with_code(code, "test");
        assert_eq!(
            err.status_code(),
            StatusCode::FORBIDDEN,
            "{} should be 403",
            code
        );
    }
}

#[test]
fn error_code_not_found_is_404() {
    assert_eq!(
        ApiError::not_found("resource").status_code(),
        StatusCode::NOT_FOUND
    );
}

#[test]
fn error_code_connection_errors_are_503() {
    for code in [
        "CONNECTION_ERROR",
        "POOL_BACKPRESSURE",
        "QDRANT_NOT_CONFIGURED",
        "QDRANT_CONNECTION_ERROR",
    ] {
        let err = ApiError::with_code(code, "test");
        assert_eq!(
            err.status_code(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{} should be 503",
            code
        );
    }
}

#[test]
fn pool_backpressure_response_includes_retry_and_metadata_headers() {
    let err = ApiError::with_code(
        "POOL_BACKPRESSURE",
        crate::db_backpressure::POOL_BACKPRESSURE_MSG_TENANT,
    );
    let response = err.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("1")
    );
    assert_eq!(
        response
            .headers()
            .get("x-qail-backpressure-scope")
            .and_then(|v| v.to_str().ok()),
        Some("tenant")
    );
    assert_eq!(
        response
            .headers()
            .get("x-qail-backpressure-reason")
            .and_then(|v| v.to_str().ok()),
        Some("tenant_waiters_exceeded")
    );
}

#[test]
fn pool_backpressure_unknown_message_falls_back_to_unknown_metadata() {
    let err = ApiError::with_code("POOL_BACKPRESSURE", "Queue saturated");
    let response = err.into_response();
    assert_eq!(
        response
            .headers()
            .get("x-qail-backpressure-scope")
            .and_then(|v| v.to_str().ok()),
        Some("unknown")
    );
    assert_eq!(
        response
            .headers()
            .get("x-qail-backpressure-reason")
            .and_then(|v| v.to_str().ok()),
        Some("queue_saturated")
    );
}

#[test]
fn error_code_batch_too_large_is_413() {
    let err = ApiError::bad_request("BATCH_TOO_LARGE", "test");
    assert_eq!(err.status_code(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[test]
fn error_code_internal_errors_are_500() {
    for code in [
        "INTERNAL_ERROR",
        "TXN_ERROR",
        "QDRANT_ERROR",
        "TENANT_BOUNDARY_VIOLATION",
    ] {
        let err = ApiError::with_code(code, "test");
        assert_eq!(
            err.status_code(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "{} should be 500",
            code
        );
    }
}

#[test]
fn error_code_unknown_falls_back_to_500() {
    let err = ApiError::with_code("TOTALLY_UNKNOWN_CODE", "test");
    assert_eq!(
        err.status_code(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "unknown codes should fall back to 500"
    );
}

#[test]
fn error_serialization_includes_code_and_message() {
    let err = ApiError::rate_limited();
    let json = serde_json::to_string(&err).unwrap();
    assert!(json.contains("\"code\":\"RATE_LIMITED\""));
    assert!(json.contains("\"message\":"));
}

#[test]
fn error_serialization_omits_null_fields() {
    let err = ApiError::timeout();
    let json = serde_json::to_string(&err).unwrap();
    // skip_serializing_if fields should not appear when None
    assert!(!json.contains("\"details\""));
    assert!(!json.contains("\"request_id\""));
    assert!(!json.contains("\"hint\""));
    assert!(!json.contains("\"table\""));
    assert!(!json.contains("\"column\""));
}

#[test]
fn error_builder_chain_attaches_metadata() {
    let err = ApiError::not_found("users")
        .with_request_id("req-123")
        .with_hint("try a different ID")
        .with_table("users")
        .with_column("id");

    assert_eq!(err.request_id.as_deref(), Some("req-123"));
    assert_eq!(err.hint.as_deref(), Some("try a different ID"));
    assert_eq!(err.table.as_deref(), Some("users"));
    assert_eq!(err.column.as_deref(), Some("id"));
}

// ══════════════════════════════════════════════════════════════════
// Phase 5: W3C Trace Context parsing
// ══════════════════════════════════════════════════════════════════

#[test]
fn traceparent_valid_sampled() {
    let ctx = super::parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        .unwrap();
    assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(ctx.parent_id, "00f067aa0ba902b7");
    assert_eq!(ctx.flags, 1);
}

#[test]
fn traceparent_valid_not_sampled() {
    let ctx = super::parse_traceparent("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1-bbbbbbbbbbbbbb01-00")
        .unwrap();
    assert_eq!(ctx.flags, 0);
}

#[test]
fn traceparent_rejects_unsupported_version() {
    assert!(
        super::parse_traceparent("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .is_none()
    );
}

#[test]
fn traceparent_rejects_all_zero_trace_id() {
    assert!(
        super::parse_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
            .is_none()
    );
}

#[test]
fn traceparent_rejects_all_zero_parent_id() {
    assert!(
        super::parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01")
            .is_none()
    );
}

#[test]
fn traceparent_rejects_short_trace_id() {
    assert!(super::parse_traceparent("00-4bf92f3577b34da6-00f067aa0ba902b7-01").is_none());
}

#[test]
fn traceparent_rejects_wrong_part_count() {
    assert!(super::parse_traceparent("00-abc-01").is_none());
    assert!(super::parse_traceparent("").is_none());
}

#[test]
fn traceparent_rejects_non_hex() {
    assert!(
        super::parse_traceparent("00-ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ-00f067aa0ba902b7-01")
            .is_none()
    );
}
