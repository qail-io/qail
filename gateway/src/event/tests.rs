use super::*;
use std::sync::Arc;

fn base_trigger() -> EventTrigger {
    EventTrigger {
        name: "t".to_string(),
        table: "orders".to_string(),
        operations: vec![OperationType::Create],
        webhook_url: "https://example.com/hook".to_string(),
        headers: HashMap::new(),
        retry_count: 3,
        enabled: true,
    }
}

#[test]
fn test_trigger_matching() {
    let mut engine = EventTriggerEngine::default();
    engine.add_trigger(EventTrigger {
        name: "order_created".to_string(),
        table: "orders".to_string(),
        operations: vec![OperationType::Create],
        webhook_url: "https://example.com/hook".to_string(),
        headers: HashMap::new(),
        retry_count: 3,
        enabled: true,
    });
    engine.add_trigger(EventTrigger {
        name: "order_any".to_string(),
        table: "orders".to_string(),
        operations: vec![
            OperationType::Create,
            OperationType::Update,
            OperationType::Delete,
        ],
        webhook_url: "https://example.com/hook2".to_string(),
        headers: HashMap::new(),
        retry_count: 1,
        enabled: true,
    });
    engine.add_trigger(EventTrigger {
        name: "user_deleted".to_string(),
        table: "users".to_string(),
        operations: vec![OperationType::Delete],
        webhook_url: "https://example.com/hook3".to_string(),
        headers: HashMap::new(),
        retry_count: 2,
        enabled: false, // disabled
    });

    // INSERT on orders → matches order_created + order_any
    let matches = engine.triggers_for("orders", &OperationType::Create);
    assert_eq!(matches.len(), 2);

    // UPDATE on orders → matches order_any only
    let matches = engine.triggers_for("orders", &OperationType::Update);
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].name, "order_any");

    // DELETE on users → disabled trigger, no match
    let matches = engine.triggers_for("users", &OperationType::Delete);
    assert_eq!(matches.len(), 0);

    // INSERT on users → no trigger defined
    let matches = engine.triggers_for("users", &OperationType::Create);
    assert_eq!(matches.len(), 0);
}

#[test]
fn test_webhook_payload_serialization() {
    let payload = WebhookPayload {
        trigger: "order_created".to_string(),
        table: "orders".to_string(),
        operation: "INSERT".to_string(),
        data: WebhookData {
            new: Some(serde_json::json!({"id": 1, "total": 100})),
            old: None,
        },
        timestamp: "2026-01-01T00:00:00Z".to_string(),
    };

    let json = serde_json::to_string(&payload).unwrap();
    assert!(json.contains("order_created"));
    assert!(json.contains("INSERT"));
    assert!(!json.contains("\"old\"")); // skip_serializing_if = None
}

#[test]
fn normalize_trigger_clamps_retry_count() {
    let mut trigger = base_trigger();
    trigger.retry_count = MAX_WEBHOOK_RETRIES + 100;
    let normalized = normalize_trigger(trigger).expect("trigger should normalize");
    assert_eq!(normalized.retry_count, MAX_WEBHOOK_RETRIES);
}

#[test]
fn normalize_trigger_rejects_reserved_header() {
    let mut trigger = base_trigger();
    trigger
        .headers
        .insert("Content-Type".to_string(), "application/json".to_string());
    let err = normalize_trigger(trigger).expect_err("reserved header must be rejected");
    assert!(err.contains("reserved header"));
}

#[test]
fn normalize_trigger_rejects_invalid_header_name() {
    let mut trigger = base_trigger();
    trigger
        .headers
        .insert("bad header".to_string(), "x".to_string());
    let err = normalize_trigger(trigger).expect_err("invalid header name must be rejected");
    assert!(err.contains("invalid header name"));
}

#[test]
fn retry_delay_is_bounded() {
    assert_eq!(retry_delay(1), Duration::from_secs(1));
    assert_eq!(retry_delay(2), Duration::from_secs(2));
    assert_eq!(
        retry_delay(MAX_BACKOFF_EXPONENT + 20),
        Duration::from_secs(1u64 << MAX_BACKOFF_EXPONENT)
    );
}

#[test]
fn webhook_permit_gate_sheds_when_full() {
    let sem = Arc::new(tokio::sync::Semaphore::new(1));
    let held = try_acquire_webhook_permit(&sem, "t").expect("first permit should succeed");
    assert!(
        try_acquire_webhook_permit(&sem, "t").is_none(),
        "second permit should be rejected while full"
    );
    drop(held);
    assert!(
        try_acquire_webhook_permit(&sem, "t").is_some(),
        "permit should be available after release"
    );
}

// ══════════════════════════════════════════════════════════════════
// SECURITY: SSRF protection red-team tests (E4)
// ══════════════════════════════════════════════════════════════════

#[test]
fn ssrf_allows_public_https() {
    assert!(validate_webhook_url("https://api.example.com/hook").is_ok());
    assert!(validate_webhook_url("https://hooks.slack.com/services/T00/B00/xxx").is_ok());
}

#[test]
fn ssrf_allows_public_http() {
    assert!(validate_webhook_url("http://webhook.example.com/trigger").is_ok());
}

#[test]
fn ssrf_rejects_localhost_variations() {
    assert!(validate_webhook_url("http://localhost/hook").is_err());
    assert!(validate_webhook_url("http://localhost:8080/hook").is_err());
    assert!(validate_webhook_url("http://LOCALHOST/hook").is_err());
    assert!(validate_webhook_url("http://api.localhost/hook").is_err());
    assert!(validate_webhook_url("http://printer.local/hook").is_err());
    assert!(validate_webhook_url("http://router.localdomain/hook").is_err());
    assert!(validate_webhook_url("http://internal.home.arpa/hook").is_err());
    assert!(validate_webhook_url("http://127.0.0.1/hook").is_err());
    assert!(validate_webhook_url("http://127.0.0.1:3000/hook").is_err());
    assert!(validate_webhook_url("http://[::1]/hook").is_err());
}

#[test]
fn ssrf_rejects_private_rfc1918() {
    // 10.0.0.0/8
    assert!(validate_webhook_url("http://10.0.0.1/hook").is_err());
    assert!(validate_webhook_url("http://10.255.255.255/hook").is_err());
    // 172.16.0.0/12
    assert!(validate_webhook_url("http://172.16.0.1/hook").is_err());
    assert!(validate_webhook_url("http://172.31.255.255/hook").is_err());
    // 192.168.0.0/16
    assert!(validate_webhook_url("http://192.168.1.1/hook").is_err());
    assert!(validate_webhook_url("http://192.168.0.1:9090/hook").is_err());
}

#[test]
fn ssrf_rejects_link_local() {
    assert!(validate_webhook_url("http://169.254.1.1/hook").is_err());
    assert!(validate_webhook_url("http://169.254.169.254/hook").is_err());
}

#[test]
fn ssrf_rejects_cloud_metadata() {
    // AWS/GCP/Azure metadata endpoint
    assert!(validate_webhook_url("http://169.254.169.254/latest/meta-data/").is_err());
    assert!(validate_webhook_url("http://169.254.169.254/computeMetadata/v1/").is_err());
}

#[test]
fn ssrf_rejects_zero_address() {
    assert!(validate_webhook_url("http://0.0.0.0/hook").is_err());
    assert!(validate_webhook_url("http://0.0.0.0:8080/hook").is_err());
}

#[test]
fn ssrf_rejects_file_scheme() {
    assert!(validate_webhook_url("file:///etc/passwd").is_err());
}

#[test]
fn ssrf_rejects_gopher_scheme() {
    assert!(validate_webhook_url("gopher://evil.com").is_err());
}

#[test]
fn ssrf_rejects_ftp_scheme() {
    assert!(validate_webhook_url("ftp://evil.com/file").is_err());
}

#[test]
fn ssrf_rejects_url_credentials() {
    // user:pass@host can be used to smuggle requests
    assert!(validate_webhook_url("http://admin:password@internal.example.com/hook").is_err());
    assert!(validate_webhook_url("https://user@10.0.0.1/hook").is_err());
}

#[test]
fn ssrf_rejects_ipv6_mapped_ipv4_loopback() {
    // ::ffff:127.0.0.1 — IPv6-mapped IPv4 bypass attempt
    assert!(validate_webhook_url("http://[::ffff:127.0.0.1]/hook").is_err());
}

#[test]
fn ssrf_rejects_ipv6_mapped_ipv4_private() {
    assert!(validate_webhook_url("http://[::ffff:10.0.0.1]/hook").is_err());
    assert!(validate_webhook_url("http://[::ffff:192.168.1.1]/hook").is_err());
}

#[test]
fn ssrf_rejects_ipv6_unique_local() {
    // fd00::/8 — IPv6 private range
    assert!(validate_webhook_url("http://[fd00::1]/hook").is_err());
    assert!(validate_webhook_url("http://[fc00::1]/hook").is_err());
}

#[test]
fn ssrf_rejects_ipv6_link_local() {
    assert!(validate_webhook_url("http://[fe80::1]/hook").is_err());
}

#[test]
fn ssrf_rejects_metadata_hostname() {
    // GCP metadata hostname
    assert!(validate_webhook_url("http://metadata.google.internal/computeMetadata/v1/").is_err());
    // Generic metadata keyword
    assert!(validate_webhook_url("http://instance-metadata.local/hook").is_err());
}

#[test]
fn ssrf_rejects_internal_hostname() {
    assert!(validate_webhook_url("http://api.internal/hook").is_err());
    assert!(validate_webhook_url("http://service.internal:8080/hook").is_err());
}

#[test]
fn ssrf_rejects_current_network() {
    // 0.x.x.x — current network
    assert!(validate_webhook_url("http://0.1.2.3/hook").is_err());
}

#[test]
fn ssrf_allows_public_ip() {
    assert!(validate_webhook_url("http://8.8.8.8/hook").is_ok());
    assert!(validate_webhook_url("https://1.1.1.1/hook").is_ok());
}

#[test]
fn ssrf_rejects_empty_and_garbage() {
    assert!(validate_webhook_url("").is_err());
    assert!(validate_webhook_url("not-a-url").is_err());
    assert!(validate_webhook_url("://missing-scheme").is_err());
}
