//! Event Trigger Engine
//!
//! Fires webhooks on REST mutations (INSERT, UPDATE, DELETE).
//! Each trigger is a `{table, operation, webhook_url}` rule.
//! Webhooks are fired asynchronously (`tokio::spawn`) with retry logic.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::policy::OperationType;

/// An event trigger definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTrigger {
    /// Unique name for this trigger
    pub name: String,
    /// Table to watch
    pub table: String,
    /// Which operations fire this trigger
    pub operations: Vec<OperationType>,
    /// Webhook URL to POST to
    pub webhook_url: String,
    /// Optional extra headers for the webhook request
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Number of retry attempts (default: 3)
    #[serde(default = "default_retry_count")]
    pub retry_count: u32,
    /// Whether the trigger is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// Serde default for retry count (returns 3).
fn default_retry_count() -> u32 {
    3
}
/// Serde default that returns `true`.
fn default_true() -> bool {
    true
}

/// Payload sent to the webhook
#[derive(Debug, Serialize)]
pub struct WebhookPayload {
    /// Trigger name
    pub trigger: String,
    /// Table affected
    pub table: String,
    /// Operation: "INSERT", "UPDATE", "DELETE"
    pub operation: String,
    /// The row data (new for INSERT/UPDATE, old for DELETE)
    pub data: WebhookData,
    /// ISO 8601 timestamp
    pub timestamp: String,
}

/// Data included in webhook payload
#[derive(Debug, Serialize)]
pub struct WebhookData {
    /// New row data (for INSERT and UPDATE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new: Option<Value>,
    /// Old row data (for UPDATE and DELETE, if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old: Option<Value>,
}

/// Delivery attempt result for logging
#[derive(Debug, Serialize)]
pub struct DeliveryLog {
    /// Name of the trigger that fired.
    pub trigger: String,
    /// Table that was mutated.
    pub table: String,
    /// Operation type ("INSERT", "UPDATE", "DELETE").
    pub operation: String,
    /// Target webhook URL.
    pub webhook_url: String,
    /// Final delivery status.
    pub status: DeliveryStatus,
    /// Total delivery attempts made.
    pub attempts: u32,
    /// HTTP status code received (if any).
    pub response_status: Option<u16>,
    /// Error message (if delivery failed).
    pub error: Option<String>,
}

/// Outcome of a webhook delivery attempt.
#[derive(Debug, Serialize)]
pub enum DeliveryStatus {
    /// Webhook was delivered and acknowledged.
    Success,
    /// All retry attempts exhausted without success.
    Failed,
    /// Delivery is being retried.
    Retrying,
}

/// The event trigger engine — holds all registered triggers
/// and provides the `fire` method for mutation handlers.
#[derive(Debug)]
pub struct EventTriggerEngine {
    triggers: Vec<EventTrigger>,
    /// Shared HTTP client (reuses connections)
    client: Option<Arc<reqwest::Client>>,
    /// SECURITY: Bounded concurrency semaphore to prevent DoS from burst writes.
    /// Limits max in-flight webhook deliveries.
    webhook_semaphore: Arc<tokio::sync::Semaphore>,
}

impl Default for EventTriggerEngine {
    fn default() -> Self {
        Self {
            triggers: Vec::new(),
            client: None,
            webhook_semaphore: Arc::new(tokio::sync::Semaphore::new(64)),
        }
    }
}

impl EventTriggerEngine {
    /// Create a new event trigger engine with a shared HTTP client.
    pub fn new() -> Self {
        Self {
            triggers: Vec::new(),
            client: Some(Arc::new(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    // SECURITY: Disable redirects to prevent SSRF bypass via 301/302
                    // to private/internal targets after DNS validation.
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .expect("Failed to build webhook HTTP client — SSRF-hardened client is required"),
            )),
            webhook_semaphore: Arc::new(tokio::sync::Semaphore::new(64)),
        }
    }

    /// Register a trigger
    pub fn add_trigger(&mut self, trigger: EventTrigger) {
        tracing::info!(
            "Event trigger registered: {} on {}.{:?} → {}",
            trigger.name,
            trigger.table,
            trigger.operations,
            trigger.webhook_url
        );
        self.triggers.push(trigger);
    }

    /// Load triggers from a YAML file
    pub fn load_from_file(&mut self, path: &str) -> Result<(), String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read event triggers file: {}", e))?;

        let triggers: Vec<EventTrigger> = serde_yaml::from_str(&content)
            .map_err(|e| format!("Failed to parse event triggers: {}", e))?;

        for trigger in triggers {
            self.add_trigger(trigger);
        }
        Ok(())
    }

    /// Get triggers matching a specific table and operation
    pub fn triggers_for(&self, table: &str, op: &OperationType) -> Vec<&EventTrigger> {
        self.triggers
            .iter()
            .filter(|t| t.enabled && t.table == table && t.operations.contains(op))
            .collect()
    }

    /// Fire matching triggers for a mutation.
    ///
    /// This is **non-blocking** — each webhook call is spawned as a
    /// separate tokio task so the REST response is not delayed.
    pub fn fire(
        &self,
        table: &str,
        op: OperationType,
        new_data: Option<Value>,
        old_data: Option<Value>,
    ) {
        let matching = self.triggers_for(table, &op);
        if matching.is_empty() {
            return;
        }

        let client = match &self.client {
            Some(c) => Arc::clone(c),
            None => return,
        };

        let op_str = match op {
            OperationType::Read => return, // No events on reads
            OperationType::Create => "INSERT",
            OperationType::Update => "UPDATE",
            OperationType::Delete => "DELETE",
        };

        let timestamp = chrono::Utc::now().to_rfc3339();

        for trigger in matching {
            let payload = WebhookPayload {
                trigger: trigger.name.clone(),
                table: table.to_string(),
                operation: op_str.to_string(),
                data: WebhookData {
                    new: new_data.clone(),
                    old: old_data.clone(),
                },
                timestamp: timestamp.clone(),
            };

            let client = Arc::clone(&client);
            let url = trigger.webhook_url.clone();
            let headers = trigger.headers.clone();
            let retry_count = trigger.retry_count;
            let trigger_name = trigger.name.clone();
            let sem = Arc::clone(&self.webhook_semaphore);

            // SECURITY: Bounded concurrency — drop webhook if semaphore saturated.
            tokio::spawn(async move {
                let _permit = match sem.try_acquire() {
                    Ok(p) => p,
                    Err(_) => {
                        tracing::warn!(
                            trigger = %trigger_name,
                            "Webhook dropped: concurrency limit reached"
                        );
                        return;
                    }
                };
                deliver_webhook(client, &url, &headers, &payload, retry_count, &trigger_name).await;
            });
        }
    }
}

/// Deliver webhook with exponential backoff retry
async fn deliver_webhook(
    mut client: Arc<reqwest::Client>,
    url: &str,
    headers: &HashMap<String, String>,
    payload: &WebhookPayload,
    max_retries: u32,
    trigger_name: &str,
) {
    // SECURITY (E4): Validate webhook URL to prevent SSRF.
    if let Err(reason) = validate_webhook_url(url) {
        tracing::error!(
            trigger = %trigger_name,
            url = %url,
            reason = %reason,
            "Webhook URL rejected (SSRF protection)"
        );
        return;
    }

    // SECURITY: Resolve hostname and verify resolved IPs are not private.
    // Prevents DNS rebinding where a public hostname resolves to 127.0.0.1.
    if let Ok(parsed) = url::Url::parse(url) {
        if let Some(host) = parsed.host_str() {
            // Only resolve if host is not already a raw IP
            if host.parse::<std::net::IpAddr>().is_err() {
                let port = parsed.port().unwrap_or(if parsed.scheme() == "https" { 443 } else { 80 });
                let addr_str = format!("{}:{}", host, port);
                match tokio::net::lookup_host(&addr_str).await {
                    Ok(addrs) => {
                        let addrs_vec: Vec<std::net::SocketAddr> = addrs.collect();
                        if addrs_vec.is_empty() {
                            tracing::error!(
                                trigger = %trigger_name,
                                url = %url,
                                "Webhook DNS returned no addresses"
                            );
                            return;
                        }
                        for addr in &addrs_vec {
                            if let Err(reason) = reject_private_ip(addr.ip()) {
                                tracing::error!(
                                    trigger = %trigger_name,
                                    url = %url,
                                    resolved_ip = %addr.ip(),
                                    reason = %reason,
                                    "Webhook DNS resolves to private IP (SSRF protection)"
                                );
                                return;
                            }
                        }
                        // SECURITY: Pin outbound connection to verified IP to prevent TOCTOU
                        // DNS rebinding. reqwest .resolve() ensures the actual TCP connect
                        // goes to this address while keeping Host/SNI correct.
                        let pinned_addr = addrs_vec[0];
                        let pinned_client = match reqwest::Client::builder()
                            .timeout(Duration::from_secs(30))
                            .redirect(reqwest::redirect::Policy::none())
                            .resolve(host, pinned_addr)
                            .build()
                        {
                            Ok(c) => c,
                            Err(e) => {
                                // SECURITY: Fail-closed — do NOT fall back to unpinned client.
                                tracing::error!(
                                    error = %e,
                                    url = %url,
                                    "Webhook SSRF-pinned client build failed — aborting delivery"
                                );
                                return;
                            }
                        };
                        // Replace shared client with pinned client for this delivery
                        client = Arc::new(pinned_client);
                    }
                    Err(e) => {
                        tracing::error!(
                            trigger = %trigger_name,
                            url = %url,
                            error = %e,
                            "Webhook DNS resolution failed"
                        );
                        return;
                    }
                }
            }
        }
    }

    for attempt in 0..=max_retries {
        if attempt > 0 {
            // Exponential backoff: 1s, 2s, 4s, ...
            let delay = Duration::from_secs(2u64.pow(attempt - 1));
            tracing::debug!(
                "Event trigger '{}': retry {} after {:?}",
                trigger_name,
                attempt,
                delay
            );
            tokio::time::sleep(delay).await;
        }

        let mut req = client
            .post(url)
            .header("content-type", "application/json")
            .header("x-qail-trigger", trigger_name);

        for (key, value) in headers {
            req = req.header(key, value);
        }

        match req.json(payload).send().await {
            Ok(response) => {
                let status = response.status().as_u16();
                if (200..300).contains(&(status as usize)) {
                    tracing::info!(
                        "Event trigger '{}' delivered: {} → {} (attempt {})",
                        trigger_name,
                        payload.table,
                        url,
                        attempt + 1,
                    );
                    return;
                }
                tracing::warn!(
                    "Event trigger '{}' got HTTP {}: {} (attempt {}/{})",
                    trigger_name,
                    status,
                    url,
                    attempt + 1,
                    max_retries + 1,
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Event trigger '{}' failed: {} — {} (attempt {}/{})",
                    trigger_name,
                    url,
                    e,
                    attempt + 1,
                    max_retries + 1,
                );
            }
        }
    }

    tracing::error!(
        "Event trigger '{}' exhausted retries: {} → {}",
        trigger_name,
        payload.table,
        url,
    );
}

/// SECURITY (E4): Validate webhook URL to prevent SSRF attacks.
///
/// Rejects:
/// - Non-HTTP(S) schemes (e.g., `file://`, `gopher://`)
/// - Localhost and loopback addresses (127.x.x.x, ::1)
/// - Private network ranges (RFC 1918 / link-local)
/// - Cloud metadata endpoints (169.254.169.254)
/// - Zero/unspecified addresses (0.0.0.0, ::)
/// - URLs with embedded credentials (user:pass@host)
/// - Hostnames containing suspicious keywords
fn validate_webhook_url(url: &str) -> Result<(), String> {
    let parsed = url::Url::parse(url).map_err(|e| format!("Invalid URL: {}", e))?;

    // Only allow http and https schemes
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => return Err(format!("Disallowed scheme: {}", scheme)),
    }

    // Reject URLs with embedded credentials (user:pass@host SSRF vector)
    if parsed.username() != "" || parsed.password().is_some() {
        return Err("URL credentials not allowed".to_string());
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| "No host in URL".to_string())?;

    // Reject localhost (case-insensitive)
    let lower_host = host.to_ascii_lowercase();
    if lower_host == "localhost"
        || lower_host == "127.0.0.1"
        || lower_host == "::1"
        || lower_host == "[::1]"
        || lower_host == "0.0.0.0"
    {
        return Err("Loopback/unspecified address rejected".to_string());
    }

    // Reject hostnames that look like internal service discovery
    // (e.g., metadata.google.internal, instance-data.ec2.internal)
    for keyword in &["metadata", ".internal", "instance-data"] {
        if lower_host.contains(keyword) {
            return Err(format!(
                "Hostname contains suspicious keyword '{}': {}",
                keyword, host
            ));
        }
    }

    // Reject private and link-local IPs
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        reject_private_ip(ip)?;
    }

    // Also check when url::Url parsed it as a bracketed IPv6 (e.g., [::ffff:127.0.0.1])
    if let Some(url::Host::Ipv4(v4)) = parsed.host() {
        reject_private_ip(std::net::IpAddr::V4(v4))?;
    }
    if let Some(url::Host::Ipv6(v6)) = parsed.host() {
        reject_private_ip(std::net::IpAddr::V6(v6))?;

        // Check IPv6-mapped IPv4 (::ffff:127.0.0.1)
        if let Some(mapped_v4) = v6.to_ipv4_mapped() {
            reject_private_ip(std::net::IpAddr::V4(mapped_v4))?;
        }
    }

    Ok(())
}

/// Reject private, loopback, link-local, and cloud metadata IPs.
fn reject_private_ip(ip: std::net::IpAddr) -> Result<(), String> {
    let is_bad = match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback()                                  // 127.0.0.0/8
            || v4.is_private()                                // 10/8, 172.16/12, 192.168/16
            || v4.is_link_local()                             // 169.254.0.0/16
            || v4.is_unspecified()                            // 0.0.0.0
            || v4.octets()[0] == 169 && v4.octets()[1] == 254 // link-local (redundant but explicit)
            || v4.octets()[0] == 0                            // current network (0.x.x.x)
            || v4.is_broadcast() // 255.255.255.255
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback()                                  // ::1
            || v6.is_unspecified()                            // ::
            || (v6.segments()[0] & 0xfe00) == 0xfc00          // unique local (fc00::/7)
            || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local (fe80::/10)
        }
    };
    if is_bad {
        return Err(format!("Private/reserved IP rejected: {}", ip));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(
            validate_webhook_url("http://metadata.google.internal/computeMetadata/v1/").is_err()
        );
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
}
