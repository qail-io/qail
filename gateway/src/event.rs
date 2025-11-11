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

fn default_retry_count() -> u32 { 3 }
fn default_true() -> bool { true }

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
    pub trigger: String,
    pub table: String,
    pub operation: String,
    pub webhook_url: String,
    pub status: DeliveryStatus,
    pub attempts: u32,
    pub response_status: Option<u16>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub enum DeliveryStatus {
    Success,
    Failed,
    Retrying,
}

/// The event trigger engine — holds all registered triggers
/// and provides the `fire` method for mutation handlers.
#[derive(Debug, Default)]
pub struct EventTriggerEngine {
    triggers: Vec<EventTrigger>,
    /// Shared HTTP client (reuses connections)
    client: Option<Arc<reqwest::Client>>,
}

impl EventTriggerEngine {
    pub fn new() -> Self {
        Self {
            triggers: Vec::new(),
            client: Some(Arc::new(
                reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .build()
                    .unwrap_or_default(),
            )),
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

            // Fire-and-forget async task
            tokio::spawn(async move {
                deliver_webhook(client, &url, &headers, &payload, retry_count, &trigger_name)
                    .await;
            });
        }
    }
}

/// Deliver webhook with exponential backoff retry
async fn deliver_webhook(
    client: Arc<reqwest::Client>,
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

        let mut req = client.post(url)
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
/// - Non-HTTP(S) schemes (e.g., `file://`)
/// - Localhost and loopback addresses
/// - Private network ranges (RFC 1918 / link-local)
fn validate_webhook_url(url: &str) -> Result<(), String> {
    let parsed = url::Url::parse(url)
        .map_err(|e| format!("Invalid URL: {}", e))?;

    // Only allow http and https schemes
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => return Err(format!("Disallowed scheme: {}", scheme)),
    }

    let host = parsed.host_str()
        .ok_or_else(|| "No host in URL".to_string())?;

    // Reject localhost
    if host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]" {
        return Err("Loopback address rejected".to_string());
    }

    // Reject private and link-local IPs
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        let is_private = match ip {
            std::net::IpAddr::V4(v4) => {
                v4.is_loopback()
                    || v4.is_private()
                    || v4.is_link_local()
                    || v4.octets()[0] == 169 && v4.octets()[1] == 254  // link-local
                    || v4.octets()[0] == 0  // current network
            }
            std::net::IpAddr::V6(v6) => {
                v6.is_loopback()
                // Note: is_unique_local() and is_unicast_link_local() are nightly-only;
                // check fd00::/8 and fe80::/10 manually.
                || (v6.segments()[0] & 0xfe00) == 0xfc00  // unique local
                || (v6.segments()[0] & 0xffc0) == 0xfe80  // link-local
            }
        };
        if is_private {
            return Err(format!("Private/link-local IP rejected: {}", ip));
        }
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
            operations: vec![OperationType::Create, OperationType::Update, OperationType::Delete],
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
    // SECURITY: SSRF protection (E4)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_ssrf_allows_public_url() {
        assert!(super::validate_webhook_url("https://api.example.com/hook").is_ok());
        assert!(super::validate_webhook_url("http://webhook.example.com/trigger").is_ok());
    }

    #[test]
    fn test_ssrf_rejects_localhost() {
        assert!(super::validate_webhook_url("http://localhost/hook").is_err());
        assert!(super::validate_webhook_url("http://127.0.0.1/hook").is_err());
    }

    #[test]
    fn test_ssrf_rejects_private_ip() {
        assert!(super::validate_webhook_url("http://10.0.0.1/hook").is_err());
        assert!(super::validate_webhook_url("http://192.168.1.1/hook").is_err());
        assert!(super::validate_webhook_url("http://172.16.0.1/hook").is_err());
    }

    #[test]
    fn test_ssrf_rejects_file_scheme() {
        assert!(super::validate_webhook_url("file:///etc/passwd").is_err());
    }

    #[test]
    fn test_ssrf_rejects_link_local() {
        assert!(super::validate_webhook_url("http://169.254.1.1/hook").is_err());
    }
}
