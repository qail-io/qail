//! Event Trigger Engine
//!
//! Fires webhooks on REST mutations (INSERT, UPDATE, DELETE).
//! Each trigger is a `{table, operation, webhook_url}` rule.
//! Webhooks are fired asynchronously (`tokio::spawn`) with retry logic.

use reqwest::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

use crate::policy::OperationType;

mod delivery;
mod engine;
mod ssrf;
#[cfg(test)]
mod tests;
mod types;

pub use engine::EventTriggerEngine;
#[cfg(test)]
use engine::try_acquire_webhook_permit;
use ssrf::validate_webhook_url;
pub use types::{DeliveryLog, DeliveryStatus, WebhookData, WebhookPayload};

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

/// Safety caps for webhook delivery/config.
const MAX_WEBHOOK_RETRIES: u32 = 10;
const MAX_BACKOFF_EXPONENT: u32 = 10; // 2^10 = 1024s max delay step
const MAX_WEBHOOK_HEADERS: usize = 32;
const MAX_WEBHOOK_HEADER_NAME_LEN: usize = 128;
const MAX_WEBHOOK_HEADER_VALUE_LEN: usize = 4096;

/// Headers controlled by the gateway transport and must not be user-overridden.
const RESERVED_WEBHOOK_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "expect",
    "upgrade",
    "te",
    "trailer",
    "proxy-authorization",
    "proxy-authenticate",
    "content-type",
    "x-qail-trigger",
];

fn is_reserved_webhook_header(name: &str) -> bool {
    RESERVED_WEBHOOK_HEADERS.contains(&name)
}

fn retry_delay(attempt: u32) -> Duration {
    let exp = attempt.saturating_sub(1).min(MAX_BACKOFF_EXPONENT);
    Duration::from_secs(1u64 << exp)
}

fn normalize_trigger(mut trigger: EventTrigger) -> Result<EventTrigger, String> {
    if trigger.name.trim().is_empty() {
        return Err("Trigger name must not be empty".to_string());
    }
    if trigger.table.trim().is_empty() {
        return Err(format!(
            "Trigger '{}' has empty table name",
            trigger.name.trim()
        ));
    }
    if trigger.operations.is_empty() {
        return Err(format!(
            "Trigger '{}' must specify at least one operation",
            trigger.name.trim()
        ));
    }

    validate_webhook_url(&trigger.webhook_url).map_err(|e| {
        format!(
            "Trigger '{}' invalid webhook_url: {}",
            trigger.name.trim(),
            e
        )
    })?;

    if trigger.retry_count > MAX_WEBHOOK_RETRIES {
        tracing::warn!(
            trigger = %trigger.name,
            requested = trigger.retry_count,
            capped = MAX_WEBHOOK_RETRIES,
            "Webhook retry_count exceeds cap; clamping"
        );
        trigger.retry_count = MAX_WEBHOOK_RETRIES;
    }

    if trigger.headers.len() > MAX_WEBHOOK_HEADERS {
        return Err(format!(
            "Trigger '{}' has too many headers ({} > {})",
            trigger.name.trim(),
            trigger.headers.len(),
            MAX_WEBHOOK_HEADERS
        ));
    }

    for (key, value) in &trigger.headers {
        let key_trimmed = key.trim();
        if key_trimmed.is_empty() {
            return Err(format!(
                "Trigger '{}' has empty header name",
                trigger.name.trim()
            ));
        }
        if key_trimmed.len() > MAX_WEBHOOK_HEADER_NAME_LEN {
            return Err(format!(
                "Trigger '{}' header '{}' exceeds max name length {}",
                trigger.name.trim(),
                key_trimmed,
                MAX_WEBHOOK_HEADER_NAME_LEN
            ));
        }
        let lower = key_trimmed.to_ascii_lowercase();
        if is_reserved_webhook_header(&lower) {
            return Err(format!(
                "Trigger '{}' uses reserved header '{}'",
                trigger.name.trim(),
                key_trimmed
            ));
        }
        if value.len() > MAX_WEBHOOK_HEADER_VALUE_LEN {
            return Err(format!(
                "Trigger '{}' header '{}' exceeds max value length {}",
                trigger.name.trim(),
                key_trimmed,
                MAX_WEBHOOK_HEADER_VALUE_LEN
            ));
        }

        HeaderName::from_bytes(key_trimmed.as_bytes()).map_err(|e| {
            format!(
                "Trigger '{}' has invalid header name '{}': {}",
                trigger.name.trim(),
                key_trimmed,
                e
            )
        })?;
        HeaderValue::from_str(value).map_err(|e| {
            format!(
                "Trigger '{}' has invalid header value for '{}': {}",
                trigger.name.trim(),
                key_trimmed,
                e
            )
        })?;
    }

    Ok(trigger)
}
