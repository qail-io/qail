use reqwest::header::{HeaderName, HeaderValue};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::ssrf::{reject_private_ip, validate_webhook_url};
use super::{MAX_WEBHOOK_RETRIES, WebhookPayload, is_reserved_webhook_header, retry_delay};

#[derive(Debug, Clone)]
pub(super) struct WebhookDeliveryResult {
    pub success: bool,
    pub attempts: u32,
    pub response_status: Option<u16>,
    pub error: Option<String>,
}

impl WebhookDeliveryResult {
    fn success(response_status: u16, attempts: u32) -> Self {
        Self {
            success: true,
            attempts,
            response_status: Some(response_status),
            error: None,
        }
    }

    fn failure(response_status: Option<u16>, error: String, attempts: u32) -> Self {
        Self {
            success: false,
            attempts,
            response_status,
            error: Some(error),
        }
    }
}

/// Deliver webhook with exponential backoff retry.
pub(super) async fn deliver_webhook(
    client: Arc<reqwest::Client>,
    url: &str,
    headers: &HashMap<String, String>,
    payload: &WebhookPayload,
    max_retries: u32,
    trigger_name: &str,
) -> WebhookDeliveryResult {
    let capped_retries = max_retries.min(MAX_WEBHOOK_RETRIES);
    if max_retries > capped_retries {
        tracing::warn!(
            trigger = %trigger_name,
            requested = max_retries,
            capped = capped_retries,
            "Webhook retry_count exceeds runtime cap; clamping"
        );
    }

    let mut last = WebhookDeliveryResult::failure(None, "delivery not attempted".to_string(), 0);
    for attempt in 0..=capped_retries {
        if attempt > 0 {
            let delay = retry_delay(attempt);
            tracing::debug!(
                "Event trigger '{}': retry {} after {:?}",
                trigger_name,
                attempt,
                delay
            );
            tokio::time::sleep(delay).await;
        }

        let result =
            deliver_webhook_once(Arc::clone(&client), url, headers, payload, trigger_name).await;
        last = WebhookDeliveryResult {
            attempts: attempt + 1,
            ..result
        };
        if last.success {
            tracing::info!(
                "Event trigger '{}' delivered: {} -> {} (attempt {})",
                trigger_name,
                payload.table,
                url,
                attempt + 1,
            );
            return last;
        }
        if let Some(status) = last.response_status {
            tracing::warn!(
                "Event trigger '{}' got HTTP {}: {} (attempt {}/{})",
                trigger_name,
                status,
                url,
                attempt + 1,
                capped_retries + 1,
            );
        } else if let Some(error) = &last.error {
            tracing::warn!(
                "Event trigger '{}' failed: {} - {} (attempt {}/{})",
                trigger_name,
                url,
                error,
                attempt + 1,
                capped_retries + 1,
            );
        }
    }

    tracing::error!(
        "Event trigger '{}' exhausted retries: {} -> {}",
        trigger_name,
        payload.table,
        url,
    );
    last
}

pub(super) async fn deliver_webhook_once(
    mut client: Arc<reqwest::Client>,
    url: &str,
    headers: &HashMap<String, String>,
    payload: &WebhookPayload,
    trigger_name: &str,
) -> WebhookDeliveryResult {
    if let Err(reason) = validate_webhook_url(url) {
        tracing::error!(
            trigger = %trigger_name,
            url = %url,
            reason = %reason,
            "Webhook URL rejected (SSRF protection)"
        );
        return WebhookDeliveryResult::failure(
            None,
            format!("Webhook URL rejected: {}", reason),
            1,
        );
    }

    if let Ok(parsed) = url::Url::parse(url)
        && let Some(host) = parsed.host_str()
        && host.parse::<std::net::IpAddr>().is_err()
    {
        let port = parsed
            .port()
            .unwrap_or(if parsed.scheme() == "https" { 443 } else { 80 });
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
                    return WebhookDeliveryResult::failure(
                        None,
                        "Webhook DNS returned no addresses".to_string(),
                        1,
                    );
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
                        return WebhookDeliveryResult::failure(
                            None,
                            format!("Webhook DNS resolves to private IP: {}", reason),
                            1,
                        );
                    }
                }
                let pinned_addr = addrs_vec[0];
                let pinned_client = match reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    .redirect(reqwest::redirect::Policy::none())
                    .resolve(host, pinned_addr)
                    .build()
                {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            url = %url,
                            "Webhook SSRF-pinned client build failed; aborting delivery"
                        );
                        return WebhookDeliveryResult::failure(
                            None,
                            format!("Webhook pinned client build failed: {}", e),
                            1,
                        );
                    }
                };
                client = Arc::new(pinned_client);
            }
            Err(e) => {
                tracing::error!(
                    trigger = %trigger_name,
                    url = %url,
                    error = %e,
                    "Webhook DNS resolution failed"
                );
                return WebhookDeliveryResult::failure(
                    None,
                    format!("Webhook DNS resolution failed: {}", e),
                    1,
                );
            }
        }
    }

    let mut req = client
        .post(url)
        .header("content-type", "application/json")
        .header("x-qail-trigger", trigger_name);

    for (key, value) in headers {
        let key_trimmed = key.trim();
        if key_trimmed.is_empty() {
            tracing::warn!(
                trigger = %trigger_name,
                "Webhook header skipped: empty name"
            );
            continue;
        }

        let lower = key_trimmed.to_ascii_lowercase();
        if is_reserved_webhook_header(&lower) {
            tracing::warn!(
                trigger = %trigger_name,
                header = %key_trimmed,
                "Webhook header skipped: reserved name"
            );
            continue;
        }

        let header_name = match HeaderName::from_bytes(key_trimmed.as_bytes()) {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(
                    trigger = %trigger_name,
                    header = %key_trimmed,
                    error = %e,
                    "Webhook header skipped: invalid header name"
                );
                continue;
            }
        };

        let header_value = match HeaderValue::from_str(value) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    trigger = %trigger_name,
                    header = %key_trimmed,
                    error = %e,
                    "Webhook header skipped: invalid header value"
                );
                continue;
            }
        };

        req = req.header(header_name, header_value);
    }

    match req.json(payload).send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            if (200..300).contains(&(status as usize)) {
                WebhookDeliveryResult::success(status, 1)
            } else {
                WebhookDeliveryResult::failure(
                    Some(status),
                    format!("Webhook HTTP status {}", status),
                    1,
                )
            }
        }
        Err(e) => WebhookDeliveryResult::failure(None, e.to_string(), 1),
    }
}
