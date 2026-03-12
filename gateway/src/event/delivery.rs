use reqwest::header::{HeaderName, HeaderValue};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::ssrf::{reject_private_ip, validate_webhook_url};
use super::{MAX_WEBHOOK_RETRIES, WebhookPayload, is_reserved_webhook_header, retry_delay};

/// Deliver webhook with exponential backoff retry.
pub(super) async fn deliver_webhook(
    mut client: Arc<reqwest::Client>,
    url: &str,
    headers: &HashMap<String, String>,
    payload: &WebhookPayload,
    max_retries: u32,
    trigger_name: &str,
) {
    let capped_retries = max_retries.min(MAX_WEBHOOK_RETRIES);
    if max_retries > capped_retries {
        tracing::warn!(
            trigger = %trigger_name,
            requested = max_retries,
            capped = capped_retries,
            "Webhook retry_count exceeds runtime cap; clamping"
        );
    }

    if let Err(reason) = validate_webhook_url(url) {
        tracing::error!(
            trigger = %trigger_name,
            url = %url,
            reason = %reason,
            "Webhook URL rejected (SSRF protection)"
        );
        return;
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
                            "Webhook SSRF-pinned client build failed — aborting delivery"
                        );
                        return;
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
                return;
            }
        }
    }

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
                    capped_retries + 1,
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Event trigger '{}' failed: {} — {} (attempt {}/{})",
                    trigger_name,
                    url,
                    e,
                    attempt + 1,
                    capped_retries + 1,
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
