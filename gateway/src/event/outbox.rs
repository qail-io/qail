use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use qail_pg::{PgError, PgPool, PgRow, PooledConnection};

use super::delivery::{WebhookDeliveryResult, deliver_webhook_once};
use super::ssrf::validate_webhook_url;
use super::{MAX_WEBHOOK_RETRIES, WebhookPayload, retry_delay};

const OUTBOX_TABLE: &str = "qail_webhook_outbox";
const OUTBOX_BATCH_SIZE: usize = 16;
const OUTBOX_LOCK_STALE_SECS: u64 = 300;
const OUTBOX_POLL_SECS: u64 = 2;

#[derive(Debug)]
struct OutboxItem {
    id: String,
    trigger_name: String,
    webhook_url: String,
    headers: HashMap<String, String>,
    payload: WebhookPayload,
    retry_count: u32,
    attempts: u32,
    locked_at: String,
}

#[derive(Debug)]
struct MalformedOutboxItem {
    id: String,
    reason: String,
    locked_at: String,
}

pub(super) struct OutboxEventInsert<'a> {
    pub(super) trigger_name: &'a str,
    pub(super) table: &'a str,
    pub(super) operation: &'a str,
    pub(super) webhook_url: &'a str,
    pub(super) headers: &'a HashMap<String, String>,
    pub(super) payload: &'a WebhookPayload,
    pub(super) retry_count: u32,
}

pub(super) async fn ensure_outbox_schema(pool: &PgPool) -> Result<(), PgError> {
    let mut conn = pool.acquire_system().await?;
    let result = async {
        let pg_conn = conn.get_mut()?;
        pg_conn
            .execute_simple(
                r#"
CREATE TABLE IF NOT EXISTS qail_webhook_outbox (
    id text PRIMARY KEY,
    trigger_name text NOT NULL,
    table_name text NOT NULL,
    operation text NOT NULL,
    webhook_url text NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload jsonb NOT NULL,
    retry_count integer NOT NULL DEFAULT 3,
    status text NOT NULL DEFAULT 'pending',
    attempts integer NOT NULL DEFAULT 0,
    response_status integer,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_at timestamptz,
    delivered_at timestamptz
);
CREATE INDEX IF NOT EXISTS qail_webhook_outbox_due_idx
    ON qail_webhook_outbox (status, next_attempt_at, created_at);
"#,
            )
            .await
    }
    .await;

    match result {
        Ok(()) => conn.release_checked().await,
        Err(e) => {
            let _ = conn.rollback_and_release().await;
            Err(e)
        }
    }
}

pub(super) async fn insert_outbox_event(
    conn: &mut PooledConnection,
    event: OutboxEventInsert<'_>,
) -> Result<(), PgError> {
    let id = uuid::Uuid::new_v4().to_string();
    let headers_json = serde_json::to_string(event.headers)
        .map_err(|e| PgError::Encode(format!("webhook headers encode failed: {}", e)))?;
    let payload_json = serde_json::to_string(event.payload)
        .map_err(|e| PgError::Encode(format!("webhook payload encode failed: {}", e)))?;
    let retry_count = event.retry_count.min(MAX_WEBHOOK_RETRIES).to_string();

    let params = vec![
        Some(id.into_bytes()),
        Some(event.trigger_name.as_bytes().to_vec()),
        Some(event.table.as_bytes().to_vec()),
        Some(event.operation.as_bytes().to_vec()),
        Some(event.webhook_url.as_bytes().to_vec()),
        Some(headers_json.into_bytes()),
        Some(payload_json.into_bytes()),
        Some(retry_count.into_bytes()),
    ];

    conn.query_rows_with_params(
        r#"
INSERT INTO qail_webhook_outbox
    (id, trigger_name, table_name, operation, webhook_url, headers, payload, retry_count)
VALUES
    ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::int4)
RETURNING id
"#,
        &params,
    )
    .await?;

    Ok(())
}

pub(super) fn spawn_outbox_worker(
    pool: PgPool,
    client: Arc<reqwest::Client>,
    sem: Arc<tokio::sync::Semaphore>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(OUTBOX_POLL_SECS));
        loop {
            interval.tick().await;
            let items = match claim_due_items(&pool).await {
                Ok(items) => items,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Webhook outbox claim failed"
                    );
                    continue;
                }
            };

            for item in items {
                let pool = pool.clone();
                let client = Arc::clone(&client);
                let sem = Arc::clone(&sem);
                tokio::spawn(async move {
                    deliver_claimed_item(pool, client, sem, item).await;
                });
            }
        }
    });
}

async fn claim_due_items(pool: &PgPool) -> Result<Vec<OutboxItem>, PgError> {
    let mut conn = pool.acquire_system().await?;
    let result = conn
        .query_rows_with_params(
            &format!(
                r#"
WITH claim AS (
    SELECT id
    FROM {OUTBOX_TABLE}
    WHERE status IN ('pending', 'retrying', 'delivering')
      AND next_attempt_at <= now()
      AND (locked_at IS NULL OR locked_at < now() - ($1::int4 * interval '1 second'))
    ORDER BY created_at
    LIMIT {OUTBOX_BATCH_SIZE}
    FOR UPDATE SKIP LOCKED
)
UPDATE {OUTBOX_TABLE} o
SET status = 'delivering',
    locked_at = now()
FROM claim
WHERE o.id = claim.id
RETURNING
    o.id,
    o.trigger_name,
    o.webhook_url,
    o.headers::text,
    o.payload::text,
    o.retry_count,
    o.attempts,
    o.locked_at::text
"#
            ),
            &[Some(OUTBOX_LOCK_STALE_SECS.to_string().into_bytes())],
        )
        .await;

    let rows = match result {
        Ok(rows) => {
            conn.release_checked().await?;
            rows
        }
        Err(e) => {
            let _ = conn.rollback_and_release().await;
            return Err(e);
        }
    };

    let mut items = Vec::with_capacity(rows.len());
    let mut malformed = Vec::new();
    for row in rows {
        match parse_claimed_outbox_row(&row) {
            Ok(item) => items.push(item),
            Err(item) => {
                tracing::warn!(
                    id = %item.id,
                    reason = %item.reason,
                    "Webhook outbox row is malformed"
                );
                malformed.push(item);
            }
        }
    }

    for item in malformed {
        if let Err(e) = mark_malformed_claimed_item(pool, &item).await {
            tracing::error!(
                id = %item.id,
                error = %e,
                "Webhook outbox malformed-row update failed"
            );
        }
    }

    Ok(items)
}

fn required_string(
    row: &PgRow,
    idx: usize,
    id: &str,
    field: &str,
    locked_at: &str,
) -> Result<String, MalformedOutboxItem> {
    row.get_string(idx)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| MalformedOutboxItem {
            id: id.to_string(),
            reason: format!("missing {}", field),
            locked_at: locked_at.to_string(),
        })
}

fn required_non_negative_u32(
    row: &PgRow,
    idx: usize,
    id: &str,
    field: &str,
    locked_at: &str,
) -> Result<u32, MalformedOutboxItem> {
    let value = row.get_i32(idx).ok_or_else(|| MalformedOutboxItem {
        id: id.to_string(),
        reason: format!("invalid {}", field),
        locked_at: locked_at.to_string(),
    })?;
    u32::try_from(value).map_err(|_| MalformedOutboxItem {
        id: id.to_string(),
        reason: format!("invalid negative {}", field),
        locked_at: locked_at.to_string(),
    })
}

fn parse_claimed_outbox_row(row: &PgRow) -> Result<OutboxItem, MalformedOutboxItem> {
    let missing_id_lock_token = row
        .get_string(7)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "<missing>".to_string());
    let id = required_string(row, 0, "<missing>", "id", &missing_id_lock_token)?;
    let locked_at = row
        .get_string(7)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| MalformedOutboxItem {
            id: id.clone(),
            reason: "missing locked_at".to_string(),
            locked_at: "<missing>".to_string(),
        })?;
    let trigger_name = required_string(row, 1, &id, "trigger_name", &locked_at)?;
    let webhook_url = required_string(row, 2, &id, "webhook_url", &locked_at)?;
    validate_webhook_url(&webhook_url).map_err(|e| MalformedOutboxItem {
        id: id.clone(),
        reason: format!("invalid webhook_url: {}", e),
        locked_at: locked_at.clone(),
    })?;
    let headers_raw = required_string(row, 3, &id, "headers", &locked_at)?;
    let headers: HashMap<String, String> =
        serde_json::from_str(&headers_raw).map_err(|e| MalformedOutboxItem {
            id: id.clone(),
            reason: format!("invalid headers JSON: {}", e),
            locked_at: locked_at.clone(),
        })?;
    let payload_raw = required_string(row, 4, &id, "payload", &locked_at)?;
    let payload =
        serde_json::from_str::<WebhookPayload>(&payload_raw).map_err(|e| MalformedOutboxItem {
            id: id.clone(),
            reason: format!("invalid payload JSON: {}", e),
            locked_at: locked_at.clone(),
        })?;
    let retry_count = required_non_negative_u32(row, 5, &id, "retry_count", &locked_at)?;
    let attempts = required_non_negative_u32(row, 6, &id, "attempts", &locked_at)?;

    Ok(OutboxItem {
        id,
        trigger_name,
        webhook_url,
        headers,
        payload,
        retry_count,
        attempts,
        locked_at,
    })
}

async fn deliver_claimed_item(
    pool: PgPool,
    client: Arc<reqwest::Client>,
    sem: Arc<tokio::sync::Semaphore>,
    item: OutboxItem,
) {
    let permit = match tokio::time::timeout(Duration::from_secs(10), sem.acquire_owned()).await {
        Ok(Ok(permit)) => permit,
        _ => {
            let result = WebhookDeliveryResult {
                success: false,
                attempts: 1,
                response_status: None,
                error: Some("webhook concurrency limit timeout".to_string()),
            };
            if let Err(e) = mark_failed_or_retry(&pool, &item, &result).await {
                tracing::error!(
                    id = %item.id,
                    error = %e,
                    "Webhook outbox retry update failed"
                );
            }
            return;
        }
    };

    let result = deliver_webhook_once(
        client,
        &item.webhook_url,
        &item.headers,
        &item.payload,
        &item.trigger_name,
    )
    .await;
    drop(permit);

    let update = if result.success {
        mark_delivered(&pool, &item, &result).await
    } else {
        mark_failed_or_retry(&pool, &item, &result).await
    };

    if let Err(e) = update {
        tracing::error!(
            id = %item.id,
            trigger = %item.trigger_name,
            error = %e,
            "Webhook outbox status update failed"
        );
    }
}

async fn mark_malformed_claimed_item(
    pool: &PgPool,
    item: &MalformedOutboxItem,
) -> Result<(), PgError> {
    execute_outbox_update(
        pool,
        r#"
UPDATE qail_webhook_outbox
SET status = 'failed',
    attempts = attempts + 1,
    last_error = $2,
    locked_at = NULL,
    next_attempt_at = now()
WHERE id = $1
  AND status = 'delivering'
  AND locked_at::text = $3
"#,
        &[
            Some(item.id.as_bytes().to_vec()),
            Some(item.reason.as_bytes().to_vec()),
            Some(item.locked_at.as_bytes().to_vec()),
        ],
    )
    .await
}

async fn mark_delivered(
    pool: &PgPool,
    item: &OutboxItem,
    result: &WebhookDeliveryResult,
) -> Result<(), PgError> {
    let attempts = result.attempts.max(1).to_string();
    let response_status = result.response_status.map(|s| s.to_string());
    execute_outbox_update(
        pool,
        r#"
UPDATE qail_webhook_outbox
SET status = 'delivered',
    attempts = attempts + $2::int4,
    response_status = $3::int4,
    last_error = NULL,
    locked_at = NULL,
    delivered_at = now()
WHERE id = $1
  AND status = 'delivering'
  AND locked_at::text = $4
"#,
        &[
            Some(item.id.as_bytes().to_vec()),
            Some(attempts.into_bytes()),
            response_status.map(String::into_bytes),
            Some(item.locked_at.as_bytes().to_vec()),
        ],
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(values: &[Option<&str>]) -> PgRow {
        PgRow {
            columns: values
                .iter()
                .map(|value| value.map(|value| value.as_bytes().to_vec()))
                .collect(),
            column_info: None,
        }
    }

    fn payload_json() -> String {
        serde_json::json!({
            "trigger": "order_created",
            "table": "orders",
            "operation": "INSERT",
            "data": {
                "new": {"id": 1}
            },
            "timestamp": "2026-01-01T00:00:00Z"
        })
        .to_string()
    }

    #[test]
    fn parses_claimed_outbox_row() {
        let row = row(&[
            Some("evt_1"),
            Some("order_created"),
            Some("https://example.com/hook"),
            Some(r#"{"x-api-key":"secret"}"#),
            Some(&payload_json()),
            Some("3"),
            Some("1"),
            Some("2026-01-01 00:00:00+00"),
        ]);

        let item = parse_claimed_outbox_row(&row).expect("row should parse");
        assert_eq!(item.id, "evt_1");
        assert_eq!(item.trigger_name, "order_created");
        assert_eq!(item.retry_count, 3);
        assert_eq!(item.attempts, 1);
        assert_eq!(item.locked_at, "2026-01-01 00:00:00+00");
        assert_eq!(
            item.headers.get("x-api-key").map(String::as_str),
            Some("secret")
        );
    }

    #[test]
    fn rejects_claimed_outbox_row_with_invalid_payload() {
        let row = row(&[
            Some("evt_bad"),
            Some("order_created"),
            Some("https://example.com/hook"),
            Some("{}"),
            Some("{bad-json"),
            Some("3"),
            Some("0"),
            Some("2026-01-01 00:00:00+00"),
        ]);

        let err = parse_claimed_outbox_row(&row).expect_err("payload must be rejected");
        assert_eq!(err.id, "evt_bad");
        assert!(err.reason.contains("invalid payload JSON"));
        assert_eq!(err.locked_at, "2026-01-01 00:00:00+00");
    }

    #[test]
    fn rejects_claimed_outbox_row_with_invalid_headers() {
        let row = row(&[
            Some("evt_bad"),
            Some("order_created"),
            Some("https://example.com/hook"),
            Some("[\"not\", \"headers\"]"),
            Some(&payload_json()),
            Some("3"),
            Some("0"),
            Some("2026-01-01 00:00:00+00"),
        ]);

        let err = parse_claimed_outbox_row(&row).expect_err("headers must be rejected");
        assert_eq!(err.id, "evt_bad");
        assert!(err.reason.contains("invalid headers JSON"));
        assert_eq!(err.locked_at, "2026-01-01 00:00:00+00");
    }

    #[test]
    fn rejects_claimed_outbox_row_with_invalid_webhook_url() {
        let row = row(&[
            Some("evt_bad"),
            Some("order_created"),
            Some("http://127.0.0.1/hook"),
            Some("{}"),
            Some(&payload_json()),
            Some("3"),
            Some("0"),
            Some("2026-01-01 00:00:00+00"),
        ]);

        let err = parse_claimed_outbox_row(&row).expect_err("webhook_url must be SSRF-checked");
        assert_eq!(err.id, "evt_bad");
        assert!(err.reason.contains("invalid webhook_url"));
        assert_eq!(err.locked_at, "2026-01-01 00:00:00+00");
    }

    #[test]
    fn rejects_claimed_outbox_row_without_lock_token() {
        let row = row(&[
            Some("evt_bad"),
            Some("order_created"),
            Some("https://example.com/hook"),
            Some("{}"),
            Some(&payload_json()),
            Some("3"),
            Some("0"),
            None,
        ]);

        let err = parse_claimed_outbox_row(&row).expect_err("locked_at must be required");
        assert_eq!(err.id, "evt_bad");
        assert!(err.reason.contains("missing locked_at"));
        assert_eq!(err.locked_at, "<missing>");
    }

    #[test]
    fn rejects_claimed_outbox_row_without_lock_token_before_payload_validation() {
        let row = row(&[
            Some("evt_bad"),
            Some("order_created"),
            Some("https://example.com/hook"),
            Some("{}"),
            Some("{bad-json"),
            Some("3"),
            Some("0"),
            None,
        ]);

        let err = parse_claimed_outbox_row(&row).expect_err("locked_at must be required first");
        assert_eq!(err.id, "evt_bad");
        assert!(err.reason.contains("missing locked_at"));
        assert_eq!(err.locked_at, "<missing>");
    }

    #[test]
    fn rejects_claimed_outbox_row_without_id() {
        let row = row(&[
            None,
            Some("order_created"),
            Some("https://example.com/hook"),
            Some("{}"),
            Some(&payload_json()),
            Some("3"),
            Some("0"),
            Some("2026-01-01 00:00:00+00"),
        ]);

        let err = parse_claimed_outbox_row(&row).expect_err("id must be required");
        assert_eq!(err.id, "<missing>");
        assert!(err.reason.contains("missing id"));
    }

    #[test]
    fn rejects_claimed_outbox_row_with_negative_counters() {
        let retry_row = row(&[
            Some("evt_bad"),
            Some("order_created"),
            Some("https://example.com/hook"),
            Some("{}"),
            Some(&payload_json()),
            Some("-1"),
            Some("0"),
            Some("2026-01-01 00:00:00+00"),
        ]);

        let err =
            parse_claimed_outbox_row(&retry_row).expect_err("retry_count must be non-negative");
        assert_eq!(err.id, "evt_bad");
        assert!(err.reason.contains("negative retry_count"));

        let attempts_row = row(&[
            Some("evt_bad"),
            Some("order_created"),
            Some("https://example.com/hook"),
            Some("{}"),
            Some(&payload_json()),
            Some("3"),
            Some("-1"),
            Some("2026-01-01 00:00:00+00"),
        ]);

        let err =
            parse_claimed_outbox_row(&attempts_row).expect_err("attempts must be non-negative");
        assert_eq!(err.id, "evt_bad");
        assert!(err.reason.contains("negative attempts"));
    }
}

async fn mark_failed_or_retry(
    pool: &PgPool,
    item: &OutboxItem,
    result: &WebhookDeliveryResult,
) -> Result<(), PgError> {
    let delivered_attempts = result.attempts.max(1);
    let next_attempt = item.attempts.saturating_add(delivered_attempts);
    let max_attempts = item.retry_count.min(MAX_WEBHOOK_RETRIES).saturating_add(1);
    let exhausted = next_attempt >= max_attempts;
    let status = if exhausted { "failed" } else { "retrying" };
    let delay_secs = if exhausted {
        0
    } else {
        retry_delay(next_attempt).as_secs()
    };
    let error = result.error.clone().unwrap_or_else(|| {
        result
            .response_status
            .map(|status| format!("Webhook HTTP status {}", status))
            .unwrap_or_else(|| "Webhook delivery failed".to_string())
    });
    let response_status = result.response_status.map(|s| s.to_string());

    execute_outbox_update(
        pool,
        r#"
UPDATE qail_webhook_outbox
SET status = $2,
    attempts = attempts + $3::int4,
    response_status = $4::int4,
    last_error = $5,
    locked_at = NULL,
    next_attempt_at = CASE
        WHEN $2 = 'failed' THEN next_attempt_at
        ELSE now() + ($6::int4 * interval '1 second')
    END
WHERE id = $1
  AND status = 'delivering'
  AND locked_at::text = $7
"#,
        &[
            Some(item.id.as_bytes().to_vec()),
            Some(status.as_bytes().to_vec()),
            Some(delivered_attempts.to_string().into_bytes()),
            response_status.map(String::into_bytes),
            Some(error.into_bytes()),
            Some(delay_secs.to_string().into_bytes()),
            Some(item.locked_at.as_bytes().to_vec()),
        ],
    )
    .await
}

async fn execute_outbox_update(
    pool: &PgPool,
    sql: &str,
    params: &[Option<Vec<u8>>],
) -> Result<(), PgError> {
    let mut conn = pool.acquire_system().await?;
    let result = conn.query_rows_with_params(sql, params).await;
    match result {
        Ok(_) => conn.release_checked().await,
        Err(e) => {
            let _ = conn.rollback_and_release().await;
            Err(e)
        }
    }
}
