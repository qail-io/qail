use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use qail_pg::{PgError, PgPool, PooledConnection};

use super::delivery::{WebhookDeliveryResult, deliver_webhook_once};
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
    o.attempts
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
    for row in rows {
        let id = row.get_string(0).unwrap_or_default();
        let Some(trigger_name) = row.get_string(1) else {
            tracing::warn!("Webhook outbox row missing trigger_name");
            continue;
        };
        let Some(webhook_url) = row.get_string(2) else {
            tracing::warn!(id = %id, "Webhook outbox row missing webhook_url");
            continue;
        };
        let headers: HashMap<String, String> = row
            .get_string(3)
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();
        let Some(payload) = row
            .get_string(4)
            .and_then(|raw| serde_json::from_str::<WebhookPayload>(&raw).ok())
        else {
            tracing::warn!(id = %id, "Webhook outbox row has invalid payload JSON");
            continue;
        };
        let retry_count = row.get_i32(5).unwrap_or(0).max(0) as u32;
        let attempts = row.get_i32(6).unwrap_or(0).max(0) as u32;
        items.push(OutboxItem {
            id,
            trigger_name,
            webhook_url,
            headers,
            payload,
            retry_count,
            attempts,
        });
    }

    Ok(items)
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
"#,
        &[
            Some(item.id.as_bytes().to_vec()),
            Some(attempts.into_bytes()),
            response_status.map(String::into_bytes),
        ],
    )
    .await
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
"#,
        &[
            Some(item.id.as_bytes().to_vec()),
            Some(status.as_bytes().to_vec()),
            Some(delivered_attempts.to_string().into_bytes()),
            response_status.map(String::into_bytes),
            Some(error.into_bytes()),
            Some(delay_secs.to_string().into_bytes()),
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
