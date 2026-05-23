use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use crate::policy::OperationType;

use super::delivery::deliver_webhook;
use super::outbox::{
    OutboxEventInsert, ensure_outbox_schema, insert_outbox_event, spawn_outbox_worker,
};
use super::{EventTrigger, WebhookData, WebhookPayload, normalize_trigger};

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

#[cfg(test)]
pub(super) fn try_acquire_webhook_permit(
    sem: &Arc<tokio::sync::Semaphore>,
    trigger_name: &str,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    match Arc::clone(sem).try_acquire_owned() {
        Ok(permit) => Some(permit),
        Err(_) => {
            tracing::warn!(
                trigger = %trigger_name,
                "Webhook dropped: concurrency limit reached"
            );
            None
        }
    }
}

impl Default for EventTriggerEngine {
    fn default() -> Self {
        Self {
            triggers: Vec::new(),
            client: None,
            webhook_semaphore: Arc::new(tokio::sync::Semaphore::new(512)),
        }
    }
}

impl EventTriggerEngine {
    fn add_validated_trigger(&mut self, trigger: EventTrigger) {
        tracing::info!(
            "Event trigger registered: {} on {}.{:?} → {}",
            trigger.name,
            trigger.table,
            trigger.operations,
            trigger.webhook_url
        );
        self.triggers.push(trigger);
    }

    /// Create a new event trigger engine with a shared HTTP client.
    pub fn new() -> Self {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            // SECURITY: Disable redirects to prevent SSRF bypass via 301/302
            // to private/internal targets after DNS validation.
            .redirect(reqwest::redirect::Policy::none())
            .build()
        {
            Ok(client) => Some(Arc::new(client)),
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "Failed to build webhook HTTP client; event delivery disabled"
                );
                None
            }
        };

        Self {
            triggers: Vec::new(),
            client,
            webhook_semaphore: Arc::new(tokio::sync::Semaphore::new(512)),
        }
    }

    /// Register a trigger
    pub fn add_trigger(&mut self, trigger: EventTrigger) {
        match normalize_trigger(trigger) {
            Ok(valid) => self.add_validated_trigger(valid),
            Err(e) => {
                tracing::error!("Event trigger rejected: {}", e);
            }
        }
    }

    /// Load triggers from a YAML file
    pub fn load_from_file(&mut self, path: &str) -> Result<(), String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read event triggers file: {}", e))?;

        let triggers: Vec<EventTrigger> = serde_yaml::from_str(&content)
            .map_err(|e| format!("Failed to parse event triggers: {}", e))?;

        for trigger in triggers {
            let valid = normalize_trigger(trigger)?;
            self.add_validated_trigger(valid);
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

    /// Return whether any enabled mutation trigger is registered.
    pub fn has_enabled_mutation_triggers(&self) -> bool {
        self.triggers.iter().any(|trigger| {
            trigger.enabled
                && trigger.operations.iter().any(|op| {
                    matches!(
                        op,
                        OperationType::Create | OperationType::Update | OperationType::Delete
                    )
                })
        })
    }

    /// Create the durable webhook outbox table if event delivery is configured.
    pub async fn ensure_durable_outbox(
        &self,
        pool: &qail_pg::PgPool,
    ) -> Result<(), qail_pg::PgError> {
        if self.has_enabled_mutation_triggers() {
            ensure_outbox_schema(pool).await?;
        }
        Ok(())
    }

    /// Start the background outbox dispatcher.
    pub fn start_outbox_worker(&self, pool: qail_pg::PgPool) {
        if !self.has_enabled_mutation_triggers() {
            return;
        }
        let Some(client) = self.client.as_ref().map(Arc::clone) else {
            tracing::warn!("Webhook outbox worker disabled: HTTP client is unavailable");
            return;
        };
        spawn_outbox_worker(pool, client, Arc::clone(&self.webhook_semaphore));
    }

    /// Persist matching trigger deliveries into the outbox inside the caller's
    /// open mutation transaction.
    pub async fn enqueue_durable(
        &self,
        conn: &mut qail_pg::PooledConnection,
        table: &str,
        op: OperationType,
        new_data: Option<Value>,
        old_data: Option<Value>,
    ) -> Result<usize, qail_pg::PgError> {
        let matching = self.triggers_for(table, &op);
        if matching.is_empty() {
            return Ok(0);
        }

        let op_str = match op {
            OperationType::Read => return Ok(0),
            OperationType::Create => "INSERT",
            OperationType::Update => "UPDATE",
            OperationType::Delete => "DELETE",
        };

        let timestamp = chrono::Utc::now().to_rfc3339();
        let mut queued = 0;
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
            insert_outbox_event(
                conn,
                OutboxEventInsert {
                    trigger_name: &trigger.name,
                    table,
                    operation: op_str,
                    webhook_url: &trigger.webhook_url,
                    headers: &trigger.headers,
                    payload: &payload,
                    retry_count: trigger.retry_count,
                },
            )
            .await?;
            queued += 1;
        }
        Ok(queued)
    }

    /// Fire matching triggers for a mutation without persistence.
    ///
    /// This legacy path is **non-blocking** — each webhook call is spawned as
    /// a separate tokio task. REST mutations should use [`Self::enqueue_durable`]
    /// while their database transaction is still open.
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
            // SECURITY: Bounded concurrency — acquire permit before delivery.
            // Bursts are queued for up to 10s before dropping, preventing
            // unbounded memory growth while handling transient spikes.
            tokio::spawn(async move {
                let permit = match tokio::time::timeout(
                    Duration::from_secs(10),
                    sem.acquire_owned(),
                )
                .await
                {
                    Ok(Ok(p)) => p,
                    _ => {
                        tracing::error!(
                            trigger = %trigger_name,
                            "Webhook dropped: concurrency limit timeout (10s)"
                        );
                        return;
                    }
                };

                let _permit = permit;
                deliver_webhook(client, &url, &headers, &payload, retry_count, &trigger_name).await;
            });
        }
    }
}
