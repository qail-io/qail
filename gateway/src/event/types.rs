use serde::Serialize;
use serde_json::Value;

/// Payload sent to the webhook.
#[derive(Debug, Serialize)]
pub struct WebhookPayload {
    /// Trigger name.
    pub trigger: String,
    /// Table affected.
    pub table: String,
    /// Operation: "INSERT", "UPDATE", "DELETE".
    pub operation: String,
    /// The row data (new for INSERT/UPDATE, old for DELETE).
    pub data: WebhookData,
    /// ISO 8601 timestamp.
    pub timestamp: String,
}

/// Data included in webhook payload.
#[derive(Debug, Serialize)]
pub struct WebhookData {
    /// New row data (for INSERT and UPDATE).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new: Option<Value>,
    /// Old row data (for UPDATE and DELETE, if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old: Option<Value>,
}

/// Delivery attempt result for logging.
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
