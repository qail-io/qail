//! Payment provider trait — the abstraction over Xendit, Midtrans, DOKU, etc.
//!
//! Consumers implement `PaymentProvider` to connect the workflow engine
//! to their payment gateway. The workflow engine handles orchestration
//! (charge → wait for webhook → branch on result) while providers
//! handle the actual API communication.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ─── Payment Kind ───────────────────────────────────────────────────

/// Supported payment provider types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentKind {
    Xendit,
}

impl std::fmt::Display for PaymentKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PaymentKind::Xendit => write!(f, "xendit"),
        }
    }
}

// ─── Charge Request / Response ──────────────────────────────────────

/// Currency for payment amounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Currency {
    #[default]
    IDR,
    USD,
    PHP,
    THB,
    MYR,
}

/// Request to create a payment charge.
///
/// The workflow engine populates this from context keys, then passes
/// it to the `PaymentProvider::create_charge()` implementation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChargeRequest {
    /// Amount in smallest currency unit (e.g. cents, IDR whole).
    pub amount: i64,
    /// Currency (defaults to IDR).
    pub currency: Currency,
    /// Unique reference for this charge (e.g. order ID, booking ID).
    pub reference_id: String,
    /// Human-readable description shown to payer.
    pub description: Option<String>,
    /// Desired payment method (e.g. "QRIS", "VIRTUAL_ACCOUNT", "CARD", "EWALLET").
    pub payment_method: Option<String>,
    /// Return URL after payment (for redirect-based flows).
    pub return_url: Option<String>,
    /// Arbitrary metadata passed through to the provider.
    pub metadata: Option<serde_json::Value>,
}

/// Charge creation response from the payment provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChargeResponse {
    /// Provider-assigned charge/payment ID.
    pub charge_id: String,
    /// Current status of the charge.
    pub status: ChargeStatus,
    /// Redirect URL (for card/ewallet flows).
    pub redirect_url: Option<String>,
    /// QR code content string (for QRIS).
    pub qr_code: Option<String>,
    /// Virtual account number or payment code.
    pub payment_code: Option<String>,
    /// When this charge expires.
    pub expires_at: Option<String>,
    /// Raw provider response for debugging.
    pub raw: Option<serde_json::Value>,
}

/// Status of a payment charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChargeStatus {
    Pending,
    Paid,
    Failed,
    Expired,
    Refunded,
}

impl std::fmt::Display for ChargeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChargeStatus::Pending => write!(f, "pending"),
            ChargeStatus::Paid => write!(f, "paid"),
            ChargeStatus::Failed => write!(f, "failed"),
            ChargeStatus::Expired => write!(f, "expired"),
            ChargeStatus::Refunded => write!(f, "refunded"),
        }
    }
}

// ─── Webhook / Event ────────────────────────────────────────────────

/// Payment event parsed from a webhook callback.
///
/// The consumer's webhook handler calls `PaymentProvider::verify_webhook()`
/// which validates the signature and returns this struct. The workflow
/// engine then uses `resume_workflow()` to continue the flow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentEvent {
    /// Provider-assigned charge/payment ID.
    pub charge_id: String,
    /// Updated status.
    pub status: ChargeStatus,
    /// Confirmed amount (may differ for partial payments).
    pub amount: Option<i64>,
    /// Reference ID echoed back from create_charge.
    pub reference_id: Option<String>,
    /// Payment method used (e.g. "BCA_VA", "QRIS").
    pub payment_method: Option<String>,
    /// Arbitrary metadata from the provider.
    pub metadata: Option<serde_json::Value>,
}

// ─── Error ──────────────────────────────────────────────────────────

/// Errors from payment provider operations.
#[derive(Debug)]
pub enum PaymentError {
    /// Amount is invalid (zero, negative, or exceeds limits).
    InvalidAmount(String),
    /// Provider API returned an error.
    ProviderError(String),
    /// Webhook signature verification failed.
    WebhookVerificationFailed(String),
    /// Rate limited by provider.
    RateLimited { retry_after_secs: u64 },
    /// Generic error.
    Other(String),
}

impl std::fmt::Display for PaymentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PaymentError::InvalidAmount(e) => write!(f, "Invalid amount: {}", e),
            PaymentError::ProviderError(e) => write!(f, "Provider error: {}", e),
            PaymentError::WebhookVerificationFailed(e) => {
                write!(f, "Webhook verification failed: {}", e)
            }
            PaymentError::RateLimited { retry_after_secs } => {
                write!(f, "Rate limited, retry after {}s", retry_after_secs)
            }
            PaymentError::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for PaymentError {}

// ─── Trait ──────────────────────────────────────────────────────────

/// Trait for payment provider implementations.
///
/// Each payment gateway (Xendit, Midtrans, DOKU) implements this trait.
/// The workflow engine calls `create_charge()` during `WorkflowStep::Charge`
/// execution, and consumers call `verify_webhook()` in their webhook handlers.
///
/// # Example
///
/// ```rust,ignore
/// use qail_workflow::{PaymentProvider, PaymentKind, ChargeRequest, ChargeResponse, PaymentError, PaymentEvent};
///
/// struct XenditProvider { secret_key: String }
///
/// #[async_trait]
/// impl PaymentProvider for XenditProvider {
///     async fn create_charge(&self, req: ChargeRequest) -> Result<ChargeResponse, PaymentError> {
///         // Call Xendit Payment Request API
///         todo!()
///     }
///
///     fn verify_webhook(
///         &self, headers: &[(String, String)], body: &[u8],
///     ) -> Result<PaymentEvent, PaymentError> {
///         // Verify x-callback-token header, parse body
///         todo!()
///     }
///
///     fn kind(&self) -> PaymentKind { PaymentKind::Xendit }
/// }
/// ```
#[async_trait]
pub trait PaymentProvider: Send + Sync {
    /// Create a payment charge.
    ///
    /// Returns a `ChargeResponse` with the charge ID, status, and
    /// any redirect/QR/VA information the consumer needs to show the payer.
    async fn create_charge(&self, req: ChargeRequest) -> Result<ChargeResponse, PaymentError>;

    /// Verify a webhook callback and extract the payment event.
    ///
    /// # Arguments
    /// * `headers` — HTTP headers from the webhook request
    /// * `body` — Raw request body bytes
    fn verify_webhook(
        &self,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<PaymentEvent, PaymentError>;

    /// Which payment provider this implementation handles.
    fn kind(&self) -> PaymentKind;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_charge_request_serialization() {
        let req = ChargeRequest {
            amount: 150_000,
            currency: Currency::IDR,
            reference_id: "booking-001".into(),
            description: Some("Ferry ticket Bali-Nusa Penida".into()),
            payment_method: Some("QRIS".into()),
            return_url: None,
            metadata: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let restored: ChargeRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.amount, 150_000);
        assert_eq!(restored.reference_id, "booking-001");
    }

    #[test]
    fn test_charge_response_serialization() {
        let res = ChargeResponse {
            charge_id: "xendit-abc123".into(),
            status: ChargeStatus::Pending,
            redirect_url: None,
            qr_code: Some("00020101021226610014ID.CO.XENDIT".into()),
            payment_code: None,
            expires_at: Some("2026-02-13T16:00:00Z".into()),
            raw: None,
        };
        let json = serde_json::to_string(&res).unwrap();
        let restored: ChargeResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.charge_id, "xendit-abc123");
        assert_eq!(restored.status, ChargeStatus::Pending);
    }

    #[test]
    fn test_payment_event_serialization() {
        let event = PaymentEvent {
            charge_id: "xendit-abc123".into(),
            status: ChargeStatus::Paid,
            amount: Some(150_000),
            reference_id: Some("booking-001".into()),
            payment_method: Some("QRIS".into()),
            metadata: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        let restored: PaymentEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.status, ChargeStatus::Paid);
    }

    #[test]
    fn test_payment_kind_display() {
        assert_eq!(PaymentKind::Xendit.to_string(), "xendit");
    }

    #[test]
    fn test_charge_status_display() {
        assert_eq!(ChargeStatus::Pending.to_string(), "pending");
        assert_eq!(ChargeStatus::Paid.to_string(), "paid");
        assert_eq!(ChargeStatus::Expired.to_string(), "expired");
    }
}
