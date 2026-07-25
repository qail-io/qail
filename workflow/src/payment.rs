//! Payment provider trait — the abstraction over Xendit, Midtrans, DOKU, etc.
//!
//! Consumers implement `PaymentProvider` to connect the workflow engine
//! to their payment gateway. The workflow engine handles orchestration
//! (charge → wait for webhook → branch on result) while providers
//! handle the actual API communication.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ─── Order Origin ──────────────────────────────────────────────────

/// Source channel that created the order or checkout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderOrigin {
    /// Order was created from a WhatsApp conversation.
    WhatsApp,
    /// Order was created through an MCP/ChatGPT tool flow.
    Mcp,
    /// Order was created from a web checkout.
    Web,
    /// Order was created from the native iOS app.
    IosApp,
    /// Order was created from the native Android app.
    AndroidApp,
    /// Order was created by a direct API/backend integration.
    Api,
}

impl OrderOrigin {
    /// Parse an order origin from a workflow context string.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "whatsapp" | "wa" => Some(Self::WhatsApp),
            "mcp" | "chatgpt" | "chatgpt_mcp" | "chatgpt-mcp" => Some(Self::Mcp),
            "web" | "checkout" => Some(Self::Web),
            "ios" | "ios_app" | "ios-app" | "iphone" | "ipad" => Some(Self::IosApp),
            "android" | "android_app" | "android-app" => Some(Self::AndroidApp),
            "api" | "backend" => Some(Self::Api),
            _ => None,
        }
    }

    /// Stable lowercase value for logging, metadata, and provider payloads.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WhatsApp => "whatsapp",
            Self::Mcp => "mcp",
            Self::Web => "web",
            Self::IosApp => "ios_app",
            Self::AndroidApp => "android_app",
            Self::Api => "api",
        }
    }
}

// ─── Payment Kind ───────────────────────────────────────────────────

/// Supported payment provider types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentKind {
    /// Xendit payment provider.
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
    /// Indonesian Rupiah.
    #[default]
    IDR,
    /// US Dollar.
    USD,
    /// Philippine Peso.
    PHP,
    /// Thai Baht.
    THB,
    /// Malaysian Ringgit.
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
    /// Source channel that created the order or checkout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_origin: Option<OrderOrigin>,
    /// Stable idempotency key to pass through to the payment provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
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
    /// Raw provider response for internal debugging.
    ///
    /// Do not expose this field to chat, WhatsApp, email, or other user-facing
    /// channels. Use [`ChargeResponse::display_for`] instead.
    pub raw: Option<serde_json::Value>,
}

/// Payment details safe to show in chat or notification channels.
///
/// This intentionally omits provider `raw` data and cardholder data. For card
/// payments, expose only `redirect_url` and keep collection on the PSP page.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentDisplay {
    /// Provider-assigned charge/payment ID.
    pub charge_id: String,
    /// Current status of the charge.
    pub status: ChargeStatus,
    /// Amount in smallest currency unit (e.g. IDR whole).
    pub amount: i64,
    /// Currency.
    pub currency: Currency,
    /// Unique reference for this charge (e.g. order ID, booking ID).
    pub reference_id: String,
    /// Human-readable description shown to payer.
    pub description: Option<String>,
    /// Desired payment method (e.g. "QRIS", "VIRTUAL_ACCOUNT", "CARD", "EWALLET").
    pub payment_method: Option<String>,
    /// Source channel that created the order or checkout.
    pub order_origin: Option<OrderOrigin>,
    /// Redirect URL (for card/ewallet flows).
    pub redirect_url: Option<String>,
    /// QR code content string (for QRIS).
    pub qr_code: Option<String>,
    /// Virtual account number or payment code.
    pub payment_code: Option<String>,
    /// When this charge expires.
    pub expires_at: Option<String>,
}

impl ChargeResponse {
    /// Build a redacted display object from the original charge request.
    pub fn display_for(&self, request: &ChargeRequest) -> PaymentDisplay {
        PaymentDisplay {
            charge_id: self.charge_id.clone(),
            status: self.status,
            amount: request.amount,
            currency: request.currency,
            reference_id: request.reference_id.clone(),
            description: request.description.clone(),
            payment_method: request.payment_method.clone(),
            order_origin: request.order_origin,
            redirect_url: self.redirect_url.clone(),
            qr_code: self.qr_code.clone(),
            payment_code: self.payment_code.clone(),
            expires_at: self.expires_at.clone(),
        }
    }
}

/// Status of a payment charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChargeStatus {
    /// Charge created, awaiting payment.
    Pending,
    /// Payment confirmed.
    Paid,
    /// Payment failed (declined, insufficient funds, etc.).
    Failed,
    /// Charge expired before payment.
    Expired,
    /// Payment was refunded after completion.
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
    /// Source channel that created the order or checkout, if echoed by metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_origin: Option<OrderOrigin>,
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
    RateLimited {
        /// Seconds to wait before retrying.
        retry_after_secs: u64,
    },
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
            order_origin: Some(OrderOrigin::WhatsApp),
            idempotency_key: Some("charge:booking-001".into()),
            return_url: None,
            metadata: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let restored: ChargeRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.amount, 150_000);
        assert_eq!(restored.reference_id, "booking-001");
        assert_eq!(restored.order_origin, Some(OrderOrigin::WhatsApp));
        assert_eq!(
            restored.idempotency_key.as_deref(),
            Some("charge:booking-001")
        );
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
            order_origin: Some(OrderOrigin::WhatsApp),
            metadata: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        let restored: PaymentEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.status, ChargeStatus::Paid);
        assert_eq!(restored.order_origin, Some(OrderOrigin::WhatsApp));
    }

    #[test]
    fn test_payment_kind_display() {
        assert_eq!(PaymentKind::Xendit.to_string(), "xendit");
    }

    #[test]
    fn test_order_origin_parse() {
        assert_eq!(OrderOrigin::parse("whatsapp"), Some(OrderOrigin::WhatsApp));
        assert_eq!(OrderOrigin::parse("wa"), Some(OrderOrigin::WhatsApp));
        assert_eq!(OrderOrigin::parse("mcp"), Some(OrderOrigin::Mcp));
        assert_eq!(OrderOrigin::parse("chatgpt"), Some(OrderOrigin::Mcp));
        assert_eq!(OrderOrigin::parse("ios"), Some(OrderOrigin::IosApp));
        assert_eq!(
            OrderOrigin::parse("android-app"),
            Some(OrderOrigin::AndroidApp)
        );
        assert_eq!(OrderOrigin::parse("unknown"), None);
    }

    #[test]
    fn test_payment_display_omits_raw_provider_data() {
        let req = ChargeRequest {
            amount: 150_000,
            currency: Currency::IDR,
            reference_id: "booking-001".into(),
            description: Some("Ferry ticket Bali-Nusa Penida".into()),
            payment_method: Some("QRIS".into()),
            order_origin: Some(OrderOrigin::Mcp),
            idempotency_key: Some("idem-1".into()),
            return_url: None,
            metadata: None,
        };
        let res = ChargeResponse {
            charge_id: "xendit-abc123".into(),
            status: ChargeStatus::Pending,
            redirect_url: None,
            qr_code: Some("00020101021226610014ID.CO.XENDIT".into()),
            payment_code: None,
            expires_at: Some("2026-02-13T16:00:00Z".into()),
            raw: Some(serde_json::json!({"secret": "do-not-show"})),
        };
        let display = res.display_for(&req);
        let value = serde_json::to_value(display).unwrap();
        assert!(value.get("raw").is_none());
        assert_eq!(
            value.get("order_origin").and_then(|v| v.as_str()),
            Some("mcp")
        );
    }

    #[test]
    fn test_charge_status_display() {
        assert_eq!(ChargeStatus::Pending.to_string(), "pending");
        assert_eq!(ChargeStatus::Paid.to_string(), "paid");
        assert_eq!(ChargeStatus::Expired.to_string(), "expired");
    }
}
