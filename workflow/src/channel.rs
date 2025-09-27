//! Notification channels — trait for sending messages via WhatsApp, Email, SMS, etc.
//!
//! Consumers implement `NotifyChannel` to connect the workflow engine
//! to their existing messaging infrastructure.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Supported notification channel types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelKind {
    WhatsApp,
    Email,
    Sms,
    Push,
}

impl std::fmt::Display for ChannelKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelKind::WhatsApp => write!(f, "whatsapp"),
            ChannelKind::Email => write!(f, "email"),
            ChannelKind::Sms => write!(f, "sms"),
            ChannelKind::Push => write!(f, "push"),
        }
    }
}

/// Error type for channel operations.
#[derive(Debug)]
pub enum ChannelError {
    /// Recipient not found or invalid
    InvalidRecipient(String),
    /// Template not found
    TemplateNotFound(String),
    /// Provider API error
    ProviderError(String),
    /// Rate limited
    RateLimited { retry_after_secs: u64 },
}

impl std::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelError::InvalidRecipient(r) => write!(f, "Invalid recipient: {}", r),
            ChannelError::TemplateNotFound(t) => write!(f, "Template not found: {}", t),
            ChannelError::ProviderError(e) => write!(f, "Provider error: {}", e),
            ChannelError::RateLimited { retry_after_secs } => {
                write!(f, "Rate limited, retry after {}s", retry_after_secs)
            }
        }
    }
}

impl std::error::Error for ChannelError {}

/// Trait for notification channel implementations.
///
/// Each messaging provider (WhatsApp/WABA, email, SMS) implements this trait.
/// The workflow engine calls `send()` during `WorkflowStep::Notify` execution.
///
/// # Example
///
/// ```rust,ignore
/// use qail_workflow::{NotifyChannel, ChannelKind, ChannelError};
///
/// struct WabaChannel { client: WabaClient }
///
/// #[async_trait]
/// impl NotifyChannel for WabaChannel {
///     async fn send(
///         &self, recipient: &str, template: &str, params: &serde_json::Value,
///     ) -> Result<(), ChannelError> {
///         self.client.send_template(recipient, template, params).await
///             .map_err(|e| ChannelError::ProviderError(e.to_string()))
///     }
///     fn kind(&self) -> ChannelKind { ChannelKind::WhatsApp }
/// }
/// ```
#[async_trait]
pub trait NotifyChannel: Send + Sync {
    /// Send a template message to a recipient.
    ///
    /// # Arguments
    /// * `recipient` — Phone number, email address, or device token
    /// * `template` — Template name registered with the provider
    /// * `params` — Template variable values as JSON
    async fn send(
        &self,
        recipient: &str,
        template: &str,
        params: &serde_json::Value,
    ) -> Result<(), ChannelError>;

    /// Which channel kind this implementation handles.
    fn kind(&self) -> ChannelKind;
}
