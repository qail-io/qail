//! Production middleware
//!
//! Rate limiting, timeouts, structured error responses, and request tracing.

use std::time::Duration;

mod allow_list;
mod api_error;
mod complexity;
mod rate_limit;
mod request_trace;

pub use allow_list::QueryAllowList;
pub use api_error::{ApiError, ApiErrorData};
pub use complexity::QueryComplexityGuard;
pub use rate_limit::{RateLimiter, rate_limit_middleware};
#[cfg(test)]
use request_trace::parse_traceparent;
pub use request_trace::request_tracer;

/// Request ID extension — inserted by `request_tracer` middleware,
/// extracted by handlers to populate `ApiError.request_id`.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

/// Request timeout duration.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests;
