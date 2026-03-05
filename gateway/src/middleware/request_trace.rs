use axum::{http::Request, middleware::Next, response::Response};
use std::time::Instant;

use super::RequestId;

/// Request tracing middleware — wraps each request with a structured tracing span.
///
/// Logs: request_id (UUID), method, path, status, duration_ms.
/// Injects `x-request-id` and `x-response-time` headers.
///
/// **W3C Trace Context (Phase 5)**: If the client sends a `traceparent` header,
/// the trace_id and parent_id are extracted and included in log entries.
/// Both `traceparent` and `tracestate` are propagated in the response.
pub async fn request_tracer(mut request: Request<axum::body::Body>, next: Next) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let start = Instant::now();

    // P1-A: Store request_id in extensions for downstream handlers
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));

    // Phase 5: Extract W3C Trace Context from incoming headers
    let trace_ctx = request
        .headers()
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_traceparent);

    let tracestate = request
        .headers()
        .get("tracestate")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Extract table from path: /api/{table}/... → table
    let table = path
        .trim_start_matches('/')
        .split('/')
        .nth(1)
        .unwrap_or("unknown")
        .to_string();

    // Log with trace context when available
    if let Some(ref ctx) = trace_ctx {
        tracing::info!(
            request_id = %request_id,
            trace_id = %ctx.trace_id,
            parent_id = %ctx.parent_id,
            method = %method,
            path = %path,
            table = %table,
            "→ request started"
        );
    } else {
        tracing::info!(
            request_id = %request_id,
            method = %method,
            path = %path,
            table = %table,
            "→ request started"
        );
    }

    let mut response = next.run(request).await;

    let duration = start.elapsed();
    let status = response.status().as_u16();
    let duration_ms = duration.as_secs_f64() * 1000.0;

    if let Some(ref ctx) = trace_ctx {
        tracing::info!(
            request_id = %request_id,
            trace_id = %ctx.trace_id,
            parent_id = %ctx.parent_id,
            method = %method,
            path = %path,
            status = status,
            duration_ms = %format!("{:.2}", duration_ms),
            "← request completed"
        );
    } else {
        tracing::info!(
            request_id = %request_id,
            method = %method,
            path = %path,
            status = status,
            duration_ms = %format!("{:.2}", duration_ms),
            "← request completed"
        );
    }

    // Inject tracing headers
    if let Ok(v) = request_id.parse() {
        response.headers_mut().insert("x-request-id", v);
    }
    if let Ok(v) = format!("{:.2}ms", duration_ms).parse() {
        response.headers_mut().insert("x-response-time", v);
    }

    // Phase 5: Propagate W3C trace context in response
    if let Some(ref ctx) = trace_ctx {
        // Generate a new span_id for this gateway hop
        let span_id = &request_id[..16].replace('-', "");
        let traceparent = format!("00-{}-{}-{:02x}", ctx.trace_id, span_id, ctx.flags);
        if let Ok(v) = traceparent.parse() {
            response.headers_mut().insert("traceparent", v);
        }
    }
    if let Some(ref ts) = tracestate
        && let Ok(v) = ts.parse()
    {
        response.headers_mut().insert("tracestate", v);
    }

    response
}

/// Parsed W3C Trace Context from `traceparent` header.
#[derive(Debug, Clone)]
pub(super) struct TraceContext {
    /// 32-hex-char trace ID
    pub(super) trace_id: String,
    /// 16-hex-char parent span ID
    pub(super) parent_id: String,
    /// Trace flags (bit 0 = sampled)
    pub(super) flags: u8,
}

/// Parse a W3C `traceparent` header value.
///
/// Format: `{version}-{trace_id}-{parent_id}-{trace_flags}`
/// Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
pub(super) fn parse_traceparent(value: &str) -> Option<TraceContext> {
    let parts: Vec<&str> = value.trim().split('-').collect();
    if parts.len() != 4 {
        return None;
    }

    let version = parts[0];
    let trace_id = parts[1];
    let parent_id = parts[2];
    let flags_hex = parts[3];

    // Version must be "00" (currently only version supported)
    if version != "00" {
        return None;
    }

    // trace_id: 32 hex chars, must not be all zeros
    if trace_id.len() != 32 || !trace_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    if trace_id.chars().all(|c| c == '0') {
        return None;
    }

    // parent_id: 16 hex chars, must not be all zeros
    if parent_id.len() != 16 || !parent_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    if parent_id.chars().all(|c| c == '0') {
        return None;
    }

    // flags: 2 hex chars
    let flags = u8::from_str_radix(flags_hex, 16).ok()?;

    Some(TraceContext {
        trace_id: trace_id.to_string(),
        parent_id: parent_id.to_string(),
        flags,
    })
}
