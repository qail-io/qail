//! JWKS (JSON Web Key Set) key store for JWT key rotation.
//!
//! Supports fetching and caching public keys from a JWKS endpoint (e.g.,
//! `https://auth.example.com/.well-known/jwks.json`), enabling seamless
//! JWT key rotation without gateway restarts.
//!
//! ## Usage
//!
//! Set `JWKS_URL` to enable JWKS-based JWT validation:
//! ```text
//! JWKS_URL=https://auth.example.com/.well-known/jwks.json
//! ```
//!
//! When configured, the gateway will:
//! 1. Fetch the JWKS on startup
//! 2. Cache keys by `kid` (Key ID)
//! 3. Refresh the keyset periodically (default: every 5 minutes)
//! 4. Match incoming JWT `kid` header to the cached keys

use jsonwebtoken::{DecodingKey, jwk::JwkSet};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

const DEFAULT_JWKS_REFRESH_SECS: u64 = 300;
const MIN_JWKS_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_JWKS_BODY_BYTES: usize = 1024 * 1024;

/// JWKS key store — caches DecodingKeys fetched from a JWKS endpoint.
#[derive(Clone)]
pub struct JwksKeyStore {
    /// JWKS endpoint URL
    url: String,
    /// Cached keys: kid → DecodingKey
    keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
    /// Refresh interval
    refresh_interval: Duration,
}

impl JwksKeyStore {
    /// Create a new JWKS key store from a URL.
    ///
    /// Call `initial_fetch()` after construction to populate keys.
    pub fn new(url: impl Into<String>, refresh_interval: Duration) -> Self {
        let refresh_interval = refresh_interval.max(MIN_JWKS_REFRESH_INTERVAL);
        Self {
            url: url.into(),
            keys: Arc::new(RwLock::new(HashMap::new())),
            refresh_interval,
        }
    }

    /// Create from environment variables.
    ///
    /// Returns `None` if `JWKS_URL` is not set.
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("JWKS_URL").ok()?;
        if url.is_empty() {
            return None;
        }
        let refresh_secs = std::env::var("JWKS_REFRESH_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_JWKS_REFRESH_SECS);

        Some(Self::new(url, Duration::from_secs(refresh_secs)))
    }

    /// Fetch JWKS from the endpoint and populate the key cache.
    pub async fn initial_fetch(&self) -> Result<usize, String> {
        self.refresh_keys().await
    }

    /// Look up a DecodingKey by `kid` (Key ID from JWT header).
    pub async fn get_key(&self, kid: &str) -> Option<DecodingKey> {
        let keys = self.keys.read().await;
        keys.get(kid).cloned()
    }

    /// Number of cached keys.
    pub async fn key_count(&self) -> usize {
        self.keys.read().await.len()
    }

    /// Synchronous read access to cached keys.
    ///
    /// Used by the sync `extract_auth_from_headers` path. If the lock
    /// is held by a writer (during refresh), returns an empty map.
    pub fn keys_blocking(&self) -> std::sync::Arc<HashMap<String, DecodingKey>> {
        // try_read avoids blocking the async runtime
        match self.keys.try_read() {
            Ok(guard) => std::sync::Arc::new(guard.clone()),
            Err(_) => std::sync::Arc::new(HashMap::new()),
        }
    }

    /// Start background refresh task.
    pub fn start_refresh_task(&self) {
        let store = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(store.refresh_interval).await;
                match store.refresh_keys().await {
                    Ok(n) => {
                        tracing::debug!(keys = n, "JWKS refreshed");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "JWKS refresh failed — using stale keys");
                    }
                }
            }
        });
    }

    /// Fetch JWKS and update cached keys.
    async fn refresh_keys(&self) -> Result<usize, String> {
        let client = reqwest::Client::builder()
            .timeout(JWKS_FETCH_TIMEOUT)
            .build()
            .map_err(|e| format!("JWKS HTTP client build failed: {}", e))?;

        let response = client
            .get(&self.url)
            .send()
            .await
            .map_err(|e| format!("JWKS fetch failed: {}", e))?;

        let status = response.status();
        if !status.is_success() {
            return Err(format!("JWKS endpoint returned status {}", status));
        }

        let body = read_limited_response_body(response).await?;
        let jwks = parse_jwks_body(&body)?;

        let mut new_keys = HashMap::new();
        for jwk in &jwks.keys {
            if let Some(ref kid) = jwk.common.key_id {
                match DecodingKey::from_jwk(jwk) {
                    Ok(key) => {
                        new_keys.insert(kid.clone(), key);
                    }
                    Err(e) => {
                        tracing::warn!(kid = %kid, error = %e, "Skipping invalid JWK");
                    }
                }
            }
        }

        let count = new_keys.len();
        {
            let mut keys = self.keys.write().await;
            *keys = new_keys;
        }

        Ok(count)
    }
}

async fn read_limited_response_body(mut response: reqwest::Response) -> Result<Vec<u8>, String> {
    if let Some(content_length) = response.content_length()
        && content_length > MAX_JWKS_BODY_BYTES as u64
    {
        return Err(format!(
            "JWKS body too large: {} bytes exceeds {} byte limit",
            content_length, MAX_JWKS_BODY_BYTES
        ));
    }

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("JWKS body read failed: {}", e))?
    {
        if body.len().saturating_add(chunk.len()) > MAX_JWKS_BODY_BYTES {
            return Err(format!(
                "JWKS body too large: exceeds {} byte limit",
                MAX_JWKS_BODY_BYTES
            ));
        }
        body.extend_from_slice(&chunk);
    }

    Ok(body)
}

fn parse_jwks_body(body: &[u8]) -> Result<JwkSet, String> {
    if body.len() > MAX_JWKS_BODY_BYTES {
        return Err(format!(
            "JWKS body too large: {} bytes exceeds {} byte limit",
            body.len(),
            MAX_JWKS_BODY_BYTES
        ));
    }

    serde_json::from_slice(body).map_err(|e| format!("JWKS parse failed: {}", e))
}

/// Parse `kid` from a JWT header without full validation.
///
/// Used to select the correct JWKS key before validation.
pub fn extract_kid_from_jwt(token: &str) -> Option<String> {
    let header_b64 = token.split('.').next()?;

    // JWT base64url decode (no padding)
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let header_json = URL_SAFE_NO_PAD.decode(header_b64).ok()?;
    let header: serde_json::Value = serde_json::from_slice(&header_json).ok()?;
    header.get("kid")?.as_str().map(String::from)
}

#[cfg(test)]
mod tests;
