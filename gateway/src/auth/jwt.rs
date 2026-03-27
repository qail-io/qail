use crate::error::GatewayError;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};

use super::{AuthContext, JwtClaims};

pub struct JwtConfig {
    /// HMAC shared secret (for HS256/HS384/HS512).
    pub secret: Option<String>,
    /// RSA/EC public key in PEM format (for RS*/ES*).
    pub public_key: Option<String>,
    /// Signing algorithm (default: HS256).
    pub algorithm: Algorithm,
    /// Expected `iss` claim (if set, tokens without it are rejected).
    pub issuer: Option<String>,
    /// Expected `aud` claim.
    pub audience: Option<String>,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            secret: None,
            public_key: None,
            algorithm: Algorithm::HS256,
            issuer: None,
            audience: None,
        }
    }
}

/// Decode and validate a JWT token, returning an [`AuthContext`] on success.
pub fn validate_jwt(token: &str, config: &JwtConfig) -> Result<AuthContext, GatewayError> {
    let decoding_key = match config.algorithm {
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
            let secret = config
                .secret
                .as_ref()
                .ok_or_else(|| GatewayError::Auth("JWT secret not configured".to_string()))?;
            DecodingKey::from_secret(secret.as_bytes())
        }
        Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512 => {
            let key = config
                .public_key
                .as_ref()
                .ok_or_else(|| GatewayError::Auth("JWT public key not configured".to_string()))?;
            DecodingKey::from_rsa_pem(key.as_bytes())
                .map_err(|e| GatewayError::Auth(format!("Invalid RSA key: {}", e)))?
        }
        _ => return Err(GatewayError::Auth("Unsupported JWT algorithm".to_string())),
    };

    let mut validation = Validation::new(config.algorithm);

    if let Some(ref issuer) = config.issuer {
        validation.set_issuer(&[issuer]);
    }
    if let Some(ref audience) = config.audience {
        validation.set_audience(&[audience]);
    }

    let token_data = decode::<JwtClaims>(token, &decoding_key, &validation)
        .map_err(|e| GatewayError::Auth(format!("Invalid token: {}", e)))?;

    let claims = token_data.claims;
    let mut extra_claims = claims.extra;
    extra_claims.insert(
        "exp".to_string(),
        serde_json::Value::from(claims.exp as u64),
    );

    Ok(AuthContext {
        user_id: claims.sub,
        role: claims.role.unwrap_or_else(|| "user".to_string()),
        tenant_id: claims.tenant_id,
        claims: extra_claims,
    })
}

/// Parse configured JWT algorithm names into [`Algorithm`] values.
///
/// Input is case-insensitive and duplicate values are de-duplicated while
/// preserving first-seen order.
pub fn parse_allowed_algorithms(values: &[String]) -> Result<Vec<Algorithm>, GatewayError> {
    let mut out = Vec::new();

    for raw in values {
        let alg = match raw.trim().to_ascii_uppercase().as_str() {
            "RS256" => Algorithm::RS256,
            "RS384" => Algorithm::RS384,
            "RS512" => Algorithm::RS512,
            "ES256" => Algorithm::ES256,
            "ES384" => Algorithm::ES384,
            "PS256" => Algorithm::PS256,
            "PS384" => Algorithm::PS384,
            "PS512" => Algorithm::PS512,
            "HS256" => Algorithm::HS256,
            "HS384" => Algorithm::HS384,
            "HS512" => Algorithm::HS512,
            "EDDSA" => Algorithm::EdDSA,
            other => {
                return Err(GatewayError::Config(format!(
                    "Unsupported JWT algorithm '{}' in jwt_allowed_algorithms",
                    other
                )));
            }
        };

        if !out.contains(&alg) {
            out.push(alg);
        }
    }

    Ok(out)
}

/// Detect JWT algorithm from token header without full validation.
pub(super) fn detect_jwt_algorithm(token: &str) -> Option<Algorithm> {
    let header_b64 = token.split('.').next()?;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let header_json = URL_SAFE_NO_PAD.decode(header_b64).ok()?;
    let header: serde_json::Value = serde_json::from_slice(&header_json).ok()?;
    let alg_str = header.get("alg")?.as_str()?;
    match alg_str {
        "RS256" => Some(Algorithm::RS256),
        "RS384" => Some(Algorithm::RS384),
        "RS512" => Some(Algorithm::RS512),
        "ES256" => Some(Algorithm::ES256),
        "ES384" => Some(Algorithm::ES384),
        "PS256" => Some(Algorithm::PS256),
        "PS384" => Some(Algorithm::PS384),
        "PS512" => Some(Algorithm::PS512),
        "HS256" => Some(Algorithm::HS256),
        "HS384" => Some(Algorithm::HS384),
        "HS512" => Some(Algorithm::HS512),
        "EdDSA" => Some(Algorithm::EdDSA),
        _ => None,
    }
}
